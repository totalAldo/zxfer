#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024-2026 Aldo Gonzalez
# Copyright (c) 2013-2019 Allan Jude <allanjude@freebsd.org>
# Copyright (c) 2010,2011 Ivan Nash Dreckman
# Copyright (c) 2007,2008 Constantin Gonzalez
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# BSD HEADER END
# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# OWNED LOCK / LEASE COORDINATION
################################################################################

# Module contract:
# owns globals: owned-lock metadata scratch and the memoized own-process start
#   token.
# reads globals: none directly.
# mutates caches: local lock directories.
# returns via stdout: normalized process-start tokens, metadata paths, and
#   created lock paths.
#
# Lock identity is owner pid + process start token ONLY. Earlier metadata
# formats (V1: kind/purpose/hostname/created_at fields) parse as corrupt
# (status 2) and are reaped through the existing corrupt-metadata policy
# instead of crashing; locks are seconds-lived so no cross-version
# compatibility is required.

ZXFER_LOCK_METADATA_HEADER="ZXFER_LOCK_METADATA_V2"

# Purpose: Reset the owned-lock metadata scratch results so the next lookup
# starts from a clean state.
# Usage: Called before metadata loads and during owned-lock tracking resets.
zxfer_reset_owned_lock_metadata_result() {
	g_zxfer_owned_lock_pid_result=""
	g_zxfer_owned_lock_start_token_result=""
}

# Purpose: Reset the owned-lock tracking state so the next runtime pass starts
# from a clean state.
# Usage: Called during runtime bootstrap before this module reuses mutable
# scratch globals or cached decisions.
zxfer_reset_owned_lock_tracking() {
	g_zxfer_own_process_start_token=""
	zxfer_reset_owned_lock_metadata_result
}

# Purpose: Return the metadata file path inside one owned lock directory.
# Usage: Called by the metadata read/write helpers and layout probes.
zxfer_get_owned_lock_metadata_path() {
	l_lock_dir=$1
	printf '%s/metadata\n' "$l_lock_dir"
}

# Purpose: Normalize one free-form text field to a single-line, single-spaced
# value using field splitting only (no tr/sed spawns).
# Usage: Called by callers that embed operator-facing labels in messages;
# returns non-zero when the field normalizes to empty.
zxfer_normalize_owned_lock_text_field() {
	l_field_value=$1

	# Field splitting collapses every whitespace run (spaces, tabs, newlines)
	# and trims the ends; set -f keeps glob characters literal. Preserve both
	# caller states exactly because these helpers run in the current shell.
	case $- in
	*f*)
		l_lock_text_restore_glob=0
		;;
	*)
		l_lock_text_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_lock_text_saved_ifs_set=1
		l_lock_text_saved_ifs=$IFS
	else
		l_lock_text_saved_ifs_set=0
		l_lock_text_saved_ifs=""
	fi
	unset IFS
	# shellcheck disable=SC2086
	set -- $l_field_value
	l_normalized_value=$*
	if [ "$l_lock_text_saved_ifs_set" -eq 1 ]; then
		IFS=$l_lock_text_saved_ifs
	else
		unset IFS
	fi
	if [ "$l_lock_text_restore_glob" -eq 1 ]; then
		set +f
	fi
	[ -n "$l_normalized_value" ] || return 1

	printf '%s\n' "$l_normalized_value"
}

# Purpose: Read and normalize one requested process-start field for a PID.
# Usage: Process identity callers use this so headerless and header-form ps
# output are handled consistently across supported platforms.
zxfer_get_process_start_token_for_selector() {
	l_process_token_pid=$1
	l_process_token_field=$2

	case "$l_process_token_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_process_token_field" in
	lstart | stime) ;;
	*) return 1 ;;
	esac

	# Normalize whitespace in pure shell: field-split and rejoin with single
	# spaces (set -f keeps glob characters in ps output literal). FreeBSD ps
	# requires the header form for some multi-column/selector combinations.
	if l_process_token_raw=$(LC_ALL=C ps -p "$l_process_token_pid" \
		-o "$l_process_token_field=" 2>/dev/null); then
		:
	else
		l_process_token_raw=""
	fi
	if [ -z "$l_process_token_raw" ]; then
		if l_process_token_header=$(LC_ALL=C ps -p "$l_process_token_pid" \
			-o "$l_process_token_field" 2>/dev/null); then
			:
		else
			l_process_token_header=""
		fi
		l_process_token_raw=""
		l_process_token_line_number=0
		while IFS= read -r l_process_token_line ||
			[ -n "$l_process_token_line" ]; do
			l_process_token_line_number=$((l_process_token_line_number + 1))
			[ "$l_process_token_line_number" -eq 2 ] || continue
			l_process_token_raw=$l_process_token_line
			break
		done <<-EOF
			$l_process_token_header
		EOF
	fi
	case $- in
	*f*)
		l_process_token_restore_glob=0
		;;
	*)
		l_process_token_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_process_token_saved_ifs_set=1
		l_process_token_saved_ifs=$IFS
	else
		l_process_token_saved_ifs_set=0
		l_process_token_saved_ifs=""
	fi
	unset IFS
	# shellcheck disable=SC2086
	set -- $l_process_token_raw
	l_process_token_argc=$#
	l_process_token_normalized=$*
	if [ "$l_process_token_saved_ifs_set" -eq 1 ]; then
		IFS=$l_process_token_saved_ifs
	else
		unset IFS
	fi
	if [ "$l_process_token_restore_glob" -eq 1 ]; then
		set +f
	fi
	[ "$l_process_token_argc" -gt 0 ] || return 1
	printf '%s:%s\n' \
		"$l_process_token_field" "$l_process_token_normalized"
}

# Purpose: Return a stable start-of-process token for one PID so pid reuse can
# be told apart from the original process.
# Usage: Lock ownership callers omit the selector and prefer lstart with stime
# fallback. Descendant revalidation supplies the selector captured earlier so
# identities are always compared in the same format.
zxfer_get_process_start_token() {
	l_process_start_pid=$1
	l_process_start_selector=${2:-}

	if [ -n "$l_process_start_selector" ]; then
		zxfer_get_process_start_token_for_selector \
			"$l_process_start_pid" "$l_process_start_selector"
		return $?
	fi
	zxfer_get_process_start_token_for_selector \
		"$l_process_start_pid" lstart 2>/dev/null ||
		zxfer_get_process_start_token_for_selector \
			"$l_process_start_pid" stime
}

# Purpose: Return the memoized start token of the current process, capturing
# it with one ps call on first need.
# Usage: Called when creating lock metadata and checking lock ownership so
# repeated lock operations never re-spawn ps for the same answer.
# Side effects: Publishes the token in $g_zxfer_own_process_start_token; module
# call sites read that global after a plain (non-command-substitution) call so
# the memo actually persists in the calling shell.
zxfer_get_own_process_start_token() {
	if [ -n "${g_zxfer_own_process_start_token:-}" ]; then
		printf '%s\n' "$g_zxfer_own_process_start_token"
		return 0
	fi

	l_own_start_token=$(zxfer_get_process_start_token "$$") || return 1
	g_zxfer_own_process_start_token=$l_own_start_token
	printf '%s\n' "$l_own_start_token"
}

# Purpose: Validate that one lock/lease container directory is a private,
# owner-held, mode-0700 real directory before trusting anything inside it.
# Usage: Called before lock metadata is read or written.
zxfer_validate_owned_lock_container_dir() {
	l_dir_path=$1

	[ -d "$l_dir_path" ] || return 1
	[ ! -L "$l_dir_path" ] || return 1
	[ ! -h "$l_dir_path" ] || return 1
	l_effective_uid=$(zxfer_get_effective_user_uid) || return 1
	l_owner_uid=$(zxfer_get_path_owner_uid "$l_dir_path") || return 1
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	l_mode=$(zxfer_get_path_mode_octal "$l_dir_path") || return 1
	[ "$l_mode" = "700" ] || return 1
}

# Purpose: Validate that one lock metadata file is a private, owner-held,
# mode-0600 regular file before parsing it.
# Usage: Called by zxfer_load_owned_lock_metadata_from_dir.
zxfer_validate_owned_lock_metadata_file() {
	l_metadata_path=$1

	[ -f "$l_metadata_path" ] || return 1
	[ ! -L "$l_metadata_path" ] || return 1
	[ ! -h "$l_metadata_path" ] || return 1
	l_effective_uid=$(zxfer_get_effective_user_uid) || return 1
	l_owner_uid=$(zxfer_get_path_owner_uid "$l_metadata_path") || return 1
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	l_mode=$(zxfer_get_path_mode_octal "$l_metadata_path") || return 1
	[ "$l_mode" = "600" ] || return 1
}

# Purpose: Write the owner pid + start-token metadata file into a lock
# directory this process just created.
# Usage: Called by the lock-dir creation helpers after mkdir succeeds.
zxfer_write_owned_lock_metadata_file() {
	l_lock_dir=$1
	l_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_lock_dir")
	l_stage_metadata_path="$l_lock_dir/.metadata.stage"

	# Plain call (no command substitution) so the first ps capture memoizes in
	# this shell instead of a throwaway subshell.
	zxfer_get_own_process_start_token >/dev/null || return 1
	l_start_token=$g_zxfer_own_process_start_token

	# Stage with a fixed name (this process exclusively owns the just-created
	# 0700 lock dir) and publish with one atomic rename so readers only ever
	# see missing or complete metadata, never a partial file. The subshell
	# keeps redirection-open failures as a plain nonzero status.
	if ! (
		umask 077
		printf '%s\npid\t%s\nstart_token\t%s\n' \
			"$ZXFER_LOCK_METADATA_HEADER" "$$" "$l_start_token" \
			>"$l_stage_metadata_path"
	) 2>/dev/null; then
		rm -f "$l_stage_metadata_path" 2>/dev/null || :
		return 1
	fi
	chmod 600 "$l_stage_metadata_path" 2>/dev/null || :
	if ! mv -f "$l_stage_metadata_path" "$l_metadata_path" 2>/dev/null; then
		rm -f "$l_stage_metadata_path" 2>/dev/null || :
		return 1
	fi
	chmod 600 "$l_metadata_path" 2>/dev/null || :
	return 0
}

# Purpose: Parse one pid + start-token lock metadata file into the module
# scratch results.
# Usage: Called by zxfer_load_owned_lock_metadata_from_dir; any deviation from
# the exact three-line format fails so the caller treats the file as corrupt.
zxfer_parse_owned_lock_metadata_file() {
	l_metadata_path=$1
	l_tab=$(printf '\t')
	l_line_number=0

	zxfer_reset_owned_lock_metadata_result

	while IFS= read -r l_line || [ -n "$l_line" ]; do
		l_line_number=$((l_line_number + 1))
		case "$l_line_number" in
		1)
			[ "$l_line" = "$ZXFER_LOCK_METADATA_HEADER" ] || return 1
			;;
		2)
			case "$l_line" in
			"pid$l_tab"*)
				l_value=${l_line#"pid$l_tab"}
				;;
			*)
				return 1
				;;
			esac
			case "$l_value" in
			'' | *[!0-9]*)
				return 1
				;;
			esac
			g_zxfer_owned_lock_pid_result=$l_value
			;;
		3)
			case "$l_line" in
			"start_token$l_tab"*)
				l_value=${l_line#"start_token$l_tab"}
				;;
			*)
				return 1
				;;
			esac
			case "$l_value" in
			'' | *"$l_tab"*)
				return 1
				;;
			esac
			g_zxfer_owned_lock_start_token_result=$l_value
			;;
		*)
			return 1
			;;
		esac
	done <"$l_metadata_path"

	[ "$l_line_number" -eq 3 ] || return 1
	[ -n "$g_zxfer_owned_lock_pid_result" ] || return 1
	[ -n "$g_zxfer_owned_lock_start_token_result" ] || return 1
	return 0
}

# Purpose: Load and validate the owner metadata of one lock directory.
# Usage: Called before liveness, ownership, and reap decisions.
# Return codes:
# 0 = secure directory plus valid metadata loaded
# 1 = hard validation failure
# 2 = corrupt or missing metadata (including pre-V2 metadata formats)
zxfer_load_owned_lock_metadata_from_dir() {
	l_lock_dir=$1
	l_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_lock_dir")

	zxfer_reset_owned_lock_metadata_result

	zxfer_validate_owned_lock_container_dir "$l_lock_dir" || return 1
	if [ ! -e "$l_metadata_path" ]; then
		return 2
	fi
	zxfer_validate_owned_lock_metadata_file "$l_metadata_path" || return 1
	zxfer_parse_owned_lock_metadata_file "$l_metadata_path" || return 2
	return 0
}

# Purpose: Decide whether the recorded lock owner is still the same live
# process (pid alive AND start token unchanged).
# Usage: Called before stale locks are reaped.
# Return codes:
# 0 = owner is still live
# 1 = owner is stale
# 2 = owner liveness could not be determined safely
zxfer_owned_lock_owner_is_live() {
	l_pid=$1
	l_start_token=$2

	kill -s 0 "$l_pid" 2>/dev/null || return 1
	l_current_start_token=$(zxfer_get_process_start_token "$l_pid") || return 2
	if [ "$l_current_start_token" = "$l_start_token" ]; then
		return 0
	fi
	return 1
}

# Purpose: Verify that a lock container contains only the two bounded entries
# zxfer may create.
# Usage: Called immediately before cleanup so unknown children make the
# operation fail closed without deleting anything.
zxfer_owned_lock_dir_has_known_layout() {
	l_layout_lock_dir=$1
	l_layout_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_layout_lock_dir")
	l_layout_stage_path="$l_layout_lock_dir/.metadata.stage"
	l_layout_status=0

	case $- in
	*f*)
		l_layout_restore_glob=1
		set +f
		;;
	*)
		l_layout_restore_glob=0
		;;
	esac
	set -- \
		"$l_layout_lock_dir"/* \
		"$l_layout_lock_dir"/.[!.]* \
		"$l_layout_lock_dir"/..?*
	if [ "$l_layout_restore_glob" -eq 1 ]; then
		set -f
	fi

	for l_layout_entry in "$@"; do
		if [ ! -e "$l_layout_entry" ] &&
			[ ! -L "$l_layout_entry" ] && [ ! -h "$l_layout_entry" ]; then
			continue
		fi
		case "$l_layout_entry" in
		"$l_layout_metadata_path" | "$l_layout_stage_path")
			if [ -d "$l_layout_entry" ] &&
				[ ! -L "$l_layout_entry" ] && [ ! -h "$l_layout_entry" ]; then
				l_layout_status=1
				break
			fi
			;;
		*)
			l_layout_status=1
			break
			;;
		esac
	done

	return "$l_layout_status"
}

# Purpose: Require one lock directory to retain the exact owner metadata that
# the release or stale-reap decision already validated.
# Usage: Called immediately before bounded cleanup and again after pathname
# revalidation so an ownership change makes cleanup fail closed.
zxfer_owned_lock_metadata_matches() {
	l_match_lock_dir=$1
	l_match_expected_pid=$2
	l_match_expected_start_token=$3

	[ -n "$l_match_expected_pid" ] || return 1
	[ -n "$l_match_expected_start_token" ] || return 1
	zxfer_load_owned_lock_metadata_from_dir "$l_match_lock_dir" || return 1
	[ "$g_zxfer_owned_lock_pid_result" = "$l_match_expected_pid" ] || return 1
	[ "$g_zxfer_owned_lock_start_token_result" = "$l_match_expected_start_token" ]
}

# Purpose: Unlink only the two fixed leaf names that zxfer may create in one
# already-validated owned lock directory, then remove the empty container.
# Usage: Called only after layout and ownership revalidation have succeeded.
zxfer_remove_known_owned_lock_entries() {
	l_remove_lock_dir=$1
	l_remove_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_remove_lock_dir")
	l_remove_stage_metadata_path="$l_remove_lock_dir/.metadata.stage"

	for l_remove_entry in "$l_remove_metadata_path" "$l_remove_stage_metadata_path"; do
		if [ -e "$l_remove_entry" ] || [ -L "$l_remove_entry" ] || [ -h "$l_remove_entry" ]; then
			rm -f "$l_remove_entry" 2>/dev/null || return 1
		fi
	done
	if rmdir "$l_remove_lock_dir" 2>/dev/null; then
		return 0
	fi
	[ ! -e "$l_remove_lock_dir" ] &&
		[ ! -L "$l_remove_lock_dir" ] && [ ! -h "$l_remove_lock_dir" ]
}

# Purpose: Remove one validated lock directory using bounded unlink + rmdir,
# never generic recursive deletion.
# Usage: Called from release and reap flows. Optional expected pid/start-token
# arguments require the same validated metadata to still own the path at the
# cleanup boundary; corrupt-metadata reaps omit them.
zxfer_cleanup_owned_lock_dir() {
	l_cleanup_lock_dir=$1
	l_cleanup_expected_pid=${2:-}
	l_cleanup_expected_start_token=${3:-}

	[ -n "$l_cleanup_lock_dir" ] || return 0
	if [ ! -e "$l_cleanup_lock_dir" ] && [ ! -L "$l_cleanup_lock_dir" ] && [ ! -h "$l_cleanup_lock_dir" ]; then
		return 0
	fi
	zxfer_validate_owned_lock_container_dir "$l_cleanup_lock_dir" || return 1
	if [ -n "$l_cleanup_expected_pid" ] || [ -n "$l_cleanup_expected_start_token" ]; then
		zxfer_owned_lock_metadata_matches \
			"$l_cleanup_lock_dir" "$l_cleanup_expected_pid" \
			"$l_cleanup_expected_start_token" || return 1
	fi
	zxfer_owned_lock_dir_has_known_layout "$l_cleanup_lock_dir" || return 1
	# Revalidate after layout inspection. Portable POSIX shell has no dirfd-based
	# removal primitive, but bounded names plus rmdir keep any same-UID pathname
	# race from widening into recursive deletion.
	zxfer_validate_owned_lock_container_dir "$l_cleanup_lock_dir" || return 1
	if [ -n "$l_cleanup_expected_pid" ]; then
		zxfer_owned_lock_metadata_matches \
			"$l_cleanup_lock_dir" "$l_cleanup_expected_pid" \
			"$l_cleanup_expected_start_token" || return 1
	fi

	zxfer_remove_known_owned_lock_entries "$l_cleanup_lock_dir"
}

# Purpose: Acquire one lock by creating the exact directory path with owner
# pid + start-token metadata; mkdir is the atomic acquisition step.
# Usage: zxfer_create_owned_lock_dir <dir> [kind] [purpose] -- the trailing
# labels are accepted for caller compatibility and ignored.
zxfer_create_owned_lock_dir() {
	l_lock_dir=$1

	[ -n "$l_lock_dir" ] || return 1
	mkdir -m 700 "$l_lock_dir" 2>/dev/null || return 1

	if ! zxfer_validate_owned_lock_container_dir "$l_lock_dir"; then
		zxfer_cleanup_owned_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi
	if ! zxfer_write_owned_lock_metadata_file "$l_lock_dir"; then
		zxfer_cleanup_owned_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi

	printf '%s\n' "$l_lock_dir"
	return 0
}

# Purpose: Reap one lock directory when its owner is provably stale, or when
# its metadata is corrupt/missing and the caller policy allows corrupt reaps.
# Usage: zxfer_try_reap_stale_owned_lock_dir <dir> [allow-corrupt] [kind]
# [purpose] -- the trailing labels are accepted for caller compatibility and
# ignored.
# Return codes:
# 0 = stale or corrupt entry was reaped
# 1 = hard failure
# 2 = entry is still busy or not yet reapable under the caller policy
zxfer_try_reap_stale_owned_lock_dir() {
	l_lock_dir=$1
	l_allow_corrupt_reap=${2:-0}
	l_reap_owner_pid=""
	l_reap_owner_start_token=""

	zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir"
	l_load_status=$?
	case "$l_load_status" in
	0)
		l_reap_owner_pid=$g_zxfer_owned_lock_pid_result
		l_reap_owner_start_token=$g_zxfer_owned_lock_start_token_result
		zxfer_owned_lock_owner_is_live \
			"$l_reap_owner_pid" \
			"$l_reap_owner_start_token"
		l_live_status=$?
		if [ "$l_live_status" -eq 0 ]; then
			return 2
		fi
		if [ "$l_live_status" -eq 2 ]; then
			return 1
		fi
		;;
	1)
		return 1
		;;
	2)
		case "$l_allow_corrupt_reap" in
		1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn])
			:
			;;
		*)
			return 2
			;;
		esac
		;;
	*)
		return 1
		;;
	esac

	zxfer_cleanup_owned_lock_dir \
		"$l_lock_dir" "$l_reap_owner_pid" "$l_reap_owner_start_token" || return 1
	return 0
}

# Purpose: Check whether the current process is the recorded owner of one lock
# directory (pid match AND start-token match).
# Usage: Called by the checked release path so only the owner ever releases.
zxfer_current_process_owns_owned_lock_dir() {
	l_lock_dir=$1

	zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir" || return 1
	[ "$g_zxfer_owned_lock_pid_result" = "$$" ] || return 1
	# Plain call (no command substitution) so the first ps capture memoizes in
	# this shell instead of a throwaway subshell.
	zxfer_get_own_process_start_token >/dev/null || return 1
	[ "$g_zxfer_owned_lock_start_token_result" = "$g_zxfer_own_process_start_token" ]
}

# Purpose: Release one owned lock directory with a checked owner match so this
# process never deletes a lock held by a live sibling.
# Usage: zxfer_release_owned_lock_dir <dir> [kind] [purpose] -- the trailing
# labels are accepted for caller compatibility and ignored.
zxfer_release_owned_lock_dir() {
	l_lock_dir=$1

	[ -n "$l_lock_dir" ] || return 0
	if [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; then
		return 0
	fi
	zxfer_current_process_owns_owned_lock_dir "$l_lock_dir" || return 1
	zxfer_cleanup_owned_lock_dir \
		"$l_lock_dir" "$$" "$g_zxfer_own_process_start_token" || return 1
	return 0
}
