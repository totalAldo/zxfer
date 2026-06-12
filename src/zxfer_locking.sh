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
# owns globals: owned-lock metadata scratch, the memoized own-process start
#   token, and run-owned long-lived lock/lease cleanup registrations.
# reads globals: none directly, but later runtime helpers may call into the
#   registration helpers.
# mutates caches: local lock and lease directories.
# returns via stdout: normalized process-start tokens, metadata paths, and
#   created lock/lease paths.
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
	g_zxfer_owned_lock_cleanup_paths=""
	g_zxfer_own_process_start_token=""
	zxfer_reset_owned_lock_metadata_result
}

# Purpose: Register the long-lived owned lock or lease path for trap cleanup.
# Usage: Called after acquiring a lock/lease that must not outlive this run.
zxfer_register_owned_lock_path() {
	l_lock_path=$1

	[ -n "$l_lock_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_lock_path" ] && return 0
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	if [ -n "${g_zxfer_owned_lock_cleanup_paths:-}" ]; then
		g_zxfer_owned_lock_cleanup_paths=$g_zxfer_owned_lock_cleanup_paths'
'$l_lock_path
	else
		g_zxfer_owned_lock_cleanup_paths=$l_lock_path
	fi
}

# Purpose: Remove the owned lock or lease path from the trap-cleanup tracking.
# Usage: Called after a registered lock/lease has been released.
zxfer_unregister_owned_lock_path() {
	l_lock_path=$1
	l_remaining_paths=""

	[ -n "$l_lock_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_lock_path" ] && continue
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_existing_path
		else
			l_remaining_paths=$l_existing_path
		fi
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	g_zxfer_owned_lock_cleanup_paths=$l_remaining_paths
}

# Purpose: Normalize one registered lock path to its physical parent spelling
# so overlap checks compare like with like.
# Usage: Called by zxfer_owned_lock_cleanup_conflicts_with_path.
zxfer_normalize_owned_lock_cleanup_path() {
	l_lock_path=$1

	[ -n "$l_lock_path" ] || return 1
	case "$l_lock_path" in
	/*) ;;
	*)
		printf '%s\n' "$l_lock_path"
		return 0
		;;
	esac

	if ! l_parent_dir=$(zxfer_get_path_parent_dir "$l_lock_path"); then
		return 1
	fi
	if [ "$l_parent_dir" = "/" ]; then
		l_physical_parent=/
	else
		if ! l_physical_parent=$(CDPATH='' cd -P "$l_parent_dir" 2>/dev/null && pwd); then
			return 1
		fi
	fi
	l_lock_name=${l_lock_path##*/}
	if [ "$l_physical_parent" = "/" ]; then
		printf '/%s\n' "$l_lock_name"
		return 0
	fi
	printf '%s/%s\n' "$l_physical_parent" "$l_lock_name"
}

# Purpose: Check whether one cleanup candidate path overlaps a registered
# owned lock or lease path that must be preserved for checked release.
# Usage: Called by the remote-host cache-root teardown before it removes a
# cache directory tree.
zxfer_owned_lock_cleanup_conflicts_with_path() {
	l_cleanup_path=$1

	[ -n "$l_cleanup_path" ] || return 1
	if ! l_cleanup_path=$(zxfer_normalize_owned_lock_cleanup_path "$l_cleanup_path"); then
		return 1
	fi

	while IFS= read -r l_lock_path || [ -n "$l_lock_path" ]; do
		[ -n "$l_lock_path" ] || continue
		if ! l_lock_path=$(zxfer_normalize_owned_lock_cleanup_path "$l_lock_path"); then
			continue
		fi
		case "$l_cleanup_path" in
		"$l_lock_path" | "$l_lock_path"/*)
			return 0
			;;
		esac
		case "$l_lock_path" in
		"$l_cleanup_path" | "$l_cleanup_path"/*)
			return 0
			;;
		esac
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	return 1
}

# Purpose: Warn the operator when a registered owned lock cannot be released
# during cleanup.
# Usage: Called from zxfer_release_registered_owned_locks failure handling.
zxfer_warn_owned_lock_cleanup_failure() {
	l_lock_path=$1
	l_status=$2

	if command -v zxfer_warn_stderr >/dev/null 2>&1; then
		zxfer_warn_stderr "zxfer: warning: unable to release owned lock or lease \"$l_lock_path\" during cleanup (status $l_status)."
	else
		printf '%s\n' "zxfer: warning: unable to release owned lock or lease \"$l_lock_path\" during cleanup (status $l_status)." >&2
	fi
}

# Purpose: Release every still-registered owned lock or lease during shutdown.
# Usage: Called from zxfer_trap_exit; failures warn and keep the path
# registered for later inspection instead of failing the run.
zxfer_release_registered_owned_locks() {
	l_remaining_paths=""
	l_cleanup_status=0

	while IFS= read -r l_lock_path || [ -n "$l_lock_path" ]; do
		[ -n "$l_lock_path" ] || continue
		zxfer_release_owned_lock_dir "$l_lock_path"
		l_release_status=$?
		if [ "$l_release_status" -eq 0 ]; then
			continue
		fi
		zxfer_warn_owned_lock_cleanup_failure "$l_lock_path" "$l_release_status"
		l_cleanup_status=1
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_lock_path
		else
			l_remaining_paths=$l_lock_path
		fi
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	g_zxfer_owned_lock_cleanup_paths=$l_remaining_paths
	return "$l_cleanup_status"
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
	# and trims the ends; set -f keeps glob characters literal.
	set -f
	# shellcheck disable=SC2086
	set -- $l_field_value
	set +f
	l_normalized_value=$*
	[ -n "$l_normalized_value" ] || return 1

	printf '%s\n' "$l_normalized_value"
}

# Purpose: Return a stable start-of-process token for one PID so pid reuse can
# be told apart from the original lock owner.
# Usage: Called when writing lock metadata and when checking owner liveness.
# One ps call covers the supported platforms; stime is the fallback selector
# for ps implementations without the long-form lstart column, and the selector
# name is embedded so tokens from different selectors never compare equal.
zxfer_get_process_start_token() {
	l_pid=$1

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# Normalize whitespace in pure shell: field-split and rejoin with single
	# spaces (set -f keeps glob characters in ps output literal).
	l_token_field=lstart
	l_raw_token=$(LC_ALL=C ps -p "$l_pid" -o lstart= 2>/dev/null || :)
	set -f
	# shellcheck disable=SC2086
	set -- $l_raw_token
	set +f
	if [ "$#" -eq 0 ]; then
		l_token_field=stime
		l_raw_token=$(LC_ALL=C ps -p "$l_pid" -o stime= 2>/dev/null || :)
		set -f
		# shellcheck disable=SC2086
		set -- $l_raw_token
		set +f
	fi
	[ "$#" -gt 0 ] || return 1
	printf '%s:%s\n' "$l_token_field" "$*"
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

	if ! l_own_start_token=$(zxfer_get_process_start_token "$$"); then
		return 1
	fi
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
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_dir_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_dir_path"); then
		return 1
	fi
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
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_metadata_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_metadata_path"); then
		return 1
	fi
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
	if ! zxfer_get_own_process_start_token >/dev/null; then
		return 1
	fi
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

	if ! zxfer_validate_owned_lock_container_dir "$l_lock_dir"; then
		return 1
	fi
	if [ ! -e "$l_metadata_path" ]; then
		return 2
	fi
	if ! zxfer_validate_owned_lock_metadata_file "$l_metadata_path"; then
		return 1
	fi
	if ! zxfer_parse_owned_lock_metadata_file "$l_metadata_path"; then
		return 2
	fi
	return 0
}

# Purpose: Compatibility wrapper retained for consumers that still pass
# kind/purpose labels; lock identity is owner pid + start token only, so the
# labels are accepted and ignored.
# Usage: zxfer_load_owned_lock_metadata_for_kind_and_purpose <dir> [kind] [purpose]
# Returns: the zxfer_load_owned_lock_metadata_from_dir status codes.
zxfer_load_owned_lock_metadata_for_kind_and_purpose() {
	zxfer_load_owned_lock_metadata_from_dir "$1"
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

	if ! kill -s 0 "$l_pid" 2>/dev/null; then
		return 1
	fi
	if ! l_current_start_token=$(zxfer_get_process_start_token "$l_pid"); then
		return 2
	fi
	if [ "$l_current_start_token" = "$l_start_token" ]; then
		return 0
	fi
	return 1
}

# Purpose: Remove one owned lock directory after the caller has proven it is
# safe to delete (owned, stale, or corrupt per policy).
# Usage: Called from release and reap flows; symlinked paths fail closed.
zxfer_cleanup_owned_lock_dir() {
	l_lock_dir=$1

	[ -n "$l_lock_dir" ] || return 0
	if [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; then
		return 0
	fi
	[ ! -L "$l_lock_dir" ] || return 1
	[ ! -h "$l_lock_dir" ] || return 1
	[ -d "$l_lock_dir" ] || return 1
	if rm -rf "$l_lock_dir" 2>/dev/null ||
		{ [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; }; then
		return 0
	fi
	return 1
}

# Purpose: Acquire one lock by creating the exact directory path with owner
# pid + start-token metadata; mkdir is the atomic acquisition step.
# Usage: zxfer_create_owned_lock_dir <dir> [kind] [purpose] -- the trailing
# labels are accepted for caller compatibility and ignored.
zxfer_create_owned_lock_dir() {
	l_lock_dir=$1

	[ -n "$l_lock_dir" ] || return 1
	if ! mkdir -m 700 "$l_lock_dir" 2>/dev/null; then
		return 1
	fi

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

# Purpose: Acquire one uniquely named lease under a validated private parent
# directory with owner pid + start-token metadata.
# Usage: zxfer_create_owned_lock_dir_in_parent <parent> <prefix> [kind]
# [purpose] -- the trailing labels are accepted for caller compatibility and
# ignored. The entry name embeds this PID, so live sibling processes can never
# collide; a taken attempt is either an earlier live lease of this process or
# debris from a dead PID reuse, so creation steps to the next slot.
zxfer_create_owned_lock_dir_in_parent() {
	l_parent_dir=$1
	l_prefix=$2

	[ -n "$l_prefix" ] || return 1
	if ! zxfer_validate_owned_lock_container_dir "$l_parent_dir"; then
		return 1
	fi

	l_attempt=0
	while [ "$l_attempt" -lt 8 ]; do
		l_attempt=$((l_attempt + 1))
		l_lock_dir="$l_parent_dir/$l_prefix.$$.$l_attempt"
		if ! mkdir -m 700 "$l_lock_dir" 2>/dev/null; then
			if [ -e "$l_lock_dir" ] || [ -L "$l_lock_dir" ]; then
				continue
			fi
			return 1
		fi
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
	done

	return 1
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

	zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir"
	l_load_status=$?
	case "$l_load_status" in
	0)
		zxfer_owned_lock_owner_is_live \
			"$g_zxfer_owned_lock_pid_result" \
			"$g_zxfer_owned_lock_start_token_result"
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

	if ! zxfer_cleanup_owned_lock_dir "$l_lock_dir"; then
		return 1
	fi
	return 0
}

# Purpose: Check whether the current process is the recorded owner of one lock
# directory (pid match AND start-token match).
# Usage: Called by the checked release path so only the owner ever releases.
zxfer_current_process_owns_owned_lock_dir() {
	l_lock_dir=$1

	if ! zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir"; then
		return 1
	fi
	[ "$g_zxfer_owned_lock_pid_result" = "$$" ] || return 1
	# Plain call (no command substitution) so the first ps capture memoizes in
	# this shell instead of a throwaway subshell.
	if ! zxfer_get_own_process_start_token >/dev/null; then
		return 1
	fi
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
		zxfer_unregister_owned_lock_path "$l_lock_dir"
		return 0
	fi
	if ! zxfer_current_process_owns_owned_lock_dir "$l_lock_dir"; then
		return 1
	fi
	if ! zxfer_cleanup_owned_lock_dir "$l_lock_dir"; then
		return 1
	fi
	zxfer_unregister_owned_lock_path "$l_lock_dir"
	return 0
}
