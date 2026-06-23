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
# RUNTIME STATE / TEMP FILES / CLEANUP
################################################################################

# Module contract:
# owns globals: per-run option/default state, temp-root selection, runtime-artifact allocation/readback/cleanup state, cleanup PID state, transport/bootstrap defaults, reporting/profile session state, secure-staging result scratch, and owned-lock metadata scratch (sections below).
# reads globals: TMPDIR, ZXFER_BACKUP_DIR, g_option_* cleanup flags, and resolved helper paths.
# mutates caches: reporting, destination-existence, property, and snapshot-index state through reset helpers; local lock directories.
# returns via stdout: temp-file/temp-dir paths, source-to-destination dataset mappings, OS detection results, owner/mode/symlink probes, and process-start tokens.
#
# The SECURE PATH / OWNER / MODE HELPERS and OWNED LOCK / LEASE COORDINATION
# sections below were merged verbatim from src/zxfer_path_security.sh and
# src/zxfer_locking.sh (Phase 8). All of their consumers (reporting's
# ZXFER_ERROR_LOG lock, backup-metadata path checks, remote-host staging, and
# this module's temp/staging helpers) call them at call time only, so the
# definitions are source-order safe anywhere before main() runs.

ZXFER_MAX_YIELD_ITERATIONS=8
ZXFER_CACHE_OBJECT_HEADER_LINE="ZXFER_CACHE_OBJECT_V1"
ZXFER_CACHE_OBJECT_END_LINE="ZXFER_CACHE_OBJECT_END"

################################################################################
# SECURE PATH / OWNER / MODE HELPERS
################################################################################

# Section contract:
# owns globals: secure-staging result scratch.
# reads globals: none.
# mutates caches: none.
# returns via stdout: owner/mode probes, symlink probes, and validated temp-root paths.
# Temp roots validate once per run via the single-pass physical resolution in
# zxfer_validate_temp_root_candidate; the component-walk symlink scanner only
# serves the cold backup-metadata and ZXFER_ERROR_LOG checks whose
# "path component ... is a symlink" errors are pinned public output.

# Purpose: Return the path owner UID in the form expected by later helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_path_owner_uid() {
	l_path=$1

	if [ ! -e "$l_path" ]; then
		return 1
	fi

	if command -v stat >/dev/null 2>&1; then
		if l_uid=$(stat -c '%u' "$l_path" 2>/dev/null); then
			case "$l_uid" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_uid"
				return 0
				;;
			esac
		fi
		if l_uid=$(stat -f '%u' "$l_path" 2>/dev/null); then
			case "$l_uid" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_uid"
				return 0
				;;
			esac
		fi
	fi

	l_ls_path=$l_path
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	if l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null); then
		# Field-split the ls -ldn line in pure shell; field 3 is the owner UID.
		set -f
		# shellcheck disable=SC2086
		set -- $l_ls_output
		set +f
		if [ "$#" -ge 3 ] && [ "$3" != "" ]; then
			printf '%s\n' "$3"
			return 0
		fi
	fi

	return 1
}

# Purpose: Return the path mode octal in the form expected by later helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_path_mode_octal() {
	l_path=$1

	if [ ! -e "$l_path" ]; then
		return 1
	fi

	if command -v stat >/dev/null 2>&1; then
		if l_mode=$(stat -c '%a' "$l_path" 2>/dev/null); then
			case "$l_mode" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_mode"
				return 0
				;;
			esac
		fi
		if l_mode=$(stat -f '%OLp' "$l_path" 2>/dev/null); then
			case "$l_mode" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_mode"
				return 0
				;;
			esac
		fi
	fi

	l_ls_path=$l_path
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	if l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null); then
		l_perm_str=${l_ls_output%% *}
		if [ "$l_perm_str" = "-rw-------" ]; then
			printf '600\n'
			return 0
		fi
	fi

	return 1
}

# Purpose: Check that one ls -l permission string describes a directory that is
# safe to share: well-formed and either free of group/other write bits or
# protected by the sticky bit.
# Usage: Called by the temp-root and trusted-symlink validators in place of the
# old '| cut -c N' subshell chains; pure parameter expansion, no spawns.
zxfer_validate_shared_dir_permission_string() {
	l_perm_str=$1

	case "$l_perm_str" in
	??????????*) ;;
	*)
		return 1
		;;
	esac
	# Single-character slices via parameter expansion: strip N leading
	# characters, then keep only the first character of the remainder.
	l_perm_tail=${l_perm_str#?????}
	l_group_write=${l_perm_tail%"${l_perm_tail#?}"}
	l_perm_tail=${l_perm_str#????????}
	l_other_write=${l_perm_tail%"${l_perm_tail#?}"}
	l_perm_tail=${l_perm_str#?????????}
	l_sticky_char=${l_perm_tail%"${l_perm_tail#?}"}
	case "$l_group_write$l_other_write" in
	*w*)
		case "$l_sticky_char" in
		t | T) ;;
		*)
			return 1
			;;
		esac
		;;
	esac

	return 0
}

# Purpose: Return the effective user UID in the form expected by later helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_effective_user_uid() {
	if command -v id >/dev/null 2>&1; then
		if l_uid=$(id -u 2>/dev/null); then
			printf '%s\n' "$l_uid"
			return 0
		fi
	fi
	return 1
}

# Purpose: Check whether the backup owner UID is allowed.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers need a boolean answer about the backup owner UID.
zxfer_backup_owner_uid_is_allowed() {
	l_owner_uid=$1

	if [ "$l_owner_uid" = "0" ]; then
		return 0
	fi

	if l_effective_uid=$(zxfer_get_effective_user_uid); then
		if [ "$l_owner_uid" = "$l_effective_uid" ]; then
			return 0
		fi
	fi

	return 1
}

# Purpose: Describe the expected backup owner in operator-facing text.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# validation or reporting logic needs one canonical explanation string.
zxfer_describe_expected_backup_owner() {
	l_desc="root (UID 0)"

	if l_effective_uid=$(zxfer_get_effective_user_uid); then
		if [ "$l_effective_uid" != "0" ]; then
			l_desc="$l_desc or UID $l_effective_uid"
		fi
	fi

	printf '%s\n' "$l_desc"
}

# Purpose: Reject the backup metadata path with the validation failure owned by
# this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when a
# path or input should fail closed with one consistent error path.
zxfer_reject_backup_metadata_path() {
	l_msg=$1

	printf '%s\n' "$l_msg" >&2
	return 1
}

# Purpose: Require the backup metadata path without symlinks before the
# surrounding flow continues.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers should stop immediately if the precondition is not met.
zxfer_require_backup_metadata_path_without_symlinks() {
	l_path=$1

	if l_symlink_component=$(zxfer_find_symlink_path_component "$l_path"); then
		if [ "$l_symlink_component" = "$l_path" ]; then
			zxfer_reject_backup_metadata_path "Refusing to use backup metadata $l_path because it is a symlink."
		fi
		zxfer_reject_backup_metadata_path "Refusing to use backup metadata $l_path because path component $l_symlink_component is a symlink."
	fi
}

# Purpose: Find the symlink path component in the tracked state owned by this
# module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers need an existing record instead of rebuilding one.
zxfer_find_symlink_path_component() {
	l_path=$1

	[ -n "$l_path" ] || return 1

	l_remaining=$l_path
	l_candidate_path=""
	while [ -n "$l_remaining" ]; do
		case "$l_remaining" in
		/*)
			if [ "$l_candidate_path" = "" ]; then
				l_candidate_path="/"
				l_remaining=${l_remaining#/}
				continue
			fi
			;;
		esac

		l_component=${l_remaining%%/*}
		if [ "$l_component" = "$l_remaining" ]; then
			l_remaining=""
		else
			l_remaining=${l_remaining#*/}
		fi
		[ -n "$l_component" ] || continue

		case "$l_candidate_path" in
		"")
			l_candidate_path=$l_component
			;;
		/)
			l_candidate_path="/$l_component"
			;;
		*)
			l_candidate_path="$l_candidate_path/$l_component"
			;;
		esac

		if [ -L "$l_candidate_path" ] || [ -h "$l_candidate_path" ]; then
			if zxfer_is_trusted_symlink_path_component "$l_candidate_path"; then
				continue
			fi
			printf '%s\n' "$l_candidate_path"
			return 0
		fi
	done

	return 1
}

# Purpose: Check whether the symlink path component is trusted.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers need a boolean answer about a validated or trusted state.
zxfer_is_trusted_symlink_path_component() {
	l_path=$1

	case "$l_path" in
	/*) ;;
	*)
		return 1
		;;
	esac
	[ -L "$l_path" ] || [ -h "$l_path" ] || return 1

	l_owner_uid=$(zxfer_get_path_owner_uid "$l_path" 2>/dev/null) || return 1
	[ "$l_owner_uid" = "0" ] || return 1

	case "$l_path" in
	*/*)
		l_parent=${l_path%/*}
		[ -n "$l_parent" ] || l_parent="/"
		;;
	*)
		return 1
		;;
	esac
	l_parent_owner_uid=$(zxfer_get_path_owner_uid "$l_parent" 2>/dev/null) || return 1
	[ "$l_parent_owner_uid" = "0" ] || return 1
	[ "$l_parent" = "/" ] || return 1

	l_ls_path=$l_parent
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null) || return 1
	zxfer_validate_shared_dir_permission_string "${l_ls_output%% *}"
}

# Purpose: Validate the temp root candidate before zxfer relies on it.
# Usage: Called once per requested temp root (the result is memoized by
# zxfer_try_get_effective_tmpdir) before zxfer trusts temp-root or
# backup-metadata parents; fails closed on malformed, unsafe, or stale input.
# The single-pass 'cd -P && pwd' resolution below replaces the old
# per-component symlink walk: an unsafe symlink target fails the owner/mode
# checks on the resolved physical directory.
zxfer_validate_temp_root_candidate() {
	l_candidate=$1

	[ -n "$l_candidate" ] || return 1
	case "$l_candidate" in
	/*) ;;
	*)
		return 1
		;;
	esac

	l_physical_dir=$(CDPATH='' cd -P "$l_candidate" 2>/dev/null && pwd) || return 1
	case "$l_physical_dir" in
	/*) ;;
	*)
		return 1
		;;
	esac
	[ -d "$l_physical_dir" ] || return 1

	l_owner_uid=$(zxfer_get_path_owner_uid "$l_physical_dir") || return 1
	if [ "$l_owner_uid" != "0" ]; then
		l_effective_uid=$(zxfer_get_effective_user_uid) || return 1
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	fi
	l_ls_path=$l_physical_dir
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null) || return 1
	zxfer_validate_shared_dir_permission_string "${l_ls_output%% *}" || return 1

	printf '%s\n' "$l_physical_dir"
}

# Purpose: Return the path parent directory in the form expected by later
# helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_path_parent_dir() {
	l_path=$1

	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ] || [ "$l_parent" = "" ]; then
		l_parent=/
	fi

	printf '%s\n' "$l_parent"
}

# Purpose: Create one unpredictably named staging entry (file or directory)
# from a randomized temp-name template under a caller-validated parent.
# Usage: zxfer_create_unpredictable_staging_entry <template> <file|dir>.
# Validated staging parents may still be shared sticky directories (a
# /tmp-style ZXFER_ERROR_LOG parent), where predictable pid+attempt slot names
# would let a local process-table reader pre-create every slot and deny
# staging; the randomized names close that squat window and the forced 077
# umask keeps entries 0600 (files) or 0700 (directories).
zxfer_create_unpredictable_staging_entry() {
	l_template=$1
	l_entry_kind=$2

	l_dir_flag=""
	if [ "$l_entry_kind" = "dir" ]; then
		l_dir_flag="-d"
	fi
	# l_dir_flag intentionally expands unquoted: empty means no extra word.
	# shellcheck disable=SC2086
	(umask 077 && exec mktemp $l_dir_flag "$l_template") 2>/dev/null
}

# Purpose: Create the secure staging directory for path using the safety checks
# owned by this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_secure_staging_dir_for_path() {
	l_path=$1
	l_prefix=${2:-zxfer.stage}

	g_zxfer_secure_staging_dir_result=""
	l_parent=$(zxfer_get_path_parent_dir "$l_path") || return 1
	l_parent=$(zxfer_validate_temp_root_candidate "$l_parent") || return 1

	l_stage_dir=$(zxfer_create_unpredictable_staging_entry "$l_parent/.$l_prefix.XXXXXX" dir) || return 1
	# Register same-directory staging so trap cleanup reaps it on aborts.
	if command -v zxfer_register_runtime_artifact_path >/dev/null 2>&1; then
		zxfer_register_runtime_artifact_path "$l_stage_dir"
	fi
	g_zxfer_secure_staging_dir_result=$l_stage_dir
	printf '%s\n' "$l_stage_dir"
	return 0
}

# Purpose: Check the secure backup file using the fail-closed rules owned by
# this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths before
# later helpers act on a result that must be validated first.
zxfer_check_secure_backup_file() {
	l_check_path=$1
	l_check_display_path=${2:-$l_check_path}

	if ! l_check_owner_uid=$(zxfer_get_path_owner_uid "$l_check_path"); then
		printf '%s\n' "Cannot determine the owner of backup metadata $l_check_display_path."
		return 1
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_check_owner_uid"; then
		l_check_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		printf '%s\n' "Refusing to use backup metadata $l_check_display_path because it is owned by UID $l_check_owner_uid instead of $l_check_expected_owner_desc."
		return 1
	fi
	if ! l_check_mode=$(zxfer_get_path_mode_octal "$l_check_path"); then
		printf '%s\n' "Cannot determine the permissions for backup metadata $l_check_display_path."
		return 1
	fi
	if [ "$l_check_mode" != "600" ]; then
		printf '%s\n' "Refusing to use backup metadata $l_check_display_path because its permissions ($l_check_mode) are not 0600."
		return 1
	fi
}

################################################################################
# OWNED LOCK / LEASE COORDINATION
################################################################################

# Section contract:
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

	zxfer_cleanup_owned_lock_dir "$l_lock_dir" || return 1
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
	zxfer_cleanup_owned_lock_dir "$l_lock_dir" || return 1
	return 0
}

# Purpose: Refresh the backup storage root from the current configuration and
# runtime state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after
# inputs change and downstream helpers need the derived value rebuilt.
zxfer_refresh_backup_storage_root() {
	if [ -n "${ZXFER_BACKUP_DIR:-}" ]; then
		l_backup_storage_root=$ZXFER_BACKUP_DIR
	elif [ -n "${g_backup_storage_root:-}" ]; then
		l_backup_storage_root=$g_backup_storage_root
	elif [ -z "${g_backup_storage_root:-}" ]; then
		l_backup_storage_root=/var/db/zxfer
	fi

	case "$l_backup_storage_root" in
	/*)
		g_backup_storage_root=$l_backup_storage_root
		;;
	*)
		zxfer_throw_error "Refusing to use backup metadata root \"$l_backup_storage_root\" because ZXFER_BACKUP_DIR must be an absolute path."
		;;
	esac
}

# Purpose: Return the destination snapshot root dataset in the form expected by
# later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_destination_snapshot_root_dataset() {
	l_source_dataset_tail=${g_initial_source##*/}

	if [ "${g_initial_source_had_trailing_slash:-0}" -eq 1 ]; then
		printf '%s\n' "$g_destination"
	else
		printf '%s\n' "$g_destination/$l_source_dataset_tail"
	fi
}

# Purpose: Return the destination dataset for source dataset in the form
# expected by later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_destination_dataset_for_source_dataset() {
	l_source_dataset=$1
	l_destination_root_dataset=$(zxfer_get_destination_snapshot_root_dataset)

	case "$l_source_dataset" in
	"$g_initial_source")
		printf '%s\n' "$l_destination_root_dataset"
		;;
	"$g_initial_source"/*)
		printf '%s\n' "$l_destination_root_dataset${l_source_dataset#"$g_initial_source"}"
		;;
	*)
		printf '%s\n' "$l_destination_root_dataset"
		;;
	esac
}

zxfer_get_cleanup_child_wrapper_script_path() {
	l_cleanup_child_wrapper_script="${ZXFER_SOURCE_MODULES_ROOT:-.}/src/zxfer_cleanup_child_wrapper.sh"
	[ -r "$l_cleanup_child_wrapper_script" ] || return 1
	printf '%s\n' "$l_cleanup_child_wrapper_script"
}

# Purpose: Reset the cleanup-helper tracking state so the next runtime pass
# starts from a clean state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_cleanup_pid_tracking() {
	g_zxfer_cleanup_pids=""
	g_zxfer_cleanup_pid_records=""
	g_zxfer_cleanup_pid_record_purpose=""
	g_zxfer_cleanup_pid_abort_failure_message=""
}

# Purpose: Find one tracked cleanup-helper record by PID.
# Usage: Called during teardown lookups; publishes the stored purpose in
# $g_zxfer_cleanup_pid_record_purpose.
zxfer_find_cleanup_pid_record() {
	l_cleanup_find_pid=$1

	g_zxfer_cleanup_pid_record_purpose=""

	while IFS='	' read -r l_cleanup_find_record_pid l_cleanup_find_record_purpose || [ -n "${l_cleanup_find_record_pid}${l_cleanup_find_record_purpose}" ]; do
		[ -n "$l_cleanup_find_record_pid" ] || continue
		[ "$l_cleanup_find_record_pid" = "$l_cleanup_find_pid" ] || continue
		g_zxfer_cleanup_pid_record_purpose=$l_cleanup_find_record_purpose
		return 0
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF

	return 1
}

# Purpose: Register the cleanup helper with the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup so cleanup
# and later lookups can find the live resource. Rows are (pid, purpose) only.
# SAFETY: every helper tracked here is a direct child of the current shell
# that has not been waited on, so the numeric PID is sufficient identity --
# POSIX keeps an un-reaped child's PID from being recycled. That invariant
# replaces the per-registration start-token, hostname, and process-group
# captures this registry used to take.
zxfer_register_cleanup_pid() {
	l_cleanup_register_pid=$1
	l_cleanup_register_purpose=${2:-cleanup helper}

	case "$l_cleanup_register_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	[ "$l_cleanup_register_pid" = "$$" ] && return 0

	l_cleanup_register_purpose=$(zxfer_normalize_owned_lock_text_field "$l_cleanup_register_purpose") ||
		return "$?"
	if zxfer_find_cleanup_pid_record "$l_cleanup_register_pid"; then
		return 0
	fi
	kill -s 0 "$l_cleanup_register_pid" 2>/dev/null || return 0

	if [ -n "${g_zxfer_cleanup_pid_records:-}" ]; then
		g_zxfer_cleanup_pid_records=$g_zxfer_cleanup_pid_records"
$l_cleanup_register_pid	$l_cleanup_register_purpose"
	else
		g_zxfer_cleanup_pid_records="$l_cleanup_register_pid	$l_cleanup_register_purpose"
	fi
	if [ -n "${g_zxfer_cleanup_pids:-}" ]; then
		g_zxfer_cleanup_pids="$g_zxfer_cleanup_pids $l_cleanup_register_pid"
	else
		g_zxfer_cleanup_pids=$l_cleanup_register_pid
	fi

	return 0
}

# Purpose: Remove the cleanup helper from the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after the
# tracked resource has completed or been cleaned up.
zxfer_unregister_cleanup_pid() {
	l_cleanup_unregister_pid=$1
	l_cleanup_unregister_remaining_pids=""
	l_cleanup_unregister_remaining_records=""

	case "$l_cleanup_unregister_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	for l_cleanup_unregister_existing_pid in ${g_zxfer_cleanup_pids:-}; do
		[ "$l_cleanup_unregister_existing_pid" = "$l_cleanup_unregister_pid" ] && continue
		if [ -n "$l_cleanup_unregister_remaining_pids" ]; then
			l_cleanup_unregister_remaining_pids="$l_cleanup_unregister_remaining_pids $l_cleanup_unregister_existing_pid"
		else
			l_cleanup_unregister_remaining_pids=$l_cleanup_unregister_existing_pid
		fi
	done
	while IFS='	' read -r l_cleanup_unregister_record_pid l_cleanup_unregister_record_purpose || [ -n "${l_cleanup_unregister_record_pid}${l_cleanup_unregister_record_purpose}" ]; do
		[ -n "$l_cleanup_unregister_record_pid" ] || continue
		[ "$l_cleanup_unregister_record_pid" = "$l_cleanup_unregister_pid" ] && continue
		if [ -n "$l_cleanup_unregister_remaining_records" ]; then
			l_cleanup_unregister_remaining_records=$l_cleanup_unregister_remaining_records"
$l_cleanup_unregister_record_pid	$l_cleanup_unregister_record_purpose"
		else
			l_cleanup_unregister_remaining_records="$l_cleanup_unregister_record_pid	$l_cleanup_unregister_record_purpose"
		fi
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF

	g_zxfer_cleanup_pids=$l_cleanup_unregister_remaining_pids
	g_zxfer_cleanup_pid_records=$l_cleanup_unregister_remaining_records
}

# Purpose: Signal one direct child helper that this shell has not waited on.
# Usage: Called by callers that spawned a helper but could not register it (or
# registered it elsewhere) and must stop it before failing.
# SAFETY: only ever invoked for un-reaped direct children of the current
# shell, so per the registry safety invariant the signal cannot reach an
# unrelated process. Helpers with descendants are spawned through the cleanup
# child wrapper, whose TERM trap reaps the rest of the tree.
zxfer_abort_direct_child_pid() {
	l_cleanup_direct_abort_pid=$1
	l_cleanup_direct_abort_signal=${2:-TERM}
	l_cleanup_direct_abort_purpose=${3:-cleanup helper}

	g_zxfer_cleanup_pid_abort_failure_message=""
	case "$l_cleanup_direct_abort_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	[ "$l_cleanup_direct_abort_pid" = "$$" ] && return 1

	l_cleanup_direct_abort_purpose=$(zxfer_normalize_owned_lock_text_field "$l_cleanup_direct_abort_purpose") ||
		return "$?"
	kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null || return 0
	if kill -s "$l_cleanup_direct_abort_signal" "$l_cleanup_direct_abort_pid" 2>/dev/null; then
		return 0
	fi
	kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null || return 0
	g_zxfer_cleanup_pid_abort_failure_message="Failed to signal cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid)."
	return 1
}

# Purpose: Stop one registered cleanup helper and drop its registry row.
# Usage: Called during shutdown and failure handling. Untracked PIDs return
# success; helpers that already exited are unregistered without signalling.
# SAFETY: tracked helpers are un-reaped direct children (see the registration
# safety note), so kill -0 plus one TERM-class signal to the PID is the whole
# teardown -- no process-table snapshots or identity revalidation needed.
zxfer_abort_cleanup_pid() {
	l_cleanup_abort_pid=$1
	l_cleanup_abort_signal=${2:-TERM}

	g_zxfer_cleanup_pid_abort_failure_message=""
	zxfer_find_cleanup_pid_record "$l_cleanup_abort_pid" || return 0

	if ! kill -s 0 "$l_cleanup_abort_pid" 2>/dev/null; then
		zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
		return 0
	fi
	if kill -s "$l_cleanup_abort_signal" "$l_cleanup_abort_pid" 2>/dev/null; then
		zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
		return 0
	fi
	if ! kill -s 0 "$l_cleanup_abort_pid" 2>/dev/null; then
		zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
		return 0
	fi
	g_zxfer_cleanup_pid_abort_failure_message="Failed to signal cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
	return 1
}

# Purpose: Stop the registered cleanup helpers that this module still tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# shutdown or failure handling must stop background work that should not
# survive the current run.
zxfer_kill_registered_cleanup_pids() {
	l_cleanup_kill_abort_status=0
	l_cleanup_kill_first_failure_message=""
	l_cleanup_kill_tracked_pids=${g_zxfer_cleanup_pids:-}
	l_cleanup_kill_remaining_pids=""

	for l_cleanup_kill_pid in $l_cleanup_kill_tracked_pids; do
		case "$l_cleanup_kill_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		[ "$l_cleanup_kill_pid" = "$$" ] && continue
		l_cleanup_kill_status=0
		zxfer_abort_cleanup_pid "$l_cleanup_kill_pid" TERM || l_cleanup_kill_status=$?
		if [ "$l_cleanup_kill_status" -ne 0 ]; then
			[ -n "$l_cleanup_kill_first_failure_message" ] || l_cleanup_kill_first_failure_message=$g_zxfer_cleanup_pid_abort_failure_message
			[ "$l_cleanup_kill_abort_status" -ne 0 ] || l_cleanup_kill_abort_status=$l_cleanup_kill_status
		fi
	done

	if [ "$l_cleanup_kill_abort_status" -eq 0 ]; then
		g_zxfer_cleanup_pid_abort_failure_message=""
	fi
	if [ -n "$l_cleanup_kill_first_failure_message" ]; then
		g_zxfer_cleanup_pid_abort_failure_message=$l_cleanup_kill_first_failure_message
	fi
	while IFS='	' read -r l_cleanup_kill_record_pid l_cleanup_kill_record_purpose || [ -n "${l_cleanup_kill_record_pid}${l_cleanup_kill_record_purpose}" ]; do
		[ -n "$l_cleanup_kill_record_pid" ] || continue
		if [ -n "$l_cleanup_kill_remaining_pids" ]; then
			l_cleanup_kill_remaining_pids="$l_cleanup_kill_remaining_pids $l_cleanup_kill_record_pid"
		else
			l_cleanup_kill_remaining_pids=$l_cleanup_kill_record_pid
		fi
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF
	g_zxfer_cleanup_pids=$l_cleanup_kill_remaining_pids

	return "$l_cleanup_kill_abort_status"
}

# Purpose: List the default temporary directory candidates in the stable order
# or format later helpers expect.
# Usage: Called by the zxfer_try_get_effective_tmpdir fallback walk; tests
# override it by name to steer candidate selection.
zxfer_list_default_tmpdir_candidates() {
	printf '%s\n' "/dev/shm" "/run/shm" "/tmp"
}

# Purpose: Try to resolve or create the get socket cache temporary directory
# without treating every miss as fatal.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# has an optional or fallback path that still needs one checked helper.
zxfer_try_get_socket_cache_tmpdir() {
	l_requested_tmpdir=${TMPDIR:-}

	if [ -n "$l_requested_tmpdir" ] &&
		l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir"); then
		# Keep the literal TMPDIR spelling only when the single-pass cd -P
		# resolution proves no symlink or dot segment changes its meaning;
		# otherwise fall through to the validated physical path.
		if [ "$l_effective_tmpdir" = "$l_requested_tmpdir" ]; then
			printf '%s\n' "$l_requested_tmpdir"
			return 0
		fi
	fi

	zxfer_try_get_effective_tmpdir
}

# Purpose: Try to resolve the effective temporary directory once -- a safe
# TMPDIR when one is requested, else the first safe default candidate --
# without treating every miss as fatal.
# Usage: Called by zxfer_ensure_run_tmp_root and
# zxfer_try_get_socket_cache_tmpdir; memoizes per requested TMPDIR in
# current-shell state.
zxfer_try_get_effective_tmpdir() {
	if [ -n "${TMPDIR:-}" ]; then
		l_requested_tmpdir=$TMPDIR
		l_request_key=$l_requested_tmpdir
	else
		l_requested_tmpdir=""
		l_request_key="__ZXFER_DEFAULT_TMPDIR__"
	fi

	if [ -n "${g_zxfer_effective_tmpdir:-}" ] &&
		[ "${g_zxfer_effective_tmpdir_requested:-}" = "$l_request_key" ]; then
		printf '%s\n' "$g_zxfer_effective_tmpdir"
		return 0
	fi

	l_effective_tmpdir=""
	if [ -n "$l_requested_tmpdir" ]; then
		l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir") ||
			l_effective_tmpdir=""
	fi
	if [ -z "$l_effective_tmpdir" ]; then
		l_candidates=$(zxfer_list_default_tmpdir_candidates)
		while IFS= read -r l_candidate || [ -n "$l_candidate" ]; do
			[ -n "$l_candidate" ] || continue
			if l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_candidate"); then
				break
			fi
			l_effective_tmpdir=""
		done <<EOF
$l_candidates
EOF
		if [ -z "$l_effective_tmpdir" ]; then
			g_zxfer_effective_tmpdir_requested=$l_request_key
			g_zxfer_effective_tmpdir=""
			return 1
		fi
		if [ -n "$l_requested_tmpdir" ]; then
			# The fallback decision can run before option parsing (the eager
			# run temp root in zxfer_init_globals), so hold the advisory and
			# let zxfer_emit_pending_tmpdir_fallback_note replay it once -V
			# state is known; when -V is already live it emits immediately.
			g_zxfer_tmpdir_fallback_note="Ignoring unsafe TMPDIR $l_requested_tmpdir; using $l_effective_tmpdir instead."
			zxfer_emit_pending_tmpdir_fallback_note
		fi
	fi

	g_zxfer_effective_tmpdir_requested=$l_request_key
	g_zxfer_effective_tmpdir=$l_effective_tmpdir
	printf '%s\n' "$g_zxfer_effective_tmpdir"
}

# Purpose: Emit the held unsafe-TMPDIR fallback advisory under -V once option
# parsing has made the verbosity state known.
# Usage: Called by zxfer_try_get_effective_tmpdir at decision time and once
# after zxfer_read_command_line_switches; a no-op when no fallback happened or
# -V is off.
zxfer_emit_pending_tmpdir_fallback_note() {
	[ -n "${g_zxfer_tmpdir_fallback_note:-}" ] || return 0
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_echoV "$g_zxfer_tmpdir_fallback_note"
		g_zxfer_tmpdir_fallback_note=""
	fi
	return 0
}

# Purpose: Reset the runtime artifact state so the next runtime pass starts
# from a clean state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_runtime_artifact_state() {
	if zxfer_cleanup_registered_runtime_artifacts; then
		l_cleanup_status=0
	else
		l_cleanup_status=$?
	fi
	if ! zxfer_remove_run_tmp_root; then
		l_cleanup_status=1
	fi
	g_zxfer_run_tmp_counter=0
	g_zxfer_runtime_artifact_path_result=""
	g_zxfer_runtime_artifact_read_result=""
	g_zxfer_temp_file_group_result=""
	return "$l_cleanup_status"
}

# Purpose: Create the one per-run private temp root on first need and reuse it
# for every later runtime artifact allocation.
# Usage: Called from zxfer_init_globals after TMPDIR validation and by the
# runtime artifact allocators before they build child paths.
# SAFETY: the root is created mode 0700 (umask 077 + mktemp -d) under the
# validated effective temp directory, so the predictable <prefix>.<counter>
# child names inside it are safe: no other user can traverse, pre-create, or
# replace entries under a private root this process just created.
zxfer_ensure_run_tmp_root() {
	if [ -n "${g_zxfer_run_tmp_root:-}" ] && [ -d "$g_zxfer_run_tmp_root" ] &&
		[ ! -L "$g_zxfer_run_tmp_root" ]; then
		return 0
	fi

	# Plain call (no command substitution) so the once-per-run validation
	# memoizes in this shell and a held unsafe-TMPDIR fallback advisory
	# survives until option parsing can emit it.
	zxfer_try_get_effective_tmpdir >/dev/null || return "$?"
	l_effective_tmpdir=$g_zxfer_effective_tmpdir

	l_old_umask=$(umask)
	umask 077
	l_status=0
	l_run_tmp_root=$(mktemp -d "$l_effective_tmpdir/zxfer.$$.XXXXXX" 2>/dev/null) ||
		l_status=$?
	umask "$l_old_umask"
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	g_zxfer_run_tmp_root=$l_run_tmp_root
	g_zxfer_run_tmp_counter=0
	return 0
}

# Purpose: Remove the per-run temp root and every runtime artifact below it.
# Usage: Called from runtime state resets and zxfer_trap_exit so one rm -rf
# covers success and failure paths.
zxfer_remove_run_tmp_root() {
	l_run_tmp_root=${g_zxfer_run_tmp_root:-}

	[ -n "$l_run_tmp_root" ] || return 0
	if rm -rf "$l_run_tmp_root" 2>/dev/null ||
		{ [ ! -e "$l_run_tmp_root" ] && [ ! -L "$l_run_tmp_root" ]; }; then
		g_zxfer_run_tmp_root=""
		zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
		return 0
	fi

	return 1
}

# Purpose: Register the runtime artifact path with the tracking state owned by
# this module.
# Usage: Called for path-adjacent staging artifacts (cache and log staging
# outside the per-run temp root) so trap cleanup can reap them on aborts.
# Artifacts under $g_zxfer_run_tmp_root never register; the root removal in
# zxfer_trap_exit already covers them.
zxfer_register_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_artifact_path" ] && return 0
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF

	if [ -n "${g_zxfer_runtime_artifact_cleanup_paths:-}" ]; then
		g_zxfer_runtime_artifact_cleanup_paths=$g_zxfer_runtime_artifact_cleanup_paths'
'$l_artifact_path
	else
		g_zxfer_runtime_artifact_cleanup_paths=$l_artifact_path
	fi
}

# Purpose: Remove the runtime artifact path from the tracking state owned by
# this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after the
# tracked resource has completed or been cleaned up.
zxfer_unregister_runtime_artifact_path() {
	l_artifact_path=$1
	l_remaining_paths=""

	[ -n "$l_artifact_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_artifact_path" ] && continue
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_existing_path
		else
			l_remaining_paths=$l_existing_path
		fi
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF

	g_zxfer_runtime_artifact_cleanup_paths=$l_remaining_paths
}

# Purpose: Clean up the runtime artifact path that this module created or
# tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0
	if rm -rf "$l_artifact_path" 2>/dev/null ||
		{ [ ! -e "$l_artifact_path" ] && [ ! -L "$l_artifact_path" ] && [ ! -h "$l_artifact_path" ]; }; then
		zxfer_unregister_runtime_artifact_path "$l_artifact_path"
		zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
		return 0
	fi

	return 1
}

# Purpose: Clean up the runtime artifact paths that this module created or
# tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_runtime_artifact_paths() {
	l_cleanup_status=0

	for l_artifact_path in "$@"; do
		[ -n "$l_artifact_path" ] || continue
		if ! zxfer_cleanup_runtime_artifact_path "$l_artifact_path"; then
			l_cleanup_status=1
		fi
	done

	return "$l_cleanup_status"
}

# Purpose: Clean up the newline-delimited runtime artifact path list that this
# module created or tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when a
# caller allocated a dynamic group of artifacts and wants one cleanup call.
zxfer_cleanup_runtime_artifact_path_list() {
	l_artifact_path_list=$1
	l_cleanup_status=0

	while IFS= read -r l_artifact_path || [ -n "$l_artifact_path" ]; do
		[ -n "$l_artifact_path" ] || continue
		if ! zxfer_cleanup_runtime_artifact_path "$l_artifact_path"; then
			l_cleanup_status=1
		fi
	done <<-EOF
		$l_artifact_path_list
	EOF

	return "$l_cleanup_status"
}

# Purpose: Clean up the newline-delimited runtime artifact path list and return
# the caller's original status.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on error
# paths that must preserve the lower-level failure status after cleanup.
zxfer_cleanup_runtime_artifact_path_list_and_return() {
	l_return_status=$1
	l_artifact_path_list=$2

	zxfer_cleanup_runtime_artifact_path_list "$l_artifact_path_list" >/dev/null 2>&1 || :
	return "$l_return_status"
}

# Purpose: Clean up the registered runtime artifacts that this module created
# or tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_registered_runtime_artifacts() {
	l_remaining_paths=""

	while IFS= read -r l_artifact_path || [ -n "$l_artifact_path" ]; do
		[ -n "$l_artifact_path" ] || continue
		if rm -rf "$l_artifact_path" 2>/dev/null ||
			{ [ ! -e "$l_artifact_path" ] && [ ! -L "$l_artifact_path" ] && [ ! -h "$l_artifact_path" ]; }; then
			zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
			continue
		fi
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_artifact_path
		else
			l_remaining_paths=$l_artifact_path
		fi
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF

	g_zxfer_runtime_artifact_cleanup_paths=$l_remaining_paths
	[ -z "$l_remaining_paths" ]
}

# Purpose: Create a private 0700 scratch directory under the per-run temp
# root.
# Usage: Called by send/receive progress and completion-queue staging,
# snapshot-discovery fast no-op staging, remote-host probe staging, and backup
# metadata helper staging when zxfer needs a fresh scratch directory. Never
# registered for cleanup; the run-root removal already covers it.
zxfer_create_private_temp_dir() {
	l_prefix=${1:-zxfer-temp-dir}

	g_zxfer_runtime_artifact_path_result=""
	zxfer_ensure_run_tmp_root || return "$?"

	# A taken name means an earlier allocation ran in a subshell and its
	# counter bump never reached this shell; skip ahead to a free name.
	while :; do
		g_zxfer_run_tmp_counter=$((g_zxfer_run_tmp_counter + 1))
		l_artifact_dir="$g_zxfer_run_tmp_root/$l_prefix.$g_zxfer_run_tmp_counter"
		if mkdir -m 700 "$l_artifact_dir" 2>/dev/null; then
			break
		fi
		if [ -e "$l_artifact_dir" ] || [ -L "$l_artifact_dir" ]; then
			continue
		fi
		return 1
	done
	zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_dirs_created
	g_zxfer_runtime_artifact_path_result=$l_artifact_dir
	printf '%s\n' "$l_artifact_dir"
}

# Purpose: Create the runtime artifact file under the per-run temp root.
# Usage: Called when zxfer needs a fresh scratch file. Never registered for
# cleanup; the run-root removal already covers it.
zxfer_create_runtime_artifact_file() {
	l_prefix=${1:-zxfer-temp}

	g_zxfer_runtime_artifact_path_result=""
	zxfer_ensure_run_tmp_root || return "$?"

	# A taken name means an earlier allocation ran in a subshell and its
	# counter bump never reached this shell. The noclobber redirection in a
	# subshell is the exclusive 0600 creation step (run umask untouched,
	# failures as a plain nonzero status); a failed create whose name turns
	# out to exist steps ahead to the next free name instead of failing.
	while :; do
		g_zxfer_run_tmp_counter=$((g_zxfer_run_tmp_counter + 1))
		l_artifact_file="$g_zxfer_run_tmp_root/$l_prefix.$g_zxfer_run_tmp_counter"
		if (umask 077 && set -C && : >"$l_artifact_file") 2>/dev/null; then
			break
		fi
		if [ -e "$l_artifact_file" ] || [ -L "$l_artifact_file" ]; then
			continue
		fi
		return 1
	done
	zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_files_created
	g_zxfer_runtime_artifact_path_result=$l_artifact_file
	printf '%s\n' "$l_artifact_file"
}

# Purpose: Write the runtime artifact file in the normalized form later zxfer
# steps expect.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_runtime_artifact_file() {
	l_artifact_path=$1
	l_artifact_payload=$2

	[ -n "$l_artifact_path" ] || return 1
	if (
		printf '%s' "$l_artifact_payload" >"$l_artifact_path"
	) 2>/dev/null; then
		return 0
	else
		l_status=$?
	fi

	case "$l_status" in
	1 | 2)
		# dash reports redirection-open failures as status 2 while other
		# supported /bin/sh implementations collapse the same failure to 1.
		return 1
		;;
	esac

	return "$l_status"
}

# Purpose: Read the runtime artifact file from staged state into the current
# shell.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when later
# helpers need a checked reload instead of ad hoc file reads.
zxfer_read_runtime_artifact_file() {
	l_artifact_path=$1
	l_artifact_contents=""

	g_zxfer_runtime_artifact_read_result=""
	[ -r "$l_artifact_path" ] || return 1

	l_read_status=0
	l_artifact_contents=$(
		cat "$l_artifact_path"
		l_read_status=$?
		# Keep one non-newline sentinel inside the substitution so trailing
		# blank lines from the artifact survive command substitution intact.
		printf x
		exit "$l_read_status"
	) || l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	l_artifact_contents=${l_artifact_contents%?}

	g_zxfer_runtime_artifact_read_result=$l_artifact_contents
	printf '%s' "$l_artifact_contents"
}

# Purpose: Read the runtime artifact file and trim one trailing newline from
# the result for callers whose staged text format is line-oriented.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when a
# module previously wrapped readback only to normalize a final newline.
zxfer_read_runtime_artifact_file_trimmed() {
	l_artifact_path=$1

	zxfer_read_runtime_artifact_file "$l_artifact_path" >/dev/null ||
		return "$?"
	case "$g_zxfer_runtime_artifact_read_result" in
	*'
')
		g_zxfer_runtime_artifact_read_result=${g_zxfer_runtime_artifact_read_result%?}
		;;
	esac
	printf '%s\n' "$g_zxfer_runtime_artifact_read_result"
}

# Purpose: Capture command stdout through a runtime artifact and checked
# readback.
# Usage: Called by modules that need a command's output in current-shell scratch
# without repeating temp allocation, readback, and cleanup ladders.
# Side effects: Publishes captured output in $g_zxfer_runtime_artifact_read_result.
zxfer_capture_runtime_artifact_command_output() {
	l_artifact_prefix=$1
	shift

	g_zxfer_runtime_artifact_read_result=""
	[ -n "$l_artifact_prefix" ] || return 1
	[ "$#" -gt 0 ] || return 1

	zxfer_create_runtime_artifact_file "$l_artifact_prefix" >/dev/null ||
		return "$?"
	l_capture_file=$g_zxfer_runtime_artifact_path_result

	"$@" >"$l_capture_file" || {
		l_capture_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return "$l_capture_status"
	}

	zxfer_read_runtime_artifact_file "$l_capture_file" >/dev/null || {
		l_capture_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return "$l_capture_status"
	}

	zxfer_cleanup_runtime_artifact_path "$l_capture_file"
	return 0
}

# Purpose: Capture command stdout and stderr through a runtime artifact while
# preserving the command status after checked readback.
# Usage: Called by modules that need diagnostic output from commands that may
# fail normally, without repeating temp allocation, readback, and cleanup
# ladders.
# Side effects: Publishes captured output in $g_zxfer_runtime_artifact_read_result.
zxfer_capture_runtime_artifact_combined_command_output() {
	l_artifact_prefix=$1
	shift

	g_zxfer_runtime_artifact_read_result=""
	[ -n "$l_artifact_prefix" ] || return 1
	[ "$#" -gt 0 ] || return 1

	zxfer_create_runtime_artifact_file "$l_artifact_prefix" >/dev/null ||
		return "$?"
	l_capture_file=$g_zxfer_runtime_artifact_path_result

	l_command_status=0
	"$@" >"$l_capture_file" 2>&1 || l_command_status=$?

	zxfer_read_runtime_artifact_file "$l_capture_file" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return "$l_read_status"
	}

	zxfer_cleanup_runtime_artifact_path "$l_capture_file"
	return "$l_command_status"
}

# Purpose: Return the temp file in the form expected by later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_temp_file() {
	g_zxfer_temp_file_result=""
	zxfer_create_runtime_artifact_file "zxfer-temp" >/dev/null ||
		zxfer_throw_error "Error creating temporary file." "$?"
	zxfer_echoV "New temporary file: $g_zxfer_runtime_artifact_path_result"
	g_zxfer_temp_file_result=$g_zxfer_runtime_artifact_path_result
	echo "$g_zxfer_temp_file_result"
}

# Purpose: Allocate a group of temp files and publish their paths as one
# newline-delimited result.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when a
# multi-stage operation needs several files and must clean up partial
# allocation on failure.
zxfer_create_temp_file_group() {
	l_temp_file_count=$1
	l_temp_file_index=0
	l_temp_file_group_paths=""

	g_zxfer_temp_file_group_result=""
	case "$l_temp_file_count" in
	'' | *[!0-9]* | 0)
		return 1
		;;
	esac

	while [ "$l_temp_file_index" -lt "$l_temp_file_count" ]; do
		zxfer_get_temp_file >/dev/null || {
			l_temp_file_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_temp_file_group_paths" >/dev/null 2>&1 || :
			return "$l_temp_file_status"
		}
		if [ -n "$l_temp_file_group_paths" ]; then
			l_temp_file_group_paths=$l_temp_file_group_paths'
'$g_zxfer_temp_file_result
		else
			l_temp_file_group_paths=$g_zxfer_temp_file_result
		fi
		l_temp_file_index=$((l_temp_file_index + 1))
	done

	g_zxfer_temp_file_group_result=$l_temp_file_group_paths
	printf '%s\n' "$l_temp_file_group_paths"
}

# Purpose: Return the operating system in the form expected by later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
#
# Gets a $(uname), i.e. the operating system, for origin or target, if remote.
# Takes: $1=either $g_option_O_origin_host or $g_option_T_target_host
zxfer_get_os() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_output_os=""

	# Get uname of the destination (target) machine, local or remote
	if [ "$l_host_spec" = "" ]; then
		l_output_os=$(uname)
	else
		l_output_os=$(zxfer_get_remote_host_operating_system "$l_host_spec" "$l_profile_side") ||
			return "$?"
	fi

	echo "$l_output_os"
}

# Purpose: Return the max yield iterations in the form expected by later
# helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_max_yield_iterations() {
	printf '%s\n' "$ZXFER_MAX_YIELD_ITERATIONS"
}

# Purpose: Initialize the dependency tool defaults before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_dependency_tool_defaults() {
	g_cmd_zfs=""
	g_cmd_ssh=""

	# default compression commands
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	g_origin_cmd_compress_safe=""
	g_origin_cmd_decompress_safe=""
	g_target_cmd_compress_safe=""
	g_target_cmd_decompress_safe=""
	g_cmd_cat=""
	g_cmd_ps=""

	zxfer_assign_required_tool g_cmd_awk awk "awk"
	zxfer_assign_required_tool g_cmd_zfs zfs "zfs"
	g_cmd_parallel=$(PATH=$g_zxfer_dependency_path command -v parallel 2>/dev/null || :)
	if [ "$g_cmd_parallel" != "" ]; then
		l_dependency_status=0
		g_cmd_parallel=$(zxfer_validate_resolved_tool_path "$g_cmd_parallel" "parallel") ||
			l_dependency_status=$?
		if [ "$l_dependency_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_cmd_parallel" "$l_dependency_status"
		fi
	fi

	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	zxfer_assign_required_tool g_cmd_ps ps "ps"
	zxfer_refresh_compression_commands
}

# Purpose: Refresh the ssh control-socket capability flag from the current
# local ssh helper state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap and later lazy ssh resolution so downstream code can rely on one
# cached support flag.
zxfer_refresh_ssh_control_socket_support_state() {
	g_ssh_supports_control_sockets=0
	if zxfer_ssh_supports_control_sockets; then
		g_ssh_supports_control_sockets=1
	fi
}

# Purpose: Initialize the transport remote defaults before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_transport_remote_defaults() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_zxfer_remote_capability_response_result=""
	g_zxfer_remote_capability_tool_records=""
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	g_zxfer_remote_capability_requested_tools_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
	g_source_operating_system=""
	g_destination_operating_system=""
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
	g_zxfer_parallel_source_job_check_kind=""

	# per-run ssh control sockets used for origin (-O) and target (-T) hosts
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""

	# per-role rendered transport-token and host-spec parse memos
	g_zxfer_ssh_transport_tokens_origin=""
	g_zxfer_ssh_transport_tokens_origin_socket=""
	g_zxfer_ssh_transport_tokens_origin_set=0
	g_zxfer_ssh_transport_tokens_target=""
	g_zxfer_ssh_transport_tokens_target_socket=""
	g_zxfer_ssh_transport_tokens_target_set=0
	g_zxfer_ssh_shell_context_memo_origin_spec=""
	g_zxfer_ssh_shell_context_memo_origin_host=""
	g_zxfer_ssh_shell_context_memo_origin_wrapper=""
	g_zxfer_ssh_shell_context_memo_target_spec=""
	g_zxfer_ssh_shell_context_memo_target_host=""
	g_zxfer_ssh_shell_context_memo_target_wrapper=""
	zxfer_refresh_ssh_control_socket_support_state

	# default zfs commands, can be overridden by -O or -T
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs
}

# Purpose: Initialize the runtime state defaults before later helpers depend on
# it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_runtime_state_defaults() {
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

	# profiling and session-scoped scratch state
	zxfer_reset_cleanup_pid_tracking
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_tmpdir_fallback_note=""
	g_zxfer_temp_file_result=""
	zxfer_reset_owned_lock_tracking
	zxfer_reset_runtime_artifact_state
	g_zxfer_profile_start_epoch=$(date '+%s' 2>/dev/null || :)
	if ! g_zxfer_profile_start_ms=$(zxfer_profile_now_ms 2>/dev/null); then
		g_zxfer_profile_start_ms=""
	fi
	g_zxfer_profile_has_data=0
	g_zxfer_profile_summary_emitted=0
	g_zxfer_profile_startup_latency_ms=0
	g_zxfer_profile_startup_latency_recorded=0
	g_zxfer_profile_cleanup_ms=0
	g_zxfer_profile_ssh_setup_ms=0
	g_zxfer_profile_source_snapshot_listing_ms=0
	g_zxfer_profile_destination_snapshot_listing_ms=0
	g_zxfer_profile_snapshot_diff_sort_ms=0
	g_zxfer_profile_ssh_control_socket_lock_wait_count=0
	g_zxfer_profile_ssh_control_socket_lock_wait_ms=0
	g_zxfer_profile_remote_capability_cache_wait_count=0
	g_zxfer_profile_remote_capability_cache_wait_ms=0
	g_zxfer_profile_remote_capability_bootstrap_live=0
	g_zxfer_profile_remote_capability_bootstrap_cache=0
	g_zxfer_profile_remote_capability_bootstrap_memory=0
	g_zxfer_profile_remote_cli_tool_direct_probes=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0
	g_zxfer_profile_source_snapshot_list_commands=0
	g_zxfer_profile_source_snapshot_list_parallel_commands=0
	g_zxfer_profile_send_receive_pipeline_commands=0
	g_zxfer_profile_send_receive_background_pipeline_commands=0
	g_zxfer_profile_exists_destination_calls=0
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_profile_parent_destination_property_reads=0
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_zxfer_profile_runtime_artifact_files_created=0
	g_zxfer_profile_runtime_artifact_dirs_created=0
	g_zxfer_profile_runtime_artifact_paths_cleaned=0
	g_zxfer_profile_runtime_cache_object_writes=0
	g_zxfer_profile_runtime_cache_object_readbacks=0
	g_zxfer_profile_command_render_calls=0
	g_zxfer_profile_live_destination_snapshot_rechecks=0
	g_destination=""
	zxfer_refresh_backup_storage_root

	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off
	g_backup_file_extension=".zxfer_backup_info"
}

# Purpose: Initialize the temp artifacts before later helpers depend on it.
# Usage: Called by zxfer_init_globals during bootstrap so downstream code sees
# consistent defaults and runtime state.
zxfer_init_temp_artifacts() {
	g_zxfer_temp_prefix="zxfer.$$.${g_option_Y_yield_iterations}.$(date +%s)"
	# Delete-planning scratch paths stay empty until
	# zxfer_ensure_snapshot_delete_temp_artifacts allocates them lazily.
	g_delete_source_tmp_file=""
	g_delete_dest_tmp_file=""
	g_delete_snapshots_to_delete_tmp_file=""
}

# Purpose: Ensure the snapshot delete temp artifacts exists and is ready before
# the flow continues.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before
# later helpers assume the resource or cache is available.
zxfer_ensure_snapshot_delete_temp_artifacts() {
	l_delete_source_tmp_file=${g_delete_source_tmp_file:-}
	l_delete_dest_tmp_file=${g_delete_dest_tmp_file:-}
	l_delete_snapshots_to_delete_tmp_file=${g_delete_snapshots_to_delete_tmp_file:-}
	l_new_delete_source_tmp_file=""
	l_new_delete_dest_tmp_file=""

	if [ -z "$l_delete_source_tmp_file" ]; then
		zxfer_get_temp_file >/dev/null || return "$?"
		l_delete_source_tmp_file=$g_zxfer_temp_file_result
		l_new_delete_source_tmp_file=$l_delete_source_tmp_file
	fi

	if [ -z "$l_delete_dest_tmp_file" ]; then
		zxfer_get_temp_file >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$l_new_delete_source_tmp_file"
			return "$l_status"
		}
		l_delete_dest_tmp_file=$g_zxfer_temp_file_result
		l_new_delete_dest_tmp_file=$l_delete_dest_tmp_file
	fi

	if [ -z "$l_delete_snapshots_to_delete_tmp_file" ]; then
		zxfer_get_temp_file >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$l_new_delete_source_tmp_file" \
				"$l_new_delete_dest_tmp_file"
			return "$l_status"
		}
		l_delete_snapshots_to_delete_tmp_file=$g_zxfer_temp_file_result
	fi

	g_delete_source_tmp_file=$l_delete_source_tmp_file
	g_delete_dest_tmp_file=$l_delete_dest_tmp_file
	g_delete_snapshots_to_delete_tmp_file=$l_delete_snapshots_to_delete_tmp_file
	return 0
}

# Purpose: Initialize the globals before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_globals() {
	zxfer_reset_failure_context "startup"
	zxfer_refresh_secure_path_state

	g_zxfer_version="2.0.0-20260623"
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_d_delete_destination_snapshots=0
	g_option_D_display_progress_bar=""
	g_option_e_restore_property_mode=0
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_I_ignore_properties=""
	# Default 1 avoids parallel source listing and background send jobs.
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_O_origin_host_safe=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_T_target_host_safe=""
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_w_raw_send=0
	g_option_z_compress=0
	zxfer_init_runtime_state_defaults
	if command -v zxfer_reset_replication_runtime_state >/dev/null 2>&1; then
		zxfer_reset_replication_runtime_state
	fi
	if command -v zxfer_reset_send_receive_state >/dev/null 2>&1; then
		zxfer_reset_send_receive_state
	fi
	if command -v zxfer_reset_background_job_state >/dev/null 2>&1; then
		zxfer_reset_background_job_state
	fi
	if command -v zxfer_reset_destination_existence_cache >/dev/null 2>&1; then
		zxfer_reset_destination_existence_cache
	fi
	if command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1; then
		zxfer_reset_snapshot_record_indexes
	fi
	if command -v zxfer_reset_snapshot_discovery_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_discovery_state
	fi
	if command -v zxfer_reset_snapshot_reconcile_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_reconcile_state
	fi
	if command -v zxfer_reset_backup_metadata_state >/dev/null 2>&1; then
		zxfer_reset_backup_metadata_state
	fi
	if command -v zxfer_reset_property_runtime_state >/dev/null 2>&1; then
		zxfer_reset_property_runtime_state
	fi
	# Property scratch state lives with the property modules; reset it through
	# their public helpers so startup and iteration resets cannot drift apart.
	if command -v zxfer_reset_property_iteration_caches >/dev/null 2>&1; then
		zxfer_reset_property_iteration_caches
	fi
	if command -v zxfer_reset_property_reconcile_state >/dev/null 2>&1; then
		zxfer_reset_property_reconcile_state
	fi
	zxfer_init_dependency_tool_defaults
	zxfer_init_transport_remote_defaults
	zxfer_init_temp_artifacts
	zxfer_apply_secure_path
	# Create the per-run temp root eagerly in this shell (after the secure
	# PATH is live so mktemp resolves through it) so subshell allocators share
	# one root the already-registered exit trap removes. Failure stays
	# non-fatal; the first allocation that needs the root reports it.
	zxfer_ensure_run_tmp_root || :
}

# Purpose: Run the centralized shutdown path that cleans up runtime artifacts,
# transports, and end-of-run reporting state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# shell exits so cleanup and failure reporting stay consistent across success
# and failure paths.
zxfer_trap_exit() {
	# get the exit status of the last command
	l_exit_status=$?
	l_cleanup_start_ms=""
	if command -v zxfer_profile_metrics_enabled >/dev/null 2>&1 &&
		zxfer_profile_metrics_enabled; then
		l_cleanup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	# Only terminate zxfer-owned background processes. Killing every direct child
	# of the shell is too broad and can clobber coverage helpers or command
	# substitution plumbing in the caller.
	if command -v zxfer_abort_all_background_jobs >/dev/null 2>&1; then
		l_background_cleanup_status=0
		zxfer_abort_all_background_jobs || l_background_cleanup_status=$?
		if [ "$l_background_cleanup_status" -ne 0 ]; then
			[ "$l_exit_status" -eq 0 ] && l_exit_status=$l_background_cleanup_status
			if [ -z "${g_zxfer_failure_message:-}" ]; then
				g_zxfer_failure_class=runtime
				g_zxfer_failure_stage="trap cleanup"
				g_zxfer_failure_message=${g_zxfer_background_job_abort_failure_message:-Failed to tear down one or more supervised background jobs during exit.}
			fi
		fi
	fi
	l_cleanup_pid_status=0
	zxfer_kill_registered_cleanup_pids || l_cleanup_pid_status=$?
	if [ "$l_cleanup_pid_status" -ne 0 ]; then
		[ "$l_exit_status" -eq 0 ] && l_exit_status=$l_cleanup_pid_status
		if [ -z "${g_zxfer_failure_message:-}" ]; then
			g_zxfer_failure_class=runtime
			g_zxfer_failure_stage="trap cleanup"
			g_zxfer_failure_message=${g_zxfer_cleanup_pid_abort_failure_message:-Failed to tear down one or more validated cleanup helpers during exit.}
		fi
	fi

	if command -v zxfer_close_all_ssh_control_sockets >/dev/null 2>&1; then
		if zxfer_close_all_ssh_control_sockets; then
			:
		else
			l_close_status=$?
			if [ "$l_exit_status" -eq 0 ]; then
				l_exit_status=$l_close_status
				if [ -z "${g_zxfer_failure_message:-}" ]; then
					g_zxfer_failure_class=runtime
					g_zxfer_failure_stage="trap cleanup"
					g_zxfer_failure_message="Failed to close one or more ssh control sockets during exit."
				fi
			fi
		fi
	fi
	# Every per-run transient lives under the one private temp root; one
	# rm -rf replaces per-artifact bookkeeping. Registered path-adjacent
	# staging debris is reaped first because it lives outside the root.
	l_artifact_cleanup_failed=0
	zxfer_cleanup_registered_runtime_artifacts || l_artifact_cleanup_failed=1
	zxfer_remove_run_tmp_root || l_artifact_cleanup_failed=1
	if [ "$l_artifact_cleanup_failed" -ne 0 ]; then
		[ "$l_exit_status" -eq 0 ] && l_exit_status=1
		if [ -z "${g_zxfer_failure_message:-}" ]; then
			g_zxfer_failure_class=runtime
			g_zxfer_failure_stage="trap cleanup"
			g_zxfer_failure_message="Failed to remove one or more runtime temp artifacts during exit."
		fi
	fi
	if [ "${g_services_need_relaunch:-0}" -eq 1 ]; then
		if [ "${g_services_relaunch_in_progress:-0}" -eq 1 ]; then
			zxfer_echoV "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
		elif command -v zxfer_relaunch >/dev/null 2>&1; then
			zxfer_echoV "zxfer exiting early; restarting stopped services."
			zxfer_relaunch
		else
			zxfer_echoV "zxfer exiting with services still stopped; zxfer_relaunch() unavailable."
		fi
	fi

	zxfer_profile_add_elapsed_ms g_zxfer_profile_cleanup_ms "$l_cleanup_start_ms"
	zxfer_echoV "zxfer exiting with status $l_exit_status"
	zxfer_profile_emit_summary
	zxfer_emit_failure_report "$l_exit_status"

	# Failure reporting may lazily recreate the run temp root or stage log
	# files (ZXFER_ERROR_LOG mirroring); sweep again so nothing survives exit.
	zxfer_cleanup_registered_runtime_artifacts >/dev/null 2>&1 || :
	zxfer_remove_run_tmp_root >/dev/null 2>&1 || :

	# exit this script
	exit $l_exit_status
}

# Purpose: Register the runtime traps with the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup so cleanup
# and later lookups can find the live resource.
zxfer_register_runtime_traps() {
	# catch any signals to terminate the script
	# INT (Interrupt) 2 (Ctrl-C)
	# TERM (Terminate) 15 (kill)
	# HUP (Hangup) 1 (kill -HUP)
	# QUIT (Quit) 3 (Ctrl-\)
	# EXIT (Exit) 0 (exit)
	trap zxfer_trap_exit INT TERM HUP QUIT EXIT
}

# Purpose: Initialize the source execution context before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_source_execution_context() {
	if [ "$g_option_O_origin_host" != "" ]; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			g_source_operating_system=""
			g_origin_cmd_zfs=${g_origin_cmd_zfs:-$g_cmd_zfs}
			if [ "$g_option_z_compress" -eq 1 ] &&
				[ -z "${g_origin_cmd_compress_safe:-}" ]; then
				g_origin_cmd_compress_safe=$(zxfer_quote_cli_tokens "$g_cmd_compress" "compression command") ||
					zxfer_throw_error "$g_origin_cmd_compress_safe" "$?"
			fi
			zxfer_echoV "Dry run: skipping live remote source helper validation."
			return
		fi
		l_source_context_status=0
		g_source_operating_system=$(zxfer_get_os "$g_option_O_origin_host" source) ||
			l_source_context_status=$?
		if [ "$l_source_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_O_origin_host." "$l_source_context_status"
		fi
		l_source_context_status=0
		g_origin_cmd_zfs=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" zfs "zfs" source) ||
			l_source_context_status=$?
		if [ "$l_source_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_origin_cmd_zfs" "$l_source_context_status"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_source_context_status=0
			g_origin_cmd_compress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_O_origin_host" "$g_cmd_compress" "compression command" source) ||
				l_source_context_status=$?
			if [ "$l_source_context_status" -ne 0 ]; then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_origin_cmd_compress_safe" "$l_source_context_status"
			fi
		fi
		return
	fi

	l_source_context_status=0
	g_source_operating_system=$(zxfer_get_os "") || l_source_context_status=$?
	if [ "$l_source_context_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_source_context_status"
	fi
	g_origin_cmd_zfs=$g_cmd_zfs
}

# Purpose: Initialize the destination execution context before later helpers
# depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_destination_execution_context() {
	if [ "$g_option_T_target_host" != "" ]; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			g_destination_operating_system=""
			g_target_cmd_zfs=${g_target_cmd_zfs:-$g_cmd_zfs}
			if [ "$g_option_z_compress" -eq 1 ] &&
				[ -z "${g_target_cmd_decompress_safe:-}" ]; then
				g_target_cmd_decompress_safe=$(zxfer_quote_cli_tokens "$g_cmd_decompress" "decompression command") ||
					zxfer_throw_error "$g_target_cmd_decompress_safe" "$?"
			fi
			zxfer_echoV "Dry run: skipping live remote destination helper validation."
			return
		fi
		l_destination_context_status=0
		g_destination_operating_system=$(zxfer_get_os "$g_option_T_target_host" destination) ||
			l_destination_context_status=$?
		if [ "$l_destination_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_T_target_host." "$l_destination_context_status"
		fi
		l_destination_context_status=0
		g_target_cmd_zfs=$(zxfer_resolve_remote_required_tool "$g_option_T_target_host" zfs "zfs" destination) ||
			l_destination_context_status=$?
		if [ "$l_destination_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_target_cmd_zfs" "$l_destination_context_status"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_destination_context_status=0
			g_target_cmd_decompress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "$g_cmd_decompress" "decompression command" destination) ||
				l_destination_context_status=$?
			if [ "$l_destination_context_status" -ne 0 ]; then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_target_cmd_decompress_safe" "$l_destination_context_status"
			fi
		fi
		return
	fi

	l_destination_context_status=0
	g_destination_operating_system=$(zxfer_get_os "") || l_destination_context_status=$?
	if [ "$l_destination_context_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_destination_context_status"
	fi
	g_target_cmd_zfs=$g_cmd_zfs
}

# Purpose: Initialize the restore property helpers before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_restore_property_helpers() {
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return

	if [ "$g_option_O_origin_host" = "" ]; then
		zxfer_assign_required_tool g_cmd_cat cat "cat"
		return
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		[ -n "${g_cmd_cat:-}" ] || g_cmd_cat="cat"
		zxfer_echoV "Dry run: skipping live remote backup-restore helper validation."
		return
	fi

	l_restore_property_status=0
	g_cmd_cat=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" cat "cat" source) ||
		l_restore_property_status=$?
	if [ "$l_restore_property_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "$g_cmd_cat" "$l_restore_property_status"
	fi
}

# Purpose: Initialize the local awk compatibility before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_local_awk_compatibility() {
	l_awk_compatibility_status=0
	l_home_operating_system=$(zxfer_get_os "") || l_awk_compatibility_status=$?
	if [ "$l_awk_compatibility_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_awk_compatibility_status"
	fi
	if [ "$l_home_operating_system" != "SunOS" ]; then
		return
	fi

	l_gawk_path=$(PATH=$g_zxfer_dependency_path command -v gawk 2>/dev/null || :)
	if [ "$l_gawk_path" != "" ]; then
		g_cmd_awk=$l_gawk_path
	fi
}

# Purpose: Initialize the variables before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_variables() {
	g_origin_cmd_compress_safe=$g_cmd_compress_safe
	g_origin_cmd_decompress_safe=$g_cmd_decompress_safe
	g_target_cmd_compress_safe=$g_cmd_compress_safe
	g_target_cmd_decompress_safe=$g_cmd_decompress_safe
	zxfer_init_source_execution_context
	zxfer_init_destination_execution_context
	zxfer_refresh_remote_zfs_commands
	zxfer_init_restore_property_helpers
	zxfer_init_local_awk_compatibility
}
