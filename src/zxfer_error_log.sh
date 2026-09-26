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
# SECURE ERROR LOGGING
################################################################################

# Module contract:
# owns globals: ZXFER_LOCK_METADATA_HEADER; g_zxfer_owned_lock_pid_result and
#   g_zxfer_owned_lock_start_token_result (the last loaded lock owner); and
#   g_zxfer_own_process_start_token (this shell's memoized start token).
# reads globals: ZXFER_ERROR_LOG, TMPDIR, and g_zxfer_secure_staging_dir_result
#   (set by zxfer_create_secure_staging_dir_for_path in zxfer_runtime.sh).
#   Start tokens source the cleanup wrapper found by
#   zxfer_get_cleanup_child_wrapper_script_path (ZXFER_SOURCE_MODULES_ROOT).
# mutates caches: the ZXFER_ERROR_LOG file, its lock directory, and its
#   staging directory.
# returns via stdout: process-start tokens, lock identities and paths, and the
#   trusted log parent.
#
# zxfer_emit_failure_report (zxfer_reporting.sh) mirrors every failure report
# here. Refusals and write failures only warn: mirroring never changes the
# exit status.

################################################################################
# OWNED LOCK DIRECTORIES
################################################################################

# A lock is a 0700 directory whose mkdir is the atomic acquisition step. Its
# metadata names the owner by pid and process start token only, so a reused
# pid never passes for the owner. Any other metadata format, the old V1
# kind/purpose/hostname layout included, loads as corrupt (status 2) and is
# reaped only under the caller's corrupt-reap policy; locks live for seconds,
# so no cross-version compatibility is needed.

ZXFER_LOCK_METADATA_HEADER="ZXFER_LOCK_METADATA_V2"

# Purpose: Forget the memoized own start token and the last loaded lock owner.
# Usage: zxfer_reset_owned_lock_tracking, from session startup.
zxfer_reset_owned_lock_tracking() {
	g_zxfer_own_process_start_token=""
	g_zxfer_owned_lock_pid_result=""
	g_zxfer_owned_lock_start_token_result=""
}

# Purpose: Print PID's start token, "lstart:TIME" or else "stime:TIME", with
# the cleanup wrapper's ps parser, so lock owners and wrapper descendants
# share one token format.
# Usage: l_token=$(zxfer_get_process_start_token PID); returns 1 for a
# non-numeric PID, an unreadable wrapper, or when ps gives neither field. The
# body is a subshell so the wrapper's source-only guard never reaches this
# shell or its children.
zxfer_get_process_start_token() (
	ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1
	l_token_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path) || return 1
	# shellcheck source=src/zxfer_cleanup_child_wrapper.sh
	. "$l_token_wrapper_script"
	zxfer_cleanup_child_wrapper_get_process_start_token "$1" lstart ||
		zxfer_cleanup_child_wrapper_get_process_start_token "$1" stime
)

# Purpose: Memoize this shell's start token in g_zxfer_own_process_start_token,
# running ps only on first need.
# Usage: zxfer_get_own_process_start_token || return 1, in the current shell
# (a command substitution would lose the memo).
zxfer_get_own_process_start_token() {
	[ -z "${g_zxfer_own_process_start_token:-}" ] || return 0
	l_own_start_token=$(zxfer_get_process_start_token "$$") || return 1
	g_zxfer_own_process_start_token=$l_own_start_token
}

# Purpose: Require a lock path to be a real (non-symlink) directory (d) or
# regular file (f) owned by the effective user with exactly MODE.
# Usage: zxfer_validate_owned_lock_path PATH d|f MODE, for example
# "DIR d 700" or "DIR/metadata f 600".
zxfer_validate_owned_lock_path() {
	l_validate_owned_path=$1

	case $2 in
	d) [ -d "$l_validate_owned_path" ] || return 1 ;;
	f) [ -f "$l_validate_owned_path" ] || return 1 ;;
	*) return 1 ;;
	esac
	[ ! -L "$l_validate_owned_path" ] || return 1
	l_validate_owned_uid=$(zxfer_get_effective_user_uid) || return 1
	l_validate_owned_owner_uid=$(zxfer_get_path_owner_uid "$l_validate_owned_path") || return 1
	[ "$l_validate_owned_owner_uid" = "$l_validate_owned_uid" ] || return 1
	l_validate_owned_mode=$(zxfer_get_path_mode_octal "$l_validate_owned_path") || return 1
	[ "$l_validate_owned_mode" = "$3" ]
}

# Purpose: Load a lock directory's owner into g_zxfer_owned_lock_pid_result
# and g_zxfer_owned_lock_start_token_result.
# Usage: zxfer_load_owned_lock_metadata_from_dir DIR
# Returns: 0 when loaded; 1 when DIR or its metadata file fails validation;
# 2 when the metadata is missing or is not exactly the three V2 lines (older
# formats included). Both results stay empty unless the status is 0.
zxfer_load_owned_lock_metadata_from_dir() {
	l_load_lock_dir=$1
	l_load_metadata_path=$l_load_lock_dir/metadata
	g_zxfer_owned_lock_pid_result=""
	g_zxfer_owned_lock_start_token_result=""

	zxfer_validate_owned_lock_path "$l_load_lock_dir" d 700 || return 1
	[ -e "$l_load_metadata_path" ] || return 2
	zxfer_validate_owned_lock_path "$l_load_metadata_path" f 600 || return 1

	# Exact format: the header, "pid<TAB>DIGITS", "start_token<TAB>TEXT".
	l_load_line_number=0
	l_load_pid=""
	l_load_start_token=""
	while IFS= read -r l_load_line || [ -n "$l_load_line" ]; do
		l_load_line_number=$((l_load_line_number + 1))
		case $l_load_line_number:$l_load_line in
		1:"$ZXFER_LOCK_METADATA_HEADER") ;;
		2:"pid$ZXFER_TAB"*) l_load_pid=${l_load_line#"pid$ZXFER_TAB"} ;;
		3:"start_token$ZXFER_TAB"*) l_load_start_token=${l_load_line#"start_token$ZXFER_TAB"} ;;
		*) return 2 ;;
		esac
	done <"$l_load_metadata_path"
	[ "$l_load_line_number" -eq 3 ] || return 2
	zxfer_is_uint "$l_load_pid" || return 2
	case $l_load_start_token in
	'' | *"$ZXFER_TAB"*) return 2 ;;
	esac

	g_zxfer_owned_lock_pid_result=$l_load_pid
	g_zxfer_owned_lock_start_token_result=$l_load_start_token
}

# Purpose: Require a lock directory to still hold the owner that a release or
# stale-reap decision already validated.
# Usage: zxfer_owned_lock_metadata_matches DIR PID START_TOKEN, just before
# and after the cleanup revalidation so an ownership change fails closed.
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

# Purpose: Remove a validated lock directory with bounded unlink + rmdir, never
# recursive deletion.
# Usage: zxfer_cleanup_owned_lock_dir DIR [PID START_TOKEN]; with an owner the
# metadata must still name it at both checks (corrupt-metadata reaps omit
# it). A blank or missing DIR succeeds.
zxfer_cleanup_owned_lock_dir() {
	l_cleanup_lock_dir=$1
	l_cleanup_expected_pid=${2:-}
	l_cleanup_expected_start_token=${3:-}
	l_cleanup_metadata_path=$l_cleanup_lock_dir/metadata
	l_cleanup_stage_path=$l_cleanup_lock_dir/.metadata.stage

	[ -n "$l_cleanup_lock_dir" ] || return 0
	if [ ! -e "$l_cleanup_lock_dir" ] && [ ! -L "$l_cleanup_lock_dir" ]; then
		return 0
	fi
	zxfer_validate_owned_lock_path "$l_cleanup_lock_dir" d 700 || return 1
	if [ -n "$l_cleanup_expected_pid" ] || [ -n "$l_cleanup_expected_start_token" ]; then
		zxfer_owned_lock_metadata_matches \
			"$l_cleanup_lock_dir" "$l_cleanup_expected_pid" \
			"$l_cleanup_expected_start_token" || return 1
	fi

	# Fail closed, deleting nothing, when the directory holds anything but
	# the two fixed names zxfer creates (unmatched globs stay literal and are
	# skipped as missing).
	case $- in
	*f*)
		l_cleanup_restore_noglob=1
		set +f
		;;
	*)
		l_cleanup_restore_noglob=0
		;;
	esac
	set -- \
		"$l_cleanup_lock_dir"/* \
		"$l_cleanup_lock_dir"/.[!.]* \
		"$l_cleanup_lock_dir"/..?*
	if [ "$l_cleanup_restore_noglob" -eq 1 ]; then
		set -f
	fi
	for l_cleanup_entry in "$@"; do
		if [ ! -e "$l_cleanup_entry" ] &&
			[ ! -L "$l_cleanup_entry" ]; then
			continue
		fi
		case $l_cleanup_entry in
		"$l_cleanup_metadata_path" | "$l_cleanup_stage_path")
			if [ -d "$l_cleanup_entry" ] &&
				[ ! -L "$l_cleanup_entry" ]; then
				return 1
			fi
			;;
		*)
			return 1
			;;
		esac
	done

	# Revalidate after layout inspection. Portable POSIX shell has no dirfd-based
	# removal primitive, but bounded names plus rmdir keep any same-UID pathname
	# race from widening into recursive deletion.
	zxfer_validate_owned_lock_path "$l_cleanup_lock_dir" d 700 || return 1
	if [ -n "$l_cleanup_expected_pid" ]; then
		zxfer_owned_lock_metadata_matches \
			"$l_cleanup_lock_dir" "$l_cleanup_expected_pid" \
			"$l_cleanup_expected_start_token" || return 1
	fi

	for l_cleanup_entry in "$l_cleanup_metadata_path" "$l_cleanup_stage_path"; do
		if [ -e "$l_cleanup_entry" ] || [ -L "$l_cleanup_entry" ]; then
			rm -f "$l_cleanup_entry" 2>/dev/null || return 1
		fi
	done
	if rmdir "$l_cleanup_lock_dir" 2>/dev/null; then
		return 0
	fi
	[ ! -e "$l_cleanup_lock_dir" ] &&
		[ ! -L "$l_cleanup_lock_dir" ]
}

# Purpose: Take a lock by creating DIR (mkdir is the atomic step) and
# publishing this process's pid + start-token metadata inside it.
# Usage: zxfer_create_owned_lock_dir DIR; returns 1 when DIR already exists or
# cannot be set up, removing a half-built DIR unless it fails revalidation.
zxfer_create_owned_lock_dir() {
	l_create_lock_dir=$1
	l_create_lock_stage_path=$l_create_lock_dir/.metadata.stage

	[ -n "$l_create_lock_dir" ] || return 1
	mkdir -m 700 "$l_create_lock_dir" 2>/dev/null || return 1

	# Stage the metadata under a fixed name (this process alone owns the new
	# 0700 directory) and publish it with one rename, so readers only ever
	# see missing or complete metadata. The subshell turns a failed
	# redirection into a plain non-zero status.
	if ! zxfer_validate_owned_lock_path "$l_create_lock_dir" d 700 ||
		! zxfer_get_own_process_start_token ||
		! (
			umask 077
			printf '%s\npid\t%s\nstart_token\t%s\n' \
				"$ZXFER_LOCK_METADATA_HEADER" "$$" "$g_zxfer_own_process_start_token" \
				>"$l_create_lock_stage_path" &&
				{ chmod 600 "$l_create_lock_stage_path" || :; }
		) 2>/dev/null ||
		! mv -f "$l_create_lock_stage_path" "$l_create_lock_dir/metadata" 2>/dev/null; then
		zxfer_cleanup_owned_lock_dir "$l_create_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi
	chmod 600 "$l_create_lock_dir/metadata" 2>/dev/null || :
}

# Purpose: Reap a lock directory whose owner is provably gone, or whose
# metadata is missing or corrupt when the caller allows corrupt reaps.
# Usage: zxfer_try_reap_stale_owned_lock_dir DIR [ALLOW_CORRUPT], where
# ALLOW_CORRUPT is 1/yes/true/on to reap corrupt metadata.
# Returns: 0 when reaped; 1 on a hard failure (including an owner whose
# liveness cannot be checked); 2 when the lock is busy or not yet reapable.
zxfer_try_reap_stale_owned_lock_dir() {
	l_reap_lock_dir=$1
	l_reap_allow_corrupt=${2:-0}
	l_reap_owner_pid=""
	l_reap_owner_start_token=""

	zxfer_load_owned_lock_metadata_from_dir "$l_reap_lock_dir"
	l_reap_load_status=$?
	case $l_reap_load_status in
	0)
		# The owner is alive only while its pid exists with an unchanged start
		# token; a pid that exists but cannot be checked is not reaped.
		l_reap_owner_pid=$g_zxfer_owned_lock_pid_result
		l_reap_owner_start_token=$g_zxfer_owned_lock_start_token_result
		if kill -s 0 "$l_reap_owner_pid" 2>/dev/null; then
			l_reap_current_start_token=$(zxfer_get_process_start_token "$l_reap_owner_pid") ||
				return 1
			[ "$l_reap_current_start_token" != "$l_reap_owner_start_token" ] ||
				return 2
		fi
		;;
	2)
		case $l_reap_allow_corrupt in
		1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn]) ;;
		*) return 2 ;;
		esac
		;;
	*)
		return 1
		;;
	esac

	zxfer_cleanup_owned_lock_dir \
		"$l_reap_lock_dir" "$l_reap_owner_pid" "$l_reap_owner_start_token" || return 1
}

# Purpose: Release a lock directory only when this process (pid and start
# token) owns it, so a live sibling's lock is never deleted.
# Usage: zxfer_release_owned_lock_dir DIR; a blank or missing DIR succeeds,
# and any other owner or failed check returns 1.
zxfer_release_owned_lock_dir() {
	l_release_owned_lock_dir=$1

	[ -n "$l_release_owned_lock_dir" ] || return 0
	if [ ! -e "$l_release_owned_lock_dir" ] && [ ! -L "$l_release_owned_lock_dir" ]; then
		return 0
	fi
	zxfer_load_owned_lock_metadata_from_dir "$l_release_owned_lock_dir" || return 1
	[ "$g_zxfer_owned_lock_pid_result" = "$$" ] || return 1
	zxfer_get_own_process_start_token || return 1
	[ "$g_zxfer_owned_lock_start_token_result" = "$g_zxfer_own_process_start_token" ] || return 1
	zxfer_cleanup_owned_lock_dir \
		"$l_release_owned_lock_dir" "$$" "$g_zxfer_own_process_start_token" || return 1
}

################################################################################
# ERROR-LOG LOCK
################################################################################

# Purpose: Print a log path as lowercase hex, the collision-free name of its
# fallback lock directory.
# Usage: l_hex=$(zxfer_error_log_lock_identity_hex LOG_PATH)
zxfer_error_log_lock_identity_hex() {
	l_key_path=$1

	l_key_hex=$(printf '%s' "$l_key_path" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	[ -n "$l_key_hex" ] || return 1

	printf '%s\n' "$l_key_hex"
}

# Purpose: Create (mode 0700) or reuse one fallback-lock path component and
# require a private 0700 directory owned by this user.
# Usage: zxfer_ensure_error_log_fallback_lock_component_dir DIR
zxfer_ensure_error_log_fallback_lock_component_dir() {
	l_component_dir=$1

	[ -n "$l_component_dir" ] || return 1
	if [ -L "$l_component_dir" ]; then
		return 1
	fi
	# A concurrent run may create the component first; validation decides.
	[ -e "$l_component_dir" ] ||
		mkdir -m 700 "$l_component_dir" 2>/dev/null ||
		[ -d "$l_component_dir" ] ||
		return 1

	zxfer_validate_owned_lock_path "$l_component_dir" d 700
}

# Purpose: Create TEMP_ROOT/.zxfer-error-log.lock.d/hBYTES/HEX... for one log
# path (its hex split into 96-character components) and print the lock path.
# Usage: zxfer_prepare_error_log_fallback_lock_dir TEMP_ROOT LOG_PATH
zxfer_prepare_error_log_fallback_lock_dir() {
	l_fallback_tmpdir=$1
	l_fallback_log_path=$2

	l_fallback_identity_hex=$(zxfer_error_log_lock_identity_hex "$l_fallback_log_path") || return 1
	l_fallback_identity_hex_len=${#l_fallback_identity_hex}
	l_fallback_identity_byte_len=$((l_fallback_identity_hex_len / 2))
	l_fallback_parent_dir=$l_fallback_tmpdir/.zxfer-error-log.lock.d

	zxfer_ensure_error_log_fallback_lock_component_dir "$l_fallback_parent_dir" || return 1
	l_fallback_parent_dir=$l_fallback_parent_dir/h$l_fallback_identity_byte_len
	zxfer_ensure_error_log_fallback_lock_component_dir "$l_fallback_parent_dir" || return 1

	l_fallback_remaining_hex=$l_fallback_identity_hex
	while [ -n "$l_fallback_remaining_hex" ]; do
		l_fallback_chunk=$(printf '%s' "$l_fallback_remaining_hex" | cut -c 1-96)
		l_fallback_remaining_hex=$(printf '%s' "$l_fallback_remaining_hex" | cut -c 97-)
		[ -n "$l_fallback_chunk" ] || return 1
		l_fallback_parent_dir=$l_fallback_parent_dir/$l_fallback_chunk
		zxfer_ensure_error_log_fallback_lock_component_dir "$l_fallback_parent_dir" || return 1
	done

	printf '%s/lock\n' "$l_fallback_parent_dir"
}

# Purpose: Print the private fallback lock path for a log whose parent this
# user cannot write, under the first trusted temp root among TMPDIR,
# /dev/shm, /run/shm, and /tmp.
# Usage: zxfer_get_error_log_fallback_lock_dir LOG_PATH; fails closed when no
# root is trusted or the first trusted root cannot hold the lock path.
zxfer_get_error_log_fallback_lock_dir() {
	for l_fallback_root_candidate in ${TMPDIR:+"$TMPDIR"} /dev/shm /run/shm /tmp; do
		if l_fallback_root=$(zxfer_validate_temp_root_candidate "$l_fallback_root_candidate"); then
			zxfer_prepare_error_log_fallback_lock_dir "$l_fallback_root" "$1"
			return $?
		fi
	done
	return 1
}

# Purpose: Print the lock directory for a validated log: a lock beside the log
# when its parent is writable, else the private fallback lock.
# Usage: zxfer_get_error_log_lock_dir LOG_PATH TRUSTED_PARENT EXISTS
# PARENT_WRITABLE, where the last two are 0 or 1.
zxfer_get_error_log_lock_dir() {
	l_error_log_lock_target_path=$1
	l_error_log_lock_trusted_parent=$2
	l_error_log_lock_exists=$3
	l_error_log_lock_parent_writable=$4

	if [ "$l_error_log_lock_exists" -eq 1 ] &&
		[ "$l_error_log_lock_parent_writable" -eq 0 ]; then
		zxfer_get_error_log_fallback_lock_dir "$l_error_log_lock_target_path"
		return $?
	fi

	printf '%s/.zxfer-error-log.lock.%s\n' \
		"$l_error_log_lock_trusted_parent" "${l_error_log_lock_target_path##*/}"
}

# Purpose: Take the error-log lock directory, retrying up to three times one
# second apart and reaping a stale owner on the way.
# Usage: zxfer_acquire_error_log_lock LOCK_DIR; returns 1 when the lock stays
# busy, is a symlink, or cannot be reaped safely.
zxfer_acquire_error_log_lock() {
	l_lock_dir_path=$1
	l_lock_attempts=0
	l_corrupt_metadata_sightings=0

	while ! zxfer_create_owned_lock_dir "$l_lock_dir_path"; do
		if [ -L "$l_lock_dir_path" ]; then
			return 1
		fi
		if [ -d "$l_lock_dir_path" ]; then
			# Missing or corrupt metadata can be a live winner inside its
			# mkdir-to-metadata publish window, so the first sighting is
			# treated as busy; the corrupt reap is allowed only when a
			# sleep-and-recheck round still reports corrupt metadata.
			l_error_log_allow_corrupt_reap=0
			zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir_path"
			if [ "$?" -eq 2 ]; then
				l_corrupt_metadata_sightings=$((l_corrupt_metadata_sightings + 1))
				[ "$l_corrupt_metadata_sightings" -lt 2 ] ||
					l_error_log_allow_corrupt_reap=1
			fi
			zxfer_try_reap_stale_owned_lock_dir \
				"$l_lock_dir_path" "$l_error_log_allow_corrupt_reap"
			case $? in
			0) continue ;;
			1) return 1 ;;
			esac
		fi
		l_lock_attempts=$((l_lock_attempts + 1))
		[ "$l_lock_attempts" -lt 3 ] || return 1
		sleep 1
	done
	return 0
}

# Purpose: Release the error-log lock, warning when the release fails.
# Usage: zxfer_release_error_log_lock LOG_PATH LOCK_DIR; returns 1 after the
# warning.
zxfer_release_error_log_lock() {
	zxfer_release_owned_lock_dir "$2" && return 0
	l_release_error_log_lock_status=$?
	zxfer_warn_stderr "zxfer: warning: unable to release ZXFER_ERROR_LOG lock for \"$1\" (status $l_release_error_log_lock_status)."
	return 1
}

################################################################################
# LOG FILE
################################################################################

# Purpose: Refuse an existing log that is a symlink, not a regular file, not
# owned by an allowed user, or not mode 0600.
# Usage: zxfer_validate_existing_error_log_file PATH DISPLAY_PATH; warns and
# returns 1 on refusal.
zxfer_validate_existing_error_log_file() {
	l_validate_candidate_path=$1
	l_validate_display_path=$2

	if [ -L "$l_validate_candidate_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_validate_display_path\" because it is a symlink."
		return 1
	fi
	if [ -e "$l_validate_candidate_path" ] && [ ! -f "$l_validate_candidate_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_validate_display_path\" because it is not a regular file."
		return 1
	fi
	if ! l_validate_owner_uid=$(zxfer_get_path_owner_uid "$l_validate_candidate_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because its owner could not be determined."
		return 1
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_validate_owner_uid"; then
		l_validate_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because it is owned by UID $l_validate_owner_uid instead of $l_validate_expected_owner_desc."
		return 1
	fi
	if ! l_validate_mode=$(zxfer_get_path_mode_octal "$l_validate_candidate_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because its permissions could not be determined."
		return 1
	fi
	if [ "$l_validate_mode" != "600" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because its permissions ($l_validate_mode) are not 0600."
		return 1
	fi
}

# Purpose: Validate a ZXFER_ERROR_LOG target (absolute, no symlink component,
# existing trusted parent) and print its physical parent.
# Usage: l_parent=$(zxfer_get_trusted_error_log_parent LOG_PATH); warns and
# returns 1 on refusal.
zxfer_get_trusted_error_log_parent() {
	l_error_log_target_path=$1

	case "$l_error_log_target_path" in
	/*) ;;
	*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_error_log_target_path\" because it is not absolute."
		return 1
		;;
	esac

	if l_error_log_symlink_component=$(zxfer_find_symlink_path_component "$l_error_log_target_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_error_log_target_path\" because path component \"$l_error_log_symlink_component\" is a symlink."
		return 1
	fi

	l_error_log_parent=$(zxfer_get_path_parent_dir "$l_error_log_target_path")
	if [ ! -d "$l_error_log_parent" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_error_log_target_path\" because parent directory \"$l_error_log_parent\" does not exist."
		return 1
	fi
	if ! l_error_log_trusted_parent=$(zxfer_validate_temp_root_candidate "$l_error_log_parent"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_error_log_target_path\" because parent directory \"$l_error_log_parent\" is not owned by root or the effective user, or is writable by others without sticky-bit protection."
		return 1
	fi

	printf '%s\n' "$l_error_log_trusted_parent"
}

# Purpose: Publish an empty 0600 log at LOG_PATH by staging it beside LOG_PATH
# and renaming it into place.
# Usage: zxfer_create_error_log_file LOG_PATH; always removes its staging
# directory.
zxfer_create_error_log_file() {
	l_create_log_path=$1

	zxfer_create_secure_staging_dir_for_path "$l_create_log_path" "zxfer-error-log" || return 1
	l_create_stage_dir=$g_zxfer_secure_staging_dir_result
	l_create_status=1
	if (
		umask 077
		zxfer_write_runtime_artifact_file "$l_create_stage_dir/log.write" ""
	) && mv -f "$l_create_stage_dir/log.write" "$l_create_log_path"; then
		l_create_status=0
	fi
	zxfer_cleanup_runtime_artifact_path "$l_create_stage_dir" >/dev/null 2>&1 || :
	return "$l_create_status"
}

# Purpose: With the lock held, validate the existing log or create a private
# 0600 one.
# Usage: zxfer_prepare_locked_error_log_file LOG_PATH EXISTED (0 or 1); warns
# and returns 1 on refusal.
zxfer_prepare_locked_error_log_file() {
	l_error_log_prepare_path=$1

	# A concurrent holder may have created the log while this run waited on
	# the lock; recheck existence under the lock so the create path cannot
	# clobber a freshly published log with an empty staged file.
	if [ "$2" -eq 0 ] && [ ! -e "$l_error_log_prepare_path" ]; then
		if ! zxfer_create_error_log_file "$l_error_log_prepare_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_error_log_prepare_path\"."
			return 1
		fi
		if ! chmod 600 "$l_error_log_prepare_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_error_log_prepare_path\" to 0600."
			return 1
		fi
	fi
	zxfer_validate_existing_error_log_file "$l_error_log_prepare_path" \
		"$l_error_log_prepare_path"
}

# Purpose: Append REPORT by copying a hard-linked snapshot of the log plus
# REPORT into a private staging directory, then renaming the copy over the log.
# Usage: zxfer_append_failure_report_with_atomic_replace REPORT LOG_PATH, with
# the lock held; warns and returns 1 on failure and always removes the
# staging directory.
zxfer_append_failure_report_with_atomic_replace() {
	l_atomic_report=$1
	l_atomic_log_path=$2
	l_atomic_append_warning="zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_atomic_log_path\"."

	if ! zxfer_create_secure_staging_dir_for_path "$l_atomic_log_path" "zxfer-error-log"; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG staging directory for \"$l_atomic_log_path\"."
		return 1
	fi
	l_atomic_stage_dir=$g_zxfer_secure_staging_dir_result
	l_atomic_snapshot_path=$l_atomic_stage_dir/log.snapshot
	l_atomic_staged_path=$l_atomic_stage_dir/log.write

	# The hard link pins the log's inode, so the file that passes validation
	# is the one copied even if the path is swapped meanwhile.
	l_atomic_status=1
	if ! ln "$l_atomic_log_path" "$l_atomic_snapshot_path" 2>/dev/null; then
		zxfer_warn_stderr "$l_atomic_append_warning"
	elif ! zxfer_validate_existing_error_log_file "$l_atomic_snapshot_path" "$l_atomic_log_path"; then
		: # The validator has already warned.
	elif ! (
		umask 077
		cat "$l_atomic_snapshot_path" >"$l_atomic_staged_path" &&
			printf '%s\n' "$l_atomic_report" >>"$l_atomic_staged_path"
	); then
		zxfer_warn_stderr "$l_atomic_append_warning"
	elif ! chmod 600 "$l_atomic_staged_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_atomic_log_path\" to 0600."
	elif ! mv -f "$l_atomic_staged_path" "$l_atomic_log_path"; then
		zxfer_warn_stderr "$l_atomic_append_warning"
	else
		l_atomic_status=0
	fi
	zxfer_cleanup_runtime_artifact_path "$l_atomic_stage_dir" >/dev/null 2>&1 || :
	return "$l_atomic_status"
}

# Purpose: Mirror one failure report into ZXFER_ERROR_LOG under its lock.
# Usage: zxfer_append_failure_report_to_log REPORT; a no-op when
# ZXFER_ERROR_LOG is unset, otherwise warns and returns 1 on any refusal or
# write failure.
zxfer_append_failure_report_to_log() {
	l_errlog_report=$1
	l_errlog_path=${ZXFER_ERROR_LOG:-}

	[ -n "$l_errlog_path" ] || return 0
	l_errlog_parent=$(zxfer_get_trusted_error_log_parent "$l_errlog_path") || return 1
	l_errlog_exists=0
	[ ! -e "$l_errlog_path" ] || l_errlog_exists=1
	l_errlog_parent_writable=0
	[ ! -w "$l_errlog_parent" ] || l_errlog_parent_writable=1
	if [ "$l_errlog_exists" -eq 0 ] && [ "$l_errlog_parent_writable" -eq 0 ]; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_errlog_path\"."
		return 1
	fi

	if ! l_errlog_lock_dir=$(zxfer_get_error_log_lock_dir "$l_errlog_path" \
		"$l_errlog_parent" "$l_errlog_exists" "$l_errlog_parent_writable") ||
		! zxfer_acquire_error_log_lock "$l_errlog_lock_dir"; then
		zxfer_warn_stderr "zxfer: warning: unable to acquire ZXFER_ERROR_LOG lock for \"$l_errlog_path\"."
		return 1
	fi

	l_errlog_status=0
	if ! zxfer_prepare_locked_error_log_file "$l_errlog_path" "$l_errlog_exists"; then
		l_errlog_status=1
	elif [ "$l_errlog_parent_writable" -eq 0 ]; then
		# No rename is possible in a parent this user cannot write, so append
		# to the validated log in place.
		if ! printf '%s\n' "$l_errlog_report" >>"$l_errlog_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_errlog_path\"."
			l_errlog_status=1
		fi
	elif ! zxfer_append_failure_report_with_atomic_replace "$l_errlog_report" "$l_errlog_path"; then
		l_errlog_status=1
	fi
	# Release on every path; a failed release warns and fails the mirror.
	zxfer_release_error_log_lock "$l_errlog_path" "$l_errlog_lock_dir" || l_errlog_status=1
	return "$l_errlog_status"
}
