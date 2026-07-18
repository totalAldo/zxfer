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
# owns globals: g_zxfer_reporting_capture_result error-log capture scratch state.
# reads globals: ZXFER_ERROR_LOG, g_zxfer_failure_*, and runtime path/artifact/lock results.
# mutates caches: failure-report emission state and secure error-log files, locks, and staging.
# returns via stdout: lock identities and validated fallback lock paths.

# Purpose: Validate the existing error log file before zxfer relies on it.
# Usage: Called during failure reporting, profiling, and verbose operator
# output to fail closed on malformed, unsafe, or stale input.
zxfer_validate_existing_error_log_file() {
	l_validate_candidate_path=$1
	l_validate_display_path=$2

	if [ -L "$l_validate_candidate_path" ] || [ -h "$l_validate_candidate_path" ]; then
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

# Purpose: Render the error-log path identity as lowercase hex.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before fallback lock directories are created or reused for one
# error-log file.
zxfer_error_log_lock_identity_hex() {
	l_key_path=$1

	l_key_hex=$(printf '%s' "$l_key_path" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	[ -n "$l_key_hex" ] || return 1

	printf '%s\n' "$l_key_hex"
}

# Purpose: Ensure an error-log fallback lock component directory is private
# and owned by the current user.
# Usage: Called during failure reporting, profiling, and verbose operator
# output while preparing exact fallback lock paths under a validated temp root.
zxfer_ensure_error_log_fallback_lock_component_dir() {
	l_component_dir=$1
	l_old_umask=$(umask)

	[ -n "$l_component_dir" ] || return 1
	if [ -L "$l_component_dir" ] || [ -h "$l_component_dir" ]; then
		return 1
	fi
	if [ ! -e "$l_component_dir" ]; then
		umask 077
		if ! mkdir "$l_component_dir" 2>/dev/null; then
			umask "$l_old_umask"
			[ -d "$l_component_dir" ] || return 1
		else
			umask "$l_old_umask"
		fi
	else
		umask "$l_old_umask"
	fi

	zxfer_validate_owned_lock_container_dir "$l_component_dir"
}

# Purpose: Prepare the exact fallback lock directory path for `ZXFER_ERROR_LOG`.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the real log parent is trusted but not writable, forcing lock
# state under the validated temp root instead.
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

# Purpose: Capture the reporting helper output into staged state or module
# globals for later use.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need a checked snapshot of command output or
# computed state.
zxfer_capture_reporting_helper_output() {
	l_result_var=${1:-}
	case "$l_result_var" in
	l_?*)
		zxfer_shell_variable_name_is_valid "$l_result_var" || return 1
		;;
	*)
		return 1
		;;
	esac
	shift

	g_zxfer_reporting_capture_result=""
	zxfer_capture_runtime_artifact_command_output "zxfer-reporting" "$@" ||
		return "$?"

	g_zxfer_reporting_capture_result=$g_zxfer_runtime_artifact_read_result
	case "$g_zxfer_reporting_capture_result" in
	*'
')
		g_zxfer_reporting_capture_result=${g_zxfer_reporting_capture_result%?}
		;;
	esac
	eval "$l_result_var=\$g_zxfer_reporting_capture_result"
	return 0
}

# Purpose: Return the error log fallback lock directory in the form expected by
# later helpers.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_error_log_fallback_lock_dir() {
	l_fallback_log_path=$1

	l_fallback_tmpdir=""
	if [ -n "${TMPDIR:-}" ] &&
		zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "$TMPDIR"; then
		:
	elif zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "/dev/shm"; then
		:
	elif zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "/run/shm"; then
		:
	elif zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "/tmp"; then
		:
	else
		return 1
	fi
	if ! zxfer_capture_reporting_helper_output l_fallback_lock_dir \
		zxfer_prepare_error_log_fallback_lock_dir \
		"$l_fallback_tmpdir" "$l_fallback_log_path"; then
		return 1
	fi

	printf '%s\n' "$l_fallback_lock_dir"
}

# Purpose: Acquire the error log lock so concurrent zxfer work does not reuse
# it unsafely.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before a shared cache, lock, or transport resource is used by this
# run.
zxfer_acquire_error_log_lock() {
	l_lock_dir_path=$1
	l_lock_attempts=0
	l_corrupt_metadata_sightings=0

	while ! zxfer_create_owned_lock_dir \
		"$l_lock_dir_path" lock "error-log-lock" >/dev/null; do
		if [ -L "$l_lock_dir_path" ] || [ -h "$l_lock_dir_path" ]; then
			return 1
		fi
		if [ -d "$l_lock_dir_path" ]; then
			# Missing or corrupt metadata can be a live winner inside its
			# mkdir-to-metadata publish window, so the first sighting is
			# treated as busy; the corrupt reap is allowed only when a
			# sleep-and-recheck round still reports corrupt metadata. The
			# stale-owner reap policy itself is unchanged.
			l_allow_corrupt_reap=0
			zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir_path"
			l_lock_metadata_status=$?
			if [ "$l_lock_metadata_status" -eq 2 ]; then
				l_corrupt_metadata_sightings=$((l_corrupt_metadata_sightings + 1))
				if [ "$l_corrupt_metadata_sightings" -ge 2 ]; then
					l_allow_corrupt_reap=1
				fi
			fi
			zxfer_try_reap_stale_owned_lock_dir \
				"$l_lock_dir_path" "$l_allow_corrupt_reap" lock "error-log-lock" >/dev/null
			l_reap_status=$?
			if [ "$l_reap_status" -eq 0 ]; then
				continue
			fi
			if [ "$l_reap_status" -eq 1 ]; then
				return 1
			fi
		fi
		l_lock_attempts=$((l_lock_attempts + 1))
		if [ "$l_lock_attempts" -ge 3 ]; then
			return 1
		fi
		sleep 1
	done
	return 0
}

# Purpose: Release the error log lock after the protected work finishes.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when a shared cache, lock, or transport resource should no longer be
# held.
zxfer_release_error_log_lock() {
	l_release_lock_dir=$1

	zxfer_release_owned_lock_dir \
		"$l_release_lock_dir" lock "error-log-lock"
}

# Purpose: Warn that a held error-log lock could not be released.
# Usage: Called by both release policies after the owned-lock helper returns a
# non-zero status.
# Side effects: Emits one operator-visible warning to stderr.
zxfer_warn_error_log_lock_release_failure() {
	l_log_path=$1
	l_status=$2

	zxfer_warn_stderr "zxfer: warning: unable to release ZXFER_ERROR_LOG lock for \"$l_log_path\" (status $l_status)."
}

# Purpose: Release an error-log lock without replacing the primary failure.
# Usage: Called from error paths where the append or validation failure remains
# authoritative even when lock cleanup also fails.
# Returns: Always zero after warning about any release failure.
zxfer_release_error_log_lock_warn_only() {
	l_log_path=$1
	l_lock_dir=$2

	zxfer_release_error_log_lock "$l_lock_dir"
	l_release_status=$?
	if [ "$l_release_status" -eq 0 ]; then
		return 0
	fi
	zxfer_warn_error_log_lock_release_failure "$l_log_path" "$l_release_status"
	return 0
}

# Purpose: Release an error-log lock and expose cleanup failure to the caller.
# Usage: Called after a successful append when lock release is the only
# remaining operation that can fail.
# Returns: Zero on release success and one after warning on release failure.
zxfer_release_error_log_lock_checked() {
	l_log_path=$1
	l_lock_dir=$2

	zxfer_release_error_log_lock "$l_lock_dir"
	l_release_status=$?
	if [ "$l_release_status" -eq 0 ]; then
		return 0
	fi
	zxfer_warn_error_log_lock_release_failure "$l_log_path" "$l_release_status"
	return 1
}

# Purpose: Clean up the error log stage directory that this module created or
# tracks.
# Usage: Called during failure reporting, profiling, and verbose operator
# output on success and failure paths so temporary state does not linger.
zxfer_cleanup_error_log_stage_dir() {
	l_cleanup_stage_dir=$1

	[ -n "$l_cleanup_stage_dir" ] || return 0
	zxfer_cleanup_runtime_artifact_path "$l_cleanup_stage_dir" >/dev/null 2>&1 || true
}

# Purpose: Append the failure report to existing log directly to the module-
# owned accumulator.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_failure_report_to_existing_log_directly() {
	l_direct_report=$1
	l_direct_log_path=$2

	printf '%s\n' "$l_direct_report" >>"$l_direct_log_path"
}

# Purpose: Check whether the error log parent is writable.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need a boolean answer about the error log parent.
zxfer_error_log_parent_is_writable() {
	[ -w "$1" ]
}

# Purpose: Create the error log file using the safety checks owned by this
# module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_error_log_file() {
	l_create_log_path=$1

	zxfer_create_secure_staging_dir_for_path "$l_create_log_path" "zxfer-error-log" >/dev/null || return 1
	l_create_stage_dir=$g_zxfer_secure_staging_dir_result
	l_create_stage_file="$l_create_stage_dir/log.write"

	if ! (
		umask 077
		zxfer_write_runtime_artifact_file "$l_create_stage_file" ""
	); then
		zxfer_cleanup_error_log_stage_dir "$l_create_stage_dir"
		return 1
	fi
	if ! mv -f "$l_create_stage_file" "$l_create_log_path"; then
		zxfer_cleanup_error_log_stage_dir "$l_create_stage_dir"
		return 1
	fi
	zxfer_cleanup_error_log_stage_dir "$l_create_stage_dir"
}

# Purpose: Apply the required permissions to the error log file.
# Usage: Called during failure reporting, profiling, and verbose operator
# output after a file is created so later reads honor zxfer's security
# expectations.
zxfer_chmod_error_log_file() {
	l_chmod_log_path=$1

	chmod 600 "$l_chmod_log_path"
}

# Purpose: Validate an error-log target and print its trusted physical parent.
# Usage: Called before lock selection so every append path shares the same
# absolute-path, symlink, ownership, and mode checks.
# Returns: The trusted parent path on stdout, or non-zero after warning.
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

# Purpose: Select the lock directory for one validated error-log target.
# Usage: Writable parents use a path-adjacent lock; existing files in a
# non-writable parent use the private fallback-lock location.
# Returns: The selected lock directory on stdout, or non-zero.
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

# Purpose: Validate or securely create the error-log file while its lock is held.
# Usage: Called after lock acquisition and before either direct append or
# path-adjacent atomic replacement.
# Returns: Zero when the locked target is safe and ready; non-zero after warning.
zxfer_prepare_locked_error_log_file() {
	l_error_log_prepare_path=$1
	l_error_log_prepare_existed=$2

	# A concurrent holder may have created the log while this run waited on
	# the lock; recheck existence under the lock so the create path cannot
	# clobber a freshly published log with an empty staged file.
	if [ "$l_error_log_prepare_existed" -eq 0 ] && [ -e "$l_error_log_prepare_path" ]; then
		l_error_log_prepare_existed=1
	fi

	if [ "$l_error_log_prepare_existed" -eq 1 ]; then
		zxfer_validate_existing_error_log_file "$l_error_log_prepare_path" \
			"$l_error_log_prepare_path"
		return $?
	fi

	if ! zxfer_create_error_log_file "$l_error_log_prepare_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_error_log_prepare_path\"."
		return 1
	fi
	if ! zxfer_chmod_error_log_file "$l_error_log_prepare_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_error_log_prepare_path\" to 0600."
		return 1
	fi
	zxfer_validate_existing_error_log_file "$l_error_log_prepare_path" \
		"$l_error_log_prepare_path"
}

# Purpose: Append a report by atomically replacing a validated writable log.
# Usage: Called with the error-log lock held; this helper always releases the
# lock and removes its staging directory before returning.
# Returns: Zero after a published append, otherwise non-zero after warning.
zxfer_append_failure_report_with_atomic_replace() {
	l_error_log_atomic_report=$1
	l_error_log_atomic_path=$2
	l_error_log_atomic_lock_dir=$3

	if ! zxfer_create_secure_staging_dir_for_path "$l_error_log_atomic_path" "zxfer-error-log" >/dev/null; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG staging directory for \"$l_error_log_atomic_path\"."
		zxfer_release_error_log_lock_warn_only "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
		return 1
	fi
	l_error_log_atomic_stage_dir=$g_zxfer_secure_staging_dir_result
	l_error_log_atomic_snapshot_path="$l_error_log_atomic_stage_dir/log.snapshot"
	l_error_log_atomic_staged_path="$l_error_log_atomic_stage_dir/log.write"
	if ! ln "$l_error_log_atomic_path" "$l_error_log_atomic_snapshot_path" 2>/dev/null; then
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_error_log_atomic_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_error_log_atomic_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
		return 1
	fi
	if ! zxfer_validate_existing_error_log_file "$l_error_log_atomic_snapshot_path" "$l_error_log_atomic_path"; then
		zxfer_cleanup_error_log_stage_dir "$l_error_log_atomic_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
		return 1
	fi

	l_error_log_atomic_old_umask=$(umask)
	umask 077
	if ! cat "$l_error_log_atomic_snapshot_path" >"$l_error_log_atomic_staged_path" ||
		! printf '%s\n' "$l_error_log_atomic_report" >>"$l_error_log_atomic_staged_path"; then
		umask "$l_error_log_atomic_old_umask"
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_error_log_atomic_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_error_log_atomic_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
		return 1
	fi
	umask "$l_error_log_atomic_old_umask"

	if ! zxfer_chmod_error_log_file "$l_error_log_atomic_staged_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_error_log_atomic_path\" to 0600."
		zxfer_cleanup_error_log_stage_dir "$l_error_log_atomic_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
		return 1
	fi
	if ! mv -f "$l_error_log_atomic_staged_path" "$l_error_log_atomic_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_error_log_atomic_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_error_log_atomic_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
		return 1
	fi

	zxfer_cleanup_error_log_stage_dir "$l_error_log_atomic_stage_dir"
	zxfer_release_error_log_lock_checked "$l_error_log_atomic_path" "$l_error_log_atomic_lock_dir"
}

# Purpose: Append the failure report to log to the module-owned accumulator.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_failure_report_to_log() {
	l_report=$1
	l_log_path=${ZXFER_ERROR_LOG:-}

	[ -n "$l_log_path" ] || return 0
	if ! l_trusted_log_parent=$(zxfer_get_trusted_error_log_parent "$l_log_path"); then
		return 1
	fi

	l_log_exists=0
	if [ -e "$l_log_path" ]; then
		l_log_exists=1
	fi
	l_log_parent_writable=0
	if zxfer_error_log_parent_is_writable "$l_trusted_log_parent"; then
		l_log_parent_writable=1
	fi

	if [ "$l_log_exists" -eq 0 ] && [ "$l_log_parent_writable" -eq 0 ]; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_log_path\"."
		return 1
	fi

	if ! l_lock_dir=$(zxfer_get_error_log_lock_dir "$l_log_path" \
		"$l_trusted_log_parent" "$l_log_exists" "$l_log_parent_writable"); then
		zxfer_warn_stderr "zxfer: warning: unable to acquire ZXFER_ERROR_LOG lock for \"$l_log_path\"."
		return 1
	fi
	if ! zxfer_acquire_error_log_lock "$l_lock_dir"; then
		zxfer_warn_stderr "zxfer: warning: unable to acquire ZXFER_ERROR_LOG lock for \"$l_log_path\"."
		return 1
	fi

	if ! zxfer_prepare_locked_error_log_file "$l_log_path" "$l_log_exists"; then
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi

	if [ "$l_log_parent_writable" -eq 0 ]; then
		if ! zxfer_append_failure_report_to_existing_log_directly "$l_report" "$l_log_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
			zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
			return 1
		fi
		zxfer_release_error_log_lock_checked "$l_log_path" "$l_lock_dir"
		return "$?"
	fi

	zxfer_append_failure_report_with_atomic_replace "$l_report" "$l_log_path" "$l_lock_dir"
}

# Purpose: Emit the failure report in the operator-facing format owned by this
# module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to surface status, warning, or diagnostic text.
zxfer_emit_failure_report() {
	l_exit_status=$1

	zxfer_init_failure_context_defaults

	[ "$l_exit_status" -ne 0 ] || return 0
	[ "${g_zxfer_failure_report_emitted:-0}" -eq 0 ] || return 0

	l_report=$(zxfer_render_failure_report "$l_exit_status")
	printf '%s\n' "$l_report" >&2
	zxfer_mark_failure_report_emitted
	zxfer_append_failure_report_to_log "$l_report" || true
}
