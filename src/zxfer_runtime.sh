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
# owns globals: temp-root selection, cleanup-PID tracking, and runtime-artifact
#   allocation, readback, registry, and cleanup state.
# reads globals: TMPDIR, yield-iteration state, and resolved base helper paths.
# mutates caches: runtime artifact and cleanup-PID registries only.
# returns via stdout: temp paths and source-to-destination mappings.

ZXFER_MAX_YIELD_ITERATIONS=8
ZXFER_CACHE_OBJECT_HEADER_LINE="ZXFER_CACHE_OBJECT_V1"
ZXFER_CACHE_OBJECT_END_LINE="ZXFER_CACHE_OBJECT_END"

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
	g_zxfer_cleanup_pid_abort_grace_seconds=2
}

# Purpose: Publish an aggregate cleanup-helper abort diagnostic.
# Usage: Domain cleanup paths call this after preserving the first failed
# direct-child teardown message.
zxfer_set_cleanup_pid_abort_failure_message() {
	g_zxfer_cleanup_pid_abort_failure_message=${1:-}
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

# Purpose: Append one zxfer-owned direct-child PID and purpose to runtime's
# cleanup registry.
# Usage: Registration and failed immediate-abort recovery share this owner-only
# state mutation. Rows are (pid, purpose).
zxfer_track_cleanup_pid_record() {
	l_cleanup_track_pid=$1
	l_cleanup_track_purpose=$2

	if [ -n "${g_zxfer_cleanup_pid_records:-}" ]; then
		g_zxfer_cleanup_pid_records=$g_zxfer_cleanup_pid_records"
$l_cleanup_track_pid	$l_cleanup_track_purpose"
	else
		g_zxfer_cleanup_pid_records="$l_cleanup_track_pid	$l_cleanup_track_purpose"
	fi
	if [ -n "${g_zxfer_cleanup_pids:-}" ]; then
		g_zxfer_cleanup_pids="$g_zxfer_cleanup_pids $l_cleanup_track_pid"
	else
		g_zxfer_cleanup_pids=$l_cleanup_track_pid
	fi
}

# Purpose: Register the cleanup helper with the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup so cleanup
# and later lookups can find the live resource. Rows are (pid, purpose).
# SAFETY: callers register only a zxfer-owned `$!` direct child and remove the
# record immediately after their one explicit wait. This is the pre-refactor
# supervision-lite tradeoff; it avoids process snapshots but does not claim
# that every POSIX shell delays internal reap until that wait call.
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
	zxfer_track_cleanup_pid_record \
		"$l_cleanup_register_pid" "$l_cleanup_register_purpose"
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

# Purpose: Signal one zxfer-owned direct child helper before its caller waits.
# Usage: Called by callers that spawned a helper but could not register it (or
# registered it elsewhere) and must stop it before failing.
# Helpers with descendants are spawned through the cleanup child wrapper,
# whose TERM trap reaps the rest of the tree.
zxfer_abort_direct_child_pid() {
	l_cleanup_direct_abort_pid=$1
	l_cleanup_direct_abort_signal=${2:-TERM}
	l_cleanup_direct_abort_purpose=${3:-cleanup helper}
	l_cleanup_direct_abort_tracked=0

	g_zxfer_cleanup_pid_abort_failure_message=""
	case "$l_cleanup_direct_abort_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	[ "$l_cleanup_direct_abort_pid" = "$$" ] && return 1

	l_cleanup_direct_abort_purpose=$(zxfer_normalize_owned_lock_text_field "$l_cleanup_direct_abort_purpose") ||
		return "$?"
	if zxfer_find_cleanup_pid_record "$l_cleanup_direct_abort_pid"; then
		l_cleanup_direct_abort_tracked=1
	fi
	if ! kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null; then
		return 0
	fi
	if kill -s "$l_cleanup_direct_abort_signal" "$l_cleanup_direct_abort_pid" 2>/dev/null; then
		return 0
	fi
	if ! kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null; then
		return 0
	fi
	g_zxfer_cleanup_pid_abort_failure_message="Failed to signal cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid)."
	# Retain the owned direct-child handle under runtime for ordered trap retry.
	if [ "$l_cleanup_direct_abort_tracked" -eq 0 ]; then
		zxfer_track_cleanup_pid_record \
			"$l_cleanup_direct_abort_pid" "$l_cleanup_direct_abort_purpose"
	fi
	return 1
}

# Purpose: Signal one registered cleanup helper before its owner waits.
# Usage: Called during shutdown and failure handling. Untracked PIDs return
# success; the record remains owned until an explicit wait and unregister.
# SAFETY: direct-child records retain the baseline `$!`/registered/no-user-wait
# invariant and receive a liveness check immediately before signalling,
# without a normal-path process-table spawn.
zxfer_abort_cleanup_pid() {
	l_cleanup_abort_pid=$1
	l_cleanup_abort_signal=${2:-TERM}

	g_zxfer_cleanup_pid_abort_failure_message=""
	zxfer_find_cleanup_pid_record "$l_cleanup_abort_pid" || return 0

	l_cleanup_abort_purpose=$g_zxfer_cleanup_pid_record_purpose
	if ! kill -s 0 "$l_cleanup_abort_pid" 2>/dev/null; then
		return 0
	fi
	if kill -s "$l_cleanup_abort_signal" "$l_cleanup_abort_pid" 2>/dev/null; then
		return 0
	fi
	if ! kill -s 0 "$l_cleanup_abort_pid" 2>/dev/null; then
		return 0
	fi
	g_zxfer_cleanup_pid_abort_failure_message="Failed to signal cleanup helper [$l_cleanup_abort_purpose] (PID $l_cleanup_abort_pid)."
	return 1
}

# Purpose: Give registered cleanup helpers one bounded opportunity to finish
# their TERM handling before shutdown escalates survivors with KILL.
# Usage: Called once per aggregate cleanup pass, never on normal execution
# paths. Suites set the internal grace value to 0 for deterministic speed.
zxfer_cleanup_pid_abort_grace_wait() {
	case "${g_zxfer_cleanup_pid_abort_grace_seconds:-2}" in
	0)
		:
		;;
	'' | *[!0-9]*)
		sleep 2
		;;
	*)
		sleep "$g_zxfer_cleanup_pid_abort_grace_seconds"
		;;
	esac
	return 0
}

# Purpose: Stop the registered cleanup helpers that this module still tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# shutdown or failure handling must stop background work that should not
# survive the current run.
zxfer_kill_registered_cleanup_pids() {
	l_cleanup_kill_abort_status=0
	l_cleanup_kill_first_failure_message=""
	l_cleanup_kill_tracked_pids=${g_zxfer_cleanup_pids:-}

	# TERM every owned helper first so their wrapper traps can clean up
	# descendants concurrently. A TERM delivery failure is provisional: KILL
	# below may still terminate the same owned direct child safely.
	for l_cleanup_kill_pid in $l_cleanup_kill_tracked_pids; do
		case "$l_cleanup_kill_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		[ "$l_cleanup_kill_pid" = "$$" ] && continue
		zxfer_abort_cleanup_pid "$l_cleanup_kill_pid" TERM >/dev/null 2>&1 || :
	done

	[ -z "$l_cleanup_kill_tracked_pids" ] ||
		zxfer_cleanup_pid_abort_grace_wait

	# Escalate every survivor once, then reap and unregister it. Only a helper
	# that remains live after KILL failure is retained and reported; this keeps
	# trap shutdown bounded without promoting a recoverable TERM failure.
	for l_cleanup_kill_pid in $l_cleanup_kill_tracked_pids; do
		case "$l_cleanup_kill_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		[ "$l_cleanup_kill_pid" = "$$" ] && continue
		if kill -s 0 "$l_cleanup_kill_pid" 2>/dev/null; then
			l_cleanup_kill_status=0
			zxfer_abort_cleanup_pid "$l_cleanup_kill_pid" KILL ||
				l_cleanup_kill_status=$?
			if [ "$l_cleanup_kill_status" -ne 0 ] &&
				kill -s 0 "$l_cleanup_kill_pid" 2>/dev/null; then
				[ -n "$l_cleanup_kill_first_failure_message" ] ||
					l_cleanup_kill_first_failure_message=$g_zxfer_cleanup_pid_abort_failure_message
				[ "$l_cleanup_kill_abort_status" -ne 0 ] ||
					l_cleanup_kill_abort_status=$l_cleanup_kill_status
				continue
			fi
		fi
		wait "$l_cleanup_kill_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$l_cleanup_kill_pid"
	done

	if [ "$l_cleanup_kill_abort_status" -eq 0 ]; then
		g_zxfer_cleanup_pid_abort_failure_message=""
	fi
	if [ -n "$l_cleanup_kill_first_failure_message" ]; then
		g_zxfer_cleanup_pid_abort_failure_message=$l_cleanup_kill_first_failure_message
	fi
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

# Purpose: Discard inherited cleanup handles without acting on any referenced
# process or path.
# Usage: Called before session traps are installed and by runtime bootstrap so
# exported shell variables can never grant cleanup ownership.
zxfer_discard_runtime_cleanup_state() {
	zxfer_reset_cleanup_pid_tracking
	g_zxfer_run_tmp_root=""
	g_zxfer_owned_run_tmp_root=""
	g_zxfer_owned_run_tmp_root_parent=""
	g_zxfer_owned_run_tmp_root_identity=""
	g_zxfer_run_tmp_counter=0
	g_zxfer_runtime_artifact_cleanup_paths=""
	g_zxfer_runtime_artifact_cleanup_dir_identities=""
	g_zxfer_runtime_artifact_path_result=""
	g_zxfer_runtime_artifact_read_result=""
	g_zxfer_runtime_artifact_directory_identity_result=""
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_tmpdir_fallback_note=""
	g_zxfer_temp_file_result=""
	g_zxfer_temp_file_group_result=""
}

# Purpose: Check whether a run-root handle matches the exact path published by
# this runtime after a successful mktemp allocation.
# Usage: Artifact cleanup requires this identity before treating descendants as
# owned; session bootstrap clears inherited copies before any allocation.
zxfer_run_tmp_root_has_owner_identity() {
	l_run_root_identity_path=$1

	[ -n "$l_run_root_identity_path" ] || return 1
	[ "$l_run_root_identity_path" = "${g_zxfer_owned_run_tmp_root:-}" ]
}

# Purpose: Validate the lexical shape and trusted parent recorded for the
# exact per-run root created by this process.
# Usage: Whole-root recursive removal calls this immediately before rm -rf.
zxfer_run_tmp_root_has_safe_owned_shape() {
	l_owned_root_shape_path=$1
	l_owned_root_shape_parent=${g_zxfer_owned_run_tmp_root_parent:-}

	zxfer_run_tmp_root_has_owner_identity "$l_owned_root_shape_path" || return 1
	case "$l_owned_root_shape_parent" in
	/*) ;;
	*) return 1 ;;
	esac
	if [ "$l_owned_root_shape_parent" = "/" ]; then
		case "$l_owned_root_shape_path" in
		/*) ;;
		*) return 1 ;;
		esac
		l_owned_root_shape_name=${l_owned_root_shape_path#/}
	else
		case "$l_owned_root_shape_path" in
		"$l_owned_root_shape_parent"/*) ;;
		*) return 1 ;;
		esac
		l_owned_root_shape_name=${l_owned_root_shape_path#"$l_owned_root_shape_parent"/}
	fi
	case "$l_owned_root_shape_name" in
	"zxfer.$$."?*) ;;
	*) return 1 ;;
	esac
	case "$l_owned_root_shape_name" in
	*/* | *'
'*) return 1 ;;
	esac
	return 0
}

# Purpose: Revalidate that the exact run root allocated by this process is
# still a real, mode-0700 directory owned by the effective user.
# Usage: Called at each recursive-cleanup boundary; lexical owner globals alone
# never authorize traversal through a pathname that has since been replaced.
zxfer_run_tmp_root_is_current_private_dir() {
	l_private_root_path=$1

	zxfer_run_tmp_root_has_safe_owned_shape "$l_private_root_path" || return 1
	[ -d "$l_private_root_path" ] || return 1
	[ ! -L "$l_private_root_path" ] || return 1
	[ ! -h "$l_private_root_path" ] || return 1
	l_private_root_effective_uid=$(zxfer_get_effective_user_uid) || return 1
	l_private_root_security_record=$(zxfer_get_private_directory_security_record \
		"$l_private_root_path") || return 1
	IFS='	' read -r l_private_root_current_identity \
		l_private_root_owner_uid l_private_root_mode <<-EOF
			$l_private_root_security_record
		EOF
	[ -n "$l_private_root_current_identity" ] || return 1
	[ -n "$l_private_root_owner_uid" ] || return 1
	[ -n "$l_private_root_mode" ] || return 1
	[ "$l_private_root_owner_uid" = "$l_private_root_effective_uid" ] || return 1
	[ "$l_private_root_mode" = "700" ] || return 1
	[ -n "${g_zxfer_owned_run_tmp_root_identity:-}" ] || return 1
	[ "$l_private_root_current_identity" = "$g_zxfer_owned_run_tmp_root_identity" ]
}

# Purpose: Reset the runtime artifact state so the next runtime pass starts
# from a clean state.
# Usage: Called during staging and trap cleanup after owner initialization;
# unlike zxfer_discard_runtime_cleanup_state, this removes owned resources.
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
	if [ -n "${g_zxfer_run_tmp_root:-}" ]; then
		if zxfer_run_tmp_root_is_current_private_dir "$g_zxfer_run_tmp_root"; then
			return 0
		fi
		# Never adopt a directory merely because an internal-looking global was
		# inherited or overwritten. The session discard path clears such state
		# before normal startup; later inconsistencies fail closed.
		return 1
	fi
	[ -z "${g_zxfer_owned_run_tmp_root:-}" ] || return 1
	[ -z "${g_zxfer_owned_run_tmp_root_parent:-}" ] || return 1
	[ -z "${g_zxfer_owned_run_tmp_root_identity:-}" ] || return 1

	# Plain call (no command substitution) so the once-per-run validation
	# memoizes in this shell and a held unsafe-TMPDIR fallback advisory
	# survives until option parsing can emit it.
	zxfer_try_get_effective_tmpdir >/dev/null || return "$?"
	l_effective_tmpdir=$g_zxfer_effective_tmpdir
	l_revalidated_effective_tmpdir=$(zxfer_validate_temp_root_candidate \
		"$l_effective_tmpdir") || return 1
	[ "$l_revalidated_effective_tmpdir" = "$l_effective_tmpdir" ] || return 1

	l_old_umask=$(umask)
	umask 077
	l_status=0
	if [ "$l_effective_tmpdir" = "/" ]; then
		l_run_tmp_template="/zxfer.$$.XXXXXX"
	else
		l_run_tmp_template="$l_effective_tmpdir/zxfer.$$.XXXXXX"
	fi
	l_run_tmp_root=$(mktemp -d "$l_run_tmp_template" 2>/dev/null) ||
		l_status=$?
	umask "$l_old_umask"
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_run_tmp_root_identity=$(zxfer_get_path_device_inode "$l_run_tmp_root") || {
		rmdir "$l_run_tmp_root" 2>/dev/null || :
		return 1
	}

	g_zxfer_run_tmp_root=$l_run_tmp_root
	g_zxfer_owned_run_tmp_root=$l_run_tmp_root
	g_zxfer_owned_run_tmp_root_parent=$l_effective_tmpdir
	g_zxfer_owned_run_tmp_root_identity=$l_run_tmp_root_identity
	g_zxfer_run_tmp_counter=0
	return 0
}

# Purpose: Remove the per-run temp root and every runtime artifact below it.
# Usage: Called from runtime state resets and zxfer_trap_exit so one rm -rf
# covers success and failure paths.
zxfer_remove_run_tmp_root() {
	l_run_tmp_root=${g_zxfer_run_tmp_root:-}

	if [ -z "$l_run_tmp_root" ]; then
		[ -z "${g_zxfer_owned_run_tmp_root:-}" ] || return 1
		[ -z "${g_zxfer_owned_run_tmp_root_parent:-}" ] || return 1
		[ -z "${g_zxfer_owned_run_tmp_root_identity:-}" ] || return 1
		return 0
	fi
	zxfer_run_tmp_root_has_safe_owned_shape "$l_run_tmp_root" || return 1
	if [ ! -e "$l_run_tmp_root" ] && [ ! -L "$l_run_tmp_root" ] && [ ! -h "$l_run_tmp_root" ]; then
		g_zxfer_run_tmp_root=""
		g_zxfer_owned_run_tmp_root=""
		g_zxfer_owned_run_tmp_root_parent=""
		g_zxfer_owned_run_tmp_root_identity=""
		return 0
	fi
	zxfer_run_tmp_root_is_current_private_dir "$l_run_tmp_root" || return 1
	if rm -rf "$l_run_tmp_root" 2>/dev/null ||
		{ [ ! -e "$l_run_tmp_root" ] && [ ! -L "$l_run_tmp_root" ]; }; then
		g_zxfer_run_tmp_root=""
		g_zxfer_owned_run_tmp_root=""
		g_zxfer_owned_run_tmp_root_parent=""
		g_zxfer_owned_run_tmp_root_identity=""
		zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
		return 0
	fi

	return 1
}

# Purpose: Validate the lexical shape reserved for exact path-adjacent runtime
# staging entries.
# Usage: Registration and trap cleanup share this check so a corrupted registry
# cannot widen recursive deletion to an arbitrary path.
zxfer_runtime_artifact_registration_path_has_safe_shape() {
	l_registration_shape_path=$1

	case "$l_registration_shape_path" in
	/*) ;;
	*) return 1 ;;
	esac
	case "$l_registration_shape_path" in
	*'
'*) return 1 ;;
	esac
	l_registration_shape_name=${l_registration_shape_path##*/}
	case "$l_registration_shape_name" in
	zxfer.* | .zxfer-* | .zxfer.*) ;;
	*) return 1 ;;
	esac
	return 0
}

# Purpose: Validate a newly created path-adjacent staging entry before the
# runtime registry accepts cleanup ownership.
# Usage: Registration requires a regular file or directory, never a symlink.
zxfer_runtime_artifact_registration_path_is_safe() {
	l_registration_path=$1

	zxfer_runtime_artifact_registration_path_has_safe_shape "$l_registration_path" || return 1
	[ ! -L "$l_registration_path" ] || return 1
	[ ! -h "$l_registration_path" ] || return 1
	[ -f "$l_registration_path" ] || [ -d "$l_registration_path" ]
}

# Purpose: Check whether a path is an exact registered adjacent artifact.
# Usage: Cleanup uses this membership test before accepting a path outside the
# private run root.
zxfer_runtime_artifact_path_is_registered() {
	l_artifact_path=$1

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_artifact_path" ] && return 0
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF
	return 1
}

# Purpose: Publish the registration-time filesystem identity for one
# path-adjacent directory.
# Usage: Recursive cleanup compares this value to the current object. Records
# use alternating identity/path lines, preserving path whitespace without eval
# or field splitting.
zxfer_get_registered_runtime_artifact_directory_identity() {
	l_registered_identity_path=$1
	l_registered_identity_pending=""
	g_zxfer_runtime_artifact_directory_identity_result=""

	while IFS= read -r l_registered_identity_line || [ -n "$l_registered_identity_line" ]; do
		if [ -z "$l_registered_identity_pending" ]; then
			[ -n "$l_registered_identity_line" ] || return 1
			l_registered_identity_pending=$l_registered_identity_line
			continue
		fi
		if [ "$l_registered_identity_line" = "$l_registered_identity_path" ]; then
			g_zxfer_runtime_artifact_directory_identity_result=$l_registered_identity_pending
			return 0
		fi
		l_registered_identity_pending=""
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_dir_identities:-}
EOF

	return 1
}

# Purpose: Remove one directory identity from runtime's adjacent-artifact
# registry while retaining every other exact identity/path pair.
# Usage: Called whenever the matching path is unregistered.
zxfer_unregister_runtime_artifact_directory_identity() {
	l_unregister_identity_path=$1
	l_unregister_identity_pending=""
	l_unregister_identity_remaining=""

	while IFS= read -r l_unregister_identity_line || [ -n "$l_unregister_identity_line" ]; do
		if [ -z "$l_unregister_identity_pending" ]; then
			[ -n "$l_unregister_identity_line" ] || continue
			l_unregister_identity_pending=$l_unregister_identity_line
			continue
		fi
		if [ "$l_unregister_identity_line" != "$l_unregister_identity_path" ]; then
			if [ -n "$l_unregister_identity_remaining" ]; then
				l_unregister_identity_remaining=$l_unregister_identity_remaining'
'$l_unregister_identity_pending'
'$l_unregister_identity_line
			else
				l_unregister_identity_remaining=$l_unregister_identity_pending'
'$l_unregister_identity_line
			fi
		fi
		l_unregister_identity_pending=""
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_dir_identities:-}
EOF

	g_zxfer_runtime_artifact_cleanup_dir_identities=$l_unregister_identity_remaining
}

# Purpose: Check whether a path is one direct child of the private run root.
# Usage: Runtime allocations are deliberately flat; contained workspaces own
# their descendants and are removed as one direct-child directory.
zxfer_runtime_artifact_path_is_run_root_child() {
	l_artifact_path=$1
	l_run_tmp_root=${g_zxfer_run_tmp_root:-}

	[ -n "$l_run_tmp_root" ] || return 1
	zxfer_run_tmp_root_is_current_private_dir "$l_run_tmp_root" || return 1
	case "$l_artifact_path" in
	"$l_run_tmp_root"/*)
		l_artifact_name=${l_artifact_path#"$l_run_tmp_root"/}
		case "$l_artifact_name" in
		'' | '.' | '..' | */*) return 1 ;;
		esac
		return 0
		;;
	esac
	return 1
}

# Purpose: Register one validated path-adjacent staging artifact for exact
# trap cleanup.
# Usage: Callers must have just created the safely named file or directory.
zxfer_register_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0
	zxfer_runtime_artifact_registration_path_is_safe "$l_artifact_path" || return 1

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_artifact_path" ] && return 0
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF
	if [ -d "$l_artifact_path" ]; then
		l_registration_identity=$(zxfer_get_path_device_inode "$l_artifact_path") || return 1
		if [ -n "${g_zxfer_runtime_artifact_cleanup_dir_identities:-}" ]; then
			g_zxfer_runtime_artifact_cleanup_dir_identities=$g_zxfer_runtime_artifact_cleanup_dir_identities'
'$l_registration_identity'
'$l_artifact_path
		else
			g_zxfer_runtime_artifact_cleanup_dir_identities=$l_registration_identity'
'$l_artifact_path
		fi
	fi

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
	zxfer_unregister_runtime_artifact_directory_identity "$l_artifact_path"
}

# Purpose: Clean up the runtime artifact path that this module created or
# tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_runtime_artifact_path() {
	l_artifact_path=$1
	l_runtime_cleanup_is_registered=0

	[ -n "$l_artifact_path" ] || return 0
	if zxfer_runtime_artifact_path_is_run_root_child "$l_artifact_path"; then
		:
	elif zxfer_runtime_artifact_path_is_registered "$l_artifact_path"; then
		l_runtime_cleanup_is_registered=1
	else
		return 1
	fi
	if { [ -L "$l_artifact_path" ] || [ -h "$l_artifact_path" ]; } &&
		rm -f "$l_artifact_path" 2>/dev/null; then
		l_runtime_cleanup_removed=1
	elif [ -d "$l_artifact_path" ]; then
		if [ "$l_runtime_cleanup_is_registered" -eq 1 ]; then
			zxfer_get_registered_runtime_artifact_directory_identity \
				"$l_artifact_path" || return 1
			l_runtime_cleanup_registered_identity=$g_zxfer_runtime_artifact_directory_identity_result
			l_runtime_cleanup_current_identity=$(zxfer_get_path_device_inode \
				"$l_artifact_path") || return 1
			[ "$l_runtime_cleanup_current_identity" = "$l_runtime_cleanup_registered_identity" ] || return 1
		fi
		if rm -rf "$l_artifact_path" 2>/dev/null; then
			l_runtime_cleanup_removed=1
		else
			l_runtime_cleanup_removed=0
		fi
	elif [ -e "$l_artifact_path" ] && rm -f "$l_artifact_path" 2>/dev/null; then
		l_runtime_cleanup_removed=1
	elif [ ! -e "$l_artifact_path" ] &&
		[ ! -L "$l_artifact_path" ] && [ ! -h "$l_artifact_path" ]; then
		l_runtime_cleanup_removed=1
	else
		l_runtime_cleanup_removed=0
	fi
	if [ "$l_runtime_cleanup_removed" -eq 1 ]; then
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
	l_registered_artifact_paths=${g_zxfer_runtime_artifact_cleanup_paths:-}
	l_registered_artifact_status=0

	while IFS= read -r l_registered_cleanup_path || [ -n "$l_registered_cleanup_path" ]; do
		[ -n "$l_registered_cleanup_path" ] || continue
		zxfer_runtime_artifact_registration_path_has_safe_shape "$l_registered_cleanup_path" || {
			l_registered_artifact_status=1
			continue
		}
		zxfer_cleanup_runtime_artifact_path "$l_registered_cleanup_path" ||
			l_registered_artifact_status=1
	done <<EOF
$l_registered_artifact_paths
EOF

	return "$l_registered_artifact_status"
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
	case "$l_prefix" in
	'' | '.' | '..' | *[!A-Za-z0-9._-]*) return 1 ;;
	esac
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
	case "$l_prefix" in
	'' | '.' | '..' | *[!A-Za-z0-9._-]*) return 1 ;;
	esac
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

# Purpose: Return the max yield iterations in the form expected by later
# helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_max_yield_iterations() {
	printf '%s\n' "$ZXFER_MAX_YIELD_ITERATIONS"
}

# Purpose: Reset the runtime-owned lifecycle state for a new session.
# Usage: Called by the session composition root before artifact allocation.
# Side effects: Clears cleanup tracking, temp-root selection, and the
# runtime-artifact registry state.
zxfer_init_runtime_state_defaults() {
	# Startup state may be inherited from an exported caller environment. Drop
	# every handle without cleanup first; only paths allocated after this point
	# can acquire the private owner identity used by recursive removal.
	zxfer_discard_runtime_cleanup_state
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
