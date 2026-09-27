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
# owns globals: temp-root selection (the g_zxfer_effective_tmpdir memo and
#   g_zxfer_default_tmpdir_result); the per-run temp root, its validated
#   parent and its creation record (g_zxfer_owned_run_tmp_root_identity:
#   identity, owner and mode); the caller umask recorded with the root
#   (g_zxfer_run_umask); the cleanup-PID rows (g_zxfer_cleanup_pid_records);
#   the path-adjacent artifact registry (g_zxfer_runtime_artifact_cleanup_paths);
#   the allocation and readback results (g_zxfer_temp_file_result,
#   g_zxfer_temp_file_group_result, g_zxfer_staging_dir_result,
#   g_zxfer_runtime_artifact_*_result).
# reads globals: TMPDIR, g_option_V_very_verbose, ZXFER_SOURCE_MODULES_ROOT
#   (the cleanup wrapper path), and ZXFER_TAB/ZXFER_LF from zxfer_quoting.sh.
# mutates caches: the cleanup-PID rows and the adjacent-artifact registry.
# returns via stdout: the cleanup wrapper path and the default temp-directory
#   candidates only; every temp and staging path is a result global.

ZXFER_MAX_YIELD_ITERATIONS=8

################################################################################
# CLEANUP-PID ROWS
################################################################################

# Each row of g_zxfer_cleanup_pid_records is PID<TAB>PURPOSE<TAB>SCOPE: a
# zxfer-owned direct child (pid), a separate process group (pgid), or the
# descendant cleanup wrapper (wrapper).

# Purpose: Clear the cleanup-PID rows and the last abort result.
# Usage: zxfer_reset_cleanup_pid_tracking; drops rows without signalling.
zxfer_reset_cleanup_pid_tracking() {
	g_zxfer_cleanup_pid_records=""
	g_zxfer_cleanup_pid_record_purpose=""
	g_zxfer_cleanup_pid_record_scope=""
	g_zxfer_cleanup_pid_abort_failure_message=""
	g_zxfer_cleanup_pid_abort_grace_seconds=2
}

# Purpose: Locate the fallback child wrapper shipped beside the modules.
# Usage: Background spawning and cold descendant teardown only.
zxfer_get_cleanup_child_wrapper_script_path() {
	l_cleanup_child_wrapper_script="${ZXFER_SOURCE_MODULES_ROOT:-.}/src/zxfer_cleanup_child_wrapper.sh"
	[ -r "$l_cleanup_child_wrapper_script" ] || return 1
	printf '%s\n' "$l_cleanup_child_wrapper_script"
}

# Purpose: Find one tracked cleanup-helper row by PID.
# Usage: zxfer_find_cleanup_pid_record PID; publishes the row's purpose and
# signal scope in g_zxfer_cleanup_pid_record_purpose and
# g_zxfer_cleanup_pid_record_scope, or returns 1.
zxfer_find_cleanup_pid_record() {
	l_cleanup_find_pid=$1

	g_zxfer_cleanup_pid_record_purpose=""
	g_zxfer_cleanup_pid_record_scope=""

	while IFS='	' read -r l_cleanup_find_record_pid l_cleanup_find_record_purpose l_cleanup_find_record_scope || [ -n "${l_cleanup_find_record_pid}${l_cleanup_find_record_purpose}" ]; do
		[ -n "$l_cleanup_find_record_pid" ] || continue
		[ "$l_cleanup_find_record_pid" = "$l_cleanup_find_pid" ] || continue
		g_zxfer_cleanup_pid_record_purpose=$l_cleanup_find_record_purpose
		g_zxfer_cleanup_pid_record_scope=${l_cleanup_find_record_scope:-pid}
		return 0
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF

	return 1
}

# Purpose: Append one cleanup-PID row.
# Usage: zxfer_track_cleanup_pid_record PID PURPOSE [SCOPE]; callers have
# already validated PID and PURPOSE. SCOPE defaults to pid.
zxfer_track_cleanup_pid_record() {
	g_zxfer_cleanup_pid_records=${g_zxfer_cleanup_pid_records:+$g_zxfer_cleanup_pid_records$ZXFER_LF}$1$ZXFER_TAB$2$ZXFER_TAB${3:-pid}
}

# Purpose: Add a cleanup-PID row for one live zxfer-owned child.
# Usage: zxfer_register_cleanup_pid PID [PURPOSE] [SCOPE]; called right after
# a helper is spawned. SCOPE is pgid for a child spawned through
# zxfer_spawn_background_shell as its own process group, wrapper for fallback
# shells, otherwise pid. An invalid, own, dead or already tracked PID is
# ignored; a PURPOSE with a tab or line break returns 1.
# SAFETY: callers register only a zxfer-owned `$!` direct child and remove the
# record after waiting and completing any failure teardown. This preserves
# the supervision-lite tradeoff: it avoids process snapshots but does not claim
# that every POSIX shell delays internal reap until that wait call.
zxfer_register_cleanup_pid() {
	l_cleanup_register_pid=$1
	l_cleanup_register_purpose=${2:-cleanup helper}
	l_cleanup_register_scope=${3:-pid}

	zxfer_is_uint "$l_cleanup_register_pid" || return 0
	[ "$l_cleanup_register_pid" = "$$" ] && return 0

	zxfer_value_is_single_line "$l_cleanup_register_purpose" || return 1
	if zxfer_find_cleanup_pid_record "$l_cleanup_register_pid"; then
		return 0
	fi
	# A group can outlive its leader; immediately after launch the leader
	# can also be alive before setsid creates the group. Keep either case.
	if [ "$l_cleanup_register_scope" = pgid ] &&
		zxfer_signal_process_group 0 "$l_cleanup_register_pid"; then
		:
	else
		kill -s 0 "$l_cleanup_register_pid" 2>/dev/null || return 0
	fi
	zxfer_track_cleanup_pid_record \
		"$l_cleanup_register_pid" "$l_cleanup_register_purpose" \
		"$l_cleanup_register_scope"
}

# Purpose: Drop one PID's cleanup row.
# Usage: zxfer_unregister_cleanup_pid PID; called after the helper was waited
# for. A non-numeric PID is ignored.
zxfer_unregister_cleanup_pid() {
	l_cleanup_unregister_pid=$1
	l_cleanup_unregister_remaining=""

	zxfer_is_uint "$l_cleanup_unregister_pid" || return 0
	while IFS= read -r l_cleanup_unregister_row; do
		[ -n "$l_cleanup_unregister_row" ] || continue
		[ "${l_cleanup_unregister_row%%"$ZXFER_TAB"*}" != "$l_cleanup_unregister_pid" ] ||
			continue
		l_cleanup_unregister_remaining=${l_cleanup_unregister_remaining:+$l_cleanup_unregister_remaining$ZXFER_LF}$l_cleanup_unregister_row
	done <<EOF
${g_zxfer_cleanup_pid_records:-}
EOF
	g_zxfer_cleanup_pid_records=$l_cleanup_unregister_remaining
}

# Purpose: Signal one zxfer-owned direct child helper before its caller waits.
# Usage: zxfer_abort_direct_child_pid PID [SIGNAL] [PURPOSE] [SCOPE]; called
# by callers that spawned a helper but could not register it (or registered
# it elsewhere) and must stop it before failing. A pgid scope (from
# zxfer_spawn_background_shell) signals the helper's whole process group.
# A PURPOSE holding a tab or line break is replaced by "cleanup helper", so
# the signal is always tried.
zxfer_abort_direct_child_pid() {
	l_cleanup_direct_abort_pid=$1
	l_cleanup_direct_abort_signal=${2:-TERM}
	l_cleanup_direct_abort_purpose=${3:-cleanup helper}
	l_cleanup_direct_abort_scope=${4:-pid}
	l_cleanup_direct_abort_tracked=0

	g_zxfer_cleanup_pid_abort_failure_message=""
	zxfer_is_uint "$l_cleanup_direct_abort_pid" || return 0
	[ "$l_cleanup_direct_abort_pid" = "$$" ] && return 1

	zxfer_value_is_single_line "$l_cleanup_direct_abort_purpose" ||
		l_cleanup_direct_abort_purpose="cleanup helper"
	if zxfer_find_cleanup_pid_record "$l_cleanup_direct_abort_pid"; then
		l_cleanup_direct_abort_tracked=1
		l_cleanup_direct_abort_scope=$g_zxfer_cleanup_pid_record_scope
	fi
	if [ "$l_cleanup_direct_abort_scope" != pgid ] &&
		! kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null; then
		return 0
	fi
	if zxfer_signal_background_shell "$l_cleanup_direct_abort_pid" \
		"$l_cleanup_direct_abort_scope" "$l_cleanup_direct_abort_signal"; then
		return 0
	fi
	g_zxfer_cleanup_pid_abort_failure_message="Failed to signal cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid)."
	# Retain the owned direct-child handle under runtime for ordered trap retry.
	if [ "$l_cleanup_direct_abort_tracked" -eq 0 ]; then
		zxfer_track_cleanup_pid_record \
			"$l_cleanup_direct_abort_pid" "$l_cleanup_direct_abort_purpose" \
			"$l_cleanup_direct_abort_scope"
	fi
	return 1
}

# Purpose: Signal one registered cleanup helper before its owner waits.
# Usage: zxfer_abort_cleanup_pid PID [SIGNAL]; an untracked PID returns 0, and
# the row stays until the owner waits and unregisters it.
# SAFETY: direct-child records retain the baseline `$!`/registered/no-user-wait
# invariant and receive a liveness check immediately before signalling,
# without a normal-path process-table spawn.
zxfer_abort_cleanup_pid() {
	l_cleanup_abort_pid=$1
	l_cleanup_abort_signal=${2:-TERM}

	g_zxfer_cleanup_pid_abort_failure_message=""
	zxfer_find_cleanup_pid_record "$l_cleanup_abort_pid" || return 0

	l_cleanup_abort_purpose=$g_zxfer_cleanup_pid_record_purpose
	if [ "$g_zxfer_cleanup_pid_record_scope" != pgid ] &&
		! kill -s 0 "$l_cleanup_abort_pid" 2>/dev/null; then
		return 0
	fi
	if zxfer_signal_background_shell "$l_cleanup_abort_pid" \
		"$g_zxfer_cleanup_pid_record_scope" "$l_cleanup_abort_signal"; then
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

# Purpose: Stop every helper that still has a cleanup-PID row.
# Usage: zxfer_kill_registered_cleanup_pids; called by zxfer_trap_exit. It
# sends TERM to every row, waits once, then sends KILL to survivors, reaps
# them and drops their rows. Returns the first KILL failure and keeps that
# row and its message.
zxfer_kill_registered_cleanup_pids() {
	l_cleanup_kill_abort_status=0
	l_cleanup_kill_first_failure_message=""
	l_cleanup_kill_tracked_pids=""

	# Take the PID field of every row first: the loops below signal, wait and
	# unregister, so they must not read the rows through their own stdin.
	while IFS= read -r l_cleanup_kill_row; do
		[ -n "$l_cleanup_kill_row" ] || continue
		l_cleanup_kill_tracked_pids="$l_cleanup_kill_tracked_pids ${l_cleanup_kill_row%%"$ZXFER_TAB"*}"
	done <<EOF
${g_zxfer_cleanup_pid_records:-}
EOF

	# TERM every owned helper (or its process group) first so they wind down
	# concurrently. A TERM delivery failure is provisional: KILL below may
	# still terminate the same owned direct child safely.
	for l_cleanup_kill_pid in $l_cleanup_kill_tracked_pids; do
		zxfer_is_uint "$l_cleanup_kill_pid" || continue
		[ "$l_cleanup_kill_pid" = "$$" ] && continue
		zxfer_abort_cleanup_pid "$l_cleanup_kill_pid" TERM >/dev/null 2>&1 || :
	done

	[ -z "$l_cleanup_kill_tracked_pids" ] ||
		zxfer_cleanup_pid_abort_grace_wait

	# Escalate every survivor once, then reap and unregister it. Only a helper
	# that remains live after KILL failure is retained and reported; this keeps
	# trap shutdown bounded without promoting a recoverable TERM failure.
	for l_cleanup_kill_pid in $l_cleanup_kill_tracked_pids; do
		zxfer_is_uint "$l_cleanup_kill_pid" || continue
		[ "$l_cleanup_kill_pid" = "$$" ] && continue
		zxfer_find_cleanup_pid_record "$l_cleanup_kill_pid" || continue
		if [ "$g_zxfer_cleanup_pid_record_scope" = pgid ] ||
			kill -s 0 "$l_cleanup_kill_pid" 2>/dev/null; then
			l_cleanup_kill_status=0
			zxfer_abort_cleanup_pid "$l_cleanup_kill_pid" KILL ||
				l_cleanup_kill_status=$?
			if [ "$l_cleanup_kill_status" -ne 0 ]; then
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

################################################################################
# TEMP DIRECTORY SELECTION
################################################################################

# Purpose: List the default temp directory candidates, most preferred first.
# Usage: zxfer_list_default_tmpdir_candidates; one path per line. Tests
# override it by name to steer candidate selection.
zxfer_list_default_tmpdir_candidates() {
	printf '%s\n' "/dev/shm" "/run/shm" "/tmp"
}

# Purpose: Find the first safe default temp directory, ignoring TMPDIR.
# Usage: zxfer_find_default_tmpdir; publishes its physical path in
# g_zxfer_default_tmpdir_result, or returns 1 when no candidate is safe.
zxfer_find_default_tmpdir() {
	g_zxfer_default_tmpdir_result=""
	l_default_candidates=$(zxfer_list_default_tmpdir_candidates)
	while IFS= read -r l_default_candidate; do
		# A missing candidate (/dev/shm on macOS) costs no fork.
		[ -d "$l_default_candidate" ] || continue
		g_zxfer_default_tmpdir_result=$(zxfer_validate_temp_root_candidate \
			"$l_default_candidate") && return 0
	done <<EOF
$l_default_candidates
EOF
	g_zxfer_default_tmpdir_result=""
	return 1
}

# Purpose: Resolve the effective temp directory: a safe TMPDIR when one is
# set, else the first safe default candidate.
# Usage: zxfer_try_get_effective_tmpdir [1]; publishes g_zxfer_effective_tmpdir
# or returns 1. The choice is memoized per requested TMPDIR; pass 1 to
# revalidate a memoized choice before allocating a new root.
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
		if [ "${1:-0}" = 1 ]; then
			l_effective_tmpdir=$(zxfer_validate_temp_root_candidate \
				"$g_zxfer_effective_tmpdir") || return 1
			[ "$l_effective_tmpdir" = "$g_zxfer_effective_tmpdir" ] || return 1
		fi
		return 0
	fi

	l_effective_tmpdir=""
	if [ -n "$l_requested_tmpdir" ]; then
		l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir") ||
			l_effective_tmpdir=""
	fi
	if [ -z "$l_effective_tmpdir" ]; then
		if ! zxfer_find_default_tmpdir; then
			g_zxfer_effective_tmpdir_requested=$l_request_key
			g_zxfer_effective_tmpdir=""
			return 1
		fi
		l_effective_tmpdir=$g_zxfer_default_tmpdir_result
		if [ -n "$l_requested_tmpdir" ]; then
			# The fallback decision can run before option parsing (the eager
			# run temp root in zxfer_init_session_environment), so hold the
			# advisory and let zxfer_emit_pending_tmpdir_fallback_note replay
			# it once -V state is known; when -V is already live it emits
			# immediately.
			g_zxfer_tmpdir_fallback_note="Ignoring unsafe TMPDIR $l_requested_tmpdir; using $l_effective_tmpdir instead."
			zxfer_emit_pending_tmpdir_fallback_note
		fi
	fi

	g_zxfer_effective_tmpdir_requested=$l_request_key
	g_zxfer_effective_tmpdir=$l_effective_tmpdir
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

################################################################################
# PER-RUN TEMP ROOT
################################################################################

# Purpose: Discard inherited cleanup handles without acting on any referenced
# process or path.
# Usage: Called by zxfer_reset_session_state before the traps are installed,
# so exported shell variables can never grant cleanup ownership.
zxfer_discard_runtime_cleanup_state() {
	zxfer_reset_cleanup_pid_tracking
	g_zxfer_run_tmp_root=""
	g_zxfer_owned_run_tmp_root=""
	g_zxfer_owned_run_tmp_root_parent=""
	g_zxfer_owned_run_tmp_root_identity=""
	g_zxfer_run_umask=""
	g_zxfer_run_tmp_counter=0
	g_zxfer_runtime_artifact_cleanup_paths=""
	g_zxfer_runtime_artifact_path_result=""
	g_zxfer_runtime_artifact_read_result=""
	g_zxfer_runtime_artifact_directory_identity_result=""
	g_zxfer_staging_dir_result=""
	g_zxfer_default_tmpdir_result=""
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_tmpdir_fallback_note=""
	g_zxfer_temp_file_result=""
	g_zxfer_temp_file_group_result=""
}

# Purpose: Check that PATH is the run root this process created, by the
# recorded pathname, parent and reserved zxfer.<pid>.* name.
# Usage: zxfer_run_tmp_root_has_safe_owned_shape PATH; lexical checks only.
zxfer_run_tmp_root_has_safe_owned_shape() {
	l_owned_root_shape_path=$1
	l_owned_root_shape_parent=${g_zxfer_owned_run_tmp_root_parent:-}

	[ -n "$l_owned_root_shape_path" ] || return 1
	[ "$l_owned_root_shape_path" = "${g_zxfer_owned_run_tmp_root:-}" ] || return 1
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
	*/* | *"$ZXFER_LF"*) return 1 ;;
	esac
	return 0
}

# Purpose: Check, without forking, that the run root is still a real
# directory at its recorded pathname.
# Usage: zxfer_run_tmp_root_is_usable_dir PATH; called on every contained
# allocation and cleanup. The stat-backed check runs only before whole-root
# removal.
zxfer_run_tmp_root_is_usable_dir() {
	zxfer_run_tmp_root_has_safe_owned_shape "$1" || return 1
	[ -d "$1" ] && [ ! -L "$1" ]
}

# Purpose: Check that the run root is still the exact private directory this
# process created.
# Usage: zxfer_run_tmp_root_is_current_private_dir PATH; called right before
# whole-root removal. One fresh security record (identity, owner, mode) must
# equal the private record taken when mktemp created the root.
zxfer_run_tmp_root_is_current_private_dir() {
	zxfer_run_tmp_root_is_usable_dir "$1" || return 1
	l_private_root_record=$(zxfer_get_private_directory_security_record "$1") ||
		return 1
	[ "$l_private_root_record" = "${g_zxfer_owned_run_tmp_root_identity:-}" ]
}

# Purpose: Create the one per-run private temp root on first need and reuse it
# for every later runtime artifact allocation.
# Usage: zxfer_ensure_run_tmp_root; called from
# zxfer_init_session_environment and by the runtime artifact allocators.
# Creating the root also records the caller's umask in g_zxfer_run_umask and
# the root's security record in g_zxfer_owned_run_tmp_root_identity.
# SAFETY: the root is created mode 0700 (umask 077 + mktemp -d) under the
# validated effective temp directory, so the predictable <prefix>.<counter>
# child names inside it are safe: no other user can traverse, pre-create, or
# replace entries under a private root this process just created.
zxfer_ensure_run_tmp_root() {
	if [ -n "${g_zxfer_run_tmp_root:-}" ]; then
		if zxfer_run_tmp_root_is_usable_dir "$g_zxfer_run_tmp_root"; then
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
	zxfer_try_get_effective_tmpdir 1 || return
	l_ensure_run_tmp_root_effective_tmpdir=$g_zxfer_effective_tmpdir

	if [ "$l_ensure_run_tmp_root_effective_tmpdir" = "/" ]; then
		l_run_tmp_template="/zxfer.$$.XXXXXX"
	else
		l_run_tmp_template="$l_ensure_run_tmp_root_effective_tmpdir/zxfer.$$.XXXXXX"
	fi
	# One substitution prints the caller's umask on its first line, then
	# creates the root under umask 077 without changing this shell's umask.
	# Recording the umask here spares the file allocator a $(umask) fork.
	l_run_tmp_root=$(umask && umask 077 && mktemp -d "$l_run_tmp_template" 2>/dev/null) ||
		return
	l_run_umask=${l_run_tmp_root%%"$ZXFER_LF"*}
	l_run_tmp_root=${l_run_tmp_root#"$l_run_umask"}
	l_run_tmp_root=${l_run_tmp_root#"$ZXFER_LF"}
	# Record identity, owner and mode now; whole-root removal compares one
	# fresh record against this one instead of asking id and stat again.
	l_run_tmp_root_record=$(zxfer_get_private_directory_security_record \
		"$l_run_tmp_root") || l_run_tmp_root_record=""
	# GNU stat prints special bits too: a root under a setgid TMPDIR is 2700.
	case $l_run_tmp_root_record in
	*"$ZXFER_TAB"700 | *"$ZXFER_TAB"[1-7]700) ;;
	*)
		rmdir "$l_run_tmp_root" 2>/dev/null || :
		return 1
		;;
	esac

	g_zxfer_run_tmp_root=$l_run_tmp_root
	g_zxfer_owned_run_tmp_root=$l_run_tmp_root
	g_zxfer_owned_run_tmp_root_parent=$l_ensure_run_tmp_root_effective_tmpdir
	g_zxfer_owned_run_tmp_root_identity=$l_run_tmp_root_record
	g_zxfer_run_umask=$l_run_umask
	g_zxfer_run_tmp_counter=0
	return 0
}

# Purpose: Remove the per-run temp root and every runtime artifact below it.
# Usage: zxfer_remove_run_tmp_root; called by zxfer_trap_exit. Returns 1 and
# keeps the handles when the root is no longer the private directory this
# process created, or when rm fails.
zxfer_remove_run_tmp_root() {
	l_run_tmp_root=${g_zxfer_run_tmp_root:-}

	if [ -z "$l_run_tmp_root" ]; then
		# Owner fields without a root handle are inconsistent state.
		[ -z "${g_zxfer_owned_run_tmp_root:-}${g_zxfer_owned_run_tmp_root_parent:-}${g_zxfer_owned_run_tmp_root_identity:-}" ]
		return
	fi
	zxfer_run_tmp_root_has_safe_owned_shape "$l_run_tmp_root" || return 1
	if [ -e "$l_run_tmp_root" ] || [ -L "$l_run_tmp_root" ]; then
		zxfer_run_tmp_root_is_current_private_dir "$l_run_tmp_root" || return 1
		rm -rf "$l_run_tmp_root" 2>/dev/null ||
			{ [ ! -e "$l_run_tmp_root" ] && [ ! -L "$l_run_tmp_root" ]; } ||
			return 1
		zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
	fi
	g_zxfer_run_tmp_root=""
	g_zxfer_owned_run_tmp_root=""
	g_zxfer_owned_run_tmp_root_parent=""
	g_zxfer_owned_run_tmp_root_identity=""
	return 0
}

################################################################################
# PATH-ADJACENT ARTIFACT REGISTRY
################################################################################

# A few staging entries must live next to their target instead of under the
# run root. g_zxfer_runtime_artifact_cleanup_paths holds one pair of lines per
# entry: its identity (device-inode:..., inode:..., or - for a file), then its
# absolute path. Paths are absolute and never hold LF, and identities never
# start with /, so a whole-line match on a path finds only that path line.

# Purpose: Check the reserved lexical shape of an adjacent staging path.
# Usage: zxfer_runtime_artifact_registration_path_has_safe_shape PATH;
# registration and trap cleanup share it, so a corrupted registry cannot
# widen recursive deletion to an arbitrary path.
zxfer_runtime_artifact_registration_path_has_safe_shape() {
	l_registration_shape_path=$1

	case "$l_registration_shape_path" in
	/*) ;;
	*) return 1 ;;
	esac
	case "$l_registration_shape_path" in
	*"$ZXFER_LF"*) return 1 ;;
	esac
	l_registration_shape_name=${l_registration_shape_path##*/}
	case "$l_registration_shape_name" in
	zxfer.* | .zxfer-* | .zxfer.*) ;;
	*) return 1 ;;
	esac
	return 0
}

# Purpose: Look up PATH in the adjacent-artifact registry, optionally
# dropping it.
# Usage: zxfer_runtime_artifact_path_is_registered PATH [drop]; returns 0 when
# PATH is registered and leaves its identity (- for a file) in
# g_zxfer_runtime_artifact_directory_identity_result; with drop it also
# removes the pair.
zxfer_runtime_artifact_path_is_registered() {
	g_zxfer_runtime_artifact_directory_identity_result=""
	# Only a reserved single-line absolute path can be a registered path line.
	zxfer_runtime_artifact_registration_path_has_safe_shape "$1" || return 1
	l_registry="$ZXFER_LF${g_zxfer_runtime_artifact_cleanup_paths:-}$ZXFER_LF"
	case $l_registry in
	*"$ZXFER_LF$1$ZXFER_LF"*) ;;
	*) return 1 ;;
	esac
	l_registry_before=${l_registry%%"$ZXFER_LF$1$ZXFER_LF"*}
	g_zxfer_runtime_artifact_directory_identity_result=${l_registry_before##*"$ZXFER_LF"}
	[ "${2:-}" = drop ] || return 0
	# Keep the pairs before the identity line and after the path line.
	l_registry=${l_registry_before%"$ZXFER_LF"*}$ZXFER_LF${l_registry#*"$ZXFER_LF$1$ZXFER_LF"}
	l_registry=${l_registry#"$ZXFER_LF"}
	g_zxfer_runtime_artifact_cleanup_paths=${l_registry%"$ZXFER_LF"}
}

# Purpose: Register one just-created adjacent staging file or directory for
# trap cleanup.
# Usage: zxfer_register_runtime_artifact_path PATH; PATH must have the
# reserved shape and be a regular file or a directory, never a symlink. A
# directory is registered with its current identity.
zxfer_register_runtime_artifact_path() {
	l_register_path=$1

	[ -n "$l_register_path" ] || return 0
	zxfer_runtime_artifact_registration_path_has_safe_shape "$l_register_path" ||
		return 1
	[ ! -L "$l_register_path" ] || return 1
	[ -f "$l_register_path" ] || [ -d "$l_register_path" ] || return 1
	zxfer_runtime_artifact_path_is_registered "$l_register_path" && return 0
	l_register_identity=-
	if [ -d "$l_register_path" ]; then
		l_register_identity=$(zxfer_get_path_device_inode "$l_register_path") ||
			return 1
	fi
	g_zxfer_runtime_artifact_cleanup_paths=${g_zxfer_runtime_artifact_cleanup_paths:+$g_zxfer_runtime_artifact_cleanup_paths$ZXFER_LF}$l_register_identity$ZXFER_LF$l_register_path
}

# Purpose: Create one unpredictably named 0700 staging directory from a mktemp
# template under a caller-validated parent.
# Usage: zxfer_create_unpredictable_staging_dir TEMPLATE; publishes the path
# in g_zxfer_staging_dir_result. Staging parents may be shared sticky
# directories (a /tmp-style ZXFER_ERROR_LOG parent), where predictable
# pid+attempt names could be pre-created by a local process-table reader; the
# random name closes that window and umask 077 keeps the directory private.
zxfer_create_unpredictable_staging_dir() {
	g_zxfer_staging_dir_result=$(umask 077 && exec mktemp -d "$1" 2>/dev/null)
}

################################################################################
# ARTIFACT CLEANUP
################################################################################

# Purpose: Check whether a path is one direct child of the private run root.
# Usage: Runtime allocations are deliberately flat; contained workspaces own
# their descendants and are removed as one direct-child directory.
zxfer_runtime_artifact_path_is_run_root_child() {
	l_artifact_path=$1
	l_run_tmp_root=${g_zxfer_run_tmp_root:-}

	[ -n "$l_run_tmp_root" ] || return 1
	zxfer_run_tmp_root_is_usable_dir "$l_run_tmp_root" || return 1
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

# Purpose: Remove one run-root child or registered adjacent artifact.
# Usage: zxfer_cleanup_runtime_artifact_path PATH; returns 1 for any other
# path, for a registered directory whose identity changed, or when rm fails.
# A registered path leaves the registry only once it is gone.
zxfer_cleanup_runtime_artifact_path() {
	l_cleanup_path=$1
	l_cleanup_registered=0

	[ -n "$l_cleanup_path" ] || return 0
	if zxfer_runtime_artifact_path_is_run_root_child "$l_cleanup_path"; then
		:
	elif zxfer_runtime_artifact_path_is_registered "$l_cleanup_path"; then
		l_cleanup_registered=1
		l_cleanup_registered_identity=$g_zxfer_runtime_artifact_directory_identity_result
	else
		return 1
	fi
	if [ -L "$l_cleanup_path" ]; then
		rm -f "$l_cleanup_path" 2>/dev/null || return 1
	elif [ -d "$l_cleanup_path" ]; then
		# Recurse into a registered directory only while it is still the
		# object that was registered.
		if [ "$l_cleanup_registered" -eq 1 ]; then
			[ "$l_cleanup_registered_identity" != - ] || return 1
			l_cleanup_current_identity=$(zxfer_get_path_device_inode "$l_cleanup_path") ||
				return 1
			[ "$l_cleanup_current_identity" = "$l_cleanup_registered_identity" ] ||
				return 1
		fi
		rm -rf "$l_cleanup_path" 2>/dev/null || return 1
	elif [ -e "$l_cleanup_path" ]; then
		rm -f "$l_cleanup_path" 2>/dev/null || return 1
	fi
	[ "$l_cleanup_registered" -eq 0 ] ||
		zxfer_runtime_artifact_path_is_registered "$l_cleanup_path" drop || :
	zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
	return 0
}

# Purpose: Remove several run-root children or registered artifacts.
# Usage: zxfer_cleanup_runtime_artifact_paths PATH...; empty arguments are
# skipped, and it returns 1 when any path stays.
zxfer_cleanup_runtime_artifact_paths() {
	l_cleanup_status=0

	for l_cleanup_runtime_artifact_paths_artifact_path in "$@"; do
		[ -n "$l_cleanup_runtime_artifact_paths_artifact_path" ] || continue
		if ! zxfer_cleanup_runtime_artifact_path "$l_cleanup_runtime_artifact_paths_artifact_path"; then
			l_cleanup_status=1
		fi
	done

	return "$l_cleanup_status"
}

# Purpose: Remove a newline-separated list of runtime artifacts.
# Usage: zxfer_cleanup_runtime_artifact_path_list LIST; run-root files and
# FIFOs share one non-recursive rm, and every other path goes through
# zxfer_cleanup_runtime_artifact_path. Returns 1 when any path stays.
zxfer_cleanup_runtime_artifact_path_list() {
	l_artifact_path_list=$1
	l_cleanup_status=0
	set --

	while IFS= read -r l_cleanup_runtime_artifact_path_list_artifact_path || [ -n "$l_cleanup_runtime_artifact_path_list_artifact_path" ]; do
		[ -n "$l_cleanup_runtime_artifact_path_list_artifact_path" ] || continue
		if zxfer_runtime_artifact_path_is_run_root_child "$l_cleanup_runtime_artifact_path_list_artifact_path" &&
			{ [ -f "$l_cleanup_runtime_artifact_path_list_artifact_path" ] || [ -p "$l_cleanup_runtime_artifact_path_list_artifact_path" ]; }; then
			set -- "$@" "$l_cleanup_runtime_artifact_path_list_artifact_path"
		elif ! zxfer_cleanup_runtime_artifact_path "$l_cleanup_runtime_artifact_path_list_artifact_path"; then
			l_cleanup_status=1
		fi
	done <<-EOF
		$l_artifact_path_list
	EOF
	if [ "$#" -gt 0 ]; then
		zxfer_run_tmp_root_is_usable_dir "${g_zxfer_run_tmp_root:-}" || return 1
		rm -f "$@" 2>/dev/null || l_cleanup_status=1
		for l_cleanup_runtime_artifact_path_list_artifact_path; do
			if [ -e "$l_cleanup_runtime_artifact_path_list_artifact_path" ] ||
				[ -L "$l_cleanup_runtime_artifact_path_list_artifact_path" ]; then
				l_cleanup_status=1
			else
				zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_paths_cleaned
			fi
		done
	fi

	return "$l_cleanup_status"
}

# Purpose: Remove a runtime artifact list and return the caller's status.
# Usage: zxfer_cleanup_runtime_artifact_path_list_and_return STATUS LIST;
# cleanup failures are ignored so STATUS survives.
zxfer_cleanup_runtime_artifact_path_list_and_return() {
	l_return_status=$1
	l_cleanup_runtime_artifact_path_list_and_return_artifact_path_list=$2

	zxfer_cleanup_runtime_artifact_path_list "$l_cleanup_runtime_artifact_path_list_and_return_artifact_path_list" >/dev/null 2>&1 || :
	return "$l_return_status"
}

# Purpose: Remove every registered adjacent artifact.
# Usage: zxfer_cleanup_registered_runtime_artifacts; called by zxfer_trap_exit
# before whole-root removal. Returns 1 when any entry stays or has an unsafe
# shape.
zxfer_cleanup_registered_runtime_artifacts() {
	l_registered_status=0
	l_registered_identity=""

	# Walk a copy: each successful cleanup drops its pair from the registry.
	while IFS= read -r l_registered_line; do
		if [ -z "$l_registered_identity" ]; then
			l_registered_identity=$l_registered_line
			continue
		fi
		l_registered_identity=""
		if zxfer_runtime_artifact_registration_path_has_safe_shape "$l_registered_line"; then
			zxfer_cleanup_runtime_artifact_path "$l_registered_line" ||
				l_registered_status=1
		else
			l_registered_status=1
		fi
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF
	# An identity line without its path line means the registry is damaged.
	[ -z "$l_registered_identity" ] || l_registered_status=1

	return "$l_registered_status"
}

################################################################################
# ARTIFACT ALLOCATION AND READBACK
################################################################################

# Purpose: Create a private 0700 scratch directory under the per-run temp
# root.
# Usage: zxfer_create_private_temp_dir [PREFIX]; publishes
# g_zxfer_runtime_artifact_path_result. Never registered for cleanup; the
# run-root removal covers it.
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
}

# Purpose: Create an empty 0600 scratch file under the per-run temp root.
# Usage: zxfer_create_runtime_artifact_file [PREFIX]; publishes
# g_zxfer_runtime_artifact_path_result. Never registered for cleanup;
# the run-root removal covers it. It restores g_zxfer_run_umask afterwards, so
# callers must not hold a temporary umask across the call.
zxfer_create_runtime_artifact_file() {
	l_prefix=${1:-zxfer-temp}

	g_zxfer_runtime_artifact_path_result=""
	case "$l_prefix" in
	'' | '.' | '..' | *[!A-Za-z0-9._-]*) return 1 ;;
	esac
	zxfer_ensure_run_tmp_root || return "$?"
	# A root that zxfer_ensure_run_tmp_root did not create here has no recorded
	# umask. Read it once; never pass umask an empty value (ksh reads 0777).
	case ${g_zxfer_run_umask:-} in
	'' | *[!0-7]*) g_zxfer_run_umask=$(umask) ;;
	esac

	# Exclusive 0600 creation in this shell: umask 077 plus noclobber, both
	# restored below. printf, not the special builtin :, because a failed
	# redirection on : exits dash and ksh. A taken name means an earlier
	# allocation ran in a subshell and its counter bump never reached this
	# shell, so step ahead to the next free name instead of failing.
	case $- in
	*C*) l_artifact_noclobber_was_set=1 ;;
	*) l_artifact_noclobber_was_set=0 ;;
	esac
	umask 077
	set -C
	l_artifact_status=1
	while :; do
		g_zxfer_run_tmp_counter=$((g_zxfer_run_tmp_counter + 1))
		l_artifact_file="$g_zxfer_run_tmp_root/$l_prefix.$g_zxfer_run_tmp_counter"
		if { printf '' >"$l_artifact_file"; } 2>/dev/null; then
			l_artifact_status=0
			break
		fi
		[ -e "$l_artifact_file" ] || [ -L "$l_artifact_file" ] || break
	done
	[ "$l_artifact_noclobber_was_set" -eq 1 ] || set +C
	umask "$g_zxfer_run_umask"
	[ "$l_artifact_status" -eq 0 ] || return 1
	zxfer_profile_increment_counter g_zxfer_profile_runtime_artifact_files_created
	g_zxfer_runtime_artifact_path_result=$l_artifact_file
}

# Purpose: Replace a runtime artifact file's contents with a payload.
# Usage: zxfer_write_runtime_artifact_file PATH PAYLOAD; silent, returning 1
# when PATH cannot be opened and any other writer status unchanged.
zxfer_write_runtime_artifact_file() {
	[ -n "$1" ] || return 1
	l_runtime_write_status=0
	# printf is a regular builtin, so a failed redirection returns a status
	# instead of exiting the shell; the brace group silences its message.
	# >| overwrites even when a signal trap inherits the allocator's set -C.
	{ printf '%s' "$2" >|"$1"; } 2>/dev/null || l_runtime_write_status=$?

	case "$l_runtime_write_status" in
	1 | 2)
		# dash reports redirection-open failures as status 2 while other
		# supported /bin/sh implementations collapse the same failure to 1.
		return 1
		;;
	esac

	return "$l_runtime_write_status"
}

# Purpose: Read a runtime artifact file into the current shell, trailing
# newlines included.
# Usage: zxfer_read_runtime_artifact_file PATH; publishes
# g_zxfer_runtime_artifact_read_result, or returns 1 for an unreadable PATH
# and the cat status when the read fails.
zxfer_read_runtime_artifact_file() {
	l_runtime_read_artifact_path=$1
	l_runtime_read_artifact_contents=""

	g_zxfer_runtime_artifact_read_result=""
	[ -r "$l_runtime_read_artifact_path" ] || return 1

	l_runtime_artifact_read_status=0
	l_runtime_read_artifact_contents=$(
		cat "$l_runtime_read_artifact_path"
		l_runtime_artifact_read_status=$?
		# Keep one non-newline sentinel inside the substitution so trailing
		# blank lines from the artifact survive command substitution intact.
		printf x
		exit "$l_runtime_artifact_read_status"
	) || l_runtime_artifact_read_status=$?
	if [ "$l_runtime_artifact_read_status" -ne 0 ]; then
		return "$l_runtime_artifact_read_status"
	fi
	l_runtime_read_artifact_contents=${l_runtime_read_artifact_contents%?}

	g_zxfer_runtime_artifact_read_result=$l_runtime_read_artifact_contents
}

# Purpose: Read a runtime artifact file without its final newline.
# Usage: zxfer_read_runtime_artifact_file_trimmed PATH; same result and status
# as zxfer_read_runtime_artifact_file.
zxfer_read_runtime_artifact_file_trimmed() {
	l_artifact_path=$1

	zxfer_read_runtime_artifact_file "$l_artifact_path" ||
		return "$?"
	case "$g_zxfer_runtime_artifact_read_result" in
	*'
')
		g_zxfer_runtime_artifact_read_result=${g_zxfer_runtime_artifact_read_result%?}
		;;
	esac
}

# Purpose: Allocate one scratch file under the run root, or stop the run.
# Usage: zxfer_get_temp_file; publishes g_zxfer_temp_file_result.
zxfer_get_temp_file() {
	g_zxfer_temp_file_result=""
	zxfer_create_runtime_artifact_file "zxfer-temp" ||
		zxfer_throw_error "Error creating temporary file." "$?"
	zxfer_echoV "New temporary file: $g_zxfer_runtime_artifact_path_result"
	g_zxfer_temp_file_result=$g_zxfer_runtime_artifact_path_result
}

# Purpose: Allocate COUNT scratch files as one group.
# Usage: zxfer_create_temp_file_group COUNT; publishes the paths, one per line,
# in g_zxfer_temp_file_group_result. A failed allocation removes the files
# already made and returns its status.
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
		zxfer_get_temp_file || {
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
}
