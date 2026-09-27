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
# SEND/RECEIVE JOB SCHEDULING AND SUPERVISION
################################################################################

# Module contract:
# owns globals: the in-memory send-job list g_zxfer_send_jobs (one
# tab-separated row per job: job_id, pid, dest, status_file, snapshot,
# scope), g_count_zfs_send_jobs, g_zxfer_send_job_sequence, the abort grace
# and poll knobs, g_zxfer_send_job_conflict_dest_dataset, and
# g_zxfer_send_job_abort_failure_message.
# reads globals: g_option_j_jobs, g_option_T_target_host, and the background
# shell spawn helpers.
# mutates caches: destination snapshot and property state after each
# successful receive (zxfer_finish_destination_receive), and the replication
# pass marker g_is_performed_send_destroy after each send.
# returns via stdout: none.
#
# Model: a job is `/bin/sh -c 'PIPELINE & wait; echo status >$1'` started
# through zxfer_spawn_background_shell (its own process group when the host
# allows).
# Completion is detected by polling the per-job status files. The jobs
# builtin refreshes child status before kill -0 checks for abnormal death.
# Abort is one TERM per job scope, a bounded grace window, one KILL, then
# wait. Hosts without process groups use the descendant cleanup wrapper. A
# job that wrote its status has exited, so its PID may already belong to an
# unrelated process: it only ever gets a process-group signal.

# Purpose: Reset all send-job scheduling state for a new session.
# Usage: Called by the session composition root before traps are installed
# and again before any transfer is queued; assignments only.
zxfer_reset_send_job_state() {
	g_count_zfs_send_jobs=0
	g_zxfer_send_jobs=""
	g_zxfer_send_job_sequence=0
	g_zxfer_send_job_conflict_dest_dataset=""
	g_zxfer_send_job_abort_grace_seconds=1
	g_zxfer_send_job_poll_seconds=""
	g_zxfer_send_job_abort_failure_message=""
}

# Purpose: Detect an active send job whose destination equals, contains, or
# lies inside DEST (every job in one run targets the same host).
# Usage: zxfer_send_job_conflicts_with_destination DEST; publishes the
# conflicting destination in $g_zxfer_send_job_conflict_dest_dataset.
zxfer_send_job_conflicts_with_destination() {
	l_conflict_dest=$1

	g_zxfer_send_job_conflict_dest_dataset=""
	[ -n "$l_conflict_dest" ] || return 1
	[ -n "${g_zxfer_send_jobs:-}" ] || return 1
	while IFS='	' read -r l_conflict_job_id l_conflict_pid l_conflict_job_dest l_conflict_rest; do
		[ -n "$l_conflict_job_id" ] || continue
		case $l_conflict_dest in
		"$l_conflict_job_dest" | "$l_conflict_job_dest"/*) ;;
		*)
			case $l_conflict_job_dest in
			"$l_conflict_dest"/*) ;;
			*) continue ;;
			esac
			;;
		esac
		g_zxfer_send_job_conflict_dest_dataset=$l_conflict_job_dest
		return 0
	done <<EOF
$g_zxfer_send_jobs
EOF

	return 1
}

# Purpose: Probe once whether sleep(1) accepts sub-second intervals.
# Usage: Called before the first job-limit wait; suites may pre-set
# $g_zxfer_send_job_poll_seconds.
zxfer_init_send_job_poll_interval() {
	[ -z "${g_zxfer_send_job_poll_seconds:-}" ] || return 0
	if sleep 0.001 2>/dev/null; then
		g_zxfer_send_job_poll_seconds=0.2
	else
		g_zxfer_send_job_poll_seconds=1
	fi
}

# Purpose: Start one rendered send/receive pipeline as a supervised job.
# Usage: zxfer_spawn_send_job EXEC_CMD SNAPSHOT DEST; appends the job row and
# throws when the job shell cannot be started.
zxfer_spawn_send_job() {
	l_spawn_send_job_cmd=$1
	l_spawn_send_job_snapshot=$2
	l_spawn_send_job_dest=$3

	g_zxfer_send_job_sequence=$((g_zxfer_send_job_sequence + 1))
	l_spawn_send_job_id="sendjob.$$.$g_zxfer_send_job_sequence"
	zxfer_get_temp_file || return "$?"
	l_spawn_send_job_status_file=$g_zxfer_temp_file_result
	# Keep the job shell alive long enough to record interrupted waits. Whole
	# process-group or fallback-wrapper cleanup owns all pipeline stages; the
	# status path arrives as $1 so no additional shell quoting is needed.
	# ksh93 (illumos /bin/sh) reports a signal death as 256+N; record 128+N.
	l_spawn_send_job_shell="trap 'kill -s TERM \"\$zxfer_job_pipeline\" 2>/dev/null' TERM INT HUP
$l_spawn_send_job_cmd &
zxfer_job_pipeline=\$!
wait \"\$zxfer_job_pipeline\"
zxfer_job_status=\$?
[ \"\$zxfer_job_status\" -le 128 ] || wait \"\$zxfer_job_pipeline\" 2>/dev/null
[ \"\$zxfer_job_status\" -le 255 ] || zxfer_job_status=\$((zxfer_job_status - 128))
printf '%s\\n' \"\$zxfer_job_status\" >\"\$1\"
exit \"\$zxfer_job_status\""
	l_spawn_send_job_status=0
	zxfer_spawn_background_shell "$l_spawn_send_job_shell" "" "" \
		"$l_spawn_send_job_status_file" || l_spawn_send_job_status=$?
	if [ "$l_spawn_send_job_status" -ne 0 ]; then
		zxfer_throw_error "Failed to start the send/receive job for [$l_spawn_send_job_snapshot -> $l_spawn_send_job_dest]." "$l_spawn_send_job_status"
		return "$l_spawn_send_job_status"
	fi
	g_zxfer_send_jobs=${g_zxfer_send_jobs:+$g_zxfer_send_jobs
}"$l_spawn_send_job_id	$g_last_background_pid	$l_spawn_send_job_dest	$l_spawn_send_job_status_file	$l_spawn_send_job_snapshot	$g_zxfer_background_shell_scope"
	g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
}

# Purpose: Remove one job row from the list.
# Usage: zxfer_unregister_send_job JOB_ID; called after the job was reaped.
zxfer_unregister_send_job() {
	l_unregister_send_job_id=$1
	l_unregister_send_job_remaining=""

	while IFS= read -r l_unregister_send_job_row; do
		[ -n "$l_unregister_send_job_row" ] || continue
		case $l_unregister_send_job_row in
		"$l_unregister_send_job_id	"*) continue ;;
		esac
		l_unregister_send_job_remaining=${l_unregister_send_job_remaining:+$l_unregister_send_job_remaining
}$l_unregister_send_job_row
	done <<EOF
${g_zxfer_send_jobs:-}
EOF
	g_zxfer_send_jobs=$l_unregister_send_job_remaining
	[ "${g_count_zfs_send_jobs:-0}" -le 0 ] ||
		g_count_zfs_send_jobs=$((g_count_zfs_send_jobs - 1))
}

# Purpose: Record one successful receive into DEST in the destination caches.
# Usage: zxfer_finish_destination_receive DEST; runs in the main shell after
# a foreground pipeline and when a job is reaped.
zxfer_finish_destination_receive() {
	l_finish_dest=$1

	zxfer_note_destination_receive_completed "$l_finish_dest"
	zxfer_invalidate_destination_property_mutation_cache "$l_finish_dest" exact
	# The convergence check lists DEST live. The whole-tree snapshot record
	# cache stays: later -d planning needs it.
	zxfer_verify_converged_destination_after_receive "$l_finish_dest"
}

# Purpose: Reap one finished job: finish its receive on success, otherwise
# tear the remaining jobs down and throw the structured failure.
# Usage: zxfer_reap_send_job JOB_ID PID DEST STATUS_FILE SNAPSHOT
zxfer_reap_send_job() {
	l_reap_send_job_id=$1
	l_reap_send_job_pid=$2
	l_reap_send_job_dest=$3
	l_reap_send_job_status_file=$4
	l_reap_send_job_snapshot=$5

	l_reap_send_job_status=""
	if [ -s "$l_reap_send_job_status_file" ]; then
		IFS= read -r l_reap_send_job_status <"$l_reap_send_job_status_file" ||
			l_reap_send_job_status=""
	fi
	case $l_reap_send_job_status in
	0 | [1-9] | [1-9][0-9] | 1[0-9][0-9] | 2[0-4][0-9] | 25[0-5]) ;;
	*)
		# The job shell died before recording its status.
		l_reap_send_job_status=125
		;;
	esac
	if [ "$l_reap_send_job_status" -eq 0 ]; then
		# jobs may discard a completed child's wait entry (notably dash).
		# The private status file is the pipeline result; wait only reaps.
		wait "$l_reap_send_job_pid" 2>/dev/null || :
		zxfer_unregister_send_job "$l_reap_send_job_id"
		zxfer_finish_destination_receive "$l_reap_send_job_dest"
		return 0
	fi

	if [ -n "$l_reap_send_job_snapshot" ]; then
		l_reap_send_job_context="[$l_reap_send_job_snapshot -> $l_reap_send_job_dest]"
	else
		l_reap_send_job_context="[$l_reap_send_job_dest]"
	fi
	[ -z "${g_option_T_target_host:-}" ] ||
		l_reap_send_job_context="$l_reap_send_job_context on target [$g_option_T_target_host]"
	# Keep the failed job registered until teardown: a killed job shell can
	# leave its pipeline alive, and the failure must stop those stages too.
	l_reap_send_job_abort_status=0
	zxfer_abort_all_send_jobs || l_reap_send_job_abort_status=$?
	zxfer_set_failure_stage "send/receive"
	if [ "$l_reap_send_job_abort_status" -ne 0 ]; then
		zxfer_throw_error "$g_zxfer_send_job_abort_failure_message" "$l_reap_send_job_abort_status"
		return "$l_reap_send_job_abort_status"
	fi
	zxfer_throw_error "zfs send/receive job failed for $l_reap_send_job_context (PID $l_reap_send_job_pid, exit $l_reap_send_job_status)." "$l_reap_send_job_status"
}

# Purpose: Block until at least one active job has finished, then reap every
# finished job found in that scan.
# Usage: zxfer_wait_for_any_send_job REASON; called when the job limit or a
# destination-ancestry conflict blocks the next transfer.
zxfer_wait_for_any_send_job() {
	l_wait_any_send_job_reason=$1

	[ -n "${g_zxfer_send_jobs:-}" ] || return 0
	[ -z "$l_wait_any_send_job_reason" ] ||
		zxfer_echoV "Waiting for zfs send/receive jobs ($l_wait_any_send_job_reason)."
	while :; do
		# Some shells keep dead children as zombies until jobs/wait updates
		# their table; kill -0 alone would then mistake SIGKILL for liveness.
		if command -v jobs >/dev/null 2>&1; then
			command jobs >/dev/null 2>&1 || :
		fi
		l_wait_any_send_job_reaped=0
		while IFS='	' read -r l_wait_any_send_job_id l_wait_any_send_job_pid l_wait_any_send_job_dest l_wait_any_send_job_status_file l_wait_any_send_job_snapshot l_wait_any_send_job_scope; do
			[ -n "$l_wait_any_send_job_id" ] || continue
			if [ -s "$l_wait_any_send_job_status_file" ] ||
				! kill -s 0 "$l_wait_any_send_job_pid" 2>/dev/null; then
				# The -T convergence check runs ssh, which must not read
				# the remaining rows from this loop's stdin.
				zxfer_reap_send_job "$l_wait_any_send_job_id" \
					"$l_wait_any_send_job_pid" "$l_wait_any_send_job_dest" \
					"$l_wait_any_send_job_status_file" "$l_wait_any_send_job_snapshot" \
					</dev/null || return "$?"
				l_wait_any_send_job_reaped=1
			fi
		done <<EOF
$g_zxfer_send_jobs
EOF
		[ "$l_wait_any_send_job_reaped" -eq 0 ] || return 0
		zxfer_init_send_job_poll_interval
		sleep "$g_zxfer_send_job_poll_seconds"
	done
}

# Purpose: Reap every active job as it finishes.
# Usage: zxfer_wait_for_zfs_send_jobs REASON; called at the end of a
# replication pass so a failed job surfaces before the pass completes.
zxfer_wait_for_zfs_send_jobs() {
	l_wait_send_jobs_reason=$1

	if [ -z "${g_zxfer_send_jobs:-}" ]; then
		g_count_zfs_send_jobs=0
		return 0
	fi
	[ -z "$l_wait_send_jobs_reason" ] ||
		zxfer_echoV "Waiting for zfs send/receive jobs ($l_wait_send_jobs_reason)."
	while [ -n "${g_zxfer_send_jobs:-}" ]; do
		zxfer_wait_for_any_send_job "" || return "$?"
	done
}

# Purpose: Send SIGNAL to one job. A job whose status file is written has
# exited, so it only gets a signal to its process group, and only for a
# pgid scope; never its bare PID or the wrapper's STOP/KILL.
# Usage: zxfer_signal_send_job PID SCOPE STATUS_FILE SIGNAL; returns non-zero
# when a running job cannot be signalled.
zxfer_signal_send_job() {
	l_signal_job_pid=$1
	l_signal_job_scope=$2
	l_signal_job_status_file=$3
	l_signal_job_signal=$4

	if [ -s "$l_signal_job_status_file" ]; then
		[ "$l_signal_job_scope" != pgid ] ||
			zxfer_signal_process_group "$l_signal_job_signal" "$l_signal_job_pid" || :
		return 0
	fi
	zxfer_signal_background_shell "$l_signal_job_pid" "$l_signal_job_scope" \
		"$l_signal_job_signal"
}

# Purpose: Stop every active job: TERM each job scope, wait up to the grace
# window for the job shells to go away, KILL survivors, reap, and clear.
# Usage: Called from trap exit before the run root is removed and before a
# job failure is thrown. Retains jobs whose final signal failed, reports the
# first failure, and never blocks in wait on a child it could not stop.
zxfer_abort_all_send_jobs() {
	g_zxfer_send_job_abort_failure_message=""
	[ -n "${g_zxfer_send_jobs:-}" ] || return 0
	l_abort_send_jobs_rows=$g_zxfer_send_jobs
	l_abort_send_jobs_status=0

	while IFS='	' read -r l_abort_send_jobs_id l_abort_send_jobs_pid l_abort_send_jobs_dest l_abort_send_jobs_status_file l_abort_send_jobs_snapshot l_abort_send_jobs_scope; do
		[ -n "$l_abort_send_jobs_id" ] || continue
		zxfer_signal_send_job "$l_abort_send_jobs_pid" "$l_abort_send_jobs_scope" \
			"$l_abort_send_jobs_status_file" TERM || :
	done <<EOF
$l_abort_send_jobs_rows
EOF

	# Poll rather than sleep the whole grace window so an interrupted run
	# exits as soon as the running job shells are gone.
	zxfer_init_send_job_poll_interval
	# Keep only a plain number before it reaches arithmetic.
	l_abort_send_jobs_ticks=${g_zxfer_send_job_abort_grace_seconds##*[!0-9]*}
	l_abort_send_jobs_ticks=${l_abort_send_jobs_ticks:-1}
	[ "$g_zxfer_send_job_poll_seconds" != 0.2 ] ||
		l_abort_send_jobs_ticks=$((l_abort_send_jobs_ticks * 5))
	while [ "$l_abort_send_jobs_ticks" -gt 0 ]; do
		l_abort_send_jobs_live=0
		while IFS='	' read -r l_abort_send_jobs_id l_abort_send_jobs_pid l_abort_send_jobs_dest l_abort_send_jobs_status_file l_abort_send_jobs_snapshot l_abort_send_jobs_scope; do
			[ -n "$l_abort_send_jobs_id" ] || continue
			[ ! -s "$l_abort_send_jobs_status_file" ] || continue
			if kill -s 0 "$l_abort_send_jobs_pid" 2>/dev/null; then
				l_abort_send_jobs_live=1
				break
			fi
		done <<EOF
$l_abort_send_jobs_rows
EOF
		[ "$l_abort_send_jobs_live" -eq 1 ] || break
		sleep "$g_zxfer_send_job_poll_seconds"
		l_abort_send_jobs_ticks=$((l_abort_send_jobs_ticks - 1))
	done

	while IFS='	' read -r l_abort_send_jobs_id l_abort_send_jobs_pid l_abort_send_jobs_dest l_abort_send_jobs_status_file l_abort_send_jobs_snapshot l_abort_send_jobs_scope; do
		[ -n "$l_abort_send_jobs_id" ] || continue
		l_abort_send_jobs_signal_status=0
		zxfer_signal_send_job "$l_abort_send_jobs_pid" "$l_abort_send_jobs_scope" \
			"$l_abort_send_jobs_status_file" KILL || l_abort_send_jobs_signal_status=$?
		if [ "$l_abort_send_jobs_signal_status" -ne 0 ]; then
			if [ "$l_abort_send_jobs_status" -eq 0 ]; then
				l_abort_send_jobs_status=$l_abort_send_jobs_signal_status
				g_zxfer_send_job_abort_failure_message="Failed to stop send/receive job [$l_abort_send_jobs_snapshot -> $l_abort_send_jobs_dest] (PID $l_abort_send_jobs_pid)."
			fi
			continue
		fi
		wait "$l_abort_send_jobs_pid" 2>/dev/null || :
		zxfer_unregister_send_job "$l_abort_send_jobs_id"
	done <<EOF
$l_abort_send_jobs_rows
EOF

	return "$l_abort_send_jobs_status"
}

# Purpose: Run one rendered send/receive pipeline in the foreground or as a
# supervised job, then mark the pass as having sent.
# Usage: zxfer_schedule_send_receive_pipeline EXEC_CMD SNAPSHOT DEST
# ALLOW_BACKGROUND; a job is used only when ALLOW_BACKGROUND is 1 and -j
# exceeds 1, once the job limit and destination ancestry allow it.
# Side effects: Finishes a foreground receive here (a job's when reaped) and
# sets g_is_performed_send_destroy=1, the marker -Y decides on.
zxfer_schedule_send_receive_pipeline() {
	l_schedule_pipeline_cmd=$1
	l_schedule_pipeline_snapshot=$2
	l_schedule_pipeline_dest=$3

	if [ "$4" -ne 1 ] || [ "$g_option_j_jobs" -le 1 ]; then
		zxfer_execute_rendered_shell_command "$l_schedule_pipeline_cmd" || return
		zxfer_finish_destination_receive "$l_schedule_pipeline_dest"
	else
		while :; do
			if [ "${g_count_zfs_send_jobs:-0}" -ge "$g_option_j_jobs" ]; then
				zxfer_echov "Max jobs reached [$g_count_zfs_send_jobs]. Waiting for jobs to complete."
				zxfer_wait_for_any_send_job "job limit" || return
			elif zxfer_send_job_conflicts_with_destination "$l_schedule_pipeline_dest"; then
				zxfer_echov "Waiting for conflicting zfs send/receive ancestry to finish for destination [$l_schedule_pipeline_dest]; active destination [$g_zxfer_send_job_conflict_dest_dataset] is still running."
				zxfer_wait_for_any_send_job "destination ancestry" || return
			else
				break
			fi
		done
		g_zxfer_profile_send_receive_background_pipeline_commands=$((g_zxfer_profile_send_receive_background_pipeline_commands + 1))
		zxfer_record_last_command_string "$l_schedule_pipeline_cmd"
		zxfer_echov "$l_schedule_pipeline_cmd"
		zxfer_spawn_send_job "$l_schedule_pipeline_cmd" \
			"$l_schedule_pipeline_snapshot" "$l_schedule_pipeline_dest" || return
	fi
	g_is_performed_send_destroy=1
}
