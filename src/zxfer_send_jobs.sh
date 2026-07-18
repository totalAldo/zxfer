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
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# BSD HEADER END
# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# SEND JOB QUEUE AND SCHEDULING
################################################################################

# Module contract:
# owns globals: send-job queue handles, job/domain records, ancestry-conflict
# scratch, and g_zxfer_send_receive_ran_in_background_result.
# reads globals: configured job limit and target transport identity.
# mutates caches: generic supervisor registrations and destination state after a
# completed receive.
# returns via stdout: job identifiers and operator-facing job context.

# Purpose: Reset all send-job scheduling state for a new session.
# Usage: Called by the session composition root before any transfer is queued.
zxfer_reset_send_job_state() {
	g_count_zfs_send_jobs=0
	g_zfs_send_job_pids=""
	g_zfs_send_job_supervisor_records=""
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_unavailable=0
	g_zfs_send_job_queue_path=""
	g_zfs_send_job_queue_dir=""
	g_zfs_send_job_queue_writer_open=0
	g_zxfer_send_job_queue_open_failure_fatal=0
	g_zxfer_send_job_record_runner_pid=""
	g_zxfer_send_job_record_source_dataset=""
	g_zxfer_send_job_record_source_snapshot=""
	g_zxfer_send_job_record_dest_dataset=""
	g_zxfer_send_job_record_target_host=""
	g_zxfer_send_job_conflict_dest_dataset=""
	g_zxfer_send_job_ids_result=""
	g_zxfer_send_receive_ran_in_background_result=0
}

# Purpose: Open the send job completion queue and publish the handles or state
# later helpers need.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination before asynchronous work starts using the shared coordination
# resource.
zxfer_open_send_job_completion_queue() {
	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ]; then
		if [ "${g_zfs_send_job_queue_writer_open:-0}" -eq 1 ]; then
			return 0
		fi
		if [ -z "${g_zfs_send_job_queue_path:-}" ]; then
			zxfer_close_send_job_completion_queue
			g_zfs_send_job_queue_unavailable=1
			return 1
		fi
		if ! zxfer_open_send_job_completion_queue_writer_fd "$g_zfs_send_job_queue_path"; then
			zxfer_echoV "Unable to reopen rolling send/receive completion queue; falling back to batch waits."
			zxfer_close_send_job_completion_queue
			g_zfs_send_job_queue_unavailable=1
			return 1
		fi
		g_zfs_send_job_queue_writer_open=1
		return 0
	fi
	if [ "${g_zfs_send_job_queue_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.queue"
	l_queue_status=0
	zxfer_create_private_temp_dir "$l_temp_prefix" >/dev/null || l_queue_status=$?
	if [ "$l_queue_status" -ne 0 ]; then
		zxfer_echoV "Unable to create rolling send/receive completion queue; falling back to batch waits."
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi
	l_queue_dir=$g_zxfer_runtime_artifact_path_result
	l_queue_path=$l_queue_dir/queue

	l_old_umask=$(umask)
	umask 077
	if ! mkfifo "$l_queue_path"; then
		umask "$l_old_umask"
		zxfer_echoV "Unable to create rolling send/receive completion queue; falling back to batch waits."
		zxfer_cleanup_runtime_artifact_path "$l_queue_dir"
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi
	umask "$l_old_umask"

	if ! chmod 600 "$l_queue_path"; then
		zxfer_echoV "Unable to secure rolling send/receive completion queue; falling back to batch waits."
		zxfer_cleanup_runtime_artifact_path "$l_queue_dir"
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi

	l_queue_open_status=0
	zxfer_open_send_job_completion_queue_fd "$l_queue_path" ||
		l_queue_open_status=$?
	if [ "$l_queue_open_status" -ne 0 ]; then
		if [ "${g_zxfer_send_job_queue_open_failure_fatal:-0}" -eq 1 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_queue_dir" >/dev/null 2>&1 || :
			zxfer_throw_error \
				"${g_zxfer_cleanup_pid_abort_failure_message:-Failed to stop the rolling send/receive completion queue open helper after queue setup failed.}" \
				"$l_queue_open_status"
			return "$l_queue_open_status"
		fi
		zxfer_echoV "Unable to open rolling send/receive completion queue; falling back to batch waits."
		zxfer_cleanup_runtime_artifact_path "$l_queue_dir"
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi

	g_zfs_send_job_queue_open=1
	g_zfs_send_job_queue_path=$l_queue_path
	g_zfs_send_job_queue_dir=$l_queue_dir
	g_zfs_send_job_queue_writer_open=1
	return 0
}

# Purpose: Open the send job completion queue file descriptor and publish the
# handles or state later helpers need.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination before asynchronous work starts using the shared coordination
# resource.
zxfer_open_send_job_completion_queue_fd() {
	l_queue_path=$1
	l_open_reader_status=0
	l_open_reader_registration_status=0
	g_zxfer_send_job_queue_open_failure_fatal=0

	# POSIX leaves read/write FIFO opens undefined. Hold a short-lived
	# reader so the parent can open its write-only fd before its reader fd.
	(exec 7<"$l_queue_path") &
	l_open_reader_pid=$!
	zxfer_register_cleanup_pid \
		"$l_open_reader_pid" \
		"rolling send/receive completion queue open helper" ||
		l_open_reader_registration_status=$?
	if [ "$l_open_reader_registration_status" -ne 0 ]; then
		l_abort_status=0
		zxfer_abort_direct_child_pid \
			"$l_open_reader_pid" TERM \
			"rolling send/receive completion queue open helper" ||
			l_abort_status=$?
		if [ "$l_abort_status" -ne 0 ]; then
			l_abort_status=0
			zxfer_abort_direct_child_pid \
				"$l_open_reader_pid" KILL \
				"rolling send/receive completion queue open helper" ||
				l_abort_status=$?
		fi
		if [ "$l_abort_status" -eq 0 ]; then
			wait "$l_open_reader_pid" 2>/dev/null || :
			zxfer_unregister_cleanup_pid "$l_open_reader_pid"
		else
			g_zxfer_send_job_queue_open_failure_fatal=1
			return "$l_abort_status"
		fi
		return "$l_open_reader_registration_status"
	fi
	zxfer_open_send_job_completion_queue_writer_fd "$l_queue_path" || {
		l_open_reader_status=$?
		l_open_reader_abort_status=0
		zxfer_abort_cleanup_pid "$l_open_reader_pid" TERM >/dev/null 2>&1 ||
			l_open_reader_abort_status=$?
		if [ "$l_open_reader_abort_status" -ne 0 ]; then
			l_open_reader_abort_status=0
			zxfer_abort_cleanup_pid "$l_open_reader_pid" KILL >/dev/null 2>&1 ||
				l_open_reader_abort_status=$?
		fi
		if [ "$l_open_reader_abort_status" -eq 0 ] ||
			! kill -s 0 "$l_open_reader_pid" 2>/dev/null; then
			wait "$l_open_reader_pid" 2>/dev/null || :
			zxfer_unregister_cleanup_pid "$l_open_reader_pid"
		else
			g_zxfer_send_job_queue_open_failure_fatal=1
			return "$l_open_reader_abort_status"
		fi
		return "$l_open_reader_status"
	}
	wait "$l_open_reader_pid" 2>/dev/null || l_open_reader_status=$?
	zxfer_unregister_cleanup_pid "$l_open_reader_pid"
	if [ "$l_open_reader_status" -ne 0 ]; then
		# Never add 2>/dev/null to bare exec: it would redirect the
		# main shell's stderr for every later warning and failure report.
		exec 9>&- || true
		return "$l_open_reader_status"
	fi
	zxfer_open_send_job_completion_queue_reader_fd "$l_queue_path" || {
		l_open_reader_status=$?
		exec 9>&- || true
		return "$l_open_reader_status"
	}

	return 0
}

# Purpose: Open the rolling completion queue writer fd.
# Usage: Called before async send/receive work starts or reopens the queue.
zxfer_open_send_job_completion_queue_writer_fd() {
	exec 9>&- || true
	for l_open_attempt in 1 2 3 4 5 6 7 8; do
		{ exec 9>&1; } >"$1" && return 0
		l_open_status=$?
	done

	return "$l_open_status"
}

# Purpose: Open the rolling completion queue reader fd.
# Usage: Called after the queue writer is held so completion waits can read.
zxfer_open_send_job_completion_queue_reader_fd() {
	exec 8<&- || true
	for l_open_attempt in 1 2 3 4 5 6 7 8; do
		{ exec 8<&0; } <"$1" && return 0
		l_open_status=$?
	done

	return "$l_open_status"
}

# Purpose: Close the rolling completion queue writer fd.
# Usage: Called after work finishes or before waiting on queue notifications.
zxfer_close_send_job_completion_queue_writer_fd() {
	if [ "${g_zfs_send_job_queue_writer_open:-0}" -eq 1 ]; then
		# Never 2>/dev/null a bare exec; that can permanently redirect the
		# main shell's stderr and hide later warnings or failure reports.
		exec 9>&- || true
	fi
	g_zfs_send_job_queue_writer_open=0
}

# Purpose: Close the rolling completion queue and release related state.
# Usage: Called after protected work finishes or cleanup takes over.
zxfer_close_send_job_completion_queue() {
	zxfer_close_send_job_completion_queue_writer_fd
	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ]; then
		exec 8<&- || true
	fi
	g_zfs_send_job_queue_open=0
	if [ -n "${g_zfs_send_job_queue_dir:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zfs_send_job_queue_dir"
	elif [ -n "${g_zfs_send_job_queue_path:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zfs_send_job_queue_path"
	fi
	g_zfs_send_job_queue_path=""
	g_zfs_send_job_queue_dir=""
}

zxfer_register_supervised_send_job() {
	l_job_id=$1
	l_runner_pid=$2
	l_source_snapshot=${3:-}
	l_dest_dataset=${4:-}
	l_target_host=${5:-}

	if [ -n "${g_zfs_send_job_pids:-}" ]; then
		g_zfs_send_job_pids="$g_zfs_send_job_pids $l_runner_pid"
	else
		g_zfs_send_job_pids=$l_runner_pid
	fi

	if [ -n "${g_zfs_send_job_supervisor_records:-}" ]; then
		g_zfs_send_job_supervisor_records="$g_zfs_send_job_supervisor_records
$l_job_id	$l_runner_pid	$l_source_snapshot	$l_dest_dataset	$l_target_host"
	else
		g_zfs_send_job_supervisor_records="$l_job_id	$l_runner_pid	$l_source_snapshot	$l_dest_dataset	$l_target_host"
	fi

	g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
}

# Purpose: Resolve one supervised send-job record into the shared scratch
# state this module uses.
# Usage: Called during send/receive job scheduling, waiting, and failure
# reporting when later helpers need the tracked dataset metadata for one job.
zxfer_find_supervised_send_job_record() {
	l_job_id=$1

	g_zxfer_send_job_record_runner_pid=""
	g_zxfer_send_job_record_source_dataset=""
	g_zxfer_send_job_record_source_snapshot=""
	g_zxfer_send_job_record_dest_dataset=""
	g_zxfer_send_job_record_target_host=""

	while IFS='	' read -r l_record_job_id l_record_pid l_record_source_snapshot l_record_dest_dataset l_record_target_host || [ -n "${l_record_job_id}${l_record_pid}${l_record_source_snapshot}${l_record_dest_dataset}${l_record_target_host}" ]; do
		[ -n "$l_record_job_id" ] || continue
		[ "$l_record_job_id" = "$l_job_id" ] || continue
		g_zxfer_send_job_record_runner_pid=$l_record_pid
		g_zxfer_send_job_record_source_snapshot=$l_record_source_snapshot
		g_zxfer_send_job_record_source_dataset=$(zxfer_extract_snapshot_dataset "$l_record_source_snapshot")
		g_zxfer_send_job_record_dest_dataset=$l_record_dest_dataset
		g_zxfer_send_job_record_target_host=$l_record_target_host
		return 0
	done <<-EOF
		${g_zfs_send_job_supervisor_records:-}
	EOF

	return 1
}

# Purpose: Resolve the tracked runner PID for one supervised send job.
# Usage: Called during send/receive job waiting and cleanup when later helpers
# need the runner PID without reparsing the full tracked record.
zxfer_find_supervised_send_job_pid_by_job_id() {
	l_job_id=$1

	zxfer_find_supervised_send_job_record "$l_job_id" || return 1

	printf '%s\n' "$g_zxfer_send_job_record_runner_pid"
	return 0
}

# Purpose: Return whether two dataset paths overlap by exact match or ancestry.
# Usage: Called during send/receive scheduling when zxfer must avoid running
# parent and child receive jobs against the same destination tree at once.
zxfer_dataset_paths_conflict_by_ancestry() {
	l_left_dataset=$1
	l_right_dataset=$2

	[ -n "$l_left_dataset" ] || return 1
	[ -n "$l_right_dataset" ] || return 1
	[ "$l_left_dataset" = "$l_right_dataset" ] && return 0

	case "$l_left_dataset" in
	"$l_right_dataset"/*)
		return 0
		;;
	esac
	case "$l_right_dataset" in
	"$l_left_dataset"/*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Detect whether an active supervised send job conflicts with the
# requested destination dataset.
# Usage: Called during send/receive scheduling before zxfer backgrounds a new
# receive so parent and child destinations do not run concurrently.
zxfer_supervised_send_job_conflicts_with_destination() {
	l_target_host=${1:-}
	l_dest_dataset=$2

	g_zxfer_send_job_conflict_dest_dataset=""

	while IFS='	' read -r l_record_job_id l_record_pid l_record_source_snapshot l_record_dest_dataset l_record_target_host || [ -n "${l_record_job_id}${l_record_pid}${l_record_source_snapshot}${l_record_dest_dataset}${l_record_target_host}" ]; do
		[ -n "$l_record_job_id" ] || continue
		[ "${l_record_target_host:-}" = "${l_target_host:-}" ] || continue
		if ! zxfer_dataset_paths_conflict_by_ancestry "$l_record_dest_dataset" "$l_dest_dataset"; then
			continue
		fi
		g_zxfer_send_job_conflict_dest_dataset=$l_record_dest_dataset
		return 0
	done <<-EOF
		${g_zfs_send_job_supervisor_records:-}
	EOF

	return 1
}

# Purpose: Render one supervised send-job context for operator-facing
# diagnostics.
# Usage: Called during send/receive waits and failure handling so background
# job errors identify the dataset transfer that failed instead of only a PID.
zxfer_get_supervised_send_job_error_context() {
	l_job_id=$1

	zxfer_find_supervised_send_job_record "$l_job_id" || return 1

	l_source_label=${g_zxfer_send_job_record_source_snapshot:-$g_zxfer_send_job_record_source_dataset}
	if [ -n "$l_source_label" ] && [ -n "$g_zxfer_send_job_record_dest_dataset" ]; then
		l_context="[$l_source_label -> $g_zxfer_send_job_record_dest_dataset]"
	elif [ -n "$g_zxfer_send_job_record_dest_dataset" ]; then
		l_context="[$g_zxfer_send_job_record_dest_dataset]"
	else
		l_context="[job $l_job_id]"
	fi
	if [ -n "${g_zxfer_send_job_record_target_host:-}" ]; then
		l_context="$l_context on target [$g_zxfer_send_job_record_target_host]"
	fi

	printf '%s\n' "$l_context"
}

zxfer_finalize_supervised_send_job_success() {
	l_job_id=$1

	zxfer_find_supervised_send_job_record "$l_job_id" || return 0
	[ -n "${g_zxfer_send_job_record_dest_dataset:-}" ] || return 0

	zxfer_note_destination_receive_completed "$g_zxfer_send_job_record_dest_dataset"
	zxfer_invalidate_destination_property_mutation_cache "$g_zxfer_send_job_record_dest_dataset"
	# A dataset that was diverged and converged this run must leave its
	# receive without any remaining name-match/guid-mismatch snapshot.
	zxfer_verify_converged_destination_after_receive "$g_zxfer_send_job_record_dest_dataset"
	# The completed receive only changed the completed dataset's own snapshot
	# records, and those have no same-iteration consumers: planning for the
	# dataset already finished, other datasets filter the shared records to
	# their own paths, every send/seed decision is preceded by a live
	# destination recheck, and -Y iterations rebuild discovery state from
	# scratch. Invalidating the whole-tree destination snapshot record cache
	# here forced every later dataset to fall back to slower lookups once the
	# first background job completed, without fixing any staleness that
	# mattered.
	return 0
}

zxfer_collect_supervised_send_job_ids() {
	g_zxfer_send_job_ids_result=""
	l_job_ids=""

	while IFS='	' read -r l_record_job_id l_record_pid l_record_source_snapshot l_record_dest_dataset l_record_target_host || [ -n "${l_record_job_id}${l_record_pid}${l_record_source_snapshot}${l_record_dest_dataset}${l_record_target_host}" ]; do
		[ -n "$l_record_job_id" ] || continue
		if [ -n "$l_job_ids" ]; then
			l_job_ids=$l_job_ids"
$l_record_job_id"
		else
			l_job_ids=$l_record_job_id
		fi
	done <<-EOF
		${g_zfs_send_job_supervisor_records:-}
	EOF

	g_zxfer_send_job_ids_result=$l_job_ids
	printf '%s\n' "$l_job_ids"
}

zxfer_unregister_supervised_send_job() {
	l_job_id=$1
	l_remaining_pids=""
	l_remaining_records=""
	l_removed_pid=""

	while IFS='	' read -r l_record_job_id l_record_pid l_record_source_snapshot l_record_dest_dataset l_record_target_host || [ -n "${l_record_job_id}${l_record_pid}${l_record_source_snapshot}${l_record_dest_dataset}${l_record_target_host}" ]; do
		[ -n "$l_record_job_id" ] || continue
		if [ "$l_record_job_id" = "$l_job_id" ]; then
			l_removed_pid=$l_record_pid
			continue
		fi
		if [ -n "$l_remaining_records" ]; then
			l_remaining_records="$l_remaining_records
$l_record_job_id	$l_record_pid	$l_record_source_snapshot	$l_record_dest_dataset	$l_record_target_host"
		else
			l_remaining_records="$l_record_job_id	$l_record_pid	$l_record_source_snapshot	$l_record_dest_dataset	$l_record_target_host"
		fi
		if [ -n "$l_remaining_pids" ]; then
			l_remaining_pids="$l_remaining_pids $l_record_pid"
		else
			l_remaining_pids=$l_record_pid
		fi
	done <<-EOF
		${g_zfs_send_job_supervisor_records:-}
	EOF

	g_zfs_send_job_supervisor_records=$l_remaining_records
	g_zfs_send_job_pids=$l_remaining_pids
	if [ -n "$l_removed_pid" ] && [ "${g_count_zfs_send_jobs:-0}" -gt 0 ]; then
		g_count_zfs_send_jobs=$((g_count_zfs_send_jobs - 1))
	fi
}

# Purpose: Terminate the remaining send jobs that zxfer no longer wants to keep
# running.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when failure handling or shutdown must stop background work
# cleanly.
zxfer_terminate_remaining_send_jobs() {
	if [ -n "${g_zfs_send_job_supervisor_records:-}" ]; then
		zxfer_collect_supervised_send_job_ids >/dev/null || return "$?"
		l_job_ids=$g_zxfer_send_job_ids_result
		l_terminate_send_jobs_abort_status=0
		l_first_abort_failure_message=""
		while IFS= read -r l_job_id || [ -n "$l_job_id" ]; do
			[ -n "$l_job_id" ] || continue
			l_current_abort_status=0
			zxfer_abort_background_job "$l_job_id" TERM || l_current_abort_status=$?
			if [ "$l_current_abort_status" -ne 0 ]; then
				[ -n "$l_first_abort_failure_message" ] || l_first_abort_failure_message=${g_zxfer_background_job_abort_failure_message:-}
				[ "$l_terminate_send_jobs_abort_status" -ne 0 ] ||
					l_terminate_send_jobs_abort_status=$l_current_abort_status
				continue
			fi
			zxfer_unregister_supervised_send_job "$l_job_id"
		done <<-EOF
			$l_job_ids
		EOF

		zxfer_close_send_job_completion_queue
		if [ "$l_terminate_send_jobs_abort_status" -ne 0 ]; then
			zxfer_set_background_job_abort_failure_message "$l_first_abort_failure_message"
			return "$l_terminate_send_jobs_abort_status"
		fi
		g_zfs_send_job_pids=""
		g_zfs_send_job_supervisor_records=""
		g_count_zfs_send_jobs=0
		return 0
	fi

	l_terminate_send_jobs_abort_status=0
	l_first_abort_failure_message=""
	l_terminate_remaining_send_jobs_pids=$(zxfer_split_tokens_on_whitespace "${g_zfs_send_job_pids:-}")
	while IFS= read -r l_terminate_remaining_send_jobs_pid || [ -n "$l_terminate_remaining_send_jobs_pid" ]; do
		[ -n "$l_terminate_remaining_send_jobs_pid" ] || continue
		l_current_abort_status=0
		zxfer_abort_cleanup_pid "$l_terminate_remaining_send_jobs_pid" TERM || l_current_abort_status=$?
		if [ "$l_current_abort_status" -ne 0 ]; then
			[ -n "$l_first_abort_failure_message" ] || l_first_abort_failure_message=$g_zxfer_cleanup_pid_abort_failure_message
			[ "$l_terminate_send_jobs_abort_status" -ne 0 ] ||
				l_terminate_send_jobs_abort_status=$l_current_abort_status
			continue
		fi
		wait "$l_terminate_remaining_send_jobs_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$l_terminate_remaining_send_jobs_pid"
	done <<EOF
$l_terminate_remaining_send_jobs_pids
EOF

	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=0
	zxfer_close_send_job_completion_queue
	if [ "$l_terminate_send_jobs_abort_status" -ne 0 ]; then
		zxfer_set_cleanup_pid_abort_failure_message "$l_first_abort_failure_message"
		return "$l_terminate_send_jobs_abort_status"
	fi
	return 0
}

# Purpose: Terminate remaining supervised send jobs, then throw the requested
# operator-facing error unless cleanup itself must take precedence.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination on supervised failure paths so cleanup-precedence rules stay in
# one place.
zxfer_throw_supervised_send_job_error_after_cleanup() {
	l_error_message=$1
	l_error_status=${2:-}
	l_cleanup_status=0

	zxfer_terminate_remaining_send_jobs || l_cleanup_status=$?
	if [ "$l_cleanup_status" -ne 0 ]; then
		zxfer_throw_error "${g_zxfer_background_job_abort_failure_message:-Failed to tear down supervised send/receive jobs.}" "$l_cleanup_status"
	fi
	if [ "$#" -ge 2 ]; then
		zxfer_throw_error "$l_error_message" "$l_error_status"
	fi
	zxfer_throw_error "$l_error_message"
}

zxfer_wait_for_next_supervised_zfs_send_job_completion() {
	l_reason=$1
	l_completed_record=""
	l_job_context=""
	l_pid=""
	l_pid_status=""

	[ "${g_count_zfs_send_jobs:-0}" -gt 0 ] || return 0

	if [ "${g_zfs_send_job_queue_open:-0}" -ne 1 ] || [ -z "${g_zfs_send_job_supervisor_records:-}" ]; then
		zxfer_wait_for_zfs_send_jobs "$l_reason"
		return 0
	fi

	zxfer_close_send_job_completion_queue_writer_fd
	if ! IFS= read -r l_completed_record <&8; then
		zxfer_close_send_job_completion_queue
		g_zfs_send_job_queue_unavailable=1
		zxfer_wait_for_supervised_zfs_send_jobs_batch
		return 0
	fi

	zxfer_parse_background_job_queue_record "$l_completed_record"
	l_queue_record_status=${g_zxfer_background_job_queue_record_status:-}
	case $l_queue_record_status in
	'' | *[!0-9]*)
		l_queue_record_status=125
		;;
	esac
	if [ "${g_zxfer_background_job_queue_record_job_id:-}" = "" ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to parse a completed zfs send/receive job notification."
	fi

	l_pid_lookup_status=0
	l_pid=$(zxfer_find_supervised_send_job_pid_by_job_id "$g_zxfer_background_job_queue_record_job_id") ||
		l_pid_lookup_status=$?
	if [ "$l_pid_lookup_status" -ne 0 ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to match a completed zfs send/receive job to a tracked PID." "$l_pid_lookup_status"
	fi
	l_job_context=$(zxfer_get_supervised_send_job_error_context "$g_zxfer_background_job_queue_record_job_id" || printf '[job %s]' "$g_zxfer_background_job_queue_record_job_id")

	l_wait_helper_status=0
	zxfer_wait_for_background_job "$g_zxfer_background_job_queue_record_job_id" ||
		l_wait_helper_status=$?
	if [ "$l_wait_helper_status" -ne 0 ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to read zfs send/receive completion metadata for $l_job_context." "$l_wait_helper_status"
	fi
	l_pid_status=$g_zxfer_background_job_wait_exit_status
	if [ "${g_zxfer_background_job_queue_record_type:-}" != "completion_write_failed" ] &&
		[ "${g_zxfer_background_job_wait_report_failure:-}" = "" ] &&
		[ "$l_pid_status" -eq 0 ]; then
		zxfer_finalize_supervised_send_job_success "$g_zxfer_background_job_queue_record_job_id"
	fi
	zxfer_unregister_supervised_send_job "$g_zxfer_background_job_queue_record_job_id"
	if [ "${g_count_zfs_send_jobs:-0}" -eq 0 ]; then
		zxfer_close_send_job_completion_queue
	fi

	if [ "${g_zxfer_background_job_queue_record_type:-}" = "completion_write_failed" ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to record zfs send/receive background completion for $l_job_context (PID $l_pid, exit $l_queue_record_status)." "$l_queue_record_status"
	fi
	if [ "${g_zxfer_background_job_wait_report_failure:-}" = "queue_write" ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to publish zfs send/receive background completion for $l_job_context (PID $l_pid, exit $l_pid_status)." "$l_pid_status"
	fi
	if [ "${g_zxfer_background_job_wait_report_failure:-}" = "completion_write" ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to report zfs send/receive background completion for $l_job_context (PID $l_pid, exit $l_pid_status)." "$l_pid_status"
	fi
	if [ "$l_pid_status" -ne 0 ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"zfs send/receive job failed for $l_job_context (PID $l_pid, exit $l_pid_status)." "$l_pid_status"
	fi
}

zxfer_wait_for_supervised_zfs_send_jobs_batch() {
	zxfer_collect_supervised_send_job_ids >/dev/null ||
		zxfer_throw_error "Failed to collect supervised send/receive job ids." "$?"
	l_job_ids=$g_zxfer_send_job_ids_result

	while IFS= read -r l_job_id || [ -n "$l_job_id" ]; do
		[ -n "$l_job_id" ] || continue
		l_pid=$(zxfer_find_supervised_send_job_pid_by_job_id "$l_job_id" || :)
		l_job_context=$(zxfer_get_supervised_send_job_error_context "$l_job_id" || printf '[job %s]' "$l_job_id")
		l_wait_helper_status=0
		zxfer_wait_for_background_job "$l_job_id" || l_wait_helper_status=$?
		if [ "$l_wait_helper_status" -ne 0 ]; then
			zxfer_throw_supervised_send_job_error_after_cleanup \
				"Failed to read zfs send/receive completion metadata for $l_job_context." "$l_wait_helper_status"
		fi
		l_pid_status=$g_zxfer_background_job_wait_exit_status
		if [ "${g_zxfer_background_job_wait_report_failure:-}" = "" ] &&
			[ "$l_pid_status" -eq 0 ]; then
			zxfer_finalize_supervised_send_job_success "$l_job_id"
		fi
		zxfer_unregister_supervised_send_job "$l_job_id"
		if [ "${g_zxfer_background_job_wait_report_failure:-}" = "queue_write" ]; then
			zxfer_throw_supervised_send_job_error_after_cleanup \
				"Failed to publish zfs send/receive background completion for $l_job_context (PID $l_pid, exit $l_pid_status)." "$l_pid_status"
		fi
		if [ "${g_zxfer_background_job_wait_report_failure:-}" = "completion_write" ]; then
			zxfer_throw_supervised_send_job_error_after_cleanup \
				"Failed to report zfs send/receive background completion for $l_job_context (PID $l_pid, exit $l_pid_status)." "$l_pid_status"
		fi
		if [ "$l_pid_status" -ne 0 ]; then
			zxfer_throw_supervised_send_job_error_after_cleanup \
				"zfs send/receive job failed for $l_job_context (PID $l_pid, exit $l_pid_status)." "$l_pid_status"
		fi
	done <<-EOF
		$l_job_ids
	EOF

	g_zfs_send_job_pids=""
	g_zfs_send_job_supervisor_records=""
	g_count_zfs_send_jobs=0
	zxfer_close_send_job_completion_queue
}

# Purpose: Wait for the for next ZFS send job completion to reach the state
# this module expects.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later steps must block until background work or shared
# state catches up.
zxfer_wait_for_next_zfs_send_job_completion() {
	zxfer_wait_for_next_supervised_zfs_send_job_completion "$1"
}

# Purpose: Wait for the for ZFS send jobs to reach the state this module
# expects.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later steps must block until background work or shared
# state catches up.
zxfer_wait_for_zfs_send_jobs() {
	l_reason=$1

	if [ "$l_reason" != "" ] && [ -n "$g_zfs_send_job_pids" ]; then
		zxfer_echoV "Waiting for zfs send/receive jobs ($l_reason)."
	fi

	if [ -z "$g_zfs_send_job_pids" ]; then
		g_count_zfs_send_jobs=0
		zxfer_close_send_job_completion_queue
		return 0
	fi

	if [ -z "${g_zfs_send_job_supervisor_records:-}" ]; then
		zxfer_throw_supervised_send_job_error_after_cleanup \
			"Failed to match tracked zfs send/receive PIDs to supervised job records."
	fi

	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ]; then
		while [ "${g_count_zfs_send_jobs:-0}" -gt 0 ]; do
			zxfer_wait_for_next_supervised_zfs_send_job_completion ""
		done
		g_zfs_send_job_pids=""
		g_zfs_send_job_supervisor_records=""
		g_count_zfs_send_jobs=0
		zxfer_close_send_job_completion_queue
		return 0
	fi

	zxfer_wait_for_supervised_zfs_send_jobs_batch
}

# Purpose: Execute or schedule one fully rendered send/receive pipeline.
# Usage: Called after transfer command construction so queue limits, ancestry
# serialization, and rolling-completion recovery stay in the send-job concern.
# Side effects: Publishes whether the pipeline was scheduled in the background.
zxfer_schedule_send_receive_pipeline() {
	l_send_schedule_exec_command=$1
	l_send_schedule_display_command=$2
	l_send_schedule_current_snapshot=$3
	l_send_schedule_destination=$4
	l_send_schedule_allow_background=$5
	g_zxfer_send_receive_ran_in_background_result=0

	l_send_schedule_job_limit=${g_option_j_jobs:-1}
	case $l_send_schedule_job_limit in
	'' | *[!0-9]*)
		l_send_schedule_job_limit=1
		;;
	esac

	if [ "$l_send_schedule_allow_background" -ne 1 ] ||
		[ "$l_send_schedule_job_limit" -le 1 ] ||
		[ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_execute_rendered_shell_command "$l_send_schedule_exec_command" 0 \
			"$l_send_schedule_display_command"
		return "$?"
	fi

	l_send_schedule_use_rolling_pool=0
	if zxfer_open_send_job_completion_queue; then
		l_send_schedule_use_rolling_pool=1
	fi

	while :; do
		l_send_schedule_wait_reason=""
		if [ "$g_count_zfs_send_jobs" -ge "$l_send_schedule_job_limit" ]; then
			l_send_schedule_wait_reason="job limit"
			zxfer_echov "Max jobs reached [$g_count_zfs_send_jobs]. Waiting for jobs to complete."
		elif [ -n "${g_zfs_send_job_supervisor_records:-}" ] &&
			zxfer_supervised_send_job_conflicts_with_destination \
				"$g_option_T_target_host" "$l_send_schedule_destination"; then
			l_send_schedule_wait_reason="destination ancestry"
			zxfer_echov "Waiting for conflicting zfs send/receive ancestry to finish for destination [$l_send_schedule_destination]; active destination [${g_zxfer_send_job_conflict_dest_dataset:-unknown}] is still running."
		fi
		[ -n "$l_send_schedule_wait_reason" ] || break
		if [ "$l_send_schedule_use_rolling_pool" -eq 1 ]; then
			zxfer_wait_for_next_zfs_send_job_completion \
				"$l_send_schedule_wait_reason"
		else
			zxfer_wait_for_zfs_send_jobs "$l_send_schedule_wait_reason"
		fi

		# A rolling wait closes the queue writer while it blocks on the reader.
		# Reopen before the next spawn; if that fails, drain supervised jobs and
		# continue with the batch-wait path.
		if [ "$l_send_schedule_use_rolling_pool" -eq 1 ] &&
			[ "${g_zfs_send_job_queue_writer_open:-0}" -ne 1 ]; then
			if ! zxfer_open_send_job_completion_queue; then
				if [ -n "${g_zfs_send_job_pids:-}" ]; then
					zxfer_wait_for_zfs_send_jobs "rolling queue recovery"
				fi
				l_send_schedule_use_rolling_pool=0
			fi
		fi
	done

	zxfer_profile_increment_counter \
		g_zxfer_profile_send_receive_background_pipeline_commands
	zxfer_record_last_command_string "$l_send_schedule_exec_command"
	zxfer_echov "$l_send_schedule_display_command"
	if [ "$l_send_schedule_use_rolling_pool" -eq 1 ]; then
		l_send_schedule_notify_fd=9
	else
		l_send_schedule_notify_fd=""
	fi
	g_zxfer_send_receive_ran_in_background_result=1
	zxfer_spawn_supervised_background_job \
		"send_receive" \
		"$l_send_schedule_exec_command" \
		"$l_send_schedule_display_command" \
		"" \
		"" \
		"$l_send_schedule_notify_fd"
	zxfer_register_supervised_send_job \
		"$g_zxfer_background_job_last_id" \
		"$g_zxfer_background_job_last_runner_pid" \
		"$l_send_schedule_current_snapshot" \
		"$l_send_schedule_destination" \
		"$g_option_T_target_host"
}

# Purpose: Run one guarded ZFS send/receive transfer, including progress setup,
# compression, and optional background-job tracking.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination after replication planning has chosen the exact snapshot range
# and destination.
#
# Handle zfs send/receive
# Takes $g_option_D_display_progress_bar $g_option_z_compress, $g_option_O_origin_host, $g_option_T_target_host
