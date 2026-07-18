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
# shellcheck shell=sh disable=SC2016,SC2034,SC2154

################################################################################
# BACKGROUND JOB SUPERVISION (supervision-lite)
################################################################################

# Module contract:
# owns globals: background-job registry, spawn/wait/abort scratch, setsid cache.
# reads globals: runtime artifact/temp helpers and cleanup wrapper path lookup.
# mutates caches: runtime artifact tracking through shared temp helpers.
# returns via stdout: background job ids.
#
# Supervision-lite model: spawn runs the pipeline in one backgrounded job
# shell. With setsid(1), that shell leads a process group; without setsid it
# runs through the cleanup child wrapper, whose TERM trap bounds direct-child
# teardown and token-validates best-effort cleanup of cooperative descendants.
# - The job shell writes the pipeline exit status to a per-run temp
#   status file before queue notification; missing or malformed status means
#   the shell died abnormally.
# - SAFETY INVARIANT: root PID/PGID signals use zxfer-owned registered `$!`
#   values before wait, preserving the supervision-lite baseline without a
#   per-job ps spawn. Wrapper descendants are snapshotted and token-validated
#   only on abort.

# Purpose: Reset the background-job registry and scratch state so the next
# runtime pass starts from a clean state.
# Usage: Called during runtime bootstrap and from suites before this module
# reuses mutable scratch globals or cached decisions.
zxfer_reset_background_job_state() {
	g_zxfer_background_job_records=""
	g_zxfer_background_job_sequence=0
	g_zxfer_background_job_last_id=""
	g_zxfer_background_job_last_runner_pid=""
	g_zxfer_background_job_last_status_file=""
	g_zxfer_background_job_record_pid=""
	g_zxfer_background_job_record_teardown=""
	g_zxfer_background_job_record_status_file=""
	g_zxfer_background_job_completion_exit_status=""
	g_zxfer_background_job_completion_report_failure=""
	g_zxfer_background_job_wait_exit_status=""
	g_zxfer_background_job_wait_report_failure=""
	g_zxfer_background_job_abort_failure_message=""
	g_zxfer_background_job_queue_record_type=""
	g_zxfer_background_job_queue_record_job_id=""
	g_zxfer_background_job_queue_record_status=""
	# Abort grace and the setsid capability survive resets: the grace window
	# is an environment-independent policy knob and the capability is a fact
	# about the host, probed at most once per process.
	g_zxfer_background_job_abort_grace_seconds=${g_zxfer_background_job_abort_grace_seconds:-1}
	g_zxfer_background_job_use_setsid=${g_zxfer_background_job_use_setsid:-}
}

# Purpose: Discard inherited supervisor state without signalling or waiting on
# any referenced process.
# Usage: Called by the session composition root before traps are installed so
# exported internal globals cannot grant process-cleanup ownership.
zxfer_discard_background_job_cleanup_state() {
	g_zxfer_background_job_use_setsid=""
	g_zxfer_background_job_abort_grace_seconds=1
	zxfer_reset_background_job_state
}

# Purpose: Publish an aggregate supervisor abort diagnostic.
# Usage: Domain schedulers call this after attempting every tracked abort so
# cleanup reporting retains the first failure message.
zxfer_set_background_job_abort_failure_message() {
	g_zxfer_background_job_abort_failure_message=${1:-}
}

# Purpose: Allocate the next unique background job id for this process.
# Usage: Called during spawn; publishes the id in
# $g_zxfer_background_job_last_id and echoes it for capture-style callers.
zxfer_next_background_job_id() {
	g_zxfer_background_job_sequence=$((g_zxfer_background_job_sequence + 1))
	g_zxfer_background_job_last_id="bgjob.$$.$g_zxfer_background_job_sequence"
	printf '%s\n' "$g_zxfer_background_job_last_id"
}

# Purpose: Feature-test setsid once per process and cache whether spawned job
# shells lead their own process group (pid == pgid).
# Usage: Called lazily by the first spawn (memoized through
# $g_zxfer_background_job_use_setsid); suites may pre-set the flag to force
# either spawn path.
zxfer_init_background_job_spawn_support() {
	if [ -n "${g_zxfer_background_job_use_setsid:-}" ]; then
		return 0
	fi
	g_zxfer_background_job_use_setsid=0
	command -v setsid >/dev/null 2>&1 || return 0
	# The probe child prints its own pid and pgid; requiring them to match
	# pins the invariant the process-group teardown relies on. A setsid that
	# forks (or fails) yields a mismatch and falls back to the wrapper path.
	l_spawn_probe=$(setsid sh -c 'printf "%s " "$$"; ps -o pgid= -p "$$"' 2>/dev/null) ||
		return 0
	case $- in
	*f*)
		l_background_spawn_probe_restore_glob=0
		;;
	*)
		l_background_spawn_probe_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_background_spawn_probe_saved_ifs_set=1
		l_background_spawn_probe_saved_ifs=$IFS
	else
		l_background_spawn_probe_saved_ifs_set=0
		l_background_spawn_probe_saved_ifs=""
	fi
	# Use the shell's default whitespace field splitting regardless of caller
	# state, then restore both IFS and pathname expansion before branching.
	unset IFS
	# shellcheck disable=SC2086  # probe output is two space-separated fields
	set -- $l_spawn_probe
	l_background_spawn_probe_argc=$#
	l_background_spawn_probe_pid=${1:-}
	l_background_spawn_probe_pgid=${2:-}
	if [ "$l_background_spawn_probe_saved_ifs_set" -eq 1 ]; then
		IFS=$l_background_spawn_probe_saved_ifs
	else
		unset IFS
	fi
	if [ "$l_background_spawn_probe_restore_glob" -eq 1 ]; then
		set +f
	fi
	if [ "$l_background_spawn_probe_argc" -ne 2 ]; then
		return 0
	fi
	case "$l_background_spawn_probe_pid$l_background_spawn_probe_pgid" in
	*[!0-9]*)
		return 0
		;;
	esac
	if [ "$l_background_spawn_probe_pid" = "$l_background_spawn_probe_pgid" ]; then
		g_zxfer_background_job_use_setsid=1
	fi
	return 0
}

# Purpose: Register one background job with the in-memory registry owned by
# this module.
# Usage: Called during spawn so wait/abort and trap cleanup can find the live
# job. One tab-separated row per job: job_id, kind, pid, teardown mode
# (process_group | wrapper), status file path.
zxfer_register_background_job_record() {
	l_job_id=$1
	l_kind=$2
	l_pid=$3
	l_teardown=$4
	l_status_file=$5

	[ -n "$l_job_id" ] || return 1
	[ -n "$l_pid" ] || return 1
	[ -n "$l_status_file" ] || return 1

	while IFS='	' read -r l_existing_job_id l_existing_kind l_existing_pid l_existing_teardown l_existing_status_file || [ -n "${l_existing_job_id}${l_existing_kind}${l_existing_pid}${l_existing_teardown}${l_existing_status_file}" ]; do
		[ -n "$l_existing_job_id" ] || continue
		[ "$l_existing_job_id" = "$l_job_id" ] && return 0
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	if [ -n "${g_zxfer_background_job_records:-}" ]; then
		g_zxfer_background_job_records=$g_zxfer_background_job_records"
$l_job_id	$l_kind	$l_pid	$l_teardown	$l_status_file"
	else
		g_zxfer_background_job_records="$l_job_id	$l_kind	$l_pid	$l_teardown	$l_status_file"
	fi

	return 0
}

# Purpose: Find one tracked background job record by job id.
# Usage: Called during wait and abort; publishes the row fields in the
# g_zxfer_background_job_record_* scratch globals.
zxfer_find_background_job_record() {
	l_job_id=$1

	g_zxfer_background_job_record_pid=""
	g_zxfer_background_job_record_teardown=""
	g_zxfer_background_job_record_status_file=""

	while IFS='	' read -r l_existing_job_id l_existing_kind l_existing_pid l_existing_teardown l_existing_status_file || [ -n "${l_existing_job_id}${l_existing_kind}${l_existing_pid}${l_existing_teardown}${l_existing_status_file}" ]; do
		[ -n "$l_existing_job_id" ] || continue
		[ "$l_existing_job_id" = "$l_job_id" ] || continue
		g_zxfer_background_job_record_pid=$l_existing_pid
		g_zxfer_background_job_record_teardown=$l_existing_teardown
		g_zxfer_background_job_record_status_file=$l_existing_status_file
		return 0
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	return 1
}

# Purpose: Remove one background job from the in-memory registry.
# Usage: Called after a job has been waited on or aborted.
zxfer_unregister_background_job_record() {
	l_job_id=$1
	l_remaining_records=""

	while IFS='	' read -r l_existing_job_id l_existing_kind l_existing_pid l_existing_teardown l_existing_status_file || [ -n "${l_existing_job_id}${l_existing_kind}${l_existing_pid}${l_existing_teardown}${l_existing_status_file}" ]; do
		[ -n "$l_existing_job_id" ] || continue
		[ "$l_existing_job_id" = "$l_job_id" ] && continue
		if [ -n "$l_remaining_records" ]; then
			l_remaining_records=$l_remaining_records"
$l_existing_job_id	$l_existing_kind	$l_existing_pid	$l_existing_teardown	$l_existing_status_file"
		else
			l_remaining_records="$l_existing_job_id	$l_existing_kind	$l_existing_pid	$l_existing_teardown	$l_existing_status_file"
		fi
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	g_zxfer_background_job_records=$l_remaining_records
}

# Purpose: Tear down an unregistered direct-child job after registry insertion
# fails, promoting the first cleanup failure over the earlier registration
# status while still reaping the child and removing its status artifact.
# Usage: Called only by spawn before the direct child has been waited on.
zxfer_fail_background_job_registration() {
	l_registration_failure_job_id=$1
	l_registration_failure_pid=$2
	l_registration_failure_teardown=$3
	l_registration_failure_status_file=$4
	l_registration_failure_status=$5
	l_registration_failure_teardown_status=0

	zxfer_signal_background_job_scope \
		"$l_registration_failure_pid" "$l_registration_failure_teardown" TERM ||
		l_registration_failure_teardown_status=$?
	zxfer_background_job_abort_grace_wait
	zxfer_signal_background_job_scope \
		"$l_registration_failure_pid" "$l_registration_failure_teardown" KILL || {
		l_registration_failure_kill_status=$?
		[ "$l_registration_failure_teardown_status" -ne 0 ] ||
			l_registration_failure_teardown_status=$l_registration_failure_kill_status
	}
	wait "$l_registration_failure_pid" 2>/dev/null || :
	zxfer_cleanup_runtime_artifact_path \
		"$l_registration_failure_status_file" >/dev/null 2>&1 || :
	if [ "$l_registration_failure_teardown_status" -ne 0 ]; then
		l_registration_failure_message=${g_zxfer_background_job_abort_failure_message:-Failed to tear down unregistered background job [$l_registration_failure_job_id].}
		zxfer_throw_error \
			"$l_registration_failure_message" "$l_registration_failure_teardown_status"
	fi
	zxfer_throw_error \
		"Failed to register background job [$l_registration_failure_job_id]." \
		"$l_registration_failure_status"
}

# Purpose: Spawn one supervised background job that runs the caller's command
# string, records its exit status, and optionally notifies the rolling
# completion queue.
# Usage: Called during send/receive job scheduling (and any future background
# consumers) with: kind, exec command, display command (carried by the
# caller's own records; accepted for call-site compatibility), optional
# stdout capture file, optional stderr capture file, optional notify fd.
# Side effects: Publishes the job id, job-shell pid, and status file in
# $g_zxfer_background_job_last_id, $g_zxfer_background_job_last_runner_pid,
# and $g_zxfer_background_job_last_status_file.
zxfer_spawn_supervised_background_job() {
	l_kind=$1
	l_exec_cmd=$2
	l_display_cmd=$3
	l_output_file=${4:-}
	l_error_file=${5:-}
	l_notify_fd=${6:-}

	g_zxfer_background_job_last_id=""
	g_zxfer_background_job_last_runner_pid=""
	g_zxfer_background_job_last_status_file=""

	zxfer_init_background_job_spawn_support
	zxfer_next_background_job_id >/dev/null
	l_job_id=$g_zxfer_background_job_last_id
	zxfer_get_temp_file >/dev/null
	l_status_file=$g_zxfer_temp_file_result

	l_status_file_safe=$(zxfer_build_shell_command_from_argv "$l_status_file") || {
		l_spawn_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_status_file" >/dev/null 2>&1 || :
		zxfer_throw_error "Failed to quote the background job [$l_job_id] status file path." "$l_spawn_status"
	}

	l_redirections=""
	if [ -n "$l_output_file" ]; then
		l_output_file_safe=$(zxfer_build_shell_command_from_argv "$l_output_file") || {
			l_spawn_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_status_file" >/dev/null 2>&1 || :
			zxfer_throw_error "Failed to quote the background job [$l_job_id] output file path." "$l_spawn_status"
		}
		l_redirections=" > $l_output_file_safe"
	fi
	if [ -n "$l_error_file" ]; then
		l_error_file_safe=$(zxfer_build_shell_command_from_argv "$l_error_file") || {
			l_spawn_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_status_file" >/dev/null 2>&1 || :
			zxfer_throw_error "Failed to quote the background job [$l_job_id] error file path." "$l_spawn_status"
		}
		l_redirections="$l_redirections 2> $l_error_file_safe"
	fi

	# The job shell records its own completion: pipeline first (per-stage
	# exit captures inside $l_exec_cmd stay untouched), then the status-file
	# write, then the queue notification -- in that order, so a queue reader
	# always finds the status file already written.
	l_notify_cmds=""
	l_notify_write_failed_cmd=""
	case "$l_notify_fd" in
	'' | *[!0-9]*) ;;
	*)
		l_notify_write_failed_cmd="printf 'completion_write_failed\t%s\t%s\n' '$l_job_id' \"\$l_zxfer_job_status\" >&$l_notify_fd 2>/dev/null || :
	"
		l_notify_cmds="
if ! printf '%s\n' '$l_job_id' >&$l_notify_fd 2>/dev/null; then
	if ! printf 'report_failure\tqueue_write\n' >> $l_status_file_safe 2>/dev/null; then
		rm -f $l_status_file_safe 2>/dev/null || :
	fi
	printf '%s\n' 'Failed to publish background job completion for [$l_job_id].' >&2
	exit 125
fi"
		;;
	esac
	# The pipeline runs in a subshell so an "exit N" inside the caller's
	# command string (per-stage exit captures) ends the pipeline, not the
	# job shell, and the status write below still happens.
	l_job_cmd="l_zxfer_job_status=0
(
$l_exec_cmd
)$l_redirections || l_zxfer_job_status=\$?
if ! printf 'status\t%s\n' \"\$l_zxfer_job_status\" > $l_status_file_safe 2>/dev/null; then
	printf '%s\n' 'Failed to record background job [$l_job_id] status.' >&2
	${l_notify_write_failed_cmd}exit 125
fi$l_notify_cmds
exit \"\$l_zxfer_job_status\""

	if [ "${g_zxfer_background_job_use_setsid:-0}" = "1" ]; then
		l_teardown=process_group
		setsid sh -c "$l_job_cmd" &
	else
		l_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path) || {
			l_spawn_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_status_file" >/dev/null 2>&1 || :
			zxfer_throw_error "Failed to locate the background job cleanup wrapper." "$l_spawn_status"
		}
		l_teardown=wrapper
		/bin/sh "$l_wrapper_script" "$l_job_cmd" &
	fi
	l_job_pid=$!
	g_zxfer_background_job_last_runner_pid=$l_job_pid

	l_spawn_status=0
	zxfer_register_background_job_record \
		"$l_job_id" \
		"$l_kind" \
		"$l_job_pid" \
		"$l_teardown" \
		"$l_status_file" ||
		l_spawn_status=$?
	if [ "$l_spawn_status" -ne 0 ]; then
		# The job shell is still our un-reaped child here, so the teardown
		# signals cannot reach an unrelated process.
		zxfer_fail_background_job_registration \
			"$l_job_id" "$l_job_pid" "$l_teardown" \
			"$l_status_file" "$l_spawn_status"
	fi

	g_zxfer_background_job_last_id=$l_job_id
	g_zxfer_background_job_last_runner_pid=$l_job_pid
	g_zxfer_background_job_last_status_file=$l_status_file
	return 0
}

# Purpose: Read one background job status file from staged state into the
# current shell, failing closed on malformed contents.
# Usage: Called by the completion-status helper; publishes the parsed fields
# in $g_zxfer_background_job_completion_exit_status and
# $g_zxfer_background_job_completion_report_failure.
zxfer_read_background_job_status_file() {
	l_status_file=$1
	l_read_status=0
	l_status_seen=0
	l_report_failure_seen=0

	g_zxfer_background_job_completion_exit_status=""
	g_zxfer_background_job_completion_report_failure=""

	zxfer_read_runtime_artifact_file "$l_status_file" >/dev/null ||
		l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	# A completed writer always terminates its last protocol row. Remove
	# exactly that delimiter before the here-document supplies its own; any
	# remaining blank row is then real malformed input rather than a parser
	# artifact.
	case $g_zxfer_runtime_artifact_read_result in
	*'
')
		l_status_contents=${g_zxfer_runtime_artifact_read_result%?}
		;;
	*)
		return 1
		;;
	esac
	while IFS='	' read -r l_key l_value || [ -n "${l_key}${l_value}" ]; do
		case $l_key in
		status)
			[ "$l_status_seen" -eq 0 ] || return 1
			case "$l_value" in
			0 | [1-9] | [1-9][0-9] | [12][0-9][0-9])
				:
				;;
			*)
				return 1
				;;
			esac
			[ "$l_value" -le 255 ] || return 1
			g_zxfer_background_job_completion_exit_status=$l_value
			l_status_seen=1
			;;
		report_failure)
			[ "$l_report_failure_seen" -eq 0 ] || return 1
			case "$l_value" in
			'' | queue_write | completion_write)
				:
				;;
			*)
				return 1
				;;
			esac
			g_zxfer_background_job_completion_report_failure=$l_value
			l_report_failure_seen=1
			;;
		*)
			# The file is a private protocol, not an extensible key/value
			# store. Unknown or blank records indicate truncation or
			# corruption and must not be treated as a successful completion.
			return 1
			;;
		esac
	done <<-EOF || l_read_status=$?
		$l_status_contents
	EOF

	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	[ "$l_status_seen" -eq 1 ]
}

# Purpose: Return the background job completion status in the form expected by
# wait callers.
# Usage: Called after the job shell has been reaped, with the status file path
# and the wait(1) status. A missing status file means the job shell died
# before its status write -- abnormal death -- and is reported through the
# completion_write failure marker with the waited status preserved.
zxfer_get_background_job_completion_status() {
	l_status_file=$1
	l_wait_status=$2

	g_zxfer_background_job_completion_exit_status=$l_wait_status
	g_zxfer_background_job_completion_report_failure=""

	if [ ! -f "$l_status_file" ]; then
		g_zxfer_background_job_completion_report_failure=completion_write
		return 0
	fi
	zxfer_read_background_job_status_file "$l_status_file" ||
		return "$?"

	return 0
}

# Purpose: Wait for one tracked background job, reap it, and publish its
# recorded completion status.
# Usage: Called during send/receive job coordination; publishes results in the
# g_zxfer_background_job_wait_* globals and removes the registry row plus the
# status file. Returns non-zero for unknown jobs or malformed status files.
zxfer_wait_for_background_job() {
	l_job_id=$1
	l_wait_status=0

	g_zxfer_background_job_wait_exit_status=""
	g_zxfer_background_job_wait_report_failure=""

	zxfer_find_background_job_record "$l_job_id" || return 1

	wait "$g_zxfer_background_job_record_pid" 2>/dev/null || l_wait_status=$?
	l_completion_status=0
	zxfer_get_background_job_completion_status \
		"$g_zxfer_background_job_record_status_file" \
		"$l_wait_status" ||
		l_completion_status=$?
	if [ "$l_completion_status" -ne 0 ]; then
		zxfer_unregister_background_job_record "$l_job_id"
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_background_job_record_status_file" >/dev/null 2>&1 || :
		return "$l_completion_status"
	fi

	g_zxfer_background_job_wait_exit_status=$g_zxfer_background_job_completion_exit_status
	g_zxfer_background_job_wait_report_failure=$g_zxfer_background_job_completion_report_failure

	zxfer_unregister_background_job_record "$l_job_id"
	zxfer_cleanup_runtime_artifact_path "$g_zxfer_background_job_record_status_file" >/dev/null 2>&1 || :
	return 0
}

# Purpose: Capture descendant PID/start-token pairs from one process-table
# snapshot so a later numeric PID can be rejected if it has been recycled.
# Usage: Wrapper-mode KILL teardown calls this after stopping the root process.
zxfer_capture_background_job_descendant_identity_records() {
	l_descendant_identity_root=$1
	l_descendant_identity_selector=lstart

	if l_descendant_identity_snapshot=$(LC_ALL=C ps -A -o pid= -o ppid= -o lstart= 2>/dev/null); then
		:
	elif l_descendant_identity_snapshot=$(LC_ALL=C ps -A -o pid -o ppid -o lstart 2>/dev/null); then
		:
	else
		l_descendant_identity_selector=stime
		if l_descendant_identity_snapshot=$(LC_ALL=C ps -A -o pid= -o ppid= -o stime= 2>/dev/null); then
			:
		else
			l_descendant_identity_snapshot=$(LC_ALL=C ps -A -o pid -o ppid -o stime 2>/dev/null) ||
				return "$?"
		fi
	fi
	printf '%s\n' "$l_descendant_identity_snapshot" |
		"${g_cmd_awk:-awk}" \
			-v root="$l_descendant_identity_root" \
			-v selector="$l_descendant_identity_selector" '
		$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ {
			pid = $1
			parent[pid] = $2
			seen[pid] = 1
			token = ""
			for (field = 3; field <= NF; field++)
				token = token (token == "" ? "" : " ") $field
			start_token[pid] = selector ":" token
		}
		END {
			if (!(root in seen)) exit 1
			target[root] = 1
			for (changed = 1; changed;) {
				changed = 0
				for (pid in seen)
					if ((parent[pid] in target) && !(pid in target))
						{ target[pid] = 1; changed = 1 }
			}
			for (pid in target)
				if (pid != root) {
					if (start_token[pid] == selector ":") exit 1
					print pid "\t" start_token[pid]
				}
		}'
}

# Purpose: Signal captured descendants only while each stored start token still
# matches immediately before delivery.
# Usage: Wrapper-mode KILL teardown passes the one-snapshot identity records.
zxfer_signal_background_job_descendant_records() {
	l_descendant_signal_records=$1
	l_descendant_signal_status=0

	while IFS='	' read -r l_descendant_signal_pid l_descendant_signal_start_token || [ -n "${l_descendant_signal_pid}${l_descendant_signal_start_token}" ]; do
		[ -n "$l_descendant_signal_pid" ] || continue
		l_descendant_signal_selector=${l_descendant_signal_start_token%%:*}
		if l_descendant_signal_current_token=$(zxfer_get_process_start_token \
			"$l_descendant_signal_pid" \
			"$l_descendant_signal_selector" 2>/dev/null); then
			if [ "$l_descendant_signal_current_token" != \
				"$l_descendant_signal_start_token" ]; then
				l_descendant_signal_status=2
				continue
			fi
			if ! kill -s KILL "$l_descendant_signal_pid" 2>/dev/null; then
				# A matching descendant can exit between token validation and
				# delivery. Treat ESRCH as success, but fail closed if the PID
				# is still live and could not be signalled.
				kill -s 0 "$l_descendant_signal_pid" 2>/dev/null &&
					l_descendant_signal_status=1
			fi
		elif kill -s 0 "$l_descendant_signal_pid" 2>/dev/null; then
			l_descendant_signal_status=2
		fi
	done <<-EOF
		$l_descendant_signal_records
	EOF
	if [ "$l_descendant_signal_status" -ne 0 ]; then
		zxfer_set_background_job_abort_failure_message \
			"Refusing to signal one or more supervised background job descendants because their process identities changed or were unavailable."
	fi
	return "$l_descendant_signal_status"
}

# Purpose: Signal one background job's teardown scope during abort.
# Usage: Root PID/PGID values come only from zxfer's registered `$!` before
# wait. Wrapper descendant token snapshots happen only for KILL teardown.
zxfer_signal_background_job_scope() {
	l_scope_pid=$1
	l_scope_teardown=$2
	l_scope_signal=$3
	l_scope_status=0

	case "$l_scope_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	if [ "$l_scope_teardown" = process_group ]; then
		kill "-$l_scope_signal" "-$l_scope_pid" 2>/dev/null || :
		return 0
	fi
	if [ "$l_scope_signal" != KILL ]; then
		kill -s "$l_scope_signal" "$l_scope_pid" 2>/dev/null || :
		return 0
	fi

	kill -s STOP "$l_scope_pid" 2>/dev/null || :
	if l_scope_descendant_records=$(zxfer_capture_background_job_descendant_identity_records \
		"$l_scope_pid"); then
		zxfer_signal_background_job_descendant_records \
			"$l_scope_descendant_records" || l_scope_status=$?
	else
		l_scope_status=$?
		# TERM may have completed during the grace window. An absent owned
		# root has no remaining teardown scope, so a failed process-table
		# snapshot must not turn that normal completion into cleanup failure.
		if ! kill -s 0 "$l_scope_pid" 2>/dev/null; then
			return 0
		fi
		zxfer_set_background_job_abort_failure_message \
			"Failed to discover all descendants of supervised background job process [$l_scope_pid] during abort."
	fi
	kill -s KILL "$l_scope_pid" 2>/dev/null || :
	return "$l_scope_status"
}
# Purpose: Give a signaled background job a brief bounded window to exit
# before the single KILL escalation.
# Usage: Called between the TERM pass and the KILL pass during aborts; suites
# set $g_zxfer_background_job_abort_grace_seconds to 0 to skip the wait.
zxfer_background_job_abort_grace_wait() {
	case "${g_zxfer_background_job_abort_grace_seconds:-1}" in
	0)
		:
		;;
	'' | *[!0-9]*)
		sleep 1
		;;
	*)
		sleep "$g_zxfer_background_job_abort_grace_seconds"
		;;
	esac
	return 0
}

# Purpose: Parse one record read from the background job completion queue.
# Usage: Called during send/receive rolling waits; publishes the record type,
# job id, and optional status in the g_zxfer_background_job_queue_record_*
# globals.
zxfer_parse_background_job_queue_record() {
	l_record=$1

	g_zxfer_background_job_queue_record_type=""
	g_zxfer_background_job_queue_record_job_id=""
	g_zxfer_background_job_queue_record_status=""

	case $l_record in
	completion_write_failed'	'*)
		IFS='	' read -r g_zxfer_background_job_queue_record_type g_zxfer_background_job_queue_record_job_id g_zxfer_background_job_queue_record_status <<-EOF
			$l_record
		EOF
		;;
	*)
		g_zxfer_background_job_queue_record_type=completion
		g_zxfer_background_job_queue_record_job_id=$l_record
		;;
	esac
}

# Purpose: Abort one tracked background job: TERM its teardown scope, wait
# briefly, escalate once with KILL, then reap and clean up.
# Usage: Called during send/receive failure handling and shutdown. Unknown
# jobs return success. All signalling happens before wait() per the module
# safety invariant.
zxfer_abort_background_job() {
	l_job_id=$1
	l_signal=${2:-TERM}

	g_zxfer_background_job_abort_failure_message=""
	zxfer_find_background_job_record "$l_job_id" || return 0
	l_abort_pid=$g_zxfer_background_job_record_pid
	l_abort_teardown=$g_zxfer_background_job_record_teardown
	l_abort_status_file=$g_zxfer_background_job_record_status_file
	l_abort_status=0

	zxfer_signal_background_job_scope \
		"$l_abort_pid" "$l_abort_teardown" "$l_signal" ||
		l_abort_status=$?
	zxfer_background_job_abort_grace_wait
	zxfer_signal_background_job_scope \
		"$l_abort_pid" "$l_abort_teardown" KILL ||
		l_abort_status=$?
	wait "$l_abort_pid" 2>/dev/null || :

	zxfer_unregister_background_job_record "$l_job_id"
	zxfer_cleanup_runtime_artifact_path "$l_abort_status_file" >/dev/null 2>&1 || :
	return "$l_abort_status"
}

# Purpose: Abort every tracked background job with one shared grace window:
# TERM all scopes, wait once, KILL all scopes, then reap and clean up.
# Usage: Called from the trap-exit path before the short-lived cleanup-PID
# registry teardown so long-lived pipelines stop first.
zxfer_abort_all_background_jobs() {
	g_zxfer_background_job_abort_failure_message=""
	if [ -z "${g_zxfer_background_job_records:-}" ]; then
		return 0
	fi
	l_abort_all_records=$g_zxfer_background_job_records
	l_abort_all_status=0

	while IFS='	' read -r l_job_id l_kind l_pid l_teardown l_status_file || [ -n "${l_job_id}${l_kind}${l_pid}${l_teardown}${l_status_file}" ]; do
		[ -n "$l_job_id" ] || continue
		zxfer_signal_background_job_scope \
			"$l_pid" "$l_teardown" TERM ||
			l_abort_all_status=$?
	done <<-EOF
		$l_abort_all_records
	EOF

	zxfer_background_job_abort_grace_wait

	while IFS='	' read -r l_job_id l_kind l_pid l_teardown l_status_file || [ -n "${l_job_id}${l_kind}${l_pid}${l_teardown}${l_status_file}" ]; do
		[ -n "$l_job_id" ] || continue
		zxfer_signal_background_job_scope \
			"$l_pid" "$l_teardown" KILL ||
			l_abort_all_status=$?
	done <<-EOF
		$l_abort_all_records
	EOF

	while IFS='	' read -r l_job_id l_kind l_pid l_teardown l_status_file || [ -n "${l_job_id}${l_kind}${l_pid}${l_teardown}${l_status_file}" ]; do
		[ -n "$l_job_id" ] || continue
		wait "$l_pid" 2>/dev/null || :
		zxfer_unregister_background_job_record "$l_job_id"
		zxfer_cleanup_runtime_artifact_path "$l_status_file" >/dev/null 2>&1 || :
	done <<-EOF
		$l_abort_all_records
	EOF

	return "$l_abort_all_status"
}
