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
# BACKGROUND JOB SUPERVISION
################################################################################

# Module contract:
# owns globals: background-job registry state, launch/completion parse scratch,
# and trap-abort result scratch for supervised long-lived workers.
# reads globals: g_cmd_ps, g_zxfer_temp_prefix, process-start identity helpers,
# and runtime artifact helpers.
# mutates caches: runtime artifact tracking through shared temp helpers.
# returns via stdout: supervisor job ids, runner script paths, and validated
# process-id sets for teardown.

ZXFER_BACKGROUND_JOB_METADATA_VERSION=1

zxfer_reset_background_job_state() {
	g_zxfer_background_job_records=""
	g_zxfer_background_job_sequence=0
	g_zxfer_background_job_last_id=""
	g_zxfer_background_job_last_runner_pid=""
	g_zxfer_background_job_last_control_dir=""
	g_zxfer_background_job_record_kind=""
	g_zxfer_background_job_record_runner_pid=""
	g_zxfer_background_job_record_control_dir=""
	g_zxfer_background_job_record_runner_script=""
	g_zxfer_background_job_record_runner_token=""
	g_zxfer_background_job_record_runner_start_token=""
	g_zxfer_background_job_launch_job_id=""
	g_zxfer_background_job_launch_kind=""
	g_zxfer_background_job_launch_runner_pid=""
	g_zxfer_background_job_launch_runner_script=""
	g_zxfer_background_job_launch_runner_token=""
	g_zxfer_background_job_launch_worker_pid=""
	g_zxfer_background_job_launch_worker_pgid=""
	g_zxfer_background_job_launch_teardown_mode=""
	g_zxfer_background_job_launch_started_epoch=""
	g_zxfer_background_job_completion_exit_status=""
	g_zxfer_background_job_completion_report_failure=""
	g_zxfer_background_job_wait_exit_status=""
	g_zxfer_background_job_wait_report_failure=""
	g_zxfer_background_job_wait_job_id=""
	g_zxfer_background_job_wait_runner_pid=""
	g_zxfer_background_job_wait_control_dir=""
	g_zxfer_background_job_abort_failure_message=""
	g_zxfer_background_job_process_snapshot_result=""
	g_zxfer_background_job_pid_set_result=""
	g_zxfer_background_job_queue_record_type=""
	g_zxfer_background_job_queue_record_job_id=""
	g_zxfer_background_job_queue_record_status=""
}

zxfer_get_background_job_runner_script_path() {
	l_runner_script="${ZXFER_SOURCE_MODULES_ROOT:-.}/src/zxfer_background_job_runner.sh"
	[ -r "$l_runner_script" ] || return 1
	printf '%s\n' "$l_runner_script"
}

zxfer_next_background_job_id() {
	g_zxfer_background_job_sequence=$((g_zxfer_background_job_sequence + 1))
	g_zxfer_background_job_last_id="bgjob.$$.$g_zxfer_background_job_sequence"
	printf '%s\n' "$g_zxfer_background_job_last_id"
}

zxfer_register_background_job_record() {
	l_job_id=$1
	l_kind=$2
	l_runner_pid=$3
	l_control_dir=$4
	l_runner_script=$5
	l_runner_token=$6
	l_runner_start_token=$7

	[ -n "$l_job_id" ] || return 1
	[ -n "$l_runner_pid" ] || return 1
	[ -n "$l_control_dir" ] || return 1
	[ -n "$l_runner_start_token" ] || return 1

	while IFS='	' read -r l_existing_job_id l_existing_kind l_existing_runner_pid l_existing_control_dir l_existing_runner_script l_existing_runner_token l_existing_runner_start_token || [ -n "${l_existing_job_id}${l_existing_kind}${l_existing_runner_pid}${l_existing_control_dir}${l_existing_runner_script}${l_existing_runner_token}${l_existing_runner_start_token}" ]; do
		[ -n "$l_existing_job_id" ] || continue
		[ "$l_existing_job_id" = "$l_job_id" ] && return 0
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	if [ -n "${g_zxfer_background_job_records:-}" ]; then
		g_zxfer_background_job_records=$g_zxfer_background_job_records"
	$l_job_id	$l_kind	$l_runner_pid	$l_control_dir	$l_runner_script	$l_runner_token	$l_runner_start_token"
	else
		g_zxfer_background_job_records="$l_job_id	$l_kind	$l_runner_pid	$l_control_dir	$l_runner_script	$l_runner_token	$l_runner_start_token"
	fi

	return 0
}

zxfer_find_background_job_record() {
	l_job_id=$1

	g_zxfer_background_job_record_kind=""
	g_zxfer_background_job_record_runner_pid=""
	g_zxfer_background_job_record_control_dir=""
	g_zxfer_background_job_record_runner_script=""
	g_zxfer_background_job_record_runner_token=""
	g_zxfer_background_job_record_runner_start_token=""

	while IFS='	' read -r l_existing_job_id l_existing_kind l_existing_runner_pid l_existing_control_dir l_existing_runner_script l_existing_runner_token l_existing_runner_start_token || [ -n "${l_existing_job_id}${l_existing_kind}${l_existing_runner_pid}${l_existing_control_dir}${l_existing_runner_script}${l_existing_runner_token}${l_existing_runner_start_token}" ]; do
		[ -n "$l_existing_job_id" ] || continue
		[ "$l_existing_job_id" = "$l_job_id" ] || continue
		g_zxfer_background_job_record_kind=$l_existing_kind
		g_zxfer_background_job_record_runner_pid=$l_existing_runner_pid
		g_zxfer_background_job_record_control_dir=$l_existing_control_dir
		g_zxfer_background_job_record_runner_script=$l_existing_runner_script
		g_zxfer_background_job_record_runner_token=$l_existing_runner_token
		g_zxfer_background_job_record_runner_start_token=$l_existing_runner_start_token
		return 0
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	return 1
}

zxfer_unregister_background_job_record() {
	l_job_id=$1
	l_remaining_records=""

	while IFS='	' read -r l_existing_job_id l_existing_kind l_existing_runner_pid l_existing_control_dir l_existing_runner_script l_existing_runner_token l_existing_runner_start_token || [ -n "${l_existing_job_id}${l_existing_kind}${l_existing_runner_pid}${l_existing_control_dir}${l_existing_runner_script}${l_existing_runner_token}${l_existing_runner_start_token}" ]; do
		[ -n "$l_existing_job_id" ] || continue
		[ "$l_existing_job_id" = "$l_job_id" ] && continue
		if [ -n "$l_remaining_records" ]; then
			l_remaining_records=$l_remaining_records"
	$l_existing_job_id	$l_existing_kind	$l_existing_runner_pid	$l_existing_control_dir	$l_existing_runner_script	$l_existing_runner_token	$l_existing_runner_start_token"
		else
			l_remaining_records="$l_existing_job_id	$l_existing_kind	$l_existing_runner_pid	$l_existing_control_dir	$l_existing_runner_script	$l_existing_runner_token	$l_existing_runner_start_token"
		fi
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	g_zxfer_background_job_records=$l_remaining_records
}

zxfer_write_background_job_launch_file() {
	l_control_dir=$1
	l_job_id=$2
	l_kind=$3
	l_runner_pid=$4
	l_runner_script=$5
	l_runner_token=$6
	l_worker_pid=$7
	l_worker_pgid=$8
	l_teardown_mode=$9
	l_started_epoch=${10:-}
	l_launch_path=$l_control_dir/launch.tsv

	l_payload="version	$ZXFER_BACKGROUND_JOB_METADATA_VERSION
job_id	$l_job_id
kind	$l_kind
runner_pid	$l_runner_pid
runner_script	$l_runner_script
runner_token	$l_runner_token
worker_pid	$l_worker_pid
worker_pgid	$l_worker_pgid
teardown_mode	$l_teardown_mode
started_epoch	$l_started_epoch"

	zxfer_write_runtime_cache_file_atomically "$l_launch_path" "$l_payload" "zxfer-bgjob-launch"
}

zxfer_read_background_job_launch_file() {
	l_control_dir=$1
	l_launch_path=$l_control_dir/launch.tsv
	l_read_status=0

	g_zxfer_background_job_launch_job_id=""
	g_zxfer_background_job_launch_kind=""
	g_zxfer_background_job_launch_runner_pid=""
	g_zxfer_background_job_launch_runner_script=""
	g_zxfer_background_job_launch_runner_token=""
	g_zxfer_background_job_launch_worker_pid=""
	g_zxfer_background_job_launch_worker_pgid=""
	g_zxfer_background_job_launch_teardown_mode=""
	g_zxfer_background_job_launch_started_epoch=""

	l_read_status=0
	zxfer_read_runtime_artifact_file "$l_launch_path" >/dev/null ||
		l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	while IFS='	' read -r l_key l_value || [ -n "${l_key}${l_value}" ]; do
		case $l_key in
		job_id)
			g_zxfer_background_job_launch_job_id=$l_value
			;;
		kind)
			g_zxfer_background_job_launch_kind=$l_value
			;;
		runner_pid)
			g_zxfer_background_job_launch_runner_pid=$l_value
			;;
		runner_script)
			g_zxfer_background_job_launch_runner_script=$l_value
			;;
		runner_token)
			g_zxfer_background_job_launch_runner_token=$l_value
			;;
		worker_pid)
			g_zxfer_background_job_launch_worker_pid=$l_value
			;;
		worker_pgid)
			g_zxfer_background_job_launch_worker_pgid=$l_value
			;;
		teardown_mode)
			g_zxfer_background_job_launch_teardown_mode=$l_value
			;;
		started_epoch)
			g_zxfer_background_job_launch_started_epoch=$l_value
			;;
		esac
	done <<-EOF || l_read_status=$?
		$g_zxfer_runtime_artifact_read_result
	EOF

	[ "$l_read_status" -eq 0 ]
}

zxfer_read_background_job_completion_file() {
	l_control_dir=$1
	l_completion_path=$l_control_dir/completion.tsv
	l_read_status=0
	l_status_seen=0
	l_report_failure_seen=0

	g_zxfer_background_job_completion_exit_status=""
	g_zxfer_background_job_completion_report_failure=""

	l_read_status=0
	zxfer_read_runtime_artifact_file "$l_completion_path" >/dev/null ||
		l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	while IFS='	' read -r l_key l_value || [ -n "${l_key}${l_value}" ]; do
		case $l_key in
		status)
			[ "$l_status_seen" -eq 0 ] || return 1
			case "$l_value" in
			'' | *[!0-9]*)
				return 1
				;;
			esac
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
		esac
	done <<-EOF || l_read_status=$?
		$g_zxfer_runtime_artifact_read_result
	EOF

	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	[ "$l_status_seen" -eq 1 ]
}

zxfer_get_background_job_completion_status() {
	l_control_dir=$1
	l_wait_status=$2
	l_completion_path=$l_control_dir/completion.tsv

	g_zxfer_background_job_completion_exit_status=$l_wait_status
	g_zxfer_background_job_completion_report_failure=""

	if [ ! -f "$l_completion_path" ]; then
		g_zxfer_background_job_completion_report_failure=completion_write
		return 0
	fi
	l_completion_read_status=0
	zxfer_read_background_job_completion_file "$l_control_dir" || l_completion_read_status=$?
	if [ "$l_completion_read_status" -ne 0 ]; then
		return "$l_completion_read_status"
	fi

	return 0
}

zxfer_spawn_supervised_background_job() {
	l_kind=$1
	l_exec_cmd=$2
	l_display_cmd=$3
	l_output_file=${4:-}
	l_error_file=${5:-}
	l_notify_fd=${6:-}

	g_zxfer_background_job_last_id=""
	g_zxfer_background_job_last_runner_pid=""
	g_zxfer_background_job_last_control_dir=""

	l_spawn_status=0
	l_runner_script=$(zxfer_get_background_job_runner_script_path) ||
		l_spawn_status=$?
	if [ "$l_spawn_status" -ne 0 ]; then
		zxfer_throw_error "Failed to locate the background job runner helper." "$l_spawn_status"
	fi
	l_spawn_status=0
	l_job_id=$(zxfer_next_background_job_id) ||
		l_spawn_status=$?
	if [ "$l_spawn_status" -ne 0 ]; then
		zxfer_throw_error "Failed to allocate a background job id." "$l_spawn_status"
	fi
	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.$l_job_id"
	l_spawn_status=0
	l_control_dir=$(zxfer_create_private_temp_dir "$l_temp_prefix") ||
		l_spawn_status=$?
	if [ "$l_spawn_status" -ne 0 ]; then
		zxfer_throw_error "Error creating temporary file." "$l_spawn_status"
	fi
	l_runner_token="$l_job_id.$(date +%s)"
	ZXFER_BACKGROUND_JOB_NOTIFY_FD=$l_notify_fd \
		/bin/sh "$l_runner_script" \
		"$l_runner_token" \
		"$l_job_id" \
		"$l_kind" \
		"$l_control_dir" \
		"$l_exec_cmd" \
		"$l_display_cmd" \
		"$l_output_file" \
		"$l_error_file" &
	l_runner_pid=$!
	l_spawn_status=0
	l_runner_start_token=$(zxfer_get_process_start_token "$l_runner_pid") ||
		l_spawn_status=$?
	if [ "$l_spawn_status" -ne 0 ]; then
		zxfer_teardown_unregistered_background_runner \
			"$l_job_id" \
			"$l_runner_pid" \
			"$l_control_dir" \
			"$l_runner_script" \
			"$l_runner_token" \
			"" \
			"TERM" >/dev/null 2>&1 || :
		zxfer_throw_error "Failed to validate background job [$l_job_id] runner identity." "$l_spawn_status"
	fi

	l_spawn_status=0
	zxfer_register_background_job_record \
		"$l_job_id" \
		"$l_kind" \
		"$l_runner_pid" \
		"$l_control_dir" \
		"$l_runner_script" \
		"$l_runner_token" \
		"$l_runner_start_token" ||
		l_spawn_status=$?
	if [ "$l_spawn_status" -ne 0 ]; then
		zxfer_teardown_unregistered_background_runner \
			"$l_job_id" \
			"$l_runner_pid" \
			"$l_control_dir" \
			"$l_runner_script" \
			"$l_runner_token" \
			"$l_runner_start_token" \
			"TERM" >/dev/null 2>&1 || :
		zxfer_throw_error "Failed to register background job [$l_job_id]." "$l_spawn_status"
	fi

	g_zxfer_background_job_last_id=$l_job_id
	g_zxfer_background_job_last_runner_pid=$l_runner_pid
	g_zxfer_background_job_last_control_dir=$l_control_dir
	return 0
}

zxfer_wait_for_background_job() {
	l_job_id=$1
	l_wait_status=0

	g_zxfer_background_job_wait_exit_status=""
	g_zxfer_background_job_wait_report_failure=""
	g_zxfer_background_job_wait_job_id=""
	g_zxfer_background_job_wait_runner_pid=""
	g_zxfer_background_job_wait_control_dir=""

	if ! zxfer_find_background_job_record "$l_job_id"; then
		return 1
	fi

	wait "$g_zxfer_background_job_record_runner_pid" 2>/dev/null || l_wait_status=$?
	l_completion_status=0
	zxfer_get_background_job_completion_status "$g_zxfer_background_job_record_control_dir" "$l_wait_status" ||
		l_completion_status=$?
	if [ "$l_completion_status" -ne 0 ]; then
		zxfer_unregister_background_job_record "$l_job_id"
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_background_job_record_control_dir" >/dev/null 2>&1 || :
		return "$l_completion_status"
	fi

	g_zxfer_background_job_wait_exit_status=$g_zxfer_background_job_completion_exit_status
	g_zxfer_background_job_wait_report_failure=$g_zxfer_background_job_completion_report_failure
	g_zxfer_background_job_wait_job_id=$l_job_id
	g_zxfer_background_job_wait_runner_pid=$g_zxfer_background_job_record_runner_pid
	g_zxfer_background_job_wait_control_dir=$g_zxfer_background_job_record_control_dir

	zxfer_unregister_background_job_record "$l_job_id"
	zxfer_cleanup_runtime_artifact_path "$g_zxfer_background_job_wait_control_dir" >/dev/null 2>&1 || :
	return 0
}

zxfer_read_background_job_process_snapshot() {
	g_zxfer_background_job_process_snapshot_result=""
	l_snapshot_status=0
	l_snapshot=$("$g_cmd_ps" -o pid= -o ppid= -o pgid= 2>/dev/null) || l_snapshot_status=$?
	[ "$l_snapshot_status" -eq 0 ] || return "$l_snapshot_status"
	g_zxfer_background_job_process_snapshot_result=$l_snapshot
	printf '%s\n' "$l_snapshot"
}

zxfer_background_job_snapshot_has_pid() {
	l_snapshot=$1
	l_pid=$2

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | "${g_cmd_awk:-awk}" -v want_pid="$l_pid" '
	$1 == want_pid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_background_job_runner_matches() {
	l_snapshot=$1
	l_runner_pid=$2
	l_runner_start_token=$3

	case "$l_runner_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	if ! zxfer_background_job_snapshot_has_pid "$l_snapshot" "$l_runner_pid"; then
		return 1
	fi
	if ! l_current_start_token=$(zxfer_get_process_start_token "$l_runner_pid" 2>/dev/null); then
		return 2
	fi
	[ "$l_current_start_token" = "$l_runner_start_token" ] || return 3
	return 0
}

zxfer_background_job_snapshot_has_pid_with_pgid() {
	l_snapshot=$1
	l_pid=$2
	l_pgid=$3

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_pgid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | "${g_cmd_awk:-awk}" -v want_pid="$l_pid" -v want_pgid="$l_pgid" '
	$1 == want_pid && $3 == want_pgid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_background_job_snapshot_has_pid_with_parent() {
	l_snapshot=$1
	l_pid=$2
	l_parent_pid=$3

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_parent_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | "${g_cmd_awk:-awk}" -v want_pid="$l_pid" -v want_parent_pid="$l_parent_pid" '
	$1 == want_pid && $2 == want_parent_pid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_get_background_job_pid_set() {
	l_snapshot=$1
	l_root_pid=$2

	g_zxfer_background_job_pid_set_result=""
	l_pid_set_status=0
	# shellcheck disable=SC2016
	l_pid_set_raw=$(printf '%s\n' "$l_snapshot" | "${g_cmd_awk:-awk}" -v root="$l_root_pid" '
	{
		pid = $1
		ppid = $2
		if (pid != "") {
			parent[pid] = ppid
			seen[pid] = 1
		}
	}
	END {
		if (root == "") {
			exit 1
		}
		target[root] = 1
		changed = 1
		while (changed) {
			changed = 0
			for (pid in seen) {
				if ((parent[pid] in target) && !(pid in target)) {
					target[pid] = 1
					changed = 1
				}
			}
		}
		for (pid in target) {
			print pid
		}
	}') || l_pid_set_status=$?
	[ "$l_pid_set_status" -eq 0 ] || return "$l_pid_set_status"
	l_pid_set_status=0
	l_pid_set=$(printf '%s\n' "$l_pid_set_raw" | LC_ALL=C sort -n) ||
		l_pid_set_status=$?
	[ "$l_pid_set_status" -eq 0 ] || return "$l_pid_set_status"
	g_zxfer_background_job_pid_set_result=$l_pid_set
	printf '%s\n' "$l_pid_set"
}

zxfer_signal_background_job_pid_set() {
	l_pid_set=$1
	l_signal=$2
	l_status=0

	while IFS= read -r l_pid || [ -n "$l_pid" ]; do
		[ -n "$l_pid" ] || continue
		kill "-$l_signal" "$l_pid" 2>/dev/null || l_status=1
	done <<-EOF
		$l_pid_set
	EOF

	return "$l_status"
}

zxfer_signal_background_job_process_group() {
	l_pgid=$1
	l_signal=$2

	case "$l_pgid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	kill "-$l_signal" "-$l_pgid" 2>/dev/null
}

zxfer_signal_validated_background_job_scope() {
	l_signal_job_id=$1
	l_signal_runner_pid=$2
	l_signal_have_launch_metadata=$3
	l_signal_name=$4
	l_signal_status=0
	l_signal_target_pid_set=""

	l_signal_current_shell_pgid_status=0
	l_signal_current_shell_pgid_raw=$("$g_cmd_ps" -o pgid= -p "$$" 2>/dev/null) ||
		l_signal_current_shell_pgid_status=$?
	if [ "$l_signal_current_shell_pgid_status" -eq 0 ]; then
		l_signal_current_shell_pgid=$(printf '%s\n' "$l_signal_current_shell_pgid_raw" |
			sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed -n '1p') ||
			l_signal_current_shell_pgid_status=$?
	else
		l_signal_current_shell_pgid=""
	fi
	[ "$l_signal_current_shell_pgid_status" -eq 0 ] || l_signal_current_shell_pgid=""
	case ${g_zxfer_background_job_launch_worker_pgid:-} in
	'' | *[!0-9]*)
		l_signal_use_process_group=0
		;;
	*)
		l_signal_use_process_group=0
		if [ "$l_signal_have_launch_metadata" -eq 1 ] &&
			[ "${g_zxfer_background_job_launch_teardown_mode:-}" = "process_group" ] &&
			[ "$g_zxfer_background_job_launch_worker_pgid" != "${l_signal_current_shell_pgid:-}" ] &&
			zxfer_background_job_snapshot_has_pid_with_parent \
				"$g_zxfer_background_job_process_snapshot_result" \
				"$g_zxfer_background_job_launch_worker_pid" \
				"$l_signal_runner_pid" &&
			zxfer_background_job_snapshot_has_pid_with_pgid \
				"$g_zxfer_background_job_process_snapshot_result" \
				"$g_zxfer_background_job_launch_worker_pid" \
				"$g_zxfer_background_job_launch_worker_pgid"; then
			l_signal_use_process_group=1
		fi
		;;
	esac

	if [ "$l_signal_use_process_group" -eq 1 ]; then
		zxfer_signal_background_job_process_group "$g_zxfer_background_job_launch_worker_pgid" "$l_signal_name" || l_signal_status=1
		kill "-$l_signal_name" "$l_signal_runner_pid" 2>/dev/null || l_signal_status=1
	else
		l_signal_pid_set_status=0
		zxfer_get_background_job_pid_set \
			"$g_zxfer_background_job_process_snapshot_result" \
			"$l_signal_runner_pid" >/dev/null ||
			l_signal_pid_set_status=$?
		if [ "$l_signal_pid_set_status" -ne 0 ]; then
			g_zxfer_background_job_abort_failure_message="Failed to derive the owned child set for background job [$l_signal_job_id] cleanup."
			return "$l_signal_pid_set_status"
		fi
		l_signal_target_pid_set=$g_zxfer_background_job_pid_set_result
		[ -n "$l_signal_target_pid_set" ] || l_signal_target_pid_set=$l_signal_runner_pid
		zxfer_signal_background_job_pid_set "$l_signal_target_pid_set" "$l_signal_name" || l_signal_status=1
	fi

	return "$l_signal_status"
}

zxfer_cleanup_completed_background_job() {
	l_job_id=$1
	l_runner_pid=$2
	l_control_dir=$3

	case "$l_runner_pid" in
	'' | *[!0-9]*)
		:
		;;
	*)
		wait "$l_runner_pid" 2>/dev/null || :
		;;
	esac

	zxfer_unregister_background_job_record "$l_job_id"
	zxfer_cleanup_runtime_artifact_path "$l_control_dir" >/dev/null 2>&1 || :
	return 0
}

zxfer_cleanup_aborted_background_job() {
	l_job_id=$1
	l_control_dir=$2

	zxfer_unregister_background_job_record "$l_job_id"
	zxfer_cleanup_runtime_artifact_path "$l_control_dir" >/dev/null 2>&1 || :
	return 0
}

zxfer_finish_signaled_background_job_abort() {
	l_finish_job_id=$1
	l_finish_runner_pid=$2
	l_finish_control_dir=$3
	l_finish_runner_start_token=$4
	l_finish_have_launch_metadata=$5
	l_finish_signal_status=${6:-0}
	l_finish_completion_path=$l_finish_control_dir/completion.tsv
	l_finish_runner_match_status=0
	l_finish_escalation_status=0

	if [ -f "$l_finish_completion_path" ]; then
		zxfer_cleanup_completed_background_job \
			"$l_finish_job_id" \
			"$l_finish_runner_pid" \
			"$l_finish_control_dir"
		return 0
	fi
	l_finish_snapshot_status=0
	zxfer_read_background_job_process_snapshot >/dev/null || l_finish_snapshot_status=$?
	if [ "$l_finish_snapshot_status" -ne 0 ]; then
		if [ -f "$l_finish_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_finish_job_id" \
				"$l_finish_runner_pid" \
				"$l_finish_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Failed to inspect the process table for background job [$l_finish_job_id] cleanup."
		return "$l_finish_snapshot_status"
	fi

	zxfer_background_job_runner_matches \
		"$g_zxfer_background_job_process_snapshot_result" \
		"$l_finish_runner_pid" \
		"$l_finish_runner_start_token" ||
		l_finish_runner_match_status=$?
	case "$l_finish_runner_match_status" in
	1)
		zxfer_cleanup_aborted_background_job "$l_finish_job_id" "$l_finish_control_dir"
		return 0
		;;
	2)
		if [ -f "$l_finish_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_finish_job_id" \
				"$l_finish_runner_pid" \
				"$l_finish_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Failed to validate the live runner identity for background job [$l_finish_job_id]."
		return 1
		;;
	3)
		if [ -f "$l_finish_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_finish_job_id" \
				"$l_finish_runner_pid" \
				"$l_finish_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Refusing to tear down background job [$l_finish_job_id] because the tracked runner PID [$l_finish_runner_pid] no longer matches the recorded helper identity."
		return 1
		;;
	esac

	zxfer_signal_validated_background_job_scope \
		"$l_finish_job_id" \
		"$l_finish_runner_pid" \
		"$l_finish_have_launch_metadata" \
		"KILL" ||
		l_finish_escalation_status=$?

	if [ -f "$l_finish_completion_path" ]; then
		zxfer_cleanup_completed_background_job \
			"$l_finish_job_id" \
			"$l_finish_runner_pid" \
			"$l_finish_control_dir"
		return 0
	fi
	l_finish_snapshot_status=0
	zxfer_read_background_job_process_snapshot >/dev/null || l_finish_snapshot_status=$?
	if [ "$l_finish_snapshot_status" -ne 0 ]; then
		if [ -f "$l_finish_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_finish_job_id" \
				"$l_finish_runner_pid" \
				"$l_finish_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Failed to inspect the process table for background job [$l_finish_job_id] cleanup."
		return "$l_finish_snapshot_status"
	fi

	l_finish_runner_match_status=0
	zxfer_background_job_runner_matches \
		"$g_zxfer_background_job_process_snapshot_result" \
		"$l_finish_runner_pid" \
		"$l_finish_runner_start_token" ||
		l_finish_runner_match_status=$?
	case "$l_finish_runner_match_status" in
	1)
		zxfer_cleanup_aborted_background_job "$l_finish_job_id" "$l_finish_control_dir"
		return 0
		;;
	2)
		if [ -f "$l_finish_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_finish_job_id" \
				"$l_finish_runner_pid" \
				"$l_finish_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Failed to validate the live runner identity for background job [$l_finish_job_id]."
		return 1
		;;
	3)
		if [ -f "$l_finish_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_finish_job_id" \
				"$l_finish_runner_pid" \
				"$l_finish_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Refusing to tear down background job [$l_finish_job_id] because the tracked runner PID [$l_finish_runner_pid] no longer matches the recorded helper identity."
		return 1
		;;
	esac

	if [ "$l_finish_escalation_status" -ne 0 ] && [ -n "${g_zxfer_background_job_abort_failure_message:-}" ]; then
		:
	elif [ "$l_finish_signal_status" -ne 0 ] || [ "$l_finish_escalation_status" -ne 0 ]; then
		g_zxfer_background_job_abort_failure_message="Failed to signal the validated teardown target for background job [$l_finish_job_id]."
	else
		g_zxfer_background_job_abort_failure_message="Refusing to remove background job [$l_finish_job_id] state because the validated runner PID [$l_finish_runner_pid] is still live after abort cleanup signaling."
	fi
	if [ "$l_finish_signal_status" -ne 0 ]; then
		return "$l_finish_signal_status"
	fi
	if [ "$l_finish_escalation_status" -ne 0 ]; then
		return "$l_finish_escalation_status"
	fi
	return 1
}

zxfer_teardown_unregistered_background_runner() {
	l_unregistered_job_id=$1
	l_unregistered_runner_pid=$2
	l_unregistered_control_dir=$3
	l_unregistered_runner_script=$4
	l_unregistered_runner_token=$5
	l_unregistered_runner_start_token=${6:-}
	l_unregistered_signal=${7:-TERM}
	l_unregistered_have_launch_metadata=0
	l_unregistered_signal_status=0
	l_unregistered_runner_match_status=0

	case "$l_unregistered_runner_pid" in
	'' | *[!0-9]*)
		zxfer_cleanup_runtime_artifact_path "$l_unregistered_control_dir" >/dev/null 2>&1 || :
		return 0
		;;
	esac

	if [ -f "$l_unregistered_control_dir/launch.tsv" ] &&
		zxfer_read_background_job_launch_file "$l_unregistered_control_dir" &&
		[ "${g_zxfer_background_job_launch_job_id:-}" = "$l_unregistered_job_id" ] &&
		[ "${g_zxfer_background_job_launch_runner_pid:-}" = "$l_unregistered_runner_pid" ] &&
		[ "${g_zxfer_background_job_launch_runner_script:-}" = "$l_unregistered_runner_script" ] &&
		[ "${g_zxfer_background_job_launch_runner_token:-}" = "$l_unregistered_runner_token" ]; then
		l_unregistered_have_launch_metadata=1
	fi

	l_unregistered_snapshot_status=0
	zxfer_read_background_job_process_snapshot >/dev/null || l_unregistered_snapshot_status=$?
	if [ "$l_unregistered_snapshot_status" -ne 0 ]; then
		g_zxfer_background_job_abort_failure_message="Failed to inspect the process table for background job [$l_unregistered_job_id] cleanup."
		return "$l_unregistered_snapshot_status"
	fi
	if ! zxfer_background_job_snapshot_has_pid_with_parent \
		"$g_zxfer_background_job_process_snapshot_result" \
		"$l_unregistered_runner_pid" \
		"$$"; then
		wait "$l_unregistered_runner_pid" 2>/dev/null || :
		zxfer_cleanup_runtime_artifact_path "$l_unregistered_control_dir" >/dev/null 2>&1 || :
		return 0
	fi
	if [ -n "$l_unregistered_runner_start_token" ]; then
		zxfer_background_job_runner_matches \
			"$g_zxfer_background_job_process_snapshot_result" \
			"$l_unregistered_runner_pid" \
			"$l_unregistered_runner_start_token" ||
			l_unregistered_runner_match_status=$?
		case "$l_unregistered_runner_match_status" in
		0)
			:
			;;
		*)
			g_zxfer_background_job_abort_failure_message="Refusing to tear down background job [$l_unregistered_job_id] because the tracked runner PID [$l_unregistered_runner_pid] no longer matches the recorded helper identity."
			return 1
			;;
		esac
	fi

	zxfer_signal_validated_background_job_scope \
		"$l_unregistered_job_id" \
		"$l_unregistered_runner_pid" \
		"$l_unregistered_have_launch_metadata" \
		"$l_unregistered_signal" ||
		l_unregistered_signal_status=$?
	if [ "$l_unregistered_signal_status" -ne 0 ] &&
		! zxfer_read_background_job_process_snapshot >/dev/null; then
		return 1
	fi
	if [ "$l_unregistered_signal_status" -ne 0 ] &&
		zxfer_background_job_snapshot_has_pid_with_parent \
			"$g_zxfer_background_job_process_snapshot_result" \
			"$l_unregistered_runner_pid" \
			"$$"; then
		return 1
	fi

	if zxfer_read_background_job_process_snapshot >/dev/null &&
		zxfer_background_job_snapshot_has_pid_with_parent \
			"$g_zxfer_background_job_process_snapshot_result" \
			"$l_unregistered_runner_pid" \
			"$$"; then
		zxfer_signal_validated_background_job_scope \
			"$l_unregistered_job_id" \
			"$l_unregistered_runner_pid" \
			"$l_unregistered_have_launch_metadata" \
			"KILL" >/dev/null 2>&1 || :
	fi

	wait "$l_unregistered_runner_pid" 2>/dev/null || :
	zxfer_cleanup_runtime_artifact_path "$l_unregistered_control_dir" >/dev/null 2>&1 || :
	return 0
}

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

zxfer_abort_background_job() {
	l_job_id=$1
	l_signal=${2:-TERM}
	l_launch_path=""
	l_completion_path=""
	l_have_launch_metadata=0
	l_signal_status=0

	g_zxfer_background_job_abort_failure_message=""
	if ! zxfer_find_background_job_record "$l_job_id"; then
		return 0
	fi

	l_launch_path=$g_zxfer_background_job_record_control_dir/launch.tsv
	l_completion_path=$g_zxfer_background_job_record_control_dir/completion.tsv
	if [ -f "$l_launch_path" ]; then
		l_launch_read_status=0
		zxfer_read_background_job_launch_file "$g_zxfer_background_job_record_control_dir" ||
			l_launch_read_status=$?
		if [ "$l_launch_read_status" -ne 0 ]; then
			if [ -f "$l_completion_path" ]; then
				zxfer_cleanup_completed_background_job \
					"$l_job_id" \
					"$g_zxfer_background_job_record_runner_pid" \
					"$g_zxfer_background_job_record_control_dir"
				return 0
			fi
			g_zxfer_background_job_abort_failure_message="Failed to read launch metadata for background job [$l_job_id]."
			return "$l_launch_read_status"
		fi
		l_have_launch_metadata=1
	else
		g_zxfer_background_job_launch_job_id=""
		g_zxfer_background_job_launch_kind=""
		g_zxfer_background_job_launch_runner_pid=""
		g_zxfer_background_job_launch_runner_script=""
		g_zxfer_background_job_launch_runner_token=""
		g_zxfer_background_job_launch_worker_pid=""
		g_zxfer_background_job_launch_worker_pgid=""
		g_zxfer_background_job_launch_teardown_mode=""
		g_zxfer_background_job_launch_started_epoch=""
	fi
	l_abort_snapshot_status=0
	zxfer_read_background_job_process_snapshot >/dev/null || l_abort_snapshot_status=$?
	if [ "$l_abort_snapshot_status" -ne 0 ]; then
		if [ -f "$l_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_job_id" \
				"$g_zxfer_background_job_record_runner_pid" \
				"$g_zxfer_background_job_record_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Failed to inspect the process table for background job [$l_job_id] cleanup."
		return "$l_abort_snapshot_status"
	fi
	if [ "$l_have_launch_metadata" -eq 1 ] &&
		{
			[ "${g_zxfer_background_job_launch_job_id:-}" != "$l_job_id" ] ||
				[ "${g_zxfer_background_job_launch_runner_pid:-}" != "$g_zxfer_background_job_record_runner_pid" ] ||
				[ "${g_zxfer_background_job_launch_runner_script:-}" != "$g_zxfer_background_job_record_runner_script" ] ||
				[ "${g_zxfer_background_job_launch_runner_token:-}" != "$g_zxfer_background_job_record_runner_token" ]
		}; then
		if [ -f "$l_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_job_id" \
				"$g_zxfer_background_job_record_runner_pid" \
				"$g_zxfer_background_job_record_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Refusing to tear down background job [$l_job_id] because the recorded launch metadata no longer matches the tracked runner identity."
		return 1
	fi
	l_runner_match_status=0
	zxfer_background_job_runner_matches \
		"$g_zxfer_background_job_process_snapshot_result" \
		"$g_zxfer_background_job_record_runner_pid" \
		"$g_zxfer_background_job_record_runner_start_token" ||
		l_runner_match_status=$?
	case "$l_runner_match_status" in
	0)
		:
		;;
	1 | 3)
		if [ -f "$l_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_job_id" \
				"$g_zxfer_background_job_record_runner_pid" \
				"$g_zxfer_background_job_record_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Refusing to tear down background job [$l_job_id] because the tracked runner PID [$g_zxfer_background_job_record_runner_pid] no longer matches the recorded helper identity."
		return 1
		;;
	*)
		if [ -f "$l_completion_path" ]; then
			zxfer_cleanup_completed_background_job \
				"$l_job_id" \
				"$g_zxfer_background_job_record_runner_pid" \
				"$g_zxfer_background_job_record_control_dir"
			return 0
		fi
		g_zxfer_background_job_abort_failure_message="Failed to validate the live runner identity for background job [$l_job_id]."
		return 1
		;;
	esac

	zxfer_signal_validated_background_job_scope \
		"$l_job_id" \
		"$g_zxfer_background_job_record_runner_pid" \
		"$l_have_launch_metadata" \
		"$l_signal" ||
		l_signal_status=$?

	zxfer_finish_signaled_background_job_abort \
		"$l_job_id" \
		"$g_zxfer_background_job_record_runner_pid" \
		"$g_zxfer_background_job_record_control_dir" \
		"$g_zxfer_background_job_record_runner_start_token" \
		"$l_have_launch_metadata" \
		"$l_signal_status"
}

zxfer_abort_all_background_jobs() {
	l_job_ids=""
	l_abort_failure=0
	l_first_failure_message=""

	while IFS='	' read -r l_job_id l_kind l_runner_pid l_control_dir l_runner_script l_runner_token l_runner_start_token || [ -n "${l_job_id}${l_kind}${l_runner_pid}${l_control_dir}${l_runner_script}${l_runner_token}${l_runner_start_token}" ]; do
		[ -n "$l_job_id" ] || continue
		if [ -n "$l_job_ids" ]; then
			l_job_ids=$l_job_ids"
$l_job_id"
		else
			l_job_ids=$l_job_id
		fi
	done <<-EOF
		${g_zxfer_background_job_records:-}
	EOF

	while IFS= read -r l_job_id || [ -n "$l_job_id" ]; do
		[ -n "$l_job_id" ] || continue
		l_abort_status=0
		zxfer_abort_background_job "$l_job_id" TERM || l_abort_status=$?
		if [ "$l_abort_status" -ne 0 ]; then
			if [ "$l_abort_failure" -eq 0 ]; then
				l_abort_failure=$l_abort_status
				l_first_failure_message=${g_zxfer_background_job_abort_failure_message:-}
			fi
		fi
	done <<-EOF
		$l_job_ids
	EOF

	if [ "$l_abort_failure" -ne 0 ]; then
		g_zxfer_background_job_abort_failure_message=$l_first_failure_message
		return "$l_abort_failure"
	fi

	return 0
}
