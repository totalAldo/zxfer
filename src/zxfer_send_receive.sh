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
# SEND / RECEIVE PIPELINE HELPERS
################################################################################

# Module contract:
# owns globals: send/receive job queue state and rolling completion queue handles.
# reads globals: g_option_j_jobs, g_option_D_display_progress_bar, remote host specs, and zfs/compression helpers.
# mutates caches: destination-existence/property caches and background job tracking after live receives.
# returns via stdout: rendered send/receive commands, progress dialogs, and size estimates.

zxfer_reset_send_receive_state() {
	g_count_zfs_send_jobs=0
	g_zfs_send_job_pids=""
	g_zfs_send_job_records=""
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_unavailable=0
}

#
# The snapshot size is estimated. The estimate does not take into consideration
# the compression ratio of the data. The estimate is based on the size of the
# dataset. When compression is used, the bar will terminate sooner,
# ending at the compression ratio.
# Uses the source-side zfs helper selected by $g_LZFS.
#
zxfer_progress_dialog_uses_size_estimate() {
	case ${g_option_D_display_progress_bar:-} in
	*%%size%%*) return 0 ;;
	esac

	return 1
}

zxfer_should_use_fast_progress_estimate() {
	l_job_limit=${g_option_j_jobs:-1}
	case $l_job_limit in
	'' | *[!0-9]*) l_job_limit=1 ;;
	esac

	if [ -n "${g_option_O_origin_host:-}" ] ||
		[ -n "${g_option_T_target_host:-}" ] ||
		[ "$l_job_limit" -gt 1 ]; then
		return 0
	fi

	return 1
}

zxfer_extract_numeric_progress_estimate() {
	l_estimate_value=$(printf '%s\n' "$1" | tail -n 1 | tr -d '\r')

	case $l_estimate_value in
	'' | *[!0-9]*) return 1 ;;
	esac

	printf '%s\n' "$l_estimate_value"
}

zxfer_calculate_fast_full_size_estimate() {
	l_current_snapshot=$1

	if ! l_size_dataset=$(zxfer_run_source_zfs_cmd list -Hp -o referenced "$l_current_snapshot" 2>&1); then
		return 1
	fi

	zxfer_extract_numeric_progress_estimate "$l_size_dataset"
}

zxfer_calculate_fast_incremental_size_estimate() {
	l_current_snapshot=$1
	l_previous_snapshot=$2
	l_current_dataset=${l_current_snapshot%@*}
	l_previous_snapshot_name=${l_previous_snapshot#*@}

	if [ -z "$l_current_dataset" ] ||
		[ "$l_current_dataset" = "$l_current_snapshot" ] ||
		[ -z "$l_previous_snapshot_name" ]; then
		return 1
	fi

	if ! l_size_dataset=$(zxfer_run_source_zfs_cmd get -Hpo value "written@$l_previous_snapshot_name" "$l_current_dataset" 2>&1); then
		return 1
	fi

	zxfer_extract_numeric_progress_estimate "$l_size_dataset"
}

zxfer_calculate_size_estimate() {
	l_current_snapshot=$1
	l_previous_snapshot=$2
	l_prefer_fast_estimate=${3:-0}

	if [ "$l_prefer_fast_estimate" -eq 1 ]; then
		if [ -n "$l_previous_snapshot" ]; then
			if l_size_est=$(zxfer_calculate_fast_incremental_size_estimate "$l_current_snapshot" "$l_previous_snapshot"); then
				zxfer_echoV "Using fast approximate incremental progress estimate for $l_current_snapshot."
				echo "$l_size_est"
				return 0
			fi
			zxfer_echoV "Falling back to exact incremental progress estimate for $l_current_snapshot."
		else
			if l_size_est=$(zxfer_calculate_fast_full_size_estimate "$l_current_snapshot"); then
				zxfer_echoV "Using fast approximate full progress estimate for $l_current_snapshot."
				echo "$l_size_est"
				return 0
			fi
			zxfer_echoV "Falling back to exact full progress estimate for $l_current_snapshot."
		fi
	fi

	if [ -n "$l_previous_snapshot" ]; then
		if ! l_size_dataset=$(zxfer_run_source_zfs_cmd send -nPv -I "$l_previous_snapshot" "$l_current_snapshot" 2>&1); then
			zxfer_throw_error "Error calculating incremental estimate: $l_size_dataset"
		fi
	else
		if ! l_size_dataset=$(zxfer_run_source_zfs_cmd send -nPv "$l_current_snapshot" 2>&1); then
			zxfer_throw_error "Error calculating estimate: $l_size_dataset"
		fi
	fi
	l_size_est=$(echo "$l_size_dataset" | grep ^size | tail -n 1 | cut -f 2)

	echo "$l_size_est"
}

zxfer_setup_progress_dialog() {
	l_size_est=$1
	l_snapshot=$2

	l_progress_dialog=$(echo "$g_option_D_display_progress_bar" |
		sed "s#%%size%%#$l_size_est#g" |
		sed "s#%%title%%#$l_snapshot#g")

	echo "$l_progress_dialog"
}

zxfer_progress_passthrough() {
	l_progress_dialog=$1

	# Tee stdin to the progress command while preserving the send stream.
	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.progress"
	l_fifo_dir=$(zxfer_create_private_temp_dir "$l_temp_prefix") || {
		zxfer_echoV "Unable to create FIFO for progress bar; continuing without it."
		cat
		return $?
	}
	l_fifo=$l_fifo_dir/fifo
	l_old_umask=$(umask)
	umask 077
	if ! mkfifo "$l_fifo"; then
		umask "$l_old_umask"
		zxfer_echoV "Unable to mkfifo $l_fifo for progress bar; continuing without it."
		rm -rf "$l_fifo_dir"
		cat
		return $?
	fi
	umask "$l_old_umask"

	# Explicitly lock down the FIFO permissions in case umask enforcement fails.
	if ! chmod 600 "$l_fifo"; then
		zxfer_echoV "Unable to secure permissions on $l_fifo for progress bar; continuing without it."
		rm -rf "$l_fifo_dir"
		cat
		return $?
	fi

	sh -c "$l_progress_dialog" <"$l_fifo" &
	l_progress_pid=$!
	zxfer_register_cleanup_pid "$l_progress_pid"

	tee "$l_fifo"
	l_tee_status=$?

	wait "$l_progress_pid" 2>/dev/null
	l_progress_status=$?
	zxfer_unregister_cleanup_pid "$l_progress_pid"
	rm -f "$l_fifo"
	rmdir "$l_fifo_dir" 2>/dev/null || rm -rf "$l_fifo_dir"

	if [ "$l_progress_status" -ne 0 ]; then
		zxfer_echoV "Progress bar command exited with status $l_progress_status"
	fi

	return "$l_tee_status"
}

zxfer_handle_progress_bar_option() {
	l_snapshot=$1
	l_previous_snapshot=$2
	l_progress_bar_cmd=""
	l_size_est=""
	l_use_fast_estimate=0

	# Calculate the size estimate only when the progress template uses it.
	if zxfer_progress_dialog_uses_size_estimate; then
		if zxfer_should_use_fast_progress_estimate; then
			l_use_fast_estimate=1
		fi
		l_size_est=$(zxfer_calculate_size_estimate "$l_snapshot" "$l_previous_snapshot" "$l_use_fast_estimate")
	fi
	l_progress_dialog=$(zxfer_setup_progress_dialog "$l_size_est" "$l_snapshot")

	# Modify the send command to include the progress dialog
	l_escaped_progress_dialog=$(zxfer_escape_for_single_quotes "$l_progress_dialog")
	l_progress_bar_cmd="| dd obs=1048576 | dd bs=1048576 | zxfer_progress_passthrough '$l_escaped_progress_dialog'"

	echo "$l_progress_bar_cmd"
}

#
# Returns the send command. If no previous snapshot is provided,
# a full snapshot is sent starting from the first snapshot which is set
# in zxfer_get_last_common_snapshot()
# Takes g_option_V_very_verbose, g_option_w_raw_send, g_first_source_snap
#
zxfer_get_send_command() {
	l_previous_snapshot=$1
	l_current_snapshot=$2
	l_zfs_cmd=${3:-$g_cmd_zfs}
	l_mode=${4:-display}

	l_v=""
	if [ "$g_option_V_very_verbose" -eq 1 ]; then
		l_v="-v"
	fi

	# Include raw-send mode when requested.
	l_w=""
	if [ "$g_option_w_raw_send" -eq 1 ]; then
		l_w="-w"
	fi

	# Without a previous snapshot, send the current snapshot and create the target dataset.
	if [ -z "$l_previous_snapshot" ]; then
		if [ "$l_mode" = "exec" ]; then
			set -- "$l_zfs_cmd" send
			[ "$l_v" != "" ] && set -- "$@" "$l_v"
			[ "$l_w" != "" ] && set -- "$@" "$l_w"
			set -- "$@" "$l_current_snapshot"
			zxfer_build_shell_command_from_argv "$@"
			return # exit the function
		fi
		echo "$l_zfs_cmd send $l_v $l_w $l_current_snapshot"
		return # exit the function
	fi

	# Stream the full incremental range in one send operation.
	if [ "$l_mode" = "exec" ]; then
		set -- "$l_zfs_cmd" send
		[ "$l_v" != "" ] && set -- "$@" "$l_v"
		[ "$l_w" != "" ] && set -- "$@" "$l_w"
		set -- "$@" -I "$l_previous_snapshot" "$l_current_snapshot"
		zxfer_build_shell_command_from_argv "$@"
		return
	fi

	echo "$l_zfs_cmd send $l_v $l_w -I $l_previous_snapshot $l_current_snapshot"
}

zxfer_get_receive_command() {
	l_dest=$1
	l_zfs_cmd=${2:-$g_cmd_zfs}
	l_mode=${3:-display}
	if [ $# -ge 4 ]; then
		l_receive_force_flag=$4
	else
		l_receive_force_flag=${g_option_F_force_rollback:-}
	fi

	if [ "$l_mode" = "exec" ]; then
		set -- "$l_zfs_cmd" receive
		[ "$l_receive_force_flag" != "" ] && set -- "$@" "$l_receive_force_flag"
		set -- "$@" "$l_dest"
		zxfer_build_shell_command_from_argv "$@"
		return
	fi

	echo "$l_zfs_cmd receive $l_receive_force_flag $l_dest"
}

zxfer_wrap_command_with_ssh() {
	l_cmd=$1
	l_option=$2
	l_is_compress=$3
	l_direction=$4
	l_remote_compress_safe=${g_cmd_compress_safe:-}
	l_remote_decompress_safe=${g_cmd_decompress_safe:-}

	l_host_tokens=$(zxfer_split_host_spec_tokens "$l_option")
	l_host_token_count=0
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			[ "$l_token" = "" ] && continue
			l_host_token_count=$((l_host_token_count + 1))
		done <<EOF
$l_host_tokens
EOF
	fi

	if [ "$l_option" = "${g_option_O_origin_host:-}" ]; then
		l_remote_compress_safe=${g_origin_cmd_compress_safe:-$l_remote_compress_safe}
		l_remote_decompress_safe=${g_origin_cmd_decompress_safe:-$l_remote_decompress_safe}
	elif [ "$l_option" = "${g_option_T_target_host:-}" ]; then
		l_remote_compress_safe=${g_target_cmd_compress_safe:-$l_remote_compress_safe}
		l_remote_decompress_safe=${g_target_cmd_decompress_safe:-$l_remote_decompress_safe}
	fi

	if [ "$l_is_compress" -eq 0 ]; then
		if [ "$l_host_token_count" -gt 1 ]; then
			l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_cmd")
			zxfer_build_ssh_shell_command_for_host "$l_option" "$l_remote_shell_cmd"
		else
			zxfer_build_ssh_shell_command_for_host "$l_option" "$l_cmd"
		fi
	else
		if [ "$g_cmd_compress_safe" = "" ] || [ "$g_cmd_decompress_safe" = "" ] ||
			[ "$l_remote_compress_safe" = "" ] || [ "$l_remote_decompress_safe" = "" ]; then
			zxfer_throw_error "Compression enabled but commands are not configured safely."
		fi
		# when compression is enabled, send and receive are wrapped differently
		if [ "$l_direction" = "send" ]; then
			l_remote_cmd="$l_cmd | $l_remote_compress_safe"
			if [ "$l_host_token_count" -gt 1 ]; then
				l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_cmd")
				l_wrapped_remote_cmd=$(zxfer_build_ssh_shell_command_for_host "$l_option" "$l_remote_shell_cmd") || return 1
				echo "$l_wrapped_remote_cmd | $g_cmd_decompress_safe"
			else
				l_wrapped_remote_cmd=$(zxfer_build_ssh_shell_command_for_host "$l_option" "$l_remote_cmd") || return 1
				echo "$l_wrapped_remote_cmd | $g_cmd_decompress_safe"
			fi
		else
			l_remote_cmd="$l_remote_decompress_safe | $l_cmd"
			if [ "$l_host_token_count" -gt 1 ]; then
				l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_cmd")
				l_wrapped_remote_cmd=$(zxfer_build_ssh_shell_command_for_host "$l_option" "$l_remote_shell_cmd") || return 1
				echo "$g_cmd_compress_safe | $l_wrapped_remote_cmd"
			else
				l_wrapped_remote_cmd=$(zxfer_build_ssh_shell_command_for_host "$l_option" "$l_remote_cmd") || return 1
				echo "$g_cmd_compress_safe | $l_wrapped_remote_cmd"
			fi
		fi
	fi
}

zxfer_profile_record_send_receive_pipeline_metrics() {
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		return 0
	fi

	zxfer_profile_increment_counter g_zxfer_profile_source_zfs_calls
	zxfer_profile_increment_counter g_zxfer_profile_destination_zfs_calls
	zxfer_profile_increment_counter g_zxfer_profile_zfs_send_calls
	zxfer_profile_increment_counter g_zxfer_profile_zfs_receive_calls

	if [ -n "${g_option_O_origin_host:-}" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi
	if [ -n "${g_option_T_target_host:-}" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_T_target_host" destination
	fi
}

zxfer_open_send_job_completion_queue() {
	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ]; then
		return 0
	fi
	if [ "${g_zfs_send_job_queue_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.queue"
	l_queue_dir=$(zxfer_create_private_temp_dir "$l_temp_prefix")
	if [ "$l_queue_dir" = "" ]; then
		zxfer_echoV "Unable to create rolling send/receive completion queue; falling back to batch waits."
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi
	l_queue_path=$l_queue_dir/queue

	l_old_umask=$(umask)
	umask 077
	if ! mkfifo "$l_queue_path"; then
		umask "$l_old_umask"
		zxfer_echoV "Unable to create rolling send/receive completion queue; falling back to batch waits."
		rm -rf "$l_queue_dir"
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi
	umask "$l_old_umask"

	if ! chmod 600 "$l_queue_path"; then
		zxfer_echoV "Unable to secure rolling send/receive completion queue; falling back to batch waits."
		rm -rf "$l_queue_dir"
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi

	if ! zxfer_open_send_job_completion_queue_fd "$l_queue_path"; then
		zxfer_echoV "Unable to open rolling send/receive completion queue; falling back to batch waits."
		rm -rf "$l_queue_dir"
		g_zfs_send_job_queue_unavailable=1
		return 1
	fi

	rm -f "$l_queue_path"
	rmdir "$l_queue_dir" 2>/dev/null || rm -rf "$l_queue_dir"
	g_zfs_send_job_queue_open=1
	return 0
}

zxfer_open_send_job_completion_queue_fd() {
	exec 8<>"$1"
}

zxfer_close_send_job_completion_queue() {
	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ]; then
		exec 8<&- 2>/dev/null || true
	fi
	g_zfs_send_job_queue_open=0
}

zxfer_register_send_job() {
	l_pid=$1
	l_status_file=$2

	zxfer_register_cleanup_pid "$l_pid"

	if [ -n "${g_zfs_send_job_pids:-}" ]; then
		g_zfs_send_job_pids="$g_zfs_send_job_pids $l_pid"
	else
		g_zfs_send_job_pids=$l_pid
	fi

	if [ -n "${g_zfs_send_job_records:-}" ]; then
		g_zfs_send_job_records="$g_zfs_send_job_records
$l_pid	$l_status_file"
	else
		g_zfs_send_job_records="$l_pid	$l_status_file"
	fi

	g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
}

zxfer_find_send_job_pid_by_status_file() {
	l_status_file=$1

	while IFS='	' read -r l_pid l_record_status_file || [ -n "${l_pid}${l_record_status_file}" ]; do
		[ -n "$l_pid" ] || continue
		if [ "$l_record_status_file" = "$l_status_file" ]; then
			printf '%s\n' "$l_pid"
			return 0
		fi
	done <<-EOF
		${g_zfs_send_job_records:-}
	EOF

	return 1
}

zxfer_unregister_send_job() {
	l_pid=$1
	l_remaining_pids=""
	l_remaining_records=""
	l_removed=0
	l_removed_status_file=""

	for l_existing_pid in ${g_zfs_send_job_pids:-}; do
		[ "$l_existing_pid" = "$l_pid" ] && continue
		if [ -n "$l_remaining_pids" ]; then
			l_remaining_pids="$l_remaining_pids $l_existing_pid"
		else
			l_remaining_pids=$l_existing_pid
		fi
	done

	while IFS='	' read -r l_record_pid l_record_status_file || [ -n "${l_record_pid}${l_record_status_file}" ]; do
		[ -n "$l_record_pid" ] || continue
		if [ "$l_record_pid" = "$l_pid" ]; then
			l_removed=1
			l_removed_status_file=$l_record_status_file
			continue
		fi
		if [ -n "$l_remaining_records" ]; then
			l_remaining_records="$l_remaining_records
$l_record_pid	$l_record_status_file"
		else
			l_remaining_records="$l_record_pid	$l_record_status_file"
		fi
	done <<-EOF
		${g_zfs_send_job_records:-}
	EOF

	g_zfs_send_job_pids=$l_remaining_pids
	g_zfs_send_job_records=$l_remaining_records
	zxfer_unregister_cleanup_pid "$l_pid"
	if [ "$l_removed" -eq 1 ] && [ "$g_count_zfs_send_jobs" -gt 0 ]; then
		g_count_zfs_send_jobs=$((g_count_zfs_send_jobs - 1))
	fi
	if [ "$l_removed_status_file" != "" ]; then
		rm -f "$l_removed_status_file"
	fi
}

zxfer_terminate_remaining_send_jobs() {
	for l_remaining_pid in ${g_zfs_send_job_pids:-}; do
		kill "$l_remaining_pid" 2>/dev/null || true
		zxfer_unregister_cleanup_pid "$l_remaining_pid"
	done

	while IFS='	' read -r l_record_pid l_record_status_file || [ -n "${l_record_pid}${l_record_status_file}" ]; do
		[ -n "$l_record_status_file" ] || continue
		rm -f "$l_record_status_file"
	done <<-EOF
		${g_zfs_send_job_records:-}
	EOF

	g_zfs_send_job_pids=""
	g_zfs_send_job_records=""
	g_count_zfs_send_jobs=0
	zxfer_close_send_job_completion_queue
}

zxfer_run_background_pipeline() {
	l_exec_cmd=$1
	l_display_cmd=$2
	l_status_file=$3
	l_job_status=0

	zxfer_record_last_command_string "$l_exec_cmd"
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $l_display_cmd"
	else
		zxfer_echov "$l_display_cmd"
		eval "$l_exec_cmd"
		l_job_status=$?
	fi

	printf '%s\n' "$l_job_status" >"$l_status_file" 2>/dev/null || :
	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ]; then
		printf '%s\n' "$l_status_file" >&8 2>/dev/null || :
	fi

	return "$l_job_status"
}

zxfer_wait_for_next_zfs_send_job_completion() {
	l_reason=$1
	l_completed_status_file=""
	l_pid=""
	l_pid_status=""
	l_wait_status=0

	[ "${g_count_zfs_send_jobs:-0}" -gt 0 ] || return 0

	if [ "${g_zfs_send_job_queue_open:-0}" -ne 1 ] || [ -z "${g_zfs_send_job_records:-}" ]; then
		zxfer_wait_for_zfs_send_jobs "$l_reason"
		return 0
	fi

	if ! IFS= read -r l_completed_status_file <&8; then
		zxfer_terminate_remaining_send_jobs
		zxfer_throw_error "Failed waiting for zfs send/receive jobs."
	fi

	if ! l_pid=$(zxfer_find_send_job_pid_by_status_file "$l_completed_status_file"); then
		rm -f "$l_completed_status_file"
		zxfer_terminate_remaining_send_jobs
		zxfer_throw_error "Failed to match a completed zfs send/receive job to a tracked PID."
	fi

	wait "$l_pid" 2>/dev/null || l_wait_status=$?
	if [ -f "$l_completed_status_file" ]; then
		l_pid_status=$(tr -d '\r\n' <"$l_completed_status_file" 2>/dev/null || :)
	fi
	case "$l_pid_status" in
	'' | *[!0-9]*)
		l_pid_status=$l_wait_status
		;;
	esac

	zxfer_unregister_send_job "$l_pid"
	if [ "${g_count_zfs_send_jobs:-0}" -eq 0 ]; then
		zxfer_close_send_job_completion_queue
	fi

	if [ "$l_pid_status" -ne 0 ]; then
		zxfer_terminate_remaining_send_jobs
		zxfer_throw_error "zfs send/receive job failed (PID $l_pid, exit $l_pid_status)."
	fi
}

zxfer_wait_for_zfs_send_jobs_legacy() {
	l_reason=$1

	for l_pid in $g_zfs_send_job_pids; do
		wait "$l_pid"
		l_pid_status=$?
		zxfer_unregister_cleanup_pid "$l_pid"
		if [ "$l_pid_status" -ne 0 ]; then
			for l_remaining_pid in $g_zfs_send_job_pids; do
				[ "$l_remaining_pid" = "$l_pid" ] && continue
				kill "$l_remaining_pid" 2>/dev/null || true
				zxfer_unregister_cleanup_pid "$l_remaining_pid"
			done
			g_zfs_send_job_pids=""
			g_count_zfs_send_jobs=0
			zxfer_throw_error "zfs send/receive job failed (PID $l_pid, exit $l_pid_status)."
		fi
	done

	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=0
}

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

	if [ "${g_zfs_send_job_queue_open:-0}" -eq 1 ] && [ -n "${g_zfs_send_job_records:-}" ]; then
		while [ "${g_count_zfs_send_jobs:-0}" -gt 0 ]; do
			zxfer_wait_for_next_zfs_send_job_completion ""
		done
		g_zfs_send_job_pids=""
		g_zfs_send_job_records=""
		g_count_zfs_send_jobs=0
		zxfer_close_send_job_completion_queue
		return 0
	fi

	zxfer_wait_for_zfs_send_jobs_legacy "$l_reason"
}

#
# Handle zfs send/receive
# Takes $g_option_D_display_progress_bar $g_option_z_compress, $g_option_O_origin_host, $g_option_T_target_host
#
zxfer_zfs_send_receive() {
	zxfer_set_failure_stage "send/receive"
	zxfer_echoV "Begin zxfer_zfs_send_receive()"
	l_previous_snapshot=$1
	l_current_snapshot=$2
	l_dest=$3
	# 4th optional parameter specifies if background process is allowed, with a default to 1
	l_is_allow_background=${4:-1}
	if [ $# -ge 5 ]; then
		l_receive_force_flag=$5
	else
		l_receive_force_flag=${g_option_F_force_rollback:-}
	fi
	l_send_zfs_cmd=$g_cmd_zfs
	l_recv_zfs_cmd=$g_cmd_zfs
	l_did_run_in_background=0

	if [ "$g_option_O_origin_host" != "" ]; then
		l_send_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	fi
	if [ "$g_option_T_target_host" != "" ]; then
		l_recv_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	fi

	# Set up the send and receive commands
	l_send_display_cmd=$(zxfer_get_send_command "$l_previous_snapshot" "$l_current_snapshot" "$l_send_zfs_cmd")
	l_recv_display_cmd=$(zxfer_get_receive_command "$l_dest" "$l_recv_zfs_cmd" display "$l_receive_force_flag")
	l_send_cmd=$(zxfer_get_send_command "$l_previous_snapshot" "$l_current_snapshot" "$l_send_zfs_cmd" "exec")
	l_recv_cmd=$(zxfer_get_receive_command "$l_dest" "$l_recv_zfs_cmd" "exec" "$l_receive_force_flag")
	if [ "$l_receive_force_flag" != "" ]; then
		zxfer_echov "Receive-side force flag (-F) is active for destination [$l_dest]."
	fi

	if [ "$g_option_O_origin_host" != "" ]; then
		l_send_display_cmd=$(zxfer_wrap_command_with_ssh "$l_send_display_cmd" "$g_option_O_origin_host" "$g_option_z_compress" "send")
		l_send_cmd=$(zxfer_wrap_command_with_ssh "$l_send_cmd" "$g_option_O_origin_host" "$g_option_z_compress" "send")
	fi
	if [ "$g_option_T_target_host" != "" ]; then
		l_recv_display_cmd=$(zxfer_wrap_command_with_ssh "$l_recv_display_cmd" "$g_option_T_target_host" "$g_option_z_compress" "receive")
		l_recv_cmd=$(zxfer_wrap_command_with_ssh "$l_recv_cmd" "$g_option_T_target_host" "$g_option_z_compress" "receive")
	fi

	# Perform this after ssh wrapping occurs
	if [ "$g_option_D_display_progress_bar" != "" ]; then
		l_progress_bar_cmd=$(zxfer_handle_progress_bar_option "$l_current_snapshot" "$l_previous_snapshot")
		l_send_display_cmd="$l_send_display_cmd $l_progress_bar_cmd"
		l_send_cmd="$l_send_cmd $l_progress_bar_cmd"
	fi

	l_pipeline_display_cmd="$l_send_display_cmd | $l_recv_display_cmd"
	l_pipeline_exec_cmd="$l_send_cmd | $l_recv_cmd"
	zxfer_profile_increment_counter g_zxfer_profile_send_receive_pipeline_commands
	zxfer_profile_record_bucket send_receive_setup
	zxfer_profile_record_send_receive_pipeline_metrics

	l_job_limit=${g_option_j_jobs:-1}
	case $l_job_limit in
	'' | *[!0-9]*) l_job_limit=1 ;; # fall back to safe single-job mode if unset/invalid
	esac

	if [ "$l_is_allow_background" -eq 1 ] && [ "$l_job_limit" -gt 1 ]; then
		l_use_rolling_pool=0
		if zxfer_open_send_job_completion_queue; then
			l_use_rolling_pool=1
		fi

		if [ "$g_count_zfs_send_jobs" -ge "$l_job_limit" ]; then
			zxfer_echov "Max jobs reached [$g_count_zfs_send_jobs]. Waiting for jobs to complete."
			if [ "$l_use_rolling_pool" -eq 1 ]; then
				zxfer_wait_for_next_zfs_send_job_completion "job limit"
			else
				zxfer_wait_for_zfs_send_jobs "job limit"
			fi
		fi

		zxfer_profile_increment_counter g_zxfer_profile_send_receive_background_pipeline_commands
		if [ "$l_use_rolling_pool" -eq 1 ]; then
			l_status_file=$(zxfer_get_temp_file)
			zxfer_run_background_pipeline "$l_pipeline_exec_cmd" "$l_pipeline_display_cmd" "$l_status_file" &
		else
			zxfer_execute_command "$l_pipeline_exec_cmd" 0 "$l_pipeline_display_cmd" &
		fi
		l_background_job_pid=$!
		l_did_run_in_background=1
		if [ "$l_use_rolling_pool" -eq 1 ]; then
			zxfer_register_send_job "$l_background_job_pid" "$l_status_file"
		else
			zxfer_register_cleanup_pid "$l_background_job_pid"
			g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
			if [ -z "$g_zfs_send_job_pids" ]; then
				g_zfs_send_job_pids=$l_background_job_pid
			else
				g_zfs_send_job_pids="$g_zfs_send_job_pids $l_background_job_pid"
			fi
		fi
	else
		zxfer_execute_command "$l_pipeline_exec_cmd" 0 "$l_pipeline_display_cmd"
	fi
	if [ "$g_option_n_dryrun" -ne 1 ]; then
		if [ "$l_did_run_in_background" -eq 0 ]; then
			zxfer_note_destination_dataset_exists "$l_dest"
		fi
		zxfer_invalidate_destination_property_cache "$l_dest"
		# shellcheck disable=SC2034
		g_is_performed_send_destroy=1
	fi

	zxfer_echoV "End zxfer_zfs_send_receive()"
}
