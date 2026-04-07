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

# for ShellCheck
if false; then
	# shellcheck source=src/zxfer_globals.sh
	. ./zxfer_globals.sh
fi

################################################################################
# ZFS MODE FUNCTIONS RELATED TO zfs_send_receive
################################################################################

#
# The snapshot size is estimated. The estimate does not take into consideration
# the compression ratio of the data. The estimate is based on the size of the
# dataset. When compression is used, the bar will terminate sooner,
# ending at the compression ratio.
# Uses the source-side zfs helper selected by $g_LZFS.
#
calculate_size_estimate() {
	l_current_snapshot=$1
	l_previous_snapshot=$2

	if [ -n "$l_previous_snapshot" ]; then
		if ! l_size_dataset=$(run_source_zfs_cmd send -nPv -I "$l_previous_snapshot" "$l_current_snapshot" 2>&1); then
			throw_error "Error calculating incremental estimate: $l_size_dataset"
		fi
	else
		if ! l_size_dataset=$(run_source_zfs_cmd send -nPv "$l_current_snapshot" 2>&1); then
			throw_error "Error calculating estimate: $l_size_dataset"
		fi
	fi
	l_size_est=$(echo "$l_size_dataset" | grep ^size | tail -n 1 | cut -f 2)

	echo "$l_size_est"
}

setup_progress_dialog() {
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
	l_tmpdir=${TMPDIR:-/tmp}
	l_fifo=$(mktemp "$l_tmpdir/zxfer-progress.XXXXXX") || {
		echoV "Unable to create FIFO for progress bar; continuing without it."
		cat
		return $?
	}

	rm -f "$l_fifo"
	l_old_umask=$(umask)
	umask 077
	if ! mkfifo "$l_fifo"; then
		umask "$l_old_umask"
		echoV "Unable to mkfifo $l_fifo for progress bar; continuing without it."
		rm -f "$l_fifo"
		cat
		return $?
	fi
	umask "$l_old_umask"

	# Explicitly lock down the FIFO permissions in case umask enforcement fails.
	if ! chmod 600 "$l_fifo"; then
		echoV "Unable to secure permissions on $l_fifo for progress bar; continuing without it."
		rm -f "$l_fifo"
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

	if [ "$l_progress_status" -ne 0 ]; then
		echoV "Progress bar command exited with status $l_progress_status"
	fi

	return "$l_tee_status"
}

handle_progress_bar_option() {
	l_snapshot=$1
	l_previous_snapshot=$2
	l_progress_bar_cmd=""

	# Calculate the size estimate and set up the progress dialog
	l_size_est=$(calculate_size_estimate "$l_snapshot" "$l_previous_snapshot")
	l_progress_dialog=$(setup_progress_dialog "$l_size_est" "$l_snapshot")

	# Modify the send command to include the progress dialog
	l_escaped_progress_dialog=$(escape_for_single_quotes "$l_progress_dialog")
	l_progress_bar_cmd="| dd obs=1048576 | dd bs=1048576 | zxfer_progress_passthrough '$l_escaped_progress_dialog'"

	echo "$l_progress_bar_cmd"
}

#
# Returns the send command. If no previous snapshot is provided,
# a full snapshot is sent starting from the first snapshot which is set
# in get_last_common_snapshot()
# Takes g_option_V_very_verbose, g_option_w_raw_send, g_first_source_snap
#
get_send_command() {
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
			build_shell_command_from_argv "$@"
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
		build_shell_command_from_argv "$@"
		return
	fi

	echo "$l_zfs_cmd send $l_v $l_w -I $l_previous_snapshot $l_current_snapshot"
}

get_receive_command() {
	l_dest=$1
	l_zfs_cmd=${2:-$g_cmd_zfs}
	l_mode=${3:-display}

	if [ "$l_mode" = "exec" ]; then
		set -- "$l_zfs_cmd" receive
		[ "$g_option_F_force_rollback" != "" ] && set -- "$@" "$g_option_F_force_rollback"
		set -- "$@" "$l_dest"
		build_shell_command_from_argv "$@"
		return
	fi

	echo "$l_zfs_cmd receive $g_option_F_force_rollback $l_dest"
}

wrap_command_with_ssh() {
	l_cmd=$1
	l_option=$2
	l_is_compress=$3
	l_direction=$4

	l_host_tokens=$(split_host_spec_tokens "$l_option")
	l_host_token_count=0
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			[ "$l_token" = "" ] && continue
			l_host_token_count=$((l_host_token_count + 1))
		done <<EOF
$l_host_tokens
EOF
	fi

	if [ "$l_is_compress" -eq 0 ]; then
		if [ "$l_host_token_count" -gt 1 ]; then
			l_remote_shell_cmd=$(build_remote_sh_c_command "$l_cmd")
			build_ssh_shell_command_for_host "$l_option" "$l_remote_shell_cmd"
		else
			build_ssh_shell_command_for_host "$l_option" "$l_cmd"
		fi
	else
		if [ "$g_cmd_compress_safe" = "" ] || [ "$g_cmd_decompress_safe" = "" ]; then
			throw_error "Compression enabled but commands are not configured safely."
		fi
		# when compression is enabled, send and receive are wrapped differently
		if [ "$l_direction" = "send" ]; then
			l_remote_cmd="$l_cmd | $g_cmd_compress_safe"
			if [ "$l_host_token_count" -gt 1 ]; then
				l_remote_shell_cmd=$(build_remote_sh_c_command "$l_remote_cmd")
				l_wrapped_remote_cmd=$(build_ssh_shell_command_for_host "$l_option" "$l_remote_shell_cmd") || return 1
				echo "$l_wrapped_remote_cmd | $g_cmd_decompress_safe"
			else
				l_wrapped_remote_cmd=$(build_ssh_shell_command_for_host "$l_option" "$l_remote_cmd") || return 1
				echo "$l_wrapped_remote_cmd | $g_cmd_decompress_safe"
			fi
		else
			l_remote_cmd="$g_cmd_decompress_safe | $l_cmd"
			if [ "$l_host_token_count" -gt 1 ]; then
				l_remote_shell_cmd=$(build_remote_sh_c_command "$l_remote_cmd")
				l_wrapped_remote_cmd=$(build_ssh_shell_command_for_host "$l_option" "$l_remote_shell_cmd") || return 1
				echo "$g_cmd_compress_safe | $l_wrapped_remote_cmd"
			else
				l_wrapped_remote_cmd=$(build_ssh_shell_command_for_host "$l_option" "$l_remote_cmd") || return 1
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

wait_for_zfs_send_jobs() {
	l_reason=$1

	if [ "$l_reason" != "" ] && [ -n "$g_zfs_send_job_pids" ]; then
		echoV "Waiting for zfs send/receive jobs ($l_reason)."
	fi

	if [ -z "$g_zfs_send_job_pids" ]; then
		g_count_zfs_send_jobs=0
		return 0
	fi

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
			throw_error "zfs send/receive job failed (PID $l_pid, exit $l_pid_status)."
		fi
	done

	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=0
}

#
# Handle zfs send/receive
# Takes $g_option_D_display_progress_bar $g_option_z_compress, $g_option_O_origin_host, $g_option_T_target_host
#
zfs_send_receive() {
	zxfer_set_failure_stage "send/receive"
	echoV "Begin zfs_send_receive()"
	l_previous_snapshot=$1
	l_current_snapshot=$2
	l_dest=$3
	# 4th optional parameter specifies if background process is allowed, with a default to 1
	l_is_allow_background=${4:-1}
	l_send_zfs_cmd=$g_cmd_zfs
	l_recv_zfs_cmd=$g_cmd_zfs

	if [ "$g_option_O_origin_host" != "" ]; then
		l_send_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	fi
	if [ "$g_option_T_target_host" != "" ]; then
		l_recv_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	fi

	# Set up the send and receive commands
	l_send_display_cmd=$(get_send_command "$l_previous_snapshot" "$l_current_snapshot" "$l_send_zfs_cmd")
	l_recv_display_cmd=$(get_receive_command "$l_dest" "$l_recv_zfs_cmd")
	l_send_cmd=$(get_send_command "$l_previous_snapshot" "$l_current_snapshot" "$l_send_zfs_cmd" "exec")
	l_recv_cmd=$(get_receive_command "$l_dest" "$l_recv_zfs_cmd" "exec")
	if [ "${g_option_F_force_rollback:-}" != "" ]; then
		echov "Receive-side force flag (-F) is active for destination [$l_dest]."
	fi

	if [ "$g_option_O_origin_host" != "" ]; then
		l_send_display_cmd=$(wrap_command_with_ssh "$l_send_display_cmd" "$g_option_O_origin_host" "$g_option_z_compress" "send")
		l_send_cmd=$(wrap_command_with_ssh "$l_send_cmd" "$g_option_O_origin_host" "$g_option_z_compress" "send")
	fi
	if [ "$g_option_T_target_host" != "" ]; then
		l_recv_display_cmd=$(wrap_command_with_ssh "$l_recv_display_cmd" "$g_option_T_target_host" "$g_option_z_compress" "receive")
		l_recv_cmd=$(wrap_command_with_ssh "$l_recv_cmd" "$g_option_T_target_host" "$g_option_z_compress" "receive")
	fi

	# Perform this after ssh wrapping occurs
	if [ "$g_option_D_display_progress_bar" != "" ]; then
		l_progress_bar_cmd=$(handle_progress_bar_option "$l_current_snapshot" "$l_previous_snapshot")
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
		# implement naive job control.
		# if there are more than this many jobs, wait until they are all
		# completed before spawning new ones
		if [ "$g_count_zfs_send_jobs" -ge "$l_job_limit" ]; then
			echov "Max jobs reached [$g_count_zfs_send_jobs]. Waiting for jobs to complete."
			wait_for_zfs_send_jobs "job limit"
		fi

		# increment the job count
		g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
		zxfer_profile_increment_counter g_zxfer_profile_send_receive_background_pipeline_commands

		execute_command "$l_pipeline_exec_cmd" 0 "$l_pipeline_display_cmd" &
		l_background_job_pid=$!
		zxfer_register_cleanup_pid "$l_background_job_pid"
		if [ -z "$g_zfs_send_job_pids" ]; then
			g_zfs_send_job_pids=$l_background_job_pid
		else
			g_zfs_send_job_pids="$g_zfs_send_job_pids $l_background_job_pid"
		fi
	else
		execute_command "$l_pipeline_exec_cmd" 0 "$l_pipeline_display_cmd"
	fi
	if [ "$g_option_n_dryrun" -ne 1 ]; then
		# shellcheck disable=SC2034
		g_is_performed_send_destroy=1
	fi

	echoV "End zfs_send_receive()"
}
