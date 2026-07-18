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
# owns globals: current-shell result scratch for progress size/progress command
# rendering.
# reads globals: g_option_j_jobs, g_option_D_display_progress_bar, remote host specs, and zfs/compression helpers.
# mutates caches: none; transfer completion state is published by send jobs.
# returns via stdout: rendered send/receive commands, progress dialogs, and size estimates.

# Purpose: Reset the send receive state so the next send/receive pass starts
# from a clean state.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_send_receive_state() {
	g_zxfer_progress_size_estimate_result=""
	g_zxfer_progress_probe_output_result=""
	g_zxfer_progress_bar_command_result=""
}

# Purpose: Check whether the progress dialog uses size estimate.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need a boolean answer about the progress
# dialog.
#
# The snapshot size is estimated. The estimate does not take into consideration
# the compression ratio of the data. The estimate is based on the size of the
# dataset. When compression is used, the bar will terminate sooner,
# ending at the compression ratio.
# Uses the source-side zfs helper selected by $g_LZFS.
zxfer_progress_dialog_uses_size_estimate() {
	case ${g_option_D_display_progress_bar:-} in
	*%%size%%*) return 0 ;;
	esac

	return 1
}

# Purpose: Check whether zxfer should use fast progress estimate.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need a boolean branch decision about the
# current configuration or live state.
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

# Purpose: Extract the numeric progress estimate from the serialized input this
# module works with.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
zxfer_extract_numeric_progress_estimate() {
	l_estimate_output=$1
	l_estimate_value=$(printf '%s\n' "$l_estimate_output" | tail -n 1 | tr -d '\r')

	case $l_estimate_value in
	'' | *[!0-9]*)
		# shellcheck disable=SC2016  # $1/$2 are awk fields, not shell expansions.
		l_estimate_value=$(printf '%s\n' "$l_estimate_output" |
			"${g_cmd_awk:-awk}" '
				BEGIN { l_size = "" }
				$1 == "size" { l_size = $2 }
				END {
					if (l_size != "") {
						print l_size
					}
				}
			' | tr -d '\r')
		case $l_estimate_value in
		'' | *[!0-9]*) return 1 ;;
		esac
		;;
	esac

	printf '%s\n' "$l_estimate_value"
}

# Purpose: Capture the progress estimate probe output into staged state or
# module globals for later use.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need a checked snapshot of command output or
# computed state.
zxfer_capture_progress_estimate_probe_output() {
	g_zxfer_progress_probe_output_result=""

	l_status=0
	zxfer_capture_runtime_artifact_combined_command_output "zxfer-progress-estimate" "$@" ||
		l_status=$?
	l_capture_result=$g_zxfer_runtime_artifact_read_result
	case "$l_capture_result" in
	*'
')
		l_capture_result=${l_capture_result%?}
		;;
	esac
	g_zxfer_progress_probe_output_result=$l_capture_result
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	return 0
}

# Purpose: Calculate the fast full size estimate from the active configuration
# and runtime state.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need a derived value without duplicating the
# calculation.
zxfer_calculate_fast_full_size_estimate() {
	l_current_snapshot=$1

	l_size_dataset=$(zxfer_run_source_zfs_cmd list -Hp -o referenced "$l_current_snapshot" 2>&1) ||
		return "$?"

	zxfer_extract_numeric_progress_estimate "$l_size_dataset"
}

# Purpose: Calculate the fast incremental size estimate from the active
# configuration and runtime state.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need a derived value without duplicating the
# calculation.
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

	l_size_dataset=$(zxfer_run_source_zfs_cmd get -Hpo value "written@$l_previous_snapshot_name" "$l_current_dataset" 2>&1) ||
		return "$?"

	zxfer_extract_numeric_progress_estimate "$l_size_dataset"
}

# Purpose: Calculate the size estimate from the active configuration and
# runtime state.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need a derived value without duplicating the
# calculation.
zxfer_calculate_size_estimate() {
	l_current_snapshot=$1
	l_previous_snapshot=$2
	l_prefer_fast_estimate=${3:-0}
	g_zxfer_progress_size_estimate_result=""

	if [ "$l_prefer_fast_estimate" -eq 1 ]; then
		if [ -n "$l_previous_snapshot" ]; then
			if l_size_est=$(zxfer_calculate_fast_incremental_size_estimate "$l_current_snapshot" "$l_previous_snapshot"); then
				g_zxfer_progress_size_estimate_result=$l_size_est
				zxfer_echoV "Using fast approximate incremental progress estimate for $l_current_snapshot."
				echo "$l_size_est"
				return 0
			fi
			zxfer_echoV "Falling back to exact incremental progress estimate for $l_current_snapshot."
		else
			if l_size_est=$(zxfer_calculate_fast_full_size_estimate "$l_current_snapshot"); then
				g_zxfer_progress_size_estimate_result=$l_size_est
				zxfer_echoV "Using fast approximate full progress estimate for $l_current_snapshot."
				echo "$l_size_est"
				return 0
			fi
			zxfer_echoV "Falling back to exact full progress estimate for $l_current_snapshot."
		fi
	fi

	if [ -n "$l_previous_snapshot" ]; then
		l_status=0
		zxfer_capture_progress_estimate_probe_output \
			zxfer_run_source_zfs_cmd send -nPv -I "$l_previous_snapshot" "$l_current_snapshot" ||
			l_status=$?
		l_size_dataset=$g_zxfer_progress_probe_output_result
	else
		l_status=0
		zxfer_capture_progress_estimate_probe_output \
			zxfer_run_source_zfs_cmd send -nPv "$l_current_snapshot" ||
			l_status=$?
		l_size_dataset=$g_zxfer_progress_probe_output_result
	fi
	if l_size_est=$(zxfer_extract_numeric_progress_estimate "$l_size_dataset"); then
		:
	else
		if [ -n "$l_previous_snapshot" ]; then
			if [ "$l_status" -ne 0 ]; then
				zxfer_throw_error "Error calculating incremental estimate: $l_size_dataset" "$l_status"
			fi
			zxfer_throw_error "Error parsing incremental estimate: $l_size_dataset"
		fi
		if [ "$l_status" -ne 0 ]; then
			zxfer_throw_error "Error calculating estimate: $l_size_dataset" "$l_status"
		fi
		zxfer_throw_error "Error parsing estimate: $l_size_dataset"
	fi

	g_zxfer_progress_size_estimate_result=$l_size_est
	echo "$l_size_est"
}

# Purpose: Set up the progress dialog before the surrounding flow depends on
# it.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need the supporting state or transport
# prepared in advance.
zxfer_setup_progress_dialog() {
	l_size_est=$1
	l_snapshot=$2

	l_progress_dialog=$(echo "$g_option_D_display_progress_bar" |
		sed "s#%%size%%#$l_size_est#g" |
		sed "s#%%title%%#$l_snapshot#g")

	echo "$l_progress_dialog"
}

# Purpose: Handle progress passthrough for the send/receive pipeline.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when the transfer path needs one shared progress helper instead
# of scattering progress-specific branches.
zxfer_progress_passthrough() {
	l_progress_dialog=$1

	# Tee stdin to the progress command while preserving the send stream.
	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.progress"
	zxfer_create_private_temp_dir "$l_temp_prefix" >/dev/null || {
		zxfer_echoV "Unable to create FIFO for progress bar; continuing without it."
		l_passthrough_status=0
		cat || l_passthrough_status=$?
		return "$l_passthrough_status"
	}
	l_fifo_dir=$g_zxfer_runtime_artifact_path_result
	l_fifo=$l_fifo_dir/fifo
	l_old_umask=$(umask)
	umask 077
	if ! mkfifo "$l_fifo"; then
		umask "$l_old_umask"
		zxfer_echoV "Unable to mkfifo $l_fifo for progress bar; continuing without it."
		zxfer_cleanup_runtime_artifact_path "$l_fifo_dir"
		l_passthrough_status=0
		cat || l_passthrough_status=$?
		return "$l_passthrough_status"
	fi
	umask "$l_old_umask"

	# Explicitly lock down the FIFO permissions in case umask enforcement fails.
	if ! chmod 600 "$l_fifo"; then
		zxfer_echoV "Unable to secure permissions on $l_fifo for progress bar; continuing without it."
		zxfer_cleanup_runtime_artifact_path "$l_fifo_dir"
		l_passthrough_status=0
		cat || l_passthrough_status=$?
		return "$l_passthrough_status"
	fi
	l_cleanup_wrapper_status=0
	l_cleanup_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path) || l_cleanup_wrapper_status=$?
	if [ "$l_cleanup_wrapper_status" -ne 0 ]; then
		zxfer_echoV "Unable to resolve the cleanup wrapper for the progress dialog; continuing without it."
		zxfer_cleanup_runtime_artifact_path "$l_fifo_dir"
		l_passthrough_status=0
		cat || l_passthrough_status=$?
		return "$l_passthrough_status"
	fi

	sh -c 'exec "$1" "$2" <"$3" >/dev/null' sh \
		"$l_cleanup_wrapper_script" \
		"$l_progress_dialog" \
		"$l_fifo" &
	l_progress_pid=$!
	if ! zxfer_register_cleanup_pid \
		"$l_progress_pid" "progress dialog helper"; then
		zxfer_abort_direct_child_pid \
			"$l_progress_pid" TERM "progress dialog helper" || {
			l_abort_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_fifo_dir"
			return "$l_abort_status"
		}
		wait "$l_progress_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$l_progress_pid"
		zxfer_echoV "Unable to register validated cleanup metadata for the progress dialog; continuing without it."
		zxfer_cleanup_runtime_artifact_path "$l_fifo_dir"
		l_passthrough_status=0
		cat || l_passthrough_status=$?
		return "$l_passthrough_status"
	fi

	l_tee_status=0
	tee "$l_fifo" || l_tee_status=$?

	if wait "$l_progress_pid" 2>/dev/null; then
		l_progress_status=0
	else
		l_progress_status=$?
	fi
	zxfer_unregister_cleanup_pid "$l_progress_pid"
	zxfer_cleanup_runtime_artifact_path "$l_fifo_dir"

	if [ "$l_progress_status" -ne 0 ]; then
		zxfer_echoV "Progress bar command exited with status $l_progress_status"
	fi

	return "$l_tee_status"
}

# Purpose: Resolve the effective progress-bar command for the current
# transfer.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination before the pipeline decides whether to run a progress wrapper,
# estimate size, or pass the stream through unchanged.
zxfer_handle_progress_bar_option() {
	l_snapshot=$1
	l_previous_snapshot=$2
	l_progress_bar_cmd=""
	l_size_est=""
	l_use_fast_estimate=0
	g_zxfer_progress_bar_command_result=""

	# Calculate the size estimate only when the progress template uses it.
	if zxfer_progress_dialog_uses_size_estimate; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			zxfer_echoV "Dry run: skipping live %%size%% progress estimate discovery."
			l_size_est="UNKNOWN"
		else
			if zxfer_should_use_fast_progress_estimate; then
				l_use_fast_estimate=1
			fi
			l_size_est_status=0
			zxfer_calculate_size_estimate "$l_snapshot" "$l_previous_snapshot" "$l_use_fast_estimate" >/dev/null ||
				l_size_est_status=$?
			[ "$l_size_est_status" -eq 0 ] || return "$l_size_est_status"
			l_size_est=$g_zxfer_progress_size_estimate_result
			if [ -z "$l_size_est" ]; then
				zxfer_throw_error "Failed to calculate progress size estimate for $l_snapshot."
			fi
		fi
	fi
	l_progress_dialog=$(zxfer_setup_progress_dialog "$l_size_est" "$l_snapshot")

	# Modify the send command to include the progress dialog.
	l_escaped_progress_dialog=$(zxfer_escape_for_single_quotes "$l_progress_dialog")
	l_progress_bar_cmd="| zxfer_progress_passthrough '$l_escaped_progress_dialog'"
	g_zxfer_progress_bar_command_result=$l_progress_bar_cmd

	echo "$l_progress_bar_cmd"
}

# Purpose: Return the send command in the form expected by later helpers.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when sibling helpers need the same lookup without duplicating
# module logic.
#
# Returns the send command. If no previous snapshot is provided,
# a full snapshot is sent starting from the first snapshot which is set
# in zxfer_get_last_common_snapshot()
# Takes g_option_V_very_verbose, g_option_w_raw_send, g_first_source_snap
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

# Purpose: Return the receive command in the form expected by later helpers.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when sibling helpers need the same lookup without duplicating
# module logic.
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

# Purpose: Wrap the command with SSH in the execution or transport layer this
# module owns.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when later helpers need an existing command or payload adapted
# to a different shell or transport context.
zxfer_wrap_command_with_ssh() {
	l_cmd=$1
	l_option=$2
	l_is_compress=$3
	l_direction=$4
	l_remote_compress_safe=${g_cmd_compress_safe:-}
	l_remote_decompress_safe=${g_cmd_decompress_safe:-}

	if [ "$l_option" = "${g_option_O_origin_host:-}" ]; then
		l_remote_compress_safe=${g_origin_cmd_compress_safe:-$l_remote_compress_safe}
		l_remote_decompress_safe=${g_origin_cmd_decompress_safe:-$l_remote_decompress_safe}
	elif [ "$l_option" = "${g_option_T_target_host:-}" ]; then
		l_remote_compress_safe=${g_target_cmd_compress_safe:-$l_remote_compress_safe}
		l_remote_decompress_safe=${g_target_cmd_decompress_safe:-$l_remote_decompress_safe}
	fi

	if [ "$l_is_compress" -eq 0 ]; then
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$l_option" "$l_cmd" ||
			return "$?"
		printf '%s' "$g_zxfer_prepared_ssh_shell_command_result"
	else
		if [ "$g_cmd_compress_safe" = "" ] || [ "$g_cmd_decompress_safe" = "" ] ||
			[ "$l_remote_compress_safe" = "" ] || [ "$l_remote_decompress_safe" = "" ]; then
			zxfer_throw_error "Compression enabled but commands are not configured safely."
		fi
		# when compression is enabled, send and receive are wrapped differently
		if [ "$l_direction" = "send" ]; then
			l_remote_cmd="$l_cmd | $l_remote_compress_safe"
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$l_option" "$l_remote_cmd" ||
				return "$?"
			l_wrapped_remote_cmd=$g_zxfer_prepared_ssh_shell_command_result
			echo "$l_wrapped_remote_cmd | $g_cmd_decompress_safe"
		else
			l_remote_cmd="$l_remote_decompress_safe | $l_cmd"
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$l_option" "$l_remote_cmd" ||
				return "$?"
			l_wrapped_remote_cmd=$g_zxfer_prepared_ssh_shell_command_result
			echo "$g_cmd_compress_safe | $l_wrapped_remote_cmd"
		fi
	fi
}

# Purpose: Record or emit the record send receive pipeline metrics for end-of-
# run profiling.
# Usage: Called during send/receive command setup, progress handling, and job
# coordination when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_record_send_receive_pipeline_metrics() {
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		return 0
	fi

	zxfer_profile_record_startup_latency_once
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
		l_progress_bar_status=0
		zxfer_handle_progress_bar_option "$l_current_snapshot" "$l_previous_snapshot" >/dev/null ||
			l_progress_bar_status=$?
		[ "$l_progress_bar_status" -eq 0 ] || return "$l_progress_bar_status"
		l_progress_bar_cmd=$g_zxfer_progress_bar_command_result
		if [ -z "$l_progress_bar_cmd" ]; then
			zxfer_throw_error "Failed to build progress wrapper for $l_current_snapshot."
		fi
		l_send_display_cmd="$l_send_display_cmd $l_progress_bar_cmd"
		l_send_cmd="$l_send_cmd $l_progress_bar_cmd"
	fi

	l_pipeline_display_cmd="$l_send_display_cmd | $l_recv_display_cmd"
	l_pipeline_exec_cmd="$l_send_cmd | $l_recv_cmd"
	zxfer_profile_increment_counter g_zxfer_profile_send_receive_pipeline_commands
	zxfer_profile_record_bucket send_receive_setup
	zxfer_profile_record_send_receive_pipeline_metrics
	zxfer_schedule_send_receive_pipeline \
		"$l_pipeline_exec_cmd" \
		"$l_pipeline_display_cmd" \
		"$l_current_snapshot" \
		"$l_dest" \
		"$l_is_allow_background" || return "$?"
	if [ "$g_option_n_dryrun" -ne 1 ]; then
		if [ "$g_zxfer_send_receive_ran_in_background_result" -eq 0 ]; then
			zxfer_note_destination_receive_completed "$l_dest"
			zxfer_invalidate_destination_property_mutation_cache "$l_dest"
			# Post-receive divergence verification (foreground completion
			# side): the invalidation above bumped the destination mutation
			# generation, so the verification re-captures a fresh live view.
			zxfer_verify_converged_destination_after_receive "$l_dest"
			# Do not wipe the whole-tree destination snapshot record cache
			# here: the receive only changed this dataset's own snapshots,
			# planning for it already finished, and every later send/seed is
			# preceded by a live destination recheck. The wipe also cleared
			# the in-memory fallback list, so -d delete planning for every
			# dataset after the first receive saw an empty destination and
			# silently skipped its deletions.
		fi
		zxfer_mark_send_or_destroy_performed
	fi

	zxfer_echoV "End zxfer_zfs_send_receive()"
}
