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
# SEND / RECEIVE PIPELINE
################################################################################

# Module contract:
# owns globals: g_zxfer_progress_size_estimate_result,
# g_zxfer_progress_bar_command_result, and g_zxfer_wrapped_command_result.
# reads globals: the send options (-D, -F, -j, -O, -T, -V, -w, -z), the local
# and remote zfs paths, and the safe compression commands.
# mutates caches: none here; zxfer_schedule_send_receive_pipeline finishes
# each receive and sets the -Y marker.
# returns via stdout: none.
#
# Every command is rendered in the current shell as plain shell text, so the
# same string runs under eval (-j 1) or inside a -j job shell that has no
# zxfer functions, and is what -v displays.

# Purpose: Reset the send/receive result globals for a new session.
# Usage: zxfer_reset_send_receive_state; assignments only.
zxfer_reset_send_receive_state() {
	g_zxfer_progress_size_estimate_result=""
	g_zxfer_progress_bar_command_result=""
	g_zxfer_wrapped_command_result=""
}

# Purpose: Return 0 when the -D template uses the %%size%% macro.
# Usage: zxfer_progress_dialog_uses_size_estimate; prints nothing.
zxfer_progress_dialog_uses_size_estimate() {
	case ${g_option_D_display_progress_bar:-} in
	*%%size%%*) return 0 ;;
	esac
	return 1
}

# Purpose: Estimate the stream size that replaces %%size%% in the -D template.
# Usage: zxfer_calculate_size_estimate CURRENT_SNAPSHOT [PREVIOUS_SNAPSHOT];
# publishes g_zxfer_progress_size_estimate_result and throws when no size can
# be read.
#
# With -O, -T, or -j above 1, the cheap written@PREVIOUS or referenced
# property is tried first; otherwise, or when that fails, `zfs send -nPv`
# answers. Neither accounts for compression, so the bar may end early.
zxfer_calculate_size_estimate() {
	l_size_current=$1
	l_size_previous=${2:-}
	g_zxfer_progress_size_estimate_result=""
	if [ -n "$l_size_previous" ]; then
		l_size_kind=incremental
	else
		l_size_kind=full
	fi

	if [ -n "${g_option_O_origin_host:-}" ] || [ -n "${g_option_T_target_host:-}" ] ||
		[ "${g_option_j_jobs:-1}" -gt 1 ]; then
		if [ -n "$l_size_previous" ]; then
			l_size_output=$(zxfer_run_source_zfs_cmd get -Hpo value \
				"written@${l_size_previous#*@}" "${l_size_current%@*}" 2>&1) ||
				l_size_output=""
		else
			l_size_output=$(zxfer_run_source_zfs_cmd list -Hp -o referenced \
				"$l_size_current" 2>&1) || l_size_output=""
		fi
		# The property value is the last line of the probe output.
		l_size_output=${l_size_output##*"$ZXFER_LF"}
		l_size_output=${l_size_output%"$ZXFER_CR"}
		if zxfer_is_uint "$l_size_output"; then
			g_zxfer_progress_size_estimate_result=$l_size_output
			zxfer_echoV "Using fast approximate $l_size_kind progress estimate for $l_size_current."
			return 0
		fi
		zxfer_echoV "Falling back to exact $l_size_kind progress estimate for $l_size_current."
	fi

	l_size_status=0
	if [ -n "$l_size_previous" ]; then
		l_size_output=$(zxfer_run_source_zfs_cmd send -nPv -I "$l_size_previous" \
			"$l_size_current" 2>&1) || l_size_status=$?
	else
		l_size_output=$(zxfer_run_source_zfs_cmd send -nPv "$l_size_current" 2>&1) ||
			l_size_status=$?
	fi
	# The size is the -P "size" row, or a bare number on the last line. A
	# failed probe that still printed a size is accepted.
	# shellcheck disable=SC2016  # $0/$1/$2 are awk fields.
	l_size_value=$(printf '%s\n' "$l_size_output" | "${g_cmd_awk:-awk}" '
		{ sub(/\r$/, ""); last = $0 }
		$1 == "size" { size = $2 }
		END { print (size != "" ? size : last) }')
	if zxfer_is_uint "$l_size_value"; then
		g_zxfer_progress_size_estimate_result=$l_size_value
		return 0
	fi

	l_size_label=estimate
	[ -z "$l_size_previous" ] || l_size_label="incremental estimate"
	[ "$l_size_status" -eq 0 ] ||
		zxfer_throw_error "Error calculating $l_size_label: $l_size_output" "$l_size_status"
	zxfer_throw_error "Error parsing $l_size_label: $l_size_output"
}

# Purpose: Render the -D progress stage for one send as plain shell.
# Usage: zxfer_handle_progress_bar_option SNAPSHOT [PREVIOUS_SNAPSHOT];
# publishes "| { ... }" in g_zxfer_progress_bar_command_result and throws on
# failure.
#
# The stage tees the stream into a private FIFO. The dialog runs under the
# cleanup child wrapper, reads the FIFO to EOF, and has its stdout discarded;
# the stage exits with tee's status.
zxfer_handle_progress_bar_option() {
	l_progress_snapshot=$1
	g_zxfer_progress_bar_command_result=""

	l_progress_size=""
	if zxfer_progress_dialog_uses_size_estimate; then
		zxfer_calculate_size_estimate "$l_progress_snapshot" "${2:-}" || return
		l_progress_size=$g_zxfer_progress_size_estimate_result
	fi

	# Replace each %%size%% and %%title%% in template order. Parameter
	# expansion keeps any metacharacters in the values literal.
	l_progress_dialog=""
	l_progress_rest=$g_option_D_display_progress_bar
	while [ -n "$l_progress_rest" ]; do
		l_progress_size_head=${l_progress_rest%%"%%size%%"*}
		l_progress_title_head=${l_progress_rest%%"%%title%%"*}
		if [ "${#l_progress_size_head}" -lt "${#l_progress_title_head}" ]; then
			l_progress_dialog=$l_progress_dialog$l_progress_size_head$l_progress_size
			l_progress_rest=${l_progress_rest#"$l_progress_size_head%%size%%"}
		elif [ "$l_progress_title_head" != "$l_progress_rest" ]; then
			l_progress_dialog=$l_progress_dialog$l_progress_title_head$l_progress_snapshot
			l_progress_rest=${l_progress_rest#"$l_progress_title_head%%title%%"}
		else
			l_progress_dialog=$l_progress_dialog$l_progress_rest
			l_progress_rest=""
		fi
	done

	# One FIFO per send, so concurrent -j jobs never share one. The 0700
	# directory lives under the run root, whose removal cleans it up.
	if ! zxfer_create_private_temp_dir zxfer-progress ||
		! mkfifo -m 600 "$g_zxfer_runtime_artifact_path_result/fifo" ||
		[ ! -r "$ZXFER_CLEANUP_CHILD_WRAPPER" ]; then
		zxfer_throw_error "Failed to prepare the progress dialog FIFO for $l_progress_snapshot."
	fi
	zxfer_escape_single_quotes_into_result "$g_zxfer_runtime_artifact_path_result/fifo"
	l_progress_fifo="'$g_zxfer_escaped_single_quotes_result'"
	zxfer_render_shell_command_from_argv /bin/sh "$ZXFER_CLEANUP_CHILD_WRAPPER" "$l_progress_dialog"
	g_zxfer_progress_bar_command_result="| { $g_zxfer_shell_command_result <$l_progress_fifo >/dev/null & tee $l_progress_fifo; l_tee=\$?; wait \$!; exit \$l_tee; }"
}

# Purpose: Wrap one rendered zfs send or receive command for its -O or -T
# host, compressing across the ssh hop when COMPRESS is non-zero.
# Usage: zxfer_wrap_command_with_ssh CMD HOST_SPEC COMPRESS send|receive;
# publishes g_zxfer_wrapped_command_result. Throws on unsafe compression or a
# captured ssh diagnostic; otherwise returns the ssh builder's failure status.
zxfer_wrap_command_with_ssh() {
	l_wrap_cmd=$1
	l_wrap_host=$2
	l_wrap_compress=$3
	l_wrap_direction=$4
	g_zxfer_wrapped_command_result=""

	if [ "$l_wrap_compress" -ne 0 ]; then
		# The sending side compresses and the receiving side decompresses.
		# Pick the codec by direction and role, so a -T spec equal to the -O
		# spec still gets the target host's decompressor.
		if [ "$l_wrap_direction" = send ]; then
			l_wrap_remote_codec=${g_cmd_compress_safe:-}
			[ "$l_wrap_host" != "${g_option_O_origin_host:-}" ] ||
				l_wrap_remote_codec=${g_origin_cmd_compress_safe:-$l_wrap_remote_codec}
		else
			l_wrap_remote_codec=${g_cmd_decompress_safe:-}
			[ "$l_wrap_host" != "${g_option_T_target_host:-}" ] ||
				l_wrap_remote_codec=${g_target_cmd_decompress_safe:-$l_wrap_remote_codec}
		fi
		if [ -z "${g_cmd_compress_safe:-}" ] || [ -z "${g_cmd_decompress_safe:-}" ] ||
			[ -z "$l_wrap_remote_codec" ]; then
			zxfer_throw_error "Compression enabled but commands are not configured safely."
		fi
		if [ "$l_wrap_direction" = send ]; then
			l_wrap_cmd="$l_wrap_cmd | $l_wrap_remote_codec"
		else
			l_wrap_cmd="$l_wrap_remote_codec | $l_wrap_cmd"
		fi
	fi

	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$l_wrap_host" "$l_wrap_cmd" ||
		return
	g_zxfer_wrapped_command_result=$g_zxfer_prepared_ssh_shell_command_result
	if [ "$l_wrap_compress" -eq 0 ]; then
		return 0
	elif [ "$l_wrap_direction" = send ]; then
		g_zxfer_wrapped_command_result="$g_zxfer_wrapped_command_result | $g_cmd_decompress_safe"
	else
		g_zxfer_wrapped_command_result="$g_cmd_compress_safe | $g_zxfer_wrapped_command_result"
	fi
}

# Purpose: Render one `zfs send | zfs receive` pipeline and hand it to the
# send-job scheduler.
# Usage: zxfer_zfs_send_receive PREVIOUS_SNAPSHOT CURRENT_SNAPSHOT DEST
# [ALLOW_BACKGROUND] [FORCE_FLAG]; an empty PREVIOUS sends the full stream,
# otherwise one -I stream covers the range. ALLOW_BACKGROUND defaults to 1; a
# FORCE_FLAG argument, even an empty one, replaces the -F option. Throws when
# the pipeline cannot be prepared or run.
zxfer_zfs_send_receive() {
	zxfer_set_failure_stage "send/receive"
	zxfer_echoV "Begin zxfer_zfs_send_receive()"
	l_send_previous=$1
	l_send_current=$2
	l_send_dest=$3
	l_send_allow_background=${4:-1}
	if [ $# -ge 5 ]; then
		l_send_force=$5
	else
		l_send_force=${g_option_F_force_rollback:-}
	fi

	if [ -n "$g_option_O_origin_host" ]; then
		set -- "${g_origin_cmd_zfs:-$g_cmd_zfs}" send
	else
		set -- "$g_cmd_zfs" send
	fi
	[ "$g_option_V_very_verbose" -ne 1 ] || set -- "$@" -v
	[ "$g_option_w_raw_send" -ne 1 ] || set -- "$@" -w
	[ -z "$l_send_previous" ] || set -- "$@" -I "$l_send_previous"
	zxfer_render_shell_command_from_argv "$@" "$l_send_current"
	l_send_cmd=$g_zxfer_shell_command_result

	if [ -n "$g_option_T_target_host" ]; then
		set -- "${g_target_cmd_zfs:-$g_cmd_zfs}" receive
	else
		set -- "$g_cmd_zfs" receive
	fi
	[ -z "$l_send_force" ] || set -- "$@" "$l_send_force"
	zxfer_render_shell_command_from_argv "$@" "$l_send_dest"
	l_recv_cmd=$g_zxfer_shell_command_result
	[ -z "$l_send_force" ] ||
		zxfer_echov "Receive-side force flag (-F) is active for destination [$l_send_dest]."

	# Callers ignore this function's status, so a helper that fails without
	# throwing (the ssh builder can) must still stop the run here.
	if [ -n "$g_option_O_origin_host" ]; then
		zxfer_wrap_command_with_ssh "$l_send_cmd" "$g_option_O_origin_host" \
			"$g_option_z_compress" send ||
			zxfer_throw_error "Failed to prepare the ssh send command for [$l_send_current]." "$?"
		l_send_cmd=$g_zxfer_wrapped_command_result
	fi
	if [ -n "$g_option_T_target_host" ]; then
		zxfer_wrap_command_with_ssh "$l_recv_cmd" "$g_option_T_target_host" \
			"$g_option_z_compress" receive ||
			zxfer_throw_error "Failed to prepare the ssh receive command for [$l_send_dest]." "$?"
		l_recv_cmd=$g_zxfer_wrapped_command_result
	fi
	# The progress stage sees the stream after the origin ssh hop.
	if [ -n "$g_option_D_display_progress_bar" ]; then
		zxfer_handle_progress_bar_option "$l_send_current" "$l_send_previous" ||
			zxfer_throw_error "Failed to prepare the progress dialog for [$l_send_current]." "$?"
		l_send_cmd="$l_send_cmd $g_zxfer_progress_bar_command_result"
	fi

	# -V counts the pipeline as one zfs send on the source and one receive on
	# the destination, each over ssh when its side is remote. Without -V the
	# one test skips all eight counts.
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		# Startup latency is the time to the first live transfer.
		if [ "${g_zxfer_profile_startup_latency_recorded:-0}" -eq 0 ]; then
			zxfer_profile_stop_timer "${g_zxfer_profile_start_ms:-}"
			g_zxfer_profile_startup_latency_ms=$((g_zxfer_profile_startup_latency_ms + g_zxfer_profile_elapsed_ms))
			g_zxfer_profile_startup_latency_recorded=1
		fi
		g_zxfer_profile_send_receive_pipeline_commands=$((g_zxfer_profile_send_receive_pipeline_commands + 1))
		g_zxfer_profile_bucket_send_receive_setup=$((g_zxfer_profile_bucket_send_receive_setup + 1))
		g_zxfer_profile_source_zfs_calls=$((g_zxfer_profile_source_zfs_calls + 1))
		g_zxfer_profile_destination_zfs_calls=$((g_zxfer_profile_destination_zfs_calls + 1))
		g_zxfer_profile_zfs_send_calls=$((g_zxfer_profile_zfs_send_calls + 1))
		g_zxfer_profile_zfs_receive_calls=$((g_zxfer_profile_zfs_receive_calls + 1))
		[ -z "$g_option_O_origin_host" ] ||
			g_zxfer_profile_source_ssh_shell_invocations=$((g_zxfer_profile_source_ssh_shell_invocations + 1))
		[ -z "$g_option_T_target_host" ] ||
			g_zxfer_profile_destination_ssh_shell_invocations=$((g_zxfer_profile_destination_ssh_shell_invocations + 1))
	fi

	zxfer_schedule_send_receive_pipeline "$l_send_cmd | $l_recv_cmd" \
		"$l_send_current" "$l_send_dest" "$l_send_allow_background" || return
	zxfer_echoV "End zxfer_zfs_send_receive()"
}
