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
# SEND / RECEIVE: PIPELINE PREPARATION AND JOB SUPERVISION
################################################################################

# Module contract:
# owns globals: the progress estimate and progress command results; the
# send-job registry g_zxfer_send_jobs (job_id, pid, dest, status_file, snapshot,
# scope), g_count_zfs_send_jobs, job sequence,
# polling/abort knobs, conflict destination and abort diagnostic.
# Wrapped transfers publish quoting's g_zxfer_shell_command_result directly.
# reads globals: send options (-D, -F, -j, -O, -T, -V, -w, -z), local and
# endpoint zfs paths, safe codecs, and background-shell spawn helpers.
# mutates caches: destination snapshot/property state after successful
# receives, through zxfer_finish_destination_receive. Sets replication's
# per-pass g_is_performed_send_destroy marker when a send is scheduled;
# replication also sets it after a -d destroy.
# returns via stdout: none.
#
# Commands are rendered in this shell as plain shell text. The same pipeline
# runs under eval (-j 1) or in a -j job shell with no zxfer functions, and is
# what -v displays.
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

# Purpose: Reset the send/receive jobs and result globals for a new session.
# Usage: zxfer_reset_send_receive_state; assignments only.
zxfer_reset_send_receive_state() {
	g_count_zfs_send_jobs=0
	g_zxfer_send_jobs=""
	g_zxfer_send_job_sequence=0
	g_zxfer_send_job_conflict_dest_dataset=""
	g_zxfer_send_job_abort_grace_seconds=1
	g_zxfer_send_job_poll_seconds=""
	g_zxfer_send_job_abort_failure_message=""
	g_zxfer_progress_size_estimate_result=""
	g_zxfer_progress_bar_command_result=""
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
# publishes g_zxfer_shell_command_result. Throws on unsafe compression or a
# captured ssh diagnostic; otherwise returns the ssh builder's failure status.
# Retain the result before preparing another command; failures leave it empty.
zxfer_wrap_command_with_ssh() {
	l_wrap_cmd=$1
	l_wrap_host=$2
	l_wrap_compress=$3
	l_wrap_direction=$4
	g_zxfer_shell_command_result=""

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
	if [ "$l_wrap_compress" -eq 0 ]; then
		return 0
	elif [ "$l_wrap_direction" = send ]; then
		g_zxfer_shell_command_result="$g_zxfer_shell_command_result | $g_cmd_decompress_safe"
	else
		g_zxfer_shell_command_result="$g_cmd_compress_safe | $g_zxfer_shell_command_result"
	fi
}

################################################################################
# PIPELINE CONSTRUCTION AND SCHEDULING
################################################################################

# Purpose: Prepare and dispatch one `zfs send | zfs receive` pipeline,
# finishing foreground receives here and supervised receives when reaped.
# Usage: zxfer_zfs_send_receive PREVIOUS_SNAPSHOT CURRENT_SNAPSHOT DEST
# [ALLOW_BACKGROUND] [FORCE_FLAG]; an empty PREVIOUS sends the full stream,
# otherwise one -I stream covers the range. ALLOW_BACKGROUND defaults to 1; a
# FORCE_FLAG argument, even an empty one, replaces the -F option. Throws when
# the pipeline cannot be prepared or run. Background dispatch waits for both
# the job limit and destination ancestry, then marks the pass for -Y.
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
		l_send_cmd=$g_zxfer_shell_command_result
	fi
	if [ -n "$g_option_T_target_host" ]; then
		zxfer_wrap_command_with_ssh "$l_recv_cmd" "$g_option_T_target_host" \
			"$g_option_z_compress" receive ||
			zxfer_throw_error "Failed to prepare the ssh receive command for [$l_send_dest]." "$?"
		l_recv_cmd=$g_zxfer_shell_command_result
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

	l_send_pipeline_cmd="$l_send_cmd | $l_recv_cmd"
	if [ "$l_send_allow_background" -ne 1 ] || [ "$g_option_j_jobs" -le 1 ]; then
		zxfer_execute_rendered_shell_command "$l_send_pipeline_cmd" || return
		zxfer_finish_destination_receive "$l_send_dest"
	else
		while :; do
			if [ "${g_count_zfs_send_jobs:-0}" -ge "$g_option_j_jobs" ]; then
				zxfer_echov "Max jobs reached [$g_count_zfs_send_jobs]. Waiting for jobs to complete."
				zxfer_wait_for_any_send_job "job limit" || return
			elif zxfer_send_job_conflicts_with_destination "$l_send_dest"; then
				zxfer_echov "Waiting for conflicting zfs send/receive ancestry to finish for destination [$l_send_dest]; active destination [$g_zxfer_send_job_conflict_dest_dataset] is still running."
				zxfer_wait_for_any_send_job "destination ancestry" || return
			else
				break
			fi
		done
		g_zxfer_profile_send_receive_background_pipeline_commands=$((g_zxfer_profile_send_receive_background_pipeline_commands + 1))
		zxfer_record_last_command_string "$l_send_pipeline_cmd"
		zxfer_echov "$l_send_pipeline_cmd"
		zxfer_spawn_send_job "$l_send_pipeline_cmd" \
			"$l_send_current" "$l_send_dest" || return
	fi
	g_is_performed_send_destroy=1
	zxfer_echoV "End zxfer_zfs_send_receive()"
}

################################################################################
# JOB SCHEDULING
################################################################################

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
	l_spawn_send_job_status_file=$g_zxfer_runtime_artifact_path_result
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

################################################################################
# JOB COMPLETION
################################################################################

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

################################################################################
# JOB TEARDOWN
################################################################################

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
