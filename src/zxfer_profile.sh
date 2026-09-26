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
# PROFILING (-V end-of-run counters)
################################################################################

# Module contract:
# owns globals: g_zxfer_profile_* counters, timings, clock scratch, and
#   summary state, including the command-render count that the renderers in
#   zxfer_quoting.sh bump.
# reads globals: g_option_V_very_verbose, g_option_O_origin_host,
#   g_option_T_target_host, g_zxfer_failure_stage (bucket attribution), and
#   g_zxfer_profile_prescan (set by the launcher).
# mutates caches: profile counters only. Recorders always return 0 so a
#   recorder used as a caller's last statement never changes its status.
# returns via stdout: epoch milliseconds (zxfer_profile_now_ms).
#
# Every "zxfer profile: key=value" line of the -V summary is a stable key.
# Keys whose producers were removed print a literal 0.

# Purpose: Read the clock with one date fork into g_zxfer_profile_clock_ms as
# epoch milliseconds; where date lacks %N (BSD, illumos) the value is
# seconds * 1000.
# Usage: zxfer_profile_read_clock_ms && use "$g_zxfer_profile_clock_ms";
# returns 1, with the result cleared, when date is unusable.
zxfer_profile_read_clock_ms() {
	g_zxfer_profile_clock_ms=""
	l_profile_clock=$(date '+%s %s%3N' 2>/dev/null) || return 1
	case $l_profile_clock in
	*' '*) ;;
	*) return 1 ;;
	esac
	l_profile_clock_s=${l_profile_clock%% *}
	l_profile_clock_ms=${l_profile_clock#* }
	zxfer_is_uint "$l_profile_clock_s" || return 1
	zxfer_is_uint "$l_profile_clock_ms" ||
		l_profile_clock_ms=$((l_profile_clock_s * 1000))
	g_zxfer_profile_clock_ms=$l_profile_clock_ms
}

# Purpose: Print the current epoch time in milliseconds, or seconds * 1000
# where date lacks %N.
# Usage: l_start_ms=$(zxfer_profile_now_ms) before timing a -V stage; returns
# 1 when date is unusable.
zxfer_profile_now_ms() {
	zxfer_profile_read_clock_ms || return 1
	printf '%s\n' "$g_zxfer_profile_clock_ms"
}

# Purpose: Zero every profile counter and, when the launcher saw -V, record
# the session start time.
# Usage: Called once by zxfer_reset_session_state, before options are parsed.
# Other entry points leave g_zxfer_profile_prescan unset, which counts as -V.
zxfer_reset_profile_state() {
	g_zxfer_profile_start_ms=""
	if [ "${g_zxfer_profile_prescan:-1}" = 1 ] && zxfer_profile_read_clock_ms; then
		g_zxfer_profile_start_ms=$g_zxfer_profile_clock_ms
	fi
	g_zxfer_profile_has_data=0
	g_zxfer_profile_summary_emitted=0
	g_zxfer_profile_startup_latency_recorded=0
	g_zxfer_profile_startup_latency_ms=0
	g_zxfer_profile_cleanup_ms=0
	g_zxfer_profile_ssh_setup_ms=0
	g_zxfer_profile_source_snapshot_listing_ms=0
	g_zxfer_profile_destination_snapshot_listing_ms=0
	g_zxfer_profile_snapshot_diff_sort_ms=0
	g_zxfer_profile_remote_capability_bootstrap_live=0
	g_zxfer_profile_remote_capability_bootstrap_memory=0
	g_zxfer_profile_remote_cli_tool_direct_probes=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0
	g_zxfer_profile_source_snapshot_list_commands=0
	g_zxfer_profile_source_snapshot_list_parallel_commands=0
	g_zxfer_profile_send_receive_pipeline_commands=0
	g_zxfer_profile_send_receive_background_pipeline_commands=0
	g_zxfer_profile_exists_destination_calls=0
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_profile_parent_destination_property_reads=0
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_zxfer_profile_runtime_artifact_files_created=0
	g_zxfer_profile_runtime_artifact_dirs_created=0
	g_zxfer_profile_runtime_artifact_paths_cleaned=0
	g_zxfer_profile_command_render_calls=0
	g_zxfer_profile_live_destination_snapshot_rechecks=0
	g_zxfer_profile_diverged_snapshot_warnings=0
}

# Purpose: Report whether -V profiling is active.
# Usage: zxfer_profile_metrics_enabled || return 0
zxfer_profile_metrics_enabled() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ]
}

# Purpose: Add AMOUNT (default 1) to one profile counter under -V.
# Usage: zxfer_profile_increment_counter g_zxfer_profile_NAME [AMOUNT]; a
# non-numeric AMOUNT counts as 1 and unknown names are ignored.
zxfer_profile_increment_counter() {
	zxfer_profile_metrics_enabled || return 0
	l_profile_amount=${2:-1}
	zxfer_is_uint "$l_profile_amount" || l_profile_amount=1

	# A table keeps counter names away from eval. ${g##*[!0-9]*} is the
	# value when it is all digits and empty otherwise, so a non-numeric value
	# counts as 0 and is never evaluated as an arithmetic expression.
	case ${1:-} in
	g_zxfer_profile_startup_latency_ms) g_zxfer_profile_startup_latency_ms=$((${g_zxfer_profile_startup_latency_ms##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_cleanup_ms) g_zxfer_profile_cleanup_ms=$((${g_zxfer_profile_cleanup_ms##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_ssh_setup_ms) g_zxfer_profile_ssh_setup_ms=$((${g_zxfer_profile_ssh_setup_ms##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_source_snapshot_listing_ms) g_zxfer_profile_source_snapshot_listing_ms=$((${g_zxfer_profile_source_snapshot_listing_ms##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_destination_snapshot_listing_ms) g_zxfer_profile_destination_snapshot_listing_ms=$((${g_zxfer_profile_destination_snapshot_listing_ms##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_snapshot_diff_sort_ms) g_zxfer_profile_snapshot_diff_sort_ms=$((${g_zxfer_profile_snapshot_diff_sort_ms##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_remote_capability_bootstrap_live) g_zxfer_profile_remote_capability_bootstrap_live=$((${g_zxfer_profile_remote_capability_bootstrap_live##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_remote_capability_bootstrap_memory) g_zxfer_profile_remote_capability_bootstrap_memory=$((${g_zxfer_profile_remote_capability_bootstrap_memory##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_remote_cli_tool_direct_probes) g_zxfer_profile_remote_cli_tool_direct_probes=$((${g_zxfer_profile_remote_cli_tool_direct_probes##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_source_zfs_calls) g_zxfer_profile_source_zfs_calls=$((${g_zxfer_profile_source_zfs_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_destination_zfs_calls) g_zxfer_profile_destination_zfs_calls=$((${g_zxfer_profile_destination_zfs_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_other_zfs_calls) g_zxfer_profile_other_zfs_calls=$((${g_zxfer_profile_other_zfs_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_zfs_list_calls) g_zxfer_profile_zfs_list_calls=$((${g_zxfer_profile_zfs_list_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_zfs_get_calls) g_zxfer_profile_zfs_get_calls=$((${g_zxfer_profile_zfs_get_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_zfs_send_calls) g_zxfer_profile_zfs_send_calls=$((${g_zxfer_profile_zfs_send_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_zfs_receive_calls) g_zxfer_profile_zfs_receive_calls=$((${g_zxfer_profile_zfs_receive_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_ssh_shell_invocations) g_zxfer_profile_ssh_shell_invocations=$((${g_zxfer_profile_ssh_shell_invocations##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_source_ssh_shell_invocations) g_zxfer_profile_source_ssh_shell_invocations=$((${g_zxfer_profile_source_ssh_shell_invocations##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_destination_ssh_shell_invocations) g_zxfer_profile_destination_ssh_shell_invocations=$((${g_zxfer_profile_destination_ssh_shell_invocations##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_other_ssh_shell_invocations) g_zxfer_profile_other_ssh_shell_invocations=$((${g_zxfer_profile_other_ssh_shell_invocations##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_source_snapshot_list_commands) g_zxfer_profile_source_snapshot_list_commands=$((${g_zxfer_profile_source_snapshot_list_commands##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_source_snapshot_list_parallel_commands) g_zxfer_profile_source_snapshot_list_parallel_commands=$((${g_zxfer_profile_source_snapshot_list_parallel_commands##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_send_receive_pipeline_commands) g_zxfer_profile_send_receive_pipeline_commands=$((${g_zxfer_profile_send_receive_pipeline_commands##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_send_receive_background_pipeline_commands) g_zxfer_profile_send_receive_background_pipeline_commands=$((${g_zxfer_profile_send_receive_background_pipeline_commands##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_exists_destination_calls) g_zxfer_profile_exists_destination_calls=$((${g_zxfer_profile_exists_destination_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_normalized_property_reads_source) g_zxfer_profile_normalized_property_reads_source=$((${g_zxfer_profile_normalized_property_reads_source##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_normalized_property_reads_destination) g_zxfer_profile_normalized_property_reads_destination=$((${g_zxfer_profile_normalized_property_reads_destination##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_normalized_property_reads_other) g_zxfer_profile_normalized_property_reads_other=$((${g_zxfer_profile_normalized_property_reads_other##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_required_property_backfill_gets) g_zxfer_profile_required_property_backfill_gets=$((${g_zxfer_profile_required_property_backfill_gets##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_parent_destination_property_reads) g_zxfer_profile_parent_destination_property_reads=$((${g_zxfer_profile_parent_destination_property_reads##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_bucket_source_inspection) g_zxfer_profile_bucket_source_inspection=$((${g_zxfer_profile_bucket_source_inspection##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_bucket_destination_inspection) g_zxfer_profile_bucket_destination_inspection=$((${g_zxfer_profile_bucket_destination_inspection##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_bucket_property_reconciliation) g_zxfer_profile_bucket_property_reconciliation=$((${g_zxfer_profile_bucket_property_reconciliation##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_bucket_send_receive_setup) g_zxfer_profile_bucket_send_receive_setup=$((${g_zxfer_profile_bucket_send_receive_setup##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_runtime_artifact_files_created) g_zxfer_profile_runtime_artifact_files_created=$((${g_zxfer_profile_runtime_artifact_files_created##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_runtime_artifact_dirs_created) g_zxfer_profile_runtime_artifact_dirs_created=$((${g_zxfer_profile_runtime_artifact_dirs_created##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_runtime_artifact_paths_cleaned) g_zxfer_profile_runtime_artifact_paths_cleaned=$((${g_zxfer_profile_runtime_artifact_paths_cleaned##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_command_render_calls) g_zxfer_profile_command_render_calls=$((${g_zxfer_profile_command_render_calls##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_live_destination_snapshot_rechecks) g_zxfer_profile_live_destination_snapshot_rechecks=$((${g_zxfer_profile_live_destination_snapshot_rechecks##*[!0-9]*} + l_profile_amount)) ;;
	g_zxfer_profile_diverged_snapshot_warnings) g_zxfer_profile_diverged_snapshot_warnings=$((${g_zxfer_profile_diverged_snapshot_warnings##*[!0-9]*} + l_profile_amount)) ;;
	*) return 0 ;;
	esac
	g_zxfer_profile_has_data=1
}

# Purpose: Add the milliseconds elapsed since START_MS to a timing counter
# under -V.
# Usage: zxfer_profile_add_elapsed_ms g_zxfer_profile_NAME_ms START_MS
# [END_MS]; END_MS defaults to zxfer_profile_now_ms. Non-numeric or backwards
# readings are ignored.
zxfer_profile_add_elapsed_ms() {
	zxfer_profile_metrics_enabled || return 0
	l_profile_start_ms=$2
	l_profile_end_ms=${3:-}

	zxfer_is_uint "$l_profile_start_ms" || return 0
	if [ -z "$l_profile_end_ms" ]; then
		l_profile_end_ms=$(zxfer_profile_now_ms) || return 0
	fi
	zxfer_is_uint "$l_profile_end_ms" || return 0
	[ "$l_profile_end_ms" -ge "$l_profile_start_ms" ] || return 0
	zxfer_profile_increment_counter "$1" "$((l_profile_end_ms - l_profile_start_ms))"
}

# Purpose: Count one zfs invocation by side, by verb, and in the bucket of the
# active failure stage.
# Usage: zxfer_profile_record_zfs_call source|destination|other VERB; verbs
# other than list, get, send, and receive count only toward the side.
zxfer_profile_record_zfs_call() {
	zxfer_profile_metrics_enabled || return 0
	l_profile_zfs_side=$1
	l_profile_zfs_verb=$2

	case $l_profile_zfs_side in
	source | destination) ;;
	*) l_profile_zfs_side=other ;;
	esac
	zxfer_profile_increment_counter "g_zxfer_profile_${l_profile_zfs_side}_zfs_calls"
	zxfer_profile_increment_counter "g_zxfer_profile_zfs_${l_profile_zfs_verb}_calls"

	# Property transfer owns every call in its stage; send/receive setup owns
	# the send and receive verbs; inspection owns list and get everywhere else
	# and every call during snapshot discovery. Other-side calls have no
	# inspection bucket, so that counter name is ignored.
	case ${g_zxfer_failure_stage:-}:$l_profile_zfs_verb in
	"property transfer":*)
		zxfer_profile_increment_counter g_zxfer_profile_bucket_property_reconciliation
		;;
	"send/receive":send | "send/receive":receive)
		zxfer_profile_increment_counter g_zxfer_profile_bucket_send_receive_setup
		;;
	"snapshot discovery":* | *:list | *:get)
		zxfer_profile_increment_counter "g_zxfer_profile_bucket_${l_profile_zfs_side}_inspection"
		;;
	esac
	return 0
}

# Purpose: Count one ssh shell invocation, attributed to a side.
# Usage: zxfer_profile_record_ssh_invocation HOST_SPEC [source|destination|
# other]; without a side, HOST_SPEC is matched against -O and -T.
zxfer_profile_record_ssh_invocation() {
	zxfer_profile_metrics_enabled || return 0
	case ${2:-} in
	source | destination | other)
		l_profile_ssh_side=$2
		;;
	*)
		l_profile_ssh_side=other
		if [ -n "${g_option_O_origin_host:-}" ] &&
			[ "$1" = "$g_option_O_origin_host" ]; then
			l_profile_ssh_side=source
		elif [ -n "${g_option_T_target_host:-}" ] &&
			[ "$1" = "$g_option_T_target_host" ]; then
			l_profile_ssh_side=destination
		fi
		;;
	esac
	zxfer_profile_increment_counter g_zxfer_profile_ssh_shell_invocations
	zxfer_profile_increment_counter "g_zxfer_profile_${l_profile_ssh_side}_ssh_shell_invocations"
}

# Purpose: Print the -V profile summary to stderr once per run.
# Usage: Called by the EXIT trap; silent unless -V recorded any data.
zxfer_profile_emit_summary() {
	zxfer_profile_metrics_enabled || return 0
	[ "${g_zxfer_profile_has_data:-0}" -eq 1 ] || return 0
	[ "${g_zxfer_profile_summary_emitted:-0}" -eq 0 ] || return 0
	g_zxfer_profile_summary_emitted=1

	# Whole seconds between the two clock readings, as with date +%s.
	l_profile_elapsed=unknown
	if zxfer_is_uint "${g_zxfer_profile_start_ms:-}" && zxfer_profile_read_clock_ms; then
		l_profile_elapsed=$((g_zxfer_profile_clock_ms / 1000 - g_zxfer_profile_start_ms / 1000))
	fi

	printf 'zxfer profile: %s\n' \
		"elapsed_seconds=$l_profile_elapsed" \
		"startup_latency_ms=${g_zxfer_profile_startup_latency_ms:-0}" \
		"cleanup_ms=${g_zxfer_profile_cleanup_ms:-0}" \
		"ssh_setup_ms=${g_zxfer_profile_ssh_setup_ms:-0}" \
		"source_snapshot_listing_ms=${g_zxfer_profile_source_snapshot_listing_ms:-0}" \
		"destination_snapshot_listing_ms=${g_zxfer_profile_destination_snapshot_listing_ms:-0}" \
		"snapshot_diff_sort_ms=${g_zxfer_profile_snapshot_diff_sort_ms:-0}" \
		"ssh_control_socket_lock_wait_count=0" \
		"ssh_control_socket_lock_wait_ms=0" \
		"remote_capability_cache_wait_count=0" \
		"remote_capability_cache_wait_ms=0" \
		"remote_capability_bootstrap_live=${g_zxfer_profile_remote_capability_bootstrap_live:-0}" \
		"remote_capability_bootstrap_cache=0" \
		"remote_capability_bootstrap_memory=${g_zxfer_profile_remote_capability_bootstrap_memory:-0}" \
		"remote_cli_tool_direct_probes=${g_zxfer_profile_remote_cli_tool_direct_probes:-0}" \
		"source_zfs_calls=${g_zxfer_profile_source_zfs_calls:-0}" \
		"destination_zfs_calls=${g_zxfer_profile_destination_zfs_calls:-0}" \
		"other_zfs_calls=${g_zxfer_profile_other_zfs_calls:-0}" \
		"zfs_list_calls=${g_zxfer_profile_zfs_list_calls:-0}" \
		"zfs_get_calls=${g_zxfer_profile_zfs_get_calls:-0}" \
		"zfs_send_calls=${g_zxfer_profile_zfs_send_calls:-0}" \
		"zfs_receive_calls=${g_zxfer_profile_zfs_receive_calls:-0}" \
		"ssh_shell_invocations=${g_zxfer_profile_ssh_shell_invocations:-0}" \
		"source_ssh_shell_invocations=${g_zxfer_profile_source_ssh_shell_invocations:-0}" \
		"destination_ssh_shell_invocations=${g_zxfer_profile_destination_ssh_shell_invocations:-0}" \
		"other_ssh_shell_invocations=${g_zxfer_profile_other_ssh_shell_invocations:-0}" \
		"source_snapshot_list_commands=${g_zxfer_profile_source_snapshot_list_commands:-0}" \
		"source_snapshot_list_parallel_commands=${g_zxfer_profile_source_snapshot_list_parallel_commands:-0}" \
		"send_receive_pipeline_commands=${g_zxfer_profile_send_receive_pipeline_commands:-0}" \
		"send_receive_background_pipeline_commands=${g_zxfer_profile_send_receive_background_pipeline_commands:-0}" \
		"exists_destination_calls=${g_zxfer_profile_exists_destination_calls:-0}" \
		"normalized_property_reads_source=${g_zxfer_profile_normalized_property_reads_source:-0}" \
		"normalized_property_reads_destination=${g_zxfer_profile_normalized_property_reads_destination:-0}" \
		"normalized_property_reads_other=${g_zxfer_profile_normalized_property_reads_other:-0}" \
		"required_property_backfill_gets=${g_zxfer_profile_required_property_backfill_gets:-0}" \
		"parent_destination_property_reads=${g_zxfer_profile_parent_destination_property_reads:-0}" \
		"bucket_source_inspection=${g_zxfer_profile_bucket_source_inspection:-0}" \
		"bucket_destination_inspection=${g_zxfer_profile_bucket_destination_inspection:-0}" \
		"bucket_property_reconciliation=${g_zxfer_profile_bucket_property_reconciliation:-0}" \
		"bucket_send_receive_setup=${g_zxfer_profile_bucket_send_receive_setup:-0}" \
		"runtime_artifact_files_created=${g_zxfer_profile_runtime_artifact_files_created:-0}" \
		"runtime_artifact_dirs_created=${g_zxfer_profile_runtime_artifact_dirs_created:-0}" \
		"runtime_artifact_paths_cleaned=${g_zxfer_profile_runtime_artifact_paths_cleaned:-0}" \
		"runtime_cache_object_writes=0" \
		"runtime_cache_object_readbacks=0" \
		"command_render_calls=${g_zxfer_profile_command_render_calls:-0}" \
		"live_destination_snapshot_rechecks=${g_zxfer_profile_live_destination_snapshot_rechecks:-0}" \
		"diverged_snapshot_warnings=${g_zxfer_profile_diverged_snapshot_warnings:-0}" \
		>&2
}
