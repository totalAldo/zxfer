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
# PROFILING
################################################################################

# Module contract:
# owns globals: g_zxfer_profile_* counters, timings, and summary emission state.
# reads globals: g_option_* verbosity/host roles and g_zxfer_failure_stage.
# mutates caches: profile counters, elapsed timings, and summary emission state.
# returns via stdout: millisecond timestamps.

# Purpose: Reset all profile timing and counter state for a new session.
# Usage: Called by the session composition root after runtime artifact state is
# reset so the new run starts with fresh timing and emission state.
# Side effects: Reinitializes every g_zxfer_profile_* value owned here.
zxfer_reset_profile_state() {
	g_zxfer_profile_start_epoch=$(date '+%s' 2>/dev/null || :)
	if ! g_zxfer_profile_start_ms=$(zxfer_profile_now_ms 2>/dev/null); then
		g_zxfer_profile_start_ms=""
	fi
	g_zxfer_profile_has_data=0
	g_zxfer_profile_summary_emitted=0
	g_zxfer_profile_startup_latency_ms=0
	g_zxfer_profile_startup_latency_recorded=0
	g_zxfer_profile_cleanup_ms=0
	g_zxfer_profile_ssh_setup_ms=0
	g_zxfer_profile_source_snapshot_listing_ms=0
	g_zxfer_profile_destination_snapshot_listing_ms=0
	g_zxfer_profile_snapshot_diff_sort_ms=0
	g_zxfer_profile_ssh_control_socket_lock_wait_count=0
	g_zxfer_profile_ssh_control_socket_lock_wait_ms=0
	g_zxfer_profile_remote_capability_cache_wait_count=0
	g_zxfer_profile_remote_capability_cache_wait_ms=0
	g_zxfer_profile_remote_capability_bootstrap_live=0
	g_zxfer_profile_remote_capability_bootstrap_cache=0
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
	g_zxfer_profile_runtime_cache_object_writes=0
	g_zxfer_profile_runtime_cache_object_readbacks=0
	g_zxfer_profile_live_destination_snapshot_rechecks=0
	g_zxfer_profile_diverged_snapshot_warnings=0
}

# Purpose: Record or emit the metrics enabled for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_metrics_enabled() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ]
}

# Purpose: Record or emit the increment counter for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_increment_counter() {
	l_counter_name=$1
	l_increment_by=${2:-1}

	zxfer_profile_metrics_enabled || return 0

	case "$l_counter_name" in
	g_zxfer_profile_?*)
		zxfer_shell_variable_name_is_valid "$l_counter_name" || return 0
		;;
	*)
		return 0
		;;
	esac

	g_zxfer_profile_has_data=1

	case "$l_increment_by" in
	'' | *[!0-9]*)
		l_increment_by=1
		;;
	esac

	eval "l_counter_value=\${$l_counter_name:-0}"
	case "$l_counter_value" in
	'' | *[!0-9]*)
		l_counter_value=0
		;;
	esac

	l_counter_value=$((l_counter_value + l_increment_by))
	eval "$l_counter_name=\$l_counter_value"
}

# Purpose: Record or emit the now ms for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_now_ms() {
	l_now_ms=$(date '+%s%3N' 2>/dev/null || :)
	case "$l_now_ms" in
	'' | *[!0-9]*)
		l_now_epoch=$(date '+%s' 2>/dev/null || :)
		case "$l_now_epoch" in
		'' | *[!0-9]*)
			return 1
			;;
		esac
		l_now_ms=$((l_now_epoch * 1000))
		;;
	esac

	printf '%s\n' "$l_now_ms"
}

# Purpose: Record or emit the add elapsed ms for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_add_elapsed_ms() {
	l_counter_name=$1
	l_start_ms=$2
	l_end_ms=${3:-}

	zxfer_profile_metrics_enabled || return 0

	case "$l_counter_name" in
	g_zxfer_profile_?*)
		zxfer_shell_variable_name_is_valid "$l_counter_name" || return 0
		;;
	*)
		return 0
		;;
	esac

	case "$l_start_ms" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	if [ -z "$l_end_ms" ]; then
		l_end_ms=$(zxfer_profile_now_ms) || return 0
	fi

	case "$l_end_ms" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	[ "$l_end_ms" -ge "$l_start_ms" ] || return 0

	g_zxfer_profile_has_data=1

	eval "l_counter_value=\${$l_counter_name:-0}"
	case "$l_counter_value" in
	'' | *[!0-9]*)
		l_counter_value=0
		;;
	esac

	l_elapsed_ms=$((l_end_ms - l_start_ms))
	l_counter_value=$((l_counter_value + l_elapsed_ms))
	eval "$l_counter_name=\$l_counter_value"
}

# Purpose: Record warm startup latency at the first live transfer only.
# Usage: Called by send/receive metrics; repeated calls preserve the first
# transfer boundary and do not rewrite owner state.
zxfer_profile_record_startup_latency_once() {
	if [ "${g_zxfer_profile_startup_latency_recorded:-0}" -eq 0 ]; then
		zxfer_profile_add_elapsed_ms g_zxfer_profile_startup_latency_ms \
			"${g_zxfer_profile_start_ms:-}"
		g_zxfer_profile_startup_latency_recorded=1
	fi
}

# Purpose: Record or emit the record bucket for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_record_bucket() {
	l_bucket=$1

	case "$l_bucket" in
	source_inspection)
		zxfer_profile_increment_counter g_zxfer_profile_bucket_source_inspection
		;;
	destination_inspection)
		zxfer_profile_increment_counter g_zxfer_profile_bucket_destination_inspection
		;;
	property_reconciliation)
		zxfer_profile_increment_counter g_zxfer_profile_bucket_property_reconciliation
		;;
	send_receive_setup)
		zxfer_profile_increment_counter g_zxfer_profile_bucket_send_receive_setup
		;;
	esac

	return 0
}

# Purpose: Record or emit the record ZFS call for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_record_zfs_call() {
	l_side=$1
	l_verb=$2

	zxfer_profile_metrics_enabled || return 0

	case "$l_side" in
	source)
		zxfer_profile_increment_counter g_zxfer_profile_source_zfs_calls
		;;
	destination)
		zxfer_profile_increment_counter g_zxfer_profile_destination_zfs_calls
		;;
	*)
		zxfer_profile_increment_counter g_zxfer_profile_other_zfs_calls
		;;
	esac

	case "$l_verb" in
	list)
		zxfer_profile_increment_counter g_zxfer_profile_zfs_list_calls
		;;
	get)
		zxfer_profile_increment_counter g_zxfer_profile_zfs_get_calls
		;;
	send)
		zxfer_profile_increment_counter g_zxfer_profile_zfs_send_calls
		;;
	receive)
		zxfer_profile_increment_counter g_zxfer_profile_zfs_receive_calls
		;;
	esac

	case "${g_zxfer_failure_stage:-}" in
	"property transfer")
		zxfer_profile_record_bucket property_reconciliation
		;;
	"send/receive")
		case "$l_verb" in
		send | receive)
			zxfer_profile_record_bucket send_receive_setup
			;;
		list | get)
			if [ "$l_side" = "destination" ]; then
				zxfer_profile_record_bucket destination_inspection
			elif [ "$l_side" = "source" ]; then
				zxfer_profile_record_bucket source_inspection
			fi
			;;
		esac
		;;
	"snapshot discovery")
		if [ "$l_side" = "destination" ]; then
			zxfer_profile_record_bucket destination_inspection
		elif [ "$l_side" = "source" ]; then
			zxfer_profile_record_bucket source_inspection
		fi
		;;
	*)
		case "$l_verb" in
		list | get)
			if [ "$l_side" = "destination" ]; then
				zxfer_profile_record_bucket destination_inspection
			elif [ "$l_side" = "source" ]; then
				zxfer_profile_record_bucket source_inspection
			fi
			;;
		esac
		;;
	esac

	# Profiling must never alter caller control flow; callers may invoke a
	# recorder as their final statement and propagate its status.
	return 0
}

# Purpose: Record or emit the record SSH invocation for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_record_ssh_invocation() {
	l_host_spec=$1
	l_side=${2:-}

	zxfer_profile_metrics_enabled || return 0

	zxfer_profile_increment_counter g_zxfer_profile_ssh_shell_invocations

	case "$l_side" in
	source)
		zxfer_profile_increment_counter g_zxfer_profile_source_ssh_shell_invocations
		return 0
		;;
	destination)
		zxfer_profile_increment_counter g_zxfer_profile_destination_ssh_shell_invocations
		return 0
		;;
	other)
		zxfer_profile_increment_counter g_zxfer_profile_other_ssh_shell_invocations
		return 0
		;;
	esac

	if [ -n "${g_option_O_origin_host:-}" ] && [ "$l_host_spec" = "$g_option_O_origin_host" ]; then
		zxfer_profile_increment_counter g_zxfer_profile_source_ssh_shell_invocations
	elif [ -n "${g_option_T_target_host:-}" ] && [ "$l_host_spec" = "$g_option_T_target_host" ]; then
		zxfer_profile_increment_counter g_zxfer_profile_destination_ssh_shell_invocations
	else
		zxfer_profile_increment_counter g_zxfer_profile_other_ssh_shell_invocations
	fi

	return 0
}

# Purpose: Record or emit the record remote capability bootstrap source for
# end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_record_remote_capability_bootstrap_source() {
	l_source=$1

	case "$l_source" in
	live)
		zxfer_profile_increment_counter g_zxfer_profile_remote_capability_bootstrap_live
		;;
	cache)
		zxfer_profile_increment_counter g_zxfer_profile_remote_capability_bootstrap_cache
		;;
	memory)
		zxfer_profile_increment_counter g_zxfer_profile_remote_capability_bootstrap_memory
		;;
	esac

	return 0
}

# Purpose: Record or emit the emit summary for end-of-run profiling.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer updates performance counters or prints the profiling
# summary.
zxfer_profile_emit_summary() {
	zxfer_profile_metrics_enabled || return 0
	[ "${g_zxfer_profile_has_data:-0}" -eq 1 ] || return 0

	if [ "${g_zxfer_profile_summary_emitted:-0}" -eq 1 ]; then
		return 0
	fi
	g_zxfer_profile_summary_emitted=1

	l_end_epoch=$(date '+%s' 2>/dev/null || :)
	l_start_epoch=${g_zxfer_profile_start_epoch:-}
	l_elapsed=unknown
	case "$l_start_epoch:$l_end_epoch" in
	*[!0-9:]* | :* | *:) ;;
	*)
		l_elapsed=$((l_end_epoch - l_start_epoch))
		;;
	esac

	zxfer_warn_stderr "zxfer profile: elapsed_seconds=$l_elapsed"
	zxfer_warn_stderr "zxfer profile: startup_latency_ms=${g_zxfer_profile_startup_latency_ms:-0}"
	zxfer_warn_stderr "zxfer profile: cleanup_ms=${g_zxfer_profile_cleanup_ms:-0}"
	zxfer_warn_stderr "zxfer profile: ssh_setup_ms=${g_zxfer_profile_ssh_setup_ms:-0}"
	zxfer_warn_stderr "zxfer profile: source_snapshot_listing_ms=${g_zxfer_profile_source_snapshot_listing_ms:-0}"
	zxfer_warn_stderr "zxfer profile: destination_snapshot_listing_ms=${g_zxfer_profile_destination_snapshot_listing_ms:-0}"
	zxfer_warn_stderr "zxfer profile: snapshot_diff_sort_ms=${g_zxfer_profile_snapshot_diff_sort_ms:-0}"
	zxfer_warn_stderr "zxfer profile: ssh_control_socket_lock_wait_count=${g_zxfer_profile_ssh_control_socket_lock_wait_count:-0}"
	zxfer_warn_stderr "zxfer profile: ssh_control_socket_lock_wait_ms=${g_zxfer_profile_ssh_control_socket_lock_wait_ms:-0}"
	zxfer_warn_stderr "zxfer profile: remote_capability_cache_wait_count=${g_zxfer_profile_remote_capability_cache_wait_count:-0}"
	zxfer_warn_stderr "zxfer profile: remote_capability_cache_wait_ms=${g_zxfer_profile_remote_capability_cache_wait_ms:-0}"
	zxfer_warn_stderr "zxfer profile: remote_capability_bootstrap_live=${g_zxfer_profile_remote_capability_bootstrap_live:-0}"
	zxfer_warn_stderr "zxfer profile: remote_capability_bootstrap_cache=${g_zxfer_profile_remote_capability_bootstrap_cache:-0}"
	zxfer_warn_stderr "zxfer profile: remote_capability_bootstrap_memory=${g_zxfer_profile_remote_capability_bootstrap_memory:-0}"
	zxfer_warn_stderr "zxfer profile: remote_cli_tool_direct_probes=${g_zxfer_profile_remote_cli_tool_direct_probes:-0}"
	zxfer_warn_stderr "zxfer profile: source_zfs_calls=${g_zxfer_profile_source_zfs_calls:-0}"
	zxfer_warn_stderr "zxfer profile: destination_zfs_calls=${g_zxfer_profile_destination_zfs_calls:-0}"
	zxfer_warn_stderr "zxfer profile: other_zfs_calls=${g_zxfer_profile_other_zfs_calls:-0}"
	zxfer_warn_stderr "zxfer profile: zfs_list_calls=${g_zxfer_profile_zfs_list_calls:-0}"
	zxfer_warn_stderr "zxfer profile: zfs_get_calls=${g_zxfer_profile_zfs_get_calls:-0}"
	zxfer_warn_stderr "zxfer profile: zfs_send_calls=${g_zxfer_profile_zfs_send_calls:-0}"
	zxfer_warn_stderr "zxfer profile: zfs_receive_calls=${g_zxfer_profile_zfs_receive_calls:-0}"
	zxfer_warn_stderr "zxfer profile: ssh_shell_invocations=${g_zxfer_profile_ssh_shell_invocations:-0}"
	zxfer_warn_stderr "zxfer profile: source_ssh_shell_invocations=${g_zxfer_profile_source_ssh_shell_invocations:-0}"
	zxfer_warn_stderr "zxfer profile: destination_ssh_shell_invocations=${g_zxfer_profile_destination_ssh_shell_invocations:-0}"
	zxfer_warn_stderr "zxfer profile: other_ssh_shell_invocations=${g_zxfer_profile_other_ssh_shell_invocations:-0}"
	zxfer_warn_stderr "zxfer profile: source_snapshot_list_commands=${g_zxfer_profile_source_snapshot_list_commands:-0}"
	zxfer_warn_stderr "zxfer profile: source_snapshot_list_parallel_commands=${g_zxfer_profile_source_snapshot_list_parallel_commands:-0}"
	zxfer_warn_stderr "zxfer profile: send_receive_pipeline_commands=${g_zxfer_profile_send_receive_pipeline_commands:-0}"
	zxfer_warn_stderr "zxfer profile: send_receive_background_pipeline_commands=${g_zxfer_profile_send_receive_background_pipeline_commands:-0}"
	zxfer_warn_stderr "zxfer profile: exists_destination_calls=${g_zxfer_profile_exists_destination_calls:-0}"
	zxfer_warn_stderr "zxfer profile: normalized_property_reads_source=${g_zxfer_profile_normalized_property_reads_source:-0}"
	zxfer_warn_stderr "zxfer profile: normalized_property_reads_destination=${g_zxfer_profile_normalized_property_reads_destination:-0}"
	zxfer_warn_stderr "zxfer profile: normalized_property_reads_other=${g_zxfer_profile_normalized_property_reads_other:-0}"
	zxfer_warn_stderr "zxfer profile: required_property_backfill_gets=${g_zxfer_profile_required_property_backfill_gets:-0}"
	zxfer_warn_stderr "zxfer profile: parent_destination_property_reads=${g_zxfer_profile_parent_destination_property_reads:-0}"
	zxfer_warn_stderr "zxfer profile: bucket_source_inspection=${g_zxfer_profile_bucket_source_inspection:-0}"
	zxfer_warn_stderr "zxfer profile: bucket_destination_inspection=${g_zxfer_profile_bucket_destination_inspection:-0}"
	zxfer_warn_stderr "zxfer profile: bucket_property_reconciliation=${g_zxfer_profile_bucket_property_reconciliation:-0}"
	zxfer_warn_stderr "zxfer profile: bucket_send_receive_setup=${g_zxfer_profile_bucket_send_receive_setup:-0}"
	zxfer_warn_stderr "zxfer profile: runtime_artifact_files_created=${g_zxfer_profile_runtime_artifact_files_created:-0}"
	zxfer_warn_stderr "zxfer profile: runtime_artifact_dirs_created=${g_zxfer_profile_runtime_artifact_dirs_created:-0}"
	zxfer_warn_stderr "zxfer profile: runtime_artifact_paths_cleaned=${g_zxfer_profile_runtime_artifact_paths_cleaned:-0}"
	zxfer_warn_stderr "zxfer profile: runtime_cache_object_writes=${g_zxfer_profile_runtime_cache_object_writes:-0}"
	zxfer_warn_stderr "zxfer profile: runtime_cache_object_readbacks=${g_zxfer_profile_runtime_cache_object_readbacks:-0}"
	zxfer_warn_stderr "zxfer profile: command_render_calls=${g_zxfer_profile_command_render_calls:-0}"
	zxfer_warn_stderr "zxfer profile: live_destination_snapshot_rechecks=${g_zxfer_profile_live_destination_snapshot_rechecks:-0}"
	zxfer_warn_stderr "zxfer profile: diverged_snapshot_warnings=${g_zxfer_profile_diverged_snapshot_warnings:-0}"
}
