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
# owns globals: the g_zxfer_profile_* counters and stage timings, the clock
#   results g_zxfer_profile_clock_ms and g_zxfer_profile_elapsed_ms, and the
#   summary state. A producer in another module bumps its counter inline,
#   g_x=$((g_x + 1)), with or without -V: the session reset zeroes every
#   counter before any producer runs, so an inherited environment value
#   never reaches that arithmetic.
# reads globals: g_option_V_very_verbose, g_option_O_origin_host,
#   g_option_T_target_host, g_zxfer_failure_stage (bucket attribution), and
#   g_zxfer_profile_prescan (set by the launcher).
# mutates caches: profile counters only. The recorders and timers always
#   return 0, so one used as a caller's last statement never changes its
#   status.
# returns via stdout: none.
#
# Every "zxfer profile: key=value" line of the -V summary is a stable key, in
# a fixed order. Keys whose producers were removed print a literal 0, and
# ssh_shell_invocations is the sum of the three per-side ssh counters.

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
	# The millisecond field counts only as the seconds field followed by
	# exactly three digits, as GNU date prints %3N. BSD and illumos date
	# print %3N literally, and BusyBox date prints the nanoseconds without
	# zero padding, so two of its readings can differ in length; both fall
	# back to whole seconds.
	case $l_profile_clock_ms in
	"$l_profile_clock_s"[0123456789][0123456789][0123456789]) ;;
	*) l_profile_clock_ms=$((l_profile_clock_s * 1000)) ;;
	esac
	g_zxfer_profile_clock_ms=$l_profile_clock_ms
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

# Purpose: Start timing a -V stage in the current shell.
# Usage: zxfer_profile_start_timer, then keep g_zxfer_profile_clock_ms as the
# stage's start for zxfer_profile_stop_timer. It is empty without -V or a
# usable date, and the stage then adds nothing.
zxfer_profile_start_timer() {
	g_zxfer_profile_clock_ms=""
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	zxfer_profile_read_clock_ms || :
}

# Purpose: Measure a -V stage started with zxfer_profile_start_timer.
# Usage: zxfer_profile_stop_timer START_MS, then add g_zxfer_profile_elapsed_ms
# to the stage's counter. It is 0 without -V, when START_MS is not a number,
# when date is unusable, and when the clock went backwards.
zxfer_profile_stop_timer() {
	g_zxfer_profile_elapsed_ms=0
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	zxfer_is_uint "$1" || return 0
	zxfer_profile_read_clock_ms || return 0
	[ "$g_zxfer_profile_clock_ms" -ge "$1" ] || return 0
	g_zxfer_profile_elapsed_ms=$((g_zxfer_profile_clock_ms - $1))
	# A measured stage prints the summary even when it took 0 ms.
	g_zxfer_profile_has_data=1
}

# Purpose: Count one zfs invocation under -V by side, by verb, and in the
# bucket of the active failure stage.
# Usage: zxfer_profile_record_zfs_call source|destination|other VERB; any
# other side counts as other, and verbs other than list, get, send, and
# receive count only toward the side.
zxfer_profile_record_zfs_call() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	case $1 in
	source) g_zxfer_profile_source_zfs_calls=$((g_zxfer_profile_source_zfs_calls + 1)) ;;
	destination) g_zxfer_profile_destination_zfs_calls=$((g_zxfer_profile_destination_zfs_calls + 1)) ;;
	*) g_zxfer_profile_other_zfs_calls=$((g_zxfer_profile_other_zfs_calls + 1)) ;;
	esac
	case $2 in
	list) g_zxfer_profile_zfs_list_calls=$((g_zxfer_profile_zfs_list_calls + 1)) ;;
	get) g_zxfer_profile_zfs_get_calls=$((g_zxfer_profile_zfs_get_calls + 1)) ;;
	send) g_zxfer_profile_zfs_send_calls=$((g_zxfer_profile_zfs_send_calls + 1)) ;;
	receive) g_zxfer_profile_zfs_receive_calls=$((g_zxfer_profile_zfs_receive_calls + 1)) ;;
	esac

	# Property transfer owns every call in its stage; send/receive setup owns
	# the send and receive verbs; inspection owns list and get everywhere else
	# and every call during snapshot discovery. Other-side calls have no
	# inspection bucket.
	case ${g_zxfer_failure_stage:-}:$2 in
	"property transfer":*)
		g_zxfer_profile_bucket_property_reconciliation=$((g_zxfer_profile_bucket_property_reconciliation + 1))
		;;
	"send/receive":send | "send/receive":receive)
		g_zxfer_profile_bucket_send_receive_setup=$((g_zxfer_profile_bucket_send_receive_setup + 1))
		;;
	"snapshot discovery":* | *:list | *:get)
		case $1 in
		source) g_zxfer_profile_bucket_source_inspection=$((g_zxfer_profile_bucket_source_inspection + 1)) ;;
		destination) g_zxfer_profile_bucket_destination_inspection=$((g_zxfer_profile_bucket_destination_inspection + 1)) ;;
		esac
		;;
	esac
	return 0
}

# Purpose: Count one ssh shell invocation under -V, attributed to a side.
# Usage: zxfer_profile_record_ssh_invocation HOST_SPEC [source|destination|
# other]; without a side, HOST_SPEC is matched against -O, then -T.
zxfer_profile_record_ssh_invocation() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	l_profile_ssh_side=${2:-}
	case $l_profile_ssh_side in
	source | destination | other) ;;
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
	case $l_profile_ssh_side in
	source) g_zxfer_profile_source_ssh_shell_invocations=$((g_zxfer_profile_source_ssh_shell_invocations + 1)) ;;
	destination) g_zxfer_profile_destination_ssh_shell_invocations=$((g_zxfer_profile_destination_ssh_shell_invocations + 1)) ;;
	*) g_zxfer_profile_other_ssh_shell_invocations=$((g_zxfer_profile_other_ssh_shell_invocations + 1)) ;;
	esac
}

# Purpose: Print the -V profile summary to stderr once per run.
# Usage: Called by the EXIT trap; silent without -V, and when -V neither
# timed a stage nor counted anything.
zxfer_profile_emit_summary() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	[ "${g_zxfer_profile_summary_emitted:-0}" -eq 0 ] || return 0

	set -- \
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
		"ssh_shell_invocations=$((${g_zxfer_profile_source_ssh_shell_invocations:-0} + ${g_zxfer_profile_destination_ssh_shell_invocations:-0} + ${g_zxfer_profile_other_ssh_shell_invocations:-0}))" \
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
		"normalized_property_reads_other=0" \
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
		"diverged_snapshot_warnings=${g_zxfer_profile_diverged_snapshot_warnings:-0}"
	# Counts are plain decimals, so a count above 0 starts with 1-9.
	case ${g_zxfer_profile_has_data:-0}$* in
	1* | *=[1-9]*) ;;
	*) return 0 ;;
	esac
	g_zxfer_profile_summary_emitted=1

	# Whole seconds between the two clock readings, as with date +%s.
	l_profile_elapsed=unknown
	if zxfer_is_uint "${g_zxfer_profile_start_ms:-}" && zxfer_profile_read_clock_ms; then
		l_profile_elapsed=$((g_zxfer_profile_clock_ms / 1000 - g_zxfer_profile_start_ms / 1000))
		# A clock that went backwards reports 0, never a negative count.
		[ "$l_profile_elapsed" -ge 0 ] || l_profile_elapsed=0
	fi
	printf 'zxfer profile: %s\n' "elapsed_seconds=$l_profile_elapsed" "$@" >&2
}
