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
# REPORTING / FAILURE HANDLING / PROFILING
################################################################################

# Module contract:
# owns globals: g_zxfer_failure_* and g_zxfer_profile_* reporting state.
# reads globals: g_option_* verbosity/beep flags, g_cmd_awk, and current dataset context.
# mutates caches: none.
# returns via stdout: escaped values, rendered reports, timestamps, and counter values.

# Purpose: Initialize the failure context defaults before later helpers depend
# on it.
# Usage: Called during failure reporting, profiling, and verbose operator
# output during bootstrap so downstream code sees consistent defaults and
# runtime state.
zxfer_init_failure_context_defaults() {
	: "${g_zxfer_failure_report_emitted:=0}"
	: "${g_zxfer_failure_class:=}"
	: "${g_zxfer_failure_stage:=startup}"
	: "${g_zxfer_failure_message:=}"
	: "${g_zxfer_failure_source_root:=}"
	: "${g_zxfer_failure_current_source:=}"
	: "${g_zxfer_failure_destination_root:=}"
	: "${g_zxfer_failure_current_destination:=}"
	: "${g_zxfer_failure_last_command:=}"
	: "${g_zxfer_original_invocation:=}"
}

# Purpose: Reset the failure context so the next reporting pass starts from a
# clean state.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_failure_context() {
	g_zxfer_failure_report_emitted=0
	g_zxfer_failure_class=""
	g_zxfer_failure_stage=${1:-startup}
	g_zxfer_failure_message=""
	g_zxfer_failure_source_root=""
	g_zxfer_failure_current_source=""
	g_zxfer_failure_destination_root=""
	g_zxfer_failure_current_destination=""
	g_zxfer_failure_last_command=""
}

# Purpose: Emit the stderr in the operator-facing format owned by this module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to surface status, warning, or diagnostic text.
zxfer_warn_stderr() {
	printf '%s\n' "$*" >&2
}

# Purpose: Escape the report value for the serialization or quoting context
# used here.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before values are embedded in rendered commands or structured reports.
zxfer_escape_report_value() {
	# Escape raw ASCII control bytes so failure-report fields stay inert when
	# mirrored to terminals, pagers, and ZXFER_ERROR_LOG.
	l_report_value=$1
	l_trailing_newlines=0
	l_scan_value=$l_report_value
	while :; do
		case $l_scan_value in
		*'
')
			l_trailing_newlines=$((l_trailing_newlines + 1))
			l_scan_value=${l_scan_value%?}
			;;
		*)
			break
			;;
		esac
	done

	# shellcheck disable=SC2016
	printf '%s' "$l_report_value" | LC_ALL=C ${g_cmd_awk:-awk} '
BEGIN {
	ORS = ""
	for (i = 1; i < 32; i++) {
		ctrl[sprintf("%c", i)] = sprintf("\\x%02X", i)
	}
	ctrl[sprintf("%c", 9)] = "\\t"
	ctrl[sprintf("%c", 13)] = "\\r"
	ctrl[sprintf("%c", 127)] = "\\x7F"
}
{
	if (NR > 1) {
		printf "\\n"
	}
	line = $0
	for (i = 1; i <= length(line); i++) {
		c = substr(line, i, 1)
		if (c == "\\") {
			printf "\\\\"
		} else if (c in ctrl) {
			printf "%s", ctrl[c]
		} else {
			printf "%s", c
		}
	}
}
'
	while [ "$l_trailing_newlines" -gt 0 ]; do
		printf '\\n'
		l_trailing_newlines=$((l_trailing_newlines - 1))
	done
}

# Purpose: Quote the token for report for the shell or report format used by
# zxfer.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when raw tokens must be preserved without reopening parsing or
# injection risks.
zxfer_quote_token_for_report() {
	l_value_escaped=$(zxfer_escape_report_value "$1")
	l_value_safe=$(printf '%s' "$l_value_escaped" | sed "s/'/'\"'\"'/g")
	printf "'%s'" "$l_value_safe"
}

# Purpose: Quote the command argv for the shell or report format used by zxfer.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when raw tokens must be preserved without reopening parsing or
# injection risks.
zxfer_quote_command_argv() {
	l_output=""
	for l_arg in "$@"; do
		l_quoted_arg=$(zxfer_quote_token_for_report "$l_arg")
		if [ "$l_output" = "" ]; then
			l_output=$l_quoted_arg
		else
			l_output="$l_output $l_quoted_arg"
		fi
	done
	printf '%s\n' "$l_output"
}

# Purpose: Check whether failure reports should expose unsafe verbatim command
# strings.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before report renderers decide whether to preserve shell-quoted
# command details for local debugging or replace them with the redaction
# marker.
zxfer_failure_report_uses_unsafe_command_fields() {
	case "${ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS:-}" in
	1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn])
		return 0
		;;
	esac

	return 1
}

# Purpose: Return the failure report redaction marker in the form expected by
# later helpers.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_failure_report_redaction_marker() {
	printf '%s\n' "[redacted]"
}

# Purpose: Render the command for report as a stable shell-safe or operator-
# facing string.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to display or transport the value without reparsing
# it.
#
# Render an optional shell-ready command prefix plus argv tokens into the
# single-line report format used by dry-run output and failure summaries.
zxfer_render_command_for_report() {
	l_prefix=$1
	shift

	if [ $# -gt 0 ]; then
		l_quoted_args=$(zxfer_quote_command_argv "$@")
	else
		l_quoted_args=""
	fi

	if [ "$l_prefix" != "" ] && [ "$l_quoted_args" != "" ]; then
		printf '%s %s\n' "$l_prefix" "$l_quoted_args"
	elif [ "$l_prefix" != "" ]; then
		printf '%s\n' "$l_prefix"
	else
		printf '%s\n' "$l_quoted_args"
	fi
}

# Purpose: Update the failure stage in the shared runtime state.
# Usage: Called during failure reporting, profiling, and verbose operator
# output after a probe or planning step changes the active context that later
# helpers should use.
zxfer_set_failure_stage() {
	zxfer_init_failure_context_defaults
	[ -n "$1" ] && g_zxfer_failure_stage=$1
}

# Purpose: Update the failure roots in the shared runtime state.
# Usage: Called during failure reporting, profiling, and verbose operator
# output after a probe or planning step changes the active context that later
# helpers should use.
zxfer_set_failure_roots() {
	zxfer_init_failure_context_defaults
	[ $# -ge 1 ] && [ -n "$1" ] && g_zxfer_failure_source_root=$1
	[ $# -ge 2 ] && [ -n "$2" ] && g_zxfer_failure_destination_root=$2
}

# Purpose: Update the current dataset context in the shared runtime state.
# Usage: Called during failure reporting, profiling, and verbose operator
# output after a probe or planning step changes the active context that later
# helpers should use.
zxfer_set_current_dataset_context() {
	zxfer_init_failure_context_defaults
	[ $# -ge 1 ] && [ -n "$1" ] && g_zxfer_failure_current_source=$1
	[ $# -ge 2 ] && [ -n "$2" ] && g_zxfer_failure_current_destination=$2
}

# Purpose: Record the last command string for later diagnostics or control
# decisions.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs the state preserved for follow-on helpers or
# reporting.
zxfer_record_last_command_string() {
	zxfer_init_failure_context_defaults
	if [ $# -eq 0 ] || [ "$1" = "" ]; then
		g_zxfer_failure_last_command=""
		return
	fi
	if zxfer_failure_report_uses_unsafe_command_fields; then
		g_zxfer_failure_last_command=$(zxfer_escape_report_value "$1")
	else
		g_zxfer_failure_last_command=$(zxfer_get_failure_report_redaction_marker)
	fi
}

# Purpose: Record the last command argv for later diagnostics or control
# decisions.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs the state preserved for follow-on helpers or
# reporting.
zxfer_record_last_command_argv() {
	zxfer_init_failure_context_defaults
	if [ $# -eq 0 ]; then
		g_zxfer_failure_last_command=""
		return
	fi
	if zxfer_failure_report_uses_unsafe_command_fields; then
		g_zxfer_failure_last_command=$(zxfer_quote_command_argv "$@")
	else
		g_zxfer_failure_last_command=$(zxfer_get_failure_report_redaction_marker)
	fi
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
	'')
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
	'')
		return 0
		;;
	esac

	case "$l_start_ms" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	if [ -z "$l_end_ms" ]; then
		if ! l_end_ms=$(zxfer_profile_now_ms); then
			return 0
		fi
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
			[ "$l_side" = "destination" ] && zxfer_profile_record_bucket destination_inspection
			[ "$l_side" = "source" ] && zxfer_profile_record_bucket source_inspection
			;;
		esac
		;;
	"snapshot discovery")
		[ "$l_side" = "destination" ] && zxfer_profile_record_bucket destination_inspection
		[ "$l_side" = "source" ] && zxfer_profile_record_bucket source_inspection
		;;
	*)
		case "$l_verb" in
		list | get)
			[ "$l_side" = "destination" ] && zxfer_profile_record_bucket destination_inspection
			[ "$l_side" = "source" ] && zxfer_profile_record_bucket source_inspection
			;;
		esac
		;;
	esac
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
}

# Purpose: Emit the usage to stderr in the operator-facing format owned by this
# module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to surface status, warning, or diagnostic text.
zxfer_print_usage_to_stderr() {
	if command -v zxfer_usage >/dev/null 2>&1; then
		zxfer_usage >&2
	fi
}

# Purpose: Return the failure mode label in the form expected by later helpers.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_failure_mode_label() {
	if [ -n "${g_option_R_recursive:-}" ]; then
		printf 'recursive\n'
	elif [ -n "${g_option_N_nonrecursive:-}" ]; then
		printf 'nonrecursive\n'
	fi
}

# Purpose: Append the report field to the module-owned accumulator.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_report_field() {
	l_key=$1
	l_value=$2

	[ -n "$l_value" ] || return
	printf '%s: %s\n' "$l_key" "$(zxfer_escape_report_value "$l_value")"
}

# Purpose: Append the preescaped report field to the module-owned accumulator.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_preescaped_report_field() {
	l_key=$1
	l_value=$2

	[ -n "$l_value" ] || return
	printf '%s: %s\n' "$l_key" "$l_value"
}

# Purpose: Render the failure report as a stable shell-safe or operator-facing
# string.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to display or transport the value without reparsing
# it.
zxfer_render_failure_report() {
	l_exit_status=$1

	zxfer_init_failure_context_defaults

	l_timestamp=$(date '+%Y-%m-%dT%H:%M:%S%z' 2>/dev/null || date)
	l_hostname=$(uname -n 2>/dev/null || hostname 2>/dev/null || echo unknown)
	l_failure_class=$g_zxfer_failure_class
	l_failure_message=$g_zxfer_failure_message
	l_failure_stage=$g_zxfer_failure_stage
	l_mode=$(zxfer_get_failure_mode_label)
	l_report_invocation=${g_zxfer_original_invocation:-}
	l_report_last_command=${g_zxfer_failure_last_command:-}

	if [ -z "$l_failure_class" ]; then
		if [ "$l_exit_status" -eq 2 ]; then
			l_failure_class=usage
		else
			l_failure_class=runtime
		fi
	fi

	if [ -z "$l_failure_message" ]; then
		l_failure_message="zxfer exited with status $l_exit_status."
	fi
	if ! zxfer_failure_report_uses_unsafe_command_fields; then
		if [ -n "$l_report_invocation" ]; then
			l_report_invocation=$(zxfer_get_failure_report_redaction_marker)
		fi
		if [ -n "$l_report_last_command" ]; then
			l_report_last_command=$(zxfer_get_failure_report_redaction_marker)
		fi
	fi

	printf 'zxfer: failure report begin\n'
	zxfer_append_report_field timestamp "$l_timestamp"
	zxfer_append_report_field hostname "$l_hostname"
	zxfer_append_report_field zxfer_version "${g_zxfer_version:-unknown}"
	zxfer_append_report_field exit_status "$l_exit_status"
	zxfer_append_report_field failure_class "$l_failure_class"
	zxfer_append_report_field failure_stage "$l_failure_stage"
	zxfer_append_report_field message "$l_failure_message"
	zxfer_append_report_field source_root "${g_zxfer_failure_source_root:-}"
	zxfer_append_report_field current_source "${g_zxfer_failure_current_source:-}"
	zxfer_append_report_field destination_root "${g_zxfer_failure_destination_root:-}"
	zxfer_append_report_field current_destination "${g_zxfer_failure_current_destination:-}"
	zxfer_append_report_field origin_host "${g_option_O_origin_host:-}"
	zxfer_append_report_field target_host "${g_option_T_target_host:-}"
	zxfer_append_report_field dry_run "${g_option_n_dryrun:-0}"
	zxfer_append_report_field mode "$l_mode"
	zxfer_append_report_field yield_iterations "${g_option_Y_yield_iterations:-}"
	zxfer_append_preescaped_report_field invocation "$l_report_invocation"
	zxfer_append_preescaped_report_field last_command "$l_report_last_command"
	printf 'zxfer: failure report end\n'
}

# Purpose: Return the error log parent directory in the form expected by later
# helpers.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_error_log_parent_dir() {
	zxfer_get_path_parent_dir "$1"
}

# Purpose: Validate the existing error log file before zxfer relies on it.
# Usage: Called during failure reporting, profiling, and verbose operator
# output to fail closed on malformed, unsafe, or stale input.
zxfer_validate_existing_error_log_file() {
	l_validate_candidate_path=$1
	l_validate_display_path=$2

	if [ -L "$l_validate_candidate_path" ] || [ -h "$l_validate_candidate_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_validate_display_path\" because it is a symlink."
		return 1
	fi
	if [ -e "$l_validate_candidate_path" ] && [ ! -f "$l_validate_candidate_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_validate_display_path\" because it is not a regular file."
		return 1
	fi
	if ! l_validate_owner_uid=$(zxfer_get_path_owner_uid "$l_validate_candidate_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because its owner could not be determined."
		return 1
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_validate_owner_uid"; then
		l_validate_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because it is owned by UID $l_validate_owner_uid instead of $l_validate_expected_owner_desc."
		return 1
	fi
	if ! l_validate_mode=$(zxfer_get_path_mode_octal "$l_validate_candidate_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because its permissions could not be determined."
		return 1
	fi
	if [ "$l_validate_mode" != "600" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_display_path\" because its permissions ($l_validate_mode) are not 0600."
		return 1
	fi
}

# Purpose: Return the error log lock directory in the form expected by later
# helpers.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_error_log_lock_dir() {
	l_lock_log_path=$1
	l_lock_log_parent=$2
	l_lock_log_name=${l_lock_log_path##*/}

	printf '%s/.zxfer-error-log.lock.%s\n' "$l_lock_log_parent" "$l_lock_log_name"
}

zxfer_get_error_log_lock_purpose() {
	printf '%s\n' "error-log-lock"
}

# Purpose: Build the per-log lock key used to serialize `ZXFER_ERROR_LOG`
# appends.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before lock directories are created or reused for one error-log file.
zxfer_error_log_lock_key() {
	l_key_path=$1

	if l_key_cksum=$(printf '%s' "$l_key_path" | cksum 2>/dev/null); then
		# shellcheck disable=SC2086
		set -- $l_key_cksum
		if [ $# -ge 1 ] && [ -n "$1" ]; then
			printf 'k%s\n' "$1"
			return 0
		fi
	fi

	l_key_hex=$(printf '%s' "$l_key_path" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n' | cut -c 1-16)
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	printf 'k%s\n' "$l_key_hex"
}

# Purpose: Capture the reporting helper output into staged state or module
# globals for later use.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need a checked snapshot of command output or
# computed state.
zxfer_capture_reporting_helper_output() {
	l_result_var=$1
	shift

	g_zxfer_reporting_capture_result=""
	if ! zxfer_create_runtime_artifact_file "zxfer-reporting" >/dev/null; then
		return 1
	fi
	l_capture_file=$g_zxfer_runtime_artifact_path_result
	if ! "$@" >"$l_capture_file"; then
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return 1
	fi

	if zxfer_read_runtime_artifact_file "$l_capture_file" >/dev/null; then
		:
	else
		l_capture_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return "$l_capture_status"
	fi
	zxfer_cleanup_runtime_artifact_path "$l_capture_file"

	g_zxfer_reporting_capture_result=$g_zxfer_runtime_artifact_read_result
	case "$g_zxfer_reporting_capture_result" in
	*'
')
		g_zxfer_reporting_capture_result=${g_zxfer_reporting_capture_result%?}
		;;
	esac
	eval "$l_result_var=\$g_zxfer_reporting_capture_result"
	return 0
}

# Purpose: Return the error log fallback lock directory in the form expected by
# later helpers.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_error_log_fallback_lock_dir() {
	l_fallback_log_path=$1

	l_fallback_tmpdir=""
	if [ -n "${TMPDIR:-}" ] &&
		zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "$TMPDIR"; then
		:
	elif zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "/dev/shm"; then
		:
	elif zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "/run/shm"; then
		:
	elif zxfer_capture_reporting_helper_output l_fallback_tmpdir zxfer_validate_temp_root_candidate "/tmp"; then
		:
	else
		return 1
	fi
	if ! zxfer_capture_reporting_helper_output l_fallback_key zxfer_error_log_lock_key "$l_fallback_log_path"; then
		return 1
	fi

	printf '%s/.zxfer-error-log.lock.%s\n' "$l_fallback_tmpdir" "$l_fallback_key"
}

# Purpose: Acquire the error log lock so concurrent zxfer work does not reuse
# it unsafely.
# Usage: Called during failure reporting, profiling, and verbose operator
# output before a shared cache, lock, or transport resource is used by this
# run.
zxfer_acquire_error_log_lock() {
	l_lock_dir_path=$1
	l_lock_attempts=0

	while ! zxfer_create_owned_lock_dir \
		"$l_lock_dir_path" lock "$(zxfer_get_error_log_lock_purpose)" >/dev/null; do
		if [ -L "$l_lock_dir_path" ] || [ -h "$l_lock_dir_path" ]; then
			return 1
		fi
		if [ -d "$l_lock_dir_path" ]; then
			if zxfer_try_reap_stale_owned_lock_dir \
				"$l_lock_dir_path" 1 lock "$(zxfer_get_error_log_lock_purpose)" >/dev/null; then
				continue
			fi
			l_reap_status=$?
			if [ "$l_reap_status" -eq 1 ]; then
				return 1
			fi
		fi
		l_lock_attempts=$((l_lock_attempts + 1))
		if [ "$l_lock_attempts" -ge 3 ]; then
			return 1
		fi
		sleep 1
	done
}

# Purpose: Release the error log lock after the protected work finishes.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when a shared cache, lock, or transport resource should no longer be
# held.
zxfer_release_error_log_lock() {
	l_release_lock_dir=$1

	zxfer_release_owned_lock_dir \
		"$l_release_lock_dir" lock "$(zxfer_get_error_log_lock_purpose)"
}

zxfer_warn_error_log_lock_release_failure() {
	l_log_path=$1
	l_status=$2

	zxfer_warn_stderr "zxfer: warning: unable to release ZXFER_ERROR_LOG lock for \"$l_log_path\" (status $l_status)."
}

zxfer_release_error_log_lock_warn_only() {
	l_log_path=$1
	l_lock_dir=$2

	zxfer_release_error_log_lock "$l_lock_dir"
	l_release_status=$?
	if [ "$l_release_status" -eq 0 ]; then
		return 0
	fi
	zxfer_warn_error_log_lock_release_failure "$l_log_path" "$l_release_status"
	return 0
}

zxfer_release_error_log_lock_checked() {
	l_log_path=$1
	l_lock_dir=$2

	zxfer_release_error_log_lock "$l_lock_dir"
	l_release_status=$?
	if [ "$l_release_status" -eq 0 ]; then
		return 0
	fi
	zxfer_warn_error_log_lock_release_failure "$l_log_path" "$l_release_status"
	return 1
}

# Purpose: Clean up the error log stage directory that this module created or
# tracks.
# Usage: Called during failure reporting, profiling, and verbose operator
# output on success and failure paths so temporary state does not linger.
zxfer_cleanup_error_log_stage_dir() {
	l_cleanup_stage_dir=$1

	[ -n "$l_cleanup_stage_dir" ] || return 0
	if command -v zxfer_cleanup_runtime_artifact_path >/dev/null 2>&1; then
		zxfer_cleanup_runtime_artifact_path "$l_cleanup_stage_dir" >/dev/null 2>&1 || true
		return 0
	fi
	rm -f "$l_cleanup_stage_dir/log.snapshot" "$l_cleanup_stage_dir/log.write" 2>/dev/null || true
	rmdir "$l_cleanup_stage_dir" 2>/dev/null || true
}

# Purpose: Append the failure report to existing log directly to the module-
# owned accumulator.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_failure_report_to_existing_log_directly() {
	l_direct_report=$1
	l_direct_log_path=$2

	printf '%s\n' "$l_direct_report" >>"$l_direct_log_path"
}

# Purpose: Check whether the error log parent is writable.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need a boolean answer about the error log parent.
zxfer_error_log_parent_is_writable() {
	[ -w "$1" ]
}

# Purpose: Create the error log file using the safety checks owned by this
# module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_error_log_file() {
	l_create_log_path=$1

	if ! zxfer_create_secure_staging_dir_for_path "$l_create_log_path" "zxfer-error-log" >/dev/null; then
		return 1
	fi
	l_create_stage_dir=$g_zxfer_secure_staging_dir_result
	l_create_stage_file="$l_create_stage_dir/log.write"

	if ! (
		umask 077
		zxfer_write_runtime_artifact_file "$l_create_stage_file" ""
	); then
		zxfer_cleanup_error_log_stage_dir "$l_create_stage_dir"
		return 1
	fi
	if ! mv -f "$l_create_stage_file" "$l_create_log_path"; then
		zxfer_cleanup_error_log_stage_dir "$l_create_stage_dir"
		return 1
	fi
	zxfer_cleanup_error_log_stage_dir "$l_create_stage_dir"
}

# Purpose: Apply the required permissions to the error log file.
# Usage: Called during failure reporting, profiling, and verbose operator
# output after a file is created so later reads honor zxfer's security
# expectations.
zxfer_chmod_error_log_file() {
	l_chmod_log_path=$1

	chmod 600 "$l_chmod_log_path"
}

# Purpose: Append the failure report to log to the module-owned accumulator.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_failure_report_to_log() {
	l_report=$1
	l_log_path=${ZXFER_ERROR_LOG:-}

	[ -n "$l_log_path" ] || return 0

	case "$l_log_path" in
	/*) ;;
	*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_log_path\" because it is not absolute."
		return 1
		;;
	esac

	if l_symlink_component=$(zxfer_find_symlink_path_component "$l_log_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_log_path\" because path component \"$l_symlink_component\" is a symlink."
		return 1
	fi

	l_log_parent=$(zxfer_get_error_log_parent_dir "$l_log_path")
	if [ ! -d "$l_log_parent" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_log_path\" because parent directory \"$l_log_parent\" does not exist."
		return 1
	fi
	if ! l_trusted_log_parent=$(zxfer_validate_temp_root_candidate "$l_log_parent"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_log_path\" because parent directory \"$l_log_parent\" is not owned by root or the effective user, or is writable by others without sticky-bit protection."
		return 1
	fi

	l_log_exists=0
	if [ -e "$l_log_path" ]; then
		l_log_exists=1
	fi
	l_log_parent_writable=0
	if zxfer_error_log_parent_is_writable "$l_trusted_log_parent"; then
		l_log_parent_writable=1
	fi

	if [ "$l_log_exists" -eq 0 ] && [ "$l_log_parent_writable" -eq 0 ]; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_log_path\"."
		return 1
	fi

	if [ "$l_log_exists" -eq 1 ] && [ "$l_log_parent_writable" -eq 0 ]; then
		if ! l_lock_dir=$(zxfer_get_error_log_fallback_lock_dir "$l_log_path"); then
			zxfer_warn_stderr "zxfer: warning: unable to acquire ZXFER_ERROR_LOG lock for \"$l_log_path\"."
			return 1
		fi
	else
		l_lock_dir=$(zxfer_get_error_log_lock_dir "$l_log_path" "$l_trusted_log_parent")
	fi
	if ! zxfer_acquire_error_log_lock "$l_lock_dir"; then
		zxfer_warn_stderr "zxfer: warning: unable to acquire ZXFER_ERROR_LOG lock for \"$l_log_path\"."
		return 1
	fi

	if [ "$l_log_exists" -eq 1 ]; then
		if ! zxfer_validate_existing_error_log_file "$l_log_path" "$l_log_path"; then
			zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
			return 1
		fi
	else
		if ! zxfer_create_error_log_file "$l_log_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_log_path\"."
			zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
			return 1
		fi
		if ! zxfer_chmod_error_log_file "$l_log_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_log_path\" to 0600."
			zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
			return 1
		fi
		if ! zxfer_validate_existing_error_log_file "$l_log_path" "$l_log_path"; then
			zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
			return 1
		fi
	fi

	if [ "$l_log_parent_writable" -eq 0 ]; then
		if ! zxfer_append_failure_report_to_existing_log_directly "$l_report" "$l_log_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
			zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
			return 1
		fi
		zxfer_release_error_log_lock_checked "$l_log_path" "$l_lock_dir"
		return "$?"
	fi

	if ! zxfer_create_secure_staging_dir_for_path "$l_log_path" "zxfer-error-log" >/dev/null; then
		zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG staging directory for \"$l_log_path\"."
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	l_stage_dir=$g_zxfer_secure_staging_dir_result
	l_snapshot_path="$l_stage_dir/log.snapshot"
	l_staged_log_path="$l_stage_dir/log.write"
	if ! ln "$l_log_path" "$l_snapshot_path" 2>/dev/null; then
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	if ! zxfer_validate_existing_error_log_file "$l_snapshot_path" "$l_log_path"; then
		zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	l_old_umask=$(umask)
	umask 077
	if ! cat "$l_snapshot_path" >"$l_staged_log_path"; then
		umask "$l_old_umask"
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	if ! printf '%s\n' "$l_report" >>"$l_staged_log_path"; then
		umask "$l_old_umask"
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	umask "$l_old_umask"
	if ! zxfer_chmod_error_log_file "$l_staged_log_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_log_path\" to 0600."
		zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	if ! mv -f "$l_staged_log_path" "$l_log_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
		zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
		zxfer_release_error_log_lock_warn_only "$l_log_path" "$l_lock_dir"
		return 1
	fi
	zxfer_cleanup_error_log_stage_dir "$l_stage_dir"
	zxfer_release_error_log_lock_checked "$l_log_path" "$l_lock_dir"
}

# Purpose: Emit the failure report in the operator-facing format owned by this
# module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to surface status, warning, or diagnostic text.
zxfer_emit_failure_report() {
	l_exit_status=$1

	zxfer_init_failure_context_defaults

	[ "$l_exit_status" -ne 0 ] || return 0
	[ "${g_zxfer_failure_report_emitted:-0}" -eq 0 ] || return 0

	l_report=$(zxfer_render_failure_report "$l_exit_status")
	printf '%s\n' "$l_report" >&2
	g_zxfer_failure_report_emitted=1
	zxfer_append_failure_report_to_log "$l_report" || true
}

# Purpose: Raise the error through zxfer's structured failure reporting path.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the current error should stop the run with the module's normal
# reporting contract.
#
# Create a temporary file and return the filename.
zxfer_throw_error() {
	l_msg=$1
	l_exit_status=${2:-1} # global used by zxfer_beep

	zxfer_init_failure_context_defaults
	[ -n "$g_zxfer_failure_class" ] || g_zxfer_failure_class=runtime
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	zxfer_warn_stderr "$l_msg"
	zxfer_beep "$l_exit_status"
	exit "$l_exit_status"
}

# Purpose: Raise the usage error through zxfer's structured failure reporting
# path.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_usage_error() {
	l_msg=$1
	l_exit_status=${2:-2} # global used by zxfer_beep
	zxfer_init_failure_context_defaults
	g_zxfer_failure_class=usage
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	if [ "$l_msg" != "" ]; then
		zxfer_warn_stderr "Error: $l_msg"
	fi
	zxfer_print_usage_to_stderr
	zxfer_beep "$l_exit_status"
	exit "$l_exit_status"
}

# Purpose: Raise the error with usage through zxfer's structured failure
# reporting path.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_error_with_usage() {
	l_msg=$1
	l_exit_status=${2:-1}

	zxfer_init_failure_context_defaults
	[ -n "$g_zxfer_failure_class" ] || g_zxfer_failure_class=runtime
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	if [ "$l_msg" != "" ]; then
		zxfer_warn_stderr "Error: $l_msg"
	fi
	zxfer_print_usage_to_stderr
	zxfer_beep "$l_exit_status"
	exit "$l_exit_status"
}

# Purpose: Emit normal verbose output only when `-v` is active.
# Usage: Called during failure reporting, profiling, and verbose operator
# output anywhere zxfer wants operator-facing progress text without enabling
# very-verbose diagnostics.
#
# sample usage:
# zxfer_execute_command "ls -l" 1
# l_cmd: command to execute
# l_is_continue_on_fail: 1 to continue on fail, 0 to stop on fail
zxfer_echov() {
	if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
		echo "$@"
	fi
}

# Purpose: Emit very-verbose diagnostic output only when `-V` is active.
# Usage: Called during failure reporting, profiling, and verbose operator
# output for low-level debug messages that should stay hidden in normal verbose
# mode.
#
# Very verbose mode - print message to standard error
zxfer_echoV() {
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		echo "$@" >&2
	fi
}

# Purpose: Trigger the configured beep behavior for success or failure
# notifications.
# Usage: Called during failure reporting, profiling, and verbose operator
# output at the end of a run when the operator requested an audible alert.
#
# Beeps a success sound if -B enabled, and a failure sound if -b or -B enabled.
zxfer_beep() {
	l_exit_status=${1:-1} # default to 1 (failure)

	if [ "${g_option_b_beep_always:-0}" -ne 1 ] && [ "${g_option_B_beep_on_success:-0}" -ne 1 ]; then
		return
	fi

	# Speaker control is FreeBSD-specific; skip on other hosts so replication continues.
	l_os=$(uname 2>/dev/null || echo "unknown")
	if [ "$l_os" != "FreeBSD" ]; then
		zxfer_echoV "Beep requested but unsupported on $l_os; skipping."
		return
	fi

	if ! command -v kldstat >/dev/null 2>&1 || ! command -v kldload >/dev/null 2>&1; then
		zxfer_echoV "Beep requested but speaker tools are missing; skipping."
		return
	fi

	if ! [ -c /dev/speaker ]; then
		zxfer_echoV "Beep requested but /dev/speaker missing; skipping."
		return
	fi

	# load the speaker kernel module if not loaded already
	l_speaker_km_loaded=$(kldstat | grep -c speaker.ko)
	if [ "$l_speaker_km_loaded" = "0" ]; then
		if ! kldload "speaker" >/dev/null 2>&1; then
			zxfer_echoV "Unable to load speaker module; skipping beep."
			return
		fi
	fi

	# play the appropriate beep
	if [ "$l_exit_status" -eq 0 ]; then
		if [ "$g_option_B_beep_on_success" -eq 1 ]; then
			echo "T255CCMLEG~EG..." >/dev/speaker 2>/dev/null ||
				zxfer_echoV "Success beep failed; skipping."
		fi
	else
		echo "T150A<C.." >/dev/speaker 2>/dev/null ||
			zxfer_echoV "Failure beep failed; skipping."
	fi
}
