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
# REPORTING / FAILURE HANDLING
################################################################################

# Module contract:
# owns globals: g_zxfer_failure_* structured failure context.
# reads globals: g_option_* verbosity/beep flags, g_cmd_awk, and current dataset context.
# mutates caches: none.
# returns via stdout: escaped values and rendered failure reports.

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

# Purpose: Publish the launcher-captured invocation through the reporting owner.
# Usage: Called once after the pure module load and before session initialization
# resets the remaining failure context.
zxfer_set_original_invocation() {
	g_zxfer_original_invocation=${1:-}
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

# Purpose: Check whether an operator-facing command rendering has any consumer.
# Usage: Called before display-only command rendering so quiet runs perform
# zero renders; verbose output (-v/-V) and unsafe failure-report command
# fields are the only consumers of rendered command strings.
zxfer_command_display_render_enabled() {
	[ "${g_option_v_verbose:-0}" -eq 1 ] && return 0
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] && return 0
	zxfer_failure_report_uses_unsafe_command_fields
}

# Purpose: Record the redacted failure-context marker without rendering the
# command.
# Usage: Called on quiet paths instead of zxfer_record_last_command_string so
# skipped display renders still leave the same redacted last_command field.
zxfer_record_last_command_opaque() {
	zxfer_init_failure_context_defaults
	# Assign the marker inline so hot exec paths skip a command substitution;
	# must match zxfer_get_failure_report_redaction_marker.
	g_zxfer_failure_last_command="[redacted]"
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

# Purpose: Set the structured failure class through its owning module.
# Usage: Called immediately before throwing an error whose category is more
# specific than the runtime default.
zxfer_set_failure_class() {
	zxfer_init_failure_context_defaults
	case ${1:-} in
	usage | dependency | runtime | '')
		g_zxfer_failure_class=${1:-}
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Set the operator-facing structured failure message.
# Usage: Called by composition cleanup when it must promote a cleanup failure
# without invoking a throwing helper from inside the EXIT trap.
zxfer_set_failure_message() {
	zxfer_init_failure_context_defaults
	g_zxfer_failure_message=${1:-}
}

# Purpose: Publish a complete cleanup failure only when no earlier failure
# message already owns the diagnostic context.
# Usage: Keeps EXIT cleanup precedence and the established first-failure rule
# in one owner operation.
zxfer_set_failure_context_if_empty() {
	zxfer_init_failure_context_defaults
	[ -z "${g_zxfer_failure_message:-}" ] || return 0
	zxfer_set_failure_class "${1:-runtime}" || return 1
	g_zxfer_failure_stage=${2:-trap cleanup}
	g_zxfer_failure_message=${3:-}
}

# Purpose: Mark the current structured failure report as emitted.
# Usage: Called by secure error-log coordination after stderr rendering and
# before optional mirroring, preventing duplicate reports during later cleanup.
zxfer_mark_failure_report_emitted() {
	zxfer_init_failure_context_defaults
	g_zxfer_failure_report_emitted=1
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
	elif zxfer_failure_report_uses_unsafe_command_fields; then
		g_zxfer_failure_last_command=$(zxfer_escape_report_value "$1")
	else
		zxfer_record_last_command_opaque
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
	elif zxfer_failure_report_uses_unsafe_command_fields; then
		g_zxfer_failure_last_command=$(zxfer_quote_command_argv "$@")
	else
		zxfer_record_last_command_opaque
	fi
}

# Purpose: Emit the usage to stderr in the operator-facing format owned by this
# module.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when zxfer needs to surface status, warning, or diagnostic text.
zxfer_print_usage_to_stderr() {
	zxfer_usage >&2
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

# Purpose: Raise the error through zxfer's structured failure reporting path.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the current error should stop the run with the module's normal
# reporting contract.
#
# Create a temporary file and return the filename.
zxfer_throw_error() {
	l_msg=$1
	l_throw_error_exit_status=${2:-1} # global used by zxfer_beep

	zxfer_init_failure_context_defaults
	[ -n "$g_zxfer_failure_class" ] || g_zxfer_failure_class=runtime
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	zxfer_warn_stderr "$l_msg"
	zxfer_beep "$l_throw_error_exit_status"
	exit "$l_throw_error_exit_status"
}

# Purpose: Raise the usage error through zxfer's structured failure reporting
# path.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_usage_error() {
	l_msg=$1
	l_throw_usage_error_exit_status=${2:-2} # global used by zxfer_beep
	zxfer_init_failure_context_defaults
	g_zxfer_failure_class=usage
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	if [ "$l_msg" != "" ]; then
		zxfer_warn_stderr "Error: $l_msg"
	fi
	zxfer_print_usage_to_stderr
	zxfer_beep "$l_throw_usage_error_exit_status"
	exit "$l_throw_usage_error_exit_status"
}

# Purpose: Raise the error with usage through zxfer's structured failure
# reporting path.
# Usage: Called during failure reporting, profiling, and verbose operator
# output when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_error_with_usage() {
	l_msg=$1
	l_throw_error_with_usage_exit_status=${2:-1}

	zxfer_init_failure_context_defaults
	[ -n "$g_zxfer_failure_class" ] || g_zxfer_failure_class=runtime
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	if [ "$l_msg" != "" ]; then
		zxfer_warn_stderr "Error: $l_msg"
	fi
	zxfer_print_usage_to_stderr
	zxfer_beep "$l_throw_error_with_usage_exit_status"
	exit "$l_throw_error_with_usage_exit_status"
}

# Purpose: Emit normal verbose output only when `-v` is active.
# Usage: Called during failure reporting, profiling, and verbose operator
# output anywhere zxfer wants operator-facing progress text without enabling
# very-verbose diagnostics.
#
# sample usage:
# zxfer_execute_rendered_shell_command "ls -l" 1
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
