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

zxfer_warn_stderr() {
	printf '%s\n' "$*" >&2
}

zxfer_escape_report_value() {
	# shellcheck disable=SC2016
	printf '%s' "$1" | ${g_cmd_awk:-awk} '
BEGIN {
	ORS = ""
}
{
	if (NR > 1) {
		printf "\\n"
	}
	line = $0
	gsub(/\\/, "\\\\", line)
	gsub(/\t/, "\\t", line)
	gsub(/\r/, "\\r", line)
	printf "%s", line
}
'
}

zxfer_quote_token_for_report() {
	l_value_escaped=$(zxfer_escape_report_value "$1")
	l_value_safe=$(printf '%s' "$l_value_escaped" | sed "s/'/'\"'\"'/g")
	printf "'%s'" "$l_value_safe"
}

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

zxfer_set_failure_stage() {
	zxfer_init_failure_context_defaults
	[ -n "$1" ] && g_zxfer_failure_stage=$1
}

zxfer_set_failure_roots() {
	zxfer_init_failure_context_defaults
	[ $# -ge 1 ] && [ -n "$1" ] && g_zxfer_failure_source_root=$1
	[ $# -ge 2 ] && [ -n "$2" ] && g_zxfer_failure_destination_root=$2
}

zxfer_set_current_dataset_context() {
	zxfer_init_failure_context_defaults
	[ $# -ge 1 ] && [ -n "$1" ] && g_zxfer_failure_current_source=$1
	[ $# -ge 2 ] && [ -n "$2" ] && g_zxfer_failure_current_destination=$2
}

zxfer_record_last_command_string() {
	zxfer_init_failure_context_defaults
	g_zxfer_failure_last_command=$1
}

zxfer_record_last_command_argv() {
	zxfer_init_failure_context_defaults
	g_zxfer_failure_last_command=$(zxfer_quote_command_argv "$@")
}

zxfer_profile_metrics_enabled() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ]
}

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

zxfer_print_usage_to_stderr() {
	if command -v zxfer_usage >/dev/null 2>&1; then
		zxfer_usage >&2
	fi
}

zxfer_get_failure_mode_label() {
	if [ -n "${g_option_R_recursive:-}" ]; then
		printf 'recursive\n'
	elif [ -n "${g_option_N_nonrecursive:-}" ]; then
		printf 'nonrecursive\n'
	fi
}

zxfer_append_report_field() {
	l_key=$1
	l_value=$2

	[ -n "$l_value" ] || return
	printf '%s: %s\n' "$l_key" "$(zxfer_escape_report_value "$l_value")"
}

zxfer_render_failure_report() {
	l_exit_status=$1

	zxfer_init_failure_context_defaults

	l_timestamp=$(date '+%Y-%m-%dT%H:%M:%S%z' 2>/dev/null || date)
	l_hostname=$(uname -n 2>/dev/null || hostname 2>/dev/null || echo unknown)
	l_failure_class=$g_zxfer_failure_class
	l_failure_message=$g_zxfer_failure_message
	l_failure_stage=$g_zxfer_failure_stage
	l_mode=$(zxfer_get_failure_mode_label)

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
	zxfer_append_report_field invocation "${g_zxfer_original_invocation:-}"
	zxfer_append_report_field last_command "${g_zxfer_failure_last_command:-}"
	printf 'zxfer: failure report end\n'
}

zxfer_get_error_log_parent_dir() {
	l_path=$1
	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ] || [ "$l_parent" = "" ]; then
		l_parent=/
	fi
	printf '%s\n' "$l_parent"
}
zxfer_create_error_log_file() {
	l_log_path=$1

	# Avoid using a redirection on the special builtin ":" in the current shell.
	# On dash, a permission-denied redirection for a special builtin is a fatal
	# shell error, which can abort the caller before we emit the warning.
	(
		umask 077
		: >"$l_log_path"
	)
}

zxfer_chmod_error_log_file() {
	l_log_path=$1

	chmod 600 "$l_log_path"
}

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

	if [ -L "$l_log_path" ] || [ -h "$l_log_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_log_path\" because it is a symlink."
		return 1
	fi

	if [ -e "$l_log_path" ] && [ ! -f "$l_log_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_log_path\" because it is not a regular file."
		return 1
	fi

	if [ -e "$l_log_path" ]; then
		if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_log_path"); then
			zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_log_path\" because its owner could not be determined."
			return 1
		fi
		if ! zxfer_backup_owner_uid_is_allowed "$l_owner_uid"; then
			l_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
			zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_log_path\" because it is owned by UID $l_owner_uid instead of $l_expected_owner_desc."
			return 1
		fi
		if ! l_mode=$(zxfer_get_path_mode_octal "$l_log_path"); then
			zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_log_path\" because its permissions could not be determined."
			return 1
		fi
		if [ "$l_mode" != "600" ]; then
			zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_log_path\" because its permissions ($l_mode) are not 0600."
			return 1
		fi
	else
		if ! zxfer_create_error_log_file "$l_log_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_log_path\"."
			return 1
		fi
		if ! zxfer_chmod_error_log_file "$l_log_path"; then
			zxfer_warn_stderr "zxfer: warning: unable to chmod ZXFER_ERROR_LOG file \"$l_log_path\" to 0600."
			return 1
		fi
	fi

	if ! printf '%s\n' "$l_report" >>"$l_log_path"; then
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_log_path\"."
		return 1
	fi
}

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

#
# Create a temporary file and return the filename.
#
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

# sample usage:
# zxfer_execute_command "ls -l" 1
# l_cmd: command to execute
# l_is_continue_on_fail: 1 to continue on fail, 0 to stop on fail
zxfer_echov() {
	if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
		echo "$@"
	fi
}

#
# Very verbose mode - print message to standard error
#
zxfer_echoV() {
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		echo "$@" >&2
	fi
}

#
# Beeps a success sound if -B enabled, and a failure sound if -b or -B enabled.
#
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
