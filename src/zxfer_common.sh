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
# COMMON FUNCTIONS FOR ZXFER
#
# Global variables that must be defined in the calling script:
#  g_option_n_dryrun
#  g_option_v_verbose
#  g_option_V_very_verbose
#  g_option_b_beep_always
#  g_option_B_beep_on_success
################################################################################

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

zxfer_register_cleanup_pid() {
	l_pid=$1

	case "$l_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	[ "$l_pid" = "$$" ] && return 0

	for l_existing_pid in ${g_zxfer_cleanup_pids:-}; do
		[ "$l_existing_pid" = "$l_pid" ] && return 0
	done

	if [ -n "${g_zxfer_cleanup_pids:-}" ]; then
		g_zxfer_cleanup_pids="$g_zxfer_cleanup_pids $l_pid"
	else
		g_zxfer_cleanup_pids=$l_pid
	fi
}

zxfer_unregister_cleanup_pid() {
	l_pid=$1
	l_remaining_pids=""

	case "$l_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	for l_existing_pid in ${g_zxfer_cleanup_pids:-}; do
		[ "$l_existing_pid" = "$l_pid" ] && continue
		if [ -n "$l_remaining_pids" ]; then
			l_remaining_pids="$l_remaining_pids $l_existing_pid"
		else
			l_remaining_pids=$l_existing_pid
		fi
	done

	g_zxfer_cleanup_pids=$l_remaining_pids
}

zxfer_kill_registered_cleanup_pids() {
	for l_pid in ${g_zxfer_cleanup_pids:-}; do
		case "$l_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		[ "$l_pid" = "$$" ] && continue
		kill "$l_pid" 2>/dev/null || true
	done

	g_zxfer_cleanup_pids=""
}

zxfer_print_usage_to_stderr() {
	if command -v usage >/dev/null 2>&1; then
		usage >&2
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

zxfer_find_symlink_path_component() {
	l_path=$1

	case "$l_path" in
	/*) ;;
	*)
		return 1
		;;
	esac

	l_remaining=${l_path#/}
	l_candidate_path=""
	while [ -n "$l_remaining" ]; do
		l_component=${l_remaining%%/*}
		if [ "$l_component" = "$l_remaining" ]; then
			l_remaining=""
		else
			l_remaining=${l_remaining#*/}
		fi
		[ -n "$l_component" ] || continue

		if [ "$l_candidate_path" = "" ]; then
			l_candidate_path="/$l_component"
		else
			l_candidate_path="$l_candidate_path/$l_component"
		fi

		if [ -L "$l_candidate_path" ] || [ -h "$l_candidate_path" ]; then
			printf '%s\n' "$l_candidate_path"
			return 0
		fi
	done

	return 1
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
		if ! l_owner_uid=$(get_path_owner_uid "$l_log_path"); then
			zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_log_path\" because its owner could not be determined."
			return 1
		fi
		if ! backup_owner_uid_is_allowed "$l_owner_uid"; then
			l_expected_owner_desc=$(describe_expected_backup_owner)
			zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_log_path\" because it is owned by UID $l_owner_uid instead of $l_expected_owner_desc."
			return 1
		fi
		if ! l_mode=$(get_path_mode_octal "$l_log_path"); then
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
get_temp_file() {
	l_tmpdir=${TMPDIR:-/tmp}
	# On GNU mktemp the template must include X, so build the template ourselves.
	l_prefix=${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}
	l_file=$(mktemp "$l_tmpdir/$l_prefix.XXXXXX") ||
		throw_error "Error creating temporary file."
	echoV "New temporary file: $l_file"

	# return the temp file name
	echo "$l_file"
}

#
# Gets a $(uname), i.e. the operating system, for origin or target, if remote.
# Takes: $1=either $g_option_O_origin_host or $g_option_T_target_host
#
get_os() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_output_os=""

	# Get uname of the destination (target) machine, local or remote
	if [ "$l_host_spec" = "" ]; then
		l_output_os=$(uname)
	else
		l_remote_cmd=$(build_shell_command_from_argv uname)
		l_output_os=$(invoke_ssh_shell_command_for_host "$l_host_spec" "$l_remote_cmd" "$l_profile_side")
	fi

	echo "$l_output_os"
}

#
# Function to handle errors
#
# ext status
# 0 - success
# 1 - general error
# 2 - usage error
# 3 - error that prevents the script from continuing
throw_error() {
	l_msg=$1
	l_exit_status=${2:-1} # global used by beep

	zxfer_init_failure_context_defaults
	[ -n "$g_zxfer_failure_class" ] || g_zxfer_failure_class=runtime
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	zxfer_warn_stderr "$l_msg"
	beep "$l_exit_status"
	exit "$l_exit_status"
}

throw_usage_error() {
	l_msg=$1
	l_exit_status=${2:-2} # global used by beep
	zxfer_init_failure_context_defaults
	g_zxfer_failure_class=usage
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	if [ "$l_msg" != "" ]; then
		zxfer_warn_stderr "Error: $l_msg"
	fi
	zxfer_print_usage_to_stderr
	beep "$l_exit_status"
	exit "$l_exit_status"
}

throw_error_with_usage() {
	l_msg=$1
	l_exit_status=${2:-1}

	zxfer_init_failure_context_defaults
	[ -n "$g_zxfer_failure_class" ] || g_zxfer_failure_class=runtime
	[ -n "$l_msg" ] && g_zxfer_failure_message=$l_msg
	if [ "$l_msg" != "" ]; then
		zxfer_warn_stderr "Error: $l_msg"
	fi
	zxfer_print_usage_to_stderr
	beep "$l_exit_status"
	exit "$l_exit_status"
}

# sample usage:
# execute_command "ls -l" 1
# l_cmd: command to execute
# l_is_continue_on_fail: 1 to continue on fail, 0 to stop on fail
execute_command() {
	l_cmd=$1
	l_is_continue_on_fail=${2:-0}
	l_display_cmd=${3:-$l_cmd}
	zxfer_record_last_command_string "$l_cmd"

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		echov "Dry run: $l_display_cmd"
		return
	fi

	echov "$l_display_cmd"
	if [ "$l_is_continue_on_fail" -eq 1 ]; then
		eval "$l_cmd" || {
			echo "Non-critical error when executing command. Continuing."
		}
	else
		eval "$l_cmd" || throw_error "Error when executing command."
	fi
}

#
# Execute a command in the background and write the output to a file.
# Background commands do not honor the dry run option
#
# l_cmd: command to execute
# l_output_file: file to write the output to
#
execute_background_cmd() {
	l_cmd=$1
	l_output_file=$2
	l_error_file=${3:-}

	echoV "Executing command in the background: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	if [ -n "$l_error_file" ]; then
		eval "$l_cmd" >"$l_output_file" 2>"$l_error_file" &
	else
		eval "$l_cmd" >"$l_output_file" &
	fi
	# shellcheck disable=SC2034
	g_last_background_pid=$!
	zxfer_register_cleanup_pid "$g_last_background_pid"
}

# Escape characters for a single-quoted context by closing and reopening quotes
# around embedded apostrophes.
escape_for_single_quotes() {
	printf '%s' "$1" | sed "s/'/'\\\\''/g"
}

# Split whitespace-delimited arguments into separate lines without invoking the
# shell parser. This intentionally ignores quoting so callers must escape
# metacharacters themselves, preventing shell injection attacks.
split_tokens_on_whitespace() {
	l_input=$1
	if [ "$l_input" = "" ]; then
		return
	fi

	# Ensure shell metacharacters such as ';', '|', and '&' break tokens even
	# when users omit the following whitespace, so injected commands remain
	# literal arguments instead of spawning new pipelines.
	l_normalized_input=$(printf '%s' "$l_input" | sed 's/[;|&]/& /g')

	l_awk_cmd=${g_cmd_awk:-$(command -v awk 2>/dev/null || echo awk)}
	# shellcheck disable=SC2016
	# $i references belong to awk, not the shell.
	printf '%s\n' "$l_normalized_input" | "$l_awk_cmd" '
	{
		for (i = 1; i <= NF; i++) {
			print $i
		}
	}'
}

# Split a user-supplied -O/-T host spec into tokens without invoking the shell
# parser so whitespace-separated ssh arguments (like "user@host pfexec") are
# preserved verbatim and characters such as ';' cannot escape into new commands.
split_host_spec_tokens() {
	split_tokens_on_whitespace "$1"
}

split_cli_tokens() {
	split_tokens_on_whitespace "$1"
}

quote_token_stream() {
	l_tokens=$1
	if [ "$l_tokens" = "" ]; then
		return
	fi

	l_output=""
	while IFS= read -r l_token || [ -n "$l_token" ]; do
		[ "$l_token" = "" ] && continue
		l_safe_token=$(escape_for_single_quotes "$l_token")
		if [ "$l_output" = "" ]; then
			l_output="'$l_safe_token'"
		else
			l_output="$l_output '$l_safe_token'"
		fi
	done <<EOF
$l_tokens
EOF
	printf '%s' "$l_output"
}

build_shell_command_from_argv() {
	l_output=""
	for l_arg in "$@"; do
		l_safe_arg=$(escape_for_single_quotes "$l_arg")
		if [ "$l_output" = "" ]; then
			l_output="'$l_safe_arg'"
		else
			l_output="$l_output '$l_safe_arg'"
		fi
	done

	printf '%s' "$l_output"
}

# Quote a host spec for safe reinsertion into eval'd strings by wrapping each
# token in single quotes. This keeps multi-word ssh arguments working while
# preventing the shell from interpreting metacharacters provided by the user.
quote_host_spec_tokens() {
	l_host_spec=$1
	if [ "$l_host_spec" = "" ]; then
		return
	fi

	l_tokens=$(split_host_spec_tokens "$l_host_spec")
	if [ "$l_tokens" = "" ]; then
		return
	fi

	quote_token_stream "$l_tokens"
}

quote_cli_tokens() {
	l_cli_string=$1
	if [ "$l_cli_string" = "" ]; then
		return
	fi

	l_tokens=$(split_cli_tokens "$l_cli_string")
	if [ "$l_tokens" = "" ]; then
		return
	fi

	quote_token_stream "$l_tokens"
}

# Render the ssh transport argv for a given host, including any control socket,
# as a newline-delimited token stream that can be safely re-quoted or executed.
get_ssh_transport_tokens_for_host() {
	l_host=$1

	printf '%s\n' "$g_cmd_ssh"

	if [ "$l_host" = "" ]; then
		return
	fi

	if [ "$l_host" = "$g_option_O_origin_host" ] && [ "$g_ssh_origin_control_socket" != "" ]; then
		printf '%s\n%s\n' "-S" "$g_ssh_origin_control_socket"
		return
	fi

	if [ "$l_host" = "$g_option_T_target_host" ] && [ "$g_ssh_target_control_socket" != "" ]; then
		printf '%s\n%s\n' "-S" "$g_ssh_target_control_socket"
	fi
}

# Render the local ssh transport command used for a host spec. This is a
# display helper only; execution paths should use the argv-based helpers below.
get_ssh_cmd_for_host() {
	l_host=$1
	l_transport_tokens=$(get_ssh_transport_tokens_for_host "$l_host")
	quote_token_stream "$l_transport_tokens"
}

# Expand the ssh transport and host spec into discrete arguments before
# executing the remote command so multi-token -O/-T inputs (like "host pfexec")
# are preserved without reparsing a shell string. STDIN/STDOUT/STDERR are
# passed through to the invoked ssh process.
invoke_ssh_command_for_host() {
	l_host_spec=$1
	shift

	l_transport_tokens=$(get_ssh_transport_tokens_for_host "$l_host_spec")
	l_host_tokens=$(split_host_spec_tokens "$l_host_spec")
	l_remote_args_stream=""
	if [ $# -gt 0 ]; then
		l_remote_args_stream=$(printf '%s\n' "$@")
	fi

	set --
	if [ "$l_transport_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			[ "$l_token" = "" ] && continue
			set -- "$@" "$l_token"
		done <<EOF
$l_transport_tokens
EOF
	fi

	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			[ "$l_token" = "" ] && continue
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi

	if [ "$l_remote_args_stream" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_remote_args_stream
EOF
	fi

	zxfer_record_last_command_argv "$@"
	"$@"
}

# Build a shell-ready local ssh command string while preserving any wrapper
# tokens embedded in the -O/-T host spec (for example "host pfexec"). The
# remote command must already be quoted for execution by the remote shell.
build_ssh_shell_command_for_host() {
	l_host_spec=$1
	l_remote_shell_cmd=$2

	[ "$l_remote_shell_cmd" = "" ] && return 1

	l_transport_tokens=$(get_ssh_transport_tokens_for_host "$l_host_spec")
	l_host_tokens=$(split_host_spec_tokens "$l_host_spec")
	[ "$l_host_tokens" != "" ] || return 1

	l_ssh_host=""
	l_wrapper_tokens=""
	while IFS= read -r l_token || [ -n "$l_token" ]; do
		[ "$l_token" = "" ] && continue
		if [ "$l_ssh_host" = "" ]; then
			l_ssh_host=$l_token
		elif [ "$l_wrapper_tokens" = "" ]; then
			l_wrapper_tokens=$l_token
		else
			l_wrapper_tokens="$l_wrapper_tokens
$l_token"
		fi
	done <<EOF
$l_host_tokens
EOF

	[ "$l_ssh_host" != "" ] || return 1

	l_full_remote_cmd=$l_remote_shell_cmd
	if [ "$l_wrapper_tokens" != "" ]; then
		l_wrapper_cmd=$(quote_token_stream "$l_wrapper_tokens")
		l_full_remote_cmd="$l_wrapper_cmd $l_remote_shell_cmd"
	fi

	l_command_tokens=$(printf '%s\n%s\n%s\n' "$l_transport_tokens" "$l_ssh_host" "$l_full_remote_cmd")
	quote_token_stream "$l_command_tokens"
}

# Execute a shell-ready remote command string through ssh without reparsing a
# local shell string. Wrapper tokens embedded in the -O/-T host spec are
# preserved as part of the single remote command argument.
invoke_ssh_shell_command_for_host() {
	l_host_spec=$1
	l_remote_shell_cmd=$2
	l_profile_side=${3:-}

	[ "$l_remote_shell_cmd" = "" ] && return 1
	zxfer_profile_record_ssh_invocation "$l_host_spec" "$l_profile_side"

	l_transport_tokens=$(get_ssh_transport_tokens_for_host "$l_host_spec")
	l_host_tokens=$(split_host_spec_tokens "$l_host_spec")
	[ "$l_host_tokens" != "" ] || return 1

	l_ssh_host=""
	l_wrapper_tokens=""
	while IFS= read -r l_token || [ -n "$l_token" ]; do
		[ "$l_token" = "" ] && continue
		if [ "$l_ssh_host" = "" ]; then
			l_ssh_host=$l_token
		elif [ "$l_wrapper_tokens" = "" ]; then
			l_wrapper_tokens=$l_token
		else
			l_wrapper_tokens="$l_wrapper_tokens
$l_token"
		fi
	done <<EOF
$l_host_tokens
EOF

	[ "$l_ssh_host" != "" ] || return 1

	l_full_remote_cmd=$l_remote_shell_cmd
	if [ "$l_wrapper_tokens" != "" ]; then
		l_wrapper_cmd=$(quote_token_stream "$l_wrapper_tokens")
		l_full_remote_cmd="$l_wrapper_cmd $l_remote_shell_cmd"
	fi

	set --
	if [ "$l_transport_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			[ "$l_token" = "" ] && continue
			set -- "$@" "$l_token"
		done <<EOF
$l_transport_tokens
EOF
	fi
	set -- "$@" "$l_ssh_host" "$l_full_remote_cmd"

	zxfer_record_last_command_argv "$@"
	"$@"
}

build_remote_sh_c_command() {
	l_remote_script=$1
	l_remote_tokens=$(printf '%s\n%s\n%s\n' "sh" "-c" "$l_remote_script")
	quote_token_stream "$l_remote_tokens"
}

# Execute a zfs command on the origin (source) host, transparently invoking
# ssh when -O is in effect so callers can treat this like a local command.
run_source_zfs_cmd() {
	zxfer_profile_record_zfs_call source "$1"

	if [ "$g_option_O_origin_host" = "" ]; then
		if [ -n "$g_LZFS" ] && [ "$g_LZFS" != "$g_cmd_zfs" ]; then
			zxfer_record_last_command_argv "$g_LZFS" "$@"
			"$g_LZFS" "$@"
		else
			zxfer_record_last_command_argv "$g_cmd_zfs" "$@"
			"$g_cmd_zfs" "$@"
		fi
		return
	fi

	l_origin_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n' "$l_origin_zfs_cmd")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(quote_token_stream "$l_remote_tokens")
	invoke_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_cmd" source
}

# Execute a zfs command on the destination (target) host, using ssh when -T is
# active so shell quoting does not leak into the remote hostname.
run_destination_zfs_cmd() {
	zxfer_profile_record_zfs_call destination "$1"

	if [ "$g_option_T_target_host" = "" ]; then
		if [ -n "$g_RZFS" ] && [ "$g_RZFS" != "$g_cmd_zfs" ]; then
			zxfer_record_last_command_argv "$g_RZFS" "$@"
			"$g_RZFS" "$@"
		else
			zxfer_record_last_command_argv "$g_cmd_zfs" "$@"
			"$g_cmd_zfs" "$@"
		fi
		return
	fi

	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n' "$l_target_zfs_cmd")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(quote_token_stream "$l_remote_tokens")
	invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd" destination
}

zxfer_render_source_zfs_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_O_origin_host" = "" ]; then
		l_source_zfs_cmd=$g_cmd_zfs
		if [ -n "$g_LZFS" ] && [ "$g_LZFS" != "$g_cmd_zfs" ]; then
			l_source_zfs_cmd=$g_LZFS
		fi
		build_shell_command_from_argv "$l_source_zfs_cmd" "$l_subcommand" "$@"
		return
	fi

	l_origin_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_origin_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(quote_token_stream "$l_remote_tokens")
	build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_cmd"
}

zxfer_render_destination_zfs_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_T_target_host" = "" ]; then
		l_target_zfs_cmd=$g_cmd_zfs
		if [ -n "$g_RZFS" ] && [ "$g_RZFS" != "$g_cmd_zfs" ]; then
			l_target_zfs_cmd=$g_RZFS
		fi
		build_shell_command_from_argv "$l_target_zfs_cmd" "$l_subcommand" "$@"
		return
	fi

	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_target_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(quote_token_stream "$l_remote_tokens")
	build_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd"
}

zxfer_render_zfs_command_for_spec() {
	l_cmd_spec=$1
	shift

	if [ "$l_cmd_spec" = "$g_LZFS" ]; then
		zxfer_render_source_zfs_command "$@"
	elif [ "$l_cmd_spec" = "$g_RZFS" ]; then
		zxfer_render_destination_zfs_command "$@"
	else
		build_shell_command_from_argv "$l_cmd_spec" "$@"
	fi
}

# Run a zfs command based on the provided command specifier, delegating to the
# source or destination helper when the spec references $g_LZFS or $g_RZFS.
run_zfs_cmd_for_spec() {
	l_cmd_spec=$1
	shift

	if [ "$l_cmd_spec" = "$g_LZFS" ]; then
		run_source_zfs_cmd "$@"
	elif [ "$l_cmd_spec" = "$g_RZFS" ]; then
		run_destination_zfs_cmd "$@"
	else
		zxfer_profile_record_zfs_call other "$1"
		zxfer_record_last_command_argv "$l_cmd_spec" "$@"
		"$l_cmd_spec" "$@"
	fi
}

# Remove trailing slash characters from dataset-like arguments while leaving
# strings that consist entirely of '/' untouched so callers can still reject
# absolute paths explicitly.
strip_trailing_slashes() {
	l_path=$1

	case "$l_path" in
	*[!/]*) ;;
	*)
		printf '%s\n' "$l_path"
		return
		;;
	esac

	while [ "${l_path%/}" != "$l_path" ]; do
		l_path=${l_path%/}
	done

	printf '%s\n' "$l_path"
}

zxfer_destination_probe_reports_missing() {
	l_probe_err=$1

	case "$l_probe_err" in
	*"dataset does not exist"* | *"Dataset does not exist"* | *"no such dataset"* | *"No such dataset"*)
		return 0
		;;
	esac

	return 1
}

#
# Checks whether the destination dataset exists.
# Prints 1 when it exists, 0 when it is explicitly missing, and returns non-zero
# with an explanatory message when the probe itself fails.
#
exists_destination() {
	l_dest=$1
	zxfer_profile_increment_counter g_zxfer_profile_exists_destination_calls

	l_cmd=$(zxfer_render_destination_zfs_command list -H "$l_dest")
	echoV "Checking if destination exists: $l_cmd"

	if l_probe_err=$(run_destination_zfs_cmd list -H "$l_dest" 2>&1 >/dev/null); then
		printf '%s\n' 1
		return 0
	fi

	if zxfer_destination_probe_reports_missing "$l_probe_err"; then
		printf '%s\n' 0
		return 0
	fi

	if [ -n "$l_probe_err" ]; then
		printf 'Failed to determine whether destination dataset [%s] exists: %s\n' "$l_dest" "$l_probe_err"
	else
		printf 'Failed to determine whether destination dataset [%s] exists.\n' "$l_dest"
	fi
	return 1
}

#
# Print out information if in verbose mode
#
echov() {
	if [ "$g_option_v_verbose" -eq 1 ]; then
		echo "$@"
	fi
}

#
# Very verbose mode - print message to standard error
#
echoV() {
	if [ "$g_option_V_very_verbose" -eq 1 ]; then
		echo "$@" >&2
	fi
}

#
# Beeps a success sound if -B enabled, and a failure sound if -b or -B enabled.
#
beep() {
	l_exit_status=${1:-1} # default to 1 (failure)

	if [ "$g_option_b_beep_always" -ne 1 ] && [ "$g_option_B_beep_on_success" -ne 1 ]; then
		return
	fi

	# Speaker control is FreeBSD-specific; skip on other hosts so replication continues.
	l_os=$(uname 2>/dev/null || echo "unknown")
	if [ "$l_os" != "FreeBSD" ]; then
		echoV "Beep requested but unsupported on $l_os; skipping."
		return
	fi

	if ! command -v kldstat >/dev/null 2>&1 || ! command -v kldload >/dev/null 2>&1; then
		echoV "Beep requested but speaker tools are missing; skipping."
		return
	fi

	if ! [ -c /dev/speaker ]; then
		echoV "Beep requested but /dev/speaker missing; skipping."
		return
	fi

	# load the speaker kernel module if not loaded already
	l_speaker_km_loaded=$(kldstat | grep -c speaker.ko)
	if [ "$l_speaker_km_loaded" = "0" ]; then
		if ! kldload "speaker" >/dev/null 2>&1; then
			echoV "Unable to load speaker module; skipping beep."
			return
		fi
	fi

	# play the appropriate beep
	if [ "$l_exit_status" -eq 0 ]; then
		if [ "$g_option_B_beep_on_success" -eq 1 ]; then
			echo "T255CCMLEG~EG..." >/dev/speaker 2>/dev/null ||
				echoV "Success beep failed; skipping."
		fi
	else
		echo "T150A<C.." >/dev/speaker 2>/dev/null ||
			echoV "Failure beep failed; skipping."
	fi
}
