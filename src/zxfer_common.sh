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

#
# Add the debug_start() and debug_end() functions to enable/disable debugging
# between code blocks.
debug_start() {
	set -x
}

debug_end() {
	set +x
}

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

zxfer_record_failure() {
	l_failure_class=$1
	l_failure_message=$2

	zxfer_init_failure_context_defaults

	[ -n "$l_failure_class" ] && g_zxfer_failure_class=$l_failure_class
	[ -n "$l_failure_message" ] && g_zxfer_failure_message=$l_failure_message
}

zxfer_record_last_command_string() {
	zxfer_init_failure_context_defaults
	g_zxfer_failure_last_command=$1
}

zxfer_record_last_command_argv() {
	zxfer_init_failure_context_defaults
	g_zxfer_failure_last_command=$(zxfer_quote_command_argv "$@")
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
		# Avoid using a redirection on the special builtin ":" in the current shell.
		# On dash, a permission-denied redirection for a special builtin is a fatal
		# shell error, which can abort the caller before we emit the warning.
		if ! (
			umask 077
			: >"$l_log_path"
		); then
			zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_log_path\"."
			return 1
		fi
		if ! chmod 600 "$l_log_path"; then
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
	l_input_options=$1
	l_output_os=""

	# Get uname of the destination (target) machine, local or remote
	if [ "$l_input_options" = "" ]; then
		l_output_os=$(uname)
	else
		l_cmd="$l_input_options uname"
		l_output_os=$(eval "$l_cmd")
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
	zxfer_record_last_command_string "$l_cmd"

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		echov "Dry run: $l_cmd"
		return
	fi

	echov "$l_cmd"
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
}

# Escape characters that have special meaning inside double quotes so that the
# returned string can be safely reinserted into a double-quoted context without
# triggering command substitution or other expansions.
escape_for_double_quotes() {
	printf '%s' "$1" | sed 's/[\\$`\"]/\\&/g'
}

# Escape characters for a single-quoted context by closing and reopening quotes
# around embedded apostrophes.
escape_for_single_quotes() {
	printf '%s' "$1" | sed "s/'/'\"'\"'/g"
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

# Expand a composed ssh command and host spec into discrete arguments before
# executing the remote command so multi-token -O/-T inputs (like "host pfexec")
# are not collapsed into a single hostname. STDIN/STDOUT/STDERR are passed
# through to the invoked ssh process.
invoke_ssh_command_for_host() {
	l_ssh_cmd=$1
	l_host_spec=$2
	shift 2

	[ "$l_ssh_cmd" = "" ] && return 1

	if [ $# -gt 0 ]; then
		l_remote_args_stream=$(printf '%s\n' "$@")
	else
		l_remote_args_stream=""
	fi

	l_inner_remote_stream=$l_remote_args_stream
	l_ssh_tokens=$(split_cli_tokens "$l_ssh_cmd")
	set --
	if [ "$l_ssh_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_ssh_tokens
EOF
	else
		set -- "$l_ssh_cmd"
	fi

	l_host_tokens=$(split_host_spec_tokens "$l_host_spec")
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi

	if [ "$l_inner_remote_stream" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_inner_remote_stream
EOF
	fi

	zxfer_record_last_command_argv "$@"
	"$@"
}

# Execute a shell-ready remote command string through ssh while preserving any
# wrapper tokens embedded in the -O/-T host spec (for example "host pfexec").
# The remote command must already be quoted for execution by the remote shell.
build_ssh_shell_command_for_host() {
	l_ssh_cmd=$1
	l_host_spec=$2
	l_remote_shell_cmd=$3

	[ "$l_ssh_cmd" = "" ] && return 1
	[ "$l_remote_shell_cmd" = "" ] && return 1

	l_ssh_tokens=$(split_cli_tokens "$l_ssh_cmd")
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

	l_command_tokens=$(printf '%s\n%s\n%s\n' "$l_ssh_tokens" "$l_ssh_host" "$l_full_remote_cmd")
	quote_token_stream "$l_command_tokens"
}

# Execute a shell-ready remote command string through ssh while preserving any
# wrapper tokens embedded in the -O/-T host spec (for example "host pfexec").
# The remote command must already be quoted for execution by the remote shell.
invoke_ssh_shell_command_for_host() {
	l_ssh_cmd=$1
	l_host_spec=$2
	l_remote_shell_cmd=$3

	[ "$l_ssh_cmd" = "" ] && return 1
	[ "$l_remote_shell_cmd" = "" ] && return 1

	l_local_cmd=$(build_ssh_shell_command_for_host "$l_ssh_cmd" "$l_host_spec" "$l_remote_shell_cmd") || return 1

	zxfer_record_last_command_string "$l_local_cmd"
	eval "$l_local_cmd"
}

build_remote_sh_c_command() {
	l_remote_script=$1
	l_remote_tokens=$(printf '%s\n%s\n%s\n' "sh" "-c" "$l_remote_script")
	quote_token_stream "$l_remote_tokens"
}

# Execute a zfs command on the origin (source) host, transparently invoking
# ssh when -O is in effect so callers can treat this like a local command.
run_source_zfs_cmd() {
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

	l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_O_origin_host")
	l_origin_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	invoke_ssh_command_for_host "$l_origin_ssh_cmd" "$g_option_O_origin_host" "$l_origin_zfs_cmd" "$@"
}

# Execute a zfs command on the destination (target) host, using ssh when -T is
# active so shell quoting does not leak into the remote hostname.
run_destination_zfs_cmd() {
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

	l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	invoke_ssh_command_for_host "$l_target_ssh_cmd" "$g_option_T_target_host" "$l_target_zfs_cmd" "$@"
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

#
# Checks if the destination dataset exists, returns 1 if it does, 0 if it does not.
#
exists_destination() {
	l_dest=$1

	# Check if the destination dataset exists
	# quote the command in case it is being run within an ssh command
	l_cmd="$g_RZFS list -H $l_dest"
	echoV "Checking if destination exists: $l_cmd"

	if eval "$l_cmd" >/dev/null 2>&1; then
		echo 1
	else
		echo 0
	fi
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
