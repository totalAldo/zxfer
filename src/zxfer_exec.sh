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
# COMMAND RENDERING / EXECUTION HELPERS
################################################################################

# Module contract:
# owns globals: g_last_background_pid.
# reads globals: g_option_* verbosity/dry-run flags, g_cmd_ssh, g_LZFS/g_RZFS, and current dataset context.
# mutates caches: destination-existence cache and cleanup PID tracking through shared helpers.
# returns via stdout: quoted tokens, rendered commands, remote outputs, and destination probes.

# Purpose: Run a foreground command through zxfer's dry-run, reporting, and
# failure-context wrapper.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# a helper wants one shared execution path for live and dry-run commands.
zxfer_execute_command() {
	l_cmd=$1
	l_is_continue_on_fail=${2:-0}
	l_display_cmd=${3:-$l_cmd}
	zxfer_record_last_command_string "$l_cmd"

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $l_display_cmd"
		return
	fi

	zxfer_echov "$l_display_cmd"
	if [ "$l_is_continue_on_fail" -eq 1 ]; then
		eval "$l_cmd" || {
			echo "Non-critical error when executing command. Continuing."
		}
	else
		eval "$l_cmd" || zxfer_throw_error "Error when executing command."
	fi
}

# Purpose: Launch a background command and capture its output through the
# staging path expected by later helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# discovery or probe flows need asynchronous execution without losing the
# checked output file contract.
#
# Execute a command in the background and write the output to a file.
# Dry-run callers receive empty placeholder files so later tempfile consumers
# can continue without executing the background probe.
#
# l_cmd: command to execute
# l_output_file: file to write the output to
zxfer_execute_background_cmd() {
	l_cmd=$1
	l_output_file=$2
	l_error_file=${3:-}

	zxfer_echoV "Executing command in the background: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: $l_cmd"
		g_last_background_pid=""
		if ! zxfer_write_runtime_artifact_file "$l_output_file" ""; then
			return 1
		fi
		if [ -n "$l_error_file" ]; then
			if ! zxfer_write_runtime_artifact_file "$l_error_file" ""; then
				zxfer_cleanup_runtime_artifact_path "$l_output_file"
				return 1
			fi
		fi
		return 0
	fi
	if ! l_cleanup_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path); then
		return 1
	fi
	if [ -n "$l_error_file" ]; then
		"$l_cleanup_wrapper_script" "$l_cmd" >"$l_output_file" 2>"$l_error_file" &
	else
		"$l_cleanup_wrapper_script" "$l_cmd" >"$l_output_file" &
	fi
	# shellcheck disable=SC2034
	g_last_background_pid=$!
	if ! zxfer_register_cleanup_pid "$g_last_background_pid" "background command helper"; then
		if kill -s 0 "$g_last_background_pid" 2>/dev/null; then
			if ! zxfer_abort_direct_child_pid "$g_last_background_pid" TERM "background command helper"; then
				g_last_background_pid=""
				return 1
			fi
			wait "$g_last_background_pid" 2>/dev/null || :
		fi
		g_last_background_pid=""
		return 1
	fi
}

# Purpose: Escape a value for reinsertion into a single-quoted shell string.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before values are embedded in rendered shell commands or remote helper
# payloads.
#
# Escape characters for a single-quoted context by closing and reopening quotes
# around embedded apostrophes.
zxfer_escape_for_single_quotes() {
	printf '%s' "$1" | sed "s/'/'\\\\''/g"
}

# Purpose: Split the tokens on whitespace into the token stream expected by
# later helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer must preserve argument boundaries without invoking a shell parser.
#
# Split whitespace-delimited arguments into separate lines without invoking the
# shell parser. This intentionally ignores quoting so callers must escape
# metacharacters themselves, preventing shell injection attacks.
zxfer_split_tokens_on_whitespace() {
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

# Purpose: Reject token strings that would require shell parsing semantics
# zxfer does not implement.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before public string interfaces are re-tokenized into argv-like streams.
zxfer_validate_literal_token_string() {
	l_input=$1
	l_label=${2:-command}

	case "$l_input" in
	*\\* | *\"* | *\'*)
		printf '%s\n' "$l_label must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
		return 1
		;;
	esac

	printf '%s\n' "$l_input"
}

# Purpose: Split the host spec tokens into the token stream expected by later
# helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer must preserve argument boundaries without invoking a shell parser.
#
# Split a user-supplied -O/-T host spec into tokens without invoking the shell
# parser so whitespace-separated ssh arguments (like "user@host pfexec") are
# preserved verbatim and characters such as ';' cannot escape into new commands.
zxfer_split_host_spec_tokens() {
	if ! l_host_spec=$(zxfer_validate_literal_token_string "$1" "Host spec (-O/-T)"); then
		printf '%s\n' "$l_host_spec"
		return 1
	fi

	zxfer_split_tokens_on_whitespace "$l_host_spec"
}

# Purpose: Split the CLI tokens into the token stream expected by later
# helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer must preserve argument boundaries without invoking a shell parser.
zxfer_split_cli_tokens() {
	l_cli_string=$1
	l_label=${2:-CLI command}

	if ! l_cli_string=$(zxfer_validate_literal_token_string "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_string"
		return 1
	fi

	zxfer_split_tokens_on_whitespace "$l_cli_string"
}

# Purpose: Quote the token stream for the shell or report format used by zxfer.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# raw tokens must be preserved without reopening parsing or injection risks.
zxfer_quote_token_stream() {
	l_tokens=$1
	if [ "$l_tokens" = "" ]; then
		return
	fi

	l_output=""
	while IFS= read -r l_token || [ -n "$l_token" ]; do
		[ "$l_token" = "" ] && continue
		l_safe_token=$(zxfer_escape_for_single_quotes "$l_token")
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

# Purpose: Build the shell command from argv for the next execution or
# comparison step.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before other helpers consume the assembled value.
zxfer_build_shell_command_from_argv() {
	l_output=""
	for l_arg in "$@"; do
		l_safe_arg=$(zxfer_escape_for_single_quotes "$l_arg")
		if [ "$l_output" = "" ]; then
			l_output="'$l_safe_arg'"
		else
			l_output="$l_output '$l_safe_arg'"
		fi
	done

	printf '%s' "$l_output"
}

# Purpose: Quote the host spec tokens for the shell or report format used by
# zxfer.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# raw tokens must be preserved without reopening parsing or injection risks.
#
# Quote a host spec for safe reinsertion into eval'd strings by wrapping each
# token in single quotes. This keeps multi-word ssh arguments working while
# preventing the shell from interpreting metacharacters provided by the user.
zxfer_quote_host_spec_tokens() {
	l_host_spec=$1
	if [ "$l_host_spec" = "" ]; then
		return
	fi

	if ! l_tokens=$(zxfer_split_host_spec_tokens "$l_host_spec"); then
		printf '%s\n' "$l_tokens"
		return 1
	fi
	if [ "$l_tokens" = "" ]; then
		return
	fi

	zxfer_quote_token_stream "$l_tokens"
}

# Purpose: Quote the CLI tokens for the shell or report format used by zxfer.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# raw tokens must be preserved without reopening parsing or injection risks.
zxfer_quote_cli_tokens() {
	l_cli_string=$1
	l_label=${2:-CLI command}
	if [ "$l_cli_string" = "" ]; then
		return
	fi

	if ! l_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_tokens"
		return 1
	fi
	if [ "$l_tokens" = "" ]; then
		return
	fi

	zxfer_quote_token_stream "$l_tokens"
}

# Purpose: Check whether the SSH policy uses ambient config.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# later helpers need a boolean answer about the SSH policy.
zxfer_ssh_policy_uses_ambient_config() {
	case "${ZXFER_SSH_USE_AMBIENT_CONFIG:-}" in
	1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn])
		return 0
		;;
	esac

	return 1
}

# Purpose: Validate the SSH option value before zxfer relies on it.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution to
# fail closed on malformed, unsafe, or stale input.
zxfer_validate_ssh_option_value() {
	l_value=$1
	l_label=$2
	l_tab=$(printf '\t')
	l_cr=$(printf '\r')
	l_lf=$(printf '\n_')
	l_lf=${l_lf%_}

	case "$l_value" in
	'' | *"$l_tab"* | *"$l_cr"* | *"$l_lf"*)
		printf '%s\n' "$l_label must be a single-line non-empty value."
		return 1
		;;
	esac

	printf '%s\n' "$l_value"
}

# Purpose: Validate the SSH option path before zxfer relies on it.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution to
# fail closed on malformed, unsafe, or stale input.
zxfer_validate_ssh_option_path() {
	l_path=$1
	l_label=$2

	if ! l_path=$(zxfer_validate_ssh_option_value "$l_path" "$l_label"); then
		printf '%s\n' "$l_path"
		return 1
	fi

	case "$l_path" in
	/*)
		printf '%s\n' "$l_path"
		return 0
		;;
	esac

	printf '%s\n' "$l_label must be an absolute path."
	return 1
}

# Purpose: Return the managed SSH option tokens in the form expected by later
# helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_managed_ssh_option_tokens() {
	if zxfer_ssh_policy_uses_ambient_config; then
		return 0
	fi

	if ! l_batch_mode=$(zxfer_validate_ssh_option_value "${ZXFER_SSH_BATCH_MODE:-yes}" "ZXFER_SSH_BATCH_MODE"); then
		printf '%s\n' "$l_batch_mode"
		return 1
	fi

	if ! l_strict_host_key_checking=$(zxfer_validate_ssh_option_value "${ZXFER_SSH_STRICT_HOST_KEY_CHECKING:-yes}" "ZXFER_SSH_STRICT_HOST_KEY_CHECKING"); then
		printf '%s\n' "$l_strict_host_key_checking"
		return 1
	fi

	if [ -n "${ZXFER_SSH_USER_KNOWN_HOSTS_FILE:-}" ]; then
		if ! l_known_hosts_file=$(zxfer_validate_ssh_option_path "$ZXFER_SSH_USER_KNOWN_HOSTS_FILE" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE"); then
			printf '%s\n' "$l_known_hosts_file"
			return 1
		fi
	fi

	printf '%s\n%s\n' "-o" "BatchMode=$l_batch_mode"
	printf '%s\n%s\n' "-o" "StrictHostKeyChecking=$l_strict_host_key_checking"

	if [ -n "${ZXFER_SSH_USER_KNOWN_HOSTS_FILE:-}" ]; then
		printf '%s\n%s\n' "-o" "UserKnownHostsFile=$l_known_hosts_file"
	fi
}

# Purpose: Render the SSH transport policy identity as a stable shell-safe or
# operator-facing string.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer needs to display or transport the value without reparsing it.
zxfer_render_ssh_transport_policy_identity() {
	if zxfer_ssh_policy_uses_ambient_config; then
		printf '%s\n' "ambient"
		return 0
	fi

	if ! l_managed_option_tokens=$(zxfer_get_managed_ssh_option_tokens); then
		printf '%s\n' "$l_managed_option_tokens"
		return 1
	fi

	printf '%s\n' "managed"
	if [ "$l_managed_option_tokens" != "" ]; then
		printf '%s\n' "$l_managed_option_tokens"
	fi
}

# Purpose: Return the SSH base transport tokens in the form expected by later
# helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_ssh_base_transport_tokens() {
	if ! l_managed_option_tokens=$(zxfer_get_managed_ssh_option_tokens); then
		printf '%s\n' "$l_managed_option_tokens"
		return 1
	fi

	if ! zxfer_ensure_local_ssh_command; then
		printf '%s\n' "$g_zxfer_resolved_local_ssh_command_result"
		return 1
	fi
	l_ssh_cmd=$g_zxfer_resolved_local_ssh_command_result

	printf '%s\n' "$l_ssh_cmd"
	if [ "$l_managed_option_tokens" != "" ]; then
		printf '%s\n' "$l_managed_option_tokens"
	fi
}

# Purpose: Return the SSH transport tokens for host in the form expected by
# later helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# sibling helpers need the same lookup without duplicating module logic.
#
# Render the ssh transport argv for a given host, including any control socket,
# as a newline-delimited token stream that can be safely re-quoted or executed.
zxfer_get_ssh_transport_tokens_for_host() {
	l_host=$1

	if ! l_base_transport_tokens=$(zxfer_get_ssh_base_transport_tokens); then
		printf '%s\n' "$l_base_transport_tokens"
		return 1
	fi
	printf '%s\n' "$l_base_transport_tokens"

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

# Purpose: Return the SSH command for host in the form expected by later
# helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# sibling helpers need the same lookup without duplicating module logic.
#
# Render the local ssh transport command used for a host spec. This is a
# display helper only; execution paths should use the argv-based helpers below.
zxfer_get_ssh_cmd_for_host() {
	l_host=$1
	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host"); then
		:
	else
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi
	zxfer_quote_token_stream "$l_transport_tokens"
}

# Purpose: Return the remote command context label in the form expected by
# later helpers.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_remote_command_context_label() {
	l_host_spec=$1
	l_profile_side=${2:-}

	case "$l_profile_side" in
	source)
		l_role_label="origin"
		;;
	destination)
		l_role_label="target"
		;;
	other)
		l_role_label="remote"
		;;
	*)
		if [ -n "$l_host_spec" ] &&
			[ "$l_host_spec" = "${g_option_O_origin_host:-}" ] &&
			[ "$l_host_spec" = "${g_option_T_target_host:-}" ]; then
			l_role_label="origin/target"
		elif [ -n "$l_host_spec" ] &&
			[ "$l_host_spec" = "${g_option_O_origin_host:-}" ]; then
			l_role_label="origin"
		elif [ -n "$l_host_spec" ] &&
			[ "$l_host_spec" = "${g_option_T_target_host:-}" ]; then
			l_role_label="target"
		else
			l_role_label="remote"
		fi
		;;
	esac

	if [ -n "$l_host_spec" ]; then
		printf '%s: %s\n' "$l_role_label" "$l_host_spec"
	else
		printf '%s\n' "$l_role_label"
	fi
}

# Purpose: Emit very-verbose diagnostic output for `-V` runs.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer wants low-level debug output that should stay hidden in normal verbose
# mode.
zxfer_echoV_remote_command_for_host() {
	l_host_spec=$1
	l_profile_side=${2:-}
	shift 2

	l_command_context=$(zxfer_get_remote_command_context_label \
		"$l_host_spec" "$l_profile_side")
	l_rendered_command=$(zxfer_render_command_for_report "" "$@")
	zxfer_echoV "Running remote command [$l_command_context]: $l_rendered_command"
}

# Purpose: Run the SSH command for host through the controlled execution path
# owned by this module.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution once
# planning is complete and zxfer is ready to execute the action.
#
# Expand the ssh transport and host spec into discrete arguments before
# executing the remote command so multi-token -O/-T inputs (like "host pfexec")
# are preserved without reparsing a shell string. STDIN/STDOUT/STDERR are
# passed through to the invoked ssh process.
zxfer_invoke_ssh_command_for_host() {
	l_host_spec=$1
	shift

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host_spec"); then
		:
	else
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host_spec"); then
		zxfer_throw_error "$l_host_tokens"
	fi
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
	zxfer_echoV_remote_command_for_host "$l_host_spec" "" "$@"
	"$@"
}

# Purpose: Build the SSH shell command for host for the next execution or
# comparison step.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before other helpers consume the assembled value.
#
# Build a shell-ready local ssh command string while preserving any wrapper
# tokens embedded in the -O/-T host spec (for example "host pfexec"). The
# remote command must already be quoted for execution by the remote shell.
zxfer_build_ssh_shell_command_for_host() {
	l_host_spec=$1
	l_remote_shell_cmd=$2

	[ "$l_remote_shell_cmd" = "" ] && return 1

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host_spec"); then
		:
	else
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host_spec"); then
		zxfer_throw_error "$l_host_tokens"
	fi
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
		l_wrapper_cmd=$(zxfer_quote_token_stream "$l_wrapper_tokens")
		l_full_remote_cmd="$l_wrapper_cmd $l_remote_shell_cmd"
	fi

	l_command_tokens=$(printf '%s\n%s\n%s\n' "$l_transport_tokens" "$l_ssh_host" "$l_full_remote_cmd")
	zxfer_quote_token_stream "$l_command_tokens"
}

# Purpose: Run the SSH shell command for host through the controlled execution
# path owned by this module.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution once
# planning is complete and zxfer is ready to execute the action.
#
# Execute a shell-ready remote command string through ssh without reparsing a
# local shell string. Wrapper tokens embedded in the -O/-T host spec are
# preserved as part of the single remote command argument.
zxfer_invoke_ssh_shell_command_for_host() {
	l_host_spec=$1
	l_remote_shell_cmd=$2
	l_profile_side=${3:-}

	[ "$l_remote_shell_cmd" = "" ] && return 1
	zxfer_profile_record_ssh_invocation "$l_host_spec" "$l_profile_side"

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host_spec"); then
		:
	else
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host_spec"); then
		zxfer_throw_error "$l_host_tokens"
	fi
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
		l_wrapper_cmd=$(zxfer_quote_token_stream "$l_wrapper_tokens")
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
	zxfer_echoV_remote_command_for_host "$l_host_spec" "$l_profile_side" "$@"
	"$@"
}

# Purpose: Build the remote sh c command for the next execution or comparison
# step.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before other helpers consume the assembled value.
zxfer_build_remote_sh_c_command() {
	l_remote_script=$1
	zxfer_build_shell_command_from_argv "sh" "-c" "$l_remote_script"
}

# Purpose: Run the source ZFS command through the controlled execution path
# owned by this module.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution once
# planning is complete and zxfer is ready to execute the action.
#
# Execute a zfs command on the origin (source) host, transparently invoking
# ssh when -O is in effect so callers can treat this like a local command.
zxfer_run_source_zfs_cmd() {
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
	l_remote_cmd=$(zxfer_quote_token_stream "$l_remote_tokens")
	zxfer_invoke_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_cmd" source
}

# Purpose: Run the destination ZFS command through the controlled execution
# path owned by this module.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution once
# planning is complete and zxfer is ready to execute the action.
#
# Execute a zfs command on the destination (target) host, using ssh when -T is
# active so shell quoting does not leak into the remote hostname.
zxfer_run_destination_zfs_cmd() {
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
	l_remote_cmd=$(zxfer_quote_token_stream "$l_remote_tokens")
	zxfer_invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd" destination
}

# Purpose: Render the source ZFS command as a stable shell-safe or operator-
# facing string.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer needs to display or transport the value without reparsing it.
zxfer_render_source_zfs_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_O_origin_host" = "" ]; then
		l_source_zfs_cmd=$g_cmd_zfs
		if [ -n "$g_LZFS" ] && [ "$g_LZFS" != "$g_cmd_zfs" ]; then
			l_source_zfs_cmd=$g_LZFS
		fi
		zxfer_build_shell_command_from_argv "$l_source_zfs_cmd" "$l_subcommand" "$@"
		return
	fi

	l_origin_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_origin_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(zxfer_quote_token_stream "$l_remote_tokens")
	zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_cmd"
}

# Purpose: Render the destination ZFS command as a stable shell-safe or
# operator-facing string.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer needs to display or transport the value without reparsing it.
zxfer_render_destination_zfs_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_T_target_host" = "" ]; then
		l_target_zfs_cmd=$g_cmd_zfs
		if [ -n "$g_RZFS" ] && [ "$g_RZFS" != "$g_cmd_zfs" ]; then
			l_target_zfs_cmd=$g_RZFS
		fi
		zxfer_build_shell_command_from_argv "$l_target_zfs_cmd" "$l_subcommand" "$@"
		return
	fi

	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_target_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(zxfer_quote_token_stream "$l_remote_tokens")
	zxfer_build_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd"
}

# Purpose: Render the ZFS command for spec as a stable shell-safe or operator-
# facing string.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# zxfer needs to display or transport the value without reparsing it.
zxfer_render_zfs_command_for_spec() {
	l_cmd_spec=$1
	shift

	if [ "$l_cmd_spec" = "$g_LZFS" ]; then
		zxfer_render_source_zfs_command "$@"
	elif [ "$l_cmd_spec" = "$g_RZFS" ]; then
		zxfer_render_destination_zfs_command "$@"
	else
		zxfer_build_shell_command_from_argv "$l_cmd_spec" "$@"
	fi
}

# Purpose: Run the ZFS command for spec through the controlled execution path
# owned by this module.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution once
# planning is complete and zxfer is ready to execute the action.
#
# Run a zfs command based on the provided command specifier, delegating to the
# source or destination helper when the spec references $g_LZFS or $g_RZFS.
zxfer_run_zfs_cmd_for_spec() {
	l_cmd_spec=$1
	shift

	if [ "$l_cmd_spec" = "$g_LZFS" ]; then
		zxfer_run_source_zfs_cmd "$@"
	elif [ "$l_cmd_spec" = "$g_RZFS" ]; then
		zxfer_run_destination_zfs_cmd "$@"
	else
		zxfer_profile_record_zfs_call other "$1"
		zxfer_record_last_command_argv "$l_cmd_spec" "$@"
		"$l_cmd_spec" "$@"
	fi
}

# Purpose: Strip the trailing slashes while preserving the semantics later
# helpers expect.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before comparison or execution consumes the cleaned value.
#
# Remove trailing slash characters from dataset-like arguments while leaving
# strings that consist entirely of '/' untouched so callers can still reject
# absolute paths explicitly.
zxfer_strip_trailing_slashes() {
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

# Purpose: Check whether the destination probe reports missing.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# later helpers need a boolean answer about the destination probe.
zxfer_destination_probe_reports_missing() {
	l_probe_err=$1

	case "$l_probe_err" in
	*"dataset does not exist"* | *"Dataset does not exist"* | *"no such dataset"* | *"No such dataset"* | *"no such pool or dataset"* | *"No such pool or dataset"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Check whether the destination probe is ambiguous.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution when
# later helpers need a boolean answer about the destination probe.
zxfer_destination_probe_is_ambiguous() {
	l_probe_err=$1

	case "$l_probe_err" in
	*[![:space:]]*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Check whether the destination via parent recursive listing exists.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before later create, seed, or delete decisions depend on presence or absence.
zxfer_exists_destination_via_parent_recursive_listing() {
	l_dest=$1
	l_parent_dataset=${l_dest%/*}

	case "${g_destination_operating_system:-}" in
	SunOS) ;;
	*)
		return 2
		;;
	esac

	[ "$l_parent_dataset" != "$l_dest" ] || return 2

	l_cmd=$(zxfer_render_destination_zfs_command list -H -r -o name "$l_parent_dataset")
	zxfer_echoV "Exact destination probe was ambiguous on SunOS; checking parent recursively: $l_cmd"

	if l_parent_listing=$(zxfer_run_destination_zfs_cmd list -H -r -o name "$l_parent_dataset" 2>&1); then
		if printf '%s\n' "$l_parent_listing" | grep -F -x "$l_dest" >/dev/null 2>&1; then
			zxfer_mark_destination_hierarchy_exists "$l_dest"
			printf '%s\n' 1
			return 0
		fi

		if printf '%s\n' "$l_parent_listing" | grep -F -x "$l_parent_dataset" >/dev/null 2>&1; then
			zxfer_mark_destination_hierarchy_exists "$l_parent_dataset"
			zxfer_set_destination_existence_cache_entry "$l_dest" 0
			printf '%s\n' 0
			return 0
		fi

		printf 'Failed to determine whether destination dataset [%s] exists: parent recursive listing for [%s] did not contain the parent dataset.\n' \
			"$l_dest" "$l_parent_dataset"
		return 1
	fi

	if zxfer_destination_probe_reports_missing "$l_parent_listing"; then
		zxfer_set_destination_existence_cache_entry "$l_dest" 0
		printf '%s\n' 0
		return 0
	fi

	if [ -n "$l_parent_listing" ]; then
		printf 'Failed to determine whether destination dataset [%s] exists: parent recursive listing for [%s] failed: %s\n' \
			"$l_dest" "$l_parent_dataset" "$l_parent_listing"
	else
		printf 'Failed to determine whether destination dataset [%s] exists: parent recursive listing for [%s] failed.\n' \
			"$l_dest" "$l_parent_dataset"
	fi
	return 1
}

# Purpose: Check whether the destination exists.
# Usage: Called during command rendering, ssh wrapping, and ZFS execution
# before later create, seed, or delete decisions depend on presence or absence.
#
# Checks whether the destination dataset exists.
# Prints 1 when it exists, 0 when it is explicitly missing, and returns non-zero
# with an explanatory message when the probe itself fails.
zxfer_exists_destination() {
	l_dest=$1
	l_probe_mode=${2:-cache}

	if [ "$l_probe_mode" != "live" ]; then
		if l_cached_exists=$(zxfer_get_destination_existence_cache_entry "$l_dest"); then
			zxfer_echoV "Using cached destination existence for [$l_dest]: $l_cached_exists"
			printf '%s\n' "$l_cached_exists"
			return 0
		fi
	fi

	zxfer_profile_increment_counter g_zxfer_profile_exists_destination_calls

	l_cmd=$(zxfer_render_destination_zfs_command list -H "$l_dest")
	zxfer_echoV "Checking if destination exists: $l_cmd"

	if l_probe_output=$(zxfer_run_destination_zfs_cmd list -H "$l_dest" 2>&1); then
		zxfer_set_destination_existence_cache_entry "$l_dest" 1
		printf '%s\n' 1
		return 0
	fi

	l_probe_err=$l_probe_output

	if zxfer_destination_probe_reports_missing "$l_probe_err"; then
		zxfer_set_destination_existence_cache_entry "$l_dest" 0
		printf '%s\n' 0
		return 0
	fi

	if zxfer_destination_probe_is_ambiguous "$l_probe_err"; then
		l_parent_fallback_result=$(zxfer_exists_destination_via_parent_recursive_listing "$l_dest")
		l_parent_fallback_status=$?
		if [ "$l_parent_fallback_status" -eq 0 ]; then
			printf '%s\n' "$l_parent_fallback_result"
			return 0
		fi
		if [ "$l_parent_fallback_status" -eq 1 ]; then
			printf '%s\n' "$l_parent_fallback_result"
			return 1
		fi
	fi

	if [ -n "$l_probe_err" ]; then
		printf 'Failed to determine whether destination dataset [%s] exists: %s\n' "$l_dest" "$l_probe_err"
	else
		printf 'Failed to determine whether destination dataset [%s] exists.\n' "$l_dest"
	fi
	return 1
}
