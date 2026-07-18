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
# COMMAND EXECUTION HELPERS
################################################################################

# Module contract:
# owns globals: generic command execution scratch and g_last_background_pid.
# reads globals: dry-run/reporting state plus runtime cleanup helpers.
# mutates caches: cleanup PID tracking through shared helpers.
# returns via stdout: command output only.

# Purpose: Publish the PID created by a direct background helper.
# Usage: Producer modules call this immediately after `$!`; consumers may read
# the legacy result channel without becoming external writers.
zxfer_set_last_background_pid() {
	g_last_background_pid=${1:-}
}

# Purpose: Clear the legacy direct-background PID result channel.
# Usage: Called after registration failure or final reap.
zxfer_clear_last_background_pid() {
	g_last_background_pid=""
}

# Purpose: Execute one command with its argument boundaries preserved.
# Usage: zxfer_execute_argv_command CONTINUE_ON_FAIL [--] COMMAND [ARG ...]
# Side effects: Records and optionally displays the command; never reparses
# arguments as shell syntax.
zxfer_execute_argv_command() {
	[ "$#" -gt 0 ] || return 2
	l_is_continue_on_fail=$1
	shift
	if [ "${1:-}" = "--" ]; then
		shift
	fi
	[ "$#" -gt 0 ] || return 2

	l_display_cmd=$(zxfer_quote_command_argv "$@") || return "$?"
	zxfer_record_last_command_argv "$@"
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echov "Dry run: $l_display_cmd"
		return 0
	fi

	zxfer_echov "$l_display_cmd"
	if [ "$l_is_continue_on_fail" -eq 1 ]; then
		"$@" || {
			echo "Non-critical error when executing command. Continuing."
		}
	else
		"$@" || zxfer_throw_error "Error when executing command."
	fi
}

# Purpose: Execute a pre-rendered internal shell command through zxfer's
# dry-run, reporting, and failure-context wrapper.
# Usage: Only callers that deliberately constructed operators or a pipeline
# with hardened renderers may use this API; direct argv belongs above.
zxfer_execute_rendered_shell_command() {
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

# Purpose: Launch a pre-rendered internal shell command in the background and
# capture its output through the checked staging path.
# Usage: Only pipeline/operator renderers may call this API; a single helper
# and its arguments must use an argv-preserving execution path instead.
#
# Execute a command in the background and write the output to a file.
# Dry-run callers receive empty placeholder files so later tempfile consumers
# can continue without executing the background probe.
#
# l_cmd: command to execute
# l_output_file: file to write the output to
zxfer_execute_rendered_background_shell_command() {
	l_cmd=$1
	l_output_file=$2
	l_error_file=${3:-}

	zxfer_echoV "Executing command in the background: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: $l_cmd"
		zxfer_clear_last_background_pid
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
	zxfer_set_last_background_pid "$!"
	if ! zxfer_register_cleanup_pid \
		"$g_last_background_pid" "background command helper"; then
		if ! zxfer_abort_direct_child_pid \
			"$g_last_background_pid" TERM "background command helper"; then
			# Keep the exact child handle published and retained by runtime so
			# the ordered trap path can retry without trusting a numeric PID.
			return 1
		fi
		wait "$g_last_background_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_last_background_pid"
		zxfer_clear_last_background_pid
		return 1
	fi
}
