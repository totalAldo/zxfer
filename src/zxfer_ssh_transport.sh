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
# SSH TRANSPORT / REMOTE COMMAND CHANNELS
################################################################################

# Module contract:
# owns globals: the ssh policy (g_zxfer_ssh_policy_*), the -O/-T host specs
#   parsed once per value (g_zxfer_ssh_{origin,target}_{spec,host,wrapper}),
#   the per-role control sockets and their directory (with
#   g_zxfer_ssh_control_socket_short_dir, the short directory this module
#   creates and removes when the run root's socket path is too long), the
#   control-socket action results, the per-role zfs render
#   (g_zxfer_zfs_role_*), and the host/command/prepared-command result
#   globals.
# reads globals: g_option_O_origin_host, g_option_T_target_host, g_cmd_zfs,
#   g_origin_cmd_zfs, g_target_cmd_zfs, ZXFER_SSH_*, and runtime state.
# mutates caches: sets g_cmd_ssh on first remote use; opens the per-run ssh
#   control masters (each open is in the runtime cleanup-PID registry until
#   it finishes) and closes them at exit.
# returns via stdout: remote command output, the remote context label, and
#   the commands rendered by zxfer_build_remote_sh_c_command and
#   zxfer_render_{source,destination}_zfs_command (each also publishes its
#   result global).

ZXFER_SSH_CONTROL_SOCKET_PATH_MAX=104
ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE=".Mvij6x1tYLn6woxm"

################################################################################
# SESSION STATE / LOCAL SSH / TRANSPORT POLICY
################################################################################

# Purpose: Check whether the local ssh supports control sockets.
# Usage: zxfer_ssh_supports_control_sockets; probes `ssh -M -V` (one fork).
zxfer_ssh_supports_control_sockets() {
	[ -n "${g_cmd_ssh:-}" ] || return 1
	"$g_cmd_ssh" -M -V >/dev/null 2>&1
}

# Purpose: Record whether the local ssh supports control sockets.
# Usage: Called at session reset and after ssh is resolved; sets
# g_ssh_supports_control_sockets to 1 or 0.
zxfer_refresh_ssh_control_socket_support_state() {
	g_ssh_supports_control_sockets=0
	if zxfer_ssh_supports_control_sockets; then
		g_ssh_supports_control_sockets=1
	fi
}

# Purpose: Reset all per-run ssh transport state before a new session.
# Usage: Called by zxfer_reset_session_state after the dependency reset, so
# g_cmd_ssh is empty and no ssh runs.
zxfer_reset_ssh_transport_state() {
	g_zxfer_resolved_local_ssh_command_result=""
	g_zxfer_ssh_transport_error=""
	g_zxfer_ssh_shell_host_result=""
	g_zxfer_ssh_shell_full_remote_command_result=""
	g_zxfer_ssh_shell_context_error_result=""
	g_zxfer_prepared_ssh_shell_command_result=""
	zxfer_reset_ssh_control_socket_action_state

	# -O/-T host specs parsed by zxfer_refresh_remote_zfs_commands.
	g_zxfer_ssh_origin_spec=""
	g_zxfer_ssh_origin_host=""
	g_zxfer_ssh_origin_wrapper=""
	g_zxfer_ssh_target_spec=""
	g_zxfer_ssh_target_host=""
	g_zxfer_ssh_target_wrapper=""

	# Forget inherited control-socket handles without running ssh or removing
	# any path, so exported globals cannot make early cleanup contact a host.
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	g_zxfer_ssh_control_socket_short_dir=""
	zxfer_refresh_ssh_control_socket_support_state
}

# Purpose: Resolve the local ssh on first remote use, so local-only runs never
# require ssh.
# Usage: zxfer_ensure_local_ssh_command; sets g_cmd_ssh and
# g_zxfer_resolved_local_ssh_command_result, or returns 1 with the lookup
# diagnostic in g_zxfer_resolved_local_ssh_command_result.
zxfer_ensure_local_ssh_command() {
	g_zxfer_resolved_local_ssh_command_result=""

	if [ -n "${g_cmd_ssh:-}" ]; then
		g_zxfer_resolved_local_ssh_command_result=$g_cmd_ssh
		return 0
	fi

	zxfer_find_required_tool ssh ssh || {
		g_zxfer_resolved_local_ssh_command_result=$g_zxfer_required_tool_result
		return 1
	}
	g_cmd_ssh=$g_zxfer_required_tool_result
	g_zxfer_resolved_local_ssh_command_result=$g_cmd_ssh
}

# Purpose: Validate the ZXFER_SSH_* policy and publish the ssh options it adds.
# Usage: zxfer_load_ssh_transport_policy; publishes g_zxfer_ssh_policy_options
# (ssh -o tokens, one per line, empty under ZXFER_SSH_USE_AMBIENT_CONFIG), or
# returns 1 with g_zxfer_ssh_policy_error. It forks nothing, so it revalidates
# on every call rather than keep a memo.
zxfer_load_ssh_transport_policy() {
	g_zxfer_ssh_policy_options=""
	g_zxfer_ssh_policy_error=""

	case ${ZXFER_SSH_USE_AMBIENT_CONFIG:-} in
	1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn]) return 0 ;;
	esac

	l_policy_batch_mode=${ZXFER_SSH_BATCH_MODE:-yes}
	l_policy_strict_host_key_checking=${ZXFER_SSH_STRICT_HOST_KEY_CHECKING:-yes}
	l_policy_known_hosts=${ZXFER_SSH_USER_KNOWN_HOSTS_FILE:-}
	if ! zxfer_value_is_single_line "$l_policy_batch_mode"; then
		g_zxfer_ssh_policy_error="ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
		return 1
	fi
	if ! zxfer_value_is_single_line "$l_policy_strict_host_key_checking"; then
		g_zxfer_ssh_policy_error="ZXFER_SSH_STRICT_HOST_KEY_CHECKING must be a single-line non-empty value."
		return 1
	fi
	l_policy_options="-o${ZXFER_LF}BatchMode=$l_policy_batch_mode${ZXFER_LF}-o${ZXFER_LF}StrictHostKeyChecking=$l_policy_strict_host_key_checking"
	if [ -n "$l_policy_known_hosts" ]; then
		if ! zxfer_value_is_single_line "$l_policy_known_hosts"; then
			g_zxfer_ssh_policy_error="ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be a single-line non-empty value."
			return 1
		fi
		case $l_policy_known_hosts in
		/*) ;;
		*)
			g_zxfer_ssh_policy_error="ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
			return 1
			;;
		esac
		l_policy_options="$l_policy_options${ZXFER_LF}-o${ZXFER_LF}UserKnownHostsFile=$l_policy_known_hosts"
	fi

	g_zxfer_ssh_policy_options=$l_policy_options
}

# Purpose: Validate the ssh policy and resolve local ssh before ssh argv is
# built.
# Usage: zxfer_prepare_ssh_transport; returns 1 with the diagnostic in
# g_zxfer_ssh_transport_error.
zxfer_prepare_ssh_transport() {
	g_zxfer_ssh_transport_error=""
	if ! zxfer_load_ssh_transport_policy; then
		g_zxfer_ssh_transport_error=$g_zxfer_ssh_policy_error
		return 1
	fi
	zxfer_ensure_local_ssh_command && return 0
	g_zxfer_ssh_transport_error=$g_zxfer_resolved_local_ssh_command_result
	return 1
}

################################################################################
# HOST SPECS (-O/-T)
################################################################################

# Purpose: Split one host spec into the ssh host and the wrapper tokens (such
# as "pfexec" in "user@host pfexec") that run in front of every remote command.
# Usage: zxfer_parse_ssh_host_spec HOST_SPEC; publishes
# g_zxfer_ssh_shell_host_result (empty for a blank spec), the quoted
# g_zxfer_ssh_wrapper_result and the raw g_zxfer_ssh_host_spec_tokens_result
# (one per line). A spec that needs shell quoting returns 1 with the rejection
# text in g_zxfer_ssh_shell_context_error_result.
zxfer_parse_ssh_host_spec() {
	g_zxfer_ssh_shell_host_result=""
	g_zxfer_ssh_wrapper_result=""
	g_zxfer_ssh_host_spec_tokens_result=""
	g_zxfer_ssh_shell_context_error_result=""
	# Split without a shell parser, so ssh arguments keep their boundaries and
	# characters such as ';' can never start a new command.
	if ! zxfer_check_literal_token_string "$1" "Host spec (-O/-T)"; then
		g_zxfer_ssh_shell_context_error_result=$g_zxfer_literal_token_error_result
		return 1
	fi
	zxfer_split_tokens_into_result "$1"
	g_zxfer_ssh_host_spec_tokens_result=$g_zxfer_split_tokens_result

	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086 # One token per line, already checked literal.
	set -- $g_zxfer_ssh_host_spec_tokens_result
	zxfer_split_end
	[ "$#" -gt 0 ] || return 0
	g_zxfer_ssh_shell_host_result=$1
	shift
	[ "$#" -gt 0 ] || return 0
	zxfer_render_shell_command_from_argv "$@"
	g_zxfer_ssh_wrapper_result=$g_zxfer_shell_command_result
}

# Purpose: Parse the -O and -T host specs once per value.
# Usage: Called whenever -O/-T may have changed (CLI parsing, session setup);
# a spec that needs shell quoting is a usage error with status 2.
zxfer_refresh_remote_zfs_commands() {
	if [ "${g_option_O_origin_host:-}" != "${g_zxfer_ssh_origin_spec:-}" ]; then
		if zxfer_parse_ssh_host_spec "$g_option_O_origin_host"; then
			g_zxfer_ssh_origin_spec=$g_option_O_origin_host
			g_zxfer_ssh_origin_host=$g_zxfer_ssh_shell_host_result
			g_zxfer_ssh_origin_wrapper=$g_zxfer_ssh_wrapper_result
		else
			zxfer_throw_usage_error "$g_zxfer_ssh_shell_context_error_result" 2
		fi
	fi
	if [ "${g_option_T_target_host:-}" != "${g_zxfer_ssh_target_spec:-}" ]; then
		if zxfer_parse_ssh_host_spec "$g_option_T_target_host"; then
			g_zxfer_ssh_target_spec=$g_option_T_target_host
			g_zxfer_ssh_target_host=$g_zxfer_ssh_shell_host_result
			g_zxfer_ssh_target_wrapper=$g_zxfer_ssh_wrapper_result
		else
			zxfer_throw_usage_error "$g_zxfer_ssh_shell_context_error_result" 2
		fi
	fi
}

# Purpose: Look up the ssh host and wrapper for a host spec, reusing the -O/-T
# parse when the spec is a role spec.
# Usage: zxfer_resolve_ssh_host_spec HOST_SPEC; same results and status as
# zxfer_parse_ssh_host_spec.
zxfer_resolve_ssh_host_spec() {
	g_zxfer_ssh_shell_context_error_result=""
	if [ -n "$1" ] && [ "$1" = "${g_zxfer_ssh_origin_spec:-}" ]; then
		g_zxfer_ssh_shell_host_result=$g_zxfer_ssh_origin_host
		g_zxfer_ssh_wrapper_result=$g_zxfer_ssh_origin_wrapper
	elif [ -n "$1" ] && [ "$1" = "${g_zxfer_ssh_target_spec:-}" ]; then
		g_zxfer_ssh_shell_host_result=$g_zxfer_ssh_target_host
		g_zxfer_ssh_wrapper_result=$g_zxfer_ssh_target_wrapper
	else
		zxfer_parse_ssh_host_spec "$1"
	fi
}

# Purpose: Resolve a host spec and put its wrapper tokens in front of a remote
# shell command.
# Usage: zxfer_prepare_ssh_shell_command_context HOST_SPEC REMOTE_CMD;
# publishes g_zxfer_ssh_shell_host_result and
# g_zxfer_ssh_shell_full_remote_command_result. Returns 1 with the rejection
# text in g_zxfer_ssh_shell_context_error_result for a spec that needs shell
# quoting, and 1 with no text for an empty spec or command.
zxfer_prepare_ssh_shell_command_context() {
	g_zxfer_ssh_shell_full_remote_command_result=""
	if [ -z "$2" ]; then
		g_zxfer_ssh_shell_host_result=""
		g_zxfer_ssh_shell_context_error_result=""
		return 1
	fi
	zxfer_resolve_ssh_host_spec "$1" || return
	[ -n "$g_zxfer_ssh_shell_host_result" ] || return 1

	g_zxfer_ssh_shell_full_remote_command_result=$2
	[ -z "$g_zxfer_ssh_wrapper_result" ] ||
		g_zxfer_ssh_shell_full_remote_command_result="$g_zxfer_ssh_wrapper_result $2"
}

################################################################################
# SSH COMMANDS
################################################################################

# Purpose: Pick the control socket that serves a host spec.
# Usage: zxfer_select_ssh_control_socket HOST_SPEC; publishes
# g_zxfer_ssh_control_socket_result, empty when no role socket applies.
zxfer_select_ssh_control_socket() {
	g_zxfer_ssh_control_socket_result=""
	[ -n "$1" ] || return 0
	if [ "$1" = "${g_option_O_origin_host:-}" ] && [ -n "${g_ssh_origin_control_socket:-}" ]; then
		g_zxfer_ssh_control_socket_result=$g_ssh_origin_control_socket
	elif [ "$1" = "${g_option_T_target_host:-}" ]; then
		g_zxfer_ssh_control_socket_result=${g_ssh_target_control_socket:-}
	fi
}

# Purpose: Label a remote command with its role and host spec for -V output.
# Usage: zxfer_get_remote_command_context_label HOST_SPEC [source|destination|other];
# prints "origin: HOST", "target: HOST", "origin/target: HOST" or "remote: HOST".
zxfer_get_remote_command_context_label() {
	l_label_host_spec=$1

	case ${2:-} in
	source) l_label_role="origin" ;;
	destination) l_label_role="target" ;;
	other) l_label_role="remote" ;;
	*)
		l_label_role="remote"
		if [ -n "$l_label_host_spec" ]; then
			if [ "$l_label_host_spec" = "${g_option_O_origin_host:-}" ] &&
				[ "$l_label_host_spec" = "${g_option_T_target_host:-}" ]; then
				l_label_role="origin/target"
			elif [ "$l_label_host_spec" = "${g_option_O_origin_host:-}" ]; then
				l_label_role="origin"
			elif [ "$l_label_host_spec" = "${g_option_T_target_host:-}" ]; then
				l_label_role="target"
			fi
		fi
		;;
	esac

	if [ -n "$l_label_host_spec" ]; then
		printf '%s: %s\n' "$l_label_role" "$l_label_host_spec"
	else
		printf '%s\n' "$l_label_role"
	fi
}

# Purpose: Print one remote command under -V.
# Usage: zxfer_echoV_remote_command_for_host HOST_SPEC PROFILE_SIDE ARG...;
# renders nothing unless -V is on.
zxfer_echoV_remote_command_for_host() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	l_echo_host_spec=$1
	l_echo_profile_side=$2
	shift 2

	zxfer_echoV "Running remote command [$(zxfer_get_remote_command_context_label "$l_echo_host_spec" "$l_echo_profile_side")]: $(zxfer_render_command_for_report "" "$@")"
}

# Purpose: Build the ssh argv for one host spec and remote shell command, then
# run it, render it or record it.
# Usage: zxfer_ssh_shell_command_for_host run|render|record HOST_SPEC
# REMOTE_CMD [PROFILE_SIDE]. render publishes g_zxfer_shell_command_result;
# record only stores the argv as the report's last command, for a caller that
# runs it in a subshell; run returns the ssh status. Policy, ssh lookup and
# host-spec failures throw; an empty host or command returns 1.
zxfer_ssh_shell_command_for_host() {
	l_ssh_mode=$1
	l_ssh_host_spec=$2
	l_ssh_remote_cmd=$3
	l_ssh_profile_side=${4:-}

	[ -n "$l_ssh_remote_cmd" ] || return 1
	[ "$l_ssh_mode" != run ] ||
		zxfer_profile_record_ssh_invocation "$l_ssh_host_spec" "$l_ssh_profile_side"
	zxfer_prepare_ssh_transport ||
		zxfer_throw_error "$g_zxfer_ssh_transport_error"
	zxfer_prepare_ssh_shell_command_context "$l_ssh_host_spec" "$l_ssh_remote_cmd" || {
		l_ssh_context_status=$?
		[ -z "$g_zxfer_ssh_shell_context_error_result" ] ||
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		return "$l_ssh_context_status"
	}
	zxfer_select_ssh_control_socket "$l_ssh_host_spec"

	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086 # Policy options are checked single-line tokens.
	set -- "$g_cmd_ssh" $g_zxfer_ssh_policy_options
	zxfer_split_end
	[ -z "$g_zxfer_ssh_control_socket_result" ] ||
		set -- "$@" -S "$g_zxfer_ssh_control_socket_result"
	set -- "$@" "$g_zxfer_ssh_shell_host_result" \
		"$g_zxfer_ssh_shell_full_remote_command_result"

	if [ "$l_ssh_mode" = render ]; then
		zxfer_render_shell_command_from_argv "$@"
		return 0
	fi
	zxfer_record_last_command_argv "$@"
	[ "$l_ssh_mode" = run ] || return 0
	zxfer_echoV_remote_command_for_host "$l_ssh_host_spec" "$l_ssh_profile_side" "$@"
	"$@"
}

# Purpose: Run a shell-ready remote command over ssh.
# Usage: zxfer_invoke_ssh_shell_command_for_host HOST_SPEC REMOTE_CMD
# [PROFILE_SIDE]; REMOTE_CMD is already quoted for the remote shell and
# travels as one ssh argument after any wrapper tokens.
zxfer_invoke_ssh_shell_command_for_host() {
	zxfer_ssh_shell_command_for_host run "$@"
}

# Purpose: Render `sh -c SCRIPT` for a remote login shell, csh included,
# without changing the script bytes or consuming its stdin.
# Usage: zxfer_build_remote_sh_c_command SCRIPT; publishes and prints
# g_zxfer_remote_sh_c_command_result. A single-line command of at most 768
# bytes keeps the plain form; a longer or multi-line script travels as tagged
# chunks that a fixed bootstrap reassembles, so no word reaches illumos csh's
# 1020-byte lexical buffer.
zxfer_build_remote_sh_c_command() {
	l_shc_script=$1
	l_shc_lc_all_set=${LC_ALL+1}
	l_shc_lc_all=${LC_ALL-}
	# In the C locale each `?` below matches exactly one byte, and every
	# quote byte is escaped.
	LC_ALL=C
	l_shc_chunk_pattern='????????????????????????????????'
	l_shc_chunk_pattern=$l_shc_chunk_pattern$l_shc_chunk_pattern$l_shc_chunk_pattern$l_shc_chunk_pattern
	l_shc_plain_pattern=$l_shc_chunk_pattern$l_shc_chunk_pattern$l_shc_chunk_pattern
	l_shc_plain_pattern=$l_shc_plain_pattern$l_shc_plain_pattern
	l_shc_chunked=1

	case $l_shc_script in
	*"$ZXFER_LF"*) ;;
	*)
		zxfer_render_shell_command_from_argv sh -c "$l_shc_script"
		# The whole command, not just the script, must stay within 768 bytes.
		case $g_zxfer_shell_command_result in
		$l_shc_plain_pattern?*) ;;
		*) l_shc_chunked=0 ;;
		esac
		;;
	esac

	if [ "$l_shc_chunked" -eq 1 ]; then
		# The bootstrap uses only POSIX sh builtins. A `d` argument carries up
		# to 128 script bytes (at most 515 once quoted) and an `n` argument one
		# newline, so every newline survives, trailing ones included. `exec`
		# keeps stdin and the script's exit status.
		# shellcheck disable=SC2016 # Expanded by the remote bootstrap.
		zxfer_render_shell_command_from_argv sh -c 'l_nl=$(printf "\\nx") || exit $?; l_nl=${l_nl%x}; l_script=; for l_part do case $l_part in d*) l_script=$l_script${l_part#d} ;; n) l_script=$l_script$l_nl ;; *) exit 125 ;; esac; done; exec sh -c "$l_script"' sh
		# Append each chunk already quoted, as the renderer would quote it:
		# one pass, and only chunks holding a quote pay for the escape.
		l_shc_rendered=$g_zxfer_shell_command_result
		while [ -n "$l_shc_script" ]; do
			case $l_shc_script in
			"$ZXFER_LF"*)
				l_shc_rendered="$l_shc_rendered 'n'"
				l_shc_script=${l_shc_script#"$ZXFER_LF"}
				continue
				;;
			esac
			l_shc_chunk=${l_shc_script%%"$ZXFER_LF"*}
			case $l_shc_chunk in
			$l_shc_chunk_pattern?*)
				# The unquoted pattern strips exactly 128 bytes; the quoted
				# remainder then trims the chunk to them.
				# shellcheck disable=SC2295
				l_shc_tail=${l_shc_chunk#$l_shc_chunk_pattern}
				l_shc_chunk=${l_shc_chunk%"$l_shc_tail"}
				;;
			esac
			l_shc_script=${l_shc_script#"$l_shc_chunk"}
			case $l_shc_chunk in
			*\'*)
				zxfer_escape_single_quotes_into_result "$l_shc_chunk"
				l_shc_chunk=$g_zxfer_escaped_single_quotes_result
				;;
			esac
			l_shc_rendered="$l_shc_rendered 'd$l_shc_chunk'"
		done
		g_zxfer_shell_command_result=$l_shc_rendered
	fi

	if [ -n "$l_shc_lc_all_set" ]; then
		LC_ALL=$l_shc_lc_all
	else
		unset LC_ALL
	fi
	g_zxfer_remote_sh_c_command_result=$g_zxfer_shell_command_result
	printf '%s' "$g_zxfer_remote_sh_c_command_result"
}

# Purpose: Render the ssh command for a remote shell command and publish it,
# or throw.
# Usage: zxfer_publish_prepared_ssh_shell_command_for_host_or_throw HOST_SPEC
# REMOTE_CMD; publishes g_zxfer_prepared_ssh_shell_command_result. Invalid
# specs, policy and ssh lookup failures throw; an empty spec or command
# returns 1.
zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
	g_zxfer_prepared_ssh_shell_command_result=""
	[ -n "$2" ] || return 1
	zxfer_resolve_ssh_host_spec "$1" ||
		zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"

	l_prepared_remote_cmd=$2
	# A wrapper such as doas or pfexec runs one command, so hand it an explicit
	# `sh -c` to run the whole remote pipeline under the wrapper.
	if [ -n "$g_zxfer_ssh_wrapper_result" ]; then
		zxfer_build_remote_sh_c_command "$l_prepared_remote_cmd" >/dev/null
		l_prepared_remote_cmd=$g_zxfer_remote_sh_c_command_result
	fi
	zxfer_ssh_shell_command_for_host render "$1" "$l_prepared_remote_cmd" || return
	g_zxfer_prepared_ssh_shell_command_result=$g_zxfer_shell_command_result
}

################################################################################
# ZFS COMMANDS BY ROLE
################################################################################

# Purpose: Resolve where one role's zfs runs and, for a remote role, render
# the remote command with every argument kept intact.
# Usage: zxfer_prepare_zfs_role_command ROLE ARG...; publishes
# g_zxfer_zfs_role_side, g_zxfer_zfs_role_host (empty to run locally) and
# either the local g_zxfer_zfs_role_zfs or the remote g_zxfer_zfs_role_command.
# An unknown ROLE prints "zxfer: unknown zfs command role [ROLE]." to stderr
# and returns 1.
zxfer_prepare_zfs_role_command() {
	g_zxfer_zfs_role_side=$1
	g_zxfer_zfs_role_host=""
	g_zxfer_zfs_role_zfs=$g_cmd_zfs
	g_zxfer_zfs_role_command=""
	case $1 in
	source)
		g_zxfer_zfs_role_host=${g_option_O_origin_host:-}
		l_zfs_role_remote_zfs=${g_origin_cmd_zfs:-$g_cmd_zfs}
		;;
	destination)
		g_zxfer_zfs_role_host=${g_option_T_target_host:-}
		l_zfs_role_remote_zfs=${g_target_cmd_zfs:-$g_cmd_zfs}
		;;
	local)
		g_zxfer_zfs_role_side=other
		;;
	*)
		printf 'zxfer: unknown zfs command role [%s].\n' "$1" >&2
		return 1
		;;
	esac
	[ -n "$g_zxfer_zfs_role_host" ] || return 0

	shift
	zxfer_render_shell_command_from_argv "$l_zfs_role_remote_zfs" "$@"
	g_zxfer_zfs_role_command=$g_zxfer_shell_command_result
	case $g_zxfer_zfs_role_command in
	*"$ZXFER_LF"*)
		# A raw newline would end the command early in a csh or tcsh login
		# shell, so a multi-line argument travels in the csh-safe sh -c form.
		zxfer_build_remote_sh_c_command "$g_zxfer_zfs_role_command" >/dev/null
		g_zxfer_zfs_role_command=$g_zxfer_remote_sh_c_command_result
		;;
	esac
}

# Purpose: Run zfs for one role: locally, or over ssh when the role has an
# -O/-T host.
# Usage: zxfer_run_zfs_cmd_for_role source|destination|local ARG...; returns
# the zfs or ssh status, or 1 for an unknown role.
zxfer_run_zfs_cmd_for_role() {
	zxfer_prepare_zfs_role_command "$@" || return 1
	shift
	zxfer_profile_record_zfs_call "$g_zxfer_zfs_role_side" "$1"
	if [ -z "$g_zxfer_zfs_role_host" ]; then
		zxfer_record_last_command_argv "$g_zxfer_zfs_role_zfs" "$@"
		"$g_zxfer_zfs_role_zfs" "$@"
		return
	fi
	zxfer_invoke_ssh_shell_command_for_host "$g_zxfer_zfs_role_host" \
		"$g_zxfer_zfs_role_command" "$g_zxfer_zfs_role_side"
}

# Purpose: Render the command zxfer_run_zfs_cmd_for_role would run, for
# display, dry runs and pipelines rendered as shell text.
# Usage: zxfer_render_zfs_command_for_role source|destination|local ARG...;
# publishes g_zxfer_shell_command_result, or returns 1 for an unknown role.
zxfer_render_zfs_command_for_role() {
	zxfer_prepare_zfs_role_command "$@" || return 1
	shift
	if [ -z "$g_zxfer_zfs_role_host" ]; then
		zxfer_render_shell_command_from_argv "$g_zxfer_zfs_role_zfs" "$@"
		return 0
	fi
	zxfer_ssh_shell_command_for_host render "$g_zxfer_zfs_role_host" \
		"$g_zxfer_zfs_role_command"
}

# Purpose: Run zfs on the source side (the -O host when given).
# Usage: zxfer_run_source_zfs_cmd ARG...
zxfer_run_source_zfs_cmd() {
	zxfer_run_zfs_cmd_for_role source "$@"
}

# Purpose: Run zfs on the destination side (the -T host when given).
# Usage: zxfer_run_destination_zfs_cmd ARG...
zxfer_run_destination_zfs_cmd() {
	zxfer_run_zfs_cmd_for_role destination "$@"
}

# Purpose: Print the source-side zfs command.
# Usage: zxfer_render_source_zfs_command ARG...
zxfer_render_source_zfs_command() {
	zxfer_render_zfs_command_for_role source "$@" || return
	printf '%s' "$g_zxfer_shell_command_result"
}

# Purpose: Print the destination-side zfs command.
# Usage: zxfer_render_destination_zfs_command ARG...
zxfer_render_destination_zfs_command() {
	zxfer_render_zfs_command_for_role destination "$@" || return
	printf '%s' "$g_zxfer_shell_command_result"
}

################################################################################
# SSH CONTROL SOCKETS
################################################################################

# Purpose: Check that a control-socket path stays within the sun_path limit
# once ssh appends its temporary listener suffix.
# Usage: zxfer_is_ssh_control_socket_path_short_enough PATH.
zxfer_is_ssh_control_socket_path_short_enough() {
	l_socket_temp_listener_path="$1$ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE"
	[ "${#l_socket_temp_listener_path}" -lt "$ZXFER_SSH_CONTROL_SOCKET_PATH_MAX" ]
}

# Purpose: Create the per-run control-socket directory: the private run root,
# or a short private directory when sockets under the run root would pass the
# sun_path limit.
# Usage: zxfer_ensure_ssh_control_socket_dir, once per run; publishes
# g_zxfer_ssh_control_socket_dir_result, or returns 1. A short directory
# lies outside the run root, so this module records it in
# g_zxfer_ssh_control_socket_short_dir and zxfer_trap_exit removes it with
# zxfer_remove_ssh_control_socket_dir once the masters are closed.
zxfer_ensure_ssh_control_socket_dir() {
	g_zxfer_ssh_control_socket_dir_result=""
	zxfer_ensure_run_tmp_root || return 1
	if zxfer_is_ssh_control_socket_path_short_enough \
		"$g_zxfer_run_tmp_root/ssh-target.sock"; then
		g_zxfer_ssh_control_socket_dir_result=$g_zxfer_run_tmp_root
		return 0
	fi

	# A long TMPDIR pushes the run root past the ~104-byte sun_path limit, so
	# the sockets get one private 0700 directory under the default temp root
	# instead. That root may be a shared sticky /tmp, where another local
	# user could create a predictable name first, so mktemp picks a random
	# one.
	zxfer_find_default_tmpdir || return 1
	l_socket_short_dir=$(umask 077 && exec mktemp -d \
		"$g_zxfer_default_tmpdir_result/zxfer.ssh.XXXXXX" 2>/dev/null) ||
		return 1
	if ! zxfer_is_ssh_control_socket_path_short_enough \
		"$l_socket_short_dir/ssh-target.sock"; then
		rmdir "$l_socket_short_dir" 2>/dev/null || :
		return 1
	fi
	g_zxfer_ssh_control_socket_short_dir=$l_socket_short_dir
	zxfer_echoV "Ignoring TMPDIR ${TMPDIR:-} for ssh control sockets; using shorter socket root $l_socket_short_dir."
	g_zxfer_ssh_control_socket_dir_result=$l_socket_short_dir
}

# Purpose: Remove the short control-socket directory once the masters are
# closed.
# Usage: zxfer_remove_ssh_control_socket_dir; called by zxfer_trap_exit after
# zxfer_close_all_ssh_control_sockets. Sockets under the run root need nothing
# here. The removal is not recursive: it unlinks the two role sockets and
# ssh's temporary listener names, then removes the empty directory. Returns 1,
# keeping the handle, when that fails or the path is no longer a real
# directory that this module named.
zxfer_remove_ssh_control_socket_dir() {
	l_socket_remove_dir=${g_zxfer_ssh_control_socket_short_dir:-}
	[ -n "$l_socket_remove_dir" ] || return 0
	case ${l_socket_remove_dir##*/} in
	zxfer.ssh.?*) ;;
	*) return 1 ;;
	esac
	if [ -e "$l_socket_remove_dir" ] || [ -L "$l_socket_remove_dir" ]; then
		[ -d "$l_socket_remove_dir" ] && [ ! -L "$l_socket_remove_dir" ] ||
			return 1
		# ssh binds each master at SOCKET.<random>, links SOCKET to it and
		# unlinks the temporary name; an interrupted open can leave it.
		rm -f "$l_socket_remove_dir/ssh-origin.sock" \
			"$l_socket_remove_dir/ssh-target.sock" \
			"$l_socket_remove_dir"/ssh-*.sock.* 2>/dev/null
		rmdir "$l_socket_remove_dir" 2>/dev/null || return 1
	fi
	g_zxfer_ssh_control_socket_short_dir=""
}

# Purpose: Clear the result of the last control-socket action.
# Usage: zxfer_reset_ssh_control_socket_action_state.
zxfer_reset_ssh_control_socket_action_state() {
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
	g_zxfer_ssh_control_socket_action_pid=""
}

# Purpose: Tell a dead control master apart from other ssh failures, so zxfer
# only reaps a socket after a clean close or a verified dead master.
# Usage: zxfer_ssh_control_socket_failure_is_stale_master SSH_STDERR.
zxfer_ssh_control_socket_failure_is_stale_master() {
	case ${1:-} in
	*"Control socket connect("*"): No such file or directory"* | \
		*"Control socket connect("*"): Connection refused"* | \
		*"Control socket connect("*"): Connection reset by peer"* | \
		*"Control socket connect("*"): Broken pipe"*)
		return 0
		;;
	esac
	return 1
}

# Purpose: Print the last control-socket failure: the captured ssh stderr, or
# DEFAULT_MESSAGE when there is none.
# Usage: zxfer_emit_ssh_control_socket_action_failure_message [DEFAULT_MESSAGE].
zxfer_emit_ssh_control_socket_action_failure_message() {
	if [ -n "${g_zxfer_ssh_control_socket_action_stderr:-}" ]; then
		printf '%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		return 0
	fi
	[ -z "${1:-}" ] || printf '%s\n' "$1"
}

# Purpose: Run one ssh control-socket action for a host spec and socket path.
# Usage: zxfer_run_ssh_control_socket_action open|exit HOST_SPEC SOCKET.
# open starts `ssh -M -fN` in the background and publishes its PID in
# g_zxfer_ssh_control_socket_action_pid for the caller to wait on. exit
# captures ssh stderr in g_zxfer_ssh_control_socket_action_stderr and sets
# g_zxfer_ssh_control_socket_action_result to closed, stale (dead master) or
# error. An invalid policy or host spec is an error with the diagnostic as
# its stderr.
zxfer_run_ssh_control_socket_action() {
	l_socket_action=$1
	l_socket_host_spec=$2
	l_socket_path=$3

	zxfer_reset_ssh_control_socket_action_state
	case $l_socket_action in
	open | exit) ;;
	*) return 1 ;;
	esac
	[ -n "$l_socket_host_spec" ] && [ -n "$l_socket_path" ] || return 1
	if ! zxfer_prepare_ssh_transport; then
		g_zxfer_ssh_control_socket_action_result=error
		g_zxfer_ssh_control_socket_action_stderr=$g_zxfer_ssh_transport_error
		return 1
	fi
	if ! zxfer_parse_ssh_host_spec "$l_socket_host_spec"; then
		g_zxfer_ssh_control_socket_action_result=error
		g_zxfer_ssh_control_socket_action_stderr=$g_zxfer_ssh_shell_context_error_result
		return 1
	fi

	# The whole host spec, wrapper tokens included, follows the ssh options.
	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086 # Checked single-line policy and host tokens.
	if [ "$l_socket_action" = open ]; then
		set -- "$g_cmd_ssh" $g_zxfer_ssh_policy_options -M -S "$l_socket_path" -fN \
			$g_zxfer_ssh_host_spec_tokens_result
	else
		set -- "$g_cmd_ssh" $g_zxfer_ssh_policy_options -S "$l_socket_path" \
			-O exit $g_zxfer_ssh_host_spec_tokens_result
	fi
	zxfer_split_end
	zxfer_render_shell_command_from_argv "$@"
	g_zxfer_ssh_control_socket_action_command=$g_zxfer_shell_command_result
	zxfer_record_last_command_argv "$@"

	if [ "$l_socket_action" = open ]; then
		if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
			zxfer_echoV "Opening ssh control socket [$(zxfer_get_remote_command_context_label "$l_socket_host_spec")]: $(zxfer_render_command_for_report "" "$@")"
		fi
		"$@" &
		g_zxfer_ssh_control_socket_action_pid=$!
		return 0
	fi

	if g_zxfer_ssh_control_socket_action_stderr=$("$@" 2>&1 >/dev/null); then
		g_zxfer_ssh_control_socket_action_result=closed
		return 0
	fi
	g_zxfer_ssh_control_socket_action_result=error
	if zxfer_ssh_control_socket_failure_is_stale_master \
		"$g_zxfer_ssh_control_socket_action_stderr"; then
		g_zxfer_ssh_control_socket_action_result=stale
	fi
	return 1
}

# Purpose: Record a role's control socket, or forget it with an empty PATH.
# Usage: zxfer_set_ssh_control_socket_role_state origin|target PATH.
zxfer_set_ssh_control_socket_role_state() {
	case $1 in
	origin) g_ssh_origin_control_socket=$2 ;;
	target) g_ssh_target_control_socket=$2 ;;
	esac
}

# Purpose: Wait for started control-master opens and fail closed if any
# failed.
# Usage: zxfer_wait_for_ssh_control_masters "ROLE:PID..."; a role whose ssh
# failed forgets its socket (ssh printed its own diagnostic, and no master is
# left to close), then the first such role throws.
zxfer_wait_for_ssh_control_masters() {
	l_wait_failed_role=""
	for l_wait_entry in $1; do
		l_wait_status=0
		wait "${l_wait_entry#*:}" || l_wait_status=$?
		zxfer_unregister_cleanup_pid "${l_wait_entry#*:}"
		[ "$l_wait_status" -ne 0 ] || continue
		zxfer_set_ssh_control_socket_role_state "${l_wait_entry%%:*}" ""
		[ -n "$l_wait_failed_role" ] || l_wait_failed_role=${l_wait_entry%%:*}
	done
	[ -z "$l_wait_failed_role" ] ||
		zxfer_throw_error "Error creating ssh control socket for $l_wait_failed_role host."
}

# Purpose: Open the control master of each remote role before its first
# remote command, so every remote command of the run multiplexes over it.
# Usage: zxfer_open_ssh_control_sockets; called by
# zxfer_prepare_remote_host_connections once ssh is resolved, never in a dry
# run. A role that already has a socket keeps it, and a -T spec equal to the
# -O spec shares the origin master. Under BatchMode=yes two masters handshake
# concurrently; otherwise ssh may prompt, so they open one at a time. Any
# failure throws once every started open has finished.
zxfer_open_ssh_control_sockets() {
	zxfer_prepare_ssh_transport ||
		zxfer_throw_error "$g_zxfer_ssh_transport_error"
	l_open_concurrent=0
	case $ZXFER_LF$g_zxfer_ssh_policy_options$ZXFER_LF in
	*"${ZXFER_LF}BatchMode=yes$ZXFER_LF"*) l_open_concurrent=1 ;;
	esac
	l_open_dir=""
	l_open_started=""
	for l_open_role in origin target; do
		if [ "$l_open_role" = origin ]; then
			l_open_host=${g_option_O_origin_host:-}
			l_open_socket=${g_ssh_origin_control_socket:-}
		else
			l_open_host=${g_option_T_target_host:-}
			l_open_socket=${g_ssh_target_control_socket:-}
		fi
		[ -n "$l_open_host" ] || continue
		if [ "${g_ssh_supports_control_sockets:-0}" -ne 1 ]; then
			zxfer_echoV "ssh client does not support control sockets; continuing without connection reuse for $l_open_role host."
			continue
		fi
		# zxfer_select_ssh_control_socket sends a -T spec equal to the -O
		# spec through the origin master.
		if [ "$l_open_role" = target ] &&
			[ "$l_open_host" = "${g_option_O_origin_host:-}" ]; then
			continue
		fi
		[ -z "$l_open_socket" ] || continue

		# Both roles share one directory, checked once.
		if [ -z "$l_open_dir" ]; then
			zxfer_ensure_ssh_control_socket_dir ||
				zxfer_throw_error "Error creating temporary directory for ssh control socket."
			l_open_dir=$g_zxfer_ssh_control_socket_dir_result
		fi
		l_open_socket="$l_open_dir/ssh-$l_open_role.sock"
		# Only this run writes the private socket directory, so a path that
		# already exists is not a socket zxfer can trust.
		if [ -e "$l_open_socket" ] || [ -L "$l_open_socket" ]; then
			zxfer_throw_error "Error creating ssh control socket for $l_open_role host."
		fi
		# Record the socket before ssh starts, so trap cleanup also closes a
		# master that comes up after an interrupt.
		zxfer_set_ssh_control_socket_role_state "$l_open_role" "$l_open_socket"
		if ! zxfer_run_ssh_control_socket_action open "$l_open_host" "$l_open_socket"; then
			zxfer_set_ssh_control_socket_role_state "$l_open_role" ""
			zxfer_emit_ssh_control_socket_action_failure_message >&2
			zxfer_throw_error "Error creating ssh control socket for $l_open_role host."
		fi
		zxfer_register_cleanup_pid "$g_zxfer_ssh_control_socket_action_pid" \
			"ssh control master open" || :
		l_open_started="$l_open_started $l_open_role:$g_zxfer_ssh_control_socket_action_pid"
		if [ "$l_open_concurrent" -eq 0 ]; then
			zxfer_wait_for_ssh_control_masters "$l_open_started"
			l_open_started=""
		fi
	done
	zxfer_wait_for_ssh_control_masters "$l_open_started"
}

# Purpose: Close one role's ssh control socket and forget it.
# Usage: zxfer_close_ssh_control_socket_for_role origin|target; returns 0 when
# nothing is open or the master is already gone. A failed close keeps the role
# state so trap cleanup reports it instead of claiming a clean run. The socket
# path goes with its directory, which trap cleanup removes next (the run root,
# or zxfer_remove_ssh_control_socket_dir).
zxfer_close_ssh_control_socket_for_role() {
	case $1 in
	origin)
		l_close_host_spec=${g_option_O_origin_host:-}
		l_close_socket=${g_ssh_origin_control_socket:-}
		;;
	target)
		l_close_host_spec=${g_option_T_target_host:-}
		l_close_socket=${g_ssh_target_control_socket:-}
		;;
	*) return 1 ;;
	esac
	[ -n "$l_close_host_spec" ] && [ -n "$l_close_socket" ] || return 0

	zxfer_run_ssh_control_socket_action exit "$l_close_host_spec" "$l_close_socket" || :
	zxfer_echoV "Closing $1 ssh control socket: $g_zxfer_ssh_control_socket_action_command"
	case $g_zxfer_ssh_control_socket_action_result in
	closed | stale) ;;
	*)
		zxfer_emit_ssh_control_socket_action_failure_message \
			"Error closing $1 ssh control socket." >&2
		return 1
		;;
	esac
	zxfer_set_ssh_control_socket_role_state "$1" ""
}

# Purpose: Close both roles' ssh control sockets.
# Usage: zxfer_close_all_ssh_control_sockets; tries both and returns the first
# failure status.
zxfer_close_all_ssh_control_sockets() {
	l_close_all_status=0
	for l_close_all_role in origin target; do
		zxfer_close_ssh_control_socket_for_role "$l_close_all_role" && continue
		l_close_all_role_status=$?
		[ "$l_close_all_status" -ne 0 ] || l_close_all_status=$l_close_all_role_status
	done
	return "$l_close_all_status"
}
