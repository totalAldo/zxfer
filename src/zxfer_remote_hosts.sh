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
# REMOTE HOST / SSH CONTROL SOCKET / REMOTE TOOL RESOLUTION
################################################################################

# Module contract:
# owns globals: per-role ssh control-socket state plus remote capability/tool resolution state such as g_ssh_origin_control_socket, g_ssh_target_control_socket, g_origin_remote_capabilities_*, g_target_remote_capabilities_*, and the resolved remote zfs helper selections.
# reads globals: g_cmd_ssh, g_option_O_*/g_option_T_*, local helper paths, and temp-root helpers.
# mutates caches: in-memory per-run remote capability state and per-run ssh control sockets under the run temp root; nothing is shared across runs or processes.
# returns via stdout: remote OS/tool paths, ssh argv renderings, and remote-safe command strings.

ZXFER_SSH_CONTROL_SOCKET_PATH_MAX=104
ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE=".Mvij6x1tYLn6woxm"

################################################################################
# SSH CONTROL SOCKET SUPPORT / PER-RUN SOCKET PATHS
################################################################################

# Purpose: Check whether the active SSH binary supports control sockets.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before zxfer tries to multiplex connections through `ssh
# -M` style options.
zxfer_ssh_supports_control_sockets() {
	[ -n "${g_cmd_ssh:-}" ] || return 1
	"$g_cmd_ssh" -M -V >/dev/null 2>&1
}

# Purpose: Return the resolved local ssh helper in the form expected by later
# helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when remote transport is actually needed so local-only runs
# do not hard-require ssh during startup.
zxfer_ensure_local_ssh_command() {
	g_zxfer_resolved_local_ssh_command_result=""

	if [ -n "${g_cmd_ssh:-}" ]; then
		g_zxfer_resolved_local_ssh_command_result=$g_cmd_ssh
		return 0
	fi

	if ! l_ssh_path=$(zxfer_find_required_tool ssh "ssh"); then
		g_zxfer_resolved_local_ssh_command_result=$l_ssh_path
		return 1
	fi

	g_cmd_ssh=$l_ssh_path
	g_zxfer_resolved_local_ssh_command_result=$g_cmd_ssh
	return 0
}

# Purpose: Check whether one SSH control-socket path stays within the platform
# sun_path limit once ssh appends its temporary listener suffix.
# Usage: Called during remote bootstrap and ssh control-socket management
# before a socket path is handed to `ssh -M -S`.
zxfer_is_ssh_control_socket_path_short_enough() {
	l_socket_path=$1
	l_temp_listener_path="$l_socket_path$ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE"

	[ "${#l_temp_listener_path}" -lt "$ZXFER_SSH_CONTROL_SOCKET_PATH_MAX" ]
}

# Purpose: Ensure the per-run SSH control-socket directory exists before the
# flow continues, preferring the private run temp root and falling back to a
# short validated temp directory when the root would exceed sun_path limits.
# Usage: Called from zxfer_setup_ssh_control_socket in the main shell so the
# resolved directory memoizes in $g_zxfer_ssh_control_socket_dir_result for
# the rest of the run.
zxfer_ensure_ssh_control_socket_dir() {
	if [ -n "${g_zxfer_ssh_control_socket_dir_result:-}" ] &&
		[ -d "$g_zxfer_ssh_control_socket_dir_result" ] &&
		[ ! -L "$g_zxfer_ssh_control_socket_dir_result" ]; then
		printf '%s\n' "$g_zxfer_ssh_control_socket_dir_result"
		return 0
	fi
	g_zxfer_ssh_control_socket_dir_result=""

	zxfer_ensure_run_tmp_root || return 1
	if zxfer_is_ssh_control_socket_path_short_enough \
		"$g_zxfer_run_tmp_root/ssh-target.sock"; then
		g_zxfer_ssh_control_socket_dir_result=$g_zxfer_run_tmp_root
		printf '%s\n' "$g_zxfer_ssh_control_socket_dir_result"
		return 0
	fi

	# A long TMPDIR pushes the run temp root past the ~104-byte sun_path
	# limit, so sockets get one short-lived private 0700 directory under the
	# default short temp root instead. It sits outside the run root, so it
	# registers for trap cleanup, which runs after sockets are closed.
	if ! l_short_tmpdir=$(
		unset TMPDIR
		zxfer_try_get_socket_cache_tmpdir
	); then
		return 1
	fi
	if ! l_socket_dir=$(zxfer_create_unpredictable_staging_entry \
		"$l_short_tmpdir/zxfer.ssh.XXXXXX" dir); then
		return 1
	fi
	if ! zxfer_is_ssh_control_socket_path_short_enough \
		"$l_socket_dir/ssh-target.sock"; then
		rmdir "$l_socket_dir" 2>/dev/null || :
		return 1
	fi
	zxfer_register_runtime_artifact_path "$l_socket_dir"
	zxfer_echoV "Ignoring TMPDIR ${TMPDIR:-} for ssh control sockets; using shorter socket root $l_socket_dir."
	g_zxfer_ssh_control_socket_dir_result=$l_socket_dir
	printf '%s\n' "$l_socket_dir"
}

# Purpose: Return the per-run SSH control-socket path for one role in the form
# expected by later helpers.
# Usage: Called during ssh control-socket setup after
# zxfer_ensure_ssh_control_socket_dir has resolved the per-run directory.
zxfer_get_ssh_control_socket_path_for_role() {
	l_role=$1

	case "$l_role" in
	origin | target) ;;
	*)
		return 1
		;;
	esac
	[ -n "${g_zxfer_ssh_control_socket_dir_result:-}" ] || return 1

	printf '%s/ssh-%s.sock\n' "$g_zxfer_ssh_control_socket_dir_result" "$l_role"
}

# Purpose: Reset the SSH control socket action state so the next remote-host
# pass starts from a clean state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_ssh_control_socket_action_state() {
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
}

# Purpose: Read the SSH control socket action stderr file from staged state
# into the current shell.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a checked reload instead of ad hoc
# file reads.
zxfer_read_ssh_control_socket_action_stderr_file() {
	l_stderr_path=$1

	g_zxfer_ssh_control_socket_action_stderr=""
	[ -r "$l_stderr_path" ] || return 1

	if zxfer_read_runtime_artifact_file "$l_stderr_path" >/dev/null 2>&1; then
		l_stderr_contents=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi
	case "$l_stderr_contents" in
	*'
')
		l_stderr_contents=${l_stderr_contents%?}
		;;
	esac

	g_zxfer_ssh_control_socket_action_stderr=$l_stderr_contents
	printf '%s\n' "$l_stderr_contents"
}

# Purpose: Check whether the SSH control socket failure is stale master.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a boolean answer about the SSH
# control socket failure.
#
# `ssh -O check` / `-O exit` talk to the local control socket. Distinguish a
# stale master from transport/bootstrap failures so zxfer only reaps cache
# state after a verified clean close or an explicitly dead master.
zxfer_ssh_control_socket_failure_is_stale_master() {
	l_stderr=${1:-}

	case "$l_stderr" in
	*"Control socket connect("*"): No such file or directory"* | \
		*"Control socket connect("*"): Connection refused"* | \
		*"Control socket connect("*"): Connection reset by peer"* | \
		*"Control socket connect("*"): Broken pipe"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Emit the SSH control socket action failure message in the operator-
# facing format owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_emit_ssh_control_socket_action_failure_message() {
	l_default_message=${1:-}

	if [ -n "${g_zxfer_ssh_control_socket_action_stderr:-}" ]; then
		printf '%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		return 0
	fi
	[ -z "$l_default_message" ] || printf '%s\n' "$l_default_message"
}

# Purpose: Emit very-verbose diagnostic output for `-V` runs.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer wants low-level debug output that should stay
# hidden in normal verbose mode.
zxfer_echoV_ssh_control_socket_command_for_host() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	l_host=$1
	l_action_label=$2
	shift 2

	zxfer_echoV "$l_action_label [$(zxfer_get_remote_command_context_label "$l_host")]: $(zxfer_render_command_for_report "" "$@")"
}

# Purpose: Run the SSH control socket action for host through the controlled
# execution path owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management once planning is complete and zxfer is ready to execute the
# action.
zxfer_run_ssh_control_socket_action_for_host() {
	l_host=$1
	l_socket_path=$2
	l_action=$3

	zxfer_reset_ssh_control_socket_action_state
	[ -n "$l_host" ] || return 1
	[ -n "$l_socket_path" ] || return 1

	case "$l_action" in
	check | exit) ;;
	*)
		return 1
		;;
	esac

	if ! l_transport_tokens=$(zxfer_get_ssh_base_transport_tokens); then
		g_zxfer_ssh_control_socket_action_result="error"
		g_zxfer_ssh_control_socket_action_stderr=$l_transport_tokens
		return 1
	fi
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host"); then
		g_zxfer_ssh_control_socket_action_result="error"
		g_zxfer_ssh_control_socket_action_stderr=$l_host_tokens
		return 1
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
	set -- "$@" -S "$l_socket_path" -O "$l_action"
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi
	g_zxfer_ssh_control_socket_action_command=$(zxfer_build_shell_command_from_argv "$@")
	zxfer_record_last_command_argv "$@"
	if [ "$l_action" = "check" ]; then
		zxfer_echoV_ssh_control_socket_command_for_host \
			"$l_host" "Checking ssh control socket" "$@"
	fi

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_stage_status=$?
		g_zxfer_ssh_control_socket_action_result="capture_error"
		g_zxfer_ssh_control_socket_action_stderr="Failed to stage ssh control socket stderr for $l_action action."
		return "$l_stage_status"
	fi
	l_stderr_path=$g_zxfer_temp_file_result

	if "$@" >/dev/null 2>"$l_stderr_path"; then
		l_action_status=0
	else
		l_action_status=$?
	fi

	if ! zxfer_read_ssh_control_socket_action_stderr_file "$l_stderr_path" >/dev/null; then
		zxfer_cleanup_runtime_artifact_path "$l_stderr_path"
		g_zxfer_ssh_control_socket_action_result="capture_error"
		g_zxfer_ssh_control_socket_action_stderr="Failed to read ssh control socket stderr for $l_action action."
		return 1
	fi
	zxfer_cleanup_runtime_artifact_path "$l_stderr_path"

	if [ "$l_action_status" -eq 0 ]; then
		case "$l_action" in
		check)
			g_zxfer_ssh_control_socket_action_result="live"
			;;
		exit)
			g_zxfer_ssh_control_socket_action_result="closed"
			;;
		esac
		return 0
	fi

	if zxfer_ssh_control_socket_failure_is_stale_master \
		"$g_zxfer_ssh_control_socket_action_stderr"; then
		g_zxfer_ssh_control_socket_action_result="stale"
		return 1
	fi

	g_zxfer_ssh_control_socket_action_result="error"
	return 1
}

# Purpose: Check the SSH control socket for host using the fail-closed rules
# owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before later helpers act on a result that must be validated
# first.
zxfer_check_ssh_control_socket_for_host() {
	l_host=$1
	l_socket_path=$2

	zxfer_run_ssh_control_socket_action_for_host "$l_host" "$l_socket_path" check
}

# Purpose: Open the SSH control socket for host and publish the handles or
# state later helpers need.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before asynchronous work starts using the shared
# coordination resource.
zxfer_open_ssh_control_socket_for_host() {
	l_host=$1
	l_socket_path=$2

	[ -n "$l_host" ] || return 1
	[ -n "$l_socket_path" ] || return 1

	l_transport_tokens=$(zxfer_get_ssh_base_transport_tokens) ||
		zxfer_throw_error "$l_transport_tokens" "$?"
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host"); then
		zxfer_throw_error "$l_host_tokens"
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
	set -- "$@" -M -S "$l_socket_path" -fN
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi

	zxfer_record_last_command_argv "$@"
	zxfer_echoV_ssh_control_socket_command_for_host \
		"$l_host" "Opening ssh control socket" "$@"
	"$@"
}

# Purpose: Update the SSH control socket role state in the shared runtime
# state.
# Usage: Called during remote bootstrap, capability probing, and ssh control-
# socket management after a probe or planning step changes the active context
# that later helpers should use.
zxfer_set_ssh_control_socket_role_state() {
	l_role=$1
	l_socket_path=$2

	case "$l_role" in
	origin)
		g_ssh_origin_control_socket="$l_socket_path"
		;;
	target)
		g_ssh_target_control_socket="$l_socket_path"
		;;
	esac
	if command -v zxfer_refresh_ssh_transport_tokens_for_role >/dev/null 2>&1; then
		zxfer_refresh_ssh_transport_tokens_for_role "$l_role"
	fi
	return 0
}

# Purpose: Clear the SSH control socket role state from the module-owned state.
# Usage: Called during remote bootstrap, capability probing, and ssh control-
# socket management when later helpers must not see an old cached or role-
# specific value.
zxfer_clear_ssh_control_socket_role_state() {
	l_role=$1

	case "$l_role" in
	origin)
		g_ssh_origin_control_socket=""
		;;
	target)
		g_ssh_target_control_socket=""
		;;
	esac
	if command -v zxfer_refresh_ssh_transport_tokens_for_role >/dev/null 2>&1; then
		zxfer_refresh_ssh_transport_tokens_for_role "$l_role"
	fi
	return 0
}

# Purpose: Reset the remote capability parse state so the next remote-host pass
# starts from a clean state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_remote_capability_parse_state() {
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_tool_records=""
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
}

# Purpose: Append the remote capability tool record to the module-owned
# accumulator.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need one shared place to extend staged
# or in-memory state.
zxfer_append_remote_capability_tool_record() {
	l_capability_tool=$1
	l_capability_status=$2
	l_capability_path=$3

	[ -n "$l_capability_tool" ] || return 1

	if [ -n "${g_zxfer_remote_capability_tool_records:-}" ]; then
		g_zxfer_remote_capability_tool_records=$g_zxfer_remote_capability_tool_records'
'$l_capability_tool'	'$l_capability_status'	'$l_capability_path
	else
		g_zxfer_remote_capability_tool_records=$l_capability_tool'	'$l_capability_status'	'$l_capability_path
	fi
}

# Purpose: Return the parsed remote capability tool record in the form expected
# by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_parsed_remote_capability_tool_record() {
	l_capability_tool=$1
	l_tab='	'

	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	[ -n "$l_capability_tool" ] || return 1

	while IFS= read -r l_capability_record || [ -n "$l_capability_record" ]; do
		[ -n "$l_capability_record" ] || continue
		case "$l_capability_record" in
		"$l_capability_tool""$l_tab"*)
			l_capability_record_rest=${l_capability_record#"$l_capability_tool""$l_tab"}
			l_capability_record_status=${l_capability_record_rest%%"$l_tab"*}
			if [ "$l_capability_record_status" = "$l_capability_record_rest" ]; then
				return 1
			fi
			l_capability_record_path=${l_capability_record_rest#*"$l_tab"}
			g_zxfer_remote_capability_tool_status_result=$l_capability_record_status
			g_zxfer_remote_capability_tool_path_result=$l_capability_record_path
			return 0
			;;
		esac
	done <<EOF
${g_zxfer_remote_capability_tool_records:-}
EOF

	return 1
}

# Purpose: Check whether the remote capability requested tool is present.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a boolean answer about the remote
# capability requested tool.
zxfer_remote_capability_requested_tool_is_present() {
	l_tool=$1

	[ -n "$l_tool" ] || return 1
	while IFS= read -r l_existing_tool || [ -n "$l_existing_tool" ]; do
		[ -n "$l_existing_tool" ] || continue
		[ "$l_existing_tool" = "$l_tool" ] && return 0
	done <<EOF
${g_zxfer_remote_capability_requested_tools_result:-}
EOF

	return 1
}

# Purpose: Append the remote capability requested tool to the module-owned
# accumulator.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need one shared place to extend staged
# or in-memory state.
zxfer_append_remote_capability_requested_tool() {
	l_tool=$1

	[ -n "$l_tool" ] || return 0
	if zxfer_remote_capability_requested_tool_is_present "$l_tool"; then
		return 0
	fi

	if [ -n "${g_zxfer_remote_capability_requested_tools_result:-}" ]; then
		g_zxfer_remote_capability_requested_tools_result=$g_zxfer_remote_capability_requested_tools_result'
'$l_tool
	else
		g_zxfer_remote_capability_requested_tools_result=$l_tool
	fi
}

# Purpose: Render the remote capability requested tools as a stable shell-safe
# or operator-facing string.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to display or transport the value without
# reparsing it.
zxfer_render_remote_capability_requested_tools() {
	g_zxfer_remote_capability_requested_tools_result=""
	zxfer_append_remote_capability_requested_tool zfs

	while [ $# -gt 0 ]; do
		zxfer_append_remote_capability_requested_tool "$1"
		shift
	done

	printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
}

# Purpose: Resolve the effective remote capability requested tools for host
# that zxfer should use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_capability_requested_tools_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if [ -n "$l_requested_tools" ]; then
		zxfer_render_remote_capability_requested_tools >/dev/null
		while IFS= read -r l_tool || [ -n "$l_tool" ]; do
			[ -n "$l_tool" ] || continue
			zxfer_append_remote_capability_requested_tool "$l_tool"
		done <<EOF
$l_requested_tools
EOF
		printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
		return 0
	fi

	zxfer_get_remote_capability_requested_tools_for_host "$l_host_spec"
}

# Purpose: Return the remote capability requested tools for tool in the form
# expected by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_capability_requested_tools_for_tool() {
	l_tool=$1

	case "$l_tool" in
	'' | zfs)
		zxfer_render_remote_capability_requested_tools
		;;
	*)
		zxfer_render_remote_capability_requested_tools "$l_tool"
		;;
	esac
}

# Purpose: Extract the remote CLI command head from the serialized input this
# module works with.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need one field or derived fragment
# without reparsing the full payload themselves.
zxfer_extract_remote_cli_command_head() {
	l_cli_string=$1
	l_label=${2:-CLI command}
	if ! l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_tokens"
		return 1
	fi
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	[ -n "$l_cli_head" ] || return 1
	printf '%s\n' "$l_cli_head"
}

# Purpose: Decide whether the clean recursive no-op proof can defer origin
# parallel resolution.
# Usage: Called while building the active remote capability scope. This mirrors
# the proof eligibility checks in snapshot discovery because this module is
# sourced earlier and cannot call that helper directly.
zxfer_remote_capability_origin_can_defer_parallel_for_fast_noop_proof() {
	[ "${g_option_O_origin_host:-}" != "" ] || return 1
	[ "${g_option_T_target_host:-}" = "" ] || return 1
	[ "${g_option_R_recursive:-}" != "" ] || return 1
	[ "${g_option_s_make_snapshot:-0}" -eq 0 ] || return 1
	[ "${g_option_m_migrate:-0}" -eq 0 ] || return 1
	[ "${g_option_P_transfer_property:-0}" -eq 0 ] || return 1
	[ -z "${g_option_o_override_property:-}" ] || return 1
	[ "${g_option_e_restore_property_mode:-0}" -eq 0 ] || return 1
	[ "${g_option_k_backup_property_mode:-0}" -eq 0 ] || return 1

	return 0
}

# Purpose: Decide whether origin capability preloading should include parallel.
# Usage: Called while building the active remote capability scope. Changed-
# source discovery still honors `-j`, but the fast recursive no-op proof uses
# one recursive source stream, so clean no-op startup can defer parallel until a
# fallback path actually needs it.
zxfer_remote_capability_origin_should_preload_parallel() {
	[ "${g_option_j_jobs:-1}" -gt 1 ] || return 1
	if zxfer_remote_capability_origin_can_defer_parallel_for_fast_noop_proof; then
		return 1
	fi
	return 0
}

# Purpose: Return the remote capability requested tools for host in the form
# expected by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_capability_requested_tools_for_host() {
	l_host_spec=$1

	g_zxfer_remote_capability_requested_tools_result=""
	zxfer_append_remote_capability_requested_tool zfs

	if [ -n "${g_option_O_origin_host:-}" ] &&
		{ [ -z "$l_host_spec" ] || [ "$l_host_spec" = "$g_option_O_origin_host" ]; }; then
		if zxfer_remote_capability_origin_should_preload_parallel; then
			zxfer_append_remote_capability_requested_tool parallel
		fi
		if [ "${g_option_e_restore_property_mode:-0}" -eq 1 ]; then
			zxfer_append_remote_capability_requested_tool cat
		fi
		if [ "${g_option_z_compress:-0}" -eq 1 ]; then
			if l_compress_head=$(zxfer_extract_remote_cli_command_head "${g_cmd_compress:-}" "compression command"); then
				zxfer_append_remote_capability_requested_tool "$l_compress_head"
			fi
		fi
	fi

	if [ -n "${g_option_T_target_host:-}" ] &&
		{ [ -z "$l_host_spec" ] || [ "$l_host_spec" = "$g_option_T_target_host" ]; }; then
		if [ "${g_option_k_backup_property_mode:-0}" -eq 1 ]; then
			zxfer_append_remote_capability_requested_tool cat
		fi
		if [ "${g_option_z_compress:-0}" -eq 1 ]; then
			if l_decompress_head=$(zxfer_extract_remote_cli_command_head "${g_cmd_decompress:-}" "decompression command"); then
				zxfer_append_remote_capability_requested_tool "$l_decompress_head"
			fi
		fi
	fi

	printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
}

# Purpose: Return the remote capability requested tools for resolving one tool
# in the form expected by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the prewarmed host scope when it
# already includes the requested helper.
zxfer_get_remote_capability_requested_tools_for_resolved_tool() {
	l_host_spec=$1
	l_tool=$2

	[ -n "$l_tool" ] || return 1
	if l_host_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_host \
		"$l_host_spec"); then
		case "
$l_host_requested_tools
" in
		*"
$l_tool
"*)
			printf '%s\n' "$l_host_requested_tools"
			return 0
			;;
		esac
	fi

	zxfer_get_remote_capability_requested_tools_for_tool "$l_tool"
}

################################################################################
# PER-RUN REMOTE CAPABILITY STATE / HANDSHAKE PARSING
################################################################################
# Purpose: Render the remote capability cache identity for host as a stable
# shell-safe or operator-facing string.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to display or transport the value without
# reparsing it.
zxfer_render_remote_capability_cache_identity_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	l_dependency_path=$(zxfer_get_effective_dependency_path)
	if ! l_transport_policy_identity=$(zxfer_render_ssh_transport_policy_identity); then
		[ "$l_transport_policy_identity" = "" ] || printf '%s\n' "$l_transport_policy_identity"
		return 1
	fi
	if ! zxfer_resolve_remote_capability_requested_tools_for_host \
		"$l_host_spec" "$l_requested_tools" >/dev/null; then
		return 1
	fi

	printf '%s\n%s\n' "$l_dependency_path" "$l_transport_policy_identity"
	printf '%s\n' "${g_zxfer_remote_capability_requested_tools_result:-zfs}"
}

# Purpose: Parse one remote capability payload into the structured globals that
# later remote-helper logic consumes.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after a live or cached capability payload is loaded into
# the current shell.
zxfer_parse_remote_capability_response() {
	l_response=$1
	l_tab='	'
	l_cr=$(printf '\r')
	l_lf=$(printf '\n_')
	l_lf=${l_lf%_}

	zxfer_reset_remote_capability_parse_state
	case "$l_response" in
	*'
')
		l_response=${l_response%?}
		;;
	esac

	l_line_number=0
	l_tool_count=0
	while IFS= read -r l_line || [ -n "$l_line" ]; do
		l_line_number=$((l_line_number + 1))
		case "$l_line_number" in
		1)
			case "$l_line" in
			ZXFER_REMOTE_CAPS_V2) ;;
			*)
				return 1
				;;
			esac
			;;
		2)
			case "$l_line" in
			os"$l_tab"*)
				g_zxfer_remote_capability_os=${l_line#os"$l_tab"}
				[ -n "$g_zxfer_remote_capability_os" ] || return 1
				;;
			*)
				return 1
				;;
			esac
			;;
		*)
			OLDIFS=$IFS
			IFS='	'
			read -r l_record_kind l_record_tool l_record_status l_record_path l_record_extra <<-EOF
				$l_line
			EOF
			IFS=$OLDIFS

			[ "$l_record_kind" = "tool" ] || return 1
			[ -z "$l_record_extra" ] || return 1
			case "$l_record_tool" in
			'' | *"$l_tab"* | *"$l_cr"* | *"$l_lf"*)
				return 1
				;;
			esac
			case "$l_record_status" in
			'' | *[!0-9]*)
				return 1
				;;
			esac
			if [ "$l_record_status" -eq 0 ]; then
				[ -n "$l_record_path" ] || return 1
				[ "$l_record_path" != "-" ] || return 1
				(zxfer_validate_resolved_tool_path "$l_record_path" "$l_record_tool" >/dev/null 2>&1) || return 1
			else
				[ "$l_record_path" = "-" ] || return 1
				l_record_path=""
			fi

			if zxfer_get_parsed_remote_capability_tool_record "$l_record_tool"; then
				return 1
			fi
			if ! zxfer_append_remote_capability_tool_record \
				"$l_record_tool" "$l_record_status" "$l_record_path"; then
				return 1
			fi

			case "$l_record_tool" in
			zfs)
				g_zxfer_remote_capability_zfs_status=$l_record_status
				;;
			esac
			l_tool_count=$((l_tool_count + 1))
			;;
		esac
	done <<-EOF
		$l_response
	EOF

	[ "$l_line_number" -ge 3 ] || return 1
	[ -n "$g_zxfer_remote_capability_os" ] || return 1
	[ "${l_tool_count:-0}" -gt 0 ] || return 1
	[ -n "$g_zxfer_remote_capability_zfs_status" ] || return 1
	return 0
}

# Purpose: Reset the remote probe capture state so the next remote-host pass
# starts from a clean state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before this module reuses mutable scratch globals or cached
# decisions.
#
# Run a remote shell probe while preserving stdout and stderr separately in the
# current shell. The probe helpers use the captured stderr to surface ssh,
# bootstrap, or host-authentication failures instead of collapsing them into a
# generic dependency lookup error.
zxfer_reset_remote_probe_capture_state() {
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
}

# Purpose: Read the remote probe capture file from staged state into the
# current shell.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a checked reload instead of ad hoc
# file reads.
zxfer_read_remote_probe_capture_file() {
	l_capture_path=$1

	g_zxfer_remote_probe_capture_read_result=""
	if zxfer_read_runtime_artifact_file "$l_capture_path" >/dev/null; then
		g_zxfer_remote_probe_capture_read_result=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi

	printf '%s\n' "$g_zxfer_remote_probe_capture_read_result"
}

# Purpose: Load the remote probe capture files from the module-owned cache or
# staged source.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a checked in-memory copy of staged
# data.
zxfer_load_remote_probe_capture_files() {
	l_capture_label=$1
	l_stdout_path=$2
	l_stderr_path=$3

	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_failed=0

	zxfer_read_remote_probe_capture_file "$l_stdout_path" >/dev/null
	l_stdout_read_status=$?
	l_stdout_contents=$g_zxfer_remote_probe_capture_read_result

	zxfer_read_remote_probe_capture_file "$l_stderr_path" >/dev/null
	l_stderr_read_status=$?
	l_stderr_contents=$g_zxfer_remote_probe_capture_read_result

	if [ "$l_stdout_read_status" -eq 0 ] && [ "$l_stderr_read_status" -eq 0 ]; then
		g_zxfer_remote_probe_stdout=$l_stdout_contents
		g_zxfer_remote_probe_stderr=$l_stderr_contents
		return 0
	fi

	g_zxfer_remote_probe_capture_failed=1
	case "${l_stdout_read_status}:${l_stderr_read_status}" in
	0:*)
		g_zxfer_remote_probe_stderr="Failed to read $l_capture_label stderr capture from local staging."
		return "$l_stderr_read_status"
		;;
	*:0)
		g_zxfer_remote_probe_stderr="Failed to read $l_capture_label stdout capture from local staging."
		return "$l_stdout_read_status"
		;;
	*)
		g_zxfer_remote_probe_stderr="Failed to read $l_capture_label stdout and stderr capture from local staging."
		return "$l_stdout_read_status"
		;;
	esac
}

# Purpose: Capture the remote probe output into staged state or module globals
# for later use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a checked snapshot of command
# output or computed state.
zxfer_capture_remote_probe_output() {
	l_host_spec=$1
	l_remote_probe_cmd=$2
	l_profile_side=${3:-}

	zxfer_reset_remote_probe_capture_state

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host_spec"); then
		:
	else
		zxfer_profile_record_ssh_invocation "$l_host_spec" "$l_profile_side"
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi

	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.remote-probe"
	zxfer_create_private_temp_dir "$l_temp_prefix" >/dev/null
	l_capture_status=$?
	if [ "$l_capture_status" -ne 0 ]; then
		zxfer_throw_error "Error creating temporary file."
	fi
	l_capture_dir=$g_zxfer_runtime_artifact_path_result
	l_stdout_path="$l_capture_dir/stdout"
	l_stderr_path="$l_capture_dir/stderr"
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_echoV "Running remote probe [$(zxfer_get_remote_command_context_label "$l_host_spec" "$l_profile_side")]: $l_remote_probe_cmd"
	fi

	if zxfer_invoke_ssh_shell_command_for_host \
		"$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side" >"$l_stdout_path" 2>"$l_stderr_path"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi

	zxfer_load_remote_probe_capture_files "remote probe" "$l_stdout_path" "$l_stderr_path"
	l_capture_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_capture_dir"
	if [ "$l_capture_status" -ne 0 ]; then
		return "$l_capture_status"
	fi
	return "$l_remote_status"
}

# Purpose: Emit the remote probe failure message in the operator-facing format
# owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_emit_remote_probe_failure_message() {
	l_default_message=${1:-}

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		printf '%s\n' "$g_zxfer_remote_probe_stderr"
		return 0
	fi
	[ -z "$l_default_message" ] || printf '%s\n' "$l_default_message"
}

# Purpose: Return the cached remote capability response for host in the form
# expected by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_cached_remote_capability_response_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi

	if [ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
		[ "$l_cache_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ] &&
		[ -n "${g_origin_remote_capabilities_response:-}" ]; then
		printf '%s\n' "$g_origin_remote_capabilities_response"
		return 0
	fi

	if [ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
		[ "$l_cache_identity" = "${g_target_remote_capabilities_cache_identity:-}" ] &&
		[ -n "${g_target_remote_capabilities_response:-}" ]; then
		printf '%s\n' "$g_target_remote_capabilities_response"
		return 0
	fi

	return 1
}

# Purpose: Store the cached remote capability response for host in the cache or
# staging location owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after zxfer has a validated value that later helpers may
# reuse.
zxfer_store_cached_remote_capability_response_for_host() {
	l_host_spec=$1
	l_response=$2
	l_requested_tools=${3:-}
	l_stored=0
	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		l_cache_identity=""
	fi

	if [ "$l_host_spec" = "${g_option_O_origin_host:-}" ] ||
		[ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ]; then
		if [ "${g_origin_remote_capabilities_cache_identity:-}" != "$l_cache_identity" ] ||
			[ "${g_origin_remote_capabilities_host:-}" != "$l_host_spec" ]; then
			g_origin_remote_capabilities_bootstrap_source=""
		fi
		g_origin_remote_capabilities_host=$l_host_spec
		g_origin_remote_capabilities_cache_identity=$l_cache_identity
		g_origin_remote_capabilities_response=$l_response
		l_stored=1
	fi

	if [ "$l_host_spec" = "${g_option_T_target_host:-}" ] ||
		[ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ]; then
		if [ "${g_target_remote_capabilities_cache_identity:-}" != "$l_cache_identity" ] ||
			[ "${g_target_remote_capabilities_host:-}" != "$l_host_spec" ]; then
			g_target_remote_capabilities_bootstrap_source=""
		fi
		g_target_remote_capabilities_host=$l_host_spec
		g_target_remote_capabilities_cache_identity=$l_cache_identity
		g_target_remote_capabilities_response=$l_response
		l_stored=1
	fi

	if [ "$l_stored" -eq 0 ] &&
		[ "${g_origin_remote_capabilities_host:-}" = "" ]; then
		g_origin_remote_capabilities_host=$l_host_spec
		g_origin_remote_capabilities_cache_identity=$l_cache_identity
		g_origin_remote_capabilities_response=$l_response
		g_origin_remote_capabilities_bootstrap_source=""
		return
	fi

	if [ "$l_stored" -eq 0 ]; then
		g_target_remote_capabilities_host=$l_host_spec
		g_target_remote_capabilities_cache_identity=$l_cache_identity
		g_target_remote_capabilities_response=$l_response
		g_target_remote_capabilities_bootstrap_source=""
	fi
}

# Purpose: Record the remote capability bootstrap source for host for later
# diagnostics or control decisions.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs the state preserved for follow-on helpers
# or reporting.
zxfer_note_remote_capability_bootstrap_source_for_host() {
	l_host_spec=$1
	l_source=$2
	l_requested_tools=${3:-}
	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		return 0
	fi

	[ -n "$l_host_spec" ] || return 0
	[ -n "$l_source" ] || return 0

	if { [ "$l_host_spec" = "${g_option_O_origin_host:-}" ] &&
		{ [ "${g_origin_remote_capabilities_cache_identity:-}" = "" ] ||
			[ "$l_cache_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ]; }; } ||
		{ [ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
			[ "$l_cache_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ]; }; then
		if [ "${g_origin_remote_capabilities_bootstrap_source:-}" = "" ]; then
			g_origin_remote_capabilities_bootstrap_source=$l_source
		fi
	fi

	if { [ "$l_host_spec" = "${g_option_T_target_host:-}" ] &&
		{ [ "${g_target_remote_capabilities_cache_identity:-}" = "" ] ||
			[ "$l_cache_identity" = "${g_target_remote_capabilities_cache_identity:-}" ]; }; } ||
		{ [ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
			[ "$l_cache_identity" = "${g_target_remote_capabilities_cache_identity:-}" ]; }; then
		if [ "${g_target_remote_capabilities_bootstrap_source:-}" = "" ]; then
			g_target_remote_capabilities_bootstrap_source=$l_source
		fi
	fi
}

# Purpose: Build the remote capability probe script for the next execution or
# comparison step.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before other helpers consume the assembled value.
zxfer_build_remote_capability_probe_script() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	if ! zxfer_resolve_remote_capability_requested_tools_for_host \
		"$l_host_spec" "$l_requested_tools" >/dev/null; then
		return 1
	fi
	l_requested_tool_tokens=$(zxfer_quote_token_stream \
		"${g_zxfer_remote_capability_requested_tools_result:-zfs}")
	[ "$l_requested_tool_tokens" != "" ] || l_requested_tool_tokens="'zfs'"

	printf "%s\n" "PATH='$l_dependency_path_single'; export PATH; l_os=\$(uname 2>/dev/null) || exit \$?; printf '%s\n' 'ZXFER_REMOTE_CAPS_V2'; printf '%s\t%s\n' 'os' \"\$l_os\"; for l_tool in $l_requested_tool_tokens; do [ -n \"\$l_tool\" ] || continue; l_path=\$(command -v \"\$l_tool\" 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\t%s\t0\t%s\n' 'tool' \"\$l_tool\" \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then printf '%s\t%s\t1\t-\n' 'tool' \"\$l_tool\"; else printf '%s\t%s\t%s\t-\n' 'tool' \"\$l_tool\" \"\$l_status\"; fi; done"
}

# Purpose: Probe a remote host live for the capability payload that describes
# its helper and platform state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when cached capability data is missing or invalid.
zxfer_fetch_remote_host_capabilities_live() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_requested_tools=${3:-}

	g_zxfer_remote_capability_response_result=""
	[ -n "$l_host_spec" ] || return 1

	if ! l_remote_probe=$(zxfer_build_remote_capability_probe_script \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if ! zxfer_capture_remote_probe_output "$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side"; then
		zxfer_emit_remote_probe_failure_message >&2
		return 1
	fi
	l_remote_output=$g_zxfer_remote_probe_stdout

	zxfer_parse_remote_capability_response "$l_remote_output" || return 1

	g_zxfer_remote_capability_response_result=$l_remote_output
	printf '%s\n' "$l_remote_output"
}

# Purpose: Ensure the remote host capabilities exist and are ready before the
# flow continues.
# Usage: Called during remote bootstrap, capability probing, and ssh control-
# socket management before later helpers assume the capability payload is
# available. Capability state is per-run only: one live probe per host fills
# the in-memory tier and every later lookup in this run is answered from
# memory.
zxfer_ensure_remote_host_capabilities() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_requested_tools=${3:-}

	g_zxfer_remote_capability_response_result=""
	[ -n "$l_host_spec" ] || return 1

	if l_cached_response=$(zxfer_get_cached_remote_capability_response_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		if zxfer_parse_remote_capability_response "$l_cached_response"; then
			zxfer_note_remote_capability_bootstrap_source_for_host \
				"$l_host_spec" memory "$l_requested_tools"
			zxfer_profile_record_remote_capability_bootstrap_source memory
			g_zxfer_remote_capability_response_result=$l_cached_response
			printf '%s\n' "$l_cached_response"
			return 0
		fi
	fi

	if zxfer_fetch_remote_host_capabilities_live \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null; then
		:
	else
		l_live_status=$?
		return "$l_live_status"
	fi
	l_live_response=$g_zxfer_remote_capability_response_result

	zxfer_store_cached_remote_capability_response_for_host \
		"$l_host_spec" "$l_live_response" "$l_requested_tools"
	zxfer_note_remote_capability_bootstrap_source_for_host \
		"$l_host_spec" live "$l_requested_tools"
	zxfer_profile_record_remote_capability_bootstrap_source live
	g_zxfer_remote_capability_response_result=$l_live_response
	printf '%s\n' "$l_live_response"
}

# Purpose: Preload the remote host capabilities before later helpers need them.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer wants startup or iteration work to resolve
# expensive state ahead of time.
zxfer_preload_remote_host_capabilities() {
	l_host_spec=$1
	l_profile_side=${2:-}
	if ! l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_host \
		"$l_host_spec"); then
		l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_tool zfs)
	fi

	if [ "${g_option_v_verbose:-0}" -eq 1 ] || [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		l_preload_status=0
		zxfer_ensure_remote_host_capabilities \
			"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null ||
			l_preload_status=$?
		return "$l_preload_status"
	fi

	zxfer_ensure_remote_host_capabilities \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null 2>&1
}

# Purpose: Return the remote host operating system direct in the form expected
# by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_host_operating_system_direct() {
	l_host_spec=$1
	l_profile_side=${2:-}

	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; uname 2>/dev/null"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if ! zxfer_capture_remote_probe_output "$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side"; then
		zxfer_emit_remote_probe_failure_message
		return 1
	fi
	l_remote_output=$g_zxfer_remote_probe_stdout

	l_remote_os=$(printf '%s\n' "$l_remote_output" | sed -n '1p')
	[ -n "$l_remote_os" ] || return 1
	printf '%s\n' "$l_remote_os"
}

# Purpose: Return the remote host operating system in the form expected by
# later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_host_operating_system() {
	l_host_spec=$1
	l_profile_side=${2:-}
	if ! l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_host "$l_host_spec"); then
		l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_tool zfs)
	fi

	if ! l_response=$(zxfer_ensure_remote_host_capabilities \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools"); then
		if ! l_fallback_os=$(zxfer_get_remote_host_operating_system_direct "$l_host_spec" "$l_profile_side"); then
			[ "$l_fallback_os" = "" ] || printf '%s\n' "$l_fallback_os"
			return 1
		fi
		printf '%s\n' "$l_fallback_os"
		return 0
	fi
	if ! zxfer_parse_remote_capability_response "$l_response"; then
		if ! l_fallback_os=$(zxfer_get_remote_host_operating_system_direct "$l_host_spec" "$l_profile_side"); then
			[ "$l_fallback_os" = "" ] || printf '%s\n' "$l_fallback_os"
			return 1
		fi
		printf '%s\n' "$l_fallback_os"
		return 0
	fi
	printf '%s\n' "$g_zxfer_remote_capability_os"
}

################################################################################
# REMOTE TOOL / COMMAND RESOLUTION
################################################################################

# Purpose: Emit the missing remote dependency message in the operator-facing
# format owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_print_missing_remote_dependency_message() {
	l_host=$1
	l_label=$2
	l_dependency_path=$(zxfer_get_effective_dependency_path)

	printf '%s\n' "Required dependency \"$l_label\" not found on host $l_host in secure PATH ($l_dependency_path). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
}

# Purpose: Resolve the effective remote tool from parsed capabilities that
# zxfer should use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after configuration, cache state, or remote state can
# change the final choice.
#
# Resolve a tool from the parsed remote-capability payload already loaded into
# the current shell. Return status 2 when the tool is absent from the payload
# so callers can fall back to a direct secure probe.
zxfer_resolve_remote_tool_from_parsed_capabilities() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}

	[ -n "$l_host" ] || return 1
	[ -n "$l_tool" ] || return 1

	zxfer_get_parsed_remote_capability_tool_record "$l_tool" || return 2

	case "$g_zxfer_remote_capability_tool_status_result" in
	0)
		zxfer_validate_resolved_tool_path \
			"$g_zxfer_remote_capability_tool_path_result" \
			"$l_label" \
			"host $l_host"
		;;
	1)
		zxfer_print_missing_remote_dependency_message "$l_host" "$l_label"
		return 1
		;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote required tool that zxfer should use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_required_tool() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}
	l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_resolved_tool \
		"$l_host" "$l_tool")

	[ -n "$l_host" ] || return 1

	if ! l_remote_caps=$(zxfer_ensure_remote_host_capabilities \
		"$l_host" "$l_profile_side" "$l_requested_tools"); then
		if l_fallback_path=$(zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"); then
			printf '%s\n' "$l_fallback_path"
			return 0
		fi
		printf '%s\n' "$l_fallback_path"
		return 1
	fi
	if ! zxfer_parse_remote_capability_response "$l_remote_caps"; then
		if l_fallback_path=$(zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"); then
			printf '%s\n' "$l_fallback_path"
			return 0
		fi
		printf '%s\n' "$l_fallback_path"
		return 1
	fi

	case "$l_tool" in
	zfs | parallel | cat)
		l_resolved_path=$(zxfer_resolve_remote_tool_from_parsed_capabilities \
			"$l_host" "$l_tool" "$l_label")
		l_resolve_status=$?
		if [ "$l_resolve_status" -eq 0 ]; then
			printf '%s\n' "$l_resolved_path"
			return 0
		fi
		case "$l_resolve_status" in
		2)
			if l_fallback_path=$(zxfer_resolve_remote_cli_tool_direct \
				"$l_host" "$l_tool" "$l_label" "$l_profile_side"); then
				printf '%s\n' "$l_fallback_path"
				return 0
			fi
			printf '%s\n' "$l_fallback_path"
			return 1
			;;
		*)
			printf '%s\n' "$l_resolved_path"
			return 1
			;;
		esac
		;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote CLI tool direct that zxfer should use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_cli_tool_direct() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}

	zxfer_profile_increment_counter g_zxfer_profile_remote_cli_tool_direct_probes
	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_tool_single=$(zxfer_escape_for_single_quotes "$l_tool")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; l_path=\$(command -v '$l_tool_single' 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\n' \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then exit 10; else exit \"\$l_status\"; fi"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if zxfer_capture_remote_probe_output "$l_host" "$l_remote_probe_cmd" "$l_profile_side"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi
	l_remote_output=$g_zxfer_remote_probe_stdout

	case "$l_remote_status" in
	0)
		zxfer_validate_resolved_tool_path "$l_remote_output" "$l_label" "host $l_host"
		;;
	10)
		zxfer_print_missing_remote_dependency_message "$l_host" "$l_label"
		return 1
		;;
	*)
		zxfer_emit_remote_probe_failure_message \
			"Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote CLI tool that zxfer should use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_cli_tool() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}
	l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_resolved_tool \
		"$l_host" "$l_tool")

	case "$l_tool" in
	zfs | parallel | cat)
		zxfer_resolve_remote_required_tool "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
		;;
	esac

	if ! l_remote_caps=$(zxfer_ensure_remote_host_capabilities \
		"$l_host" "$l_profile_side" "$l_requested_tools"); then
		zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
	fi
	if ! zxfer_parse_remote_capability_response "$l_remote_caps"; then
		zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
	fi

	l_resolved_path=$(zxfer_resolve_remote_tool_from_parsed_capabilities \
		"$l_host" "$l_tool" "$l_label")
	l_resolve_status=$?
	if [ "$l_resolve_status" -eq 0 ]; then
		printf '%s\n' "$l_resolved_path"
		return 0
	fi
	case "$l_resolve_status" in
	2)
		zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		;;
	*)
		printf '%s\n' "$l_resolved_path"
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote CLI command safe that zxfer should use.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_cli_command_safe() {
	l_host=$1
	l_cli_string=$2
	l_label=${3:-command}
	l_profile_side=${4:-}
	if ! l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_tokens"
		return 1
	fi
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	if [ -z "$l_cli_head" ]; then
		printf '%s\n' "Required dependency \"$l_label\" must not be empty or whitespace-only."
		return 1
	fi

	if ! l_resolved_head=$(zxfer_resolve_remote_cli_tool "$l_host" "$l_cli_head" "$l_label" "$l_profile_side"); then
		printf '%s\n' "$l_resolved_head"
		return 1
	fi

	zxfer_requote_cli_command_with_resolved_head "$l_cli_string" "$l_resolved_head" "$l_label"
}

################################################################################
# SSH CONTROL SOCKET SETUP / TEARDOWN
################################################################################

# Purpose: Set up the per-run SSH control socket for one remote role.
# Usage: Called during remote bootstrap and ssh control-socket management
# before remote probes or replication traffic reuse a multiplexed transport.
#
# setup an ssh control socket for the specified role (origin or target)
zxfer_setup_ssh_control_socket() {
	l_host=$1
	l_role=$2

	[ -z "$l_host" ] && return

	case "$l_role" in
	origin)
		if [ "$g_ssh_origin_control_socket" != "" ] &&
			! zxfer_close_origin_ssh_control_socket; then
			zxfer_throw_error "Error closing ssh control socket for origin host."
		fi
		;;
	target)
		if [ "$g_ssh_target_control_socket" != "" ] &&
			! zxfer_close_target_ssh_control_socket; then
			zxfer_throw_error "Error closing ssh control socket for target host."
		fi
		;;
	esac

	if ! zxfer_ensure_ssh_control_socket_dir >/dev/null; then
		zxfer_throw_error "Error creating temporary directory for ssh control socket."
	fi
	if ! l_control_socket=$(zxfer_get_ssh_control_socket_path_for_role "$l_role"); then
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	if ! l_transport_tokens=$(zxfer_get_ssh_base_transport_tokens); then
		zxfer_throw_error "$l_transport_tokens"
	fi

	# The socket lives under the private per-run directory, so it can only
	# pre-exist when this run already opened a master for the role; the
	# `-O check` gate keeps that reuse honest and reaps a stale leftover
	# before a fresh master is opened.
	if [ -e "$l_control_socket" ] || [ -L "$l_control_socket" ] ||
		[ -h "$l_control_socket" ]; then
		if zxfer_check_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
			zxfer_set_ssh_control_socket_role_state "$l_role" "$l_control_socket"
			return 0
		fi
		case "${g_zxfer_ssh_control_socket_action_result:-}" in
		stale)
			rm -f "$l_control_socket"
			;;
		*)
			zxfer_emit_ssh_control_socket_action_failure_message \
				"Error checking ssh control socket for $l_role host." >&2
			zxfer_throw_error "Error creating ssh control socket for $l_role host."
			;;
		esac
	fi

	if ! zxfer_open_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	zxfer_set_ssh_control_socket_role_state "$l_role" "$l_control_socket"
}

# Purpose: Close one role's SSH control socket and release the related state.
# Usage: Called during remote bootstrap and ssh control-socket management
# after protected work finishes or trap cleanup takes over. A close failure
# keeps the role state so trap cleanup reports it instead of claiming a clean
# run.
zxfer_close_ssh_control_socket_for_role() {
	l_role=$1

	case "$l_role" in
	origin)
		l_host=${g_option_O_origin_host:-}
		l_control_socket=${g_ssh_origin_control_socket:-}
		;;
	target)
		l_host=${g_option_T_target_host:-}
		l_control_socket=${g_ssh_target_control_socket:-}
		;;
	*)
		return 1
		;;
	esac
	if [ "$l_host" = "" ] || [ "$l_control_socket" = "" ]; then
		return 0
	fi

	if zxfer_run_ssh_control_socket_action_for_host "$l_host" "$l_control_socket" exit; then
		zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
	elif [ "${g_zxfer_ssh_control_socket_action_result:-}" = "stale" ]; then
		zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
	else
		zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
		zxfer_emit_ssh_control_socket_action_failure_message \
			"Error closing $l_role ssh control socket." >&2
		return 1
	fi
	rm -f "$l_control_socket" 2>/dev/null || :
	zxfer_clear_ssh_control_socket_role_state "$l_role"
	return 0
}

# Purpose: Close the origin SSH control socket and release the related handles
# or state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after the protected work finishes or cleanup takes over.
zxfer_close_origin_ssh_control_socket() {
	zxfer_close_ssh_control_socket_for_role origin
}

# Purpose: Close the target SSH control socket and release the related handles
# or state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after the protected work finishes or cleanup takes over.
zxfer_close_target_ssh_control_socket() {
	zxfer_close_ssh_control_socket_for_role target
}

# Purpose: Close the all SSH control sockets and release the related handles or
# state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after the protected work finishes or cleanup takes over.
zxfer_close_all_ssh_control_sockets() {
	l_close_status=0

	zxfer_close_origin_ssh_control_socket
	l_origin_close_status=$?
	if [ "$l_origin_close_status" -ne 0 ]; then
		l_close_status=$l_origin_close_status
	fi
	zxfer_close_target_ssh_control_socket
	l_target_close_status=$?
	if [ "$l_close_status" -eq 0 ] && [ "$l_target_close_status" -ne 0 ]; then
		l_close_status=$l_target_close_status
	fi

	return "$l_close_status"
}

# Purpose: Prepare SSH control sockets only when replication work can use them.
# Usage: Called after snapshot discovery has identified send/delete/property
# work, avoiding an extra SSH master setup on clean no-op runs.
zxfer_prepare_ssh_control_sockets_for_active_hosts() {
	l_ssh_setup_start_ms=""

	if [ "$g_option_O_origin_host" = "" ] && [ "$g_option_T_target_host" = "" ]; then
		return
	fi
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		return
	fi

	l_ssh_setup_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_ssh_setup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	if [ -z "${g_cmd_ssh:-}" ]; then
		if ! zxfer_ensure_local_ssh_command; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_zxfer_resolved_local_ssh_command_result"
		fi
	fi
	zxfer_refresh_ssh_control_socket_support_state

	if [ "$g_option_O_origin_host" != "" ]; then
		if [ "${g_ssh_supports_control_sockets:-0}" -eq 1 ]; then
			[ -n "${g_ssh_origin_control_socket:-}" ] ||
				zxfer_setup_ssh_control_socket "$g_option_O_origin_host" "origin"
		else
			zxfer_echoV "ssh client does not support control sockets; continuing without connection reuse for origin host."
		fi
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		if [ "${g_ssh_supports_control_sockets:-0}" -eq 1 ]; then
			[ -n "${g_ssh_target_control_socket:-}" ] ||
				zxfer_setup_ssh_control_socket "$g_option_T_target_host" "target"
		else
			zxfer_echoV "ssh client does not support control sockets; continuing without connection reuse for target host."
		fi
	fi

	zxfer_refresh_remote_zfs_commands
	zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
}

################################################################################
# REMOTE CONNECTION BOOTSTRAP / ACTIVE COMMAND SELECTION
################################################################################

# Purpose: Refresh the remote ZFS commands from the current configuration and
# runtime state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after inputs change and downstream helpers need the derived
# value rebuilt.
#
# shellcheck disable=SC2034
zxfer_refresh_remote_zfs_commands() {
	if [ "$g_option_O_origin_host" != "" ]; then
		if ! g_option_O_origin_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_O_origin_host"); then
			zxfer_throw_usage_error "$g_option_O_origin_host_safe" 2
		fi
		g_LZFS=${g_origin_cmd_zfs:-$g_cmd_zfs}
	else
		g_option_O_origin_host_safe=""
		g_LZFS=$g_cmd_zfs
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		if ! g_option_T_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host"); then
			zxfer_throw_usage_error "$g_option_T_target_host_safe" 2
		fi
		g_RZFS=${g_target_cmd_zfs:-$g_cmd_zfs}
	else
		g_option_T_target_host_safe=""
		g_RZFS=$g_cmd_zfs
	fi
}

# Purpose: Prepare remote host capability state before the surrounding flow uses
# it.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management once prerequisites are known. SSH control sockets are
# opened later only when replication work exists.
zxfer_prepare_remote_host_connections() {
	l_ssh_setup_start_ms=""

	if { [ "$g_option_O_origin_host" != "" ] || [ "$g_option_T_target_host" != "" ]; } &&
		zxfer_profile_metrics_enabled; then
		l_ssh_setup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		if [ "$g_option_O_origin_host" != "" ]; then
			zxfer_echoV "Dry run: skipping ssh control-socket setup and remote capability preload for origin host."
		fi
		if [ "$g_option_T_target_host" != "" ]; then
			zxfer_echoV "Dry run: skipping ssh control-socket setup and remote capability preload for target host."
		fi
		zxfer_refresh_remote_zfs_commands
		zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
		return
	fi

	if [ "$g_option_O_origin_host" != "" ] || [ "$g_option_T_target_host" != "" ]; then
		if [ -z "${g_cmd_ssh:-}" ]; then
			if ! zxfer_ensure_local_ssh_command; then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_zxfer_resolved_local_ssh_command_result"
			fi
		fi
		zxfer_refresh_ssh_control_socket_support_state
	fi

	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_preload_remote_host_capabilities "$g_option_O_origin_host" source || :
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		zxfer_preload_remote_host_capabilities "$g_option_T_target_host" destination || :
	fi

	zxfer_refresh_remote_zfs_commands
	# Warm the per-role transport-token memo in the main shell once the host
	# specs and managed ssh options are validated (OPTIMIZATION 11): later
	# remote commands replay the rendered tokens instead of re-validating.
	zxfer_refresh_ssh_transport_tokens_for_role origin
	zxfer_refresh_ssh_transport_tokens_for_role target
	zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
}
