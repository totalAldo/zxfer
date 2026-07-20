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
# owns globals: validated host/wrapper parsing memos, managed SSH transport
#   argv, prepared/rendered remote command results, active ZFS command routing,
#   and per-role control-socket lifecycle state.
# reads globals: parsed -O/-T options, resolved local/remote helper paths,
#   secure staging/runtime helpers, and reporting/profile state.
# mutates caches: per-role host parsing and transport-token memos plus per-run
#   SSH control sockets under the private runtime root.
# returns via stdout: validated host tokens, transport argv, rendered commands,
#   and direct remote/ZFS command output.

ZXFER_SSH_CONTROL_SOCKET_PATH_MAX=104
ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE=".Mvij6x1tYLn6woxm"

################################################################################
# SSH CONTROL SOCKET SUPPORT / PER-RUN SOCKET PATHS
################################################################################

# Purpose: Check whether the active SSH binary supports control sockets.
# Usage: Called during SSH transport setup, rendering, and control-
# socket management before zxfer tries to multiplex connections through `ssh
# -M` style options.
zxfer_ssh_supports_control_sockets() {
	[ -n "${g_cmd_ssh:-}" ] || return 1
	"$g_cmd_ssh" -M -V >/dev/null 2>&1
}

# Purpose: Refresh the ssh control-socket capability flag from local ssh state.
# Usage: Called while resetting transport defaults and after lazy ssh
# resolution so downstream code can rely on one cached support flag.
zxfer_refresh_ssh_control_socket_support_state() {
	g_ssh_supports_control_sockets=0
	if zxfer_ssh_supports_control_sockets; then
		g_ssh_supports_control_sockets=1
	fi
}

# Purpose: Reset all per-run SSH transport state before a new session.
# Usage: Called by the session composition root after dependency defaults and
# before remote capability negotiation or command rendering.
# Side effects: Clears prepared command channels, role memos, socket state, and
# restores local ZFS routing defaults.
zxfer_reset_ssh_transport_state() {
	g_zxfer_resolved_local_ssh_command_result=""
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
	g_zxfer_ssh_shell_host_result=""
	g_zxfer_ssh_shell_full_remote_command_result=""
	g_zxfer_ssh_shell_context_error_result=""
	g_zxfer_remote_shell_command_for_host_result=""
	g_zxfer_prepared_ssh_shell_command_result=""
	g_zxfer_prepared_ssh_shell_command_error_result=""

	# Per-run ssh control sockets used for origin (-O) and target (-T) hosts.
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""

	# Per-role rendered transport-token and host-spec parse memos.
	g_zxfer_ssh_transport_tokens_origin=""
	g_zxfer_ssh_transport_tokens_origin_socket=""
	g_zxfer_ssh_transport_tokens_origin_set=0
	g_zxfer_ssh_transport_tokens_target=""
	g_zxfer_ssh_transport_tokens_target_socket=""
	g_zxfer_ssh_transport_tokens_target_set=0
	g_zxfer_ssh_shell_context_memo_origin_spec=""
	g_zxfer_ssh_shell_context_memo_origin_host=""
	g_zxfer_ssh_shell_context_memo_origin_wrapper=""
	g_zxfer_ssh_shell_context_memo_target_spec=""
	g_zxfer_ssh_shell_context_memo_target_host=""
	g_zxfer_ssh_shell_context_memo_target_wrapper=""
	zxfer_refresh_ssh_control_socket_support_state

	# Active ZFS routes default to local commands until -O/-T is prepared.
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs
}

# Purpose: Discard inherited SSH cleanup handles without invoking ssh or
# removing any referenced socket path.
# Usage: Called by the session composition root before traps are installed so
# exported internal globals cannot make early-startup cleanup contact a host.
zxfer_discard_ssh_cleanup_state() {
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
}

# Purpose: Split the host spec tokens into the token stream expected by later
# helpers.
# Usage: Called by SSH transport rendering and remote ZFS execution when
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

# Purpose: Quote the host spec tokens for the shell or report format used by
# zxfer.
# Usage: Called by SSH transport rendering and remote ZFS execution when
# raw tokens must be preserved without reopening parsing or injection risks.
#
# Quote a host spec for safe reinsertion into eval'd strings by wrapping each
# token in single quotes. This keeps multi-word ssh arguments working while
# preventing the shell from interpreting metacharacters provided by the user.
zxfer_quote_host_spec_tokens() {
	l_quoted_host_spec=$1
	if [ "$l_quoted_host_spec" = "" ]; then
		return
	fi

	if ! l_quoted_host_tokens=$(zxfer_split_host_spec_tokens \
		"$l_quoted_host_spec"); then
		printf '%s\n' "$l_quoted_host_tokens"
		return 1
	fi
	if [ "$l_quoted_host_tokens" = "" ]; then
		return
	fi

	zxfer_quote_token_stream "$l_quoted_host_tokens"
}

# Purpose: Return the resolved local ssh helper in the form expected by later
# helpers.
# Usage: Called during SSH transport setup, rendering, and control-
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

	zxfer_set_dependency_command g_cmd_ssh "$l_ssh_path"
	g_zxfer_resolved_local_ssh_command_result=$l_ssh_path
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

# Purpose: Revalidate that a memoized SSH control-socket directory is still
# the exact private directory allocated and owned by this run.
# Usage: The memo fast path calls this before reusing either the run root or a
# registered short path-adjacent directory.
zxfer_ssh_control_socket_dir_is_current_private() {
	l_ssh_socket_dir_path=$1

	[ -n "$l_ssh_socket_dir_path" ] || return 1
	if [ "$l_ssh_socket_dir_path" = "${g_zxfer_run_tmp_root:-}" ]; then
		zxfer_run_tmp_root_is_current_private_dir "$l_ssh_socket_dir_path"
		return
	fi

	zxfer_runtime_artifact_path_is_registered "$l_ssh_socket_dir_path" || return 1
	zxfer_get_registered_runtime_artifact_directory_identity \
		"$l_ssh_socket_dir_path" || return 1
	l_ssh_socket_dir_registered_identity=$g_zxfer_runtime_artifact_directory_identity_result
	[ -n "$l_ssh_socket_dir_registered_identity" ] || return 1
	[ -d "$l_ssh_socket_dir_path" ] || return 1
	[ ! -L "$l_ssh_socket_dir_path" ] || return 1
	[ ! -h "$l_ssh_socket_dir_path" ] || return 1
	l_ssh_socket_dir_current_identity=$(zxfer_get_path_device_inode \
		"$l_ssh_socket_dir_path") || return 1
	[ "$l_ssh_socket_dir_current_identity" = \
		"$l_ssh_socket_dir_registered_identity" ] || return 1
	l_ssh_socket_dir_effective_uid=$(zxfer_get_effective_user_uid) || return 1
	l_ssh_socket_dir_owner_uid=$(zxfer_get_path_owner_uid \
		"$l_ssh_socket_dir_path") || return 1
	[ "$l_ssh_socket_dir_owner_uid" = "$l_ssh_socket_dir_effective_uid" ] || return 1
	l_ssh_socket_dir_mode=$(zxfer_get_path_mode_octal \
		"$l_ssh_socket_dir_path") || return 1
	[ "$l_ssh_socket_dir_mode" = "700" ] || return 1
	l_ssh_socket_dir_verified_identity=$(zxfer_get_path_device_inode \
		"$l_ssh_socket_dir_path") || return 1
	[ "$l_ssh_socket_dir_verified_identity" = \
		"$l_ssh_socket_dir_registered_identity" ]
}

# Purpose: Ensure the per-run SSH control-socket directory exists before the
# flow continues, preferring the private run temp root and falling back to a
# short validated temp directory when the root would exceed sun_path limits.
# Usage: Called from zxfer_setup_ssh_control_socket in the main shell so the
# resolved directory memoizes in $g_zxfer_ssh_control_socket_dir_result for
# the rest of the run.
zxfer_ensure_ssh_control_socket_dir() {
	if [ -n "${g_zxfer_ssh_control_socket_dir_result:-}" ]; then
		l_ssh_socket_dir_memo=$g_zxfer_ssh_control_socket_dir_result
		if zxfer_ssh_control_socket_dir_is_current_private \
			"$l_ssh_socket_dir_memo"; then
			printf '%s\n' "$l_ssh_socket_dir_memo"
			return 0
		fi
		g_zxfer_ssh_control_socket_dir_result=""
		return 1
	fi

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
	if ! zxfer_register_runtime_artifact_path "$l_socket_dir"; then
		rmdir "$l_socket_dir" 2>/dev/null || :
		return 1
	fi
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
# Usage: Called during SSH transport setup, rendering, and control-
# socket management before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_ssh_control_socket_action_state() {
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
}

# Purpose: Read the SSH control socket action stderr file from staged state
# into the current shell.
# Usage: Called during SSH transport setup, rendering, and control-
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
# Usage: Called during SSH transport setup, rendering, and control-
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
# Usage: Called during SSH transport setup, rendering, and control-
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
# Usage: Called during SSH transport setup, rendering, and control-
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
# Usage: Called during SSH transport setup, rendering, and control-
# socket management once planning is complete and zxfer is ready to execute the
# action.
zxfer_run_ssh_control_socket_action_for_host() {
	l_run_ssh_control_socket_action_for_host_host=$1
	l_socket_path=$2
	l_action=$3

	zxfer_reset_ssh_control_socket_action_state
	[ -n "$l_run_ssh_control_socket_action_for_host_host" ] || return 1
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
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_run_ssh_control_socket_action_for_host_host"); then
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
			"$l_run_ssh_control_socket_action_for_host_host" "Checking ssh control socket" "$@"
	fi

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_stage_status=$?
		g_zxfer_ssh_control_socket_action_result="capture_error"
		g_zxfer_ssh_control_socket_action_stderr="Failed to stage ssh control socket stderr for $l_action action."
		return "$l_stage_status"
	fi
	l_run_ssh_control_socket_action_for_host_stderr_path=$g_zxfer_temp_file_result

	if "$@" >/dev/null 2>"$l_run_ssh_control_socket_action_for_host_stderr_path"; then
		l_action_status=0
	else
		l_action_status=$?
	fi

	if ! zxfer_read_ssh_control_socket_action_stderr_file "$l_run_ssh_control_socket_action_for_host_stderr_path" >/dev/null; then
		zxfer_cleanup_runtime_artifact_path "$l_run_ssh_control_socket_action_for_host_stderr_path"
		g_zxfer_ssh_control_socket_action_result="capture_error"
		g_zxfer_ssh_control_socket_action_stderr="Failed to read ssh control socket stderr for $l_action action."
		return 1
	fi
	zxfer_cleanup_runtime_artifact_path "$l_run_ssh_control_socket_action_for_host_stderr_path"

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
# Usage: Called during SSH transport setup, rendering, and control-
# socket management before later helpers act on a result that must be validated
# first.
zxfer_check_ssh_control_socket_for_host() {
	l_check_ssh_control_socket_for_host_host=$1
	l_check_ssh_control_socket_for_host_socket_path=$2

	zxfer_run_ssh_control_socket_action_for_host "$l_check_ssh_control_socket_for_host_host" "$l_check_ssh_control_socket_for_host_socket_path" check
}

# Purpose: Open the SSH control socket for host and publish the handles or
# state later helpers need.
# Usage: Called during SSH transport setup, rendering, and control-
# socket management before asynchronous work starts using the shared
# coordination resource.
zxfer_open_ssh_control_socket_for_host() {
	l_open_ssh_control_socket_for_host_host=$1
	l_socket_path=$2

	[ -n "$l_open_ssh_control_socket_for_host_host" ] || return 1
	[ -n "$l_socket_path" ] || return 1

	l_transport_tokens=$(zxfer_get_ssh_base_transport_tokens) ||
		zxfer_throw_error "$l_transport_tokens" "$?"
	if ! l_host_tokens=$(zxfer_split_host_spec_tokens "$l_open_ssh_control_socket_for_host_host"); then
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
		"$l_open_ssh_control_socket_for_host_host" "Opening ssh control socket" "$@"
	"$@"
}

# Purpose: Update the SSH control socket role state in the shared runtime
# state.
# Usage: Called during remote bootstrap, capability probing, and ssh control-
# socket management after a probe or planning step changes the active context
# that later helpers should use.
zxfer_set_ssh_control_socket_role_state() {
	l_set_ssh_control_socket_role_state_role=$1
	l_socket_path=$2

	case "$l_set_ssh_control_socket_role_state_role" in
	origin)
		g_ssh_origin_control_socket="$l_socket_path"
		;;
	target)
		g_ssh_target_control_socket="$l_socket_path"
		;;
	esac
	zxfer_refresh_ssh_transport_tokens_for_role "$l_set_ssh_control_socket_role_state_role"
	return 0
}

# Purpose: Clear the SSH control socket role state from the module-owned state.
# Usage: Called during remote bootstrap, capability probing, and ssh control-
# socket management when later helpers must not see an old cached or role-
# specific value.
zxfer_clear_ssh_control_socket_role_state() {
	l_clear_ssh_control_socket_role_state_role=$1

	case "$l_clear_ssh_control_socket_role_state_role" in
	origin)
		g_ssh_origin_control_socket=""
		;;
	target)
		g_ssh_target_control_socket=""
		;;
	esac
	zxfer_refresh_ssh_transport_tokens_for_role "$l_clear_ssh_control_socket_role_state_role"
	return 0
}

# Purpose: Check whether the SSH policy uses ambient config.
# Usage: Called by SSH transport rendering and remote ZFS execution when
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
# Usage: Called by SSH transport rendering and remote ZFS execution to
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
# Usage: Called by SSH transport rendering and remote ZFS execution to
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
# Usage: Called by SSH transport rendering and remote ZFS execution when
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
# Usage: Called by SSH transport rendering and remote ZFS execution when
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
# Usage: Called by SSH transport rendering and remote ZFS execution when
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

# Purpose: Render the SSH transport tokens for host from the current managed
# option policy and control-socket state without consulting the per-role memo.
# Usage: Called by zxfer_get_ssh_transport_tokens_for_host on memo misses and
# by zxfer_refresh_ssh_transport_tokens_for_role to fill the memo.
#
# Render the ssh transport argv for a given host, including any control socket,
# as a newline-delimited token stream that can be safely re-quoted or executed.
zxfer_render_ssh_transport_tokens_for_host() {
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

# Purpose: Refresh the per-role rendered SSH transport-token memo from the
# current managed-option policy and control-socket state.
# Usage: Called in the main shell after CLI validation and whenever a role's
# control-socket state changes, so per-command transport rendering becomes one
# memo read instead of re-validating managed ssh options every time.
zxfer_refresh_ssh_transport_tokens_for_role() {
	l_role=$1

	case "$l_role" in
	origin)
		g_zxfer_ssh_transport_tokens_origin_set=0
		g_zxfer_ssh_transport_tokens_origin=""
		g_zxfer_ssh_transport_tokens_origin_socket=""
		[ -n "${g_option_O_origin_host:-}" ] || return 0
		if l_role_tokens=$(zxfer_render_ssh_transport_tokens_for_host \
			"$g_option_O_origin_host"); then
			g_zxfer_ssh_transport_tokens_origin=$l_role_tokens
			g_zxfer_ssh_transport_tokens_origin_socket=${g_ssh_origin_control_socket:-}
			g_zxfer_ssh_transport_tokens_origin_set=1
		fi
		;;
	target)
		g_zxfer_ssh_transport_tokens_target_set=0
		g_zxfer_ssh_transport_tokens_target=""
		g_zxfer_ssh_transport_tokens_target_socket=""
		[ -n "${g_option_T_target_host:-}" ] || return 0
		# A target spec equal to the origin spec renders origin-socket
		# tokens, so the origin memo already covers it; skip a target memo
		# whose staleness could not be keyed on the target socket.
		[ "${g_option_T_target_host}" != "${g_option_O_origin_host:-}" ] || return 0
		if l_role_tokens=$(zxfer_render_ssh_transport_tokens_for_host \
			"$g_option_T_target_host"); then
			g_zxfer_ssh_transport_tokens_target=$l_role_tokens
			g_zxfer_ssh_transport_tokens_target_socket=${g_ssh_target_control_socket:-}
			g_zxfer_ssh_transport_tokens_target_set=1
		fi
		;;
	esac
	return 0
}

# Purpose: Return the SSH transport tokens for host in the form expected by
# later helpers.
# Usage: Called by SSH transport rendering and remote ZFS execution when
# sibling helpers need the same lookup without duplicating module logic.
#
# The per-role memo answers -O/-T hosts when the recorded control-socket state
# still matches; anything else falls through to a fresh render so correctness
# never depends on the memo being warm.
zxfer_get_ssh_transport_tokens_for_host() {
	l_get_ssh_transport_tokens_for_host_host=$1

	if [ -n "$l_get_ssh_transport_tokens_for_host_host" ]; then
		if [ "$l_get_ssh_transport_tokens_for_host_host" = "${g_option_O_origin_host:-}" ] &&
			[ "${g_zxfer_ssh_transport_tokens_origin_set:-0}" -eq 1 ] &&
			[ "${g_zxfer_ssh_transport_tokens_origin_socket:-}" = "${g_ssh_origin_control_socket:-}" ]; then
			printf '%s\n' "$g_zxfer_ssh_transport_tokens_origin"
			return 0
		fi
		if [ "$l_get_ssh_transport_tokens_for_host_host" = "${g_option_T_target_host:-}" ] &&
			[ "$l_get_ssh_transport_tokens_for_host_host" != "${g_option_O_origin_host:-}" ] &&
			[ "${g_zxfer_ssh_transport_tokens_target_set:-0}" -eq 1 ] &&
			[ "${g_zxfer_ssh_transport_tokens_target_socket:-}" = "${g_ssh_target_control_socket:-}" ]; then
			printf '%s\n' "$g_zxfer_ssh_transport_tokens_target"
			return 0
		fi
	fi

	zxfer_render_ssh_transport_tokens_for_host "$l_get_ssh_transport_tokens_for_host_host"
}

# Purpose: Return the remote command context label in the form expected by
# later helpers.
# Usage: Called by SSH transport rendering and remote ZFS execution when
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
# Usage: Called by SSH transport rendering and remote ZFS execution when
# zxfer wants low-level debug output that should stay hidden in normal verbose
# mode. Returns before rendering anything when `-V` is off.
zxfer_echoV_remote_command_for_host() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	l_host_spec=$1
	l_profile_side=${2:-}
	shift 2

	zxfer_echoV "Running remote command [$(zxfer_get_remote_command_context_label "$l_host_spec" "$l_profile_side")]: $(zxfer_render_command_for_report "" "$@")"
}

# Purpose: Prepare the parsed SSH host and wrapped remote command for one host
# spec.
# Usage: Called before SSH command rendering or invocation so wrapper tokens
# embedded in -O/-T host specs are decomposed in one place.
# Side effects: Publishes the host and full remote command in
# $g_zxfer_ssh_shell_host_result and
# $g_zxfer_ssh_shell_full_remote_command_result.
zxfer_prepare_ssh_shell_command_context() {
	l_host_spec=$1
	l_remote_shell_cmd=$2

	g_zxfer_ssh_shell_host_result=""
	g_zxfer_ssh_shell_full_remote_command_result=""
	g_zxfer_ssh_shell_context_error_result=""
	[ "$l_remote_shell_cmd" = "" ] && return 1

	# Per-role parse memo: -O/-T host specs are fixed after CLI validation,
	# so the host/wrapper split renders once per spec string and later remote
	# commands reuse it. Keyed on the exact spec text, so a memo hit always
	# replays a validated parse of the identical input.
	if [ -n "$l_host_spec" ]; then
		if [ "${g_zxfer_ssh_shell_context_memo_origin_spec:-}" = "$l_host_spec" ]; then
			g_zxfer_ssh_shell_host_result=$g_zxfer_ssh_shell_context_memo_origin_host
			if [ "${g_zxfer_ssh_shell_context_memo_origin_wrapper:-}" != "" ]; then
				g_zxfer_ssh_shell_full_remote_command_result="$g_zxfer_ssh_shell_context_memo_origin_wrapper $l_remote_shell_cmd"
			else
				g_zxfer_ssh_shell_full_remote_command_result=$l_remote_shell_cmd
			fi
			return 0
		fi
		if [ "${g_zxfer_ssh_shell_context_memo_target_spec:-}" = "$l_host_spec" ]; then
			g_zxfer_ssh_shell_host_result=$g_zxfer_ssh_shell_context_memo_target_host
			if [ "${g_zxfer_ssh_shell_context_memo_target_wrapper:-}" != "" ]; then
				g_zxfer_ssh_shell_full_remote_command_result="$g_zxfer_ssh_shell_context_memo_target_wrapper $l_remote_shell_cmd"
			else
				g_zxfer_ssh_shell_full_remote_command_result=$l_remote_shell_cmd
			fi
			return 0
		fi
	fi

	l_context_status=0
	l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host_spec") ||
		l_context_status=$?
	if [ "$l_context_status" -ne 0 ]; then
		g_zxfer_ssh_shell_context_error_result=$l_host_tokens
		return "$l_context_status"
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
	l_wrapper_cmd=""
	if [ "$l_wrapper_tokens" != "" ]; then
		l_wrapper_cmd=$(zxfer_quote_token_stream "$l_wrapper_tokens")
		l_full_remote_cmd="$l_wrapper_cmd $l_remote_shell_cmd"
	fi

	g_zxfer_ssh_shell_host_result=$l_ssh_host
	g_zxfer_ssh_shell_full_remote_command_result=$l_full_remote_cmd
	# Memoize role specs only; plain calls from the invoke path persist the
	# memo in the main shell, command-substituted callers just recompute.
	if [ "$l_host_spec" = "${g_option_O_origin_host:-}" ] && [ -n "$l_host_spec" ]; then
		g_zxfer_ssh_shell_context_memo_origin_spec=$l_host_spec
		g_zxfer_ssh_shell_context_memo_origin_host=$l_ssh_host
		g_zxfer_ssh_shell_context_memo_origin_wrapper=$l_wrapper_cmd
	fi
	if [ "$l_host_spec" = "${g_option_T_target_host:-}" ] && [ -n "$l_host_spec" ]; then
		g_zxfer_ssh_shell_context_memo_target_spec=$l_host_spec
		g_zxfer_ssh_shell_context_memo_target_host=$l_ssh_host
		g_zxfer_ssh_shell_context_memo_target_wrapper=$l_wrapper_cmd
	fi
	return 0
}

# Purpose: Build the SSH shell command for host for the next execution or
# comparison step.
# Usage: Called by SSH transport rendering and remote ZFS execution
# before other helpers consume the assembled value.
#
# Build a shell-ready local ssh command string while preserving any wrapper
# tokens embedded in the -O/-T host spec (for example "host pfexec"). The
# remote command must already be quoted for execution by the remote shell.
zxfer_build_ssh_shell_command_for_host() {
	l_build_ssh_shell_command_for_host_host_spec=$1
	l_build_ssh_shell_command_for_host_remote_shell_cmd=$2

	[ "$l_build_ssh_shell_command_for_host_remote_shell_cmd" = "" ] && return 1

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_build_ssh_shell_command_for_host_host_spec"); then
		:
	else
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi
	if zxfer_prepare_ssh_shell_command_context "$l_build_ssh_shell_command_for_host_host_spec" "$l_build_ssh_shell_command_for_host_remote_shell_cmd"; then
		:
	else
		l_build_ssh_shell_command_for_host_context_status=$?
		if [ "$g_zxfer_ssh_shell_context_error_result" != "" ]; then
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		fi
		return "$l_build_ssh_shell_command_for_host_context_status"
	fi

	l_command_tokens=$(printf '%s\n%s\n%s\n' "$l_transport_tokens" "$g_zxfer_ssh_shell_host_result" "$g_zxfer_ssh_shell_full_remote_command_result")
	zxfer_quote_token_stream "$l_command_tokens"
}

# Purpose: Run the SSH shell command for host through the controlled execution
# path owned by this module.
# Usage: Called by SSH transport rendering and remote ZFS execution once
# planning is complete and zxfer is ready to execute the action.
#
# Execute a shell-ready remote command string through ssh without reparsing a
# local shell string. Wrapper tokens embedded in the -O/-T host spec are
# preserved as part of the single remote command argument.
zxfer_invoke_ssh_shell_command_for_host() {
	l_ssh_invoke_host_spec=$1
	l_ssh_invoke_remote_shell_cmd=$2
	l_ssh_invoke_profile_side=${3:-}

	[ "$l_ssh_invoke_remote_shell_cmd" = "" ] && return 1
	zxfer_profile_record_ssh_invocation \
		"$l_ssh_invoke_host_spec" "$l_ssh_invoke_profile_side"

	if l_ssh_invoke_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host \
		"$l_ssh_invoke_host_spec"); then
		:
	else
		l_ssh_invoke_transport_status=$?
		zxfer_throw_error \
			"$l_ssh_invoke_transport_tokens" "$l_ssh_invoke_transport_status"
	fi
	if zxfer_prepare_ssh_shell_command_context \
		"$l_ssh_invoke_host_spec" "$l_ssh_invoke_remote_shell_cmd"; then
		:
	else
		l_ssh_invoke_context_status=$?
		if [ "$g_zxfer_ssh_shell_context_error_result" != "" ]; then
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		fi
		return "$l_ssh_invoke_context_status"
	fi

	set --
	if [ "$l_ssh_invoke_transport_tokens" != "" ]; then
		while IFS= read -r l_ssh_invoke_token ||
			[ -n "$l_ssh_invoke_token" ]; do
			[ "$l_ssh_invoke_token" = "" ] && continue
			set -- "$@" "$l_ssh_invoke_token"
		done <<EOF
$l_ssh_invoke_transport_tokens
EOF
	fi
	set -- "$@" "$g_zxfer_ssh_shell_host_result" "$g_zxfer_ssh_shell_full_remote_command_result"

	zxfer_record_last_command_argv "$@"
	zxfer_echoV_remote_command_for_host \
		"$l_ssh_invoke_host_spec" "$l_ssh_invoke_profile_side" "$@"
	"$@"
}

# Purpose: Restore the caller's set or unset LC_ALL state after byte chunking.
# Usage: Internal companion to zxfer_build_remote_sh_c_command.
zxfer_restore_remote_sh_c_lc_all() {
	if [ "$1" -eq 1 ]; then
		LC_ALL=$2
	else
		unset LC_ALL
	fi
}

# Purpose: Build a csh-safe remote `sh -c` command without changing the script
# bytes or consuming its standard input.
# Usage: Called by SSH transport rendering and remote ZFS execution before the
# login shell receives a command string. Short scripts retain the historical
# rendering; longer or multiline scripts use bounded argv chunks so illumos
# csh never has to lex one oversized word.
zxfer_build_remote_sh_c_command() {
	l_build_remote_sh_c_command_script=$1
	l_build_remote_sh_c_command_newline='
'
	l_build_remote_sh_c_command_use_chunks=0
	l_build_remote_sh_c_command_status=0

	if [ "${LC_ALL+x}" = "x" ]; then
		l_build_remote_sh_c_command_saved_lc_all_set=1
		l_build_remote_sh_c_command_saved_lc_all=$LC_ALL
	else
		l_build_remote_sh_c_command_saved_lc_all_set=0
		l_build_remote_sh_c_command_saved_lc_all=""
	fi
	LC_ALL=C
	case $l_build_remote_sh_c_command_script in
	*"$l_build_remote_sh_c_command_newline"*)
		l_build_remote_sh_c_command_use_chunks=1
		;;
	*)
		if l_build_remote_sh_c_command_candidate=$(
			zxfer_build_shell_command_from_argv \
				"sh" "-c" "$l_build_remote_sh_c_command_script"
		); then
			:
		else
			l_build_remote_sh_c_command_status=$?
		fi
		# Bound the complete command, not just its script argument, with ample
		# headroom below illumos csh's 1020-character lexical buffer. This
		# pattern is 768 single-byte characters in the C locale.
		l_build_remote_sh_c_command_safe_pattern='????????????'
		l_build_remote_sh_c_command_safe_pattern=$l_build_remote_sh_c_command_safe_pattern$l_build_remote_sh_c_command_safe_pattern
		l_build_remote_sh_c_command_safe_pattern=$l_build_remote_sh_c_command_safe_pattern$l_build_remote_sh_c_command_safe_pattern
		l_build_remote_sh_c_command_safe_pattern=$l_build_remote_sh_c_command_safe_pattern$l_build_remote_sh_c_command_safe_pattern
		l_build_remote_sh_c_command_safe_pattern=$l_build_remote_sh_c_command_safe_pattern$l_build_remote_sh_c_command_safe_pattern
		l_build_remote_sh_c_command_safe_pattern=$l_build_remote_sh_c_command_safe_pattern$l_build_remote_sh_c_command_safe_pattern
		l_build_remote_sh_c_command_safe_pattern=$l_build_remote_sh_c_command_safe_pattern$l_build_remote_sh_c_command_safe_pattern
		case $l_build_remote_sh_c_command_candidate in
		${l_build_remote_sh_c_command_safe_pattern}?*)
			l_build_remote_sh_c_command_use_chunks=1
			;;
		esac
		;;
	esac

	if [ "$l_build_remote_sh_c_command_status" -ne 0 ]; then
		zxfer_restore_remote_sh_c_lc_all \
			"$l_build_remote_sh_c_command_saved_lc_all_set" \
			"$l_build_remote_sh_c_command_saved_lc_all"
		printf '%s' "$l_build_remote_sh_c_command_candidate"
		return "$l_build_remote_sh_c_command_status"
	fi

	if [ "$l_build_remote_sh_c_command_use_chunks" -eq 0 ]; then
		zxfer_restore_remote_sh_c_lc_all \
			"$l_build_remote_sh_c_command_saved_lc_all_set" \
			"$l_build_remote_sh_c_command_saved_lc_all"
		printf '%s' "$l_build_remote_sh_c_command_candidate"
		return $?
	fi

	# The fixed bootstrap uses only POSIX sh builtins. Data chunks never contain
	# a newline; tagged `n` arguments restore every original newline, including
	# consecutive and trailing ones. `exec` keeps stdin and the script status.
	# shellcheck disable=SC2016 # Expanded by the remote bootstrap, not locally.
	l_build_remote_sh_c_command_bootstrap='l_nl=$(printf "\\nx") || exit $?; l_nl=${l_nl%x}; l_script=; for l_part do case $l_part in d*) l_script=$l_script${l_part#d} ;; n) l_script=$l_script$l_nl ;; *) exit 125 ;; esac; done; exec sh -c "$l_script"'
	set -- sh -c "$l_build_remote_sh_c_command_bootstrap" sh

	# 128 raw bytes expand to at most 515 rendered bytes even when every byte is
	# a single quote, keeping each csh lexical word comfortably below its limit.
	l_build_remote_sh_c_command_chunk_pattern='????????'
	l_build_remote_sh_c_command_chunk_pattern=$l_build_remote_sh_c_command_chunk_pattern$l_build_remote_sh_c_command_chunk_pattern
	l_build_remote_sh_c_command_chunk_pattern=$l_build_remote_sh_c_command_chunk_pattern$l_build_remote_sh_c_command_chunk_pattern
	l_build_remote_sh_c_command_chunk_pattern=$l_build_remote_sh_c_command_chunk_pattern$l_build_remote_sh_c_command_chunk_pattern
	l_build_remote_sh_c_command_chunk_pattern=$l_build_remote_sh_c_command_chunk_pattern$l_build_remote_sh_c_command_chunk_pattern
	l_build_remote_sh_c_command_remaining=$l_build_remote_sh_c_command_script
	while [ -n "$l_build_remote_sh_c_command_remaining" ]; do
		case $l_build_remote_sh_c_command_remaining in
		"$l_build_remote_sh_c_command_newline"*)
			set -- "$@" n
			l_build_remote_sh_c_command_remaining=${l_build_remote_sh_c_command_remaining#"$l_build_remote_sh_c_command_newline"}
			continue
			;;
		*"$l_build_remote_sh_c_command_newline"*)
			l_build_remote_sh_c_command_before_newline=${l_build_remote_sh_c_command_remaining%%"$l_build_remote_sh_c_command_newline"*}
			;;
		*)
			l_build_remote_sh_c_command_before_newline=$l_build_remote_sh_c_command_remaining
			;;
		esac

		case $l_build_remote_sh_c_command_before_newline in
		${l_build_remote_sh_c_command_chunk_pattern}*)
			# The unquoted pattern expansion intentionally removes exactly 128
			# C-locale bytes; the derived suffix is quoted when extracting them.
			# shellcheck disable=SC2295
			l_build_remote_sh_c_command_tail=${l_build_remote_sh_c_command_before_newline#$l_build_remote_sh_c_command_chunk_pattern}
			l_build_remote_sh_c_command_chunk=${l_build_remote_sh_c_command_before_newline%"$l_build_remote_sh_c_command_tail"}
			;;
		*)
			l_build_remote_sh_c_command_chunk=$l_build_remote_sh_c_command_before_newline
			;;
		esac
		if [ -z "$l_build_remote_sh_c_command_chunk" ]; then
			l_build_remote_sh_c_command_status=1
			break
		fi
		set -- "$@" "d$l_build_remote_sh_c_command_chunk"
		l_build_remote_sh_c_command_remaining=${l_build_remote_sh_c_command_remaining#"$l_build_remote_sh_c_command_chunk"}
	done

	zxfer_restore_remote_sh_c_lc_all \
		"$l_build_remote_sh_c_command_saved_lc_all_set" \
		"$l_build_remote_sh_c_command_saved_lc_all"
	[ "$l_build_remote_sh_c_command_status" -eq 0 ] ||
		return "$l_build_remote_sh_c_command_status"

	zxfer_build_shell_command_from_argv "$@"
}

# Purpose: Prepare a shell-ready remote command for one host spec.
# Usage: Called during command rendering before ssh wrapping so wrapper-style
# host specs consistently receive an explicit remote `sh -c` command.
# Side effects: Publishes the prepared command in
# $g_zxfer_remote_shell_command_for_host_result.
zxfer_prepare_remote_shell_command_for_host() {
	l_host_spec=$1
	l_remote_shell_cmd=$2

	g_zxfer_remote_shell_command_for_host_result=""
	[ "$l_remote_shell_cmd" != "" ] || return 1

	l_prepare_status=0
	l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host_spec") ||
		l_prepare_status=$?
	if [ "$l_prepare_status" -ne 0 ]; then
		g_zxfer_remote_shell_command_for_host_result=$l_host_tokens
		return "$l_prepare_status"
	fi

	l_host_token_count=0
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			[ "$l_token" = "" ] && continue
			l_host_token_count=$((l_host_token_count + 1))
		done <<EOF
$l_host_tokens
EOF
	fi

	if [ "$l_host_token_count" -gt 1 ]; then
		l_prepared_remote_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_shell_cmd") ||
			return "$?"
		g_zxfer_remote_shell_command_for_host_result=$l_prepared_remote_cmd
	else
		g_zxfer_remote_shell_command_for_host_result=$l_remote_shell_cmd
	fi
	return 0
}

# Purpose: Build an SSH shell command after preparing the remote command for
# wrapper-style host specs.
# Usage: Called by send/receive and dry-run renderers so host-token diagnostics,
# wrapper `sh -c` handling, and SSH rendering stay centralized.
# Side effects: Publishes the rendered command in
# $g_zxfer_prepared_ssh_shell_command_result or the diagnostic in
# $g_zxfer_prepared_ssh_shell_command_error_result.
zxfer_build_prepared_ssh_shell_command_for_host() {
	l_build_prepared_ssh_shell_command_for_host_host_spec=$1
	l_build_prepared_ssh_shell_command_for_host_remote_shell_cmd=$2

	g_zxfer_prepared_ssh_shell_command_result=""
	g_zxfer_prepared_ssh_shell_command_error_result=""
	if zxfer_prepare_remote_shell_command_for_host "$l_build_prepared_ssh_shell_command_for_host_host_spec" "$l_build_prepared_ssh_shell_command_for_host_remote_shell_cmd"; then
		:
	else
		l_build_prepared_ssh_shell_command_for_host_prepare_status=$?
		if [ "$g_zxfer_remote_shell_command_for_host_result" != "" ]; then
			g_zxfer_prepared_ssh_shell_command_error_result=$g_zxfer_remote_shell_command_for_host_result
		fi
		return "$l_build_prepared_ssh_shell_command_for_host_prepare_status"
	fi

	l_build_status=0
	l_rendered_command=$(zxfer_build_ssh_shell_command_for_host "$l_build_prepared_ssh_shell_command_for_host_host_spec" "$g_zxfer_remote_shell_command_for_host_result") ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		if [ "$l_rendered_command" != "" ]; then
			g_zxfer_prepared_ssh_shell_command_error_result=$l_rendered_command
		fi
		return "$l_build_status"
	fi
	g_zxfer_prepared_ssh_shell_command_result=$l_rendered_command
	printf '%s' "$g_zxfer_prepared_ssh_shell_command_result"
}

# Purpose: Publish a prepared SSH shell command or rethrow the captured
# diagnostic outside command substitutions.
# Usage: Called by modules that need the rendered command in
# $g_zxfer_prepared_ssh_shell_command_result while preserving failure text.
# Side effects: Publishes the rendered command or exits through zxfer_throw_error.
zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
	l_publish_prepared_ssh_shell_command_for_host_or_throw_host_spec=$1
	l_publish_prepared_ssh_shell_command_for_host_or_throw_remote_shell_cmd=$2

	zxfer_build_prepared_ssh_shell_command_for_host "$l_publish_prepared_ssh_shell_command_for_host_or_throw_host_spec" "$l_publish_prepared_ssh_shell_command_for_host_or_throw_remote_shell_cmd" >/dev/null
	l_publish_prepared_ssh_shell_command_for_host_or_throw_prepare_status=$?
	if [ "$l_publish_prepared_ssh_shell_command_for_host_or_throw_prepare_status" -eq 0 ]; then
		return 0
	fi
	if [ "$g_zxfer_prepared_ssh_shell_command_error_result" != "" ]; then
		zxfer_throw_error "$g_zxfer_prepared_ssh_shell_command_error_result" "$l_publish_prepared_ssh_shell_command_for_host_or_throw_prepare_status"
	fi
	return "$l_publish_prepared_ssh_shell_command_for_host_or_throw_prepare_status"
}

# Purpose: Run the source ZFS command through the controlled execution path
# owned by this module.
# Usage: Called by SSH transport rendering and remote ZFS execution once
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
# Usage: Called by SSH transport rendering and remote ZFS execution once
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

	l_destination_zfs_command=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_destination_zfs_remote_tokens=$(printf '%s\n' \
		"$l_destination_zfs_command")
	for l_destination_zfs_arg in "$@"; do
		l_destination_zfs_remote_tokens=$(printf '%s\n%s' \
			"$l_destination_zfs_remote_tokens" "$l_destination_zfs_arg")
	done
	l_destination_zfs_remote_command=$(zxfer_quote_token_stream \
		"$l_destination_zfs_remote_tokens")
	zxfer_invoke_ssh_shell_command_for_host "$g_option_T_target_host" \
		"$l_destination_zfs_remote_command" destination
}

# Purpose: Render the source ZFS command as a stable shell-safe or operator-
# facing string.
# Usage: Called by SSH transport rendering and remote ZFS execution when
# zxfer needs to display or transport the value without reparsing it.
zxfer_render_source_zfs_command() {
	l_source_render_subcommand=$1
	shift

	if [ "$g_option_O_origin_host" = "" ]; then
		l_source_render_zfs_command=$g_cmd_zfs
		if [ -n "$g_LZFS" ] && [ "$g_LZFS" != "$g_cmd_zfs" ]; then
			l_source_render_zfs_command=$g_LZFS
		fi
		zxfer_build_shell_command_from_argv \
			"$l_source_render_zfs_command" "$l_source_render_subcommand" "$@"
		return
	fi

	l_source_render_zfs_command=${g_origin_cmd_zfs:-$g_cmd_zfs}
	l_source_render_remote_tokens=$(printf '%s\n%s' \
		"$l_source_render_zfs_command" "$l_source_render_subcommand")
	for l_source_render_arg in "$@"; do
		l_source_render_remote_tokens=$(printf '%s\n%s' \
			"$l_source_render_remote_tokens" "$l_source_render_arg")
	done
	l_source_render_remote_command=$(zxfer_quote_token_stream \
		"$l_source_render_remote_tokens")
	zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" \
		"$l_source_render_remote_command"
}

# Purpose: Render the destination ZFS command as a stable shell-safe or
# operator-facing string.
# Usage: Called by SSH transport rendering and remote ZFS execution when
# zxfer needs to display or transport the value without reparsing it.
zxfer_render_destination_zfs_command() {
	l_destination_render_subcommand=$1
	shift

	if [ "$g_option_T_target_host" = "" ]; then
		l_destination_render_zfs_command=$g_cmd_zfs
		if [ -n "$g_RZFS" ] && [ "$g_RZFS" != "$g_cmd_zfs" ]; then
			l_destination_render_zfs_command=$g_RZFS
		fi
		zxfer_build_shell_command_from_argv \
			"$l_destination_render_zfs_command" \
			"$l_destination_render_subcommand" "$@"
		return
	fi

	l_destination_render_zfs_command=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_destination_render_remote_tokens=$(printf '%s\n%s' \
		"$l_destination_render_zfs_command" \
		"$l_destination_render_subcommand")
	for l_destination_render_arg in "$@"; do
		l_destination_render_remote_tokens=$(printf '%s\n%s' \
			"$l_destination_render_remote_tokens" \
			"$l_destination_render_arg")
	done
	l_destination_render_remote_command=$(zxfer_quote_token_stream \
		"$l_destination_render_remote_tokens")
	zxfer_build_ssh_shell_command_for_host "$g_option_T_target_host" \
		"$l_destination_render_remote_command"
}

# Purpose: Render the ZFS command for spec as a stable shell-safe or operator-
# facing string.
# Usage: Called by SSH transport rendering and remote ZFS execution when
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
# Usage: Called by SSH transport rendering and remote ZFS execution once
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

################################################################################
# SSH CONTROL SOCKET SETUP / TEARDOWN
################################################################################

# Purpose: Set up the per-run SSH control socket for one remote role.
# Usage: Called during remote bootstrap and ssh control-socket management
# before remote probes or replication traffic reuse a multiplexed transport.
#
# setup an ssh control socket for the specified role (origin or target)
zxfer_setup_ssh_control_socket() {
	l_setup_ssh_control_socket_host=$1
	l_setup_ssh_control_socket_role=$2

	[ -z "$l_setup_ssh_control_socket_host" ] && return

	case "$l_setup_ssh_control_socket_role" in
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
	if ! l_control_socket=$(zxfer_get_ssh_control_socket_path_for_role "$l_setup_ssh_control_socket_role"); then
		zxfer_throw_error "Error creating ssh control socket for $l_setup_ssh_control_socket_role host."
	fi
	if ! l_setup_ssh_control_socket_transport_tokens=$(zxfer_get_ssh_base_transport_tokens); then
		zxfer_throw_error "$l_setup_ssh_control_socket_transport_tokens"
	fi

	# The socket lives under the private per-run directory, so it can only
	# pre-exist when this run already opened a master for the role; the
	# `-O check` gate keeps that reuse honest and reaps a stale leftover
	# before a fresh master is opened.
	if [ -e "$l_control_socket" ] || [ -L "$l_control_socket" ] ||
		[ -h "$l_control_socket" ]; then
		if zxfer_check_ssh_control_socket_for_host "$l_setup_ssh_control_socket_host" "$l_control_socket"; then
			zxfer_set_ssh_control_socket_role_state "$l_setup_ssh_control_socket_role" "$l_control_socket"
			return 0
		fi
		case "${g_zxfer_ssh_control_socket_action_result:-}" in
		stale)
			rm -f "$l_control_socket"
			;;
		*)
			zxfer_emit_ssh_control_socket_action_failure_message \
				"Error checking ssh control socket for $l_setup_ssh_control_socket_role host." >&2
			zxfer_throw_error "Error creating ssh control socket for $l_setup_ssh_control_socket_role host."
			;;
		esac
	fi

	if ! zxfer_open_ssh_control_socket_for_host "$l_setup_ssh_control_socket_host" "$l_control_socket"; then
		zxfer_throw_error "Error creating ssh control socket for $l_setup_ssh_control_socket_role host."
	fi
	zxfer_set_ssh_control_socket_role_state "$l_setup_ssh_control_socket_role" "$l_control_socket"
}

# Purpose: Close one role's SSH control socket and release the related state.
# Usage: Called during remote bootstrap and ssh control-socket management
# after protected work finishes or trap cleanup takes over. A close failure
# keeps the role state so trap cleanup reports it instead of claiming a clean
# run.
zxfer_close_ssh_control_socket_for_role() {
	l_close_ssh_control_socket_for_role_role=$1

	case "$l_close_ssh_control_socket_for_role_role" in
	origin)
		l_close_ssh_control_socket_for_role_host=${g_option_O_origin_host:-}
		l_control_socket=${g_ssh_origin_control_socket:-}
		;;
	target)
		l_close_ssh_control_socket_for_role_host=${g_option_T_target_host:-}
		l_control_socket=${g_ssh_target_control_socket:-}
		;;
	*)
		return 1
		;;
	esac
	if [ "$l_close_ssh_control_socket_for_role_host" = "" ] || [ "$l_control_socket" = "" ]; then
		return 0
	fi

	if zxfer_run_ssh_control_socket_action_for_host "$l_close_ssh_control_socket_for_role_host" "$l_control_socket" exit; then
		zxfer_echoV "Closing $l_close_ssh_control_socket_for_role_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
	elif [ "${g_zxfer_ssh_control_socket_action_result:-}" = "stale" ]; then
		zxfer_echoV "Closing $l_close_ssh_control_socket_for_role_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
	else
		zxfer_echoV "Closing $l_close_ssh_control_socket_for_role_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
		zxfer_emit_ssh_control_socket_action_failure_message \
			"Error closing $l_close_ssh_control_socket_for_role_role ssh control socket." >&2
		return 1
	fi
	rm -f "$l_control_socket" 2>/dev/null || :
	zxfer_clear_ssh_control_socket_role_state "$l_close_ssh_control_socket_for_role_role"
	return 0
}

# Purpose: Close the origin SSH control socket and release the related handles
# or state.
# Usage: Called during SSH transport setup, rendering, and control-
# socket management after the protected work finishes or cleanup takes over.
zxfer_close_origin_ssh_control_socket() {
	zxfer_close_ssh_control_socket_for_role origin
}

# Purpose: Close the target SSH control socket and release the related handles
# or state.
# Usage: Called during SSH transport setup, rendering, and control-
# socket management after the protected work finishes or cleanup takes over.
zxfer_close_target_ssh_control_socket() {
	zxfer_close_ssh_control_socket_for_role target
}

# Purpose: Close the all SSH control sockets and release the related handles or
# state.
# Usage: Called during SSH transport setup, rendering, and control-
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
			zxfer_set_failure_class dependency
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
# Usage: Called during SSH transport setup, rendering, and control-
# socket management after inputs change and downstream helpers need the derived
# value rebuilt.
#
# shellcheck disable=SC2034
zxfer_refresh_remote_zfs_commands() {
	if [ "$g_option_O_origin_host" != "" ]; then
		if ! l_origin_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_O_origin_host"); then
			zxfer_throw_usage_error "$l_origin_host_safe" 2
		fi
		g_LZFS=${g_origin_cmd_zfs:-$g_cmd_zfs}
	else
		g_LZFS=$g_cmd_zfs
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		if ! l_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host"); then
			zxfer_throw_usage_error "$l_target_host_safe" 2
		fi
		g_RZFS=${g_target_cmd_zfs:-$g_cmd_zfs}
	else
		g_RZFS=$g_cmd_zfs
	fi
}
