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
# owns globals: per-host ssh control-socket state plus remote capability/tool resolution state such as g_ssh_origin_control_socket*, g_ssh_target_control_socket*, g_origin_remote_capabilities_*, g_target_remote_capabilities_*, and the resolved remote zfs helper selections.
# reads globals: g_cmd_ssh, g_option_O_*/g_option_T_*, local helper paths, and temp-root helpers.
# mutates caches: remote capability cache files plus run-owned ssh control-socket and remote-capability cache directories.
# returns via stdout: remote OS/tool paths, ssh argv renderings, and remote-safe command strings.

ZXFER_SSH_CONTROL_SOCKET_PATH_MAX=104
ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE=".Mvij6x1tYLn6woxm"
ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=20
ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES=20

# Purpose: Record the SSH control socket lock wait metrics for later
# diagnostics or control decisions.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs the state preserved for follow-on helpers
# or reporting.
zxfer_record_ssh_control_socket_lock_wait_metrics() {
	l_waited=$1
	l_wait_start_ms=${2:-}

	[ "$l_waited" -eq 1 ] || return 0
	zxfer_profile_increment_counter g_zxfer_profile_ssh_control_socket_lock_wait_count
	zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_control_socket_lock_wait_ms "$l_wait_start_ms"
}

# Purpose: Record the remote capability cache wait metrics for later
# diagnostics or control decisions.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs the state preserved for follow-on helpers
# or reporting.
zxfer_record_remote_capability_cache_wait_metrics() {
	l_waited=$1
	l_wait_start_ms=${2:-}

	[ "$l_waited" -eq 1 ] || return 0
	zxfer_profile_increment_counter g_zxfer_profile_remote_capability_cache_wait_count
	zxfer_profile_add_elapsed_ms g_zxfer_profile_remote_capability_cache_wait_ms "$l_wait_start_ms"
}

# Purpose: Reset the SSH control socket lock state so the next remote-host pass
# starts from a clean state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_ssh_control_socket_lock_state() {
	g_zxfer_ssh_control_socket_lock_dir_result=""
	g_zxfer_ssh_control_socket_lock_error=""
	g_zxfer_ssh_control_socket_lease_count_result=""
}

# Purpose: Record the SSH control socket lock error for later diagnostics or
# control decisions.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs the state preserved for follow-on helpers
# or reporting.
zxfer_note_ssh_control_socket_lock_error() {
	g_zxfer_ssh_control_socket_lock_error=$1
}

# Purpose: Emit the SSH control socket lock failure message in the operator-
# facing format owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_emit_ssh_control_socket_lock_failure_message() {
	l_default_message=${1:-}

	if [ -n "${g_zxfer_ssh_control_socket_lock_error:-}" ]; then
		if [ -n "$l_default_message" ]; then
			l_default_prefix=${l_default_message%.}
			printf '%s: %s\n' "$l_default_prefix" \
				"$g_zxfer_ssh_control_socket_lock_error"
			return 0
		fi
		printf '%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		return 0
	fi
	[ -z "$l_default_message" ] || printf '%s\n' "$l_default_message"
}

zxfer_get_ssh_control_socket_lock_purpose() {
	printf '%s\n' "ssh-control-socket-lock"
}

zxfer_get_ssh_control_socket_lease_purpose() {
	printf '%s\n' "ssh-control-socket-lease"
}

zxfer_get_remote_capability_cache_lock_purpose() {
	printf '%s\n' "remote-capability-cache-lock"
}

# Purpose: Return the remote host cache root prefix in the form expected by
# later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_host_cache_root_prefix() {
	if [ -n "${g_zxfer_temp_prefix:-}" ]; then
		printf '%s\n' "$g_zxfer_temp_prefix"
		return 0
	fi

	g_zxfer_temp_prefix="zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)"
	printf '%s\n' "$g_zxfer_temp_prefix"
}

################################################################################
# SSH CONTROL SOCKET SUPPORT / CACHE KEYS
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

# Purpose: Return the resolved local ssh helper in the form expected by later
# helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when remote transport is actually needed so local-only runs
# do not hard-require ssh during startup.
zxfer_get_resolved_local_ssh_command() {
	if ! zxfer_ensure_local_ssh_command; then
		printf '%s\n' "$g_zxfer_resolved_local_ssh_command_result"
		return 1
	fi

	printf '%s\n' "$g_zxfer_resolved_local_ssh_command_result"
}

# Purpose: Manage SSH control socket cache key for remote transport
# coordination.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management while zxfer sets up, validates, or tears down multiplexed
# SSH state.
zxfer_ssh_control_socket_cache_key() {
	l_host_spec=$1
	if ! l_policy_identity=$(zxfer_render_ssh_transport_policy_identity); then
		[ "$l_policy_identity" = "" ] || printf '%s\n' "$l_policy_identity"
		return 1
	fi
	l_identity=$(printf '%s\n%s\n%s\n' "${g_cmd_ssh:-ssh}" "$l_policy_identity" "$l_host_spec")
	if l_key_cksum=$(printf '%s' "$l_identity" | cksum 2>/dev/null); then
		# shellcheck disable=SC2086
		set -- $l_key_cksum
		if [ $# -ge 2 ] && [ -n "$1" ] && [ -n "$2" ]; then
			printf 'k%s.%s\n' "$1" "$2"
			return 0
		fi
	fi
	l_key_hex=$(printf '%s' "$l_identity" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n' | cut -c 1-12)
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	printf 'k%s\n' "$l_key_hex"
}

# Purpose: Render the SSH control socket entry identity as a stable shell-safe
# or operator-facing string.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to display or transport the value without
# reparsing it.
zxfer_render_ssh_control_socket_entry_identity() {
	l_host_spec=$1
	if ! l_policy_identity=$(zxfer_render_ssh_transport_policy_identity); then
		[ "$l_policy_identity" = "" ] || printf '%s\n' "$l_policy_identity"
		return 1
	fi
	printf '%s\n%s\n%s\n' "${g_cmd_ssh:-ssh}" "$l_policy_identity" "$l_host_spec"
}

# Purpose: Check whether the control socket entry path short enough is SSH.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a boolean answer about a validated
# or trusted state.
zxfer_is_ssh_control_socket_entry_path_short_enough() {
	l_entry_dir=$1
	l_temp_listener_path="$l_entry_dir/s$ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE"

	[ "${#l_temp_listener_path}" -lt "$ZXFER_SSH_CONTROL_SOCKET_PATH_MAX" ]
}

# Purpose: Return the SSH control socket cache directory for key in the form
# expected by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_ssh_control_socket_cache_dir_for_key() {
	l_cache_key=$1

	if ! l_cache_dir=$(zxfer_ensure_ssh_control_socket_cache_dir); then
		return 1
	fi
	if zxfer_is_ssh_control_socket_entry_path_short_enough "$l_cache_dir/$l_cache_key"; then
		printf '%s\n' "$l_cache_dir"
		return 0
	fi

	if ! l_short_cache_dir=$(
		unset TMPDIR
		zxfer_ensure_ssh_control_socket_cache_dir
	); then
		return 1
	fi
	if [ "$l_short_cache_dir" = "$l_cache_dir" ]; then
		return 1
	fi
	if ! zxfer_is_ssh_control_socket_entry_path_short_enough "$l_short_cache_dir/$l_cache_key"; then
		return 1
	fi

	zxfer_echoV "Ignoring TMPDIR ${TMPDIR:-} for ssh control sockets; using shorter cache root $l_short_cache_dir."
	printf '%s\n' "$l_short_cache_dir"
}

# Purpose: Read the SSH control socket entry identity file from staged state
# into the current shell.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a checked reload instead of ad hoc
# file reads.
zxfer_read_ssh_control_socket_entry_identity_file() {
	l_identity_path=$1

	[ -f "$l_identity_path" ] || return 1
	[ ! -L "$l_identity_path" ] || return 1
	[ ! -h "$l_identity_path" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_identity_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_identity_path"); then
		return 1
	fi
	[ "$l_mode" = "600" ] || return 1

	zxfer_read_runtime_artifact_file "$l_identity_path" 2>/dev/null
}

# Purpose: Write the SSH control socket entry identity file in the normalized
# form later zxfer steps expect.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when the module needs a stable staged file or emitted
# stream for downstream use.
zxfer_write_ssh_control_socket_entry_identity_file() {
	l_entry_dir=$1
	l_host_spec=$2
	l_identity_path="$l_entry_dir/id"

	[ ! -L "$l_identity_path" ] || return 1
	[ ! -h "$l_identity_path" ] || return 1
	if [ -e "$l_identity_path" ]; then
		[ -f "$l_identity_path" ] || return 1
		if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
			return 1
		fi
		if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_identity_path"); then
			return 1
		fi
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	fi
	if ! zxfer_stage_runtime_artifact_file_for_path "$l_identity_path" "zxfer-ssh-control-id" >/dev/null; then
		return 1
	fi
	l_tmp_identity_path=$g_zxfer_runtime_artifact_path_result

	if ! l_identity_contents=$(zxfer_render_ssh_control_socket_entry_identity "$l_host_spec"); then
		zxfer_cleanup_runtime_artifact_path "$l_tmp_identity_path"
		return 1
	fi

	if ! zxfer_write_runtime_artifact_file "$l_tmp_identity_path" "$l_identity_contents"; then
		zxfer_cleanup_runtime_artifact_path "$l_tmp_identity_path"
		return 1
	fi

	if ! zxfer_publish_runtime_artifact_file "$l_tmp_identity_path" "$l_identity_path"; then
		zxfer_cleanup_runtime_artifact_path "$l_tmp_identity_path"
		return 1
	fi
	chmod 600 "$l_identity_path" 2>/dev/null || :
	return 0
}

# Purpose: Ensure the SSH control socket cache directory exists and is ready
# before the flow continues.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before later helpers assume the resource or cache is
# available.
zxfer_ensure_ssh_control_socket_cache_dir() {
	if ! l_tmpdir=$(zxfer_try_get_socket_cache_tmpdir); then
		return 1
	fi
	if ! l_cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$l_tmpdir"); then
		return 1
	fi
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi

	if [ -L "$l_cache_dir" ] || [ -h "$l_cache_dir" ]; then
		return 1
	fi

	if [ -e "$l_cache_dir" ]; then
		[ -d "$l_cache_dir" ] || return 1
		if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_cache_dir"); then
			return 1
		fi
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
		if ! l_mode=$(zxfer_get_path_mode_octal "$l_cache_dir"); then
			return 1
		fi
		[ "$l_mode" = "700" ] || return 1
		printf '%s\n' "$l_cache_dir"
		return 0
	fi

	l_old_umask=$(umask)
	umask 077
	if ! mkdir "$l_cache_dir" 2>/dev/null; then
		umask "$l_old_umask"
		return 1
	fi
	umask "$l_old_umask"

	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_cache_dir"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_cache_dir"); then
		return 1
	fi
	[ "$l_mode" = "700" ] || return 1
	printf '%s\n' "$l_cache_dir"
}

# Purpose: Return the SSH control-socket cache directory path for one temporary
# root.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before zxfer creates or validates the per-run cache
# directory for multiplexed SSH state.
zxfer_ssh_control_socket_cache_dir_path_for_tmpdir() {
	l_tmpdir=$1

	[ -n "$l_tmpdir" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_root_prefix=$(zxfer_get_remote_host_cache_root_prefix); then
		return 1
	fi

	printf '%s/%s.s.%s.d\n' "$l_tmpdir" "$l_root_prefix" "$l_effective_uid"
}

# Purpose: Ensure the SSH control socket entry directory exists and is ready
# before the flow continues.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before later helpers assume the resource or cache is
# available.
zxfer_ensure_ssh_control_socket_entry_dir() {
	l_host_spec=$1

	if ! l_cache_key=$(zxfer_ssh_control_socket_cache_key "$l_host_spec"); then
		[ "$l_cache_key" = "" ] || printf '%s\n' "$l_cache_key"
		return 1
	fi
	if ! l_cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$l_cache_key"); then
		return 1
	fi
	if ! l_expected_identity=$(zxfer_render_ssh_control_socket_entry_identity "$l_host_spec"); then
		[ "$l_expected_identity" = "" ] || printf '%s\n' "$l_expected_identity"
		return 1
	fi
	l_suffix=0
	while :; do
		if [ "$l_suffix" -eq 0 ]; then
			l_entry_dir="$l_cache_dir/$l_cache_key"
		else
			l_entry_dir="$l_cache_dir/$l_cache_key.$l_suffix"
		fi

		if ! zxfer_is_ssh_control_socket_entry_path_short_enough "$l_entry_dir"; then
			return 1
		fi

		if [ -L "$l_entry_dir" ] || [ -h "$l_entry_dir" ]; then
			return 1
		fi

		if [ -e "$l_entry_dir" ]; then
			[ -d "$l_entry_dir" ] || return 1
			if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_entry_dir"); then
				return 1
			fi
			if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
				return 1
			fi
			[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
			if ! l_mode=$(zxfer_get_path_mode_octal "$l_entry_dir"); then
				return 1
			fi
			[ "$l_mode" = "700" ] || return 1
		else
			l_old_umask=$(umask)
			umask 077
			if ! mkdir "$l_entry_dir" 2>/dev/null; then
				umask "$l_old_umask"
				if [ ! -d "$l_entry_dir" ]; then
					return 1
				fi
			else
				umask "$l_old_umask"
			fi
			if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_entry_dir"); then
				return 1
			fi
			if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
				return 1
			fi
			[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
			if ! l_mode=$(zxfer_get_path_mode_octal "$l_entry_dir"); then
				return 1
			fi
			[ "$l_mode" = "700" ] || return 1
		fi

		l_leases_dir="$l_entry_dir/leases"
		if [ -L "$l_leases_dir" ] || [ -h "$l_leases_dir" ]; then
			return 1
		fi
		if [ -e "$l_leases_dir" ]; then
			[ -d "$l_leases_dir" ] || return 1
			if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_leases_dir"); then
				return 1
			fi
			if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
				return 1
			fi
			[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
			if ! l_mode=$(zxfer_get_path_mode_octal "$l_leases_dir"); then
				return 1
			fi
			[ "$l_mode" = "700" ] || return 1
		else
			l_old_umask=$(umask)
			umask 077
			if ! mkdir "$l_leases_dir" 2>/dev/null; then
				umask "$l_old_umask"
				if [ ! -d "$l_leases_dir" ]; then
					return 1
				fi
			else
				umask "$l_old_umask"
			fi
			if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_leases_dir"); then
				return 1
			fi
			if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
				return 1
			fi
			[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
			if ! l_mode=$(zxfer_get_path_mode_octal "$l_leases_dir"); then
				return 1
			fi
			[ "$l_mode" = "700" ] || return 1
		fi

		l_identity_path="$l_entry_dir/id"
		if [ -e "$l_identity_path" ]; then
			if l_identity_contents=$(zxfer_read_ssh_control_socket_entry_identity_file "$l_identity_path"); then
				if [ "$l_identity_contents" = "$l_expected_identity" ]; then
					printf '%s\n' "$l_entry_dir"
					return 0
				fi
			else
				return 1
			fi
			l_suffix=$((l_suffix + 1))
			continue
		fi

		if ! zxfer_write_ssh_control_socket_entry_identity_file "$l_entry_dir" "$l_host_spec"; then
			return 1
		fi
		printf '%s\n' "$l_entry_dir"
		return 0
	done
}

# Purpose: Acquire the SSH control socket lock so concurrent zxfer work does
# not reuse it unsafely.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before a shared cache, lock, or transport resource is used
# by this run.
zxfer_acquire_ssh_control_socket_lock() {
	l_entry_dir=$1
	l_lock_dir="$l_entry_dir.lock"
	l_attempts=0
	l_fast_retries=${ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES:-20}
	l_waited=0
	l_wait_start_ms=""

	zxfer_reset_ssh_control_socket_lock_state
	while ! zxfer_create_ssh_control_socket_lock_dir "$l_lock_dir"; do
		if [ -L "$l_lock_dir" ] || [ -h "$l_lock_dir" ]; then
			zxfer_note_ssh_control_socket_lock_error \
				"Refusing symlinked ssh control socket lock path \"$l_lock_dir\"."
			zxfer_record_ssh_control_socket_lock_wait_metrics "$l_waited" "$l_wait_start_ms"
			return 1
		fi
		if [ -d "$l_lock_dir" ]; then
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$l_lock_dir" 0
			l_reap_status=$?
			if [ "$l_reap_status" -eq 0 ]; then
				continue
			fi
			if [ "$l_reap_status" -eq 1 ]; then
				zxfer_record_ssh_control_socket_lock_wait_metrics "$l_waited" "$l_wait_start_ms"
				return 1
			fi
		fi

		if [ "$l_waited" -eq 0 ]; then
			l_waited=1
			l_wait_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
		fi

		case "$l_fast_retries" in
		'' | *[!0-9]*)
			l_fast_retries=0
			;;
		esac
		if [ "$l_fast_retries" -gt 0 ]; then
			l_fast_retries=$((l_fast_retries - 1))
			continue
		fi

		l_attempts=$((l_attempts + 1))
		if [ "$l_attempts" -ge 10 ]; then
			if [ -d "$l_lock_dir" ]; then
				zxfer_try_reap_stale_ssh_control_socket_lock_dir "$l_lock_dir" 1
				l_reap_status=$?
				if [ "$l_reap_status" -eq 0 ]; then
					continue
				fi
				if [ "$l_reap_status" -eq 1 ]; then
					zxfer_record_ssh_control_socket_lock_wait_metrics "$l_waited" "$l_wait_start_ms"
					return 1
				fi
			fi
			zxfer_note_ssh_control_socket_lock_error \
				"Timed out waiting for ssh control socket lock path \"$l_lock_dir\"."
			zxfer_record_ssh_control_socket_lock_wait_metrics "$l_waited" "$l_wait_start_ms"
			return 1
		fi
		sleep 1
	done

	zxfer_reset_ssh_control_socket_lock_state
	zxfer_record_ssh_control_socket_lock_wait_metrics "$l_waited" "$l_wait_start_ms"
	g_zxfer_ssh_control_socket_lock_dir_result=$l_lock_dir
	printf '%s\n' "$l_lock_dir"
}

# Purpose: Validate the SSH control socket lock directory before zxfer relies
# on it.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management to fail closed on malformed, unsafe, or stale input.
zxfer_validate_ssh_control_socket_lock_dir() {
	l_lock_dir=$1

	if [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; then
		zxfer_note_ssh_control_socket_lock_error \
			"ssh control socket lock path \"$l_lock_dir\" is missing."
		return 1
	fi
	if [ -L "$l_lock_dir" ] || [ -h "$l_lock_dir" ]; then
		zxfer_note_ssh_control_socket_lock_error \
			"Refusing symlinked ssh control socket lock path \"$l_lock_dir\"."
		return 1
	fi
	if zxfer_load_owned_lock_metadata_for_kind_and_purpose \
		"$l_lock_dir" lock "$(zxfer_get_ssh_control_socket_lock_purpose)"; then
		return 0
	else
		l_status=$?
	fi
	case "$l_status" in
	2)
		zxfer_note_ssh_control_socket_lock_error \
			"ssh control socket lock path \"$l_lock_dir\" has missing or invalid metadata."
		;;
	*)
		zxfer_note_ssh_control_socket_lock_error \
			"ssh control socket lock path \"$l_lock_dir\" failed ownership, permission, or metadata validation."
		;;
	esac
	return 1
}

# Purpose: Validate the SSH control socket lock directory for reap before zxfer
# relies on it.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management to fail closed on malformed, unsafe, or stale input.
zxfer_validate_ssh_control_socket_lock_dir_for_reap() {
	l_lock_dir=$1

	if [ ! -d "$l_lock_dir" ]; then
		zxfer_note_ssh_control_socket_lock_error \
			"ssh control socket lock path \"$l_lock_dir\" is not a directory."
		return 1
	fi
	if [ -L "$l_lock_dir" ] || [ -h "$l_lock_dir" ]; then
		zxfer_note_ssh_control_socket_lock_error \
			"Refusing symlinked ssh control socket lock path \"$l_lock_dir\"."
		return 1
	fi
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		zxfer_note_ssh_control_socket_lock_error \
			"Unable to determine the effective uid for ssh control socket lock validation."
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_lock_dir"); then
		zxfer_note_ssh_control_socket_lock_error \
			"Unable to determine the owner of ssh control socket lock path \"$l_lock_dir\"."
		return 1
	fi
	if [ "$l_owner_uid" != "$l_effective_uid" ]; then
		zxfer_note_ssh_control_socket_lock_error \
			"ssh control socket lock path \"$l_lock_dir\" is not owned by the effective uid."
		return 1
	fi
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_lock_dir"); then
		zxfer_note_ssh_control_socket_lock_error \
			"Unable to determine permissions for ssh control socket lock path \"$l_lock_dir\"."
		return 1
	fi
	case "$l_mode" in
	7[0-5][0-5])
		return 0
		;;
	esac

	zxfer_note_ssh_control_socket_lock_error \
		"Existing ssh control socket lock path \"$l_lock_dir\" has unsupported permissions ($l_mode). Remove the stale lock directory and retry."
	return 1
}

# Purpose: Clean up the SSH control socket lock directory that this module
# created or tracks.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management on success and failure paths so temporary state does not
# linger.
zxfer_cleanup_ssh_control_socket_lock_dir() {
	l_lock_dir=$1

	if zxfer_cleanup_owned_lock_dir "$l_lock_dir"; then
		return 0
	fi
	zxfer_note_ssh_control_socket_lock_error \
		"Unable to remove stale ssh control socket lock path \"$l_lock_dir\"."
	return 1
}

# Purpose: Create the SSH control socket lock directory using the safety checks
# owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs a fresh staged resource or persistent
# helper state.
zxfer_create_ssh_control_socket_lock_dir() {
	l_lock_dir=$1

	if ! zxfer_create_owned_lock_dir \
		"$l_lock_dir" lock "$(zxfer_get_ssh_control_socket_lock_purpose)" >/dev/null; then
		if [ -e "$l_lock_dir" ] || [ -L "$l_lock_dir" ] || [ -h "$l_lock_dir" ]; then
			zxfer_validate_ssh_control_socket_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		fi
		return 1
	fi
	return 0
}

# Purpose: Detect retired pid-file lock directories so current releases fail
# closed instead of reaping or trusting old owner state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before a missing-metadata lock directory is treated as a
# reap candidate.
zxfer_owned_lock_dir_uses_unsupported_pid_file_layout() {
	l_lock_dir=$1
	l_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_lock_dir")
	l_pid_path="$l_lock_dir/pid"

	if [ -e "$l_metadata_path" ] || [ -L "$l_metadata_path" ] ||
		[ -h "$l_metadata_path" ]; then
		return 1
	fi
	if [ -e "$l_pid_path" ] || [ -L "$l_pid_path" ] || [ -h "$l_pid_path" ]; then
		return 0
	fi
	return 1
}

# Purpose: Try to resolve or create the reap stale SSH control socket lock
# directory without treating every miss as fatal.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer has an optional or fallback path that still
# needs one checked helper.
zxfer_try_reap_stale_ssh_control_socket_lock_dir() {
	l_lock_dir=$1
	l_allow_pidless_reap=${2:-0}

	if ! zxfer_validate_ssh_control_socket_lock_dir_for_reap "$l_lock_dir"; then
		return 1
	fi
	if zxfer_owned_lock_dir_uses_unsupported_pid_file_layout "$l_lock_dir"; then
		zxfer_note_ssh_control_socket_lock_error \
			"ssh control socket lock path \"$l_lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
		return 1
	fi
	zxfer_try_reap_stale_owned_lock_dir \
		"$l_lock_dir" "$l_allow_pidless_reap" \
		lock "$(zxfer_get_ssh_control_socket_lock_purpose)"
	l_reap_status=$?
	if [ "$l_reap_status" -eq 0 ]; then
		return 0
	fi
	if [ "$l_reap_status" -eq 2 ]; then
		return 2
	fi
	zxfer_note_ssh_control_socket_lock_error \
		"Unable to reap stale or corrupt ssh control socket lock path \"$l_lock_dir\"."
	return 1
}

# Purpose: Release the SSH control socket lock after the protected work
# finishes.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when a shared cache, lock, or transport resource should no
# longer be held.
zxfer_release_ssh_control_socket_lock() {
	l_lock_dir=$1

	if zxfer_release_owned_lock_dir \
		"$l_lock_dir" lock "$(zxfer_get_ssh_control_socket_lock_purpose)"; then
		return 0
	fi
	zxfer_note_ssh_control_socket_lock_error \
		"Failed to release ssh control socket lock path \"$l_lock_dir\"."
	return 1
}

# Purpose: Warn when the SSH control socket lock could not be released after a
# primary failure already decided the caller's return status.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when lock release should be checked without suppressing the
# original transport, cleanup, or lease failure.
zxfer_warn_ssh_control_socket_lock_release_failure() {
	l_role=$1

	zxfer_emit_ssh_control_socket_lock_failure_message \
		"Warning: Failed to release ssh control socket lock for $l_role host." >&2
}

# Purpose: Release the SSH control socket lock while preserving the caller's
# primary failure when one already exists.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when lock release should be checked but must not suppress
# an earlier lease, cleanup, or transport failure.
zxfer_release_ssh_control_socket_lock_with_precedence() {
	l_role=$1
	l_lock_dir=$2
	l_primary_status=${3:-0}

	[ -n "$l_lock_dir" ] || return "$l_primary_status"

	zxfer_release_ssh_control_socket_lock "$l_lock_dir" >/dev/null 2>&1
	l_release_status=$?
	if [ "$l_release_status" -eq 0 ]; then
		return "$l_primary_status"
	fi
	zxfer_warn_ssh_control_socket_lock_release_failure "$l_role"
	if [ "$l_primary_status" -ne 0 ]; then
		return "$l_primary_status"
	fi
	return 1
}

# Purpose: Reap stale SSH control-socket lease entries that no longer belong to
# a live process.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before lease counts or cleanup decisions rely on the
# current lease directory contents.
zxfer_prune_stale_ssh_control_socket_leases() {
	l_entry_dir=$1
	l_leases_dir="$l_entry_dir/leases"

	[ -d "$l_leases_dir" ] || return 0

	set -- "$l_leases_dir"/lease.*
	if [ ! -e "$1" ] && [ ! -L "$1" ] && [ ! -h "$1" ]; then
		return 0
	fi

	for l_lease_path in "$@"; do
		if [ ! -e "$l_lease_path" ] && [ ! -L "$l_lease_path" ] &&
			[ ! -h "$l_lease_path" ]; then
			continue
		fi
		if [ -L "$l_lease_path" ] || [ -h "$l_lease_path" ]; then
			zxfer_note_ssh_control_socket_lock_error \
				"Refusing symlinked ssh control socket lease entry \"$l_lease_path\"."
			return 1
		fi
		if [ ! -d "$l_lease_path" ]; then
			zxfer_note_ssh_control_socket_lock_error \
				"ssh control socket lease entry \"$l_lease_path\" is not a metadata-bearing directory. Remove the stale entry and retry."
			return 1
		fi
		zxfer_try_reap_stale_owned_lock_dir \
			"$l_lease_path" 0 lease "$(zxfer_get_ssh_control_socket_lease_purpose)" >/dev/null 2>&1
		l_reap_status=$?
		case "$l_reap_status" in
		0 | 2)
			continue
			;;
		esac
		zxfer_note_ssh_control_socket_lock_error \
			"Unable to inspect ssh control socket lease entry \"$l_lease_path\"."
		return 1
	done

	return 0
}

# Purpose: Count the SSH control socket leases for the surrounding remote-host
# flow.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a threshold or size decision.
zxfer_count_ssh_control_socket_leases() {
	l_entry_dir=$1
	l_leases_dir="$l_entry_dir/leases"
	l_count=0
	g_zxfer_ssh_control_socket_lease_count_result=""

	[ -d "$l_leases_dir" ] || {
		g_zxfer_ssh_control_socket_lease_count_result=0
		printf '%s\n' "0"
		return 0
	}

	set -- "$l_leases_dir"/lease.*
	if [ ! -e "$1" ] && [ ! -L "$1" ] && [ ! -h "$1" ]; then
		g_zxfer_ssh_control_socket_lease_count_result=0
		printf '%s\n' "0"
		return 0
	fi

	for l_lease_path in "$@"; do
		if [ ! -e "$l_lease_path" ] && [ ! -L "$l_lease_path" ] &&
			[ ! -h "$l_lease_path" ]; then
			continue
		fi
		if [ -L "$l_lease_path" ] || [ -h "$l_lease_path" ]; then
			zxfer_note_ssh_control_socket_lock_error \
				"Refusing symlinked ssh control socket lease entry \"$l_lease_path\"."
			return 1
		fi
		if [ ! -d "$l_lease_path" ]; then
			zxfer_note_ssh_control_socket_lock_error \
				"ssh control socket lease entry \"$l_lease_path\" is not a metadata-bearing directory. Remove the stale entry and retry."
			return 1
		fi
		zxfer_try_reap_stale_owned_lock_dir \
			"$l_lease_path" 0 lease "$(zxfer_get_ssh_control_socket_lease_purpose)" >/dev/null 2>&1
		l_reap_status=$?
		if [ "$l_reap_status" -eq 0 ]; then
			continue
		fi
		if [ "$l_reap_status" -eq 2 ]; then
			l_count=$((l_count + 1))
			continue
		fi
		zxfer_note_ssh_control_socket_lock_error \
			"Unable to inspect ssh control socket lease entry \"$l_lease_path\"."
		return 1
	done
	g_zxfer_ssh_control_socket_lease_count_result=$l_count
	printf '%s\n' "$l_count"
}

# Purpose: Create the SSH control socket lease entry using the safety checks
# owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs a fresh staged resource or persistent
# helper state.
zxfer_create_ssh_control_socket_lease_file() {
	l_entry_dir=$1
	l_leases_dir="$l_entry_dir/leases"
	l_timestamp=$(date +%s)

	g_zxfer_runtime_artifact_path_result=""
	if ! l_lease_dir=$(zxfer_create_owned_lock_dir_in_parent \
		"$l_leases_dir" "lease.$$.${l_timestamp}" \
		lease "$(zxfer_get_ssh_control_socket_lease_purpose)"); then
		return 1
	fi
	zxfer_register_owned_lock_path "$l_lease_dir"
	g_zxfer_runtime_artifact_path_result=$l_lease_dir
	printf '%s\n' "$l_lease_dir"
}

# Purpose: Release the SSH control socket lease entry after the protected work
# finishes.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer no longer needs to hold one process lease on a
# shared control-socket entry.
zxfer_release_ssh_control_socket_lease_file() {
	l_lease_dir=$1

	zxfer_release_owned_lock_dir \
		"$l_lease_dir" lease "$(zxfer_get_ssh_control_socket_lease_purpose)"
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

# Purpose: Clean up the SSH control socket entry directory that this module
# created or tracks.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management on success and failure paths so temporary state does not
# linger.
zxfer_cleanup_ssh_control_socket_entry_dir() {
	l_entry_dir=$1

	[ -n "$l_entry_dir" ] || return 0
	[ -d "$l_entry_dir" ] || return 0
	rm -rf "$l_entry_dir" 2>/dev/null
}

# Purpose: Emit very-verbose diagnostic output for `-V` runs.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer wants low-level debug output that should stay
# hidden in normal verbose mode.
zxfer_echoV_ssh_control_socket_command_for_host() {
	l_host=$1
	l_action_label=$2
	shift 2

	l_command_context=$(zxfer_get_remote_command_context_label "$l_host")
	l_rendered_command=$(zxfer_render_command_for_report "" "$@")
	zxfer_echoV "$l_action_label [$l_command_context]: $l_rendered_command"
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

	if l_transport_tokens=$(zxfer_get_ssh_base_transport_tokens); then
		:
	else
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi
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
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after a probe or planning step changes the active context
# that later helpers should use.
zxfer_set_ssh_control_socket_role_state() {
	l_role=$1
	l_socket_path=$2
	l_entry_dir=$3
	l_lease_file=$4

	case "$l_role" in
	origin)
		g_ssh_origin_control_socket="$l_socket_path"
		g_ssh_origin_control_socket_dir="$l_entry_dir"
		g_ssh_origin_control_socket_lease_file="$l_lease_file"
		;;
	target)
		g_ssh_target_control_socket="$l_socket_path"
		g_ssh_target_control_socket_dir="$l_entry_dir"
		g_ssh_target_control_socket_lease_file="$l_lease_file"
		;;
	esac
}

# Purpose: Clear the SSH control socket role state from the module-owned state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers must not see an old cached or role-
# specific value.
zxfer_clear_ssh_control_socket_role_state() {
	l_role=$1

	case "$l_role" in
	origin)
		g_ssh_origin_control_socket=""
		g_ssh_origin_control_socket_dir=""
		g_ssh_origin_control_socket_lease_file=""
		;;
	target)
		g_ssh_target_control_socket=""
		g_ssh_target_control_socket_dir=""
		g_ssh_target_control_socket_lease_file=""
		;;
	esac
}

# Purpose: Reset the remote capability parse state so the next remote-host pass
# starts from a clean state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_remote_capability_parse_state() {
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_zfs_path=""
	g_zxfer_remote_capability_parallel_status=""
	g_zxfer_remote_capability_parallel_path=""
	g_zxfer_remote_capability_cat_status=""
	g_zxfer_remote_capability_cat_path=""
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

# Purpose: Check whether the remote capability host matches origin role.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a boolean answer about the remote
# capability host.
zxfer_remote_capability_host_matches_origin_role() {
	l_host_spec=$1

	[ -n "${g_option_O_origin_host:-}" ] || return 1
	[ -n "$l_host_spec" ] || return 0
	[ "$l_host_spec" = "$g_option_O_origin_host" ]
}

# Purpose: Check whether the remote capability host matches target role.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a boolean answer about the remote
# capability host.
zxfer_remote_capability_host_matches_target_role() {
	l_host_spec=$1

	[ -n "${g_option_T_target_host:-}" ] || return 1
	[ -n "$l_host_spec" ] || return 0
	[ "$l_host_spec" = "$g_option_T_target_host" ]
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
	[ "${g_option_U_skip_unsupported_properties:-0}" -eq 0 ] || return 1
	[ "${g_option_e_restore_property_mode:-0}" -eq 0 ] || return 1
	[ "${g_option_k_backup_property_mode:-0}" -eq 0 ] || return 1
	[ -z "${g_option_g_grandfather_protection:-}" ] || return 1

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

	if zxfer_remote_capability_host_matches_origin_role "$l_host_spec"; then
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

	if zxfer_remote_capability_host_matches_target_role "$l_host_spec"; then
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
# REMOTE CAPABILITY CACHE / HANDSHAKE PARSING
################################################################################

ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND="remote-capability"

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

# Purpose: Render the remote capability cache identity as a stable shell-safe
# or operator-facing string.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to display or transport the value without
# reparsing it.
zxfer_render_remote_capability_cache_identity() {
	zxfer_render_remote_capability_cache_identity_for_host "${1:-}" "${2:-}"
}

# Purpose: Render the full remote capability cache artifact identity for host.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management so path keys and cache-object metadata use the same
# host-scoped identity.
zxfer_render_remote_capability_cache_artifact_identity_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		[ "$l_cache_identity" = "" ] || printf '%s\n' "$l_cache_identity"
		return 1
	fi

	printf '%s\n%s\n' "$l_host_spec" "$l_cache_identity"
}

# Purpose: Return the remote capability cache artifact identity as hex.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when cache paths or metadata need a newline-safe exact
# identity for later verification.
zxfer_remote_capability_cache_identity_hex_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if ! l_identity=$(zxfer_render_remote_capability_cache_artifact_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		[ "$l_identity" = "" ] || printf '%s\n' "$l_identity"
		return 1
	fi

	l_identity_hex=$(printf '%s' "$l_identity" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	[ -n "$l_identity_hex" ] || return 1

	printf '%s\n' "$l_identity_hex"
}

# Purpose: Return a bounded remote capability cache key for the remote-host
# management flow.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when remote bootstrap or capability logic needs one shared
# helper for this state.
zxfer_remote_capability_cache_key() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if ! l_identity_hex=$(zxfer_remote_capability_cache_identity_hex_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		[ "$l_identity_hex" = "" ] || printf '%s\n' "$l_identity_hex"
		return 1
	fi

	l_identity_hex_len=${#l_identity_hex}
	l_identity_byte_len=$((l_identity_hex_len / 2))
	if [ "$l_identity_hex_len" -le 180 ]; then
		printf 'h%s.%s\n' "$l_identity_byte_len" "$l_identity_hex"
		return 0
	fi

	l_key_head=$(printf '%s' "$l_identity_hex" | cut -c 1-64)
	l_key_tail=$l_identity_hex
	while [ "${#l_key_tail}" -gt 64 ]; do
		l_key_tail=${l_key_tail#?}
	done
	[ -n "$l_key_head" ] || return 1
	[ -n "$l_key_tail" ] || return 1

	printf 'h%s.%s.%s\n' "$l_identity_byte_len" "$l_key_head" "$l_key_tail"
}

# Purpose: Ensure the remote capability cache directory exists and is ready
# before the flow continues.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before later helpers assume the resource or cache is
# available.
zxfer_ensure_remote_capability_cache_dir() {
	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		return 1
	fi
	if ! l_cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$l_tmpdir"); then
		return 1
	fi
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi

	if [ -L "$l_cache_dir" ] || [ -h "$l_cache_dir" ]; then
		return 1
	fi

	if [ -e "$l_cache_dir" ]; then
		[ -d "$l_cache_dir" ] || return 1
		if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_cache_dir"); then
			return 1
		fi
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
		if ! l_mode=$(zxfer_get_path_mode_octal "$l_cache_dir"); then
			return 1
		fi
		[ "$l_mode" = "700" ] || return 1
		printf '%s\n' "$l_cache_dir"
		return 0
	fi

	l_old_umask=$(umask)
	umask 077
	if ! mkdir "$l_cache_dir" 2>/dev/null; then
		umask "$l_old_umask"
		return 1
	fi
	umask "$l_old_umask"

	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_cache_dir"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_cache_dir"); then
		return 1
	fi
	[ "$l_mode" = "700" ] || return 1
	printf '%s\n' "$l_cache_dir"
}

# Purpose: Return the remote-capability cache directory path for one temporary
# root.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before zxfer creates or validates the per-user remote-
# capability cache directory.
zxfer_remote_capability_cache_dir_path_for_tmpdir() {
	l_tmpdir=$1

	[ -n "$l_tmpdir" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi

	printf '%s/zxfer.remote-capabilities.%s.d\n' "$l_tmpdir" "$l_effective_uid"
}

# Purpose: Manage remote capability cache path for the remote-host management
# flow.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when remote bootstrap or capability logic needs one shared
# helper for this state.
zxfer_remote_capability_cache_path() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	if ! l_cache_dir=$(zxfer_ensure_remote_capability_cache_dir); then
		return 1
	fi
	if ! l_cache_key=$(zxfer_remote_capability_cache_key "$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	printf '%s\n' "$l_cache_dir/$l_cache_key"
}

# Purpose: Manage remote capability cache lock path for the remote-host
# management flow.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when remote bootstrap or capability logic needs one shared
# helper for this state.
zxfer_remote_capability_cache_lock_path() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	if ! l_cache_path=$(zxfer_remote_capability_cache_path \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	printf '%s.lock\n' "$l_cache_path"
}

# Purpose: Validate the remote capability cache lock directory before zxfer
# relies on it.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management to fail closed on malformed, unsafe, or stale input.
zxfer_validate_remote_capability_cache_lock_dir() {
	l_lock_dir=$1

	zxfer_load_owned_lock_metadata_for_kind_and_purpose \
		"$l_lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" >/dev/null 2>&1
}

# Purpose: Create the remote capability cache lock directory using the safety
# checks owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs a fresh staged resource or persistent
# helper state.
zxfer_create_remote_capability_cache_lock_dir() {
	l_lock_dir=$1

	if ! zxfer_create_owned_lock_dir \
		"$l_lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" >/dev/null; then
		return 1
	fi
	return 0
}

# Purpose: Surface an unsupported remote capability cache lock layout before
# the caller reuses or destroys it.
# Usage: Called during remote bootstrap and capability caching when current
# releases encounter a pre-metadata pid-file lock directory that operators must
# remove explicitly.
zxfer_warn_unsupported_remote_capability_cache_lock_path() {
	l_lock_dir=$1

	zxfer_warn_stderr \
		"Error: remote capability cache lock path \"$l_lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
}

# Purpose: Try to resolve or create the acquire remote capability cache lock
# without treating every miss as fatal.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer has an optional or fallback path that still
# needs one checked helper.
zxfer_try_acquire_remote_capability_cache_lock() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if ! l_lock_dir=$(zxfer_remote_capability_cache_lock_path \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	[ ! -L "$l_lock_dir" ] || return 1
	[ ! -h "$l_lock_dir" ] || return 1

	if zxfer_create_remote_capability_cache_lock_dir "$l_lock_dir"; then
		printf '%s\n' "$l_lock_dir"
		return 0
	fi

	[ -d "$l_lock_dir" ] || return 1
	if zxfer_owned_lock_dir_uses_unsupported_pid_file_layout "$l_lock_dir"; then
		zxfer_warn_unsupported_remote_capability_cache_lock_path "$l_lock_dir"
		return 1
	fi
	l_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_lock_dir")
	if [ ! -e "$l_metadata_path" ] && [ ! -L "$l_metadata_path" ] &&
		[ ! -h "$l_metadata_path" ]; then
		zxfer_try_reap_stale_owned_lock_dir \
			"$l_lock_dir" 0 lock "$(zxfer_get_remote_capability_cache_lock_purpose)"
		l_reap_status=$?
	else
		zxfer_try_reap_stale_owned_lock_dir \
			"$l_lock_dir" 1 lock "$(zxfer_get_remote_capability_cache_lock_purpose)"
		l_reap_status=$?
	fi
	if [ "$l_reap_status" -eq 0 ]; then
		if zxfer_create_remote_capability_cache_lock_dir "$l_lock_dir"; then
			printf '%s\n' "$l_lock_dir"
			return 0
		fi
		[ -d "$l_lock_dir" ] || return 1
		if ! zxfer_validate_remote_capability_cache_lock_dir "$l_lock_dir"; then
			return 1
		fi
		return 2
	fi
	case "$l_reap_status" in
	2)
		return 2
		;;
	esac
	return 1
}

# Purpose: Release the remote capability cache lock after the protected work
# finishes.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when a shared cache, lock, or transport resource should no
# longer be held.
zxfer_release_remote_capability_cache_lock() {
	l_lock_dir=$1

	zxfer_release_owned_lock_dir \
		"$l_lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"
}

# Purpose: Match only the current-format remote-host cache root paths that this
# module owns so generic temp cleanup does not treat arbitrary scratch
# directories as remote-host cache state.
# Usage: Called during trap cleanup and explicit cache-root teardown before
# unsupported old-format entries are allowed to block directory removal.
zxfer_is_remote_host_cache_root_path() {
	l_cache_dir=$1

	[ -n "$l_cache_dir" ] || return 1
	l_cache_basename=${l_cache_dir##*/}

	if [ -n "${g_zxfer_temp_prefix:-}" ]; then
		l_root_prefix=$g_zxfer_temp_prefix
		case "$l_cache_basename" in
		"${l_root_prefix}.s."[0-9]*.d | \
			"${l_root_prefix}.remote-capabilities."[0-9]*.d)
			return 0
			;;
		esac
	fi

	case "$l_cache_basename" in
	zxfer.*.s.[0-9]*.d | \
		zxfer.*.remote-capabilities.[0-9]*.d | \
		zxfer.remote-capabilities.[0-9]*.d)
		return 0
		;;
	esac
	return 1
}

# Purpose: Match the stable per-user remote-capability cache root.
# Usage: Called during trap cleanup so short-lived run-owned SSH state is
# removed while useful helper-discovery cache entries survive concurrent and
# near-future zxfer invocations.
zxfer_is_stable_remote_capability_cache_root_path() {
	l_cache_dir=$1

	[ -n "$l_cache_dir" ] || return 1
	l_cache_basename=${l_cache_dir##*/}
	case "$l_cache_basename" in
	zxfer.remote-capabilities.[0-9]*.d)
		return 0
		;;
	esac
	return 1
}

# Purpose: Clean up the empty remote host cache root that this module created
# or tracks.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management on success and failure paths so temporary state does not
# linger.
zxfer_cleanup_empty_remote_host_cache_root() {
	l_cache_dir=$1

	[ -n "$l_cache_dir" ] || return 0
	[ ! -L "$l_cache_dir" ] || return 1
	[ ! -h "$l_cache_dir" ] || return 1
	[ -e "$l_cache_dir" ] || return 0
	[ -d "$l_cache_dir" ] || return 1

	set -- "$l_cache_dir"/.[!.]* "$l_cache_dir"/..?* "$l_cache_dir"/*
	for l_cache_entry in "$@"; do
		if [ -e "$l_cache_entry" ] || [ -L "$l_cache_entry" ] || [ -h "$l_cache_entry" ]; then
			return 0
		fi
	done

	if rmdir "$l_cache_dir" >/dev/null 2>&1 ||
		{ [ ! -e "$l_cache_dir" ] && [ ! -L "$l_cache_dir" ] && [ ! -h "$l_cache_dir" ]; }; then
		return 0
	fi
	return 1
}

# Purpose: Detect remote-host cache roots that still contain unsupported
# old-format lock or lease entries so cleanup does not silently delete them.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before trap or explicit cache-root cleanup removes a
# current-run temp-prefix root.
zxfer_remote_host_cache_root_contains_unsupported_entries() {
	l_cache_dir=$1

	[ -n "$l_cache_dir" ] || return 1
	[ -d "$l_cache_dir" ] || return 1
	[ ! -L "$l_cache_dir" ] || return 1
	[ ! -h "$l_cache_dir" ] || return 1

	set -- "$l_cache_dir"/*.lock
	if [ -e "$1" ] || [ -L "$1" ] || [ -h "$1" ]; then
		for l_lock_path in "$@"; do
			if [ ! -e "$l_lock_path" ] && [ ! -L "$l_lock_path" ] &&
				[ ! -h "$l_lock_path" ]; then
				continue
			fi
			if [ -d "$l_lock_path" ] &&
				zxfer_owned_lock_dir_uses_unsupported_pid_file_layout "$l_lock_path"; then
				return 0
			fi
		done
	fi

	set -- "$l_cache_dir"/*/leases/lease.*
	if [ ! -e "$1" ] && [ ! -L "$1" ] && [ ! -h "$1" ]; then
		return 1
	fi

	for l_lease_path in "$@"; do
		if [ ! -e "$l_lease_path" ] && [ ! -L "$l_lease_path" ] &&
			[ ! -h "$l_lease_path" ]; then
			continue
		fi
		if [ -L "$l_lease_path" ] || [ -h "$l_lease_path" ] ||
			[ ! -d "$l_lease_path" ]; then
			return 0
		fi
	done

	return 1
}

# Purpose: Detect remote-host cache cleanup conflicts before generic temp-root
# cleanup or cache-root teardown removes a path.
# Usage: Called during trap cleanup and explicit remote-host cache cleanup when
# owned lock state or unsupported old-format entries must be preserved for
# checked release or operator cleanup.
zxfer_remote_host_cache_cleanup_conflicts_with_path() {
	l_cache_dir=$1

	if command -v zxfer_owned_lock_cleanup_conflicts_with_path >/dev/null 2>&1 &&
		zxfer_owned_lock_cleanup_conflicts_with_path "$l_cache_dir"; then
		return 0
	fi
	if ! zxfer_is_remote_host_cache_root_path "$l_cache_dir"; then
		return 1
	fi
	zxfer_remote_host_cache_root_contains_unsupported_entries "$l_cache_dir"
}

# Purpose: Clean up the remote host cache root that this module created or
# tracks.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management on success and failure paths so temporary state does not
# linger.
zxfer_cleanup_remote_host_cache_root() {
	l_cache_dir=$1

	[ -n "$l_cache_dir" ] || return 0
	if zxfer_is_stable_remote_capability_cache_root_path "$l_cache_dir"; then
		zxfer_cleanup_empty_remote_host_cache_root "$l_cache_dir" >/dev/null 2>&1 || :
		return 0
	fi
	if zxfer_remote_host_cache_cleanup_conflicts_with_path "$l_cache_dir"; then
		zxfer_cleanup_empty_remote_host_cache_root "$l_cache_dir" >/dev/null 2>&1 || :
		return 0
	fi
	if [ -L "$l_cache_dir" ] || [ -h "$l_cache_dir" ]; then
		rm -f "$l_cache_dir" 2>/dev/null || :
		return 0
	fi
	rm -rf "$l_cache_dir" 2>/dev/null || :
}

# Purpose: Clean up the remote host cache roots that this module created or
# tracks.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management on success and failure paths so temporary state does not
# linger.
zxfer_cleanup_remote_host_cache_roots() {
	if l_socket_tmpdir=$(zxfer_try_get_socket_cache_tmpdir 2>/dev/null); then
		if l_socket_cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$l_socket_tmpdir" 2>/dev/null); then
			zxfer_cleanup_remote_host_cache_root "$l_socket_cache_dir"
		fi
	fi
	if l_default_tmpdir=$(zxfer_try_get_default_tmpdir 2>/dev/null); then
		if [ "${l_socket_tmpdir:-}" != "$l_default_tmpdir" ] &&
			l_socket_cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$l_default_tmpdir" 2>/dev/null); then
			zxfer_cleanup_remote_host_cache_root "$l_socket_cache_dir"
		fi
	fi
	if l_capability_tmpdir=$(zxfer_try_get_effective_tmpdir 2>/dev/null); then
		if l_capability_cache_dir=$(
			zxfer_remote_capability_cache_dir_path_for_tmpdir "$l_capability_tmpdir" 2>/dev/null
		); then
			zxfer_cleanup_remote_host_cache_root "$l_capability_cache_dir"
		fi
	fi
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

	l_ssh_setup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
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

# Purpose: Wait for the for remote capability cache fill to reach the state
# this module expects.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later steps must block until background work or shared
# state catches up.
zxfer_wait_for_remote_capability_cache_fill() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	l_wait_retries=${g_zxfer_remote_capability_cache_wait_retries:-5}
	l_fast_retries=${ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES:-20}
	l_wait_count=0
	l_waited=0
	l_wait_start_ms=""

	case "$l_wait_retries" in
	'' | *[!0-9]*)
		l_wait_retries=5
		;;
	esac
	[ "$l_wait_retries" -gt 0 ] || l_wait_retries=5
	case "$l_fast_retries" in
	'' | *[!0-9]*)
		l_fast_retries=0
		;;
	esac

	while [ "$l_fast_retries" -gt 0 ]; do
		if l_cached_response=$(zxfer_read_remote_capability_cache_file \
			"$l_host_spec" "$l_requested_tools"); then
			zxfer_record_remote_capability_cache_wait_metrics "$l_waited" "$l_wait_start_ms"
			printf '%s\n' "$l_cached_response"
			return 0
		fi
		if [ "$l_waited" -eq 0 ]; then
			l_waited=1
			l_wait_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
		fi
		l_fast_retries=$((l_fast_retries - 1))
	done

	while [ "$l_wait_count" -lt "$l_wait_retries" ]; do
		if l_cached_response=$(zxfer_read_remote_capability_cache_file \
			"$l_host_spec" "$l_requested_tools"); then
			zxfer_record_remote_capability_cache_wait_metrics "$l_waited" "$l_wait_start_ms"
			printf '%s\n' "$l_cached_response"
			return 0
		fi
		if [ "$l_waited" -eq 0 ]; then
			l_waited=1
			l_wait_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
		fi
		l_wait_count=$((l_wait_count + 1))
		[ "$l_wait_count" -lt "$l_wait_retries" ] || break
		sleep 1
	done

	zxfer_record_remote_capability_cache_wait_metrics "$l_waited" "$l_wait_start_ms"
	return 1
}

# Purpose: Reap stale the stale pidless remote capability cache lock left
# behind by earlier runs or dead owners.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer can prove an old lock, cache entry, or transport
# artifact is no longer live.
zxfer_reap_stale_pidless_remote_capability_cache_lock() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if ! l_lock_dir=$(zxfer_remote_capability_cache_lock_path \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	[ -e "$l_lock_dir" ] || return 0
	if zxfer_owned_lock_dir_uses_unsupported_pid_file_layout "$l_lock_dir"; then
		zxfer_warn_unsupported_remote_capability_cache_lock_path "$l_lock_dir"
		return 1
	fi
	zxfer_try_reap_stale_owned_lock_dir \
		"$l_lock_dir" 1 lock "$(zxfer_get_remote_capability_cache_lock_purpose)"
	l_reap_status=$?
	case "$l_reap_status" in
	0 | 2)
		return 0
		;;
	esac
	return 1
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
				if [ "$l_record_status" -eq 0 ]; then
					g_zxfer_remote_capability_zfs_path=$l_record_path
				else
					g_zxfer_remote_capability_zfs_path=""
				fi
				;;
			parallel)
				g_zxfer_remote_capability_parallel_status=$l_record_status
				if [ "$l_record_status" -eq 0 ]; then
					g_zxfer_remote_capability_parallel_path=$l_record_path
				else
					g_zxfer_remote_capability_parallel_path=""
				fi
				;;
			cat)
				g_zxfer_remote_capability_cat_status=$l_record_status
				if [ "$l_record_status" -eq 0 ]; then
					g_zxfer_remote_capability_cat_path=$l_record_path
				else
					g_zxfer_remote_capability_cat_path=""
				fi
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
	l_command_context=$(zxfer_get_remote_command_context_label \
		"$l_host_spec" "$l_profile_side")
	zxfer_echoV "Running remote probe [$l_command_context]: $l_remote_probe_cmd"

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
	l_dependency_path=$(zxfer_get_effective_dependency_path)
	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		l_cache_identity=""
	fi

	if [ "$l_host_spec" = "${g_option_O_origin_host:-}" ] ||
		[ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ]; then
		if [ "${g_origin_remote_capabilities_cache_identity:-}" != "$l_cache_identity" ] ||
			[ "${g_origin_remote_capabilities_host:-}" != "$l_host_spec" ]; then
			g_origin_remote_capabilities_bootstrap_source=""
			g_origin_remote_capabilities_cache_write_unavailable=0
		fi
		g_origin_remote_capabilities_host=$l_host_spec
		g_origin_remote_capabilities_dependency_path=$l_dependency_path
		g_origin_remote_capabilities_cache_identity=$l_cache_identity
		g_origin_remote_capabilities_response=$l_response
		l_stored=1
	fi

	if [ "$l_host_spec" = "${g_option_T_target_host:-}" ] ||
		[ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ]; then
		if [ "${g_target_remote_capabilities_cache_identity:-}" != "$l_cache_identity" ] ||
			[ "${g_target_remote_capabilities_host:-}" != "$l_host_spec" ]; then
			g_target_remote_capabilities_bootstrap_source=""
			g_target_remote_capabilities_cache_write_unavailable=0
		fi
		g_target_remote_capabilities_host=$l_host_spec
		g_target_remote_capabilities_dependency_path=$l_dependency_path
		g_target_remote_capabilities_cache_identity=$l_cache_identity
		g_target_remote_capabilities_response=$l_response
		l_stored=1
	fi

	if [ "$l_stored" -eq 0 ] &&
		[ "${g_origin_remote_capabilities_host:-}" = "" ]; then
		g_origin_remote_capabilities_host=$l_host_spec
		g_origin_remote_capabilities_dependency_path=$l_dependency_path
		g_origin_remote_capabilities_cache_identity=$l_cache_identity
		g_origin_remote_capabilities_response=$l_response
		g_origin_remote_capabilities_bootstrap_source=""
		g_origin_remote_capabilities_cache_write_unavailable=0
		return
	fi

	if [ "$l_stored" -eq 0 ]; then
		g_target_remote_capabilities_host=$l_host_spec
		g_target_remote_capabilities_dependency_path=$l_dependency_path
		g_target_remote_capabilities_cache_identity=$l_cache_identity
		g_target_remote_capabilities_response=$l_response
		g_target_remote_capabilities_bootstrap_source=""
		g_target_remote_capabilities_cache_write_unavailable=0
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

# Purpose: Read the remote capability cache file from staged state into the
# current shell.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when later helpers need a checked reload instead of ad hoc
# file reads.
zxfer_read_remote_capability_cache_file() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	l_now=$(date '+%s' 2>/dev/null || :)
	[ -n "$l_now" ] || return 1

	if ! l_cache_path=$(zxfer_remote_capability_cache_path \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	if ! l_expected_identity_hex=$(zxfer_remote_capability_cache_identity_hex_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	[ -f "$l_cache_path" ] || return 1
	[ ! -L "$l_cache_path" ] || return 1
	[ ! -h "$l_cache_path" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_cache_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_cache_path"); then
		return 1
	fi
	[ "$l_mode" = "600" ] || return 1

	if ! zxfer_read_cache_object_file \
		"$l_cache_path" "$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" >/dev/null; then
		return 1
	fi
	l_cached_response=$g_zxfer_cache_object_payload_result
	if ! l_cache_epoch=$(zxfer_get_cache_object_metadata_value \
		"$g_zxfer_cache_object_metadata_result" created_epoch); then
		return 1
	fi
	if ! l_cache_identity_hex=$(zxfer_get_cache_object_metadata_value \
		"$g_zxfer_cache_object_metadata_result" identity_hex); then
		return 1
	fi
	[ "$l_cache_identity_hex" = "$l_expected_identity_hex" ] || return 1

	case "$l_cache_epoch" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	l_cache_age=$((l_now - l_cache_epoch))
	[ "$l_cache_age" -ge 0 ] || return 1
	[ "$l_cache_age" -le "${g_zxfer_remote_capability_cache_ttl:-15}" ] || return 1

	if ! zxfer_parse_remote_capability_response "$l_cached_response"; then
		return 1
	fi

	printf '%s\n' "$l_cached_response"
}

# Purpose: Write the remote capability cache file in the normalized form later
# zxfer steps expect.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when the module needs a stable staged file or emitted
# stream for downstream use.
zxfer_write_remote_capability_cache_file() {
	l_host_spec=$1
	l_response=$2
	l_requested_tools=${3:-}
	l_now=$(date '+%s' 2>/dev/null || :)
	[ -n "$l_now" ] || return 1

	if ! l_cache_path=$(zxfer_remote_capability_cache_path \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	if ! l_identity_hex=$(zxfer_remote_capability_cache_identity_hex_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	[ ! -L "$l_cache_path" ] || return 1
	[ ! -h "$l_cache_path" ] || return 1
	if [ -e "$l_cache_path" ]; then
		[ -f "$l_cache_path" ] || return 1
		if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
			return 1
		fi
		if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_cache_path"); then
			return 1
		fi
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	fi

	zxfer_write_cache_object_file_atomically \
		"$l_cache_path" \
		"$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" \
		"created_epoch=$l_now
identity_hex=$l_identity_hex" \
		"$l_response"
}

# Purpose: Emit the remote capability cache write failure in the operator-
# facing format owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_warn_remote_capability_cache_write_failure() {
	l_host_spec=$1
	l_status=$2

	printf '%s\n' "Warning: Failed to write local remote capability cache for host $l_host_spec (status $l_status); disabling further local cache writes for this host during this run." >&2
}

# Purpose: Check whether local remote capability cache writes are unavailable
# for host in this run.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before zxfer retries a local cache write that already
# failed once for the same host/cache identity.
zxfer_remote_capability_cache_write_is_unavailable_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi

	if [ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
		[ "$l_cache_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ] &&
		[ "${g_origin_remote_capabilities_cache_write_unavailable:-0}" -eq 1 ]; then
		return 0
	fi

	if [ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
		[ "$l_cache_identity" = "${g_target_remote_capabilities_cache_identity:-}" ] &&
		[ "${g_target_remote_capabilities_cache_write_unavailable:-0}" -eq 1 ]; then
		return 0
	fi

	return 1
}

# Purpose: Mark local remote capability cache writes unavailable for host in
# this run after a checked persistence failure.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs an explicit degraded cache state instead
# of repeated best-effort writes.
zxfer_note_remote_capability_cache_write_unavailable_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if ! l_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_host_spec" "$l_requested_tools"); then
		return 0
	fi

	if [ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
		[ "$l_cache_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ]; then
		g_origin_remote_capabilities_cache_write_unavailable=1
	fi

	if [ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
		[ "$l_cache_identity" = "${g_target_remote_capabilities_cache_identity:-}" ]; then
		g_target_remote_capabilities_cache_write_unavailable=1
	fi
}

# Purpose: Emit the remote capability cache lock release failure in the
# operator-facing format owned by this module.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_warn_remote_capability_cache_lock_release_failure() {
	l_host_spec=$1
	l_status=$2

	printf '%s\n' "Warning: Failed to release local remote capability cache lock for host $l_host_spec (status $l_status)." >&2
}

# Purpose: Release the remote capability cache lock while preserving the
# caller's primary failure when one already exists.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when lock release should be checked but must not suppress
# an earlier live-probe or cache-path failure.
zxfer_release_remote_capability_cache_lock_with_precedence() {
	l_host_spec=$1
	l_lock_dir=$2
	l_primary_status=${3:-0}

	[ -n "$l_lock_dir" ] || return "$l_primary_status"

	zxfer_release_remote_capability_cache_lock "$l_lock_dir" >/dev/null 2>&1
	l_release_status=$?
	if [ "$l_release_status" -eq 0 ]; then
		return "$l_primary_status"
	fi
	zxfer_warn_remote_capability_cache_lock_release_failure \
		"$l_host_spec" "$l_release_status"
	if [ "$l_primary_status" -ne 0 ]; then
		return "$l_primary_status"
	fi
	return 1
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

# Purpose: Ensure the remote host capabilities exists and is ready before the
# flow continues.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before later helpers assume the resource or cache is
# available.
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

	if l_cached_response=$(zxfer_read_remote_capability_cache_file \
		"$l_host_spec" "$l_requested_tools"); then
		zxfer_store_cached_remote_capability_response_for_host \
			"$l_host_spec" "$l_cached_response" "$l_requested_tools"
		zxfer_note_remote_capability_bootstrap_source_for_host \
			"$l_host_spec" cache "$l_requested_tools"
		zxfer_profile_record_remote_capability_bootstrap_source cache
		g_zxfer_remote_capability_response_result=$l_cached_response
		printf '%s\n' "$l_cached_response"
		return 0
	fi

	l_capability_lock_dir=""
	l_capability_lock_dir=$(zxfer_try_acquire_remote_capability_cache_lock \
		"$l_host_spec" "$l_requested_tools")
	l_lock_status=$?
	if [ "$l_lock_status" -ne 0 ]; then
		case "$l_lock_status" in
		1)
			return 1
			;;
		2)
			if l_cached_response=$(zxfer_wait_for_remote_capability_cache_fill \
				"$l_host_spec" "$l_requested_tools"); then
				zxfer_store_cached_remote_capability_response_for_host \
					"$l_host_spec" "$l_cached_response" "$l_requested_tools"
				zxfer_note_remote_capability_bootstrap_source_for_host \
					"$l_host_spec" cache "$l_requested_tools"
				zxfer_profile_record_remote_capability_bootstrap_source cache
				g_zxfer_remote_capability_response_result=$l_cached_response
				printf '%s\n' "$l_cached_response"
				return 0
			fi
			if ! zxfer_reap_stale_pidless_remote_capability_cache_lock \
				"$l_host_spec" "$l_requested_tools"; then
				return 1
			fi
			l_capability_lock_dir=$(zxfer_try_acquire_remote_capability_cache_lock \
				"$l_host_spec" "$l_requested_tools")
			l_lock_status=$?
			if [ "$l_lock_status" -ne 0 ]; then
				case "$l_lock_status" in
				1)
					return 1
					;;
				2)
					l_capability_lock_dir=""
					;;
				*)
					return 1
					;;
				esac
			fi
			;;
		*)
			return 1
			;;
		esac
	fi

	if zxfer_fetch_remote_host_capabilities_live \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null; then
		:
	else
		l_live_status=$?
		if [ -n "$l_capability_lock_dir" ]; then
			zxfer_release_remote_capability_cache_lock_with_precedence \
				"$l_host_spec" "$l_capability_lock_dir" "$l_live_status"
			l_live_status=$?
		fi
		return "$l_live_status"
	fi
	l_live_response=$g_zxfer_remote_capability_response_result

	zxfer_store_cached_remote_capability_response_for_host \
		"$l_host_spec" "$l_live_response" "$l_requested_tools"
	zxfer_note_remote_capability_bootstrap_source_for_host \
		"$l_host_spec" live "$l_requested_tools"
	zxfer_profile_record_remote_capability_bootstrap_source live
	if ! zxfer_remote_capability_cache_write_is_unavailable_for_host \
		"$l_host_spec" "$l_requested_tools"; then
		if zxfer_write_remote_capability_cache_file \
			"$l_host_spec" "$l_live_response" "$l_requested_tools" >/dev/null 2>&1; then
			:
		else
			l_cache_write_status=$?
			zxfer_note_remote_capability_cache_write_unavailable_for_host \
				"$l_host_spec" "$l_requested_tools"
			zxfer_warn_remote_capability_cache_write_failure \
				"$l_host_spec" "$l_cache_write_status"
		fi
	fi
	if [ -n "$l_capability_lock_dir" ]; then
		zxfer_release_remote_capability_cache_lock_with_precedence \
			"$l_host_spec" "$l_capability_lock_dir" 0
		l_release_status=$?
		[ "$l_release_status" -eq 0 ] || return "$l_release_status"
	fi
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

	if ! zxfer_get_parsed_remote_capability_tool_record "$l_tool"; then
		return 2
	fi

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

# Purpose: Return the remote resolved tool version output in the form expected
# by later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
#
# Probe the full `--version` output for a resolved remote helper path using only
# shell builtins, so version checks do not depend on the remote PATH.
zxfer_get_remote_resolved_tool_version_output() {
	l_host=$1
	l_tool_path=$2
	l_label=${3:-tool}
	l_profile_side=${4:-}

	[ -n "$l_host" ] || return 1
	[ -n "$l_tool_path" ] || return 1

	l_tool_path_single=$(zxfer_escape_for_single_quotes "$l_tool_path")
	l_remote_probe="l_tool='$l_tool_path_single'
if ! l_output=\$(\"\$l_tool\" --version 2>&1); then
	exit 11
fi
printf '%s\n' \"\$l_output\""
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if ! zxfer_capture_remote_probe_output "$l_host" "$l_remote_probe_cmd" "$l_profile_side"; then
		zxfer_emit_remote_probe_failure_message \
			"Failed to query dependency \"$l_label\" on host $l_host."
		return 1
	fi

	printf '%s\n' "$g_zxfer_remote_probe_stdout"
}

# Purpose: Return the remote resolved tool version line in the form expected by
# later helpers.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when sibling helpers need the same lookup without
# duplicating module logic.
#
# Probe the first `--version` line for a resolved remote helper path using only
# shell builtins, so signature checks do not depend on the remote PATH.
zxfer_get_remote_resolved_tool_version_line() {
	l_host=$1
	l_tool_path=$2
	l_label=${3:-tool}
	l_profile_side=${4:-}

	[ -n "$l_host" ] || return 1
	[ -n "$l_tool_path" ] || return 1

	if ! l_output=$(zxfer_get_remote_resolved_tool_version_output \
		"$l_host" "$l_tool_path" "$l_label" "$l_profile_side"); then
		[ "$l_output" = "" ] || printf '%s\n' "$l_output"
		return 1
	fi

	l_oldifs=$IFS
	IFS='
'
	set -f
	# shellcheck disable=SC2086 # Intentional newline-only splitting to keep the first output line.
	set -- $l_output
	IFS=$l_oldifs

	printf '%s\n' "$1"
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

# Purpose: Best-effort cleanup for a freshly opened SSH control socket when
# setup fails before zxfer can publish the corresponding process lease.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after lease creation fails for a newly opened master so the
# untracked socket and cache directory do not leak.
zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure() {
	l_host=$1
	l_role=$2
	l_socket_path=$3
	l_entry_dir=$4

	if zxfer_run_ssh_control_socket_action_for_host \
		"$l_host" "$l_socket_path" exit; then
		:
	elif [ "${g_zxfer_ssh_control_socket_action_result:-}" = "stale" ]; then
		:
	else
		zxfer_emit_ssh_control_socket_action_failure_message \
			"Warning: Failed to close ssh control socket for $l_role host after lease creation failure." >&2
		return 1
	fi
	if [ -n "${g_zxfer_ssh_control_socket_action_command:-}" ]; then
		zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
	fi
	if zxfer_cleanup_ssh_control_socket_entry_dir "$l_entry_dir"; then
		return 0
	fi
	printf '%s\n' "Warning: Failed to remove ssh control socket cache directory for $l_role host after lease creation failure." >&2
	return 1
}

# Purpose: Set up the shared SSH control socket state for one remote role
# without duplicating lock and cache handling.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management before remote probes or replication traffic reuse a
# multiplexed transport.
#
# setup an ssh control socket for the specified role (origin or target)
zxfer_setup_ssh_control_socket() {
	l_host=$1
	l_role=$2
	l_opened_control_socket=0

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

	if ! l_control_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "$l_host"); then
		[ "$l_control_dir" = "" ] || zxfer_throw_error "$l_control_dir"
		zxfer_throw_error "Error creating temporary directory for ssh control socket."
	fi
	l_control_socket="$l_control_dir/s"
	if ! l_transport_tokens=$(zxfer_get_ssh_base_transport_tokens); then
		zxfer_throw_error "$l_transport_tokens"
	fi

	if zxfer_acquire_ssh_control_socket_lock "$l_control_dir" >/dev/null; then
		l_ssh_lock_dir=$g_zxfer_ssh_control_socket_lock_dir_result
	else
		if [ -n "${g_zxfer_ssh_control_socket_lock_error:-}" ]; then
			zxfer_throw_error \
				"Error creating ssh control socket for $l_role host: $g_zxfer_ssh_control_socket_lock_error"
		fi
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	zxfer_prune_stale_ssh_control_socket_leases "$l_control_dir"
	l_prune_status=$?
	if [ "$l_prune_status" -ne 0 ]; then
		zxfer_release_ssh_control_socket_lock_with_precedence \
			"$l_role" "$l_ssh_lock_dir" "$l_prune_status" >/dev/null 2>&1 || :
		zxfer_emit_ssh_control_socket_lock_failure_message \
			"Error pruning ssh control socket lease entries for $l_role host." >&2
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	if ! zxfer_check_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
		case "${g_zxfer_ssh_control_socket_action_result:-}" in
		stale)
			rm -f "$l_control_socket"
			;;
		*)
			zxfer_release_ssh_control_socket_lock_with_precedence \
				"$l_role" "$l_ssh_lock_dir" 1 >/dev/null 2>&1 || :
			zxfer_emit_ssh_control_socket_action_failure_message \
				"Error checking ssh control socket for $l_role host." >&2
			zxfer_throw_error "Error creating ssh control socket for $l_role host."
			;;
		esac
		if ! zxfer_open_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
			zxfer_release_ssh_control_socket_lock_with_precedence \
				"$l_role" "$l_ssh_lock_dir" 1 >/dev/null 2>&1 || :
			zxfer_throw_error "Error creating ssh control socket for $l_role host."
		fi
		l_opened_control_socket=1
	fi

	zxfer_create_ssh_control_socket_lease_file "$l_control_dir" >/dev/null
	l_lease_status=$?
	if [ "$l_lease_status" -ne 0 ]; then
		if [ "$l_opened_control_socket" -eq 1 ]; then
			zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure \
				"$l_host" "$l_role" "$l_control_socket" "$l_control_dir" || :
		fi
		zxfer_release_ssh_control_socket_lock_with_precedence \
			"$l_role" "$l_ssh_lock_dir" "$l_lease_status" >/dev/null 2>&1 || :
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	l_lease_file=$g_zxfer_runtime_artifact_path_result
	zxfer_set_ssh_control_socket_role_state "$l_role" "$l_control_socket" "$l_control_dir" "$l_lease_file"
	if ! zxfer_release_ssh_control_socket_lock "$l_ssh_lock_dir"; then
		zxfer_emit_ssh_control_socket_lock_failure_message \
			"Error releasing ssh control socket lock for $l_role host." >&2
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
}

# Purpose: Load the SSH control socket role state into shared scratch globals.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management so role-driven helpers can avoid origin/target branches in
# their main control flow.
zxfer_get_ssh_control_socket_role_state() {
	l_role=$1

	g_zxfer_ssh_control_socket_role_host=""
	g_zxfer_ssh_control_socket_role_socket=""
	g_zxfer_ssh_control_socket_role_dir=""
	g_zxfer_ssh_control_socket_role_lease_file=""
	case "$l_role" in
	origin)
		g_zxfer_ssh_control_socket_role_host=${g_option_O_origin_host:-}
		g_zxfer_ssh_control_socket_role_socket=${g_ssh_origin_control_socket:-}
		g_zxfer_ssh_control_socket_role_dir=${g_ssh_origin_control_socket_dir:-}
		g_zxfer_ssh_control_socket_role_lease_file=${g_ssh_origin_control_socket_lease_file:-}
		;;
	target)
		g_zxfer_ssh_control_socket_role_host=${g_option_T_target_host:-}
		g_zxfer_ssh_control_socket_role_socket=${g_ssh_target_control_socket:-}
		g_zxfer_ssh_control_socket_role_dir=${g_ssh_target_control_socket_dir:-}
		g_zxfer_ssh_control_socket_role_lease_file=${g_ssh_target_control_socket_lease_file:-}
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Restore the role's SSH control socket lease after a close/check
# failure while preserving the existing socket and directory state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management when the last-lease close path fails and zxfer must leave
# the shared socket protected for later cleanup or retry.
zxfer_restore_ssh_control_socket_lease_for_role() {
	l_role=$1
	l_socket_path=$2
	l_entry_dir=$3

	zxfer_create_ssh_control_socket_lease_file "$l_entry_dir" >/dev/null ||
		return "$?"
	zxfer_set_ssh_control_socket_role_state \
		"$l_role" "$l_socket_path" "$l_entry_dir" "$g_zxfer_runtime_artifact_path_result"
}

# Purpose: Clean up the SSH control socket entry directory for one role.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after a live socket is closed or found stale.
zxfer_cleanup_ssh_control_socket_entry_dir_for_role() {
	l_role=$1
	l_entry_dir=$2
	l_lock_dir=${3:-}

	l_cleanup_status=0
	zxfer_cleanup_ssh_control_socket_entry_dir "$l_entry_dir" ||
		l_cleanup_status=$?
	if [ "$l_cleanup_status" -eq 0 ]; then
		return 0
	fi
	if [ -n "$l_lock_dir" ]; then
		zxfer_release_ssh_control_socket_lock_with_precedence \
			"$l_role" "$l_lock_dir" "$l_cleanup_status" >/dev/null 2>&1 || :
	fi
	printf '%s\n' "Error removing ssh control socket cache directory for $l_role host." >&2
	return "$l_cleanup_status"
}

# Purpose: Close one role's SSH control socket and release related handles or
# state.
# Usage: Called during remote bootstrap, capability caching, and ssh control-
# socket management after protected work finishes or cleanup takes over.
zxfer_close_ssh_control_socket_for_role() {
	l_role=$1

	zxfer_get_ssh_control_socket_role_state "$l_role" || return 1
	l_host=$g_zxfer_ssh_control_socket_role_host
	l_control_socket=$g_zxfer_ssh_control_socket_role_socket
	l_control_dir=$g_zxfer_ssh_control_socket_role_dir
	l_lease_file=$g_zxfer_ssh_control_socket_role_lease_file
	if [ "$l_host" = "" ] || [ "$l_control_socket" = "" ]; then
		return
	fi

	if [ "$l_lease_file" != "" ]; then
		if zxfer_acquire_ssh_control_socket_lock "$l_control_dir" >/dev/null; then
			l_ssh_lock_dir=$g_zxfer_ssh_control_socket_lock_dir_result
		else
			zxfer_emit_ssh_control_socket_lock_failure_message \
				"Error acquiring ssh control socket lock for $l_role host." >&2
			return 1
		fi
		l_cleanup_status=0
		zxfer_release_ssh_control_socket_lease_file "$l_lease_file" ||
			l_cleanup_status=$?
		if [ "$l_cleanup_status" -ne 0 ]; then
			zxfer_release_ssh_control_socket_lock_with_precedence \
				"$l_role" "$l_ssh_lock_dir" "$l_cleanup_status" >/dev/null 2>&1 || :
			printf '%s\n' "Error removing ssh control socket lease for $l_role host." >&2
			return "$l_cleanup_status"
		fi
		zxfer_prune_stale_ssh_control_socket_leases "$l_control_dir"
		l_prune_status=$?
		if [ "$l_prune_status" -ne 0 ]; then
			zxfer_release_ssh_control_socket_lock_with_precedence \
				"$l_role" "$l_ssh_lock_dir" "$l_prune_status" >/dev/null 2>&1 || :
			zxfer_emit_ssh_control_socket_lock_failure_message \
				"Error pruning ssh control socket lease entries for $l_role host." >&2
			return "$l_prune_status"
		fi
		zxfer_count_ssh_control_socket_leases "$l_control_dir" >/dev/null
		l_count_status=$?
		if [ "$l_count_status" -ne 0 ]; then
			zxfer_release_ssh_control_socket_lock_with_precedence \
				"$l_role" "$l_ssh_lock_dir" "$l_count_status" >/dev/null 2>&1 || :
			zxfer_emit_ssh_control_socket_lock_failure_message \
				"Error counting ssh control socket lease entries for $l_role host." >&2
			return "$l_count_status"
		fi
		l_remaining_leases=$g_zxfer_ssh_control_socket_lease_count_result
		if [ "$l_remaining_leases" -ne 0 ]; then
			zxfer_clear_ssh_control_socket_role_state "$l_role"
			if ! zxfer_release_ssh_control_socket_lock "$l_ssh_lock_dir"; then
				zxfer_emit_ssh_control_socket_lock_failure_message \
					"Error releasing ssh control socket lock for $l_role host." >&2
				return 1
			fi
			return 0
		fi

		if zxfer_check_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
			zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
			if zxfer_run_ssh_control_socket_action_for_host "$l_host" "$l_control_socket" exit; then
				zxfer_cleanup_ssh_control_socket_entry_dir_for_role \
					"$l_role" "$l_control_dir" "$l_ssh_lock_dir" ||
					return "$?"
			elif [ "${g_zxfer_ssh_control_socket_action_result:-}" = "stale" ]; then
				zxfer_cleanup_ssh_control_socket_entry_dir_for_role \
					"$l_role" "$l_control_dir" "$l_ssh_lock_dir" ||
					return "$?"
			else
				if zxfer_restore_ssh_control_socket_lease_for_role \
					"$l_role" "$l_control_socket" "$l_control_dir"; then
					zxfer_release_ssh_control_socket_lock_with_precedence \
						"$l_role" "$l_ssh_lock_dir" 1 >/dev/null 2>&1 || :
					zxfer_emit_ssh_control_socket_action_failure_message \
						"Error closing $l_role ssh control socket." >&2
					return 1
				fi
				zxfer_release_ssh_control_socket_lock_with_precedence \
					"$l_role" "$l_ssh_lock_dir" 1 >/dev/null 2>&1 || :
				zxfer_emit_ssh_control_socket_action_failure_message \
					"Error closing $l_role ssh control socket." >&2
				printf '%s\n' "Error restoring ssh control socket lease for $l_role host." >&2
				return 1
			fi
		elif [ "${g_zxfer_ssh_control_socket_action_result:-}" = "stale" ] &&
			[ "$l_control_dir" != "" ] &&
			[ -d "$l_control_dir" ]; then
			zxfer_cleanup_ssh_control_socket_entry_dir_for_role \
				"$l_role" "$l_control_dir" "$l_ssh_lock_dir" ||
				return "$?"
		else
			if zxfer_restore_ssh_control_socket_lease_for_role \
				"$l_role" "$l_control_socket" "$l_control_dir"; then
				zxfer_release_ssh_control_socket_lock_with_precedence \
					"$l_role" "$l_ssh_lock_dir" 1 >/dev/null 2>&1 || :
				zxfer_emit_ssh_control_socket_action_failure_message \
					"Error checking $l_role ssh control socket." >&2
				return 1
			fi
			zxfer_release_ssh_control_socket_lock_with_precedence \
				"$l_role" "$l_ssh_lock_dir" 1 >/dev/null 2>&1 || :
			zxfer_emit_ssh_control_socket_action_failure_message \
				"Error checking $l_role ssh control socket." >&2
			printf '%s\n' "Error restoring ssh control socket lease for $l_role host." >&2
			return 1
		fi
		if ! zxfer_release_ssh_control_socket_lock "$l_ssh_lock_dir"; then
			zxfer_clear_ssh_control_socket_role_state "$l_role"
			zxfer_emit_ssh_control_socket_lock_failure_message \
				"Error releasing ssh control socket lock for $l_role host." >&2
			return 1
		fi
	else
		if zxfer_run_ssh_control_socket_action_for_host "$l_host" "$l_control_socket" exit; then
			zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
			zxfer_cleanup_ssh_control_socket_entry_dir_for_role "$l_role" "$l_control_dir" ||
				return "$?"
		elif [ "${g_zxfer_ssh_control_socket_action_result:-}" = "stale" ]; then
			zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
			zxfer_cleanup_ssh_control_socket_entry_dir_for_role "$l_role" "$l_control_dir" ||
				return "$?"
		else
			zxfer_echoV "Closing $l_role ssh control socket: $g_zxfer_ssh_control_socket_action_command"
			zxfer_emit_ssh_control_socket_action_failure_message \
				"Error closing $l_role ssh control socket." >&2
			return 1
		fi
	fi
	zxfer_clear_ssh_control_socket_role_state "$l_role"
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

	if [ "$g_option_O_origin_host" != "" ] || [ "$g_option_T_target_host" != "" ]; then
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
	zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
}
