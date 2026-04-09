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
# mutates caches: remote capability cache files and shared ssh control-socket directories.
# returns via stdout: remote OS/tool paths, ssh argv renderings, and remote-safe command strings.

ZXFER_SSH_CONTROL_SOCKET_PATH_MAX=104
ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE=".Mvij6x1tYLn6woxm"

################################################################################
# SSH CONTROL SOCKET SUPPORT / CACHE KEYS
################################################################################

zxfer_ssh_supports_control_sockets() {
	[ -n "${g_cmd_ssh:-}" ] || return 1
	"$g_cmd_ssh" -M -V >/dev/null 2>&1
}

zxfer_ssh_control_socket_cache_key() {
	l_host_spec=$1
	l_identity=$(printf '%s\n%s\n' "${g_cmd_ssh:-ssh}" "$l_host_spec")
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

zxfer_render_ssh_control_socket_entry_identity() {
	l_host_spec=$1
	printf '%s\n%s\n' "${g_cmd_ssh:-ssh}" "$l_host_spec"
}

zxfer_is_ssh_control_socket_entry_path_short_enough() {
	l_entry_dir=$1
	l_temp_listener_path="$l_entry_dir/s$ZXFER_SSH_CONTROL_SOCKET_TEMP_SUFFIX_SAMPLE"

	[ "${#l_temp_listener_path}" -lt "$ZXFER_SSH_CONTROL_SOCKET_PATH_MAX" ]
}

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

	cat "$l_identity_path" 2>/dev/null
}

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
	if ! l_tmp_identity_path=$(mktemp "$l_entry_dir/id.XXXXXX" 2>/dev/null); then
		return 1
	fi

	if ! (
		umask 077
		zxfer_render_ssh_control_socket_entry_identity "$l_host_spec"
	) >"$l_tmp_identity_path"; then
		rm -f "$l_tmp_identity_path"
		return 1
	fi

	chmod 600 "$l_tmp_identity_path" 2>/dev/null || :
	if ! mv -f "$l_tmp_identity_path" "$l_identity_path" 2>/dev/null; then
		rm -f "$l_tmp_identity_path"
		return 1
	fi
	chmod 600 "$l_identity_path" 2>/dev/null || :
	return 0
}

zxfer_ensure_ssh_control_socket_cache_dir() {
	if ! l_tmpdir=$(zxfer_try_get_socket_cache_tmpdir); then
		return 1
	fi
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	l_cache_dir="$l_tmpdir/zxfer-s.$l_effective_uid.d"

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

zxfer_ensure_ssh_control_socket_entry_dir() {
	l_host_spec=$1

	if ! l_cache_key=$(zxfer_ssh_control_socket_cache_key "$l_host_spec"); then
		return 1
	fi
	if ! l_cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$l_cache_key"); then
		return 1
	fi
	l_expected_identity=$(zxfer_render_ssh_control_socket_entry_identity "$l_host_spec")
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

zxfer_acquire_ssh_control_socket_lock() {
	l_entry_dir=$1
	l_lock_dir="$l_entry_dir.lock"
	l_attempts=0

	while ! mkdir "$l_lock_dir" 2>/dev/null; do
		if [ -L "$l_lock_dir" ] || [ -h "$l_lock_dir" ]; then
			return 1
		fi
		l_attempts=$((l_attempts + 1))
		[ "$l_attempts" -lt 10 ] || return 1
		sleep 1
	done

	printf '%s\n' "$l_lock_dir"
}

zxfer_release_ssh_control_socket_lock() {
	l_lock_dir=$1
	[ -n "$l_lock_dir" ] || return 0
	[ -d "$l_lock_dir" ] || return 0
	rmdir "$l_lock_dir" >/dev/null 2>&1 || :
}

zxfer_prune_stale_ssh_control_socket_leases() {
	l_entry_dir=$1
	l_leases_dir="$l_entry_dir/leases"

	[ -d "$l_leases_dir" ] || return 0

	set -- "$l_leases_dir"/lease.*
	[ -e "$1" ] || return 0

	for l_lease_path in "$@"; do
		[ -e "$l_lease_path" ] || continue
		l_lease_name=$(basename "$l_lease_path")
		l_lease_pid=${l_lease_name#lease.}
		l_lease_pid=${l_lease_pid%%.*}
		case "$l_lease_pid" in
		'' | *[!0-9]*)
			rm -f "$l_lease_path"
			continue
			;;
		esac
		if ! kill -s 0 "$l_lease_pid" 2>/dev/null; then
			rm -f "$l_lease_path"
		fi
	done
}

zxfer_count_ssh_control_socket_leases() {
	l_entry_dir=$1
	l_leases_dir="$l_entry_dir/leases"
	l_count=0

	[ -d "$l_leases_dir" ] || {
		printf '%s\n' "0"
		return 0
	}

	set -- "$l_leases_dir"/lease.*
	if [ ! -e "$1" ]; then
		printf '%s\n' "0"
		return 0
	fi

	for l_lease_path in "$@"; do
		[ -e "$l_lease_path" ] || continue
		l_count=$((l_count + 1))
	done
	printf '%s\n' "$l_count"
}

zxfer_create_ssh_control_socket_lease_file() {
	l_entry_dir=$1
	l_leases_dir="$l_entry_dir/leases"
	l_timestamp=$(date +%s)
	mktemp "$l_leases_dir/lease.$$.${l_timestamp}.XXXXXX" 2>/dev/null
}

zxfer_check_ssh_control_socket_for_host() {
	l_host=$1
	l_socket_path=$2

	[ -n "$l_host" ] || return 1
	[ -n "$l_socket_path" ] || return 1

	l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host")
	set -- "$g_cmd_ssh" -S "$l_socket_path" -O check
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi

	"$@" >/dev/null 2>&1
}

zxfer_open_ssh_control_socket_for_host() {
	l_host=$1
	l_socket_path=$2

	[ -n "$l_host" ] || return 1
	[ -n "$l_socket_path" ] || return 1

	l_host_tokens=$(zxfer_split_host_spec_tokens "$l_host")
	set -- "$g_cmd_ssh" -M -S "$l_socket_path" -fN
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi

	"$@"
}

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

################################################################################
# REMOTE CAPABILITY CACHE / HANDSHAKE PARSING
################################################################################

zxfer_remote_capability_cache_key() {
	l_host_spec=$1
	l_key_hex=$(printf '%s\n%s' "$l_host_spec" "${g_zxfer_dependency_path:-$ZXFER_DEFAULT_SECURE_PATH}" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	printf '%s\n' "$l_key_hex"
}

zxfer_ensure_remote_capability_cache_dir() {
	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		return 1
	fi
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	l_cache_dir="$l_tmpdir/zxfer-remote-capabilities.$l_effective_uid.d"

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

zxfer_remote_capability_cache_path() {
	l_host_spec=$1
	if ! l_cache_dir=$(zxfer_ensure_remote_capability_cache_dir); then
		return 1
	fi
	if ! l_cache_key=$(zxfer_remote_capability_cache_key "$l_host_spec"); then
		return 1
	fi
	printf '%s\n' "$l_cache_dir/$l_cache_key"
}

zxfer_remote_capability_cache_lock_path() {
	l_host_spec=$1
	if ! l_cache_path=$(zxfer_remote_capability_cache_path "$l_host_spec"); then
		return 1
	fi
	printf '%s.lock\n' "$l_cache_path"
}

zxfer_validate_remote_capability_cache_lock_dir() {
	l_lock_dir=$1

	[ -d "$l_lock_dir" ] || return 1
	[ ! -L "$l_lock_dir" ] || return 1
	[ ! -h "$l_lock_dir" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_lock_dir"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_lock_dir"); then
		return 1
	fi
	[ "$l_mode" = "700" ] || return 1
}

zxfer_read_remote_capability_cache_lock_pid_file() {
	l_pid_path=$1

	[ -f "$l_pid_path" ] || return 1
	[ ! -L "$l_pid_path" ] || return 1
	[ ! -h "$l_pid_path" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_pid_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_pid_path"); then
		return 1
	fi
	[ "$l_mode" = "600" ] || return 1

	l_lock_pid=$(cat "$l_pid_path" 2>/dev/null) || return 1
	case "$l_lock_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	printf '%s\n' "$l_lock_pid"
}

zxfer_write_remote_capability_cache_lock_pid_file() {
	l_lock_dir=$1
	l_pid_path="$l_lock_dir/pid"

	[ ! -L "$l_pid_path" ] || return 1
	[ ! -h "$l_pid_path" ] || return 1
	if ! l_tmp_pid_path=$(mktemp "$l_lock_dir/pid.XXXXXX" 2>/dev/null); then
		return 1
	fi

	if ! (
		umask 077
		printf '%s\n' "$$"
	) >"$l_tmp_pid_path"; then
		rm -f "$l_tmp_pid_path"
		return 1
	fi

	chmod 600 "$l_tmp_pid_path" 2>/dev/null || :
	if ! mv -f "$l_tmp_pid_path" "$l_pid_path" 2>/dev/null; then
		rm -f "$l_tmp_pid_path"
		return 1
	fi
	chmod 600 "$l_pid_path" 2>/dev/null || :
	return 0
}

zxfer_create_remote_capability_cache_lock_dir() {
	l_lock_dir=$1
	l_old_umask=$(umask)
	umask 077
	if ! mkdir "$l_lock_dir" 2>/dev/null; then
		umask "$l_old_umask"
		return 1
	fi
	umask "$l_old_umask"

	if ! zxfer_validate_remote_capability_cache_lock_dir "$l_lock_dir"; then
		rm -rf "$l_lock_dir"
		return 1
	fi
	if ! zxfer_write_remote_capability_cache_lock_pid_file "$l_lock_dir"; then
		rm -rf "$l_lock_dir"
		return 1
	fi
	return 0
}

zxfer_try_acquire_remote_capability_cache_lock() {
	l_host_spec=$1

	if ! l_lock_dir=$(zxfer_remote_capability_cache_lock_path "$l_host_spec"); then
		return 1
	fi
	[ ! -L "$l_lock_dir" ] || return 1
	[ ! -h "$l_lock_dir" ] || return 1

	if zxfer_create_remote_capability_cache_lock_dir "$l_lock_dir"; then
		printf '%s\n' "$l_lock_dir"
		return 0
	fi

	[ -d "$l_lock_dir" ] || return 1
	if ! zxfer_validate_remote_capability_cache_lock_dir "$l_lock_dir"; then
		return 1
	fi

	l_lock_pid=""
	l_lock_pid_path="$l_lock_dir/pid"
	if [ -e "$l_lock_pid_path" ] &&
		! l_lock_pid=$(zxfer_read_remote_capability_cache_lock_pid_file "$l_lock_pid_path"); then
		return 1
	fi

	if [ -n "$l_lock_pid" ]; then
		if ! kill -s 0 "$l_lock_pid" 2>/dev/null; then
			if ! rm -rf "$l_lock_dir" 2>/dev/null; then
				return 1
			fi
			if zxfer_create_remote_capability_cache_lock_dir "$l_lock_dir"; then
				printf '%s\n' "$l_lock_dir"
				return 0
			fi
			[ -d "$l_lock_dir" ] || return 1
			if ! zxfer_validate_remote_capability_cache_lock_dir "$l_lock_dir"; then
				return 1
			fi
		fi
	fi

	return 2
}

zxfer_release_remote_capability_cache_lock() {
	l_lock_dir=$1

	[ -n "$l_lock_dir" ] || return 0
	[ -e "$l_lock_dir" ] || return 0
	[ ! -L "$l_lock_dir" ] || return 1
	[ ! -h "$l_lock_dir" ] || return 1
	[ -d "$l_lock_dir" ] || return 1
	rm -rf "$l_lock_dir" 2>/dev/null
}

zxfer_wait_for_remote_capability_cache_fill() {
	l_host_spec=$1
	l_wait_retries=${g_zxfer_remote_capability_cache_wait_retries:-5}
	l_wait_count=0

	case "$l_wait_retries" in
	'' | *[!0-9]*)
		l_wait_retries=5
		;;
	esac
	[ "$l_wait_retries" -gt 0 ] || l_wait_retries=5

	while [ "$l_wait_count" -lt "$l_wait_retries" ]; do
		if l_cached_response=$(zxfer_read_remote_capability_cache_file "$l_host_spec"); then
			printf '%s\n' "$l_cached_response"
			return 0
		fi
		l_wait_count=$((l_wait_count + 1))
		[ "$l_wait_count" -lt "$l_wait_retries" ] || break
		sleep 1
	done

	return 1
}

zxfer_reap_stale_pidless_remote_capability_cache_lock() {
	l_host_spec=$1

	if ! l_lock_dir=$(zxfer_remote_capability_cache_lock_path "$l_host_spec"); then
		return 1
	fi
	[ -e "$l_lock_dir" ] || return 0
	if ! zxfer_validate_remote_capability_cache_lock_dir "$l_lock_dir"; then
		return 1
	fi
	[ ! -e "$l_lock_dir/pid" ] || return 0
	rm -rf "$l_lock_dir" 2>/dev/null
}

zxfer_parse_remote_capability_response() {
	l_response=$1
	l_tab='	'

	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_zfs_path=""
	g_zxfer_remote_capability_parallel_status=""
	g_zxfer_remote_capability_parallel_path=""
	g_zxfer_remote_capability_cat_status=""
	g_zxfer_remote_capability_cat_path=""

	l_line_number=0
	while IFS= read -r l_line || [ -n "$l_line" ]; do
		l_line_number=$((l_line_number + 1))
		case "$l_line_number" in
		1)
			[ "$l_line" = "ZXFER_REMOTE_CAPS_V1" ] || return 1
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
		3 | 4 | 5)
			OLDIFS=$IFS
			IFS='	'
			read -r l_record_kind l_record_tool l_record_status l_record_path l_record_extra <<-EOF
				$l_line
			EOF
			IFS=$OLDIFS

			[ "$l_record_kind" = "tool" ] || return 1
			[ -z "$l_record_extra" ] || return 1
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
			*)
				return 1
				;;
			esac
			;;
		*)
			return 1
			;;
		esac
	done <<-EOF
		$l_response
	EOF

	[ "$l_line_number" -eq 5 ] || return 1
	[ -n "$g_zxfer_remote_capability_os" ] || return 1
	[ -n "$g_zxfer_remote_capability_zfs_status" ] || return 1
	[ -n "$g_zxfer_remote_capability_parallel_status" ] || return 1
	[ -n "$g_zxfer_remote_capability_cat_status" ] || return 1
	return 0
}

zxfer_get_cached_remote_capability_response_for_host() {
	l_host_spec=$1

	if [ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
		[ -n "${g_origin_remote_capabilities_response:-}" ]; then
		printf '%s\n' "$g_origin_remote_capabilities_response"
		return 0
	fi

	if [ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
		[ -n "${g_target_remote_capabilities_response:-}" ]; then
		printf '%s\n' "$g_target_remote_capabilities_response"
		return 0
	fi

	return 1
}

zxfer_store_cached_remote_capability_response_for_host() {
	l_host_spec=$1
	l_response=$2
	l_stored=0

	if [ "$l_host_spec" = "${g_option_O_origin_host:-}" ] ||
		[ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ]; then
		g_origin_remote_capabilities_host=$l_host_spec
		g_origin_remote_capabilities_response=$l_response
		l_stored=1
	fi

	if [ "$l_host_spec" = "${g_option_T_target_host:-}" ] ||
		[ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ]; then
		g_target_remote_capabilities_host=$l_host_spec
		g_target_remote_capabilities_response=$l_response
		l_stored=1
	fi

	if [ "$l_stored" -eq 0 ] &&
		[ "${g_origin_remote_capabilities_host:-}" = "" ]; then
		g_origin_remote_capabilities_host=$l_host_spec
		g_origin_remote_capabilities_response=$l_response
		return
	fi

	if [ "$l_stored" -eq 0 ]; then
		g_target_remote_capabilities_host=$l_host_spec
		g_target_remote_capabilities_response=$l_response
	fi
}

zxfer_note_remote_capability_bootstrap_source_for_host() {
	l_host_spec=$1
	l_source=$2

	[ -n "$l_host_spec" ] || return 0
	[ -n "$l_source" ] || return 0

	if [ "$l_host_spec" = "${g_option_O_origin_host:-}" ] ||
		[ "$l_host_spec" = "${g_origin_remote_capabilities_host:-}" ]; then
		if [ "${g_origin_remote_capabilities_bootstrap_source:-}" = "" ]; then
			g_origin_remote_capabilities_bootstrap_source=$l_source
		fi
	fi

	if [ "$l_host_spec" = "${g_option_T_target_host:-}" ] ||
		[ "$l_host_spec" = "${g_target_remote_capabilities_host:-}" ]; then
		if [ "${g_target_remote_capabilities_bootstrap_source:-}" = "" ]; then
			g_target_remote_capabilities_bootstrap_source=$l_source
		fi
	fi
}

zxfer_read_remote_capability_cache_file() {
	l_host_spec=$1
	l_now=$(date '+%s' 2>/dev/null || :)
	[ -n "$l_now" ] || return 1

	if ! l_cache_path=$(zxfer_remote_capability_cache_path "$l_host_spec"); then
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

	l_cached_contents=$(cat "$l_cache_path" 2>/dev/null) || return 1
	l_cache_epoch=$(printf '%s\n' "$l_cached_contents" | sed -n '1p')
	l_cached_response=$(printf '%s\n' "$l_cached_contents" | sed '1d')

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

zxfer_write_remote_capability_cache_file() {
	l_host_spec=$1
	l_response=$2
	l_now=$(date '+%s' 2>/dev/null || :)
	[ -n "$l_now" ] || return 1

	if ! l_cache_path=$(zxfer_remote_capability_cache_path "$l_host_spec"); then
		return 1
	fi
	l_cache_dir=${l_cache_path%/*}
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
	if ! l_tmp_cache_path=$(mktemp "$l_cache_dir/zxfer.remote-capabilities.XXXXXX" 2>/dev/null); then
		return 1
	fi

	if ! (
		umask 077
		printf '%s\n' "$l_now"
		printf '%s\n' "$l_response"
	) >"$l_tmp_cache_path"; then
		rm -f "$l_tmp_cache_path"
		return 1
	fi

	chmod 600 "$l_tmp_cache_path" 2>/dev/null || :
	if ! mv -f "$l_tmp_cache_path" "$l_cache_path" 2>/dev/null; then
		rm -f "$l_tmp_cache_path"
		return 1
	fi
	chmod 600 "$l_cache_path" 2>/dev/null || :
	return 0
}

zxfer_fetch_remote_host_capabilities_live() {
	l_host_spec=$1
	l_profile_side=${2:-}

	[ -n "$l_host_spec" ] || return 1

	l_dependency_path=${g_zxfer_dependency_path:-$ZXFER_DEFAULT_SECURE_PATH}
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; l_os=\$(uname 2>/dev/null) || exit \$?; printf '%s\n' 'ZXFER_REMOTE_CAPS_V1'; printf '%s\t%s\n' 'os' \"\$l_os\"; for l_tool in zfs parallel cat; do l_path=\$(command -v \"\$l_tool\" 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\t%s\t0\t%s\n' 'tool' \"\$l_tool\" \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then printf '%s\t%s\t1\t-\n' 'tool' \"\$l_tool\"; else printf '%s\t%s\t%s\t-\n' 'tool' \"\$l_tool\" \"\$l_status\"; fi; done"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	l_remote_output=$(zxfer_invoke_ssh_shell_command_for_host "$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side" 2>/dev/null)
	l_remote_status=$?

	[ "$l_remote_status" -eq 0 ] || return 1
	zxfer_parse_remote_capability_response "$l_remote_output" || return 1

	printf '%s\n' "$l_remote_output"
}

zxfer_ensure_remote_host_capabilities() {
	l_host_spec=$1
	l_profile_side=${2:-}

	[ -n "$l_host_spec" ] || return 1

	if l_cached_response=$(zxfer_get_cached_remote_capability_response_for_host "$l_host_spec"); then
		if zxfer_parse_remote_capability_response "$l_cached_response"; then
			zxfer_note_remote_capability_bootstrap_source_for_host "$l_host_spec" memory
			printf '%s\n' "$l_cached_response"
			return 0
		fi
	fi

	if l_cached_response=$(zxfer_read_remote_capability_cache_file "$l_host_spec"); then
		zxfer_store_cached_remote_capability_response_for_host "$l_host_spec" "$l_cached_response"
		zxfer_note_remote_capability_bootstrap_source_for_host "$l_host_spec" cache
		printf '%s\n' "$l_cached_response"
		return 0
	fi

	l_capability_lock_dir=""
	l_capability_lock_dir=$(zxfer_try_acquire_remote_capability_cache_lock "$l_host_spec")
	l_lock_status=$?
	if [ "$l_lock_status" -ne 0 ]; then
		case "$l_lock_status" in
		1)
			return 1
			;;
		2)
			if l_cached_response=$(zxfer_wait_for_remote_capability_cache_fill "$l_host_spec"); then
				zxfer_store_cached_remote_capability_response_for_host "$l_host_spec" "$l_cached_response"
				zxfer_note_remote_capability_bootstrap_source_for_host "$l_host_spec" cache
				printf '%s\n' "$l_cached_response"
				return 0
			fi
			if ! zxfer_reap_stale_pidless_remote_capability_cache_lock "$l_host_spec"; then
				return 1
			fi
			l_capability_lock_dir=$(zxfer_try_acquire_remote_capability_cache_lock "$l_host_spec")
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

	if ! l_live_response=$(zxfer_fetch_remote_host_capabilities_live "$l_host_spec" "$l_profile_side"); then
		if [ -n "$l_capability_lock_dir" ]; then
			zxfer_release_remote_capability_cache_lock "$l_capability_lock_dir" >/dev/null 2>&1 || :
		fi
		return 1
	fi

	zxfer_store_cached_remote_capability_response_for_host "$l_host_spec" "$l_live_response"
	zxfer_note_remote_capability_bootstrap_source_for_host "$l_host_spec" live
	zxfer_write_remote_capability_cache_file "$l_host_spec" "$l_live_response" >/dev/null 2>&1 || :
	if [ -n "$l_capability_lock_dir" ]; then
		zxfer_release_remote_capability_cache_lock "$l_capability_lock_dir" >/dev/null 2>&1 || :
	fi
	printf '%s\n' "$l_live_response"
}

zxfer_preload_remote_host_capabilities() {
	l_host_spec=$1
	l_profile_side=${2:-}

	zxfer_ensure_remote_host_capabilities "$l_host_spec" "$l_profile_side" >/dev/null
}

zxfer_get_remote_host_operating_system_direct() {
	l_host_spec=$1
	l_profile_side=${2:-}

	l_dependency_path=${g_zxfer_dependency_path:-$ZXFER_DEFAULT_SECURE_PATH}
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; uname 2>/dev/null"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	l_remote_output=$(zxfer_invoke_ssh_shell_command_for_host "$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side" 2>/dev/null)
	l_remote_status=$?

	[ "$l_remote_status" -eq 0 ] || return 1
	l_remote_os=$(printf '%s\n' "$l_remote_output" | sed -n '1p')
	[ -n "$l_remote_os" ] || return 1
	printf '%s\n' "$l_remote_os"
}

zxfer_get_remote_host_operating_system() {
	l_host_spec=$1
	l_profile_side=${2:-}

	if ! l_response=$(zxfer_ensure_remote_host_capabilities "$l_host_spec" "$l_profile_side"); then
		zxfer_get_remote_host_operating_system_direct "$l_host_spec" "$l_profile_side"
		return
	fi
	if ! zxfer_parse_remote_capability_response "$l_response"; then
		zxfer_get_remote_host_operating_system_direct "$l_host_spec" "$l_profile_side"
		return
	fi
	printf '%s\n' "$g_zxfer_remote_capability_os"
}

################################################################################
# REMOTE TOOL / COMMAND RESOLUTION
################################################################################

zxfer_resolve_remote_required_tool() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}

	[ -n "$l_host" ] || return 1

	if ! l_remote_caps=$(zxfer_ensure_remote_host_capabilities "$l_host" "$l_profile_side"); then
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
	zfs)
		l_tool_status=$g_zxfer_remote_capability_zfs_status
		l_resolved_path=$g_zxfer_remote_capability_zfs_path
		;;
	parallel)
		l_tool_status=$g_zxfer_remote_capability_parallel_status
		l_resolved_path=$g_zxfer_remote_capability_parallel_path
		;;
	cat)
		l_tool_status=$g_zxfer_remote_capability_cat_status
		l_resolved_path=$g_zxfer_remote_capability_cat_path
		;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac

	case "$l_tool_status" in
	0)
		zxfer_validate_resolved_tool_path "$l_resolved_path" "$l_label" "host $l_host"
		;;
	1)
		printf '%s\n' "Required dependency \"$l_label\" not found on host $l_host in secure PATH ($g_zxfer_dependency_path). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
		return 1
		;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

zxfer_resolve_remote_cli_tool_direct() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}

	l_dependency_path=${g_zxfer_dependency_path:-$ZXFER_DEFAULT_SECURE_PATH}
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_tool_single=$(zxfer_escape_for_single_quotes "$l_tool")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; l_path=\$(command -v '$l_tool_single' 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\n' \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then exit 10; else exit \"\$l_status\"; fi"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	l_remote_output=$(zxfer_invoke_ssh_shell_command_for_host "$l_host" "$l_remote_probe_cmd" "$l_profile_side" 2>/dev/null)
	l_remote_status=$?

	case "$l_remote_status" in
	0)
		zxfer_validate_resolved_tool_path "$l_remote_output" "$l_label" "host $l_host"
		;;
	10)
		printf '%s\n' "Required dependency \"$l_label\" not found on host $l_host in secure PATH ($g_zxfer_dependency_path). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
		return 1
		;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

zxfer_resolve_remote_cli_tool() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}

	case "$l_tool" in
	zfs | parallel | cat)
		zxfer_resolve_remote_required_tool "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
		;;
	esac

	zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
}

zxfer_resolve_remote_cli_command_safe() {
	l_host=$1
	l_cli_string=$2
	l_label=${3:-command}
	l_profile_side=${4:-}
	l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string")
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	if [ -z "$l_cli_head" ]; then
		printf '%s\n' "Required dependency \"$l_label\" must not be empty or whitespace-only."
		return 1
	fi

	if ! l_resolved_head=$(zxfer_resolve_remote_cli_tool "$l_host" "$l_cli_head" "$l_label" "$l_profile_side"); then
		printf '%s\n' "$l_resolved_head"
		return 1
	fi

	zxfer_requote_cli_command_with_resolved_head "$l_cli_string" "$l_resolved_head"
}

# setup an ssh control socket for the specified role (origin or target)
zxfer_setup_ssh_control_socket() {
	l_host=$1
	l_role=$2

	[ -z "$l_host" ] && return

	case "$l_role" in
	origin)
		[ "$g_ssh_origin_control_socket" != "" ] && zxfer_close_origin_ssh_control_socket
		;;
	target)
		[ "$g_ssh_target_control_socket" != "" ] && zxfer_close_target_ssh_control_socket
		;;
	esac

	if ! l_control_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "$l_host"); then
		zxfer_throw_error "Error creating temporary directory for ssh control socket."
	fi
	l_control_socket="$l_control_dir/s"

	if ! l_lock_dir=$(zxfer_acquire_ssh_control_socket_lock "$l_control_dir"); then
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	zxfer_prune_stale_ssh_control_socket_leases "$l_control_dir"
	if ! zxfer_check_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
		rm -f "$l_control_socket"
		if ! zxfer_open_ssh_control_socket_for_host "$l_host" "$l_control_socket"; then
			zxfer_release_ssh_control_socket_lock "$l_lock_dir"
			zxfer_throw_error "Error creating ssh control socket for $l_role host."
		fi
	fi

	if ! l_lease_file=$(zxfer_create_ssh_control_socket_lease_file "$l_control_dir"); then
		zxfer_release_ssh_control_socket_lock "$l_lock_dir"
		zxfer_throw_error "Error creating ssh control socket for $l_role host."
	fi
	zxfer_release_ssh_control_socket_lock "$l_lock_dir"

	zxfer_set_ssh_control_socket_role_state "$l_role" "$l_control_socket" "$l_control_dir" "$l_lease_file"
}

zxfer_close_origin_ssh_control_socket() {
	if [ "$g_option_O_origin_host" = "" ] || [ "$g_ssh_origin_control_socket" = "" ]; then
		return
	fi

	l_host_tokens=$(zxfer_split_host_spec_tokens "$g_option_O_origin_host")
	set -- "$g_cmd_ssh" -S "$g_ssh_origin_control_socket" -O exit
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi
	if [ "$g_ssh_origin_control_socket_lease_file" != "" ]; then
		if l_lock_dir=$(zxfer_acquire_ssh_control_socket_lock "$g_ssh_origin_control_socket_dir"); then
			rm -f "$g_ssh_origin_control_socket_lease_file"
			zxfer_prune_stale_ssh_control_socket_leases "$g_ssh_origin_control_socket_dir"
			l_remaining_leases=$(zxfer_count_ssh_control_socket_leases "$g_ssh_origin_control_socket_dir")
			zxfer_release_ssh_control_socket_lock "$l_lock_dir"
		else
			l_remaining_leases=1
		fi
		if [ "$l_remaining_leases" -eq 0 ]; then
			if zxfer_check_ssh_control_socket_for_host "$g_option_O_origin_host" "$g_ssh_origin_control_socket"; then
				l_log_cmd=$(zxfer_build_shell_command_from_argv "$g_cmd_ssh" -S "$g_ssh_origin_control_socket" -O exit)
				l_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_O_origin_host")
				if [ "$l_host_safe" != "" ]; then
					l_log_cmd="$l_log_cmd $l_host_safe"
				fi
				zxfer_echoV "Closing origin ssh control socket: $l_log_cmd"
				if "$@" 2>/dev/null; then
					[ -d "$g_ssh_origin_control_socket_dir" ] && rm -rf "$g_ssh_origin_control_socket_dir"
				fi
			elif [ "$g_ssh_origin_control_socket_dir" != "" ] && [ -d "$g_ssh_origin_control_socket_dir" ]; then
				rm -rf "$g_ssh_origin_control_socket_dir"
			fi
		fi
	else
		l_log_cmd=$(zxfer_build_shell_command_from_argv "$g_cmd_ssh" -S "$g_ssh_origin_control_socket" -O exit)
		l_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_O_origin_host")
		if [ "$l_host_safe" != "" ]; then
			l_log_cmd="$l_log_cmd $l_host_safe"
		fi
		zxfer_echoV "Closing origin ssh control socket: $l_log_cmd"
		"$@" 2>/dev/null

		if [ "$g_ssh_origin_control_socket_dir" != "" ] && [ -d "$g_ssh_origin_control_socket_dir" ]; then
			rm -rf "$g_ssh_origin_control_socket_dir"
		fi
	fi
	zxfer_clear_ssh_control_socket_role_state origin
}

zxfer_close_target_ssh_control_socket() {
	if [ "$g_option_T_target_host" = "" ] || [ "$g_ssh_target_control_socket" = "" ]; then
		return
	fi

	l_host_tokens=$(zxfer_split_host_spec_tokens "$g_option_T_target_host")
	set -- "$g_cmd_ssh" -S "$g_ssh_target_control_socket" -O exit
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi
	if [ "$g_ssh_target_control_socket_lease_file" != "" ]; then
		if l_lock_dir=$(zxfer_acquire_ssh_control_socket_lock "$g_ssh_target_control_socket_dir"); then
			rm -f "$g_ssh_target_control_socket_lease_file"
			zxfer_prune_stale_ssh_control_socket_leases "$g_ssh_target_control_socket_dir"
			l_remaining_leases=$(zxfer_count_ssh_control_socket_leases "$g_ssh_target_control_socket_dir")
			zxfer_release_ssh_control_socket_lock "$l_lock_dir"
		else
			l_remaining_leases=1
		fi
		if [ "$l_remaining_leases" -eq 0 ]; then
			if zxfer_check_ssh_control_socket_for_host "$g_option_T_target_host" "$g_ssh_target_control_socket"; then
				l_log_cmd=$(zxfer_build_shell_command_from_argv "$g_cmd_ssh" -S "$g_ssh_target_control_socket" -O exit)
				l_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
				if [ "$l_host_safe" != "" ]; then
					l_log_cmd="$l_log_cmd $l_host_safe"
				fi
				zxfer_echoV "Closing target ssh control socket: $l_log_cmd"
				if "$@" 2>/dev/null; then
					[ -d "$g_ssh_target_control_socket_dir" ] && rm -rf "$g_ssh_target_control_socket_dir"
				fi
			elif [ "$g_ssh_target_control_socket_dir" != "" ] && [ -d "$g_ssh_target_control_socket_dir" ]; then
				rm -rf "$g_ssh_target_control_socket_dir"
			fi
		fi
	else
		l_log_cmd=$(zxfer_build_shell_command_from_argv "$g_cmd_ssh" -S "$g_ssh_target_control_socket" -O exit)
		l_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
		if [ "$l_host_safe" != "" ]; then
			l_log_cmd="$l_log_cmd $l_host_safe"
		fi
		zxfer_echoV "Closing target ssh control socket: $l_log_cmd"
		"$@" 2>/dev/null

		if [ "$g_ssh_target_control_socket_dir" != "" ] && [ -d "$g_ssh_target_control_socket_dir" ]; then
			rm -rf "$g_ssh_target_control_socket_dir"
		fi
	fi
	zxfer_clear_ssh_control_socket_role_state target
}

zxfer_close_all_ssh_control_sockets() {
	zxfer_close_origin_ssh_control_socket
	zxfer_close_target_ssh_control_socket
}

################################################################################
# REMOTE CONNECTION BOOTSTRAP / ACTIVE COMMAND SELECTION
################################################################################

# shellcheck disable=SC2034
zxfer_refresh_remote_zfs_commands() {
	if [ "$g_option_O_origin_host" != "" ]; then
		g_option_O_origin_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_O_origin_host")
		g_LZFS=${g_origin_cmd_zfs:-$g_cmd_zfs}
	else
		g_option_O_origin_host_safe=""
		g_LZFS=$g_cmd_zfs
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		g_option_T_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
		g_RZFS=${g_target_cmd_zfs:-$g_cmd_zfs}
	else
		g_option_T_target_host_safe=""
		g_RZFS=$g_cmd_zfs
	fi
}

zxfer_prepare_remote_host_connections() {
	l_ssh_setup_start_ms=""

	if [ "$g_option_O_origin_host" != "" ] || [ "$g_option_T_target_host" != "" ]; then
		l_ssh_setup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	if [ "$g_option_O_origin_host" != "" ]; then
		if [ "${g_ssh_supports_control_sockets:-0}" -eq 1 ]; then
			zxfer_setup_ssh_control_socket "$g_option_O_origin_host" "origin"
		else
			zxfer_echoV "ssh client does not support control sockets; continuing without connection reuse for origin host."
		fi
		zxfer_preload_remote_host_capabilities "$g_option_O_origin_host" source >/dev/null 2>&1 || :
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		if [ "${g_ssh_supports_control_sockets:-0}" -eq 1 ]; then
			zxfer_setup_ssh_control_socket "$g_option_T_target_host" "target"
		else
			zxfer_echoV "ssh client does not support control sockets; continuing without connection reuse for target host."
		fi
		zxfer_preload_remote_host_capabilities "$g_option_T_target_host" destination >/dev/null 2>&1 || :
	fi

	zxfer_refresh_remote_zfs_commands
	zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
}
