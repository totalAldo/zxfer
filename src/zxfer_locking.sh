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
# OWNED LOCK / LEASE COORDINATION
################################################################################

# Module contract:
# owns globals: owned-lock metadata scratch and run-owned long-lived lock/lease cleanup registrations.
# reads globals: none directly, but later runtime helpers may call into the registration helpers.
# mutates caches: metadata-bearing local lock and lease directories.
# returns via stdout: normalized process-start tokens, metadata paths, and created lock/lease paths.

ZXFER_LOCK_METADATA_HEADER="ZXFER_LOCK_METADATA_V1"

zxfer_reset_owned_lock_metadata_result() {
	g_zxfer_owned_lock_kind_result=""
	g_zxfer_owned_lock_purpose_result=""
	g_zxfer_owned_lock_pid_result=""
	g_zxfer_owned_lock_start_token_result=""
	g_zxfer_owned_lock_hostname_result=""
	g_zxfer_owned_lock_created_at_result=""
}

zxfer_reset_owned_lock_tracking() {
	g_zxfer_owned_lock_cleanup_paths=""
	zxfer_reset_owned_lock_metadata_result
}

zxfer_register_owned_lock_path() {
	l_lock_path=$1

	[ -n "$l_lock_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_lock_path" ] && return 0
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	if [ -n "${g_zxfer_owned_lock_cleanup_paths:-}" ]; then
		g_zxfer_owned_lock_cleanup_paths=$g_zxfer_owned_lock_cleanup_paths'
'$l_lock_path
	else
		g_zxfer_owned_lock_cleanup_paths=$l_lock_path
	fi
}

zxfer_unregister_owned_lock_path() {
	l_lock_path=$1
	l_remaining_paths=""

	[ -n "$l_lock_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_lock_path" ] && continue
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_existing_path
		else
			l_remaining_paths=$l_existing_path
		fi
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	g_zxfer_owned_lock_cleanup_paths=$l_remaining_paths
}

zxfer_normalize_owned_lock_cleanup_path() {
	l_lock_path=$1

	[ -n "$l_lock_path" ] || return 1
	case "$l_lock_path" in
	/*) ;;
	*)
		printf '%s\n' "$l_lock_path"
		return 0
		;;
	esac

	if ! l_parent_dir=$(zxfer_get_path_parent_dir "$l_lock_path"); then
		return 1
	fi
	if [ "$l_parent_dir" = "/" ]; then
		l_physical_parent=/
	else
		if ! l_physical_parent=$(CDPATH='' cd -P "$l_parent_dir" 2>/dev/null && pwd); then
			return 1
		fi
	fi
	l_lock_name=${l_lock_path##*/}
	if [ "$l_physical_parent" = "/" ]; then
		printf '/%s\n' "$l_lock_name"
		return 0
	fi
	printf '%s/%s\n' "$l_physical_parent" "$l_lock_name"
}

zxfer_owned_lock_cleanup_conflicts_with_path() {
	l_cleanup_path=$1

	[ -n "$l_cleanup_path" ] || return 1
	if ! l_cleanup_path=$(zxfer_normalize_owned_lock_cleanup_path "$l_cleanup_path"); then
		return 1
	fi

	while IFS= read -r l_lock_path || [ -n "$l_lock_path" ]; do
		[ -n "$l_lock_path" ] || continue
		if ! l_lock_path=$(zxfer_normalize_owned_lock_cleanup_path "$l_lock_path"); then
			continue
		fi
		case "$l_cleanup_path" in
		"$l_lock_path" | "$l_lock_path"/*)
			return 0
			;;
		esac
		case "$l_lock_path" in
		"$l_cleanup_path" | "$l_cleanup_path"/*)
			return 0
			;;
		esac
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	return 1
}

zxfer_warn_owned_lock_cleanup_failure() {
	l_lock_path=$1
	l_status=$2

	if command -v zxfer_warn_stderr >/dev/null 2>&1; then
		zxfer_warn_stderr "zxfer: warning: unable to release owned lock or lease \"$l_lock_path\" during cleanup (status $l_status)."
	else
		printf '%s\n' "zxfer: warning: unable to release owned lock or lease \"$l_lock_path\" during cleanup (status $l_status)." >&2
	fi
}

zxfer_release_registered_owned_locks() {
	l_remaining_paths=""
	l_cleanup_status=0

	while IFS= read -r l_lock_path || [ -n "$l_lock_path" ]; do
		[ -n "$l_lock_path" ] || continue
		zxfer_release_owned_lock_dir "$l_lock_path"
		l_release_status=$?
		if [ "$l_release_status" -eq 0 ]; then
			continue
		fi
		zxfer_warn_owned_lock_cleanup_failure "$l_lock_path" "$l_release_status"
		l_cleanup_status=1
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_lock_path
		else
			l_remaining_paths=$l_lock_path
		fi
	done <<EOF
${g_zxfer_owned_lock_cleanup_paths:-}
EOF

	g_zxfer_owned_lock_cleanup_paths=$l_remaining_paths
	return "$l_cleanup_status"
}

zxfer_get_owned_lock_metadata_path() {
	l_lock_dir=$1
	printf '%s/metadata\n' "$l_lock_dir"
}

zxfer_validate_owned_lock_kind() {
	case "$1" in
	lock | lease)
		return 0
		;;
	esac

	return 1
}

zxfer_validate_owned_lock_text_field() {
	l_field_value=$1
	l_tab=$(printf '\t')
	l_lf='
'

	case "$l_field_value" in
	'' | *"$l_tab"* | *"$l_lf"*)
		return 1
		;;
	esac

	return 0
}

zxfer_normalize_owned_lock_text_field() {
	l_field_value=$1

	l_normalized_value=$(printf '%s\n' "$l_field_value" |
		tr '\t' ' ' | tr -s ' ' | sed 's/^ *//; s/ *$//')
	if ! zxfer_validate_owned_lock_text_field "$l_normalized_value"; then
		return 1
	fi

	printf '%s\n' "$l_normalized_value"
}

zxfer_get_owned_lock_hostname() {
	l_hostname=$(uname -n 2>/dev/null || hostname 2>/dev/null || printf '%s\n' unknown)
	if ! l_hostname=$(zxfer_normalize_owned_lock_text_field "$l_hostname"); then
		return 1
	fi
	printf '%s\n' "$l_hostname"
}

zxfer_get_owned_lock_created_at() {
	l_created_at=$(date '+%Y-%m-%dT%H:%M:%S%z' 2>/dev/null || :)
	if ! zxfer_validate_owned_lock_text_field "$l_created_at"; then
		return 1
	fi
	printf '%s\n' "$l_created_at"
}

zxfer_get_process_start_token_from_procfs() {
	l_pid=$1

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	[ -r "/proc/$l_pid/stat" ] || return 1
	l_proc_stat=$(cat "/proc/$l_pid/stat" 2>/dev/null || :)
	[ -n "$l_proc_stat" ] || return 1
	l_proc_rest=$(printf '%s\n' "$l_proc_stat" | sed 's/^[0-9][0-9]* (.*) //')
	[ "$l_proc_rest" != "$l_proc_stat" ] || return 1
	l_proc_start=$(printf '%s\n' "$l_proc_rest" | awk '{ print $20 }')
	case "$l_proc_start" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	l_boot_identity=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || :)
	if ! l_boot_identity=$(zxfer_normalize_owned_lock_text_field "$l_boot_identity"); then
		l_boot_identity=$(LC_ALL=C awk '/^btime / { print $2; exit }' /proc/stat 2>/dev/null || :)
		if ! l_boot_identity=$(zxfer_normalize_owned_lock_text_field "$l_boot_identity"); then
			l_boot_identity=unknown
		fi
	fi

	printf 'procfs:%s:%s\n' "$l_boot_identity" "$l_proc_start"
}

#
# Prefer `lstart`, then shorter `start` / `stime` fallbacks for supported
# platforms that do not expose the long-form start time selector.
#
zxfer_get_process_start_token() {
	l_pid=$1

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	for l_field in lstart start stime; do
		l_raw_token=$(LC_ALL=C ps -p "$l_pid" -o "$l_field=" 2>/dev/null || :)
		[ -n "$l_raw_token" ] || continue
		if ! l_normalized_token=$(zxfer_normalize_owned_lock_text_field "$l_raw_token"); then
			continue
		fi
		printf '%s:%s\n' "$l_field" "$l_normalized_token"
		return 0
	done

	if l_procfs_token=$(zxfer_get_process_start_token_from_procfs "$l_pid"); then
		printf '%s\n' "$l_procfs_token"
		return 0
	fi

	return 1
}

zxfer_validate_owned_lock_container_dir() {
	l_dir_path=$1

	[ -d "$l_dir_path" ] || return 1
	[ ! -L "$l_dir_path" ] || return 1
	[ ! -h "$l_dir_path" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_dir_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_dir_path"); then
		return 1
	fi
	[ "$l_mode" = "700" ] || return 1
}

zxfer_validate_owned_lock_metadata_file() {
	l_metadata_path=$1

	[ -f "$l_metadata_path" ] || return 1
	[ ! -L "$l_metadata_path" ] || return 1
	[ ! -h "$l_metadata_path" ] || return 1
	if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
		return 1
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_metadata_path"); then
		return 1
	fi
	[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	if ! l_mode=$(zxfer_get_path_mode_octal "$l_metadata_path"); then
		return 1
	fi
	[ "$l_mode" = "600" ] || return 1
}

zxfer_write_owned_lock_metadata_file() {
	l_lock_dir=$1
	l_kind=$2
	l_purpose=$3
	l_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_lock_dir")

	if ! zxfer_validate_owned_lock_kind "$l_kind"; then
		return 1
	fi
	if ! l_purpose=$(zxfer_normalize_owned_lock_text_field "$l_purpose"); then
		return 1
	fi
	if ! l_start_token=$(zxfer_get_process_start_token "$$"); then
		return 1
	fi
	if ! l_hostname=$(zxfer_get_owned_lock_hostname); then
		return 1
	fi
	if ! l_created_at=$(zxfer_get_owned_lock_created_at); then
		return 1
	fi
	if ! l_tmp_metadata_path=$(mktemp "$l_lock_dir/.metadata.XXXXXX" 2>/dev/null); then
		return 1
	fi
	if ! touch "$l_tmp_metadata_path" 2>/dev/null; then
		rm -f "$l_tmp_metadata_path" 2>/dev/null || :
		return 1
	fi

	# Write through a child shell so open-time redirection failures on the staged
	# file return a normal nonzero status instead of leaking past the parent.
	if ! /bin/sh -c 'cat >"$1"' sh "$l_tmp_metadata_path" 2>/dev/null <<EOF; then
$ZXFER_LOCK_METADATA_HEADER
kind	$l_kind
purpose	$l_purpose
pid	$$
start_token	$l_start_token
hostname	$l_hostname
created_at	$l_created_at
EOF
		rm -f "$l_tmp_metadata_path" 2>/dev/null || :
		return 1
	fi
	chmod 600 "$l_tmp_metadata_path" 2>/dev/null || :
	if ! mv -f "$l_tmp_metadata_path" "$l_metadata_path" 2>/dev/null; then
		rm -f "$l_tmp_metadata_path" 2>/dev/null || :
		return 1
	fi
	chmod 600 "$l_metadata_path" 2>/dev/null || :
	return 0
}

zxfer_parse_owned_lock_metadata_file() {
	l_metadata_path=$1
	l_tab=$(printf '\t')
	l_line_number=0

	zxfer_reset_owned_lock_metadata_result

	while IFS= read -r l_line || [ -n "$l_line" ]; do
		l_line_number=$((l_line_number + 1))
		case "$l_line_number" in
		1)
			[ "$l_line" = "$ZXFER_LOCK_METADATA_HEADER" ] || return 1
			;;
		2 | 3 | 4 | 5 | 6 | 7)
			case "$l_line" in
			*"$l_tab"*)
				l_key=${l_line%%"$l_tab"*}
				l_value=${l_line#*"$l_tab"}
				[ "$l_value" != "$l_line" ] || return 1
				;;
			*)
				return 1
				;;
			esac
			case "$l_value" in
			*"$l_tab"*)
				return 1
				;;
			esac

			case "$l_line_number:$l_key" in
			2:kind)
				zxfer_validate_owned_lock_kind "$l_value" || return 1
				g_zxfer_owned_lock_kind_result=$l_value
				;;
			3:purpose)
				zxfer_validate_owned_lock_text_field "$l_value" || return 1
				g_zxfer_owned_lock_purpose_result=$l_value
				;;
			4:pid)
				case "$l_value" in
				'' | *[!0-9]*)
					return 1
					;;
				esac
				g_zxfer_owned_lock_pid_result=$l_value
				;;
			5:start_token)
				zxfer_validate_owned_lock_text_field "$l_value" || return 1
				g_zxfer_owned_lock_start_token_result=$l_value
				;;
			6:hostname)
				zxfer_validate_owned_lock_text_field "$l_value" || return 1
				g_zxfer_owned_lock_hostname_result=$l_value
				;;
			7:created_at)
				zxfer_validate_owned_lock_text_field "$l_value" || return 1
				g_zxfer_owned_lock_created_at_result=$l_value
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
	done <"$l_metadata_path"

	[ "$l_line_number" -eq 7 ] || return 1
	[ -n "$g_zxfer_owned_lock_kind_result" ] || return 1
	[ -n "$g_zxfer_owned_lock_purpose_result" ] || return 1
	[ -n "$g_zxfer_owned_lock_pid_result" ] || return 1
	[ -n "$g_zxfer_owned_lock_start_token_result" ] || return 1
	[ -n "$g_zxfer_owned_lock_hostname_result" ] || return 1
	[ -n "$g_zxfer_owned_lock_created_at_result" ] || return 1
	return 0
}

#
# Return codes:
# 0 = secure directory plus valid metadata loaded
# 1 = hard validation failure
# 2 = corrupt or missing metadata
#
zxfer_load_owned_lock_metadata_from_dir() {
	l_lock_dir=$1
	l_metadata_path=$(zxfer_get_owned_lock_metadata_path "$l_lock_dir")

	zxfer_reset_owned_lock_metadata_result

	if ! zxfer_validate_owned_lock_container_dir "$l_lock_dir"; then
		return 1
	fi
	if [ ! -e "$l_metadata_path" ]; then
		return 2
	fi
	if ! zxfer_validate_owned_lock_metadata_file "$l_metadata_path"; then
		return 1
	fi
	if ! zxfer_parse_owned_lock_metadata_file "$l_metadata_path"; then
		return 2
	fi
	return 0
}

zxfer_load_owned_lock_metadata_for_kind_and_purpose() {
	l_lock_dir=$1
	l_kind=$2
	l_purpose=$3

	if ! l_purpose=$(zxfer_normalize_owned_lock_text_field "$l_purpose"); then
		return 1
	fi
	zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir"
	l_load_status=$?
	case "$l_load_status" in
	0)
		[ "$g_zxfer_owned_lock_kind_result" = "$l_kind" ] || return 2
		[ "$g_zxfer_owned_lock_purpose_result" = "$l_purpose" ] || return 2
		return 0
		;;
	esac
	return "$l_load_status"
}

#
# Return codes:
# 0 = owner is still live
# 1 = owner is stale
# 2 = owner liveness could not be determined safely
#
zxfer_owned_lock_owner_is_live() {
	l_pid=$1
	l_start_token=$2
	l_hostname=$3

	if ! l_current_hostname=$(zxfer_get_owned_lock_hostname); then
		return 2
	fi
	if [ "$l_hostname" != "$l_current_hostname" ]; then
		return 1
	fi
	if ! kill -s 0 "$l_pid" 2>/dev/null; then
		return 1
	fi
	if ! l_current_start_token=$(zxfer_get_process_start_token "$l_pid"); then
		return 2
	fi
	if [ "$l_current_start_token" = "$l_start_token" ]; then
		return 0
	fi
	return 1
}

zxfer_cleanup_owned_lock_dir() {
	l_lock_dir=$1

	[ -n "$l_lock_dir" ] || return 0
	if [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; then
		return 0
	fi
	[ ! -L "$l_lock_dir" ] || return 1
	[ ! -h "$l_lock_dir" ] || return 1
	[ -d "$l_lock_dir" ] || return 1
	if rm -rf "$l_lock_dir" 2>/dev/null ||
		{ [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; }; then
		return 0
	fi
	return 1
}

zxfer_create_owned_lock_dir() {
	l_lock_dir=$1
	l_kind=$2
	l_purpose=$3
	l_old_umask=$(umask)

	[ -n "$l_lock_dir" ] || return 1
	if ! zxfer_validate_owned_lock_kind "$l_kind"; then
		return 1
	fi
	if ! l_purpose=$(zxfer_normalize_owned_lock_text_field "$l_purpose"); then
		return 1
	fi

	umask 077
	if ! mkdir "$l_lock_dir" 2>/dev/null; then
		umask "$l_old_umask"
		return 1
	fi
	umask "$l_old_umask"

	if ! zxfer_validate_owned_lock_container_dir "$l_lock_dir"; then
		zxfer_cleanup_owned_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi
	if ! zxfer_write_owned_lock_metadata_file "$l_lock_dir" "$l_kind" "$l_purpose"; then
		zxfer_cleanup_owned_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi

	printf '%s\n' "$l_lock_dir"
	return 0
}

zxfer_create_owned_lock_dir_in_parent() {
	l_parent_dir=$1
	l_prefix=$2
	l_kind=$3
	l_purpose=$4

	[ -n "$l_prefix" ] || return 1
	if ! zxfer_validate_owned_lock_container_dir "$l_parent_dir"; then
		return 1
	fi
	if ! l_lock_dir=$(mktemp -d "$l_parent_dir/$l_prefix.XXXXXX" 2>/dev/null); then
		return 1
	fi
	if ! zxfer_validate_owned_lock_container_dir "$l_lock_dir"; then
		zxfer_cleanup_owned_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi
	if ! zxfer_write_owned_lock_metadata_file "$l_lock_dir" "$l_kind" "$l_purpose"; then
		zxfer_cleanup_owned_lock_dir "$l_lock_dir" >/dev/null 2>&1 || :
		return 1
	fi

	printf '%s\n' "$l_lock_dir"
	return 0
}

#
# Return codes:
# 0 = stale or corrupt entry was reaped
# 1 = hard failure
# 2 = entry is still busy or not yet reapable under the caller policy
#
zxfer_try_reap_stale_owned_lock_dir() {
	l_lock_dir=$1
	l_allow_corrupt_reap=${2:-0}
	l_kind=${3:-}
	l_purpose=${4:-}

	if [ -n "$l_kind" ] || [ -n "$l_purpose" ]; then
		zxfer_load_owned_lock_metadata_for_kind_and_purpose \
			"$l_lock_dir" "$l_kind" "$l_purpose"
	else
		zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir"
	fi
	l_load_status=$?
	case "$l_load_status" in
	0)
		zxfer_owned_lock_owner_is_live \
			"$g_zxfer_owned_lock_pid_result" \
			"$g_zxfer_owned_lock_start_token_result" \
			"$g_zxfer_owned_lock_hostname_result"
		l_live_status=$?
		if [ "$l_live_status" -eq 0 ]; then
			return 2
		fi
		if [ "$l_live_status" -eq 2 ]; then
			return 1
		fi
		;;
	1)
		return 1
		;;
	2)
		case "$l_allow_corrupt_reap" in
		1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn])
			:
			;;
		*)
			return 2
			;;
		esac
		;;
	*)
		return 1
		;;
	esac

	if ! zxfer_cleanup_owned_lock_dir "$l_lock_dir"; then
		return 1
	fi
	return 0
}

zxfer_current_process_owns_owned_lock_dir() {
	l_lock_dir=$1
	l_kind=${2:-}
	l_purpose=${3:-}

	if [ -n "$l_kind" ] || [ -n "$l_purpose" ]; then
		if ! zxfer_load_owned_lock_metadata_for_kind_and_purpose \
			"$l_lock_dir" "$l_kind" "$l_purpose"; then
			return 1
		fi
	elif ! zxfer_load_owned_lock_metadata_from_dir "$l_lock_dir"; then
		return 1
	fi
	[ "$g_zxfer_owned_lock_pid_result" = "$$" ] || return 1
	if ! l_current_hostname=$(zxfer_get_owned_lock_hostname); then
		return 1
	fi
	[ "$g_zxfer_owned_lock_hostname_result" = "$l_current_hostname" ] || return 1
	if ! l_current_start_token=$(zxfer_get_process_start_token "$$"); then
		return 1
	fi
	[ "$g_zxfer_owned_lock_start_token_result" = "$l_current_start_token" ]
}

zxfer_release_owned_lock_dir() {
	l_lock_dir=$1
	l_kind=${2:-}
	l_purpose=${3:-}

	[ -n "$l_lock_dir" ] || return 0
	if [ ! -e "$l_lock_dir" ] && [ ! -L "$l_lock_dir" ] && [ ! -h "$l_lock_dir" ]; then
		zxfer_unregister_owned_lock_path "$l_lock_dir"
		return 0
	fi
	if ! zxfer_current_process_owns_owned_lock_dir \
		"$l_lock_dir" "$l_kind" "$l_purpose"; then
		return 1
	fi
	if ! zxfer_cleanup_owned_lock_dir "$l_lock_dir"; then
		return 1
	fi
	zxfer_unregister_owned_lock_path "$l_lock_dir"
	return 0
}
