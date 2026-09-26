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
# PATH SECURITY / OWNER / MODE VALIDATION
################################################################################

# Module contract:
# owns globals: none.
# reads globals: none.
# mutates caches: none.
# returns via stdout: owner/mode/identity probes, symlink paths and validated
#   temp roots. The backup-metadata symlink refusal goes to stderr.
#
# Temp roots validate once per run via the single-pass physical resolution in
# zxfer_validate_temp_root_candidate; the component-walk symlink scanner only
# serves the cold backup-metadata and ZXFER_ERROR_LOG checks whose
# "path component ... is a symlink" errors are pinned public output.

# Purpose: Print the numeric owner UID of an existing path.
# Usage: zxfer_get_path_owner_uid PATH; tries GNU stat, BSD stat, then
# ls -ldn, and returns 1 when none of them yields a number.
zxfer_get_path_owner_uid() {
	l_path=$1

	if [ ! -e "$l_path" ]; then
		return 1
	fi

	if command -v stat >/dev/null 2>&1; then
		if l_uid=$(stat -c '%u' "$l_path" 2>/dev/null); then
			case "$l_uid" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_uid"
				return 0
				;;
			esac
		fi
		if l_uid=$(stat -f '%u' "$l_path" 2>/dev/null); then
			case "$l_uid" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_uid"
				return 0
				;;
			esac
		fi
	fi

	l_ls_path=$l_path
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null) || return 1
	# Field 3 of ls -ldn is the numeric owner.
	IFS=' 	' read -r l_ls_perm l_ls_links l_uid l_ls_rest <<EOF
$l_ls_output
EOF
	case "$l_uid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	printf '%s\n' "$l_uid"
}

# Purpose: Print the octal permission bits of an existing path.
# Usage: zxfer_get_path_mode_octal PATH; tries GNU stat, BSD stat, then
# ls -ldn (which knows only 600 and 700), and returns 1 when none answers.
zxfer_get_path_mode_octal() {
	l_path=$1

	if [ ! -e "$l_path" ]; then
		return 1
	fi

	if command -v stat >/dev/null 2>&1; then
		if l_mode=$(stat -c '%a' "$l_path" 2>/dev/null); then
			case "$l_mode" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_mode"
				return 0
				;;
			esac
		fi
		if l_mode=$(stat -f '%OLp' "$l_path" 2>/dev/null); then
			case "$l_mode" in
			'' | *[!0-9]*) ;;
			*)
				printf '%s\n' "$l_mode"
				return 0
				;;
			esac
		fi
	fi

	l_ls_path=$l_path
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	if l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null); then
		l_perm_str=${l_ls_output%% *}
		if [ "$l_perm_str" = "-rw-------" ]; then
			printf '600\n'
			return 0
		fi
		if [ "$l_perm_str" = "drwx------" ]; then
			printf '700\n'
			return 0
		fi
	fi

	return 1
}

# Purpose: Print the filesystem identity of one existing, non-symlink path.
# Usage: zxfer_get_path_device_inode PATH; prints device-inode:DEV:INO from
# stat, or inode:INO from ls -din on systems without stat. Runtime compares
# it before recursive cleanup so a replaced pathname is never adopted; the
# inode-only form suffices because the registered path's parent is fixed.
zxfer_get_path_device_inode() {
	l_identity_path=$1

	[ -e "$l_identity_path" ] || return 1
	[ ! -L "$l_identity_path" ] || return 1
	if command -v stat >/dev/null 2>&1; then
		if l_identity_value=$(stat -c '%d:%i' "$l_identity_path" 2>/dev/null); then
			case "$l_identity_value" in
			'' | *[!0-9:]*) ;;
			*)
				printf 'device-inode:%s\n' "$l_identity_value"
				return 0
				;;
			esac
		fi
		if l_identity_value=$(stat -f '%d:%i' "$l_identity_path" 2>/dev/null); then
			case "$l_identity_value" in
			'' | *[!0-9:]*) ;;
			*)
				printf 'device-inode:%s\n' "$l_identity_value"
				return 0
				;;
			esac
		fi
	fi

	l_identity_ls_path=$l_identity_path
	case "$l_identity_ls_path" in
	-*) l_identity_ls_path=./$l_identity_ls_path ;;
	esac
	l_identity_output=$(ls -din "$l_identity_ls_path" 2>/dev/null) || return 1
	# Field 1 of ls -din is the inode number.
	IFS=' 	' read -r l_identity_value l_identity_rest <<EOF
$l_identity_output
EOF
	case "$l_identity_value" in
	'' | *[!0-9]*) return 1 ;;
	esac
	printf 'inode:%s\n' "$l_identity_value"
}

# Purpose: Print one directory's identity, owner UID and mode from a single
# stat snapshot.
# Usage: zxfer_get_private_directory_security_record DIR; prints
# IDENTITY<TAB>UID<TAB>MODE. Runtime records it when it creates the run root
# and compares a fresh one right before removing the root.
zxfer_get_private_directory_security_record() {
	l_private_record_path=$1
	l_private_record_raw=""

	[ -d "$l_private_record_path" ] || return 1
	[ ! -L "$l_private_record_path" ] || return 1
	if command -v stat >/dev/null 2>&1; then
		l_private_record_raw=$(stat -c '%d:%i:%u:%a' \
			"$l_private_record_path" 2>/dev/null) || l_private_record_raw=""
		if [ -z "$l_private_record_raw" ]; then
			l_private_record_raw=$(stat -f '%d:%i:%u:%OLp' \
				"$l_private_record_path" 2>/dev/null) || l_private_record_raw=""
		fi
	fi
	if [ -n "$l_private_record_raw" ]; then
		IFS=: read -r l_private_record_device l_private_record_inode \
			l_private_record_uid l_private_record_mode \
			l_private_record_extra <<-EOF
				$l_private_record_raw
			EOF
		case "$l_private_record_device$l_private_record_inode$l_private_record_uid$l_private_record_mode" in
		'' | *[!0-9]*) return 1 ;;
		esac
		[ -z "$l_private_record_extra" ] || return 1
		printf 'device-inode:%s:%s\t%s\t%s\n' \
			"$l_private_record_device" "$l_private_record_inode" \
			"$l_private_record_uid" "$l_private_record_mode"
		return 0
	fi

	# Legacy systems without formatted stat retain the established fail-closed
	# probes. This path is cold and preserves the inode-only compatibility
	# fallback used before the consolidated record was introduced.
	l_private_record_identity=$(zxfer_get_path_device_inode \
		"$l_private_record_path") || return 1
	l_private_record_uid=$(zxfer_get_path_owner_uid \
		"$l_private_record_path") || return 1
	l_private_record_mode=$(zxfer_get_path_mode_octal \
		"$l_private_record_path") || return 1
	printf '%s\t%s\t%s\n' "$l_private_record_identity" \
		"$l_private_record_uid" "$l_private_record_mode"
}

# Purpose: Check that one ls -l permission string describes a directory that is
# safe to share: well-formed and either free of group/other write bits or
# protected by the sticky bit.
# Usage: zxfer_validate_shared_dir_permission_string PERMS, from the temp-root
# and trusted-symlink validators; parameter expansion only, no spawns.
zxfer_validate_shared_dir_permission_string() {
	l_perm_str=$1

	case "$l_perm_str" in
	??????????*) ;;
	*)
		return 1
		;;
	esac
	# Single-character slices via parameter expansion: strip N leading
	# characters, then keep only the first character of the remainder.
	l_perm_tail=${l_perm_str#?????}
	l_group_write=${l_perm_tail%"${l_perm_tail#?}"}
	l_perm_tail=${l_perm_str#????????}
	l_other_write=${l_perm_tail%"${l_perm_tail#?}"}
	l_perm_tail=${l_perm_str#?????????}
	l_sticky_char=${l_perm_tail%"${l_perm_tail#?}"}
	case "$l_group_write$l_other_write" in
	*w*)
		case "$l_sticky_char" in
		t | T) ;;
		*)
			return 1
			;;
		esac
		;;
	esac

	return 0
}

# Purpose: Print the effective user UID.
# Usage: zxfer_get_effective_user_uid; returns 1 when id is missing or fails.
zxfer_get_effective_user_uid() {
	if command -v id >/dev/null 2>&1; then
		if l_uid=$(id -u 2>/dev/null); then
			printf '%s\n' "$l_uid"
			return 0
		fi
	fi
	return 1
}

# Purpose: Check that a backup path owner is root or the effective user.
# Usage: zxfer_backup_owner_uid_is_allowed UID; returns 1 otherwise.
zxfer_backup_owner_uid_is_allowed() {
	l_backup_owner_uid=$1

	if [ "$l_backup_owner_uid" = "0" ]; then
		return 0
	fi

	if l_backup_effective_uid=$(zxfer_get_effective_user_uid); then
		if [ "$l_backup_owner_uid" = "$l_backup_effective_uid" ]; then
			return 0
		fi
	fi

	return 1
}

# Purpose: Print the allowed backup owners for an operator message.
# Usage: zxfer_describe_expected_backup_owner; prints "root (UID 0)" plus
# " or UID N" when the effective user is not root.
zxfer_describe_expected_backup_owner() {
	l_desc="root (UID 0)"

	if l_effective_uid=$(zxfer_get_effective_user_uid); then
		if [ "$l_effective_uid" != "0" ]; then
			l_desc="$l_desc or UID $l_effective_uid"
		fi
	fi

	printf '%s\n' "$l_desc"
}

# Purpose: Refuse a backup metadata path that is, or passes through, an
# untrusted symlink.
# Usage: zxfer_require_backup_metadata_path_without_symlinks PATH; returns 0
# when no component is a symlink, else prints one refusal to stderr and
# returns 1.
zxfer_require_backup_metadata_path_without_symlinks() {
	l_backup_metadata_symlink=$(zxfer_find_symlink_path_component "$1") ||
		return 0
	if [ "$l_backup_metadata_symlink" = "$1" ]; then
		printf '%s\n' "Refusing to use backup metadata $1 because it is a symlink." >&2
	else
		printf '%s\n' "Refusing to use backup metadata $1 because path component $l_backup_metadata_symlink is a symlink." >&2
	fi
	return 1
}

# Purpose: Print the first component of a path that is an untrusted symlink.
# Usage: zxfer_find_symlink_path_component PATH; returns 1 when there is none.
# Root-owned top-level system symlinks (zxfer_is_trusted_symlink_path_component)
# are skipped.
zxfer_find_symlink_path_component() {
	l_find_symlink_path_component_path=$1

	[ -n "$l_find_symlink_path_component_path" ] || return 1

	l_remaining=$l_find_symlink_path_component_path
	l_candidate_path=""
	while [ -n "$l_remaining" ]; do
		case "$l_remaining" in
		/*)
			if [ "$l_candidate_path" = "" ]; then
				l_candidate_path="/"
				l_remaining=${l_remaining#/}
				continue
			fi
			;;
		esac

		l_component=${l_remaining%%/*}
		if [ "$l_component" = "$l_remaining" ]; then
			l_remaining=""
		else
			l_remaining=${l_remaining#*/}
		fi
		[ -n "$l_component" ] || continue

		case "$l_candidate_path" in
		"")
			l_candidate_path=$l_component
			;;
		/)
			l_candidate_path="/$l_component"
			;;
		*)
			l_candidate_path="$l_candidate_path/$l_component"
			;;
		esac

		if [ -L "$l_candidate_path" ]; then
			if zxfer_is_trusted_symlink_path_component "$l_candidate_path"; then
				continue
			fi
			printf '%s\n' "$l_candidate_path"
			return 0
		fi
	done

	return 1
}

# Purpose: Check that a symlink is a root-owned entry of a root-owned, safely
# permissioned /.
# Usage: zxfer_is_trusted_symlink_path_component PATH; returns 0 only for such
# a top-level system symlink (for example /tmp -> private/tmp on macOS).
zxfer_is_trusted_symlink_path_component() {
	l_path=$1

	case "$l_path" in
	/*) ;;
	*)
		return 1
		;;
	esac
	[ -L "$l_path" ] || return 1

	l_owner_uid=$(zxfer_get_path_owner_uid "$l_path" 2>/dev/null) || return 1
	[ "$l_owner_uid" = "0" ] || return 1

	case "$l_path" in
	*/*)
		l_parent=${l_path%/*}
		[ -n "$l_parent" ] || l_parent="/"
		;;
	*)
		return 1
		;;
	esac
	l_parent_owner_uid=$(zxfer_get_path_owner_uid "$l_parent" 2>/dev/null) || return 1
	[ "$l_parent_owner_uid" = "0" ] || return 1
	[ "$l_parent" = "/" ] || return 1

	l_ls_path=$l_parent
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null) || return 1
	zxfer_validate_shared_dir_permission_string "${l_ls_output%% *}"
}

# Purpose: Check that a directory is a safe parent for zxfer temp files and
# print its physical path.
# Usage: zxfer_validate_temp_root_candidate DIR; DIR must be absolute. The
# directory it resolves to (cd -P) must be owned by root or the effective
# user and must not be group- or world-writable unless it is sticky. Returns
# 1 when any check fails. zxfer_try_get_effective_tmpdir memoizes the result.
zxfer_validate_temp_root_candidate() {
	l_candidate=$1

	case "$l_candidate" in
	/*) ;;
	*)
		return 1
		;;
	esac

	l_physical_dir=$(CDPATH='' cd -P "$l_candidate" 2>/dev/null && pwd) || return 1
	case "$l_physical_dir" in
	/*) ;;
	*)
		return 1
		;;
	esac
	[ -d "$l_physical_dir" ] || return 1

	# One ls -ldn gives both the permission string and the numeric owner.
	l_ls_output=$(ls -ldn "$l_physical_dir" 2>/dev/null) || return 1
	IFS=' 	' read -r l_ls_perm l_ls_links l_owner_uid l_ls_rest <<EOF
$l_ls_output
EOF
	case "$l_owner_uid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	if [ "$l_owner_uid" != "0" ]; then
		l_effective_uid=$(zxfer_get_effective_user_uid) || return 1
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	fi
	zxfer_validate_shared_dir_permission_string "$l_ls_perm" || return 1

	printf '%s\n' "$l_physical_dir"
}

# Purpose: Print the parent directory of a path.
# Usage: zxfer_get_path_parent_dir PATH; prints / for a top-level or
# slash-free PATH.
zxfer_get_path_parent_dir() {
	l_path=$1

	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ] || [ "$l_parent" = "" ]; then
		l_parent=/
	fi

	printf '%s\n' "$l_parent"
}

# Purpose: Check that a backup metadata file is owned by root or the
# effective user and has mode 0600.
# Usage: zxfer_check_secure_backup_file PATH [DISPLAY_PATH]; prints the
# operator message and returns 1 when a check fails or cannot run.
zxfer_check_secure_backup_file() {
	l_check_path=$1
	l_check_display_path=${2:-$l_check_path}

	if ! l_check_owner_uid=$(zxfer_get_path_owner_uid "$l_check_path"); then
		printf '%s\n' "Cannot determine the owner of backup metadata $l_check_display_path."
		return 1
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_check_owner_uid"; then
		l_check_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		printf '%s\n' "Refusing to use backup metadata $l_check_display_path because it is owned by UID $l_check_owner_uid instead of $l_check_expected_owner_desc."
		return 1
	fi
	if ! l_check_mode=$(zxfer_get_path_mode_octal "$l_check_path"); then
		printf '%s\n' "Cannot determine the permissions for backup metadata $l_check_display_path."
		return 1
	fi
	if [ "$l_check_mode" != "600" ]; then
		printf '%s\n' "Refusing to use backup metadata $l_check_display_path because its permissions ($l_check_mode) are not 0600."
		return 1
	fi
}
