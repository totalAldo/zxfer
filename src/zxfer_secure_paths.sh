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
# SECURE PATH / OWNER / MODE HELPERS
################################################################################

get_path_owner_uid() {
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
	if l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null); then
		# shellcheck disable=SC2016
		# awk needs literal $3.
		l_uid=$(printf '%s\n' "$l_ls_output" | ${g_cmd_awk:-awk} '{print $3}')
		if [ "$l_uid" != "" ]; then
			printf '%s\n' "$l_uid"
			return 0
		fi
	fi

	return 1
}

get_path_mode_octal() {
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
		# shellcheck disable=SC2016
		# awk needs literal $1.
		l_perm_str=$(printf '%s\n' "$l_ls_output" | ${g_cmd_awk:-awk} '{print $1}')
		if [ "$l_perm_str" = "-rw-------" ]; then
			printf '600\n'
			return 0
		fi
	fi

	return 1
}

get_effective_user_uid() {
	if command -v id >/dev/null 2>&1; then
		if l_uid=$(id -u 2>/dev/null); then
			printf '%s\n' "$l_uid"
			return 0
		fi
	fi
	return 1
}

backup_owner_uid_is_allowed() {
	l_owner_uid=$1

	if [ "$l_owner_uid" = "0" ]; then
		return 0
	fi

	if l_effective_uid=$(get_effective_user_uid); then
		if [ "$l_owner_uid" = "$l_effective_uid" ]; then
			return 0
		fi
	fi

	return 1
}

describe_expected_backup_owner() {
	l_desc="root (UID 0)"

	if l_effective_uid=$(get_effective_user_uid); then
		if [ "$l_effective_uid" != "0" ]; then
			l_desc="$l_desc or UID $l_effective_uid"
		fi
	fi

	printf '%s\n' "$l_desc"
}

require_secure_backup_file() {
	l_path=$1

	if ! l_owner_uid=$(get_path_owner_uid "$l_path"); then
		throw_error "Cannot determine the owner of backup metadata $l_path."
	fi
	if ! backup_owner_uid_is_allowed "$l_owner_uid"; then
		l_expected_owner_desc=$(describe_expected_backup_owner)
		throw_error "Refusing to use backup metadata $l_path because it is owned by UID $l_owner_uid instead of $l_expected_owner_desc."
	fi
	if ! l_mode=$(get_path_mode_octal "$l_path"); then
		throw_error "Cannot determine the permissions for backup metadata $l_path."
	fi
	if [ "$l_mode" != "600" ]; then
		throw_error "Refusing to use backup metadata $l_path because its permissions ($l_mode) are not 0600."
	fi
}

zxfer_reject_backup_metadata_path() {
	l_msg=$1

	printf '%s\n' "$l_msg" >&2
	return 1
}

require_backup_metadata_path_without_symlinks() {
	l_path=$1

	if l_symlink_component=$(zxfer_find_symlink_path_component "$l_path"); then
		if [ "$l_symlink_component" = "$l_path" ]; then
			zxfer_reject_backup_metadata_path "Refusing to use backup metadata $l_path because it is a symlink."
		fi
		zxfer_reject_backup_metadata_path "Refusing to use backup metadata $l_path because path component $l_symlink_component is a symlink."
	fi
}
