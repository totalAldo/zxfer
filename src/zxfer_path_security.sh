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

# Module contract:
# owns globals: secure-staging result scratch.
# reads globals: g_cmd_awk.
# mutates caches: none.
# returns via stdout: owner/mode probes, symlink probes, and validated temp-root paths.

# Purpose: Return the path owner UID in the form expected by later helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
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

# Purpose: Return the path mode octal in the form expected by later helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
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

# Purpose: Return the effective user UID in the form expected by later helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_effective_user_uid() {
	if command -v id >/dev/null 2>&1; then
		if l_uid=$(id -u 2>/dev/null); then
			printf '%s\n' "$l_uid"
			return 0
		fi
	fi
	return 1
}

# Purpose: Check whether the backup owner UID is allowed.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers need a boolean answer about the backup owner UID.
zxfer_backup_owner_uid_is_allowed() {
	l_owner_uid=$1

	if [ "$l_owner_uid" = "0" ]; then
		return 0
	fi

	if l_effective_uid=$(zxfer_get_effective_user_uid); then
		if [ "$l_owner_uid" = "$l_effective_uid" ]; then
			return 0
		fi
	fi

	return 1
}

# Purpose: Describe the expected backup owner in operator-facing text.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# validation or reporting logic needs one canonical explanation string.
zxfer_describe_expected_backup_owner() {
	l_desc="root (UID 0)"

	if l_effective_uid=$(zxfer_get_effective_user_uid); then
		if [ "$l_effective_uid" != "0" ]; then
			l_desc="$l_desc or UID $l_effective_uid"
		fi
	fi

	printf '%s\n' "$l_desc"
}

# Purpose: Require the secure backup file before the surrounding flow
# continues.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers should stop immediately if the precondition is not met.
zxfer_require_secure_backup_file() {
	l_path=$1
	l_display_path=${2:-$l_path}

	if ! l_error=$(zxfer_check_secure_backup_file "$l_path" "$l_display_path"); then
		zxfer_throw_error "$l_error"
	fi
}

# Purpose: Reject the backup metadata path with the validation failure owned by
# this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when a
# path or input should fail closed with one consistent error path.
zxfer_reject_backup_metadata_path() {
	l_msg=$1

	printf '%s\n' "$l_msg" >&2
	return 1
}

# Purpose: Require the backup metadata path without symlinks before the
# surrounding flow continues.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers should stop immediately if the precondition is not met.
zxfer_require_backup_metadata_path_without_symlinks() {
	l_path=$1

	if l_symlink_component=$(zxfer_find_symlink_path_component "$l_path"); then
		if [ "$l_symlink_component" = "$l_path" ]; then
			zxfer_reject_backup_metadata_path "Refusing to use backup metadata $l_path because it is a symlink."
		fi
		zxfer_reject_backup_metadata_path "Refusing to use backup metadata $l_path because path component $l_symlink_component is a symlink."
	fi
}

# Purpose: Find the symlink path component in the tracked state owned by this
# module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers need an existing record instead of rebuilding one.
zxfer_find_symlink_path_component() {
	l_path=$1

	[ -n "$l_path" ] || return 1

	l_remaining=$l_path
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

		if [ -L "$l_candidate_path" ] || [ -h "$l_candidate_path" ]; then
			if zxfer_is_trusted_symlink_path_component "$l_candidate_path"; then
				continue
			fi
			printf '%s\n' "$l_candidate_path"
			return 0
		fi
	done

	return 1
}

# Purpose: Check whether the symlink path component is trusted.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# later helpers need a boolean answer about a validated or trusted state.
zxfer_is_trusted_symlink_path_component() {
	l_path=$1

	case "$l_path" in
	/*) ;;
	*)
		return 1
		;;
	esac
	[ -L "$l_path" ] || [ -h "$l_path" ] || return 1

	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_path" 2>/dev/null); then
		return 1
	fi
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
	if ! l_parent_owner_uid=$(zxfer_get_path_owner_uid "$l_parent" 2>/dev/null); then
		return 1
	fi
	[ "$l_parent_owner_uid" = "0" ] || return 1
	[ "$l_parent" = "/" ] || return 1

	l_ls_path=$l_parent
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	if ! l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null); then
		return 1
	fi
	# shellcheck disable=SC2016
	# awk needs literal $1.
	l_perm_str=$(printf '%s\n' "$l_ls_output" | ${g_cmd_awk:-awk} '{print $1}')
	case "$l_perm_str" in
	??????????*)
		:
		;;
	*)
		return 1
		;;
	esac
	l_group_write=$(printf '%s' "$l_perm_str" | cut -c 6)
	l_other_write=$(printf '%s' "$l_perm_str" | cut -c 9)
	l_sticky_char=$(printf '%s' "$l_perm_str" | cut -c 10)
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

# Purpose: Validate the temp root candidate before zxfer relies on it.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths to fail
# closed on malformed, unsafe, or stale input.
zxfer_validate_temp_root_candidate() {
	l_candidate=$1

	[ -n "$l_candidate" ] || return 1
	case "$l_candidate" in
	/*) ;;
	*)
		return 1
		;;
	esac

	if ! l_physical_dir=$(CDPATH='' cd -P "$l_candidate" 2>/dev/null && pwd); then
		return 1
	fi
	case "$l_physical_dir" in
	/*) ;;
	*)
		return 1
		;;
	esac
	[ -d "$l_physical_dir" ] || return 1

	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_physical_dir"); then
		return 1
	fi
	if [ "$l_owner_uid" != "0" ]; then
		if ! l_effective_uid=$(zxfer_get_effective_user_uid); then
			return 1
		fi
		[ "$l_owner_uid" = "$l_effective_uid" ] || return 1
	fi
	l_ls_path=$l_physical_dir
	case "$l_ls_path" in
	-*)
		l_ls_path=./$l_ls_path
		;;
	esac
	if ! l_ls_output=$(ls -ldn "$l_ls_path" 2>/dev/null); then
		return 1
	fi
	# shellcheck disable=SC2016
	# awk needs literal $1.
	l_perm_str=$(printf '%s\n' "$l_ls_output" | ${g_cmd_awk:-awk} '{print $1}')
	case "$l_perm_str" in
	??????????*)
		:
		;;
	*)
		return 1
		;;
	esac
	l_group_write=$(printf '%s' "$l_perm_str" | cut -c 6)
	l_other_write=$(printf '%s' "$l_perm_str" | cut -c 9)
	l_sticky_char=$(printf '%s' "$l_perm_str" | cut -c 10)
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

	printf '%s\n' "$l_physical_dir"
}

# Purpose: Return the path parent directory in the form expected by later
# helpers.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_path_parent_dir() {
	l_path=$1

	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ] || [ "$l_parent" = "" ]; then
		l_parent=/
	fi

	printf '%s\n' "$l_parent"
}

# Purpose: Create the secure staging directory for path using the safety checks
# owned by this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths when
# zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_secure_staging_dir_for_path() {
	l_path=$1
	l_prefix=${2:-zxfer.stage}

	g_zxfer_secure_staging_dir_result=""
	if ! l_parent=$(zxfer_get_path_parent_dir "$l_path"); then
		return 1
	fi
	if ! l_parent=$(zxfer_validate_temp_root_candidate "$l_parent"); then
		return 1
	fi

	l_old_umask=$(umask)
	umask 077
	l_stage_dir=$(mktemp -d "$l_parent/.$l_prefix.XXXXXX" 2>/dev/null)
	l_stage_status=$?
	umask "$l_old_umask"
	[ $l_stage_status -eq 0 ] || return 1

	# Register same-directory staging so trap cleanup can reap it on aborts.
	if command -v zxfer_register_runtime_artifact_path >/dev/null 2>&1; then
		zxfer_register_runtime_artifact_path "$l_stage_dir"
	fi

	g_zxfer_secure_staging_dir_result=$l_stage_dir
	printf '%s\n' "$l_stage_dir"
}

# Purpose: Check the secure backup file using the fail-closed rules owned by
# this module.
# Usage: Called before zxfer trusts temp-root or backup-metadata paths before
# later helpers act on a result that must be validated first.
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
