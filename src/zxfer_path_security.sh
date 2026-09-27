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
# owns globals: the effective-UID memo g_zxfer_effective_uid; the results of
#   one metadata read (g_zxfer_path_inode_result,
#   g_zxfer_path_owner_uid_result, g_zxfer_path_mode_result and
#   g_zxfer_path_permissions_result); the private-directory record
#   g_zxfer_private_directory_record_result; and the validated temp root
#   g_zxfer_temp_root_candidate_result.
# reads globals: ZXFER_TAB/ZXFER_LF from zxfer_quoting.sh.
# mutates caches: g_zxfer_effective_uid, filled once per process.
# returns via stdout: the owner and mode probes and symlink paths. The
#   backup-metadata symlink refusal goes to stderr.
#
# Every owner, mode and inode read is one `ls -ldin` line, parsed by
# zxfer_parse_path_metadata_line. Temp roots validate once per run through
# the single-subshell check in zxfer_validate_temp_root_candidate; the
# component-walk symlink scanner only serves the cold backup-metadata and
# ZXFER_ERROR_LOG checks whose "path component ... is a symlink" errors are
# pinned public output.

# Purpose: Forget the effective-UID memo and every path-security result.
# Usage: Called by zxfer_reset_session_state, so an exported value can never
# stand in for the real effective UID.
zxfer_reset_path_security_state() {
	g_zxfer_effective_uid=""
	g_zxfer_path_inode_result=""
	g_zxfer_path_owner_uid_result=""
	g_zxfer_path_mode_result=""
	g_zxfer_path_permissions_result=""
	g_zxfer_private_directory_record_result=""
	g_zxfer_temp_root_candidate_result=""
}

# Purpose: Parse one `ls -ldin` line into the path metadata results.
# Usage: zxfer_parse_path_metadata_line LINE; publishes
# g_zxfer_path_inode_result, g_zxfer_path_owner_uid_result,
# g_zxfer_path_permissions_result (the mode string) and
# g_zxfer_path_mode_result (octal, as GNU `stat -c %a` prints it: 600, or
# 2700 when the set-group-ID bit is set), or returns 1 with all four empty.
# The line holds the fields POSIX gives `ls -l`, and GNU coreutils, FreeBSD,
# macOS, illumos and BusyBox print them alike. Only the first four are read:
# 1 the inode (-i; BusyBox and illumos pad it with blanks), 2 the mode string,
# 3 the link count and 4 the numeric owner (-n). The name comes last, so
# blanks, a leading - or a line feed in it cannot move them.
zxfer_parse_path_metadata_line() {
	g_zxfer_path_inode_result=""
	g_zxfer_path_owner_uid_result=""
	g_zxfer_path_mode_result=""
	g_zxfer_path_permissions_result=""
	IFS=' 	' read -r l_metadata_inode l_metadata_permissions l_metadata_links \
		l_metadata_owner l_metadata_rest <<EOF
$1
EOF
	for l_metadata_number in "$l_metadata_inode" "$l_metadata_links" \
		"$l_metadata_owner"; do
		case $l_metadata_number in
		'' | *[!0-9]*) return 1 ;;
		esac
	done
	# A type character and nine permission characters, then at most one
	# alternate-access marker (+ for an ACL, @ for macOS extended attributes,
	# . for an SELinux context). illumos writes l instead of S for
	# set-group-ID without group execute (mandatory locking).
	case $l_metadata_permissions in
	?[-r][-w][-xsS][-r][-w][-xsSl][-r][-w][-xtT] | \
		?[-r][-w][-xsS][-r][-w][-xsSl][-r][-w][-xtT]?) ;;
	*) return 1 ;;
	esac

	# Walk the owner, group and other triplets; the special bit of each is
	# set-user-ID (4), set-group-ID (2) and sticky (1).
	l_metadata_rest=${l_metadata_permissions#?}
	l_metadata_special=0
	l_metadata_mode=""
	for l_metadata_special_bit in 4 2 1; do
		l_metadata_digit=0
		case $l_metadata_rest in r*) l_metadata_digit=4 ;; esac
		l_metadata_rest=${l_metadata_rest#?}
		case $l_metadata_rest in w*) l_metadata_digit=$((l_metadata_digit + 2)) ;; esac
		l_metadata_rest=${l_metadata_rest#?}
		case $l_metadata_rest in
		[xst]*) l_metadata_digit=$((l_metadata_digit + 1)) ;;
		esac
		case $l_metadata_rest in
		[sStTl]*) l_metadata_special=$((l_metadata_special + l_metadata_special_bit)) ;;
		esac
		l_metadata_rest=${l_metadata_rest#?}
		l_metadata_mode=$l_metadata_mode$l_metadata_digit
	done
	# Drop leading zeros as `stat -c %a` does: 0700 is 700, 0 stays 0.
	l_metadata_mode=$l_metadata_special$l_metadata_mode
	while :; do
		case $l_metadata_mode in
		0?*) l_metadata_mode=${l_metadata_mode#0} ;;
		*) break ;;
		esac
	done

	g_zxfer_path_inode_result=$l_metadata_inode
	g_zxfer_path_owner_uid_result=$l_metadata_owner
	g_zxfer_path_mode_result=$l_metadata_mode
	g_zxfer_path_permissions_result=$l_metadata_permissions
}

# Purpose: Read one path's inode, owner and mode with a single `ls -ldin`.
# Usage: zxfer_read_path_metadata PATH; publishes the results of
# zxfer_parse_path_metadata_line, or returns 1 with them empty when ls fails
# or prints a line that does not parse. A symlink is described, not
# followed. ls replaces the substitution's subshell, so bash 3.2 forks once.
zxfer_read_path_metadata() {
	# A leading - would read as an ls option.
	case $1 in
	-*) l_metadata_path=./$1 ;;
	*) l_metadata_path=$1 ;;
	esac
	l_metadata_line=$(exec ls -ldin "$l_metadata_path" 2>/dev/null) ||
		l_metadata_line=""
	zxfer_parse_path_metadata_line "$l_metadata_line"
}

# Purpose: Print the numeric owner UID of an existing path.
# Usage: zxfer_get_path_owner_uid PATH; returns 1 when PATH does not exist or
# its metadata cannot be read.
zxfer_get_path_owner_uid() {
	[ -e "$1" ] || return 1
	zxfer_read_path_metadata "$1" || return 1
	printf '%s\n' "$g_zxfer_path_owner_uid_result"
}

# Purpose: Print the octal permission bits of an existing path.
# Usage: zxfer_get_path_mode_octal PATH; prints the `stat -c %a` form (600,
# or 2700 with the set-group-ID bit) and returns 1 when PATH does not exist or
# its metadata cannot be read.
zxfer_get_path_mode_octal() {
	[ -e "$1" ] || return 1
	zxfer_read_path_metadata "$1" || return 1
	printf '%s\n' "$g_zxfer_path_mode_result"
}

# Purpose: Record one real directory's inode, owner UID and mode from a single
# metadata read.
# Usage: zxfer_get_private_directory_security_record DIR; publishes
# INODE<TAB>UID<TAB>MODE in g_zxfer_private_directory_record_result, or
# returns 1 with it empty. Runtime records it when it creates the run root
# and compares a fresh one right before removing the root. The parent is
# fixed, so a device number would add nothing short of a mount at that exact
# path: a directory swapped in while the original still exists has another
# inode on the same file system.
zxfer_get_private_directory_security_record() {
	g_zxfer_private_directory_record_result=""
	[ -d "$1" ] && [ ! -L "$1" ] || return 1
	zxfer_read_path_metadata "$1" || return 1
	g_zxfer_private_directory_record_result=$g_zxfer_path_inode_result$ZXFER_TAB$g_zxfer_path_owner_uid_result$ZXFER_TAB$g_zxfer_path_mode_result
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

# Purpose: Look up the effective user UID once per process.
# Usage: zxfer_get_effective_user_uid; publishes g_zxfer_effective_uid, or
# returns 1 when id is missing or prints anything but a number. Call it
# outside $(...) so later checks reuse the memo.
zxfer_get_effective_user_uid() {
	[ -z "${g_zxfer_effective_uid:-}" ] || return 0
	l_effective_uid_output=$(exec id -u 2>/dev/null) || return 1
	case $l_effective_uid_output in
	'' | *[!0-9]*) return 1 ;;
	esac
	g_zxfer_effective_uid=$l_effective_uid_output
}

# Purpose: Check that a backup path owner is root or the effective user.
# Usage: zxfer_backup_owner_uid_is_allowed UID; returns 1 otherwise.
zxfer_backup_owner_uid_is_allowed() {
	[ "$1" != 0 ] || return 0
	zxfer_get_effective_user_uid || return 1
	[ "$1" = "$g_zxfer_effective_uid" ]
}

# Purpose: Print the allowed backup owners for an operator message.
# Usage: zxfer_describe_expected_backup_owner; prints "root (UID 0)" plus
# " or UID N" when the effective user is not root.
zxfer_describe_expected_backup_owner() {
	if zxfer_get_effective_user_uid && [ "$g_zxfer_effective_uid" != 0 ]; then
		printf '%s\n' "root (UID 0) or UID $g_zxfer_effective_uid"
	else
		printf '%s\n' "root (UID 0)"
	fi
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
	case $1 in
	/*/* | /) return 1 ;;
	/*) ;;
	*) return 1 ;;
	esac
	[ -L "$1" ] || return 1

	zxfer_read_path_metadata "$1" || return 1
	[ "$g_zxfer_path_owner_uid_result" = 0 ] || return 1
	zxfer_read_path_metadata / || return 1
	[ "$g_zxfer_path_owner_uid_result" = 0 ] || return 1
	zxfer_validate_shared_dir_permission_string "$g_zxfer_path_permissions_result"
}

# Purpose: Check that a directory is a safe parent for zxfer temp files and
# publish its physical path.
# Usage: zxfer_validate_temp_root_candidate DIR; DIR must be absolute. The
# directory it resolves to (cd -P) must be owned by root or the effective
# user and must not be group- or world-writable unless it is sticky.
# Publishes the physical path in g_zxfer_temp_root_candidate_result, or
# returns 1 with it empty. zxfer_try_get_effective_tmpdir memoizes the
# result.
zxfer_validate_temp_root_candidate() {
	g_zxfer_temp_root_candidate_result=""
	case $1 in
	/*) ;;
	*) return 1 ;;
	esac

	# One subshell: cd -P and pwd print the physical path, then ls replaces
	# the subshell and lists that directory as ".". The ls line is the last
	# line, and everything before it is the path, line feeds included.
	l_candidate_output=$(CDPATH='' cd -P "$1" 2>/dev/null && pwd &&
		exec ls -ldin . 2>/dev/null) || return 1
	l_candidate_physical=${l_candidate_output%"$ZXFER_LF"*}
	case $l_candidate_physical in
	/*) ;;
	*) return 1 ;;
	esac
	[ -d "$l_candidate_physical" ] || return 1
	zxfer_parse_path_metadata_line "${l_candidate_output##*"$ZXFER_LF"}" ||
		return 1
	if [ "$g_zxfer_path_owner_uid_result" != 0 ]; then
		zxfer_get_effective_user_uid || return 1
		[ "$g_zxfer_path_owner_uid_result" = "$g_zxfer_effective_uid" ] ||
			return 1
	fi
	zxfer_validate_shared_dir_permission_string \
		"$g_zxfer_path_permissions_result" || return 1

	g_zxfer_temp_root_candidate_result=$l_candidate_physical
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
