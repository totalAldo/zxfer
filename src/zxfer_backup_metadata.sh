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
# BACKUP METADATA / BACKUP STORAGE LAYOUT HELPERS
################################################################################

sanitize_backup_component() {
	l_component=$1
	if [ "$l_component" = "" ]; then
		printf 'h00\n'
		return
	fi
	l_sanitized=$(printf '%s' "$l_component" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n' | tr '[:lower:]' '[:upper:]')
	if [ "$l_sanitized" = "" ]; then
		l_sanitized="00"
	fi
	printf 'h%s\n' "$l_sanitized"
}

zxfer_legacy_sanitize_backup_component() {
	l_component=$1
	if [ "$l_component" = "" ]; then
		printf '_\n'
		return
	fi
	l_sanitized=$(printf '%s' "$l_component" | tr -c 'A-Za-z0-9._-' '_')
	if [ "$l_sanitized" = "" ]; then
		l_sanitized="_"
	fi
	printf '%s\n' "$l_sanitized"
}

sanitize_dataset_relpath() {
	l_path=$1
	l_trim=${l_path#/}
	l_trim=${l_trim%/}
	if [ "$l_trim" = "" ]; then
		printf 'dataset\n'
		return
	fi
	OLDIFS=$IFS
	IFS="/"
	l_result=""
	for l_part in $l_trim; do
		[ "$l_part" = "" ] && continue
		l_part=$(sanitize_backup_component "$l_part")
		l_result="$l_result/$l_part"
	done
	IFS=$OLDIFS
	l_result=${l_result#/}
	if [ "$l_result" = "" ]; then
		l_result="dataset"
	fi
	printf '%s\n' "$l_result"
}

zxfer_legacy_sanitize_dataset_relpath() {
	l_path=$1
	l_trim=${l_path#/}
	l_trim=${l_trim%/}
	if [ "$l_trim" = "" ]; then
		printf 'dataset\n'
		return
	fi
	OLDIFS=$IFS
	IFS="/"
	l_result=""
	for l_part in $l_trim; do
		[ "$l_part" = "" ] && continue
		l_part=$(zxfer_legacy_sanitize_backup_component "$l_part")
		l_result="$l_result/$l_part"
	done
	IFS=$OLDIFS
	l_result=${l_result#/}
	if [ "$l_result" = "" ]; then
		l_result="dataset"
	fi
	printf '%s\n' "$l_result"
}

get_backup_storage_dir() {
	l_mountpoint=$1
	l_dataset=$2
	zxfer_refresh_backup_storage_root

	case "$l_mountpoint" in
	"" | "-")
		l_relative="detached"
		;;
	legacy | none)
		l_relative=$l_mountpoint
		;;
	/)
		l_relative="root"
		;;
	*)
		l_trim=${l_mountpoint#/}
		l_trim=${l_trim%/}
		if [ "$l_trim" = "" ]; then
			l_relative="root"
		else
			OLDIFS=$IFS
			IFS="/"
			l_relative=""
			for l_part in $l_trim; do
				[ "$l_part" = "" ] && continue
				l_part=$(sanitize_backup_component "$l_part")
				l_relative="$l_relative/$l_part"
			done
			IFS=$OLDIFS
			l_relative=${l_relative#/}
			if [ "$l_relative" = "" ]; then
				l_relative="root"
			fi
		fi
		;;
	esac

	case "$l_mountpoint" in
	"" | "-" | legacy | none)
		l_dataset_rel=$(sanitize_dataset_relpath "$l_dataset")
		l_relative="$l_relative/$l_dataset_rel"
		;;
	esac

	printf '%s/%s\n' "$g_backup_storage_root" "$l_relative"
}

zxfer_get_legacy_backup_storage_dir() {
	l_mountpoint=$1
	l_dataset=$2
	zxfer_refresh_backup_storage_root

	case "$l_mountpoint" in
	"" | "-")
		l_relative="detached"
		;;
	legacy | none)
		l_relative=$l_mountpoint
		;;
	/)
		l_relative="root"
		;;
	*)
		l_trim=${l_mountpoint#/}
		l_trim=${l_trim%/}
		if [ "$l_trim" = "" ]; then
			l_relative="root"
		else
			OLDIFS=$IFS
			IFS="/"
			l_relative=""
			for l_part in $l_trim; do
				[ "$l_part" = "" ] && continue
				[ "$l_part" = "." ] && continue
				[ "$l_part" = ".." ] && continue
				l_part=$(zxfer_legacy_sanitize_backup_component "$l_part")
				l_relative="$l_relative/$l_part"
			done
			IFS=$OLDIFS
			l_relative=${l_relative#/}
			if [ "$l_relative" = "" ]; then
				l_relative="root"
			fi
		fi
		;;
	esac

	case "$l_mountpoint" in
	"" | "-" | legacy | none)
		l_dataset_rel=$(zxfer_legacy_sanitize_dataset_relpath "$l_dataset")
		l_relative="$l_relative/$l_dataset_rel"
		;;
	esac

	printf '%s/%s\n' "$g_backup_storage_root" "$l_relative"
}

zxfer_get_safe_raw_backup_storage_dir_for_mountpoint() {
	l_mountpoint=$1
	zxfer_refresh_backup_storage_root

	case "$l_mountpoint" in
	"" | "-" | legacy | none | /)
		return 1
		;;
	esac

	l_trim=${l_mountpoint#/}
	l_trim=${l_trim%/}
	if [ "$l_trim" = "" ]; then
		return 1
	fi

	OLDIFS=$IFS
	IFS="/"
	l_relative=""
	l_depth=0
	for l_part in $l_trim; do
		[ "$l_part" = "" ] && continue
		case "$l_part" in
		.)
			continue
			;;
		..)
			if [ "$l_depth" -le 0 ]; then
				IFS=$OLDIFS
				return 1
			fi
			case "$l_relative" in
			*/*)
				l_relative=${l_relative%/*}
				;;
			*)
				l_relative=""
				;;
			esac
			l_depth=$((l_depth - 1))
			continue
			;;
		esac
		if [ "$l_relative" = "" ]; then
			l_relative=$l_part
		else
			l_relative=$l_relative/$l_part
		fi
		l_depth=$((l_depth + 1))
	done
	IFS=$OLDIFS

	if [ "$l_relative" = "" ]; then
		return 1
	fi

	printf '%s/%s\n' "$g_backup_storage_root" "$l_relative"
}

get_backup_storage_dir_for_dataset_tree() {
	l_dataset=$1
	zxfer_refresh_backup_storage_root

	l_dataset_rel=${l_dataset#/}
	l_dataset_rel=${l_dataset%/}
	if [ "$l_dataset_rel" = "" ]; then
		l_dataset_rel="dataset"
	fi

	printf '%s/%s\n' "$g_backup_storage_root" "$l_dataset_rel"
}

zxfer_get_legacy_backup_metadata_filename() {
	l_source=$1
	l_tail=$(echo "$l_source" | sed -e 's/.*\///g')
	printf '%s.%s\n' "$g_backup_file_extension" "$l_tail"
}

zxfer_backup_metadata_file_key() {
	l_source=$1
	l_destination=$2
	l_identity=$(printf '%s\n%s\n' "$l_source" "$l_destination")
	if l_key_cksum=$(printf '%s' "$l_identity" | cksum 2>/dev/null); then
		# shellcheck disable=SC2086
		set -- $l_key_cksum
		if [ $# -ge 2 ] && [ -n "$1" ] && [ -n "$2" ]; then
			printf 'k%s.%s\n' "$1" "$2"
			return 0
		fi
	fi
	l_key_hex=$(printf '%s' "$l_identity" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n' | cut -c 1-16)
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	printf 'k%s\n' "$l_key_hex"
}

zxfer_get_backup_metadata_filename() {
	l_source=$1
	l_destination=$2
	l_legacy_name=$(zxfer_get_legacy_backup_metadata_filename "$l_source")
	if ! l_key=$(zxfer_backup_metadata_file_key "$l_source" "$l_destination"); then
		return 1
	fi
	printf '%s.%s\n' "$l_legacy_name" "$l_key"
}

ensure_local_backup_dir() {
	l_dir=$1
	if l_symlink_component=$(zxfer_find_symlink_path_component "$l_dir"); then
		if [ "$l_symlink_component" = "$l_dir" ]; then
			throw_error "Refusing to use backup directory $l_dir because it is a symlink."
		fi
		throw_error "Refusing to use backup directory $l_dir because path component $l_symlink_component is a symlink."
	fi
	if [ -L "$l_dir" ]; then
		throw_error "Refusing to use backup directory $l_dir because it is a symlink."
	fi
	if [ -e "$l_dir" ] && [ ! -d "$l_dir" ]; then
		throw_error "Refusing to use backup directory $l_dir because it is not a directory."
	fi
	if [ ! -d "$l_dir" ]; then
		l_old_umask=$(umask)
		umask 077
		if ! mkdir -p "$l_dir"; then
			umask "$l_old_umask"
			throw_error "Error creating secure backup directory $l_dir."
		fi
		umask "$l_old_umask"
	fi
	if ! l_owner_uid=$(get_path_owner_uid "$l_dir"); then
		throw_error "Cannot determine the owner of backup directory $l_dir."
	fi
	if ! backup_owner_uid_is_allowed "$l_owner_uid"; then
		l_expected_owner_desc=$(describe_expected_backup_owner)
		throw_error "Refusing to use backup directory $l_dir because it is owned by UID $l_owner_uid instead of $l_expected_owner_desc."
	fi
	if ! chmod 700 "$l_dir"; then
		throw_error "Error securing backup directory $l_dir."
	fi
}

zxfer_build_remote_backup_dir_symlink_guard_cmd() {
	l_dir_single=$1

	printf '%s' "l_scan_path='$l_dir_single'; l_scan_remaining=\$l_scan_path; l_scan_candidate=''; while [ -n \"\$l_scan_remaining\" ]; do case \"\$l_scan_remaining\" in /*) if [ \"\$l_scan_candidate\" = '' ]; then l_scan_candidate=/; l_scan_remaining=\${l_scan_remaining#/}; continue; fi ;; esac; l_scan_component=\${l_scan_remaining%%/*}; if [ \"\$l_scan_component\" = \"\$l_scan_remaining\" ]; then l_scan_remaining=''; else l_scan_remaining=\${l_scan_remaining#*/}; fi; [ -n \"\$l_scan_component\" ] || continue; case \"\$l_scan_candidate\" in '') l_scan_candidate=\$l_scan_component ;; /) l_scan_candidate=/\$l_scan_component ;; *) l_scan_candidate=\$l_scan_candidate/\$l_scan_component ;; esac; if [ -L \"\$l_scan_candidate\" ] || [ -h \"\$l_scan_candidate\" ]; then l_scan_trusted=0; case \"\$l_scan_candidate\" in /*) l_scan_parent=\${l_scan_candidate%/*}; [ -n \"\$l_scan_parent\" ] || l_scan_parent=/; l_scan_owner=''; l_scan_parent_owner=''; if command -v stat >/dev/null 2>&1; then l_scan_owner=\$(stat -c '%u' \"\$l_scan_candidate\" 2>/dev/null); if [ \"\$l_scan_owner\" = '' ] || printf '%s' \"\$l_scan_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_owner=\$(stat -f '%u' \"\$l_scan_candidate\" 2>/dev/null); fi; l_scan_parent_owner=\$(stat -c '%u' \"\$l_scan_parent\" 2>/dev/null); if [ \"\$l_scan_parent_owner\" = '' ] || printf '%s' \"\$l_scan_parent_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_parent_owner=\$(stat -f '%u' \"\$l_scan_parent\" 2>/dev/null); fi; fi; if [ \"\$l_scan_owner\" = '0' ] && [ \"\$l_scan_parent_owner\" = '0' ] && [ \"\$l_scan_parent\" = '/' ]; then l_scan_ls_path=\$l_scan_parent; case \"\$l_scan_ls_path\" in -*) l_scan_ls_path=./\$l_scan_ls_path ;; esac; l_scan_ls_line=\$(ls -ldn \"\$l_scan_ls_path\" 2>/dev/null) || l_scan_ls_line=''; if [ \"\$l_scan_ls_line\" != '' ]; then l_scan_parent_perm=\$(printf '%s\n' \"\$l_scan_ls_line\" | awk '{print \$1}'); case \"\$l_scan_parent_perm\" in ??????????*) l_scan_group_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 6); l_scan_other_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 9); l_scan_sticky=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 10); case \"\$l_scan_group_write\$l_scan_other_write\" in *w*) case \"\$l_scan_sticky\" in t|T) l_scan_trusted=1 ;; esac ;; *) l_scan_trusted=1 ;; esac ;; esac; fi; fi ;; esac; if [ \"\$l_scan_trusted\" = '1' ]; then continue; fi; if [ \"\$l_scan_candidate\" = \"\$l_scan_path\" ]; then echo 'Refusing to use symlinked zxfer backup directory.' >&2; else echo \"Refusing to use backup directory \$l_scan_path because path component \$l_scan_candidate is a symlink.\" >&2; fi; exit 1; fi; done"
}

zxfer_build_remote_backup_metadata_symlink_guard_cmd() {
	l_path_single=$1

	printf '%s' "l_scan_path='$l_path_single'; l_scan_remaining=\$l_scan_path; l_scan_candidate=''; while [ -n \"\$l_scan_remaining\" ]; do case \"\$l_scan_remaining\" in /*) if [ \"\$l_scan_candidate\" = '' ]; then l_scan_candidate=/; l_scan_remaining=\${l_scan_remaining#/}; continue; fi ;; esac; l_scan_component=\${l_scan_remaining%%/*}; if [ \"\$l_scan_component\" = \"\$l_scan_remaining\" ]; then l_scan_remaining=''; else l_scan_remaining=\${l_scan_remaining#*/}; fi; [ -n \"\$l_scan_component\" ] || continue; case \"\$l_scan_candidate\" in '') l_scan_candidate=\$l_scan_component ;; /) l_scan_candidate=/\$l_scan_component ;; *) l_scan_candidate=\$l_scan_candidate/\$l_scan_component ;; esac; if [ -L \"\$l_scan_candidate\" ] || [ -h \"\$l_scan_candidate\" ]; then l_scan_trusted=0; case \"\$l_scan_candidate\" in /*) l_scan_parent=\${l_scan_candidate%/*}; [ -n \"\$l_scan_parent\" ] || l_scan_parent=/; l_scan_owner=''; l_scan_parent_owner=''; if command -v stat >/dev/null 2>&1; then l_scan_owner=\$(stat -c '%u' \"\$l_scan_candidate\" 2>/dev/null); if [ \"\$l_scan_owner\" = '' ] || printf '%s' \"\$l_scan_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_owner=\$(stat -f '%u' \"\$l_scan_candidate\" 2>/dev/null); fi; l_scan_parent_owner=\$(stat -c '%u' \"\$l_scan_parent\" 2>/dev/null); if [ \"\$l_scan_parent_owner\" = '' ] || printf '%s' \"\$l_scan_parent_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_parent_owner=\$(stat -f '%u' \"\$l_scan_parent\" 2>/dev/null); fi; fi; if [ \"\$l_scan_owner\" = '0' ] && [ \"\$l_scan_parent_owner\" = '0' ] && [ \"\$l_scan_parent\" = '/' ]; then l_scan_ls_path=\$l_scan_parent; case \"\$l_scan_ls_path\" in -*) l_scan_ls_path=./\$l_scan_ls_path ;; esac; l_scan_ls_line=\$(ls -ldn \"\$l_scan_ls_path\" 2>/dev/null) || l_scan_ls_line=''; if [ \"\$l_scan_ls_line\" != '' ]; then l_scan_parent_perm=\$(printf '%s\n' \"\$l_scan_ls_line\" | awk '{print \$1}'); case \"\$l_scan_parent_perm\" in ??????????*) l_scan_group_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 6); l_scan_other_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 9); l_scan_sticky=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 10); case \"\$l_scan_group_write\$l_scan_other_write\" in *w*) case \"\$l_scan_sticky\" in t|T) l_scan_trusted=1 ;; esac ;; *) l_scan_trusted=1 ;; esac ;; esac; fi; fi ;; esac; if [ \"\$l_scan_trusted\" = '1' ]; then continue; fi; if [ \"\$l_scan_candidate\" = \"\$l_scan_path\" ]; then echo \"Refusing to use backup metadata \$l_scan_path because it is a symlink.\" >&2; else echo \"Refusing to use backup metadata \$l_scan_path because path component \$l_scan_candidate is a symlink.\" >&2; fi; exit 1; fi; done"
}

ensure_remote_backup_dir() {
	l_dir=$1
	l_host=$2
	l_profile_side=${3:-}

	[ "$l_host" = "" ] && return

	l_dir_single=$(escape_for_single_quotes "$l_dir")
	l_dir_ls_path=$l_dir
	case "$l_dir_ls_path" in
	-*)
		l_dir_ls_path=./$l_dir_ls_path
		;;
	esac
	l_dir_ls_single=$(escape_for_single_quotes "$l_dir_ls_path")
	l_remote_symlink_guard_cmd=$(zxfer_build_remote_backup_dir_symlink_guard_cmd "$l_dir_single")
	l_remote_cmd="$l_remote_symlink_guard_cmd; [ -L '$l_dir_single' ] && { echo 'Refusing to use symlinked zxfer backup directory.' >&2; exit 1; }; if [ -e '$l_dir_single' ] && [ ! -d '$l_dir_single' ]; then echo 'Backup path exists but is not a directory.' >&2; exit 1; fi; umask 077; if ! mkdir -p '$l_dir_single'; then echo 'Error creating secure backup directory.' >&2; exit 1; fi; if ! chmod 700 '$l_dir_single'; then echo 'Error securing backup directory.' >&2; exit 1; fi; l_expected_uid=\$(id -u); l_dir_uid=''; if command -v stat >/dev/null 2>&1; then l_dir_uid=\$(stat -c '%u' '$l_dir_single' 2>/dev/null); if [ \"\$l_dir_uid\" = '' ] || printf '%s' \"\$l_dir_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_dir_uid=\$(stat -f '%u' '$l_dir_single' 2>/dev/null); fi; fi; if [ \"\$l_dir_uid\" = '' ] || printf '%s' \"\$l_dir_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_ls_line=\$(ls -ldn '$l_dir_ls_single' 2>/dev/null) || l_ls_line=''; if [ \"\$l_ls_line\" != '' ]; then l_dir_uid=\$(printf '%s\n' \"\$l_ls_line\" | awk '{print \$3}'); fi; fi; if [ \"\$l_dir_uid\" = '' ]; then echo 'Unable to determine backup directory owner.' >&2; exit 1; fi; if [ \"\$l_dir_uid\" != 0 ] && [ \"\$l_dir_uid\" != \"\$l_expected_uid\" ]; then echo 'Backup directory must be owned by root or the ssh user.' >&2; exit 1; fi"
	l_remote_shell_cmd=$(build_remote_sh_c_command "$l_remote_cmd")
	if ! invoke_ssh_shell_command_for_host "$l_host" "$l_remote_shell_cmd" "$l_profile_side"; then
		throw_error "Error preparing backup directory on $l_host."
	fi
}

read_local_backup_file() {
	l_path=$1
	require_backup_metadata_path_without_symlinks "$l_path" || return 1
	if [ ! -f "$l_path" ] || [ -h "$l_path" ]; then
		return 1
	fi
	require_secure_backup_file "$l_path"
	cat "$l_path"
}

read_remote_backup_file() {
	l_host=$1
	l_path=$2
	l_profile_side=${3:-}

	l_path_single=$(escape_for_single_quotes "$l_path")
	l_remote_insecure_owner_status=95
	l_remote_insecure_mode_status=96
	l_remote_unknown_status=97
	l_remote_awk_cmd="awk"
	l_remote_cat_helper=${g_cmd_cat:-cat}
	l_path_ls_path=$l_path
	case "$l_path_ls_path" in
	-*)
		l_path_ls_path=./$l_path_ls_path
		;;
	esac
	l_path_ls_single=$(escape_for_single_quotes "$l_path_ls_path")
	l_remote_symlink_guard_cmd=$(zxfer_build_remote_backup_metadata_symlink_guard_cmd "$l_path_single")
	l_remote_cat_cmd=$(build_shell_command_from_argv "$l_remote_cat_helper" "$l_path")
	l_remote_secure_cat_cmd="$l_remote_symlink_guard_cmd; if [ ! -f '$l_path_single' ] || [ -h '$l_path_single' ]; then exit 1; fi; \
l_expected_uid=''; \
if command -v id >/dev/null 2>&1; then l_expected_uid=\$(id -u 2>/dev/null); fi; \
if [ \"\$l_expected_uid\" = '' ] || printf '%s' \"\$l_expected_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then exit $l_remote_unknown_status; fi; \
l_uid=''; \
if command -v stat >/dev/null 2>&1; then l_uid=\$(stat -c '%u' '$l_path_single' 2>/dev/null); if [ \"\$l_uid\" = '' ] || printf '%s' \"\$l_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_uid=\$(stat -f '%u' '$l_path_single' 2>/dev/null); fi; fi; \
if [ \"\$l_uid\" = '' ] || printf '%s' \"\$l_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_ls_line=\$(ls -ldn '$l_path_ls_single' 2>/dev/null) || l_ls_line=''; if [ \"\$l_ls_line\" != '' ]; then l_uid=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$3}'); fi; fi; \
if [ \"\$l_uid\" = '' ]; then exit $l_remote_unknown_status; fi; \
if [ \"\$l_uid\" != '0' ] && [ \"\$l_uid\" != \"\$l_expected_uid\" ]; then exit $l_remote_insecure_owner_status; fi; \
l_mode=''; \
if command -v stat >/dev/null 2>&1; then l_mode=\$(stat -c '%a' '$l_path_single' 2>/dev/null); if [ \"\$l_mode\" = '' ] || printf '%s' \"\$l_mode\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_mode=\$(stat -f '%OLp' '$l_path_single' 2>/dev/null); fi; fi; \
if [ \"\$l_mode\" = '' ] || printf '%s' \"\$l_mode\" | grep -q '[^0-9]' >/dev/null 2>&1; then if [ \"\$l_ls_line\" = '' ]; then l_ls_line=\$(ls -ldn '$l_path_ls_single' 2>/dev/null) || l_ls_line=''; fi; if [ \"\$l_ls_line\" != '' ]; then l_perm=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$1}'); if [ \"\$l_perm\" = '-rw-------' ]; then l_mode='600'; fi; fi; fi; \
	if [ \"\$l_mode\" = '' ]; then exit $l_remote_unknown_status; fi; \
if [ \"\$l_mode\" != '600' ]; then exit $l_remote_insecure_mode_status; fi; \
$l_remote_cat_cmd"
	l_remote_secure_cat_shell_cmd=$(build_remote_sh_c_command "$l_remote_secure_cat_cmd")
	invoke_ssh_shell_command_for_host "$l_host" "$l_remote_secure_cat_shell_cmd" "$l_profile_side"
	l_remote_status=$?
	if [ $l_remote_status -eq $l_remote_insecure_owner_status ]; then
		throw_error "Refusing to use backup metadata $l_path on $l_host because it is not owned by root or the ssh user."
	fi
	if [ $l_remote_status -eq $l_remote_insecure_mode_status ]; then
		throw_error "Refusing to use backup metadata $l_path on $l_host because its permissions are not 0600."
	fi
	if [ $l_remote_status -eq $l_remote_unknown_status ]; then
		throw_error "Cannot determine ownership or permissions for backup metadata $l_path on $l_host."
	fi
	return $l_remote_status
}

zxfer_find_remote_backup_files_by_name() {
	l_host=$1
	l_backup_root=$2
	l_find_name=$3
	l_profile_side=${4:-}

	[ -n "$l_host" ] || return 0
	[ -n "$l_backup_root" ] || return 0

	if ! l_remote_find_cmd=$(zxfer_resolve_remote_cli_command_safe "$l_host" "find" "find" "$l_profile_side"); then
		return 1
	fi

	l_backup_root_single=$(escape_for_single_quotes "$l_backup_root")
	l_find_name_single=$(escape_for_single_quotes "$l_find_name")
	l_remote_find_shell_cmd=$(build_remote_sh_c_command "if [ ! -d '$l_backup_root_single' ]; then exit 0; fi; $l_remote_find_cmd '$l_backup_root_single' -name '$l_find_name_single' -type f 2>/dev/null")
	invoke_ssh_shell_command_for_host "$l_host" "$l_remote_find_shell_cmd" "$l_profile_side"
}

backup_metadata_extract_properties_for_dataset_pair() {
	l_backup_contents=$1
	l_expected_source=$2
	l_expected_destination=$3

	# shellcheck disable=SC2016
	printf '%s\n' "$l_backup_contents" | "${g_cmd_awk:-awk}" \
		-v expected_source="$l_expected_source" \
		-v expected_destination="$l_expected_destination" '
{
	if ($0 == "" || substr($0, 1, 1) == "#")
		next

	first_comma = index($0, ",")
	if (first_comma == 0)
		next
	rest = substr($0, first_comma + 1)
	second_comma = index(rest, ",")
	if (second_comma == 0)
		next

	first = substr($0, 1, first_comma - 1)
	second = substr(rest, 1, second_comma - 1)
	props = substr(rest, second_comma + 1)

	if (first == expected_source && second == expected_destination) {
		modern_count++
		modern_props = props
		next
	}
	if (first == expected_destination && second == expected_source) {
		legacy_count++
		legacy_props = props
	}
}
END {
	if (modern_count == 1 && legacy_count == 0) {
		print modern_props
		exit 0
	}
	if (modern_count == 0 && legacy_count == 1) {
		print legacy_props
		exit 0
	}
	if (modern_count == 0 && legacy_count == 0)
		exit 1
	exit 2
}'
}

backup_metadata_matches_source() {
	l_backup_contents=$1
	l_expected_source=$2
	l_expected_destination=$3

	backup_metadata_extract_properties_for_dataset_pair "$l_backup_contents" "$l_expected_source" "$l_expected_destination" >/dev/null
}

zxfer_get_expected_backup_destination_for_source() {
	l_source=$1

	l_base_fs=${initial_source##*/}
	l_part_of_source_to_delete=${initial_source%"$l_base_fs"}

	if [ "${g_initial_source_had_trailing_slash:-0}" -eq 0 ]; then
		l_dest_tail=$(echo "$l_source" | sed -e "s%^$l_part_of_source_to_delete%%g")
		printf '%s/%s\n' "$g_destination" "$l_dest_tail"
	else
		l_trailing_slash_dest_tail=$(echo "$l_source" | sed -e "s%^$initial_source%%g")
		printf '%s%s\n' "$g_destination" "$l_trailing_slash_dest_tail"
	fi
}

#
# Gets the backup properties from a previous backup of those properties
# This takes $initial_source. Secure backup metadata is keyed by the source
# dataset hierarchy under ZXFER_BACKUP_DIR, so recursive child restores can
# walk up through ancestor source datasets until they reach the matching root
# backup file.
#
get_backup_properties() {
	zxfer_set_failure_stage "backup metadata read"

	# We will step back through the filesystem hierarchy from $initial_source
	# until the pool level, looking for the backup file, stopping when we find
	# it or terminating with an error.
	# shellcheck disable=SC2154
	zxfer_refresh_backup_storage_root

	l_suspect_fs=$initial_source
	l_found_backup_file=0
	l_used_legacy_backup=0
	l_legacy_backup_path=""
	l_expected_secure_backup=""
	l_fallback_error_message=""
	l_fallback_usage_error_message=""
	l_expected_root_destination=$(zxfer_get_expected_backup_destination_for_source "$initial_source")
	l_fallback_datasets_file=$(get_temp_file)
	: >"$l_fallback_datasets_file"

	while [ $l_found_backup_file -eq 0 ]; do
		l_backup_file_name=$(zxfer_get_backup_metadata_filename "$l_suspect_fs" "$g_destination")
		l_legacy_backup_file_name=$(zxfer_get_legacy_backup_metadata_filename "$l_suspect_fs")
		l_mountpoint=$(run_source_zfs_cmd get -H -o value mountpoint "$l_suspect_fs")
		l_dataset_secure_dir=$(get_backup_storage_dir_for_dataset_tree "$l_suspect_fs")
		l_dataset_backup_file="$l_dataset_secure_dir/$l_backup_file_name"
		l_dataset_legacy_backup_file="$l_dataset_secure_dir/$l_legacy_backup_file_name"
		l_mountpoint_rel=${l_mountpoint#/}
		l_encoded_secure_dir=$(get_backup_storage_dir "$l_mountpoint" "$l_suspect_fs")
		l_encoded_backup_file="$l_encoded_secure_dir/$l_legacy_backup_file_name"
		l_legacy_secure_dir=$(zxfer_get_legacy_backup_storage_dir "$l_mountpoint" "$l_suspect_fs")
		l_backup_file="$l_legacy_secure_dir/$l_legacy_backup_file_name"
		# Fall back to the raw mountpoint path under ZXFER_BACKUP_DIR; some callers
		# lay out backup metadata using the literal mountpoint rather than the
		# hardened compatibility path. Only accept canonicalized paths that stay
		# inside ZXFER_BACKUP_DIR.
		l_mount_backup_file=""
		if [ "$l_mountpoint_rel" != "" ] &&
			l_mount_backup_dir=$(zxfer_get_safe_raw_backup_storage_dir_for_mountpoint "$l_mountpoint"); then
			l_mount_backup_file="$l_mount_backup_dir/$l_legacy_backup_file_name"
		fi
		l_legacy_backup_file=""
		case "$l_mountpoint" in
		"" | "-" | legacy | none) ;;
		*)
			l_legacy_backup_file=$l_mountpoint/$l_legacy_backup_file_name
			;;
		esac

		# Try each candidate path in order. The dataset-relative secure path is
		# the current stable layout shared by -k and -e. Older sanitized-mountpoint,
		# raw-mountpoint, and legacy live-mountpoint paths remain as compatibility
		# fallbacks. Security failures propagate via require_secure_backup_file()
		# and will terminate the run with the expected error message.
		if [ "$g_option_O_origin_host" = "" ]; then
			for l_candidate in "$l_dataset_backup_file" "$l_dataset_legacy_backup_file" "$l_encoded_backup_file" "$l_backup_file" "$l_mount_backup_file" "$l_legacy_backup_file"; do
				[ "$l_candidate" = "" ] && continue
				if l_backup_contents=$(read_local_backup_file "$l_candidate"); then
					backup_metadata_matches_source "$l_backup_contents" "$initial_source" "$l_expected_root_destination"
					l_backup_match_status=$?
					if [ $l_backup_match_status -eq 0 ]; then
						g_restored_backup_file_contents=$l_backup_contents
						l_found_backup_file=1
						if [ "$l_candidate" = "$l_legacy_backup_file" ]; then
							l_used_legacy_backup=1
							l_legacy_backup_path="$l_candidate"
							l_expected_secure_backup="$l_dataset_backup_file"
						fi
						break
					fi
					if [ $l_backup_match_status -eq 2 ]; then
						l_fallback_usage_error_message="Backup property file $l_candidate contains multiple entries for source dataset $initial_source and destination $l_expected_root_destination. Remove the ambiguous rows or restore from a specific exact backup path."
						break
					fi
				fi
			done
		else
			for l_candidate in "$l_dataset_backup_file" "$l_dataset_legacy_backup_file" "$l_encoded_backup_file" "$l_backup_file" "$l_mount_backup_file" "$l_legacy_backup_file"; do
				[ "$l_candidate" = "" ] && continue
				if l_backup_contents=$(read_remote_backup_file "$g_option_O_origin_host" "$l_candidate" source); then
					backup_metadata_matches_source "$l_backup_contents" "$initial_source" "$l_expected_root_destination"
					l_backup_match_status=$?
					if [ $l_backup_match_status -eq 0 ]; then
						g_restored_backup_file_contents=$l_backup_contents
						l_found_backup_file=1
						if [ "$l_candidate" = "$l_legacy_backup_file" ]; then
							l_used_legacy_backup=1
							l_legacy_backup_path="$l_candidate"
							l_expected_secure_backup="$l_dataset_backup_file"
						fi
						break
					fi
					if [ $l_backup_match_status -eq 2 ]; then
						l_fallback_usage_error_message="Backup property file $l_candidate contains multiple entries for source dataset $initial_source and destination $l_expected_root_destination. Remove the ambiguous rows or restore from a specific exact backup path."
						break
					fi
				fi
			done
		fi

		if [ "$l_fallback_usage_error_message" != "" ]; then
			break
		fi

		if [ $l_found_backup_file -eq 0 ]; then
			printf '%s\n' "$l_suspect_fs" >>"$l_fallback_datasets_file"
			l_suspect_fs_parent=$(echo "$l_suspect_fs" | sed -e 's%/[^/]*$%%g')
			if [ "$l_suspect_fs_parent" = "$l_suspect_fs" ]; then
				break
			else
				l_suspect_fs=$l_suspect_fs_parent
			fi
		fi
	done

	if [ $l_found_backup_file -eq 0 ] && [ "$l_fallback_usage_error_message" = "" ]; then
		l_fallback_parse_failed=0
		while IFS= read -r l_find_dataset; do
			[ -n "$l_find_dataset" ] || continue
			l_find_name=$(zxfer_get_backup_metadata_filename "$l_find_dataset" "$g_destination")
			l_find_legacy_name=$(zxfer_get_legacy_backup_metadata_filename "$l_find_dataset")
			for l_find_candidate_name in "$l_find_name" "$l_find_legacy_name"; do
				[ -n "$l_find_candidate_name" ] || continue
				if [ "$g_option_O_origin_host" = "" ]; then
					[ -d "$g_backup_storage_root" ] || continue
					l_find_results_file=$(get_temp_file)
					find "$g_backup_storage_root" -name "$l_find_candidate_name" -type f 2>/dev/null >"$l_find_results_file"
					l_matching_backup_count=0
					l_matching_backup_contents=""
					while IFS= read -r l_found_path; do
						[ -n "$l_found_path" ] || continue
						if l_backup_contents=$(read_local_backup_file "$l_found_path"); then
							backup_metadata_matches_source "$l_backup_contents" "$initial_source" "$l_expected_root_destination"
							l_backup_match_status=$?
							if [ $l_backup_match_status -eq 0 ]; then
								l_matching_backup_count=$((l_matching_backup_count + 1))
								if [ $l_matching_backup_count -eq 1 ]; then
									l_matching_backup_contents=$l_backup_contents
								fi
								continue
							fi
							if [ $l_backup_match_status -eq 2 ]; then
								l_fallback_usage_error_message="Backup property file $l_found_path contains multiple entries for source dataset $initial_source and destination $l_expected_root_destination. Remove the ambiguous rows or restore from a specific exact backup path."
								l_fallback_parse_failed=1
								break
							fi
						fi
					done <"$l_find_results_file"
					rm -f "$l_find_results_file"
				else
					[ -n "$g_backup_storage_root" ] || continue
					if ! l_find_results=$(zxfer_find_remote_backup_files_by_name "$g_option_O_origin_host" "$g_backup_storage_root" "$l_find_candidate_name" source); then
						l_fallback_error_message="Failed to search for backup property file $l_find_candidate_name under $g_backup_storage_root on $g_option_O_origin_host."
						break 2
					fi
					l_matching_backup_count=0
					l_matching_backup_contents=""
					while IFS= read -r l_found_path; do
						[ -n "$l_found_path" ] || continue
						if l_backup_contents=$(read_remote_backup_file "$g_option_O_origin_host" "$l_found_path" source); then
							backup_metadata_matches_source "$l_backup_contents" "$initial_source" "$l_expected_root_destination"
							l_backup_match_status=$?
							if [ $l_backup_match_status -eq 0 ]; then
								l_matching_backup_count=$((l_matching_backup_count + 1))
								if [ $l_matching_backup_count -eq 1 ]; then
									l_matching_backup_contents=$l_backup_contents
								fi
								continue
							fi
							if [ $l_backup_match_status -eq 2 ]; then
								l_fallback_usage_error_message="Backup property file $l_found_path contains multiple entries for source dataset $initial_source and destination $l_expected_root_destination. Remove the ambiguous rows or restore from a specific exact backup path."
								l_fallback_parse_failed=1
								break
							fi
						fi
					done <<-EOF
						$l_find_results
					EOF
				fi

				if [ $l_fallback_parse_failed -eq 1 ]; then
					break 2
				fi

				case "$l_matching_backup_count" in
				0) ;;
				1)
					g_restored_backup_file_contents=$l_backup_contents
					l_found_backup_file=1
					g_restored_backup_file_contents=$l_matching_backup_contents
					l_found_backup_file=1
					break 2
					;;
				*)
					l_fallback_usage_error_message="Multiple backup property files named $l_find_candidate_name under $g_backup_storage_root matched source dataset $initial_source and destination $l_expected_root_destination. Remove the ambiguous files or restore from a specific exact backup path."
					break 2
					;;
				esac
			done
		done <"$l_fallback_datasets_file"
	fi

	rm -f "$l_fallback_datasets_file"

	if [ "$l_fallback_error_message" != "" ]; then
		throw_error "$l_fallback_error_message"
	fi

	if [ "$l_fallback_usage_error_message" != "" ]; then
		throw_error_with_usage "$l_fallback_usage_error_message"
	fi

	if [ $l_found_backup_file -eq 0 ]; then
		throw_error_with_usage "Cannot find backup property file. Ensure that it
exists under the source-dataset-relative tree inside ZXFER_BACKUP_DIR
or, for older backups, in a directory corresponding to one of the
ancestor filesystem mountpoints of the source."
	fi

	if [ $l_used_legacy_backup -eq 1 ]; then
		echo "Warning: read legacy backup metadata from $l_legacy_backup_path. Move it to $l_expected_secure_backup (or set ZXFER_BACKUP_DIR) to use the hardened storage path." >&2
	fi

	# at this point the $g_backup_file_contents will be a list of lines with
	# $source,$g_actual_dest,$source_pvs
}

#
# Writes the backup properties to a file in the source-dataset-relative secure
# backup tree under ZXFER_BACKUP_DIR. That keeps -k and -e keyed from the same
# stable identifier set even when source and destination mountpoints differ.
#
write_backup_properties() {
	zxfer_set_failure_stage "backup metadata write"

	if [ "$g_backup_file_contents" = "" ]; then
		echov "No property data collected; skipping backup write."
		return
	fi
	zxfer_refresh_backup_storage_root
	l_backup_file_name=$(zxfer_get_backup_metadata_filename "$initial_source" "$g_destination")
	l_backup_file_dir=$(get_backup_storage_dir_for_dataset_tree "$initial_source")
	l_backup_file_path=$l_backup_file_dir/$l_backup_file_name
	echov "Writing backup info to secure path $l_backup_file_path (dataset $initial_source)"

	# Construct the backup file contents
	l_backup_file_header="#zxfer property backup file;#version:$g_zxfer_version;#R options:$g_option_R_recursive;#N options:$g_option_N_nonrecursive;#destination:$g_destination;#initial_source:${initial_source##*/};"
	l_backup_date=$(date)
	g_backup_file_contents="$l_backup_file_header#backup_date:$l_backup_date$g_backup_file_contents"

	# Execute the command
	if [ "$g_option_n_dryrun" -eq 0 ]; then
		if [ "$g_option_T_target_host" = "" ]; then
			ensure_local_backup_dir "$g_backup_storage_root"
			ensure_local_backup_dir "$l_backup_file_dir"
			l_old_umask=$(umask)
			umask 077
			if ! printf '%s' "$g_backup_file_contents" | tr ";" "\n" >"$l_backup_file_path"; then
				umask "$l_old_umask"
				throw_error "Error writing backup file. Is filesystem mounted?"
			fi
			umask "$l_old_umask"
		else
			ensure_remote_backup_dir "$g_backup_storage_root" "$g_option_T_target_host" destination
			ensure_remote_backup_dir "$l_backup_file_dir" "$g_option_T_target_host" destination
			if ! l_remote_write_helper_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "cat" "cat" destination); then
				g_zxfer_failure_class=dependency
				throw_error "$l_remote_write_helper_safe"
			fi
			l_backup_file_path_single=$(escape_for_single_quotes "$l_backup_file_path")
			l_remote_write_cmd="umask 077; $l_remote_write_helper_safe > '$l_backup_file_path_single'"
			l_remote_write_shell_cmd=$(build_remote_sh_c_command "$l_remote_write_cmd")
			if ! printf '%s' "$g_backup_file_contents" | tr ";" "\n" |
				invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_write_shell_cmd" destination; then
				throw_error "Error writing backup file. Is filesystem mounted?"
			fi
		fi
	else
		l_backup_contents_cmd=$(zxfer_render_command_for_report "" printf '%s' "$g_backup_file_contents")
		l_translate_cmd=$(zxfer_render_command_for_report "" tr ";" "\n")
		l_backup_file_path_safe=$(zxfer_quote_token_for_report "$l_backup_file_path")
		if [ "$g_option_T_target_host" = "" ]; then
			printf '%s\n' "umask 077; $l_backup_contents_cmd | $l_translate_cmd > $l_backup_file_path_safe"
		else
			l_remote_write_cmd="umask 077; cat > $l_backup_file_path_safe"
			l_host_tokens=$(split_host_spec_tokens "$g_option_T_target_host")
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
				l_remote_write_cmd=$(build_remote_sh_c_command "$l_remote_write_cmd")
			fi
			l_remote_write_shell_cmd=$(build_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_write_cmd")
			printf '%s\n' "$l_backup_contents_cmd | $l_translate_cmd | $l_remote_write_shell_cmd"
		fi
	fi
}
