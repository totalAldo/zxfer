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
# SECURE BACKUP STORAGE
################################################################################

# Module contract:
# owns globals: g_backup_storage_root plus backup path, read/write, staging,
#   publication, rollback, and remote-command result scratch.
# reads globals: backup extension, local and remote host context, secure PATH
#   policy, path-security helpers, and runtime/remote execution helpers.
# mutates caches: runtime artifact registry while local staging paths exist.
# returns via stdout: secure storage paths and filenames, checked file contents,
#   and rendered remote storage commands.

ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE="__ZXFER_BACKUP_METADATA_PAIR_SPLIT__"

# Purpose: Refresh the backup storage root from the public override or the
# module-owned value already validated for this session.
# Usage: Called by backup storage, metadata, and replication preparation before
# they derive or create paths below the configured root.
zxfer_refresh_backup_storage_root() {
	if [ -n "${ZXFER_BACKUP_DIR:-}" ]; then
		l_backup_storage_root=$ZXFER_BACKUP_DIR
	elif [ -n "${g_backup_storage_root:-}" ]; then
		l_backup_storage_root=$g_backup_storage_root
	else
		l_backup_storage_root=/var/db/zxfer
	fi
	l_backup_storage_root_tab=$(printf '\t')
	l_backup_storage_root_cr=$(printf '\r')
	l_backup_storage_root_lf=$(printf '\n_')
	l_backup_storage_root_lf=${l_backup_storage_root_lf%_}
	case "$l_backup_storage_root" in
	*"$l_backup_storage_root_tab"* | *"$l_backup_storage_root_cr"* | *"$l_backup_storage_root_lf"*)
		zxfer_throw_error "Refusing to use ZXFER_BACKUP_DIR because the backup metadata root must be a single-line absolute path without control whitespace."
		;;
	esac

	case "$l_backup_storage_root" in
	/*)
		g_backup_storage_root=$l_backup_storage_root
		;;
	*)
		zxfer_throw_error "Refusing to use backup metadata root \"$l_backup_storage_root\" because ZXFER_BACKUP_DIR must be an absolute path."
		;;
	esac
}

# Purpose: Initialize the module-owned backup root from public configuration,
# never from inherited internal state.
# Usage: Called once by session bootstrap before backup paths are derived.
zxfer_init_backup_storage_root() {
	g_backup_storage_root=""
	zxfer_refresh_backup_storage_root
}

# Purpose: Reset secure backup-storage scratch before a new read, write, or
# publication flow reuses its result channels.
# Usage: Called by backup-storage operations and by the composed metadata reset.
zxfer_reset_backup_storage_state() {
	g_zxfer_remote_backup_dry_run_shell_command_result=""
	g_zxfer_backup_file_read_result=""
	g_zxfer_backup_stage_dir_result=""
	g_zxfer_backup_stage_file_result=""
	g_zxfer_backup_commit_had_existing_target_result=""
	g_zxfer_backup_commit_rollback_file_result=""
	g_zxfer_backup_local_read_failure_result=""
	g_zxfer_backup_local_write_failure_result=""
}

# Purpose: Return the backup storage directory for dataset tree in the form
# expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_storage_dir_for_dataset_tree() {
	l_dataset=$1
	zxfer_refresh_backup_storage_root

	l_dataset_rel=${l_dataset#/}
	l_dataset_rel=${l_dataset_rel%/}
	if [ "$l_dataset_rel" = "" ]; then
		l_dataset_rel="dataset"
	fi

	printf '%s/%s\n' "$g_backup_storage_root" "$l_dataset_rel"
}

# Purpose: Build the current exact key path used to name backup-metadata files
# for a dataset pair.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows so reads and writes address the same source/destination identity.
zxfer_backup_metadata_file_key() {
	l_source=$1
	l_destination=$2

	l_identity=$(printf '%s\n%s' "$l_source" "$l_destination")
	l_key_hex=$(printf '%s' "$l_identity" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	if [ "$l_key_hex" = "" ]; then
		if [ "$l_identity" != "" ]; then
			return 1
		fi
		l_key_hex="00"
	fi
	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_key_path=$(
		printf '%s\n' "$l_key_hex" |
			"${g_cmd_awk:-awk}" '
				{
					key_path = "h"
					for (i = 1; i <= length($0); i += 48)
					key_path = key_path "/" substr($0, i, 48)
				print key_path
			}'
	) || return "$?"
	[ -n "$l_key_path" ] || return 1
	printf '%s\n' "$l_key_path"
}

# Purpose: Build the retired cksum key string used by older current-format
# backup-metadata filenames.
# Usage: Called only by restore fallback paths so existing v2 backup files keep
# working after current writes move to lossless identity keys.
zxfer_backup_metadata_legacy_file_key() {
	l_source=$1
	l_destination=$2
	l_identity=$(printf '%s\n%s\n' "$l_source" "$l_destination")
	if l_key_cksum=$(printf '%s' "$l_identity" | cksum 2>/dev/null); then
		l_key_checksum=""
		l_key_length=""
		l_key_remainder=""
		IFS=' 	' read -r l_key_checksum l_key_length l_key_remainder <<EOF
$l_key_cksum
EOF
		if [ -n "$l_key_checksum" ] && [ -n "$l_key_length" ]; then
			printf 'k%s.%s\n' "$l_key_checksum" "$l_key_length"
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

# Purpose: Return the backup metadata filename in the form expected by later
# helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_metadata_filename() {
	l_source=$1
	l_destination=$2
	l_key=$(zxfer_backup_metadata_file_key "$l_source" "$l_destination") || return 1
	# Keep current exact-pair identities lossless without exceeding NAME_MAX on
	# long pool/dataset names: the identity is chunked into directories and the
	# leaf file has a fixed bounded name.
	printf '%s.v2/%s/%s.v2\n' "$g_backup_file_extension" "$l_key" "$g_backup_file_extension"
}

# Purpose: Return the retired backup metadata filename used before exact
# dataset-pair identities became lossless.
# Usage: Called by restore fallback helpers only; new writes always use
# zxfer_get_backup_metadata_filename.
zxfer_get_legacy_backup_metadata_filename() {
	l_source=$1
	l_destination=$2
	l_tail=${l_source##*/}
	l_key=$(zxfer_backup_metadata_legacy_file_key "$l_source" "$l_destination") || return 1
	printf '%s.%s.%s\n' "$g_backup_file_extension" "$l_tail" "$l_key"
}

# Purpose: Return the forwarded backup metadata filename in the form expected
# by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_forwarded_backup_metadata_filename() {
	l_dataset_root=$1

	zxfer_get_backup_metadata_filename "$l_dataset_root" "$l_dataset_root"
}

# Purpose: Ensure the local backup directory exists and is ready before the
# flow continues.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before later helpers assume the resource or cache is available.
zxfer_ensure_local_backup_dir() {
	l_ensure_local_backup_dir=$1
	if l_ensure_local_backup_symlink=$(zxfer_find_symlink_path_component \
		"$l_ensure_local_backup_dir"); then
		if [ "$l_ensure_local_backup_symlink" = "$l_ensure_local_backup_dir" ]; then
			zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is a symlink."
		fi
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because path component $l_ensure_local_backup_symlink is a symlink."
	fi
	if [ -L "$l_ensure_local_backup_dir" ]; then
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is a symlink."
	fi
	if [ -e "$l_ensure_local_backup_dir" ] && [ ! -d "$l_ensure_local_backup_dir" ]; then
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is not a directory."
	fi
	if [ ! -d "$l_ensure_local_backup_dir" ]; then
		l_ensure_local_backup_old_umask=$(umask)
		umask 077
		if ! mkdir -p "$l_ensure_local_backup_dir"; then
			umask "$l_ensure_local_backup_old_umask"
			zxfer_throw_error "Error creating secure backup directory $l_ensure_local_backup_dir."
		fi
		umask "$l_ensure_local_backup_old_umask"
	fi
	if ! l_ensure_local_backup_owner_uid=$(zxfer_get_path_owner_uid \
		"$l_ensure_local_backup_dir"); then
		zxfer_throw_error "Cannot determine the owner of backup directory $l_ensure_local_backup_dir."
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_ensure_local_backup_owner_uid"; then
		l_ensure_local_backup_expected_owner=$(zxfer_describe_expected_backup_owner)
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is owned by UID $l_ensure_local_backup_owner_uid instead of $l_ensure_local_backup_expected_owner."
	fi
	if ! chmod 700 "$l_ensure_local_backup_dir"; then
		zxfer_throw_error "Error securing backup directory $l_ensure_local_backup_dir."
	fi
}

# Purpose: Build a remote backup symlink guard command.
# Usage: Shared by remote backup directory prepare and metadata read paths.
zxfer_build_remote_backup_symlink_guard_cmd() {
	l_backup_guard_path_single=$1
	l_backup_guard_reject_status=${2:-1}
	l_backup_guard_kind=$3
	case "$l_backup_guard_kind" in
	directory)
		l_backup_guard_exact_cmd="echo 'Refusing to use symlinked zxfer backup directory.' >&2"
		l_backup_guard_component_cmd="echo \"Refusing to use backup directory \$l_scan_path because path component \$l_scan_candidate is a symlink.\" >&2"
		;;
	metadata)
		l_backup_guard_exact_cmd="echo \"Refusing to use backup metadata \$l_scan_path because it is a symlink.\" >&2"
		l_backup_guard_component_cmd="echo \"Refusing to use backup metadata \$l_scan_path because path component \$l_scan_candidate is a symlink.\" >&2"
		;;
	*) return 1 ;;
	esac

	while IFS= read -r l_backup_guard_script_line ||
		[ -n "$l_backup_guard_script_line" ]; do
		printf '%s\n' "$l_backup_guard_script_line"
	done <<-EOF
		l_scan_path='$l_backup_guard_path_single';
		l_scan_remaining=\$l_scan_path;
		l_scan_candidate='';
		while [ -n "\$l_scan_remaining" ]; do
		  case "\$l_scan_remaining" in
		  /*)
		    if [ "\$l_scan_candidate" = '' ]; then
		      l_scan_candidate=/;
		      l_scan_remaining=\${l_scan_remaining#/};
		      continue;
		    fi;
		    ;;
		  esac;
		  l_scan_component=\${l_scan_remaining%%/*};
		  if [ "\$l_scan_component" = "\$l_scan_remaining" ]; then
		    l_scan_remaining='';
		  else
		    l_scan_remaining=\${l_scan_remaining#*/};
		  fi;
		  [ -n "\$l_scan_component" ] || continue;
		  case "\$l_scan_candidate" in
		  '') l_scan_candidate=\$l_scan_component ;;
		  /) l_scan_candidate=/\$l_scan_component ;;
		  *) l_scan_candidate=\$l_scan_candidate/\$l_scan_component ;;
		  esac;
		  if [ -L "\$l_scan_candidate" ] || [ -h "\$l_scan_candidate" ]; then
		    l_scan_trusted=0;
		    case "\$l_scan_candidate" in
		    /*)
		      l_scan_parent=\${l_scan_candidate%/*};
		      [ -n "\$l_scan_parent" ] || l_scan_parent=/;
		      l_scan_owner='';
		      l_scan_parent_owner='';
		      if command -v stat >/dev/null 2>&1; then
		        l_scan_owner=\$(stat -c '%u' "\$l_scan_candidate" 2>/dev/null);
		        if [ "\$l_scan_owner" = '' ] || printf '%s' "\$l_scan_owner" | grep -q '[^0-9]' >/dev/null 2>&1; then
		          l_scan_owner=\$(stat -f '%u' "\$l_scan_candidate" 2>/dev/null);
		        fi;
		        l_scan_parent_owner=\$(stat -c '%u' "\$l_scan_parent" 2>/dev/null);
		        if [ "\$l_scan_parent_owner" = '' ] || printf '%s' "\$l_scan_parent_owner" | grep -q '[^0-9]' >/dev/null 2>&1; then
		          l_scan_parent_owner=\$(stat -f '%u' "\$l_scan_parent" 2>/dev/null);
		        fi;
		      fi;
		      if [ "\$l_scan_owner" = '0' ] && [ "\$l_scan_parent_owner" = '0' ] && [ "\$l_scan_parent" = '/' ]; then
		        l_scan_ls_path=\$l_scan_parent;
		        case "\$l_scan_ls_path" in -*) l_scan_ls_path=./\$l_scan_ls_path ;; esac;
		        l_scan_ls_line=\$(ls -ldn "\$l_scan_ls_path" 2>/dev/null) || l_scan_ls_line='';
		        if [ "\$l_scan_ls_line" != '' ]; then
		          l_scan_parent_perm=\$(printf '%s\n' "\$l_scan_ls_line" | awk '{print \$1}');
		          case "\$l_scan_parent_perm" in
		          ??????????*)
		            l_scan_group_write=\$(printf '%s' "\$l_scan_parent_perm" | cut -c 6);
		            l_scan_other_write=\$(printf '%s' "\$l_scan_parent_perm" | cut -c 9);
		            l_scan_sticky=\$(printf '%s' "\$l_scan_parent_perm" | cut -c 10);
		            case "\$l_scan_group_write\$l_scan_other_write" in
		            *w*) case "\$l_scan_sticky" in t|T) l_scan_trusted=1 ;; esac ;;
		            *) l_scan_trusted=1 ;;
		            esac;
		            ;;
		          esac;
		        fi;
		      fi;
		      ;;
		    esac;
		    if [ "\$l_scan_trusted" = '1' ]; then
		      continue;
		    fi;
		    if [ "\$l_scan_candidate" = "\$l_scan_path" ]; then
		      $l_backup_guard_exact_cmd;
		    else
		      $l_backup_guard_component_cmd;
		    fi;
		    exit $l_backup_guard_reject_status;
		  fi;
		done;
	EOF
}

# Purpose: Return the remote backup helper dependency path in the form expected
# by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_remote_backup_helper_dependency_path() {
	zxfer_get_effective_dependency_path
}

# Purpose: Wrap a remote backup helper command so it runs under the validated
# remote secure-PATH contract.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before remote helper payloads are sent over SSH.
zxfer_wrap_remote_backup_helper_with_secure_path() {
	l_backup_secure_path_script=$1
	if l_backup_secure_path=$(zxfer_get_remote_backup_helper_dependency_path); then
		:
	else
		l_backup_secure_path_status=$?
		return "$l_backup_secure_path_status"
	fi
	l_backup_secure_path_single=$(zxfer_escape_for_single_quotes "$l_backup_secure_path")

	printf "PATH='%s';\nexport PATH;\n\n%s\n" \
		"$l_backup_secure_path_single" "$l_backup_secure_path_script"
}

# Purpose: Collapse a readable remote backup renderer into the single physical
# command line expected by csh/tcsh login shells before the explicit sh -c
# handoff.
# Usage: Called only at SSH/dry-run transport boundaries. Every renderer ends
# each POSIX command with a semicolon, so replacing line boundaries with spaces
# preserves the rendered program.
zxfer_prepare_remote_backup_transport_script() {
	l_backup_transport_script=${1:-}
	l_backup_transport_result=""

	while IFS= read -r l_backup_transport_line ||
		[ -n "$l_backup_transport_line" ]; do
		[ -n "$l_backup_transport_line" ] || continue
		if [ -n "$l_backup_transport_result" ]; then
			l_backup_transport_result=$l_backup_transport_result' '$l_backup_transport_line
		else
			l_backup_transport_result=$l_backup_transport_line
		fi
	done <<EOF
$l_backup_transport_script
EOF

	[ -n "$l_backup_transport_result" ] || return 1
	printf '%s\n' "$l_backup_transport_result"
}

# Purpose: Build the remote backup helper dependency check command for the next
# execution or comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_helper_dependency_check_cmd() {
	l_backup_dependency_host=$1
	l_backup_dependency_status=$2
	shift 2

	l_backup_dependency_host_single=$(zxfer_escape_for_single_quotes "$l_backup_dependency_host")
	l_backup_dependency_tools=""
	for l_backup_dependency_tool in "$@"; do
		l_backup_dependency_tool_single=$(zxfer_escape_for_single_quotes "$l_backup_dependency_tool")
		l_backup_dependency_tools="$l_backup_dependency_tools '$l_backup_dependency_tool_single'"
	done

	while IFS= read -r l_backup_dependency_script_line ||
		[ -n "$l_backup_dependency_script_line" ]; do
		printf '%s\n' "$l_backup_dependency_script_line"
	done <<-EOF
		l_required_host='$l_backup_dependency_host_single';
		zxfer_require_remote_backup_tool() {
		  l_required_tool=\$1;
		  if command -v "\$l_required_tool" >/dev/null 2>&1; then
		    return 0;
		  fi;
		  printf 'Required dependency "%s" not found on host %s in secure PATH (%s). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary.\n' "\$l_required_tool" "\$l_required_host" "\$PATH" >&2;
		  exit $l_backup_dependency_status;
		};
		for l_required_tool in$l_backup_dependency_tools; do
		  zxfer_require_remote_backup_tool "\$l_required_tool";
		done;
	EOF
}

# Purpose: Build the remote backup directory prepare command for the next
# execution or comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_dir_prepare_cmd() {
	l_backup_prepare_dir=$1
	l_backup_prepare_host=$2
	l_backup_prepare_dependency_status=${3:-99}
	l_backup_prepare_failure_status=${4:-92}

	l_backup_prepare_dir_single=$(zxfer_escape_for_single_quotes "$l_backup_prepare_dir")
	l_backup_prepare_ls_path=$l_backup_prepare_dir
	case "$l_backup_prepare_ls_path" in
	-*)
		l_backup_prepare_ls_path=./$l_backup_prepare_ls_path
		;;
	esac
	l_backup_prepare_ls_single=$(zxfer_escape_for_single_quotes "$l_backup_prepare_ls_path")
	l_backup_prepare_symlink_guard=$(zxfer_build_remote_backup_symlink_guard_cmd \
		"$l_backup_prepare_dir_single" "$l_backup_prepare_failure_status" directory) || return "$?"
	l_backup_prepare_dependency_check=$(zxfer_build_remote_backup_helper_dependency_check_cmd \
		"$l_backup_prepare_host" "$l_backup_prepare_dependency_status" \
		mkdir chmod id grep ls awk cut) || return "$?"

	l_backup_prepare_script=$(
		while IFS= read -r l_backup_prepare_script_line ||
			[ -n "$l_backup_prepare_script_line" ]; do
			printf '%s\n' "$l_backup_prepare_script_line"
		done <<-EOF
			$l_backup_prepare_dependency_check

			$l_backup_prepare_symlink_guard

			if [ -L '$l_backup_prepare_dir_single' ]; then
			  echo 'Refusing to use symlinked zxfer backup directory.' >&2;
			  exit $l_backup_prepare_failure_status;
			fi;
			if [ -e '$l_backup_prepare_dir_single' ] && [ ! -d '$l_backup_prepare_dir_single' ]; then
			  echo 'Backup path exists but is not a directory.' >&2;
			  exit $l_backup_prepare_failure_status;
			fi;

			umask 077;
			if ! mkdir -p '$l_backup_prepare_dir_single'; then
			  echo 'Error creating secure backup directory.' >&2;
			  exit $l_backup_prepare_failure_status;
			fi;
			if ! chmod 700 '$l_backup_prepare_dir_single'; then
			  echo 'Error securing backup directory.' >&2;
			  exit $l_backup_prepare_failure_status;
			fi;

			l_expected_uid=\$(id -u);
			l_dir_uid='';
			if command -v stat >/dev/null 2>&1; then
			  l_dir_uid=\$(stat -c '%u' '$l_backup_prepare_dir_single' 2>/dev/null);
			  if [ "\$l_dir_uid" = '' ] || printf '%s' "\$l_dir_uid" | grep -q '[^0-9]' >/dev/null 2>&1; then
			    l_dir_uid=\$(stat -f '%u' '$l_backup_prepare_dir_single' 2>/dev/null);
			  fi;
			fi;
			if [ "\$l_dir_uid" = '' ] || printf '%s' "\$l_dir_uid" | grep -q '[^0-9]' >/dev/null 2>&1; then
			  l_ls_line=\$(ls -ldn '$l_backup_prepare_ls_single' 2>/dev/null) || l_ls_line='';
			  if [ "\$l_ls_line" != '' ]; then
			    l_dir_uid=\$(printf '%s\n' "\$l_ls_line" | awk '{print \$3}');
			  fi;
			fi;
			if [ "\$l_dir_uid" = '' ]; then
			  echo 'Unable to determine backup directory owner.' >&2;
			  exit $l_backup_prepare_failure_status;
			fi;
			if [ "\$l_dir_uid" != 0 ] && [ "\$l_dir_uid" != "\$l_expected_uid" ]; then
			  echo 'Backup directory must be owned by root or the ssh user.' >&2;
			  exit $l_backup_prepare_failure_status;
			fi;
		EOF
	) || return "$?"

	zxfer_wrap_remote_backup_helper_with_secure_path "$l_backup_prepare_script"
}

# Purpose: Ensure the remote backup directory exists and is ready before the
# flow continues.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before later helpers assume the resource or cache is available.
zxfer_ensure_remote_backup_dir() {
	l_ensure_backup_dir=$1
	l_ensure_backup_host=$2
	l_ensure_backup_profile_side=${3:-}

	[ "$l_ensure_backup_host" = "" ] && return

	l_ensure_backup_dependency_status=99
	l_ensure_backup_prepare_status=92
	l_ensure_backup_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
	l_ensure_backup_script=$(zxfer_build_remote_backup_dir_prepare_cmd \
		"$l_ensure_backup_dir" "$l_ensure_backup_host" \
		"$l_ensure_backup_dependency_status" "$l_ensure_backup_prepare_status") || return "$?"
	l_ensure_backup_transport_script=$(zxfer_prepare_remote_backup_transport_script \
		"$l_ensure_backup_script") || return "$?"
	l_ensure_backup_shell_cmd=$(zxfer_build_remote_sh_c_command \
		"$l_ensure_backup_transport_script") || return "$?"
	if zxfer_capture_remote_probe_output "$l_ensure_backup_host" \
		"$l_ensure_backup_shell_cmd" "$l_ensure_backup_profile_side"; then
		l_ensure_backup_remote_status=0
	else
		l_ensure_backup_remote_status=$?
	fi
	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		zxfer_throw_remote_backup_capture_error "$l_ensure_backup_host" \
			"preparing backup directory $l_ensure_backup_dir"
	fi
	if [ "$l_ensure_backup_remote_status" -eq "$l_ensure_backup_dependency_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_set_failure_class dependency
		zxfer_throw_error "Required remote backup-directory helper dependency not found on host $l_ensure_backup_host in secure PATH ($l_ensure_backup_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ "$l_ensure_backup_remote_status" -eq "$l_ensure_backup_prepare_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_throw_error "Error preparing backup directory on $l_ensure_backup_host."
	fi
	if [ "$l_ensure_backup_remote_status" -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
			zxfer_throw_error "Failed to contact target host $l_ensure_backup_host while preparing backup directory $l_ensure_backup_dir. Review prior stderr for the transport or authentication error."
		fi
		zxfer_throw_error "Error preparing backup directory on $l_ensure_backup_host."
	fi
}

# Purpose: Clean up the backup metadata stage directory that this module
# created or tracks.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows on success and failure paths so temporary state does not linger.
zxfer_cleanup_backup_metadata_stage_dir() {
	l_stage_dir=$1

	[ -n "$l_stage_dir" ] || return 0
	zxfer_cleanup_runtime_artifact_path "$l_stage_dir" >/dev/null 2>&1 || true
}

# Purpose: Register the backup metadata runtime artifact path with the tracking
# state owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows so cleanup and later lookups can find the live resource.
zxfer_register_backup_metadata_runtime_artifact_path() {
	l_register_backup_artifact_path=$1

	[ -n "$l_register_backup_artifact_path" ] || return 0
	zxfer_register_runtime_artifact_path "$l_register_backup_artifact_path"
}

# Purpose: Remove the backup metadata runtime artifact path from the tracking
# state owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after the tracked resource has completed or been cleaned up.
zxfer_unregister_backup_metadata_runtime_artifact_path() {
	l_unregister_backup_artifact_path=$1

	[ -n "$l_unregister_backup_artifact_path" ] || return 0
	zxfer_unregister_runtime_artifact_path "$l_unregister_backup_artifact_path"
}

# Purpose: Remove the local backup metadata path if present from the current
# working set while preserving the module's special-case rules.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when filtering logic must trim staged data before later reconciliation
# or apply steps run.
zxfer_remove_local_backup_metadata_path_if_present() {
	l_path=$1

	[ -n "$l_path" ] || return 0
	if [ ! -e "$l_path" ] && [ ! -L "$l_path" ] && [ ! -h "$l_path" ]; then
		return 0
	fi
	if rm -f "$l_path" 2>/dev/null; then
		return 0
	else
		l_status=$?
		return "$l_status"
	fi
}

# Purpose: Move the local backup metadata path through the controlled local
# publish path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when staged local data is ready for its final path.
zxfer_move_local_backup_metadata_path() {
	l_source_path=$1
	l_target_path=$2

	if mv -f "$l_source_path" "$l_target_path" 2>/dev/null; then
		return 0
	else
		l_status=$?
		return "$l_status"
	fi
}

# Purpose: Create the backup metadata stage directory for path using the safety
# checks owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_backup_metadata_stage_dir_for_path() {
	l_backup_stage_path=$1
	l_backup_stage_prefix=${2:-zxfer-backup-stage}

	g_zxfer_backup_stage_dir_result=""
	l_backup_stage_parent=$(zxfer_get_path_parent_dir "$l_backup_stage_path") || return "$?"
	if [ ! -d "$l_backup_stage_parent" ]; then
		return 1
	fi

	l_backup_stage_old_umask=$(umask)
	umask 077
	if l_backup_stage_dir=$(mktemp -d "$l_backup_stage_parent/.$l_backup_stage_prefix.XXXXXX" 2>/dev/null); then
		l_backup_stage_status=0
	else
		l_backup_stage_status=$?
	fi
	umask "$l_backup_stage_old_umask"
	[ "$l_backup_stage_status" -eq 0 ] || return "$l_backup_stage_status"
	if ! zxfer_register_backup_metadata_runtime_artifact_path "$l_backup_stage_dir"; then
		rmdir "$l_backup_stage_dir" 2>/dev/null || :
		return 1
	fi

	g_zxfer_backup_stage_dir_result=$l_backup_stage_dir
	printf '%s\n' "$l_backup_stage_dir"
}

# Purpose: Check whether the backup metadata path uses trusted nonwritable
# parent.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a boolean answer about the backup metadata
# path.
zxfer_backup_metadata_path_uses_trusted_nonwritable_parent() {
	l_backup_io_path=$1

	l_backup_io_parent=$(zxfer_get_path_parent_dir "$l_backup_io_path") || return 1
	l_backup_io_parent=$(zxfer_validate_temp_root_candidate "$l_backup_io_parent") || return 1

	[ ! -w "$l_backup_io_parent" ]
}

# Purpose: Require the backup write target path before the surrounding flow
# continues.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers should stop immediately if the precondition is not
# met.
zxfer_require_backup_write_target_path() {
	l_path=$1

	if [ -L "$l_path" ] || [ -h "$l_path" ]; then
		zxfer_throw_error "Refusing to write backup metadata $l_path because it is a symlink."
	fi
	if [ -e "$l_path" ] && [ ! -f "$l_path" ]; then
		zxfer_throw_error "Refusing to write backup metadata $l_path because it is not a regular file."
	fi
}

# Purpose: Prepare the local backup file stage before the surrounding flow uses
# it.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows once prerequisites are known but before live work depends on the
# prepared state.
zxfer_prepare_local_backup_file_stage() {
	l_backup_file_path=$1
	l_rendered_backup_contents=$2
	g_zxfer_backup_local_write_failure_result=""
	g_zxfer_backup_stage_dir_result=""
	g_zxfer_backup_stage_file_result=""

	if zxfer_create_backup_metadata_stage_dir_for_path "$l_backup_file_path" "zxfer-backup-write" >/dev/null; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_write_failure_result=staging
		return "$l_status"
	fi
	l_prepare_local_backup_file_stage_stage_dir=$g_zxfer_backup_stage_dir_result
	l_stage_file="$l_prepare_local_backup_file_stage_stage_dir/backup.write"
	if (
		umask 077
		printf '%s\n' "$l_rendered_backup_contents" >"$l_stage_file"
	); then
		:
	else
		l_status=$?
		g_zxfer_backup_local_write_failure_result=staging
		g_zxfer_backup_stage_dir_result=""
		g_zxfer_backup_stage_file_result=""
		zxfer_cleanup_backup_metadata_stage_dir "$l_prepare_local_backup_file_stage_stage_dir"
		return "$l_status"
	fi
	if chmod 600 "$l_stage_file"; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_write_failure_result=staging
		g_zxfer_backup_stage_dir_result=""
		g_zxfer_backup_stage_file_result=""
		zxfer_cleanup_backup_metadata_stage_dir "$l_prepare_local_backup_file_stage_stage_dir"
		return "$l_status"
	fi

	g_zxfer_backup_stage_dir_result=$l_prepare_local_backup_file_stage_stage_dir
	g_zxfer_backup_stage_file_result=$l_stage_file
	printf '%s\n' "$l_prepare_local_backup_file_stage_stage_dir"
	printf '%s\n' "$l_stage_file"
}

# Purpose: Commit the local backup file stage once staged validation has
# already succeeded.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after the staged backup or cache payload is ready to become live.
zxfer_commit_local_backup_file_stage() {
	l_backup_file_path=$1
	l_stage_file=$2
	g_zxfer_backup_commit_had_existing_target_result=""
	g_zxfer_backup_commit_rollback_file_result=""

	if [ -L "$l_backup_file_path" ] || [ -h "$l_backup_file_path" ]; then
		return 1
	fi
	if [ -e "$l_backup_file_path" ] && [ ! -f "$l_backup_file_path" ]; then
		return 1
	fi

	l_had_existing_target=0
	l_rollback_file=""
	if [ -e "$l_backup_file_path" ]; then
		l_had_existing_target=1
		l_backup_parent=$(zxfer_get_path_parent_dir "$l_backup_file_path") || return "$?"
		l_rollback_file=$(mktemp "$l_backup_parent/.zxfer-backup-rollback.XXXXXX" 2>/dev/null) ||
			return "$?"
		if zxfer_move_local_backup_metadata_path "$l_backup_file_path" "$l_rollback_file"; then
			:
		else
			l_commit_local_backup_file_stage_status=$?
			zxfer_remove_local_backup_metadata_path_if_present "$l_rollback_file" >/dev/null 2>&1 || :
			return "$l_commit_local_backup_file_stage_status"
		fi
	fi

	if zxfer_move_local_backup_metadata_path "$l_stage_file" "$l_backup_file_path"; then
		:
	else
		l_stage_move_status=$?
		if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
			if zxfer_move_local_backup_metadata_path "$l_rollback_file" "$l_backup_file_path"; then
				:
			else
				l_restore_status=$?
				g_zxfer_backup_local_write_failure_result=rollback
				if [ -e "$l_rollback_file" ]; then
					zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path" >/dev/null 2>&1 || :
				fi
				return "$l_restore_status"
			fi
			if [ -e "$l_rollback_file" ]; then
				zxfer_remove_local_backup_metadata_path_if_present "$l_rollback_file" >/dev/null 2>&1 || :
			fi
		else
			zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path" >/dev/null 2>&1 || :
		fi
		return "$l_stage_move_status"
	fi

	g_zxfer_backup_commit_had_existing_target_result=$l_had_existing_target
	g_zxfer_backup_commit_rollback_file_result=$l_rollback_file
	printf '%s\n' "$l_had_existing_target"
	printf '%s\n' "$l_rollback_file"
}

# Purpose: Rollback the local backup file commit to the last safe state this
# module recognizes.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer detects divergence and must re-establish a safe base.
zxfer_rollback_local_backup_file_commit() {
	l_backup_file_path=$1
	l_had_existing_target=$2
	l_rollback_file=$3

	if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
		zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path" || return "$?"
		zxfer_move_local_backup_metadata_path "$l_rollback_file" "$l_backup_file_path" || return "$?"
		zxfer_unregister_backup_metadata_runtime_artifact_path "$l_rollback_file"
		return 0
	fi

	zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path"
}

# Purpose: Finalize the local backup file commit once all prerequisites have
# succeeded.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after staged or deferred work is ready to become the module's final
# result.
zxfer_finalize_local_backup_file_commit() {
	l_had_existing_target=$1
	l_rollback_file=$2

	if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
		if zxfer_remove_local_backup_metadata_path_if_present "$l_rollback_file"; then
			zxfer_unregister_backup_metadata_runtime_artifact_path "$l_rollback_file"
			return 0
		else
			l_finalize_local_backup_file_commit_status=$?
			return "$l_finalize_local_backup_file_commit_status"
		fi
	fi

	return 0
}

# Purpose: Raise the backup write rollback error through zxfer's
# structured failure reporting path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_backup_write_rollback_error() {
	zxfer_throw_error "Error writing backup file and restoring backup metadata rollback state. Inspect rollback files under ZXFER_BACKUP_DIR for manual recovery."
}

# Purpose: Raise the remote backup transport error through zxfer's structured
# failure reporting path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_remote_backup_transport_error() {
	l_host=$1
	l_action=$2

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		zxfer_emit_remote_probe_failure_message >&2
	fi
	zxfer_throw_error "Failed to contact target host $l_host while $l_action. Review prior stderr for the transport or authentication error."
}

# Purpose: Raise the remote backup capture error through zxfer's structured
# failure reporting path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_remote_backup_capture_error() {
	l_host=$1
	l_action=$2

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		zxfer_emit_remote_probe_failure_message >&2
	fi
	zxfer_throw_error "Failed to reload local remote helper capture while $l_action on host $l_host."
}

# Purpose: Raise remote backup write statuses through the shared backup
# metadata reporting paths.
# Usage: Called after single-file and pair remote metadata write helpers
# return so both flows preserve the same stderr text and failure classes.
zxfer_throw_remote_backup_write_status() {
	l_remote_write_status=$1
	l_remote_dependency_status=$2
	l_remote_write_failure_status=$3
	l_remote_rollback_failure_status=$4
	l_throw_remote_backup_write_status_host=$5
	l_throw_remote_backup_write_status_action=$6
	l_dependency_path=$7

	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		zxfer_throw_remote_backup_capture_error "$l_throw_remote_backup_write_status_host" "$l_throw_remote_backup_write_status_action"
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_dependency_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_set_failure_class dependency
		zxfer_throw_error "Required remote backup-write helper dependency not found on host $l_throw_remote_backup_write_status_host in secure PATH ($l_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ -n "$l_remote_rollback_failure_status" ] &&
		[ "$l_remote_write_status" -eq "$l_remote_rollback_failure_status" ]; then
		zxfer_throw_backup_write_rollback_error
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_write_failure_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
	fi
	if [ "$l_remote_write_status" -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_throw_remote_backup_transport_error "$l_throw_remote_backup_write_status_host" "$l_throw_remote_backup_write_status_action"
		fi
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
	fi
	return 0
}

# Purpose: Run the remote backup helper with payload through the controlled
# execution path owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows once planning is complete and zxfer is ready to execute the action.
zxfer_run_remote_backup_helper_with_payload() {
	l_backup_helper_host=$1
	l_backup_helper_shell_cmd=$2
	l_backup_helper_payload=$3
	l_backup_helper_profile_side=${4:-}

	zxfer_reset_remote_probe_capture_state

	if l_backup_helper_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host \
		"$l_backup_helper_host"); then
		:
	else
		l_backup_helper_transport_status=$?
		zxfer_profile_record_ssh_invocation \
			"$l_backup_helper_host" "$l_backup_helper_profile_side"
		zxfer_throw_error "$l_backup_helper_transport_tokens" \
			"$l_backup_helper_transport_status"
	fi

	zxfer_create_private_temp_dir "zxfer-remote-backup-helper" >/dev/null
	l_backup_helper_stage_status=$?
	if [ "$l_backup_helper_stage_status" -ne 0 ]; then
		zxfer_throw_error "Error creating temporary file."
	fi
	l_backup_helper_stage_dir=$g_zxfer_runtime_artifact_path_result
	l_backup_helper_stdin_path="$l_backup_helper_stage_dir/stdin"
	l_backup_helper_stdout_path="$l_backup_helper_stage_dir/stdout"
	l_backup_helper_stderr_path="$l_backup_helper_stage_dir/stderr"

	if ! zxfer_write_runtime_artifact_file \
		"$l_backup_helper_stdin_path" "$l_backup_helper_payload"; then
		zxfer_cleanup_runtime_artifact_path "$l_backup_helper_stage_dir"
		zxfer_throw_error "Error creating temporary file."
	fi

	if zxfer_invoke_ssh_shell_command_for_host \
		"$l_backup_helper_host" "$l_backup_helper_shell_cmd" \
		"$l_backup_helper_profile_side" <"$l_backup_helper_stdin_path" \
		>"$l_backup_helper_stdout_path" 2>"$l_backup_helper_stderr_path"; then
		l_backup_helper_remote_status=0
	else
		l_backup_helper_remote_status=$?
	fi

	zxfer_load_remote_probe_capture_files "remote backup helper" \
		"$l_backup_helper_stdout_path" "$l_backup_helper_stderr_path"
	l_backup_helper_capture_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_backup_helper_stage_dir"
	if [ "$l_backup_helper_capture_status" -ne 0 ]; then
		return "$l_backup_helper_capture_status"
	fi
	return "$l_backup_helper_remote_status"
}

# Purpose: Write the local backup file pair atomically in the normalized form
# later zxfer steps expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
zxfer_write_local_backup_file_pair_atomically() {
	l_primary_backup_file_path=$1
	l_primary_rendered_backup_contents=$2
	l_forwarded_backup_file_path=$3
	l_forwarded_backup_contents=$4

	g_zxfer_backup_local_write_failure_result=""
	zxfer_prepare_local_backup_file_stage "$l_primary_backup_file_path" "$l_primary_rendered_backup_contents" >/dev/null ||
		return "$?"
	l_primary_stage_dir=$g_zxfer_backup_stage_dir_result
	l_primary_stage_file=$g_zxfer_backup_stage_file_result

	zxfer_prepare_local_backup_file_stage "$l_forwarded_backup_file_path" "$l_forwarded_backup_contents" >/dev/null || {
		l_write_local_backup_file_pair_atomically_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		return "$l_write_local_backup_file_pair_atomically_status"
	}
	l_forwarded_stage_dir=$g_zxfer_backup_stage_dir_result
	l_forwarded_stage_file=$g_zxfer_backup_stage_file_result

	zxfer_commit_local_backup_file_stage "$l_forwarded_backup_file_path" "$l_forwarded_stage_file" >/dev/null || {
		l_write_local_backup_file_pair_atomically_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_write_local_backup_file_pair_atomically_status"
	}
	l_forwarded_had_existing_target=$g_zxfer_backup_commit_had_existing_target_result
	l_forwarded_rollback_file=$g_zxfer_backup_commit_rollback_file_result

	if zxfer_commit_local_backup_file_stage "$l_primary_backup_file_path" "$l_primary_stage_file" >/dev/null; then
		:
	else
		l_write_local_backup_file_pair_atomically_status=$?
		if ! zxfer_rollback_local_backup_file_commit "$l_forwarded_backup_file_path" "$l_forwarded_had_existing_target" "$l_forwarded_rollback_file" >/dev/null 2>&1; then
			zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
			zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
			return 2
		fi
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_write_local_backup_file_pair_atomically_status"
	fi
	l_primary_had_existing_target=$g_zxfer_backup_commit_had_existing_target_result
	l_primary_rollback_file=$g_zxfer_backup_commit_rollback_file_result
	if [ "$l_forwarded_had_existing_target" -eq 1 ] &&
		[ -n "$l_forwarded_rollback_file" ]; then
		zxfer_register_backup_metadata_runtime_artifact_path \
			"$l_forwarded_rollback_file"
	fi
	if [ "$l_primary_had_existing_target" -eq 1 ] &&
		[ -n "$l_primary_rollback_file" ]; then
		zxfer_register_backup_metadata_runtime_artifact_path \
			"$l_primary_rollback_file"
	fi

	zxfer_finalize_local_backup_file_commit "$l_forwarded_had_existing_target" "$l_forwarded_rollback_file" || {
		l_write_local_backup_file_pair_atomically_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_write_local_backup_file_pair_atomically_status"
	}
	zxfer_finalize_local_backup_file_commit "$l_primary_had_existing_target" "$l_primary_rollback_file" || {
		l_write_local_backup_file_pair_atomically_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_write_local_backup_file_pair_atomically_status"
	}
	zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
	zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
}

# Purpose: Write the local backup file atomically in the normalized form later
# zxfer steps expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
zxfer_write_local_backup_file_atomically() {
	l_write_local_backup_file_atomically_backup_file_path=$1
	l_write_local_backup_file_atomically_rendered_backup_contents=$2

	g_zxfer_backup_local_write_failure_result=""
	zxfer_prepare_local_backup_file_stage "$l_write_local_backup_file_atomically_backup_file_path" "$l_write_local_backup_file_atomically_rendered_backup_contents" >/dev/null ||
		return "$?"
	l_write_local_backup_file_atomically_stage_dir=$g_zxfer_backup_stage_dir_result
	l_write_local_backup_file_atomically_stage_file=$g_zxfer_backup_stage_file_result
	zxfer_commit_local_backup_file_stage "$l_write_local_backup_file_atomically_backup_file_path" "$l_write_local_backup_file_atomically_stage_file" >/dev/null || {
		l_write_local_backup_file_atomically_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_write_local_backup_file_atomically_stage_dir"
		return "$l_write_local_backup_file_atomically_status"
	}
	l_write_local_backup_file_atomically_had_existing_target=$g_zxfer_backup_commit_had_existing_target_result
	l_write_local_backup_file_atomically_rollback_file=$g_zxfer_backup_commit_rollback_file_result
	if [ "$l_write_local_backup_file_atomically_had_existing_target" -eq 1 ] && [ -n "$l_write_local_backup_file_atomically_rollback_file" ]; then
		# Rollback files only become disposable after the staged backup is live.
		zxfer_register_backup_metadata_runtime_artifact_path "$l_write_local_backup_file_atomically_rollback_file"
	fi
	zxfer_finalize_local_backup_file_commit "$l_write_local_backup_file_atomically_had_existing_target" "$l_write_local_backup_file_atomically_rollback_file" || {
		l_write_local_backup_file_atomically_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_write_local_backup_file_atomically_stage_dir"
		return "$l_write_local_backup_file_atomically_status"
	}
	zxfer_cleanup_backup_metadata_stage_dir "$l_write_local_backup_file_atomically_stage_dir"
}

# Purpose: Render the reusable remote target guard for backup metadata writes.
# Usage: Called by remote single-file and pair write command builders before
# they stage or publish backup metadata on the target host.
zxfer_render_remote_backup_target_write_guard_cmd() {
	l_backup_target_guard_path_single=$1
	l_backup_target_guard_failure_status=$2

	while IFS= read -r l_backup_target_guard_line ||
		[ -n "$l_backup_target_guard_line" ]; do
		printf '%s\n' "$l_backup_target_guard_line"
	done <<-EOF
		if [ -L '$l_backup_target_guard_path_single' ] || [ -h '$l_backup_target_guard_path_single' ]; then
		  echo 'Refusing to write backup metadata because the target is a symlink.' >&2;
		  exit $l_backup_target_guard_failure_status;
		fi;
		if [ -e '$l_backup_target_guard_path_single' ] && [ ! -f '$l_backup_target_guard_path_single' ]; then
		  echo 'Refusing to write backup metadata because the target is not a regular file.' >&2;
		  exit $l_backup_target_guard_failure_status;
		fi;
	EOF
}

# Purpose: Build the remote backup write command for the next execution or
# comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_write_cmd() {
	l_backup_write_dir=$1
	l_backup_write_path=$2
	l_backup_write_host=$3
	l_backup_write_helper_safe=$4
	l_backup_write_dependency_status=${5:-99}
	l_backup_write_failure_status=${6:-92}

	l_backup_write_dir_single=$(zxfer_escape_for_single_quotes "$l_backup_write_dir")
	l_backup_write_path_single=$(zxfer_escape_for_single_quotes "$l_backup_write_path")
	l_backup_write_dependency_check=$(zxfer_build_remote_backup_helper_dependency_check_cmd \
		"$l_backup_write_host" "$l_backup_write_dependency_status" \
		mktemp chmod mv rm rmdir) || return "$?"
	l_backup_write_target_guard=$(zxfer_render_remote_backup_target_write_guard_cmd \
		"$l_backup_write_path_single" "$l_backup_write_failure_status") || return "$?"

	l_backup_write_script=$(
		while IFS= read -r l_backup_write_script_line ||
			[ -n "$l_backup_write_script_line" ]; do
			printf '%s\n' "$l_backup_write_script_line"
		done <<-EOF
			$l_backup_write_dependency_check

			cleanup_stage() {
			  rm -f "\$l_stage_file";
			  rmdir "\$l_stage_dir" 2>/dev/null || true;
			};

			$l_backup_write_target_guard

			umask 077;
			l_stage_dir=\$(mktemp -d '$l_backup_write_dir_single/.zxfer-backup-write.XXXXXX' 2>/dev/null) || exit $l_backup_write_failure_status;
			l_stage_file="\$l_stage_dir/backup.write";
			if ! $l_backup_write_helper_safe >"\$l_stage_file"; then
			  cleanup_stage;
			  exit $l_backup_write_failure_status;
			fi;
			if ! chmod 600 "\$l_stage_file"; then
			  cleanup_stage;
			  exit $l_backup_write_failure_status;
			fi;

			if [ -L '$l_backup_write_path_single' ] || [ -h '$l_backup_write_path_single' ]; then
			  cleanup_stage;
			  exit $l_backup_write_failure_status;
			fi;
			if [ -e '$l_backup_write_path_single' ] && [ ! -f '$l_backup_write_path_single' ]; then
			  cleanup_stage;
			  exit $l_backup_write_failure_status;
			fi;
			if ! mv -f "\$l_stage_file" '$l_backup_write_path_single'; then
			  cleanup_stage;
			  exit $l_backup_write_failure_status;
			fi;
			rmdir "\$l_stage_dir" 2>/dev/null || true;
		EOF
	) || return "$?"

	zxfer_wrap_remote_backup_helper_with_secure_path "$l_backup_write_script"
}

# Purpose: Render cleanup and forwarded-rollback functions for one remote pair
# write transaction.
# Usage: Called only by zxfer_build_remote_backup_pair_write_cmd.
zxfer_render_remote_backup_pair_transaction_functions() {
	l_backup_pair_functions_forwarded_path_single=$1

	while IFS= read -r l_backup_pair_functions_line ||
		[ -n "$l_backup_pair_functions_line" ]; do
		printf '%s\n' "$l_backup_pair_functions_line"
	done <<-EOF
		create_backup_temp() {
		  mktemp "\$@";
		};
		cleanup_stages() {
		  rm -f "\$l_primary_stage_file" "\$l_forwarded_stage_file" 2>/dev/null || true;
		  rmdir "\$l_primary_stage_dir" "\$l_forwarded_stage_dir" 2>/dev/null || true;
		};
		rollback_forwarded() {
		  rm -f '$l_backup_pair_functions_forwarded_path_single' 2>/dev/null || true;
		  if [ "\${l_forwarded_had_existing:-0}" -eq 1 ] && [ "\${l_forwarded_rollback_file:-}" != '' ]; then
		    if ! mv -f "\$l_forwarded_rollback_file" '$l_backup_pair_functions_forwarded_path_single' 2>/dev/null; then
		      return 1;
		    fi;
		    if [ -e "\$l_forwarded_rollback_file" ]; then
		      rm -f "\$l_forwarded_rollback_file" 2>/dev/null || true;
		    fi;
		  fi;
		  return 0;
		};
	EOF
}

# Purpose: Render staging, payload splitting, and mode hardening for one remote
# pair write transaction.
# Usage: Called only by zxfer_build_remote_backup_pair_write_cmd.
zxfer_render_remote_backup_pair_stage_setup() {
	l_backup_pair_stage_primary_dir_single=$1
	l_backup_pair_stage_forwarded_dir_single=$2
	l_backup_pair_stage_split_line_single=$3
	l_backup_pair_stage_failure_status=$4

	while IFS= read -r l_backup_pair_stage_line ||
		[ -n "$l_backup_pair_stage_line" ]; do
		printf '%s\n' "$l_backup_pair_stage_line"
	done <<-EOF
		l_primary_stage_dir=\$(create_backup_temp -d '$l_backup_pair_stage_primary_dir_single/.zxfer-backup-write.XXXXXX' 2>/dev/null) || exit $l_backup_pair_stage_failure_status;
		l_primary_stage_file="\$l_primary_stage_dir/backup.write";
		l_forwarded_stage_dir=\$(create_backup_temp -d '$l_backup_pair_stage_forwarded_dir_single/.zxfer-backup-write.XXXXXX' 2>/dev/null) || {
		  cleanup_stages;
		  exit $l_backup_pair_stage_failure_status;
		};
		l_forwarded_stage_file="\$l_forwarded_stage_dir/backup.write";
		if ! awk -v split_line='$l_backup_pair_stage_split_line_single' -v primary_file="\$l_primary_stage_file" -v forwarded_file="\$l_forwarded_stage_file" 'BEGIN { current = primary_file; saw_split = 0 } \$0 == split_line { current = forwarded_file; saw_split = 1; next } { print > current } END { if (!saw_split) exit 1 }'; then
		  cleanup_stages;
		  exit $l_backup_pair_stage_failure_status;
		fi;
		if ! chmod 600 "\$l_primary_stage_file"; then
		  cleanup_stages;
		  exit $l_backup_pair_stage_failure_status;
		fi;
		if ! chmod 600 "\$l_forwarded_stage_file"; then
		  cleanup_stages;
		  exit $l_backup_pair_stage_failure_status;
		fi;
	EOF
}

# Purpose: Render publication and rollback setup for the forwarded half of a
# remote pair write transaction.
# Usage: Called only by zxfer_build_remote_backup_pair_write_cmd.
zxfer_render_remote_backup_pair_forwarded_publish() {
	l_backup_pair_forwarded_dir_single=$1
	l_backup_pair_forwarded_path_single=$2
	l_backup_pair_forwarded_failure_status=$3
	l_backup_pair_forwarded_rollback_status=$4

	while IFS= read -r l_backup_pair_forwarded_line ||
		[ -n "$l_backup_pair_forwarded_line" ]; do
		printf '%s\n' "$l_backup_pair_forwarded_line"
	done <<-EOF
		l_forwarded_had_existing=0;
		l_forwarded_rollback_file='';
		if [ -e '$l_backup_pair_forwarded_path_single' ]; then
		  l_forwarded_had_existing=1;
		  l_forwarded_rollback_file=\$(create_backup_temp '$l_backup_pair_forwarded_dir_single/.zxfer-backup-rollback.XXXXXX' 2>/dev/null) || {
		    cleanup_stages;
		    exit $l_backup_pair_forwarded_failure_status;
		  };
		  if ! mv -f '$l_backup_pair_forwarded_path_single' "\$l_forwarded_rollback_file"; then
		    rm -f "\$l_forwarded_rollback_file" 2>/dev/null || true;
		    cleanup_stages;
		    exit $l_backup_pair_forwarded_failure_status;
		  fi;
		fi;
		if ! mv -f "\$l_forwarded_stage_file" '$l_backup_pair_forwarded_path_single'; then
		  if ! rollback_forwarded; then
		    cleanup_stages;
		    exit $l_backup_pair_forwarded_rollback_status;
		  fi;
		  cleanup_stages;
		  exit $l_backup_pair_forwarded_failure_status;
		fi;
	EOF
}

# Purpose: Render publication and rollback coordination for the primary half of
# a remote pair write transaction.
# Usage: Called only by zxfer_build_remote_backup_pair_write_cmd.
zxfer_render_remote_backup_pair_primary_publish() {
	l_backup_pair_primary_dir_single=$1
	l_backup_pair_primary_path_single=$2
	l_backup_pair_primary_failure_status=$3
	l_backup_pair_primary_rollback_status=$4

	while IFS= read -r l_backup_pair_primary_line ||
		[ -n "$l_backup_pair_primary_line" ]; do
		printf '%s\n' "$l_backup_pair_primary_line"
	done <<-EOF
		l_primary_had_existing=0;
		l_primary_rollback_file='';
		if [ -e '$l_backup_pair_primary_path_single' ]; then
		  l_primary_had_existing=1;
		  l_primary_rollback_file=\$(create_backup_temp '$l_backup_pair_primary_dir_single/.zxfer-backup-rollback.XXXXXX' 2>/dev/null) || {
		    if ! rollback_forwarded; then
		      cleanup_stages;
		      exit $l_backup_pair_primary_rollback_status;
		    fi;
		    cleanup_stages;
		    exit $l_backup_pair_primary_failure_status;
		  };
		  if ! mv -f '$l_backup_pair_primary_path_single' "\$l_primary_rollback_file"; then
		    rm -f "\$l_primary_rollback_file" 2>/dev/null || true;
		    if ! rollback_forwarded; then
		      cleanup_stages;
		      exit $l_backup_pair_primary_rollback_status;
		    fi;
		    cleanup_stages;
		    exit $l_backup_pair_primary_failure_status;
		  fi;
		fi;
		if ! mv -f "\$l_primary_stage_file" '$l_backup_pair_primary_path_single'; then
		  l_primary_restore_failed=0;
		  if [ "\$l_primary_had_existing" -eq 1 ] && [ "\$l_primary_rollback_file" != '' ]; then
		    if ! mv -f "\$l_primary_rollback_file" '$l_backup_pair_primary_path_single' 2>/dev/null; then
		      l_primary_restore_failed=1;
		    elif [ -e "\$l_primary_rollback_file" ]; then
		      rm -f "\$l_primary_rollback_file" 2>/dev/null || true;
		    fi;
		  fi;
		  if ! rollback_forwarded; then
		    cleanup_stages;
		    exit $l_backup_pair_primary_rollback_status;
		  fi;
		  if [ "\$l_primary_restore_failed" -eq 1 ]; then
		    cleanup_stages;
		    exit $l_backup_pair_primary_rollback_status;
		  fi;
		  cleanup_stages;
		  exit $l_backup_pair_primary_failure_status;
		fi;
	EOF
}

# Purpose: Render successful rollback-file disposal for a remote pair write.
# Usage: Called only by zxfer_build_remote_backup_pair_write_cmd.
zxfer_render_remote_backup_pair_finish() {
	while IFS= read -r l_backup_pair_finish_line ||
		[ -n "$l_backup_pair_finish_line" ]; do
		printf '%s\n' "$l_backup_pair_finish_line"
	done <<-'EOF'
		if [ "$l_forwarded_had_existing" -eq 1 ] && [ "$l_forwarded_rollback_file" != '' ]; then
		  rm -f "$l_forwarded_rollback_file" 2>/dev/null || true;
		fi;
		if [ "$l_primary_had_existing" -eq 1 ] && [ "$l_primary_rollback_file" != '' ]; then
		  rm -f "$l_primary_rollback_file" 2>/dev/null || true;
		fi;
		cleanup_stages;
	EOF
}

# Purpose: Build the remote backup pair write command for the next execution or
# comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_pair_write_cmd() {
	l_backup_pair_primary_dir=$1
	l_backup_pair_primary_path=$2
	l_backup_pair_forwarded_dir=$3
	l_backup_pair_forwarded_path=$4
	l_backup_pair_host=$5
	l_backup_pair_dependency_status=${6:-99}
	l_backup_pair_failure_status=${7:-92}
	l_backup_pair_rollback_status=98

	l_backup_pair_primary_dir_single=$(zxfer_escape_for_single_quotes "$l_backup_pair_primary_dir")
	l_backup_pair_primary_path_single=$(zxfer_escape_for_single_quotes "$l_backup_pair_primary_path")
	l_backup_pair_forwarded_dir_single=$(zxfer_escape_for_single_quotes "$l_backup_pair_forwarded_dir")
	l_backup_pair_forwarded_path_single=$(zxfer_escape_for_single_quotes "$l_backup_pair_forwarded_path")
	l_backup_pair_split_single=$(zxfer_escape_for_single_quotes "$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE")
	l_backup_pair_dependency_check=$(zxfer_build_remote_backup_helper_dependency_check_cmd \
		"$l_backup_pair_host" "$l_backup_pair_dependency_status" \
		mktemp chmod mv rm rmdir awk) || return "$?"
	l_backup_pair_functions=$(zxfer_render_remote_backup_pair_transaction_functions \
		"$l_backup_pair_forwarded_path_single") || return "$?"
	l_backup_pair_primary_guard=$(zxfer_render_remote_backup_target_write_guard_cmd \
		"$l_backup_pair_primary_path_single" "$l_backup_pair_failure_status") || return "$?"
	l_backup_pair_forwarded_guard=$(zxfer_render_remote_backup_target_write_guard_cmd \
		"$l_backup_pair_forwarded_path_single" "$l_backup_pair_failure_status") || return "$?"
	l_backup_pair_stage=$(zxfer_render_remote_backup_pair_stage_setup \
		"$l_backup_pair_primary_dir_single" "$l_backup_pair_forwarded_dir_single" \
		"$l_backup_pair_split_single" "$l_backup_pair_failure_status") || return "$?"
	l_backup_pair_forwarded_publish=$(zxfer_render_remote_backup_pair_forwarded_publish \
		"$l_backup_pair_forwarded_dir_single" "$l_backup_pair_forwarded_path_single" \
		"$l_backup_pair_failure_status" "$l_backup_pair_rollback_status") || return "$?"
	l_backup_pair_primary_publish=$(zxfer_render_remote_backup_pair_primary_publish \
		"$l_backup_pair_primary_dir_single" "$l_backup_pair_primary_path_single" \
		"$l_backup_pair_failure_status" "$l_backup_pair_rollback_status") || return "$?"
	l_backup_pair_finish=$(zxfer_render_remote_backup_pair_finish) || return "$?"

	l_backup_pair_script="$l_backup_pair_dependency_check

$l_backup_pair_functions

$l_backup_pair_primary_guard
$l_backup_pair_forwarded_guard

umask 077;
$l_backup_pair_stage
$l_backup_pair_forwarded_publish
$l_backup_pair_primary_publish
$l_backup_pair_finish"

	zxfer_wrap_remote_backup_helper_with_secure_path "$l_backup_pair_script"
}

# Purpose: Read the local backup file from staged state into the current shell.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_local_backup_file() {
	l_read_local_backup_path=$1
	g_zxfer_backup_file_read_result=""
	g_zxfer_backup_local_read_failure_result=""
	zxfer_require_backup_metadata_path_without_symlinks "$l_read_local_backup_path" || return 1
	if [ ! -f "$l_read_local_backup_path" ] || [ -h "$l_read_local_backup_path" ]; then
		return 4
	fi
	# A trusted parent that is not writable cannot swap the directory entry,
	# so direct validated reads are safe when same-directory staging is blocked.
	if zxfer_backup_metadata_path_uses_trusted_nonwritable_parent "$l_read_local_backup_path"; then
		if ! l_error=$(zxfer_check_secure_backup_file \
			"$l_read_local_backup_path" "$l_read_local_backup_path"); then
			zxfer_throw_error "$l_error"
		fi
		zxfer_read_runtime_artifact_file "$l_read_local_backup_path" >/dev/null || return "$?"
		l_backup_contents=$g_zxfer_runtime_artifact_read_result
		g_zxfer_backup_file_read_result=$l_backup_contents
		printf '%s' "$l_backup_contents"
		return 0
	fi
	if zxfer_create_backup_metadata_stage_dir_for_path \
		"$l_read_local_backup_path" "zxfer-backup-read" >/dev/null; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_read_failure_result=staging
		return "$l_status"
	fi
	l_read_local_backup_file_stage_dir=$g_zxfer_backup_stage_dir_result
	l_snapshot_path="$l_read_local_backup_file_stage_dir/backup.snapshot"
	if ln "$l_read_local_backup_path" "$l_snapshot_path" 2>/dev/null; then
		:
	else
		l_link_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_read_local_backup_file_stage_dir"
		if [ ! -f "$l_read_local_backup_path" ] || [ -h "$l_read_local_backup_path" ]; then
			return 4
		fi
		return "$l_link_status"
	fi
	if ! l_error=$(zxfer_check_secure_backup_file \
		"$l_snapshot_path" "$l_read_local_backup_path"); then
		zxfer_cleanup_backup_metadata_stage_dir "$l_read_local_backup_file_stage_dir"
		zxfer_throw_error "$l_error"
	fi
	if zxfer_read_runtime_artifact_file "$l_snapshot_path" >/dev/null; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_read_failure_result=staging
		zxfer_cleanup_backup_metadata_stage_dir "$l_read_local_backup_file_stage_dir"
		return "$l_status"
	fi
	l_backup_contents=$g_zxfer_runtime_artifact_read_result
	g_zxfer_backup_file_read_result=$l_backup_contents
	printf '%s' "$l_backup_contents"
	zxfer_cleanup_backup_metadata_stage_dir "$l_read_local_backup_file_stage_dir"
}

# Purpose: Translate a completed remote backup-read probe into zxfer's stable
# security, dependency, transport, and missing-file result contract.
# Usage: Called once after the remote helper finishes. Publishes successful
# contents through g_zxfer_backup_file_read_result and otherwise returns or
# throws with the same established status/message mapping.
zxfer_handle_remote_backup_read_status() {
	l_handle_backup_read_host=$1
	l_handle_backup_read_path=$2
	l_handle_backup_read_status=$3

	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		return 7
	fi
	if [ "$l_handle_backup_read_status" -eq 95 ]; then
		zxfer_throw_error "Refusing to use backup metadata $l_handle_backup_read_path on $l_handle_backup_read_host because it is not owned by root or the ssh user."
	fi
	if [ "$l_handle_backup_read_status" -eq 96 ]; then
		zxfer_throw_error "Refusing to use backup metadata $l_handle_backup_read_path on $l_handle_backup_read_host because its permissions are not 0600."
	fi
	if [ "$l_handle_backup_read_status" -eq 97 ]; then
		zxfer_throw_error "Cannot determine ownership or permissions for backup metadata $l_handle_backup_read_path on $l_handle_backup_read_host."
	fi
	[ "$l_handle_backup_read_status" -ne 94 ] || return 4
	if [ "$l_handle_backup_read_status" -eq 93 ]; then
		l_handle_backup_read_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_set_failure_class dependency
		zxfer_throw_error "Required remote backup-metadata helper dependency not found on host $l_handle_backup_read_host in secure PATH ($l_handle_backup_read_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ "$l_handle_backup_read_status" -eq 98 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		return 1
	fi
	if [ "$l_handle_backup_read_status" -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
			return 6
		fi
		return 5
	fi

	g_zxfer_backup_file_read_result=${g_zxfer_remote_probe_stdout:-}
	printf '%s' "${g_zxfer_remote_probe_stdout:-}"
	return 0
}

# Purpose: Read the remote backup file from staged state into the current
# shell.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_remote_backup_file() {
	l_read_remote_backup_host=$1
	l_path=$2
	l_read_remote_backup_profile_side=${3:-}

	g_zxfer_backup_file_read_result=""
	l_path_single=$(zxfer_escape_for_single_quotes "$l_path")
	l_remote_missing_status=94
	l_remote_insecure_owner_status=95
	l_remote_insecure_mode_status=96
	l_remote_unknown_status=97
	l_remote_symlink_status=98
	l_remote_dependency_status=93
	l_remote_awk_cmd="awk"
	l_remote_cat_helper=${g_cmd_cat:-cat}
	l_remote_cat_helper_cmd=$(zxfer_build_shell_command_from_argv "$l_remote_cat_helper")
	l_remote_symlink_guard_cmd=$(zxfer_build_remote_backup_symlink_guard_cmd "$l_path_single" "$l_remote_symlink_status" metadata)
	l_remote_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd \
		"$l_read_remote_backup_host" "$l_remote_dependency_status" id grep ls awk cut)
	l_remote_stage_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd \
		"$l_read_remote_backup_host" "$l_remote_dependency_status" mktemp ln rm rmdir)
	l_remote_path_setup_cmd="l_target_path='$l_path_single'; \
l_parent=\${l_target_path%/*}; \
if [ \"\$l_parent\" = \"\$l_target_path\" ] || [ \"\$l_parent\" = '' ]; then l_parent=/; fi; \
l_target_ls_path=\$l_target_path; \
case \"\$l_target_ls_path\" in -*) l_target_ls_path=./\$l_target_ls_path ;; esac; \
l_parent_ls_path=\$l_parent; \
case \"\$l_parent_ls_path\" in -*) l_parent_ls_path=./\$l_parent_ls_path ;; esac; \
l_expected_uid=''; \
if command -v id >/dev/null 2>&1; then l_expected_uid=\$(id -u 2>/dev/null); fi; \
if [ \"\$l_expected_uid\" = '' ] || printf '%s' \"\$l_expected_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then exit $l_remote_unknown_status; fi"
	l_remote_parent_trust_cmd="l_use_stage_dir=1; \
if [ ! -w \"\$l_parent\" ]; then \
	l_parent_ls_line=\$(ls -ldn \"\$l_parent_ls_path\" 2>/dev/null) || l_parent_ls_line=''; \
	if [ \"\$l_parent_ls_line\" != '' ]; then \
		l_parent_uid=\$(printf '%s\n' \"\$l_parent_ls_line\" | $l_remote_awk_cmd '{print \$3}'); \
		l_parent_perm=\$(printf '%s\n' \"\$l_parent_ls_line\" | $l_remote_awk_cmd '{print \$1}'); \
		l_parent_trusted=0; \
		case \"\$l_parent_perm\" in ??????????*) ;; *) l_parent_perm='' ;; esac; \
		if [ \"\$l_parent_uid\" = '0' ] || [ \"\$l_parent_uid\" = \"\$l_expected_uid\" ]; then \
			l_parent_trusted=1; \
			if [ \"\$l_parent_perm\" = '' ]; then \
				l_parent_trusted=0; \
			else \
				l_group_write=\$(printf '%s' \"\$l_parent_perm\" | cut -c 6); \
				l_other_write=\$(printf '%s' \"\$l_parent_perm\" | cut -c 9); \
				l_sticky_char=\$(printf '%s' \"\$l_parent_perm\" | cut -c 10); \
				case \"\$l_group_write\$l_other_write\" in \
				*w*) case \"\$l_sticky_char\" in t|T) ;; *) l_parent_trusted=0 ;; esac ;; \
				esac; \
			fi; \
		fi; \
		if [ \"\$l_parent_trusted\" = '1' ]; then l_use_stage_dir=0; fi; \
	fi; \
fi"
	l_remote_stage_setup_cmd="if [ \"\$l_use_stage_dir\" = '1' ]; then \
	$l_remote_stage_dependency_check_cmd \
	umask 077; \
	l_stage_dir=\$(mktemp -d \"\$l_parent/.zxfer-backup-read.XXXXXX\" 2>/dev/null) || exit $l_remote_unknown_status; \
	l_snapshot_path=\"\$l_stage_dir/backup.snapshot\"; \
	if ! ln \"\$l_target_path\" \"\$l_snapshot_path\" 2>/dev/null; then if [ ! -f \"\$l_target_path\" ] || [ -h \"\$l_target_path\" ]; then rmdir \"\$l_stage_dir\" 2>/dev/null || true; exit $l_remote_missing_status; fi; rmdir \"\$l_stage_dir\" 2>/dev/null || true; exit $l_remote_unknown_status; fi; \
	l_snapshot_ls_path=\$l_snapshot_path; \
	case \"\$l_snapshot_ls_path\" in -*) l_snapshot_ls_path=./\$l_snapshot_ls_path ;; esac; \
else \
	l_snapshot_path=\$l_target_path; \
	l_snapshot_ls_path=\$l_target_ls_path; \
fi"
	l_remote_read_cleanup_cmd="if [ \"\$l_use_stage_dir\" = '1' ]; then rm -f \"\$l_snapshot_path\"; rmdir \"\$l_stage_dir\" 2>/dev/null || true; fi"
	l_remote_validation_cmd="l_uid=''; \
if command -v stat >/dev/null 2>&1; then l_uid=\$(stat -c '%u' \"\$l_snapshot_path\" 2>/dev/null); if [ \"\$l_uid\" = '' ] || printf '%s' \"\$l_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_uid=\$(stat -f '%u' \"\$l_snapshot_path\" 2>/dev/null); fi; fi; \
if [ \"\$l_uid\" = '' ] || printf '%s' \"\$l_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_ls_line=\$(ls -ldn \"\$l_snapshot_ls_path\" 2>/dev/null) || l_ls_line=''; if [ \"\$l_ls_line\" != '' ]; then l_uid=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$3}'); fi; fi; \
if [ \"\$l_uid\" = '' ]; then $l_remote_read_cleanup_cmd; exit $l_remote_unknown_status; fi; \
if [ \"\$l_uid\" != '0' ] && [ \"\$l_uid\" != \"\$l_expected_uid\" ]; then $l_remote_read_cleanup_cmd; exit $l_remote_insecure_owner_status; fi; \
l_mode=''; \
if command -v stat >/dev/null 2>&1; then l_mode=\$(stat -c '%a' \"\$l_snapshot_path\" 2>/dev/null); if [ \"\$l_mode\" = '' ] || printf '%s' \"\$l_mode\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_mode=\$(stat -f '%OLp' \"\$l_snapshot_path\" 2>/dev/null); fi; fi; \
if [ \"\$l_mode\" = '' ] || printf '%s' \"\$l_mode\" | grep -q '[^0-9]' >/dev/null 2>&1; then if [ \"\$l_ls_line\" = '' ]; then l_ls_line=\$(ls -ldn \"\$l_snapshot_ls_path\" 2>/dev/null) || l_ls_line=''; fi; if [ \"\$l_ls_line\" != '' ]; then l_perm=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$1}'); if [ \"\$l_perm\" = '-rw-------' ]; then l_mode='600'; fi; fi; fi; \
	if [ \"\$l_mode\" = '' ]; then $l_remote_read_cleanup_cmd; exit $l_remote_unknown_status; fi; \
if [ \"\$l_mode\" != '600' ]; then $l_remote_read_cleanup_cmd; exit $l_remote_insecure_mode_status; fi"
	l_remote_payload_cmd="$l_remote_cat_helper_cmd \"\$l_snapshot_path\"; \
l_read_status=\$?; \
$l_remote_read_cleanup_cmd; \
exit \$l_read_status"
	l_remote_secure_cat_cmd="$l_remote_dependency_check_cmd $l_remote_symlink_guard_cmd if [ ! -f '$l_path_single' ] || [ -h '$l_path_single' ]; then exit $l_remote_missing_status; fi; $l_remote_path_setup_cmd; $l_remote_parent_trust_cmd; $l_remote_stage_setup_cmd; $l_remote_validation_cmd; $l_remote_payload_cmd"
	l_remote_secure_cat_cmd=$(zxfer_wrap_remote_backup_helper_with_secure_path "$l_remote_secure_cat_cmd")
	l_remote_secure_cat_transport_cmd=$(zxfer_prepare_remote_backup_transport_script \
		"$l_remote_secure_cat_cmd") || return "$?"
	l_remote_secure_cat_shell_cmd=$(zxfer_build_remote_sh_c_command \
		"$l_remote_secure_cat_transport_cmd") || return "$?"
	if zxfer_capture_remote_probe_output "$l_read_remote_backup_host" \
		"$l_remote_secure_cat_shell_cmd" "$l_read_remote_backup_profile_side"; then
		l_read_remote_backup_status=0
	else
		l_read_remote_backup_status=$?
	fi
	zxfer_handle_remote_backup_read_status \
		"$l_read_remote_backup_host" "$l_path" "$l_read_remote_backup_status"
}

# Purpose: Render a remote backup write command as the ssh pipeline segment used
# by dry-run output.
# Usage: Called by backup-metadata dry-run rendering for single-file and pair
# remote writes so wrapped host specs get the same `sh -c` handling.
zxfer_render_remote_backup_dry_run_shell_command() {
	l_backup_dry_run_host=$1
	l_backup_dry_run_script=$2

	g_zxfer_remote_backup_dry_run_shell_command_result=""
	l_backup_dry_run_transport_script=$(zxfer_prepare_remote_backup_transport_script \
		"$l_backup_dry_run_script") || return "$?"
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw \
		"$l_backup_dry_run_host" "$l_backup_dry_run_transport_script" ||
		return "$?"
	g_zxfer_remote_backup_dry_run_shell_command_result=$g_zxfer_prepared_ssh_shell_command_result
	return 0
}
