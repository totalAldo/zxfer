#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024-2025 Aldo Gonzalez
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

# shellcheck disable=SC2034
# Global variables defined here are used across multiple zxfer modules.

################################################################################
# DEFINE GLOBALS used by zxfer
################################################################################

#
# Define global variables
#

# Secure location for property backup files (override via ZXFER_BACKUP_DIR).
g_backup_storage_root=${ZXFER_BACKUP_DIR:-/var/db/zxfer}

# Some unit tests source zxfer helpers without calling init_globals(), so make
# sure the awk command resolves to something usable even before the real
# initialization logic runs.
if [ -z "${g_cmd_awk:-}" ]; then
	g_cmd_awk=$(command -v awk 2>/dev/null || :)
	[ -n "$g_cmd_awk" ] || g_cmd_awk=awk
fi

init_globals() {
	# zxfer version
	g_zxfer_version="2.0.0-20251117"

	# max number of iterations to run iterate through run_zfs_mode
	# if changes are made to the filesystems
	g_MAX_YIELD_ITERATIONS=8

	# Default values
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_d_delete_destination_snapshots=0
	g_option_D_display_progress_bar=""
	g_option_e_restore_property_mode=0
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_I_ignore_properties=""
	# number of parallel job processes to run when listing zfs snapshots
	# in the source (default 1 does not use parallel).
	# This also sets the maximum number of background zfs send processes
	# that can run at the same time.
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_O_origin_host_safe=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_T_target_host_safe=""
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_w_raw_send=0
	g_option_z_compress=0

	# services stopped by -c/-m that must be restarted on exit
	g_services_need_relaunch=0

	source=""
	g_initial_source_had_trailing_slash=0

	# keep track of the number of background zfs send jobs
	g_count_zfs_send_jobs=0
	g_zfs_send_job_pids=""

	g_destination=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_file_contents=""
	g_backup_storage_root=${ZXFER_BACKUP_DIR:-/var/db/zxfer}

	# operating systems
	g_source_operating_system=""
	g_destination_operating_system=""

	# default compression commands
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""

	g_cmd_cat=""

	g_cmd_awk=$(which awk) # location of awk or gawk on home OS
	g_cmd_zfs=$(which zfs)
	g_cmd_parallel=$(which parallel)
	g_origin_parallel_cmd=""
	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	g_cmd_ssh=$(which ssh)
	# ssh control sockets used for origin (-O) and target (-T) hosts
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""

	# default zfs commands, can be overridden by -O or -T
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs

	# dataset and snapshot lists
	g_recursive_source_list=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""

	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_actual_dest=""
	g_src_snapshot_transfer_list=""
	g_pending_receive_create_opts=""
	g_pending_receive_create_dest=""

	# temporary files used by get_dest_snapshots_to_delete_per_dataset()
	g_delete_source_tmp_file=$(get_temp_file)
	g_delete_dest_tmp_file=$(get_temp_file)
	g_delete_snapshots_to_delete_tmp_file=$(get_temp_file)

	# specific to zfs mode
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

	g_restored_backup_file_contents=""

	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off

	# the readonly properties list 3 properties that are technically not
	# readonly but we will remove them from the override list as it does not make
	# sense to try and transfer them - version, volsize and mountpoint
	# Others have been added since. This is a potential refactor point
	# to split into two lists, $g_readonly_properties and $zxfer_unsupported_properties
	g_readonly_properties="type,creation,used,available,referenced,\
compressratio,mounted,version,primarycache,secondarycache,\
usedbysnapshots,usedbydataset,usedbychildren,usedbyrefreservation,\
version,volsize,mountpoint,mlslabel,keysource,keystatus,rekeydate,encryption,\
refcompressratio,written,logicalused,logicalreferenced,createtxg,guid,origin,\
filesystem_count,snapshot_count,clones,defer_destroy,receive_resume_token,\
userrefs,objsetid"

	# Properties not supported on FreeBSD
	g_fbsd_readonly_properties="aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,\
xattr,dnodesize"

	# Properties not supported on Solaris Express 11
	g_solexp_readonly_properties="jailed,aclmode,shareiscsi"

	refresh_compression_commands
}

refresh_compression_commands() {
	g_cmd_compress_safe=$(quote_cli_tokens "$g_cmd_compress")
	g_cmd_decompress_safe=$(quote_cli_tokens "$g_cmd_decompress")

	if [ "$g_option_z_compress" -eq 1 ]; then
		if [ "$g_cmd_compress_safe" = "" ]; then
			throw_usage_error "Compression command (-Z/ZXFER_COMPRESSION) cannot be empty." 2
		fi
		if [ "$g_cmd_decompress_safe" = "" ]; then
			throw_error "Compression requested but decompression command missing."
		fi
	fi
}

# setup an ssh control socket for the specified role (origin or target)
setup_ssh_control_socket() {
	l_host=$1
	l_role=$2

	[ -z "$l_host" ] && return

	l_timestamp=$(date +%s)
	if ! l_control_dir=$(mktemp -d -t "zxfer_ssh_control_socket.${l_role}.$l_timestamp"); then
		throw_error "Error creating temporary directory for ssh control socket."
	fi
	l_control_socket="$l_control_dir/socket"

	case "$l_role" in
	origin)
		[ "$g_ssh_origin_control_socket" != "" ] && close_origin_ssh_control_socket
		g_ssh_origin_control_socket_dir="$l_control_dir"
		g_ssh_origin_control_socket="$l_control_socket"
		;;
	target)
		[ "$g_ssh_target_control_socket" != "" ] && close_target_ssh_control_socket
		g_ssh_target_control_socket_dir="$l_control_dir"
		g_ssh_target_control_socket="$l_control_socket"
		;;
	esac

	l_host_tokens=$(split_host_spec_tokens "$l_host")
	set -- "$g_cmd_ssh" -M -S "$l_control_socket"
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi
	set -- "$@" -fN
	if ! "$@"; then
		throw_error "Error creating ssh control socket for $l_role host."
	fi
}

close_origin_ssh_control_socket() {
	if [ "$g_option_O_origin_host" = "" ] || [ "$g_ssh_origin_control_socket" = "" ]; then
		return
	fi

	l_host_tokens=$(split_host_spec_tokens "$g_option_O_origin_host")
	set -- "$g_cmd_ssh" -S "$g_ssh_origin_control_socket" -O exit
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi
	l_log_cmd="$g_cmd_ssh -S $g_ssh_origin_control_socket -O exit $(quote_host_spec_tokens "$g_option_O_origin_host")"
	echoV "Closing origin ssh control socket: $l_log_cmd"
	"$@" 2>/dev/null

	if [ "$g_ssh_origin_control_socket_dir" != "" ] && [ -d "$g_ssh_origin_control_socket_dir" ]; then
		rm -rf "$g_ssh_origin_control_socket_dir"
	fi
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
}

close_target_ssh_control_socket() {
	if [ "$g_option_T_target_host" = "" ] || [ "$g_ssh_target_control_socket" = "" ]; then
		return
	fi

	l_host_tokens=$(split_host_spec_tokens "$g_option_T_target_host")
	set -- "$g_cmd_ssh" -S "$g_ssh_target_control_socket" -O exit
	if [ "$l_host_tokens" != "" ]; then
		while IFS= read -r l_token || [ -n "$l_token" ]; do
			set -- "$@" "$l_token"
		done <<EOF
$l_host_tokens
EOF
	fi
	l_log_cmd="$g_cmd_ssh -S $g_ssh_target_control_socket -O exit $(quote_host_spec_tokens "$g_option_T_target_host")"
	echoV "Closing target ssh control socket: $l_log_cmd"
	"$@" 2>/dev/null

	if [ "$g_ssh_target_control_socket_dir" != "" ] && [ -d "$g_ssh_target_control_socket_dir" ]; then
		rm -rf "$g_ssh_target_control_socket_dir"
	fi
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
}

close_all_ssh_control_sockets() {
	close_origin_ssh_control_socket
	close_target_ssh_control_socket
}

get_ssh_cmd_for_host() {
	l_host=$1
	if [ "$l_host" = "" ]; then
		echo "$g_cmd_ssh"
		return
	fi

	if [ "$l_host" = "$g_option_O_origin_host" ] && [ "$g_ssh_origin_control_socket" != "" ]; then
		echo "$g_cmd_ssh -S $g_ssh_origin_control_socket"
		return
	fi

	if [ "$l_host" = "$g_option_T_target_host" ] && [ "$g_ssh_target_control_socket" != "" ]; then
		echo "$g_cmd_ssh -S $g_ssh_target_control_socket"
		return
	fi

	echo "$g_cmd_ssh"
}

#
# function that always executes if the script is terminated by a signal
#
trap_exit() {
	# get the exit status of the last command
	l_exit_status=$?

	# kill all background jobs
	l_job_pids=$(jobs -p)
	if [ -n "$l_job_pids" ]; then
		# shellcheck disable=SC2086  # split into individual PIDs on purpose
		kill $l_job_pids 2>/dev/null
	fi

	close_all_ssh_control_sockets

	# Remove temporary files if they exist
	for l_temp_file in "$g_delete_source_tmp_file" \
		"$g_delete_dest_tmp_file" \
		"$g_delete_snapshots_to_delete_tmp_file"; do
		if [ -f "$l_temp_file" ]; then
			rm "$l_temp_file"
		fi
	done

	if [ "${g_services_need_relaunch:-0}" -eq 1 ]; then
		# Prevent re-entrancy loops if relaunch exits due to failure.
		g_services_need_relaunch=0
		if command -v relaunch >/dev/null 2>&1; then
			echoV "zxfer exiting early; restarting stopped services."
			relaunch
		else
			echoV "zxfer exiting with services still stopped; relaunch() unavailable."
		fi
	fi

	echoV "zxfer exiting with status $l_exit_status"

	# exit this script
	exit $l_exit_status
}

# catch any signals to terminate the script
# INT (Interrupt) 2 (Ctrl-C)
# TERM (Terminate) 15 (kill)
# HUP (Hangup) 1 (kill -HUP)
# QUIT (Quit) 3 (Ctrl-\)
# EXIT (Exit) 0 (exit)
trap trap_exit INT TERM HUP QUIT EXIT

#
# Check command line parameters.
#
read_command_line_switches() {
	while getopts bBc:dD:eFg:hI:j:kmnN:o:O:PR:sT:UvVwx:YzZ: l_i; do
		case $l_i in
		b)
			g_option_b_beep_always=1
			;;
		B)
			g_option_B_beep_on_success=1
			;;
		c)
			g_option_c_services="$OPTARG"
			;;
		d)
			g_option_d_delete_destination_snapshots=1
			;;
		D)
			g_option_D_display_progress_bar="$OPTARG"
			;;
		e)
			g_option_e_restore_property_mode=1
			# Need to transfer properties, just the backed up properties
			# are substituted
			g_option_P_transfer_property=1
			;;
		F)
			g_option_F_force_rollback="-F"
			;;
		g)
			g_option_g_grandfather_protection="$OPTARG"
			;;
		h)
			throw_usage_error
			;;
		I)
			g_option_I_ignore_properties="$OPTARG"
			;;
		j)
			# number of parallel jobs and background sends
			g_option_j_jobs="$OPTARG"
			;;
		k)
			g_option_k_backup_property_mode=1
			# In order to back up the properties of the source, the
			# properties of the source must be transferred as well.
			g_option_P_transfer_property=1
			;;
		m)
			g_option_m_migrate=1
			g_option_s_make_snapshot=1
			g_option_P_transfer_property=1
			;;
		n)
			g_option_n_dryrun=1
			;;
		N)
			g_option_N_nonrecursive="$OPTARG"
			;;
		o)
			g_option_o_override_property="$OPTARG"
			;;
		O)
			# since we are using the -O option, we are pulling a remote transfer
			# so we need to use the ssh command to execute the zfs commands
			# $OPTARG is the user@host
			l_new_origin_host="$OPTARG"
			setup_ssh_control_socket "$l_new_origin_host" "origin"
			g_option_O_origin_host="$l_new_origin_host"
			g_option_O_origin_host_safe=$(quote_host_spec_tokens "$g_option_O_origin_host")
			l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_O_origin_host")
			g_LZFS="$l_origin_ssh_cmd $g_option_O_origin_host_safe $g_cmd_zfs"
			;;
		P)
			g_option_P_transfer_property=1
			;;
		R)
			g_option_R_recursive="$OPTARG"
			;;
		s)
			g_option_s_make_snapshot=1
			;;
		T)
			# since we are using the -T option, we are pushing a remote transfer
			# so we need to use the ssh command to execute the zfs commands
			# $OPTARG is the user@host
			l_new_target_host="$OPTARG"
			setup_ssh_control_socket "$l_new_target_host" "target"
			g_option_T_target_host="$l_new_target_host"
			g_option_T_target_host_safe=$(quote_host_spec_tokens "$g_option_T_target_host")
			l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
			g_RZFS="$l_target_ssh_cmd $g_option_T_target_host_safe $g_cmd_zfs"
			;;
		U)
			g_option_U_skip_unsupported_properties=1
			;;
		v)
			g_option_v_verbose=1
			;;
		V)
			g_option_v_verbose=1
			g_option_V_very_verbose=1
			;;
		w)
			g_option_w_raw_send=1
			;;
		x)
			g_option_x_exclude_datasets="$OPTARG"
			;;
		Y)
			# set the number of iterations to run through the zfs mode
			g_option_Y_yield_iterations=$g_MAX_YIELD_ITERATIONS
			;;
		z)
			# Pipes the send and receive commands through zstd
			g_option_z_compress=1
			;;
		Z)
			# specify the zstd compression command, like "zstd -T0 -6"
			g_option_z_compress=1
			g_cmd_compress="$OPTARG"
			;;
		\?)
			throw_usage_error "Invalid option provided." 2
			;;
		esac
	done

	refresh_compression_commands
}

# Function to extract snapshot name
extract_snapshot_name() {
	# use the method below to not spawn grep and cut
	#echo "$1" | grep @ | cut -d@ -f2

	# Check if the input contains an '@' symbol
	case "$1" in
	*@*)
		# Extract the part after the '@' symbol using parameter expansion
		echo "${1#*@}"
		;;
	*)
		# If no '@' symbol is found, return an empty string or handle as needed
		echo ""
		;;
	esac
}

#
# Initializes OS and local/remote specific variables
#
init_variables() {
	# determine the source operating system
	if [ "$g_option_O_origin_host" != "" ]; then
		l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_O_origin_host")
		g_source_operating_system=$(get_os "$l_origin_ssh_cmd $g_option_O_origin_host_safe")
	else
		g_source_operating_system=$(get_os "")
	fi

	# determine the destination operating system
	if [ "$g_option_T_target_host" != "" ]; then
		l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
		g_destination_operating_system=$(get_os "$l_target_ssh_cmd $g_option_T_target_host_safe")
	else
		g_destination_operating_system=$(get_os "")
	fi

	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		if [ "$g_option_O_origin_host" = "" ]; then
			g_cmd_cat=$(which cat)
		else
			l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_O_origin_host")
			g_cmd_cat=$($l_origin_ssh_cmd "$g_option_O_origin_host" which cat)
		fi
	fi

	l_home_operating_system=$(get_os "")
	if [ "$l_home_operating_system" = "SunOS" ]; then
		g_cmd_awk=$(which gawk)
	fi
}

#
# Checks that options make sense, etc.
#
consistency_check() {
	# Validate -j early so arithmetic comparisons do not trip /bin/sh errors.
	case ${g_option_j_jobs:-} in
	'' | *[!0-9]*)
		throw_usage_error "The -j option requires a positive integer job count, but received \"${g_option_j_jobs:-}\"."
		;;
	esac
	if [ "$g_option_j_jobs" -le 0 ]; then
		throw_usage_error "The -j option requires a job count of at least 1."
	fi

	# disallow backup and restore of properties at same time
	if [ "$g_option_k_backup_property_mode" -eq 1 ] &&
		[ "$g_option_e_restore_property_mode" -eq 1 ]; then
		throw_usage_error "You cannot bac(k)up and r(e)store properties at the same time."
	fi

	# disallow both beep modes, enforce using one or the other.
	if [ "$g_option_b_beep_always" -eq 1 ] &&
		[ "$g_option_B_beep_on_success" -eq 1 ]; then
		throw_usage_error "You cannot use both beep modes at the same time."
	fi

	if [ "$g_option_z_compress" -eq 1 ] &&
		[ "$g_option_O_origin_host" = "" ] &&
		[ "$g_option_T_target_host" = "" ]; then
		throw_usage_error "-z option can only be used with -O or -T option"
	fi

	# disallow migration related options and remote transfers at same time
	if [ "$g_option_T_target_host" != "" ] || [ "$g_option_O_origin_host" != "" ]; then
		if [ "$g_option_m_migrate" -eq 1 ] || [ "$g_option_c_services" != "" ]; then
			throw_usage_error "You cannot migrate to or from a remote host."
		fi
	fi
}

sanitize_backup_component() {
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

get_backup_storage_dir() {
	l_mountpoint=$1
	l_dataset=$2
	[ -z "${g_backup_storage_root:-}" ] && g_backup_storage_root=${ZXFER_BACKUP_DIR:-/var/db/zxfer}

	case "$l_mountpoint" in
	"" | "-")
		l_relative="detached"
		;;
	legacy | none)
		l_relative=$(sanitize_backup_component "$l_mountpoint")
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

get_path_owner_uid() {
	l_path=$1

	if [ ! -e "$l_path" ]; then
		return 1
	fi

	if command -v stat >/dev/null 2>&1; then
		if l_uid=$(stat -f '%u' "$l_path" 2>/dev/null); then
			printf '%s\n' "$l_uid"
			return 0
		fi
		if l_uid=$(stat -c '%u' "$l_path" 2>/dev/null); then
			printf '%s\n' "$l_uid"
			return 0
		fi
	fi

	if l_ls_output=$(ls -ldn -- "$l_path" 2>/dev/null); then
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
		if l_mode=$(stat -f '%OLp' "$l_path" 2>/dev/null); then
			printf '%s\n' "$l_mode"
			return 0
		fi
		if l_mode=$(stat -c '%a' "$l_path" 2>/dev/null); then
			printf '%s\n' "$l_mode"
			return 0
		fi
	fi

	if l_ls_output=$(ls -ldn -- "$l_path" 2>/dev/null); then
		l_perm_str=$(printf '%s\n' "$l_ls_output" | ${g_cmd_awk:-awk} '{print $1}')
		if [ "$l_perm_str" = "-rw-------" ]; then
			printf '600\n'
			return 0
		fi
	fi

	return 1
}

require_secure_backup_file() {
	l_path=$1

	if ! l_owner_uid=$(get_path_owner_uid "$l_path"); then
		throw_error "Cannot determine the owner of backup metadata $l_path."
	fi
	if [ "$l_owner_uid" != "0" ]; then
		throw_error "Refusing to use backup metadata $l_path because it is owned by UID $l_owner_uid instead of root."
	fi
	if ! l_mode=$(get_path_mode_octal "$l_path"); then
		throw_error "Cannot determine the permissions for backup metadata $l_path."
	fi
	if [ "$l_mode" != "600" ]; then
		throw_error "Refusing to use backup metadata $l_path because its permissions ($l_mode) are not 0600."
	fi
}

ensure_local_backup_dir() {
	l_dir=$1
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
	if [ "$l_owner_uid" != "0" ]; then
		throw_error "Refusing to use backup directory $l_dir because it is owned by UID $l_owner_uid instead of root."
	fi
	if ! chmod 700 "$l_dir"; then
		throw_error "Error securing backup directory $l_dir."
	fi
}

escape_for_single_quotes() {
	l_value=$1
	printf '%s' "$l_value" | sed "s/'/'\\\\''/g"
}

ensure_remote_backup_dir() {
	l_dir=$1
	l_host=$2

	[ "$l_host" = "" ] && return

	l_target_ssh_cmd=$(get_ssh_cmd_for_host "$l_host")
	l_dir_single=$(escape_for_single_quotes "$l_dir")
	l_remote_cmd="[ -L '$l_dir_single' ] && { echo 'Refusing to use symlinked zxfer backup directory.' >&2; exit 1; }; if [ -e '$l_dir_single' ] && [ ! -d '$l_dir_single' ]; then echo 'Backup path exists but is not a directory.' >&2; exit 1; fi; umask 077; if ! mkdir -p '$l_dir_single'; then echo 'Error creating secure backup directory.' >&2; exit 1; fi; if ! chmod 700 '$l_dir_single'; then echo 'Error securing backup directory.' >&2; exit 1; fi; l_expected_uid=\$(id -u); l_dir_uid=''; if l_dir_uid=\$(stat -f '%u' '$l_dir_single' 2>/dev/null); then :; elif l_dir_uid=\$(stat -c '%u' '$l_dir_single' 2>/dev/null); then :; else l_ls_line=\$(ls -ldn -- '$l_dir_single' 2>/dev/null) || l_ls_line=''; if [ \"\$l_ls_line\" != '' ]; then l_dir_uid=\$(printf '%s\n' \"\$l_ls_line\" | awk '{print \$3}'); fi; fi; if [ \"\$l_dir_uid\" = '' ]; then echo 'Unable to determine backup directory owner.' >&2; exit 1; fi; if [ \"\$l_dir_uid\" != 0 ] && [ \"\$l_dir_uid\" != \"\$l_expected_uid\" ]; then echo 'Backup directory must be owned by root or the ssh user.' >&2; exit 1; fi"
	l_remote_cmd=$(escape_for_double_quotes "$l_remote_cmd")
	if ! $l_target_ssh_cmd "$l_host" "$l_remote_cmd"; then
		throw_error "Error preparing backup directory on $l_host."
	fi
}

read_local_backup_file() {
	l_path=$1
	if [ ! -f "$l_path" ] || [ -h "$l_path" ]; then
		return 1
	fi
	require_secure_backup_file "$l_path"
	cat "$l_path"
}

read_remote_backup_file() {
	l_host=$1
	l_path=$2

	l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$l_host")
	l_path_single=$(escape_for_single_quotes "$l_path")
	l_remote_insecure_owner_status=95
	l_remote_insecure_mode_status=96
	l_remote_unknown_status=97
	l_remote_awk_cmd=${g_cmd_awk:-awk}
	l_remote_secure_cat_cmd=$(escape_for_double_quotes "
if [ ! -f '$l_path_single' ] || [ -h '$l_path_single' ]; then
	exit 1
fi
l_uid=''
if command -v stat >/dev/null 2>&1; then
	l_uid=\$(stat -f '%u' '$l_path_single' 2>/dev/null)
	if [ \"\$l_uid\" = '' ]; then
		l_uid=\$(stat -c '%u' '$l_path_single' 2>/dev/null)
	fi
fi
if [ \"\$l_uid\" = '' ]; then
	l_ls_line=\$(ls -ldn -- '$l_path_single' 2>/dev/null) || l_ls_line=''
	if [ \"\$l_ls_line\" != '' ]; then
		l_uid=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$3}')
	fi
fi
if [ \"\$l_uid\" = '' ]; then
	exit $l_remote_unknown_status
fi
if [ \"\$l_uid\" != '0' ]; then
	exit $l_remote_insecure_owner_status
fi
l_mode=''
if command -v stat >/dev/null 2>&1; then
	l_mode=\$(stat -f '%OLp' '$l_path_single' 2>/dev/null)
	if [ \"\$l_mode\" = '' ]; then
		l_mode=\$(stat -c '%a' '$l_path_single' 2>/dev/null)
	fi
fi
if [ \"\$l_mode\" = '' ]; then
	if [ \"\$l_ls_line\" = '' ]; then
		l_ls_line=\$(ls -ldn -- '$l_path_single' 2>/dev/null) || l_ls_line=''
	fi
	if [ \"\$l_ls_line\" != '' ]; then
		l_perm=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$1}')
		if [ \"\$l_perm\" = '-rw-------' ]; then
			l_mode='600'
		fi
	fi
fi
if [ \"\$l_mode\" = '' ]; then
	exit $l_remote_unknown_status
fi
if [ \"\$l_mode\" != '600' ]; then
	exit $l_remote_insecure_mode_status
fi
$g_cmd_cat '$l_path_single'
")
	$l_origin_ssh_cmd "$l_host" "$l_remote_secure_cat_cmd"
	l_remote_status=$?
	if [ $l_remote_status -eq $l_remote_insecure_owner_status ]; then
		throw_error "Refusing to use backup metadata $l_path on $l_host because it is not owned by root."
	fi
	if [ $l_remote_status -eq $l_remote_insecure_mode_status ]; then
		throw_error "Refusing to use backup metadata $l_path on $l_host because its permissions are not 0600."
	fi
	if [ $l_remote_status -eq $l_remote_unknown_status ]; then
		throw_error "Cannot determine ownership or permissions for backup metadata $l_path on $l_host."
	fi
	return $l_remote_status
}

#
# Gets the backup properties from a previous backup of those properties
# This takes $initial_source. The backup file is usually in directory
# corresponding to the parent filesystem of $initial_source
#
get_backup_properties() {
	# We will step back through the filesystem hierarchy from $initial_source
	# until the pool level, looking for the backup file, stopping when we find
	# it or terminating with an error.
	# shellcheck disable=SC2154
	l_suspect_fs=$initial_source
	l_suspect_fs_tail=""
	l_found_backup_file=0
	l_used_legacy_backup=0
	l_legacy_backup_path=""
	l_expected_secure_backup=""

	while [ $l_found_backup_file -eq 0 ]; do
		l_mountpoint=$($g_LZFS get -H -o value mountpoint "$l_suspect_fs")
		l_secure_dir=$(get_backup_storage_dir "$l_mountpoint" "$l_suspect_fs")
		l_backup_file="$l_secure_dir/$g_backup_file_extension.$l_suspect_fs_tail"
		l_legacy_backup_file=""
		case "$l_mountpoint" in
		"" | "-" | legacy | none) ;;
		*)
			l_legacy_backup_file=$l_mountpoint/$g_backup_file_extension.$l_suspect_fs_tail
			;;
		esac

		if [ "$g_option_O_origin_host" = "" ]; then
			if l_backup_contents=$(read_local_backup_file "$l_backup_file"); then
				g_restored_backup_file_contents=$l_backup_contents
				l_found_backup_file=1
			elif [ "$l_legacy_backup_file" != "" ] &&
				l_backup_contents=$(read_local_backup_file "$l_legacy_backup_file"); then
				g_restored_backup_file_contents=$l_backup_contents
				l_found_backup_file=1
				l_used_legacy_backup=1
				l_legacy_backup_path="$l_legacy_backup_file"
				l_expected_secure_backup="$l_backup_file"
			fi
		else
			if l_backup_contents=$(read_remote_backup_file "$g_option_O_origin_host" "$l_backup_file"); then
				g_restored_backup_file_contents=$l_backup_contents
				l_found_backup_file=1
			elif [ "$l_legacy_backup_file" != "" ] &&
				l_backup_contents=$(read_remote_backup_file "$g_option_O_origin_host" "$l_legacy_backup_file"); then
				g_restored_backup_file_contents=$l_backup_contents
				l_found_backup_file=1
				l_used_legacy_backup=1
				l_legacy_backup_path="$l_legacy_backup_file"
				l_expected_secure_backup="$l_backup_file"
			fi
		fi

		if [ $l_found_backup_file -eq 0 ]; then
			l_suspect_fs_parent=$(echo "$l_suspect_fs" | sed -e 's%/[^/]*$%%g')
			if [ "$l_suspect_fs_parent" = "$l_suspect_fs" ]; then
				echo "Error: Cannot find backup property file. Ensure that it"
				echo "exists and that it is in a directory corresponding to the"
				echo "mountpoints of one of the ancestor filesystems of the source."
				usage
				exit 1
			else
				l_suspect_fs_tail=$(echo "$l_suspect_fs" | sed -e 's/.*\///g')
				l_suspect_fs=$l_suspect_fs_parent
			fi
		fi
	done

	if [ $l_used_legacy_backup -eq 1 ]; then
		echo "Warning: read legacy backup metadata from $l_legacy_backup_path. Move it to $l_expected_secure_backup (or set ZXFER_BACKUP_DIR) to use the hardened storage path." >&2
	fi

	# at this point the $g_backup_file_contents will be a list of lines with
	# $source,$g_actual_dest,$source_pvs
}

#
# Writes the backup properties to a file that is in the directory
# corresponding to the destination filesystem
#
write_backup_properties() {
	[ -z "${g_backup_storage_root:-}" ] && g_backup_storage_root=${ZXFER_BACKUP_DIR:-/var/db/zxfer}
	l_is_tail=$(echo "$initial_source" | sed -e 's/.*\///g')
	l_destination_mountpoint=$($g_RZFS get -H -o value mountpoint "$g_destination")
	l_backup_file_dir=$(get_backup_storage_dir "$l_destination_mountpoint" "$g_destination")
	l_backup_file_path=$l_backup_file_dir/$g_backup_file_extension.$l_is_tail
	echov "Writing backup info to secure path $l_backup_file_path (mountpoint $l_destination_mountpoint)"

	# Construct the backup file contents
	l_backup_file_header="#zxfer property backup file;#version:$g_zxfer_version;#R options:$g_option_R_recursive;#N options:$g_option_N_nonrecursive;#destination:$g_destination;#initial_source:$l_is_tail;"
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
			ensure_remote_backup_dir "$g_backup_storage_root" "$g_option_T_target_host"
			ensure_remote_backup_dir "$l_backup_file_dir" "$g_option_T_target_host"
			l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
			if ! printf '%s' "$g_backup_file_contents" | tr ";" "\n" |
				$l_target_ssh_cmd "$g_option_T_target_host" "$(escape_for_double_quotes "umask 077; cat > \"$l_backup_file_path\"")"; then
				throw_error "Error writing backup file. Is filesystem mounted?"
			fi
		fi
	else
		l_backup_file_contents_safe=$(escape_for_double_quotes "$g_backup_file_contents")
		if [ "$g_option_T_target_host" = "" ]; then
			printf '%s\n' "umask 077; printf '%s' \"$l_backup_file_contents_safe\" | tr ';' \"\\n\" > \"$l_backup_file_path\""
		else
			l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
			printf '%s\n' "printf '%s' \"$l_backup_file_contents_safe\" | tr ';' \"\\n\" | $l_target_ssh_cmd \"$g_option_T_target_host\" \"umask 077; cat > \\\"$l_backup_file_path\\\"\""
		fi
	fi
}
