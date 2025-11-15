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
init_globals() {
	# zxfer version
	g_zxfer_version="2.0.1-20251115"

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
	g_option_E_rsync_exclude_patterns=""
	g_option_f_rsync_file_options=""
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_i_rsync_include_zfs_mountpoints=0
	g_option_I_ignore_properties=""
	# number of parallel job processes to run when listing zfs snapshots
	# in the source (default 1 does not use parallel).
	# This also sets the maximum number of background zfs send processes
	# that can run at the same time.
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_l_rsync_legacy_mountpoint=0
	g_option_L_rsync_levels_deep=""
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_p_rsync_persist=0
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_S_rsync_mode=0
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_u_rsync_use_existing_snapshot=0
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_w_raw_send=0
	g_option_z_compress=0

	source=""

	# keep track of the number of background zfs send jobs
	g_count_zfs_send_jobs=0

	g_destination=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_file_contents=""

	# operating systems
	g_source_operating_system=""
	g_destination_operating_system=""

	# default compression commands
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"

	g_cmd_cat=""

	g_cmd_awk=$(which awk) # location of awk or gawk on home OS
	g_cmd_zfs=$(which zfs)
	g_cmd_parallel=$(which parallel)
	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	g_cmd_ssh=$(which ssh)
	# ssh control sockets used for origin (-O) and target (-T) hosts
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""

	g_cmd_rsync=""

	# default zfs commands, can be overridden by -O or -T
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs

	# dataset and snapshot lists
	g_recursive_source_list=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""

	g_last_common_snap=""
	g_actual_dest=""
	g_src_snapshot_transfer_list=""

	# temporary files used by get_dest_snapshots_to_delete_per_dataset()
	g_delete_source_tmp_file=$(get_temp_file)
	g_delete_dest_tmp_file=$(get_temp_file)
	g_delete_snapshots_to_delete_tmp_file=$(get_temp_file)

	# specific to zfs mode
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

	g_restored_backup_file_contents=""

	# used in rsync transfers, to turn off the backup file writing
	# the first time
	g_dont_write_backup=0

	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off

	# default rsync options - see http://www.daemonforums.org/showthread.php?t=3948
	g_default_rsync_options="-clptgoD --inplace --relative -H --numeric-ids"

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

	eval "$g_cmd_ssh -M -S $l_control_socket $l_host -fN"
}

close_origin_ssh_control_socket() {
	if [ "$g_option_O_origin_host" = "" ] || [ "$g_ssh_origin_control_socket" = "" ]; then
		return
	fi

	l_cmd="$g_cmd_ssh -S $g_ssh_origin_control_socket -O exit $g_option_O_origin_host"
	echoV "Closing origin ssh control socket: $l_cmd"
	eval "$l_cmd" 2>/dev/null

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

	l_cmd="$g_cmd_ssh -S $g_ssh_target_control_socket -O exit $g_option_T_target_host"
	echoV "Closing target ssh control socket: $l_cmd"
	eval "$l_cmd" 2>/dev/null

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
	while getopts bBc:deE:f:Fg:hiI:j:klL:lmnN:o:O:pPPR:sST:u:UvVwY?:D:zZ:x: l_i; do
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
		E)
			g_option_E_rsync_exclude_patterns="--exclude=$OPTARG $g_option_E_rsync_exclude_patterns"
			;;
		f)
			g_option_f_rsync_file_options="$OPTARG"
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
		i)
			g_option_i_rsync_include_zfs_mountpoints=1
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
		l)
			g_option_l_rsync_legacy_mountpoint=1
			;;
		L)
			g_option_L_rsync_levels_deep="$OPTARG"
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
			l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_O_origin_host")
			g_LZFS="$l_origin_ssh_cmd $g_option_O_origin_host $g_cmd_zfs"
			;;
		p)
			g_option_p_rsync_persist=1
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
		S)
			g_option_S_rsync_mode=1
			;;
		T)
			# since we are using the -T option, we are pushing a remote transfer
			# so we need to use the ssh command to execute the zfs commands
			# $OPTARG is the user@host
			l_new_target_host="$OPTARG"
			setup_ssh_control_socket "$l_new_target_host" "target"
			g_option_T_target_host="$l_new_target_host"
			l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
			g_RZFS="$l_target_ssh_cmd $g_option_T_target_host $g_cmd_zfs"
			;;
		u)
			g_option_u_rsync_use_existing_snapshot=1
			g_snapshot_name="$OPTARG"
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
		g_source_operating_system=$(get_os "$l_origin_ssh_cmd $g_option_O_origin_host")
	else
		g_source_operating_system=$(get_os "")
	fi

	# determine the destination operating system
	if [ "$g_option_T_target_host" != "" ]; then
		l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
		g_destination_operating_system=$(get_os "$l_target_ssh_cmd $g_option_T_target_host")
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

	if [ "$g_option_S_rsync_mode" -eq 1 ]; then
		g_cmd_rsync=$(which rsync)
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

	if [ "$g_option_S_rsync_mode" -eq 1 ]; then
		# rsync mode

		# check for incompatible options
		if [ "$g_option_F_force_rollback" = "-F" ]; then
			throw_usage_error "-F option cannot be used with -S (rsync mode)"
		fi

		if [ "$g_option_s_make_snapshot" -eq 1 ]; then
			throw_usage_error "-s option cannot be used with -S (rsync mode)"
		fi

		if [ "$g_option_O_origin_host" != "" ] ||
			[ "$g_option_T_target_host" != "" ]; then
			throw_usage_error "-O or -T option cannot be used with -S (rsync mode)"
		fi

		if [ "$g_option_m_migrate" -eq 1 ]; then
			throw_usage_error "-m option cannot be used with -S (rsync mode)"
		fi

		if [ "$g_option_Y_yield_iterations" -gt 1 ]; then
			throw_usage_error "-Y option cannot be used with -S (rsync mode)"
		fi
	else
		#zfs send mode

		# check for incompatible options
		if [ "$g_option_f_rsync_file_options" != "" ]; then
			throw_usage_error "-f option can only be used with -S (rsync mode)"
		fi

		if [ "$g_option_L_rsync_levels_deep" != "" ]; then
			throw_usage_error "-L option can only be used with -S (rsync mode)"
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
	fi
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

	while [ $l_found_backup_file -eq 0 ]; do
		l_backup_file_dir=$($g_LZFS get -H -o value mountpoint "$l_suspect_fs")
		l_backup_file="$l_backup_file_dir/$g_backup_file_extension.$l_suspect_fs_tail"

		if [ "$g_option_O_origin_host" = "" ]; then
			if [ -r "$l_backup_file" ]; then
				g_restored_backup_file_contents=$(cat "$l_backup_file")
				l_found_backup_file=1
			fi
		else
			l_origin_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_O_origin_host")
			if $l_origin_ssh_cmd "$g_option_O_origin_host" "[ -r '$l_backup_file' ]"; then
				g_restored_backup_file_contents=$($l_origin_ssh_cmd "$g_option_O_origin_host" "$g_cmd_cat '$l_backup_file'")
				l_found_backup_file=1
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

	# at this point the $g_backup_file_contents will be a list of lines with
	# $source,$g_actual_dest,$source_pvs
}

#
# Writes the backup properties to a file that is in the directory
# corresponding to the destination filesystem
#
write_backup_properties() {
	l_is_tail=$(echo "$initial_source" | sed -e 's/.*\///g')
	l_backup_file_dir=$($g_RZFS get -H -o value mountpoint "$g_destination")
	echov "Writing backup info to location $l_backup_file_dir/$g_backup_file_extension.$l_is_tail"

	# Construct the backup file contents
	l_backup_file_header="#zxfer property backup file;#version:$g_zxfer_version;#R options:$g_option_R_recursive;#N options:$g_option_N_nonrecursive;#destination:$g_destination;#initial_source:$l_is_tail;#g_option_S_rsync_mode:$g_option_S_rsync_mode;"
	l_backup_date=$(date)
	g_backup_file_contents="$l_backup_file_header#backup_date:$l_backup_date$g_backup_file_contents"

	# Construct the command to write the backup file
	l_backup_file_cmd="echo \"$g_backup_file_contents\" | tr \";\" \"\n\" > $l_backup_file_dir/$g_backup_file_extension.$l_is_tail"

	# Execute the command
	if [ "$g_option_n_dryrun" -eq 0 ]; then
		if [ "$g_option_T_target_host" = "" ]; then
			sh -c "$l_backup_file_cmd" ||
				throw_error "Error writing backup file. Is filesystem mounted?"
		else
			l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
			echo "$l_backup_file_cmd" | $l_target_ssh_cmd "$g_option_T_target_host" sh ||
				throw_error "Error writing backup file. Is filesystem mounted?"
		fi
	else
		if [ "$g_option_T_target_host" = "" ]; then
			echo "sh -c \"$l_backup_file_cmd\""
		else
			l_target_ssh_cmd=$(get_ssh_cmd_for_host "$g_option_T_target_host")
			echo "echo \"$l_backup_file_cmd\" | $l_target_ssh_cmd \"$g_option_T_target_host\" sh"
		fi
	fi
}
