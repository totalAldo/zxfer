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

# for ShellCheck
if false; then
	# shellcheck source=src/zxfer_globals.sh
	. ./zxfer_globals.sh
fi

################################################################################
# ZFS MODE FUNCTIONS
################################################################################

# module variables
m_services_to_restart=""

#
# Prepare the actual destination (g_actual_dest) as used in zfs receive.
# Uses $g_destination, $initial_source
# Output is $g_actual_dest
#
set_actual_dest() {
	l_source=$1

	# This gets the root filesystem transferred - e.g.
	# the string after the very last "/" e.g. backup/test/zroot -> zroot
	l_base_fs=${initial_source##*/}
	# This gets everything but the base_fs, so that we can later delete it from
	# $l_source
	l_part_of_source_to_delete=${initial_source%"$l_base_fs"}

	# A trailing slash means that the root filesystem is transferred straight
	# into the dest fs, no trailing slash means that this fs is created
	# inside the destination.
	if [ "$g_initial_source_had_trailing_slash" -eq 0 ]; then
		# If the original source was backup/test/zroot and we are transferring
		# backup/test/zroot/tmp/foo, $l_dest_tail is zroot/tmp/foo
		l_dest_tail=$(echo "$l_source" | sed -e "s%^$l_part_of_source_to_delete%%g")
		g_actual_dest="$g_destination"/"$l_dest_tail"
	else
		l_trailing_slash_dest_tail=$(echo "$l_source" | sed -e "s%^$initial_source%%g")
		g_actual_dest="$g_destination$l_trailing_slash_dest_tail"
	fi
}

#
# Copy from the last common snapshot to the most recent snapshot.
# Assumes that the list of snapshots is given in creation order ascending.
# Takes: $g_last_common_snap, $g_src_snapshot_transfer_list
#
copy_snapshots() {
	# This can get stale, especially if it has taken hours to copy the
	# previous snapshot. Consider adding a time check and refreshing the list of
	# snapshots if it has been too long since we got the list.
	# 2024.07.15 - the recommended solution is to use -Y to repeat the process
	# until there are no further differences
	l_first_snapshot=""
	l_final_snapshot=""

	# find the first and final snapshot for this dataset on the source
	for l_snapshot in $g_src_snapshot_transfer_list; do
		# set the first snapshot
		[ -z "$l_first_snapshot" ] && l_first_snapshot=$l_snapshot

		# keep looping until the end of the list
		l_final_snapshot=$l_snapshot
	done

	if [ -z "$l_final_snapshot" ]; then
		echoV "No snapshots to copy, skipping destination dataset: $g_actual_dest."
		return
	fi

	# check if the destination exists and if not,
	# create it by sending the first snapshot
	if [ "$(exists_destination "$g_actual_dest")" -eq 0 ]; then
		# get the first snapshot name with full path
		echov "Destination dataset does not exist [$g_actual_dest]. Sending first snapshot [$l_first_snapshot]"
		# do not allow this to be run in the background
		zfs_send_receive "" "$l_first_snapshot" "$g_actual_dest" "0"

		# set the last common snapshot to the first snapshot so that all snapshots
		# are copied below
		g_last_common_snap=$l_first_snapshot
		g_dest_has_snapshots=1
	elif [ "$g_dest_has_snapshots" -eq 0 ]; then
		echov "Destination dataset [$g_actual_dest] exists but has no snapshots. Seeding with [$l_first_snapshot]"
		zfs_send_receive "" "$l_first_snapshot" "$g_actual_dest" "0"
		g_last_common_snap=$l_first_snapshot
		g_dest_has_snapshots=1
	fi

	# get the final snapshot name
	echoV "Final snapshot: $l_final_snapshot"

	if [ "$g_last_common_snap" = "$l_final_snapshot" ]; then
		echoV "No new snapshots to copy for $g_actual_dest."
		return
	fi

	# begin copying snapshots to the final snap from the last common snapshot
	zfs_send_receive "$g_last_common_snap" "$l_final_snapshot" "$g_actual_dest" "1"
}

#
# Stop a list of SMF services. The services are read in from stdin.
#
stopsvcs() {
	l_raw_services=$(cat)

	# Nothing to do if the caller provided an empty string.
	[ -n "$l_raw_services" ] || return

	l_normalized_services=$(printf '%s\n' "$l_raw_services" | awk '
{
	for (i = 1; i <= NF; i++)
		print $i
}')

	[ -n "$l_normalized_services" ] || return

	while IFS= read -r service; do
		echov "Disabling service $service."
		svcadm disable -st "$service" ||
			{
				echo "Could not disable service $service."
				relaunch
				exit 1
			}
		m_services_to_restart="$m_services_to_restart $service"
		g_services_need_relaunch=1
	done <<EOF
$l_normalized_services
EOF
}

#
# Relaunch a list of stopped services
#
relaunch() {
	[ -z "$m_services_to_restart" ] && return

	# Assume relaunch completes; set flag early so trap_exit won't loop
	g_services_need_relaunch=0

	for l_i in $m_services_to_restart; do
		echov "Restarting service $l_i"
		svcadm enable "$l_i" || {
			echo "Couldn't re-enable service $l_i."
			exit
		}
	done
}

#
# Create a new recursive snapshot.
#
newsnap() {
	l_initial_source=$1

	# We snapshot from the base of the initial source
	# Extract the filesystem name from the initial source snapshot by removing the '@' and everything after it
	l_sourcefs="${l_initial_source%@*}"

	l_snap=$g_zxfer_new_snapshot_name

	if [ "$g_option_R_recursive" != "" ]; then
		echov "Creating recursive snapshot $l_sourcefs@$l_snap."
		cmd="$g_LZFS snapshot -r $l_sourcefs@$l_snap"
	else
		echov "Creating snapshot $l_sourcefs@$l_snap."
		cmd="$g_LZFS snapshot $l_sourcefs@$l_snap"
	fi

	execute_command "$cmd"
}

#
# Tests to see if they are trying to sync a snapshots; exit if so
#
check_snapshot() {
	l_initial_source=$1

	l_initial_sourcesnap=$(extract_snapshot_name "$l_initial_source")

	# When using -s or -m, we don't want the source to be a snapshot.
	[ -n "$l_initial_sourcesnap" ] && throw_error "Snapshots are not allowed as a source."
}

#
# Calculate a list of properties that are not supported on the destination so
# they can be excluded from the properties transfer
# This allows replicating data from newer version of ZFS to older versions
#
calculate_unsupported_properties() {
	# Get a list of the supported properties from the destination
	l_dest_pool_name=${g_destination%%/*}
	l_dest_supported_properties=$(run_destination_zfs_cmd get -Ho property all "$l_dest_pool_name")

	# Get a list of the supported properties from the source
	l_source_pool_name=${initial_source%%/*}
	l_source_supported_properties=$(run_source_zfs_cmd get -Ho property all "$l_source_pool_name")

	unsupported_properties=""

	for s_p in $l_source_supported_properties; do
		l_found_supported_prop=0
		for d_p in $l_dest_supported_properties; do
			if [ "$s_p" = "$d_p" ]; then
				l_found_supported_prop=1
				break
			fi
		done
		if [ $l_found_supported_prop -eq 0 ]; then
			unsupported_properties="${unsupported_properties}${s_p},"
		fi
	done
	unsupported_properties=${unsupported_properties%,}
}

#
# main loop that copies the filesystems
#
copy_filesystems() {
	echoV "Begin copy_filesystems()"

	l_property_pass_required=0
	if [ "$g_option_P_transfer_property" -eq 1 ] || [ "$g_option_o_override_property" != "" ]; then
		l_property_pass_required=1
	fi

	l_iteration_list=$g_recursive_source_list
	l_force_dataset_iteration=0

	if [ "$g_option_R_recursive" != "" ] && [ $l_property_pass_required -eq 1 ]; then
		l_force_dataset_iteration=1
	fi

	if [ "$g_option_d_delete_destination_snapshots" -eq 1 ]; then
		l_force_dataset_iteration=1
	fi

	if [ "$l_force_dataset_iteration" -eq 1 ]; then
		l_iteration_list=$(printf '%s\n%s\n' "$l_iteration_list" "$g_recursive_source_dataset_list" |
			grep -v '^[[:space:]]*$' | sort -u)
	fi

	if [ "$l_iteration_list" = "" ] && [ "$l_force_dataset_iteration" -eq 1 ]; then
		l_iteration_list=$initial_source
	fi

	for l_source in $l_iteration_list; do

		set_actual_dest "$l_source"

		# If using the -m feature, check if the source is mounted,
		# otherwise there's no point in us doing the remounting.
		if [ "$g_option_m_migrate" -eq 1 ]; then
			l_source_to_migrate_mounted=$(run_source_zfs_cmd get -Ho value mounted "$l_source")
			if [ "$l_source_to_migrate_mounted" != "yes" ]; then
				echo "The source filesystem is not mounted, cannot use -m."
				exit 1
			fi
			mountpoint=$(run_source_zfs_cmd get -Ho value mountpoint "$l_source")
			propsource=$(run_source_zfs_cmd get -Ho source mountpoint "$l_source")
			echov "Mountpoint is: $mountpoint. Source: $propsource."
		fi

		# Inspect the source and destination snapshots so that we are in position to
		# transfer using the latest common snapshot as a base, and transferring the
		# newer snapshots on source, in order.
		inspect_delete_snap "$g_option_d_delete_destination_snapshots" "$l_source"

		# Transfer source properties to destination if required.
		if [ "$g_option_P_transfer_property" -eq 1 ] || [ "$g_option_o_override_property" != "" ]; then
			transfer_properties "$l_source"
		fi

		#
		# We now have a valid source filesystem, volume or snapshot to copy from and an
		# assumed valid destination filesystem to copy to with a possible snapshot name
		# to give to the destination snapshot.
		#
		copy_snapshots

		#
		# Now we have replicated all existing snapshots.
		#
	done

	# wait for background zfs_send_receive processes before proceeding
	wait_for_zfs_send_jobs "final sync"

	echoV "End copy_filesystems()"
}

resolve_initial_source_from_options() {
	if [ "$g_option_R_recursive" != "" ] && [ "$g_option_N_nonrecursive" != "" ]; then
		throw_usage_error "You must choose either -N to transfer a single filesystem or -R to transfer \
a single filesystem and its children recursively, but not both -N and -R at the same time."
	elif [ "$g_option_R_recursive" != "" ]; then
		initial_source="$g_option_R_recursive"
	elif [ "$g_option_N_nonrecursive" != "" ]; then
		initial_source="$g_option_N_nonrecursive"
	else
		throw_usage_error "You must specify a source with either -N or -R."
	fi
}

normalize_source_destination_paths() {
	# Record whether the user supplied a trailing slash before normalizing the path.
	g_initial_source_had_trailing_slash=$(echo "$initial_source" | grep -c '..*/$')

	# Now that we know whether there was a trailing slash on the source, no
	# need to confuse things by keeping it on there. Get rid of it.
	initial_source=$(strip_trailing_slashes "$initial_source")

	# Source and destination can't start with "/", but it's an easy mistake to make
	if [ "$(echo "$initial_source" | grep -c '^/')" -eq "1" ] ||
		[ "$(echo "$g_destination" | grep -c '^/')" -eq "1" ]; then
		throw_usage_error "Source and destination must not begin with \"/\". Note the example."
	fi

	# Trailing slashes on the destination are meaningless for dataset names but
	# make later concatenation produce an illegal double slash, so normalize it
	# once up front.
	g_destination=$(strip_trailing_slashes "$g_destination")
}

validate_zfs_mode_preconditions() {
	echoV "Checking source snapshot."
	check_snapshot "$initial_source"

	# When using -c you must use -m as well rule. This forces the user
	# To think twice if they really mean to do the migration.
	[ -n "$g_option_c_services" ] && [ "$g_option_m_migrate" -eq 0 ] &&
		throw_error "When using -c, -m needs to be specified as well."
}

update_recursive_source_list_if_needed() {
	if [ "$g_option_R_recursive" = "" ]; then
		g_recursive_source_list=$initial_source
	fi
}

initialize_replication_context() {
	# Caches all the zfs list calls, gets the recursive list, and gives
	# an opportunity to exit if the source is not present
	get_zfs_list

	# If we are restoring properties get the backup properties
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		get_backup_properties
	fi

	if [ "$g_option_U_skip_unsupported_properties" -eq 1 ]; then
		calculate_unsupported_properties
	fi

	update_recursive_source_list_if_needed
}

refresh_dataset_iteration_state() {
	get_zfs_list
	update_recursive_source_list_if_needed
}

maybe_capture_preflight_snapshot() {
	#
	# If using -s, do a new recursive snapshot, then copy all new snapshots too.
	#
	if [ "$g_option_s_make_snapshot" -eq 0 ] || [ "$g_option_m_migrate" -eq 1 ]; then
		return
	fi

	# Create the new snapshot with a unique name.
	newsnap "$initial_source"

	# Because there are new snapshots, need to refresh the cached lists.
	refresh_dataset_iteration_state
}

prepare_migration_services() {
	[ "$g_option_m_migrate" -eq 1 ] || return

	# Check if any services need to be disabled before doing a migration.
	if [ -n "$g_option_c_services" ]; then
		echo "$g_option_c_services" | stopsvcs
	fi

	for l_source in $g_recursive_source_list; do
		# Unmount the source filesystem before doing the last snapshot.
		echov "Unmounting $l_source."
		if ! run_source_zfs_cmd unmount "$l_source"; then
			echo "Couldn't unmount source $l_source."
			relaunch
			exit 1
		fi
	done

	# Create the new snapshot with a unique name.
	newsnap "$initial_source"

	# We include the mountpoint as a property that should be transferred.
	# Note that $g_option_P_transfer_property is automatically set to 1, to transfer the property.
	g_readonly_properties=$(echo "$g_readonly_properties" |
		sed -e 's/,mountpoint//g')

	# Now we must make the script aware of the new snapshots in existence so
	# we can copy them over.
	refresh_dataset_iteration_state
}

perform_grandfather_protection_checks() {
	[ "$g_option_g_grandfather_protection" != "" ] || return

	echov "Checking grandfather status of all snapshots marked for deletion..."

	for l_source in $g_recursive_source_list; do
		set_actual_dest "$l_source"
		# turn off delete so that we are only checking snapshots, pass 0
		inspect_delete_snap 0 "$l_source"
	done
	echov "Grandfather check passed."
}

#
# zfs send/receive mode, aka zfs-replicate mode, aka normal mode
#
run_zfs_mode() {
	resolve_initial_source_from_options
	normalize_source_destination_paths
	validate_zfs_mode_preconditions
	initialize_replication_context
	maybe_capture_preflight_snapshot
	prepare_migration_services
	perform_grandfather_protection_checks

	copy_filesystems

	if [ "$g_option_m_migrate" -eq 1 ]; then
		# Re-launch any stopped services.
		relaunch
	fi
}

#
# if -Y is set, run the zfs mode in a loop until no more changes are made
# or until the maximum number of iterations is reached.
# Otherwise, run the zfs mode once
#
run_zfs_mode_loop() {
	l_num_iterations=0

	# run this in a loop until there are no more zfs send or zfs destroy commands
	# that are issued
	while true; do
		# if a send or destroy command is performed, set this to 1 indicating
		# that a change was made during run_zfs_mode
		g_is_performed_send_destroy=0

		l_num_iterations=$((l_num_iterations + 1))
		if [ "$g_option_Y_yield_iterations" -gt 1 ]; then
			echov "Begin Iteration[$l_num_iterations of $g_option_Y_yield_iterations]. Running in zfs send/receive mode."
		fi

		run_zfs_mode

		if [ "$g_option_Y_yield_iterations" -gt 1 ]; then
			echov "End Iteration[$l_num_iterations of $g_option_Y_yield_iterations]."
		fi

		# check if we need to perform another iteration
		if [ "$g_is_performed_send_destroy" -eq 0 ]; then
			echoV "Exiting loop. No send or destroy commands were performed during last iteration."
			break
		fi
		if [ "$l_num_iterations" -ge "$g_option_Y_yield_iterations" ]; then
			if [ "$g_option_Y_yield_iterations" -ge "$g_MAX_YIELD_ITERATIONS" ]; then
				echoV "Exiting loop. Reached maximum number of iterations.
If consistently not completing replication in allotted iterations,
consider using compression, increasing bandwidth, increasing I/O or reducing snapshot frequency."
			fi
			break
		fi
	done
}
