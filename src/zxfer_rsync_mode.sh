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

###############################################################################
# RSYNC MODE FUNCTIONS
###############################################################################

#
# This destroys snapshots to clear up the remains of a previous
# incomplete transfer. For -S mode only.
#
clean_up() {

	# A list of snapshots to delete
	snap_delete_list=$($g_LZFS list -Hr -t snapshot -o name | grep "$g_snapshot_name")
	snap_delete_list=$(echo "$snap_delete_list" | cat -n | sort -nr)
	snap_delete_list=$(echo "$snap_delete_list" | cut -c 8-)

	# delete the snapshots
	for l_source_snap in $snap_delete_list; do
		$g_LZFS destroy "$l_source_snap"
	done
}

#
# Prepares variables for rsync based transfer of properties
# using transfer_properties()
# This takes  $source, $part_of_source_to_delete, $g_destination, $initial_source
#
prepare_rs_property_transfer() {

	# This gets the root filesystem transferred - e.g.
	# the string after the very last "/" e.g. backup/test/zroot -> zroot
	l_base_fs=${initial_source##*/}
	# This gets everything but the base_fs, so that we can later delete it from
	# $source
	l_part_of_source_to_delete=${initial_source%"$l_base_fs"}

	# Prepare the actual destination (g_actual_dest) for property transfer.
	# A trailing slash means that the root filesystem is transferred straight
	# into the dest fs, no trailing slash means that this fs is created
	# inside the destination.
	# Note that where L is specified, we use trailing slash mode, where
	# L is not specified, we use non-trailing slash mode.
	if [ "$g_option_L_rsync_levels_deep" = "" ]; then
		# If the original source was backup/test/zroot and we are transferring
		# backup/test/zroot/tmp/foo, $l_dest_tail is zroot/tmp/foo
		l_dest_tail=$(echo "$source" | sed -e "s%^$l_part_of_source_to_delete%%g")
		# shellcheck disable=SC2034
		g_actual_dest="$g_destination"/"$l_dest_tail"
	else
		l_trailing_slash_dest_tail=$(echo "$source" | sed -e "s%^$initial_source%%g")
		# shellcheck disable=SC2034
		g_actual_dest="$g_destination$l_trailing_slash_dest_tail"
	fi
}

#
# Creates a list of exclude options to pass to rsync, so that empty directories
# on source corresponding to filesystem mountpoints don't cause a delete
# on the destination, and also optionally to exclude fs mountpoints on dest.
# Takes $opt_srs_all_inc_fs, $opt_src_fs_modif
# Outputs $exclude_options, exclude options to be added to other rsync options
#
get_exclude_list() {
	exclude_options=""

	# First, exclude known source-side filesystem mountpoints
	# They will be empty, and if not excluded --del will delete those folders
	# which will hold valid data on the destination
	exclude_dir_list=$(echo "$opt_srs_all_inc_fs" | grep "^$opt_src_fs_modif" |
		grep -v "^$opt_src_fs_modif$")
	exclude_dir_list=$(echo "$exclude_dir_list" | sed -e "s%^$opt_src_fs_modif%%g")

	# Second, by default we exclude filesystem mountpoints on the
	# destination, as they will have their own rsync transfer or be
	# left for an independent restore procedure.
	if [ "$g_option_i_rsync_include_zfs_mountpoints" -eq 0 ]; then
		dest_exclude_dir_list=$(echo "$rzfs_list_Ho_mountpoint" | grep "^$rs_dest" | grep -v "^$rs_dest$")
		dest_exclude_dir_list=$(echo "$dest_exclude_dir_list" |
			sed -e "s%^$rs_dest%%g")
		if [ "$rs_dest" = "/" ]; then
			dest_exclude_dir_list=$(echo "$dest_exclude_dir_list" | sed -e 's%^%/%')
		fi
		# now to remove duplicates
		exclude_dir_list="$exclude_dir_list
$dest_exclude_dir_list"
		exclude_dir_list=$(echo "$exclude_dir_list" | sort -u | grep -v "^$")
	fi

	for exd in $exclude_dir_list; do
		exclude_options="--exclude=$exd $exclude_options"
	done

	# Removes space at end
	exclude_options=$(echo "$exclude_options" | sed -e "s% $%%g")
}

#
# Caches zfs list commands to cut execution time, for option -S
#
get_zfs_list_rsync_mode() {
	g_recursive_dest_list=$($g_RZFS list -t filesystem,volume -Hr -o name "$g_destination")

	#  Exit if destination not sound
	if [ "$g_recursive_dest_list" = "" ]; then
		throw_usage_error "Destination filesystem does not exist. Create it first."
	fi

	OLD_IFS=$IFS
	IFS=","

	source_fs_list=""
	root_fs=""

	# We want to get a list of every ZFS filesystem that holds data that will be
	# transferred. Note that trailing slashes don't seem to matter.
	option_RN="$g_option_R_recursive,$g_option_N_nonrecursive"
	option_RN=${option_RN#,}
	option_RN=${option_RN%,}

	if [ "$g_option_L_rsync_levels_deep" != "" ]; then
		if [ "$g_option_L_rsync_levels_deep" -lt "1" ]; then
			throw_usage_error "Option L, if specified, should be 1 or greater."
		fi
		# shellcheck disable=SC2003
		inc_g_option_L_rsync_levels_deep=$(expr "$g_option_L_rsync_levels_deep" + 1)
	fi

	for source_RN in $option_RN; do
		source_RN_trailing_slash=$(echo "$source_RN" | grep -c '..*/$')

		if [ "$source_RN_trailing_slash" -eq 1 ]; then
			throw_usage_error "Do not specify trailing slashes in sources. \
\nThere is no meaning in the context of this program and so this has been disabled."
		fi

		temp_fs_list=$($g_LZFS list -t filesystem,volume -Hr -o name "$source_RN")
		temp_fs_list_comma=$(echo "$temp_fs_list" | tr -s "\n" ",")
		temp_fs_list_comma=${temp_fs_list_comma%,}

		if [ "$g_option_L_rsync_levels_deep" != "" ]; then
			for fs in $temp_fs_list_comma; do
				# count the "/" in the line, should be equal or greater to g_option_L_rsync_levels_deep
				slash_no=$(echo "$fs" | $g_cmd_awk "$0= NF-1" FS=/)
				if [ "$slash_no" -lt "$g_option_L_rsync_levels_deep" ]; then
					throw_usage_error "If using option L, ensure that all source files and\
directories are contained in filesystems with as many \"/\" as L."
				fi
				old_root_fs=$root_fs
				# shellcheck disable=SC2016
				root_fs=$(
					echo "$fs" |
						"$g_cmd_awk" -v depth="$inc_g_option_L_rsync_levels_deep" '
                            BEGIN { FS = "/" }
                            {
                                out = $1
                                for (i = 2; i <= depth; i++) {
                                    out = out "/" $i
                                }
                                print out
                            }'
				)

				if [ "$root_fs" != "$old_root_fs" ] && [ "$old_root_fs" != "" ]; then
					throw_usage_error "No common root filesystem. If using option L, ensure \
that each source file/directory comes from a common filesystem, and that the
the level specified is not after this common filesystem. e.g. if your pool
has been backed up to storage/backups/root_pool then the level you should
specify is \"2\"."
				fi
			done
		fi

		# exit if source is bogus
		if [ "$temp_fs_list" = "" ]; then
			throw_usage_error "Source in -N or -R option does not exist, or is stored \
on a filesystem that is not ZFS."
		fi

		# used printf in order to print the newlines
		source_fs_list=$(printf "%s\n%s" "$temp_fs_list" "$source_fs_list")
	done
	# We will use primarily the root_fs_parent, but got root_fs because
	# we wanted to check that root_fs is unique.
	# This line strips out the last bit, e.g. tank/back/zroot becomes tank/back
	# The regex is a slash, followed by zero or more non-slash characters,
	# until the end of the line.
	# unused variable
	#root_fs_parent=$(echo "$root_fs" | sed -e 's%/[^/]*$%%g')

	# Remove redundant entries and sort properly
	source_fs_list=$(echo "$source_fs_list" | sort -u)
	# unused variable
	#source_fs_list_rev=$(echo "$source_fs_list" | sort -r)

	# Gets the pools for the fs (e.g. in storage/tmp/foo, this would be "storage")
	source_pool_list=$(echo "$source_fs_list" | cut -f1 -d/ | sort -u)
	source_pool_number=$(echo "$source_pool_list" | wc -l | sed -e 's/ //g')
	if [ "$source_pool_number" -ne 1 ]; then
		throw_usage_error "The sources you list are stored on a total of $source_pool_number pools.\
\nAmend your list of sources until there is only one\
 pool relating to them all."
	fi

	# prepares the variables we will end up using later
	if [ "$g_option_L_rsync_levels_deep" = "" ]; then
		root_fs="$source_pool_list"
		# unused variable
		#root_fs_parent=""
	fi

	# for recursive option
	# unused variable
	#zfs_list_Ho_name=$($g_LZFS list -t filesystem,volume -H -o name)
	lzfs_list_Ho_mountpoint=$($g_LZFS list -t filesystem,volume -Ho mountpoint)
	rzfs_list_Ho_mountpoint=$($g_RZFS list -t filesystem,volume -Ho mountpoint)

	initial_source=$root_fs
	g_recursive_source_list=$($g_LZFS list -t filesystem,volume -Hr -o name "$root_fs" |
		grep -v "^$g_destination")

	IFS="$OLD_IFS"
}

#
# Transfers a source via rsync
# Takes $1 (source_type), where:
# n = non-recursive
# r = recursive
#
# full_rs_options are the rsync options to be used
# Takes $opt_source, which is a source directly from the -N or -R option
#
rsync_transfer() {
	l_source_type=$1
	full_rs_options=$2

	# Note that the source is actually in the snapshot. This gets the
	# ZFS filesystem corresponding to the source in the original -R or -N list.
	opt_src_fs=$($g_LZFS list -t filesystem,volume -H -o name "$opt_source")

	# We need to get all the filesystems at or below the level of the source.
	# This is a bit tricky; if opt_source is /tmp/foo/bar, and the filesystem
	# that contains it is zroot/tmp/foo (and there is also a zroot/tmp/foo/yip,
	# which we don't want, we only should be concerned about zroot/tmp/foo as
	# everything will be contained therein.
	# If OTOH, we want to transfer /tmp/foo, and this maps to zroot/tmp/foo
	# which also has zroot/tmp/foo/yip, then we must transfer everything in
	# every filesystem under this across.
	# To find out whether the directory is a filesystem, look at the mountpoint
	# properties. Note that this approach won't work for legacy mounts, but as
	# long as we aren't trying to recursively transfer "/", we should be fine.

	# is the source (directory) a filesystem?
	is_fs=$(echo "$lzfs_list_Ho_mountpoint" | tr " " "\n" | grep -c "^$opt_source$")

	if [ "$l_source_type" = "r" ] && [ "$is_fs" -eq "1" ]; then
		# the directory is a filesystem - need to include all fs under source fs
		opt_srs_all_inc_fs=$($g_LZFS list -t filesystem,volume -H -o name | grep "^$opt_src_fs")
	else
		opt_srs_all_inc_fs=$opt_src_fs # we will only loop through once
	fi

	for opt_src_fs in $opt_srs_all_inc_fs; do
		if [ "$l_source_type" = "r" ] && [ "$is_fs" -eq "1" ]; then
			# We need to get the mountpoints of each sub filesystem
			sub_opt_source=$($g_LZFS get -H -o value mountpoint "$opt_src_fs")
		else
			sub_opt_source=$opt_source
		fi

		# the mountpoint of the above
		opt_src_fs_mountpoint=$($g_LZFS get -Ho value mountpoint "$opt_src_fs")

		if [ "$opt_src_fs_mountpoint" = "legacy" ]; then
			if [ "$g_option_l_rsync_legacy_mountpoint" -eq 0 ]; then
				throw_usage_error "Legacy mountpoint encountered. Enable -l to assume \
that the legacy mountpoint is \"/\"."
			fi

			opt_src_fs_mountpoint="/"
			opt_src_tail=$opt_source
			rs_source="$opt_src_fs_mountpoint.zfs/snapshot/$g_snapshot_name/.$opt_src_tail"
		else
			# the part of the original source less the mountpoint part
			opt_src_tail=$(echo "$sub_opt_source" |
				sed -e "s%^$opt_src_fs_mountpoint%%g")

			# This is the source (from the atomic snapshot), nearly suitable for
			# rsync usage. What it is will depend on whether it is filesystem
			# or directory.
			rs_source="$opt_src_fs_mountpoint/.zfs/snapshot/$g_snapshot_name/.$opt_src_tail"
		fi

		# We can think of using L option as restoring, and without L option as
		# backing up. When we back up we copy the root filesystem and everything
		# in it into the destination. (e.g. zroot, zroot/tmp etc. goes into
		# storage/backups/zroot, storage/backups/zroot/tmp etc.) In the case of
		# restoring, we are going the other way around, but it is as if we are
		# coping the original root filesystem directly into the destination (which
		# may be a pool) otherwise we would not be able to restore a pool.

		opt_src_fs_modif=$opt_src_fs
		# This will impact the source and destination that we feed to rsync.
		if [ "$g_option_L_rsync_levels_deep" != "" ]; then
			# This will trim ($g_option_L_rsync_levels_deep + 1) folders off the beginning of the fs, e.g.
			# "tank/foo/bar/zroot/tmp/yum" becomes (with g_option_L_rsync_levels_deep = 3) "tmp/yum"
			# shellcheck disable=SC2003
			inc_g_option_L_rsync_levels_deep=$(expr "$g_option_L_rsync_levels_deep" + 1)

			n=1
			while [ "$n" -le "$inc_g_option_L_rsync_levels_deep" ]; do
				opt_src_fs_modif=$(echo "$opt_src_fs_modif" | sed -e 's%^[^/]*%%' | sed -e 's%^/%%')
				# shellcheck disable=SC2003
				n=$(expr "$n" + 1)
			done
		fi
		#rs_dest="/$g_destination/$opt_src_fs_modif$opt_src_tail"
		rs_dest="/$g_destination/$opt_src_fs_modif"

		# if $g_destination is "zroot/foo/bar", this regex gets "zroot"
		dest_root=$(echo "$g_destination" | sed -e 's%/.*$%%')

		dest_root_mountpoint=$($g_RZFS get -Ho value mountpoint "$dest_root")
		if [ "$dest_root_mountpoint" = "legacy" ]; then
			if [ "$g_option_l_rsync_legacy_mountpoint" -eq 0 ]; then
				throw_usage_error "Legacy mountpoint encountered. Enable -l to assume \
that the legacy mountpoint is \"/\"."
			fi
			rs_dest=$(echo "$rs_dest" | sed -e "s%^/[^/]*%%g")
		fi

		get_exclude_list

		if [ "$l_source_type" = "r" ]; then
			# Appends a slash and the "-r" option, to suit rsync
			rs_source="$rs_source/"
			full_rs_options="$full_rs_options $exclude_options -r"
			if [ "$g_option_d_delete_destination_snapshots" -eq 1 ]; then
				full_rs_options="$full_rs_options --del"
			fi
		elif [ -d "$rs_source" ]; then
			# Appends a slash and the "-d" option if a directory, to suit rsync
			# if a directory
			rs_source="$rs_source/"
			full_rs_options="$full_rs_options -d"
			if [ "$g_option_d_delete_destination_snapshots" -eq 1 ]; then
				full_rs_options="$full_rs_options --del"
			fi
		else
			: # is a file; note that ":" is a dummy command.
		fi

		if [ "$g_option_E_rsync_exclude_patterns" != "" ]; then # add user-specified exclude patterns.
			g_option_E_rsync_exclude_patterns=$(echo "$g_option_E_rsync_exclude_patterns" | sed -e "s% $%%g")
			full_rs_options="$full_rs_options $g_option_E_rsync_exclude_patterns"
		fi

		rs_source_safe=$(escape_for_double_quotes "$rs_source")
		rs_dest_safe=$(escape_for_double_quotes "$rs_dest")

		echov "Using rsync to recursively transfer $rs_source to $rs_dest with \
options $full_rs_options"

		# Now that we have something to feed rsync, we will call it.
		if [ "$g_option_n_dryrun" -eq 0 ]; then
			if [ "$g_option_p_rsync_persist" -eq 1 ]; then
				$g_cmd_rsync "$full_rs_options" "$rs_source_safe" "$rs_dest_safe" # persist in face of error
			else
				$g_cmd_rsync "$full_rs_options" "$rs_source_safe" "$rs_dest_safe" ||
					throw_error "Error when executing rsync."
			fi
		else
			echo "$g_cmd_rsync $full_rs_options $rs_source_safe $rs_dest_safe"
		fi

	done # End of sub-filesystem loop
}

#
# rsync mode
#
# From here, the basic algorithm is:
# 1. Delete anything that could have been left over from previous transfers.
# This could include: snapshots of original fs.
# 2. Get the pools relating to any filesystem relating to any directories that
#     will be transferred.
# 3. Take a recursive snapshot of each of those pools.
# 4. Create the destination filesystems or set appropriately. (If elected to
#     restore the properties, restore them from the backup file or fail. If
#     elected to backup the properties, back them up to the file
#     .zxfer_backup_info.$poolname at the filesystem
#     that $poolname will sit in.)
#  (see transfer_properties() )
# 5. Ensure that if any property is readonly, it is set to writable before transfer.
# 6. rsync the directories and files across, using the snapshots.
# 7. Set any previously readonly destination filesystems to be writable.
# 9. Delete the remnants, probably very similar process to 1.

# optimization summary: recursive snapshots take 8 seconds - unavoidable.
#                     : cloning filesystems takes a long time, clone as few as possible
#                     : deleting clones takes even longer, clone as few as possible

# Note that using clones was far easier to implement the rsync version of the script.
# We have however opted to use just snapshots as the time taken to create and delete
# clones is prohibitive in comparison. It also stops automatic snapshotting taking
# snapshots of the clones and creating further havoc.
#
run_rsync_mode() {
	# destroys old snapshots used in any previous (incomplete) use of
	# the script's functionality, if not using a custom snapshot
	if [ "$g_option_u_rsync_use_existing_snapshot" -eq 0 ]; then
		clean_up
	fi

	get_zfs_list_rsync_mode

	# If we are restoring properties get the backup properties
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		get_backup_properties
	fi

	# get the correct options to feed to rsync (excluding recursive)
	l_rsync_options="$g_default_rsync_options"
	if [ "$g_option_f_rsync_file_options" != "" ]; then
		# gets the options to be passed to rsync, if able to be read.
		if [ -r "$g_option_f_rsync_file_options" ]; then
			l_rsync_options=$(cat "$g_option_f_rsync_file_options")
		else
			throw_usage_error "Reading contents of $g_option_f_rsync_file_options."
		fi
	fi

	# recursively snapshot the source (if not using custom snapshot)
	if [ "$g_option_u_rsync_use_existing_snapshot" -eq 0 ]; then
		$g_LZFS snapshot -r "${initial_source}@${g_snapshot_name}"
	fi

	# for the first iteration of property transfer, we need to override the
	# readonly property of the filesystem so that rsync will work.
	# shellcheck disable=SC2034
	g_ensure_writable=1

	# make sure override list includes "readonly=off"
	old_g_option_o_override_property=$g_option_o_override_property
	g_option_o_override_property=$(echo "$g_option_o_override_property" | sed -e 's/readonly=on/readonly=off/g')
	ro_exist=$(echo "$g_option_o_override_property" | grep -c "readonly")
	if [ "$ro_exist" -eq 0 ]; then
		if [ "$g_option_o_override_property" = "" ]; then
			g_option_o_override_property="readonly=off"
		else
			g_option_o_override_property="$g_option_o_override_property,readonly=off"
		fi
	fi

	# we don't want to write the backup info this time, as it will be done later
	# shellcheck disable=SC2034
	g_dont_write_backup=1

	# Transfer source properties to destination if required, or create the fs.
	if [ "$g_option_P_transfer_property" -eq 1 ] || [ "$g_option_o_override_property" != "" ]; then
		# loop that sets the filesystem properties
		for source in $g_recursive_source_list; do
			# prepares some variables for property_transfer
			prepare_rs_property_transfer

			# Needs: $source, $initial_source, $g_actual_dest, $g_recursive_dest_list
			transfer_properties
		done
	fi

	# NOW, create the loop that will transfer each source file/directory across.
	# Need one loop for recursive, one loop for non-recursive
	g_option_N_nonrecursive_space=$(echo "$g_option_N_nonrecursive" | tr "," "\n")
	g_option_R_recursive_space=$(echo "$g_option_R_recursive" | tr "," "\n")

	# Loop for the non-recursive
	for opt_source in $g_option_N_nonrecursive_space; do
		rsync_transfer "n" "$l_rsync_options"
	done

	# Loop for the recursive directories
	for opt_source in $g_option_R_recursive_space; do
		# We want to ensure that the source is a directory, and to fail if not
		if [ -d "$opt_source" ]; then
			: # if a directory, do nothing
		else
			# if not a directory, fail with error
			throw_usage_error "Only directories are allowed when using recursive \
rsync transfer mode (i.e. -R). If you are trying to transfer \
a single file, use -N."
		fi

		rsync_transfer "r" "$l_rsync_options"
	done # End of recursive directory loop

	# reset backup file contents as they are built up in transfer_properties
	g_backup_file_contents=""

	# Now the readonly property will be as it is supposed to be when
	# properties are transferred.

	# this time we want to write the backup
	# shellcheck disable=SC2034
	g_dont_write_backup=0

	# this time the properties should be as intended on dest.
	# shellcheck disable=SC2034
	g_ensure_writable=0

	# clean up g_option_o_override_property to remove readonly=off
	g_option_o_override_property=$old_g_option_o_override_property

	# get new lists as there may be new filesystems now
	get_zfs_list_rsync_mode

	# Transfer source properties to destination if required.
	if [ "$g_option_P_transfer_property" -eq 1 ] || [ "$g_option_o_override_property" != "" ]; then
		# loop that sets the filesystem properties
		for source in $g_recursive_source_list; do

			prepare_rs_property_transfer

			# Needs: $source, $initial_source, $g_actual_dest, $g_recursive_dest_list
			transfer_properties
		done
	fi

	# We clean up snapshots if we aren't using a custom snapshot
	if [ "$g_option_u_rsync_use_existing_snapshot" -eq 0 ]; then
		clean_up
	fi
}
