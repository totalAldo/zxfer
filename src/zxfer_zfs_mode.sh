#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024 Aldo Gonzalez
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


# for shellcheck linting, uncomment this line
#. ./zxfer_globals.sh;

################################################################################
# ZFS MODE FUNCTIONS
################################################################################

# module variables
m_services_to_restart=""
# as in the "cp" man page
m_trailing_slash=0

m_sourcefs=""

#
# Prepare the actual destination (g_actual_dest) as used in zfs receive.
# Uses $m_trailing_slash, $source, $part_of_source_to_delete, $g_destination,
# $initial_source
# Output is $g_actual_dest
#
set_actual_dest() {
    # A trailing slash means that the root filesystem is transferred straight
    # into the dest fs, no trailing slash means that this fs is created
    # inside the destination.
    if [ "$m_trailing_slash" -eq 0 ]; then
        # If the original source was backup/test/zroot and we are transferring
        # backup/test/zroot/tmp/foo, $l_dest_tail is zroot/tmp/foo
        l_dest_tail=$(echo "$source" | sed -e "s%^$part_of_source_to_delete%%g")
        g_actual_dest="$g_destination"/"$l_dest_tail"
    else
        l_trailing_slash_dest_tail=$(echo "$source" | sed -e "s%^$initial_source%%g")
        g_actual_dest="$g_destination$l_trailing_slash_dest_tail"
    fi
}

# Check if the snapshot already exists
snapshot_exists() {
    l_snapshot=$1
    l_snapshot_list=$2
    echo "$l_snapshot_list" | grep -q "^$l_snapshot"
}

#
# Copy a snapshot using zfs send/receive. If a third argument is used, then use
# send -i and the third argument is the base to create the increment from.
# Arguments should be compatible with zfs send and receive commands. Does
# nothing if the snapshot already exists.
# Takes $dest, rzfs_list_ho_s
#
copy_snap() {
    l_copysrc=$1
    l_copyprev=$2
    l_copydest=$g_actual_dest

    l_copysrctail=$(echo "$l_copysrc" | cut -d/ -f2-)

    # Check if the snapshot already exists
    if snapshot_exists "$l_copydest/$l_copysrctail" "$g_rzfs_list_ho_s"; then
        echov "Snapshot $l_copysrc already exists at destination $l_copydest. Exiting function."
        return
    fi

    zfs_send_receive "$l_copysrc" "$l_copydest" "$l_copyprev"
}

#
# Copy the list of snapshots given in stdin to the destination
# Use incremental snapshots where possible. Assumes that the list of snapshots
# is given in creation order. copy_snap is responsible for skipping already
# existing snapshots on the destination side.
# Takes: $g_found_last_common_snap, $g_last_common_snap, $source, $g_src_snapshot_transfer_list
#
copy_snap_multiple() {
    # Instead of transferring all the source snapshots, this just transfers
    # the ones starting from the latest common snapshot on src and dest
    l_copy_fs_snapshot_list=$(echo "$g_src_snapshot_transfer_list" | grep ".")

    l_lastsnap=""

    # if there is a snapshot common to both src and dest, set that to be $lastsnap
    if [ "$g_found_last_common_snap" -eq 1 ]; then
        l_lastsnap=$g_last_common_snap
    fi

    # XXX: This can get stale, especially if it has taken hours to copy the
    # previous snapshot. Consider adding a time check and refreshing the list of
    # snapshots if it has been too long since we got the list
    l_final_snap=""
    for l_snapshot in $l_copy_fs_snapshot_list; do
        l_final_snap=$l_snapshot
    done

    if [ "$l_final_snap" != "" ]; then
        copy_snap "$l_final_snap" "$l_lastsnap"
    fi
}

#
# this version is intended to work with zfs send -i
# deprecated
#
copy_snap_multipleOld() {
    # Instead of transferring all the source snapshots, this just transfers
    # the ones starting from the latest common snapshot on src and dest
    l_copy_fs_snapshot_list=$(echo "$g_src_snapshot_transfer_list" | grep ".")

    l_lastsnap=""

    # if there is a snapshot common to both src and dest, set that to be $lastsnap
    if [ "$g_found_last_common_snap" -eq 1 ]; then
        l_lastsnap="$source@$g_last_common_snap"
    fi

    # XXX: This can get stale, especially if it has taken hours to copy the
    # previous snapshot. Consider adding a time check and refreshing the list of
    # snapshots if it has been too long since we got the list
    l_final_snap=""
    for l_snapshot in $l_copy_fs_snapshot_list; do
        copy_snap "$l_snapshot" "$l_lastsnap"
        l_lastsnap=$l_snapshot
    done
}

#
# Stop a list of SMF services. The services are read in from stdin.
#
stopsvcs() {
    while read -r service; do
        echov "Disabling service $service."
        svcadm disable -st "$service" ||
            {
                echo "Could not disable service $service."
                relaunch
                exit 1
            }
        m_services_to_restart="$m_services_to_restart $service"
    done
}

#
# Relaunch a list of stopped services
#
relaunch() {
    for i in $m_services_to_restart; do
        echov "Restarting service $i"
        svcadm enable "$i" || {
            echo "Couldn't re-enable service $i."
            exit
        }
    done
}

#
# Create a new recursive snapshot.
#
newsnap() {
    snap=$g_zxfer_new_snapshot_name

    if [ "$g_option_R_recursive" != "" ]; then
        echov "Creating recursive snapshot $m_sourcefs@$snap."
        cmd="$g_LZFS snapshot -r $m_sourcefs@$snap"
    else
        echov "Creating snapshot $m_sourcefs@$snap."
        cmd="$g_LZFS snapshot $m_sourcefs@$snap"
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
    l_dest_supported_properties=$($g_RZFS get -Ho property all "$l_dest_pool_name")

    # Get a list of the supported properties from the source
    l_source_pool_name=${initial_source%%/*}
    l_source_supported_properties=$($g_LZFS get -Ho property all "$l_source_pool_name")

    unsupported_properties=

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
    for source in $g_recursive_source_list; do
        # Split up source into source fs, last component
        m_sourcefs=$(echo "$source" | cut -d@ -f1)

        set_actual_dest

        # If using the -m feature, check if the source is mounted,
        # otherwise there's no point in us doing the remounting.
        if [ "$g_option_m_migrate" -eq 1 ]; then
            l_source_to_migrate_mounted=$($g_LZFS get -Ho value mounted "$source")
            if [ "$l_source_to_migrate_mounted" = "yes" ]; then
                echo "The source filesystem is not mounted, why use -m?"
                exit 1
            fi
            mountpoint=$($g_LZFS get -Ho value mountpoint "$source")
            propsource=$($g_LZFS get -Ho source mountpoint "$source")
            echov "Mountpoint is: $mountpoint. Source: $propsource."
        fi

        # Inspect the source and destination snapshots so that we are in position to
        # transfer using the latest common snapshot as a base, and transferring the
        # newer snapshots on source, in order.
        inspect_delete_snap

        # Transfer source properties to destination if required.
        # in the function.
        if [ "$g_option_P_transfer_property" -eq 1 ] || [ "$g_option_o_override_property" != "" ]; then
            transfer_properties
        fi

        # Since we'll mostly wrap around zfs send/receive, we'll leave further
        # error-checking to them.

        #
        # We now have a valid source filesystem, volume or snapshot to copy from and an
        # assumed valid destination filesystem to copy to with a possible snapshot name
        # to give to the destination snapshot.
        #
        copy_snap_multiple

        #
        # Now we have replicated all existing snapshots.
        #
    done
}

#
# zfs send/receive mode, aka zfs-replicate mode, aka normal mode
#
run_zfs_mode() {
    if [ "$g_option_R_recursive" != "" ] && [ "$g_option_N_nonrecursive" != "" ]; then
        throw_usage_error "If using normal mode (i.e. no -S), you must choose either -N to transfer \
a single filesystem or -R to transfer a single filesystem and its children \
recursively, but not both -N and -R at the same time."
    elif [ "$g_option_R_recursive" != "" ]; then
        initial_source="$g_option_R_recursive"
    elif [ "$g_option_N_nonrecursive" != "" ]; then
        initial_source="$g_option_N_nonrecursive"
    else
        throw_usage_error "You must specify a source with either -N or -R."
    fi

    # 0 if not a trailing slash; regex is one character of any sort followed by
    # zero or more of any character until "/" followed by the end of the
    # string.
    # used by set_actual_dest()
    m_trailing_slash=$(echo "$initial_source" | grep -c '..*/$')

    # Now that we know whether there was a trailing slash on the source, no
    # need to confuse things by keeping it on there. Get rid of it.
    initial_source=${initial_source%/}

    # Source and destination can't start with "/", but it's an easy mistake to make
    if [ "$(echo "$initial_source" | grep -c '^/')" -eq "1" ] ||
        [ "$(echo "$g_destination" | grep -c '^/')" -eq "1" ]; then
        throw_usage_error "Source and destination must not begin with \"/\". Note the example."
    fi

    # Checks options to see if appropriate for a source snapshot
    echoV "Checking source snapshot."
    check_snapshot "$initial_source"

    # When using -c you must use -m as well rule. This forces the user
    # To think twice if they really mean to do the migration.
    [ -n "$g_option_c_services" ] && [ "$g_option_m_migrate" -eq 0 ] &&
        throw_error "When using -c, -m needs to be specified as well."

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

    # If recursive option is not selected, then we only iterate once through
    # the initial source as source
    if [ "$g_option_R_recursive" = "" ]; then
        g_recursive_source_list=$initial_source
    fi

    # This gets the root filesystem transferred - e.g.
    # the string after the very last "/" e.g. backup/test/zroot -> zroot
    base_fs=${initial_source##*/}
    # This gets everything but the base_fs, so that we can later delete it from
    # $source
    part_of_source_to_delete=${initial_source%"$base_fs"}

    #
    # If using -s, do a new recursive snapshot, then copy all new snapshots too.
    #
    if [ "$g_option_s_make_snapshot" -eq 1 ] && [ "$g_option_m_migrate" -eq 0 ]; then
        # We snapshot from the base of the initial source
        m_sourcefs=$(echo "$initial_source" | cut -d@ -f1)
        # Create the new snapshot with a unique name.
        newsnap
        # Because there are new snapshots, need to get_zfs_list again
        get_zfs_list
    fi

    #
    # If migrating, stop the affected services, unmount the source filesystem, do
    # one last snapshot and replicate that, then give the destination file system
    # the mount point of the source one and restart the services.
    # Note that the replication and transfer of the mountpoint property is done
    # by the main loop.
    # The restarting of the services is done after the main loop is finished.
    if [ "$g_option_m_migrate" -eq 1 ]; then
        # Check if any services need to be disabled before doing a migration.
        if [ -n "$g_option_c_services" ]; then
            echo "$g_option_c_services" | stopsvcs
        fi

        for source in $g_recursive_source_list; do
            # unmount the source filesystem before doing the last snapshot.
            echov "Unmounting $source."
            $g_LZFS unmount "$source" ||
                {
                    echo "Couldn't unmount source $source."
                    relaunch
                    exit 1
                }
        done

        # We snapshot from the base of the initial source
        m_sourcefs=$(echo "$initial_source" | cut -d@ -f1)

        # Create the last snapshot with a unique name.
        newsnap

        # We include the mountpoint as a property that should be transferred.
        # Note that $g_option_P_transfer_property is automatically set to 1, to transfer the property.
        g_readonly_properties=$(echo "$g_readonly_properties" |
            sed -e 's/,mountpoint//g')

        # Now we must make the script aware of the new snapshots in existence so
        # we can copy them over.
        get_zfs_list
    fi

    if [ "$g_option_g_grandfather_protection" != "" ]; then
        echov "Checking grandfather status of all snapshots marked for deletion..."
        l_old_g_option_d_delete_destination_snapshots=$g_option_d_delete_destination_snapshots
        g_option_d_delete_destination_snapshots=0 # turn off delete so that we are only checking snapshots
        for source in $g_recursive_source_list; do
            set_actual_dest
            inspect_delete_snap
        done
        g_option_d_delete_destination_snapshots=$l_old_g_option_d_delete_destination_snapshots
        echov "Grandfather check passed."
    fi

    copy_filesystems

    if [ "$g_option_m_migrate" -eq 1 ]; then
        # Re-launch any stopped services.
        relaunch
    fi
}
