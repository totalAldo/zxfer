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

#
# Returns a list of destination snapshots that don't exist in the source.
#
get_dest_snapshots_to_delete() {
    _zfs_source_snaps=$1
    _zfs_dest_snaps=$2

    # Create temporary files
    _original_zfs_dest_snaps=$(get_temp_file)
    _src_tmp=$(get_temp_file)
    _dest_tmp=$(get_temp_file)
    _snaps_to_delete_tmp=$(get_temp_file)

    # Write the snapshot names to the temporary files
    echo "$_zfs_dest_snaps" >"$_original_zfs_dest_snaps"
    echo "$_zfs_source_snaps" | tr ' ' '\n' | sort | $g_cmd_awk -F'@' "{print \$2}" >"$_src_tmp"
    echo "$_zfs_dest_snaps" | tr ' ' '\n' | sort | $g_cmd_awk -F'@' "{print \$2}" >"$_dest_tmp"

    # Use comm to find snapshots in _dest_tmp that don't have a match in _src_tmp
    # write the snapshots to delete to a temporary file for use by grep
    comm -13 "$_src_tmp" "$_dest_tmp" >"$_snaps_to_delete_tmp"

    # Use grep to find the matching lines in _original_zfs_dest_snaps
    _dest_snaps_to_delete=$(grep -F -f "$_snaps_to_delete_tmp" "$_original_zfs_dest_snaps")

    # Clean up temporary files
    rm "$_original_zfs_dest_snaps" \
        "$_src_tmp" \
        "$_dest_tmp" \
        "$_snaps_to_delete_tmp"

    # Print the matching lines
    echo "$_dest_snaps_to_delete"
}

#
# find the most recent common snapshot (since the lists or sorted in descending
# order by by creation date, the first common snapshot is the most recent common)
#
set_last_common_snapshot() {
    echoV "Begin set_last_common_snapshot()"
    _zfs_source_snaps=$1
    _zfs_dest_snaps=$2

    g_found_last_common_snap=0

    # Convert the source snapshots into a list with newlines
    _src_snap_list=$(echo "$_zfs_source_snaps" | tr ' ' '\n')

    # loop through the destination snapshots sorted in descending creation order
    # to find the most recent common snapshot
    for _dest_snap in $_zfs_dest_snaps; do
        _dest_snap_name=$(extract_snapshot_name "$_dest_snap")

        # Use grep to check if the destination snapshot is in the source snapshots
        _is_match_found=$(echo "$_src_snap_list" | grep "$_dest_snap_name$")
        if [ "$_is_match_found" != "" ]; then
            g_found_last_common_snap=1
            g_last_common_snap="$_dest_snap_name"
            echoV "Found last common snapshot: $g_last_common_snap."
            # once found, exit the function
            return
        fi
    done
    echoV "End set_last_common_snapshot()"
}

#
# Tests a snapshot to see if it is older than the grandfather option allows for.
#
grandfather_test() {
    l_destination_snapshot=$1

    l_current_date=$(date +%s) # current date in seconds from 1970
    l_snap_date=$($g_RZFS get -H -o value -p creation "$l_destination_snapshot")

    diff_sec=$((l_current_date - l_snap_date))
    diff_day=$((diff_sec / 86400))

    if [ $diff_day -ge "$g_option_g_grandfather_protection" ]; then
        snap_date_english=$($g_RZFS get -H -o value creation "$l_destination_snapshot")
        current_date_english=$(date)
        echo "Error: On the destination there is a snapshot marked for destruction"
        echo "by zxfer that is protected by the use of the \"grandfather"
        echo "protection\" option, -g."
        echo
        echo "You have set grandfather protection at $g_option_g_grandfather_protection days."
        echo "Snapshot name: $l_destination_snapshot"
        echo "Snapshot age : $diff_day days old"
        echo "Snapshot date: $snap_date_english."
        echo "Your current system date: $current_date_english."
        echo
        echo "Either amend/remove option g, fix your system date, or manually"
        echo "destroy the offending snapshot. Also double check that your"
        echo "snapshot management tool isn't erroneously deleting source snapshots."
        echo "Note that for option g to work correctly, you should set it just"
        echo "above a number of days that will preclude \"father\" snapshots from"
        echo "being encountered."
        echo
        usage
        beep
        exit 1
    fi
}

delete_snaps() {
    echoV "Begin delete_snaps()"
    _zfs_source_snaps=$1
    _zfs_dest_snaps=$2

    _snaps_to_delete=$(get_dest_snapshots_to_delete "$_zfs_source_snaps" "$_zfs_dest_snaps")

    # deletes non-common snaps on destination if asked to.
    for _snap_to_delete in $_snaps_to_delete; do
        if [ "$g_option_g_grandfather_protection" != "" ]; then
            grandfather_test "$_snap_to_delete"
        fi

        #echoV "Destroying destination snapshot $_snap_to_delete."
        _cmd="$g_RZFS destroy $_snap_to_delete"
        # pass 1 to continue command if it fails
        execute_command "$_cmd" 1
    done

    echoV "End delete_snaps()"
}

set_src_snapshot_transfer_list() {
    _zfs_source_snaps=$1

    _found_common=0

    g_src_snapshot_transfer_list=""

    # This prepares a list of source snapshots to transfer, beginning with
    # the first snapshot after the last common one.
    for test_snap in $_zfs_source_snaps; do
        if [ "$test_snap" != "$source@$g_last_common_snap" ]; then
            if [ $_found_common = 0 ]; then
                g_src_snapshot_transfer_list="$test_snap,$g_src_snapshot_transfer_list"
            fi
        else
            _found_common=1
        fi
    done

    g_src_snapshot_transfer_list=$(echo "$g_src_snapshot_transfer_list" | tr -s "," "\n")
}

inspect_delete_snap() {
    # Get the list of source snapshots in descending order by creation date
    _zfs_source_snaps=$(echo "$g_lzfs_list_hr_S_snap" | grep "^$source@") >/dev/null 2>&1

    # Get the list of destination snapshots in descending order by creation date
    _zfs_dest_snaps=$(echo "$g_rzfs_list_hr_S_snap" | grep "^$g_actual_dest@") >/dev/null 2>&1

    # Deletes non-common snaps on destination if asked to.
    if [ $g_option_d_delete_destination_snapshots -eq 1 ]; then
        delete_snaps "$_zfs_source_snaps" "$_zfs_dest_snaps"
    fi

    # Find the most recent common snapshot on source and destination.
    set_last_common_snapshot "$_zfs_source_snaps" "$_zfs_dest_snaps"

    # Create a list of source snapshots to transfer, beginning with the
    # first snapshot after the last common one.
    set_src_snapshot_transfer_list "$_zfs_source_snaps"
}
