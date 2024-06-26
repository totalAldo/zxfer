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
#. ./zxfer_globals.sh; . ./zxfer_inspect_delete_snap.sh; . ./zxfer_zfs_mode.sh;

#
# Returns a list of destination snapshots that don't exist in the source.
#
get_dest_snapshots_to_delete() {
    l_zfs_source_snaps=$1
    l_zfs_dest_snaps=$2

    # Create temporary files
    l_original_zfs_dest_snaps=$(get_temp_file)
    l_src_tmp=$(get_temp_file)
    l_dest_tmp=$(get_temp_file)
    l_snaps_to_delete_tmp=$(get_temp_file)

    # Write the snapshot names to the temporary files
    echo "$l_zfs_dest_snaps" >"$l_original_zfs_dest_snaps"
    echo "$l_zfs_source_snaps" | tr ' ' '\n' | sort | $g_cmd_awk -F'@' "{print \$2}" >"$l_src_tmp"
    echo "$l_zfs_dest_snaps" | tr ' ' '\n' | sort | $g_cmd_awk -F'@' "{print \$2}" >"$l_dest_tmp"

    # Use comm to find snapshots in l_dest_tmp that don't have a match in l_src_tmp
    # write the snapshots to delete to a temporary file for use by grep
    comm -13 "$l_src_tmp" "$l_dest_tmp" >"$l_snaps_to_delete_tmp"

    # Use grep to find the matching lines in l_original_zfs_dest_snaps
    l_dest_snaps_to_delete=$(grep -F -f "$l_snaps_to_delete_tmp" "$l_original_zfs_dest_snaps")

    # Clean up temporary files
    rm "$l_original_zfs_dest_snaps" \
        "$l_src_tmp" \
        "$l_dest_tmp" \
        "$l_snaps_to_delete_tmp"

    # Print the matching lines
    echo "$l_dest_snaps_to_delete"
}

#
# find the most recent common snapshot (since the lists are sorted in descending
# order by by creation date, the first common snapshot is the most recent common)
#
set_last_common_snapshot() {
    echoV "Begin set_last_common_snapshot()"
    l_zfs_source_snaps=$1
    l_zfs_dest_snaps=$2

    g_found_last_common_snap=0

    # Convert the source snapshots into a list with newlines
    l_src_snap_list=$(echo "$l_zfs_source_snaps" | tr ' ' '\n')

    # loop through the destination snapshots sorted in descending creation order
    # to find the most recent common snapshot
    for l_dest_snap in $l_zfs_dest_snaps; do
        l_dest_snap_name=$(extract_snapshot_name "$l_dest_snap")

        # Use grep to check if the destination snapshot is in the source snapshots
        # optimize grep by using q for first match, F since we are searching for a fixed string
        if echo "$l_src_snap_list" | grep -qF "$l_dest_snap_name"; then
            g_found_last_common_snap=1
            g_last_common_snap="$l_dest_snap_name"
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

    l_diff_sec=$((l_current_date - l_snap_date))
    l_diff_day=$((l_diff_sec / 86400))

    if [ $l_diff_day -ge "$g_option_g_grandfather_protection" ]; then
        l_snap_date_english=$($g_RZFS get -H -o value creation "$l_destination_snapshot")
        l_current_date_english=$(date)
        l_error_msg="On the destination there is a snapshot marked for destruction
            by zxfer that is protected by the use of the \"grandfather
            protection\" option, -g.

            You have set grandfather protection at $g_option_g_grandfather_protection days.
            Snapshot name: $l_destination_snapshot
            Snapshot age : $l_diff_day days old
            Snapshot date: $l_snap_date_english.
            Your current system date: $l_current_date_english.

            Either amend/remove option g, fix your system date, or manually
            destroy the offending snapshot. Also double check that your
            snapshot management tool isn't erroneously deleting source snapshots.
            Note that for option g to work correctly, you should set it just
            above a number of days that will preclude \"father\" snapshots from
            being encountered."

        throw_usage_error "$l_error_msg"
    fi
}

#
# Delete snapshots in destination that aren't in source
#
delete_snaps() {
    echoV "Begin delete_snaps()"
    l_zfs_source_snaps=$1
    l_zfs_dest_snaps=$2

    l_snaps_to_delete=$(get_dest_snapshots_to_delete "$l_zfs_source_snaps" "$l_zfs_dest_snaps")

    # deletes non-common snaps on destination if asked to.
    for l_snap_to_delete in $l_snaps_to_delete; do
        if [ "$g_option_g_grandfather_protection" != "" ]; then
            grandfather_test "$l_snap_to_delete"
        fi

        #echoV "Destroying destination snapshot $l_snap_to_delete."
        g_is_performed_send_destroy=1
        l_cmd="$g_RZFS destroy $l_snap_to_delete"

        # could combine multiple snapshots into one comma delimited destroy command
        # pass 1 to continue command if it fails
        #execute_command "$l_cmd" 1
        echov "$l_cmd"
        execute_background_cmd "$l_cmd" /dev/null
    done

    echoV "End delete_snaps()"
}

set_src_snapshot_transfer_list() {
    l_zfs_source_snaps=$1

    l_found_common=0

    g_src_snapshot_transfer_list=""

    # This prepares a list of source snapshots to transfer, beginning with
    # the first snapshot after the last common one.
    for l_test_snap in $l_zfs_source_snaps; do
        if [ "$l_test_snap" != "$source@$g_last_common_snap" ]; then
            if [ $l_found_common = 0 ]; then
                g_src_snapshot_transfer_list="$l_test_snap,$g_src_snapshot_transfer_list"
            fi
        else
            l_found_common=1
        fi
    done

    g_src_snapshot_transfer_list=$(echo "$g_src_snapshot_transfer_list" | tr -s "," "\n")
}

inspect_delete_snap() {
    # Get the list of source snapshots in descending order by creation date
    l_zfs_source_snaps=$(echo "$g_lzfs_list_hr_S_snap" | grep "^$source@")

    # Get the list of destination snapshots in descending order by creation date
    l_zfs_dest_snaps=$(echo "$g_rzfs_list_hr_S_snap" | grep "^$g_actual_dest@")

    # Deletes non-common snaps on destination if asked to.
    if [ "$g_option_d_delete_destination_snapshots" -eq 1 ]; then
        delete_snaps "$l_zfs_source_snaps" "$l_zfs_dest_snaps"
    fi

    # Find the most recent common snapshot on source and destination.
    set_last_common_snapshot "$l_zfs_source_snaps" "$l_zfs_dest_snaps"

    # Create a list of source snapshots to transfer, beginning with the
    # first snapshot after the last common one.
    set_src_snapshot_transfer_list "$l_zfs_source_snaps"
}
