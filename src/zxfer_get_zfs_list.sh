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
#. ./zxfer_globals.sh;  . ./zxfer_get_zfs_list.sh; . ./zxfer_inspect_delete_snap.sh;

#
# Determine the source snapshots sorted by creation time. Since this
# can take a long time, the command is run in the background. In addition,
# to optimize the process, gnu parallel is used to retrieve snapshots from
# multiple datasets concurrently.
#
write_source_snapshot_list_to_file() {
    l_outfile=$1

    #
    # it is important to get this in ascending order because when getting
    # in descending order, the datasets names are not ordered as we want.
    # Don't use -S creation for this command, instead, reverse the results below
    #
    # Get a list of source snapshots in ascending order by creation date
    if [ $g_option_j_jobs -gt 1 ]; then
        # 2024.07.15
        # xargs mangles the output of the snapshots and is not reliable.
        # gnu parallel is used instead which must be installed on source systems
        #
        # eventhough the snapshots are not ordered in creation time globally,
        # they are ordered by dataset which is what is needed.
        #
        # if the g_LZFS command is remote, then escape the command to execute
        # it wrapped around an ssh command
        if [ ! "$g_option_O_origin_host" = "" ]; then
            #l_cmd="$g_cmd_ssh $g_option_O_origin_host \"$g_cmd_zfs list -Hr -o name $initial_source | xargs -n 1 -P $g_option_j_jobs -I {} sh -c '$g_cmd_zfs list -H -o name -s creation -t snapshot {}'\""
            # use gnu parallel to prevent mangling of output
            # when there are a lot of snapshtos, the output can be several megabytes and benefit
            # from compression. We use zstd -9 due to the small size and time that it takes
            # to generate the output. zstd -9 has shown better compression than zstd -12
            # and significantly better compression than gzip.
            # zstd -19 takes too long

            # check if compression is enabled
            if [ "$g_option_z_compress" -eq 1 ]; then
                # IllumOS requires -d 1 when listing snapshots for one dataset
                l_cmd="$g_cmd_ssh $g_option_O_origin_host \"$g_cmd_zfs list -Hr -o name $initial_source | $g_cmd_parallel -j $g_option_j_jobs --line-buffer '$g_cmd_zfs list -H -o name -s creation -d 1 -t snapshot {}' | zstd -9\" | zstd -d"
            else
                l_cmd="$g_cmd_ssh $g_option_O_origin_host \"$g_cmd_zfs list -Hr -o name $initial_source | $g_cmd_parallel -j $g_option_j_jobs --line-buffer '$g_cmd_zfs list -H -o name -s creation -d 1 -t snapshot {}'\""
            fi
        else
            #l_cmd="$g_LZFS list -Hr -o name $initial_source | xargs -n 1 -P $g_option_j_jobs -I {} sh -c '$g_LZFS list -H -o name -s creation -d 1 -t snapshot {}'"
            l_cmd="$g_LZFS list -Hr -o name $initial_source | $g_cmd_parallel -j $g_option_j_jobs --line-buffer '$g_LZFS list -H -o name -s creation -d 1 -t snapshot {}'"
        fi

        echoV "Running command in the background: $l_cmd"
        eval "$l_cmd" >"$l_outfile" &

    else
        l_cmd="$g_LZFS list -Hr -o name -s creation -t snapshot $initial_source"

        execute_background_cmd \
            "$l_cmd" \
            "$l_outfile"
    fi
}

# We only need the snapshots of the intended destination dataset, not
# all the snapshots of the parent $g_destination.
# In addition, sorting by creation time has been removed in the
# destination since it is not needed.
# This significantly improves performance as the metadata
# doesn't need to be searched for the creation time of each snapshot.
# Parallelization support has been added and is useful in situations when
# the ARC is not populated such as when a removable disk us mounted.
write_destination_snapshot_list_to_files() {
    l_rzfs_list_hr_snap_tmp_file=$1
    l_dest_snaps_stripped_sorted_tmp_file=$2

    # determine the last dataset in $initial_source. This will be the last
    # dataset after a forward slash "/" or if no forward slash exists, then
    # is is the name of the dataset itself.
    l_source_dataset=$(echo "$initial_source" | awk -F'/' '{print $NF}')

    l_destination_dataset="$g_destination/$l_source_dataset"

    # check if the destination zfs dataset exists before listing snapshots
    if [ $(exists_destination "$l_destination_dataset") -eq 1 ]; then
        # dataset exists

        # using parallel when metadata cached is slower - disabling
        #if [ $g_option_j_jobs -gt 1 ]; then
        #    # if the g_RZFS command is remote, then escape the command to execute
        #    if [ ! "$g_option_T_origin_host" = "" ]; then
        #        if [ "$g_option_z_compress" -eq 1 ]; then
        #            l_cmd="$g_cmd_ssh $g_option_T_target_host \"$g_cmd_zfs list -Hr -o name $l_destination_dataset | $g_cmd_parallel -j $g_option_j_jobs --line-buffer '$g_cmd_zfs list -H -o name -d 1 -t snapshot {}' | zstd -9\" | zstd -d"
        #        else
        #            l_cmd="$g_cmd_ssh $g_option_T_target_host \"$g_cmd_zfs list -Hr -o name $l_destination_dataset | $g_cmd_parallel -j $g_option_j_jobs --line-buffer '$g_cmd_zfs list -H -o name -d 1 -t snapshot {}'\""
        #        fi
        #    else
        #        l_cmd="$g_RZFS list -Hr -o name $l_destination_dataset | $g_cmd_parallel -j $g_option_j_jobs --line-buffer '$g_RZFS list -H -o name -d 1 -t snapshot {}'"
        #    fi
        #else
        #    # do not perform in the background so we can sort the results
        #    # before the longest operation is complete
        #    l_cmd="$g_RZFS list -Hr -o name -t snapshot $l_destination_dataset"
        #fi

        l_cmd="$g_RZFS list -Hr -o name -t snapshot $l_destination_dataset"
        echoV "Running command: $l_cmd"
        # make sure to eval and then pipe the contents to the file in case
        # the command uses ssh
        eval "$l_cmd" > "$l_rzfs_list_hr_snap_tmp_file"

    else
        # dataset does not exist
        echoV "Destination dataset does not exist: $l_destination_dataset"
        echo "" >"$l_rzfs_list_hr_snap_tmp_file"
    fi

    # sort the destination snapshots and replace the destination dataset with the prefix
    # of the source for comparison
    l_cmd="sed -e 's|$l_destination_dataset|$initial_source|g' $l_rzfs_list_hr_snap_tmp_file | sort > $l_dest_snaps_stripped_sorted_tmp_file"
    echoV "Running command: $l_cmd"
    eval "$l_cmd"
}

# compare the source and destination snapshots and identify source datasets
# that are not in the destination. Set g_recursive_source_list to the
# datasets that contain snapshots that are not in the destination.
# Afterwards, g_recursive_source_list only contains the names of
# the datasets that need to be transferred.
set_g_recursive_source_list() {
    l_lzfs_list_hr_s_snap_tmp_file=$1
    l_dest_snaps_stripped_sorted_tmp_file=$2

    l_source_snaps_sorted_tmp_file=$(get_temp_file)

    # sort the source snapshots for use with comm
    # wait until background processes are finished before attempting to sort
    l_cmd="sort $l_lzfs_list_hr_s_snap_tmp_file > $l_source_snaps_sorted_tmp_file"
    echoV "Running command: $l_cmd"
    eval "$l_cmd"

    g_recursive_source_list=$(comm -23 \
        "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" |
        "$g_cmd_awk" -F@ '{print $1}' | sort | uniq)

    # debugging
    if [ "$g_option_V_very_verbose" -eq 1 ]; then
        echo "==== Snapshots in source but not in destination ===="
        comm -23 "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
        echo "===================================================="
        echo "g_recursive_source_list: (Source datasets that differ from destination)"
        echo "$g_recursive_source_list"
        echo "===================================================="
    fi

    echoV "Source dataset count: $(echo "$g_recursive_source_list" | wc -l)"

    if [ "$g_recursive_source_list" = "" ]; then
        echov "No snapshots to transfer."
    fi

    rm "$l_source_snaps_sorted_tmp_file"
}

#
# Caches zfs list commands to cut execution time
# Uses background processes when listing snapshots to speed up the results.
# Often, the source and destination datasets reside on different pools
# and running the zfs list commands in parallel can speed up the process.
#
# zfs list options used in this function include:
# -H  Used for scripting mode.  Do not print headers and separate fields by
#     a single tab instead of arbitrary white space.
# -r  Recursively display any children of the dataset on the command line.
# -o property
#    A comma-separated list of properties to display.  The property must
#    be:
#    •   One of the properties described in the Native Properties section
#        of zfsprops(7)
#    •   A user property
#    •   The value name to display the dataset name
#    •   The value space to display space usage properties on file systems
#        and volumes.  This is a shortcut for specifying
#        -o name,avail,used,usedsnap,usedds,usedrefreserv,usedchild -t
#        filesystem,volume.
# -s property
#    A property for sorting the output by column in ascending order based
#    on the value of the property.  The property must be one of the
#    properties described in the Properties section of zfsprops(7) or the
#    value name to sort by the dataset name.  Multiple properties can be
#    specified at one time using multiple -s property options.  Multiple
#    -s options are evaluated from left to right in decreasing order of
#    importance.  The following is a list of sorting criteria:
#    •   Numeric types sort in numeric order.
#    •   String types sort in alphabetical order.
#    •   Types inappropriate for a row sort that row to the literal
#        bottom, regardless of the specified ordering.
#
#    If no sorting options are specified the existing behavior of zfs list
#    is preserved.
# -S property
#    Same as -s, but sorts by property in descending order.
# -t type
#    A comma-separated list of types to display, where type is one of
#    filesystem, snapshot, volume, bookmark, or all.  For example,
#    specifying -t snapshot displays only snapshots.
#
get_zfs_list() {
    echoV "Begin get_zfs_list()"

    # create temporary files used by the background processes
    l_lzfs_list_hr_s_snap_tmp_file=$(get_temp_file)

    #
    # BEGIN background process
    #
    write_source_snapshot_list_to_file "$l_lzfs_list_hr_s_snap_tmp_file"

    #
    # Run as many commands prior to the wait command as possible.
    #

    l_rzfs_list_hr_snap_tmp_file=$(get_temp_file)
    l_dest_snaps_stripped_sorted_tmp_file=$(get_temp_file)

    # this function writes to both files passed as parameters
    write_destination_snapshot_list_to_files "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"

    g_rzfs_list_hr_snap=$(cat "$l_rzfs_list_hr_snap_tmp_file")

    # get a list of all desintation datasets recursively
    l_cmd="$g_RZFS list -t filesystem,volume -Hr -o name $g_destination"
    echoV "Running command: $l_cmd"
    g_recursive_dest_list=$($l_cmd)

    echoV "Waiting for background processes to finish."
    wait
    echoV "Background processes finished."

    #
    # END background process
    #
    set_g_recursive_source_list "$l_lzfs_list_hr_s_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"

    # get the reversed order (not using tac due to solaris compatibility)
    g_lzfs_list_hr_S_snap=$(cat -n "$l_lzfs_list_hr_s_snap_tmp_file" | sort -nr | cut -c 8-)

    # remove temporary files
    rm "$l_lzfs_list_hr_s_snap_tmp_file" \
        "$l_rzfs_list_hr_snap_tmp_file" \
        "$l_dest_snaps_stripped_sorted_tmp_file"

    #
    # Errors
    #

    if [ "$g_lzfs_list_hr_S_snap" = "" ]; then
        throw_error "Failed to retrieve snapshots from the source" 3
    fi

    if [ "$g_recursive_dest_list" = "" ]; then
        throw_usage_error "Failed to retrieve list of datasets from the destination"
    fi

    echoV "End get_zfs_list()"
}
