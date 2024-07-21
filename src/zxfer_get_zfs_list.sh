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
#  -t type
#    A comma-separated list of types to display, where type is one of
#    filesystem, snapshot, volume, bookmark, or all.  For example,
#    specifying -t snapshot displays only snapshots.
#
get_zfs_list() {
    echoV "Begin get_zfs_list()"

    # create temporary files used by the background processes
    l_lzfs_list_hr_s_snap_tmp_file=$(get_temp_file)
    l_rzfs_list_hr_snap_tmp_file=$(get_temp_file)

    # This method proved to be slower when using ssh for remote source.
    #
    # it is important to get this in ascending order because when getting
    # in descending order, the datasets names are not ordered as we want.
    # Don't use -S creation for this command, instead, reverse the results below
    #
    # Get a list of source snapshots in ascending order by creation date
    if [ $g_option_x_args_parallel -gt 1 ]; then
        # 2024.07.15
        # xargs mangles the output of the snapshots and is not reliable.
        # parallel is used instead which must be installed on source systems
        #
        # eventhough the snapshots are not ordered in creation time globally,
        # they are ordered by dataset which is what is needed.
        #
        # if the g_LZFS command is remote, then escape the command to execute
        # it wrapped around an ssh command
        if [ ! "$g_option_O_origin_host" = "" ]; then
            #l_cmd="$g_cmd_ssh $g_option_O_origin_host \"$g_cmd_zfs list -Hr -o name $initial_source | xargs -n 1 -P $g_option_x_args_parallel -I {} sh -c '$g_cmd_zfs list -H -o name -s creation -t snapshot {}'\""
            # use parallel to prevent mangling of output
            # when there are a lot of snapshtos, the output can be several megabytes and benefit
            # from compression. We use zstd -9 due to the small size and time that it takes
            # to generate the output. zstd -9 has shown better compression than zstd -12
            # and significantly better compression than gzip.
            # zstd -19 takes too long
            l_cmd="$g_cmd_ssh $g_option_O_origin_host \"$g_cmd_zfs list -Hr -o name $initial_source | parallel -j $g_option_x_args_parallel --line-buffer '$g_cmd_zfs list -H -o name -s creation -t snapshot {}' | zstd -9 \" | zstd -d"
        else
            #l_cmd="$g_LZFS list -Hr -o name $initial_source | xargs -n 1 -P $g_option_x_args_parallel -I {} sh -c '$g_LZFS list -H -o name -s creation -t snapshot {}'"
            l_cmd="$g_LZFS list -Hr -o name $initial_source | parallel -j $g_option_x_args_parallel --line-buffer '$g_LZFS list -H -o name -s creation -t snapshot {}'"
        fi

        echoV "Executing command in the background: $l_cmd"
        eval "$l_cmd" > "$l_lzfs_list_hr_s_snap_tmp_file" &

    else
        l_cmd="$g_LZFS list -Hr -o name -s creation -t snapshot $initial_source"

        execute_background_cmd \
            "$l_cmd" \
            "$l_lzfs_list_hr_s_snap_tmp_file"
    fi

    # determine the last dataset in $initial_source. This will be the last
    # dataset after a forward slash "/" or if no forward slash exists, then
    # is is the name of the dataset itself.
    l_source_dataset=$(echo "$initial_source" | awk -F'/' '{print $NF}')

    l_destination_dataset="$g_destination/$l_source_dataset"

    # 2024.07.09
    # we only need the snapshots of the intended destination dataset, not
    # all the snapshots of the parent $g_destination.
    # In addition, the sorting by creation time has been removed in the
    # destination since it is not needed. We only need the names of the
    # snapshots. This significantly improves performance as the metadata
    # doesn't need to be searched for the creation time of each snapshot.

    # check if the destination zfs dataset exists before listing snapshots
    if "$g_RZFS" list "$l_destination_dataset" >/dev/null 2>&1; then
        # dataset exists

        # we only need the names of the snapshots, they don't need to be sorted
        execute_background_cmd \
            "$g_RZFS list -Hr -o name -t snapshot $l_destination_dataset" \
            "$l_rzfs_list_hr_snap_tmp_file"
    else
        # dataset does not exist
        echo "" >"$l_rzfs_list_hr_snap_tmp_file"
    fi

    # these commands can be run serially because listing snapshots in the
    # background take the longest

    # get a list of datasets in the target
    l_cmd="$g_RZFS list -t filesystem,volume -H -o name"
    echoV "Running command: $l_cmd"
    g_rzfs_list_ho=$($l_cmd)

    # get a list of source datasets
    l_cmd="$g_LZFS list -t filesystem,volume -Hr -o name $initial_source"
    echoV "Running command: $l_cmd"
    g_recursive_source_list=$($l_cmd)

    # get a list of desintation datasets
    l_cmd="$g_RZFS list -t filesystem,volume -Hr -o name $g_destination"
    echoV "Running command: $l_cmd"
    g_recursive_dest_list=$($l_cmd)

    echoV "Waiting for background processes to finish."
    wait
    echoV "Wait finished."

    l_lzfs_list_hr_s_snap=$(cat "$l_lzfs_list_hr_s_snap_tmp_file")
    g_rzfs_list_hr_snap=$(cat "$l_rzfs_list_hr_snap_tmp_file")

    # get the reversed order (not using tac due to solaris compatibility)
    g_lzfs_list_hr_S_snap=$(echo "$l_lzfs_list_hr_s_snap" | cat -n | sort -nr | cut -c 8-)

    # remove temporary files
    rm "$l_lzfs_list_hr_s_snap_tmp_file" \
        "$l_rzfs_list_hr_snap_tmp_file"

    if [ "$l_lzfs_list_hr_s_snap" = "" ]; then
        throw_error "Failed to retrieve snapshots from the source" 3
    fi

    # the destination may not have any snapshots if it was just created so
    # there is no need to check if it is empty as that is a valid state

    # perform other checks
    if [ "$g_rzfs_list_ho" = "" ]; then
        throw_error "Failed to retrieve datasets from the destination" 3
    fi

    if [ "$g_recursive_source_list" = "" ]; then
        throw_usage_error "Failed to retrieve list of datasets from the source"
    fi

    if [ "$g_recursive_dest_list" = "" ]; then
        throw_usage_error "Failed to retrieve list of datasets from the destination"
    fi

    echoV "End get_zfs_list()"
}
