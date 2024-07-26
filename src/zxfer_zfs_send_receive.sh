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
# ZFS MODE FUNCTIONS RELATED TO zfs_send_receive
################################################################################

#
# The snapshot size is estimated. The estimate does not take into consideration
# the compression ratio of the data. The estimate is based on the size of the
# dataset. When compression is used, the bar will terminate sooner,
# ending at the compression ratio.
# Takes $g_LZFS (which may contain the ssh commmand if -O is used)
#
calculate_size_estimate() {
    l_snapshot=$1

    l_size_dataset=$($g_LZFS send -nPv "$l_snapshot" 2>&1) ||
        throw_error "Error calculating estimate: $l_size_dataset"
    l_size_est=$(echo "$l_size_dataset" | grep ^size | tail -n 1 | cut -f 2)

    echo "$l_size_est"
}

setup_progress_dialog() {
    l_size_est=$1
    l_snapshot=$2

    l_progress_dialog=$(echo "$g_option_D_display_progress_bar" |
        sed "s#%%size%%#$l_size_est#g" |
        sed "s#%%title%%#$l_snapshot#g")

    echo "$l_progress_dialog"
}

#
# 2024.03.08 - generates error when creating a new dataset in target with:
# cannot receive new filesystem stream: incomplete stream
# Error when executing command.
#
handle_progress_bar_option() {
    l_snapshot=$1
    l_progress_bar_cmd=""

    # Calculate the size estimate and set up the progress dialog
    l_size_est=$(calculate_size_estimate "$l_snapshot")
    l_progress_dialog=$(setup_progress_dialog "$l_size_est" "$l_snapshot")

    # Modify the send command to include the progress dialog
    l_progress_bar_cmd="| dd obs=1048576 | dd bs=1048576 | $l_progress_dialog"

    echo "$l_progress_bar_cmd"
}

set_send_command() {
    l_snapshot=$1
    l_prevsnap=$2

    if [ -z "$l_prevsnap" ]; then
        echo "$g_cmd_zfs send $l_snapshot"
    else
        # previous version
        #echo "$g_cmd_zfs send -i $l_prevsnap $l_snapshot"

        # 2024.03.19 new version - send all incremental snapshots in one stream
        l_v=""
        if [ "$g_option_V_very_verbose" -eq 1 ]; then
            l_v="-v"
        fi

        # 2024.03.31 - add support for -w option (raw send)
        l_w=""
        if [ "$g_option_w_raw_send" -eq 1 ]; then
            l_w="-w"
        fi

        echo "$g_cmd_zfs send $l_v $l_w -I $l_prevsnap $l_snapshot"
    fi
}

set_receive_command() {
    l_dest=$1
    echo "$g_cmd_zfs receive $g_option_F_force_rollback $l_dest"
}

wrap_command_with_ssh() {
    l_cmd=$1
    l_option=$2
    l_compress=$3
    l_direction=$4

    if [ "$l_compress" -eq 0 ]; then
        echo "$g_cmd_ssh $l_option \"$l_cmd\""
    else
        if [ "$l_direction" = "send" ]; then
            echo "$g_cmd_ssh $l_option \"$l_cmd | $g_cmd_compress\" | $g_cmd_decompress"
        else
            echo "$g_cmd_compress | $g_cmd_ssh $l_option \"$g_cmd_decompress | $l_cmd\""
        fi
    fi
}

#
# Handle zfs send/receive
# Takes $g_option_D_display_progress_bar $g_option_z_compress, $g_option_O_origin_host, $g_option_T_target_host
#
zfs_send_receive() {
    l_snapshot=$1
    l_dest=$2
    l_prevsnap=$3

    # Set up the send and receive commands
    l_send_cmd=$(set_send_command "$l_snapshot" "$l_prevsnap")
    l_recv_cmd=$(set_receive_command "$l_dest")

    if [ "$g_option_O_origin_host" != "" ]; then
        l_send_cmd=$(wrap_command_with_ssh "$l_send_cmd" "$g_option_O_origin_host" "$g_option_z_compress" "send")
    fi
    if [ "$g_option_T_target_host" != "" ]; then
        l_recv_cmd=$(wrap_command_with_ssh "$l_recv_cmd" "$g_option_T_target_host" "$g_option_z_compress" "receive")
    fi

    # Perform this after ssh wrapping occurs
    if [ "$g_option_D_display_progress_bar" != "" ]; then
        _progress_bar_cmd=$(handle_progress_bar_option "$l_snapshot")
        l_send_cmd="$l_send_cmd $_progress_bar_cmd"
    fi

    g_is_performed_send_destroy=1

    if [ "$g_option_j_jobs" -gt 1 ]; then
        # implement naive job control.
        # if there are more than this many jobs, wait until they are all
        # completed before spawning new ones
        if [ "$g_count_zfs_send_jobs" -ge "$g_option_j_jobs" ]; then
            echov "Max jobs reached [$g_count_zfs_send_jobs]. Waiting for jobs to complete."
            wait
            g_count_zfs_send_jobs=0
        fi

        # increment the job count
        g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
        execute_command "$l_send_cmd | $l_recv_cmd" &
    else
        execute_command "$l_send_cmd | $l_recv_cmd"
    fi
}
