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
    _snapshot=$1

    _size_dataset=$($g_LZFS send -nPv "$_snapshot" 2>&1) ||
        throw_error "Error calculating estimate: $_size_dataset"
    _size_est=$(echo "$_size_dataset" | grep ^size | tail -n 1 | cut -f 2)

    echo "$_size_est"
}

setup_progress_dialog() {
    _size_est=$1
    _snapshot=$2

    _progress_dialog=$(echo "$g_option_D_display_progress_bar" |
        sed "s#%%size%%#$_size_est#g" |
        sed "s#%%title%%#$_snapshot#g")

    echo "$_progress_dialog"
}

#
# 2024.03.08 - generates error when creating a new dataset in target with:
# cannot receive new filesystem stream: incomplete stream
# Error when executing command.
#
handle_progress_bar_option() {
    _snapshot=$1
    _progress_bar_cmd=""

    # Calculate the size estimate and set up the progress dialog
    _size_est=$(calculate_size_estimate "$_snapshot")
    _progress_dialog=$(setup_progress_dialog "$_size_est" "$_snapshot")

    # Modify the send command to include the progress dialog
    _progress_bar_cmd="| dd obs=1048576 | dd bs=1048576 | $_progress_dialog"

    echo "$_progress_bar_cmd"
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
        if [ "$g_option_V_verbose" != "" ]; then
            l_v="-v"
        fi

        # 2024.03.31 - add support for -w option (raw send)
        l_w=""
        if [ "$g_option_w_raw_send" != "" ]; then
            l_w="-w"
        fi

        echo "$g_cmd_zfs send $l_v$l_w -I $l_prevsnap $l_snapshot"
    fi
}

set_receive_command() {
    l_dest=$1
    echo "$g_cmd_zfs receive $g_option_F_force_rollback $l_dest"
}

wrap_command_with_ssh() {
    _cmd=$1
    _option=$2
    _compress=$3
    _direction=$4

    if [ "$_compress" -eq 0 ]; then
        echo "$g_cmd_ssh $_option \"$_cmd\""
    else
        if [ "$_direction" = "send" ]; then
            echo "$g_cmd_ssh $_option \"$_cmd | $g_cmd_compress\" | $g_cmd_decompress"
        else
            echo "$g_cmd_compress | $g_cmd_ssh $_option \"$g_cmd_decompress | $_cmd\""
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
        l_send_cmd=$(wrap_command_with_ssh "$l_send_cmd" "$g_option_O_origin_host" $g_option_z_compress "send")
    fi
    if [ "$g_option_T_target_host" != "" ]; then
        l_recv_cmd=$(wrap_command_with_ssh "$l_recv_cmd" "$g_option_T_target_host" $g_option_z_compress "receive")
    fi

    # Perform this after ssh wrapping occurs
    if [ "$g_option_D_display_progress_bar" != "" ]; then
        _progress_bar_cmd=$(handle_progress_bar_option "$l_snapshot")
        l_send_cmd="$l_send_cmd $_progress_bar_cmd"
    fi

    execute_command "$l_send_cmd | $l_recv_cmd"
}
