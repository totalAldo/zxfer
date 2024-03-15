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

# Function to extract snapshot name
extract_snapshot_name() {
    echo "$1" | grep @ | cut -d@ -f2
}

#
# Initializes OS and local/remote specific variables
#
init_variables() {
    g_home_operating_system=$(get_os "")

    # determine the source operating system
    if [ "$g_option_O_origin_host" != "" ]; then
        g_source_operating_system=$(get_os "$g_cmd_ssh $g_option_O_origin_host")
    else
        g_source_operating_system=$(get_os "")
    fi

    # determine the destination operating system
    if [ "$g_option_T_target_host" != "" ]; then
        g_destination_operating_system=$(get_os "$g_cmd_ssh $g_option_T_target_host")
    else
        g_destination_operating_system=$(get_os "")
    fi

    if [ $g_option_e_restore_property_mode -eq 1 ]; then
        g_cmd_cat=$("$g_cmd_ssh" "$g_option_O_origin_host" which cat)
    fi

    if [ $g_option_S_rsync_mode -eq 1 ]; then
        g_cmd_rsync=$(which rsync)
    fi

    if [ "$g_home_operating_system" = "SunOS" ]; then
        g_cmd_awk=$(which gawk)
    fi
}

#
# Checks that options make sense, etc.
#
consistency_check() {
    # disallow backup and restore of properties at same time
    if [ $g_option_k_backup_property_mode -eq 1 ] &&
        [ $g_option_e_restore_property_mode -eq 1 ]; then
        throw_usage_error "You cannot bac(k)up and r(e)store properties at the same time."
    fi

    # disallow both beep modes, enforce using one or the other.
    if [ $g_option_b_beep_always -eq 1 ] &&
        [ $g_option_B_beep_on_success -eq 1 ]; then
        throw_usage_error "You cannot use both beep modes at the same time."
    fi

    if [ $g_option_S_rsync_mode -eq 1 ]; then
        # rsync mode

        # check for incompatible options
        if [ "$g_option_F_force_rollback" = "-F" ]; then
            throw_usage_error "-F option cannot be used with -S (rsync mode)"
        fi

        if [ $g_option_s_make_snapshot -eq 1 ]; then
            throw_usage_error "-s option cannot be used with -S (rsync mode)"
        fi

        if [ "$g_option_O_origin_host" != "" ] ||
            [ "$g_option_T_target_host" != "" ]; then
            throw_usage_error "-O or -T option cannot be used with -S (rsync mode)"
        fi

        if [ $g_option_m_migrate -eq 1 ]; then
            throw_usage_error "-m option cannot be used with -S (rsync mode)"
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

        if [ $g_option_z_compress -eq 1 ] &&
            [ "$g_option_O_origin_host" = "" ] &&
            [ "$g_option_T_target_host" = "" ]; then
            throw_usage_error "-z option can only be used with -O or -T option"
        fi

        # disallow migration related options and remote transfers at same time
        if [ "$g_option_T_target_host" != "" ] || [ "$g_option_O_origin_host" != "" ]; then
            if [ $g_option_m_migrate -eq 1 ] || [ "$g_services" != "" ]; then
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
    l_suspect_fs=$initial_source
    l_suspect_fs_tail=""
    l_found_backup_file=0

    while [ $l_found_backup_file -eq 0 ]; do
        l_backup_file_dir=$($g_LZFS get -H -o value mountpoint "$l_suspect_fs")

        if $g_option_O_origin_host [ -r "$l_backup_file_dir/$g_backup_file_extension.$l_suspect_fs_tail" ]; then
            restored_backup_file_contents=$($g_option_O_origin_host "$g_cmd_cat" "$l_backup_file_dir/$g_backup_file_extension.$l_suspect_fs_tail")
            l_found_backup_file=1
        else
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
    _is_tail=$(echo "$initial_source" | sed -e 's/.*\///g')
    l_backup_file_dir=$($g_RZFS get -H -o value mountpoint "$g_destination")
    echov "Writing backup info to location $l_backup_file_dir/$g_backup_file_extension.$_is_tail"

    # Construct the backup file contents
    _backup_file_header="#zxfer property backup file;#version:$g_zxfer_version;#R options:$g_option_R_recursive;#N options:$g_option_N_nonrecursive;#destination:$g_destination;#initial_source:$_is_tail;#g_option_S_rsync_mode:$g_option_S_rsync_mode;"
    _backup_date=$(date)
    g_backup_file_contents="$_backup_file_header#backup_date:$_backup_date$g_backup_file_contents"

    # Construct the command to write the backup file
    backup_file_cmd="echo \"$g_backup_file_contents\" | tr \";\" \"\n\" > $l_backup_file_dir/$g_backup_file_extension.$_is_tail"

    # Execute the command
    if [ $g_option_n_dryrun -eq 0 ]; then
        echo "$backup_file_cmd" | "$g_cmd_ssh $g_option_T_target_host" sh ||
            throw_error "Error writing backup file. Is filesystem mounted?"
    else
        echo "echo \"$backup_file_cmd\" | \"$g_cmd_ssh $g_option_T_target_host\" sh"
    fi
}
