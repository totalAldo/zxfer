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
#. ./zxfer_common.sh; . ./zxfer_get_zfs_list.sh; . ./zxfer_inspect_delete_snap.sh; . ./zxfer_rsync_mode.sh; . ./zxfer_transfer_properties.sh; . ./zxfer_zfs_mode.sh; . ./zxfer_zfs_send_receive.sh

################################################################################
# DEFINE GLOBALS used by zxfer
################################################################################

#
# Define global variables
#
init_globals() {
    # zxfer version
    g_zxfer_version="2.0.0-20240331"

    # Default values
    g_option_b_beep_always=0
    g_option_B_beep_on_success=0
    g_option_d_delete_destination_snapshots=0
    g_option_D_display_progress_bar=""
    g_option_e_restore_property_mode=0
    g_option_E_rsync_exclude_patterns=""
    g_option_f_rsync_file_options=""
    g_option_g_grandfather_protection=""
    g_option_i_rsync_include_zfs_mountpoints=0
    g_option_I_ignore_properties=""
    g_option_F_force_rollback=""
    g_option_k_backup_property_mode=0
    g_option_l_rsync_legacy_mountpoint=0
    g_option_L_rsync_levels_deep=""
    g_option_o_override_property=""
    g_option_O_origin_host=""
    g_option_p_rsync_persist=0
    g_option_P_transfer_property=0
    g_option_R_recursive=""
    g_option_m_migrate=0
    g_option_n_dryrun=0
    g_option_N_nonrecursive=""
    g_option_S_rsync_mode=0
    g_option_s_make_snapshot=0
    g_option_T_target_host=""
    g_option_u_rsync_use_existing_snapshot=0
    g_option_U_skip_unsupported_properties=0
    g_option_v_verbose=0
    g_option_V_very_verbose=0
    g_option_w_raw_send=0
    g_option_z_compress=0

    g_services=""
    g_services_to_restart=""

    source=""
    sourcefs=""
    g_destination=""
    g_backup_file_extension=".zxfer_backup_info"
    g_backup_file_contents=""

    # operating systems
    g_home_operating_system=""
    g_source_operating_system=""
    g_destination_operating_system=""

    # as in the "cp" man page
    g_trailing_slash=0

    # default compression commands
    g_cmd_compress="zstd -3"
    g_cmd_decompress="zstd -d"

    g_cmd_cat=""
    g_cmd_awk=$(which awk) # location of awk or gawk on home OS

    g_cmd_zfs=$(which zfs)
    g_cmd_ssh=$(which ssh)
    g_cmd_rsync=""

    # default zfs commands, can be overridden by -O or -T
    g_LZFS=$g_cmd_zfs
    g_RZFS=$g_cmd_zfs

    # dataset and snapshot lists
    g_recursive_source_list=""
    g_lzfs_list_hr_S_snap=""
    g_rzfs_list_hr_S_snap=""
    g_rzfs_list_ho_s=""

    g_found_last_common_snap=0
    g_last_common_snap=""
    g_actual_dest=""
    g_src_snapshot_transfer_list=""

    # specific to zfs mode
    g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

    # specific to rsync mode
    snapshot_name="zxfertempsnap"

    g_new_rmvs_pv=""
    g_new_rmv_pvs=""
    g_new_mc_pvs=""

    g_restored_backup_file_contents=""

    # used in rsync transfers, to turn off the backup file writing
    # the first time
    g_dont_write_backup=0

    g_ensure_writable=0 # when creating/setting properties, ensures readonly=off

    # default rsync options - see http://www.daemonforums.org/showthread.php?t=3948
    g_default_rsync_options="-clptgoD --inplace --relative -H --numeric-ids"

    # the readonly properties list 3 properties that are technically not
    # readonly but we will remove them from the override list as it does not make
    # sense to try and transfer them - version, volsize and mountpoint
    # Others have been added since. This is a potential refactor point
    # to split into two lists, $g_readonly_properties and $zxfer_unsupported_properties
    g_readonly_properties="type,creation,used,available,referenced,\
compressratio,mounted,version,primarycache,secondarycache,\
usedbysnapshots,usedbydataset,usedbychildren,usedbyrefreservation,\
version,volsize,mountpoint,mlslabel,keysource,keystatus,rekeydate,encryption,\
refcompressratio,written,logicalused,logicalreferenced,createtxg,guid,origin,\
filesystem_count,snapshot_count,clones,defer_destroy,receive_resume_token,\
userrefs,objsetid"

    # Properties not supported on FreeBSD
    g_fbsd_readonly_properties="aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,\
xattr,dnodesize"

    # Properties not supported on Solaris Express 11
    g_solexp_readonly_properties="jailed,aclmode,shareiscsi"
}

#
# Check command line parameters.
#
read_command_line_switches() {
    while getopts bBc:deE:f:Fg:hiI:klL:lmnN:o:O:pPPR:sST:u:UvVw?:D:zZ: l_i; do
        case $l_i in
        b)
            g_option_b_beep_always=1
            ;;
        B)
            g_option_B_beep_on_success=1
            ;;
        c)
            g_services="$OPTARG"
            ;;
        d)
            g_option_d_delete_destination_snapshots=1
            ;;
        D)
            g_option_D_display_progress_bar="$OPTARG"
            ;;
        e)
            g_option_e_restore_property_mode=1
            # Need to transfer properties, just the backed up properties
            # are substituted
            g_option_P_transfer_property=1
            ;;
        E)
            g_option_E_rsync_exclude_patterns="--exclude=$OPTARG $g_option_E_rsync_exclude_patterns"
            ;;
        f)
            g_option_f_rsync_file_options="$OPTARG"
            ;;
        F)
            g_option_F_force_rollback="-F"
            ;;
        g)
            g_option_g_grandfather_protection="$OPTARG"
            ;;
        h)
            throw_usage_error
            ;;
        i)
            g_option_i_rsync_include_zfs_mountpoints=1
            ;;
        I)
            g_option_I_ignore_properties="$OPTARG"
            ;;
        k)
            g_option_k_backup_property_mode=1
            # In order to back up the properties of the source, the
            # properties of the source must be transferred as well.
            g_option_P_transfer_property=1
            ;;
        l)
            g_option_l_rsync_legacy_mountpoint=1
            ;;
        L)
            g_option_L_rsync_levels_deep="$OPTARG"
            ;;
        m)
            g_option_m_migrate=1
            g_option_s_make_snapshot=1
            g_option_P_transfer_property=1
            ;;
        n)
            g_option_n_dryrun=1
            ;;
        N)
            g_option_N_nonrecursive="$OPTARG"
            ;;
        o)
            g_option_o_override_property="$OPTARG"
            ;;
        O)
            # since we are using the -O option, we are pulling a remote transfer
            # so we need to use the ssh command to execute the zfs commands
            # $OPTARG is the user@host
            g_LZFS="$g_cmd_ssh $OPTARG $g_cmd_zfs"
            g_option_O_origin_host="$OPTARG"
            ;;
        p)
            g_option_p_rsync_persist=1
            ;;
        P)
            g_option_P_transfer_property=1
            ;;
        R)
            g_option_R_recursive="$OPTARG"
            ;;
        s)
            g_option_s_make_snapshot=1
            ;;
        S)
            g_option_S_rsync_mode=1
            ;;
        T)
            # since we are using the -T option, we are pushing a remote transfer
            # so we need to use the ssh command to execute the zfs commands
            # $OPTARG is the user@host
            g_RZFS="$g_cmd_ssh $OPTARG $g_cmd_zfs"
            g_option_T_target_host="$OPTARG"
            ;;
        u)
            g_option_u_rsync_use_existing_snapshot=1
            snapshot_name="$OPTARG"
            ;;
        U)
            g_option_U_skip_unsupported_properties=1
            ;;
        v)
            g_option_v_verbose=1
            ;;
        V)
            g_option_v_verbose=1
            g_option_V_very_verbose=1
            ;;
        w)
            g_option_w_raw_send=1
            ;;
        z)
            # Pipes the send and receive commands through zstd
            g_option_z_compress=1
            ;;
        Z)
            # specify the zstd compression command, like "zstd -T0 -6"
            g_option_z_compress=1
            g_cmd_compress="$OPTARG"
            ;;
        \?)
            throw_usage_error "Invalid option provided." 2
            ;;
        esac
    done
}

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

    if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
        g_cmd_cat=$("$g_cmd_ssh" "$g_option_O_origin_host" which cat)
    fi

    if [ "$g_option_S_rsync_mode" -eq 1 ]; then
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
    if [ "$g_option_k_backup_property_mode" -eq 1 ] &&
        [ "$g_option_e_restore_property_mode" -eq 1 ]; then
        throw_usage_error "You cannot bac(k)up and r(e)store properties at the same time."
    fi

    # disallow both beep modes, enforce using one or the other.
    if [ "$g_option_b_beep_always" -eq 1 ] &&
        [ "$g_option_B_beep_on_success" -eq 1 ]; then
        throw_usage_error "You cannot use both beep modes at the same time."
    fi

    if [ "$g_option_S_rsync_mode" -eq 1 ]; then
        # rsync mode

        # check for incompatible options
        if [ "$g_option_F_force_rollback" = "-F" ]; then
            throw_usage_error "-F option cannot be used with -S (rsync mode)"
        fi

        if [ "$g_option_s_make_snapshot" -eq 1 ]; then
            throw_usage_error "-s option cannot be used with -S (rsync mode)"
        fi

        if [ "$g_option_O_origin_host" != "" ] ||
            [ "$g_option_T_target_host" != "" ]; then
            throw_usage_error "-O or -T option cannot be used with -S (rsync mode)"
        fi

        if [ "$g_option_m_migrate" -eq 1 ]; then
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

        if [ "$g_option_z_compress" -eq 1 ] &&
            [ "$g_option_O_origin_host" = "" ] &&
            [ "$g_option_T_target_host" = "" ]; then
            throw_usage_error "-z option can only be used with -O or -T option"
        fi

        # disallow migration related options and remote transfers at same time
        if [ "$g_option_T_target_host" != "" ] || [ "$g_option_O_origin_host" != "" ]; then
            if [ "$g_option_m_migrate" -eq 1 ] || [ "$g_services" != "" ]; then
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
            g_restored_backup_file_contents=$($g_option_O_origin_host "$g_cmd_cat" "$l_backup_file_dir/$g_backup_file_extension.$l_suspect_fs_tail")
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
    if [ "$g_option_n_dryrun" -eq 0 ]; then
        echo "$backup_file_cmd" | "$g_cmd_ssh $g_option_T_target_host" sh ||
            throw_error "Error writing backup file. Is filesystem mounted?"
    else
        echo "echo \"$backup_file_cmd\" | \"$g_cmd_ssh $g_option_T_target_host\" sh"
    fi
}
