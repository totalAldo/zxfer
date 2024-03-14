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
# DEFINE GLOBALS used by zxfer
################################################################################

#
# Define global variables
#
init_globals() {
    # zxfer version
    g_zxfer_version="2.0.0-20240314"

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
    while getopts bBc:deE:f:Fg:hiI:klL:lmnN:o:O:pPPR:sST:u:UvV?:D:zZ: l_i; do
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
