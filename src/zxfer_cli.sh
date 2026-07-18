#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024-2026 Aldo Gonzalez
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
# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# CLI OPTION PARSING / VALIDATION
################################################################################

# Module contract:
# owns globals: g_option_* parse results and compression-command safety state.
# reads globals: OPTARG inputs, resolved helper paths, and existing runtime option context.
# mutates caches: none.
# returns via stdout: none; parser and validators update shared runtime globals directly.

# Purpose: Reset every parsed CLI option to its established startup default.
# Usage: Called by the session composition root before parsing a new invocation.
# Side effects: Reinitializes the complete g_option_* state owned by this module.
zxfer_init_cli_option_defaults() {
	g_destination=""
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_d_delete_destination_snapshots=0
	g_option_D_display_progress_bar=""
	g_option_e_restore_property_mode=0
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_I_ignore_properties=""
	# Default 1 avoids parallel source listing and background send jobs.
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_w_raw_send=0
	g_option_z_compress=0
}

# Purpose: Publish the validated positional destination through configuration
# ownership.
# Usage: Session parsing and later path normalization call this instead of
# assigning shared configuration directly.
zxfer_set_destination_argument() {
	g_destination=${1:-}
}

# Purpose: Apply one parsed option from the first half of zxfer's stable CLI.
# Usage: Called by zxfer_read_command_line_switches with the option name and
# its captured argument. Returns 1 only when the option belongs to the second
# handler, keeping each decision table bounded and independently reviewable.
zxfer_apply_cli_option_b_to_o() {
	l_cli_option=$1
	l_cli_option_argument=$2

	case $l_cli_option in
	b)
		g_option_b_beep_always=1
		;;
	B)
		g_option_B_beep_on_success=1
		;;
	c)
		g_option_c_services=$l_cli_option_argument
		;;
	d)
		g_option_d_delete_destination_snapshots=1
		;;
	D)
		g_option_D_display_progress_bar=$l_cli_option_argument
		;;
	e)
		g_option_e_restore_property_mode=1
		# Restore mode still flows through the property-transfer path.
		g_option_P_transfer_property=1
		;;
	F)
		g_option_F_force_rollback="-F"
		;;
	g)
		g_option_g_grandfather_protection=$l_cli_option_argument
		;;
	h)
		zxfer_usage
		exit 0
		;;
	I)
		g_option_I_ignore_properties=$l_cli_option_argument
		;;
	j)
		g_option_j_jobs=$l_cli_option_argument
		;;
	k)
		g_option_k_backup_property_mode=1
		# Backup mode still needs live source properties so they can be saved.
		g_option_P_transfer_property=1
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
		g_option_N_nonrecursive=$l_cli_option_argument
		;;
	o)
		g_option_o_override_property=$l_cli_option_argument
		;;
	O)
		l_new_origin_host=$l_cli_option_argument
		g_option_O_origin_host="$l_new_origin_host"
		# Rebuild rendered zfs commands after the origin host spec changes.
		zxfer_refresh_remote_zfs_commands
		;;
	*)
		return 1
		;;
	esac
	return 0
}

# Purpose: Apply one parsed option from the second half of zxfer's stable CLI.
# Usage: Called after zxfer_apply_cli_option_b_to_o declines the option.
zxfer_apply_cli_option_p_to_z() {
	l_cli_option=$1
	l_cli_option_argument=$2

	case $l_cli_option in
	P)
		g_option_P_transfer_property=1
		;;
	R)
		g_option_R_recursive=$l_cli_option_argument
		;;
	s)
		g_option_s_make_snapshot=1
		;;
	T)
		l_new_target_host=$l_cli_option_argument
		g_option_T_target_host="$l_new_target_host"
		# Rebuild rendered zfs commands after the target host spec changes.
		zxfer_refresh_remote_zfs_commands
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
	x)
		g_option_x_exclude_datasets=$l_cli_option_argument
		;;
	Y)
		g_option_Y_yield_iterations=$(zxfer_get_max_yield_iterations)
		;;
	z)
		g_option_z_compress=1
		;;
	Z)
		g_option_z_compress=1
		zxfer_set_dependency_command g_cmd_compress "$l_cli_option_argument"
		;;
	\?)
		zxfer_throw_usage_error "Invalid option provided." 2
		;;
	*)
		zxfer_throw_usage_error "Invalid option provided." 2
		;;
	esac
	return 0
}

# Purpose: Parse supported command-line switches into the shared `g_option_*`
# runtime state.
# Usage: Called during CLI parsing and startup validation before consistency
# checks and transport bootstrap depend on the parsed flags.
zxfer_read_command_line_switches() {
	while getopts bBc:dD:eFg:hI:j:kmnN:o:O:PR:sT:UvVwx:YzZ: l_i; do
		if zxfer_apply_cli_option_b_to_o "$l_i" "${OPTARG:-}"; then
			continue
		fi
		zxfer_apply_cli_option_p_to_z "$l_i" "${OPTARG:-}"
	done

	zxfer_refresh_compression_commands
	# The eager run temp root decided TMPDIR safety before -V was parsed;
	# replay a held unsafe-TMPDIR fallback advisory now that -V is known.
	zxfer_emit_pending_tmpdir_fallback_note
}

# Purpose: Reject malformed or incompatible CLI combinations before zxfer opens
# transports or touches datasets.
# Usage: Called during CLI parsing and startup validation immediately after
# option parsing so usage failures stop the run before any live side effects.
zxfer_consistency_check() {
	# Validate -j early so arithmetic comparisons do not trip /bin/sh errors.
	case ${g_option_j_jobs:-} in
	'' | *[!0-9]*)
		zxfer_throw_usage_error "The -j option requires a positive integer job count, but received \"${g_option_j_jobs:-}\"."
		;;
	esac
	if [ "$g_option_j_jobs" -le 0 ]; then
		zxfer_throw_usage_error "The -j option requires a job count of at least 1."
	fi

	# disallow backup and restore of properties at same time
	if [ "$g_option_k_backup_property_mode" -eq 1 ] &&
		[ "$g_option_e_restore_property_mode" -eq 1 ]; then
		zxfer_throw_usage_error "You cannot bac(k)up and r(e)store properties at the same time."
	fi

	# disallow both beep modes, enforce using one or the other.
	if [ "$g_option_b_beep_always" -eq 1 ] &&
		[ "$g_option_B_beep_on_success" -eq 1 ]; then
		zxfer_throw_usage_error "You cannot use both beep modes at the same time."
	fi

	if [ "$g_option_z_compress" -eq 1 ] &&
		[ "$g_option_O_origin_host" = "" ] &&
		[ "$g_option_T_target_host" = "" ]; then
		zxfer_throw_usage_error "-z option can only be used with -O or -T option"
	fi

	if [ "$g_option_g_grandfather_protection" != "" ]; then
		case $g_option_g_grandfather_protection in
		*[!0-9]*)
			zxfer_throw_usage_error "grandfather protection requires a positive integer; received \"$g_option_g_grandfather_protection\"."
			;;
		*)
			if [ "$g_option_g_grandfather_protection" -le 0 ]; then
				zxfer_throw_usage_error "grandfather protection requires days greater than 0; received \"$g_option_g_grandfather_protection\"."
			fi
			;;
		esac
	fi

	# disallow migration related options and remote transfers at same time
	if [ "$g_option_T_target_host" != "" ] || [ "$g_option_O_origin_host" != "" ]; then
		if [ "$g_option_m_migrate" -eq 1 ] || [ "$g_option_c_services" != "" ]; then
			zxfer_throw_usage_error "You cannot migrate to or from a remote host."
		fi
	fi
}
