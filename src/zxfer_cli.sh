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
# owns globals: g_option_* parse results and g_destination.
# reads globals: OPTARG and ZXFER_MAX_YIELD_ITERATIONS; -Z writes the
#   dependency-owned g_cmd_compress, and the -o check publishes the property
#   transfer module's g_zxfer_override_properties_result and prepared policy.
# mutates caches: none.
# returns via stdout: none; -h prints usage and exits.

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

# Purpose: Parse the command-line switches into the g_option_* state.
# Usage: zxfer_read_command_line_switches "$@"; the caller shifts by OPTIND.
# An unknown option is a usage error, and -h prints usage and exits 0.
# The launcher's -h prescan (zxfer_prescan_help_flag) parses with this option
# string too, so it handles -h before this runs; keep the two equal
# (tests/test_zxfer_launcher.sh checks).
zxfer_read_command_line_switches() {
	while getopts bBc:dD:eFg:hI:j:kmnN:o:O:PR:sT:UvVwx:YzZ: l_cli_option; do
		case $l_cli_option in
		b) g_option_b_beep_always=1 ;;
		B) g_option_B_beep_on_success=1 ;;
		c) g_option_c_services=$OPTARG ;;
		d) g_option_d_delete_destination_snapshots=1 ;;
		D) g_option_D_display_progress_bar=$OPTARG ;;
		e)
			g_option_e_restore_property_mode=1
			# Restore mode still flows through the property-transfer path.
			g_option_P_transfer_property=1
			;;
		F) g_option_F_force_rollback="-F" ;;
		g) g_option_g_grandfather_protection=$OPTARG ;;
		h)
			zxfer_usage
			exit 0
			;;
		I) g_option_I_ignore_properties=$OPTARG ;;
		j) g_option_j_jobs=$OPTARG ;;
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
		n) g_option_n_dryrun=1 ;;
		N) g_option_N_nonrecursive=$OPTARG ;;
		o) g_option_o_override_property=$OPTARG ;;
		O)
			g_option_O_origin_host=$OPTARG
			# Rebuild rendered zfs commands after the origin host spec changes.
			zxfer_refresh_remote_zfs_commands
			;;
		P) g_option_P_transfer_property=1 ;;
		R) g_option_R_recursive=$OPTARG ;;
		s) g_option_s_make_snapshot=1 ;;
		T)
			g_option_T_target_host=$OPTARG
			# Rebuild rendered zfs commands after the target host spec changes.
			zxfer_refresh_remote_zfs_commands
			;;
		U) g_option_U_skip_unsupported_properties=1 ;;
		v) g_option_v_verbose=1 ;;
		V)
			g_option_v_verbose=1
			g_option_V_very_verbose=1
			;;
		w) g_option_w_raw_send=1 ;;
		x) g_option_x_exclude_datasets=$OPTARG ;;
		Y) g_option_Y_yield_iterations=$ZXFER_MAX_YIELD_ITERATIONS ;;
		z) g_option_z_compress=1 ;;
		Z)
			g_option_z_compress=1
			g_cmd_compress=$OPTARG
			;;
		*) zxfer_throw_usage_error "Invalid option provided." 2 ;;
		esac
	done

	zxfer_refresh_compression_commands
	# The eager run temp root decided TMPDIR safety before -V was parsed;
	# replay a held unsafe-TMPDIR fallback advisory now that -V is known.
	zxfer_emit_pending_tmpdir_fallback_note
}

# Purpose: Reject malformed or incompatible CLI combinations before zxfer opens
# transports or touches datasets.
# Usage: zxfer_consistency_check, right after option parsing; every problem is
# a usage error.
zxfer_consistency_check() {
	# Validate -j early so arithmetic comparisons do not trip /bin/sh errors.
	zxfer_is_uint "${g_option_j_jobs:-}" ||
		zxfer_throw_usage_error "The -j option requires a positive integer job count, but received \"${g_option_j_jobs:-}\"."
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
		if ! zxfer_is_uint "$g_option_g_grandfather_protection"; then
			zxfer_throw_usage_error "grandfather protection requires a positive integer; received \"$g_option_g_grandfather_protection\"."
		elif [ "$g_option_g_grandfather_protection" -le 0 ]; then
			zxfer_throw_usage_error "grandfather protection requires days greater than 0; received \"$g_option_g_grandfather_protection\"."
		fi
	fi

	# disallow migration related options and remote transfers at same time
	if [ "$g_option_T_target_host" != "" ] || [ "$g_option_O_origin_host" != "" ]; then
		if [ "$g_option_m_migrate" -eq 1 ] || [ "$g_option_c_services" != "" ]; then
			zxfer_throw_usage_error "You cannot migrate to or from a remote host."
		fi
	fi

	# A malformed -o item or a property named twice stops the run before any
	# zfs command; the property transfer module owns the -o syntax.
	zxfer_read_override_properties "$g_option_o_override_property"
	g_zxfer_property_override_policy=$g_zxfer_override_properties_result
}
