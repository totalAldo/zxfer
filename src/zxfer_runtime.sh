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
# RUNTIME STATE / TEMP FILES / CLEANUP
################################################################################

# Module contract:
# owns globals: per-run option/default state, temp-root selection, cleanup PID state, transport/bootstrap defaults, and reporting/profile session state.
# reads globals: TMPDIR, ZXFER_BACKUP_DIR, g_option_* cleanup flags, and resolved helper paths.
# mutates caches: reporting, destination-existence, property, and snapshot-index state through reset helpers.
# returns via stdout: temp-file/temp-dir paths and OS detection results.

ZXFER_MAX_YIELD_ITERATIONS=8

zxfer_refresh_backup_storage_root() {
	if [ -n "${ZXFER_BACKUP_DIR:-}" ]; then
		g_backup_storage_root=$ZXFER_BACKUP_DIR
	elif [ -z "${g_backup_storage_root:-}" ]; then
		g_backup_storage_root=/var/db/zxfer
	fi
}
zxfer_register_cleanup_pid() {
	l_pid=$1

	case "$l_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	[ "$l_pid" = "$$" ] && return 0

	for l_existing_pid in ${g_zxfer_cleanup_pids:-}; do
		[ "$l_existing_pid" = "$l_pid" ] && return 0
	done

	if [ -n "${g_zxfer_cleanup_pids:-}" ]; then
		g_zxfer_cleanup_pids="$g_zxfer_cleanup_pids $l_pid"
	else
		g_zxfer_cleanup_pids=$l_pid
	fi
}

zxfer_unregister_cleanup_pid() {
	l_pid=$1
	l_remaining_pids=""

	case "$l_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	for l_existing_pid in ${g_zxfer_cleanup_pids:-}; do
		[ "$l_existing_pid" = "$l_pid" ] && continue
		if [ -n "$l_remaining_pids" ]; then
			l_remaining_pids="$l_remaining_pids $l_existing_pid"
		else
			l_remaining_pids=$l_existing_pid
		fi
	done

	g_zxfer_cleanup_pids=$l_remaining_pids
}

zxfer_kill_registered_cleanup_pids() {
	for l_pid in ${g_zxfer_cleanup_pids:-}; do
		case "$l_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		[ "$l_pid" = "$$" ] && continue
		kill "$l_pid" 2>/dev/null || true
	done

	g_zxfer_cleanup_pids=""
}

zxfer_list_default_tmpdir_candidates() {
	printf '%s\n' "/dev/shm"
	printf '%s\n' "/run/shm"
	printf '%s\n' "/tmp"
}

zxfer_try_get_default_tmpdir() {
	l_candidates=$(zxfer_list_default_tmpdir_candidates)

	while IFS= read -r l_candidate || [ -n "$l_candidate" ]; do
		[ -n "$l_candidate" ] || continue
		if l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_candidate"); then
			printf '%s\n' "$l_effective_tmpdir"
			return 0
		fi
	done <<EOF
$l_candidates
EOF

	return 1
}

zxfer_try_get_socket_cache_tmpdir() {
	l_requested_tmpdir=${TMPDIR:-}

	if [ -n "$l_requested_tmpdir" ] &&
		l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir"); then
		case "$l_requested_tmpdir" in
		*/./* | */../* | */. | */..)
			:
			;;
		*)
			if ! zxfer_find_symlink_path_component "$l_requested_tmpdir" >/dev/null 2>&1; then
				printf '%s\n' "$l_requested_tmpdir"
				return 0
			fi
			;;
		esac
	fi

	zxfer_try_get_effective_tmpdir
}

zxfer_try_get_effective_tmpdir() {
	if [ -n "${TMPDIR:-}" ]; then
		l_requested_tmpdir=$TMPDIR
		l_request_key=$l_requested_tmpdir
	else
		l_requested_tmpdir=""
		l_request_key="__ZXFER_DEFAULT_TMPDIR__"
	fi

	if [ -n "${g_zxfer_effective_tmpdir:-}" ] &&
		[ "${g_zxfer_effective_tmpdir_requested:-}" = "$l_request_key" ]; then
		printf '%s\n' "$g_zxfer_effective_tmpdir"
		return 0
	fi

	if [ -n "$l_requested_tmpdir" ]; then
		if l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir"); then
			:
		elif l_effective_tmpdir=$(zxfer_try_get_default_tmpdir); then
			zxfer_echoV "Ignoring unsafe TMPDIR $l_requested_tmpdir; using $l_effective_tmpdir instead."
		else
			g_zxfer_effective_tmpdir_requested=$l_request_key
			g_zxfer_effective_tmpdir=""
			return 1
		fi
	elif ! l_effective_tmpdir=$(zxfer_try_get_default_tmpdir); then
		g_zxfer_effective_tmpdir_requested=$l_request_key
		g_zxfer_effective_tmpdir=""
		return 1
	fi

	g_zxfer_effective_tmpdir_requested=$l_request_key
	g_zxfer_effective_tmpdir=$l_effective_tmpdir
	printf '%s\n' "$g_zxfer_effective_tmpdir"
}

zxfer_create_private_temp_dir() {
	l_prefix=$1

	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		return 1
	fi

	mktemp -d "$l_tmpdir/$l_prefix.XXXXXX"
}

zxfer_get_temp_file() {
	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		zxfer_throw_error "Error creating temporary file."
	fi
	# On GNU mktemp the template must include X, so build the template ourselves.
	l_prefix=${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}
	l_file=$(mktemp "$l_tmpdir/$l_prefix.XXXXXX") ||
		zxfer_throw_error "Error creating temporary file."
	zxfer_echoV "New temporary file: $l_file"

	# return the temp file name
	echo "$l_file"
}

#
# Gets a $(uname), i.e. the operating system, for origin or target, if remote.
# Takes: $1=either $g_option_O_origin_host or $g_option_T_target_host
#
zxfer_get_os() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_output_os=""

	# Get uname of the destination (target) machine, local or remote
	if [ "$l_host_spec" = "" ]; then
		l_output_os=$(uname)
	else
		if ! l_output_os=$(zxfer_get_remote_host_operating_system "$l_host_spec" "$l_profile_side"); then
			return 1
		fi
	fi

	echo "$l_output_os"
}

zxfer_get_max_yield_iterations() {
	printf '%s\n' "$ZXFER_MAX_YIELD_ITERATIONS"
}

zxfer_init_runtime_metadata() {
	# zxfer version
	g_zxfer_version="2.0.0-20260409"
}

zxfer_init_option_defaults() {
	# Default values
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_d_delete_destination_snapshots=0
	g_option_D_display_progress_bar=""
	g_option_e_restore_property_mode=0
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_I_ignore_properties=""
	# number of parallel job processes to run when listing zfs snapshots
	# in the source (default 1 does not use parallel).
	# This also sets the maximum number of background zfs send processes
	# that can run at the same time.
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_O_origin_host_safe=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_T_target_host_safe=""
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_w_raw_send=0
	g_option_z_compress=0
}

zxfer_init_dependency_tool_defaults() {
	g_cmd_zfs=""

	# default compression commands
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	g_origin_cmd_compress_safe=""
	g_origin_cmd_decompress_safe=""
	g_target_cmd_compress_safe=""
	g_target_cmd_decompress_safe=""
	g_cmd_cat=""

	zxfer_assign_required_tool g_cmd_awk awk "awk"
	zxfer_assign_required_tool g_cmd_zfs zfs "zfs"
	g_cmd_parallel=$(PATH=$g_zxfer_dependency_path command -v parallel 2>/dev/null || :)
	if [ "$g_cmd_parallel" != "" ]; then
		if ! g_cmd_parallel=$(zxfer_validate_resolved_tool_path "$g_cmd_parallel" "GNU parallel"); then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_cmd_parallel"
		fi
	fi

	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	zxfer_assign_required_tool g_cmd_ssh ssh "ssh"
	zxfer_refresh_compression_commands
}

zxfer_init_transport_remote_defaults() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_zxfer_remote_capability_cache_ttl=15
	g_zxfer_remote_capability_cache_wait_retries=5
	g_source_operating_system=""
	g_destination_operating_system=""
	g_origin_parallel_cmd=""

	# ssh control sockets used for origin (-O) and target (-T) hosts
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	g_ssh_supports_control_sockets=0
	if zxfer_ssh_supports_control_sockets; then
		g_ssh_supports_control_sockets=1
	fi

	# default zfs commands, can be overridden by -O or -T
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs
}

zxfer_init_runtime_state_defaults() {
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

	# profiling and session-scoped scratch state
	g_zxfer_cleanup_pids=""
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_profile_start_epoch=$(date '+%s' 2>/dev/null || :)
	g_zxfer_profile_has_data=0
	g_zxfer_profile_summary_emitted=0
	g_zxfer_profile_ssh_setup_ms=0
	g_zxfer_profile_source_snapshot_listing_ms=0
	g_zxfer_profile_destination_snapshot_listing_ms=0
	g_zxfer_profile_snapshot_diff_sort_ms=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0
	g_zxfer_profile_source_snapshot_list_commands=0
	g_zxfer_profile_source_snapshot_list_parallel_commands=0
	g_zxfer_profile_send_receive_pipeline_commands=0
	g_zxfer_profile_send_receive_background_pipeline_commands=0
	g_zxfer_profile_exists_destination_calls=0
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_profile_parent_destination_property_reads=0
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_destination=""
	zxfer_refresh_backup_storage_root

	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off
	g_backup_file_extension=".zxfer_backup_info"
}

zxfer_init_temp_artifacts() {
	g_zxfer_temp_prefix="zxfer.$$.${g_option_Y_yield_iterations}.$(date +%s)"

	# temporary files used by zxfer_get_dest_snapshots_to_delete_per_dataset()
	g_delete_source_tmp_file=$(zxfer_get_temp_file)
	g_delete_dest_tmp_file=$(zxfer_get_temp_file)
	g_delete_snapshots_to_delete_tmp_file=$(zxfer_get_temp_file)
}

zxfer_init_globals() {
	zxfer_reset_failure_context "startup"

	zxfer_init_runtime_metadata
	zxfer_init_option_defaults
	zxfer_init_runtime_state_defaults
	if command -v zxfer_reset_replication_runtime_state >/dev/null 2>&1; then
		zxfer_reset_replication_runtime_state
	fi
	if command -v zxfer_reset_send_receive_state >/dev/null 2>&1; then
		zxfer_reset_send_receive_state
	fi
	if command -v zxfer_reset_destination_existence_cache >/dev/null 2>&1; then
		zxfer_reset_destination_existence_cache
	fi
	if command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1; then
		zxfer_reset_snapshot_record_indexes
	fi
	if command -v zxfer_reset_snapshot_discovery_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_discovery_state
	fi
	if command -v zxfer_reset_snapshot_reconcile_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_reconcile_state
	fi
	if command -v zxfer_reset_backup_metadata_state >/dev/null 2>&1; then
		zxfer_reset_backup_metadata_state
	fi
	if command -v zxfer_reset_property_runtime_state >/dev/null 2>&1; then
		zxfer_reset_property_runtime_state
	fi
	# Property scratch state lives with the property modules; reset it through
	# their public helpers so startup and iteration resets cannot drift apart.
	if command -v zxfer_reset_property_iteration_caches >/dev/null 2>&1; then
		zxfer_reset_property_iteration_caches
	fi
	if command -v zxfer_reset_property_reconcile_state >/dev/null 2>&1; then
		zxfer_reset_property_reconcile_state
	fi
	zxfer_init_dependency_tool_defaults
	zxfer_init_transport_remote_defaults
	zxfer_init_temp_artifacts
}

zxfer_trap_exit() {
	# get the exit status of the last command
	l_exit_status=$?

	# Only terminate zxfer-owned background processes. Killing every direct child
	# of the shell is too broad and can clobber coverage helpers or command
	# substitution plumbing in the caller.
	zxfer_kill_registered_cleanup_pids

	if command -v zxfer_close_all_ssh_control_sockets >/dev/null 2>&1; then
		zxfer_close_all_ssh_control_sockets
	fi

	# Remove temporary files if they exist
	for l_temp_file in "$g_delete_source_tmp_file" \
		"$g_delete_dest_tmp_file" \
		"$g_delete_snapshots_to_delete_tmp_file"; do
		if [ -f "$l_temp_file" ]; then
			rm "$l_temp_file"
		fi
	done
	if l_tmpdir=$(zxfer_try_get_effective_tmpdir 2>/dev/null); then
		for l_temp_file in "$l_tmpdir/${g_zxfer_temp_prefix:-zxfer.unset}".*; do
			[ -e "$l_temp_file" ] || continue
			rm -rf "$l_temp_file"
		done
	fi
	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		rm -rf "$g_zxfer_property_cache_dir"
	fi
	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		rm -rf "$g_zxfer_snapshot_index_dir"
	fi

	if [ "${g_services_need_relaunch:-0}" -eq 1 ]; then
		if [ "${g_services_relaunch_in_progress:-0}" -eq 1 ]; then
			zxfer_echoV "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
		elif command -v zxfer_relaunch >/dev/null 2>&1; then
			zxfer_echoV "zxfer exiting early; restarting stopped services."
			zxfer_relaunch
		else
			zxfer_echoV "zxfer exiting with services still stopped; zxfer_relaunch() unavailable."
		fi
	fi

	zxfer_echoV "zxfer exiting with status $l_exit_status"
	zxfer_profile_emit_summary
	zxfer_emit_failure_report "$l_exit_status"

	# exit this script
	exit $l_exit_status
}

zxfer_register_runtime_traps() {
	# catch any signals to terminate the script
	# INT (Interrupt) 2 (Ctrl-C)
	# TERM (Terminate) 15 (kill)
	# HUP (Hangup) 1 (kill -HUP)
	# QUIT (Quit) 3 (Ctrl-\)
	# EXIT (Exit) 0 (exit)
	trap zxfer_trap_exit INT TERM HUP QUIT EXIT
}

zxfer_init_transfer_command_context() {
	g_origin_cmd_compress_safe=$g_cmd_compress_safe
	g_origin_cmd_decompress_safe=$g_cmd_decompress_safe
	g_target_cmd_compress_safe=$g_cmd_compress_safe
	g_target_cmd_decompress_safe=$g_cmd_decompress_safe
}

zxfer_init_source_execution_context() {
	if [ "$g_option_O_origin_host" != "" ]; then
		if ! g_source_operating_system=$(zxfer_get_os "$g_option_O_origin_host" source); then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_O_origin_host."
		fi
		if ! g_origin_cmd_zfs=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" zfs "zfs" source); then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_origin_cmd_zfs"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			if ! g_origin_cmd_compress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_O_origin_host" "$g_cmd_compress" "compression command" source); then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_origin_cmd_compress_safe"
			fi
		fi
		return
	fi

	g_source_operating_system=$(zxfer_get_os "")
	g_origin_cmd_zfs=$g_cmd_zfs
}

zxfer_init_destination_execution_context() {
	if [ "$g_option_T_target_host" != "" ]; then
		if ! g_destination_operating_system=$(zxfer_get_os "$g_option_T_target_host" destination); then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_T_target_host."
		fi
		if ! g_target_cmd_zfs=$(zxfer_resolve_remote_required_tool "$g_option_T_target_host" zfs "zfs" destination); then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_target_cmd_zfs"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			if ! g_target_cmd_decompress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "$g_cmd_decompress" "decompression command" destination); then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_target_cmd_decompress_safe"
			fi
		fi
		return
	fi

	g_destination_operating_system=$(zxfer_get_os "")
	g_target_cmd_zfs=$g_cmd_zfs
}

zxfer_init_restore_property_helpers() {
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return

	if [ "$g_option_O_origin_host" = "" ]; then
		zxfer_assign_required_tool g_cmd_cat cat "cat"
		return
	fi

	if ! g_cmd_cat=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" cat "cat" source); then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "$g_cmd_cat"
	fi
}

zxfer_init_local_awk_compatibility() {
	l_home_operating_system=$(zxfer_get_os "")
	if [ "$l_home_operating_system" != "SunOS" ]; then
		return
	fi

	l_gawk_path=$(PATH=$g_zxfer_dependency_path command -v gawk 2>/dev/null || :)
	if [ "$l_gawk_path" != "" ]; then
		g_cmd_awk=$l_gawk_path
	fi
}

zxfer_init_variables() {
	zxfer_init_transfer_command_context
	zxfer_init_source_execution_context
	zxfer_init_destination_execution_context
	zxfer_refresh_remote_zfs_commands
	zxfer_init_restore_property_helpers
	zxfer_init_local_awk_compatibility
}
