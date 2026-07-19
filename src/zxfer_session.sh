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
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# BSD HEADER END
# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# SESSION COMPOSITION
################################################################################

# Module contract:
# owns globals: top-level version and lifecycle coordination state.
# reads globals: parsed option state and replication context after initialization.
# mutates caches: invokes each owner module's reset, startup, and cleanup APIs.
# returns via stdout: none.
#
# This module remains last in the canonical manifest. Its exit path coordinates
# the migration-service owner so stopped illumos/Solaris services are restarted
# during shutdown.

# Purpose: Drop every inherited handle that could make trap cleanup signal a
# process, contact a host, remove a path, or restart a service.
# Usage: Called before traps become active. Owner discard/reset APIs used here
# perform assignments only and never act on the referenced external resource.
zxfer_discard_inherited_cleanup_state() {
	# CLI defaults make early trap decisions inert, while the reporting reset
	# preserves the launcher-captured original invocation for diagnostics.
	zxfer_init_cli_option_defaults
	zxfer_reset_failure_context "startup"
	zxfer_discard_runtime_cleanup_state
	zxfer_reset_owned_lock_tracking
	zxfer_discard_background_job_cleanup_state
	zxfer_discard_ssh_cleanup_state
	zxfer_reset_migration_service_state
}

# Purpose: Initialize every owner module in the established session order.
# Usage: Called once during session bootstrap after the complete module graph
# is loaded, so owner resets can be direct calls instead of optional probes.
zxfer_init_globals() {
	zxfer_reset_failure_context "startup"
	zxfer_refresh_secure_path_state ||
		zxfer_reject_invalid_secure_path_configuration

	g_zxfer_version="2.0.0-20260623"
	zxfer_init_cli_option_defaults
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)
	zxfer_reset_quoting_state
	zxfer_init_runtime_state_defaults
	zxfer_reset_owned_lock_tracking
	zxfer_reset_profile_state
	zxfer_init_backup_storage_root
	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off
	g_backup_file_extension=".zxfer_backup_info"
	zxfer_reset_replication_runtime_state
	zxfer_reset_migration_service_state
	zxfer_reset_send_job_state
	zxfer_reset_send_receive_state
	zxfer_reset_background_job_state
	zxfer_reset_operation_state
	zxfer_reset_destination_existence_cache
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_snapshot_producer_session_state
	zxfer_reset_snapshot_discovery_state
	zxfer_reset_snapshot_reconcile_state
	zxfer_reset_snapshot_delete_artifact_state
	zxfer_reset_backup_metadata_state
	zxfer_reset_property_runtime_state
	# Property scratch state lives with the property modules; reset it through
	# their public helpers so startup and iteration resets cannot drift apart.
	zxfer_reset_property_iteration_caches
	zxfer_reset_property_reconcile_state
	zxfer_init_dependency_tool_defaults
	zxfer_reset_ssh_transport_state
	zxfer_reset_remote_host_state
	zxfer_init_temp_artifacts
	# Create the per-run temp root before narrowing PATH so bootstrap helpers
	# still have access to base utilities such as mktemp even when an explicit
	# ZXFER_SECURE_PATH intentionally omits their directories. Failure stays
	# non-fatal; the first allocation that needs the root reports it.
	zxfer_ensure_run_tmp_root || :
	zxfer_apply_secure_path
}

# Purpose: Run the centralized shutdown path that cleans up runtime artifacts,
# transports, and end-of-run reporting state.
# Usage: Installed as the shared signal/EXIT handler during session bootstrap.
# Side effects: Preserves shutdown ordering, promotes cleanup failures, restarts
# stopped migration services, emits profile/failure output, and exits.
zxfer_trap_exit() {
	# get the exit status of the last command
	l_trap_exit_status=$?
	l_cleanup_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_cleanup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	# Only terminate zxfer-owned background processes. Killing every direct child
	# of the shell is too broad and can clobber coverage helpers or command
	# substitution plumbing in the caller.
	l_background_cleanup_status=0
	zxfer_abort_all_background_jobs || l_background_cleanup_status=$?
	if [ "$l_background_cleanup_status" -ne 0 ]; then
		[ "$l_trap_exit_status" -eq 0 ] &&
			l_trap_exit_status=$l_background_cleanup_status
		zxfer_set_failure_context_if_empty runtime "trap cleanup" \
			"${g_zxfer_background_job_abort_failure_message:-Failed to tear down one or more supervised background jobs during exit.}"
	fi
	l_cleanup_pid_status=0
	zxfer_kill_registered_cleanup_pids || l_cleanup_pid_status=$?
	if [ "$l_cleanup_pid_status" -ne 0 ]; then
		[ "$l_trap_exit_status" -eq 0 ] &&
			l_trap_exit_status=$l_cleanup_pid_status
		zxfer_set_failure_context_if_empty runtime "trap cleanup" \
			"${g_zxfer_cleanup_pid_abort_failure_message:-Failed to tear down one or more validated cleanup helpers during exit.}"
	fi

	if zxfer_close_all_ssh_control_sockets; then
		:
	else
		l_trap_close_status=$?
		if [ "$l_trap_exit_status" -eq 0 ]; then
			l_trap_exit_status=$l_trap_close_status
			zxfer_set_failure_context_if_empty runtime "trap cleanup" \
				"Failed to close one or more ssh control sockets during exit."
		fi
	fi
	# Every per-run transient lives under the one private temp root; one
	# rm -rf replaces per-artifact bookkeeping. Registered path-adjacent
	# staging debris is reaped first because it lives outside the root.
	l_artifact_cleanup_failed=0
	zxfer_cleanup_registered_runtime_artifacts || l_artifact_cleanup_failed=1
	zxfer_remove_run_tmp_root || l_artifact_cleanup_failed=1
	if [ "$l_artifact_cleanup_failed" -ne 0 ]; then
		[ "$l_trap_exit_status" -eq 0 ] && l_trap_exit_status=1
		zxfer_set_failure_context_if_empty runtime "trap cleanup" \
			"Failed to remove one or more runtime temp artifacts during exit."
	fi
	if [ "${g_services_need_relaunch:-0}" -eq 1 ]; then
		if [ "${g_services_relaunch_in_progress:-0}" -eq 1 ]; then
			zxfer_echoV "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
		else
			zxfer_echoV "zxfer exiting early; restarting stopped services."
			l_migration_service_cleanup_status=0
			zxfer_restore_migration_services_status_only ||
				l_migration_service_cleanup_status=$?
			if [ "$l_migration_service_cleanup_status" -ne 0 ]; then
				l_migration_service_cleanup_message=${g_zxfer_migration_service_restore_failure_message:-Failed to restore stopped migration services during exit.}
				[ "$l_trap_exit_status" -eq 0 ] &&
					l_trap_exit_status=$l_migration_service_cleanup_status
				if [ -n "${g_zxfer_failure_message:-}" ]; then
					# The primary structured failure keeps ownership, but a failed
					# service restart must never disappear: the operator may need to
					# restore the still-stopped SMF service manually.
					zxfer_warn_stderr "$l_migration_service_cleanup_message"
				else
					zxfer_set_failure_context_if_empty runtime "trap cleanup" \
						"$l_migration_service_cleanup_message"
				fi
			fi
		fi
	fi

	zxfer_profile_add_elapsed_ms g_zxfer_profile_cleanup_ms "$l_cleanup_start_ms"
	zxfer_echoV "zxfer exiting with status $l_trap_exit_status"
	zxfer_profile_emit_summary
	zxfer_emit_failure_report "$l_trap_exit_status"

	# Failure reporting may lazily recreate the run temp root or stage log
	# files (ZXFER_ERROR_LOG mirroring); sweep again so nothing survives exit.
	zxfer_cleanup_registered_runtime_artifacts >/dev/null 2>&1 || :
	zxfer_remove_run_tmp_root >/dev/null 2>&1 || :

	# exit this script
	exit $l_trap_exit_status
}

# Purpose: Register the legacy-named runtime traps with session-owned cleanup.
# Usage: Called once during session initialization before mutable startup work.
zxfer_register_runtime_traps() {
	# catch any signals to terminate the script
	# INT (Interrupt) 2 (Ctrl-C)
	# TERM (Terminate) 15 (kill)
	# HUP (Hangup) 1 (kill -HUP)
	# QUIT (Quit) 3 (Ctrl-\)
	# EXIT (Exit) 0 (exit)
	trap zxfer_trap_exit INT TERM HUP QUIT EXIT
}

# Purpose: Initialize the source execution context after CLI validation.
# Usage: Called by zxfer_init_variables once remote capability state is ready.
zxfer_init_source_execution_context() {
	if [ "$g_option_O_origin_host" != "" ]; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			l_source_zfs_command=${g_origin_cmd_zfs:-$g_cmd_zfs}
			zxfer_publish_endpoint_runtime_context origin "" "$l_source_zfs_command"
			if [ "$g_option_z_compress" -eq 1 ] &&
				[ -z "${g_origin_cmd_compress_safe:-}" ]; then
				l_source_compress_status=0
				l_source_compress_safe=$(zxfer_quote_cli_tokens "$g_cmd_compress" "compression command") ||
					l_source_compress_status=$?
				if [ "$l_source_compress_status" -ne 0 ]; then
					zxfer_throw_error "$l_source_compress_safe" "$l_source_compress_status"
				fi
				zxfer_set_endpoint_compression_command origin compress "$l_source_compress_safe"
			fi
			zxfer_echoV "Dry run: skipping live remote source helper validation."
			return
		fi
		l_source_context_status=0
		l_source_operating_system=$(zxfer_get_os "$g_option_O_origin_host" source) ||
			l_source_context_status=$?
		if [ "$l_source_context_status" -ne 0 ]; then
			zxfer_set_failure_class dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_O_origin_host." "$l_source_context_status"
		fi
		l_source_context_status=0
		l_source_zfs_command=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" zfs "zfs" source) ||
			l_source_context_status=$?
		if [ "$l_source_context_status" -ne 0 ]; then
			zxfer_set_failure_class dependency
			zxfer_throw_error "$l_source_zfs_command" "$l_source_context_status"
		fi
		zxfer_publish_endpoint_runtime_context origin "$l_source_operating_system" "$l_source_zfs_command"
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_source_context_status=0
			l_source_compress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_O_origin_host" "$g_cmd_compress" "compression command" source) ||
				l_source_context_status=$?
			if [ "$l_source_context_status" -ne 0 ]; then
				zxfer_set_failure_class dependency
				zxfer_throw_error "$l_source_compress_safe" "$l_source_context_status"
			fi
			zxfer_set_endpoint_compression_command origin compress "$l_source_compress_safe"
		fi
		return
	fi

	l_source_context_status=0
	l_source_operating_system=$(zxfer_get_os "") || l_source_context_status=$?
	if [ "$l_source_context_status" -ne 0 ]; then
		zxfer_set_failure_class dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_source_context_status"
	fi
	zxfer_publish_endpoint_runtime_context origin "$l_source_operating_system" "$g_cmd_zfs"
}

# Purpose: Initialize the destination execution context after CLI validation.
# Usage: Called by zxfer_init_variables once remote capability state is ready.
zxfer_init_destination_execution_context() {
	if [ "$g_option_T_target_host" != "" ]; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			l_destination_zfs_command=${g_target_cmd_zfs:-$g_cmd_zfs}
			zxfer_publish_endpoint_runtime_context target "" "$l_destination_zfs_command"
			if [ "$g_option_z_compress" -eq 1 ] &&
				[ -z "${g_target_cmd_decompress_safe:-}" ]; then
				l_destination_decompress_status=0
				l_destination_decompress_safe=$(zxfer_quote_cli_tokens "$g_cmd_decompress" "decompression command") ||
					l_destination_decompress_status=$?
				if [ "$l_destination_decompress_status" -ne 0 ]; then
					zxfer_throw_error "$l_destination_decompress_safe" "$l_destination_decompress_status"
				fi
				zxfer_set_endpoint_compression_command target decompress "$l_destination_decompress_safe"
			fi
			zxfer_echoV "Dry run: skipping live remote destination helper validation."
			return
		fi
		l_destination_context_status=0
		l_destination_operating_system=$(zxfer_get_os "$g_option_T_target_host" destination) ||
			l_destination_context_status=$?
		if [ "$l_destination_context_status" -ne 0 ]; then
			zxfer_set_failure_class dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_T_target_host." "$l_destination_context_status"
		fi
		l_destination_context_status=0
		l_destination_zfs_command=$(zxfer_resolve_remote_required_tool "$g_option_T_target_host" zfs "zfs" destination) ||
			l_destination_context_status=$?
		if [ "$l_destination_context_status" -ne 0 ]; then
			zxfer_set_failure_class dependency
			zxfer_throw_error "$l_destination_zfs_command" "$l_destination_context_status"
		fi
		zxfer_publish_endpoint_runtime_context target "$l_destination_operating_system" "$l_destination_zfs_command"
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_destination_context_status=0
			l_destination_decompress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "$g_cmd_decompress" "decompression command" destination) ||
				l_destination_context_status=$?
			if [ "$l_destination_context_status" -ne 0 ]; then
				zxfer_set_failure_class dependency
				zxfer_throw_error "$l_destination_decompress_safe" "$l_destination_context_status"
			fi
			zxfer_set_endpoint_compression_command target decompress "$l_destination_decompress_safe"
		fi
		return
	fi

	l_destination_context_status=0
	l_destination_operating_system=$(zxfer_get_os "") || l_destination_context_status=$?
	if [ "$l_destination_context_status" -ne 0 ]; then
		zxfer_set_failure_class dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_destination_context_status"
	fi
	zxfer_publish_endpoint_runtime_context target "$l_destination_operating_system" "$g_cmd_zfs"
}

# Purpose: Resolve the local or remote restore helper selected by CLI state.
# Usage: Called by zxfer_init_variables after source context initialization.
zxfer_init_restore_property_helpers() {
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return

	if [ "$g_option_O_origin_host" = "" ]; then
		zxfer_assign_required_tool g_cmd_cat cat "cat"
		return
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		[ -n "${g_cmd_cat:-}" ] || zxfer_set_dependency_command g_cmd_cat cat
		zxfer_echoV "Dry run: skipping live remote backup-restore helper validation."
		return
	fi

	l_restore_property_status=0
	l_restore_cat_command=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" cat "cat" source) ||
		l_restore_property_status=$?
	if [ "$l_restore_property_status" -ne 0 ]; then
		zxfer_set_failure_class dependency
		zxfer_throw_error "$l_restore_cat_command" "$l_restore_property_status"
	fi
	zxfer_set_dependency_command g_cmd_cat "$l_restore_cat_command"
}

# Purpose: Prefer gawk for the established SunOS compatibility path.
# Usage: Called by zxfer_init_variables after local OS detection is available.
zxfer_init_local_awk_compatibility() {
	l_awk_compatibility_status=0
	l_home_operating_system=$(zxfer_get_os "") || l_awk_compatibility_status=$?
	if [ "$l_awk_compatibility_status" -ne 0 ]; then
		zxfer_set_failure_class dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_awk_compatibility_status"
	fi
	if [ "$l_home_operating_system" != "SunOS" ]; then
		return
	fi

	l_gawk_path=$(PATH=$g_zxfer_dependency_path command -v gawk 2>/dev/null || :)
	if [ "$l_gawk_path" != "" ]; then
		zxfer_set_dependency_command g_cmd_awk "$l_gawk_path"
	fi
}

# Purpose: Prepare transport and capability state for configured remote roles.
# Usage: Called after CLI validation and before execution-context helper
# resolution. Control sockets remain deferred until replication work exists.
# Side effects: Resolves the local SSH command, preloads remote capabilities,
# refreshes active ZFS routing, and warms per-role transport memos.
zxfer_prepare_remote_host_connections() {
	l_ssh_setup_start_ms=""

	if { [ "$g_option_O_origin_host" != "" ] || [ "$g_option_T_target_host" != "" ]; } &&
		zxfer_profile_metrics_enabled; then
		l_ssh_setup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		if [ "$g_option_O_origin_host" != "" ]; then
			zxfer_echoV "Dry run: skipping ssh control-socket setup and remote capability preload for origin host."
		fi
		if [ "$g_option_T_target_host" != "" ]; then
			zxfer_echoV "Dry run: skipping ssh control-socket setup and remote capability preload for target host."
		fi
		zxfer_refresh_remote_zfs_commands
		zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
		return
	fi

	if [ "$g_option_O_origin_host" != "" ] || [ "$g_option_T_target_host" != "" ]; then
		if [ -z "${g_cmd_ssh:-}" ]; then
			if ! zxfer_ensure_local_ssh_command; then
				zxfer_set_failure_class dependency
				zxfer_throw_error "$g_zxfer_resolved_local_ssh_command_result"
			fi
		fi
		zxfer_refresh_ssh_control_socket_support_state
	fi

	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_preload_remote_host_capabilities "$g_option_O_origin_host" source || :
	fi

	if [ "$g_option_T_target_host" != "" ]; then
		zxfer_preload_remote_host_capabilities "$g_option_T_target_host" destination || :
	fi

	zxfer_refresh_remote_zfs_commands
	# Warm the per-role transport-token memo in the main shell once the host
	# specs and managed ssh options are validated (OPTIMIZATION 11): later
	# remote commands replay the rendered tokens instead of re-validating.
	zxfer_refresh_ssh_transport_tokens_for_role origin
	zxfer_refresh_ssh_transport_tokens_for_role target
	zxfer_profile_add_elapsed_ms g_zxfer_profile_ssh_setup_ms "$l_ssh_setup_start_ms"
}

# Purpose: Compose post-CLI execution-context initialization in stable order.
# Usage: Called by zxfer_session_run after remote host preparation.
zxfer_init_variables() {
	zxfer_reset_endpoint_compression_commands
	zxfer_init_source_execution_context
	zxfer_init_destination_execution_context
	zxfer_refresh_remote_zfs_commands
	zxfer_init_restore_property_helpers
	zxfer_init_local_awk_compatibility
}

# Purpose: Initialize one complete zxfer session after all modules are loaded.
# Usage: Called once by zxfer_main before parsing options or preparing hosts.
# Side effects: Secures early reporting dependencies, installs traps, resolves
# configured dependency defaults, and resets session state.
zxfer_session_initialize() {
	zxfer_discard_inherited_cleanup_state
	zxfer_initialize_dependency_reporting_defaults
	zxfer_register_runtime_traps
	zxfer_initialize_dependency_defaults
	zxfer_init_globals
}

# Purpose: Parse, validate, and execute one zxfer replication session.
# Usage: Called by zxfer_main with the original launcher arguments after startup.
# Side effects: May inspect or modify ZFS state according to the parsed options.
zxfer_session_run() {
	zxfer_set_failure_stage "cli parse"
	zxfer_read_command_line_switches "$@"

	shift "$((OPTIND - 1))"
	zxfer_set_destination_argument "${1:-}"

	if [ $# -lt 1 ]; then
		zxfer_throw_usage_error "Need a destination."
	fi

	zxfer_set_failure_roots "" "$g_destination"
	zxfer_set_failure_stage "cli validation"
	zxfer_consistency_check
	zxfer_prepare_remote_host_connections
	zxfer_init_variables

	zxfer_set_failure_stage "replication"
	zxfer_run_zfs_mode_loop

	# Live -k runs persist after each successful dataset; this final pass also
	# preserves the established dry-run preview and safety-flush behavior.
	if [ "$g_option_k_backup_property_mode" -eq 1 ]; then
		if zxfer_write_backup_properties; then
			:
		else
			l_session_backup_write_status=$?
			zxfer_throw_error "Failed to write backup metadata." \
				"$l_session_backup_write_status"
			return "$l_session_backup_write_status"
		fi
	fi

	zxfer_beep 0
	# Notification is best-effort. Preserve the historical launcher contract:
	# successful replication exits zero even when the optional beep path fails.
	return 0
}

# Purpose: Compose startup and execution behind one launcher entry point.
# Usage: The zxfer launcher calls this after loading the canonical module set.
# Side effects: Runs a complete zxfer session and leaves final cleanup to traps.
zxfer_main() {
	zxfer_session_initialize
	zxfer_session_run "$@"
}
