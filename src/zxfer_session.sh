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
# owns globals: g_zxfer_version, g_zxfer_local_os, g_backup_file_extension and
#   the exit status zxfer_trap_exit builds, g_zxfer_trap_exit_status.
# reads globals: parsed option state and replication context after
#   initialization; zxfer_trap_exit also reads g_zxfer_run_umask and the
#   quoting line constants.
# writes dependency-owned globals: g_cmd_cat (-e), g_cmd_awk (gawk on SunOS)
#   and the endpoint g_origin_cmd_compress_safe/g_target_cmd_decompress_safe.
# mutates caches: invokes each owner module's reset, startup, and cleanup APIs.
# returns via stdout: none.
#
# Startup is zxfer_reset_session_state (assignments only), then the traps, then
# zxfer_init_session_environment (secure PATH, helpers, temp root). This module
# remains last in the canonical manifest. Its exit path coordinates the
# migration-service owner so stopped illumos/Solaris services are restarted
# during shutdown.

# Purpose: Reset every owner module's session state, dropping inherited
# handles without acting on them.
# Usage: Called first by zxfer_session_initialize. Each owner reset runs once
# and only assigns: nothing here signals a process, contacts a host, removes a
# path or restarts a service.
zxfer_reset_session_state() {
	# Dependency commands go first: the ssh and remote-host resets copy
	# g_cmd_zfs and probe g_cmd_ssh, so neither may keep an inherited value.
	zxfer_reset_dependency_state
	# CLI defaults make early trap decisions inert; the failure reset keeps
	# the launcher-captured original invocation.
	zxfer_init_cli_option_defaults
	zxfer_reset_failure_context "startup"
	zxfer_reset_path_security_state
	# Dropping the run-root handles first leaves the snapshot-discovery reset
	# no owned cache path to remove.
	zxfer_discard_runtime_cleanup_state
	zxfer_reset_send_job_state
	zxfer_reset_background_shell_spawn_mode
	zxfer_reset_ssh_transport_state
	zxfer_reset_migration_service_state
	zxfer_reset_profile_state
	zxfer_reset_replication_runtime_state
	zxfer_reset_send_receive_state
	zxfer_reset_destination_existence_cache
	zxfer_reset_live_destination_listing_state
	zxfer_reset_snapshot_producer_session_state
	zxfer_reset_snapshot_discovery_state
	zxfer_reset_snapshot_plan_state
	zxfer_reset_snapshot_delete_artifact_state
	zxfer_reset_backup_metadata_state
	zxfer_reset_property_runtime_state
	zxfer_reset_property_iteration_caches
	zxfer_reset_property_read_state
	zxfer_reset_remote_host_state

	g_zxfer_version="2.0.20260930"
	g_zxfer_local_os=""
	g_backup_file_extension=".zxfer_backup_info"
}

# Purpose: Prepare the host side of a session: secure PATH, backup root,
# required helpers, run temp root, then the narrowed PATH.
# Usage: Called after zxfer_reset_session_state. Throws on an unusable secure
# PATH, a bad ZXFER_BACKUP_DIR or a missing helper.
zxfer_init_session_environment() {
	zxfer_refresh_secure_path_state ||
		zxfer_reject_invalid_secure_path_configuration
	zxfer_init_backup_storage_root
	zxfer_init_dependency_tool_defaults
	# Create the per-run temp root before narrowing PATH so bootstrap helpers
	# still have access to base utilities such as mktemp even when an explicit
	# ZXFER_SECURE_PATH intentionally omits their directories. Failure stays
	# non-fatal; the first allocation that needs the root reports it.
	zxfer_ensure_run_tmp_root || :
	zxfer_apply_secure_path
}

# Purpose: Fold one failed trap cleanup step into the exit status and the
# failure report.
# Usage: zxfer_note_trap_cleanup_failure STATUS MESSAGE, from zxfer_trap_exit;
# a zero STATUS does nothing. An otherwise clean exit takes STATUS, and
# MESSAGE becomes the report's message unless one is already recorded.
zxfer_note_trap_cleanup_failure() {
	[ "$1" -ne 0 ] || return 0
	[ "$g_zxfer_trap_exit_status" -ne 0 ] || g_zxfer_trap_exit_status=$1
	zxfer_set_failure_context_if_empty runtime "trap cleanup" "$2"
}

# Purpose: Run the centralized shutdown path that cleans up runtime artifacts,
# transports, and end-of-run reporting state.
# Usage: zxfer_trap_exit [SIGNAL_STATUS]; the EXIT trap passes nothing and
# each signal trap passes its 128+signo status.
# Side effects: Preserves shutdown ordering, promotes cleanup failures, restarts
# stopped migration services, emits profile/failure output, and exits. After a
# signal, that exit runs the EXIT trap once more with $? already set; the
# emitted-report guard keeps the second pass from printing a second report.
zxfer_trap_exit() {
	# get the exit status of the last command
	g_zxfer_trap_exit_status=$?
	# A signal can land while zxfer_create_runtime_artifact_file holds umask
	# 077 and noclobber, or between zxfer_split_begin and zxfer_split_end.
	# Put the run's shell modes back before any cleanup or report write.
	set +C +f
	IFS=" $ZXFER_TAB$ZXFER_LF"
	case ${g_zxfer_run_umask:-} in
	'' | *[!0-7]*) ;;
	*) umask "$g_zxfer_run_umask" ;;
	esac
	# After a signal, $? is the status of the command it interrupted (0 after
	# a -j poll sleep), so each signal trap passes its own status instead.
	if [ -n "${1:-}" ]; then
		g_zxfer_trap_exit_status=$1
		zxfer_set_failure_context_if_empty runtime "signal" \
			"zxfer was interrupted by a signal (exit status $1)."
	fi
	zxfer_profile_start_timer
	l_cleanup_start_ms=$g_zxfer_profile_clock_ms

	# Only terminate zxfer-owned background processes. Killing every direct child
	# of the shell is too broad and can clobber coverage helpers or command
	# substitution plumbing in the caller. Long-lived send/receive jobs stop
	# first, then the short-lived registered helpers.
	zxfer_abort_all_send_jobs ||
		zxfer_note_trap_cleanup_failure "$?" \
			"${g_zxfer_send_job_abort_failure_message:-Failed to tear down one or more send/receive jobs during exit.}"
	zxfer_kill_registered_cleanup_pids ||
		zxfer_note_trap_cleanup_failure "$?" \
			"${g_zxfer_cleanup_pid_abort_failure_message:-Failed to tear down one or more validated cleanup helpers during exit.}"
	# A failed close counts only as the run's first failure: after another
	# one, its own diagnostic on stderr is all it adds.
	l_trap_close_status=0
	zxfer_close_all_ssh_control_sockets || l_trap_close_status=$?
	[ "$g_zxfer_trap_exit_status" -ne 0 ] ||
		zxfer_note_trap_cleanup_failure "$l_trap_close_status" \
			"Failed to close one or more ssh control sockets during exit."
	# Every per-run transient lives under the one private temp root, which
	# one rm -rf removes; only ssh's short socket directory, made when the
	# root's socket path would be too long, lives outside it.
	l_trap_artifact_status=0
	zxfer_remove_ssh_control_socket_dir || l_trap_artifact_status=1
	zxfer_remove_run_tmp_root || l_trap_artifact_status=1
	zxfer_note_trap_cleanup_failure "$l_trap_artifact_status" \
		"Failed to remove one or more runtime temp artifacts during exit."
	zxfer_restore_migration_services_on_exit ||
		zxfer_note_trap_cleanup_failure "$?" \
			"$g_zxfer_migration_service_restore_failure_message"

	zxfer_profile_stop_timer "$l_cleanup_start_ms"
	g_zxfer_profile_cleanup_ms=$((g_zxfer_profile_cleanup_ms + g_zxfer_profile_elapsed_ms))
	zxfer_echoV "zxfer exiting with status $g_zxfer_trap_exit_status"
	zxfer_profile_emit_summary
	zxfer_emit_failure_report "$g_zxfer_trap_exit_status"

	# Failure reporting may lazily recreate the run temp root; sweep again so
	# nothing survives exit.
	zxfer_remove_run_tmp_root >/dev/null 2>&1 || :

	# exit this script
	exit "$g_zxfer_trap_exit_status"
}

# Purpose: Resolve one endpoint's operating system, zfs command and, under -z,
# its compression command: the origin compresses, the target decompresses.
# Usage: zxfer_init_endpoint_execution_context origin|target; called by
# zxfer_init_variables once g_zxfer_local_os is set. A dry run never contacts
# the remote host and renders the local command names instead.
zxfer_init_endpoint_execution_context() {
	if [ "$1" = origin ]; then
		l_endpoint_host=$g_option_O_origin_host
		l_endpoint_side=source
		l_endpoint_zfs=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_endpoint_codec=$g_cmd_compress
		l_endpoint_codec_label="compression command"
		l_endpoint_codec_safe=${g_origin_cmd_compress_safe:-}
	else
		l_endpoint_host=$g_option_T_target_host
		l_endpoint_side=destination
		l_endpoint_zfs=${g_target_cmd_zfs:-$g_cmd_zfs}
		l_endpoint_codec=$g_cmd_decompress
		l_endpoint_codec_label="decompression command"
		l_endpoint_codec_safe=${g_target_cmd_decompress_safe:-}
	fi

	if [ -z "$l_endpoint_host" ]; then
		zxfer_publish_endpoint_runtime_context "$1" "$g_zxfer_local_os" "$g_cmd_zfs"
		return
	fi

	l_endpoint_os=""
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		if [ "$g_option_z_compress" -eq 1 ] && [ -z "$l_endpoint_codec_safe" ]; then
			zxfer_quote_cli_tokens "$l_endpoint_codec" "$l_endpoint_codec_label" ||
				zxfer_throw_error "$g_zxfer_literal_token_error_result"
			l_endpoint_codec_safe=$g_zxfer_shell_command_result
		fi
		zxfer_echoV "Dry run: skipping live remote $l_endpoint_side helper validation."
	else
		zxfer_get_os "$l_endpoint_host" "$l_endpoint_side" >/dev/null ||
			zxfer_throw_dependency_error "Failed to determine operating system on host $l_endpoint_host." "$?"
		l_endpoint_os=$g_zxfer_os_result
		zxfer_resolve_remote_required_tool "$l_endpoint_host" zfs zfs "$l_endpoint_side" ||
			zxfer_throw_dependency_error "$g_zxfer_required_tool_result" "$?"
		l_endpoint_zfs=$g_zxfer_required_tool_result
		if [ "$g_option_z_compress" -eq 1 ]; then
			zxfer_resolve_cli_command_safe "$l_endpoint_host" "$l_endpoint_codec" \
				"$l_endpoint_codec_label" "$l_endpoint_side" ||
				zxfer_throw_dependency_error "$g_zxfer_resolved_cli_command_result" "$?"
			l_endpoint_codec_safe=$g_zxfer_resolved_cli_command_result
		fi
	fi

	zxfer_publish_endpoint_runtime_context "$1" "$l_endpoint_os" "$l_endpoint_zfs"
	if [ "$1" = origin ]; then
		g_origin_cmd_compress_safe=$l_endpoint_codec_safe
	else
		g_target_cmd_decompress_safe=$l_endpoint_codec_safe
	fi
}

# Purpose: Resolve the cat helper that -e reads backup metadata with, locally
# or on the -O host.
# Usage: Called by zxfer_init_variables after the endpoint contexts.
zxfer_init_restore_property_helpers() {
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return

	if [ "$g_option_O_origin_host" = "" ]; then
		zxfer_require_tool cat
		g_cmd_cat=$g_zxfer_required_tool_result
		return
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		[ -n "${g_cmd_cat:-}" ] || g_cmd_cat='cat'
		zxfer_echoV "Dry run: skipping live remote backup-restore helper validation."
		return
	fi

	zxfer_resolve_remote_required_tool "$g_option_O_origin_host" cat cat source ||
		zxfer_throw_dependency_error "$g_zxfer_required_tool_result" "$?"
	g_cmd_cat=$g_zxfer_required_tool_result
}

# Purpose: Prefer gawk on SunOS, the established compatibility path.
# Usage: Called by zxfer_init_variables once g_zxfer_local_os is set.
zxfer_init_local_awk_compatibility() {
	[ "$g_zxfer_local_os" = "SunOS" ] || return 0
	if zxfer_find_tool_in_path gawk "$g_zxfer_secure_path"; then
		g_cmd_awk=$g_zxfer_tool_path_result
	fi
}

# Purpose: Connect the -O and -T hosts: resolve ssh, open each role's control
# master, then probe each host's capabilities over it.
# Usage: Called after CLI validation and before zxfer_init_variables, so every
# remote command of the run multiplexes over the role's master. A dry run
# contacts no host; without control-socket support each command connects
# directly.
# Side effects: Sets g_cmd_ssh, the role sockets and capability slots, and
# refreshes the -O/-T zfs routing.
zxfer_prepare_remote_host_connections() {
	if [ -z "$g_option_O_origin_host" ] && [ -z "$g_option_T_target_host" ]; then
		zxfer_refresh_remote_zfs_commands
		return
	fi
	zxfer_profile_start_timer
	l_ssh_setup_start_ms=$g_zxfer_profile_clock_ms

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		if [ -n "$g_option_O_origin_host" ]; then
			zxfer_echoV "Dry run: skipping ssh control-socket setup and remote capability preload for origin host."
		fi
		if [ -n "$g_option_T_target_host" ]; then
			zxfer_echoV "Dry run: skipping ssh control-socket setup and remote capability preload for target host."
		fi
	else
		zxfer_ensure_local_ssh_command ||
			zxfer_throw_dependency_error "$g_zxfer_resolved_local_ssh_command_result"
		zxfer_refresh_ssh_control_socket_support_state
		zxfer_open_ssh_control_sockets
		if [ -n "$g_option_O_origin_host" ]; then
			zxfer_preload_remote_host_capabilities "$g_option_O_origin_host" source || :
		fi
		if [ -n "$g_option_T_target_host" ]; then
			zxfer_preload_remote_host_capabilities "$g_option_T_target_host" destination || :
		fi
	fi
	zxfer_refresh_remote_zfs_commands
	zxfer_profile_stop_timer "$l_ssh_setup_start_ms"
	g_zxfer_profile_ssh_setup_ms=$((g_zxfer_profile_ssh_setup_ms + g_zxfer_profile_elapsed_ms))
}

# Purpose: Resolve both endpoints' execution context after CLI validation.
# Usage: Called by zxfer_session_run after zxfer_prepare_remote_host_connections.
zxfer_init_variables() {
	zxfer_get_os "" >/dev/null ||
		zxfer_throw_dependency_error "Failed to determine the local operating system." "$?"
	g_zxfer_local_os=$g_zxfer_os_result
	zxfer_reset_endpoint_compression_commands
	zxfer_init_endpoint_execution_context origin
	zxfer_init_endpoint_execution_context target
	zxfer_refresh_remote_zfs_commands
	zxfer_init_restore_property_helpers
	zxfer_init_local_awk_compatibility
	zxfer_prepare_readonly_property_policy
}

# Purpose: Start one zxfer session: reset state, install the traps, then
# prepare the environment.
# Usage: Called once by zxfer_main before parsing options.
# Side effects: Installs the EXIT and signal traps, may create the run temp
# root, and narrows PATH to the secure PATH.
zxfer_session_initialize() {
	zxfer_reset_session_state
	zxfer_initialize_dependency_reporting_defaults
	# EXIT takes its status from $?; each signal exits 128+signo.
	trap 'zxfer_trap_exit 129' HUP
	trap 'zxfer_trap_exit 130' INT
	trap 'zxfer_trap_exit 131' QUIT
	trap 'zxfer_trap_exit 143' TERM
	trap zxfer_trap_exit EXIT
	zxfer_init_session_environment
}

# Purpose: Parse, validate, and execute one zxfer replication session.
# Usage: Called by zxfer_main with the original launcher arguments after startup.
# Side effects: May inspect or modify ZFS state according to the parsed options.
zxfer_session_run() {
	zxfer_set_failure_stage "cli parse"
	zxfer_read_command_line_switches "$@"

	shift "$((OPTIND - 1))"
	g_destination=${1:-}

	if [ $# -lt 1 ]; then
		zxfer_throw_usage_error "Need a destination."
	fi

	zxfer_set_failure_roots "" "$g_destination"
	zxfer_set_failure_stage "cli validation"
	zxfer_consistency_check
	# Local -j source discovery needs parallel: fail before any ssh master or
	# background helper starts. With -O the origin's parallel is checked when
	# discovery first needs it.
	if [ -z "${g_option_O_origin_host:-}" ] &&
		! zxfer_ensure_parallel_available_for_source_jobs; then
		zxfer_throw_dependency_error "$g_zxfer_parallel_source_job_check_result"
	fi
	zxfer_prepare_remote_host_connections
	zxfer_init_variables

	zxfer_set_failure_stage "replication"
	zxfer_run_zfs_mode_loop

	# -k rows stay buffered and are published only at the post-seed checkpoint
	# and here at run end, so a run that fails before a publish leaves the
	# previous metadata files untouched. A dry run buffers no rows, so it
	# only prints the -v skip note.
	if [ "$g_option_k_backup_property_mode" -eq 1 ]; then
		zxfer_write_backup_properties || {
			l_session_backup_write_status=$?
			zxfer_throw_error "Failed to write backup metadata." \
				"$l_session_backup_write_status"
			return "$l_session_backup_write_status"
		}
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
