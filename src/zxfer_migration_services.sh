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
# SOLARIS / ILLUMOS MIGRATION SERVICE HANDLING
################################################################################

# Module contract:
# owns globals: g_zxfer_services_to_restart (space-separated, restart order),
# g_services_need_relaunch, g_services_relaunch_in_progress,
# g_zxfer_migration_service_restore_failure_message, and
# g_zxfer_normalized_service_list_result.
# reads globals: g_option_m_migrate, g_option_c_services, g_option_n_dryrun,
# g_initial_source, g_recursive_source_list, and g_zxfer_failure_message at
# exit.
# mutates caches: pending SMF service-restart state; -m preparation rebuilds
# the replication dataset lists through zxfer_refresh_dataset_iteration_state.
# returns via stdout: none.

# Purpose: Reset pending migration-service recovery state for a new session.
# Usage: Called by the session composition root before migration preflight.
zxfer_reset_migration_service_state() {
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0
	g_zxfer_services_to_restart=""
	g_zxfer_migration_service_restore_failure_message=""
	g_zxfer_normalized_service_list_result=""
}

# Purpose: Split a whitespace-separated SMF service list into one name per
# line; patterns such as svc:/site/* stay literal.
# Usage: zxfer_normalize_service_list LIST; publishes
# g_zxfer_normalized_service_list_result.
zxfer_normalize_service_list() {
	g_zxfer_normalized_service_list_result=""
	zxfer_split_begin
	# shellcheck disable=SC2086 # split the operator's whitespace-separated list
	set -- $1
	zxfer_split_end
	for l_normalize_service_name in "$@"; do
		g_zxfer_normalized_service_list_result=${g_zxfer_normalized_service_list_result:+$g_zxfer_normalized_service_list_result$ZXFER_LF}$l_normalize_service_name
	done
}

# Purpose: Disable the SMF services in LIST and queue each one for relaunch;
# under -n only print the commands.
# Usage: zxfer_stopsvcs LIST; a failed disable re-enables what was already
# stopped and throws.
zxfer_stopsvcs() {
	zxfer_set_failure_stage "migration service handling"
	zxfer_normalize_service_list "$1"
	while IFS= read -r l_stopsvcs_service; do
		[ -n "$l_stopsvcs_service" ] || continue
		if [ "$g_option_n_dryrun" -eq 1 ]; then
			zxfer_render_shell_command_from_argv svcadm disable -st "$l_stopsvcs_service"
			zxfer_echov "Dry run: $g_zxfer_shell_command_result"
		else
			zxfer_echov "Disabling service $l_stopsvcs_service."
			svcadm disable -st "$l_stopsvcs_service" || {
				zxfer_relaunch
				zxfer_throw_error "Could not disable service $l_stopsvcs_service."
			}
		fi
		g_zxfer_services_to_restart="$g_zxfer_services_to_restart $l_stopsvcs_service"
		g_services_need_relaunch=1
	done <<EOF
$g_zxfer_normalized_service_list_result
EOF
}

# Purpose: Stop the -c services, unmount every source dataset and take the -m
# snapshot.
# Usage: zxfer_prepare_migration_services, after the first pass's discovery;
# under -n only previews the commands.
zxfer_prepare_migration_services() {
	[ "$g_option_m_migrate" -eq 1 ] || return
	zxfer_set_failure_stage "migration service handling"

	[ -z "$g_option_c_services" ] || zxfer_stopsvcs "$g_option_c_services"

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		if zxfer_command_display_render_enabled; then
			while IFS= read -r l_migration_source; do
				[ -n "$l_migration_source" ] || continue
				zxfer_echov "Dry run: $(zxfer_render_source_zfs_command unmount "$l_migration_source")"
			done <<EOF
${g_recursive_source_list:-}
EOF
		fi
		zxfer_newsnap "$g_initial_source"
		return
	fi

	# Every dataset must be mounted before any is unmounted or snapshotted.
	# ssh inside these loops must not read the list.
	while IFS= read -r l_migration_source; do
		[ -n "$l_migration_source" ] || continue
		if ! l_migration_mounted=$(zxfer_run_source_zfs_cmd get -Ho value mounted "$l_migration_source" </dev/null); then
			zxfer_throw_error "Couldn't determine whether source $l_migration_source is mounted."
		fi
		if [ "$l_migration_mounted" != "yes" ]; then
			zxfer_throw_usage_error "The source filesystem is not mounted, cannot use -m."
		fi
	done <<EOF
${g_recursive_source_list:-}
EOF

	while IFS= read -r l_migration_source; do
		[ -n "$l_migration_source" ] || continue
		zxfer_echov "Unmounting $l_migration_source."
		if ! zxfer_run_source_zfs_cmd unmount "$l_migration_source" </dev/null; then
			zxfer_relaunch
			zxfer_throw_error "Couldn't unmount source $l_migration_source."
		fi
	done <<EOF
${g_recursive_source_list:-}
EOF

	zxfer_newsnap "$g_initial_source"
	# The new snapshots must be discovered before they can be sent.
	zxfer_refresh_dataset_iteration_state
}

# Purpose: Re-enable stopped SMF services without exiting the calling shell.
# Usage: Called by zxfer_restore_migration_services_on_exit at exit, and by
# zxfer_relaunch before it applies the ordinary operator-facing throw. Failed
# services remain queued.
# Returns: Zero on complete restoration, otherwise 1 with the failure message
# published in $g_zxfer_migration_service_restore_failure_message.
zxfer_restore_migration_services_status_only() {
	g_zxfer_migration_service_restore_failure_message=""
	if [ -z "$g_zxfer_services_to_restart" ]; then
		g_services_need_relaunch=0
		g_services_relaunch_in_progress=0
		return 0
	fi

	g_services_relaunch_in_progress=1
	l_restore_services_failed=""
	l_restore_services_failed_count=0
	zxfer_normalize_service_list "$g_zxfer_services_to_restart"
	while IFS= read -r l_restore_service; do
		[ -n "$l_restore_service" ] || continue
		zxfer_echov "Restarting service $l_restore_service"
		if [ "$g_option_n_dryrun" -eq 1 ]; then
			zxfer_render_shell_command_from_argv svcadm enable "$l_restore_service"
			zxfer_echov "Dry run: $g_zxfer_shell_command_result"
			continue
		fi
		if ! svcadm enable "$l_restore_service"; then
			l_restore_services_failed_count=$((l_restore_services_failed_count + 1))
			l_restore_services_failed=${l_restore_services_failed:+$l_restore_services_failed }$l_restore_service
		fi
	done <<EOF
$g_zxfer_normalized_service_list_result
EOF

	if [ "$l_restore_services_failed_count" -gt 0 ]; then
		g_zxfer_services_to_restart=$l_restore_services_failed
		g_services_need_relaunch=1
		if [ "$l_restore_services_failed_count" -eq 1 ]; then
			g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service $l_restore_services_failed."
		else
			g_zxfer_migration_service_restore_failure_message="Couldn't re-enable services: $l_restore_services_failed."
		fi
		return 1
	fi

	g_zxfer_services_to_restart=""
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0
	return 0
}

# Purpose: Restart the SMF services a run leaves stopped, from the exit trap.
# Usage: zxfer_restore_migration_services_on_exit, from zxfer_trap_exit.
# Returns 0 when no service waits, or when a failed zxfer_relaunch already
# left them stopped; otherwise the status of the restore, with the operator
# message in g_zxfer_migration_service_restore_failure_message. When the run
# has already recorded its own failure, which keeps the report, that message
# also goes to stderr: the operator may need to restart the service by hand.
zxfer_restore_migration_services_on_exit() {
	[ "${g_services_need_relaunch:-0}" -eq 1 ] || return 0
	if [ "${g_services_relaunch_in_progress:-0}" -eq 1 ]; then
		zxfer_echoV "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
		return 0
	fi
	zxfer_echoV "zxfer exiting early; restarting stopped services."
	zxfer_restore_migration_services_status_only && return 0
	l_exit_restore_status=$?
	g_zxfer_migration_service_restore_failure_message=${g_zxfer_migration_service_restore_failure_message:-Failed to restore stopped migration services during exit.}
	[ -z "${g_zxfer_failure_message:-}" ] ||
		zxfer_warn_stderr "$g_zxfer_migration_service_restore_failure_message"
	return "$l_exit_restore_status"
}

# Purpose: Re-enable every SMF service stopped during migration preparation.
# Usage: Called by ordinary replication flows that retain the established
# operator-facing throw behavior on restoration failure.
zxfer_relaunch() {
	zxfer_set_failure_stage "migration service handling"
	zxfer_restore_migration_services_status_only ||
		zxfer_throw_error "${g_zxfer_migration_service_restore_failure_message:-Could not restore stopped migration services.}"
}
