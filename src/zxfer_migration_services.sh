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
# g_services_relaunch_in_progress and
# g_zxfer_migration_service_restore_failure_message. The queue alone says
# whether services need restoring; the guard prevents a second attempt at
# exit after an ordinary relaunch failed.
# reads globals: g_option_n_dryrun and g_zxfer_failure_message at exit.
# mutates caches: pending SMF service-restart state. Replication owns -m
# preparation and calls this module only to stop or restore services.
# returns via stdout: none.

# Purpose: Reset pending migration-service recovery state for a new session.
# Usage: Called by the session composition root before migration preflight.
zxfer_reset_migration_service_state() {
	g_services_relaunch_in_progress=0
	g_zxfer_services_to_restart=""
	g_zxfer_migration_service_restore_failure_message=""
}

# Purpose: Disable the SMF services in LIST and queue each one for relaunch;
# under -n only print the commands.
# Usage: zxfer_stopsvcs LIST; a failed disable re-enables what was already
# stopped and throws.
zxfer_stopsvcs() {
	zxfer_set_failure_stage "migration service handling"
	# Own the service arguments across recovery and other helper calls.
	zxfer_split_begin
	# shellcheck disable=SC2086 # Whitespace separates names; patterns stay literal.
	set -- $1
	zxfer_split_end
	for l_stopsvcs_service; do
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
	done
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
		g_services_relaunch_in_progress=0
		return 0
	fi

	g_services_relaunch_in_progress=1
	l_restore_services_failed=""
	l_restore_services_failed_count=0
	zxfer_split_begin
	# shellcheck disable=SC2086 # The queue is a whitespace-separated service list.
	set -- $g_zxfer_services_to_restart
	zxfer_split_end
	for l_restore_service; do
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
	done

	if [ "$l_restore_services_failed_count" -gt 0 ]; then
		g_zxfer_services_to_restart=$l_restore_services_failed
		if [ "$l_restore_services_failed_count" -eq 1 ]; then
			g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service $l_restore_services_failed."
		else
			g_zxfer_migration_service_restore_failure_message="Couldn't re-enable services: $l_restore_services_failed."
		fi
		return 1
	fi

	g_zxfer_services_to_restart=""
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
	[ -n "${g_zxfer_services_to_restart:-}" ] || return 0
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
