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
# SOLARIS / ILLUMOS MIGRATION SERVICE HANDLING
################################################################################

# Module contract:
# owns globals: g_zxfer_services_to_restart, g_services_need_relaunch,
# g_services_relaunch_in_progress, and the status-only restore failure result.
# reads globals: dry-run option state.
# mutates caches: pending SMF service-restart state only.
# returns via stdout: normalized service identifiers.

# Purpose: Reset pending migration-service recovery state for a new session.
# Usage: Called by the session composition root before migration preflight.
zxfer_reset_migration_service_state() {
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0
	g_zxfer_services_to_restart=""
	g_zxfer_migration_service_restore_failure_message=""
}

# Purpose: Stop requested SMF services and remember each successful disable.
# Usage: Reads a whitespace-delimited service list from stdin during live
# Solaris/illumos migration preparation.
zxfer_stopsvcs() {
	zxfer_set_failure_stage "migration service handling"
	l_raw_services=$(cat)

	[ -n "$l_raw_services" ] || return
	l_normalized_services=$(zxfer_normalize_service_list "$l_raw_services")
	[ -n "$l_normalized_services" ] || return

	while IFS= read -r service; do
		zxfer_echov "Disabling service $service."
		svcadm disable -st "$service" || {
			zxfer_relaunch
			zxfer_throw_error "Could not disable service $service."
		}
		g_zxfer_services_to_restart="$g_zxfer_services_to_restart $service"
		g_services_need_relaunch=1
	done <<EOF
$l_normalized_services
EOF
}

# Purpose: Normalize a service list into one identifier per line.
# Usage: Shared by live, preview, and recovery bookkeeping paths.
zxfer_normalize_service_list() {
	l_raw_services=$1
	[ -n "$l_raw_services" ] || return

	printf '%s\n' "$l_raw_services" | awk '
{
	for (i = 1; i <= NF; i++)
		print $i
}'
}

# Purpose: Render the exact SMF disable commands for a dry run.
# Usage: Called by migration orchestration without changing service state.
zxfer_preview_service_disable_commands() {
	l_raw_services=$1
	l_normalized_services=$(zxfer_normalize_service_list "$l_raw_services")
	[ -n "$l_normalized_services" ] || return

	while IFS= read -r service; do
		zxfer_echov "Dry run: $(zxfer_build_shell_command_from_argv svcadm disable -st "$service")"
	done <<EOF
$l_normalized_services
EOF
}

# Purpose: Record services that a migration dry run would later restart.
# Usage: Keeps cleanup state identical between live and preview flows.
zxfer_record_services_for_relaunch() {
	l_raw_services=$1
	l_normalized_services=$(zxfer_normalize_service_list "$l_raw_services")
	[ -n "$l_normalized_services" ] || return

	while IFS= read -r service; do
		[ -n "$service" ] || continue
		g_zxfer_services_to_restart="$g_zxfer_services_to_restart $service"
		g_services_need_relaunch=1
	done <<EOF
$l_normalized_services
EOF
}

# Purpose: Re-enable stopped SMF services without exiting the calling shell.
# Usage: Called by session trap cleanup, and by zxfer_relaunch before it applies
# the ordinary operator-facing throw behavior. Failed services remain queued.
# Returns: Zero on complete restoration, otherwise a checked nonzero status and
# an owner-published failure message.
zxfer_restore_migration_services_status_only() {
	g_zxfer_migration_service_restore_failure_message=""
	[ -z "$g_zxfer_services_to_restart" ] && {
		g_services_need_relaunch=0
		g_services_relaunch_in_progress=0
		return 0
	}

	g_services_relaunch_in_progress=1
	l_migration_restore_failed_services=""
	l_migration_restore_failed_count=0
	l_migration_restore_normalize_status=0
	l_migration_restore_services=$(zxfer_normalize_service_list "$g_zxfer_services_to_restart") ||
		l_migration_restore_normalize_status=$?
	if [ "$l_migration_restore_normalize_status" -ne 0 ]; then
		g_zxfer_migration_service_restore_failure_message="Could not normalize the pending service restart list."
		return "$l_migration_restore_normalize_status"
	fi

	while IFS= read -r l_migration_restore_service ||
		[ -n "$l_migration_restore_service" ]; do
		[ -n "$l_migration_restore_service" ] || continue
		zxfer_echov "Restarting service $l_migration_restore_service"
		if [ "$g_option_n_dryrun" -eq 1 ]; then
			zxfer_echov "Dry run: $(zxfer_build_shell_command_from_argv svcadm enable "$l_migration_restore_service")"
			continue
		fi
		if ! svcadm enable "$l_migration_restore_service"; then
			l_migration_restore_failed_count=$((l_migration_restore_failed_count + 1))
			if [ -z "$l_migration_restore_failed_services" ]; then
				l_migration_restore_failed_services=$l_migration_restore_service
			else
				l_migration_restore_failed_services="$l_migration_restore_failed_services $l_migration_restore_service"
			fi
		fi
	done <<EOF
$l_migration_restore_services
EOF

	if [ "$l_migration_restore_failed_count" -gt 0 ]; then
		g_zxfer_services_to_restart=$l_migration_restore_failed_services
		g_services_need_relaunch=1
		if [ "$l_migration_restore_failed_count" -eq 1 ]; then
			g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service $l_migration_restore_failed_services."
		else
			g_zxfer_migration_service_restore_failure_message="Couldn't re-enable services: $l_migration_restore_failed_services."
		fi
		return 1
	fi

	g_zxfer_services_to_restart=""
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0
	return 0
}

# Purpose: Re-enable every SMF service stopped during migration preparation.
# Usage: Called by ordinary replication flows that retain the established
# operator-facing throw behavior on restoration failure.
zxfer_relaunch() {
	zxfer_set_failure_stage "migration service handling"
	l_relaunch_restore_status=0
	zxfer_restore_migration_services_status_only ||
		l_relaunch_restore_status=$?
	if [ "$l_relaunch_restore_status" -ne 0 ]; then
		zxfer_throw_error "${g_zxfer_migration_service_restore_failure_message:-Could not restore stopped migration services.}"
	fi
}
