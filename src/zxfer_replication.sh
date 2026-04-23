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
# REPLICATION ORCHESTRATION
################################################################################

# Module contract:
# owns globals: per-run orchestration scratch state such as g_zxfer_services_to_restart, g_zxfer_post_seed_property_sources_result, g_zxfer_replication_iteration_list_result, and g_zxfer_replication_file_read_result plus per-dataset replication state like g_actual_dest, g_last_common_snap, and g_src_snapshot_transfer_list.
# reads globals: g_option_*, g_initial_source, g_destination, and current dataset/property state.
# mutates caches: per-iteration property and destination cache state through shared reset helpers.
# returns via stdout: none; orchestration mutates runtime state and delegates output to shared helpers.

# Purpose: Reset the replication runtime state so the next replication pass
# starts from a clean state.
# Usage: Called during top-level dataset iteration and replication
# orchestration before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_replication_runtime_state() {
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0
	g_zxfer_services_to_restart=""
	g_initial_source=""
	g_initial_source_had_trailing_slash=0
	g_actual_dest=""
	g_pending_receive_create_opts=""
	g_pending_receive_create_dest=""
	g_dest_seed_requires_property_reconcile=0
	g_zxfer_post_seed_property_sources_result=""
	g_zxfer_replication_iteration_list_result=""
	g_zxfer_replication_file_read_result=""
}

# Purpose: Read the replication stage file from staged state into the current
# shell.
# Usage: Called during top-level dataset iteration and replication
# orchestration when later helpers need a checked reload instead of ad hoc file
# reads.
zxfer_read_replication_stage_file() {
	l_stage_file=$1

	g_zxfer_replication_file_read_result=""
	if zxfer_read_runtime_artifact_file "$l_stage_file" >/dev/null; then
		l_stage_contents=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi
	case "$l_stage_contents" in
	*'
')
		l_stage_contents=${l_stage_contents%?}
		;;
	esac

	g_zxfer_replication_file_read_result=$l_stage_contents
	printf '%s\n' "$l_stage_contents"
}

# Purpose: Compute the actual dest for source from the active configuration and
# runtime state.
# Usage: Called during top-level dataset iteration and replication
# orchestration when later helpers need a derived value without duplicating the
# calculation.
zxfer_compute_actual_dest_for_source() {
	l_source=$1

	zxfer_get_destination_dataset_for_source_dataset "$l_source"
}

# Purpose: Check whether the current destination is initial source dataset.
# Usage: Called during top-level dataset iteration and replication
# orchestration when later helpers need a boolean answer about the current
# destination.
zxfer_current_destination_is_initial_source_dataset() {
	if ! l_initial_dest=$(zxfer_compute_actual_dest_for_source "$g_initial_source"); then
		return 1
	fi

	[ "$g_actual_dest" = "$l_initial_dest" ]
}

# Purpose: Update the actual dest in the shared runtime state.
# Usage: Called during top-level dataset iteration and replication
# orchestration after a probe or planning step changes the active context that
# later helpers should use.
#
# Prepare the actual destination (g_actual_dest) as used in zfs receive.
# Uses $g_destination, $g_initial_source
# Output is $g_actual_dest
zxfer_set_actual_dest() {
	l_source=$1

	g_actual_dest=$(zxfer_compute_actual_dest_for_source "$l_source")

	zxfer_set_current_dataset_context "$l_source" "$g_actual_dest"
}

# Purpose: Rollback the destination to last common snapshot to the last safe
# state this module recognizes.
# Usage: Called during top-level dataset iteration and replication
# orchestration when zxfer detects divergence and must re-establish a safe
# base.
zxfer_rollback_destination_to_last_common_snapshot() {
	# Never perform a destructive rollback unless the caller explicitly opted in
	# to receive-side forcing with -F. Without that flag, zxfer should fail safe
	# if the destination current head has diverged.
	if [ "${g_option_F_force_rollback:-}" = "" ]; then
		return
	fi

	# Only roll back when snapshot deletion pruned newer points on the destination.
	if [ "${g_did_delete_dest_snapshots:-0}" -ne 1 ]; then
		return
	fi
	if [ "${g_deleted_dest_newer_snapshots:-0}" -ne 1 ]; then
		return
	fi

	if ! l_dest_exists=$(zxfer_exists_destination "$g_actual_dest" live); then
		zxfer_throw_error "$l_dest_exists"
	fi
	if [ "$l_dest_exists" -eq 0 ]; then
		return
	fi

	l_last_common_name=$(zxfer_extract_snapshot_name "$g_last_common_snap")
	if [ -z "$l_last_common_name" ]; then
		return
	fi

	l_dest_snapshot="$g_actual_dest@$l_last_common_name"
	zxfer_echov "Rolling back $g_actual_dest to last common snapshot [$l_dest_snapshot] after deletions."
	if ! zxfer_run_destination_zfs_cmd rollback -r "$l_dest_snapshot"; then
		zxfer_throw_error "Failed to roll back destination [$g_actual_dest] to $l_dest_snapshot after deleting snapshots."
	fi

	g_did_delete_dest_snapshots=0
}

# Purpose: Return the live destination snapshots in the form expected by later
# helpers.
# Usage: Called during top-level dataset iteration and replication
# orchestration when sibling helpers need the same lookup without duplicating
# module logic.
zxfer_get_live_destination_snapshots() {
	if ! l_snapshot_records=$(zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$g_actual_dest"); then
		printf '%s\n' "$l_snapshot_records"
		return 1
	fi

	while IFS= read -r l_snapshot_record; do
		[ -n "$l_snapshot_record" ] || continue
		l_snapshot_path=$(zxfer_extract_snapshot_path "$l_snapshot_record")
		case "$l_snapshot_path" in
		"$g_actual_dest"@*)
			printf '%s\n' "$l_snapshot_record"
			;;
		esac
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_snapshot_records")
	EOF
}

# Purpose: Return the snapshot transfer bounds in the form expected by later
# helpers.
# Usage: Called during top-level dataset iteration and replication
# orchestration when sibling helpers need the same lookup without duplicating
# module logic.
zxfer_get_snapshot_transfer_bounds() {
	l_first_snapshot=""
	l_final_snapshot=""

	while IFS= read -r l_snapshot; do
		[ -n "$l_snapshot" ] || continue
		[ -z "$l_first_snapshot" ] && l_first_snapshot=$l_snapshot
		l_final_snapshot=$l_snapshot
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$g_src_snapshot_transfer_list")
	EOF

	[ -n "$l_final_snapshot" ] || return 1

	printf '%s\n%s\n' "$l_first_snapshot" "$l_final_snapshot"
}

# Purpose: Seed the destination for snapshot transfer so incremental work can
# continue from a valid base.
# Usage: Called during top-level dataset iteration and replication
# orchestration when zxfer must bootstrap a destination before sending the
# remaining range.
zxfer_seed_destination_for_snapshot_transfer() {
	l_first_snapshot=$1
	l_first_snapshot_path=$2

	if ! l_dest_exists=$(zxfer_exists_destination "$g_actual_dest"); then
		zxfer_throw_error "$l_dest_exists"
	fi
	if [ "$l_dest_exists" -eq 1 ] &&
		[ "${g_last_common_snap:-}" = "" ] &&
		[ "$g_dest_has_snapshots" -eq 1 ]; then
		if ! l_live_dest_snaps=$(zxfer_get_live_destination_snapshots 2>&1); then
			zxfer_throw_error "Failed to retrieve live destination snapshots for [$g_actual_dest]: $l_live_dest_snaps"
		fi
		if [ -z "$l_live_dest_snaps" ]; then
			g_dest_has_snapshots=0
		else
			zxfer_throw_error "Destination dataset [$g_actual_dest] has snapshots but none share a common guid with the source. Refusing to perform a full receive into an existing snapshotted dataset."
		fi
	fi
	if [ "$l_dest_exists" -eq 0 ]; then
		zxfer_echov "Destination dataset does not exist [$g_actual_dest]. Sending first snapshot [$l_first_snapshot_path]"
		zxfer_zfs_send_receive "" "$l_first_snapshot_path" "$g_actual_dest" "0"
		g_dest_seed_requires_property_reconcile=1
		g_last_common_snap=$l_first_snapshot
		g_dest_has_snapshots=1
		return
	fi
	if [ "$g_dest_has_snapshots" -eq 0 ]; then
		zxfer_echov "Destination dataset [$g_actual_dest] exists but has no snapshots. Seeding with [$l_first_snapshot_path]"
		zxfer_echov "Temporarily enabling receive-side -F to seed existing empty destination dataset [$g_actual_dest]."
		zxfer_zfs_send_receive "" "$l_first_snapshot_path" "$g_actual_dest" "0" "-F"
		g_dest_seed_requires_property_reconcile=1
		g_last_common_snap=$l_first_snapshot
		g_dest_has_snapshots=1
	fi
}

# Purpose: Check whether the snapshot transfer is complete.
# Usage: Called during top-level dataset iteration and replication
# orchestration when later helpers need a boolean answer about the snapshot
# transfer.
zxfer_snapshot_transfer_is_complete() {
	l_final_snapshot_path=$1
	l_last_common_path=$(zxfer_extract_snapshot_path "$g_last_common_snap")

	if [ -n "$l_last_common_path" ] && [ "$l_last_common_path" = "$l_final_snapshot_path" ]; then
		zxfer_echoV "Seed snapshot already matches final snapshot for $g_actual_dest."
		return 0
	fi

	return 1
}

# Purpose: Send the snapshot transfer range through the active replication
# pipeline.
# Usage: Called during top-level dataset iteration and replication
# orchestration after planning has chosen the exact source range and
# destination for the transfer.
zxfer_send_snapshot_transfer_range() {
	l_final_snapshot_path=$1

	zxfer_echoV "Final snapshot: $l_final_snapshot_path"
	zxfer_zfs_send_receive "$(zxfer_extract_snapshot_path "$g_last_common_snap")" "$l_final_snapshot_path" "$g_actual_dest" "1"
}

# Purpose: Copy the snapshots through the replication path owned by this
# module.
# Usage: Called during top-level dataset iteration and replication
# orchestration after discovery and reconciliation have produced the exact work
# list.
#
# Copy from the last common snapshot to the most recent snapshot.
# Assumes that the list of snapshots is given in creation order ascending.
# Takes: $g_last_common_snap, $g_src_snapshot_transfer_list
zxfer_copy_snapshots() {
	# Long-running transfers can drift from the original plan; refresh live
	# destination state before sending, and use -Y to repeat until convergence.
	g_dest_seed_requires_property_reconcile=0

	zxfer_reconcile_live_destination_snapshot_state

	if ! l_snapshot_transfer_bounds=$(zxfer_get_snapshot_transfer_bounds); then
		zxfer_echoV "No snapshots to copy, skipping destination dataset: $g_actual_dest."
		return
	fi
	{
		IFS= read -r l_first_snapshot
		IFS= read -r l_final_snapshot
	} <<-EOF
		$l_snapshot_transfer_bounds
	EOF

	l_first_snapshot_path=$(zxfer_extract_snapshot_path "$l_first_snapshot")
	l_final_snapshot_path=$(zxfer_extract_snapshot_path "$l_final_snapshot")
	l_last_common_path=$(zxfer_extract_snapshot_path "$g_last_common_snap")

	# When there is nothing new to send, there is no need to roll the
	# destination back after deleting extra snapshots.
	if [ -n "$l_last_common_path" ] && [ "$l_last_common_path" = "$l_final_snapshot_path" ]; then
		zxfer_echoV "No new snapshots to copy for $g_actual_dest."
		return
	fi

	zxfer_rollback_destination_to_last_common_snapshot
	zxfer_seed_destination_for_snapshot_transfer "$l_first_snapshot" "$l_first_snapshot_path"

	# A destination bootstrap/seed can fully satisfy the transfer when there is
	# only one source snapshot. Do not attempt an incremental send from a
	# snapshot to itself.
	if zxfer_snapshot_transfer_is_complete "$l_final_snapshot_path"; then
		return
	fi

	zxfer_send_snapshot_transfer_range "$l_final_snapshot_path"
}

# Purpose: Recheck live destination snapshot state and reconcile delete or seed
# decisions before the next transfer step runs.
# Usage: Called during top-level dataset iteration and replication
# orchestration after property work and before send/receive so zxfer acts on
# current destination state instead of stale planning data.
#
# When running with -Y, the cached destination snapshot list can briefly lag a
# deletion from the previous iteration. Before reseeding an existing dataset,
# verify whether the destination already has a common snapshot so we do not try
# to receive a full stream into an existing filesystem.
zxfer_reconcile_live_destination_snapshot_state() {
	if ! l_snapshot_transfer_bounds=$(zxfer_get_snapshot_transfer_bounds); then
		return 0
	fi
	{
		IFS= read -r l_first_snapshot
		IFS= read -r l_final_snapshot
	} <<-EOF
		$l_snapshot_transfer_bounds
	EOF
	l_final_snapshot_path=$(zxfer_extract_snapshot_path "$l_final_snapshot")
	l_last_common_path=$(zxfer_extract_snapshot_path "$g_last_common_snap")

	# Re-check the live destination whenever the cached plan still intends to
	# send snapshots. The staged recursive discovery results can lag behind the
	# actual destination tip, especially on long-running or remote runs, and we
	# want to collapse the transfer list back to the newest live common snapshot
	# before deciding whether any send is still necessary.
	if [ -n "$l_last_common_path" ] && [ "$l_last_common_path" = "$l_final_snapshot_path" ]; then
		return 0
	fi

	if ! l_dest_exists=$(zxfer_exists_destination "$g_actual_dest"); then
		zxfer_throw_error "$l_dest_exists"
	fi

	# When snapshot discovery already proved the initial destination subtree is
	# absent, do not insist on a second live existence probe before the first
	# root bootstrap send. illumos/OpenZFS can surface that second probe as a
	# bare failure even though the cached "missing" result is authoritative for
	# the initial root dataset. Child datasets still recheck live state because a
	# recursive parent receive may have created them earlier in the iteration.
	if [ "$l_dest_exists" -eq 0 ] && zxfer_current_destination_is_initial_source_dataset; then
		return
	fi

	if [ "$l_dest_exists" -eq 0 ]; then
		if ! l_dest_exists=$(zxfer_exists_destination "$g_actual_dest" live); then
			zxfer_throw_error "$l_dest_exists"
		fi
	fi
	[ "$l_dest_exists" -eq 1 ] || return

	if ! l_live_dest_snaps=$(zxfer_get_live_destination_snapshots 2>&1); then
		zxfer_throw_error "Failed to retrieve live destination snapshots for [$g_actual_dest]: $l_live_dest_snaps"
	fi
	if [ -z "$l_live_dest_snaps" ]; then
		g_dest_has_snapshots=0
		return 0
	fi
	g_dest_has_snapshots=1

	# Build a destination identity set once, then scan the source records in
	# order so we keep the newest matching snapshot and the remaining tail after
	# it without repeated large shell-string membership checks.
	l_reconcile_source_records=$g_src_snapshot_transfer_list
	if zxfer_snapshot_record_lists_share_snapshot_name "$g_src_snapshot_transfer_list" "$l_live_dest_snaps"; then
		if ! zxfer_snapshot_record_list_contains_guid "$g_src_snapshot_transfer_list"; then
			l_reconcile_source_dataset=""
			while IFS= read -r l_reconcile_source_record; do
				[ -n "$l_reconcile_source_record" ] || continue
				l_reconcile_source_dataset=$(zxfer_extract_snapshot_dataset "$l_reconcile_source_record")
				[ -n "$l_reconcile_source_dataset" ] && break
			done <<-EOF
				$(zxfer_normalize_snapshot_record_list "$g_src_snapshot_transfer_list")
			EOF

			if [ -n "$l_reconcile_source_dataset" ]; then
				if ! l_reconcile_source_records=$(zxfer_get_snapshot_identity_records_for_dataset source "$l_reconcile_source_dataset" "$g_src_snapshot_transfer_list"); then
					zxfer_throw_error "Failed to retrieve source snapshot identities for [$l_reconcile_source_dataset]."
				fi
			fi
		fi
	fi

	l_section_break="@@ZXFER_SET_BREAK@@"
	l_reconcile_snapshot_awk=$(
		cat <<'EOF'
BEGIN {
	in_source = 0
	found_common = 0
	remaining_count = 0
}
$0 == section_break {
	in_source = 1
	next
}
!in_source {
	if ($0 != "")
		dest_identities[$0] = 1
	next
}
$0 != "" {
	record = $0
	tab_pos = index(record, "\t")
	snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
	snapshot_guid = (tab_pos > 0 ? substr(record, tab_pos + 1) : "")
	at_pos = index(snapshot_path, "@")
	if (at_pos <= 0)
		next
	snapshot_identity = substr(snapshot_path, at_pos + 1)
	if (snapshot_guid != "")
		snapshot_identity = snapshot_identity "\t" snapshot_guid
	if (snapshot_identity in dest_identities) {
		common = record
		found_common = 1
		remaining_count = 0
		next
	}
	if (found_common)
		remaining[++remaining_count] = record
}
END {
	if (common != "")
		print common
	for (i = 1; i <= remaining_count; i++)
		print remaining[i]
}
EOF
	)
	l_reconcile_result=$(
		{
			while IFS= read -r l_live_dest_snap; do
				[ -n "$l_live_dest_snap" ] || continue
				l_live_dest_identity=$(zxfer_extract_snapshot_identity "$l_live_dest_snap")
				[ -n "$l_live_dest_identity" ] || continue
				printf '%s\n' "$l_live_dest_identity"
			done <<-EOF
				$(zxfer_normalize_snapshot_record_list "$l_live_dest_snaps")
			EOF
			printf '%s\n' "$l_section_break"
			zxfer_normalize_snapshot_record_list "$l_reconcile_source_records"
		} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_reconcile_snapshot_awk"
	)

	l_confirmed_common_snap=""
	l_remaining_source_snaps=""
	l_is_first_result_line=1
	while IFS= read -r l_reconcile_line; do
		if [ "$l_is_first_result_line" -eq 1 ]; then
			l_confirmed_common_snap=$l_reconcile_line
			l_is_first_result_line=0
		elif [ -z "$l_remaining_source_snaps" ]; then
			l_remaining_source_snaps=$l_reconcile_line
		else
			l_remaining_source_snaps="$l_remaining_source_snaps
$l_reconcile_line"
		fi
	done <<-EOF
		$(printf '%s\n' "$l_reconcile_result")
	EOF

	[ -n "$l_confirmed_common_snap" ] || return

	g_dest_has_snapshots=1
	g_last_common_snap=$l_confirmed_common_snap
	g_src_snapshot_transfer_list=$l_remaining_source_snaps
	zxfer_echoV "Refreshed destination snapshot cache for $g_actual_dest using live snapshot state."
}

# Purpose: Stop migration-mode services and remember which ones need to be re-
# enabled later.
# Usage: Called during top-level dataset iteration and replication
# orchestration when `-m` prepares a source host for a controlled service
# migration.
#
# Stop a list of SMF services. The services are read in from stdin.
zxfer_stopsvcs() {
	zxfer_set_failure_stage "migration service handling"
	l_raw_services=$(cat)

	# Nothing to do if the caller provided an empty string.
	[ -n "$l_raw_services" ] || return

	l_normalized_services=$(zxfer_normalize_service_list "$l_raw_services")

	[ -n "$l_normalized_services" ] || return

	while IFS= read -r service; do
		zxfer_echov "Disabling service $service."
		svcadm disable -st "$service" ||
			{
				zxfer_relaunch
				zxfer_throw_error "Could not disable service $service."
			}
		g_zxfer_services_to_restart="$g_zxfer_services_to_restart $service"
		g_services_need_relaunch=1
	done <<EOF
$l_normalized_services
EOF
}

# Purpose: Normalize the service list into the stable form used across zxfer.
# Usage: Called during top-level dataset iteration and replication
# orchestration before comparison, caching, or reporting depends on exact
# formatting.
zxfer_normalize_service_list() {
	l_raw_services=$1
	[ -n "$l_raw_services" ] || return

	printf '%s\n' "$l_raw_services" | awk '
{
	for (i = 1; i <= NF; i++)
		print $i
}'
}

# Purpose: Preview the service disable commands without performing the live
# change.
# Usage: Called during top-level dataset iteration and replication
# orchestration on dry-run paths where zxfer still needs the exact command or
# action shape.
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

# Purpose: Record the services for relaunch for later diagnostics or control
# decisions.
# Usage: Called during top-level dataset iteration and replication
# orchestration when zxfer needs the state preserved for follow-on helpers or
# reporting.
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

# Purpose: Re-enable the services that migration mode previously stopped for
# this run.
# Usage: Called during top-level dataset iteration and replication
# orchestration after replication work finishes or abort paths need to restore
# service state.
#
# Relaunch a list of stopped services
zxfer_relaunch() {
	zxfer_set_failure_stage "migration service handling"
	[ -z "$g_zxfer_services_to_restart" ] && {
		g_services_need_relaunch=0
		g_services_relaunch_in_progress=0
		return
	}

	g_services_relaunch_in_progress=1
	l_failed_services=""
	l_failed_count=0

	for l_i in $g_zxfer_services_to_restart; do
		zxfer_echov "Restarting service $l_i"
		if [ "$g_option_n_dryrun" -eq 1 ]; then
			zxfer_echov "Dry run: $(zxfer_build_shell_command_from_argv svcadm enable "$l_i")"
			continue
		fi
		if ! svcadm enable "$l_i"; then
			l_failed_count=$((l_failed_count + 1))
			if [ -z "$l_failed_services" ]; then
				l_failed_services=$l_i
			else
				l_failed_services="$l_failed_services $l_i"
			fi
		fi
	done

	if [ "$l_failed_count" -gt 0 ]; then
		g_zxfer_services_to_restart=$l_failed_services
		g_services_need_relaunch=1
		if [ "$l_failed_count" -eq 1 ]; then
			zxfer_throw_error "Couldn't re-enable service $l_failed_services."
		fi
		zxfer_throw_error "Couldn't re-enable services: $l_failed_services."
	fi

	g_zxfer_services_to_restart=""
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0
}

# Purpose: Create the new snapshot requested by the current run before
# replication uses it as a transfer source.
# Usage: Called during top-level dataset iteration and replication
# orchestration when `-s` or migration mode requires a fresh source snapshot.
#
# Create a new recursive snapshot.
zxfer_newsnap() {
	l_initial_source=$1

	# We snapshot from the base of the initial source
	# Extract the filesystem name from the initial source snapshot by removing the '@' and everything after it
	l_sourcefs="${l_initial_source%@*}"

	l_snap=$g_zxfer_new_snapshot_name

	if [ "$g_option_R_recursive" != "" ]; then
		zxfer_echov "Creating recursive snapshot $l_sourcefs@$l_snap."
		cmd=$(zxfer_render_source_zfs_command snapshot -r "$l_sourcefs@$l_snap")
	else
		zxfer_echov "Creating snapshot $l_sourcefs@$l_snap."
		cmd=$(zxfer_render_source_zfs_command snapshot "$l_sourcefs@$l_snap")
	fi

	zxfer_record_last_command_string "$cmd"
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $cmd"
		return
	fi

	zxfer_echov "$cmd"
	if [ "$g_option_R_recursive" != "" ]; then
		zxfer_run_source_zfs_cmd snapshot -r "$l_sourcefs@$l_snap" || zxfer_throw_error "Error when executing command."
	else
		zxfer_run_source_zfs_cmd snapshot "$l_sourcefs@$l_snap" || zxfer_throw_error "Error when executing command."
	fi
}

# Purpose: Check the snapshot using the fail-closed rules owned by this module.
# Usage: Called during top-level dataset iteration and replication
# orchestration before later helpers act on a result that must be validated
# first.
#
# Tests to see if they are trying to sync a snapshots; exit if so
zxfer_check_snapshot() {
	l_initial_source=$1

	l_initial_sourcesnap=$(zxfer_extract_snapshot_name "$l_initial_source")

	# When using -s or -m, we don't want the source to be a snapshot.
	[ -n "$l_initial_sourcesnap" ] && zxfer_throw_error "Snapshots are not allowed as a source."
}

# Purpose: Check whether the property pass is required.
# Usage: Called during top-level dataset iteration and replication
# orchestration when later helpers need a boolean answer about the property
# pass.
zxfer_property_pass_is_required() {
	[ "$g_option_P_transfer_property" -eq 1 ] || [ "$g_option_o_override_property" != "" ]
}

# Purpose: Sort replication iteration work breadth-first by dataset ancestry.
# Usage: Called while building the dataset iteration list so recursive parent
# receives are scheduled before descendants, without clustering each child
# immediately behind its parent when unrelated same-depth work can run first.
zxfer_sort_replication_iteration_file() {
	l_input_file=$1
	l_output_file=$2
	l_scratch_file=$3

	# shellcheck disable=SC2016
	l_depth_prefix_awk='
{
	depth = gsub("/", "/")
	printf "%06d\t%s\n", depth, $0
}'
	"${g_cmd_awk:-awk}" "$l_depth_prefix_awk" "$l_input_file" >"$l_scratch_file" || return $?
	sort -u "$l_scratch_file" >"$l_output_file" || return $?

	# shellcheck disable=SC2016
	l_strip_prefix_awk='
{
	tab = index($0, "\t")
	if (tab <= 0)
		exit 1
	print substr($0, tab + 1)
}'
	"${g_cmd_awk:-awk}" "$l_strip_prefix_awk" "$l_output_file" >"$l_scratch_file" || return $?
	cat "$l_scratch_file" >"$l_output_file" || return $?
}

# Purpose: Build the replication iteration list for the next execution or
# comparison step.
# Usage: Called during top-level dataset iteration and replication
# orchestration before other helpers consume the assembled value.
zxfer_build_replication_iteration_list() {
	l_property_pass_required=$1
	g_zxfer_replication_iteration_list_result=""

	if ! zxfer_get_temp_file >/dev/null; then
		return 1
	fi
	l_iteration_input_file=$g_zxfer_temp_file_result
	if ! zxfer_get_temp_file >/dev/null; then
		zxfer_cleanup_runtime_artifact_path "$l_iteration_input_file"
		return 1
	fi
	l_iteration_filtered_file=$g_zxfer_temp_file_result
	if ! zxfer_get_temp_file >/dev/null; then
		zxfer_cleanup_runtime_artifact_paths "$l_iteration_input_file" "$l_iteration_filtered_file"
		return 1
	fi
	l_iteration_sorted_file=$g_zxfer_temp_file_result

	if ! zxfer_write_runtime_artifact_file "$l_iteration_input_file" ""; then
		zxfer_cleanup_runtime_artifact_paths \
			"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
		return 1
	fi

	if ! printf '%s\n' "$g_recursive_source_list" >>"$l_iteration_input_file"; then
		zxfer_cleanup_runtime_artifact_paths \
			"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
		return 1
	fi

	if [ "$g_option_R_recursive" != "" ] && [ "$l_property_pass_required" -eq 1 ]; then
		if ! printf '%s\n' "$g_recursive_source_dataset_list" >>"$l_iteration_input_file"; then
			zxfer_cleanup_runtime_artifact_paths \
				"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
			return 1
		fi
	fi

	if [ "$g_option_d_delete_destination_snapshots" -eq 1 ] &&
		[ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		if ! printf '%s\n' "$g_recursive_destination_extra_dataset_list" >>"$l_iteration_input_file"; then
			zxfer_cleanup_runtime_artifact_paths \
				"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
			return 1
		fi
	fi

	grep -v '^[[:space:]]*$' "$l_iteration_input_file" >"$l_iteration_filtered_file"
	l_filter_status=$?
	case "$l_filter_status" in
	0 | 1) ;;
	*)
		zxfer_cleanup_runtime_artifact_paths \
			"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
		return "$l_filter_status"
		;;
	esac

	zxfer_sort_replication_iteration_file \
		"$l_iteration_filtered_file" \
		"$l_iteration_sorted_file" \
		"$l_iteration_input_file"
	l_sort_status=$?
	if [ "$l_sort_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths \
			"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
		return "$l_sort_status"
	fi

	zxfer_read_replication_stage_file "$l_iteration_sorted_file" >/dev/null
	l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths \
			"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
		return "$l_read_status"
	fi
	g_zxfer_replication_iteration_list_result=$g_zxfer_replication_file_read_result

	zxfer_cleanup_runtime_artifact_paths \
		"$l_iteration_input_file" "$l_iteration_filtered_file" "$l_iteration_sorted_file"
	return 0
}

# Purpose: Append the post seed property source to the module-owned
# accumulator.
# Usage: Called during top-level dataset iteration and replication
# orchestration when later helpers need one shared place to extend staged or
# in-memory state.
zxfer_append_post_seed_property_source() {
	l_post_seed_property_sources_file=$1
	l_source=$2

	[ -n "$l_post_seed_property_sources_file" ] || return 0
	printf '%s\n' "$l_source" >>"$l_post_seed_property_sources_file"
}

# Purpose: Collect the post seed property sources into the module-owned format
# used by later steps.
# Usage: Called during top-level dataset iteration and replication
# orchestration before reconciliation or apply logic consumes the combined
# result.
zxfer_collect_post_seed_property_sources() {
	l_post_seed_property_sources_file=$1

	g_zxfer_post_seed_property_sources_result=""
	[ -n "$l_post_seed_property_sources_file" ] || return 0
	[ -s "$l_post_seed_property_sources_file" ] || return 0

	if ! zxfer_get_temp_file >/dev/null; then
		return 1
	fi
	l_filtered_sources_file=$g_zxfer_temp_file_result
	if ! zxfer_get_temp_file >/dev/null; then
		zxfer_cleanup_runtime_artifact_path "$l_filtered_sources_file"
		return 1
	fi
	l_sorted_sources_file=$g_zxfer_temp_file_result

	grep -v '^[[:space:]]*$' "$l_post_seed_property_sources_file" >"$l_filtered_sources_file"
	l_filter_status=$?
	case "$l_filter_status" in
	0 | 1) ;;
	*)
		zxfer_cleanup_runtime_artifact_paths "$l_filtered_sources_file" "$l_sorted_sources_file"
		return "$l_filter_status"
		;;
	esac

	sort -u "$l_filtered_sources_file" >"$l_sorted_sources_file"
	l_sort_status=$?
	if [ "$l_sort_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_filtered_sources_file" "$l_sorted_sources_file"
		return "$l_sort_status"
	fi

	zxfer_read_replication_stage_file "$l_sorted_sources_file" >/dev/null
	l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_filtered_sources_file" "$l_sorted_sources_file"
		return "$l_read_status"
	fi
	g_zxfer_post_seed_property_sources_result=$g_zxfer_replication_file_read_result

	zxfer_cleanup_runtime_artifact_paths "$l_filtered_sources_file" "$l_sorted_sources_file"
	return 0
}

# Purpose: Process the source dataset inside the main replication flow.
# Usage: Called during top-level dataset iteration and replication
# orchestration when the surrounding orchestration has selected one work item
# for detailed handling.
zxfer_process_source_dataset() {
	l_source=$1
	l_property_pass_required=$2
	l_post_seed_property_sources_file=${3:-}

	zxfer_set_actual_dest "$l_source"
	if [ -n "${g_zfs_send_job_pids:-}" ]; then
		zxfer_reset_destination_property_iteration_cache
	fi
	# Reset per-dataset state derived from zxfer_transfer_properties().
	# shellcheck disable=SC2034
	g_dest_created_by_zxfer=0

	zxfer_inspect_delete_snap "$g_option_d_delete_destination_snapshots" "$l_source"

	if [ "$l_property_pass_required" -eq 1 ]; then
		zxfer_transfer_properties "$l_source"
	fi

	zxfer_copy_snapshots

	if [ "$l_property_pass_required" -eq 1 ] &&
		[ -z "${g_zfs_send_job_pids:-}" ] &&
		[ "${g_dest_seed_requires_property_reconcile:-0}" -eq 0 ]; then
		zxfer_flush_captured_backup_metadata_if_live
	fi

	if [ "$l_property_pass_required" -eq 1 ] &&
		[ "${g_dest_seed_requires_property_reconcile:-0}" -eq 1 ] &&
		[ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_defer_buffered_backup_metadata_record "$l_source" "$g_actual_dest" "$g_zxfer_source_pvs_raw"
		zxfer_note_destination_dataset_exists "$g_actual_dest"
		if ! zxfer_append_post_seed_property_source "$l_post_seed_property_sources_file" "$l_source"; then
			zxfer_throw_error "Failed to queue post-seed property reconcile source [$l_source]."
		fi
	fi
}

# Purpose: Run the post seed property reconcile through the controlled
# execution path owned by this module.
# Usage: Called during top-level dataset iteration and replication
# orchestration once planning is complete and zxfer is ready to execute the
# action.
zxfer_run_post_seed_property_reconcile() {
	l_post_seed_property_sources=$1

	[ -n "$l_post_seed_property_sources" ] || return

	zxfer_reset_destination_property_iteration_cache
	while IFS= read -r l_source; do
		[ -n "$l_source" ] || continue
		zxfer_set_actual_dest "$l_source"
		zxfer_transfer_properties "$l_source" 1
		zxfer_finalize_deferred_backup_metadata_record "$l_source" "$g_actual_dest" "$g_zxfer_source_pvs_raw"
	done <<-EOF
		$l_post_seed_property_sources
	EOF
}

# Purpose: Copy the filesystems through the replication path owned by this
# module.
# Usage: Called during top-level dataset iteration and replication
# orchestration after discovery and reconciliation have produced the exact work
# list.
#
# main loop that copies the filesystems
zxfer_copy_filesystems() {
	zxfer_echoV "Begin zxfer_copy_filesystems()"

	l_property_pass_required=0
	if zxfer_property_pass_is_required; then
		l_property_pass_required=1
	fi
	if ! zxfer_build_replication_iteration_list "$l_property_pass_required"; then
		zxfer_throw_error "Failed to prepare replication dataset iteration list."
	fi
	l_iteration_list=$g_zxfer_replication_iteration_list_result
	if ! zxfer_get_temp_file >/dev/null; then
		zxfer_throw_error "Error creating temporary file."
	fi
	l_post_seed_property_sources_file=$g_zxfer_temp_file_result
	if ! zxfer_write_runtime_artifact_file "$l_post_seed_property_sources_file" ""; then
		zxfer_cleanup_runtime_artifact_path "$l_post_seed_property_sources_file"
		zxfer_throw_error "Error creating temporary file."
	fi

	zxfer_refresh_property_tree_prefetch_context

	for l_source in $l_iteration_list; do
		zxfer_process_source_dataset "$l_source" "$l_property_pass_required" "$l_post_seed_property_sources_file"
	done

	l_has_post_seed_property_sources=0
	if [ -s "$l_post_seed_property_sources_file" ]; then
		l_has_post_seed_property_sources=1
	fi

	l_had_pending_send_jobs=0
	[ -n "${g_zfs_send_job_pids:-}" ] && l_had_pending_send_jobs=1
	zxfer_wait_for_zfs_send_jobs "final sync"

	if [ "$l_property_pass_required" -eq 1 ] &&
		[ "$g_option_n_dryrun" -eq 0 ] &&
		[ "$l_has_post_seed_property_sources" -eq 1 ]; then
		if ! zxfer_collect_post_seed_property_sources "$l_post_seed_property_sources_file"; then
			zxfer_cleanup_runtime_artifact_path "$l_post_seed_property_sources_file"
			zxfer_throw_error "Failed to prepare post-seed property reconcile source queue."
		fi
		l_post_seed_property_sources=$g_zxfer_post_seed_property_sources_result
		zxfer_run_post_seed_property_reconcile "$l_post_seed_property_sources"
	fi

	if [ "$l_property_pass_required" -eq 1 ] &&
		{ [ "$l_had_pending_send_jobs" -eq 1 ] || [ "$l_has_post_seed_property_sources" -eq 1 ]; }; then
		zxfer_flush_captured_backup_metadata_if_live
	fi

	zxfer_cleanup_runtime_artifact_path "$l_post_seed_property_sources_file"
	zxfer_echoV "End zxfer_copy_filesystems()"
}

# Purpose: Resolve the effective initial source from options that zxfer should
# use.
# Usage: Called during top-level dataset iteration and replication
# orchestration after configuration, cache state, or remote state can change
# the final choice.
zxfer_resolve_initial_source_from_options() {
	if [ "$g_option_R_recursive" != "" ] && [ "$g_option_N_nonrecursive" != "" ]; then
		zxfer_throw_usage_error "You must choose either -N to transfer a single filesystem or -R to transfer \
a single filesystem and its children recursively, but not both -N and -R at the same time."
	elif [ "$g_option_R_recursive" != "" ]; then
		g_initial_source="$g_option_R_recursive"
	elif [ "$g_option_N_nonrecursive" != "" ]; then
		g_initial_source="$g_option_N_nonrecursive"
	else
		zxfer_throw_usage_error "You must specify a source with either -N or -R."
	fi
}

# Purpose: Normalize the source destination paths into the stable form used
# across zxfer.
# Usage: Called during top-level dataset iteration and replication
# orchestration before comparison, caching, or reporting depends on exact
# formatting.
zxfer_normalize_source_destination_paths() {
	# Record whether the user supplied a trailing slash before normalizing the path.
	g_initial_source_had_trailing_slash=$(echo "$g_initial_source" | grep -c '..*/$')

	# Now that we know whether there was a trailing slash on the source, no
	# need to confuse things by keeping it on there. Get rid of it.
	g_initial_source=$(zxfer_strip_trailing_slashes "$g_initial_source")

	# Source and destination can't start with "/", but it's an easy mistake to make
	if [ "$(echo "$g_initial_source" | grep -c '^/')" -eq "1" ] ||
		[ "$(echo "$g_destination" | grep -c '^/')" -eq "1" ]; then
		zxfer_throw_usage_error "Source and destination must not begin with \"/\". Note the example."
	fi

	# Trailing slashes on the destination are meaningless for dataset names but
	# make later concatenation produce an illegal double slash, so normalize it
	# once up front.
	g_destination=$(zxfer_strip_trailing_slashes "$g_destination")
	zxfer_set_failure_roots "$g_initial_source" "$g_destination"
}

# Purpose: Validate the ZFS mode preconditions before zxfer relies on it.
# Usage: Called during top-level dataset iteration and replication
# orchestration to fail closed on malformed, unsafe, or stale input.
zxfer_validate_zfs_mode_preconditions() {
	zxfer_echoV "Checking source snapshot."
	zxfer_check_snapshot "$g_initial_source"

	# When using -c you must use -m as well rule. This forces the user
	# To think twice if they really mean to do the migration.
	[ -n "$g_option_c_services" ] && [ "$g_option_m_migrate" -eq 0 ] &&
		zxfer_throw_error "When using -c, -m needs to be specified as well."

	if [ -n "$g_option_c_services" ] && ! command -v svcadm >/dev/null 2>&1; then
		zxfer_throw_usage_error "The -c service-management option requires Solaris/illumos SMF (svcadm)."
	fi
}

# Purpose: Check the backup storage directory if needed using the fail-closed
# rules owned by this module.
# Usage: Called during top-level dataset iteration and replication
# orchestration before later helpers act on a result that must be validated
# first.
zxfer_check_backup_storage_dir_if_needed() {
	[ "$g_option_k_backup_property_mode" -eq 1 ] || return

	zxfer_refresh_backup_storage_root

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		l_backup_dir_cmd=$(zxfer_build_shell_command_from_argv mkdir -p "$g_backup_storage_root")
		l_backup_dir_mode_cmd=$(zxfer_build_shell_command_from_argv chmod 700 "$g_backup_storage_root")
		if [ "$g_option_T_target_host" = "" ]; then
			zxfer_echov "Dry run: umask 077; $l_backup_dir_cmd; $l_backup_dir_mode_cmd"
		else
			l_remote_backup_dir_cmd=$(zxfer_build_remote_backup_dir_prepare_cmd "$g_backup_storage_root" "$g_option_T_target_host" 99)
			l_remote_backup_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_backup_dir_cmd")
			l_remote_backup_display_cmd=$(zxfer_build_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_backup_shell_cmd")
			zxfer_echov "Dry run: $l_remote_backup_display_cmd"
		fi
		return
	fi

	# Validate or create the backup directory before any replication work so we
	# fail closed on unsafe paths (e.g., symlinks) instead of performing ZFS
	# operations first.
	if [ "$g_option_T_target_host" = "" ]; then
		zxfer_ensure_local_backup_dir "$g_backup_storage_root"
	else
		zxfer_ensure_remote_backup_dir "$g_backup_storage_root" "$g_option_T_target_host" destination
	fi
}

# Purpose: Update the recursive source list if needed to reflect the latest
# module state.
# Usage: Called during top-level dataset iteration and replication
# orchestration after upstream inputs or staged data change.
zxfer_update_recursive_source_list_if_needed() {
	if [ "$g_option_R_recursive" = "" ]; then
		g_recursive_source_list=$g_initial_source
	fi
}

# Purpose: Initialize the replication context before later helpers depend on
# it.
# Usage: Called during top-level dataset iteration and replication
# orchestration during bootstrap so downstream code sees consistent defaults
# and runtime state.
zxfer_initialize_replication_context() {
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: skipping live backup-restore validation, snapshot discovery, and unsupported-property detection."
		zxfer_seed_dry_run_preview_source_list
		return
	fi

	# Fail fast when restoring properties from backup metadata so we do not
	# attempt destination inspections before confirming the backup exists.
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		zxfer_get_backup_properties
	fi

	# Caches all the zfs list calls, gets the recursive list, and gives
	# an opportunity to exit if the source is not present
	zxfer_get_zfs_list

	if [ "$g_option_U_skip_unsupported_properties" -eq 1 ]; then
		zxfer_calculate_unsupported_properties
	fi

	zxfer_update_recursive_source_list_if_needed
}

# Purpose: Refresh the dataset iteration state from the current configuration
# and runtime state.
# Usage: Called during top-level dataset iteration and replication
# orchestration after inputs change and downstream helpers need the derived
# value rebuilt.
zxfer_refresh_dataset_iteration_state() {
	zxfer_get_zfs_list
	zxfer_update_recursive_source_list_if_needed
	zxfer_refresh_property_tree_prefetch_context
}

# Purpose: Run the optional capture preflight snapshot step only when the
# current state requires it.
# Usage: Called during top-level dataset iteration and replication
# orchestration to keep the optional branch in one place instead of scattering
# the condition across callers.
zxfer_maybe_capture_preflight_snapshot() {
	#
	# If using -s, do a new recursive snapshot, then copy all new snapshots too.
	#
	if [ "$g_option_s_make_snapshot" -eq 0 ] || [ "$g_option_m_migrate" -eq 1 ]; then
		return
	fi

	# Create the new snapshot with a unique name.
	zxfer_newsnap "$g_initial_source"

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		return
	fi

	# Because there are new snapshots, need to refresh the cached lists.
	zxfer_refresh_dataset_iteration_state
}

# Purpose: Preview the migration services dry run without performing the live
# change.
# Usage: Called during top-level dataset iteration and replication
# orchestration on dry-run paths where zxfer still needs the exact command or
# action shape.
zxfer_preview_migration_services_dry_run() {
	if [ -n "$g_option_c_services" ]; then
		zxfer_preview_service_disable_commands "$g_option_c_services"
		zxfer_record_services_for_relaunch "$g_option_c_services"
	fi

	for l_source in $g_recursive_source_list; do
		zxfer_echov "Dry run: $(zxfer_render_source_zfs_command unmount "$l_source")"
	done

	zxfer_newsnap "$g_initial_source"
}

# Purpose: Prepare the migration services before the surrounding flow uses it.
# Usage: Called during top-level dataset iteration and replication
# orchestration once prerequisites are known but before live work depends on
# the prepared state.
zxfer_prepare_migration_services() {
	zxfer_set_failure_stage "migration service handling"
	[ "$g_option_m_migrate" -eq 1 ] || return

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_preview_migration_services_dry_run
		return
	fi

	# Check if any services need to be disabled before doing a migration.
	if [ -n "$g_option_c_services" ]; then
		zxfer_stopsvcs <<EOF
$g_option_c_services
EOF
	fi

	# Validate that each dataset is mounted before we attempt to unmount or snapshot.
	for l_source in $g_recursive_source_list; do
		if ! l_source_mounted=$(zxfer_run_source_zfs_cmd get -Ho value mounted "$l_source"); then
			zxfer_throw_error "Couldn't determine whether source $l_source is mounted."
		fi
		if [ "$l_source_mounted" != "yes" ]; then
			zxfer_throw_usage_error "The source filesystem is not mounted, cannot use -m."
		fi
	done

	for l_source in $g_recursive_source_list; do
		# Unmount the source filesystem before doing the last snapshot.
		zxfer_echov "Unmounting $l_source."
		if ! zxfer_run_source_zfs_cmd unmount "$l_source"; then
			zxfer_relaunch
			zxfer_throw_error "Couldn't unmount source $l_source."
		fi
	done

	# Create the new snapshot with a unique name.
	zxfer_newsnap "$g_initial_source"

	# Now we must make the script aware of the new snapshots in existence so
	# we can copy them over.
	zxfer_refresh_dataset_iteration_state
}

# Purpose: Seed the dry run preview source list so incremental work can
# continue from a valid base.
# Usage: Called during top-level dataset iteration and replication
# orchestration when zxfer must bootstrap a destination before sending the
# remaining range.
zxfer_seed_dry_run_preview_source_list() {
	if command -v zxfer_reset_snapshot_discovery_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_discovery_state
	fi
	if command -v zxfer_reset_destination_existence_cache >/dev/null 2>&1; then
		zxfer_reset_destination_existence_cache
	fi
	if command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1; then
		zxfer_reset_snapshot_record_indexes
	fi

	g_recursive_source_list=$g_initial_source
	g_recursive_source_dataset_list=$g_initial_source

	if [ "$g_option_R_recursive" != "" ]; then
		zxfer_echoV "Dry run: recursive descendant discovery is skipped; previewing only the explicitly requested source dataset."
	fi
}

# Purpose: Preview the ZFS mode dry run without performing the live change.
# Usage: Called during top-level dataset iteration and replication
# orchestration on dry-run paths where zxfer still needs the exact command or
# action shape.
zxfer_preview_zfs_mode_dry_run() {
	zxfer_seed_dry_run_preview_source_list
	zxfer_echoV "Dry run: skipping live replication-state validation and command planning."

	if [ "${g_option_e_restore_property_mode:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: skipping live backup-metadata restore validation."
	fi
	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: skipping live unsupported-property detection."
	fi
	if [ -n "${g_option_D_display_progress_bar:-}" ] &&
		zxfer_progress_dialog_uses_size_estimate; then
		zxfer_echoV "Dry run: skipping live %%size%% progress estimate discovery."
	fi

	zxfer_maybe_capture_preflight_snapshot
	zxfer_prepare_migration_services
	zxfer_echoV "Dry run: send/receive and property-reconcile commands require live snapshot discovery and are not rendered."
}

# Purpose: Perform the grandfather protection checks after planning and
# preconditions are satisfied.
# Usage: Called during top-level dataset iteration and replication
# orchestration when zxfer is ready to execute a grouped safety check or
# action.
zxfer_perform_grandfather_protection_checks() {
	[ "$g_option_g_grandfather_protection" != "" ] || return 0

	zxfer_echov "Checking grandfather status of all snapshots marked for deletion..."

	for l_source in $g_recursive_source_list; do
		zxfer_set_actual_dest "$l_source"
		# turn off delete so that we are only checking snapshots, pass 0
		zxfer_inspect_delete_snap 0 "$l_source"
	done
	zxfer_echov "Grandfather check passed."
}

# Purpose: Run one full live or dry-run replication pass for the current source
# and destination roots.
# Usage: Called during top-level dataset iteration and replication
# orchestration from the outer loop after startup, validation, and remote
# bootstrap are complete.
#
# Run one replication pass.
zxfer_run_zfs_mode() {
	zxfer_resolve_initial_source_from_options
	zxfer_normalize_source_destination_paths
	zxfer_validate_zfs_mode_preconditions
	zxfer_check_backup_storage_dir_if_needed

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_preview_zfs_mode_dry_run
		return
	fi

	zxfer_initialize_replication_context
	zxfer_maybe_capture_preflight_snapshot
	zxfer_prepare_migration_services
	zxfer_perform_grandfather_protection_checks

	zxfer_copy_filesystems

	if [ "$g_option_m_migrate" -eq 1 ]; then
		# Re-launch any stopped services.
		zxfer_relaunch
	fi
}

# Purpose: Repeat the top-level replication pass until zxfer converges or the
# configured iteration cap is reached.
# Usage: Called during top-level dataset iteration and replication
# orchestration as the final launcher entrypoint for the ZFS replication mode.
#
# Repeat replication passes until a pass performs no send/destroy work or the
# configured yield limit is reached.
zxfer_run_zfs_mode_loop() {
	l_num_iterations=0
	l_max_yield_iterations=$(zxfer_get_max_yield_iterations)

	while true; do
		# A pass sets this when it performs send/destroy work that may require
		# another replication iteration.
		g_is_performed_send_destroy=0

		zxfer_reset_property_iteration_caches

		l_num_iterations=$((l_num_iterations + 1))
		if [ "$g_option_Y_yield_iterations" -gt 1 ]; then
			zxfer_echov "Begin Iteration[$l_num_iterations of $g_option_Y_yield_iterations]. Running in zfs send/receive mode."
		fi

		zxfer_run_zfs_mode

		if [ "$g_option_Y_yield_iterations" -gt 1 ]; then
			zxfer_echov "End Iteration[$l_num_iterations of $g_option_Y_yield_iterations]."
		fi

		if [ "$g_is_performed_send_destroy" -eq 0 ]; then
			zxfer_echoV "Exiting loop. No send or destroy commands were performed during last iteration."
			break
		fi
		if [ "$l_num_iterations" -ge "$g_option_Y_yield_iterations" ]; then
			if [ "$g_option_Y_yield_iterations" -ge "$l_max_yield_iterations" ]; then
				zxfer_echoV "Exiting loop. Reached maximum number of iterations.
If consistently not completing replication in allotted iterations,
consider using compression, increasing bandwidth, increasing I/O or reducing snapshot frequency."
			fi
			break
		fi
	done
}
