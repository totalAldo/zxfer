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
# owns globals: the pass roots g_initial_source and
#   g_initial_source_had_trailing_slash, the current dataset g_actual_dest,
#   g_dest_seed_requires_property_reconcile, the post-seed accumulator
#   g_zxfer_post_seed_property_sources, g_zxfer_replication_iteration_list_result,
#   the run's -s/-m snapshot name g_zxfer_new_snapshot_name (stamped once by
#   zxfer_stamp_new_snapshot_name), and the per-pass mutation marker
#   g_is_performed_send_destroy (set by send/receive and snapshot destroy, read
#   by the -Y loop).
# reads globals: g_option_*, g_destination, discovery's recursive dataset
#   lists, and the snapshot plan: g_last_common_snap,
#   g_src_snapshot_transfer_list, g_dest_has_snapshots, the delete markers and
#   the -g pre-pass's g_zxfer_plan_delete_snapshots. The plan changes only
#   through zxfer_snapshot_plan.sh; a seed receive publishes its snapshot as
#   the new anchor with zxfer_publish_snapshot_transfer_plan.
# mutates caches: destination existence through the destination-state
#   helpers; the destination property iteration cache before the post-seed
#   pass.
# returns via stdout: none.

# Purpose: Reset the replication state for a new session.
# Usage: zxfer_reset_replication_runtime_state, from session initialization.
zxfer_reset_replication_runtime_state() {
	g_is_performed_send_destroy=0
	g_initial_source=""
	g_initial_source_had_trailing_slash=0
	g_actual_dest=""
	g_dest_seed_requires_property_reconcile=0
	g_zxfer_post_seed_property_sources=""
	g_zxfer_replication_iteration_list_result=""
	g_zxfer_new_snapshot_name=""
}

# Purpose: Make SOURCE the current dataset and map its destination.
# Usage: zxfer_set_actual_dest SOURCE; sets g_actual_dest and the failure
# report's dataset pair.
zxfer_set_actual_dest() {
	zxfer_map_destination_dataset "$1"
	g_actual_dest=$g_zxfer_destination_dataset_result
	zxfer_set_current_dataset_context "$1" "$g_actual_dest"
}

# Purpose: Roll the destination back to the last common snapshot after -d
# pruned snapshots newer than it.
# Usage: zxfer_rollback_destination_to_last_common_snapshot; acts only with
# -F, after a delete of snapshots newer than the anchor.
zxfer_rollback_destination_to_last_common_snapshot() {
	# Never roll back without an explicit -F; without it zxfer fails safe if
	# the destination head has diverged.
	[ "${g_option_F_force_rollback:-}" != "" ] || return 0
	[ "${g_did_delete_dest_snapshots:-0}" -eq 1 ] || return 0
	[ "${g_deleted_dest_newer_snapshots:-0}" -eq 1 ] || return 0

	zxfer_probe_destination_existence "$g_actual_dest" live ||
		zxfer_throw_error "$g_zxfer_destination_exists_error"
	[ "$g_zxfer_destination_exists_result" -eq 1 ] || return 0

	# The snapshot name is everything after the first "@" of the record path.
	l_rollback_path=${g_last_common_snap%%	*}
	case $l_rollback_path in
	*@?*) ;;
	*) return 0 ;;
	esac
	l_rollback_snapshot=$g_actual_dest@${l_rollback_path#*@}
	zxfer_echov "Rolling back $g_actual_dest to last common snapshot [$l_rollback_snapshot] after deletions."
	if ! zxfer_run_destination_zfs_cmd rollback -r "$l_rollback_snapshot"; then
		zxfer_throw_error "Failed to roll back destination [$g_actual_dest] to $l_rollback_snapshot after deleting snapshots."
	fi
}

# Purpose: Seed a missing or snapshot-less destination with the first pending
# snapshot.
# Usage: zxfer_seed_destination_for_snapshot_transfer FIRST_RECORD FIRST_PATH;
# refuses a full receive into a destination whose planned snapshots share no
# guid with the source.
zxfer_seed_destination_for_snapshot_transfer() {
	l_seed_record=$1
	l_seed_path=$2

	zxfer_probe_destination_existence "$g_actual_dest" ||
		zxfer_throw_error "$g_zxfer_destination_exists_error"
	if [ "$g_zxfer_destination_exists_result" -eq 0 ]; then
		# Live-probe a cached-missing dataset once more before choosing the
		# missing-dataset receive path: discovery may be minutes old.
		zxfer_probe_destination_existence "$g_actual_dest" live ||
			zxfer_throw_error "$g_zxfer_destination_exists_error"
	fi
	l_seed_dest_exists=$g_zxfer_destination_exists_result

	# The plan published g_dest_has_snapshots from discovery, or from the live
	# rows when this run destroyed some of the dataset's snapshots.
	if [ "$l_seed_dest_exists" -eq 1 ] &&
		[ "${g_last_common_snap:-}" = "" ] &&
		[ "$g_dest_has_snapshots" -eq 1 ]; then
		zxfer_throw_error "Destination dataset [$g_actual_dest] has snapshots but none share a common guid with the source. Refusing to perform a full receive into an existing snapshotted dataset."
	fi
	if [ "$l_seed_dest_exists" -eq 0 ]; then
		zxfer_echov "Destination dataset does not exist [$g_actual_dest]. Sending first snapshot [$l_seed_path]"
		zxfer_zfs_send_receive "" "$l_seed_path" "$g_actual_dest" "0"
	elif [ "$g_dest_has_snapshots" -eq 0 ]; then
		zxfer_echov "Destination dataset [$g_actual_dest] exists but has no snapshots. Seeding with [$l_seed_path]"
		zxfer_echov "Temporarily enabling receive-side -F to seed existing empty destination dataset [$g_actual_dest]."
		zxfer_zfs_send_receive "" "$l_seed_path" "$g_actual_dest" "0" "-F"
	else
		return 0
	fi
	# The received seed is the new common snapshot.
	g_dest_seed_requires_property_reconcile=1
	zxfer_publish_snapshot_transfer_plan "$l_seed_record" \
		"${g_src_snapshot_transfer_list:-}" 1
}

# Purpose: Send the current dataset's pending snapshots, seeding the
# destination first when needed.
# Usage: zxfer_copy_snapshots SOURCE, after zxfer_inspect_delete_snap
# published the plan for SOURCE and g_actual_dest.
zxfer_copy_snapshots() {
	g_dest_seed_requires_property_reconcile=0

	# A dataset whose snapshots -d just destroyed is re-planned from a live
	# listing; -Y repeats whole passes to converge under outside drift.
	zxfer_reconcile_live_destination_snapshot_state "$1"

	# One record per line, oldest first; drop stray blank edge lines once.
	l_copy_list=${g_src_snapshot_transfer_list:-}
	while :; do
		case $l_copy_list in
		"$ZXFER_LF"*) l_copy_list=${l_copy_list#"$ZXFER_LF"} ;;
		*"$ZXFER_LF") l_copy_list=${l_copy_list%"$ZXFER_LF"} ;;
		*) break ;;
		esac
	done
	if [ -z "$l_copy_list" ]; then
		zxfer_echoV "No snapshots to copy, skipping destination dataset: $g_actual_dest."
		return
	fi
	l_copy_first=${l_copy_list%%"$ZXFER_LF"*}
	l_copy_final=${l_copy_list##*"$ZXFER_LF"}
	# Record paths are everything before the first tab (guid column).
	l_copy_first_path=${l_copy_first%%	*}
	l_copy_final_path=${l_copy_final%%	*}

	# Nothing new to send: no rollback after deleting extra snapshots either.
	if [ "${g_last_common_snap%%	*}" = "$l_copy_final_path" ]; then
		zxfer_echoV "No new snapshots to copy for $g_actual_dest."
		return
	fi

	zxfer_rollback_destination_to_last_common_snapshot
	zxfer_seed_destination_for_snapshot_transfer "$l_copy_first" "$l_copy_first_path"

	# Seeding the only pending snapshot completes the transfer; never send a
	# snapshot incrementally to itself.
	if [ "${g_last_common_snap%%	*}" = "$l_copy_final_path" ]; then
		zxfer_echoV "Seed snapshot already matches final snapshot for $g_actual_dest."
		return
	fi

	zxfer_echoV "Final snapshot: $l_copy_final_path"
	# The rollback and seed steps can move g_last_common_snap, so read it here.
	zxfer_zfs_send_receive "${g_last_common_snap%%	*}" "$l_copy_final_path" "$g_actual_dest" "1"
}

# Purpose: Name the run's -s/-m snapshot zxfer_<pid>_<YYYYmmddHHMMSS> once.
# Usage: zxfer_stamp_new_snapshot_name; keeps an existing name and throws
# when date cannot stamp a new one.
zxfer_stamp_new_snapshot_name() {
	[ -z "${g_zxfer_new_snapshot_name:-}" ] || return 0
	if ! l_stamp_date=$(date +%Y%m%d%H%M%S) || ! zxfer_is_uint "$l_stamp_date"; then
		zxfer_throw_error "Failed to read the date for the -s/-m snapshot name."
	fi
	g_zxfer_new_snapshot_name=zxfer_$$_$l_stamp_date
}

# Purpose: Create the -s or -m snapshot of the source root.
# Usage: zxfer_newsnap SOURCE; recursive under -R, and only rendered under -n.
zxfer_newsnap() {
	zxfer_stamp_new_snapshot_name
	# Snapshot the dataset part of SOURCE.
	l_newsnap_snapshot=${1%@*}@$g_zxfer_new_snapshot_name

	if [ "$g_option_R_recursive" != "" ]; then
		zxfer_echov "Creating recursive snapshot $l_newsnap_snapshot."
		set -- snapshot -r "$l_newsnap_snapshot"
	else
		zxfer_echov "Creating snapshot $l_newsnap_snapshot."
		set -- snapshot "$l_newsnap_snapshot"
	fi

	l_cmd=""
	if zxfer_command_display_render_enabled; then
		l_cmd=$(zxfer_render_source_zfs_command "$@")
		zxfer_record_last_command_string "$l_cmd"
	else
		zxfer_record_last_command_opaque
	fi
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $l_cmd"
		return
	fi
	zxfer_echov "$l_cmd"
	zxfer_run_source_zfs_cmd "$@" || zxfer_throw_error "Error when executing command."
}

# Purpose: Check whether -U must probe destination property support.
# Usage: zxfer_unsupported_property_scan_is_required, after discovery; a clean
# recursive no-op without property work skips the probes.
zxfer_unsupported_property_scan_is_required() {
	[ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ] || return 1

	if [ "$g_option_P_transfer_property" -eq 1 ] ||
		[ "$g_option_o_override_property" != "" ]; then
		return 0
	fi
	if [ "${g_option_e_restore_property_mode:-0}" -eq 1 ] ||
		[ "${g_option_k_backup_property_mode:-0}" -eq 1 ]; then
		return 0
	fi
	if [ "${g_option_R_recursive:-}" = "" ]; then
		return 0
	fi
	[ -n "${g_recursive_source_list:-}" ] || return 1

	return 0
}

# Purpose: Merge dataset work once, keeping parents ahead of descendants and
# independent siblings ready for -j. Source and destination delta lists may
# overlap; a destination-only snapshot can belong to an existing source parent.
# Usage: zxfer_build_replication_iteration_list PROPERTY_PASS_REQUIRED;
# publishes $g_zxfer_replication_iteration_list_result, one
# "POSITION<TAB>SOURCE" row per dataset, only after the snapshot record files
# are split by position, so every list comes with its own slices.
zxfer_build_replication_iteration_list() {
	l_iteration_sources=${g_recursive_source_list:-}
	if [ "$g_option_R_recursive" != "" ] && [ "$1" -eq 1 ]; then
		l_iteration_sources="${g_recursive_source_dataset_list:-}
$l_iteration_sources"
	fi
	if [ "$g_option_d_delete_destination_snapshots" -eq 1 ]; then
		l_iteration_sources="$l_iteration_sources
${g_recursive_destination_extra_dataset_list:-}"
	fi
	g_zxfer_replication_iteration_list_result=""
	# One pass deduplicates into depth buckets. This avoids both per-dataset
	# shell scans of the growing list and the former three staged files.
	# shellcheck disable=SC2016 # awk must receive literal record fields.
	l_iteration_result=$(
		"${g_cmd_awk:-awk}" '
		NF && !seen[$0]++ {
			depth = gsub("/", "/")
			rows[depth, ++count[depth]] = $0
			if (depth > max_depth) max_depth = depth
		}
		END {
			for (depth = 0; depth <= max_depth; depth++)
				for (row = 1; row <= count[depth]; row++)
					print ++position "\t" rows[depth, row]
		}
	' <<EOF
$l_iteration_sources
EOF
	) || return "$?"
	zxfer_split_snapshot_records "$l_iteration_result" || return "$?"
	g_zxfer_replication_iteration_list_result=$l_iteration_result
}

# Purpose: Replicate one dataset: plan and -d delete, reconcile properties,
# then send.
# Usage: zxfer_process_source_dataset SOURCE PROPERTY_PASS(0|1) [POSITION];
# POSITION, SOURCE's iteration-list position, selects its snapshot slices.
# Appends a seeded SOURCE to g_zxfer_post_seed_property_sources.
zxfer_process_source_dataset() {
	l_process_source=$1
	l_process_property_pass=$2

	zxfer_set_actual_dest "$l_process_source"
	zxfer_select_snapshot_slice "${3:-}" "$l_process_source"
	# In-flight background receives cannot affect this dataset's cached
	# destination state: the ready-queue ancestry gate defers any dataset
	# whose destination conflicts with an active job, zxfer_reap_send_job
	# invalidates a completed job's own subtree, and the seed live-probes a
	# destination the cache calls missing before a full receive.
	zxfer_inspect_delete_snap "$g_option_d_delete_destination_snapshots" \
		"$l_process_source"

	if [ "$l_process_property_pass" -eq 1 ]; then
		zxfer_transfer_properties "$l_process_source"
	fi

	zxfer_copy_snapshots "$l_process_source"

	# A seed receive creates the dataset without its properties: reconcile it
	# once more after the queue drains. -k rows stay buffered until then.
	if [ "$l_process_property_pass" -eq 1 ] &&
		[ "${g_dest_seed_requires_property_reconcile:-0}" -eq 1 ]; then
		zxfer_note_destination_dataset_exists "$g_actual_dest"
		g_zxfer_post_seed_property_sources=${g_zxfer_post_seed_property_sources:+$g_zxfer_post_seed_property_sources$ZXFER_LF}$l_process_source
	fi
}

# Purpose: Process replication datasets in order; with -j above 1 this is a
# dependency-aware ready queue so destination descendants blocked by active
# parent receives do not stop later independent datasets from starting.
# Usage: zxfer_process_replication_ready_queue PENDING_ROWS
# PROPERTY_PASS_REQUIRED, with the "POSITION<TAB>SOURCE" rows of
# zxfer_build_replication_iteration_list; a deferred row keeps its position.
zxfer_process_replication_ready_queue() {
	l_queue_pending=$1
	l_queue_property_pass=$2
	l_queue_job_limit=${g_option_j_jobs:-1}
	l_queue_processed=0
	l_queue_waits=0

	while [ -n "$l_queue_pending" ]; do
		l_queue_next=""
		l_queue_progress=0
		while IFS= read -r l_queue_row; do
			[ -n "$l_queue_row" ] || continue
			l_queue_source=${l_queue_row#*"$ZXFER_TAB"}
			if [ "$l_queue_job_limit" -gt 1 ] && [ -n "${g_zxfer_send_jobs:-}" ]; then
				zxfer_map_destination_dataset "$l_queue_source"
				if [ "${g_count_zfs_send_jobs:-0}" -ge "$l_queue_job_limit" ] ||
					zxfer_send_job_conflicts_with_destination \
						"$g_zxfer_destination_dataset_result"; then
					l_queue_next=${l_queue_next:+$l_queue_next$ZXFER_LF}$l_queue_row
					continue
				fi
			fi
			# ssh inside the dataset's work must not read the queue.
			zxfer_process_source_dataset "$l_queue_source" "$l_queue_property_pass" \
				"${l_queue_row%%"$ZXFER_TAB"*}" </dev/null
			l_queue_progress=1
			l_queue_processed=$((l_queue_processed + 1))
		done <<EOF
$l_queue_pending
EOF
		l_queue_pending=$l_queue_next
		[ -n "$l_queue_pending" ] || break
		[ "$l_queue_progress" -eq 0 ] || continue

		[ -n "${g_zxfer_send_jobs:-}" ] ||
			zxfer_throw_error "Failed to select a ready replication dataset while no send/receive jobs are active."
		l_queue_waits=$((l_queue_waits + 1))
		if [ "${g_count_zfs_send_jobs:-0}" -ge "$l_queue_job_limit" ]; then
			zxfer_wait_for_any_send_job "job limit"
		else
			zxfer_wait_for_any_send_job "destination ancestry"
		fi
	done
	# Every queued dataset is processed exactly once, so both counts match.
	[ "$l_queue_job_limit" -le 1 ] ||
		zxfer_echov "Replication ready queue summary: queued_datasets=$l_queue_processed processed_datasets=$l_queue_processed waits=$l_queue_waits active_jobs=${g_count_zfs_send_jobs:-0}"
	return 0
}

# Purpose: Replicate every dataset of the pass, then reconcile the properties
# of seeded destinations.
# Usage: zxfer_copy_filesystems, after discovery, the -s/-m snapshot and the
# -g pre-pass.
zxfer_copy_filesystems() {
	zxfer_echoV "Begin zxfer_copy_filesystems()"

	l_copy_property_pass=0
	if [ "$g_option_P_transfer_property" -eq 1 ] ||
		[ "$g_option_o_override_property" != "" ]; then
		l_copy_property_pass=1
	fi
	if [ "$l_copy_property_pass" -eq 0 ] &&
		[ -z "${g_recursive_source_list:-}" ] &&
		{ [ "$g_option_d_delete_destination_snapshots" -ne 1 ] ||
			[ -z "${g_recursive_destination_extra_dataset_list:-}" ]; }; then
		zxfer_wait_for_zfs_send_jobs "final sync"
		zxfer_echoV "End zxfer_copy_filesystems()"
		return
	fi
	zxfer_build_replication_iteration_list "$l_copy_property_pass" ||
		zxfer_throw_error "Failed to prepare replication dataset iteration list." "$?"
	if [ -z "$g_zxfer_replication_iteration_list_result" ]; then
		zxfer_wait_for_zfs_send_jobs "final sync"
		zxfer_echoV "End zxfer_copy_filesystems()"
		return
	fi
	zxfer_refresh_property_tree_prefetch_context

	g_zxfer_post_seed_property_sources=""
	zxfer_process_replication_ready_queue "$g_zxfer_replication_iteration_list_result" \
		"$l_copy_property_pass"
	zxfer_wait_for_zfs_send_jobs "final sync"

	if [ -n "$g_zxfer_post_seed_property_sources" ]; then
		l_copy_post_seed_sources=$(
			sort -u <<EOF
$g_zxfer_post_seed_property_sources
EOF
		) || zxfer_throw_error "Failed to prepare post-seed property reconcile source queue." "$?"
		zxfer_reset_destination_property_iteration_cache
		while IFS= read -r l_copy_post_seed_source; do
			[ -n "$l_copy_post_seed_source" ] || continue
			zxfer_set_actual_dest "$l_copy_post_seed_source"
			# This pass re-captures the dataset's -k row; the write boundary
			# keeps the newest row per dataset. ssh must not read the list.
			zxfer_transfer_properties "$l_copy_post_seed_source" </dev/null
		done <<EOF
$l_copy_post_seed_sources
EOF
		# Publish the buffered -k rows now that every seeded dataset has been
		# reconciled; the session's final write repeats this at the end.
		if [ "${g_option_k_backup_property_mode:-0}" -eq 1 ]; then
			zxfer_write_backup_properties ||
				zxfer_throw_error "Failed to write backup metadata." "$?"
			zxfer_set_failure_stage "replication"
		fi
	fi

	zxfer_echoV "End zxfer_copy_filesystems()"
}

# Purpose: Resolve and validate the pass's source and destination roots.
# Usage: zxfer_prepare_zfs_mode_roots, at the start of each pass; sets
# g_initial_source, g_initial_source_had_trailing_slash and the normalized
# g_destination, or exits through the usage and error helpers.
zxfer_prepare_zfs_mode_roots() {
	if [ "$g_option_R_recursive" != "" ] && [ "$g_option_N_nonrecursive" != "" ]; then
		zxfer_throw_usage_error "You must choose either -N to transfer a single filesystem or -R to transfer \
a single filesystem and its children recursively, but not both -N and -R at the same time."
	fi
	g_initial_source=$g_option_R_recursive$g_option_N_nonrecursive
	[ -n "$g_initial_source" ] ||
		zxfer_throw_usage_error "You must specify a source with either -N or -R."

	# A trailing slash on the source selects the destination-root mapping.
	case $g_initial_source in
	?*/) g_initial_source_had_trailing_slash=1 ;;
	*) g_initial_source_had_trailing_slash=0 ;;
	esac
	zxfer_strip_trailing_slashes "$g_initial_source"
	g_initial_source=$g_zxfer_stripped_path_result

	# Dataset names hold no control bytes and are relative to a pool, never
	# filesystem paths. The explicit tab, CR and LF arms cover shells whose
	# patterns lack [:cntrl:].
	for l_root_operand in "$g_initial_source" "$g_destination"; do
		case $l_root_operand in
		*[[:cntrl:]]* | *"$ZXFER_TAB"* | *"$ZXFER_CR"* | *"$ZXFER_LF"*)
			zxfer_throw_usage_error "Source and destination must not contain control characters."
			;;
		/*) zxfer_throw_usage_error "Source and destination must not begin with \"/\". Note the example." ;;
		esac
	done

	# A trailing slash on the destination would make later concatenation
	# produce an illegal double slash.
	zxfer_strip_trailing_slashes "$g_destination"
	g_destination=$g_zxfer_stripped_path_result
	zxfer_set_failure_roots "$g_initial_source" "$g_destination"

	zxfer_echoV "Checking source snapshot."
	case $g_initial_source in
	*@*) zxfer_throw_error "Snapshots are not allowed as a source." ;;
	esac

	# -c requires -m, so the operator has to think twice about a migration.
	[ -z "$g_option_c_services" ] || [ "$g_option_m_migrate" -eq 1 ] ||
		zxfer_throw_error "When using -c, -m needs to be specified as well."
	if [ -n "$g_option_c_services" ] && ! command -v svcadm >/dev/null 2>&1; then
		zxfer_throw_usage_error "The -c service-management option requires Solaris/illumos SMF (svcadm)."
	fi
}

# Purpose: Discover the pass's snapshots and dataset lists before live work.
# Usage: zxfer_initialize_replication_context, once per live pass.
zxfer_initialize_replication_context() {
	# Confirm the backup metadata exists before inspecting the destination.
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		zxfer_get_backup_properties
	fi

	zxfer_refresh_dataset_iteration_state

	if zxfer_unsupported_property_scan_is_required; then
		zxfer_calculate_unsupported_properties
	fi
}

# Purpose: Rediscover snapshots and rebuild the dataset lists and property
# prefetch context.
# Usage: zxfer_refresh_dataset_iteration_state, at pass start and after the
# -s or -m snapshot; exits when discovery fails.
zxfer_refresh_dataset_iteration_state() {
	# A discovery that returns non-zero without throwing must never reach the
	# -m unmounts or any send.
	zxfer_get_zfs_list ||
		zxfer_throw_error "Failed to retrieve the snapshot lists for [$g_initial_source] and [$g_destination]." "$?"
	# Without -R the only dataset to iterate is the initial source itself.
	[ "$g_option_R_recursive" != "" ] ||
		g_recursive_source_list=$g_initial_source
	zxfer_refresh_property_tree_prefetch_context
}

# Purpose: Take the -s snapshot and rediscover, unless -m takes it instead.
# Usage: zxfer_maybe_capture_preflight_snapshot, after discovery or from the
# dry-run preview.
zxfer_maybe_capture_preflight_snapshot() {
	if [ "$g_option_s_make_snapshot" -eq 0 ] || [ "$g_option_m_migrate" -eq 1 ]; then
		return
	fi

	zxfer_newsnap "$g_initial_source"
	[ "$g_option_n_dryrun" -eq 0 ] || return 0

	# The new snapshots must be discovered before they can be sent.
	zxfer_refresh_dataset_iteration_state
}

# Purpose: Preview a -n pass without live discovery or planning.
# Usage: zxfer_preview_zfs_mode_dry_run, instead of the live pass under -n;
# previews only the requested source dataset.
zxfer_preview_zfs_mode_dry_run() {
	zxfer_reset_snapshot_discovery_state
	zxfer_reset_destination_existence_cache
	g_recursive_source_list=$g_initial_source
	g_recursive_source_dataset_list=$g_initial_source
	if [ "$g_option_R_recursive" != "" ]; then
		zxfer_echoV "Dry run: recursive descendant discovery is skipped; previewing only the explicitly requested source dataset."
	fi
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

# Purpose: Refuse a -g pass before any change when a dataset has diverged or,
# with -d, when a planned destination delete is protected by -g.
# Usage: zxfer_perform_grandfather_protection_checks, before
# zxfer_copy_filesystems; plans every dataset the pass visits and enforces
# the divergence contract without deleting anything, so a -F receive never
# runs on an earlier dataset of a pass that refuses a later one.
zxfer_perform_grandfather_protection_checks() {
	[ "$g_option_g_grandfather_protection" != "" ] || return 0

	zxfer_echov "Checking grandfather status of all snapshots marked for deletion..."
	zxfer_build_replication_iteration_list 0 ||
		zxfer_throw_error "Failed to prepare replication dataset iteration list." "$?"
	while IFS= read -r l_grandfather_row; do
		[ -n "$l_grandfather_row" ] || continue
		l_grandfather_source=${l_grandfather_row#*"$ZXFER_TAB"}
		zxfer_set_actual_dest "$l_grandfather_source"
		zxfer_select_snapshot_slice "${l_grandfather_row%%"$ZXFER_TAB"*}" \
			"$l_grandfather_source"
		# DELETE=0 plans and enforces divergence only; ssh must not read the
		# list.
		zxfer_inspect_delete_snap 0 "$l_grandfather_source" </dev/null
		[ "$g_option_d_delete_destination_snapshots" -eq 1 ] || continue
		[ -n "$g_zxfer_plan_delete_snapshots" ] || continue
		zxfer_prepare_snapshot_delete_creation_state "$g_zxfer_plan_delete_snapshots" </dev/null
	done <<EOF
$g_zxfer_replication_iteration_list_result
EOF
	zxfer_echov "Grandfather check passed."
}

# Purpose: Run one live or dry-run replication pass.
# Usage: zxfer_run_zfs_mode, once per -Y iteration.
zxfer_run_zfs_mode() {
	zxfer_prepare_zfs_mode_roots
	# -m stops services and unmounts before its snapshot: name it first.
	[ "$g_option_m_migrate" -eq 0 ] || zxfer_stamp_new_snapshot_name
	zxfer_check_backup_storage_dir_if_needed ||
		zxfer_throw_error "Failed to prepare backup metadata storage." "$?"

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_preview_zfs_mode_dry_run
		return
	fi

	zxfer_initialize_replication_context
	zxfer_maybe_capture_preflight_snapshot
	zxfer_prepare_migration_services
	# Discovery and -m preparation name their own stages; failures from here
	# on that set none (the -g pre-pass, planning) are replication failures.
	zxfer_set_failure_stage "replication"
	zxfer_perform_grandfather_protection_checks

	zxfer_copy_filesystems

	# Re-launch any stopped services.
	[ "$g_option_m_migrate" -eq 0 ] || zxfer_relaunch
}

# Purpose: Repeat replication passes until one performs no send or destroy,
# or the -Y limit is reached.
# Usage: zxfer_run_zfs_mode_loop, the launcher entry point for replication.
zxfer_run_zfs_mode_loop() {
	l_num_iterations=0

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
			if [ "$g_option_Y_yield_iterations" -ge "$ZXFER_MAX_YIELD_ITERATIONS" ]; then
				zxfer_echoV "Exiting loop. Reached maximum number of iterations.
If consistently not completing replication in allotted iterations,
consider using compression, increasing bandwidth, increasing I/O or reducing snapshot frequency."
			fi
			break
		fi
	done
}
