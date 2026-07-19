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
# SNAPSHOT DELETE / LAST-COMMON / ROLLBACK HELPERS
################################################################################

# Module contract:
# owns globals: current-dataset delete/rollback state, its three lazy staging
#   artifacts, and the divergence contract (g_zxfer_diverged_snapshot_count,
#   g_zxfer_diverged_snapshot_examples, g_zxfer_diverged_converged_datasets).
# reads globals: g_actual_dest, runtime artifact results, and current dataset context.
# mutates caches: delete-planning artifacts and current rollback state.
# returns via stdout: last-common snapshot values, delete lists, and filtered snapshot identities.

# Purpose: Reset run-scoped snapshot delete-planning artifact handles.
# Usage: Called by session initialization separately from per-dataset reconcile
# resets so the same three contained files can be reused throughout one run.
zxfer_reset_snapshot_delete_artifact_state() {
	g_zxfer_snapshot_delete_source_identities_file=""
	g_zxfer_snapshot_delete_destination_identities_file=""
	g_zxfer_snapshot_delete_difference_file=""
}

# Purpose: Lazily allocate the three contained snapshot delete-planning files.
# Usage: Called before identity sorting and comparison. Existing paths are
# reused; partial allocation failures clean only paths created by this call and
# preserve the exact allocator status without publishing incomplete state.
zxfer_ensure_snapshot_delete_temp_artifacts() {
	l_snapshot_delete_source_file=${g_zxfer_snapshot_delete_source_identities_file:-}
	l_snapshot_delete_destination_file=${g_zxfer_snapshot_delete_destination_identities_file:-}
	l_snapshot_delete_difference_file=${g_zxfer_snapshot_delete_difference_file:-}
	l_snapshot_delete_missing_count=0
	[ -n "$l_snapshot_delete_source_file" ] ||
		l_snapshot_delete_missing_count=$((l_snapshot_delete_missing_count + 1))
	[ -n "$l_snapshot_delete_destination_file" ] ||
		l_snapshot_delete_missing_count=$((l_snapshot_delete_missing_count + 1))
	[ -n "$l_snapshot_delete_difference_file" ] ||
		l_snapshot_delete_missing_count=$((l_snapshot_delete_missing_count + 1))

	if [ "$l_snapshot_delete_missing_count" -gt 0 ]; then
		zxfer_create_temp_file_group "$l_snapshot_delete_missing_count" \
			>/dev/null || return "$?"
		while IFS= read -r l_snapshot_delete_new_file ||
			[ -n "$l_snapshot_delete_new_file" ]; do
			[ -n "$l_snapshot_delete_new_file" ] || continue
			if [ -z "$l_snapshot_delete_source_file" ]; then
				l_snapshot_delete_source_file=$l_snapshot_delete_new_file
			elif [ -z "$l_snapshot_delete_destination_file" ]; then
				l_snapshot_delete_destination_file=$l_snapshot_delete_new_file
			else
				l_snapshot_delete_difference_file=$l_snapshot_delete_new_file
			fi
		done <<EOF
$g_zxfer_temp_file_group_result
EOF
	fi

	g_zxfer_snapshot_delete_source_identities_file=$l_snapshot_delete_source_file
	g_zxfer_snapshot_delete_destination_identities_file=$l_snapshot_delete_destination_file
	g_zxfer_snapshot_delete_difference_file=$l_snapshot_delete_difference_file
	return 0
}

# Purpose: Reset the snapshot reconcile state so the next snapshot-reconcile
# pass starts from a clean state.
# Usage: Called during last-common-snapshot selection and delete planning
# before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_snapshot_reconcile_state() {
	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0
	g_src_snapshot_transfer_list=""
	g_zxfer_snapshot_record_capture_result=""
	g_zxfer_inspect_source_snapshots_result=""
	g_zxfer_inspect_destination_snapshots_result=""
	g_zxfer_inspect_identity_source_snapshots_result=""
	g_zxfer_inspect_identity_destination_snapshots_result=""
	g_zxfer_inspect_diverged_snapshot_records_result=""
	# Per-dataset divergence scratch (also reset per dataset by
	# zxfer_record_diverged_destination_snapshots) and the run-level
	# "diverged and converged this run" marker list consumed by the
	# post-receive verification.
	g_zxfer_diverged_snapshot_count=0
	g_zxfer_diverged_snapshot_examples=""
	g_zxfer_diverged_converged_datasets=""
	g_zxfer_diverged_converged_marker_source=""
	zxfer_reset_destination_snapshot_creation_cache
}

# Purpose: Publish the current snapshot transfer plan through its owner.
# Usage: Live reconciliation supplies the last common snapshot, remaining
# source records, and validated destination-snapshot presence as one update.
zxfer_publish_snapshot_transfer_plan() {
	g_last_common_snap=${1:-}
	g_src_snapshot_transfer_list=${2:-}
	case ${3:-0} in
	0 | 1) g_dest_has_snapshots=$3 ;;
	*) return 2 ;;
	esac
}

# Purpose: Mark a destination seed as the new common snapshot base.
# Usage: Called only after the foreground seed receive succeeds.
zxfer_mark_destination_snapshot_seeded() {
	g_last_common_snap=${1:-}
	g_dest_has_snapshots=1
}

# Purpose: Publish destination snapshot presence without changing the plan.
# Usage: Used when a live probe proves an empty or non-empty destination.
zxfer_set_destination_snapshot_presence() {
	case ${1:-} in
	0 | 1) g_dest_has_snapshots=$1 ;;
	*) return 2 ;;
	esac
}

# Purpose: Clear the per-dataset destination-deletion marker.
# Usage: Called when a fresh planning or dry-run pass starts.
zxfer_clear_destination_delete_marker() {
	g_did_delete_dest_snapshots=0
}

# Purpose: Capture the snapshot records for dataset into staged state or module
# globals for later use.
# Usage: Called during last-common-snapshot selection and delete planning when
# later helpers need a checked snapshot of command output or computed state.
zxfer_capture_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	g_zxfer_snapshot_record_capture_result=""
	if zxfer_capture_runtime_artifact_command_output \
		"zxfer-snapshot-records" \
		zxfer_get_snapshot_records_for_dataset "$l_side" "$l_dataset"; then
		:
	else
		l_snapshot_record_capture_status=$?
		return "$l_snapshot_record_capture_status"
	fi

	g_zxfer_snapshot_record_capture_result=$g_zxfer_runtime_artifact_read_result
	case "$g_zxfer_snapshot_record_capture_result" in
	*'
')
		g_zxfer_snapshot_record_capture_result=${g_zxfer_snapshot_record_capture_result%?}
		;;
	esac

	return 0
}

# Purpose: Write the snapshot identities to file in the normalized form later
# zxfer steps expect.
# Usage: Called during last-common-snapshot selection and delete planning when
# the module needs a stable staged file or emitted stream for downstream use.
#
# Returns a list of destination snapshots that don't exist in the source.
# The source and destination snapshots should correspond to 1 dataset.
# Uses global temporary files to reduce mktemp operations per call
# g_zxfer_snapshot_delete_source_identities_file
# g_zxfer_snapshot_delete_destination_identities_file
# g_zxfer_snapshot_delete_difference_file
zxfer_write_snapshot_identities_to_file() {
	l_snapshot_records=$1
	l_output_file=$2

	if zxfer_read_normalized_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	{
		while IFS= read -r l_snapshot_record; do
			[ -n "$l_snapshot_record" ] || continue
			l_snapshot_identity=$(zxfer_extract_snapshot_identity "$l_snapshot_record")
			[ -n "$l_snapshot_identity" ] || continue
			printf '%s\n' "$l_snapshot_identity"
		done <<EOF
$g_zxfer_runtime_artifact_read_result
EOF
	} | LC_ALL=C sort -u >"$l_output_file"
}

# Purpose: Write the destination snapshot paths for identity file in the
# normalized form later zxfer steps expect.
# Usage: Called during last-common-snapshot selection and delete planning when
# the module needs a stable staged file or emitted stream for downstream use.
zxfer_write_destination_snapshot_paths_for_identity_file() {
	l_snapshot_records=$1
	l_identity_file=$2

	if zxfer_read_normalized_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	# shellcheck disable=SC2016
	"${g_cmd_awk:-awk}" 'NR == FNR { if ($0 != "") delete_identities[$0] = 1; next }
	$0 != "" { record = $0; tab_pos = index(record, "\t"); snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record); snapshot_guid = (tab_pos > 0 ? substr(record, tab_pos + 1) : ""); at_pos = index(snapshot_path, "@"); if (at_pos > 0) { snapshot_identity = substr(snapshot_path, at_pos + 1); if (snapshot_guid != "") snapshot_identity = snapshot_identity "\t" snapshot_guid; if (snapshot_identity in delete_identities) print snapshot_path } }' "$l_identity_file" - <<-EOF
		$g_zxfer_runtime_artifact_read_result
	EOF
}

# Purpose: Return the dest snapshots to delete per dataset in the form expected
# by later helpers.
# Usage: Called during last-common-snapshot selection and delete planning when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_dest_snapshots_to_delete_per_dataset() {
	zxfer_echoV "Begin zxfer_get_dest_snapshots_to_delete_per_dataset()"
	l_zfs_source_snaps=$1
	l_zfs_dest_snaps=$2
	l_source_identity_status=0
	l_dest_identity_status=0
	l_snapshot_diff_status=0
	l_snapshot_path_status=0

	if zxfer_ensure_snapshot_delete_temp_artifacts; then
		:
	else
		l_delete_artifact_status=$?
		return "$l_delete_artifact_status"
	fi

	# Write snapshot identity keys (name + guid) to the temporary files so that
	# `comm` can distinguish same-named but unrelated snapshots.
	# run the first process in the background
	zxfer_write_snapshot_identities_to_file "$l_zfs_source_snaps" "$g_zxfer_snapshot_delete_source_identities_file" &
	l_source_identity_pid=$!
	l_source_identity_waited=0
	if ! zxfer_register_cleanup_pid \
		"$l_source_identity_pid" "delete planning identity writer"; then
		l_source_identity_status=0
		wait "$l_source_identity_pid" 2>/dev/null || l_source_identity_status=$?
		l_source_identity_waited=1
	fi

	l_dest_identity_status=0
	zxfer_write_snapshot_identities_to_file "$l_zfs_dest_snaps" "$g_zxfer_snapshot_delete_destination_identities_file" ||
		l_dest_identity_status=$?

	# wait for the background process to finish
	if [ "$l_source_identity_waited" -ne 1 ]; then
		l_source_identity_status=0
		wait "$l_source_identity_pid" 2>/dev/null || l_source_identity_status=$?
		zxfer_unregister_cleanup_pid "$l_source_identity_pid"
	fi

	if [ "$l_dest_identity_status" -ne 0 ]; then
		zxfer_throw_error "Failed to generate destination snapshot identities for delete planning." "$l_dest_identity_status"
	fi
	if [ "$l_source_identity_status" -ne 0 ]; then
		zxfer_throw_error "Failed to generate source snapshot identities for delete planning." "$l_source_identity_status"
	fi

	# Use comm to find snapshots in g_zxfer_snapshot_delete_destination_identities_file that don't have a match in g_zxfer_snapshot_delete_source_identities_file
	l_snapshot_diff_status=0
	LC_ALL=C comm -13 "$g_zxfer_snapshot_delete_source_identities_file" "$g_zxfer_snapshot_delete_destination_identities_file" >"$g_zxfer_snapshot_delete_difference_file" ||
		l_snapshot_diff_status=$?
	if [ "$l_snapshot_diff_status" -ne 0 ]; then
		zxfer_throw_error "Failed to diff source and destination snapshot identities for delete planning." "$l_snapshot_diff_status"
	fi

	l_snapshot_path_status=0
	l_dest_snaps_to_delete=$(zxfer_write_destination_snapshot_paths_for_identity_file "$l_zfs_dest_snaps" "$g_zxfer_snapshot_delete_difference_file") ||
		l_snapshot_path_status=$?
	if [ "$l_snapshot_path_status" -ne 0 ]; then
		zxfer_throw_error "Failed to map destination snapshot identities back to snapshot paths for delete planning." "$l_snapshot_path_status"
	fi

	# Print the matching lines
	printf '%s\n' "$l_dest_snaps_to_delete"
}

# Purpose: Return the last common snapshot in the form expected by later
# helpers.
# Usage: Called during last-common-snapshot selection and delete planning when
# sibling helpers need the same lookup without duplicating module logic.
#
# find the most recent common snapshot. The source list is in descending order
# by creation date. The destination list is unordered.
zxfer_get_last_common_snapshot() {
	zxfer_echoV "Begin zxfer_get_last_common_snapshot()"

	# sorted list of source datasets and snapshots
	l_zfs_source_snaps=$1
	# unordered list of destination datasets and snapshots
	l_zfs_dest_snaps=$2
	l_dest_identity_file=""
	l_source_snapshot_file=""

	# Build a destination identity set and then scan the source list once in its
	# existing newest-first order so we still choose the most recent common
	# snapshot without repeated shell-string scans.
	l_common_snapshot_awk=$(
		cat <<'EOF'
NR == FNR {
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
		print record
		exit
	}
}
EOF
	)
	if zxfer_read_normalized_snapshot_record_list "$l_zfs_dest_snaps" >/dev/null; then
		:
	else
		l_get_last_common_snapshot_status=$?
		return "$l_get_last_common_snapshot_status"
	fi
	l_normalized_dest_snaps=$g_zxfer_runtime_artifact_read_result

	l_get_last_common_snapshot_status=0
	zxfer_create_temp_file_group 2 >/dev/null || l_get_last_common_snapshot_status=$?
	if [ "$l_get_last_common_snapshot_status" -ne 0 ]; then
		return "$l_get_last_common_snapshot_status"
	fi
	l_last_common_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dest_identity_file
		IFS= read -r l_source_snapshot_file
	} <<-EOF
		$l_last_common_stage_files
	EOF

	if zxfer_write_snapshot_identities_to_file "$l_normalized_dest_snaps" "$l_dest_identity_file"; then
		:
	else
		l_get_last_common_snapshot_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_get_last_common_snapshot_status" "$l_last_common_stage_files"
		return "$?"
	fi
	if zxfer_read_normalized_snapshot_record_list "$l_zfs_source_snaps" >/dev/null; then
		:
	else
		l_get_last_common_snapshot_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_get_last_common_snapshot_status" "$l_last_common_stage_files"
		return "$?"
	fi
	if zxfer_write_runtime_artifact_file \
		"$l_source_snapshot_file" "$g_zxfer_runtime_artifact_read_result"; then
		:
	else
		l_get_last_common_snapshot_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_get_last_common_snapshot_status" "$l_last_common_stage_files"
		return "$?"
	fi

	if l_last_common_snap=$("${g_cmd_awk:-awk}" "$l_common_snapshot_awk" \
		"$l_dest_identity_file" "$l_source_snapshot_file"); then
		:
	else
		l_get_last_common_snapshot_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_get_last_common_snapshot_status" "$l_last_common_stage_files"
		return "$?"
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_last_common_stage_files"

	if [ -n "$l_last_common_snap" ]; then
		zxfer_echoV "Found last common snapshot: $l_last_common_snap."

		# once found, exit the function
		echo "$l_last_common_snap"
		return
	fi

	zxfer_echoV "No common snapshot found."

	# return blank because no common snapshots has been found
	echo ""

	zxfer_echoV "End zxfer_get_last_common_snapshot()"
}

# Purpose: Reset the destination snapshot creation cache so the next snapshot-
# reconcile pass starts from a clean state.
# Usage: Called during last-common-snapshot selection and delete planning
# before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_destination_snapshot_creation_cache() {
	g_destination_snapshot_creation_cache=""
}

# Purpose: Check whether the snapshot creation epoch is numeric.
# Usage: Called during last-common-snapshot selection and delete planning when
# later helpers need a boolean answer about the snapshot creation epoch.
zxfer_snapshot_creation_epoch_is_numeric() {
	case "$1" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Format the snapshot creation epoch for display for display or
# serialized output.
# Usage: Called during last-common-snapshot selection and delete planning when
# operators or downstream helpers need a stable presentation.
#
# Render a validated snapshot creation epoch for operator-facing diagnostics
# without issuing a second live remote `zfs get creation` query.
zxfer_format_snapshot_creation_epoch_for_display() {
	l_creation_epoch=$1

	if ! zxfer_snapshot_creation_epoch_is_numeric "$l_creation_epoch"; then
		return 1
	fi

	if l_creation_display=$(date -r "$l_creation_epoch" 2>/dev/null); then
		printf '%s\n' "$l_creation_display"
		return 0
	fi

	if l_creation_display=$(date -d "@$l_creation_epoch" 2>/dev/null); then
		printf '%s\n' "$l_creation_display"
		return 0
	fi

	printf '%s\n' "$l_creation_epoch (unix epoch)"
	return 0
}

# Purpose: Look up the destination snapshot creation cache in the cache or
# staged state owned by this module.
# Usage: Called during last-common-snapshot selection and delete planning when
# later helpers need a reusable answer without repeating a live probe.
zxfer_lookup_destination_snapshot_creation_cache() {
	l_lookup_snapshot_path=$1

	[ -n "$l_lookup_snapshot_path" ] || return 1

	while IFS='	' read -r l_cache_snapshot_path l_cache_creation_value || [ -n "${l_cache_snapshot_path}${l_cache_creation_value}" ]; do
		[ -n "$l_cache_snapshot_path" ] || continue
		if [ "$l_cache_snapshot_path" = "$l_lookup_snapshot_path" ]; then
			printf '%s\n' "$l_cache_creation_value"
			return 0
		fi
	done <<-EOF
		${g_destination_snapshot_creation_cache:-}
	EOF

	return 1
}

# Purpose: Store the destination snapshot creation cache entries in the cache
# or staging location owned by this module.
# Usage: Called during last-common-snapshot selection and delete planning after
# zxfer has a validated value that later helpers may reuse.
zxfer_store_destination_snapshot_creation_cache_entries() {
	l_cache_results=$1

	[ -n "$l_cache_results" ] || return 0

	while IFS='	' read -r l_cache_snapshot_path l_cache_creation_value || [ -n "${l_cache_snapshot_path}${l_cache_creation_value}" ]; do
		[ -n "$l_cache_snapshot_path" ] || continue
		zxfer_snapshot_creation_epoch_is_numeric "$l_cache_creation_value" || continue
		if [ -n "${g_destination_snapshot_creation_cache:-}" ]; then
			g_destination_snapshot_creation_cache="$g_destination_snapshot_creation_cache
$l_cache_snapshot_path	$l_cache_creation_value"
		else
			g_destination_snapshot_creation_cache="$l_cache_snapshot_path	$l_cache_creation_value"
		fi
	done <<-EOF
		$l_cache_results
	EOF
}

# Purpose: Prefetch the destination snapshot creation paths so later lookups
# can reuse staged data.
# Usage: Called during last-common-snapshot selection and delete planning
# before a loop would otherwise repeat the same live probe or read.
zxfer_prefetch_destination_snapshot_creation_paths() {
	l_prefetch_snapshot_records=$1
	l_prefetch_batch_limit=128
	l_prefetch_batch_count=0

	set --

	while IFS= read -r l_prefetch_snapshot_record; do
		[ -n "$l_prefetch_snapshot_record" ] || continue
		l_prefetch_snapshot_path=$(zxfer_extract_snapshot_path "$l_prefetch_snapshot_record")
		[ -n "$l_prefetch_snapshot_path" ] || continue
		set -- "$@" "$l_prefetch_snapshot_path"
		l_prefetch_batch_count=$((l_prefetch_batch_count + 1))
		if [ "$l_prefetch_batch_count" -lt "$l_prefetch_batch_limit" ]; then
			continue
		fi
		if l_prefetch_creation_results=$(zxfer_run_destination_zfs_cmd get -H -o name,value -p creation "$@"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		zxfer_store_destination_snapshot_creation_cache_entries "$l_prefetch_creation_results"
		set --
		l_prefetch_batch_count=0
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_prefetch_snapshot_records")
	EOF

	[ "$l_prefetch_batch_count" -gt 0 ] || return 0

	if l_prefetch_creation_results=$(zxfer_run_destination_zfs_cmd get -H -o name,value -p creation "$@"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	zxfer_store_destination_snapshot_creation_cache_entries "$l_prefetch_creation_results"
}

# Purpose: Prefetch the delete snapshot creation times so later lookups can
# reuse staged data.
# Usage: Called during last-common-snapshot selection and delete planning
# before a loop would otherwise repeat the same live probe or read.
zxfer_prefetch_delete_snapshot_creation_times() {
	l_delete_snapshot_records=$1
	l_delete_prefetch_records=""

	if [ -n "$l_delete_snapshot_records" ] &&
		{ [ -n "${g_option_g_grandfather_protection:-}" ] ||
			{ [ -n "${g_last_common_snap:-}" ] && [ -n "${g_actual_dest:-}" ]; }; }; then
		l_delete_prefetch_records=$l_delete_snapshot_records
	fi

	if [ -n "${g_last_common_snap:-}" ] && [ -n "${g_actual_dest:-}" ]; then
		l_delete_last_common_name=$(zxfer_extract_snapshot_name "$g_last_common_snap")
		if [ -n "$l_delete_last_common_name" ]; then
			if [ -n "$l_delete_prefetch_records" ]; then
				l_delete_prefetch_records="$g_actual_dest@$l_delete_last_common_name
$l_delete_prefetch_records"
			else
				l_delete_prefetch_records="$g_actual_dest@$l_delete_last_common_name"
			fi
		fi
	fi

	[ -n "$l_delete_prefetch_records" ] || return 0
	zxfer_prefetch_destination_snapshot_creation_paths "$l_delete_prefetch_records"
}

# Purpose: Return the destination snapshot creation epoch in the form expected
# by later helpers.
# Usage: Called during last-common-snapshot selection and delete planning when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_destination_snapshot_creation_epoch() {
	l_snapshot_path=$1

	if l_cached_creation=$(zxfer_lookup_destination_snapshot_creation_cache "$l_snapshot_path" 2>/dev/null); then
		printf '%s\n' "$l_cached_creation"
		return 0
	fi

	if l_creation_value=$(zxfer_run_destination_zfs_cmd get -H -o value -p creation "$l_snapshot_path"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	if zxfer_snapshot_creation_epoch_is_numeric "$l_creation_value"; then
		zxfer_store_destination_snapshot_creation_cache_entries "$(printf '%s\t%s\n' "$l_snapshot_path" "$l_creation_value")"
	fi
	printf '%s\n' "$l_creation_value"
}

# Purpose: Check how deleted snapshots include newer than last common interacts
# with the current safety rules.
# Usage: Called during last-common-snapshot selection and delete planning
# before zxfer deletes snapshots that could invalidate the last-common-snapshot
# anchor.
zxfer_deleted_snapshots_include_newer_than_last_common() {
	l_deleted_snapshots=$1

	[ -n "$l_deleted_snapshots" ] || return 1
	[ -n "${g_last_common_snap:-}" ] || return 1
	[ -n "${g_actual_dest:-}" ] || return 1

	l_last_common_name=$(zxfer_extract_snapshot_name "$g_last_common_snap")
	[ -n "$l_last_common_name" ] || return 1

	l_last_common_dest_snapshot="$g_actual_dest@$l_last_common_name"
	l_last_common_creation_status=0
	l_last_common_creation=$(zxfer_get_destination_snapshot_creation_epoch "$l_last_common_dest_snapshot") ||
		l_last_common_creation_status=$?
	if [ "$l_last_common_creation_status" -ne 0 ]; then
		return 2
	fi
	case "$l_last_common_creation" in
	'' | *[!0-9]*)
		# Fail safe: if we cannot compare creation times, keep rollback eligible.
		return 0
		;;
	esac

	while IFS= read -r l_deleted_snapshot; do
		[ -n "$l_deleted_snapshot" ] || continue
		l_deleted_snapshot_path=$(zxfer_extract_snapshot_path "$l_deleted_snapshot")
		l_deleted_creation_status=0
		l_deleted_creation=$(zxfer_get_destination_snapshot_creation_epoch "$l_deleted_snapshot_path") ||
			l_deleted_creation_status=$?
		if [ "$l_deleted_creation_status" -ne 0 ]; then
			return 2
		fi
		case "$l_deleted_creation" in
		'' | *[!0-9]*)
			return 0
			;;
		esac
		if [ "$l_deleted_creation" -gt "$l_last_common_creation" ]; then
			return 0
		fi
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_deleted_snapshots")
	EOF

	return 1
}

# Purpose: Apply grandfather-retention safety checks to the test.
# Usage: Called during last-common-snapshot selection and delete planning
# before delete planning removes snapshots that may still fall inside the
# protected retention window.
#
# Tests a snapshot to see if it is older than the grandfather option allows for.
zxfer_grandfather_test() {
	l_destination_snapshot=$1

	l_current_date=$(date +%s) # current date in seconds from 1970
	l_snap_date_status=0
	l_snap_date=$(zxfer_get_destination_snapshot_creation_epoch "$l_destination_snapshot") ||
		l_snap_date_status=$?
	if [ "$l_snap_date_status" -ne 0 ]; then
		zxfer_throw_error "Failed to query creation time for destination snapshot $l_destination_snapshot. Review prior stderr for the transport or query error." "$l_snap_date_status"
	fi
	case "$l_snap_date" in
	'' | *[!0-9]*)
		zxfer_throw_error "Couldn't determine creation time for destination snapshot $l_destination_snapshot."
		;;
	esac

	l_diff_sec=$((l_current_date - l_snap_date))
	l_diff_day=$((l_diff_sec / 86400))

	if [ $l_diff_day -ge "$g_option_g_grandfather_protection" ]; then
		l_snap_date_english_status=0
		l_snap_date_english=$(zxfer_format_snapshot_creation_epoch_for_display "$l_snap_date") ||
			l_snap_date_english_status=$?
		if [ "$l_snap_date_english_status" -ne 0 ] || [ -z "$l_snap_date_english" ]; then
			l_snap_date_english="$l_snap_date (unix epoch)"
		fi
		l_current_date_english=$(date)
		l_error_msg="On the destination there is a snapshot marked for destruction
            by zxfer that is protected by the use of the \"grandfather
            protection\" option, -g.

            You have set grandfather protection at $g_option_g_grandfather_protection days.
            Snapshot name: $l_destination_snapshot
            Snapshot age : $l_diff_day days old
            Snapshot date: $l_snap_date_english.
            Your current system date: $l_current_date_english.

            Either amend/remove option g, fix your system date, or manually
            destroy the offending snapshot. Also double check that your
            snapshot management tool isn't erroneously deleting source snapshots.
            Note that for option g to work correctly, you should set it just
            above a number of days that will preclude \"father\" snapshots from
            being encountered."

		zxfer_throw_usage_error "$l_error_msg"
	fi
}

# Purpose: Revalidate a plan that would delete every destination snapshot.
# Usage: Called only for an empty cached source list with a known source
# dataset; returns non-zero when a live source snapshot makes deletion unsafe.
# Returns: Zero when the delete plan remains safe, one after a skip warning.
zxfer_all_destination_snapshot_delete_is_safe() {
	l_delete_safety_source_snapshots=$1
	l_delete_safety_source_dataset=$2

	[ -n "$l_delete_safety_source_dataset" ] || return 0
	case ${l_delete_safety_source_snapshots:-} in
	*[![:space:]]*)
		return 0
		;;
	esac

	l_delete_safety_live_status=0
	l_delete_safety_live_snapshots=$(zxfer_run_source_zfs_cmd \
		list -H -d 1 -o name -t snapshot \
		"$l_delete_safety_source_dataset" 2>&1) ||
		l_delete_safety_live_status=$?
	if [ "$l_delete_safety_live_status" -ne 0 ]; then
		zxfer_throw_error "Failed to re-verify source snapshots for [$l_delete_safety_source_dataset] before deleting all destination snapshots: $l_delete_safety_live_snapshots" \
			"$l_delete_safety_live_status"
	fi

	# Remote stderr can share the capture with stdout. Count only real source
	# snapshot rows so benign transport diagnostics cannot change the decision.
	while IFS= read -r l_delete_safety_live_line; do
		case $l_delete_safety_live_line in
		"$l_delete_safety_source_dataset@"*)
			zxfer_warn_stderr "WARNING: skipping destination snapshot deletion for [$l_delete_safety_source_dataset]: the plan would delete every destination snapshot, but a live source re-check still shows snapshots. The cached source listing was likely incomplete."
			return 1
			;;
		esac
	done <<-EOF
		$l_delete_safety_live_snapshots
	EOF

	return 0
}

# Purpose: Prefetch and classify creation-time state for a snapshot delete plan.
# Usage: Called after the all-snapshots safety recheck and before grandfather
# policy or destroy-target rendering.
# Side effects: Publishes whether the plan deletes snapshots newer than common.
zxfer_prepare_snapshot_delete_creation_state() {
	l_delete_creation_snapshots=$1

	zxfer_reset_destination_snapshot_creation_cache
	l_delete_creation_prefetch_status=0
	zxfer_prefetch_delete_snapshot_creation_times \
		"$l_delete_creation_snapshots" >/dev/null ||
		l_delete_creation_prefetch_status=$?
	if [ "$l_delete_creation_prefetch_status" -ne 0 ]; then
		zxfer_throw_error "Failed to query destination snapshot creation times while planning snapshot deletions. Review prior stderr for the transport or query error." \
			"$l_delete_creation_prefetch_status"
	fi

	g_deleted_dest_newer_snapshots=0
	l_delete_creation_newer_status=0
	zxfer_deleted_snapshots_include_newer_than_last_common \
		"$l_delete_creation_snapshots" ||
		l_delete_creation_newer_status=$?
	if [ "$l_delete_creation_newer_status" -eq 2 ]; then
		zxfer_throw_error "Failed to query destination snapshot creation times while evaluating rollback eligibility. Review prior stderr for the transport or query error."
	fi
	if [ "$l_delete_creation_newer_status" -eq 0 ]; then
		g_deleted_dest_newer_snapshots=1
	fi
}

# Purpose: Apply grandfather retention policy to one snapshot delete plan.
# Usage: Called in the owning shell before destroy-target rendering so a usage
# error cannot be confined to and discarded with command-substitution state.
# Side effects: Exits through zxfer_grandfather_test when a protected snapshot
# would be deleted.
zxfer_check_snapshot_delete_grandfather_policy() {
	l_grandfather_plan_snapshots=$1

	[ "$g_option_g_grandfather_protection" != "" ] || return 0
	while IFS= read -r l_grandfather_plan_snapshot; do
		[ -n "$l_grandfather_plan_snapshot" ] || continue
		zxfer_grandfather_test "$l_grandfather_plan_snapshot"
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_grandfather_plan_snapshots")
	EOF
}

# Purpose: Render the exact dataset@snapshot-list target for one delete plan.
# Usage: Called only after main-shell grandfather checks have passed; builds
# the comma-delimited target consumed by `zfs destroy`.
# Returns: The destroy target on stdout.
zxfer_get_snapshot_destroy_target() {
	l_destroy_plan_snapshots=$1
	l_destroy_plan_unprotected_names=""

	while IFS= read -r l_destroy_plan_snapshot; do
		[ -n "$l_destroy_plan_snapshot" ] || continue
		l_destroy_plan_name=$(zxfer_extract_snapshot_name \
			"$l_destroy_plan_snapshot")
		l_destroy_plan_unprotected_names="$l_destroy_plan_name,$l_destroy_plan_unprotected_names"
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_destroy_plan_snapshots")
	EOF
	l_destroy_plan_unprotected_names=${l_destroy_plan_unprotected_names%,}

	# shellcheck disable=SC2016
	l_destroy_plan_dataset=$(printf '%s\n' "$l_destroy_plan_snapshots" |
		head -n 1 | "$g_cmd_awk" -F'@' '{print $1}')
	printf '%s@%s\n' "$l_destroy_plan_dataset" \
		"$l_destroy_plan_unprotected_names"
}

# Purpose: Delete the snaps through the guarded reconciliation path owned by
# this module.
# Usage: Called during last-common-snapshot selection and delete planning after
# safety checks confirm the extra state can be removed.
#
# Delete snapshots in destination that aren't in source
zxfer_delete_snaps() {
	zxfer_echoV "Begin zxfer_delete_snaps()"
	l_zfs_source_snaps=$1
	l_zfs_dest_snaps=$2
	# Optional: the source dataset backing $1. When provided, an empty source
	# snapshot list is re-verified live before the planned deletion removes
	# every destination snapshot for the dataset.
	l_delete_source_dataset=${3:-}

	if l_snaps_to_delete=$(zxfer_get_dest_snapshots_to_delete_per_dataset "$l_zfs_source_snaps" "$l_zfs_dest_snaps"); then
		:
	else
		l_delete_plan_status=$?
		return "$l_delete_plan_status"
	fi

	# if l_snaps_to_delete is empty, there is nothing to do
	if [ "$l_snaps_to_delete" = "" ]; then
		zxfer_echoV "No snapshots to delete."
		return
	fi

	# An empty source list plans deletion of every destination snapshot. Recheck
	# the source live in that suspicious case and skip when the cache was stale.
	if ! zxfer_all_destination_snapshot_delete_is_safe \
		"$l_zfs_source_snaps" "$l_delete_source_dataset"; then
		return 0
	fi

	zxfer_prepare_snapshot_delete_creation_state "$l_snaps_to_delete"
	zxfer_check_snapshot_delete_grandfather_policy "$l_snaps_to_delete"
	l_destroy_target=$(zxfer_get_snapshot_destroy_target "$l_snaps_to_delete") ||
		return "$?"
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $(zxfer_render_destination_zfs_command destroy "$l_destroy_target")"
		return
	fi

	g_did_delete_dest_snapshots=1
	l_destroy_status=0
	zxfer_run_destination_zfs_cmd destroy "$l_destroy_target" || l_destroy_status=$?
	if [ "$l_destroy_status" -ne 0 ]; then
		zxfer_throw_error "Error when executing command." "$l_destroy_status"
	fi
	# The destroy mutated this run's destination: stale batched live views
	# must be refreshed before the next recheck-driven decision.
	zxfer_bump_destination_mutation_generation
	# Do not wipe the whole-tree destination snapshot record cache here: the
	# destroy only removed this dataset's own snapshots, the copy planning that
	# follows re-probes the destination live, and the wipe also cleared the
	# in-memory fallback list so -d delete planning for every later dataset saw
	# an empty destination and silently skipped its deletions.

	# set the flag to indicate that a destroy command was sent
	zxfer_mark_send_or_destroy_performed

	zxfer_echoV "End zxfer_delete_snaps()"
}

# Purpose: Update the src snapshot transfer list in the shared runtime state.
# Usage: Called during last-common-snapshot selection and delete planning after
# a probe or planning step changes the active context that later helpers should
# use.
#
# g_last_common_snap may be blank when no common snapshot is found.
zxfer_set_src_snapshot_transfer_list() {
	l_zfs_source_snaps=$1
	l_source=$2
	l_last_common_path=$(zxfer_extract_snapshot_path "$g_last_common_snap")

	l_found_common=0

	g_src_snapshot_transfer_list=""

	# This prepares a list of source snapshots to transfer, beginning with
	# the first snapshot after the last common one.
	while IFS= read -r l_test_snap; do
		[ -n "$l_test_snap" ] || continue
		if [ "$g_last_common_snap" != "" ] &&
			[ "$(zxfer_extract_snapshot_path "$l_test_snap")" = "$l_last_common_path" ]; then
			l_found_common=1
			continue
		fi

		if [ "$l_found_common" -eq 0 ]; then
			if [ -n "$g_src_snapshot_transfer_list" ]; then
				g_src_snapshot_transfer_list="$l_test_snap
$g_src_snapshot_transfer_list"
			else
				g_src_snapshot_transfer_list=$l_test_snap
			fi
		fi
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_zfs_source_snaps")
	EOF
}

# Purpose: Scan one dataset's source and destination snapshot record lists in
# a single awk pass for shared snapshot names and guid divergence.
# Usage: Called during delete planning and post-receive verification. Prints
# one "name<TAB>source_guid<TAB>destination_guid" line per destination
# snapshot whose name exists on the source with a DIFFERENT guid (diverged
# data under an identical name). Returns 0 when at least one snapshot name is
# shared between the two lists, 1 otherwise, so zxfer_inspect_delete_snap can
# reuse this one pass as its shared-name gate without spawning a second awk
# on in-sync planning paths. Records lacking guids cannot be classified and
# never produce divergence lines.
zxfer_scan_snapshot_record_lists_for_divergence() {
	l_scan_source_records=$1
	l_scan_dest_records=$2
	l_section_break="@@ZXFER_DIVERGENCE_SET_BREAK@@"
	l_divergence_scan_awk=$(
		cat <<'EOF'
BEGIN {
	in_source = 0
	shared = 0
}
$0 == section_break {
	in_source = 1
	next
}
!in_source {
	if ($0 != "") {
		record = $0
		tab_pos = index(record, "\t")
		snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
		snapshot_guid = (tab_pos > 0 ? substr(record, tab_pos + 1) : "")
		at_pos = index(snapshot_path, "@")
		if (at_pos > 0)
			dest_guids[substr(snapshot_path, at_pos + 1)] = snapshot_guid
	}
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
	snapshot_name = substr(snapshot_path, at_pos + 1)
	if (!(snapshot_name in dest_guids))
		next
	shared = 1
	dest_guid = dest_guids[snapshot_name]
	if (snapshot_guid != "" && dest_guid != "" && snapshot_guid != dest_guid)
		printf "%s\t%s\t%s\n", snapshot_name, snapshot_guid, dest_guid
}
END { exit(shared ? 0 : 1) }
EOF
	)

	{
		zxfer_normalize_snapshot_record_list "$l_scan_dest_records"
		printf '%s\n' "$l_section_break"
		zxfer_normalize_snapshot_record_list "$l_scan_source_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_divergence_scan_awk"
}

# Purpose: Record the current dataset's diverged destination snapshots into
# the per-dataset divergence scratch globals.
# Usage: Called by zxfer_inspect_delete_snap with the divergence-scan output
# before the divergence contract is enforced. Resets and repopulates
# g_zxfer_diverged_snapshot_count and g_zxfer_diverged_snapshot_examples (up
# to three "name<TAB>source_guid<TAB>destination_guid" example lines).
zxfer_record_diverged_destination_snapshots() {
	l_diverged_records=$1

	g_zxfer_diverged_snapshot_count=0
	g_zxfer_diverged_snapshot_examples=""
	[ -n "$l_diverged_records" ] || return 0

	while IFS= read -r l_diverged_record; do
		[ -n "$l_diverged_record" ] || continue
		g_zxfer_diverged_snapshot_count=$((g_zxfer_diverged_snapshot_count + 1))
		[ "$g_zxfer_diverged_snapshot_count" -le 3 ] || continue
		if [ -n "$g_zxfer_diverged_snapshot_examples" ]; then
			g_zxfer_diverged_snapshot_examples="$g_zxfer_diverged_snapshot_examples
$l_diverged_record"
		else
			g_zxfer_diverged_snapshot_examples=$l_diverged_record
		fi
	done <<-EOF
		$l_diverged_records
	EOF

	return 0
}

# Purpose: Find the "diverged and converged this run" marker for a destination
# dataset.
# Usage: Called by the divergence contract gate (to deduplicate warnings when
# planning inspects a dataset more than once per run) and by the post-receive
# verification. Publishes the marker's source dataset in
# g_zxfer_diverged_converged_marker_source and returns 0 on a hit, 1 on a miss.
zxfer_find_diverged_converged_marker() {
	l_marker_dest=$1

	g_zxfer_diverged_converged_marker_source=""
	[ -n "${g_zxfer_diverged_converged_datasets:-}" ] || return 1

	while IFS='	' read -r l_marker_record_dest l_marker_record_source; do
		[ -n "$l_marker_record_dest" ] || continue
		if [ "$l_marker_record_dest" = "$l_marker_dest" ]; then
			g_zxfer_diverged_converged_marker_source=$l_marker_record_source
			return 0
		fi
	done <<-EOF
		$g_zxfer_diverged_converged_datasets
	EOF

	return 1
}

# Purpose: Drop one destination dataset from the "diverged and converged this
# run" marker list.
# Usage: Called by the post-receive verification after the live destination
# view confirms the dataset no longer carries name-match/guid-mismatch
# snapshots.
zxfer_unmark_diverged_converged_dataset() {
	l_unmark_dest=$1
	l_remaining_markers=""

	while IFS='	' read -r l_marker_record_dest l_marker_record_source; do
		[ -n "$l_marker_record_dest" ] || continue
		[ "$l_marker_record_dest" = "$l_unmark_dest" ] && continue
		if [ -n "$l_remaining_markers" ]; then
			l_remaining_markers="$l_remaining_markers
$l_marker_record_dest	$l_marker_record_source"
		else
			l_remaining_markers="$l_marker_record_dest	$l_marker_record_source"
		fi
	done <<-EOF
		${g_zxfer_diverged_converged_datasets:-}
	EOF

	g_zxfer_diverged_converged_datasets=$l_remaining_markers
	return 0
}

# Purpose: Render the recorded divergence examples as operator-facing lines
# naming the destination snapshot and both guids.
# Usage: Called while building the divergence warning and the fail-closed
# divergence error so both surfaces show identical evidence.
zxfer_render_diverged_snapshot_example_lines() {
	l_example_dest_dataset=$1
	l_example_lines=""

	while IFS='	' read -r l_example_name l_example_src_guid l_example_dst_guid; do
		[ -n "$l_example_name" ] || continue
		l_example_line="  $l_example_dest_dataset@$l_example_name: source guid $l_example_src_guid vs destination guid $l_example_dst_guid"
		if [ -n "$l_example_lines" ]; then
			l_example_lines="$l_example_lines
$l_example_line"
		else
			l_example_lines=$l_example_line
		fi
	done <<-EOF
		${g_zxfer_diverged_snapshot_examples:-}
	EOF

	printf '%s\n' "$l_example_lines"
}

# Purpose: Enforce the destination divergence contract for the current dataset
# before any delete, rollback, or send is planned for it.
# Usage: Called by zxfer_inspect_delete_snap after the last common snapshot is
# known and before zxfer_delete_snaps can mutate the destination. Emits the
# per-dataset -V transparency line for every dataset. When name-match/guid-
# mismatch snapshots were recorded: with BOTH -d and -F active it prints the
# always-on convergence warning (stderr, not gated on -v/-V) and marks the
# dataset for post-receive verification; otherwise it fails closed via
# zxfer_throw_error so zero actions are taken for the diverged dataset.
zxfer_enforce_destination_divergence_contract() {
	l_divergence_source=$1

	zxfer_echoV "Last common snapshot: ${g_last_common_snap:-none}; diverged destination snapshots: ${g_zxfer_diverged_snapshot_count:-0}."

	[ "${g_zxfer_diverged_snapshot_count:-0}" -gt 0 ] || return 0
	# Planning can inspect the same dataset more than once per run (the -g
	# grandfather pre-pass runs zxfer_inspect_delete_snap before the main
	# pass); warn and count each diverged dataset only once.
	if zxfer_find_diverged_converged_marker "$g_actual_dest"; then
		return 0
	fi

	zxfer_profile_increment_counter g_zxfer_profile_diverged_snapshot_warnings
	l_diverged_example_lines=$(zxfer_render_diverged_snapshot_example_lines "$g_actual_dest")

	if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
		[ "${g_option_F_force_rollback:-}" != "" ]; then
		zxfer_warn_stderr "WARNING: destination dataset [$g_actual_dest] has ${g_zxfer_diverged_snapshot_count} snapshot(s) whose names match source dataset [$l_divergence_source] but whose guids differ (the destination diverged under identical snapshot names), e.g.:
$l_diverged_example_lines
-d and -F are active; converging: destroy + rollback + resend (destroy the diverged destination snapshots, roll back to the last guid-matching common snapshot, and resend the source range over them)."
		if [ -n "${g_zxfer_diverged_converged_datasets:-}" ]; then
			g_zxfer_diverged_converged_datasets="$g_zxfer_diverged_converged_datasets
$g_actual_dest	$l_divergence_source"
		else
			g_zxfer_diverged_converged_datasets="$g_actual_dest	$l_divergence_source"
		fi
		return 0
	fi

	zxfer_set_failure_stage "divergence reconciliation"
	zxfer_throw_error "Destination dataset [$g_actual_dest] has diverged from source dataset [$l_divergence_source]: ${g_zxfer_diverged_snapshot_count} destination snapshot(s) share a source snapshot name but carry different guids, e.g.:
$l_diverged_example_lines
No deletes or sends were planned for this dataset. Re-run with BOTH -d and -F to converge destructively (destroy the diverged destination snapshots, roll back to the last guid-matching common snapshot, and resend), or reconcile the destination manually."
}

# Purpose: Verify that a destination dataset converged this run no longer
# carries name-match/guid-mismatch snapshots after its receive completed.
# Usage: Called from the receive finalize choke points (foreground completion
# in zxfer_zfs_send_receive and -j reap-time finalize) with the received
# destination dataset. The receive already bumped the destination mutation
# generation, so the batched live view re-captures a fresh listing here. Any
# remaining divergence is a structured failure: silent re-divergence loops
# become a precise error naming the snapshot. No-ops for datasets that were
# never marked, so in-sync runs pay one string test.
zxfer_verify_converged_destination_after_receive() {
	l_verify_dest=$1

	[ -n "${g_zxfer_diverged_converged_datasets:-}" ] || return 0
	if ! zxfer_find_diverged_converged_marker "$l_verify_dest"; then
		return 0
	fi
	l_verify_source=$g_zxfer_diverged_converged_marker_source

	l_verify_records_status=0
	zxfer_capture_snapshot_records_for_dataset source "$l_verify_source" ||
		l_verify_records_status=$?
	if [ "$l_verify_records_status" -ne 0 ]; then
		zxfer_throw_error "Failed to retrieve source snapshot records for [$l_verify_source] during post-receive divergence verification." "$l_verify_records_status"
	fi
	l_verify_source_snaps=$g_zxfer_snapshot_record_capture_result
	if [ -n "$l_verify_source_snaps" ] &&
		! zxfer_snapshot_record_list_contains_guid "$l_verify_source_snaps"; then
		l_verify_identity_status=0
		l_verify_source_snaps=$(zxfer_get_snapshot_identity_records_for_dataset source "$l_verify_source" "$l_verify_source_snaps") ||
			l_verify_identity_status=$?
		if [ "$l_verify_identity_status" -ne 0 ]; then
			zxfer_throw_error "Failed to retrieve source snapshot identities for [$l_verify_source] during post-receive divergence verification." "$l_verify_identity_status"
		fi
	fi

	# At -j reap time the current dataset context may belong to another dataset,
	# so pass the completed destination explicitly instead of mutating shared
	# orchestration state.
	zxfer_ensure_live_destination_snapshot_view "$l_verify_dest"
	l_verify_dest_status=0
	l_verify_dest_snaps=$(zxfer_get_live_destination_snapshots "$l_verify_dest" 2>&1) ||
		l_verify_dest_status=$?
	if [ "$l_verify_dest_status" -ne 0 ]; then
		zxfer_throw_error "Failed to retrieve live destination snapshots for [$l_verify_dest] during post-receive divergence verification: $l_verify_dest_snaps"
	fi

	l_verify_diverged_records=$(zxfer_scan_snapshot_record_lists_for_divergence \
		"$l_verify_source_snaps" "$l_verify_dest_snaps") || :
	if [ -n "$l_verify_diverged_records" ]; then
		zxfer_record_diverged_destination_snapshots "$l_verify_diverged_records"
		l_verify_example_lines=$(zxfer_render_diverged_snapshot_example_lines "$l_verify_dest")
		zxfer_set_failure_stage "post-receive divergence verification"
		zxfer_throw_error "Destination dataset [$l_verify_dest] re-diverged after convergence: ${g_zxfer_diverged_snapshot_count} destination snapshot(s) still share a source snapshot name from [$l_verify_source] with a different guid, e.g.:
$l_verify_example_lines
An external writer is modifying the destination while zxfer converges it; stop that writer (or exclude this dataset) and re-run zxfer."
	fi

	zxfer_unmark_diverged_converged_dataset "$l_verify_dest"
	return 0
}

# Purpose: Capture the source and destination snapshot records used by one
# inspect/delete planning operation.
# Usage: Called once at the start of zxfer_inspect_delete_snap. Publishes the
# checked lists through this module's g_zxfer_inspect_* result globals and
# updates the established g_dest_has_snapshots status flag.
zxfer_capture_inspect_snapshot_record_lists() {
	l_capture_inspect_source=$1

	g_zxfer_inspect_source_snapshots_result=""
	g_zxfer_inspect_destination_snapshots_result=""

	l_capture_inspect_status=0
	zxfer_capture_snapshot_records_for_dataset source "$l_capture_inspect_source" ||
		l_capture_inspect_status=$?
	if [ "$l_capture_inspect_status" -ne 0 ]; then
		zxfer_throw_error "Failed to retrieve source snapshot records for [$l_capture_inspect_source]." "$l_capture_inspect_status"
	fi
	g_zxfer_inspect_source_snapshots_result=$g_zxfer_snapshot_record_capture_result

	l_capture_inspect_status=0
	zxfer_capture_snapshot_records_for_dataset destination "$g_actual_dest" ||
		l_capture_inspect_status=$?
	if [ "$l_capture_inspect_status" -ne 0 ]; then
		zxfer_throw_error "Failed to retrieve destination snapshot records for [$g_actual_dest]." "$l_capture_inspect_status"
	fi
	g_zxfer_inspect_destination_snapshots_result=$g_zxfer_snapshot_record_capture_result

	if [ -n "$g_zxfer_inspect_destination_snapshots_result" ]; then
		g_dest_has_snapshots=1
	else
		g_dest_has_snapshots=0
	fi
}

# Purpose: Resolve guid-bearing identity lists and classify divergence for one
# inspect/delete planning operation.
# Usage: Called after zxfer_capture_inspect_snapshot_record_lists. Publishes
# identity lists and divergence records through module-owned result globals.
zxfer_classify_inspect_snapshot_record_lists() {
	l_classify_inspect_source=$1
	l_classify_inspect_source_snapshots=$g_zxfer_inspect_source_snapshots_result
	l_classify_inspect_destination_snapshots=$g_zxfer_inspect_destination_snapshots_result

	g_zxfer_inspect_identity_source_snapshots_result=$l_classify_inspect_source_snapshots
	g_zxfer_inspect_identity_destination_snapshots_result=$l_classify_inspect_destination_snapshots
	g_zxfer_inspect_diverged_snapshot_records_result=""

	l_classify_inspect_shared_status=0
	g_zxfer_inspect_diverged_snapshot_records_result=$(zxfer_scan_snapshot_record_lists_for_divergence \
		"$l_classify_inspect_source_snapshots" "$l_classify_inspect_destination_snapshots") ||
		l_classify_inspect_shared_status=$?
	[ "$l_classify_inspect_shared_status" -eq 0 ] || return 0

	l_classify_inspect_refetched=0
	if ! zxfer_snapshot_record_list_contains_guid "$l_classify_inspect_source_snapshots"; then
		l_classify_inspect_identity_status=0
		g_zxfer_inspect_identity_source_snapshots_result=$(zxfer_get_snapshot_identity_records_for_dataset \
			source "$l_classify_inspect_source" "$l_classify_inspect_source_snapshots") ||
			l_classify_inspect_identity_status=$?
		if [ "$l_classify_inspect_identity_status" -ne 0 ]; then
			zxfer_throw_error "Failed to retrieve source snapshot identities for [$l_classify_inspect_source]." "$l_classify_inspect_identity_status"
		fi
		l_classify_inspect_refetched=1
	fi

	if ! zxfer_snapshot_record_list_contains_guid "$l_classify_inspect_destination_snapshots"; then
		l_classify_inspect_identity_status=0
		g_zxfer_inspect_identity_destination_snapshots_result=$(zxfer_get_snapshot_identity_records_for_dataset \
			destination "$g_actual_dest" "$l_classify_inspect_destination_snapshots") ||
			l_classify_inspect_identity_status=$?
		if [ "$l_classify_inspect_identity_status" -ne 0 ]; then
			zxfer_throw_error "Failed to retrieve destination snapshot identities for [$g_actual_dest]." "$l_classify_inspect_identity_status"
		fi
		l_classify_inspect_refetched=1
	fi

	# Guid-less raw records cannot be classified. Reuse the already-fetched
	# identity lists instead of issuing a second set of live queries.
	if [ "$l_classify_inspect_refetched" -eq 1 ]; then
		g_zxfer_inspect_diverged_snapshot_records_result=$(zxfer_scan_snapshot_record_lists_for_divergence \
			"$g_zxfer_inspect_identity_source_snapshots_result" \
			"$g_zxfer_inspect_identity_destination_snapshots_result") || :
	fi
}

# Purpose: Publish the last common snapshot for the current inspect/delete
# planning operation.
# Usage: Called after identity resolution and before the divergence contract.
zxfer_set_inspect_last_common_snapshot() {
	l_set_inspect_common_source=$1
	l_set_inspect_common_status=0

	g_last_common_snap=$(zxfer_get_last_common_snapshot \
		"$g_zxfer_inspect_identity_source_snapshots_result" \
		"$g_zxfer_inspect_identity_destination_snapshots_result") ||
		l_set_inspect_common_status=$?
	if [ "$l_set_inspect_common_status" -ne 0 ]; then
		zxfer_throw_error "Failed to determine the last common snapshot for [$l_set_inspect_common_source] and [$g_actual_dest]." "$l_set_inspect_common_status"
	fi
}

# Purpose: Inspect the delete snap before later delete or rollback decisions.
# Usage: Called during last-common-snapshot selection and delete planning when
# zxfer needs one focused probe before it mutates live state.
zxfer_inspect_delete_snap() {
	l_is_delete_snap=$1
	l_inspect_delete_snap_source=$2

	# shellcheck disable=SC2034
	g_did_delete_dest_snapshots=0
	# shellcheck disable=SC2034
	g_deleted_dest_newer_snapshots=0

	zxfer_capture_inspect_snapshot_record_lists "$l_inspect_delete_snap_source"
	zxfer_classify_inspect_snapshot_record_lists "$l_inspect_delete_snap_source"
	zxfer_record_diverged_destination_snapshots "$g_zxfer_inspect_diverged_snapshot_records_result"
	zxfer_set_inspect_last_common_snapshot "$l_inspect_delete_snap_source"

	# Enforce the divergence contract BEFORE any destructive planning: with
	# both -d and -F this warns and proceeds (converge); otherwise it fails
	# closed with zero actions for the diverged dataset.
	zxfer_enforce_destination_divergence_contract "$l_inspect_delete_snap_source"

	# Deletes non-common snaps on destination if asked to.
	if [ "$l_is_delete_snap" -eq 1 ]; then
		zxfer_delete_snaps \
			"$g_zxfer_inspect_identity_source_snapshots_result" \
			"$g_zxfer_inspect_identity_destination_snapshots_result" \
			"$l_inspect_delete_snap_source" || return "$?"
	fi

	# Create a list of source snapshots to transfer, beginning with the
	# first snapshot after the last common one.
	zxfer_set_src_snapshot_transfer_list "$g_zxfer_inspect_source_snapshots_result" "$l_inspect_delete_snap_source"
}
