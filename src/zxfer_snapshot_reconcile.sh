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
# owns globals: current-dataset delete/rollback scratch state such as g_last_common_snap.
# reads globals: g_actual_dest, delete scratch temp files, and current dataset context.
# mutates caches: none; updates current delete/rollback state for replication.
# returns via stdout: last-common snapshot values, delete lists, and filtered snapshot identities.

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
	zxfer_reset_destination_snapshot_creation_cache
}

# Purpose: Capture the snapshot records for dataset into staged state or module
# globals for later use.
# Usage: Called during last-common-snapshot selection and delete planning when
# later helpers need a checked snapshot of command output or computed state.
zxfer_capture_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	g_zxfer_snapshot_record_capture_result=""
	if ! zxfer_create_runtime_artifact_file "zxfer-snapshot-records" >/dev/null; then
		return 1
	fi
	l_capture_file=$g_zxfer_runtime_artifact_path_result
	if zxfer_get_snapshot_records_for_dataset "$l_side" "$l_dataset" >"$l_capture_file"; then
		:
	else
		l_capture_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return "$l_capture_status"
	fi
	if ! zxfer_read_runtime_artifact_file "$l_capture_file" >/dev/null; then
		l_capture_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_file"
		return "$l_capture_status"
	fi
	zxfer_cleanup_runtime_artifact_path "$l_capture_file"

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
# g_delete_source_tmp_file
# g_delete_dest_tmp_file
# g_delete_snapshots_to_delete_tmp_file
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
		l_status=$?
		return "$l_status"
	fi

	# Write snapshot identity keys (name + guid) to the temporary files so that
	# `comm` can distinguish same-named but unrelated snapshots.
	# run the first process in the background
	zxfer_write_snapshot_identities_to_file "$l_zfs_source_snaps" "$g_delete_source_tmp_file" &
	l_source_identity_pid=$!
	zxfer_register_cleanup_pid "$l_source_identity_pid"

	zxfer_write_snapshot_identities_to_file "$l_zfs_dest_snaps" "$g_delete_dest_tmp_file"
	l_dest_identity_status=$?

	# wait for the background process to finish
	wait "$l_source_identity_pid" 2>/dev/null
	l_source_identity_status=$?
	zxfer_unregister_cleanup_pid "$l_source_identity_pid"

	if [ "$l_dest_identity_status" -ne 0 ]; then
		zxfer_throw_error "Failed to generate destination snapshot identities for delete planning."
	fi
	if [ "$l_source_identity_status" -ne 0 ]; then
		zxfer_throw_error "Failed to generate source snapshot identities for delete planning."
	fi

	# Use comm to find snapshots in g_delete_dest_tmp_file that don't have a match in g_delete_source_tmp_file
	LC_ALL=C comm -13 "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" >"$g_delete_snapshots_to_delete_tmp_file"
	l_snapshot_diff_status=$?
	if [ "$l_snapshot_diff_status" -ne 0 ]; then
		zxfer_throw_error "Failed to diff source and destination snapshot identities for delete planning."
	fi

	l_dest_snaps_to_delete=$(zxfer_write_destination_snapshot_paths_for_identity_file "$l_zfs_dest_snaps" "$g_delete_snapshots_to_delete_tmp_file")
	l_snapshot_path_status=$?
	if [ "$l_snapshot_path_status" -ne 0 ]; then
		zxfer_throw_error "Failed to map destination snapshot identities back to snapshot paths for delete planning."
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
		l_status=$?
		return "$l_status"
	fi
	l_normalized_dest_snaps=$g_zxfer_runtime_artifact_read_result

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_dest_identity_file=$g_zxfer_temp_file_result
	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dest_identity_file"
		return "$l_status"
	fi
	l_source_snapshot_file=$g_zxfer_temp_file_result

	if zxfer_write_snapshot_identities_to_file "$l_normalized_dest_snaps" "$l_dest_identity_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_dest_identity_file" "$l_source_snapshot_file"
		return "$l_status"
	fi
	if zxfer_read_normalized_snapshot_record_list "$l_zfs_source_snaps" >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_dest_identity_file" "$l_source_snapshot_file"
		return "$l_status"
	fi
	if zxfer_write_runtime_artifact_file \
		"$l_source_snapshot_file" "$g_zxfer_runtime_artifact_read_result"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_dest_identity_file" "$l_source_snapshot_file"
		return "$l_status"
	fi

	if l_last_common_snap=$("${g_cmd_awk:-awk}" "$l_common_snapshot_awk" \
		"$l_dest_identity_file" "$l_source_snapshot_file"); then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_dest_identity_file" "$l_source_snapshot_file"
		return "$l_status"
	fi
	zxfer_cleanup_runtime_artifact_paths "$l_dest_identity_file" "$l_source_snapshot_file"

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
	l_last_common_creation=$(zxfer_get_destination_snapshot_creation_epoch "$l_last_common_dest_snapshot")
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
		l_deleted_creation=$(zxfer_get_destination_snapshot_creation_epoch "$l_deleted_snapshot_path")
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
	l_snap_date=$(zxfer_get_destination_snapshot_creation_epoch "$l_destination_snapshot")
	l_snap_date_status=$?
	if [ "$l_snap_date_status" -ne 0 ]; then
		zxfer_throw_error "Failed to query creation time for destination snapshot $l_destination_snapshot. Review prior stderr for the transport or query error."
	fi
	case "$l_snap_date" in
	'' | *[!0-9]*)
		zxfer_throw_error "Couldn't determine creation time for destination snapshot $l_destination_snapshot."
		;;
	esac

	l_diff_sec=$((l_current_date - l_snap_date))
	l_diff_day=$((l_diff_sec / 86400))

	if [ $l_diff_day -ge "$g_option_g_grandfather_protection" ]; then
		l_snap_date_english=$(zxfer_format_snapshot_creation_epoch_for_display "$l_snap_date")
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

	zxfer_reset_destination_snapshot_creation_cache
	if ! zxfer_prefetch_delete_snapshot_creation_times "$l_snaps_to_delete" >/dev/null; then
		zxfer_throw_error "Failed to query destination snapshot creation times while planning snapshot deletions. Review prior stderr for the transport or query error."
	fi

	g_deleted_dest_newer_snapshots=0
	zxfer_deleted_snapshots_include_newer_than_last_common "$l_snaps_to_delete"
	l_deleted_newer_status=$?
	if [ "$l_deleted_newer_status" -eq 2 ]; then
		zxfer_throw_error "Failed to query destination snapshot creation times while evaluating rollback eligibility. Review prior stderr for the transport or query error."
	fi
	if [ "$l_deleted_newer_status" -eq 0 ]; then
		g_deleted_dest_newer_snapshots=1
	fi

	l_unprotected_snaps_to_delete=""

	# checks if any of the snapshots to delete are protected by the grandfather option
	while IFS= read -r l_snap_to_delete; do
		[ -n "$l_snap_to_delete" ] || continue
		if [ "$g_option_g_grandfather_protection" != "" ]; then
			zxfer_grandfather_test "$l_snap_to_delete"
		fi

		l_snapshot=$(zxfer_extract_snapshot_name "$l_snap_to_delete")

		# prepend this snapshot to the list of snapshots to delete in a comma
		# delimited list; the trailing comma is trimmed before issuing zfs destroy.
		l_unprotected_snaps_to_delete="$l_snapshot,$l_unprotected_snaps_to_delete"
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_snaps_to_delete")
	EOF

	# drop any trailing delimiter so the destroy command receives valid names
	l_unprotected_snaps_to_delete=${l_unprotected_snaps_to_delete%,}

	# get the dataset name from the first snapshot in the list
	#
	# - get the first element of the list
	# - get the portion of the string prior to the @ symbol
	# shellcheck disable=SC2016
	l_zfs_dest_dataset=$(echo "$l_snaps_to_delete" | head -n 1 | "$g_cmd_awk" -F'@' '{print $1}')

	# build the destroy command
	l_destroy_target="$l_zfs_dest_dataset@$l_unprotected_snaps_to_delete"
	l_cmd=$(zxfer_render_destination_zfs_command destroy "$l_destroy_target")
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $l_cmd"
		return
	fi

	g_did_delete_dest_snapshots=1
	if ! zxfer_run_destination_zfs_cmd destroy "$l_destroy_target"; then
		zxfer_throw_error "Error when executing command."
	fi

	# set the flag to indicate that a destroy command was sent
	# shellcheck disable=SC2034
	g_is_performed_send_destroy=1

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

# Purpose: Inspect the delete snap before later delete or rollback decisions.
# Usage: Called during last-common-snapshot selection and delete planning when
# zxfer needs one focused probe before it mutates live state.
zxfer_inspect_delete_snap() {
	l_is_delete_snap=$1
	l_source=$2

	# shellcheck disable=SC2034
	g_did_delete_dest_snapshots=0
	# shellcheck disable=SC2034
	g_deleted_dest_newer_snapshots=0

	# Get only the snapshots for the exact source dataset in descending order
	# by creation date.
	if ! zxfer_capture_snapshot_records_for_dataset source "$l_source"; then
		zxfer_throw_error "Failed to retrieve source snapshot records for [$l_source]."
	fi
	l_zfs_source_snaps=$g_zxfer_snapshot_record_capture_result

	# Get the list of destination snapshots for the matching destination dataset.
	if ! zxfer_capture_snapshot_records_for_dataset destination "$g_actual_dest"; then
		zxfer_throw_error "Failed to retrieve destination snapshot records for [$g_actual_dest]."
	fi
	l_zfs_dest_snaps=$g_zxfer_snapshot_record_capture_result
	l_identity_source_snaps=$l_zfs_source_snaps
	l_identity_dest_snaps=$l_zfs_dest_snaps
	if [ -n "$l_zfs_dest_snaps" ]; then
		# shellcheck disable=SC2034
		# consumed by zxfer_replication.sh for status checks.
		g_dest_has_snapshots=1
	else
		# shellcheck disable=SC2034
		# consumed by zxfer_replication.sh for status checks.
		g_dest_has_snapshots=0
	fi

	if zxfer_snapshot_record_lists_share_snapshot_name "$l_zfs_source_snaps" "$l_zfs_dest_snaps"; then
		if ! zxfer_snapshot_record_list_contains_guid "$l_zfs_source_snaps"; then
			if ! l_identity_source_snaps=$(zxfer_get_snapshot_identity_records_for_dataset source "$l_source" "$l_zfs_source_snaps"); then
				zxfer_throw_error "Failed to retrieve source snapshot identities for [$l_source]."
			fi
		fi

		if ! zxfer_snapshot_record_list_contains_guid "$l_zfs_dest_snaps"; then
			if ! l_identity_dest_snaps=$(zxfer_get_snapshot_identity_records_for_dataset destination "$g_actual_dest" "$l_zfs_dest_snaps"); then
				zxfer_throw_error "Failed to retrieve destination snapshot identities for [$g_actual_dest]."
			fi
		fi
	fi

	# Find the most recent common snapshot on source and destination.
	if ! g_last_common_snap=$(zxfer_get_last_common_snapshot "$l_identity_source_snaps" "$l_identity_dest_snaps"); then
		zxfer_throw_error "Failed to determine the last common snapshot for [$l_source] and [$g_actual_dest]."
	fi

	# Deletes non-common snaps on destination if asked to.
	if [ "$l_is_delete_snap" -eq 1 ]; then
		zxfer_delete_snaps "$l_identity_source_snaps" "$l_identity_dest_snaps"
	fi

	# Create a list of source snapshots to transfer, beginning with the
	# first snapshot after the last common one.
	zxfer_set_src_snapshot_transfer_list "$l_zfs_source_snaps" "$l_source"
}
