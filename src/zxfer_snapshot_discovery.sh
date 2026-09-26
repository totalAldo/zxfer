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
# SNAPSHOT DISCOVERY ORCHESTRATION / DIFF / STATE
################################################################################

# Module contract:
# owns globals: recursive snapshot-discovery state, full-discovery and fast
#   no-op operation state, the destination listing a declined fast proof hands
#   to full discovery, staged record-cache paths, and recursive dataset results.
# reads globals: discovery options, source/destination context, producer and
#   remote-batch result channels, and profiling state.
# mutates caches: the destination-existence cache through shared helpers.
# returns via stdout: reversed record files and the -v/-V delta report; drives
#   the fast no-op and full source/destination discovery protocols.

# Purpose: Reset the file-backed state for one full snapshot-discovery operation.
# Usage: zxfer_reset_full_snapshot_discovery_operation_state, before full
# discovery starts and from the module reset; assignments only.
zxfer_reset_full_snapshot_discovery_operation_state() {
	g_zxfer_full_source_snapshot_stage_files=""
	g_zxfer_full_source_snapshot_file=""
	g_zxfer_full_source_snapshot_error_file=""
	g_zxfer_full_source_snapshot_sorted_file=""
	g_zxfer_full_source_snapshot_stage_start_ms=""
	g_zxfer_full_destination_snapshot_file=""
	g_zxfer_full_destination_snapshot_sorted_file=""
	g_zxfer_full_destination_snapshot_error_file=""
	g_zxfer_full_destination_inventory_attempted=0
	g_zxfer_full_remote_destination_inventory_stage_files=""
	g_zxfer_full_remote_destination_list_file=""
	g_zxfer_full_remote_destination_list_error_file=""
	g_zxfer_full_remote_destination_failure_error_file=""
}

# Purpose: Reset the owned scratch for one fast recursive no-op proof attempt.
# Usage: zxfer_reset_fast_recursive_noop_discovery_operation_state; called
# before allocation and by the module reset path.
zxfer_reset_fast_recursive_noop_discovery_operation_state() {
	g_zxfer_snapshot_discovery_fast_noop_stage_files=""
	g_zxfer_snapshot_discovery_fast_noop_source_stream_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_stream_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_error_file=""
	g_zxfer_snapshot_discovery_fast_noop_compare_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_count_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_error_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_status_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_list_status=""
	g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status=""
	g_zxfer_snapshot_discovery_fast_noop_destination_sort_status=""
	g_zxfer_snapshot_discovery_fast_noop_destination_raw_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_pid=""
	g_zxfer_snapshot_discovery_fast_noop_destination_pid=""
	g_zxfer_snapshot_discovery_fast_noop_source_wait_status=0
	g_zxfer_snapshot_discovery_fast_noop_destination_wait_status=0
	g_zxfer_snapshot_discovery_fast_noop_missing_destination=0
	g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms=""
	g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms=""
}

# Purpose: Clean up one fast recursive no-op proof attempt and clear its scratch.
# Usage: zxfer_cleanup_fast_recursive_noop_discovery_operation_state; called on
# every post-allocation terminal path. A raw destination listing handed to
# full discovery is kept.
zxfer_cleanup_fast_recursive_noop_discovery_operation_state() {
	l_fast_noop_cleanup_stage_files=${g_zxfer_snapshot_discovery_fast_noop_stage_files:-}
	l_fast_noop_cleanup_raw_file=${g_zxfer_snapshot_discovery_fast_noop_destination_raw_file:-}
	if [ -n "$l_fast_noop_cleanup_raw_file" ] &&
		[ "$l_fast_noop_cleanup_raw_file" != "${g_zxfer_snapshot_discovery_destination_listing_file:-}" ]; then
		l_fast_noop_cleanup_stage_files=$l_fast_noop_cleanup_stage_files$ZXFER_LF$l_fast_noop_cleanup_raw_file
	fi
	if [ -n "$l_fast_noop_cleanup_stage_files" ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_noop_cleanup_stage_files"
	fi
	zxfer_reset_fast_recursive_noop_discovery_operation_state
}

# Purpose: Remove the staged snapshot record files and forget them.
# Usage: zxfer_cleanup_snapshot_record_cache_files, before a discovery pass and
# on its failure paths.
zxfer_cleanup_snapshot_record_cache_files() {
	if [ -n "${g_zxfer_source_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_source_snapshot_record_cache_file"
	fi
	if [ -n "${g_zxfer_destination_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_destination_snapshot_record_cache_file"
	fi
	if [ -n "${g_source_snapshot_list_sorted_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_source_snapshot_list_sorted_file"
	fi

	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_source_snapshot_list_sorted_file=""
}

# Purpose: Reset the snapshot discovery state so the next pass starts clean.
# Usage: zxfer_reset_snapshot_discovery_state; called at the start of each
# discovery pass and by session and dry-run resets.
zxfer_reset_snapshot_discovery_state() {
	zxfer_cleanup_snapshot_record_cache_files
	zxfer_reset_full_snapshot_discovery_operation_state
	zxfer_reset_fast_recursive_noop_discovery_operation_state
	zxfer_reset_snapshot_producer_state
	g_source_snapshot_list_background_sort_requested=0
	zxfer_reset_recursive_dataset_lists
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_recursive_dataset_list_result=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
	zxfer_reset_destination_discovery_batch_state
}

# Purpose: List the local destination's datasets and publish them as the
# recursive destination inventory.
# Usage: zxfer_collect_local_destination_dataset_inventory; called after the
# snapshot diff when later work reads the destination existence cache.
zxfer_collect_local_destination_dataset_inventory() {
	zxfer_create_temp_file_group 2 || return "$?"
	l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dest_list_tmp_file
		IFS= read -r l_dest_list_err_file
	} <<-EOF
		$l_destination_inventory_stage_files
	EOF

	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")"
	else
		zxfer_record_last_command_opaque
	fi
	l_dest_inventory_status=0
	zxfer_run_destination_zfs_cmd list -t filesystem,volume -Hr -o name "$g_destination" >"$l_dest_list_tmp_file" 2>"$l_dest_list_err_file" ||
		l_dest_inventory_status=$?

	l_status=0
	zxfer_publish_destination_dataset_inventory_from_stage \
		"$l_dest_list_tmp_file" \
		"$l_dest_list_err_file" \
		"$l_dest_inventory_status" ||
		l_status=$?
	zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
	return "$l_status"
}

# Purpose: Copy a snapshot record file without the records whose dataset
# matches the -x pattern.
# Usage: zxfer_filter_snapshot_file_with_excludes INPUT OUTPUT
zxfer_filter_snapshot_file_with_excludes() {
	"${g_cmd_awk:-awk}" -v "exclude_pattern=${g_option_x_exclude_datasets:-}" \
		"$ZXFER_SNAPSHOT_RECORD_AWK" "$1" >"$2"
}

# Purpose: Print a file's lines in reverse order.
# Usage: zxfer_reverse_file_lines FILE; a file longer than
# g_zxfer_linear_reverse_max_lines lines (default 50000) is reversed with a
# numbered sort instead, so awk memory stays bounded.
zxfer_reverse_file_lines() {
	l_reverse_max_lines=${g_zxfer_linear_reverse_max_lines:-50000}
	zxfer_is_uint "$l_reverse_max_lines" || l_reverse_max_lines=0

	l_reverse_status=0
	# Exits 3 without printing as soon as the file outgrows the limit.
	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" -v "max_lines=$l_reverse_max_lines" '
		NR > max_lines + 0 { too_long = 1; exit 3 }
		{ line[NR] = $0 }
		END { if (!too_long) for (i = NR; i > 0; i--) print line[i] }' "$1" ||
		l_reverse_status=$?
	[ "$l_reverse_status" -eq 3 ] || return "$l_reverse_status"

	zxfer_get_temp_file || return "$?"
	l_reverse_numbered_file=$g_zxfer_temp_file_result
	l_reverse_status=0
	cat -n "$1" >"$l_reverse_numbered_file" || l_reverse_status=$?
	if [ "$l_reverse_status" -eq 0 ]; then
		# Sort on the cat -n line number, last line first, then drop it.
		LC_ALL=C sort -nr "$l_reverse_numbered_file" | cut -f2- || l_reverse_status=$?
	fi
	zxfer_cleanup_runtime_artifact_path "$l_reverse_numbered_file"
	return "$l_reverse_status"
}

# Purpose: Publish the sorted, unique dataset names of a snapshot record file.
# Usage: zxfer_capture_recursive_dataset_list_from_snapshot_file FILE SCRATCH;
# sets g_zxfer_recursive_dataset_list_result and overwrites SCRATCH.
zxfer_capture_recursive_dataset_list_from_snapshot_file() {
	g_zxfer_recursive_dataset_list_result=""
	# A record's dataset is everything before its first @.
	# shellcheck disable=SC2016  # awk program should see literal $1.
	"${g_cmd_awk:-awk}" -F@ '$1 != "" { print $1 }' "$1" >"$2" || return "$?"
	l_capture_dataset_list=$(LC_ALL=C sort -u "$2") || return "$?"
	g_zxfer_recursive_dataset_list_result=$l_capture_dataset_list
}

# Purpose: Drop the datasets that match the -x pattern from a dataset list.
# Usage: zxfer_filter_recursive_dataset_list_with_excludes LIST; sets
# g_zxfer_recursive_dataset_list_result, or returns grep's error status.
zxfer_filter_recursive_dataset_list_with_excludes() {
	g_zxfer_recursive_dataset_list_result=$1
	[ -n "$1" ] && [ -n "${g_option_x_exclude_datasets:-}" ] || return 0

	g_zxfer_recursive_dataset_list_result=""
	l_filter_status=0
	l_filter_dataset_list=$(
		grep -v -e "$g_option_x_exclude_datasets" <<-EOF
			$1
		EOF
	) || l_filter_status=$?
	# grep exits 1 when it drops every dataset; 2 is a real failure.
	[ "$l_filter_status" -le 1 ] || return "$l_filter_status"
	g_zxfer_recursive_dataset_list_result=$l_filter_dataset_list
}

# Purpose: Publish the -x filtered dataset names of one delta record file.
# Usage: zxfer_capture_delta_dataset_list FILE SCRATCH LABEL; sets
# g_zxfer_recursive_dataset_list_result, empty for an empty FILE. Failures
# throw "Failed to derive LABEL." or "Failed to filter LABEL against exclude
# patterns.".
zxfer_capture_delta_dataset_list() {
	g_zxfer_recursive_dataset_list_result=""
	[ -s "$1" ] || return 0
	zxfer_capture_recursive_dataset_list_from_snapshot_file "$1" "$2" ||
		zxfer_throw_error "Failed to derive $3." "$?"
	zxfer_filter_recursive_dataset_list_with_excludes "$g_zxfer_recursive_dataset_list_result" ||
		zxfer_throw_error "Failed to filter $3 against exclude patterns." "$?"
}

# Purpose: Decide whether recursive source dataset inventory must be derived
# from the full source snapshot list.
# Usage: zxfer_snapshot_discovery_needs_source_dataset_inventory; returns 1
# for a recursive run with no property work (-P, -o, or -U with source
# datasets to scan), which never reads the inventory.
zxfer_snapshot_discovery_needs_source_dataset_inventory() {
	if [ "${g_option_R_recursive:-}" = "" ]; then
		return 0
	fi
	if [ "${g_option_P_transfer_property:-0}" -eq 1 ] ||
		[ -n "${g_option_o_override_property:-}" ]; then
		return 0
	fi
	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ] &&
		[ -n "${g_recursive_source_list:-}" ]; then
		return 0
	fi

	return 1
}

# Purpose: Split two sorted record files into the records only the source has
# and the records only the destination has.
# Usage: zxfer_write_snapshot_delta_files SOURCE DESTINATION MISSING EXTRA
# SCRATCH; writes MISSING and EXTRA in sorted order and overwrites SCRATCH.
zxfer_write_snapshot_delta_files() {
	LC_ALL=C comm -3 "$1" "$2" >"$5" || return "$?"
	# comm -3 indents destination-only records with one tab. A snapshot name
	# cannot start with a tab, so dropping exactly one keeps the record.
	# shellcheck disable=SC2016  # awk program should see literal $0.
	ZXFER_AWK_DELTA_EXTRA_FILE=$4 "${g_cmd_awk:-awk}" '
		substr($0, 1, 1) == "\t" {
			print substr($0, 2) > (ENVIRON["ZXFER_AWK_DELTA_EXTRA_FILE"])
			next
		}
		{ print }' "$5" >"$3"
}

# Purpose: Print the recursive delta summary under -v, and the delta records
# as well under -V.
# Usage: zxfer_report_recursive_snapshot_delta MISSING EXTRA; call once the
# recursive dataset lists are final.
zxfer_report_recursive_snapshot_delta() {
	l_report_delta_missing_file=$1
	l_report_delta_extra_file=$2

	if [ "${g_option_v_verbose:-0}" -ne 1 ] && [ "${g_option_V_very_verbose:-0}" -ne 1 ]; then
		return 0
	fi
	# One pass counts the records of both delta files, then the datasets of
	# both lists from stdin, split by a lone "@" (never a dataset name). The
	# lists go through a here-doc: argv and environment strings are
	# size-limited, and a large tree would overflow them.
	# shellcheck disable=SC2016  # awk program should see literal ARGV/FILENAME.
	l_report_delta_counts=$(
		"${g_cmd_awk:-awk}" '
		FILENAME == ARGV[1] { missing++; next }
		FILENAME == ARGV[2] { extra++; next }
		$0 == "@" { in_extra_list = 1; next }
		NF { if (in_extra_list) extra_datasets++; else source_datasets++ }
		END { print missing + 0, extra + 0, source_datasets + 0, extra_datasets + 0 }' \
			"$l_report_delta_missing_file" "$l_report_delta_extra_file" - <<-EOF
				$g_recursive_source_list
				@
				$g_recursive_destination_extra_dataset_list
			EOF
	) || zxfer_throw_error "Failed to count the recursive snapshot delta." "$?"
	read -r l_report_delta_missing_count l_report_delta_extra_count \
		l_report_delta_source_dataset_count l_report_delta_extra_dataset_count <<-EOF
			$l_report_delta_counts
		EOF

	if [ "${l_report_delta_missing_count:-0}" -gt 0 ] || [ "${l_report_delta_extra_count:-0}" -gt 0 ]; then
		zxfer_echov "Recursive snapshot delta summary: source_missing_snapshots=$l_report_delta_missing_count destination_extra_snapshots=$l_report_delta_extra_count source_datasets=$l_report_delta_source_dataset_count destination_extra_datasets=$l_report_delta_extra_dataset_count"
		if [ -n "$g_recursive_source_list" ]; then
			zxfer_echov "Recursive source datasets queued for transfer:"
			printf '%s\n' "$g_recursive_source_list" | while IFS= read -r l_report_delta_source_dataset; do
				[ -n "$l_report_delta_source_dataset" ] || continue
				zxfer_echov "  $l_report_delta_source_dataset"
			done
		fi
		if [ -n "$g_recursive_destination_extra_dataset_list" ]; then
			zxfer_echov "Recursive destination datasets queued for delete inspection:"
			printf '%s\n' "$g_recursive_destination_extra_dataset_list" | while IFS= read -r l_report_delta_destination_dataset; do
				[ -n "$l_report_delta_destination_dataset" ] || continue
				zxfer_echov "  $l_report_delta_destination_dataset"
			done
		fi
	fi

	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	echo "====================================================================="
	echo "====== Snapshots present in source but missing in destination ======"
	[ -s "$l_report_delta_missing_file" ] && cat "$l_report_delta_missing_file"
	echo "====== Source datasets that differ from destination ======"
	echo "g_recursive_source_list:"
	echo "$g_recursive_source_list"
	echo "Source dataset count: $l_report_delta_source_dataset_count"
	echo "====================================================================="
	echo "====== Extra Destination snapshots not in source ======"
	[ -s "$l_report_delta_extra_file" ] && cat "$l_report_delta_extra_file"
	echo "====== Destination datasets with extra snapshots not in source ======"
	if [ "$g_recursive_destination_extra_dataset_list" != "" ]; then
		printf '%s\n' "$g_recursive_destination_extra_dataset_list"
	fi
	echo "====================================================================="
}

# Purpose: Diff the source and destination snapshot listings and publish the
# recursive work lists.
# Usage: zxfer_set_g_recursive_source_list RAW_SOURCE SORTED_DESTINATION
# [SORTED_SOURCE]; RAW_SOURCE is sorted here unless SORTED_SOURCE is given.
# Publishes g_recursive_source_list, g_recursive_destination_extra_dataset_list
# and, when later work needs it, g_recursive_source_dataset_list, all without
# -x matches. Failures throw.
zxfer_set_g_recursive_source_list() {
	l_delta_raw_source_file=$1
	l_delta_destination_file=$2
	l_delta_source_file=${3:-}

	zxfer_create_temp_file_group 6 || return "$?"
	l_delta_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_delta_sorted_source_file
		IFS= read -r l_delta_missing_file
		IFS= read -r l_delta_extra_file
		IFS= read -r l_delta_filtered_source_file
		IFS= read -r l_delta_filtered_destination_file
		IFS= read -r l_delta_scratch_file
	} <<-EOF
		$l_delta_stage_files
	EOF

	if [ -z "$l_delta_source_file" ]; then
		l_delta_source_file=$l_delta_sorted_source_file
		if zxfer_command_trace_enabled; then
			zxfer_trace_rendered_command "Running command" \
				"$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_delta_raw_source_file") > $(zxfer_quote_token_for_report "$l_delta_source_file")"
		else
			zxfer_record_last_command_opaque
		fi
		LC_ALL=C sort "$l_delta_raw_source_file" >"$l_delta_source_file" ||
			zxfer_throw_error "Failed to sort source snapshots for recursive delta planning." "$?"
	elif [ ! -f "$l_delta_source_file" ]; then
		zxfer_throw_error "Failed to locate staged sorted source snapshots for recursive delta planning."
	fi

	# -x first drops excluded records, so differences confined to excluded
	# datasets cannot keep a run from comparing as a no-op.
	if [ -n "${g_option_x_exclude_datasets:-}" ]; then
		zxfer_filter_snapshot_file_with_excludes "$l_delta_source_file" "$l_delta_filtered_source_file" ||
			zxfer_throw_error "Failed to filter source snapshots against exclude patterns for recursive delta planning." "$?"
		zxfer_filter_snapshot_file_with_excludes "$l_delta_destination_file" "$l_delta_filtered_destination_file" ||
			zxfer_throw_error "Failed to filter destination snapshots against exclude patterns for recursive delta planning." "$?"
		l_delta_source_file=$l_delta_filtered_source_file
		l_delta_destination_file=$l_delta_filtered_destination_file
	fi

	# Equal listings leave both freshly allocated delta files empty.
	l_delta_compare_status=0
	cmp -s "$l_delta_source_file" "$l_delta_destination_file" || l_delta_compare_status=$?
	case $l_delta_compare_status in
	0) ;;
	1)
		zxfer_write_snapshot_delta_files "$l_delta_source_file" "$l_delta_destination_file" \
			"$l_delta_missing_file" "$l_delta_extra_file" "$l_delta_scratch_file" ||
			zxfer_throw_error "Failed to diff source and destination snapshots for recursive delta planning." "$?"
		;;
	*)
		zxfer_throw_error "Failed to compare source and destination snapshots for recursive delta planning." "$l_delta_compare_status"
		;;
	esac

	zxfer_capture_delta_dataset_list "$l_delta_missing_file" "$l_delta_scratch_file" \
		"recursive source dataset transfer list"
	g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	zxfer_capture_delta_dataset_list "$l_delta_extra_file" "$l_delta_scratch_file" \
		"recursive destination dataset delete list"
	g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	g_recursive_source_dataset_list=""
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		zxfer_capture_delta_dataset_list "$l_delta_source_file" "$l_delta_scratch_file" \
			"recursive source dataset inventory"
		g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
	fi

	zxfer_report_recursive_snapshot_delta "$l_delta_missing_file" "$l_delta_extra_file"
	if [ "$g_recursive_source_list" = "" ]; then
		zxfer_echov "No new snapshots to transfer."
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
}

# Purpose: Decide whether snapshot discovery must keep per-dataset record
# caches for later replication work.
# Usage: Called after recursive snapshot diffing has populated the dataset
# work lists, before discovery decides whether to carry large snapshot
# inventories forward.
zxfer_snapshot_discovery_needs_record_caches() {
	if [ "${g_option_R_recursive:-}" = "" ]; then
		return 0
	fi
	if [ -n "${g_recursive_source_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
		[ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_P_transfer_property:-0}" -eq 1 ] ||
		[ -n "${g_option_o_override_property:-}" ]; then
		return 0
	fi

	return 1
}

# Purpose: Decide whether local recursive destination dataset inventory should
# be collected after snapshot diffing.
# Usage: Called after recursive snapshot deltas are known so no-op runs avoid
# building a destination existence cache that no later stage can consume.
zxfer_snapshot_discovery_needs_destination_dataset_inventory() {
	if [ -n "${g_option_T_target_host:-}" ]; then
		return 1
	fi
	if [ -n "${g_recursive_source_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
		[ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_P_transfer_property:-0}" -eq 1 ] ||
		[ -n "${g_option_o_override_property:-}" ]; then
		return 0
	fi

	return 1
}

# Purpose: Allocate the staged files for one fast recursive no-op proof attempt.
# Usage: zxfer_allocate_fast_recursive_noop_discovery_stages; called after
# eligibility succeeds and before either producer starts.
# Side effects: Publishes the seven stage paths and, outside that group, the
# raw destination listing path that full discovery may take over.
zxfer_allocate_fast_recursive_noop_discovery_stages() {
	zxfer_reset_fast_recursive_noop_discovery_operation_state
	zxfer_get_temp_file || return "$?"
	g_zxfer_snapshot_discovery_fast_noop_destination_raw_file=$g_zxfer_temp_file_result
	zxfer_create_temp_file_group 7 || return "$?"
	g_zxfer_snapshot_discovery_fast_noop_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_stream_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_stream_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_error_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_compare_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_count_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_error_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_status_file
	} <<-EOF
		$g_zxfer_snapshot_discovery_fast_noop_stage_files
	EOF
	return 0
}

# Purpose: Start the source producer for a fast recursive no-op proof attempt.
# Usage: zxfer_start_fast_recursive_noop_source_discovery; called after stage
# allocation. Publishes the producer PID and keeps the rendered command in
# g_source_snapshot_list_cmd for failure reports.
zxfer_start_fast_recursive_noop_source_discovery() {
	g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	zxfer_build_source_snapshot_name_list_cmd || {
		l_fast_noop_source_start_status=$?
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_source_start_status"
	}
	l_fast_noop_source_start_command=$g_zxfer_source_snapshot_list_cmd_result

	l_fast_noop_source_start_uses_parallel=0
	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		l_fast_noop_source_start_uses_parallel=1
	fi
	g_source_snapshot_list_cmd=$l_fast_noop_source_start_command
	zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_commands
	if [ "$l_fast_noop_source_start_uses_parallel" -eq 1 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
	fi
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	# Stage into regular temp files. FIFO comparisons can strand a producer
	# when the compare command exits or cannot open both streams.
	zxfer_execute_source_snapshot_name_list_background_sort_cmd \
		"$l_fast_noop_source_start_command" \
		"$g_zxfer_snapshot_discovery_fast_noop_source_stream_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_source_error_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_source_count_file" || {
		l_fast_noop_source_start_status=$?
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_source_start_status"
	}
	g_zxfer_snapshot_discovery_fast_noop_source_pid=$g_last_background_pid
	return 0
}

# Purpose: Start the destination producer for a fast recursive no-op proof.
# Usage: zxfer_start_fast_recursive_noop_destination_discovery; called after
# the source producer so both listings overlap. If this start fails, it stops
# and reaps the source producer and returns the start status.
zxfer_start_fast_recursive_noop_destination_discovery() {
	g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	zxfer_start_destination_snapshot_name_sorted_fifo_producer \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stream_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_error_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_status_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_raw_file" || {
		l_fast_noop_destination_start_status=$?
		l_fast_noop_source_pid=$g_zxfer_snapshot_discovery_fast_noop_source_pid
		# Signal the source producer only while it is unreaped: TERM, a grace
		# period for stages that ignore it, KILL, then wait. A failed signal
		# leaves its scope registered for trap cleanup. The destination setup
		# status is the one reported.
		if zxfer_abort_fast_noop_background_pid "$l_fast_noop_source_pid" \
			"background source snapshot no-op proof helper"; then
			zxfer_cleanup_pid_abort_grace_wait
			if zxfer_abort_cleanup_pid "$l_fast_noop_source_pid" KILL; then
				wait "$l_fast_noop_source_pid" 2>/dev/null || :
				zxfer_unregister_cleanup_pid "$l_fast_noop_source_pid"
				g_last_background_pid=""
			fi
		fi
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_destination_start_status"
	}
	g_zxfer_snapshot_discovery_fast_noop_destination_pid=$g_last_background_pid
	return 0
}

# Purpose: Stop the pipeline stages a failed source producer left in its
# process group after wait reaped the producer.
# Usage: zxfer_kill_reaped_producer_group PID
zxfer_kill_reaped_producer_group() {
	# Never signal a reaped PID: its number may already belong to an unrelated
	# process. A process group outlives its leader, so a pgid-scoped producer's
	# group is still reachable. Stages that a wrapper-scoped producer left
	# behind were reparented and cannot be stopped from here.
	zxfer_find_cleanup_pid_record "$1" || return 0
	[ "$g_zxfer_cleanup_pid_record_scope" = pgid ] || return 0
	zxfer_signal_process_group KILL "$1" || :
}

# Purpose: Wait for both fast recursive no-op proof producers.
# Usage: zxfer_wait_for_fast_recursive_noop_discovery; called after both
# producers start. Records each wait status, source first.
zxfer_wait_for_fast_recursive_noop_discovery() {
	g_zxfer_snapshot_discovery_fast_noop_source_wait_status=0
	wait "$g_zxfer_snapshot_discovery_fast_noop_source_pid" ||
		g_zxfer_snapshot_discovery_fast_noop_source_wait_status=$?
	[ "$g_zxfer_snapshot_discovery_fast_noop_source_wait_status" -eq 0 ] ||
		zxfer_kill_reaped_producer_group "$g_zxfer_snapshot_discovery_fast_noop_source_pid"
	zxfer_unregister_cleanup_pid "$g_zxfer_snapshot_discovery_fast_noop_source_pid"
	zxfer_profile_add_elapsed_ms \
		g_zxfer_profile_source_snapshot_listing_ms \
		"$g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms"

	g_zxfer_snapshot_discovery_fast_noop_destination_wait_status=0
	wait "$g_zxfer_snapshot_discovery_fast_noop_destination_pid" ||
		g_zxfer_snapshot_discovery_fast_noop_destination_wait_status=$?
	zxfer_unregister_cleanup_pid "$g_zxfer_snapshot_discovery_fast_noop_destination_pid"
	g_last_background_pid=""
	zxfer_profile_add_elapsed_ms \
		g_zxfer_profile_destination_snapshot_listing_ms \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms"
	return 0
}

# Purpose: Read the fast proof destination producer's status line.
# Usage: zxfer_read_fast_recursive_noop_destination_statuses; sets the
# g_zxfer_snapshot_discovery_fast_noop_destination_{list,normalize,sort}_status
# globals from its "LIST NORMALIZE SORT" line, and returns 1 unless all three
# are present and numeric.
zxfer_read_fast_recursive_noop_destination_statuses() {
	g_zxfer_snapshot_discovery_fast_noop_destination_list_status=""
	g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status=""
	g_zxfer_snapshot_discovery_fast_noop_destination_sort_status=""
	[ -f "$g_zxfer_snapshot_discovery_fast_noop_destination_status_file" ] || return 1
	read -r g_zxfer_snapshot_discovery_fast_noop_destination_list_status \
		g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status \
		g_zxfer_snapshot_discovery_fast_noop_destination_sort_status \
		<"$g_zxfer_snapshot_discovery_fast_noop_destination_status_file" || :
	zxfer_is_uint "$g_zxfer_snapshot_discovery_fast_noop_destination_list_status" &&
		zxfer_is_uint "$g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status" &&
		zxfer_is_uint "$g_zxfer_snapshot_discovery_fast_noop_destination_sort_status"
}

# Purpose: Compare the completed fast recursive no-op proof streams.
# Usage: zxfer_compare_fast_recursive_noop_discovery_streams; called before
# sidecar validation, so a real delta declines the proof (returns 1) at once.
zxfer_compare_fast_recursive_noop_discovery_streams() {
	l_fast_noop_compare_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_fast_noop_compare_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	# Any line comm prints is a snapshot that differs.
	l_fast_noop_compare_status=0
	if LC_ALL=C comm -3 \
		"$g_zxfer_snapshot_discovery_fast_noop_source_stream_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stream_file" \
		>"$g_zxfer_snapshot_discovery_fast_noop_compare_file"; then
		if [ -s "$g_zxfer_snapshot_discovery_fast_noop_compare_file" ]; then
			l_fast_noop_compare_status=1
		fi
	else
		l_fast_noop_compare_status=$?
	fi
	zxfer_profile_add_elapsed_ms \
		g_zxfer_profile_snapshot_diff_sort_ms \
		"$l_fast_noop_compare_stage_start_ms"

	if [ "$l_fast_noop_compare_status" -ne 0 ]; then
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		if [ "$l_fast_noop_compare_status" -eq 1 ]; then
			zxfer_reset_destination_existence_cache
			return 1
		fi
		zxfer_throw_error \
			"Failed to compare source and destination snapshots for recursive no-op proof." \
			"$l_fast_noop_compare_status"
	fi
	return 0
}

# Purpose: Validate the destination producer's statuses for a fast no-op proof.
# Usage: zxfer_validate_fast_recursive_noop_destination_discovery; called after
# the streams compare equal. Sets
# g_zxfer_snapshot_discovery_fast_noop_missing_destination when the listing
# reported the destination missing.
zxfer_validate_fast_recursive_noop_destination_discovery() {
	zxfer_read_fast_recursive_noop_destination_statuses || {
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		zxfer_throw_error \
			"Failed to validate destination snapshot status for recursive no-op proof." 1
	}

	g_zxfer_snapshot_discovery_fast_noop_missing_destination=0
	l_fast_noop_destination_list_status=$g_zxfer_snapshot_discovery_fast_noop_destination_list_status
	if [ "$l_fast_noop_destination_list_status" -ne 0 ]; then
		zxfer_read_snapshot_discovery_capture_file \
			"$g_zxfer_snapshot_discovery_fast_noop_destination_error_file" || {
			l_fast_noop_destination_error_read_status=$?
			zxfer_cleanup_fast_recursive_noop_discovery_operation_state
			zxfer_throw_error \
				"Failed to read staged destination snapshot stderr." \
				"$l_fast_noop_destination_error_read_status"
		}
		l_fast_noop_destination_error=$g_zxfer_snapshot_discovery_file_read_result
		if zxfer_destination_probe_reports_missing "$l_fast_noop_destination_error"; then
			g_zxfer_snapshot_discovery_fast_noop_missing_destination=1
		else
			if [ -n "$l_fast_noop_destination_error" ]; then
				printf '%s\n' "$l_fast_noop_destination_error" >&2
			fi
			zxfer_cleanup_fast_recursive_noop_discovery_operation_state
			zxfer_throw_error \
				"Failed to retrieve snapshot list from the destination." \
				"$l_fast_noop_destination_list_status"
		fi
	fi

	for l_fast_noop_destination_status in \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_sort_status" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_wait_status"; do
		[ "$l_fast_noop_destination_status" -eq 0 ] && continue
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_destination_status"
	done
	return 0
}

# Purpose: Validate source stderr, wait status, and count sidecar for a fast proof.
# Usage: Called after destination validation; preserves exact source diagnostics
# and the exclusion-specific empty-source fallback.
zxfer_validate_fast_recursive_noop_source_discovery() {
	if [ "$g_zxfer_snapshot_discovery_fast_noop_source_wait_status" -ne 0 ]; then
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		zxfer_read_snapshot_discovery_capture_file \
			"$g_zxfer_snapshot_discovery_fast_noop_source_error_file" || {
			l_fast_noop_source_error_read_status=$?
			zxfer_cleanup_fast_recursive_noop_discovery_operation_state
			zxfer_throw_error \
				"Failed to read staged source snapshot stderr." \
				"$l_fast_noop_source_error_read_status"
		}
		l_fast_noop_source_error=$g_zxfer_snapshot_discovery_file_read_result
		l_fast_noop_source_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_fast_noop_source_error" 10)
		l_fast_noop_source_wait_status=$g_zxfer_snapshot_discovery_fast_noop_source_wait_status
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		if [ "$l_fast_noop_source_error" != "" ]; then
			zxfer_throw_error \
				"Failed to retrieve snapshots from the source: $l_fast_noop_source_error" \
				"$l_fast_noop_source_wait_status"
		fi
		zxfer_throw_error \
			"Failed to retrieve snapshots from the source" \
			"$l_fast_noop_source_wait_status"
	fi

	l_fast_noop_source_count_status=0
	zxfer_read_snapshot_discovery_status_file \
		"$g_zxfer_snapshot_discovery_fast_noop_source_count_file" 1 ||
		l_fast_noop_source_count_status=$?
	l_fast_noop_source_snapshot_count=$g_zxfer_snapshot_discovery_status_file_result
	if [ "$l_fast_noop_source_count_status" -ne 0 ] ||
		[ "$l_fast_noop_source_snapshot_count" -ne 1 ]; then
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		if [ -n "${g_option_x_exclude_datasets:-}" ]; then
			zxfer_reset_destination_existence_cache
			return 1
		fi
		zxfer_throw_error "Failed to retrieve snapshots from the source"
	fi
	return 0
}

# Purpose: Validate all completed fast recursive no-op proof results.
# Usage: zxfer_validate_fast_recursive_noop_discovery; runs the compare, then
# destination and source validation, and declines (returns 1) for a missing
# destination.
zxfer_validate_fast_recursive_noop_discovery() {
	zxfer_compare_fast_recursive_noop_discovery_streams || return "$?"
	zxfer_validate_fast_recursive_noop_destination_discovery || return "$?"
	zxfer_validate_fast_recursive_noop_source_discovery || return "$?"

	if [ "$g_zxfer_snapshot_discovery_fast_noop_missing_destination" -eq 1 ]; then
		zxfer_map_destination_dataset
		zxfer_echoV "Destination dataset does not exist: $g_zxfer_destination_dataset_result"
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		zxfer_reset_destination_existence_cache
		return 1
	fi
	return 0
}

# Purpose: Publish a proven fast recursive no-op result.
# Usage: zxfer_publish_fast_recursive_noop_discovery; called only after all
# validation succeeds.
zxfer_publish_fast_recursive_noop_discovery() {
	zxfer_reset_recursive_dataset_lists
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
	# The proven no-op ends this pass. Trap exit removes the private root
	# once; deleting the proof's files here would only add process work.
	zxfer_reset_fast_recursive_noop_discovery_operation_state
	zxfer_echov "No new snapshots to transfer."
	return 0
}

# Purpose: Try to prove a clean recursive no-op (local or remote-origin source)
# with identity-aware discovery before the full creation-order source listing.
# Usage: zxfer_try_fast_recursive_noop_discovery; returns 0 when the no-op is
# proven, 1 when full discovery should run, other statuses on failure.
zxfer_try_fast_recursive_noop_discovery() {
	zxfer_fast_recursive_noop_options_are_eligible || return 1

	zxfer_allocate_fast_recursive_noop_discovery_stages || return "$?"
	zxfer_start_fast_recursive_noop_source_discovery || return "$?"
	zxfer_start_fast_recursive_noop_destination_discovery || return "$?"
	zxfer_wait_for_fast_recursive_noop_discovery || return "$?"
	# If the proof declines, full discovery reuses a successful raw
	# destination listing instead of running the same zfs list again.
	if zxfer_read_fast_recursive_noop_destination_statuses &&
		[ "$g_zxfer_snapshot_discovery_fast_noop_destination_list_status" -eq 0 ]; then
		g_zxfer_snapshot_discovery_destination_listing_file=$g_zxfer_snapshot_discovery_fast_noop_destination_raw_file
	fi
	zxfer_validate_fast_recursive_noop_discovery || return "$?"
	zxfer_publish_fast_recursive_noop_discovery
}

# Purpose: Allocate and start the source side of full snapshot discovery.
# Usage: zxfer_start_full_source_snapshot_discovery; called after the fast
# no-op proof declines. Publishes the stage paths and the listing's start time.
# Returns: Zero after launch, otherwise the original allocation/launch status.
zxfer_start_full_source_snapshot_discovery() {
	zxfer_create_temp_file_group 2 || return "$?"
	g_zxfer_full_source_snapshot_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r g_zxfer_full_source_snapshot_file
		IFS= read -r g_zxfer_full_source_snapshot_error_file
	} <<-EOF
		$g_zxfer_full_source_snapshot_stage_files
	EOF

	g_source_snapshot_list_pid=""
	g_zxfer_full_source_snapshot_sorted_file=""
	g_zxfer_full_source_snapshot_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		g_zxfer_full_source_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	l_full_source_start_status=0
	g_source_snapshot_list_background_sort_requested=1
	zxfer_write_source_snapshot_list_to_file \
		"$g_zxfer_full_source_snapshot_file" \
		"$g_zxfer_full_source_snapshot_error_file" ||
		l_full_source_start_status=$?
	g_source_snapshot_list_background_sort_requested=0
	g_zxfer_full_source_snapshot_sorted_file=${g_source_snapshot_list_sorted_file:-}
	if [ "$l_full_source_start_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_full_source_snapshot_sorted_file"
		g_source_snapshot_list_sorted_file=""
		zxfer_cleanup_runtime_artifact_path_list_and_return \
			"$l_full_source_start_status" \
			"$g_zxfer_full_source_snapshot_stage_files"
		return "$?"
	fi

	return 0
}

# Purpose: Collect and normalize the destination side of full discovery.
# Usage: zxfer_collect_full_destination_snapshot_discovery; runs while the
# source listing is in flight. A destination listing handed over by a declined
# fast proof is normalized instead of listed again.
# Returns: Zero with owned destination stage paths published, otherwise non-zero.
zxfer_collect_full_destination_snapshot_discovery() {
	l_full_destination_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_full_destination_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	g_zxfer_full_destination_inventory_attempted=0
	zxfer_map_destination_dataset
	l_full_destination_dataset=$g_zxfer_destination_dataset_result
	l_full_destination_listing=${g_zxfer_snapshot_discovery_destination_listing_file:-}
	g_zxfer_snapshot_discovery_destination_listing_file=""

	if [ -n "$l_full_destination_listing" ]; then
		g_zxfer_full_destination_snapshot_file=$l_full_destination_listing
	elif zxfer_get_temp_file; then
		g_zxfer_full_destination_snapshot_file=$g_zxfer_temp_file_result
	else
		l_full_destination_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_destination_status"
	fi
	zxfer_get_temp_file || {
		l_full_destination_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file" \
			"$g_zxfer_full_destination_snapshot_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_destination_status"
	}
	g_zxfer_full_destination_snapshot_sorted_file=$g_zxfer_temp_file_result

	l_full_destination_status=0
	if [ -n "${g_option_T_target_host:-}" ]; then
		zxfer_collect_full_remote_destination_snapshot_discovery \
			"$l_full_destination_dataset" || return "$?"
	elif [ -n "$l_full_destination_listing" ]; then
		# The fast proof's listing succeeded, so the root exists.
		zxfer_set_destination_existence_cache_entry "$l_full_destination_dataset" 1
		zxfer_normalize_destination_snapshot_list "$l_full_destination_dataset" \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file" ||
			l_full_destination_status=$?
	else
		zxfer_write_destination_snapshot_list_to_files \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file" ||
			l_full_destination_status=$?
	fi
	if [ "$l_full_destination_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file" \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_destination_status"
	fi

	zxfer_profile_add_elapsed_ms \
		g_zxfer_profile_destination_snapshot_listing_ms \
		"$l_full_destination_stage_start_ms"
	return 0
}

# Purpose: Clear the caller-owned inventory staging for one remote batch.
# Usage: Normal completion and every post-allocation failure share this owner
# operation so the contained transport workspace remains separately owned.
zxfer_cleanup_full_remote_destination_inventory_stages() {
	l_full_remote_inventory_cleanup_paths=${g_zxfer_full_remote_destination_inventory_stage_files:-}
	if [ -n "$l_full_remote_inventory_cleanup_paths" ]; then
		zxfer_cleanup_runtime_artifact_path_list \
			"$l_full_remote_inventory_cleanup_paths" >/dev/null 2>&1 || :
	fi
	g_zxfer_full_remote_destination_inventory_stage_files=""
	g_zxfer_full_remote_destination_list_file=""
	g_zxfer_full_remote_destination_list_error_file=""
}

# Purpose: Clean all full-discovery state after a remote destination failure.
# Usage: zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
# [STATUS]; returns STATUS (default 0), so `step || cleanup "$?" || return`
# keeps the step's status. Call it only after saving any staged stderr text
# the failure report needs.
zxfer_cleanup_failed_full_remote_destination_snapshot_discovery() {
	zxfer_cleanup_full_remote_destination_inventory_stages
	zxfer_cleanup_runtime_artifact_paths \
		"$g_zxfer_full_source_snapshot_file" \
		"$g_zxfer_full_source_snapshot_error_file" \
		"$g_zxfer_full_destination_snapshot_file" \
		"$g_zxfer_full_destination_snapshot_sorted_file" \
		"${g_zxfer_full_destination_snapshot_error_file:-}" \
		"${g_zxfer_full_remote_destination_failure_error_file:-}" \
		>/dev/null 2>&1 || :
	g_zxfer_full_destination_snapshot_error_file=""
	g_zxfer_full_remote_destination_failure_error_file=""
	zxfer_cleanup_snapshot_record_cache_files
	return "${1:-0}"
}

# Purpose: Allocate the dataset-list and error files of one remote
# destination inventory.
# Usage: zxfer_allocate_full_remote_destination_inventory_stages; publishes
# g_zxfer_full_remote_destination_list_file and _list_error_file.
zxfer_allocate_full_remote_destination_inventory_stages() {
	zxfer_create_temp_file_group 2 || return "$?"
	g_zxfer_full_remote_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r g_zxfer_full_remote_destination_list_file
		IFS= read -r g_zxfer_full_remote_destination_list_error_file
	} <<-EOF
		$g_zxfer_full_remote_destination_inventory_stage_files
	EOF

	return 0
}

# Purpose: Stage a remote-batch failure diagnostic without modifying outputs.
# Usage: The extra file exists only on failure; a validated SSH diagnostic is
# copied exactly, while malformed protocol failures use an empty stage.
zxfer_stage_full_remote_destination_failure_error() {
	g_zxfer_full_remote_destination_failure_error_file=""
	zxfer_get_temp_file || return "$?"
	g_zxfer_full_remote_destination_failure_error_file=$g_zxfer_temp_file_result
	if zxfer_remote_destination_discovery_failure_is_transport; then
		zxfer_get_remote_destination_discovery_transport_stderr \
			>"$g_zxfer_full_remote_destination_failure_error_file" || return "$?"
	fi
}

# Purpose: Report a remote-batch failure when its diagnostic cannot be staged.
# Usage: Called only after preserving the meaningful transport or protocol
# status. Cleanup precedes reporting because the production reporter exits.
zxfer_report_unstaged_full_remote_destination_failure() {
	l_full_remote_unstaged_status=$1
	l_full_remote_unstaged_error=""

	if zxfer_remote_destination_discovery_failure_is_transport; then
		l_full_remote_unstaged_error=$(zxfer_get_remote_destination_discovery_transport_stderr)
		l_full_remote_unstaged_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_full_remote_unstaged_error" 5)
	fi
	zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
	if [ -n "$l_full_remote_unstaged_error" ]; then
		zxfer_throw_error "Failed to retrieve list of datasets from the destination: $l_full_remote_unstaged_error" \
			"$l_full_remote_unstaged_status"
	else
		zxfer_throw_error "Failed to retrieve list of datasets from the destination" \
			"$l_full_remote_unstaged_status"
	fi
	return "$l_full_remote_unstaged_status"
}

# Purpose: Run one remote batch and publish its dataset-inventory result.
# Usage: A failed batch gets a separate diagnostic stage so all four transaction
# outputs remain either the old generation or the complete new generation.
zxfer_run_and_publish_full_remote_destination_discovery_batch() {
	l_full_remote_batch_dataset=$1
	l_full_remote_batch_status=0
	l_full_remote_batch_error_file=$g_zxfer_full_remote_destination_list_error_file

	zxfer_run_remote_destination_discovery_batch_to_files \
		"$l_full_remote_batch_dataset" \
		"$g_zxfer_full_remote_destination_list_file" \
		"$g_zxfer_full_remote_destination_list_error_file" \
		"$g_zxfer_full_destination_snapshot_file" \
		"$g_zxfer_full_destination_snapshot_error_file" ||
		l_full_remote_batch_status=$?
	if [ "$l_full_remote_batch_status" -eq 0 ]; then
		l_full_remote_batch_status=$g_zxfer_destination_discovery_batch_inventory_status
	else
		zxfer_stage_full_remote_destination_failure_error
		l_full_remote_failure_stage_status=$?
		if [ "$l_full_remote_failure_stage_status" -ne 0 ]; then
			zxfer_report_unstaged_full_remote_destination_failure \
				"$l_full_remote_batch_status"
			return "$l_full_remote_batch_status"
		fi
		l_full_remote_batch_error_file=$g_zxfer_full_remote_destination_failure_error_file
	fi

	zxfer_publish_destination_dataset_inventory_from_stage \
		"$g_zxfer_full_remote_destination_list_file" \
		"$l_full_remote_batch_error_file" \
		"$l_full_remote_batch_status" \
		"${g_zxfer_destination_discovery_batch_pool_status:-}"
	g_zxfer_full_destination_inventory_attempted=1
}

# Purpose: Report a target-side snapshot-list failure after checked readback.
# Usage: Called after inventory publication; cleanup precedes either exact legacy
# error so the full run-root trap is only the failure fallback.
zxfer_report_full_remote_destination_snapshot_failure() {
	[ "${g_zxfer_destination_discovery_batch_snapshot_status:-0}" -ne 0 ] || return 0

	l_full_remote_snapshot_failure_read_status=0
	zxfer_read_snapshot_discovery_capture_file \
		"$g_zxfer_full_destination_snapshot_error_file" ||
		l_full_remote_snapshot_failure_read_status=$?
	l_full_remote_snapshot_failure_stderr=$g_zxfer_snapshot_discovery_file_read_result
	l_full_remote_snapshot_failure_status=$g_zxfer_destination_discovery_batch_snapshot_status
	zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
	if [ "$l_full_remote_snapshot_failure_read_status" -ne 0 ]; then
		zxfer_throw_error "Failed to read staged destination snapshot stderr." \
			"$l_full_remote_snapshot_failure_read_status"
		return "$l_full_remote_snapshot_failure_read_status"
	fi
	if [ -n "$l_full_remote_snapshot_failure_stderr" ]; then
		zxfer_warn_stderr "$l_full_remote_snapshot_failure_stderr"
	fi
	zxfer_throw_error "Failed to retrieve snapshot list from the destination." \
		"$l_full_remote_snapshot_failure_status"
	return "$l_full_remote_snapshot_failure_status"
}

# Purpose: Normalize and release successful full remote destination stages.
# Usage: Inventory staging is no longer needed once publication succeeds; the
# raw snapshot stream remains owned by full-discovery result publication.
zxfer_normalize_full_remote_destination_snapshot_discovery() {
	l_full_remote_normalize_dataset=$1
	zxfer_cleanup_full_remote_destination_inventory_stages
	zxfer_normalize_destination_snapshot_list \
		"$l_full_remote_normalize_dataset" \
		"$g_zxfer_full_destination_snapshot_file" \
		"$g_zxfer_full_destination_snapshot_sorted_file" ||
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery "$?" || return
	zxfer_cleanup_runtime_artifact_paths \
		"$g_zxfer_full_destination_snapshot_error_file" \
		"${g_zxfer_full_remote_destination_failure_error_file:-}" \
		>/dev/null 2>&1 || :
	g_zxfer_full_destination_snapshot_error_file=""
	g_zxfer_full_remote_destination_failure_error_file=""
}

# Purpose: Collect the remote destination inventory and snapshot stream.
# Usage: zxfer_collect_full_remote_destination_snapshot_discovery DATASET;
# called by full destination discovery under -T. Returns 0 with the
# normalized destination files, else the first failing status after cleanup.
zxfer_collect_full_remote_destination_snapshot_discovery() {
	l_full_remote_collect_dataset=$1
	zxfer_allocate_full_remote_destination_inventory_stages ||
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery "$?" || return
	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")"
	else
		zxfer_record_last_command_opaque
	fi
	# The snapshot stderr stage follows the trace, so -V output keeps its order.
	g_zxfer_full_destination_snapshot_error_file=""
	zxfer_get_temp_file ||
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery "$?" || return
	g_zxfer_full_destination_snapshot_error_file=$g_zxfer_temp_file_result
	zxfer_run_and_publish_full_remote_destination_discovery_batch \
		"$l_full_remote_collect_dataset" ||
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery "$?" || return
	zxfer_report_full_remote_destination_snapshot_failure || return
	zxfer_normalize_full_remote_destination_snapshot_discovery \
		"$l_full_remote_collect_dataset"
}

# Purpose: Wait for and validate the full source snapshot producer.
# Usage: zxfer_wait_for_full_source_snapshot_discovery; called after the
# destination side is collected, so both sides overlap.
# Returns: Zero when source staging is complete; failures throw.
zxfer_wait_for_full_source_snapshot_discovery() {
	zxfer_echoV "Waiting for background processes to finish."
	l_full_source_wait_status=0
	if [ -n "${g_source_snapshot_list_pid:-}" ]; then
		wait "$g_source_snapshot_list_pid" || l_full_source_wait_status=$?
		[ "$l_full_source_wait_status" -eq 0 ] ||
			zxfer_kill_reaped_producer_group "$g_source_snapshot_list_pid"
		zxfer_unregister_cleanup_pid "$g_source_snapshot_list_pid"
		g_source_snapshot_list_pid=""
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_source_snapshot_listing_ms \
		"$g_zxfer_full_source_snapshot_stage_start_ms"

	if [ "$l_full_source_wait_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file"
		zxfer_cleanup_snapshot_record_cache_files
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		zxfer_read_snapshot_discovery_capture_file \
			"$g_zxfer_full_source_snapshot_error_file" || {
			l_full_source_stderr_read_status=$?
			zxfer_cleanup_runtime_artifact_path \
				"$g_zxfer_full_source_snapshot_error_file"
			zxfer_throw_error "Failed to read staged source snapshot stderr." \
				"$l_full_source_stderr_read_status"
		}
		l_full_source_snapshot_error=$g_zxfer_snapshot_discovery_file_read_result
		l_full_source_snapshot_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_full_source_snapshot_error" 10)
		zxfer_cleanup_runtime_artifact_path \
			"$g_zxfer_full_source_snapshot_error_file"
		if [ "$l_full_source_snapshot_error" != "" ]; then
			zxfer_throw_error "Failed to retrieve snapshots from the source: $l_full_source_snapshot_error" \
				"$l_full_source_wait_status"
		fi
		zxfer_throw_error "Failed to retrieve snapshots from the source" \
			"$l_full_source_wait_status"
	fi
	zxfer_echoV "Background processes finished."

	if [ ! -s "$g_zxfer_full_source_snapshot_file" ]; then
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file" \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to retrieve snapshots from the source"
	fi
	return 0
}

# Purpose: Publish the full discovery deltas, the local destination inventory
# and the record caches that later planning needs.
# Usage: zxfer_publish_full_snapshot_discovery_results; the last full-discovery
# stage, after both producers finish. One cleanup removes the pass's
# transient listings; the record caches stay for planning.
# Returns: Zero after publication, otherwise the original helper status.
zxfer_publish_full_snapshot_discovery_results() {
	l_full_publish_diff_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_full_publish_diff_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	l_full_publish_status=0
	zxfer_set_g_recursive_source_list \
		"$g_zxfer_full_source_snapshot_file" \
		"$g_zxfer_full_destination_snapshot_sorted_file" \
		"$g_zxfer_full_source_snapshot_sorted_file" ||
		l_full_publish_status=$?
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms \
		"$l_full_publish_diff_start_ms"

	if [ "$l_full_publish_status" -eq 0 ] &&
		zxfer_snapshot_discovery_needs_destination_dataset_inventory; then
		zxfer_collect_local_destination_dataset_inventory || l_full_publish_status=$?
		g_zxfer_full_destination_inventory_attempted=1
	fi

	l_full_publish_transient_files=$g_zxfer_full_destination_snapshot_sorted_file$ZXFER_LF${g_zxfer_full_source_snapshot_sorted_file:-}$ZXFER_LF$g_zxfer_full_source_snapshot_error_file$ZXFER_LF$g_zxfer_full_source_snapshot_file
	if [ "$l_full_publish_status" -eq 0 ] && zxfer_snapshot_discovery_needs_record_caches; then
		zxfer_publish_full_snapshot_record_caches || l_full_publish_status=$?
	else
		l_full_publish_transient_files=$l_full_publish_transient_files$ZXFER_LF$g_zxfer_full_destination_snapshot_file
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_full_publish_transient_files"
	g_source_snapshot_list_sorted_file=""
	if [ "$l_full_publish_status" -ne 0 ]; then
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_publish_status"
	fi

	if [ "$g_zxfer_full_destination_inventory_attempted" -eq 1 ] &&
		[ "$g_recursive_dest_list" = "" ]; then
		zxfer_echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi
	return 0
}

# Purpose: Keep the full listings as the record caches that per-dataset
# planning reads: the destination listing as is, the source listing reversed
# to newest first.
# Usage: zxfer_publish_full_snapshot_record_caches; publishes
# g_zxfer_destination_snapshot_record_cache_file and
# g_zxfer_source_snapshot_record_cache_file.
zxfer_publish_full_snapshot_record_caches() {
	g_zxfer_destination_snapshot_record_cache_file=$g_zxfer_full_destination_snapshot_file
	zxfer_get_temp_file || return "$?"
	g_zxfer_source_snapshot_record_cache_file=$g_zxfer_temp_file_result
	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_command_for_report "" zxfer_reverse_file_lines "$g_zxfer_full_source_snapshot_file") > $(zxfer_quote_token_for_report "$g_zxfer_source_snapshot_record_cache_file")"
	else
		zxfer_record_last_command_opaque
	fi
	zxfer_reverse_file_lines "$g_zxfer_full_source_snapshot_file" \
		>"$g_zxfer_source_snapshot_record_cache_file" ||
		zxfer_throw_error "Failed to stage source snapshot record cache." "$?"
}

# Purpose: Build the source and destination snapshot inventories that the rest
# of replication planning depends on.
# Usage: zxfer_get_zfs_list; called at the start of each live pass. It throws
# on any failure, because its callers do not check its status.
#
# zxfer relies on `zfs list` in machine-readable mode (`-H`), recursive dataset
# traversal (`-r`) where needed, identity-aware output during initial discovery
# (`-o name,guid`), snapshot-only listing (`-t snapshot`), and creation-order
# sorting for per-dataset snapshot discovery on the source side. The fast
# recursive no-op proof uses the same identity-aware records without the
# creation-order sort so equal snapshot names with different GUIDs fall back to
# full discovery instead of being treated as clean.
zxfer_get_zfs_list() {
	zxfer_set_failure_stage "snapshot discovery"
	zxfer_echoV "Begin zxfer_get_zfs_list()"
	zxfer_reset_snapshot_discovery_state
	zxfer_reset_destination_existence_cache

	l_get_zfs_list_status=0
	zxfer_try_fast_recursive_noop_discovery || l_get_zfs_list_status=$?
	if [ "$l_get_zfs_list_status" -eq 1 ]; then
		# The no-op proof declined: run full discovery.
		l_get_zfs_list_status=0
		zxfer_reset_full_snapshot_discovery_operation_state
		zxfer_start_full_source_snapshot_discovery &&
			zxfer_collect_full_destination_snapshot_discovery &&
			zxfer_wait_for_full_source_snapshot_discovery &&
			zxfer_publish_full_snapshot_discovery_results ||
			l_get_zfs_list_status=$?
	fi
	if [ "$l_get_zfs_list_status" -ne 0 ]; then
		zxfer_throw_error "Failed to discover source and destination snapshots." \
			"$l_get_zfs_list_status"
		return "$l_get_zfs_list_status"
	fi

	zxfer_echoV "End zxfer_get_zfs_list()"
}
