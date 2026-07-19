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
#   no-op operation state, staged cache paths, and recursive dataset results.
# reads globals: discovery options, source/destination context, producer and
#   remote-batch result channels, and profiling state.
# mutates caches: destination-existence and snapshot-record indexes through
#   shared helpers.
# returns via stdout: diffed dataset streams and orchestration results; drives
#   fast no-op and full source/destination discovery protocols.

# Purpose: Reset the file-backed state for one full snapshot-discovery operation.
# Usage: Called before full discovery starts and by the module reset path so
# stage ownership is explicit instead of being carried in one large function.
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
# Usage: Called before allocation and by the module reset path so helper stages
# exchange only explicitly named hot results.
zxfer_reset_fast_recursive_noop_discovery_operation_state() {
	g_zxfer_snapshot_discovery_fast_noop_stage_files=""
	g_zxfer_snapshot_discovery_fast_noop_source_stream_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_stream_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_error_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_command_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_count_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_error_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_status_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status_file=""
	g_zxfer_snapshot_discovery_fast_noop_destination_stream_status_file=""
	g_zxfer_snapshot_discovery_fast_noop_diff_file=""
	g_zxfer_snapshot_discovery_fast_noop_source_pid=""
	g_zxfer_snapshot_discovery_fast_noop_destination_pid=""
	g_zxfer_snapshot_discovery_fast_noop_source_wait_status=0
	g_zxfer_snapshot_discovery_fast_noop_destination_wait_status=0
	g_zxfer_snapshot_discovery_fast_noop_missing_destination=0
	g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms=""
	g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms=""
}

# Purpose: Clean up one fast recursive no-op proof attempt and clear its scratch.
# Usage: Called on every post-allocation terminal path so staged artifacts and
# cross-helper hot results have one owner.
zxfer_cleanup_fast_recursive_noop_discovery_operation_state() {
	l_fast_noop_cleanup_stage_files=${g_zxfer_snapshot_discovery_fast_noop_stage_files:-}
	if [ -n "$l_fast_noop_cleanup_stage_files" ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_noop_cleanup_stage_files"
	fi
	zxfer_reset_fast_recursive_noop_discovery_operation_state
}

# Purpose: Clean up the snapshot record cache files that this module created or
# tracks.
# Usage: Called during source and destination snapshot discovery on success and
# failure paths so temporary state does not linger.
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
	zxfer_clear_source_snapshot_list_sorted_file
}

# Purpose: Reset the snapshot discovery state so the next snapshot-discovery
# pass starts from a clean state.
# Usage: Called during source and destination snapshot discovery before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_snapshot_discovery_state() {
	zxfer_cleanup_snapshot_record_cache_files
	zxfer_reset_full_snapshot_discovery_operation_state
	zxfer_reset_fast_recursive_noop_discovery_operation_state
	zxfer_reset_snapshot_producer_state
	g_source_snapshot_list_background_sort_requested=0
	g_source_snapshot_fast_noop_attempted=0
	zxfer_reset_recursive_dataset_lists
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_recursive_dataset_list_result=""
	zxfer_reset_destination_discovery_batch_state
	g_lzfs_list_hr_snap=""
	zxfer_reset_snapshot_record_indexes
	g_rzfs_list_hr_snap=""
}

# Purpose: Publish the nonblank recursive dataset list from the latest checked
# snapshot-discovery readback.
# Usage: Called after staged recursive dataset captures and exclude filters so
# blank-line handling stays identical across file-backed paths.
zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result() {
	g_zxfer_recursive_dataset_list_result=""
	while IFS= read -r l_dataset || [ -n "$l_dataset" ]; do
		[ -n "$l_dataset" ] || continue
		if [ -n "$g_zxfer_recursive_dataset_list_result" ]; then
			g_zxfer_recursive_dataset_list_result=$g_zxfer_recursive_dataset_list_result'
'$l_dataset
		else
			g_zxfer_recursive_dataset_list_result=$l_dataset
		fi
	done <<EOF
$g_zxfer_snapshot_discovery_file_read_result
EOF
}

# Purpose: Collect and publish destination dataset inventory through the local
# destination execution path.
# Usage: Called after snapshot diffing when later work has proven it can
# consume the recursive destination existence cache.
zxfer_collect_local_destination_dataset_inventory() {
	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dest_list_tmp_file
		IFS= read -r l_dest_list_err_file
	} <<-EOF
		$l_destination_inventory_stage_files
	EOF

	if zxfer_command_display_render_enabled; then
		l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
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

# Purpose: Write both recursive snapshot delta directions in one sorted-list
# pass.
# Usage: Called after source and destination snapshot lists have been sorted
# and normalized, before recursive dataset work queues are derived.
zxfer_write_snapshot_delta_files() {
	l_source_sorted_file=$1
	l_destination_sorted_file=$2
	l_source_missing_file=$3
	l_destination_extra_file=$4

	zxfer_get_temp_file >/dev/null || return "$?"
	l_combined_delta_file=$g_zxfer_temp_file_result

	zxfer_write_runtime_artifact_file "$l_source_missing_file" "" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
		return "$l_status"
	}
	zxfer_write_runtime_artifact_file "$l_destination_extra_file" "" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
		return "$l_status"
	}

	LC_ALL=C comm -3 "$l_source_sorted_file" "$l_destination_sorted_file" >"$l_combined_delta_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
		return "$l_status"
	}

	# `comm -3` prefixes destination-only records with one tab. Snapshot names
	# cannot begin with tabs, while GUID records may contain tabs after the
	# name, so removing exactly one leading tab preserves the serialized record.
	l_status=0
	# shellcheck disable=SC2016  # awk script should see literal $0.
	"${g_cmd_awk:-awk}" \
		-v source_file="$l_source_missing_file" \
		-v destination_file="$l_destination_extra_file" '
		substr($0, 1, 1) == "\t" {
			print substr($0, 2) >> destination_file
			next
		}
		{
			print $0 >> source_file
		}
		END {
			close(source_file)
			close(destination_file)
		}
	' "$l_combined_delta_file" ||
		l_status=$?

	zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
	return "$l_status"
}

# Purpose: Filter sorted snapshot records by dataset using the configured
# exclude pattern.
# Usage: Called before recursive diff planning so excluded datasets cannot keep
# otherwise no-op source and destination snapshot lists from short-circuiting.
zxfer_filter_snapshot_file_with_excludes() {
	l_snapshot_input_file=$1
	l_snapshot_output_file=$2

	[ -n "${g_option_x_exclude_datasets:-}" ] || {
		cat "$l_snapshot_input_file" >"$l_snapshot_output_file"
		return "$?"
	}

	# shellcheck disable=SC2016  # awk script should see literal $0.
	"${g_cmd_awk:-awk}" \
		-v exclude_pattern="$g_option_x_exclude_datasets" '
		{
			snapshot_path = $0
			if (substr(snapshot_path, 1, 1) == "\t") {
				snapshot_path = substr(snapshot_path, 2)
			}
			tab_pos = index(snapshot_path, "\t")
			if (tab_pos > 0) {
				snapshot_path = substr(snapshot_path, 1, tab_pos - 1)
			}
		at_pos = index(snapshot_path, "@")
		snapshot_dataset = snapshot_path
		if (at_pos > 0) {
			snapshot_dataset = substr(snapshot_path, 1, at_pos - 1)
		}
		if (snapshot_dataset !~ exclude_pattern) {
			print
		}
	}' "$l_snapshot_input_file" >"$l_snapshot_output_file"
}

# Purpose: Check whether zxfer should use linear reverse for file.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean branch decision about the current configuration or
# live state.
#
# Reverse snapshot lists with a bounded linear awk path, but keep the older
# sort-based fallback for very large inputs so reversal does not become
# unbounded-memory work on hosts with small awk heaps.
zxfer_should_use_linear_reverse_for_file() {
	l_input_file=$1
	l_max_lines=${g_zxfer_linear_reverse_max_lines:-50000}

	case "$l_max_lines" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	[ "$l_max_lines" -gt 0 ] || return 1

	l_line_count_status=0
	l_line_count=$("${g_cmd_awk:-awk}" 'END { print NR + 0 }' "$l_input_file" 2>/dev/null) ||
		l_line_count_status=$?
	[ "$l_line_count_status" -eq 0 ] || return "$l_line_count_status"
	case "$l_line_count" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	[ "$l_line_count" -le "$l_max_lines" ]
}

# Purpose: Reverse the plain file lines with sort while preserving the record
# structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_plain_file_lines_with_sort() {
	l_input_file=$1
	zxfer_get_temp_file >/dev/null || return "$?"
	l_numbered_tmp_file=$g_zxfer_temp_file_result

	cat -n "$l_input_file" >"$l_numbered_tmp_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_numbered_tmp_file"
		return "$l_status"
	}

	l_status=0
	# Reverse by the cat -n line numbers, then strip the number column.
	LC_ALL=C sort -nr "$l_numbered_tmp_file" | cut -f2- || l_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_numbered_tmp_file"
	return "$l_status"
}

# Purpose: Reverse the file lines while preserving the record structure later
# helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_file_lines() {
	l_reverse_file_lines_input_file=$1

	if zxfer_should_use_linear_reverse_for_file "$l_reverse_file_lines_input_file"; then
		# shellcheck disable=SC2016  # awk program should see literal $0/NR.
		"${g_cmd_awk:-awk}" '{ l_lines[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) print l_lines[l_i] }' "$l_reverse_file_lines_input_file"
	else
		zxfer_reverse_plain_file_lines_with_sort "$l_reverse_file_lines_input_file"
	fi
}

# Purpose: Capture the recursive dataset list from lines file into staged state
# or module globals for later use.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a checked snapshot of command output or computed state.
zxfer_capture_recursive_dataset_list_from_lines_file() {
	l_dataset_lines_file=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_dataset_lines_file" ] || return 0
	[ -f "$l_dataset_lines_file" ] || return 0

	zxfer_get_temp_file >/dev/null || return "$?"
	l_dataset_list_sorted_file=$g_zxfer_temp_file_result

	LC_ALL=C sort -u "$l_dataset_lines_file" >"$l_dataset_list_sorted_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
		return "$l_status"
	}

	zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_sorted_file" || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
		return "$l_read_status"
	}

	zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result

	zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
	return 0
}

# Purpose: Capture the recursive dataset list from snapshot file into staged
# state or module globals for later use.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a checked snapshot of command output or computed state.
zxfer_capture_recursive_dataset_list_from_snapshot_file() {
	l_snapshot_records_file=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_snapshot_records_file" ] || return 0
	[ -f "$l_snapshot_records_file" ] || return 0

	zxfer_get_temp_file >/dev/null || return "$?"
	l_capture_recursive_dataset_list_from_snapshot_file_dataset_lines_file=$g_zxfer_temp_file_result

	# Stage the dataset name (everything before @) of every snapshot record.
	# shellcheck disable=SC2016  # awk script should see literal $1.
	"$g_cmd_awk" -F@ '{print $1}' "$l_snapshot_records_file" >"$l_capture_recursive_dataset_list_from_snapshot_file_dataset_lines_file" || {
		l_capture_recursive_dataset_list_from_snapshot_file_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_recursive_dataset_list_from_snapshot_file_dataset_lines_file"
		return "$l_capture_recursive_dataset_list_from_snapshot_file_status"
	}

	zxfer_capture_recursive_dataset_list_from_lines_file "$l_capture_recursive_dataset_list_from_snapshot_file_dataset_lines_file" || {
		l_capture_recursive_dataset_list_from_snapshot_file_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_capture_recursive_dataset_list_from_snapshot_file_dataset_lines_file"
		return "$l_capture_recursive_dataset_list_from_snapshot_file_status"
	}

	zxfer_cleanup_runtime_artifact_path "$l_capture_recursive_dataset_list_from_snapshot_file_dataset_lines_file"
	return 0
}

# Purpose: Filter the recursive dataset list with excludes down to the subset
# later helpers should act on.
# Usage: Called during source and destination snapshot discovery before
# reconciliation, execution, or reporting consumes the reduced set.
zxfer_filter_recursive_dataset_list_with_excludes() {
	l_dataset_list=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_dataset_list" ] || return 0
	[ -n "${g_option_x_exclude_datasets:-}" ] || {
		g_zxfer_recursive_dataset_list_result=$l_dataset_list
		return 0
	}

	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	l_filter_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dataset_list_input_file
		IFS= read -r l_dataset_list_filtered_file
	} <<-EOF
		$l_filter_stage_files
	EOF

	if zxfer_write_runtime_artifact_file "$l_dataset_list_input_file" "$l_dataset_list
"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_status" "$l_filter_stage_files"
		return "$?"
	fi

	l_filter_status=0
	grep -v -e "$g_option_x_exclude_datasets" "$l_dataset_list_input_file" >"$l_dataset_list_filtered_file" ||
		l_filter_status=$?
	case "$l_filter_status" in
	0 | 1) ;;
	*)
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_filter_status" "$l_filter_stage_files"
		return "$?"
		;;
	esac

	zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_filtered_file" || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_read_status" "$l_filter_stage_files"
		return "$?"
	}

	zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result

	zxfer_cleanup_runtime_artifact_path_list "$l_filter_stage_files"
	return 0
}

# Purpose: Decide whether recursive source dataset inventory must be derived
# from the full source snapshot list.
# Usage: Called during recursive diff planning so no-op runs without property
# work can avoid an otherwise unused whole-tree dataset extraction.
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

# Purpose: Prepare the sorted and optionally filtered snapshot inputs used by
# recursive delta comparison.
# Usage: Called after the caller allocates one contained five-file workspace.
zxfer_prepare_recursive_snapshot_delta_inputs() {
	l_prepare_delta_raw_source_file=$1
	l_prepare_delta_destination_file=$2
	l_prepare_delta_presorted_source_file=$3
	l_prepare_delta_sorted_source_file=$4
	l_prepare_delta_filtered_source_file=$5
	l_prepare_delta_filtered_destination_file=$6
	l_prepare_delta_stage_files=$7

	if [ -n "$l_prepare_delta_presorted_source_file" ]; then
		if [ ! -f "$l_prepare_delta_presorted_source_file" ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_prepare_delta_stage_files"
			zxfer_throw_error "Failed to locate staged sorted source snapshots for recursive delta planning."
			return 1
		fi
	else
		if zxfer_command_display_render_enabled; then
			l_prepare_delta_cmd="$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_prepare_delta_raw_source_file") > $(zxfer_quote_token_for_report "$l_prepare_delta_sorted_source_file")"
			zxfer_echoV "Running command: $l_prepare_delta_cmd"
			zxfer_record_last_command_string "$l_prepare_delta_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		LC_ALL=C sort "$l_prepare_delta_raw_source_file" >"$l_prepare_delta_sorted_source_file" || {
			l_prepare_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_prepare_delta_stage_files"
			zxfer_throw_error "Failed to sort source snapshots for recursive delta planning." "$l_prepare_delta_status"
			return "$l_prepare_delta_status"
		}
	fi

	[ "${g_option_x_exclude_datasets:-}" != "" ] || return 0
	zxfer_filter_snapshot_file_with_excludes \
		"${l_prepare_delta_presorted_source_file:-$l_prepare_delta_sorted_source_file}" \
		"$l_prepare_delta_filtered_source_file" || {
		l_prepare_delta_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_prepare_delta_stage_files"
		zxfer_throw_error "Failed to filter source snapshots against exclude patterns for recursive delta planning." "$l_prepare_delta_status"
		return "$l_prepare_delta_status"
	}
	zxfer_filter_snapshot_file_with_excludes \
		"$l_prepare_delta_destination_file" \
		"$l_prepare_delta_filtered_destination_file" || {
		l_prepare_delta_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_prepare_delta_stage_files"
		zxfer_throw_error "Failed to filter destination snapshots against exclude patterns for recursive delta planning." "$l_prepare_delta_status"
		return "$l_prepare_delta_status"
	}
}

# Purpose: Materialize both recursive snapshot delta directions.
# Usage: Called with sorted comparison inputs and two owned output files.
zxfer_materialize_recursive_snapshot_delta_files() {
	l_materialize_delta_source_file=$1
	l_materialize_delta_destination_file=$2
	l_materialize_delta_missing_file=$3
	l_materialize_delta_extra_file=$4
	l_materialize_delta_stage_files=$5

	if cmp -s "$l_materialize_delta_source_file" "$l_materialize_delta_destination_file"; then
		zxfer_write_runtime_artifact_file "$l_materialize_delta_missing_file" "" || {
			l_materialize_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_materialize_delta_stage_files"
			zxfer_throw_error "Failed to stage empty recursive source snapshot delta." "$l_materialize_delta_status"
			return "$l_materialize_delta_status"
		}
		zxfer_write_runtime_artifact_file "$l_materialize_delta_extra_file" "" || {
			l_materialize_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_materialize_delta_stage_files"
			zxfer_throw_error "Failed to stage empty recursive destination snapshot delta." "$l_materialize_delta_status"
			return "$l_materialize_delta_status"
		}
		return 0
	else
		l_materialize_delta_compare_status=$?
	fi

	if [ "$l_materialize_delta_compare_status" -gt 1 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_materialize_delta_stage_files"
		zxfer_throw_error "Failed to compare source and destination snapshots for recursive delta planning." "$l_materialize_delta_compare_status"
		return "$l_materialize_delta_compare_status"
	fi
	zxfer_write_snapshot_delta_files \
		"$l_materialize_delta_source_file" \
		"$l_materialize_delta_destination_file" \
		"$l_materialize_delta_missing_file" \
		"$l_materialize_delta_extra_file" || {
		l_materialize_delta_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_materialize_delta_stage_files"
		zxfer_throw_error "Failed to diff source and destination snapshots for recursive delta planning." "$l_materialize_delta_status"
		return "$l_materialize_delta_status"
	}
}

# Purpose: Publish dataset work lists derived from the two snapshot delta files.
# Usage: Called after delta materialization and before exclude-list postfilters.
zxfer_publish_recursive_snapshot_delta_dataset_lists() {
	l_publish_delta_missing_file=$1
	l_publish_delta_extra_file=$2
	l_publish_delta_source_inventory_file=$3
	l_publish_delta_stage_files=$4

	if [ -s "$l_publish_delta_missing_file" ]; then
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_publish_delta_missing_file" || {
			l_publish_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_publish_delta_stage_files"
			zxfer_throw_error "Failed to derive recursive source dataset transfer list." "$l_publish_delta_status"
			return "$l_publish_delta_status"
		}
		zxfer_set_recursive_source_list "$g_zxfer_recursive_dataset_list_result"
	else
		zxfer_set_recursive_source_list ""
	fi
	if [ -s "$l_publish_delta_extra_file" ]; then
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_publish_delta_extra_file" || {
			l_publish_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_publish_delta_stage_files"
			zxfer_throw_error "Failed to derive recursive destination dataset delete list." "$l_publish_delta_status"
			return "$l_publish_delta_status"
		}
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_destination_extra_dataset_list=""
	fi
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_publish_delta_source_inventory_file" || {
			l_publish_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_publish_delta_stage_files"
			zxfer_throw_error "Failed to derive recursive source dataset inventory." "$l_publish_delta_status"
			return "$l_publish_delta_status"
		}
		zxfer_set_recursive_source_dataset_list "$g_zxfer_recursive_dataset_list_result"
	else
		zxfer_set_recursive_source_dataset_list ""
	fi
}

# Purpose: Apply dataset exclude patterns to every published recursive list.
# Usage: Called after raw work-list publication when -x is active.
zxfer_filter_published_recursive_snapshot_delta_lists() {
	l_filter_published_delta_stage_files=$1

	[ "${g_option_x_exclude_datasets:-}" != "" ] || return 0
	zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_list" || {
		l_filter_published_delta_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_filter_published_delta_stage_files"
		zxfer_throw_error "Failed to filter recursive source dataset transfer list against exclude patterns." "$l_filter_published_delta_status"
		return "$l_filter_published_delta_status"
	}
	zxfer_set_recursive_source_list "$g_zxfer_recursive_dataset_list_result"
	zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_destination_extra_dataset_list" || {
		l_filter_published_delta_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_filter_published_delta_stage_files"
		zxfer_throw_error "Failed to filter recursive destination dataset delete list against exclude patterns." "$l_filter_published_delta_status"
		return "$l_filter_published_delta_status"
	}
	g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_dataset_list" || {
			l_filter_published_delta_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_filter_published_delta_stage_files"
			zxfer_throw_error "Failed to filter recursive source dataset inventory against exclude patterns." "$l_filter_published_delta_status"
			return "$l_filter_published_delta_status"
		}
		zxfer_set_recursive_source_dataset_list "$g_zxfer_recursive_dataset_list_result"
	fi
}

# Purpose: Render recursive snapshot-delta summaries and very-verbose details.
# Usage: Called after all work lists have reached their final filtered form.
zxfer_report_recursive_snapshot_delta() {
	l_report_delta_missing_file=$1
	l_report_delta_extra_file=$2

	if [ "${g_option_v_verbose:-0}" -ne 1 ] && [ "${g_option_V_very_verbose:-0}" -ne 1 ]; then
		return 0
	fi
	l_report_delta_missing_count=$("${g_cmd_awk:-awk}" 'END { print NR + 0 }' "$l_report_delta_missing_file")
	l_report_delta_extra_count=$("${g_cmd_awk:-awk}" 'END { print NR + 0 }' "$l_report_delta_extra_file")
	l_report_delta_source_dataset_count=$(printf '%s\n' "$g_recursive_source_list" | "${g_cmd_awk:-awk}" 'NF { count++ } END { print count + 0 }')
	l_report_delta_extra_dataset_count=$(printf '%s\n' "$g_recursive_destination_extra_dataset_list" | "${g_cmd_awk:-awk}" 'NF { count++ } END { print count + 0 }')

	if [ "$l_report_delta_missing_count" -gt 0 ] || [ "$l_report_delta_extra_count" -gt 0 ]; then
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

# Purpose: Update the g recursive source list in the shared runtime state.
# Usage: Called during source and destination snapshot discovery after a probe
# or planning step changes the active context that later helpers should use.
zxfer_set_g_recursive_source_list() {
	l_set_recursive_source_raw_file=$1
	l_set_recursive_source_destination_file=$2
	l_set_recursive_source_presorted_file=${3:-}

	zxfer_create_temp_file_group 5 >/dev/null || return "$?"
	l_set_recursive_source_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_set_recursive_source_sorted_file
		IFS= read -r l_set_recursive_source_missing_file
		IFS= read -r l_set_recursive_source_extra_file
		IFS= read -r l_set_recursive_source_filtered_file
		IFS= read -r l_set_recursive_destination_filtered_file
	} <<-EOF
		$l_set_recursive_source_stage_files
	EOF

	l_set_recursive_source_sorted_input=$l_set_recursive_source_sorted_file
	[ -z "$l_set_recursive_source_presorted_file" ] || l_set_recursive_source_sorted_input=$l_set_recursive_source_presorted_file
	l_set_recursive_source_diff_input=$l_set_recursive_source_sorted_input
	l_set_recursive_destination_diff_input=$l_set_recursive_source_destination_file
	if [ "${g_option_x_exclude_datasets:-}" != "" ]; then
		l_set_recursive_source_diff_input=$l_set_recursive_source_filtered_file
		l_set_recursive_destination_diff_input=$l_set_recursive_destination_filtered_file
	fi

	zxfer_prepare_recursive_snapshot_delta_inputs \
		"$l_set_recursive_source_raw_file" "$l_set_recursive_source_destination_file" \
		"$l_set_recursive_source_presorted_file" "$l_set_recursive_source_sorted_file" \
		"$l_set_recursive_source_filtered_file" "$l_set_recursive_destination_filtered_file" \
		"$l_set_recursive_source_stage_files" || return "$?"
	zxfer_materialize_recursive_snapshot_delta_files \
		"$l_set_recursive_source_diff_input" "$l_set_recursive_destination_diff_input" \
		"$l_set_recursive_source_missing_file" "$l_set_recursive_source_extra_file" \
		"$l_set_recursive_source_stage_files" || return "$?"
	zxfer_publish_recursive_snapshot_delta_dataset_lists \
		"$l_set_recursive_source_missing_file" "$l_set_recursive_source_extra_file" \
		"$l_set_recursive_source_diff_input" "$l_set_recursive_source_stage_files" || return "$?"
	zxfer_filter_published_recursive_snapshot_delta_lists \
		"$l_set_recursive_source_stage_files" || return "$?"
	zxfer_report_recursive_snapshot_delta \
		"$l_set_recursive_source_missing_file" "$l_set_recursive_source_extra_file"

	if [ "$g_recursive_source_list" = "" ]; then
		zxfer_echov "No new snapshots to transfer."
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_set_recursive_source_stage_files"
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
# Usage: Called after eligibility succeeds and before either producer starts.
# Side effects: Publishes the nine owned stage paths through module hot results.
zxfer_allocate_fast_recursive_noop_discovery_stages() {
	zxfer_reset_fast_recursive_noop_discovery_operation_state
	zxfer_create_temp_file_group 9 >/dev/null || return "$?"
	g_zxfer_snapshot_discovery_fast_noop_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_stream_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_stream_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_error_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_command_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_source_count_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_error_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_status_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status_file
		IFS= read -r g_zxfer_snapshot_discovery_fast_noop_destination_stream_status_file
	} <<-EOF
		$g_zxfer_snapshot_discovery_fast_noop_stage_files
	EOF
	return 0
}

# Purpose: Start the source producer for a fast recursive no-op proof attempt.
# Usage: Called after stage allocation; publishes the registered producer PID
# and retains the rendered command for profiling and failure context.
zxfer_start_fast_recursive_noop_source_discovery() {
	g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	zxfer_build_source_snapshot_name_list_cmd \
		>"$g_zxfer_snapshot_discovery_fast_noop_source_command_file" || {
		l_fast_noop_source_start_status=$?
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_source_start_status"
	}
	zxfer_read_source_snapshot_discovery_command_file \
		"$g_zxfer_snapshot_discovery_fast_noop_source_command_file" || {
		l_fast_noop_source_start_status=$?
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_source_start_status"
	}
	l_fast_noop_source_start_command=$g_zxfer_snapshot_discovery_file_read_result
	if [ -z "$l_fast_noop_source_start_command" ]; then
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		zxfer_throw_error "Staged source snapshot no-op proof command was empty."
	fi

	g_zxfer_snapshot_discovery_fast_noop_diff_file=$g_zxfer_snapshot_discovery_fast_noop_source_command_file
	l_fast_noop_source_start_uses_parallel=0
	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		l_fast_noop_source_start_uses_parallel=1
	fi
	zxfer_set_source_snapshot_list_command "$l_fast_noop_source_start_command"
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
# Usage: Called after the source producer so both listings overlap; aborts and
# reaps the source producer if destination setup cannot start.
zxfer_start_fast_recursive_noop_destination_discovery() {
	g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi

	zxfer_start_destination_snapshot_name_sorted_fifo_producer \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stream_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_error_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_status_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stream_status_file" || {
		l_fast_noop_destination_start_status=$?
		l_fast_noop_source_abort_status=0
		zxfer_abort_fast_noop_background_pid \
			"$g_zxfer_snapshot_discovery_fast_noop_source_pid" \
			"background source snapshot no-op proof helper" ||
			l_fast_noop_source_abort_status=$?
		if [ "$l_fast_noop_source_abort_status" -ne 0 ]; then
			# Preserve the destination setup failure that caused cleanup. The
			# wait-only record remains registered so session teardown can report
			# the identity-unavailable helper without ever signalling a stale PID.
			zxfer_cleanup_fast_recursive_noop_discovery_operation_state
			return "$l_fast_noop_destination_start_status"
		fi
		wait "$g_zxfer_snapshot_discovery_fast_noop_source_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_zxfer_snapshot_discovery_fast_noop_source_pid"
		zxfer_clear_last_background_pid
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		return "$l_fast_noop_destination_start_status"
	}
	g_zxfer_snapshot_discovery_fast_noop_destination_pid=$g_last_background_pid
	return 0
}

# Purpose: Wait for both fast recursive no-op proof producers.
# Usage: Called only after both producers start; records exact wait statuses and
# preserves the original source-then-destination wait and profiling order.
zxfer_wait_for_fast_recursive_noop_discovery() {
	g_zxfer_snapshot_discovery_fast_noop_source_wait_status=0
	wait "$g_zxfer_snapshot_discovery_fast_noop_source_pid" ||
		g_zxfer_snapshot_discovery_fast_noop_source_wait_status=$?
	zxfer_unregister_cleanup_pid "$g_zxfer_snapshot_discovery_fast_noop_source_pid"
	zxfer_profile_add_elapsed_ms \
		g_zxfer_profile_source_snapshot_listing_ms \
		"$g_zxfer_snapshot_discovery_fast_noop_source_stage_start_ms"

	g_zxfer_snapshot_discovery_fast_noop_destination_wait_status=0
	wait "$g_zxfer_snapshot_discovery_fast_noop_destination_pid" ||
		g_zxfer_snapshot_discovery_fast_noop_destination_wait_status=$?
	zxfer_unregister_cleanup_pid "$g_zxfer_snapshot_discovery_fast_noop_destination_pid"
	zxfer_clear_last_background_pid
	zxfer_profile_add_elapsed_ms \
		g_zxfer_profile_destination_snapshot_listing_ms \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stage_start_ms"
	return 0
}

# Purpose: Compare the completed fast recursive no-op proof streams.
# Usage: Called before sidecar validation so a real source/destination delta
# falls back immediately, preserving the original no-op proof protocol.
zxfer_compare_fast_recursive_noop_discovery_streams() {
	l_fast_noop_compare_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_fast_noop_compare_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	l_fast_noop_compare_status=0
	if LC_ALL=C comm -3 \
		"$g_zxfer_snapshot_discovery_fast_noop_source_stream_file" \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stream_file" \
		>"$g_zxfer_snapshot_discovery_fast_noop_diff_file"; then
		if [ -s "$g_zxfer_snapshot_discovery_fast_noop_diff_file" ]; then
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

# Purpose: Validate destination sidecars and producer status for a fast no-op proof.
# Usage: Called only after the normalized streams compare equal; publishes
# whether the destination dataset was authoritatively reported missing.
zxfer_validate_fast_recursive_noop_destination_discovery() {
	l_fast_noop_destination_status_read_status=0
	zxfer_read_snapshot_discovery_status_file \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_status_file" 1 ||
		l_fast_noop_destination_status_read_status=$?
	l_fast_noop_destination_list_status=$g_zxfer_snapshot_discovery_status_file_result
	zxfer_read_snapshot_discovery_status_file \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_normalize_status_file" 1 ||
		l_fast_noop_destination_status_read_status=$?
	l_fast_noop_destination_normalize_status=$g_zxfer_snapshot_discovery_status_file_result
	zxfer_read_snapshot_discovery_status_file \
		"$g_zxfer_snapshot_discovery_fast_noop_destination_stream_status_file" 1 ||
		l_fast_noop_destination_status_read_status=$?
	l_fast_noop_destination_stream_status=$g_zxfer_snapshot_discovery_status_file_result
	if [ "$l_fast_noop_destination_status_read_status" -ne 0 ]; then
		l_fast_noop_destination_status_read_failure=$l_fast_noop_destination_status_read_status
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		zxfer_throw_error \
			"Failed to validate destination snapshot status for recursive no-op proof." \
			"$l_fast_noop_destination_status_read_failure"
	fi

	g_zxfer_snapshot_discovery_fast_noop_missing_destination=0
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
		"$l_fast_noop_destination_normalize_status" \
		"$l_fast_noop_destination_stream_status" \
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
# Usage: Runs compare, destination, and source validation in protocol order,
# then handles the authoritative missing-destination fallback.
zxfer_validate_fast_recursive_noop_discovery() {
	zxfer_compare_fast_recursive_noop_discovery_streams || return "$?"
	zxfer_validate_fast_recursive_noop_destination_discovery || return "$?"
	zxfer_validate_fast_recursive_noop_source_discovery || return "$?"

	if [ "$g_zxfer_snapshot_discovery_fast_noop_missing_destination" -eq 1 ]; then
		zxfer_echoV \
			"Destination dataset does not exist: $(zxfer_get_destination_snapshot_root_dataset)"
		zxfer_cleanup_fast_recursive_noop_discovery_operation_state
		zxfer_reset_destination_existence_cache
		return 1
	fi
	return 0
}

# Purpose: Publish a proven fast recursive no-op result.
# Usage: Called only after all stream and sidecar validation succeeds.
zxfer_publish_fast_recursive_noop_discovery() {
	zxfer_reset_recursive_dataset_lists
	g_recursive_destination_extra_dataset_list=""
	g_lzfs_list_hr_snap=""
	zxfer_reset_snapshot_record_indexes
	g_rzfs_list_hr_snap=""
	zxfer_cleanup_fast_recursive_noop_discovery_operation_state
	zxfer_echov "No new snapshots to transfer."
	return 0
}

# Purpose: Try to prove a clean recursive no-op (local or remote-origin source)
# with identity-aware discovery before the full creation-order source listing.
# Usage: Called by zxfer_get_zfs_list; returns 0 when no-op was proven and the
# caller can return, returns 1 when the normal discovery path should continue.
zxfer_try_fast_recursive_noop_discovery() {
	zxfer_fast_recursive_noop_options_are_eligible || return 1

	g_source_snapshot_fast_noop_attempted=1
	zxfer_allocate_fast_recursive_noop_discovery_stages || return "$?"
	zxfer_start_fast_recursive_noop_source_discovery || return "$?"
	zxfer_start_fast_recursive_noop_destination_discovery || return "$?"
	zxfer_wait_for_fast_recursive_noop_discovery
	zxfer_validate_fast_recursive_noop_discovery || return "$?"
	zxfer_publish_fast_recursive_noop_discovery
}

# Purpose: Allocate and start the source side of full snapshot discovery.
# Usage: Called after the fast no-op proof declines; publishes the owned stage
# paths and background-job timing state for the remaining protocol stages.
# Returns: Zero after launch, otherwise the original allocation/launch status.
zxfer_start_full_source_snapshot_discovery() {
	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	g_zxfer_full_source_snapshot_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r g_zxfer_full_source_snapshot_file
		IFS= read -r g_zxfer_full_source_snapshot_error_file
	} <<-EOF
		$g_zxfer_full_source_snapshot_stage_files
	EOF

	zxfer_clear_source_snapshot_list_job
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
		zxfer_clear_source_snapshot_list_sorted_file
		zxfer_cleanup_runtime_artifact_path_list_and_return \
			"$l_full_source_start_status" \
			"$g_zxfer_full_source_snapshot_stage_files"
		return "$?"
	fi

	return 0
}

# Purpose: Collect and normalize the destination side of full discovery.
# Usage: Runs while source discovery is in flight so the original concurrency,
# remote batch behavior, and profile timing remain unchanged.
# Returns: Zero with owned destination stage paths published, otherwise non-zero.
zxfer_collect_full_destination_snapshot_discovery() {
	l_full_destination_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_full_destination_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	g_zxfer_full_destination_inventory_attempted=0
	l_full_destination_dataset=$(zxfer_get_destination_snapshot_root_dataset)

	zxfer_get_temp_file >/dev/null || {
		l_full_destination_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_destination_status"
	}
	g_zxfer_full_destination_snapshot_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null || {
		l_full_destination_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file" \
			"$g_zxfer_full_destination_snapshot_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_destination_status"
	}
	g_zxfer_full_destination_snapshot_sorted_file=$g_zxfer_temp_file_result

	if [ -n "${g_option_T_target_host:-}" ]; then
		zxfer_collect_full_remote_destination_snapshot_discovery \
			"$l_full_destination_dataset" || return "$?"
	else
		zxfer_write_destination_snapshot_list_to_files \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file" || {
			l_full_destination_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$g_zxfer_full_source_snapshot_file" \
				"$g_zxfer_full_source_snapshot_error_file" \
				"$g_zxfer_full_destination_snapshot_file" \
				"$g_zxfer_full_destination_snapshot_sorted_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_full_destination_status"
		}
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
# Usage: Called only after preserving the meaningful lower-level status or
# staged stderr text needed by the failure reporter.
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
}

# Purpose: Allocate caller-owned inventory stages for one remote destination.
# Usage: Runs before command rendering, preserving the historical allocation,
# verbose-rendering, and snapshot-stderr allocation order.
zxfer_allocate_full_remote_destination_inventory_stages() {
	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	g_zxfer_full_remote_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r g_zxfer_full_remote_destination_list_file
		IFS= read -r g_zxfer_full_remote_destination_list_error_file
	} <<-EOF
		$g_zxfer_full_remote_destination_inventory_stage_files
	EOF

	return 0
}

# Purpose: Allocate the fourth caller-owned remote-batch output stage.
# Usage: Runs after command rendering, exactly where the former collector
# allocated destination snapshot stderr.
zxfer_allocate_full_remote_destination_snapshot_error_stage() {
	g_zxfer_full_destination_snapshot_error_file=""
	zxfer_get_temp_file >/dev/null || return "$?"
	g_zxfer_full_destination_snapshot_error_file=$g_zxfer_temp_file_result
}

# Purpose: Preserve the operator-visible command rendering for remote inventory.
# Usage: Runs after stage allocation and before the one SSH batch, matching the
# prior verbose output and last-command ordering.
zxfer_render_full_remote_destination_inventory_command() {
	if zxfer_command_display_render_enabled; then
		l_full_remote_rendered_command=$(zxfer_render_destination_zfs_command \
			list -t filesystem,volume -Hr -o name "$g_destination")
		zxfer_echoV "Running command: $l_full_remote_rendered_command"
		zxfer_record_last_command_string "$l_full_remote_rendered_command"
	else
		zxfer_record_last_command_opaque
	fi
}

# Purpose: Stage a remote-batch failure diagnostic without modifying outputs.
# Usage: The extra file exists only on failure; a validated SSH diagnostic is
# copied exactly, while malformed protocol failures use an empty stage.
zxfer_stage_full_remote_destination_failure_error() {
	g_zxfer_full_remote_destination_failure_error_file=""
	zxfer_get_temp_file >/dev/null || return "$?"
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
	if zxfer_normalize_destination_snapshot_list \
		"$l_full_remote_normalize_dataset" \
		"$g_zxfer_full_destination_snapshot_file" \
		"$g_zxfer_full_destination_snapshot_sorted_file"; then
		:
	else
		l_full_remote_normalize_status=$?
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
		return "$l_full_remote_normalize_status"
	fi
	zxfer_cleanup_runtime_artifact_paths \
		"$g_zxfer_full_destination_snapshot_error_file" \
		"${g_zxfer_full_remote_destination_failure_error_file:-}" \
		>/dev/null 2>&1 || :
	g_zxfer_full_destination_snapshot_error_file=""
	g_zxfer_full_remote_destination_failure_error_file=""
}

# Purpose: Collect the remote destination inventory and snapshot stream.
# Usage: Called only by full destination discovery for a configured target
# host; protocol stages remain independently testable and cleanup is centralized.
# Returns: Zero with normalized destination files, otherwise the original status.
zxfer_collect_full_remote_destination_snapshot_discovery() {
	l_full_remote_collect_dataset=$1
	if zxfer_allocate_full_remote_destination_inventory_stages; then
		:
	else
		l_full_remote_collect_status=$?
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
		return "$l_full_remote_collect_status"
	fi
	zxfer_render_full_remote_destination_inventory_command
	if zxfer_allocate_full_remote_destination_snapshot_error_stage; then
		:
	else
		l_full_remote_collect_status=$?
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
		return "$l_full_remote_collect_status"
	fi
	if zxfer_run_and_publish_full_remote_destination_discovery_batch \
		"$l_full_remote_collect_dataset"; then
		:
	else
		l_full_remote_collect_status=$?
		zxfer_cleanup_failed_full_remote_destination_snapshot_discovery
		return "$l_full_remote_collect_status"
	fi
	if zxfer_report_full_remote_destination_snapshot_failure; then
		:
	else
		return "$?"
	fi
	zxfer_normalize_full_remote_destination_snapshot_discovery \
		"$l_full_remote_collect_dataset"
}

# Purpose: Wait for and validate the full source snapshot producer.
# Usage: Called after destination collection so source and destination work
# continue to overlap exactly as before.
# Returns: Zero when source staging is complete; failures retain prior messages.
zxfer_wait_for_full_source_snapshot_discovery() {
	zxfer_echoV "Waiting for background processes to finish."
	l_full_source_wait_status=0
	l_full_source_wait_report_failure=""
	if [ -n "${g_source_snapshot_list_job_id:-}" ]; then
		zxfer_wait_for_background_job "$g_source_snapshot_list_job_id" || {
			l_full_source_wait_helper_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$g_zxfer_full_source_snapshot_file" \
				"$g_zxfer_full_destination_snapshot_file" \
				"$g_zxfer_full_destination_snapshot_sorted_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read source snapshot discovery completion metadata." \
				"$l_full_source_wait_helper_status"
		}
		l_full_source_wait_status=$g_zxfer_background_job_wait_exit_status
		l_full_source_wait_report_failure=${g_zxfer_background_job_wait_report_failure:-}
		zxfer_clear_source_snapshot_list_job
	elif [ -n "${g_source_snapshot_list_pid:-}" ]; then
		wait "$g_source_snapshot_list_pid" || l_full_source_wait_status=$?
		zxfer_unregister_cleanup_pid "$g_source_snapshot_list_pid"
		zxfer_clear_source_snapshot_list_job
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_source_snapshot_listing_ms \
		"$g_zxfer_full_source_snapshot_stage_start_ms"

	case $l_full_source_wait_report_failure in
	queue_write)
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to publish source snapshot discovery completion."
		;;
	completion_write)
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_sorted_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to report source snapshot discovery completion."
		;;
	esac

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

# Purpose: Publish full discovery deltas and any retained record caches.
# Usage: Final full-discovery protocol stage after both producers complete;
# performs the normal one-pass cleanup of transient files.
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
	zxfer_cleanup_runtime_artifact_paths \
		"$g_zxfer_full_destination_snapshot_sorted_file"
	zxfer_cleanup_runtime_artifact_path \
		"$g_zxfer_full_source_snapshot_sorted_file"
	zxfer_clear_source_snapshot_list_sorted_file
	if [ "$l_full_publish_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file" \
			"$g_zxfer_full_destination_snapshot_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_publish_status"
	fi

	if zxfer_snapshot_discovery_needs_destination_dataset_inventory; then
		zxfer_collect_local_destination_dataset_inventory || {
			l_full_publish_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$g_zxfer_full_source_snapshot_file" \
				"$g_zxfer_full_source_snapshot_error_file" \
				"$g_zxfer_full_destination_snapshot_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_full_publish_status"
		}
		g_zxfer_full_destination_inventory_attempted=1
	fi

	if zxfer_snapshot_discovery_needs_record_caches; then
		zxfer_publish_full_snapshot_record_caches || return "$?"
	else
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_destination_snapshot_file"
	fi

	zxfer_cleanup_runtime_artifact_path \
		"$g_zxfer_full_source_snapshot_error_file"
	if [ "$g_zxfer_full_destination_inventory_attempted" -eq 1 ] &&
		[ "$g_recursive_dest_list" = "" ]; then
		zxfer_echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi
	return 0
}

# Purpose: Materialize the source and destination record caches retained after
# full discovery.
# Usage: Called only when later snapshot planning needs identity records.
# Returns: Zero with both owner cache globals published, otherwise non-zero.
zxfer_publish_full_snapshot_record_caches() {
	g_zxfer_destination_snapshot_record_cache_file=$g_zxfer_full_destination_snapshot_file
	zxfer_read_snapshot_discovery_capture_file \
		"$g_zxfer_full_destination_snapshot_file" || {
		l_full_cache_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to read staged destination snapshot list." \
			"$l_full_cache_status"
	}
	g_rzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result

	zxfer_read_snapshot_discovery_capture_file \
		"$g_zxfer_full_source_snapshot_file" || {
		l_full_cache_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to read staged source snapshot list." \
			"$l_full_cache_status"
	}
	g_lzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result

	zxfer_get_temp_file >/dev/null || {
		l_full_cache_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_full_cache_status"
	}
	l_full_cache_source_file=$g_zxfer_temp_file_result
	if zxfer_command_display_render_enabled; then
		l_full_cache_command="$(zxfer_render_command_for_report "" \
			zxfer_reverse_file_lines "$g_zxfer_full_source_snapshot_file") > $(zxfer_quote_token_for_report "$l_full_cache_source_file")"
		zxfer_echoV "Running command: $l_full_cache_command"
		zxfer_record_last_command_string "$l_full_cache_command"
	else
		zxfer_record_last_command_opaque
	fi
	zxfer_reverse_file_lines "$g_zxfer_full_source_snapshot_file" \
		>"$l_full_cache_source_file" || {
		l_full_cache_status=$?
		zxfer_cleanup_runtime_artifact_paths \
			"$g_zxfer_full_source_snapshot_file" \
			"$g_zxfer_full_source_snapshot_error_file" \
			"$l_full_cache_source_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to stage source snapshot record cache." \
			"$l_full_cache_status"
	}
	g_zxfer_source_snapshot_record_cache_file=$l_full_cache_source_file
	zxfer_cleanup_runtime_artifact_path "$g_zxfer_full_source_snapshot_file"
	return 0
}

# Purpose: Build the source and destination snapshot inventories that the rest
# of replication planning depends on.
# Usage: Called during source and destination snapshot discovery near the start
# of each live pass so later delete, seed, and send decisions work from one
# shared discovery result.
#
# Build the source and destination snapshot caches used by replication.
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
	zxfer_reset_snapshot_record_indexes

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: skipping live snapshot discovery for $g_initial_source -> $g_destination."
		zxfer_echoV "End zxfer_get_zfs_list()"
		return 0
	fi

	l_get_zfs_list_fast_noop_status=0
	zxfer_try_fast_recursive_noop_discovery ||
		l_get_zfs_list_fast_noop_status=$?
	if [ "$l_get_zfs_list_fast_noop_status" -eq 0 ]; then
		zxfer_echoV "End zxfer_get_zfs_list()"
		return 0
	fi
	if [ "$l_get_zfs_list_fast_noop_status" -ne 1 ]; then
		return "$l_get_zfs_list_fast_noop_status"
	fi

	zxfer_reset_full_snapshot_discovery_operation_state
	zxfer_start_full_source_snapshot_discovery || return "$?"
	zxfer_collect_full_destination_snapshot_discovery || return "$?"
	zxfer_wait_for_full_source_snapshot_discovery || return "$?"
	zxfer_publish_full_snapshot_discovery_results || return "$?"

	zxfer_echoV "End zxfer_get_zfs_list()"
}
