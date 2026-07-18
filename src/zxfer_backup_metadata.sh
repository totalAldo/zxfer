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
# BACKUP METADATA FORMAT / PROVENANCE / RESTORE ORCHESTRATION
################################################################################

# Module contract:
# owns globals: backup metadata accumulation, record/render results, forwarded
#   provenance, restore-selection scratch, and restored backup contents.
# reads globals: secure backup-storage helpers, source/destination dataset
#   context, backup options, property state, and the configured awk helper.
# mutates caches: none.
# returns via stdout: validated metadata records, formatted backup contents,
#   provenance properties, and restored property payloads.

ZXFER_BACKUP_METADATA_HEADER_LINE="#zxfer property backup file"
ZXFER_BACKUP_METADATA_FORMAT_VERSION="2"

# Purpose: Reset the backup metadata state so the next backup-metadata pass
# starts from a clean state.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_backup_metadata_state() {
	g_backup_file_contents=""
	g_pending_backup_file_contents=""
	g_zxfer_backup_metadata_record_list_result=""
	g_zxfer_backup_metadata_record_properties_result=""
	g_zxfer_rendered_backup_metadata_contents=""
	zxfer_reset_backup_storage_state
	g_zxfer_backup_restore_candidate_path_result=""
	g_forwarded_backup_properties=""
	g_restored_backup_file_contents=""
}

# Purpose: Return the backup metadata relative dataset path for a source under
# a metadata root.
# Usage: Called during backup-metadata capture and restore lookup so v2 rows
# are keyed by source-root-relative path instead of by source/destination pairs.
zxfer_backup_metadata_relative_path_for_dataset() {
	l_root=$1
	l_dataset=$2

	if [ -z "$l_root" ] || [ -z "$l_dataset" ]; then
		return 1
	fi
	if [ "$l_dataset" = "$l_root" ]; then
		printf '%s\n' "."
		return 0
	fi
	l_root_prefix=$l_root/
	case "$l_dataset" in
	"$l_root_prefix"*)
		printf '%s\n' "${l_dataset#"$l_root_prefix"}"
		return 0
		;;
	esac

	return 1
}

# Purpose: Return the v2 backup metadata row key for a source dataset.
# Usage: Called by buffered-row helpers that store only source-root-relative
# rows internally.
zxfer_get_backup_metadata_record_key_for_source() {
	l_source=$1
	l_metadata_source_root=${g_initial_source:-$l_source}

	if ! l_record_key=$(zxfer_backup_metadata_relative_path_for_dataset "$l_metadata_source_root" "$l_source"); then
		zxfer_throw_error "Backup metadata source dataset [$l_source] is outside source root [$l_metadata_source_root]."
	fi
	printf '%s\n' "$l_record_key"
}

# Purpose: Validate, deduplicate, and return the backup metadata record list.
# Usage: Called once per write boundary (and for forwarded-provenance
# rendering) before rows are published under a current v2 metadata header.
#
# Buffered appends are plain O(1) string appends, so this single pass is where
# every buffered row is format-checked and where duplicate keys from repeated
# property passes collapse newest-row-wins in first-appearance order. That
# reproduces the row order and values the retired per-append replacement
# produced, while validating each row once per write instead of once per
# append.
zxfer_validate_backup_metadata_record_list() {
	l_existing_records=$1

	# shellcheck disable=SC2016  # awk program should see literal field references.
	if ! l_validated_records=$(printf '%s\n' "$l_existing_records" |
		"${g_cmd_awk:-awk}" '
function validate_properties(properties, item_count, i, field_count) {
	if (properties == "")
		return 0
	item_count = split(properties, prop_items, ",")
	for (i = 1; i <= item_count; i++) {
		if (prop_items[i] == "")
			return 0
		field_count = split(prop_items[i], prop_fields, "=")
		if (field_count < 2 || prop_fields[1] == "" || prop_fields[field_count] == "")
			return 0
	}
	return 1
}
{
	if ($0 == "")
		next
	tab = index($0, "\t")
	if (tab <= 0)
		exit 3
	current_key = substr($0, 1, tab - 1)
	current_properties = substr($0, tab + 1)
	if (current_key == "" || !validate_properties(current_properties))
		exit 3
	if (!(current_key in row_properties)) {
		row_count++
		row_keys[row_count] = current_key
	}
	row_properties[current_key] = current_properties
}
END {
	for (row_index = 1; row_index <= row_count; row_index++) {
		line = row_keys[row_index] "\t" row_properties[row_keys[row_index]]
		if (output == "")
			output = line
		else
			output = output "\n" line
	}
	printf "%s", output
}'); then
		zxfer_throw_error "Failed to validate buffered backup metadata records for chained backup provenance."
	fi

	g_zxfer_backup_metadata_record_list_result=$l_validated_records
	printf '%s\n' "$l_validated_records"
}

# Purpose: Append the v2 row for a source dataset to a buffered record list.
# Usage: Called by the append, defer, and finalize buffering helpers; the
# extended list is returned through the record-list result scratch channel.
#
# This is a plain O(1) string append. Duplicate keys are legitimate transient
# buffer state; they collapse newest-row-wins inside
# zxfer_validate_backup_metadata_record_list at the write boundary.
zxfer_append_backup_metadata_row_to_record_list() {
	l_existing_records=$1
	l_source=$2
	l_properties=$3

	l_record_key=$(zxfer_get_backup_metadata_record_key_for_source "$l_source") ||
		zxfer_throw_error "Backup metadata source dataset [$l_source] is outside source root [${g_initial_source:-$l_source}]."

	if [ -n "$l_existing_records" ]; then
		g_zxfer_backup_metadata_record_list_result="$l_existing_records
$l_record_key	$l_properties"
	else
		g_zxfer_backup_metadata_record_list_result="$l_record_key	$l_properties"
	fi
	return 0
}

# Purpose: Append the backup metadata record to the module-owned accumulator.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need one shared place to extend staged or in-memory
# state.
#
# Buffering stays O(1) per row; full row validation runs once per write
# boundary, so a malformed buffered row surfaces when the metadata file is
# written instead of on the append that follows it.
zxfer_append_backup_metadata_record() {
	l_source=$1
	l_properties=$2

	zxfer_append_backup_metadata_row_to_record_list "${g_backup_file_contents:-}" \
		"$l_source" "$l_properties"
	g_backup_file_contents=$g_zxfer_backup_metadata_record_list_result
}

# Purpose: Return the buffered backup metadata record properties in the form
# expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
#
# Buffered rows are plain appends, so duplicate keys are legitimate transient
# state: the newest buffered row for a key wins, mirroring the newest-row-wins
# collapse the write boundary applies.
zxfer_get_buffered_backup_metadata_record_properties() {
	l_existing_records=$1
	l_source=$2
	l_record_key=$(zxfer_get_backup_metadata_record_key_for_source "$l_source")

	# shellcheck disable=SC2016  # awk program should see literal field references.
	if l_record_properties=$(printf '%s\n' "$l_existing_records" |
		ZXFER_BACKUP_METADATA_RECORD_KEY=$l_record_key \
			"${g_cmd_awk:-awk}" '
function validate_properties(properties, item_count, i, field_count) {
	if (properties == "")
		return 0
	item_count = split(properties, prop_items, ",")
	for (i = 1; i <= item_count; i++) {
		if (prop_items[i] == "")
			return 0
		field_count = split(prop_items[i], prop_fields, "=")
		if (field_count < 2 || prop_fields[1] == "" || prop_fields[field_count] == "")
			return 0
	}
	return 1
}
BEGIN {
	record_key = ENVIRON["ZXFER_BACKUP_METADATA_RECORD_KEY"]
}
{
	if ($0 == "")
		next
	tab = index($0, "\t")
	if (tab <= 0) {
		malformed = 1
		next
	}
	current_key = substr($0, 1, tab - 1)
	current_properties = substr($0, tab + 1)
	if (current_key == "" || !validate_properties(current_properties)) {
		malformed = 1
		next
	}
	if (current_key == record_key) {
		match_count++
		match_properties = current_properties
	}
}
END {
	if (malformed)
		exit 3
	if (match_count == 0)
		exit 1
	print match_properties
	exit 0
}'); then
		:
	else
		l_status=$?
		case $l_status in
		1 | 3)
			g_zxfer_backup_metadata_record_properties_result=""
			return "$l_status"
			;;
		*)
			zxfer_throw_error "Failed to inspect buffered backup metadata records."
			;;
		esac
	fi

	g_zxfer_backup_metadata_record_properties_result=$l_record_properties
	printf '%s\n' "$l_record_properties"
}

# Purpose: Remove the backup metadata record list from the current working set
# while preserving the module's special-case rules.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when filtering logic must trim staged data before later reconciliation
# or apply steps run.
zxfer_remove_backup_metadata_record_list() {
	l_existing_records=$1
	l_source=$2
	l_record_key=$(zxfer_get_backup_metadata_record_key_for_source "$l_source")

	# shellcheck disable=SC2016  # awk program should see literal field references.
	if ! l_filtered_records=$(printf '%s\n' "$l_existing_records" |
		ZXFER_BACKUP_METADATA_RECORD_KEY=$l_record_key \
			"${g_cmd_awk:-awk}" '
function append_line(line) {
	if (line == "")
		return
	if (output == "")
		output = line
	else
		output = output "\n" line
}
BEGIN {
	record_key = ENVIRON["ZXFER_BACKUP_METADATA_RECORD_KEY"]
}
{
	if ($0 == "")
		next
	tab = index($0, "\t")
	if (tab <= 0) {
		append_line($0)
		next
	}
	current_key = substr($0, 1, tab - 1)
	if (current_key == record_key)
		next
	append_line($0)
}
END {
	printf "%s", output
}'); then
		zxfer_throw_error "Failed to remove buffered backup metadata records."
	fi

	g_zxfer_backup_metadata_record_list_result=$l_filtered_records
	printf '%s\n' "$l_filtered_records"
}

# Purpose: Defer the buffered backup metadata record until a later checkpoint
# in the run.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer has to preserve state now but can only commit it safely
# after later work succeeds.
zxfer_defer_buffered_backup_metadata_record() {
	l_source=$1

	[ "${g_option_k_backup_property_mode:-0}" -eq 1 ] || return 0
	[ "${g_option_n_dryrun:-0}" -eq 0 ] || return 0

	if zxfer_get_buffered_backup_metadata_record_properties "${g_backup_file_contents:-}" \
		"$l_source" >/dev/null; then
		:
	else
		l_live_lookup_status=$?
		case $l_live_lookup_status in
		1)
			zxfer_throw_error "Buffered backup metadata row for source dataset [$l_source] is missing."
			;;
		3)
			zxfer_throw_error "Buffered backup metadata rows are malformed while deferring source dataset [$l_source]."
			;;
		*)
			zxfer_throw_error "Failed to inspect buffered backup metadata row for source dataset [$l_source]."
			;;
		esac
	fi
	l_buffered_properties=$g_zxfer_backup_metadata_record_properties_result

	zxfer_remove_backup_metadata_record_list "${g_backup_file_contents:-}" "$l_source" >/dev/null
	l_next_backup_file_contents=$g_zxfer_backup_metadata_record_list_result
	zxfer_append_backup_metadata_row_to_record_list "${g_pending_backup_file_contents:-}" \
		"$l_source" "$l_buffered_properties"
	l_next_pending_backup_file_contents=$g_zxfer_backup_metadata_record_list_result

	g_backup_file_contents=$l_next_backup_file_contents
	g_pending_backup_file_contents=$l_next_pending_backup_file_contents
}

# Purpose: Finalize the deferred backup metadata record once all prerequisites
# have succeeded.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after staged or deferred work is ready to become the module's final
# result.
zxfer_finalize_deferred_backup_metadata_record() {
	l_source=$1

	[ "${g_option_k_backup_property_mode:-0}" -eq 1 ] || return 0
	[ "${g_option_n_dryrun:-0}" -eq 0 ] || return 0

	if zxfer_get_buffered_backup_metadata_record_properties "${g_pending_backup_file_contents:-}" \
		"$l_source" >/dev/null; then
		:
	else
		l_pending_lookup_status=$?
		case $l_pending_lookup_status in
		1)
			zxfer_throw_error "Deferred backup metadata row for source dataset [$l_source] is missing."
			;;
		3)
			zxfer_throw_error "Deferred backup metadata rows are malformed while finalizing source dataset [$l_source]."
			;;
		*)
			zxfer_throw_error "Failed to inspect deferred backup metadata row for source dataset [$l_source]."
			;;
		esac
	fi
	l_deferred_properties=$g_zxfer_backup_metadata_record_properties_result

	zxfer_remove_backup_metadata_record_list "${g_pending_backup_file_contents:-}" "$l_source" >/dev/null
	l_next_pending_backup_file_contents=$g_zxfer_backup_metadata_record_list_result
	zxfer_append_backup_metadata_row_to_record_list "${g_backup_file_contents:-}" \
		"$l_source" "$l_deferred_properties"
	l_next_backup_file_contents=$g_zxfer_backup_metadata_record_list_result

	g_pending_backup_file_contents=$l_next_pending_backup_file_contents
	g_backup_file_contents=$l_next_backup_file_contents
}

# Purpose: Capture the backup metadata for completed transfer into staged state
# or module globals for later use.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a checked snapshot of command output or
# computed state.
#
# Record backup metadata only after a dataset property pass succeeds. Live runs
# keep the captured rows buffered in memory until orchestration decides the
# dataset or iteration is safe to persist.
zxfer_capture_backup_metadata_for_completed_transfer() {
	l_source=$1
	l_properties=$2
	l_skip_backup_capture=${3:-0}

	[ "${g_option_k_backup_property_mode:-0}" -eq 1 ] || return 0
	[ "$l_skip_backup_capture" -eq 0 ] || return 0

	if [ "${g_option_n_dryrun:-0}" -eq 0 ] && [ -n "${g_backup_file_extension:-}" ]; then
		if zxfer_get_forwarded_backup_properties_for_source "$l_source" >/dev/null; then
			l_properties=$g_forwarded_backup_properties
		else
			l_forwarded_lookup_status=$?
			if [ "$l_forwarded_lookup_status" -ne 1 ]; then
				zxfer_throw_error "Failed to derive forwarded backup properties for source dataset [$l_source]."
			fi
		fi
	fi

	zxfer_append_backup_metadata_record "$l_source" "$l_properties"
}

# Purpose: Flush the captured backup metadata if live that was buffered earlier
# in the run.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when staged state is ready to move from deferred accumulation into its
# final destination.
#
# Persist the currently buffered backup metadata when live orchestration has
# finished the part of the dataset flow that should survive later failures.
# Dry runs keep the existing one-shot final preview behavior.
zxfer_flush_captured_backup_metadata_if_live() {
	[ "${g_option_k_backup_property_mode:-0}" -eq 1 ] || return 0
	[ "${g_option_n_dryrun:-0}" -eq 0 ] || return 0
	[ -n "${g_backup_file_contents:-}" ] || return 0

	l_saved_failure_stage=${g_zxfer_failure_stage:-startup}
	zxfer_write_backup_properties
	zxfer_set_failure_stage "$l_saved_failure_stage"
}

# Purpose: Validate the backup metadata format before zxfer relies on it.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows to fail closed on malformed, unsafe, or stale input.
zxfer_validate_backup_metadata_format() {
	l_backup_contents=$1
	l_expected_header=$ZXFER_BACKUP_METADATA_HEADER_LINE
	l_expected_format_version=$ZXFER_BACKUP_METADATA_FORMAT_VERSION

	# shellcheck disable=SC2016
	printf '%s\n' "$l_backup_contents" | "${g_cmd_awk:-awk}" \
		-v expected_header="$l_expected_header" \
		-v expected_format_version="$l_expected_format_version" '
	{
		if (!header_seen) {
			if ($0 == "") {
				preamble_invalid = 1
				next
			}
			if ($0 != expected_header) {
				preamble_invalid = 1
				next
			}
			header_count++
			header_seen = 1
			next
		}
		if (!format_seen && $0 != "" && substr($0, 1, 1) != "#") {
			preamble_invalid = 1
			next
		}
		if (index($0, "#format_version:") == 1) {
			format_count++
			if (seen_data)
				preamble_invalid = 1
			format_value = substr($0, length("#format_version:") + 1)
			if (format_value == expected_format_version)
				format_ok = 1
			else
				format_invalid = 1
			format_seen = 1
			next
		}
		if ($0 == expected_header) {
			header_count++
			preamble_invalid = 1
			next
		}
		if ($0 != "" && substr($0, 1, 1) != "#")
			seen_data = 1
	}
	END {
		if (header_count != 1 || preamble_invalid)
			exit 1
		if (format_count != 1 || format_invalid || !format_ok)
			exit 2
		exit 0
	}'
}

# Purpose: Render the backup metadata contents for roots as a stable shell-safe
# or operator-facing string.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs to display or transport the value without reparsing
# it.
zxfer_render_backup_metadata_contents_for_roots() {
	l_source_root=$1
	l_destination_root=$2
	l_record_list=$3
	l_backup_date=$(date)

	{
		printf '%s\n' "$ZXFER_BACKUP_METADATA_HEADER_LINE"
		printf '%s\n' "#format_version:$ZXFER_BACKUP_METADATA_FORMAT_VERSION"
		printf '%s\n' "#version:$g_zxfer_version"
		printf '%s\n' "#R options:$g_option_R_recursive"
		printf '%s\n' "#N options:$g_option_N_nonrecursive"
		printf '%s\n' "#source_root:$l_source_root"
		printf '%s\n' "#destination_root:$l_destination_root"
		printf '%s\n' "#backup_date:$l_backup_date"
		if [ -n "${l_record_list:-}" ]; then
			printf '%s\n' "$l_record_list"
		fi
	}
}

# Purpose: Render the backup metadata contents as a stable shell-safe or
# operator-facing string.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs to display or transport the value without reparsing
# it.
zxfer_render_backup_metadata_contents() {
	l_backup_destination_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	g_zxfer_rendered_backup_metadata_contents=$(zxfer_render_backup_metadata_contents_for_roots \
		"$g_initial_source" "$l_backup_destination_root" "${g_backup_file_contents:-}")
	printf '%s\n' "$g_zxfer_rendered_backup_metadata_contents"
}

# Purpose: Render the forwarded backup metadata contents as a stable shell-safe
# or operator-facing string.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs to display or transport the value without reparsing
# it.
zxfer_render_forwarded_backup_metadata_contents() {
	l_forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	zxfer_validate_backup_metadata_record_list "${g_backup_file_contents:-}" >/dev/null
	l_forwarded_records=$g_zxfer_backup_metadata_record_list_result

	g_zxfer_rendered_backup_metadata_contents=$(zxfer_render_backup_metadata_contents_for_roots \
		"$l_forwarded_root" "$l_forwarded_root" "$l_forwarded_records")
	printf '%s\n' "$g_zxfer_rendered_backup_metadata_contents"
}

# Purpose: Return the forwarded backup properties for source in the form
# expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_forwarded_backup_properties_for_source() {
	l_source=$1
	l_saved_restored_backup_file_contents=${g_restored_backup_file_contents:-}
	l_suspect_fs=$l_source
	g_forwarded_backup_properties=""

	while :; do
		l_dataset_secure_dir=$(zxfer_get_backup_storage_dir_for_dataset_tree "$l_suspect_fs")
		if zxfer_try_backup_restore_candidate_set "$l_dataset_secure_dir" "$l_suspect_fs" "$l_suspect_fs" "$l_source" "$l_source" "$g_option_O_origin_host" source; then
			l_backup_match_status=0
		else
			l_backup_match_status=$?
		fi
		l_dataset_backup_file=$g_zxfer_backup_restore_candidate_path_result
		case $l_backup_match_status in
		0)
			l_forwarded_properties=$(zxfer_backup_metadata_extract_properties_for_dataset_pair \
				"$g_restored_backup_file_contents" "$l_source" "$l_source") || {
				g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
				zxfer_throw_error "Failed to extract forwarded backup properties from $l_dataset_backup_file for source dataset $l_source."
			}
			g_forwarded_backup_properties=$l_forwarded_properties
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			printf '%s\n' "$l_forwarded_properties"
			return 0
			;;
		1) ;;
		11)
			break
			;;
		3)
			if [ "$l_suspect_fs" = "$l_source" ]; then
				g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
				zxfer_throw_error "Forwarded backup property file $l_dataset_backup_file does not contain a current-format relative row for source dataset $l_source."
			fi
			;;
		2)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Forwarded backup property file $l_dataset_backup_file contains multiple relative rows for source dataset $l_source."
			;;
		4)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Forwarded backup property file $l_dataset_backup_file is malformed. Expected current-format relative-path and properties rows."
			;;
		5)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Failed to read forwarded backup property file $l_dataset_backup_file."
			;;
		10)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Failed to stage local forwarded backup property file $l_dataset_backup_file for secure read."
			;;
		8)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Failed to contact origin host $g_option_O_origin_host while reading forwarded backup property file $l_dataset_backup_file. Review prior stderr for the transport or authentication error."
			;;
		6)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Forwarded backup property file $l_dataset_backup_file does not start with the required zxfer backup metadata header."
			;;
		7)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Forwarded backup property file $l_dataset_backup_file does not declare supported zxfer backup metadata format version #format_version:$ZXFER_BACKUP_METADATA_FORMAT_VERSION."
			;;
		*)
			g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
			zxfer_throw_error "Failed to validate forwarded backup property file $l_dataset_backup_file."
			;;
		esac

		l_suspect_fs_parent=$(echo "$l_suspect_fs" | sed -e 's%/[^/]*$%%g')
		if [ "$l_suspect_fs_parent" = "$l_suspect_fs" ]; then
			break
		fi
		l_suspect_fs=$l_suspect_fs_parent
	done

	g_restored_backup_file_contents=$l_saved_restored_backup_file_contents
	return 1
}

# Purpose: Handle backup metadata metadata extract properties for dataset pair
# for the backup/restore flow.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when backup capture, lookup, or publish logic needs one shared helper.
zxfer_backup_metadata_extract_properties_for_dataset_pair() {
	l_backup_contents=$1
	l_expected_source=$2
	l_expected_destination=$3

	# shellcheck disable=SC2016
	printf '%s\n' "$l_backup_contents" | "${g_cmd_awk:-awk}" \
		-v expected_source="$l_expected_source" \
		-v expected_destination="$l_expected_destination" '
function relative_path(root, dataset, prefix) {
	if (root == "" || dataset == "")
		return ""
	if (dataset == root)
		return "."
	prefix = root "/"
	if (substr(dataset, 1, length(prefix)) == prefix)
		return substr(dataset, length(prefix) + 1)
	return "__ZXFER_NO_MATCH__"
}
function validate_properties(properties, item_count, i, field_count) {
	if (properties == "")
		return 0
	item_count = split(properties, prop_items, ",")
	for (i = 1; i <= item_count; i++) {
		if (prop_items[i] == "")
			return 0
		field_count = split(prop_items[i], prop_fields, "=")
		if (field_count < 2 || prop_fields[1] == "" || prop_fields[field_count] == "")
			return 0
	}
	return 1
}
{
	if (index($0, "#source_root:") == 1) {
		source_root_count++
		source_root = substr($0, length("#source_root:") + 1)
		next
	}
	if (index($0, "#destination_root:") == 1) {
		destination_root_count++
		destination_root = substr($0, length("#destination_root:") + 1)
		next
	}
	if ($0 == "" || substr($0, 1, 1) == "#")
		next

	tab = index($0, "\t")
	if (tab <= 0) {
		malformed_count++
		next
	}
	row_key = substr($0, 1, tab - 1)
	props = substr($0, tab + 1)
	if (row_key == "" || row_key ~ /^\// || row_key ~ /\/$/ || !validate_properties(props)) {
		malformed_count++
		next
	}
	body_count++
	row_properties[row_key] = props
	row_count[row_key]++
}
END {
	if (source_root_count != 1 || destination_root_count != 1 || source_root == "" || destination_root == "")
		exit 3
	expected_source_key = relative_path(source_root, expected_source)
	expected_destination_key = relative_path(destination_root, expected_destination)
	if (expected_source_key == "" || expected_destination_key == "" ||
		expected_source_key == "__ZXFER_NO_MATCH__" ||
		expected_destination_key == "__ZXFER_NO_MATCH__" ||
		expected_source_key != expected_destination_key)
		exit 1
	if (malformed_count > 0)
		exit 3
	if (row_count[expected_source_key] == 1) {
		print row_properties[expected_source_key]
		exit 0
	}
	if (row_count[expected_source_key] == 0)
		exit 1
	exit 2
}'
}

# Purpose: Check whether the backup metadata matches source.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a boolean answer about the backup metadata.
zxfer_backup_metadata_matches_source() {
	l_backup_contents=$1
	l_expected_source=$2
	l_expected_destination=$3

	zxfer_backup_metadata_extract_properties_for_dataset_pair "$l_backup_contents" "$l_expected_source" "$l_expected_destination" >/dev/null
}

# Purpose: Return the expected backup destination for source in the form
# expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_expected_backup_destination_for_source() {
	l_source=$1

	zxfer_get_destination_dataset_for_source_dataset "$l_source"
}

# Purpose: Try to resolve or create the backup restore candidate without
# treating every miss as fatal.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer has an optional exact candidate that still needs one checked
# helper.
zxfer_try_backup_restore_candidate() {
	l_candidate=$1
	l_expected_source=$2
	l_expected_destination=$3
	l_host=${4:-}
	l_profile_side=${5:-}
	l_missing_status=4
	l_remote_transport_status=6
	l_transport_failure_status=8
	l_remote_capture_status=7
	l_capture_failure_status=9
	l_local_staging_status=10

	if [ "$l_host" = "" ]; then
		if zxfer_read_local_backup_file "$l_candidate" >/dev/null; then
			l_read_status=0
			l_backup_contents=$g_zxfer_backup_file_read_result
		else
			l_read_status=$?
			if [ "$l_read_status" -eq "$l_missing_status" ]; then
				return 1
			fi
			if [ "${g_zxfer_backup_local_read_failure_result:-}" = "staging" ]; then
				return "$l_local_staging_status"
			fi
			return 5
		fi
	else
		if zxfer_read_remote_backup_file "$l_host" "$l_candidate" "$l_profile_side" >/dev/null; then
			l_read_status=0
			l_backup_contents=$g_zxfer_backup_file_read_result
		else
			l_read_status=$?
			if [ "$l_read_status" -eq "$l_missing_status" ]; then
				return 1
			fi
			if [ "$l_read_status" -eq "$l_remote_transport_status" ]; then
				return "$l_transport_failure_status"
			fi
			if [ "$l_read_status" -eq "$l_remote_capture_status" ]; then
				return "$l_capture_failure_status"
			fi
			return 5
		fi
	fi

	if zxfer_validate_backup_metadata_format "$l_backup_contents"; then
		l_format_status=0
	else
		l_format_status=$?
	fi
	case $l_format_status in
	0) ;;
	1)
		return 6
		;;
	2)
		return 7
		;;
	*)
		return 5
		;;
	esac

	if zxfer_backup_metadata_matches_source "$l_backup_contents" "$l_expected_source" "$l_expected_destination"; then
		l_match_status=0
	else
		l_match_status=$?
	fi
	case $l_match_status in
	0) ;;
	1)
		return 3
		;;
	2)
		return 2
		;;
	3)
		return 4
		;;
	*)
		return 5
		;;
	esac

	g_restored_backup_file_contents=$l_backup_contents
	return 0
}

# Purpose: Try the current exact backup metadata path and then the retired
# cksum-keyed path when the current path is absent.
# Usage: Called by restore lookup helpers that need to keep existing v2
# metadata readable while new writes use lossless identity filenames.
zxfer_try_backup_restore_candidate_set() {
	l_candidate_dir=$1
	l_filename_source=$2
	l_filename_destination=$3
	l_expected_source=$4
	l_expected_destination=$5
	l_host=${6:-}
	l_profile_side=${7:-}
	g_zxfer_backup_restore_candidate_path_result=""

	l_current_backup_file_name=$(zxfer_get_backup_metadata_filename "$l_filename_source" "$l_filename_destination") ||
		return 11
	l_current_candidate=$l_candidate_dir/$l_current_backup_file_name
	g_zxfer_backup_restore_candidate_path_result=$l_current_candidate
	if zxfer_try_backup_restore_candidate "$l_current_candidate" "$l_expected_source" "$l_expected_destination" "$l_host" "$l_profile_side"; then
		return 0
	else
		l_current_status=$?
	fi
	if [ "$l_current_status" -ne 1 ]; then
		return "$l_current_status"
	fi

	l_legacy_backup_file_name=$(zxfer_get_legacy_backup_metadata_filename "$l_filename_source" "$l_filename_destination") ||
		return 1
	if [ "$l_legacy_backup_file_name" = "$l_current_backup_file_name" ]; then
		return 1
	fi
	l_legacy_candidate=$l_candidate_dir/$l_legacy_backup_file_name
	g_zxfer_backup_restore_candidate_path_result=$l_legacy_candidate
	if zxfer_try_backup_restore_candidate "$l_legacy_candidate" "$l_expected_source" "$l_expected_destination" "$l_host" "$l_profile_side"; then
		return 0
	else
		l_legacy_status=$?
	fi
	if [ "$l_legacy_status" -eq 1 ]; then
		g_zxfer_backup_restore_candidate_path_result=$l_current_candidate
	fi
	return "$l_legacy_status"
}

# Purpose: Return the backup properties in the form expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
#
# Gets the backup properties from a previous backup of those properties
# This takes $g_initial_source. Secure backup metadata is keyed by the exact
# current source/destination root pair under ZXFER_BACKUP_DIR; recursive child
# restores are resolved by relative rows inside that one v2 file.
zxfer_get_backup_properties() {
	zxfer_set_failure_stage "backup metadata read"
	zxfer_refresh_backup_storage_root

	l_expected_root_destination=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	l_dataset_secure_dir=$(zxfer_get_backup_storage_dir_for_dataset_tree "$g_initial_source")
	if zxfer_try_backup_restore_candidate_set "$l_dataset_secure_dir" "$g_initial_source" "$g_destination" "$g_initial_source" "$l_expected_root_destination" "$g_option_O_origin_host" source; then
		l_backup_match_status=0
	else
		l_backup_match_status=$?
	fi
	l_dataset_backup_file=$g_zxfer_backup_restore_candidate_path_result
	case $l_backup_match_status in
	0) ;;
	11)
		zxfer_throw_error "Failed to derive backup metadata filename for source dataset [$g_initial_source]."
		;;
	1)
		zxfer_throw_error_with_usage "Cannot find backup property file. Ensure that it
exists under the source-dataset-relative tree inside ZXFER_BACKUP_DIR."
		;;
	2)
		zxfer_throw_error_with_usage "Backup property file $l_dataset_backup_file contains multiple relative rows for source dataset $g_initial_source. Remove the ambiguous rows or restore from a specific exact backup path."
		;;
	3)
		zxfer_throw_error_with_usage "Backup property file $l_dataset_backup_file does not contain a current-format relative row for source dataset $g_initial_source."
		;;
	4)
		zxfer_throw_error_with_usage "Backup property file $l_dataset_backup_file is malformed. Expected current-format relative-path and properties rows."
		;;
	6)
		zxfer_throw_error_with_usage "Backup property file $l_dataset_backup_file does not start with the required zxfer backup metadata header."
		;;
	7)
		zxfer_throw_error_with_usage "Backup property file $l_dataset_backup_file does not declare supported zxfer backup metadata format version #format_version:$ZXFER_BACKUP_METADATA_FORMAT_VERSION."
		;;
	8)
		zxfer_throw_error "Failed to contact source host $g_option_O_origin_host while reading backup property file $l_dataset_backup_file. Review prior stderr for the transport or authentication error."
		;;
	9)
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_throw_error "Failed to reload local remote helper capture while reading backup property file $l_dataset_backup_file on host $g_option_O_origin_host."
		;;
	5)
		zxfer_throw_error "Failed to read backup property file $l_dataset_backup_file."
		;;
	10)
		zxfer_throw_error "Failed to stage local backup property file $l_dataset_backup_file for secure read."
		;;
	esac

	# g_restored_backup_file_contents now holds v2 metadata with
	# relative-path/property rows under source_root and destination_root.
}

# Purpose: Write the backup metadata contents to store in the normalized form
# later zxfer steps expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
zxfer_write_backup_metadata_contents_to_store() {
	l_backup_file_dir=$1
	l_backup_file_path=$2
	l_rendered_backup_contents=$3

	if [ "$g_option_T_target_host" = "" ]; then
		zxfer_ensure_local_backup_dir "$g_backup_storage_root"
		zxfer_ensure_local_backup_dir "$l_backup_file_dir"
		zxfer_require_backup_write_target_path "$l_backup_file_path"
		if ! zxfer_write_local_backup_file_atomically "$l_backup_file_path" "$l_rendered_backup_contents"; then
			if [ "${g_zxfer_backup_local_write_failure_result:-}" = "staging" ]; then
				zxfer_throw_error "Failed to stage local backup file $l_backup_file_path for atomic write."
			fi
			if [ "${g_zxfer_backup_local_write_failure_result:-}" = "rollback" ]; then
				zxfer_throw_backup_write_rollback_error
			fi
			zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
		fi
		return 0
	fi

	zxfer_ensure_remote_backup_dir "$g_backup_storage_root" "$g_option_T_target_host" destination
	zxfer_ensure_remote_backup_dir "$l_backup_file_dir" "$g_option_T_target_host" destination
	if ! l_remote_write_helper_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "cat" "cat" destination); then
		zxfer_set_failure_class dependency
		zxfer_throw_error "$l_remote_write_helper_safe"
	fi
	l_remote_dependency_status=99
	l_remote_write_failure_status=92
	l_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
	l_remote_write_cmd=$(zxfer_build_remote_backup_write_cmd "$l_backup_file_dir" "$l_backup_file_path" "$g_option_T_target_host" "$l_remote_write_helper_safe" "$l_remote_dependency_status" "$l_remote_write_failure_status")
	l_remote_write_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_write_cmd")
	l_remote_write_payload=$(printf '%s\n' "$l_rendered_backup_contents")
	if zxfer_run_remote_backup_helper_with_payload "$g_option_T_target_host" "$l_remote_write_shell_cmd" "$l_remote_write_payload" destination; then
		l_remote_write_status=0
	else
		l_remote_write_status=$?
	fi
	zxfer_throw_remote_backup_write_status "$l_remote_write_status" \
		"$l_remote_dependency_status" "$l_remote_write_failure_status" "" \
		"$g_option_T_target_host" "writing backup metadata $l_backup_file_path" \
		"$l_dependency_path"
}

# Purpose: Write the backup metadata pair contents to store in the normalized
# form later zxfer steps expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
zxfer_write_backup_metadata_pair_contents_to_store() {
	l_primary_backup_file_dir=$1
	l_primary_backup_file_path=$2
	l_primary_rendered_backup_contents=$3
	l_forwarded_backup_file_dir=$4
	l_forwarded_backup_file_path=$5
	l_forwarded_backup_contents=$6

	if [ "$g_option_T_target_host" = "" ]; then
		zxfer_ensure_local_backup_dir "$g_backup_storage_root"
		zxfer_ensure_local_backup_dir "$l_primary_backup_file_dir"
		zxfer_ensure_local_backup_dir "$l_forwarded_backup_file_dir"
		zxfer_require_backup_write_target_path "$l_primary_backup_file_path"
		zxfer_require_backup_write_target_path "$l_forwarded_backup_file_path"
		zxfer_write_local_backup_file_pair_atomically "$l_primary_backup_file_path" "$l_primary_rendered_backup_contents" "$l_forwarded_backup_file_path" "$l_forwarded_backup_contents"
		l_local_pair_write_status=$?
		if [ "$l_local_pair_write_status" -eq 0 ]; then
			return 0
		fi
		if [ "$l_local_pair_write_status" -eq 2 ] ||
			[ "${g_zxfer_backup_local_write_failure_result:-}" = "rollback" ]; then
			zxfer_throw_backup_write_rollback_error
		elif [ "${g_zxfer_backup_local_write_failure_result:-}" = "staging" ]; then
			zxfer_throw_error "Failed to stage local backup file pair for atomic write."
		else
			zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
		fi
		return 0
	fi

	zxfer_ensure_remote_backup_dir "$g_backup_storage_root" "$g_option_T_target_host" destination
	zxfer_ensure_remote_backup_dir "$l_primary_backup_file_dir" "$g_option_T_target_host" destination
	zxfer_ensure_remote_backup_dir "$l_forwarded_backup_file_dir" "$g_option_T_target_host" destination
	l_remote_dependency_status=99
	l_remote_write_failure_status=92
	l_remote_rollback_failure_status=98
	l_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
	l_remote_pair_write_cmd=$(zxfer_build_remote_backup_pair_write_cmd "$l_primary_backup_file_dir" "$l_primary_backup_file_path" "$l_forwarded_backup_file_dir" "$l_forwarded_backup_file_path" "$g_option_T_target_host" "$l_remote_dependency_status" "$l_remote_write_failure_status")
	l_remote_pair_write_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_pair_write_cmd")
	l_pair_split_line=$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE
	l_remote_pair_payload=$(printf '%s\n%s\n%s\n' "$l_primary_rendered_backup_contents" "$l_pair_split_line" "$l_forwarded_backup_contents")
	if zxfer_run_remote_backup_helper_with_payload "$g_option_T_target_host" "$l_remote_pair_write_shell_cmd" "$l_remote_pair_payload" destination; then
		l_remote_write_status=0
	else
		l_remote_write_status=$?
	fi
	zxfer_throw_remote_backup_write_status "$l_remote_write_status" \
		"$l_remote_dependency_status" "$l_remote_write_failure_status" \
		"$l_remote_rollback_failure_status" "$g_option_T_target_host" \
		"writing backup metadata $l_primary_backup_file_path" "$l_dependency_path"
}

# Purpose: Write the backup properties in the normalized form later zxfer steps
# expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
#
# Writes the backup properties to a file in the source-dataset-relative secure
# backup tree under ZXFER_BACKUP_DIR. That keeps -k and -e keyed from the same
# stable identifier set even when source and destination mountpoints differ.
zxfer_write_backup_properties() {
	zxfer_set_failure_stage "backup metadata write"

	if [ "$g_backup_file_contents" = "" ]; then
		zxfer_echov "No property data collected; skipping backup write."
		return
	fi

	# Validate-once boundary: appends buffer rows without per-row awk passes,
	# so every buffered row is format-checked here and duplicate keys collapse
	# newest-row-wins before anything is rendered or published. The buffer is
	# replaced by its canonical equivalent so later flushes and deferred-row
	# lookups start from the compacted list.
	zxfer_validate_backup_metadata_record_list "$g_backup_file_contents" >/dev/null
	g_backup_file_contents=$g_zxfer_backup_metadata_record_list_result

	zxfer_refresh_backup_storage_root
	l_backup_file_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	l_backup_file_dir=$(zxfer_get_backup_storage_dir_for_dataset_tree "$g_initial_source")
	l_backup_file_path=$l_backup_file_dir/$l_backup_file_name
	if ! l_backup_file_parent=$(zxfer_get_path_parent_dir "$l_backup_file_path"); then
		zxfer_throw_error "Failed to derive backup metadata directory for $l_backup_file_path."
	fi
	zxfer_echov "Writing backup info to secure path $l_backup_file_path (dataset $g_initial_source)"

	# Construct the backup file contents without mutating the owner scratch state.
	zxfer_render_backup_metadata_contents >/dev/null
	l_rendered_backup_contents=$g_zxfer_rendered_backup_metadata_contents
	l_has_forwarded_backup_alias=0
	l_forwarded_backup_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	l_forwarded_backup_file_name=$(zxfer_get_forwarded_backup_metadata_filename "$l_forwarded_backup_root")
	l_forwarded_backup_file_dir=$(zxfer_get_backup_storage_dir_for_dataset_tree "$l_forwarded_backup_root")
	l_forwarded_backup_file_path=$l_forwarded_backup_file_dir/$l_forwarded_backup_file_name
	if ! l_forwarded_backup_file_parent=$(zxfer_get_path_parent_dir "$l_forwarded_backup_file_path"); then
		zxfer_throw_error "Failed to derive forwarded backup metadata directory for $l_forwarded_backup_file_path."
	fi
	if [ "$l_forwarded_backup_file_path" != "$l_backup_file_path" ]; then
		l_has_forwarded_backup_alias=1
		zxfer_render_forwarded_backup_metadata_contents >/dev/null
		l_forwarded_backup_contents=$g_zxfer_rendered_backup_metadata_contents
	fi

	# Execute the command
	if [ "$g_option_n_dryrun" -eq 0 ]; then
		if [ "$l_has_forwarded_backup_alias" -eq 1 ]; then
			zxfer_write_backup_metadata_pair_contents_to_store "$l_backup_file_parent" "$l_backup_file_path" "$l_rendered_backup_contents" "$l_forwarded_backup_file_parent" "$l_forwarded_backup_file_path" "$l_forwarded_backup_contents"
		else
			zxfer_write_backup_metadata_contents_to_store "$l_backup_file_parent" "$l_backup_file_path" "$l_rendered_backup_contents"
		fi
	else
		l_backup_contents_cmd=$(zxfer_render_command_for_report "" printf '%s' "$l_rendered_backup_contents")
		l_backup_stage_template_safe=$(zxfer_quote_token_for_report "$l_backup_file_parent/.zxfer-backup-write.XXXXXX")
		l_backup_file_path_safe=$(zxfer_quote_token_for_report "$l_backup_file_path")
		if [ "$l_has_forwarded_backup_alias" -eq 1 ]; then
			if [ "$g_option_T_target_host" = "" ]; then
				l_forwarded_backup_contents_cmd=$(zxfer_render_command_for_report "" printf '%s' "$l_forwarded_backup_contents")
				l_forwarded_backup_stage_template_safe=$(zxfer_quote_token_for_report "$l_forwarded_backup_file_parent/.zxfer-backup-write.XXXXXX")
				l_primary_backup_rollback_template_safe=$(zxfer_quote_token_for_report "$l_backup_file_parent/.zxfer-backup-rollback.XXXXXX")
				l_forwarded_backup_rollback_template_safe=$(zxfer_quote_token_for_report "$l_forwarded_backup_file_parent/.zxfer-backup-rollback.XXXXXX")
				l_forwarded_backup_file_path_safe=$(zxfer_quote_token_for_report "$l_forwarded_backup_file_path")
				printf '%s\n' "umask 077; l_primary_stage_dir=\$(mktemp -d $l_backup_stage_template_safe) && l_forwarded_stage_dir=\$(mktemp -d $l_forwarded_backup_stage_template_safe) && $l_backup_contents_cmd > \"\$l_primary_stage_dir/backup.write\" && $l_forwarded_backup_contents_cmd > \"\$l_forwarded_stage_dir/backup.write\" && chmod 600 \"\$l_primary_stage_dir/backup.write\" \"\$l_forwarded_stage_dir/backup.write\" && if [ -e $l_forwarded_backup_file_path_safe ]; then l_forwarded_rollback=\$(mktemp $l_forwarded_backup_rollback_template_safe) && mv -f $l_forwarded_backup_file_path_safe \"\$l_forwarded_rollback\"; else l_forwarded_rollback=''; fi && if ! mv -f \"\$l_forwarded_stage_dir/backup.write\" $l_forwarded_backup_file_path_safe; then rm -f $l_forwarded_backup_file_path_safe && if [ \"\$l_forwarded_rollback\" != '' ]; then mv -f \"\$l_forwarded_rollback\" $l_forwarded_backup_file_path_safe; fi; exit 1; fi && if [ -e $l_backup_file_path_safe ]; then l_primary_rollback=\$(mktemp $l_primary_backup_rollback_template_safe) && mv -f $l_backup_file_path_safe \"\$l_primary_rollback\"; else l_primary_rollback=''; fi && if ! mv -f \"\$l_primary_stage_dir/backup.write\" $l_backup_file_path_safe; then rm -f $l_backup_file_path_safe && if [ \"\$l_primary_rollback\" != '' ]; then mv -f \"\$l_primary_rollback\" $l_backup_file_path_safe; fi; rm -f $l_forwarded_backup_file_path_safe && if [ \"\$l_forwarded_rollback\" != '' ]; then mv -f \"\$l_forwarded_rollback\" $l_forwarded_backup_file_path_safe; fi; exit 1; fi && rm -f \"\${l_forwarded_rollback:-}\" \"\${l_primary_rollback:-}\" && rmdir \"\$l_primary_stage_dir\" \"\$l_forwarded_stage_dir\""
			else
				l_pair_split_line=$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE
				l_pair_backup_contents_cmd=$(zxfer_render_command_for_report "" printf '%s\\n%s\\n%s\\n' "$l_rendered_backup_contents" "$l_pair_split_line" "$l_forwarded_backup_contents")
				l_remote_pair_write_cmd=$(zxfer_build_remote_backup_pair_write_cmd "$l_backup_file_parent" "$l_backup_file_path" "$l_forwarded_backup_file_parent" "$l_forwarded_backup_file_path" "$g_option_T_target_host" 99)
				zxfer_render_remote_backup_dry_run_shell_command "$g_option_T_target_host" "$l_remote_pair_write_cmd" ||
					return "$?"
				l_remote_pair_write_shell_cmd=$g_zxfer_remote_backup_dry_run_shell_command_result
				printf '%s\n' "$l_pair_backup_contents_cmd | $l_remote_pair_write_shell_cmd"
			fi
		elif [ "$g_option_T_target_host" = "" ]; then
			printf '%s\n' "umask 077; l_stage_dir=\$(mktemp -d $l_backup_stage_template_safe) && $l_backup_contents_cmd > \"\$l_stage_dir/backup.write\" && chmod 600 \"\$l_stage_dir/backup.write\" && mv -f \"\$l_stage_dir/backup.write\" $l_backup_file_path_safe && rmdir \"\$l_stage_dir\""
		else
			l_remote_write_cmd=$(zxfer_build_remote_backup_write_cmd "$l_backup_file_parent" "$l_backup_file_path" "$g_option_T_target_host" "cat" 99)
			zxfer_render_remote_backup_dry_run_shell_command "$g_option_T_target_host" "$l_remote_write_cmd" ||
				return "$?"
			l_remote_write_shell_cmd=$g_zxfer_remote_backup_dry_run_shell_command_result
			printf '%s\n' "$l_backup_contents_cmd | $l_remote_write_shell_cmd"
		fi
	fi
}
