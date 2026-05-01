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
# BACKUP METADATA / BACKUP STORAGE LAYOUT HELPERS
################################################################################

# Module contract:
# owns globals: backup metadata accumulation, record-list/render result scratch, forwarded provenance scratch, and restored backup contents.
# reads globals: g_backup_storage_root, g_option_O_*/g_option_T_*, g_cmd_awk, remote cat helpers, and current dataset context.
# mutates caches: none.
# returns via stdout: backup-storage paths, metadata file locations, and property payloads.

ZXFER_BACKUP_METADATA_HEADER_LINE="#zxfer property backup file"
ZXFER_BACKUP_METADATA_FORMAT_VERSION="2"
ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE="__ZXFER_BACKUP_METADATA_PAIR_SPLIT__"

# Purpose: Return the backup metadata header line in the form expected by later
# helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_metadata_header_line() {
	printf '%s\n' "$ZXFER_BACKUP_METADATA_HEADER_LINE"
}

# Purpose: Return the backup metadata format version in the form expected by
# later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_metadata_format_version() {
	printf '%s\n' "$ZXFER_BACKUP_METADATA_FORMAT_VERSION"
}

# Purpose: Return the backup metadata pair split line in the form expected by
# later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_metadata_pair_split_line() {
	printf '%s\n' "$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE"
}

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
	g_zxfer_remote_backup_dry_run_shell_command_result=""
	g_zxfer_backup_file_read_result=""
	g_zxfer_backup_stage_dir_result=""
	g_zxfer_backup_stage_file_result=""
	g_zxfer_backup_commit_had_existing_target_result=""
	g_zxfer_backup_commit_rollback_file_result=""
	g_zxfer_backup_restore_candidate_path_result=""
	g_zxfer_backup_local_read_failure_result=""
	g_zxfer_backup_local_write_failure_result=""
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

# Purpose: Update the backup metadata record list to reflect the latest module
# state.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after upstream inputs or staged data change.
#
# Keep backup metadata buffered as one source-root-relative row per dataset so
# repeated property passes replace stale data instead of accumulating ambiguous
# duplicates.
zxfer_update_backup_metadata_record_list() {
	l_existing_records=$1
	l_source=$2
	l_properties=$3
	l_record_key=$(zxfer_get_backup_metadata_record_key_for_source "$l_source")

	# shellcheck disable=SC2016  # awk program should see literal field references.
	if ! l_updated_records=$(printf '%s\n' "$l_existing_records" |
		ZXFER_BACKUP_METADATA_RECORD_KEY=$l_record_key \
			ZXFER_BACKUP_METADATA_PROPERTIES=$l_properties \
			"${g_cmd_awk:-awk}" '
function append_line(line) {
	if (line == "")
		return
	if (output == "")
		output = line
	else
		output = output "\n" line
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
BEGIN {
	record_key = ENVIRON["ZXFER_BACKUP_METADATA_RECORD_KEY"]
	properties = ENVIRON["ZXFER_BACKUP_METADATA_PROPERTIES"]
	if (record_key == "" || !validate_properties(properties))
		exit 3
	replacement = record_key "\t" properties
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
		if (!replaced) {
			append_line(replacement)
			replaced = 1
		}
		next
	}
	append_line($0)
}
END {
	if (malformed)
		exit 3
	if (!replaced)
		append_line(replacement)
	printf "%s", output
}'); then
		zxfer_throw_error "Failed to update buffered backup metadata records."
	fi

	g_zxfer_backup_metadata_record_list_result=$l_updated_records
	printf '%s\n' "$l_updated_records"
}

# Purpose: Validate and return the backup metadata record list.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before rendering rows under a current v2 metadata header.
zxfer_validate_backup_metadata_record_list() {
	l_existing_records=$1

	# shellcheck disable=SC2016  # awk program should see literal field references.
	if ! l_validated_records=$(printf '%s\n' "$l_existing_records" |
		"${g_cmd_awk:-awk}" '
function append_line(line) {
	if (line == "")
		return
	if (output == "")
		output = line
	else
		output = output "\n" line
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
	if ($0 == "")
		next
	tab = index($0, "\t")
	if (tab <= 0)
		exit 3
	current_key = substr($0, 1, tab - 1)
	current_properties = substr($0, tab + 1)
	if (current_key == "" || !validate_properties(current_properties))
		exit 3
	append_line($0)
}
END {
	printf "%s", output
}'); then
		zxfer_throw_error "Failed to validate buffered backup metadata records for chained backup provenance."
	fi

	g_zxfer_backup_metadata_record_list_result=$l_validated_records
	printf '%s\n' "$l_validated_records"
}

# Purpose: Append the backup metadata record to the module-owned accumulator.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need one shared place to extend staged or in-memory
# state.
zxfer_append_backup_metadata_record() {
	l_source=$1
	l_properties=$2

	zxfer_update_backup_metadata_record_list "${g_backup_file_contents:-}" \
		"$l_source" "$l_properties" >/dev/null
	g_backup_file_contents=$g_zxfer_backup_metadata_record_list_result
}

# Purpose: Return the buffered backup metadata record properties in the form
# expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
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
	if (match_count == 1) {
		print match_properties
		exit 0
	}
	if (match_count == 0)
		exit 1
	exit 2
}'); then
		:
	else
		l_status=$?
		case $l_status in
		1 | 2 | 3)
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
		2)
			zxfer_throw_error "Buffered backup metadata rows for source dataset [$l_source] are ambiguous."
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
	zxfer_update_backup_metadata_record_list "${g_pending_backup_file_contents:-}" \
		"$l_source" "$l_buffered_properties" >/dev/null
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
		2)
			zxfer_throw_error "Deferred backup metadata rows for source dataset [$l_source] are ambiguous."
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
	zxfer_update_backup_metadata_record_list "${g_backup_file_contents:-}" \
		"$l_source" "$l_deferred_properties" >/dev/null
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
	l_expected_header=$(zxfer_get_backup_metadata_header_line)
	l_expected_format_version=$(zxfer_get_backup_metadata_format_version)

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
		printf '%s\n' "$(zxfer_get_backup_metadata_header_line)"
		printf '%s\n' "#format_version:$(zxfer_get_backup_metadata_format_version)"
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

# Purpose: Return the backup storage directory for dataset tree in the form
# expected by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_storage_dir_for_dataset_tree() {
	l_dataset=$1
	zxfer_refresh_backup_storage_root

	l_dataset_rel=${l_dataset#/}
	l_dataset_rel=${l_dataset_rel%/}
	if [ "$l_dataset_rel" = "" ]; then
		l_dataset_rel="dataset"
	fi

	printf '%s/%s\n' "$g_backup_storage_root" "$l_dataset_rel"
}

# Purpose: Build the current exact key path used to name backup-metadata files
# for a dataset pair.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows so reads and writes address the same source/destination identity.
zxfer_backup_metadata_file_key() {
	l_source=$1
	l_destination=$2

	l_identity=$(printf '%s\n%s' "$l_source" "$l_destination")
	l_key_hex=$(printf '%s' "$l_identity" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	if [ "$l_key_hex" = "" ]; then
		if [ "$l_identity" != "" ]; then
			return 1
		fi
		l_key_hex="00"
	fi
	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_key_path=$(
		printf '%s\n' "$l_key_hex" |
			"${g_cmd_awk:-awk}" '
				{
					key_path = "h"
					for (i = 1; i <= length($0); i += 48)
					key_path = key_path "/" substr($0, i, 48)
				print key_path
			}'
	) || return "$?"
	[ -n "$l_key_path" ] || return 1
	printf '%s\n' "$l_key_path"
}

# Purpose: Build the retired cksum key string used by older current-format
# backup-metadata filenames.
# Usage: Called only by restore fallback paths so existing v2 backup files keep
# working after current writes move to lossless identity keys.
zxfer_backup_metadata_legacy_file_key() {
	l_source=$1
	l_destination=$2
	l_identity=$(printf '%s\n%s\n' "$l_source" "$l_destination")
	if l_key_cksum=$(printf '%s' "$l_identity" | cksum 2>/dev/null); then
		# shellcheck disable=SC2086
		set -- $l_key_cksum
		if [ $# -ge 2 ] && [ -n "$1" ] && [ -n "$2" ]; then
			printf 'k%s.%s\n' "$1" "$2"
			return 0
		fi
	fi
	l_key_hex=$(printf '%s' "$l_identity" |
		LC_ALL=C od -An -tx1 -v | tr -d ' \n' | cut -c 1-16)
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	printf 'k%s\n' "$l_key_hex"
}

# Purpose: Return the backup metadata filename in the form expected by later
# helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_backup_metadata_filename() {
	l_source=$1
	l_destination=$2
	if ! l_key=$(zxfer_backup_metadata_file_key "$l_source" "$l_destination"); then
		return 1
	fi
	# Keep current exact-pair identities lossless without exceeding NAME_MAX on
	# long pool/dataset names: the identity is chunked into directories and the
	# leaf file has a fixed bounded name.
	printf '%s.v2/%s/%s.v2\n' "$g_backup_file_extension" "$l_key" "$g_backup_file_extension"
}

# Purpose: Return the retired backup metadata filename used before exact
# dataset-pair identities became lossless.
# Usage: Called by restore fallback helpers only; new writes always use
# zxfer_get_backup_metadata_filename.
zxfer_get_legacy_backup_metadata_filename() {
	l_source=$1
	l_destination=$2
	l_tail=${l_source##*/}
	if ! l_key=$(zxfer_backup_metadata_legacy_file_key "$l_source" "$l_destination"); then
		return 1
	fi
	printf '%s.%s.%s\n' "$g_backup_file_extension" "$l_tail" "$l_key"
}

# Purpose: Return the forwarded backup metadata filename in the form expected
# by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_forwarded_backup_metadata_filename() {
	l_dataset_root=$1

	zxfer_get_backup_metadata_filename "$l_dataset_root" "$l_dataset_root"
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
			zxfer_throw_error "Forwarded backup property file $l_dataset_backup_file does not declare supported zxfer backup metadata format version #format_version:$(zxfer_get_backup_metadata_format_version)."
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

# Purpose: Ensure the local backup directory exists and is ready before the
# flow continues.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before later helpers assume the resource or cache is available.
zxfer_ensure_local_backup_dir() {
	l_dir=$1
	if l_symlink_component=$(zxfer_find_symlink_path_component "$l_dir"); then
		if [ "$l_symlink_component" = "$l_dir" ]; then
			zxfer_throw_error "Refusing to use backup directory $l_dir because it is a symlink."
		fi
		zxfer_throw_error "Refusing to use backup directory $l_dir because path component $l_symlink_component is a symlink."
	fi
	if [ -L "$l_dir" ]; then
		zxfer_throw_error "Refusing to use backup directory $l_dir because it is a symlink."
	fi
	if [ -e "$l_dir" ] && [ ! -d "$l_dir" ]; then
		zxfer_throw_error "Refusing to use backup directory $l_dir because it is not a directory."
	fi
	if [ ! -d "$l_dir" ]; then
		l_old_umask=$(umask)
		umask 077
		if ! mkdir -p "$l_dir"; then
			umask "$l_old_umask"
			zxfer_throw_error "Error creating secure backup directory $l_dir."
		fi
		umask "$l_old_umask"
	fi
	if ! l_owner_uid=$(zxfer_get_path_owner_uid "$l_dir"); then
		zxfer_throw_error "Cannot determine the owner of backup directory $l_dir."
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_owner_uid"; then
		l_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		zxfer_throw_error "Refusing to use backup directory $l_dir because it is owned by UID $l_owner_uid instead of $l_expected_owner_desc."
	fi
	if ! chmod 700 "$l_dir"; then
		zxfer_throw_error "Error securing backup directory $l_dir."
	fi
}

# Purpose: Build the remote backup directory symlink guard command for the next
# execution or comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_dir_symlink_guard_cmd() {
	l_dir_single=$1
	l_reject_status=${2:-1}

	printf '%s' "l_scan_path='$l_dir_single'; l_scan_remaining=\$l_scan_path; l_scan_candidate=''; while [ -n \"\$l_scan_remaining\" ]; do case \"\$l_scan_remaining\" in /*) if [ \"\$l_scan_candidate\" = '' ]; then l_scan_candidate=/; l_scan_remaining=\${l_scan_remaining#/}; continue; fi ;; esac; l_scan_component=\${l_scan_remaining%%/*}; if [ \"\$l_scan_component\" = \"\$l_scan_remaining\" ]; then l_scan_remaining=''; else l_scan_remaining=\${l_scan_remaining#*/}; fi; [ -n \"\$l_scan_component\" ] || continue; case \"\$l_scan_candidate\" in '') l_scan_candidate=\$l_scan_component ;; /) l_scan_candidate=/\$l_scan_component ;; *) l_scan_candidate=\$l_scan_candidate/\$l_scan_component ;; esac; if [ -L \"\$l_scan_candidate\" ] || [ -h \"\$l_scan_candidate\" ]; then l_scan_trusted=0; case \"\$l_scan_candidate\" in /*) l_scan_parent=\${l_scan_candidate%/*}; [ -n \"\$l_scan_parent\" ] || l_scan_parent=/; l_scan_owner=''; l_scan_parent_owner=''; if command -v stat >/dev/null 2>&1; then l_scan_owner=\$(stat -c '%u' \"\$l_scan_candidate\" 2>/dev/null); if [ \"\$l_scan_owner\" = '' ] || printf '%s' \"\$l_scan_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_owner=\$(stat -f '%u' \"\$l_scan_candidate\" 2>/dev/null); fi; l_scan_parent_owner=\$(stat -c '%u' \"\$l_scan_parent\" 2>/dev/null); if [ \"\$l_scan_parent_owner\" = '' ] || printf '%s' \"\$l_scan_parent_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_parent_owner=\$(stat -f '%u' \"\$l_scan_parent\" 2>/dev/null); fi; fi; if [ \"\$l_scan_owner\" = '0' ] && [ \"\$l_scan_parent_owner\" = '0' ] && [ \"\$l_scan_parent\" = '/' ]; then l_scan_ls_path=\$l_scan_parent; case \"\$l_scan_ls_path\" in -*) l_scan_ls_path=./\$l_scan_ls_path ;; esac; l_scan_ls_line=\$(ls -ldn \"\$l_scan_ls_path\" 2>/dev/null) || l_scan_ls_line=''; if [ \"\$l_scan_ls_line\" != '' ]; then l_scan_parent_perm=\$(printf '%s\n' \"\$l_scan_ls_line\" | awk '{print \$1}'); case \"\$l_scan_parent_perm\" in ??????????*) l_scan_group_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 6); l_scan_other_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 9); l_scan_sticky=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 10); case \"\$l_scan_group_write\$l_scan_other_write\" in *w*) case \"\$l_scan_sticky\" in t|T) l_scan_trusted=1 ;; esac ;; *) l_scan_trusted=1 ;; esac ;; esac; fi; fi ;; esac; if [ \"\$l_scan_trusted\" = '1' ]; then continue; fi; if [ \"\$l_scan_candidate\" = \"\$l_scan_path\" ]; then echo 'Refusing to use symlinked zxfer backup directory.' >&2; else echo \"Refusing to use backup directory \$l_scan_path because path component \$l_scan_candidate is a symlink.\" >&2; fi; exit $l_reject_status; fi; done"
}

# Purpose: Build the remote backup metadata symlink guard command for the next
# execution or comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_metadata_symlink_guard_cmd() {
	l_path_single=$1
	l_reject_status=${2:-1}

	printf '%s' "l_scan_path='$l_path_single'; l_scan_remaining=\$l_scan_path; l_scan_candidate=''; while [ -n \"\$l_scan_remaining\" ]; do case \"\$l_scan_remaining\" in /*) if [ \"\$l_scan_candidate\" = '' ]; then l_scan_candidate=/; l_scan_remaining=\${l_scan_remaining#/}; continue; fi ;; esac; l_scan_component=\${l_scan_remaining%%/*}; if [ \"\$l_scan_component\" = \"\$l_scan_remaining\" ]; then l_scan_remaining=''; else l_scan_remaining=\${l_scan_remaining#*/}; fi; [ -n \"\$l_scan_component\" ] || continue; case \"\$l_scan_candidate\" in '') l_scan_candidate=\$l_scan_component ;; /) l_scan_candidate=/\$l_scan_component ;; *) l_scan_candidate=\$l_scan_candidate/\$l_scan_component ;; esac; if [ -L \"\$l_scan_candidate\" ] || [ -h \"\$l_scan_candidate\" ]; then l_scan_trusted=0; case \"\$l_scan_candidate\" in /*) l_scan_parent=\${l_scan_candidate%/*}; [ -n \"\$l_scan_parent\" ] || l_scan_parent=/; l_scan_owner=''; l_scan_parent_owner=''; if command -v stat >/dev/null 2>&1; then l_scan_owner=\$(stat -c '%u' \"\$l_scan_candidate\" 2>/dev/null); if [ \"\$l_scan_owner\" = '' ] || printf '%s' \"\$l_scan_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_owner=\$(stat -f '%u' \"\$l_scan_candidate\" 2>/dev/null); fi; l_scan_parent_owner=\$(stat -c '%u' \"\$l_scan_parent\" 2>/dev/null); if [ \"\$l_scan_parent_owner\" = '' ] || printf '%s' \"\$l_scan_parent_owner\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_scan_parent_owner=\$(stat -f '%u' \"\$l_scan_parent\" 2>/dev/null); fi; fi; if [ \"\$l_scan_owner\" = '0' ] && [ \"\$l_scan_parent_owner\" = '0' ] && [ \"\$l_scan_parent\" = '/' ]; then l_scan_ls_path=\$l_scan_parent; case \"\$l_scan_ls_path\" in -*) l_scan_ls_path=./\$l_scan_ls_path ;; esac; l_scan_ls_line=\$(ls -ldn \"\$l_scan_ls_path\" 2>/dev/null) || l_scan_ls_line=''; if [ \"\$l_scan_ls_line\" != '' ]; then l_scan_parent_perm=\$(printf '%s\n' \"\$l_scan_ls_line\" | awk '{print \$1}'); case \"\$l_scan_parent_perm\" in ??????????*) l_scan_group_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 6); l_scan_other_write=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 9); l_scan_sticky=\$(printf '%s' \"\$l_scan_parent_perm\" | cut -c 10); case \"\$l_scan_group_write\$l_scan_other_write\" in *w*) case \"\$l_scan_sticky\" in t|T) l_scan_trusted=1 ;; esac ;; *) l_scan_trusted=1 ;; esac ;; esac; fi; fi ;; esac; if [ \"\$l_scan_trusted\" = '1' ]; then continue; fi; if [ \"\$l_scan_candidate\" = \"\$l_scan_path\" ]; then echo \"Refusing to use backup metadata \$l_scan_path because it is a symlink.\" >&2; else echo \"Refusing to use backup metadata \$l_scan_path because path component \$l_scan_candidate is a symlink.\" >&2; fi; exit $l_reject_status; fi; done"
}

# Purpose: Return the remote backup helper dependency path in the form expected
# by later helpers.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_remote_backup_helper_dependency_path() {
	zxfer_get_effective_dependency_path
}

# Purpose: Wrap a remote backup helper command so it runs under the validated
# remote secure-PATH contract.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before remote helper payloads are sent over SSH.
zxfer_wrap_remote_backup_helper_with_secure_path() {
	l_remote_cmd=$1
	l_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")

	printf "PATH='%s'; export PATH; %s" "$l_dependency_path_single" "$l_remote_cmd"
}

# Purpose: Build the remote backup helper dependency check command for the next
# execution or comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_helper_dependency_check_cmd() {
	l_host=$1
	l_status=$2
	shift 2

	l_host_single=$(zxfer_escape_for_single_quotes "$l_host")
	l_required_tools=""
	for l_required_tool in "$@"; do
		l_required_tool_single=$(zxfer_escape_for_single_quotes "$l_required_tool")
		l_required_tools="$l_required_tools '$l_required_tool_single'"
	done

	printf '%s' "zxfer_require_remote_backup_tool() { l_required_tool=\$1; if command -v \"\$l_required_tool\" >/dev/null 2>&1; then return 0; fi; printf '%s\n' \"Required dependency \\\"\$l_required_tool\\\" not found on host $l_host_single in secure PATH (\$PATH). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary.\" >&2; exit $l_status; }; for l_required_tool in$l_required_tools; do zxfer_require_remote_backup_tool \"\$l_required_tool\"; done"
}

# Purpose: Build the remote backup directory prepare command for the next
# execution or comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_dir_prepare_cmd() {
	l_dir=$1
	l_host=$2
	l_remote_dependency_status=${3:-99}
	l_remote_prepare_failure_status=${4:-92}

	l_dir_single=$(zxfer_escape_for_single_quotes "$l_dir")
	l_dir_ls_path=$l_dir
	case "$l_dir_ls_path" in
	-*)
		l_dir_ls_path=./$l_dir_ls_path
		;;
	esac
	l_dir_ls_single=$(zxfer_escape_for_single_quotes "$l_dir_ls_path")
	l_remote_symlink_guard_cmd=$(zxfer_build_remote_backup_dir_symlink_guard_cmd "$l_dir_single" "$l_remote_prepare_failure_status")
	l_remote_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd "$l_host" "$l_remote_dependency_status" mkdir chmod id grep ls awk cut)
	l_remote_cmd="$l_remote_dependency_check_cmd; $l_remote_symlink_guard_cmd; [ -L '$l_dir_single' ] && { echo 'Refusing to use symlinked zxfer backup directory.' >&2; exit $l_remote_prepare_failure_status; }; if [ -e '$l_dir_single' ] && [ ! -d '$l_dir_single' ]; then echo 'Backup path exists but is not a directory.' >&2; exit $l_remote_prepare_failure_status; fi; umask 077; if ! mkdir -p '$l_dir_single'; then echo 'Error creating secure backup directory.' >&2; exit $l_remote_prepare_failure_status; fi; if ! chmod 700 '$l_dir_single'; then echo 'Error securing backup directory.' >&2; exit $l_remote_prepare_failure_status; fi; l_expected_uid=\$(id -u); l_dir_uid=''; if command -v stat >/dev/null 2>&1; then l_dir_uid=\$(stat -c '%u' '$l_dir_single' 2>/dev/null); if [ \"\$l_dir_uid\" = '' ] || printf '%s' \"\$l_dir_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_dir_uid=\$(stat -f '%u' '$l_dir_single' 2>/dev/null); fi; fi; if [ \"\$l_dir_uid\" = '' ] || printf '%s' \"\$l_dir_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_ls_line=\$(ls -ldn '$l_dir_ls_single' 2>/dev/null) || l_ls_line=''; if [ \"\$l_ls_line\" != '' ]; then l_dir_uid=\$(printf '%s\n' \"\$l_ls_line\" | awk '{print \$3}'); fi; fi; if [ \"\$l_dir_uid\" = '' ]; then echo 'Unable to determine backup directory owner.' >&2; exit $l_remote_prepare_failure_status; fi; if [ \"\$l_dir_uid\" != 0 ] && [ \"\$l_dir_uid\" != \"\$l_expected_uid\" ]; then echo 'Backup directory must be owned by root or the ssh user.' >&2; exit $l_remote_prepare_failure_status; fi"

	zxfer_wrap_remote_backup_helper_with_secure_path "$l_remote_cmd"
}

# Purpose: Ensure the remote backup directory exists and is ready before the
# flow continues.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before later helpers assume the resource or cache is available.
zxfer_ensure_remote_backup_dir() {
	l_dir=$1
	l_host=$2
	l_profile_side=${3:-}

	[ "$l_host" = "" ] && return

	l_remote_dependency_status=99
	l_remote_prepare_failure_status=92
	l_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
	l_remote_cmd=$(zxfer_build_remote_backup_dir_prepare_cmd "$l_dir" "$l_host" "$l_remote_dependency_status" "$l_remote_prepare_failure_status")
	l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_cmd")
	if zxfer_capture_remote_probe_output "$l_host" "$l_remote_shell_cmd" "$l_profile_side"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi
	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		zxfer_throw_remote_backup_capture_error "$l_host" "preparing backup directory $l_dir"
	fi
	if [ "$l_remote_status" -eq "$l_remote_dependency_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Required remote backup-directory helper dependency not found on host $l_host in secure PATH ($l_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ "$l_remote_status" -eq "$l_remote_prepare_failure_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_throw_error "Error preparing backup directory on $l_host."
	fi
	if [ "$l_remote_status" -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
			zxfer_throw_error "Failed to contact target host $l_host while preparing backup directory $l_dir. Review prior stderr for the transport or authentication error."
		fi
		zxfer_throw_error "Error preparing backup directory on $l_host."
	fi
}

# Purpose: Clean up the backup metadata stage directory that this module
# created or tracks.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows on success and failure paths so temporary state does not linger.
zxfer_cleanup_backup_metadata_stage_dir() {
	l_stage_dir=$1

	[ -n "$l_stage_dir" ] || return 0
	if command -v zxfer_cleanup_runtime_artifact_path >/dev/null 2>&1; then
		zxfer_cleanup_runtime_artifact_path "$l_stage_dir" >/dev/null 2>&1 || true
		return 0
	fi
	rm -f "$l_stage_dir/backup.snapshot" "$l_stage_dir/backup.write" 2>/dev/null || true
	rmdir "$l_stage_dir" 2>/dev/null || true
}

# Purpose: Register the backup metadata runtime artifact path with the tracking
# state owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows so cleanup and later lookups can find the live resource.
zxfer_register_backup_metadata_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0
	if command -v zxfer_register_runtime_artifact_path >/dev/null 2>&1; then
		zxfer_register_runtime_artifact_path "$l_artifact_path"
	fi
}

# Purpose: Remove the backup metadata runtime artifact path from the tracking
# state owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after the tracked resource has completed or been cleaned up.
zxfer_unregister_backup_metadata_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0
	if command -v zxfer_unregister_runtime_artifact_path >/dev/null 2>&1; then
		zxfer_unregister_runtime_artifact_path "$l_artifact_path"
	fi
}

# Purpose: Remove the local backup metadata path if present from the current
# working set while preserving the module's special-case rules.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when filtering logic must trim staged data before later reconciliation
# or apply steps run.
zxfer_remove_local_backup_metadata_path_if_present() {
	l_path=$1

	[ -n "$l_path" ] || return 0
	if [ ! -e "$l_path" ] && [ ! -L "$l_path" ] && [ ! -h "$l_path" ]; then
		return 0
	fi
	if rm -f "$l_path" 2>/dev/null; then
		return 0
	else
		l_status=$?
		return "$l_status"
	fi
}

# Purpose: Move the local backup metadata path through the controlled local
# publish path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when staged local data is ready for its final path.
zxfer_move_local_backup_metadata_path() {
	l_source_path=$1
	l_target_path=$2

	if mv -f "$l_source_path" "$l_target_path" 2>/dev/null; then
		return 0
	else
		l_status=$?
		return "$l_status"
	fi
}

# Purpose: Create the backup metadata stage directory for path using the safety
# checks owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs a fresh staged resource or persistent helper state.
zxfer_create_backup_metadata_stage_dir_for_path() {
	l_backup_stage_path=$1
	l_backup_stage_prefix=${2:-zxfer-backup-stage}

	g_zxfer_backup_stage_dir_result=""
	if l_backup_stage_parent=$(zxfer_get_path_parent_dir "$l_backup_stage_path"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	if [ ! -d "$l_backup_stage_parent" ]; then
		return 1
	fi

	l_backup_stage_old_umask=$(umask)
	umask 077
	if l_backup_stage_dir=$(mktemp -d "$l_backup_stage_parent/.$l_backup_stage_prefix.XXXXXX" 2>/dev/null); then
		l_backup_stage_status=0
	else
		l_backup_stage_status=$?
	fi
	umask "$l_backup_stage_old_umask"
	[ "$l_backup_stage_status" -eq 0 ] || return "$l_backup_stage_status"
	zxfer_register_backup_metadata_runtime_artifact_path "$l_backup_stage_dir"

	g_zxfer_backup_stage_dir_result=$l_backup_stage_dir
	printf '%s\n' "$l_backup_stage_dir"
}

# Purpose: Check whether the backup metadata path uses trusted nonwritable
# parent.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a boolean answer about the backup metadata
# path.
zxfer_backup_metadata_path_uses_trusted_nonwritable_parent() {
	l_backup_io_path=$1

	if ! l_backup_io_parent=$(zxfer_get_path_parent_dir "$l_backup_io_path"); then
		return 1
	fi
	if ! l_backup_io_parent=$(zxfer_validate_temp_root_candidate "$l_backup_io_parent"); then
		return 1
	fi

	[ ! -w "$l_backup_io_parent" ]
}

# Purpose: Require the backup write target path before the surrounding flow
# continues.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers should stop immediately if the precondition is not
# met.
zxfer_require_backup_write_target_path() {
	l_path=$1

	if [ -L "$l_path" ] || [ -h "$l_path" ]; then
		zxfer_throw_error "Refusing to write backup metadata $l_path because it is a symlink."
	fi
	if [ -e "$l_path" ] && [ ! -f "$l_path" ]; then
		zxfer_throw_error "Refusing to write backup metadata $l_path because it is not a regular file."
	fi
}

# Purpose: Prepare the local backup file stage before the surrounding flow uses
# it.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows once prerequisites are known but before live work depends on the
# prepared state.
zxfer_prepare_local_backup_file_stage() {
	l_backup_file_path=$1
	l_rendered_backup_contents=$2
	g_zxfer_backup_local_write_failure_result=""
	g_zxfer_backup_stage_dir_result=""
	g_zxfer_backup_stage_file_result=""

	if zxfer_create_backup_metadata_stage_dir_for_path "$l_backup_file_path" "zxfer-backup-write" >/dev/null; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_write_failure_result=staging
		return "$l_status"
	fi
	l_stage_dir=$g_zxfer_backup_stage_dir_result
	l_stage_file="$l_stage_dir/backup.write"
	if (
		umask 077
		printf '%s\n' "$l_rendered_backup_contents" >"$l_stage_file"
	); then
		:
	else
		l_status=$?
		g_zxfer_backup_local_write_failure_result=staging
		g_zxfer_backup_stage_dir_result=""
		g_zxfer_backup_stage_file_result=""
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		return "$l_status"
	fi
	if chmod 600 "$l_stage_file"; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_write_failure_result=staging
		g_zxfer_backup_stage_dir_result=""
		g_zxfer_backup_stage_file_result=""
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		return "$l_status"
	fi

	g_zxfer_backup_stage_dir_result=$l_stage_dir
	g_zxfer_backup_stage_file_result=$l_stage_file
	printf '%s\n' "$l_stage_dir"
	printf '%s\n' "$l_stage_file"
}

# Purpose: Commit the local backup file stage once staged validation has
# already succeeded.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after the staged backup or cache payload is ready to become live.
zxfer_commit_local_backup_file_stage() {
	l_backup_file_path=$1
	l_stage_file=$2
	g_zxfer_backup_commit_had_existing_target_result=""
	g_zxfer_backup_commit_rollback_file_result=""

	if [ -L "$l_backup_file_path" ] || [ -h "$l_backup_file_path" ]; then
		return 1
	fi
	if [ -e "$l_backup_file_path" ] && [ ! -f "$l_backup_file_path" ]; then
		return 1
	fi

	l_had_existing_target=0
	l_rollback_file=""
	if [ -e "$l_backup_file_path" ]; then
		l_had_existing_target=1
		if l_backup_parent=$(zxfer_get_path_parent_dir "$l_backup_file_path"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if l_rollback_file=$(mktemp "$l_backup_parent/.zxfer-backup-rollback.XXXXXX" 2>/dev/null); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if zxfer_move_local_backup_metadata_path "$l_backup_file_path" "$l_rollback_file"; then
			:
		else
			l_status=$?
			zxfer_remove_local_backup_metadata_path_if_present "$l_rollback_file" >/dev/null 2>&1 || :
			return "$l_status"
		fi
	fi

	if zxfer_move_local_backup_metadata_path "$l_stage_file" "$l_backup_file_path"; then
		:
	else
		l_stage_move_status=$?
		if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
			if zxfer_move_local_backup_metadata_path "$l_rollback_file" "$l_backup_file_path"; then
				:
			else
				l_restore_status=$?
				g_zxfer_backup_local_write_failure_result=rollback
				if [ -e "$l_rollback_file" ]; then
					zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path" >/dev/null 2>&1 || :
				fi
				return "$l_restore_status"
			fi
			if [ -e "$l_rollback_file" ]; then
				zxfer_remove_local_backup_metadata_path_if_present "$l_rollback_file" >/dev/null 2>&1 || :
			fi
		else
			zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path" >/dev/null 2>&1 || :
		fi
		return "$l_stage_move_status"
	fi

	g_zxfer_backup_commit_had_existing_target_result=$l_had_existing_target
	g_zxfer_backup_commit_rollback_file_result=$l_rollback_file
	printf '%s\n' "$l_had_existing_target"
	printf '%s\n' "$l_rollback_file"
}

# Purpose: Rollback the local backup file commit to the last safe state this
# module recognizes.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer detects divergence and must re-establish a safe base.
zxfer_rollback_local_backup_file_commit() {
	l_backup_file_path=$1
	l_had_existing_target=$2
	l_rollback_file=$3

	if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
		if zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path"; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if zxfer_move_local_backup_metadata_path "$l_rollback_file" "$l_backup_file_path"; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		zxfer_unregister_backup_metadata_runtime_artifact_path "$l_rollback_file"
		return 0
	fi

	zxfer_remove_local_backup_metadata_path_if_present "$l_backup_file_path"
}

# Purpose: Finalize the local backup file commit once all prerequisites have
# succeeded.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows after staged or deferred work is ready to become the module's final
# result.
zxfer_finalize_local_backup_file_commit() {
	l_had_existing_target=$1
	l_rollback_file=$2

	if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
		if zxfer_remove_local_backup_metadata_path_if_present "$l_rollback_file"; then
			zxfer_unregister_backup_metadata_runtime_artifact_path "$l_rollback_file"
			return 0
		else
			l_status=$?
			return "$l_status"
		fi
	fi

	return 0
}

# Purpose: Raise the backup write rollback error through zxfer's
# structured failure reporting path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_backup_write_rollback_error() {
	zxfer_throw_error "Error writing backup file and restoring backup metadata rollback state. Inspect rollback files under ZXFER_BACKUP_DIR for manual recovery."
}

# Purpose: Raise the remote backup transport error through zxfer's structured
# failure reporting path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_remote_backup_transport_error() {
	l_host=$1
	l_action=$2

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		zxfer_emit_remote_probe_failure_message >&2
	fi
	zxfer_throw_error "Failed to contact target host $l_host while $l_action. Review prior stderr for the transport or authentication error."
}

# Purpose: Raise the remote backup capture error through zxfer's structured
# failure reporting path.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the current error should stop the run with the module's normal
# reporting contract.
zxfer_throw_remote_backup_capture_error() {
	l_host=$1
	l_action=$2

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		zxfer_emit_remote_probe_failure_message >&2
	fi
	zxfer_throw_error "Failed to reload local remote helper capture while $l_action on host $l_host."
}

# Purpose: Run the remote backup helper with payload through the controlled
# execution path owned by this module.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows once planning is complete and zxfer is ready to execute the action.
zxfer_run_remote_backup_helper_with_payload() {
	l_host=$1
	l_remote_shell_cmd=$2
	l_payload=$3
	l_profile_side=${4:-}

	zxfer_reset_remote_probe_capture_state

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host"); then
		:
	else
		zxfer_profile_record_ssh_invocation "$l_host" "$l_profile_side"
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi

	zxfer_create_private_temp_dir "zxfer-remote-backup-helper" >/dev/null
	l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_throw_error "Error creating temporary file."
	fi
	l_stage_dir=$g_zxfer_runtime_artifact_path_result
	l_stdin_path="$l_stage_dir/stdin"
	l_stdout_path="$l_stage_dir/stdout"
	l_stderr_path="$l_stage_dir/stderr"

	if ! zxfer_write_runtime_artifact_file "$l_stdin_path" "$l_payload"; then
		zxfer_cleanup_runtime_artifact_path "$l_stage_dir"
		zxfer_throw_error "Error creating temporary file."
	fi

	if zxfer_invoke_ssh_shell_command_for_host \
		"$l_host" "$l_remote_shell_cmd" "$l_profile_side" <"$l_stdin_path" >"$l_stdout_path" 2>"$l_stderr_path"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi

	zxfer_load_remote_probe_capture_files "remote backup helper" "$l_stdout_path" "$l_stderr_path"
	l_capture_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_stage_dir"
	if [ "$l_capture_status" -ne 0 ]; then
		return "$l_capture_status"
	fi
	return "$l_remote_status"
}

# Purpose: Write the local backup file pair atomically in the normalized form
# later zxfer steps expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
zxfer_write_local_backup_file_pair_atomically() {
	l_primary_backup_file_path=$1
	l_primary_rendered_backup_contents=$2
	l_forwarded_backup_file_path=$3
	l_forwarded_backup_contents=$4

	g_zxfer_backup_local_write_failure_result=""
	if zxfer_prepare_local_backup_file_stage "$l_primary_backup_file_path" "$l_primary_rendered_backup_contents" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_primary_stage_dir=$g_zxfer_backup_stage_dir_result
	l_primary_stage_file=$g_zxfer_backup_stage_file_result

	if zxfer_prepare_local_backup_file_stage "$l_forwarded_backup_file_path" "$l_forwarded_backup_contents" >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		return "$l_status"
	fi
	l_forwarded_stage_dir=$g_zxfer_backup_stage_dir_result
	l_forwarded_stage_file=$g_zxfer_backup_stage_file_result

	if zxfer_commit_local_backup_file_stage "$l_forwarded_backup_file_path" "$l_forwarded_stage_file" >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_status"
	fi
	l_forwarded_had_existing_target=$g_zxfer_backup_commit_had_existing_target_result
	l_forwarded_rollback_file=$g_zxfer_backup_commit_rollback_file_result

	if zxfer_commit_local_backup_file_stage "$l_primary_backup_file_path" "$l_primary_stage_file" >/dev/null; then
		:
	else
		l_status=$?
		if ! zxfer_rollback_local_backup_file_commit "$l_forwarded_backup_file_path" "$l_forwarded_had_existing_target" "$l_forwarded_rollback_file" >/dev/null 2>&1; then
			zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
			zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
			return 2
		fi
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_status"
	fi
	l_primary_had_existing_target=$g_zxfer_backup_commit_had_existing_target_result
	l_primary_rollback_file=$g_zxfer_backup_commit_rollback_file_result
	if [ "$l_forwarded_had_existing_target" -eq 1 ] &&
		[ -n "$l_forwarded_rollback_file" ]; then
		zxfer_register_backup_metadata_runtime_artifact_path \
			"$l_forwarded_rollback_file"
	fi
	if [ "$l_primary_had_existing_target" -eq 1 ] &&
		[ -n "$l_primary_rollback_file" ]; then
		zxfer_register_backup_metadata_runtime_artifact_path \
			"$l_primary_rollback_file"
	fi

	if zxfer_finalize_local_backup_file_commit "$l_forwarded_had_existing_target" "$l_forwarded_rollback_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_status"
	fi
	if zxfer_finalize_local_backup_file_commit "$l_primary_had_existing_target" "$l_primary_rollback_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
		zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
		return "$l_status"
	fi
	zxfer_cleanup_backup_metadata_stage_dir "$l_primary_stage_dir"
	zxfer_cleanup_backup_metadata_stage_dir "$l_forwarded_stage_dir"
}

# Purpose: Write the local backup file atomically in the normalized form later
# zxfer steps expect.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when the module needs a stable staged file or emitted stream for
# downstream use.
zxfer_write_local_backup_file_atomically() {
	l_backup_file_path=$1
	l_rendered_backup_contents=$2

	g_zxfer_backup_local_write_failure_result=""
	if zxfer_prepare_local_backup_file_stage "$l_backup_file_path" "$l_rendered_backup_contents" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_stage_dir=$g_zxfer_backup_stage_dir_result
	l_stage_file=$g_zxfer_backup_stage_file_result
	if zxfer_commit_local_backup_file_stage "$l_backup_file_path" "$l_stage_file" >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		return "$l_status"
	fi
	l_had_existing_target=$g_zxfer_backup_commit_had_existing_target_result
	l_rollback_file=$g_zxfer_backup_commit_rollback_file_result
	if [ "$l_had_existing_target" -eq 1 ] && [ -n "$l_rollback_file" ]; then
		# Rollback files only become disposable after the staged backup is live.
		zxfer_register_backup_metadata_runtime_artifact_path "$l_rollback_file"
	fi
	if zxfer_finalize_local_backup_file_commit "$l_had_existing_target" "$l_rollback_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		return "$l_status"
	fi
	zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
}

# Purpose: Render the reusable remote target guard for backup metadata writes.
# Usage: Called by remote single-file and pair write command builders before
# they stage or publish backup metadata on the target host.
zxfer_render_remote_backup_target_write_guard_cmd() {
	l_backup_file_path_single=$1
	l_remote_write_failure_status=$2

	printf '%s' "if [ -L '$l_backup_file_path_single' ] || [ -h '$l_backup_file_path_single' ]; then echo 'Refusing to write backup metadata because the target is a symlink.' >&2; exit $l_remote_write_failure_status; fi; if [ -e '$l_backup_file_path_single' ] && [ ! -f '$l_backup_file_path_single' ]; then echo 'Refusing to write backup metadata because the target is not a regular file.' >&2; exit $l_remote_write_failure_status; fi"
}

# Purpose: Render the remote staged target guard for backup metadata writes.
# Usage: Called by the single-file write builder after staging so the target is
# checked again immediately before the final rename.
zxfer_render_remote_backup_staged_target_write_guard_cmd() {
	l_backup_file_path_single=$1
	l_remote_write_failure_status=$2
	l_stage_cleanup_cmd=$3

	printf '%s' "if [ -L '$l_backup_file_path_single' ] || [ -h '$l_backup_file_path_single' ]; then $l_stage_cleanup_cmd; exit $l_remote_write_failure_status; fi; if [ -e '$l_backup_file_path_single' ] && [ ! -f '$l_backup_file_path_single' ]; then $l_stage_cleanup_cmd; exit $l_remote_write_failure_status; fi"
}

# Purpose: Render the remote single-file backup stage cleanup fragment.
# Usage: Called by the single-file write builder anywhere a staged write must
# unwind before returning an error to the local side.
zxfer_render_remote_backup_single_stage_cleanup_cmd() {
	# shellcheck disable=SC2016  # Remote shell variables should remain literal.
	printf '%s' 'rm -f "$l_stage_file"; rmdir "$l_stage_dir" 2>/dev/null || true'
}

# Purpose: Render remote single-file backup stage allocation.
# Usage: Called by the single-file write builder after the target preflight
# guard and before the payload helper writes into the stage file.
zxfer_render_remote_backup_single_stage_setup_cmd() {
	l_backup_file_dir_single=$1
	l_remote_write_failure_status=$2

	printf '%s' "l_stage_dir=\$(mktemp -d '$l_backup_file_dir_single/.zxfer-backup-write.XXXXXX' 2>/dev/null) || exit $l_remote_write_failure_status; l_stage_file=\"\$l_stage_dir/backup.write\""
}

# Purpose: Render the remote pair-write stage cleanup function.
# Usage: Called by the transactional pair write builder before any branch can
# need cleanup for primary or forwarded staged files.
zxfer_render_remote_backup_pair_cleanup_function_cmd() {
	# shellcheck disable=SC2016  # Remote shell variables should remain literal.
	printf '%s' 'cleanup_stages() { rm -f "$l_primary_stage_file" "$l_forwarded_stage_file" 2>/dev/null || true; rmdir "$l_primary_stage_dir" "$l_forwarded_stage_dir" 2>/dev/null || true; }'
}

# Purpose: Render the remote rollback helper for the forwarded backup alias.
# Usage: Called by the transactional pair write builder so all later primary
# failure paths share the same forwarded-alias rollback behavior.
zxfer_render_remote_backup_pair_forwarded_rollback_function_cmd() {
	l_forwarded_backup_file_path_single=$1

	printf '%s' "rollback_forwarded() { rm -f '$l_forwarded_backup_file_path_single' 2>/dev/null || true; if [ \"\${l_forwarded_had_existing:-0}\" -eq 1 ] && [ \"\${l_forwarded_rollback_file:-}\" != '' ]; then if ! mv -f \"\$l_forwarded_rollback_file\" '$l_forwarded_backup_file_path_single' 2>/dev/null; then return 1; fi; if [ -e \"\$l_forwarded_rollback_file\" ]; then rm -f \"\$l_forwarded_rollback_file\" 2>/dev/null || true; fi; fi; return 0; }"
}

# Purpose: Render target guards for both files in a remote pair write.
# Usage: Called by the transactional pair write builder before allocating any
# remote stage directories.
zxfer_render_remote_backup_pair_target_guard_cmd() {
	l_primary_backup_file_path_single=$1
	l_forwarded_backup_file_path_single=$2
	l_remote_write_failure_status=$3

	l_primary_guard_cmd=$(zxfer_render_remote_backup_target_write_guard_cmd "$l_primary_backup_file_path_single" "$l_remote_write_failure_status")
	l_forwarded_guard_cmd=$(zxfer_render_remote_backup_target_write_guard_cmd "$l_forwarded_backup_file_path_single" "$l_remote_write_failure_status")
	printf '%s; %s' "$l_primary_guard_cmd" "$l_forwarded_guard_cmd"
}

# Purpose: Render remote pair-write stage allocation.
# Usage: Called by the transactional pair write builder after both target paths
# have passed preflight checks.
zxfer_render_remote_backup_pair_stage_setup_cmd() {
	l_primary_backup_file_dir_single=$1
	l_forwarded_backup_file_dir_single=$2
	l_remote_write_failure_status=$3

	printf '%s' "l_primary_stage_dir=\$(mktemp -d '$l_primary_backup_file_dir_single/.zxfer-backup-write.XXXXXX' 2>/dev/null) || exit $l_remote_write_failure_status; l_primary_stage_file=\"\$l_primary_stage_dir/backup.write\"; l_forwarded_stage_dir=\$(mktemp -d '$l_forwarded_backup_file_dir_single/.zxfer-backup-write.XXXXXX' 2>/dev/null) || { cleanup_stages; exit $l_remote_write_failure_status; }; l_forwarded_stage_file=\"\$l_forwarded_stage_dir/backup.write\""
}

# Purpose: Render the remote pair payload splitter.
# Usage: Called by the transactional pair write builder to split one stdin
# payload into the primary and forwarded backup stage files.
zxfer_render_remote_backup_pair_payload_split_cmd() {
	l_pair_split_line_single=$1
	l_remote_write_failure_status=$2

	printf '%s' "if ! awk -v split_line='$l_pair_split_line_single' -v primary_file=\"\$l_primary_stage_file\" -v forwarded_file=\"\$l_forwarded_stage_file\" 'BEGIN { current = primary_file; saw_split = 0 } \$0 == split_line { current = forwarded_file; saw_split = 1; next } { print > current } END { if (!saw_split) exit 1 }'; then cleanup_stages; exit $l_remote_write_failure_status; fi"
}

# Purpose: Render the remote pair stage permission checks.
# Usage: Called by the transactional pair write builder after both staged
# payload files have been written.
zxfer_render_remote_backup_pair_stage_chmod_cmd() {
	l_remote_write_failure_status=$1

	printf '%s' "if ! chmod 600 \"\$l_primary_stage_file\"; then cleanup_stages; exit $l_remote_write_failure_status; fi; if ! chmod 600 \"\$l_forwarded_stage_file\"; then cleanup_stages; exit $l_remote_write_failure_status; fi"
}

# Purpose: Render the forwarded side of a remote pair publish.
# Usage: Called by the transactional pair write builder before the primary side
# so a primary failure can roll the forwarded alias back first.
zxfer_render_remote_backup_pair_forwarded_publish_cmd() {
	l_forwarded_backup_file_dir_single=$1
	l_forwarded_backup_file_path_single=$2
	l_remote_write_failure_status=$3
	l_remote_rollback_failure_status=$4
	l_remote_indent='	'

	printf '%s' "l_forwarded_had_existing=0; l_forwarded_rollback_file=''; if [ -e '$l_forwarded_backup_file_path_single' ]; then ${l_remote_indent}l_forwarded_had_existing=1; ${l_remote_indent}l_forwarded_rollback_file=\$(mktemp '$l_forwarded_backup_file_dir_single/.zxfer-backup-rollback.XXXXXX' 2>/dev/null) || { cleanup_stages; exit $l_remote_write_failure_status; }; ${l_remote_indent}if ! mv -f '$l_forwarded_backup_file_path_single' \"\$l_forwarded_rollback_file\"; then rm -f \"\$l_forwarded_rollback_file\" 2>/dev/null || true; cleanup_stages; exit $l_remote_write_failure_status; fi; fi; if ! mv -f \"\$l_forwarded_stage_file\" '$l_forwarded_backup_file_path_single'; then if ! rollback_forwarded; then cleanup_stages; exit $l_remote_rollback_failure_status; fi; cleanup_stages; exit $l_remote_write_failure_status; fi"
}

# Purpose: Render the primary side of a remote pair publish.
# Usage: Called by the transactional pair write builder after the forwarded
# alias is live, with rollback of both files on primary failure.
zxfer_render_remote_backup_pair_primary_publish_cmd() {
	l_primary_backup_file_dir_single=$1
	l_primary_backup_file_path_single=$2
	l_remote_write_failure_status=$3
	l_remote_rollback_failure_status=$4
	l_remote_indent='	'

	printf '%s' "l_primary_had_existing=0; l_primary_rollback_file=''; if [ -e '$l_primary_backup_file_path_single' ]; then ${l_remote_indent}l_primary_had_existing=1; ${l_remote_indent}l_primary_rollback_file=\$(mktemp '$l_primary_backup_file_dir_single/.zxfer-backup-rollback.XXXXXX' 2>/dev/null) || { if ! rollback_forwarded; then cleanup_stages; exit $l_remote_rollback_failure_status; fi; cleanup_stages; exit $l_remote_write_failure_status; }; ${l_remote_indent}if ! mv -f '$l_primary_backup_file_path_single' \"\$l_primary_rollback_file\"; then rm -f \"\$l_primary_rollback_file\" 2>/dev/null || true; if ! rollback_forwarded; then cleanup_stages; exit $l_remote_rollback_failure_status; fi; cleanup_stages; exit $l_remote_write_failure_status; fi; fi; if ! mv -f \"\$l_primary_stage_file\" '$l_primary_backup_file_path_single'; then ${l_remote_indent}l_primary_restore_failed=0; ${l_remote_indent}if [ \"\$l_primary_had_existing\" -eq 1 ] && [ \"\$l_primary_rollback_file\" != '' ]; then if ! mv -f \"\$l_primary_rollback_file\" '$l_primary_backup_file_path_single' 2>/dev/null; then l_primary_restore_failed=1; else if [ -e \"\$l_primary_rollback_file\" ]; then rm -f \"\$l_primary_rollback_file\" 2>/dev/null || true; fi; fi; fi; ${l_remote_indent}if ! rollback_forwarded; then cleanup_stages; exit $l_remote_rollback_failure_status; fi; ${l_remote_indent}if [ \"\$l_primary_restore_failed\" -eq 1 ]; then cleanup_stages; exit $l_remote_rollback_failure_status; fi; ${l_remote_indent}cleanup_stages; ${l_remote_indent}exit $l_remote_write_failure_status; fi"
}

# Purpose: Render successful remote pair-write rollback cleanup.
# Usage: Called by the transactional pair write builder after both final backup
# files are live and rollback files can be discarded.
zxfer_render_remote_backup_pair_finish_cmd() {
	printf '%s' "if [ \"\$l_forwarded_had_existing\" -eq 1 ] && [ \"\$l_forwarded_rollback_file\" != '' ]; then rm -f \"\$l_forwarded_rollback_file\" 2>/dev/null || true; fi; if [ \"\$l_primary_had_existing\" -eq 1 ] && [ \"\$l_primary_rollback_file\" != '' ]; then rm -f \"\$l_primary_rollback_file\" 2>/dev/null || true; fi; cleanup_stages"
}

# Purpose: Build the remote backup write command for the next execution or
# comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_write_cmd() {
	l_backup_file_dir=$1
	l_backup_file_path=$2
	l_host=$3
	l_remote_write_helper_safe=$4
	l_remote_dependency_status=${5:-99}
	l_remote_write_failure_status=${6:-92}

	l_backup_file_dir_single=$(zxfer_escape_for_single_quotes "$l_backup_file_dir")
	l_backup_file_path_single=$(zxfer_escape_for_single_quotes "$l_backup_file_path")
	l_remote_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd "$l_host" "$l_remote_dependency_status" mktemp chmod mv rm rmdir)
	l_target_guard_cmd=$(zxfer_render_remote_backup_target_write_guard_cmd "$l_backup_file_path_single" "$l_remote_write_failure_status")
	l_stage_cleanup_cmd=$(zxfer_render_remote_backup_single_stage_cleanup_cmd)
	l_stage_setup_cmd=$(zxfer_render_remote_backup_single_stage_setup_cmd "$l_backup_file_dir_single" "$l_remote_write_failure_status")
	l_staged_target_guard_cmd=$(zxfer_render_remote_backup_staged_target_write_guard_cmd "$l_backup_file_path_single" "$l_remote_write_failure_status" "$l_stage_cleanup_cmd")
	# Stage remote writes inside the secure backup directory so validation and
	# the final rename operate on the same object.
	l_remote_write_cmd="$l_remote_dependency_check_cmd; $l_target_guard_cmd; umask 077; $l_stage_setup_cmd; if ! $l_remote_write_helper_safe >\"\$l_stage_file\"; then $l_stage_cleanup_cmd; exit $l_remote_write_failure_status; fi; if ! chmod 600 \"\$l_stage_file\"; then $l_stage_cleanup_cmd; exit $l_remote_write_failure_status; fi; $l_staged_target_guard_cmd; if ! mv -f \"\$l_stage_file\" '$l_backup_file_path_single'; then $l_stage_cleanup_cmd; exit $l_remote_write_failure_status; fi; rmdir \"\$l_stage_dir\" 2>/dev/null || true"

	zxfer_wrap_remote_backup_helper_with_secure_path "$l_remote_write_cmd"
}

# Purpose: Build the remote backup pair write command for the next execution or
# comparison step.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows before other helpers consume the assembled value.
zxfer_build_remote_backup_pair_write_cmd() {
	l_primary_backup_file_dir=$1
	l_primary_backup_file_path=$2
	l_forwarded_backup_file_dir=$3
	l_forwarded_backup_file_path=$4
	l_host=$5
	l_remote_dependency_status=${6:-99}
	l_remote_write_failure_status=${7:-92}

	l_primary_backup_file_dir_single=$(zxfer_escape_for_single_quotes "$l_primary_backup_file_dir")
	l_primary_backup_file_path_single=$(zxfer_escape_for_single_quotes "$l_primary_backup_file_path")
	l_forwarded_backup_file_dir_single=$(zxfer_escape_for_single_quotes "$l_forwarded_backup_file_dir")
	l_forwarded_backup_file_path_single=$(zxfer_escape_for_single_quotes "$l_forwarded_backup_file_path")
	l_pair_split_line_single=$(zxfer_escape_for_single_quotes "$(zxfer_get_backup_metadata_pair_split_line)")
	l_remote_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd "$l_host" "$l_remote_dependency_status" mktemp chmod mv rm rmdir awk)
	l_remote_rollback_failure_status=98
	l_cleanup_function_cmd=$(zxfer_render_remote_backup_pair_cleanup_function_cmd)
	l_forwarded_rollback_function_cmd=$(zxfer_render_remote_backup_pair_forwarded_rollback_function_cmd "$l_forwarded_backup_file_path_single")
	l_pair_target_guard_cmd=$(zxfer_render_remote_backup_pair_target_guard_cmd "$l_primary_backup_file_path_single" "$l_forwarded_backup_file_path_single" "$l_remote_write_failure_status")
	l_pair_stage_setup_cmd=$(zxfer_render_remote_backup_pair_stage_setup_cmd "$l_primary_backup_file_dir_single" "$l_forwarded_backup_file_dir_single" "$l_remote_write_failure_status")
	l_pair_payload_split_cmd=$(zxfer_render_remote_backup_pair_payload_split_cmd "$l_pair_split_line_single" "$l_remote_write_failure_status")
	l_pair_stage_chmod_cmd=$(zxfer_render_remote_backup_pair_stage_chmod_cmd "$l_remote_write_failure_status")
	l_forwarded_publish_cmd=$(zxfer_render_remote_backup_pair_forwarded_publish_cmd "$l_forwarded_backup_file_dir_single" "$l_forwarded_backup_file_path_single" "$l_remote_write_failure_status" "$l_remote_rollback_failure_status")
	l_primary_publish_cmd=$(zxfer_render_remote_backup_pair_primary_publish_cmd "$l_primary_backup_file_dir_single" "$l_primary_backup_file_path_single" "$l_remote_write_failure_status" "$l_remote_rollback_failure_status")
	l_pair_finish_cmd=$(zxfer_render_remote_backup_pair_finish_cmd)
	l_remote_pair_write_cmd="$l_remote_dependency_check_cmd; $l_cleanup_function_cmd; $l_forwarded_rollback_function_cmd; $l_pair_target_guard_cmd; umask 077; $l_pair_stage_setup_cmd; $l_pair_payload_split_cmd; $l_pair_stage_chmod_cmd; $l_forwarded_publish_cmd; $l_primary_publish_cmd; $l_pair_finish_cmd"

	zxfer_wrap_remote_backup_helper_with_secure_path "$l_remote_pair_write_cmd"
}

# Purpose: Read the local backup file from staged state into the current shell.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_local_backup_file() {
	l_path=$1
	g_zxfer_backup_file_read_result=""
	g_zxfer_backup_local_read_failure_result=""
	zxfer_require_backup_metadata_path_without_symlinks "$l_path" || return 1
	if [ ! -f "$l_path" ] || [ -h "$l_path" ]; then
		return 4
	fi
	# A trusted parent that is not writable cannot swap the directory entry,
	# so direct validated reads are safe when same-directory staging is blocked.
	if zxfer_backup_metadata_path_uses_trusted_nonwritable_parent "$l_path"; then
		if ! l_error=$(zxfer_check_secure_backup_file "$l_path" "$l_path"); then
			zxfer_throw_error "$l_error"
		fi
		if zxfer_read_runtime_artifact_file "$l_path" >/dev/null; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		l_backup_contents=$g_zxfer_runtime_artifact_read_result
		g_zxfer_backup_file_read_result=$l_backup_contents
		printf '%s' "$l_backup_contents"
		return 0
	fi
	if zxfer_create_backup_metadata_stage_dir_for_path "$l_path" "zxfer-backup-read" >/dev/null; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_read_failure_result=staging
		return "$l_status"
	fi
	l_stage_dir=$g_zxfer_backup_stage_dir_result
	l_snapshot_path="$l_stage_dir/backup.snapshot"
	if ln "$l_path" "$l_snapshot_path" 2>/dev/null; then
		:
	else
		l_link_status=$?
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		if [ ! -f "$l_path" ] || [ -h "$l_path" ]; then
			return 4
		fi
		return "$l_link_status"
	fi
	if ! l_error=$(zxfer_check_secure_backup_file "$l_snapshot_path" "$l_path"); then
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		zxfer_throw_error "$l_error"
	fi
	if zxfer_read_runtime_artifact_file "$l_snapshot_path" >/dev/null; then
		:
	else
		l_status=$?
		g_zxfer_backup_local_read_failure_result=staging
		zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
		return "$l_status"
	fi
	l_backup_contents=$g_zxfer_runtime_artifact_read_result
	g_zxfer_backup_file_read_result=$l_backup_contents
	printf '%s' "$l_backup_contents"
	zxfer_cleanup_backup_metadata_stage_dir "$l_stage_dir"
}

# Purpose: Render remote backup-read path setup and expected-user checks.
# Usage: Called by the remote backup read builder before choosing whether to
# stage a hard-linked snapshot beside the target file.
zxfer_render_remote_backup_read_path_setup_cmd() {
	l_path_single=$1
	l_remote_unknown_status=$2

	printf '%s' "l_target_path='$l_path_single'; \
l_parent=\${l_target_path%/*}; \
if [ \"\$l_parent\" = \"\$l_target_path\" ] || [ \"\$l_parent\" = '' ]; then l_parent=/; fi; \
l_target_ls_path=\$l_target_path; \
case \"\$l_target_ls_path\" in -*) l_target_ls_path=./\$l_target_ls_path ;; esac; \
l_parent_ls_path=\$l_parent; \
case \"\$l_parent_ls_path\" in -*) l_parent_ls_path=./\$l_parent_ls_path ;; esac; \
l_expected_uid=''; \
if command -v id >/dev/null 2>&1; then l_expected_uid=\$(id -u 2>/dev/null); fi; \
if [ \"\$l_expected_uid\" = '' ] || printf '%s' \"\$l_expected_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then exit $l_remote_unknown_status; fi"
}

# Purpose: Render remote backup-read trusted-parent detection.
# Usage: Called by the remote backup read builder so trusted non-writable
# parents can be read directly while writable parents use same-directory staging.
zxfer_render_remote_backup_read_parent_trust_cmd() {
	l_remote_awk_cmd=$1

	printf '%s' "l_use_stage_dir=1; \
if [ ! -w \"\$l_parent\" ]; then \
	l_parent_ls_line=\$(ls -ldn \"\$l_parent_ls_path\" 2>/dev/null) || l_parent_ls_line=''; \
	if [ \"\$l_parent_ls_line\" != '' ]; then \
		l_parent_uid=\$(printf '%s\n' \"\$l_parent_ls_line\" | $l_remote_awk_cmd '{print \$3}'); \
		l_parent_perm=\$(printf '%s\n' \"\$l_parent_ls_line\" | $l_remote_awk_cmd '{print \$1}'); \
		l_parent_trusted=0; \
		case \"\$l_parent_perm\" in ??????????*) ;; *) l_parent_perm='' ;; esac; \
		if [ \"\$l_parent_uid\" = '0' ] || [ \"\$l_parent_uid\" = \"\$l_expected_uid\" ]; then \
			l_parent_trusted=1; \
			if [ \"\$l_parent_perm\" = '' ]; then \
				l_parent_trusted=0; \
			else \
				l_group_write=\$(printf '%s' \"\$l_parent_perm\" | cut -c 6); \
				l_other_write=\$(printf '%s' \"\$l_parent_perm\" | cut -c 9); \
				l_sticky_char=\$(printf '%s' \"\$l_parent_perm\" | cut -c 10); \
				case \"\$l_group_write\$l_other_write\" in \
				*w*) case \"\$l_sticky_char\" in t|T) ;; *) l_parent_trusted=0 ;; esac ;; \
				esac; \
			fi; \
		fi; \
		if [ \"\$l_parent_trusted\" = '1' ]; then l_use_stage_dir=0; fi; \
	fi; \
fi"
}

# Purpose: Render remote backup-read staging setup.
# Usage: Called by the remote backup read builder when the target should be
# hard-linked into a private sibling directory before validation and reading.
zxfer_render_remote_backup_read_stage_setup_cmd() {
	l_remote_stage_dependency_check_cmd=$1
	l_remote_missing_status=$2
	l_remote_unknown_status=$3

	printf '%s' "if [ \"\$l_use_stage_dir\" = '1' ]; then \
	$l_remote_stage_dependency_check_cmd; \
	umask 077; \
	l_stage_dir=\$(mktemp -d \"\$l_parent/.zxfer-backup-read.XXXXXX\" 2>/dev/null) || exit $l_remote_unknown_status; \
	l_snapshot_path=\"\$l_stage_dir/backup.snapshot\"; \
	if ! ln \"\$l_target_path\" \"\$l_snapshot_path\" 2>/dev/null; then if [ ! -f \"\$l_target_path\" ] || [ -h \"\$l_target_path\" ]; then rmdir \"\$l_stage_dir\" 2>/dev/null || true; exit $l_remote_missing_status; fi; rmdir \"\$l_stage_dir\" 2>/dev/null || true; exit $l_remote_unknown_status; fi; \
	l_snapshot_ls_path=\$l_snapshot_path; \
	case \"\$l_snapshot_ls_path\" in -*) l_snapshot_ls_path=./\$l_snapshot_ls_path ;; esac; \
else \
	l_snapshot_path=\$l_target_path; \
	l_snapshot_ls_path=\$l_target_ls_path; \
fi"
}

# Purpose: Render remote backup-read stage cleanup.
# Usage: Called by remote backup-read validation and payload builders so each
# failure path removes staged hard links and private stage directories the same
# way.
zxfer_render_remote_backup_read_stage_cleanup_cmd() {
	printf '%s' "if [ \"\$l_use_stage_dir\" = '1' ]; then rm -f \"\$l_snapshot_path\"; rmdir \"\$l_stage_dir\" 2>/dev/null || true; fi"
}

# Purpose: Render remote backup-read owner and mode validation.
# Usage: Called by the remote backup read builder before the remote cat helper
# reads the staged snapshot path.
zxfer_render_remote_backup_read_validation_cmd() {
	l_remote_awk_cmd=$1
	l_remote_unknown_status=$2
	l_remote_insecure_owner_status=$3
	l_remote_insecure_mode_status=$4
	l_remote_read_cleanup_cmd=$5

	printf '%s' "l_uid=''; \
if command -v stat >/dev/null 2>&1; then l_uid=\$(stat -c '%u' \"\$l_snapshot_path\" 2>/dev/null); if [ \"\$l_uid\" = '' ] || printf '%s' \"\$l_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_uid=\$(stat -f '%u' \"\$l_snapshot_path\" 2>/dev/null); fi; fi; \
if [ \"\$l_uid\" = '' ] || printf '%s' \"\$l_uid\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_ls_line=\$(ls -ldn \"\$l_snapshot_ls_path\" 2>/dev/null) || l_ls_line=''; if [ \"\$l_ls_line\" != '' ]; then l_uid=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$3}'); fi; fi; \
if [ \"\$l_uid\" = '' ]; then $l_remote_read_cleanup_cmd; exit $l_remote_unknown_status; fi; \
if [ \"\$l_uid\" != '0' ] && [ \"\$l_uid\" != \"\$l_expected_uid\" ]; then $l_remote_read_cleanup_cmd; exit $l_remote_insecure_owner_status; fi; \
l_mode=''; \
if command -v stat >/dev/null 2>&1; then l_mode=\$(stat -c '%a' \"\$l_snapshot_path\" 2>/dev/null); if [ \"\$l_mode\" = '' ] || printf '%s' \"\$l_mode\" | grep -q '[^0-9]' >/dev/null 2>&1; then l_mode=\$(stat -f '%OLp' \"\$l_snapshot_path\" 2>/dev/null); fi; fi; \
if [ \"\$l_mode\" = '' ] || printf '%s' \"\$l_mode\" | grep -q '[^0-9]' >/dev/null 2>&1; then if [ \"\$l_ls_line\" = '' ]; then l_ls_line=\$(ls -ldn \"\$l_snapshot_ls_path\" 2>/dev/null) || l_ls_line=''; fi; if [ \"\$l_ls_line\" != '' ]; then l_perm=\$(printf '%s\n' \"\$l_ls_line\" | $l_remote_awk_cmd '{print \$1}'); if [ \"\$l_perm\" = '-rw-------' ]; then l_mode='600'; fi; fi; fi; \
	if [ \"\$l_mode\" = '' ]; then $l_remote_read_cleanup_cmd; exit $l_remote_unknown_status; fi; \
if [ \"\$l_mode\" != '600' ]; then $l_remote_read_cleanup_cmd; exit $l_remote_insecure_mode_status; fi"
}

# Purpose: Render remote backup-read payload emission and stage cleanup.
# Usage: Called by the remote backup read builder after validation has selected
# the snapshot path to read.
zxfer_render_remote_backup_read_payload_cmd() {
	l_remote_cat_helper_cmd=$1
	l_remote_read_cleanup_cmd=$2

	printf '%s' "$l_remote_cat_helper_cmd \"\$l_snapshot_path\"; \
l_read_status=\$?; \
$l_remote_read_cleanup_cmd; \
exit \$l_read_status"
}

# Purpose: Read the remote backup file from staged state into the current
# shell.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_remote_backup_file() {
	l_host=$1
	l_path=$2
	l_profile_side=${3:-}

	g_zxfer_backup_file_read_result=""
	l_path_single=$(zxfer_escape_for_single_quotes "$l_path")
	l_remote_transport_status=6
	l_remote_capture_status=7
	l_remote_missing_status=94
	l_remote_insecure_owner_status=95
	l_remote_insecure_mode_status=96
	l_remote_unknown_status=97
	l_remote_symlink_status=98
	l_remote_dependency_status=93
	l_remote_awk_cmd="awk"
	l_remote_cat_helper=${g_cmd_cat:-cat}
	l_remote_cat_helper_cmd=$(zxfer_build_shell_command_from_argv "$l_remote_cat_helper")
	l_remote_symlink_guard_cmd=$(zxfer_build_remote_backup_metadata_symlink_guard_cmd "$l_path_single" "$l_remote_symlink_status")
	l_remote_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd "$l_host" "$l_remote_dependency_status" id grep ls awk cut)
	l_remote_stage_dependency_check_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd "$l_host" "$l_remote_dependency_status" mktemp ln rm rmdir)
	l_remote_path_setup_cmd=$(zxfer_render_remote_backup_read_path_setup_cmd "$l_path_single" "$l_remote_unknown_status")
	l_remote_parent_trust_cmd=$(zxfer_render_remote_backup_read_parent_trust_cmd "$l_remote_awk_cmd")
	l_remote_stage_setup_cmd=$(zxfer_render_remote_backup_read_stage_setup_cmd "$l_remote_stage_dependency_check_cmd" "$l_remote_missing_status" "$l_remote_unknown_status")
	l_remote_read_cleanup_cmd=$(zxfer_render_remote_backup_read_stage_cleanup_cmd)
	l_remote_validation_cmd=$(zxfer_render_remote_backup_read_validation_cmd "$l_remote_awk_cmd" "$l_remote_unknown_status" "$l_remote_insecure_owner_status" "$l_remote_insecure_mode_status" "$l_remote_read_cleanup_cmd")
	l_remote_payload_cmd=$(zxfer_render_remote_backup_read_payload_cmd "$l_remote_cat_helper_cmd" "$l_remote_read_cleanup_cmd")
	l_remote_secure_cat_cmd="$l_remote_dependency_check_cmd; $l_remote_symlink_guard_cmd; if [ ! -f '$l_path_single' ] || [ -h '$l_path_single' ]; then exit $l_remote_missing_status; fi; $l_remote_path_setup_cmd; $l_remote_parent_trust_cmd; $l_remote_stage_setup_cmd; $l_remote_validation_cmd; $l_remote_payload_cmd"
	l_remote_secure_cat_cmd=$(zxfer_wrap_remote_backup_helper_with_secure_path "$l_remote_secure_cat_cmd")
	l_remote_secure_cat_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_secure_cat_cmd")
	if zxfer_capture_remote_probe_output "$l_host" "$l_remote_secure_cat_shell_cmd" "$l_profile_side"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi
	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		return "$l_remote_capture_status"
	fi
	if [ $l_remote_status -eq $l_remote_insecure_owner_status ]; then
		zxfer_throw_error "Refusing to use backup metadata $l_path on $l_host because it is not owned by root or the ssh user."
	fi
	if [ $l_remote_status -eq $l_remote_insecure_mode_status ]; then
		zxfer_throw_error "Refusing to use backup metadata $l_path on $l_host because its permissions are not 0600."
	fi
	if [ $l_remote_status -eq $l_remote_unknown_status ]; then
		zxfer_throw_error "Cannot determine ownership or permissions for backup metadata $l_path on $l_host."
	fi
	if [ $l_remote_status -eq $l_remote_missing_status ]; then
		return 4
	fi
	if [ $l_remote_status -eq $l_remote_dependency_status ]; then
		l_dependency_path=$(zxfer_get_remote_backup_helper_dependency_path)
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Required remote backup-metadata helper dependency not found on host $l_host in secure PATH ($l_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ $l_remote_status -eq $l_remote_symlink_status ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		return 1
	fi
	if [ $l_remote_status -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
			return "$l_remote_transport_status"
		fi
		return 5
	fi
	g_zxfer_backup_file_read_result=${g_zxfer_remote_probe_stdout:-}
	printf '%s' "${g_zxfer_remote_probe_stdout:-}"
	return 0
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

	if ! l_current_backup_file_name=$(zxfer_get_backup_metadata_filename "$l_filename_source" "$l_filename_destination"); then
		return 11
	fi
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

	if ! l_legacy_backup_file_name=$(zxfer_get_legacy_backup_metadata_filename "$l_filename_source" "$l_filename_destination"); then
		return 1
	fi
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
		zxfer_throw_error_with_usage "Backup property file $l_dataset_backup_file does not declare supported zxfer backup metadata format version #format_version:$(zxfer_get_backup_metadata_format_version)."
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
		g_zxfer_failure_class=dependency
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
	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		zxfer_throw_remote_backup_capture_error "$g_option_T_target_host" "writing backup metadata $l_backup_file_path"
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_dependency_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Required remote backup-write helper dependency not found on host $g_option_T_target_host in secure PATH ($l_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_write_failure_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
	fi
	if [ "$l_remote_write_status" -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_throw_remote_backup_transport_error "$g_option_T_target_host" "writing backup metadata $l_backup_file_path"
		fi
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
	fi
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
	l_pair_split_line=$(zxfer_get_backup_metadata_pair_split_line)
	l_remote_pair_payload=$(printf '%s\n%s\n%s\n' "$l_primary_rendered_backup_contents" "$l_pair_split_line" "$l_forwarded_backup_contents")
	if zxfer_run_remote_backup_helper_with_payload "$g_option_T_target_host" "$l_remote_pair_write_shell_cmd" "$l_remote_pair_payload" destination; then
		l_remote_write_status=0
	else
		l_remote_write_status=$?
	fi
	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		zxfer_throw_remote_backup_capture_error "$g_option_T_target_host" "writing backup metadata $l_primary_backup_file_path"
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_dependency_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Required remote backup-write helper dependency not found on host $g_option_T_target_host in secure PATH ($l_dependency_path). Review prior stderr for the missing tool name."
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_rollback_failure_status" ]; then
		zxfer_throw_backup_write_rollback_error
	fi
	if [ "$l_remote_write_status" -eq "$l_remote_write_failure_status" ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_emit_remote_probe_failure_message >&2
		fi
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
	fi
	if [ "$l_remote_write_status" -ne 0 ]; then
		if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
			zxfer_throw_remote_backup_transport_error "$g_option_T_target_host" "writing backup metadata $l_primary_backup_file_path"
		fi
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
	fi
}

# Purpose: Render the backup metadata pair payload command as a stable shell-
# safe or operator-facing string.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs to display or transport the value without reparsing
# it.
zxfer_render_backup_metadata_pair_payload_command() {
	l_primary_rendered_backup_contents=$1
	l_forwarded_backup_contents=$2
	l_pair_split_line=$(zxfer_get_backup_metadata_pair_split_line)

	zxfer_render_command_for_report "" printf '%s\\n%s\\n%s\\n' "$l_primary_rendered_backup_contents" "$l_pair_split_line" "$l_forwarded_backup_contents"
}

# Purpose: Render a remote backup write command as the ssh pipeline segment used
# by dry-run output.
# Usage: Called by backup-metadata dry-run rendering for single-file and pair
# remote writes so wrapped host specs get the same `sh -c` handling.
zxfer_render_remote_backup_dry_run_shell_command() {
	l_host=$1
	l_remote_backup_cmd=$2

	g_zxfer_remote_backup_dry_run_shell_command_result=""
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$l_host" "$l_remote_backup_cmd" ||
		return "$?"
	g_zxfer_remote_backup_dry_run_shell_command_result=$g_zxfer_prepared_ssh_shell_command_result
	return 0
}

# Purpose: Render the local backup file pair write command as a stable shell-
# safe or operator-facing string.
# Usage: Called during backup-metadata capture, readback, and atomic publish
# flows when zxfer needs to display or transport the value without reparsing
# it.
zxfer_render_local_backup_file_pair_write_command() {
	l_primary_backup_file_dir=$1
	l_primary_backup_file_path=$2
	l_primary_rendered_backup_contents=$3
	l_forwarded_backup_file_dir=$4
	l_forwarded_backup_file_path=$5
	l_forwarded_backup_contents=$6

	l_primary_backup_contents_cmd=$(zxfer_render_command_for_report "" printf '%s' "$l_primary_rendered_backup_contents")
	l_forwarded_backup_contents_cmd=$(zxfer_render_command_for_report "" printf '%s' "$l_forwarded_backup_contents")
	l_primary_backup_stage_template_safe=$(zxfer_quote_token_for_report "$l_primary_backup_file_dir/.zxfer-backup-write.XXXXXX")
	l_forwarded_backup_stage_template_safe=$(zxfer_quote_token_for_report "$l_forwarded_backup_file_dir/.zxfer-backup-write.XXXXXX")
	l_primary_backup_rollback_template_safe=$(zxfer_quote_token_for_report "$l_primary_backup_file_dir/.zxfer-backup-rollback.XXXXXX")
	l_forwarded_backup_rollback_template_safe=$(zxfer_quote_token_for_report "$l_forwarded_backup_file_dir/.zxfer-backup-rollback.XXXXXX")
	l_primary_backup_file_path_safe=$(zxfer_quote_token_for_report "$l_primary_backup_file_path")
	l_forwarded_backup_file_path_safe=$(zxfer_quote_token_for_report "$l_forwarded_backup_file_path")

	printf '%s\n' "umask 077; l_primary_stage_dir=\$(mktemp -d $l_primary_backup_stage_template_safe) && l_forwarded_stage_dir=\$(mktemp -d $l_forwarded_backup_stage_template_safe) && $l_primary_backup_contents_cmd > \"\$l_primary_stage_dir/backup.write\" && $l_forwarded_backup_contents_cmd > \"\$l_forwarded_stage_dir/backup.write\" && chmod 600 \"\$l_primary_stage_dir/backup.write\" \"\$l_forwarded_stage_dir/backup.write\" && if [ -e $l_forwarded_backup_file_path_safe ]; then l_forwarded_rollback=\$(mktemp $l_forwarded_backup_rollback_template_safe) && mv -f $l_forwarded_backup_file_path_safe \"\$l_forwarded_rollback\"; else l_forwarded_rollback=''; fi && if ! mv -f \"\$l_forwarded_stage_dir/backup.write\" $l_forwarded_backup_file_path_safe; then rm -f $l_forwarded_backup_file_path_safe && if [ \"\$l_forwarded_rollback\" != '' ]; then mv -f \"\$l_forwarded_rollback\" $l_forwarded_backup_file_path_safe; fi; exit 1; fi && if [ -e $l_primary_backup_file_path_safe ]; then l_primary_rollback=\$(mktemp $l_primary_backup_rollback_template_safe) && mv -f $l_primary_backup_file_path_safe \"\$l_primary_rollback\"; else l_primary_rollback=''; fi && if ! mv -f \"\$l_primary_stage_dir/backup.write\" $l_primary_backup_file_path_safe; then rm -f $l_primary_backup_file_path_safe && if [ \"\$l_primary_rollback\" != '' ]; then mv -f \"\$l_primary_rollback\" $l_primary_backup_file_path_safe; fi; rm -f $l_forwarded_backup_file_path_safe && if [ \"\$l_forwarded_rollback\" != '' ]; then mv -f \"\$l_forwarded_rollback\" $l_forwarded_backup_file_path_safe; fi; exit 1; fi && rm -f \"\${l_forwarded_rollback:-}\" \"\${l_primary_rollback:-}\" && rmdir \"\$l_primary_stage_dir\" \"\$l_forwarded_stage_dir\""
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
				zxfer_render_local_backup_file_pair_write_command "$l_backup_file_parent" "$l_backup_file_path" "$l_rendered_backup_contents" "$l_forwarded_backup_file_parent" "$l_forwarded_backup_file_path" "$l_forwarded_backup_contents"
			else
				l_pair_backup_contents_cmd=$(zxfer_render_backup_metadata_pair_payload_command "$l_rendered_backup_contents" "$l_forwarded_backup_contents")
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
