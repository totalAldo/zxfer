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
# PROPERTY STATE / SERIALIZATION / PREFETCH HELPERS
################################################################################

# Module contract:
# owns globals: immutable prefetch-grouping AWK programs plus mutable property
#   result channels and their reset/access lifecycle, including serialization
#   results, normalized/required-property lookup state, per-iteration tables,
#   lookup memos, and recursive prefetch context.
# reads globals: g_LZFS/g_RZFS, recursive dataset lists, runtime artifact
#   results, destination mutation state, and profiling counters.
# mutates caches: in-memory property tables through prefetch, live-lookup
#   appends, and targeted destination mutation invalidation.
# returns via stdout: checked stage payloads, serialized/decoded property
#   records, table lookups, and normalized property payloads.

# Purpose: Reset the property reconcile state so the next property-reconcile
# pass starts from a clean state.
# Usage: Called during property filtering, diffing, and apply before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_property_reconcile_state() {
	g_zxfer_new_rmvs_pv=""
	g_zxfer_new_rmv_pvs=""
	g_zxfer_only_supported_properties=""
	g_zxfer_adjusted_set_list=""
	g_zxfer_adjusted_inherit_list=""
	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
	g_zxfer_override_pvs_result=""
	g_zxfer_creation_pvs_result=""
	g_zxfer_property_reconcile_stage_file_result=""
	g_zxfer_property_stage_file_read_result=""
	g_zxfer_required_property_backfill_result=""
	g_zxfer_property_transfer_is_initial_source=0
	g_zxfer_property_transfer_source_dstype=""
	g_zxfer_property_transfer_source_volsize=""
	g_zxfer_property_transfer_must_create_properties=""
	g_zxfer_property_transfer_source_pvs=""
	g_zxfer_property_transfer_override_pvs=""
	g_zxfer_property_transfer_creation_pvs=""
	g_zxfer_property_transfer_dest_pvs=""
	g_zxfer_property_transfer_initial_set_list=""
	g_zxfer_property_transfer_child_set_list=""
	g_zxfer_property_transfer_inherit_list=""
}

# Purpose: Publish the source-removal result through its state-owned channel.
# Usage: Called by property policy after rebuilding the property/value list.
# Side effects: Replaces g_zxfer_new_rmvs_pv without writing stdout.
zxfer_publish_remove_sources_result() {
	g_zxfer_new_rmvs_pv=$1
}

# Purpose: Publish the property-removal result through its state-owned channel.
# Usage: Called by property policy after filtering readonly or ignored entries.
# Side effects: Replaces g_zxfer_new_rmv_pvs without writing stdout.
zxfer_publish_remove_properties_result() {
	g_zxfer_new_rmv_pvs=$1
}

# Purpose: Publish the supported-property result through its state-owned channel.
# Usage: Called by property policy before and after unsupported-property filtering.
# Side effects: Replaces g_zxfer_only_supported_properties without writing stdout.
zxfer_publish_supported_properties_result() {
	g_zxfer_only_supported_properties=$1
}

# Purpose: Publish raw and effective source-property results as one state update.
# Usage: Called while source collection, restore, and required-property backfill
# advance the two related result channels.
# Side effects: Replaces g_zxfer_source_pvs_raw and
# g_zxfer_source_pvs_effective without writing stdout.
zxfer_publish_source_property_results() {
	g_zxfer_source_pvs_raw=$1
	g_zxfer_source_pvs_effective=$2
}

# Purpose: Publish derived override and creation-property results together.
# Usage: Called by property policy after clearing or parsing the two-line AWK result.
# Side effects: Replaces g_zxfer_override_pvs_result and
# g_zxfer_creation_pvs_result without writing stdout.
zxfer_publish_override_property_results() {
	g_zxfer_override_pvs_result=$1
	g_zxfer_creation_pvs_result=$2
}

# Purpose: Set adjusted child property lists without emitting their public output.
# Usage: Called before inheritance adjustment so stale results are unavailable.
# Side effects: Replaces g_zxfer_adjusted_set_list and
# g_zxfer_adjusted_inherit_list.
zxfer_set_adjusted_property_lists() {
	g_zxfer_adjusted_set_list=$1
	g_zxfer_adjusted_inherit_list=$2
}

# Purpose: Publish adjusted child property lists through state and stdout.
# Usage: Called by inheritance reconciliation for both early and calculated results.
# Returns: The set and inherit lists as two newline-delimited stdout records.
zxfer_publish_adjusted_property_lists() {
	zxfer_set_adjusted_property_lists "$1" "$2"
	printf '%s\n' "$g_zxfer_adjusted_set_list"
	printf '%s\n' "$g_zxfer_adjusted_inherit_list"
}

# Purpose: Publish one required-property backfill result through state.
# Usage: Called after clearing or checking a staged required-property payload.
# Side effects: Replaces g_zxfer_required_property_backfill_result.
zxfer_publish_required_property_backfill_result() {
	g_zxfer_required_property_backfill_result=$1
}

# Purpose: Set whether the current property transfer is for the initial source.
# Usage: Called when source-context preparation identifies the current dataset.
# Side effects: Replaces g_zxfer_property_transfer_is_initial_source.
zxfer_set_property_transfer_initial_source() {
	g_zxfer_property_transfer_is_initial_source=$1
}

# Purpose: Publish create-time source metadata for the current property transfer.
# Usage: Called after validated type/volume metadata and required properties are known.
# Side effects: Replaces the source type, volume size, and must-create channels.
zxfer_publish_property_transfer_source_metadata() {
	g_zxfer_property_transfer_source_dstype=$1
	g_zxfer_property_transfer_source_volsize=$2
	g_zxfer_property_transfer_must_create_properties=$3
}

# Purpose: Publish the effective source properties for the current transfer.
# Usage: Called after required-property backfill finishes for the source dataset.
# Side effects: Replaces g_zxfer_property_transfer_source_pvs.
zxfer_publish_property_transfer_source_properties() {
	g_zxfer_property_transfer_source_pvs=$1
}

# Purpose: Publish override and creation lists for the current property transfer.
# Usage: Called after each derivation, sanitization, or unsupported-property stage.
# Side effects: Replaces the paired transfer override and creation channels.
zxfer_publish_property_transfer_override_results() {
	g_zxfer_property_transfer_override_pvs=$1
	g_zxfer_property_transfer_creation_pvs=$2
}

# Purpose: Publish destination properties for the current property transfer.
# Usage: Called after destination backfill and sanitization complete.
# Side effects: Replaces g_zxfer_property_transfer_dest_pvs.
zxfer_publish_property_transfer_destination_properties() {
	g_zxfer_property_transfer_dest_pvs=$1
}

# Purpose: Publish the three-way diff plan for the current property transfer.
# Usage: Called after diff readback and again when child inheritance is adjusted.
# Side effects: Replaces the initial-set, child-set, and inherit plan channels.
zxfer_publish_property_transfer_diff_results() {
	g_zxfer_property_transfer_initial_set_list=$1
	g_zxfer_property_transfer_child_set_list=$2
	g_zxfer_property_transfer_inherit_list=$3
}

# Purpose: Read the property reconcile stage file from staged state into the
# current shell.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a checked reload instead of ad hoc file reads.
zxfer_read_property_reconcile_stage_file() {
	l_property_stage_file=$1

	g_zxfer_property_stage_file_read_result=""
	if zxfer_read_runtime_artifact_file_trimmed \
		"$l_property_stage_file" >/dev/null; then
		g_zxfer_property_stage_file_read_result=$g_zxfer_runtime_artifact_read_result
	else
		l_property_stage_read_status=$?
		return "$l_property_stage_read_status"
	fi
	printf '%s\n' "$g_zxfer_property_stage_file_read_result"
}

# Purpose: Allocate a property-reconcile stage file and preserve allocation
# failures without assuming the lower-level temp helper throws.
# Usage: Called during property filtering, diffing, and apply when a helper
# needs one scratch file owned by runtime cleanup.
# Side effects: Publishes the allocated path in
# $g_zxfer_property_reconcile_stage_file_result.
zxfer_create_property_reconcile_stage_file() {
	g_zxfer_property_reconcile_stage_file_result=""
	zxfer_get_temp_file >/dev/null || return "$?"
	g_zxfer_property_reconcile_stage_file_result=$g_zxfer_temp_file_result
}

################################################################################
# PROPERTY NORMALIZATION / IN-MEMORY PROPERTY TABLES / PREFETCH HELPERS
################################################################################

# The per-iteration property cache is a set of in-memory tables, one per lookup
# side per replication iteration. Each table is a flat newline-delimited
# variable of 'dataset<TAB>payload' rows where the payload reuses the
# serialized property encoding (values escape %ROW delimiters, tabs, carriage
# returns, and line feeds), so one logical record is always exactly one line.
# Lookups slice the table with parameter expansion (no process spawns), a
# last-dataset memo short-circuits repeated lookups, and destination mutations
# strip only the mutated dataset and its descendants so the rest of the
# prefetched tree stays warm.

ZXFER_REQUIRED_PROPERTY_UNSUPPORTED_SENTINEL="__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__"

# Purpose: Serialize property records from stdin into zxfer's stable property
# record encoding.
# Usage: Called during property prefetch and normalized property lookup before
# property payloads are stored in the in-memory tables or compared.
zxfer_serialize_property_records_from_stdin() {
	# shellcheck disable=SC2016
	"${g_cmd_awk:-awk}" -F '	' '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function encode_value(value) {
	gsub(/%/, "%25", value)
	gsub(/,/, "%2C", value)
	gsub(/=/, "%3D", value)
	gsub(/;/, "%3B", value)
	gsub(/\t/, "%09", value)
	gsub(/\r/, "%0D", value)
	gsub(/\n/, "%0A", value)
	return value
}
function valid_property_name(name) {
	return name ~ /^[A-Za-z0-9_.:@-][A-Za-z0-9_.:@-]*$/
}
function valid_source(source) {
	return source == "-" ||
		source == "local" ||
		source == "default" ||
		source == "temporary" ||
		source == "received" ||
		source == "inherited" ||
		source == "none" ||
		source ~ /^inherited from [^	]+$/
}
function record_is_complete(record, fields, field_count) {
	field_count = split(record, fields, "[	]")
	return field_count >= 3 && valid_source(fields[field_count])
}
function line_starts_property_record(line, fields, field_count) {
	field_count = split(line, fields, "[	]")
	return field_count >= 2 && valid_property_name(fields[1])
}
function flush_record(record, fields, field_count, value, i, property_name, property_source) {
	field_count = split(record, fields, "[	]")
	property_name = fields[1]
	property_source = fields[field_count]
	if (field_count < 3 || !valid_property_name(property_name) || !valid_source(property_source)) {
		parse_failed = 1
		return
	}
	value = fields[2]
	for (i = 3; i < field_count; i++)
		value = value "\t" fields[i]
	output = append_csv(output, property_name "=" encode_value(value) "=" property_source)
}
{
	if (current_record == "") {
		current_record = $0
		next
	}
	if (record_is_complete(current_record) && line_starts_property_record($0)) {
		flush_record(current_record)
		current_record = $0
		next
	}
	current_record = current_record "\n" $0
}
END {
	if (current_record != "")
		flush_record(current_record)
	if (parse_failed)
		exit 1
	print output
}'
}

# Purpose: Capture the serialized property records into staged state or module
# globals for later use.
# Usage: Called during property prefetch and normalized property lookup when
# later helpers need a checked snapshot of command output or computed state.
zxfer_capture_serialized_property_records() {
	l_property_records=$1

	g_zxfer_serialized_property_records_result=""
	g_zxfer_serialized_property_records_parse_failed=0

	zxfer_get_temp_file >/dev/null || return "$?"
	l_serialized_output_file=$g_zxfer_temp_file_result

	l_serialize_status=0
	zxfer_serialize_property_records_from_stdin >"$l_serialized_output_file" <<EOF || l_serialize_status=$?
$l_property_records
EOF
	if [ "$l_serialize_status" -ne 0 ]; then
		if [ "$l_serialize_status" -eq 1 ]; then
			g_zxfer_serialized_property_records_parse_failed=1
		fi
		zxfer_cleanup_runtime_artifact_path "$l_serialized_output_file"
		return "$l_serialize_status"
	fi

	zxfer_read_runtime_artifact_file "$l_serialized_output_file" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_serialized_output_file"
		return "$l_read_status"
	}
	zxfer_cleanup_runtime_artifact_path "$l_serialized_output_file"

	g_zxfer_serialized_property_records_result=$g_zxfer_runtime_artifact_read_result
	case "$g_zxfer_serialized_property_records_result" in
	*'
')
		g_zxfer_serialized_property_records_result=${g_zxfer_serialized_property_records_result%?}
		;;
	esac

	return 0
}

# Purpose: Decode one serialized property assignment while preserving trailing
# line feeds in the decoded value.
# Usage: Called before building `zfs create` or `zfs set` argv entries so one
# encoded property item becomes exactly one shell argument.
zxfer_decode_serialized_property_assignment() {
	l_property_item=$1

	g_zxfer_decoded_property_assignment_result=""
	l_decode_sentinel=$(printf '\001')
	l_decoded_assignment=$("${g_cmd_awk:-awk}" \
		-v property_item="$l_property_item" \
		-v sentinel="$l_decode_sentinel" '
function decode_value(value) {
	gsub(/%0D/, "\r", value)
	gsub(/%0A/, "\n", value)
	gsub(/%09/, "\t", value)
	gsub(/%3B/, ";", value)
	gsub(/%3D/, "=", value)
	gsub(/%2C/, ",", value)
	gsub(/%25/, "%", value)
	return value
}
BEGIN {
	split(property_item, property_fields, "=")
	property_name = property_fields[1]
	property_value = substr(property_item, length(property_name) + 2)
	printf "%s=%s%s", property_name, decode_value(property_value), sentinel
}')
	l_decode_status=$?
	[ "$l_decode_status" -eq 0 ] || return "$l_decode_status"

	g_zxfer_decoded_property_assignment_result=${l_decoded_assignment%"$l_decode_sentinel"}
	printf '%s\n' "$g_zxfer_decoded_property_assignment_result"
}

# Purpose: Decode a serialized property list into the operator-facing form used
# by reports and debugging output.
# Usage: Called during property apply logging when cached property data needs
# to be displayed instead of reapplied directly.
zxfer_decode_serialized_property_list_for_display() {
	l_property_list=$1

	"${g_cmd_awk:-awk}" -v property_list="$l_property_list" '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function decode_value(value) {
	gsub(/%0D/, "\r", value)
	gsub(/%0A/, "\n", value)
	gsub(/%09/, "\t", value)
	gsub(/%3B/, ";", value)
	gsub(/%3D/, "=", value)
	gsub(/%2C/, ",", value)
	gsub(/%25/, "%", value)
	return value
}
BEGIN {
	property_count = split(property_list, property_items, ",")
	for (i = 1; i <= property_count; i++) {
		if (property_items[i] == "")
			continue
		field_count = split(property_items[i], property_fields, "=")
		property_name = property_fields[1]
		if (field_count >= 3) {
			property_source = property_fields[field_count]
			property_value = substr(property_items[i], length(property_name) + 2)
			property_value = substr(property_value, 1, length(property_value) - length(property_source) - 1)
			output = append_csv(output, property_name "=" decode_value(property_value) "=" property_source)
		} else {
			property_value = substr(property_items[i], length(property_name) + 2)
			output = append_csv(output, property_name "=" decode_value(property_value))
		}
	}
	print output
}'
}

# Purpose: Clear the last-dataset property table memo so the next lookup cannot
# reuse a payload that table maintenance just removed or replaced.
# Usage: Called whenever a property table for the memoized side is reset,
# stripped, or repopulated.
zxfer_property_table_clear_memo() {
	g_zxfer_property_table_memo_side=""
	g_zxfer_property_table_memo_dataset=""
	g_zxfer_property_table_memo_payload=""
}

# Purpose: Reset the per-iteration property tables and the per-lookup scratch
# result globals so the next property pass starts from a clean state.
# Usage: Called during runtime startup and at the top of every replication
# iteration before this module reuses the in-memory property tables.
zxfer_reset_property_iteration_caches() {
	g_zxfer_normalized_dataset_properties=""
	g_zxfer_normalized_dataset_properties_cache_hit=0
	g_zxfer_required_properties_result=""
	g_zxfer_required_property_probe_result=""
	g_zxfer_serialized_property_records_result=""
	g_zxfer_serialized_property_records_parse_failed=0
	g_zxfer_decoded_property_assignment_result=""
	g_zxfer_destination_pvs_raw=""
	g_zxfer_property_table_lookup_result=""
	zxfer_property_table_clear_memo
	g_zxfer_source_property_table=""
	g_zxfer_destination_property_table=""
	g_zxfer_source_required_property_table=""
	g_zxfer_destination_required_property_table=""
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Refresh the property tree prefetch context from the current
# configuration and runtime state.
# Usage: Called during replication iteration setup after inputs change and
# downstream helpers need the derived value rebuilt.
zxfer_refresh_property_tree_prefetch_context() {
	if [ "${g_option_R_recursive:-}" = "" ] ||
		{ [ "${g_option_P_transfer_property:-0}" -ne 1 ] &&
			[ -z "${g_option_o_override_property:-}" ]; }; then
		g_zxfer_source_property_tree_prefetch_root=""
		g_zxfer_source_property_tree_prefetch_zfs_cmd=""
		g_zxfer_source_property_tree_prefetch_state=0
		g_zxfer_destination_property_tree_prefetch_root=""
		g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
		g_zxfer_destination_property_tree_prefetch_state=0
		return
	fi

	g_zxfer_source_property_tree_prefetch_root=${g_initial_source:-}
	g_zxfer_source_property_tree_prefetch_zfs_cmd=${g_LZFS:-}
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=${g_destination:-}
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=${g_RZFS:-}
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Find one payload row in the requested side's in-memory property
# tables: the dataset-keyed normalized-property table when $3 is empty, or the
# dataset+property-keyed required-property probe table when $3 names a
# required property.
# Usage: Called during normalized property lookup
# (zxfer_load_normalized_dataset_properties,
# zxfer_maybe_prefetch_recursive_normalized_properties) and required-property
# backfill (zxfer_get_required_property_probe); publishes the payload in
# g_zxfer_property_table_lookup_result and returns non-zero on a miss.
zxfer_property_table_find_dataset() {
	l_find_side=$1
	l_find_dataset=$2
	l_find_property=${3:-}

	g_zxfer_property_table_lookup_result=""
	case "$l_find_side" in
	source)
		if [ -n "$l_find_property" ]; then
			l_find_table=${g_zxfer_source_required_property_table:-}
		else
			l_find_table=${g_zxfer_source_property_table:-}
		fi
		;;
	destination)
		if [ -n "$l_find_property" ]; then
			l_find_table=${g_zxfer_destination_required_property_table:-}
		else
			l_find_table=${g_zxfer_destination_property_table:-}
		fi
		;;
	*)
		return 1
		;;
	esac
	[ -n "$l_find_table" ] || return 1

	l_find_tab='	'
	l_find_nl='
'
	l_find_key=$l_find_nl$l_find_dataset$l_find_tab
	if [ -n "$l_find_property" ]; then
		l_find_key=$l_find_key$l_find_property$l_find_tab
	fi
	l_find_wrapped=$l_find_nl$l_find_table$l_find_nl
	case "$l_find_wrapped" in
	*"$l_find_key"*) ;;
	*)
		return 1
		;;
	esac

	l_find_payload=${l_find_wrapped#*"$l_find_key"}
	l_find_payload=${l_find_payload%%"$l_find_nl"*}
	[ -n "$l_find_payload" ] || return 1

	g_zxfer_property_table_lookup_result=$l_find_payload
	return 0
}

# Purpose: Append one payload row to the requested side's in-memory property
# tables: the dataset-keyed normalized-property table (refreshing the
# last-dataset memo) when $4 is empty, or the dataset+property-keyed
# required-property probe table (no memo; the payload may be the
# unsupported-property sentinel) when $4 names a required property.
# Usage: Called after live normalized property reads
# (zxfer_load_normalized_dataset_properties) and live required-property probes
# (zxfer_get_required_property_probe) so repeated lookups in the same
# iteration reuse the in-memory row instead of re-probing zfs.
zxfer_property_table_append_dataset() {
	l_append_side=$1
	l_append_dataset=$2
	l_append_payload=$3
	l_append_property=${4:-}

	[ -n "$l_append_payload" ] || return 0

	l_append_nl='
'
	if [ -n "$l_append_property" ]; then
		l_append_row="$l_append_dataset	$l_append_property	$l_append_payload"
	else
		l_append_row="$l_append_dataset	$l_append_payload"
	fi
	case "$l_append_side" in
	source)
		if [ -n "$l_append_property" ]; then
			if [ -n "${g_zxfer_source_required_property_table:-}" ]; then
				g_zxfer_source_required_property_table=$g_zxfer_source_required_property_table$l_append_nl$l_append_row
			else
				g_zxfer_source_required_property_table=$l_append_row
			fi
			return 0
		fi
		if [ -n "${g_zxfer_source_property_table:-}" ]; then
			g_zxfer_source_property_table=$g_zxfer_source_property_table$l_append_nl$l_append_row
		else
			g_zxfer_source_property_table=$l_append_row
		fi
		;;
	destination)
		if [ -n "$l_append_property" ]; then
			if [ -n "${g_zxfer_destination_required_property_table:-}" ]; then
				g_zxfer_destination_required_property_table=$g_zxfer_destination_required_property_table$l_append_nl$l_append_row
			else
				g_zxfer_destination_required_property_table=$l_append_row
			fi
			return 0
		fi
		if [ -n "${g_zxfer_destination_property_table:-}" ]; then
			g_zxfer_destination_property_table=$g_zxfer_destination_property_table$l_append_nl$l_append_row
		else
			g_zxfer_destination_property_table=$l_append_row
		fi
		;;
	*)
		return 0
		;;
	esac

	g_zxfer_property_table_memo_side=$l_append_side
	g_zxfer_property_table_memo_dataset=$l_append_dataset
	g_zxfer_property_table_memo_payload=$l_append_payload
	return 0
}

# Purpose: Print a property table with every row for one dataset key removed,
# optionally including the dataset's descendants.
# Usage: Called during table invalidation; reads the table from $1 and matches
# the first tab-delimited field exactly so hostile dataset names cannot widen
# or narrow the strip scope.
zxfer_property_table_strip_dataset_rows() {
	l_strip_table=$1
	l_strip_dataset=$2
	l_strip_descendants=$3

	# Pass the dataset through the environment: awk -v would reinterpret
	# backslash escapes inside hostile dataset names.
	# shellcheck disable=SC2016
	ZXFER_PROPERTY_TABLE_STRIP_DATASET=$l_strip_dataset \
		"${g_cmd_awk:-awk}" -F '	' -v strip_descendants="$l_strip_descendants" '
BEGIN {
	dataset = ENVIRON["ZXFER_PROPERTY_TABLE_STRIP_DATASET"]
	prefix = dataset "/"
	prefix_length = length(prefix)
}
$0 == "" { next }
$1 == dataset { next }
strip_descendants == 1 && substr($1, 1, prefix_length) == prefix { next }
{ print }
' <<EOF
$l_strip_table
EOF
}

# Purpose: Remove one dataset's rows (optionally with descendants) from one
# side's normalized and required property tables.
# Usage: Called during invalidation so later lookups for the stripped datasets
# fall back to live zfs gets while every other row stays warm. A failed strip
# clears the affected tables entirely, which is the safe direction: an empty
# table only forces live probes.
zxfer_property_table_invalidate_dataset() {
	l_invalidate_side=$1
	l_invalidate_dataset=$2
	l_invalidate_descendants=$3

	case "$l_invalidate_side" in
	source)
		l_invalidate_table=${g_zxfer_source_property_table:-}
		l_invalidate_required_table=${g_zxfer_source_required_property_table:-}
		;;
	destination)
		l_invalidate_table=${g_zxfer_destination_property_table:-}
		l_invalidate_required_table=${g_zxfer_destination_required_property_table:-}
		;;
	*)
		return 0
		;;
	esac

	if [ -n "$l_invalidate_table" ]; then
		l_strip_status=0
		l_invalidate_table=$(zxfer_property_table_strip_dataset_rows \
			"$l_invalidate_table" "$l_invalidate_dataset" "$l_invalidate_descendants") ||
			l_strip_status=$?
		if [ "$l_strip_status" -ne 0 ]; then
			l_invalidate_table=""
		fi
	fi
	if [ -n "$l_invalidate_required_table" ]; then
		l_strip_status=0
		l_invalidate_required_table=$(zxfer_property_table_strip_dataset_rows \
			"$l_invalidate_required_table" "$l_invalidate_dataset" "$l_invalidate_descendants") ||
			l_strip_status=$?
		if [ "$l_strip_status" -ne 0 ]; then
			l_invalidate_required_table=""
		fi
	fi

	case "$l_invalidate_side" in
	source)
		g_zxfer_source_property_table=$l_invalidate_table
		g_zxfer_source_required_property_table=$l_invalidate_required_table
		;;
	destination)
		g_zxfer_destination_property_table=$l_invalidate_table
		g_zxfer_destination_required_property_table=$l_invalidate_required_table
		;;
	esac

	if [ "${g_zxfer_property_table_memo_side:-}" = "$l_invalidate_side" ]; then
		zxfer_property_table_clear_memo
	fi
	return 0
}

# Purpose: Reset the destination property tables so the next destination
# property pass starts from a clean state.
# Usage: Called before the post-seed property reconcile pass so freshly seeded
# destinations are re-probed while source tables stay warm.
zxfer_reset_destination_property_iteration_cache() {
	g_zxfer_destination_property_table=""
	g_zxfer_destination_required_property_table=""
	if [ "${g_zxfer_property_table_memo_side:-}" = "destination" ]; then
		zxfer_property_table_clear_memo
	fi
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Invalidate destination property table rows after a live destination
# mutation.
# Usage: Called after receives, creates, sets, and inherits so descendant
# inherited-property and required-property lookups cannot reuse old state.
#
# A mutation on one destination dataset can only change cached properties for
# that dataset and its descendants (through inheritance), so invalidation is
# scoped to that subtree. The in-memory tables are their own authority for
# descendant enumeration, so no tree-wide fallback reset is needed: lookups
# for the stripped subtree fall back to live zfs gets for those datasets only
# while every other prefetched row stays warm.
zxfer_invalidate_destination_property_mutation_cache() {
	l_mutated_dataset=${1:-}

	# This is the shared choke point for receive completions (including -j
	# reap time), dataset creates, and property set/inherit: every caller
	# just mutated the destination, so stale batched live snapshot views
	# must be refreshed before the next recheck-driven decision.
	zxfer_bump_destination_mutation_generation

	if [ -z "$l_mutated_dataset" ]; then
		zxfer_reset_destination_property_iteration_cache
		return 0
	fi

	zxfer_property_table_invalidate_dataset destination "$l_mutated_dataset" 1
}

# Purpose: Return the property tree prefetch dataset list in the form expected
# by later helpers.
# Usage: Called during property prefetch when sibling helpers need the same
# lookup without duplicating module logic.
zxfer_get_property_tree_prefetch_dataset_list() {
	l_side=$1

	case "$l_side" in
	source)
		if [ -n "${g_recursive_source_dataset_list:-}" ]; then
			printf '%s\n' "$g_recursive_source_dataset_list" | tr ' ' '\n'
			return 0
		fi
		if [ -n "${g_recursive_source_list:-}" ]; then
			printf '%s\n' "$g_recursive_source_list" | tr ' ' '\n'
			return 0
		fi
		if [ -n "${g_initial_source:-}" ]; then
			printf '%s\n' "$g_initial_source"
			return 0
		fi
		;;
	destination)
		if [ -n "${g_recursive_dest_list:-}" ]; then
			printf '%s\n' "$g_recursive_dest_list" | tr ' ' '\n'
			return 0
		fi
		;;
	esac

	return 1
}

# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_GROUP_RECURSIVE_PROPERTY_TREE_AWK='
function encode_value(value) {
	gsub(/%/, "%25", value)
	gsub(/,/, "%2C", value)
	gsub(/=/, "%3D", value)
	gsub(/;/, "%3B", value)
	gsub(/\t/, "%09", value)
	gsub(/\r/, "%0D", value)
	gsub(/\n/, "%0A", value)
	return value
}
function valid_property_name(name) {
	return name ~ /^[A-Za-z0-9_.:@-][A-Za-z0-9_.:@-]*$/
}
function valid_source(source) {
	return source == "-" ||
		source == "local" ||
		source == "default" ||
		source == "temporary" ||
		source == "received" ||
		source == "inherited" ||
		source == "none" ||
		source ~ /^inherited from [^	]+$/
}
function record_is_complete(record, fields, field_count) {
	field_count = split(record, fields, "[	]")
	return field_count >= 4 && valid_source(fields[field_count])
}
function line_starts_property_record(line, fields, field_count) {
	field_count = split(line, fields, "[	]")
	return field_count >= 3 && fields[1] != "" && valid_property_name(fields[2])
}
function flush_record(record, fields, field_count, value, i, dataset, property_name, property_source, line) {
	field_count = split(record, fields, "[	]")
	dataset = fields[1]
	property_name = fields[2]
	property_source = fields[field_count]
	if (field_count < 4 || dataset == "" || !valid_property_name(property_name) || !valid_source(property_source)) {
		parse_failed = 1
		return
	}
	if (!(dataset in wanted))
		return
	if (!seen_dataset[dataset]++) {
		order[++count] = dataset
	}
	value = fields[3]
	for (i = 4; i < field_count; i++)
		value = value "\t" fields[i]
	line = property_name "=" encode_value(value) "=" property_source
	if (grouped[dataset] != "")
		grouped[dataset] = grouped[dataset] "," line
	else
		grouped[dataset] = line
}
NR == FNR {
	if ($0 != "" && !seen_filter[$0]++)
		wanted[$0] = 1
	next
}
{
	if (current_record == "") {
		current_record = $0
		next
	}
	if (record_is_complete(current_record) && line_starts_property_record($0)) {
		flush_record(current_record)
		current_record = $0
		next
	}
	current_record = current_record "\n" $0
}
END {
	if (current_record != "")
		flush_record(current_record)
	if (parse_failed)
		exit 1
	for (i = 1; i <= count; i++)
		printf "%s\t%s\n", order[i], grouped[order[i]]
}'

# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_MERGE_RECURSIVE_PROPERTY_TREES_AWK='
NR == FNR {
	machine[$1] = $2
	if (!seen[$1]++)
		order[++count] = $1
	next
}
{
	human[$1] = $2
	if (!seen[$1]++)
		order[++count] = $1
}
END {
	for (i = 1; i <= count; i++) {
		dataset = order[i]
		printf "%s\t%s\t%s\n", dataset, machine[dataset], human[dataset]
	}
}'

# Group both recursive ZFS views and emit the merged dataset table in one AWK
# process. This is deliberately POSIX AWK: FILENAME and the untouched ARGV
# filename operands replace non-portable ARGIND/nextfile features. Reading the
# paths from ARGV also avoids AWK -v backslash decoding changing valid temp
# paths before comparison with FILENAME.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_GROUP_MERGE_RECURSIVE_PROPERTY_TREES_AWK='
BEGIN {
	filter_file = ARGV[1]
	machine_file = ARGV[2]
	human_file = ARGV[3]
	wanted_sentinel = "\034"
}
function encode_value(value) {
	gsub(/%/, "%25", value)
	gsub(/,/, "%2C", value)
	gsub(/=/, "%3D", value)
	gsub(/;/, "%3B", value)
	gsub(/\t/, "%09", value)
	gsub(/\r/, "%0D", value)
	gsub(/\n/, "%0A", value)
	return value
}
function valid_property_name(name) {
	return name ~ /^[A-Za-z0-9_.:@-][A-Za-z0-9_.:@-]*$/
}
function valid_source(source) {
	return source == "-" ||
		source == "local" ||
		source == "default" ||
		source == "temporary" ||
		source == "received" ||
		source == "inherited" ||
		source == "none" ||
		source ~ /^inherited from [^	]+$/
}
function append_grouped_record(view, dataset, line) {
	if (!(dataset in machine_grouped) && !(dataset in human_grouped))
		dataset_order[++dataset_count] = dataset
	if (view == "machine") {
		if (machine_grouped[dataset] != "")
			machine_grouped[dataset] = machine_grouped[dataset] "," line
		else
			machine_grouped[dataset] = line
		return
	}
	if (human_grouped[dataset] != "")
		human_grouped[dataset] = human_grouped[dataset] "," line
	else
		human_grouped[dataset] = line
}
function cache_current_input(i) {
	current_record = $0
	current_field_count = NF
	current_dataset = $1
	current_property_name = $2
	current_property_source = $NF
	current_record_complete = current_field_count >= 4 && valid_source(current_property_source)
	current_value = $3
	for (i = 4; i < current_field_count; i++)
		current_value = current_value "\t" $i
}
function cache_current_record(record, fields, field_count, i) {
	current_field_count = split(record, fields, "[	]")
	current_dataset = fields[1]
	current_property_name = fields[2]
	current_property_source = fields[current_field_count]
	current_record_complete = current_field_count >= 4 && valid_source(current_property_source)
	current_value = fields[3]
	for (i = 4; i < current_field_count; i++)
		current_value = current_value "\t" fields[i]
}
function flush_active_record(line) {
	if (current_record == "")
		return
	if (current_field_count < 4 || current_dataset == "" ||
		!valid_property_name(current_property_name) || !valid_source(current_property_source)) {
		parse_failed = 1
	} else if ((current_dataset in machine_grouped) || (current_dataset in human_grouped)) {
		# Reuse the machine table for the filter membership marker. A grouped
		# payload starts with a validated property name, so this control-byte
		# sentinel cannot collide with a real value.
		if (machine_grouped[current_dataset] == wanted_sentinel)
			delete machine_grouped[current_dataset]
		line = current_property_name "=" encode_value(current_value) "=" current_property_source
		append_grouped_record(active_view, current_dataset, line)
	}
	current_record = ""
	current_field_count = 0
	current_record_complete = 0
}
FILENAME == filter_file {
	if ($0 != "" && !($0 in machine_grouped))
		machine_grouped[$0] = wanted_sentinel
	next
}
{
	view = ""
	if (FILENAME == machine_file)
		view = "machine"
	else if (FILENAME == human_file)
		view = "human"
	else {
		parse_failed = 1
		next
	}
	if (active_view != "" && active_view != view)
		flush_active_record()
	active_view = view
	if (current_record == "") {
		cache_current_input()
		next
	}
	if (current_record_complete && NF >= 3 && $1 != "" && valid_property_name($2)) {
		flush_active_record()
		cache_current_input()
		next
	}
	current_record = current_record "\n" $0
	cache_current_record(current_record)
}
END {
	flush_active_record()
	if (parse_failed)
		exit 1
	for (i = 1; i <= dataset_count; i++) {
		dataset = dataset_order[i]
		printf "%s\t%s\t%s\n", dataset, machine_grouped[dataset], human_grouped[dataset]
	}
}'

# Purpose: Group the recursive property tree by dataset into the shape later
# helpers expect.
# Usage: Called during property prefetch before the grouped result is merged
# into the in-memory property tables.
zxfer_group_recursive_property_tree_by_dataset() {
	l_dataset_filter_file=$1
	l_property_tree_file=$2

	"${g_cmd_awk:-awk}" -F '	' "$ZXFER_GROUP_RECURSIVE_PROPERTY_TREE_AWK" \
		"$l_dataset_filter_file" "$l_property_tree_file"
}

# Purpose: Group and merge both recursive property views in one POSIX AWK
# process while retaining the legacy byte format and dataset ordering.
# Usage: Called after both recursive reads succeed; writes the complete merged
# table to stdout and returns non-zero without publishing partial state when
# either view is malformed.
zxfer_group_and_merge_recursive_property_trees_by_dataset() {
	l_grouped_property_filter_file=$1
	l_grouped_property_machine_file=$2
	l_grouped_property_human_file=$3

	"${g_cmd_awk:-awk}" -F '	' \
		"$ZXFER_GROUP_MERGE_RECURSIVE_PROPERTY_TREES_AWK" \
		"$l_grouped_property_filter_file" \
		"$l_grouped_property_machine_file" \
		"$l_grouped_property_human_file"
}

# Purpose: Mark one side's recursive property prefetch failed, clean up any
# active staging files, and return the original failure status.
# Usage: Called from every zxfer_prefetch_recursive_normalized_properties
# failure path (with an empty stage-file list before staging exists) so
# cleanup and state marking stay in one module-owned path.
zxfer_abort_recursive_property_prefetch() {
	l_side=$1
	l_stage_files=$2
	l_status=$3

	if [ -n "$l_stage_files" ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_stage_files"
	fi
	case "$l_side" in
	source)
		g_zxfer_source_property_tree_prefetch_state=2
		;;
	destination)
		g_zxfer_destination_property_tree_prefetch_state=2
		;;
	esac
	return "$l_status"
}

# Purpose: Allocate and name the complete recursive-property prefetch staging
# group in one current-shell step.
# Usage: Called before recursive property probes so every later failure path
# can clean the same ordered artifact list without duplicating its unpacking.
# Side effects: Publishes stage paths through l_prefetch_stage_files and the
# l_*_file variables consumed by the prefetch orchestration.
zxfer_allocate_recursive_property_prefetch_stage_files() {
	l_prefetch_stage_status=0
	zxfer_create_temp_file_group 5 >/dev/null || l_prefetch_stage_status=$?
	[ "$l_prefetch_stage_status" -eq 0 ] || return "$l_prefetch_stage_status"

	l_prefetch_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dataset_filter_file
		IFS= read -r l_machine_tree_file
		IFS= read -r l_human_tree_file
		IFS= read -r l_combined_grouped_file
		IFS= read -r l_tree_err_file
	} <<-EOF
		$l_prefetch_stage_files
	EOF
}

# Purpose: Capture machine and human recursive property trees with the same
# live-probe order used by per-dataset normalization.
# Usage: Called after prefetch staging is ready; returns the first exact probe
# status so the caller can mark the side failed and clean every artifact.
zxfer_capture_recursive_property_prefetch_trees() {
	l_prefetch_zfs_cmd=$1
	l_prefetch_root_dataset=$2
	l_prefetch_profile_counter=$3
	l_prefetch_machine_tree_file=$4
	l_prefetch_human_tree_file=$5
	l_prefetch_tree_err_file=$6

	zxfer_profile_increment_counter "$l_prefetch_profile_counter"
	zxfer_run_zfs_cmd_for_spec "$l_prefetch_zfs_cmd" get -r -Hpo name,property,value,source all "$l_prefetch_root_dataset" \
		>"$l_prefetch_machine_tree_file" 2>"$l_prefetch_tree_err_file" || return "$?"
	zxfer_run_zfs_cmd_for_spec "$l_prefetch_zfs_cmd" get -r -Ho name,property,value,source all "$l_prefetch_root_dataset" \
		>"$l_prefetch_human_tree_file" 2>"$l_prefetch_tree_err_file"
}

# Purpose: Group and merge recursive machine/human property captures through
# the characterized normalization stages.
# Usage: Called after both recursive reads succeed; returns the first grouping
# or merge status unchanged to the prefetch failure path.
zxfer_group_recursive_property_prefetch_trees() {
	l_prefetch_filter_file=$1
	l_prefetch_machine_tree_file=$2
	l_prefetch_human_tree_file=$3
	l_prefetch_combined_grouped_file=$4

	zxfer_group_and_merge_recursive_property_trees_by_dataset \
		"$l_prefetch_filter_file" \
		"$l_prefetch_machine_tree_file" \
		"$l_prefetch_human_tree_file" \
		>"$l_prefetch_combined_grouped_file"
}

# Purpose: Decode a merged recursive property capture into the in-memory table
# payload format used by exact dataset lookups.
# Usage: Called before publication so malformed/empty grouped rows remain
# skipped exactly as they are for the live normalization path.
# Side effects: Publishes the decoded block through l_prefetched_table.
zxfer_load_recursive_property_prefetch_table() {
	l_prefetch_combined_grouped_file=$1
	l_prefetch_tab='	'
	l_prefetch_nl='
'
	l_prefetched_table=""

	zxfer_read_runtime_artifact_file "$l_prefetch_combined_grouped_file" >/dev/null ||
		return "$?"
	while IFS= read -r l_prefetch_grouped_line || [ -n "$l_prefetch_grouped_line" ]; do
		[ -n "$l_prefetch_grouped_line" ] || continue
		l_prefetch_dataset=${l_prefetch_grouped_line%%"$l_prefetch_tab"*}
		l_prefetch_grouped_rest=${l_prefetch_grouped_line#*"$l_prefetch_tab"}
		case "$l_prefetch_grouped_rest" in
		*"$l_prefetch_tab"*)
			l_prefetch_machine_pvs=${l_prefetch_grouped_rest%%"$l_prefetch_tab"*}
			l_prefetch_human_pvs=${l_prefetch_grouped_rest#*"$l_prefetch_tab"}
			;;
		*)
			continue
			;;
		esac
		[ -n "$l_prefetch_dataset" ] || continue
		[ -n "$l_prefetch_machine_pvs" ] || continue
		[ -n "$l_prefetch_human_pvs" ] || continue
		zxfer_resolve_human_vars "$l_prefetch_machine_pvs" "$l_prefetch_human_pvs"
		[ -n "$human_results" ] || continue
		if [ -n "$l_prefetched_table" ]; then
			l_prefetched_table=$l_prefetched_table$l_prefetch_nl$l_prefetch_dataset$l_prefetch_tab$human_results
		else
			l_prefetched_table=$l_prefetch_dataset$l_prefetch_tab$human_results
		fi
	done <<EOF
$g_zxfer_runtime_artifact_read_result
EOF
}

# Purpose: Publish one freshly decoded recursive prefetch block ahead of older
# live rows and mark that side complete.
# Usage: Called only after capture, grouping, and readback all succeed so failed
# prefetches never expose a partial table.
zxfer_publish_recursive_property_prefetch_table() {
	l_prefetch_publish_side=$1
	l_prefetch_publish_table=$2
	l_prefetch_publish_nl='
'

	case "$l_prefetch_publish_side" in
	source)
		if [ -n "$l_prefetch_publish_table" ] && [ -n "${g_zxfer_source_property_table:-}" ]; then
			g_zxfer_source_property_table=$l_prefetch_publish_table$l_prefetch_publish_nl$g_zxfer_source_property_table
		elif [ -n "$l_prefetch_publish_table" ]; then
			g_zxfer_source_property_table=$l_prefetch_publish_table
		fi
		g_zxfer_source_property_tree_prefetch_state=1
		;;
	destination)
		if [ -n "$l_prefetch_publish_table" ] && [ -n "${g_zxfer_destination_property_table:-}" ]; then
			g_zxfer_destination_property_table=$l_prefetch_publish_table$l_prefetch_publish_nl$g_zxfer_destination_property_table
		elif [ -n "$l_prefetch_publish_table" ]; then
			g_zxfer_destination_property_table=$l_prefetch_publish_table
		fi
		g_zxfer_destination_property_tree_prefetch_state=1
		;;
	esac
}

# Purpose: Prefetch the recursive normalized properties for one side into its
# in-memory property table.
# Usage: Called during normalized property lookup before a loop would
# otherwise repeat the same live probe for every dataset in the tree.
zxfer_prefetch_recursive_normalized_properties() {
	l_prefetch_recursive_normalized_properties_side=$1

	case "$l_prefetch_recursive_normalized_properties_side" in
	source)
		l_prefetch_state=${g_zxfer_source_property_tree_prefetch_state:-0}
		l_root_dataset=${g_zxfer_source_property_tree_prefetch_root:-}
		l_zfs_cmd=${g_zxfer_source_property_tree_prefetch_zfs_cmd:-}
		l_profile_counter=g_zxfer_profile_normalized_property_reads_source
		;;
	destination)
		l_prefetch_state=${g_zxfer_destination_property_tree_prefetch_state:-0}
		l_root_dataset=${g_zxfer_destination_property_tree_prefetch_root:-}
		l_zfs_cmd=${g_zxfer_destination_property_tree_prefetch_zfs_cmd:-}
		l_profile_counter=g_zxfer_profile_normalized_property_reads_destination
		;;
	*)
		return 1
		;;
	esac

	case "$l_prefetch_state" in
	1)
		return 0
		;;
	2)
		return 1
		;;
	esac

	l_dataset_list_status=0
	l_dataset_list=$(zxfer_get_property_tree_prefetch_dataset_list "$l_prefetch_recursive_normalized_properties_side") ||
		l_dataset_list_status=$?
	if [ "$l_dataset_list_status" -ne 0 ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "" "$l_dataset_list_status"
		return "$?"
	fi

	if [ -z "$l_root_dataset" ] || [ -z "$l_zfs_cmd" ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "" 1
		return "$?"
	fi

	l_stage_status=0
	zxfer_allocate_recursive_property_prefetch_stage_files || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "" "$l_stage_status"
		return "$?"
	fi

	# shellcheck disable=SC2016
	printf '%s\n' "$l_dataset_list" | grep -v '^[[:space:]]*$' |
		"${g_cmd_awk:-awk}" '!seen[$0]++' >"$l_dataset_filter_file"
	if [ ! -s "$l_dataset_filter_file" ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "$l_prefetch_stage_files" 1
		return "$?"
	fi

	l_tree_status=0
	zxfer_capture_recursive_property_prefetch_trees "$l_zfs_cmd" "$l_root_dataset" \
		"$l_profile_counter" "$l_machine_tree_file" "$l_human_tree_file" "$l_tree_err_file" ||
		l_tree_status=$?
	if [ "$l_tree_status" -ne 0 ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "$l_prefetch_stage_files" "$l_tree_status"
		return "$?"
	fi

	l_group_status=0
	zxfer_group_recursive_property_prefetch_trees "$l_dataset_filter_file" \
		"$l_machine_tree_file" "$l_human_tree_file" \
		"$l_combined_grouped_file" ||
		l_group_status=$?
	if [ "$l_group_status" -ne 0 ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "$l_prefetch_stage_files" "$l_group_status"
		return "$?"
	fi

	l_grouped_read_status=0
	zxfer_load_recursive_property_prefetch_table "$l_combined_grouped_file" ||
		l_grouped_read_status=$?
	if [ "$l_grouped_read_status" -ne 0 ]; then
		zxfer_abort_recursive_property_prefetch "$l_prefetch_recursive_normalized_properties_side" "$l_prefetch_stage_files" "$l_grouped_read_status"
		return "$?"
	fi

	zxfer_cleanup_runtime_artifact_path_list "$l_prefetch_stage_files"

	# Fresh prefetch rows precede earlier live reads, so first-match lookup keeps
	# the new tree authoritative without invalidating the memo separately.
	zxfer_publish_recursive_property_prefetch_table "$l_prefetch_recursive_normalized_properties_side" "$l_prefetched_table"
}

# Purpose: Run the optional prefetch recursive normalized properties step only
# when the current state requires it.
# Usage: Called during normalized property lookup to keep the optional branch
# in one place instead of scattering the condition across callers. Succeeds
# only when prefetch materialized the requested dataset's table row.
zxfer_maybe_prefetch_recursive_normalized_properties() {
	l_dataset=$1
	l_maybe_prefetch_recursive_normalized_properties_zfs_cmd=$2
	l_lookup_side=$3

	case "$l_lookup_side" in
	source)
		[ -n "${g_zxfer_source_property_tree_prefetch_root:-}" ] || return 1
		[ "${g_zxfer_source_property_tree_prefetch_zfs_cmd:-}" = "$l_maybe_prefetch_recursive_normalized_properties_zfs_cmd" ] || return 1
		case "
${g_recursive_source_dataset_list:-}
$(printf '%s\n' "${g_recursive_source_list:-}" | tr ' ' '\n')" in
		*"
$l_dataset
"*) ;;
		*) return 1 ;;
		esac
		;;
	destination)
		[ -n "${g_zxfer_destination_property_tree_prefetch_root:-}" ] || return 1
		[ "${g_zxfer_destination_property_tree_prefetch_zfs_cmd:-}" = "$l_maybe_prefetch_recursive_normalized_properties_zfs_cmd" ] || return 1
		case "
${g_recursive_dest_list:-}
" in
		*"
$l_dataset
"*) ;;
		*) return 1 ;;
		esac
		;;
	*)
		return 1
		;;
	esac

	zxfer_prefetch_recursive_normalized_properties "$l_lookup_side" || return "$?"

	zxfer_property_table_find_dataset "$l_lookup_side" "$l_dataset"
}

# Purpose: Load the normalized dataset properties from the in-memory property
# tables or a live zfs probe.
# Usage: Called during property collection when later helpers need a checked
# in-memory copy of the dataset's normalized property list.
zxfer_load_normalized_dataset_properties() {
	l_load_normalized_dataset_properties_dataset=$1
	l_load_normalized_dataset_properties_zfs_cmd=$2
	l_load_normalized_dataset_properties_lookup_side=${3:-other}

	if [ -z "$l_load_normalized_dataset_properties_zfs_cmd" ]; then
		l_load_normalized_dataset_properties_zfs_cmd=$g_LZFS
	fi

	g_zxfer_normalized_dataset_properties=""
	g_zxfer_normalized_dataset_properties_cache_hit=0

	case "$l_load_normalized_dataset_properties_lookup_side" in
	source | destination)
		if [ "${g_zxfer_property_table_memo_side:-}" = "$l_load_normalized_dataset_properties_lookup_side" ] &&
			[ "${g_zxfer_property_table_memo_dataset:-}" = "$l_load_normalized_dataset_properties_dataset" ] &&
			[ -n "${g_zxfer_property_table_memo_payload:-}" ]; then
			g_zxfer_normalized_dataset_properties=$g_zxfer_property_table_memo_payload
			g_zxfer_normalized_dataset_properties_cache_hit=1
			return 0
		fi
		if zxfer_property_table_find_dataset "$l_load_normalized_dataset_properties_lookup_side" "$l_load_normalized_dataset_properties_dataset"; then
			g_zxfer_normalized_dataset_properties=$g_zxfer_property_table_lookup_result
			g_zxfer_normalized_dataset_properties_cache_hit=1
			g_zxfer_property_table_memo_side=$l_load_normalized_dataset_properties_lookup_side
			g_zxfer_property_table_memo_dataset=$l_load_normalized_dataset_properties_dataset
			g_zxfer_property_table_memo_payload=$g_zxfer_property_table_lookup_result
			return 0
		fi
		if zxfer_maybe_prefetch_recursive_normalized_properties "$l_load_normalized_dataset_properties_dataset" "$l_load_normalized_dataset_properties_zfs_cmd" "$l_load_normalized_dataset_properties_lookup_side" >/dev/null 2>&1 &&
			zxfer_property_table_find_dataset "$l_load_normalized_dataset_properties_lookup_side" "$l_load_normalized_dataset_properties_dataset"; then
			g_zxfer_normalized_dataset_properties=$g_zxfer_property_table_lookup_result
			g_zxfer_normalized_dataset_properties_cache_hit=1
			g_zxfer_property_table_memo_side=$l_load_normalized_dataset_properties_lookup_side
			g_zxfer_property_table_memo_dataset=$l_load_normalized_dataset_properties_dataset
			g_zxfer_property_table_memo_payload=$g_zxfer_property_table_lookup_result
			return 0
		fi
		;;
	esac

	case "$l_load_normalized_dataset_properties_lookup_side" in
	source)
		zxfer_profile_increment_counter g_zxfer_profile_normalized_property_reads_source
		;;
	destination)
		zxfer_profile_increment_counter g_zxfer_profile_normalized_property_reads_destination
		;;
	*)
		zxfer_profile_increment_counter g_zxfer_profile_normalized_property_reads_other
		;;
	esac

	l_machine_status=0
	l_machine_pvs=$(zxfer_run_zfs_cmd_for_spec "$l_load_normalized_dataset_properties_zfs_cmd" get -Hpo property,value,source all "$l_load_normalized_dataset_properties_dataset" 2>&1) ||
		l_machine_status=$?
	if [ "$l_machine_status" -ne 0 ]; then
		printf '%s\n' "$l_machine_pvs"
		return "$l_machine_status"
	fi
	zxfer_capture_serialized_property_records "$l_machine_pvs" || return "$?"
	l_machine_pvs=$g_zxfer_serialized_property_records_result
	l_human_status=0
	l_human_pvs=$(zxfer_run_zfs_cmd_for_spec "$l_load_normalized_dataset_properties_zfs_cmd" get -Ho property,value,source all "$l_load_normalized_dataset_properties_dataset" 2>&1) ||
		l_human_status=$?
	if [ "$l_human_status" -ne 0 ]; then
		printf '%s\n' "$l_human_pvs"
		return "$l_human_status"
	fi
	zxfer_capture_serialized_property_records "$l_human_pvs" || return "$?"
	l_human_pvs=$g_zxfer_serialized_property_records_result
	zxfer_resolve_human_vars "$l_machine_pvs" "$l_human_pvs"
	g_zxfer_normalized_dataset_properties=$human_results

	zxfer_property_table_append_dataset "$l_load_normalized_dataset_properties_lookup_side" "$l_load_normalized_dataset_properties_dataset" "$g_zxfer_normalized_dataset_properties"

	return 0
}

# Purpose: Return the required property probe in the form expected by later
# helpers.
# Usage: Called during required-property backfill when sibling helpers need the
# same lookup without duplicating module logic.
zxfer_get_required_property_probe() {
	l_dataset=$1
	l_required_property=$2
	l_zfs_cmd=$3
	l_lookup_side=${4:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	g_zxfer_required_properties_result=""
	g_zxfer_required_property_probe_result=""

	if zxfer_property_table_find_dataset "$l_lookup_side" "$l_dataset" "$l_required_property"; then
		g_zxfer_required_property_probe_result=$g_zxfer_property_table_lookup_result
		return 0
	fi

	zxfer_profile_increment_counter g_zxfer_profile_required_property_backfill_gets
	l_explicit_probe_status=0
	l_explicit_probe_output=$(zxfer_run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source "$l_required_property" "$l_dataset" 2>&1) ||
		l_explicit_probe_status=$?
	if [ "$l_explicit_probe_status" -eq 0 ]; then
		l_status=0
		zxfer_capture_serialized_property_records "$l_explicit_probe_output" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			if [ "${g_zxfer_serialized_property_records_parse_failed:-0}" -eq 1 ]; then
				case "$l_explicit_probe_output" in
				"$l_required_property	"*)
					g_zxfer_required_properties_result="Failed to parse required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
					printf '%s\n' "$g_zxfer_required_properties_result"
					return 1
					;;
				esac
			fi
			return "$l_status"
		fi
		l_explicit_property=$g_zxfer_serialized_property_records_result
		case "$l_explicit_property" in
		"$l_required_property"=*=*) ;;
		*)
			g_zxfer_required_properties_result="Failed to parse required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
			printf '%s\n' "$g_zxfer_required_properties_result"
			return 1
			;;
		esac
		g_zxfer_required_property_probe_result=$l_explicit_property
	else
		case "$l_explicit_probe_output" in
		*"does not apply"* | *"invalid property"* | *"no such property"* | *"not supported"*)
			g_zxfer_required_property_probe_result=$ZXFER_REQUIRED_PROPERTY_UNSUPPORTED_SENTINEL
			;;
		*)
			g_zxfer_required_properties_result="Failed to retrieve required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
			printf '%s\n' "$g_zxfer_required_properties_result"
			return "$l_explicit_probe_status"
			;;
		esac
	fi

	zxfer_property_table_append_dataset "$l_lookup_side" "$l_dataset" \
		"$g_zxfer_required_property_probe_result" "$l_required_property"

	return 0
}

# Purpose: Populate the required properties present from the active source
# data.
# Usage: Called during required-property backfill when the surrounding flow
# needs a fully expanded in-memory view.
zxfer_populate_required_properties_present() {
	l_populate_required_properties_present_dataset=$1
	l_property_list=$2
	l_populate_required_properties_present_zfs_cmd=$3
	l_required_properties=$4
	l_populate_required_properties_present_lookup_side=${5:-other}

	if [ -z "$l_populate_required_properties_present_zfs_cmd" ]; then
		l_populate_required_properties_present_zfs_cmd=$g_LZFS
	fi

	g_zxfer_required_properties_result=""
	l_result=$l_property_list
	l_required_properties_remaining=$l_required_properties
	while [ -n "$l_required_properties_remaining" ]; do
		case "$l_required_properties_remaining" in
		*,*)
			l_populate_required_properties_present_required_property=${l_required_properties_remaining%%,*}
			l_required_properties_remaining=${l_required_properties_remaining#*,}
			;;
		*)
			l_populate_required_properties_present_required_property=$l_required_properties_remaining
			l_required_properties_remaining=""
			;;
		esac
		[ -n "$l_populate_required_properties_present_required_property" ] || continue
		l_found_property=0
		l_required_property_lines_remaining=$l_result
		while [ -n "$l_required_property_lines_remaining" ]; do
			case "$l_required_property_lines_remaining" in
			*,*)
				l_property_line=${l_required_property_lines_remaining%%,*}
				l_required_property_lines_remaining=${l_required_property_lines_remaining#*,}
				;;
			*)
				l_property_line=$l_required_property_lines_remaining
				l_required_property_lines_remaining=""
				;;
			esac
			l_property_name=${l_property_line%%=*}
			if [ "$l_property_name" = "$l_populate_required_properties_present_required_property" ]; then
				l_found_property=1
				break
			fi
		done

		[ "$l_found_property" -eq 0 ] || continue

		l_populate_required_properties_present_status=0
		zxfer_get_required_property_probe "$l_populate_required_properties_present_dataset" "$l_populate_required_properties_present_required_property" "$l_populate_required_properties_present_zfs_cmd" "$l_populate_required_properties_present_lookup_side" ||
			l_populate_required_properties_present_status=$?
		if [ "$l_populate_required_properties_present_status" -ne 0 ]; then
			return "$l_populate_required_properties_present_status"
		fi

		case "$g_zxfer_required_property_probe_result" in
		"" | "$ZXFER_REQUIRED_PROPERTY_UNSUPPORTED_SENTINEL")
			continue
			;;
		esac

		if [ -n "$l_result" ]; then
			l_result="$l_result,$g_zxfer_required_property_probe_result"
		else
			l_result=$g_zxfer_required_property_probe_result
		fi
	done

	g_zxfer_required_properties_result=$l_result
	return 0
}

# Purpose: Load the destination props from the in-memory tables or a live
# probe.
# Usage: Called during property collection when later helpers need a checked
# in-memory copy of the destination's normalized property list.
zxfer_load_destination_props() {
	l_load_destination_props_dataset=$1
	l_load_destination_props_zfs_cmd=$2

	if [ -z "$l_load_destination_props_zfs_cmd" ]; then
		l_load_destination_props_zfs_cmd=$g_RZFS
	fi

	g_zxfer_destination_pvs_raw=""
	zxfer_load_normalized_dataset_properties "$l_load_destination_props_dataset" "$l_load_destination_props_zfs_cmd" destination ||
		return "$?"

	g_zxfer_destination_pvs_raw=$g_zxfer_normalized_dataset_properties
	return 0
}

# Purpose: Resolve the effective human vars that zxfer should use.
# Usage: Called during property prefetch and normalized property lookup after
# both machine and human property reads exist for one dataset.
#
# Normalize the list of properties to set by using a mix of human-readable and
# machine-readable values
zxfer_resolve_human_vars() {
	l_machine_vars=$1
	l_human_vars=$2

	l_human_results=
	l_human_vars_remaining=$l_human_vars
	while [ -n "$l_human_vars_remaining" ]; do
		case "$l_human_vars_remaining" in
		*,*)
			l_human_var=${l_human_vars_remaining%%,*}
			l_human_vars_remaining=${l_human_vars_remaining#*,}
			;;
		*)
			l_human_var=$l_human_vars_remaining
			l_human_vars_remaining=""
			;;
		esac
		[ -n "$l_human_var" ] || continue
		l_human_prop=${l_human_var%%=*}
		l_machine_vars_remaining=$l_machine_vars
		while [ -n "$l_machine_vars_remaining" ]; do
			case "$l_machine_vars_remaining" in
			*,*)
				l_machine_var=${l_machine_vars_remaining%%,*}
				l_machine_vars_remaining=${l_machine_vars_remaining#*,}
				;;
			*)
				l_machine_var=$l_machine_vars_remaining
				l_machine_vars_remaining=""
				;;
			esac
			[ -n "$l_machine_var" ] || continue
			l_machine_prop=${l_machine_var%%=*}
			if [ "$l_human_prop" = "$l_machine_prop" ]; then
				l_machine_rest=${l_machine_var#*=}
				l_machine_value=${l_machine_rest%%=*}
				l_machine_source=${l_machine_rest#*=}
				l_human_rest=${l_human_var#*=}
				l_human_value=${l_human_rest%%=*}
				if [ "$l_human_value" = "none" ]; then
					l_machine_value=$l_human_value
				fi
				l_human_results="${l_human_results}$l_machine_prop=$l_machine_value=$l_machine_source,"
			fi
		done
	done
	l_human_results=${l_human_results%,}
	human_results=$l_human_results
}

# Purpose: Return the normalized dataset properties in the form expected by
# later helpers.
# Usage: Called during property collection when sibling helpers need the same
# lookup without duplicating module logic.
#
# Retrieve the normalized property/value/source list for a dataset while
# handling locales that require both machine (-Hp) and human (-H) parsing.
# $1: dataset to query
# $2: zfs command to execute (defaults to $g_LZFS)
# $3: optional lookup side label (source/destination/other) for profiling
zxfer_get_normalized_dataset_properties() {
	zxfer_load_normalized_dataset_properties "$1" "$2" "$3" || return "$?"
	printf '%s\n' "$g_zxfer_normalized_dataset_properties"
}

# Purpose: Ensure the required properties present exists and is ready before
# the flow continues.
# Usage: Called during required-property backfill before later helpers assume
# the expanded property list is available.
#
# Some OpenZFS implementations do not include every creation-time property in
# `zfs get all` output even though the property is queryable directly. Append
# any missing required properties so later diffing can still enforce
# creation-time mismatch rules consistently.
# $1: dataset name
# $2: existing property list
# $3: zfs command used to query properties
# $4: comma-separated list of required property names
zxfer_ensure_required_properties_present() {
	zxfer_populate_required_properties_present "$1" "$2" "$3" "$4" "$5" || return "$?"
	printf '%s\n' "$g_zxfer_required_properties_result"
}

# Purpose: Collect the destination props into the module-owned format used by
# later steps.
# Usage: Called during property collection before reconciliation or apply
# logic consumes the combined result.
#
# Collect destination properties via the remote/local zfs command.
# $1: dataset name
# $2: command used to query properties (defaults to $g_RZFS)
zxfer_collect_destination_props() {
	zxfer_load_destination_props "$1" "$2" || return "$?"
	printf '%s\n' "$g_zxfer_destination_pvs_raw"
}
