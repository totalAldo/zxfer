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
# PROPERTY NORMALIZATION / CACHE / PREFETCH HELPERS
################################################################################

# Module contract:
# owns globals: per-iteration property cache paths, recursive prefetch state, and per-call lookup/result channels such as g_zxfer_normalized_dataset_properties, g_zxfer_required_properties_result, g_zxfer_required_property_probe_result, g_zxfer_serialized_property_records_result, g_zxfer_destination_pvs_raw, g_zxfer_property_cache_path, g_zxfer_property_cache_key, and g_zxfer_property_cache_read_result.
# reads globals: g_LZFS/g_RZFS, g_cmd_awk, temp-root helpers, and current dataset context.
# mutates caches: on-disk property cache entries and recursive prefetch state.
# returns via stdout: serialized property records, decoded assignments, and normalized property payloads.

ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED="property-normalized"
ZXFER_PROPERTY_CACHE_OBJECT_KIND_REQUIRED="property-required"

# Purpose: Reset the property lookup results so the next property-cache pass
# starts from a clean state.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_property_lookup_results() {
	g_zxfer_normalized_dataset_properties=""
	g_zxfer_normalized_dataset_properties_cache_hit=0
	g_zxfer_required_properties_result=""
	g_zxfer_required_property_probe_result=""
	g_zxfer_serialized_property_records_result=""
	g_zxfer_destination_pvs_raw=""
	g_zxfer_property_cache_read_result=""
}

# Purpose: Reset the property cache path state so the next property-cache pass
# starts from a clean state.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_property_cache_path_state() {
	g_zxfer_property_cache_path=""
	g_zxfer_property_cache_key=""
}

# Purpose: Serialize property records from stdin into zxfer's stable on-disk
# cache format.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before property payloads are written to cache files or staged
# artifacts.
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
	return value
}
NF >= 3 {
	output = append_csv(output, $1 "=" encode_value($2) "=" $3)
}
END {
	print output
}'
}

# Purpose: Capture the serialized property records into staged state or module
# globals for later use.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when later helpers need a checked snapshot of command output
# or computed state.
zxfer_capture_serialized_property_records() {
	l_property_records=$1

	g_zxfer_serialized_property_records_result=""

	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_serialized_output_file=$g_zxfer_temp_file_result

	l_serialize_status=0
	zxfer_serialize_property_records_from_stdin >"$l_serialized_output_file" <<EOF || l_serialize_status=$?
$l_property_records
EOF
	if [ "$l_serialize_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_serialized_output_file"
		return "$l_serialize_status"
	fi

	if zxfer_read_runtime_artifact_file "$l_serialized_output_file" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_serialized_output_file"
		return "$l_read_status"
	fi
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

# Purpose: Emit the decoded property assignments in the operator-facing format
# owned by this module.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_emit_decoded_property_assignments() {
	l_property_list=$1

	"${g_cmd_awk:-awk}" -v property_list="$l_property_list" '
function decode_value(value) {
	gsub(/%0D/, "\r", value)
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
		split(property_items[i], property_fields, "=")
		property_name = property_fields[1]
		property_value = substr(property_items[i], length(property_name) + 2)
		print property_name "=" decode_value(property_value)
	}
}'
}

# Purpose: Decode a serialized property list into the operator-facing form used
# by reports and debugging output.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when cached property data needs to be displayed instead of
# reapplied directly.
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

# Purpose: Reset the property iteration caches so the next property-cache pass
# starts from a clean state.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_property_iteration_caches() {
	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_property_cache_dir"
	fi

	zxfer_reset_property_lookup_results
	zxfer_reset_property_cache_path_state
	g_zxfer_property_cache_dir=""
	g_zxfer_property_cache_unavailable=0
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Refresh the property tree prefetch context from the current
# configuration and runtime state.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup after inputs change and downstream helpers need the derived
# value rebuilt.
zxfer_refresh_property_tree_prefetch_context() {
	if [ "${g_option_R_recursive:-}" = "" ]; then
		g_zxfer_source_property_tree_prefetch_root=""
		g_zxfer_source_property_tree_prefetch_zfs_cmd=""
		g_zxfer_source_property_tree_prefetch_state=0
		g_zxfer_destination_property_tree_prefetch_root=""
		g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
		g_zxfer_destination_property_tree_prefetch_state=0
		return
	fi

	if [ "${g_option_P_transfer_property:-0}" -ne 1 ] &&
		[ -z "${g_option_o_override_property:-}" ]; then
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

# Purpose: Ensure the property cache directory exists and is ready before the
# flow continues.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before later helpers assume the resource or cache is
# available.
zxfer_ensure_property_cache_dir() {
	if [ "${g_zxfer_property_cache_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		return 0
	fi

	l_cache_dir_status=0
	zxfer_create_runtime_artifact_dir "zxfer-property-cache" >/dev/null || l_cache_dir_status=$?
	if [ "$l_cache_dir_status" -ne 0 ]; then
		g_zxfer_property_cache_unavailable=1
		g_zxfer_property_cache_dir=""
		return "$l_cache_dir_status"
	fi
	g_zxfer_property_cache_dir=$g_zxfer_runtime_artifact_path_result

	return 0
}

# Purpose: Disable the property iteration cache in the current module context.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when a fallback or safety decision needs to turn off an
# optimization or cache path.
zxfer_disable_property_iteration_cache() {
	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_property_cache_dir"
	fi

	g_zxfer_property_cache_dir=""
	g_zxfer_property_cache_unavailable=1
}

# Purpose: Encode a cache key component so dataset and property names can be
# stored safely in property-cache paths.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before cache paths are derived from live dataset or property
# identifiers.
zxfer_property_cache_encode_key() {
	l_key=$1
	g_zxfer_property_cache_key=""
	l_key_hex=$(printf '%s' "$l_key" | LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	g_zxfer_property_cache_key=$l_key_hex
	printf '%s\n' "$g_zxfer_property_cache_key"
}

# Purpose: Return the dataset-scoped property-cache path for one dataset key.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before cache reads or writes address the dataset-level cache
# file.
zxfer_property_cache_dataset_path() {
	l_bucket=$1
	l_side=$2
	l_dataset=$3
	g_zxfer_property_cache_path=""

	l_status=0
	zxfer_ensure_property_cache_dir || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	l_status=0
	l_dataset_key=$(zxfer_property_cache_encode_key "$l_dataset") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	g_zxfer_property_cache_path=$g_zxfer_property_cache_dir/$l_bucket/$l_side/$l_dataset_key
	printf '%s\n' "$g_zxfer_property_cache_path"
}

# Purpose: Return the property-scoped cache path for one dataset and property
# key pair.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before cache reads or writes address one normalized property
# record.
zxfer_property_cache_property_path() {
	l_bucket=$1
	l_side=$2
	l_dataset=$3
	l_property=$4
	g_zxfer_property_cache_path=""

	l_status=0
	zxfer_ensure_property_cache_dir || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	l_status=0
	l_dataset_key=$(zxfer_property_cache_encode_key "$l_dataset") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	l_property_key=$(zxfer_property_cache_encode_key "$l_property") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	g_zxfer_property_cache_path=$g_zxfer_property_cache_dir/$l_bucket/$l_side/$l_dataset_key/$l_property_key
	printf '%s\n' "$g_zxfer_property_cache_path"
}

# Purpose: Store one normalized property value in the property cache through
# the module's checked publish path.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup after zxfer has a validated property result worth caching for
# reuse.
zxfer_property_cache_store() {
	l_cache_path=$1
	l_cache_value=$2
	l_cache_kind=${3:-}

	if [ -z "$l_cache_kind" ]; then
		case "$l_cache_path" in
		*/normalized/*)
			l_cache_kind=$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED
			;;
		*/required/*)
			l_cache_kind=$ZXFER_PROPERTY_CACHE_OBJECT_KIND_REQUIRED
			;;
		*)
			return 1
			;;
		esac
	fi

	zxfer_write_cache_object_file_atomically "$l_cache_path" "$l_cache_kind" "" "$l_cache_value"
}

# Purpose: Read the property cache file from staged state into the current
# shell.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when later helpers need a checked reload instead of ad hoc
# file reads.
zxfer_read_property_cache_file() {
	l_cache_path=$1
	l_cache_kind=${2:-}

	g_zxfer_property_cache_read_result=""

	if [ -z "$l_cache_kind" ]; then
		case "$l_cache_path" in
		*/normalized/*)
			l_cache_kind=$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED
			;;
		*/required/*)
			l_cache_kind=$ZXFER_PROPERTY_CACHE_OBJECT_KIND_REQUIRED
			;;
		esac
	fi

	l_status=0
	zxfer_read_cache_object_file "$l_cache_path" "$l_cache_kind" >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		g_zxfer_property_cache_read_result=""
		return "$l_status"
	fi

	g_zxfer_property_cache_read_result=$g_zxfer_cache_object_payload_result
	return 0
}

# Purpose: Invalidate the dataset property cache so later helpers rebuild or
# re-probe it.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when a prior cache result is no longer safe to trust.
zxfer_invalidate_dataset_property_cache() {
	l_side=$1
	l_dataset=$2

	if [ -z "${g_zxfer_property_cache_dir:-}" ] || [ ! -d "$g_zxfer_property_cache_dir" ]; then
		return
	fi

	if ! l_dataset_key=$(zxfer_property_cache_encode_key "$l_dataset"); then
		return
	fi

	rm -f "$g_zxfer_property_cache_dir/normalized/$l_side/$l_dataset_key"
	rm -rf "$g_zxfer_property_cache_dir/required/$l_side/$l_dataset_key"
}

# Purpose: Invalidate the destination property cache so later helpers rebuild
# or re-probe it.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when a prior cache result is no longer safe to trust.
zxfer_invalidate_destination_property_cache() {
	zxfer_invalidate_dataset_property_cache destination "$1"
}

# Purpose: Reset the destination property iteration cache so the next property-
# cache pass starts from a clean state.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_destination_property_iteration_cache() {
	if [ -z "${g_zxfer_property_cache_dir:-}" ] || [ ! -d "$g_zxfer_property_cache_dir" ]; then
		g_zxfer_destination_property_tree_prefetch_state=0
		return
	fi

	rm -rf "$g_zxfer_property_cache_dir/normalized/destination"
	rm -rf "$g_zxfer_property_cache_dir/required/destination"
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Return the property tree prefetch dataset list in the form expected
# by later helpers.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when sibling helpers need the same lookup without duplicating
# module logic.
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

# Purpose: Group the recursive property tree by dataset into the shape later
# helpers expect.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before reconciliation or staging paths consume the grouped
# result.
zxfer_group_recursive_property_tree_by_dataset() {
	l_dataset_filter_file=$1
	l_property_tree_file=$2

	# shellcheck disable=SC2016
	"${g_cmd_awk:-awk}" -F '	' '
function encode_value(value) {
	gsub(/%/, "%25", value)
	gsub(/,/, "%2C", value)
	gsub(/=/, "%3D", value)
	gsub(/;/, "%3B", value)
	gsub(/\t/, "%09", value)
	gsub(/\r/, "%0D", value)
	return value
}
NR == FNR {
	if ($0 != "" && !seen_filter[$0]++)
		wanted[$0] = 1
	next
}
{
	dataset = $1
	if (!(dataset in wanted))
		next
	if (!seen_dataset[dataset]++) {
		order[++count] = dataset
	}
	line = $2 "=" encode_value($3) "=" $4
	if (grouped[dataset] != "")
		grouped[dataset] = grouped[dataset] "," line
	else
		grouped[dataset] = line
}
END {
	for (i = 1; i <= count; i++)
		printf "%s\t%s\n", order[i], grouped[order[i]]
}' "$l_dataset_filter_file" "$l_property_tree_file"
}

# Purpose: Clean up the recursive property prefetch stage files that this
# module created or tracks.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup on success and failure paths so temporary state does not
# linger.
zxfer_cleanup_recursive_property_prefetch_stage_files() {
	zxfer_cleanup_runtime_artifact_paths "$@"
}

# Purpose: Mark the recursive property prefetch failed in the module-owned
# state.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup so later helpers can make decisions from one shared marker
# instead of re-deriving it.
zxfer_mark_recursive_property_prefetch_failed() {
	l_side=$1

	case "$l_side" in
	source)
		g_zxfer_source_property_tree_prefetch_state=2
		;;
	destination)
		g_zxfer_destination_property_tree_prefetch_state=2
		;;
	esac
}

# Purpose: Prefetch the recursive normalized properties so later lookups can
# reuse staged data.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before a loop would otherwise repeat the same live probe or
# read.
zxfer_prefetch_recursive_normalized_properties() {
	l_side=$1

	case "$l_side" in
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
	l_dataset_list=$(zxfer_get_property_tree_prefetch_dataset_list "$l_side") ||
		l_dataset_list_status=$?
	if [ "$l_dataset_list_status" -ne 0 ]; then
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_dataset_list_status"
	fi

	[ -n "$l_root_dataset" ] || {
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return 1
	}
	[ -n "$l_zfs_cmd" ] || {
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return 1
	}
	l_cache_dir_status=0
	zxfer_ensure_property_cache_dir || l_cache_dir_status=$?
	if [ "$l_cache_dir_status" -ne 0 ]; then
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_cache_dir_status"
	fi

	l_dataset_filter_file=""
	l_machine_tree_file=""
	l_human_tree_file=""
	l_machine_grouped_file=""
	l_human_grouped_file=""
	l_combined_grouped_file=""
	l_tree_err_file=""
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_dataset_filter_file=$g_zxfer_temp_file_result
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_machine_tree_file=$g_zxfer_temp_file_result
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_human_tree_file=$g_zxfer_temp_file_result
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_machine_grouped_file=$g_zxfer_temp_file_result
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" "$l_machine_grouped_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_human_grouped_file=$g_zxfer_temp_file_result
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" "$l_machine_grouped_file" "$l_human_grouped_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_combined_grouped_file=$g_zxfer_temp_file_result
	l_stage_status=0
	zxfer_get_temp_file >/dev/null || l_stage_status=$?
	if [ "$l_stage_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" "$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_stage_status"
	fi
	l_tree_err_file=$g_zxfer_temp_file_result

	# shellcheck disable=SC2016
	printf '%s\n' "$l_dataset_list" | grep -v '^[[:space:]]*$' |
		"${g_cmd_awk:-awk}" '!seen[$0]++' >"$l_dataset_filter_file"
	if [ ! -s "$l_dataset_filter_file" ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return 1
	fi

	zxfer_profile_increment_counter "$l_profile_counter"
	l_tree_status=0
	zxfer_run_zfs_cmd_for_spec "$l_zfs_cmd" get -r -Hpo name,property,value,source all "$l_root_dataset" >"$l_machine_tree_file" 2>"$l_tree_err_file" ||
		l_tree_status=$?
	if [ "$l_tree_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_tree_status"
	fi
	l_tree_status=0
	zxfer_run_zfs_cmd_for_spec "$l_zfs_cmd" get -r -Ho name,property,value,source all "$l_root_dataset" >"$l_human_tree_file" 2>"$l_tree_err_file" ||
		l_tree_status=$?
	if [ "$l_tree_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_tree_status"
	fi

	l_group_status=0
	zxfer_group_recursive_property_tree_by_dataset "$l_dataset_filter_file" "$l_machine_tree_file" >"$l_machine_grouped_file" ||
		l_group_status=$?
	if [ "$l_group_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_group_status"
	fi
	l_group_status=0
	zxfer_group_recursive_property_tree_by_dataset "$l_dataset_filter_file" "$l_human_tree_file" >"$l_human_grouped_file" ||
		l_group_status=$?
	if [ "$l_group_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_group_status"
	fi

	# shellcheck disable=SC2016
	"${g_cmd_awk:-awk}" -F '	' '
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
}' "$l_machine_grouped_file" "$l_human_grouped_file" >"$l_combined_grouped_file"
	l_group_merge_status=$?
	if [ "$l_group_merge_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		return "$l_group_merge_status"
	fi

	l_tab='	'
	l_grouped_apply_status=0
	if zxfer_read_runtime_artifact_file "$l_combined_grouped_file" >/dev/null; then
		while IFS= read -r l_grouped_line || [ -n "$l_grouped_line" ]; do
			[ -n "$l_grouped_line" ] || continue
			l_dataset=${l_grouped_line%%"$l_tab"*}
			l_grouped_rest=${l_grouped_line#*"$l_tab"}
			case "$l_grouped_rest" in
			*"$l_tab"*)
				l_machine_pvs=${l_grouped_rest%%"$l_tab"*}
				l_human_pvs=${l_grouped_rest#*"$l_tab"}
				;;
			*)
				continue
				;;
			esac
			[ -n "$l_dataset" ] || continue
			[ -n "$l_machine_pvs" ] || continue
			[ -n "$l_human_pvs" ] || continue
			zxfer_resolve_human_vars "$l_machine_pvs" "$l_human_pvs"
			l_cache_path_status=0
			zxfer_property_cache_dataset_path normalized "$l_side" "$l_dataset" >/dev/null 2>&1 ||
				l_cache_path_status=$?
			if [ "$l_cache_path_status" -ne 0 ]; then
				l_grouped_apply_status=$l_cache_path_status
				break
			fi
			l_cache_store_status=0
			zxfer_property_cache_store "$g_zxfer_property_cache_path" "$human_results" >/dev/null 2>&1 ||
				l_cache_store_status=$?
			if [ "$l_cache_store_status" -ne 0 ]; then
				l_grouped_apply_status=$l_cache_store_status
				break
			fi
		done <<EOF
$g_zxfer_runtime_artifact_read_result
EOF
		l_grouped_read_status=0
	else
		l_grouped_read_status=$?
	fi

	if [ "$l_grouped_read_status" -ne 0 ] || [ "$l_grouped_apply_status" -ne 0 ]; then
		zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		zxfer_mark_recursive_property_prefetch_failed "$l_side"
		if [ "$l_grouped_read_status" -ne 0 ]; then
			return "$l_grouped_read_status"
		fi
		return "$l_grouped_apply_status"
	fi

	zxfer_cleanup_recursive_property_prefetch_stage_files "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
		"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
		"$l_tree_err_file"

	case "$l_side" in
	source) g_zxfer_source_property_tree_prefetch_state=1 ;;
	destination) g_zxfer_destination_property_tree_prefetch_state=1 ;;
	esac
	return 0
}

# Purpose: Run the optional prefetch recursive normalized properties step only
# when the current state requires it.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup to keep the optional branch in one place instead of
# scattering the condition across callers.
zxfer_maybe_prefetch_recursive_normalized_properties() {
	l_dataset=$1
	l_zfs_cmd=$2
	l_lookup_side=$3

	case "$l_lookup_side" in
	source)
		[ -n "${g_zxfer_source_property_tree_prefetch_root:-}" ] || return 1
		[ "${g_zxfer_source_property_tree_prefetch_zfs_cmd:-}" = "$l_zfs_cmd" ] || return 1
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
		[ "${g_zxfer_destination_property_tree_prefetch_zfs_cmd:-}" = "$l_zfs_cmd" ] || return 1
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

	l_prefetch_status=0
	zxfer_prefetch_recursive_normalized_properties "$l_lookup_side" || l_prefetch_status=$?
	if [ "$l_prefetch_status" -ne 0 ]; then
		return "$l_prefetch_status"
	fi

	l_cache_path_status=0
	zxfer_property_cache_dataset_path normalized "$l_lookup_side" "$l_dataset" >/dev/null 2>&1 ||
		l_cache_path_status=$?
	if [ "$l_cache_path_status" -ne 0 ]; then
		return "$l_cache_path_status"
	fi
	[ -f "$g_zxfer_property_cache_path" ]
}

# Purpose: Load the normalized dataset properties from the module-owned cache
# or staged source.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when later helpers need a checked in-memory copy of staged
# data.
zxfer_load_normalized_dataset_properties() {
	l_dataset=$1
	l_zfs_cmd=$2
	l_lookup_side=${3:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	g_zxfer_normalized_dataset_properties=""
	g_zxfer_normalized_dataset_properties_cache_hit=0

	l_cache_path=""
	if zxfer_property_cache_dataset_path normalized "$l_lookup_side" "$l_dataset" >/dev/null 2>&1; then
		l_cache_path=$g_zxfer_property_cache_path
		if [ -f "$l_cache_path" ]; then
			if zxfer_read_property_cache_file "$l_cache_path" &&
				[ -n "$g_zxfer_property_cache_read_result" ]; then
				g_zxfer_normalized_dataset_properties=$g_zxfer_property_cache_read_result
				g_zxfer_normalized_dataset_properties_cache_hit=1
				return 0
			fi
		fi
		if zxfer_maybe_prefetch_recursive_normalized_properties "$l_dataset" "$l_zfs_cmd" "$l_lookup_side" >/dev/null 2>&1 &&
			[ -f "$l_cache_path" ]; then
			if zxfer_read_property_cache_file "$l_cache_path" &&
				[ -n "$g_zxfer_property_cache_read_result" ]; then
				g_zxfer_normalized_dataset_properties=$g_zxfer_property_cache_read_result
				g_zxfer_normalized_dataset_properties_cache_hit=1
				return 0
			fi
		fi
	fi

	case "$l_lookup_side" in
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
	l_machine_pvs=$(zxfer_run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source all "$l_dataset" 2>&1) ||
		l_machine_status=$?
	if [ "$l_machine_status" -ne 0 ]; then
		printf '%s\n' "$l_machine_pvs"
		return "$l_machine_status"
	fi
	l_status=0
	zxfer_capture_serialized_property_records "$l_machine_pvs" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_machine_pvs=$g_zxfer_serialized_property_records_result
	l_human_status=0
	l_human_pvs=$(zxfer_run_zfs_cmd_for_spec "$l_zfs_cmd" get -Ho property,value,source all "$l_dataset" 2>&1) ||
		l_human_status=$?
	if [ "$l_human_status" -ne 0 ]; then
		printf '%s\n' "$l_human_pvs"
		return "$l_human_status"
	fi
	l_status=0
	zxfer_capture_serialized_property_records "$l_human_pvs" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_human_pvs=$g_zxfer_serialized_property_records_result
	zxfer_resolve_human_vars "$l_machine_pvs" "$l_human_pvs"
	g_zxfer_normalized_dataset_properties=$human_results

	if [ -n "$l_cache_path" ]; then
		if ! zxfer_property_cache_store "$l_cache_path" "$g_zxfer_normalized_dataset_properties" >/dev/null 2>&1; then
			zxfer_disable_property_iteration_cache
		fi
	fi

	return 0
}

# Purpose: Return the required property probe in the form expected by later
# helpers.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when sibling helpers need the same lookup without duplicating
# module logic.
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

	l_cache_path=""
	if zxfer_property_cache_property_path required "$l_lookup_side" "$l_dataset" "$l_required_property" >/dev/null 2>&1; then
		l_cache_path=$g_zxfer_property_cache_path
		if [ -f "$l_cache_path" ]; then
			if zxfer_read_property_cache_file "$l_cache_path" &&
				[ -n "$g_zxfer_property_cache_read_result" ]; then
				g_zxfer_required_property_probe_result=$g_zxfer_property_cache_read_result
				return 0
			fi
		fi
	fi

	zxfer_profile_increment_counter g_zxfer_profile_required_property_backfill_gets
	l_explicit_probe_status=0
	l_explicit_probe_output=$(zxfer_run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source "$l_required_property" "$l_dataset" 2>&1) ||
		l_explicit_probe_status=$?
	if [ "$l_explicit_probe_status" -eq 0 ]; then
		l_status=0
		zxfer_capture_serialized_property_records "$l_explicit_probe_output" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
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
			g_zxfer_required_property_probe_result="__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__"
			;;
		*)
			g_zxfer_required_properties_result="Failed to retrieve required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
			printf '%s\n' "$g_zxfer_required_properties_result"
			return "$l_explicit_probe_status"
			;;
		esac
	fi

	if [ -n "$l_cache_path" ]; then
		if ! zxfer_property_cache_store "$l_cache_path" "$g_zxfer_required_property_probe_result" >/dev/null 2>&1; then
			zxfer_disable_property_iteration_cache
		fi
	fi

	return 0
}

# Purpose: Populate the required properties present from the active source
# data.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when the surrounding flow needs a fully expanded in-memory
# view.
zxfer_populate_required_properties_present() {
	l_dataset=$1
	l_property_list=$2
	l_zfs_cmd=$3
	l_required_properties=$4
	l_lookup_side=${5:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	g_zxfer_required_properties_result=""
	l_result=$l_property_list
	l_oldifs=$IFS
	IFS=","
	for l_required_property in $l_required_properties; do
		[ -n "$l_required_property" ] || continue
		l_found_property=0
		for l_property_line in $l_result; do
			l_property_name=$(echo "$l_property_line" | cut -f1 -d=)
			if [ "$l_property_name" = "$l_required_property" ]; then
				l_found_property=1
				break
			fi
		done

		[ "$l_found_property" -eq 0 ] || continue

		l_status=0
		zxfer_get_required_property_probe "$l_dataset" "$l_required_property" "$l_zfs_cmd" "$l_lookup_side" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			IFS=$l_oldifs
			return "$l_status"
		fi

		case "$g_zxfer_required_property_probe_result" in
		"" | "__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__")
			continue
			;;
		esac

		if [ -n "$l_result" ]; then
			l_result="$l_result,$g_zxfer_required_property_probe_result"
		else
			l_result=$g_zxfer_required_property_probe_result
		fi
	done
	IFS=$l_oldifs

	g_zxfer_required_properties_result=$l_result
	return 0
}

# Purpose: Load the destination props from the module-owned cache or staged
# source.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when later helpers need a checked in-memory copy of staged
# data.
zxfer_load_destination_props() {
	l_dataset=$1
	l_zfs_cmd=$2

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_RZFS
	fi

	g_zxfer_destination_pvs_raw=""
	l_status=0
	zxfer_load_normalized_dataset_properties "$l_dataset" "$l_zfs_cmd" destination ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	g_zxfer_destination_pvs_raw=$g_zxfer_normalized_dataset_properties
	return 0
}

# Purpose: Resolve the effective human vars that zxfer should use.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup after configuration, cache state, or remote state can change
# the final choice.
#
# Normalize the list of properties to set by using a mix of human-readable and
# machine-readable values
zxfer_resolve_human_vars() {
	l_machine_vars=$1
	l_human_vars=$2
	l_funcifs=$IFS
	IFS=","

	l_human_results=
	for l_human_var in $l_human_vars; do
		l_human_prop=${l_human_var%%=*}
		for l_machine_var in $l_machine_vars; do
			l_machine_prop=${l_machine_var%%=*}
			if [ "$l_human_prop" = "$l_machine_prop" ]; then
				l_machine_property=$(echo "$l_machine_var" | cut -f1 -d=)
				l_machine_value=$(echo "$l_machine_var" | cut -f2 -d=)
				l_machine_source=$(echo "$l_machine_var" | cut -f3 -d=)
				l_human_value=$(echo "$l_human_var" | cut -f2 -d=)
				if [ "$l_human_value" = "none" ]; then
					l_machine_value=$l_human_value
				fi
				l_human_results="${l_human_results}$l_machine_property=$l_machine_value=$l_machine_source,"
			fi
		done
	done
	l_human_results=${l_human_results%,}
	IFS=$l_funcifs
	human_results=$l_human_results
}

# Purpose: Return the normalized dataset properties in the form expected by
# later helpers.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup when sibling helpers need the same lookup without duplicating
# module logic.
#
# Retrieve the normalized property/value/source list for a dataset while
# handling locales that require both machine (-Hp) and human (-H) parsing.
# $1: dataset to query
# $2: zfs command to execute (defaults to $g_LZFS)
# $3: optional lookup side label (source/destination/other) for profiling
zxfer_get_normalized_dataset_properties() {
	l_status=0
	zxfer_load_normalized_dataset_properties "$1" "$2" "$3" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	printf '%s\n' "$g_zxfer_normalized_dataset_properties"
}

# Purpose: Ensure the required properties present exists and is ready before
# the flow continues.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before later helpers assume the resource or cache is
# available.
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
	l_status=0
	zxfer_populate_required_properties_present "$1" "$2" "$3" "$4" "$5" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	printf '%s\n' "$g_zxfer_required_properties_result"
}

# Purpose: Collect the destination props into the module-owned format used by
# later steps.
# Usage: Called during property prefetch, cache staging, and normalized
# property lookup before reconciliation or apply logic consumes the combined
# result.
#
# Collect destination properties via the remote/local zfs command.
# $1: dataset name
# $2: command used to query properties (defaults to $g_RZFS)
zxfer_collect_destination_props() {
	l_status=0
	zxfer_load_destination_props "$1" "$2" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	printf '%s\n' "$g_zxfer_destination_pvs_raw"
}
