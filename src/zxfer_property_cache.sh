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

# module variables
m_normalized_dataset_properties=""
m_normalized_dataset_properties_cache_hit=0
m_required_properties_result=""
m_required_property_probe_result=""
m_destination_pvs_raw=""
m_property_cache_path=""
m_property_cache_key=""

zxfer_escape_serialized_property_value() {
	l_value=$1

	printf '%s\n' "$l_value" | "${g_cmd_awk:-awk}" '
{
	gsub(/%/, "%25")
	gsub(/,/, "%2C")
	gsub(/=/, "%3D")
	gsub(/;/, "%3B")
	gsub(/\t/, "%09")
	gsub(/\r/, "%0D")
	print
}'
}

zxfer_unescape_serialized_property_value() {
	l_value=$1

	printf '%s\n' "$l_value" | "${g_cmd_awk:-awk}" '
{
	gsub(/%0D/, "\r")
	gsub(/%09/, "\t")
	gsub(/%3B/, ";")
	gsub(/%3D/, "=")
	gsub(/%2C/, ",")
	gsub(/%25/, "%")
	print
}'
}

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

zxfer_reset_property_iteration_caches() {
	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		rm -rf "$g_zxfer_property_cache_dir"
	fi

	g_zxfer_property_cache_dir=""
	g_zxfer_property_cache_unavailable=0
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0
}

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

	g_zxfer_source_property_tree_prefetch_root=${initial_source:-}
	g_zxfer_source_property_tree_prefetch_zfs_cmd=${g_LZFS:-}
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=${g_destination:-}
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=${g_RZFS:-}
	g_zxfer_destination_property_tree_prefetch_state=0
}

zxfer_ensure_property_cache_dir() {
	if [ "${g_zxfer_property_cache_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		return 0
	fi

	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		g_zxfer_property_cache_unavailable=1
		g_zxfer_property_cache_dir=""
		return 1
	fi
	if ! g_zxfer_property_cache_dir=$(mktemp -d "$l_tmpdir/zxfer-property-cache.XXXXXX" 2>/dev/null); then
		g_zxfer_property_cache_unavailable=1
		g_zxfer_property_cache_dir=""
		return 1
	fi

	return 0
}

zxfer_property_cache_encode_key() {
	l_key=$1
	l_key_hex=$(printf '%s' "$l_key" | LC_ALL=C od -An -tx1 -v | tr -d ' \n')
	if [ "$l_key_hex" = "" ]; then
		l_key_hex="00"
	fi
	m_property_cache_key=$l_key_hex
	printf '%s\n' "$m_property_cache_key"
}

zxfer_property_cache_dataset_path() {
	l_bucket=$1
	l_side=$2
	l_dataset=$3

	if ! zxfer_ensure_property_cache_dir; then
		return 1
	fi

	if ! l_dataset_key=$(zxfer_property_cache_encode_key "$l_dataset"); then
		return 1
	fi

	m_property_cache_path=$g_zxfer_property_cache_dir/$l_bucket/$l_side/$l_dataset_key
	printf '%s\n' "$m_property_cache_path"
}

zxfer_property_cache_property_path() {
	l_bucket=$1
	l_side=$2
	l_dataset=$3
	l_property=$4

	if ! zxfer_ensure_property_cache_dir; then
		return 1
	fi

	if ! l_dataset_key=$(zxfer_property_cache_encode_key "$l_dataset"); then
		return 1
	fi
	if ! l_property_key=$(zxfer_property_cache_encode_key "$l_property"); then
		return 1
	fi

	m_property_cache_path=$g_zxfer_property_cache_dir/$l_bucket/$l_side/$l_dataset_key/$l_property_key
	printf '%s\n' "$m_property_cache_path"
}

zxfer_property_cache_store() {
	l_cache_path=$1
	l_cache_value=$2
	l_cache_dir=${l_cache_path%/*}

	if ! mkdir -p "$l_cache_dir"; then
		return 1
	fi

	printf '%s\n' "$l_cache_value" >"$l_cache_path"
}

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

zxfer_invalidate_destination_property_cache() {
	zxfer_invalidate_dataset_property_cache destination "$1"
}

zxfer_reset_destination_property_iteration_cache() {
	if [ -z "${g_zxfer_property_cache_dir:-}" ] || [ ! -d "$g_zxfer_property_cache_dir" ]; then
		g_zxfer_destination_property_tree_prefetch_state=0
		return
	fi

	rm -rf "$g_zxfer_property_cache_dir/normalized/destination"
	rm -rf "$g_zxfer_property_cache_dir/required/destination"
	g_zxfer_destination_property_tree_prefetch_state=0
}

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
		if [ -n "${initial_source:-}" ]; then
			printf '%s\n' "$initial_source"
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

	if ! l_dataset_list=$(zxfer_get_property_tree_prefetch_dataset_list "$l_side"); then
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	fi

	[ -n "$l_root_dataset" ] || {
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	}
	[ -n "$l_zfs_cmd" ] || {
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	}
	if ! zxfer_ensure_property_cache_dir; then
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	fi

	l_dataset_filter_file=$(get_temp_file)
	l_machine_tree_file=$(get_temp_file)
	l_human_tree_file=$(get_temp_file)
	l_machine_grouped_file=$(get_temp_file)
	l_human_grouped_file=$(get_temp_file)
	l_combined_grouped_file=$(get_temp_file)
	l_tree_err_file=$(get_temp_file)

	# shellcheck disable=SC2016
	printf '%s\n' "$l_dataset_list" | grep -v '^[[:space:]]*$' |
		"${g_cmd_awk:-awk}" '!seen[$0]++' >"$l_dataset_filter_file"
	if [ ! -s "$l_dataset_filter_file" ]; then
		rm -f "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	fi

	zxfer_profile_increment_counter "$l_profile_counter"
	if ! run_zfs_cmd_for_spec "$l_zfs_cmd" get -r -Hpo name,property,value,source all "$l_root_dataset" >"$l_machine_tree_file" 2>"$l_tree_err_file"; then
		rm -f "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	fi
	if ! run_zfs_cmd_for_spec "$l_zfs_cmd" get -r -Ho name,property,value,source all "$l_root_dataset" >"$l_human_tree_file" 2>"$l_tree_err_file"; then
		rm -f "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	fi

	if ! zxfer_group_recursive_property_tree_by_dataset "$l_dataset_filter_file" "$l_machine_tree_file" >"$l_machine_grouped_file"; then
		rm -f "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
	fi
	if ! zxfer_group_recursive_property_tree_by_dataset "$l_dataset_filter_file" "$l_human_tree_file" >"$l_human_grouped_file"; then
		rm -f "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
			"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
			"$l_tree_err_file"
		case "$l_side" in
		source) g_zxfer_source_property_tree_prefetch_state=2 ;;
		destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
		esac
		return 1
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

	l_tab='	'
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
		resolve_human_vars "$l_machine_pvs" "$l_human_pvs"
		if zxfer_property_cache_dataset_path normalized "$l_side" "$l_dataset" >/dev/null 2>&1; then
			zxfer_property_cache_store "$m_property_cache_path" "$human_results" >/dev/null 2>&1 || :
		fi
	done <"$l_combined_grouped_file"

	rm -f "$l_dataset_filter_file" "$l_machine_tree_file" "$l_human_tree_file" \
		"$l_machine_grouped_file" "$l_human_grouped_file" "$l_combined_grouped_file" \
		"$l_tree_err_file"

	case "$l_side" in
	source) g_zxfer_source_property_tree_prefetch_state=1 ;;
	destination) g_zxfer_destination_property_tree_prefetch_state=1 ;;
	esac
	return 0
}

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

	if ! zxfer_prefetch_recursive_normalized_properties "$l_lookup_side"; then
		return 1
	fi

	if ! zxfer_property_cache_dataset_path normalized "$l_lookup_side" "$l_dataset" >/dev/null 2>&1; then
		return 1
	fi
	[ -f "$m_property_cache_path" ]
}

zxfer_load_normalized_dataset_properties() {
	l_dataset=$1
	l_zfs_cmd=$2
	l_lookup_side=${3:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	m_normalized_dataset_properties=""
	m_normalized_dataset_properties_cache_hit=0

	l_cache_path=""
	if zxfer_property_cache_dataset_path normalized "$l_lookup_side" "$l_dataset" >/dev/null 2>&1; then
		l_cache_path=$m_property_cache_path
		if [ -f "$l_cache_path" ]; then
			m_normalized_dataset_properties=$(cat "$l_cache_path")
			m_normalized_dataset_properties_cache_hit=1
			return 0
		fi
		if zxfer_maybe_prefetch_recursive_normalized_properties "$l_dataset" "$l_zfs_cmd" "$l_lookup_side" >/dev/null 2>&1 &&
			[ -f "$l_cache_path" ]; then
			m_normalized_dataset_properties=$(cat "$l_cache_path")
			m_normalized_dataset_properties_cache_hit=1
			return 0
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

	if ! l_machine_pvs=$(run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source all "$l_dataset" 2>&1); then
		printf '%s\n' "$l_machine_pvs"
		return 1
	fi
	l_machine_pvs=$(printf '%s\n' "$l_machine_pvs" | zxfer_serialize_property_records_from_stdin)
	if ! l_human_pvs=$(run_zfs_cmd_for_spec "$l_zfs_cmd" get -Ho property,value,source all "$l_dataset" 2>&1); then
		printf '%s\n' "$l_human_pvs"
		return 1
	fi
	l_human_pvs=$(printf '%s\n' "$l_human_pvs" | zxfer_serialize_property_records_from_stdin)
	resolve_human_vars "$l_machine_pvs" "$l_human_pvs"
	m_normalized_dataset_properties=$human_results

	if [ -n "$l_cache_path" ]; then
		zxfer_property_cache_store "$l_cache_path" "$m_normalized_dataset_properties" >/dev/null 2>&1 || :
	fi

	return 0
}

zxfer_get_required_property_probe() {
	l_dataset=$1
	l_required_property=$2
	l_zfs_cmd=$3
	l_lookup_side=${4:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	m_required_property_probe_result=""

	l_cache_path=""
	if zxfer_property_cache_property_path required "$l_lookup_side" "$l_dataset" "$l_required_property" >/dev/null 2>&1; then
		l_cache_path=$m_property_cache_path
		if [ -f "$l_cache_path" ]; then
			m_required_property_probe_result=$(cat "$l_cache_path")
			return 0
		fi
	fi

	zxfer_profile_increment_counter g_zxfer_profile_required_property_backfill_gets
	if l_explicit_probe_output=$(run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source "$l_required_property" "$l_dataset" 2>&1); then
		l_explicit_property=$(printf '%s\n' "$l_explicit_probe_output" | zxfer_serialize_property_records_from_stdin)
		case "$l_explicit_property" in
		"$l_required_property"=*=*) ;;
		*)
			m_required_properties_result="Failed to parse required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
			printf '%s\n' "$m_required_properties_result"
			return 1
			;;
		esac
		m_required_property_probe_result=$l_explicit_property
	else
		case "$l_explicit_probe_output" in
		*"does not apply"* | *"invalid property"* | *"no such property"* | *"not supported"*)
			m_required_property_probe_result="__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__"
			;;
		*)
			m_required_properties_result="Failed to retrieve required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
			printf '%s\n' "$m_required_properties_result"
			return 1
			;;
		esac
	fi

	if [ -n "$l_cache_path" ]; then
		zxfer_property_cache_store "$l_cache_path" "$m_required_property_probe_result" >/dev/null 2>&1 || :
	fi

	return 0
}

populate_required_properties_present() {
	l_dataset=$1
	l_property_list=$2
	l_zfs_cmd=$3
	l_required_properties=$4
	l_lookup_side=${5:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

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

		if ! zxfer_get_required_property_probe "$l_dataset" "$l_required_property" "$l_zfs_cmd" "$l_lookup_side"; then
			IFS=$l_oldifs
			return 1
		fi

		case "$m_required_property_probe_result" in
		"" | "__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__")
			continue
			;;
		esac

		if [ -n "$l_result" ]; then
			l_result="$l_result,$m_required_property_probe_result"
		else
			l_result=$m_required_property_probe_result
		fi
	done
	IFS=$l_oldifs

	m_required_properties_result=$l_result
	return 0
}

load_destination_props() {
	l_dataset=$1
	l_zfs_cmd=$2

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_RZFS
	fi

	if ! zxfer_load_normalized_dataset_properties "$l_dataset" "$l_zfs_cmd" destination; then
		return 1
	fi

	m_destination_pvs_raw=$m_normalized_dataset_properties
	return 0
}

#
# Normalize the list of properties to set by using a mix of human-readable and
# machine-readable values
#
resolve_human_vars() {
	_machine_vars=$1
	_human_vars=$2
	_FUNCIFS=$IFS
	IFS=","

	human_results=
	for h_var in $_human_vars; do
		h_prop=${h_var%%=*}
		for m_var in $_machine_vars; do
			m_prop=${m_var%%=*}
			if [ "$h_prop" = "$m_prop" ]; then
				machine_property=$(echo "$m_var" | cut -f1 -d=)
				machine_value=$(echo "$m_var" | cut -f2 -d=)
				machine_source=$(echo "$m_var" | cut -f3 -d=)
				human_value=$(echo "$h_var" | cut -f2 -d=)
				if [ "$human_value" = "none" ]; then
					machine_value=$human_value
				fi
				human_results="${human_results}$machine_property=$machine_value=$machine_source,"
			fi
		done
	done
	human_results=${human_results%,}
	IFS=$_FUNCIFS
}

# Retrieve the normalized property/value/source list for a dataset while
# handling locales that require both machine (-Hp) and human (-H) parsing.
# $1: dataset to query
# $2: zfs command to execute (defaults to $g_LZFS)
# $3: optional lookup side label (source/destination/other) for profiling
#
get_normalized_dataset_properties() {
	zxfer_load_normalized_dataset_properties "$1" "$2" "$3" || return 1
	printf '%s\n' "$m_normalized_dataset_properties"
}

#
# Some OpenZFS implementations do not include every creation-time property in
# `zfs get all` output even though the property is queryable directly. Append
# any missing required properties so later diffing can still enforce
# creation-time mismatch rules consistently.
# $1: dataset name
# $2: existing property list
# $3: zfs command used to query properties
# $4: comma-separated list of required property names
#
ensure_required_properties_present() {
	populate_required_properties_present "$1" "$2" "$3" "$4" "$5" || return 1
	printf '%s\n' "$m_required_properties_result"
}

#
# Collect destination properties via the remote/local zfs command.
# $1: dataset name
# $2: command used to query properties (defaults to $g_RZFS)
#
collect_destination_props() {
	load_destination_props "$1" "$2" || return 1
	printf '%s\n' "$m_destination_pvs_raw"
}
