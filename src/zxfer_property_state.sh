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
# owns globals: ZXFER_PROPERTY_AWK_LIB and ZXFER_PROPERTY_NORMALIZE_AWK, the
#   per-iteration property tables and recursive prefetch state, the per-run
#   read scratch files (g_zxfer_property_*_file), and the result globals
#   g_zxfer_normalized_dataset_properties(_cache_hit),
#   g_zxfer_required_properties_result, g_zxfer_property_record_value/source,
#   g_zxfer_encoded_property_value, g_zxfer_decoded_property_value (with the
#   helper results g_zxfer_property_replace_result and
#   g_zxfer_property_code_byte), g_zxfer_property_list_value_result,
#   g_zxfer_property_display_list_result
#   and the shared failure text g_zxfer_property_error_result (the policy
#   module's create-metadata helper writes it too). The policy and reconcile
#   modules own their own results; zxfer_reset_property_reconcile_state only
#   clears them at session start.
# reads globals: the -R/-P/-o options, g_initial_source, g_destination, the
#   recursive dataset lists, and g_cmd_awk.
# mutates caches: the property tables, through prefetch, live-read appends,
#   and destination invalidation.
# returns via stdout: zxfer_parse_property_views prints the parser output;
#   every other helper publishes result globals.
#
# Every `zfs get all` read also lists the property names alone (the
# skeleton, read after the values). A value is taken from the listing only
# where the skeleton pins its record to one line in an unbroken run from the
# first line, and read alone otherwise: see ZXFER_PROPERTY_NORMALIZE_AWK.

# A property list is comma-separated "property=value=source" items whose
# values are percent-encoded (%25 %2C %3D %3B %09 %0D %0A for % , = ; tab CR
# LF), so a value never holds a list delimiter. Each side's table holds
# newline-separated "dataset<TAB>list" rows in that encoding: lookups read it
# row by row, and a destination mutation strips the mutated dataset's row, plus
# its descendants' rows when their inherited values may have changed.

# Purpose: Reset the per-transfer property result globals and forget the
# property read scratch files.
# Usage: Called once at session initialization.
zxfer_reset_property_reconcile_state() {
	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
	g_zxfer_override_pvs_result=""
	g_zxfer_creation_pvs_result=""
	g_zxfer_sanitized_property_list_result=""
	g_zxfer_source_dataset_type_result=""
	g_zxfer_source_volume_size_result=""
	g_zxfer_diff_dest_pvs_result=""
	g_zxfer_diff_initial_set_result=""
	g_zxfer_diff_child_set_result=""
	g_zxfer_diff_inherit_result=""
	g_zxfer_adjusted_set_list=""
	g_zxfer_adjusted_inherit_list=""
	g_zxfer_property_error_result=""
	g_zxfer_property_skeleton_file=""
	g_zxfer_property_machine_file=""
	g_zxfer_property_human_file=""
	g_zxfer_property_error_file=""
	g_zxfer_property_wanted_file=""
}

# Purpose: Reset the per-iteration property tables, prefetch state, and lookup
# results so the next property pass starts clean.
# Usage: zxfer_reset_property_iteration_caches, at session start and at the
# top of every replication iteration.
zxfer_reset_property_iteration_caches() {
	g_zxfer_normalized_dataset_properties=""
	g_zxfer_normalized_dataset_properties_cache_hit=0
	g_zxfer_required_properties_result=""
	g_zxfer_required_property_probe_result=""
	g_zxfer_property_table_lookup_result=""
	g_zxfer_source_property_table=""
	g_zxfer_destination_property_table=""
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Arm (or disarm) the recursive property prefetch for both sides from
# the current options and dataset context.
# Usage: zxfer_refresh_property_tree_prefetch_context, when an iteration
# starts; the prefetch itself runs on the first table miss for that side.
zxfer_refresh_property_tree_prefetch_context() {
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_state=0
	if [ "${g_option_R_recursive:-}" = "" ] ||
		{ [ "${g_option_P_transfer_property:-0}" -ne 1 ] &&
			[ -z "${g_option_o_override_property:-}" ]; }; then
		return
	fi

	g_zxfer_source_property_tree_prefetch_root=${g_initial_source:-}
	g_zxfer_destination_property_tree_prefetch_root=${g_destination:-}
}

################################################################################
# SERIALIZATION
################################################################################

# Shared AWK helpers, the prefix of every property AWK program:
#   append_csv(list, item)             list with item appended after a comma
#   encode_value(value)                percent-encode % , = ; tab CR LF
#   decode_value(value)                undo encode_value (%25 last)
#   csv_to_set(csv, set)               set[item] = 1 for each non-empty item
#   first_values(list, value, source)  the first value and source of each
#                                      property in a serialized list
#   split_override_csv(input, output)  split -o text on commas; "\," stays a
#                                      literal comma
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_AWK_LIB='
function append_csv(list, item) {
	if (list == "")
		return item
	return list "," item
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
function csv_to_set(csv, set, count, items, i) {
	count = split(csv, items, ",")
	for (i = 1; i <= count; i++)
		if (items[i] != "")
			set[items[i]] = 1
}
function first_values(list, value, source, count, items, i, fields) {
	count = split(list, items, ",")
	for (i = 1; i <= count; i++) {
		if (items[i] == "")
			continue
		split(items[i], fields, "=")
		if (!(fields[1] in value)) {
			value[fields[1]] = fields[2]
			source[fields[1]] = fields[3]
		}
	}
}
function split_override_csv(input, output, count, i, character, field) {
	count = 0
	field = ""
	for (i = 1; i <= length(input); i++) {
		character = substr(input, i, 1)
		if (character == "\\" && substr(input, i + 1, 1) == ",") {
			field = field ","
			i++
		} else if (character == ",") {
			output[++count] = field
			field = ""
		} else {
			field = field character
		}
	}
	if (input != "")
		output[++count] = field
	return count
}
'

# One POSIX AWK program parses the `zfs get -H` captures of one property read.
# zfs prints values raw, and a value may hold TAB and LF, so a value line can
# look exactly like another record. Each read therefore also lists the record
# keys alone, in the same order: the skeleton (`zfs get -H -o property all`,
# or `-o name,property` for a recursive read), since names hold neither byte.
# A view line's head is its text before the TAB that ends a key (the first
# TAB, or the second for a recursive read). Record i is known when records 1
# to i-1 are, line i is headed by key i and line i+1 by key i+1 (or line i is
# the view's last line and key i the last key), and no other line has either
# head. A record of the two that the views hold can then start only on its
# own line, so record i is line i alone, and its value runs from the TAB
# after the key to the line's last TAB, since a source holds no TAB. The first
# record not known ends the run; it and every later record are left to be
# re-read alone.
# The views and the skeleton come from separate zfs calls. The skeleton is
# read last, so a record removed between them is never listed, but a record
# created between them is listed while the views lack it, and a line of the
# value printed just before it (the carrier) can then pass for its record.
# The carrier is published cut at its first LF, with a source taken from its
# own text, and the created record (with any created right after it) takes
# its value and source from that text; the read still succeeds. The cut
# stays even when the created record is then re-read alone. zfs prints a
# dataset's native properties before its user properties, so creating a user
# property (the userprop permission) forges only user properties; a native
# record can be forged only when a whole dataset is recreated between the
# calls. KNOWN_ISSUES.md lists both windows as Low.
#   mode=merge     ARGV: SKELETON MACHINE HUMAN for one dataset. Prints the
#                  payload in skeleton order (machine values and sources, a
#                  human "none" replacing the machine value) with a bare name
#                  for each record not known in both views, then a line with
#                  the comma list of those names.
#   mode=prefetch  ARGV: WANTED SKELETON MACHINE HUMAN for a recursive read,
#                  every row led by the dataset name. Prints one
#                  "dataset<TAB>payload" row per wanted dataset whose records
#                  are all known in both views.
# An unreadable file, a malformed or repeated skeleton row, or an empty
# skeleton beside a non-empty view exits 1. Runs behind ZXFER_PROPERTY_AWK_LIB.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_NORMALIZE_AWK='
function valid_property_name(name) {
	return name ~ /^[A-Za-z0-9_.:@-][A-Za-z0-9_.:@-]*$/
}
function valid_source(source) {
	return source == "-" || source == "local" || source == "default" ||
		source == "temporary" || source == "received" ||
		source == "inherited" || source == "none" ||
		source ~ /^inherited from [^\t]+$/
}
function read_lines(file, lines,    count, line, status) {
	count = 0
	while ((status = (getline line < file)) > 0)
		lines[++count] = line
	if (status < 0)
		failed = 1
	close(file)
	return count
}
function read_skeleton(file,    rows, count, i, fields, n) {
	key_count = read_lines(file, rows)
	for (i = 1; i <= key_count; i++) {
		n = split(rows[i], fields, "\t")
		if (n != 1 + has_name || fields[1] == "" ||
			!valid_property_name(fields[n]) || (rows[i] in key_seen))
			failed = 1
		key_seen[rows[i]] = 1
		key[i] = rows[i]
		key_dataset[i] = fields[1]
		key_property[i] = fields[n]
	}
}
function line_head(line,    at, next_at) {
	at = index(line, "\t")
	if (at && has_name) {
		next_at = index(substr(line, at + 1), "\t")
		at = next_at ? at + next_at : 0
	}
	return at ? substr(line, 1, at - 1) : ""
}
function starts_record(i, head, head_count) {
	return head[i] == key[i] && head_count[key[i]] == 1
}
function read_view(view, file,    lines, count, head, head_count, i, rest) {
	count = read_lines(file, lines)
	if (key_count == 0 && count > 0)
		failed = 1
	for (i = 1; i <= count; i++) {
		head[i] = line_head(lines[i])
		head_count[head[i]]++
	}
	for (i = 1; i <= key_count && starts_record(i, head, head_count); i++) {
		if (i < key_count ? !starts_record(i + 1, head, head_count) : count != key_count)
			break
		rest = substr(lines[i], length(key[i]) + 2)
		if (!match(rest, /\t[^\t]*$/) || !valid_source(substr(rest, RSTART + 1)))
			break
		known[view, i] = 1
		value[view, i] = substr(rest, 1, RSTART - 1)
		source[view, i] = substr(rest, RSTART + 1)
	}
}
function complete(i) {
	return known["machine", i] && known["human", i]
}
function merged_item(i,    item_value) {
	item_value = (value["human", i] == "none") ? "none" : value["machine", i]
	return key_property[i] "=" encode_value(item_value) "=" source["machine", i]
}
BEGIN {
	has_name = (mode == "prefetch")
	read_skeleton(ARGV[1 + has_name])
	read_view("machine", ARGV[2 + has_name])
	read_view("human", ARGV[3 + has_name])
	if (has_name)
		wanted_count = read_lines(ARGV[1], wanted_rows)
	if (failed)
		exit 1
	if (mode == "merge") {
		for (i = 1; i <= key_count; i++) {
			if (complete(i)) {
				payload = append_csv(payload, merged_item(i))
			} else {
				payload = append_csv(payload, key_property[i])
				reread = append_csv(reread, key_property[i])
			}
		}
		print payload
		print reread
		exit 0
	}
	for (i = 1; i <= wanted_count; i++)
		wanted[wanted_rows[i]] = 1
	for (i = 1; i <= key_count; i++) {
		dataset = key_dataset[i]
		if (!(dataset in dataset_payload)) {
			order[++dataset_count] = dataset
			dataset_payload[dataset] = ""
		}
		if (complete(i))
			dataset_payload[dataset] = append_csv(dataset_payload[dataset], merged_item(i))
		else
			incomplete[dataset] = 1
	}
	for (i = 1; i <= dataset_count; i++) {
		dataset = order[i]
		if ((dataset in wanted) && !(dataset in incomplete) && dataset_payload[dataset] != "")
			printf "%s\t%s\n", dataset, dataset_payload[dataset]
	}
	exit 0
}'

# Purpose: Run ZXFER_PROPERTY_NORMALIZE_AWK over staged captures, byte for
# byte (C locale).
# Usage: zxfer_parse_property_views merge SKELETON MACHINE HUMAN, or
# zxfer_parse_property_views prefetch WANTED SKELETON MACHINE HUMAN; prints the
# program's output and returns its status.
zxfer_parse_property_views() {
	l_parse_mode=$1
	shift
	LC_ALL=C "${g_cmd_awk:-awk}" -v mode="$l_parse_mode" \
		"$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_NORMALIZE_AWK" "$@"
}

# Purpose: Split one `property<TAB>value<TAB>source` record of PROPERTY. The
# value ends at the last TAB, since a source holds none.
# Usage: zxfer_split_property_record PROPERTY RECORD; publishes the raw
# g_zxfer_property_record_value and g_zxfer_property_record_source, or returns
# 1 when RECORD is not such a record with a known source.
zxfer_split_property_record() {
	case $2 in
	"$1$ZXFER_TAB"*"$ZXFER_TAB"*) ;;
	*) return 1 ;;
	esac
	l_record_rest=${2#"$1$ZXFER_TAB"}
	g_zxfer_property_record_value=${l_record_rest%"$ZXFER_TAB"*}
	g_zxfer_property_record_source=${l_record_rest##*"$ZXFER_TAB"}
	case $g_zxfer_property_record_source in
	*"$ZXFER_LF"*) return 1 ;;
	- | local | default | temporary | received | inherited | none | "inherited from "?*) ;;
	*) return 1 ;;
	esac
}

# Purpose: Replace every FROM in TEXT with TO, left to right as gsub does,
# without forking.
# Usage: zxfer_property_replace_all TEXT FROM TO; publishes
# g_zxfer_property_replace_result.
zxfer_property_replace_all() {
	g_zxfer_property_replace_result=""
	l_replace_rest=$1
	while :; do
		case $l_replace_rest in
		*"$2"*) ;;
		*) break ;;
		esac
		g_zxfer_property_replace_result=$g_zxfer_property_replace_result${l_replace_rest%%"$2"*}$3
		l_replace_rest=${l_replace_rest#*"$2"}
	done
	g_zxfer_property_replace_result=$g_zxfer_property_replace_result$l_replace_rest
}

# Purpose: Give the byte one percent code of the property-list encoding
# stands for.
# Usage: zxfer_property_code_byte 0D|0A|09|3B|3D|2C|25; publishes
# g_zxfer_property_code_byte.
zxfer_property_code_byte() {
	case $1 in
	0D) g_zxfer_property_code_byte=$ZXFER_CR ;;
	0A) g_zxfer_property_code_byte=$ZXFER_LF ;;
	09) g_zxfer_property_code_byte=$ZXFER_TAB ;;
	3B) g_zxfer_property_code_byte=';' ;;
	3D) g_zxfer_property_code_byte='=' ;;
	2C) g_zxfer_property_code_byte=',' ;;
	25) g_zxfer_property_code_byte='%' ;;
	esac
}

# Purpose: Percent-encode one raw property value without forking, as the AWK
# encode_value does (% first).
# Usage: zxfer_encode_property_value VALUE; publishes
# g_zxfer_encoded_property_value.
zxfer_encode_property_value() {
	g_zxfer_encoded_property_value=$1
	for l_encode_code in 25 2C 3D 3B 09 0D 0A; do
		zxfer_property_code_byte "$l_encode_code"
		zxfer_property_replace_all "$g_zxfer_encoded_property_value" \
			"$g_zxfer_property_code_byte" "%$l_encode_code"
		g_zxfer_encoded_property_value=$g_zxfer_property_replace_result
	done
}

# Purpose: Decode one percent-encoded property value without forking.
# Usage: zxfer_decode_property_value ENCODED; publishes
# g_zxfer_decoded_property_value. Codes are replaced in the order %0D %0A %09
# %3B %3D %2C %25, the order of the AWK decode_value.
zxfer_decode_property_value() {
	g_zxfer_decoded_property_value=$1
	case $1 in
	*%*) ;;
	*) return 0 ;;
	esac
	for l_decode_code in 0D 0A 09 3B 3D 2C 25; do
		zxfer_property_code_byte "$l_decode_code"
		zxfer_property_replace_all "$g_zxfer_decoded_property_value" \
			"%$l_decode_code" "$g_zxfer_property_code_byte"
		g_zxfer_decoded_property_value=$g_zxfer_property_replace_result
	done
}

# Purpose: Decode a serialized property list for verbose apply logging.
# Usage: zxfer_decode_serialized_property_list_for_display LIST; publishes the
# decoded "property=value[=source]" list in
# g_zxfer_property_display_list_result.
zxfer_decode_serialized_property_list_for_display() {
	g_zxfer_property_display_list_result=""
	l_display_rest=$1,
	while [ -n "$l_display_rest" ]; do
		l_display_item=${l_display_rest%%,*}
		l_display_rest=${l_display_rest#*,}
		[ -n "$l_display_item" ] || continue
		# Values are encoded, so an item holds at most two "=": the value
		# sits between the name and an optional source.
		l_display_value=""
		l_display_source=""
		case $l_display_item in
		*=*=*)
			l_display_value=${l_display_item#*=}
			l_display_source="=${l_display_value##*=}"
			l_display_value=${l_display_value%=*}
			;;
		*=*) l_display_value=${l_display_item#*=} ;;
		esac
		zxfer_decode_property_value "$l_display_value"
		l_display_item=${l_display_item%%=*}=$g_zxfer_decoded_property_value$l_display_source
		g_zxfer_property_display_list_result=${g_zxfer_property_display_list_result:+$g_zxfer_property_display_list_result,}$l_display_item
	done
}

# Purpose: Look up one property in a serialized property list.
# Usage: zxfer_property_list_value LIST NAME; returns 0 and publishes the text
# after the first "NAME=" (the encoded value, then "=source" when the list
# carries sources) in g_zxfer_property_list_value_result, or returns 1.
zxfer_property_list_value() {
	g_zxfer_property_list_value_result=""
	case ",$1," in
	*",$2="*) ;;
	*) return 1 ;;
	esac
	# Split on commas instead of cutting at the match: a ${1#*,NAME=} cut is
	# quadratic in the list length under bash and dash.
	l_list_value_name=$2
	zxfer_split_begin ,
	# shellcheck disable=SC2086  # Intentional comma splitting.
	set -- $1
	zxfer_split_end
	for l_list_value_item; do
		case $l_list_value_item in
		"$l_list_value_name="*)
			g_zxfer_property_list_value_result=${l_list_value_item#"$l_list_value_name="}
			return 0
			;;
		esac
	done
	return 1
}

################################################################################
# IN-MEMORY PROPERTY TABLES
################################################################################

# Purpose: Find one dataset's row in one side's table.
# Usage: zxfer_property_table_find_dataset source|destination DATASET;
# publishes the list in g_zxfer_property_table_lookup_result and returns
# non-zero on a miss. The first matching row wins, so fresh prefetch rows
# prepended ahead of older live rows stay authoritative.
zxfer_property_table_find_dataset() {
	l_find_dataset=$2

	g_zxfer_property_table_lookup_result=""
	case $1 in
	source) l_find_table=${g_zxfer_source_property_table:-} ;;
	destination) l_find_table=${g_zxfer_destination_property_table:-} ;;
	*) return 1 ;;
	esac
	[ -n "$l_find_table" ] || return 1

	# Read one row at a time: matching a glob against the whole table becomes
	# expensive on larger trees.
	while IFS=$ZXFER_TAB read -r l_find_name l_find_payload; do
		[ "$l_find_name" = "$l_find_dataset" ] || continue
		[ -n "$l_find_payload" ] || return 1
		g_zxfer_property_table_lookup_result=$l_find_payload
		return 0
	done <<EOF
$l_find_table
EOF
	return 1
}

# Purpose: Reset the destination table and re-arm its prefetch so freshly
# seeded destinations are re-read while source rows stay warm.
# Usage: Called before the post-seed property reconcile pass.
zxfer_reset_destination_property_iteration_cache() {
	g_zxfer_destination_property_table=""
	g_zxfer_destination_property_tree_prefetch_state=0
}

# Purpose: Drop the destination table rows that one mutation may have changed.
# Usage: zxfer_invalidate_destination_property_mutation_cache [DATASET
# [subtree|exact]]; without DATASET the whole destination table is reset.
# subtree (the default, for create, set and inherit) strips DATASET and its
# descendants, whose inherited values may have changed. exact (for receive)
# strips DATASET only: zxfer's receive never carries properties, because its
# send has no -p or -R and its receive no -o or -x (a -w raw stream carries
# only encryption settings, which are on the readonly list). A failed strip
# empties the table, which only forces live reads. Snapshot-view dirtiness is
# tracked separately by zxfer_mark_live_destination_dataset_dirty.
zxfer_invalidate_destination_property_mutation_cache() {
	if [ -z "${1:-}" ]; then
		zxfer_reset_destination_property_iteration_cache
		return 0
	fi
	[ -n "${g_zxfer_destination_property_table:-}" ] || return 0
	l_invalidate_subtree=1
	[ "${2:-subtree}" != exact ] || l_invalidate_subtree=0

	# The dataset travels through the environment: awk -v would reinterpret
	# backslash escapes in hostile dataset names.
	# shellcheck disable=SC2016
	g_zxfer_destination_property_table=$(
		ZXFER_AWK_STRIP_DATASET=$1 "${g_cmd_awk:-awk}" -F "$ZXFER_TAB" \
			-v subtree="$l_invalidate_subtree" '
BEGIN {
	dataset = ENVIRON["ZXFER_AWK_STRIP_DATASET"]
	prefix = dataset "/"
}
$0 == "" || $1 == dataset { next }
subtree == 1 && substr($1, 1, length(prefix)) == prefix { next }
{ print }
' <<EOF
$g_zxfer_destination_property_table
EOF
	) || g_zxfer_destination_property_table=""
}

################################################################################
# LIVE READS / RECURSIVE PREFETCH / NORMALIZED LOOKUP
################################################################################

# Purpose: Allocate the scratch files property reads reuse: the skeleton, the
# machine and human views, zfs stderr, and the prefetch dataset filter.
# Usage: zxfer_prepare_property_read_files; allocates them once per run (every
# read overwrites them) and throws when a file cannot be created. The run-root
# removal deletes them.
zxfer_prepare_property_read_files() {
	[ -z "${g_zxfer_property_skeleton_file:-}" ] || return 0
	zxfer_create_temp_file_group 5 || return "$?"
	{
		IFS= read -r g_zxfer_property_skeleton_file
		IFS= read -r g_zxfer_property_machine_file
		IFS= read -r g_zxfer_property_human_file
		IFS= read -r g_zxfer_property_error_file
		IFS= read -r g_zxfer_property_wanted_file
	} <<EOF
$g_zxfer_temp_file_group_result
EOF
}

# Purpose: Read one property of one dataset alone: a single record is
# unambiguous whatever its value holds.
# Usage: zxfer_read_one_property source|destination DATASET PROPERTY -Hpo|-Ho;
# publishes g_zxfer_property_record_value and g_zxfer_property_record_source.
# A zfs failure returns its status with the zfs stderr in
# g_zxfer_property_error_result; output that is not one PROPERTY record
# returns 1 with a short reason there.
zxfer_read_one_property() {
	zxfer_prepare_property_read_files || return "$?"
	g_zxfer_property_error_result=""
	l_one_status=0
	# A user property name may start with "-" (for example -x:y), which zfs
	# would parse as an option, so -- ends the options first.
	l_one_output=$(zxfer_run_zfs_cmd_for_role "$1" get "$4" property,value,source -- "$3" "$2" \
		2>"$g_zxfer_property_error_file" </dev/null) || l_one_status=$?
	if [ "$l_one_status" -ne 0 ]; then
		zxfer_read_runtime_artifact_file_trimmed "$g_zxfer_property_error_file" || :
		g_zxfer_property_error_result=$g_zxfer_runtime_artifact_read_result
		return "$l_one_status"
	fi
	zxfer_split_property_record "$3" "$l_one_output" && return 0
	g_zxfer_property_error_result="zfs get printed no single [$3] record"
	return 1
}

# Purpose: Read one side's whole recursive property tree with three
# `zfs get -r` calls (machine and human views, then the skeleton) and load
# each wanted dataset whose records are all known into that side's table.
# Usage: zxfer_prefetch_recursive_normalized_properties source|destination;
# runs at most once per side per iteration (state 0 armed, 1 done, 2 failed)
# and returns non-zero when nothing could be published. A dataset left out,
# because a read failed or a multi-line value leaves its records ambiguous,
# is read live on lookup.
zxfer_prefetch_recursive_normalized_properties() {
	l_prefetch_side=$1

	case $l_prefetch_side in
	source)
		l_prefetch_state=${g_zxfer_source_property_tree_prefetch_state:-0}
		l_prefetch_root=${g_zxfer_source_property_tree_prefetch_root:-}
		l_prefetch_datasets=${g_recursive_source_dataset_list:-${g_recursive_source_list:-${g_initial_source:-}}}
		;;
	destination)
		l_prefetch_state=${g_zxfer_destination_property_tree_prefetch_state:-0}
		l_prefetch_root=${g_zxfer_destination_property_tree_prefetch_root:-}
		l_prefetch_datasets=${g_recursive_dest_list:-}
		;;
	*)
		return 1
		;;
	esac
	case $l_prefetch_state in
	1) return 0 ;;
	2) return 1 ;;
	esac

	# Mark the side failed first; only a complete publish flips it to done.
	case $l_prefetch_side in
	source) g_zxfer_source_property_tree_prefetch_state=2 ;;
	destination) g_zxfer_destination_property_tree_prefetch_state=2 ;;
	esac
	[ -n "$l_prefetch_root" ] || return 1
	case $l_prefetch_datasets in
	*[![:space:]]*) ;;
	*) return 1 ;;
	esac

	zxfer_prepare_property_read_files || return "$?"
	# The dataset lists hold one dataset per line.
	printf '%s\n' "$l_prefetch_datasets" >"$g_zxfer_property_wanted_file" || return 1

	zxfer_profile_increment_counter "g_zxfer_profile_normalized_property_reads_$l_prefetch_side"
	# -t keeps snapshots, whose rows the filter would drop anyway, out of the
	# recursive listing. The skeleton comes last, so a dataset destroyed
	# before it is not listed and no value can pass for its records.
	l_prefetch_status=0
	zxfer_run_zfs_cmd_for_role "$l_prefetch_side" get -r -t filesystem,volume \
		-Hpo name,property,value,source all "$l_prefetch_root" \
		>"$g_zxfer_property_machine_file" 2>/dev/null </dev/null &&
		zxfer_run_zfs_cmd_for_role "$l_prefetch_side" get -r -t filesystem,volume \
			-Ho name,property,value,source all "$l_prefetch_root" \
			>"$g_zxfer_property_human_file" 2>/dev/null </dev/null &&
		zxfer_run_zfs_cmd_for_role "$l_prefetch_side" get -r -t filesystem,volume \
			-Ho name,property all "$l_prefetch_root" \
			>"$g_zxfer_property_skeleton_file" 2>/dev/null </dev/null &&
		l_prefetch_table=$(zxfer_parse_property_views prefetch "$g_zxfer_property_wanted_file" \
			"$g_zxfer_property_skeleton_file" "$g_zxfer_property_machine_file" \
			"$g_zxfer_property_human_file") ||
		l_prefetch_status=$?
	[ "$l_prefetch_status" -eq 0 ] || return "$l_prefetch_status"

	# Fresh prefetch rows precede earlier live rows so first-match lookup keeps
	# the new tree authoritative.
	case $l_prefetch_side in
	source)
		g_zxfer_source_property_table=$l_prefetch_table${g_zxfer_source_property_table:+$ZXFER_LF$g_zxfer_source_property_table}
		g_zxfer_source_property_tree_prefetch_state=1
		;;
	destination)
		g_zxfer_destination_property_table=$l_prefetch_table${g_zxfer_destination_property_table:+$ZXFER_LF$g_zxfer_destination_property_table}
		g_zxfer_destination_property_tree_prefetch_state=1
		;;
	esac
}

# Purpose: Read one dataset's properties live: the machine (-p) and human
# views, then the skeleton they are parsed against, then each property the
# views leave ambiguous on its own (a user property gone by then is left out).
# Usage: zxfer_read_live_dataset_properties DATASET source|destination;
# publishes the list in g_zxfer_normalized_dataset_properties, or returns
# non-zero with the diagnostic in g_zxfer_property_error_result.
zxfer_read_live_dataset_properties() {
	l_live_dataset=$1
	l_live_side=$2

	zxfer_prepare_property_read_files || return "$?"
	l_live_status=0
	zxfer_run_zfs_cmd_for_role "$l_live_side" get -Hpo property,value,source all "$l_live_dataset" \
		>"$g_zxfer_property_machine_file" 2>"$g_zxfer_property_error_file" </dev/null &&
		zxfer_run_zfs_cmd_for_role "$l_live_side" get -Ho property,value,source all "$l_live_dataset" \
			>"$g_zxfer_property_human_file" 2>"$g_zxfer_property_error_file" </dev/null &&
		zxfer_run_zfs_cmd_for_role "$l_live_side" get -Ho property all "$l_live_dataset" \
			>"$g_zxfer_property_skeleton_file" 2>"$g_zxfer_property_error_file" </dev/null ||
		l_live_status=$?
	if [ "$l_live_status" -ne 0 ]; then
		zxfer_read_runtime_artifact_file_trimmed "$g_zxfer_property_error_file" || :
		g_zxfer_property_error_result=$g_zxfer_runtime_artifact_read_result
		return "$l_live_status"
	fi
	l_live_parsed=$(zxfer_parse_property_views merge "$g_zxfer_property_skeleton_file" \
		"$g_zxfer_property_machine_file" "$g_zxfer_property_human_file") || {
		g_zxfer_property_error_result="Failed to parse the properties of dataset [$l_live_dataset]: the zfs get property list is malformed, repeats a name, or does not match the values."
		return 1
	}

	# Line 1 is the payload, line 2 the names still to read one at a time.
	l_live_payload=${l_live_parsed%%"$ZXFER_LF"*}
	l_live_reread=${l_live_parsed#"$l_live_payload"}
	l_live_reread=${l_live_reread#"$ZXFER_LF"}
	if [ -z "$l_live_reread" ]; then
		g_zxfer_normalized_dataset_properties=$l_live_payload
		return 0
	fi
	zxfer_echoV "Reading properties [$l_live_reread] of [$l_live_dataset] one at a time: their zfs get records are ambiguous."
	g_zxfer_normalized_dataset_properties=""
	l_live_rest=$l_live_payload,
	while [ -n "$l_live_rest" ]; do
		l_live_item=${l_live_rest%%,*}
		l_live_rest=${l_live_rest#*,}
		case $l_live_item in
		*=* | "") ;;
		*)
			# A bare name: the human view says "none", the machine view
			# gives the value and source.
			zxfer_read_one_property "$l_live_side" "$l_live_dataset" "$l_live_item" -Ho ||
				l_live_status=$?
			if [ "$l_live_status" -eq 0 ]; then
				l_live_human_value=$g_zxfer_property_record_value
				zxfer_read_one_property "$l_live_side" "$l_live_dataset" "$l_live_item" -Hpo ||
					l_live_status=$?
			fi
			if [ "$l_live_status" -ne 0 ]; then
				g_zxfer_normalized_dataset_properties=""
				g_zxfer_property_error_result="Failed to read property [$l_live_item] of dataset [$l_live_dataset]: $g_zxfer_property_error_result"
				return "$l_live_status"
			fi
			# zfs gives a user property removed since the name list the
			# source "-", which a set user property never has: leave it out,
			# as if the read had begun after the removal.
			case $l_live_item in
			*:*)
				if [ "$g_zxfer_property_record_source" = - ]; then
					zxfer_echoV "Property [$l_live_item] of [$l_live_dataset] was removed during the read; leaving it out."
					continue
				fi
				;;
			esac
			[ "$l_live_human_value" != none ] || g_zxfer_property_record_value=none
			zxfer_encode_property_value "$g_zxfer_property_record_value"
			l_live_item=$l_live_item=$g_zxfer_encoded_property_value=$g_zxfer_property_record_source
			;;
		esac
		[ -z "$l_live_item" ] ||
			g_zxfer_normalized_dataset_properties=${g_zxfer_normalized_dataset_properties:+$g_zxfer_normalized_dataset_properties,}$l_live_item
	done
}

# Purpose: Load one dataset's normalized property list from the side's table,
# the recursive prefetch, or a live read.
# Usage: zxfer_load_normalized_dataset_properties DATASET source|destination;
# the side selects the table, the profiling counter, and the zfs command role
# (so reads reach the -O or -T host). Publishes the list in
# g_zxfer_normalized_dataset_properties (a table hit sets
# g_zxfer_normalized_dataset_properties_cache_hit) and, on failure, the
# diagnostic in g_zxfer_property_error_result with a non-zero status.
zxfer_load_normalized_dataset_properties() {
	l_load_dataset=$1
	l_load_side=$2

	g_zxfer_normalized_dataset_properties=""
	g_zxfer_normalized_dataset_properties_cache_hit=0
	g_zxfer_property_error_result=""

	if zxfer_property_table_find_dataset "$l_load_side" "$l_load_dataset" ||
		{ zxfer_prefetch_recursive_normalized_properties "$l_load_side" &&
			zxfer_property_table_find_dataset "$l_load_side" "$l_load_dataset"; }; then
		g_zxfer_normalized_dataset_properties=$g_zxfer_property_table_lookup_result
		g_zxfer_normalized_dataset_properties_cache_hit=1
		return 0
	fi
	zxfer_profile_increment_counter "g_zxfer_profile_normalized_property_reads_$l_load_side"
	zxfer_read_live_dataset_properties "$l_load_dataset" "$l_load_side" || return "$?"

	[ -n "$g_zxfer_normalized_dataset_properties" ] || return 0
	l_load_row=$l_load_dataset$ZXFER_TAB$g_zxfer_normalized_dataset_properties
	case $l_load_side in
	source) g_zxfer_source_property_table=${g_zxfer_source_property_table:+$g_zxfer_source_property_table$ZXFER_LF}$l_load_row ;;
	destination) g_zxfer_destination_property_table=${g_zxfer_destination_property_table:+$g_zxfer_destination_property_table$ZXFER_LF}$l_load_row ;;
	esac
}

################################################################################
# REQUIRED CREATION-TIME PROPERTY BACKFILL
################################################################################

# Purpose: Probe one required creation-time property that `zfs get all` did
# not report.
# Usage: zxfer_probe_required_property DATASET PROPERTY source|destination;
# publishes the serialized item in g_zxfer_required_property_probe_result
# (empty when the platform reports the property as inapplicable or unknown)
# or the failure message in g_zxfer_property_error_result with a non-zero
# status.
zxfer_probe_required_property() {
	l_probe_dataset=$1
	l_probe_property=$2

	g_zxfer_required_property_probe_result=""
	zxfer_profile_increment_counter g_zxfer_profile_required_property_backfill_gets
	l_probe_status=0
	zxfer_read_one_property "$3" "$l_probe_dataset" "$l_probe_property" -Hpo ||
		l_probe_status=$?
	if [ "$l_probe_status" -ne 0 ]; then
		case $g_zxfer_property_error_result in
		*"does not apply"* | *"invalid property"* | *"no such property"* | *"not supported"*)
			g_zxfer_property_error_result=""
			return 0
			;;
		esac
		g_zxfer_property_error_result="Failed to retrieve required creation-time property [$l_probe_property] for dataset [$l_probe_dataset]: $g_zxfer_property_error_result"
		return "$l_probe_status"
	fi
	zxfer_encode_property_value "$g_zxfer_property_record_value"
	g_zxfer_required_property_probe_result=$l_probe_property=$g_zxfer_encoded_property_value=$g_zxfer_property_record_source
}

# Purpose: Append any required creation-time property missing from a property
# list so the diff can enforce creation-time rules on every platform (some
# OpenZFS builds omit them from `zfs get all`).
# Usage: zxfer_backfill_required_properties DATASET LIST REQUIRED_CSV
# source|destination [SIBLING_LIST]; a missing property is copied from the
# sibling list when it has one, otherwise probed with one comma-list
# `zfs get` (then per property when that call fails or answers incompletely).
# Publishes the extended list in g_zxfer_required_properties_result, or the
# failure message in g_zxfer_property_error_result with a non-zero status.
zxfer_backfill_required_properties() {
	l_backfill_dataset=$1
	l_backfill_side=$4
	l_backfill_sibling=${5:-}

	g_zxfer_required_properties_result=$2
	g_zxfer_property_error_result=""
	l_backfill_missing=""
	l_backfill_rest=$3,
	while [ -n "$l_backfill_rest" ]; do
		l_backfill_name=${l_backfill_rest%%,*}
		l_backfill_rest=${l_backfill_rest#*,}
		[ -n "$l_backfill_name" ] || continue
		case ",$g_zxfer_required_properties_result," in
		*",$l_backfill_name="*) continue ;;
		esac
		if zxfer_property_list_value "$l_backfill_sibling" "$l_backfill_name"; then
			g_zxfer_required_properties_result=${g_zxfer_required_properties_result:+$g_zxfer_required_properties_result,}$l_backfill_name=$g_zxfer_property_list_value_result
			continue
		fi
		l_backfill_missing=${l_backfill_missing:+$l_backfill_missing,}$l_backfill_name
	done
	[ -n "$l_backfill_missing" ] || return 0

	case $l_backfill_missing in
	*,*)
		zxfer_profile_increment_counter g_zxfer_profile_required_property_backfill_gets
		if l_backfill_batch=$(zxfer_run_zfs_cmd_for_role "$l_backfill_side" get -Hpo property,value,source \
			"$l_backfill_missing" "$l_backfill_dataset" 2>/dev/null </dev/null) &&
			zxfer_append_required_properties_from_capture "$l_backfill_missing" "$l_backfill_batch"; then
			return 0
		fi
		;;
	esac

	l_backfill_rest=$l_backfill_missing,
	while [ -n "$l_backfill_rest" ]; do
		l_backfill_name=${l_backfill_rest%%,*}
		l_backfill_rest=${l_backfill_rest#*,}
		zxfer_probe_required_property "$l_backfill_dataset" "$l_backfill_name" "$l_backfill_side" ||
			return "$?"
		[ -n "$g_zxfer_required_property_probe_result" ] || continue
		g_zxfer_required_properties_result=${g_zxfer_required_properties_result:+$g_zxfer_required_properties_result,}$g_zxfer_required_property_probe_result
	done
}

# Purpose: Append the named properties from one comma-list `zfs get -Hpo
# property,value,source` capture to g_zxfer_required_properties_result. The
# names are the skeleton: zfs prints them in order, so the capture must hold
# exactly one record line per name.
# Usage: zxfer_append_required_properties_from_capture NAMES_CSV CAPTURE;
# returns 1 without publishing on any other shape, so the backfill falls back
# to per-property probes.
zxfer_append_required_properties_from_capture() {
	l_append_result=$g_zxfer_required_properties_result
	l_append_names=$1,
	l_append_lines=$2$ZXFER_LF
	while [ -n "$l_append_names" ]; do
		l_append_name=${l_append_names%%,*}
		l_append_names=${l_append_names#*,}
		l_append_line=${l_append_lines%%"$ZXFER_LF"*}
		l_append_lines=${l_append_lines#*"$ZXFER_LF"}
		zxfer_split_property_record "$l_append_name" "$l_append_line" || return 1
		zxfer_encode_property_value "$g_zxfer_property_record_value"
		l_append_result=${l_append_result:+$l_append_result,}$l_append_name=$g_zxfer_encoded_property_value=$g_zxfer_property_record_source
	done
	[ -z "$l_append_lines" ] || return 1
	g_zxfer_required_properties_result=$l_append_result
}
