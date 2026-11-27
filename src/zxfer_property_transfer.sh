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
# PROPERTY TRANSFER: POLICY, PLAN, CREATE, APPLY
################################################################################

# Module contract:
# owns globals: the readonly, noninheritable and required-creation property
#   constants, the filter, plan core, single/batch plan and child AWK programs,
#   the run-wide unsupported-property lists (g_zxfer_unsupported_*_properties),
#   the prepared g_zxfer_property_readonly_policy/g_zxfer_property_override_policy,
#   reusable planner input, action, parent-work and batch-plan files
#   (g_zxfer_property_batch_plan_*),
#   result globals g_zxfer_override_properties_result,
#   g_zxfer_source_dataset_type_result, g_zxfer_source_volume_size_result,
#   g_zxfer_property_batch_plan_context_result,
#   g_zxfer_property_batch_plan_file_result,
#   and the small g_zxfer_property_plan_summary_result (action and override
#   counts), cleared before planning. It also writes the property state module's
#   failure text g_zxfer_property_error_result.
# reads globals: the property CLI options, the destination platform and -m
#   state, g_initial_source, g_actual_dest, the recursive
#   source and destination lists, the property tables and row directory, the
#   restore metadata, and the rendered -T command in
#   g_zxfer_shell_command_result.
# mutates caches: the unsupported-property lists, batch-plan state (reset on
#   destination/iteration cache resets), destination property rows
#   (dropped by the state module's invalidation after every create, set and
#   inherit), and the destination existence cache through probes.
# returns via stdout: diagnostic plan lists (zxfer_property_plan_list).
#   Property values remain encoded until command construction.
#
# Per dataset, zxfer_transfer_properties reads the source, checks the -o
# properties against the initial source, decides whether the destination
# exists, then consumes a provenance-checked batch plan (for existing
# prefetched pairs) or runs one plan awk: derive the apply and creation lists,
# and diff them against the destination when it exists. A missing destination
# is created; an existing one gets its sets and inherits, a child's after they
# are adjusted to what its destination parent already provides.

ZXFER_BASE_READONLY_PROPERTIES="type,creation,used,available,referenced,\
compressratio,mounted,version,primarycache,secondarycache,\
usedbysnapshots,usedbydataset,usedbychildren,usedbyrefreservation,\
volsize,mountpoint,mlslabel,keysource,keystatus,rekeydate,encryption,encryptionroot,keylocation,keyformat,pbkdf2iters,snapshots_changed,special_small_blocks,\
refcompressratio,written,logicalused,logicalreferenced,createtxg,guid,origin,\
filesystem_count,snapshot_count,clones,defer_destroy,receive_resume_token,\
userrefs,objsetid"
ZXFER_FREEBSD_READONLY_PROPERTIES="aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,\
xattr,dnodesize"
ZXFER_NONINHERITABLE_PROPERTIES="quota,reservation,canmount,refquota,refreservation"
# Creation-time properties a filesystem must match; volumes have none.
ZXFER_REQUIRED_CREATION_PROPERTIES="casesensitivity,normalization,utf8only"

# Purpose: Reset run-wide property policy and forget inherited planner scratch.
# Usage: Called at session initialization; never acts on inherited file paths.
zxfer_reset_property_runtime_state() {
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
	g_zxfer_property_readonly_policy=""
	g_zxfer_property_override_policy=""
	g_zxfer_property_input_file=""
	g_zxfer_property_plan_file=""
	g_zxfer_property_parent_plan_file=""
	g_zxfer_property_plan_summary_result=""
	g_zxfer_property_batch_plan_base=""
	g_zxfer_property_batch_plan_state=0
	g_zxfer_property_batch_plan_index=""
	g_zxfer_property_batch_plan_context=""
	g_zxfer_property_batch_plan_context_result=""
	g_zxfer_property_batch_plan_file_result=""
}

# Purpose: Resolve the readonly property list for the destination platform,
# without mountpoint when -m migration must move mountpoints.
# Usage: zxfer_prepare_readonly_property_policy, once after endpoint startup;
# publishes g_zxfer_property_readonly_policy for every dataset and -Y pass.
zxfer_prepare_readonly_property_policy() {
	g_zxfer_property_readonly_policy=$ZXFER_BASE_READONLY_PROPERTIES
	if [ "${g_destination_operating_system:-}" = "FreeBSD" ] &&
		[ -n "$ZXFER_FREEBSD_READONLY_PROPERTIES" ]; then
		g_zxfer_property_readonly_policy=${g_zxfer_property_readonly_policy:+$g_zxfer_property_readonly_policy,}$ZXFER_FREEBSD_READONLY_PROPERTIES
	fi
	[ "${g_option_m_migrate:-0}" -eq 1 ] || return 0

	l_readonly_rest=$g_zxfer_property_readonly_policy,
	g_zxfer_property_readonly_policy=""
	while [ -n "$l_readonly_rest" ]; do
		l_readonly_name=${l_readonly_rest%%,*}
		l_readonly_rest=${l_readonly_rest#*,}
		case $l_readonly_name in
		"" | mountpoint) continue ;;
		esac
		g_zxfer_property_readonly_policy=${g_zxfer_property_readonly_policy:+$g_zxfer_property_readonly_policy,}$l_readonly_name
	done
}

################################################################################
# -o OVERRIDE LIST
################################################################################

# Purpose: Parse the -o text into a serialized override list: an item without
# "NAME=" is a syntax error, and a property named twice is refused, as zfs
# refuses a property given twice.
# Usage: zxfer_read_override_properties TEXT; publishes the
# "property=value=override" items, in -o order with names and values
# percent-encoded, in g_zxfer_override_properties_result, or throws a usage
# error. Commas separate items and "\," is a literal comma. CLI validation
# calls it before any zfs command runs and keeps the validated list in
# g_zxfer_property_override_policy, separate from this scratch result.
zxfer_read_override_properties() {
	g_zxfer_override_properties_result=""
	[ -n "$1" ] || return 0

	# Split on commas. A piece that ends in a backslash was cut at an escaped
	# comma, so it joins the next piece with a literal comma in place of that
	# backslash; the trailing comma added here only ends the last piece.
	l_override_rest=$1,
	l_override_item=""
	while [ -n "$l_override_rest" ]; do
		l_override_piece=${l_override_rest%%,*}
		l_override_rest=${l_override_rest#*,}
		case $l_override_piece in
		*\\)
			if [ -n "$l_override_rest" ]; then
				l_override_item=$l_override_item${l_override_piece%\\},
				continue
			fi
			;;
		esac
		l_override_item=$l_override_item$l_override_piece
		[ -n "$l_override_item" ] || continue

		l_override_name=${l_override_item%%=*}
		if [ -z "$l_override_name" ] || [ "$l_override_name" = "$l_override_item" ]; then
			zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
		fi
		# The name is encoded too, so a comma or line feed in it cannot break
		# the list; such a name never matches a real property.
		zxfer_encode_property_value "$l_override_name"
		l_override_key=$g_zxfer_encoded_property_value
		case ",$g_zxfer_override_properties_result," in
		*",$l_override_key="*)
			zxfer_escape_report_value_into_result "$l_override_name" ||
				g_zxfer_escaped_report_value_result="[unprintable]"
			zxfer_throw_usage_error "Duplicate property for -o override: $g_zxfer_escaped_report_value_result."
			;;
		esac
		zxfer_encode_property_value "${l_override_item#*=}"
		g_zxfer_override_properties_result=${g_zxfer_override_properties_result:+$g_zxfer_override_properties_result,}$l_override_key=$g_zxfer_encoded_property_value=override
		l_override_item=""
	done
}

# Purpose: Require every -o property on the initial source, before the
# destination is read, probed or changed.
# Usage: zxfer_check_override_properties_on_source OVERRIDE_LIST SOURCE_PVS;
# throws a usage error naming the first -o property the source list lacks.
zxfer_check_override_properties_on_source() {
	l_override_check_rest=$1,
	while [ -n "$l_override_check_rest" ]; do
		l_override_check_item=${l_override_check_rest%%,*}
		l_override_check_rest=${l_override_check_rest#*,}
		[ -n "$l_override_check_item" ] || continue
		l_override_check_name=${l_override_check_item%%=*}
		case ",$2," in
		*",$l_override_check_name="*) continue ;;
		esac
		zxfer_decode_property_value "$l_override_check_name"
		zxfer_escape_report_value_into_result "$g_zxfer_decoded_property_value" ||
			g_zxfer_escaped_report_value_result="[unprintable]"
		zxfer_throw_usage_error "Missing source property for -o override: $g_zxfer_escaped_report_value_result."
	done
}

################################################################################
# UNSUPPORTED-PROPERTY SCAN (-U)
################################################################################

# Purpose: Record one property as unsupported on the destination for one
# source dataset type, without duplicates.
# Usage: zxfer_append_unsupported_property filesystem|volume PROPERTY.
zxfer_append_unsupported_property() {
	case $1 in
	volume) l_unsupported_existing=${g_zxfer_unsupported_volume_properties:-} ;;
	*) l_unsupported_existing=${g_zxfer_unsupported_filesystem_properties:-} ;;
	esac
	case ",$l_unsupported_existing," in
	*",$2,"*) return 0 ;;
	esac
	l_unsupported_existing=${l_unsupported_existing:+$l_unsupported_existing,}$2
	case $1 in
	volume) g_zxfer_unsupported_volume_properties=$l_unsupported_existing ;;
	*) g_zxfer_unsupported_filesystem_properties=$l_unsupported_existing ;;
	esac
}

# Purpose: Probe destination support for all source properties of one dataset
# type, recording unsupported names via zxfer_append_unsupported_property.
# Usage: zxfer_probe_unsupported_properties_for_type SOURCE TYPE TYPES_TABLE;
# SOURCE is the first source dataset of TYPE, and TYPES_TABLE is the
# newline-separated "dataset<TAB>type" list used to pick an existing
# destination probe dataset of that type (falling back to the destination pool
# root). Every remote call inside a loop reads /dev/null.
zxfer_probe_unsupported_properties_for_type() {
	l_type_probe_source=$1
	l_type_probe_type=$2
	l_type_probe_types=$3

	l_type_probe_status=0
	l_type_probe_source_names=$(zxfer_run_zfs_cmd_capturing_stderr source merge \
		get -Hpo property all "$l_type_probe_source" </dev/null) ||
		l_type_probe_status=$?
	[ "$l_type_probe_status" -eq 0 ] ||
		zxfer_throw_error "Failed to retrieve source property list for dataset [$l_type_probe_source]: $l_type_probe_source_names" "$l_type_probe_status"

	# The existence cache is seeded from the recursive destination
	# listing, so these probes normally answer without a zfs call.
	l_type_probe_dest=""
	while IFS=$ZXFER_TAB read -r l_type_probe_row_source l_type_probe_row_type; do
		[ "$l_type_probe_row_type" = "$l_type_probe_type" ] || continue
		zxfer_map_destination_dataset "$l_type_probe_row_source"
		[ -n "$l_type_probe_dest" ] || l_type_probe_dest=${g_zxfer_destination_dataset_result%%/*}
		if zxfer_destination_exists "$g_zxfer_destination_dataset_result" </dev/null; then
			l_type_probe_dest=$g_zxfer_destination_dataset_result
			break
		fi
	done <<EOF
$l_type_probe_types
EOF
	[ -n "$l_type_probe_dest" ] ||
		zxfer_throw_error "Failed to determine the destination property-support probe dataset."

	l_type_probe_status=0
	l_type_probe_dest_type=$(zxfer_run_zfs_cmd_capturing_stderr destination merge \
		get -Hpo value type "$l_type_probe_dest" </dev/null) ||
		l_type_probe_status=$?
	[ "$l_type_probe_status" -eq 0 ] ||
		zxfer_throw_error "Failed to determine the destination property-support probe dataset type for [$l_type_probe_dest]: $l_type_probe_dest_type" "$l_type_probe_status"
	l_type_probe_dest_names=$(zxfer_run_zfs_cmd_capturing_stderr destination merge \
		get -Hpo property all "$l_type_probe_dest" </dev/null) ||
		l_type_probe_status=$?
	[ "$l_type_probe_status" -eq 0 ] ||
		zxfer_throw_error "Failed to retrieve destination property list for dataset [$l_type_probe_dest]: $l_type_probe_dest_names" "$l_type_probe_status"

	# shellcheck disable=SC2016
	l_type_probe_candidates=$(
		ZXFER_AWK_DESTINATION_PROPERTIES=$l_type_probe_dest_names
		export ZXFER_AWK_DESTINATION_PROPERTIES
		"${g_cmd_awk:-awk}" '
BEGIN {
	count = split(ENVIRON["ZXFER_AWK_DESTINATION_PROPERTIES"], names, "\n")
	for (i = 1; i <= count; i++)
		supported[names[i]] = 1
}
$0 != "" && !($0 in supported) && index($0, ":") == 0 && !seen[$0]++ { print }
' <<EOF
$l_type_probe_source_names
EOF
	) || zxfer_throw_error "Failed to compare source and destination property inventories for [$l_type_probe_dest]."

	# zfs reports "does not apply" for a property of another dataset type,
	# so that answer only counts when the probe has the source's type.
	while IFS= read -r l_type_probe_property; do
		[ -n "$l_type_probe_property" ] || continue
		l_type_probe_status=0
		l_type_probe_output=$(zxfer_run_zfs_cmd_capturing_stderr destination merge \
			get -Hpo property,value,source \
			"$l_type_probe_property" "$l_type_probe_dest" </dev/null) || l_type_probe_status=$?
		[ "$l_type_probe_status" -ne 0 ] || continue
		case $l_type_probe_output in
		*"invalid property"* | *"no such property"* | *"not supported"*)
			zxfer_append_unsupported_property "$l_type_probe_type" "$l_type_probe_property"
			continue
			;;
		*"does not apply"*)
			[ "$l_type_probe_dest_type" != "$l_type_probe_type" ] ||
				zxfer_append_unsupported_property "$l_type_probe_type" "$l_type_probe_property"
			continue
			;;
		esac
		if zxfer_destination_probe_is_ambiguous "$l_type_probe_output"; then
			l_type_probe_output="probe exited nonzero without stdout/stderr"
		fi
		zxfer_throw_error "Failed to probe destination support for property [$l_type_probe_property] on [$l_type_probe_dest]: $l_type_probe_output" "$l_type_probe_status"
	done <<EOF
$l_type_probe_candidates
EOF
}

# Purpose: Record, per source dataset type, the source properties the
# destination cannot accept, so -U can drop them before create and set.
# Usage: Called once at replication startup when -U has property work. Source
# types come from batched `zfs get type` calls of at most 128 names each. For
# each type, zxfer_probe_unsupported_properties_for_type compares the first
# source dataset's inventory with a destination probe dataset and probes only
# missing non-user names. Every remote call inside a here-document loop reads
# /dev/null, so an -O or -T ssh cannot drain the loop's input.
zxfer_calculate_unsupported_properties() {
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
	l_scan_sources=${g_recursive_source_list:-$g_initial_source}
	[ -n "$l_scan_sources" ] || return 0

	l_scan_types=""
	set --
	l_scan_done=0
	while [ "$l_scan_done" -eq 0 ]; do
		if IFS= read -r l_scan_source; then
			[ -n "$l_scan_source" ] || continue
			set -- "$@" "$l_scan_source"
			[ "$#" -ge 128 ] || continue
		else
			l_scan_done=1
			[ "$#" -gt 0 ] || break
		fi
		l_scan_status=0
		l_scan_batch=$(zxfer_run_zfs_cmd_capturing_stderr source merge \
			get -Hpo name,value type "$@" </dev/null) ||
			l_scan_status=$?
		[ "$l_scan_status" -eq 0 ] ||
			zxfer_throw_error "Failed to retrieve source dataset types for unsupported-property scan: $l_scan_batch" "$l_scan_status"
		l_scan_types=${l_scan_types:+$l_scan_types$ZXFER_LF}$l_scan_batch
		set --
	done <<EOF
$l_scan_sources
EOF

	l_scan_seen_types=""
	while IFS=$ZXFER_TAB read -r l_scan_source l_scan_type; do
		[ -n "$l_scan_source" ] && [ -n "$l_scan_type" ] || continue
		case ",$l_scan_seen_types," in
		*",$l_scan_type,"*) continue ;;
		esac
		l_scan_seen_types=${l_scan_seen_types:+$l_scan_seen_types,}$l_scan_type
		zxfer_probe_unsupported_properties_for_type "$l_scan_source" "$l_scan_type" "$l_scan_types" </dev/null
	done <<EOF
$l_scan_types
EOF
}

################################################################################
# SOURCE SIDE
################################################################################

# Purpose: Read a source property from a loaded list or fall back to a live
# `zfs get -Hpo value` query.
# Usage: zxfer_lookup_or_get_source_property SOURCE PVS PROPERTY LABEL;
# publishes g_zxfer_property_list_value_result, or returns the zfs status with
# g_zxfer_property_error_result set.
zxfer_lookup_or_get_source_property() {
	if zxfer_property_list_value "$2" "$3"; then
		g_zxfer_property_list_value_result=${g_zxfer_property_list_value_result%%=*}
		if [ -n "$g_zxfer_property_list_value_result" ]; then
			return 0
		fi
	fi
	l_lookup_status=0
	g_zxfer_property_list_value_result=$(zxfer_run_zfs_cmd_capturing_stderr source merge \
		get -Hpo value "$3" "$1") ||
		l_lookup_status=$?
	if [ "$l_lookup_status" -ne 0 ]; then
		g_zxfer_property_error_result="Failed to retrieve source $4 for [$1]: $g_zxfer_property_list_value_result"
		g_zxfer_property_list_value_result=""
		return "$l_lookup_status"
	fi
}

# Purpose: Resolve and validate the source dataset type plus the zvol size a
# volume needs at creation time.
# Usage: zxfer_get_validated_source_dataset_create_metadata SOURCE SOURCE_PVS;
# both values come from the loaded property list and are probed live only
# when the list lacks them. Publishes g_zxfer_source_dataset_type_result and
# g_zxfer_source_volume_size_result, or the failure message in
# g_zxfer_property_error_result with a non-zero status.
zxfer_get_validated_source_dataset_create_metadata() {
	l_metadata_source=$1
	l_metadata_pvs=$2

	g_zxfer_source_dataset_type_result=""
	g_zxfer_source_volume_size_result=""
	g_zxfer_property_error_result=""

	zxfer_lookup_or_get_source_property "$l_metadata_source" "$l_metadata_pvs" \
		type "dataset type" || return "$?"
	l_metadata_type=$g_zxfer_property_list_value_result

	case $l_metadata_type in
	filesystem) ;;
	volume)
		zxfer_lookup_or_get_source_property "$l_metadata_source" "$l_metadata_pvs" \
			volsize "zvol size" || return "$?"
		l_metadata_volsize=$g_zxfer_property_list_value_result
		if [ -z "$l_metadata_volsize" ] || [ "$l_metadata_volsize" = "-" ]; then
			g_zxfer_property_error_result="Failed to retrieve source zvol size for [$l_metadata_source]: empty volsize"
			return 1
		fi
		g_zxfer_source_volume_size_result=$l_metadata_volsize
		;;
	*)
		g_zxfer_property_error_result="Invalid source dataset type for [$l_metadata_source]: $l_metadata_type"
		return 1
		;;
	esac

	g_zxfer_source_dataset_type_result=$l_metadata_type
}

################################################################################
# PLAN: DERIVE AND DIFF
################################################################################

# Purpose: Stage encoded property lists for one synchronous AWK operation.
# Usage: zxfer_stage_property_inputs KEY<TAB>LIST ...; overwrites one private
# run-owned file and appends a completion marker. Lists contain no literal
# TAB or LF. Call before command substitution so allocation survives in the
# parent shell. The operation consumes the file before another may reuse it;
# EXIT owns its release. Returns the allocation or write status on failure.
zxfer_stage_property_inputs() {
	zxfer_ensure_runtime_artifact_file "${g_zxfer_property_input_file:-}" property-input || return "$?"
	g_zxfer_property_input_file=$g_zxfer_runtime_artifact_path_result
	l_property_input_status=0
	# A builtin's failed redirection returns instead of exiting the shell.
	{ printf '%s\n' "$@" '__ZXFER_PROPERTY_INPUT__' >|"$g_zxfer_property_input_file"; } 2>/dev/null ||
		l_property_input_status=$?
	case $l_property_input_status in
	1 | 2) return 1 ;;
	*) return "$l_property_input_status" ;;
	esac
}

# Read complete KEY<TAB>LIST rows literally, without awk -v escape handling
# or exec-size limits on property values. Only the small private path crosses
# the environment for property values. Reject incomplete input before any
# plan can be published.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_INPUT_AWK='
BEGIN {
	input_path = ENVIRON["ZXFER_PROPERTY_INPUT_FILE"]
	while ((input_status = (getline input_line < input_path)) > 0) {
		if (input_line == "__ZXFER_PROPERTY_INPUT__") {
			input_complete = 1
			break
		}
		input_separator = index(input_line, "\t")
		if (input_separator < 2)
			exit 1
		input_key = substr(input_line, 1, input_separator - 1)
		if (input_key in property_input)
			exit 1
		property_input[input_key] = substr(input_line, input_separator + 1)
	}
	if (!input_complete || (getline input_line < input_path) != 0)
		exit 1
	close(input_path)
}
'

# Shared rules prefix of the plan, child-create and child-inherit programs.
#   filter_property_list(list, drop_unsupported)  drop readonly and ignored
#       (-I) properties unless the item is an explicit override and, when
#       drop_unsupported is 1, destination-unsupported (-U) properties,
#       collecting one verbose warning per dropped unsupported item
#   sets_locally(property, source)  the inheritance rule: a local value or a
#       noninheritable property is set on the dataset itself; any other value
#       may be inherited from its parent
# Name-only policies arrive in ZXFER_AWK_REMOVE_LIST and
# ZXFER_AWK_UNSUPPORTED_LIST, the noninheritable names and verbose with -v.
# Every caller supplies both, so exported values never leak in. Keeping these
# literal preserves the existing AWK handling of -I items with control bytes.
# Property values come from the staged input file. Runs behind
# ZXFER_PROPERTY_INPUT_AWK and ZXFER_PROPERTY_AWK_LIB.
# shellcheck disable=SC2016  # AWK program is intentionally single-quoted.
ZXFER_PROPERTY_RULES_AWK='
function is_unsupported_property(property) {
	if (property in unsupported_property)
		return 1
	if (!use_typed_unsupported)
		return 0
	if (source_dstype == "volume")
		return (property in unsupported_volume_property)
	return (property in unsupported_filesystem_property)
}
function filter_property_list(list, drop_unsupported, count, items, i, fields, field_count, output) {
	count = split(list, items, ",")
	output = ""
	for (i = 1; i <= count; i++) {
		if (items[i] == "")
			continue
		field_count = split(items[i], fields, "=")
		if ((fields[1] in remove_property) && (field_count < 3 || fields[3] != "override"))
			continue
		if (drop_unsupported && is_unsupported_property(fields[1])) {
			if (verbose == 1)
				warning_items[++warning_count] = fields[1] "=" fields[2]
			continue
		}
		output = append_csv(output, items[i])
	}
	return output
}
function sets_locally(property, source) {
	return (source == "local" || (property in noninheritable))
}
BEGIN {
	csv_to_set(ENVIRON["ZXFER_AWK_REMOVE_LIST"], remove_property)
	csv_to_set(ENVIRON["ZXFER_AWK_UNSUPPORTED_LIST"], unsupported_property)
	csv_to_set(noninheritable_properties, noninheritable)
}
'

# Shared property planner core for single-dataset and batch planning. Each
# plan file holds selected create/set/inherit actions (with encoded values and
# first-source/override provenance for late parent reconciliation), optional
# -V diagnostic CSV views, -U warnings (warn<TAB>NAME=VAL), or a creation-time
# mismatch (error<TAB>3<TAB>PROPERTY), closed with __ZXFER_PROPERTY_ACTIONS__
# and a checked close().
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_PLAN_CORE_AWK='
function emit_action(verb, property, value, source, desired) {
	print verb "\t" property "=" value "=" source "=" (source == "override" ? desired : "") > output_path
}
function plan_action(verb, property, value) {
	if (verb == "") return
	if ((is_initial == 1 && verb == "root_set") ||
		(is_initial == 0 && verb != "root_set"))
		emit_action(verb == "inherit" ? "inherit" : "set", property, value,
			desired_source[property], desired_value[property])
	if (very_verbose == 1) {
		if (verb == "root_set") initial_set_list = append_csv(initial_set_list, property "=" value)
		else if (verb == "set") child_set_list = append_csv(child_set_list, property "=" value)
		else inherit_list = append_csv(inherit_list, property "=" value)
	}
}
function root_action(property, value, source,    root_sets) {
	root_sets = (source == "local" || source == "override")
	if (!(property in dest_available)) return root_sets ? "root_set" : ""
	return (dest_value[property] != value ||
		(root_sets && dest_source[property] != "local")) ? "root_set" : ""
}
function child_action(property, value, source) {
	if (!(property in dest_available) || dest_value[property] != value)
		return sets_locally(property, source) ? "set" : "inherit"
	if (source == "local" && dest_source[property] != "local") return "set"
	if (!sets_locally(property, source) && dest_source[property] == "local") return "inherit"
	return ""
}
function append_creation(property, value, source) {
	if (!(property in creation_seen)) {
		creation_output = append_csv(creation_output, property "=" value "=" source)
		creation_seen[property] = 1
	}
}
function emit_warnings(    i) {
	for (i = 1; i <= warning_count; i++)
		print "warn\t" warning_items[i] > output_path
}
function init_override_plan_state() {
	first_values(property_input["override"], override_value, override_source)
	if (transfer_all_flag == 0)
		override_count = split(property_input["override"], override_items, ",")
}
function plan_one_dataset(ds_is_initial, ds_has_dest, ds_type, ds_source_pvs, ds_dest_pvs, ds_output_path,    k, i, fields, source_items, source_count, property, is_creation, plan_items, plan_count, create_items, create_count) {
	is_initial = ds_is_initial
	has_destination = ds_has_dest
	source_dstype = ds_type
	output_path = ds_output_path
	# POSIX awk deletes entries individually to keep batch memory bounded.
	for (k in required_create) delete required_create[k]
	for (k in creation_seen) delete creation_seen[k]
	for (k in dest_value) delete dest_value[k]
	for (k in dest_source) delete dest_source[k]
	for (k in dest_available) delete dest_available[k]
	for (k in desired_value) delete desired_value[k]
	for (k in desired_source) delete desired_source[k]
	for (k in warning_items) delete warning_items[k]
	warning_count = 0
	override_output = ""
	creation_output = ""
	dest_output = ""
	initial_set_list = ""
	child_set_list = ""
	inherit_list = ""

	printf "" > output_path
	if (source_dstype != "volume")
		csv_to_set(required_creation_properties, required_create)

	# Derive. The -o items arrive parsed, encoded and unique. Without -P they
	# lead both lists; with -P they replace the source values they name.
	if (transfer_all_flag == 0) {
		for (i = 1; i <= override_count; i++) {
			if (override_items[i] == "")
				continue
			split(override_items[i], fields, "=")
			override_output = append_csv(override_output, override_items[i])
			append_creation(fields[1], fields[2], "override")
		}
	}

	source_count = split(ds_source_pvs, source_items, ",")
	for (i = 1; i <= source_count; i++) {
		if (source_items[i] == "")
			continue
		split(source_items[i], fields, "=")
		property = fields[1]

		# Some OpenZFS variants expose volume-only properties in `zfs get all`
		# for filesystem trees. Replaying those into filesystem create/set paths
		# is invalid, so drop them before deriving override and creation lists.
		if (source_dstype != "volume" &&
			(property == "volblocksize" || property == "volthreading"))
			continue

		is_creation = (fields[3] == "local" ||
			(source_dstype == "volume" && property == "refreservation") ||
			(property in required_create))

		if (property in override_value) {
			if (transfer_all_flag != 0)
				override_output = append_csv(override_output, property "=" override_value[property] "=override")
			if (is_creation)
				append_creation(property, override_value[property], "override")
			continue
		}
		if (transfer_all_flag != 0 || (property in required_create)) {
			override_output = append_csv(override_output, property "=" fields[2] "=" fields[3])
			if (is_creation)
				append_creation(property, fields[2], fields[3])
		}
	}
	override_output = filter_property_list(override_output, 1)
	creation_output = filter_property_list(creation_output, 1)

	# Diff. A creation-time property the destination holds with another
	# value is refused before anything is planned.
	if (has_destination == 1) {
		dest_output = filter_property_list(ds_dest_pvs, 0)
		first_values(dest_output, dest_value, dest_source)
		for (property in dest_value)
			dest_available[property] = 1

		plan_count = split(override_output, plan_items, ",")
		for (i = 1; i <= plan_count; i++) {
			if (plan_items[i] == "")
				continue
			split(plan_items[i], fields, "=")
			property = fields[1]
			if (!(property in desired_value)) {
				desired_value[property] = fields[2]
				desired_source[property] = fields[3]
			}
			if ((property in required_create) && (property in dest_available) &&
				fields[2] != dest_value[property]) {
				emit_warnings()
				print "error\t3\t" property > output_path
				print "__ZXFER_PROPERTY_ACTIONS__" > output_path
				if (close(output_path) != 0) exit 1
				return 3
			}
		}

		# Keep the ordered encoded records: restored metadata can repeat a
		# property with another value. Actions retain each record, but their
		# provenance remains the first record above. Validate all creation
		# constraints before emitting any action.
		for (i = 1; i <= plan_count; i++) {
			split(plan_items[i], fields, "=")
			property = fields[1]
			if (property == "" || (property in required_create))
				continue
			if (is_initial == 1 || very_verbose == 1)
				plan_action(root_action(property, fields[2], fields[3]), property, fields[2])
			if (is_initial == 0 || very_verbose == 1)
				plan_action(child_action(property, fields[2], fields[3]), property, fields[2])
			# Later repeated records compare as absent, as before.
			delete dest_available[property]
		}
	}

	if (has_destination == 0) {
		create_count = split(is_initial == 1 ? override_output : creation_output, create_items, ",")
		for (i = 1; i <= create_count; i++) {
			if (create_items[i] == "") continue
			split(create_items[i], fields, "=")
			emit_action("create", fields[1], fields[2], fields[3], fields[2])
		}
	}
	if (very_verbose == 1) {
		print "view\toverride_pvs=" override_output > output_path
		print "view\tcreation_pvs=" creation_output > output_path
		if (has_destination == 1) {
			print "view\tdest_pvs=" dest_output > output_path
			print "view\tinit_set=" initial_set_list > output_path
			print "view\tchild_set=" child_set_list > output_path
			print "view\tinherit=" inherit_list > output_path
		}
	}
	emit_warnings()
	print "__ZXFER_PROPERTY_ACTIONS__" > output_path
	if (close(output_path) != 0) exit 1
	return 0
}'

# Single-dataset driver sharing plan_one_dataset; preserves direct-AWK callers.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_PLAN_AWK=$ZXFER_PROPERTY_PLAN_CORE_AWK'
BEGIN {
	init_override_plan_state()
	plan_one_dataset(is_initial, has_destination, source_dstype,
		property_input["source"], property_input["destination"],
		ENVIRON["ZXFER_PROPERTY_PLAN_FILE"])
}'

# Batch driver for existing prefetched (source_row, dest_row) pairs. Reads the
# staged manifest in BEGIN, closes it, then opens/closes each source row, dest
# row, and output plan file inside the loop (<= 1 FD open at a time for
# illumos/BSD awk). Datasets missing valid type or required creation properties
# are skipped so they fall through to live backfill and single-dataset planning.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_BATCH_PLAN_AWK='
function valid_row_name(name) {
	return name ~ /^[abcdefghijklmnopqrstuvwxyz][0123456789]+$/
}
function read_row_file(file,    status, line, extra_line) {
	line = ""
	status = (getline line < file)
	if (status <= 0) {
		close(file)
		return ""
	}
	if ((getline extra_line < file) != 0) {
		close(file)
		return ""
	}
	close(file)
	return line
}
function map_destination(src,    prefix) {
	if (src == initial_source)
		return dest_root
	prefix = initial_source "/"
	if (substr(src, 1, length(prefix)) == prefix)
		return dest_root substr(src, length(initial_source) + 1)
	return ""
}
function list_first_value(list, prop_name,    hay, needle, at, rest, eq, comma) {
	hay = "," list ","
	needle = "," prop_name "="
	at = index(hay, needle)
	if (at == 0)
		return ""
	rest = substr(hay, at + length(needle))
	eq = index(rest, "=")
	comma = index(rest, ",")
	if (eq <= 1 || (comma > 0 && eq > comma))
		return ""
	return substr(rest, 1, eq - 1)
}
function has_all_required_creation(list,    k) {
	for (k = 1; k <= req_create_count; k++) {
		if (req_create_names[k] != "" &&
			index("," list ",", "," req_create_names[k] "=") == 0)
			return 0
	}
	return 1
}
BEGIN {
	row_dir = ENVIRON["ZXFER_AWK_ROW_DIR"]
	plan_base = ENVIRON["ZXFER_AWK_PLAN_BASE"]
	initial_source = ENVIRON["ZXFER_AWK_INITIAL_SOURCE"]
	dest_root = ENVIRON["ZXFER_AWK_DESTINATION_ROOT"]
	if (row_dir == "" || plan_base == "" || initial_source == "" || dest_root == "")
		exit 1
	use_typed_unsupported = (skip_unsupported == 1)
	csv_to_set(ENVIRON["ZXFER_AWK_UNSUPPORTED_FILESYSTEM_LIST"], unsupported_filesystem_property)
	csv_to_set(ENVIRON["ZXFER_AWK_UNSUPPORTED_VOLUME_LIST"], unsupported_volume_property)
	req_create_count = split(required_creation_properties, req_create_names, ",")
	init_override_plan_state()

	section = 0
	manifest_complete = 0
	while ((m_status = (getline m_line < plan_base)) > 0) {
		if (m_line == "__ZXFER_BATCH_SOURCE__") { section = 1; continue }
		if (m_line == "__ZXFER_BATCH_DEST__") { section = 2; continue }
		if (m_line == "__ZXFER_BATCH_DEST_LIST__") { section = 3; continue }
		if (m_line == "__ZXFER_BATCH_END__") { manifest_complete = 1; break }
		if (m_line == "") continue
		if (section == 1 || section == 2) {
			m_tab = index(m_line, "\t")
			if (m_tab < 2) continue
			m_row = substr(m_line, 1, m_tab - 1)
			m_ds = substr(m_line, m_tab + 1)
			if (section == 1) {
				if (!(m_ds in src_row_seen)) {
					src_row_seen[m_ds] = 1
					src_order[++src_count] = m_ds
					src_row[m_ds] = m_row
				}
			} else if (!(m_ds in dst_row_seen)) {
				dst_row_seen[m_ds] = 1
				dst_row[m_ds] = m_row
			}
		} else if (section == 3) {
			dest_exists[m_line] = 1
		}
	}
	if (!manifest_complete || (getline m_line < plan_base) != 0) {
		close(plan_base)
		exit 1
	}
	if (close(plan_base) != 0)
		exit 1

	cand_count = 0
	for (bi = 1; bi <= src_count; bi++) {
		ds = src_order[bi]
		srow = src_row[ds]
		if (!valid_row_name(srow)) continue
		dst = map_destination(ds)
		if (dst == "" || !(dst in dest_exists)) continue
		if (!(dst in dst_row)) continue
		drow = dst_row[dst]
		if (!valid_row_name(drow)) continue
		cand_count++
		cand_src[cand_count] = ds
		cand_srow[cand_count] = srow
		cand_drow[cand_count] = drow
	}
	if (cand_count >= 2) {
		pos = 0
		for (bi = 1; bi <= cand_count; bi++) {
			ds = cand_src[bi]
			srow = cand_srow[bi]
			drow = cand_drow[bi]
			src_pvs = read_row_file(row_dir "/" srow)
			if (src_pvs == "") continue
			dst_pvs = read_row_file(row_dir "/" drow)
			if (dst_pvs == "") continue
			ds_type = list_first_value(src_pvs, "type")
			if (ds_type != "filesystem" && ds_type != "volume") continue
			if (ds_type == "filesystem" &&
				(!has_all_required_creation(src_pvs) || !has_all_required_creation(dst_pvs)))
				continue
			pos++
			plan_one_dataset(ds == initial_source ? 1 : 0, 1, ds_type,
				src_pvs, dst_pvs, plan_base "." pos ".p")
			printf "%d:%s:%s\t%s\n", pos, srow, drow, ds
		}
	}
	print "__ZXFER_PROPERTY_BATCH_PLAN__"
}'

# Purpose: Validate a complete action file before any property mutation.
# Usage: zxfer_validate_property_plan FILE [batch]; returns 1 on read failure,
# unknown records, a missing final marker or data after it. In "batch" mode,
# create rows are also rejected. After full validation, replays any encoded -U
# warnings (warn<TAB>NAME=VAL) to stderr and throws on error<TAB>3<TAB>PROP.
zxfer_validate_property_plan() {
	g_zxfer_property_plan_summary_result=""
	l_val_plan_mode=${2:-}
	l_val_plan_complete=0
	l_val_plan_actions=0
	l_val_plan_overrides=0
	l_val_plan_warnings=""
	l_val_plan_error_prop=""
	{
		while IFS= read -r l_val_plan_row; do
			[ "$l_val_plan_complete" -eq 0 ] || return 1
			case $l_val_plan_row in
			"__ZXFER_PROPERTY_ACTIONS__") l_val_plan_complete=1 ;;
			create"$ZXFER_TAB"?*=*=*=* | set"$ZXFER_TAB"?*=*=*=* | inherit"$ZXFER_TAB"?*=*=*=*)
				[ -z "$l_val_plan_error_prop" ] || return 1
				if [ "$l_val_plan_mode" = batch ]; then
					case $l_val_plan_row in
					create"$ZXFER_TAB"*) return 1 ;;
					esac
				fi
				l_val_plan_actions=$((l_val_plan_actions + 1))
				l_val_plan_source=${l_val_plan_row#*"$ZXFER_TAB"}
				case $l_val_plan_source in
				*"$ZXFER_TAB"*) return 1 ;;
				esac
				l_val_plan_source=${l_val_plan_source#*=}
				l_val_plan_source=${l_val_plan_source#*=}
				l_val_plan_desired=${l_val_plan_source#*=}
				case $l_val_plan_desired in *'='*) return 1 ;; esac
				case $l_val_plan_source in
				override=*) l_val_plan_overrides=$((l_val_plan_overrides + 1)) ;;
				esac
				;;
			view"$ZXFER_TAB"?*=*)
				[ -z "$l_val_plan_error_prop" ] || return 1
				;;
			warn"$ZXFER_TAB"?*=*)
				l_val_plan_warn_item=${l_val_plan_row#"warn$ZXFER_TAB"}
				case $l_val_plan_warn_item in
				*"$ZXFER_TAB"*) return 1 ;;
				esac
				case ${l_val_plan_warn_item#*=} in
				*'='*) return 1 ;;
				esac
				l_val_plan_warnings=${l_val_plan_warnings:+$l_val_plan_warnings$ZXFER_LF}$l_val_plan_warn_item
				;;
			"error${ZXFER_TAB}3${ZXFER_TAB}"?*)
				[ "$l_val_plan_actions" -eq 0 ] && [ -z "$l_val_plan_error_prop" ] || return 1
				l_val_plan_error_prop=${l_val_plan_row#"error${ZXFER_TAB}3${ZXFER_TAB}"}
				case $l_val_plan_error_prop in
				*"$ZXFER_TAB"*) return 1 ;;
				esac
				;;
			*) return 1 ;;
			esac
		done
		[ -z "$l_val_plan_row" ] && [ "$l_val_plan_complete" -eq 1 ]
	} 2>/dev/null <"$1" || return 1

	if [ -n "$l_val_plan_warnings" ]; then
		while IFS= read -r l_val_plan_warn_item; do
			[ -n "$l_val_plan_warn_item" ] || continue
			zxfer_decode_property_value "${l_val_plan_warn_item#*=}"
			zxfer_warn_stderr "Destination does not support property ${l_val_plan_warn_item%%=*}=$g_zxfer_decoded_property_value"
		done <<EOF
$l_val_plan_warnings
EOF
	fi
	if [ -n "$l_val_plan_error_prop" ]; then
		zxfer_throw_error_with_usage "The property \"$l_val_plan_error_prop\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
		return 1
	fi
	g_zxfer_property_plan_summary_result="$l_val_plan_actions $l_val_plan_overrides"
}

# Purpose: Build the current policy and endpoint fingerprint for batch property
# planning so any policy, verbosity, row-store, or root change invalidates
# previously prepared plans.
# Usage: zxfer_build_property_batch_plan_context; publishes
# g_zxfer_property_batch_plan_context_result and leaves the mapped destination
# root in g_zxfer_destination_dataset_result, or returns 1 without roots.
zxfer_build_property_batch_plan_context() {
	g_zxfer_property_batch_plan_context_result=""
	[ -n "${g_initial_source:-}" ] && [ -n "${g_destination:-}" ] || return 1
	zxfer_map_destination_dataset
	l_batch_ctx_dest_root=$g_zxfer_destination_dataset_result
	l_batch_ctx_unsup_fs=""
	l_batch_ctx_unsup_vol=""
	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ]; then
		l_batch_ctx_unsup_fs=${g_zxfer_unsupported_filesystem_properties:-}
		l_batch_ctx_unsup_vol=${g_zxfer_unsupported_volume_properties:-}
	fi
	g_zxfer_property_batch_plan_context_result="${g_zxfer_property_row_dir:-}$ZXFER_LF$g_initial_source$ZXFER_LF$l_batch_ctx_dest_root$ZXFER_LF${g_option_P_transfer_property:-0}:${g_option_U_skip_unsupported_properties:-0}:${g_option_v_verbose:-0}:${g_option_V_very_verbose:-0}$ZXFER_LF${g_zxfer_property_readonly_policy:-}$ZXFER_LF${g_option_I_ignore_properties:-}$ZXFER_LF${g_zxfer_property_override_policy:-}$ZXFER_LF$l_batch_ctx_unsup_fs$ZXFER_LF$l_batch_ctx_unsup_vol"
}

# Purpose: Batch-plan all existing prefetched (source_row, dest_row) pairs in
# one AWK invocation under umask 077, writing 0600 per-dataset plan files.
# Usage: zxfer_prepare_batch_property_plans; runs lazily once per destination
# cache iteration when >= 2 eligible pairs exist. Publishes
# g_zxfer_property_batch_plan_index ("POS:SROW:DROW<TAB>SOURCE" lines) and sets
# g_zxfer_property_batch_plan_state=1 on complete validated output; failures
# mark state=2 and publish nothing so callers fall back to single-dataset plans.
zxfer_prepare_batch_property_plans() {
	g_zxfer_property_batch_plan_state=2
	g_zxfer_property_batch_plan_index=""
	g_zxfer_property_batch_plan_context=""
	g_zxfer_property_batch_plan_file_result=""

	[ "${g_option_e_restore_property_mode:-0}" -eq 0 ] || return 1
	[ "${g_zxfer_source_property_tree_prefetch_state:-0}" -ne 2 ] || return 1
	[ "${g_zxfer_destination_property_tree_prefetch_state:-0}" -ne 2 ] || return 1
	[ -n "${g_zxfer_property_row_dir:-}" ] || return 1
	case ${g_zxfer_source_property_table:-} in
	*"$ZXFER_TAB"*"$ZXFER_TAB"*) ;;
	*) return 1 ;;
	esac
	case ${g_zxfer_destination_property_table:-} in
	*"$ZXFER_TAB"*"$ZXFER_TAB"*) ;;
	*) return 1 ;;
	esac
	case ${g_recursive_dest_list:-} in
	*"$ZXFER_LF"?*) ;;
	*) return 1 ;;
	esac
	zxfer_build_property_batch_plan_context || return 1
	l_batch_prep_ctx=$g_zxfer_property_batch_plan_context_result
	l_batch_prep_dest_root=$g_zxfer_destination_dataset_result

	zxfer_stage_property_inputs "override$ZXFER_TAB$g_zxfer_property_override_policy" || return "$?"
	zxfer_ensure_runtime_artifact_file "${g_zxfer_property_batch_plan_base:-}" property-batch-plan || return "$?"
	g_zxfer_property_batch_plan_base=$g_zxfer_runtime_artifact_path_result
	{
		printf '%s\n' '__ZXFER_BATCH_SOURCE__' "$g_zxfer_source_property_table" \
			'__ZXFER_BATCH_DEST__' "$g_zxfer_destination_property_table" \
			'__ZXFER_BATCH_DEST_LIST__' "$g_recursive_dest_list" \
			'__ZXFER_BATCH_END__' >|"$g_zxfer_property_batch_plan_base"
	} 2>/dev/null || return 1

	# Create batch plan files 0600 inside the subshell so the parent shell's
	# umask is never mutated.
	l_batch_prep_status=0
	l_batch_prep_output=$(
		umask 077 &&
			ZXFER_PROPERTY_INPUT_FILE=$g_zxfer_property_input_file \
				ZXFER_AWK_ROW_DIR=$g_zxfer_property_row_dir \
				ZXFER_AWK_PLAN_BASE=$g_zxfer_property_batch_plan_base \
				ZXFER_AWK_INITIAL_SOURCE=$g_initial_source \
				ZXFER_AWK_DESTINATION_ROOT=$l_batch_prep_dest_root \
				ZXFER_AWK_REMOVE_LIST="$g_zxfer_property_readonly_policy,$g_option_I_ignore_properties" \
				ZXFER_AWK_UNSUPPORTED_LIST='' \
				ZXFER_AWK_UNSUPPORTED_FILESYSTEM_LIST=${g_zxfer_unsupported_filesystem_properties:-} \
				ZXFER_AWK_UNSUPPORTED_VOLUME_LIST=${g_zxfer_unsupported_volume_properties:-} \
				"${g_cmd_awk:-awk}" \
				-v transfer_all_flag="$g_option_P_transfer_property" \
				-v skip_unsupported="${g_option_U_skip_unsupported_properties:-0}" \
				-v required_creation_properties="$ZXFER_REQUIRED_CREATION_PROPERTIES" \
				-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
				-v verbose="${g_option_v_verbose:-0}" \
				-v very_verbose="${g_option_V_very_verbose:-0}" \
				"$ZXFER_PROPERTY_INPUT_AWK$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_PROPERTY_PLAN_CORE_AWK$ZXFER_PROPERTY_BATCH_PLAN_AWK"
	) || l_batch_prep_status=$?
	zxfer_write_runtime_artifact_file "$g_zxfer_property_batch_plan_base" "" || :
	[ "$l_batch_prep_status" -eq 0 ] || return "$l_batch_prep_status"

	l_batch_prep_complete=0
	l_batch_prep_index=""
	while IFS= read -r l_batch_prep_line; do
		[ "$l_batch_prep_complete" -eq 0 ] || return 1
		case $l_batch_prep_line in
		"__ZXFER_PROPERTY_BATCH_PLAN__")
			l_batch_prep_complete=1
			;;
		*"$ZXFER_TAB"?*)
			l_batch_prep_meta=${l_batch_prep_line%%"$ZXFER_TAB"*}
			case $l_batch_prep_meta in
			[0123456789]*:[abcdefghijklmnopqrstuvwxyz]*:[abcdefghijklmnopqrstuvwxyz]*) ;;
			*) return 1 ;;
			esac
			l_batch_prep_index=${l_batch_prep_index:+$l_batch_prep_index$ZXFER_LF}$l_batch_prep_line
			;;
		*) return 1 ;;
		esac
	done <<EOF
$l_batch_prep_output
EOF
	[ "$l_batch_prep_complete" -eq 1 ] || return 1
	[ -z "$l_batch_prep_index" ] ||
		g_zxfer_property_batch_plan_index=$ZXFER_LF$l_batch_prep_index$ZXFER_LF
	g_zxfer_property_batch_plan_context=$l_batch_prep_ctx
	g_zxfer_property_batch_plan_state=1
}

# Purpose: Look up and consume a pre-planned batch property file for SOURCE/DEST
# when its (source_row, dest_row) provenance and policy context still match.
# Usage: zxfer_try_use_prepared_property_plan SOURCE DEST; lazily triggers
# zxfer_prepare_batch_property_plans once per pass. Returns 0 with
# g_zxfer_property_batch_plan_file_result and
# g_zxfer_property_plan_summary_result published, throws on a deferred
# creation-time mismatch or corrupted matched plan file, or returns 1 to fall
# back to single-dataset planning.
zxfer_try_use_prepared_property_plan() {
	g_zxfer_property_batch_plan_file_result=""
	l_batch_use_source=$1
	l_batch_use_dest=$2

	[ "${g_option_e_restore_property_mode:-0}" -eq 0 ] || return 1
	if [ "${g_zxfer_property_batch_plan_state:-0}" -eq 0 ]; then
		zxfer_prepare_batch_property_plans || return 1
	fi
	[ "${g_zxfer_property_batch_plan_state:-0}" -eq 1 ] || return 1
	[ -n "${g_zxfer_property_batch_plan_index:-}" ] || return 1
	[ -n "${g_zxfer_property_batch_plan_base:-}" ] || return 1

	zxfer_build_property_batch_plan_context || return 1
	[ "$g_zxfer_property_batch_plan_context_result" = "$g_zxfer_property_batch_plan_context" ] ||
		return 1
	zxfer_map_destination_dataset "$l_batch_use_source"
	[ "$g_zxfer_destination_dataset_result" = "$l_batch_use_dest" ] || return 1

	zxfer_find_property_row "$g_zxfer_property_batch_plan_index" "$l_batch_use_source" ||
		return 1
	l_batch_use_meta=$g_zxfer_property_row_result
	l_batch_use_pos=${l_batch_use_meta%%:*}
	l_batch_use_rest=${l_batch_use_meta#*:}
	l_batch_use_srow=${l_batch_use_rest%%:*}
	l_batch_use_drow=${l_batch_use_rest#*:}
	zxfer_is_uint "$l_batch_use_pos" || return 1

	zxfer_find_property_row "${g_zxfer_source_property_table:-}" "$l_batch_use_source" ||
		return 1
	[ "$g_zxfer_property_row_result" = "$l_batch_use_srow" ] || return 1
	zxfer_find_property_row "${g_zxfer_destination_property_table:-}" "$l_batch_use_dest" ||
		return 1
	[ "$g_zxfer_property_row_result" = "$l_batch_use_drow" ] || return 1

	l_batch_use_file=$g_zxfer_property_batch_plan_base.$l_batch_use_pos.p
	zxfer_validate_property_plan "$l_batch_use_file" batch ||
		zxfer_throw_error "Failed to plan dataset properties."
	g_zxfer_property_batch_plan_file_result=$l_batch_use_file
}

# Purpose: Plan and validate this dataset without publishing payload globals.
# Usage: zxfer_plan_property_changes IS_INITIAL HAS_DESTINATION TYPE
# SOURCE_PVS DEST_PVS UNSUPPORTED_CSV; both flags are 0 or 1. Run-wide -P,
# -o, readonly and -I policies are already prepared by session startup.
# An existing destination may have an empty list. Publishes the reusable
# action file and a small action/override count summary; failures clear it.
zxfer_plan_property_changes() {
	g_zxfer_property_plan_summary_result=""
	[ "$#" -eq 6 ] || zxfer_throw_error "Failed to plan dataset properties."
	case $1:$2 in 0:0 | 0:1 | 1:0 | 1:1) ;; *) zxfer_throw_error "Failed to plan dataset properties." ;; esac
	zxfer_stage_property_inputs "source$ZXFER_TAB$4" \
		"override$ZXFER_TAB$g_zxfer_property_override_policy" "destination$ZXFER_TAB$5" ||
		zxfer_throw_error "Failed to stage property planner input." "$?"
	zxfer_ensure_runtime_artifact_file "${g_zxfer_property_plan_file:-}" property-plan ||
		zxfer_throw_error "Failed to plan dataset properties." "$?"
	g_zxfer_property_plan_file=$g_zxfer_runtime_artifact_path_result
	zxfer_write_runtime_artifact_file "$g_zxfer_property_plan_file" "" ||
		zxfer_throw_error "Failed to plan dataset properties." "$?"
	ZXFER_PROPERTY_INPUT_FILE=$g_zxfer_property_input_file \
		ZXFER_PROPERTY_PLAN_FILE=$g_zxfer_property_plan_file \
		ZXFER_AWK_REMOVE_LIST="$g_zxfer_property_readonly_policy,$g_option_I_ignore_properties" \
		ZXFER_AWK_UNSUPPORTED_LIST=$6 \
		"${g_cmd_awk:-awk}" -v is_initial="$1" \
		-v transfer_all_flag="$g_option_P_transfer_property" -v source_dstype="$3" \
		-v has_destination="$2" \
		-v required_creation_properties="$ZXFER_REQUIRED_CREATION_PROPERTIES" \
		-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
		-v verbose="${g_option_v_verbose:-0}" -v very_verbose="${g_option_V_very_verbose:-0}" \
		"$ZXFER_PROPERTY_INPUT_AWK$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_PROPERTY_PLAN_AWK" ||
		zxfer_throw_error "Failed to plan dataset properties."
	zxfer_validate_property_plan "$g_zxfer_property_plan_file" ||
		zxfer_throw_error "Failed to plan dataset properties."
}

# Purpose: Read one diagnostic action list without retaining normal-run CSVs.
# Usage: zxfer_property_plan_list FILE VERB; prints name=value items in order.
# The complete file has already been validated by its producer.
zxfer_property_plan_list() {
	l_property_plan_list=""
	{
		while IFS="$ZXFER_TAB" read -r l_property_plan_list_verb l_property_plan_list_item; do
			[ "$l_property_plan_list_verb" = "$2" ] || continue
			l_property_plan_list_name=${l_property_plan_list_item%%=*}
			l_property_plan_list_value=${l_property_plan_list_item#*=}
			l_property_plan_list="$l_property_plan_list${l_property_plan_list:+,}$l_property_plan_list_name=${l_property_plan_list_value%%=*}"
		done
	} <"$1" || return "$?"
	printf '%s\n' "$l_property_plan_list"
}

# Purpose: Display the planner views only under -V, before parent resolution.
# Usage: zxfer_show_property_plan FILE; values remain encoded and escaped.
zxfer_show_property_plan() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	{
		while IFS="$ZXFER_TAB" read -r l_show_property_plan_kind l_show_property_plan_view; do
			[ "$l_show_property_plan_kind" = view ] || continue
			zxfer_echoV_escaped "zxfer_transfer_properties ${l_show_property_plan_view%%=*}" "${l_show_property_plan_view#*=}"
		done
	} <"$1" || zxfer_throw_error "Failed to read dataset property plan." "$?"
}

################################################################################
# DESTINATION CREATE
################################################################################

# Purpose: Tell whether the current dataset's destination exists before its
# properties are planned.
# Usage: zxfer_property_destination_exists DESTINATION; returns 0 when the
# recursive destination list holds it or a live probe finds it (then noted in
# the existence cache and the list), and 1 when it is missing. Throws when
# the probe fails.
zxfer_property_destination_exists() {
	case "$ZXFER_LF${g_recursive_dest_list:-}$ZXFER_LF" in
	*"$ZXFER_LF$1$ZXFER_LF"*) return 0 ;;
	esac
	zxfer_destination_exists "$1" live || return 1
	zxfer_note_destination_dataset_exists "$1"
	return 0
}

# Purpose: Create a missing destination using its selected action plan.
# Usage: zxfer_create_destination_dataset FILE IS_INITIAL TYPE VOLSIZE
# DESTINATION READONLY_CSV. Probe parents before creation; filter only child
# overrides actually supplied by that parent. -p must never carry properties.
zxfer_create_destination_dataset() {
	l_missing_plan=$1
	l_missing_initial=$2
	l_missing_type=$3
	l_missing_volsize=$4
	l_missing_dataset=$5
	l_missing_readonly=$6
	zxfer_echov "Creating destination filesystem \"$l_missing_dataset\" with specified properties."
	l_missing_parent_exists=""
	l_missing_parent=${l_missing_dataset%/*}
	if [ "$l_missing_parent" != "$l_missing_dataset" ]; then
		if zxfer_destination_exists "$l_missing_parent"; then
			l_missing_parent_exists=1
		else
			l_missing_parent_exists=0
		fi
	fi
	l_missing_action_count=${g_zxfer_property_plan_summary_result%% *}
	l_missing_override_count=${g_zxfer_property_plan_summary_result#* }
	if [ "$l_missing_initial" -eq 0 ] && [ "$l_missing_parent_exists" = 1 ] &&
		[ "$l_missing_override_count" -ne 0 ]; then
		zxfer_load_normalized_dataset_properties "$l_missing_parent" destination ||
			zxfer_throw_error "Failed to retrieve parent destination properties for [$l_missing_parent]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
		zxfer_resolve_parent_property_plan "$l_missing_plan" "$g_zxfer_normalized_dataset_properties" "$l_missing_readonly" create ||
			zxfer_throw_error "Failed to filter child creation override properties." "$?"
		l_missing_plan=$g_zxfer_property_parent_plan_file
	fi
	zxfer_warn_missing_recursive_destination "$l_missing_dataset"
	l_missing_with_parents=no
	if [ "$l_missing_parent_exists" = 0 ]; then
		if [ "$l_missing_action_count" -ne 0 ]; then
			zxfer_execute_property_plan create "" filesystem "" "$l_missing_parent" yes ||
				zxfer_throw_error "Error when creating destination filesystem." "$?"
			zxfer_note_destination_dataset_exists "$l_missing_parent"
			zxfer_invalidate_destination_property_mutation_cache "$l_missing_parent"
		else
			l_missing_with_parents=yes
		fi
	fi
	zxfer_execute_property_plan create "$l_missing_plan" "$l_missing_type" "$l_missing_volsize" "$l_missing_dataset" "$l_missing_with_parents" ||
		zxfer_throw_error "Error when creating destination filesystem." "$?"
	zxfer_note_destination_dataset_exists "$l_missing_dataset"
	zxfer_invalidate_destination_property_mutation_cache "$l_missing_dataset"
}

################################################################################
# DESTINATION SET / INHERIT COMMANDS
################################################################################

# Purpose: Run one destination property verb (set or inherit) live.
# Usage: zxfer_run_destination_property_verb VERB ERROR_MESSAGE DESTINATION
# [ARG...]. Shows the rendered command under -v (keeping -T single-quote
# escapes intact unless a control byte requires report escaping), runs it, then
# invalidates cached destination rows. A failed run throws ERROR_MESSAGE with
# the failing status.
zxfer_run_destination_property_verb() {
	l_verb=$1
	l_verb_error_message=$2
	l_verb_destination=$3
	shift 3

	if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
		l_verb_display=""
		if [ -z "${g_option_T_target_host:-}" ]; then
			if zxfer_render_command_for_report_into_result "" "$g_cmd_zfs" "$l_verb" "$@" "$l_verb_destination"; then
				l_verb_display=$g_zxfer_rendered_report_command_result
			fi
		elif zxfer_render_zfs_command_for_role destination "$l_verb" "$@" "$l_verb_destination"; then
			l_verb_display=$g_zxfer_shell_command_result
			# Without character classes in case patterns (posh), always escape.
			# With them, skip escaping when no control byte is present so '\''
			# inside rendered -T commands is not double-escaped.
			l_verb_needs_escape=1
			if [ "${g_zxfer_report_fast_path:-0}" = 1 ]; then
				case $l_verb_display in
				*[[:cntrl:]]*) ;;
				*) l_verb_needs_escape=0 ;;
				esac
			fi
			if [ "$l_verb_needs_escape" -eq 1 ]; then
				if zxfer_escape_report_value_into_result "$l_verb_display"; then
					l_verb_display=$g_zxfer_escaped_report_value_result
				else
					l_verb_display=""
				fi
			fi
		fi
		zxfer_echov "$l_verb_display"
	fi
	zxfer_run_destination_zfs_cmd "$l_verb" "$@" "$l_verb_destination" ||
		zxfer_throw_error "$l_verb_error_message" "$?"
	zxfer_invalidate_destination_property_mutation_cache "$l_verb_destination"
}

################################################################################
# CHILD INHERITANCE / APPLY
################################################################################

# Parent resolution has two explicit rules: creation drops inheritable
# overrides only when the actual parent matches; reconciliation also keeps
# a matching inheritable -o request inherited. Process original sets before
# original inherits to preserve command argument and inherit ordering.
# shellcheck disable=SC2016  # AWK fields must remain literal.
ZXFER_PARENT_PROPERTY_PLAN_AWK='
function resolve_action(verb, item, fields, property, value, source, desired, inherit) {
	split(item, fields, "=")
	property = fields[1]; value = fields[2]; source = fields[3]; desired = fields[4]
	if (mode == "create") {
		if (source == "override" && !(property in noninheritable) &&
			(property in parent_value) && parent_value[property] == value) return
	} else {
		inherit = 0
		if (!(property in noninheritable) &&
			(verb == "inherit" || !sets_locally(property, source)))
			inherit = ((source == "override" && desired == value) ||
				((property in parent_value) && parent_value[property] == value))
		verb = inherit ? "inherit" : "set"
	}
	print verb "\t" item > output_path
}
BEGIN {
	output_path = ENVIRON["ZXFER_PROPERTY_PLAN_OUTPUT"]
	input_path = ENVIRON["ZXFER_PROPERTY_PLAN_FILE"]
	first_values(filter_property_list(property_input["parent"], 0), parent_value, parent_source)
	while ((status = (getline row < input_path)) > 0) {
		if (row == "__ZXFER_PROPERTY_ACTIONS__") { complete = 1; break }
		at = index(row, "\t")
		if (at < 2) exit 1
		verb = substr(row, 1, at - 1); item = substr(row, at + 1)
		if (verb == "create" || verb == "set") {
			if ((mode == "create") != (verb == "create")) exit 1
			sets[++set_count] = item
		} else if (verb == "inherit") {
			if (mode != "apply") exit 1
			inherits[++inherit_count] = item
		}
		else if (verb != "view" && verb != "warn") exit 1
	}
	if (!complete || (getline row < input_path) != 0 || close(input_path) != 0) exit 1
	printf "" > output_path
	for (i = 1; i <= set_count; i++) resolve_action(mode == "create" ? "create" : "set", sets[i])
	for (i = 1; i <= inherit_count; i++) resolve_action("inherit", inherits[i])
	print "__ZXFER_PROPERTY_ACTIONS__" > output_path
	if (close(output_path) != 0) exit 1
}'

# Purpose: Rewrite a selected action plan against a freshly read parent.
# Usage: zxfer_resolve_parent_property_plan FILE PARENT_PVS READONLY_CSV
# create|apply; publish the checked parent-work file and its count summary.
# Allocation, staging and AWK statuses are returned for the caller to report.
zxfer_resolve_parent_property_plan() {
	g_zxfer_property_plan_summary_result=""
	case $4 in create | apply) ;; *) return 1 ;; esac
	zxfer_stage_property_inputs "parent$ZXFER_TAB$2" || return "$?"
	zxfer_ensure_runtime_artifact_file "$g_zxfer_property_parent_plan_file" property-parent-plan || return "$?"
	g_zxfer_property_parent_plan_file=$g_zxfer_runtime_artifact_path_result
	zxfer_write_runtime_artifact_file "$g_zxfer_property_parent_plan_file" "" || return "$?"
	l_parent_property_plan_status=0
	ZXFER_PROPERTY_INPUT_FILE=$g_zxfer_property_input_file \
		ZXFER_PROPERTY_PLAN_FILE=$1 ZXFER_PROPERTY_PLAN_OUTPUT=$g_zxfer_property_parent_plan_file \
		ZXFER_AWK_REMOVE_LIST="$3,${g_option_I_ignore_properties:-}" ZXFER_AWK_UNSUPPORTED_LIST='' \
		"${g_cmd_awk:-awk}" -v mode="$4" -v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
		"$ZXFER_PROPERTY_INPUT_AWK$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_PARENT_PROPERTY_PLAN_AWK" || l_parent_property_plan_status=$?
	if [ "$l_parent_property_plan_status" -eq 0 ]; then
		zxfer_validate_property_plan "$g_zxfer_property_parent_plan_file" || l_parent_property_plan_status=1
	fi
	[ "$l_parent_property_plan_status" -ne 0 ] || return 0
	[ "$4" != apply ] || zxfer_throw_error "Failed to reconcile child property inheritance."
	return "$l_parent_property_plan_status"
}

# Purpose: Validate and execute an action plan in one pass: build create/set
# argv directly from encoded action rows and apply ordered inherits after the
# single batched set. Never pass a decoded value to eval.
# Usage: zxfer_execute_property_plan create|apply FILE TYPE VOLSIZE DEST [yes|no].
# Empty FILE is allowed only for property-free parent creation. Create returns
# its original status; set/inherit retain their centralized failure reports.
zxfer_execute_property_plan() {
	case $1 in create | apply) ;; *) return 1 ;; esac
	l_exec_plan_mode=$1
	l_exec_plan_file=$2
	l_exec_plan_type=$3
	l_exec_plan_size=$4
	l_exec_plan_dest=$5
	l_exec_plan_parents=${6:-no}
	l_exec_plan_inherits=""
	l_exec_plan_set_csv=""
	l_exec_plan_inherit_csv=""

	if [ "$l_exec_plan_mode" = create ]; then
		[ "$l_exec_plan_type" != volume ] ||
			[ -n "$l_exec_plan_size" ] || return 1
		set -- create
		[ "$l_exec_plan_parents" != yes ] || set -- "$@" -p
		[ "$l_exec_plan_type" != volume ] ||
			set -- "$@" -V "$l_exec_plan_size"
	else
		[ -n "$l_exec_plan_file" ] || return 1
		set --
	fi

	if [ -n "$l_exec_plan_file" ]; then
		l_exec_plan_complete=0
		{
			while IFS= read -r l_exec_plan_row; do
				[ "$l_exec_plan_complete" -eq 0 ] || return 1
				case $l_exec_plan_row in
				"__ZXFER_PROPERTY_ACTIONS__")
					l_exec_plan_complete=1
					;;
				view"$ZXFER_TAB"?*=* | warn"$ZXFER_TAB"?*=*) ;;
				create"$ZXFER_TAB"?*=*=*=* | set"$ZXFER_TAB"?*=*=*=* | inherit"$ZXFER_TAB"?*=*=*=*)
					l_exec_plan_verb=${l_exec_plan_row%%"$ZXFER_TAB"*}
					l_exec_plan_item=${l_exec_plan_row#*"$ZXFER_TAB"}
					case $l_exec_plan_item in
					*"$ZXFER_TAB"*) return 1 ;;
					esac
					l_exec_plan_rest=${l_exec_plan_item#*=}
					l_exec_plan_value=${l_exec_plan_rest%%=*}
					l_exec_plan_rest=${l_exec_plan_rest#*=}
					case ${l_exec_plan_rest#*=} in *'='*) return 1 ;; esac
					l_exec_plan_name=${l_exec_plan_item%%=*}
					case $l_exec_plan_mode,$l_exec_plan_verb in
					create,create)
						[ "$l_exec_plan_parents" != yes ] || return 1
						zxfer_decode_property_value "$l_exec_plan_value"
						set -- "$@" -o "$l_exec_plan_name=$g_zxfer_decoded_property_value"
						;;
					apply,set)
						zxfer_decode_property_value "$l_exec_plan_value"
						set -- "$@" "$l_exec_plan_name=$g_zxfer_decoded_property_value"
						[ "${g_option_v_verbose:-0}" -ne 1 ] ||
							l_exec_plan_set_csv=${l_exec_plan_set_csv:+$l_exec_plan_set_csv,}$l_exec_plan_name=$l_exec_plan_value
						;;
					apply,inherit)
						l_exec_plan_inherits=${l_exec_plan_inherits:+$l_exec_plan_inherits$ZXFER_LF}$l_exec_plan_name
						[ "${g_option_v_verbose:-0}" -ne 1 ] ||
							l_exec_plan_inherit_csv=${l_exec_plan_inherit_csv:+$l_exec_plan_inherit_csv,}$l_exec_plan_name=$l_exec_plan_value
						;;
					*) return 1 ;;
					esac
					;;
				*) return 1 ;;
				esac
			done
			[ -z "$l_exec_plan_row" ] && [ "$l_exec_plan_complete" -eq 1 ]
		} 2>/dev/null <"$l_exec_plan_file" || return 1
	fi

	if [ "$l_exec_plan_mode" = create ]; then
		zxfer_run_destination_zfs_cmd "$@" "$l_exec_plan_dest"
		return "$?"
	fi
	[ "$#" -gt 0 ] || [ -n "$l_exec_plan_inherits" ] || return 0
	if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
		zxfer_echov "Setting properties/sources on destination filesystem \"$l_exec_plan_dest\"."
		for l_exec_plan_kind in set inherit; do
			if [ "$l_exec_plan_kind" = set ]; then
				l_exec_plan_list=$l_exec_plan_set_csv
			else
				l_exec_plan_list=$l_exec_plan_inherit_csv
			fi
			[ -n "$l_exec_plan_list" ] || continue
			zxfer_decode_serialized_property_list_for_display "$l_exec_plan_list"
			zxfer_escape_report_value_into_result "$g_zxfer_property_display_list_result"
			zxfer_echov "Property $l_exec_plan_kind list: $g_zxfer_escaped_report_value_result"
		done
	fi
	[ "$#" -eq 0 ] || zxfer_run_destination_property_verb set \
		"Error when setting properties on destination filesystem." "$l_exec_plan_dest" "$@"
	[ -n "$l_exec_plan_inherits" ] || return 0
	while IFS= read -r l_exec_plan_inherit_name; do
		[ -n "$l_exec_plan_inherit_name" ] || continue
		zxfer_run_destination_property_verb inherit \
			"Error when inheriting properties on destination filesystem." \
			"$l_exec_plan_dest" "$l_exec_plan_inherit_name" </dev/null
	done <<EOF
$l_exec_plan_inherits
EOF
}

################################################################################
# TOP-LEVEL PROPERTY TRANSFER
################################################################################

# Purpose: Reconcile one dataset's properties: read the source, check the -o
# properties against the initial source, plan the override and creation lists
# from -P/-o/-I/-U, create the destination with its creation-time properties
# when it is missing, otherwise diff against the destination in the same plan
# and apply the resulting sets and inherits, then buffer -k metadata.
# Usage: zxfer_transfer_properties SOURCE, after session policy preparation; called from
# the replication loop for every dataset that needs property work and again
# for the post-seed reconcile pass, which re-captures the dataset's -k row
# (the write keeps the newest row). Reads g_initial_source, g_actual_dest and
# g_recursive_dest_list. Owns l_transfer_* views: raw is reserved for -k,
# effective is the restored/backfilled planner input. The selected action file
# stays in this operation until execution; capture retains its original view.
zxfer_transfer_properties() {
	zxfer_set_failure_stage "property transfer"
	zxfer_echoV "zxfer_transfer_properties: $1"
	zxfer_echoV "initial_source: $g_initial_source"

	l_transfer_source=$1
	if [ "$g_initial_source" = "$l_transfer_source" ]; then
		l_transfer_is_initial_source=1
	else
		l_transfer_is_initial_source=0
	fi

	# Source properties, create-time metadata, and required-property backfill
	# for both the raw (-k) and effective (apply) views.
	zxfer_load_normalized_dataset_properties "$l_transfer_source" source ||
		zxfer_throw_error "${g_zxfer_property_error_result:-Failed to retrieve source properties for [$l_transfer_source].}" "$?"
	l_transfer_raw_pvs=$g_zxfer_normalized_dataset_properties
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		zxfer_require_restored_backup_properties "$l_transfer_source" "$g_actual_dest"
		l_transfer_restored_pvs=$g_zxfer_property_row_payload_result
	fi
	zxfer_get_validated_source_dataset_create_metadata "$l_transfer_source" "$l_transfer_raw_pvs" ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	l_transfer_source_dstype=$g_zxfer_source_dataset_type_result
	l_transfer_source_volsize=$g_zxfer_source_volume_size_result
	case $l_transfer_source_dstype in
	volume)
		l_transfer_must_create_properties=""
		l_transfer_unsupported_properties=${g_zxfer_unsupported_volume_properties:-}
		;;
	*)
		l_transfer_must_create_properties=$ZXFER_REQUIRED_CREATION_PROPERTIES
		l_transfer_unsupported_properties=${g_zxfer_unsupported_filesystem_properties:-}
		;;
	esac
	[ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ] ||
		l_transfer_unsupported_properties=""
	l_transfer_src_backfilled=0
	l_transfer_dst_backfilled=0
	zxfer_backfill_required_properties "$l_transfer_source" "$l_transfer_raw_pvs" \
		"$l_transfer_must_create_properties" source ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	[ "$g_zxfer_required_properties_result" = "$l_transfer_raw_pvs" ] ||
		l_transfer_src_backfilled=1
	l_transfer_raw_pvs=$g_zxfer_required_properties_result
	l_transfer_effective_pvs=$l_transfer_raw_pvs
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		zxfer_backfill_required_properties "$l_transfer_source" "$l_transfer_restored_pvs" \
			"$l_transfer_must_create_properties" source "$l_transfer_raw_pvs" ||
			zxfer_throw_error "$g_zxfer_property_error_result" "$?"
		l_transfer_effective_pvs=$g_zxfer_required_properties_result
	fi

	# The -o policy, prepared at startup; the initial source must hold
	# every -o property before any destination property changes (-d may already
	# have destroyed destination-only snapshots).
	if [ "$l_transfer_is_initial_source" -eq 1 ]; then
		zxfer_check_override_properties_on_source "$g_zxfer_property_override_policy" \
			"$l_transfer_effective_pvs"
	fi

	# Retain both views across reads; existence is independent of payload size.
	l_transfer_destination_exists=0
	l_transfer_destination_pvs=""
	if zxfer_property_destination_exists "$g_actual_dest"; then
		l_transfer_destination_exists=1
		zxfer_load_normalized_dataset_properties "$g_actual_dest" destination ||
			zxfer_throw_error "Failed to retrieve destination properties for [$g_actual_dest]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
		zxfer_backfill_required_properties "$g_actual_dest" "$g_zxfer_normalized_dataset_properties" \
			"$l_transfer_must_create_properties" destination ||
			zxfer_throw_error "$g_zxfer_property_error_result" "$?"
		[ "$g_zxfer_required_properties_result" = "$g_zxfer_normalized_dataset_properties" ] ||
			l_transfer_dst_backfilled=1
		l_transfer_destination_pvs=$g_zxfer_required_properties_result
	fi
	if [ "$l_transfer_destination_exists" -eq 1 ] &&
		[ "$g_option_e_restore_property_mode" -eq 0 ] &&
		[ "$l_transfer_src_backfilled" -eq 0 ] &&
		[ "$l_transfer_dst_backfilled" -eq 0 ] &&
		zxfer_try_use_prepared_property_plan "$l_transfer_source" "$g_actual_dest"; then
		l_transfer_plan_file=$g_zxfer_property_batch_plan_file_result
	else
		zxfer_plan_property_changes "$l_transfer_is_initial_source" "$l_transfer_destination_exists" \
			"$l_transfer_source_dstype" "$l_transfer_effective_pvs" "$l_transfer_destination_pvs" \
			"$l_transfer_unsupported_properties"
		l_transfer_plan_file=$g_zxfer_property_plan_file
	fi
	l_transfer_action_count=${g_zxfer_property_plan_summary_result%% *}
	zxfer_show_property_plan "$l_transfer_plan_file"
	if [ "$l_transfer_destination_exists" -eq 0 ]; then
		zxfer_create_destination_dataset "$l_transfer_plan_file" "$l_transfer_is_initial_source" \
			"$l_transfer_source_dstype" "$l_transfer_source_volsize" "$g_actual_dest" "$g_zxfer_property_readonly_policy"
		zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$l_transfer_raw_pvs"
		return 0
	fi
	l_transfer_parent_reconciled=0
	if [ "$l_transfer_is_initial_source" -eq 0 ] && [ "$l_transfer_action_count" -ne 0 ]; then
		l_transfer_parent=${g_actual_dest%/*}
		if [ "$l_transfer_parent" != "$g_actual_dest" ] &&
			zxfer_destination_exists "$l_transfer_parent"; then
			zxfer_load_normalized_dataset_properties "$l_transfer_parent" destination ||
				zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$?"
			if [ "$g_zxfer_normalized_dataset_properties_cache_hit" -eq 0 ]; then
				g_zxfer_profile_parent_destination_property_reads=$((g_zxfer_profile_parent_destination_property_reads + 1))
			fi
			zxfer_resolve_parent_property_plan "$l_transfer_plan_file" "$g_zxfer_normalized_dataset_properties" "$g_zxfer_property_readonly_policy" apply ||
				zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$?"
			l_transfer_plan_file=$g_zxfer_property_parent_plan_file
			l_transfer_parent_reconciled=1
		fi
		if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
			l_transfer_adjusted_set=""
			l_transfer_adjusted_inherit=""
			{
				while IFS="$ZXFER_TAB" read -r l_transfer_adj_verb l_transfer_adj_item; do
					case $l_transfer_adj_verb in
					set | inherit)
						l_transfer_adj_name=${l_transfer_adj_item%%=*}
						l_transfer_adj_value=${l_transfer_adj_item#*=}
						l_transfer_adj_pair=$l_transfer_adj_name=${l_transfer_adj_value%%=*}
						if [ "$l_transfer_adj_verb" = set ]; then
							l_transfer_adjusted_set=${l_transfer_adjusted_set:+$l_transfer_adjusted_set,}$l_transfer_adj_pair
						else
							l_transfer_adjusted_inherit=${l_transfer_adjusted_inherit:+$l_transfer_adjusted_inherit,}$l_transfer_adj_pair
						fi
						;;
					esac
				done
			} <"$l_transfer_plan_file" || zxfer_throw_error "Failed to read dataset property plan." "$?"
			zxfer_echoV_escaped "zxfer_transfer_properties adjusted child_set" "$l_transfer_adjusted_set"
			zxfer_echoV_escaped "zxfer_transfer_properties adjusted inherit" "$l_transfer_adjusted_inherit"
		fi
	fi
	if [ "$l_transfer_action_count" -eq 0 ] && [ "$l_transfer_parent_reconciled" -eq 0 ]; then
		if [ ! -f "$l_transfer_plan_file" ] || [ ! -r "$l_transfer_plan_file" ]; then
			zxfer_throw_error "Failed to read dataset property plan." 1
		fi
	else
		zxfer_execute_property_plan apply "$l_transfer_plan_file" "" "" "$g_actual_dest" ||
			zxfer_throw_error "Failed to read dataset property plan." "$?"
	fi
	zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$l_transfer_raw_pvs"
}
