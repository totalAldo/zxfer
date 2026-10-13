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
#   constants, the filter, plan and child AWK programs, the run-wide
#   unsupported-property lists (g_zxfer_unsupported_*_properties), the
#   prepared g_zxfer_property_readonly_policy/g_zxfer_property_override_policy,
#   reusable planner input, action and parent-work files,
#   result globals g_zxfer_override_properties_result,
#   g_zxfer_source_dataset_type_result, g_zxfer_source_volume_size_result
#   and the small g_zxfer_property_plan_summary_result (action and override
#   counts), cleared before planning. It also writes the property state module's
#   failure text g_zxfer_property_error_result.
# reads globals: the property CLI options, -R/-n, the destination platform and -m
#   state, g_initial_source, g_actual_dest, the recursive source and
#   destination lists, the restore metadata, and the rendered -T command in
#   g_zxfer_shell_command_result.
# mutates caches: the unsupported-property lists, destination property rows
#   (dropped by the state module's invalidation after every create, set and
#   inherit), and the destination existence cache through probes.
# returns via stdout: diagnostic plan lists and rendered destination commands
#   on dry runs. Property values remain encoded until command construction.
#
# Per dataset, zxfer_transfer_properties reads the source, checks the -o
# properties against the initial source, decides whether the destination
# exists, then runs one plan awk: derive the apply and creation lists, and
# diff them against the destination when it exists. A missing destination is
# created; an existing one gets its sets and inherits, a child's after they
# are adjusted to what its destination parent already provides.

ZXFER_BASE_READONLY_PROPERTIES="type,creation,used,available,referenced,\
compressratio,mounted,version,primarycache,secondarycache,\
usedbysnapshots,usedbydataset,usedbychildren,usedbyrefreservation,\
version,volsize,mountpoint,mlslabel,keysource,keystatus,rekeydate,encryption,encryptionroot,keylocation,keyformat,pbkdf2iters,snapshots_changed,special_small_blocks,\
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
			l_override_name=$(zxfer_escape_report_value "$l_override_name") ||
				l_override_name="[unprintable]"
			zxfer_throw_usage_error "Duplicate property for -o override: $l_override_name."
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
		l_override_check_name=$(zxfer_escape_report_value "$g_zxfer_decoded_property_value") ||
			l_override_check_name="[unprintable]"
		zxfer_throw_usage_error "Missing source property for -o override: $l_override_check_name."
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

# Purpose: Record, per source dataset type, the source properties the
# destination cannot accept, so -U can drop them before create and set.
# Usage: Called once at replication startup when -U has property work. Source
# types come from batched `zfs get type` calls of at most 128 names each. For
# each type, the property names of its first source dataset are compared with
# those of a destination probe dataset (the first existing mapped destination
# of that type, else the destination pool root), and only the names missing on
# the destination are probed one by one. User properties (with a colon) are
# valid everywhere and never probed. Every remote call inside a here-document
# loop reads /dev/null, so an -O or -T ssh cannot drain the loop's input.
zxfer_calculate_unsupported_properties() {
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
	l_scan_sources=${g_recursive_source_list:-$g_initial_source}
	[ -n "$l_scan_sources" ] || return 0

	# The trailing empty line ends the list and flushes the last batch.
	l_scan_types=""
	set --
	while IFS= read -r l_scan_source; do
		if [ -n "$l_scan_source" ]; then
			set -- "$@" "$l_scan_source"
			[ "$#" -ge 128 ] || continue
		fi
		[ "$#" -gt 0 ] || continue
		l_scan_status=0
		l_scan_batch=$(zxfer_run_source_zfs_cmd get -Hpo name,value type "$@" 2>&1 </dev/null) ||
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

		l_scan_status=0
		l_scan_source_names=$(zxfer_run_source_zfs_cmd get -Hpo property all "$l_scan_source" 2>&1 </dev/null) ||
			l_scan_status=$?
		[ "$l_scan_status" -eq 0 ] ||
			zxfer_throw_error "Failed to retrieve source property list for dataset [$l_scan_source]: $l_scan_source_names" "$l_scan_status"

		# The existence cache is seeded from the recursive destination
		# listing, so these probes normally answer without a zfs call.
		l_scan_probe=""
		while IFS=$ZXFER_TAB read -r l_scan_row_source l_scan_row_type; do
			[ "$l_scan_row_type" = "$l_scan_type" ] || continue
			zxfer_map_destination_dataset "$l_scan_row_source"
			[ -n "$l_scan_probe" ] || l_scan_probe=${g_zxfer_destination_dataset_result%%/*}
			zxfer_probe_destination_existence "$g_zxfer_destination_dataset_result" </dev/null ||
				zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
			if [ "$g_zxfer_destination_exists_result" -eq 1 ]; then
				l_scan_probe=$g_zxfer_destination_dataset_result
				break
			fi
		done <<EOF
$l_scan_types
EOF
		[ -n "$l_scan_probe" ] ||
			zxfer_throw_error "Failed to determine the destination property-support probe dataset."

		l_scan_status=0
		l_scan_probe_type=$(zxfer_run_destination_zfs_cmd get -Hpo value type "$l_scan_probe" 2>&1 </dev/null) ||
			l_scan_status=$?
		[ "$l_scan_status" -eq 0 ] ||
			zxfer_throw_error "Failed to determine the destination property-support probe dataset type for [$l_scan_probe]: $l_scan_probe_type" "$l_scan_status"
		l_scan_dest_names=$(zxfer_run_destination_zfs_cmd get -Hpo property all "$l_scan_probe" 2>&1 </dev/null) ||
			l_scan_status=$?
		[ "$l_scan_status" -eq 0 ] ||
			zxfer_throw_error "Failed to retrieve destination property list for dataset [$l_scan_probe]: $l_scan_dest_names" "$l_scan_status"

		# shellcheck disable=SC2016
		l_scan_candidates=$(
			ZXFER_AWK_DESTINATION_PROPERTIES=$l_scan_dest_names "${g_cmd_awk:-awk}" '
BEGIN {
	count = split(ENVIRON["ZXFER_AWK_DESTINATION_PROPERTIES"], names, "\n")
	for (i = 1; i <= count; i++)
		supported[names[i]] = 1
}
$0 != "" && !($0 in supported) && index($0, ":") == 0 && !seen[$0]++ { print }
' <<EOF
$l_scan_source_names
EOF
		) || zxfer_throw_error "Failed to compare source and destination property inventories for [$l_scan_probe]."

		# zfs reports "does not apply" for a property of another dataset type,
		# so that answer only counts when the probe has the source's type.
		while IFS= read -r l_scan_property; do
			[ -n "$l_scan_property" ] || continue
			l_scan_status=0
			l_scan_output=$(zxfer_run_destination_zfs_cmd get -Hpo property,value,source \
				"$l_scan_property" "$l_scan_probe" 2>&1 </dev/null) || l_scan_status=$?
			[ "$l_scan_status" -ne 0 ] || continue
			case $l_scan_output in
			*"invalid property"* | *"no such property"* | *"not supported"*)
				zxfer_append_unsupported_property "$l_scan_type" "$l_scan_property"
				continue
				;;
			*"does not apply"*)
				[ "$l_scan_probe_type" != "$l_scan_type" ] ||
					zxfer_append_unsupported_property "$l_scan_type" "$l_scan_property"
				continue
				;;
			esac
			if zxfer_destination_probe_is_ambiguous "$l_scan_output"; then
				l_scan_output="probe exited nonzero without stdout/stderr"
			fi
			zxfer_throw_error "Failed to probe destination support for property [$l_scan_property] on [$l_scan_probe]: $l_scan_output" "$l_scan_status"
		done <<EOF
$l_scan_candidates
EOF
	done <<EOF
$l_scan_types
EOF
}

################################################################################
# SOURCE SIDE
################################################################################

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

	l_metadata_type=""
	if zxfer_property_list_value "$l_metadata_pvs" type; then
		l_metadata_type=${g_zxfer_property_list_value_result%%=*}
	fi
	if [ -z "$l_metadata_type" ]; then
		l_metadata_status=0
		l_metadata_type=$(zxfer_run_source_zfs_cmd get -Hpo value type "$l_metadata_source" 2>&1) ||
			l_metadata_status=$?
		if [ "$l_metadata_status" -ne 0 ]; then
			g_zxfer_property_error_result="Failed to retrieve source dataset type for [$l_metadata_source]: $l_metadata_type"
			return "$l_metadata_status"
		fi
	fi

	case $l_metadata_type in
	filesystem) ;;
	volume)
		l_metadata_volsize=""
		if zxfer_property_list_value "$l_metadata_pvs" volsize; then
			l_metadata_volsize=${g_zxfer_property_list_value_result%%=*}
		fi
		if [ -z "$l_metadata_volsize" ]; then
			l_metadata_status=0
			l_metadata_volsize=$(zxfer_run_source_zfs_cmd get -Hpo value volsize "$l_metadata_source" 2>&1) ||
				l_metadata_status=$?
			if [ "$l_metadata_status" -ne 0 ]; then
				g_zxfer_property_error_result="Failed to retrieve source zvol size for [$l_metadata_source]: $l_metadata_volsize"
				return "$l_metadata_status"
			fi
		fi
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
	if [ -z "${g_zxfer_property_input_file:-}" ]; then
		zxfer_get_temp_file || return "$?"
		g_zxfer_property_input_file=$g_zxfer_runtime_artifact_path_result
	fi
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
function filter_property_list(list, drop_unsupported, count, items, i, fields, field_count, output) {
	count = split(list, items, ",")
	output = ""
	for (i = 1; i <= count; i++) {
		if (items[i] == "")
			continue
		field_count = split(items[i], fields, "=")
		if ((fields[1] in remove_property) && (field_count < 3 || fields[3] != "override"))
			continue
		if (drop_unsupported && (fields[1] in unsupported_property)) {
			if (verbose == 1)
				warnings[++warning_count] = "Destination does not support property " fields[1] "=" decode_value(fields[2])
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

# Plan one dataset in one AWK. The file holds only selected create/set/inherit
# actions, with encoded values and first-source/override provenance for late
# parent reconciliation. -V alone retains the diagnostic CSV views. Checked
# close and a final record precede the stdout marker and warnings; validation
# publishes small action/override counts. Creation mismatches retain status 3
# and the original property-name and warning protocol.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_PLAN_AWK='
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
function print_warnings(i) {
	for (i = 1; i <= warning_count; i++)
		print warnings[i]
}
BEGIN {
	output_path = ENVIRON["ZXFER_PROPERTY_PLAN_FILE"]
	printf "" > output_path
	if (source_dstype != "volume")
		csv_to_set(required_creation_properties, required_create)

	# Derive. The -o items arrive parsed, encoded and unique. Without -P they
	# lead both lists; with -P they replace the source values they name.
	first_values(property_input["override"], override_value, override_source)
	if (transfer_all_flag == 0) {
		override_count = split(property_input["override"], override_items, ",")
		for (i = 1; i <= override_count; i++) {
			if (override_items[i] == "")
				continue
			split(override_items[i], fields, "=")
			override_output = append_csv(override_output, override_items[i])
			append_creation(fields[1], fields[2], "override")
		}
	}

	source_count = split(property_input["source"], source_items, ",")
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
		dest_output = filter_property_list(property_input["destination"], 0)
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
				print property
				print_warnings()
				exit 3
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
	print "__ZXFER_PROPERTY_ACTIONS__" > output_path
	if (close(output_path) != 0) exit 1
	print "__ZXFER_PROPERTY_PLAN__"
	print_warnings()
}'

# Purpose: Validate a complete action file before any property mutation.
# Usage: zxfer_validate_property_plan FILE; returns 1 on read failure,
# unknown records, a missing final marker or data after it.
zxfer_validate_property_plan() {
	g_zxfer_property_plan_summary_result=""
	l_validate_property_plan_complete=0
	l_validate_property_plan_actions=0
	l_validate_property_plan_overrides=0
	{
		while IFS= read -r l_validate_property_plan_row; do
			[ "$l_validate_property_plan_complete" -eq 0 ] || return 1
			case $l_validate_property_plan_row in
			"__ZXFER_PROPERTY_ACTIONS__") l_validate_property_plan_complete=1 ;;
			create"$ZXFER_TAB"?*=*=*=* | set"$ZXFER_TAB"?*=*=*=* | inherit"$ZXFER_TAB"?*=*=*=*)
				l_validate_property_plan_actions=$((l_validate_property_plan_actions + 1))
				l_validate_property_plan_source=${l_validate_property_plan_row#*"$ZXFER_TAB"}
				case $l_validate_property_plan_source in
				*"$ZXFER_TAB"*) return 1 ;;
				esac
				l_validate_property_plan_source=${l_validate_property_plan_source#*=}
				l_validate_property_plan_source=${l_validate_property_plan_source#*=}
				l_validate_property_plan_desired=${l_validate_property_plan_source#*=}
				case $l_validate_property_plan_desired in *'='*) return 1 ;; esac
				case $l_validate_property_plan_source in
				override=*) l_validate_property_plan_overrides=$((l_validate_property_plan_overrides + 1)) ;;
				esac
				;;
			view"$ZXFER_TAB"?*=*) ;;
			*) return 1 ;;
			esac
		done
		[ -z "$l_validate_property_plan_row" ] && [ "$l_validate_property_plan_complete" -eq 1 ]
	} 2>/dev/null <"$1" || return 1
	g_zxfer_property_plan_summary_result="$l_validate_property_plan_actions $l_validate_property_plan_overrides"
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
	zxfer_ensure_runtime_artifact_file "$g_zxfer_property_plan_file" property-plan ||
		zxfer_throw_error "Failed to plan dataset properties." "$?"
	g_zxfer_property_plan_file=$g_zxfer_runtime_artifact_path_result
	zxfer_write_runtime_artifact_file "$g_zxfer_property_plan_file" "" ||
		zxfer_throw_error "Failed to plan dataset properties." "$?"
	l_property_plan_status=0
	l_property_plan_output=$(
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
			"$ZXFER_PROPERTY_INPUT_AWK$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_PROPERTY_PLAN_AWK"
	) || l_property_plan_status=$?
	case $l_property_plan_status in
	0 | 3) ;;
	*) zxfer_throw_error "Failed to plan dataset properties." ;;
	esac
	{
		IFS= read -r l_property_plan_header
		while IFS= read -r l_property_plan_warning; do
			[ -z "$l_property_plan_warning" ] || zxfer_warn_stderr "$l_property_plan_warning"
		done
	} <<EOF
$l_property_plan_output
EOF
	if [ "$l_property_plan_status" -eq 3 ]; then
		zxfer_throw_error_with_usage "The property \"$l_property_plan_header\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
	fi
	case $l_property_plan_header in
	"__ZXFER_PROPERTY_PLAN__") ;;
	*) zxfer_throw_error "Failed to plan dataset properties." ;;
	esac
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
	zxfer_probe_destination_existence "$1" live ||
		zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
	[ "$g_zxfer_destination_exists_result" -ne 0 ] || return 1
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
		zxfer_probe_destination_existence "$l_missing_parent" ||
			zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
		l_missing_parent_exists=$g_zxfer_destination_exists_result
	fi
	if [ "$l_missing_initial" -eq 0 ] && [ "$l_missing_parent_exists" = 1 ] &&
		[ "${g_zxfer_property_plan_summary_result#* }" -ne 0 ]; then
		zxfer_load_normalized_dataset_properties "$l_missing_parent" destination ||
			zxfer_throw_error "Failed to retrieve parent destination properties for [$l_missing_parent]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
		zxfer_resolve_parent_property_plan "$l_missing_plan" "$g_zxfer_normalized_dataset_properties" "$l_missing_readonly" create ||
			zxfer_throw_error "Failed to filter child creation override properties." "$?"
		l_missing_plan=$g_zxfer_property_parent_plan_file
	fi
	if [ -n "${g_option_R_recursive:-}" ] && [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_warn_stderr "zxfer: destination dataset [$l_missing_dataset] is missing; attempting creation before continuing recursive replication."
	fi
	l_missing_with_parents=no
	if [ "$l_missing_parent_exists" = 0 ]; then
		if [ "${g_zxfer_property_plan_summary_result%% *}" -ne 0 ]; then
			zxfer_execute_property_plan create "" filesystem "" "$l_missing_parent" yes ||
				zxfer_throw_error "Error when creating destination filesystem." "$?"
			if [ "$g_option_n_dryrun" -eq 0 ]; then
				zxfer_note_destination_dataset_exists "$l_missing_parent"
				zxfer_invalidate_destination_property_mutation_cache "$l_missing_parent"
			fi
		else
			l_missing_with_parents=yes
		fi
	fi
	zxfer_execute_property_plan create "$l_missing_plan" "$l_missing_type" "$l_missing_volsize" "$l_missing_dataset" "$l_missing_with_parents" ||
		zxfer_throw_error "Error when creating destination filesystem." "$?"
	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_note_destination_dataset_exists "$l_missing_dataset"
		zxfer_invalidate_destination_property_mutation_cache "$l_missing_dataset"
	fi
}

################################################################################
# DESTINATION SET / INHERIT COMMANDS
################################################################################

# Purpose: Render one destination zfs command line for dry runs and -v display.
# Usage: zxfer_build_destination_zfs_command SUBCOMMAND [ARG...]; prints the
# report-quoted local command, or the ssh command line under -T. That line
# carries values raw, so it is escaped as a report value when it holds a
# control byte.
zxfer_build_destination_zfs_command() {
	if [ -z "$g_option_T_target_host" ]; then
		zxfer_render_command_for_report "" "$g_cmd_zfs" "$@"
		return
	fi
	zxfer_render_zfs_command_for_role destination "$@" || return
	# Without character classes in case patterns (posh), always escape.
	if [ "$g_zxfer_report_fast_path" = 1 ]; then
		case $g_zxfer_shell_command_result in
		*[[:cntrl:]]*) ;;
		*)
			printf '%s\n' "$g_zxfer_shell_command_result"
			return
			;;
		esac
	fi
	zxfer_escape_report_value "$g_zxfer_shell_command_result"
	printf '\n'
}

# Purpose: Run one destination property verb (set or inherit) live, or print
# it on dry runs.
# Usage: zxfer_run_destination_property_verb VERB ERROR_MESSAGE DESTINATION
# [ARG...]. A live run shows the command under -v, then drops the
# destination's cached rows. A failed run, or a dry-run line that cannot be
# rendered, throws ERROR_MESSAGE with the failing status.
zxfer_run_destination_property_verb() {
	l_verb=$1
	l_verb_error_message=$2
	l_verb_destination=$3
	shift 3

	if [ "$g_option_n_dryrun" -ne 0 ]; then
		zxfer_build_destination_zfs_command "$l_verb" "$@" "$l_verb_destination" ||
			zxfer_throw_error "$l_verb_error_message" "$?"
		return 0
	fi
	if zxfer_command_display_render_enabled; then
		zxfer_echov "$(zxfer_build_destination_zfs_command "$l_verb" "$@" "$l_verb_destination")"
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
		else if (verb != "view") exit 1
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

# Purpose: Build create/set argv once from encoded action rows; apply ordered
# inherits only after the single set. Never pass a decoded value to eval.
# Usage: zxfer_execute_property_plan create|apply FILE TYPE VOLSIZE DEST [yes|no].
# Empty FILE is allowed only for property-free parent creation. Create returns
# its original status; set/inherit retain their centralized failure reports.
zxfer_execute_property_plan() {
	case $1 in create | apply) ;; *) return 1 ;; esac
	l_execute_property_plan_mode=$1
	l_execute_property_plan_file=$2
	l_execute_property_plan_type=$3
	l_execute_property_plan_size=$4
	l_execute_property_plan_dest=$5
	l_execute_property_plan_parents=${6:-no}
	if [ -n "$l_execute_property_plan_file" ]; then
		zxfer_validate_property_plan "$l_execute_property_plan_file" || return 1
	else
		[ "$l_execute_property_plan_mode" = create ] || return 1
	fi
	if [ "$l_execute_property_plan_mode" = create ]; then
		[ "$l_execute_property_plan_type" != volume ] || [ -n "$l_execute_property_plan_size" ] || return 1
		set -- create
		[ "$l_execute_property_plan_parents" != yes ] || set -- "$@" -p
		[ "$l_execute_property_plan_type" != volume ] || set -- "$@" -V "$l_execute_property_plan_size"
	else
		set --
		if [ "${g_option_v_verbose:-0}" -eq 1 ] && [ "${g_zxfer_property_plan_summary_result%% *}" -ne 0 ]; then
			zxfer_echov "Setting properties/sources on destination filesystem \"$l_execute_property_plan_dest\"."
			for l_execute_property_plan_kind in set inherit; do
				l_execute_property_plan_display=$(zxfer_property_plan_list "$l_execute_property_plan_file" "$l_execute_property_plan_kind") ||
					zxfer_throw_error "Failed to read dataset property plan." "$?"
				[ -n "$l_execute_property_plan_display" ] || continue
				zxfer_decode_serialized_property_list_for_display "$l_execute_property_plan_display"
				zxfer_echov "Property $l_execute_property_plan_kind list: $(zxfer_escape_report_value "$g_zxfer_property_display_list_result")"
			done
		fi
	fi
	if [ -n "$l_execute_property_plan_file" ]; then
		{
			while IFS="$ZXFER_TAB" read -r l_execute_property_plan_verb l_execute_property_plan_item; do
				case $l_execute_property_plan_verb in
				create | set)
					case $l_execute_property_plan_mode,$l_execute_property_plan_verb in
					create,create | apply,set) ;; *) return 1 ;; esac
					[ "$l_execute_property_plan_parents" != yes ] || return 1
					l_execute_property_plan_value=${l_execute_property_plan_item#*=}
					zxfer_decode_property_value "${l_execute_property_plan_value%%=*}"
					[ "$l_execute_property_plan_mode" != create ] || set -- "$@" -o
					set -- "$@" "${l_execute_property_plan_item%%=*}=$g_zxfer_decoded_property_value"
					;;
				inherit) [ "$l_execute_property_plan_mode" = apply ] || return 1 ;;
				esac
			done
		} <"$l_execute_property_plan_file" || return "$?"
	fi
	if [ "$l_execute_property_plan_mode" = create ]; then
		if [ "$g_option_n_dryrun" -eq 0 ]; then
			zxfer_run_destination_zfs_cmd "$@" "$l_execute_property_plan_dest"
		else
			zxfer_build_destination_zfs_command "$@" "$l_execute_property_plan_dest"
		fi
		return "$?"
	fi
	[ "$#" -eq 0 ] || zxfer_run_destination_property_verb set \
		"Error when setting properties on destination filesystem." "$l_execute_property_plan_dest" "$@"
	{
		while IFS="$ZXFER_TAB" read -r l_execute_property_plan_verb l_execute_property_plan_item; do
			[ "$l_execute_property_plan_verb" = inherit ] || continue
			zxfer_run_destination_property_verb inherit \
				"Error when inheriting properties on destination filesystem." \
				"$l_execute_property_plan_dest" "${l_execute_property_plan_item%%=*}" </dev/null
		done
	} <"$l_execute_property_plan_file" || return "$?"
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
	l_transfer_effective_pvs=$l_transfer_raw_pvs
	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		l_transfer_restore_status=0
		zxfer_find_restored_backup_properties "$l_transfer_source" "$g_actual_dest" ||
			l_transfer_restore_status=$?
		case $l_transfer_restore_status in
		0) l_transfer_effective_pvs=$g_zxfer_property_row_payload_result ;;
		3 | 8)
			zxfer_throw_usage_error "Can't find the properties for the filesystem $l_transfer_source and destination $g_actual_dest"
			;;
		9)
			zxfer_throw_usage_error "Multiple restored property entries matched filesystem $l_transfer_source and destination $g_actual_dest"
			;;
		*)
			zxfer_throw_usage_error "Failed to parse the restored properties for the filesystem $l_transfer_source and destination $g_actual_dest"
			;;
		esac
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
	zxfer_backfill_required_properties "$l_transfer_source" "$l_transfer_raw_pvs" \
		"$l_transfer_must_create_properties" source ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	l_transfer_raw_pvs=$g_zxfer_required_properties_result
	zxfer_backfill_required_properties "$l_transfer_source" "$l_transfer_effective_pvs" \
		"$l_transfer_must_create_properties" source "$l_transfer_raw_pvs" ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	l_transfer_effective_pvs=$g_zxfer_required_properties_result

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
		l_transfer_destination_pvs=$g_zxfer_required_properties_result
	fi
	zxfer_plan_property_changes "$l_transfer_is_initial_source" "$l_transfer_destination_exists" \
		"$l_transfer_source_dstype" "$l_transfer_effective_pvs" "$l_transfer_destination_pvs" \
		"$l_transfer_unsupported_properties"
	set -- "$g_zxfer_property_plan_file"
	zxfer_show_property_plan "$1"
	if [ "$l_transfer_destination_exists" -eq 0 ]; then
		zxfer_create_destination_dataset "$1" "$l_transfer_is_initial_source" \
			"$l_transfer_source_dstype" "$l_transfer_source_volsize" "$g_actual_dest" "$g_zxfer_property_readonly_policy"
		zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$l_transfer_raw_pvs"
		return 0
	fi
	if [ "$l_transfer_is_initial_source" -eq 0 ] && [ "${g_zxfer_property_plan_summary_result%% *}" -ne 0 ]; then
		l_transfer_parent=${g_actual_dest%/*}
		if [ "$l_transfer_parent" != "$g_actual_dest" ]; then
			zxfer_probe_destination_existence "$l_transfer_parent" ||
				zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
			if [ "$g_zxfer_destination_exists_result" -eq 1 ]; then
				zxfer_load_normalized_dataset_properties "$l_transfer_parent" destination ||
					zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$?"
				if [ "$g_zxfer_normalized_dataset_properties_cache_hit" -eq 0 ]; then
					g_zxfer_profile_parent_destination_property_reads=$((g_zxfer_profile_parent_destination_property_reads + 1))
				fi
				zxfer_resolve_parent_property_plan "$1" "$g_zxfer_normalized_dataset_properties" "$g_zxfer_property_readonly_policy" apply ||
					zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$?"
				set -- "$g_zxfer_property_parent_plan_file"
			fi
		fi
		if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
			zxfer_echoV_escaped "zxfer_transfer_properties adjusted child_set" "$(zxfer_property_plan_list "$1" set)"
			zxfer_echoV_escaped "zxfer_transfer_properties adjusted inherit" "$(zxfer_property_plan_list "$1" inherit)"
		fi
	fi
	zxfer_execute_property_plan apply "$1" "" "" "$g_actual_dest" ||
		zxfer_throw_error "Failed to read dataset property plan." "$?"
	zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$l_transfer_raw_pvs"
}
