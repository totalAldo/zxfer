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
#   unsupported-property lists (g_zxfer_unsupported_*_properties), and the
#   result globals g_zxfer_readonly_properties_result,
#   g_zxfer_override_properties_result, g_zxfer_source_pvs_raw/effective,
#   g_zxfer_source_dataset_type_result, g_zxfer_source_volume_size_result,
#   g_zxfer_plan_*_result and g_zxfer_adjusted_*_list, each cleared by the
#   function that publishes it. It also writes the property state module's
#   failure text g_zxfer_property_error_result.
# reads globals: the property CLI options, the destination platform and -m
#   state, g_initial_source, g_actual_dest, the recursive source and
#   destination lists, the restore metadata, and the rendered -T command in
#   g_zxfer_shell_command_result.
# mutates caches: the unsupported-property lists, destination property rows
#   (dropped by the state module's invalidation after every create, set and
#   inherit), and the destination existence cache through probes.
# returns via stdout: zxfer_filter_child_creation_overrides_for_parent, and
#   rendered destination commands on dry runs.
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

# Purpose: Reset the run-wide unsupported-property lists.
# Usage: Called at session initialization and before a fresh -U scan.
zxfer_reset_property_runtime_state() {
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
}

# Purpose: Resolve the readonly property list for the destination platform,
# without mountpoint when -m migration must move mountpoints.
# Usage: zxfer_resolve_readonly_properties; publishes
# g_zxfer_readonly_properties_result. Not memoized: every transfer resolves it
# from the current platform and -m state.
zxfer_resolve_readonly_properties() {
	g_zxfer_readonly_properties_result=$ZXFER_BASE_READONLY_PROPERTIES
	if [ "${g_destination_operating_system:-}" = "FreeBSD" ] &&
		[ -n "$ZXFER_FREEBSD_READONLY_PROPERTIES" ]; then
		g_zxfer_readonly_properties_result=${g_zxfer_readonly_properties_result:+$g_zxfer_readonly_properties_result,}$ZXFER_FREEBSD_READONLY_PROPERTIES
	fi
	[ "${g_option_m_migrate:-0}" -eq 1 ] || return 0

	l_readonly_rest=$g_zxfer_readonly_properties_result,
	g_zxfer_readonly_properties_result=""
	while [ -n "$l_readonly_rest" ]; do
		l_readonly_name=${l_readonly_rest%%,*}
		l_readonly_rest=${l_readonly_rest#*,}
		case $l_readonly_name in
		"" | mountpoint) continue ;;
		esac
		g_zxfer_readonly_properties_result=${g_zxfer_readonly_properties_result:+$g_zxfer_readonly_properties_result,}$l_readonly_name
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
# calls it before any zfs command runs; each property transfer parses the
# text again instead of trusting a stored copy.
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
	zxfer_reset_property_runtime_state
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

# Purpose: Load the source property list and the effective list used for
# transfer (the restored -e view when enabled).
# Usage: zxfer_collect_source_props SOURCE DESTINATION; publishes
# g_zxfer_source_pvs_raw and g_zxfer_source_pvs_effective, or returns the zfs
# status with its diagnostic in g_zxfer_property_error_result. The source is
# read through the source zfs role, so it reaches the -O host. Startup has
# already validated the whole -e file and stored its rows, so each dataset
# only looks its row up.
zxfer_collect_source_props() {
	l_collect_source=$1
	l_collect_destination=$2

	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
	zxfer_load_normalized_dataset_properties "$l_collect_source" source || return "$?"
	g_zxfer_source_pvs_raw=$g_zxfer_normalized_dataset_properties
	g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return 0

	l_restore_status=0
	zxfer_find_restored_backup_properties "$l_collect_source" "$l_collect_destination" ||
		l_restore_status=$?
	case $l_restore_status in
	0) g_zxfer_source_pvs_effective=$g_zxfer_backup_restore_properties_result ;;
	3 | 8)
		zxfer_throw_usage_error "Can't find the properties for the filesystem $l_collect_source and destination $l_collect_destination"
		;;
	9)
		zxfer_throw_usage_error "Multiple restored property entries matched filesystem $l_collect_source and destination $l_collect_destination"
		;;
	*)
		zxfer_throw_usage_error "Failed to parse the restored properties for the filesystem $l_collect_source and destination $l_collect_destination"
		;;
	esac
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

# Shared rules prefix of the plan, child-create and child-inherit programs.
#   filter_property_list(list, drop_unsupported)  drop readonly and ignored
#       (-I) properties unless the item is an explicit override and, when
#       drop_unsupported is 1, destination-unsupported (-U) properties,
#       collecting one verbose warning per dropped unsupported item
#   sets_locally(property, source)  the inheritance rule: a local value or a
#       noninheritable property is set on the dataset itself; any other value
#       may be inherited from its parent
# The lists arrive in ZXFER_AWK_REMOVE_LIST and ZXFER_AWK_UNSUPPORTED_LIST,
# the noninheritable names and verbose with -v. Every caller sets both
# variables, empty when unused, so an exported value never leaks in. Runs
# behind ZXFER_PROPERTY_AWK_LIB.
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

# Plans one dataset from the source list in ZXFER_AWK_SOURCE_PVS, the parsed
# -o list in ZXFER_AWK_OVERRIDE_PVS and, with has_destination=1, the
# destination list in ZXFER_AWK_DEST_PVS. Derive builds the override (apply)
# and creation lists and filters both; diff filters the destination list
# with the readonly and -I lists only and compares the apply list with it.
# Prints the apply, creation, filtered destination, initial-set, child-set
# and inherit lists, one per line, then __ZXFER_PROPERTY_PLAN__ and one line
# per -U warning. A creation-time property that differs on the destination
# prints its name and the warnings and exits 3. Runs behind
# ZXFER_PROPERTY_AWK_LIB and ZXFER_PROPERTY_RULES_AWK.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_PLAN_AWK='
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
	if (source_dstype != "volume")
		csv_to_set(required_creation_properties, required_create)

	# Derive. The -o items arrive parsed, encoded and unique. Without -P they
	# lead both lists; with -P they replace the source values they name.
	first_values(ENVIRON["ZXFER_AWK_OVERRIDE_PVS"], override_value, override_source)
	if (transfer_all_flag == 0) {
		override_count = split(ENVIRON["ZXFER_AWK_OVERRIDE_PVS"], override_items, ",")
		for (i = 1; i <= override_count; i++) {
			if (override_items[i] == "")
				continue
			split(override_items[i], fields, "=")
			override_output = append_csv(override_output, override_items[i])
			append_creation(fields[1], fields[2], "override")
		}
	}

	source_count = split(ENVIRON["ZXFER_AWK_SOURCE_PVS"], source_items, ",")
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
		dest_output = filter_property_list(ENVIRON["ZXFER_AWK_DEST_PVS"], 0)
		first_values(dest_output, dest_value, dest_source)
		for (property in dest_value)
			dest_available[property] = 1

		plan_count = split(override_output, plan_items, ",")
		for (i = 1; i <= plan_count; i++) {
			if (plan_items[i] == "")
				continue
			split(plan_items[i], fields, "=")
			plan_property[i] = fields[1]
			plan_value[i] = fields[2]
			plan_source[i] = fields[3]
			if ((plan_property[i] in required_create) &&
				(plan_property[i] in dest_available) &&
				plan_value[i] != dest_value[plan_property[i]]) {
				print plan_property[i]
				print_warnings()
				exit 3
			}
		}

		for (i = 1; i <= plan_count; i++) {
			property = plan_property[i]
			if (property == "" || (property in required_create))
				continue
			item = property "=" plan_value[i]
			# Local and -o values are set on the replicated root itself.
			root_sets = (plan_source[i] == "local" || plan_source[i] == "override")
			if (!(property in dest_available)) {
				if (root_sets)
					initial_set_list = append_csv(initial_set_list, item)
				if (sets_locally(property, plan_source[i]))
					child_set_list = append_csv(child_set_list, item)
				else
					inherit_list = append_csv(inherit_list, item)
				continue
			}

			if (dest_value[property] != plan_value[i] ||
				(root_sets && dest_source[property] != "local"))
				initial_set_list = append_csv(initial_set_list, item)

			if (plan_value[i] != dest_value[property]) {
				if (sets_locally(property, plan_source[i]))
					child_set_list = append_csv(child_set_list, item)
				else
					inherit_list = append_csv(inherit_list, item)
			} else if (plan_source[i] == "local" &&
				dest_source[property] != "local") {
				child_set_list = append_csv(child_set_list, item)
			} else if (!sets_locally(property, plan_source[i]) &&
				dest_source[property] == "local") {
				inherit_list = append_csv(inherit_list, item)
			}

			delete dest_available[property]
		}
	}

	print override_output
	print creation_output
	print dest_output
	print initial_set_list
	print child_set_list
	print inherit_list
	print "__ZXFER_PROPERTY_PLAN__"
	print_warnings()
}'

# Purpose: Plan one dataset's properties with one awk: the override (apply)
# and creation lists from the source list, the parsed -o list and -P,
# filtered by the readonly, -I and -U lists (explicit overrides survive the
# readonly and -I filters; unsupported items are always dropped and reported
# on stderr when verbose), and, when the destination exists, the diff of its
# filtered list against the apply list.
# Usage: zxfer_plan_property_changes SOURCE_PVS OVERRIDE_LIST TRANSFER_ALL_FLAG
# DATASET_TYPE READONLY_CSV IGNORE_CSV UNSUPPORTED_CSV [DEST_PVS]; an eighth
# argument, even an empty one, means the destination exists. Publishes
# g_zxfer_plan_override_pvs_result and g_zxfer_plan_creation_pvs_result, and
# for an existing destination g_zxfer_plan_dest_pvs_result plus
# g_zxfer_plan_initial_set_result, g_zxfer_plan_child_set_result and
# g_zxfer_plan_inherit_result. Throws a usage error when a filesystem's
# creation-time property differs on the destination and an error when awk
# fails.
zxfer_plan_property_changes() {
	g_zxfer_plan_override_pvs_result=""
	g_zxfer_plan_creation_pvs_result=""
	g_zxfer_plan_dest_pvs_result=""
	g_zxfer_plan_initial_set_result=""
	g_zxfer_plan_child_set_result=""
	g_zxfer_plan_inherit_result=""
	l_property_plan_has_destination=0
	[ "$#" -lt 8 ] || l_property_plan_has_destination=1

	l_property_plan_status=0
	# Every list travels through the environment: awk -v would reinterpret
	# backslash escapes in property values.
	l_property_plan_output=$(
		ZXFER_AWK_SOURCE_PVS=$1 \
			ZXFER_AWK_OVERRIDE_PVS=$2 \
			ZXFER_AWK_REMOVE_LIST="$5,$6" \
			ZXFER_AWK_UNSUPPORTED_LIST=$7 \
			ZXFER_AWK_DEST_PVS=${8:-} \
			"${g_cmd_awk:-awk}" \
			-v transfer_all_flag="$3" \
			-v source_dstype="$4" \
			-v has_destination="$l_property_plan_has_destination" \
			-v required_creation_properties="$ZXFER_REQUIRED_CREATION_PROPERTIES" \
			-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
			-v verbose="${g_option_v_verbose:-0}" \
			"$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_PROPERTY_PLAN_AWK"
	) || l_property_plan_status=$?
	case $l_property_plan_status in
	0 | 3) ;;
	*) zxfer_throw_error "Failed to plan dataset properties." ;;
	esac

	# Six lists and the completion marker, or on a creation-time mismatch the
	# property alone; then one -U warning per line.
	l_property_plan_marker=""
	{
		IFS= read -r l_property_plan_override
		if [ "$l_property_plan_status" -eq 0 ]; then
			IFS= read -r l_property_plan_creation
			IFS= read -r l_property_plan_dest
			IFS= read -r l_property_plan_initial_set
			IFS= read -r l_property_plan_child_set
			IFS= read -r l_property_plan_inherit
			IFS= read -r l_property_plan_marker
		fi
		while IFS= read -r l_property_plan_warning; do
			[ -z "$l_property_plan_warning" ] || zxfer_warn_stderr "$l_property_plan_warning"
		done
	} <<EOF || :
$l_property_plan_output
EOF
	if [ "$l_property_plan_status" -eq 3 ]; then
		zxfer_throw_error_with_usage "The property \"$l_property_plan_override\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
	fi
	[ "$l_property_plan_marker" = "__ZXFER_PROPERTY_PLAN__" ] ||
		zxfer_throw_error "Failed to plan dataset properties."
	g_zxfer_plan_override_pvs_result=$l_property_plan_override
	g_zxfer_plan_creation_pvs_result=$l_property_plan_creation
	g_zxfer_plan_dest_pvs_result=$l_property_plan_dest
	g_zxfer_plan_initial_set_result=$l_property_plan_initial_set
	g_zxfer_plan_child_set_result=$l_property_plan_child_set
	g_zxfer_plan_inherit_result=$l_property_plan_inherit
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

# Purpose: Drop child create overrides the parent already supplies, so
# recursive -o overrides of inheritable properties stay inherited on
# descendants once the parent has the requested value.
# Usage: zxfer_filter_child_creation_overrides_for_parent CREATION_PVS
# PARENT_PVS READONLY_CSV; the parent list is filtered with the readonly and
# -I lists first. Prints the filtered creation list.
zxfer_filter_child_creation_overrides_for_parent() {
	# shellcheck disable=SC2016
	ZXFER_AWK_CREATION_PVS=$1 ZXFER_AWK_PARENT_PVS=$2 \
		ZXFER_AWK_REMOVE_LIST="${3:-},${g_option_I_ignore_properties:-}" \
		ZXFER_AWK_UNSUPPORTED_LIST='' "${g_cmd_awk:-awk}" \
		-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
		"$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK"'
BEGIN {
	first_values(filter_property_list(ENVIRON["ZXFER_AWK_PARENT_PVS"], 0), parent_value, parent_source)
	count = split(ENVIRON["ZXFER_AWK_CREATION_PVS"], items, ",")
	for (i = 1; i <= count; i++) {
		if (items[i] == "")
			continue
		split(items[i], fields, "=")
		if (fields[3] == "override" && !(fields[1] in noninheritable) &&
			(fields[1] in parent_value) && parent_value[fields[1]] == fields[2])
			continue
		output = append_csv(output, items[i])
	}
	print output
}'
}

# Purpose: Build and run `zfs create` with each property as its own -o
# argument, so property data can never become shell syntax or extra argv.
# Usage: zxfer_run_zfs_create_with_properties yes|no TYPE VOLSIZE LIST
# DESTINATION. yes adds -p, which must carry no properties because OpenZFS
# ignores -o on that path; a volume needs its size.
zxfer_run_zfs_create_with_properties() {
	l_create_with_parents=$1
	l_create_dataset_type=$2
	l_create_volume_size=$3
	l_create_property_list=$4
	l_create_destination=$5

	if [ "$l_create_dataset_type" = "volume" ] && [ -z "$l_create_volume_size" ]; then
		return 1
	fi
	set -- create
	if [ "$l_create_with_parents" = "yes" ]; then
		case $l_create_property_list in
		*[!,]*) return 1 ;;
		esac
		set -- "$@" -p
	fi
	if [ "$l_create_dataset_type" = "volume" ]; then
		set -- "$@" -V "$l_create_volume_size"
	fi
	l_create_rest=$l_create_property_list,
	while [ -n "$l_create_rest" ]; do
		l_create_item=${l_create_rest%%,*}
		l_create_rest=${l_create_rest#*,}
		[ -n "$l_create_item" ] || continue
		l_create_value=${l_create_item#*=}
		zxfer_decode_property_value "${l_create_value%%=*}"
		set -- "$@" -o "${l_create_item%%=*}=$g_zxfer_decoded_property_value"
	done
	set -- "$@" "$l_create_destination"

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_run_destination_zfs_cmd "$@"
	else
		zxfer_build_destination_zfs_command "$@"
	fi
}

# Purpose: Create a missing destination dataset with its creation-time
# properties.
# Usage: zxfer_create_destination_dataset IS_INITIAL_SOURCE OVERRIDE_PVS
# CREATION_PVS SOURCE_TYPE SOURCE_VOLSIZE DESTINATION READONLY_CSV, once
# zxfer_property_destination_exists found it missing. The initial source is
# created with its full override list; a child gets its creation list, minus
# overrides the existing parent already supplies for inheritable properties.
# A missing parent is created first with `zfs create -p`. Throws on probe,
# read or create failures.
zxfer_create_destination_dataset() {
	l_missing_is_initial_source=$1
	l_missing_override_pvs=$2
	l_missing_creation_pvs=$3
	l_missing_source_dstype=$4
	l_missing_source_volsize=$5
	l_missing_dataset=$6
	l_missing_readonly_properties=$7

	zxfer_echov "Creating destination filesystem \"$l_missing_dataset\" with specified properties."

	l_missing_parent_exists=""
	l_missing_parent_dataset=${l_missing_dataset%/*}
	if [ "$l_missing_parent_dataset" != "$l_missing_dataset" ]; then
		zxfer_probe_destination_existence "$l_missing_parent_dataset" ||
			zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
		l_missing_parent_exists=$g_zxfer_destination_exists_result
	fi

	if [ "$l_missing_is_initial_source" -eq 1 ]; then
		l_missing_list=$l_missing_override_pvs
	else
		l_missing_list=$l_missing_creation_pvs
		case "$l_missing_parent_exists,$l_missing_list," in
		1,*"=override,"*)
			zxfer_load_normalized_dataset_properties "$l_missing_parent_dataset" destination ||
				zxfer_throw_error "Failed to retrieve parent destination properties for [$l_missing_parent_dataset]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
			l_missing_list=$(zxfer_filter_child_creation_overrides_for_parent \
				"$l_missing_list" "$g_zxfer_normalized_dataset_properties" \
				"$l_missing_readonly_properties") ||
				zxfer_throw_error "Failed to filter child creation override properties." "$?"
			;;
		esac
	fi

	l_missing_with_parents="no"
	if [ "$l_missing_parent_exists" = "0" ]; then
		case $l_missing_list in
		*[!,]*)
			zxfer_run_zfs_create_with_properties "yes" "filesystem" "" "" "$l_missing_parent_dataset" ||
				zxfer_throw_error "Error when creating destination filesystem." "$?"
			if [ "$g_option_n_dryrun" -eq 0 ]; then
				zxfer_note_destination_dataset_exists "$l_missing_parent_dataset"
				zxfer_invalidate_destination_property_mutation_cache "$l_missing_parent_dataset"
			fi
			;;
		*)
			l_missing_with_parents="yes"
			;;
		esac
	fi

	zxfer_run_zfs_create_with_properties "$l_missing_with_parents" "$l_missing_source_dstype" \
		"$l_missing_source_volsize" "$l_missing_list" "$l_missing_dataset" ||
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

# Rewrites a child's set/inherit plan against the destination parent, whose
# list in ZXFER_AWK_PARENT_PVS is filtered with the readonly and -I lists
# first: a property stays inherited only when the parent already provides the
# desired effective value (or the value is a matching inheritable -o
# override); otherwise it must be set locally on the child. Runs behind
# ZXFER_PROPERTY_AWK_LIB and ZXFER_PROPERTY_RULES_AWK.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_CHILD_INHERIT_ADJUST_AWK='
function matches_inheritable_override(property_name, property_value) {
	return ((property_name in override_source) &&
		override_source[property_name] == "override" &&
		!(property_name in noninheritable) &&
		override_value[property_name] == property_value)
}
BEGIN {
	first_values(filter_property_list(ENVIRON["ZXFER_AWK_PARENT_PVS"], 0), parent_value, parent_source)
	first_values(ENVIRON["ZXFER_AWK_OVERRIDE_PVS"], override_value, override_source)

	set_count = split(ENVIRON["ZXFER_AWK_SET_LIST"], set_items, ",")
	for (i = 1; i <= set_count; i++) {
		if (set_items[i] == "")
			continue
		split(set_items[i], set_fields, "=")
		set_property = set_fields[1]
		set_value = set_fields[2]

		if (!(set_property in override_source) ||
			sets_locally(set_property, override_source[set_property])) {
			new_set_list = append_csv(new_set_list, set_items[i])
			continue
		}

		if (matches_inheritable_override(set_property, set_value)) {
			new_inherit_list = append_csv(new_inherit_list, set_items[i])
			continue
		}

		if ((set_property in parent_value) &&
			parent_value[set_property] == set_value) {
			new_inherit_list = append_csv(new_inherit_list, set_items[i])
		} else {
			new_set_list = append_csv(new_set_list, set_items[i])
		}
	}

	inherit_count = split(ENVIRON["ZXFER_AWK_INHERIT_LIST"], inherit_items, ",")
	for (i = 1; i <= inherit_count; i++) {
		if (inherit_items[i] == "")
			continue
		split(inherit_items[i], inherit_fields, "=")
		inherit_property = inherit_fields[1]
		inherit_value = inherit_fields[2]

		if (inherit_property in noninheritable) {
			new_set_list = append_csv(new_set_list, inherit_property "=" inherit_value)
			continue
		}

		if (matches_inheritable_override(inherit_property, inherit_value)) {
			new_inherit_list = append_csv(new_inherit_list, inherit_items[i])
			continue
		}

		if ((inherit_property in parent_value) &&
			parent_value[inherit_property] == inherit_value) {
			new_inherit_list = append_csv(new_inherit_list, inherit_items[i])
		} else {
			new_set_list = append_csv(new_set_list, inherit_property "=" inherit_value)
		}
	}

	print new_set_list
	print new_inherit_list
}'

# Purpose: Keep a child's inherit requests inherited only when the
# destination parent already provides the desired effective value; otherwise
# set the property locally on the child.
# Usage: zxfer_adjust_child_inherit_to_match_parent DESTINATION OVERRIDE_PVS
# CHILD_SET_LIST INHERIT_LIST READONLY_CSV; publishes g_zxfer_adjusted_set_list
# and g_zxfer_adjusted_inherit_list (unchanged for a root dataset, an empty
# plan, or a missing parent), returns the parent property read status on
# failure, and throws on a probe or awk failure.
zxfer_adjust_child_inherit_to_match_parent() {
	l_adjust_destination=$1
	l_adjust_override_pvs=$2
	l_adjust_set_list=$3
	l_adjust_inherit_list=$4
	l_adjust_readonly_properties=$5

	g_zxfer_adjusted_set_list=$l_adjust_set_list
	g_zxfer_adjusted_inherit_list=$l_adjust_inherit_list
	[ -n "$l_adjust_set_list" ] || [ -n "$l_adjust_inherit_list" ] || return 0

	l_adjust_parent_dataset=${l_adjust_destination%/*}
	[ "$l_adjust_parent_dataset" != "$l_adjust_destination" ] || return 0
	zxfer_probe_destination_existence "$l_adjust_parent_dataset" ||
		zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
	[ "$g_zxfer_destination_exists_result" -eq 1 ] || return 0

	zxfer_load_normalized_dataset_properties "$l_adjust_parent_dataset" destination || return "$?"
	if [ "$g_zxfer_normalized_dataset_properties_cache_hit" -eq 0 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_parent_destination_property_reads
	fi

	l_adjust_status=0
	l_adjusted_lists=$(
		ZXFER_AWK_OVERRIDE_PVS=$l_adjust_override_pvs \
			ZXFER_AWK_PARENT_PVS=$g_zxfer_normalized_dataset_properties \
			ZXFER_AWK_SET_LIST=$l_adjust_set_list \
			ZXFER_AWK_INHERIT_LIST=$l_adjust_inherit_list \
			ZXFER_AWK_REMOVE_LIST="$l_adjust_readonly_properties,${g_option_I_ignore_properties:-}" \
			ZXFER_AWK_UNSUPPORTED_LIST='' \
			"${g_cmd_awk:-awk}" -v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
			"$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_CHILD_INHERIT_ADJUST_AWK"
	) || l_adjust_status=$?
	[ "$l_adjust_status" -eq 0 ] ||
		zxfer_throw_error "Failed to reconcile child property inheritance."

	# A trailing empty list is stripped by the command substitution, so the
	# last read may hit EOF; that is not a failure.
	{
		IFS= read -r g_zxfer_adjusted_set_list
		IFS= read -r g_zxfer_adjusted_inherit_list
	} <<EOF || :
$l_adjusted_lists
EOF
	return 0
}

# Purpose: Apply the planned property sets and inherits to one destination.
# Usage: zxfer_apply_property_changes DESTINATION IS_INITIAL_SOURCE
# INITIAL_SET_LIST CHILD_SET_LIST INHERIT_LIST; the initial source applies its
# initial-set list only, a child its child-set list plus one `zfs inherit`
# per inherit item. The sets run as one `zfs set` with each item decoded into
# exactly one property=value argument. Throws when a set or inherit fails.
zxfer_apply_property_changes() {
	l_apply_destination=$1
	if [ "$2" -eq 1 ]; then
		l_apply_set_list=$3
		l_apply_inherit_list=""
	else
		l_apply_set_list=$4
		l_apply_inherit_list=$5
	fi
	[ -n "$l_apply_set_list" ] || [ -n "$l_apply_inherit_list" ] || return 0

	# Decoded values are escaped for display: they may hold any byte.
	if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
		zxfer_echov "Setting properties/sources on destination filesystem \"$l_apply_destination\"."
		if [ -n "$l_apply_set_list" ]; then
			zxfer_decode_serialized_property_list_for_display "$l_apply_set_list"
			zxfer_echov "Property set list: $(zxfer_escape_report_value "$g_zxfer_property_display_list_result")"
		fi
		if [ -n "$l_apply_inherit_list" ]; then
			zxfer_decode_serialized_property_list_for_display "$l_apply_inherit_list"
			zxfer_echov "Property inherit list: $(zxfer_escape_report_value "$g_zxfer_property_display_list_result")"
		fi
	fi

	l_apply_rest=$l_apply_set_list,
	set --
	while [ -n "$l_apply_rest" ]; do
		l_apply_item=${l_apply_rest%%,*}
		l_apply_rest=${l_apply_rest#*,}
		[ -n "$l_apply_item" ] || continue
		l_apply_value=${l_apply_item#*=}
		zxfer_decode_property_value "${l_apply_value%%=*}"
		set -- "$@" "${l_apply_item%%=*}=$g_zxfer_decoded_property_value"
	done
	if [ "$#" -gt 0 ]; then
		zxfer_run_destination_property_verb set \
			"Error when setting properties on destination filesystem." \
			"$l_apply_destination" "$@"
	fi

	l_apply_rest=$l_apply_inherit_list,
	while [ -n "$l_apply_rest" ]; do
		l_apply_item=${l_apply_rest%%,*}
		l_apply_rest=${l_apply_rest#*,}
		[ -n "$l_apply_item" ] || continue
		zxfer_run_destination_property_verb inherit \
			"Error when inheriting properties on destination filesystem." \
			"$l_apply_destination" "${l_apply_item%%=*}"
	done
}

################################################################################
# TOP-LEVEL PROPERTY TRANSFER
################################################################################

# Purpose: Reconcile one dataset's properties: read the source, check the -o
# properties against the initial source, plan the override and creation lists
# from -P/-o/-I/-U, create the destination with its creation-time properties
# when it is missing, otherwise diff against the destination in the same plan
# and apply the resulting sets and inherits, then buffer -k metadata.
# Usage: zxfer_transfer_properties SOURCE [SKIP_BACKUP_CAPTURE]; called from
# the replication loop for every dataset that needs property work and again,
# with the flag set, for the post-seed reconcile pass. Reads g_initial_source,
# g_actual_dest and g_recursive_dest_list.
zxfer_transfer_properties() {
	zxfer_set_failure_stage "property transfer"
	zxfer_echoV "zxfer_transfer_properties: $1"
	zxfer_echoV "initial_source: $g_initial_source"

	l_transfer_source=$1
	l_transfer_skip_backup_capture=${2:-0}
	zxfer_resolve_readonly_properties
	l_transfer_readonly_properties=$g_zxfer_readonly_properties_result
	if [ "$g_initial_source" = "$l_transfer_source" ]; then
		l_transfer_is_initial_source=1
	else
		l_transfer_is_initial_source=0
	fi

	# Source properties, create-time metadata, and required-property backfill
	# for both the raw (-k) and effective (apply) views.
	zxfer_collect_source_props "$l_transfer_source" "$g_actual_dest" ||
		zxfer_throw_error "${g_zxfer_property_error_result:-Failed to retrieve source properties for [$l_transfer_source].}" "$?"
	zxfer_get_validated_source_dataset_create_metadata "$l_transfer_source" "$g_zxfer_source_pvs_raw" ||
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
	zxfer_backfill_required_properties "$l_transfer_source" "$g_zxfer_source_pvs_raw" \
		"$l_transfer_must_create_properties" source ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	g_zxfer_source_pvs_raw=$g_zxfer_required_properties_result
	zxfer_backfill_required_properties "$l_transfer_source" "$g_zxfer_source_pvs_effective" \
		"$l_transfer_must_create_properties" source "$g_zxfer_source_pvs_raw" ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	l_transfer_source_pvs=$g_zxfer_required_properties_result

	# The -o list, already validated at startup; the initial source must hold
	# every -o property before the destination is touched.
	zxfer_read_override_properties "$g_option_o_override_property"
	l_transfer_override_properties=$g_zxfer_override_properties_result
	if [ "$l_transfer_is_initial_source" -eq 1 ]; then
		zxfer_check_override_properties_on_source "$l_transfer_override_properties" \
			"$l_transfer_source_pvs"
	fi

	# A missing destination is created with its creation-time properties and
	# needs no diff.
	if ! zxfer_property_destination_exists "$g_actual_dest"; then
		zxfer_plan_property_changes "$l_transfer_source_pvs" "$l_transfer_override_properties" \
			"$g_option_P_transfer_property" "$l_transfer_source_dstype" \
			"$l_transfer_readonly_properties" "$g_option_I_ignore_properties" \
			"$l_transfer_unsupported_properties"
		zxfer_echoV_escaped "zxfer_transfer_properties override_pvs" "$g_zxfer_plan_override_pvs_result"
		zxfer_echoV_escaped "zxfer_transfer_properties creation_pvs" "$g_zxfer_plan_creation_pvs_result"
		zxfer_create_destination_dataset "$l_transfer_is_initial_source" \
			"$g_zxfer_plan_override_pvs_result" "$g_zxfer_plan_creation_pvs_result" \
			"$l_transfer_source_dstype" "$l_transfer_source_volsize" "$g_actual_dest" \
			"$l_transfer_readonly_properties"
		zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$g_zxfer_source_pvs_raw" "$l_transfer_skip_backup_capture"
		return 0
	fi

	# Destination properties, the plan with its diff, child inheritance
	# adjustment, apply. The zfs or parse diagnostic, when there is one,
	# follows the context.
	zxfer_load_normalized_dataset_properties "$g_actual_dest" destination ||
		zxfer_throw_error "Failed to retrieve destination properties for [$g_actual_dest]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
	zxfer_backfill_required_properties "$g_actual_dest" "$g_zxfer_normalized_dataset_properties" \
		"$l_transfer_must_create_properties" destination ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	zxfer_plan_property_changes "$l_transfer_source_pvs" "$l_transfer_override_properties" \
		"$g_option_P_transfer_property" "$l_transfer_source_dstype" \
		"$l_transfer_readonly_properties" "$g_option_I_ignore_properties" \
		"$l_transfer_unsupported_properties" "$g_zxfer_required_properties_result"
	l_transfer_override_pvs=$g_zxfer_plan_override_pvs_result
	l_transfer_initial_set_list=$g_zxfer_plan_initial_set_result
	l_transfer_child_set_list=$g_zxfer_plan_child_set_result
	l_transfer_inherit_list=$g_zxfer_plan_inherit_result
	zxfer_echoV_escaped "zxfer_transfer_properties override_pvs" "$l_transfer_override_pvs"
	zxfer_echoV_escaped "zxfer_transfer_properties creation_pvs" "$g_zxfer_plan_creation_pvs_result"
	zxfer_echoV_escaped "zxfer_transfer_properties dest_pvs" "$g_zxfer_plan_dest_pvs_result"
	zxfer_echoV_escaped "zxfer_transfer_properties init_set" "$l_transfer_initial_set_list"
	zxfer_echoV_escaped "zxfer_transfer_properties child_set" "$l_transfer_child_set_list"
	zxfer_echoV_escaped "zxfer_transfer_properties inherit" "$l_transfer_inherit_list"

	if [ "$l_transfer_is_initial_source" -eq 0 ] &&
		{ [ -n "$l_transfer_child_set_list" ] || [ -n "$l_transfer_inherit_list" ]; }; then
		zxfer_adjust_child_inherit_to_match_parent "$g_actual_dest" "$l_transfer_override_pvs" \
			"$l_transfer_child_set_list" "$l_transfer_inherit_list" "$l_transfer_readonly_properties" ||
			zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$?"
		l_transfer_child_set_list=$g_zxfer_adjusted_set_list
		l_transfer_inherit_list=$g_zxfer_adjusted_inherit_list
		zxfer_echoV_escaped "zxfer_transfer_properties adjusted child_set" "$l_transfer_child_set_list"
		zxfer_echoV_escaped "zxfer_transfer_properties adjusted inherit" "$l_transfer_inherit_list"
	fi

	zxfer_apply_property_changes "$g_actual_dest" "$l_transfer_is_initial_source" \
		"$l_transfer_initial_set_list" "$l_transfer_child_set_list" "$l_transfer_inherit_list"
	zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$g_zxfer_source_pvs_raw" "$l_transfer_skip_backup_capture"
}
