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
# PROPERTY FILTER / OVERRIDE / COMPATIBILITY POLICY
################################################################################

# Module contract:
# owns globals: the readonly, noninheritable and required-creation property
#   constants, the run-wide unsupported-property lists
#   (g_zxfer_unsupported_*_properties), the filter and override-derivation AWK
#   programs, and the result globals g_zxfer_readonly_properties_result,
#   g_zxfer_override_pvs_result, g_zxfer_creation_pvs_result,
#   g_zxfer_sanitized_property_list_result, g_zxfer_source_dataset_type_result
#   and g_zxfer_source_volume_size_result.
# reads globals: destination platform and migration state, CLI property
#   options, the recursive source and destination lists, and the
#   source/destination dataset context.
# mutates caches: the unsupported-property lists, and the destination
#   existence cache through probes.
# returns via stdout: zxfer_filter_child_creation_overrides_for_parent only.

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

# Shared filter prefix: drops readonly and ignored (-I) properties unless the
# item is an explicit override, and always drops destination-unsupported (-U)
# properties, collecting one verbose warning per dropped unsupported item.
# The lists arrive in ZXFER_AWK_REMOVE_LIST and ZXFER_AWK_UNSUPPORTED_LIST, and
# verbose with -v. Every caller sets both variables, empty when unused, so an
# exported value never leaks in. Runs behind ZXFER_PROPERTY_AWK_LIB.
# shellcheck disable=SC2016  # AWK program is intentionally single-quoted.
ZXFER_PROPERTY_FILTER_AWK='
function filter_property_list(list, count, items, i, fields, field_count, output) {
	count = split(list, items, ",")
	output = ""
	for (i = 1; i <= count; i++) {
		if (items[i] == "")
			continue
		field_count = split(items[i], fields, "=")
		if ((fields[1] in remove_property) && (field_count < 3 || fields[3] != "override"))
			continue
		if (fields[1] in unsupported_property) {
			if (verbose == 1)
				warnings[++warning_count] = "Destination does not support property " fields[1] "=" decode_value(fields[2])
			continue
		}
		output = append_csv(output, items[i])
	}
	return output
}
BEGIN {
	csv_to_set(ENVIRON["ZXFER_AWK_REMOVE_LIST"], remove_property)
	csv_to_set(ENVIRON["ZXFER_AWK_UNSUPPORTED_LIST"], unsupported_property)
}
'

# Derives the override (apply) and creation lists from the source properties
# in ZXFER_AWK_SOURCE_PVS and the -o text in ZXFER_AWK_OVERRIDE_OPTIONS, then
# prints both through filter_property_list, followed by any warnings. A -o
# item without "NAME=" prints the syntax marker and exits 1; with validate=1,
# a -o property the source lacks prints its name and exits 4. Runs behind
# ZXFER_PROPERTY_AWK_LIB and ZXFER_PROPERTY_FILTER_AWK.
# shellcheck disable=SC2016  # AWK program is intentionally single-quoted.
ZXFER_DERIVE_OVERRIDE_LISTS_AWK='
function append_creation(property, value, source) {
	if (!(property in creation_seen)) {
		creation_output = append_csv(creation_output, property "=" value "=" source)
		creation_seen[property] = 1
	}
}
BEGIN {
	source_count = split(ENVIRON["ZXFER_AWK_SOURCE_PVS"], source_items, ",")
	for (i = 1; i <= source_count; i++) {
		if (source_items[i] == "")
			continue
		split(source_items[i], source_fields, "=")
		source_has[source_fields[1]] = 1
	}

	override_count = split_override_csv(ENVIRON["ZXFER_AWK_OVERRIDE_OPTIONS"], override_items)
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		separator = index(override_items[i], "=")
		if (separator <= 1) {
			print "__ZXFER_OVERRIDE_SYNTAX__"
			exit 1
		}
		property = substr(override_items[i], 1, separator - 1)
		value = encode_value(substr(override_items[i], separator + 1))
		if (validate == 1 && !(property in source_has)) {
			print property
			exit 4
		}
		if (transfer_all_flag == 0)
			override_output = append_csv(override_output, property "=" value "=override")
		if (!(property in override_value)) {
			override_value[property] = value
			if (transfer_all_flag == 0)
				append_creation(property, value, "override")
		}
	}

	if (source_dstype != "volume")
		csv_to_set(required_creation_properties, required_create)

	for (i = 1; i <= source_count; i++) {
		if (source_items[i] == "")
			continue
		split(source_items[i], source_fields, "=")
		source_property = source_fields[1]
		source_value = source_fields[2]
		source_source = source_fields[3]

		# Some OpenZFS variants expose volume-only properties in `zfs get all`
		# for filesystem trees. Replaying those into filesystem create/set paths
		# is invalid, so drop them before deriving override and creation lists.
		if (source_dstype != "volume" &&
			(source_property == "volblocksize" || source_property == "volthreading"))
			continue

		source_is_creation = (source_source == "local" ||
			(source_dstype == "volume" && source_property == "refreservation") ||
			(source_property in required_create))

		if (source_property in override_value) {
			if (transfer_all_flag != 0)
				override_output = append_csv(override_output, source_property "=" override_value[source_property] "=override")
			if (source_is_creation)
				append_creation(source_property, override_value[source_property], "override")
			continue
		}

		if (transfer_all_flag != 0 || (source_property in required_create))
			override_output = append_csv(override_output, source_property "=" source_value "=" source_source)
		if (source_is_creation && (transfer_all_flag != 0 || (source_property in required_create)))
			append_creation(source_property, source_value, source_source)
	}

	print filter_property_list(override_output)
	print filter_property_list(creation_output)
	for (i = 1; i <= warning_count; i++)
		print warnings[i]
}'

# Purpose: Build the override (apply) and creation property lists from the
# source properties and the -P/-o options, filtered by the readonly, -I and
# -U lists (explicit overrides survive the readonly and -I filters;
# unsupported items are always dropped and reported on stderr when verbose).
# Usage: zxfer_derive_override_lists SOURCE_PVS OVERRIDE_OPTIONS
# TRANSFER_ALL_FLAG DATASET_TYPE [READONLY_CSV] [IGNORE_CSV] [UNSUPPORTED_CSV]
# [VALIDATE]; VALIDATE=1 also requires every -o property to exist on the
# source. Publishes g_zxfer_override_pvs_result and
# g_zxfer_creation_pvs_result; throws a usage error on bad -o input and an
# error when awk fails.
zxfer_derive_override_lists() {
	g_zxfer_override_pvs_result=""
	g_zxfer_creation_pvs_result=""
	l_derive_status=0
	# Every list travels through the environment: awk -v would reinterpret
	# backslash escapes in property values.
	l_derived_lists=$(
		ZXFER_AWK_SOURCE_PVS=$1 \
			ZXFER_AWK_OVERRIDE_OPTIONS=$2 \
			ZXFER_AWK_REMOVE_LIST="${5:-},${6:-}" \
			ZXFER_AWK_UNSUPPORTED_LIST=${7:-} \
			"${g_cmd_awk:-awk}" \
			-v transfer_all_flag="$3" \
			-v source_dstype="$4" \
			-v validate="${8:-0}" \
			-v required_creation_properties="$ZXFER_REQUIRED_CREATION_PROPERTIES" \
			-v verbose="${g_option_v_verbose:-0}" \
			"$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_FILTER_AWK$ZXFER_DERIVE_OVERRIDE_LISTS_AWK"
	) || l_derive_status=$?

	case $l_derive_status in
	0) ;;
	4)
		l_derive_missing_property=$(zxfer_escape_report_value "$l_derived_lists") ||
			l_derive_missing_property="[unprintable]"
		zxfer_throw_usage_error "Missing source property for -o override: $l_derive_missing_property."
		;;
	*)
		if [ "$l_derive_status" -eq 1 ] && [ "$l_derived_lists" = "__ZXFER_OVERRIDE_SYNTAX__" ]; then
			zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
		fi
		zxfer_throw_error "Failed to derive override property lists."
		;;
	esac

	# The two lists come first, then one -U warning per line. A trailing empty
	# list is stripped by the command substitution, so a read may hit EOF;
	# that is not a failure.
	{
		IFS= read -r g_zxfer_override_pvs_result
		IFS= read -r g_zxfer_creation_pvs_result
		while IFS= read -r l_derive_warning; do
			[ -z "$l_derive_warning" ] || zxfer_warn_stderr "$l_derive_warning"
		done
	} <<EOF || :
$l_derived_lists
EOF
}

# Purpose: Drop readonly and ignored (-I) properties from one property list
# with the shared filter rules; explicit overrides stay.
# Usage: zxfer_sanitize_property_list LIST READONLY_CSV IGNORE_CSV; publishes
# g_zxfer_sanitized_property_list_result and throws when awk fails.
zxfer_sanitize_property_list() {
	g_zxfer_sanitized_property_list_result=$1
	[ -n "$1" ] || return 0
	case "$2,$3" in
	*[!,]*) ;;
	*) return 0 ;;
	esac

	l_sanitize_status=0
	# shellcheck disable=SC2016
	g_zxfer_sanitized_property_list_result=$(
		ZXFER_AWK_PROPERTY_LIST=$1 ZXFER_AWK_REMOVE_LIST="$2,$3" \
			ZXFER_AWK_UNSUPPORTED_LIST='' \
			"${g_cmd_awk:-awk}" "$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_FILTER_AWK"'
BEGIN { print filter_property_list(ENVIRON["ZXFER_AWK_PROPERTY_LIST"]) }'
	) || l_sanitize_status=$?
	[ "$l_sanitize_status" -eq 0 ] ||
		zxfer_throw_error "Failed to filter unsupported destination properties."
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
# CREATE-TIME PROPERTY POLICY
################################################################################

# Purpose: Drop child create overrides the parent already supplies, so
# recursive -o overrides of inheritable properties stay inherited on
# descendants once the parent has the requested value.
# Usage: zxfer_filter_child_creation_overrides_for_parent CREATION_PVS
# PARENT_PVS; prints the filtered creation list.
zxfer_filter_child_creation_overrides_for_parent() {
	# shellcheck disable=SC2016
	ZXFER_AWK_CREATION_PVS=$1 ZXFER_AWK_PARENT_PVS=$2 "${g_cmd_awk:-awk}" \
		-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
		"$ZXFER_PROPERTY_AWK_LIB"'
BEGIN {
	csv_to_set(noninheritable_properties, noninheritable)
	first_values(ENVIRON["ZXFER_AWK_PARENT_PVS"], parent_value, parent_source)
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
