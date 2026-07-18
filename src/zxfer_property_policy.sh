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
# owns globals: readonly/noninheritable property constants, unsupported-property
#   caches, and the immutable override-derivation AWK program; publishes other
#   property decisions through state-owned result channels.
# reads globals: destination platform/migration state, source/destination
#   dataset context, CLI property options, and property-state result channels.
# mutates caches: resets and appends the policy-owned unsupported-property
#   caches; writes other filtered and derived results through property state.
# returns via stdout: filtered property lists, validated dataset-create
#   metadata, override/create plans, and unsupported-property selections.

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

# Purpose: Reset policy-owned unsupported-property caches.
# Usage: Called at session initialization and before a fresh compatibility scan.
# Side effects: Clears the filesystem and volume unsupported-property results.
zxfer_reset_unsupported_property_state() {
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
}

# Purpose: Reset run-wide property policy state through its owner operation.
# Usage: Called by the existing session initialization path after property
# state and policy modules are loaded.
zxfer_reset_property_runtime_state() {
	zxfer_reset_unsupported_property_state
}

# Purpose: Return properties whose effective values cannot be preserved through
# child inheritance.
# Usage: Called by property reconciliation before converting child set/create
# work into inherit operations.
zxfer_get_noninheritable_properties() {
	printf '%s\n' "${g_test_noninheritable_properties:-$ZXFER_NONINHERITABLE_PROPERTIES}"
}

# Purpose: Return the effective readonly properties in the form expected by
# later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_effective_readonly_properties() {
	l_effective_readonly_properties=$ZXFER_BASE_READONLY_PROPERTIES

	if [ "${g_destination_operating_system:-}" = "FreeBSD" ]; then
		l_platform_readonly_properties=$ZXFER_FREEBSD_READONLY_PROPERTIES
		if [ -n "$l_platform_readonly_properties" ]; then
			if [ -n "$l_effective_readonly_properties" ]; then
				l_effective_readonly_properties="$l_effective_readonly_properties,$l_platform_readonly_properties"
			else
				l_effective_readonly_properties=$l_platform_readonly_properties
			fi
		fi
	fi
	if [ "${g_option_m_migrate:-0}" -eq 1 ] && [ -n "$l_effective_readonly_properties" ]; then
		l_effective_readonly_properties=$(printf '%s' ",$l_effective_readonly_properties," |
			sed -e 's/,mountpoint,/,/g' -e 's/^,//' -e 's/,$//')
	fi

	printf '%s\n' "$l_effective_readonly_properties"
}

# Purpose: Remove the sources from the current working set while preserving the
# module's special-case rules.
# Usage: Called during property filtering, diffing, and apply when filtering
# logic must trim staged data before later reconciliation or apply steps run.
#
# Drop the source field from property=value=source entries.
# Result is stored in $g_zxfer_new_rmvs_pv as property=value CSV.
zxfer_remove_sources() {
	l_new_rmvs_pv=""

	l_rmvs_list=$1
	l_rmvs_remaining=$l_rmvs_list
	while [ -n "$l_rmvs_remaining" ]; do
		case "$l_rmvs_remaining" in
		*,*)
			l_rmvs_line=${l_rmvs_remaining%%,*}
			l_rmvs_remaining=${l_rmvs_remaining#*,}
			;;
		*)
			l_rmvs_line=$l_rmvs_remaining
			l_rmvs_remaining=""
			;;
		esac
		[ -n "$l_rmvs_line" ] || continue
		l_rmvs_property=${l_rmvs_line%%=*}
		l_rmvs_remainder=${l_rmvs_line#*=}
		l_rmvs_value=${l_rmvs_remainder%%=*}
		l_new_rmvs_pv="$l_new_rmvs_pv$l_rmvs_property=$l_rmvs_value,"
	done

	l_new_rmvs_pv=${l_new_rmvs_pv%,}
	zxfer_publish_remove_sources_result "$l_new_rmvs_pv"
}

# Purpose: Remove the properties from the current working set while preserving
# the module's special-case rules.
# Usage: Called during property filtering, diffing, and apply when filtering
# logic must trim staged data before later reconciliation or apply steps run.
#
# Remove listed properties from property=value=source entries.
# Explicit override entries are preserved. Result is stored in
# $g_zxfer_new_rmv_pvs.
zxfer_remove_properties() {
	l_new_rmv_pvs=""

	l_rmv_list=$1    # the list of properties=values=sources,...
	l_remove_list=$2 # list of properties to remove

	l_rmv_remaining=$l_rmv_list
	while [ -n "$l_rmv_remaining" ]; do
		case "$l_rmv_remaining" in
		*,*)
			l_rmv_line=${l_rmv_remaining%%,*}
			l_rmv_remaining=${l_rmv_remaining#*,}
			;;
		*)
			l_rmv_line=$l_rmv_remaining
			l_rmv_remaining=""
			;;
		esac
		[ -n "$l_rmv_line" ] || continue
		l_found_readonly=0
		l_rmv_property=${l_rmv_line%%=*}
		l_rmv_remainder=${l_rmv_line#*=}
		l_rmv_value=${l_rmv_remainder%%=*}
		l_rmv_source=${l_rmv_remainder#*=}
		l_remove_remaining=$l_remove_list
		while [ -n "$l_remove_remaining" ]; do
			case "$l_remove_remaining" in
			*,*)
				l_property=${l_remove_remaining%%,*}
				l_remove_remaining=${l_remove_remaining#*,}
				;;
			*)
				l_property=$l_remove_remaining
				l_remove_remaining=""
				;;
			esac
			[ -n "$l_property" ] || continue
			if [ "$l_property" = "$l_rmv_property" ]; then
				if [ "$l_rmv_source" = "override" ]; then
					# The user has specifically required we set this property
					continue
				fi
				l_found_readonly=1
				# Since the property was matched, remove it from the remaining
				# filter list so later iterations do not rescan it unnecessarily.
				l_filtered_remove_list=""
				l_filter_remove_remaining=$l_remove_list
				while [ -n "$l_filter_remove_remaining" ]; do
					case "$l_filter_remove_remaining" in
					*,*)
						l_filter_property=${l_filter_remove_remaining%%,*}
						l_filter_remove_remaining=${l_filter_remove_remaining#*,}
						;;
					*)
						l_filter_property=$l_filter_remove_remaining
						l_filter_remove_remaining=""
						;;
					esac
					[ -n "$l_filter_property" ] || continue
					[ "$l_filter_property" = "$l_property" ] && continue
					if [ -n "$l_filtered_remove_list" ]; then
						l_filtered_remove_list="$l_filtered_remove_list,$l_filter_property"
					else
						l_filtered_remove_list=$l_filter_property
					fi
				done
				l_remove_list=$l_filtered_remove_list
				break
			fi
		done
		if [ $l_found_readonly -eq 0 ]; then
			l_new_rmv_pvs="$l_new_rmv_pvs$l_rmv_property=$l_rmv_value=$l_rmv_source,"
		fi
	done

	l_new_rmv_pvs=${l_new_rmv_pvs%,}
	zxfer_publish_remove_properties_result "$l_new_rmv_pvs"
}

# Purpose: Remove the unsupported properties from the current working set while
# preserving the module's special-case rules.
# Usage: Called during property filtering, diffing, and apply when filtering
# logic must trim staged data before later reconciliation or apply steps run.
#
# Remove properties the destination cannot support from
# property=value=source entries. Result is stored in $g_zxfer_new_rmv_pvs.
zxfer_remove_unsupported_properties() {
	l_orig_set_list=$1 # the list of properties=values=sources,...
	l_unsupported_list=$2
	zxfer_publish_supported_properties_result ""

	zxfer_create_property_reconcile_stage_file ||
		return "$?"
	l_filter_tmp=$g_zxfer_property_reconcile_stage_file_result

	if ! "${g_cmd_awk:-awk}" -v input_list="$l_orig_set_list" \
		-v unsupported_list="$l_unsupported_list" \
		-v verbose="${g_option_v_verbose:-0}" '
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
	unsupported_count = split(unsupported_list, unsupported_items, ",")
	for (i = 1; i <= unsupported_count; i++) {
		if (unsupported_items[i] == "")
			continue
		unsupported[unsupported_items[i]] = 1
	}

	input_count = split(input_list, input_items, ",")
	for (i = 1; i <= input_count; i++) {
		if (input_items[i] == "")
			continue
		split(input_items[i], input_fields, "=")
		input_property = input_fields[1]
		input_value = input_fields[2]
		if (input_property in unsupported) {
			if (verbose == 1)
				warnings[++warning_count] = "Destination does not support property " input_property "=" decode_value(input_value)
			continue
		}
		supported_output = append_csv(supported_output, input_items[i])
	}

	print supported_output
	for (i = 1; i <= warning_count; i++)
		print warnings[i]
	}' >"$l_filter_tmp"; then
		zxfer_cleanup_runtime_artifact_path "$l_filter_tmp"
		zxfer_throw_error "Failed to filter unsupported destination properties."
	fi

	zxfer_read_property_reconcile_stage_file "$l_filter_tmp" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_filter_tmp"
		return "$l_read_status"
	}
	l_only_supported_properties=""
	{
		IFS= read -r l_only_supported_properties
		zxfer_publish_supported_properties_result "$l_only_supported_properties"
		while IFS= read -r l_warning || [ -n "$l_warning" ]; do
			[ -n "$l_warning" ] || continue
			zxfer_warn_stderr "$l_warning"
		done
	} <<EOF
$g_zxfer_property_stage_file_read_result
EOF
	zxfer_cleanup_runtime_artifact_path "$l_filter_tmp"
}

# Purpose: Clear readonly properties from the live apply set while preserving
# the module's property-application rules.
# Usage: Called during property filtering, diffing, and apply before `zfs set`
# or `zfs inherit` would otherwise try to modify properties the destination
# cannot accept.
#
# Replace any readonly=on entries with readonly=off so zxfer can ensure the
# destination stays writable when --ensure-writable is enabled.
# $1: comma-separated property list
zxfer_force_readonly_off() {
	if [ -z "$1" ]; then
		printf '%s\n' ""
		return
	fi

	l_forced_property_list=""
	l_force_readonly_remaining=$1
	while [ -n "$l_force_readonly_remaining" ]; do
		case "$l_force_readonly_remaining" in
		*,*)
			l_property_entry=${l_force_readonly_remaining%%,*}
			l_force_readonly_remaining=${l_force_readonly_remaining#*,}
			;;
		*)
			l_property_entry=$l_force_readonly_remaining
			l_force_readonly_remaining=""
			;;
		esac
		if [ -z "$l_property_entry" ]; then
			continue
		fi
		l_forced_property_entry=$l_property_entry
		l_property_name=${l_property_entry%%=*}
		l_property_remainder=${l_property_entry#*=}
		if [ "$l_property_name" = "readonly" ]; then
			case $l_property_remainder in
			on)
				l_forced_property_entry="readonly=off"
				;;
			on=*)
				l_property_source=${l_property_remainder#*=}
				l_forced_property_entry="readonly=off=$l_property_source"
				;;
			esac
		fi
		if [ -n "$l_forced_property_list" ]; then
			l_forced_property_list="$l_forced_property_list,$l_forced_property_entry"
		else
			l_forced_property_list=$l_forced_property_entry
		fi
	done

	printf '%s\n' "$l_forced_property_list"
}

# Purpose: Validate the override properties before zxfer relies on it.
# Usage: Called during property filtering, diffing, and apply to fail closed on
# malformed, unsafe, or stale input.
#
# Validate that every override property supplied via -o exists on the source,
# while keeping syntax and missing-property diagnostics distinct.
# Literal commas inside one override value must be escaped as \,.
# $1: comma-separated override list (property=value)
# $2: comma-separated source property/value/source list
zxfer_validate_override_properties() {
	l_override_list=$1
	l_source_pvs=$2
	l_validation_result=""

	if [ -z "$l_override_list" ]; then
		return
	fi

	l_status=0
	l_validation_result=$(
		ZXFER_AWK_OVERRIDE_LIST=$l_override_list "${g_cmd_awk:-awk}" -v source_pvs="$l_source_pvs" '
function split_override_csv(input, output, field_count, i, character, next_character, field_value) {
	delete output
	field_count = 0
	field_value = ""

	for (i = 1; i <= length(input); i++) {
		character = substr(input, i, 1)
		if (character == "\\") {
			if (i < length(input)) {
				next_character = substr(input, i + 1, 1)
				if (next_character == ",") {
					field_value = field_value next_character
					i++
					continue
				}
			}
			field_value = field_value character
			continue
		}
		if (character == ",") {
			output[++field_count] = field_value
			field_value = ""
			continue
		}
		field_value = field_value character
	}

	if (field_value != "" || input != "")
		output[++field_count] = field_value

	return field_count
}
BEGIN {
	override_list = ENVIRON["ZXFER_AWK_OVERRIDE_LIST"]
	source_count = split(source_pvs, source_items, ",")
	for (i = 1; i <= source_count; i++) {
		if (source_items[i] == "")
			continue
		split(source_items[i], source_fields, "=")
		source_property[source_fields[1]] = 1
	}

	override_count = split_override_csv(override_list, override_items)
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		override_separator = index(override_items[i], "=")
		if (override_separator <= 1) {
			print "__ZXFER_OVERRIDE_SYNTAX__"
			exit 1
		}
		override_property = substr(override_items[i], 1, override_separator - 1)
		if (!(override_property in source_property)) {
			print override_property
			exit 1
		}
		}
	}'
	) || l_status=$?

	if [ "$l_status" -eq 1 ]; then
		case "$l_validation_result" in
		"__ZXFER_OVERRIDE_SYNTAX__" | "")
			zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
			;;
		*)
			l_validation_display_property=$(zxfer_escape_report_value "$l_validation_result") ||
				l_validation_display_property="[unprintable]"
			zxfer_throw_usage_error "Missing source property for -o override: $l_validation_display_property."
			;;
		esac
	elif [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to validate override properties."
	fi
}

# shellcheck disable=SC2016  # AWK program is intentionally single-quoted.
ZXFER_DERIVE_OVERRIDE_LISTS_AWK='
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function append_creation(property, value, source) {
	if (!(property in creation_property_seen)) {
		creation_output = append_csv(creation_output, property "=" value "=" source)
		creation_property_seen[property] = 1
	}
}
function split_override_csv(input, output, field_count, i, character, next_character, field_value) {
	delete output
	field_count = 0
	field_value = ""

	for (i = 1; i <= length(input); i++) {
		character = substr(input, i, 1)
		if (character == "\\") {
			if (i < length(input)) {
				next_character = substr(input, i + 1, 1)
				if (next_character == ",") {
					field_value = field_value next_character
					i++
					continue
				}
			}
			field_value = field_value character
			continue
		}
		if (character == ",") {
			output[++field_count] = field_value
			field_value = ""
			continue
		}
		field_value = field_value character
	}

	if (field_value != "" || input != "")
		output[++field_count] = field_value

	return field_count
}
BEGIN {
	override_options = ENVIRON["ZXFER_AWK_OVERRIDE_OPTIONS"]
	override_count = split_override_csv(override_options, override_items)
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		override_separator = index(override_items[i], "=")
		if (override_separator <= 1) {
			print "__ZXFER_OVERRIDE_SYNTAX__"
			exit 1
		}
		override_fields[1] = substr(override_items[i], 1, override_separator - 1)
		override_fields[2] = substr(override_items[i], override_separator + 1)
		gsub(/%/, "%25", override_fields[2])
		gsub(/,/, "%2C", override_fields[2])
		gsub(/=/, "%3D", override_fields[2])
		gsub(/;/, "%3B", override_fields[2])
		gsub(/\t/, "%09", override_fields[2])
		gsub(/\r/, "%0D", override_fields[2])
		gsub(/\n/, "%0A", override_fields[2])
		if (transfer_all_flag == 0)
			override_output = append_csv(override_output, override_fields[1] "=" override_fields[2] "=override")
		if (!(override_fields[1] in override_value)) {
			override_value[override_fields[1]] = override_fields[2]
			if (transfer_all_flag == 0)
				append_creation(override_fields[1], override_fields[2], "override")
		}
	}

	required_count = split(required_creation_properties, required_items, ",")
	for (i = 1; i <= required_count; i++) {
		if (required_items[i] == "")
			continue
		required_create[required_items[i]] = 1
	}

	source_count = split(source_pvs, source_items, ",")
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

	print override_output
	print creation_output
}'

# Purpose: Derive the override lists from the active property or replication
# state.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a computed plan input.
#
# Build the override and creation property sets based on -P/-o arguments.
# Returns two newline-separated lines: override_pvs and creation_pvs.
# $1: source property/value/source list
# $2: -o override list (property=value), with literal commas escaped as \,
# $3: $g_option_P_transfer_property flag
# $4: dataset type (filesystem/volume)
zxfer_derive_override_lists() {
	l_source_pvs=$1
	l_override_options=$2
	l_transfer_all_flag=$3
	l_source_dstype=$4
	l_required_creation_properties=$(zxfer_get_required_creation_properties_for_dataset_type "$l_source_dstype")
	zxfer_publish_override_property_results "" ""

	# awk program needs literal $-style fields; shell variables are passed with -v.
	l_status=0
	# shellcheck disable=SC2016
	l_derived_lists=$(
		ZXFER_AWK_OVERRIDE_OPTIONS=$l_override_options "${g_cmd_awk:-awk}" \
			-v source_pvs="$l_source_pvs" \
			-v transfer_all_flag="$l_transfer_all_flag" \
			-v source_dstype="$l_source_dstype" \
			-v required_creation_properties="$l_required_creation_properties" \
			"$ZXFER_DERIVE_OVERRIDE_LISTS_AWK"
	) || l_status=$?

	if [ "$l_status" -eq 1 ] && [ "$l_derived_lists" = "__ZXFER_OVERRIDE_SYNTAX__" ]; then
		zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
	elif [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to derive override property lists."
	fi

	l_override_pvs_result=""
	l_creation_pvs_result=""
	{
		IFS= read -r l_override_pvs_result
		IFS= read -r l_creation_pvs_result
	} <<EOF
$l_derived_lists
EOF
	zxfer_publish_override_property_results "$l_override_pvs_result" "$l_creation_pvs_result"

	printf '%s\n' "$l_derived_lists"
}

# Purpose: Sanitize the property list before zxfer trusts it.
# Usage: Called during property filtering, diffing, and apply to remove
# unsupported or unsafe input before execution.
#
# Remove readonly/ignored properties from a list while preserving formatting.
# $1: comma-separated property list
# $2: readonly property list to remove
# $3: additional ignore list to remove
zxfer_sanitize_property_list() {
	l_input_list=$1
	l_remove_list=$2
	l_ignore_list=$3

	if [ -z "$l_input_list" ]; then
		printf '%s\n' ""
		return
	fi

	l_filtered_list=$l_input_list

	if [ -n "$l_remove_list" ]; then
		zxfer_remove_properties "$l_filtered_list" "$l_remove_list"
		l_filtered_list="$g_zxfer_new_rmv_pvs"
	fi

	if [ -n "$l_ignore_list" ]; then
		zxfer_remove_properties "$l_filtered_list" "$l_ignore_list"
		l_filtered_list="$g_zxfer_new_rmv_pvs"
	fi

	printf '%s\n' "$l_filtered_list"
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

# Purpose: Return the validated source dataset create metadata in the form
# expected by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
#
# Retrieve and validate the source dataset type plus any required creation
# metadata before planning destination creation or property diffs.
# Returns two newline-separated lines: dataset_type, volume_size.
# $1: source dataset
zxfer_get_validated_source_dataset_create_metadata() {
	l_source=$1
	l_source_volsize=""

	l_source_dstype_status=0
	l_source_dstype=$(zxfer_run_source_zfs_cmd get -Hpo value type "$l_source" 2>&1) ||
		l_source_dstype_status=$?
	if [ "$l_source_dstype_status" -ne 0 ]; then
		printf '%s\n' "Failed to retrieve source dataset type for [$l_source]: $l_source_dstype"
		return "$l_source_dstype_status"
	fi

	case "$l_source_dstype" in
	filesystem) ;;
	volume)
		l_source_volsize_status=0
		l_source_volsize=$(zxfer_run_source_zfs_cmd get -Hpo value volsize "$l_source" 2>&1) ||
			l_source_volsize_status=$?
		if [ "$l_source_volsize_status" -ne 0 ]; then
			printf '%s\n' "Failed to retrieve source zvol size for [$l_source]: $l_source_volsize"
			return "$l_source_volsize_status"
		fi
		if [ -z "$l_source_volsize" ] || [ "$l_source_volsize" = "-" ]; then
			printf '%s\n' "Failed to retrieve source zvol size for [$l_source]: empty volsize"
			return 1
		fi
		;;
	*)
		printf '%s\n' "Invalid source dataset type for [$l_source]: $l_source_dstype"
		return 1
		;;
	esac

	printf '%s\n' "$l_source_dstype"
	printf '%s\n' "$l_source_volsize"
}

# Purpose: Return the required creation properties for dataset type in the form
# expected by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
#
# Return the applicable creation-time properties for the source dataset type.
# Filesystems need these properties to be compared at creation time; volumes do
# not support them and should not probe them opportunistically.
# $1: dataset type (filesystem/volume)
zxfer_get_required_creation_properties_for_dataset_type() {
	l_dataset_type=$1

	case "$l_dataset_type" in
	volume)
		printf '\n'
		;;
	*)
		printf '%s\n' "casesensitivity,normalization,utf8only"
		;;
	esac
}

# Purpose: Strip the unsupported properties while preserving the semantics
# later helpers expect.
# Usage: Called during property filtering, diffing, and apply before comparison
# or execution consumes the cleaned value.
#
# Drop properties unsupported on the destination.
# $1: comma-separated property list
# $2: unsupported property names
zxfer_strip_unsupported_properties() {
	l_input_list=$1
	l_unsupported_list=$2

	if [ -z "$l_unsupported_list" ] || [ -z "$l_input_list" ]; then
		printf '%s\n' "$l_input_list"
		return
	fi

	zxfer_remove_unsupported_properties "$l_input_list" "$l_unsupported_list"
	printf '%s\n' "$g_zxfer_only_supported_properties"
}

# Purpose: Return the unsupported property probe dataset in the form expected
# by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_unsupported_property_probe_dataset() {
	l_requested_destination=$1
	l_probe_dataset=${l_requested_destination:-${g_destination:-}}

	if [ -z "$l_probe_dataset" ]; then
		printf '%s\n' "Failed to determine the destination property-support probe dataset."
		return 1
	fi

	l_dest_exists_status=0
	l_dest_exists=$(zxfer_exists_destination "$l_probe_dataset") || l_dest_exists_status=$?
	if [ "$l_dest_exists_status" -ne 0 ]; then
		printf '%s\n' "Failed to determine whether destination dataset [$l_probe_dataset] exists: $l_dest_exists"
		return "$l_dest_exists_status"
	fi
	if [ "$l_dest_exists" -eq 1 ]; then
		printf '%s\n' "$l_probe_dataset"
		return 0
	fi

	printf '%s\n' "${l_probe_dataset%%/*}"
}

# Purpose: Return the unsupported property probe dataset type in the form
# expected by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_unsupported_property_probe_dataset_type() {
	l_probe_dataset=$1

	l_probe_dataset_type_status=0
	l_probe_dataset_type=$(zxfer_run_destination_zfs_cmd get -Hpo value type "$l_probe_dataset" 2>&1) ||
		l_probe_dataset_type_status=$?
	if [ "$l_probe_dataset_type_status" -ne 0 ]; then
		printf '%s\n' "Failed to determine the destination property-support probe dataset type for [$l_probe_dataset]: $l_probe_dataset_type"
		return "$l_probe_dataset_type_status"
	fi

	printf '%s\n' "$l_probe_dataset_type"
}

# Purpose: Return the unsupported property probe destination for source in the
# form expected by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_unsupported_property_probe_destination_for_source() {
	l_source_dataset=$1

	if [ -z "${g_initial_source:-}" ]; then
		printf '%s\n' "Failed to determine the initial source dataset for unsupported-property probe mapping."
		return 1
	fi

	case "$l_source_dataset" in
	"$g_initial_source" | "$g_initial_source"/*) ;;
	*)
		printf '%s\n' "Unsupported-property probe source dataset [$l_source_dataset] is outside the initial source tree [$g_initial_source]."
		return 1
		;;
	esac

	zxfer_get_destination_dataset_for_source_dataset "$l_source_dataset"
}

# Purpose: Return the unsupported property probe dataset for source in the form
# expected by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_unsupported_property_probe_dataset_for_source() {
	l_source_dataset=$1

	l_probe_destination_status=0
	l_requested_destination=$(zxfer_get_unsupported_property_probe_destination_for_source "$l_source_dataset") ||
		l_probe_destination_status=$?
	if [ "$l_probe_destination_status" -ne 0 ]; then
		printf '%s\n' "$l_requested_destination"
		return "$l_probe_destination_status"
	fi

	zxfer_get_unsupported_property_probe_dataset "$l_requested_destination"
}

# Purpose: Append the unsupported property for dataset type to the module-owned
# accumulator.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need one shared place to extend staged or in-memory state.
zxfer_append_unsupported_property_for_dataset_type() {
	l_source_dataset_type=$1
	l_property_name=$2

	case "$l_source_dataset_type" in
	volume)
		l_existing_unsupported_properties=${g_zxfer_unsupported_volume_properties:-}
		;;
	*)
		l_existing_unsupported_properties=${g_zxfer_unsupported_filesystem_properties:-}
		;;
	esac

	case ",$l_existing_unsupported_properties," in
	*,"$l_property_name",*) ;;
	*)
		if [ -n "$l_existing_unsupported_properties" ]; then
			l_existing_unsupported_properties="${l_existing_unsupported_properties},$l_property_name"
		else
			l_existing_unsupported_properties=$l_property_name
		fi
		;;
	esac

	case "$l_source_dataset_type" in
	volume)
		g_zxfer_unsupported_volume_properties=$l_existing_unsupported_properties
		;;
	*)
		g_zxfer_unsupported_filesystem_properties=$l_existing_unsupported_properties
		;;
	esac
}

# Purpose: Select the unsupported properties for dataset type from the
# available input set.
# Usage: Called during property filtering, diffing, and apply when only a
# subset should flow into later comparison or apply steps.
# Returns: Comma-separated unsupported-property names for the requested dataset
# type.
zxfer_select_unsupported_properties_for_dataset_type() {
	l_source_dataset_type=$1

	case "$l_source_dataset_type" in
	volume)
		printf '%s\n' "${g_zxfer_unsupported_volume_properties:-}"
		;;
	*)
		printf '%s\n' "${g_zxfer_unsupported_filesystem_properties:-}"
		;;
	esac
}

# Purpose: Stage and reload one source dataset's property-name list through the
# checked runtime-artifact path.
# Usage: Called once per dataset before destination support probes so staging,
# cleanup, and exact failure statuses remain centralized.
# Side effects: Publishes the checked list through
# g_zxfer_property_stage_file_read_result.
zxfer_load_unsupported_property_source_list() {
	l_unsupported_scan_source=$1
	l_unsupported_source_property_list=$2

	zxfer_create_property_reconcile_stage_file ||
		zxfer_throw_error "Failed to allocate source property staging for unsupported-property scan [$l_unsupported_scan_source]." "$?"
	l_unsupported_source_props_tmp=$g_zxfer_property_reconcile_stage_file_result
	l_unsupported_source_stage_status=0
	zxfer_write_runtime_artifact_file "$l_unsupported_source_props_tmp" "$l_unsupported_source_property_list
" || l_unsupported_source_stage_status=$?
	if [ "$l_unsupported_source_stage_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_unsupported_source_props_tmp"
		zxfer_throw_error "Failed to stage source property list for unsupported-property scan [$l_unsupported_scan_source]." "$l_unsupported_source_stage_status"
	fi
	l_unsupported_source_read_status=0
	zxfer_read_property_reconcile_stage_file "$l_unsupported_source_props_tmp" >/dev/null ||
		l_unsupported_source_read_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_unsupported_source_props_tmp"
	if [ "$l_unsupported_source_read_status" -ne 0 ]; then
		zxfer_throw_error "Failed to read staged source property list for unsupported-property scan [$l_unsupported_scan_source]." "$l_unsupported_source_read_status"
	fi
}

# Purpose: Probe destination support for one source dataset's checked property
# list while deduplicating authoritative property/type results.
# Usage: Called once per recursive source dataset after its probe context is
# resolved; stops at the first ambiguous hard failure.
# Side effects: Updates l_resolved_source_property_type_pairs and publishes any
# failure through l_probe_error/l_probe_error_status.
zxfer_probe_unsupported_properties_for_dataset() {
	l_unsupported_source_type=$1
	l_unsupported_dest_probe_dataset=$2
	l_unsupported_dest_probe_type=$3
	l_unsupported_source_properties=$4
	l_probe_error=""

	while IFS= read -r l_probe_unsupported_property_name ||
		[ -n "$l_probe_unsupported_property_name" ]; do
		[ -n "$l_probe_unsupported_property_name" ] || continue
		l_seen_key="${l_unsupported_source_type}:${l_probe_unsupported_property_name}"
		case ",$l_resolved_source_property_type_pairs," in
		*,"$l_seen_key",*)
			continue
			;;
		esac

		l_dest_property_probe_status=0
		l_dest_property_probe=$(zxfer_run_destination_zfs_cmd get -Hpo property,value,source \
			"$l_probe_unsupported_property_name" "$l_unsupported_dest_probe_dataset" 2>&1) ||
			l_dest_property_probe_status=$?
		if [ "$l_dest_property_probe_status" -eq 0 ]; then
			l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
			continue
		fi
		case "$l_dest_property_probe" in
		*"invalid property"* | *"no such property"* | *"not supported"*)
			zxfer_append_unsupported_property_for_dataset_type "$l_unsupported_source_type" "$l_probe_unsupported_property_name"
			l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
			continue
			;;
		esac
		case "$l_dest_property_probe" in
		*"does not apply"*)
			if [ "$l_unsupported_dest_probe_type" = "$l_unsupported_source_type" ]; then
				zxfer_append_unsupported_property_for_dataset_type "$l_unsupported_source_type" "$l_probe_unsupported_property_name"
				l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
			fi
			continue
			;;
		esac
		if zxfer_destination_probe_is_ambiguous "$l_dest_property_probe"; then
			l_dest_property_probe="probe exited nonzero without stdout/stderr"
		fi
		l_probe_error="Failed to probe destination support for property [$l_probe_unsupported_property_name] on [$l_unsupported_dest_probe_dataset]: $l_dest_property_probe"
		l_probe_error_status=$l_dest_property_probe_status
		break
	done <<EOF
$l_unsupported_source_properties
EOF
}

# Purpose: Calculate the unsupported properties from the active configuration
# and runtime state.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a derived value without duplicating the calculation.
#
# Calculate the list of source properties unsupported on the destination by
# probing the destination directly for each source-side property name instead
# of inferring support from pool-root property presence. Unsupported results are
# cached per source dataset type so recursive trees with mixed filesystems and
# volumes do not treat one probe context as authoritative for every dataset.
# Dataset-type-specific results live in g_zxfer_unsupported_*_properties and
# callers select the current dataset type before filtering.
zxfer_calculate_unsupported_properties() {
	zxfer_reset_unsupported_property_state
	l_resolved_source_property_type_pairs=""
	l_scan_source_list=${g_recursive_source_list:-$g_initial_source}
	l_scan_sources=$(zxfer_split_tokens_on_whitespace "$l_scan_source_list")

	while IFS= read -r l_scan_source || [ -n "$l_scan_source" ]; do
		[ -n "$l_scan_source" ] || continue

		l_scan_source_type=$(zxfer_run_source_zfs_cmd get -Hpo value type "$l_scan_source" 2>&1) ||
			zxfer_throw_error "Failed to retrieve source dataset type for unsupported-property scan [$l_scan_source]: $l_scan_source_type" "$?"
		l_source_property_list=$(zxfer_run_source_zfs_cmd get -Hpo property all "$l_scan_source" 2>&1) ||
			zxfer_throw_error "Failed to retrieve source property list for dataset [$l_scan_source]: $l_source_property_list" "$?"
		l_dest_probe_dataset=$(zxfer_get_unsupported_property_probe_dataset_for_source "$l_scan_source") ||
			zxfer_throw_error "$l_dest_probe_dataset" "$?"
		l_dest_probe_dataset_type=$(zxfer_get_unsupported_property_probe_dataset_type "$l_dest_probe_dataset") ||
			zxfer_throw_error "$l_dest_probe_dataset_type" "$?"

		zxfer_load_unsupported_property_source_list "$l_scan_source" "$l_source_property_list"
		zxfer_probe_unsupported_properties_for_dataset "$l_scan_source_type" \
			"$l_dest_probe_dataset" "$l_dest_probe_dataset_type" \
			"$g_zxfer_property_stage_file_read_result"
		if [ -n "$l_probe_error" ]; then
			zxfer_throw_error "$l_probe_error" "$l_probe_error_status"
		fi
	done <<EOF
$l_scan_sources
EOF
}

################################################################################
# CREATE-TIME PROPERTY POLICY
################################################################################

# Purpose: Report whether a comma-separated property list has entries.
# Usage: Called before destination create planning so parent-creation handling
# can avoid combining `zfs create -p` with create-time properties.
zxfer_property_list_has_entries() {
	l_property_list=$1

	l_has_entries=1
	l_property_list_remaining=$l_property_list
	while [ -n "$l_property_list_remaining" ]; do
		case "$l_property_list_remaining" in
		*,*)
			l_property_entry=${l_property_list_remaining%%,*}
			l_property_list_remaining=${l_property_list_remaining#*,}
			;;
		*)
			l_property_entry=$l_property_list_remaining
			l_property_list_remaining=""
			;;
		esac
		if [ -n "$l_property_entry" ]; then
			l_has_entries=0
			break
		fi
	done

	return "$l_has_entries"
}

# Purpose: Remove child create overrides that the parent already supplies.
# Usage: Called during destination create planning so recursive -o overrides
# for inheritable properties can remain inherited on descendants when the
# parent has already been converged to the requested value.
zxfer_filter_child_creation_overrides_for_parent() {
	l_creation_pvs=$1
	l_parent_pvs=$2
	l_noninheritable_properties=$(zxfer_get_noninheritable_properties)

	"${g_cmd_awk:-awk}" \
		-v creation_pvs="$l_creation_pvs" \
		-v parent_pvs="$l_parent_pvs" \
		-v noninheritable_properties="$l_noninheritable_properties" '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
BEGIN {
	noninheritable_count = split(noninheritable_properties, noninheritable_items, ",")
	for (i = 1; i <= noninheritable_count; i++) {
		if (noninheritable_items[i] == "")
			continue
		noninheritable[noninheritable_items[i]] = 1
	}

	parent_count = split(parent_pvs, parent_items, ",")
	for (i = 1; i <= parent_count; i++) {
		if (parent_items[i] == "")
			continue
		split(parent_items[i], parent_fields, "=")
		if (!(parent_fields[1] in parent_value))
			parent_value[parent_fields[1]] = parent_fields[2]
	}

	creation_count = split(creation_pvs, creation_items, ",")
	for (i = 1; i <= creation_count; i++) {
		if (creation_items[i] == "")
			continue
		split(creation_items[i], creation_fields, "=")
		if (creation_fields[3] == "override" &&
			!(creation_fields[1] in noninheritable) &&
			(creation_fields[1] in parent_value) &&
			parent_value[creation_fields[1]] == creation_fields[2])
			continue
		filtered_creation = append_csv(filtered_creation, creation_items[i])
	}

	print filtered_creation
}'
}
