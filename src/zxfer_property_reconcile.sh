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
# PROPERTY APPLY / DIFF / RECONCILIATION HELPERS
################################################################################

# Module contract:
# owns globals: run-wide property filter state plus per-call property reconciliation scratch/result globals such as g_unsupported_properties, g_zxfer_new_rmvs_pv, g_zxfer_new_rmv_pvs, g_zxfer_new_mc_pvs, g_zxfer_only_supported_properties, g_zxfer_adjusted_set_list, g_zxfer_adjusted_inherit_list, g_zxfer_source_pvs_raw, g_zxfer_source_pvs_effective, g_zxfer_override_pvs_result, g_zxfer_creation_pvs_result, and g_zxfer_property_stage_file_read_result.
# reads globals: g_LZFS/g_RZFS, g_actual_dest, migration/platform state, and restore/backup mode state.
# mutates caches: destination property cache through create/set/inherit helpers.
# returns via stdout: filtered property lists, diff results, and dataset-create metadata.

ZXFER_BASE_READONLY_PROPERTIES="type,creation,used,available,referenced,\
compressratio,mounted,version,primarycache,secondarycache,\
usedbysnapshots,usedbydataset,usedbychildren,usedbyrefreservation,\
version,volsize,mountpoint,mlslabel,keysource,keystatus,rekeydate,encryption,encryptionroot,keylocation,keyformat,pbkdf2iters,snapshots_changed,special_small_blocks,\
refcompressratio,written,logicalused,logicalreferenced,createtxg,guid,origin,\
filesystem_count,snapshot_count,clones,defer_destroy,receive_resume_token,\
userrefs,objsetid"
ZXFER_FREEBSD_READONLY_PROPERTIES="aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,\
xattr,dnodesize"
ZXFER_SOLEXP_READONLY_PROPERTIES="jailed,aclmode,shareiscsi"

# Purpose: Return the base readonly properties in the form expected by later
# helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_base_readonly_properties() {
	printf '%s\n' "$ZXFER_BASE_READONLY_PROPERTIES"
}

# Purpose: Return the freebsd readonly properties in the form expected by later
# helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_freebsd_readonly_properties() {
	printf '%s\n' "$ZXFER_FREEBSD_READONLY_PROPERTIES"
}

# Purpose: Return the Solaris export readonly properties in the form expected
# by later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_solexp_readonly_properties() {
	printf '%s\n' "$ZXFER_SOLEXP_READONLY_PROPERTIES"
}

# Purpose: Return the effective readonly properties in the form expected by
# later helpers.
# Usage: Called during property filtering, diffing, and apply when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_effective_readonly_properties() {
	l_effective_readonly_properties=$(zxfer_get_base_readonly_properties)

	if [ "${g_destination_operating_system:-}" = "FreeBSD" ]; then
		l_platform_readonly_properties=$(zxfer_get_freebsd_readonly_properties)
		if [ -n "$l_platform_readonly_properties" ]; then
			if [ -n "$l_effective_readonly_properties" ]; then
				l_effective_readonly_properties="$l_effective_readonly_properties,$l_platform_readonly_properties"
			else
				l_effective_readonly_properties=$l_platform_readonly_properties
			fi
		fi
	fi
	if [ "${g_destination_operating_system:-}" = "SunOS" ] &&
		[ "${g_source_operating_system:-}" = "FreeBSD" ]; then
		l_platform_readonly_properties=$(zxfer_get_solexp_readonly_properties)
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

# Purpose: Reset the property runtime state so the next property-reconcile pass
# starts from a clean state.
# Usage: Called during property filtering, diffing, and apply before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_property_runtime_state() {
	g_unsupported_properties=""
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
}

# Purpose: Reset the property reconcile state so the next property-reconcile
# pass starts from a clean state.
# Usage: Called during property filtering, diffing, and apply before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_property_reconcile_state() {
	g_zxfer_new_rmvs_pv=""
	g_zxfer_new_rmv_pvs=""
	g_zxfer_new_mc_pvs=""
	g_zxfer_only_supported_properties=""
	g_zxfer_adjusted_set_list=""
	g_zxfer_adjusted_inherit_list=""
	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
	g_zxfer_override_pvs_result=""
	g_zxfer_creation_pvs_result=""
	g_zxfer_property_stage_file_read_result=""
}

# Purpose: Read the property reconcile stage file from staged state into the
# current shell.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a checked reload instead of ad hoc file reads.
zxfer_read_property_reconcile_stage_file() {
	l_stage_file=$1

	g_zxfer_property_stage_file_read_result=""
	if zxfer_read_runtime_artifact_file "$l_stage_file" >/dev/null; then
		g_zxfer_property_stage_file_read_result=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi
	case "$g_zxfer_property_stage_file_read_result" in
	*'
')
		g_zxfer_property_stage_file_read_result=${g_zxfer_property_stage_file_read_result%?}
		;;
	esac
	printf '%s\n' "$g_zxfer_property_stage_file_read_result"
}

################################################################################
# PROPERTY FILTER / NORMALIZATION HELPERS
################################################################################

# Purpose: Remove the sources from the current working set while preserving the
# module's special-case rules.
# Usage: Called during property filtering, diffing, and apply when filtering
# logic must trim staged data before later reconciliation or apply steps run.
#
# Drop the source field from property=value=source entries.
# Result is stored in $g_zxfer_new_rmvs_pv as property=value CSV.
zxfer_remove_sources() {
	g_zxfer_new_rmvs_pv=""

	l_rmvs_list=$1

	for l_rmvs_line in $l_rmvs_list; do
		l_rmvs_property=$(echo "$l_rmvs_line" | cut -f1 -d=)
		l_rmvs_value=$(echo "$l_rmvs_line" | cut -f2 -d=)
		g_zxfer_new_rmvs_pv="$g_zxfer_new_rmvs_pv$l_rmvs_property=$l_rmvs_value,"
	done

	g_zxfer_new_rmvs_pv=${g_zxfer_new_rmvs_pv%,}
}

# Purpose: Select only the must-create property entries that belong in the
# destination creation set.
# Usage: Called during property filtering, diffing, and apply when zxfer builds
# the minimal property subset that must be present at `zfs create` time.
#
# Keep only the requested property=value=source entries.
# Used for the "must create" property set.
zxfer_select_mc() {
	g_zxfer_new_mc_pvs=""

	l_mc_list=$1          # target list of properties, values
	l_mc_property_list=$2 # list of properties to select

	for l_mc_line in $l_mc_list; do
		l_found_mc=0

		l_mc_property=$(echo "$l_mc_line" | cut -f1 -d=)
		l_mc_value=$(echo "$l_mc_line" | cut -f2 -d=)
		l_mc_source=$(echo "$l_mc_line" | cut -f3 -d=)

		for l_property in $l_mc_property_list; do
			if [ "$l_property" = "$l_mc_property" ]; then
				l_found_mc=1
				# Remove matched properties from the remaining filter list so
				# later iterations do not rescan them unnecessarily.
				l_mc_property_list=$(echo "$l_mc_property_list" | tr -s "," "\n" |
					grep -v ^"$l_property"$ | tr -s "\n" ",")
				break
			fi
		done

		if [ $l_found_mc -eq 1 ]; then
			g_zxfer_new_mc_pvs="$g_zxfer_new_mc_pvs$l_mc_property=$l_mc_value=$l_mc_source,"
		fi
	done

	g_zxfer_new_mc_pvs=${g_zxfer_new_mc_pvs%,}
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
	g_zxfer_new_rmv_pvs="" # global

	l_rmv_list=$1    # the list of properties=values=sources,...
	l_remove_list=$2 # list of properties to remove

	for l_rmv_line in $l_rmv_list; do
		l_found_readonly=0
		l_rmv_property=$(echo "$l_rmv_line" | cut -f1 -d=)
		l_rmv_value=$(echo "$l_rmv_line" | cut -f2 -d=)
		l_rmv_source=$(echo "$l_rmv_line" | cut -f3 -d=)
		for l_property in $l_remove_list; do
			if [ "$l_property" = "$l_rmv_property" ]; then
				if [ "$l_rmv_source" = "override" ]; then
					# The user has specifically required we set this property
					continue
				fi
				l_found_readonly=1
				# Since the property was matched, remove it from the remaining
				# filter list so later iterations do not rescan it unnecessarily.
				l_remove_list=$(echo "$l_remove_list" | tr -s "," "\n" | grep -v ^"$l_property"$)
				l_remove_list=$(echo "$l_remove_list" | tr -s "\n" ",")
				break
			fi
		done
		if [ $l_found_readonly -eq 0 ]; then
			g_zxfer_new_rmv_pvs="$g_zxfer_new_rmv_pvs$l_rmv_property=$l_rmv_value=$l_rmv_source,"
		fi
	done

	g_zxfer_new_rmv_pvs=${g_zxfer_new_rmv_pvs%,}
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
	zxfer_get_temp_file >/dev/null
	l_filter_tmp=$g_zxfer_temp_file_result
	g_zxfer_only_supported_properties=""

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

	if zxfer_read_property_reconcile_stage_file "$l_filter_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_filter_tmp"
		return "$l_read_status"
	fi
	{
		IFS= read -r g_zxfer_only_supported_properties
		while IFS= read -r l_warning || [ -n "$l_warning" ]; do
			[ -n "$l_warning" ] || continue
			zxfer_warn_stderr "$l_warning"
		done
	} <<EOF
$g_zxfer_property_stage_file_read_result
EOF
	zxfer_cleanup_runtime_artifact_path "$l_filter_tmp"
}

# Purpose: Run the ZFS create with properties through the controlled execution
# path owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
#
# Build and execute a "zfs create" command while safely passing property=value
# assignments as individual arguments, avoiding eval so property data cannot be
# treated as shell syntax.
# $1: "yes" to include -p (create parent datasets), anything else skips it
# $2: dataset type (volume/filesystem) to decide whether -V is required
# $3: volume size (only used when type=volume)
# $4: comma-separated property=value list (sources already removed)
# $5: destination dataset name
zxfer_run_zfs_create_with_properties() {
	l_with_parents=$1
	l_dataset_type=$2
	l_volume_size=$3
	l_property_list=$4
	l_destination=$5

	if [ "$l_dataset_type" = "volume" ] && [ -z "$l_volume_size" ]; then
		return 1
	fi

	(
		set -- create

		if [ "$l_with_parents" = "yes" ]; then
			set -- "$@" "-p"
		fi

		if [ "$l_dataset_type" = "volume" ] && [ -n "$l_volume_size" ]; then
			set -- "$@" "-V" "$l_volume_size"
		fi

		l_cmd_ifs=$IFS
		IFS=","
		for l_prop_value in $l_property_list; do
			if [ "$l_prop_value" != "" ]; then
				l_decoded_assignment=$(zxfer_emit_decoded_property_assignments "$l_prop_value")
				set -- "$@" "-o" "$l_decoded_assignment"
			fi
		done
		IFS=$l_cmd_ifs

		set -- "$@" "$l_destination"

		if [ "$g_option_n_dryrun" -eq 0 ]; then
			zxfer_run_destination_zfs_cmd "$@"
		else
			zxfer_build_destination_zfs_command "$@"
		fi
	)
}

# Purpose: Clear readonly properties from the live apply set while preserving
# the module's compatibility rules.
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

	printf '%s\n' "$(echo "$1" | sed -e 's/readonly=on/readonly=off/g')"
}

# Purpose: Collect the source props into the module-owned format used by later
# steps.
# Usage: Called during property filtering, diffing, and apply before
# reconciliation or apply logic consumes the combined result.
#
# Collect the source property list and derive the effective list used for
# transfer. Results are stored in module variables:
#  g_zxfer_source_pvs_raw - normalized properties from the live source
#  g_zxfer_source_pvs_effective - properties after restore/writable handling
# $1: source dataset
# $2: destination dataset
# $3: ensure-writable flag (1 to force readonly=off)
# $4: zfs command used to inspect the source (defaults to $g_LZFS)
zxfer_collect_source_props() {
	l_source=$1
	l_destination=$2
	l_ensure_writable=$3
	l_zfs_cmd=$4

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
	l_source_props_tmp_status=0
	zxfer_get_temp_file >/dev/null || l_source_props_tmp_status=$?
	if [ "$l_source_props_tmp_status" -ne 0 ]; then
		return "$l_source_props_tmp_status"
	fi
	l_source_props_tmp=$g_zxfer_temp_file_result
	l_source_props_status=0
	zxfer_get_normalized_dataset_properties "$l_source" "$l_zfs_cmd" source >"$l_source_props_tmp" ||
		l_source_props_status=$?
	if [ "$l_source_props_status" -ne 0 ]; then
		if zxfer_read_property_reconcile_stage_file "$l_source_props_tmp" >/dev/null; then
			:
		else
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
			return "$l_read_status"
		fi
		g_zxfer_source_pvs_raw=$g_zxfer_property_stage_file_read_result
		zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
		printf '%s\n' "$g_zxfer_source_pvs_raw"
		return "$l_source_props_status"
	fi
	if zxfer_read_property_reconcile_stage_file "$l_source_props_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
		return "$l_read_status"
	fi
	g_zxfer_source_pvs_raw=$g_zxfer_property_stage_file_read_result
	zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
	g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw

	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		if [ -n "$g_restored_backup_file_contents" ]; then
			l_restore_format_status=0
			zxfer_validate_backup_metadata_format "$g_restored_backup_file_contents" ||
				l_restore_format_status=$?
			case $l_restore_format_status in
			0) ;;
			1)
				zxfer_throw_usage_error "Restored properties for the filesystem $l_source and destination $l_destination do not start with the required zxfer backup metadata header"
				;;
			2)
				zxfer_throw_usage_error "Restored properties for the filesystem $l_source and destination $l_destination do not declare supported zxfer backup metadata format version #format_version:$(zxfer_get_backup_metadata_format_version)"
				;;
			*)
				zxfer_throw_usage_error "Failed to validate the restored backup metadata for the filesystem $l_source and destination $l_destination"
				;;
			esac
		fi
		l_restore_status=0
		g_zxfer_source_pvs_effective=$(zxfer_backup_metadata_extract_properties_for_dataset_pair \
			"$g_restored_backup_file_contents" "$l_source" "$l_destination") ||
			l_restore_status=$?
		case $l_restore_status in
		0) ;;
		1)
			zxfer_throw_usage_error "Can't find the properties for the filesystem $l_source and destination $l_destination"
			;;
		2)
			zxfer_throw_usage_error "Multiple restored property entries matched filesystem $l_source and destination $l_destination"
			;;
		*)
			zxfer_throw_usage_error "Failed to parse the restored properties for the filesystem $l_source and destination $l_destination"
			;;
		esac
	fi

	if [ "$l_ensure_writable" -eq 1 ]; then
		g_zxfer_source_pvs_effective=$(zxfer_force_readonly_off "$g_zxfer_source_pvs_effective")
	fi
}

# Purpose: Validate the override properties before zxfer relies on it.
# Usage: Called during property filtering, diffing, and apply to fail closed on
# malformed, unsafe, or stale input.
#
# Validate that every override property supplied via -o exists on the source.
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

	if [ "$l_status" -eq 1 ] && [ -n "$l_validation_result" ]; then
		zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
	elif [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to validate override properties."
	fi
}

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
	g_zxfer_override_pvs_result=""
	g_zxfer_creation_pvs_result=""

	# awk program needs literal $-style fields; shell variables are passed with -v.
	l_status=0
	# shellcheck disable=SC2016
	l_derived_lists=$(
		ZXFER_AWK_OVERRIDE_OPTIONS=$l_override_options "${g_cmd_awk:-awk}" \
			-v source_pvs="$l_source_pvs" \
			-v transfer_all_flag="$l_transfer_all_flag" \
			-v source_dstype="$l_source_dstype" '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
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
		if (transfer_all_flag == 0)
			override_output = append_csv(override_output, override_fields[1] "=" override_fields[2] "=override")
		if (!(override_fields[1] in override_value))
			override_value[override_fields[1]] = override_fields[2]
	}

	if (transfer_all_flag == 0) {
		print override_output
		print creation_output
		exit 0
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

		if (source_property in override_value) {
			override_output = append_csv(override_output, source_property "=" override_value[source_property] "=override")
			continue
		}

		override_output = append_csv(override_output, source_property "=" source_value "=" source_source)
		if (source_source == "local" || (source_dstype == "volume" && source_property == "refreservation"))
			creation_output = append_csv(creation_output, source_property "=" source_value "=" source_source)
	}

	print override_output
	print creation_output
}'
	) || l_status=$?

	if [ "$l_status" -eq 1 ] && [ "$l_derived_lists" = "__ZXFER_OVERRIDE_SYNTAX__" ]; then
		zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
	elif [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to derive override property lists."
	fi

	{
		IFS= read -r g_zxfer_override_pvs_result
		IFS= read -r g_zxfer_creation_pvs_result
	} <<EOF
$l_derived_lists
EOF

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
	l_oldifs=$IFS
	IFS=","

	if [ -n "$l_remove_list" ]; then
		zxfer_remove_properties "$l_filtered_list" "$l_remove_list"
		l_filtered_list="$g_zxfer_new_rmv_pvs"
	fi

	if [ -n "$l_ignore_list" ]; then
		zxfer_remove_properties "$l_filtered_list" "$l_ignore_list"
		l_filtered_list="$g_zxfer_new_rmv_pvs"
	fi

	IFS=$l_oldifs
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

# Purpose: Check whether the destination property probe is unsupported.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a boolean answer about the destination property probe.
zxfer_destination_property_probe_is_unsupported() {
	case "$1" in
	*"invalid property"* | *"no such property"* | *"not supported"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Check whether the destination property probe is inconclusive.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a boolean answer about the destination property probe.
zxfer_destination_property_probe_is_inconclusive() {
	case "$1" in
	*"does not apply"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Check whether the destination property probe dataset is queryable.
# Usage: Called during property filtering, diffing, and apply when later
# helpers need a boolean answer about the destination property probe dataset.
zxfer_destination_property_probe_dataset_is_queryable() {
	l_probe_dataset=$1

	zxfer_run_destination_zfs_cmd get -Hpo property all "$l_probe_dataset" >/dev/null 2>&1
}

# Purpose: Format the destination property probe failure detail for display or
# serialized output.
# Usage: Called during property filtering, diffing, and apply when operators or
# downstream helpers need a stable presentation.
zxfer_format_destination_property_probe_failure_detail() {
	l_probe_output=$1

	if zxfer_destination_probe_is_ambiguous "$l_probe_output"; then
		printf '%s\n' "probe exited nonzero without stdout/stderr"
		return 0
	fi

	printf '%s\n' "$l_probe_output"
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
zxfer_select_unsupported_properties_for_dataset_type() {
	l_source_dataset_type=$1

	case "$l_source_dataset_type" in
	volume)
		g_unsupported_properties=${g_zxfer_unsupported_volume_properties:-}
		;;
	*)
		g_unsupported_properties=${g_zxfer_unsupported_filesystem_properties:-}
		;;
	esac
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
# The union is stored in g_unsupported_properties for compatibility, while
# dataset-type-specific results live in g_zxfer_unsupported_*_properties.
zxfer_calculate_unsupported_properties() {
	g_unsupported_properties=""
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
	l_resolved_source_property_type_pairs=""
	l_scan_source_list=${g_recursive_source_list:-$g_initial_source}

	for l_scan_source in $l_scan_source_list; do
		[ -n "$l_scan_source" ] || continue

		l_scan_source_type_status=0
		l_scan_source_type=$(zxfer_run_source_zfs_cmd get -Hpo value type "$l_scan_source" 2>&1) ||
			l_scan_source_type_status=$?
		if [ "$l_scan_source_type_status" -ne 0 ]; then
			zxfer_throw_error "Failed to retrieve source dataset type for unsupported-property scan [$l_scan_source]: $l_scan_source_type" "$l_scan_source_type_status"
		fi
		l_source_property_list_status=0
		l_source_property_list=$(zxfer_run_source_zfs_cmd get -Hpo property all "$l_scan_source" 2>&1) ||
			l_source_property_list_status=$?
		if [ "$l_source_property_list_status" -ne 0 ]; then
			zxfer_throw_error "Failed to retrieve source property list for dataset [$l_scan_source]: $l_source_property_list" "$l_source_property_list_status"
		fi
		l_dest_probe_dataset_status=0
		l_dest_probe_dataset=$(zxfer_get_unsupported_property_probe_dataset_for_source "$l_scan_source") ||
			l_dest_probe_dataset_status=$?
		if [ "$l_dest_probe_dataset_status" -ne 0 ]; then
			zxfer_throw_error "$l_dest_probe_dataset" "$l_dest_probe_dataset_status"
		fi
		l_dest_probe_dataset_type_status=0
		l_dest_probe_dataset_type=$(zxfer_get_unsupported_property_probe_dataset_type "$l_dest_probe_dataset") ||
			l_dest_probe_dataset_type_status=$?
		if [ "$l_dest_probe_dataset_type_status" -ne 0 ]; then
			zxfer_throw_error "$l_dest_probe_dataset_type" "$l_dest_probe_dataset_type_status"
		fi

		l_source_props_tmp_status=0
		zxfer_get_temp_file >/dev/null || l_source_props_tmp_status=$?
		if [ "$l_source_props_tmp_status" -ne 0 ]; then
			zxfer_throw_error "Failed to allocate source property staging for unsupported-property scan [$l_scan_source]." "$l_source_props_tmp_status"
		fi
		l_source_props_tmp=$g_zxfer_temp_file_result
		l_source_stage_status=0
		zxfer_write_runtime_artifact_file "$l_source_props_tmp" "$l_source_property_list
" || l_source_stage_status=$?
		if [ "$l_source_stage_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
			zxfer_throw_error "Failed to stage source property list for unsupported-property scan [$l_scan_source]." "$l_source_stage_status"
		fi
		l_probe_error=""
		l_source_read_status=0
		zxfer_read_property_reconcile_stage_file "$l_source_props_tmp" >/dev/null ||
			l_source_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
		if [ "$l_source_read_status" -ne 0 ]; then
			zxfer_throw_error "Failed to read staged source property list for unsupported-property scan [$l_scan_source]." "$l_source_read_status"
		fi

		while IFS= read -r s_p || [ -n "$s_p" ]; do
			[ -n "$s_p" ] || continue
			l_seen_key="${l_scan_source_type}:${s_p}"
			case ",$l_resolved_source_property_type_pairs," in
			*,"$l_seen_key",*)
				continue
				;;
			esac

			l_dest_property_probe_status=0
			l_dest_property_probe=$(zxfer_run_destination_zfs_cmd get -Hpo property,value,source "$s_p" "$l_dest_probe_dataset" 2>&1) ||
				l_dest_property_probe_status=$?
			if [ "$l_dest_property_probe_status" -eq 0 ]; then
				l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
				continue
			fi
			if zxfer_destination_property_probe_is_unsupported "$l_dest_property_probe"; then
				zxfer_append_unsupported_property_for_dataset_type "$l_scan_source_type" "$s_p"
				l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
				continue
			fi
			if zxfer_destination_property_probe_is_inconclusive "$l_dest_property_probe"; then
				if [ "$l_dest_probe_dataset_type" = "$l_scan_source_type" ]; then
					zxfer_append_unsupported_property_for_dataset_type "$l_scan_source_type" "$s_p"
					l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
				fi
				continue
			fi
			# Some illumos/OpenZFS builds reject unsupported property-name probes on
			# pool-root fallback datasets with a nonzero status and no stderr. When a
			# generic property listing still succeeds on that dataset, treat the blank
			# property-specific rejection like the existing inconclusive path.
			if zxfer_destination_probe_is_ambiguous "$l_dest_property_probe" &&
				zxfer_destination_property_probe_dataset_is_queryable "$l_dest_probe_dataset"; then
				if [ "$l_dest_probe_dataset_type" = "$l_scan_source_type" ]; then
					zxfer_append_unsupported_property_for_dataset_type "$l_scan_source_type" "$s_p"
					l_resolved_source_property_type_pairs="${l_resolved_source_property_type_pairs}${l_seen_key},"
				fi
				continue
			fi
			l_dest_property_probe=$(zxfer_format_destination_property_probe_failure_detail "$l_dest_property_probe")
			l_probe_error="Failed to probe destination support for property [$s_p] on [$l_dest_probe_dataset]: $l_dest_property_probe"
			l_probe_error_status=$l_dest_property_probe_status
			break
		done <<EOF
$g_zxfer_property_stage_file_read_result
EOF
		if [ -n "$l_probe_error" ]; then
			zxfer_throw_error "$l_probe_error" "$l_probe_error_status"
		fi
	done

	g_unsupported_properties=${g_zxfer_unsupported_filesystem_properties:-}
	l_oldifs=$IFS
	IFS=","
	for l_unsupported_property in ${g_zxfer_unsupported_volume_properties:-}; do
		[ -n "$l_unsupported_property" ] || continue
		case ",$g_unsupported_properties," in
		*,"$l_unsupported_property",*) ;;
		*)
			if [ -n "$g_unsupported_properties" ]; then
				g_unsupported_properties="${g_unsupported_properties},$l_unsupported_property"
			else
				g_unsupported_properties=$l_unsupported_property
			fi
			;;
		esac
	done
	IFS=$l_oldifs
}

################################################################################
# DESTINATION CREATE / APPLY HELPERS
################################################################################

# Purpose: Ensure the destination exists exists and is ready before the flow
# continues.
# Usage: Called during property filtering, diffing, and apply before later
# helpers assume the resource or cache is available.
#
# Create the destination dataset when it does not exist.
# Returns 0 when the dataset was created (no further work needed) and 1 when
# it already exists and requires diffing.
# $1: dest_exist flag (0 when absent)
# $2: is_initial_source flag (1 when processing the root source)
# $3: override property list
# $4: creation property list
# $5: source dataset type
# $6: source volume size
# $7: destination dataset name
# $8: readonly property list used for child creation sanitization
# $9: optional runner for zfs create (defaults to zxfer_run_zfs_create_with_properties)
zxfer_ensure_destination_exists() {
	l_dest_exist=$1
	l_is_initial_source=$2
	l_override_pvs=$3
	l_creation_pvs=$4
	l_source_dstype=$5
	l_source_volsize=$6
	l_destination=$7
	l_readonly_properties=$8
	l_create_runner=$9

	if [ "$l_dest_exist" != "0" ]; then
		return 1
	fi

	if [ -z "$l_create_runner" ]; then
		l_create_runner="zxfer_run_zfs_create_with_properties"
	fi

	zxfer_echov "Creating destination filesystem \"$l_destination\" with specified properties."

	l_oldifs=$IFS
	IFS=","
	if [ "$l_is_initial_source" -eq 1 ]; then
		zxfer_remove_sources "$l_override_pvs"
		l_property_list="$g_zxfer_new_rmvs_pv"
		l_with_parents="no"
		l_parent_dataset=${l_destination%/*}
		if [ "$l_parent_dataset" != "$l_destination" ]; then
			l_parent_exists_status=0
			l_parent_exists=$(zxfer_exists_destination "$l_parent_dataset") ||
				l_parent_exists_status=$?
			if [ "$l_parent_exists_status" -ne 0 ]; then
				zxfer_throw_error "$l_parent_exists" "$l_parent_exists_status"
			fi
			if [ "$l_parent_exists" -eq 0 ]; then
				l_with_parents="yes"
			fi
		fi
	else
		l_filtered_creation=$(zxfer_sanitize_property_list "$l_creation_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")
		zxfer_remove_sources "$l_filtered_creation"
		l_property_list="$g_zxfer_new_rmvs_pv"
		l_with_parents="yes"
	fi
	IFS=$l_oldifs

	l_create_status=0
	$l_create_runner "$l_with_parents" "$l_source_dstype" "$l_source_volsize" "$l_property_list" "$l_destination" ||
		l_create_status=$?
	if [ "$l_create_status" -ne 0 ]; then
		zxfer_throw_error "Error when creating destination filesystem." "$l_create_status"
	fi

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_note_destination_dataset_exists "$l_destination"
		zxfer_invalidate_destination_property_cache "$l_destination"
	fi

	return 0
}

# Purpose: Build the destination ZFS command for the next execution or
# comparison step.
# Usage: Called during property filtering, diffing, and apply before other
# helpers consume the assembled value.
zxfer_build_destination_zfs_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_T_target_host" = "" ]; then
		if [ -n "$g_RZFS" ] && [ "$g_RZFS" != "$g_cmd_zfs" ]; then
			zxfer_render_command_for_report "$g_RZFS" "$l_subcommand" "$@"
		else
			zxfer_render_command_for_report "" "$g_cmd_zfs" "$l_subcommand" "$@"
		fi
		return
	fi

	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_target_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(zxfer_quote_token_stream "$l_remote_tokens")
	zxfer_build_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd"
}

# Purpose: Build the destination ZFS property command for the next execution or
# comparison step.
# Usage: Called during property filtering, diffing, and apply before other
# helpers consume the assembled value.
zxfer_build_destination_zfs_property_command() {
	zxfer_build_destination_zfs_command "$@"
}

# Purpose: Run the destination ZFS property command through the controlled
# execution path owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
zxfer_run_destination_zfs_property_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_T_target_host" = "" ]; then
		zxfer_run_destination_zfs_cmd "$l_subcommand" "$@"
		return
	fi

	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_target_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(zxfer_quote_token_stream "$l_remote_tokens")
	zxfer_invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd" destination
}

# Purpose: Run the ZFS set assignments through the controlled execution path
# owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
zxfer_run_zfs_set_assignments() {
	l_destination=$1
	shift

	[ "$#" -gt 0 ] || return 0

	l_display_cmd=$(zxfer_build_destination_zfs_property_command set "$@" "$l_destination")

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_echov "$l_display_cmd"
		l_set_status=0
		zxfer_run_destination_zfs_property_command set "$@" "$l_destination" || l_set_status=$?
		if [ "$l_set_status" -ne 0 ]; then
			zxfer_throw_error "Error when setting properties on destination filesystem." "$l_set_status"
		fi
		zxfer_invalidate_destination_property_cache "$l_destination"
	else
		echo "$l_display_cmd"
	fi
}

# Purpose: Run the ZFS set properties through the controlled execution path
# owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
#
# Default runner for batched `zfs set`.
# $1: comma-separated property=value list
# $2: destination dataset
zxfer_run_zfs_set_properties() {
	l_property_list=$1
	l_destination=$2

	[ -n "$l_property_list" ] || return 0

	set --
	while IFS= read -r l_assignment || [ -n "$l_assignment" ]; do
		[ -n "$l_assignment" ] || continue
		set -- "$@" "$l_assignment"
	done <<EOF
$(zxfer_emit_decoded_property_assignments "$l_property_list")
EOF

	zxfer_run_zfs_set_assignments "$l_destination" "$@"
}

# Purpose: Run the ZFS set property through the controlled execution path owned
# by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
#
# Compatibility wrapper for code paths that still set one property at a time.
# $1: property name
# $2: property value
# $3: destination dataset
zxfer_run_zfs_set_property() {
	l_property=$1
	l_value=$2
	l_destination=$3

	zxfer_run_zfs_set_assignments "$l_destination" "$l_property=$l_value"
}

# Purpose: Run the ZFS inherit property through the controlled execution path
# owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
#
# Default runner for `zfs inherit`.
# $1: property name
# $2: destination dataset
zxfer_run_zfs_inherit_property() {
	l_property=$1
	l_destination=$2
	l_display_cmd=$(zxfer_build_destination_zfs_property_command inherit "$l_property" "$l_destination")

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_echov "$l_display_cmd"
		l_inherit_status=0
		zxfer_run_destination_zfs_property_command inherit "$l_property" "$l_destination" ||
			l_inherit_status=$?
		if [ "$l_inherit_status" -ne 0 ]; then
			zxfer_throw_error "Error when inheriting properties on destination filesystem." "$l_inherit_status"
		fi
		zxfer_invalidate_destination_property_cache "$l_destination"
	else
		echo "$l_display_cmd"
	fi
}

# Purpose: Diff the properties so later helpers act on exact deltas.
# Usage: Called during property filtering, diffing, and apply before
# reconciliation or apply logic mutates live state from the computed
# difference.
#
# Compare override and destination property lists, enforcing "must create"
# restrictions and returning the required set/inherit operations.
# Returns three newline-separated lines: initial_set_list, set_list, inherit_list.
# $1: override property list
# $2: destination property list
# $3: must-create property names
zxfer_diff_properties() {
	l_override_pvs=$1
	l_dest_pvs=$2
	l_must_create_properties=$3

	zxfer_get_temp_file >/dev/null
	l_diff_tmp=$g_zxfer_temp_file_result
	"${g_cmd_awk:-awk}" -v override_pvs="$l_override_pvs" \
		-v dest_pvs="$l_dest_pvs" \
		-v must_create_properties="$l_must_create_properties" '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function source_requires_local_set(source_value) {
	return (source_value == "local" || source_value == "override")
}
BEGIN {
	must_create_count = split(must_create_properties, must_create_items, ",")
	for (i = 1; i <= must_create_count; i++) {
		if (must_create_items[i] == "")
			continue
		must_create[must_create_items[i]] = 1
	}

	dest_count = split(dest_pvs, dest_items, ",")
	for (i = 1; i <= dest_count; i++) {
		if (dest_items[i] == "")
			continue
		split(dest_items[i], dest_fields, "=")
		dest_property = dest_fields[1]
		if (!(dest_property in dest_available)) {
			dest_available[dest_property] = 1
			dest_value[dest_property] = dest_fields[2]
			dest_source[dest_property] = dest_fields[3]
		}
	}

	override_count = split(override_pvs, override_items, ",")
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		split(override_items[i], override_fields, "=")
		override_property[i] = override_fields[1]
		override_value[i] = override_fields[2]
		override_source[i] = override_fields[3]
		if ((override_property[i] in must_create) &&
			(override_property[i] in dest_available) &&
			override_value[i] != dest_value[override_property[i]]) {
			print override_property[i]
			exit 3
		}
	}

	print "__ZXFER_DIFF_OK__"
		for (i = 1; i <= override_count; i++) {
			if (override_property[i] == "" || (override_property[i] in must_create))
				continue
			if (!(override_property[i] in dest_available)) {
				if (source_requires_local_set(override_source[i])) {
					initial_set_list = append_csv(initial_set_list, override_property[i] "=" override_value[i])
					child_set_list = append_csv(child_set_list, override_property[i] "=" override_value[i])
				}
				continue
			}

			if (dest_value[override_property[i]] != override_value[i] ||
				dest_source[override_property[i]] != "local") {
				initial_set_list = append_csv(initial_set_list, override_property[i] "=" override_value[i])
			}

		if (override_value[i] != dest_value[override_property[i]]) {
			if (source_requires_local_set(override_source[i]))
				child_set_list = append_csv(child_set_list, override_property[i] "=" override_value[i])
			else
				inherit_list = append_csv(inherit_list, override_property[i] "=" override_value[i])
		} else if (source_requires_local_set(override_source[i]) &&
			dest_source[override_property[i]] != "local") {
			child_set_list = append_csv(child_set_list, override_property[i] "=" override_value[i])
		} else if (!source_requires_local_set(override_source[i]) &&
			dest_source[override_property[i]] == "local") {
			inherit_list = append_csv(inherit_list, override_property[i] "=" override_value[i])
		}

		delete dest_available[override_property[i]]
	}

	print initial_set_list
	print child_set_list
	print inherit_list
}' >"$l_diff_tmp"
	l_status=$?

	if [ "$l_status" -eq 3 ]; then
		if zxfer_read_property_reconcile_stage_file "$l_diff_tmp" >/dev/null; then
			:
		else
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
			return "$l_read_status"
		fi
		IFS= read -r l_mismatch_property <<EOF
$g_zxfer_property_stage_file_read_result
EOF
		zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
		zxfer_throw_error_with_usage "The property \"$l_mismatch_property\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
	elif [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
		zxfer_throw_error "Failed to diff dataset properties."
	fi

	if zxfer_read_property_reconcile_stage_file "$l_diff_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
		return "$l_read_status"
	fi
	{
		IFS= read -r l_diff_ok_marker
		IFS= read -r l_initial_set_list
		IFS= read -r l_child_set_list
		IFS= read -r l_inherit_list
	} <<EOF
$g_zxfer_property_stage_file_read_result
EOF
	zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
	[ "$l_diff_ok_marker" = "__ZXFER_DIFF_OK__" ] || zxfer_throw_error "Failed to diff dataset properties."
	printf '%s\n' "$l_initial_set_list"
	printf '%s\n' "$l_child_set_list"
	printf '%s\n' "$l_inherit_list"
}

# Purpose: Adjust the child inherit to match parent to match the state later
# helpers expect.
# Usage: Called during property filtering, diffing, and apply when a planned
# property or inheritance change needs one centralized rewrite step.
#
# Adjust child inheritance requests so they only remain inherited when the
# destination parent already provides the desired effective value. Otherwise the
# property must be set locally on the child to converge on the source value.
# Returns two newline-separated lines: updated_set_list, updated_inherit_list.
# $1: destination dataset
# $2: override property list
# $3: current child set list
# $4: current inherit list
# $5: readonly property list used when sanitizing parent properties
zxfer_adjust_child_inherit_to_match_parent() {
	l_destination=$1
	l_override_pvs=$2
	l_set_list=$3
	l_inherit_list=$4
	l_readonly_properties=$5
	g_zxfer_adjusted_set_list=""
	g_zxfer_adjusted_inherit_list=""

	if [ -z "$l_inherit_list" ] && [ -z "$l_set_list" ]; then
		g_zxfer_adjusted_set_list=$l_set_list
		g_zxfer_adjusted_inherit_list=$l_inherit_list
		printf '%s\n' "$g_zxfer_adjusted_set_list"
		printf '%s\n' "$g_zxfer_adjusted_inherit_list"
		return
	fi

	l_parent_dataset=${l_destination%/*}
	if [ "$l_parent_dataset" = "$l_destination" ]; then
		g_zxfer_adjusted_set_list=$l_set_list
		g_zxfer_adjusted_inherit_list=$l_inherit_list
		printf '%s\n' "$g_zxfer_adjusted_set_list"
		printf '%s\n' "$g_zxfer_adjusted_inherit_list"
		return
	fi

	l_parent_exists_status=0
	l_parent_exists=$(zxfer_exists_destination "$l_parent_dataset") || l_parent_exists_status=$?
	if [ "$l_parent_exists_status" -ne 0 ]; then
		zxfer_throw_error "$l_parent_exists" "$l_parent_exists_status"
	fi
	if [ "$l_parent_exists" -eq 0 ]; then
		g_zxfer_adjusted_set_list=$l_set_list
		g_zxfer_adjusted_inherit_list=$l_inherit_list
		printf '%s\n' "$g_zxfer_adjusted_set_list"
		printf '%s\n' "$g_zxfer_adjusted_inherit_list"
		return
	fi

	zxfer_get_temp_file >/dev/null
	l_parent_dest_tmp=$g_zxfer_temp_file_result
	l_parent_dest_status=0
	zxfer_collect_destination_props "$l_parent_dataset" "$g_RZFS" >"$l_parent_dest_tmp" ||
		l_parent_dest_status=$?
	if [ "$l_parent_dest_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_parent_dest_tmp"
		return "$l_parent_dest_status"
	fi
	if [ "$g_zxfer_normalized_dataset_properties_cache_hit" -eq 0 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_parent_destination_property_reads
	fi
	if zxfer_read_property_reconcile_stage_file "$l_parent_dest_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_parent_dest_tmp"
		return "$l_read_status"
	fi
	l_parent_dest_pvs=$g_zxfer_property_stage_file_read_result
	zxfer_cleanup_runtime_artifact_path "$l_parent_dest_tmp"
	l_parent_dest_pvs=$(zxfer_sanitize_property_list "$l_parent_dest_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")

	l_status=0
	l_adjusted_lists=$(
		"${g_cmd_awk:-awk}" \
			-v override_pvs="$l_override_pvs" \
			-v parent_pvs="$l_parent_dest_pvs" \
			-v current_set_list="$l_set_list" \
			-v inherit_list="$l_inherit_list" '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function source_requires_local_set(source_value) {
	return (source_value == "local" || source_value == "override")
}
BEGIN {
	override_count = split(override_pvs, override_items, ",")
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		split(override_items[i], override_fields, "=")
		override_source[override_fields[1]] = override_fields[3]
		override_value[override_fields[1]] = override_fields[2]
	}

	parent_count = split(parent_pvs, parent_items, ",")
	for (i = 1; i <= parent_count; i++) {
		if (parent_items[i] == "")
			continue
		split(parent_items[i], parent_fields, "=")
		if (!(parent_fields[1] in parent_value))
			parent_value[parent_fields[1]] = parent_fields[2]
	}

	set_count = split(current_set_list, set_items, ",")
	for (i = 1; i <= set_count; i++) {
		if (set_items[i] == "")
			continue
		split(set_items[i], set_fields, "=")
		set_property = set_fields[1]
		set_value = set_fields[2]

		if (!(set_property in override_source) ||
			source_requires_local_set(override_source[set_property])) {
			new_set_list = append_csv(new_set_list, set_items[i])
			continue
		}

		if ((set_property in parent_value) &&
			parent_value[set_property] == set_value) {
			new_inherit_list = append_csv(new_inherit_list, set_items[i])
		} else {
			new_set_list = append_csv(new_set_list, set_items[i])
		}
	}

	inherit_count = split(inherit_list, inherit_items, ",")
	for (i = 1; i <= inherit_count; i++) {
		if (inherit_items[i] == "")
			continue
		split(inherit_items[i], inherit_fields, "=")
		inherit_property = inherit_fields[1]
		inherit_value = inherit_fields[2]

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
	) || l_status=$?

	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to reconcile child property inheritance."
	fi

	{
		IFS= read -r g_zxfer_adjusted_set_list
		IFS= read -r g_zxfer_adjusted_inherit_list
	} <<EOF
$l_adjusted_lists
EOF

	printf '%s\n' "$g_zxfer_adjusted_set_list"
	printf '%s\n' "$g_zxfer_adjusted_inherit_list"
}

# Purpose: Apply the property changes through the controlled helper path owned
# by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to mutate live state.
#
# Apply pending property modifications/inheritance via set_runner/inherit_runner.
# $1: destination dataset
# $2: is_initial_source flag (1 means use initial_set_list)
# $3: initial source set list
# $4: child set list
# $5: inherit list
# $6: optional set runner function (defaults to zxfer_run_zfs_set_properties)
#     with signature: set_list, destination
# $7: optional inherit runner function (defaults to zxfer_run_zfs_inherit_property)
zxfer_apply_property_changes() {
	l_destination=$1
	l_is_initial_source=$2
	l_initial_set_list=$3
	l_child_set_list=$4
	l_inherit_list=$5
	l_set_runner=$6
	l_inherit_runner=$7

	if [ -z "$l_set_runner" ]; then
		l_set_runner="zxfer_run_zfs_set_properties"
	fi
	if [ -z "$l_inherit_runner" ]; then
		l_inherit_runner="zxfer_run_zfs_inherit_property"
	fi

	if [ "$l_is_initial_source" -eq 1 ]; then
		l_active_set_list=$l_initial_set_list
	else
		l_active_set_list=$l_child_set_list
	fi

	if [ "$l_active_set_list" != "" ] ||
		{ [ "$l_is_initial_source" -eq 0 ] && [ "$l_inherit_list" != "" ]; }; then
		zxfer_echov "Setting properties/sources on destination filesystem \"$l_destination\"."
		if [ -n "$l_active_set_list" ]; then
			l_display_set_list=$(zxfer_decode_serialized_property_list_for_display "$l_active_set_list")
			zxfer_echov "Property set list: $l_display_set_list"
		fi
		if [ -n "$l_inherit_list" ]; then
			l_display_inherit_list=$(zxfer_decode_serialized_property_list_for_display "$l_inherit_list")
			zxfer_echov "Property inherit list: $l_display_inherit_list"
		fi
	fi

	if [ "$l_active_set_list" != "" ]; then
		$l_set_runner "$l_active_set_list" "$l_destination"
	fi

	l_oldifs=$IFS
	if [ "$l_is_initial_source" -eq 0 ] && [ "$l_inherit_list" != "" ]; then
		IFS=","
		for ov_line in $l_inherit_list; do
			ov_property=$(echo "$ov_line" | cut -f1 -d=)
			IFS=$l_oldifs
			$l_inherit_runner "$ov_property" "$l_destination"
			IFS=","
		done
	fi

	IFS=$l_oldifs
}

################################################################################
# TOP-LEVEL PROPERTY TRANSFER
################################################################################

# Purpose: Drive the full per-dataset property reconciliation flow, including
# create-time property handling and optional backup capture.
# Usage: Called during property filtering, diffing, and apply from the
# replication loop once snapshot planning has identified a dataset that still
# needs property work.
#
# Transfers properties from any source to destination.
# Either creates the filesystem if it doesn't exist,
# or sets it after the fact.
# Also, checks to see if the override properties given as options are valid.
# Needs: $g_initial_source, $g_actual_dest, $g_recursive_dest_list
# $g_ensure_writable
# $2: set to 1 to skip -k backup capture during post-seed reconciliation
zxfer_transfer_properties() {
	zxfer_set_failure_stage "property transfer"
	zxfer_echoV "zxfer_transfer_properties: $1"
	zxfer_echoV "initial_source: $g_initial_source"
	zxfer_reset_property_reconcile_state

	l_source=$1
	l_skip_backup_capture=${2:-0}
	l_effective_readonly_properties=$(zxfer_get_effective_readonly_properties)

	if [ "$g_initial_source" = "$l_source" ]; then
		l_is_initial_source=1
	else
		l_is_initial_source=0
	fi

	l_collect_source_status=0
	zxfer_collect_source_props "$l_source" "$g_actual_dest" "$g_ensure_writable" "$g_LZFS" ||
		l_collect_source_status=$?
	if [ "$l_collect_source_status" -ne 0 ]; then
		zxfer_throw_error "${g_zxfer_source_pvs_raw:-Failed to retrieve source properties for [$l_source].}" "$l_collect_source_status"
	fi
	l_source_create_metadata_status=0
	l_source_create_metadata=$(zxfer_get_validated_source_dataset_create_metadata "$l_source") ||
		l_source_create_metadata_status=$?
	if [ "$l_source_create_metadata_status" -ne 0 ]; then
		zxfer_throw_error "$l_source_create_metadata" "$l_source_create_metadata_status"
	fi
	{
		IFS= read -r l_source_dstype
		IFS= read -r l_source_volsize
	} <<EOF
$l_source_create_metadata
EOF
	l_must_create_properties=$(zxfer_get_required_creation_properties_for_dataset_type "$l_source_dstype")

	if [ "$g_option_e_restore_property_mode" -eq 0 ]; then
		l_required_props_tmp_status=0
		zxfer_get_temp_file >/dev/null || l_required_props_tmp_status=$?
		if [ "$l_required_props_tmp_status" -ne 0 ]; then
			return "$l_required_props_tmp_status"
		fi
		l_required_props_tmp=$g_zxfer_temp_file_result
		l_required_props_status=0
		zxfer_ensure_required_properties_present "$l_source" "$g_zxfer_source_pvs_raw" "$g_LZFS" "$l_must_create_properties" source >"$l_required_props_tmp" ||
			l_required_props_status=$?
		if [ "$l_required_props_status" -ne 0 ]; then
			if zxfer_read_property_reconcile_stage_file "$l_required_props_tmp" >/dev/null; then
				:
			else
				l_read_status=$?
				zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
				return "$l_read_status"
			fi
			g_zxfer_source_pvs_raw=$g_zxfer_property_stage_file_read_result
			zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
			zxfer_throw_error "$g_zxfer_source_pvs_raw" "$l_required_props_status"
		fi
		if zxfer_read_property_reconcile_stage_file "$l_required_props_tmp" >/dev/null; then
			:
		else
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
			return "$l_read_status"
		fi
		g_zxfer_source_pvs_raw=$g_zxfer_property_stage_file_read_result
		l_required_props_status=0
		zxfer_ensure_required_properties_present "$l_source" "$g_zxfer_source_pvs_effective" "$g_LZFS" "$l_must_create_properties" source >"$l_required_props_tmp" ||
			l_required_props_status=$?
		if [ "$l_required_props_status" -ne 0 ]; then
			if zxfer_read_property_reconcile_stage_file "$l_required_props_tmp" >/dev/null; then
				:
			else
				l_read_status=$?
				zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
				return "$l_read_status"
			fi
			g_zxfer_source_pvs_effective=$g_zxfer_property_stage_file_read_result
			zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
			zxfer_throw_error "$g_zxfer_source_pvs_effective" "$l_required_props_status"
		fi
		if zxfer_read_property_reconcile_stage_file "$l_required_props_tmp" >/dev/null; then
			:
		else
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
			return "$l_read_status"
		fi
		g_zxfer_source_pvs_effective=$g_zxfer_property_stage_file_read_result
		zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"
	fi

	l_source_pvs=$g_zxfer_source_pvs_effective

	l_override_property_pv=$g_option_o_override_property

	if [ "$g_ensure_writable" -eq 1 ]; then
		l_override_property_pv=$(zxfer_force_readonly_off "$l_override_property_pv")
	fi

	if [ $l_is_initial_source -eq 1 ]; then
		zxfer_validate_override_properties "$l_override_property_pv" "$l_source_pvs"
	fi

	l_derive_override_status=0
	zxfer_derive_override_lists "$l_source_pvs" "$l_override_property_pv" "$g_option_P_transfer_property" "$l_source_dstype" >/dev/null ||
		l_derive_override_status=$?
	[ "$l_derive_override_status" -eq 0 ] || return "$l_derive_override_status"
	l_override_pvs=$g_zxfer_override_pvs_result
	l_creation_pvs=$g_zxfer_creation_pvs_result

	l_override_pvs=$(zxfer_sanitize_property_list "$l_override_pvs" "$l_effective_readonly_properties" "$g_option_I_ignore_properties")
	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ]; then
		zxfer_select_unsupported_properties_for_dataset_type "$l_source_dstype"
	else
		g_unsupported_properties=""
	fi
	l_override_pvs=$(zxfer_strip_unsupported_properties "$l_override_pvs" "$g_unsupported_properties")
	zxfer_echoV "zxfer_transfer_properties override_pvs: $l_override_pvs"

	l_dest_regex=$(printf '%s\n' "$g_actual_dest" | sed 's/[].[^$\\*]/\\&/g')
	l_dest_exist=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$l_dest_regex$")

	# Track when zxfer created the destination during this property pass so the
	# seed receive can safely use -F on an empty dataset.
	# shellcheck disable=SC2034
	g_dest_created_by_zxfer=0
	if zxfer_ensure_destination_exists "$l_dest_exist" "$l_is_initial_source" "$l_override_pvs" "$l_creation_pvs" "$l_source_dstype" "$l_source_volsize" "$g_actual_dest" "$l_effective_readonly_properties" ""; then
		# shellcheck disable=SC2034
		g_dest_created_by_zxfer=1
		zxfer_capture_backup_metadata_for_completed_transfer "$l_source" "$g_actual_dest" "$g_zxfer_source_pvs_raw" "$l_skip_backup_capture"
		return
	fi

	l_dest_pvs_tmp_status=0
	zxfer_get_temp_file >/dev/null || l_dest_pvs_tmp_status=$?
	if [ "$l_dest_pvs_tmp_status" -ne 0 ]; then
		return "$l_dest_pvs_tmp_status"
	fi
	l_dest_pvs_tmp=$g_zxfer_temp_file_result
	l_dest_pvs_status=0
	zxfer_collect_destination_props "$g_actual_dest" "$g_RZFS" >"$l_dest_pvs_tmp" ||
		l_dest_pvs_status=$?
	if [ "$l_dest_pvs_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
		zxfer_throw_error "Failed to retrieve destination properties for [$g_actual_dest]." "$l_dest_pvs_status"
	fi
	if zxfer_read_property_reconcile_stage_file "$l_dest_pvs_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
		return "$l_read_status"
	fi
	l_dest_pvs=$g_zxfer_property_stage_file_read_result
	l_dest_required_status=0
	zxfer_ensure_required_properties_present "$g_actual_dest" "$l_dest_pvs" "$g_RZFS" "$l_must_create_properties" destination >"$l_dest_pvs_tmp" ||
		l_dest_required_status=$?
	if [ "$l_dest_required_status" -ne 0 ]; then
		if zxfer_read_property_reconcile_stage_file "$l_dest_pvs_tmp" >/dev/null; then
			:
		else
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
			return "$l_read_status"
		fi
		l_dest_pvs=$g_zxfer_property_stage_file_read_result
		zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
		zxfer_throw_error "$l_dest_pvs" "$l_dest_required_status"
	fi
	if zxfer_read_property_reconcile_stage_file "$l_dest_pvs_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
		return "$l_read_status"
	fi
	l_dest_pvs=$g_zxfer_property_stage_file_read_result
	zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
	l_dest_pvs=$(zxfer_sanitize_property_list "$l_dest_pvs" "$l_effective_readonly_properties" "$g_option_I_ignore_properties")
	zxfer_echoV "zxfer_transfer_properties dest_pvs: $l_dest_pvs"

	l_diff_properties_tmp_status=0
	zxfer_get_temp_file >/dev/null || l_diff_properties_tmp_status=$?
	if [ "$l_diff_properties_tmp_status" -ne 0 ]; then
		return "$l_diff_properties_tmp_status"
	fi
	l_diff_properties_tmp=$g_zxfer_temp_file_result
	l_diff_properties_status=0
	zxfer_diff_properties "$l_override_pvs" "$l_dest_pvs" "$l_must_create_properties" >"$l_diff_properties_tmp" ||
		l_diff_properties_status=$?
	if [ "$l_diff_properties_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_diff_properties_tmp"
		zxfer_throw_error "Failed to calculate property reconciliation changes for destination [$g_actual_dest]." "$l_diff_properties_status"
	fi
	if zxfer_read_property_reconcile_stage_file "$l_diff_properties_tmp" >/dev/null; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_diff_properties_tmp"
		return "$l_read_status"
	fi
	{
		IFS= read -r l_ov_initsrc_set_list
		IFS= read -r l_ov_set_list
		IFS= read -r l_ov_inherit_list
	} <<EOF
$g_zxfer_property_stage_file_read_result
EOF
	zxfer_cleanup_runtime_artifact_path "$l_diff_properties_tmp"
	zxfer_echoV "zxfer_transfer_properties init_set: $l_ov_initsrc_set_list"
	zxfer_echoV "zxfer_transfer_properties child_set: $l_ov_set_list"
	zxfer_echoV "zxfer_transfer_properties inherit: $l_ov_inherit_list"

	if [ "$l_is_initial_source" -eq 0 ] &&
		{ [ "$l_ov_set_list" != "" ] || [ "$l_ov_inherit_list" != "" ]; }; then
		l_adjust_status=0
		zxfer_adjust_child_inherit_to_match_parent "$g_actual_dest" "$l_override_pvs" "$l_ov_set_list" "$l_ov_inherit_list" "$l_effective_readonly_properties" >/dev/null ||
			l_adjust_status=$?
		if [ "$l_adjust_status" -ne 0 ]; then
			zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$l_adjust_status"
		fi
		l_ov_set_list=$g_zxfer_adjusted_set_list
		l_ov_inherit_list=$g_zxfer_adjusted_inherit_list
		zxfer_echoV "zxfer_transfer_properties adjusted child_set: $l_ov_set_list"
		zxfer_echoV "zxfer_transfer_properties adjusted inherit: $l_ov_inherit_list"
	fi

	zxfer_apply_property_changes "$g_actual_dest" "$l_is_initial_source" "$l_ov_initsrc_set_list" "$l_ov_set_list" "$l_ov_inherit_list" "" ""
	zxfer_capture_backup_metadata_for_completed_transfer "$l_source" "$g_actual_dest" "$g_zxfer_source_pvs_raw" "$l_skip_backup_capture"
}
