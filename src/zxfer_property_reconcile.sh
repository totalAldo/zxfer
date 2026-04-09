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
# owns globals: run-wide property filter state plus per-call property reconciliation scratch/result globals such as g_unsupported_properties, g_zxfer_new_rmvs_pv, g_zxfer_new_rmv_pvs, g_zxfer_new_mc_pvs, g_zxfer_only_supported_properties, g_zxfer_adjusted_set_list, g_zxfer_adjusted_inherit_list, g_zxfer_source_pvs_raw, and g_zxfer_source_pvs_effective.
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

zxfer_get_base_readonly_properties() {
	printf '%s\n' "$ZXFER_BASE_READONLY_PROPERTIES"
}

zxfer_get_freebsd_readonly_properties() {
	printf '%s\n' "$ZXFER_FREEBSD_READONLY_PROPERTIES"
}

zxfer_get_solexp_readonly_properties() {
	printf '%s\n' "$ZXFER_SOLEXP_READONLY_PROPERTIES"
}

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

zxfer_reset_property_runtime_state() {
	g_unsupported_properties=""
}

zxfer_reset_property_reconcile_state() {
	g_zxfer_new_rmvs_pv=""
	g_zxfer_new_rmv_pvs=""
	g_zxfer_new_mc_pvs=""
	g_zxfer_only_supported_properties=""
	g_zxfer_adjusted_set_list=""
	g_zxfer_adjusted_inherit_list=""
	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
}

################################################################################
# PROPERTY FILTER / NORMALIZATION HELPERS
################################################################################

#
# Drop the source field from property=value=source entries.
# Result is stored in $g_zxfer_new_rmvs_pv as property=value CSV.
#
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

#
# Keep only the requested property=value=source entries.
# Used for the "must create" property set.
#
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

#
# Remove listed properties from property=value=source entries.
# Explicit override entries are preserved. Result is stored in
# $g_zxfer_new_rmv_pvs.
#
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

#
# Remove properties the destination cannot support from
# property=value=source entries. Result is stored in $g_zxfer_new_rmv_pvs.
#
zxfer_remove_unsupported_properties() {
	l_orig_set_list=$1 # the list of properties=values=sources,...
	l_filter_tmp=$(zxfer_get_temp_file)
	g_zxfer_only_supported_properties=""

	if ! "${g_cmd_awk:-awk}" -v input_list="$l_orig_set_list" \
		-v unsupported_list="${g_unsupported_properties:-}" \
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
		rm -f "$l_filter_tmp"
		zxfer_throw_error "Failed to filter unsupported destination properties."
	fi

	{
		IFS= read -r g_zxfer_only_supported_properties
		while IFS= read -r l_warning; do
			[ -n "$l_warning" ] || continue
			zxfer_warn_stderr "$l_warning"
		done
	} <"$l_filter_tmp"
	rm -f "$l_filter_tmp"
}

#
# Build and execute a "zfs create" command while safely passing property=value
# assignments as individual arguments, avoiding eval so property data cannot be
# treated as shell syntax.
# $1: "yes" to include -p (create parent datasets), anything else skips it
# $2: dataset type (volume/filesystem) to decide whether -V is required
# $3: volume size (only used when type=volume)
# $4: comma-separated property=value list (sources already removed)
# $5: destination dataset name
#
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

#
# Replace any readonly=on entries with readonly=off so zxfer can ensure the
# destination stays writable when --ensure-writable is enabled.
# $1: comma-separated property list
#
zxfer_force_readonly_off() {
	if [ -z "$1" ]; then
		printf '%s\n' ""
		return
	fi

	printf '%s\n' "$(echo "$1" | sed -e 's/readonly=on/readonly=off/g')"
}

#
# Collect the source property list and derive the effective list used for
# transfer. Results are stored in module variables:
#  g_zxfer_source_pvs_raw - normalized properties from the live source
#  g_zxfer_source_pvs_effective - properties after restore/writable handling
# $1: source dataset
# $2: destination dataset
# $3: ensure-writable flag (1 to force readonly=off)
# $4: zfs command used to inspect the source (defaults to $g_LZFS)
#
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
	l_source_props_tmp=$(zxfer_get_temp_file)
	if ! zxfer_get_normalized_dataset_properties "$l_source" "$l_zfs_cmd" source >"$l_source_props_tmp"; then
		g_zxfer_source_pvs_raw=$(cat "$l_source_props_tmp")
		rm -f "$l_source_props_tmp"
		printf '%s\n' "$g_zxfer_source_pvs_raw"
		return 1
	fi
	g_zxfer_source_pvs_raw=$(cat "$l_source_props_tmp")
	rm -f "$l_source_props_tmp"
	g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw

	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		g_zxfer_source_pvs_effective=$(zxfer_backup_metadata_extract_properties_for_dataset_pair \
			"$g_restored_backup_file_contents" "$l_source" "$l_destination")
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

#
# Validate that every override property supplied via -o exists on the source.
# $1: comma-separated override list (property=value)
# $2: comma-separated source property/value/source list
#
zxfer_validate_override_properties() {
	l_override_list=$1
	l_source_pvs=$2

	if [ -z "$l_override_list" ]; then
		return
	fi

	"${g_cmd_awk:-awk}" -v override_list="$l_override_list" -v source_pvs="$l_source_pvs" '
BEGIN {
	source_count = split(source_pvs, source_items, ",")
	for (i = 1; i <= source_count; i++) {
		if (source_items[i] == "")
			continue
		split(source_items[i], source_fields, "=")
		source_property[source_fields[1]] = 1
	}

	override_count = split(override_list, override_items, ",")
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		split(override_items[i], override_fields, "=")
		if (!(override_fields[1] in source_property)) {
			print override_fields[1]
			exit 1
		}
	}
}' >/dev/null
	l_status=$?

	if [ "$l_status" -eq 1 ]; then
		zxfer_throw_usage_error "Invalid option property - check -o list for syntax errors."
	elif [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to validate override properties."
	fi
}

#
# Build the override and creation property sets based on -P/-o arguments.
# Returns two newline-separated lines: override_pvs and creation_pvs.
# $1: source property/value/source list
# $2: -o override list (property=value)
# $3: $g_option_P_transfer_property flag
# $4: dataset type (filesystem/volume)
#
zxfer_derive_override_lists() {
	l_source_pvs=$1
	l_override_options=$2
	l_transfer_all_flag=$3
	l_source_dstype=$4

	# shellcheck disable=SC2016
	# awk program needs literal $-style fields; shell variables are passed with -v.
	l_derived_lists=$(
		"${g_cmd_awk:-awk}" \
			-v source_pvs="$l_source_pvs" \
			-v override_options="$l_override_options" \
			-v transfer_all_flag="$l_transfer_all_flag" \
			-v source_dstype="$l_source_dstype" '
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
BEGIN {
	override_count = split(override_options, override_items, ",")
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		override_separator = index(override_items[i], "=")
		if (override_separator == 0)
			continue
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
	)
	l_status=$?

	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to derive override property lists."
	fi

	printf '%s\n' "$l_derived_lists"
}

#
# Remove readonly/ignored properties from a list while preserving formatting.
# $1: comma-separated property list
# $2: readonly property list to remove
# $3: additional ignore list to remove
#
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

#
# Retrieve and validate the source dataset type plus any required creation
# metadata before planning destination creation or property diffs.
# Returns two newline-separated lines: dataset_type, volume_size.
# $1: source dataset
#
zxfer_get_validated_source_dataset_create_metadata() {
	l_source=$1
	l_source_volsize=""

	if ! l_source_dstype=$(zxfer_run_source_zfs_cmd get -Hpo value type "$l_source" 2>&1); then
		printf '%s\n' "Failed to retrieve source dataset type for [$l_source]: $l_source_dstype"
		return 1
	fi

	case "$l_source_dstype" in
	filesystem) ;;
	volume)
		if ! l_source_volsize=$(zxfer_run_source_zfs_cmd get -Hpo value volsize "$l_source" 2>&1); then
			printf '%s\n' "Failed to retrieve source zvol size for [$l_source]: $l_source_volsize"
			return 1
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

#
# Return the applicable creation-time properties for the source dataset type.
# Filesystems need these properties to be compared at creation time; volumes do
# not support them and should not probe them opportunistically.
# $1: dataset type (filesystem/volume)
#
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

#
# Drop properties unsupported on the destination.
# $1: comma-separated property list
# $2: unsupported property names
#
zxfer_strip_unsupported_properties() {
	l_input_list=$1
	l_unsupported_list=$2

	if [ -z "$l_unsupported_list" ] || [ -z "$l_input_list" ]; then
		printf '%s\n' "$l_input_list"
		return
	fi

	zxfer_remove_unsupported_properties "$l_input_list"
	printf '%s\n' "$g_zxfer_only_supported_properties"
}

#
# Calculate the list of source properties unsupported on the destination.
# The result is stored in g_unsupported_properties for the current run.
#
zxfer_calculate_unsupported_properties() {
	l_dest_pool_name=${g_destination%%/*}
	if ! l_dest_supported_properties=$(zxfer_run_destination_zfs_cmd get -Ho property all "$l_dest_pool_name" 2>&1); then
		zxfer_throw_error "Failed to retrieve destination supported property list for pool [$l_dest_pool_name]: $l_dest_supported_properties"
	fi

	l_source_pool_name=${g_initial_source%%/*}
	if ! l_source_supported_properties=$(zxfer_run_source_zfs_cmd get -Ho property all "$l_source_pool_name" 2>&1); then
		zxfer_throw_error "Failed to retrieve source supported property list for pool [$l_source_pool_name]: $l_source_supported_properties"
	fi

	g_unsupported_properties=""

	for s_p in $l_source_supported_properties; do
		l_found_supported_prop=0
		for d_p in $l_dest_supported_properties; do
			if [ "$s_p" = "$d_p" ]; then
				l_found_supported_prop=1
				break
			fi
		done
		if [ $l_found_supported_prop -eq 0 ]; then
			g_unsupported_properties="${g_unsupported_properties}${s_p},"
		fi
	done
	g_unsupported_properties=${g_unsupported_properties%,}
}

################################################################################
# DESTINATION CREATE / APPLY HELPERS
################################################################################

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
#
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
			if ! l_parent_exists=$(zxfer_exists_destination "$l_parent_dataset"); then
				zxfer_throw_error "$l_parent_exists"
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

	if ! $l_create_runner "$l_with_parents" "$l_source_dstype" "$l_source_volsize" "$l_property_list" "$l_destination"; then
		zxfer_throw_error "Error when creating destination filesystem."
	fi

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_note_destination_dataset_exists "$l_destination"
		zxfer_invalidate_destination_property_cache "$l_destination"
	fi

	return 0
}

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

zxfer_build_destination_zfs_property_command() {
	zxfer_build_destination_zfs_command "$@"
}

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

#
zxfer_run_zfs_set_assignments() {
	l_destination=$1
	shift

	[ "$#" -gt 0 ] || return 0

	l_display_cmd=$(zxfer_build_destination_zfs_property_command set "$@" "$l_destination")

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_echov "$l_display_cmd"
		if ! zxfer_run_destination_zfs_property_command set "$@" "$l_destination"; then
			zxfer_throw_error "Error when setting properties on destination filesystem."
		fi
		zxfer_invalidate_destination_property_cache "$l_destination"
	else
		echo "$l_display_cmd"
	fi
}

#
# Default runner for batched `zfs set`.
# $1: comma-separated property=value list
# $2: destination dataset
#
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

#
# Compatibility wrapper for code paths that still set one property at a time.
# $1: property name
# $2: property value
# $3: destination dataset
#
zxfer_run_zfs_set_property() {
	l_property=$1
	l_value=$2
	l_destination=$3

	zxfer_run_zfs_set_assignments "$l_destination" "$l_property=$l_value"
}

#
# Default runner for `zfs inherit`.
# $1: property name
# $2: destination dataset
#
zxfer_run_zfs_inherit_property() {
	l_property=$1
	l_destination=$2
	l_display_cmd=$(zxfer_build_destination_zfs_property_command inherit "$l_property" "$l_destination")

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_echov "$l_display_cmd"
		if ! zxfer_run_destination_zfs_property_command inherit "$l_property" "$l_destination"; then
			zxfer_throw_error "Error when inheriting properties on destination filesystem."
		fi
		zxfer_invalidate_destination_property_cache "$l_destination"
	else
		echo "$l_display_cmd"
	fi
}

#
# Compare override and destination property lists, enforcing "must create"
# restrictions and returning the required set/inherit operations.
# Returns three newline-separated lines: initial_set_list, set_list, inherit_list.
# $1: override property list
# $2: destination property list
# $3: must-create property names
#
zxfer_diff_properties() {
	l_override_pvs=$1
	l_dest_pvs=$2
	l_must_create_properties=$3

	l_diff_tmp=$(zxfer_get_temp_file)
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
		if (!(override_property[i] in dest_available))
			continue

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
		l_mismatch_property=$(sed -n '1p' "$l_diff_tmp")
		rm -f "$l_diff_tmp"
		zxfer_throw_error_with_usage "The property \"$l_mismatch_property\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
	elif [ "$l_status" -ne 0 ]; then
		rm -f "$l_diff_tmp"
		zxfer_throw_error "Failed to diff dataset properties."
	fi

	sed '1d' "$l_diff_tmp"
	rm -f "$l_diff_tmp"
}

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
#
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

	if ! l_parent_exists=$(zxfer_exists_destination "$l_parent_dataset"); then
		zxfer_throw_error "$l_parent_exists"
	fi
	if [ "$l_parent_exists" -eq 0 ]; then
		g_zxfer_adjusted_set_list=$l_set_list
		g_zxfer_adjusted_inherit_list=$l_inherit_list
		printf '%s\n' "$g_zxfer_adjusted_set_list"
		printf '%s\n' "$g_zxfer_adjusted_inherit_list"
		return
	fi

	l_parent_dest_tmp=$(zxfer_get_temp_file)
	if ! zxfer_collect_destination_props "$l_parent_dataset" "$g_RZFS" >"$l_parent_dest_tmp"; then
		rm -f "$l_parent_dest_tmp"
		return 1
	fi
	if [ "$g_zxfer_normalized_dataset_properties_cache_hit" -eq 0 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_parent_destination_property_reads
	fi
	l_parent_dest_pvs=$(cat "$l_parent_dest_tmp")
	rm -f "$l_parent_dest_tmp"
	l_parent_dest_pvs=$(zxfer_sanitize_property_list "$l_parent_dest_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")

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
	)
	l_status=$?

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
#
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

#
# Transfers properties from any source to destination.
# Either creates the filesystem if it doesn't exist,
# or sets it after the fact.
# Also, checks to see if the override properties given as options are valid.
# Needs: $g_initial_source, $g_actual_dest, $g_recursive_dest_list
# $g_ensure_writable
# $2: set to 1 to skip -k backup capture during post-seed reconciliation
#
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

	if ! zxfer_collect_source_props "$l_source" "$g_actual_dest" "$g_ensure_writable" "$g_LZFS"; then
		zxfer_throw_error "${g_zxfer_source_pvs_raw:-Failed to retrieve source properties for [$l_source].}"
	fi
	if ! l_source_create_metadata=$(zxfer_get_validated_source_dataset_create_metadata "$l_source"); then
		zxfer_throw_error "$l_source_create_metadata"
	fi
	{
		IFS= read -r l_source_dstype
		IFS= read -r l_source_volsize
	} <<EOF
$l_source_create_metadata
EOF
	l_must_create_properties=$(zxfer_get_required_creation_properties_for_dataset_type "$l_source_dstype")

	if [ "$g_option_e_restore_property_mode" -eq 0 ]; then
		l_required_props_tmp=$(zxfer_get_temp_file)
		if ! zxfer_ensure_required_properties_present "$l_source" "$g_zxfer_source_pvs_raw" "$g_LZFS" "$l_must_create_properties" source >"$l_required_props_tmp"; then
			g_zxfer_source_pvs_raw=$(cat "$l_required_props_tmp")
			rm -f "$l_required_props_tmp"
			zxfer_throw_error "$g_zxfer_source_pvs_raw"
		fi
		g_zxfer_source_pvs_raw=$(cat "$l_required_props_tmp")
		if ! zxfer_ensure_required_properties_present "$l_source" "$g_zxfer_source_pvs_effective" "$g_LZFS" "$l_must_create_properties" source >"$l_required_props_tmp"; then
			g_zxfer_source_pvs_effective=$(cat "$l_required_props_tmp")
			rm -f "$l_required_props_tmp"
			zxfer_throw_error "$g_zxfer_source_pvs_effective"
		fi
		g_zxfer_source_pvs_effective=$(cat "$l_required_props_tmp")
		rm -f "$l_required_props_tmp"
	fi

	l_source_pvs=$g_zxfer_source_pvs_effective

	# Persist raw source properties for -k backups in the parent shell (avoid
	# command-substitution subshells dropping global state).
	if [ "$g_option_k_backup_property_mode" -eq 1 ] && [ "$l_skip_backup_capture" -eq 0 ]; then
		zxfer_append_backup_metadata_record "$l_source" "$g_actual_dest" "$g_zxfer_source_pvs_raw"
	fi

	l_override_property_pv=$g_option_o_override_property

	if [ "$g_ensure_writable" -eq 1 ]; then
		l_override_property_pv=$(zxfer_force_readonly_off "$l_override_property_pv")
	fi

	if [ $l_is_initial_source -eq 1 ]; then
		zxfer_validate_override_properties "$l_override_property_pv" "$l_source_pvs"
	fi

	{
		IFS= read -r l_override_pvs
		IFS= read -r l_creation_pvs
	} <<EOF
$(zxfer_derive_override_lists "$l_source_pvs" "$l_override_property_pv" "$g_option_P_transfer_property" "$l_source_dstype")
EOF

	l_override_pvs=$(zxfer_sanitize_property_list "$l_override_pvs" "$l_effective_readonly_properties" "$g_option_I_ignore_properties")
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
		return
	fi

	l_dest_pvs_tmp=$(zxfer_get_temp_file)
	if ! zxfer_collect_destination_props "$g_actual_dest" "$g_RZFS" >"$l_dest_pvs_tmp"; then
		rm -f "$l_dest_pvs_tmp"
		zxfer_throw_error "Failed to retrieve destination properties for [$g_actual_dest]."
	fi
	l_dest_pvs=$(cat "$l_dest_pvs_tmp")
	if ! zxfer_ensure_required_properties_present "$g_actual_dest" "$l_dest_pvs" "$g_RZFS" "$l_must_create_properties" destination >"$l_dest_pvs_tmp"; then
		l_dest_pvs=$(cat "$l_dest_pvs_tmp")
		rm -f "$l_dest_pvs_tmp"
		zxfer_throw_error "$l_dest_pvs"
	fi
	l_dest_pvs=$(cat "$l_dest_pvs_tmp")
	rm -f "$l_dest_pvs_tmp"
	l_dest_pvs=$(zxfer_sanitize_property_list "$l_dest_pvs" "$l_effective_readonly_properties" "$g_option_I_ignore_properties")
	zxfer_echoV "zxfer_transfer_properties dest_pvs: $l_dest_pvs"

	l_diff_properties_tmp=$(zxfer_get_temp_file)
	zxfer_diff_properties "$l_override_pvs" "$l_dest_pvs" "$l_must_create_properties" >"$l_diff_properties_tmp"
	l_ov_initsrc_set_list=$(sed -n '1p' "$l_diff_properties_tmp")
	l_ov_set_list=$(sed -n '2p' "$l_diff_properties_tmp")
	l_ov_inherit_list=$(sed -n '3p' "$l_diff_properties_tmp")
	rm -f "$l_diff_properties_tmp"
	zxfer_echoV "zxfer_transfer_properties init_set: $l_ov_initsrc_set_list"
	zxfer_echoV "zxfer_transfer_properties child_set: $l_ov_set_list"
	zxfer_echoV "zxfer_transfer_properties inherit: $l_ov_inherit_list"

	if [ "$l_is_initial_source" -eq 0 ] &&
		{ [ "$l_ov_set_list" != "" ] || [ "$l_ov_inherit_list" != "" ]; }; then
		if ! zxfer_adjust_child_inherit_to_match_parent "$g_actual_dest" "$l_override_pvs" "$l_ov_set_list" "$l_ov_inherit_list" "$l_effective_readonly_properties" >/dev/null; then
			zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]."
		fi
		l_ov_set_list=$g_zxfer_adjusted_set_list
		l_ov_inherit_list=$g_zxfer_adjusted_inherit_list
		zxfer_echoV "zxfer_transfer_properties adjusted child_set: $l_ov_set_list"
		zxfer_echoV "zxfer_transfer_properties adjusted inherit: $l_ov_inherit_list"
	fi

	zxfer_apply_property_changes "$g_actual_dest" "$l_is_initial_source" "$l_ov_initsrc_set_list" "$l_ov_set_list" "$l_ov_inherit_list" "" ""
}
