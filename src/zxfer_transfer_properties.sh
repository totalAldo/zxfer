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

# for ShellCheck
if false; then
	# shellcheck source=src/zxfer_globals.sh
	. ./zxfer_globals.sh
fi

# module variables
m_new_rmvs_pv=""
m_new_rmv_pvs=""
m_new_mc_pvs=""
m_only_supported_properties=""

#
# Strips the sources from a list of properties=values=sources,
# e.g. output is properties=values,
# output is in $m_new_rmvs_pv
#
remove_sources() {
	m_new_rmvs_pv=""

	l_rmvs_list=$1

	for l_rmvs_line in $l_rmvs_list; do
		l_rmvs_property=$(echo "$l_rmvs_line" | cut -f1 -d=)
		l_rmvs_value=$(echo "$l_rmvs_line" | cut -f2 -d=)
		m_new_rmvs_pv="$m_new_rmvs_pv$l_rmvs_property=$l_rmvs_value,"
	done

	# remove trailing comma
	m_new_rmvs_pv=${m_new_rmvs_pv%,}
}

#
# Selects only the specified properties
# and values in the format property1=value1=source,...
# Used to select the "must create" properties
#
select_mc() {
	m_new_mc_pvs=""

	l_mc_list=$1          # target list of properties, values
	l_mc_property_list=$2 # list of properties to select

	# remove readonly properties from the override list
	for l_mc_line in $l_mc_list; do
		l_found_mc=0

		l_mc_property=$(echo "$l_mc_line" | cut -f1 -d=)
		l_mc_value=$(echo "$l_mc_line" | cut -f2 -d=)
		l_mc_source=$(echo "$l_mc_line" | cut -f3 -d=)

		# test for readonly properties
		for l_property in $l_mc_property_list; do
			if [ "$l_property" = "$l_mc_property" ]; then
				l_found_mc=1
				#since the property was matched let's not waste time looking for it again
				l_mc_property_list=$(echo "$l_mc_property_list" | tr -s "," "\n" |
					grep -v ^"$l_property"$ | tr -s "\n" ",")
				break
			fi
		done

		if [ $l_found_mc -eq 1 ]; then
			m_new_mc_pvs="$m_new_mc_pvs$l_mc_property=$l_mc_value=$l_mc_source,"
		fi
	done

	m_new_mc_pvs=${m_new_mc_pvs%,}
}

#
# Removes the readonly properties and values from a list of properties
# values and sources in the format property1=value1=source1,...
# output is in m_new_rmv_pvs
#
remove_properties() {
	m_new_rmv_pvs="" # global

	_rmv_list=$1    # the list of properties=values=sources,...
	_remove_list=$2 # list of properties to remove

	for rmv_line in $_rmv_list; do
		found_readonly=0
		rmv_property=$(echo "$rmv_line" | cut -f1 -d=)
		rmv_value=$(echo "$rmv_line" | cut -f2 -d=)
		rmv_source=$(echo "$rmv_line" | cut -f3 -d=)
		# test for readonly properties
		for property in $_remove_list; do
			if [ "$property" = "$rmv_property" ]; then
				if [ "$rmv_source" = "override" ]; then
					# The user has specifically required we set this property
					continue
				fi
				found_readonly=1
				#since the property was matched let's not waste time looking for it again
				_remove_list=$(echo "$_remove_list" | tr -s "," "\n" | grep -v ^"$property"$)
				_remove_list=$(echo "$_remove_list" | tr -s "\n" ",")
				break
			fi
		done
		if [ $found_readonly -eq 0 ]; then
			m_new_rmv_pvs="$m_new_rmv_pvs$rmv_property=$rmv_value=$rmv_source,"
		fi
	done

	m_new_rmv_pvs=${m_new_rmv_pvs%,}
}

#
# Removes the readonly properties and values from a list of properties
# values and sources in the format property1=value1=source1,...
# output is in m_new_rmv_pvs
#
remove_unsupported_properties() {
	l_orig_set_list=$1 # the list of properties=values=sources,...
	l_FUNCIFS=$IFS
	IFS=","

	m_only_supported_properties=""
	unsupported_properties=${unsupported_properties:-}
	for l_orig_line in $l_orig_set_list; do
		l_found_unsup=0
		l_orig_set_property=$(echo "$l_orig_line" | cut -f1 -d=)
		l_orig_set_value=$(echo "$l_orig_line" | cut -f2 -d=)
		l_orig_set_source=$(echo "$l_orig_line" | cut -f3 -d=)
		for l_property in ${unsupported_properties:-}; do
			if [ "$l_property" = "$l_orig_set_property" ]; then
				l_found_unsup=1
				break
			fi
		done
		if [ $l_found_unsup -eq 0 ]; then
			m_only_supported_properties="$m_only_supported_properties$l_orig_set_property=$l_orig_set_value=$l_orig_set_source,"
		else
			if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
				zxfer_warn_stderr "Destination does not support property ${l_orig_set_property}=${l_orig_set_value}"
			fi
		fi
	done
	m_only_supported_properties=${m_only_supported_properties%,}
	IFS=$l_FUNCIFS
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
run_zfs_create_with_properties() {
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
				set -- "$@" "-o" "$l_prop_value"
			fi
		done
		IFS=$l_cmd_ifs

		set -- "$@" "$l_destination"

		if [ "$g_option_n_dryrun" -eq 0 ]; then
			run_destination_zfs_cmd "$@"
		else
			zxfer_build_destination_zfs_command "$@"
		fi
	)
}

#
# Retrieve the normalized property/value/source list for a dataset while
# handling locales that require both machine (-Hp) and human (-H) parsing.
# $1: dataset to query
# $2: zfs command to execute (defaults to $g_LZFS)
# $3: optional lookup side label (source/destination/other) for profiling
#
get_normalized_dataset_properties() {
	l_dataset=$1
	l_zfs_cmd=$2
	l_lookup_side=${3:-other}

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
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

	l_machine_pvs=$(run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source all "$l_dataset" |
		tr "\t" "=" | tr "\n" ",")
	l_machine_pvs=${l_machine_pvs%,}
	l_human_pvs=$(run_zfs_cmd_for_spec "$l_zfs_cmd" get -Ho property,value,source all "$l_dataset" |
		tr "\t" "=" | tr "\n" ",")
	l_human_pvs=${l_human_pvs%,}
	resolve_human_vars "$l_machine_pvs" "$l_human_pvs"

	printf '%s\n' "$human_results"
}

#
# Replace any readonly=on entries with readonly=off so zxfer can ensure the
# destination stays writable when --ensure-writable is enabled.
# $1: comma-separated property list
#
force_readonly_off() {
	if [ -z "$1" ]; then
		printf '%s\n' ""
		return
	fi

	printf '%s\n' "$(echo "$1" | sed -e 's/readonly=on/readonly=off/g')"
}

#
# Collect the source property list and derive the effective list used for
# transfer. Results are stored in module variables:
#  m_source_pvs_raw - normalized properties from the live source
#  m_source_pvs_effective - properties after restore/writable handling
# $1: source dataset
# $2: reserved to keep caller signatures aligned
# $3: ensure-writable flag (1 to force readonly=off)
# $4: zfs command used to inspect the source (defaults to $g_LZFS)
#
collect_source_props() {
	l_source=$1
	l_ensure_writable=$3
	l_zfs_cmd=$4

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_LZFS
	fi

	m_source_pvs_raw=$(get_normalized_dataset_properties "$l_source" "$l_zfs_cmd" source)
	m_source_pvs_effective=$m_source_pvs_raw

	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		l_source_regex=$(printf '%s\n' "$l_source" | sed 's/[].[^$\\*]/\\&/g')
		m_source_pvs_effective=$(echo "$g_restored_backup_file_contents" |
			grep "^$l_source_regex," | sed -e 's/^[^,]*,[^,]*,//g')
		if [ "$m_source_pvs_effective" = "" ]; then
			# Fall back to legacy order if older backups stored dest,source,props.
			m_source_pvs_effective=$(echo "$g_restored_backup_file_contents" |
				grep "^[^,]*,$l_source_regex," | sed -e 's/^[^,]*,[^,]*,//g')
		fi
		if [ "$m_source_pvs_effective" = "" ]; then
			throw_usage_error "Can't find the properties for the filesystem $l_source"
		fi
	fi

	if [ "$l_ensure_writable" -eq 1 ]; then
		m_source_pvs_effective=$(force_readonly_off "$m_source_pvs_effective")
	fi
}

#
# Validate that every override property supplied via -o exists on the source.
# $1: comma-separated override list (property=value)
# $2: comma-separated source property/value/source list
#
validate_override_properties() {
	l_override_list=$1
	l_source_pvs=$2

	if [ -z "$l_override_list" ]; then
		return
	fi

	l_oldifs=$IFS
	IFS=","
	for op_line in $l_override_list; do
		l_found_property=0
		op_property=$(echo "$op_line" | cut -f1 -d=)
		for sp_line in $l_source_pvs; do
			sp_property=$(echo "$sp_line" | cut -f1 -d=)
			if [ "$op_property" = "$sp_property" ]; then
				l_found_property=1
				break
			fi
		done
		if [ $l_found_property -eq 0 ]; then
			IFS=$l_oldifs
			throw_usage_error "Invalid option property - check -o list for syntax errors."
		fi
	done
	IFS=$l_oldifs
}

#
# Build the override and creation property sets based on -P/-o arguments.
# Returns two newline-separated lines: override_pvs and creation_pvs.
# $1: source property/value/source list
# $2: -o override list (property=value)
# $3: $g_option_P_transfer_property flag
# $4: dataset type (filesystem/volume)
#
derive_override_lists() {
	l_source_pvs=$1
	l_override_options=$2
	l_transfer_all_flag=$3
	l_source_dstype=$4

	override_pvs=""
	creation_pvs=""

	l_oldifs=$IFS
	IFS=","

	if [ "$l_transfer_all_flag" -eq 0 ]; then
		for op_line in $l_override_options; do
			op_property=$(echo "$op_line" | cut -f1 -d=)
			op_value=$(echo "$op_line" | cut -f2 -d=)
			override_pvs="$override_pvs$op_property=$op_value=override,"
		done
	else
		for sp_line in $l_source_pvs; do
			override_property=$(echo "$sp_line" | cut -f1 -d=)
			override_value=$(echo "$sp_line" | cut -f2 -d=)
			override_source=$(echo "$sp_line" | cut -f3 -d=)
			creation_property=$override_property
			creation_value=$override_value
			creation_source=$override_source
			for op_line in $l_override_options; do
				op_property=$(echo "$op_line" | cut -f1 -d=)
				op_value=$(echo "$op_line" | cut -f2 -d=)
				if [ "$op_property" = "$override_property" ]; then
					override_property=$op_property
					override_value=$op_value
					override_source="override"
					creation_property="NULL"
					break
				fi
			done
			override_pvs="$override_pvs$override_property=$override_value=$override_source,"
			if [ "$creation_property" != "NULL" ] && [ "$creation_source" = "local" ]; then
				creation_pvs="$creation_pvs$creation_property=$creation_value=$creation_source,"
			elif [ "$l_source_dstype" = "volume" ] && [ "$creation_property" = "refreservation" ]; then
				creation_pvs="$creation_pvs$creation_property=$creation_value=$creation_source,"
			fi
		done
	fi

	IFS=$l_oldifs

	override_pvs=${override_pvs%,}
	creation_pvs=${creation_pvs%,}

	printf '%s\n' "$override_pvs"
	printf '%s\n' "$creation_pvs"
}

#
# Remove readonly/ignored properties from a list while preserving formatting.
# $1: comma-separated property list
# $2: readonly property list to remove
# $3: additional ignore list to remove
#
sanitize_property_list() {
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
		remove_properties "$l_filtered_list" "$l_remove_list"
		l_filtered_list="$m_new_rmv_pvs"
	fi

	if [ -n "$l_ignore_list" ]; then
		remove_properties "$l_filtered_list" "$l_ignore_list"
		l_filtered_list="$m_new_rmv_pvs"
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
ensure_required_properties_present() {
	l_dataset=$1
	l_property_list=$2
	l_zfs_cmd=$3
	l_required_properties=$4

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

		zxfer_profile_increment_counter g_zxfer_profile_required_property_backfill_gets
		if l_explicit_probe_output=$(run_zfs_cmd_for_spec "$l_zfs_cmd" get -Hpo property,value,source "$l_required_property" "$l_dataset" 2>&1); then
			l_explicit_property=$(printf '%s\n' "$l_explicit_probe_output" | sed -n '1p' | tr '\t' '=')
			case "$l_explicit_property" in
			"$l_required_property"=*=*) ;;
			*)
				IFS=$l_oldifs
				printf '%s\n' "Failed to parse required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
				return 1
				;;
			esac
		else
			case "$l_explicit_probe_output" in
			*"does not apply"* | *"invalid property"* | *"no such property"* | *"not supported"*)
				continue
				;;
			*)
				IFS=$l_oldifs
				printf '%s\n' "Failed to retrieve required creation-time property [$l_required_property] for dataset [$l_dataset]: $l_explicit_probe_output"
				return 1
				;;
			esac
			continue
		fi

		if [ -n "$l_result" ]; then
			l_result="$l_result,$l_explicit_property"
		else
			l_result=$l_explicit_property
		fi
	done
	IFS=$l_oldifs

	printf '%s\n' "$l_result"
}

#
# Retrieve and validate the source dataset type plus any required creation
# metadata before planning destination creation or property diffs.
# Returns two newline-separated lines: dataset_type, volume_size.
# $1: source dataset
#
get_validated_source_dataset_create_metadata() {
	l_source=$1
	l_source_volsize=""

	if ! l_source_dstype=$(run_source_zfs_cmd get -Hpo value type "$l_source" 2>&1); then
		printf '%s\n' "Failed to retrieve source dataset type for [$l_source]: $l_source_dstype"
		return 1
	fi

	case "$l_source_dstype" in
	filesystem) ;;
	volume)
		if ! l_source_volsize=$(run_source_zfs_cmd get -Hpo value volsize "$l_source" 2>&1); then
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
get_required_creation_properties_for_dataset_type() {
	l_dataset_type=$1

	case "$l_dataset_type" in
	volume)
		printf '\n'
		;;
	*)
		printf '%s\n' "casesensitivity,normalization,jailed,utf8only"
		;;
	esac
}

#
# Drop properties unsupported on the destination.
# $1: comma-separated property list
# $2: unsupported property names
#
strip_unsupported_properties() {
	l_input_list=$1
	l_unsupported_list=$2

	if [ -z "$l_unsupported_list" ] || [ -z "$l_input_list" ]; then
		printf '%s\n' "$l_input_list"
		return
	fi

	remove_unsupported_properties "$l_input_list"
	printf '%s\n' "$m_only_supported_properties"
}

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
# $9: optional runner for zfs create (defaults to run_zfs_create_with_properties)
#
ensure_destination_exists() {
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
		l_create_runner="run_zfs_create_with_properties"
	fi

	echov "Creating destination filesystem \"$l_destination\" with specified properties."

	l_oldifs=$IFS
	IFS=","
	if [ "$l_is_initial_source" -eq 1 ]; then
		remove_sources "$l_override_pvs"
		l_property_list="$m_new_rmvs_pv"
		l_with_parents="no"
		l_parent_dataset=${l_destination%/*}
		if [ "$l_parent_dataset" != "$l_destination" ]; then
			if ! l_parent_exists=$(exists_destination "$l_parent_dataset"); then
				throw_error "$l_parent_exists"
			fi
			if [ "$l_parent_exists" -eq 0 ]; then
				l_with_parents="yes"
			fi
		fi
	else
		l_filtered_creation=$(sanitize_property_list "$l_creation_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")
		remove_sources "$l_filtered_creation"
		l_property_list="$m_new_rmvs_pv"
		l_with_parents="yes"
	fi
	IFS=$l_oldifs

	if ! $l_create_runner "$l_with_parents" "$l_source_dstype" "$l_source_volsize" "$l_property_list" "$l_destination"; then
		throw_error "Error when creating destination filesystem."
	fi

	return 0
}

#
# Collect destination properties via the remote/local zfs command.
# $1: dataset name
# $2: command used to query properties (defaults to $g_RZFS)
#
collect_destination_props() {
	l_dataset=$1
	l_zfs_cmd=$2

	if [ -z "$l_zfs_cmd" ]; then
		l_zfs_cmd=$g_RZFS
	fi

	get_normalized_dataset_properties "$l_dataset" "$l_zfs_cmd" destination
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
	l_remote_cmd=$(quote_token_stream "$l_remote_tokens")
	build_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd"
}

zxfer_build_destination_zfs_property_command() {
	zxfer_build_destination_zfs_command "$@"
}

zxfer_run_destination_zfs_property_command() {
	l_subcommand=$1
	shift

	if [ "$g_option_T_target_host" = "" ]; then
		run_destination_zfs_cmd "$l_subcommand" "$@"
		return
	fi

	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}
	l_remote_tokens=$(printf '%s\n%s' "$l_target_zfs_cmd" "$l_subcommand")
	for l_arg in "$@"; do
		l_remote_tokens=$(printf '%s\n%s' "$l_remote_tokens" "$l_arg")
	done
	l_remote_cmd=$(quote_token_stream "$l_remote_tokens")
	invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd" destination
}

#
# Default runner for `zfs set`, allowing unit tests to override this behavior.
# $1: property name
# $2: property value
# $3: destination dataset
#
zxfer_run_zfs_set_property() {
	l_property=$1
	l_value=$2
	l_destination=$3
	l_assignment=$l_property=$l_value
	l_display_cmd=$(zxfer_build_destination_zfs_property_command set "$l_assignment" "$l_destination")

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		echov "$l_display_cmd"
		if ! zxfer_run_destination_zfs_property_command set "$l_assignment" "$l_destination"; then
			throw_error "Error when setting properties on destination filesystem."
		fi
	else
		echo "$l_display_cmd"
	fi
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
		echov "$l_display_cmd"
		if ! zxfer_run_destination_zfs_property_command inherit "$l_property" "$l_destination"; then
			throw_error "Error when inheriting properties on destination filesystem."
		fi
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
diff_properties() {
	l_override_pvs=$1
	l_dest_pvs=$2
	l_must_create_properties=$3

	l_oldifs=$IFS
	IFS=","

	select_mc "$l_override_pvs" "$l_must_create_properties"
	l_mc_override_pvs="$m_new_mc_pvs"

	select_mc "$l_dest_pvs" "$l_must_create_properties"
	l_mc_dest_pvs="$m_new_mc_pvs"

	for ov_line in $l_mc_override_pvs; do
		ov_property=$(echo "$ov_line" | cut -f1 -d=)
		ov_value=$(echo "$ov_line" | cut -f2 -d=)
		for dest_line in $l_mc_dest_pvs; do
			dest_property=$(echo "$dest_line" | cut -f1 -d=)
			dest_value=$(echo "$dest_line" | cut -f2 -d=)
			if [ "$ov_property" = "$dest_property" ] && [ "$ov_value" != "$dest_value" ]; then
				throw_error_with_usage "The property \"$dest_property\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
			fi
		done
	done

	remove_properties "$l_override_pvs" "$l_must_create_properties"
	l_filtered_override="$m_new_rmv_pvs"

	remove_properties "$l_dest_pvs" "$l_must_create_properties"
	l_filtered_dest="$m_new_rmv_pvs"

	ov_initsrc_set_list=""
	ov_set_list=""
	ov_inherit_list=""

	for ov_line in $l_filtered_override; do
		ov_property=$(echo "$ov_line" | cut -f1 -d=)
		ov_value=$(echo "$ov_line" | cut -f2 -d=)
		ov_source=$(echo "$ov_line" | cut -f3 -d=)
		for dest_line in $l_filtered_dest; do
			dest_property=$(echo "$dest_line" | cut -f1 -d=)
			dest_value=$(echo "$dest_line" | cut -f2 -d=)
			dest_source=$(echo "$dest_line" | cut -f3 -d=)
			if [ "$ov_property" = "$dest_property" ]; then
				if [ "$dest_value" != "$ov_value" ] || [ "$dest_source" != "local" ]; then
					ov_initsrc_set_list="$ov_initsrc_set_list$ov_property=$ov_value,"
				fi

				if [ "$ov_value" != "$dest_value" ]; then
					if [ "$ov_source" = "local" ] || [ "$ov_source" = "override" ]; then
						ov_set_list="$ov_set_list$ov_property=$ov_value,"
					else
						ov_inherit_list="$ov_inherit_list$ov_property=$ov_value,"
					fi
				elif { [ "$ov_source" = "local" ] || [ "$ov_source" = "override" ]; } &&
					[ "$dest_source" != "local" ]; then
					ov_set_list="$ov_set_list$ov_property=$ov_value,"
				elif [ "$ov_source" != "local" ] && [ "$ov_source" != "override" ] &&
					[ "$dest_source" = "local" ]; then
					ov_inherit_list="$ov_inherit_list$ov_property=$ov_value,"
				fi

				l_filtered_dest=$(echo "$l_filtered_dest" | tr -s "," "\n" |
					grep -v ^"$dest_line"$ | tr -s "\n" ",")
				break
			fi
		done
	done

	IFS=$l_oldifs

	printf '%s\n' "${ov_initsrc_set_list%,}"
	printf '%s\n' "${ov_set_list%,}"
	printf '%s\n' "${ov_inherit_list%,}"
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
adjust_child_inherit_to_match_parent() {
	l_destination=$1
	l_override_pvs=$2
	l_set_list=$3
	l_inherit_list=$4
	l_readonly_properties=$5

	if [ -z "$l_inherit_list" ]; then
		printf '%s\n' "$l_set_list"
		printf '%s\n' ""
		return
	fi

	l_parent_dataset=${l_destination%/*}
	if [ "$l_parent_dataset" = "$l_destination" ]; then
		printf '%s\n' "$l_set_list"
		printf '%s\n' "$l_inherit_list"
		return
	fi

	if ! l_parent_exists=$(exists_destination "$l_parent_dataset"); then
		throw_error "$l_parent_exists"
	fi
	if [ "$l_parent_exists" -eq 0 ]; then
		printf '%s\n' "$l_set_list"
		printf '%s\n' "$l_inherit_list"
		return
	fi

	zxfer_profile_increment_counter g_zxfer_profile_parent_destination_property_reads
	l_parent_dest_pvs=$(collect_destination_props "$l_parent_dataset" "$g_RZFS")
	l_parent_dest_pvs=$(sanitize_property_list "$l_parent_dest_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")

	l_new_set_list=$l_set_list
	l_new_inherit_list=""
	l_oldifs=$IFS
	IFS=","
	for l_inherit_line in $l_inherit_list; do
		[ -n "$l_inherit_line" ] || continue
		l_property=$(echo "$l_inherit_line" | cut -f1 -d=)
		l_desired_value=$(echo "$l_inherit_line" | cut -f2 -d=)
		l_parent_value=""

		for l_parent_line in $l_parent_dest_pvs; do
			[ -n "$l_parent_line" ] || continue
			l_parent_property=$(echo "$l_parent_line" | cut -f1 -d=)
			if [ "$l_parent_property" = "$l_property" ]; then
				l_parent_value=$(echo "$l_parent_line" | cut -f2 -d=)
				break
			fi
		done

		if [ "$l_parent_value" = "$l_desired_value" ]; then
			l_new_inherit_list="$l_new_inherit_list$l_inherit_line,"
		else
			if [ -n "$l_new_set_list" ]; then
				l_new_set_list="$l_new_set_list,$l_property=$l_desired_value"
			else
				l_new_set_list="$l_property=$l_desired_value"
			fi
		fi
	done
	IFS=$l_oldifs

	printf '%s\n' "$l_new_set_list"
	printf '%s\n' "${l_new_inherit_list%,}"
}

#
# Apply pending property modifications/inheritance via set_runner/inherit_runner.
# $1: destination dataset
# $2: is_initial_source flag (1 means use initial_set_list)
# $3: initial source set list
# $4: child set list
# $5: inherit list
# $6: optional set runner function (defaults to zxfer_run_zfs_set_property)
# $7: optional inherit runner function (defaults to zxfer_run_zfs_inherit_property)
#
apply_property_changes() {
	l_destination=$1
	l_is_initial_source=$2
	l_initial_set_list=$3
	l_child_set_list=$4
	l_inherit_list=$5
	l_set_runner=$6
	l_inherit_runner=$7

	if [ -z "$l_set_runner" ]; then
		l_set_runner="zxfer_run_zfs_set_property"
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
		echov "Setting properties/sources on destination filesystem \"$l_destination\"."
		[ -n "$l_active_set_list" ] && echov "Property set list: $l_active_set_list"
		[ -n "$l_inherit_list" ] && echov "Property inherit list: $l_inherit_list"
	fi

	l_oldifs=$IFS
	if [ "$l_active_set_list" != "" ]; then
		IFS=","
		for ov_line in $l_active_set_list; do
			ov_property=$(echo "$ov_line" | cut -f1 -d=)
			ov_value=$(echo "$ov_line" | cut -f2 -d=)
			IFS=$l_oldifs
			$l_set_runner "$ov_property" "$ov_value" "$l_destination"
			IFS=","
		done
	fi

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

#
# Transfers properties from any source to destination.
# Either creates the filesystem if it doesn't exist,
# or sets it after the fact.
# Also, checks to see if the override properties given as options are valid.
# Needs: $initial_source, $g_actual_dest, $g_recursive_dest_list
# $g_ensure_writable
# $2: set to 1 to skip -k backup capture during post-seed reconciliation
#
transfer_properties() {
	zxfer_set_failure_stage "property transfer"
	echoV "transfer_properties: $1"
	echoV "initial_source: $initial_source"

	l_source=$1
	l_skip_backup_capture=${2:-0}
	l_effective_readonly_properties=$g_readonly_properties

	if [ "$initial_source" = "$l_source" ]; then
		l_is_initial_source=1
	else
		l_is_initial_source=0
	fi

	collect_source_props "$l_source" "$g_actual_dest" "$g_ensure_writable" "$g_LZFS"
	if ! l_source_create_metadata=$(get_validated_source_dataset_create_metadata "$l_source"); then
		throw_error "$l_source_create_metadata"
	fi
	{
		IFS= read -r l_source_dstype
		IFS= read -r l_source_volsize
	} <<EOF
$l_source_create_metadata
EOF
	must_create_properties=$(get_required_creation_properties_for_dataset_type "$l_source_dstype")

	if [ "$g_option_e_restore_property_mode" -eq 0 ]; then
		if ! m_source_pvs_raw=$(ensure_required_properties_present "$l_source" "$m_source_pvs_raw" "$g_LZFS" "$must_create_properties"); then
			throw_error "$m_source_pvs_raw"
		fi
		if ! m_source_pvs_effective=$(ensure_required_properties_present "$l_source" "$m_source_pvs_effective" "$g_LZFS" "$must_create_properties"); then
			throw_error "$m_source_pvs_effective"
		fi
	fi

	l_source_pvs=$m_source_pvs_effective

	# Persist raw source properties for -k backups in the parent shell (avoid
	# command-substitution subshells dropping global state).
	if [ "$g_option_k_backup_property_mode" -eq 1 ] && [ "$l_skip_backup_capture" -eq 0 ]; then
		g_backup_file_contents="$g_backup_file_contents;\
$l_source,$g_actual_dest,$m_source_pvs_raw"
	fi

	g_option_o_override_property_pv=$g_option_o_override_property

	if [ "$g_ensure_writable" -eq 1 ]; then
		g_option_o_override_property_pv=$(force_readonly_off "$g_option_o_override_property_pv")
	fi

	if [ $l_is_initial_source -eq 1 ]; then
		validate_override_properties "$g_option_o_override_property_pv" "$l_source_pvs"
	fi

	if [ "$g_destination_operating_system" = "FreeBSD" ] && [ -n "$g_fbsd_readonly_properties" ]; then
		if [ -n "$l_effective_readonly_properties" ]; then
			l_effective_readonly_properties="$l_effective_readonly_properties,$g_fbsd_readonly_properties"
		else
			l_effective_readonly_properties=$g_fbsd_readonly_properties
		fi
	fi
	if [ "$g_destination_operating_system" = "SunOS" ] &&
		[ "$g_source_operating_system" = "FreeBSD" ] &&
		[ -n "$g_solexp_readonly_properties" ]; then
		if [ -n "$l_effective_readonly_properties" ]; then
			l_effective_readonly_properties="$l_effective_readonly_properties,$g_solexp_readonly_properties"
		else
			l_effective_readonly_properties=$g_solexp_readonly_properties
		fi
	fi

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$(derive_override_lists "$l_source_pvs" "$g_option_o_override_property_pv" "$g_option_P_transfer_property" "$l_source_dstype")
EOF

	override_pvs=$(sanitize_property_list "$override_pvs" "$l_effective_readonly_properties" "$g_option_I_ignore_properties")
	override_pvs=$(strip_unsupported_properties "$override_pvs" "$unsupported_properties")
	echoV "transfer_properties override_pvs: $override_pvs"

	dest_regex=$(printf '%s\n' "$g_actual_dest" | sed 's/[].[^$\\*]/\\&/g')
	dest_exist=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$dest_regex$")

	# Track when zxfer created the destination during this property pass so the
	# seed receive can safely use -F on an empty dataset.
	# shellcheck disable=SC2034
	g_dest_created_by_zxfer=0
	if ensure_destination_exists "$dest_exist" "$l_is_initial_source" "$override_pvs" "$creation_pvs" "$l_source_dstype" "$l_source_volsize" "$g_actual_dest" "$l_effective_readonly_properties" ""; then
		# shellcheck disable=SC2034
		g_dest_created_by_zxfer=1
		return
	fi

	dest_pvs=$(collect_destination_props "$g_actual_dest" "$g_RZFS")
	if ! dest_pvs=$(ensure_required_properties_present "$g_actual_dest" "$dest_pvs" "$g_RZFS" "$must_create_properties"); then
		throw_error "$dest_pvs"
	fi
	dest_pvs=$(sanitize_property_list "$dest_pvs" "$l_effective_readonly_properties" "$g_option_I_ignore_properties")
	echoV "transfer_properties dest_pvs: $dest_pvs"

	l_diff_properties_tmp=$(get_temp_file)
	diff_properties "$override_pvs" "$dest_pvs" "$must_create_properties" >"$l_diff_properties_tmp"
	ov_initsrc_set_list=$(sed -n '1p' "$l_diff_properties_tmp")
	ov_set_list=$(sed -n '2p' "$l_diff_properties_tmp")
	ov_inherit_list=$(sed -n '3p' "$l_diff_properties_tmp")
	rm -f "$l_diff_properties_tmp"
	echoV "transfer_properties init_set: $ov_initsrc_set_list"
	echoV "transfer_properties child_set: $ov_set_list"
	echoV "transfer_properties inherit: $ov_inherit_list"

	if [ "$l_is_initial_source" -eq 0 ] && [ "$ov_inherit_list" != "" ]; then
		{
			IFS= read -r ov_set_list
			IFS= read -r ov_inherit_list
		} <<EOF
$(adjust_child_inherit_to_match_parent "$g_actual_dest" "$override_pvs" "$ov_set_list" "$ov_inherit_list" "$l_effective_readonly_properties")
EOF
		echoV "transfer_properties adjusted child_set: $ov_set_list"
		echoV "transfer_properties adjusted inherit: $ov_inherit_list"
	fi

	apply_property_changes "$g_actual_dest" "$l_is_initial_source" "$ov_initsrc_set_list" "$ov_set_list" "$ov_inherit_list" "" ""
}
