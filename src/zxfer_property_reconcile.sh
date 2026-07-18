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
# PROPERTY CREATE / DIFF / APPLY / RECONCILIATION HELPERS
################################################################################

# Module contract:
# owns globals: immutable diff/inheritance AWK programs; per-call plans and
#   results use lifecycle channels owned by zxfer_property_state.sh.
# reads globals: current source/destination context, property CLI options,
#   backup/restore state, and policy/state results.
# mutates caches: destination property state only through the state module's
#   targeted invalidation helpers after successful mutations.
# returns via stdout: destination commands, diff plans, and checked
#   reconciliation results; may create datasets or apply property changes.

# Purpose: Run the ZFS create with properties through the controlled execution
# path owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
#
# Build and execute a "zfs create" command while safely passing property=value
# assignments as individual arguments, avoiding eval so property data cannot be
# treated as shell syntax. Parent hierarchy creation with -p must be issued
# without properties because OpenZFS ignores -o arguments on that path.
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

	if [ "$l_with_parents" = "yes" ] && zxfer_property_list_has_entries "$l_property_list"; then
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

		l_create_property_remaining=$l_property_list
		while [ -n "$l_create_property_remaining" ]; do
			case "$l_create_property_remaining" in
			*,*)
				l_prop_value=${l_create_property_remaining%%,*}
				l_create_property_remaining=${l_create_property_remaining#*,}
				;;
			*)
				l_prop_value=$l_create_property_remaining
				l_create_property_remaining=""
				;;
			esac
			if [ "$l_prop_value" != "" ]; then
				zxfer_decode_serialized_property_assignment "$l_prop_value" >/dev/null ||
					exit "$?"
				set -- "$@" "-o" "$g_zxfer_decoded_property_assignment_result"
			fi
		done

		set -- "$@" "$l_destination"

		if [ "$g_option_n_dryrun" -eq 0 ]; then
			zxfer_run_destination_zfs_cmd "$@"
		else
			zxfer_build_destination_zfs_command "$@"
		fi
	)
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

	zxfer_publish_source_property_results "" ""
	zxfer_create_property_reconcile_stage_file ||
		return "$?"
	l_source_props_tmp=$g_zxfer_property_reconcile_stage_file_result
	l_source_props_status=0
	zxfer_get_normalized_dataset_properties "$l_source" "$l_zfs_cmd" source >"$l_source_props_tmp" ||
		l_source_props_status=$?
	if [ "$l_source_props_status" -ne 0 ]; then
		zxfer_read_property_reconcile_stage_file "$l_source_props_tmp" >/dev/null || {
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
			return "$l_read_status"
		}
		zxfer_publish_source_property_results "$g_zxfer_property_stage_file_read_result" ""
		zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
		printf '%s\n' "$g_zxfer_source_pvs_raw"
		return "$l_source_props_status"
	fi
	zxfer_read_property_reconcile_stage_file "$l_source_props_tmp" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"
		return "$l_read_status"
	}
	zxfer_publish_source_property_results "$g_zxfer_property_stage_file_read_result" \
		"$g_zxfer_property_stage_file_read_result"
	zxfer_cleanup_runtime_artifact_path "$l_source_props_tmp"

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
				zxfer_throw_usage_error "Restored properties for the filesystem $l_source and destination $l_destination do not declare supported zxfer backup metadata format version #format_version:$ZXFER_BACKUP_METADATA_FORMAT_VERSION"
				;;
			*)
				zxfer_throw_usage_error "Failed to validate the restored backup metadata for the filesystem $l_source and destination $l_destination"
				;;
			esac
		fi
		if [ -z "$g_restored_backup_file_contents" ]; then
			zxfer_throw_usage_error "Can't find the properties for the filesystem $l_source and destination $l_destination"
		fi
		l_restore_status=0
		l_source_pvs_effective=$(zxfer_backup_metadata_extract_properties_for_dataset_pair \
			"$g_restored_backup_file_contents" "$l_source" "$l_destination") ||
			l_restore_status=$?
		zxfer_publish_source_property_results "$g_zxfer_source_pvs_raw" "$l_source_pvs_effective"
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
		l_source_pvs_effective=$(zxfer_force_readonly_off "$g_zxfer_source_pvs_effective")
		zxfer_publish_source_property_results "$g_zxfer_source_pvs_raw" "$l_source_pvs_effective"
	fi
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

	l_parent_exists=""
	l_parent_dataset=${l_destination%/*}
	if [ "$l_parent_dataset" != "$l_destination" ]; then
		l_parent_exists=$(zxfer_exists_destination "$l_parent_dataset") ||
			zxfer_throw_error "$l_parent_exists" "$?"
	fi

	if [ "$l_is_initial_source" -eq 1 ]; then
		zxfer_remove_sources "$l_override_pvs"
		l_property_list="$g_zxfer_new_rmvs_pv"
	else
		l_filtered_creation=$(zxfer_sanitize_property_list "$l_creation_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")
		l_child_creation_has_override_sources=1
		l_creation_override_remaining=$l_filtered_creation
		while [ -n "$l_creation_override_remaining" ]; do
			case "$l_creation_override_remaining" in
			*,*)
				l_property_entry=${l_creation_override_remaining%%,*}
				l_creation_override_remaining=${l_creation_override_remaining#*,}
				;;
			*)
				l_property_entry=$l_creation_override_remaining
				l_creation_override_remaining=""
				;;
			esac
			case "$l_property_entry" in
			*=override)
				l_child_creation_has_override_sources=0
				break
				;;
			esac
		done
		if [ "$l_parent_exists" = "1" ] && [ "$l_child_creation_has_override_sources" -eq 0 ]; then
			zxfer_create_property_reconcile_stage_file ||
				zxfer_throw_error "Failed to allocate parent destination property staging for child override inheritance." "$?"
			l_parent_dest_tmp=$g_zxfer_property_reconcile_stage_file_result
			zxfer_collect_destination_props "$l_parent_dataset" "$g_RZFS" >"$l_parent_dest_tmp" || {
				l_parent_dest_status=$?
				zxfer_cleanup_runtime_artifact_path "$l_parent_dest_tmp"
				zxfer_throw_error "Failed to retrieve parent destination properties for [$l_parent_dataset]." "$l_parent_dest_status"
			}
			zxfer_read_property_reconcile_stage_file "$l_parent_dest_tmp" >/dev/null || {
				l_read_status=$?
				zxfer_cleanup_runtime_artifact_path "$l_parent_dest_tmp"
				zxfer_throw_error "Failed to read parent destination properties for child override inheritance." "$l_read_status"
			}
			l_parent_dest_pvs=$g_zxfer_property_stage_file_read_result
			zxfer_cleanup_runtime_artifact_path "$l_parent_dest_tmp"
			l_parent_dest_pvs=$(zxfer_sanitize_property_list "$l_parent_dest_pvs" "$l_readonly_properties" "$g_option_I_ignore_properties")
			l_filtered_creation=$(zxfer_filter_child_creation_overrides_for_parent "$l_filtered_creation" "$l_parent_dest_pvs") ||
				zxfer_throw_error "Failed to filter child creation override properties." "$?"
		fi
		zxfer_remove_sources "$l_filtered_creation"
		l_property_list="$g_zxfer_new_rmvs_pv"
	fi

	l_with_parents="no"
	if [ "$l_parent_dataset" != "$l_destination" ]; then
		if [ "$l_parent_exists" -eq 0 ]; then
			if zxfer_property_list_has_entries "$l_property_list"; then
				$l_create_runner "yes" "filesystem" "" "" "$l_parent_dataset" ||
					zxfer_throw_error "Error when creating destination filesystem." "$?"
				if [ "$g_option_n_dryrun" -eq 0 ]; then
					zxfer_note_destination_dataset_exists "$l_parent_dataset"
					zxfer_invalidate_destination_property_mutation_cache "$l_parent_dataset"
				fi
			else
				l_with_parents="yes"
			fi
		fi
	fi

	$l_create_runner "$l_with_parents" "$l_source_dstype" "$l_source_volsize" "$l_property_list" "$l_destination" ||
		zxfer_throw_error "Error when creating destination filesystem." "$?"

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_note_destination_dataset_exists "$l_destination"
		zxfer_invalidate_destination_property_mutation_cache "$l_destination"
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

# Purpose: Run one destination property verb (set/inherit) through the
# controlled execution path owned by this module: render the verbose display
# line, execute live (raising the verb-specific error message on failure),
# and invalidate the mutated destination's cached rows, or print the rendered
# command instead on dry runs.
# Usage: Called by zxfer_run_zfs_set_assignments and
# zxfer_run_zfs_inherit_property once planning is complete and zxfer is ready
# to execute the action.
zxfer_run_destination_property_verb() {
	l_verb=$1
	l_verb_error_message=$2
	l_destination=$3
	shift 3

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		if zxfer_command_display_render_enabled; then
			zxfer_echov "$(zxfer_build_destination_zfs_command "$l_verb" "$@" "$l_destination")"
		fi
		zxfer_run_destination_zfs_property_command "$l_verb" "$@" "$l_destination" ||
			zxfer_throw_error "$l_verb_error_message" "$?"
		zxfer_invalidate_destination_property_mutation_cache "$l_destination"
	else
		printf '%s\n' "$(zxfer_build_destination_zfs_command "$l_verb" "$@" "$l_destination")"
	fi
}

# Purpose: Run the ZFS set assignments through the controlled execution path
# owned by this module.
# Usage: Called during property filtering, diffing, and apply once planning is
# complete and zxfer is ready to execute the action.
zxfer_run_zfs_set_assignments() {
	l_set_destination=$1
	shift

	[ "$#" -gt 0 ] || return 0

	zxfer_run_destination_property_verb set \
		"Error when setting properties on destination filesystem." \
		"$l_set_destination" "$@"
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
	l_set_properties_remaining=$l_property_list
	while [ -n "$l_set_properties_remaining" ]; do
		case "$l_set_properties_remaining" in
		*,*)
			l_prop_value=${l_set_properties_remaining%%,*}
			l_set_properties_remaining=${l_set_properties_remaining#*,}
			;;
		*)
			l_prop_value=$l_set_properties_remaining
			l_set_properties_remaining=""
			;;
		esac
		[ -n "$l_prop_value" ] || continue
		zxfer_decode_serialized_property_assignment "$l_prop_value" >/dev/null ||
			return "$?"
		set -- "$@" "$g_zxfer_decoded_property_assignment_result"
	done

	zxfer_run_zfs_set_assignments "$l_destination" "$@"
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
	zxfer_run_destination_property_verb inherit \
		"Error when inheriting properties on destination filesystem." \
		"$2" "$1"
}

ZXFER_PROPERTY_DIFF_AWK='
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function source_requires_local_set(source_value) {
	return (source_value == "local")
}
function source_requires_initial_set(source_value) {
	return (source_value == "local" || source_value == "override")
}
function property_blocks_inherit(property_name) {
	return (property_name in noninheritable)
}
function source_can_inherit_on_child(property_name, source_value) {
	return (source_value != "local" && !(property_name in noninheritable))
}
BEGIN {
	noninheritable_count = split(noninheritable_properties, noninheritable_items, ",")
	for (i = 1; i <= noninheritable_count; i++) {
		if (noninheritable_items[i] == "")
			continue
		noninheritable[noninheritable_items[i]] = 1
	}

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
			if (source_requires_initial_set(override_source[i]))
				initial_set_list = append_csv(initial_set_list, override_property[i] "=" override_value[i])
			if (source_requires_local_set(override_source[i]) ||
				property_blocks_inherit(override_property[i]))
				child_set_list = append_csv(child_set_list, override_property[i] "=" override_value[i])
			else if (source_can_inherit_on_child(override_property[i], override_source[i]))
				inherit_list = append_csv(inherit_list, override_property[i] "=" override_value[i])
			continue
		}

		if (dest_value[override_property[i]] != override_value[i] ||
			(source_requires_initial_set(override_source[i]) &&
			dest_source[override_property[i]] != "local")) {
			initial_set_list = append_csv(initial_set_list, override_property[i] "=" override_value[i])
		}

		if (override_value[i] != dest_value[override_property[i]]) {
			if (source_requires_local_set(override_source[i]) ||
				property_blocks_inherit(override_property[i]))
				child_set_list = append_csv(child_set_list, override_property[i] "=" override_value[i])
			else
				inherit_list = append_csv(inherit_list, override_property[i] "=" override_value[i])
		} else if (source_requires_local_set(override_source[i]) &&
			dest_source[override_property[i]] != "local") {
			child_set_list = append_csv(child_set_list, override_property[i] "=" override_value[i])
		} else if (source_can_inherit_on_child(override_property[i], override_source[i]) &&
			dest_source[override_property[i]] == "local") {
			inherit_list = append_csv(inherit_list, override_property[i] "=" override_value[i])
		}

		delete dest_available[override_property[i]]
	}

	print initial_set_list
	print child_set_list
	print inherit_list
}'

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

	zxfer_create_property_reconcile_stage_file ||
		return "$?"
	l_diff_tmp=$g_zxfer_property_reconcile_stage_file_result
	l_noninheritable_properties=$(zxfer_get_noninheritable_properties)
	"${g_cmd_awk:-awk}" -v override_pvs="$l_override_pvs" \
		-v dest_pvs="$l_dest_pvs" \
		-v must_create_properties="$l_must_create_properties" \
		-v noninheritable_properties="$l_noninheritable_properties" \
		"$ZXFER_PROPERTY_DIFF_AWK" >"$l_diff_tmp"
	l_status=$?

	if [ "$l_status" -eq 3 ]; then
		zxfer_read_property_reconcile_stage_file "$l_diff_tmp" >/dev/null || {
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
			return "$l_read_status"
		}
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

	zxfer_read_property_reconcile_stage_file "$l_diff_tmp" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_diff_tmp"
		return "$l_read_status"
	}
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

ZXFER_CHILD_INHERIT_ADJUST_AWK='
function append_csv(current, value) {
	if (current == "")
		return value
	return current "," value
}
function source_requires_local_set(property_name, source_value) {
	return (source_value == "local" || (property_name in noninheritable))
}
function matches_inheritable_override(property_name, property_value) {
	return ((property_name in override_source) &&
		override_source[property_name] == "override" &&
		!(property_name in noninheritable) &&
		override_value[property_name] == property_value)
}
BEGIN {
	noninheritable_count = split(noninheritable_properties, noninheritable_items, ",")
	for (i = 1; i <= noninheritable_count; i++) {
		if (noninheritable_items[i] == "")
			continue
		noninheritable[noninheritable_items[i]] = 1
	}

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
			source_requires_local_set(set_property, override_source[set_property])) {
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

	inherit_count = split(inherit_list, inherit_items, ",")
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

# Purpose: Load and sanitize one destination parent's effective properties for
# child inheritance reconciliation.
# Usage: Called only after the parent is known to exist; preserves exact stage,
# probe, readback, cleanup, and profiling behavior.
# Side effects: Publishes the sanitized list through l_parent_dest_pvs.
zxfer_load_parent_properties_for_inherit_adjustment() {
	l_inherit_parent_dataset=$1
	l_inherit_readonly_properties=$2

	zxfer_create_property_reconcile_stage_file || return "$?"
	l_inherit_parent_tmp=$g_zxfer_property_reconcile_stage_file_result
	zxfer_collect_destination_props "$l_inherit_parent_dataset" "$g_RZFS" >"$l_inherit_parent_tmp" || {
		l_inherit_parent_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_inherit_parent_tmp"
		return "$l_inherit_parent_status"
	}
	if [ "$g_zxfer_normalized_dataset_properties_cache_hit" -eq 0 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_parent_destination_property_reads
	fi
	zxfer_read_property_reconcile_stage_file "$l_inherit_parent_tmp" >/dev/null || {
		l_inherit_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_inherit_parent_tmp"
		return "$l_inherit_read_status"
	}
	l_parent_dest_pvs=$g_zxfer_property_stage_file_read_result
	zxfer_cleanup_runtime_artifact_path "$l_inherit_parent_tmp"
	l_parent_dest_pvs=$(zxfer_sanitize_property_list "$l_parent_dest_pvs" \
		"$l_inherit_readonly_properties" "$g_option_I_ignore_properties")
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
	zxfer_set_adjusted_property_lists "" ""

	if [ -z "$l_inherit_list" ] && [ -z "$l_set_list" ]; then
		zxfer_publish_adjusted_property_lists "$l_set_list" "$l_inherit_list"
		return
	fi

	l_parent_dataset=${l_destination%/*}
	if [ "$l_parent_dataset" = "$l_destination" ]; then
		zxfer_publish_adjusted_property_lists "$l_set_list" "$l_inherit_list"
		return
	fi

	l_parent_exists=$(zxfer_exists_destination "$l_parent_dataset") ||
		zxfer_throw_error "$l_parent_exists" "$?"
	if [ "$l_parent_exists" -eq 0 ]; then
		zxfer_publish_adjusted_property_lists "$l_set_list" "$l_inherit_list"
		return
	fi

	zxfer_load_parent_properties_for_inherit_adjustment "$l_parent_dataset" \
		"$l_readonly_properties" || return "$?"

	l_noninheritable_properties=$(zxfer_get_noninheritable_properties)
	l_status=0
	l_adjusted_lists=$(
		"${g_cmd_awk:-awk}" \
			-v override_pvs="$l_override_pvs" \
			-v parent_pvs="$l_parent_dest_pvs" \
			-v current_set_list="$l_set_list" \
			-v inherit_list="$l_inherit_list" \
			-v noninheritable_properties="$l_noninheritable_properties" \
			"$ZXFER_CHILD_INHERIT_ADJUST_AWK"
	) || l_status=$?

	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to reconcile child property inheritance."
	fi

	l_adjusted_set_list=""
	l_adjusted_inherit_list=""
	{
		IFS= read -r l_adjusted_set_list
		IFS= read -r l_adjusted_inherit_list
	} <<EOF
$l_adjusted_lists
EOF
	zxfer_publish_adjusted_property_lists "$l_adjusted_set_list" "$l_adjusted_inherit_list"
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

	if [ "$l_is_initial_source" -eq 0 ] && [ "$l_inherit_list" != "" ]; then
		l_inherit_remaining=$l_inherit_list
		while [ -n "$l_inherit_remaining" ]; do
			case "$l_inherit_remaining" in
			*,*)
				ov_line=${l_inherit_remaining%%,*}
				l_inherit_remaining=${l_inherit_remaining#*,}
				;;
			*)
				ov_line=$l_inherit_remaining
				l_inherit_remaining=""
				;;
			esac
			[ -n "$ov_line" ] || continue
			ov_property=$(echo "$ov_line" | cut -f1 -d=)
			$l_inherit_runner "$ov_property" "$l_destination"
		done
	fi
}

# Purpose: Backfill required creation-time properties through a caller-owned
# stage file.
# Usage: Called by property-transfer stages that need the same readback,
# failure propagation, and cleanup behavior for source and destination payloads.
#
# Result is stored in g_zxfer_required_property_backfill_result.
# $1: dataset name
# $2: existing property list
# $3: zfs command used to query properties
# $4: comma-separated list of required property names
# $5: source/destination side label
# $6: caller-owned stage file
zxfer_backfill_required_properties_for_transfer() {
	l_backfill_dataset=$1
	l_backfill_pvs=$2
	l_backfill_zfs_cmd=$3
	l_backfill_must_create_properties=$4
	l_backfill_side=$5
	l_backfill_stage_file=$6

	zxfer_publish_required_property_backfill_result ""
	l_backfill_status=0
	zxfer_ensure_required_properties_present "$l_backfill_dataset" "$l_backfill_pvs" "$l_backfill_zfs_cmd" "$l_backfill_must_create_properties" "$l_backfill_side" >"$l_backfill_stage_file" ||
		l_backfill_status=$?
	if [ "$l_backfill_status" -ne 0 ]; then
		zxfer_read_property_reconcile_stage_file "$l_backfill_stage_file" >/dev/null || {
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_backfill_stage_file"
			return "$l_read_status"
		}
		zxfer_publish_required_property_backfill_result "$g_zxfer_property_stage_file_read_result"
		zxfer_cleanup_runtime_artifact_path "$l_backfill_stage_file"
		zxfer_throw_error "$g_zxfer_required_property_backfill_result" "$l_backfill_status"
	fi

	zxfer_read_property_reconcile_stage_file "$l_backfill_stage_file" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_backfill_stage_file"
		return "$l_read_status"
	}
	zxfer_publish_required_property_backfill_result "$g_zxfer_property_stage_file_read_result"
}

# Purpose: Collect and normalize the source-side context for a property
# transfer.
# Usage: First stage of zxfer_transfer_properties.
#
# Populates g_zxfer_property_transfer_is_initial_source,
# g_zxfer_property_transfer_source_dstype,
# g_zxfer_property_transfer_source_volsize,
# g_zxfer_property_transfer_must_create_properties, and
# g_zxfer_property_transfer_source_pvs.
zxfer_prepare_property_transfer_source_context() {
	l_property_source=$1

	if [ "$g_initial_source" = "$l_property_source" ]; then
		zxfer_set_property_transfer_initial_source 1
	else
		zxfer_set_property_transfer_initial_source 0
	fi

	zxfer_collect_source_props "$l_property_source" "$g_actual_dest" "$g_ensure_writable" "$g_LZFS" ||
		zxfer_throw_error "${g_zxfer_source_pvs_raw:-Failed to retrieve source properties for [$l_property_source].}" "$?"

	l_source_create_metadata=$(zxfer_get_validated_source_dataset_create_metadata "$l_property_source") ||
		zxfer_throw_error "$l_source_create_metadata" "$?"
	l_property_transfer_source_dstype=""
	l_property_transfer_source_volsize=""
	{
		IFS= read -r l_property_transfer_source_dstype
		IFS= read -r l_property_transfer_source_volsize
	} <<EOF
$l_source_create_metadata
EOF
	zxfer_publish_property_transfer_source_metadata "$l_property_transfer_source_dstype" \
		"$l_property_transfer_source_volsize" ""
	l_property_transfer_must_create_properties=$(zxfer_get_required_creation_properties_for_dataset_type "$l_property_transfer_source_dstype")
	zxfer_publish_property_transfer_source_metadata "$l_property_transfer_source_dstype" \
		"$l_property_transfer_source_volsize" "$l_property_transfer_must_create_properties"

	zxfer_create_property_reconcile_stage_file ||
		return "$?"
	l_required_props_tmp=$g_zxfer_property_reconcile_stage_file_result

	zxfer_backfill_required_properties_for_transfer "$l_property_source" "$g_zxfer_source_pvs_raw" "$g_LZFS" "$g_zxfer_property_transfer_must_create_properties" source "$l_required_props_tmp" ||
		return "$?"
	zxfer_publish_source_property_results "$g_zxfer_required_property_backfill_result" \
		"$g_zxfer_source_pvs_effective"

	zxfer_backfill_required_properties_for_transfer "$l_property_source" "$g_zxfer_source_pvs_effective" "$g_LZFS" "$g_zxfer_property_transfer_must_create_properties" source "$l_required_props_tmp" ||
		return "$?"
	zxfer_publish_source_property_results "$g_zxfer_source_pvs_raw" \
		"$g_zxfer_required_property_backfill_result"
	zxfer_cleanup_runtime_artifact_path "$l_required_props_tmp"

	zxfer_publish_property_transfer_source_properties "$g_zxfer_source_pvs_effective"
}

# Purpose: Derive the transfer override and create-time property lists.
# Usage: Called after source context is available and before destination create
# or diff planning.
#
# Populates g_zxfer_property_transfer_override_pvs and
# g_zxfer_property_transfer_creation_pvs.
zxfer_prepare_property_transfer_override_context() {
	l_property_is_initial_source=$1
	l_property_source_pvs=$2
	l_property_source_dstype=$3
	l_property_effective_readonly_properties=$4
	l_override_property_pv=$g_option_o_override_property

	if [ "$g_ensure_writable" -eq 1 ]; then
		l_override_property_pv=$(zxfer_force_readonly_off "$l_override_property_pv")
	fi

	if [ "$l_property_is_initial_source" -eq 1 ]; then
		zxfer_validate_override_properties "$l_override_property_pv" "$l_property_source_pvs"
	fi

	l_derive_override_status=0
	zxfer_derive_override_lists "$l_property_source_pvs" "$l_override_property_pv" "$g_option_P_transfer_property" "$l_property_source_dstype" >/dev/null ||
		l_derive_override_status=$?
	[ "$l_derive_override_status" -eq 0 ] || return "$l_derive_override_status"

	l_property_transfer_override_pvs=$g_zxfer_override_pvs_result
	l_property_transfer_creation_pvs=$g_zxfer_creation_pvs_result
	zxfer_publish_property_transfer_override_results "$l_property_transfer_override_pvs" \
		"$l_property_transfer_creation_pvs"
	l_property_transfer_override_pvs=$(zxfer_sanitize_property_list "$g_zxfer_property_transfer_override_pvs" "$l_property_effective_readonly_properties" "$g_option_I_ignore_properties")
	l_property_transfer_creation_pvs=$(zxfer_sanitize_property_list "$g_zxfer_property_transfer_creation_pvs" "$l_property_effective_readonly_properties" "$g_option_I_ignore_properties")
	zxfer_publish_property_transfer_override_results "$l_property_transfer_override_pvs" \
		"$l_property_transfer_creation_pvs"

	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ]; then
		l_unsupported_properties=$(zxfer_select_unsupported_properties_for_dataset_type "$l_property_source_dstype")
	else
		l_unsupported_properties=""
	fi
	l_property_transfer_override_pvs=$(zxfer_strip_unsupported_properties "$g_zxfer_property_transfer_override_pvs" "$l_unsupported_properties")
	l_property_transfer_creation_pvs=$(zxfer_strip_unsupported_properties "$g_zxfer_property_transfer_creation_pvs" "$l_unsupported_properties")
	zxfer_publish_property_transfer_override_results "$l_property_transfer_override_pvs" \
		"$l_property_transfer_creation_pvs"
	zxfer_echoV "zxfer_transfer_properties override_pvs: $g_zxfer_property_transfer_override_pvs"
	zxfer_echoV "zxfer_transfer_properties creation_pvs: $g_zxfer_property_transfer_creation_pvs"
}

# Purpose: Try the create-path part of property transfer.
# Usage: Called before destination property diffing; returns 0 when the
# destination was created and the caller should stop, or 1 when diffing should
# continue against an existing destination.
zxfer_try_property_transfer_destination_create() {
	l_property_source=$1
	l_property_skip_backup_capture=$2
	l_property_is_initial_source=$3
	l_property_override_pvs=$4
	l_property_creation_pvs=$5
	l_property_source_dstype=$6
	l_property_source_volsize=$7
	l_property_effective_readonly_properties=$8

	l_dest_exist=0
	l_destinations=$(printf '%s\n' "${g_recursive_dest_list:-}" | tr ' ' '\n')
	while IFS= read -r l_recorded_destination || [ -n "$l_recorded_destination" ]; do
		[ -n "$l_recorded_destination" ] || continue
		if [ "$l_recorded_destination" = "$g_actual_dest" ]; then
			l_dest_exist=1
			break
		fi
	done <<-EOF
		$l_destinations
	EOF
	if [ "$l_dest_exist" -eq 0 ]; then
		l_dest_exist_status=0
		l_live_dest_exist=$(zxfer_exists_destination "$g_actual_dest" live) ||
			l_dest_exist_status=$?
		if [ "$l_dest_exist_status" -ne 0 ]; then
			zxfer_throw_error "$l_live_dest_exist" "$l_dest_exist_status"
			return "$l_dest_exist_status"
		fi
		if [ "$l_live_dest_exist" -ne 0 ]; then
			zxfer_note_destination_dataset_exists "$g_actual_dest"
			l_dest_exist=1
		fi
	fi

	if zxfer_ensure_destination_exists "$l_dest_exist" "$l_property_is_initial_source" "$l_property_override_pvs" "$l_property_creation_pvs" "$l_property_source_dstype" "$l_property_source_volsize" "$g_actual_dest" "$l_property_effective_readonly_properties" ""; then
		zxfer_capture_backup_metadata_for_completed_transfer "$l_property_source" "$g_zxfer_source_pvs_raw" "$l_property_skip_backup_capture"
		return 0
	fi

	return 1
}

# Purpose: Collect and normalize destination-side properties for diffing.
# Usage: Called after destination creation is ruled out.
#
# Populates g_zxfer_property_transfer_dest_pvs.
zxfer_collect_property_transfer_destination_context() {
	l_property_must_create_properties=$1
	l_property_effective_readonly_properties=$2

	zxfer_create_property_reconcile_stage_file ||
		return "$?"
	l_dest_pvs_tmp=$g_zxfer_property_reconcile_stage_file_result

	zxfer_collect_destination_props "$g_actual_dest" "$g_RZFS" >"$l_dest_pvs_tmp" || {
		l_dest_pvs_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
		zxfer_throw_error "Failed to retrieve destination properties for [$g_actual_dest]." "$l_dest_pvs_status"
	}
	zxfer_read_property_reconcile_stage_file "$l_dest_pvs_tmp" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"
		return "$l_read_status"
	}
	l_dest_pvs=$g_zxfer_property_stage_file_read_result

	zxfer_backfill_required_properties_for_transfer "$g_actual_dest" "$l_dest_pvs" "$g_RZFS" "$l_property_must_create_properties" destination "$l_dest_pvs_tmp" ||
		return "$?"
	l_dest_pvs=$g_zxfer_required_property_backfill_result
	zxfer_cleanup_runtime_artifact_path "$l_dest_pvs_tmp"

	l_property_transfer_dest_pvs=$(zxfer_sanitize_property_list "$l_dest_pvs" "$l_property_effective_readonly_properties" "$g_option_I_ignore_properties")
	zxfer_publish_property_transfer_destination_properties "$l_property_transfer_dest_pvs"
	zxfer_echoV "zxfer_transfer_properties dest_pvs: $g_zxfer_property_transfer_dest_pvs"
}

# Purpose: Diff destination property state and adjust child inheritance where
# needed.
# Usage: Called after source override and destination property contexts exist.
#
# Populates g_zxfer_property_transfer_initial_set_list,
# g_zxfer_property_transfer_child_set_list, and
# g_zxfer_property_transfer_inherit_list.
zxfer_diff_property_transfer_changes() {
	l_property_is_initial_source=$1
	l_property_override_pvs=$2
	l_property_dest_pvs=$3
	l_property_must_create_properties=$4
	l_property_effective_readonly_properties=$5

	zxfer_create_property_reconcile_stage_file ||
		return "$?"
	l_diff_properties_tmp=$g_zxfer_property_reconcile_stage_file_result

	zxfer_diff_properties "$l_property_override_pvs" "$l_property_dest_pvs" "$l_property_must_create_properties" >"$l_diff_properties_tmp" || {
		l_diff_properties_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_diff_properties_tmp"
		zxfer_throw_error "Failed to calculate property reconciliation changes for destination [$g_actual_dest]." "$l_diff_properties_status"
	}
	zxfer_read_property_reconcile_stage_file "$l_diff_properties_tmp" >/dev/null || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_diff_properties_tmp"
		return "$l_read_status"
	}
	l_property_transfer_initial_set_list=""
	l_property_transfer_child_set_list=""
	l_property_transfer_inherit_list=""
	{
		IFS= read -r l_property_transfer_initial_set_list
		IFS= read -r l_property_transfer_child_set_list
		IFS= read -r l_property_transfer_inherit_list
	} <<EOF
$g_zxfer_property_stage_file_read_result
EOF
	zxfer_publish_property_transfer_diff_results "$l_property_transfer_initial_set_list" \
		"$l_property_transfer_child_set_list" "$l_property_transfer_inherit_list"
	zxfer_cleanup_runtime_artifact_path "$l_diff_properties_tmp"

	zxfer_echoV "zxfer_transfer_properties init_set: $g_zxfer_property_transfer_initial_set_list"
	zxfer_echoV "zxfer_transfer_properties child_set: $g_zxfer_property_transfer_child_set_list"
	zxfer_echoV "zxfer_transfer_properties inherit: $g_zxfer_property_transfer_inherit_list"

	if [ "$l_property_is_initial_source" -eq 0 ] &&
		{ [ "$g_zxfer_property_transfer_child_set_list" != "" ] || [ "$g_zxfer_property_transfer_inherit_list" != "" ]; }; then
		zxfer_adjust_child_inherit_to_match_parent "$g_actual_dest" "$l_property_override_pvs" "$g_zxfer_property_transfer_child_set_list" "$g_zxfer_property_transfer_inherit_list" "$l_property_effective_readonly_properties" >/dev/null ||
			zxfer_throw_error "Failed to reconcile inherited child properties for destination [$g_actual_dest]." "$?"
		zxfer_publish_property_transfer_diff_results "$g_zxfer_property_transfer_initial_set_list" \
			"$g_zxfer_adjusted_set_list" "$g_zxfer_adjusted_inherit_list"
		zxfer_echoV "zxfer_transfer_properties adjusted child_set: $g_zxfer_property_transfer_child_set_list"
		zxfer_echoV "zxfer_transfer_properties adjusted inherit: $g_zxfer_property_transfer_inherit_list"
	fi
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

	l_property_transfer_source=$1
	l_property_transfer_skip_backup_capture=${2:-0}
	l_effective_readonly_properties=$(zxfer_get_effective_readonly_properties)

	zxfer_prepare_property_transfer_source_context "$l_property_transfer_source" ||
		return "$?"
	zxfer_prepare_property_transfer_override_context "$g_zxfer_property_transfer_is_initial_source" "$g_zxfer_property_transfer_source_pvs" "$g_zxfer_property_transfer_source_dstype" "$l_effective_readonly_properties" ||
		return "$?"

	l_property_transfer_create_status=0
	zxfer_try_property_transfer_destination_create "$l_property_transfer_source" "$l_property_transfer_skip_backup_capture" "$g_zxfer_property_transfer_is_initial_source" "$g_zxfer_property_transfer_override_pvs" "$g_zxfer_property_transfer_creation_pvs" "$g_zxfer_property_transfer_source_dstype" "$g_zxfer_property_transfer_source_volsize" "$l_effective_readonly_properties" ||
		l_property_transfer_create_status=$?
	if [ "$l_property_transfer_create_status" -eq 0 ]; then
		return
	fi
	[ "$l_property_transfer_create_status" -eq 1 ] ||
		return "$l_property_transfer_create_status"

	zxfer_collect_property_transfer_destination_context "$g_zxfer_property_transfer_must_create_properties" "$l_effective_readonly_properties" ||
		return "$?"
	zxfer_diff_property_transfer_changes "$g_zxfer_property_transfer_is_initial_source" "$g_zxfer_property_transfer_override_pvs" "$g_zxfer_property_transfer_dest_pvs" "$g_zxfer_property_transfer_must_create_properties" "$l_effective_readonly_properties" ||
		return "$?"
	zxfer_apply_property_changes "$g_actual_dest" "$g_zxfer_property_transfer_is_initial_source" "$g_zxfer_property_transfer_initial_set_list" "$g_zxfer_property_transfer_child_set_list" "$g_zxfer_property_transfer_inherit_list" "" ""
	zxfer_capture_backup_metadata_for_completed_transfer "$l_property_transfer_source" "$g_zxfer_source_pvs_raw" "$l_property_transfer_skip_backup_capture"
}
