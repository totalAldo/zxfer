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
# PROPERTY CREATE / DIFF / APPLY / TRANSFER
################################################################################

# Module contract:
# owns globals: the diff and child-inheritance AWK programs and the result
#   globals g_zxfer_source_pvs_raw/effective, g_zxfer_diff_*_result and
#   g_zxfer_adjusted_*_list.
# reads globals: the current source/destination context, property CLI options,
#   restore metadata, the property state/policy result globals, and the
#   rendered -T command in g_zxfer_shell_command_result.
# mutates caches: destination property rows, through the state module's
#   invalidation after each successful mutation, and the destination existence
#   cache through probes.
# returns via stdout: rendered destination commands on dry runs; live runs
#   create datasets and apply property changes instead.

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

# Purpose: Load the source property list and the effective list used for
# transfer (the restored -e view when enabled).
# Usage: zxfer_collect_source_props SOURCE DESTINATION; publishes
# g_zxfer_source_pvs_raw and g_zxfer_source_pvs_effective, or returns the zfs
# status with its diagnostic in g_zxfer_property_error_result. The source is
# read through the source zfs role, so it reaches the -O host. Startup has
# already validated the -e metadata header.
zxfer_collect_source_props() {
	l_collect_source=$1
	l_collect_destination=$2

	g_zxfer_source_pvs_raw=""
	g_zxfer_source_pvs_effective=""
	zxfer_load_normalized_dataset_properties "$l_collect_source" source || return "$?"
	g_zxfer_source_pvs_raw=$g_zxfer_normalized_dataset_properties
	g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return 0

	if [ -z "$g_restored_backup_file_contents" ]; then
		zxfer_throw_usage_error "Can't find the properties for the filesystem $l_collect_source and destination $l_collect_destination"
	fi
	l_restore_status=0
	g_zxfer_source_pvs_effective=$(zxfer_backup_metadata_extract_properties_for_dataset_pair \
		"$g_restored_backup_file_contents" "$l_collect_source" "$l_collect_destination") ||
		l_restore_status=$?
	case $l_restore_status in
	0) ;;
	1 | 8)
		zxfer_throw_usage_error "Can't find the properties for the filesystem $l_collect_source and destination $l_collect_destination"
		;;
	2)
		zxfer_throw_usage_error "Multiple restored property entries matched filesystem $l_collect_source and destination $l_collect_destination"
		;;
	*)
		zxfer_throw_usage_error "Failed to parse the restored properties for the filesystem $l_collect_source and destination $l_collect_destination"
		;;
	esac
}

################################################################################
# DESTINATION CREATE / APPLY HELPERS
################################################################################

# Purpose: Create the destination dataset when it does not exist.
# Usage: zxfer_ensure_destination_exists IS_INITIAL_SOURCE OVERRIDE_PVS
# CREATION_PVS SOURCE_TYPE SOURCE_VOLSIZE DESTINATION READONLY_CSV. Existence
# comes from the recursive destination list, else a live probe. Returns 0 when
# the dataset was created (no further property work is needed) and 1 when it
# already exists and needs a diff; throws on probe or create failures. The
# initial source is created with its full override list; a child gets its
# creation list, minus overrides the existing parent already supplies for
# inheritable properties.
zxfer_ensure_destination_exists() {
	l_ensure_is_initial_source=$1
	l_ensure_override_pvs=$2
	l_ensure_creation_pvs=$3
	l_ensure_source_dstype=$4
	l_ensure_source_volsize=$5
	l_ensure_destination=$6
	l_ensure_readonly_properties=$7

	case "$ZXFER_LF${g_recursive_dest_list:-}$ZXFER_LF" in
	*"$ZXFER_LF$l_ensure_destination$ZXFER_LF"*) return 1 ;;
	esac
	zxfer_probe_destination_existence "$l_ensure_destination" live ||
		zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
	if [ "$g_zxfer_destination_exists_result" -ne 0 ]; then
		zxfer_note_destination_dataset_exists "$l_ensure_destination"
		return 1
	fi

	zxfer_echov "Creating destination filesystem \"$l_ensure_destination\" with specified properties."

	l_ensure_parent_exists=""
	l_ensure_parent_dataset=${l_ensure_destination%/*}
	if [ "$l_ensure_parent_dataset" != "$l_ensure_destination" ]; then
		zxfer_probe_destination_existence "$l_ensure_parent_dataset" ||
			zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
		l_ensure_parent_exists=$g_zxfer_destination_exists_result
	fi

	if [ "$l_ensure_is_initial_source" -eq 1 ]; then
		l_ensure_property_list=$l_ensure_override_pvs
	else
		l_ensure_property_list=$l_ensure_creation_pvs
		case "$l_ensure_parent_exists,$l_ensure_property_list," in
		1,*"=override,"*)
			zxfer_load_normalized_dataset_properties "$l_ensure_parent_dataset" destination ||
				zxfer_throw_error "Failed to retrieve parent destination properties for [$l_ensure_parent_dataset]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
			zxfer_sanitize_property_list "$g_zxfer_normalized_dataset_properties" \
				"$l_ensure_readonly_properties" "$g_option_I_ignore_properties"
			l_ensure_property_list=$(zxfer_filter_child_creation_overrides_for_parent \
				"$l_ensure_property_list" "$g_zxfer_sanitized_property_list_result") ||
				zxfer_throw_error "Failed to filter child creation override properties." "$?"
			;;
		esac
	fi

	l_ensure_with_parents="no"
	if [ "$l_ensure_parent_exists" = "0" ]; then
		case $l_ensure_property_list in
		*[!,]*)
			zxfer_run_zfs_create_with_properties "yes" "filesystem" "" "" "$l_ensure_parent_dataset" ||
				zxfer_throw_error "Error when creating destination filesystem." "$?"
			if [ "$g_option_n_dryrun" -eq 0 ]; then
				zxfer_note_destination_dataset_exists "$l_ensure_parent_dataset"
				zxfer_invalidate_destination_property_mutation_cache "$l_ensure_parent_dataset"
			fi
			;;
		*)
			l_ensure_with_parents="yes"
			;;
		esac
	fi

	zxfer_run_zfs_create_with_properties "$l_ensure_with_parents" "$l_ensure_source_dstype" \
		"$l_ensure_source_volsize" "$l_ensure_property_list" "$l_ensure_destination" ||
		zxfer_throw_error "Error when creating destination filesystem." "$?"

	if [ "$g_option_n_dryrun" -eq 0 ]; then
		zxfer_note_destination_dataset_exists "$l_ensure_destination"
		zxfer_invalidate_destination_property_mutation_cache "$l_ensure_destination"
	fi

	return 0
}

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
# [ARG...]. A live run shows the command under -v, throws ERROR_MESSAGE with
# the zfs status on failure, and then drops the destination's cached rows.
zxfer_run_destination_property_verb() {
	l_verb=$1
	l_verb_error_message=$2
	l_verb_destination=$3
	shift 3

	if [ "$g_option_n_dryrun" -ne 0 ]; then
		zxfer_build_destination_zfs_command "$l_verb" "$@" "$l_verb_destination"
		return
	fi
	if zxfer_command_display_render_enabled; then
		zxfer_echov "$(zxfer_build_destination_zfs_command "$l_verb" "$@" "$l_verb_destination")"
	fi
	zxfer_run_destination_zfs_cmd "$l_verb" "$@" "$l_verb_destination" ||
		zxfer_throw_error "$l_verb_error_message" "$?"
	zxfer_invalidate_destination_property_mutation_cache "$l_verb_destination"
}

# Purpose: Run one batched `zfs set` from a serialized property list, each
# item decoded into exactly one property=value argument.
# Usage: zxfer_run_zfs_set_properties LIST DESTINATION.
zxfer_run_zfs_set_properties() {
	l_set_rest=$1,
	l_set_destination=$2

	set --
	while [ -n "$l_set_rest" ]; do
		l_set_item=${l_set_rest%%,*}
		l_set_rest=${l_set_rest#*,}
		[ -n "$l_set_item" ] || continue
		l_set_value=${l_set_item#*=}
		zxfer_decode_property_value "${l_set_value%%=*}"
		set -- "$@" "${l_set_item%%=*}=$g_zxfer_decoded_property_value"
	done
	[ "$#" -gt 0 ] || return 0

	zxfer_run_destination_property_verb set \
		"Error when setting properties on destination filesystem." \
		"$l_set_destination" "$@"
}

################################################################################
# DIFF
################################################################################

# Filters the destination list in ZXFER_AWK_DEST_PVS with the readonly and -I
# lists and prints it, then compares the override (apply) list in
# ZXFER_AWK_OVERRIDE_PVS with it: prints a must-create mismatch and exits 3, or
# prints the OK marker followed by the initial-set, child-set, and inherit
# lists. Runs behind ZXFER_PROPERTY_AWK_LIB and ZXFER_PROPERTY_FILTER_AWK.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_PROPERTY_DIFF_AWK='
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
	dest_pvs = filter_property_list(ENVIRON["ZXFER_AWK_DEST_PVS"])
	print dest_pvs
	csv_to_set(noninheritable_properties, noninheritable)
	csv_to_set(must_create_properties, must_create)
	first_values(dest_pvs, dest_value, dest_source)
	for (dest_property in dest_value)
		dest_available[dest_property] = 1

	override_count = split(ENVIRON["ZXFER_AWK_OVERRIDE_PVS"], override_items, ",")
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

# Purpose: Filter the destination list and diff it against the override list,
# enforcing the must-create restriction, into the set and inherit operations
# to apply.
# Usage: zxfer_diff_properties OVERRIDE_PVS DEST_PVS MUST_CREATE_CSV
# [READONLY_CSV] [IGNORE_CSV]; publishes the filtered destination list in
# g_zxfer_diff_dest_pvs_result plus g_zxfer_diff_initial_set_result,
# g_zxfer_diff_child_set_result and g_zxfer_diff_inherit_result. Throws a
# usage error on a must-create mismatch and an error when awk fails.
zxfer_diff_properties() {
	g_zxfer_diff_dest_pvs_result=""
	g_zxfer_diff_initial_set_result=""
	g_zxfer_diff_child_set_result=""
	g_zxfer_diff_inherit_result=""

	l_diff_status=0
	l_diff_output=$(
		ZXFER_AWK_OVERRIDE_PVS=$1 ZXFER_AWK_DEST_PVS=$2 \
			ZXFER_AWK_REMOVE_LIST="${4:-},${5:-}" \
			ZXFER_AWK_UNSUPPORTED_LIST='' "${g_cmd_awk:-awk}" \
			-v must_create_properties="$3" \
			-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
			"$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_FILTER_AWK$ZXFER_PROPERTY_DIFF_AWK"
	) || l_diff_status=$?

	# Trailing empty lists are stripped by the command substitution, so the
	# last reads may hit EOF; that is not a failure.
	{
		IFS= read -r l_diff_dest_pvs
		IFS= read -r l_diff_marker
		IFS= read -r l_diff_initial_set
		IFS= read -r l_diff_child_set
		IFS= read -r l_diff_inherit
	} <<EOF || :
$l_diff_output
EOF
	if [ "$l_diff_status" -eq 3 ]; then
		zxfer_throw_error_with_usage "The property \"$l_diff_marker\" may only be set
at filesystem creation time. To modify this property
you will need to first destroy target filesystem."
	fi
	if [ "$l_diff_status" -ne 0 ] || [ "$l_diff_marker" != "__ZXFER_DIFF_OK__" ]; then
		zxfer_throw_error "Failed to diff dataset properties."
	fi
	g_zxfer_diff_dest_pvs_result=$l_diff_dest_pvs
	g_zxfer_diff_initial_set_result=$l_diff_initial_set
	g_zxfer_diff_child_set_result=$l_diff_child_set
	g_zxfer_diff_inherit_result=$l_diff_inherit
}

# Rewrites a child's set/inherit plan against the destination parent: a
# property stays inherited only when the parent already provides the desired
# effective value (or the value is a matching inheritable -o override);
# otherwise it must be set locally on the child. Runs behind
# ZXFER_PROPERTY_AWK_LIB.
# shellcheck disable=SC2016  # AWK field references must remain literal.
ZXFER_CHILD_INHERIT_ADJUST_AWK='
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
	csv_to_set(noninheritable_properties, noninheritable)
	first_values(ENVIRON["ZXFER_AWK_PARENT_PVS"], parent_value, parent_source)

	# The last override item of a property wins.
	override_count = split(ENVIRON["ZXFER_AWK_OVERRIDE_PVS"], override_items, ",")
	for (i = 1; i <= override_count; i++) {
		if (override_items[i] == "")
			continue
		split(override_items[i], override_fields, "=")
		override_source[override_fields[1]] = override_fields[3]
		override_value[override_fields[1]] = override_fields[2]
	}

	set_count = split(ENVIRON["ZXFER_AWK_SET_LIST"], set_items, ",")
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
	zxfer_sanitize_property_list "$g_zxfer_normalized_dataset_properties" \
		"$l_adjust_readonly_properties" "$g_option_I_ignore_properties"

	l_adjust_status=0
	l_adjusted_lists=$(
		ZXFER_AWK_OVERRIDE_PVS=$l_adjust_override_pvs \
			ZXFER_AWK_PARENT_PVS=$g_zxfer_sanitized_property_list_result \
			ZXFER_AWK_SET_LIST=$l_adjust_set_list \
			ZXFER_AWK_INHERIT_LIST=$l_adjust_inherit_list \
			"${g_cmd_awk:-awk}" -v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
			"$ZXFER_PROPERTY_AWK_LIB$ZXFER_CHILD_INHERIT_ADJUST_AWK"
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
# per inherit item. Throws when a set or inherit fails.
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

	if [ -n "$l_apply_set_list" ]; then
		zxfer_run_zfs_set_properties "$l_apply_set_list" "$l_apply_destination" ||
			zxfer_throw_error "Error when setting properties on destination filesystem." "$?"
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

# Purpose: Reconcile one dataset's properties: read the source, derive the
# override and creation plans from -P/-o/-I/-U, create the destination with
# its creation-time properties when it is missing, otherwise diff against the
# destination and apply the resulting sets and inherits, then buffer -k
# metadata.
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

	# Override and creation plans; the initial source also checks that every
	# -o property exists on the source.
	zxfer_derive_override_lists "$l_transfer_source_pvs" "$g_option_o_override_property" \
		"$g_option_P_transfer_property" "$l_transfer_source_dstype" "$l_transfer_readonly_properties" \
		"$g_option_I_ignore_properties" "$l_transfer_unsupported_properties" "$l_transfer_is_initial_source"
	l_transfer_override_pvs=$g_zxfer_override_pvs_result
	l_transfer_creation_pvs=$g_zxfer_creation_pvs_result
	zxfer_echoV_escaped "zxfer_transfer_properties override_pvs" "$l_transfer_override_pvs"
	zxfer_echoV_escaped "zxfer_transfer_properties creation_pvs" "$l_transfer_creation_pvs"

	# A missing destination is created with its creation-time properties and
	# needs no diff.
	if zxfer_ensure_destination_exists "$l_transfer_is_initial_source" "$l_transfer_override_pvs" \
		"$l_transfer_creation_pvs" "$l_transfer_source_dstype" "$l_transfer_source_volsize" \
		"$g_actual_dest" "$l_transfer_readonly_properties"; then
		zxfer_capture_backup_metadata_for_completed_transfer "$l_transfer_source" "$g_zxfer_source_pvs_raw" "$l_transfer_skip_backup_capture"
		return 0
	fi

	# Destination properties, diff, child inheritance adjustment, apply.
	# The zfs or parse diagnostic, when there is one, follows the context.
	zxfer_load_normalized_dataset_properties "$g_actual_dest" destination ||
		zxfer_throw_error "Failed to retrieve destination properties for [$g_actual_dest]${g_zxfer_property_error_result:+: }${g_zxfer_property_error_result:-.}" "$?"
	zxfer_backfill_required_properties "$g_actual_dest" "$g_zxfer_normalized_dataset_properties" \
		"$l_transfer_must_create_properties" destination ||
		zxfer_throw_error "$g_zxfer_property_error_result" "$?"
	zxfer_diff_properties "$l_transfer_override_pvs" "$g_zxfer_required_properties_result" \
		"$l_transfer_must_create_properties" "$l_transfer_readonly_properties" "$g_option_I_ignore_properties"
	l_transfer_initial_set_list=$g_zxfer_diff_initial_set_result
	l_transfer_child_set_list=$g_zxfer_diff_child_set_result
	l_transfer_inherit_list=$g_zxfer_diff_inherit_result
	zxfer_echoV_escaped "zxfer_transfer_properties dest_pvs" "$g_zxfer_diff_dest_pvs_result"
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
