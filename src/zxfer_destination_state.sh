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
# DESTINATION STATE: EXISTENCE CACHE / DATASET INVENTORY / LIVE LISTING
################################################################################

# Module contract:
# owns globals: the destination existence cache (g_destination_existence_cache,
#   _root, _root_complete), the destination dataset inventory
#   g_recursive_dest_list (seeded from discovery's recursive listing, grown as
#   datasets are created or received), the reusable depth-1 listing file
#   g_zxfer_live_destination_listing_file, and these in-shell results:
#   g_zxfer_destination_exists_result and _error,
#   g_zxfer_destination_existence_cache_entry_result,
#   g_zxfer_destination_dataset_result,
#   g_zxfer_live_destination_record_file_result and _error, and
#   g_zxfer_snapshot_scratch_file_result.
# reads globals: g_initial_source, g_initial_source_had_trailing_slash,
#   g_destination, g_destination_operating_system and g_zxfer_run_tmp_root.
# mutates caches: destination existence and the reusable live listing file.
# returns via stdout: none.

# Purpose: Reset the destination existence cache and the destination dataset
# inventory it is seeded from.
# Usage: Called at startup, before each discovery pass, by the -n preview and
# whenever the fast no-op proof declines.
zxfer_reset_destination_existence_cache() {
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
	g_recursive_dest_list=""
}

# Purpose: Forget the reusable live listing file.
# Usage: zxfer_reset_live_destination_listing_state; called by the session
# reset, so the next live listing allocates a file under the new run root.
zxfer_reset_live_destination_listing_state() {
	g_zxfer_live_destination_listing_file=""
}

# Purpose: Map a source dataset to its destination dataset without forking.
# Usage: zxfer_map_destination_dataset [SOURCE]; publishes
# g_zxfer_destination_dataset_result. Without SOURCE, or for a SOURCE outside
# g_initial_source, the result is the destination root.
zxfer_map_destination_dataset() {
	if [ "${g_initial_source_had_trailing_slash:-0}" -eq 1 ]; then
		g_zxfer_destination_dataset_result=$g_destination
	else
		g_zxfer_destination_dataset_result=$g_destination/${g_initial_source##*/}
	fi
	case ${1:-} in
	"$g_initial_source"/*)
		g_zxfer_destination_dataset_result=$g_zxfer_destination_dataset_result${1#"$g_initial_source"}
		;;
	esac
}

# Purpose: Reuse a run-root scratch file or allocate a fresh one.
# Usage: zxfer_ensure_snapshot_scratch_file CURRENT_PATH NAME_PREFIX; publishes
# the path in g_zxfer_snapshot_scratch_file_result. CURRENT_PATH is reused
# only when it lies under this run's temp root, so an inherited or stale path
# is never written through.
zxfer_ensure_snapshot_scratch_file() {
	g_zxfer_snapshot_scratch_file_result=""
	if [ -n "${g_zxfer_run_tmp_root:-}" ]; then
		case ${1:-} in
		"$g_zxfer_run_tmp_root"/?*)
			g_zxfer_snapshot_scratch_file_result=$1
			return 0
			;;
		esac
	fi
	zxfer_create_runtime_artifact_file "$2" || return
	g_zxfer_snapshot_scratch_file_result=$g_zxfer_runtime_artifact_path_result
}

# Purpose: List one destination dataset's live snapshots into a reusable
# run-root file.
# Usage: zxfer_get_live_destination_record_file DEST, in the main shell, for a
# dataset this run changed (the pre-send re-plan after -d destroys, and the
# post-receive divergence check). Publishes
# g_zxfer_live_destination_record_file_result, the file holding the depth-1
# listing's stdout and stderr (zxfer_plan_dataset_snapshots keeps only DEST
# rows). A failed listing returns its status and publishes the listing output
# in g_zxfer_live_destination_record_file_error.
zxfer_get_live_destination_record_file() {
	l_live_record_dest=$1
	g_zxfer_live_destination_record_file_result=""
	g_zxfer_live_destination_record_file_error=""

	zxfer_profile_increment_counter g_zxfer_profile_live_destination_snapshot_rechecks
	zxfer_ensure_snapshot_scratch_file "${g_zxfer_live_destination_listing_file:-}" \
		zxfer-live-dest-listing || return
	g_zxfer_live_destination_listing_file=$g_zxfer_snapshot_scratch_file_result

	l_live_record_status=0
	zxfer_run_destination_zfs_cmd list -H -d 1 -o name,guid -t snapshot "$l_live_record_dest" \
		>"$g_zxfer_live_destination_listing_file" 2>&1 || l_live_record_status=$?
	if [ "$l_live_record_status" -ne 0 ]; then
		zxfer_read_runtime_artifact_file_trimmed "$g_zxfer_live_destination_listing_file" || :
		g_zxfer_live_destination_record_file_error=${g_zxfer_runtime_artifact_read_result:-}
		return "$l_live_record_status"
	fi
	g_zxfer_live_destination_record_file_result=$g_zxfer_live_destination_listing_file
}

# The destination existence cache is a prepend-only newline list of
# "state<TAB>dataset" rows (state 1 = exists, 0 = missing); the newest row for
# a dataset shadows older ones. After a complete recursive listing,
# g_destination_existence_cache_root_complete=1 makes every unlisted dataset
# under g_destination_existence_cache_root read as missing.

# Purpose: Record one dataset's existence state in the cache.
# Usage: zxfer_set_destination_existence_cache_entry DATASET 1|0.
zxfer_set_destination_existence_cache_entry() {
	l_cache_entry_dataset=$1
	l_cache_entry_state=$2

	[ -n "$l_cache_entry_dataset" ] || return 0
	if [ -n "${g_destination_existence_cache:-}" ]; then
		g_destination_existence_cache="$l_cache_entry_state$ZXFER_TAB$l_cache_entry_dataset
$g_destination_existence_cache"
	else
		g_destination_existence_cache="$l_cache_entry_state$ZXFER_TAB$l_cache_entry_dataset"
	fi
}

# Purpose: Look one dataset up in the destination existence cache.
# Usage: zxfer_lookup_destination_existence_cache DATASET, in the current
# shell; on a hit returns 0 and publishes the state in
# g_zxfer_destination_existence_cache_entry_result, on a miss returns 1.
zxfer_lookup_destination_existence_cache() {
	l_cache_lookup_dataset=$1
	g_zxfer_destination_existence_cache_entry_result=""

	if [ -n "$l_cache_lookup_dataset" ] && [ -n "${g_destination_existence_cache:-}" ]; then
		# Two whole-row presence tests stay linear in the cache size. Only a
		# dataset with rows in both states needs the newest-row prefix cut,
		# which the shell evaluates in quadratic time.
		l_cache_lookup_scan="$ZXFER_LF$g_destination_existence_cache$ZXFER_LF"
		l_cache_lookup_states=""
		case $l_cache_lookup_scan in
		*"${ZXFER_LF}1$ZXFER_TAB$l_cache_lookup_dataset$ZXFER_LF"*) l_cache_lookup_states=1 ;;
		esac
		case $l_cache_lookup_scan in
		*"${ZXFER_LF}0$ZXFER_TAB$l_cache_lookup_dataset$ZXFER_LF"*) l_cache_lookup_states=${l_cache_lookup_states}0 ;;
		esac
		case $l_cache_lookup_states in
		1 | 0)
			g_zxfer_destination_existence_cache_entry_result=$l_cache_lookup_states
			return 0
			;;
		esac
		case $l_cache_lookup_scan in
		*"$ZXFER_TAB$l_cache_lookup_dataset$ZXFER_LF"*)
			l_cache_lookup_preceding=${l_cache_lookup_scan%%"$ZXFER_TAB$l_cache_lookup_dataset$ZXFER_LF"*}
			g_zxfer_destination_existence_cache_entry_result=${l_cache_lookup_preceding##*"$ZXFER_LF"}
			return 0
			;;
		esac
	fi

	if [ "${g_destination_existence_cache_root_complete:-0}" -eq 1 ] &&
		[ -n "${g_destination_existence_cache_root:-}" ]; then
		case "$l_cache_lookup_dataset" in
		"$g_destination_existence_cache_root" | "$g_destination_existence_cache_root"/*)
			g_zxfer_destination_existence_cache_entry_result=0
			return 0
			;;
		esac
	fi

	return 1
}

# Purpose: Publish a complete recursive destination listing as the destination
# dataset inventory and seed the existence cache from it.
# Usage: zxfer_seed_destination_existence_cache_from_recursive_list ROOT LIST;
# LIST becomes g_recursive_dest_list, every listed dataset exists and every
# other dataset under ROOT is missing.
zxfer_seed_destination_existence_cache_from_recursive_list() {
	l_root_dataset=$1
	l_recursive_dest_list=$2

	zxfer_reset_destination_existence_cache
	g_recursive_dest_list=$l_recursive_dest_list
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1

	while IFS= read -r l_seed_destination_existence_cache_from_recursive_list_dataset; do
		[ -n "$l_seed_destination_existence_cache_from_recursive_list_dataset" ] || continue
		zxfer_set_destination_existence_cache_entry "$l_seed_destination_existence_cache_from_recursive_list_dataset" 1
	done <<-EOF
		$l_recursive_dest_list
	EOF
}

# Purpose: Mark the destination root and its whole subtree missing.
# Usage: zxfer_mark_destination_root_missing_in_cache ROOT, after discovery
# proved the root absent; the destination dataset inventory is left empty.
zxfer_mark_destination_root_missing_in_cache() {
	l_root_dataset=$1

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1
	[ -n "$l_root_dataset" ] && zxfer_set_destination_existence_cache_entry "$l_root_dataset" 0
}

# Purpose: Mark a destination dataset and its ancestors as existing.
# Usage: zxfer_mark_destination_hierarchy_exists DATASET; stops at the cache
# root and skips datasets whose newest row already says 1, so repeated marks
# do not grow the cache.
zxfer_mark_destination_hierarchy_exists() {
	l_mark_hierarchy_dataset=$1
	l_mark_hierarchy_root=${g_destination_existence_cache_root:-}

	while [ -n "$l_mark_hierarchy_dataset" ]; do
		if ! zxfer_lookup_destination_existence_cache "$l_mark_hierarchy_dataset" ||
			[ "$g_zxfer_destination_existence_cache_entry_result" != 1 ]; then
			zxfer_set_destination_existence_cache_entry "$l_mark_hierarchy_dataset" 1
		fi
		[ "$l_mark_hierarchy_dataset" != "$l_mark_hierarchy_root" ] || break
		l_mark_hierarchy_parent=${l_mark_hierarchy_dataset%/*}
		[ "$l_mark_hierarchy_parent" != "$l_mark_hierarchy_dataset" ] || break
		l_mark_hierarchy_dataset=$l_mark_hierarchy_parent
	done
}

# Purpose: Record that a destination dataset now exists.
# Usage: zxfer_note_destination_dataset_exists DATASET; marks its hierarchy in
# the existence cache and appends it to g_recursive_dest_list once.
zxfer_note_destination_dataset_exists() {
	l_note_destination_dataset_exists_dataset=$1
	l_created_dataset=$1
	l_recursive_dest_list=${g_recursive_dest_list:-}

	[ -n "$l_note_destination_dataset_exists_dataset" ] || return

	zxfer_mark_destination_hierarchy_exists "$l_note_destination_dataset_exists_dataset"

	case "
$l_recursive_dest_list
" in
	*"
$l_created_dataset
"*) ;;
	*)
		if [ -n "$l_recursive_dest_list" ]; then
			g_recursive_dest_list="$g_recursive_dest_list
$l_created_dataset"
		else
			g_recursive_dest_list=$l_created_dataset
		fi
		;;
	esac
}

# Purpose: Record a successful destination receive in cache state.
# Usage: Called after foreground and supervised receive completion so exact
# receive targets are known-present while descendants are live-probed instead
# of inherited from an old missing-root assumption.
zxfer_note_destination_receive_completed() {
	l_note_destination_receive_completed_dataset=$1

	[ -n "$l_note_destination_receive_completed_dataset" ] || return 0
	if [ "${g_destination_existence_cache_root_complete:-0}" -eq 1 ] &&
		[ -n "${g_destination_existence_cache_root:-}" ]; then
		case "$l_note_destination_receive_completed_dataset" in
		"$g_destination_existence_cache_root" | "$g_destination_existence_cache_root"/*)
			g_destination_existence_cache_root_complete=0
			;;
		esac
	fi

	zxfer_note_destination_dataset_exists "$l_note_destination_receive_completed_dataset"
}

# Purpose: Check whether the destination probe reports missing.
# Usage: Called after destination ZFS probes to distinguish supported
# platform-specific missing-dataset diagnostics from operational failures.
zxfer_destination_probe_reports_missing() {
	l_probe_err=$1

	case "$l_probe_err" in
	*"dataset does not exist"* | *"Dataset does not exist"* | *"no such dataset"* | *"No such dataset"* | *"no such pool or dataset"* | *"No such pool or dataset"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Check whether the destination probe is ambiguous.
# Usage: Called after failed destination ZFS probes when an empty diagnostic
# may require the SunOS ancestor-listing fallback.
zxfer_destination_probe_is_ambiguous() {
	l_probe_err=$1

	case "$l_probe_err" in
	*[![:space:]]*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Confirm whether an ambiguous SunOS parent-listing failure means the
# requested parent is absent.
# Usage: zxfer_destination_parent_missing_confirmed_by_ancestor_listing
# PARENT, in the current shell; walks up recursively listing ancestors and
# caches what it proves. Returns 0 when PARENT is proven missing.
zxfer_destination_parent_missing_confirmed_by_ancestor_listing() {
	l_ancestor_probe_missing=$1
	l_ancestor_probe_original=$1

	while :; do
		l_ancestor_probe_parent=${l_ancestor_probe_missing%/*}
		[ "$l_ancestor_probe_parent" != "$l_ancestor_probe_missing" ] || return 1

		if zxfer_command_trace_enabled; then
			zxfer_trace_rendered_command "Parent recursive destination probe was ambiguous on SunOS; checking ancestor recursively" \
				"$(zxfer_render_destination_zfs_command list -H -r -o name "$l_ancestor_probe_parent")"
		else
			zxfer_record_last_command_opaque
		fi

		if l_ancestor_probe_listing=$(zxfer_run_destination_zfs_cmd list -H -r -o name "$l_ancestor_probe_parent" 2>&1); then
			if printf '%s\n' "$l_ancestor_probe_listing" | grep -F -x "$l_ancestor_probe_missing" >/dev/null 2>&1; then
				return 1
			fi

			if printf '%s\n' "$l_ancestor_probe_listing" | grep -F -x "$l_ancestor_probe_parent" >/dev/null 2>&1; then
				zxfer_mark_destination_hierarchy_exists "$l_ancestor_probe_parent"
				zxfer_set_destination_existence_cache_entry "$l_ancestor_probe_missing" 0
				zxfer_set_destination_existence_cache_entry "$l_ancestor_probe_original" 0
				return 0
			fi

			return 1
		fi

		if zxfer_destination_probe_reports_missing "$l_ancestor_probe_listing"; then
			zxfer_set_destination_existence_cache_entry "$l_ancestor_probe_missing" 0
			zxfer_set_destination_existence_cache_entry "$l_ancestor_probe_original" 0
			return 0
		fi

		zxfer_destination_probe_is_ambiguous "$l_ancestor_probe_listing" || return 1
		l_ancestor_probe_missing=$l_ancestor_probe_parent
	done
}

# Purpose: Resolve an ambiguous SunOS exact probe from a recursive listing of
# the parent.
# Usage: zxfer_exists_destination_via_parent_recursive_listing DATASET, in the
# current shell; returns 0 with g_zxfer_destination_exists_result set, 1 with
# g_zxfer_destination_exists_error set, or 2 when the fallback does not apply
# (not SunOS, or DATASET has no parent).
zxfer_exists_destination_via_parent_recursive_listing() {
	l_parent_probe_dest=$1
	l_parent_probe_parent=${l_parent_probe_dest%/*}

	case "${g_destination_operating_system:-}" in
	SunOS) ;;
	*)
		return 2
		;;
	esac

	[ "$l_parent_probe_parent" != "$l_parent_probe_dest" ] || return 2

	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Exact destination probe was ambiguous on SunOS; checking parent recursively" \
			"$(zxfer_render_destination_zfs_command list -H -r -o name "$l_parent_probe_parent")"
	else
		zxfer_record_last_command_opaque
	fi

	if l_parent_probe_listing=$(zxfer_run_destination_zfs_cmd list -H -r -o name "$l_parent_probe_parent" 2>&1); then
		if printf '%s\n' "$l_parent_probe_listing" | grep -F -x "$l_parent_probe_dest" >/dev/null 2>&1; then
			zxfer_mark_destination_hierarchy_exists "$l_parent_probe_dest"
			g_zxfer_destination_exists_result=1
			return 0
		fi

		if printf '%s\n' "$l_parent_probe_listing" | grep -F -x "$l_parent_probe_parent" >/dev/null 2>&1; then
			zxfer_mark_destination_hierarchy_exists "$l_parent_probe_parent"
			zxfer_set_destination_existence_cache_entry "$l_parent_probe_dest" 0
			g_zxfer_destination_exists_result=0
			return 0
		fi

		g_zxfer_destination_exists_error="Failed to determine whether destination dataset [$l_parent_probe_dest] exists: parent recursive listing for [$l_parent_probe_parent] did not contain the parent dataset."
		return 1
	fi

	if zxfer_destination_probe_reports_missing "$l_parent_probe_listing"; then
		zxfer_set_destination_existence_cache_entry "$l_parent_probe_parent" 0
		zxfer_set_destination_existence_cache_entry "$l_parent_probe_dest" 0
		g_zxfer_destination_exists_result=0
		return 0
	fi

	if zxfer_destination_probe_is_ambiguous "$l_parent_probe_listing" &&
		zxfer_destination_parent_missing_confirmed_by_ancestor_listing "$l_parent_probe_parent"; then
		zxfer_set_destination_existence_cache_entry "$l_parent_probe_dest" 0
		g_zxfer_destination_exists_result=0
		return 0
	fi

	if [ -n "$l_parent_probe_listing" ]; then
		g_zxfer_destination_exists_error="Failed to determine whether destination dataset [$l_parent_probe_dest] exists: parent recursive listing for [$l_parent_probe_parent] failed: $l_parent_probe_listing"
	else
		g_zxfer_destination_exists_error="Failed to determine whether destination dataset [$l_parent_probe_dest] exists: parent recursive listing for [$l_parent_probe_parent] failed."
	fi
	return 1
}

# Purpose: Decide whether a destination dataset exists, in the current shell.
# Usage: zxfer_probe_destination_existence DATASET [live]. Returns 0 with 1 or
# 0 in g_zxfer_destination_exists_result, or non-zero with the operator
# message in g_zxfer_destination_exists_error. Without "live" a cache hit
# answers without probing; probes update the cache and the -V
# exists_destination_calls counter.
zxfer_probe_destination_existence() {
	l_probe_exists_dest=$1
	l_probe_exists_mode=${2:-cache}
	g_zxfer_destination_exists_result=""
	g_zxfer_destination_exists_error=""

	if [ "$l_probe_exists_mode" != "live" ] &&
		zxfer_lookup_destination_existence_cache "$l_probe_exists_dest"; then
		zxfer_echoV "Using cached destination existence for [$l_probe_exists_dest]: $g_zxfer_destination_existence_cache_entry_result"
		g_zxfer_destination_exists_result=$g_zxfer_destination_existence_cache_entry_result
		return 0
	fi

	zxfer_profile_increment_counter g_zxfer_profile_exists_destination_calls

	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Checking if destination exists" \
			"$(zxfer_render_destination_zfs_command list -H "$l_probe_exists_dest")"
	else
		zxfer_record_last_command_opaque
	fi

	if l_probe_exists_output=$(zxfer_run_destination_zfs_cmd list -H "$l_probe_exists_dest" 2>&1); then
		zxfer_set_destination_existence_cache_entry "$l_probe_exists_dest" 1
		g_zxfer_destination_exists_result=1
		return 0
	fi

	if zxfer_destination_probe_reports_missing "$l_probe_exists_output"; then
		zxfer_set_destination_existence_cache_entry "$l_probe_exists_dest" 0
		g_zxfer_destination_exists_result=0
		return 0
	fi

	if zxfer_destination_probe_is_ambiguous "$l_probe_exists_output"; then
		l_probe_exists_fallback_status=0
		zxfer_exists_destination_via_parent_recursive_listing "$l_probe_exists_dest" ||
			l_probe_exists_fallback_status=$?
		case $l_probe_exists_fallback_status in
		0 | 1) return "$l_probe_exists_fallback_status" ;;
		esac
	fi

	if [ -n "$l_probe_exists_output" ]; then
		g_zxfer_destination_exists_error="Failed to determine whether destination dataset [$l_probe_exists_dest] exists: $l_probe_exists_output"
	else
		g_zxfer_destination_exists_error="Failed to determine whether destination dataset [$l_probe_exists_dest] exists."
	fi
	return 1
}
