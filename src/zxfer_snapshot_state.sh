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
# SNAPSHOT RECORD STATE / LOOKUPS / IDENTITIES
################################################################################

# Module contract:
# owns globals: destination-existence cache, derived snapshot-record lookup state, and the generation-gated live destination view (g_zxfer_destination_mutation_generation plus the g_zxfer_live_destination_view_* stamp/root/file trio).
# reads globals: g_cmd_awk and the flat per-run snapshot record files staged by discovery.
# mutates caches: in-memory destination-existence state, the derived reversed source record list, and the batched live destination view file.
# returns via stdout: parsed snapshot identities, per-dataset record lookups, and cache-backed existence probes.

# Snapshot discovery stages at most one flat, sorted snapshot record file per
# side inside the 0700 run-private temp root (g_zxfer_source_... and
# g_zxfer_destination_snapshot_record_cache_file). Those files ARE the
# snapshot-record index: a per-dataset lookup is a single awk prefix filter
# over the staged file, with no per-dataset cache objects, manifests, or
# readback validation in between. The files are written once by exit-status
# checked pipelines and never legitimately mutated afterwards, so a staged
# file that cannot be read back is corrupted run-private state and aborts the
# run instead of degrading to an empty snapshot list.

# Purpose: Reset the destination existence cache so the next snapshot-state
# pass starts from a clean state.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_destination_existence_cache() {
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
}

# Purpose: Reset the derived snapshot-record lookup state so the next
# snapshot-state pass starts from a clean state.
# Usage: Called during startup, discovery resets, and dry-run preview seeding
# so per-dataset lookups cannot reuse a reversed source record list derived
# from a previous pass. The flat record files themselves are owned by
# discovery (zxfer_cleanup_snapshot_record_cache_files).
zxfer_reset_snapshot_record_indexes() {
	g_lzfs_list_hr_S_snap=""
}

# Generation-gated live destination view (approved trade-off): a live view of
# the destination is valid until THIS RUN mutates the destination, and never
# across a -Y pass boundary — each replication pass starts from a fresh
# batched listing because -Y exists to converge under concurrent drift. Every
# destination mutation this run performs (receive completion including -j
# reap time, dataset create, property set/inherit, snapshot destroy, and
# rollback) bumps g_zxfer_destination_mutation_generation in the main shell.
# Live rechecks are then served from ONE batched snapshot listing of the
# run's destination root, captured lazily into
# g_zxfer_live_destination_view_file and stamped with the generation it was
# captured under; a stale stamp forces a fresh listing before the next
# recheck-driven decision. Fail closed: a failed or partial capture never
# stamps the view, so it can never be served as fresh.

# Purpose: Record that this run mutated the destination so live views older
# than the mutation are refreshed before the next recheck-driven decision.
# Usage: Called from the destination mutation choke points (receive
# completion including -j reap time in the main shell, dataset create,
# property set/inherit, snapshot destroy, and rollback).
zxfer_bump_destination_mutation_generation() {
	g_zxfer_destination_mutation_generation=$((${g_zxfer_destination_mutation_generation:-0} + 1))
	return 0
}

# Purpose: Drop the batched live destination view stamp so the next recheck
# must capture a fresh listing regardless of the mutation generation.
# Usage: Called in the main shell at the top of every -Y replication pass.
# View validity is bounded by a single pass: when a pass's last refresh
# postdates its last destination mutation, the stamp still matches the
# generation at the pass boundary, and without this reset the next pass would
# serve its first rechecks from the previous pass's listing.
zxfer_invalidate_live_destination_view() {
	g_zxfer_live_destination_view_generation=""
	g_zxfer_live_destination_view_root=""
	return 0
}

# Purpose: Capture one batched live destination snapshot listing for the view
# root and stamp it with the current destination mutation generation.
# Usage: Called by zxfer_ensure_live_destination_snapshot_view when the view
# is missing or older than this run's last destination mutation. Returns
# non-zero without stamping when the listing cannot be captured, so callers
# fail closed instead of serving a stale or partial view as fresh.
zxfer_refresh_live_destination_view() {
	l_view_refresh_root=$1

	# Semantic change with the batched view: this counter now counts batched
	# view refreshes (plus per-dataset fallback listings) instead of
	# per-dataset live rechecks. The counter key is unchanged.
	zxfer_profile_increment_counter g_zxfer_profile_live_destination_snapshot_rechecks

	# Drop the stamp before listing so a failed or partial capture can never
	# be mistaken for a fresh view.
	g_zxfer_live_destination_view_generation=""
	if [ -z "${g_zxfer_live_destination_view_file:-}" ]; then
		l_view_file_prefix="${g_zxfer_temp_prefix:-zxfer.$$}.live-dest-view"
		if ! zxfer_create_runtime_artifact_file "$l_view_file_prefix" >/dev/null; then
			return 1
		fi
		g_zxfer_live_destination_view_file=$g_zxfer_runtime_artifact_path_result
	fi

	# Non-recursive runs replicate exactly one dataset, so their batched view
	# is the same depth-1 listing the per-dataset recheck used.
	if [ "${g_option_R_recursive:-}" != "" ]; then
		set -- -Hr
	else
		set -- -H -d 1
	fi
	l_view_capture_generation=${g_zxfer_destination_mutation_generation:-0}
	if ! zxfer_run_destination_zfs_cmd list "$@" -o name,guid -t snapshot "$l_view_refresh_root" \
		>"$g_zxfer_live_destination_view_file"; then
		return 1
	fi

	g_zxfer_live_destination_view_root=$l_view_refresh_root
	g_zxfer_live_destination_view_generation=$l_view_capture_generation
	return 0
}

# Purpose: Ensure the source snapshot record cache exists and is ready before
# the flow continues.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks before later helpers assume the resource or cache is available.
zxfer_ensure_source_snapshot_record_cache() {
	[ -n "${g_lzfs_list_hr_S_snap:-}" ] && return 0
	[ -n "${g_lzfs_list_hr_snap:-}" ] || return 1

	if g_lzfs_list_hr_S_snap=$(zxfer_reverse_snapshot_record_list "$g_lzfs_list_hr_snap"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	[ -n "${g_lzfs_list_hr_S_snap:-}" ]
}

# Purpose: Filter the snapshot record file for dataset down to the subset later
# helpers should act on.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks before reconciliation, execution, or reporting consumes the reduced
# set.
zxfer_filter_snapshot_record_file_for_dataset() {
	l_snapshot_records_file=$1
	l_dataset=$2

	[ -r "$l_snapshot_records_file" ] || return 1

	# Splitting on the first "@" matches exactly "dataset@" record prefixes,
	# so sibling datasets that share a name prefix ("a/b" vs "a/bc") can
	# never collide.
	# shellcheck disable=SC2016  # awk program should see literal $1/$0.
	"${g_cmd_awk:-awk}" -F@ -v ds="$l_dataset" '$1 == ds { print $0 }' "$l_snapshot_records_file"
}

# Purpose: Return the snapshot records for dataset in the form expected by
# later helpers.
# Usage: Called during last-common-snapshot selection and delete planning when
# sibling helpers need the same per-dataset lookup without duplicating module
# logic.
zxfer_get_snapshot_records_for_dataset() {
	l_snapshot_lookup_side=$1
	l_snapshot_lookup_dataset=$2

	case "$l_snapshot_lookup_side" in
	source)
		l_snapshot_record_cache_file=${g_zxfer_source_snapshot_record_cache_file:-}
		;;
	destination)
		l_snapshot_record_cache_file=${g_zxfer_destination_snapshot_record_cache_file:-}
		;;
	*)
		return 1
		;;
	esac

	# Discovery stages one flat sorted record file per side; filtering it
	# directly is the whole per-dataset lookup.
	if [ -n "$l_snapshot_record_cache_file" ]; then
		if [ ! -r "$l_snapshot_record_cache_file" ]; then
			# Fail closed: the staged record file is run-private state that
			# is never legitimately removed while lookups can still happen,
			# so an unreadable file is corruption, not an empty snapshot
			# list.
			zxfer_throw_error "Failed to read staged $l_snapshot_lookup_side snapshot record cache."
		fi
		l_lookup_status=0
		zxfer_filter_snapshot_record_file_for_dataset \
			"$l_snapshot_record_cache_file" "$l_snapshot_lookup_dataset" ||
			l_lookup_status=$?
		return "$l_lookup_status"
	fi

	case "$l_snapshot_lookup_side" in
	source)
		if zxfer_ensure_source_snapshot_record_cache; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_lzfs_list_hr_S_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_snapshot_lookup_dataset" '$1 == ds { print $0 }'
		;;
	destination)
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_rzfs_list_hr_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_snapshot_lookup_dataset" '$1 == ds { print $0 }'
		;;
	esac
}

# Purpose: Update the destination existence cache entry in the shared runtime
# state.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks after a probe or planning step changes the active context that later
# helpers should use.
#
# The cache is a prepend-only newline list of "state<TAB>dataset" rows: a
# mutation is one O(1) string prepend, and the newest row for a dataset
# shadows older rows, so invalidation just prepends authoritative entries.
zxfer_set_destination_existence_cache_entry() {
	l_dataset=$1
	l_exists_state=$2

	[ -n "$l_dataset" ] || return 0
	if [ -n "${g_destination_existence_cache:-}" ]; then
		g_destination_existence_cache="$l_exists_state	$l_dataset
$g_destination_existence_cache"
	else
		g_destination_existence_cache="$l_exists_state	$l_dataset"
	fi
}

# Purpose: Return the destination existence cache entry in the form expected by
# later helpers.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
#
# First match scanning top-down wins: rows are prepended, so the earliest
# "<TAB>dataset<NL>" needle hit (dataset names cannot contain tab/newline)
# is the newest shadowing row, and the text between the preceding newline
# and that tab is its state. Misses return 1 so callers live-probe.
zxfer_get_destination_existence_cache_entry() {
	l_dataset=$1
	l_nl='
'

	if [ -n "$l_dataset" ] && [ -n "${g_destination_existence_cache:-}" ]; then
		l_cache_scan="$g_destination_existence_cache$l_nl"
		case "$l_cache_scan" in
		*"	$l_dataset$l_nl"*)
			l_preceding=${l_cache_scan%%"	$l_dataset$l_nl"*}
			printf '%s\n' "${l_preceding##*"$l_nl"}"
			return 0
			;;
		esac
	fi

	if [ "${g_destination_existence_cache_root_complete:-0}" -eq 1 ] &&
		[ -n "${g_destination_existence_cache_root:-}" ]; then
		case "$l_dataset" in
		"$g_destination_existence_cache_root" | "$g_destination_existence_cache_root"/*)
			printf '%s\n' 0
			return 0
			;;
		esac
	fi

	return 1
}

# Purpose: Seed the destination existence cache from recursive list so
# incremental work can continue from a valid base.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when zxfer must bootstrap a destination before sending the remaining
# range.
zxfer_seed_destination_existence_cache_from_recursive_list() {
	l_root_dataset=$1
	l_recursive_dest_list=$2

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1

	while IFS= read -r l_dataset; do
		[ -n "$l_dataset" ] || continue
		zxfer_set_destination_existence_cache_entry "$l_dataset" 1
	done <<-EOF
		$l_recursive_dest_list
	EOF
}

# Purpose: Mark the destination root missing in cache in the module-owned
# state.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks so later helpers can make decisions from one shared marker instead of
# re-deriving it.
zxfer_mark_destination_root_missing_in_cache() {
	l_root_dataset=$1

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1
	[ -n "$l_root_dataset" ] && zxfer_set_destination_existence_cache_entry "$l_root_dataset" 0
}

# Purpose: Mark the destination hierarchy exists in the module-owned state.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks so later helpers can make decisions from one shared marker instead of
# re-deriving it.
zxfer_mark_destination_hierarchy_exists() {
	l_dataset=$1
	l_cache_root=${g_destination_existence_cache_root:-}

	while [ -n "$l_dataset" ]; do
		zxfer_set_destination_existence_cache_entry "$l_dataset" 1
		if [ -n "$l_cache_root" ] && [ "$l_dataset" = "$l_cache_root" ]; then
			break
		fi
		l_parent_dataset=${l_dataset%/*}
		[ "$l_parent_dataset" = "$l_dataset" ] && break
		l_dataset=$l_parent_dataset
	done
}

# Purpose: Record the destination dataset exists for later diagnostics or
# control decisions.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when zxfer needs the state preserved for follow-on helpers or
# reporting.
zxfer_note_destination_dataset_exists() {
	l_dataset=$1
	l_created_dataset=$1
	l_recursive_dest_list=${g_recursive_dest_list:-}

	[ -n "$l_dataset" ] || return

	zxfer_mark_destination_hierarchy_exists "$l_dataset"

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
	l_dataset=$1

	[ -n "$l_dataset" ] || return 0
	if [ "${g_destination_existence_cache_root_complete:-0}" -eq 1 ] &&
		[ -n "${g_destination_existence_cache_root:-}" ]; then
		case "$l_dataset" in
		"$g_destination_existence_cache_root" | "$g_destination_existence_cache_root"/*)
			g_destination_existence_cache_root_complete=0
			;;
		esac
	fi

	zxfer_note_destination_dataset_exists "$l_dataset"
}

# Purpose: Extract the snapshot path from the serialized input this module
# works with.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
zxfer_extract_snapshot_path() {
	l_snapshot_record=$1
	l_tab='	'

	case "$l_snapshot_record" in
	*"$l_tab"*)
		printf '%s\n' "${l_snapshot_record%%	*}"
		;;
	*)
		printf '%s\n' "$l_snapshot_record"
		;;
	esac
}

# Purpose: Extract the snapshot name from the serialized input this module
# works with.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
#
# Function to extract snapshot name
zxfer_extract_snapshot_name() {
	l_snapshot_path=$(zxfer_extract_snapshot_path "$1")

	case "$l_snapshot_path" in
	*@*)
		printf '%s\n' "${l_snapshot_path#*@}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

# Purpose: Extract the snapshot dataset from the serialized input this module
# works with.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
zxfer_extract_snapshot_dataset() {
	l_snapshot_path=$(zxfer_extract_snapshot_path "$1")

	case "$l_snapshot_path" in
	*@*)
		printf '%s\n' "${l_snapshot_path%@*}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

# Purpose: Extract the snapshot guid from the serialized input this module
# works with.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
zxfer_extract_snapshot_guid() {
	l_snapshot_record=$1
	l_tab='	'

	case "$l_snapshot_record" in
	*"$l_tab"*)
		printf '%s\n' "${l_snapshot_record#*	}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

# Purpose: Extract the snapshot identity from the serialized input this module
# works with.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
zxfer_extract_snapshot_identity() {
	l_snapshot_name=$(zxfer_extract_snapshot_name "$1")
	l_snapshot_guid=$(zxfer_extract_snapshot_guid "$1")

	[ -n "$l_snapshot_name" ] || {
		printf '%s\n' ""
		return
	}

	if [ -n "$l_snapshot_guid" ]; then
		printf '%s\t%s\n' "$l_snapshot_name" "$l_snapshot_guid"
	else
		printf '%s\n' "$l_snapshot_name"
	fi
}

# Purpose: Normalize the snapshot record list into the stable form used across
# zxfer.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks before comparison, caching, or reporting depends on exact formatting.
zxfer_normalize_snapshot_record_list() {
	printf '%s\n' "$1" | tr ' ' '\n'
}

# Purpose: Check whether the snapshot record list contains guid.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need a boolean answer about the snapshot record
# list.
zxfer_snapshot_record_list_contains_guid() {
	l_tab='	'
	case "$1" in
	*"$l_tab"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Reverse the snapshot record list while preserving the record
# structure later helpers rely on.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when comparison or replay logic needs the same data in the opposite
# order.
zxfer_reverse_snapshot_record_list() {
	l_snapshot_records=$1

	[ -n "$l_snapshot_records" ] || return 0

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	printf '%s\n' "$l_snapshot_records" | "${g_cmd_awk:-awk}" '{ l_records[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) if (l_records[l_i] != "") print l_records[l_i] }'
}

# Purpose: Transform snapshot records through a staged file and read the result.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need a checked reload after normalizing or reversing
# record lists.
zxfer_read_transformed_snapshot_record_list() {
	l_snapshot_records=$1
	l_snapshot_record_transform=$2

	g_zxfer_runtime_artifact_read_result=""
	[ -n "$l_snapshot_records" ] || return 0
	case "$l_snapshot_record_transform" in
	normalized)
		zxfer_capture_runtime_artifact_command_output \
			"zxfer-snapshot-records" \
			zxfer_normalize_snapshot_record_list "$l_snapshot_records"
		;;
	reversed)
		zxfer_capture_runtime_artifact_command_output \
			"zxfer-snapshot-records" \
			zxfer_reverse_snapshot_record_list "$l_snapshot_records"
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Read the normalized snapshot record list from staged state into the
# current shell.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_normalized_snapshot_record_list() {
	zxfer_read_transformed_snapshot_record_list "$1" normalized
}

# Purpose: Read the reversed snapshot record list from staged state into the
# current shell.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_reversed_snapshot_record_list() {
	zxfer_read_transformed_snapshot_record_list "$1" reversed
}

# Purpose: Check whether the snapshot record lists share snapshot name.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when later helpers need a boolean answer about the snapshot record
# lists.
zxfer_snapshot_record_lists_share_snapshot_name() {
	l_source_records=$1
	l_destination_records=$2
	l_section_break="@@ZXFER_SNAPSHOT_NAME_SET_BREAK@@"
	l_overlap_awk=$(
		cat <<'EOF'
BEGIN { in_source = 0 }
$0 == section_break {
	in_source = 1
	next
}
!in_source {
	if ($0 != "") {
		record = $0
		tab_pos = index(record, "\t")
		snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
		at_pos = index(snapshot_path, "@")
		if (at_pos > 0)
			destination_names[substr(snapshot_path, at_pos + 1)] = 1
	}
	next
}
$0 != "" {
	record = $0
	tab_pos = index(record, "\t")
	snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
	at_pos = index(snapshot_path, "@")
	if (at_pos > 0) {
		snapshot_name = substr(snapshot_path, at_pos + 1)
		if (snapshot_name in destination_names)
			found = 1
	}
}
END { exit(found ? 0 : 1) }
EOF
	)

	{
		zxfer_normalize_snapshot_record_list "$l_destination_records"
		printf '%s\n' "$l_section_break"
		zxfer_normalize_snapshot_record_list "$l_source_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_overlap_awk"
}

# Purpose: Filter the snapshot identity records to reference paths down to the
# subset later helpers should act on.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks before reconciliation, execution, or reporting consumes the reduced
# set.
zxfer_filter_snapshot_identity_records_to_reference_paths() {
	l_identity_records=$1
	l_reference_records=$2
	l_section_break="@@ZXFER_SNAPSHOT_PATH_FILTER_BREAK@@"
	l_filter_awk=$(
		cat <<'EOF'
BEGIN { in_identity = 0 }
$0 == section_break {
	in_identity = 1
	next
}
!in_identity {
	if ($0 != "") {
		record = $0
		tab_pos = index(record, "\t")
		snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
		reference_paths[snapshot_path] = 1
	}
	next
}
$0 != "" {
	record = $0
	tab_pos = index(record, "\t")
	snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
	if (snapshot_path in reference_paths)
		print record
}
EOF
	)

	{
		zxfer_normalize_snapshot_record_list "$l_reference_records"
		printf '%s\n' "$l_section_break"
		zxfer_normalize_snapshot_record_list "$l_identity_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_filter_awk"
}

# Purpose: Return the source snapshot identity records for dataset in the form
# expected by later helpers.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_source_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if l_snapshot_records=$(zxfer_run_source_zfs_cmd list -H -o name,guid -s creation -d 1 -t snapshot "$l_dataset"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	if zxfer_read_normalized_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_snapshot_records=$g_zxfer_runtime_artifact_read_result

	if zxfer_read_reversed_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	printf '%s' "$g_zxfer_runtime_artifact_read_result"
}

# Purpose: Return the destination snapshot identity records for dataset in the
# form expected by later helpers.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_destination_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	# Filtered to "$l_dataset"@* below, so list at depth 1 rather than
	# recursively (mirrors the source side in
	# zxfer_get_source_snapshot_identity_records_for_dataset). Avoids pulling the
	# whole destination subtree just to drop the descendant snapshots.
	if l_snapshot_records=$(zxfer_run_destination_zfs_cmd list -H -d 1 -o name,guid -t snapshot "$l_dataset"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	if zxfer_read_normalized_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	l_filtered_identity_records=""
	while IFS= read -r l_snapshot_record; do
		[ -n "$l_snapshot_record" ] || continue
		l_snapshot_path=$(zxfer_extract_snapshot_path "$l_snapshot_record")
		case "$l_snapshot_path" in
		"$l_dataset"@*)
			if [ -n "$l_filtered_identity_records" ]; then
				l_filtered_identity_records=$l_filtered_identity_records'
'$l_snapshot_record
			else
				l_filtered_identity_records=$l_snapshot_record
			fi
			;;
		esac
	done <<EOF
$g_zxfer_runtime_artifact_read_result
EOF

	printf '%s' "$l_filtered_identity_records"
}

# Purpose: Return the snapshot identity records for dataset in the form
# expected by later helpers.
# Usage: Called during snapshot lookups, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_snapshot_identity_records_for_dataset() {
	l_side=$1
	l_dataset=$2
	l_reference_records=${3:-}

	case "$l_side" in
	source)
		if l_identity_records=$(zxfer_get_source_snapshot_identity_records_for_dataset "$l_dataset"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		;;
	destination)
		if l_identity_records=$(zxfer_get_destination_snapshot_identity_records_for_dataset "$l_dataset"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		;;
	*)
		return 1
		;;
	esac

	if [ -n "$l_reference_records" ]; then
		zxfer_filter_snapshot_identity_records_to_reference_paths "$l_identity_records" "$l_reference_records"
	else
		printf '%s\n' "$l_identity_records"
	fi
}
