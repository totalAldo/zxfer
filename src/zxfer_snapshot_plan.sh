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
# SNAPSHOT PLAN: PASS PLANS / DELETES / DIVERGENCE / LIVE RE-PLAN
################################################################################

# Module contract:
# owns globals: the per-dataset plan published for replication
#   (g_last_common_snap, g_src_snapshot_transfer_first,
#   g_src_snapshot_transfer_final, g_dest_has_snapshots, g_zxfer_transfer_plan_file),
#   written only by zxfer_publish_snapshot_transfer_plan, the delete/rollback
#   markers (g_did_delete_dest_snapshots, g_deleted_dest_newer_snapshots), the
#   g_zxfer_plan_* results of zxfer_plan_dataset_snapshots, the run-scoped
#   scratch files g_zxfer_snapshot_plan_file, g_zxfer_snapshot_recheck_file,
#   g_zxfer_snapshot_creation_file and g_zxfer_snapshot_slice_base, immutable
#   pass plans and source slices, and the current dataset's selection
#   (g_zxfer_snapshot_slice_records, _key, _source, _destination), and the
#   divergence contract
#   (g_zxfer_diverged_snapshot_count, g_zxfer_diverged_snapshot_examples,
#   g_zxfer_diverged_converged_datasets,
#   g_zxfer_diverged_converged_marker_source).
# reads globals: g_actual_dest (the dataset being planned), g_cmd_awk,
#   g_initial_source, g_zxfer_run_umask (restored after the slices are
#   written), g_option_d_delete_destination_snapshots,
#   g_option_F_force_rollback, g_option_g_grandfather_protection, verbosity, and
#   discovery's snapshot record files.
# mutates caches: none; after a destroy g_did_delete_dest_snapshots tells
#   replication to mark the pass as having done work.
# returns via stdout: creation-date display text, divergence examples and
#   verbose transfer skip reasons; plans and destroy targets use owned state.
#
# Replication prepares the pass plans once, loads each with
# zxfer_inspect_delete_snap, and re-plans a dataset whose -d destroy ran with
# zxfer_reconcile_live_destination_snapshot_state before its send, and
# records a seed receive as the new anchor through
# zxfer_publish_snapshot_transfer_plan.

# Purpose: Forget the run-scoped snapshot plan, creation-time and slice files.
# Usage: Called by session initialization; the next use allocates a fresh
# file under the run root.
zxfer_reset_snapshot_delete_artifact_state() {
	g_zxfer_snapshot_plan_file=""
	g_zxfer_snapshot_recheck_file=""
	g_zxfer_snapshot_creation_file=""
	g_zxfer_snapshot_slice_base=""
	zxfer_forget_snapshot_slices
}

# Purpose: Forget the prepared pass and the current dataset's selection.
# Usage: zxfer_forget_snapshot_slices; the planner then reads the whole
# record files until the next preparation.
zxfer_forget_snapshot_slices() {
	g_zxfer_snapshot_slice_records=""
	g_zxfer_snapshot_slice_key=""
	g_zxfer_snapshot_slice_source=""
	g_zxfer_snapshot_slice_destination=""
}

# Purpose: Reset the per-dataset plan, delete, and divergence state.
# Usage: Called by session initialization.
zxfer_reset_snapshot_plan_state() {
	zxfer_publish_snapshot_transfer_plan "" "" "" 0 ""
	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0
	g_zxfer_plan_common_snapshot=""
	g_zxfer_plan_transfer_first=""
	g_zxfer_plan_transfer_final=""
	g_zxfer_plan_dest_has_snapshots=0
	g_zxfer_plan_file=""
	g_zxfer_plan_destroy_target=""
	g_zxfer_plan_source_count=0
	# Per-dataset divergence scratch and the run-level "diverged and
	# converged this run" markers read by post-receive verification.
	g_zxfer_diverged_snapshot_count=0
	g_zxfer_diverged_snapshot_examples=""
	g_zxfer_diverged_converged_datasets=""
	g_zxfer_diverged_converged_marker_source=""
}

# Purpose: Publish the current dataset's transfer range and exact eligibility.
# Usage: zxfer_publish_snapshot_transfer_plan COMMON FIRST FINAL 0|1 PLAN_FILE.
# Records include the GUID. PLAN_FILE retains the original common/send records
# for exact live-anchor membership checks; it must survive other datasets'
# receive verification. Only this function writes the active transfer state.
zxfer_publish_snapshot_transfer_plan() {
	case ${4:-0} in
	0 | 1) ;;
	*) return 2 ;;
	esac
	g_last_common_snap=${1:-}
	g_src_snapshot_transfer_first=${2:-}
	g_src_snapshot_transfer_final=${3:-}
	g_dest_has_snapshots=$4
	g_zxfer_transfer_plan_file=${5:-}
}

# Key each listed dataset by its queue position, preserving both sides' row
# order. The operands are "side=list - side=destination DEST_FILE
# side=source SOURCE_FILE". Output is "POSITION<TAB>SIDE<TAB>LINE<TAB>row";
# SIDE is 1 for destination and 2 for source, LINE 0 marks an empty side.
# Exact dataset keys prevent prefix siblings from sharing rows. Mapping uses
# ENVIRON so names never pass through awk -v escape interpretation. Invalid
# identity stays in the stream for the planner to fail at its dataset boundary.
# shellcheck disable=SC2016  # awk program should see literal $0.
ZXFER_SNAPSHOT_SLICE_KEY_AWK='
BEGIN {
	initial_source = ENVIRON["ZXFER_AWK_INITIAL_SOURCE"]
	destination_root = ENVIRON["ZXFER_AWK_DESTINATION_ROOT"]
	root_length = length(destination_root)
}
side == "list" {
	tab = index($0, "\t")
	if (tab < 2)
		next
	position = substr($0, 1, tab - 1)
	key[substr($0, tab + 1)] = position
	print position "\t1\t0\t"
	print position "\t2\t0\t"
	next
}
{
	at = index($0, "@")
	if (at < 2)
		next
	dataset = substr($0, 1, at - 1)
}
side == "destination" {
	if (dataset == destination_root)
		dataset = initial_source
	else if (substr(dataset, 1, root_length + 1) == destination_root "/")
		dataset = initial_source substr(dataset, root_length + 1)
	else
		next
	if (dataset in key)
		print key[dataset] "\t1\t" FNR "\t" $0
	next
}
dataset in key {
	print key[dataset] "\t2\t" FNR "\t" $0
}'

# The grouped consumer shares the dataset planner below. It retains only one
# dataset's arrays and two open files: its source slice for live rechecks and
# its prepared plan. A guid-less row records error 3 in that dataset's plan;
# the shell reports it at the original dataset/preflight boundary. File errors
# fail the whole preparation, as they did when writing the input slices.
# shellcheck disable=SC2016 # awk reads literal fields and ENVIRON.
ZXFER_SNAPSHOT_GROUP_PLAN_AWK='
BEGIN { base = ENVIRON["ZXFER_AWK_SLICE_BASE"] }
function finish_group() {
	if (position == "") return
	plan_finish()
	if (close(plan_file) != 0) failed = 1
	if (close(source_file) != 0) failed = 1
}
{
	tab = index($0, "\t")
	next_position = substr($0, 1, tab - 1)
	rest = substr($0, tab + 1)
	tab = index(rest, "\t")
	role = substr(rest, 1, tab - 1)
	rest = substr(rest, tab + 1)
	tab = index(rest, "\t")
	line = substr(rest, 1, tab - 1)
	record = substr(rest, tab + 1)
	if (next_position != position) {
		finish_group()
		if (failed) exit 2
		plan_reset()
		position = next_position
		plan_file = base "." position ".p"
		source_file = base "." position ".s"
		printf "" >plan_file
		printf "" >source_file
	}
	if (line == "0") next
	if (role == "2") print record >source_file
	plan_record(record, role == "1" ? "destination" : "source")
}
END {
	if (!failed) finish_group()
	if (failed) exit 2
}'

# Purpose: Prepare all snapshot plans and source slices in one grouped pass.
# Usage: zxfer_prepare_snapshot_plans LIST, with "POSITION<TAB>SOURCE" rows.
# One key/sort/group traversal writes BASE.POSITION.p plans and .s source
# slices (0600). The group planner retains one dataset's snapshot arrays;
# the key index holds one entry per listed dataset. A malformed identity is
# stored as error 3, reported only when that dataset is inspected. File/stage
# failures return their original status and publish no prepared plans.
# Without readable input or listed datasets, publish nothing; direct planning
# then reads and diagnoses the original record files as before.
zxfer_prepare_snapshot_plans() {
	l_split_list=$1
	l_split_source_file=${g_zxfer_source_snapshot_record_cache_file:-}
	l_split_destination_file=${g_zxfer_destination_snapshot_record_cache_file:-}

	zxfer_forget_snapshot_slices
	[ -n "$l_split_list" ] || return 0
	[ -r "$l_split_source_file" ] && [ -r "$l_split_destination_file" ] ||
		return 0
	zxfer_ensure_runtime_artifact_file "${g_zxfer_snapshot_slice_base:-}" \
		zxfer-snapshot-slices || return
	g_zxfer_snapshot_slice_base=$g_zxfer_runtime_artifact_path_result

	# Names travel through the environment: awk -v would reinterpret
	# backslash escapes.
	zxfer_map_destination_dataset
	l_split_status=0
	ZXFER_AWK_INITIAL_SOURCE=$g_initial_source \
		ZXFER_AWK_DESTINATION_ROOT=$g_zxfer_destination_dataset_result \
		"${g_cmd_awk:-awk}" "$ZXFER_SNAPSHOT_SLICE_KEY_AWK" side=list - \
		side=destination "$l_split_destination_file" \
		side=source "$l_split_source_file" \
		>|"$g_zxfer_snapshot_slice_base" <<EOF || l_split_status=$?
$l_split_list
EOF
	# POSIX lets sort -o name its own input file.
	[ "$l_split_status" -ne 0 ] ||
		LC_ALL=C sort -t "$ZXFER_TAB" -k1,1n -k2,2n -k3,3n \
			-o "$g_zxfer_snapshot_slice_base" "$g_zxfer_snapshot_slice_base" ||
		l_split_status=$?
	if [ "$l_split_status" -eq 0 ]; then
		# Never restore an empty umask: ksh reads it as 0777.
		l_split_umask=${g_zxfer_run_umask:-}
		case $l_split_umask in
		'' | *[!0-7]*) l_split_umask=$(umask) ;;
		esac
		umask 077
		ZXFER_AWK_SLICE_BASE=$g_zxfer_snapshot_slice_base \
			ZXFER_AWK_PLAN_VERBOSE=${g_option_v_verbose:-0} \
			"${g_cmd_awk:-awk}" "$ZXFER_SNAPSHOT_PLAN_AWK$ZXFER_SNAPSHOT_GROUP_PLAN_AWK" \
			"$g_zxfer_snapshot_slice_base" || l_split_status=$?
		umask "$l_split_umask"
	fi
	[ "$l_split_status" -eq 0 ] || return "$l_split_status"
	# The keyed copy is spent; a memory-backed temp root need not hold it.
	zxfer_write_runtime_artifact_file "$g_zxfer_snapshot_slice_base" "" || :
	g_zxfer_snapshot_slice_records=$l_split_source_file$ZXFER_LF$l_split_destination_file
}

# Purpose: Select one listed dataset's prepared plan and source slice.
# Usage: zxfer_select_snapshot_plan POSITION SOURCE, immediately before SOURCE
# is inspected. Without a published pass or numeric position, select nothing.
zxfer_select_snapshot_plan() {
	g_zxfer_snapshot_slice_key=""
	g_zxfer_snapshot_slice_source=""
	g_zxfer_snapshot_slice_destination=""
	[ -n "${g_zxfer_snapshot_slice_records:-}" ] || return 0
	zxfer_is_uint "${1:-}" || return 0
	zxfer_map_destination_dataset "$2"
	g_zxfer_snapshot_slice_key=$1
	g_zxfer_snapshot_slice_source=$2
	g_zxfer_snapshot_slice_destination=$g_zxfer_destination_dataset_result
}

# Shared planning algorithm for grouped preparation and a single live recheck.
# Input records are "dataset@snapshot<TAB>guid", source newest first. Identity
# requires name AND guid. Exact common/send records stay file-backed (source
# order), as do deletes (destination order). Summaries publish first/final,
# divergence count and three examples, destroy target and destination presence.
# A fixed-order summary block follows the identity rows; its final marker
# makes partial output invalid. Empty values and record tabs remain literal.
# Invalid identity emits error 3 without a completion row. The grouped driver
# continues to later datasets; the single-dataset driver returns 3 immediately.
# POSIX awk deletes entries individually to keep memory bounded by one dataset.
# shellcheck disable=SC2016 # awk reads literal fields and ENVIRON.
ZXFER_SNAPSHOT_PLAN_AWK='
BEGIN { plan_verbose = ENVIRON["ZXFER_AWK_PLAN_VERBOSE"] == "1" }
function plan_reset(    i) {
	for (i in destination_path) delete destination_path[i]
	for (i in destination_id) delete destination_id[i]
	for (i in destination_ids) delete destination_ids[i]
	for (i in destination_guid) delete destination_guid[i]
	for (i in source_ids) delete source_ids[i]
	destination_count = source_count = found_common = missing_guid = diverged_count = 0
	first = final = ""
}
function plan_record(record, role,    at, tail, tab, name, guid, next_at) {
	if (missing_guid) return
	at = index(record, "@")
	tail = substr(record, at + 1)
	# Keep the original -F@ field interpretation for malformed input too.
	next_at = index(tail, "@")
	if (next_at) tail = substr(tail, 1, next_at - 1)
	tab = index(tail, "\t")
	if (tab < 2 || tab == length(tail)) {
		missing_guid = 1
		return
	}
	name = substr(tail, 1, tab - 1)
	guid = substr(tail, tab + 1)
	if (role == "destination") {
		destination_count++
		destination_path[destination_count] = substr(record, 1, at) name
		destination_id[destination_count] = name "\t" guid
		destination_ids[name "\t" guid] = 1
		destination_guid[name] = guid
		return
	}
	source_count++
	source_ids[name "\t" guid] = 1
	if ((name in destination_guid) && destination_guid[name] != guid) {
		diverged_count++
		if (diverged_count <= 3)
			print "diverged\t" name "\t" guid "\t" destination_guid[name] > plan_file
	}
	if (found_common) {
		# Missing identities before the anchor are discovery differences, but
		# outside the incremental send range. Retain reasons only under -v.
		if (plan_verbose && !((name "\t" guid) in destination_ids))
			print "skip\tbefore_common\t" record > plan_file
		return
	}
	if ((name "\t" guid) in destination_ids) {
		found_common = 1
		print "common\t" record > plan_file
		return
	}
	# Source records arrive newest first. Keep endpoints in memory and exact
	# pending identities on disk, without an array or a shell payload copy.
	if (final == "") final = record
	first = record
	print "send\t" record > plan_file
}
function plan_finish(    i, target, at) {
	if (missing_guid) {
		print "error\t3" > plan_file
		return 3
	}
	for (i = 1; i <= destination_count; i++)
		if (!(destination_id[i] in source_ids))
			print "delete\t" destination_path[i] > plan_file
	# Preserve the old reverse delete-list order in the single destroy argv.
	target = ""
	for (i = destination_count; i >= 1; i--)
		if (!(destination_id[i] in source_ids)) {
			at = index(destination_path[i], "@")
			if (target == "") target = substr(destination_path[i], 1, at)
			else target = target ","
			target = target substr(destination_path[i], at + 1)
		}
	print "__ZXFER_SNAPSHOT_SUMMARY__" > plan_file
	print first ORS final ORS target > plan_file
	print (destination_count > 0 ? 1 : 0) ORS (source_count + 0) ORS (diverged_count + 0) > plan_file
	print "__ZXFER_SNAPSHOT_PLAN__" > plan_file
	return 0
}'
# shellcheck disable=SC2016 # Append the single-dataset awk driver literally.
ZXFER_PLAN_DATASET_SNAPSHOTS_AWK=$ZXFER_SNAPSHOT_PLAN_AWK'
BEGIN { plan_file = ENVIRON["ZXFER_AWK_PLAN_FILE"] }
$1 == (side == "destination" ? destination_dataset : source_dataset) {
	plan_record($0, side)
	if (missing_guid) exit 3
}
END { if (plan_finish() != 0) exit 3 }'

# Purpose: Read the identity examples and fixed-order summary of a plan.
# Usage: zxfer_load_snapshot_plan_summary PLAN_FILE, after clearing draft state.
# Returns: 0 for a complete summary, 3 for missing GUIDs, 1 for unreadable or
# incomplete output. Reads whole lines so empty endpoints and literal tabs
# survive; no awk or shell evaluation is needed. Active state is never written.
zxfer_load_snapshot_plan_summary() {
	[ -r "$1" ] || return 1
	while IFS= read -r l_plan_summary_row; do
		case $l_plan_summary_row in
		"common	"*) g_zxfer_plan_common_snapshot=${l_plan_summary_row#common	} ;;
		"diverged	"*)
			g_zxfer_diverged_snapshot_examples=${g_zxfer_diverged_snapshot_examples:+$g_zxfer_diverged_snapshot_examples$ZXFER_LF}${l_plan_summary_row#diverged	}
			;;
		"error	3") return 3 ;;
		"error	"*) return 1 ;;
		__ZXFER_SNAPSHOT_SUMMARY__)
			IFS= read -r g_zxfer_plan_transfer_first &&
				IFS= read -r g_zxfer_plan_transfer_final &&
				IFS= read -r g_zxfer_plan_destroy_target &&
				IFS= read -r g_zxfer_plan_dest_has_snapshots &&
				IFS= read -r g_zxfer_plan_source_count &&
				IFS= read -r g_zxfer_diverged_snapshot_count &&
				IFS= read -r l_plan_summary_row &&
				[ "$l_plan_summary_row" = __ZXFER_SNAPSHOT_PLAN__ ] &&
				zxfer_is_uint "$g_zxfer_plan_source_count" &&
				zxfer_is_uint "$g_zxfer_diverged_snapshot_count" || return 1
			case $g_zxfer_plan_dest_has_snapshots in 0 | 1) ;; *) return 1 ;; esac
			# The completion marker must end the file, including any partial row.
			l_plan_summary_row=""
			if IFS= read -r l_plan_summary_row || [ -n "$l_plan_summary_row" ]; then
				return 1
			fi
			return 0
			;;
		esac
	done <"$1"
	return 1
}

# Purpose: Load one prepared plan, or plan a live/direct dataset with one awk.
# Usage: zxfer_plan_dataset_snapshots SOURCE DEST [DEST_RECORD_FILE]. A selected
# plan is reused only while source, destination and input-file provenance match.
# An explicit destination file always re-plans, using the selected source slice
# when applicable; unselected callers read the whole immutable record files.
# Publishes common/first/final records, destination presence, source count,
# destroy target, the checked plan path, and the capped divergence summary.
# Active transfer state is separate. Two reusable scratch files keep even a
# direct caller's active plan intact during another receive's verification.
# Aborts with the original status on invalid identity or incomplete output.
zxfer_plan_dataset_snapshots() {
	l_plan_source=$1
	l_plan_dest=$2
	l_plan_source_file=${g_zxfer_source_snapshot_record_cache_file:-}
	l_plan_dest_file=${g_zxfer_destination_snapshot_record_cache_file:-}
	l_plan_file=""
	if [ -n "${g_zxfer_snapshot_slice_key:-}" ] &&
		[ "$l_plan_source" = "$g_zxfer_snapshot_slice_source" ] &&
		[ "$l_plan_dest" = "$g_zxfer_snapshot_slice_destination" ] &&
		[ "$g_zxfer_snapshot_slice_records" = "$l_plan_source_file$ZXFER_LF$l_plan_dest_file" ]; then
		l_plan_source_file=$g_zxfer_snapshot_slice_base.$g_zxfer_snapshot_slice_key.s
		[ -n "${3:-}" ] || l_plan_file=$g_zxfer_snapshot_slice_base.$g_zxfer_snapshot_slice_key.p
	fi
	[ -z "${3:-}" ] || l_plan_dest_file=$3

	[ -r "$l_plan_source_file" ] ||
		zxfer_throw_error "Failed to read staged source snapshot record cache."
	[ -r "$l_plan_dest_file" ] ||
		zxfer_throw_error "Failed to read staged destination snapshot record cache."
	l_plan_status=0
	if [ -z "$l_plan_file" ]; then
		# Live rechecks and direct callers use the same algorithm. Their result
		# stays separate from the immutable pass plans and active transfer state.
		if [ -n "${g_zxfer_snapshot_plan_file:-}" ] &&
			[ "$g_zxfer_snapshot_plan_file" = "${g_zxfer_transfer_plan_file:-}" ]; then
			zxfer_ensure_runtime_artifact_file "${g_zxfer_snapshot_recheck_file:-}" zxfer-snapshot-plan ||
				zxfer_throw_error "Failed to allocate the snapshot plan file." "$?"
			g_zxfer_snapshot_recheck_file=$g_zxfer_runtime_artifact_path_result
		else
			zxfer_ensure_runtime_artifact_file "${g_zxfer_snapshot_plan_file:-}" zxfer-snapshot-plan ||
				zxfer_throw_error "Failed to allocate the snapshot plan file." "$?"
			g_zxfer_snapshot_plan_file=$g_zxfer_runtime_artifact_path_result
		fi
		l_plan_file=$g_zxfer_runtime_artifact_path_result
		ZXFER_AWK_PLAN_FILE=$l_plan_file \
			ZXFER_AWK_PLAN_VERBOSE=${g_option_v_verbose:-0} \
			"${g_cmd_awk:-awk}" -F@ -v source_dataset="$l_plan_source" \
			-v destination_dataset="$l_plan_dest" "$ZXFER_PLAN_DATASET_SNAPSHOTS_AWK" \
			side=destination "$l_plan_dest_file" side=source "$l_plan_source_file" ||
			l_plan_status=$?
	fi

	g_zxfer_plan_common_snapshot=""
	g_zxfer_plan_transfer_first=""
	g_zxfer_plan_transfer_final=""
	g_zxfer_plan_file=""
	g_zxfer_plan_destroy_target=""
	g_zxfer_plan_source_count=""
	g_zxfer_plan_dest_has_snapshots=""
	g_zxfer_diverged_snapshot_count=""
	g_zxfer_diverged_snapshot_examples=""
	[ "$l_plan_status" -ne 0 ] ||
		zxfer_load_snapshot_plan_summary "$l_plan_file" || l_plan_status=$?
	if [ "$l_plan_status" -ne 0 ]; then
		zxfer_throw_error "Failed to determine the last common snapshot for [$l_plan_source] and [$l_plan_dest]." "$l_plan_status"
	fi
	g_zxfer_plan_file=$l_plan_file
}

# Purpose: Explain missing identities outside a checked plan's send range.
# Usage: zxfer_report_snapshot_transfer_skips SOURCE DEST PLAN_FILE COMMON;
# verbose only, after divergence enforcement. Reads diagnostic rows without
# changing the active plan or performing ZFS queries; unreadable plans abort.
zxfer_report_snapshot_transfer_skips() {
	[ "${g_option_v_verbose:-0}" -eq 1 ] && [ -n "$4" ] || return 0
	l_report_skips_error="Failed to read snapshot transfer skip reasons for [$1]."
	[ -r "$3" ] || zxfer_throw_error "$l_report_skips_error"
	l_report_skips_common_path=${4%%"$ZXFER_TAB"*}
	l_report_skips_common_guid=${4#*"$ZXFER_TAB"}
	l_report_skips_header=0
	while IFS= read -r l_report_skips_row; do
		case $l_report_skips_row in
		"skip	before_common	"*)
			if [ "$l_report_skips_header" -eq 0 ]; then
				zxfer_echov "Source snapshots not scheduled for transfer to [$2]:"
				l_report_skips_header=1
			fi
			l_report_skips_record=${l_report_skips_row#"skip	before_common	"}
			zxfer_echov "  ${l_report_skips_record%%"$ZXFER_TAB"*} (GUID: ${l_report_skips_record#*"$ZXFER_TAB"}): before newest common snapshot $l_report_skips_common_path (GUID: $l_report_skips_common_guid) in source creation order; older gaps are not backfilled."
			;;
		__ZXFER_SNAPSHOT_SUMMARY__) break ;;
		esac
	done <"$3" || zxfer_throw_error "$l_report_skips_error" "$?"
}

# Purpose: Recheck the source live before deleting every destination
# snapshot.
# Usage: zxfer_all_destination_snapshot_delete_is_safe SOURCE_DATASET, only
# when the plan found no source snapshots. Returns 0 when the deletion stays
# safe (or no dataset is given), 1 after a skip warning; aborts when the
# recheck fails.
zxfer_all_destination_snapshot_delete_is_safe() {
	l_delete_safety_source_dataset=$1

	[ -n "$l_delete_safety_source_dataset" ] || return 0

	l_delete_safety_live_status=0
	l_delete_safety_live_snapshots=$(zxfer_run_source_zfs_cmd \
		list -H -d 1 -o name -t snapshot \
		"$l_delete_safety_source_dataset" 2>&1) ||
		l_delete_safety_live_status=$?
	if [ "$l_delete_safety_live_status" -ne 0 ]; then
		zxfer_throw_error "Failed to re-verify source snapshots for [$l_delete_safety_source_dataset] before deleting all destination snapshots: $l_delete_safety_live_snapshots" \
			"$l_delete_safety_live_status"
	fi

	# Remote stderr can share the capture with stdout. Count only real source
	# snapshot rows so benign transport diagnostics cannot change the decision.
	while IFS= read -r l_delete_safety_live_line; do
		case $l_delete_safety_live_line in
		"$l_delete_safety_source_dataset@"*)
			zxfer_warn_stderr "WARNING: skipping destination snapshot deletion for [$l_delete_safety_source_dataset]: the plan would delete every destination snapshot, but a live source re-check still shows snapshots. The cached source listing was likely incomplete."
			return 1
			;;
		esac
	done <<-EOF
		$l_delete_safety_live_snapshots
	EOF

	return 0
}

# Purpose: Print a creation epoch as a local date for operator messages.
# Usage: zxfer_format_snapshot_creation_epoch_for_display EPOCH; tries BSD
# date -r, then GNU date -d, then prints "EPOCH (unix epoch)". Returns 1 for
# a non-numeric EPOCH.
zxfer_format_snapshot_creation_epoch_for_display() {
	zxfer_is_uint "$1" || return 1

	if l_creation_display=$(date -r "$1" 2>/dev/null); then
		printf '%s\n' "$l_creation_display"
	elif l_creation_display=$(date -d "@$1" 2>/dev/null); then
		printf '%s\n' "$l_creation_display"
	else
		printf '%s\n' "$1 (unix epoch)"
	fi
}

# Purpose: Refuse to delete a destination snapshot that -g protects.
# Usage: zxfer_throw_grandfather_protection_error SNAPSHOT EPOCH AGE_DAYS;
# exits through zxfer_throw_usage_error.
zxfer_throw_grandfather_protection_error() {
	l_grandfather_date=$(zxfer_format_snapshot_creation_epoch_for_display "$2") ||
		l_grandfather_date=""
	[ -n "$l_grandfather_date" ] || l_grandfather_date="$2 (unix epoch)"
	l_grandfather_now=$(date)
	zxfer_throw_usage_error "On the destination there is a snapshot marked for destruction
            by zxfer that is protected by the use of the \"grandfather
            protection\" option, -g.

            You have set grandfather protection at $g_option_g_grandfather_protection days.
            Snapshot name: $1
            Snapshot age : $3 days old
            Snapshot date: $l_grandfather_date.
            Your current system date: $l_grandfather_now.

            Either amend/remove option g, fix your system date, or manually
            destroy the offending snapshot. Also double check that your
            snapshot management tool isn't erroneously deleting source snapshots.
            Note that for option g to work correctly, you should set it just
            above a number of days that will preclude \"father\" snapshots from
            being encountered."
}

# One awk pass answers a delete plan's creation-time questions. Operands are
# "side=creation FILE side=delete PLAN_FILE": FILE holds the creation rows
# from `zfs get -H -o name,value -p creation`; PLAN_FILE holds tagged deletes.
# Variables: common (the last common
# snapshot's destination path, or empty), now, and days (-g, or empty). A
# missing or non-numeric epoch is unknown. Prints:
#   1 or 0      1 keeps rollback eligible: with a common snapshot, a deleted
#               snapshot is newer than it or a needed epoch is unknown
#   PROTECTED   with days only, when found: the first deleted snapshot in list
#               order that is days old or older ("path<TAB>epoch<TAB>age_days")
#               or whose epoch is unknown ("path")
# shellcheck disable=SC2016  # awk program should see literal $0.
ZXFER_SNAPSHOT_DELETE_CREATION_AWK='
side == "creation" {
	tab = index($0, "\t")
	if (tab > 1)
		epoch[substr($0, 1, tab - 1)] = substr($0, tab + 1)
	next
}
substr($0, 1, 7) == "delete\t" {
	path = substr($0, 8)
	if (path == "")
		next
	value = (path in epoch) ? epoch[path] : ""
	known = (value ~ /^[0-9]+$/)
	if (common != "") {
		common_value = (common in epoch) ? epoch[common] : ""
		if (!known || common_value !~ /^[0-9]+$/ || value + 0 > common_value + 0)
			newer = 1
	}
	if (days == "" || found)
		next
	if (!known) {
		found = 1
		protected = path
	} else if (int((now - value) / 86400) >= days + 0) {
		found = 1
		protected = path "\t" value "\t" int((now - value) / 86400)
	}
}
END {
	print newer + 0
	if (found)
		print protected
}'

# Purpose: Read the creation times a delete plan needs with one batched zfs
# get, record rollback eligibility, and enforce -g.
# Usage: zxfer_prepare_snapshot_delete_creation_state PLAN_FILE, in the main
# shell with g_actual_dest and g_last_common_snap set for the dataset. Sets
# g_deleted_dest_newer_snapshots to 1 when a deleted snapshot is newer than
# the last common snapshot or a needed creation time is unknown. With -g,
# refuses the first delete record in PLAN_FILE that is -g days old or older, or
# whose creation time is unknown. Aborts when the query fails.
zxfer_prepare_snapshot_delete_creation_state() {
	l_creation_plan=$1
	l_creation_days=${g_option_g_grandfather_protection:-}
	g_deleted_dest_newer_snapshots=0

	# Rollback compares against the destination copy of the last common
	# snapshot.
	l_creation_common=""
	if [ -n "${g_actual_dest:-}" ]; then
		l_creation_common_source=${g_last_common_snap%%"$ZXFER_TAB"*}
		case $l_creation_common_source in
		*@?*) l_creation_common=$g_actual_dest@${l_creation_common_source#*@} ;;
		esac
	fi
	if [ -z "$l_creation_common$l_creation_days" ]; then
		return 0
	fi

	l_creation_now=""
	if [ -n "$l_creation_days" ]; then
		l_creation_now=$(date +%s)
		zxfer_is_uint "$l_creation_now" ||
			zxfer_throw_error "Failed to read the current time for grandfather protection (-g)."
	fi

	zxfer_ensure_runtime_artifact_file "${g_zxfer_snapshot_creation_file:-}" \
		zxfer-snapshot-creation ||
		zxfer_throw_error "Failed to allocate the snapshot creation-time file." "$?"
	g_zxfer_snapshot_creation_file=$g_zxfer_runtime_artifact_path_result
	: >"$g_zxfer_snapshot_creation_file" ||
		zxfer_throw_error "Failed to reset the snapshot creation-time file."

	# One zfs get per 128 paths: the common snapshot first, then each delete.
	# ssh must not read the delete list, so zfs gets /dev/null as stdin.
	l_creation_status=0
	set --
	[ -z "$l_creation_common" ] || set -- "$l_creation_common"
	[ -r "$l_creation_plan" ] || zxfer_throw_error "Failed to read the snapshot delete plan."
	while IFS= read -r l_creation_record; do
		case $l_creation_record in
		"delete	"*) set -- "$@" "${l_creation_record#delete	}" ;;
		*) continue ;;
		esac
		[ "$#" -ge 128 ] || continue
		zxfer_run_destination_zfs_cmd get -H -o name,value -p creation "$@" \
			</dev/null >>"$g_zxfer_snapshot_creation_file" || {
			l_creation_status=$?
			break
		}
		set --
	done <"$l_creation_plan"
	if [ "$l_creation_status" -eq 0 ] && [ "$#" -gt 0 ]; then
		zxfer_run_destination_zfs_cmd get -H -o name,value -p creation "$@" \
			</dev/null >>"$g_zxfer_snapshot_creation_file" || l_creation_status=$?
	fi
	if [ "$l_creation_status" -ne 0 ]; then
		zxfer_throw_error "Failed to query destination snapshot creation times while planning snapshot deletions. Review prior stderr for the transport or query error." \
			"$l_creation_status"
	fi

	l_creation_verdict=$(
		"${g_cmd_awk:-awk}" -v common="$l_creation_common" \
			-v now="$l_creation_now" -v days="$l_creation_days" \
			"$ZXFER_SNAPSHOT_DELETE_CREATION_AWK" \
			side=creation "$g_zxfer_snapshot_creation_file" side=delete "$l_creation_plan"
	) || l_creation_status=$?
	g_deleted_dest_newer_snapshots=${l_creation_verdict%%"$ZXFER_LF"*}
	case $l_creation_status:$g_deleted_dest_newer_snapshots in
	0:0 | 0:1) ;;
	*)
		g_deleted_dest_newer_snapshots=1
		[ "$l_creation_status" -ne 0 ] || l_creation_status=1
		zxfer_throw_error "Failed to evaluate destination snapshot creation times while planning snapshot deletions." \
			"$l_creation_status"
		;;
	esac

	# -g: the awk printed the first protected snapshot, if any.
	case $l_creation_verdict in
	*"$ZXFER_LF"*) ;;
	*) return 0 ;;
	esac
	l_creation_protected=${l_creation_verdict#*"$ZXFER_LF"}
	l_creation_path=${l_creation_protected%%"$ZXFER_TAB"*}
	if [ "$l_creation_path" = "$l_creation_protected" ]; then
		zxfer_throw_error "Couldn't determine creation time for destination snapshot $l_creation_path."
	fi
	l_creation_protected=${l_creation_protected#*"$ZXFER_TAB"}
	zxfer_throw_grandfather_protection_error "$l_creation_path" \
		"${l_creation_protected%%"$ZXFER_TAB"*}" "${l_creation_protected#*"$ZXFER_TAB"}"
}

# Purpose: Destroy one dataset's planned destination-only snapshots.
# Usage: zxfer_delete_snaps SOURCE_DATASET, right after
# zxfer_plan_dataset_snapshots checked the plan and published its destroy
# target. When that plan found no source snapshots the source is rechecked live first. Records rollback
# eligibility and applies -g before the destroy.
zxfer_delete_snaps() {
	l_delete_source=$1

	zxfer_echoV "Begin zxfer_delete_snaps()"
	if [ -z "$g_zxfer_plan_destroy_target" ]; then
		zxfer_echoV "No snapshots to delete."
		return 0
	fi

	# An empty source plan deletes every destination snapshot: recheck the
	# source live in that suspicious case and skip when the cache was stale.
	if [ "${g_zxfer_plan_source_count:-0}" -eq 0 ] &&
		! zxfer_all_destination_snapshot_delete_is_safe "$l_delete_source"; then
		return 0
	fi

	zxfer_prepare_snapshot_delete_creation_state "$g_zxfer_plan_file"

	# The destroy changes this dataset's snapshots: the marker makes the
	# pre-send recheck re-plan it from a live listing, allows the -F rollback
	# and tells replication that the pass did work (-Y).
	g_did_delete_dest_snapshots=1
	zxfer_run_destination_zfs_cmd destroy "$g_zxfer_plan_destroy_target" ||
		zxfer_throw_error "Error when executing command." "$?"

	zxfer_echoV "End zxfer_delete_snaps()"
}

# Purpose: Find the "diverged and converged this run" marker for a destination
# dataset.
# Usage: Called by the divergence contract gate (to deduplicate warnings when
# planning inspects a dataset more than once per run) and by the post-receive
# verification. Publishes the marker's source dataset in
# g_zxfer_diverged_converged_marker_source and returns 0 on a hit, 1 on a miss.
zxfer_find_diverged_converged_marker() {
	l_marker_dest=$1

	g_zxfer_diverged_converged_marker_source=""
	[ -n "${g_zxfer_diverged_converged_datasets:-}" ] || return 1

	while IFS='	' read -r l_marker_record_dest l_marker_record_source; do
		[ -n "$l_marker_record_dest" ] || continue
		if [ "$l_marker_record_dest" = "$l_marker_dest" ]; then
			g_zxfer_diverged_converged_marker_source=$l_marker_record_source
			return 0
		fi
	done <<-EOF
		$g_zxfer_diverged_converged_datasets
	EOF

	return 1
}

# Purpose: Drop one destination dataset from the "diverged and converged this
# run" marker list.
# Usage: Called by the post-receive verification after a live listing
# confirms the dataset no longer carries name-match/guid-mismatch snapshots.
zxfer_unmark_diverged_converged_dataset() {
	l_unmark_dest=$1
	l_remaining_markers=""

	while IFS='	' read -r l_marker_record_dest l_marker_record_source; do
		[ -n "$l_marker_record_dest" ] || continue
		[ "$l_marker_record_dest" = "$l_unmark_dest" ] && continue
		if [ -n "$l_remaining_markers" ]; then
			l_remaining_markers="$l_remaining_markers
$l_marker_record_dest	$l_marker_record_source"
		else
			l_remaining_markers="$l_marker_record_dest	$l_marker_record_source"
		fi
	done <<-EOF
		${g_zxfer_diverged_converged_datasets:-}
	EOF

	g_zxfer_diverged_converged_datasets=$l_remaining_markers
	return 0
}

# Purpose: Render the recorded divergence examples as operator-facing lines
# naming the destination snapshot and both guids.
# Usage: Called while building the divergence warning and the fail-closed
# divergence error so both surfaces show identical evidence.
zxfer_render_diverged_snapshot_example_lines() {
	l_example_dest_dataset=$1
	l_example_lines=""

	while IFS='	' read -r l_example_name l_example_src_guid l_example_dst_guid; do
		[ -n "$l_example_name" ] || continue
		l_example_line="  $l_example_dest_dataset@$l_example_name: source guid $l_example_src_guid vs destination guid $l_example_dst_guid"
		if [ -n "$l_example_lines" ]; then
			l_example_lines="$l_example_lines
$l_example_line"
		else
			l_example_lines=$l_example_line
		fi
	done <<-EOF
		${g_zxfer_diverged_snapshot_examples:-}
	EOF

	printf '%s\n' "$l_example_lines"
}

# Purpose: Enforce the destination divergence contract for the current dataset
# before any delete, rollback, or send is planned for it.
# Usage: Called by zxfer_inspect_delete_snap after the last common snapshot is
# known and before zxfer_delete_snaps can mutate the destination. Emits the
# per-dataset -V transparency line for every dataset. When name-match/guid-
# mismatch snapshots were recorded: with BOTH -d and -F active it prints the
# always-on convergence warning (stderr, not gated on -v/-V) and marks the
# dataset for post-receive verification; otherwise it fails closed via
# zxfer_throw_error so zero actions are taken for the diverged dataset.
zxfer_enforce_destination_divergence_contract() {
	l_divergence_source=$1

	zxfer_echoV "Last common snapshot: ${g_last_common_snap:-none}; diverged destination snapshots: ${g_zxfer_diverged_snapshot_count:-0}."

	[ "${g_zxfer_diverged_snapshot_count:-0}" -gt 0 ] || return 0
	# Planning can inspect the same dataset more than once per run (the -g
	# grandfather pre-pass runs zxfer_inspect_delete_snap before the main
	# pass); warn and count each diverged dataset only once.
	if zxfer_find_diverged_converged_marker "$g_actual_dest"; then
		return 0
	fi

	g_zxfer_profile_diverged_snapshot_warnings=$((g_zxfer_profile_diverged_snapshot_warnings + 1))
	l_diverged_example_lines=$(zxfer_render_diverged_snapshot_example_lines "$g_actual_dest")

	if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
		[ "${g_option_F_force_rollback:-}" != "" ]; then
		zxfer_warn_stderr "WARNING: destination dataset [$g_actual_dest] has ${g_zxfer_diverged_snapshot_count} snapshot(s) whose names match source dataset [$l_divergence_source] but whose guids differ (the destination diverged under identical snapshot names), e.g.:
$l_diverged_example_lines
-d and -F are active; converging: destroy + rollback + resend (destroy the diverged destination snapshots, roll back to the last guid-matching common snapshot, and resend the source range over them)."
		if [ -n "${g_zxfer_diverged_converged_datasets:-}" ]; then
			g_zxfer_diverged_converged_datasets="$g_zxfer_diverged_converged_datasets
$g_actual_dest	$l_divergence_source"
		else
			g_zxfer_diverged_converged_datasets="$g_actual_dest	$l_divergence_source"
		fi
		return 0
	fi

	zxfer_set_failure_stage "divergence reconciliation"
	zxfer_throw_error "Destination dataset [$g_actual_dest] has diverged from source dataset [$l_divergence_source]: ${g_zxfer_diverged_snapshot_count} destination snapshot(s) share a source snapshot name but carry different guids, e.g.:
$l_diverged_example_lines
No deletes or sends were planned for this dataset. Re-run with BOTH -d and -F to converge destructively (destroy the diverged destination snapshots, roll back to the last guid-matching common snapshot, and resend), or reconcile the destination manually."
}

# Purpose: Verify that a dataset converged this run no longer carries
# name-match/guid-mismatch snapshots after its receive.
# Usage: zxfer_verify_converged_destination_after_receive DEST, from the
# receive finalize choke points (foreground and -j reap time). No-op unless
# DEST carries a convergence marker. Plans DEST against a live depth-1 listing
# without touching the current dataset's published plan, and fails when
# divergence remains.
zxfer_verify_converged_destination_after_receive() {
	l_verify_dest=$1

	[ -n "${g_zxfer_diverged_converged_datasets:-}" ] || return 0
	if ! zxfer_find_diverged_converged_marker "$l_verify_dest"; then
		return 0
	fi
	l_verify_source=$g_zxfer_diverged_converged_marker_source

	if ! zxfer_get_live_destination_record_file "$l_verify_dest"; then
		zxfer_throw_error "Failed to retrieve live destination snapshots for [$l_verify_dest] during post-receive divergence verification: ${g_zxfer_live_destination_record_file_error:-}"
	fi
	zxfer_plan_dataset_snapshots "$l_verify_source" "$l_verify_dest" \
		"$g_zxfer_live_destination_listing_file"

	if [ "$g_zxfer_diverged_snapshot_count" -gt 0 ]; then
		l_verify_example_lines=$(zxfer_render_diverged_snapshot_example_lines "$l_verify_dest")
		zxfer_set_failure_stage "post-receive divergence verification"
		zxfer_throw_error "Destination dataset [$l_verify_dest] re-diverged after convergence: ${g_zxfer_diverged_snapshot_count} destination snapshot(s) still share a source snapshot name from [$l_verify_source] with a different guid, e.g.:
$l_verify_example_lines
An external writer is modifying the destination while zxfer converges it; stop that writer (or exclude this dataset) and re-run zxfer."
	fi

	zxfer_unmark_diverged_converged_dataset "$l_verify_dest"
	return 0
}

# Purpose: Plan the current dataset, enforce the divergence contract, and run
# -d deletions before any send.
# Usage: zxfer_inspect_delete_snap DELETE(0|1) SOURCE_DATASET, with
# g_actual_dest set. Publishes the active range and its eligibility file,
# and leaves the checked plan's summary results for deletion/preflight.
zxfer_inspect_delete_snap() {
	l_inspect_delete=$1
	l_inspect_source=$2

	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0

	zxfer_plan_dataset_snapshots "$l_inspect_source" "$g_actual_dest"
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" \
		"$g_zxfer_plan_transfer_first" "$g_zxfer_plan_transfer_final" \
		"$g_zxfer_plan_dest_has_snapshots" "$g_zxfer_plan_file"
	if [ -n "$g_last_common_snap" ]; then
		zxfer_echoV "Found last common snapshot: $g_last_common_snap."
	else
		zxfer_echoV "No common snapshot found."
	fi

	# Enforce the divergence contract before any destructive step: with both
	# -d and -F it warns and converges; otherwise it fails closed.
	zxfer_enforce_destination_divergence_contract "$l_inspect_source"

	if [ "$l_inspect_delete" -eq 1 ]; then
		zxfer_delete_snaps "$l_inspect_source" ||
			return "$?"
	fi
}

# Purpose: Re-plan the current dataset from its live destination snapshots
# when this run destroyed some of them.
# Usage: zxfer_reconcile_live_destination_snapshot_state SOURCE, from
# zxfer_copy_snapshots before SOURCE is sent, with the plan that
# zxfer_inspect_delete_snap published for SOURCE. Only a dataset whose -d
# destroy ran (g_did_delete_dest_snapshots) is listed again; every other
# dataset keeps the plan made from discovery, and zfs receive refuses an
# incremental whose base is gone. Republishes the plan from the live rows,
# keeping the anchor unless the anchor or a pending snapshot is common.
zxfer_reconcile_live_destination_snapshot_state() {
	[ "${g_did_delete_dest_snapshots:-0}" -eq 1 ] || return 0
	# Without an anchor or pending snapshots there is nothing to re-plan.
	[ -n "${g_last_common_snap:-}${g_src_snapshot_transfer_first:-}" ] || return 0

	# The planner fails closed on guid-less rows and finds the newest
	# snapshot whose name and guid both exist on the destination.
	zxfer_get_live_destination_record_file "$g_actual_dest" ||
		zxfer_throw_error "Failed to retrieve live destination snapshots for [$g_actual_dest]: ${g_zxfer_live_destination_record_file_error:-}"
	zxfer_plan_dataset_snapshots "$1" "$g_actual_dest" \
		"$g_zxfer_live_destination_listing_file"
	zxfer_echoV "Refreshed destination snapshot cache for $g_actual_dest using live snapshot state."

	# Only the anchor or a pending snapshot may become the new anchor.
	# Otherwise publish the same records with no anchor: the seed then
	# refuses a destination whose snapshots share no guid with them, or
	# re-seeds an emptied destination from the old anchor.
	[ -r "$g_zxfer_transfer_plan_file" ] ||
		zxfer_throw_error "Failed to read the active snapshot transfer plan."
	if [ -n "$g_zxfer_plan_common_snapshot" ]; then
		while IFS= read -r l_recheck_row; do
			case $l_recheck_row in
			"common	"* | "send	"*) ;;
			*) continue ;;
			esac
			[ "${l_recheck_row#*"$ZXFER_TAB"}" = "$g_zxfer_plan_common_snapshot" ] || continue
			zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" \
				"$g_zxfer_plan_transfer_first" "$g_zxfer_plan_transfer_final" \
				1 "$g_zxfer_plan_file"
			return
		done <"$g_zxfer_transfer_plan_file"
	fi
	# No eligible live anchor: prepend the old anchor to the same range, so an
	# emptied destination can be seeded from it. Keep the original identities.
	zxfer_publish_snapshot_transfer_plan "" \
		"${g_last_common_snap:-$g_src_snapshot_transfer_first}" \
		"${g_src_snapshot_transfer_final:-$g_last_common_snap}" \
		"$g_zxfer_plan_dest_has_snapshots" "$g_zxfer_transfer_plan_file"
}
