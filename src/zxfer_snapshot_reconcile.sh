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
# SNAPSHOT PLAN / DELETE / DIVERGENCE HELPERS
################################################################################

# Module contract:
# owns globals: the per-dataset plan published for replication
#   (g_last_common_snap, g_src_snapshot_transfer_list, g_dest_has_snapshots),
#   delete/rollback markers (g_did_delete_dest_snapshots,
#   g_deleted_dest_newer_snapshots), the g_zxfer_plan_* results of
#   zxfer_plan_dataset_snapshots, the run-scoped scratch files
#   g_zxfer_snapshot_plan_file and g_zxfer_snapshot_creation_file, and the
#   divergence contract (g_zxfer_diverged_snapshot_count,
#   g_zxfer_diverged_snapshot_examples, g_zxfer_diverged_converged_datasets,
#   g_zxfer_diverged_converged_marker_source).
# reads globals: g_actual_dest, g_cmd_awk,
#   g_option_d_delete_destination_snapshots, g_option_F_force_rollback,
#   g_option_g_grandfather_protection, and the staged snapshot record files.
# mutates caches: the live view dirty list and replication's
#   g_is_performed_send_destroy marker (after a destroy).
# returns via stdout: the destroy target, the creation-date display text and
#   the divergence example lines only.

# Purpose: Forget the run-scoped snapshot plan and creation-time files.
# Usage: Called by session initialization; the next use allocates a fresh
# file under the run root.
zxfer_reset_snapshot_delete_artifact_state() {
	g_zxfer_snapshot_plan_file=""
	g_zxfer_snapshot_creation_file=""
}

# Purpose: Reset the per-dataset plan, delete, and divergence state.
# Usage: Called by session initialization.
zxfer_reset_snapshot_reconcile_state() {
	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0
	g_src_snapshot_transfer_list=""
	g_zxfer_plan_common_snapshot=""
	g_zxfer_plan_transfer_list=""
	g_zxfer_plan_dest_has_snapshots=0
	g_zxfer_plan_diverged_records=""
	g_zxfer_plan_delete_snapshots=""
	g_zxfer_plan_source_count=0
	g_zxfer_plan_destination_records=""
	# Per-dataset divergence scratch and the run-level "diverged and
	# converged this run" markers read by post-receive verification.
	g_zxfer_diverged_snapshot_count=0
	g_zxfer_diverged_snapshot_examples=""
	g_zxfer_diverged_converged_datasets=""
	g_zxfer_diverged_converged_marker_source=""
}

# Purpose: Publish the current snapshot transfer plan through its owner.
# Usage: Live reconciliation supplies the last common snapshot, remaining
# source records, and validated destination-snapshot presence as one update.
zxfer_publish_snapshot_transfer_plan() {
	g_last_common_snap=${1:-}
	g_src_snapshot_transfer_list=${2:-}
	case ${3:-0} in
	0 | 1) g_dest_has_snapshots=$3 ;;
	*) return 2 ;;
	esac
}

# One awk pass plans a dataset. Operands are "side=destination DEST_FILE
# side=source SOURCE_FILE"; rows are "dataset@snapshot<TAB>guid" and the
# source rows are newest first. A snapshot is common only when name AND guid
# match; a same-named snapshot with another guid is divergence. The dataset's
# destination rows go to stdout; the plan goes to ENVIRON["ZXFER_AWK_PLAN_FILE"]:
#   common<TAB>record         newest source record also on the destination
#   diverged<TAB>name<TAB>source_guid<TAB>destination_guid   (source order)
#   send<TAB>record           each source record newer than common, oldest
#                             first (every source record when none is common)
#   delete<TAB>path           destination snapshot whose name and guid are not
#                             on the source (destination order)
#   sources<TAB>count         always last; proves the plan is complete
# A matching row without a snapshot name or guid exits 3 (fail closed).
# shellcheck disable=SC2016  # awk program should see literal $0/$1/$2.
ZXFER_PLAN_DATASET_SNAPSHOTS_AWK='
BEGIN { plan_file = ENVIRON["ZXFER_AWK_PLAN_FILE"] }
$1 != (side == "destination" ? destination_dataset : source_dataset) { next }
{
	tab = index($2, "\t")
	if (tab < 2 || tab == length($2)) {
		missing_guid = 1
		exit 3
	}
	name = substr($2, 1, tab - 1)
	guid = substr($2, tab + 1)
}
side == "destination" {
	print
	destination_count++
	destination_path[destination_count] = $1 "@" name
	destination_id[destination_count] = name "\t" guid
	destination_ids[name "\t" guid] = 1
	destination_guid[name] = guid
	next
}
{
	source_count++
	source_ids[name "\t" guid] = 1
	if ((name in destination_guid) && destination_guid[name] != guid)
		print "diverged\t" name "\t" guid "\t" destination_guid[name] > plan_file
	if (found_common)
		next
	if ((name "\t" guid) in destination_ids) {
		found_common = 1
		print "common\t" $0 > plan_file
		next
	}
	pending[++pending_count] = $0
}
END {
	if (missing_guid)
		exit 3
	for (i = pending_count; i >= 1; i--)
		print "send\t" pending[i] > plan_file
	for (i = 1; i <= destination_count; i++)
		if (!(destination_id[i] in source_ids))
			print "delete\t" destination_path[i] > plan_file
	print "sources\t" source_count + 0 > plan_file
}'

# Purpose: Plan one dataset's snapshots in a single awk pass.
# Usage: zxfer_plan_dataset_snapshots SOURCE_DATASET DEST_DATASET
# [DEST_RECORD_FILE], in the main shell. Source rows come from the staged
# source record file; destination rows from DEST_RECORD_FILE (real destination
# names, e.g. zxfer_get_live_destination_record_file) or by default the staged
# destination record file. Publishes g_zxfer_plan_common_snapshot,
# g_zxfer_plan_transfer_list (oldest first), g_zxfer_plan_dest_has_snapshots,
# g_zxfer_plan_diverged_records, g_zxfer_plan_delete_snapshots (paths),
# g_zxfer_plan_source_count, and g_zxfer_plan_destination_records. Callers
# publish the plan themselves, so a post-receive check never clobbers the
# current dataset's plan; replication's live recheck reuses inspect's results
# while the live rows equal g_zxfer_plan_destination_records. Aborts on
# unreadable input or a guid-less record.
zxfer_plan_dataset_snapshots() {
	l_plan_source=$1
	l_plan_dest=$2
	l_plan_dest_file=${3:-${g_zxfer_destination_snapshot_record_cache_file:-}}
	l_plan_source_file=${g_zxfer_source_snapshot_record_cache_file:-}

	[ -r "$l_plan_source_file" ] ||
		zxfer_throw_error "Failed to read staged source snapshot record cache."
	[ -r "$l_plan_dest_file" ] ||
		zxfer_throw_error "Failed to read staged destination snapshot record cache."
	zxfer_ensure_snapshot_scratch_file "${g_zxfer_snapshot_plan_file:-}" zxfer-snapshot-plan ||
		zxfer_throw_error "Failed to allocate the snapshot plan file." "$?"
	g_zxfer_snapshot_plan_file=$g_zxfer_snapshot_scratch_file_result

	l_plan_status=0
	g_zxfer_plan_destination_records=$(ZXFER_AWK_PLAN_FILE=$g_zxfer_snapshot_plan_file \
		"${g_cmd_awk:-awk}" -F@ -v source_dataset="$l_plan_source" \
		-v destination_dataset="$l_plan_dest" "$ZXFER_PLAN_DATASET_SNAPSHOTS_AWK" \
		side=destination "$l_plan_dest_file" side=source "$l_plan_source_file") ||
		l_plan_status=$?

	g_zxfer_plan_common_snapshot=""
	g_zxfer_plan_transfer_list=""
	g_zxfer_plan_diverged_records=""
	g_zxfer_plan_delete_snapshots=""
	g_zxfer_plan_source_count=""
	if [ "$l_plan_status" -eq 0 ]; then
		while IFS= read -r l_plan_row; do
			case $l_plan_row in
			"common	"*)
				g_zxfer_plan_common_snapshot=${l_plan_row#common	}
				;;
			"send	"*)
				g_zxfer_plan_transfer_list=${g_zxfer_plan_transfer_list:+$g_zxfer_plan_transfer_list$ZXFER_LF}${l_plan_row#send	}
				;;
			"diverged	"*)
				g_zxfer_plan_diverged_records=${g_zxfer_plan_diverged_records:+$g_zxfer_plan_diverged_records$ZXFER_LF}${l_plan_row#diverged	}
				;;
			"delete	"*)
				g_zxfer_plan_delete_snapshots=${g_zxfer_plan_delete_snapshots:+$g_zxfer_plan_delete_snapshots$ZXFER_LF}${l_plan_row#delete	}
				;;
			"sources	"*)
				g_zxfer_plan_source_count=${l_plan_row#sources	}
				;;
			esac
		done <"$g_zxfer_snapshot_plan_file"
		# The count row comes last; without it the plan is incomplete.
		[ -n "$g_zxfer_plan_source_count" ] || l_plan_status=1
	fi
	if [ "$l_plan_status" -ne 0 ]; then
		zxfer_throw_error "Failed to determine the last common snapshot for [$l_plan_source] and [$l_plan_dest]." "$l_plan_status"
	fi

	g_zxfer_plan_dest_has_snapshots=0
	[ -z "$g_zxfer_plan_destination_records" ] || g_zxfer_plan_dest_has_snapshots=1
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
# "side=creation FILE side=delete -": FILE holds `zfs get -H -o name,value -p
# creation` rows and stdin the delete list. Variables: common (the last common
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
{
	path = $0
	tab = index(path, "\t")
	if (tab)
		path = substr(path, 1, tab - 1)
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
# Usage: zxfer_prepare_snapshot_delete_creation_state SNAPSHOTS, in the main
# shell with g_actual_dest and g_last_common_snap set for the dataset. Sets
# g_deleted_dest_newer_snapshots to 1 when a deleted snapshot is newer than
# the last common snapshot or a needed creation time is unknown. With -g,
# refuses the first snapshot in SNAPSHOTS that is -g days old or older, or
# whose creation time is unknown. Aborts when the query fails.
zxfer_prepare_snapshot_delete_creation_state() {
	l_creation_snapshots=$1
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
	if [ -z "$l_creation_snapshots" ] || [ -z "$l_creation_common$l_creation_days" ]; then
		return 0
	fi

	l_creation_now=""
	if [ -n "$l_creation_days" ]; then
		l_creation_now=$(date +%s)
		zxfer_is_uint "$l_creation_now" ||
			zxfer_throw_error "Failed to read the current time for grandfather protection (-g)."
	fi

	zxfer_ensure_snapshot_scratch_file "${g_zxfer_snapshot_creation_file:-}" \
		zxfer-snapshot-creation ||
		zxfer_throw_error "Failed to allocate the snapshot creation-time file." "$?"
	g_zxfer_snapshot_creation_file=$g_zxfer_snapshot_scratch_file_result
	: >"$g_zxfer_snapshot_creation_file" ||
		zxfer_throw_error "Failed to reset the snapshot creation-time file."

	# One zfs get per 128 paths: the common snapshot first, then each delete.
	# ssh must not read the delete list, so zfs gets /dev/null as stdin.
	l_creation_status=0
	set --
	[ -z "$l_creation_common" ] || set -- "$l_creation_common"
	while IFS= read -r l_creation_record; do
		[ -n "$l_creation_record" ] || continue
		set -- "$@" "${l_creation_record%%"$ZXFER_TAB"*}"
		[ "$#" -ge 128 ] || continue
		zxfer_run_destination_zfs_cmd get -H -o name,value -p creation "$@" \
			</dev/null >>"$g_zxfer_snapshot_creation_file" || {
			l_creation_status=$?
			break
		}
		set --
	done <<EOF
$l_creation_snapshots
EOF
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
			side=creation "$g_zxfer_snapshot_creation_file" side=delete - <<EOF
$l_creation_snapshots
EOF
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

# Purpose: Render the dataset@snap1,snap2 target for one delete list.
# Usage: zxfer_get_snapshot_destroy_target SNAPSHOTS, after the grandfather
# check; prints the comma-joined target consumed by `zfs destroy`.
zxfer_get_snapshot_destroy_target() {
	l_destroy_plan_snapshots=$1
	l_destroy_plan_unprotected_names=""

	while IFS= read -r l_destroy_plan_snapshot; do
		[ -n "$l_destroy_plan_snapshot" ] || continue
		# Snapshot name: after the first "@" of the path (before any guid tab).
		l_destroy_plan_path=${l_destroy_plan_snapshot%%	*}
		case "$l_destroy_plan_path" in
		*@*) l_destroy_plan_name=${l_destroy_plan_path#*@} ;;
		*) l_destroy_plan_name="" ;;
		esac
		l_destroy_plan_unprotected_names="$l_destroy_plan_name,$l_destroy_plan_unprotected_names"
	done <<EOF
$l_destroy_plan_snapshots
EOF
	l_destroy_plan_unprotected_names=${l_destroy_plan_unprotected_names%,}

	# The dataset is the first line of the plan up to its first "@".
	l_destroy_plan_dataset=${l_destroy_plan_snapshots%%
*}
	l_destroy_plan_dataset=${l_destroy_plan_dataset%%@*}
	printf '%s@%s\n' "$l_destroy_plan_dataset" \
		"$l_destroy_plan_unprotected_names"
}

# Purpose: Destroy one dataset's planned destination-only snapshots.
# Usage: zxfer_delete_snaps SOURCE_DATASET SNAPSHOTS, right after
# zxfer_plan_dataset_snapshots produced SNAPSHOTS. When that plan found no
# source snapshots the source is rechecked live first. Records rollback
# eligibility and applies -g before the destroy.
zxfer_delete_snaps() {
	l_delete_source=$1
	l_delete_snapshots=$2

	zxfer_echoV "Begin zxfer_delete_snaps()"
	if [ -z "$l_delete_snapshots" ]; then
		zxfer_echoV "No snapshots to delete."
		return 0
	fi

	# An empty source plan deletes every destination snapshot: recheck the
	# source live in that suspicious case and skip when the cache was stale.
	if [ "${g_zxfer_plan_source_count:-0}" -eq 0 ] &&
		! zxfer_all_destination_snapshot_delete_is_safe "$l_delete_source"; then
		return 0
	fi

	zxfer_prepare_snapshot_delete_creation_state "$l_delete_snapshots"
	l_destroy_target=$(zxfer_get_snapshot_destroy_target "$l_delete_snapshots") ||
		return "$?"

	g_did_delete_dest_snapshots=1
	zxfer_run_destination_zfs_cmd destroy "$l_destroy_target" ||
		zxfer_throw_error "Error when executing command." "$?"
	# Only this dataset's snapshots changed: its later rechecks (and any
	# post-receive verification) list it live instead of from the batched view.
	zxfer_mark_live_destination_dataset_dirty "${l_destroy_target%%@*}"

	# A destroy changed replication state; -Y decides on this marker.
	g_is_performed_send_destroy=1

	zxfer_echoV "End zxfer_delete_snaps()"
}

# Purpose: Record the current dataset's diverged destination snapshots into
# the per-dataset divergence scratch globals.
# Usage: zxfer_record_diverged_destination_snapshots RECORDS, with the plan's
# "name<TAB>source_guid<TAB>destination_guid" lines. Resets and repopulates
# g_zxfer_diverged_snapshot_count and g_zxfer_diverged_snapshot_examples (up
# to three example lines).
zxfer_record_diverged_destination_snapshots() {
	l_diverged_records=$1

	g_zxfer_diverged_snapshot_count=0
	g_zxfer_diverged_snapshot_examples=""
	[ -n "$l_diverged_records" ] || return 0

	while IFS= read -r l_diverged_record; do
		[ -n "$l_diverged_record" ] || continue
		g_zxfer_diverged_snapshot_count=$((g_zxfer_diverged_snapshot_count + 1))
		[ "$g_zxfer_diverged_snapshot_count" -le 3 ] || continue
		if [ -n "$g_zxfer_diverged_snapshot_examples" ]; then
			g_zxfer_diverged_snapshot_examples="$g_zxfer_diverged_snapshot_examples
$l_diverged_record"
		else
			g_zxfer_diverged_snapshot_examples=$l_diverged_record
		fi
	done <<-EOF
		$l_diverged_records
	EOF

	return 0
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
# Usage: Called by the post-receive verification after the live destination
# view confirms the dataset no longer carries name-match/guid-mismatch
# snapshots.
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

	zxfer_profile_increment_counter g_zxfer_profile_diverged_snapshot_warnings
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
# DEST carries a convergence marker. Plans DEST against its live rows (the
# receive marked it dirty, so a depth-1 listing) without touching the current
# dataset's published plan, and fails when divergence remains.
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
		"$g_zxfer_live_destination_record_file_result"

	if [ -n "$g_zxfer_plan_diverged_records" ]; then
		zxfer_record_diverged_destination_snapshots "$g_zxfer_plan_diverged_records"
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
# g_actual_dest set. Publishes g_last_common_snap, g_src_snapshot_transfer_list
# and g_dest_has_snapshots, and leaves the plan's g_zxfer_plan_* results.
zxfer_inspect_delete_snap() {
	l_inspect_delete=$1
	l_inspect_source=$2

	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0

	zxfer_plan_dataset_snapshots "$l_inspect_source" "$g_actual_dest"
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" \
		"$g_zxfer_plan_transfer_list" "$g_zxfer_plan_dest_has_snapshots"

	zxfer_record_diverged_destination_snapshots "$g_zxfer_plan_diverged_records"
	if [ -n "$g_last_common_snap" ]; then
		zxfer_echoV "Found last common snapshot: $g_last_common_snap."
	else
		zxfer_echoV "No common snapshot found."
	fi

	# Enforce the divergence contract before any destructive step: with both
	# -d and -F it warns and converges; otherwise it fails closed.
	zxfer_enforce_destination_divergence_contract "$l_inspect_source"

	if [ "$l_inspect_delete" -eq 1 ]; then
		zxfer_delete_snaps "$l_inspect_source" "$g_zxfer_plan_delete_snapshots" ||
			return "$?"
	fi
}
