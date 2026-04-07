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

#
# Returns a list of destination snapshots that don't exist in the source.
# The source and destination snapshots should correspond to 1 dataset.
# Uses global temporary files to reduce mktemp operations per call
# g_delete_source_tmp_file
# g_delete_dest_tmp_file
# g_delete_snapshots_to_delete_tmp_file
#
write_snapshot_identities_to_file() {
	l_snapshot_records=$1
	l_output_file=$2

	{
		while IFS= read -r l_snapshot_record; do
			[ -n "$l_snapshot_record" ] || continue
			l_snapshot_identity=$(extract_snapshot_identity "$l_snapshot_record")
			[ -n "$l_snapshot_identity" ] || continue
			printf '%s\n' "$l_snapshot_identity"
		done <<-EOF
			$(normalize_snapshot_record_list "$l_snapshot_records")
		EOF
	} | LC_ALL=C sort -u >"$l_output_file"
}

get_dest_snapshots_to_delete_per_dataset() {
	echoV "Begin get_dest_snapshots_to_delete_per_dataset()"
	l_zfs_source_snaps=$1
	l_zfs_dest_snaps=$2

	# Write snapshot identity keys (name + guid) to the temporary files so that
	# `comm` can distinguish same-named but unrelated snapshots.
	# run the first process in the background
	write_snapshot_identities_to_file "$l_zfs_source_snaps" "$g_delete_source_tmp_file" &
	PID=$!
	zxfer_register_cleanup_pid "$PID"

	write_snapshot_identities_to_file "$l_zfs_dest_snaps" "$g_delete_dest_tmp_file"

	# wait for the background process to finish
	wait $PID
	zxfer_unregister_cleanup_pid "$PID"

	# Use comm to find snapshots in g_delete_dest_tmp_file that don't have a match in g_delete_source_tmp_file
	LC_ALL=C comm -13 "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" >"$g_delete_snapshots_to_delete_tmp_file"

	l_dest_snaps_to_delete=""
	while IFS= read -r l_snapshot_name; do
		[ -n "$l_snapshot_name" ] || continue
		while IFS= read -r l_dest_snapshot; do
			[ -n "$l_dest_snapshot" ] || continue
			if [ "$(extract_snapshot_identity "$l_dest_snapshot")" = "$l_snapshot_name" ]; then
				l_dest_snapshot_path=$(extract_snapshot_path "$l_dest_snapshot")
				if [ -n "$l_dest_snaps_to_delete" ]; then
					l_dest_snaps_to_delete="$l_dest_snaps_to_delete
$l_dest_snapshot_path"
				else
					l_dest_snaps_to_delete=$l_dest_snapshot_path
				fi
			fi
		done <<-EOF
			$(normalize_snapshot_record_list "$l_zfs_dest_snaps")
		EOF
	done <"$g_delete_snapshots_to_delete_tmp_file"

	# Print the matching lines
	echo "$l_dest_snaps_to_delete"
}

#
# find the most recent common snapshot. The source list is in descending order
# by creation date. The destination list is unordered.
#
get_last_common_snapshot() {
	echoV "Begin get_last_common_snapshot()"

	# sorted list of source datasets and snapshots
	l_zfs_source_snaps=$1
	# unordered list of destination datasets and snapshots
	l_zfs_dest_snaps=$2

	# Convert destination snapshots into a newline-delimited list of identity keys
	# (name + guid) so same-named but unrelated snapshots are not treated as
	# common anchors.
	l_newline='
'
	l_dest_snap_list="$l_newline"
	while IFS= read -r l_dest_snap; do
		[ -n "$l_dest_snap" ] || continue
		l_dest_identity=$(extract_snapshot_identity "$l_dest_snap")
		[ -n "$l_dest_identity" ] || continue
		l_dest_snap_list="${l_dest_snap_list}${l_dest_identity}${l_newline}"
	done <<-EOF
		$(normalize_snapshot_record_list "$l_zfs_dest_snaps")
	EOF

	# the last common snapshot
	l_snap_identity=""

	# loop through the source snapshots sorted in descending creation order
	# (newest first) to find the most recent common snapshot
	while IFS= read -r l_source_snap; do
		[ -n "$l_source_snap" ] || continue
		l_snap_identity=$(extract_snapshot_identity "$l_source_snap")
		[ -n "$l_snap_identity" ] || continue

		case "$l_dest_snap_list" in
		*"$l_newline$l_snap_identity$l_newline"*)
			l_last_common_snap=$l_source_snap

			echoV "Found last common snapshot: $l_last_common_snap."

			# once found, exit the function
			echo "$l_last_common_snap"
			return
			;;
		esac
	done <<-EOF
		$(normalize_snapshot_record_list "$l_zfs_source_snaps")
	EOF

	echoV "No common snapshot found."

	# return blank because no common snapshots has been found
	echo ""

	echoV "End get_last_common_snapshot()"
}

deleted_snapshots_include_newer_than_last_common() {
	l_deleted_snapshots=$1

	[ -n "$l_deleted_snapshots" ] || return 1
	[ -n "${g_last_common_snap:-}" ] || return 1
	[ -n "${g_actual_dest:-}" ] || return 1

	l_last_common_name=$(extract_snapshot_name "$g_last_common_snap")
	[ -n "$l_last_common_name" ] || return 1

	l_last_common_dest_snapshot="$g_actual_dest@$l_last_common_name"
	l_last_common_creation=$(run_destination_zfs_cmd get -H -o value -p creation "$l_last_common_dest_snapshot" 2>/dev/null || :)
	case "$l_last_common_creation" in
	'' | *[!0-9]*)
		# Fail safe: if we cannot compare creation times, keep rollback eligible.
		return 0
		;;
	esac

	while IFS= read -r l_deleted_snapshot; do
		[ -n "$l_deleted_snapshot" ] || continue
		l_deleted_snapshot_path=$(extract_snapshot_path "$l_deleted_snapshot")
		l_deleted_creation=$(run_destination_zfs_cmd get -H -o value -p creation "$l_deleted_snapshot_path" 2>/dev/null || :)
		case "$l_deleted_creation" in
		'' | *[!0-9]*)
			return 0
			;;
		esac
		if [ "$l_deleted_creation" -gt "$l_last_common_creation" ]; then
			return 0
		fi
	done <<-EOF
		$(normalize_snapshot_record_list "$l_deleted_snapshots")
	EOF

	return 1
}

#
# Tests a snapshot to see if it is older than the grandfather option allows for.
#
grandfather_test() {
	l_destination_snapshot=$1

	l_current_date=$(date +%s) # current date in seconds from 1970
	l_snap_date=$(run_destination_zfs_cmd get -H -o value -p creation "$l_destination_snapshot")

	l_diff_sec=$((l_current_date - l_snap_date))
	l_diff_day=$((l_diff_sec / 86400))

	if [ $l_diff_day -ge "$g_option_g_grandfather_protection" ]; then
		l_snap_date_english=$(run_destination_zfs_cmd get -H -o value creation "$l_destination_snapshot")
		l_current_date_english=$(date)
		l_error_msg="On the destination there is a snapshot marked for destruction
            by zxfer that is protected by the use of the \"grandfather
            protection\" option, -g.

            You have set grandfather protection at $g_option_g_grandfather_protection days.
            Snapshot name: $l_destination_snapshot
            Snapshot age : $l_diff_day days old
            Snapshot date: $l_snap_date_english.
            Your current system date: $l_current_date_english.

            Either amend/remove option g, fix your system date, or manually
            destroy the offending snapshot. Also double check that your
            snapshot management tool isn't erroneously deleting source snapshots.
            Note that for option g to work correctly, you should set it just
            above a number of days that will preclude \"father\" snapshots from
            being encountered."

		throw_usage_error "$l_error_msg"
	fi
}

#
# Delete snapshots in destination that aren't in source
#
delete_snaps() {
	echoV "Begin delete_snaps()"
	l_zfs_source_snaps=$1
	l_zfs_dest_snaps=$2

	l_snaps_to_delete=$(get_dest_snapshots_to_delete_per_dataset "$l_zfs_source_snaps" "$l_zfs_dest_snaps")

	# if l_snaps_to_delete is empty, there is nothing to do
	if [ "$l_snaps_to_delete" = "" ]; then
		echoV "No snapshots to delete."
		return
	fi

	g_deleted_dest_newer_snapshots=0
	if deleted_snapshots_include_newer_than_last_common "$l_snaps_to_delete"; then
		g_deleted_dest_newer_snapshots=1
	fi

	l_unprotected_snaps_to_delete=""

	# checks if any of the snapshots to delete are protected by the grandfather option
	while IFS= read -r l_snap_to_delete; do
		[ -n "$l_snap_to_delete" ] || continue
		if [ "$g_option_g_grandfather_protection" != "" ]; then
			grandfather_test "$l_snap_to_delete"
		fi

		l_snapshot=$(extract_snapshot_name "$l_snap_to_delete")

		# prepend this snapshot to the list of snapshots to delete in a comma
		# delimited list; the trailing comma is trimmed before issuing zfs destroy.
		l_unprotected_snaps_to_delete="$l_snapshot,$l_unprotected_snaps_to_delete"
	done <<-EOF
		$(normalize_snapshot_record_list "$l_snaps_to_delete")
	EOF

	# drop any trailing delimiter so the destroy command receives valid names
	l_unprotected_snaps_to_delete=${l_unprotected_snaps_to_delete%,}

	# if there are no snapshots because they are all protected by the grandfather
	# option, then there is nothing to do
	if [ "$l_unprotected_snaps_to_delete" = "" ]; then
		echoV "No unprotected snapshots to delete."
		return
	fi

	# get the dataset name from the first snapshot in the list
	#
	# - get the first element of the list
	# - get the portion of the string prior to the @ symbol
	# shellcheck disable=SC2016
	l_zfs_dest_dataset=$(echo "$l_snaps_to_delete" | head -n 1 | "$g_cmd_awk" -F'@' '{print $1}')

	# build the destroy command
	l_destroy_target="$l_zfs_dest_dataset@$l_unprotected_snaps_to_delete"
	l_cmd=$(zxfer_render_destination_zfs_command destroy "$l_destroy_target")
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		echov "Dry run: $l_cmd"
		return
	fi

	g_did_delete_dest_snapshots=1
	if ! run_destination_zfs_cmd destroy "$l_destroy_target"; then
		throw_error "Error when executing command."
	fi

	# set the flag to indicate that a destroy command was sent
	# shellcheck disable=SC2034
	g_is_performed_send_destroy=1

	echoV "End delete_snaps()"
}

# g_lat_common_snap is set even when a common snapshots is not found
set_src_snapshot_transfer_list() {
	l_zfs_source_snaps=$1
	l_source=$2

	l_found_common=0

	g_src_snapshot_transfer_list=""

	# This prepares a list of source snapshots to transfer, beginning with
	# the first snapshot after the last common one.
	while IFS= read -r l_test_snap; do
		[ -n "$l_test_snap" ] || continue
		if [ "$g_last_common_snap" != "" ] && [ "$l_test_snap" = "$g_last_common_snap" ]; then
			l_found_common=1
			continue
		fi

		if [ "$l_found_common" -eq 0 ]; then
			if [ -n "$g_src_snapshot_transfer_list" ]; then
				g_src_snapshot_transfer_list="$l_test_snap
$g_src_snapshot_transfer_list"
			else
				g_src_snapshot_transfer_list=$l_test_snap
			fi
		fi
	done <<-EOF
		$(normalize_snapshot_record_list "$l_zfs_source_snaps")
	EOF
}

inspect_delete_snap() {
	l_is_delete_snap=$1
	l_source=$2

	# shellcheck disable=SC2034
	g_did_delete_dest_snapshots=0
	# shellcheck disable=SC2034
	g_deleted_dest_newer_snapshots=0

	# Get only the snapshots for the exact source dataset in descending order
	# by creation date.
	l_zfs_source_snaps=$(printf '%s\n' "$g_lzfs_list_hr_S_snap" |
		"$g_cmd_awk" -F@ -v ds="$l_source" "\$1 == ds {print \$0}")

	# Get the list of destination snapshots for the matching destination dataset.
	l_zfs_dest_snaps=$(printf '%s\n' "$g_rzfs_list_hr_snap" |
		"$g_cmd_awk" -F@ -v ds="$g_actual_dest" "\$1 == ds {print \$0}")
	if [ -n "$l_zfs_dest_snaps" ]; then
		# shellcheck disable=SC2034
		# consumed by zxfer_zfs_mode.sh for status checks.
		g_dest_has_snapshots=1
	else
		# shellcheck disable=SC2034
		# consumed by zxfer_zfs_mode.sh for status checks.
		g_dest_has_snapshots=0
	fi

	# Find the most recent common snapshot on source and destination.
	g_last_common_snap=$(get_last_common_snapshot "$l_zfs_source_snaps" "$l_zfs_dest_snaps")

	# Deletes non-common snaps on destination if asked to.
	if [ "$l_is_delete_snap" -eq 1 ]; then
		delete_snaps "$l_zfs_source_snaps" "$l_zfs_dest_snaps"
	fi

	# Create a list of source snapshots to transfer, beginning with the
	# first snapshot after the last common one.
	set_src_snapshot_transfer_list "$l_zfs_source_snaps" "$l_source"
}
