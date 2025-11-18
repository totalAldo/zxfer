#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024-2025 Aldo Gonzalez
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
get_dest_snapshots_to_delete_per_dataset() {
	echoV "Begin get_dest_snapshots_to_delete_per_dataset()"
	l_zfs_source_snaps=$1
	l_zfs_dest_snaps=$2

	# Write the snapshot names to the temporary files so that we can pass them to comm
	# run the first process in the background
	echo "$l_zfs_source_snaps" | tr ' ' '\n' | $g_cmd_awk -F'@' "{print \$2}" | sort >"$g_delete_source_tmp_file" &
	PID=$!

	echo "$l_zfs_dest_snaps" | tr ' ' '\n' | $g_cmd_awk -F'@' "{print \$2}" | sort >"$g_delete_dest_tmp_file"

	# wait for the background process to finish
	wait $PID

	# Use comm to find snapshots in g_delete_dest_tmp_file that don't have a match in g_delete_source_tmp_file
	comm -13 "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" >"$g_delete_snapshots_to_delete_tmp_file"

	# Use grep to find the matching lines in l_zfs_dest_snaps
	l_dest_snaps_to_delete=$(echo "$l_zfs_dest_snaps" | grep -F -f "$g_delete_snapshots_to_delete_tmp_file")

	# Print the matching lines
	echo "$l_dest_snaps_to_delete"
	#echoV "End get_dest_snapshots_to_delete_per_dataset()"
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

	# Convert destination snapshots into a newline-delimited list of just the
	# snapshot component so we can do exact membership checks without relying on
	# awk (unit tests source helpers before init_globals runs).
	l_newline='
'
	l_dest_snap_list="$l_newline"
	for l_dest_snap in $l_zfs_dest_snaps; do
		l_dest_name=$(extract_snapshot_name "$l_dest_snap")
		[ -n "$l_dest_name" ] || continue
		l_dest_snap_list="${l_dest_snap_list}${l_dest_name}${l_newline}"
	done

	# the last common snapshot
	l_snap_name=""

	# loop through the source snapshots sorted in descending creation order
	# (newest first) to find the most recent common snapshot
	for l_source_snap in $l_zfs_source_snaps; do
		l_snap_name=$(extract_snapshot_name "$l_source_snap")

		[ -n "$l_snap_name" ] || continue

		case "$l_dest_snap_list" in
		*"$l_newline$l_snap_name$l_newline"*)
			l_last_common_snap=$l_source_snap

			echoV "Found last common snapshot: $l_last_common_snap."

			# once found, exit the function
			echo "$l_last_common_snap"
			return
			;;
		esac
	done

	echoV "No common snapshot found."

	# return blank because no common snapshots has been found
	echo ""

	echoV "End get_last_common_snapshot()"
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

	l_unprotected_snaps_to_delete=""

	# checks if any of the snapshots to delete are protected by the grandfather option
	for l_snap_to_delete in $l_snaps_to_delete; do
		if [ "$g_option_g_grandfather_protection" != "" ]; then
			grandfather_test "$l_snap_to_delete"
		fi

		l_snapshot=$(extract_snapshot_name "$l_snap_to_delete")

		# prepend this snapshot to the list of snapshots to delete in a comma
		# delimited list. It is ok for the list to have a trailing comma.
		l_unprotected_snaps_to_delete="$l_snapshot,$l_unprotected_snaps_to_delete"
	done

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
	l_cmd="$g_RZFS destroy $l_zfs_dest_dataset@$l_unprotected_snaps_to_delete"
	echov "$l_cmd"
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		echov "Dry run, skipping delete."
		return
	fi
	execute_background_cmd "$l_cmd" /dev/null

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
	for l_test_snap in $l_zfs_source_snaps; do
		if [ "$g_last_common_snap" != "" ] && [ "$l_test_snap" = "$g_last_common_snap" ]; then
			l_found_common=1
			continue
		fi

		if [ $l_found_common = 0 ]; then
			g_src_snapshot_transfer_list="$l_test_snap,$g_src_snapshot_transfer_list"
		fi
	done

	g_src_snapshot_transfer_list=$(echo "$g_src_snapshot_transfer_list" | tr -s "," "\n")
}

inspect_delete_snap() {
	#echoV "Begin inspect_delete_snap()"
	l_is_delete_snap=$1
	l_source=$2

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

	# Deletes non-common snaps on destination if asked to.
	if [ "$l_is_delete_snap" -eq 1 ]; then
		delete_snaps "$l_zfs_source_snaps" "$l_zfs_dest_snaps"
	fi

	# Find the most recent common snapshot on source and destination.
	g_last_common_snap=$(get_last_common_snapshot "$l_zfs_source_snaps" "$l_zfs_dest_snaps")

	# Create a list of source snapshots to transfer, beginning with the
	# first snapshot after the last common one.
	set_src_snapshot_transfer_list "$l_zfs_source_snaps" "$l_source"
	#echoV "End inspect_delete_snap()"
}
