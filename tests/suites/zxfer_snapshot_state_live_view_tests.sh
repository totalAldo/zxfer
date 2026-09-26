#!/bin/sh
# Live destination view tests for src/zxfer_snapshot_state.sh: one batched
# listing per pass, dirty datasets read live, and fail-closed listing errors,
# read the way the replication recheck reads them. Run by
# tests/test_zxfer_snapshot_state.sh under the replication fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Print the live destination records of DEST (default g_actual_dest) the way
# the replication recheck reads them.
zxfer_test_live_destination_records() {
	zxfer_get_live_destination_record_file "${1:-$g_actual_dest}" || return
	zxfer_filter_snapshot_record_file_for_dataset \
		"$g_zxfer_live_destination_record_file_result" "${1:-$g_actual_dest}"
}

test_live_destination_view_one_batched_listing_serves_multiple_datasets() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	view_log="$TEST_TMPDIR/live_view_shared.log"
	: >"$view_log"

	output=$(
		(
			VIEW_LOG="$view_log"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
					[ "$5" = "-t" ] && [ "$6" = "snapshot" ] && [ "$7" = "backup/target/src" ]; then
					printf 'view\n' >>"$VIEW_LOG"
					printf 'backup/target/src@snap1\t111\nbackup/target/src/child@snap1\t211\n'
					return 0
				fi
				printf 'unexpected %s\n' "$*" >>"$VIEW_LOG"
				return 1
			}

			g_actual_dest="backup/target/src"
			zxfer_ensure_live_destination_snapshot_view
			printf 'root=<%s>\n' "$(zxfer_test_live_destination_records 2>&1)"
			g_actual_dest="backup/target/src/child"
			zxfer_ensure_live_destination_snapshot_view
			printf 'child=<%s>\n' "$(zxfer_test_live_destination_records 2>&1)"
		)
	)

	assertEquals "Two covered datasets with no destination mutation between them must be served from exactly one batched listing." \
		"view" "$(cat "$view_log")"
	assertContains "The batched view must serve the root dataset exactly its own records." \
		"$output" "root=<backup/target/src@snap1	111>"
	assertContains "The batched view must serve the child dataset exactly its own records." \
		"$output" "child=<backup/target/src/child@snap1	211>"
}

test_live_destination_view_serves_dirty_dataset_live_and_siblings_from_batched_view() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	view_log="$TEST_TMPDIR/live_view_dirty.log"
	: >"$view_log"

	output=$(
		(
			VIEW_LOG="$view_log"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
					[ "$5" = "-t" ] && [ "$6" = "snapshot" ] && [ "$7" = "backup/target/src" ]; then
					printf 'view\n' >>"$VIEW_LOG"
					printf 'backup/target/src@snap1\t111\nbackup/target/src/child@snap1\t211\nbackup/target/src/child2@snap1\t311\n'
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] &&
					[ "$5" = "-o" ] && [ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ]; then
					printf 'depth1 %s\n' "$9" >>"$VIEW_LOG"
					printf '%s@snap1\t211\n%s@snap2\t222\n' "$9" "$9"
					return 0
				fi
				printf 'unexpected %s\n' "$*" >>"$VIEW_LOG"
				return 1
			}

			g_actual_dest="backup/target/src/child"
			zxfer_ensure_live_destination_snapshot_view
			printf 'before=<%s>\n' "$(zxfer_test_live_destination_records 2>&1)"
			# A receive into child completed: the mutation choke points mark
			# exactly that dataset dirty in the main shell.
			zxfer_mark_live_destination_dataset_dirty "backup/target/src/child"
			zxfer_ensure_live_destination_snapshot_view
			printf 'dirty=<%s>\n' "$(zxfer_test_live_destination_records 2>&1)"
			g_actual_dest="backup/target/src/child2"
			zxfer_ensure_live_destination_snapshot_view
			printf 'sibling=<%s>\n' "$(zxfer_test_live_destination_records 2>&1)"
			g_actual_dest="backup/target/src/child"
			zxfer_ensure_live_destination_snapshot_view
			printf 'dirty_again=<%s>\n' "$(zxfer_test_live_destination_records 2>&1)"
		)
	)

	assertEquals "The batched view must be captured exactly once; a dirty dataset is listed live at depth 1 on every later recheck and never re-captures the view, and an unmarked sibling issues no zfs call at all." \
		"view
depth1 backup/target/src/child
depth1 backup/target/src/child" "$(cat "$view_log")"
	assertContains "Before the mutation the dataset is served from the batched view." \
		"$output" "before=<backup/target/src/child@snap1	211>"
	assertContains "After the mutation the dirty dataset must be served the live depth-1 rows, not its stale batched rows." \
		"$output" "dirty=<backup/target/src/child@snap1	211
backup/target/src/child@snap2	222>"
	assertContains "An unmarked sibling must still be served from the batched view captured before the mutation." \
		"$output" "sibling=<backup/target/src/child2@snap1	311>"
	assertContains "A dirty dataset stays dirty for the rest of the pass." \
		"$output" "dirty_again=<backup/target/src/child@snap1	211
backup/target/src/child@snap2	222>"
}

test_live_destination_view_listing_failure_fails_closed() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base	111"
	send_log="$TEST_TMPDIR/live_view_failure_send.log"
	: >"$send_log"

	set +e
	output=$(
		(
			SEND_LOG="$send_log"
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_zfs_send_receive() {
				printf 'send\n' >>"$SEND_LOG"
			}

			zxfer_copy_snapshots "tank/src"
		)
	)
	status=$?

	assertEquals "A failed batched live view listing must abort instead of serving stale or empty state as fresh." \
		1 "$status"
	assertContains "The batched view refresh failure should identify the dataset and view root." \
		"$output" "Failed to refresh the batched live destination snapshot view for [backup/target/src] from [backup/target/src]."
	assertEquals "No send may be planned after a failed batched live view listing." \
		"" "$(cat "$send_log")"
}

test_live_destination_view_reap_time_property_invalidation_marks_dataset_dirty() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	view_log="$TEST_TMPDIR/live_view_reap.log"
	: >"$view_log"

	(
		VIEW_LOG="$view_log"
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
				[ "$5" = "-t" ] && [ "$6" = "snapshot" ] && [ "$7" = "backup/target/src" ]; then
				printf 'view\n' >>"$VIEW_LOG"
				return 0
			fi
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] &&
				[ "$5" = "-o" ] && [ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ]; then
				printf 'depth1 %s\n' "$9" >>"$VIEW_LOG"
				return 0
			fi
			printf 'unexpected %s\n' "$*" >>"$VIEW_LOG"
			return 1
		}

		g_actual_dest="backup/target/src"
		zxfer_ensure_live_destination_snapshot_view
		# Reap-time receive completion marks exactly the reaped dataset dirty
		# in the main shell (zxfer_reap_send_job) so
		# its post-receive verification lists it live while the next
		# dataset's recheck keeps the view.
		zxfer_mark_live_destination_dataset_dirty "backup/target/src"
		zxfer_test_live_destination_records "backup/target/src" >/dev/null 2>&1
		g_actual_dest="backup/target/src/child"
		zxfer_ensure_live_destination_snapshot_view
		zxfer_test_live_destination_records >/dev/null 2>&1
	)

	assertEquals "The reap-time dirty mark must cover only the reaped dataset: it is listed live at depth 1 while the next dataset is still served from the one batched view." \
		"view
depth1 backup/target/src" "$(cat "$view_log")"
}

test_live_destination_view_pass_boundary_forces_fresh_batched_listing() {
	g_option_Y_yield_iterations=4
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	view_log="$TEST_TMPDIR/live_view_pass_boundary.log"
	: >"$view_log"

	(
		VIEW_LOG="$view_log"
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
				[ "$5" = "-t" ] && [ "$6" = "snapshot" ] && [ "$7" = "backup/target/src" ]; then
				printf 'view\n' >>"$VIEW_LOG"
				return 0
			fi
			printf 'unexpected %s\n' "$*" >>"$VIEW_LOG"
			return 1
		}
		iteration=0
		zxfer_run_zfs_mode() {
			iteration=$((iteration + 1))
			printf 'pass %s\n' "$iteration" >>"$VIEW_LOG"
			# Pass shape: dataset A's recheck captures the view, A's receive
			# completes (A dirty), then a trailing in-sync dataset B's
			# recheck is still served from that same view. Only the pass
			# boundary can force the next pass's fresh listing here.
			g_actual_dest="backup/target/src"
			zxfer_ensure_live_destination_snapshot_view
			zxfer_mark_live_destination_dataset_dirty "backup/target/src"
			g_actual_dest="backup/target/src/child"
			zxfer_ensure_live_destination_snapshot_view
			if [ "$iteration" -ge 2 ]; then
				g_is_performed_send_destroy=0
			else
				g_is_performed_send_destroy=1
			fi
		}
		zxfer_run_zfs_mode_loop
	)

	assertEquals "A -Y pass boundary must invalidate the batched live view so the next pass's first recheck captures a fresh listing even though the previous pass's view was never invalidated within that pass." \
		"pass 1
view
pass 2
view" "$(cat "$view_log")"
}
