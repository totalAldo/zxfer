#!/bin/sh
# The live re-plan of src/zxfer_snapshot_plan.sh
# (zxfer_reconcile_live_destination_snapshot_state), run by
# tests/test_zxfer_snapshot_plan.sh. Only a dataset whose -d destroy ran is
# listed again, so most cases set g_did_delete_dest_snapshots=1.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Stage the source record file the planner reads: "dataset@snap<TAB>guid"
# rows, newest first.
zxfer_test_stage_source_records() {
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/source_snapshot.records"
	printf '%s\n' "$1" >"$g_zxfer_source_snapshot_record_cache_file"
}

test_zxfer_reconcile_live_destination_snapshot_state_shortcuts_empty_source_and_requeues_when_live_empty() {
	zxfer_test_stage_source_records "tank/src@snap2	222
tank/src@snap1	111"
	output=$(
		(
			# The -d destroy ran on this dataset, so the recheck re-plans it.
			g_did_delete_dest_snapshots=1
			g_actual_dest="backup/target/src"
			g_last_common_snap=""
			g_src_snapshot_transfer_list=""
			g_dest_has_snapshots=1
			zxfer_run_destination_zfs_cmd() {
				printf 'unexpected listing\n'
			}

			zxfer_reconcile_live_destination_snapshot_state "tank/src"
			printf 'no_source_status=%s\n' "$?"

			g_last_common_snap="tank/src@snap1	111"
			g_src_snapshot_transfer_list="tank/src@snap2	222"
			g_dest_has_snapshots=1
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			zxfer_reconcile_live_destination_snapshot_state "tank/src"
			printf 'empty_live_status=%s\n' "$?"
			printf 'dest_has_snapshots=%s\n' "${g_dest_has_snapshots:-1}"
			printf 'last=<%s>\n' "$g_last_common_snap"
			printf 'transfer=<%s>\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertContains "Live destination-state reconciliation should return success when there are no source records to reconcile." \
		"$output" "no_source_status=0"
	assertNotContains "Nothing to re-plan must not list the destination." \
		"$output" "unexpected listing"
	assertContains "Live destination-state reconciliation should return success when the destination has no live snapshots." \
		"$output" "empty_live_status=0"
	assertContains "Live destination-state reconciliation should clear the destination snapshot marker when no live snapshots remain." \
		"$output" "dest_has_snapshots=0"
	assertContains "Live destination-state reconciliation should clear a stale common snapshot when no live snapshots remain." \
		"$output" "last=<>"
	assertContains "An empty live destination should be re-planned from the whole source history, oldest first." \
		"$output" "transfer=<tank/src@snap1	111
tank/src@snap2	222>"
}

# Inspect's plan of a dataset with two pending snapshots: the published plan
# and the planner results it came from.
zxfer_test_prepare_inspected_plan() {
	g_actual_dest="backup/target/src"
	g_zxfer_plan_common_snapshot="tank/src@base	111"
	g_zxfer_plan_transfer_list="tank/src@next	222
tank/src@final	333"
	g_zxfer_plan_destination_records="backup/target/src@base	111"
	g_zxfer_plan_dest_has_snapshots=1
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" \
		"$g_zxfer_plan_transfer_list" 1
}

test_reconcile_live_destination_snapshot_state_keeps_the_discovery_plan_of_unchanged_datasets() {
	call_log="$TEST_TMPDIR/unchanged_plan_calls.log"
	: >"$call_log"
	output=$(
		zxfer_test_prepare_inspected_plan
		g_did_delete_dest_snapshots=0
		zxfer_probe_destination_existence() { printf 'probe %s\n' "$*" >>"$call_log"; }
		zxfer_run_destination_zfs_cmd() { printf 'zfs %s\n' "$*" >>"$call_log"; }
		zxfer_plan_dataset_snapshots() { exit 23; }
		zxfer_reconcile_live_destination_snapshot_state "tank/src"
		printf 'common=%s\npending=%s\nhas=%s\n' "$g_last_common_snap" \
			"$g_src_snapshot_transfer_list" "$g_dest_has_snapshots"
	)
	status=$?
	assertEquals "A dataset this run did not change should keep inspect's plan." 0 "$status"
	assertEquals "A dataset this run did not change must be neither probed nor listed again." \
		"" "$(cat "$call_log")"
	assertEquals "The discovery plan's common snapshot and pending range should stay intact." \
		"common=tank/src@base	111
pending=tank/src@next	222
tank/src@final	333
has=1" "$output"
}

test_reconcile_live_destination_snapshot_state_replans_changed_datasets_from_live_rows() {
	listing_log="$TEST_TMPDIR/changed_plan_listing.log"
	: >"$listing_log"
	status=0
	(
		zxfer_test_prepare_inspected_plan
		g_did_delete_dest_snapshots=1
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$listing_log"
			printf '%s\n' "$g_zxfer_plan_destination_records"
		}
		zxfer_plan_dataset_snapshots() { exit 23; }
		zxfer_reconcile_live_destination_snapshot_state "tank/src"
	) || status=$?
	assertEquals "A dataset whose snapshots this run destroyed must be planned again, even when its live rows look unchanged." \
		23 "$status"
	assertEquals "The re-plan should read one depth-1 listing of the dataset." \
		"list -H -d 1 -o name,guid -t snapshot backup/target/src" "$(cat "$listing_log")"
}

test_reconcile_live_destination_snapshot_state_aborts_when_the_live_listing_fails() {
	status=0
	output=$(
		zxfer_test_prepare_inspected_plan
		g_did_delete_dest_snapshots=1
		zxfer_run_destination_zfs_cmd() { return 29; }
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_reconcile_live_destination_snapshot_state "tank/src"
	) || status=$?
	assertEquals "A live listing failure must abort instead of keeping the stale plan." 1 "$status"
	assertContains "The failure should identify the live destination lookup." \
		"$output" "Failed to retrieve live destination snapshots for [backup/target/src]"
}

test_reconcile_live_destination_snapshot_state_keeps_newest_matching_snapshot() {
	g_did_delete_dest_snapshots=1
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list=$(
		cat <<'EOF'
tank/src@snap1	111
tank/src@snap2	222
tank/src@snap3	333
tank/src@snap4	444
EOF
	)
	zxfer_test_stage_source_records "tank/src@snap4	444
tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					cat <<'EOF'
backup/target/src@snap1	111
backup/target/src@snap3	333
EOF
					return 0
				fi
				return 1
			}

			zxfer_reconcile_live_destination_snapshot_state "tank/src"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "The live reconciliation should keep the newest matching source snapshot as the common anchor." \
		"$output" "last=tank/src@snap3	333"
	assertContains "Only snapshots after the newest live common snapshot should remain queued for transfer." \
		"$output" "remaining=<tank/src@snap4	444>"
	assertContains "A successful live reconciliation should still mark the destination as snapshotted." \
		"$output" "dest_has=1"
}

test_reconcile_live_destination_snapshot_state_never_anchors_on_guid_less_pending_records() {
	g_did_delete_dest_snapshots=1
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list=$(
		cat <<'EOF'
tank/src@snap1
tank/src@snap2
tank/src@snap3
tank/src@snap4
EOF
	)
	zxfer_test_stage_source_records "tank/src@snap4	444
tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					cat <<'EOF'
backup/target/src@snap1	111
backup/target/src@snap3	333
EOF
					return 0
				fi
				return 1
			}

			zxfer_reconcile_live_destination_snapshot_state "tank/src"
			printf 'last=<%s>\n' "$g_last_common_snap"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "A guid-less pending record never matches the planner's guid-bearing common snapshot." \
		"$output" "last=<>"
	assertContains "Without an anchor the pending records stay queued as they were." \
		"$output" "remaining=<tank/src@snap1
tank/src@snap2
tank/src@snap3
tank/src@snap4>"
	assertContains "The live rows keep the destination marked as snapshotted, so the seed refuses." \
		"$output" "dest_has=1"
}

test_reconcile_live_destination_snapshot_state_refreshes_stale_common_snapshot_when_destination_already_has_snapshots() {
	g_did_delete_dest_snapshots=1
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@snap1	111"
	g_src_snapshot_transfer_list=$(
		cat <<'EOF'
tank/src@snap2	222
tank/src@snap3	333
tank/src@snap4	444
EOF
	)
	zxfer_test_stage_source_records "tank/src@snap4	444
tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@snap4	444"
					return 0
				fi
				return 1
			}

			zxfer_reconcile_live_destination_snapshot_state "tank/src"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "Live reconciliation should refresh a stale cached common snapshot even when destination snapshots were already detected earlier." \
		"$output" "last=tank/src@snap4	444"
	assertContains "Live reconciliation should clear the pending transfer list when the destination already has the final snapshot." \
		"$output" "remaining=<>"
	assertContains "Refreshing a stale cached common snapshot should keep the destination marked as snapshotted." \
		"$output" "dest_has=1"
}

test_reconcile_live_destination_snapshot_state_clears_stale_common_snapshot_when_no_live_match_remains() {
	g_did_delete_dest_snapshots=1
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@snap1	111"
	g_src_snapshot_transfer_list=$(
		cat <<'EOF'
tank/src@snap2	222
tank/src@snap3	333
EOF
	)
	zxfer_test_stage_source_records "tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@unrelated	999"
					return 0
				fi
				return 1
			}

			zxfer_reconcile_live_destination_snapshot_state "tank/src"
			printf 'last=<%s>\n' "$g_last_common_snap"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "Live reconciliation should clear a cached common snapshot that no live destination snapshot still confirms." \
		"$output" "last=<>"
	assertContains "Live reconciliation should requeue the planned source range from the old anchor when no live common snapshot remains." \
		"$output" "remaining=<tank/src@snap1	111
tank/src@snap2	222
tank/src@snap3	333>"
	assertContains "A live destination with unrelated snapshots should still be marked as snapshotted so seed planning fails closed." \
		"$output" "dest_has=1"
}
