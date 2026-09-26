#!/bin/sh
# Live destination reconciliation and snapshot-transfer behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Print the live destination records of DEST (default g_actual_dest) the way
# the replication recheck reads them.
zxfer_test_live_destination_records() {
	zxfer_get_live_destination_record_file "${1:-$g_actual_dest}" || return
	zxfer_filter_snapshot_record_file_for_dataset \
		"$g_zxfer_live_destination_record_file_result" "${1:-$g_actual_dest}"
}

test_copy_snapshots_skips_when_no_pending_snapshots() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_src_snapshot_transfer_list=""
	log="$TEST_TMPDIR/copy_none.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_reconcile_live_destination_snapshot_state() {
			:
		}
		zxfer_rollback_destination_to_last_common_snapshot() {
			printf 'rollback\n' >>"$COPY_LOG"
		}
		zxfer_zfs_send_receive() {
			printf 'send\n' >>"$COPY_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "zxfer_copy_snapshots should stop early when there are no source snapshots to send." \
		"" "$(cat "$log")"
}

test_copy_snapshots_bootstraps_missing_destination_and_finishes_incremental() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1
tank/src@snap2"
	g_last_common_snap=""
	g_dest_has_snapshots=0
	log="$TEST_TMPDIR/copy_bootstrap.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=0
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Missing destinations should be seeded with the first snapshot, then resumed incrementally." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0
prev=tank/src@snap1 curr=tank/src@snap2 dest=backup/target/src bg=1" "$(cat "$log")"
}

test_copy_snapshots_reports_missing_destination_seed_message_to_stdout() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1
tank/src@snap2"
	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_option_v_verbose=1

	output=$(
		(
			zxfer_reconcile_live_destination_snapshot_state() {
				:
			}
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=0
			}
			zxfer_zfs_send_receive() {
				:
			}
			zxfer_copy_snapshots "tank/src"
		)
	)

	assertContains "Missing-destination bootstraps should keep the verbose seed message on stdout for operator-facing dry-run and integration traces." \
		"$output" "Destination dataset does not exist [backup/target/src]. Sending first snapshot [tank/src@snap1]"
}

test_copy_snapshots_stops_after_seeding_single_snapshot_into_missing_destination() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1"
	g_last_common_snap=""
	g_dest_has_snapshots=0
	log="$TEST_TMPDIR/copy_seed_single.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=0
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Single-snapshot bootstraps should stop after the seed receive." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0" "$(cat "$log")"
}

test_copy_snapshots_rechecks_live_destination_snapshots_before_reseeding() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base	111"
	zxfer_test_stage_source_records "tank/src@base	111"
	log="$TEST_TMPDIR/copy_live_recheck.log"
	: >"$log"

	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@base	111"
					return 0
				fi
				printf '%s\n' "$*" >>"$log"
				return 0
			}
			zxfer_zfs_send_receive() {
				printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$log"
			}

			zxfer_copy_snapshots "tank/src"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertEquals "A live destination snapshot recheck should prevent reseeding an existing dataset." \
		"" "$(cat "$log")"
	assertContains "The live destination snapshot should be promoted to the last common snapshot." \
		"$output" "last=tank/src@base	111"
	assertContains "The destination should be marked as already containing snapshots after the live recheck." \
		"$output" "dest_has=1"
	assertContains "No further source snapshots should remain once the live common snapshot is confirmed." \
		"$output" "remaining=<>"
}

test_copy_snapshots_live_probes_initial_root_before_bootstrapping_cached_missing_destination() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	zxfer_set_actual_dest "$g_initial_source"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@snap1
tank/src@snap2"
	probe_log="$TEST_TMPDIR/copy_root_missing_probe.log"
	send_log="$TEST_TMPDIR/copy_root_missing_send.log"
	: >"$probe_log"
	: >"$send_log"
	zxfer_mark_destination_root_missing_in_cache "$g_destination"

	(
		PROBE_LOG="$probe_log"
		SEND_LOG="$send_log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/target/src" ]; then
				printf 'probe %s\n' "$*" >>"$PROBE_LOG"
				printf '%s\n' "cannot open 'backup/target/src': dataset does not exist" >&2
				return 1
			fi
			printf 'unexpected %s\n' "$*" >>"$PROBE_LOG"
			return 1
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$SEND_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Initial-root bootstraps should live-probe once before trusting cached-missing discovery state." \
		"probe list -H backup/target/src" "$(cat "$probe_log")"
	assertEquals "Initial-root bootstraps should still seed and then resume incrementally when the live probe confirms the destination is missing." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0
prev=tank/src@snap1 curr=tank/src@snap2 dest=backup/target/src bg=1" "$(cat "$send_log")"
}

test_copy_snapshots_uses_existing_empty_initial_root_when_cached_missing_state_is_stale() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	zxfer_set_actual_dest "$g_initial_source"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@snap1"
	probe_log="$TEST_TMPDIR/copy_root_stale_missing_probe.log"
	send_log="$TEST_TMPDIR/copy_root_stale_missing_send.log"
	: >"$probe_log"
	: >"$send_log"
	zxfer_mark_destination_root_missing_in_cache "$g_destination"

	(
		PROBE_LOG="$probe_log"
		SEND_LOG="$send_log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/target/src" ]; then
				printf 'probe %s\n' "$*" >>"$PROBE_LOG"
				return 0
			fi
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				return 0
			fi
			printf 'unexpected %s\n' "$*" >>"$PROBE_LOG"
			return 1
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${5:-}" >>"$SEND_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "A cached-missing initial root should be live-probed before seed planning." \
		"probe list -H backup/target/src" "$(cat "$probe_log")"
	assertEquals "When the live probe finds an existing empty initial root, zxfer should seed it with the existing-destination receive path." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0 force=-F" "$(cat "$send_log")"
}

# Inspect's plan of a dataset with two pending snapshots: the published plan
# and the planner results it came from.
zxfer_test_prepare_unchanged_live_plan() {
	g_actual_dest="backup/target/src"
	g_zxfer_plan_common_snapshot="tank/src@base	111"
	g_zxfer_plan_transfer_list="tank/src@next	222
tank/src@final	333"
	g_zxfer_plan_destination_records="backup/target/src@base	111"
	g_zxfer_plan_dest_has_snapshots=1
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" \
		"$g_zxfer_plan_transfer_list" 1
}

test_reconcile_live_destination_snapshot_state_reuses_unchanged_inspection_after_live_read() {
	view_log="$TEST_TMPDIR/unchanged_plan_live_read.log"
	: >"$view_log"
	output=$(
		zxfer_test_prepare_unchanged_live_plan
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_destination_zfs_cmd() {
			printf 'live\n' >>"$view_log"
			printf '%s\n' "$g_zxfer_plan_destination_records"
		}
		zxfer_plan_dataset_snapshots() { exit 23; }
		zxfer_reconcile_live_destination_snapshot_state "tank/src"
		printf 'common=%s\npending=%s\nhas=%s\n' "$g_last_common_snap" \
			"$g_src_snapshot_transfer_list" "$g_dest_has_snapshots"
	)
	status=$?
	assertEquals "An unchanged inspected plan should not be planned again." 0 "$status"
	assertEquals "Reusing classification still requires a successful fresh destination read." "live" "$(cat "$view_log")"
	assertEquals "The entire verified common snapshot and pending range should remain intact." \
		"common=tank/src@base	111
pending=tank/src@next	222
tank/src@final	333
has=1" "$output"
}

test_reconcile_live_destination_snapshot_state_replans_when_live_rows_changed() {
	for changed in none guid extra empty; do
		status=0
		(
			zxfer_test_prepare_unchanged_live_plan
			case $changed in
			none) LIVE_ROWS=$g_zxfer_plan_destination_records ;;
			guid) LIVE_ROWS="backup/target/src@base	999" ;;
			extra) LIVE_ROWS="$g_zxfer_plan_destination_records
backup/target/src@extra	555" ;;
			empty) LIVE_ROWS="" ;;
			esac
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_run_destination_zfs_cmd() {
				[ -z "$LIVE_ROWS" ] || printf '%s\n' "$LIVE_ROWS"
			}
			zxfer_plan_dataset_snapshots() { exit 23; }
			zxfer_reconcile_live_destination_snapshot_state "tank/src"
		) || status=$?
		if [ "$changed" = none ]; then
			assertEquals "Unchanged live rows may reuse the inspected plan." 0 "$status"
		else
			assertEquals "A $changed change to the live rows must be planned again." 23 "$status"
		fi
	done
}

test_reconcile_live_destination_snapshot_state_does_not_reuse_inspection_after_failed_read() {
	status=0
	output=$(
		zxfer_test_prepare_unchanged_live_plan
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_destination_zfs_cmd() { return 29; }
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_reconcile_live_destination_snapshot_state "tank/src"
	) || status=$?
	assertEquals "A live read failure must abort even when the retained inspection matches the plan." 1 "$status"
	assertContains "The failure should identify the live destination lookup." \
		"$output" "Failed to retrieve live destination snapshots for [backup/target/src]"
}

test_reconcile_live_destination_snapshot_state_keeps_newest_matching_snapshot() {
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
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
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
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
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
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
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
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
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

test_reconcile_live_destination_snapshot_state_live_rechecks_cached_missing_children() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	g_actual_dest="backup/target/src/child"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list=$(
		cat <<'EOF'
tank/src/child@base	111
EOF
	)
	zxfer_test_stage_source_records "tank/src/child@base	111"
	probe_log="$TEST_TMPDIR/reconcile_live_child_probe.log"
	: >"$probe_log"
	zxfer_mark_destination_root_missing_in_cache "$g_destination"

	output=$(
		(
			PROBE_LOG="$probe_log"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/target/src/child" ]; then
					printf '%s\n' "$*" >>"$PROBE_LOG"
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
					[ "$5" = "-t" ] && [ "$6" = "snapshot" ] && [ "$7" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src/child@base	111"
					return 0
				fi
				return 1
			}

			zxfer_reconcile_live_destination_snapshot_state "tank/src/child"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertEquals "Cached-missing child datasets should still perform a live existence probe because a recursive parent receive may have created them earlier in the iteration." \
		"list -H backup/target/src/child" "$(cat "$probe_log")"
	assertContains "A successful live child recheck served from the batched view should still promote the matching snapshot to the last common anchor." \
		"$output" "last=tank/src/child@base	111"
	assertContains "A successful live child recheck should clear the remaining transfer list once the destination already has the seed snapshot." \
		"$output" "remaining=<>"
	assertContains "A successful live child recheck should still mark the destination as snapshotted." \
		"$output" "dest_has=1"
}

# The next five tests pin the per-dataset dirty live destination view: one
# batched listing of the run's destination root, captured at most once per
# pass, serves every covered dataset's recheck; a self-mutation marks only
# the mutated dataset dirty so its later rechecks are depth-1 live listings
# while unmarked datasets keep the batched view; a failed batched listing
# aborts; and a -Y pass boundary always forces a fresh listing.

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

test_copy_snapshots_live_recheck_requires_matching_guid() {
	zxfer_test_stage_source_records "tank/src@base	111"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base	111"
	log="$TEST_TMPDIR/copy_live_recheck_guid.log"
	: >"$log"

	set +e
	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@base	999"
					return 0
				fi
				printf '%s\n' "$*" >>"$log"
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_zfs_send_receive() {
				printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$log"
			}

			zxfer_copy_snapshots "tank/src"
		)
	)
	status=$?

	assertEquals "A same-named but unrelated destination snapshot should fail closed instead of seeding an existing snapshotted dataset." \
		1 "$status"
	assertContains "The failure should explain that there is no common guid anchor for the existing destination dataset." \
		"$output" "Destination dataset [backup/target/src] has snapshots but none share a common guid with the source."
	assertEquals "No send should be attempted when guid matching leaves an existing destination without a common snapshot." \
		"" "$(cat "$log")"
}

# Inspect's plan while the destination held snap1..snap3: anchor snap3 with
# snap4 pending. LIVE_ROWS is what the live recheck lists.
zxfer_test_prepare_anchor_drift_plan() {
	zxfer_test_stage_source_records "tank/src@snap4	444
tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111"
	g_actual_dest="backup/target/src"
	g_zxfer_plan_destination_records="backup/target/src@snap1	111
backup/target/src@snap2	222
backup/target/src@snap3	333"
	zxfer_publish_snapshot_transfer_plan "tank/src@snap3	333" "tank/src@snap4	444" 1
}

# Purpose: Run zxfer_copy_snapshots against LIVE_ROWS, logging every other
# destination command and each send to COPY_LOG.
zxfer_test_copy_with_live_rows() {
	zxfer_probe_destination_existence() {
		g_zxfer_destination_exists_result=1
	}
	zxfer_run_destination_zfs_cmd() {
		if [ "$*" = "list -H -d 1 -o name,guid -t snapshot backup/target/src" ]; then
			[ -z "$LIVE_ROWS" ] || printf '%s\n' "$LIVE_ROWS"
			return 0
		fi
		printf '%s\n' "$*" >>"$COPY_LOG"
	}
	zxfer_zfs_send_receive() {
		printf 'prev=%s curr=%s bg=%s force=%s\n' "$1" "$2" "$4" "${5:-}" >>"$COPY_LOG"
	}
	zxfer_throw_error() {
		printf '%s\n' "$1"
		exit 1
	}
	zxfer_copy_snapshots "tank/src"
}

test_copy_snapshots_refuses_when_live_anchor_was_destroyed() {
	zxfer_test_prepare_anchor_drift_plan
	log="$TEST_TMPDIR/copy_anchor_destroyed.log"
	: >"$log"

	status=0
	output=$(
		COPY_LOG=$log
		LIVE_ROWS="backup/target/src@snap1	111
backup/target/src@snap2	222"
		zxfer_test_copy_with_live_rows
	) || status=$?

	assertEquals "An older common snapshot must not replace a destroyed anchor." 1 "$status"
	assertContains "The refusal should name the missing common snapshot." \
		"$output" "Destination dataset [backup/target/src] has snapshots but none share a common guid with the source."
	assertEquals "Nothing may be sent or received after the anchor disappeared." "" "$(cat "$log")"
}

test_copy_snapshots_reseeds_emptied_destination_from_the_inspected_anchor() {
	zxfer_test_prepare_anchor_drift_plan
	log="$TEST_TMPDIR/copy_destination_emptied.log"
	: >"$log"

	(
		COPY_LOG=$log
		LIVE_ROWS=""
		zxfer_test_copy_with_live_rows
	)

	assertEquals "An emptied destination should be re-seeded from the anchor, not from the oldest source snapshot." \
		"prev= curr=tank/src@snap3 bg=0 force=-F
prev=tank/src@snap3 curr=tank/src@snap4 bg=1 force=" "$(cat "$log")"
}

test_copy_snapshots_skips_send_when_live_destination_already_has_final_snapshot() {
	zxfer_test_stage_source_records "tank/src@snap4	444
tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111"
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
	log="$TEST_TMPDIR/copy_live_tip_already_present.log"
	: >"$log"

	output=$(
		(
			COPY_LOG="$log"
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@snap4	444"
					return 0
				fi
				printf '%s\n' "$*" >>"$COPY_LOG"
				return 0
			}
			zxfer_rollback_destination_to_last_common_snapshot() {
				printf '%s\n' "rollback" >>"$COPY_LOG"
			}
			zxfer_zfs_send_receive() {
				printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
			}

			zxfer_copy_snapshots "tank/src"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=<%s>\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertEquals "Copy planning should not roll back or resend when a live refresh confirms the destination already has the final snapshot." \
		"" "$(cat "$log")"
	assertContains "The live destination tip should replace the stale cached common snapshot before copy planning decides whether a send is needed." \
		"$output" "last=tank/src@snap4	444"
	assertContains "Copy planning should clear the remaining transfer list when the live destination already has the final snapshot." \
		"$output" "remaining=<>"
}

test_copy_snapshots_live_rechecks_empty_cached_transfer_list_before_skipping() {
	zxfer_test_stage_source_records "tank/src@base	111"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@base	111"
	g_src_snapshot_transfer_list=""
	log="$TEST_TMPDIR/copy_empty_transfer_live_recheck.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf 'live-list\n' >>"$COPY_LOG"
				return 0
			fi
			return 1
		}
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${5:-}" >>"$COPY_LOG"
		}

		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "An empty cached transfer list should still live-recheck the destination and reseed when the cached common snapshot disappeared." \
		"live-list
prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
}

test_copy_snapshots_live_rechecks_already_final_state_before_skipping() {
	zxfer_test_stage_source_records "tank/src@base	111"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@base	111"
	g_src_snapshot_transfer_list="tank/src@base	111"
	log="$TEST_TMPDIR/copy_final_live_recheck.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf 'live-list\n' >>"$COPY_LOG"
				return 0
			fi
			return 1
		}
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${5:-}" >>"$COPY_LOG"
		}

		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Cached already-final state should still be live-rechecked before deciding there is nothing to send." \
		"live-list
prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
}

test_copy_snapshots_seeds_existing_destination_when_live_probe_confirms_no_snapshots() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base"
	g_option_F_force_rollback=""
	log="$TEST_TMPDIR/copy_live_empty_seed.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				return 0
			fi
			printf '%s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${5:-}" >>"$COPY_LOG"
		}

		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "A live recheck that finds no snapshots should allow seeding an existing destination." \
		"prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
	assertEquals "Seed receives should not mutate the parsed -F option state." \
		"" "$g_option_F_force_rollback"
}

test_copy_snapshots_reports_existing_empty_destination_seed_message_to_stdout() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base"
	g_option_F_force_rollback=""
	g_option_v_verbose=1

	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					return 0
				fi
				return 1
			}
			zxfer_zfs_send_receive() {
				:
			}

			zxfer_copy_snapshots "tank/src"
		)
	)

	assertContains "Existing empty destinations should keep the verbose seed-branch message on stdout." \
		"$output" "Destination dataset [backup/target/src] exists but has no snapshots. Seeding with [tank/src@base]"
	assertContains "Existing empty destination seeding should still report the temporary internal -F enablement." \
		"$output" "Temporarily enabling receive-side -F to seed existing empty destination dataset [backup/target/src]."
}

test_copy_snapshots_ignores_descendant_snapshots_when_rechecking_parent_dataset() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base"
	g_option_F_force_rollback=""
	log="$TEST_TMPDIR/copy_live_child_only_seed.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf '%s\n' "backup/target/src/child@base	999"
				return 0
			fi
			printf '%s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${5:-}" >>"$COPY_LOG"
		}

		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Child-dataset snapshots should not block seeding the current dataset when the current dataset has no snapshots." \
		"prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
	assertEquals "Live-recheck seeding should not mutate the parsed -F option state." \
		"" "$g_option_F_force_rollback"
}

test_copy_snapshots_reports_destination_probe_failures() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1
tank/src@snap2"
	g_last_common_snap=""
	g_dest_has_snapshots=0

	set +e
	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/target/src] exists: permission denied"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_copy_snapshots "tank/src"
		)
	)
	status=$?

	assertEquals "zxfer_copy_snapshots should fail closed when destination existence checks fail." 1 "$status"
	assertContains "zxfer_copy_snapshots should surface the destination probe failure." \
		"$output" "Failed to determine whether destination dataset [backup/target/src] exists: permission denied"
}

test_copy_snapshots_reports_live_snapshot_recheck_failures() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base"

	set +e
	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_copy_snapshots "tank/src"
		)
	)
	status=$?

	assertEquals "Live destination snapshot recheck failures should abort instead of reseeding." 1 "$status"
	assertContains "Live destination snapshot recheck failures should preserve the destination context." \
		"$output" "Failed to retrieve live destination snapshots for [backup/target/src]: ssh timeout"
}

test_copy_snapshots_skips_when_last_common_matches_final_snapshot() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_option_V_very_verbose=1
	g_last_common_snap="tank/src@snap2	222"
	g_src_snapshot_transfer_list="tank/src@snap1	111
tank/src@snap2	222"
	log="$TEST_TMPDIR/copy_skip_same.log"
	: >"$log"

	output=$(
		(
			COPY_LOG="$log"
			zxfer_reconcile_live_destination_snapshot_state() {
				printf 'recheck %s\n' "$1" >>"$COPY_LOG"
			}
			zxfer_rollback_destination_to_last_common_snapshot() {
				printf 'rollback\n' >>"$COPY_LOG"
			}
			zxfer_zfs_send_receive() {
				printf 'send\n' >>"$COPY_LOG"
			}
			zxfer_copy_snapshots "tank/src"
		) 2>&1
	)

	assertEquals "No rollback or transfer should occur when the last common snapshot is already the final one." \
		"recheck tank/src" "$(cat "$log")"
	assertContains "The skip should be reported under -V." \
		"$output" "No new snapshots to copy for backup/target/src."
}

test_copy_snapshots_skips_rollback_when_deletions_left_no_new_sends() {
	zxfer_test_stage_source_records "tank/src@base	111"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_option_V_very_verbose=1
	g_last_common_snap="tank/src@base	111"
	g_src_snapshot_transfer_list=""
	log="$TEST_TMPDIR/copy_skip_rollback.log"
	: >"$log"

	output=$(
		(
			COPY_LOG="$log"
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@base	111"
					return 0
				fi
				printf '%s\n' "$*" >>"$COPY_LOG"
				return 0
			}
			zxfer_zfs_send_receive() {
				printf 'send\n' >>"$COPY_LOG"
			}
			zxfer_copy_snapshots "tank/src"
		) 2>&1
	)

	assertEquals "Deleting extra destination snapshots without any pending sends should not trigger rollback." \
		"" "$(cat "$log")"
	assertContains "The live re-plan should leave nothing to copy." \
		"$output" "No snapshots to copy, skipping destination dataset: backup/target/src."
}

test_copy_snapshots_does_not_pre_rollback_after_deletions_without_force_flag() {
	zxfer_test_stage_source_records "tank/src@snap2	222
tank/src@snap1	111"
	g_option_F_force_rollback=""
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_last_common_snap="tank/src@snap1	111"
	g_src_snapshot_transfer_list="tank/src@snap1	111
tank/src@snap2	222"
	log="$TEST_TMPDIR/copy_no_force_no_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf '%s\n' "backup/target/src@snap1	111"
				return 0
			fi
			printf 'rollback %s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zxfer_zfs_send_receive() {
			printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Snapshot deletion without -F should not trigger a destructive pre-send rollback." \
		"send tank/src@snap1 tank/src@snap2 backup/target/src 1" "$(cat "$log")"
}

test_copy_snapshots_does_not_pre_rollback_after_older_snapshot_deletions() {
	zxfer_test_stage_source_records "tank/src@snap2	222
tank/src@snap1	111"
	g_option_F_force_rollback="-F"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=0
	g_last_common_snap="tank/src@snap1	111"
	g_src_snapshot_transfer_list="tank/src@snap1	111
tank/src@snap2	222"
	log="$TEST_TMPDIR/copy_old_deletes_no_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf '%s\n' "backup/target/src@snap1	111"
				return 0
			fi
			printf 'rollback %s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zxfer_zfs_send_receive() {
			printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		zxfer_copy_snapshots "tank/src"
	)

	assertEquals "Deleting only older destination snapshots should not trigger a pre-send rollback even when -F is active." \
		"send tank/src@snap1 tank/src@snap2 backup/target/src 1" "$(cat "$log")"
}
