#!/bin/sh
# Live destination reconciliation and snapshot-transfer behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
		zxfer_copy_snapshots
	)

	assertEquals "zxfer_copy_snapshots should stop early when there are no source snapshots to send." \
		"" "$(cat "$log")"
}

test_copy_snapshots_bootstraps_missing_destination_and_finishes_incremental() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
	g_last_common_snap=""
	g_dest_has_snapshots=0
	log="$TEST_TMPDIR/copy_bootstrap.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		zxfer_copy_snapshots
	)

	assertEquals "Missing destinations should be seeded with the first snapshot, then resumed incrementally." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0
prev=tank/src@snap1 curr=tank/src@snap2 dest=backup/target/src bg=1" "$(cat "$log")"
}

test_copy_snapshots_reports_missing_destination_seed_message_to_stdout() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
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
			zxfer_exists_destination() {
				printf '0\n'
			}
			zxfer_zfs_send_receive() {
				:
			}
			zxfer_copy_snapshots
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
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		zxfer_copy_snapshots
	)

	assertEquals "Single-snapshot bootstraps should stop after the seed receive." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0" "$(cat "$log")"
}

test_copy_snapshots_rechecks_live_destination_snapshots_before_reseeding() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base	111"
	log="$TEST_TMPDIR/copy_live_recheck.log"
	: >"$log"

	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_copy_snapshots
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
			printf 'remaining=%s\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertEquals "A live destination snapshot recheck should prevent reseeding an existing dataset." \
		"" "$(cat "$log")"
	assertContains "The live destination snapshot should be promoted to the last common snapshot." \
		"$output" "last=tank/src@base	111"
	assertContains "The destination should be marked as already containing snapshots after the live recheck." \
		"$output" "dest_has=1"
	assertContains "No further source snapshots should remain once the live common snapshot is confirmed." \
		"$output" "remaining="
}

test_copy_snapshots_live_probes_initial_root_before_bootstrapping_cached_missing_destination() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	zxfer_set_actual_dest "$g_initial_source"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
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
		zxfer_copy_snapshots
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
		zxfer_copy_snapshots
	)

	assertEquals "A cached-missing initial root should be live-probed before seed planning." \
		"probe list -H backup/target/src" "$(cat "$probe_log")"
	assertEquals "When the live probe finds an existing empty initial root, zxfer should seed it with the existing-destination receive path." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0 force=-F" "$(cat "$send_log")"
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

	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_reconcile_live_destination_snapshot_state
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=%s\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "The live reconciliation should keep the newest matching source snapshot as the common anchor." \
		"$output" "last=tank/src@snap3	333"
	assertContains "Only snapshots after the newest live common snapshot should remain queued for transfer." \
		"$output" "remaining=tank/src@snap4	444"
	assertContains "A successful live reconciliation should still mark the destination as snapshotted." \
		"$output" "dest_has=1"
}

test_reconcile_live_destination_snapshot_state_fetches_source_identities_for_name_only_transfer_lists() {
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

	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
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
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1" = "source" ] && [ "$2" = "tank/src" ]; then
					cat <<'EOF'
tank/src@snap1	111
tank/src@snap2	222
tank/src@snap3	333
tank/src@snap4	444
EOF
					return 0
				fi
				return 1
			}

			zxfer_reconcile_live_destination_snapshot_state
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=%s\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "Name-only transfer lists should still reconcile against live destination snapshots using source guid validation." \
		"$output" "last=tank/src@snap3	333"
	assertContains "Only snapshots after the newest guid-confirmed live common snapshot should remain queued for transfer." \
		"$output" "remaining=tank/src@snap4	444"
	assertContains "A successful guid-backed live reconciliation should still mark the destination as snapshotted." \
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

	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_reconcile_live_destination_snapshot_state
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=%s\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertContains "Live reconciliation should refresh a stale cached common snapshot even when destination snapshots were already detected earlier." \
		"$output" "last=tank/src@snap4	444"
	assertContains "Live reconciliation should clear the pending transfer list when the destination already has the final snapshot." \
		"$output" "remaining="
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

	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_reconcile_live_destination_snapshot_state
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

			zxfer_reconcile_live_destination_snapshot_state
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=%s\n' "$g_src_snapshot_transfer_list"
			printf 'dest_has=%s\n' "$g_dest_has_snapshots"
		)
	)

	assertEquals "Cached-missing child datasets should still perform a live existence probe because a recursive parent receive may have created them earlier in the iteration." \
		"list -H backup/target/src/child" "$(cat "$probe_log")"
	assertContains "A successful live child recheck served from the batched view should still promote the matching snapshot to the last common anchor." \
		"$output" "last=tank/src/child@base	111"
	assertContains "A successful live child recheck should clear the remaining transfer list once the destination already has the seed snapshot." \
		"$output" "remaining="
	assertContains "A successful live child recheck should still mark the destination as snapshotted." \
		"$output" "dest_has=1"
}

# The next five tests pin the generation-gated live destination view: one
# batched listing of the run's destination root serves every covered
# dataset's recheck until THIS RUN mutates the destination, every
# self-mutation forces a fresh listing, a failed batched listing aborts, and
# a -Y pass boundary always forces a fresh listing.

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
			printf 'root=<%s>\n' "$(zxfer_get_live_destination_snapshots 2>&1)"
			g_actual_dest="backup/target/src/child"
			zxfer_ensure_live_destination_snapshot_view
			printf 'child=<%s>\n' "$(zxfer_get_live_destination_snapshots 2>&1)"
		)
	)

	assertEquals "Two covered datasets with no destination mutation between them must be served from exactly one batched listing." \
		"view" "$(cat "$view_log")"
	assertContains "The batched view must serve the root dataset exactly its own records." \
		"$output" "root=<backup/target/src@snap1	111>"
	assertContains "The batched view must serve the child dataset exactly its own records." \
		"$output" "child=<backup/target/src/child@snap1	211>"
}

test_live_destination_view_refreshes_after_destination_mutation_bump() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	view_log="$TEST_TMPDIR/live_view_bump.log"
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

		g_actual_dest="backup/target/src"
		zxfer_ensure_live_destination_snapshot_view
		zxfer_get_live_destination_snapshots >/dev/null 2>&1
		zxfer_bump_destination_mutation_generation
		g_actual_dest="backup/target/src/child"
		zxfer_ensure_live_destination_snapshot_view
		zxfer_get_live_destination_snapshots >/dev/null 2>&1
	)

	assertEquals "A destination mutation between two rechecks must force exactly one fresh batched listing for the second dataset." \
		"view
view" "$(cat "$view_log")"
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
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_copy_snapshots
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

test_live_destination_view_reap_time_property_invalidation_bumps_generation() {
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
			return 1
		}

		g_actual_dest="backup/target/src"
		zxfer_ensure_live_destination_snapshot_view
		# Reap-time receive completion runs this choke point in the main
		# shell (zxfer_finalize_supervised_send_job_success); it must bump
		# the generation so the next dataset's recheck refreshes the view.
		zxfer_invalidate_destination_property_mutation_cache "backup/target/src"
		g_actual_dest="backup/target/src/child"
		zxfer_ensure_live_destination_snapshot_view
	)

	assertEquals "The shared mutation choke point used at -j reap time must invalidate the batched view for the next recheck." \
		"view
view" "$(cat "$view_log")"
}

test_live_destination_view_pass_boundary_forces_fresh_batched_listing() {
	g_option_Y_yield_iterations=4
	g_test_max_yield_iterations=8
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
			# Pass shape whose last refresh postdates its last destination
			# mutation: dataset A's receive completes (bump), then a trailing
			# in-sync dataset B's recheck refreshes the view, so the stamp
			# matches the generation when the pass ends. Only the pass
			# boundary can force the next pass's fresh listing here.
			g_actual_dest="backup/target/src"
			zxfer_ensure_live_destination_snapshot_view
			zxfer_bump_destination_mutation_generation
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

	assertEquals "A -Y pass boundary must invalidate the batched live view so the next pass's first recheck captures a fresh listing even when the previous pass ended with stamp == generation." \
		"pass 1
view
view
pass 2
view
view" "$(cat "$view_log")"
}

test_reconcile_live_destination_snapshot_state_reports_source_identity_lookup_failures() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list=$(
		cat <<'EOF'
tank/src@snap1
tank/src@snap2
EOF
	)

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
					[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
					[ "$9" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@snap1	111"
					return 0
				fi
				return 1
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_reconcile_live_destination_snapshot_state
		)
	)
	status=$?

	assertEquals "Live destination reconciliation should fail closed when source guid records cannot be retrieved." 1 "$status"
	assertContains "Live destination reconciliation should identify the source dataset whose guid lookup failed." \
		"$output" "Failed to retrieve source snapshot identities for [tank/src]."
}

test_copy_snapshots_live_recheck_requires_matching_guid() {
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
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_copy_snapshots
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

test_copy_snapshots_skips_send_when_live_destination_already_has_final_snapshot() {
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
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_copy_snapshots
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'remaining=%s\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertEquals "Copy planning should not roll back or resend when a live refresh confirms the destination already has the final snapshot." \
		"" "$(cat "$log")"
	assertContains "The live destination tip should replace the stale cached common snapshot before copy planning decides whether a send is needed." \
		"$output" "last=tank/src@snap4	444"
	assertContains "Copy planning should clear the remaining transfer list when the live destination already has the final snapshot." \
		"$output" "remaining="
}

test_copy_snapshots_live_rechecks_empty_cached_transfer_list_before_skipping() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@base	111"
	g_src_snapshot_transfer_list=""
	log="$TEST_TMPDIR/copy_empty_transfer_live_recheck.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_exists_destination() {
			printf '1\n'
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

		zxfer_copy_snapshots
	)

	assertEquals "An empty cached transfer list should still live-recheck the destination and reseed when the cached common snapshot disappeared." \
		"live-list
prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
}

test_copy_snapshots_live_rechecks_already_final_state_before_skipping() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@base	111"
	g_src_snapshot_transfer_list="tank/src@base	111"
	log="$TEST_TMPDIR/copy_final_live_recheck.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_exists_destination() {
			printf '1\n'
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

		zxfer_copy_snapshots
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
		zxfer_reconcile_live_destination_snapshot_state() {
			:
		}
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_exists_destination() {
			printf '1\n'
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

		zxfer_copy_snapshots
	)

	assertEquals "A fresh live probe should allow seeding when an existing destination no longer has snapshots." \
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
			zxfer_reconcile_live_destination_snapshot_state() {
				:
			}
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_exists_destination() {
				printf '1\n'
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

			zxfer_copy_snapshots
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
		zxfer_reconcile_live_destination_snapshot_state() {
			:
		}
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_exists_destination() {
			printf '1\n'
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

		zxfer_copy_snapshots
	)

	assertEquals "Child-dataset snapshots should not block seeding the current dataset when the current dataset has no snapshots." \
		"prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
	assertEquals "Live-recheck seeding should not mutate the parsed -F option state." \
		"" "$g_option_F_force_rollback"
}

test_copy_snapshots_reports_destination_probe_failures() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
	g_last_common_snap=""
	g_dest_has_snapshots=0

	set +e
	output=$(
		(
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/target/src] exists: permission denied"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_copy_snapshots
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
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_copy_snapshots
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
	g_last_common_snap="tank/src@snap2"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
	log="$TEST_TMPDIR/copy_skip_same.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			:
		}
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf '%s\n' "backup/target/src@snap2"
				return 0
			fi
			return 1
		}
		zxfer_zfs_send_receive() {
			printf 'send\n' >>"$COPY_LOG"
		}
		zxfer_copy_snapshots
	)

	assertEquals "No transfer should occur when the last common snapshot is already the final one." "" "$(cat "$log")"
}

test_copy_snapshots_skips_rollback_when_deletions_left_no_new_sends() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_last_common_snap="tank/src@base"
	g_src_snapshot_transfer_list="tank/src@base"
	log="$TEST_TMPDIR/copy_skip_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_rollback_destination_to_last_common_snapshot() {
			printf 'rollback\n' >>"$COPY_LOG"
		}
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-d" ] && [ "$4" = "1" ] && [ "$5" = "-o" ] &&
				[ "$6" = "name,guid" ] && [ "$7" = "-t" ] && [ "$8" = "snapshot" ] &&
				[ "$9" = "backup/target/src" ]; then
				printf '%s\n' "backup/target/src@base"
				return 0
			fi
			return 1
		}
		zxfer_zfs_send_receive() {
			printf 'send\n' >>"$COPY_LOG"
		}
		zxfer_copy_snapshots
	)

	assertEquals "Deleting extra destination snapshots without any pending sends should not trigger rollback." \
		"" "$(cat "$log")"
}

test_copy_snapshots_does_not_pre_rollback_after_deletions_without_force_flag() {
	g_option_F_force_rollback=""
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_last_common_snap="tank/src@snap1	111"
	g_src_snapshot_transfer_list="tank/src@snap1	111 tank/src@snap2	222"
	log="$TEST_TMPDIR/copy_no_force_no_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_exists_destination() {
			printf '1\n'
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
		zxfer_copy_snapshots
	)

	assertEquals "Snapshot deletion without -F should not trigger a destructive pre-send rollback." \
		"send tank/src@snap1 tank/src@snap2 backup/target/src 1" "$(cat "$log")"
}

test_copy_snapshots_does_not_pre_rollback_after_older_snapshot_deletions() {
	g_option_F_force_rollback="-F"
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=0
	g_last_common_snap="tank/src@snap1	111"
	g_src_snapshot_transfer_list="tank/src@snap1	111 tank/src@snap2	222"
	log="$TEST_TMPDIR/copy_old_deletes_no_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		zxfer_exists_destination() {
			printf '1\n'
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
		zxfer_copy_snapshots
	)

	assertEquals "Deleting only older destination snapshots should not trigger a pre-send rollback even when -F is active." \
		"send tank/src@snap1 tank/src@snap2 backup/target/src 1" "$(cat "$log")"
}
