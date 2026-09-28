#!/bin/sh
# zxfer_copy_snapshots tests for src/zxfer_replication.sh: seeding, the
# rollback after -d deletes, and the send, with the plan's live re-plan of a
# dataset whose -d destroy ran as a collaborator (those cases set
# g_did_delete_dest_snapshots=1).
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
	g_did_delete_dest_snapshots=1
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

test_copy_snapshots_live_probes_cached_missing_child_before_bootstrapping() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_option_R_recursive="tank/src"
	zxfer_set_actual_dest "tank/src/child"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src/child@base	111"
	probe_log="$TEST_TMPDIR/copy_child_missing_probe.log"
	send_log="$TEST_TMPDIR/copy_child_missing_send.log"
	: >"$probe_log"
	: >"$send_log"
	zxfer_mark_destination_root_missing_in_cache "$g_destination"

	(
		PROBE_LOG="$probe_log"
		SEND_LOG="$send_log"
		zxfer_run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/target/src/child" ]; then
				printf 'probe %s\n' "$*" >>"$PROBE_LOG"
				printf '%s\n' "cannot open 'backup/target/src/child': dataset does not exist" >&2
				return 1
			fi
			printf 'unexpected %s\n' "$*" >>"$PROBE_LOG"
			return 1
		}
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$SEND_LOG"
		}
		zxfer_copy_snapshots "tank/src/child"
	)

	assertEquals "A cached-missing child should be live-probed once, and never listed, before its full receive: discovery may be minutes old." \
		"probe list -H backup/target/src/child" "$(cat "$probe_log")"
	assertEquals "A child the live probe confirms missing should get the full receive." \
		"prev= curr=tank/src/child@base dest=backup/target/src/child bg=0" "$(cat "$send_log")"
}

test_copy_snapshots_live_recheck_requires_matching_guid() {
	zxfer_test_stage_source_records "tank/src@base	111"
	g_did_delete_dest_snapshots=1
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
# snap4 pending, after a -d destroy on the dataset. LIVE_ROWS is what the live
# recheck lists.
zxfer_test_prepare_anchor_drift_plan() {
	g_did_delete_dest_snapshots=1
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
	g_did_delete_dest_snapshots=1
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
	g_did_delete_dest_snapshots=1
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
	zxfer_test_stage_source_records "tank/src@base	111"
	g_did_delete_dest_snapshots=1
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
	zxfer_test_stage_source_records "tank/src@base	111"
	g_did_delete_dest_snapshots=1
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
	zxfer_test_stage_source_records "tank/src@base	111"
	g_did_delete_dest_snapshots=1
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
	g_did_delete_dest_snapshots=1
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_last_common_snap=""
	g_src_snapshot_transfer_list="tank/src@base"
	send_log="$TEST_TMPDIR/copy_recheck_failure_send.log"
	: >"$send_log"

	set +e
	output=$(
		(
			SEND_LOG="$send_log"
			zxfer_rollback_destination_to_last_common_snapshot() {
				:
			}
			zxfer_zfs_send_receive() {
				printf 'send\n' >>"$SEND_LOG"
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
	assertEquals "No send may be planned after a failed live listing." "" "$(cat "$send_log")"
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
