#!/bin/sh
#
# shunit2 tests for src/zxfer_replication.sh: what only a unit test can pin.
# The black-box suites pin the rest through the real launcher: seeding, the
# re-plan after a -d destroy, the -j ready queue, the post-seed property
# pass, -s/-m snapshots, dry-run previews and -Y passes in
# tests/test_contract_send_receive.sh; ordering, divergence, -d/-g and
# failure stages in tests/test_contract_planning.sh; and the fail-closed
# sweep in tests/test_contract_failures.sh.
#
# Pinned here: operand validation, the -U probe decision and the iteration
# list's order (one table each); the -s/-m snapshot name; the guard that
# never sends a snapshot incrementally to itself; and fail-closed branches no
# mock can reach (a discovery or backup preflight that fails without
# throwing, awk, sort and record-split failures) plus the post-seed queue's
# dedupe and sort.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/replication_fixtures.sh
. "$TESTS_DIR/helpers/replication_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_replication"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_replication_fixture_setup
}

# ---------------------------------------------------------------------------
# Pure decisions, one table each. Table cells spell LF as \n and TAB as \t.

# Row: -R|-N|destination|-c|-m|status|output. A usage or runtime error
# prints its message; status 0 prints the published roots and the
# trailing-slash flag. A -c row runs where no svcadm can be found.
test_prepare_zfs_mode_roots_validates_the_source_and_destination_operands() {
	no_svcadm_dir="$TEST_TMPDIR/no_svcadm"
	mkdir -p "$no_svcadm_dir"
	while IFS='|' read -r row_R row_N row_destination row_c row_m row_status row_output; do
		[ -n "$row_status" ] || continue
		output=$(
			g_option_R_recursive=$(printf '%b' "$row_R")
			g_option_N_nonrecursive=$(printf '%b' "$row_N")
			g_destination=$(printf '%b' "$row_destination")
			g_option_c_services=$row_c
			g_option_m_migrate=$row_m
			[ -z "$row_c" ] || PATH=$no_svcadm_dir
			(
				trap - EXIT INT TERM HUP QUIT
				zxfer_prepare_zfs_mode_roots >/dev/null
				printf 'roots=%s|%s|%s\n' "$g_initial_source" "$g_destination" \
					"$g_initial_source_had_trailing_slash"
			) 2>&1
			printf 'status=%s\n' "$?"
		)
		assertContains "[$row_R|$row_N|$row_destination|$row_c|$row_m] exits $row_status" \
			"$output" "status=$row_status"
		assertContains "[$row_R|$row_N|$row_destination|$row_c|$row_m] prints the verdict" \
			"$output" "$row_output"
	done <<'EOF'
tank/src///||backup/target//||0|0|roots=tank/src|backup/target|1
tank/src||backup/target||0|0|roots=tank/src|backup/target|0
tank/app.v1||backup/target||0|0|roots=tank/app.v1|backup/target|0
/tank/src||backup/target||0|2|Source and destination must not begin with "/". Note the example.
tank/src||/backup/target||0|2|Source and destination must not begin with "/". Note the example.
|tank/src\ntank/other|backup/target||0|2|Source and destination must not contain control characters.
tank/src\n/tank/other||backup/target||0|2|Source and destination must not contain control characters.
tank/src||backup/target\nbackup/other||0|2|Source and destination must not contain control characters.
tank/src||backup/tar\tget||0|2|Source and destination must not contain control characters.
tank/src@snap1||backup/target||0|1|Snapshots are not allowed as a source.
tank/src||backup/target|svc:/network/nfs/server|0|1|When using -c, -m needs to be specified as well.
tank/src||backup/target|svc:/network/nfs/server|1|2|The -c service-management option requires Solaris/illumos SMF (svcadm).
EOF
}

# Row: -U|-P|-o|-e|-k|-R|source deltas|verdict (0 probes). Without -U there
# is no probe; a property mode, a non-recursive run or recursive send work
# can use its result, and a recursive run with nothing to send cannot.
test_unsupported_property_scan_runs_only_when_its_result_can_be_used() {
	while IFS='|' read -r row_U row_P row_o row_e row_k row_R row_sources row_verdict; do
		[ -n "$row_verdict" ] || continue
		g_option_U_skip_unsupported_properties=$row_U
		g_option_P_transfer_property=$row_P
		g_option_o_override_property=$row_o
		g_option_e_restore_property_mode=$row_e
		g_option_k_backup_property_mode=$row_k
		g_option_R_recursive=$row_R
		g_recursive_source_list=$row_sources
		verdict=0
		zxfer_unsupported_property_scan_is_required || verdict=$?
		assertEquals "-U probe verdict for [$row_U|$row_P|$row_o|$row_e|$row_k|$row_R|$row_sources]" \
			"$row_verdict" "$verdict"
	done <<'EOF'
0|1||1|1|tank/src|tank/src|1
1|1||0|0|tank/src||0
1|0|compression=lz4|0|0|tank/src||0
1|0||1|0|tank/src||0
1|0||0|1|tank/src||0
1|0||0|0|||0
1|0||0|0|tank/src|tank/src/child|0
1|0||0|0|tank/src||1
EOF
}

# Row: -R|-d|property pass|source deltas|source datasets|destination-only
# deltas|list. Parents come before descendants and siblings stay together,
# so -j can overlap independent receives; a dataset in several lists appears
# once; a destination-only delta can be a source parent; the source datasets
# join only a recursive property pass.
test_build_replication_iteration_list_orders_parents_first_and_lists_each_dataset_once() {
	while IFS='|' read -r row_R row_d row_property row_sources row_datasets row_extras row_list; do
		[ -n "$row_list" ] || continue
		g_option_R_recursive=$row_R
		g_option_d_delete_destination_snapshots=$row_d
		g_recursive_source_list=$(printf '%b' "$row_sources")
		g_recursive_source_dataset_list=$(printf '%b' "$row_datasets")
		g_recursive_destination_extra_dataset_list=$(printf '%b' "$row_extras")
		zxfer_build_replication_iteration_list "$row_property"
		assertEquals "iteration list for [$row_R|$row_d|$row_property|$row_sources|$row_datasets|$row_extras]" \
			"$(printf '%b' "$row_list")" "$g_zxfer_replication_iteration_list_result"
	done <<'EOF'
tank/src|1|1|tank/src|tank/src\ntank/src/child|tank/src/child\ntank/src/extra|1\ttank/src\n2\ttank/src/child\n3\ttank/src/extra
tank/src|0|0|tank/src/j/amp\ntank/src/j/amp/root\ntank/src/j/mail\ntank/src/j/mail/root|||1\ttank/src/j/amp\n2\ttank/src/j/mail\n3\ttank/src/j/amp/root\n4\ttank/src/j/mail/root
tank/src|1|0|tank/src/child||tank/src|1\ttank/src\n2\ttank/src/child
tank/src|0|0|tank/src/child|tank/src|tank/src/extra|1\ttank/src/child
|0|1|tank/src|tank/src\ntank/src/child||1\ttank/src
EOF
}

# ---------------------------------------------------------------------------
# The -s/-m snapshot name and the send guard.

test_newsnap_names_the_snapshot_lazily_once_per_run() {
	log="$TEST_TMPDIR/newsnap_lazy_name.log"
	: >"$log"
	# The suite stubs zxfer_newsnap: load the real one in a subshell only.
	first_name=$(
		g_option_R_recursive=""
		g_zxfer_new_snapshot_name="zxfer_inherited"
		# shellcheck source=src/zxfer_replication.sh
		. "$ZXFER_ROOT/src/zxfer_replication.sh"
		# The session reset drops an inherited name.
		zxfer_reset_replication_runtime_state
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "$*" >>"$log"
		}
		zxfer_newsnap "tank/src" >/dev/null
		l_name=$g_zxfer_new_snapshot_name
		zxfer_newsnap "tank/src" >/dev/null
		printf '%s\n' "$l_name"
	)

	case $first_name in
	"zxfer_$$_"[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) name_shape=ok ;;
	*) name_shape="unexpected <$first_name>" ;;
	esac
	assertEquals "The first snapshot should get the zxfer_PID_YYYYmmddHHMMSS name." ok "$name_shape"
	assertEquals "Later snapshots in the same run should reuse the name." \
		"snapshot tank/src@$first_name
snapshot tank/src@$first_name" "$(cat "$log")"
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

# ---------------------------------------------------------------------------
# Fail-closed branches no mock can reach: a step that fails without
# throwing, and awk, sort and record-split failures.

test_run_zfs_mode_stops_before_replication_when_backup_preflight_fails() {
	log="$TEST_TMPDIR/zxfer_run_zfs_mode_backup_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			RUN_LOG="$log"
			zxfer_prepare_zfs_mode_roots() { :; }
			zxfer_check_backup_storage_dir_if_needed() { return 37; }
			zxfer_initialize_replication_context() { printf 'unexpected-context\n' >>"$RUN_LOG"; }
			zxfer_throw_error() {
				printf 'throw=%s status=%s\n' "$1" "$2"
				exit "$2"
			}

			zxfer_run_zfs_mode
		) 2>&1
	)
	status=$?

	assertEquals "Backup preflight failures should retain their exact status at the replication boundary." \
		37 "$status"
	assertContains "Backup preflight failures should enter structured error reporting." \
		"$output" "throw=Failed to prepare backup metadata storage. status=37"
	assertEquals "Replication planning must not begin after backup storage preflight fails." \
		"" "$(cat "$log")"
}

test_run_zfs_mode_fails_closed_before_migration_when_discovery_returns_failure() {
	g_option_R_recursive="tank/src"
	g_option_m_migrate=1
	g_option_e_restore_property_mode=0
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			zxfer_get_zfs_list() {
				return 1
			}
			zxfer_check_backup_storage_dir_if_needed() { :; }
			zxfer_throw_error() {
				printf 'throw=%s status=%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_run_zfs_mode
		) 2>&1
	)
	status=$?

	assertEquals "A discovery that returns failure without throwing must stop the pass with its status." \
		1 "$status"
	assertContains "The discovery failure should be reported through the error helper." \
		"$output" "throw=Failed to retrieve the snapshot lists for [tank/src] and [backup/target]. status=1"
	assertEquals "No -m unmount, snapshot, or send may follow a failed discovery." \
		"" "$(cat "$STUB_ZFS_CMD_LOG")$(cat "$STUB_NEW_SNAP_LOG")"
}

test_copy_filesystems_rethrows_iteration_list_dedupe_failures() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src/extra"
	log="$TEST_TMPDIR/iteration_list_dedupe_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			ITERATION_LOG="$log"
			g_cmd_awk="awk"
			awk() {
				printf '%s\n' "awk failed" >&2
				return 9
			}
			zxfer_set_actual_dest() {
				printf 'set %s\n' "$1" >>"$ITERATION_LOG"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Iteration-list dedupe failures should abort the copy loop." \
		"1" "$status"
	assertContains "Iteration-list dedupe failures should preserve the underlying awk error." \
		"$output" "awk failed"
	assertContains "Iteration-list dedupe failures should be reported with iteration-list context." \
		"$output" "Failed to prepare replication dataset iteration list."
	assertEquals "Iteration-list dedupe failures should stop before dataset iteration begins." \
		"" "$(cat "$log")"
}

test_iteration_list_awk_failure_clears_previous_result() {
	g_recursive_source_list="tank/src"
	g_zxfer_replication_iteration_list_result="stale"
	saved_awk=${g_cmd_awk:-awk}
	g_cmd_awk=false

	zxfer_build_replication_iteration_list 0
	status=$?
	g_cmd_awk=$saved_awk

	assertEquals "A failed merge must propagate failure." 1 "$status"
	assertEquals "A failed merge must not publish stale work." "" "$g_zxfer_replication_iteration_list_result"
}

test_iteration_list_split_failure_clears_previous_result() {
	g_recursive_source_list="tank/src"
	g_zxfer_replication_iteration_list_result="stale"
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/split_failure_source.records"
	g_zxfer_destination_snapshot_record_cache_file="$TEST_TMPDIR/split_failure_destination.records"
	printf '%s\n' "tank/src@snap1	111" >"$g_zxfer_source_snapshot_record_cache_file"
	: >"$g_zxfer_destination_snapshot_record_cache_file"

	output=$(
		sort() { return 7; }
		build_status=0
		zxfer_build_replication_iteration_list 0 || build_status=$?
		printf 'status=%s published=<%s> slices=<%s>\n' "$build_status" \
			"$g_zxfer_replication_iteration_list_result" "$g_zxfer_snapshot_slice_records"
	)

	assertEquals "A failed record split must propagate its status and publish neither the list nor slices." \
		"status=7 published=<> slices=<>" "$output"
}

test_copy_filesystems_replays_each_seeded_dataset_once_in_sorted_order() {
	g_option_P_transfer_property=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	log="$TEST_TMPDIR/post_seed_dedupe.log"
	: >"$log"

	(
		QUEUE_LOG="$log"
		# A dataset queued twice must still be reconciled once.
		zxfer_build_replication_iteration_list() {
			g_zxfer_replication_iteration_list_result="tank/src/b
tank/src/my a
tank/src/b"
		}
		zxfer_set_actual_dest() {
			g_actual_dest="backup/$1"
		}
		zxfer_inspect_delete_snap() {
			:
		}
		zxfer_transfer_properties() {
			printf 'props %s\n' "$1" >>"$QUEUE_LOG"
		}
		zxfer_copy_snapshots() {
			g_dest_seed_requires_property_reconcile=1
		}
		zxfer_note_destination_dataset_exists() {
			:
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset\n' >>"$QUEUE_LOG"
		}

		zxfer_copy_filesystems
	)

	assertEquals "The post-seed pass should reconcile each seeded dataset once, sorted, after the queue drains." \
		"props tank/src/b
props tank/src/my a
props tank/src/b
reset
props tank/src/b
props tank/src/my a" "$(cat "$log")"
}

test_copy_filesystems_rethrows_post_seed_queue_dedupe_failures() {
	g_option_P_transfer_property=1
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/post_seed_queue_dedupe_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			QUEUE_LOG="$log"
			zxfer_build_replication_iteration_list() {
				g_zxfer_replication_iteration_list_result="tank/src"
			}
			zxfer_set_actual_dest() {
				g_actual_dest="backup/target/src"
				printf 'set %s\n' "$1" >>"$QUEUE_LOG"
			}
			zxfer_inspect_delete_snap() {
				printf 'inspect %s %s\n' "$1" "$2" >>"$QUEUE_LOG"
			}
			zxfer_transfer_properties() {
				printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$QUEUE_LOG"
			}
			zxfer_copy_snapshots() {
				g_dest_seed_requires_property_reconcile=1
				printf 'copy %s\n' "$g_actual_dest" >>"$QUEUE_LOG"
			}
			zxfer_note_destination_dataset_exists() {
				printf 'note %s\n' "$1" >>"$QUEUE_LOG"
			}
			zxfer_wait_for_zfs_send_jobs() {
				printf 'wait %s\n' "$1" >>"$QUEUE_LOG"
			}
			sort() {
				printf '%s\n' "sort failed" >&2
				return 9
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Post-seed queue dedupe failures should abort the copy loop." \
		"1" "$status"
	assertContains "Post-seed queue dedupe failures should surface the underlying dedupe error." \
		"$output" "sort failed"
	assertContains "Post-seed queue dedupe failures should be reported with queue context." \
		"$output" "Failed to prepare post-seed property reconcile source queue."
	assertNotContains "Post-seed queue dedupe failures should stop before the deferred reconcile pass resets destination caches." \
		"$(cat "$log")" "reset-destination-cache"
	assertNotContains "Post-seed queue dedupe failures should not run the second property pass." \
		"$(cat "$log")" "skip=1"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
