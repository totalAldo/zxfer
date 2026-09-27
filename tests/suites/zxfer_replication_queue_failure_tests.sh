#!/bin/sh
# Replication ready-queue, post-seed, loop, and failure-path behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Register a synthetic pending receive for deterministic scheduler tests.
# No child is started; each test owns its wait stub and removes the row.
zxfer_test_add_pending_send() {
	g_zxfer_send_jobs=${g_zxfer_send_jobs:+$g_zxfer_send_jobs
}"$1	$2	$4	unused	$3	pid"
	g_count_zfs_send_jobs=$((g_count_zfs_send_jobs + 1))
}

test_build_replication_iteration_list_merges_sources_in_current_shell() {
	g_option_R_recursive="tank/src"
	g_option_d_delete_destination_snapshots=1
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src/child
tank/src/extra"

	zxfer_build_replication_iteration_list 1

	assertEquals "Recursive property and delete planning should build the merged iteration list, one position per dataset, in current-shell scratch." \
		"1	tank/src
2	tank/src/child
3	tank/src/extra" "$g_zxfer_replication_iteration_list_result"
}

test_build_replication_iteration_list_orders_siblings_before_descendants() {
	g_option_R_recursive="tank/src"
	g_option_d_delete_destination_snapshots=0
	g_recursive_source_list="tank/src/jails/amp
tank/src/jails/amp/root
tank/src/jails/mail
tank/src/jails/mail/root
tank/src/jails/proxy
tank/src/jails/proxy/root"
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""

	zxfer_build_replication_iteration_list 0

	assertEquals "Recursive replication should schedule same-depth siblings before descendants so -j can keep unrelated receives running while parent/child ancestry remains serialized." \
		"1	tank/src/jails/amp
2	tank/src/jails/mail
3	tank/src/jails/proxy
4	tank/src/jails/amp/root
5	tank/src/jails/mail/root
6	tank/src/jails/proxy/root" "$g_zxfer_replication_iteration_list_result"
}

test_copy_filesystems_ready_queue_skips_blocked_descendant_for_independent_work() {
	g_option_R_recursive="tank/src"
	g_option_j_jobs=3
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_destination="backup"
	g_recursive_source_list="tank/src/app/root
tank/src/db/root"
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_send_jobs=""
	g_count_zfs_send_jobs=0
	log="$TEST_TMPDIR/ready_queue.log"
	rm -f "$log"

	(
		READY_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh\n' >>"$READY_LOG"
		}
		zxfer_process_source_dataset() {
			l_ready_source=$1
			zxfer_map_destination_dataset "$l_ready_source"
			l_ready_dest=$g_zxfer_destination_dataset_result
			printf 'process:%s dest=%s position=%s\n' "$l_ready_source" "$l_ready_dest" "$3" >>"$READY_LOG"
			if [ "$l_ready_source" = "tank/src/db/root" ]; then
				zxfer_test_add_pending_send "job-db-root" 202 "$l_ready_source@snap" "$l_ready_dest" ""
			fi
		}
		zxfer_wait_for_any_send_job() {
			printf 'wait_next:%s\n' "$1" >>"$READY_LOG"
			zxfer_unregister_send_job "job-app"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait_all:%s\n' "$1" >>"$READY_LOG"
			g_zxfer_send_jobs=""
			g_count_zfs_send_jobs=0
		}

		zxfer_test_add_pending_send "job-app" 101 "tank/src/app@snap" "backup/src/app" ""
		zxfer_copy_filesystems
	)

	assertEquals "The ready queue should skip a blocked descendant, start later independent work, then wait only when no pending source is ready; a deferred dataset keeps its list position." \
		"refresh
process:tank/src/db/root dest=backup/src/db/root position=2
wait_next:destination ancestry
process:tank/src/app/root dest=backup/src/app/root position=1
wait_all:final sync" "$(cat "$log")"
}

test_copy_filesystems_ready_queue_drains_deferred_parent_child_work_in_one_run() {
	g_option_R_recursive="tank"
	g_option_j_jobs=2
	g_option_n_dryrun=0
	g_option_v_verbose=1
	g_initial_source="tank"
	g_destination="backup"
	g_recursive_source_list="tank/iocage/jails/git
tank/iocage/jails/sftp
tank/iocage/jails/git/root
tank/iocage/jails/sftp/root"
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_send_jobs=""
	g_count_zfs_send_jobs=0
	log="$TEST_TMPDIR/ready_queue_parent_child.log"
	rm -f "$log"

	(
		READY_LOG="$log"
		JOB_SEQ=0
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh\n' >>"$READY_LOG"
		}
		zxfer_process_source_dataset() {
			l_ready_source=$1
			zxfer_map_destination_dataset "$l_ready_source"
			l_ready_dest=$g_zxfer_destination_dataset_result
			JOB_SEQ=$((JOB_SEQ + 1))
			printf 'process:%s dest=%s position=%s\n' "$l_ready_source" "$l_ready_dest" "$3" >>"$READY_LOG"
			zxfer_test_add_pending_send \
				"job-$JOB_SEQ" \
				"$((200 + JOB_SEQ))" \
				"$l_ready_source@snap" \
				"$l_ready_dest" \
				""
		}
		zxfer_wait_for_any_send_job() {
			printf 'wait_next:%s\n' "$1" >>"$READY_LOG"
			l_ready_first_job=${g_zxfer_send_jobs%%	*}
			zxfer_unregister_send_job "$l_ready_first_job"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait_all:%s\n' "$1" >>"$READY_LOG"
			g_zxfer_send_jobs=""
			g_count_zfs_send_jobs=0
		}

		zxfer_copy_filesystems >>"$READY_LOG"
	)

	assertEquals "Deferred descendants should be retried and processed before zxfer ends the same copy-filesystems pass." \
		"refresh
process:tank/iocage/jails/git dest=backup/tank/iocage/jails/git position=1
process:tank/iocage/jails/sftp dest=backup/tank/iocage/jails/sftp position=2
wait_next:job limit
process:tank/iocage/jails/git/root dest=backup/tank/iocage/jails/git/root position=3
wait_next:job limit
process:tank/iocage/jails/sftp/root dest=backup/tank/iocage/jails/sftp/root position=4
Replication ready queue summary: queued_datasets=4 processed_datasets=4 waits=2 active_jobs=2
wait_all:final sync" "$(cat "$log")"
}

# Stage record files for tank/src and tank/src/child under backup/src, as
# discovery does, so an iteration list comes with slices.
zxfer_test_stage_sliced_records() {
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_destination="backup"
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/sliced_source.records"
	g_zxfer_destination_snapshot_record_cache_file="$TEST_TMPDIR/sliced_destination.records"
	printf '%s\n' "tank/src/child@s2	22" "tank/src@s2	12" "tank/src/child@s1	21" \
		"tank/src@s1	11" >"$g_zxfer_source_snapshot_record_cache_file"
	printf '%s\n' "backup/src@s1	11" "backup/src/child@s1	21" \
		>"$g_zxfer_destination_snapshot_record_cache_file"
}

test_process_source_dataset_plans_each_dataset_from_its_own_slice() {
	zxfer_test_stage_sliced_records
	g_recursive_source_list="tank/src
tank/src/child"
	g_option_j_jobs=1
	output=$(
		zxfer_transfer_properties() { :; }
		zxfer_copy_snapshots() {
			printf '%s plan=%s|%s key=%s\n' "$1" "$g_last_common_snap" \
				"$g_src_snapshot_transfer_list" "$g_zxfer_snapshot_slice_key"
		}
		zxfer_build_replication_iteration_list 0 || exit
		zxfer_process_replication_ready_queue "$g_zxfer_replication_iteration_list_result" 0
	)

	assertEquals "Every queued dataset should select its own slice and plan from it." \
		"tank/src plan=tank/src@s1	11|tank/src@s2	12 key=1
tank/src/child plan=tank/src/child@s1	21|tank/src/child@s2	22 key=2" "$output"
}

test_perform_grandfather_protection_checks_plans_each_dataset_from_its_own_slice() {
	zxfer_test_stage_sliced_records
	g_recursive_source_list="tank/src/child
tank/src"
	g_option_g_grandfather_protection=30
	g_option_d_delete_destination_snapshots=0
	output=$(
		zxfer_enforce_destination_divergence_contract() {
			printf '%s common=%s key=%s\n' "$1" "$g_last_common_snap" \
				"$g_zxfer_snapshot_slice_key"
		}
		zxfer_perform_grandfather_protection_checks
	)

	assertContains "The -g pre-pass should plan the root from its slice." \
		"$output" "tank/src common=tank/src@s1	11 key=1"
	assertContains "The -g pre-pass should plan the child from its slice." \
		"$output" "tank/src/child common=tank/src/child@s1	21 key=2"
}

test_replication_ready_queue_preserves_pending_list_when_processing_reads_stdin() {
	g_option_j_jobs=4
	g_option_n_dryrun=0
	log="$TEST_TMPDIR/ready_queue_stdin.log"
	rm -f "$log"

	(
		READY_LOG="$log"
		zxfer_process_source_dataset() {
			printf 'process:%s\n' "$1" >>"$READY_LOG"
			if IFS= read -r l_stolen_source; then
				printf 'stole:%s\n' "$l_stolen_source" >>"$READY_LOG"
			fi
		}

		zxfer_process_replication_ready_queue "tank/src/app
tank/src/app/root
tank/src/db
tank/src/db/root" 0
	)

	assertEquals "Dataset processing must not inherit the ready queue reader, or ssh-like commands can consume deferred source names before the scheduler sees them." \
		"process:tank/src/app
process:tank/src/app/root
process:tank/src/db
process:tank/src/db/root" "$(cat "$log")"
}

test_copy_filesystems_merges_iteration_sources_and_deduplicates_post_seed_reconcile_in_current_shell() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_option_d_delete_destination_snapshots=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src/child
tank/src/extra"
	log="$TEST_TMPDIR/copy_filesystems_iteration_merge.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh-prefetch\n' >>"$REFRESH_LOG"
		}
		zxfer_set_actual_dest() {
			g_actual_dest="backup/$1"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$REFRESH_LOG"
			if [ "$g_actual_dest" = "backup/tank/src/child" ]; then
				g_dest_seed_requires_property_reconcile=1
			else
				g_dest_seed_requires_property_reconcile=0
			fi
		}
		zxfer_note_destination_dataset_exists() {
			printf 'note %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}

		zxfer_copy_filesystems
	)

	expected="refresh-prefetch
set tank/src
inspect 1 tank/src
props tank/src skip=0
copy backup/tank/src
set tank/src/child
inspect 1 tank/src/child
props tank/src/child skip=0
copy backup/tank/src/child
note backup/tank/src/child
set tank/src/extra
inspect 1 tank/src/extra
props tank/src/extra skip=0
copy backup/tank/src/extra
wait final sync
reset-destination-cache
set tank/src/child
props tank/src/child skip=0"
	assertEquals "Recursive property and delete planning should iterate over the union of source deltas, source datasets, and destination-only deltas, then reconcile each seeded dataset once." \
		"$expected" "$(cat "$log")"
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

test_copy_filesystems_refreshes_property_tree_prefetch_context_before_iteration() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src"
	log="$TEST_TMPDIR/copy_filesystems_prefetch_context.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh-prefetch\n' >>"$REFRESH_LOG"
		}
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			printf 'props %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$REFRESH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_copy_filesystems
	)

	assertEquals "zxfer_copy_filesystems should refresh the recursive property-tree prefetch context before iterating datasets so source and destination property slices stay aligned with the latest dataset lists." \
		"refresh-prefetch
set tank/src
inspect 0 tank/src
props tank/src
copy backup/target/src
wait final sync" "$(cat "$log")"
}

test_copy_filesystems_reconciles_seeded_empty_destinations_even_when_not_created_by_zxfer() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${stub_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${stub_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src created=0 skip=0 dest_present=0
copy backup/target/src created=0
wait final sync
set tank/src
props tank/src created=0 skip=0 dest_present=1"
	assertEquals "Seeded empty destinations should receive a final property reconciliation even when zxfer did not create the dataset." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_reconciles_seeded_destination_when_root_already_exists() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list="backup/target"
	log="$TEST_TMPDIR/seed_reconcile_existing_root.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${stub_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${stub_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src created=0 skip=0 dest_present=0
copy backup/target/src created=0
wait final sync
set tank/src
props tank/src created=0 skip=0 dest_present=1"
	assertEquals "When the destination root already exists, post-seed property reconciliation should still see the newly created child dataset in the in-memory destination list." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_tracks_post_seed_reconcile_sources_in_current_shell() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile_current_shell.log"
	rm -f "$log"

	zxfer_refresh_property_tree_prefetch_context() {
		:
	}
	zxfer_set_actual_dest() {
		g_actual_dest="backup/target/src"
	}
	zxfer_inspect_delete_snap() {
		:
	}
	zxfer_transfer_properties() {
		printf 'props skip=%s\n' "${2:-0}" >>"$log"
	}
	zxfer_copy_snapshots() {
		g_dest_seed_requires_property_reconcile=1
	}
	zxfer_wait_for_zfs_send_jobs() {
		printf 'wait\n' >>"$log"
	}
	zxfer_reset_destination_property_iteration_cache() {
		printf 'reset\n' >>"$log"
	}

	zxfer_copy_filesystems

	assertEquals "Seeded destinations should be queued for a second property pass in the current shell as well." \
		"props skip=0
wait
reset
props skip=0" "$(cat "$log")"
	assertContains "The real destination-cache helper should note the newly seeded dataset before the second pass." \
		"$g_recursive_dest_list" "backup/target/src"

	# shellcheck source=src/zxfer_property_state.sh
	. "$ZXFER_ROOT/src/zxfer_property_state.sh"
	# shellcheck source=src/zxfer_property_transfer.sh
	. "$ZXFER_ROOT/src/zxfer_property_transfer.sh"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
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

test_copy_filesystems_keeps_verbose_output_visible_while_tracking_post_seed_reconcile_sources() {
	g_option_P_transfer_property=1
	g_option_v_verbose=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile_verbose.log"
	stdout_file="$TEST_TMPDIR/seed_reconcile_verbose.stdout"
	stderr_file="$TEST_TMPDIR/seed_reconcile_verbose.stderr"
	rm -f "$log" "$stdout_file" "$stderr_file"

	(
		REFRESH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			zxfer_echov "verbose $1 skip=${2:-0}"
			printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		zxfer_note_destination_dataset_exists() {
			printf 'note %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}

		zxfer_copy_filesystems
	) >"$stdout_file" 2>"$stderr_file"

	assertEquals "Verbose property-transfer output should remain visible while seeded datasets are tracked for the second property pass." \
		"verbose tank/src skip=0
verbose tank/src skip=0" "$(cat "$stdout_file")"
	assertEquals "Tracking seeded datasets for deferred property reconciliation should append only dataset names, not captured verbose log lines." \
		"set tank/src
inspect 0 tank/src
props tank/src skip=0
copy backup/target/src
note backup/target/src
wait final sync
reset-destination-cache
set tank/src
props tank/src skip=0" "$(cat "$log")"
	assertNotContains "Deferred property reconciliation should never treat verbose log lines as dataset identifiers." \
		"$(cat "$log")" "set verbose"
	assertEquals "This regression path should not emit stderr output." "" "$(cat "$stderr_file")"
}

test_copy_filesystems_resets_destination_property_cache_before_post_seed_reconcile() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile_cache_reset.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${stub_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${stub_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}
		zxfer_copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src created=0 skip=0 dest_present=0
copy backup/target/src created=0
wait final sync
reset-destination-cache
set tank/src
props tank/src created=0 skip=0 dest_present=1"
	assertEquals "Deferred post-seed property reconciliation should clear destination-side property caches after background receives complete and before re-reading destination properties." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_keeps_destination_property_cache_across_datasets_when_background_receives_are_active() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	log="$TEST_TMPDIR/background_property_cache_reset.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			printf 'props %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$REFRESH_LOG"
			if [ "$g_actual_dest" = "tank/src" ]; then
				g_zxfer_send_jobs="job	12345	tank/src	unused	snap	pid"
			fi
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_copy_filesystems
	)

	# In-flight background receives cannot mutate the next dataset's destination
	# state (the ready-queue ancestry gate defers conflicting datasets, and
	# completed jobs invalidate their own subtree), so processing the next
	# dataset must reuse the shared destination property cache instead of
	# resetting it tree-wide.
	expected="set tank/src
inspect 0 tank/src
props tank/src
copy tank/src
set tank/src/child
inspect 0 tank/src/child
props tank/src/child
copy tank/src/child
wait final sync"
	assertEquals "When background receives are still active, the next dataset should reuse destination-side property caches; scoped invalidation happens at job completion." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_allows_post_unmount_migration_replication() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"
	log="$TEST_TMPDIR/migrate_context.log"
	rm -f "$log"

	(
		MIGRATE_LOG="$log"
		zxfer_run_source_zfs_cmd() {
			if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
				printf 'no\n'
			fi
		}
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$MIGRATE_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$MIGRATE_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			:
		}
		zxfer_copy_filesystems
	)

	assertEquals "Migration copy loop should proceed after zxfer_prepare_migration_services unmounts the source." \
		"inspect 0 tank/src
copy backup/target/src" "$(cat "$log")"
}

test_run_zfs_mode_loop_exits_after_single_iteration_when_no_changes() {
	g_option_Y_yield_iterations=4
	log="$TEST_TMPDIR/run_loop_single.log"
	: >"$log"

	(
		RUN_LOOP_LOG="$log"
		zxfer_run_zfs_mode() {
			printf 'run\n' >>"$RUN_LOOP_LOG"
			g_is_performed_send_destroy=0
		}
		zxfer_run_zfs_mode_loop
	)

	line_count=$(awk 'END {print NR}' "$log")
	assertEquals "Loop should stop after one iteration when no sends/destroys occur." "1" "$line_count"
}

test_run_zfs_mode_loop_repeats_until_changes_stop() {
	g_option_Y_yield_iterations=4
	log="$TEST_TMPDIR/run_loop_repeat.log"
	: >"$log"

	(
		RUN_LOOP_LOG="$log"
		iteration=0
		zxfer_run_zfs_mode() {
			iteration=$((iteration + 1))
			printf 'run %s\n' "$iteration" >>"$RUN_LOOP_LOG"
			if [ "$iteration" -ge 2 ]; then
				g_is_performed_send_destroy=0
			else
				g_is_performed_send_destroy=1
			fi
		}
		zxfer_run_zfs_mode_loop
	)

	line_count=$(awk 'END {print NR}' "$log")
	assertEquals "Loop should run until the helper clears the send/destroy flag." "2" "$line_count"
}

test_run_zfs_mode_loop_resets_property_cache_each_iteration() {
	g_option_Y_yield_iterations=4
	log="$TEST_TMPDIR/run_loop_cache_reset.log"
	: >"$log"

	(
		RUN_LOOP_LOG="$log"
		iteration=0
		zxfer_reset_property_iteration_caches() {
			printf 'reset\n' >>"$RUN_LOOP_LOG"
		}
		zxfer_run_zfs_mode() {
			iteration=$((iteration + 1))
			printf 'run %s\n' "$iteration" >>"$RUN_LOOP_LOG"
			if [ "$iteration" -ge 2 ]; then
				g_is_performed_send_destroy=0
			else
				g_is_performed_send_destroy=1
			fi
		}
		zxfer_run_zfs_mode_loop
	)

	assertEquals "Each run-loop iteration should clear the per-iteration property cache before executing zfs mode." \
		"reset
run 1
reset
run 2" "$(cat "$log")"
}

test_run_zfs_mode_loop_collapses_repeated_iteration_backup_rows_at_write_boundary() {
	g_option_Y_yield_iterations=4
	g_option_k_backup_property_mode=1
	g_backup_file_contents=""

	output=$(
		(
			iteration=0
			zxfer_run_zfs_mode() {
				iteration=$((iteration + 1))
				if [ "$iteration" -eq 1 ]; then
					zxfer_append_backup_metadata_record "tank/src" "compression=lz4=local"
					g_is_performed_send_destroy=1
				else
					zxfer_append_backup_metadata_record "tank/src" "readonly=on=local"
					g_is_performed_send_destroy=0
				fi
			}
			zxfer_run_zfs_mode_loop
			printf 'backup=%s\n' "$(zxfer_validate_backup_metadata_record_list "$g_backup_file_contents")"
		)
	)

	assertContains "Repeated -Y iterations should publish one v2 backup-metadata row per relative dataset path with the newest row winning." \
		"$output" "backup=$(zxfer_test_backup_metadata_row "." "readonly=on=local")"
	assertNotContains "Write-boundary validation should drop rows shadowed by later -Y iterations." \
		"$output" "compression=lz4=local"
}

test_run_zfs_mode_loop_logs_hint_when_hard_iteration_limit_is_reached() {
	g_option_Y_yield_iterations=2
	ZXFER_MAX_YIELD_ITERATIONS=2
	g_option_V_very_verbose=1

	output=$(
		(
			zxfer_run_zfs_mode() {
				g_is_performed_send_destroy=1
			}
			zxfer_run_zfs_mode_loop
		) 2>&1
	)

	assertContains "Reaching the hard yield-iteration limit should emit the replication tuning hint." \
		"$output" "consider using compression, increasing bandwidth, increasing I/O or reducing snapshot frequency."
}

test_seed_destination_for_snapshot_transfer_reports_destination_probe_failures() {
	g_actual_dest="backup/target/src"
	g_last_common_snap=""
	g_dest_has_snapshots=0

	set +e
	output=$(
		(
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/target/src] exists: ssh timeout"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_seed_destination_for_snapshot_transfer "tank/src@base" "tank/src@base"
		)
	)
	status=$?

	assertEquals "Destination seeding should fail closed when destination existence checks fail." \
		1 "$status"
	assertContains "Destination seeding should surface the destination existence probe failure." \
		"$output" "Failed to determine whether destination dataset [backup/target/src] exists: ssh timeout"
}

test_seed_destination_for_snapshot_transfer_refuses_snapshotted_destination_without_anchor() {
	g_actual_dest="backup/target/src"
	g_last_common_snap=""
	g_dest_has_snapshots=1
	log="$TEST_TMPDIR/seed_refuses_without_anchor.log"
	: >"$log"

	set +e
	output=$(
		(
			SEED_LOG="$log"
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$SEED_LOG"
			}
			zxfer_zfs_send_receive() {
				printf 'send %s\n' "$2" >>"$SEED_LOG"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_seed_destination_for_snapshot_transfer "tank/src@base" "tank/src@base"
		)
	)
	status=$?

	assertEquals "An existing destination with snapshots but no anchor must not be seeded." \
		1 "$status"
	assertContains "The refusal should explain that no snapshot shares a guid with the source." \
		"$output" "Destination dataset [backup/target/src] has snapshots but none share a common guid with the source."
	assertEquals "The seed trusts the snapshot presence the plan published: no listing, no send." \
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

test_iteration_list_orders_destination_delta_parent_before_source_child() {
	g_option_d_delete_destination_snapshots=1
	g_recursive_source_list="tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src"

	zxfer_build_replication_iteration_list 0

	assertEquals "A destination-only snapshot may belong to a source ancestor; visit that parent first." \
		"1	tank/src
2	tank/src/child" "$g_zxfer_replication_iteration_list_result"
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
