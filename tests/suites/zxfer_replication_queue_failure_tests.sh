#!/bin/sh
# Replication ready-queue, post-seed, loop, and failure-path behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_build_replication_iteration_list_merges_sources_in_current_shell() {
	g_option_R_recursive="tank/src"
	g_option_d_delete_destination_snapshots=1
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src/child
tank/src/extra"

	zxfer_build_replication_iteration_list 1

	assertEquals "Recursive property and delete planning should build the merged iteration list in current-shell scratch." \
		"tank/src
tank/src/child
tank/src/extra" "$g_zxfer_replication_iteration_list_result"
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
		"tank/src/jails/amp
tank/src/jails/mail
tank/src/jails/proxy
tank/src/jails/amp/root
tank/src/jails/mail/root
tank/src/jails/proxy/root" "$g_zxfer_replication_iteration_list_result"
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
	g_zfs_send_job_pids=""
	g_zfs_send_job_supervisor_records=""
	g_count_zfs_send_jobs=0
	g_zfs_send_job_queue_open=1
	log="$TEST_TMPDIR/ready_queue.log"
	rm -f "$log"

	(
		READY_LOG="$log"
		zxfer_prepare_ssh_control_sockets_for_active_hosts() {
			:
		}
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh\n' >>"$READY_LOG"
		}
		zxfer_process_source_dataset() {
			l_ready_source=$1
			l_ready_dest=$(zxfer_compute_actual_dest_for_source "$l_ready_source")
			printf 'process:%s dest=%s\n' "$l_ready_source" "$l_ready_dest" >>"$READY_LOG"
			if [ "$l_ready_source" = "tank/src/db/root" ]; then
				zxfer_register_supervised_send_job "job-db-root" 202 "$l_ready_source@snap" "$l_ready_dest" ""
			fi
		}
		zxfer_wait_for_next_zfs_send_job_completion() {
			printf 'wait_next:%s\n' "$1" >>"$READY_LOG"
			zxfer_unregister_supervised_send_job "job-app"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait_all:%s\n' "$1" >>"$READY_LOG"
			g_zfs_send_job_pids=""
			g_zfs_send_job_supervisor_records=""
			g_count_zfs_send_jobs=0
		}

		zxfer_register_supervised_send_job "job-app" 101 "tank/src/app@snap" "backup/src/app" ""
		zxfer_copy_filesystems
	)

	assertEquals "The ready queue should skip a blocked descendant, start later independent work, then wait only when no pending source is ready." \
		"refresh
process:tank/src/db/root dest=backup/src/db/root
wait_next:destination ancestry
process:tank/src/app/root dest=backup/src/app/root
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
	g_zfs_send_job_pids=""
	g_zfs_send_job_supervisor_records=""
	g_count_zfs_send_jobs=0
	g_zfs_send_job_queue_open=1
	log="$TEST_TMPDIR/ready_queue_parent_child.log"
	rm -f "$log"

	(
		READY_LOG="$log"
		JOB_SEQ=0
		zxfer_prepare_ssh_control_sockets_for_active_hosts() {
			:
		}
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh\n' >>"$READY_LOG"
		}
		zxfer_process_source_dataset() {
			l_ready_source=$1
			l_ready_dest=$(zxfer_compute_actual_dest_for_source "$l_ready_source")
			JOB_SEQ=$((JOB_SEQ + 1))
			printf 'process:%s dest=%s\n' "$l_ready_source" "$l_ready_dest" >>"$READY_LOG"
			zxfer_register_supervised_send_job \
				"job-$JOB_SEQ" \
				"$((200 + JOB_SEQ))" \
				"$l_ready_source@snap" \
				"$l_ready_dest" \
				""
		}
		zxfer_wait_for_next_zfs_send_job_completion() {
			printf 'wait_next:%s\n' "$1" >>"$READY_LOG"
			l_ready_first_job=""
			while IFS= read -r l_ready_job_id || [ -n "$l_ready_job_id" ]; do
				[ -n "$l_ready_job_id" ] || continue
				l_ready_first_job=$l_ready_job_id
				break
			done <<-EOF
				$(zxfer_collect_supervised_send_job_ids)
			EOF
			zxfer_unregister_supervised_send_job "$l_ready_first_job"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait_all:%s\n' "$1" >>"$READY_LOG"
			g_zfs_send_job_pids=""
			g_zfs_send_job_supervisor_records=""
			g_count_zfs_send_jobs=0
		}

		zxfer_copy_filesystems >>"$READY_LOG"
	)

	assertEquals "Deferred descendants should be retried and processed before zxfer ends the same copy-filesystems pass." \
		"refresh
process:tank/iocage/jails/git dest=backup/tank/iocage/jails/git
process:tank/iocage/jails/sftp dest=backup/tank/iocage/jails/sftp
wait_next:job limit
process:tank/iocage/jails/git/root dest=backup/tank/iocage/jails/git/root
wait_next:job limit
process:tank/iocage/jails/sftp/root dest=backup/tank/iocage/jails/sftp/root
Replication ready queue summary: queued_datasets=4 processed_datasets=4 waits=2 active_jobs=2
wait_all:final sync" "$(cat "$log")"
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
tank/src/db/root" 0 "$TEST_TMPDIR/post_seed_sources"
	)

	assertEquals "Dataset processing must not inherit the ready queue reader, or ssh-like commands can consume deferred source names before the scheduler sees them." \
		"process:tank/src/app
process:tank/src/app/root
process:tank/src/db
process:tank/src/db/root" "$(cat "$log")"
}

test_replication_ready_queue_splits_pending_sources_when_ifs_was_narrowed() {
	g_option_j_jobs=4
	g_option_n_dryrun=0
	log="$TEST_TMPDIR/ready_queue_ifs.log"
	rm -f "$log"

	(
		READY_LOG="$log"
		IFS='	'
		zxfer_process_source_dataset() {
			printf 'process:%s\n' "$1" >>"$READY_LOG"
		}

		zxfer_process_replication_ready_queue "tank/src
tank/src/child1
tank/src/child2" 0 "$TEST_TMPDIR/post_seed_sources"
	)

	assertEquals "The ready queue should split its newline-delimited work list even if an illumos /bin/sh read helper narrowed IFS earlier." \
		"process:tank/src
process:tank/src/child1
process:tank/src/child2" "$(cat "$log")"
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
props tank/src/child skip=1"
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
			sort() {
				printf '%s\n' "sort failed" >&2
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
	assertContains "Iteration-list dedupe failures should preserve the underlying sort error." \
		"$output" "sort failed"
	assertContains "Iteration-list dedupe failures should be reported with iteration-list context." \
		"$output" "Failed to prepare replication dataset iteration list."
	assertEquals "Iteration-list dedupe failures should stop before dataset iteration begins." \
		"" "$(cat "$log")"
}

test_copy_filesystems_rethrows_iteration_list_readback_failures() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src/extra"
	log="$TEST_TMPDIR/iteration_list_readback_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			ITERATION_LOG="$log"
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-readback-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			cat() {
				if [ "$1" = "$TEST_TMPDIR/iteration-readback-3.tmp" ]; then
					printf '%s\n' "read failed" >&2
					return 9
				fi
				command cat "$@"
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

	assertEquals "Iteration-list staged readback failures should abort the copy loop." \
		"1" "$status"
	assertContains "Iteration-list staged readback failures should preserve the underlying readback error." \
		"$output" "read failed"
	assertContains "Iteration-list staged readback failures should be reported with iteration-list context." \
		"$output" "Failed to prepare replication dataset iteration list."
	assertEquals "Iteration-list staged readback failures should stop before dataset iteration begins." \
		"" "$(cat "$log")"
}

test_copy_filesystems_reports_post_seed_property_stage_initialization_failures() {
	g_option_P_transfer_property=1
	g_option_R_recursive=""
	g_initial_source="tank/src"
	log="$TEST_TMPDIR/copy_filesystems_post_seed_stage_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			zxfer_build_replication_iteration_list() {
				g_zxfer_replication_iteration_list_result=""
				return 0
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/post_seed_stage_failure.txt"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 1
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'cleanup %s\n' "$1" >>"$log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Copy-filesystems setup should abort when the post-seed property staging file cannot be initialized." \
		1 "$status"
	assertContains "Post-seed property staging initialization failures should be reported as temp-file creation errors." \
		"$output" "Error creating temporary file."
	assertEquals "Post-seed property staging initialization failures should clean up the staged path before aborting." \
		"cleanup $TEST_TMPDIR/post_seed_stage_failure.txt" "$(cat "$log")"
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
props tank/src created=0 skip=1 dest_present=1"
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
props tank/src created=0 skip=1 dest_present=1"
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
props skip=1" "$(cat "$log")"
	assertContains "The real destination-cache helper should note the newly seeded dataset before the second pass." \
		"$g_recursive_dest_list" "backup/target/src"

	# shellcheck source=src/zxfer_property_state.sh
	. "$ZXFER_ROOT/src/zxfer_property_state.sh"
	# shellcheck source=src/zxfer_property_reconcile.sh
	. "$ZXFER_ROOT/src/zxfer_property_reconcile.sh"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
}

test_copy_filesystems_rethrows_post_seed_queue_tempfile_failures() {
	g_option_P_transfer_property=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	log="$TEST_TMPDIR/post_seed_queue_tempfile_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			QUEUE_LOG="$log"
			zxfer_build_replication_iteration_list() {
				g_zxfer_replication_iteration_list_result="tank/src"
			}
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Post-seed queue temp-file allocation failures should abort the copy loop." \
		"1" "$status"
	assertContains "Post-seed queue temp-file allocation failures should preserve the temp-file error." \
		"$output" "Error creating temporary file."
	assertEquals "Post-seed queue temp-file allocation failures should stop before any dataset work begins." \
		"" "$(cat "$log")"
}

test_copy_filesystems_rethrows_post_seed_queue_append_failures() {
	g_option_P_transfer_property=1
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/post_seed_queue_append_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			QUEUE_LOG="$log"
			zxfer_set_actual_dest() {
				g_actual_dest="backup/target/src"
				printf 'set %s\n' "$1" >>"$QUEUE_LOG"
			}
			zxfer_inspect_delete_snap() {
				printf 'inspect %s %s\n' "$1" "$2" >>"$QUEUE_LOG"
			}
			zxfer_transfer_properties() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$QUEUE_LOG"
			}
			zxfer_copy_snapshots() {
				g_dest_seed_requires_property_reconcile=1
				printf 'copy %s\n' "$g_actual_dest" >>"$QUEUE_LOG"
			}
			zxfer_note_destination_dataset_exists() {
				printf 'note %s\n' "$1" >>"$QUEUE_LOG"
			}
			zxfer_append_post_seed_property_source() {
				return 1
			}
			zxfer_wait_for_zfs_send_jobs() {
				printf 'wait %s\n' "$1" >>"$QUEUE_LOG"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Post-seed queue append failures should abort the copy loop." \
		"1" "$status"
	assertContains "Post-seed queue append failures should identify the dataset that could not be queued." \
		"$output" "Failed to queue post-seed property reconcile source [tank/src]."
	assertNotContains "Post-seed queue append failures should stop before final sync or the deferred reconcile pass." \
		"$(cat "$log")" "wait final sync"
	assertNotContains "Post-seed queue append failures should not run the second property pass." \
		"$(cat "$log")" "skip=1"
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

test_copy_filesystems_rethrows_post_seed_queue_readback_failures() {
	g_option_P_transfer_property=1
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/post_seed_queue_readback_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			QUEUE_LOG="$log"
			call_count=0
			zxfer_build_replication_iteration_list() {
				g_zxfer_replication_iteration_list_result="tank/src"
			}
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/post-seed-readback-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			cat() {
				if [ "$1" = "$TEST_TMPDIR/post-seed-readback-3.tmp" ]; then
					printf '%s\n' "read failed" >&2
					return 9
				fi
				command cat "$@"
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Post-seed queue staged readback failures should abort the copy loop." \
		"1" "$status"
	assertContains "Post-seed queue staged readback failures should preserve the underlying readback error." \
		"$output" "read failed"
	assertContains "Post-seed queue staged readback failures should be reported with queue context." \
		"$output" "Failed to prepare post-seed property reconcile source queue."
	assertNotContains "Post-seed queue staged readback failures should stop before the deferred reconcile pass resets destination caches." \
		"$(cat "$log")" "reset-destination-cache"
	assertNotContains "Post-seed queue staged readback failures should not run the second property pass." \
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
verbose tank/src skip=1" "$(cat "$stdout_file")"
	assertEquals "Tracking seeded datasets for deferred property reconciliation should append only dataset names, not captured verbose log lines." \
		"set tank/src
inspect 0 tank/src
props tank/src skip=0
copy backup/target/src
note backup/target/src
wait final sync
reset-destination-cache
set tank/src
props tank/src skip=1" "$(cat "$log")"
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
props tank/src created=0 skip=1 dest_present=1"
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
	g_zfs_send_job_pids=""
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
				g_zfs_send_job_pids="12345"
			fi
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
			g_zfs_send_job_pids=""
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

test_prepare_migration_services_rejects_unmounted_sources() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					printf 'no\n'
					return 0
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should abort when a source dataset is not mounted." 2 "$status"
	assertContains "Unmounted migration sources should use the documented usage error." \
		"$output" "The source filesystem is not mounted, cannot use -m."
}

test_prepare_migration_services_reports_mounted_probe_failures() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					return 1
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should abort when mounted-state lookup fails." 1 "$status"
	assertContains "Mounted-state lookup failures should not be misreported as an unmounted source." \
		"$output" "Couldn't determine whether source tank/src is mounted."
}

test_prepare_migration_services_live_uses_mountpoint_free_effective_readonly_list() {
	g_option_m_migrate=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	ZXFER_BASE_READONLY_PROPERTIES="type,mountpoint,creation"

	zxfer_prepare_migration_services

	assertEquals "Live migration should drop mountpoint from the effective readonly-property list." \
		"type,creation" "$(zxfer_get_effective_readonly_properties)"
	assertEquals "Live migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
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

test_prepare_migration_services_relaunches_when_unmount_fails() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					printf 'yes\n'
					return 0
				fi
				if [ "$1" = "unmount" ]; then
					return 1
				fi
				return 0
			}
			zxfer_relaunch() {
				printf 'zxfer_relaunch\n'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		)
	)
	status=$?

	assertEquals "Failed unmounts during migration should abort." 1 "$status"
	assertContains "Failed unmounts should zxfer_relaunch services before aborting." "$output" "zxfer_relaunch"
	assertContains "Failed unmounts should identify the affected source." \
		"$output" "Couldn't unmount source tank/src."
}

test_run_zfs_mode_loop_exits_after_single_iteration_when_no_changes() {
	g_option_Y_yield_iterations=4
	g_test_max_yield_iterations=8
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
	g_test_max_yield_iterations=8
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
	g_test_max_yield_iterations=8
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
	g_test_max_yield_iterations=8
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
	g_test_max_yield_iterations=2
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
			zxfer_exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/target/src] exists: ssh timeout"
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

test_seed_destination_for_snapshot_transfer_reports_live_snapshot_recheck_failures() {
	g_actual_dest="backup/target/src"
	g_last_common_snap=""
	g_dest_has_snapshots=1

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_get_live_destination_snapshots() {
				printf '%s\n' "ssh timeout"
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

	assertEquals "Destination seeding should fail closed when the live destination snapshot recheck fails." \
		1 "$status"
	assertContains "Destination seeding should preserve the live destination snapshot recheck diagnostic." \
		"$output" "Failed to retrieve live destination snapshots for [backup/target/src]: ssh timeout"
}

test_build_replication_iteration_list_reports_second_tempfile_failures() {
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-input.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_build_replication_iteration_list 0
		)
	)
	status=$?

	assertEquals "Replication iteration-list building should fail closed when the second tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Replication iteration-list building should not emit output for second-tempfile failures." \
		"" "$output"
}

test_build_replication_iteration_list_reports_filter_command_failures() {
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-filter-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			grep() {
				return 9
			}
			zxfer_build_replication_iteration_list 0
		)
	)
	status=$?

	assertEquals "Replication iteration-list building should fail closed when the blank-line filter command fails." \
		9 "$status"
	assertEquals "Replication iteration-list building should not emit output for filter-command failures." \
		"" "$output"
}

test_build_replication_iteration_list_reports_stage_write_and_append_failures() {
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_recursive_destination_extra_dataset_list="tank/src/extra"

	set +e
	write_output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-write-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 1
			}
			zxfer_build_replication_iteration_list 0
		)
	)
	write_status=$?
	recursive_output=$(
		(
			temp_call_count=0
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-recursive-$temp_call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				: >"$1"
				return 0
			}
			printf() {
				if [ "$1" = '%s\n' ] && [ "$2" = "$g_recursive_source_dataset_list" ]; then
					return 1
				fi
				command printf "$@"
			}
			g_option_R_recursive="tank/src"
			g_option_d_delete_destination_snapshots=0
			zxfer_build_replication_iteration_list 1
		)
	)
	recursive_status=$?
	extra_output=$(
		(
			temp_call_count=0
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-extra-$temp_call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				: >"$1"
				return 0
			}
			printf() {
				if [ "$1" = '%s\n' ] && [ "$2" = "$g_recursive_destination_extra_dataset_list" ]; then
					return 1
				fi
				command printf "$@"
			}
			g_option_R_recursive=""
			g_option_d_delete_destination_snapshots=1
			zxfer_build_replication_iteration_list 0
		)
	)
	extra_status=$?
	set -e

	assertEquals "Replication iteration-list building should fail closed when the staged input file cannot be initialized." \
		1 "$write_status"
	assertEquals "Replication iteration-list building should not emit output for staged-input write failures." \
		"" "$write_output"
	assertEquals "Replication iteration-list building should fail closed when appending recursive dataset rows fails." \
		1 "$recursive_status"
	assertEquals "Replication iteration-list building should not emit output for recursive append failures." \
		"" "$recursive_output"
	assertEquals "Replication iteration-list building should fail closed when appending destination-only dataset rows fails." \
		1 "$extra_status"
	assertEquals "Replication iteration-list building should not emit output for destination-extra append failures." \
		"" "$extra_output"
}

test_build_replication_iteration_list_reports_source_append_failures_in_current_shell() {
	g_recursive_source_list="tank/src"
	g_option_R_recursive=""
	g_option_d_delete_destination_snapshots=0
	cleanup_log="$TEST_TMPDIR/iteration_current_cleanup.log"
	l_temp_call_count=0

	zxfer_get_temp_file() {
		l_temp_call_count=$((l_temp_call_count + 1))
		g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-current-$l_temp_call_count.tmp"
		return 0
	}
	zxfer_write_runtime_artifact_file() {
		mkdir -p "$1"
		return 0
	}
	zxfer_cleanup_runtime_artifact_path_list() {
		printf '%s\n' "$1" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_build_replication_iteration_list 0 >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell iteration-list building should fail closed when appending the source dataset list fails." \
		1 "$status"
	assertEquals "Current-shell iteration-list building should clean up every staged tempfile after a source append failure." \
		"$TEST_TMPDIR/iteration-current-1.tmp
$TEST_TMPDIR/iteration-current-2.tmp
$TEST_TMPDIR/iteration-current-3.tmp" \
		"$cleanup_paths"
}

test_zxfer_sort_replication_iteration_file_reports_awk_failures_in_current_shell() {
	input_file="$TEST_TMPDIR/iteration-sort-awk.input"
	output_file="$TEST_TMPDIR/iteration-sort-awk.output"
	scratch_file="$TEST_TMPDIR/iteration-sort-awk.scratch"
	printf '%s\n' "tank/src/child" "tank/src" >"$input_file"
	g_cmd_awk="awk"

	awk() {
		return 41
	}

	set +e
	zxfer_sort_replication_iteration_file "$input_file" "$output_file" "$scratch_file" >/dev/null 2>&1
	first_status=$?
	set -e
	unset -f awk

	awk_call_count=0
	awk() {
		awk_call_count=$((awk_call_count + 1))
		if [ "$awk_call_count" -eq 2 ]; then
			return 42
		fi
		command awk "$@"
	}

	set +e
	zxfer_sort_replication_iteration_file "$input_file" "$output_file" "$scratch_file" >/dev/null 2>&1
	second_status=$?
	set -e
	unset -f awk

	assertEquals "Replication iteration sorting should preserve depth-prefix awk failures." \
		41 "$first_status"
	assertEquals "Replication iteration sorting should preserve prefix-strip awk failures." \
		42 "$second_status"
}

test_build_replication_iteration_list_reports_sorted_readback_failures() {
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-readback-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			cat() {
				if [ "$1" = "$TEST_TMPDIR/iteration-readback-3.tmp" ]; then
					printf '%s\n' "read failed" >&2
					return 9
				fi
				command cat "$@"
			}
			zxfer_build_replication_iteration_list 0
		) 2>&1
	)
	status=$?

	assertEquals "Replication iteration-list building should fail closed when the sorted staging file cannot be read back." \
		9 "$status"
	assertContains "Replication iteration-list building should preserve the sorted staging readback diagnostic." \
		"$output" "read failed"
}

test_collect_post_seed_property_sources_sorts_unique_sources_in_current_shell() {
	post_seed_file="$TEST_TMPDIR/post_seed_sources.txt"
	cat >"$post_seed_file" <<'EOF'

tank/src/child
tank/src
tank/src/child
EOF

	zxfer_collect_post_seed_property_sources "$post_seed_file"

	assertEquals "Post-seed property reconcile source collection should sort and deduplicate non-empty queued datasets." \
		"tank/src
tank/src/child" "$g_zxfer_post_seed_property_sources_result"
}

test_append_post_seed_property_source_appends_sources_when_tracking_file_exists() {
	post_seed_file="$TEST_TMPDIR/post_seed_append.txt"
	: >"$post_seed_file"

	zxfer_append_post_seed_property_source "$post_seed_file" "tank/src"

	assertEquals "Appending a post-seed property source should record the dataset in the staging file." \
		"tank/src" "$(cat "$post_seed_file")"
}

test_collect_post_seed_property_sources_reports_second_tempfile_failures() {
	post_seed_file="$TEST_TMPDIR/post_seed_tempfile_failure.txt"
	printf '%s\n' "tank/src" >"$post_seed_file"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/post-seed-filtered.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_collect_post_seed_property_sources "$post_seed_file"
		)
	)
	status=$?

	assertEquals "Post-seed property reconcile source collection should fail closed when the second tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Post-seed property reconcile source collection should not emit output for second-tempfile failures." \
		"" "$output"
}

test_collect_post_seed_property_sources_reports_first_tempfile_failures() {
	post_seed_file="$TEST_TMPDIR/post_seed_first_tempfile_failure.txt"
	printf '%s\n' "tank/src" >"$post_seed_file"

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_collect_post_seed_property_sources "$post_seed_file"
		)
	)
	status=$?
	set -e

	assertEquals "Post-seed property reconcile source collection should fail closed when the first tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Post-seed property reconcile source collection should not emit output for first-tempfile failures." \
		"" "$output"
}

test_collect_post_seed_property_sources_reports_filter_command_failures() {
	post_seed_file="$TEST_TMPDIR/post_seed_filter_failure.txt"
	printf '%s\n' "tank/src" >"$post_seed_file"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/post-seed-filter-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			grep() {
				return 9
			}
			zxfer_collect_post_seed_property_sources "$post_seed_file"
		)
	)
	status=$?

	assertEquals "Post-seed property reconcile source collection should fail closed when the blank-line filter command fails." \
		9 "$status"
	assertEquals "Post-seed property reconcile source collection should not emit output for filter-command failures." \
		"" "$output"
}

test_collect_post_seed_property_sources_reports_sorted_readback_failures() {
	post_seed_file="$TEST_TMPDIR/post_seed_readback_failure.txt"
	printf '%s\n' "tank/src" >"$post_seed_file"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/post-seed-readback-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			cat() {
				if [ "$1" = "$TEST_TMPDIR/post-seed-readback-2.tmp" ]; then
					printf '%s\n' "read failed" >&2
					return 9
				fi
				command cat "$@"
			}
			zxfer_collect_post_seed_property_sources "$post_seed_file"
		) 2>&1
	)
	status=$?

	assertEquals "Post-seed property reconcile source collection should fail closed when the sorted staging file cannot be read back." \
		9 "$status"
	assertContains "Post-seed property reconcile source collection should preserve the sorted staging readback diagnostic." \
		"$output" "read failed"
}

test_build_replication_iteration_list_reports_initial_tempfile_failures() {
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_build_replication_iteration_list 0
		)
	)
	status=$?

	assertEquals "Replication iteration-list building should fail closed when the first tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Replication iteration-list building should not emit output for first-tempfile failures." \
		"" "$output"
}

test_build_replication_iteration_list_reports_third_tempfile_failures() {
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 2 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/iteration-third-$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_build_replication_iteration_list 0
		)
	)
	status=$?

	assertEquals "Replication iteration-list building should fail closed when the third tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Replication iteration-list building should not emit output for third-tempfile failures." \
		"" "$output"
}
