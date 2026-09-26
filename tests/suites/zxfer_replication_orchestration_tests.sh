#!/bin/sh
# Replication initialization, orchestration, property, and metadata behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_check_backup_storage_dir_if_needed_routes_local_and_remote() {
	local_log="$TEST_TMPDIR/check_backup_local.log"
	remote_log="$TEST_TMPDIR/check_backup_remote.log"
	: >"$local_log"
	: >"$remote_log"

	(
		LOCAL_LOG="$local_log"
		zxfer_ensure_local_backup_dir() {
			printf '%s\n' "$1" >>"$LOCAL_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host=""
		g_backup_storage_root="$TEST_TMPDIR/local_backup"
		zxfer_check_backup_storage_dir_if_needed
	)

	(
		REMOTE_LOG="$remote_log"
		zxfer_run_remote_backup_script() {
			printf '%s|%s\n' "$1" "$4" >>"$REMOTE_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host="target.example"
		g_backup_storage_root="$TEST_TMPDIR/remote_backup"
		zxfer_check_backup_storage_dir_if_needed
	)

	assertEquals "Local backup checks should validate the local backup root." \
		"$TEST_TMPDIR/local_backup" "$(cat "$local_log")"
	assertEquals "Remote backup checks should run the directory program on the target host." \
		"target.example|preparing backup directory $TEST_TMPDIR/remote_backup" "$(cat "$remote_log")"
}

test_check_backup_storage_dir_if_needed_returns_success_when_disabled() {
	g_option_k_backup_property_mode=0

	zxfer_check_backup_storage_dir_if_needed
	status=$?

	assertEquals "Disabled backup metadata mode should remain a successful no-op at checked composition boundaries." \
		0 "$status"
}

# The root comes from ZXFER_BACKUP_DIR once, at session init; the check uses
# that validated root and never re-reads the environment.
test_check_backup_storage_dir_if_needed_refreshes_backup_root_from_environment() {
	output=$(
		(
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/stale_backup"
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/session backup"
			zxfer_init_backup_storage_root
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/later backup"
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)

	assertContains "Backup-dir preflight should preview the root that session init validated." \
		"$output" "'$TEST_TMPDIR/session backup'"
	assertNotContains "Backup-dir preflight should not preview an inherited root." \
		"$output" "'$TEST_TMPDIR/stale_backup'"
	assertNotContains "Backup-dir preflight should not re-read ZXFER_BACKUP_DIR after init." \
		"$output" "'$TEST_TMPDIR/later backup'"
}

test_check_backup_storage_dir_if_needed_dry_run_previews_without_mutating_dirs() {
	local_log="$TEST_TMPDIR/check_backup_dry_run_local.log"
	remote_log="$TEST_TMPDIR/check_backup_dry_run_remote.log"
	: >"$local_log"
	: >"$remote_log"

	output=$(
		(
			LOCAL_LOG="$local_log"
			REMOTE_LOG="$remote_log"
			zxfer_ensure_local_backup_dir() {
				printf '%s\n' "$1" >>"$LOCAL_LOG"
			}
			zxfer_run_remote_backup_script() {
				printf '%s %s\n' "$1" "$4" >>"$REMOTE_LOG"
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/local backup"
			zxfer_check_backup_storage_dir_if_needed
			g_zxfer_secure_path="/fresh/secure/path:/usr/bin"
			g_option_T_target_host="target.example doas"
			g_backup_storage_root="/var/db/zxfer remote"
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)

	assertEquals "Dry-run backup preflight should not call the live local backup-dir helper." \
		"" "$(cat "$local_log")"
	assertEquals "Dry-run backup preflight should not call the live remote backup-dir helper." \
		"" "$(cat "$remote_log")"
	assertContains "Dry-run backup preflight should preview the local secure backup-dir creation command." \
		"$output" "Dry run: umask 077; 'mkdir' '-p' '$TEST_TMPDIR/local backup'; 'chmod' '700' '$TEST_TMPDIR/local backup'"
	assertContains "Dry-run backup preflight should preview the remote ssh transport instead of executing it." \
		"$output" "Dry run: '/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'target.example'"
	assertContains "Dry-run backup preflight should preserve remote wrapper tokens in the rendered preview." \
		"$output" "doas"
	assertContains "Dry-run remote backup preflight should preview the secure-PATH prologue that live execution now applies." \
		"$output" "PATH="
	assertContains "Dry-run remote backup preflight should preview the session secure PATH." \
		"$output" "/fresh/secure/path:/usr/bin"
	assertContains "Dry-run remote backup preflight should preview the remote symlink guard that live execution enforces." \
		"$output" "Refusing to use symlinked zxfer backup directory."
	assertContains "Dry-run backup preflight should preview the remote secure backup-dir path." \
		"$output" "'/var/db/zxfer remote'"
	assertContains "Dry-run backup preflight should preview the remote chmod command." \
		"$output" "'chmod'"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_render_failures() {
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_render_remote_backup_dry_run_shell_command() {
			return 46
		}
		zxfer_check_backup_storage_dir_if_needed
	'

	assertEquals "Remote dry-run backup preflight should preserve prepared renderer failures." \
		46 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_builder_failures() {
	l_builder_log="$TEST_TMPDIR/remote_backup_dry_run_builder_failure.log"
	: >"$l_builder_log"

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell.
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_build_remote_backup_dir_prepare_cmd() {
			return 45
		}
		zxfer_render_remote_backup_dry_run_shell_command() {
			printf "%s\n" unexpected-renderer-call >>"$l_builder_log"
		}
		zxfer_check_backup_storage_dir_if_needed
	'

	assertEquals "Remote dry-run backup preflight should preserve directory-script builder failures." \
		45 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A failed directory-script build must stop before remote rendering." \
		"" "$(cat "$l_builder_log")"
}

# Session init validates ZXFER_BACKUP_DIR before the check can run.
test_check_backup_storage_dir_if_needed_rejects_relative_backup_dir_override() {
	zxfer_test_capture_subshell "
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_backup_storage_root='$TEST_TMPDIR/stale_backup'
		ZXFER_BACKUP_DIR='relative-backups'
		zxfer_init_backup_storage_root
		zxfer_check_backup_storage_dir_if_needed
	"

	assertEquals "Backup-dir preflight should fail closed when ZXFER_BACKUP_DIR is relative." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative backup-root preflight failures should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_BACKUP_DIR must be an absolute path"
}

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

test_initialize_replication_context_runs_restore_and_unsupported_scan() {
	log="$TEST_TMPDIR/init_context.log"
	: >"$log"
	g_initial_source="tank/src"
	g_option_R_recursive=""

	(
		CTX_LOG="$log"
		zxfer_get_backup_properties() {
			printf 'backup\n' >>"$CTX_LOG"
		}
		zxfer_get_zfs_list() {
			printf 'list\n' >>"$CTX_LOG"
		}
		zxfer_calculate_unsupported_properties() {
			printf 'unsupported\n' >>"$CTX_LOG"
		}
		g_option_e_restore_property_mode=1
		g_option_U_skip_unsupported_properties=1
		zxfer_initialize_replication_context
		printf 'recursive=%s\n' "$g_recursive_source_list" >>"$CTX_LOG"
	)

	assertEquals "Initialization should load backup properties, refresh dataset state, and derive unsupported properties." \
		"backup
list
unsupported
recursive=tank/src" "$(cat "$log")"
}

test_initialize_replication_context_skips_unsupported_scan_for_recursive_noop_without_property_work() {
	log="$TEST_TMPDIR/init_context_recursive_noop_u.log"
	: >"$log"
	g_initial_source="tank/src"

	(
		CTX_LOG="$log"
		zxfer_get_zfs_list() {
			printf 'list\n' >>"$CTX_LOG"
			g_recursive_source_list=""
			g_recursive_source_dataset_list=""
			g_recursive_destination_extra_dataset_list=""
		}
		zxfer_calculate_unsupported_properties() {
			printf 'unsupported\n' >>"$CTX_LOG"
		}
		g_option_R_recursive="tank/src"
		g_option_U_skip_unsupported_properties=1
		g_option_P_transfer_property=0
		g_option_o_override_property=""
		g_option_e_restore_property_mode=0
		g_option_k_backup_property_mode=0
		zxfer_initialize_replication_context
		printf 'recursive=%s\n' "$g_recursive_source_list" >>"$CTX_LOG"
	)

	assertEquals "Recursive -U initialization should skip unsupported-property probes when discovery found no source work and no property mode can consume the result." \
		"list
recursive=" "$(cat "$log")"
}

test_initialize_replication_context_runs_unsupported_scan_for_recursive_send_work_without_property_pass() {
	log="$TEST_TMPDIR/init_context_recursive_send_u.log"
	: >"$log"
	g_initial_source="tank/src"

	(
		CTX_LOG="$log"
		zxfer_get_zfs_list() {
			printf 'list\n' >>"$CTX_LOG"
			g_recursive_source_list="tank/src/child"
			g_recursive_source_dataset_list=""
			g_recursive_destination_extra_dataset_list=""
		}
		zxfer_calculate_unsupported_properties() {
			printf 'unsupported\n' >>"$CTX_LOG"
		}
		g_option_R_recursive="tank/src"
		g_option_U_skip_unsupported_properties=1
		g_option_P_transfer_property=0
		g_option_o_override_property=""
		g_option_e_restore_property_mode=0
		g_option_k_backup_property_mode=0
		zxfer_initialize_replication_context
		printf 'recursive=%s\n' "$g_recursive_source_list" >>"$CTX_LOG"
	)

	assertEquals "Recursive -U initialization should still probe unsupported properties when source snapshot work may need missing-dataset create options filtered." \
		"list
unsupported
recursive=tank/src/child" "$(cat "$log")"
}

test_run_zfs_mode_calls_steps_in_order_and_relaunches_for_migration() {
	log="$TEST_TMPDIR/zxfer_run_zfs_mode.log"
	: >"$log"

	(
		RUN_LOG="$log"
		zxfer_prepare_zfs_mode_roots() { printf 'roots\n' >>"$RUN_LOG"; }
		zxfer_check_backup_storage_dir_if_needed() { printf 'backupdir\n' >>"$RUN_LOG"; }
		zxfer_initialize_replication_context() { printf 'context\n' >>"$RUN_LOG"; }
		zxfer_maybe_capture_preflight_snapshot() { printf 'snapshot\n' >>"$RUN_LOG"; }
		zxfer_prepare_migration_services() {
			printf 'prepare\n' >>"$RUN_LOG"
			zxfer_set_failure_stage "migration service handling"
		}
		zxfer_perform_grandfather_protection_checks() {
			printf 'grandfather stage=%s\n' "$g_zxfer_failure_stage" >>"$RUN_LOG"
		}
		zxfer_copy_filesystems() { printf 'copy\n' >>"$RUN_LOG"; }
		zxfer_relaunch() { printf 'zxfer_relaunch\n' >>"$RUN_LOG"; }
		g_option_m_migrate=1
		zxfer_run_zfs_mode
	)

	assertEquals "zxfer_run_zfs_mode should execute its major phases in the expected order, reporting the -g pre-pass and copy as the replication stage." \
		"roots
backupdir
context
snapshot
prepare
grandfather stage=replication
copy
zxfer_relaunch" "$(cat "$log")"
}

test_run_zfs_mode_dry_run_skips_live_planning_and_copy() {
	log="$TEST_TMPDIR/zxfer_run_zfs_mode_dry_run.log"
	: >"$log"

	output=$(
		(
			RUN_LOG="$log"
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_prepare_zfs_mode_roots() {
				printf 'roots\n' >>"$RUN_LOG"
				g_initial_source="tank/src"
			}
			zxfer_check_backup_storage_dir_if_needed() { printf 'backupdir\n' >>"$RUN_LOG"; }
			zxfer_initialize_replication_context() { printf 'context\n' >>"$RUN_LOG"; }
			zxfer_maybe_capture_preflight_snapshot() { printf 'snapshot\n' >>"$RUN_LOG"; }
			zxfer_prepare_migration_services() { printf 'prepare\n' >>"$RUN_LOG"; }
			zxfer_perform_grandfather_protection_checks() { printf 'grandfather\n' >>"$RUN_LOG"; }
			zxfer_copy_filesystems() { printf 'copy\n' >>"$RUN_LOG"; }
			zxfer_relaunch() { printf 'zxfer_relaunch\n' >>"$RUN_LOG"; }
			g_option_n_dryrun=1
			g_option_s_make_snapshot=1
			g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
			zxfer_run_zfs_mode
		)
	)

	assertEquals "Strict dry-run should stop before live replication planning, grandfather checks, data copy, or zxfer_relaunch." \
		"roots
backupdir
snapshot
prepare" "$(cat "$log")"
	assertContains "Strict dry-run should explain that live planning is skipped." \
		"$output" "Dry run: skipping live replication-state validation and command planning."
	assertContains "Strict dry-run should explain that live %%size%% discovery is skipped." \
		"$output" "Dry run: skipping live %%size%% progress estimate discovery."
	assertContains "Strict dry-run should explain that send/receive rendering is skipped without live discovery." \
		"$output" "Dry run: send/receive and property-reconcile commands require live snapshot discovery and are not rendered."
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

test_preview_zfs_mode_dry_run_overwrites_stale_recursive_state() {
	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_maybe_capture_preflight_snapshot() {
				printf 'snapshot_list=<%s>\n' "$g_recursive_source_list"
			}
			zxfer_prepare_migration_services() {
				printf 'prepare_list=<%s>\n' "$g_recursive_source_list"
				printf 'prepare_datasets=<%s>\n' "$g_recursive_source_dataset_list"
				printf 'prepare_extras=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
				printf 'prepare_dest=<%s>\n' "${g_recursive_dest_list:-}"
			}
			g_initial_source="tank/src"
			g_option_R_recursive="tank/src"
			g_recursive_source_list="stale/src
stale/src/child"
			g_recursive_source_dataset_list="stale/src
stale/src/child"
			g_recursive_destination_extra_dataset_list="stale-extra"
			g_recursive_dest_list="stale-dest"
			g_source_snapshot_list_cmd="stale-command"
			g_destination_existence_cache_root="stale-root"
			zxfer_preview_zfs_mode_dry_run
			printf 'after_list=<%s>\n' "$g_recursive_source_list"
			printf 'after_datasets=<%s>\n' "$g_recursive_source_dataset_list"
			printf 'after_extras=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'after_dest=<%s>\n' "${g_recursive_dest_list:-}"
			printf 'after_source_cmd=<%s>\n' "${g_source_snapshot_list_cmd:-}"
			printf 'after_dest_cache_root=<%s>\n' "${g_destination_existence_cache_root:-}"
		)
	)

	assertContains "Strict dry-run preview should explain that recursive descendant discovery is skipped." \
		"$output" "Dry run: recursive descendant discovery is skipped; previewing only the explicitly requested source dataset."
	assertContains "Strict dry-run preview should replace stale recursive source state before the snapshot preview runs." \
		"$output" "snapshot_list=<tank/src>"
	assertContains "Strict dry-run preview should expose only the explicit source dataset to later preview helpers." \
		"$output" "prepare_list=<tank/src>"
	assertContains "Strict dry-run preview should reset the cached recursive source dataset list to the explicit source dataset." \
		"$output" "prepare_datasets=<tank/src>"
	assertContains "Strict dry-run preview should clear stale destination-extra datasets." \
		"$output" "prepare_extras=<>"
	assertContains "Strict dry-run preview should clear stale destination dataset caches." \
		"$output" "prepare_dest=<>"
	assertContains "Strict dry-run preview should leave the current-shell recursive source list normalized to the explicit source dataset." \
		"$output" "after_list=<tank/src>"
	assertContains "Strict dry-run preview should leave the current-shell recursive source dataset cache normalized to the explicit source dataset." \
		"$output" "after_datasets=<tank/src>"
	assertContains "Strict dry-run preview should leave destination-extra datasets cleared in the current shell." \
		"$output" "after_extras=<>"
	assertContains "Strict dry-run preview should leave destination dataset caches cleared in the current shell." \
		"$output" "after_dest=<>"
	assertContains "Strict dry-run preview should clear the stale rendered source snapshot command." \
		"$output" "after_source_cmd=<>"
	assertContains "Strict dry-run preview should clear the destination existence cache root." \
		"$output" "after_dest_cache_root=<>"
}

test_zxfer_preview_zfs_mode_dry_run_emits_restore_and_unsupported_property_notices() {
	output=$(
		(
			g_option_e_restore_property_mode=1
			g_option_U_skip_unsupported_properties=1
			zxfer_progress_dialog_uses_size_estimate() {
				return 1
			}
			zxfer_echoV() {
				printf '%s\n' "$1"
			}
			zxfer_preview_zfs_mode_dry_run
		)
	)

	assertContains "Dry-run preview should explain that it is skipping live backup-metadata restore validation when restore mode is enabled." \
		"$output" "Dry run: skipping live backup-metadata restore validation."
	assertContains "Dry-run preview should explain that it is skipping live unsupported-property detection when that scan is disabled." \
		"$output" "Dry run: skipping live unsupported-property detection."
}

# Without -d the -g pre-pass still plans every dataset, so divergence is
# refused before any -F receive, but it reads no creation times.
test_perform_grandfather_protection_checks_plans_every_dataset_under_g_and_checks_deletes_only_with_d() {
	log="$TEST_TMPDIR/grandfather_without_d.log"
	: >"$log"

	(
		GRANDFATHER_LOG="$log"
		g_recursive_source_list="tank/src"
		zxfer_set_actual_dest() { printf 'set %s\n' "$1" >>"$GRANDFATHER_LOG"; }
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$GRANDFATHER_LOG"
			g_zxfer_plan_delete_snapshots="backup/target/src@old"
		}
		zxfer_prepare_snapshot_delete_creation_state() {
			printf 'prefetch %s\n' "$1" >>"$GRANDFATHER_LOG"
		}

		g_option_g_grandfather_protection=""
		g_option_d_delete_destination_snapshots=1
		zxfer_perform_grandfather_protection_checks

		g_option_g_grandfather_protection=30
		g_option_d_delete_destination_snapshots=0
		zxfer_perform_grandfather_protection_checks
	)

	assertEquals "Without -g there is no pre-pass; -g alone plans each dataset but checks no delete." \
		"set tank/src
inspect 0 tank/src" "$(cat "$log")"
}

test_perform_grandfather_protection_checks_reads_creation_times_for_every_planned_delete() {
	log="$TEST_TMPDIR/grandfather_calls.log"
	: >"$log"

	output=$(
		(
			GRANDFATHER_LOG="$log"
			g_option_g_grandfather_protection=30
			g_option_d_delete_destination_snapshots=1
			g_option_R_recursive="tank/src"
			g_initial_source="tank/src"
			g_recursive_source_list="tank/src
tank/src/my child"
			g_recursive_destination_extra_dataset_list="tank/src/extra"
			zxfer_inspect_delete_snap() {
				printf 'inspect %s %s -> %s\n' "$1" "$2" "$g_actual_dest" >>"$GRANDFATHER_LOG"
				if IFS= read -r l_stolen; then
					printf 'stole %s\n' "$l_stolen" >>"$GRANDFATHER_LOG"
				fi
				g_zxfer_plan_delete_snapshots=""
				g_zxfer_plan_source_count=1
				[ "$2" != "tank/src/extra" ] || {
					g_zxfer_plan_delete_snapshots="$g_actual_dest@old"
					g_zxfer_plan_source_count=0
				}
			}
			zxfer_prepare_snapshot_delete_creation_state() {
				printf 'prefetch %s\n' "$1" >>"$GRANDFATHER_LOG"
			}
			IFS=:
			set -f
			zxfer_perform_grandfather_protection_checks
			# Glob a pattern that matches this suite's own tmpdir: it stays
			# literal only while globbing is still disabled.
			l_after_glob=$(printf '%s\n' "$TEST_TMPDIR"/*)
			printf 'ifs=<%s> literal_glob=%s\n' "$IFS" "$l_after_glob"
		)
	)

	assertEquals "The pre-pass should plan each source and destination-delta dataset once, parents first, and apply -g to each planned delete list, even a source-less one the main pass would recheck first." \
		"inspect 0 tank/src -> backup/target/src
inspect 0 tank/src/my child -> backup/target/src/my child
inspect 0 tank/src/extra -> backup/target/src/extra
prefetch backup/target/src/extra@old" "$(cat "$log")"
	assertEquals "The pre-pass should preserve a caller-defined IFS and disabled globbing." \
		"ifs=<:> literal_glob=$TEST_TMPDIR/*" "$output"
}

test_copy_filesystems_inspects_source_when_only_deletions_pending() {
	g_option_d_delete_destination_snapshots=1
	g_initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src"
	g_recursive_destination_extra_dataset_list="tank/src"
	log="$TEST_TMPDIR/delete_only_single.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$COPY_FS_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$COPY_FS_LOG"
		}
		zxfer_copy_filesystems
	)

	expected="set tank/src
inspect 1 tank/src
copy tank/src"
	assertEquals "-d should still inspect datasets with destination-only snapshots even when no new snapshots exist." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_skips_recursive_delete_iteration_when_global_snapshot_diffs_are_empty() {
	g_option_d_delete_destination_snapshots=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src
tank/src/child1
tank/src/child2"
	g_recursive_destination_extra_dataset_list=""
	log="$TEST_TMPDIR/delete_only_recursive_empty.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		zxfer_set_actual_dest() {
			printf 'set %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$COPY_FS_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$COPY_FS_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_copy_filesystems
	)

	assertEquals "Recursive -d runs should skip per-dataset inspection when discovery already proved there are no source or destination snapshot deltas." \
		"wait final sync" "$(cat "$log")"
}

test_copy_filesystems_shortcuts_clean_recursive_noop_before_iteration_staging() {
	g_option_d_delete_destination_snapshots=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	log="$TEST_TMPDIR/clean_recursive_noop_shortcut.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		zxfer_build_replication_iteration_list() {
			printf 'unexpected-build\n' >>"$COPY_FS_LOG"
			return 1
		}
		zxfer_get_temp_file() {
			printf 'unexpected-temp\n' >>"$COPY_FS_LOG"
			return 1
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_copy_filesystems
	)

	assertEquals "Clean recursive no-op runs should bypass iteration staging." \
		"wait final sync" "$(cat "$log")"
}

test_copy_filesystems_inspects_only_datasets_with_recursive_delete_deltas() {
	g_option_d_delete_destination_snapshots=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src
tank/src/child1
tank/src/child2"
	g_recursive_destination_extra_dataset_list="tank/src/child1
tank/src/child2"
	log="$TEST_TMPDIR/delete_only_recursive.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$COPY_FS_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$COPY_FS_LOG"
		}
		zxfer_copy_filesystems
	)

	expected="set tank/src/child1
inspect 1 tank/src/child1
copy tank/src/child1
set tank/src/child2
inspect 1 tank/src/child2
copy tank/src/child2"
	assertEquals "Recursive -d runs should inspect only datasets with source or destination snapshot deltas." \
		"$expected" "$(cat "$log")"
}

test_copy_snapshots_seeds_existing_destination_into_snapshot() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	stub_dest_created_by_zxfer=0
	g_src_snapshot_transfer_list="tank/src@seed1
tank/src@seed2"
	log="$TEST_TMPDIR/seed_existing.log"
	rm -f "$log"

	(
		SEED_LOG="$log"
		zxfer_reconcile_live_destination_snapshot_state() { :; }
		zxfer_rollback_destination_to_last_common_snapshot() { :; }
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_destination_zfs_cmd() { return 0; }
		zxfer_zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s force=%s bg=%s\n' \
				"${1:-<none>}" "$2" "$3" "${5:-<none>}" "$4" >>"$SEED_LOG"
		}
		zxfer_copy_snapshots
	)

	expected="prev=<none> curr=tank/src@seed1 dest=backup/target/src force=-F bg=0"
	assertEquals "Existing destinations without snapshots should be seeded with forced receive." \
		"$expected" "$(head -n 1 "$log")"
	assertEquals "Existing-destination seeding should not mutate the parsed -F option state." \
		"" "${g_option_F_force_rollback:-}"
}

test_copy_filesystems_forces_iteration_when_property_transfer_is_enabled() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	log="$TEST_TMPDIR/property_iteration.log"
	rm -f "$log"

	(
		ITER_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$ITER_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$ITER_LOG"
		}
		zxfer_transfer_properties() {
			printf 'props %s\n' "$1" >>"$ITER_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$ITER_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			:
		}
		zxfer_copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src
copy tank/src
set tank/src/child
inspect 0 tank/src/child
props tank/src/child
copy tank/src/child"
	assertEquals "Property transfer in recursive mode should force iteration over every dataset." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_property_no_snapshot_delta_does_not_send() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src"
	log="$TEST_TMPDIR/property_no_snapshot_delta.log"
	rm -f "$log"

	(
		ITER_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$ITER_LOG"
		}
		zxfer_inspect_delete_snap() {
			g_dest_has_snapshots=1
			g_last_common_snap="tank/src@autosnap_2026-05-19_18:15:01_frequently	1815"
			g_src_snapshot_transfer_list=""
			printf 'inspect %s %s\n' "$1" "$2" >>"$ITER_LOG"
		}
		zxfer_transfer_properties() {
			printf 'props %s\n' "$1" >>"$ITER_LOG"
		}
		zxfer_reconcile_live_destination_snapshot_state() {
			printf 'recheck %s\n' "$g_actual_dest" >>"$ITER_LOG"
		}
		zxfer_zfs_send_receive() {
			printf 'unexpected-send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$ITER_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$ITER_LOG"
		}

		zxfer_copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src
recheck backup/target/src
wait final sync"
	assertEquals "Property-only recursive iterations with no per-dataset snapshot delta must not start a send." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_reconciles_properties_after_seeding_created_destination() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/property_reconcile.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_set_actual_dest() {
			case "$1" in
			tank/src) g_actual_dest="backup/target/src" ;;
			tank/src/child) g_actual_dest="backup/target/src/child" ;;
			esac
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		zxfer_transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${stub_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
			if [ "$1" = "tank/src/child" ] && [ "${2:-0}" -eq 0 ]; then
				stub_dest_created_by_zxfer=1
			fi
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${stub_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			if [ "$g_actual_dest" = "backup/target/src/child" ]; then
				g_dest_seed_requires_property_reconcile=1
			else
				g_dest_seed_requires_property_reconcile=0
			fi
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
set tank/src/child
inspect 0 tank/src/child
props tank/src/child created=0 skip=0 dest_present=0
copy backup/target/src/child created=1
wait final sync
set tank/src/child
props tank/src/child created=1 skip=0 dest_present=1"
	assertEquals "Created destinations should receive a second property reconciliation after the seed snapshot is received." \
		"$expected" "$(cat "$log")"
}
