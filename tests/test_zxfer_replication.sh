#!/bin/sh
#
# shunit2 tests for zxfer_replication.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_replication.sh"

zxfer_get_zfs_list() {
	STUB_ZFS_LIST_CALLS=$((STUB_ZFS_LIST_CALLS + 1))
}

zxfer_stopsvcs() {
	cat >>"$STUB_STOPSVCS_LOG"
}

zxfer_newsnap() {
	printf '%s\n' "$1" >>"$STUB_NEW_SNAP_LOG"
}

zxfer_wait_for_zfs_send_jobs() {
	:
}

mock_zfs_tool() {
	printf '%s\n' "$*" >>"$STUB_ZFS_CMD_LOG"
}

zxfer_run_source_zfs_cmd() {
	if [ "$1" = "get" ] && [ "$2" = "-Ho" ] && [ "$3" = "value" ] && [ "$4" = "mounted" ]; then
		# Simulate a mounted filesystem so migration preflight passes.
		printf 'yes\n'
		return 0
	fi

	if [ "$1" = "unmount" ]; then
		printf 'unmount %s\n' "$2" >>"$STUB_ZFS_CMD_LOG"
		return 0
	fi

	mock_zfs_tool "$@"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_replication"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	: >"$TEST_TMPDIR/zxfer_stopsvcs.log"
	: >"$TEST_TMPDIR/zxfer_newsnap.log"
	: >"$TEST_TMPDIR/zfs_cmd.log"
	unset ZXFER_BACKUP_DIR
	STUB_STOPSVCS_LOG="$TEST_TMPDIR/zxfer_stopsvcs.log"
	STUB_NEW_SNAP_LOG="$TEST_TMPDIR/zxfer_newsnap.log"
	STUB_ZFS_CMD_LOG="$TEST_TMPDIR/zfs_cmd.log"
	STUB_ZFS_LIST_CALLS=0
	zxfer_reset_destination_existence_cache
	zxfer_get_zfs_list() {
		STUB_ZFS_LIST_CALLS=$((STUB_ZFS_LIST_CALLS + 1))
	}
	zxfer_stopsvcs() {
		cat >>"$STUB_STOPSVCS_LOG"
	}
	zxfer_newsnap() {
		printf '%s\n' "$1" >>"$STUB_NEW_SNAP_LOG"
	}
	zxfer_wait_for_zfs_send_jobs() {
		:
	}
	mock_zfs_tool() {
		printf '%s\n' "$*" >>"$STUB_ZFS_CMD_LOG"
	}
	zxfer_run_source_zfs_cmd() {
		if [ "$1" = "get" ] && [ "$2" = "-Ho" ] && [ "$3" = "value" ] && [ "$4" = "mounted" ]; then
			printf 'yes\n'
			return 0
		fi

		if [ "$1" = "unmount" ]; then
			printf 'unmount %s\n' "$2" >>"$STUB_ZFS_CMD_LOG"
			return 0
		fi

		mock_zfs_tool "$@"
	}

	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_n_dryrun=0
	g_option_s_make_snapshot=0
	g_option_m_migrate=0
	g_option_c_services=""
	g_option_d_delete_destination_snapshots=0
	g_option_F_force_rollback=""
	g_option_P_transfer_property=0
	g_option_k_backup_property_mode=0
	g_option_o_override_property=""
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	g_initial_source=""
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_recursive_dest_list=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_did_delete_dest_snapshots=0
	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_src_snapshot_transfer_list=""
	g_actual_dest=""
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
	g_backup_file_contents=""
	g_pending_backup_file_contents=""
	g_zxfer_post_seed_property_sources_result=""
	g_zxfer_replication_iteration_list_result=""
	g_zxfer_replication_file_read_result=""
	g_zxfer_new_snapshot_name="zxfer_test_snapshot"
	g_zxfer_source_pvs_raw=""
	g_test_base_readonly_properties="type,mountpoint,creation"
	g_LZFS="mock_zfs_tool"
	g_dest_created_by_zxfer=0
	g_dest_seed_requires_property_reconcile=0
	g_test_max_yield_iterations=8
	zxfer_get_base_readonly_properties() {
		printf '%s\n' "$g_test_base_readonly_properties"
	}
	zxfer_get_max_yield_iterations() {
		printf '%s\n' "$g_test_max_yield_iterations"
	}
}

test_resolve_initial_source_prefers_recursive_flag() {
	g_option_R_recursive="tank/src"

	zxfer_resolve_initial_source_from_options

	assertEquals "Recursive source should be selected when -R is provided." "$g_option_R_recursive" "$g_initial_source"
}

test_resolve_initial_source_uses_nonrecursive_when_only_N_set() {
	g_option_N_nonrecursive="tank/nonrecursive"

	zxfer_resolve_initial_source_from_options

	assertEquals "Non-recursive source should be selected when -N is provided." "$g_option_N_nonrecursive" "$g_initial_source"
}

test_resolve_initial_source_conflicts_trigger_usage_error() {
	g_option_R_recursive="tank/src"
	g_option_N_nonrecursive="tank/child"

	if (zxfer_resolve_initial_source_from_options) >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi

	if [ "$status" -eq 0 ]; then
		fail "Conflicting -N/-R flags must exit with a usage error."
	fi

	assertEquals "Conflicting options should yield usage exit status 2." "2" "$status"
}

test_validate_zfs_mode_preconditions_rejects_services_without_svcadm() {
	g_option_m_migrate=1
	g_option_c_services="svc:/network/nfs/server"
	g_initial_source="tank/src"
	empty_path="$TEST_TMPDIR/no_svcadm"
	mkdir -p "$empty_path"
	old_path=$PATH

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		PATH="$empty_path"
		zxfer_validate_zfs_mode_preconditions
	) >/dev/null 2>&1
	status=$?
	PATH=$old_path

	assertEquals "Service migration should fail fast when svcadm is unavailable." "2" "$status"
}

test_resolve_initial_source_requires_N_or_R() {
	g_option_R_recursive=""
	g_option_N_nonrecursive=""

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_resolve_initial_source_from_options
	) >/dev/null 2>&1
	status=$?

	assertEquals "Missing -N/-R options should exit with a usage error." "2" "$status"
}

test_normalize_source_destination_strips_trailing_slashes() {
	g_initial_source="tank/src///"
	g_destination="backup/target//"

	zxfer_normalize_source_destination_paths

	assertEquals "Trailing slashes should be removed from source." "tank/src" "$g_initial_source"
	assertEquals "Trailing slashes should be removed from destination." "backup/target" "$g_destination"
	assertEquals "Trailing slash flag should record the original suffix." "1" "$g_initial_source_had_trailing_slash"
}

test_normalize_source_destination_rejects_absolute_paths() {
	g_initial_source="/tank/src"
	g_destination="backup/target"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_normalize_source_destination_paths
	) >/dev/null 2>&1
	status_source=$?

	g_initial_source="tank/src"
	g_destination="/backup/target"
	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_normalize_source_destination_paths
	) >/dev/null 2>&1
	status_dest=$?

	assertEquals "Absolute source paths should be rejected." "2" "$status_source"
	assertEquals "Absolute destination paths should be rejected." "2" "$status_dest"
}

test_zxfer_read_replication_stage_file_preserves_runtime_readback_failures() {
	stage_file="$TEST_TMPDIR/replication_stage_failure"
	printf '%s\n' "tank/src" >"$stage_file"

	output=$(
		(
			g_zxfer_replication_file_read_result="stale-replication-stage"
			zxfer_read_runtime_artifact_file() {
				return 27
			}
			zxfer_read_replication_stage_file "$stage_file" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'scratch=<%s>\n' "$g_zxfer_replication_file_read_result"
		)
	)

	assertContains "Replication stage-file reads should preserve runtime readback failures exactly." \
		"$output" "status=27"
	assertContains "Replication stage-file readback failures should clear the published scratch result." \
		"$output" "scratch=<>"
}

test_current_destination_is_initial_source_dataset_matches_resolved_destination() {
	output=$(
		(
			g_initial_source="tank/src"
			g_actual_dest="backup/target/src"
			zxfer_compute_actual_dest_for_source() {
				printf '%s\n' "backup/target/src"
			}
			if zxfer_current_destination_is_initial_source_dataset; then
				printf 'status=0\n'
			else
				printf 'status=%s\n' "$?"
			fi
		)
	)

	assertEquals "Current-destination checks should succeed when the resolved initial destination matches the active destination." \
		"status=0" "$output"
}

test_current_destination_is_initial_source_dataset_fails_closed_when_resolution_fails() {
	output=$(
		(
			g_initial_source="tank/src"
			g_actual_dest="backup/target/src"
			zxfer_compute_actual_dest_for_source() {
				return 41
			}
			if zxfer_current_destination_is_initial_source_dataset; then
				printf 'status=0\n'
			else
				printf 'status=%s\n' "$?"
			fi
		)
	)

	assertEquals "Current-destination checks should fail closed when destination resolution fails." \
		"status=1" "$output"
}

test_rollback_destination_to_last_common_snapshot_shortcuts_non_destructive_cases_in_current_shell() {
	output=$(
		(
			log="$TEST_TMPDIR/rollback_shortcuts_current.log"
			: >"$log"
			g_actual_dest="backup/target/src"
			g_last_common_snap="tank/src@snap1"
			zxfer_exists_destination() {
				printf '%s\n' "exists" >>"$log"
				printf '%s\n' "0"
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "rollback" >>"$log"
			}

			g_option_F_force_rollback=""
			g_did_delete_dest_snapshots=1
			g_deleted_dest_newer_snapshots=1
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'no_force_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_option_F_force_rollback=1
			g_did_delete_dest_snapshots=0
			g_deleted_dest_newer_snapshots=1
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'no_delete_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_did_delete_dest_snapshots=1
			g_deleted_dest_newer_snapshots=0
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'no_newer_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_deleted_dest_newer_snapshots=1
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'missing_dest_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_last_common_snap=""
			zxfer_exists_destination() {
				printf '%s\n' "exists" >>"$log"
				printf '%s\n' "1"
			}
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'empty_common_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"
			printf 'rollback_calls=%s\n' "$(awk '/^rollback$/ { count++ } END { print count + 0 }' "$log")"
		)
	)

	assertContains "Destination rollback should not probe live state when receive-side forcing is disabled." \
		"$output" "no_force_exists=0"
	assertContains "Destination rollback should not probe live state when no destination snapshots were deleted." \
		"$output" "no_delete_exists=0"
	assertContains "Destination rollback should not probe live state when no newer destination snapshots were deleted." \
		"$output" "no_newer_exists=0"
	assertContains "Destination rollback should stop without rolling back when the destination no longer exists." \
		"$output" "missing_dest_exists=1"
	assertContains "Destination rollback should stop without issuing a rollback when there is no last common snapshot name." \
		"$output" "empty_common_exists=2"
	assertContains "Destination rollback should not issue rollback commands in any non-destructive shortcut path." \
		"$output" "rollback_calls=0"
}

test_set_actual_dest_without_trailing_slash_appends_relative_path() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0

	zxfer_set_actual_dest "tank/src/projects/alpha"

	assertEquals "Destination should mirror the relative source suffix." "backup/target/src/projects/alpha" "$g_actual_dest"
}

test_set_actual_dest_with_trailing_slash_preserves_destination_prefix() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=1

	zxfer_set_actual_dest "tank/src/projects/beta"

	assertEquals "Trailing slash should replicate directly under the destination root." "backup/target/projects/beta" "$g_actual_dest"
}

test_set_actual_dest_treats_regex_significant_source_names_as_literal_paths() {
	g_initial_source="tank/app.v1"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0

	zxfer_set_actual_dest "tank/app.v1/projects.release"

	assertEquals "Destination mapping should preserve dots in the source root and child dataset names as literal path components." \
		"backup/target/app.v1/projects.release" "$g_actual_dest"
}

test_refresh_dataset_iteration_state_populates_recursive_list_when_not_recursive() {
	g_option_R_recursive=""
	g_initial_source="tank/src"
	g_recursive_source_list=""
	STUB_ZFS_LIST_CALLS=0

	zxfer_refresh_dataset_iteration_state

	assertEquals "Refresh should re-populate the recursive source list when -R is unset." \
		"$g_initial_source" "$g_recursive_source_list"
	assertEquals "zxfer_get_zfs_list should be invoked once during refresh." "1" "$STUB_ZFS_LIST_CALLS"
}

test_refresh_dataset_iteration_state_preserves_list_when_recursive_mode_set() {
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src tank/src/child"
	STUB_ZFS_LIST_CALLS=0

	zxfer_refresh_dataset_iteration_state

	assertEquals "Recursive option should keep the existing dataset list untouched." \
		"tank/src tank/src/child" "$g_recursive_source_list"
	assertEquals "zxfer_get_zfs_list should still be called exactly once." "1" "$STUB_ZFS_LIST_CALLS"
}

test_refresh_dataset_iteration_state_refreshes_property_tree_prefetch_context_when_available() {
	log="$TEST_TMPDIR/refresh_prefetch_context.log"
	: >"$log"

	(
		REFRESH_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh-prefetch\n' >>"$REFRESH_LOG"
		}
		zxfer_refresh_dataset_iteration_state
	)

	assertEquals "Refreshing dataset iteration state should also refresh the recursive property-tree prefetch context when that optimization helper is available." \
		"refresh-prefetch" "$(cat "$log")"
}

test_maybe_capture_preflight_snapshot_captures_when_enabled() {
	g_option_s_make_snapshot=1
	g_option_n_dryrun=0
	g_initial_source="tank/src"

	zxfer_maybe_capture_preflight_snapshot

	assertEquals "Snapshot helper should run once when -s is enabled." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset state should call zxfer_get_zfs_list exactly once." "1" "$STUB_ZFS_LIST_CALLS"
}

test_maybe_capture_preflight_snapshot_dry_run_skips_refresh() {
	g_option_s_make_snapshot=1
	g_option_n_dryrun=1
	g_initial_source="tank/src"

	zxfer_maybe_capture_preflight_snapshot

	assertEquals "Dry-run -s should still preview the snapshot helper once." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dry-run -s should not refresh cached dataset state." "0" "$STUB_ZFS_LIST_CALLS"
}

test_maybe_capture_preflight_snapshot_skips_when_migrating() {
	g_option_s_make_snapshot=1
	g_option_m_migrate=1
	g_initial_source="tank/src"

	zxfer_maybe_capture_preflight_snapshot

	assertEquals "Migration path should not trigger new snapshots from -s." "" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dataset refresh should not run when snapshot is skipped." "0" "$STUB_ZFS_LIST_CALLS"
}

test_prepare_migration_services_stops_services_and_unmounts_sources() {
	g_option_m_migrate=1
	g_option_n_dryrun=0
	g_option_c_services="svc:/network/iscsi_target svc:/network/nfs/server"
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src tank/src/child"

	zxfer_prepare_migration_services

	assertEquals "Services should be piped to zxfer_stopsvcs intact." \
		"svc:/network/iscsi_target svc:/network/nfs/server" "$(cat "$STUB_STOPSVCS_LOG")"
	assertEquals "All recursive datasets must be unmounted before migrating." \
		"unmount tank/src
unmount tank/src/child" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Migration must create a final snapshot for the initial source." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset lists should run exactly once." "1" "$STUB_ZFS_LIST_CALLS"
	case "$(zxfer_get_effective_readonly_properties)" in
	*mountpoint*)
		fail "Readonly properties list should drop mountpoint during migration."
		;;
	esac
	assertEquals "Migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$(zxfer_get_base_readonly_properties)"
}

test_prepare_migration_services_dry_run_previews_without_mutating_state() {
	g_option_m_migrate=1
	g_option_n_dryrun=1
	g_option_v_verbose=1
	g_option_c_services="svc:/network/iscsi_target svc:/network/nfs/server"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src tank/src/child"
	state_log="$TEST_TMPDIR/migration_dry_run_state.log"
	output=$(
		(
			zxfer_prepare_migration_services
			printf 'readonly=%s\n' "$(zxfer_get_effective_readonly_properties)" >"$state_log"
			printf 'restart=%s\n' "$g_zxfer_services_to_restart" >>"$state_log"
			printf 'need=%s\n' "$g_services_need_relaunch" >>"$state_log"
		) 2>&1
	)

	assertEquals "Dry-run migration should not call zxfer_stopsvcs." "" "$(cat "$STUB_STOPSVCS_LOG")"
	assertEquals "Dry-run migration should not unmount any datasets." "" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Dry-run migration should still preview the final snapshot helper once." \
		"tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dry-run migration should not refresh cached dataset state." "0" "$STUB_ZFS_LIST_CALLS"
	case "$(grep '^readonly=' "$state_log")" in
	*mountpoint*)
		fail "Dry-run migration should still drop mountpoint from the effective readonly-property list."
		;;
	esac
	assertEquals "Dry-run migration should leave the base readonly-property defaults unchanged." \
		"type,mountpoint,creation" "$(zxfer_get_base_readonly_properties)"
	assertContains "Dry-run migration should still track which services would need zxfer_relaunch later." \
		"$(cat "$state_log")" "restart= svc:/network/iscsi_target svc:/network/nfs/server"
	assertContains "Dry-run migration should still flag zxfer_relaunch as required." \
		"$(cat "$state_log")" "need=1"
	assertContains "Dry-run migration should preview service-disabling commands." \
		"$output" "Dry run: 'svcadm' 'disable' '-st' 'svc:/network/iscsi_target'"
	assertContains "Dry-run migration should preview unmount commands for each source dataset." \
		"$output" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/src'"
	assertContains "Dry-run migration should preview descendant unmount commands too." \
		"$output" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/src/child'"
}

test_prepare_migration_services_dry_run_uses_mountpoint_free_effective_readonly_list() {
	g_option_m_migrate=1
	g_option_n_dryrun=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_test_base_readonly_properties="type,mountpoint,creation"

	zxfer_prepare_migration_services

	assertEquals "Dry-run migration should drop mountpoint from the effective readonly-property list." \
		"type,creation" "$(zxfer_get_effective_readonly_properties)"
	assertEquals "Dry-run migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$(zxfer_get_base_readonly_properties)"
}

test_prepare_migration_services_preserves_service_restart_state_in_current_shell() {
	g_option_m_migrate=1
	g_option_c_services="svc:/system/filesystem/local"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"

	zxfer_stopsvcs() {
		cat >/dev/null
		g_zxfer_services_to_restart=" svc:/system/filesystem/local"
		g_services_need_relaunch=1
	}

	zxfer_prepare_migration_services

	assertEquals "Migration preflight should retain the service restart list in the parent shell." \
		" svc:/system/filesystem/local" "$g_zxfer_services_to_restart"
	assertEquals "Migration preflight should retain the zxfer_relaunch flag in the parent shell." \
		"1" "$g_services_need_relaunch"
}

test_prepare_migration_services_passes_multiline_service_input_to_stopsvcs_in_current_shell() {
	g_option_m_migrate=1
	g_option_c_services="svc:/network/nfs/server
svc:/system/filesystem/local"
	g_recursive_source_list=""
	service_input_file="$TEST_TMPDIR/prepare_migration_services.stdin"

	zxfer_stopsvcs() {
		cat >"$service_input_file"
	}

	zxfer_prepare_migration_services
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Migration preflight should pass the configured multiline service list through stdin to zxfer_stopsvcs unchanged." \
		"svc:/network/nfs/server
svc:/system/filesystem/local" "$(cat "$service_input_file")"
}

test_prepare_migration_services_propagates_service_disable_failures() {
	g_option_m_migrate=1
	g_option_c_services="svc:/system/filesystem/local"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			zxfer_stopsvcs() {
				cat >/dev/null
				zxfer_throw_error "Could not disable service svc:/system/filesystem/local."
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should stop when service disabling fails." "1" "$status"
	assertContains "Migration preflight should surface the service-disable failure." \
		"$output" "Could not disable service svc:/system/filesystem/local."
}

test_stopsvcs_disables_services_and_tracks_restart_state() {
	log="$TEST_TMPDIR/stopsvcs_actions.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
trap - EXIT INT TERM HUP QUIT
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
svcadm() {
	printf '%s %s %s\n' "$1" "$2" "$3" >>"$SVC_LOG"
}
zxfer_stopsvcs <<'INNER'
svc:/network/nfs/server svc:/network/ssh
INNER
printf 'restart=%s\n' "$g_zxfer_services_to_restart"
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "zxfer_stopsvcs should disable each requested service with -st." \
		"disable -st svc:/network/nfs/server
disable -st svc:/network/ssh" "$(cat "$log")"
	assertContains "Disabled services should be tracked for zxfer_relaunch." \
		"$output" "restart= svc:/network/nfs/server svc:/network/ssh"
	assertContains "Disabling services should mark zxfer_relaunch as required." \
		"$output" "need=1"
}

test_stopsvcs_returns_when_no_services_are_provided() {
	log="$TEST_TMPDIR/stopsvcs_empty.log"
	: >"$log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_services_need_relaunch=0
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
zxfer_stopsvcs <<'INNER'
INNER
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "Empty service lists should not invoke svcadm." "" "$(cat "$log")"
	assertContains "Empty service lists should leave zxfer_relaunch tracking disabled." "$output" "need=0"
}

test_stopsvcs_ignores_whitespace_only_service_input() {
	log="$TEST_TMPDIR/stopsvcs_whitespace.log"
	: >"$log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_services_need_relaunch=0
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
zxfer_stopsvcs <<'INNER'

INNER
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "Whitespace-only service lists should not invoke svcadm." "" "$(cat "$log")"
	assertContains "Whitespace-only service lists should leave zxfer_relaunch tracking disabled." "$output" "need=0"
}

test_stopsvcs_relaunches_and_errors_when_disable_fails() {
	set +e
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
trap - EXIT INT TERM HUP QUIT
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
zxfer_relaunch() {
	printf 'zxfer_relaunch\n'
}
zxfer_throw_error() {
	printf '%s\n' "$1"
	exit 1
}
svcadm() {
	return 1
}
zxfer_stopsvcs <<'INNER'
svc:/network/nfs/server
INNER
EOF
	)
	status=$?

	assertEquals "Service-disable failures should abort zxfer_stopsvcs." 1 "$status"
	assertContains "zxfer_stopsvcs should zxfer_relaunch services before failing." "$output" "zxfer_relaunch"
	assertContains "zxfer_stopsvcs failures should identify the offending service." \
		"$output" "Could not disable service svc:/network/nfs/server."
}

test_stopsvcs_normalizes_multiline_service_input_in_current_shell() {
	log="$TEST_TMPDIR/stopsvcs_current.log"
	: >"$log"
	g_zxfer_services_to_restart=""
	g_services_need_relaunch=0
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	svcadm() {
		printf '%s %s %s\n' "$1" "$2" "$3" >>"$log"
	}

	zxfer_stopsvcs <<'INNER'
svc:/network/nfs/server
svc:/network/ssh    svc:/system/test
INNER

	unset -f svcadm

	assertEquals "Current-shell service handling should normalize multiline input into one disable per service." \
		"disable -st svc:/network/nfs/server
disable -st svc:/network/ssh
disable -st svc:/system/test" "$(cat "$log")"
	assertEquals "Current-shell service handling should track every disabled service for zxfer_relaunch." \
		" svc:/network/nfs/server svc:/network/ssh svc:/system/test" "$g_zxfer_services_to_restart"
	assertEquals "Disabling services should still mark zxfer_relaunch as required." "1" "$g_services_need_relaunch"
}

test_relaunch_enables_services_and_clears_need_flag() {
	log="$TEST_TMPDIR/relaunch_actions.log"
	output=$(
		(
			SVC_LOG="$log"
			svcadm() {
				printf '%s %s\n' "$1" "$2" >>"$SVC_LOG"
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server svc:/network/ssh"
			g_services_need_relaunch=1
			zxfer_relaunch
			printf 'need=%s\n' "$g_services_need_relaunch"
		)
	)

	assertEquals "zxfer_relaunch should enable each previously disabled service." \
		"enable svc:/network/nfs/server
enable svc:/network/ssh" "$(cat "$log")"
	assertContains "Successful zxfer_relaunch should clear the zxfer_relaunch-needed flag." "$output" "need=0"
}

test_relaunch_returns_success_when_no_services_are_pending_in_current_shell() {
	g_zxfer_services_to_restart=""
	g_services_need_relaunch=1
	g_services_relaunch_in_progress=1

	zxfer_relaunch

	assertEquals "Empty zxfer_relaunch queues should clear the zxfer_relaunch-needed flag." \
		"0" "$g_services_need_relaunch"
	assertEquals "Empty zxfer_relaunch queues should clear the in-progress guard." \
		"0" "$g_services_relaunch_in_progress"
}

test_relaunch_throws_when_service_enable_fails() {
	set +e
	output=$(
		(
			svcadm() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server"
			g_services_need_relaunch=1
			zxfer_relaunch
		)
	)
	status=$?

	assertEquals "zxfer_relaunch should abort when a service cannot be re-enabled." 1 "$status"
	assertContains "zxfer_relaunch failures should identify the service that failed to start." \
		"$output" "Couldn't re-enable service svc:/network/nfs/server."
}

test_relaunch_continues_after_failures_and_keeps_only_failed_services_pending() {
	log="$TEST_TMPDIR/relaunch_partial_failure.log"

	set +e
	output=$(
		(
			SVC_LOG="$log"
			svcadm() {
				printf '%s %s\n' "$1" "$2" >>"$SVC_LOG"
				if [ "$2" = "svc:/network/ssh" ]; then
					return 1
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				printf 'need=%s\n' "$g_services_need_relaunch"
				printf 'pending=%s\n' "$g_zxfer_services_to_restart"
				printf 'guard=%s\n' "$g_services_relaunch_in_progress"
				exit 1
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server svc:/network/ssh svc:/system/test"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			zxfer_relaunch
		)
	)
	status=$?

	assertEquals "zxfer_relaunch should still fail when any service cannot be re-enabled." 1 "$status"
	assertEquals "zxfer_relaunch should still attempt every queued service even after one enable fails." \
		"enable svc:/network/nfs/server
enable svc:/network/ssh
enable svc:/system/test" "$(cat "$log")"
	assertContains "Partial zxfer_relaunch failures should keep the zxfer_relaunch-needed flag asserted." \
		"$output" "need=1"
	assertContains "Partial zxfer_relaunch failures should keep only failed services queued for later recovery." \
		"$output" "pending=svc:/network/ssh"
	assertContains "Partial zxfer_relaunch failures should leave the in-progress guard asserted until exit cleanup finishes." \
		"$output" "guard=1"
}

test_relaunch_reports_all_failed_services() {
	set +e
	output=$(
		(
			svcadm() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server svc:/network/ssh"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			zxfer_relaunch
		)
	)
	status=$?

	assertEquals "zxfer_relaunch should fail when multiple services cannot be re-enabled." 1 "$status"
	assertContains "Multi-service zxfer_relaunch failures should mention every failed service." \
		"$output" "Couldn't re-enable services: svc:/network/nfs/server svc:/network/ssh."
}

test_relaunch_dry_run_previews_enable_commands_without_executing() {
	log="$TEST_TMPDIR/relaunch_dry_run.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=1
g_option_v_verbose=1
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_zxfer_services_to_restart=" svc:/network/nfs/server svc:/network/ssh"
g_services_need_relaunch=1
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
zxfer_relaunch
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	l_relaunch_log=""
	if [ -f "$log" ]; then
		l_relaunch_log=$(cat "$log")
	fi
	assertEquals "Dry-run zxfer_relaunch should not execute svcadm enable." "" "$l_relaunch_log"
	assertContains "Dry-run zxfer_relaunch should preview the first enable command." \
		"$output" "Dry run: 'svcadm' 'enable' 'svc:/network/nfs/server'"
	assertContains "Dry-run zxfer_relaunch should preview every queued enable command." \
		"$output" "Dry run: 'svcadm' 'enable' 'svc:/network/ssh'"
	assertContains "Dry-run zxfer_relaunch should still clear the zxfer_relaunch-needed flag." \
		"$output" "need=0"
}

test_relaunch_dry_run_previews_enable_commands_in_current_shell() {
	log="$TEST_TMPDIR/relaunch_dry_run_current_shell.log"
	: >"$log"

	zxfer_echov() {
		printf '%s\n' "$*" >>"$log"
	}
	svcadm() {
		printf '%s\n' "$*" >>"$log"
	}
	g_option_n_dryrun=1
	g_zxfer_services_to_restart=" svc:/network/nfs/server"
	g_services_need_relaunch=1

	zxfer_relaunch

	# Restore the shared verbose helper so later tests are not affected by the stub.
	zxfer_echov() {
		if [ "$g_option_v_verbose" -eq 1 ]; then
			echo "$@"
		fi
	}
	unset -f svcadm

	assertEquals "Current-shell dry-run zxfer_relaunch should preview enable commands without executing svcadm." \
		"Restarting service svc:/network/nfs/server
Dry run: 'svcadm' 'enable' 'svc:/network/nfs/server'" "$(cat "$log")"
}

test_check_snapshot_rejects_snapshot_sources() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_check_snapshot "tank/src@snap1"
		)
	)
	status=$?

	assertEquals "Snapshot-source validation should abort when the requested source is already a snapshot." \
		1 "$status"
	assertContains "Snapshot-source validation should explain why snapshot sources are rejected." \
		"$output" "Snapshots are not allowed as a source."
}

test_newsnap_uses_recursive_snapshot_flag() {
	log="$TEST_TMPDIR/zxfer_newsnap.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive="tank/src"
g_zxfer_new_snapshot_name="zxfer_unit"
g_LZFS="mock_zfs_tool"
zxfer_run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
zxfer_newsnap "tank/src@old"
EOF
	)
	: "$output"

	assertEquals "Recursive snapshots should use the -r flag and strip the old snapshot suffix." \
		"snapshot -r tank/src@zxfer_unit" "$(cat "$log")"
}

test_newsnap_uses_nonrecursive_snapshot_without_r_flag() {
	log="$TEST_TMPDIR/newsnap_single.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive=""
g_zxfer_new_snapshot_name="zxfer_single"
g_LZFS="mock_zfs_tool"
zxfer_run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
zxfer_newsnap "tank/src@old"
EOF
	)
	: "$output"

	assertEquals "Non-recursive snapshots should omit the -r flag." \
		"snapshot tank/src@zxfer_single" "$(cat "$log")"
}

test_newsnap_builds_recursive_command_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_recursive.log"
	g_option_R_recursive="tank/src"
	g_zxfer_new_snapshot_name="zxfer_current"
	g_LZFS="mock_zfs_tool"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	zxfer_newsnap "tank/src@old"

	unset -f zxfer_run_source_zfs_cmd

	assertEquals "Current-shell recursive snapshot generation should include the -r flag." \
		"snapshot -r tank/src@zxfer_current" "$(cat "$log")"
}

test_newsnap_builds_nonrecursive_command_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_single.log"
	g_option_R_recursive=""
	g_zxfer_new_snapshot_name="zxfer_current_single"
	g_LZFS="mock_zfs_tool"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	zxfer_newsnap "tank/src@old"

	unset -f zxfer_run_source_zfs_cmd

	assertEquals "Current-shell non-recursive snapshot generation should omit the -r flag." \
		"snapshot tank/src@zxfer_current_single" "$(cat "$log")"
}

test_newsnap_dry_run_previews_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_dry_run.log"
	: >"$log"
	g_option_n_dryrun=1
	g_option_R_recursive="tank/src"
	g_zxfer_new_snapshot_name="zxfer_current_dry_run"
	g_LZFS="mock_zfs_tool"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	zxfer_echov() {
		printf '%s\n' "$*" >>"$log"
	}
	zxfer_run_source_zfs_cmd() {
		printf 'executed %s\n' "$*" >>"$log"
	}

	zxfer_newsnap "tank/src@old"

	zxfer_echov() {
		if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
			echo "$@"
		fi
	}
	unset -f zxfer_run_source_zfs_cmd

	assertContains "Current-shell dry-run snapshots should render the dry-run command preview." \
		"$(cat "$log")" "Dry run: 'mock_zfs_tool' 'snapshot' '-r' 'tank/src@zxfer_current_dry_run'"
	assertNotContains "Current-shell dry-run snapshots should not execute the source zfs command." \
		"$(cat "$log")" "executed "
}

test_newsnap_dry_run_previews_without_executing() {
	log="$TEST_TMPDIR/newsnap_dry_run.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=1
g_option_v_verbose=1
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive="tank/src"
g_zxfer_new_snapshot_name="zxfer_dry_run"
g_LZFS="mock_zfs_tool"
zxfer_run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
zxfer_newsnap "tank/src@old"
EOF
	)

	l_snapshot_log=""
	if [ -f "$log" ]; then
		l_snapshot_log=$(cat "$log")
	fi
	assertEquals "Dry-run snapshots should not execute the source zfs command." "" "$l_snapshot_log"
	assertContains "Dry-run snapshots should render the snapshot command." \
		"$output" "Dry run: 'mock_zfs_tool' 'snapshot' '-r' 'tank/src@zxfer_dry_run'"
}

test_rollback_destination_to_last_common_snapshot_rolls_back_and_clears_flag() {
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_actual_dest="backup/target/src"
	g_last_common_snap="tank/src@snap1"
	log="$TEST_TMPDIR/rollback.log"
	: >"$log"

	output=$(
		(
			ROLLBACK_LOG="$log"
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s %s %s\n' "$1" "$2" "$3" >>"$ROLLBACK_LOG"
				return 0
			}
			zxfer_invalidate_destination_snapshot_record_cache() {
				printf '%s\n' "invalidated=snapshots" >>"$ROLLBACK_LOG"
			}
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'flag=%s\n' "$g_did_delete_dest_snapshots"
		)
	)

	assertEquals "Rollback should target the destination snapshot matching the last common snapshot." \
		"rollback -r backup/target/src@snap1
invalidated=snapshots" "$(cat "$log")"
	assertContains "Successful rollback should clear the delete marker." "$output" "flag=0"
}

test_rollback_destination_to_last_common_snapshot_skips_when_not_needed() {
	log="$TEST_TMPDIR/rollback_skip.log"
	: >"$log"

	(
		ROLLBACK_LOG="$log"
		g_option_F_force_rollback=""
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=0
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=0
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap=""
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	assertEquals "Rollback should no-op when -F is absent, deletions did not occur, deleted snapshots were not newer than the last common snapshot, destination is absent, or no common snapshot exists." \
		"" "$(cat "$log")"
}

test_zxfer_reconcile_live_destination_snapshot_state_shortcuts_empty_source_and_requeues_when_live_empty() {
	output=$(
		(
			g_actual_dest="backup/target/src"
			g_last_common_snap=""
			g_src_snapshot_transfer_list=""
			g_dest_has_snapshots=1

			zxfer_reconcile_live_destination_snapshot_state
			printf 'no_source_status=%s\n' "$?"

			g_last_common_snap="tank/src@snap1"
			g_src_snapshot_transfer_list="tank/src@snap2"
			g_dest_has_snapshots=1
			zxfer_exists_destination() {
				printf '%s\n' "1"
			}
			zxfer_get_live_destination_snapshots() {
				return 0
			}
			zxfer_reconcile_live_destination_snapshot_state
			printf 'empty_live_status=%s\n' "$?"
			printf 'dest_has_snapshots=%s\n' "${g_dest_has_snapshots:-1}"
			printf 'last=<%s>\n' "$g_last_common_snap"
			printf 'transfer=<%s>\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertContains "Live destination-state reconciliation should return success when there are no source records to reconcile." \
		"$output" "no_source_status=0"
	assertContains "Live destination-state reconciliation should return success when the destination has no live snapshots." \
		"$output" "empty_live_status=0"
	assertContains "Live destination-state reconciliation should clear the destination snapshot marker when no live snapshots remain." \
		"$output" "dest_has_snapshots=0"
	assertContains "Live destination-state reconciliation should clear a stale common snapshot when no live snapshots remain." \
		"$output" "last=<>"
	assertContains "Live destination-state reconciliation should requeue the cached common snapshot with the remaining source tail when no live snapshots remain." \
		"$output" "transfer=<tank/src@snap1
tank/src@snap2>"
}

test_rollback_destination_to_last_common_snapshot_reports_probe_failures() {
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_actual_dest="backup/target/src"
	g_last_common_snap="tank/src@snap1"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/target/src] exists: ssh failure"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_invalidate_destination_snapshot_record_cache() {
				printf '%s\n' "invalidated"
			}
			zxfer_rollback_destination_to_last_common_snapshot
		)
	)
	status=$?

	assertEquals "Rollback should fail closed when destination existence checks fail." 1 "$status"
	assertContains "Rollback should surface the destination probe failure." \
		"$output" "Failed to determine whether destination dataset [backup/target/src] exists: ssh failure"
}

test_rollback_destination_to_last_common_snapshot_reports_rollback_failures() {
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_actual_dest="backup/target/src"
	g_last_common_snap="tank/src@snap1"

	set +e
	output=$(
		(
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
			zxfer_rollback_destination_to_last_common_snapshot
		)
	)
	status=$?

	assertEquals "Rollback failures should abort instead of silently continuing." 1 "$status"
	assertContains "Rollback failures should identify the destination snapshot that could not be rolled back." \
		"$output" "Failed to roll back destination [backup/target/src] to backup/target/src@snap1 after deleting snapshots."
	assertNotContains "Rollback failures should not invalidate snapshot caches as if the mutation succeeded." \
		"$output" "invalidated"
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src/child" ]; then
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
	assertContains "A successful live child recheck should still promote the matching snapshot to the last common anchor." \
		"$output" "last=tank/src/child@base	111"
	assertContains "A successful live child recheck should clear the remaining transfer list once the destination already has the seed snapshot." \
		"$output" "remaining="
	assertContains "A successful live child recheck should still mark the destination as snapshotted." \
		"$output" "dest_has=1"
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
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

test_validate_zfs_mode_preconditions_requires_m_for_services() {
	g_option_c_services="svc:/network/nfs/server"
	g_option_m_migrate=0
	g_initial_source="tank/src"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_validate_zfs_mode_preconditions
	) >/dev/null 2>&1
	status=$?

	assertEquals "Service-management requests should require -m." "1" "$status"
}

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
		zxfer_ensure_remote_backup_dir() {
			printf '%s %s\n' "$1" "$2" >>"$REMOTE_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host="target.example"
		g_backup_storage_root="$TEST_TMPDIR/remote_backup"
		zxfer_check_backup_storage_dir_if_needed
	)

	assertEquals "Local backup checks should validate the local backup root." \
		"$TEST_TMPDIR/local_backup" "$(cat "$local_log")"
	assertEquals "Remote backup checks should validate the remote backup root and host." \
		"$TEST_TMPDIR/remote_backup target.example" "$(cat "$remote_log")"
}

test_check_backup_storage_dir_if_needed_refreshes_backup_root_from_environment() {
	output=$(
		(
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/stale_backup"
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/refreshed backup"
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)

	assertContains "Backup-dir preflight should refresh the root from ZXFER_BACKUP_DIR before rendering dry-run output." \
		"$output" "'$TEST_TMPDIR/refreshed backup'"
	assertNotContains "Backup-dir preflight should not keep previewing a stale cached backup root after ZXFER_BACKUP_DIR changes." \
		"$output" "'$TEST_TMPDIR/stale_backup'"
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
			zxfer_ensure_remote_backup_dir() {
				printf '%s %s\n' "$1" "$2" >>"$REMOTE_LOG"
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/local backup"
			zxfer_check_backup_storage_dir_if_needed
			g_zxfer_dependency_path="/stale/secure/path"
			ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
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
	assertContains "Dry-run remote backup preflight should refresh the secure-PATH wrapper from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$output" "/fresh/secure/path:/usr/bin"
	assertNotContains "Dry-run remote backup preflight should not keep previewing a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$output" "/stale/secure/path"
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

test_check_backup_storage_dir_if_needed_rejects_relative_backup_dir_override() {
	zxfer_test_capture_subshell "
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_backup_storage_root='$TEST_TMPDIR/stale_backup'
		ZXFER_BACKUP_DIR='relative-backups'
		zxfer_check_backup_storage_dir_if_needed
	"

	assertEquals "Backup-dir preflight should fail closed when ZXFER_BACKUP_DIR is relative." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative backup-root preflight failures should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_BACKUP_DIR must be an absolute path"
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

test_initialize_replication_context_skips_live_validation_in_dry_run() {
	log="$TEST_TMPDIR/init_context_dry_run.log"
	: >"$log"
	g_initial_source="tank/src"
	g_option_R_recursive=""

	output=$(
		(
			CTX_LOG="$log"
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_get_backup_properties() {
				printf 'backup\n' >>"$CTX_LOG"
			}
			zxfer_get_zfs_list() {
				printf 'list\n' >>"$CTX_LOG"
			}
			zxfer_calculate_unsupported_properties() {
				printf 'unsupported\n' >>"$CTX_LOG"
			}
			g_option_n_dryrun=1
			g_option_e_restore_property_mode=1
			g_option_U_skip_unsupported_properties=1
			g_recursive_source_dataset_list="stale-source stale-child"
			g_recursive_destination_extra_dataset_list="stale-extra"
			g_recursive_dest_list="stale-dest"
			g_lzfs_list_hr_snap="stale-source@snap"
			g_rzfs_list_hr_snap="stale-dest@snap"
			g_source_snapshot_list_cmd="stale-command"
			g_destination_existence_cache_root="stale-root"
			g_zxfer_source_snapshot_record_index_ready=1
			zxfer_initialize_replication_context
			{
				printf 'recursive=%s\n' "$g_recursive_source_list"
				printf 'datasets=%s\n' "$g_recursive_source_dataset_list"
				printf 'extras=%s\n' "${g_recursive_destination_extra_dataset_list:-}"
				printf 'dests=%s\n' "${g_recursive_dest_list:-}"
				printf 'source_snaps=%s\n' "${g_lzfs_list_hr_snap:-}"
				printf 'dest_snaps=%s\n' "${g_rzfs_list_hr_snap:-}"
				printf 'source_cmd=%s\n' "${g_source_snapshot_list_cmd:-}"
				printf 'dest_cache_root=%s\n' "${g_destination_existence_cache_root:-}"
				printf 'source_index_ready=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-}"
			} >>"$CTX_LOG"
		)
	)

	assertEquals "Dry-run initialization should not perform live backup restore, discovery, or unsupported-property scans." \
		"recursive=tank/src
datasets=tank/src
extras=
dests=
source_snaps=
dest_snaps=
source_cmd=
dest_cache_root=
source_index_ready=0" "$(cat "$log")"
	assertContains "Dry-run initialization should explain that the live validation stages are skipped." \
		"$output" "Dry run: skipping live backup-restore validation, snapshot discovery, and unsupported-property detection."
}

test_run_zfs_mode_calls_steps_in_order_and_relaunches_for_migration() {
	log="$TEST_TMPDIR/zxfer_run_zfs_mode.log"
	: >"$log"

	(
		RUN_LOG="$log"
		zxfer_resolve_initial_source_from_options() { printf 'resolve\n' >>"$RUN_LOG"; }
		zxfer_normalize_source_destination_paths() { printf 'normalize\n' >>"$RUN_LOG"; }
		zxfer_validate_zfs_mode_preconditions() { printf 'validate\n' >>"$RUN_LOG"; }
		zxfer_check_backup_storage_dir_if_needed() { printf 'backupdir\n' >>"$RUN_LOG"; }
		zxfer_initialize_replication_context() { printf 'context\n' >>"$RUN_LOG"; }
		zxfer_maybe_capture_preflight_snapshot() { printf 'snapshot\n' >>"$RUN_LOG"; }
		zxfer_prepare_migration_services() { printf 'prepare\n' >>"$RUN_LOG"; }
		zxfer_perform_grandfather_protection_checks() { printf 'grandfather\n' >>"$RUN_LOG"; }
		zxfer_copy_filesystems() { printf 'copy\n' >>"$RUN_LOG"; }
		zxfer_relaunch() { printf 'zxfer_relaunch\n' >>"$RUN_LOG"; }
		g_option_m_migrate=1
		zxfer_run_zfs_mode
	)

	assertEquals "zxfer_run_zfs_mode should execute its major phases in the expected order." \
		"resolve
normalize
validate
backupdir
context
snapshot
prepare
grandfather
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
			zxfer_resolve_initial_source_from_options() {
				printf 'resolve\n' >>"$RUN_LOG"
				g_initial_source="tank/src"
			}
			zxfer_normalize_source_destination_paths() { printf 'normalize\n' >>"$RUN_LOG"; }
			zxfer_validate_zfs_mode_preconditions() { printf 'validate\n' >>"$RUN_LOG"; }
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
		"resolve
normalize
validate
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
			g_recursive_source_list="stale/src stale/src/child"
			g_recursive_source_dataset_list="stale/src stale/src/child"
			g_recursive_destination_extra_dataset_list="stale-extra"
			g_recursive_dest_list="stale-dest"
			g_lzfs_list_hr_snap="stale-source@snap"
			g_rzfs_list_hr_snap="stale-dest@snap"
			g_source_snapshot_list_cmd="stale-command"
			g_destination_existence_cache_root="stale-root"
			g_zxfer_source_snapshot_record_index_ready=1
			zxfer_preview_zfs_mode_dry_run
			printf 'after_list=<%s>\n' "$g_recursive_source_list"
			printf 'after_datasets=<%s>\n' "$g_recursive_source_dataset_list"
			printf 'after_extras=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'after_dest=<%s>\n' "${g_recursive_dest_list:-}"
			printf 'after_source_snaps=<%s>\n' "${g_lzfs_list_hr_snap:-}"
			printf 'after_dest_snaps=<%s>\n' "${g_rzfs_list_hr_snap:-}"
			printf 'after_source_cmd=<%s>\n' "${g_source_snapshot_list_cmd:-}"
			printf 'after_dest_cache_root=<%s>\n' "${g_destination_existence_cache_root:-}"
			printf 'after_source_index_ready=<%s>\n' "${g_zxfer_source_snapshot_record_index_ready:-}"
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
	assertContains "Strict dry-run preview should clear stale source snapshot caches." \
		"$output" "after_source_snaps=<>"
	assertContains "Strict dry-run preview should clear stale destination snapshot caches." \
		"$output" "after_dest_snaps=<>"
	assertContains "Strict dry-run preview should clear the stale rendered source snapshot command." \
		"$output" "after_source_cmd=<>"
	assertContains "Strict dry-run preview should clear the destination existence cache root." \
		"$output" "after_dest_cache_root=<>"
	assertContains "Strict dry-run preview should reset snapshot-record index readiness." \
		"$output" "after_source_index_ready=<0>"
}

test_zxfer_preview_zfs_mode_dry_run_emits_restore_and_unsupported_property_notices() {
	output=$(
		(
			g_option_e_restore_property_mode=1
			g_option_U_skip_unsupported_properties=1
			zxfer_seed_dry_run_preview_source_list() {
				:
			}
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

test_perform_grandfather_protection_checks_skips_when_flag_unset() {
	g_option_g_grandfather_protection=""
	g_recursive_source_list="tank/src tank/src/child"
	log="$TEST_TMPDIR/grandfather_skip.log"
	rm -f "$log"

	ZXFER_TEST_ROOT=$ZXFER_ROOT GRANDFATHER_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_g_grandfather_protection=""
g_recursive_source_list="tank/src tank/src/child"
zxfer_set_actual_dest() { echo "set $1" >>"$GRANDFATHER_LOG"; }
zxfer_inspect_delete_snap() { echo "inspect $1 $2" >>"$GRANDFATHER_LOG"; }
zxfer_perform_grandfather_protection_checks
EOF

	assertFalse "Grandfather check should no-op when flag is unset." "[ -s \"$log\" ]"
}

test_perform_grandfather_protection_checks_calls_helpers_for_each_dataset() {
	g_option_g_grandfather_protection="enabled"
	g_recursive_source_list="tank/src tank/src/child"
	log="$TEST_TMPDIR/grandfather_calls.log"
	rm -f "$log"

	ZXFER_TEST_ROOT=$ZXFER_ROOT GRANDFATHER_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_g_grandfather_protection="enabled"
g_recursive_source_list="tank/src tank/src/child"
zxfer_set_actual_dest() { printf 'set %s\n' "$1" >>"$GRANDFATHER_LOG"; }
zxfer_inspect_delete_snap() { printf 'inspect %s %s\n' "$1" "$2" >>"$GRANDFATHER_LOG"; }
zxfer_perform_grandfather_protection_checks
EOF

	expected="set tank/src
inspect 0 tank/src
set tank/src/child
inspect 0 tank/src/child"
	assertEquals "Grandfather protection should inspect every dataset slated for replication." \
		"$expected" "$(cat "$log")"
}

test_perform_grandfather_protection_checks_runs_in_current_shell() {
	g_option_g_grandfather_protection="enabled"
	g_recursive_source_list="tank/src tank/src/child"
	g_initial_source="tank/src"
	g_destination="backup/target"
	log="$TEST_TMPDIR/grandfather_current.log"
	: >"$log"
	zxfer_inspect_delete_snap() {
		printf 'inspect %s %s %s\n' "$1" "$2" "$g_actual_dest" >>"$log"
	}

	zxfer_perform_grandfather_protection_checks

	unset -f zxfer_inspect_delete_snap

	assertEquals "Current-shell grandfather checks should compute each destination before inspection." \
		"inspect 0 tank/src backup/target/src
inspect 0 tank/src/child backup/target/src/child" "$(cat "$log")"
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
		zxfer_prepare_ssh_control_sockets_for_active_hosts() {
			printf 'unexpected-ssh-setup\n' >>"$COPY_FS_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_copy_filesystems
	)

	assertEquals "Clean recursive no-op runs should bypass iteration staging and deferred SSH socket setup." \
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

test_copy_filesystems_defers_remote_control_socket_setup_until_work_exists() {
	log="$TEST_TMPDIR/deferred_remote_socket_setup.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		g_option_R_recursive="tank/src"
		g_option_O_origin_host="origin.example"
		g_initial_source="tank/src"
		g_recursive_source_list="tank/src"
		g_recursive_source_dataset_list="tank/src"
		zxfer_prepare_ssh_control_sockets_for_active_hosts() {
			printf 'prepare-ssh\n' >>"$COPY_FS_LOG"
		}
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
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$COPY_FS_LOG"
		}
		zxfer_copy_filesystems
	)

	assertEquals "Remote SSH control sockets should be prepared only after the iteration list proves there is work." \
		"prepare-ssh
set tank/src
inspect 0 tank/src
copy tank/src
wait final sync" "$(cat "$log")"
}

test_copy_snapshots_seeds_existing_destination_into_snapshot() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=0
	g_dest_created_by_zxfer=0
	g_src_snapshot_transfer_list="tank/src@seed1 tank/src@seed2"
	log="$TEST_TMPDIR/seed_existing.log"
	rm -f "$log"

	(
		SEED_LOG="$log"
		zxfer_reconcile_live_destination_snapshot_state() { :; }
		zxfer_rollback_destination_to_last_common_snapshot() { :; }
		zxfer_exists_destination() { printf '1\n'; }
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

test_copy_filesystems_flushes_backup_metadata_after_snapshot_copy() {
	g_option_P_transfer_property=1
	g_option_k_backup_property_mode=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	root_backup_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	log="$TEST_TMPDIR/copy_filesystems_backup_flush.log"
	rm -f "$log"

	(
		FLUSH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$FLUSH_LOG"
		}
		zxfer_transfer_properties() {
			g_backup_file_contents=$root_backup_row
			printf 'props %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$FLUSH_LOG"
		}
		zxfer_flush_captured_backup_metadata_if_live() {
			printf 'flush %s\n' "$g_backup_file_contents" >>"$FLUSH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$FLUSH_LOG"
		}

		zxfer_copy_filesystems
	)

	assertEquals "Live backup metadata flushes should happen after snapshot copy orchestration succeeds, not during the property-transfer helper." \
		"set tank/src
inspect 0 tank/src
props tank/src
copy backup/target/src
flush $root_backup_row
wait final sync" "$(cat "$log")"
}

test_copy_filesystems_does_not_flush_backup_metadata_when_snapshot_copy_fails() {
	g_option_P_transfer_property=1
	g_option_k_backup_property_mode=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	root_backup_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	log="$TEST_TMPDIR/copy_filesystems_backup_flush_failure.log"
	rm -f "$log"

	set +e
	output=$(
		(
			FLUSH_LOG="$log"
			zxfer_set_actual_dest() {
				g_actual_dest="backup/target/src"
				printf 'set %s\n' "$1" >>"$FLUSH_LOG"
			}
			zxfer_inspect_delete_snap() {
				printf 'inspect %s %s\n' "$1" "$2" >>"$FLUSH_LOG"
			}
			zxfer_transfer_properties() {
				g_backup_file_contents=$root_backup_row
				printf 'props %s\n' "$1" >>"$FLUSH_LOG"
			}
			zxfer_copy_snapshots() {
				printf 'copy %s\n' "$g_actual_dest" >>"$FLUSH_LOG"
				zxfer_throw_error "copy failed"
			}
			zxfer_flush_captured_backup_metadata_if_live() {
				printf 'unexpected flush %s\n' "$g_backup_file_contents" >>"$FLUSH_LOG"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Snapshot-copy failures should still abort the property-enabled copy loop." 1 "$status"
	assertContains "Snapshot-copy failures should preserve the copy failure text." \
		"$output" "copy failed"
	assertNotContains "Backup metadata should not flush when snapshot copy fails before the dataset completes." \
		"$(cat "$log")" "unexpected flush"
}

test_copy_filesystems_defers_backup_metadata_flush_until_final_sync_when_send_jobs_are_pending() {
	g_option_P_transfer_property=1
	g_option_k_backup_property_mode=1
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	root_backup_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	log="$TEST_TMPDIR/copy_filesystems_backup_flush_deferred.log"
	rm -f "$log"

	(
		FLUSH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$FLUSH_LOG"
		}
		zxfer_transfer_properties() {
			g_backup_file_contents=$root_backup_row
			printf 'props %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_copy_snapshots() {
			g_zfs_send_job_pids="12345"
			printf 'copy %s\n' "$g_actual_dest" >>"$FLUSH_LOG"
		}
		zxfer_flush_captured_backup_metadata_if_live() {
			printf 'flush %s\n' "$g_backup_file_contents" >>"$FLUSH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$FLUSH_LOG"
			g_zfs_send_job_pids=""
		}

		zxfer_copy_filesystems
	)

	assertEquals "When background send jobs are still pending, backup metadata flush should wait until final sync confirms they have finished." \
		"set tank/src
inspect 0 tank/src
props tank/src
copy backup/target/src
wait final sync
flush $root_backup_row" "$(cat "$log")"
}

test_copy_filesystems_defers_backup_metadata_flush_until_post_seed_reconcile_finishes() {
	g_option_P_transfer_property=1
	g_option_k_backup_property_mode=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	root_backup_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	log="$TEST_TMPDIR/copy_filesystems_backup_flush_post_seed.log"
	rm -f "$log"

	(
		FLUSH_LOG="$log"
		zxfer_set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$FLUSH_LOG"
		}
		zxfer_transfer_properties() {
			g_zxfer_source_pvs_raw="compression=lz4=local"
			g_backup_file_contents=$root_backup_row
			printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$FLUSH_LOG"
		}
		zxfer_copy_snapshots() {
			g_dest_seed_requires_property_reconcile=1
			printf 'copy %s\n' "$g_actual_dest" >>"$FLUSH_LOG"
		}
		zxfer_note_destination_dataset_exists() {
			printf 'note %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset\n' >>"$FLUSH_LOG"
		}
		zxfer_flush_captured_backup_metadata_if_live() {
			printf 'flush %s\n' "$g_backup_file_contents" >>"$FLUSH_LOG"
		}

		zxfer_copy_filesystems
	)

	assertEquals "Seeded destinations should flush buffered backup metadata only after the deferred post-seed property reconcile succeeds." \
		"set tank/src
inspect 0 tank/src
props tank/src skip=0
copy backup/target/src
note backup/target/src
wait final sync
reset
set tank/src
props tank/src skip=1
flush $root_backup_row" "$(cat "$log")"
}

test_copy_filesystems_does_not_flush_backup_metadata_when_post_seed_reconcile_fails() {
	g_option_P_transfer_property=1
	g_option_k_backup_property_mode=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	root_backup_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	log="$TEST_TMPDIR/copy_filesystems_backup_flush_post_seed_failure.log"
	rm -f "$log"

	set +e
	output=$(
		(
			FLUSH_LOG="$log"
			zxfer_set_actual_dest() {
				g_actual_dest="backup/target/src"
				printf 'set %s\n' "$1" >>"$FLUSH_LOG"
			}
			zxfer_inspect_delete_snap() {
				printf 'inspect %s %s\n' "$1" "$2" >>"$FLUSH_LOG"
			}
			zxfer_transfer_properties() {
				g_backup_file_contents=$root_backup_row
				printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$FLUSH_LOG"
				if [ "${2:-0}" -eq 1 ]; then
					zxfer_throw_error "post-seed reconcile failed"
				fi
			}
			zxfer_copy_snapshots() {
				g_dest_seed_requires_property_reconcile=1
				printf 'copy %s\n' "$g_actual_dest" >>"$FLUSH_LOG"
			}
			zxfer_note_destination_dataset_exists() {
				printf 'note %s\n' "$1" >>"$FLUSH_LOG"
			}
			zxfer_wait_for_zfs_send_jobs() {
				printf 'wait %s\n' "$1" >>"$FLUSH_LOG"
			}
			zxfer_reset_destination_property_iteration_cache() {
				printf 'reset\n' >>"$FLUSH_LOG"
			}
			zxfer_flush_captured_backup_metadata_if_live() {
				printf 'unexpected flush %s\n' "$g_backup_file_contents" >>"$FLUSH_LOG"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			zxfer_copy_filesystems
		) 2>&1
	)
	status=$?

	assertEquals "Post-seed property-reconcile failures should still abort the copy loop." 1 "$status"
	assertContains "Post-seed reconcile failures should preserve the property error." \
		"$output" "post-seed reconcile failed"
	assertNotContains "Seeded destinations should not flush backup metadata before the deferred property reconcile succeeds." \
		"$(cat "$log")" "unexpected flush"
}

test_copy_filesystems_does_not_flush_seeded_rows_during_later_completed_datasets() {
	g_option_P_transfer_property=1
	g_option_k_backup_property_mode=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	root_backup_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	child_backup_row=$(zxfer_test_backup_metadata_row "child" "quota=1G=local")
	merged_backup_rows=$(printf '%s\n%s' "$child_backup_row" "$root_backup_row")
	log="$TEST_TMPDIR/copy_filesystems_backup_pending_mix.log"
	rm -f "$log"

	(
		FLUSH_LOG="$log"
		zxfer_set_actual_dest() {
			case "$1" in
			tank/src) g_actual_dest="backup/target/src" ;;
			tank/src/child) g_actual_dest="backup/target/src/child" ;;
			esac
			printf 'set %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$FLUSH_LOG"
		}
		zxfer_transfer_properties() {
			case "$1" in
			tank/src) g_zxfer_source_pvs_raw="compression=lz4=local" ;;
			tank/src/child) g_zxfer_source_pvs_raw="quota=1G=local" ;;
			esac
			zxfer_capture_backup_metadata_for_completed_transfer "$1" "$g_zxfer_source_pvs_raw" "${2:-0}"
			printf 'props %s skip=%s live=%s pending=%s\n' "$1" "${2:-0}" \
				"${g_backup_file_contents:-}" "${g_pending_backup_file_contents:-}" >>"$FLUSH_LOG"
		}
		zxfer_copy_snapshots() {
			case "$g_actual_dest" in
			backup/target/src) g_dest_seed_requires_property_reconcile=1 ;;
			backup/target/src/child) g_dest_seed_requires_property_reconcile=0 ;;
			esac
			printf 'copy %s\n' "$g_actual_dest" >>"$FLUSH_LOG"
		}
		zxfer_note_destination_dataset_exists() {
			printf 'note %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$FLUSH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset\n' >>"$FLUSH_LOG"
		}
		zxfer_flush_captured_backup_metadata_if_live() {
			printf 'flush live=%s pending=%s\n' "${g_backup_file_contents:-}" "${g_pending_backup_file_contents:-}" >>"$FLUSH_LOG"
		}

		zxfer_copy_filesystems
	)

	assertEquals "A later completed dataset should not flush an earlier seeded dataset's deferred backup row before post-seed reconcile finishes." \
		"set tank/src
inspect 0 tank/src
props tank/src skip=0 live=$root_backup_row pending=
copy backup/target/src
note backup/target/src
set tank/src/child
inspect 0 tank/src/child
props tank/src/child skip=0 live=$child_backup_row pending=$root_backup_row
copy backup/target/src/child
flush live=$child_backup_row pending=$root_backup_row
wait final sync
reset
set tank/src
props tank/src skip=1 live=$child_backup_row pending=$root_backup_row
flush live=$merged_backup_rows pending=" "$(cat "$log")"
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
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
			if [ "$1" = "tank/src/child" ] && [ "${2:-0}" -eq 0 ]; then
				g_dest_created_by_zxfer=1
			fi
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
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
props tank/src/child created=1 skip=1 dest_present=1"
	assertEquals "Created destinations should receive a second property reconciliation after the seed snapshot is received." \
		"$expected" "$(cat "$log")"
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
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
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
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
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

	# shellcheck source=src/zxfer_property_cache.sh
	. "$ZXFER_ROOT/src/zxfer_property_cache.sh"
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
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		zxfer_copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
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

test_copy_filesystems_resets_destination_property_cache_before_next_dataset_when_background_receives_are_active() {
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

	expected="set tank/src
inspect 0 tank/src
props tank/src
copy tank/src
set tank/src/child
reset-destination-cache
inspect 0 tank/src/child
props tank/src/child
copy tank/src/child
wait final sync"
	assertEquals "When background receives are still active, zxfer should clear destination-side property caches before processing the next dataset so later property reads do not reuse stale destination state." \
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
	g_test_base_readonly_properties="type,mountpoint,creation"

	zxfer_prepare_migration_services

	assertEquals "Live migration should drop mountpoint from the effective readonly-property list." \
		"type,creation" "$(zxfer_get_effective_readonly_properties)"
	assertEquals "Live migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$(zxfer_get_base_readonly_properties)"
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

test_run_zfs_mode_loop_keeps_backup_metadata_unique_across_iterations() {
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
			printf 'backup=%s\n' "$g_backup_file_contents"
		)
	)

	assertContains "Repeated -Y iterations should keep one v2 backup-metadata row per relative dataset path." \
		"$output" "backup=$(zxfer_test_backup_metadata_row "." "readonly=on=local")"
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

# shellcheck disable=SC1090
# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
