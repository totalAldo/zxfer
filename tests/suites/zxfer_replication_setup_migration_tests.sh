#!/bin/sh
# Source selection, migration-service, snapshot creation, and rollback behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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

	l_saved_ifs=$IFS
	IFS=:
	set -f
	zxfer_prepare_migration_services
	l_after_ifs=$IFS
	case $- in
	*f*) l_after_globbing=disabled ;;
	*) l_after_globbing=enabled ;;
	esac
	IFS=$l_saved_ifs
	set +f

	assertEquals "Services should be piped to zxfer_stopsvcs intact." \
		"svc:/network/iscsi_target svc:/network/nfs/server" "$(cat "$STUB_STOPSVCS_LOG")"
	assertEquals "All recursive datasets must be unmounted before migrating." \
		"unmount tank/src
unmount tank/src/child" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Migration must create a final snapshot for the initial source." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset lists should run exactly once." "1" "$STUB_ZFS_LIST_CALLS"
	assertEquals "Migration dataset iteration should preserve a caller-defined IFS." ":" "$l_after_ifs"
	assertEquals "Migration dataset iteration should preserve disabled globbing." "disabled" "$l_after_globbing"
	case "$(zxfer_get_effective_readonly_properties)" in
	*mountpoint*)
		fail "Readonly properties list should drop mountpoint during migration."
		;;
	esac
	assertEquals "Migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
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
			IFS=:
			set -f
			zxfer_prepare_migration_services
			{
				printf 'readonly=%s\n' "$(zxfer_get_effective_readonly_properties)"
				printf 'restart=%s\n' "$g_zxfer_services_to_restart"
				printf 'need=%s\n' "$g_services_need_relaunch"
				printf 'ifs=%s\n' "$IFS"
				printf 'flags=%s\n' "$-"
			} >"$state_log"
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
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
	assertContains "Dry-run migration should still track which services would need zxfer_relaunch later." \
		"$(cat "$state_log")" "restart= svc:/network/iscsi_target svc:/network/nfs/server"
	assertContains "Dry-run migration should still flag zxfer_relaunch as required." \
		"$(cat "$state_log")" "need=1"
	assertContains "Dry-run migration dataset iteration should preserve a caller-defined IFS." \
		"$(cat "$state_log")" "ifs=:"
	assertContains "Dry-run migration dataset iteration should preserve disabled globbing." \
		"$(sed -n 's/^flags=//p' "$state_log")" "f"
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
	ZXFER_BASE_READONLY_PROPERTIES="type,mountpoint,creation"

	zxfer_prepare_migration_services

	assertEquals "Dry-run migration should drop mountpoint from the effective readonly-property list." \
		"type,creation" "$(zxfer_get_effective_readonly_properties)"
	assertEquals "Dry-run migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
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
	# Reload the owner after setUp's orchestration stub replaced zxfer_stopsvcs.
	# shellcheck source=src/zxfer_migration_services.sh
	. "$ZXFER_ROOT/src/zxfer_migration_services.sh"
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

test_relaunch_preserves_smf_patterns_without_pathname_or_ifs_expansion() {
	work_dir="$TEST_TMPDIR/relaunch-pattern-cwd"
	log="$TEST_TMPDIR/relaunch-pattern.log"
	mkdir -p "$work_dir/svc:/site"
	: >"$work_dir/svc:/site/local-match"
	: >"$log"

	output=$(
		(
			cd "$work_dir" || exit 90
			SVC_LOG=$log
			svcadm() {
				printf '<%s>\n' "$2" >>"$SVC_LOG"
			}
			g_option_n_dryrun=0
			g_zxfer_services_to_restart='svc:/site/*'
			g_services_need_relaunch=1
			set +f
			zxfer_relaunch

			IFS=:
			set -f
			g_zxfer_services_to_restart='svc:/site/*'
			g_services_need_relaunch=1
			zxfer_relaunch
			shell_flags=$-
			if [ "${shell_flags#*f}" != "$shell_flags" ]; then
				glob_state=off
			else
				glob_state=on
			fi
			printf 'ifs=<%s> glob=%s\n' "$IFS" "$glob_state"
		)
	)

	assertEquals "SMF patterns must reach svcadm literally even when matching pathnames exist in the working directory." \
		"<svc:/site/*>
<svc:/site/*>" "$(cat "$log")"
	assertContains "Service relaunch should preserve a custom caller IFS and pre-existing noglob state." \
		"$output" "ifs=<:> glob=off"
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
	g_option_v_verbose=1
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
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'flag=%s\n' "$g_did_delete_dest_snapshots"
			printf 'generation=%s\n' "${g_zxfer_destination_mutation_generation:-0}"
		)
	)

	assertEquals "Rollback should target the destination snapshot matching the last common snapshot." \
		"rollback -r backup/target/src@snap1" "$(cat "$log")"
	assertContains "Successful rollback should clear the delete marker." "$output" "flag=0"
	assertContains "Successful rollback should bump the destination mutation generation so stale live views are refreshed." \
		"$output" "generation=1"
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
				printf 'generation=%s\n' "${g_zxfer_destination_mutation_generation:-0}"
				exit 1
			}
			zxfer_rollback_destination_to_last_common_snapshot
		)
	)
	status=$?

	assertEquals "Rollback failures should abort instead of silently continuing." 1 "$status"
	assertContains "Rollback failures should identify the destination snapshot that could not be rolled back." \
		"$output" "Failed to roll back destination [backup/target/src] to backup/target/src@snap1 after deleting snapshots."
	assertContains "Rollback failures should not bump the destination mutation generation as if the mutation succeeded." \
		"$output" "generation=0"
}
