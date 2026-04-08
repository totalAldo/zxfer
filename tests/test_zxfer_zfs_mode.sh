#!/bin/sh
#
# shunit2 tests for zxfer_zfs_mode helpers.
#
# shellcheck disable=SC2030,SC2031,SC2317,SC2329

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh"

# The CLI entrypoint normally provides usage(), so offer a stub to satisfy
# throw_usage_error() during unit tests.
usage() {
	:
}

get_zfs_list() {
	STUB_ZFS_LIST_CALLS=$((STUB_ZFS_LIST_CALLS + 1))
}

stopsvcs() {
	cat >>"$STUB_STOPSVCS_LOG"
}

newsnap() {
	printf '%s\n' "$1" >>"$STUB_NEW_SNAP_LOG"
}

wait_for_zfs_send_jobs() {
	:
}

mock_zfs_tool() {
	printf '%s\n' "$*" >>"$STUB_ZFS_CMD_LOG"
}

run_source_zfs_cmd() {
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
	TEST_TMPDIR=$(mktemp -d -t zxfer_zfs_mode.XXXXXX)
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	: >"$TEST_TMPDIR/stopsvcs.log"
	: >"$TEST_TMPDIR/newsnap.log"
	: >"$TEST_TMPDIR/zfs_cmd.log"
	STUB_STOPSVCS_LOG="$TEST_TMPDIR/stopsvcs.log"
	STUB_NEW_SNAP_LOG="$TEST_TMPDIR/newsnap.log"
	STUB_ZFS_CMD_LOG="$TEST_TMPDIR/zfs_cmd.log"
	STUB_ZFS_LIST_CALLS=0
	zxfer_reset_destination_existence_cache
	get_zfs_list() {
		STUB_ZFS_LIST_CALLS=$((STUB_ZFS_LIST_CALLS + 1))
	}
	stopsvcs() {
		cat >>"$STUB_STOPSVCS_LOG"
	}
	newsnap() {
		printf '%s\n' "$1" >>"$STUB_NEW_SNAP_LOG"
	}
	wait_for_zfs_send_jobs() {
		:
	}
	mock_zfs_tool() {
		printf '%s\n' "$*" >>"$STUB_ZFS_CMD_LOG"
	}
	run_source_zfs_cmd() {
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
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	initial_source=""
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_recursive_dest_list=""
	g_did_delete_dest_snapshots=0
	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_src_snapshot_transfer_list=""
	g_actual_dest=""
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
	g_zxfer_new_snapshot_name="zxfer_test_snapshot"
	g_readonly_properties="type,mountpoint,creation"
	g_LZFS="mock_zfs_tool"
	g_dest_created_by_zxfer=0
	g_dest_seed_requires_property_reconcile=0
}

test_resolve_initial_source_prefers_recursive_flag() {
	g_option_R_recursive="tank/src"

	resolve_initial_source_from_options

	assertEquals "Recursive source should be selected when -R is provided." "$g_option_R_recursive" "$initial_source"
}

test_resolve_initial_source_uses_nonrecursive_when_only_N_set() {
	g_option_N_nonrecursive="tank/nonrecursive"

	resolve_initial_source_from_options

	assertEquals "Non-recursive source should be selected when -N is provided." "$g_option_N_nonrecursive" "$initial_source"
}

test_resolve_initial_source_conflicts_trigger_usage_error() {
	g_option_R_recursive="tank/src"
	g_option_N_nonrecursive="tank/child"

	if (resolve_initial_source_from_options) >/dev/null 2>&1; then
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
	initial_source="tank/src"
	empty_path="$TEST_TMPDIR/no_svcadm"
	mkdir -p "$empty_path"
	old_path=$PATH

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		PATH="$empty_path"
		validate_zfs_mode_preconditions
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
		resolve_initial_source_from_options
	) >/dev/null 2>&1
	status=$?

	assertEquals "Missing -N/-R options should exit with a usage error." "2" "$status"
}

test_normalize_source_destination_strips_trailing_slashes() {
	initial_source="tank/src///"
	g_destination="backup/target//"

	normalize_source_destination_paths

	assertEquals "Trailing slashes should be removed from source." "tank/src" "$initial_source"
	assertEquals "Trailing slashes should be removed from destination." "backup/target" "$g_destination"
	assertEquals "Trailing slash flag should record the original suffix." "1" "$g_initial_source_had_trailing_slash"
}

test_normalize_source_destination_rejects_absolute_paths() {
	initial_source="/tank/src"
	g_destination="backup/target"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		normalize_source_destination_paths
	) >/dev/null 2>&1
	status_source=$?

	initial_source="tank/src"
	g_destination="/backup/target"
	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		normalize_source_destination_paths
	) >/dev/null 2>&1
	status_dest=$?

	assertEquals "Absolute source paths should be rejected." "2" "$status_source"
	assertEquals "Absolute destination paths should be rejected." "2" "$status_dest"
}

test_set_actual_dest_without_trailing_slash_appends_relative_path() {
	initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0

	set_actual_dest "tank/src/projects/alpha"

	assertEquals "Destination should mirror the relative source suffix." "backup/target/src/projects/alpha" "$g_actual_dest"
}

test_set_actual_dest_with_trailing_slash_preserves_destination_prefix() {
	initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=1

	set_actual_dest "tank/src/projects/beta"

	assertEquals "Trailing slash should replicate directly under the destination root." "backup/target/projects/beta" "$g_actual_dest"
}

test_refresh_dataset_iteration_state_populates_recursive_list_when_not_recursive() {
	g_option_R_recursive=""
	initial_source="tank/src"
	g_recursive_source_list=""
	STUB_ZFS_LIST_CALLS=0

	refresh_dataset_iteration_state

	assertEquals "Refresh should re-populate the recursive source list when -R is unset." \
		"$initial_source" "$g_recursive_source_list"
	assertEquals "get_zfs_list should be invoked once during refresh." "1" "$STUB_ZFS_LIST_CALLS"
}

test_refresh_dataset_iteration_state_preserves_list_when_recursive_mode_set() {
	g_option_R_recursive="tank/src"
	initial_source="tank/src"
	g_recursive_source_list="tank/src tank/src/child"
	STUB_ZFS_LIST_CALLS=0

	refresh_dataset_iteration_state

	assertEquals "Recursive option should keep the existing dataset list untouched." \
		"tank/src tank/src/child" "$g_recursive_source_list"
	assertEquals "get_zfs_list should still be called exactly once." "1" "$STUB_ZFS_LIST_CALLS"
}

test_refresh_dataset_iteration_state_refreshes_property_tree_prefetch_context_when_available() {
	log="$TEST_TMPDIR/refresh_prefetch_context.log"
	: >"$log"

	(
		REFRESH_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh-prefetch\n' >>"$REFRESH_LOG"
		}
		refresh_dataset_iteration_state
	)

	assertEquals "Refreshing dataset iteration state should also refresh the recursive property-tree prefetch context when that optimization helper is available." \
		"refresh-prefetch" "$(cat "$log")"
}

test_maybe_capture_preflight_snapshot_captures_when_enabled() {
	g_option_s_make_snapshot=1
	g_option_n_dryrun=0
	initial_source="tank/src"

	maybe_capture_preflight_snapshot

	assertEquals "Snapshot helper should run once when -s is enabled." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset state should call get_zfs_list exactly once." "1" "$STUB_ZFS_LIST_CALLS"
}

test_maybe_capture_preflight_snapshot_dry_run_skips_refresh() {
	g_option_s_make_snapshot=1
	g_option_n_dryrun=1
	initial_source="tank/src"

	maybe_capture_preflight_snapshot

	assertEquals "Dry-run -s should still preview the snapshot helper once." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dry-run -s should not refresh cached dataset state." "0" "$STUB_ZFS_LIST_CALLS"
}

test_maybe_capture_preflight_snapshot_skips_when_migrating() {
	g_option_s_make_snapshot=1
	g_option_m_migrate=1
	initial_source="tank/src"

	maybe_capture_preflight_snapshot

	assertEquals "Migration path should not trigger new snapshots from -s." "" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dataset refresh should not run when snapshot is skipped." "0" "$STUB_ZFS_LIST_CALLS"
}

test_prepare_migration_services_stops_services_and_unmounts_sources() {
	g_option_m_migrate=1
	g_option_n_dryrun=0
	g_option_c_services="svc:/network/iscsi_target svc:/network/nfs/server"
	g_option_R_recursive="tank/src"
	initial_source="tank/src"
	g_recursive_source_list="tank/src tank/src/child"

	prepare_migration_services

	assertEquals "Services should be piped to stopsvcs intact." \
		"svc:/network/iscsi_target svc:/network/nfs/server" "$(cat "$STUB_STOPSVCS_LOG")"
	assertEquals "All recursive datasets must be unmounted before migrating." \
		"unmount tank/src
unmount tank/src/child" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Migration must create a final snapshot for the initial source." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset lists should run exactly once." "1" "$STUB_ZFS_LIST_CALLS"
	case "$g_readonly_properties" in
	*mountpoint*)
		fail "Readonly properties list should drop mountpoint during migration."
		;;
	esac
}

test_prepare_migration_services_dry_run_previews_without_mutating_state() {
	g_option_m_migrate=1
	g_option_n_dryrun=1
	g_option_v_verbose=1
	g_option_c_services="svc:/network/iscsi_target svc:/network/nfs/server"
	initial_source="tank/src"
	g_recursive_source_list="tank/src tank/src/child"
	state_log="$TEST_TMPDIR/migration_dry_run_state.log"
	output=$(
		(
			prepare_migration_services
			printf 'readonly=%s\n' "$g_readonly_properties" >"$state_log"
			printf 'restart=%s\n' "$m_services_to_restart" >>"$state_log"
			printf 'need=%s\n' "$g_services_need_relaunch" >>"$state_log"
		) 2>&1
	)

	assertEquals "Dry-run migration should not call stopsvcs." "" "$(cat "$STUB_STOPSVCS_LOG")"
	assertEquals "Dry-run migration should not unmount any datasets." "" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Dry-run migration should still preview the final snapshot helper once." \
		"tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dry-run migration should not refresh cached dataset state." "0" "$STUB_ZFS_LIST_CALLS"
	case "$(grep '^readonly=' "$state_log")" in
	*mountpoint*)
		fail "Dry-run migration should still drop mountpoint from the effective readonly-property list."
		;;
	esac
	assertContains "Dry-run migration should still track which services would need relaunch later." \
		"$(cat "$state_log")" "restart= svc:/network/iscsi_target svc:/network/nfs/server"
	assertContains "Dry-run migration should still flag relaunch as required." \
		"$(cat "$state_log")" "need=1"
	assertContains "Dry-run migration should preview service-disabling commands." \
		"$output" "Dry run: 'svcadm' 'disable' '-st' 'svc:/network/iscsi_target'"
	assertContains "Dry-run migration should preview unmount commands for each source dataset." \
		"$output" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/src'"
	assertContains "Dry-run migration should preview descendant unmount commands too." \
		"$output" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/src/child'"
}

test_prepare_migration_services_dry_run_strips_mountpoint_in_current_shell() {
	g_option_m_migrate=1
	g_option_n_dryrun=1
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_readonly_properties="type,mountpoint,creation"

	prepare_migration_services

	assertEquals "Dry-run migration should drop mountpoint from the readonly-property list in the current shell." \
		"type,creation" "$g_readonly_properties"
}

test_prepare_migration_services_preserves_service_restart_state_in_current_shell() {
	g_option_m_migrate=1
	g_option_c_services="svc:/system/filesystem/local"
	initial_source="tank/src"
	g_recursive_source_list="tank/src"

	stopsvcs() {
		cat >/dev/null
		m_services_to_restart=" svc:/system/filesystem/local"
		g_services_need_relaunch=1
	}

	prepare_migration_services

	assertEquals "Migration preflight should retain the service restart list in the parent shell." \
		" svc:/system/filesystem/local" "$m_services_to_restart"
	assertEquals "Migration preflight should retain the relaunch flag in the parent shell." \
		"1" "$g_services_need_relaunch"
}

test_prepare_migration_services_propagates_service_disable_failures() {
	g_option_m_migrate=1
	g_option_c_services="svc:/system/filesystem/local"
	initial_source="tank/src"
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			stopsvcs() {
				cat >/dev/null
				throw_error "Could not disable service svc:/system/filesystem/local."
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			prepare_migration_services
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
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
trap - EXIT INT TERM HUP QUIT
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
svcadm() {
	printf '%s %s %s\n' "$1" "$2" "$3" >>"$SVC_LOG"
}
stopsvcs <<'INNER'
svc:/network/nfs/server svc:/network/ssh
INNER
printf 'restart=%s\n' "$m_services_to_restart"
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "stopsvcs should disable each requested service with -st." \
		"disable -st svc:/network/nfs/server
disable -st svc:/network/ssh" "$(cat "$log")"
	assertContains "Disabled services should be tracked for relaunch." \
		"$output" "restart= svc:/network/nfs/server svc:/network/ssh"
	assertContains "Disabling services should mark relaunch as required." \
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
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_services_need_relaunch=0
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
stopsvcs <<'INNER'
INNER
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "Empty service lists should not invoke svcadm." "" "$(cat "$log")"
	assertContains "Empty service lists should leave relaunch tracking disabled." "$output" "need=0"
}

test_stopsvcs_ignores_whitespace_only_service_input() {
	log="$TEST_TMPDIR/stopsvcs_whitespace.log"
	: >"$log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_services_need_relaunch=0
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
stopsvcs <<'INNER'

INNER
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "Whitespace-only service lists should not invoke svcadm." "" "$(cat "$log")"
	assertContains "Whitespace-only service lists should leave relaunch tracking disabled." "$output" "need=0"
}

test_stopsvcs_relaunches_and_errors_when_disable_fails() {
	set +e
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
trap - EXIT INT TERM HUP QUIT
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
relaunch() {
	printf 'relaunch\n'
}
throw_error() {
	printf '%s\n' "$1"
	exit 1
}
svcadm() {
	return 1
}
stopsvcs <<'INNER'
svc:/network/nfs/server
INNER
EOF
	)
	status=$?

	assertEquals "Service-disable failures should abort stopsvcs." 1 "$status"
	assertContains "stopsvcs should relaunch services before failing." "$output" "relaunch"
	assertContains "stopsvcs failures should identify the offending service." \
		"$output" "Could not disable service svc:/network/nfs/server."
}

test_stopsvcs_normalizes_multiline_service_input_in_current_shell() {
	log="$TEST_TMPDIR/stopsvcs_current.log"
	: >"$log"
	m_services_to_restart=""
	g_services_need_relaunch=0
	# shellcheck source=src/zxfer_zfs_mode.sh
	. "$ZXFER_ROOT/src/zxfer_zfs_mode.sh"
	svcadm() {
		printf '%s %s %s\n' "$1" "$2" "$3" >>"$log"
	}

	stopsvcs <<'INNER'
svc:/network/nfs/server
svc:/network/ssh    svc:/system/test
INNER

	unset -f svcadm

	assertEquals "Current-shell service handling should normalize multiline input into one disable per service." \
		"disable -st svc:/network/nfs/server
disable -st svc:/network/ssh
disable -st svc:/system/test" "$(cat "$log")"
	assertEquals "Current-shell service handling should track every disabled service for relaunch." \
		" svc:/network/nfs/server svc:/network/ssh svc:/system/test" "$m_services_to_restart"
	assertEquals "Disabling services should still mark relaunch as required." "1" "$g_services_need_relaunch"
}

test_relaunch_enables_services_and_clears_need_flag() {
	log="$TEST_TMPDIR/relaunch_actions.log"
	output=$(
		(
			SVC_LOG="$log"
			svcadm() {
				printf '%s %s\n' "$1" "$2" >>"$SVC_LOG"
			}
			m_services_to_restart="svc:/network/nfs/server svc:/network/ssh"
			g_services_need_relaunch=1
			relaunch
			printf 'need=%s\n' "$g_services_need_relaunch"
		)
	)

	assertEquals "relaunch should enable each previously disabled service." \
		"enable svc:/network/nfs/server
enable svc:/network/ssh" "$(cat "$log")"
	assertContains "Successful relaunch should clear the relaunch-needed flag." "$output" "need=0"
}

test_relaunch_returns_success_when_no_services_are_pending_in_current_shell() {
	m_services_to_restart=""
	g_services_need_relaunch=1
	g_services_relaunch_in_progress=1

	relaunch

	assertEquals "Empty relaunch queues should clear the relaunch-needed flag." \
		"0" "$g_services_need_relaunch"
	assertEquals "Empty relaunch queues should clear the in-progress guard." \
		"0" "$g_services_relaunch_in_progress"
}

test_relaunch_throws_when_service_enable_fails() {
	set +e
	output=$(
		(
			svcadm() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			m_services_to_restart="svc:/network/nfs/server"
			g_services_need_relaunch=1
			relaunch
		)
	)
	status=$?

	assertEquals "relaunch should abort when a service cannot be re-enabled." 1 "$status"
	assertContains "relaunch failures should identify the service that failed to start." \
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
			throw_error() {
				printf '%s\n' "$1"
				printf 'need=%s\n' "$g_services_need_relaunch"
				printf 'pending=%s\n' "$m_services_to_restart"
				printf 'guard=%s\n' "$g_services_relaunch_in_progress"
				exit 1
			}
			m_services_to_restart="svc:/network/nfs/server svc:/network/ssh svc:/system/test"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			relaunch
		)
	)
	status=$?

	assertEquals "relaunch should still fail when any service cannot be re-enabled." 1 "$status"
	assertEquals "relaunch should still attempt every queued service even after one enable fails." \
		"enable svc:/network/nfs/server
enable svc:/network/ssh
enable svc:/system/test" "$(cat "$log")"
	assertContains "Partial relaunch failures should keep the relaunch-needed flag asserted." \
		"$output" "need=1"
	assertContains "Partial relaunch failures should keep only failed services queued for later recovery." \
		"$output" "pending=svc:/network/ssh"
	assertContains "Partial relaunch failures should leave the in-progress guard asserted until exit cleanup finishes." \
		"$output" "guard=1"
}

test_relaunch_reports_all_failed_services() {
	set +e
	output=$(
		(
			svcadm() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			m_services_to_restart="svc:/network/nfs/server svc:/network/ssh"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			relaunch
		)
	)
	status=$?

	assertEquals "relaunch should fail when multiple services cannot be re-enabled." 1 "$status"
	assertContains "Multi-service relaunch failures should mention every failed service." \
		"$output" "Couldn't re-enable services: svc:/network/nfs/server svc:/network/ssh."
}

test_relaunch_dry_run_previews_enable_commands_without_executing() {
	log="$TEST_TMPDIR/relaunch_dry_run.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=1
g_option_v_verbose=1
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
m_services_to_restart=" svc:/network/nfs/server svc:/network/ssh"
g_services_need_relaunch=1
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
relaunch
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	l_relaunch_log=""
	if [ -f "$log" ]; then
		l_relaunch_log=$(cat "$log")
	fi
	assertEquals "Dry-run relaunch should not execute svcadm enable." "" "$l_relaunch_log"
	assertContains "Dry-run relaunch should preview the first enable command." \
		"$output" "Dry run: 'svcadm' 'enable' 'svc:/network/nfs/server'"
	assertContains "Dry-run relaunch should preview every queued enable command." \
		"$output" "Dry run: 'svcadm' 'enable' 'svc:/network/ssh'"
	assertContains "Dry-run relaunch should still clear the relaunch-needed flag." \
		"$output" "need=0"
}

test_relaunch_dry_run_previews_enable_commands_in_current_shell() {
	log="$TEST_TMPDIR/relaunch_dry_run_current_shell.log"
	: >"$log"

	echov() {
		printf '%s\n' "$*" >>"$log"
	}
	svcadm() {
		printf '%s\n' "$*" >>"$log"
	}
	g_option_n_dryrun=1
	m_services_to_restart=" svc:/network/nfs/server"
	g_services_need_relaunch=1

	relaunch

	# Restore the shared verbose helper so later tests are not affected by the stub.
	echov() {
		if [ "$g_option_v_verbose" -eq 1 ]; then
			echo "$@"
		fi
	}
	unset -f svcadm

	assertEquals "Current-shell dry-run relaunch should preview enable commands without executing svcadm." \
		"Restarting service svc:/network/nfs/server
Dry run: 'svcadm' 'enable' 'svc:/network/nfs/server'" "$(cat "$log")"
}

test_newsnap_uses_recursive_snapshot_flag() {
	log="$TEST_TMPDIR/newsnap.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive="tank/src"
g_zxfer_new_snapshot_name="zxfer_unit"
g_LZFS="mock_zfs_tool"
run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
newsnap "tank/src@old"
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
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive=""
g_zxfer_new_snapshot_name="zxfer_single"
g_LZFS="mock_zfs_tool"
run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
newsnap "tank/src@old"
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
	# shellcheck source=src/zxfer_zfs_mode.sh
	. "$ZXFER_ROOT/src/zxfer_zfs_mode.sh"
	run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	newsnap "tank/src@old"

	unset -f run_source_zfs_cmd

	assertEquals "Current-shell recursive snapshot generation should include the -r flag." \
		"snapshot -r tank/src@zxfer_current" "$(cat "$log")"
}

test_newsnap_builds_nonrecursive_command_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_single.log"
	g_option_R_recursive=""
	g_zxfer_new_snapshot_name="zxfer_current_single"
	g_LZFS="mock_zfs_tool"
	# shellcheck source=src/zxfer_zfs_mode.sh
	. "$ZXFER_ROOT/src/zxfer_zfs_mode.sh"
	run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	newsnap "tank/src@old"

	unset -f run_source_zfs_cmd

	assertEquals "Current-shell non-recursive snapshot generation should omit the -r flag." \
		"snapshot tank/src@zxfer_current_single" "$(cat "$log")"
}

test_newsnap_dry_run_previews_without_executing() {
	log="$TEST_TMPDIR/newsnap_dry_run.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=1
g_option_v_verbose=1
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive="tank/src"
g_zxfer_new_snapshot_name="zxfer_dry_run"
g_LZFS="mock_zfs_tool"
run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
newsnap "tank/src@old"
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

test_calculate_unsupported_properties_collects_source_only_entries() {
	initial_source="tank/src"
	g_destination="backup/dst"

	(
		run_destination_zfs_cmd() {
			printf 'compression\natime\n'
		}
		run_source_zfs_cmd() {
			printf 'compression\natime\nrecordsize\n'
		}
		calculate_unsupported_properties
		printf '%s\n' "$unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_props.out"

	assertEquals "Only properties missing from the destination should be reported unsupported." \
		"recordsize" "$(cat "$TEST_TMPDIR/unsupported_props.out")"
}

test_calculate_unsupported_properties_fails_closed_on_destination_probe_error() {
	initial_source="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				printf '%s\n' "ssh failure"
				return 1
			}
			run_source_zfs_cmd() {
				printf 'compression\natime\nrecordsize\n'
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Destination capability probe failures should abort unsupported-property calculation." \
		"1" "$status"
	assertContains "Destination capability probe failures should be surfaced instead of stripping all properties." \
		"$output" "Failed to retrieve destination supported property list for pool [backup]: ssh failure"
}

test_calculate_unsupported_properties_fails_closed_on_source_probe_error() {
	initial_source="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				printf 'compression\natime\n'
			}
			run_source_zfs_cmd() {
				printf '%s\n' "local failure"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Source capability probe failures should abort unsupported-property calculation." \
		"1" "$status"
	assertContains "Source capability probe failures should be surfaced instead of silently preserving all properties." \
		"$output" "Failed to retrieve source supported property list for pool [tank]: local failure"
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
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
				printf '%s %s %s\n' "$1" "$2" "$3" >>"$ROLLBACK_LOG"
				return 0
			}
			rollback_destination_to_last_common_snapshot
			printf 'flag=%s\n' "$g_did_delete_dest_snapshots"
		)
	)

	assertEquals "Rollback should target the destination snapshot matching the last common snapshot." \
		"rollback -r backup/target/src@snap1" "$(cat "$log")"
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
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=0
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=0
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		exists_destination() {
			printf '0\n'
		}
		run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap=""
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		rollback_destination_to_last_common_snapshot
	)

	assertEquals "Rollback should no-op when -F is absent, deletions did not occur, deleted snapshots were not newer than the last common snapshot, destination is absent, or no common snapshot exists." \
		"" "$(cat "$log")"
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
			exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/target/src] exists: ssh failure"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			rollback_destination_to_last_common_snapshot
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
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			rollback_destination_to_last_common_snapshot
		)
	)
	status=$?

	assertEquals "Rollback failures should abort instead of silently continuing." 1 "$status"
	assertContains "Rollback failures should identify the destination snapshot that could not be rolled back." \
		"$output" "Failed to roll back destination [backup/target/src] to backup/target/src@snap1 after deleting snapshots."
}

test_copy_snapshots_skips_when_no_pending_snapshots() {
	g_actual_dest="backup/target/src"
	g_dest_has_snapshots=1
	g_src_snapshot_transfer_list=""
	log="$TEST_TMPDIR/copy_none.log"
	: >"$log"

	(
		COPY_LOG="$log"
		reconcile_live_destination_snapshot_state() {
			:
		}
		rollback_destination_to_last_common_snapshot() {
			printf 'rollback\n' >>"$COPY_LOG"
		}
		zfs_send_receive() {
			printf 'send\n' >>"$COPY_LOG"
		}
		copy_snapshots
	)

	assertEquals "copy_snapshots should stop early when there are no source snapshots to send." \
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
		rollback_destination_to_last_common_snapshot() {
			:
		}
		exists_destination() {
			printf '0\n'
		}
		zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		copy_snapshots
	)

	assertEquals "Missing destinations should be seeded with the first snapshot, then resumed incrementally." \
		"prev= curr=tank/src@snap1 dest=backup/target/src bg=0
prev=tank/src@snap1 curr=tank/src@snap2 dest=backup/target/src bg=1" "$(cat "$log")"
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
		rollback_destination_to_last_common_snapshot() {
			:
		}
		exists_destination() {
			printf '0\n'
		}
		zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		copy_snapshots
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
			rollback_destination_to_last_common_snapshot() {
				:
			}
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@base	111"
					return 0
				fi
				printf '%s\n' "$*" >>"$log"
				return 0
			}
			zfs_send_receive() {
				printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$log"
			}

			copy_snapshots
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
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
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

			reconcile_live_destination_snapshot_state
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
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
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

			reconcile_live_destination_snapshot_state
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
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
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
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}

			reconcile_live_destination_snapshot_state
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
			rollback_destination_to_last_common_snapshot() {
				:
			}
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@base	999"
					return 0
				fi
				printf '%s\n' "$*" >>"$log"
				return 0
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zfs_send_receive() {
				printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$log"
			}

			copy_snapshots
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
		reconcile_live_destination_snapshot_state() {
			:
		}
		rollback_destination_to_last_common_snapshot() {
			:
		}
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
				return 0
			fi
			printf '%s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${g_option_F_force_rollback:-}" >>"$COPY_LOG"
		}

		copy_snapshots
	)

	assertEquals "A fresh live probe should allow seeding when an existing destination no longer has snapshots." \
		"prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
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
		reconcile_live_destination_snapshot_state() {
			:
		}
		rollback_destination_to_last_common_snapshot() {
			:
		}
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] &&
				[ "$4" = "name,guid" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
				[ "$7" = "backup/target/src" ]; then
				printf '%s\n' "backup/target/src/child@base	999"
				return 0
			fi
			printf '%s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s bg=%s force=%s\n' \
				"$1" "$2" "$3" "$4" "${g_option_F_force_rollback:-}" >>"$COPY_LOG"
		}

		copy_snapshots
	)

	assertEquals "Child-dataset snapshots should not block seeding the current dataset when the current dataset has no snapshots." \
		"prev= curr=tank/src@base dest=backup/target/src bg=0 force=-F" "$(cat "$log")"
}

test_copy_snapshots_reports_destination_probe_failures() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
	g_last_common_snap=""
	g_dest_has_snapshots=0

	set +e
	output=$(
		(
			rollback_destination_to_last_common_snapshot() {
				:
			}
			exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/target/src] exists: permission denied"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			copy_snapshots
		)
	)
	status=$?

	assertEquals "copy_snapshots should fail closed when destination existence checks fail." 1 "$status"
	assertContains "copy_snapshots should surface the destination probe failure." \
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
			rollback_destination_to_last_common_snapshot() {
				:
			}
			exists_destination() {
				printf '1\n'
			}
			run_destination_zfs_cmd() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			copy_snapshots
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
		rollback_destination_to_last_common_snapshot() {
			:
		}
		exists_destination() {
			printf '1\n'
		}
		zfs_send_receive() {
			printf 'send\n' >>"$COPY_LOG"
		}
		copy_snapshots
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
		rollback_destination_to_last_common_snapshot() {
			printf 'rollback\n' >>"$COPY_LOG"
		}
		zfs_send_receive() {
			printf 'send\n' >>"$COPY_LOG"
		}
		copy_snapshots
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
	g_last_common_snap="tank/src@snap1"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
	log="$TEST_TMPDIR/copy_no_force_no_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			printf 'rollback %s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zfs_send_receive() {
			printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		copy_snapshots
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
	g_last_common_snap="tank/src@snap1"
	g_src_snapshot_transfer_list="tank/src@snap1 tank/src@snap2"
	log="$TEST_TMPDIR/copy_old_deletes_no_rollback.log"
	: >"$log"

	(
		COPY_LOG="$log"
		exists_destination() {
			printf '1\n'
		}
		run_destination_zfs_cmd() {
			printf 'rollback %s\n' "$*" >>"$COPY_LOG"
			return 0
		}
		zfs_send_receive() {
			printf 'send %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$COPY_LOG"
		}
		copy_snapshots
	)

	assertEquals "Deleting only older destination snapshots should not trigger a pre-send rollback even when -F is active." \
		"send tank/src@snap1 tank/src@snap2 backup/target/src 1" "$(cat "$log")"
}

test_validate_zfs_mode_preconditions_requires_m_for_services() {
	g_option_c_services="svc:/network/nfs/server"
	g_option_m_migrate=0
	initial_source="tank/src"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		validate_zfs_mode_preconditions
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
		ensure_local_backup_dir() {
			printf '%s\n' "$1" >>"$LOCAL_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host=""
		g_backup_storage_root="$TEST_TMPDIR/local_backup"
		check_backup_storage_dir_if_needed
	)

	(
		REMOTE_LOG="$remote_log"
		ensure_remote_backup_dir() {
			printf '%s %s\n' "$1" "$2" >>"$REMOTE_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host="target.example"
		g_backup_storage_root="$TEST_TMPDIR/remote_backup"
		check_backup_storage_dir_if_needed
	)

	assertEquals "Local backup checks should validate the local backup root." \
		"$TEST_TMPDIR/local_backup" "$(cat "$local_log")"
	assertEquals "Remote backup checks should validate the remote backup root and host." \
		"$TEST_TMPDIR/remote_backup target.example" "$(cat "$remote_log")"
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
			ensure_local_backup_dir() {
				printf '%s\n' "$1" >>"$LOCAL_LOG"
			}
			ensure_remote_backup_dir() {
				printf '%s %s\n' "$1" "$2" >>"$REMOTE_LOG"
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/local backup"
			check_backup_storage_dir_if_needed
			g_option_T_target_host="target.example doas"
			g_backup_storage_root="/var/db/zxfer remote"
			check_backup_storage_dir_if_needed
		) 2>&1
	)

	assertEquals "Dry-run backup preflight should not call the live local backup-dir helper." \
		"" "$(cat "$local_log")"
	assertEquals "Dry-run backup preflight should not call the live remote backup-dir helper." \
		"" "$(cat "$remote_log")"
	assertContains "Dry-run backup preflight should preview the local secure backup-dir creation command." \
		"$output" "Dry run: umask 077; 'mkdir' '-p' '$TEST_TMPDIR/local backup'; 'chmod' '700' '$TEST_TMPDIR/local backup'"
	assertContains "Dry-run backup preflight should preview the remote ssh transport instead of executing it." \
		"$output" "Dry run: '/usr/bin/ssh' 'target.example'"
	assertContains "Dry-run backup preflight should preserve remote wrapper tokens in the rendered preview." \
		"$output" "doas"
	assertContains "Dry-run backup preflight should preview the remote secure backup-dir path." \
		"$output" "'/var/db/zxfer remote'"
	assertContains "Dry-run backup preflight should preview the remote chmod command." \
		"$output" "'chmod'"
}

test_initialize_replication_context_runs_restore_and_unsupported_scan() {
	log="$TEST_TMPDIR/init_context.log"
	: >"$log"
	initial_source="tank/src"
	g_option_R_recursive=""

	(
		CTX_LOG="$log"
		get_backup_properties() {
			printf 'backup\n' >>"$CTX_LOG"
		}
		get_zfs_list() {
			printf 'list\n' >>"$CTX_LOG"
		}
		calculate_unsupported_properties() {
			printf 'unsupported\n' >>"$CTX_LOG"
		}
		g_option_e_restore_property_mode=1
		g_option_U_skip_unsupported_properties=1
		initialize_replication_context
		printf 'recursive=%s\n' "$g_recursive_source_list" >>"$CTX_LOG"
	)

	assertEquals "Initialization should load backup properties, refresh dataset state, and derive unsupported properties." \
		"backup
list
unsupported
recursive=tank/src" "$(cat "$log")"
}

test_run_zfs_mode_calls_steps_in_order_and_relaunches_for_migration() {
	log="$TEST_TMPDIR/run_zfs_mode.log"
	: >"$log"

	(
		RUN_LOG="$log"
		resolve_initial_source_from_options() { printf 'resolve\n' >>"$RUN_LOG"; }
		normalize_source_destination_paths() { printf 'normalize\n' >>"$RUN_LOG"; }
		validate_zfs_mode_preconditions() { printf 'validate\n' >>"$RUN_LOG"; }
		check_backup_storage_dir_if_needed() { printf 'backupdir\n' >>"$RUN_LOG"; }
		initialize_replication_context() { printf 'context\n' >>"$RUN_LOG"; }
		maybe_capture_preflight_snapshot() { printf 'snapshot\n' >>"$RUN_LOG"; }
		prepare_migration_services() { printf 'prepare\n' >>"$RUN_LOG"; }
		perform_grandfather_protection_checks() { printf 'grandfather\n' >>"$RUN_LOG"; }
		copy_filesystems() { printf 'copy\n' >>"$RUN_LOG"; }
		relaunch() { printf 'relaunch\n' >>"$RUN_LOG"; }
		g_option_m_migrate=1
		run_zfs_mode
	)

	assertEquals "run_zfs_mode should execute its major phases in the expected order." \
		"resolve
normalize
validate
backupdir
context
snapshot
prepare
grandfather
copy
relaunch" "$(cat "$log")"
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
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_g_grandfather_protection=""
g_recursive_source_list="tank/src tank/src/child"
set_actual_dest() { echo "set $1" >>"$GRANDFATHER_LOG"; }
inspect_delete_snap() { echo "inspect $1 $2" >>"$GRANDFATHER_LOG"; }
perform_grandfather_protection_checks
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
zxfer_source_runtime_modules_through "zxfer_zfs_mode.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_g_grandfather_protection="enabled"
g_recursive_source_list="tank/src tank/src/child"
set_actual_dest() { printf 'set %s\n' "$1" >>"$GRANDFATHER_LOG"; }
inspect_delete_snap() { printf 'inspect %s %s\n' "$1" "$2" >>"$GRANDFATHER_LOG"; }
perform_grandfather_protection_checks
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
	initial_source="tank/src"
	g_destination="backup/target"
	log="$TEST_TMPDIR/grandfather_current.log"
	: >"$log"
	inspect_delete_snap() {
		printf 'inspect %s %s %s\n' "$1" "$2" "$g_actual_dest" >>"$log"
	}

	perform_grandfather_protection_checks

	unset -f inspect_delete_snap

	assertEquals "Current-shell grandfather checks should compute each destination before inspection." \
		"inspect 0 tank/src backup/target/src
inspect 0 tank/src/child backup/target/src/child" "$(cat "$log")"
}

test_copy_filesystems_inspects_source_when_only_deletions_pending() {
	g_option_d_delete_destination_snapshots=1
	initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src"
	g_recursive_destination_extra_dataset_list="tank/src"
	log="$TEST_TMPDIR/delete_only_single.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$COPY_FS_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$COPY_FS_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$COPY_FS_LOG"
		}
		copy_filesystems
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
	initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src
tank/src/child1
tank/src/child2"
	g_recursive_destination_extra_dataset_list=""
	log="$TEST_TMPDIR/delete_only_recursive_empty.log"
	rm -f "$log"

	(
		COPY_FS_LOG="$log"
		set_actual_dest() {
			printf 'set %s\n' "$1" >>"$COPY_FS_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$COPY_FS_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$COPY_FS_LOG"
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$COPY_FS_LOG"
		}
		copy_filesystems
	)

	assertEquals "Recursive -d runs should skip per-dataset inspection when discovery already proved there are no source or destination snapshot deltas." \
		"wait final sync" "$(cat "$log")"
}

test_copy_filesystems_inspects_only_datasets_with_recursive_delete_deltas() {
	g_option_d_delete_destination_snapshots=1
	g_option_R_recursive="tank/src"
	initial_source="tank/src"
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
		set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$COPY_FS_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$COPY_FS_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$COPY_FS_LOG"
		}
		copy_filesystems
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
	g_dest_created_by_zxfer=0
	g_src_snapshot_transfer_list="tank/src@seed1 tank/src@seed2"
	log="$TEST_TMPDIR/seed_existing.log"
	rm -f "$log"

	(
		SEED_LOG="$log"
		reconcile_live_destination_snapshot_state() { :; }
		rollback_destination_to_last_common_snapshot() { :; }
		exists_destination() { printf '1\n'; }
		run_destination_zfs_cmd() { return 0; }
		zfs_send_receive() {
			printf 'prev=%s curr=%s dest=%s force=%s bg=%s\n' \
				"${1:-<none>}" "$2" "$3" "${g_option_F_force_rollback:-<none>}" "$4" >>"$SEED_LOG"
		}
		copy_snapshots
	)

	expected="prev=<none> curr=tank/src@seed1 dest=backup/target/src force=-F bg=0"
	assertEquals "Existing destinations without snapshots should be seeded with forced receive." \
		"$expected" "$(head -n 1 "$log")"
}

test_copy_filesystems_forces_iteration_when_property_transfer_is_enabled() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	log="$TEST_TMPDIR/property_iteration.log"
	rm -f "$log"

	(
		ITER_LOG="$log"
		set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$ITER_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$ITER_LOG"
		}
		transfer_properties() {
			printf 'props %s\n' "$1" >>"$ITER_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$ITER_LOG"
		}
		wait_for_zfs_send_jobs() {
			:
		}
		copy_filesystems
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

test_copy_filesystems_reconciles_properties_after_seeding_created_destination() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/property_reconcile.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			case "$1" in
			tank/src) g_actual_dest="backup/target/src" ;;
			tank/src/child) g_actual_dest="backup/target/src/child" ;;
			esac
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
			if [ "$1" = "tank/src/child" ] && [ "${2:-0}" -eq 0 ]; then
				g_dest_created_by_zxfer=1
			fi
		}
		copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			if [ "$g_actual_dest" = "backup/target/src/child" ]; then
				g_dest_seed_requires_property_reconcile=1
			else
				g_dest_seed_requires_property_reconcile=0
			fi
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_filesystems
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

test_copy_filesystems_merges_iteration_sources_and_deduplicates_post_seed_reconcile_in_current_shell() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	g_option_d_delete_destination_snapshots=1
	initial_source="tank/src"
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
		set_actual_dest() {
			g_actual_dest="backup/$1"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			printf 'props %s skip=%s\n' "$1" "${2:-0}" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
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
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}

		copy_filesystems
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

test_copy_filesystems_refreshes_property_tree_prefetch_context_before_iteration() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="tank/src"
	log="$TEST_TMPDIR/copy_filesystems_prefetch_context.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh-prefetch\n' >>"$REFRESH_LOG"
		}
		set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			printf 'props %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$REFRESH_LOG"
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_filesystems
	)

	assertEquals "copy_filesystems should refresh the recursive property-tree prefetch context before iterating datasets so source and destination property slices stay aligned with the latest dataset lists." \
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
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_filesystems
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
	initial_source="tank/src"
	g_destination="backup/target"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list="backup/target"
	log="$TEST_TMPDIR/seed_reconcile_existing_root.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_filesystems
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
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile_current_shell.log"
	rm -f "$log"

	zxfer_refresh_property_tree_prefetch_context() {
		:
	}
	set_actual_dest() {
		g_actual_dest="backup/target/src"
	}
	inspect_delete_snap() {
		:
	}
	transfer_properties() {
		printf 'props skip=%s\n' "${2:-0}" >>"$log"
	}
	copy_snapshots() {
		g_dest_seed_requires_property_reconcile=1
	}
	wait_for_zfs_send_jobs() {
		printf 'wait\n' >>"$log"
	}
	zxfer_reset_destination_property_iteration_cache() {
		printf 'reset\n' >>"$log"
	}

	copy_filesystems

	assertEquals "Seeded destinations should be queued for a second property pass in the current shell as well." \
		"props skip=0
wait
reset
props skip=1" "$(cat "$log")"
	assertContains "The real destination-cache helper should note the newly seeded dataset before the second pass." \
		"$g_recursive_dest_list" "backup/target/src"

	# shellcheck source=src/zxfer_property_cache.sh
	. "$ZXFER_ROOT/src/zxfer_property_cache.sh"
	# shellcheck source=src/zxfer_zfs_mode.sh
	. "$ZXFER_ROOT/src/zxfer_zfs_mode.sh"
}

test_copy_filesystems_resets_destination_property_cache_before_post_seed_reconcile() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_recursive_dest_list=""
	log="$TEST_TMPDIR/seed_reconcile_cache_reset.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			g_actual_dest="backup/target/src"
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			l_dest_present=$(printf '%s\n' "${g_recursive_dest_list:-}" | grep -c "^$g_actual_dest$")
			printf 'props %s created=%s skip=%s dest_present=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" "$l_dest_present" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}
		copy_filesystems
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
	initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	g_zfs_send_job_pids=""
	log="$TEST_TMPDIR/background_property_cache_reset.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		zxfer_reset_destination_property_iteration_cache() {
			printf 'reset-destination-cache\n' >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			printf 'props %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$REFRESH_LOG"
			if [ "$g_actual_dest" = "tank/src" ]; then
				g_zfs_send_job_pids="12345"
			fi
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
			g_zfs_send_job_pids=""
		}
		copy_filesystems
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
	initial_source="tank/src"

	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					printf 'no\n'
					return 0
				fi
				return 0
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			prepare_migration_services
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
	initial_source="tank/src"

	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					return 1
				fi
				return 0
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should abort when mounted-state lookup fails." 1 "$status"
	assertContains "Mounted-state lookup failures should not be misreported as an unmounted source." \
		"$output" "Couldn't determine whether source tank/src is mounted."
}

test_prepare_migration_services_live_strips_mountpoint_in_current_shell() {
	g_option_m_migrate=1
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_readonly_properties="type,mountpoint,creation"

	prepare_migration_services

	assertEquals "Live migration should drop mountpoint from the readonly-property list in the current shell." \
		"type,creation" "$g_readonly_properties"
}

test_copy_filesystems_allows_post_unmount_migration_replication() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	initial_source="tank/src"
	log="$TEST_TMPDIR/migrate_context.log"
	rm -f "$log"

	(
		MIGRATE_LOG="$log"
		run_source_zfs_cmd() {
			if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
				printf 'no\n'
			fi
		}
		set_actual_dest() {
			g_actual_dest="backup/target/src"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$MIGRATE_LOG"
		}
		copy_snapshots() {
			printf 'copy %s\n' "$g_actual_dest" >>"$MIGRATE_LOG"
		}
		wait_for_zfs_send_jobs() {
			:
		}
		copy_filesystems
	)

	assertEquals "Migration copy loop should proceed after prepare_migration_services unmounts the source." \
		"inspect 0 tank/src
copy backup/target/src" "$(cat "$log")"
}

test_prepare_migration_services_relaunches_when_unmount_fails() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	initial_source="tank/src"

	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					printf 'yes\n'
					return 0
				fi
				if [ "$1" = "unmount" ]; then
					return 1
				fi
				return 0
			}
			relaunch() {
				printf 'relaunch\n'
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			prepare_migration_services
		)
	)
	status=$?

	assertEquals "Failed unmounts during migration should abort." 1 "$status"
	assertContains "Failed unmounts should relaunch services before aborting." "$output" "relaunch"
	assertContains "Failed unmounts should identify the affected source." \
		"$output" "Couldn't unmount source tank/src."
}

test_run_zfs_mode_loop_exits_after_single_iteration_when_no_changes() {
	g_option_Y_yield_iterations=4
	g_MAX_YIELD_ITERATIONS=8
	log="$TEST_TMPDIR/run_loop_single.log"
	: >"$log"

	(
		RUN_LOOP_LOG="$log"
		run_zfs_mode() {
			printf 'run\n' >>"$RUN_LOOP_LOG"
			g_is_performed_send_destroy=0
		}
		run_zfs_mode_loop
	)

	line_count=$(awk 'END {print NR}' "$log")
	assertEquals "Loop should stop after one iteration when no sends/destroys occur." "1" "$line_count"
}

test_run_zfs_mode_loop_repeats_until_changes_stop() {
	g_option_Y_yield_iterations=4
	g_MAX_YIELD_ITERATIONS=8
	log="$TEST_TMPDIR/run_loop_repeat.log"
	: >"$log"

	(
		RUN_LOOP_LOG="$log"
		iteration=0
		run_zfs_mode() {
			iteration=$((iteration + 1))
			printf 'run %s\n' "$iteration" >>"$RUN_LOOP_LOG"
			if [ "$iteration" -ge 2 ]; then
				g_is_performed_send_destroy=0
			else
				g_is_performed_send_destroy=1
			fi
		}
		run_zfs_mode_loop
	)

	line_count=$(awk 'END {print NR}' "$log")
	assertEquals "Loop should run until the helper clears the send/destroy flag." "2" "$line_count"
}

test_run_zfs_mode_loop_resets_property_cache_each_iteration() {
	g_option_Y_yield_iterations=4
	g_MAX_YIELD_ITERATIONS=8
	log="$TEST_TMPDIR/run_loop_cache_reset.log"
	: >"$log"

	(
		RUN_LOOP_LOG="$log"
		iteration=0
		zxfer_reset_property_iteration_caches() {
			printf 'reset\n' >>"$RUN_LOOP_LOG"
		}
		run_zfs_mode() {
			iteration=$((iteration + 1))
			printf 'run %s\n' "$iteration" >>"$RUN_LOOP_LOG"
			if [ "$iteration" -ge 2 ]; then
				g_is_performed_send_destroy=0
			else
				g_is_performed_send_destroy=1
			fi
		}
		run_zfs_mode_loop
	)

	assertEquals "Each run-loop iteration should clear the per-iteration property cache before executing zfs mode." \
		"reset
run 1
reset
run 2" "$(cat "$log")"
}

test_run_zfs_mode_loop_logs_hint_when_hard_iteration_limit_is_reached() {
	g_option_Y_yield_iterations=2
	g_MAX_YIELD_ITERATIONS=2
	g_option_V_very_verbose=1

	output=$(
		(
			run_zfs_mode() {
				g_is_performed_send_destroy=1
			}
			run_zfs_mode_loop
		) 2>&1
	)

	assertContains "Reaching the hard yield-iteration limit should emit the replication tuning hint." \
		"$output" "consider using compression, increasing bandwidth, increasing I/O or reducing snapshot frequency."
}

# shellcheck disable=SC1090
# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
