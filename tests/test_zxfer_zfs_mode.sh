#!/bin/sh
#
# shunit2 tests for zxfer_zfs_mode helpers.
#
# shellcheck disable=SC2030,SC2031,SC2317,SC2329

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_zfs_mode.sh
. "$ZXFER_ROOT/src/zxfer_zfs_mode.sh"

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

test_maybe_capture_preflight_snapshot_captures_when_enabled() {
	g_option_s_make_snapshot=1
	initial_source="tank/src"

	maybe_capture_preflight_snapshot

	assertEquals "Snapshot helper should run once when -s is enabled." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset state should call get_zfs_list exactly once." "1" "$STUB_ZFS_LIST_CALLS"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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

test_newsnap_uses_recursive_snapshot_flag() {
	log="$TEST_TMPDIR/newsnap.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
	. "$ZXFER_ROOT/src/zxfer_zfs_mode.sh"
	run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	newsnap "tank/src@old"

	unset -f run_source_zfs_cmd

	assertEquals "Current-shell non-recursive snapshot generation should omit the -r flag." \
		"snapshot tank/src@zxfer_current_single" "$(cat "$log")"
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

test_copy_snapshots_skips_when_no_pending_snapshots() {
	g_actual_dest="backup/target/src"
	g_src_snapshot_transfer_list=""
	log="$TEST_TMPDIR/copy_none.log"
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
	g_src_snapshot_transfer_list="tank/src@base"
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
					[ "$4" = "name" ] && [ "$5" = "-t" ] && [ "$6" = "snapshot" ] &&
					[ "$7" = "backup/target/src" ]; then
					printf '%s\n' "backup/target/src@base"
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
		"$output" "last=tank/src@base"
	assertContains "The destination should be marked as already containing snapshots after the live recheck." \
		"$output" "dest_has=1"
	assertContains "No further source snapshots should remain once the live common snapshot is confirmed." \
		"$output" "remaining="
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_zfs_mode.sh"
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
	assertEquals "-d should still inspect the dataset even when no new snapshots exist." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_inspects_all_datasets_for_recursive_deletes() {
	g_option_d_delete_destination_snapshots=1
	g_option_R_recursive="tank/src"
	initial_source="tank/src"
	g_recursive_source_list=""
	g_recursive_source_dataset_list="tank/src
tank/src/child1
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

	expected="set tank/src
inspect 1 tank/src
copy tank/src
set tank/src/child1
inspect 1 tank/src/child1
copy tank/src/child1
set tank/src/child2
inspect 1 tank/src/child2
copy tank/src/child2"
	assertEquals "Recursive -d runs should inspect every dataset even without pending sends." \
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
		rollback_destination_to_last_common_snapshot() { :; }
		exists_destination() { printf '1\n'; }
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
	log="$TEST_TMPDIR/property_reconcile.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			printf 'props %s created=%s skip=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" >>"$REFRESH_LOG"
			if [ "$1" = "tank/src/child" ] && [ "${2:-0}" -eq 0 ]; then
				g_dest_created_by_zxfer=1
			fi
		}
		copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			if [ "$g_actual_dest" = "tank/src/child" ]; then
				g_dest_seed_requires_property_reconcile=1
			else
				g_dest_seed_requires_property_reconcile=0
			fi
		}
		refresh_dataset_iteration_state() {
			printf 'refresh\n' >>"$REFRESH_LOG"
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src created=0 skip=0
copy tank/src created=0
set tank/src/child
inspect 0 tank/src/child
props tank/src/child created=0 skip=0
copy tank/src/child created=1
wait final sync
refresh
set tank/src/child
props tank/src/child created=1 skip=1"
	assertEquals "Created destinations should receive a second property reconciliation after the seed snapshot is received." \
		"$expected" "$(cat "$log")"
}

test_copy_filesystems_reconciles_seeded_empty_destinations_even_when_not_created_by_zxfer() {
	g_option_P_transfer_property=1
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=0
	initial_source="tank/src"
	g_recursive_source_list="tank/src"
	g_recursive_source_dataset_list="$g_recursive_source_list"
	log="$TEST_TMPDIR/seed_reconcile.log"
	rm -f "$log"

	(
		REFRESH_LOG="$log"
		set_actual_dest() {
			g_actual_dest=$1
			printf 'set %s\n' "$1" >>"$REFRESH_LOG"
		}
		inspect_delete_snap() {
			printf 'inspect %s %s\n' "$1" "$2" >>"$REFRESH_LOG"
		}
		transfer_properties() {
			printf 'props %s created=%s skip=%s\n' "$1" "${g_dest_created_by_zxfer:-0}" "${2:-0}" >>"$REFRESH_LOG"
		}
		copy_snapshots() {
			printf 'copy %s created=%s\n' "$g_actual_dest" "${g_dest_created_by_zxfer:-0}" >>"$REFRESH_LOG"
			g_dest_seed_requires_property_reconcile=1
		}
		refresh_dataset_iteration_state() {
			printf 'refresh\n' >>"$REFRESH_LOG"
		}
		wait_for_zfs_send_jobs() {
			printf 'wait %s\n' "$1" >>"$REFRESH_LOG"
		}
		copy_filesystems
	)

	expected="set tank/src
inspect 0 tank/src
props tank/src created=0 skip=0
copy tank/src created=0
wait final sync
refresh
set tank/src
props tank/src created=0 skip=1"
	assertEquals "Seeded empty destinations should receive a final property reconciliation even when zxfer did not create the dataset." \
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

# shellcheck disable=SC1090
. "$SHUNIT2_BIN"
