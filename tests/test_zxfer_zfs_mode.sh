#!/bin/sh
#
# shunit2 tests for zxfer_zfs_mode helpers.
#

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

mock_zfs_tool() {
	printf '%s\n' "$*" >>"$STUB_ZFS_CMD_LOG"
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
	g_readonly_properties="type,mountpoint,creation"
	g_LZFS="mock_zfs_tool"
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

test_normalize_source_destination_strips_trailing_slashes() {
	initial_source="tank/src///"
	g_destination="backup/target//"

	normalize_source_destination_paths

	assertEquals "Trailing slashes should be removed from source." "tank/src" "$initial_source"
	assertEquals "Trailing slashes should be removed from destination." "backup/target" "$g_destination"
	assertEquals "Trailing slash flag should record the original suffix." "1" "$g_initial_source_had_trailing_slash"
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

test_perform_grandfather_protection_checks_skips_when_flag_unset() {
	g_option_g_grandfather_protection=""
	g_recursive_source_list="tank/src tank/src/child"
	log="$TEST_TMPDIR/grandfather_skip.log"
	rm -f "$log"

	(
		GRANDFATHER_LOG="$log"
		set_actual_dest() { echo "set $1" >>"$GRANDFATHER_LOG"; }
		inspect_delete_snap() { echo "inspect $1 $2" >>"$GRANDFATHER_LOG"; }
		perform_grandfather_protection_checks
	)

	assertFalse "Grandfather check should no-op when flag is unset." "[ -s \"$log\" ]"
}

test_perform_grandfather_protection_checks_calls_helpers_for_each_dataset() {
	g_option_g_grandfather_protection="enabled"
	g_recursive_source_list="tank/src tank/src/child"
	log="$TEST_TMPDIR/grandfather_calls.log"
	rm -f "$log"

	(
		GRANDFATHER_LOG="$log"
		set_actual_dest() { printf 'set %s\n' "$1" >>"$GRANDFATHER_LOG"; }
		inspect_delete_snap() { printf 'inspect %s %s\n' "$1" "$2" >>"$GRANDFATHER_LOG"; }
		perform_grandfather_protection_checks
	)

	expected="set tank/src
inspect 0 tank/src
set tank/src/child
inspect 0 tank/src/child"
	assertEquals "Grandfather protection should inspect every dataset slated for replication." \
		"$expected" "$(cat "$log")"
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
