#!/bin/sh
#
# Stable shunit2 entry point for replication setup, snapshot transfer,
# orchestration, ready-queue, and failure-path behavior.
#
# Test definitions live in the behavior fragments below. Each fragment has a
# "zxfer-test-fragment" marker, a source line and a path in suite(); keep the
# three in the same order so listing and execution agree.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Replication orchestration fixtures render property-backup metadata.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_replication"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# Purpose: Define the command stubs every replication case starts from. Some
# cases replace one in the current shell, so setUp defines them each time.
# Usage: zxfer_test_stub_replication_commands; stubs log to the STUB_* files.
zxfer_test_stub_replication_commands() {
	zxfer_get_zfs_list() {
		STUB_ZFS_LIST_CALLS=$((STUB_ZFS_LIST_CALLS + 1))
	}
	zxfer_stopsvcs() {
		printf '%s\n' "$1" >>"$STUB_STOPSVCS_LOG"
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
}

# Stage the source record cache the planner reads: "dataset@snap<TAB>guid"
# rows, newest first.
zxfer_test_stage_source_records() {
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/source_snapshot.records"
	printf '%s\n' "$1" >"$g_zxfer_source_snapshot_record_cache_file"
}

setUp() {
	zxfer_test_reset_all_owner_state || return
	unset ZXFER_BACKUP_DIR
	STUB_STOPSVCS_LOG="$TEST_TMPDIR/zxfer_stopsvcs.log"
	STUB_NEW_SNAP_LOG="$TEST_TMPDIR/zxfer_newsnap.log"
	STUB_ZFS_CMD_LOG="$TEST_TMPDIR/zfs_cmd.log"
	: >"$STUB_STOPSVCS_LOG"
	: >"$STUB_NEW_SNAP_LOG"
	: >"$STUB_ZFS_CMD_LOG"
	STUB_ZFS_LIST_CALLS=0
	stub_dest_created_by_zxfer=0
	zxfer_test_stub_replication_commands
	g_cmd_zfs="mock_zfs_tool"
	g_destination="backup/target"
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
	g_zxfer_new_snapshot_name="zxfer_test_snapshot"
	ZXFER_BASE_READONLY_PROPERTIES="type,mountpoint,creation"
	ZXFER_MAX_YIELD_ITERATIONS=8
}

# Behavior-focused fragments keep the stable suite entry point while bounding
# the amount of replication test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_replication_setup_migration_tests.sh
# shellcheck source=tests/suites/zxfer_replication_setup_migration_tests.sh
. "$TESTS_DIR/suites/zxfer_replication_setup_migration_tests.sh"

# zxfer-test-fragment: suites/zxfer_replication_snapshot_transfer_tests.sh
# shellcheck source=tests/suites/zxfer_replication_snapshot_transfer_tests.sh
. "$TESTS_DIR/suites/zxfer_replication_snapshot_transfer_tests.sh"

# zxfer-test-fragment: suites/zxfer_replication_orchestration_tests.sh
# shellcheck source=tests/suites/zxfer_replication_orchestration_tests.sh
. "$TESTS_DIR/suites/zxfer_replication_orchestration_tests.sh"

# zxfer-test-fragment: suites/zxfer_replication_queue_failure_tests.sh
# shellcheck source=tests/suites/zxfer_replication_queue_failure_tests.sh
. "$TESTS_DIR/suites/zxfer_replication_queue_failure_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_replication.sh" \
		"$TESTS_DIR/suites/zxfer_replication_setup_migration_tests.sh" \
		"$TESTS_DIR/suites/zxfer_replication_snapshot_transfer_tests.sh" \
		"$TESTS_DIR/suites/zxfer_replication_orchestration_tests.sh" \
		"$TESTS_DIR/suites/zxfer_replication_queue_failure_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
