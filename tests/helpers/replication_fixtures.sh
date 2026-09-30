#!/bin/sh
# The unit fixture of tests/test_zxfer_replication.sh, shared with
# tests/test_zxfer_migration_services.sh: reset owner state, command stubs
# that log to the STUB_* files, a mock zfs, and fixed destination, backup-root,
# snapshot-name, readonly-property and yield settings. The source-record
# staging helper serves the live re-plan cases of
# tests/test_zxfer_snapshot_plan.sh.
# shellcheck disable=SC2034,SC2317,SC2329

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

# Purpose: Reset owner state and install the replication command stubs.
# Usage: zxfer_test_replication_fixture_setup || return, from setUp.
zxfer_test_replication_fixture_setup() {
	zxfer_test_reset_all_owner_state || return
	unset ZXFER_BACKUP_DIR
	STUB_STOPSVCS_LOG="$TEST_TMPDIR/zxfer_stopsvcs.log"
	STUB_NEW_SNAP_LOG="$TEST_TMPDIR/zxfer_newsnap.log"
	STUB_ZFS_CMD_LOG="$TEST_TMPDIR/zfs_cmd.log"
	: >"$STUB_STOPSVCS_LOG"
	: >"$STUB_NEW_SNAP_LOG"
	: >"$STUB_ZFS_CMD_LOG"
	STUB_ZFS_LIST_CALLS=0
	zxfer_test_stub_replication_commands
	g_cmd_zfs="mock_zfs_tool"
	g_destination="backup/target"
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
	g_zxfer_new_snapshot_name="zxfer_test_snapshot"
	ZXFER_BASE_READONLY_PROPERTIES="type,mountpoint,creation"
	ZXFER_MAX_YIELD_ITERATIONS=8
}

# Purpose: Stage the source record file the snapshot planner reads.
# Usage: zxfer_test_stage_source_records ROWS; ROWS are "dataset@snap<TAB>guid"
# lines, newest first. Points g_zxfer_source_snapshot_record_cache_file at
# $TEST_TMPDIR/source_snapshot.records.
zxfer_test_stage_source_records() {
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/source_snapshot.records"
	printf '%s\n' "$1" >"$g_zxfer_source_snapshot_record_cache_file"
}
