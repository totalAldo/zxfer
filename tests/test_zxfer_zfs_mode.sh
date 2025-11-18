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
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0
	initial_source=""
	g_recursive_source_list=""
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

# shellcheck disable=SC1090
. "$SHUNIT2_BIN"
