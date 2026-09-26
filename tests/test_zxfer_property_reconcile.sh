#!/bin/sh
#
# Stable shunit2 entry point for property state, policy, reconciliation, and
# transfer behavior. Test definitions live in ordered behavior fragments below.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Property-policy cases that render backup metadata opt in to that fixture.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"

# Purpose: Append one "dataset<TAB>list" row to one side's property table.
# Usage: zxfer_property_test_table_add source|destination DATASET LIST
zxfer_property_test_table_add() {
	l_table_row=$2$ZXFER_TAB$3
	case $1 in
	source)
		g_zxfer_source_property_table=${g_zxfer_source_property_table:+$g_zxfer_source_property_table$ZXFER_LF}$l_table_row
		;;
	destination)
		g_zxfer_destination_property_table=${g_zxfer_destination_property_table:+$g_zxfer_destination_property_table$ZXFER_LF}$l_table_row
		;;
	esac
}

# Purpose: Print how many control bytes other than LF (C0 or DEL) TEXT holds.
# Usage: zxfer_property_test_count_control_bytes TEXT
zxfer_property_test_count_control_bytes() {
	printf '%s' "$1" | LC_ALL=C tr -dc '\001-\011\013-\037\177' | wc -c | tr -d ' '
}

zxfer_property_test_report_globbing_state() {
	l_globbing_label=$1
	case $- in
	*f*) printf '%s_globbing=disabled\n' "$l_globbing_label" ;;
	*) printf '%s_globbing=enabled\n' "$l_globbing_label" ;;
	esac
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_transfer_props"
	TEST_SSH_PATH=$(command -v ssh 2>/dev/null || printf '%s\n' ssh)
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_reset_all_owner_state || return
	unset FAKE_REMOTE_PATH FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	ZXFER_BASE_READONLY_PROPERTIES="readonly,mountpoint"
	ZXFER_FREEBSD_READONLY_PROPERTIES="aclmode"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh=$TEST_SSH_PATH
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR"
}

# Each fragment has a "zxfer-test-fragment" marker, a source line and a path
# in suite(); keep the three in the same order so listing and execution agree.
# zxfer-test-fragment: suites/zxfer_property_state_cache_tests.sh
# shellcheck source=tests/suites/zxfer_property_state_cache_tests.sh
. "$TESTS_DIR/suites/zxfer_property_state_cache_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_policy_tests.sh
# shellcheck source=tests/suites/zxfer_property_policy_tests.sh
. "$TESTS_DIR/suites/zxfer_property_policy_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_reconcile_apply_tests.sh
# shellcheck source=tests/suites/zxfer_property_reconcile_apply_tests.sh
. "$TESTS_DIR/suites/zxfer_property_reconcile_apply_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_transfer_tests.sh
# shellcheck source=tests/suites/zxfer_property_transfer_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_property_reconcile.sh" \
		"$TESTS_DIR/suites/zxfer_property_state_cache_tests.sh" \
		"$TESTS_DIR/suites/zxfer_property_policy_tests.sh" \
		"$TESTS_DIR/suites/zxfer_property_reconcile_apply_tests.sh" \
		"$TESTS_DIR/suites/zxfer_property_transfer_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
