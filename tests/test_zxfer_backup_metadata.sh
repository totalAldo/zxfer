#!/bin/sh
#
# shunit2 tests for zxfer_backup_metadata.sh restore/write helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Property-backup renderers and fake tools are domain fixtures, not part of the
# minimal shared lifecycle. This suite opts in explicitly.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

zxfer_test_ensure_parent_dir() {
	l_path=$1
	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ] || [ "$l_parent" = "" ]; then
		l_parent=.
	fi
	mkdir -p "$l_parent"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_backup_metadata"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	zxfer_test_reset_all_owner_state || return
	OPTIND=1
	unset FAKE_SSH_LOG FAKE_SSH_EXIT_STATUS FAKE_SSH_STDOUT FAKE_SSH_STDERR \
		FAKE_SSH_SUPPRESS_STDOUT ZXFER_BACKUP_DIR ZXFER_SECURE_PATH \
		ZXFER_SECURE_PATH_APPEND
	TMPDIR="$TEST_TMPDIR"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
	# Cases name their own backup root and metadata file extension.
	g_backup_storage_root=""
	g_backup_file_extension=""
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return
}

# Behavior-focused fragments keep this stable suite entry point while
# bounding the amount of test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_backup_metadata_records_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_records_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_records_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_metadata_restore_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_restore_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_restore_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_storage_io_tests.sh
# shellcheck source=tests/suites/zxfer_backup_storage_io_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_storage_io_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_backup_metadata.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_records_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_restore_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_storage_io_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
