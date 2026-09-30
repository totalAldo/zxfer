#!/bin/sh
#
# shunit2 entry point for src/zxfer_backup_metadata.sh: -k metadata records,
# -e restore, local and remote storage, backup directories and paths.
#
# The paths fragment keeps the remote-host fixture it was written for; the
# other fragments use the fixture below.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Property-backup renderers and fake tools are domain fixtures, not part of the
# minimal shared lifecycle. This suite opts in explicitly.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_backup_metadata"
	zxfer_test_remote_host_fixture_one_time_setup
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_backup_metadata_paths_tests.sh"; then
		zxfer_test_remote_host_fixture_setup
		return
	fi
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

tearDown() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_backup_metadata_paths_tests.sh"; then
		zxfer_test_remote_host_fixture_teardown
	fi
}

# Behavior-focused fragments keep this stable suite entry point while
# bounding the amount of test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_backup_metadata_records_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_records_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_records_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_metadata_restore_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_restore_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_restore_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_metadata_storage_io_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_storage_io_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_storage_io_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_metadata_paths_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_paths_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_paths_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_backup_metadata.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_records_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_restore_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_storage_io_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_paths_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
