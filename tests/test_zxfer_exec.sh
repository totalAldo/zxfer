#!/bin/sh
#
# shunit2 entry point for src/zxfer_exec.sh: command execution, dry runs,
# background commands and the background-shell spawn modes. The
# background-shell fragment keeps the send-job fixture it was written for.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Exec behavior includes property-backup serialization cases.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"
# shellcheck source=tests/helpers/send_job_fixtures.sh
. "$TESTS_DIR/helpers/send_job_fixtures.sh"

zxfer_usage() {
	printf '%s\n' "usage: zxfer"
}

# Some macOS sandboxes report sysconf(_SC_ARG_MAX) failures when invoking
# /usr/bin/xargs without arguments. Provide a shell stub for the shunit2 lookup
# that mirrors the behavior needed by _shunit_extractTestFunctions().
# shellcheck disable=SC2120
xargs() {
	if command [ "$#" -eq 0 ]; then
		tr '\n' ' ' | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//'
	else
		command xargs "$@"
	fi
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_shunit"
	TEST_ORIGINAL_PATH=$PATH
	zxfer_test_exec_fixture_one_time_setup
}

oneTimeTearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_exec_background_shell_tests.sh"; then
		zxfer_test_send_job_fixture_setup
		return
	fi
	zxfer_test_exec_fixture_setup
}

tearDown() {
	relax_test_tmpdir_permissions
}

# Each fragment holds the tests of the src modules named in its header. They
# load and run in src/zxfer_modules.sh order.
# zxfer-test-fragment: suites/zxfer_exec_command_tests.sh
# shellcheck source=tests/suites/zxfer_exec_command_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_command_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_snapshot_producers_tests.sh
# shellcheck source=tests/suites/zxfer_exec_snapshot_producers_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_snapshot_producers_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_background_shell_tests.sh
# shellcheck source=tests/suites/zxfer_exec_background_shell_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_background_shell_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_exec.sh" \
		"$TESTS_DIR/suites/zxfer_exec_command_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_snapshot_producers_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_background_shell_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
