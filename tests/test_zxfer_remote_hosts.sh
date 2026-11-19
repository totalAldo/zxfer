#!/bin/sh
#
# shunit2 entry point for src/zxfer_remote_hosts.sh: the remote capability
# handshake and cache, helper lookup, and local/remote CLI command resolution.
#
# Test definitions use the remote-host fixture. Literal fragment source lines
# define loading, registration and discovery order.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts"
	zxfer_test_remote_host_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_remote_host_fixture_setup
}

tearDown() {
	zxfer_test_remote_host_fixture_teardown
}

# shellcheck source=tests/suites/zxfer_remote_hosts_capability_probe_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_capability_probe_tests.sh"

# shellcheck source=tests/suites/zxfer_remote_hosts_cli_commands_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_cli_commands_tests.sh"

suite() {
	zxfer_test_register_suite_tests "$0"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
