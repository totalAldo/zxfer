#!/bin/sh
#
# shunit2 entry point for src/zxfer_snapshot_discovery.sh, and for
# src/zxfer_remote_snapshot_discovery.sh, whose remote destination batch the
# remote-batch fragment pins together with its discovery adaptors.
#
# Test definitions live in the behavior fragments below. Each fragment has a
# "zxfer-test-fragment" marker, a source line and a path in suite(); keep the
# three in the same order so listing and execution agree.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/snapshot_discovery_fixtures.sh
. "$TESTS_DIR/helpers/snapshot_discovery_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_get_list"
	zxfer_test_snapshot_discovery_fixture_write_tools
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_snapshot_discovery_fixture_setup
}

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_stream_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_stream_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_stream_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_full_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_full_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_full_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_remote_batch_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_remote_batch_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_remote_batch_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_failure_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_failure_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_failure_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_snapshot_discovery.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_stream_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_full_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_remote_batch_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_failure_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
