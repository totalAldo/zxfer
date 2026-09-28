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
# shellcheck source=tests/helpers/replication_fixtures.sh
. "$TESTS_DIR/helpers/replication_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_replication"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_replication_fixture_setup
}

# Behavior-focused fragments keep the stable suite entry point while bounding
# the amount of replication test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_replication_setup_tests.sh
# shellcheck source=tests/suites/zxfer_replication_setup_tests.sh
. "$TESTS_DIR/suites/zxfer_replication_setup_tests.sh"

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
		"$TESTS_DIR/suites/zxfer_replication_setup_tests.sh" \
		"$TESTS_DIR/suites/zxfer_replication_snapshot_transfer_tests.sh" \
		"$TESTS_DIR/suites/zxfer_replication_orchestration_tests.sh" \
		"$TESTS_DIR/suites/zxfer_replication_queue_failure_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
