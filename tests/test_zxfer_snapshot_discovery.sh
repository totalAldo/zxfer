#!/bin/sh
#
# shunit2 entry point for src/zxfer_snapshot_discovery.sh: the fast no-op
# proof, full discovery, deltas and record files, the source and destination
# listing commands, parallel discovery, staged capture files and
# destination-list normalization.
#
# Test definitions live in the behavior fragments below. Each fragment has a
# "zxfer-test-fragment" marker, a source line and a path in suite(); keep the
# three in the same order so listing and execution agree. The commands
# fragment keeps the exec fixture it was written for; the others use the
# snapshot-discovery fixture.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/snapshot_discovery_fixtures.sh
. "$TESTS_DIR/helpers/snapshot_discovery_fixtures.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_snapshot_discovery"
	zxfer_test_exec_fixture_one_time_setup
	zxfer_test_snapshot_discovery_fixture_write_tools
}

oneTimeTearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_snapshot_discovery_commands_tests.sh"; then
		# Some source and destination cases stub src functions in the
		# current shell; these cases were written against the real ones.
		zxfer_source_modules_for_tests "$ZXFER_ROOT"
		zxfer_test_exec_fixture_setup
		return
	fi
	# The exec fixture empties TEST_TMPDIR and writes its own ssh stand-in.
	zxfer_test_snapshot_discovery_fixture_write_tools
	zxfer_test_snapshot_discovery_fixture_setup
}

tearDown() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_snapshot_discovery_commands_tests.sh"; then
		relax_test_tmpdir_permissions
	fi
}

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_stream_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_stream_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_stream_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_full_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_full_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_full_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_failure_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_failure_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_failure_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_source_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_source_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_source_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_destination_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_destination_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_destination_tests.sh"

# zxfer-test-fragment: suites/zxfer_snapshot_discovery_commands_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_discovery_commands_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_discovery_commands_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_snapshot_discovery.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_stream_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_full_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_failure_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_source_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_destination_tests.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_discovery_commands_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
