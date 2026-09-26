#!/bin/sh
#
# Stable shunit2 entry point for snapshot producers, remote discovery batches,
# and snapshot-discovery orchestration behavior.
#
# Test definitions live in the behavior fragments below. Each fragment has a
# "zxfer-test-fragment" marker, a source line and a path in suite(); keep the
# three in the same order so listing and execution agree.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/fixtures/snapshot_discovery/fixture.sh
. "$TESTS_DIR/fixtures/snapshot_discovery/fixture.sh"

# zxfer-test-fragment: fixtures/snapshot_discovery/source_producer_cases.sh
# shellcheck source=tests/fixtures/snapshot_discovery/source_producer_cases.sh
. "$TESTS_DIR/fixtures/snapshot_discovery/source_producer_cases.sh"

# zxfer-test-fragment: fixtures/snapshot_discovery/snapshot_stream_cases.sh
# shellcheck source=tests/fixtures/snapshot_discovery/snapshot_stream_cases.sh
. "$TESTS_DIR/fixtures/snapshot_discovery/snapshot_stream_cases.sh"

# zxfer-test-fragment: fixtures/snapshot_discovery/full_discovery_cases.sh
# shellcheck source=tests/fixtures/snapshot_discovery/full_discovery_cases.sh
. "$TESTS_DIR/fixtures/snapshot_discovery/full_discovery_cases.sh"

# zxfer-test-fragment: fixtures/snapshot_discovery/remote_batch_cases.sh
# shellcheck source=tests/fixtures/snapshot_discovery/remote_batch_cases.sh
. "$TESTS_DIR/fixtures/snapshot_discovery/remote_batch_cases.sh"

# zxfer-test-fragment: fixtures/snapshot_discovery/dry_run_failure_cases.sh
# shellcheck source=tests/fixtures/snapshot_discovery/dry_run_failure_cases.sh
. "$TESTS_DIR/fixtures/snapshot_discovery/dry_run_failure_cases.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_snapshot_discovery.sh" \
		"$TESTS_DIR/fixtures/snapshot_discovery/source_producer_cases.sh" \
		"$TESTS_DIR/fixtures/snapshot_discovery/snapshot_stream_cases.sh" \
		"$TESTS_DIR/fixtures/snapshot_discovery/full_discovery_cases.sh" \
		"$TESTS_DIR/fixtures/snapshot_discovery/remote_batch_cases.sh" \
		"$TESTS_DIR/fixtures/snapshot_discovery/dry_run_failure_cases.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
