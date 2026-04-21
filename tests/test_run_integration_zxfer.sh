#!/bin/sh
#
# shunit2 tests for the direct integration harness control flow.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_integration"
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	INTEGRATION_HARNESS="$ZXFER_ROOT/tests/run_integration_zxfer.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
	# shellcheck source=tests/run_integration_zxfer.sh
	. "$INTEGRATION_HARNESS"
	ZXFER_LIST_FAILED_TESTS_ONLY=0
	ZXFER_SKIP_TESTS=""
	ZXFER_ONLY_TESTS=""
	ZXFER_KEEP_GOING=0
	ZXFER_ABORT_REQUESTED=0
	ZXFER_FAILED_TESTS=""
	WORKDIR="$TEST_TMPDIR/workdir"
	rm -rf "$WORKDIR"
	mkdir -p "$WORKDIR"
}

tearDown() {
	rm -rf "$WORKDIR"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_parse_args_accepts_failed_tests_only() {
	parse_args --failed-tests-only

	assertEquals "The integration harness should accept failure-only output mode." \
		"1" "$ZXFER_LIST_FAILED_TESTS_ONLY"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_parse_args_accepts_only_test_lists() {
	parse_args --only-test basic_replication_test,force_rollback_test --only-test usage_error_tests

	assertEquals "The integration harness should accept comma-delimited and repeated --only-test selectors." \
		"basic_replication_test force_rollback_test usage_error_tests" "$ZXFER_ONLY_TESTS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_build_requested_test_sequence_filters_to_named_tests() {
	TEST_SEQUENCE="usage_error_tests basic_replication_test force_rollback_test"
	ZXFER_ONLY_TESTS="force_rollback_test,usage_error_tests"

	assertEquals "Requested integration tests should preserve the suite's declared order." \
		"usage_error_tests force_rollback_test" "$(build_requested_test_sequence)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_build_requested_test_sequence_accepts_multiline_declared_test_lists() {
	TEST_SEQUENCE="usage_error_tests \
basic_replication_test \
force_rollback_test"
	ZXFER_ONLY_TESTS="force_rollback_test,usage_error_tests"

	assertEquals "Requested integration tests should validate and preserve order when the declared suite list spans multiple lines." \
		"usage_error_tests force_rollback_test" "$(build_requested_test_sequence)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_build_requested_test_sequence_rejects_unknown_test_names() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
		. \"$INTEGRATION_HARNESS\"
		TEST_SEQUENCE='usage_error_tests basic_replication_test'
		ZXFER_ONLY_TESTS='nosuchtest'
		build_requested_test_sequence
	"

	assertEquals "Unknown --only-test names should fail closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The integration harness should identify the unknown requested test." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Unknown integration test requested via --only-test: nosuchtest"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_harness_declares_remote_parallel_probe_failure_case() {
	harness_contents=$(cat "$INTEGRATION_HARNESS")

	assertContains "The integration harness should define the rendered remote parallel failure integration case." \
		"$harness_contents" "remote_parallel_functional_probe_failure_origin_test()"
	assertContains "The integration harness should keep the rendered remote parallel failure case in the declared test sequence." \
		"$harness_contents" "remote_parallel_functional_probe_failure_origin_test \\"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_run_test_suppresses_passing_output_in_failed_tests_only_mode() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
		. \"$INTEGRATION_HARNESS\"
		ZXFER_LIST_FAILED_TESTS_ONLY=1
		ZXFER_KEEP_GOING=1
		WORKDIR=\"$TEST_TMPDIR/workdir-pass\"
		rm -rf \"\$WORKDIR\"
		mkdir -p \"\$WORKDIR\"
		passing_test() {
			log 'starting synthetic pass'
			printf '%s\n' 'pass-stdout'
			printf '%s\n' 'pass-stderr' >&2
			return 0
		}
		run_test 1 1 passing_test
	"

	assertEquals "Passing tests should still succeed in failure-only mode." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Passing tests should emit the compact completed-status line with the test name in failure-only mode." \
		"[1/1] PASS passing_test" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_run_test_replays_failing_output_in_failed_tests_only_mode() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
		. \"$INTEGRATION_HARNESS\"
		ZXFER_LIST_FAILED_TESTS_ONLY=1
		ZXFER_KEEP_GOING=1
		WORKDIR=\"$TEST_TMPDIR/workdir-fail\"
		rm -rf \"\$WORKDIR\"
		mkdir -p \"\$WORKDIR\"
		failing_test() {
			log 'starting synthetic failure'
			printf '%s\n' 'fail-stdout'
			printf '%s\n' 'fail-stderr' >&2
			return 7
		}
		run_test 2 3 failing_test
		printf 'failed=%s\n' \"\$ZXFER_FAILED_TESTS\"
	"

	assertEquals "Failure-only mode should still let keep-going runs return success from run_test itself." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Failure-only mode should still identify the failing test function." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "[2/3] FAIL"
	assertContains "Failure-only mode should label the replayed stdout block with the failing test name." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--- failing_test stdout ---"
	assertContains "Failure-only mode should replay captured stdout for failing tests." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "fail-stdout"
	assertContains "Failure-only mode should label the replayed stderr block with the failing test name." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--- failing_test stderr ---"
	assertContains "Failure-only mode should replay captured stderr for failing tests." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "fail-stderr"
	assertContains "Failure-only mode should still append the failing test to the summary state." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "failed=failing_test"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
