#!/bin/sh
#
# shunit2 tests for zxfer_send_receive.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_send_receive"
	TEST_PS_PATH=$(command -v ps 2>/dev/null || printf '%s\n' ps)
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	zxfer_test_reset_all_owner_state || return
	TMPDIR="$TEST_TMPDIR"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ps=$TEST_PS_PATH
	# Distinct local and endpoint codecs show which one a rendered pipeline used.
	g_cmd_compress_safe="gzip"
	g_cmd_decompress_safe="gunzip"
	g_origin_cmd_compress_safe="remote-gzip"
	g_target_cmd_decompress_safe="target-gunzip"
	g_zxfer_send_job_abort_grace_seconds=0
}

# Behavior-focused fragments keep this stable suite entry point while
# bounding the amount of test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_send_receive_command_progress_tests.sh
# shellcheck source=tests/suites/zxfer_send_receive_command_progress_tests.sh
. "$TESTS_DIR/suites/zxfer_send_receive_command_progress_tests.sh"
# zxfer-test-fragment: suites/zxfer_send_receive_pipeline_tests.sh
# shellcheck source=tests/suites/zxfer_send_receive_pipeline_tests.sh
. "$TESTS_DIR/suites/zxfer_send_receive_pipeline_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_send_receive.sh" \
		"$TESTS_DIR/suites/zxfer_send_receive_command_progress_tests.sh" \
		"$TESTS_DIR/suites/zxfer_send_receive_pipeline_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
