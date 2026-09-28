#!/bin/sh
# The unit fixture of tests/test_zxfer_runtime.sh, shared with the fragments
# that were written for it and now live in their own module's home: the
# caller's PATH, TMPDIR at the suite directory, and cleared runtime-artifact,
# send-job, cleanup-pid, failure and effective-TMPDIR state. An entry whose own
# cases need another fixture applies this one to those fragments only,
# through zxfer_test_running_test_is_in in its setUp.
# shellcheck disable=SC2034,SC2317,SC2329

# Purpose: Clear the runtime-owned state a case may have left behind.
# Usage: zxfer_test_runtime_fixture_setup, from setUp. Set
# TEST_ORIGINAL_PATH=$PATH at the top of the entry before sourcing
# tests/test_helper.sh.
zxfer_test_runtime_fixture_setup() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_BACKUP_DIR
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_runtime_artifact_state
	zxfer_reset_send_job_state
	zxfer_reset_cleanup_pid_tracking
	zxfer_reset_failure_context "unit"
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
}

# Purpose: Restore the PATH a case may have narrowed.
# Usage: zxfer_test_runtime_fixture_teardown, from tearDown.
zxfer_test_runtime_fixture_teardown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}
