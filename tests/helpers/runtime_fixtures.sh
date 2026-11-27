#!/bin/sh
# Shared unit fixture for tests/test_zxfer_runtime.sh and session lifecycle
# tests: caller PATH, TMPDIR at the suite directory, and cleared runtime,
# send-job, cleanup-pid, failure, path-security and effective-TMPDIR state.
# shellcheck disable=SC2034,SC2317,SC2329

# Purpose: Clear the runtime-owned state a case may have left behind.
# Usage: zxfer_test_runtime_fixture_setup, from setUp or a test. Set
# TEST_ORIGINAL_PATH=$PATH at the top of the suite before sourcing
# tests/test_helper.sh.
zxfer_test_runtime_fixture_setup() {
	set +e
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_BACKUP_DIR
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_runtime_artifact_state
	zxfer_reset_send_receive_state
	zxfer_reset_cleanup_pid_tracking
	zxfer_reset_failure_context "unit"
	zxfer_reset_path_security_state
	g_option_V_very_verbose=0
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_tmpdir_fallback_note=""
}

# Purpose: Restore the PATH a case may have narrowed.
# Usage: zxfer_test_runtime_fixture_teardown, from tearDown.
zxfer_test_runtime_fixture_teardown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}
