#!/bin/sh
# The unit fixture of tests/test_zxfer_send_jobs.sh, shared with the
# background-shell fragment of tests/test_zxfer_exec.sh: quiet options, two
# jobs, a fresh run root, and cleared send-job, spawn-mode and failure state.
# shellcheck disable=SC2034,SC2317,SC2329

# Purpose: Reset the job supervisor and the background-shell spawn mode.
# Usage: zxfer_test_send_job_fixture_setup, from setUp.
zxfer_test_send_job_fixture_setup() {
	TMPDIR="$TEST_TMPDIR"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_j_jobs=2
	g_option_T_target_host=""
	zxfer_reset_runtime_artifact_state
	zxfer_ensure_run_tmp_root || fail "Unable to create the send-job test run root."
	zxfer_reset_send_job_state
	zxfer_reset_background_shell_spawn_mode
	g_zxfer_send_job_abort_grace_seconds=0
	zxfer_reset_failure_context "unit"
}

# Purpose: Poll until a pid is gone so signal races stay bounded.
# Usage: bgjob_test_wait_for_pid_exit PID; fails when PID outlives ten seconds.
bgjob_test_wait_for_pid_exit() {
	l_wait_pid=$1
	l_wait_tries=0
	while kill -s 0 "$l_wait_pid" 2>/dev/null && [ "$l_wait_tries" -lt 100 ]; do
		sleep 0.1 2>/dev/null || sleep 1
		l_wait_tries=$((l_wait_tries + 1))
	done
	! kill -s 0 "$l_wait_pid" 2>/dev/null
}
