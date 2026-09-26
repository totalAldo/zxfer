#!/bin/sh
# Shared shunit lifecycle, temporary-directory, and default-state helpers.
# shellcheck disable=SC2034,SC2317,SC2329

zxfer_test_create_tmpdir() {
	l_prefix=$1

	TEST_TMPDIR=$(mktemp -d -t "${l_prefix}.XXXXXX") || {
		echo "Unable to create test temp directory with prefix ${l_prefix}." >&2
		exit 1
	}
}

zxfer_test_cleanup_tmpdir() {
	if [ -n "${TEST_TMPDIR:-}" ]; then
		rm -rf "$TEST_TMPDIR"
	fi
}

# Purpose: Remove the registered adjacent artifacts and the run root between
# test cases, then clear the allocation results.
# Usage: zxfer_reset_runtime_artifact_state; returns 1 when any cleanup fails
# and keeps what it could not remove registered. Production code relies on
# zxfer_trap_exit instead.
zxfer_reset_runtime_artifact_state() {
	l_cleanup_status=0
	zxfer_cleanup_registered_runtime_artifacts || l_cleanup_status=$?
	zxfer_remove_run_tmp_root || l_cleanup_status=1
	g_zxfer_run_tmp_counter=0
	g_zxfer_runtime_artifact_path_result=""
	g_zxfer_runtime_artifact_read_result=""
	g_zxfer_temp_file_group_result=""
	return "$l_cleanup_status"
}

# Purpose: Reset every owner module the way zxfer_reset_session_state does at
# startup, then set the default secure PATH and g_cmd_awk.
# Usage: Call first in setUp, then override only what the suite needs. It
# never creates a run root or narrows PATH. A run root an earlier case left is
# removed first; returns 1 when that removal fails, after the full reset.
zxfer_test_reset_all_owner_state() {
	l_owner_reset_status=0
	if [ -n "${g_zxfer_run_tmp_root:-}" ] &&
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_run_tmp_root"; then
		zxfer_reset_runtime_artifact_state || l_owner_reset_status=1
	fi
	# 0 skips the start-clock read the profile reset takes for a -V run.
	g_zxfer_profile_prescan=0
	zxfer_reset_session_state
	unset g_zxfer_profile_prescan
	zxfer_reset_failure_context "unit"
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	zxfer_initialize_dependency_reporting_defaults
	return "$l_owner_reset_status"
}

# Purpose: Allocate a genuine zxfer run root below a disposable test directory.
# Usage: Suites that exercise runtime-artifact cleanup call this from setUp and
# place mocked direct-child artifacts below $g_zxfer_run_tmp_root. This avoids
# forging owner globals, which production correctly refuses to trust.
zxfer_test_allocate_runtime_root() {
	l_test_runtime_parent=$1

	[ -d "$l_test_runtime_parent" ] || return 1
	if [ -n "${g_zxfer_run_tmp_root:-}" ] &&
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_run_tmp_root"; then
		zxfer_reset_runtime_artifact_state || return "$?"
	else
		zxfer_discard_runtime_cleanup_state
	fi

	l_test_runtime_tmpdir_was_set=0
	l_test_runtime_old_tmpdir=""
	if [ "${TMPDIR+x}" = x ]; then
		l_test_runtime_tmpdir_was_set=1
		l_test_runtime_old_tmpdir=$TMPDIR
	fi
	l_test_runtime_old_effective_tmpdir=${g_zxfer_effective_tmpdir:-}
	l_test_runtime_old_effective_request=${g_zxfer_effective_tmpdir_requested:-}

	TMPDIR=$l_test_runtime_parent
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	if zxfer_ensure_run_tmp_root; then
		l_test_runtime_status=0
	else
		l_test_runtime_status=$?
	fi

	if [ "$l_test_runtime_tmpdir_was_set" -eq 1 ]; then
		TMPDIR=$l_test_runtime_old_tmpdir
	else
		unset TMPDIR
	fi
	g_zxfer_effective_tmpdir=$l_test_runtime_old_effective_tmpdir
	g_zxfer_effective_tmpdir_requested=$l_test_runtime_old_effective_request

	return "$l_test_runtime_status"
}

# Purpose: Replace zxfer_throw_error with a stub that prints the message to
# stdout and exits, so a subshell capture sees what would have been thrown.
# Usage: zxfer_test_stub_throw_error_to_stdout [status]; the stub exits 1, or
# with "status" it exits with the thrown status (default 1). Call it inside
# the subshell under test.
zxfer_test_stub_throw_error_to_stdout() {
	if [ "${1:-}" = status ]; then
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit "${2:-1}"
		}
	else
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
	fi
}

# Most suites only need zxfer_usage() to exist so zxfer_throw_usage_error() has a target.
zxfer_usage() {
	:
}

oneTimeSetUp() {
	:
}

oneTimeTearDown() {
	:
}

setUp() {
	:
}

tearDown() {
	:
}

# Provide sane defaults for globals that zxfer helpers expect.
: "${g_option_n_dryrun:=0}"
: "${g_option_v_verbose:=0}"
: "${g_option_V_very_verbose:=0}"
: "${g_option_b_beep_always:=0}"
: "${g_option_B_beep_on_success:=0}"
