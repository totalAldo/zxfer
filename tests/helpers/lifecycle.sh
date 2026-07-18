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
