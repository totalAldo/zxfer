#!/bin/sh
#
# shunit2 tests for CI helpers that preserve vmactions guest artifacts.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_ci_vmactions"
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	CI_RUNNER="$ZXFER_ROOT/tests/ci/run_vmactions_integration.sh"
	CI_STATUS_CHECK="$ZXFER_ROOT/tests/ci/check_vmactions_integration_status.sh"
	CI_FREEBSD_PREPARE="$ZXFER_ROOT/tests/ci/prepare_freebsd_vmactions_integration.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
create_fake_ci_repo() {
	l_status=$1
	l_repo=$2

	mkdir -p "$l_repo/tests" || fail "Unable to create fake CI repository."
	cat <<EOF >"$l_repo/tests/run_integration_zxfer.sh"
#!/bin/sh
printf '%s\n' "fake integration harness"
exit $l_status
EOF
	chmod 700 "$l_repo/tests/run_integration_zxfer.sh" ||
		fail "Unable to chmod fake integration harness."
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_vmactions_integration_records_failure_status_but_returns_success() {
	fake_repo="$TEST_TMPDIR/fake-repo-failure"
	artifact_dir="$TEST_TMPDIR/freebsd-artifacts"
	create_fake_ci_repo 7 "$fake_repo"

	zxfer_test_capture_subshell "
		cd \"$fake_repo\"
		TMPDIR=\"$artifact_dir\" sh \"$CI_RUNNER\" sh
		printf 'stored-status=%s\n' \"\$(cat \"$artifact_dir/harness.exit-status\")\"
	"

	assertEquals "The vmactions runner helper should return success so copyback can run." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The helper should record the real harness exit status for the host-side check." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "stored-status=7"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_vmactions_integration_records_prepare_failure_status_but_returns_success() {
	fake_repo="$TEST_TMPDIR/fake-repo-prepare-failure"
	artifact_dir="$TEST_TMPDIR/freebsd-prepare-artifacts"
	create_fake_ci_repo 0 "$fake_repo"

	zxfer_test_capture_subshell "
		cd \"$fake_repo\"
		TMPDIR=\"$artifact_dir\" ZXFER_CI_GUEST_PREPARE='exit 12' sh \"$CI_RUNNER\" sh
		printf 'stored-status=%s\n' \"\$(cat \"$artifact_dir/harness.exit-status\")\"
	"

	assertEquals "The vmactions runner helper should still return success when guest preparation fails so copyback can run." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The helper should record guest preparation failures for the host-side check." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "stored-status=12"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_check_vmactions_integration_status_restores_guest_failure() {
	artifact_dir="$TEST_TMPDIR/status-check-failure"
	mkdir -p "$artifact_dir" || fail "Unable to create artifact fixture."
	printf '%s\n' "7" >"$artifact_dir/harness.exit-status" ||
		fail "Unable to create status fixture."

	zxfer_test_capture_subshell "
		sh \"$CI_STATUS_CHECK\" \"$artifact_dir\" FreeBSD
	"

	assertEquals "The host-side status check should fail with the guest harness status." \
		7 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The status check should emit a GitHub annotation with the guest label." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "FreeBSD integration failed inside the VM with exit status 7"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_check_vmactions_integration_status_reports_missing_copyback_status() {
	artifact_dir="$TEST_TMPDIR/status-check-missing"
	mkdir -p "$artifact_dir" || fail "Unable to create artifact fixture."

	zxfer_test_capture_subshell "
		sh \"$CI_STATUS_CHECK\" \"$artifact_dir\" FreeBSD
	"

	assertEquals "Missing copyback status should fail closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The status check should explain that copyback did not produce a status file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "No harness status file was copied back"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
create_fake_freebsd_pkg_tools() {
	l_mock_bin=$1
	l_pkg_body=$2

	mkdir -p "$l_mock_bin" || fail "Unable to create fake FreeBSD pkg tool directory."
	cat >"$l_mock_bin/pkg" <<EOF
#!/bin/sh
$l_pkg_body
EOF
	chmod 700 "$l_mock_bin/pkg" || fail "Unable to chmod fake pkg."

	cat <<'EOF' >"$l_mock_bin/kldload"
#!/bin/sh
exit 0
EOF
	chmod 700 "$l_mock_bin/kldload" || fail "Unable to chmod fake kldload."
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_prepare_freebsd_vmactions_integration_retries_pkg_install_after_clearing_state() {
	mock_bin="$TEST_TMPDIR/freebsd-prep-retry-bin"
	repos_dir="$TEST_TMPDIR/freebsd-prep-retry-repos"
	cache_dir="$TEST_TMPDIR/freebsd-prep-retry-cache"
	pkg_count_file="$TEST_TMPDIR/freebsd-prep-retry-count"
	mkdir -p "$repos_dir" "$cache_dir" || fail "Unable to create fake pkg state."

	create_fake_freebsd_pkg_tools "$mock_bin" "
printf '%s\n' \"\$*\" >>\"$TEST_TMPDIR/freebsd-prep-retry-pkg.log\"
if [ \"\$1\" = install ]; then
	count=0
	if [ -r \"$pkg_count_file\" ]; then
		count=\$(cat \"$pkg_count_file\")
	fi
	count=\$((count + 1))
	printf '%s\n' \"\$count\" >\"$pkg_count_file\"
	if [ \"\$count\" -eq 1 ]; then
		exit 9
	fi
fi
exit 0
"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\" \
		ZXFER_CI_FREEBSD_PKG_REPOS_DIR=\"$repos_dir\" \
		ZXFER_CI_FREEBSD_PKG_CACHE_DIR=\"$cache_dir\" \
		sh \"$CI_FREEBSD_PREPARE\"
		printf 'install-count=%s\n' \"\$(cat \"$pkg_count_file\")\"
	"

	assertEquals "FreeBSD CI preparation should recover after one failed package install." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The FreeBSD prep helper should retry the failed install once." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "install-count=2"
	assertContains "The FreeBSD prep helper should announce the retry." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "retrying once"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_prepare_freebsd_vmactions_integration_allows_degraded_package_setup_by_default() {
	mock_bin="$TEST_TMPDIR/freebsd-prep-degraded-bin"
	repos_dir="$TEST_TMPDIR/freebsd-prep-degraded-repos"
	cache_dir="$TEST_TMPDIR/freebsd-prep-degraded-cache"
	mkdir -p "$repos_dir" "$cache_dir" || fail "Unable to create fake pkg state."

	create_fake_freebsd_pkg_tools "$mock_bin" "
if [ \"\$1\" = install ]; then
	exit 4
fi
exit 0
"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\" \
		ZXFER_CI_FREEBSD_PKG_REPOS_DIR=\"$repos_dir\" \
		ZXFER_CI_FREEBSD_PKG_CACHE_DIR=\"$cache_dir\" \
		sh \"$CI_FREEBSD_PREPARE\"
	"

	assertEquals "FreeBSD CI preparation should allow reduced coverage when optional package install remains unavailable." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The FreeBSD prep helper should make reduced coverage explicit." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "continuing with reduced integration coverage"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_prepare_freebsd_vmactions_integration_can_require_package_setup_success() {
	mock_bin="$TEST_TMPDIR/freebsd-prep-strict-bin"
	repos_dir="$TEST_TMPDIR/freebsd-prep-strict-repos"
	cache_dir="$TEST_TMPDIR/freebsd-prep-strict-cache"
	mkdir -p "$repos_dir" "$cache_dir" || fail "Unable to create fake pkg state."

	create_fake_freebsd_pkg_tools "$mock_bin" "
if [ \"\$1\" = install ]; then
	exit 6
fi
exit 0
"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\" \
		ZXFER_CI_FREEBSD_PKG_REPOS_DIR=\"$repos_dir\" \
		ZXFER_CI_FREEBSD_PKG_CACHE_DIR=\"$cache_dir\" \
		ZXFER_CI_FREEBSD_ALLOW_DEGRADED_PACKAGES=0 \
		sh \"$CI_FREEBSD_PREPARE\"
	"

	assertEquals "Strict FreeBSD CI preparation should preserve the final package install failure." \
		6 "$ZXFER_TEST_CAPTURE_STATUS"
	assertNotContains "Strict FreeBSD CI preparation should not report reduced coverage continuation." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "continuing with reduced integration coverage"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
