#!/bin/sh
#
# shunit2 tests for the shunit2 runner script.
#

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_run_shunit_tests.XXXXXX)
	RUN_SHUNIT_TESTS_BIN="$ZXFER_ROOT/tests/run_shunit_tests.sh"
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	FAKE_SUITE_LOG="$TEST_TMPDIR/fake-suite.log"
	FAKE_TEST_SHELL_LOG="$TEST_TMPDIR/fake-test-shell.log"
	: >"$FAKE_SUITE_LOG"
	: >"$FAKE_TEST_SHELL_LOG"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_suite() {
	l_suite_path=$1
	l_marker=$2
	cat >"$l_suite_path" <<EOF
#!/bin/sh
printf '%s\n' "$l_marker" >>"\${FAKE_SUITE_LOG:?}"
exit 0
EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_test_shell() {
	l_shell_path=$1
	cat >"$l_shell_path" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >>"${FAKE_TEST_SHELL_LOG:?}"
exec /bin/sh "$@"
EOF
	chmod +x "$l_shell_path"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_runs_explicit_suite_directly_by_default() {
	l_suite_path="$TEST_TMPDIR/default-suite.sh"
	write_fake_suite "$l_suite_path" "direct-default"
	chmod +x "$l_suite_path"

	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			TMPDIR="${TMPDIR:-/tmp}" \
			FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" "$l_suite_path"
	)

	assertContains "The runner should execute explicit suites successfully with the default direct-exec path." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertContains "The default banner should not mention an alternate shell when ZXFER_TEST_SHELL is unset." \
		"$output" "==> Running shunit2 suite: $l_suite_path"
	assertEquals "The fake suite should run once through its own shebang when no alternate shell is configured." \
		"direct-default" "$(cat "$FAKE_SUITE_LOG")"
	assertEquals "The fake alternate-shell log should remain empty during the default dispatch path." \
		"" "$(cat "$FAKE_TEST_SHELL_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_uses_zxfer_test_shell_for_non_executable_suite() {
	l_suite_path="$TEST_TMPDIR/nonexec-suite.sh"
	l_shell_path="$TEST_TMPDIR/fake-test-shell"
	write_fake_suite "$l_suite_path" "runner-dispatch"
	chmod 644 "$l_suite_path"
	write_fake_test_shell "$l_shell_path"

	output=$(
		ZXFER_TEST_SHELL="$l_shell_path" \
			FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			FAKE_TEST_SHELL_LOG="$FAKE_TEST_SHELL_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" "$l_suite_path"
	)

	assertContains "The runner banner should include the configured alternate shell." \
		"$output" "with test shell [$l_shell_path]"
	assertContains "The alternate-shell dispatch path should still report a passing suite summary." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "The fake suite should run under the configured alternate shell even when it is not executable." \
		"runner-dispatch" "$(cat "$FAKE_SUITE_LOG")"
	assertEquals "The alternate shell should receive the target suite path as its sole argument." \
		"$l_suite_path" "$(cat "$FAKE_TEST_SHELL_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_missing_zxfer_test_shell() {
	l_suite_path="$TEST_TMPDIR/missing-shell-suite.sh"
	write_fake_suite "$l_suite_path" "missing-shell"
	chmod +x "$l_suite_path"

	set +e
	output=$(
		ZXFER_TEST_SHELL="$TEST_TMPDIR/does-not-exist" \
			FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" "$l_suite_path" 2>&1
	)
	status=$?
	set -e

	assertEquals "A missing alternate shell should fail before any suites are run." 1 "$status"
	assertContains "The runner should surface a clear error when ZXFER_TEST_SHELL cannot be executed." \
		"$output" "ZXFER_TEST_SHELL is not executable: $TEST_TMPDIR/does-not-exist"
	assertEquals "The fake suite should not run when the alternate shell is invalid." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
