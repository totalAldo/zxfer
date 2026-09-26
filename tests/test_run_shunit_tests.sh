#!/bin/sh
#
# shunit2 tests for tests/run_shunit_tests.sh, the shared test helper, and the
# local changes to the vendored shunit2.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_shunit_tests"
	RUN_SHUNIT_TESTS_BIN="$ZXFER_ROOT/tests/run_shunit_tests.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	FAKE_SUITE_LOG="$TEST_TMPDIR/fake-suite.log"
	FAKE_TEST_SHELL_LOG="$TEST_TMPDIR/fake-test-shell.log"
	FAKE_SUITE_STARTED="$TEST_TMPDIR/fake-suite.started"
	FAKE_SUITE_RELEASE="$TEST_TMPDIR/fake-suite.release"
	RUN_SHUNIT_TESTS_WAIT_LIMIT=${ZXFER_RUN_SHUNIT_TESTS_WAIT_LIMIT:-15}
	: >"$FAKE_SUITE_LOG"
	: >"$FAKE_TEST_SHELL_LOG"
	rm -f "$FAKE_SUITE_STARTED" "$FAKE_SUITE_RELEASE"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_suite() {
	l_fake_suite_path=$1
	l_fake_suite_marker=$2
	cat >"$l_fake_suite_path" <<EOF
#!/bin/sh
printf '%s\n' "$l_fake_suite_marker" >>"\${FAKE_SUITE_LOG:?}"
exit 0
EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_suite_with_body() {
	l_fake_suite_path=$1
	cat >"$l_fake_suite_path"
}

# Generate named fake functions at test runtime. Keeping the definitions out of
# this suite's literal source prevents shunit2 from discovering fixture-only
# names as tests of tests/test_run_shunit_tests.sh itself.
# shellcheck disable=SC2016,SC2317,SC2329  # Generates runtime-expanded fixture source; invoked indirectly by shunit2.
write_fake_named_suite() {
	l_fake_suite_path=$1
	l_fake_suite_marker=$2
	shift 2
	case "$l_fake_suite_marker" in
	'' | *[!A-Za-z0-9_-]*) fail "Invalid fake suite marker: $l_fake_suite_marker" ;;
	esac
	{
		printf '%s\n' '#!/bin/sh'
		for l_fake_test_name in "$@"; do
			case "$l_fake_test_name" in
			test*) ;;
			*) fail "Invalid fake test name: $l_fake_test_name" ;;
			esac
			case "$l_fake_test_name" in
			'' | [!A-Za-z_]* | *[!A-Za-z0-9_]*)
				fail "Invalid fake test name: $l_fake_test_name"
				;;
			esac
			printf '%s() {\n' "$l_fake_test_name"
			printf '\t:\n'
			printf '%s\n' '}'
		done
		printf "FAKE_NAMED_SUITE_MARKER='%s'\n" "$l_fake_suite_marker"
		printf '%s\n' 'printf "%s" "$FAKE_NAMED_SUITE_MARKER" >>"${FAKE_SUITE_LOG:?}"'
		printf '%s\n' 'printf ":%s" "$@" >>"${FAKE_SUITE_LOG:?}"'
		printf '%s\n' 'printf "\n" >>"${FAKE_SUITE_LOG:?}"'
	} >"$l_fake_suite_path"
	chmod +x "$l_fake_suite_path"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_test_shell() {
	l_fake_shell_path=$1
	cat >"$l_fake_shell_path" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >>"${FAKE_TEST_SHELL_LOG:?}"
exec /bin/sh "$@"
EOF
	chmod +x "$l_fake_shell_path"
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
test_run_shunit_tests_lists_suites_and_named_tests_without_running_them() {
	l_suite_path="$TEST_TMPDIR/listable-suite.sh"
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' 'test_first_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
		printf '%s\n' 'helper_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
		printf '%s\n' 'test_second_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
	} >"$l_suite_path"
	chmod +x "$l_suite_path"

	suite_output=$("$RUN_SHUNIT_TESTS_BIN" --list-suites "$l_suite_path")
	compat_suite_output=$("$RUN_SHUNIT_TESTS_BIN" --list "$l_suite_path")
	test_output=$("$RUN_SHUNIT_TESTS_BIN" --list-tests "$l_suite_path")

	assertEquals "Suite listing should print the resolved explicit suite once." \
		"$l_suite_path" "$suite_output"
	assertEquals "The original --list spelling should remain a compatibility alias for --list-suites." \
		"$suite_output" "$compat_suite_output"
	assertContains "Named-test listing should identify the first test and its suite." \
		"$test_output" "$l_suite_path	test_first_case"
	assertContains "Named-test listing should identify every test function." \
		"$test_output" "$l_suite_path	test_second_case"
	assertNotContains "Named-test listing should exclude non-test helpers." \
		"$test_output" "helper_case"
	assertEquals "Listing should not execute the selected suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_lists_named_tests_from_declared_behavior_fragments() {
	l_suite_path="$TEST_TMPDIR/fragmented-suite.sh"
	l_fragment_path="$TEST_TMPDIR/fragmented-suite-cases.sh"
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' '# zxfer-test-fragment: fragmented-suite-cases.sh'
		printf '%s\n' 'test_main_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
	} >"$l_suite_path"
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' 'test_fragment_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
		printf '%s\n' 'fragment_helper() {'
		printf '\t:\n'
		printf '%s\n' '}'
	} >"$l_fragment_path"
	chmod +x "$l_suite_path"

	test_output=$("$RUN_SHUNIT_TESTS_BIN" --list-tests "$l_suite_path")

	assertContains "Fragment-aware listing should preserve tests defined in the stable suite entry point." \
		"$test_output" "$l_suite_path	test_main_case"
	assertContains "Fragment-aware listing should include tests from every declared behavior fragment." \
		"$test_output" "$l_suite_path	test_fragment_case"
	assertNotContains "Fragment-aware listing should continue to exclude non-test helpers." \
		"$test_output" "fragment_helper"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_list_tests_requires_one_explicit_suite() {
	l_first_suite="$TEST_TMPDIR/list-first-suite.sh"
	l_second_suite="$TEST_TMPDIR/list-second-suite.sh"
	write_fake_suite "$l_first_suite" "list-first"
	write_fake_suite "$l_second_suite" "list-second"
	chmod +x "$l_first_suite" "$l_second_suite"

	zxfer_test_capture_subshell "
		FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \\
		\"$RUN_SHUNIT_TESTS_BIN\" --list-tests
	"

	assertEquals "Named-test listing should reject an omitted suite instead of expanding to the full suite set." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The missing-suite error should explain the single-suite contract." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--list-tests requires exactly one explicit suite; found 0."

	zxfer_test_capture_subshell "
		FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \\
		\"$RUN_SHUNIT_TESTS_BIN\" --list-tests \\
		\"$l_first_suite\" \"$l_second_suite\"
	"

	assertEquals "Named-test listing should reject ambiguous multi-suite selection." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The multi-suite error should report how many suites were supplied." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--list-tests requires exactly one explicit suite; found 2."
	assertEquals "Rejected named-test listing must not execute either suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_forwards_named_tests_to_one_suite() {
	l_suite_path="$TEST_TMPDIR/named-suite.sh"
	write_fake_named_suite "$l_suite_path" named \
		test_first_case test_second_case

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" \
			--suite "$l_suite_path" \
			--test test_first_case \
			--test test_second_case
	)

	assertContains "Named test execution should preserve the passing suite summary." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "The runner should use shunit2's documented -- separator before forwarding selected test names." \
		"named:--:test_first_case:test_second_case" \
		"$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_preserves_positional_suite_named_test_selection() {
	l_suite_path="$TEST_TMPDIR/named-positional-suite.sh"
	write_fake_named_suite "$l_suite_path" positional \
		test_positional_one test_positional_two

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 \
			--test test_positional_one \
			--test test_positional_two \
			"$l_suite_path"
	)

	assertContains "The legacy positional-suite form should retain named-test selection." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Positional named-test selection should preserve requested order and shunit2's separator." \
		"positional:--:test_positional_one:test_positional_two" \
		"$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_runs_cross_suite_named_selections_in_first_suite_order() {
	l_first_suite="$TEST_TMPDIR/named-cross-first.sh"
	l_second_suite="$TEST_TMPDIR/named-cross-second.sh"
	write_fake_named_suite "$l_first_suite" first \
		test_first_one test_first_two
	write_fake_named_suite "$l_second_suite" second test_second_one

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 \
			--suite "$l_first_suite" \
			--test test_first_one \
			--test test_first_two \
			--suite "$l_second_suite" \
			--test test_second_one
	)

	assertContains "Cross-suite named selection should report both suites as passing." \
		"$output" "==> shunit2 summary: 2 passed, 0 failed"
	assertEquals "Each suite should run once in first-selection order with only its associated names." \
		"first:--:test_first_one:test_first_two
second:--:test_second_one" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_merges_duplicate_suite_selectors_without_reordering_tests() {
	l_suite_path="$TEST_TMPDIR/named-duplicate-suite.sh"
	write_fake_named_suite "$l_suite_path" duplicate \
		test_duplicate_one test_duplicate_two

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 \
			--suite "$l_suite_path" --test test_duplicate_one \
			--suite "$l_suite_path" --test test_duplicate_two
	)

	assertContains "A repeated suite selector should still count as one suite execution." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Duplicate suite selectors should merge ordered names into one invocation." \
		"duplicate:--:test_duplicate_one:test_duplicate_two" \
		"$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_unknown_named_test_before_starting_any_suite() {
	l_first_suite="$TEST_TMPDIR/named-known-suite.sh"
	l_second_suite="$TEST_TMPDIR/named-unknown-suite.sh"
	write_fake_named_suite "$l_first_suite" first test_known_first
	write_fake_named_suite "$l_second_suite" second test_known_second

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 \
			--suite "$l_first_suite" --test test_known_first \
			--suite "$l_second_suite" --test test_missing 2>&1
	)
	status=$?

	assertEquals "An unknown selected name should fail preflight." 1 "$status"
	assertContains "The preflight failure should identify the exact suite and unknown test." \
		"$output" "Unknown test for $l_second_suite: test_missing"
	assertEquals "Named-test validation must finish before launching even an earlier valid suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_test_option_before_its_suite() {
	l_suite_path="$TEST_TMPDIR/named-orphan-suite.sh"
	write_fake_named_suite "$l_suite_path" orphan test_orphan

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" \
			--test test_orphan --suite "$l_suite_path" 2>&1
	)
	status=$?

	assertEquals "A --test without a current suite should fail closed in option-selector mode." \
		1 "$status"
	assertContains "The orphan-test diagnostic should require ordering the suite first." \
		"$output" "--test must follow the --suite it selects."
	assertEquals "An orphan named-test option must not launch its later suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_named_tests_without_one_suite() {
	l_first_suite="$TEST_TMPDIR/named-first-suite.sh"
	l_second_suite="$TEST_TMPDIR/named-second-suite.sh"
	write_fake_suite "$l_first_suite" "named-first"
	write_fake_suite "$l_second_suite" "named-second"
	chmod +x "$l_first_suite" "$l_second_suite"

	zxfer_test_capture_subshell "
		FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \
		\"$RUN_SHUNIT_TESTS_BIN\" --test test_case \
		\"$l_first_suite\" \"$l_second_suite\"
	"

	assertEquals "Named test selection should fail before launching an ambiguous multi-suite run." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should explain the one-suite requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--test requires exactly one runnable suite; found 2."
	assertEquals "Rejected named selection should not execute either suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_non_positive_and_nonnumeric_jobs() {
	l_suite_path="$TEST_TMPDIR/jobs-suite.sh"
	write_fake_suite "$l_suite_path" "jobs"
	chmod +x "$l_suite_path"

	zxfer_test_capture_subshell "
		\"$RUN_SHUNIT_TESTS_BIN\" --jobs 0 \"$l_suite_path\"
	"

	assertEquals "A zero job count should fail before any suites are launched." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A zero job count should report the positive-integer requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--jobs must be a positive integer"

	zxfer_test_capture_subshell "
		\"$RUN_SHUNIT_TESTS_BIN\" --jobs nope \"$l_suite_path\"
	"

	assertEquals "A nonnumeric job count should fail before any suites are launched." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A nonnumeric job count should report the positive-integer requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--jobs must be a positive integer"
	assertEquals "Rejected job-count parsing should not run the fake suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_limits_explicit_jobs_to_runnable_suite_count() {
	l_suite_path="$TEST_TMPDIR/clamped-jobs-suite.sh"
	write_fake_suite "$l_suite_path" "clamped"
	chmod +x "$l_suite_path"

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 4 "$l_suite_path"
	)

	assertContains "The runner should announce when an explicit job count exceeds the number of runnable suites." \
		"$output" "==> Requested 4 shunit2 jobs, but only 1 runnable suite is available; limiting to 1."
	assertContains "Clamped job counts should still run the suite successfully." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Clamped job counts should still execute the requested suite exactly once." \
		"clamped" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_replays_parallel_output_in_suite_order() {
	l_slow_suite="$TEST_TMPDIR/slow-suite.sh"
	l_fast_suite="$TEST_TMPDIR/fast-suite.sh"

	write_fake_suite_with_body "$l_slow_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "slow-start"
	printf '%s\n' "slow-start" >>"${FAKE_SUITE_LOG:?}"
	: >"${FAKE_SUITE_STARTED:?}"
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_RELEASE:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_RELEASE:?}" ]; then
		printf '%s\n' "slow-timeout" >&2
		exit 9
	fi
	printf '%s\n' "slow-end"
	printf '%s\n' "slow-end" >>"${FAKE_SUITE_LOG:?}"
EOF
	chmod +x "$l_slow_suite"

	write_fake_suite_with_body "$l_fast_suite" <<'EOF'
#!/bin/sh
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_STARTED:?}" ] && [ "$l_wait_count" -lt 10 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_STARTED:?}" ]; then
		printf '%s\n' "fast-missed-slow" >&2
		exit 7
	fi
	: >"${FAKE_SUITE_RELEASE:?}"
	printf '%s\n' "fast-sees-slow"
	printf '%s\n' "fast-sees-slow" >>"${FAKE_SUITE_LOG:?}"
EOF
	chmod +x "$l_fast_suite"

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			FAKE_SUITE_STARTED="$FAKE_SUITE_STARTED" \
			FAKE_SUITE_RELEASE="$FAKE_SUITE_RELEASE" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_slow_suite" "$l_fast_suite"
	)

	assertContains "Parallel suite runs should still report a passing summary when every suite succeeds." \
		"$output" "==> shunit2 summary: 2 passed, 0 failed"
	case "$output" in
	*"==> Running shunit2 suite: $l_slow_suite"**"slow-start"**"slow-end"**"==> Running shunit2 suite: $l_fast_suite"**"fast-sees-slow"*)
		l_ordered_output=0
		;;
	*)
		l_ordered_output=1
		;;
	esac
	assertEquals "Parallel suite output should still be replayed in suite order instead of interleaving later suites ahead of earlier ones." \
		0 "$l_ordered_output"
	assertContains "The fast suite should observe that the slow suite had already started, proving the background queue launched both suites concurrently." \
		"$(cat "$FAKE_SUITE_LOG")" "fast-sees-slow"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_reuses_parallel_slots_when_newer_suite_finishes_first() {
	l_first_suite="$TEST_TMPDIR/slot-reuse-one.sh"
	l_second_suite="$TEST_TMPDIR/slot-reuse-two.sh"
	l_third_suite="$TEST_TMPDIR/slot-reuse-three.sh"
	l_first_started="$TEST_TMPDIR/slot-reuse.first-started"
	l_third_started="$TEST_TMPDIR/slot-reuse.third-started"
	rm -f "$l_first_started" "$l_third_started"

	write_fake_suite_with_body "$l_first_suite" <<'EOF'
#!/bin/sh
	: >"${FAKE_SUITE_STARTED:?}"
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_RELEASE:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_RELEASE:?}" ]; then
		printf '%s\n' "slot-reuse-timeout" >&2
		exit 9
	fi
	printf '%s\n' "slot-one-done"
EOF
	chmod +x "$l_first_suite"

	write_fake_suite_with_body "$l_second_suite" <<'EOF'
#!/bin/sh
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_STARTED:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_STARTED:?}" ]; then
		printf '%s\n' "slot-reuse-missed-first" >&2
		exit 7
	fi
	printf '%s\n' "slot-two-done"
EOF
	chmod +x "$l_second_suite"

	write_fake_suite_with_body "$l_third_suite" <<'EOF'
#!/bin/sh
	: >"${FAKE_SUITE_RELEASE:?}"
	printf '%s\n' "slot-three-done"
EOF
	chmod +x "$l_third_suite"

	output=$(
		FAKE_SUITE_STARTED="$l_first_started" \
			FAKE_SUITE_RELEASE="$l_third_started" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_first_suite" "$l_second_suite" "$l_third_suite"
	)

	assertContains "Parallel execution should reuse a freed worker slot even when the oldest suite is still running." \
		"$output" "==> shunit2 summary: 3 passed, 0 failed"
	assertContains "A later suite should start before the oldest suite finishes once another worker frees a slot." \
		"$output" "slot-three-done"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_streams_serial_output_when_jobs_is_one() {
	l_suite_path="$TEST_TMPDIR/serial-stream-suite.sh"
	l_output_path="$TEST_TMPDIR/serial-stream.output"
	l_status_path="$TEST_TMPDIR/serial-stream.status"

	write_fake_suite_with_body "$l_suite_path" <<'EOF'
#!/bin/sh
	printf '%s\n' "serial-start"
	: >"${FAKE_SUITE_STARTED:?}"
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_RELEASE:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_RELEASE:?}" ]; then
		printf '%s\n' "serial-timeout" >&2
		exit 9
	fi
	printf '%s\n' "serial-end"
EOF
	chmod +x "$l_suite_path"

	(
		FAKE_SUITE_STARTED="$FAKE_SUITE_STARTED" \
			FAKE_SUITE_RELEASE="$FAKE_SUITE_RELEASE" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 "$l_suite_path" >"$l_output_path" 2>&1
		printf '%s\n' "$?" >"$l_status_path"
	) &
	l_runner_pid=$!

	l_saw_live_output=1
	l_wait_count=0
	while [ "$l_wait_count" -lt "$RUN_SHUNIT_TESTS_WAIT_LIMIT" ]; do
		if [ -f "$FAKE_SUITE_STARTED" ]; then
			case "$(cat "$l_output_path" 2>/dev/null || true)" in
			*"serial-start"*)
				l_saw_live_output=0
				break
				;;
			esac
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	assertEquals "Serial execution should stream suite output before the suite finishes when --jobs 1 is selected." \
		0 "$l_saw_live_output"

	: >"$FAKE_SUITE_RELEASE"
	wait "$l_runner_pid" >/dev/null 2>&1

	assertEquals "Serial execution should still complete successfully after streaming live output." \
		0 "$(cat "$l_status_path")"
	assertContains "Serial execution should preserve the suite summary after streaming live output." \
		"$(cat "$l_output_path")" "==> shunit2 summary: 1 passed, 0 failed"
	assertContains "Serial execution should still include the suite's trailing output." \
		"$(cat "$l_output_path")" "serial-end"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_preserves_last_failed_suite_status_in_parallel() {
	l_first_suite="$TEST_TMPDIR/first-fail-suite.sh"
	l_second_suite="$TEST_TMPDIR/second-fail-suite.sh"

	write_fake_suite_with_body "$l_first_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "first-fail"
	exit 3
EOF
	chmod +x "$l_first_suite"

	write_fake_suite_with_body "$l_second_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "second-fail"
	exit 7
EOF
	chmod +x "$l_second_suite"

	zxfer_test_capture_subshell "
		\"$RUN_SHUNIT_TESTS_BIN\" --jobs 2 \"$l_first_suite\" \"$l_second_suite\"
	"

	assertEquals "Parallel execution should preserve the exit status from the last failed suite in input order." \
		7 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Parallel execution should still report both suite failures in the grouped replay output." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "!! Suite failed: $l_first_suite (exit status 3)"
	assertContains "Parallel execution should still report the final failed suite's status." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "!! Suite failed: $l_second_suite (exit status 7)"
	assertContains "Parallel execution should keep the failure summary accurate." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "==> shunit2 summary: 0 passed, 2 failed"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_uses_zxfer_test_shell_for_non_executable_suites_in_parallel() {
	l_first_suite="$TEST_TMPDIR/nonexec-suite-one.sh"
	l_second_suite="$TEST_TMPDIR/nonexec-suite-two.sh"
	l_shell_path="$TEST_TMPDIR/fake-test-shell"
	write_fake_suite "$l_first_suite" "runner-dispatch-one"
	write_fake_suite "$l_second_suite" "runner-dispatch-two"
	chmod 644 "$l_first_suite" "$l_second_suite"
	write_fake_test_shell "$l_shell_path"

	output=$(
		ZXFER_TEST_SHELL="$l_shell_path" \
			FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			FAKE_TEST_SHELL_LOG="$FAKE_TEST_SHELL_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_first_suite" "$l_second_suite"
	)

	assertContains "The runner banner should include the configured alternate shell during parallel dispatch." \
		"$output" "with test shell [$l_shell_path]"
	assertContains "The alternate-shell dispatch path should still report a passing suite summary when background workers are enabled." \
		"$output" "==> shunit2 summary: 2 passed, 0 failed"
	assertContains "The first fake suite should run under the configured alternate shell." \
		"$(cat "$FAKE_SUITE_LOG")" "runner-dispatch-one"
	assertContains "The second fake suite should run under the configured alternate shell." \
		"$(cat "$FAKE_SUITE_LOG")" "runner-dispatch-two"
	assertContains "The alternate shell should receive the first suite path during parallel dispatch." \
		"$(cat "$FAKE_TEST_SHELL_LOG")" "$l_first_suite"
	assertContains "The alternate shell should receive the second suite path during parallel dispatch." \
		"$(cat "$FAKE_TEST_SHELL_LOG")" "$l_second_suite"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_missing_zxfer_test_shell() {
	l_suite_path="$TEST_TMPDIR/missing-shell-suite.sh"
	write_fake_suite "$l_suite_path" "missing-shell"
	chmod +x "$l_suite_path"

	zxfer_test_capture_subshell "
		ZXFER_TEST_SHELL=\"$TEST_TMPDIR/does-not-exist\" \
			FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \
			\"$RUN_SHUNIT_TESTS_BIN\" \"$l_suite_path\"
	"

	assertEquals "A missing alternate shell should fail before any suites are run." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The runner should surface a clear error when ZXFER_TEST_SHELL cannot be executed." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_TEST_SHELL is not executable: $TEST_TMPDIR/does-not-exist"
	assertEquals "The fake suite should not run when the alternate shell is invalid." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vendored_shunit_escape_characters_handles_bsd_userland() {
	actual=$(_shunit_escapeCharactersInString "has'quote\`and\$dollar")
	expected="has\\'quote\\\`and\\\$dollar"

	assertEquals "The vendored shunit2 string escaper should not depend on GNU sed extensions." \
		"$expected" "$actual"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_clears_unsafe_failure_report_commands_from_ambient_env() {
	zxfer_test_capture_subshell "
		TESTS_DIR=\"$TESTS_DIR\" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		PATH=\"${PATH:-/usr/bin:/bin}\" \
		/bin/sh -c '
			. \"\$1/test_helper.sh\"
			if [ -n \"\${ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS+x}\" ]; then
				exit 1
			fi
		' sh \"$TESTS_DIR\"
	"

	assertEquals "Sourcing the shared test helper should clear ambient unsafe failure-report command overrides so unrelated suites stay deterministic." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_clears_ambient_runner_test_shell() {
	zxfer_test_capture_subshell "
		TESTS_DIR=\"$TESTS_DIR\" \
		ZXFER_TEST_SHELL=/bin/dash \
		PATH=\"${PATH:-/usr/bin:/bin}\" \
		/bin/sh -c '
			. \"\$1/test_helper.sh\"
			if [ -n \"\${ZXFER_TEST_SHELL+x}\" ]; then
				exit 1
			fi
		' sh \"$TESTS_DIR\"
	"

	assertEquals "Sourcing the shared test helper should clear ambient runner-shell overrides so nested runner tests use their own explicit shell selection." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_keeps_domain_fixtures_opt_in() {
	/bin/sh -c '
		TESTS_DIR=$1
		. "$1/test_helper.sh"
		if command -v zxfer_test_render_current_backup_metadata_contents >/dev/null 2>&1 ||
			command -v zxfer_test_write_env_fake_ssh >/dev/null 2>&1; then
			exit 1
		fi
		. "$1/helpers/backup_fixtures.sh"
		. "$1/helpers/fake_tool_fixtures.sh"
		command -v zxfer_test_render_current_backup_metadata_contents >/dev/null 2>&1 &&
			command -v zxfer_test_write_env_fake_ssh >/dev/null 2>&1
	' sh "$TESTS_DIR"
	l_status=$?

	assertEquals "Domain-specific backup and fake-tool fixtures should load only after a suite explicitly sources them." \
		0 "$l_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_fake_tool_fixture_returns_write_failure_before_chmod() {
	l_fake_chmod_log="$TEST_TMPDIR/fake-tool-chmod.log"
	l_fake_ssh_path="$TEST_TMPDIR/fake-tool-ssh"

	if /bin/sh -c '
		. "$1/helpers/fake_tool_fixtures.sh"
		fake_chmod_log=$2
		cat() { return 73; }
		chmod() {
			: >"$fake_chmod_log"
			return 0
		}
		zxfer_test_write_env_fake_ssh "$3"
	' sh "$TESTS_DIR" "$l_fake_chmod_log" "$l_fake_ssh_path"; then
		l_status=0
	else
		l_status=$?
	fi
	if [ -e "$l_fake_chmod_log" ]; then
		l_chmod_called=yes
	else
		l_chmod_called=no
	fi

	assertEquals "The fake-tool writer should preserve the executable write failure." \
		73 "$l_status"
	assertEquals "The fake-tool writer should not chmod a path after its write fails." \
		no "$l_chmod_called"
}

# The vendored shunit2 matches assertContains/assertNotContains as one literal
# substring. Upstream piped the container into grep -F, which matched any one
# line of a multi-line expectation, and illumos grep rejected the empty
# pattern a trailing newline made.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
zxfer_test_contains_literal_status() {
	if _shunit_containsLiteral "$1" "$2"; then
		printf '%s\n' 0
	else
		printf '%s\n' "$?"
	fi
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vendored_shunit2_contains_matches_literal_text() {
	l_text='inventory=
snapshot=tank@a*
status=0'

	assertEquals "Adjacent lines should match as one block." 0 \
		"$(zxfer_test_contains_literal_status "$l_text" 'inventory=
snapshot=tank@a*')"
	assertEquals "Lines that are not adjacent should not match." 1 \
		"$(zxfer_test_contains_literal_status "$l_text" 'inventory=
status=0')"
	assertEquals "A trailing newline should be part of the expectation." 0 \
		"$(zxfer_test_contains_literal_status "$l_text" 'inventory=
')"
	assertEquals "Glob characters in the expectation should stay literal." 1 \
		"$(zxfer_test_contains_literal_status "$l_text" 'tank@a?')"
	assertEquals "Backslashes in the container should not be reinterpreted." 0 \
		"$(zxfer_test_contains_literal_status 'a\nb' '\n')"
	assertEquals "Matching should not call grep." 0 "$(
		grep() { return 97; }
		zxfer_test_contains_literal_status "$l_text" 'status=0'
	)"
	assertNotContains "assertNotContains should use the same literal match." \
		"$l_text" 'inventory=
status=0'
}

# Purpose: Wait up to RUN_SHUNIT_TESTS_WAIT_LIMIT seconds for FILE to hold
# data.
# Usage: wait_for_nonempty_file FILE; returns 1 on timeout.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
wait_for_nonempty_file() {
	l_wait_remaining=$RUN_SHUNIT_TESTS_WAIT_LIMIT
	while [ ! -s "$1" ]; do
		[ "$l_wait_remaining" -gt 0 ] || return 1
		l_wait_remaining=$((l_wait_remaining - 1))
		sleep 1
	done
}

# Purpose: Print the PIDs from the given files that are still alive, after
# giving killed processes a few seconds to be reaped.
# Usage: live_pids_in FILE...
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
live_pids_in() {
	l_live_pids=$(cat "$@" 2>/dev/null)
	l_live_remaining=5
	while :; do
		l_live_left=
		for l_live_pid in $l_live_pids; do
			! kill -s 0 "$l_live_pid" 2>/dev/null ||
				l_live_left="$l_live_left $l_live_pid"
		done
		if [ -z "$l_live_left" ] || [ "$l_live_remaining" -eq 0 ]; then
			break
		fi
		l_live_remaining=$((l_live_remaining - 1))
		sleep 1
	done
	printf '%s\n' "${l_live_left# }"
}

# A finished suite is reported through the event FIFO at once; the runner no
# longer polls, which cost up to a second per suite.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_reports_each_finished_suite_at_once() {
	set --
	for l_quick_index in 1 2 3 4 5 6 7 8; do
		write_fake_suite "$TEST_TMPDIR/quick-suite-$l_quick_index.sh" "quick-$l_quick_index"
		chmod +x "$TEST_TMPDIR/quick-suite-$l_quick_index.sh"
		set -- "$@" "$TEST_TMPDIR/quick-suite-$l_quick_index.sh"
	done

	l_start=$(date +%s)
	output=$(FAKE_SUITE_LOG="$FAKE_SUITE_LOG" "$RUN_SHUNIT_TESTS_BIN" --jobs 1 "$@")
	l_elapsed=$(($(date +%s) - l_start))

	assertContains "Every quick suite should pass." \
		"$output" "==> shunit2 summary: 8 passed, 0 failed"
	assertEquals "The quick suites should run once each, in order." \
		"quick-1 quick-2 quick-3 quick-4 quick-5 quick-6 quick-7 quick-8" \
		"$(tr '\n' ' ' <"$FAKE_SUITE_LOG" | sed 's/ $//')"
	assertTrue "Eight quick suites took ${l_elapsed}s; a runner that polls once a second needs at least 8s." \
		"[ $l_elapsed -le 4 ]"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_watchdog_stops_a_stalled_suite_and_continues() {
	l_stalled_suite="$TEST_TMPDIR/stalled-suite.sh"
	l_next_suite="$TEST_TMPDIR/after-stall-suite.sh"
	l_stalled_pids="$TEST_TMPDIR/stalled-suite.pids"
	rm -f "$l_stalled_pids"
	write_fake_suite_with_body "$l_stalled_suite" <<'EOS'
#!/bin/sh
printf '%s\n' "stalled-start"
sleep 30 &
printf '%s %s\n' "$$" "$!" >"${STALLED_PIDS:?}"
wait
EOS
	write_fake_suite "$l_next_suite" "after-stall"
	chmod +x "$l_stalled_suite" "$l_next_suite"

	l_start=$(date +%s)
	output=$(STALLED_PIDS="$l_stalled_pids" FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
		"$RUN_SHUNIT_TESTS_BIN" --jobs 2 --suite-timeout 1 \
		"$l_stalled_suite" "$l_next_suite" 2>&1)
	l_status=$?
	l_elapsed=$(($(date +%s) - l_start))

	assertEquals "A timed-out suite should fail the run with status 124. Output: $output" \
		124 "$l_status"
	assertContains "The watchdog should name the stalled suite when it stops it." \
		"$output" "!! Suite still running after 1s, stopping it: $l_stalled_suite"
	assertContains "The watchdog should list the stalled suite's processes." \
		"$output" "sleep 30"
	assertContains "The stalled suite's output should still be replayed." \
		"$output" "stalled-start"
	assertContains "The replay should report the timeout as the failure." \
		"$output" "!! Suite failed: $l_stalled_suite (timed out after 1s)"
	assertContains "The next suite should still run and pass." \
		"$output" "==> shunit2 summary: 1 passed, 1 failed"
	assertEquals "The next suite should run once." "after-stall" "$(cat "$FAKE_SUITE_LOG")"
	assertTrue "The watchdog should stop the suite well before its 30s sleep ends (took ${l_elapsed}s)." \
		"[ $l_elapsed -lt 20 ]"
	assertEquals "The stalled suite and its child should be gone." \
		"" "$(live_pids_in "$l_stalled_pids")"
}

# The one real teardown test: a TERM stops every running suite, including one
# that ignores TERM (as does its child), and the runner exits 143 without
# leaving its state behind.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_term_stops_running_suites_and_exits_143() {
	l_stubborn_suite="$TEST_TMPDIR/term-stubborn-suite.sh"
	l_plain_suite="$TEST_TMPDIR/term-plain-suite.sh"
	l_stubborn_pids="$TEST_TMPDIR/term-stubborn.pids"
	l_plain_pids="$TEST_TMPDIR/term-plain.pids"
	l_runner_tmpdir="$TEST_TMPDIR/term-runner-tmp"
	l_output="$TEST_TMPDIR/term-runner.output"
	rm -rf "$l_stubborn_pids" "$l_plain_pids" "$l_runner_tmpdir"
	mkdir -p "$l_runner_tmpdir"
	write_fake_suite_with_body "$l_stubborn_suite" <<'EOS'
#!/bin/sh
trap '' TERM
sleep 60 &
printf '%s %s\n' "$$" "$!" >"${STUBBORN_PIDS:?}"
wait
EOS
	write_fake_suite_with_body "$l_plain_suite" <<'EOS'
#!/bin/sh
sleep 60 &
printf '%s %s\n' "$$" "$!" >"${PLAIN_PIDS:?}"
wait
EOS
	chmod +x "$l_stubborn_suite" "$l_plain_suite"

	STUBBORN_PIDS="$l_stubborn_pids" PLAIN_PIDS="$l_plain_pids" \
		TMPDIR="$l_runner_tmpdir" \
		"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_stubborn_suite" "$l_plain_suite" \
		>"$l_output" 2>&1 &
	l_runner_pid=$!
	if ! wait_for_nonempty_file "$l_stubborn_pids" ||
		! wait_for_nonempty_file "$l_plain_pids"; then
		kill -s KILL "$l_runner_pid" 2>/dev/null
		fail "The suites never started: $(cat "$l_output")"
		return 0
	fi
	l_start=$(date +%s)
	kill -s TERM "$l_runner_pid"
	wait "$l_runner_pid"
	l_status=$?
	l_elapsed=$(($(date +%s) - l_start))

	assertEquals "A TERM should make the runner exit 143. Output: $(cat "$l_output")" \
		143 "$l_status"
	assertContains "The runner should say what it stopped." \
		"$(cat "$l_output")" "!! shunit2 runner got SIGTERM; stopping 2 running suites"
	assertTrue "Teardown should finish within its TERM and KILL grace periods (took ${l_elapsed}s)." \
		"[ $l_elapsed -lt 20 ]"
	assertEquals "The TERM-resistant suite and its child should be gone." \
		"" "$(live_pids_in "$l_stubborn_pids")"
	assertEquals "The plain suite and its child should be gone." \
		"" "$(live_pids_in "$l_plain_pids")"
	assertEquals "The runner should remove its private state." \
		"" "$(ls "$l_runner_tmpdir")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_skip_tool_suites_runs_only_product_suites() {
	l_skip_dir="$TEST_TMPDIR/skip-tool-suites"
	rm -rf "$l_skip_dir"
	mkdir -p "$l_skip_dir"
	for l_skip_name in test_ci_fake test_generate_solaris_manpage test_run_fake \
		test_validate test_zxfer_fake; do
		write_fake_suite "$l_skip_dir/$l_skip_name.sh" "$l_skip_name"
		chmod +x "$l_skip_dir/$l_skip_name.sh"
	done

	listing=$("$RUN_SHUNIT_TESTS_BIN" --skip-tool-suites --list-suites "$l_skip_dir"/*.sh)
	output=$(FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
		"$RUN_SHUNIT_TESTS_BIN" --skip-tool-suites --jobs 2 "$l_skip_dir"/*.sh)

	assertEquals "Listing should leave out the tool suites." \
		"$l_skip_dir/test_zxfer_fake.sh" "$listing"
	assertContains "The run should say how many tool suites it skips." \
		"$output" "==> Skipping 4 tool suites (--skip-tool-suites)"
	assertContains "The product suite should run." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Only the product suite should run." \
		"test_zxfer_fake" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_a_non_numeric_suite_timeout() {
	l_suite_path="$TEST_TMPDIR/timeout-option-suite.sh"
	write_fake_suite "$l_suite_path" "timeout-option"
	chmod +x "$l_suite_path"

	option_output=$("$RUN_SHUNIT_TESTS_BIN" --suite-timeout soon "$l_suite_path" 2>&1)
	option_status=$?
	env_output=$(ZXFER_TEST_SUITE_TIMEOUT=-1 "$RUN_SHUNIT_TESTS_BIN" "$l_suite_path" 2>&1)
	env_status=$?

	assertEquals "A non-numeric --suite-timeout should fail before any suite runs." 1 "$option_status"
	assertContains "The error should name the option." \
		"$option_output" "--suite-timeout (or ZXFER_TEST_SUITE_TIMEOUT) must be a whole number of seconds"
	assertEquals "A negative ZXFER_TEST_SUITE_TIMEOUT should fail the same way." 1 "$env_status"
	assertContains "The environment error should use the same message." \
		"$env_output" "must be a whole number of seconds"
	assertEquals "No suite should run." "" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
