#!/bin/sh
#
# shunit2 tests for the sensitive-caller budget gate
# (tests/run_budget_check.sh).
#
# shellcheck disable=SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_budget_check"
	RUN_BUDGET_CHECK_BIN="$ZXFER_ROOT/tests/run_budget_check.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# Purpose: Build a fixture tree with the budget runner, a launcher that calls
# mktemp once, and src/example.sh that calls mktemp twice and mentions eval
# only in a comment.
# Usage: create_budget_fixture ROOT; then write ROOT/tests/budget_policy.tsv.
create_budget_fixture() {
	mkdir -p "$1/src" "$1/tests"
	cp "$RUN_BUDGET_CHECK_BIN" "$1/tests/run_budget_check.sh"
	chmod +x "$1/tests/run_budget_check.sh"
	cat >"$1/zxfer" <<'EOF'
#!/bin/sh
l_dir=$(mktemp -d)
EOF
	cat >"$1/src/example.sh" <<'EOF'
#!/bin/sh
# eval should not count when it only documents a prohibited construct.
example() {
	l_one=$(mktemp)
	l_two=$(mktemp)
}
EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_passes_and_ignores_comment_only_mentions() {
	l_fixture_root="$TEST_TMPDIR/pass-root"
	create_budget_fixture "$l_fixture_root"
	printf 'callers\teval \t0\ncallers\tmktemp\t3\n' \
		>"$l_fixture_root/tests/budget_policy.tsv"

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Rows within budget should pass. Output: $output" 0 "$status"
	assertContains "Comment-only sensitive-symbol mentions should not consume caller budget." \
		"$output" "budget check passed: 2 policy rows within budget"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_fails_over_budget_and_outside_allow_list() {
	l_fixture_root="$TEST_TMPDIR/over-root"
	create_budget_fixture "$l_fixture_root"
	printf 'callers\tmktemp\t2\tsrc/example.sh\n' \
		>"$l_fixture_root/tests/budget_policy.tsv"

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "A count above MAX should fail the gate." 1 "$status"
	assertContains "The count violation should name the symbol and the ratchet." \
		"$output" "over budget (ratchet-down-only)"
	assertContains "Launcher matches should count and be held to the allow-list." \
		"$output" "match outside allow-list: zxfer"
	assertNotContains "Allowed files should not be reported." \
		"$output" "match outside allow-list: src/example.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_rejects_unknown_kinds_and_malformed_max() {
	l_fixture_root="$TEST_TMPDIR/malformed-root"
	create_budget_fixture "$l_fixture_root"
	printf 'module_lines\tALL\t10\ncallers\tmktemp\tmany\ncallers\tmktemp\n' \
		>"$l_fixture_root/tests/budget_policy.tsv"

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Rows the gate cannot evaluate should fail closed." 1 "$status"
	assertContains "Removed size-ceiling kinds should be rejected, not ignored." \
		"$output" "unknown policy record kind"
	assertContains "A non-numeric or missing MAX should be rejected." \
		"$output" "MAX must be a non-negative integer"
	assertContains "Every malformed row should be counted." \
		"$output" "3 violation(s) across 3 policy rows"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_fails_closed_when_the_scan_cannot_read_the_tree() {
	l_fixture_root="$TEST_TMPDIR/missing-src-root"
	create_budget_fixture "$l_fixture_root"
	rm -rf "$l_fixture_root/src" "$l_fixture_root/zxfer"
	printf 'callers\tmktemp\t3\n' >"$l_fixture_root/tests/budget_policy.tsv"

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "An unreadable source tree should fail the gate. Output: $output" 1 "$status"
	assertContains "The failure should say the scan could not run." \
		"$output" "Cannot scan src/*.sh and zxfer"
	assertNotContains "An unreadable source tree must never pass." \
		"$output" "budget check passed"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_list_prints_measured_caller_counts() {
	l_fixture_root="$TEST_TMPDIR/list-root"
	create_budget_fixture "$l_fixture_root"
	printf '# comment\ncallers\tmktemp\t9\tsrc/example.sh,zxfer\ncallers\teval \t5\n' \
		>"$l_fixture_root/tests/budget_policy.tsv"

	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" --list)

	assertEquals "List mode should print each callers row with its measured count." \
		"$(printf 'callers\tmktemp\t3\tsrc/example.sh,zxfer\ncallers\teval \t0')" "$output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
