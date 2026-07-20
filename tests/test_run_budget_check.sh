#!/bin/sh
#
# shunit2 tests for the architecture-oriented complexity budget gate.
#
# shellcheck disable=SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_budget_check"
	RUN_BUDGET_CHECK_BIN="$ZXFER_ROOT/tests/run_budget_check.sh"
	COMPLEXITY_AWK_SOURCE="$ZXFER_ROOT/tests/measure_shell_complexity.awk"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

create_budget_fixture() {
	l_budget_fixture_root=$1
	mkdir -p \
		"$l_budget_fixture_root/src" \
		"$l_budget_fixture_root/tests/helpers" \
		"$l_budget_fixture_root/tests/integration" \
		"$l_budget_fixture_root/tests/suites" \
		"$l_budget_fixture_root/tests/fixtures/snapshot_discovery"
	cp "$RUN_BUDGET_CHECK_BIN" "$l_budget_fixture_root/tests/run_budget_check.sh"
	cp "$COMPLEXITY_AWK_SOURCE" "$l_budget_fixture_root/tests/measure_shell_complexity.awk"
	chmod +x "$l_budget_fixture_root/tests/run_budget_check.sh"
	printf '%s\n' '#!/bin/sh' >"$l_budget_fixture_root/zxfer"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_enforces_integration_fragment_and_runner_ceilings() {
	l_fixture_root="$TEST_TMPDIR/integration-ceiling-root"
	create_budget_fixture "$l_fixture_root"
	printf '%s\n' '#!/bin/sh' >"$l_fixture_root/src/example.sh"
	cat >"$l_fixture_root/tests/integration/oversized_cases.sh" <<'EOF'
#!/bin/sh
# one
# two
# three
# four
# five
EOF
	cat >"$l_fixture_root/tests/run_integration_zxfer.sh" <<'EOF'
#!/bin/sh
# one
# two
# three
# four
# five
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
integration_fragment_lines	ALL	5
integration_runner_lines	ALL	5
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Integration fragment and composition-runner ceiling violations should fail the budget gate." \
		1 "$status"
	assertContains "The fragment ceiling should name the oversized integration fragment." \
		"$output" "tests/integration/oversized_cases.sh"
	assertContains "The composition-runner ceiling should name the stable integration entry point." \
		"$output" "tests/run_integration_zxfer.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_enforces_setup_function_ceiling() {
	l_fixture_root="$TEST_TMPDIR/setup-ceiling-root"
	create_budget_fixture "$l_fixture_root"
	printf '%s\n' '#!/bin/sh' >"$l_fixture_root/src/example.sh"
	cat >"$l_fixture_root/tests/test_example.sh" <<'EOF'
#!/bin/sh
setUp() {
	if true; then
		:
	fi
}
EOF
	cat >"$l_fixture_root/tests/helpers/lifecycle.sh" <<'EOF'
#!/bin/sh
setUp ( )
{
	if true; then
		:
	fi
}
EOF
	cat >"$l_fixture_root/tests/helpers/unsupported.sh" <<'EOF'
#!/bin/sh
setUp()
(
	:
)
EOF
	cat >"$l_fixture_root/tests/helpers/continued.sh" <<'EOF'
#!/bin/sh
setUp\
()
{
	:
}
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
setup_lines	ALL	4
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Oversized per-test setup fixtures should fail the budget gate." 1 "$status"
	assertContains "The setup ceiling should name the exact function span." \
		"$output" "tests/test_example.sh:2:setUp"
	assertContains "The setup ceiling should include POSIX whitespace and multiline function headers in shared fixtures." \
		"$output" "tests/helpers/lifecycle.sh:2:setUp"
	assertContains "A POSIX function body outside the measurable brace style must fail closed instead of evading the setup budget." \
		"$output" "tests/helpers/unsupported.sh:2:setUp"
	assertContains "A backslash-newline function header must remain subject to the setup budget." \
		"$output" "tests/helpers/continued.sh:2:setUp"
	assertContains "Unmeasurable setup spans should identify why the ceiling cannot be trusted." \
		"$output" "unmeasured"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_lists_integration_and_setup_measurements() {
	l_fixture_root="$TEST_TMPDIR/new-metric-list-root"
	create_budget_fixture "$l_fixture_root"
	printf '%s\n' '#!/bin/sh' >"$l_fixture_root/src/example.sh"
	cat >"$l_fixture_root/tests/integration/example_cases.sh" <<'EOF'
#!/bin/sh
# fragment
EOF
	cat >"$l_fixture_root/tests/run_integration_zxfer.sh" <<'EOF'
#!/bin/sh
# composition runner
# third line
EOF
	cat >"$l_fixture_root/tests/test_example.sh" <<'EOF'
#!/bin/sh
setUp() {
	:
}
EOF
	: >"$l_fixture_root/tests/budget_policy.tsv"

	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" "$l_fixture_root/tests/run_budget_check.sh" --list)

	assertContains "List mode should report the largest integration fragment." \
		"$output" "integration_fragment_lines	ALL	2"
	assertContains "List mode should report the integration composition runner." \
		"$output" "integration_runner_lines	ALL	3"
	assertContains "List mode should report the largest setUp function span." \
		"$output" "setup_lines	ALL	3"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_includes_extracted_snapshot_discovery_test_fragments() {
	l_fixture_root="$TEST_TMPDIR/fragment-ceiling-root"
	create_budget_fixture "$l_fixture_root"
	printf '%s\n' '#!/bin/sh' >"$l_fixture_root/src/example.sh"
	cat >"$l_fixture_root/tests/fixtures/snapshot_discovery/oversized_cases.sh" <<'EOF'
#!/bin/sh
# one
# two
# three
# four
# five
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	10
function_lines	ALL	10
function_decisions	ALL	2
test_lines	ALL	5
executable_lines	TOTAL	10
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Extracted behavior fragments should remain subject to the universal test-line ceiling." \
		1 "$status"
	assertContains "The fragment ceiling failure should name the oversized extracted file." \
		"$output" "tests/fixtures/snapshot_discovery/oversized_cases.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_applies_function_ceilings_to_the_production_launcher() {
	l_fixture_root="$TEST_TMPDIR/launcher-function-ceiling-root"
	create_budget_fixture "$l_fixture_root"
	printf '%s\n' '#!/bin/sh' >"$l_fixture_root/src/example.sh"
	cat >"$l_fixture_root/zxfer" <<'EOF'
#!/bin/sh
oversized_launcher_function() {
	if true; then
		:
	fi
}
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	10
function_lines	ALL	4
function_decisions	ALL	0
test_lines	ALL	10
executable_lines	TOTAL	20
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Production launcher functions should be subject to the universal complexity ceilings." \
		1 "$status"
	assertContains "The function ceiling failure should name the launcher function and file." \
		"$output" "zxfer:2:oversized_launcher_function"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_does_not_end_a_function_at_a_nested_brace_group() {
	l_fixture_root="$TEST_TMPDIR/nested-brace-group-root"
	create_budget_fixture "$l_fixture_root"
	cat >"$l_fixture_root/src/example.sh" <<'EOF'
#!/bin/sh
grouped_example() {
	false || {
		if true; then
			:
		fi
	}
	while false; do
		:
	done
}
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	20
function_lines	ALL	8
function_decisions	ALL	10
test_lines	ALL	10
executable_lines	TOTAL	20
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "A nested cmd || { ... } group must not truncate its containing function span." \
		1 "$status"
	assertContains "The full grouped function span should exceed the fixture ceiling." \
		"$output" "src/example.sh:2:grouped_example"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_keeps_counting_after_word_internal_hash() {
	l_fixture_root="$TEST_TMPDIR/word-internal-hash-root"
	create_budget_fixture "$l_fixture_root"
	cat >"$l_fixture_root/src/example.sh" <<'EOF'
#!/bin/sh
pattern_trim_example() {
	l_value=${1#prefix}; false || :
}
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	10
function_lines	ALL	10
function_decisions	ALL	0
test_lines	ALL	10
executable_lines	TOTAL	20
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "A parameter-trim operator must not hide a later decision from the complexity gate." \
		1 "$status"
	assertContains "The decision violation should name the function containing the word-internal hash." \
		"$output" "src/example.sh:2:pattern_trim_example"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_preserves_token_state_across_backslash_newline() {
	l_fixture_root="$TEST_TMPDIR/continued-word-internal-hash-root"
	create_budget_fixture "$l_fixture_root"
	cat >"$l_fixture_root/src/example.sh" <<'EOF'
#!/bin/sh
continued_word_hash_example() {
	: word\
#tail &&
	false
}
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	10
function_lines	ALL	10
function_decisions	ALL	0
test_lines	ALL	10
executable_lines	TOTAL	20
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Backslash-newline removal must not turn a word-internal hash into a comment." \
		1 "$status"
	assertContains "The executable short-circuit after a continued word must consume the decision budget." \
		"$output" "src/example.sh:2:continued_word_hash_example"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_shell_complexity_scanner_is_quiet_under_gnu_awk() {
	l_gawk=$(command -v gawk 2>/dev/null) || {
		startSkipping
		assertTrue "gawk is unavailable; GNU awk diagnostic regression skipped." true
		endSkipping
		return 0
	}
	l_fixture="$TEST_TMPDIR/gawk-heredoc-fixture.sh"
	l_stdout="$TEST_TMPDIR/gawk-heredoc.stdout"
	l_stderr="$TEST_TMPDIR/gawk-heredoc.stderr"
	cat >"$l_fixture" <<'EOF'
#!/bin/sh
heredoc_example() {
	cat <<'PAYLOAD'
literal payload
PAYLOAD
}
EOF

	status=0
	"$l_gawk" -f "$COMPLEXITY_AWK_SOURCE" "$l_fixture" \
		>"$l_stdout" 2>"$l_stderr" || status=$?

	assertEquals "GNU awk should accept the shell scanner." 0 "$status"
	assertEquals "Portable heredoc quote matching must not emit GNU awk regex warnings." \
		"" "$(cat "$l_stderr")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_enforces_universal_module_function_and_test_ceilings() {
	l_fixture_root="$TEST_TMPDIR/ceiling-root"
	create_budget_fixture "$l_fixture_root"
	cat >"$l_fixture_root/src/example.sh" <<'EOF'
#!/bin/sh
oversized_example() {
	if true; then
		if true; then
			:
		fi
	fi
}
EOF
	cat >"$l_fixture_root/tests/test_example.sh" <<'EOF'
#!/bin/sh
# one
# two
# three
# four
# five
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	7
function_lines	ALL	6
function_decisions	ALL	1
test_lines	ALL	5
executable_lines	TOTAL	20
EOF

	status=0
	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/run_budget_check.sh" 2>&1) || status=$?

	assertEquals "Universal ceiling violations should fail the budget gate." 1 "$status"
	assertContains "The module ceiling should name the oversized module." \
		"$output" "src/example.sh"
	assertContains "The function span ceiling should name the exact function." \
		"$output" "oversized_example"
	assertContains "The decision ceiling should be checked independently of physical size." \
		"$output" "function_decisions"
	assertContains "The focused-test ceiling should name the oversized test file." \
		"$output" "tests/test_example.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_budget_check_caller_ratchets_ignore_comment_only_mentions() {
	l_fixture_root="$TEST_TMPDIR/caller-root"
	create_budget_fixture "$l_fixture_root"
	cat >"$l_fixture_root/src/example.sh" <<'EOF'
#!/bin/sh
# eval should not count when it only documents a prohibited construct.
safe_example() {
	:
}
EOF
	cat >"$l_fixture_root/tests/budget_policy.tsv" <<'EOF'
module_lines	ALL	10
function_lines	ALL	10
function_decisions	ALL	2
test_lines	ALL	10
executable_lines	TOTAL	10
callers	eval 	0
EOF

	output=$(ZXFER_BUDGET_ROOT="$l_fixture_root" "$l_fixture_root/tests/run_budget_check.sh")

	assertContains "Comment-only sensitive-symbol mentions should not consume caller budget." \
		"$output" "budget check passed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
