#!/bin/sh
#
# shunit2 tests for the advisory --forks mode of tests/run_microbench.sh and
# the props scenario's fixture check. The default rows are covered by
# tests/test_zxfer_microbench_budgets.sh.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_microbench"
	MICROBENCH_BIN="$ZXFER_ROOT/tests/run_microbench.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_forks_mode_appends_advisory_fork_rows() {
	trace_bash=${ZXFER_MICROBENCH_BASH:-$(command -v bash 2>/dev/null)}
	# shellcheck disable=SC2016  # expanded by the bash under test
	if [ -z "$trace_bash" ] ||
		! "$trace_bash" -c '[ "${BASH_VERSINFO[0]}${BASH_VERSINFO[1]}" -ge 41 ]' \
			>/dev/null 2>&1; then
		startSkipping
		assertTrue "bash 4.1+ is unavailable; --forks coverage skipped." true
		endSkipping
		return 0
	fi

	status=0
	output=$(ZXFER_MICROBENCH_BASH=$trace_bash TMPDIR=$TEST_TMPDIR \
		sh "$MICROBENCH_BIN" --forks -d 2 -s 2 incr 2>&1) || status=$?
	run_forks=$(printf '%s\n' "$output" |
		awk -F '\t' '$2 == "advisory:forks_run" { print $3 }')

	assertEquals "--forks should succeed. Output: $output" 0 "$status"
	assertContains "--forks should keep the counted helper rows." \
		"$output" "incr	TOTAL	"
	assertContains "--forks should report startup forks." \
		"$output" "incr	advisory:forks_startup	"
	assertContains "--forks should report a fork total." \
		"$output" "incr	advisory:forks_total	"
	assertTrue "The incremental loop should fork at least once (got '$run_forks')." \
		"[ '${run_forks:-0}' -gt 0 ]"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_forks_mode_refuses_a_missing_bash() {
	status=0
	output=$(ZXFER_MICROBENCH_BASH="$TEST_TMPDIR/missing-bash" TMPDIR=$TEST_TMPDIR \
		sh "$MICROBENCH_BIN" --forks noop 2>&1) || status=$?

	assertEquals "--forks without bash 4.1+ should fail as a usage error." 2 "$status"
	assertContains "The failure should name the override variable." \
		"$output" "set ZXFER_MICROBENCH_BASH"
	assertNotContains "No scenario should run without a usable bash." \
		"$output" "noop	TOTAL"
}

# The props fixture's properties already match on both sides; a launcher
# that changes one is measuring other work, so the scenario must fail.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_props_scenario_fails_when_a_property_changes() {
	fake_zxfer="$TEST_TMPDIR/props_zxfer"
	cat >"$fake_zxfer" <<'EOF'
#!/bin/sh
PATH=$ZXFER_SECURE_PATH
zfs set atime=on dstpool/back/data
exit 0
EOF
	chmod +x "$fake_zxfer"

	status=0
	output=$(ZXFER_MOCKBIN_ZXFER_BIN=$fake_zxfer TMPDIR=$TEST_TMPDIR \
		sh "$MICROBENCH_BIN" -d 1 -s 2 props 2>&1) || status=$?

	assertEquals "a props run that changes a property should fail. Output: $output" \
		1 "$status"
	assertContains "the failure should say the fixture needs no change" \
		"$output" "scenario props changed 1 properties; its fixture must need none"
	assertNotContains "no counts should be reported for the failed run" \
		"$output" "props	TOTAL"

	status=0
	output=$(TMPDIR=$TEST_TMPDIR sh "$MICROBENCH_BIN" -d 1 -s 2 props 2>&1) ||
		status=$?
	assertEquals "the real launcher should change nothing in props. Output: $output" \
		0 "$status"
	assertContains "the props run should report its counts" \
		"$output" "props	TOTAL	"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
