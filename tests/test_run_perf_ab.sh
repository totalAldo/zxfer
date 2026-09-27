#!/bin/sh
#
# shunit2 tests for the advisory wall-clock A/B runner tests/run_perf_ab.sh:
# argument validation, harness errors (unknown ref, failing or wrong
# candidate runs), and tiny real runs (HEAD against HEAD, a SHA baseline with
# the --snapshots, --shell and --scenarios knobs, and against
# upstream-compat-final when that ref is present) that check the TSV rows,
# the appended Markdown summary and work-directory cleanup.
#
# shellcheck disable=SC2317,SC2329  # Test functions are invoked by shunit2.

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_perf_ab"
	PERF_AB_BIN="$ZXFER_ROOT/tests/run_perf_ab.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# Run the A/B runner. Sets PERF_AB_STATUS, PERF_AB_STDOUT, PERF_AB_STDERR
# and PERF_AB_WORKDIR (the work directory it announced, if any).
perf_ab_run() {
	PERF_AB_STATUS=0
	sh "$PERF_AB_BIN" "$@" \
		>"$TEST_TMPDIR/perf_ab.stdout" 2>"$TEST_TMPDIR/perf_ab.stderr" ||
		PERF_AB_STATUS=$?
	PERF_AB_STDOUT=$(cat "$TEST_TMPDIR/perf_ab.stdout")
	PERF_AB_STDERR=$(cat "$TEST_TMPDIR/perf_ab.stderr")
	PERF_AB_WORKDIR=$(sed -n 's/^run_perf_ab\.sh: work directory //p' \
		"$TEST_TMPDIR/perf_ab.stderr")
}

# Assert that the last run announced its work directory and removed it.
perf_ab_assert_workdir_removed() {
	assertNotEquals "the runner should announce its work directory" "" \
		"$PERF_AB_WORKDIR"
	assertFalse "the work directory should be removed: $PERF_AB_WORKDIR" \
		"[ -e '$PERF_AB_WORKDIR' ]"
}

# Assert that a candidate tree whose zxfer runs BODY stops the runner with a
# harness error (status 1) holding MESSAGE; extra arguments go to the runner.
perf_ab_assert_candidate_error() {
	l_body=$1
	l_message=$2
	shift 2
	l_fake_root="$TEST_TMPDIR/fake_candidate"

	mkdir -p "$l_fake_root" || fail "Unable to create the fake candidate."
	printf '#!/bin/sh\n%s\n' "$l_body" >"$l_fake_root/zxfer"
	perf_ab_run --baseline-ref HEAD --candidate-root "$l_fake_root" \
		--reps 1 --sizes 1 --latency-ms 0 "$@"
	assertEquals "a candidate running '$l_body' should be a harness error" \
		1 "$PERF_AB_STATUS"
	assertContains "the error should name the bad run" "$PERF_AB_STDERR" \
		"$l_message"
	assertEquals "a harness error should print no TSV" "" "$PERF_AB_STDOUT"
	perf_ab_assert_workdir_removed
}

# Assert a usage error: exit 2 and MESSAGE on stderr.
perf_ab_assert_usage_error() {
	l_message=$1
	shift

	perf_ab_run "$@"
	assertEquals "'$*' should be a usage error" 2 "$PERF_AB_STATUS"
	assertContains "'$*' should explain the error" "$PERF_AB_STDERR" "$l_message"
	assertEquals "'$*' should print no TSV" "" "$PERF_AB_STDOUT"
}

# Set PERF_AB_REF to the first REF this checkout resolves; otherwise skip the
# calling test and return 1.
perf_ab_require_ref() {
	for PERF_AB_REF in "$@"; do
		if git -C "$ZXFER_ROOT" rev-parse --verify -q "$PERF_AB_REF^{commit}" \
			>/dev/null 2>&1; then
			return 0
		fi
	done
	startSkipping
	assertTrue "none of the git refs $* is available; test skipped." true
	endSkipping
	return 1
}

# Assert the TSV header and one well-formed row per scenario at SIZE, in
# run order: the given scenarios, or the default four.
perf_ab_assert_tsv_rows() {
	l_size=$1
	shift
	[ $# -gt 0 ] || set -- noop incr remote_noop remote_incr
	l_expected_rows=""
	for l_scenario in "$@"; do
		l_expected_rows="$l_expected_rows$l_size $l_scenario
"
	done

	assertEquals "the TSV should start with its header" \
		"size	scenario	candidate_median_s	candidate_min_s	candidate_max_s	baseline_median_s	baseline_min_s	baseline_max_s	ratio" \
		"$(sed -n 1p "$TEST_TMPDIR/perf_ab.stdout")"
	assertEquals "one row per scenario, in run order" \
		"${l_expected_rows%?}" \
		"$(awk -F '\t' 'NR > 1 { print $1, $2 }' "$TEST_TMPDIR/perf_ab.stdout")"
	assertEquals "every row should hold six timings and a ratio" "" \
		"$(awk -F '\t' 'NR > 1 {
			for (i = 3; i <= 8; i++)
				if ($i !~ /^[0-9]+\.[0-9][0-9][0-9]$/) print "bad timing: " $0
			if (NF != 9 || $9 !~ /^([0-9]+\.[0-9][0-9]|n\/a)$/) print "bad row: " $0
		}' "$TEST_TMPDIR/perf_ab.stdout")"
}

test_help_prints_usage() {
	perf_ab_run --help
	assertEquals "--help should succeed" 0 "$PERF_AB_STATUS"
	assertContains "--help should print the interface" "$PERF_AB_STDOUT" \
		"Usage: tests/run_perf_ab.sh --baseline-ref REF"
}

test_argument_errors_exit_with_status_2() {
	perf_ab_assert_usage_error "--baseline-ref is required" --sizes 25
	perf_ab_assert_usage_error "--baseline-ref needs a value" --baseline-ref
	perf_ab_assert_usage_error "unknown argument: --bogus" --baseline-ref HEAD --bogus
	perf_ab_assert_usage_error "--reps must be a positive integer" \
		--baseline-ref HEAD --reps 0
	perf_ab_assert_usage_error "--reps must be a positive integer" \
		--baseline-ref HEAD --reps x
	perf_ab_assert_usage_error "--latency-ms must be a non-negative integer" \
		--baseline-ref HEAD --latency-ms -5
	for l_sizes in "" "25,,100" ",25" "25," "025" "0" "2x"; do
		perf_ab_assert_usage_error "--sizes must be a comma-separated list of positive integers" \
			--baseline-ref HEAD --sizes "$l_sizes"
	done
	perf_ab_assert_usage_error "--sizes lists 25 more than once" \
		--baseline-ref HEAD --sizes 25,100,25
	for l_snapshots in "" 0 1 03 x; do
		perf_ab_assert_usage_error "--snapshots must be an integer of at least 2" \
			--baseline-ref HEAD --snapshots "$l_snapshots"
	done
	for l_scenarios in "" "noop," ",noop" "noop,,incr"; do
		perf_ab_assert_usage_error "--scenarios must be a comma-separated list of scenarios" \
			--baseline-ref HEAD --scenarios "$l_scenarios"
	done
	perf_ab_assert_usage_error "unknown scenario in --scenarios: dryrun_incr" \
		--baseline-ref HEAD --scenarios noop,dryrun_incr
	perf_ab_assert_usage_error "--scenarios lists props more than once" \
		--baseline-ref HEAD --scenarios props,noop,props
	perf_ab_assert_usage_error "--shell is not an executable: $TEST_TMPDIR/no-such-shell" \
		--baseline-ref HEAD --shell "$TEST_TMPDIR/no-such-shell"
	perf_ab_assert_usage_error "--shell is not an executable: $TEST_TMPDIR" \
		--baseline-ref HEAD --shell "$TEST_TMPDIR"
	perf_ab_assert_usage_error "--shell is not an executable: zxfer-perf-ab-no-such-shell" \
		--baseline-ref HEAD --shell zxfer-perf-ab-no-such-shell
	perf_ab_assert_usage_error "--candidate-root has no zxfer launcher" \
		--baseline-ref HEAD --candidate-root "$TEST_TMPDIR"
	perf_ab_assert_usage_error "cannot append to --summary" \
		--baseline-ref HEAD --summary "$TEST_TMPDIR/missing-dir/summary.md"
}

test_unknown_ref_is_a_harness_error() {
	perf_ab_run --baseline-ref refs/heads/zxfer-perf-ab-no-such-ref --sizes 1 --reps 1
	assertEquals "an unknown ref should be a harness error" 1 "$PERF_AB_STATUS"
	assertContains "the error should name the ref" "$PERF_AB_STDERR" \
		"cannot resolve --baseline-ref refs/heads/zxfer-perf-ab-no-such-ref"
	perf_ab_assert_workdir_removed
}

# A candidate that fails, never calls zfs or receives the wrong count is a
# harness error that names the run; it is never reported as a timing.
test_bad_candidate_runs_are_harness_errors() {
	perf_ab_require_ref HEAD || return 0
	perf_ab_assert_candidate_error "exit 7" \
		"candidate noop at size 1: zxfer exited with status 7"
	perf_ab_assert_candidate_error "exit 0" \
		"candidate noop at size 1: no zfs command reached the canned zfs"
	perf_ab_assert_candidate_error "zfs list >/dev/null 2>&1; exit 0" \
		"candidate incr at size 1: 0 receives, expected 2"
	# A props run whose properties no longer match the fixture changes them.
	perf_ab_assert_candidate_error \
		"for d in a b; do echo s | zfs receive \"\$d\"; done; zfs set atime=on a; exit 0" \
		"candidate props at size 1: 1 mutating zfs command(s) besides the receives, expected none" \
		--scenarios props
}

test_head_against_head_reports_tsv_and_summary() {
	perf_ab_require_ref HEAD || return 0
	l_summary="$TEST_TMPDIR/head_summary.md"
	printf '%s\n' "earlier step output" >"$l_summary"

	perf_ab_run --baseline-ref HEAD --reps 1 --sizes 2 --latency-ms 8 \
		--summary "$l_summary"
	assertEquals "a HEAD against HEAD run should succeed; stderr: $PERF_AB_STDERR" \
		0 "$PERF_AB_STATUS"
	perf_ab_assert_tsv_rows 2

	assertEquals "--summary should append, keeping earlier content" \
		"earlier step output" "$(sed -n 1p "$l_summary")"
	assertContains "the summary should carry a heading" "$(cat "$l_summary")" \
		"### zxfer wall-clock A/B (advisory)"
	assertContains "the summary should name the baseline" "$(cat "$l_summary")" \
		"against baseline \`HEAD\`"
	assertContains "the summary should state the latency model" "$(cat "$l_summary")" \
		"charges 8 ms per new connection and 0.5 ms per multiplexed call"
	assertEquals "the summary table should hold one row per scenario" 4 \
		"$(grep -c '^| 2 | [a-z_]* | [0-9.]* ([0-9.]*-[0-9.]*) | [0-9.]* ([0-9.]*-[0-9.]*) | [0-9.na/]* |$' "$l_summary")"
	perf_ab_assert_workdir_removed
}

# A baseline given as a commit SHA, the fixture depth, the scenario list and
# the interpreter that runs both launchers all reach the runs and the report.
test_sha_baseline_with_snapshots_shell_and_scenarios() {
	perf_ab_require_ref HEAD || return 0
	l_sha=$(git -C "$ZXFER_ROOT" rev-parse HEAD)
	l_shell="$TEST_TMPDIR/recording_sh"
	l_shell_log="$TEST_TMPDIR/recording_sh.log"
	: >"$l_shell_log"
	cat >"$l_shell" <<EOF
#!/bin/sh
printf '%s\n' "\$1" >>"$l_shell_log"
exec /bin/sh "\$@"
EOF
	chmod +x "$l_shell"
	l_summary="$TEST_TMPDIR/sha_summary.md"

	perf_ab_run --baseline-ref "$l_sha" --reps 1 --sizes 2 --latency-ms 0 \
		--snapshots 3 --shell "$l_shell" --scenarios props,noop \
		--summary "$l_summary"
	assertEquals "a SHA baseline should run; stderr: $PERF_AB_STDERR" \
		0 "$PERF_AB_STATUS"
	perf_ab_assert_tsv_rows 2 props noop
	assertContains "the summary should name the SHA" "$(cat "$l_summary")" \
		"against baseline \`$l_sha\`"
	assertContains "the summary should state the fixture depth and shell" \
		"$(cat "$l_summary")" "3 snapshots per dataset, both launchers run by \`$l_shell\`"
	assertContains "the summary should explain the props rows" \
		"$(cat "$l_summary")" "68 properties per dataset that already match"
	# Two scenarios, one warm-up and one timed run per tree: four runs each.
	assertEquals "the shell should run the candidate launcher for every run" 4 \
		"$(grep -cx "$(cd "$ZXFER_ROOT" && pwd)/zxfer" "$l_shell_log")"
	assertEquals "the shell should run the baseline launcher for every run" 4 \
		"$(grep -c '/baseline/zxfer$' "$l_shell_log")"
	perf_ab_assert_workdir_removed
}

# The upstream-compat-final launcher lists names only and checks each child
# with `zfs list -H`, and hands `mktemp -t` X-less templates. A GNU-style
# mktemp that refuses those must trigger the template patch, and the extra
# fixture rules must let the old launcher finish every scenario.
test_upstream_baseline_runs_with_a_gnu_style_mktemp() {
	perf_ab_require_ref upstream-compat-final origin/upstream-compat-final ||
		return 0
	l_real_mktemp=$(command -v mktemp)
	mkdir -p "$TEST_TMPDIR/gnu_mktemp" || fail "Unable to create the shim dir."
	cat >"$TEST_TMPDIR/gnu_mktemp/mktemp" <<EOF
#!/bin/sh
for l_arg in "\$@"; do
	l_last=\$l_arg
done
case \$l_last in
*XXX | -*) exec "$l_real_mktemp" "\$@" ;;
esac
printf "mktemp: too few X's in template '%s'\n" "\$l_last" >&2
exit 1
EOF
	chmod +x "$TEST_TMPDIR/gnu_mktemp/mktemp"
	l_summary="$TEST_TMPDIR/upstream_summary.md"

	l_saved_path=$PATH
	PATH="$TEST_TMPDIR/gnu_mktemp:$PATH"
	perf_ab_run --baseline-ref "$PERF_AB_REF" --reps 1 --sizes 2 \
		--latency-ms 0 --summary "$l_summary"
	PATH=$l_saved_path
	assertEquals "the upstream baseline should run; stderr: $PERF_AB_STDERR" \
		0 "$PERF_AB_STATUS"
	perf_ab_assert_tsv_rows 2
	assertContains "the summary should report the template patch" \
		"$(cat "$l_summary")" "X-less mktemp templates were suffixed"
	assertContains "the summary should report the latency-free mock" \
		"$(cat "$l_summary")" "with no added latency"
	perf_ab_assert_workdir_removed
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
