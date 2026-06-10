#!/bin/sh
#
# shunit2 budget guard for the micro-bench inner-loop signal.
#
# Runs tests/run_microbench.sh with -V against the small CI fixture
# (8 datasets x 2 snapshots) and asserts every <scenario>_small row in
# tests/perf_budgets.tsv holds (observed <= max). The full-fixture rows
# (25 x 4, keys without the _small suffix) are checked on demand by setting
# ZXFER_MICROBENCH_CHECK_FULL=1. Budgets are ratchet-down-only; see the
# header of tests/perf_budgets.tsv.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

MICROBENCH_SMALL_DATASETS=8
MICROBENCH_SMALL_SNAPS=2

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_microbench_budgets"

	MICROBENCH_BIN="$ZXFER_ROOT/tests/run_microbench.sh"
	BUDGETS_TSV="$ZXFER_ROOT/tests/perf_budgets.tsv"
	MICROBENCH_SMALL_TSV="$TEST_TMPDIR/microbench_small.tsv"
	MICROBENCH_SMALL_ERR="$TEST_TMPDIR/microbench_small.err"

	# One -V bench run feeds every small-fixture budget assertion.
	sh "$MICROBENCH_BIN" -V -d "$MICROBENCH_SMALL_DATASETS" \
		-s "$MICROBENCH_SMALL_SNAPS" \
		>"$MICROBENCH_SMALL_TSV" 2>"$MICROBENCH_SMALL_ERR"
	MICROBENCH_SMALL_STATUS=$?
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# Look up one observed metric value in a micro-bench TSV. Prints the value;
# prints nothing when the row is missing.
microbench_observed_value() {
	l_bench_tsv=$1
	l_scenario=$2
	l_metric=$3

	awk -F'\t' -v s="$l_scenario" -v m="$l_metric" \
		'$1 == s && $2 == m { print $3; exit }' "$l_bench_tsv"
}

# Assert every budget row of one class against a bench TSV. Class "small"
# checks <scenario>_small rows (mapped to the bench scenario by stripping the
# suffix); class "full" checks the unsuffixed rows.
microbench_assert_budget_rows() {
	l_bench_tsv=$1
	l_class=$2
	l_checked=0
	l_tab=$(printf '\t')

	while IFS="$l_tab" read -r l_budget_scenario l_metric l_max; do
		case "$l_budget_scenario" in
		'' | '#'*)
			continue
			;;
		esac
		case "$l_class" in
		small)
			case "$l_budget_scenario" in
			*_small) ;;
			*)
				continue
				;;
			esac
			l_bench_scenario=${l_budget_scenario%_small}
			;;
		full)
			case "$l_budget_scenario" in
			*_small)
				continue
				;;
			esac
			l_bench_scenario=$l_budget_scenario
			;;
		esac

		l_observed=$(microbench_observed_value "$l_bench_tsv" \
			"$l_bench_scenario" "$l_metric")
		if [ -z "$l_observed" ]; then
			fail "micro-bench TSV has no row for $l_bench_scenario/$l_metric"
			continue
		fi
		case "$l_observed" in
		*[!0-9]*)
			fail "non-numeric observed value for $l_bench_scenario/$l_metric: $l_observed"
			continue
			;;
		esac
		assertTrue \
			"budget exceeded: $l_budget_scenario $l_metric observed=$l_observed max=$l_max (ratchet-down-only; do not raise the budget)" \
			"[ $l_observed -le $l_max ]"
		l_checked=$((l_checked + 1))
	done <"$BUDGETS_TSV"

	assertTrue "no $l_class budget rows were checked" "[ $l_checked -gt 0 ]"
}

test_microbench_small_run_succeeds() {
	assertEquals "micro-bench should exit 0; stderr: $(cat "$MICROBENCH_SMALL_ERR")" \
		0 "$MICROBENCH_SMALL_STATUS"
	assertTrue "micro-bench should emit noop rows" \
		"grep -q '^noop	TOTAL	' '$MICROBENCH_SMALL_TSV'"
	assertTrue "micro-bench should emit dryrun_incr rows" \
		"grep -q '^dryrun_incr	TOTAL	' '$MICROBENCH_SMALL_TSV'"
	assertTrue "-V run should emit profile rows" \
		"grep -q '^noop	profile:command_render_calls	' '$MICROBENCH_SMALL_TSV'"
}

test_budgets_file_is_well_formed() {
	l_tab=$(printf '\t')
	l_rows=0

	while IFS="$l_tab" read -r l_scenario l_metric l_max; do
		case "$l_scenario" in
		'' | '#'*)
			continue
			;;
		esac
		l_rows=$((l_rows + 1))
		case "$l_scenario" in
		noop | dryrun_incr | noop_small | dryrun_incr_small) ;;
		*)
			fail "unknown budget scenario key: $l_scenario"
			;;
		esac
		assertTrue "budget row needs a metric: $l_scenario" \
			"[ -n '$l_metric' ]"
		case "$l_max" in
		'' | *[!0-9]*)
			fail "budget max must be a non-negative integer: $l_scenario/$l_metric: $l_max"
			;;
		esac
		# Wall time is timing noise; budgeting it would make CI flaky.
		case "$l_metric" in
		advisory:*)
			fail "advisory metrics must never be budgeted: $l_scenario/$l_metric"
			;;
		esac
	done <"$BUDGETS_TSV"

	assertTrue "budgets file should contain rows" "[ $l_rows -gt 0 ]"
}

test_small_fixture_budgets_hold() {
	assertEquals "micro-bench run must succeed before budgets can be checked" \
		0 "$MICROBENCH_SMALL_STATUS"
	microbench_assert_budget_rows "$MICROBENCH_SMALL_TSV" small
}

test_full_fixture_budgets_hold_when_requested() {
	if [ "${ZXFER_MICROBENCH_CHECK_FULL:-0}" != "1" ]; then
		startSkipping
		assertTrue \
			"set ZXFER_MICROBENCH_CHECK_FULL=1 to enforce the full-fixture budgets" \
			true
		endSkipping
		return 0
	fi

	l_full_tsv="$TEST_TMPDIR/microbench_full.tsv"
	l_full_err="$TEST_TMPDIR/microbench_full.err"
	sh "$MICROBENCH_BIN" -V >"$l_full_tsv" 2>"$l_full_err"
	assertEquals "full micro-bench should exit 0; stderr: $(cat "$l_full_err")" \
		0 $?
	microbench_assert_budget_rows "$l_full_tsv" full
}

. "$SHUNIT2_BIN"
