#!/bin/sh
#
# shunit2 budget guard for the micro-bench inner-loop signal.
#
# Runs tests/run_microbench.sh with -V against the small CI fixture
# (8 datasets x 2 snapshots) and asserts every <scenario>_small row in
# tests/perf_budgets.tsv holds: observed <= max, and observed == max for the
# exact ssh_connections and ssh_master_opens rows. The full-fixture rows
# (25 x 4, keys without the _small suffix) are checked on demand by setting
# ZXFER_MICROBENCH_CHECK_FULL=1. Budgets are ratchet-down-only; see the
# header of tests/perf_budgets.tsv.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/mock_toolchain_helper.sh
. "$TESTS_DIR/mock_toolchain_helper.sh"

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

# Purpose: Print one line per budget row of a class that a bench TSV breaks,
# lacks or reports as non-numeric, or a note when the class has no rows.
# Usage: microbench_budget_violations <bench-tsv> <small|full>. Class small
# checks the <scenario>_small rows against the bench scenario without the
# suffix; class full checks the unsuffixed rows. Returns 1 when it printed,
# and awk's own status (2) when an input cannot be read.
microbench_budget_violations() {
	awk -F '\t' -v class="$2" '
		FILENAME == ARGV[1] {
			observed[$1 FS $2] = $3
			next
		}
		$1 == "" || $1 ~ /^#/ {
			next
		}
		{
			scenario = $1
			if (sub(/_small$/, "", scenario) != (class == "small"))
				next
			checked++
			key = scenario FS $2
			if (!(key in observed))
				line = "missing bench row: " $1 " " $2
			else if (observed[key] !~ /^[0-9]+$/)
				line = "non-numeric: " $1 " " $2 " observed=" observed[key]
			else if (observed[key] + 0 > $3 + 0)
				line = "budget exceeded: " $1 " " $2 " observed=" observed[key] " max=" $3
			else if ($2 ~ /^ssh_(connections|master_opens)$/ && observed[key] + 0 != $3 + 0)
				line = "exact count dropped: " $1 " " $2 " observed=" observed[key] " exact=" $3
			else
				next
			print line
			bad++
		}
		END {
			if (!checked) {
				print "no " class " budget rows were checked"
				bad++
			}
			exit (bad > 0)
		}
	' "$1" "$BUDGETS_TSV"
}

# Purpose: Assert that a bench TSV breaks no budget row of a class and that
# the checker itself succeeded, so an unreadable input fails closed.
# Usage: microbench_assert_budgets_hold <message> <bench-tsv> <small|full>
microbench_assert_budgets_hold() {
	l_violations=$(microbench_budget_violations "$2" "$3")
	l_status=$?
	assertEquals "$1" "" "$l_violations"
	assertEquals "$1: the budget checker should exit 0" 0 "$l_status"
}

# Purpose: Write a bench TSV that sits exactly at every small-fixture budget,
# except that the remote_noop ssh rows come from a mock ssh log.
# Usage: microbench_write_budget_edge_tsv <ssh-log> <out-tsv>
microbench_write_budget_edge_tsv() {
	{
		awk -F '\t' -v OFS='\t' '
			$1 ~ /^#/ || $1 !~ /_small$/ {
				next
			}
			$1 == "remote_noop_small" && $2 ~ /^ssh_/ {
				next
			}
			{
				sub(/_small$/, "", $1)
				print
			}
		' "$BUDGETS_TSV"
		zxfer_mockbin_ssh_log_rows remote_noop "$1"
	} >"$2"
}

test_microbench_small_run_succeeds() {
	assertEquals "micro-bench should exit 0; stderr: $(cat "$MICROBENCH_SMALL_ERR")" \
		0 "$MICROBENCH_SMALL_STATUS"
	for l_scenario in noop dryrun_incr incr remote_noop remote_incr props; do
		assertTrue "micro-bench should emit $l_scenario rows" \
			"grep -q '^$l_scenario	TOTAL	' '$MICROBENCH_SMALL_TSV'"
		assertTrue "micro-bench should emit $l_scenario ssh rows" \
			"grep -q '^$l_scenario	ssh_connections	' '$MICROBENCH_SMALL_TSV'"
	done
	assertTrue "-V run should emit profile rows" \
		"grep -q '^noop	profile:command_render_calls	' '$MICROBENCH_SMALL_TSV'"
	# The props rows count one recursive property read per side: a fallback
	# to per-dataset reads would show up here before it shows in TOTAL.
	assertEquals "props should read each side's properties with one recursive read" \
		"props	profile:normalized_property_reads_source	1
props	profile:normalized_property_reads_destination	1" \
		"$(grep -E '^props	profile:normalized_property_reads_(source|destination)	' "$MICROBENCH_SMALL_TSV")"
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
		case "${l_scenario%_small}" in
		noop | dryrun_incr | incr | remote_noop | remote_incr | props) ;;
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
	microbench_assert_budgets_hold \
		"small-fixture budgets (ratchet-down-only; do not raise a budget)" \
		"$MICROBENCH_SMALL_TSV" small
}

# One ssh command that skips the control socket is one more connection than
# the pinned remote no-op count, and that alone must break the budget; a
# remote no-op that never reaches ssh must break the exact rows too. The
# other rows sit exactly at their budgets, so only ssh rows can fail here.
test_ssh_connection_changes_break_the_exact_budget() {
	l_case_dir="$TEST_TMPDIR/extra_connection"
	l_socket="$l_case_dir/origin.sock"
	mkdir -p "$l_case_dir" || fail "Unable to create the case directory."
	zxfer_mockbin_write_socket_ssh "$l_case_dir/ssh" ||
		fail "Unable to write the socket-aware mock ssh."

	# Replay the shape of a remote no-op: probe, one master, a multiplexed
	# command and the close, then add one command without -S.
	MOCK_SSH_LOG="$l_case_dir/clean.log"
	export MOCK_SSH_LOG
	"$l_case_dir/ssh" -M -V 2>/dev/null
	"$l_case_dir/ssh" -M -S "$l_socket" -fN localhost
	"$l_case_dir/ssh" -S "$l_socket" localhost true
	"$l_case_dir/ssh" -S "$l_socket" -O exit localhost
	cp "$MOCK_SSH_LOG" "$l_case_dir/extra.log"
	MOCK_SSH_LOG="$l_case_dir/extra.log"
	"$l_case_dir/ssh" localhost true
	unset MOCK_SSH_LOG
	: >"$l_case_dir/none.log"
	for l_replay in clean extra none; do
		microbench_write_budget_edge_tsv "$l_case_dir/$l_replay.log" \
			"$l_case_dir/$l_replay.tsv"
	done

	microbench_assert_budgets_hold \
		"every row at its budget, with the replayed no-op ssh rows, should pass" \
		"$l_case_dir/clean.tsv" small
	assertEquals "the extra direct connection should be the only violation" \
		"budget exceeded: remote_noop_small ssh_connections observed=2 max=1" \
		"$(microbench_budget_violations "$l_case_dir/extra.tsv" small)"
	assertEquals "a remote no-op without ssh should break both exact rows" \
		"exact count dropped: remote_noop_small ssh_connections observed=0 exact=1
exact count dropped: remote_noop_small ssh_master_opens observed=0 exact=1" \
		"$(microbench_budget_violations "$l_case_dir/none.tsv" small)"
}

test_budget_checker_fails_closed_on_unreadable_input() {
	microbench_budget_violations "$TEST_TMPDIR/missing.tsv" small \
		>/dev/null 2>&1
	assertNotEquals "a missing bench TSV should fail the checker" 0 $?
	l_saved_budgets=$BUDGETS_TSV
	BUDGETS_TSV="$TEST_TMPDIR/missing_budgets.tsv"
	microbench_budget_violations "$MICROBENCH_SMALL_TSV" small \
		>/dev/null 2>&1
	l_status=$?
	BUDGETS_TSV=$l_saved_budgets
	assertNotEquals "a missing budgets file should fail the checker" 0 "$l_status"
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
	l_run_status=$?
	assertEquals "full micro-bench should exit 0; stderr: $(cat "$l_full_err")" \
		0 "$l_run_status"
	microbench_assert_budgets_hold \
		"full-fixture budgets (ratchet-down-only; do not raise a budget)" \
		"$l_full_tsv" full
}

. "$SHUNIT2_BIN"
