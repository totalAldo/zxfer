#!/bin/sh
#
# shunit2 tests for the two-binary performance comparison wrapper.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_perf_compare"
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	PERF_COMPARE_RUNNER="$ZXFER_ROOT/tests/run_perf_compare.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_RUN_PERF_COMPARE_SOURCE_ONLY=1
	# shellcheck source=tests/run_perf_compare.sh
	. "$PERF_COMPARE_RUNNER"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
write_mock_zxfer_binary() {
	l_path=$1

	printf '%s\n' "#!/bin/sh" "exit 0" >"$l_path"
	chmod 700 "$l_path"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
write_mock_perf_runner() {
	l_path=$1

	cat >"$l_path" <<'EOF'
#!/bin/sh
label=
output_dir=
while [ $# -gt 0 ]; do
	case "$1" in
	--label)
		shift
		label=$1
		;;
	--output-dir)
		shift
		output_dir=$1
		;;
	esac
	shift
done
[ -n "$output_dir" ] || exit 64
mkdir -p "$output_dir"
printf '%s\n' "run_label	case	samples	wall_ms_avg	throughput_bytes_per_sec_avg	startup_latency_ms_avg	failed_samples" >"$output_dir/summary.tsv"
case "$label" in
base)
	printf '%s\n' "base	chain_local	1	100.00	1000.00	10.00	0" >>"$output_dir/summary.tsv"
	;;
*)
	printf '%s\n' "$label	chain_local	1	125.00	875.00	20.00	0" >>"$output_dir/summary.tsv"
	;;
esac
EOF
	chmod 700 "$l_path"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_compare_parse_args_accepts_documented_interface() {
	zxfer_perf_compare_parse_args \
		--baseline-bin "$TEST_TMPDIR/base-bin" \
		--candidate-bin "$TEST_TMPDIR/cand-bin" \
		--baseline-label base \
		--candidate-label cand \
		--profile standard \
		--case chain_local \
		--samples 2 \
		--warmups 1 \
		--output-dir "$TEST_TMPDIR/out" \
		--yes

	assertEquals "The comparator should parse the baseline binary." \
		"$TEST_TMPDIR/base-bin" "$ZXFER_PERF_COMPARE_BASELINE_BIN"
	assertEquals "The comparator should parse the candidate binary." \
		"$TEST_TMPDIR/cand-bin" "$ZXFER_PERF_COMPARE_CANDIDATE_BIN"
	assertEquals "The comparator should parse the baseline label." \
		"base" "$ZXFER_PERF_COMPARE_BASELINE_LABEL"
	assertEquals "The comparator should parse the candidate label." \
		"cand" "$ZXFER_PERF_COMPARE_CANDIDATE_LABEL"
	assertEquals "The comparator should parse the profile." \
		"standard" "$ZXFER_PERF_COMPARE_PROFILE"
	assertEquals "The comparator should parse case selectors." \
		"chain_local" "$ZXFER_PERF_COMPARE_CASES"
	assertEquals "The comparator should parse sample counts." \
		"2" "$ZXFER_PERF_COMPARE_SAMPLES"
	assertEquals "The comparator should parse warmup counts." \
		"1" "$ZXFER_PERF_COMPARE_WARMUPS"
	assertEquals "The comparator should parse --yes." \
		"1" "$ZXFER_PERF_COMPARE_YES"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_compare_runs_two_binaries_and_writes_reports() {
	base_bin="$TEST_TMPDIR/base-zxfer"
	cand_bin="$TEST_TMPDIR/cand-zxfer"
	mock_runner="$TEST_TMPDIR/mock-run-perf-tests"
	output_dir="$TEST_TMPDIR/perf-compare-out"
	write_mock_zxfer_binary "$base_bin"
	write_mock_zxfer_binary "$cand_bin"
	write_mock_perf_runner "$mock_runner"
	ZXFER_PERF_COMPARE_RUNNER=$mock_runner

	zxfer_perf_compare_main \
		--baseline-bin "$base_bin" \
		--candidate-bin "$cand_bin" \
		--baseline-label base \
		--candidate-label cand \
		--profile smoke \
		--case chain_local \
		--samples 1 \
		--warmups 0 \
		--output-dir "$output_dir" \
		--yes >/dev/null
	resolved_output_dir=$ZXFER_PERF_COMPARE_OUTPUT_DIR
	if [ -s "$resolved_output_dir/baseline/summary.tsv" ]; then
		baseline_summary_exists=1
	else
		baseline_summary_exists=0
	fi
	if [ -s "$resolved_output_dir/candidate/summary.tsv" ]; then
		candidate_summary_exists=1
	else
		candidate_summary_exists=0
	fi

	assertEquals "The baseline run should get its own artifact directory." \
		1 "$baseline_summary_exists"
	assertEquals "The candidate run should get its own artifact directory." \
		1 "$candidate_summary_exists"
	assertContains "The top-level TSV should compare wall-clock metrics." \
		"$(cat "$resolved_output_dir/compare.tsv")" "wall_ms_avg"
	assertContains "The top-level TSV should include advisory regression annotations." \
		"$(cat "$resolved_output_dir/compare.tsv")" "regression"
	assertContains "The markdown report should render compared metrics." \
		"$(cat "$resolved_output_dir/compare.md")" "\`startup_latency_ms_avg\`"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_compare_sanitizes_labels_in_top_level_tsv() {
	baseline_summary="$TEST_TMPDIR/base-summary.tsv"
	candidate_summary="$TEST_TMPDIR/cand-summary.tsv"
	compare_file="$TEST_TMPDIR/compare-sanitized.tsv"
	ZXFER_PERF_COMPARE_BASELINE_LABEL=$(printf 'base\tlabel\nnext')
	ZXFER_PERF_COMPARE_CANDIDATE_LABEL=$(printf 'cand\tlabel\nnext')
	printf '%s\n' "run_label	case	samples	wall_ms_avg	failed_samples" >"$baseline_summary"
	printf '%s\n' "base	chain_local	1	100.00	0" >>"$baseline_summary"
	printf '%s\n' "run_label	case	samples	wall_ms_avg	failed_samples" >"$candidate_summary"
	printf '%s\n' "cand	chain_local	1	110.00	0" >>"$candidate_summary"

	zxfer_perf_compare_write_tsv "$baseline_summary" "$candidate_summary" "$compare_file"

	line_count=$(wc -l <"$compare_file" | awk '{ print $1 }')
	result=$(awk -F '\t' '
		NR == 1 {
			expected_nf = NF
			for (i = 1; i <= NF; i++) col[$i] = i
			next
		}
		NR == 2 {
			printf "same_nf=%s baseline=<%s> candidate=<%s>\n", (NF == expected_nf ? "yes" : "no"), $(col["baseline_label"]), $(col["candidate_label"])
		}
	' "$compare_file")

	assertEquals "TSV labels should not introduce extra rows." \
		"2" "$line_count"
	assertContains "TSV labels should not introduce extra columns." \
		"$result" "same_nf=yes"
	assertContains "The baseline label should be tab/newline sanitized." \
		"$result" "baseline=<base label next>"
	assertContains "The candidate label should be tab/newline sanitized." \
		"$result" "candidate=<cand label next>"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_compare_fails_when_sample_runner_fails() {
	base_bin="$TEST_TMPDIR/base-zxfer-fail"
	cand_bin="$TEST_TMPDIR/cand-zxfer-fail"
	mock_runner="$TEST_TMPDIR/mock-run-perf-tests-fail"
	write_mock_zxfer_binary "$base_bin"
	write_mock_zxfer_binary "$cand_bin"
	printf '%s\n' "#!/bin/sh" "exit 42" >"$mock_runner"
	chmod 700 "$mock_runner"

	zxfer_test_capture_subshell "
		ZXFER_RUN_PERF_COMPARE_SOURCE_ONLY=1
		. \"$PERF_COMPARE_RUNNER\"
		ZXFER_PERF_COMPARE_RUNNER=\"$mock_runner\"
		zxfer_perf_compare_main \
			--baseline-bin \"$base_bin\" \
			--candidate-bin \"$cand_bin\" \
			--baseline-label base \
			--candidate-label cand \
			--profile smoke \
			--output-dir \"$TEST_TMPDIR/perf-compare-fail\" \
			--yes
	"

	assertEquals "The comparator should fail when the sample harness fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The failure should identify the failed perf run." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Performance sample run failed for base"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
