#!/bin/sh
#
# shunit2 tests for the manual performance runner control flow.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_perf"
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	PERF_RUNNER="$ZXFER_ROOT/tests/run_perf_tests.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_RUN_PERF_SOURCE_ONLY=1
	ZXFER_PERF_TESTS_DIR="$TESTS_DIR"
	# shellcheck source=tests/run_perf_tests.sh
	. "$PERF_RUNNER"
	ZXFER_PERF_PROFILE=smoke
	ZXFER_PERF_CASES=""
	ZXFER_PERF_SAMPLES=""
	ZXFER_PERF_WARMUPS=""
	ZXFER_PERF_OUTPUT_DIR=""
	ZXFER_PERF_BASELINE=""
	ZXFER_PERF_YES=0
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_parse_args_accepts_profile_cases_and_counts() {
	zxfer_perf_parse_args --yes --profile standard --case chain_local,chain_remote_mock --samples 5 --warmups 2 --output-dir "$TEST_TMPDIR/out" --baseline "$TEST_TMPDIR/base.tsv"

	assertEquals "The perf runner should parse --yes." "1" "$ZXFER_PERF_YES"
	assertEquals "The perf runner should parse the requested profile." "standard" "$ZXFER_PERF_PROFILE"
	assertEquals "The perf runner should parse comma-delimited case selectors." \
		"chain_local chain_remote_mock" "$ZXFER_PERF_CASES"
	assertEquals "The perf runner should parse sample count overrides." "5" "$ZXFER_PERF_SAMPLES"
	assertEquals "The perf runner should parse warmup count overrides." "2" "$ZXFER_PERF_WARMUPS"
	assertEquals "The perf runner should preserve output-dir paths." "$TEST_TMPDIR/out" "$ZXFER_PERF_OUTPUT_DIR"
	assertEquals "The perf runner should preserve baseline paths." "$TEST_TMPDIR/base.tsv" "$ZXFER_PERF_BASELINE"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_parse_args_rejects_empty_case_selector() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_PERF_SOURCE_ONLY=1
		ZXFER_PERF_TESTS_DIR=\"$TESTS_DIR\"
		. \"$PERF_RUNNER\"
		zxfer_perf_parse_args --case ''
	"

	assertEquals "Empty perf case selectors should not expand to the default all-case list." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should identify the missing case value." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--case requires at least one case name"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_apply_profile_defaults_sets_standard_fixture_sizes() {
	ZXFER_PERF_PROFILE=standard

	zxfer_perf_apply_profile_defaults

	assertEquals "The standard profile should use three measured samples by default." "3" "$ZXFER_PERF_SAMPLES"
	assertEquals "The standard profile should use one warmup by default." "1" "$ZXFER_PERF_WARMUPS"
	assertEquals "The standard profile should use 2048 MB sparse pool files." "2048" "$ZXFER_PERF_SPARSE_SIZE_MB"
	assertEquals "The standard profile should include larger snapshot chains." "32" "$ZXFER_PERF_CHAIN_SNAPSHOTS"
	assertEquals "The standard profile should include larger fanout fixtures." "48" "$ZXFER_PERF_FANOUT_DATASETS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_apply_profile_defaults_rejects_unknown_cases() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_PERF_SOURCE_ONLY=1
		ZXFER_PERF_TESTS_DIR=\"$TESTS_DIR\"
		. \"$PERF_RUNNER\"
		ZXFER_PERF_CASES='nosuch_case'
		zxfer_perf_apply_profile_defaults
	"

	assertEquals "Unknown perf case selectors should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should name the unknown perf case." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Unknown performance case requested: nosuch_case"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_estimate_send_bytes_uses_first_full_send_plus_incrementals() {
	zfs() {
		case "$*" in
		"send -nP pool/src@s1")
			printf '%s\n' "size	100"
			;;
		"send -nP -I pool/src@s1 pool/src@s3")
			printf '%s\n' "size	900"
			;;
		*)
			printf '%s\n' "unexpected zfs args: $*" >&2
			return 1
			;;
		esac
	}

	result=$(zxfer_perf_estimate_send_bytes "pool/src@s1" "pool/src@s3")

	assertEquals "Chain fixtures should estimate full first snapshot plus incremental range." \
		"1000" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_build_mock_secure_path_preserves_configured_append_dirs() {
	ZXFER_SECURE_PATH_APPEND="/usr/local/zfs/bin:/opt/zfs/bin"

	result=$(zxfer_perf_build_mock_secure_path "$TEST_TMPDIR/mock-bin")

	assertContains "Remote mock cases should prepend the mock helper path." \
		"$result" "$TEST_TMPDIR/mock-bin:/sbin"
	assertContains "Remote mock cases should preserve secure-path append entries such as macOS OpenZFS." \
		"$result" ":/usr/local/zfs/bin:/opt/zfs/bin"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_run_cases_uses_isolated_loop_state() {
	ZXFER_PERF_CASES="chain_local fanout_local_j1_props"
	ZXFER_PERF_WARMUPS=1
	ZXFER_PERF_SAMPLES=1
	ZXFER_PERF_TEST_RUN_LOG="$TEST_TMPDIR/perf-run-cases.log"
	: >"$ZXFER_PERF_TEST_RUN_LOG"
	ZXFER_PERF_TEST_RUN_COUNT=0

	zxfer_perf_run_case_sample() {
		l_stub_case=$1
		l_stub_kind=$2
		l_stub_index=$3
		l_stub_case_number=$4
		l_stub_case_count=$5
		ZXFER_PERF_TEST_RUN_COUNT=$((ZXFER_PERF_TEST_RUN_COUNT + 1))
		printf '%s:%s:%s:%s/%s\n' \
			"$l_stub_case" "$l_stub_kind" "$l_stub_index" "$l_stub_case_number" "$l_stub_case_count" >>"$ZXFER_PERF_TEST_RUN_LOG"
		if [ "$ZXFER_PERF_TEST_RUN_COUNT" -gt 4 ]; then
			l_i=99
		else
			l_i=0
		fi
	}

	zxfer_perf_run_cases

	assertEquals "Perf case iteration should not be clobbered by sourced integration helpers that reuse l_i." \
		4 "$ZXFER_PERF_TEST_RUN_COUNT"
	assertContains "The first case should run a warmup and measured sample with case position metadata." \
		"$(cat "$ZXFER_PERF_TEST_RUN_LOG")" "chain_local:warmup:1:1/2"
	assertContains "The second case should still run after the first sample clobbers l_i." \
		"$(cat "$ZXFER_PERF_TEST_RUN_LOG")" "fanout_local_j1_props:sample:1:2/2"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_run_case_sample_logs_case_description_and_sample_position() {
	ZXFER_PERF_OUTPUT_DIR="$TEST_TMPDIR/perf-output"
	ZXFER_PERF_SAMPLES=3
	ZXFER_PERF_WARMUPS=1
	mkdir -p "$ZXFER_PERF_OUTPUT_DIR"
	zxfer_perf_run_chain_case() { :; }

	output=$(zxfer_perf_run_case_sample chain_remote_mock sample 2 4 5)

	assertContains "Perf stream output should identify the sample position and total case count." \
		"$output" "Starting perf case chain_remote_mock (sample 2/3, case 4/5)"
	assertContains "Perf stream output should explain what the selected case exercises." \
		"$output" "mock-remote chain replication"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
write_sample_perf_rows() {
	l_samples_file=$1

	{
		printf '%s\n' "case	sample_kind	sample_index	status	wall_ms	estimated_send_bytes	throughput_bytes_per_sec	startup_latency_ms	cleanup_ms	elapsed_seconds	ssh_setup_ms	source_snapshot_listing_ms	destination_snapshot_listing_ms	snapshot_diff_sort_ms	zfs_send_calls	zfs_receive_calls	ssh_shell_invocations	send_receive_pipeline_commands	send_receive_background_pipeline_commands	mock_ssh_invocations	stdout	stderr"
		printf '%s\n' "chain_local	sample	1	0	1000	2000	2000.00	100	10	1	0	0	0	0	1	1	0	2	0	0	out1	err1"
		printf '%s\n' "chain_local	sample	2	0	3000	6000	2000.00	300	30	3	0	0	0	0	3	3	0	4	0	0	out2	err2"
		printf '%s\n' "chain_local	warmup	1	0	9999	9999	1.00	999	999	9	0	0	0	0	9	9	9	9	9	9	outw	errw"
		printf '%s\n' "fanout_local_j1_props	sample	1	0	500	1000	2000.00	50	5	1	0	0	0	0	1	1	0	2	0	0	out3	err3"
	} >"$l_samples_file"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_render_summary_aggregates_measured_samples_only() {
	ZXFER_PERF_SAMPLES_FILE="$TEST_TMPDIR/samples.tsv"
	ZXFER_PERF_SUMMARY_FILE="$TEST_TMPDIR/summary.tsv"
	ZXFER_PERF_MARKDOWN_FILE="$TEST_TMPDIR/summary.md"
	write_sample_perf_rows "$ZXFER_PERF_SAMPLES_FILE"

	zxfer_perf_render_summary

	assertContains "The summary should include the measured case." \
		"$(cat "$ZXFER_PERF_SUMMARY_FILE")" "chain_local"
	assertContains "The summary should average wall-clock milliseconds across measured samples." \
		"$(cat "$ZXFER_PERF_SUMMARY_FILE")" "2000.00"
	assertNotContains "Warmup rows should not contribute to the summary." \
		"$(cat "$ZXFER_PERF_SUMMARY_FILE")" "9999"
	assertEquals "Summary rows should preserve first-seen measured case order." \
		"chain_local" "$(awk -F '\t' 'NR == 2 { print $1 }' "$ZXFER_PERF_SUMMARY_FILE")"
	assertEquals "Summary rows should not depend on awk associative-array ordering." \
		"fanout_local_j1_props" "$(awk -F '\t' 'NR == 3 { print $1 }' "$ZXFER_PERF_SUMMARY_FILE")"
	assertContains "The markdown report should render a table row for the case." \
		"$(cat "$ZXFER_PERF_MARKDOWN_FILE")" "| \`chain_local\` |"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_cleanup_fails_successful_run_when_pool_cleanup_fails() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_PERF_SOURCE_ONLY=1
		ZXFER_PERF_TESTS_DIR=\"$TESTS_DIR\"
		. \"$PERF_RUNNER\"
		set +e
		WORKDIR=\"$TEST_TMPDIR/cleanup-work\"
		mkdir -p \"\$WORKDIR\"
		SRC_POOL=src
		DEST_POOL=dst
		SRC_POOL_CREATED=1
		DEST_POOL_CREATED=1
		SRC_IMG=src.img
		DEST_IMG=dst.img
		destroy_test_pool_if_owned() { return 1; }
		safe_rm_rf() { return 0; }
		true
		zxfer_perf_cleanup
		l_cleanup_status=\$?
		printf 'cleanup_status=%s\n' \"\$l_cleanup_status\"
		exit 0
	"

	assertContains "Perf cleanup failures should make otherwise successful runs fail." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "cleanup_status=1"
	assertContains "Perf cleanup failures should preserve the workdir for inspection." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "preserving perf workdir"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_cleanup_preserves_original_failure_status() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_PERF_SOURCE_ONLY=1
		ZXFER_PERF_TESTS_DIR=\"$TESTS_DIR\"
		. \"$PERF_RUNNER\"
		set +e
		WORKDIR=\"$TEST_TMPDIR/cleanup-work-original-failure\"
		mkdir -p \"\$WORKDIR\"
		SRC_POOL=src
		DEST_POOL=dst
		SRC_POOL_CREATED=1
		DEST_POOL_CREATED=1
		SRC_IMG=src.img
		DEST_IMG=dst.img
		destroy_test_pool_if_owned() { return 1; }
		safe_rm_rf() { return 0; }
		return_seven() { return 7; }
		return_seven
		zxfer_perf_cleanup
		l_cleanup_status=\$?
		printf 'cleanup_status=%s\n' \"\$l_cleanup_status\"
		exit 0
	"

	assertContains "Cleanup failures should not mask the original perf failure status." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "cleanup_status=7"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_compare_baseline_writes_regression_annotations_without_failing() {
	ZXFER_PERF_BASELINE="$TEST_TMPDIR/baseline.tsv"
	ZXFER_PERF_SUMMARY_FILE="$TEST_TMPDIR/current.tsv"
	ZXFER_PERF_COMPARE_FILE="$TEST_TMPDIR/compare.tsv"
	printf '%s\n' "case	samples	wall_ms_avg	throughput_bytes_per_sec_avg	startup_latency_ms_avg	cleanup_ms_avg	estimated_send_bytes_avg	mock_ssh_invocations_avg	zfs_send_calls_avg	zfs_receive_calls_avg	ssh_shell_invocations_avg	failed_samples" >"$ZXFER_PERF_BASELINE"
	printf '%s\n' "chain_local	1	1000.00	4000.00	100.00	10.00	2000.00	0.00	1.00	1.00	0.00	0" >>"$ZXFER_PERF_BASELINE"
	printf '%s\n' "case	samples	wall_ms_avg	throughput_bytes_per_sec_avg	startup_latency_ms_avg	cleanup_ms_avg	estimated_send_bytes_avg	mock_ssh_invocations_avg	zfs_send_calls_avg	zfs_receive_calls_avg	ssh_shell_invocations_avg	failed_samples" >"$ZXFER_PERF_SUMMARY_FILE"
	printf '%s\n' "chain_local	1	1300.00	3000.00	130.00	20.00	2000.00	0.00	1.00	1.00	0.00	0" >>"$ZXFER_PERF_SUMMARY_FILE"

	zxfer_perf_compare_baseline

	assertContains "Baseline comparison should write a wall-clock regression annotation." \
		"$(cat "$ZXFER_PERF_COMPARE_FILE")" "wall_ms_avg"
	assertContains "Baseline comparison should mark threshold-crossing deltas as warnings." \
		"$(cat "$ZXFER_PERF_COMPARE_FILE")" "regression"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
