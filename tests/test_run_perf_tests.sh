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
	ZXFER_PERF_LABEL=current
	ZXFER_PERF_CASES=""
	ZXFER_PERF_SAMPLES=""
	ZXFER_PERF_WARMUPS=""
	ZXFER_PERF_OUTPUT_DIR=""
	ZXFER_PERF_BASELINE=""
	ZXFER_PERF_REGRESSION_THRESHOLD_PCT=10
	ZXFER_PERF_YES=0
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_parse_args_accepts_profile_label_cases_and_counts() {
	zxfer_perf_parse_args --yes --label candidate --profile standard --case chain_local,chain_remote_mock --samples 5 --warmups 2 --output-dir "$TEST_TMPDIR/out" --baseline "$TEST_TMPDIR/base.tsv"

	assertEquals "The perf runner should parse --yes." "1" "$ZXFER_PERF_YES"
	assertEquals "The perf runner should parse run labels." "candidate" "$ZXFER_PERF_LABEL"
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
write_sample_perf_row() {
	l_label=$1
	l_case=$2
	l_kind=$3
	l_index=$4
	l_status=$5
	l_wall_ms=$6
	l_bytes=$7
	l_throughput=$8
	l_startup_ms=$9
	shift 9
	l_cleanup_ms=$1
	l_elapsed_seconds=$2
	l_mock_ssh=$3
	l_stdout=$4
	l_stderr=$5

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
		"$l_label" "$l_case" "$l_kind" "$l_index" "$l_status" "$l_wall_ms" "$l_bytes" "$l_throughput"
	for l_metric in $ZXFER_PERF_PROFILE_METRICS; do
		case "$l_metric" in
		startup_latency_ms)
			l_value=$l_startup_ms
			;;
		cleanup_ms)
			l_value=$l_cleanup_ms
			;;
		elapsed_seconds)
			l_value=$l_elapsed_seconds
			;;
		zfs_send_calls | zfs_receive_calls)
			l_value=$l_index
			;;
		ssh_shell_invocations)
			l_value=0
			;;
		send_receive_pipeline_commands)
			l_value=$((l_index + 1))
			;;
		*)
			l_value=0
			;;
		esac
		printf '\t%s' "$l_value"
	done
	printf '\t%s\t%s\t%s\n' "$l_mock_ssh" "$l_stdout" "$l_stderr"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
write_sample_perf_rows() {
	l_samples_file=$1

	{
		zxfer_perf_print_sample_header
		write_sample_perf_row "candidate" "chain_local" "sample" 1 0 1000 2000 "2000.00" 100 10 1 0 out1 err1
		write_sample_perf_row "candidate" "chain_local" "sample" 2 0 3000 6000 "2000.00" 300 30 3 0 out2 err2
		write_sample_perf_row "candidate" "chain_local" "warmup" 1 0 9999 9999 "1.00" 999 999 9 9 outw errw
		write_sample_perf_row "candidate" "fanout_local_j1_props" "sample" 1 0 500 1000 "2000.00" 50 5 1 0 out3 err3
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
		"chain_local" "$(awk -F '\t' 'NR == 2 { print $2 }' "$ZXFER_PERF_SUMMARY_FILE")"
	assertEquals "Summary rows should not depend on awk associative-array ordering." \
		"fanout_local_j1_props" "$(awk -F '\t' 'NR == 3 { print $2 }' "$ZXFER_PERF_SUMMARY_FILE")"
	assertContains "The markdown report should render a table row for the case." \
		"$(cat "$ZXFER_PERF_MARKDOWN_FILE")" "| \`chain_local\` |"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_record_sample_preserves_missing_profile_counters_as_empty() {
	ZXFER_PERF_LABEL=older-upstream
	ZXFER_PERF_SAMPLES_FILE="$TEST_TMPDIR/samples-missing-counters.tsv"
	stdout_file="$TEST_TMPDIR/sample.stdout"
	stderr_file="$TEST_TMPDIR/sample.stderr"
	: >"$stdout_file"
	printf '%s\n' "zxfer profile: startup_latency_ms=77" >"$stderr_file"
	zxfer_perf_print_sample_header >"$ZXFER_PERF_SAMPLES_FILE"

	zxfer_perf_record_sample chain_local sample 1 0 100 1000 "$stdout_file" "$stderr_file" ""

	result=$(awk -F '\t' '
		NR == 1 {
			for (i = 1; i <= NF; i++) col[$i] = i
			next
		}
		NR == 2 {
			printf "label=%s startup=%s cleanup=<%s> runtime_files=<%s>\n", $1, $(col["startup_latency_ms"]), $(col["cleanup_ms"]), $(col["runtime_artifact_files_created"])
		}
	' "$ZXFER_PERF_SAMPLES_FILE")

	assertContains "Sample rows should include the run label." \
		"$result" "label=older-upstream"
	assertContains "Present -V counters should be recorded." \
		"$result" "startup=77"
	assertContains "Missing -V counters should stay empty rather than becoming zero." \
		"$result" "cleanup=<>"
	assertContains "New counters missing from older output should stay empty." \
		"$result" "runtime_files=<>"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_record_sample_sanitizes_freeform_tsv_fields() {
	ZXFER_PERF_LABEL=$(printf 'candidate\tlabel\nnext')
	ZXFER_PERF_SAMPLES_FILE="$TEST_TMPDIR/samples-sanitized.tsv"
	stdout_file=$(printf 'stdout\tpath\nnext')
	stderr_file=$(printf 'stderr\tpath\nnext')
	zxfer_perf_print_sample_header >"$ZXFER_PERF_SAMPLES_FILE"

	zxfer_perf_record_sample chain_local sample 1 0 100 1000 "$stdout_file" "$stderr_file" ""

	line_count=$(wc -l <"$ZXFER_PERF_SAMPLES_FILE" | awk '{ print $1 }')
	result=$(awk -F '\t' '
		NR == 1 {
			expected_nf = NF
			for (i = 1; i <= NF; i++) col[$i] = i
			next
		}
		NR == 2 {
			printf "same_nf=%s label=<%s> stdout=<%s> stderr=<%s>\n", (NF == expected_nf ? "yes" : "no"), $(col["run_label"]), $(col["stdout"]), $(col["stderr"])
		}
	' "$ZXFER_PERF_SAMPLES_FILE")

	assertEquals "TSV free-form fields should not introduce extra rows." \
		"2" "$line_count"
	assertContains "TSV free-form fields should not introduce extra columns." \
		"$result" "same_nf=yes"
	assertContains "The run label should be tab/newline sanitized." \
		"$result" "label=<candidate label next>"
	assertContains "The stdout path should be tab/newline sanitized." \
		"$result" "stdout=<stdout path next>"
	assertContains "The stderr path should be tab/newline sanitized." \
		"$result" "stderr=<stderr path next>"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_run_case_sample_dispatches_noop_cases() {
	ZXFER_PERF_OUTPUT_DIR="$TEST_TMPDIR/perf-noop-dispatch"
	ZXFER_PERF_SAMPLES=1
	ZXFER_PERF_WARMUPS=0
	dispatch_log="$TEST_TMPDIR/noop-dispatch.log"
	mkdir -p "$ZXFER_PERF_OUTPUT_DIR"
	: >"$dispatch_log"

	zxfer_perf_run_chain_case() {
		printf 'chain:%s:%s:%s\n' "$1" "$4" "${6:-0}" >>"$dispatch_log"
	}
	zxfer_perf_run_fanout_case() {
		printf 'fanout:%s:%s:%s\n' "$1" "$4" "${5:-0}" >>"$dispatch_log"
	}

	zxfer_perf_run_case_sample chain_local_noop sample 1 1 3
	zxfer_perf_run_case_sample fanout_local_j4_props_noop sample 1 2 3
	zxfer_perf_run_case_sample chain_remote_mock_noop sample 1 3 3

	output=$(cat "$dispatch_log")
	assertContains "Local chain no-op should set the no-op flag." \
		"$output" "chain:chain_local_noop:0:1"
	assertContains "Fanout no-op should keep j4 and set the no-op flag." \
		"$output" "fanout:fanout_local_j4_props_noop:4:1"
	assertContains "Remote mock no-op should keep the remote flag and set the no-op flag." \
		"$output" "chain:chain_remote_mock_noop:1:1"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_perf_write_run_info_records_label_versions_and_fixture_sizes() {
	ZXFER_PERF_RUN_INFO_FILE="$TEST_TMPDIR/run-info.tsv"
	ZXFER_PERF_LABEL=baseline-ref
	ZXFER_PERF_PROFILE=smoke
	ZXFER_PERF_CASES="chain_local"
	ZXFER_PERF_SAMPLES=2
	ZXFER_PERF_WARMUPS=1
	ZXFER_PERF_SPARSE_SIZE_MB=512
	ZXFER_PERF_CHAIN_SNAPSHOTS=6
	ZXFER_PERF_FANOUT_DATASETS=8
	ZXFER_PERF_PAYLOAD_MB=1
	ZXFER_BIN="$TEST_TMPDIR/zxfer"
	printf '%s\n' "#!/bin/sh" "exit 0" >"$ZXFER_BIN"
	chmod 700 "$ZXFER_BIN"
	zfs() { printf '%s\n' "zfs-test-version"; }
	zpool() { printf '%s\n' "zpool-test-version"; }

	zxfer_perf_write_run_info

	output=$(cat "$ZXFER_PERF_RUN_INFO_FILE")
	assertContains "Run info should record the run label." "$output" "label	baseline-ref"
	assertContains "Run info should record the case list." "$output" "cases	chain_local"
	assertContains "Run info should record zfs versions." "$output" "zfs_version	zfs-test-version"
	assertContains "Run info should record zpool versions." "$output" "zpool_version	zpool-test-version"
	assertContains "Run info should record fixture sizes." "$output" "fixture_chain_snapshots	6"
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
