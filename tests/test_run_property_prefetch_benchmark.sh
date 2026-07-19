#!/bin/sh
# Host-safe contract tests for the recursive property-prefetch benchmark.
# shellcheck disable=SC2317,SC2329

TESTS_DIR=$(dirname "$0")
# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_property_prefetch_benchmark"
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	PROPERTY_PREFETCH_BENCHMARK="$ZXFER_PROPERTY_PREFETCH_BENCHMARK_ROOT/tests/run_property_prefetch_benchmark.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_RUN_PROPERTY_PREFETCH_BENCHMARK_SOURCE_ONLY=1
	# shellcheck source=tests/run_property_prefetch_benchmark.sh
	. "$PROPERTY_PREFETCH_BENCHMARK"
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK=$(command -v awk)
}

test_property_prefetch_benchmark_parses_and_validates_documented_options() {
	output_dir="$TEST_TMPDIR/documented-options"
	zxfer_property_prefetch_benchmark_parse_args \
		--awk "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK" \
		--samples 5 --warmups 0 --output-dir "$output_dir"
	parse_status=$?
	zxfer_property_prefetch_benchmark_validate_args
	validate_status=$?

	assertEquals "The benchmark should accept its documented option surface." 0 "$parse_status"
	assertEquals "The benchmark should accept a new explicit artifact directory." 0 "$validate_status"
	assertEquals "The sample count should remain configurable without changing fixed workloads." \
		5 "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES"
	assertEquals "Zero warmups should be valid for contract-level smoke runs." \
		0 "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_WARMUPS"
}

test_property_prefetch_benchmark_rejects_unsafe_or_ambiguous_invocations() {
	existing_dir="$TEST_TMPDIR/existing-output"
	mkdir -p "$existing_dir"
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR=$existing_dir

	set +e
	zxfer_property_prefetch_benchmark_validate_args >/dev/null 2>&1
	existing_status=$?
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR="$TEST_TMPDIR/new-output"
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES=0
	zxfer_property_prefetch_benchmark_validate_args >/dev/null 2>&1
	sample_status=$?
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_SAMPLES=1
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR="$TEST_TMPDIR/missing-parent/output"
	zxfer_property_prefetch_benchmark_validate_args >/dev/null 2>&1
	missing_parent_status=$?
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR=-relative-output
	zxfer_property_prefetch_benchmark_validate_args >/dev/null 2>&1
	relative_option_status=$?
	zxfer_property_prefetch_benchmark_parse_args --unknown >/dev/null 2>&1
	unknown_status=$?

	assertEquals "Existing artifact directories must not be overwritten." 1 "$existing_status"
	assertEquals "Timing samples must remain positive." 1 "$sample_status"
	assertEquals "Artifact creation should require an existing parent directory." 1 "$missing_parent_status"
	assertEquals "Relative artifact paths that look like options must fail closed." 1 "$relative_option_status"
	assertEquals "Unknown options must fail closed." 1 "$unknown_status"
}

test_property_prefetch_benchmark_atomic_output_creation_preserves_race_winner() {
	race_output_dir="$TEST_TMPDIR/race-output"
	race_sentinel="$race_output_dir/sentinel"

	# Create the competing directory immediately before the benchmark's real
	# mkdir. With mkdir -p, the second call would succeed and write artifacts.
	mkdir() {
		l_race_mkdir_matches=0
		for l_race_mkdir_arg in "$@"; do
			[ "$l_race_mkdir_arg" = "$race_output_dir" ] && l_race_mkdir_matches=1
		done
		if [ "$l_race_mkdir_matches" -eq 1 ] && [ ! -e "$race_output_dir" ]; then
			command mkdir "$race_output_dir" || return "$?"
			printf '%s\n' race-winner >"$race_sentinel" || return "$?"
		fi
		command mkdir "$@"
	}
	# Keep a regressed mkdir -p implementation from running the measured work.
	zxfer_property_prefetch_benchmark_detect_memory() { :; }
	zxfer_property_prefetch_benchmark_run_size() { :; }
	zxfer_property_prefetch_benchmark_write_summary() {
		: >"$4"
	}

	set +e
	zxfer_property_prefetch_benchmark_main \
		--samples 1 --warmups 0 --output-dir "$race_output_dir" >/dev/null 2>&1
	race_status=$?
	unset -f mkdir

	assertFalse "A competing output-directory creator must make the benchmark fail." \
		"[ \"$race_status\" -eq 0 ]"
	assertEquals "The benchmark must preserve artifacts owned by the race winner." \
		race-winner "$(cat "$race_sentinel")"
	assertFalse "The benchmark must not publish headers into a directory it did not create." \
		"[ -e \"$race_output_dir/samples.tsv\" ]"
}

test_property_prefetch_benchmark_entry_paths_force_stable_c_locale() {
	locale_log="$TEST_TMPDIR/benchmark-locale.log"
	: >"$locale_log"

	(
		LOCALE_LOG="$locale_log"
		LC_ALL=POSIX
		export LC_ALL
		zxfer_property_prefetch_benchmark_parse_args() {
			ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR="$TEST_TMPDIR/locale-main"
		}
		zxfer_property_prefetch_benchmark_validate_args() { :; }
		zxfer_property_prefetch_benchmark_create_output_dir() {
			mkdir "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR"
		}
		zxfer_property_prefetch_benchmark_detect_memory() {
			printf 'main=%s\n' "$LC_ALL" >>"$LOCALE_LOG"
		}
		zxfer_property_prefetch_benchmark_run_size() { :; }
		zxfer_property_prefetch_benchmark_write_summary() {
			printf '%s\n' summary >"$4"
		}
		zxfer_property_prefetch_benchmark_main >/dev/null 2>&1 || exit "$?"

		LC_ALL=POSIX
		export LC_ALL
		zxfer_property_prefetch_benchmark_load_programs() { :; }
		zxfer_property_prefetch_benchmark_run_once() {
			printf 'worker=%s\n' "$LC_ALL" >>"$LOCALE_LOG"
		}
		zxfer_property_prefetch_benchmark_worker candidate awk \
			"$TEST_TMPDIR/locale-fixture" "$TEST_TMPDIR/locale-worker" 1 \
			>/dev/null 2>&1
	)
	status=$?

	assertEquals "Both benchmark executable entry paths should complete under an inherited non-C locale." \
		0 "$status"
	assertEquals "Benchmark timing and AWK work should always inherit the canonical C numeric locale." \
		"main=C
worker=C" "$(cat "$locale_log")"
}

test_property_prefetch_benchmark_normalizes_relative_assignment_like_output_paths() {
	physical_test_tmpdir=$(cd "$TEST_TMPDIR" && pwd -P)
	output=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR='evidence=run'
			zxfer_property_prefetch_benchmark_create_output_dir || exit "$?"
			printf 'output=%s\n' "$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR"
			zxfer_property_prefetch_benchmark_test_write_evidence \
				"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR" 80 900
			ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE=bsd
			ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT=bytes
			zxfer_property_prefetch_benchmark_write_summary \
				"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/identity.tsv" \
				"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/samples.tsv" \
				"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/memory.tsv" \
				"$ZXFER_PROPERTY_PREFETCH_BENCHMARK_OUTPUT_DIR/summary.tsv"
			printf 'summary-status=%s\n' "$?"
		)
	)

	assertContains "New benchmark artifact directories should become absolute before AWK sees their child paths." \
		"$output" "output=$physical_test_tmpdir/evidence=run"
	assertContains "Assignment-like relative output names should retain complete benchmark evidence." \
		"$output" "summary-status=0"
}

test_property_prefetch_benchmark_fixture_is_deterministic_and_offline() {
	first_fixture="$TEST_TMPDIR/fixture-first"
	second_fixture="$TEST_TMPDIR/fixture-second"
	backslash_fixture="$TEST_TMPDIR/fixture-\\test"
	baseline_scratch="$TEST_TMPDIR/baseline-scratch"
	candidate_scratch="$TEST_TMPDIR/candidate-scratch"
	forbidden_log="$TEST_TMPDIR/forbidden-command.log"
	mkdir -p "$baseline_scratch" "$candidate_scratch"

	zfs() {
		printf '%s\n' zfs >>"$forbidden_log"
		return 99
	}
	ssh() {
		printf '%s\n' ssh >>"$forbidden_log"
		return 99
	}
	zxfer_property_prefetch_benchmark_generate_fixture 12 "$first_fixture"
	zxfer_property_prefetch_benchmark_generate_fixture 12 "$second_fixture"
	zxfer_property_prefetch_benchmark_generate_fixture 12 "$backslash_fixture"
	zxfer_property_prefetch_benchmark_load_programs
	g_cmd_awk=$ZXFER_PROPERTY_PREFETCH_BENCHMARK_AWK
	zxfer_property_prefetch_benchmark_run_once baseline \
		"$first_fixture/filter.tsv" "$first_fixture/machine.tsv" \
		"$first_fixture/human.tsv" "$baseline_scratch"
	baseline_status=$?
	zxfer_property_prefetch_benchmark_run_once candidate \
		"$first_fixture/filter.tsv" "$first_fixture/machine.tsv" \
		"$first_fixture/human.tsv" "$candidate_scratch"
	candidate_status=$?
	unset -f zfs ssh

	cmp -s "$first_fixture/filter.tsv" "$second_fixture/filter.tsv"
	filter_cmp_status=$?
	cmp -s "$first_fixture/machine.tsv" "$second_fixture/machine.tsv"
	machine_cmp_status=$?
	cmp -s "$first_fixture/human.tsv" "$second_fixture/human.tsv"
	human_cmp_status=$?
	cmp -s "$first_fixture/filter.tsv" "$backslash_fixture/filter.tsv" &&
		cmp -s "$first_fixture/machine.tsv" "$backslash_fixture/machine.tsv" &&
		cmp -s "$first_fixture/human.tsv" "$backslash_fixture/human.tsv"
	backslash_cmp_status=$?
	cmp -s "$baseline_scratch/output.tsv" "$candidate_scratch/output.tsv"
	output_cmp_status=$?
	filter_rows=$(wc -l <"$first_fixture/filter.tsv" | awk '{ print $1 }')

	assertEquals "Fixture generation should be deterministic for the dataset filter." 0 "$filter_cmp_status"
	assertEquals "Fixture generation should be deterministic for the machine view." 0 "$machine_cmp_status"
	assertEquals "Fixture generation should be deterministic for the human view." 0 "$human_cmp_status"
	assertEquals "Fixture generation should preserve literal backslashes in artifact paths." 0 "$backslash_cmp_status"
	assertEquals "The fixture should contain the requested number of datasets." 12 "$filter_rows"
	assertEquals "The characterized baseline pipeline should accept generated fixtures." 0 "$baseline_status"
	assertEquals "The candidate pipeline should accept generated fixtures." 0 "$candidate_status"
	assertEquals "Generated fixtures must produce byte-identical baseline and candidate output." 0 "$output_cmp_status"
	assertFalse "The host-safe benchmark must not invoke ZFS or SSH." "[ -e \"$forbidden_log\" ]"
}

zxfer_property_prefetch_benchmark_test_write_evidence() {
	evidence_dir=$1
	candidate_1000_ms=$2
	candidate_1000_rss=$3
	mkdir -p "$evidence_dir"
	printf 'dataset_count\tbyte_identical\n100\tyes\n1000\tyes\n' >"$evidence_dir/identity.tsv"
	printf 'dataset_count\timplementation\tsample\titerations\telapsed_seconds\telapsed_ms_per_iteration\n' >"$evidence_dir/samples.tsv"
	printf '100\tbaseline\t1\t1\t0.010\t10\n100\tcandidate\t1\t1\t0.009\t9\n' >>"$evidence_dir/samples.tsv"
	printf '1000\tbaseline\t1\t1\t0.100\t100\n1000\tcandidate\t1\t1\t0.080\t%s\n' \
		"$candidate_1000_ms" >>"$evidence_dir/samples.tsv"
	printf 'dataset_count\timplementation\tsample\tpeak_rss\tunit\n' >"$evidence_dir/memory.tsv"
	printf '100\tbaseline\t1\t1000\tbytes\n100\tcandidate\t1\t900\tbytes\n' >>"$evidence_dir/memory.tsv"
	printf '1000\tbaseline\t1\t1000\tbytes\n1000\tcandidate\t1\t%s\tbytes\n' \
		"$candidate_1000_rss" >>"$evidence_dir/memory.tsv"
}

test_property_prefetch_benchmark_summary_enforces_timing_and_memory_gates() {
	passing_dir="$TEST_TMPDIR/passing-evidence"
	failing_dir="$TEST_TMPDIR/failing-evidence"
	zxfer_property_prefetch_benchmark_test_write_evidence "$passing_dir" 80 900
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE=bsd
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT=bytes
	zxfer_property_prefetch_benchmark_write_summary \
		"$passing_dir/identity.tsv" "$passing_dir/samples.tsv" \
		"$passing_dir/memory.tsv" "$passing_dir/summary.tsv"
	passing_status=$?

	zxfer_property_prefetch_benchmark_test_write_evidence "$failing_dir" 95 1100
	set +e
	zxfer_property_prefetch_benchmark_write_summary \
		"$failing_dir/identity.tsv" "$failing_dir/samples.tsv" \
		"$failing_dir/memory.tsv" "$failing_dir/summary.tsv"
	failing_status=$?

	assertEquals "Evidence meeting every threshold should pass the gate." 0 "$passing_status"
	assertContains "Passing summaries should retain the measured 1,000-dataset improvement." \
		"$(cat "$passing_dir/summary.tsv")" "$(printf '20.00\tpass')"
	assertEquals "A sub-10% improvement or RSS regression should reject the candidate." 1 "$failing_status"
	assertContains "Rejected evidence should be explicit in the durable summary." \
		"$(cat "$failing_dir/summary.tsv")" "fail"
}

test_property_prefetch_benchmark_summary_preserves_literal_backslash_paths() {
	backslash_dir="$TEST_TMPDIR/summary-\\test"
	zxfer_property_prefetch_benchmark_test_write_evidence "$backslash_dir" 80 900
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_MODE=bsd
	ZXFER_PROPERTY_PREFETCH_BENCHMARK_MEMORY_UNIT=bytes

	zxfer_property_prefetch_benchmark_write_summary \
		"$backslash_dir/identity.tsv" "$backslash_dir/samples.tsv" \
		"$backslash_dir/memory.tsv" "$backslash_dir/summary.tsv"
	status=$?

	assertEquals "Benchmark summary parsing should not let AWK decode backslashes in filename operands." \
		0 "$status"
	assertContains "Literal backslash artifact paths should retain passing 1,000-dataset evidence." \
		"$(cat "$backslash_dir/summary.tsv")" "1000	yes"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
