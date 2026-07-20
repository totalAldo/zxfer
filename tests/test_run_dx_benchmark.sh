#!/bin/sh
#
# Host-safe contract tests for report-only developer-workflow timing.
#
# shellcheck disable=SC2016,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_dx_benchmark"
	DX_BENCHMARK="$ZXFER_ROOT/tests/run_dx_benchmark.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_RUN_DX_BENCHMARK_SOURCE_ONLY=1
	# shellcheck source=tests/run_dx_benchmark.sh
	. "$DX_BENCHMARK"
	ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1
	# shellcheck source=src/zxfer_cleanup_child_wrapper.sh
	. "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh"
	DX_BENCHMARK_LOG="$TEST_TMPDIR/dx-benchmark-runner.log"
	DX_SIGNAL_BENCHMARK_RECORD=
	DX_SIGNAL_DESCENDANT_RECORDS=
	DX_SIGNAL_RUNNER_RECORD=
	DX_SIGNAL_CHILD_RECORD=
	DX_SIGNAL_LATE_CHILD_RECORD=
	: >"$DX_BENCHMARK_LOG"
}

tearDown() {
	dx_cleanup_signal_fixtures >/dev/null 2>&1 || :
}

write_dx_fake_runner() {
	l_dx_fake_path=$1
	l_dx_fake_label=$2
	l_dx_fake_status=$3
	{
		printf '%s\n' '#!/bin/sh'
		printf "l_label='%s'\n" "$l_dx_fake_label"
		printf "l_status='%s'\n" "$l_dx_fake_status"
		printf '%s\n' 'printf "%s argc=%s\n" "$l_label" "$#" >>"${DX_BENCHMARK_LOG:?}"'
		printf '%s\n' 'for l_arg in "$@"; do printf "%s arg=<%s>\n" "$l_label" "$l_arg" >>"$DX_BENCHMARK_LOG"; done'
		printf '%s\n' 'exit "$l_status"'
	} >"$l_dx_fake_path"
	chmod +x "$l_dx_fake_path"
}

write_dx_fake_time() {
	l_dx_fake_time_path=$1
	l_dx_fake_elapsed=$2
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' '[ "$1" = -p ] || exit 64'
		printf '%s\n' 'shift'
		printf '%s\n' 'l_status=0'
		printf '%s\n' '"$@" || l_status=$?'
		printf "printf 'real %s\\nuser 0.00\\nsys 0.00\\n' >&2\n" "$l_dx_fake_elapsed"
		printf '%s\n' 'exit "$l_status"'
	} >"$l_dx_fake_time_path"
	chmod +x "$l_dx_fake_time_path"
}

dx_wait_for_path() {
	l_dx_wait_path=$1
	l_dx_wait_tries=0
	while [ "$l_dx_wait_tries" -lt 100 ]; do
		[ -e "$l_dx_wait_path" ] && return 0
		sleep 0.1 2>/dev/null || sleep 1
		l_dx_wait_tries=$((l_dx_wait_tries + 1))
	done
	return 1
}

dx_load_owned_record() {
	l_dx_owned_path=$1
	[ -f "$l_dx_owned_path" ] && [ -r "$l_dx_owned_path" ] || return 1
	awk -F '	' '
		NR == 1 && NF == 2 && $1 ~ /^[0-9]+$/ &&
			$2 ~ /^(lstart|stime):[^[:space:]].*/ { record = $0; next }
		{ invalid = 1 }
		END {
			if (!invalid && NR == 1 && record != "") print record
			else exit 1
		}
	' "$l_dx_owned_path"
}

dx_owned_record_live_p() {
	l_dx_owned_pid=$1
	l_dx_owned_token=$2
	l_dx_owned_selector=${l_dx_owned_token%%:*}
	l_dx_owned_current=$(zxfer_cleanup_child_wrapper_get_process_start_token \
		"$l_dx_owned_pid" "$l_dx_owned_selector" 2>/dev/null) || return 1
	[ "$l_dx_owned_current" = "$l_dx_owned_token" ] || return 1
	kill -s 0 "$l_dx_owned_pid" 2>/dev/null || return 1
	l_dx_owned_state=$(LC_ALL=C ps -o stat= -p "$l_dx_owned_pid" 2>/dev/null |
		awk 'NF { print $1; exit }') || l_dx_owned_state=
	case "$l_dx_owned_state" in
	Z*) return 1 ;;
	esac
	return 0
}

dx_owned_records_live_p() {
	l_dx_owned_records=$1
	while IFS='	' read -r l_dx_owned_pid l_dx_owned_token ||
		[ -n "${l_dx_owned_pid}${l_dx_owned_token}" ]; do
		[ -n "$l_dx_owned_pid" ] || continue
		dx_owned_record_live_p "$l_dx_owned_pid" "$l_dx_owned_token" && return 0
	done <<-EOF
		$l_dx_owned_records
	EOF
	return 1
}

dx_wait_for_owned_record() {
	l_dx_wait_record_path=$1
	l_dx_wait_tries=0
	while [ "$l_dx_wait_tries" -lt 100 ]; do
		l_dx_wait_record=$(dx_load_owned_record \
			"$l_dx_wait_record_path" 2>/dev/null) || l_dx_wait_record=
		if [ -n "$l_dx_wait_record" ]; then
			l_dx_wait_record_pid=${l_dx_wait_record%%	*}
			l_dx_wait_record_token=${l_dx_wait_record#*	}
			dx_owned_record_live_p \
				"$l_dx_wait_record_pid" "$l_dx_wait_record_token" && return 0
		fi
		sleep 0.1 2>/dev/null || sleep 1
		l_dx_wait_tries=$((l_dx_wait_tries + 1))
	done
	return 1
}

dx_wait_for_owned_record_exit() {
	l_dx_wait_exit_path=$1
	l_dx_wait_exit_record=$(dx_load_owned_record \
		"$l_dx_wait_exit_path" 2>/dev/null) || return 1
	l_dx_wait_exit_pid=${l_dx_wait_exit_record%%	*}
	l_dx_wait_exit_token=${l_dx_wait_exit_record#*	}
	l_dx_wait_exit_tries=0
	while [ "$l_dx_wait_exit_tries" -lt 100 ]; do
		dx_owned_record_live_p \
			"$l_dx_wait_exit_pid" "$l_dx_wait_exit_token" || return 0
		sleep 0.1 2>/dev/null || sleep 1
		l_dx_wait_exit_tries=$((l_dx_wait_exit_tries + 1))
	done
	return 1
}

dx_cleanup_owned_record_file() {
	l_dx_cleanup_record_path=${1:-}
	[ -n "$l_dx_cleanup_record_path" ] &&
		[ -r "$l_dx_cleanup_record_path" ] || return 0
	l_dx_cleanup_record=$(dx_load_owned_record \
		"$l_dx_cleanup_record_path" 2>/dev/null) || return 1
	zxfer_cleanup_child_wrapper_signal_descendant_records \
		"$l_dx_cleanup_record" KILL >/dev/null 2>&1 || :
	dx_wait_for_owned_record_exit "$l_dx_cleanup_record_path"
}

dx_cleanup_owned_record_set_file() {
	l_dx_cleanup_set_path=${1:-}
	[ -n "$l_dx_cleanup_set_path" ] &&
		[ -r "$l_dx_cleanup_set_path" ] || return 0
	l_dx_cleanup_set_records=$(cat "$l_dx_cleanup_set_path") || return "$?"
	[ -n "$l_dx_cleanup_set_records" ] || return 0
	zxfer_cleanup_child_wrapper_signal_descendant_records \
		"$l_dx_cleanup_set_records" KILL >/dev/null 2>&1 || :
	l_dx_cleanup_set_tries=0
	while [ "$l_dx_cleanup_set_tries" -lt 100 ]; do
		dx_owned_records_live_p \
			"$l_dx_cleanup_set_records" || return 0
		sleep 0.1 2>/dev/null || sleep 1
		l_dx_cleanup_set_tries=$((l_dx_cleanup_set_tries + 1))
	done
	return 1
}

dx_cleanup_signal_fixtures() {
	l_dx_fixture_cleanup_status=0
	dx_cleanup_owned_record_set_file \
		"${DX_SIGNAL_DESCENDANT_RECORDS:-}" || l_dx_fixture_cleanup_status=$?
	dx_cleanup_owned_record_file \
		"${DX_SIGNAL_LATE_CHILD_RECORD:-}" || l_dx_fixture_cleanup_status=$?
	dx_cleanup_owned_record_file \
		"${DX_SIGNAL_CHILD_RECORD:-}" || l_dx_fixture_cleanup_status=$?
	dx_cleanup_owned_record_file \
		"${DX_SIGNAL_RUNNER_RECORD:-}" || l_dx_fixture_cleanup_status=$?
	dx_cleanup_owned_record_file \
		"${DX_SIGNAL_BENCHMARK_RECORD:-}" || l_dx_fixture_cleanup_status=$?
	return "$l_dx_fixture_cleanup_status"
}

test_dx_benchmark_dispatches_fixed_argv_and_records_all_default_cases() {
	named_runner="$TEST_TMPDIR/named runner.sh"
	quick_runner="$TEST_TMPDIR/quick runner.sh"
	shunit_runner="$TEST_TMPDIR/shunit runner.sh"
	validate_runner="$TEST_TMPDIR/validate runner.sh"
	fake_time="$TEST_TMPDIR/fake time.sh"
	output_dir="$TEST_TMPDIR/evidence with spaces"
	injection_marker="$TEST_TMPDIR/should-not-exist"
	write_dx_fake_runner "$named_runner" named 0
	write_dx_fake_runner "$quick_runner" quick 0
	write_dx_fake_runner "$shunit_runner" shunit 0
	write_dx_fake_runner "$validate_runner" validate 0
	write_dx_fake_time "$fake_time" 0.25

	DX_BENCHMARK_LOG="$DX_BENCHMARK_LOG" \
		ZXFER_DX_BENCHMARK_TIME="$fake_time" \
		"$DX_BENCHMARK" \
		--output-dir "$output_dir" \
		--samples 1 \
		--named-warmups 1 \
		--named-suite "tests/suite path;touch $injection_marker" \
		--named-test test_selected_case \
		--quick-path "src/path with spaces;touch $injection_marker" \
		--runner named "$named_runner" \
		--runner quick "$quick_runner" \
		--runner shunit "$shunit_runner" \
		--runner validate "$validate_runner" >/dev/null
	status=$?

	assertEquals "Successful validation commands should make the evidence run succeed." 0 "$status"
	assertEquals "The named test should run once for warmup and once for its measured sample." \
		2 "$(awk -F '\t' 'NR > 1 && $1 == "named" { count++ } END { print count + 0 }' "$output_dir/results.tsv")"
	assertEquals "Every selected workflow case should receive one measured sample." \
		4 "$(awk -F '\t' 'NR > 1 && $2 == "sample" { count++ } END { print count + 0 }' "$output_dir/results.tsv")"
	verified_group_count=0
	for supervisor_record in "$output_dir"/logs/*.supervisor.group; do
		zxfer_dx_benchmark_load_supervisor_record "$supervisor_record" >/dev/null 2>&1 ||
			continue
		verified_group_count=$((verified_group_count + 1))
	done
	assertEquals "Every warmup and sample must prove a private resident process group before runner go." \
		5 "$verified_group_count"
	assertEquals "The report-only summary should retain the fixed default case order." \
		"named
quick
shunit
validate" "$(awk -F '\t' 'NR > 1 { print $1 }' "$output_dir/summary.tsv")"
	assertEquals "Fixed dispatch must preserve every argument boundary, including spaces and shell metacharacters." \
		"named argc=4
named arg=<--suite>
named arg=<tests/suite path;touch $injection_marker>
named arg=<--test>
named arg=<test_selected_case>
named argc=4
named arg=<--suite>
named arg=<tests/suite path;touch $injection_marker>
named arg=<--test>
named arg=<test_selected_case>
quick argc=2
quick arg=<quick>
quick arg=<src/path with spaces;touch $injection_marker>
shunit argc=0
validate argc=1
validate arg=<full>" "$(cat "$DX_BENCHMARK_LOG")"
	assertFalse "Metacharacters in fixed case arguments must never execute as shell text." \
		"[ -e \"$injection_marker\" ]"
	assertEquals "Evidence directories should remain private under the caller's umask." \
		700 "$(zxfer_get_path_mode_octal "$output_dir")"
	assertContains "Metadata must explicitly identify the non-enforcing contract." \
		"$(cat "$output_dir/metadata.tsv")" "report_only"
	assertContains "Metadata must explicitly identify the non-enforcing contract." \
		"$(cat "$output_dir/metadata.tsv")" "yes"
	assertContains "Metadata should retain the exact named-suite argument used for reproducibility." \
		"$(cat "$output_dir/metadata.tsv")" "named_suite	tests/suite path;touch $injection_marker"
	assertContains "Metadata should retain each selected runner path." \
		"$(cat "$output_dir/metadata.tsv")" "runner_validate	$validate_runner"
	assertContains "Metadata should retain the timing utility path." \
		"$(cat "$output_dir/metadata.tsv")" "time_utility	$fake_time"
	assertContains "Machine-readable timing should pin the C locale." \
		"$(cat "$output_dir/metadata.tsv")" "locale	C"
}

test_dx_benchmark_narrows_and_deduplicates_cases_without_validating_unselected_runners() {
	quick_runner="$TEST_TMPDIR/narrow-quick.sh"
	fake_time="$TEST_TMPDIR/narrow-time.sh"
	output_dir="$TEST_TMPDIR/narrow-output"
	write_dx_fake_runner "$quick_runner" quick 0
	write_dx_fake_time "$fake_time" 0.10

	DX_BENCHMARK_LOG="$DX_BENCHMARK_LOG" \
		ZXFER_DX_BENCHMARK_TIME="$fake_time" \
		"$DX_BENCHMARK" \
		--output-dir "$output_dir" \
		--case quick,quick \
		--case quick \
		--samples 2 \
		--runner quick "$quick_runner" >/dev/null
	status=$?

	assertEquals "A safely narrowed case set should complete." 0 "$status"
	assertEquals "Repeated case selectors should execute one case for each requested sample." \
		2 "$(awk -F '\t' 'NR > 1 { count++ } END { print count + 0 }' "$output_dir/results.tsv")"
	assertEquals "Narrow selection should not run or summarize unselected commands." \
		quick "$(awk -F '\t' 'NR > 1 { print $1 }' "$output_dir/summary.tsv")"
	assertEquals "Only the selected quick runner should execute." \
		"quick argc=2
quick arg=<quick>
quick arg=<tests/validate.sh>
quick argc=2
quick arg=<quick>
quick arg=<tests/validate.sh>" "$(cat "$DX_BENCHMARK_LOG")"

	ZXFER_DX_BENCHMARK_OUTPUT_DIR="$TEST_TMPDIR/source-validation-output"
	ZXFER_DX_BENCHMARK_CASES=quick
	ZXFER_DX_BENCHMARK_QUICK_RUNNER=$quick_runner
	ZXFER_DX_BENCHMARK_NAMED_RUNNER="$TEST_TMPDIR/unselected-missing-runner"
	zxfer_dx_benchmark_validate_args
	assertEquals "Unselected cases should not impose tool availability on a narrowed measurement." 0 "$?"
}

test_dx_benchmark_rejects_unsafe_paths_options_and_runner_strings() {
	existing_dir="$TEST_TMPDIR/existing-dx-output"
	missing_parent="$TEST_TMPDIR/missing-parent/output"
	valid_runner="$TEST_TMPDIR/unsafe-options-valid-runner.sh"
	carriage_return=$(printf '\r')
	mkdir "$existing_dir"
	write_dx_fake_runner "$valid_runner" valid 0

	ZXFER_DX_BENCHMARK_CASES=quick
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=$existing_dir
	zxfer_dx_benchmark_validate_args >/dev/null 2>&1
	existing_status=$?
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=$missing_parent
	zxfer_dx_benchmark_validate_args >/dev/null 2>&1
	missing_parent_status=$?
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=-option-like-output
	zxfer_dx_benchmark_validate_args >/dev/null 2>&1
	option_path_status=$?
	ZXFER_DX_BENCHMARK_OUTPUT_DIR="$TEST_TMPDIR/carriage${carriage_return}return"
	zxfer_dx_benchmark_validate_args >/dev/null 2>&1
	carriage_path_status=$?
	zxfer_dx_benchmark_parse_args --case named,,quick >/dev/null 2>&1
	empty_case_status=$?
	zxfer_dx_benchmark_parse_args --case quick, >/dev/null 2>&1
	trailing_case_status=$?
	zxfer_dx_benchmark_parse_args --case ,quick >/dev/null 2>&1
	leading_case_status=$?
	zxfer_dx_benchmark_parse_args --runner quick 'sh -c true' >/dev/null 2>&1
	string_runner_parse_status=$?
	ZXFER_DX_BENCHMARK_OUTPUT_DIR="$TEST_TMPDIR/new-output"
	ZXFER_DX_BENCHMARK_CASES=quick
	ZXFER_DX_BENCHMARK_QUICK_RUNNER='sh -c true'
	zxfer_dx_benchmark_validate_args >/dev/null 2>&1
	string_runner_validate_status=$?
	ZXFER_DX_BENCHMARK_QUICK_RUNNER=$valid_runner
	ZXFER_DX_BENCHMARK_TIME='time'
	zxfer_dx_benchmark_validate_args >/dev/null 2>&1
	time_command_name_status=$?

	assertEquals "Existing evidence directories must never be overwritten." 1 "$existing_status"
	assertEquals "Evidence creation should require an existing parent directory." 1 "$missing_parent_status"
	assertEquals "Relative output paths that look like options must fail closed." 1 "$option_path_status"
	assertEquals "TSV artifact paths must reject carriage returns." 1 "$carriage_path_status"
	assertEquals "Empty comma-delimited case names must fail closed." 1 "$empty_case_status"
	assertEquals "Trailing comma-delimited case names must fail closed before command substitution drops the empty row." \
		1 "$trailing_case_status"
	assertEquals "Leading comma-delimited case names must fail closed." 1 "$leading_case_status"
	assertEquals "A runner path is parsed as one opaque argument, not a shell command string." 0 "$string_runner_parse_status"
	assertEquals "Shell command strings must not satisfy the executable-path contract." 1 "$string_runner_validate_status"
	assertEquals "The timing utility must be passed as one executable path, not a PATH command name." \
		1 "$time_command_name_status"
}

test_dx_benchmark_propagates_durable_results_append_failures() {
	quick_runner="$TEST_TMPDIR/artifact-failure-runner.sh"
	fake_time="$TEST_TMPDIR/artifact-failure-time.sh"
	output_dir="$TEST_TMPDIR/artifact-failure-output"
	write_dx_fake_runner "$quick_runner" quick 0
	write_dx_fake_time "$fake_time" 0.20
	mkdir -p "$output_dir/logs" "$output_dir/results-target"
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=$output_dir
	ZXFER_DX_BENCHMARK_RESULTS_FILE="$output_dir/results-target"
	ZXFER_DX_BENCHMARK_TIME=$fake_time
	ZXFER_DX_BENCHMARK_SCRIPT=$DX_BENCHMARK

	zxfer_dx_benchmark_measure_argv quick sample 1 "$quick_runner" quick tests/validate.sh \
		>/dev/null 2>&1
	append_status=$?

	assertEquals "A failed durable TSV append must propagate instead of reporting successful evidence collection." \
		1 "$append_status"
}

test_dx_benchmark_atomic_output_creation_preserves_a_race_winner() {
	race_output="$TEST_TMPDIR/dx-race-output"
	race_sentinel="$race_output/sentinel"
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=$race_output

	mkdir() {
		l_dx_race_matches=0
		for l_dx_race_arg in "$@"; do
			[ "$l_dx_race_arg" = "$race_output" ] && l_dx_race_matches=1
		done
		if [ "$l_dx_race_matches" -eq 1 ] && [ ! -e "$race_output" ]; then
			command mkdir "$race_output" || return "$?"
			printf '%s\n' race-winner >"$race_sentinel" || return "$?"
		fi
		command mkdir "$@"
	}
	zxfer_dx_benchmark_create_output_dir >/dev/null 2>&1
	race_status=$?
	unset -f mkdir

	assertFalse "A competing output-directory creator must make evidence publication fail." \
		"[ \"$race_status\" -eq 0 ]"
	assertEquals "The timing runner must preserve a concurrent creator's artifacts." \
		race-winner "$(cat "$race_sentinel")"
	assertFalse "The timing runner must not publish into a directory it did not claim." \
		"[ -e \"$race_output/results.tsv\" ]"
}

test_dx_benchmark_never_enforces_elapsed_time_thresholds() {
	quick_runner="$TEST_TMPDIR/slow-report-only-runner.sh"
	fake_time="$TEST_TMPDIR/slow-report-only-time.sh"
	output_dir="$TEST_TMPDIR/slow-report-only-output"
	write_dx_fake_runner "$quick_runner" quick 0
	write_dx_fake_time "$fake_time" 999.00

	DX_BENCHMARK_LOG="$DX_BENCHMARK_LOG" \
		ZXFER_DX_BENCHMARK_TIME="$fake_time" \
		"$DX_BENCHMARK" --output-dir "$output_dir" --case quick \
		--runner quick "$quick_runner" >/dev/null
	status=$?

	assertEquals "Arbitrarily large elapsed evidence must not fail a successful command." 0 "$status"
	assertEquals "Large timing evidence should be reported unchanged instead of classified against a limit." \
		999.000000 "$(awk -F '\t' 'NR == 2 { print $4 }' "$output_dir/summary.tsv")"
	assertEquals "Nearest-rank P95 should remain report-only evidence alongside the median." \
		999.000000 "$(awk -F '\t' 'NR == 2 { print $5 }' "$output_dir/summary.tsv")"
	assertNotContains "The durable summary must contain no timing gate or threshold result." \
		"$(cat "$output_dir/summary.tsv")" "threshold"
	assertNotContains "The durable summary must contain no timing gate or threshold result." \
		"$(cat "$output_dir/summary.tsv")" "timing_gate"
}

test_dx_benchmark_records_command_failures_and_preserves_evidence() {
	failing_runner="$TEST_TMPDIR/failing-runner.sh"
	fake_time="$TEST_TMPDIR/failing-time.sh"
	output_dir="$TEST_TMPDIR/failing-output"
	write_dx_fake_runner "$failing_runner" quick 7
	write_dx_fake_time "$fake_time" 0.40

	DX_BENCHMARK_LOG="$DX_BENCHMARK_LOG" \
		ZXFER_DX_BENCHMARK_TIME="$fake_time" \
		"$DX_BENCHMARK" --output-dir "$output_dir" --case quick \
		--runner quick "$failing_runner" >/dev/null
	status=$?

	assertEquals "A failed validation command should fail the evidence collection without becoming a timing gate." 1 "$status"
	assertEquals "The original command status must remain durable evidence." \
		7 "$(awk -F '\t' 'NR == 2 { print $5 }' "$output_dir/results.tsv")"
	assertEquals "The summary should classify command status, not elapsed-time acceptability." \
		failed "$(awk -F '\t' 'NR == 2 { print $6 }' "$output_dir/summary.tsv")"
	assertEquals "Failed command timings must remain raw evidence rather than misleading summary latency." \
		unavailable "$(awk -F '\t' 'NR == 2 { print $4 }' "$output_dir/summary.tsv")"
	assertEquals "An all-failed case should have no P95 among successful samples." \
		unavailable "$(awk -F '\t' 'NR == 2 { print $5 }' "$output_dir/summary.tsv")"
	assertTrue "Command stdout should remain available after a failed sample." \
		"[ -f \"$output_dir/logs/quick-sample-1.stdout\" ]"
	assertTrue "Command stderr should remain available after a failed sample." \
		"[ -f \"$output_dir/logs/quick-sample-1.stderr\" ]"
}

test_dx_benchmark_summary_uses_nearest_rank_p95() {
	output_dir="$TEST_TMPDIR/p95-output"
	mkdir "$output_dir"
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=$output_dir
	ZXFER_DX_BENCHMARK_RESULTS_FILE="$output_dir/results.tsv"
	ZXFER_DX_BENCHMARK_CASES=quick
	{
		printf 'case\tphase\titeration\telapsed_seconds\texit_status\tstdout_file\tstderr_file\ttime_file\n'
		printf 'quick\tsample\t1\t1.00\t0\t-\t-\t-\n'
		printf 'quick\tsample\t2\t2.00\t0\t-\t-\t-\n'
		printf 'quick\tsample\t3\t3.00\t0\t-\t-\t-\n'
		printf 'quick\tsample\t4\t9.00\t0\t-\t-\t-\n'
	} >"$ZXFER_DX_BENCHMARK_RESULTS_FILE"

	zxfer_dx_benchmark_write_summary

	assertEquals "The median should retain the conventional midpoint for an even sample count." \
		2.500000 "$(awk -F '\t' 'NR == 2 { print $4 }' "$output_dir/summary.tsv")"
	assertEquals "Nearest-rank P95 should select ceil(0.95*n), including the slowest of four samples." \
		9.000000 "$(awk -F '\t' 'NR == 2 { print $5 }' "$output_dir/summary.tsv")"
}

test_dx_benchmark_source_only_preserves_nounset_and_positional_arguments() {
	source_status=0
	source_output=$(
		/bin/sh -c '
			l_script=$1
			l_root=$2
			set +u
			set -- --exec-supervisor one two
			l_before=$-
			ZXFER_RUN_DX_BENCHMARK_SOURCE_ONLY=1
			ZXFER_DX_BENCHMARK_ROOT=$l_root
			. "$l_script"
			l_after=$-
			case "$l_before" in *u*) l_before_u=yes ;; *) l_before_u=no ;; esac
			case "$l_after" in *u*) l_after_u=yes ;; *) l_after_u=no ;; esac
			printf "%s:%s:%s:%s:%s:%s\n" \
				"$l_before_u" "$l_after_u" "$#" "$1" "$2" "$3"
		' zxfer-dx-source "$DX_BENCHMARK" "$ZXFER_ROOT"
	) || source_status=$?

	assertEquals "Source-only loading must not dispatch a caller's --exec-supervisor positional argument." \
		0 "$source_status"
	assertEquals "Source-only loading must preserve disabled nounset and every caller positional argument." \
		"no:no:3:--exec-supervisor:one:two" "$source_output"
}

test_dx_benchmark_supervisor_records_require_pid_as_group_leader() {
	valid_record="$TEST_TMPDIR/valid-supervisor.group"
	mismatched_record="$TEST_TMPDIR/mismatched-supervisor.group"
	extra_record="$TEST_TMPDIR/extra-supervisor.group"
	printf '%s\t%s\n' 701 701 >"$valid_record"
	printf '%s\t%s\n' 701 702 >"$mismatched_record"
	printf '%s\t%s\n%s\n' 701 701 extra >"$extra_record"

	assertEquals "A supervisor may publish only a process group led by its own PID." \
		"701	701" "$(zxfer_dx_benchmark_load_supervisor_record "$valid_record")"
	assertFalse "A process outside its own private group must fail closed before runner launch." \
		"zxfer_dx_benchmark_load_supervisor_record \"$mismatched_record\""
	assertFalse "Supervisor records must remain exact one-row protocol messages." \
		"zxfer_dx_benchmark_load_supervisor_record \"$extra_record\""
}

test_dx_benchmark_defers_signal_until_active_child_is_published() {
	publish_marker="$TEST_TMPDIR/deferred-signal-publish"
	go_marker="$TEST_TMPDIR/deferred-signal-go"
	publish_status=0
	(
		zxfer_dx_benchmark_stop_active() {
			printf '%s\t%s\n' \
				"$ZXFER_DX_BENCHMARK_ACTIVE_PID" \
				"$ZXFER_DX_BENCHMARK_ACTIVE_PGID" \
				>"$publish_marker"
		}
		zxfer_dx_benchmark_begin_active_launch
		zxfer_dx_benchmark_handle_signal TERM
		[ "$ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL" = TERM ] || exit 65
		zxfer_dx_benchmark_publish_active_launch \
			4321 4321
		: >"$go_marker"
		exit 66
	) || publish_status=$?

	assertEquals "A deferred TERM should retain the conventional status after publication." \
		143 "$publish_status"
	assertEquals "The pending signal must run cleanup only after the owned child and supervisor record are visible." \
		"4321	4321" \
		"$(cat "$publish_marker")"
	assertFalse "A signal received before readiness must retire the group before runner go is published." \
		"[ -e \"$go_marker\" ]"
}

run_dx_benchmark_signal_case() {
	l_dx_case_signal=$1
	l_dx_case_status=$2
	l_dx_case_label=$(printf '%s\n' "$l_dx_case_signal" |
		tr '[:upper:]' '[:lower:]')
	signal_runner="$TEST_TMPDIR/signal-$l_dx_case_label-runner.sh"
	output_dir="$TEST_TMPDIR/signal-$l_dx_case_label-output"
	ready_file="$TEST_TMPDIR/signal-$l_dx_case_label-ready"
	benchmark_record_file="$TEST_TMPDIR/signal-$l_dx_case_label-benchmark.record"
	descendant_records_file="$TEST_TMPDIR/signal-$l_dx_case_label-descendants.records"
	runner_record_file="$TEST_TMPDIR/signal-$l_dx_case_label-runner.record"
	child_record_file="$TEST_TMPDIR/signal-$l_dx_case_label-child.record"
	late_child_record_file="$TEST_TMPDIR/signal-$l_dx_case_label-late-child.record"
	runner_term_file="$TEST_TMPDIR/signal-$l_dx_case_label-runner.term"
	child_term_file="$TEST_TMPDIR/signal-$l_dx_case_label-child.term"
	benchmark_output="$TEST_TMPDIR/signal-$l_dx_case_label-benchmark.out"
	DX_SIGNAL_BENCHMARK_RECORD=$benchmark_record_file
	DX_SIGNAL_DESCENDANT_RECORDS=$descendant_records_file
	DX_SIGNAL_RUNNER_RECORD=$runner_record_file
	DX_SIGNAL_CHILD_RECORD=$child_record_file
	DX_SIGNAL_LATE_CHILD_RECORD=$late_child_record_file
	cat >"$signal_runner" <<'EOF'
#!/bin/sh
ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1
export ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
# shellcheck source=src/zxfer_cleanup_child_wrapper.sh
. "${DX_SIGNAL_PROCESS_HELPERS:?}"
write_owned_record() {
	l_signal_record_pid=$1
	l_signal_record_path=$2
	for l_signal_record_selector in lstart stime; do
		l_signal_record_token=$(zxfer_cleanup_child_wrapper_get_process_start_token \
			"$l_signal_record_pid" "$l_signal_record_selector" 2>/dev/null) || continue
		printf '%s\t%s\n' "$l_signal_record_pid" "$l_signal_record_token" \
			>"$l_signal_record_path"
		return "$?"
	done
	return 1
}
write_owned_record "$$" "${DX_SIGNAL_RUNNER_RECORD:?}" || exit 69
(
	trap 'printf "%s\n" term >"${DX_SIGNAL_CHILD_TERM:?}"; exit 143' TERM
	l_signal_child_ticks=0
	while [ "$l_signal_child_ticks" -lt 20 ]; do
		sleep 1
		l_signal_child_ticks=$((l_signal_child_ticks + 1))
	done
) &
l_child=$!
write_owned_record "$l_child" "${DX_SIGNAL_CHILD_RECORD:?}" || {
	wait "$l_child" 2>/dev/null || :
	exit 69
}
on_term() {
	(
		l_signal_late_ticks=0
		while [ "$l_signal_late_ticks" -lt 20 ]; do
			sleep 1
			l_signal_late_ticks=$((l_signal_late_ticks + 1))
		done
	) &
	l_late_child=$!
	write_owned_record "$l_late_child" "${DX_SIGNAL_LATE_CHILD_RECORD:?}" || :
	printf '%s\n' term >"${DX_SIGNAL_RUNNER_TERM:?}"
	exit 143
}
trap 'on_term' TERM
: >"${DX_SIGNAL_READY:?}"
wait "$l_child"
EOF
	chmod +x "$signal_runner"

	(
		dx_wait_for_path "$ready_file" || exit 1
		dx_wait_for_owned_record "$benchmark_record_file" || exit 1
		l_dx_signaller_record=$(dx_load_owned_record \
			"$benchmark_record_file" 2>/dev/null) || exit 1
		l_dx_signaller_pid=${l_dx_signaller_record%%	*}
		l_dx_signaller_token=${l_dx_signaller_record#*	}
		dx_owned_record_live_p \
			"$l_dx_signaller_pid" "$l_dx_signaller_token" || exit 1
		l_dx_signaller_descendants=$(zxfer_cleanup_child_wrapper_list_descendants \
			"$l_dx_signaller_pid" 2>/dev/null) || exit 1
		printf '%s\n' "$l_dx_signaller_descendants" \
			>"$descendant_records_file.tmp" || exit 1
		zxfer_cleanup_child_wrapper_signal_descendant_records \
			"$l_dx_signaller_record" "$l_dx_case_signal" || exit 1
		mv "$descendant_records_file.tmp" "$descendant_records_file"
	) &
	signaller_pid=$!
	benchmark_status=0
	DX_SIGNAL_READY=$ready_file \
		DX_SIGNAL_RUNNER_RECORD=$runner_record_file \
		DX_SIGNAL_CHILD_RECORD=$child_record_file \
		DX_SIGNAL_LATE_CHILD_RECORD=$late_child_record_file \
		DX_SIGNAL_RUNNER_TERM=$runner_term_file \
		DX_SIGNAL_CHILD_TERM=$child_term_file \
		DX_SIGNAL_BENCHMARK_RECORD=$benchmark_record_file \
		DX_SIGNAL_PROCESS_HELPERS="$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh" \
		/bin/sh -c '
			ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1
			export ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
			. "${DX_SIGNAL_PROCESS_HELPERS:?}"
			l_benchmark_token=
			for l_benchmark_selector in lstart stime; do
				l_benchmark_token=$(zxfer_cleanup_child_wrapper_get_process_start_token \
					"$$" "$l_benchmark_selector" 2>/dev/null) || continue
				break
			done
			[ -n "$l_benchmark_token" ] || exit 69
			printf "%s\t%s\n" "$$" "$l_benchmark_token" \
				>"${DX_SIGNAL_BENCHMARK_RECORD:?}" || exit 69
			exec "$@"
		' zxfer-dx-signal "$DX_BENCHMARK" \
		--output-dir "$output_dir" --case shunit \
		--runner shunit "$signal_runner" >"$benchmark_output" 2>&1 ||
		benchmark_status=$?
	wait "$signaller_pid"
	signaller_status=$?
	runner_exited=no
	dx_wait_for_owned_record_exit "$runner_record_file" && runner_exited=yes
	child_exited=no
	dx_wait_for_owned_record_exit "$child_record_file" && child_exited=yes
	late_child_created=no
	if [ -e "$late_child_record_file" ]; then
		late_child_created=yes
	fi
	dx_cleanup_signal_fixtures || :

	assertEquals "The signal helper should observe a ready benchmark process." 0 "$signaller_status"
	assertEquals "$l_dx_case_signal should retain the conventional shell exit status." \
		"$l_dx_case_status" "$benchmark_status"
	assertTrue "$l_dx_case_signal cleanup should wait for the timed runner to exit. Benchmark output: $(cat "$benchmark_output")" \
		"[ \"$runner_exited\" = yes ]"
	assertTrue "$l_dx_case_signal cleanup should discover and stop a non-cooperatively supervised child. Benchmark output: $(cat "$benchmark_output")" \
		"[ \"$child_exited\" = yes ]"
	assertFalse "Catchable signals must not reach an arbitrary runner that could fork and exit before ancestry refresh." \
		"[ -f \"$runner_term_file\" ]"
	assertFalse "Catchable signals must not reach descendants before the process tree is frozen." \
		"[ -f \"$child_term_file\" ]"
	assertFalse "The runner's fork-then-exit TERM trap must not get an opportunity to create an escaping child." \
		"[ \"$late_child_created\" = yes ]"
}

test_dx_benchmark_term_stops_the_active_runner_and_its_child() {
	run_dx_benchmark_signal_case TERM 143
}

test_dx_benchmark_refuses_to_launch_without_verified_group_isolation() {
	runner_marker="$TEST_TMPDIR/unisolated-runner"
	launch_status=0
	(
		set +m 2>/dev/null || :
		zxfer_dx_benchmark_enable_job_control() {
			return 1
		}
		zxfer_dx_benchmark_resolve_setsid() {
			return 1
		}
		zxfer_dx_benchmark_launch_supervisor /bin/sh -c \
			': >"$1"' zxfer-unisolated "$runner_marker"
	) || launch_status=$?

	assertEquals "A host without a verified group launcher must fail before arbitrary code starts." \
		1 "$launch_status"
	assertFalse "Isolation failure must not execute the requested runner." \
		"[ -e \"$runner_marker\" ]"
}

test_dx_benchmark_uses_fixed_argv_setsid_fallback_without_shell_text() {
	fake_setsid="$TEST_TMPDIR/fake-setsid.sh"
	runner_marker="$TEST_TMPDIR/setsid-runner"
	job_control_marker="$TEST_TMPDIR/job-control-enabled"
	cat >"$fake_setsid" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$fake_setsid"
	launch_status=0
	(
		set +m 2>/dev/null || :
		zxfer_dx_benchmark_enable_job_control() {
			: >"$job_control_marker"
			return 1
		}
		zxfer_dx_benchmark_resolve_setsid() {
			printf '%s\n' "$fake_setsid"
		}
		zxfer_dx_benchmark_launch_supervisor /bin/sh -c \
			': >"$1"' zxfer-setsid-fallback "$runner_marker" || exit $?
		wait "$g_zxfer_dx_benchmark_launch_pid"
	) || launch_status=$?

	assertEquals "The resolved setsid launcher should preserve argv and waitable child ownership." \
		0 "$launch_status"
	assertTrue "The fixed-argv setsid launcher should execute the exact requested command." \
		"[ -e \"$runner_marker\" ]"
	assertFalse "A resolved setsid launcher should avoid mutating non-interactive job-control state." \
		"[ -e \"$job_control_marker\" ]"
}

test_dx_benchmark_group_signals_use_busybox_portable_kill_argv() {
	output=$(
		zxfer_dx_benchmark_run_kill() {
			printf 'kill'
			for l_test_kill_arg; do
				printf ':<%s>' "$l_test_kill_arg"
			done
			printf '\n'
		}
		ZXFER_DX_BENCHMARK_ACTIVE_PID=7000
		ZXFER_DX_BENCHMARK_ACTIVE_PGID=7000
		zxfer_dx_benchmark_active_group_exists_p
		zxfer_dx_benchmark_signal_active_group STOP
	)

	assertContains "The existence probe should pass a negative process group directly after the signal." \
		"$output" "kill:<-s>:<0>:<-7000>"
	assertContains "Supervisor teardown should use the BusyBox-compatible signal form." \
		"$output" "kill:<-s>:<STOP>:<-7000>"
	assertNotContains "BusyBox treats -- after -s SIGNAL as a PID rather than an option terminator." \
		"$output" "<-->"
}

test_dx_benchmark_supervisor_refuses_go_when_it_is_not_group_leader() {
	runner_marker="$TEST_TMPDIR/nonleader-runner"
	ready_file="$TEST_TMPDIR/nonleader-ready"
	go_file="$TEST_TMPDIR/nonleader-go"
	cancel_file="$TEST_TMPDIR/nonleader-cancel"
	status_file="$TEST_TMPDIR/nonleader-status"
	time_file="$TEST_TMPDIR/nonleader-time"
	stderr_file="$TEST_TMPDIR/nonleader-stderr"
	: >"$go_file"
	supervisor_status=0
	(
		zxfer_dx_benchmark_get_process_group() {
			printf '%s\n' 99999
		}
		zxfer_dx_benchmark_exec_supervisor /usr/bin/time "$time_file" \
			"$ready_file" "$go_file" "$cancel_file" "$status_file" \
			"$stderr_file" /bin/sh -c ': >"$1"' zxfer-nonleader \
			"$runner_marker"
	) || supervisor_status=$?

	assertEquals "A supervisor outside its own process group must fail closed." \
		69 "$supervisor_status"
	assertFalse "Group verification must happen before readiness is published." \
		"[ -e \"$ready_file\" ]"
	assertFalse "An unisolated supervisor must never consume go or run arbitrary code." \
		"[ -e \"$runner_marker\" ]"
}

test_dx_benchmark_stop_failure_never_attempts_a_later_group_kill() {
	signal_log="$TEST_TMPDIR/stop-failure-signals"
	wait_marker="$TEST_TMPDIR/stop-failure-wait"
	stop_status=0
	(
		ZXFER_DX_BENCHMARK_ACTIVE_PID=7000
		ZXFER_DX_BENCHMARK_ACTIVE_PGID=7000
		zxfer_dx_benchmark_signal_active_group() {
			printf '%s\n' "$1" >>"$signal_log"
			return 1
		}
		zxfer_dx_benchmark_active_group_exists_p() {
			return 0
		}
		wait() {
			: >"$wait_marker"
		}
		zxfer_dx_benchmark_stop_active
	) || stop_status=$?

	assertEquals "A failed group freeze must fail closed." 1 "$stop_status"
	assertEquals "KILL must never target a numeric group that STOP did not pin." \
		STOP "$(cat "$signal_log")"
	assertFalse "A failed STOP must return without waiting on an unretired supervisor." \
		"[ -e \"$wait_marker\" ]"
}

test_dx_benchmark_stop_success_kills_and_waits_for_the_resident_group_leader() {
	signal_log="$TEST_TMPDIR/stop-success-signals"
	wait_marker="$TEST_TMPDIR/stop-success-wait"
	active_marker="$TEST_TMPDIR/stop-success-active"
	(
		ZXFER_DX_BENCHMARK_ACTIVE_PID=7000
		ZXFER_DX_BENCHMARK_ACTIVE_PGID=7000
		zxfer_dx_benchmark_signal_active_group() {
			printf '%s\n' "$1" >>"$signal_log"
		}
		wait() {
			printf '%s\n' "$1" >"$wait_marker"
		}
		zxfer_dx_benchmark_stop_active
		printf '%s\n' "$ZXFER_DX_BENCHMARK_ACTIVE_PID" >"$active_marker"
	)

	assertEquals "Successful retirement must freeze before forceful group teardown." \
		"STOP
KILL" "$(cat "$signal_log")"
	assertEquals "Retirement should wait only for the trusted resident group leader." \
		7000 "$(cat "$wait_marker")"
	assertEquals "Retirement must clear the active PID after wait." \
		"" "$(cat "$active_marker")"
}

test_dx_benchmark_stop_active_does_not_depend_on_post_ready_ps() {
	signal_log="$TEST_TMPDIR/no-post-ready-ps-signals"
	(
		ZXFER_DX_BENCHMARK_ACTIVE_PID=7000
		ZXFER_DX_BENCHMARK_ACTIVE_PGID=7000
		ps() {
			return 1
		}
		zxfer_dx_benchmark_signal_active_group() {
			printf '%s\n' "$1" >>"$signal_log"
		}
		wait() {
			return 0
		}
		zxfer_dx_benchmark_stop_active
	)

	assertEquals "Once readiness proves isolation, teardown must not reopen a ps/PID race." \
		"STOP
KILL" "$(cat "$signal_log")"
}

test_dx_benchmark_int_runs_active_cleanup_and_returns_130() {
	int_marker="$TEST_TMPDIR/int-cleanup"
	int_status=0
	(
		zxfer_dx_benchmark_stop_active() {
			printf '%s\n' cleaned >"$int_marker"
		}
		ZXFER_DX_BENCHMARK_DEFER_SIGNALS=0
		zxfer_dx_benchmark_handle_signal INT
		exit 66
	) || int_status=$?

	assertEquals "INT should retain the conventional shell exit status." 130 "$int_status"
	assertEquals "INT should run the same active-process cleanup path as TERM." \
		cleaned "$(cat "$int_marker")"
}

test_dx_benchmark_repeated_term_cannot_interrupt_committed_cleanup() {
	repeat_marker="$TEST_TMPDIR/repeated-term-cleanup"
	repeat_status=0
	ZXFER_RUN_DX_BENCHMARK_SOURCE_ONLY=1 \
		ZXFER_DX_BENCHMARK_ROOT="$ZXFER_ROOT" \
		/bin/sh -c '
			. "$1"
			l_marker=$2
			zxfer_dx_benchmark_stop_active() {
				kill -s TERM "$$" || exit 67
				printf "%s\n" completed >"$l_marker"
			}
			ZXFER_DX_BENCHMARK_DEFER_SIGNALS=0
			zxfer_dx_benchmark_handle_signal TERM
		' zxfer-repeat-term "$DX_BENCHMARK" "$repeat_marker" || repeat_status=$?

	assertEquals "The initiating TERM should retain its conventional status." \
		143 "$repeat_status"
	assertEquals "A repeated TERM must be ignored until committed group cleanup completes." \
		completed "$(cat "$repeat_marker")"
}

test_dx_benchmark_signal_cleanup_failure_returns_125() {
	cleanup_status=0
	(
		zxfer_dx_benchmark_stop_active() {
			return 1
		}
		ZXFER_DX_BENCHMARK_DEFER_SIGNALS=0
		zxfer_dx_benchmark_handle_signal TERM
		exit 66
	) || cleanup_status=$?

	assertEquals "An unconfirmed process teardown must fail closed instead of waiting without a bound." \
		125 "$cleanup_status"
}

test_dx_benchmark_source_contains_no_eval_command_path() {
	eval_matches=$(awk '
		$0 ~ /(^|[^A-Za-z0-9_])eval([[:space:]]|$)/ && $0 !~ /^[[:space:]]*#/ { print NR ":" $0 }
	' "$DX_BENCHMARK")
	assertEquals "Runner overrides and case dispatch must not use eval." "" "$eval_matches"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
