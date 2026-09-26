#!/bin/sh
#
# Contract tests for developer-facing GitHub Actions validation wiring.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

LINT_WORKFLOW_FILE="$ZXFER_ROOT/.github/workflows/lint.yml"
COVERAGE_WORKFLOW_FILE="$ZXFER_ROOT/.github/workflows/coverage.yml"
UNIT_WORKFLOW_FILE="$ZXFER_ROOT/.github/workflows/tests.yml"
PERF_WORKFLOW_FILE="$ZXFER_ROOT/.github/workflows/perf.yml"
RUN_LINT_BIN="$ZXFER_ROOT/tests/run_lint.sh"
CHECKOUT_PIN="actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1"

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
lint_workflow_matrix_targets() {
	awk '
		/^[[:space:]]+matrix:[[:space:]]*$/ {
			in_matrix = 1
			next
		}
		in_matrix && /^[[:space:]]+target:[[:space:]]*$/ {
			in_targets = 1
			next
		}
		in_targets && /^[[:space:]]+-[[:space:]]+[A-Za-z0-9_-]+[[:space:]]*$/ {
			value = $0
			sub(/^[[:space:]]+-[[:space:]]+/, "", value)
			sub(/[[:space:]]+$/, "", value)
			print value
			next
		}
		in_targets {
			exit
		}
	' "$LINT_WORKFLOW_FILE"
}

# Purpose: Print one job of a workflow, from its "  JOB:" line up to the
# next job.
# Usage: workflow_job_body WORKFLOW_FILE JOB
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
workflow_job_body() {
	awk -v job_name="$2" '
		$0 == "  " job_name ":" {
			in_job = 1
			print
			next
		}
		in_job && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
			exit
		}
		in_job { print }
	' "$1"
}

# Purpose: Print the top-level block that starts at KEY (such as "on" or
# "concurrency") up to the next top-level key.
# Usage: workflow_top_level_block WORKFLOW_FILE KEY
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
workflow_top_level_block() {
	awk -v key="$2" '
		$0 == key ":" {
			in_block = 1
			print
			next
		}
		in_block && /^[^[:space:]#]/ {
			exit
		}
		in_block { print }
	' "$1"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_lint_workflow_runs_every_public_lint_target() {
	runner_targets=$("$RUN_LINT_BIN" --list | sort)
	workflow_targets=$(lint_workflow_matrix_targets | sort)

	assertEquals "The GitHub Actions lint matrix should run every target exposed by the local lint runner." \
		"$runner_targets" "$workflow_targets"
	assertContains "The anti-rebloat budget must remain a required CI lint target." \
		"$workflow_targets" "budget"
	assertContains "Deterministic Solaris man-page rendering must remain a required CI lint target." \
		"$workflow_targets" "manpages"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal workflow expression; invoked indirectly by shunit2.
test_lint_workflow_dispatches_through_the_shared_runner() {
	workflow=$(cat "$LINT_WORKFLOW_FILE")

	assertContains "CI should dispatch matrix targets through the same pinned runner contributors use locally." \
		"$workflow" './tests/run_lint.sh "${{ matrix.target }}"'
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_coverage_workflow_runs_report_only_bash_xtrace_coverage() {
	coverage_job=$(workflow_job_body "$COVERAGE_WORKFLOW_FILE" coverage-bash-xtrace)
	coverage_runner_commands=$(printf '%s\n' "$coverage_job" | awk '
		/\.\/tests\/run_coverage\.sh/ {
			command = $0
			sub(/^[[:space:]]+/, "", command)
			print command
		}
	')

	assertContains "The bash-xtrace coverage job should force the bash-xtrace collector." \
		"$coverage_job" "ZXFER_COVERAGE_MODE: bash-xtrace"
	assertEquals "The CI bash-xtrace lane should run exactly one full-tree report-only command with no policy options." \
		"./tests/run_coverage.sh" "$coverage_runner_commands"
	assertNotContains "Suite failures under the bash-xtrace lane must still fail CI, so the job must not be marked non-blocking." \
		"$coverage_job" "continue-on-error: true"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_coverage_workflow_bounds_advisory_kcov_to_production_suites() {
	workflow=$(cat "$COVERAGE_WORKFLOW_FILE")

	assertContains "The advisory kcov artifact should cover production-focused suites without recursively instrumenting validation tooling." \
		"$workflow" "./tests/run_coverage.sh tests/test_contract_*.sh tests/test_zxfer_*.sh"
	assertContains "The advisory kcov step should remain explicitly non-blocking." \
		"$workflow" "continue-on-error: true"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_unit_workflow_installs_platform_test_prerequisites() {
	workflow=$(cat "$UNIT_WORKFLOW_FILE")

	assertContains "FreeBSD shunit coverage and Git-backed workflow fixtures require bash and Git in the guest." \
		"$workflow" "pkg install -y bash git"
	assertContains "OmniOS uses the same bash wrapper and Git-backed workflow fixtures." \
		"$workflow" "PKG_SUCCESS_ON_NOP=1 pkg install bash git"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal workflow expression; invoked indirectly by shunit2.
test_unit_workflow_bounds_process_heavy_suite_parallelism() {
	workflow=$(cat "$UNIT_WORKFLOW_FILE")
	freebsd_job=$(workflow_job_body "$UNIT_WORKFLOW_FILE" shunit2-freebsd)
	omnios_job=$(workflow_job_body "$UNIT_WORKFLOW_FILE" shunit2-omnios)

	assertNotContains "CI must not launch the entire process-heavy suite inventory concurrently." \
		"$workflow" "--jobs 30"
	assertContains "Linux and portable-shell runners should keep the documented four-worker validation default." \
		"$workflow" "./tests/run_shunit_tests.sh --jobs 4"
	assertContains "The hosted matrix should retain its four-worker Linux entry." \
		"$workflow" "unit_jobs: 4"
	assertContains "macOS should avoid nesting process-heavy suites under a parallel worker." \
		"$workflow" "unit_jobs: 1"
	assertContains "Hosted jobs should consume the validated per-platform worker count." \
		"$workflow" './tests/run_shunit_tests.sh --jobs "${{ matrix.unit_jobs }}"'
	assertContains "FreeBSD should respect its smaller guest CPU allocation." \
		"$freebsd_job" "./tests/run_shunit_tests.sh --jobs 2"
	assertContains "OmniOS should respect its smaller guest CPU allocation." \
		"$omnios_job" '"$bash_bin" ./tests/run_shunit_tests.sh --jobs 2'
	assertContains "The bounded OmniOS unit job should retain its 30-minute guard." \
		"$omnios_job" "timeout-minutes: 30"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal workflow expression; invoked indirectly by shunit2.
test_unit_workflow_gates_a_seeded_argv_fuzz_job() {
	fuzz_job=$(workflow_job_body "$UNIT_WORKFLOW_FILE" argv-fuzz)

	assertContains "The argv fuzz job should seed every run with its run number so a failure reproduces." \
		"$fuzz_job" './tests/run_argv_fuzz.sh --seed "$GITHUB_RUN_NUMBER" --iterations 200'
	assertNotContains "The argv fuzz job gates CI, so it must not be marked non-blocking." \
		"$fuzz_job" "continue-on-error"
	assertContains "The argv fuzz job should use the pinned checkout action." \
		"$fuzz_job" "uses: $CHECKOUT_PIN"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal workflow expression; invoked indirectly by shunit2.
test_perf_workflow_reports_an_advisory_ab_with_full_history() {
	perf_job=$(workflow_job_body "$PERF_WORKFLOW_FILE" perf-advisory)

	printf '%s\n' "$perf_job" | grep -q -x '    continue-on-error: true'
	assertTrue "A slowdown or harness error in the perf job must never fail the workflow (job-level continue-on-error)." $?
	assertContains "The A/B needs full history so origin/upstream-compat-final exists." \
		"$perf_job" "fetch-depth: 0"
	assertContains "The perf job should use the pinned checkout action." \
		"$perf_job" "uses: $CHECKOUT_PIN"
	assertContains "The perf job should compare against upstream-compat-final and write the job summary." \
		"$perf_job" './tests/run_perf_ab.sh --baseline-ref origin/upstream-compat-final --sizes 25,100 --reps 5 --summary "$GITHUB_STEP_SUMMARY"'
	assertContains "The perf job should add the helper spawn counts to the summary." \
		"$perf_job" "./tests/run_microbench.sh"
	assertEquals "The perf workflow should trigger like the other workflows." \
		"$(workflow_top_level_block "$UNIT_WORKFLOW_FILE" on)" \
		"$(workflow_top_level_block "$PERF_WORKFLOW_FILE" on)"
	assertEquals "The perf workflow should cancel superseded runs like the other workflows." \
		"$(workflow_top_level_block "$UNIT_WORKFLOW_FILE" concurrency)" \
		"$(workflow_top_level_block "$PERF_WORKFLOW_FILE" concurrency)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_workflows_use_the_pinned_checkout_action() {
	unpinned=$(grep -h 'uses: actions/checkout@' "$ZXFER_ROOT"/.github/workflows/*.yml |
		sed 's/^[[:space:]-]*uses: //' | grep -v -x -F "$CHECKOUT_PIN")

	assertEquals "Every workflow should check out through the same pinned commit." "" "$unpinned"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
