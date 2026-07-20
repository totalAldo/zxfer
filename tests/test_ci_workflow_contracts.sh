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
RUN_LINT_BIN="$ZXFER_ROOT/tests/run_lint.sh"

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

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
coverage_policy_job_body() {
	awk '
		/^  coverage-bash-xtrace:[[:space:]]*$/ {
			in_policy_job = 1
		}
		in_policy_job && /^  [A-Za-z0-9_-]+:[[:space:]]*$/ &&
			$0 !~ /^  coverage-bash-xtrace:[[:space:]]*$/ {
			exit
		}
		in_policy_job { print }
	' "$COVERAGE_WORKFLOW_FILE"
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
test_coverage_workflow_explicitly_enforces_the_bash_xtrace_policy() {
	policy_job=$(coverage_policy_job_body)
	policy_runner_commands=$(printf '%s\n' "$policy_job" | awk '
		/\.\/tests\/run_coverage\.sh/ {
			command = $0
			sub(/^[[:space:]]+/, "", command)
			print command
		}
	')

	assertContains "The coverage policy job should force the bash-xtrace collector." \
		"$policy_job" "ZXFER_COVERAGE_MODE: bash-xtrace"
	assertEquals "The CI policy lane should run exactly one full-tree enforcement command with no contradictory options." \
		"./tests/run_coverage.sh --enforce" "$policy_runner_commands"
	assertNotContains "The required coverage policy job must not be allowed to fail without failing CI." \
		"$policy_job" "continue-on-error: true"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_coverage_workflow_bounds_advisory_kcov_to_production_suites() {
	workflow=$(cat "$COVERAGE_WORKFLOW_FILE")

	assertContains "The advisory kcov artifact should cover production-focused suites without recursively instrumenting validation tooling." \
		"$workflow" "./tests/run_coverage.sh tests/test_zxfer_*.sh"
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

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_unit_workflow_bounds_process_heavy_suite_parallelism() {
	workflow=$(cat "$UNIT_WORKFLOW_FILE")

	assertNotContains "CI must not launch the entire process-heavy suite inventory concurrently." \
		"$workflow" "--jobs 30"
	assertContains "Hosted runners should match the documented four-worker validation default." \
		"$workflow" "./tests/run_shunit_tests.sh --jobs 4"
	assertContains "VM-backed platform jobs should respect their smaller guest CPU allocation." \
		"$workflow" "./tests/run_shunit_tests.sh --jobs 2"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
