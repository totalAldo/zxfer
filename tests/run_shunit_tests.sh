#!/bin/sh
#
# Run all zxfer shunit2 suites (or a user-specified subset) in sequence.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="$ZXFER_ROOT/tests"

print_usage() {
	cat <<'EOF'
Usage: tests/run_shunit_tests.sh [suite ...]

Runs every shunit2 suite (tests/test_*.sh) when no arguments are provided.
Pass specific suite paths to limit execution, e.g.:

  tests/run_shunit_tests.sh test_zxfer_reporting.sh
  tests/run_shunit_tests.sh tests/test_zxfer_replication.sh

Set ZXFER_TEST_SHELL to an alternate shell executable to run each suite through
that interpreter. For multi-word shell modes such as "bash --posix", point
ZXFER_TEST_SHELL at a wrapper script that execs the desired command.
EOF
}

resolve_suite_path() {
	l_suite=$1
	case "$l_suite" in
	/*)
		printf '%s\n' "$l_suite"
		;;
	"$TEST_DIR"/*)
		printf '%s\n' "$l_suite"
		;;
	tests/*)
		printf '%s\n' "$ZXFER_ROOT/$l_suite"
		;;
	*)
		printf '%s\n' "$TEST_DIR/$l_suite"
		;;
	esac
}

resolve_test_shell_runner() {
	l_test_shell=${ZXFER_TEST_SHELL:-}

	if [ -z "$l_test_shell" ]; then
		TEST_SHELL_RUNNER=""
		TEST_SHELL_LABEL=""
		return 0
	fi

	case "$l_test_shell" in
	*/*)
		l_runner=$l_test_shell
		;;
	*)
		l_runner=$(command -v "$l_test_shell" 2>/dev/null || true)
		;;
	esac

	if [ -z "${l_runner:-}" ] || [ ! -x "$l_runner" ]; then
		echo "ZXFER_TEST_SHELL is not executable: $l_test_shell" >&2
		return 1
	fi

	TEST_SHELL_RUNNER=$l_runner
	TEST_SHELL_LABEL=$l_test_shell
	return 0
}

run_suite_command() {
	l_suite_path=$1

	if [ -n "${TEST_SHELL_RUNNER:-}" ]; then
		"$TEST_SHELL_RUNNER" "$l_suite_path"
	else
		"$l_suite_path"
	fi
}

if [ "$#" -gt 0 ]; then
	case "$1" in
	-h | --help)
		print_usage
		exit 0
		;;
	esac
else
	set -- "$TEST_DIR"/test_*.sh
fi

if [ "$#" -eq 0 ]; then
	echo "No shunit2 suites found in $TEST_DIR" >&2
	exit 1
fi

resolve_test_shell_runner

overall_status=0
passed_count=0
failed_count=0

for suite in "$@"; do
	suite_path=$(resolve_suite_path "$suite")

	if [ ! -f "$suite_path" ]; then
		echo "Skipping missing suite: $suite_path" >&2
		overall_status=1
		failed_count=$((failed_count + 1))
		continue
	fi

	case "$(basename "$suite_path")" in
	test_helper.sh)
		echo "==> Skipping helper library: $suite_path"
		continue
		;;
	esac

	if [ -n "${TEST_SHELL_LABEL:-}" ]; then
		echo "==> Running shunit2 suite with test shell [$TEST_SHELL_LABEL]: $suite_path"
	else
		echo "==> Running shunit2 suite: $suite_path"
	fi
	if run_suite_command "$suite_path"; then
		passed_count=$((passed_count + 1))
	else
		status=$?
		echo "!! Suite failed: $suite_path (exit status $status)" >&2
		overall_status=$status
		failed_count=$((failed_count + 1))
	fi
done

echo "==> shunit2 summary: ${passed_count} passed, ${failed_count} failed"

exit $overall_status
