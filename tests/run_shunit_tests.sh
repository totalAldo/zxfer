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

  tests/run_shunit_tests.sh test_zxfer_common.sh
  tests/run_shunit_tests.sh tests/test_zxfer_zfs_mode.sh
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

	echo "==> Running shunit2 suite: $suite_path"
	if "$suite_path"; then
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
