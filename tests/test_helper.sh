#!/bin/sh
#
# Shared helpers for zxfer shunit2 test suites.
#

if [ -n "${TESTS_DIR:-}" ]; then
	ZXFER_ROOT=$TESTS_DIR/..
else
	case "$0" in
	/*)
		ZXFER_ROOT=$(dirname "$0")/..
		;;
	*)
		ZXFER_ROOT=${PWD:-.}/$(dirname "$0")/..
		;;
	esac
fi
SHUNIT2_BIN="$ZXFER_ROOT/tests/shunit2/shunit2"

if [ ! -r "$SHUNIT2_BIN" ]; then
	echo "Missing shunit2 dependency at $SHUNIT2_BIN" >&2
	exit 1
fi

# Keep this file as the stable compatibility entry. Focused helpers are sourced
# in dependency order so suites continue to need only tests/test_helper.sh.
# shellcheck source=tests/helpers/loader.sh
. "$ZXFER_ROOT/tests/helpers/loader.sh"

zxfer_source_dependency_modules_for_tests "$ZXFER_ROOT"

# Test suites should not inherit runner-only environment knobs from the
# developer's shell unless a specific case opts in explicitly.
unset ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
unset ZXFER_TEST_SHELL

# shellcheck source=tests/helpers/lifecycle.sh
. "$ZXFER_ROOT/tests/helpers/lifecycle.sh"
# shellcheck source=tests/helpers/process_capture.sh
. "$ZXFER_ROOT/tests/helpers/process_capture.sh"
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$ZXFER_ROOT/tests/helpers/backup_fixtures.sh"
