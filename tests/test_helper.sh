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

# shellcheck source=src/zxfer_modules.sh
ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT \
	ZXFER_SOURCE_MODULES_THROUGH=zxfer_dependencies.sh \
	. "$ZXFER_ROOT/src/zxfer_modules.sh"

zxfer_source_runtime_modules_through() {
	l_last_module=$1
	l_root=${2:-$ZXFER_ROOT}

	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$l_root \
		ZXFER_SOURCE_MODULES_THROUGH=$l_last_module \
		. "$l_root/src/zxfer_modules.sh"
}

zxfer_test_create_tmpdir() {
	l_prefix=$1

	TEST_TMPDIR=$(mktemp -d -t "${l_prefix}.XXXXXX") || {
		echo "Unable to create test temp directory with prefix ${l_prefix}." >&2
		exit 1
	}
}

zxfer_test_cleanup_tmpdir() {
	if [ -n "${TEST_TMPDIR:-}" ]; then
		rm -rf "$TEST_TMPDIR"
	fi
}

zxfer_test_capture_subshell() {
	l_script=$1
	l_restore_errexit=0

	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	# shellcheck disable=SC2034  # Consumed by calling test suites after capture.
	ZXFER_TEST_CAPTURE_OUTPUT=$(
		(
			eval "$l_script"
		) 2>&1
	)
	ZXFER_TEST_CAPTURE_STATUS=$?
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
}

zxfer_test_capture_subshell_split() {
	l_stdout_file=$1
	l_stderr_file=$2
	l_script=$3
	l_restore_errexit=0

	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	(
		eval "$l_script"
	) >"$l_stdout_file" 2>"$l_stderr_file"
	# shellcheck disable=SC2034  # Consumed by calling test suites after capture.
	ZXFER_TEST_CAPTURE_STATUS=$?
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
}

# Most suites only need zxfer_usage() to exist so zxfer_throw_usage_error() has a target.
zxfer_usage() {
	:
}

oneTimeSetUp() {
	:
}

oneTimeTearDown() {
	:
}

setUp() {
	:
}

tearDown() {
	:
}

# Provide sane defaults for globals that zxfer helpers expect.
: "${g_option_n_dryrun:=0}"
: "${g_option_v_verbose:=0}"
: "${g_option_V_very_verbose:=0}"
: "${g_option_b_beep_always:=0}"
: "${g_option_B_beep_on_success:=0}"
