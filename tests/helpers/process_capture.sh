#!/bin/sh
# Shared subprocess capture helpers.
# shellcheck disable=SC2016,SC2034,SC2317,SC2329

# These two string-script interfaces are legacy compatibility helpers. The
# test-helper eval policy in tests/run_lint.sh allows exactly these two eval
# sites; do not add another eval-based capture helper.
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
