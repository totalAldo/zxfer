#!/bin/sh
# Shared subprocess capture helpers.
# shellcheck disable=SC2034,SC2317,SC2329

# Purpose: Run a command or scenario function in an isolated subshell, retaining
# its combined output and status. Arguments remain literal argv elements.
# Usage: zxfer_test_capture_subshell COMMAND [ARG...]. The scenario starts with
# errexit disabled; it may enable it explicitly. Restore the caller's errexit.
zxfer_test_capture_subshell() {
	zxfer_test_capture_command combined "$@"
}

# Purpose: Capture an isolated command or scenario's stdout and stderr separately.
# Usage: zxfer_test_capture_subshell_split STDOUT_FILE STDERR_FILE COMMAND [ARG...].
# Errexit and status follow zxfer_test_capture_subshell; leave the output global alone.
zxfer_test_capture_subshell_split() {
	zxfer_test_capture_command split "$@"
}

# Purpose: Share option restoration and status collection between capture modes.
# Usage: Called through the combined/split adapters above.
zxfer_test_capture_command() {
	l_capture_restore_errexit=0
	case $- in *e*) l_capture_restore_errexit=1 ;; esac
	set +e
	case $1 in
	combined)
		shift
		ZXFER_TEST_CAPTURE_OUTPUT=$(
			("$@") 2>&1
		)
		;;
	split)
		l_capture_stdout_file=$2
		l_capture_stderr_file=$3
		shift 3
		("$@") >"$l_capture_stdout_file" 2>"$l_capture_stderr_file"
		;;
	esac
	ZXFER_TEST_CAPTURE_STATUS=$?
	if [ "$l_capture_restore_errexit" = 1 ]; then
		set -e
	fi
}
