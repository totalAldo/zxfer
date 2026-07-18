#!/bin/sh
# Shared subprocess capture helpers.
# shellcheck disable=SC2016,SC2034,SC2317,SC2329

# These two string-script interfaces are legacy compatibility helpers. Their
# eval sites are inventoried by tests/test_helper_eval_policy.tsv; do not add
# another eval-based capture helper.
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

# Purpose: Write a deterministic ps fixture for process-identity tests when the
# host sandbox denies start-time queries. Only lstart/stime requests are mocked;
# every other invocation is delegated to the caller-provided real ps.
# Usage: Set ZXFER_TEST_REAL_PS, call with the destination path, and prepend the
# destination directory to PATH for the subprocess under test.
zxfer_test_write_process_identity_ps() {
	l_identity_ps_path=$1

	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' 'if [ "$#" -eq 4 ] && [ "$1" = "-p" ] && [ "$3" = "-o" ]; then'
		printf '%s\n' '	case "$4" in'
		printf '%s\n' '	lstart= | stime=)'
		printf '%s\n' '		printf "zxfer-test-start %s\n" "$2"'
		printf '%s\n' '		exit 0'
		printf '%s\n' '		;;'
		printf '%s\n' '	esac'
		printf '%s\n' 'fi'
		printf '%s\n' 'exec "${ZXFER_TEST_REAL_PS:-/bin/ps}" "$@"'
	} >"$l_identity_ps_path" || return 1
	chmod +x "$l_identity_ps_path"
}
