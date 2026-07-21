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
# host sandbox denies process queries. Start tokens are mocked, and runner-owned
# parent/child relationships can be reconstructed from private *.owner.pid and
# *.pid state records. Tests may make one complete token lookup empty, keep all
# token lookups empty for one PID, or signal a recorded runner while it inspects
# a recorded worker.
# Usage: Set ZXFER_TEST_REAL_PS, call with the destination path, and prepend the
# destination directory to PATH for the subprocess under test.
zxfer_test_write_process_identity_ps() {
	l_identity_ps_path=$1

	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' 'zxfer_test_print_process_relationships() {'
		printf '%s\n' '	for l_test_owner_pid_file in "$@"; do'
		printf '%s\n' '		[ -r "$l_test_owner_pid_file" ] || continue'
		printf '%s\n' '		l_test_child_pid_file=${l_test_owner_pid_file%.owner.pid}.pid'
		printf '%s\n' '		[ -r "$l_test_child_pid_file" ] || continue'
		printf '%s\n' '		IFS= read -r l_test_owner_pid <"$l_test_owner_pid_file" || continue'
		printf '%s\n' '		IFS= read -r l_test_child_pid <"$l_test_child_pid_file" || continue'
		printf '%s\n' '		case "$l_test_owner_pid" in'
		printf '%s\n' '		"" | *[!0-9]*) continue ;;'
		printf '%s\n' '		esac'
		printf '%s\n' '		case "$l_test_child_pid" in'
		printf '%s\n' '		"" | *[!0-9]*) continue ;;'
		printf '%s\n' '		esac'
		printf '%s\n' '		printf "%s %s\n" "$l_test_child_pid" "$l_test_owner_pid"'
		printf '%s\n' '	done'
		printf '%s\n' '}'
		printf '%s\n' 'if [ "$#" -eq 4 ] && [ "$1" = "-eo" ] && [ "$2" = "pid=" ] &&'
		printf '%s\n' '	[ "$3" = "-o" ] && [ "$4" = "ppid=" ] &&'
		printf '%s\n' '	[ -n "${ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT:-}" ]; then'
		printf '%s\n' '	zxfer_test_print_process_relationships "$ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT"/zxfer_shunit.*/*.owner.pid'
		printf '%s\n' '	if [ -n "${ZXFER_TEST_PROCESS_IDENTITY_RELATION_ROOT:-}" ]; then'
		printf '%s\n' '		zxfer_test_print_process_relationships "$ZXFER_TEST_PROCESS_IDENTITY_RELATION_ROOT"/*.owner.pid'
		printf '%s\n' '	fi'
		printf '%s\n' '	exit 0'
		printf '%s\n' 'fi'
		printf '%s\n' 'if [ "$#" -eq 4 ] && [ "$1" = "-p" ] && [ "$3" = "-o" ]; then'
		printf '%s\n' '	case "$4" in'
		printf '%s\n' '	lstart= | stime=)'
		printf '%s\n' '		if [ "${ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_ALL:-0}" = "1" ] ||'
		printf '%s\n' '			[ -n "${ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_PID_FILE:-}" ]; then'
		printf '%s\n' '			l_test_ready_remaining=${ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_READY_LIMIT:-10}'
		printf '%s\n' '			while [ -n "${ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_READY_FILE:-}" ] &&'
		printf '%s\n' '				[ ! -e "$ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_READY_FILE" ] &&'
		printf '%s\n' '				[ "$l_test_ready_remaining" -gt 0 ]; do'
		printf '%s\n' '				sleep 1'
		printf '%s\n' '				l_test_ready_remaining=$((l_test_ready_remaining - 1))'
		printf '%s\n' '			done'
		printf '%s\n' '			l_test_force_empty=0'
		printf '%s\n' '			if [ "${ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_ALL:-0}" = "1" ]; then'
		printf '%s\n' '				l_test_force_empty=1'
		printf '%s\n' '			elif [ -r "$ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_PID_FILE" ] &&'
		printf '%s\n' '				[ "$2" = "$(cat "$ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_PID_FILE")" ]; then'
		printf '%s\n' '				l_test_force_empty=1'
		printf '%s\n' '			fi'
		printf '%s\n' '			if [ "$l_test_force_empty" -eq 1 ]; then'
		printf '%s\n' '				if [ -n "${ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_HIT_FILE:-}" ]; then'
		printf '%s\n' '					: >"$ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_HIT_FILE"'
		printf '%s\n' '				fi'
		printf '%s\n' '				exit 0'
		printf '%s\n' '			fi'
		printf '%s\n' '		fi'
		printf '%s\n' '		if [ -n "${ZXFER_TEST_SIGNAL_PARENT_RUNNER_PID_FILE:-}" ] &&'
		printf '%s\n' '			[ -r "$ZXFER_TEST_SIGNAL_PARENT_RUNNER_PID_FILE" ] &&'
		printf '%s\n' '			[ ! -e "${ZXFER_TEST_SIGNAL_PARENT_DONE_FILE:?}" ]; then'
		printf '%s\n' '			l_test_worker_pid_file=${ZXFER_TEST_SIGNAL_PARENT_WORKER_PID_FILE:?}'
		printf '%s\n' '			l_test_ready_remaining=${ZXFER_TEST_SIGNAL_PARENT_READY_LIMIT:-10}'
		printf '%s\n' '			while [ "$l_test_ready_remaining" -gt 0 ]; do'
		printf '%s\n' '				if [ -n "${ZXFER_TEST_SIGNAL_PARENT_READY_FILE:-}" ] &&'
		printf '%s\n' '					[ ! -e "$ZXFER_TEST_SIGNAL_PARENT_READY_FILE" ]; then'
		printf '%s\n' '					:'
		printf '%s\n' '				elif [ ! -r "$l_test_worker_pid_file" ]; then'
		printf '%s\n' '					:'
		printf '%s\n' '				else'
		printf '%s\n' '					break'
		printf '%s\n' '				fi'
		printf '%s\n' '				sleep 1'
		printf '%s\n' '				l_test_ready_remaining=$((l_test_ready_remaining - 1))'
		printf '%s\n' '			done'
		printf '%s\n' '			l_test_runner_pid=$(cat "$ZXFER_TEST_SIGNAL_PARENT_RUNNER_PID_FILE")'
		printf '%s\n' '			l_test_worker_pid=$(cat "$l_test_worker_pid_file" 2>/dev/null || true)'
		printf '%s\n' '			case "$l_test_runner_pid:$l_test_worker_pid" in'
		printf '%s\n' '			*[!0-9:]* | :* | *:) ;;'
		printf '%s\n' '			*) if [ "$2" = "$l_test_worker_pid" ]; then'
		printf '%s\n' '				: >"$ZXFER_TEST_SIGNAL_PARENT_DONE_FILE"'
		printf '%s\n' '				kill -TERM "$l_test_runner_pid"'
		printf '%s\n' '				exit 0'
		printf '%s\n' '			fi'
		printf '%s\n' '			;;'
		printf '%s\n' '			esac'
		printf '%s\n' '		fi'
		printf '%s\n' '		if [ -n "${ZXFER_TEST_EMPTY_PROCESS_TOKEN_PID_FILE:-}" ]; then'
		printf '%s\n' '			l_test_ready_remaining=${ZXFER_TEST_EMPTY_PROCESS_TOKEN_READY_LIMIT:-10}'
		printf '%s\n' '			while [ -n "${ZXFER_TEST_EMPTY_PROCESS_TOKEN_READY_FILE:-}" ] &&'
		printf '%s\n' '				[ ! -e "$ZXFER_TEST_EMPTY_PROCESS_TOKEN_READY_FILE" ] &&'
		printf '%s\n' '				[ "$l_test_ready_remaining" -gt 0 ]; do'
		printf '%s\n' '				sleep 1'
		printf '%s\n' '				l_test_ready_remaining=$((l_test_ready_remaining - 1))'
		printf '%s\n' '			done'
		printf '%s\n' '		fi'
		printf '%s\n' '		if [ -n "${ZXFER_TEST_EMPTY_PROCESS_TOKEN_PID_FILE:-}" ] &&'
		printf '%s\n' '			[ -r "$ZXFER_TEST_EMPTY_PROCESS_TOKEN_PID_FILE" ] &&'
		printf '%s\n' '			[ "$2" = "$(cat "$ZXFER_TEST_EMPTY_PROCESS_TOKEN_PID_FILE")" ] &&'
		printf '%s\n' '			[ ! -e "${ZXFER_TEST_EMPTY_PROCESS_TOKEN_DONE_FILE:?}" ]; then'
		printf '%s\n' '			case "$4" in'
		printf '%s\n' '			lstart=)'
		printf '%s\n' '				: >"$ZXFER_TEST_EMPTY_PROCESS_TOKEN_DONE_FILE.active"'
		printf '%s\n' '				exit 0'
		printf '%s\n' '				;;'
		printf '%s\n' '			stime=)'
		printf '%s\n' '				if [ -e "$ZXFER_TEST_EMPTY_PROCESS_TOKEN_DONE_FILE.active" ]; then'
		printf '%s\n' '					: >"$ZXFER_TEST_EMPTY_PROCESS_TOKEN_DONE_FILE"'
		printf '%s\n' '					exit 0'
		printf '%s\n' '				fi'
		printf '%s\n' '				;;'
		printf '%s\n' '			esac'
		printf '%s\n' '		fi'
		printf '%s\n' '		printf "zxfer-test-start %s\n" "$2"'
		printf '%s\n' '		exit 0'
		printf '%s\n' '		;;'
		printf '%s\n' '	esac'
		printf '%s\n' 'fi'
		printf '%s\n' 'exec "${ZXFER_TEST_REAL_PS:-/bin/ps}" "$@"'
	} >"$l_identity_ps_path" || return 1
	chmod +x "$l_identity_ps_path"
}
