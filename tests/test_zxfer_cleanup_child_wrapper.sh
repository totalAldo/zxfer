#!/bin/sh
#
# shunit2 tests for zxfer_cleanup_child_wrapper.sh.
#
# shellcheck disable=SC1090,SC2016,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_cleanup_child_wrapper"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1 \
		. "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh"
	unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
}

test_cleanup_child_wrapper_helpers_cover_current_shell_paths() {
	zxfer_test_capture_subshell '
		set +e
		ps() {
			printf "%s\n" "$$ 1"
			printf "%s\n" "701 $$"
			printf "%s\n" "702 701"
		}
		kill() {
			printf "kill:%s\n" "${3:-$2}"
			return 0
		}

		descendants=$(zxfer_cleanup_child_wrapper_list_descendants)
		printf "descendants=<%s>\n" "$(printf "%s" "$descendants" | tr "\n" " " | sed "s/[[:space:]]*$//")"
		zxfer_cleanup_child_wrapper_abort_descendants
		printf "abort=%s\n" "$?"
	'

	assertContains "Cleanup child wrapper should enumerate descendant pids in descending order." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "descendants=<702 701>"
	assertContains "Cleanup child wrapper should signal each enumerated descendant during abort handling." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "kill:702"
	assertContains "Cleanup child wrapper should signal later descendants during abort handling too." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "kill:701"
	assertContains "Cleanup child wrapper abort helper should return success when every descendant signal succeeds." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "abort=0"
}

test_cleanup_child_wrapper_on_signal_returns_143() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.on_signal"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_abort_descendants() {
			printf "%s\n" "aborted" >"'"$marker_file"'"
		}
		zxfer_cleanup_child_wrapper_on_signal
	'

	assertEquals "Cleanup child wrapper signal handling should use the documented 143 exit status." \
		143 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Cleanup child wrapper signal handling should abort descendants before exiting." \
		"aborted" "$(tr -d '[:space:]' <"$marker_file")"
}

test_cleanup_child_wrapper_main_requires_command() {
	zxfer_test_capture_subshell "sh '$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh'"

	assertEquals "Cleanup child wrapper should fail closed when no command is supplied." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_source_executes_main_when_not_source_only() {
	zxfer_test_capture_subshell '
		unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
		set -- "exit 0"
		. "'"$ZXFER_ROOT"'/src/zxfer_cleanup_child_wrapper.sh"
	'

	assertEquals "Sourcing the cleanup child wrapper without the source-only guard should execute the main entrypoint." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_source_requires_command_when_not_source_only() {
	zxfer_test_capture_subshell '
		unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
		set --
		. "'"$ZXFER_ROOT"'/src/zxfer_cleanup_child_wrapper.sh"
	'

	assertEquals "Sourcing the cleanup child wrapper without arguments should fail closed through the main entrypoint." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_main_preserves_worker_exit_status() {
	zxfer_test_capture_subshell "sh '$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh' 'exit 7'"

	assertEquals "Cleanup child wrapper should preserve the wrapped command's exit status." \
		7 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_main_preserves_worker_stdin_for_background_children() {
	stdin_capture="$TEST_TMPDIR/cleanup_child_wrapper.stdin"
	stdin_worker="$TEST_TMPDIR/cleanup_child_wrapper_stdin_worker.sh"

	cat >"$stdin_worker" <<EOF
#!/bin/sh
wc -c | tr -d '[:space:]' >"$stdin_capture"
EOF
	chmod +x "$stdin_worker"

	printf '%s' "wrapped stdin payload" |
		sh "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh" \
			"sh \"$stdin_worker\""
	status=$?

	assertEquals "Cleanup child wrapper should preserve success when the wrapped stdin reader exits cleanly." \
		0 "$status"
	assertEquals "Cleanup child wrapper should keep the caller's stdin attached to the background child." \
		"21" "$(tr -d '[:space:]' <"$stdin_capture")"
}

test_cleanup_child_wrapper_main_aborts_descendants_on_signal() {
	child_pid_file="$TEST_TMPDIR/cleanup_child_wrapper.child"
	child_script="$TEST_TMPDIR/cleanup_child_wrapper_child.sh"

	cat >"$child_script" <<EOF
#!/bin/sh
printf '%s\n' "\$\$" >"$child_pid_file"
while :; do
	sleep 1
done
EOF
	chmod +x "$child_script"

	sh "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh" \
		"sh \"$child_script\"" &
	wrapper_pid=$!

	wait_tries=0
	while [ ! -s "$child_pid_file" ] && [ "$wait_tries" -lt 50 ]; do
		sleep 0.1
		wait_tries=$((wait_tries + 1))
	done

	assertTrue "Cleanup child wrapper should publish the wrapped child pid before the test sends a termination signal." \
		"[ -s \"$child_pid_file\" ]"
	child_pid=$(tr -d '[:space:]' <"$child_pid_file")

	kill -s TERM "$wrapper_pid"
	wait "$wrapper_pid"
	status=$?

	assertEquals "Cleanup child wrapper should exit with the documented trap status when it is signaled." \
		143 "$status"

	kill -s TERM "$child_pid" >/dev/null 2>&1 || :
	wait "$child_pid" >/dev/null 2>&1 || :
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
