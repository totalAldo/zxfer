#!/bin/sh
#
# shunit2 tests for src/zxfer_runtime.sh: the per-run temp root, runtime
# artifacts, cleanup PIDs and the effective TMPDIR. The TMPDIR fragment keeps
# the exec fixture it was written for.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/runtime_fixtures.sh
. "$TESTS_DIR/helpers/runtime_fixtures.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"

# Session owns startup and shutdown composition; source the complete graph for
# lifecycle tests while runtime-only helpers remain independently testable.
zxfer_source_runtime_modules_through "zxfer_session.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_runtime"
	zxfer_test_exec_fixture_one_time_setup
}

oneTimeTearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_runtime_tmpdir_tests.sh"; then
		zxfer_test_exec_fixture_setup
		return
	fi
	zxfer_test_runtime_fixture_setup
}

tearDown() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_runtime_tmpdir_tests.sh"; then
		relax_test_tmpdir_permissions
		return
	fi
	zxfer_test_runtime_fixture_teardown
}

zxfer_runtime_wait_for_path() {
	l_runtime_wait_path=$1
	l_runtime_wait_tries=0

	while [ "$l_runtime_wait_tries" -lt 50 ]; do
		[ -e "$l_runtime_wait_path" ] && return 0
		sleep 0.1 2>/dev/null || sleep 1
		l_runtime_wait_tries=$((l_runtime_wait_tries + 1))
	done

	return 1
}

zxfer_runtime_spawn_term_trap_helper() {
	l_runtime_ready_file=$1
	l_runtime_marker_file=$2

	rm -f "$l_runtime_ready_file" "$l_runtime_marker_file" || return 1
	sh -c '
		l_ready_file=$1
		l_marker_file=$2
		trap '"'"'printf "%s\n" "term" >"$l_marker_file"; exit 143'"'"' TERM
		: >"$l_ready_file" || exit 1
		while :; do
			sleep 1
		done
	' zxfer-runtime-term-helper "$l_runtime_ready_file" "$l_runtime_marker_file" &
	g_zxfer_runtime_term_helper_pid=$!

	zxfer_runtime_wait_for_path "$l_runtime_ready_file"
}

test_get_temp_file_creates_unique_paths() {
	file_one=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")
	file_two=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")

	assertNotEquals "Each temp-file request should return a unique path." \
		"$file_one" "$file_two"
	assertTrue "The first temp file should exist." '[ -f "$file_one" ]'
	assertTrue "The second temp file should exist." '[ -f "$file_two" ]'
}

test_zxfer_create_temp_file_group_publishes_requested_paths() {
	group_output_file="$TEST_TMPDIR/runtime-temp-group.out"

	zxfer_create_temp_file_group 3 >"$group_output_file"
	group_status=$?
	group_count=$(printf '%s\n' "$g_zxfer_temp_file_group_result" | awk 'NF {count++} END {print count + 0}')
	missing_count=0
	while IFS= read -r group_path || [ -n "$group_path" ]; do
		[ -n "$group_path" ] || continue
		if [ ! -f "$group_path" ]; then
			missing_count=$((missing_count + 1))
		fi
	done <<EOF
$g_zxfer_temp_file_group_result
EOF

	assertEquals "Temp-file group allocation should succeed for a valid count." \
		0 "$group_status"
	assertEquals "Temp-file group allocation should publish one path per requested file." \
		3 "$group_count"
	assertEquals "Temp-file group allocation should print nothing." \
		"" "$(cat "$group_output_file")"
	assertEquals "Temp-file group allocation should create every published file." \
		0 "$missing_count"
}

test_zxfer_create_temp_file_group_cleans_partial_allocations_on_failure() {
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	first_path="$g_zxfer_run_tmp_root/runtime-temp-group-partial-one"
	second_path="$g_zxfer_run_tmp_root/runtime-temp-group-partial-two"
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result=$first_path
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			g_zxfer_temp_file_result=$second_path
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		esac
		return 73
	}

	set +e
	zxfer_create_temp_file_group 4 >/dev/null
	group_status=$?
	group_result=$g_zxfer_temp_file_group_result
	if [ -e "$first_path" ]; then
		first_exists=yes
	else
		first_exists=no
	fi
	if [ -e "$second_path" ]; then
		second_exists=yes
	else
		second_exists=no
	fi

	unset -f zxfer_get_temp_file
	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Temp-file group allocation should preserve the failed allocation status." \
		73 "$group_status"
	assertEquals "Temp-file group allocation should not publish a complete group on failure." \
		"" "$group_result"
	assertFalse "Temp-file group allocation should clean the first partial file on failure." \
		"[ \"$first_exists\" = yes ]"
	assertFalse "Temp-file group allocation should clean the second partial file on failure." \
		"[ \"$second_exists\" = yes ]"
}

test_zxfer_create_temp_file_group_rejects_invalid_counts() {
	g_zxfer_temp_file_group_result="stale-group"

	set +e
	zxfer_create_temp_file_group 0 >/dev/null
	group_status=$?

	assertEquals "Temp-file group allocation should reject zero as an invalid group size." \
		1 "$group_status"
	assertEquals "Temp-file group allocation should clear stale group results on invalid input." \
		"" "$g_zxfer_temp_file_group_result"
}

test_zxfer_create_temp_file_group_collects_current_shell_scratch_results() {
	output=$(
		(
			counter=0
			zxfer_get_temp_file() {
				counter=$((counter + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/group.$counter"
				: >"$g_zxfer_temp_file_result"
			}

			zxfer_create_temp_file_group 3 >/dev/null
			printf 'status=%s\n' "$?"
			printf 'count=%s\n' "$counter"
			printf '%s\n' "$g_zxfer_temp_file_group_result"
		)
	)

	assertEquals "Temp-file group allocation should collect each current-shell scratch result and allocate exactly once per requested path." \
		"status=0
count=3
$TEST_TMPDIR/group.1
$TEST_TMPDIR/group.2
$TEST_TMPDIR/group.3" "$output"
}

test_zxfer_create_temp_file_group_preserves_first_allocation_failure() {
	output=$(
		(
			g_zxfer_temp_file_group_result="stale-group"
			zxfer_get_temp_file() {
				return 71
			}

			set +e
			zxfer_create_temp_file_group 3 >/dev/null
			status=$?

			printf 'status=%s\n' "$status"
			printf 'group=<%s>\n' "$g_zxfer_temp_file_group_result"
		)
	)

	assertContains "Temp-file group allocation should preserve the first allocation failure status." \
		"$output" "status=71"
	assertContains "Temp-file group allocation should not publish a group when the first allocation fails." \
		"$output" "group=<>"
}

test_zxfer_create_temp_file_group_cleans_up_after_second_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/temp_group_cleanup_second.log"
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/group-second-first"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			return 71
			;;
		esac
		return 72
	}
	zxfer_cleanup_runtime_artifact_path_list() {
		printf '%s\n' "$1" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_create_temp_file_group 3 >/dev/null 2>&1
	status=$?
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	group_result=$g_zxfer_temp_file_group_result

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell temp-file group allocation should preserve the second allocation failure status." \
		71 "$status"
	assertEquals "Current-shell temp-file group allocation should clean up the already allocated file when the second allocation fails." \
		"$TEST_TMPDIR/group-second-first" "$cleanup_paths"
	assertEquals "Current-shell temp-file group allocation should not publish a group after the second allocation fails." \
		"" "$group_result"
}

test_zxfer_cleanup_pid_helpers_cover_current_shell_paths() {
	sleep 30 &
	first_pid=$!
	sleep 30 &
	second_pid=$!

	output=$(
		(
			zxfer_register_cleanup_pid ""
			zxfer_register_cleanup_pid "$first_pid" "unit cleanup helper"
			zxfer_register_cleanup_pid "$second_pid" "unit cleanup helper"
			zxfer_register_cleanup_pid "$second_pid" "unit cleanup helper"
			printf 'registered=<%s>\n' "$g_zxfer_cleanup_pid_records"

			zxfer_unregister_cleanup_pid "$first_pid"
			printf 'after_unregister=<%s>\n' "$g_zxfer_cleanup_pid_records"

			zxfer_register_cleanup_pid "$$" "current shell"
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s\n' "$1" >&3
				zxfer_unregister_cleanup_pid "$1"
				return 0
			}
			zxfer_kill_registered_cleanup_pids 3>&1
			printf 'after_kill=<%s>\n' "$g_zxfer_cleanup_pid_records"
		)
	)

	kill -s TERM "$first_pid" >/dev/null 2>&1 || true
	kill -s TERM "$second_pid" >/dev/null 2>&1 || true
	wait "$first_pid" 2>/dev/null || true
	wait "$second_pid" 2>/dev/null || true

	assertContains "Cleanup PID registration should keep one row per live helper PID." \
		"$output" "registered=<$first_pid	unit cleanup helper	pid
$second_pid	unit cleanup helper	pid>"
	assertContains "Cleanup PID unregistration should remove only the requested helper row." \
		"$output" "after_unregister=<$second_pid	unit cleanup helper	pid>"
	assertContains "Cleanup PID teardown should delegate teardown for the remaining helper PID." \
		"$output" "abort:$second_pid"
	assertContains "Cleanup PID teardown should clear the registered helper PID list after delegated teardown." \
		"$output" "after_kill=<>"
}

test_zxfer_register_cleanup_pid_tracks_direct_children_without_identity_captures() {
	sleep 30 &
	tracked_pid=$!

	zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
	register_status=$?
	zxfer_find_cleanup_pid_record "$tracked_pid"
	find_status=$?

	kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
	wait "$tracked_pid" 2>/dev/null || true

	assertEquals "Registering a live direct child should succeed." 0 "$register_status"
	assertEquals "Registered helpers should be findable by PID." 0 "$find_status"
	assertEquals "Registered rows should carry PID, purpose, and signal scope." \
		"$tracked_pid	unit cleanup helper	pid" "$g_zxfer_cleanup_pid_records"
	assertEquals "Record lookups should publish the stored purpose." \
		"unit cleanup helper" "$g_zxfer_cleanup_pid_record_purpose"
}

test_zxfer_register_cleanup_pid_does_not_capture_process_identity() {
	zxfer_test_capture_subshell '
		sleep 30 &
		tracked_pid=$!
		zxfer_get_process_start_token() {
			printf "unexpected-token-capture\n"
			return 1
		}
		zxfer_register_cleanup_pid "$tracked_pid" "identity unavailable helper"
		printf "status=%s\n" "$?"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		zxfer_abort_cleanup_pid "$tracked_pid" TERM
		printf "abort_status=%s\n" "$?"
		printf "after_signal=<%s>\n" "$g_zxfer_cleanup_pid_records"
		wait "$tracked_pid" 2>/dev/null || true
		zxfer_unregister_cleanup_pid "$tracked_pid"
		printf "after_wait=<%s>\n" "$g_zxfer_cleanup_pid_records"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT
	assertContains "Live direct children should register without a process snapshot." "$output" "status=0"
	assertNotContains "Registration and signalling must not capture a start token." "$output" "unexpected-token-capture"
	assertContains "The direct-child record should retain its purpose." "$output" "identity unavailable helper	pid>"
	assertContains "The registered direct child should be signalled." "$output" "abort_status=0"
	assertNotContains "Signalling must retain ownership until wait." "$output" "after_signal=<>"
	assertContains "Explicit wait/unregister should release ownership." "$output" "after_wait=<>"
}

test_cleanup_registration_retains_a_live_group_or_its_starting_leader() {
	output=$(
		kill() {
			[ "$*" = '-0 -23456' ] || [ "$*" = '-s 0 23457' ]
		}
		zxfer_register_cleanup_pid 23456 'departed leader' pgid
		zxfer_register_cleanup_pid 23457 'starting leader' pgid
		printf '%s\n' "$g_zxfer_cleanup_pid_records"
	)

	assertContains "A live group must remain tracked when its leader has already exited." \
		"$output" '23456	departed leader	pgid'
	assertContains "A new leader must remain tracked before it establishes its group." \
		"$output" '23457	starting leader	pgid'
}

test_zxfer_register_cleanup_pid_rejects_invalid_self_and_dead_pids() {
	zxfer_register_cleanup_pid "not-a-pid" "unit cleanup helper"
	invalid_status=$?
	zxfer_register_cleanup_pid "$$" "current shell"
	self_status=$?
	sh -c 'exit 0' &
	dead_pid=$!
	wait "$dead_pid" 2>/dev/null
	zxfer_register_cleanup_pid "$dead_pid" "already exited helper"
	dead_status=$?

	assertEquals "Non-numeric PIDs should be ignored without error." 0 "$invalid_status"
	assertEquals "The current shell PID should be ignored without error." 0 "$self_status"
	assertEquals "Already-exited helpers should be ignored without error." 0 "$dead_status"
	assertEquals "No registry rows should exist after rejected registrations." \
		"" "$g_zxfer_cleanup_pid_records"
}

test_zxfer_register_cleanup_pid_rejects_a_purpose_that_would_split_a_row() {
	zxfer_test_capture_subshell '
		sleep 30 &
		tracked_pid=$!
		zxfer_register_cleanup_pid "$tracked_pid" "unit${ZXFER_TAB}helper"
		printf "tab_status=%s\n" "$?"
		zxfer_register_cleanup_pid "$tracked_pid" "unit${ZXFER_LF}helper"
		printf "newline_status=%s\n" "$?"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
		wait "$tracked_pid" 2>/dev/null || true
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Registration should fail closed on a purpose with a tab." \
		"$output" "tab_status=1"
	assertContains "Registration should fail closed on a purpose with a line break." \
		"$output" "newline_status=1"
	assertContains "Failed registrations should not leave registry rows behind." \
		"$output" "records=<>"
}

test_zxfer_abort_cleanup_pid_signals_live_tracked_children_until_waited() {
	ready_file="$TEST_TMPDIR/abort_cleanup.ready"
	marker_file="$TEST_TMPDIR/abort_cleanup.marker"
	zxfer_runtime_spawn_term_trap_helper "$ready_file" "$marker_file" ||
		fail "Unable to start TERM-aware cleanup helper."
	tracked_pid=$g_zxfer_runtime_term_helper_pid

	zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
	zxfer_abort_cleanup_pid "$tracked_pid" TERM
	abort_status=$?
	records_after_signal=$g_zxfer_cleanup_pid_records
	wait "$tracked_pid" 2>/dev/null
	reaped_status=$?
	zxfer_unregister_cleanup_pid "$tracked_pid"

	assertEquals "Aborting a live tracked helper should succeed." 0 "$abort_status"
	assertEquals "Aborting should leave no failure message." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
	assertNotEquals "Signalling should retain the registry row until wait." "" "$records_after_signal"
	assertEquals "Wait/unregister should remove the registry row." "" "$g_zxfer_cleanup_pid_records"
	assertEquals "The aborted helper should have handled the TERM signal." \
		143 "$reaped_status"
	assertEquals "The aborted helper should have recorded its TERM trap." \
		"term" "$(tr -d '[:space:]' <"$marker_file")"
}

test_zxfer_abort_cleanup_pid_handles_untracked_and_already_exited_helpers() {
	zxfer_abort_cleanup_pid 99999 TERM
	untracked_status=$?

	sh -c 'exit 0' &
	dead_pid=$!
	g_zxfer_cleanup_pid_records="$dead_pid	already exited helper"
	wait "$dead_pid" 2>/dev/null
	zxfer_abort_cleanup_pid "$dead_pid" TERM
	dead_status=$?
	dead_records_after_signal=$g_zxfer_cleanup_pid_records
	zxfer_unregister_cleanup_pid "$dead_pid"

	assertEquals "Aborting an untracked PID should be a no-op success." 0 "$untracked_status"
	assertEquals "Aborting a tracked helper that already exited should succeed." 0 "$dead_status"
	assertNotEquals "Already-exited helpers should remain owned until explicit unregister." \
		"" "$dead_records_after_signal"
	assertEquals "Explicit unregister should release an already-exited helper." \
		"" "$g_zxfer_cleanup_pid_records"
	assertEquals "Already-exited helpers should leave no failure message." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
}

test_zxfer_abort_cleanup_pid_fails_closed_when_signalling_a_live_helper_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		tracked_pid=$!
		zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
		kill() {
			case "$2" in
			0)
				command kill -0 "$3" 2>/dev/null
				return $?
				;;
			esac
			return 1
		}
		zxfer_abort_cleanup_pid "$tracked_pid" TERM
		printf "status=%s\n" "$?"
		printf "message=%s\n" "$g_zxfer_cleanup_pid_abort_failure_message"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		unset -f kill
		kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
		wait "$tracked_pid" 2>/dev/null || true
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Aborting should fail closed when the signal cannot be delivered to a live helper." \
		"$output" "status=1"
	assertContains "Failed aborts should explain which helper could not be signalled." \
		"$output" "message=Failed to signal cleanup helper [unit cleanup helper] (PID "
	assertNotContains "Failed aborts should preserve the registry row for a later retry." \
		"$output" "records=<>"
}

test_zxfer_abort_helpers_treat_exit_during_failed_signal_as_success_without_duplicate_tracking() {
	zxfer_test_capture_subshell '
		g_zxfer_cleanup_pid_records="701	already tracked helper"
		kill() {
			case "$2" in
			0) return 0 ;;
			*) return 1 ;;
			esac
		}
		zxfer_abort_direct_child_pid 701 TERM "already tracked helper"
		printf "tracked_status=%s\n" "$?"
		printf "tracked_records=<%s>\n" "$g_zxfer_cleanup_pid_records"

		g_zxfer_cleanup_pid_records=""
		l_test_zero_calls=0
		kill() {
			case "$2" in
			0)
				l_test_zero_calls=$((l_test_zero_calls + 1))
				[ "$l_test_zero_calls" -eq 1 ]
				;;
			*) return 1 ;;
			esac
		}
		zxfer_abort_direct_child_pid 702 TERM "exiting direct helper"
		printf "direct_race_status=%s\n" "$?"
		printf "direct_race_records=<%s>\n" "$g_zxfer_cleanup_pid_records"

		g_zxfer_cleanup_pid_records="703	exiting tracked helper"
		l_test_zero_calls=0
		zxfer_abort_cleanup_pid 703 TERM
		printf "tracked_race_status=%s\n" "$?"
		printf "tracked_race_records=<%s>\n" "$g_zxfer_cleanup_pid_records"
	'

	assertContains "A failed signal to an already tracked live direct child should remain an error." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "tracked_status=1"
	assertContains "An already tracked direct child should not acquire a duplicate cleanup record." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "tracked_records=<701	already tracked helper>"
	assertContains "A direct child that exits after a failed signal should be treated as gone." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "direct_race_status=0"
	assertContains "An exited untracked direct child should not be added to cleanup tracking." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "direct_race_records=<>"
	assertContains "A registered helper that exits after a failed signal should be treated as gone." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "tracked_race_status=0"
	assertContains "Exit-race handling should retain ownership until the caller explicitly waits and unregisters." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "tracked_race_records=<703	exiting tracked helper>"
}

test_zxfer_cleanup_pid_abort_grace_wait_uses_bounded_default_for_invalid_internal_state() {
	zxfer_test_capture_subshell '
		g_zxfer_cleanup_pid_abort_grace_seconds=invalid
		sleep() {
			printf "sleep:%s\n" "$1"
		}

		zxfer_cleanup_pid_abort_grace_wait
	'

	assertEquals "Invalid internal grace state should fall back to the bounded two-second delay." \
		"sleep:2" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "The bounded grace helper should succeed after the fallback delay." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_abort_direct_child_pid_signals_unreaped_direct_children() {
	ready_file="$TEST_TMPDIR/abort_direct.ready"
	marker_file="$TEST_TMPDIR/abort_direct.marker"
	zxfer_runtime_spawn_term_trap_helper "$ready_file" "$marker_file" ||
		fail "Unable to start TERM-aware direct child helper."
	child_pid=$g_zxfer_runtime_term_helper_pid

	zxfer_abort_direct_child_pid \
		"$child_pid" TERM "unit direct helper"
	abort_status=$?
	wait "$child_pid" 2>/dev/null
	reaped_status=$?

	assertEquals "Signalling a live direct child should succeed." 0 "$abort_status"
	assertEquals "Signalling should leave no failure message." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
	assertEquals "The signalled child should have handled the TERM signal." \
		143 "$reaped_status"
	assertEquals "The signalled child should have recorded its TERM trap." \
		"term" "$(tr -d '[:space:]' <"$marker_file")"
}

test_zxfer_abort_direct_child_pid_tracks_live_child_when_immediate_signal_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		child_pid=$!
		kill() {
			case "$2" in
			0) return 0 ;;
			*) return 1 ;;
			esac
		}
		zxfer_abort_direct_child_pid \
			"$child_pid" TERM "unregistered direct helper"
		printf "status=%s\n" "$?"
		printf "tracked=%s\n" "${g_zxfer_cleanup_pid_records%%"$ZXFER_TAB"*}"
		printf "records=%s\n" "$g_zxfer_cleanup_pid_records"
		unset -f kill
		kill -s TERM "$child_pid" >/dev/null 2>&1 || true
		wait "$child_pid" 2>/dev/null || true
	'
	tracked_pid=$(printf '%s\n' "$ZXFER_TEST_CAPTURE_OUTPUT" | sed -n 's/^tracked=//p')

	assertContains "A failed immediate direct-child signal should remain a cleanup failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
	assertNotNull "A live direct child whose immediate signal failed must remain registered for trap retry." \
		"$tracked_pid"
	assertContains "The retained cleanup row should preserve the direct-child purpose for diagnostics." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unregistered direct helper"
}

test_zxfer_abort_direct_child_pid_rejects_invalid_self_and_dead_pids() {
	zxfer_abort_direct_child_pid "" TERM "unit direct helper"
	empty_status=$?
	zxfer_abort_direct_child_pid "not-a-pid" TERM "unit direct helper"
	invalid_status=$?
	zxfer_abort_direct_child_pid "$$" TERM "unit direct helper"
	self_status=$?
	sh -c 'exit 0' &
	dead_pid=$!
	wait "$dead_pid" 2>/dev/null
	zxfer_abort_direct_child_pid "$dead_pid" TERM "unit direct helper"
	dead_status=$?

	assertEquals "Empty PIDs should be a no-op success." 0 "$empty_status"
	assertEquals "Non-numeric PIDs should be a no-op success." 0 "$invalid_status"
	assertEquals "The current shell PID must be refused." 1 "$self_status"
	assertEquals "Already-exited children should be a no-op success." 0 "$dead_status"
}

test_zxfer_abort_direct_child_pid_still_signals_with_a_purpose_that_would_split_a_row() {
	zxfer_test_capture_subshell '
		sleep 30 &
		child_pid=$!
		zxfer_abort_direct_child_pid "$child_pid" TERM "unit${ZXFER_LF}direct helper"
		printf "status=%s\n" "$?"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		wait "$child_pid" 2>/dev/null
		[ "$?" -gt 128 ] && printf "%s\n" "child=signalled"
	'

	assertContains "A multi-line purpose must not stop the abort from signalling." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=0"
	assertContains "The child should have died from the signal." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "child=signalled"
	assertContains "A signalled child leaves no cleanup row." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "records=<>"
}

test_zxfer_kill_registered_cleanup_pids_reaps_successes_and_preserves_failures() {
	zxfer_test_capture_subshell '
		set +e
		g_zxfer_cleanup_pid_abort_grace_seconds=0
		g_zxfer_cleanup_pid_records="401	first helper
402	second helper"
		zxfer_abort_cleanup_pid() {
			if [ "$1" = "401" ]; then
				g_zxfer_cleanup_pid_abort_failure_message="first cleanup abort failed"
				return 1
			fi
			return 0
		}
		kill() {
			[ "$3" = "401" ]
		}

		zxfer_kill_registered_cleanup_pids
		printf "status=%s\n" "$?"
		printf "message=%s\n" "$g_zxfer_cleanup_pid_abort_failure_message"
		printf "remaining=<%s>\n" "$g_zxfer_cleanup_pid_records"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Cleanup-helper shutdown should preserve the first abort failure status." \
		"$output" "status=1"
	assertContains "Cleanup-helper shutdown should preserve the first abort failure message." \
		"$output" "message=first cleanup abort failed"
	assertContains "Cleanup-helper shutdown should reap successful helpers and retain only a live failed helper." \
		"$output" "remaining=<401	first helper>"
}

test_zxfer_kill_registered_cleanup_pids_escalates_term_resistant_children() {
	ready_file="$TEST_TMPDIR/cleanup_term_resistant.ready"
	sh -c '
		trap "" TERM
		: >"$1"
		while :; do sleep 1; done
	' zxfer-runtime-term-resistant "$ready_file" &
	child_pid=$!
	zxfer_runtime_wait_for_path "$ready_file" ||
		fail "TERM-resistant cleanup helper did not start."
	zxfer_register_cleanup_pid "$child_pid" "TERM-resistant cleanup helper"
	g_zxfer_cleanup_pid_abort_grace_seconds=0

	zxfer_kill_registered_cleanup_pids
	cleanup_status=$?

	assertEquals "Aggregate cleanup should KILL and reap a helper that ignores TERM." \
		0 "$cleanup_status"
	assertFalse "Aggregate cleanup must not leave the TERM-resistant child alive." \
		"kill -s 0 '$child_pid' 2>/dev/null"
	assertEquals "Aggregate cleanup should unregister the reaped child." \
		"" "$g_zxfer_cleanup_pid_records"
}

test_cleanup_kills_group_descendants_after_the_leader_exits_on_term() {
	zxfer_reset_background_shell_spawn_mode
	zxfer_init_background_shell_spawn_mode
	if [ "$g_zxfer_background_shell_spawn_mode" = wrapper ]; then
		startSkipping
		return
	fi
	pid_file="$TEST_TMPDIR/group-resistant-child.pid"
	rm -f "$pid_file"
	zxfer_spawn_background_shell 'sh -c '\''trap "" TERM; echo $$ >"$1"; exec sleep 30'\'' zxfer-test "$1" & wait' "" "" "$pid_file"
	group_pid=$g_last_background_pid
	zxfer_register_cleanup_pid "$group_pid" "group cleanup fixture" "$g_zxfer_background_shell_scope"
	zxfer_runtime_wait_for_path "$pid_file" || fail "Group descendant did not start."
	child_pid=$(cat "$pid_file")
	g_zxfer_cleanup_pid_abort_grace_seconds=1
	zxfer_kill_registered_cleanup_pids
	cleanup_status=$?
	attempts=0
	while kill -s 0 "$child_pid" 2>/dev/null && [ "$attempts" -lt 50 ]; do
		sleep 0.1
		attempts=$((attempts + 1))
	done
	assertEquals "Group cleanup succeeds after the leader exits." 0 "$cleanup_status"
	assertFalse "The TERM-resistant descendant is killed after its leader exits." "kill -s 0 '$child_pid' 2>/dev/null"
	# Keep a failed assertion from leaving this fixture alive.
	zxfer_signal_background_shell "$group_pid" pgid KILL || :
	assertEquals "Successful group cleanup clears its registry." "" "$g_zxfer_cleanup_pid_records"
}

test_runtime_artifact_allocators_use_the_per_run_temp_root_for_files_and_dirs() {
	zxfer_create_runtime_artifact_file "runtime-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_private_temp_dir "runtime-dir" >/dev/null
	dir_status=$?
	dir_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "Runtime artifact file allocation should succeed under the per-run temp root." \
		0 "$file_status"
	assertEquals "Runtime artifact directory allocation should succeed under the per-run temp root." \
		0 "$dir_status"
	assertNotEquals "Runtime artifact allocation should publish the per-run temp root." \
		"" "$g_zxfer_run_tmp_root"
	assertEquals "Runtime artifact allocation should retain the exact owner identity used by cleanup." \
		"$g_zxfer_run_tmp_root" "$g_zxfer_owned_run_tmp_root"
	assertContains "The per-run temp root should live under the validated temp root." \
		"$g_zxfer_run_tmp_root" "$TEST_TMPDIR/"
	assertEquals "The per-run temp root should be private to the current user (0700)." \
		"700" "$(zxfer_get_path_mode_octal "$g_zxfer_run_tmp_root")"
	assertContains "Runtime artifact files should be allocated under the per-run temp root." \
		"$file_path" "$g_zxfer_run_tmp_root/"
	assertContains "Runtime artifact directories should be allocated under the per-run temp root." \
		"$dir_path" "$g_zxfer_run_tmp_root/"
	assertTrue "Runtime artifact file allocation should create the requested file." \
		"[ -f \"$file_path\" ]"
	assertEquals "Runtime artifact files should be created owner-only (0600)." \
		"600" "$(zxfer_get_path_mode_octal "$file_path")"
	assertTrue "Runtime artifact directory allocation should create the requested directory." \
		"[ -d \"$dir_path\" ]"
	assertEquals "Runtime artifact directories should be created owner-only (0700)." \
		"700" "$(zxfer_get_path_mode_octal "$dir_path")"
	assertEquals "Per-run-root allocations should not register per-file cleanup bookkeeping." \
		"" "${g_zxfer_runtime_artifact_cleanup_paths:-}"
}

test_zxfer_discard_runtime_cleanup_state_drops_inherited_handles_without_acting_on_them() {
	external_root="$TEST_TMPDIR/operator-owned-data"
	external_stage="$TEST_TMPDIR/.zxfer-operator-stage"
	mkdir -p "$external_root"
	printf '%s\n' sentinel >"$external_root/sentinel"
	printf '%s\n' stage-sentinel >"$external_stage"

	# Simulate an exported caller environment, including forged copies of the
	# internal provenance fields. Startup must discard all of it before any
	# cleanup-capable reset runs.
	g_zxfer_run_tmp_root=$external_root
	g_zxfer_owned_run_tmp_root=$external_root
	g_zxfer_owned_run_tmp_root_parent=$TEST_TMPDIR
	g_zxfer_owned_run_tmp_root_identity="device-inode:1:2"
	g_zxfer_runtime_artifact_cleanup_paths="-
$external_stage"
	g_zxfer_cleanup_pid_records="424242	operator helper"
	g_zxfer_effective_tmpdir=$external_root
	g_zxfer_effective_tmpdir_requested=$external_root
	g_zxfer_temp_file_result="$external_root/inherited-temp-result"

	zxfer_discard_runtime_cleanup_state

	assertTrue "Runtime initialization must not recursively remove an inherited run-root path." \
		"[ -f '$external_root/sentinel' ]"
	assertTrue "Runtime initialization must not remove an inherited adjacent-artifact registration." \
		"[ -f '$external_stage' ]"
	assertEquals "Runtime initialization should discard the inherited run-root handle." \
		"" "$g_zxfer_run_tmp_root"
	assertEquals "Runtime initialization should discard inherited run-root object identity." \
		"" "$g_zxfer_owned_run_tmp_root_identity"
	assertEquals "Runtime initialization should discard inherited artifact registrations." \
		"" "$g_zxfer_runtime_artifact_cleanup_paths"
	assertEquals "Runtime initialization should discard inherited cleanup PIDs without signalling them." \
		"" "$g_zxfer_cleanup_pid_records"
	assertEquals "Runtime initialization should discard an inherited effective-temp-directory memo." \
		"" "$g_zxfer_effective_tmpdir"
	assertEquals "Runtime initialization should discard inherited temp-file result state." \
		"" "$g_zxfer_temp_file_result"
}

test_zxfer_remove_run_tmp_root_rejects_a_forged_or_unsafe_owner_shape() {
	external_root="$TEST_TMPDIR/operator-root"
	mkdir -p "$external_root"
	printf '%s\n' sentinel >"$external_root/sentinel"
	g_zxfer_run_tmp_root=$external_root
	g_zxfer_owned_run_tmp_root=$external_root
	g_zxfer_owned_run_tmp_root_parent=$TEST_TMPDIR

	zxfer_remove_run_tmp_root
	remove_status=$?

	assertEquals "Whole-root cleanup must reject paths outside the private mktemp naming contract." \
		1 "$remove_status"
	assertTrue "Rejected whole-root cleanup must leave external sentinel data untouched." \
		"[ -f '$external_root/sentinel' ]"
	# Do not carry the deliberately inconsistent fixture into the next setUp.
	g_zxfer_run_tmp_root=""
	g_zxfer_owned_run_tmp_root=""
	g_zxfer_owned_run_tmp_root_parent=""
}

test_zxfer_run_tmp_root_provenance_handles_a_root_temp_parent_without_double_slashes() {
	template_record="$TEST_TMPDIR/root-parent-mktemp-template"
	output=$(
		(
			g_zxfer_run_tmp_root=""
			g_zxfer_owned_run_tmp_root=""
			g_zxfer_owned_run_tmp_root_parent=""
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			zxfer_try_get_effective_tmpdir() {
				g_zxfer_effective_tmpdir=/
				return 0
			}
			mktemp() {
				printf '%s\n' "$2" >"$template_record"
				printf '/zxfer.%s.ABC123\n' "$$"
			}
			zxfer_get_private_directory_security_record() {
				printf 'device-inode:1:2\t%s\t700\n' 501
			}

			zxfer_ensure_run_tmp_root
			printf 'status=%s\n' "$?"
			printf 'template=%s\n' "$(cat "$template_record")"
			printf 'root=%s\n' "$g_zxfer_run_tmp_root"
			zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_run_tmp_root"
			printf 'shape=%s\n' "$?"
		)
	)

	assertContains "A root temp parent should produce a single-slash mktemp template." \
		"$output" "template=/zxfer.$$.XXXXXX"
	assertContains "A normalized root-parent mktemp result should retain valid owner provenance." \
		"$output" "status=0"
	assertContains "Root-parent provenance validation should accept the direct-child result." \
		"$output" "shape=0"
	assertNotContains "Root-parent template construction must not introduce a double slash." \
		"$output" "template=//"
}

test_zxfer_ensure_run_tmp_root_validates_a_fresh_parent_once() {
	validation_log="$TEST_TMPDIR/fresh-parent-validation.log"
	: >"$validation_log"
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1" >>"$validation_log"
			printf '%s\n' "$1"
		}
		g_zxfer_effective_tmpdir=""
		g_zxfer_effective_tmpdir_requested=""
		zxfer_ensure_run_tmp_root || exit 1
		zxfer_remove_run_tmp_root
	)
	assertEquals "A fresh parent is validated once before the first allocation." "$TEST_TMPDIR" "$(cat "$validation_log")"
}

test_zxfer_ensure_run_tmp_root_revalidates_memoized_tmpdir_before_mktemp() {
	effective_parent="$TEST_TMPDIR/effective-parent.$$"
	saved_parent="$TEST_TMPDIR/saved-effective-parent.$$"
	replacement_parent="$TEST_TMPDIR/replacement-parent.$$"
	mkdir -m 700 "$effective_parent" "$replacement_parent"
	TMPDIR=$effective_parent
	zxfer_try_get_effective_tmpdir >/dev/null ||
		fail "Unable to memoize the safe TMPDIR fixture."
	mv "$effective_parent" "$saved_parent"
	ln -s "$replacement_parent" "$effective_parent"

	zxfer_ensure_run_tmp_root >/dev/null 2>&1
	ensure_status=$?

	set -- "$replacement_parent"/zxfer.*
	assertEquals "Run-root allocation must reject a memoized TMPDIR pathname that was replaced before mktemp." \
		1 "$ensure_status"
	assertFalse "Rejected TMPDIR replacement must not allocate a run root in the symlink target." \
		"[ -e '$1' ]"
	rm -f "$effective_parent"
	mv "$saved_parent" "$effective_parent"
	TMPDIR=$TEST_TMPDIR
}

test_zxfer_find_default_tmpdir_skips_missing_and_unsafe_candidates() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	unsafe_tmp="$physical_tmpdir/default_candidate_unsafe"
	safe_tmp="$physical_tmpdir/default_candidate_safe"
	mkdir -p "$unsafe_tmp" "$safe_tmp"
	chmod 0777 "$unsafe_tmp"
	chmod 0700 "$safe_tmp"

	output=$(
		(
			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$physical_tmpdir/default_candidate_missing" "$unsafe_tmp" "$safe_tmp"
			}
			zxfer_find_default_tmpdir
			printf 'status=%s result=%s\n' "$?" "$g_zxfer_default_tmpdir_result"
			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$unsafe_tmp"
			}
			zxfer_find_default_tmpdir
			printf 'none=%s result=<%s>\n' "$?" "$g_zxfer_default_tmpdir_result"
		)
	)
	chmod 0700 "$unsafe_tmp"

	assertContains "The first safe default candidate should win over missing and unsafe ones." \
		"$output" "status=0 result=$safe_tmp"
	assertContains "No safe default candidate should fail with an empty result." \
		"$output" "none=1 result=<>"
}

test_zxfer_run_tmp_root_safe_shape_rejects_untrusted_parent_relationships() {
	zxfer_test_capture_subshell '
		g_zxfer_owned_run_tmp_root="/tmp/zxfer.$$.owned"
		g_zxfer_owned_run_tmp_root_parent="relative-parent"
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_owned_run_tmp_root"
		printf "relative_parent=%s\n" "$?"

		g_zxfer_owned_run_tmp_root="zxfer.$$.owned"
		g_zxfer_owned_run_tmp_root_parent="/"
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_owned_run_tmp_root"
		printf "relative_root=%s\n" "$?"

		g_zxfer_owned_run_tmp_root="/other/zxfer.$$.owned"
		g_zxfer_owned_run_tmp_root_parent="/tmp"
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_owned_run_tmp_root"
		printf "wrong_parent=%s\n" "$?"
	'

	assertContains "Whole-root cleanup should reject a relative recorded parent." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "relative_parent=1"
	assertContains "A root-parent allocation record should still require an absolute child path." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "relative_root=1"
	assertContains "Whole-root cleanup should reject a child outside its exact recorded parent." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "wrong_parent=1"
}

test_zxfer_ensure_run_tmp_root_removes_an_allocation_whose_identity_cannot_be_recorded() {
	candidate_root="$TEST_TMPDIR/zxfer.$$.identity-failure"
	zxfer_test_capture_subshell '
		g_zxfer_run_tmp_root=""
		g_zxfer_owned_run_tmp_root=""
		g_zxfer_owned_run_tmp_root_parent=""
		g_zxfer_owned_run_tmp_root_identity=""
		zxfer_try_get_effective_tmpdir() {
			g_zxfer_effective_tmpdir="'"$TEST_TMPDIR"'"
		}
		zxfer_validate_temp_root_candidate() {
			printf "%s\n" "'"$TEST_TMPDIR"'"
		}
		mktemp() {
			mkdir "'"$candidate_root"'" || return 1
			printf "%s\n" "'"$candidate_root"'"
		}
		zxfer_get_private_directory_security_record() {
			return 1
		}

		zxfer_ensure_run_tmp_root
		printf "status=%s\n" "$?"
		if [ -e "'"$candidate_root"'" ]; then
			printf "candidate_exists=yes\n"
		else
			printf "candidate_exists=no\n"
		fi
	'

	assertContains "Run-root allocation should fail closed when its object identity cannot be recorded." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
	assertContains "An unidentified run-root allocation should be removed before failure returns." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "candidate_exists=no"
}

test_runtime_artifact_file_helpers_restore_run_umask_and_work_under_noclobber() {
	(
		# Use 027 so the result never depends on the developer's umask.
		umask 027
		zxfer_ensure_run_tmp_root || exit 1
		printf 'recorded=%s\n' "$g_zxfer_run_umask"
		zxfer_create_runtime_artifact_file "mode-probe" >/dev/null || exit 1
		printf 'mode=%s umask=%s\n' \
			"$(zxfer_get_path_mode_octal "$g_zxfer_runtime_artifact_path_result")" "$(umask)"
		case $- in *C*) echo "noclobber=on" ;; *) echo "noclobber=off" ;; esac

		# A caller that already set noclobber keeps it. A signal trap can also
		# run with the allocator's set -C, so the overwriting helpers must work.
		set -C
		zxfer_create_runtime_artifact_file "mode-probe" >/dev/null || exit 1
		case $- in *C*) echo "noclobber=on" ;; *) echo "noclobber=off" ;; esac
		zxfer_write_runtime_artifact_file "$g_zxfer_runtime_artifact_path_result" "payload" &&
			printf 'write=%s\n' "$(cat "$g_zxfer_runtime_artifact_path_result")"
	) >"$TEST_TMPDIR/runtime_alloc_modes.out" 2>&1

	assertEquals "The file allocator should create 0600 files, restore the recorded run umask and the caller's noclobber state, and the overwriting writer should work under noclobber." \
		"recorded=0027
mode=600 umask=0027
noclobber=off
noclobber=on
write=payload" "$(cat "$TEST_TMPDIR/runtime_alloc_modes.out")"
}

test_runtime_artifact_allocators_skip_pre_seeded_counter_names_in_current_shell() {
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	g_zxfer_run_tmp_counter=0
	: >"$g_zxfer_run_tmp_root/skip-file.1"
	zxfer_create_runtime_artifact_file "skip-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	mkdir -m 700 "$g_zxfer_run_tmp_root/skip-dir.3"
	zxfer_create_private_temp_dir "skip-dir" >/dev/null
	dir_status=$?
	dir_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "File allocation should succeed after skipping a taken counter name." \
		0 "$file_status"
	assertEquals "File allocation should advance past the taken counter name." \
		"$g_zxfer_run_tmp_root/skip-file.2" "$file_path"
	assertEquals "Directory allocation should succeed after skipping a taken counter name." \
		0 "$dir_status"
	assertEquals "Directory allocation should advance past the taken counter name." \
		"$g_zxfer_run_tmp_root/skip-dir.4" "$dir_path"
}

test_runtime_artifact_allocators_fail_closed_when_the_target_path_rejects_writes() {
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	# A regular-file path component rejects child creation even for root in the
	# FreeBSD shunit2 guest; chmod-only fixtures are bypassable there.
	: >"$g_zxfer_run_tmp_root/file-blocker"
	: >"$g_zxfer_run_tmp_root/dir-blocker"
	zxfer_create_runtime_artifact_file "file-blocker/unwritable-file" >/dev/null 2>&1
	file_status=$?
	zxfer_create_private_temp_dir "dir-blocker/unwritable-dir" >/dev/null 2>&1
	dir_status=$?

	assertEquals "File allocation should fail closed when the target path rejects writes." \
		1 "$file_status"
	assertEquals "Directory allocation should fail closed when the target path rejects writes." \
		1 "$dir_status"
}

test_runtime_artifact_allocators_skip_taken_names_after_subshell_allocations() {
	# Allocate through command substitutions so the counter bumps never reach
	# this shell; the allocator must still hand out unique, existing paths.
	first_path=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")
	printf 'first payload\n' >"$first_path"
	second_path=$(zxfer_get_temp_file && printf '%s' "$g_zxfer_temp_file_result")

	assertNotEquals "Subshell allocations should never reuse a taken temp path." \
		"$first_path" "$second_path"
	assertEquals "Subshell allocations should never truncate earlier allocations." \
		"first payload" "$(cat "$first_path")"
	assertTrue "Subshell allocations should create the later temp file." \
		"[ -f \"$second_path\" ]"
}

test_trap_cleanup_steps_keep_what_rm_could_not_remove() {
	artifact_path="$TEST_TMPDIR/zxfer.runtime-reset-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			zxfer_ensure_run_tmp_root || exit 90
			rm() {
				return 1
			}
			zxfer_cleanup_registered_runtime_artifacts
			printf 'registered_status=%s\n' "$?"
			zxfer_remove_run_tmp_root
			printf 'root_status=%s\n' "$?"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
			printf 'root_retained=<%s>\n' "$([ -n "$g_zxfer_run_tmp_root" ] && printf yes || printf no)"
			unset -f rm
			zxfer_cleanup_registered_runtime_artifacts
			zxfer_remove_run_tmp_root
			printf 'retry_registered=<%s> retry_root=<%s>\n' \
				"$g_zxfer_runtime_artifact_cleanup_paths" "$g_zxfer_run_tmp_root"
		)
	)

	assertContains "Registered-artifact cleanup should report an artifact rm could not remove." \
		"$output" "registered_status=1"
	assertContains "Run-root removal should report a root rm could not remove." \
		"$output" "root_status=1"
	assertContains "An artifact rm could not remove should stay registered for a later sweep." \
		"$output" "registered=<-
$artifact_path>"
	assertContains "A run root rm could not remove should stay tracked for a later sweep." \
		"$output" "root_retained=<yes>"
	assertContains "A later sweep should remove what the failed one kept." \
		"$output" "retry_registered=<> retry_root=<>"
	assertFalse "The retried sweep should remove the adjacent artifact." \
		"[ -e \"$artifact_path\" ]"
}

test_zxfer_cleanup_runtime_artifact_path_preserves_registration_when_delete_fails() {
	artifact_path="$TEST_TMPDIR/zxfer.runtime-cleanup-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			rm() {
				return 1
			}
			zxfer_cleanup_runtime_artifact_path "$artifact_path"
			status=$?
			printf 'status=%s\n' "$status"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
		)
	)

	assertContains "Runtime artifact cleanup should preserve failure when an artifact cannot be deleted." \
		"$output" "status=1"
	assertContains "Runtime artifact cleanup should keep undeleted artifacts registered for later cleanup." \
		"$output" "registered=<-
$artifact_path>"
	assertTrue "Runtime artifact cleanup failures should leave the undeleted artifact in place." \
		"[ -e \"$artifact_path\" ]"
}

test_zxfer_cleanup_runtime_artifact_path_rejects_unowned_outside_paths() {
	outside_path="$TEST_TMPDIR/operator-data"
	mkdir -p "$outside_path"
	: >"$outside_path/must-survive"

	zxfer_cleanup_runtime_artifact_path "$outside_path" >/dev/null 2>&1
	cleanup_status=$?

	assertEquals "Generic runtime cleanup should reject paths outside the private root and exact registry." \
		1 "$cleanup_status"
	assertTrue "Rejected outside paths and their contents must remain untouched." \
		"[ -f '$outside_path/must-survive' ]"
}

test_zxfer_cleanup_runtime_artifact_path_rejects_replaced_run_root_parent() {
	zxfer_create_private_temp_dir "owned-child" >/dev/null
	owned_child=$g_zxfer_runtime_artifact_path_result
	run_root=$g_zxfer_run_tmp_root
	saved_root="$TEST_TMPDIR/saved-run-root.$$"
	external_root="$TEST_TMPDIR/external-run-root.$$"
	mkdir -p "$external_root/owned-child"
	: >"$external_root/owned-child/must-survive"
	mv "$run_root" "$saved_root"
	ln -s "$external_root" "$run_root"

	zxfer_cleanup_runtime_artifact_path "$owned_child" >/dev/null 2>&1
	cleanup_status=$?

	rm -f "$run_root"
	mv "$saved_root" "$run_root"
	zxfer_remove_run_tmp_root >/dev/null
	assertEquals "Child cleanup must reject a run-root pathname replaced by a symlink." \
		1 "$cleanup_status"
	assertTrue "Rejected run-root substitution must not traverse into and delete an external child." \
		"[ -f '$external_root/owned-child/must-survive' ]"
}

test_zxfer_remove_run_tmp_root_rejects_same_mode_owner_directory_replacement() {
	zxfer_create_private_temp_dir "original-child" >/dev/null
	run_root=$g_zxfer_run_tmp_root
	saved_root="$TEST_TMPDIR/saved-owned-run-root.$$"
	mv "$run_root" "$saved_root"
	mkdir -m 700 "$run_root"
	: >"$run_root/must-survive"

	zxfer_remove_run_tmp_root >/dev/null 2>&1
	remove_status=$?

	assertEquals "Whole-root cleanup must reject a same-owner mode-0700 directory that replaced the allocated object." \
		1 "$remove_status"
	assertTrue "Rejected run-root object replacement must not delete its contents." \
		"[ -f '$run_root/must-survive' ]"
	rm -f "$run_root/must-survive"
	rmdir "$run_root"
	mv "$saved_root" "$run_root"
	zxfer_remove_run_tmp_root >/dev/null
}

test_zxfer_run_tmp_root_lifecycle_compares_the_creation_record_without_id() {
	id_log="$TEST_TMPDIR/run-root-id.log"
	rm -f "$id_log"
	output=$(
		(
			zxfer_discard_runtime_cleanup_state
			zxfer_ensure_run_tmp_root || exit 90
			run_root=$g_zxfer_run_tmp_root
			id() {
				printf '%s\n' "$*" >>"$id_log"
				command id "$@"
			}
			printf 'record_mode=%s\n' "${g_zxfer_owned_run_tmp_root_identity##*"$ZXFER_TAB"}"
			chmod 755 "$run_root"
			zxfer_remove_run_tmp_root
			printf 'widened_status=%s\n' "$?"
			chmod 700 "$run_root"
			zxfer_remove_run_tmp_root
			printf 'restored_status=%s\n' "$?"
			[ -e "$run_root" ] && printf '%s\n' 'root=present'
		)
	)

	assertContains "Root creation should record the private mode with its identity." \
		"$output" "record_mode=700"
	assertContains "Whole-root removal must refuse a root whose mode is no longer 0700." \
		"$output" "widened_status=1"
	assertContains "Whole-root removal should succeed once the recorded state is back." \
		"$output" "restored_status=0"
	assertNotContains "The removed root must not remain." "$output" "root=present"
	assertFalse "Whole-root removal must not run id." "[ -s '$id_log' ]"
}

test_zxfer_ensure_run_tmp_root_rejects_a_root_that_is_not_mode_0700() {
	candidate_root="$TEST_TMPDIR/zxfer.$$.wide-mode"
	zxfer_test_capture_subshell '
		zxfer_discard_runtime_cleanup_state
		zxfer_try_get_effective_tmpdir() {
			g_zxfer_effective_tmpdir="'"$TEST_TMPDIR"'"
		}
		mktemp() {
			mkdir -m 755 "'"$candidate_root"'" || return 1
			printf "%s\n" "'"$candidate_root"'"
		}
		zxfer_ensure_run_tmp_root
		printf "status=%s root=<%s>\n" "$?" "$g_zxfer_run_tmp_root"
		[ -e "'"$candidate_root"'" ] && printf "%s\n" "candidate=present"
	'

	assertContains "Run-root allocation should fail closed when the new root is not private." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1 root=<>"
	assertNotContains "A rejected run root should be removed before failure returns." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "candidate=present"
}

test_zxfer_ensure_run_tmp_root_accepts_special_bits_in_front_of_mode_0700() {
	candidate_root="$TEST_TMPDIR/zxfer.$$.setgid-mode"
	zxfer_test_capture_subshell '
		zxfer_discard_runtime_cleanup_state
		zxfer_try_get_effective_tmpdir() {
			g_zxfer_effective_tmpdir="'"$TEST_TMPDIR"'"
		}
		mktemp() {
			mkdir -m 700 "'"$candidate_root"'" || return 1
			printf "%s\n" "'"$candidate_root"'"
		}
		zxfer_get_private_directory_security_record() {
			printf "device-inode:1:2\t0\t2700\n"
		}
		zxfer_ensure_run_tmp_root
		printf "status=%s root=<%s>\n" "$?" "$g_zxfer_run_tmp_root"
		zxfer_run_tmp_root_is_current_private_dir "$g_zxfer_run_tmp_root"
		printf "current=%s\n" "$?"
		rmdir "'"$candidate_root"'"
	'

	assertContains "A 0700 root with a GNU-reported setgid bit (2700) should be accepted." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=0 root=<$candidate_root>"
	assertContains "Removal should accept the unchanged 2700 creation record." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "current=0"
}

test_zxfer_cleanup_registered_runtime_artifacts_fails_on_an_unpaired_identity_line() {
	zxfer_test_capture_subshell '
		g_zxfer_runtime_artifact_cleanup_paths="device-inode:1:2"
		zxfer_cleanup_runtime_artifact_path() {
			printf "cleaned=%s\n" "$1"
		}
		zxfer_cleanup_registered_runtime_artifacts
		printf "status=%s\n" "$?"
	'

	assertContains "A registry ending on an identity line without its path must fail closed." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
	assertNotContains "Nothing should be cleaned for a damaged registry entry." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "cleaned="
}

test_zxfer_cleanup_runtime_artifact_path_rejects_replaced_registered_directory() {
	registered_dir="$TEST_TMPDIR/.zxfer-replaced-stage.$$"
	saved_dir="$TEST_TMPDIR/.zxfer-original-stage.$$"
	mkdir -m 700 "$registered_dir"
	zxfer_register_runtime_artifact_path "$registered_dir"
	mv "$registered_dir" "$saved_dir"
	mkdir -m 700 "$registered_dir"
	: >"$registered_dir/must-survive"

	zxfer_cleanup_runtime_artifact_path "$registered_dir" >/dev/null 2>&1
	cleanup_status=$?

	assertEquals "Recursive cleanup must reject a real directory that replaced the registered staging object." \
		1 "$cleanup_status"
	assertTrue "A rejected adjacent-directory replacement and its contents must remain untouched." \
		"[ -f '$registered_dir/must-survive' ]"
	rm -f "$registered_dir/must-survive"
	rmdir "$registered_dir"
	mv "$saved_dir" "$registered_dir"
	zxfer_cleanup_runtime_artifact_path "$registered_dir" >/dev/null
}

test_zxfer_register_runtime_artifact_path_rejects_unreserved_and_symlink_paths() {
	unsafe_path="$TEST_TMPDIR/operator-data-file"
	stage_target="$TEST_TMPDIR/zxfer.stage-target"
	stage_link="$TEST_TMPDIR/zxfer.stage-link"
	: >"$unsafe_path"
	: >"$stage_target"
	ln -s "$stage_target" "$stage_link"

	zxfer_register_runtime_artifact_path "$unsafe_path"
	unsafe_status=$?
	zxfer_register_runtime_artifact_path "$stage_link"
	symlink_status=$?

	assertEquals "Adjacent cleanup registration should accept only reserved zxfer staging names." \
		1 "$unsafe_status"
	assertEquals "Adjacent cleanup registration should reject symlink entries." \
		1 "$symlink_status"
	assertEquals "Rejected paths must not enter the exact cleanup registry." \
		"" "${g_zxfer_runtime_artifact_cleanup_paths:-}"
}

test_runtime_artifact_allocators_reject_path_components_in_prefixes() {
	escape_path="$TEST_TMPDIR/escape.1"

	zxfer_create_runtime_artifact_file "../escape" >/dev/null 2>&1
	file_status=$?
	zxfer_create_private_temp_dir "../escape" >/dev/null 2>&1
	dir_status=$?

	assertEquals "Runtime artifact file prefixes should reject parent-directory components." \
		1 "$file_status"
	assertEquals "Runtime artifact directory prefixes should reject parent-directory components." \
		1 "$dir_status"
	assertFalse "Rejected prefixes must not allocate outside the private run root." \
		"[ -e '$escape_path' ]"
}

test_zxfer_cleanup_runtime_artifact_paths_removes_and_unregisters_multiple_paths() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_private_temp_dir "runtime-cleanup-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result

	zxfer_cleanup_runtime_artifact_paths "$file_path" "$dir_path"
	cleanup_status=$?

	assertEquals "Multi-path runtime artifact cleanup should succeed when every registered path can be deleted." \
		0 "$cleanup_status"
	assertFalse "Multi-path runtime artifact cleanup should remove registered files." \
		"[ -e \"$file_path\" ]"
	assertFalse "Multi-path runtime artifact cleanup should remove registered directories." \
		"[ -e \"$dir_path\" ]"
	assertNotContains "Multi-path runtime artifact cleanup should unregister deleted files." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertNotContains "Multi-path runtime artifact cleanup should unregister deleted directories." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
}

test_zxfer_cleanup_runtime_artifact_paths_preserves_failures_when_one_path_cannot_be_removed() {
	output_file="$TEST_TMPDIR/runtime_cleanup_paths_failure.out"

	(
		zxfer_cleanup_runtime_artifact_path() {
			case "$1" in
			fail-path) return 1 ;;
			esac
			command printf 'cleaned=%s\n' "$1"
			return 0
		}
		set +e
		zxfer_cleanup_runtime_artifact_paths "fail-path" "ok-path"
		status=$?
		set -e
		command printf 'status=%s\n' "$status"
	) >"$output_file"
	output=$(cat "$output_file")

	assertContains "Multi-path runtime artifact cleanup should still attempt later paths after an earlier failure." \
		"$output" "cleaned=ok-path"
	assertContains "Multi-path runtime artifact cleanup should return failure when any one path cannot be removed." \
		"$output" "status=1"
}

test_zxfer_cleanup_runtime_artifact_path_list_removes_newline_delimited_paths() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-list-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_private_temp_dir "runtime-cleanup-list-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result
	path_list=$(printf '%s\n%s\n' "$file_path" "$dir_path")

	zxfer_cleanup_runtime_artifact_path_list "$path_list"
	cleanup_status=$?

	assertEquals "List-based runtime artifact cleanup should succeed when every listed artifact is removed." \
		0 "$cleanup_status"
	assertFalse "List-based runtime artifact cleanup should remove listed files." \
		"[ -e \"$file_path\" ]"
	assertFalse "List-based runtime artifact cleanup should remove listed directories." \
		"[ -e \"$dir_path\" ]"
	assertNotContains "List-based runtime artifact cleanup should unregister listed files." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertNotContains "List-based runtime artifact cleanup should unregister listed directories." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
}

test_runtime_file_list_cleanup_batches_rm_and_retains_unowned_paths() {
	zxfer_create_runtime_artifact_file "batch-one" >/dev/null
	first_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_file "batch-two" >/dev/null
	second_path=$g_zxfer_runtime_artifact_path_result
	outside_path="$TEST_TMPDIR/unowned-sentinel"
	printf sentinel >"$outside_path"
	rm_log="$TEST_TMPDIR/batched-rm.log"
	: >"$rm_log"
	(
		rm() {
			printf 'rm\n' >>"$rm_log"
			command rm "$@"
		}
		zxfer_cleanup_runtime_artifact_path_list "$first_path
$outside_path
$second_path"
	)
	cleanup_status=$?
	assertEquals "An unowned path makes cleanup fail closed." 1 "$cleanup_status"
	assertEquals "Owned regular files use one rm invocation." rm "$(cat "$rm_log")"
	assertFalse "The first owned file is removed." "[ -e '$first_path' ]"
	assertFalse "The second owned file is removed." "[ -e '$second_path' ]"
	assertEquals "Unowned data stays untouched." sentinel "$(cat "$outside_path")"
}

test_runtime_file_list_cleanup_preserves_partial_rm_failure() {
	zxfer_create_runtime_artifact_file "partial-one" >/dev/null
	first_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_file "partial-two" >/dev/null
	second_path=$g_zxfer_runtime_artifact_path_result
	(
		rm() {
			command rm -f "$first_path"
			return 1
		}
		zxfer_cleanup_runtime_artifact_path_list "$first_path
$second_path"
	)
	cleanup_status=$?
	assertEquals "A partial rm failure is preserved." 1 "$cleanup_status"
	assertFalse "A file removed before the failure stays removed." "[ -e '$first_path' ]"
	assertTrue "The failed file remains for whole-root cleanup." "[ -f '$second_path' ]"
}

test_zxfer_cleanup_runtime_artifact_path_list_and_return_preserves_original_status() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-list-return" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result

	zxfer_cleanup_runtime_artifact_path_list_and_return 37 "$file_path"
	cleanup_status=$?

	assertEquals "List cleanup return helper should preserve the caller's original status." \
		37 "$cleanup_status"
	assertFalse "List cleanup return helper should still remove listed artifacts." \
		"[ -e \"$file_path\" ]"
}

test_zxfer_write_and_read_runtime_artifact_file_preserve_multiline_payloads() {
	read_output_file="$TEST_TMPDIR/runtime-readback.out"
	zxfer_create_runtime_artifact_file "runtime-readback" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	payload=$(printf '%s\n' \
		"line one" \
		"line two")

	zxfer_write_runtime_artifact_file "$artifact_path" "$payload"
	write_status=$?
	zxfer_read_runtime_artifact_file "$artifact_path" >"$read_output_file"
	read_status=$?
	read_output=$(cat "$read_output_file")

	assertEquals "Runtime artifact writes should succeed for multiline payloads." \
		0 "$write_status"
	assertEquals "Runtime artifact reads should succeed for multiline payloads." \
		0 "$read_status"
	assertEquals "Runtime artifact reads should print nothing." \
		"" "$read_output"
	assertEquals "Runtime artifact reads should publish the exact multiline payload in shared scratch state." \
		"$payload" "$g_zxfer_runtime_artifact_read_result"
}

test_zxfer_read_runtime_artifact_file_preserves_trailing_blank_lines_exactly() {
	read_output_file="$TEST_TMPDIR/runtime-readback-trailing.out"
	scratch_output_file="$TEST_TMPDIR/runtime-readback-trailing.scratch"
	expected_hex="6c696e65206f6e650a0a0a"
	zxfer_create_runtime_artifact_file "runtime-readback-trailing" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	printf 'line one\n\n\n' >"$artifact_path"

	zxfer_read_runtime_artifact_file "$artifact_path" >"$read_output_file"
	read_status=$?
	printf '%s' "$g_zxfer_runtime_artifact_read_result" >"$scratch_output_file"
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Runtime artifact reads with trailing blank lines should succeed." \
		0 "$read_status"
	assertFalse "Runtime artifact reads should print nothing." \
		"[ -s '$read_output_file' ]"
	assertEquals "Runtime artifact reads should preserve trailing blank lines in shared scratch state." \
		"$expected_hex" "$scratch_output_hex"
}

test_zxfer_read_runtime_artifact_file_preserves_nonzero_status_and_clears_scratch() {
	artifact_path="$TEST_TMPDIR/runtime-readback-failure"
	: >"$artifact_path"

	output=$(
		(
			g_zxfer_runtime_artifact_read_result="stale-runtime-readback"
			cat() {
				return 26
			}
			zxfer_read_runtime_artifact_file "$artifact_path" >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)

	assertContains "Runtime artifact readback failures should preserve the original nonzero status." \
		"$output" "status=26"
	assertContains "Runtime artifact readback failures should clear the shared readback scratch state." \
		"$output" "scratch=<>"
}

test_zxfer_read_runtime_artifact_file_trimmed_strips_one_trailing_newline_from_scratch() {
	read_output_file="$TEST_TMPDIR/runtime-readback-trimmed.out"
	scratch_output_file="$TEST_TMPDIR/runtime-readback-trimmed.scratch"
	expected_scratch_hex="6c696e65206f6e650a0a"
	zxfer_create_runtime_artifact_file "runtime-readback-trimmed" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	printf 'line one\n\n\n' >"$artifact_path"

	zxfer_read_runtime_artifact_file_trimmed "$artifact_path" >"$read_output_file"
	read_status=$?
	printf '%s' "$g_zxfer_runtime_artifact_read_result" >"$scratch_output_file"
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Trimmed runtime artifact reads should preserve a successful status." \
		0 "$read_status"
	assertFalse "Trimmed runtime artifact reads should print nothing." \
		"[ -s '$read_output_file' ]"
	assertEquals "Trimmed runtime artifact reads should strip exactly one trailing newline in shared scratch state." \
		"$expected_scratch_hex" "$scratch_output_hex"
}

test_zxfer_read_runtime_artifact_file_trimmed_preserves_read_failures() {
	artifact_path="$TEST_TMPDIR/runtime-readback-trimmed-failure"
	: >"$artifact_path"

	output=$(
		(
			g_zxfer_runtime_artifact_read_result="stale-runtime-readback"
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result=""
				return 29
			}
			zxfer_read_runtime_artifact_file_trimmed "$artifact_path" >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)

	assertContains "Trimmed runtime artifact reads should preserve lower-level read failures." \
		"$output" "status=29"
	assertContains "Trimmed runtime artifact reads should leave the lower-level failure scratch state intact." \
		"$output" "scratch=<>"
}

test_zxfer_write_runtime_artifact_file_creates_empty_files_without_caller_truncation() {
	artifact_path="$TEST_TMPDIR/runtime-empty-payload"

	zxfer_write_runtime_artifact_file "$artifact_path" ""
	write_status=$?

	assertEquals "Runtime artifact writes should succeed when asked to create an empty file." \
		0 "$write_status"
	assertTrue "Runtime artifact writes should create the destination file for empty payloads." \
		"[ -f \"$artifact_path\" ]"
	assertTrue "Runtime artifact writes should leave empty payload files at zero bytes." \
		"[ ! -s \"$artifact_path\" ]"
}

test_zxfer_write_runtime_artifact_file_suppresses_shell_redirection_stderr() {
	artifact_path="$TEST_TMPDIR/runtime-missing-parent/payload"

	output=$(
		(
			zxfer_write_runtime_artifact_file "$artifact_path" "payload"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertEquals "Runtime artifact write failures should stay silent so callers control the operator-facing error." \
		"status=1" "$output"
}

test_zxfer_write_runtime_artifact_file_preserves_non_redirection_failure_status() {
	artifact_path="$TEST_TMPDIR/runtime-nonredirection-failure"

	output=$(
		(
			printf() {
				return 7
			}
			set +e
			zxfer_write_runtime_artifact_file "$artifact_path" "payload"
			status=$?
			set -e
			command printf 'status=%s\n' "$status"
		)
	)

	assertContains "Runtime artifact writes should preserve non-redirection shell failures from the payload writer." \
		"$output" "status=7"
}

test_runtime_artifact_registry_helpers_cover_rejected_and_missing_entries() {
	set +e
	zxfer_runtime_artifact_registration_path_has_safe_shape "relative-stage"
	relative_status=$?
	child_statuses=$(
		(
			zxfer_ensure_run_tmp_root || exit 90
			zxfer_runtime_artifact_path_is_run_root_child \
				"$g_zxfer_run_tmp_root/child"
			printf 'direct=%s ' "$?"
			zxfer_runtime_artifact_path_is_run_root_child \
				"$g_zxfer_run_tmp_root/nested/child"
			printf 'nested=%s\n' "$?"
			zxfer_remove_run_tmp_root
		)
	)

	g_zxfer_runtime_artifact_cleanup_paths=""
	zxfer_runtime_artifact_path_is_registered \
		"$TEST_TMPDIR/zxfer.missing-stage"
	missing_identity_status=$?

	assertEquals "Runtime artifact registration should reject non-absolute paths." \
		1 "$relative_status"
	assertEquals "A contained runtime artifact must be one direct run-root child, never a nested path." \
		"direct=0 nested=1" "$child_statuses"
	assertEquals "Runtime artifact identity lookup should fail for an unregistered directory." \
		1 "$missing_identity_status"
	assertEquals "Missing runtime artifact identity lookup should clear the owner result channel." \
		"" "$g_zxfer_runtime_artifact_directory_identity_result"
}

test_runtime_artifact_registry_keeps_one_identity_path_pair_per_entry() {
	stage_file="$TEST_TMPDIR/zxfer.registry-file"
	stage_dir="$TEST_TMPDIR/.zxfer-registry-dir"
	glob_dir="$TEST_TMPDIR/.zxfer-registry-[glob]*"
	: >"$stage_file"
	mkdir -p "$stage_dir" "$glob_dir"
	output=$(
		(
			g_zxfer_runtime_artifact_cleanup_paths=""
			zxfer_register_runtime_artifact_path "$stage_file" || exit 90
			zxfer_register_runtime_artifact_path "$stage_dir" || exit 91
			zxfer_register_runtime_artifact_path "$glob_dir" || exit 92
			zxfer_register_runtime_artifact_path "$stage_file" || exit 93
			dir_identity=$(zxfer_get_path_device_inode "$stage_dir") || exit 94
			printf 'pairs=%s\n' "$(printf '%s\n' "$g_zxfer_runtime_artifact_cleanup_paths" | wc -l | tr -d ' ')"
			zxfer_runtime_artifact_path_is_registered "$stage_file"
			printf 'file_identity_status=%s result=<%s>\n' "$?" \
				"$g_zxfer_runtime_artifact_directory_identity_result"
			zxfer_runtime_artifact_path_is_registered "$stage_dir"
			[ "$g_zxfer_runtime_artifact_directory_identity_result" = "$dir_identity" ] &&
				printf '%s\n' 'dir_identity=current'
			zxfer_runtime_artifact_path_is_registered "${stage_dir#/}"
			printf 'relative_lookup=%s\n' "$?"
			zxfer_runtime_artifact_path_is_registered "$TEST_TMPDIR/.zxfer-registry-*"
			printf 'pattern_lookup=%s\n' "$?"
			zxfer_runtime_artifact_path_is_registered "$stage_dir" drop
			printf 'after_drop=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
			zxfer_runtime_artifact_path_is_registered "$glob_dir" drop
			zxfer_runtime_artifact_path_is_registered "$stage_file" drop
			printf 'emptied=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
		)
	)

	assertContains "A duplicate registration should not add a second pair." "$output" "pairs=6"
	assertContains "A registered file should publish - as its identity." \
		"$output" "file_identity_status=0 result=<->"
	assertContains "A registered directory should publish the identity it had at registration." \
		"$output" "dir_identity=current"
	assertContains "Registry lookups should reject relative paths." "$output" "relative_lookup=1"
	assertContains "Registry lookups should match paths literally, never as patterns." \
		"$output" "pattern_lookup=1"
	assertContains "Dropping one entry should keep every other pair in order." \
		"$output" "after_drop=<-
$stage_file
$(zxfer_get_path_device_inode "$glob_dir")
$glob_dir>"
	assertContains "Dropping every entry should empty the registry." "$output" "emptied=<>"
}

test_try_get_effective_tmpdir_fails_cleanly_when_no_safe_default_exists() {
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			# A candidate list with no safe entry exhausts the fallback walk.
			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$TEST_TMPDIR/no-such-default-candidate"
			}
			set +e
			zxfer_try_get_effective_tmpdir >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'requested=%s\n' "${g_zxfer_effective_tmpdir_requested:-}"
			printf 'effective=<%s>\n' "${g_zxfer_effective_tmpdir:-}"
		)
	)

	assertEquals "Temp-root resolution should fail cleanly when both TMPDIR and the built-in defaults are unavailable." \
		"status=1
requested=__ZXFER_DEFAULT_TMPDIR__
effective=<>" "$output"
}

# zxfer-test-fragment: suites/zxfer_runtime_tmpdir_tests.sh
# shellcheck source=tests/suites/zxfer_runtime_tmpdir_tests.sh
. "$TESTS_DIR/suites/zxfer_runtime_tmpdir_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_runtime.sh" \
		"$TESTS_DIR/suites/zxfer_runtime_tmpdir_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
