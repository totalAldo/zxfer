#!/bin/sh
#
# shunit2 tests for zxfer_runtime.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# Session owns startup and shutdown composition; source the complete graph for
# lifecycle tests while runtime-only helpers remain independently testable.
zxfer_source_runtime_modules_through "zxfer_session.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_runtime"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_BACKUP_DIR
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_runtime_artifact_state
	zxfer_reset_background_job_state
	zxfer_reset_cleanup_pid_tracking
	zxfer_reset_failure_context "unit"
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
}

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
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

test_refresh_backup_storage_root_rejects_relative_override() {
	zxfer_test_capture_subshell '
		ZXFER_BACKUP_DIR="relative-backups"
		zxfer_refresh_backup_storage_root
	'

	assertEquals "Relative ZXFER_BACKUP_DIR overrides should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative backup-root errors should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_BACKUP_DIR must be an absolute path"
}

test_init_backup_storage_root_ignores_inherited_internal_state() {
	output=$(
		(
			unset ZXFER_BACKUP_DIR
			g_backup_storage_root="$TEST_TMPDIR/inherited-internal-root"
			zxfer_init_backup_storage_root
			printf 'default=%s\n' "$g_backup_storage_root"

			ZXFER_BACKUP_DIR="$TEST_TMPDIR/public-backup-root"
			g_backup_storage_root="$TEST_TMPDIR/second-inherited-root"
			zxfer_init_backup_storage_root
			printf 'public=%s\n' "$g_backup_storage_root"
		)
	)

	assertContains "Backup-root initialization must ignore an inherited internal cache when the public override is unset." \
		"$output" "default=/var/db/zxfer"
	assertContains "Backup-root initialization should still honor the documented public environment override." \
		"$output" "public=$TEST_TMPDIR/public-backup-root"
}

test_get_temp_file_creates_unique_paths() {
	file_one=$(zxfer_get_temp_file)
	file_two=$(zxfer_get_temp_file)

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
	assertEquals "Temp-file group allocation should print the same newline-delimited paths it stores." \
		"$g_zxfer_temp_file_group_result" "$(cat "$group_output_file")"
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
			printf 'registered=<%s>\n' "$g_zxfer_cleanup_pids"

			zxfer_unregister_cleanup_pid "$first_pid"
			printf 'after_unregister=<%s>\n' "$g_zxfer_cleanup_pids"

			zxfer_register_cleanup_pid "$$" "current shell"
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s\n' "$1"
				zxfer_unregister_cleanup_pid "$1"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'after_kill=<%s>\n' "$g_zxfer_cleanup_pids"
		)
	)

	kill -s TERM "$first_pid" >/dev/null 2>&1 || true
	kill -s TERM "$second_pid" >/dev/null 2>&1 || true
	wait "$first_pid" 2>/dev/null || true
	wait "$second_pid" 2>/dev/null || true

	assertContains "Cleanup PID registration should keep unique live helper PIDs." \
		"$output" "registered=<$first_pid $second_pid>"
	assertContains "Cleanup PID unregistration should remove only the requested helper PID." \
		"$output" "after_unregister=<$second_pid>"
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
	assertEquals "Registered rows should carry only the PID and purpose." \
		"$tracked_pid	unit cleanup helper" "$g_zxfer_cleanup_pid_records"
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
	assertContains "The direct-child record should retain its purpose." "$output" "identity unavailable helper>"
	assertContains "The registered direct child should be signalled." "$output" "abort_status=0"
	assertNotContains "Signalling must retain ownership until wait." "$output" "after_signal=<>"
	assertContains "Explicit wait/unregister should release ownership." "$output" "after_wait=<>"
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
	assertEquals "No tracked PIDs should exist after rejected registrations." \
		"" "$g_zxfer_cleanup_pids"
}

test_zxfer_register_cleanup_pid_fails_closed_when_purpose_normalization_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		tracked_pid=$!
		zxfer_normalize_owned_lock_text_field() {
			return 1
		}
		zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
		printf "status=%s\n" "$?"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
		wait "$tracked_pid" 2>/dev/null || true
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Registration should fail closed when purpose normalization fails." \
		"$output" "status=1"
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
	assertEquals "Wait/unregister should remove the tracked PID." "" "$g_zxfer_cleanup_pids"
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
	g_zxfer_cleanup_pids=$dead_pid
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
		g_zxfer_cleanup_pids="701"
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

		g_zxfer_cleanup_pids=""
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

		g_zxfer_cleanup_pids="703"
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
		printf "tracked=%s\n" "$g_zxfer_cleanup_pids"
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

test_zxfer_abort_direct_child_pid_fails_closed_when_purpose_normalization_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		child_pid=$!
		zxfer_normalize_owned_lock_text_field() {
			return 1
		}
		zxfer_abort_direct_child_pid "$child_pid" TERM "unit direct helper"
		printf "status=%s\n" "$?"
		kill -s TERM "$child_pid" >/dev/null 2>&1 || true
		wait "$child_pid" 2>/dev/null || true
	'

	assertContains "Direct-child aborts should fail closed when purpose normalization fails." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
}

test_zxfer_kill_registered_cleanup_pids_reaps_successes_and_preserves_failures() {
	zxfer_test_capture_subshell '
		set +e
		g_zxfer_cleanup_pid_abort_grace_seconds=0
		g_zxfer_cleanup_pids="401 402"
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
		printf "remaining=<%s>\n" "$g_zxfer_cleanup_pids"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Cleanup-helper shutdown should preserve the first abort failure status." \
		"$output" "status=1"
	assertContains "Cleanup-helper shutdown should preserve the first abort failure message." \
		"$output" "message=first cleanup abort failed"
	assertContains "Cleanup-helper shutdown should reap successful helpers and retain only a live failed helper." \
		"$output" "remaining=<401>"
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

test_runtime_global_init_covers_default_assignments_in_current_shell() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_refresh_secure_path_state() {
				:
			}
			zxfer_init_dependency_tool_defaults() {
				g_cmd_zfs="/sbin/zfs"
				g_cmd_compress_safe="gzip"
				g_cmd_decompress_safe="gunzip"
			}
			zxfer_apply_secure_path() {
				:
			}
			zxfer_ensure_run_tmp_root() {
				:
			}

			zxfer_init_globals
			zxfer_init_temp_artifacts

			printf 'version=%s\n' "$g_zxfer_version"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'origin_caps=<%s>\n' "$g_origin_remote_capabilities_response"
			printf 'control_sockets=%s\n' "$g_ssh_supports_control_sockets"
			printf 'local_zfs=%s\n' "$g_LZFS"
			printf 'backup_root=%s\n' "$g_backup_storage_root"
			printf 'backup_ext=%s\n' "$g_backup_file_extension"
			printf 'delete_source=<%s>\n' "$g_zxfer_snapshot_delete_source_identities_file"
			printf 'temp_prefix=%s\n' "$g_zxfer_temp_prefix"
		)
	)

	assertContains "Runtime metadata initialization should set the current zxfer version string." \
		"$output" "version=2.0.0-20260623"
	assertContains "Option default initialization should restore the single-job default." \
		"$output" "jobs=1"
	assertContains "Transport runtime defaults should clear cached remote capability payloads." \
		"$output" "origin_caps=<>"
	assertContains "Transport runtime defaults should publish the ssh control-socket support marker in current-shell state." \
		"$output" "control_sockets="
	assertContains "Transport runtime defaults should seed the local zfs helpers from the base zfs path." \
		"$output" "local_zfs=/sbin/zfs"
	assertContains "Runtime state defaults should restore the default backup metadata root." \
		"$output" "backup_root=/var/db/zxfer"
	assertContains "Runtime state defaults should restore the secure backup-file suffix." \
		"$output" "backup_ext=.zxfer_backup_info"
	assertContains "Temporary artifact initialization should leave delete-planning scratch paths unset until needed." \
		"$output" "delete_source=<>"
	assertContains "Temporary artifact initialization should publish the current run temp prefix." \
		"$output" "temp_prefix=zxfer."
}

test_zxfer_init_globals_applies_secure_path_after_reset_helpers() {
	output=$(
		(
			reset_replication_calls=0
			zxfer_refresh_secure_path_state() {
				printf '%s\n' "refresh"
			}
			zxfer_reset_replication_runtime_state() {
				reset_replication_calls=$((reset_replication_calls + 1))
			}
			zxfer_init_dependency_tool_defaults() {
				printf '%s\n' "deps"
			}
			zxfer_reset_ssh_transport_state() {
				printf '%s\n' "ssh-transport"
			}
			zxfer_reset_remote_host_state() {
				printf '%s\n' "remote-hosts"
			}
			zxfer_init_temp_artifacts() {
				printf '%s\n' "temp"
			}
			zxfer_apply_secure_path() {
				g_zxfer_runtime_path="/secure/path"
				printf '%s\n' "apply"
			}
			zxfer_init_globals
			printf 'runtime=<%s>\n' "${g_zxfer_runtime_path:-}"
			printf 'replication_resets=%s\n' "$reset_replication_calls"
		)
	)

	assertContains "Global runtime initialization should refresh the secure-path state before rebuilding defaults." \
		"$output" "refresh"
	assertContains "Global runtime initialization should reset replication state through the public helper when it is available." \
		"$output" "replication_resets=1"
	assertContains "Global runtime initialization should still run the dependency, transport, and temp default helpers." \
		"$output" "deps"
	assertContains "Global runtime initialization should still reapply the secure runtime PATH after rebuilding defaults." \
		"$output" "apply"
	assertContains "Global runtime initialization should leave the secure runtime PATH published in current-shell state." \
		"$output" "runtime=</secure/path>"
}

test_runtime_execution_context_init_helpers_cover_local_and_dry_run_remote_paths() {
	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$1"
			}
			zxfer_get_os() {
				if [ -n "$1" ]; then
					printf '%s\n' "RemoteOS"
				else
					printf '%s\n' "LocalOS"
				fi
			}
			zxfer_assign_required_tool() {
				eval "$1='/usr/bin/$2'"
			}
			zxfer_quote_cli_tokens() {
				printf 'quoted<%s>\n' "$1"
			}

			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="local-compress"
			g_cmd_decompress_safe="local-decompress"
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			g_origin_cmd_zfs=""
			g_target_cmd_zfs=""
			g_cmd_cat=""

			(
				zxfer_init_source_execution_context() {
					:
				}
				zxfer_init_destination_execution_context() {
					:
				}
				zxfer_refresh_remote_zfs_commands() {
					:
				}
				zxfer_init_restore_property_helpers() {
					:
				}
				zxfer_init_local_awk_compatibility() {
					:
				}
				zxfer_init_variables
				printf 'transfer_origin=%s\n' "$g_origin_cmd_compress_safe"
				printf 'transfer_target=%s\n' "$g_target_cmd_decompress_safe"
			)

			g_option_e_restore_property_mode=1
			g_option_O_origin_host=""
			zxfer_init_restore_property_helpers
			printf 'local_cat=%s\n' "$g_cmd_cat"

			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_option_n_dryrun=1
			g_option_z_compress=1
			g_cmd_cat=""
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			zxfer_init_source_execution_context
			zxfer_init_destination_execution_context
			zxfer_init_restore_property_helpers

			printf 'source_os=<%s>\n' "$g_source_operating_system"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
			printf 'origin_compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'dest_os=<%s>\n' "$g_destination_operating_system"
			printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
			printf 'target_decompress=%s\n' "$g_target_cmd_decompress_safe"
			printf 'remote_cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Transfer command context initialization should copy the local compression helper to the origin transport defaults." \
		"$output" "transfer_origin=local-compress"
	assertContains "Transfer command context initialization should copy the local decompression helper to the target transport defaults." \
		"$output" "transfer_target=local-decompress"
	assertContains "Restore-helper initialization should resolve the local cat helper when restore mode is enabled without an origin host." \
		"$output" "local_cat=/usr/bin/cat"
	assertContains "Dry-run remote source initialization should skip live OS probing and leave the cached source OS blank." \
		"$output" "source_os=<>"
	assertContains "Dry-run remote source initialization should still seed the origin zfs helper from the local zfs path." \
		"$output" "origin_zfs=/sbin/zfs"
	assertContains "Dry-run remote source initialization should quote the remote compression command when compression is enabled." \
		"$output" "origin_compress=quoted<zstd -3>"
	assertContains "Dry-run remote destination initialization should skip live OS probing and leave the cached destination OS blank." \
		"$output" "dest_os=<>"
	assertContains "Dry-run remote destination initialization should still seed the target zfs helper from the local zfs path." \
		"$output" "target_zfs=/sbin/zfs"
	assertContains "Dry-run remote destination initialization should quote the remote decompression command when compression is enabled." \
		"$output" "target_decompress=quoted<zstd -d>"
	assertContains "Dry-run remote restore-helper initialization should fall back to a literal cat helper." \
		"$output" "remote_cat=cat"
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

test_zxfer_init_runtime_state_defaults_discards_inherited_cleanup_paths_without_removing_them() {
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
	g_zxfer_runtime_artifact_cleanup_paths=$external_stage
	g_zxfer_cleanup_pids="424242"
	g_zxfer_cleanup_pid_records="424242	operator helper"
	g_zxfer_effective_tmpdir=$external_root
	g_zxfer_effective_tmpdir_requested=$external_root
	g_zxfer_temp_file_result="$external_root/inherited-temp-result"

	zxfer_init_runtime_state_defaults

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
		"" "$g_zxfer_cleanup_pids"
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
			zxfer_get_path_device_inode() {
				printf '%s\n' 'device-inode:1:2'
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

test_zxfer_try_get_socket_cache_tmpdir_returns_the_literal_validated_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	TMPDIR=$physical_tmpdir

	socket_tmpdir=$(zxfer_try_get_socket_cache_tmpdir)
	socket_tmpdir_status=$?

	assertEquals "A literal safe TMPDIR should satisfy socket-cache temp-root selection." \
		0 "$socket_tmpdir_status"
	assertEquals "Socket-cache temp-root selection should preserve the validated literal spelling." \
		"$physical_tmpdir" "$socket_tmpdir"
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
		zxfer_get_path_device_inode() {
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
	first_path=$(zxfer_get_temp_file)
	printf 'first payload\n' >"$first_path"
	second_path=$(zxfer_get_temp_file)

	assertNotEquals "Subshell allocations should never reuse a taken temp path." \
		"$first_path" "$second_path"
	assertEquals "Subshell allocations should never truncate earlier allocations." \
		"first payload" "$(cat "$first_path")"
	assertTrue "Subshell allocations should create the later temp file." \
		"[ -f \"$second_path\" ]"
}

test_zxfer_reset_runtime_artifact_state_cleans_registered_artifacts() {
	zxfer_create_runtime_artifact_file "runtime-reset-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_private_temp_dir "runtime-reset-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result

	zxfer_reset_runtime_artifact_state

	assertFalse "Resetting runtime artifact state should remove registered runtime files." \
		"[ -e \"$file_path\" ]"
	assertFalse "Resetting runtime artifact state should remove registered runtime directories." \
		"[ -e \"$dir_path\" ]"
	assertEquals "Resetting runtime artifact state should clear the registered cleanup path list." \
		"" "$g_zxfer_runtime_artifact_cleanup_paths"
	assertEquals "Resetting runtime artifact state should clear the shared path scratch result." \
		"" "$g_zxfer_runtime_artifact_path_result"
	assertEquals "Resetting runtime artifact state should clear the shared readback scratch result." \
		"" "$g_zxfer_runtime_artifact_read_result"
}

test_zxfer_reset_runtime_artifact_state_preserves_failed_cleanup_registrations() {
	artifact_path="$TEST_TMPDIR/zxfer.runtime-reset-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			zxfer_ensure_run_tmp_root || exit 90
			g_zxfer_runtime_artifact_path_result="stale-path"
			g_zxfer_runtime_artifact_read_result="stale-read"
			rm() {
				return 1
			}
			zxfer_reset_runtime_artifact_state
			status=$?
			printf 'status=%s\n' "$status"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
			printf 'path_result=<%s>\n' "$g_zxfer_runtime_artifact_path_result"
			printf 'read_result=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'root_retained=<%s>\n' "$([ -n "$g_zxfer_run_tmp_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Resetting runtime artifact state should preserve cleanup failures." \
		"$output" "status=1"
	assertContains "Resetting runtime artifact state should keep undeleted artifacts registered for later cleanup." \
		"$output" "registered=<$artifact_path>"
	assertContains "Resetting runtime artifact state should still clear the shared path scratch result after cleanup failures." \
		"$output" "path_result=<>"
	assertContains "Resetting runtime artifact state should still clear the shared readback scratch result after cleanup failures." \
		"$output" "read_result=<>"
	assertTrue "Resetting runtime artifact state should leave undeleted artifacts in place when cleanup fails." \
		"[ -e \"$artifact_path\" ]"
	assertContains "Resetting runtime artifact state should keep the undeleted run root tracked for later cleanup." \
		"$output" "root_retained=<yes>"
}

test_zxfer_trap_exit_cleans_registered_runtime_artifacts() {
	registered_file="$TEST_TMPDIR/zxfer.registered-runtime-file"
	registered_dir="$TEST_TMPDIR/zxfer.registered-runtime-dir"
	: >"$registered_file"
	mkdir -p "$registered_dir/subdir"
	: >"$registered_dir/subdir/payload"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_register_runtime_artifact_path "$registered_dir"
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success after removing registered runtime artifacts." \
		0 "$status"
	assertEquals "zxfer_trap_exit should keep stdout clean while removing registered runtime artifacts." \
		"" "$output"
	assertFalse "zxfer_trap_exit should remove registered runtime files." \
		"[ -e \"$registered_file\" ]"
	assertFalse "zxfer_trap_exit should remove registered runtime directories." \
		"[ -e \"$registered_dir\" ]"
}

test_zxfer_trap_exit_removes_the_per_run_temp_root_on_success_and_failure_paths() {
	success_output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_get_temp_file >/dev/null
			printf 'root=%s\n' "$g_zxfer_run_tmp_root" >&2
			true
			zxfer_trap_exit
		) 2>&1
	)
	success_status=$?
	success_root=${success_output#root=}

	assertEquals "zxfer_trap_exit should preserve success after removing the per-run temp root." \
		0 "$success_status"
	assertNotEquals "The per-run temp root should exist before the trap runs." \
		"" "$success_root"
	assertFalse "zxfer_trap_exit should remove the per-run temp root and everything below it." \
		"[ -e \"$success_root\" ]"

	failure_root_file="$TEST_TMPDIR/trap-failure-root.path"
	(
		zxfer_close_all_ssh_control_sockets() {
			:
		}
		zxfer_echoV() {
			:
		}
		zxfer_get_temp_file >/dev/null
		printf '%s\n' "$g_zxfer_run_tmp_root" >"$failure_root_file"
		false
		zxfer_trap_exit
	) 2>/dev/null
	failure_status=$?
	failure_root=$(cat "$failure_root_file")

	assertEquals "zxfer_trap_exit should preserve the failing exit status while removing the per-run temp root." \
		1 "$failure_status"
	assertFalse "zxfer_trap_exit should remove the per-run temp root on failure paths too." \
		"[ -e \"$failure_root\" ]"
}

test_zxfer_trap_exit_surfaces_failed_run_tmp_root_removal_as_trap_cleanup_failure() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			zxfer_get_temp_file >/dev/null
			rm() {
				return 1
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when the per-run temp root cannot be removed." \
		1 "$status"
	assertContains "Failed run-root removal should surface as a runtime trap-cleanup failure." \
		"$output" "class=runtime"
	assertContains "Failed run-root removal should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Failed run-root removal should report the runtime temp-artifact cleanup message." \
		"$output" "message=Failed to remove one or more runtime temp artifacts during exit."
}

test_run_tmp_root_is_removed_when_the_process_is_terminated_mid_run() {
	leak_tmpdir="$TEST_TMPDIR/sigterm-leak-tmp"
	ready_flag="$TEST_TMPDIR/sigterm-child.ready"
	child_script="$TEST_TMPDIR/sigterm-child.sh"
	child_stderr="$TEST_TMPDIR/sigterm-child.stderr"
	rm -rf "$leak_tmpdir" "$ready_flag"
	mkdir -p "$leak_tmpdir"

	cat >"$child_script" <<EOF
#!/bin/sh
ZXFER_SOURCE_MODULES_ROOT="$ZXFER_ROOT" \\
	ZXFER_SOURCE_MODULES_THROUGH=zxfer_session.sh \\
	. "$ZXFER_ROOT/src/zxfer_modules.sh"
zxfer_load_modules zxfer_session.sh
TMPDIR="$leak_tmpdir"
export TMPDIR
zxfer_reset_failure_context "sigterm-test"
zxfer_reset_runtime_artifact_state
zxfer_reset_background_job_state
zxfer_reset_cleanup_pid_tracking
zxfer_reset_owned_lock_tracking
g_option_Y_yield_iterations=1
zxfer_register_runtime_traps
zxfer_get_temp_file >/dev/null
: >"$ready_flag"
# An interruptible wait so the TERM trap runs promptly.
sleep 30 &
wait \$!
EOF

	/bin/sh "$child_script" >/dev/null 2>"$child_stderr" &
	child_pid=$!
	wait_count=0
	while [ ! -e "$ready_flag" ] && [ "$wait_count" -lt 100 ]; do
		wait_count=$((wait_count + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	assertTrue "The SIGTERM fixture child should reach its ready state." \
		"[ -e \"$ready_flag\" ]"
	assertNotEquals "The SIGTERM fixture child should allocate under the per-run temp root before the signal." \
		"" "$(ls -A "$leak_tmpdir")"

	kill -s TERM "$child_pid" 2>/dev/null
	wait "$child_pid" 2>/dev/null

	assertEquals "A SIGTERM mid-run must not leak any temp state into TMPDIR." \
		"" "$(ls -A "$leak_tmpdir")"
}

test_zxfer_trap_exit_aborts_supervised_background_jobs_before_legacy_pid_cleanup() {
	cleanup_log="$TEST_TMPDIR/trap_supervisor_cleanup.log"
	: >"$cleanup_log"

	output=$(
		(
			CLEANUP_LOG="$cleanup_log"
			zxfer_abort_all_background_jobs() {
				printf '%s\n' "abort" >>"$CLEANUP_LOG"
			}
			zxfer_kill_registered_cleanup_pids() {
				printf '%s\n' "legacy" >>"$CLEANUP_LOG"
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when supervised background cleanup succeeds." \
		0 "$status"
	assertEquals "zxfer_trap_exit should run supervised background cleanup before legacy bare-PID cleanup." \
		"abort
legacy" "$(cat "$cleanup_log")"
	assertEquals "zxfer_trap_exit should keep stdout clean when cleanup succeeds." \
		"" "$output"
}

test_zxfer_trap_exit_fails_closed_when_supervised_background_cleanup_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_abort_all_background_jobs() {
				g_zxfer_background_job_abort_failure_message="validated abort failed"
				return 17
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve supervised background cleanup failure status." \
		17 "$status"
	assertContains "Supervised background cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Supervised background cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Supervised background cleanup failures should preserve the validated abort failure message." \
		"$output" "message=validated abort failed"
}

test_zxfer_trap_exit_fails_closed_when_validated_cleanup_helper_abort_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_kill_registered_cleanup_pids() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup helper abort failed"
				return 23
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve validated cleanup-helper teardown failure status." \
		23 "$status"
	assertContains "Validated cleanup-helper teardown failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Validated cleanup-helper teardown failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Validated cleanup-helper teardown failures should preserve the validated abort failure message." \
		"$output" "message=validated cleanup helper abort failed"
}

test_zxfer_trap_exit_fails_closed_when_ssh_socket_cleanup_fails_after_success() {
	registered_file="$TEST_TMPDIR/zxfer.trap-close-failure-artifact"
	: >"$registered_file"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_close_all_ssh_control_sockets() {
				printf '%s\n' "close failed" >&2
				return 19
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when ssh socket cleanup fails after an otherwise successful run." \
		19 "$status"
	assertContains "zxfer_trap_exit should preserve ssh socket cleanup diagnostics on stderr." \
		"$output" "close failed"
	assertContains "ssh socket cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "ssh socket cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "ssh socket cleanup failures should preserve the cleanup-specific failure message." \
		"$output" "message=Failed to close one or more ssh control sockets during exit."
	assertFalse "zxfer_trap_exit should continue removing registered runtime artifacts after ssh socket cleanup failures." \
		"[ -e \"$registered_file\" ]"
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
		"$output" "registered=<$artifact_path>"
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
	assertEquals "Runtime artifact reads should reproduce the exact multiline payload on stdout." \
		"$payload" "$read_output"
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
	read_output_hex=$(od -An -tx1 -v "$read_output_file" | tr -d ' \n')
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Runtime artifact reads should preserve trailing blank lines on stdout." \
		0 "$read_status"
	assertEquals "Runtime artifact reads should preserve trailing blank lines in stdout payloads." \
		"$expected_hex" "$read_output_hex"
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
	expected_stdout_hex="6c696e65206f6e650a0a0a"
	expected_scratch_hex="6c696e65206f6e650a0a"
	zxfer_create_runtime_artifact_file "runtime-readback-trimmed" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	printf 'line one\n\n\n' >"$artifact_path"

	zxfer_read_runtime_artifact_file_trimmed "$artifact_path" >"$read_output_file"
	read_status=$?
	printf '%s' "$g_zxfer_runtime_artifact_read_result" >"$scratch_output_file"
	read_output_hex=$(od -An -tx1 -v "$read_output_file" | tr -d ' \n')
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Trimmed runtime artifact reads should preserve a successful status." \
		0 "$read_status"
	assertEquals "Trimmed runtime artifact reads should emit the trimmed payload plus the helper's output newline." \
		"$expected_stdout_hex" "$read_output_hex"
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

test_zxfer_capture_runtime_artifact_command_output_reads_and_cleans_capture_file() {
	output=$(
		(
			zxfer_emit_runtime_capture_fixture() {
				printf '%s\n%s\n' "line one" "line two"
			}
			zxfer_capture_runtime_artifact_command_output "runtime-capture" zxfer_emit_runtime_capture_fixture
			capture_status=$?
			capture_file=$g_zxfer_runtime_artifact_path_result
			printf 'status=%s\n' "$capture_status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Runtime command captures should preserve successful command output in readback scratch." \
		"$output" "scratch=<line one
line two>"
	assertContains "Runtime command captures should return success for successful commands and readbacks." \
		"$output" "status=0"
	assertContains "Runtime command captures should remove the temporary capture file after readback." \
		"$output" "exists=no"
}

test_zxfer_capture_runtime_artifact_command_output_preserves_command_and_read_failures() {
	output=$(
		(
			zxfer_failing_runtime_capture_fixture() {
				printf '%s\n' "partial"
				return 37
			}
			g_zxfer_runtime_artifact_read_result="stale"
			zxfer_capture_runtime_artifact_command_output "runtime-capture" zxfer_failing_runtime_capture_fixture
			capture_status=$?
			capture_file=$g_zxfer_runtime_artifact_path_result
			printf 'command_status=%s\n' "$capture_status"
			printf 'command_scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'command_exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
		(
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result=""
				return 38
			}
			zxfer_capture_runtime_artifact_command_output "runtime-capture" printf '%s\n' "captured"
			capture_status=$?
			capture_file=$g_zxfer_runtime_artifact_path_result
			printf 'read_status=%s\n' "$capture_status"
			printf 'read_exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Runtime command captures should preserve command failures." \
		"$output" "command_status=37"
	assertContains "Runtime command captures should not publish partial command output after command failures." \
		"$output" "command_scratch=<>"
	assertContains "Runtime command captures should remove temp files after command failures." \
		"$output" "command_exists=no"
	assertContains "Runtime command captures should preserve readback failures." \
		"$output" "read_status=38"
	assertContains "Runtime command captures should remove temp files after readback failures." \
		"$output" "read_exists=no"
}

test_zxfer_capture_runtime_artifact_combined_command_output_preserves_status_after_readback() {
	output=$(
		(
			zxfer_failing_runtime_combined_capture_fixture() {
				printf '%s\n' "stdout-line"
				printf '%s\n' "stderr-line" >&2
				return 43
			}
			zxfer_capture_runtime_artifact_combined_command_output "runtime-combined" zxfer_failing_runtime_combined_capture_fixture
			capture_status=$?
			capture_file=$g_zxfer_runtime_artifact_path_result
			printf 'status=%s\n' "$capture_status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Combined runtime command captures should preserve the command status after successful readback." \
		"$output" "status=43"
	assertContains "Combined runtime command captures should publish stdout and stderr after command failures." \
		"$output" "scratch=<stdout-line
stderr-line>"
	assertContains "Combined runtime command captures should remove temp files after readback." \
		"$output" "exists=no"
}

test_zxfer_capture_runtime_artifact_combined_command_output_preserves_readback_failures() {
	output=$(
		(
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result=""
				return 44
			}
			zxfer_capture_runtime_artifact_combined_command_output "runtime-combined" printf '%s\n' "captured"
			capture_status=$?
			capture_file=$g_zxfer_runtime_artifact_path_result
			printf 'status=%s\n' "$capture_status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Combined runtime command captures should preserve readback failures." \
		"$output" "status=44"
	assertContains "Combined runtime command captures should leave failed readback scratch empty." \
		"$output" "scratch=<>"
	assertContains "Combined runtime command captures should remove temp files after readback failures." \
		"$output" "exists=no"
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

test_get_os_handles_local_and_remote_invocations() {
	assertEquals "A local zxfer_get_os call should match uname." \
		"$(uname)" "$(zxfer_get_os "")"

	remote_os=$(
		zxfer_get_remote_host_operating_system() {
			printf '%s\n' "RemoteOS"
		}
		zxfer_get_os "origin.example" source
	)

	assertEquals "A remote zxfer_get_os call should delegate to the remote helper." \
		"RemoteOS" "$remote_os"
}

test_init_globals_initializes_dependency_state_and_temp_files() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			g_zxfer_services_to_restart="stale-service"
			g_backup_file_contents="stale-backup"
			g_restored_backup_file_contents="stale-restore"
			g_zxfer_remote_capability_response_result="stale-caps"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_ssh_control_socket_action_result="stale-action"
			g_zxfer_ssh_control_socket_action_stderr="stale-stderr"
			g_recursive_source_list="stale-source"
			g_last_common_snap="stale@snap"
			g_zfs_send_job_pids="123 456"
			g_zxfer_background_job_records="stale-job	kind	111	wrapper	/tmp/bg"
			g_zxfer_background_job_wait_exit_status="stale-status"
			g_zxfer_property_table_lookup_result="stale-lookup"
			g_zxfer_source_pvs_raw="stale=property=local"
			g_zxfer_property_stage_file_read_result="stale-stage-read"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_init_globals
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'path=%s\n' "$PATH"
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'ps=%s\n' "$g_cmd_ps"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'tmp_source=%s\n' "$g_zxfer_snapshot_delete_source_identities_file"
			printf 'tmp_dest=%s\n' "$g_zxfer_snapshot_delete_destination_identities_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'backup=<%s>\n' "$g_backup_file_contents"
			printf 'restored=<%s>\n' "$g_restored_backup_file_contents"
			printf 'remote_caps=<%s>\n' "$g_zxfer_remote_capability_response_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'socket_action=<%s>\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'socket_stderr=<%s>\n' "$g_zxfer_ssh_control_socket_action_stderr"
			printf 'recursive=<%s>\n' "$g_recursive_source_list"
			printf 'last_common=<%s>\n' "$g_last_common_snap"
			printf 'send_pids=<%s>\n' "$g_zfs_send_job_pids"
			printf 'background_records=<%s>\n' "$g_zxfer_background_job_records"
			printf 'background_wait_status=<%s>\n' "$g_zxfer_background_job_wait_exit_status"
			printf 'table_lookup=<%s>\n' "$g_zxfer_property_table_lookup_result"
			printf 'source_pvs=<%s>\n' "$g_zxfer_source_pvs_raw"
			printf 'property_stage_read=<%s>\n' "$g_zxfer_property_stage_file_read_result"
		)
	)

	assertContains "zxfer_init_globals should initialize the secure path." \
		"$output" "secure=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should export the strict runtime PATH once runtime startup begins." \
		"$output" "path=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should resolve the awk helper." \
		"$output" "awk=/usr/bin/awk"
	assertContains "zxfer_init_globals should resolve the ps helper for supervised background-job validation." \
		"$output" "ps=/usr/bin/ps"
	assertContains "zxfer_init_globals should record ssh control-socket support." \
		"$output" "control=1"
	assertContains "zxfer_init_globals should leave snapshot-delete temp paths empty until delete planning needs them." \
		"$output" "tmp_source="
	assertContains "zxfer_init_globals should leave the paired snapshot-delete temp path empty until delete planning needs it." \
		"$output" "tmp_dest="
	assertContains "zxfer_init_globals should reset orchestration restart scratch state." \
		"$output" "restart=<>"
	assertContains "zxfer_init_globals should reset backup-metadata accumulation state." \
		"$output" "backup=<>"
	assertContains "zxfer_init_globals should reset restored backup scratch state." \
		"$output" "restored=<>"
	assertContains "zxfer_init_globals should reset remote capability handshake scratch state." \
		"$output" "remote_caps=<>"
	assertContains "zxfer_init_globals should reset remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "zxfer_init_globals should reset ssh control-socket action classification state." \
		"$output" "socket_action=<>"
	assertContains "zxfer_init_globals should reset ssh control-socket action stderr scratch state." \
		"$output" "socket_stderr=<>"
	assertContains "zxfer_init_globals should reset snapshot-discovery scratch state." \
		"$output" "recursive=<>"
	assertContains "zxfer_init_globals should reset snapshot-reconcile scratch state." \
		"$output" "last_common=<>"
	assertContains "zxfer_init_globals should reset send/receive PID tracking state." \
		"$output" "send_pids=<>"
	assertContains "zxfer_init_globals should reset supervised background-job registry state." \
		"$output" "background_records=<>"
	assertContains "zxfer_init_globals should reset supervised background-job wait scratch state." \
		"$output" "background_wait_status=<>"
	assertContains "zxfer_init_globals should reset property-table lookup scratch state." \
		"$output" "table_lookup=<>"
	assertContains "zxfer_init_globals should reset property-reconcile source scratch state." \
		"$output" "source_pvs=<>"
	assertContains "zxfer_init_globals should reset staged property-file read scratch state." \
		"$output" "property_stage_read=<>"
}

test_init_globals_defers_strict_path_export_until_startup_helpers_finish() {
	secure_path_dir="$TEST_TMPDIR/narrow-secure-path"
	mkdir -p "$secure_path_dir"

	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_SECURE_PATH="$secure_path_dir"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_init_globals
			status=$?
			printf 'status=%s\n' "$status"
			printf 'path=%s\n' "$PATH"
			printf 'tmp_source=%s\n' "$g_zxfer_snapshot_delete_source_identities_file"
		) 2>&1
	)

	assertContains "zxfer_init_globals should still finish startup when ZXFER_SECURE_PATH omits date/mktemp directories." \
		"$output" "status=0"
	assertContains "zxfer_init_globals should export the narrow secure PATH after startup completes." \
		"$output" "path=$secure_path_dir"
	assertContains "zxfer_init_globals should still finish startup before switching to the strict runtime PATH even when delete tempfiles are deferred." \
		"$output" "tmp_source="
	assertNotContains "Startup should not trip over missing bootstrap utilities when the strict PATH is applied at the end of init." \
		"$output" "command not found"
}

test_ensure_snapshot_delete_temp_artifacts_allocates_paths_lazily_in_current_shell() {
	output=$(
		(
			counter=0
			g_zxfer_snapshot_delete_source_identities_file=""
			g_zxfer_snapshot_delete_destination_identities_file=""
			g_zxfer_snapshot_delete_difference_file=""
			zxfer_get_temp_file() {
				counter=$((counter + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/delete.$counter"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only.$counter"
			}

			zxfer_ensure_snapshot_delete_temp_artifacts
			first_source=$g_zxfer_snapshot_delete_source_identities_file
			first_dest=$g_zxfer_snapshot_delete_destination_identities_file
			first_diff=$g_zxfer_snapshot_delete_difference_file

			zxfer_ensure_snapshot_delete_temp_artifacts

			printf 'source=%s\n' "$g_zxfer_snapshot_delete_source_identities_file"
			printf 'dest=%s\n' "$g_zxfer_snapshot_delete_destination_identities_file"
			printf 'diff=%s\n' "$g_zxfer_snapshot_delete_difference_file"
			printf 'reused=%s\n' \
				"$([ "$first_source" = "$g_zxfer_snapshot_delete_source_identities_file" ] &&
					[ "$first_dest" = "$g_zxfer_snapshot_delete_destination_identities_file" ] &&
					[ "$first_diff" = "$g_zxfer_snapshot_delete_difference_file" ] &&
					printf yes || printf no)"
			printf 'count=%s\n' "$counter"
		)
	)

	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the source path." \
		"$output" "source=$TEST_TMPDIR/delete.1"
	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the destination path." \
		"$output" "dest=$TEST_TMPDIR/delete.2"
	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the diff path." \
		"$output" "diff=$TEST_TMPDIR/delete.3"
	assertContains "Lazy snapshot-delete tempfile setup should reuse already-assigned paths on later calls." \
		"$output" "reused=yes"
	assertContains "Lazy snapshot-delete tempfile setup should allocate exactly once per required path." \
		"$output" "count=3"
}

test_ensure_snapshot_delete_temp_artifacts_preserves_allocation_failures_without_publishing_paths() {
	output=$(
		(
			g_zxfer_snapshot_delete_source_identities_file=""
			g_zxfer_snapshot_delete_destination_identities_file=""
			g_zxfer_snapshot_delete_difference_file=""
			zxfer_get_temp_file() {
				return 71
			}

			set +e
			zxfer_ensure_snapshot_delete_temp_artifacts
			status=$?
			set -e

			printf 'status=%s\n' "$status"
			printf 'source=<%s>\n' "${g_zxfer_snapshot_delete_source_identities_file:-}"
			printf 'dest=<%s>\n' "${g_zxfer_snapshot_delete_destination_identities_file:-}"
			printf 'diff=<%s>\n' "${g_zxfer_snapshot_delete_difference_file:-}"
		)
	)

	assertContains "Lazy snapshot-delete tempfile setup should preserve the first allocation failure status." \
		"$output" "status=71"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a source temp path when allocation fails." \
		"$output" "source=<>"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a destination temp path when allocation fails." \
		"$output" "dest=<>"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a diff temp path when allocation fails." \
		"$output" "diff=<>"
}

test_ensure_snapshot_delete_temp_artifacts_cleans_up_after_second_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/delete_temp_cleanup_second.log"
	g_zxfer_snapshot_delete_source_identities_file=""
	g_zxfer_snapshot_delete_destination_identities_file=""
	g_zxfer_snapshot_delete_difference_file=""
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-second-source"
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
	zxfer_ensure_snapshot_delete_temp_artifacts >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	source_path=${g_zxfer_snapshot_delete_source_identities_file:-}
	dest_path=${g_zxfer_snapshot_delete_destination_identities_file:-}
	diff_path=${g_zxfer_snapshot_delete_difference_file:-}

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell delete-temp setup should preserve the second allocation failure status." \
		71 "$status"
	assertEquals "Current-shell delete-temp setup should clean up the already allocated source tempfile when the second allocation fails." \
		"$TEST_TMPDIR/delete-second-source" "$cleanup_paths"
	assertEquals "Current-shell delete-temp setup should not publish the source tempfile after the second allocation fails." \
		"" "$source_path"
	assertEquals "Current-shell delete-temp setup should not publish the destination tempfile after the second allocation fails." \
		"" "$dest_path"
	assertEquals "Current-shell delete-temp setup should not publish the diff tempfile after the second allocation fails." \
		"" "$diff_path"
}

test_ensure_snapshot_delete_temp_artifacts_cleans_up_after_third_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/delete_temp_cleanup_third.log"
	g_zxfer_snapshot_delete_source_identities_file=""
	g_zxfer_snapshot_delete_destination_identities_file=""
	g_zxfer_snapshot_delete_difference_file=""
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-third-source"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-third-dest"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		3)
			return 72
			;;
		esac
		return 73
	}
	zxfer_cleanup_runtime_artifact_path_list() {
		printf '%s\n' "$1" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_ensure_snapshot_delete_temp_artifacts >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	source_path=${g_zxfer_snapshot_delete_source_identities_file:-}
	dest_path=${g_zxfer_snapshot_delete_destination_identities_file:-}
	diff_path=${g_zxfer_snapshot_delete_difference_file:-}

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell delete-temp setup should preserve the third allocation failure status." \
		72 "$status"
	assertEquals "Current-shell delete-temp setup should clean up both already allocated tempfiles when the third allocation fails." \
		"$TEST_TMPDIR/delete-third-source
$TEST_TMPDIR/delete-third-dest" "$cleanup_paths"
	assertEquals "Current-shell delete-temp setup should not publish the source tempfile after the third allocation fails." \
		"" "$source_path"
	assertEquals "Current-shell delete-temp setup should not publish the destination tempfile after the third allocation fails." \
		"" "$dest_path"
	assertEquals "Current-shell delete-temp setup should not publish the diff tempfile after the third allocation fails." \
		"" "$diff_path"
}

test_init_globals_calls_owner_reset_helpers() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			reset_log="$TEST_TMPDIR/init_globals_resets.log"
			: >"$reset_log"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_reset_replication_runtime_state() {
				printf 'replication\n' >>"$reset_log"
			}
			zxfer_reset_migration_service_state() {
				printf 'migration_services\n' >>"$reset_log"
			}
			zxfer_reset_send_job_state() {
				printf 'send_jobs\n' >>"$reset_log"
			}
			zxfer_reset_send_receive_state() {
				printf 'send_receive\n' >>"$reset_log"
			}
			zxfer_reset_background_job_state() {
				printf 'background_jobs\n' >>"$reset_log"
			}
			zxfer_reset_operation_state() {
				printf 'operation_state\n' >>"$reset_log"
			}
			zxfer_reset_destination_existence_cache() {
				printf 'destination_cache\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_record_indexes() {
				printf 'snapshot_indexes\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_discovery_state() {
				printf 'snapshot_discovery\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_reconcile_state() {
				printf 'snapshot_reconcile\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_delete_artifact_state() {
				printf 'snapshot_delete_artifacts\n' >>"$reset_log"
			}
			zxfer_reset_backup_metadata_state() {
				printf 'backup_metadata\n' >>"$reset_log"
			}
			zxfer_reset_property_runtime_state() {
				printf 'property_runtime\n' >>"$reset_log"
			}
			zxfer_reset_property_iteration_caches() {
				printf 'property_cache\n' >>"$reset_log"
			}
			zxfer_reset_property_reconcile_state() {
				printf 'property_reconcile\n' >>"$reset_log"
			}

			zxfer_init_globals
			cat "$reset_log"
		)
	)

	assertContains "zxfer_init_globals should delegate replication scratch reset to the replication owner helper." \
		"$output" "replication"
	assertContains "zxfer_init_globals should delegate migration recovery reset to the migration-service owner helper." \
		"$output" "migration_services"
	assertContains "zxfer_init_globals should delegate send-job queue reset to the send-job owner helper." \
		"$output" "send_jobs"
	assertContains "zxfer_init_globals should delegate send/receive scratch reset to the send/receive owner helper." \
		"$output" "send_receive"
	assertContains "zxfer_init_globals should delegate supervised background-job scratch reset to the background-job owner helper." \
		"$output" "background_jobs"
	assertContains "zxfer_init_globals should delegate pass mutation state to its owner helper." \
		"$output" "operation_state"
	assertContains "zxfer_init_globals should delegate destination cache reset to the snapshot-state owner helper." \
		"$output" "destination_cache"
	assertContains "zxfer_init_globals should delegate snapshot index reset to the snapshot-state owner helper." \
		"$output" "snapshot_indexes"
	assertContains "zxfer_init_globals should delegate snapshot discovery reset to the snapshot-discovery owner helper." \
		"$output" "snapshot_discovery"
	assertContains "zxfer_init_globals should delegate snapshot reconcile reset to the snapshot-reconcile owner helper." \
		"$output" "snapshot_reconcile"
	assertContains "zxfer_init_globals should delegate snapshot delete artifacts to the snapshot-reconcile owner helper." \
		"$output" "snapshot_delete_artifacts"
	assertContains "zxfer_init_globals should delegate backup metadata reset to the backup owner helper." \
		"$output" "backup_metadata"
	assertContains "zxfer_init_globals should delegate run-wide property state reset to the property owner helper." \
		"$output" "property_runtime"
	assertContains "zxfer_init_globals should delegate property-cache reset to the property-cache owner helper." \
		"$output" "property_cache"
	assertContains "zxfer_init_globals should delegate per-call property reconcile reset to the property owner helper." \
		"$output" "property_reconcile"
}

# Compatibility aliases live in an unregistered sourced fragment so named
# dispatch and --list-tests preserve the pre-split suite contract without
# executing the replacement behavior twice during an unfiltered run.
# zxfer-test-fragment: suites/zxfer_runtime_compatibility_alias_tests.sh
# shellcheck source=tests/suites/zxfer_runtime_compatibility_alias_tests.sh
. "$TESTS_DIR/suites/zxfer_runtime_compatibility_alias_tests.sh"

# zxfer-test-fragment: suites/zxfer_runtime_initialization_tests.sh
# shellcheck source=tests/suites/zxfer_runtime_initialization_tests.sh
. "$TESTS_DIR/suites/zxfer_runtime_initialization_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_runtime.sh" \
		"$TESTS_DIR/suites/zxfer_runtime_initialization_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
