#!/bin/sh
#
# shunit2 tests for zxfer_background_jobs.sh (supervision-lite model).
#
# Pins: status propagation (success, failure, missing status file, non-numeric
# status), FIFO completion-queue notification ordering, abort teardown of the
# whole job pipeline on both spawn paths (setsid process group and the
# cleanup-child-wrapper fallback), trap-style abort-all teardown, and the
# spawn failure paths.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_background_jobs.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_background_jobs"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	TMPDIR="$TEST_TMPDIR"
	g_zxfer_temp_prefix="zxfer.bgtest.$$"
	g_option_Y_yield_iterations=1
	zxfer_reset_runtime_artifact_state
	g_zxfer_background_job_use_setsid=""
	zxfer_reset_background_job_state
	# Force the wrapper fallback by default so the suite behaves the same on
	# hosts with and without setsid(1); setsid-specific tests opt back in.
	g_zxfer_background_job_use_setsid=0
	g_zxfer_background_job_abort_grace_seconds=1
	zxfer_reset_failure_context "unit"
}

# Poll for a file to become non-empty so spawned-job races stay bounded.
wait_for_nonempty_file() {
	l_wait_file=$1
	l_wait_tries=0

	while [ "$l_wait_tries" -lt 100 ]; do
		[ -s "$l_wait_file" ] && return 0
		sleep 0.1 2>/dev/null || sleep 1
		l_wait_tries=$((l_wait_tries + 1))
	done
	[ -s "$l_wait_file" ]
}

open_background_job_test_fifo_writer_fd9() {
	l_queue_path=$1
	l_open_attempt=0
	l_open_status=1

	exec 9>&- || true
	while [ "$l_open_attempt" -lt 8 ]; do
		l_open_attempt=$((l_open_attempt + 1))
		if { exec 9>&1; } >"$l_queue_path"; then
			return 0
		fi
		l_open_status=$?
	done

	return "$l_open_status"
}

open_background_job_test_fifo_reader_fd8() {
	l_queue_path=$1
	l_open_attempt=0
	l_open_status=1

	exec 8<&- || true
	while [ "$l_open_attempt" -lt 8 ]; do
		l_open_attempt=$((l_open_attempt + 1))
		if { exec 8<&0; } <"$l_queue_path"; then
			return 0
		fi
		l_open_status=$?
	done

	return "$l_open_status"
}

test_background_job_record_helpers_track_and_remove_jobs() {
	zxfer_register_background_job_record "job-1" "send_receive" 101 wrapper "$TEST_TMPDIR/job-1.status"
	zxfer_register_background_job_record "job-2" "source_snapshot_list" 202 process_group "$TEST_TMPDIR/job-2.status"
	zxfer_find_background_job_record "job-2"
	find_status=$?
	zxfer_unregister_background_job_record "job-1"

	assertEquals "Tracked background jobs should be discoverable by job id." \
		0 "$find_status"
	assertEquals "Tracked background jobs should preserve the recorded job-shell pid." \
		"202" "$g_zxfer_background_job_record_pid"
	assertEquals "Tracked background jobs should preserve the recorded teardown mode." \
		"process_group" "$g_zxfer_background_job_record_teardown"
	assertEquals "Tracked background jobs should preserve the recorded status file." \
		"$TEST_TMPDIR/job-2.status" "$g_zxfer_background_job_record_status_file"
	assertNotContains "Unregistering one background job should leave later records intact." \
		"$g_zxfer_background_job_records" "job-1"
	assertContains "Unregistering one background job should preserve unrelated records." \
		"$g_zxfer_background_job_records" "job-2"
}

test_background_job_record_helpers_reject_incomplete_rows_and_duplicate_ids() {
	zxfer_register_background_job_record "" "send_receive" 101 wrapper "$TEST_TMPDIR/f"
	missing_id_status=$?
	zxfer_register_background_job_record "job-3" "send_receive" "" wrapper "$TEST_TMPDIR/f"
	missing_pid_status=$?
	zxfer_register_background_job_record "job-3" "send_receive" 101 wrapper ""
	missing_file_status=$?
	zxfer_register_background_job_record "job-3" "send_receive" 101 wrapper "$TEST_TMPDIR/f"
	zxfer_register_background_job_record "job-3" "send_receive" 999 wrapper "$TEST_TMPDIR/other"
	duplicate_status=$?
	zxfer_find_background_job_record "job-3"

	assertEquals "Registration should reject rows without a job id." 1 "$missing_id_status"
	assertEquals "Registration should reject rows without a pid." 1 "$missing_pid_status"
	assertEquals "Registration should reject rows without a status file." 1 "$missing_file_status"
	assertEquals "Duplicate job ids should be ignored without error." 0 "$duplicate_status"
	assertEquals "Duplicate registrations should keep the first row's pid." \
		"101" "$g_zxfer_background_job_record_pid"
}

test_init_background_job_spawn_support_respects_cached_flag_and_missing_setsid() {
	g_zxfer_background_job_use_setsid=1
	zxfer_init_background_job_spawn_support
	cached_value=$g_zxfer_background_job_use_setsid

	output=$(
		g_zxfer_background_job_use_setsid=""
		# shellcheck disable=SC2123  # hide setsid from the probe on purpose
		PATH=""
		zxfer_init_background_job_spawn_support
		printf '%s' "$g_zxfer_background_job_use_setsid"
	)

	assertEquals "The spawn-support probe should respect a pre-set capability flag." \
		1 "$cached_value"
	assertEquals "The spawn-support probe should fall back to the wrapper path when setsid is unavailable." \
		"0" "$output"
}

test_init_background_job_spawn_support_detects_working_setsid() {
	if ! command -v setsid >/dev/null 2>&1; then
		startSkipping
		assertTrue "setsid not available on this host; setsid probe pin skipped." 0
		endSkipping
		return 0
	fi

	g_zxfer_background_job_use_setsid=""
	zxfer_init_background_job_spawn_support

	assertEquals "A working setsid should enable the process-group spawn path." \
		1 "$g_zxfer_background_job_use_setsid"
}

test_init_background_job_spawn_support_parses_probe_output_through_a_mock_setsid() {
	mock_dir=$(mktemp -d "$TEST_TMPDIR/setsidmock.XXXXXX") ||
		fail "Unable to create the setsid mock directory."

	# Each variant pins one parse branch of the pid==pgid probe.
	output=$(
		for variant in "123 123;1" "123 456;0" "abc def;0" "1 2 3;0" "fail;0"; do
			probe_stdout=${variant%;*}
			expected=${variant#*;}
			if [ "$probe_stdout" = "fail" ]; then
				printf '#!/bin/sh\nexit 1\n' >"$mock_dir/setsid"
			else
				printf '#!/bin/sh\nprintf "%%s\\n" "%s"\n' "$probe_stdout" >"$mock_dir/setsid"
			fi
			chmod +x "$mock_dir/setsid"
			g_zxfer_background_job_use_setsid=""
			PATH="$mock_dir:$PATH" zxfer_init_background_job_spawn_support
			printf 'probe[%s]=%s expected=%s\n' "$probe_stdout" \
				"$g_zxfer_background_job_use_setsid" "$expected"
		done
	)

	assertContains "A pid==pgid probe answer should enable the setsid path." \
		"$output" "probe[123 123]=1 expected=1"
	assertContains "A pid!=pgid probe answer should keep the wrapper fallback." \
		"$output" "probe[123 456]=0 expected=0"
	assertContains "A non-numeric probe answer should keep the wrapper fallback." \
		"$output" "probe[abc def]=0 expected=0"
	assertContains "A probe answer with extra fields should keep the wrapper fallback." \
		"$output" "probe[1 2 3]=0 expected=0"
	assertContains "A failing probe should keep the wrapper fallback." \
		"$output" "probe[fail]=0 expected=0"
}

test_spawn_uses_setsid_spawn_path_when_capability_flag_is_set() {
	mock_dir=$(mktemp -d "$TEST_TMPDIR/setsidspawn.XXXXXX") ||
		fail "Unable to create the setsid spawn mock directory."
	# Pass-through mock: enough to drive the setsid spawn line without
	# requiring a real session leader; teardown is not exercised here.
	printf '#!/bin/sh\nexec "$@"\n' >"$mock_dir/setsid"
	chmod +x "$mock_dir/setsid"

	output=$(
		PATH="$mock_dir:$PATH"
		g_zxfer_background_job_use_setsid=1
		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"printf '%s\n' 'setsid-payload'" \
			"display setsid spawn"
		zxfer_find_background_job_record "$g_zxfer_background_job_last_id"
		printf 'teardown=%s\n' "$g_zxfer_background_job_record_teardown"
		zxfer_wait_for_background_job "$g_zxfer_background_job_last_id"
		printf 'wait_status=%s exit_status=%s\n' "$?" "$g_zxfer_background_job_wait_exit_status"
	)

	assertContains "The setsid spawn path should record process-group teardown." \
		"$output" "teardown=process_group"
	assertContains "The setsid spawn path should complete and report the job status." \
		"$output" "wait_status=0 exit_status=0"
}

test_spawn_reports_output_and_error_file_quoting_failures() {
	zxfer_test_capture_subshell '
		zxfer_build_shell_command_from_argv() {
			case "$1" in
			/tmp/quote-fail-out)
				return 1
				;;
			esac
			printf "%s" "$1"
		}
		zxfer_spawn_supervised_background_job "unit_test" "exit 0" "display" "/tmp/quote-fail-out"
	'
	output_file_status=$ZXFER_TEST_CAPTURE_STATUS
	output_file_output=$ZXFER_TEST_CAPTURE_OUTPUT

	zxfer_test_capture_subshell '
		zxfer_build_shell_command_from_argv() {
			case "$1" in
			/tmp/quote-fail-err)
				return 1
				;;
			esac
			printf "%s" "$1"
		}
		zxfer_spawn_supervised_background_job "unit_test" "exit 0" "display" "/tmp/out" "/tmp/quote-fail-err"
	'

	assertEquals "Spawn should fail when the output file path cannot be quoted." \
		1 "$output_file_status"
	assertContains "Spawn should report the output-file quoting failure." \
		"$output_file_output" "output file path"
	assertEquals "Spawn should fail when the error file path cannot be quoted." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Spawn should report the error-file quoting failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "error file path"
}

test_read_background_job_status_file_preserves_readback_failures() {
	status_file="$TEST_TMPDIR/readback_fail.status"
	printf 'status\t0\n' >"$status_file"

	output=$(
		zxfer_read_runtime_artifact_file() {
			return 9
		}
		zxfer_read_background_job_status_file "$status_file"
		printf 'status=%s\n' "$?"
	)

	assertContains "Status-file reads should preserve runtime readback failure statuses." \
		"$output" "status=9"
}

test_signal_scope_process_group_path_ignores_missing_groups() {
	zxfer_signal_background_job_scope 99999 process_group TERM
	group_status=$?
	g_zxfer_background_job_abort_grace_seconds="bad"
	zxfer_background_job_abort_grace_wait
	grace_status=$?
	g_zxfer_background_job_abort_grace_seconds=1

	assertEquals "Signalling a vanished process group should be a no-op success." \
		0 "$group_status"
	assertEquals "A malformed grace window should fall back to the default wait." \
		0 "$grace_status"
}

test_spawn_and_wait_round_trips_success_status_and_output_capture() {
	outfile="$TEST_TMPDIR/bg_spawn_success.out"
	errfile="$TEST_TMPDIR/bg_spawn_success.err"

	zxfer_spawn_supervised_background_job \
		"unit_test" \
		"printf '%s\n' 'payload'" \
		"display payload" \
		"$outfile" \
		"$errfile"
	job_id=$g_zxfer_background_job_last_id
	status_file=$g_zxfer_background_job_last_status_file

	zxfer_wait_for_background_job "$job_id"
	wait_status=$?

	assertEquals "Waiting on a clean background job should succeed." 0 "$wait_status"
	assertEquals "Background waits should preserve the job exit status." \
		0 "${g_zxfer_background_job_wait_exit_status:-}"
	assertEquals "Background waits should not mark a report failure when the status write succeeds." \
		"" "${g_zxfer_background_job_wait_report_failure:-}"
	assertEquals "Background jobs should write the requested stdout capture." \
		"payload" "$(tr -d '\n' <"$outfile")"
	assertEquals "Background jobs should leave the stderr capture empty when the job is quiet." \
		0 "$(wc -c <"$errfile" | tr -d '[:space:]')"
	assertEquals "Waiting on a background job should clear its registry row." \
		"" "${g_zxfer_background_job_records:-}"
	assertFalse "Waiting on a background job should remove its status file." \
		"[ -e \"$status_file\" ]"
}

test_spawn_and_wait_propagate_nonzero_job_exit_status() {
	zxfer_spawn_supervised_background_job \
		"unit_test" \
		"exit 7" \
		"display exit 7"
	job_id=$g_zxfer_background_job_last_id

	zxfer_wait_for_background_job "$job_id"
	wait_status=$?

	assertEquals "Waiting on a failed background job should still succeed as a wait." \
		0 "$wait_status"
	assertEquals "Background waits should propagate the recorded non-zero exit status." \
		7 "${g_zxfer_background_job_wait_exit_status:-}"
	assertEquals "A failed job that records its status is not a report failure." \
		"" "${g_zxfer_background_job_wait_report_failure:-}"
}

test_wait_reports_missing_status_file_as_completion_write_failure() {
	sh -c 'exit 3' &
	job_pid=$!
	zxfer_register_background_job_record "job-missing-status" "unit_test" "$job_pid" wrapper "$TEST_TMPDIR/never_written.status"

	zxfer_wait_for_background_job "job-missing-status"
	wait_status=$?

	assertEquals "A missing status file should still produce a checked wait result." \
		0 "$wait_status"
	assertEquals "A missing status file should preserve the waited job-shell status." \
		3 "${g_zxfer_background_job_wait_exit_status:-}"
	assertEquals "A missing status file means the job shell died before its status write." \
		"completion_write" "${g_zxfer_background_job_wait_report_failure:-}"
	assertEquals "Waiting should clear the registry row even on abnormal death." \
		"" "${g_zxfer_background_job_records:-}"
}

test_wait_fails_closed_on_non_numeric_status_file() {
	status_file="$TEST_TMPDIR/bad_status.status"
	printf 'status\tbad\n' >"$status_file"
	sh -c 'exit 0' &
	job_pid=$!
	zxfer_register_background_job_record "job-bad-status" "unit_test" "$job_pid" wrapper "$status_file"

	zxfer_wait_for_background_job "job-bad-status"
	wait_status=$?

	assertEquals "A non-numeric recorded status should fail the wait closed." \
		1 "$wait_status"
	assertEquals "Failing closed should still clear the registry row." \
		"" "${g_zxfer_background_job_records:-}"
	assertFalse "Failing closed should still remove the malformed status file." \
		"[ -e \"$status_file\" ]"
}

test_wait_fails_closed_on_unknown_report_failure_marker() {
	status_file="$TEST_TMPDIR/bad_marker.status"
	printf 'status\t0\nreport_failure\tbad_marker\n' >"$status_file"
	sh -c 'exit 0' &
	job_pid=$!
	zxfer_register_background_job_record "job-bad-marker" "unit_test" "$job_pid" wrapper "$status_file"

	zxfer_wait_for_background_job "job-bad-marker"
	wait_status=$?

	assertEquals "An unknown report-failure marker should fail the wait closed." \
		1 "$wait_status"
}

test_wait_preserves_queue_write_report_failure_marker() {
	status_file="$TEST_TMPDIR/queue_write.status"
	printf 'status\t0\nreport_failure\tqueue_write\n' >"$status_file"
	sh -c 'exit 125' &
	job_pid=$!
	zxfer_register_background_job_record "job-queue-write" "unit_test" "$job_pid" wrapper "$status_file"

	zxfer_wait_for_background_job "job-queue-write"
	wait_status=$?

	assertEquals "A queue-write marker should still produce a checked wait result." \
		0 "$wait_status"
	assertEquals "A queue-write marker should preserve the recorded exit status." \
		0 "${g_zxfer_background_job_wait_exit_status:-}"
	assertEquals "A queue-write marker should surface through the wait report-failure scratch." \
		"queue_write" "${g_zxfer_background_job_wait_report_failure:-}"
}

test_wait_for_unknown_job_fails() {
	zxfer_wait_for_background_job "job-not-registered"
	assertEquals "Waiting on an unknown job id should fail." 1 "$?"
}

test_get_background_job_completion_status_pins_missing_and_malformed_files() {
	missing_file="$TEST_TMPDIR/completion_missing.status"
	rm -f "$missing_file"

	zxfer_get_background_job_completion_status "$missing_file" 125
	missing_125_status=$?
	missing_125_exit=$g_zxfer_background_job_completion_exit_status
	missing_125_marker=$g_zxfer_background_job_completion_report_failure

	zxfer_get_background_job_completion_status "$missing_file" 0
	missing_zero_status=$?
	missing_zero_exit=$g_zxfer_background_job_completion_exit_status
	missing_zero_marker=$g_zxfer_background_job_completion_report_failure

	explicit_file="$TEST_TMPDIR/completion_exit_125.status"
	printf 'status\t125\nreport_failure\t\n' >"$explicit_file"
	zxfer_get_background_job_completion_status "$explicit_file" 125
	explicit_status=$?
	explicit_exit=$g_zxfer_background_job_completion_exit_status
	explicit_marker=$g_zxfer_background_job_completion_report_failure

	no_status_file="$TEST_TMPDIR/completion_no_status.status"
	printf 'report_failure\t\n' >"$no_status_file"
	zxfer_get_background_job_completion_status "$no_status_file" 0
	no_status_status=$?

	duplicate_file="$TEST_TMPDIR/completion_duplicate.status"
	printf 'status\t0\nstatus\t1\n' >"$duplicate_file"
	zxfer_get_background_job_completion_status "$duplicate_file" 0
	duplicate_status=$?

	assertEquals "Missing status files should be reported as checked completion-write failures." \
		0 "$missing_125_status"
	assertEquals "Missing status files should preserve the waited 125 status." \
		125 "$missing_125_exit"
	assertEquals "Missing status files should carry the completion-write marker." \
		"completion_write" "$missing_125_marker"
	assertEquals "Missing status files after a clean wait should still be checked results." \
		0 "$missing_zero_status"
	assertEquals "Missing status files after a clean wait should preserve the waited status." \
		0 "$missing_zero_exit"
	assertEquals "Missing status files after a clean wait should still carry the marker." \
		"completion_write" "$missing_zero_marker"
	assertEquals "An explicit recorded 125 should remain a readable completion." \
		0 "$explicit_status"
	assertEquals "An explicit recorded 125 should be preserved." 125 "$explicit_exit"
	assertEquals "An explicit recorded 125 should not gain a completion-write marker." \
		"" "$explicit_marker"
	assertEquals "Status files without the required status row should fail closed." \
		1 "$no_status_status"
	assertEquals "Status files with duplicate status rows should fail closed." \
		1 "$duplicate_status"
}

test_parse_background_job_queue_record_handles_completion_write_failures() {
	zxfer_parse_background_job_queue_record "completion_write_failed	job-9	125"

	assertEquals "Queue parsing should preserve completion-write failure record types." \
		"completion_write_failed" "$g_zxfer_background_job_queue_record_type"
	assertEquals "Queue parsing should preserve the completed job id." \
		"job-9" "$g_zxfer_background_job_queue_record_job_id"
	assertEquals "Queue parsing should preserve the queued failure status." \
		125 "$g_zxfer_background_job_queue_record_status"
}

test_parse_background_job_queue_record_handles_plain_completion_notifications() {
	zxfer_parse_background_job_queue_record "job-7"

	assertEquals "Plain queue notifications should normalize to completion records." \
		"completion" "$g_zxfer_background_job_queue_record_type"
	assertEquals "Plain queue notifications should preserve the completed job id." \
		"job-7" "$g_zxfer_background_job_queue_record_job_id"
	assertEquals "Plain queue notifications should leave the status scratch empty." \
		"" "$g_zxfer_background_job_queue_record_status"
}

test_fifo_notification_publishes_job_id_after_status_file_write() {
	output=$(
		fifo_dir=$(mktemp -d "$TEST_TMPDIR/fifo.XXXXXX") || exit 1
		mkfifo "$fifo_dir/queue" || exit 1
		# Same POSIX open ordering the send/receive queue uses: a short-lived
		# reader lets the writer fd open first, then the real reader opens
		# while that writer is held (hardened for FreeBSD/illumos 2026.05.19).
		(: <"$fifo_dir/queue") &
		open_helper_pid=$!
		open_background_job_test_fifo_writer_fd9 "$fifo_dir/queue" || exit 1
		wait "$open_helper_pid"
		open_background_job_test_fifo_reader_fd8 "$fifo_dir/queue" || exit 1

		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"printf '%s\n' 'queued-payload'" \
			"display queued" \
			"" \
			"" \
			9
		job_id=$g_zxfer_background_job_last_id
		status_file=$g_zxfer_background_job_last_status_file

		IFS= read -r completed_record <&8
		printf 'record=%s\n' "$completed_record"
		# Ordering pin: by the time the queue record is readable the status
		# file must already carry the recorded exit status.
		if grep -q '^status	0$' "$status_file" 2>/dev/null; then
			printf 'status_written_before_notify=yes\n'
		else
			printf 'status_written_before_notify=no\n'
		fi
		zxfer_parse_background_job_queue_record "$completed_record"
		printf 'record_type=%s\n' "$g_zxfer_background_job_queue_record_type"
		zxfer_wait_for_background_job "$g_zxfer_background_job_queue_record_job_id"
		printf 'wait_status=%s\n' "$?"
		printf 'exit_status=%s\n' "$g_zxfer_background_job_wait_exit_status"
		printf 'expected_job=%s actual_job=%s\n' "$job_id" "$g_zxfer_background_job_queue_record_job_id"
		exec 8<&- 2>/dev/null
		exec 9>&- 2>/dev/null
	)

	assertContains "Queue notifications should be plain completion records." \
		"$output" "record_type=completion"
	assertContains "The status file must be written before the queue notification." \
		"$output" "status_written_before_notify=yes"
	assertContains "Waiting on the notified job should succeed." \
		"$output" "wait_status=0"
	assertContains "Waiting on the notified job should read the recorded status." \
		"$output" "exit_status=0"
	assertContains "The notified job id should match the spawned job id." \
		"$output" "record=bgjob.$$."
}

test_fifo_notification_reports_status_write_failures_as_completion_write_failed_records() {
	output=$(
		fifo_dir=$(mktemp -d "$TEST_TMPDIR/fifofail.XXXXXX") || exit 1
		mkfifo "$fifo_dir/queue" || exit 1
		(: <"$fifo_dir/queue") &
		open_helper_pid=$!
		open_background_job_test_fifo_writer_fd9 "$fifo_dir/queue" || exit 1
		wait "$open_helper_pid"
		open_background_job_test_fifo_reader_fd8 "$fifo_dir/queue" || exit 1

		# Point the allocated status file into a read-only directory so the
		# job shell's status write fails and the failure record is queued.
		readonly_dir=$(mktemp -d "$TEST_TMPDIR/rofail.XXXXXX") || exit 1
		: >"$readonly_dir/status"
		chmod 400 "$readonly_dir/status"
		chmod 500 "$readonly_dir"
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result="$readonly_dir/status"
			printf '%s\n' "$g_zxfer_temp_file_result"
		}

		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"exit 4" \
			"display status write failure" \
			"" \
			"" \
			9 2>/dev/null
		job_id=$g_zxfer_background_job_last_id

		IFS= read -r completed_record <&8
		zxfer_parse_background_job_queue_record "$completed_record"
		printf 'record_type=%s\n' "$g_zxfer_background_job_queue_record_type"
		printf 'record_status=%s\n' "$g_zxfer_background_job_queue_record_status"
		printf 'expected_job=%s actual_job=%s\n' "$job_id" "$g_zxfer_background_job_queue_record_job_id"
		wait "$g_zxfer_background_job_last_runner_pid" 2>/dev/null
		printf 'job_shell_status=%s\n' "$?"
		exec 8<&- 2>/dev/null
		exec 9>&- 2>/dev/null
		chmod 700 "$readonly_dir" 2>/dev/null
	)

	assertContains "Status-write failures should publish completion_write_failed queue records." \
		"$output" "record_type=completion_write_failed"
	assertContains "Status-write failure records should carry the captured pipeline status." \
		"$output" "record_status=4"
	assertContains "Status-write failures should exit the job shell with 125." \
		"$output" "job_shell_status=125"
}

test_abort_kills_whole_pipeline_on_wrapper_fallback_path() {
	output=$(
		pid_file_one="$TEST_TMPDIR/abort_wrapper_one.pid"
		pid_file_two="$TEST_TMPDIR/abort_wrapper_two.pid"
		rm -f "$pid_file_one" "$pid_file_two"

		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"sh -c 'echo \$\$ > $pid_file_one; exec sleep 300' | sh -c 'echo \$\$ > $pid_file_two; exec sleep 300'" \
			"display abort pipeline"
		job_id=$g_zxfer_background_job_last_id
		status_file=$g_zxfer_background_job_last_status_file

		wait_for_nonempty_file "$pid_file_one" || printf 'setup=stage-one-missing\n'
		wait_for_nonempty_file "$pid_file_two" || printf 'setup=stage-two-missing\n'
		stage_one_pid=$(cat "$pid_file_one" 2>/dev/null)
		stage_two_pid=$(cat "$pid_file_two" 2>/dev/null)

		zxfer_abort_background_job "$job_id" TERM
		printf 'abort_status=%s\n' "$?"
		if kill -s 0 "$stage_one_pid" 2>/dev/null; then
			printf 'stage_one=alive\n'
		else
			printf 'stage_one=dead\n'
		fi
		if kill -s 0 "$stage_two_pid" 2>/dev/null; then
			printf 'stage_two=alive\n'
		else
			printf 'stage_two=dead\n'
		fi
		printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
		if [ -e "$status_file" ]; then
			printf 'status_file=present\n'
		else
			printf 'status_file=removed\n'
		fi
	)

	assertContains "Aborting a tracked job should succeed." "$output" "abort_status=0"
	assertContains "Aborting through the wrapper should terminate the first pipeline stage." \
		"$output" "stage_one=dead"
	assertContains "Aborting through the wrapper should terminate the second pipeline stage." \
		"$output" "stage_two=dead"
	assertContains "Aborting should clear the registry row." "$output" "records=<>"
	assertContains "Aborting should remove the job status file." "$output" "status_file=removed"
	assertNotContains "The pipeline stages must have started before the abort." \
		"$output" "setup="
}

test_abort_kills_whole_pipeline_process_group_on_setsid_path() {
	if ! command -v setsid >/dev/null 2>&1; then
		startSkipping
		assertTrue "setsid not available on this host; process-group abort pin skipped." 0
		endSkipping
		return 0
	fi
	g_zxfer_background_job_use_setsid=""
	zxfer_init_background_job_spawn_support
	if [ "$g_zxfer_background_job_use_setsid" != "1" ]; then
		startSkipping
		assertTrue "setsid present but pid!=pgid probe failed; process-group abort pin skipped." 0
		endSkipping
		return 0
	fi

	output=$(
		pid_file_one="$TEST_TMPDIR/abort_setsid_one.pid"
		pid_file_two="$TEST_TMPDIR/abort_setsid_two.pid"
		rm -f "$pid_file_one" "$pid_file_two"

		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"sh -c 'echo \$\$ > $pid_file_one; exec sleep 300' | sh -c 'echo \$\$ > $pid_file_two; exec sleep 300'" \
			"display abort process group"
		job_id=$g_zxfer_background_job_last_id
		job_pid=$g_zxfer_background_job_last_runner_pid

		wait_for_nonempty_file "$pid_file_one" || printf 'setup=stage-one-missing\n'
		wait_for_nonempty_file "$pid_file_two" || printf 'setup=stage-two-missing\n'
		stage_one_pid=$(cat "$pid_file_one" 2>/dev/null)
		stage_two_pid=$(cat "$pid_file_two" 2>/dev/null)
		stage_one_pgid=$(ps -o pgid= -p "$stage_one_pid" 2>/dev/null | tr -d '[:space:]')
		printf 'stage_one_leads_job_group=%s\n' "$([ "$stage_one_pgid" = "$job_pid" ] && echo yes || echo no)"

		zxfer_abort_background_job "$job_id" TERM
		printf 'abort_status=%s\n' "$?"
		if kill -s 0 "$stage_one_pid" 2>/dev/null; then
			printf 'stage_one=alive\n'
		else
			printf 'stage_one=dead\n'
		fi
		if kill -s 0 "$stage_two_pid" 2>/dev/null; then
			printf 'stage_two=alive\n'
		else
			printf 'stage_two=dead\n'
		fi
		printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
	)

	assertContains "The setsid job shell must lead the pipeline's process group." \
		"$output" "stage_one_leads_job_group=yes"
	assertContains "Aborting a tracked job should succeed." "$output" "abort_status=0"
	assertContains "The process-group abort should terminate the first pipeline stage." \
		"$output" "stage_one=dead"
	assertContains "The process-group abort should terminate the second pipeline stage." \
		"$output" "stage_two=dead"
	assertContains "Aborting should clear the registry row." "$output" "records=<>"
	assertNotContains "The pipeline stages must have started before the abort." \
		"$output" "setup="
}

test_abort_unknown_job_returns_success() {
	zxfer_abort_background_job "job-unknown" TERM
	status=$?

	assertEquals "Aborting an unknown job id should be a no-op success." 0 "$status"
	assertEquals "Aborting an unknown job id should leave no failure message." \
		"" "${g_zxfer_background_job_abort_failure_message:-}"
}

test_abort_completed_job_reaps_and_cleans_up() {
	output=$(
		g_zxfer_background_job_abort_grace_seconds=0
		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"exit 0" \
			"display completed"
		job_id=$g_zxfer_background_job_last_id
		status_file=$g_zxfer_background_job_last_status_file
		# Let the job finish before aborting it.
		wait_for_nonempty_file "$status_file" || printf 'setup=status-missing\n'

		zxfer_abort_background_job "$job_id" TERM
		printf 'abort_status=%s\n' "$?"
		printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
		if [ -e "$status_file" ]; then
			printf 'status_file=present\n'
		else
			printf 'status_file=removed\n'
		fi
	)

	assertContains "Aborting a job that already completed should succeed." \
		"$output" "abort_status=0"
	assertContains "Aborting a completed job should clear the registry row." \
		"$output" "records=<>"
	assertContains "Aborting a completed job should remove the status file." \
		"$output" "status_file=removed"
	assertNotContains "The completed job must have written its status before the abort." \
		"$output" "setup="
}

test_abort_all_background_jobs_terminates_every_tracked_job() {
	output=$(
		pid_file_one="$TEST_TMPDIR/abort_all_one.pid"
		pid_file_two="$TEST_TMPDIR/abort_all_two.pid"
		rm -f "$pid_file_one" "$pid_file_two"

		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"sh -c 'echo \$\$ > $pid_file_one; exec sleep 300'" \
			"display abort-all one"
		first_status_file=$g_zxfer_background_job_last_status_file
		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"sh -c 'echo \$\$ > $pid_file_two; exec sleep 300'" \
			"display abort-all two"
		second_status_file=$g_zxfer_background_job_last_status_file

		wait_for_nonempty_file "$pid_file_one" || printf 'setup=job-one-missing\n'
		wait_for_nonempty_file "$pid_file_two" || printf 'setup=job-two-missing\n'
		job_one_pid=$(cat "$pid_file_one" 2>/dev/null)
		job_two_pid=$(cat "$pid_file_two" 2>/dev/null)

		zxfer_abort_all_background_jobs
		printf 'abort_all_status=%s\n' "$?"
		if kill -s 0 "$job_one_pid" 2>/dev/null; then
			printf 'job_one=alive\n'
		else
			printf 'job_one=dead\n'
		fi
		if kill -s 0 "$job_two_pid" 2>/dev/null; then
			printf 'job_two=alive\n'
		else
			printf 'job_two=dead\n'
		fi
		printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
		if [ -e "$first_status_file" ] || [ -e "$second_status_file" ]; then
			printf 'status_files=present\n'
		else
			printf 'status_files=removed\n'
		fi
	)

	assertContains "Aborting all jobs should succeed." "$output" "abort_all_status=0"
	assertContains "Trap-style abort-all should terminate the first tracked job." \
		"$output" "job_one=dead"
	assertContains "Trap-style abort-all should terminate the second tracked job." \
		"$output" "job_two=dead"
	assertContains "Trap-style abort-all should clear the registry." "$output" "records=<>"
	assertContains "Trap-style abort-all should remove every status file." \
		"$output" "status_files=removed"
	assertNotContains "Both jobs must have started before the abort." "$output" "setup="
}

test_abort_all_background_jobs_without_tracked_jobs_is_a_noop() {
	zxfer_abort_all_background_jobs
	assertEquals "Aborting with an empty registry should succeed." 0 "$?"
}

test_term_mid_run_teardown_through_wrapper_trap_kills_descendants() {
	# TERM delivered mid-run to the job shell's wrapper (the trap path the
	# launcher's own TERM handling relies on) must reap the job's children.
	output=$(
		pid_file="$TEST_TMPDIR/term_mid_run.pid"
		rm -f "$pid_file"

		zxfer_spawn_supervised_background_job \
			"unit_test" \
			"sh -c 'echo \$\$ > $pid_file; exec sleep 300'" \
			"display term mid-run"
		job_pid=$g_zxfer_background_job_last_runner_pid
		job_id=$g_zxfer_background_job_last_id
		status_file=$g_zxfer_background_job_last_status_file

		wait_for_nonempty_file "$pid_file" || printf 'setup=worker-missing\n'
		worker_pid=$(cat "$pid_file" 2>/dev/null)

		kill -s TERM "$job_pid" 2>/dev/null
		wait "$job_pid" 2>/dev/null
		printf 'job_shell_status=%s\n' "$?"
		tries=0
		while [ "$tries" -lt 100 ] && kill -s 0 "$worker_pid" 2>/dev/null; do
			sleep 0.1 2>/dev/null || sleep 1
			tries=$((tries + 1))
		done
		if kill -s 0 "$worker_pid" 2>/dev/null; then
			printf 'worker=alive\n'
		else
			printf 'worker=dead\n'
		fi
		zxfer_unregister_background_job_record "$job_id"
		zxfer_cleanup_runtime_artifact_path "$status_file" >/dev/null 2>&1
	)

	assertContains "TERM to the wrapper should report the signal exit." \
		"$output" "job_shell_status=143"
	assertContains "TERM mid-run must reap the job's descendants through the wrapper trap." \
		"$output" "worker=dead"
	assertNotContains "The worker must have started before the TERM." "$output" "setup="
}

test_spawn_reports_wrapper_lookup_failures() {
	zxfer_test_capture_subshell '
		g_zxfer_background_job_use_setsid=0
		zxfer_get_cleanup_child_wrapper_script_path() {
			return 1
		}
		zxfer_spawn_supervised_background_job "unit_test" "exit 0" "display"
	'

	assertEquals "Spawn should fail when the fallback wrapper cannot be resolved." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Spawn should report the wrapper lookup failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to locate the background job cleanup wrapper."
}

test_spawn_cleans_up_job_and_status_file_when_registration_fails() {
	status_file_record="$TEST_TMPDIR/spawn_register_fail.statuspath"
	rm -f "$status_file_record"
	zxfer_test_capture_subshell '
		g_zxfer_background_job_abort_grace_seconds=0
		zxfer_register_background_job_record() {
			printf "%s\n" "$5" >"'"$status_file_record"'"
			return 1
		}
		zxfer_spawn_supervised_background_job "unit_test" "sleep 300" "display"
	'
	leaked_status_file=$(cat "$status_file_record" 2>/dev/null)

	assertEquals "Spawn should fail when job registration fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Spawn should report the registration failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to register background job"
	assertNotNull "The registration stub should have observed a status file path." \
		"$leaked_status_file"
	if [ -n "$leaked_status_file" ]; then
		assertFalse "Spawn should remove the status file when registration fails." \
			"[ -e \"$leaked_status_file\" ]"
	fi
}

test_spawn_reports_status_file_quoting_failures() {
	zxfer_test_capture_subshell '
		zxfer_build_shell_command_from_argv() {
			return 1
		}
		zxfer_spawn_supervised_background_job "unit_test" "exit 0" "display"
	'

	assertEquals "Spawn should fail when the status file path cannot be quoted." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Spawn should report the quoting failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status file path"
}

test_signal_scope_and_grace_helpers_reject_invalid_inputs() {
	zxfer_signal_background_job_scope "" process_group TERM
	empty_status=$?
	zxfer_signal_background_job_scope "bad" wrapper TERM
	bad_status=$?
	g_zxfer_background_job_abort_grace_seconds=0
	zxfer_background_job_abort_grace_wait
	grace_zero_status=$?
	g_zxfer_background_job_abort_grace_seconds=1

	assertEquals "Signalling an empty pid should be a no-op success." 0 "$empty_status"
	assertEquals "Signalling a non-numeric pid should be a no-op success." 0 "$bad_status"
	assertEquals "A zero grace window should return immediately." 0 "$grace_zero_status"
}

test_reset_background_job_state_preserves_cached_spawn_capability() {
	g_zxfer_background_job_use_setsid=1
	g_zxfer_background_job_abort_grace_seconds=0
	zxfer_reset_background_job_state

	assertEquals "Resets should preserve the cached setsid capability flag." \
		1 "$g_zxfer_background_job_use_setsid"
	assertEquals "Resets should preserve the configured abort grace window." \
		0 "$g_zxfer_background_job_abort_grace_seconds"
	assertEquals "Resets should clear the registry." "" "$g_zxfer_background_job_records"
	assertEquals "Resets should clear the wait scratch." "" "$g_zxfer_background_job_wait_exit_status"
	g_zxfer_background_job_abort_grace_seconds=1
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
