#!/bin/sh
#
# shunit2 tests for zxfer_background_jobs.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
	g_cmd_ps=${g_cmd_ps:-$(command -v ps 2>/dev/null || printf '%s\n' ps)}
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_zxfer_temp_prefix="zxfer.bgtest.$$"
	g_option_Y_yield_iterations=1
	zxfer_reset_runtime_artifact_state
	zxfer_reset_background_job_state
	zxfer_reset_failure_context "unit"
}

test_background_job_record_helpers_track_and_remove_jobs() {
	zxfer_register_background_job_record "job-1" "send_receive" 101 "$TEST_TMPDIR/job-1" "/tmp/runner" "token-1" "start-1"
	zxfer_register_background_job_record "job-2" "source_snapshot_list" 202 "$TEST_TMPDIR/job-2" "/tmp/runner" "token-2" "start-2"
	zxfer_find_background_job_record "job-2"
	find_status=$?
	zxfer_unregister_background_job_record "job-1"

	assertEquals "Tracked background jobs should be discoverable by job id." \
		0 "$find_status"
	assertEquals "Tracked background jobs should preserve the recorded kind." \
		"source_snapshot_list" "$g_zxfer_background_job_record_kind"
	assertEquals "Tracked background jobs should preserve the recorded runner pid." \
		"202" "$g_zxfer_background_job_record_runner_pid"
	assertEquals "Tracked background jobs should preserve the recorded runner start token." \
		"start-2" "$g_zxfer_background_job_record_runner_start_token"
	assertNotContains "Unregistering one background job should leave later records intact." \
		"$g_zxfer_background_job_records" "job-1"
	assertContains "Unregistering one background job should preserve unrelated records." \
		"$g_zxfer_background_job_records" "job-2"
}

test_spawn_and_wait_for_background_job_round_trips_metadata_and_output() {
	outfile="$TEST_TMPDIR/background_job_spawn.out"
	errfile="$TEST_TMPDIR/background_job_spawn.err"

	zxfer_spawn_supervised_background_job \
		"unit_test" \
		"printf '%s\n' 'payload'" \
		"display payload" \
		"$outfile" \
		"$errfile"
	job_id=$g_zxfer_background_job_last_id
	control_dir=$g_zxfer_background_job_last_control_dir

	zxfer_wait_for_background_job "$job_id"
	wait_status=$?

	assertEquals "Supervised background jobs should wait successfully when the worker exits cleanly." \
		0 "$wait_status"
	assertEquals "Supervised background waits should preserve the worker exit status." \
		0 "${g_zxfer_background_job_wait_exit_status:-}"
	assertEquals "Supervised background waits should not mark a completion-report failure when metadata writes succeed." \
		"" "${g_zxfer_background_job_wait_report_failure:-}"
	assertEquals "Supervised background jobs should write the requested stdout capture." \
		"payload" "$(tr -d '\n' <"$outfile")"
	assertEquals "Supervised background jobs should leave the stderr capture empty when the worker is quiet." \
		0 "$(wc -c <"$errfile" | tr -d '[:space:]')"
	assertEquals "Waiting for a supervised background job should clear its registry entry." \
		"" "${g_zxfer_background_job_records:-}"
	assertFalse "Waiting for a supervised background job should remove its private control directory." \
		"[ -d \"$control_dir\" ]"
}

test_cleanup_completed_background_job_waits_for_runner_before_removing_control_dir() {
	control_dir="$TEST_TMPDIR/background_job_completed_cleanup_waits"
	marker_file="$TEST_TMPDIR/background_job_completed_cleanup_waits.marker"
	mkdir -p "$control_dir"

	output=$(
		(
			MARKER_PATH=$marker_file \
				sh -c 'sleep 1; printf "%s\n" "done" >"$MARKER_PATH"' &
			runner_pid=$!
			zxfer_register_background_job_record "job-cleanup-completed" "send_receive" "$runner_pid" "$control_dir" "/tmp/runner" "token-cleanup-completed" "start-cleanup-completed"
			zxfer_cleanup_completed_background_job \
				"job-cleanup-completed" \
				"$runner_pid" \
				"$control_dir"
			printf 'status=%s\n' "$?"
			if [ -f "$marker_file" ]; then
				l_marker_exists=yes
			else
				l_marker_exists=no
			fi
			if [ -d "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'marker_exists=%s\n' "$l_marker_exists"
			printf 'dir_exists=%s\n' "$l_dir_exists"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
		)
	)

	assertContains "Completed-job cleanup should succeed when the tracked runner is still live." \
		"$output" "status=0"
	assertContains "Completed-job cleanup should wait for the runner to exit before returning." \
		"$output" "marker_exists=yes"
	assertContains "Completed-job cleanup should remove the private control directory after the runner exits." \
		"$output" "dir_exists=no"
	assertContains "Completed-job cleanup should unregister the tracked job record." \
		"$output" "records=<>"
}

test_get_background_job_completion_status_marks_missing_completion_after_125_as_completion_write() {
	control_dir="$TEST_TMPDIR/background_job_completion_missing"
	mkdir -p "$control_dir"

	zxfer_get_background_job_completion_status "$control_dir" 125
	status=$?

	assertEquals "Missing completion metadata should still be readable as a checked completion-write failure when the runner exited 125." \
		0 "$status"
	assertEquals "Missing completion metadata after a 125 exit should preserve the waited status." \
		125 "$g_zxfer_background_job_completion_exit_status"
	assertEquals "Missing completion metadata after a 125 exit should report a completion-write failure marker." \
		"completion_write" "$g_zxfer_background_job_completion_report_failure"
}

test_get_background_job_completion_status_marks_missing_completion_after_zero_exit_as_completion_write() {
	control_dir="$TEST_TMPDIR/background_job_completion_missing_zero"
	mkdir -p "$control_dir"

	zxfer_get_background_job_completion_status "$control_dir" 0
	status=$?

	assertEquals "Missing completion metadata should still be readable as a checked completion-write failure when the runner exited cleanly." \
		0 "$status"
	assertEquals "Missing completion metadata after a zero exit should preserve the waited status." \
		0 "$g_zxfer_background_job_completion_exit_status"
	assertEquals "Missing completion metadata after a zero exit should still report a completion-write failure marker." \
		"completion_write" "$g_zxfer_background_job_completion_report_failure"
}

test_get_background_job_completion_status_preserves_explicit_worker_exit_125_without_completion_write_marker() {
	control_dir="$TEST_TMPDIR/background_job_completion_exit_125"
	mkdir -p "$control_dir"
	cat >"$control_dir/completion.tsv" <<'EOF'
status	125
report_failure	
EOF

	zxfer_get_background_job_completion_status "$control_dir" 125
	status=$?

	assertEquals "A valid completion record should still be readable when the worker exits 125." \
		0 "$status"
	assertEquals "A valid completion record should preserve the recorded 125 worker exit status." \
		125 "$g_zxfer_background_job_completion_exit_status"
	assertEquals "A valid completion record should not be rewritten into a completion-write failure just because the worker exited 125." \
		"" "$g_zxfer_background_job_completion_report_failure"
}

test_get_background_job_completion_status_fails_closed_for_invalid_recorded_statuses() {
	control_dir="$TEST_TMPDIR/background_job_completion_invalid"
	mkdir -p "$control_dir"
	cat >"$control_dir/completion.tsv" <<'EOF'
status	bad
report_failure	
EOF

	zxfer_get_background_job_completion_status "$control_dir" 125
	status=$?

	assertEquals "Malformed completion statuses should fail closed instead of being normalized to the waited status." \
		1 "$status"
}

test_get_background_job_completion_status_fails_closed_when_status_is_missing() {
	control_dir="$TEST_TMPDIR/background_job_completion_missing_status"
	mkdir -p "$control_dir"
	cat >"$control_dir/completion.tsv" <<'EOF'
report_failure	
EOF

	zxfer_get_background_job_completion_status "$control_dir" 0
	status=$?

	assertEquals "Completion metadata without a required status field should fail closed." \
		1 "$status"
}

test_get_background_job_completion_status_fails_closed_for_unknown_failure_markers() {
	control_dir="$TEST_TMPDIR/background_job_completion_invalid_failure"
	mkdir -p "$control_dir"
	cat >"$control_dir/completion.tsv" <<'EOF'
status	7
report_failure	bad_marker
EOF

	zxfer_get_background_job_completion_status "$control_dir" 7
	status=$?

	assertEquals "Completion metadata with an unknown failure marker should fail closed." \
		1 "$status"
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

test_read_background_job_metadata_files_cover_current_shell_paths() {
	control_dir="$TEST_TMPDIR/background_job_metadata_read"
	mkdir -p "$control_dir"
	cat >"$control_dir/launch.tsv" <<'EOF'
version	1
job_id	job-meta
kind	send_receive
runner_pid	101
runner_script	/tmp/runner
runner_token	token-meta
worker_pid	102
worker_pgid	777
teardown_mode	process_group
started_epoch	1234567890
EOF
	cat >"$control_dir/completion.tsv" <<'EOF'
status	9
report_failure	queue_write
EOF

	output=$(
		(
			set +e
			zxfer_read_background_job_launch_file "$control_dir"
			printf 'launch=%s\n' "$?"
			printf 'job=<%s>\n' "$g_zxfer_background_job_launch_job_id"
			printf 'worker_pgid=<%s>\n' "$g_zxfer_background_job_launch_worker_pgid"
			zxfer_read_background_job_completion_file "$control_dir"
			printf 'completion=%s\n' "$?"
			printf 'status=<%s>\n' "$g_zxfer_background_job_completion_exit_status"
			printf 'failure=<%s>\n' "$g_zxfer_background_job_completion_report_failure"
		)
	)

	assertContains "Background-job launch metadata reads should succeed on well-formed launch files." \
		"$output" "launch=0"
	assertContains "Background-job launch metadata reads should recover the queued job id." \
		"$output" "job=<job-meta>"
	assertContains "Background-job launch metadata reads should recover the queued worker process group." \
		"$output" "worker_pgid=<777>"
	assertContains "Background-job completion metadata reads should succeed on well-formed completion files." \
		"$output" "completion=0"
	assertContains "Background-job completion metadata reads should recover the recorded exit status." \
		"$output" "status=<9>"
	assertContains "Background-job completion metadata reads should recover the recorded failure marker." \
		"$output" "failure=<queue_write>"
}

test_zxfer_read_background_job_launch_file_populates_globals_in_current_shell() {
	control_dir="$TEST_TMPDIR/background_job_launch_read_current_shell"
	mkdir -p "$control_dir"
	cat >"$control_dir/launch.tsv" <<'EOF'
version	1
job_id	job-current
kind	source_snapshot_list
runner_pid	501
runner_script	/tmp/current-runner
runner_token	token-current
worker_pid	502
worker_pgid	6501
teardown_mode	process_group
started_epoch	2222222222
EOF

	zxfer_read_background_job_launch_file "$control_dir"
	status=$?

	assertEquals "Direct current-shell launch metadata reads should succeed on well-formed launch files." \
		0 "$status"
	assertEquals "Direct current-shell launch metadata reads should publish the queued job id." \
		"job-current" "$g_zxfer_background_job_launch_job_id"
	assertEquals "Direct current-shell launch metadata reads should publish the runner pid." \
		"501" "$g_zxfer_background_job_launch_runner_pid"
	assertEquals "Direct current-shell launch metadata reads should publish the worker process group." \
		"6501" "$g_zxfer_background_job_launch_worker_pgid"
	assertEquals "Direct current-shell launch metadata reads should publish the teardown mode." \
		"process_group" "$g_zxfer_background_job_launch_teardown_mode"
}

test_zxfer_read_background_job_completion_file_populates_globals_in_current_shell() {
	control_dir="$TEST_TMPDIR/background_job_completion_read_current_shell"
	mkdir -p "$control_dir"
	cat >"$control_dir/completion.tsv" <<'EOF'
status	17
report_failure	completion_write
EOF

	zxfer_read_background_job_completion_file "$control_dir"
	status=$?

	assertEquals "Direct current-shell completion metadata reads should succeed on well-formed completion files." \
		0 "$status"
	assertEquals "Direct current-shell completion metadata reads should publish the recorded exit status." \
		"17" "$g_zxfer_background_job_completion_exit_status"
	assertEquals "Direct current-shell completion metadata reads should publish the recorded failure marker." \
		"completion_write" "$g_zxfer_background_job_completion_report_failure"
}

test_get_background_job_completion_status_fails_closed_when_completion_read_fails() {
	control_dir="$TEST_TMPDIR/background_job_completion_read_failure"
	mkdir -p "$control_dir"
	: >"$control_dir/completion.tsv"

	output=$(
		(
			set +e
			zxfer_read_background_job_completion_file() {
				return 1
			}
			zxfer_get_background_job_completion_status "$control_dir" 7
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Background-job completion status lookups should fail closed when completion metadata cannot be read." \
		"$output" "status=1"
}

test_abort_background_job_signals_validated_process_group_and_runner_without_ps_args_identity_checks() {
	control_dir="$TEST_TMPDIR/background_job_abort_pgid"
	fake_ps="$TEST_TMPDIR/fake_ps_pgid.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "1001 900 1001"
    printf '%s\n' "1002 1001 4321"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-1" \
		"send_receive" \
		1001 \
		"/tmp/runner" \
		"token-1" \
		1002 \
		4321 \
		"process_group" \
		"123"
	zxfer_register_background_job_record "job-1" "send_receive" 1001 "$control_dir" "/tmp/runner" "token-1" "start-1"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-1"
			}
			kill() {
				printf '%s %s\n' "$1" "$2"
				[ "$2" = "1001" ] && printf '%s\n' "status	143" >"$control_dir/completion.tsv"
				return 0
			}
			zxfer_abort_background_job "job-1" TERM
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Validated process-group cleanup should not require full ps args output before it signals the recorded worker process group." \
		"$output" "-TERM -4321"
	assertContains "Validated process-group cleanup should also signal the tracked runner pid." \
		"$output" "-TERM 1001"
	assertContains "Validated process-group cleanup should complete successfully." \
		"$output" "status=0"
}

test_abort_background_job_rejects_reused_worker_pid_for_process_group_cleanup() {
	control_dir="$TEST_TMPDIR/background_job_abort_reused_worker_pid"
	fake_ps="$TEST_TMPDIR/fake_ps_reused_worker_pid.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o args= -p 1101")
    printf '%s\n' "1101 /tmp/runner token-reuse"
    ;;
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "1101 900 1101"
    printf '%s\n' "1102 777 4321"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-reuse" \
		"send_receive" \
		1101 \
		"/tmp/runner" \
		"token-reuse" \
		1102 \
		4321 \
		"process_group" \
		"123"
	zxfer_register_background_job_record "job-reuse" "send_receive" 1101 "$control_dir" "/tmp/runner" "token-reuse" "start-reuse"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-reuse"
			}
			kill() {
				printf '%s %s\n' "$1" "$2"
				[ "$2" = "1101" ] && printf '%s\n' "status	143" >"$control_dir/completion.tsv"
				return 0
			}
			zxfer_abort_background_job "job-reuse" TERM
			printf 'status=%s\n' "$?"
		)
	)

	assertNotContains "Validated background-job cleanup should not trust a recorded process group when the recorded worker pid has been reused outside the tracked runner ancestry." \
		"$output" "-TERM -4321"
	assertContains "Validated background-job cleanup should fall back to the tracked runner child set when the recorded worker pid is no longer a child of the runner." \
		"$output" "-TERM 1101"
	assertContains "Validated background-job cleanup should still complete successfully when it falls back from process-group cleanup to the runner child set." \
		"$output" "status=0"
}

test_abort_background_job_falls_back_to_owned_child_set_when_process_group_is_unusable() {
	control_dir="$TEST_TMPDIR/background_job_abort_child_set"
	fake_ps="$TEST_TMPDIR/fake_ps_child_set.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o args= -p 2001")
    printf '%s\n' "2001 /tmp/runner token-2"
    ;;
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "2001 900 2001"
    printf '%s\n' "2002 2001 2001"
    printf '%s\n' "2003 2002 2001"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-2" \
		"source_snapshot_list" \
		2001 \
		"/tmp/runner" \
		"token-2" \
		2002 \
		"" \
		"child_set" \
		"456"
	zxfer_register_background_job_record "job-2" "source_snapshot_list" 2001 "$control_dir" "/tmp/runner" "token-2" "start-2"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-2"
			}
			kill() {
				printf '%s %s\n' "$1" "$2"
				[ "$2" = "2001" ] && printf '%s\n' "status	143" >"$control_dir/completion.tsv"
				return 0
			}
			zxfer_abort_background_job "job-2" TERM
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Child-set fallback cleanup should signal the tracked runner pid." \
		"$output" "-TERM 2001"
	assertContains "Child-set fallback cleanup should signal direct owned children." \
		"$output" "-TERM 2002"
	assertContains "Child-set fallback cleanup should signal deeper owned descendants too." \
		"$output" "-TERM 2003"
	assertContains "Child-set fallback cleanup should complete successfully." \
		"$output" "status=0"
}

test_abort_background_job_does_not_wait_indefinitely_after_successful_abort_signal() {
	control_dir="$TEST_TMPDIR/background_job_abort_no_wait"
	fake_ps="$TEST_TMPDIR/fake_ps_no_wait.sh"
	state_file="$TEST_TMPDIR/background_job_abort_no_wait.state"
	mkdir -p "$control_dir"
	printf '%s\n' "live" >"$state_file"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    if [ "$(cat "$ZXFER_TEST_ABORT_STATE_FILE")" = "live" ]; then
      printf '%s\n' "2101 900 2101"
      printf '%s\n' "2102 2101 2101"
    fi
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"
	ZXFER_TEST_ABORT_STATE_FILE=$state_file
	export ZXFER_TEST_ABORT_STATE_FILE
	zxfer_register_background_job_record "job-no-wait" "send_receive" 2101 "$control_dir" "/tmp/runner" "token-no-wait" "start-no-wait"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-no-wait"
			}
			kill() {
				printf 'kill:%s %s\n' "$1" "$2"
				[ "$2" = "2101" ] && printf '%s\n' "gone" >"$state_file"
				return 0
			}
			wait() {
				printf 'wait:%s\n' "$1"
				return 0
			}
			zxfer_abort_background_job "job-no-wait" TERM
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)
	unset ZXFER_TEST_ABORT_STATE_FILE

	assertContains "Abort cleanup should signal the validated runner child set." \
		"$output" "kill:-TERM 2101"
	assertContains "Abort cleanup should succeed after revalidating that the runner disappeared." \
		"$output" "status=0"
	assertContains "Abort cleanup should unregister the job after a completed abort signal." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the control directory after a completed abort signal." \
		"$output" "dir_exists=no"
	assertNotContains "Abort cleanup should not call the completed-job wait path when no completion record exists." \
		"$output" "wait:"
}

test_abort_background_job_rejects_pid_reuse_when_runner_identity_changes() {
	control_dir="$TEST_TMPDIR/background_job_abort_pid_reuse"
	fake_ps="$TEST_TMPDIR/fake_ps_pid_reuse.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "3001 900 3001"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-3" \
		"send_receive" \
		3001 \
		"/tmp/runner" \
		"token-3" \
		3002 \
		4321 \
		"process_group" \
		"789"
	zxfer_register_background_job_record "job-3" "send_receive" 3001 "$control_dir" "/tmp/runner" "token-3" "start-3"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-current"
			}
			zxfer_abort_background_job "job-3" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertContains "Abort cleanup should fail closed when the tracked runner pid no longer matches the recorded helper identity." \
		"$output" "status=1"
	assertContains "Abort cleanup should preserve the pid-reuse validation failure message." \
		"$output" "no longer matches the recorded helper identity"
}

test_abort_background_job_rejects_launch_metadata_mismatches() {
	control_dir="$TEST_TMPDIR/background_job_abort_launch_mismatch"
	fake_ps="$TEST_TMPDIR/fake_ps_launch_mismatch.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "3051 900 3051"
    printf '%s\n' "3052 3051 4321"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-launch-mismatch" \
		"send_receive" \
		3051 \
		"/tmp/runner" \
		"token-launch" \
		3052 \
		4321 \
		"process_group" \
		"789"
	zxfer_register_background_job_record "job-launch-mismatch" "send_receive" 3051 "$control_dir" "/tmp/runner" "token-record" "start-launch"

	set +e
	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-launch"
			}
			zxfer_abort_background_job "job-launch-mismatch" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	status=$?
	set -e

	assertEquals "Abort cleanup should preserve launch-metadata mismatch failures in the current shell." \
		0 "$status"
	assertContains "Abort cleanup should fail closed when the runner-published launch metadata no longer matches the tracked record." \
		"$output" "status=1"
	assertContains "Launch-metadata mismatches should preserve the dedicated failure message." \
		"$output" "recorded launch metadata no longer matches the tracked runner identity"
}

test_abort_background_job_reports_live_runner_identity_validation_failures() {
	control_dir="$TEST_TMPDIR/background_job_abort_identity_validation"
	fake_ps="$TEST_TMPDIR/fake_ps_identity_validation.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "3061 900 3061"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-identity-validate" \
		"send_receive" \
		3061 \
		"/tmp/runner" \
		"token-identity" \
		3062 \
		4321 \
		"process_group" \
		"789"
	zxfer_register_background_job_record "job-identity-validate" "send_receive" 3061 "$control_dir" "/tmp/runner" "token-identity" "start-identity"

	set +e
	output=$(
		(
			zxfer_get_process_start_token() {
				return 1
			}
			zxfer_abort_background_job "job-identity-validate" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	status=$?
	set -e

	assertEquals "Abort cleanup should preserve live runner identity validation failures in the current shell." \
		0 "$status"
	assertContains "Abort cleanup should fail closed when the live runner identity cannot be revalidated." \
		"$output" "status=1"
	assertContains "Live runner identity validation failures should preserve the dedicated failure message." \
		"$output" "Failed to validate the live runner identity for background job [job-identity-validate]."
}

test_wait_for_background_job_reports_missing_records_and_completion_read_failures() {
	control_dir="$TEST_TMPDIR/background_job_wait_failure"
	mkdir -p "$control_dir"
	current_start_token=$(zxfer_get_process_start_token "$$") ||
		fail "Unable to derive the current process start token for background job wait coverage."
	zxfer_register_background_job_record "job-4" "send_receive" "$$" "$control_dir" "/tmp/runner" "token-4" "$current_start_token"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	missing_output=$(
		(
			zxfer_wait_for_background_job "job-missing"
			printf 'status=%s\n' "$?"
		)
	)
	missing_status=$?
	wait_failure_output=$(
		(
			wait() {
				return 0
			}
			zxfer_get_background_job_completion_status() {
				return 17
			}
			zxfer_wait_for_background_job "job-4"
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)
	wait_failure_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Waiting for an unknown background job should fail in the current shell." \
		0 "$missing_status"
	assertContains "Waiting for an unknown background job should return a nonzero status." \
		"$missing_output" "status=1"
	assertEquals "Waiting for a tracked job should preserve completion-read failures in the current shell." \
		0 "$wait_failure_status"
	assertContains "Completion-read failures should unregister the tracked job record." \
		"$wait_failure_output" "records=<>"
	assertContains "Completion-read failures should remove the private control directory." \
		"$wait_failure_output" "dir_exists=no"
}

test_spawn_supervised_background_job_reports_runner_lookup_and_tempdir_failures() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	lookup_output=$(
		(
			zxfer_get_background_job_runner_script_path() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_spawn_supervised_background_job "unit" ":" "display"
		)
	)
	lookup_status=$?
	tempdir_output=$(
		(
			zxfer_create_private_temp_dir() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_spawn_supervised_background_job "unit" ":" "display"
		)
	)
	tempdir_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Spawning supervised jobs should fail closed when the runner helper cannot be located." \
		1 "$lookup_status"
	assertContains "Runner lookup failures should preserve the dedicated operator-facing error." \
		"$lookup_output" "Failed to locate the background job runner helper."
	assertEquals "Spawning supervised jobs should fail closed when the private control directory cannot be created." \
		1 "$tempdir_status"
	assertContains "Control-directory setup failures should preserve the existing temp-file error." \
		"$tempdir_output" "Error creating temporary file."
}

test_spawn_supervised_background_job_reports_runner_identity_capture_failures() {
	identity_control_dir="$TEST_TMPDIR/background_job_spawn_failure_identity_control"
	identity_cleanup_log="$TEST_TMPDIR/background_job_spawn_failure_identity_cleanup.log"
	identity_teardown_log="$TEST_TMPDIR/background_job_spawn_failure_identity_teardown.log"
	mkdir -p "$identity_control_dir"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_create_private_temp_dir() {
				printf '%s\n' "$identity_control_dir"
			}
			zxfer_get_process_start_token() {
				return 1
			}
			zxfer_teardown_unregistered_background_runner() {
				printf 'teardown:%s:%s:%s:%s:%s:%s:%s\n' "$1" "$2" "$3" "$4" "$5" "$6" "$7" >>"$identity_teardown_log"
				kill "$2" 2>/dev/null || :
				wait "$2" 2>/dev/null || :
				zxfer_cleanup_runtime_artifact_path "$3"
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'cleanup:%s\n' "$1" >>"$identity_cleanup_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_spawn_supervised_background_job "unit" "sleep 1" "display"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Spawning supervised jobs should fail closed when the live runner identity cannot be captured." \
		1 "$status"
	assertContains "Runner-identity capture failures should use the validated unregistered-runner teardown helper." \
		"$(cat "$identity_teardown_log")" "teardown:"
	assertContains "Runner-identity capture failures should clean the private control directory." \
		"$(cat "$identity_cleanup_log")" "cleanup:$identity_control_dir"
	assertContains "Runner-identity capture failures should preserve the dedicated operator-facing error." \
		"$output" "Failed to validate background job ["
}

test_spawn_supervised_background_job_reports_job_id_allocation_failures() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_next_background_job_id() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_spawn_supervised_background_job "unit" ":" "display"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Spawning supervised jobs should fail closed when a fresh job id cannot be allocated." \
		1 "$status"
	assertContains "Job-id allocation failures should preserve the dedicated operator-facing error." \
		"$output" "Failed to allocate a background job id."
}

test_spawn_supervised_background_job_does_not_use_parent_launch_staging() {
	outfile="$TEST_TMPDIR/background_job_parent_launch_stage.out"
	errfile="$TEST_TMPDIR/background_job_parent_launch_stage.err"

	output=$(
		(
			zxfer_write_background_job_launch_file() {
				fail "Parent-side launch staging should not run during supervised spawn."
			}

			zxfer_spawn_supervised_background_job \
				"unit_test" \
				"printf '%s\n' 'payload'" \
				"display payload" \
				"$outfile" \
				"$errfile"
			job_id=$g_zxfer_background_job_last_id

			zxfer_wait_for_background_job "$job_id"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Supervised spawn should succeed even when the parent-shell launch staging helper is unavailable." \
		"$output" "status=0"
	assertEquals "The runner should still publish the worker output after supervised spawn." \
		"payload" "$(tr -d '\n' <"$outfile")"
}

test_spawn_supervised_background_job_cleans_up_when_registration_fails() {
	register_control_dir="$TEST_TMPDIR/background_job_spawn_failure_register_control"
	register_cleanup_log="$TEST_TMPDIR/background_job_spawn_failure_register_cleanup.log"
	register_teardown_log="$TEST_TMPDIR/background_job_spawn_failure_register_teardown.log"
	mkdir -p "$register_control_dir"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	register_output=$(
		(
			zxfer_create_private_temp_dir() {
				printf '%s\n' "$register_control_dir"
			}
			zxfer_register_background_job_record() {
				return 1
			}
			zxfer_teardown_unregistered_background_runner() {
				printf 'teardown:%s:%s:%s:%s:%s:%s:%s\n' "$1" "$2" "$3" "$4" "$5" "$6" "$7" >>"$register_teardown_log"
				kill "$2" 2>/dev/null || :
				wait "$2" 2>/dev/null || :
				zxfer_cleanup_runtime_artifact_path "$3"
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'cleanup:%s\n' "$1" >>"$register_cleanup_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_spawn_supervised_background_job "unit" "sleep 1" "display"
		)
	)
	register_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Spawn setup should fail closed when registry insertion fails." \
		1 "$register_status"
	assertContains "Registry insertion failures should use the validated unregistered-runner teardown helper." \
		"$(cat "$register_teardown_log")" "teardown:"
	assertContains "Registry insertion failures should clean the private control directory." \
		"$(cat "$register_cleanup_log")" "cleanup:$register_control_dir"
	assertContains "Registry insertion failures should preserve the operator-facing error." \
		"$register_output" "Failed to register background job"
}

test_teardown_unregistered_background_runner_uses_launch_process_group_when_validated() {
	control_dir="$TEST_TMPDIR/background_job_unregistered_teardown_pgid"
	fake_ps="$TEST_TMPDIR/fake_ps_unregistered_teardown_pgid.sh"
	state_file="$TEST_TMPDIR/background_job_unregistered_teardown_pgid.state"
	mkdir -p "$control_dir"
	printf '%s\n' "live" >"$state_file"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    if [ "$(cat "$ZXFER_TEST_UNREGISTERED_STATE_FILE")" = "live" ]; then
      printf '%s\n' "7001 $ZXFER_TEST_PARENT_PID 7001"
      printf '%s\n' "7002 7001 7700"
    fi
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"
	ZXFER_TEST_UNREGISTERED_STATE_FILE=$state_file
	ZXFER_TEST_PARENT_PID=$$
	export ZXFER_TEST_UNREGISTERED_STATE_FILE ZXFER_TEST_PARENT_PID
	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-unregistered-pgid" \
		"send_receive" \
		7001 \
		"/tmp/runner" \
		"token-unregistered-pgid" \
		7002 \
		7700 \
		"process_group" \
		"123"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-unregistered-pgid"
			}
			kill() {
				printf 'kill:%s %s\n' "$1" "$2"
				[ "$2" = "7001" ] && printf '%s\n' "gone" >"$state_file"
				return 0
			}
			wait() {
				printf 'wait:%s\n' "$1"
				return 0
			}
			zxfer_teardown_unregistered_background_runner \
				"job-unregistered-pgid" \
				7001 \
				"$control_dir" \
				"/tmp/runner" \
				"token-unregistered-pgid" \
				"start-unregistered-pgid" \
				"TERM"
			printf 'status=%s\n' "$?"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)
	unset ZXFER_TEST_UNREGISTERED_STATE_FILE ZXFER_TEST_PARENT_PID

	assertContains "Unregistered-runner teardown should signal a validated worker process group when launch metadata is available." \
		"$output" "kill:-TERM -7700"
	assertContains "Unregistered-runner teardown should also signal the tracked runner pid." \
		"$output" "kill:-TERM 7001"
	assertContains "Unregistered-runner teardown should reap the runner after validated signaling." \
		"$output" "wait:7001"
	assertContains "Unregistered-runner teardown should clean the private control directory after reaping." \
		"$output" "dir_exists=no"
	assertContains "Unregistered-runner teardown should succeed after validated process-group cleanup." \
		"$output" "status=0"
}

test_teardown_unregistered_background_runner_falls_back_to_direct_child_set_without_launch_metadata() {
	control_dir="$TEST_TMPDIR/background_job_unregistered_teardown_child_set"
	fake_ps="$TEST_TMPDIR/fake_ps_unregistered_teardown_child_set.sh"
	state_file="$TEST_TMPDIR/background_job_unregistered_teardown_child_set.state"
	mkdir -p "$control_dir"
	printf '%s\n' "live" >"$state_file"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    if [ "$(cat "$ZXFER_TEST_UNREGISTERED_STATE_FILE")" = "live" ]; then
      printf '%s\n' "7101 $ZXFER_TEST_PARENT_PID 7101"
      printf '%s\n' "7102 7101 7101"
    fi
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"
	ZXFER_TEST_UNREGISTERED_STATE_FILE=$state_file
	ZXFER_TEST_PARENT_PID=$$
	export ZXFER_TEST_UNREGISTERED_STATE_FILE ZXFER_TEST_PARENT_PID

	output=$(
		(
			kill() {
				printf 'kill:%s %s\n' "$1" "$2"
				[ "$2" = "7101" ] && printf '%s\n' "gone" >"$state_file"
				return 0
			}
			wait() {
				printf 'wait:%s\n' "$1"
				return 0
			}
			zxfer_teardown_unregistered_background_runner \
				"job-unregistered-child-set" \
				7101 \
				"$control_dir" \
				"/tmp/runner" \
				"token-unregistered-child-set" \
				"" \
				"TERM"
			printf 'status=%s\n' "$?"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)
	unset ZXFER_TEST_UNREGISTERED_STATE_FILE ZXFER_TEST_PARENT_PID

	assertContains "Unregistered-runner teardown without launch metadata should signal the direct runner child." \
		"$output" "kill:-TERM 7101"
	assertContains "Unregistered-runner teardown without launch metadata should signal descendants from the owned child set." \
		"$output" "kill:-TERM 7102"
	assertNotContains "Unregistered-runner teardown without launch metadata should not use process-group signaling." \
		"$output" "kill:-TERM -"
	assertContains "Unregistered-runner teardown without launch metadata should reap the runner after validated signaling." \
		"$output" "wait:7101"
	assertContains "Unregistered-runner teardown without launch metadata should clean the private control directory." \
		"$output" "dir_exists=no"
	assertContains "Unregistered-runner teardown without launch metadata should succeed." \
		"$output" "status=0"
}

test_finish_signaled_background_job_abort_covers_revalidation_failure_paths() {
	control_dir="$TEST_TMPDIR/background_job_finish_abort_paths"
	mkdir -p "$control_dir"

	output=$(
		(
			set +e
			zxfer_read_background_job_process_snapshot() {
				return 1
			}
			zxfer_finish_signaled_background_job_abort "job-finish-snapshot" 7201 "$control_dir" "start" 0 0
			printf 'snapshot_status=%s\n' "$?"
			printf 'snapshot_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7202 1 7202"
			}
			zxfer_background_job_runner_matches() {
				printf '%s\n' "status	0" >"$control_dir/completion.tsv"
				return 3
			}
			zxfer_cleanup_completed_background_job() {
				printf 'completed_after_match:%s\n' "$1"
			}
			zxfer_finish_signaled_background_job_abort "job-finish-complete-match" 7202 "$control_dir" "start" 0 0
			printf 'complete_match_status=%s\n' "$?"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7203 1 7203"
			}
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_signal_validated_background_job_scope() {
				printf '%s\n' "status	0" >"$control_dir/completion.tsv"
				return 0
			}
			zxfer_cleanup_completed_background_job() {
				printf 'completed_after_escalation:%s\n' "$1"
			}
			zxfer_finish_signaled_background_job_abort "job-finish-complete-escalation" 7203 "$control_dir" "start" 0 0
			printf 'complete_escalation_status=%s\n' "$?"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			l_read_calls=0
			zxfer_read_background_job_process_snapshot() {
				l_read_calls=$((l_read_calls + 1))
				if [ "$l_read_calls" -eq 1 ]; then
					g_zxfer_background_job_process_snapshot_result="7204 1 7204"
					return 0
				fi
				return 1
			}
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_signal_validated_background_job_scope() {
				return 0
			}
			zxfer_finish_signaled_background_job_abort "job-finish-second-snapshot" 7204 "$control_dir" "start" 0 0
			printf 'second_snapshot_status=%s\n' "$?"
			printf 'second_snapshot_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			l_match_calls=0
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7205 1 7205"
			}
			zxfer_background_job_runner_matches() {
				l_match_calls=$((l_match_calls + 1))
				[ "$l_match_calls" -eq 1 ] && return 0
				return 1
			}
			zxfer_signal_validated_background_job_scope() {
				return 0
			}
			zxfer_cleanup_aborted_background_job() {
				printf 'aborted_after_second_match:%s:%s\n' "$1" "$2"
			}
			zxfer_finish_signaled_background_job_abort "job-finish-second-missing" 7205 "$control_dir" "start" 0 0
			printf 'second_missing_status=%s\n' "$?"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			l_match_calls=0
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7206 1 7206"
			}
			zxfer_background_job_runner_matches() {
				l_match_calls=$((l_match_calls + 1))
				[ "$l_match_calls" -eq 1 ] && return 0
				return 2
			}
			zxfer_signal_validated_background_job_scope() {
				return 0
			}
			zxfer_finish_signaled_background_job_abort "job-finish-second-validate" 7206 "$control_dir" "start" 0 0
			printf 'second_validate_status=%s\n' "$?"
			printf 'second_validate_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			l_match_calls=0
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7207 1 7207"
			}
			zxfer_background_job_runner_matches() {
				l_match_calls=$((l_match_calls + 1))
				[ "$l_match_calls" -eq 1 ] && return 0
				return 3
			}
			zxfer_signal_validated_background_job_scope() {
				return 0
			}
			zxfer_finish_signaled_background_job_abort "job-finish-second-mismatch" 7207 "$control_dir" "start" 0 0
			printf 'second_mismatch_status=%s\n' "$?"
			printf 'second_mismatch_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
		(
			set +e
			rm -f "$control_dir/completion.tsv"
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7208 1 7208"
			}
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_signal_validated_background_job_scope() {
				return 0
			}
			zxfer_finish_signaled_background_job_abort "job-finish-still-live" 7208 "$control_dir" "start" 0 0
			printf 'still_live_status=%s\n' "$?"
			printf 'still_live_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)

	assertContains "Abort finalization should fail closed when post-signal process snapshots cannot be read." \
		"$output" "snapshot_status=1"
	assertContains "Abort finalization should preserve the process-table failure message." \
		"$output" "snapshot_message=Failed to inspect the process table"
	assertContains "Abort finalization should accept a completion marker that appears during first identity revalidation." \
		"$output" "completed_after_match:job-finish-complete-match"
	assertContains "Abort finalization should accept a completion marker that appears during escalation." \
		"$output" "completed_after_escalation:job-finish-complete-escalation"
	assertContains "Abort finalization should fail closed when the second process snapshot cannot be read." \
		"$output" "second_snapshot_status=1"
	assertContains "Abort finalization should unregister and clean jobs whose runner disappears after escalation." \
		"$output" "aborted_after_second_match:job-finish-second-missing:$control_dir"
	assertContains "Abort finalization should report second-pass identity validation failures." \
		"$output" "second_validate_message=Failed to validate the live runner identity"
	assertContains "Abort finalization should report second-pass identity mismatches." \
		"$output" "second_mismatch_message=Refusing to tear down background job [job-finish-second-mismatch]"
	assertContains "Abort finalization should fail closed when the validated runner remains live after successful abort signaling." \
		"$output" "still_live_status=1"
	assertContains "Abort finalization should preserve the still-live runner message." \
		"$output" "still_live_message=Refusing to remove background job [job-finish-still-live]"
}

test_teardown_unregistered_background_runner_covers_fail_closed_paths() {
	control_dir="$TEST_TMPDIR/background_job_unregistered_teardown_failures"
	mkdir -p "$control_dir"

	output=$(
		(
			set +e
			zxfer_cleanup_runtime_artifact_path() {
				printf 'cleanup_invalid:%s\n' "$1"
			}
			zxfer_teardown_unregistered_background_runner "job-invalid" "bad" "$control_dir" "/tmp/runner" "token" "" "TERM"
			printf 'invalid_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_read_background_job_process_snapshot() {
				return 1
			}
			zxfer_teardown_unregistered_background_runner "job-snapshot" 7301 "$control_dir" "/tmp/runner" "token" "" "TERM"
			printf 'snapshot_status=%s\n' "$?"
			printf 'snapshot_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
		(
			set +e
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7302 9999 7302"
			}
			wait() {
				printf 'wait_unowned:%s\n' "$1"
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'cleanup_unowned:%s\n' "$1"
			}
			zxfer_teardown_unregistered_background_runner "job-unowned" 7302 "$control_dir" "/tmp/runner" "token" "" "TERM"
			printf 'unowned_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7303 $$ 7303"
			}
			zxfer_background_job_runner_matches() {
				return 3
			}
			zxfer_teardown_unregistered_background_runner "job-mismatch" 7303 "$control_dir" "/tmp/runner" "token" "start" "TERM"
			printf 'mismatch_status=%s\n' "$?"
			printf 'mismatch_message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
		(
			set +e
			l_read_calls=0
			zxfer_read_background_job_process_snapshot() {
				l_read_calls=$((l_read_calls + 1))
				[ "$l_read_calls" -eq 1 ] && {
					g_zxfer_background_job_process_snapshot_result="7304 $$ 7304"
					return 0
				}
				return 1
			}
			zxfer_signal_validated_background_job_scope() {
				return 1
			}
			zxfer_teardown_unregistered_background_runner "job-signal-reread" 7304 "$control_dir" "/tmp/runner" "token" "" "TERM"
			printf 'signal_reread_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7305 $$ 7305"
			}
			zxfer_signal_validated_background_job_scope() {
				return 1
			}
			zxfer_teardown_unregistered_background_runner "job-signal-live" 7305 "$control_dir" "/tmp/runner" "token" "" "TERM"
			printf 'signal_live_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="7306 $$ 7306"
			}
			zxfer_signal_validated_background_job_scope() {
				printf 'signal_scope:%s\n' "$4"
				return 0
			}
			wait() {
				printf 'wait_kill:%s\n' "$1"
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'cleanup_kill:%s\n' "$1"
			}
			zxfer_teardown_unregistered_background_runner "job-kill" 7306 "$control_dir" "/tmp/runner" "token" "" "TERM"
			printf 'kill_status=%s\n' "$?"
		)
	)

	assertContains "Unregistered-runner teardown should return success for invalid runner pids after cleanup." \
		"$output" "invalid_status=0"
	assertContains "Unregistered-runner teardown should fail closed when process snapshots cannot be read." \
		"$output" "snapshot_status=1"
	assertContains "Unregistered-runner teardown should report process-table failures." \
		"$output" "snapshot_message=Failed to inspect the process table"
	assertContains "Unregistered-runner teardown should reap and clean when the recorded runner is no longer an owned direct child." \
		"$output" "wait_unowned:7302"
	assertContains "Unregistered-runner teardown should reject start-token identity mismatches." \
		"$output" "mismatch_status=1"
	assertContains "Unregistered-runner teardown should fail when a failed signal cannot be revalidated." \
		"$output" "signal_reread_status=1"
	assertContains "Unregistered-runner teardown should fail when a failed signal leaves the runner live." \
		"$output" "signal_live_status=1"
	assertContains "Unregistered-runner teardown should reap after the escalation path." \
		"$output" "wait_kill:7306"
}

test_background_job_signal_helpers_reject_invalid_inputs() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_background_job_runner_matches "1001 900 1001" "" "start-token"
			printf 'runner_status=%s\n' "$?"
			zxfer_background_job_snapshot_has_pid_with_pgid "1 2 3" "" "3"
			printf 'pid_status=%s\n' "$?"
			zxfer_background_job_snapshot_has_pid_with_pgid "1 2 3" "1" ""
			printf 'pgid_status=%s\n' "$?"
			zxfer_signal_background_job_process_group "" TERM
			printf 'signal_status=%s\n' "$?"
		)
	)
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertContains "Runner identity checks should reject invalid runner pid inputs." \
		"$output" "runner_status=1"
	assertContains "Process-snapshot validation should reject invalid pid inputs." \
		"$output" "pid_status=1"
	assertContains "Process-snapshot validation should reject invalid pgid inputs." \
		"$output" "pgid_status=1"
	assertContains "Process-group signaling should reject invalid pgid inputs." \
		"$output" "signal_status=1"
}

test_background_job_snapshot_and_runner_match_helpers_cover_missing_pid_paths() {
	output=$(
		(
			set +e
			zxfer_background_job_snapshot_has_pid "1001 900 1001" ""
			printf 'snapshot_status=%s\n' "$?"
			zxfer_background_job_runner_matches "1002 900 1002" 1001 "start-token"
			printf 'runner_status=%s\n' "$?"
		)
	)

	assertContains "Process-snapshot validation should reject blank pid inputs." \
		"$output" "snapshot_status=1"
	assertContains "Runner validation should return missing when the runner pid is absent from the snapshot." \
		"$output" "runner_status=1"
}

test_background_job_snapshot_has_pid_with_parent_rejects_invalid_inputs() {
	output=$(
		(
			set +e
			zxfer_background_job_snapshot_has_pid_with_parent "1001 900 1001" "" 900
			printf 'pid=%s\n' "$?"
			zxfer_background_job_snapshot_has_pid_with_parent "1001 900 1001" 1001 ""
			printf 'parent=%s\n' "$?"
		)
	)

	assertContains "Parent-snapshot validation should reject blank pid inputs." \
		"$output" "pid=1"
	assertContains "Parent-snapshot validation should reject blank parent-pid inputs." \
		"$output" "parent=1"
}

test_abort_background_job_treats_completed_jobs_as_already_finished_when_launch_or_identity_checks_fail() {
	control_dir="$TEST_TMPDIR/background_job_abort_completed"
	fake_ps="$TEST_TMPDIR/fake_ps_completed.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "4001 900 4001"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"

	printf '%s\n' "status	0" >"$control_dir/completion.tsv"
	zxfer_register_background_job_record "job-4" "send_receive" 4001 "$control_dir" "/tmp/runner" "token-4" "start-4"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "wrong-start"
			}
			zxfer_abort_background_job "job-4" TERM
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should treat unreadable completed jobs as already finished." \
		"$output" "status=0"
	assertContains "Abort cleanup should unregister completed jobs whose runner identity no longer matches." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory for completed jobs." \
		"$output" "dir_exists=no"
}

test_abort_background_job_treats_completed_jobs_as_already_finished_when_launch_metadata_mismatches() {
	control_dir="$TEST_TMPDIR/background_job_abort_completed_launch_mismatch"
	mkdir -p "$control_dir"
	cat >"$control_dir/launch.tsv" <<'EOF'
version	1
job_id	job-mismatch
kind	send_receive
runner_pid	4101
runner_script	/tmp/runner
runner_token	wrong-token
worker_pid	4102
worker_pgid	4102
teardown_mode	process_group
started_epoch	123
EOF
	printf '%s\n' "status	0" >"$control_dir/completion.tsv"
	zxfer_register_background_job_record "job-mismatch" "send_receive" 4101 "$control_dir" "/tmp/runner" "token-mismatch" "start-mismatch"

	output=$(
		(
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="4101 1 4101"
			}
			zxfer_abort_background_job "job-mismatch" TERM
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should treat completed jobs with mismatched launch metadata as already finished." \
		"$output" "status=0"
	assertContains "Abort cleanup should unregister completed jobs whose launch metadata no longer matches." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory for completed jobs with mismatched launch metadata." \
		"$output" "dir_exists=no"
}

test_abort_background_job_treats_completed_jobs_as_already_finished_when_launch_read_fails() {
	control_dir="$TEST_TMPDIR/background_job_abort_completed_launch_read"
	mkdir -p "$control_dir"
	printf '%s\n' "version	1" >"$control_dir/launch.tsv"
	printf '%s\n' "status	0" >"$control_dir/completion.tsv"
	zxfer_register_background_job_record "job-launch-read" "send_receive" 4051 "$control_dir" "/tmp/runner" "token-launch-read" "start-launch-read"

	output=$(
		(
			zxfer_read_background_job_launch_file() {
				return 1
			}
			zxfer_abort_background_job "job-launch-read" TERM
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should treat completed jobs with unreadable launch metadata as already finished." \
		"$output" "status=0"
	assertContains "Abort cleanup should unregister completed jobs whose launch metadata can no longer be read." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory for completed jobs with unreadable launch metadata." \
		"$output" "dir_exists=no"
}

test_abort_background_job_treats_completed_jobs_as_already_finished_when_live_identity_validation_fails() {
	control_dir="$TEST_TMPDIR/background_job_abort_completed_identity_validate"
	mkdir -p "$control_dir"
	printf '%s\n' "status	0" >"$control_dir/completion.tsv"
	zxfer_register_background_job_record "job-identity-completed" "send_receive" 4151 "$control_dir" "/tmp/runner" "token-identity-completed" "start-identity-completed"

	output=$(
		(
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="4151 1 4151"
			}
			zxfer_get_process_start_token() {
				return 1
			}
			zxfer_abort_background_job "job-identity-completed" TERM
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should treat completed jobs with unreadable live identity validation as already finished." \
		"$output" "status=0"
	assertContains "Abort cleanup should unregister completed jobs whose live identity cannot be revalidated." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory for completed jobs whose live identity cannot be revalidated." \
		"$output" "dir_exists=no"
}

test_abort_background_job_treats_completed_jobs_as_already_finished_when_process_snapshot_read_fails() {
	control_dir="$TEST_TMPDIR/background_job_abort_completed_snapshot_read"
	mkdir -p "$control_dir"
	printf '%s\n' "status	0" >"$control_dir/completion.tsv"
	zxfer_register_background_job_record "job-snapshot-completed" "send_receive" 4251 "$control_dir" "/tmp/runner" "token-snapshot-completed" "start-snapshot-completed"

	output=$(
		(
			zxfer_read_background_job_process_snapshot() {
				return 1
			}
			zxfer_abort_background_job "job-snapshot-completed" TERM
			printf 'status=%s\n' "$?"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should treat completed jobs with unreadable process snapshots as already finished." \
		"$output" "status=0"
	assertContains "Abort cleanup should unregister completed jobs whose process snapshot can no longer be read." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory for completed jobs whose process snapshot can no longer be read." \
		"$output" "dir_exists=no"
}

test_abort_background_job_reports_snapshot_and_signal_failures() {
	control_dir="$TEST_TMPDIR/background_job_abort_failures"
	mkdir -p "$control_dir"
	zxfer_write_background_job_launch_file \
		"$control_dir" \
		"job-5" \
		"send_receive" \
		5001 \
		"/tmp/runner" \
		"token-5" \
		5002 \
		5002 \
		"process_group" \
		"123"
	zxfer_register_background_job_record "job-5" "send_receive" 5001 "$control_dir" "/tmp/runner" "token-5" "start-5"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	snapshot_output=$(
		(
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_read_background_job_process_snapshot() {
				return 1
			}
			zxfer_abort_background_job "job-5" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	snapshot_status=$?
	signal_output=$(
		(
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="5001 1 5001
5002 5001 5002"
			}
			zxfer_signal_background_job_process_group() {
				return 1
			}
			kill() {
				return 1
			}
			zxfer_abort_background_job "job-5" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	signal_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Abort cleanup should fail closed when the process snapshot cannot be collected." \
		0 "$snapshot_status"
	assertContains "Process-snapshot failures should preserve the cleanup error." \
		"$snapshot_output" "status=1"
	assertContains "Process-snapshot failures should preserve the dedicated failure message." \
		"$snapshot_output" "Failed to inspect the process table"
	assertEquals "Abort cleanup should fail closed when signaling the validated teardown target fails." \
		0 "$signal_status"
	assertContains "Signal failures should preserve the cleanup error." \
		"$signal_output" "status=1"
	assertContains "Signal failures should preserve the dedicated failure message." \
		"$signal_output" "Failed to signal the validated teardown target"
}

test_abort_background_job_reports_post_signal_identity_validation_failures() {
	identity_validate_control_dir="$TEST_TMPDIR/background_job_abort_post_signal_validate"
	identity_mismatch_control_dir="$TEST_TMPDIR/background_job_abort_post_signal_mismatch"
	mkdir -p "$identity_validate_control_dir" "$identity_mismatch_control_dir"
	zxfer_register_background_job_record "job-post-validate" "send_receive" 6301 "$identity_validate_control_dir" "/tmp/runner" "token-post-validate" "start-post-validate"
	zxfer_register_background_job_record "job-post-mismatch" "send_receive" 6401 "$identity_mismatch_control_dir" "/tmp/runner" "token-post-mismatch" "start-post-mismatch"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	validate_output=$(
		(
			g_test_runner_match_calls=0
			zxfer_background_job_runner_matches() {
				g_test_runner_match_calls=$((g_test_runner_match_calls + 1))
				if [ "$g_test_runner_match_calls" -eq 1 ]; then
					return 0
				fi
				return 2
			}
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="6301 1 6301"
			}
			zxfer_get_background_job_pid_set() {
				g_zxfer_background_job_pid_set_result="6301"
			}
			zxfer_signal_background_job_pid_set() {
				return 1
			}
			zxfer_abort_background_job "job-post-validate" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	validate_status=$?
	mismatch_output=$(
		(
			g_test_runner_match_calls=0
			zxfer_background_job_runner_matches() {
				g_test_runner_match_calls=$((g_test_runner_match_calls + 1))
				if [ "$g_test_runner_match_calls" -eq 1 ]; then
					return 0
				fi
				return 3
			}
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="6401 1 6401"
			}
			zxfer_get_background_job_pid_set() {
				g_zxfer_background_job_pid_set_result="6401"
			}
			zxfer_signal_background_job_pid_set() {
				return 1
			}
			zxfer_abort_background_job "job-post-mismatch" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	mismatch_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Abort cleanup should fail closed when the runner identity cannot be revalidated after signal failure." \
		0 "$validate_status"
	assertContains "Post-signal validation failures should preserve the cleanup error." \
		"$validate_output" "status=1"
	assertContains "Post-signal validation failures should preserve the dedicated failure message." \
		"$validate_output" "Failed to validate the live runner identity for background job [job-post-validate]."
	assertEquals "Abort cleanup should fail closed when the runner identity changes after signal failure." \
		0 "$mismatch_status"
	assertContains "Post-signal identity mismatches should preserve the cleanup error." \
		"$mismatch_output" "status=1"
	assertContains "Post-signal identity mismatches should preserve the dedicated failure message." \
		"$mismatch_output" "tracked runner PID [6401] no longer matches the recorded helper identity"
}

test_abort_background_job_treats_post_signal_runner_disappearance_as_success() {
	control_dir="$TEST_TMPDIR/background_job_abort_post_signal_disappeared"
	mkdir -p "$control_dir"
	zxfer_register_background_job_record "job-post-disappeared" "send_receive" 6501 "$control_dir" "/tmp/runner" "token-post-disappeared" "start-post-disappeared"

	output=$(
		(
			g_test_snapshot_calls=0
			zxfer_background_job_runner_matches() {
				if [ "$g_test_snapshot_calls" -eq 1 ]; then
					return 0
				fi
				if [ "$g_test_snapshot_calls" -eq 2 ]; then
					return 1
				fi
				return 2
			}
			zxfer_read_background_job_process_snapshot() {
				g_test_snapshot_calls=$((g_test_snapshot_calls + 1))
				if [ "$g_test_snapshot_calls" -eq 1 ]; then
					g_zxfer_background_job_process_snapshot_result="6501 1 6501
6502 6501 6502"
					return 0
				fi
				if [ "$g_test_snapshot_calls" -eq 2 ]; then
					g_zxfer_background_job_process_snapshot_result=""
				fi
			}
			zxfer_get_background_job_pid_set() {
				g_zxfer_background_job_pid_set_result="6501 6502"
			}
			zxfer_signal_background_job_pid_set() {
				return 1
			}
			zxfer_abort_background_job "job-post-disappeared" TERM
			printf 'status=%s\n' "$?"
			printf 'snapshot_calls=%s\n' "$g_test_snapshot_calls"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should succeed when the runner disappears after a failed signal attempt." \
		"$output" "status=0"
	assertContains "Abort cleanup should reread the process snapshot before post-signal revalidation." \
		"$output" "snapshot_calls=2"
	assertContains "Abort cleanup should unregister jobs whose runner disappears after signal failure." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory when the runner disappears after signal failure." \
		"$output" "dir_exists=no"
}

test_abort_background_job_treats_post_signal_snapshot_read_failure_with_late_completion_as_success() {
	control_dir="$TEST_TMPDIR/background_job_abort_post_signal_completion_snapshot"
	mkdir -p "$control_dir"
	zxfer_register_background_job_record "job-post-completion-snapshot" "send_receive" 6511 "$control_dir" "/tmp/runner" "token-post-completion-snapshot" "start-post-completion-snapshot"

	output=$(
		(
			g_test_snapshot_calls=0
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_read_background_job_process_snapshot() {
				g_test_snapshot_calls=$((g_test_snapshot_calls + 1))
				if [ "$g_test_snapshot_calls" -eq 1 ]; then
					g_zxfer_background_job_process_snapshot_result="6511 1 6511
6512 6511 6512"
					return 0
				fi
				printf '%s\n' "status	0" >"$control_dir/completion.tsv"
				return 1
			}
			zxfer_get_background_job_pid_set() {
				g_zxfer_background_job_pid_set_result="6511 6512"
			}
			zxfer_signal_background_job_pid_set() {
				return 1
			}
			zxfer_abort_background_job "job-post-completion-snapshot" TERM
			printf 'status=%s\n' "$?"
			printf 'snapshot_calls=%s\n' "$g_test_snapshot_calls"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should succeed when completion is recorded before the post-signal snapshot reread fails." \
		"$output" "status=0"
	assertContains "Abort cleanup should still perform the post-signal snapshot reread before noticing the late completion marker." \
		"$output" "snapshot_calls=2"
	assertContains "Abort cleanup should unregister jobs whose completion marker appears before the post-signal snapshot reread failure." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory when a late completion marker wins over the post-signal snapshot reread failure." \
		"$output" "dir_exists=no"
}

test_abort_background_job_treats_post_signal_identity_validation_failure_with_late_completion_as_success() {
	control_dir="$TEST_TMPDIR/background_job_abort_post_signal_completion_validate"
	mkdir -p "$control_dir"
	zxfer_register_background_job_record "job-post-completion-validate" "send_receive" 6521 "$control_dir" "/tmp/runner" "token-post-completion-validate" "start-post-completion-validate"

	output=$(
		(
			g_test_snapshot_calls=0
			g_test_runner_match_calls=0
			zxfer_background_job_runner_matches() {
				g_test_runner_match_calls=$((g_test_runner_match_calls + 1))
				if [ "$g_test_runner_match_calls" -eq 1 ]; then
					return 0
				fi
				printf '%s\n' "status	0" >"$control_dir/completion.tsv"
				return 2
			}
			zxfer_read_background_job_process_snapshot() {
				g_test_snapshot_calls=$((g_test_snapshot_calls + 1))
				g_zxfer_background_job_process_snapshot_result="6521 1 6521
6522 6521 6522"
				return 0
			}
			zxfer_get_background_job_pid_set() {
				g_zxfer_background_job_pid_set_result="6521 6522"
			}
			zxfer_signal_background_job_pid_set() {
				return 1
			}
			zxfer_abort_background_job "job-post-completion-validate" TERM
			printf 'status=%s\n' "$?"
			printf 'runner_match_calls=%s\n' "$g_test_runner_match_calls"
			printf 'records=<%s>\n' "${g_zxfer_background_job_records:-}"
			if [ -e "$control_dir" ]; then
				l_dir_exists=yes
			else
				l_dir_exists=no
			fi
			printf 'dir_exists=%s\n' "$l_dir_exists"
		)
	)

	assertContains "Abort cleanup should succeed when completion is recorded before post-signal identity validation would fail." \
		"$output" "status=0"
	assertContains "Abort cleanup should still perform refreshed identity validation before noticing the late completion marker." \
		"$output" "runner_match_calls=2"
	assertContains "Abort cleanup should unregister jobs whose completion marker appears before the post-signal identity validation failure is surfaced." \
		"$output" "records=<>"
	assertContains "Abort cleanup should remove the private control directory when a late completion marker wins over the post-signal identity validation failure." \
		"$output" "dir_exists=no"
}

test_abort_background_job_falls_back_to_child_set_when_launch_metadata_is_not_published_yet() {
	control_dir="$TEST_TMPDIR/background_job_abort_missing_launch"
	fake_ps="$TEST_TMPDIR/fake_ps_missing_launch.sh"
	mkdir -p "$control_dir"
	cat >"$fake_ps" <<'EOF'
#!/bin/sh
case "$*" in
  "-o pid= -o args= -p 6101")
    printf '%s\n' "6101 /tmp/runner token-read"
    ;;
  "-o pid= -o ppid= -o pgid=")
    printf '%s\n' "6101 900 6101"
    printf '%s\n' "6102 6101 6102"
    printf '%s\n' "6103 6102 6102"
    ;;
  "-o pgid= -p "*)
    printf '%s\n' "900"
    ;;
esac
EOF
	chmod +x "$fake_ps"
	g_cmd_ps="$fake_ps"
	zxfer_register_background_job_record "job-read" "send_receive" 6101 "$control_dir" "/tmp/runner" "token-read" "start-read"

	output=$(
		(
			zxfer_get_process_start_token() {
				printf '%s\n' "start-read"
			}
			kill() {
				printf '%s %s\n' "$1" "$2"
				[ "$2" = "6101" ] && printf '%s\n' "status	143" >"$control_dir/completion.tsv"
				return 0
			}
			zxfer_abort_background_job "job-read" TERM
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Abort cleanup should fall back to the validated child set when launch metadata has not been published yet." \
		"$output" "-TERM 6101"
	assertContains "Abort cleanup should still signal direct descendants when launch metadata is not available yet." \
		"$output" "-TERM 6102"
	assertContains "Abort cleanup should still signal deeper descendants when launch metadata is not available yet." \
		"$output" "-TERM 6103"
	assertContains "Abort cleanup should still complete successfully when launch metadata has not been published yet." \
		"$output" "status=0"
}

test_abort_background_job_reports_launch_read_and_child_set_derivation_failures() {
	read_control_dir="$TEST_TMPDIR/background_job_abort_launch_read_fail"
	child_set_control_dir="$TEST_TMPDIR/background_job_abort_child_set_fail"
	mkdir -p "$read_control_dir" "$child_set_control_dir"
	printf '%s\n' "broken" >"$read_control_dir/launch.tsv"
	zxfer_register_background_job_record "job-read" "send_receive" 6101 "$read_control_dir" "/tmp/runner" "token-read" "start-read"
	zxfer_write_background_job_launch_file \
		"$child_set_control_dir" \
		"job-child" \
		"send_receive" \
		6201 \
		"/tmp/runner" \
		"token-child" \
		6202 \
		"" \
		"child_set" \
		"123"
	zxfer_register_background_job_record "job-child" "send_receive" 6201 "$child_set_control_dir" "/tmp/runner" "token-child" "start-child"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	read_output=$(
		(
			zxfer_read_background_job_launch_file() {
				return 1
			}
			zxfer_abort_background_job "job-read" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	read_status=$?
	child_set_output=$(
		(
			zxfer_background_job_runner_matches() {
				return 0
			}
			zxfer_read_background_job_process_snapshot() {
				g_zxfer_background_job_process_snapshot_result="6201 1 6201"
			}
			zxfer_get_background_job_pid_set() {
				return 1
			}
			zxfer_abort_background_job "job-child" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	child_set_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Abort cleanup should fail closed when launch metadata cannot be read for a live tracked job." \
		0 "$read_status"
	assertContains "Launch-metadata read failures should preserve the cleanup error." \
		"$read_output" "status=1"
	assertContains "Launch-metadata read failures should preserve the dedicated failure message." \
		"$read_output" "Failed to read launch metadata for background job [job-read]."
	assertEquals "Abort cleanup should fail closed when child-set derivation fails for a validated live job." \
		0 "$child_set_status"
	assertContains "Child-set derivation failures should preserve the cleanup error." \
		"$child_set_output" "status=1"
	assertContains "Child-set derivation failures should preserve the dedicated failure message." \
		"$child_set_output" "Failed to derive the owned child set for background job [job-child] cleanup."
}

test_abort_background_job_returns_success_for_unknown_jobs() {
	zxfer_abort_background_job "job-unknown" TERM
	status=$?

	assertEquals "Abort cleanup should treat unknown jobs as already finished." \
		0 "$status"
}

test_abort_all_background_jobs_aborts_each_registered_job() {
	zxfer_register_background_job_record "job-1" "send_receive" 101 "$TEST_TMPDIR/job-1" "/tmp/runner" "token-1" "start-1"
	zxfer_register_background_job_record "job-2" "source_snapshot_list" 202 "$TEST_TMPDIR/job-2" "/tmp/runner" "token-2" "start-2"

	output=$(
		(
			zxfer_abort_background_job() {
				printf 'abort:%s:%s\n' "$1" "$2"
			}
			zxfer_abort_all_background_jobs
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Aborting all background jobs should visit the first tracked job id." \
		"$output" "abort:job-1:TERM"
	assertContains "Aborting all background jobs should visit later tracked job ids too." \
		"$output" "abort:job-2:TERM"
	assertContains "Aborting all background jobs should succeed when each tracked job abort succeeds." \
		"$output" "status=0"
}

test_abort_all_background_jobs_preserves_abort_failures() {
	zxfer_register_background_job_record "job-1" "send_receive" 101 "$TEST_TMPDIR/job-1" "/tmp/runner" "token-1" "start-1"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_abort_background_job() {
				return 1
			}
			zxfer_abort_all_background_jobs
			printf 'status=%s\n' "$?"
		)
	)
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertContains "Aborting all background jobs should preserve the first cleanup failure." \
		"$output" "status=1"
}

test_abort_all_background_jobs_continues_after_failures_and_preserves_first_message() {
	zxfer_register_background_job_record "job-1" "send_receive" 101 "$TEST_TMPDIR/job-1" "/tmp/runner" "token-1" "start-1"
	zxfer_register_background_job_record "job-2" "source_snapshot_list" 202 "$TEST_TMPDIR/job-2" "/tmp/runner" "token-2" "start-2"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_abort_background_job() {
				printf 'abort:%s:%s\n' "$1" "$2"
				if [ "$1" = "job-1" ]; then
					g_zxfer_background_job_abort_failure_message="job-1 failed"
					return 1
				fi
				return 0
			}
			zxfer_abort_all_background_jobs
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
		)
	)
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertContains "Aborting all background jobs should still attempt later tracked jobs after an earlier failure." \
		"$output" "abort:job-2:TERM"
	assertContains "Aborting all background jobs should still return failure when any tracked job abort fails." \
		"$output" "status=1"
	assertContains "Aborting all background jobs should preserve the first abort failure message after continuing cleanup." \
		"$output" "message=job-1 failed"
}

test_background_job_abort_completion_races_cover_current_shell_paths() {
	invalid_runner_control_dir="$TEST_TMPDIR/background_job_cleanup_invalid_runner"
	second_snapshot_control_dir="$TEST_TMPDIR/background_job_finish_second_snapshot_completion"
	second_validate_control_dir="$TEST_TMPDIR/background_job_finish_second_validate_completion"
	second_mismatch_control_dir="$TEST_TMPDIR/background_job_finish_second_mismatch_completion"
	launch_mismatch_control_dir="$TEST_TMPDIR/background_job_abort_launch_mismatch_completion"
	mkdir -p \
		"$invalid_runner_control_dir" \
		"$second_snapshot_control_dir" \
		"$second_validate_control_dir" \
		"$second_mismatch_control_dir" \
		"$launch_mismatch_control_dir"
	output_file="$TEST_TMPDIR/background_job_abort_completion_races.log"
	: >"$output_file"

	zxfer_register_background_job_record \
		"job-invalid-runner" "send_receive" 101 \
		"$invalid_runner_control_dir" "/tmp/runner" "token" "start"
	zxfer_cleanup_completed_background_job \
		"job-invalid-runner" "not-a-pid" "$invalid_runner_control_dir"
	printf 'invalid_runner_cleanup=%s\n' "$?" >>"$output_file"
	printf 'invalid_runner_records=<%s>\n' "${g_zxfer_background_job_records:-}" >>"$output_file"

	l_read_calls=0
	zxfer_read_background_job_process_snapshot() {
		l_read_calls=$((l_read_calls + 1))
		if [ "$l_read_calls" -eq 1 ]; then
			g_zxfer_background_job_process_snapshot_result="7301 1 7301"
			return 0
		fi
		printf '%s\n' "status	0" >"$second_snapshot_control_dir/completion.tsv"
		return 1
	}
	zxfer_background_job_runner_matches() {
		return 0
	}
	zxfer_signal_validated_background_job_scope() {
		return 0
	}
	zxfer_cleanup_completed_background_job() {
		printf 'completed_second_snapshot:%s:%s\n' "$1" "$2" >>"$output_file"
		return 0
	}
	zxfer_finish_signaled_background_job_abort \
		"job-finish-second-snapshot-complete" \
		7301 \
		"$second_snapshot_control_dir" \
		"start" \
		0 \
		0
	printf 'second_snapshot_completion_status=%s\n' "$?" >>"$output_file"

	l_match_calls=0
	zxfer_read_background_job_process_snapshot() {
		g_zxfer_background_job_process_snapshot_result="7302 1 7302"
		return 0
	}
	zxfer_background_job_runner_matches() {
		l_match_calls=$((l_match_calls + 1))
		if [ "$l_match_calls" -eq 1 ]; then
			return 0
		fi
		printf '%s\n' "status	0" >"$second_validate_control_dir/completion.tsv"
		return 2
	}
	zxfer_cleanup_completed_background_job() {
		printf 'completed_second_validate:%s:%s\n' "$1" "$2" >>"$output_file"
		return 0
	}
	zxfer_finish_signaled_background_job_abort \
		"job-finish-second-validate-complete" \
		7302 \
		"$second_validate_control_dir" \
		"start" \
		0 \
		0
	printf 'second_validate_completion_status=%s\n' "$?" >>"$output_file"

	l_match_calls=0
	zxfer_read_background_job_process_snapshot() {
		g_zxfer_background_job_process_snapshot_result="7303 1 7303"
		return 0
	}
	zxfer_background_job_runner_matches() {
		l_match_calls=$((l_match_calls + 1))
		if [ "$l_match_calls" -eq 1 ]; then
			return 0
		fi
		printf '%s\n' "status	0" >"$second_mismatch_control_dir/completion.tsv"
		return 3
	}
	zxfer_cleanup_completed_background_job() {
		printf 'completed_second_mismatch:%s:%s\n' "$1" "$2" >>"$output_file"
		return 0
	}
	zxfer_finish_signaled_background_job_abort \
		"job-finish-second-mismatch-complete" \
		7303 \
		"$second_mismatch_control_dir" \
		"start" \
		0 \
		0
	printf 'second_mismatch_completion_status=%s\n' "$?" >>"$output_file"

	{
		printf '%s\t%s\n' version 1
		printf '%s\t%s\n' job_id job-launch-mismatch-complete
		printf '%s\t%s\n' kind send_receive
		printf '%s\t%s\n' runner_pid 7401
		printf '%s\t%s\n' runner_script /tmp/runner
		printf '%s\t%s\n' runner_token wrong-token
		printf '%s\t%s\n' worker_pid 7402
		printf '%s\t%s\n' worker_pgid 7402
		printf '%s\t%s\n' teardown_mode process_group
		printf '%s\t%s\n' started_epoch 123
	} >"$launch_mismatch_control_dir/launch.tsv"
	printf '%s\n' "status	0" >"$launch_mismatch_control_dir/completion.tsv"
	zxfer_register_background_job_record \
		"job-launch-mismatch-complete" \
		"send_receive" \
		7401 \
		"$launch_mismatch_control_dir" \
		"/tmp/runner" \
		"token-launch-mismatch-complete" \
		"start-launch-mismatch-complete"
	zxfer_read_background_job_process_snapshot() {
		g_zxfer_background_job_process_snapshot_result="7401 1 7401"
		return 0
	}
	zxfer_cleanup_completed_background_job() {
		printf 'completed_launch_mismatch:%s:%s\n' "$1" "$2" >>"$output_file"
		return 0
	}
	zxfer_abort_background_job "job-launch-mismatch-complete" TERM
	printf 'launch_mismatch_completion_status=%s\n' "$?" >>"$output_file"

	output=$(cat "$output_file")

	assertContains "Completed-job cleanup should tolerate nonnumeric runner ids." \
		"$output" "invalid_runner_cleanup=0"
	assertContains "Completed-job cleanup should unregister nonnumeric runner records." \
		"$output" "invalid_runner_records=<>"
	assertContains "Abort finalization should accept completion before a second process-table read failure is surfaced." \
		"$output" "completed_second_snapshot:job-finish-second-snapshot-complete:7301"
	assertContains "Abort finalization should return success for late completion before second process-table failure." \
		"$output" "second_snapshot_completion_status=0"
	assertContains "Abort finalization should accept completion before a second validation failure is surfaced." \
		"$output" "completed_second_validate:job-finish-second-validate-complete:7302"
	assertContains "Abort finalization should return success for late completion before second validation failure." \
		"$output" "second_validate_completion_status=0"
	assertContains "Abort finalization should accept completion before a second identity mismatch is surfaced." \
		"$output" "completed_second_mismatch:job-finish-second-mismatch-complete:7303"
	assertContains "Abort finalization should return success for late completion before second identity mismatch." \
		"$output" "second_mismatch_completion_status=0"
	assertContains "Abort cleanup should treat launch-metadata mismatches with completion as finished jobs." \
		"$output" "completed_launch_mismatch:job-launch-mismatch-complete:7401"
	assertContains "Abort cleanup should return success for launch-metadata mismatches with completion." \
		"$output" "launch_mismatch_completion_status=0"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
