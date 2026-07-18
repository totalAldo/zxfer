#!/bin/sh
# Send/receive completion queue and supervised-job behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_wait_for_zfs_send_jobs_returns_immediately_when_empty() {
	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=5

	zxfer_wait_for_zfs_send_jobs "unit"

	assertEquals "Waiting with no jobs should reset the running-job count." 0 "$g_count_zfs_send_jobs"
}

test_zxfer_open_send_job_completion_queue_marks_queue_unavailable_when_tempdir_setup_fails() {
	log="$TEST_TMPDIR/queue_tempdir_fail.log"

	(
		zxfer_echoV() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_create_private_temp_dir() {
			return 1
		}
		if zxfer_open_send_job_completion_queue; then
			exit 1
		fi
		printf 'unavailable=%s\n' "$g_zfs_send_job_queue_unavailable" >>"$log"
	)

	assertContains "Tempdir setup failures should mark the rolling queue unavailable." \
		"$(cat "$log")" "unavailable=1"
	assertContains "Tempdir setup failures should log the batch-wait fallback." \
		"$(cat "$log")" "Unable to create rolling send/receive completion queue"
}

test_zxfer_open_send_job_completion_queue_marks_queue_unavailable_when_mkfifo_fails() {
	log="$TEST_TMPDIR/queue_mkfifo_fail.log"

	(
		zxfer_echoV() {
			printf '%s\n' "$1" >>"$log"
		}
		mkfifo() {
			return 1
		}
		if zxfer_open_send_job_completion_queue; then
			exit 1
		fi
		printf 'unavailable=%s\n' "$g_zfs_send_job_queue_unavailable" >>"$log"
	)

	assertContains "mkfifo failures should mark the rolling queue unavailable." \
		"$(cat "$log")" "unavailable=1"
	assertContains "mkfifo failures should log the batch-wait fallback." \
		"$(cat "$log")" "falling back to batch waits"
}

test_zxfer_open_send_job_completion_queue_marks_queue_unavailable_when_chmod_fails() {
	log="$TEST_TMPDIR/queue_chmod_fail.log"

	(
		zxfer_echoV() {
			printf '%s\n' "$1" >>"$log"
		}
		chmod() {
			return 1
		}
		if zxfer_open_send_job_completion_queue; then
			exit 1
		fi
		printf 'unavailable=%s\n' "$g_zfs_send_job_queue_unavailable" >>"$log"
	)

	assertContains "chmod failures should mark the rolling queue unavailable." \
		"$(cat "$log")" "unavailable=1"
	assertContains "chmod failures should log the batch-wait fallback." \
		"$(cat "$log")" "Unable to secure rolling send/receive completion queue"
}

test_zxfer_open_send_job_completion_queue_marks_queue_unavailable_when_open_fails() {
	log="$TEST_TMPDIR/queue_open_fail.log"

	(
		zxfer_echoV() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_open_send_job_completion_queue_fd() {
			return 1
		}
		if zxfer_open_send_job_completion_queue; then
			exit 1
		fi
		printf 'unavailable=%s\n' "$g_zfs_send_job_queue_unavailable" >>"$log"
	)

	assertContains "Open failures should mark the rolling queue unavailable." \
		"$(cat "$log")" "unavailable=1"
	assertContains "Open failures should log the batch-wait fallback." \
		"$(cat "$log")" "Unable to open rolling send/receive completion queue"
}

test_zxfer_open_send_job_completion_queue_returns_failure_when_unavailable_flag_is_set() {
	g_zfs_send_job_queue_unavailable=1

	if zxfer_open_send_job_completion_queue; then
		fail "Queues marked unavailable should not be reopened."
	fi

	assertEquals "The unavailable flag should remain set." 1 "$g_zfs_send_job_queue_unavailable"
	assertEquals "No queue should be marked open when the unavailable flag is set." 0 "${g_zfs_send_job_queue_open:-0}"
}

test_zxfer_open_send_job_completion_queue_reopens_existing_writer_or_marks_missing_paths_unavailable() {
	output=$(
		(
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=0
			g_zfs_send_job_queue_path="$TEST_TMPDIR/reopen.queue"
			zxfer_open_send_job_completion_queue_writer_fd() {
				printf 'reopened=%s\n' "$1"
			}
			zxfer_open_send_job_completion_queue
			printf 'writer=%s\n' "${g_zfs_send_job_queue_writer_open:-0}"
			g_zfs_send_job_queue_writer_open=0
			g_zfs_send_job_queue_path=""
			set +e
			zxfer_open_send_job_completion_queue
			printf 'missing_status=%s\n' "$?"
			set -e
			printf 'unavailable=%s\n' "${g_zfs_send_job_queue_unavailable:-0}"
		)
	)

	assertContains "Existing rolling queues should reopen the writer descriptor when only the writer fd was closed." \
		"$output" "reopened=$TEST_TMPDIR/reopen.queue"
	assertContains "Reopening the rolling-queue writer should mark the writer fd open again." \
		"$output" "writer=1"
	assertContains "Missing rolling-queue paths should fail closed once the queue was previously marked open." \
		"$output" "missing_status=1"
	assertContains "Missing rolling-queue paths should mark the queue unavailable for later fallback paths." \
		"$output" "unavailable=1"
}

test_zxfer_open_send_job_completion_queue_reopen_failure_marks_queue_unavailable() {
	output=$(
		(
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=0
			g_zfs_send_job_queue_path="$TEST_TMPDIR/reopen-failure.queue"
			zxfer_echoV() {
				command printf '%s\n' "$1"
			}
			zxfer_open_send_job_completion_queue_writer_fd() {
				return 1
			}
			zxfer_close_send_job_completion_queue() {
				g_zfs_send_job_queue_open=0
				command printf 'closed=yes\n'
			}
			set +e
			zxfer_open_send_job_completion_queue
			status=$?
			set -e
			command printf 'status=%s\n' "$status"
			command printf 'unavailable=%s\n' "${g_zfs_send_job_queue_unavailable:-0}"
		)
	)

	assertContains "Rolling queue reopen failures should log the documented batch-wait fallback." \
		"$output" "Unable to reopen rolling send/receive completion queue; falling back to batch waits."
	assertContains "Rolling queue reopen failures should close the remembered queue state." \
		"$output" "closed=yes"
	assertContains "Rolling queue reopen failures should fail closed." \
		"$output" "status=1"
	assertContains "Rolling queue reopen failures should mark the queue unavailable for later fallback paths." \
		"$output" "unavailable=1"
}

test_zxfer_open_send_job_completion_queue_success_sets_state_and_reuses_open_writer() {
	output=$(
		(
			if ! zxfer_open_send_job_completion_queue; then
				printf 'open_status=%s\n' "$?"
				exit 1
			fi
			l_queue_path=$g_zfs_send_job_queue_path
			l_queue_dir=$g_zfs_send_job_queue_dir
			printf 'open=%s\n' "${g_zfs_send_job_queue_open:-0}"
			printf 'writer=%s\n' "${g_zfs_send_job_queue_writer_open:-0}"
			printf 'path=%s\n' "$l_queue_path"
			printf 'dir=%s\n' "$l_queue_dir"
			zxfer_open_send_job_completion_queue
			printf 'reused_path=%s\n' "$g_zfs_send_job_queue_path"
			zxfer_close_send_job_completion_queue
			printf 'closed_open=%s\n' "${g_zfs_send_job_queue_open:-0}"
			printf 'closed_writer=%s\n' "${g_zfs_send_job_queue_writer_open:-0}"
			printf 'queue_exists=%s\n' "$([ -e "$l_queue_path" ] && printf yes || printf no)"
			printf 'dir_exists=%s\n' "$([ -e "$l_queue_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Opening the rolling completion queue should mark it open." \
		"$output" "open=1"
	assertContains "Opening the rolling completion queue should mark the writer fd open." \
		"$output" "writer=1"
	assertContains "Reopening an already-open rolling completion queue should reuse the existing queue path." \
		"$output" "reused_path=$(printf '%s\n' "$output" | sed -n 's/^path=//p')"
	assertContains "Closing an opened rolling completion queue should clear the open marker." \
		"$output" "closed_open=0"
	assertContains "Closing an opened rolling completion queue should clear the writer-open marker." \
		"$output" "closed_writer=0"
	assertContains "Closing an opened rolling completion queue should remove the queue fifo." \
		"$output" "queue_exists=no"
	assertContains "Closing an opened rolling completion queue should remove the queue directory." \
		"$output" "dir_exists=no"
}

test_zxfer_open_send_job_completion_queue_fd_closes_writer_when_reader_open_fails() {
	queue_file="$TEST_TMPDIR/open_queue_fd_fail.queue"
	: >"$queue_file"

	set +e
	(
		zxfer_open_send_job_completion_queue_reader_fd() {
			return 1
		}
		zxfer_open_send_job_completion_queue_fd "$queue_file"
	)
	status=$?

	assertEquals "Opening the rolling queue should fail when the reader fd cannot be opened." \
		1 "$status"
}

test_zxfer_open_send_job_completion_queue_fd_returns_failure_when_writer_open_fails() {
	queue_file="$TEST_TMPDIR/open_queue_fd_writer_fail.queue"
	: >"$queue_file"

	set +e
	(
		zxfer_open_send_job_completion_queue_writer_fd() {
			return 1
		}
		zxfer_open_send_job_completion_queue_fd "$queue_file"
	)
	status=$?

	assertEquals "Opening the rolling queue should fail when the writer fd cannot be opened." \
		1 "$status"
}

test_zxfer_open_send_job_completion_queue_fd_promotes_unstoppable_registration_cleanup() {
	queue_file="$TEST_TMPDIR/open_queue_registration_cleanup_fail.queue"
	: >"$queue_file"

	output=$(
		(
			zxfer_register_cleanup_pid() { return 41; }
			zxfer_abort_direct_child_pid() { return 42; }
			zxfer_open_send_job_completion_queue_fd "$queue_file"
			printf 'status=%s\n' "$?"
			printf 'fatal=%s\n' "$g_zxfer_send_job_queue_open_failure_fatal"
		)
	)

	assertContains "An open helper that cannot be registered or stopped must report the cleanup failure." \
		"$output" "status=42"
	assertContains "The queue owner must distinguish unsafe cleanup failure from an ordinary batch-wait fallback." \
		"$output" "fatal=1"
}

test_zxfer_open_send_job_completion_queue_fails_closed_on_live_helper_cleanup_failure() {
	output=$(
		(
			zxfer_open_send_job_completion_queue_fd() {
				g_zxfer_send_job_queue_open_failure_fatal=1
				g_zxfer_cleanup_pid_abort_failure_message="queue helper remains live"
				return 42
			}
			zxfer_throw_error() {
				printf 'fatal-error=%s status=%s\n' "$1" "$2"
				return "$2"
			}
			zxfer_echoV() {
				printf 'fallback=%s\n' "$1"
			}
			zxfer_open_send_job_completion_queue
			printf 'status=%s unavailable=%s\n' \
				"$?" "${g_zfs_send_job_queue_unavailable:-0}"
		)
	)

	assertContains "A live queue helper cleanup failure should retain its diagnostic and status." \
		"$output" "fatal-error=queue helper remains live status=42"
	assertContains "Unsafe helper cleanup must not be relabeled as an unavailable-queue fallback." \
		"$output" "status=42 unavailable=0"
	assertNotContains "Unsafe helper cleanup must not continue through the ordinary batch-wait message." \
		"$output" "fallback="
}

test_zxfer_open_send_job_completion_queue_fd_reaps_blocked_fifo_reader_on_writer_failure() {
	queue_file="$TEST_TMPDIR/open_queue_fd_blocked_reader.queue"
	token_marker="$TEST_TMPDIR/open_queue_fd_blocked_reader.token"
	mkfifo "$queue_file" || fail "Unable to create FIFO writer-failure fixture."

	output=$(
		(
			zxfer_open_send_job_completion_queue_writer_fd() {
				return 37
			}
			zxfer_get_process_start_token() {
				: >"$token_marker"
				return 1
			}
			set +e
			zxfer_open_send_job_completion_queue_fd "$queue_file"
			printf 'status=%s\n' "$?"
			printf 'records=<%s> pids=<%s>\n' \
				"$g_zxfer_cleanup_pid_records" "$g_zxfer_cleanup_pids"
		)
	)

	assertEquals "A FIFO writer-open failure should preserve its original status after reader teardown." \
		"status=37" "$(printf '%s\n' "$output" | sed -n '1p')"
	assertContains "The blocked FIFO reader should be reaped and unregistered." \
		"$output" "records=<> pids=<>"
	assertFalse "Normal FIFO helper ownership must not capture a process start token." \
		"[ -e '$token_marker' ]"
}

test_zxfer_open_send_job_completion_queue_fd_preserves_reader_helper_failure() {
	missing_queue="$TEST_TMPDIR/missing-open-helper/queue"

	set +e
	(
		zxfer_open_send_job_completion_queue_writer_fd() {
			return 0
		}
		zxfer_open_send_job_completion_queue_reader_fd() {
			return 0
		}
		zxfer_open_send_job_completion_queue_fd "$missing_queue" 2>/dev/null
	)
	status=$?

	assertNotEquals "Opening the rolling queue should fail when the FIFO reader helper cannot open the queue path." \
		0 "$status"
}

test_zxfer_open_send_job_completion_queue_writer_fd_opens_write_only() {
	queue_file="$TEST_TMPDIR/open_queue_writer_mode"
	writer_helper=$(sed -n '/^zxfer_open_send_job_completion_queue_writer_fd()/,/^}/p' "$ZXFER_ROOT/src/zxfer_send_jobs.sh")
	printf '%s\n' "existing" >"$queue_file"

	(
		zxfer_open_send_job_completion_queue_writer_fd "$queue_file" || exit 1
		printf '%s\n' "written" >&9 || exit 3
		exec 9>&-
	)
	status=$?

	assertContains "The rolling queue writer helper must use output-only redirection, not POSIX-undefined read/write FIFO opens." \
		"$writer_helper" "{ exec 9>&1; } >\"\$1\""
	assertEquals "The rolling queue writer helper should open fd 9 successfully." \
		0 "$status"
	assertEquals "The write-only helper should still publish bytes through fd 9." \
		"written" "$(cat "$queue_file")"
}

test_zxfer_open_send_job_completion_queue_writer_fd_returns_failure_without_exiting_shell() {
	output=$(
		(
			zxfer_open_send_job_completion_queue_writer_fd "$TEST_TMPDIR/missing-writer/queue" 2>/dev/null
			printf 'status=%s\n' "$?"
			printf 'after=yes\n'
		)
	)

	assertContains "Failed writer opens should return without exiting the shell." \
		"$output" "after=yes"
	assertNotContains "Failed writer opens should not report success." \
		"$output" "status=0"
}

test_zxfer_open_send_job_completion_queue_reader_fd_returns_failure_without_exiting_shell() {
	output=$(
		(
			zxfer_open_send_job_completion_queue_reader_fd "$TEST_TMPDIR/missing-reader/queue" 2>/dev/null
			printf 'status=%s\n' "$?"
			printf 'after=yes\n'
		)
	)

	assertContains "Failed reader opens should return without exiting the shell." \
		"$output" "after=yes"
	assertNotContains "Failed reader opens should not report success." \
		"$output" "status=0"
}

test_zxfer_open_send_job_completion_queue_fd_round_trips_fifo_notification() {
	queue_dir="$TEST_TMPDIR/open_queue_fifo"
	queue_fifo="$queue_dir/queue"
	mkdir "$queue_dir"
	mkfifo "$queue_fifo"

	(
		zxfer_open_send_job_completion_queue_fd "$queue_fifo" || exit 1
		printf '%s\n' "job-1" >&9 || exit 2
		IFS= read -r line <&8 || exit 3
		[ "$line" = "job-1" ] || exit 4
		exec 9>&-
		exec 8<&-
	)
	status=$?

	assertEquals "The rolling queue should open a FIFO portably and pass notifications between fd 9 and fd 8." \
		0 "$status"
}

# Regression: closing the rolling-queue descriptors must never redirect the
# main shell's stderr. The old `exec 9>&- 2>/dev/null` applied BOTH
# redirections to the shell permanently, so from the first rolling wait on,
# every later warning and failure report in a -j run vanished into /dev/null
# (including the post-receive divergence verification error).
test_zxfer_close_send_job_completion_queue_fds_keeps_shell_stderr_attached() {
	stderr_probe_file="$TEST_TMPDIR/queue_close_stderr_probe.out"

	(
		exec 9>/dev/null 8</dev/null
		g_zfs_send_job_queue_writer_open=1
		g_zfs_send_job_queue_open=1
		g_zfs_send_job_queue_dir=""
		g_zfs_send_job_queue_path=""

		zxfer_close_send_job_completion_queue

		printf 'stderr-still-attached\n' >&2
	) 2>"$stderr_probe_file"

	assertEquals "Closing the rolling-queue reader and writer descriptors must leave the shell's stderr attached." \
		"stderr-still-attached" "$(cat "$stderr_probe_file")"
}

test_zxfer_close_send_job_completion_queue_cleans_orphaned_queue_paths() {
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	queue_path="$g_zxfer_run_tmp_root/orphaned-queue"
	: >"$queue_path"
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_dir=""
	g_zfs_send_job_queue_path="$queue_path"
	zxfer_close_send_job_completion_queue

	assertFalse "Closing remembered rolling queues should clean an orphaned queue path when no queue directory was tracked." \
		"[ -e '$queue_path' ]"
	assertEquals "Closing remembered rolling queues should clear the stored queue path." \
		"" "${g_zfs_send_job_queue_path:-}"
}

test_supervised_send_job_helpers_collect_unregister_and_report_missing_jobs() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	zxfer_register_supervised_send_job "job-1" 101 "tank/src@snap1" "backup/dst" ""
	zxfer_register_supervised_send_job "job-2" 202 "tank/src/child@snap1" "backup/dst/child" ""
	zxfer_register_supervised_send_job "job-3" 303 "tank/other@snap1" "backup/other" "target.example"
	ids=$(zxfer_collect_supervised_send_job_ids)
	found=$(zxfer_find_supervised_send_job_pid_by_job_id "job-2")
	set +e
	zxfer_find_supervised_send_job_pid_by_job_id "job-missing" >/dev/null
	missing_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi
	zxfer_unregister_supervised_send_job "job-2"

	assertEquals "Collecting supervised send-job ids should preserve their registration order." \
		"job-1
job-2
job-3" "$ids"
	assertEquals "Supervised send-job lookup should resolve tracked job ids back to their runner pid." \
		202 "$found"
	assertEquals "Supervised send-job lookup should fail for unknown job ids." \
		1 "$missing_status"
	assertEquals "Unregistering one supervised send job should preserve earlier and later tracked runner pids." \
		"101 303" "${g_zfs_send_job_pids:-}"
	expected_records=$(printf 'job-1\t101\ttank/src@snap1\tbackup/dst\t\njob-3\t303\ttank/other@snap1\tbackup/other\ttarget.example')
	assertEquals "Unregistering one supervised send job should preserve earlier and later tracked records." \
		"$expected_records" "${g_zfs_send_job_supervisor_records:-}"
	assertEquals "Unregistering one supervised send job should decrement the tracked job count." \
		2 "${g_count_zfs_send_jobs:-0}"
}

test_supervised_send_job_helpers_track_metadata_conflicts_and_render_context() {
	zxfer_register_supervised_send_job "job-1" 101 "tank/src@snap2" "backup/dst" ""
	zxfer_register_supervised_send_job "job-2" 202 "tank/other@snap9" "backup/other" "target.example"

	if ! zxfer_find_supervised_send_job_record "job-1"; then
		fail "Expected to find the registered supervised send job."
	fi
	assertEquals "Tracked supervised send jobs should preserve the source dataset metadata." \
		"tank/src" "$g_zxfer_send_job_record_source_dataset"
	assertEquals "Tracked supervised send jobs should preserve the source snapshot metadata." \
		"tank/src@snap2" "$g_zxfer_send_job_record_source_snapshot"
	assertEquals "Tracked supervised send jobs should preserve the destination dataset metadata." \
		"backup/dst" "$g_zxfer_send_job_record_dest_dataset"

	if ! zxfer_supervised_send_job_conflicts_with_destination "" "backup/dst/child"; then
		fail "Expected ancestor and descendant destination datasets to conflict."
	fi
	assertEquals "Destination-ancestry conflict detection should expose the conflicting active destination dataset." \
		"backup/dst" "$g_zxfer_send_job_conflict_dest_dataset"
	if ! zxfer_dataset_paths_conflict_by_ancestry "backup/dst/child" "backup/dst"; then
		fail "Expected descendant and ancestor destination datasets to conflict regardless of argument order."
	fi

	if zxfer_supervised_send_job_conflicts_with_destination "" "backup/unrelated"; then
		fail "Unrelated local destination datasets should not conflict."
	fi
	if zxfer_supervised_send_job_conflicts_with_destination "" "backup/other/child"; then
		fail "Different target-host contexts should not conflict with local destinations."
	fi

	assertEquals "Dataset-aware send-job error contexts should identify the tracked source snapshot and destination dataset." \
		"[tank/src@snap2 -> backup/dst]" "$(zxfer_get_supervised_send_job_error_context "job-1")"
	assertEquals "Dataset-aware send-job error contexts should include the target host when present." \
		"[tank/other@snap9 -> backup/other] on target [target.example]" "$(zxfer_get_supervised_send_job_error_context "job-2")"
}

test_wait_for_next_zfs_send_job_completion_dispatches_to_supervised_handler() {
	output=$(
		(
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_next_supervised_zfs_send_job_completion() {
				printf 'supervised=%s\n' "$1"
			}
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)

	assertContains "Rolling completion waits should delegate to the supervised handler when supervised jobs are tracked." \
		"$output" "supervised=unit"
}

test_zxfer_terminate_remaining_send_jobs_aborts_supervised_jobs_and_clears_state() {
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	queue_dir="$g_zxfer_run_tmp_root/terminate-supervised-queue"
	mkdir "$queue_dir"
	queue_path=$queue_dir/queue
	: >"$queue_path"

	output=$(
		(
			zxfer_register_supervised_send_job "job-1" 101 "tank/src@snap1" "backup/dst" ""
			zxfer_register_supervised_send_job "job-2" 202 "tank/src@snap2" "backup/other" ""
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_path=$queue_path
			g_zfs_send_job_queue_dir=$queue_dir
			zxfer_abort_background_job() {
				printf 'abort:%s:%s\n' "$1" "$2"
			}
			zxfer_terminate_remaining_send_jobs
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_supervisor_records:-}"
			printf 'queue_open=%s\n' "${g_zfs_send_job_queue_open:-0}"
			printf 'dir_exists=%s\n' "$([ -e "$queue_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Terminating supervised send jobs should abort each tracked job id through the supervisor." \
		"$output" "abort:job-1:TERM"
	assertContains "Terminating supervised send jobs should abort later tracked job ids too." \
		"$output" "abort:job-2:TERM"
	assertContains "Terminating supervised send jobs should clear the tracked job count." \
		"$output" "count=0"
	assertContains "Terminating supervised send jobs should clear the tracked runner pid list." \
		"$output" "pids=<>"
	assertContains "Terminating supervised send jobs should clear the tracked supervisor records." \
		"$output" "records=<>"
	assertContains "Terminating supervised send jobs should close the rolling queue." \
		"$output" "queue_open=0"
	assertContains "Terminating supervised send jobs should remove the rolling queue directory." \
		"$output" "dir_exists=no"
}

test_zxfer_terminate_remaining_send_jobs_returns_failure_when_supervised_id_collection_fails() {
	output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_collect_supervised_send_job_ids() {
				return 37
			}
			set +e
			zxfer_terminate_remaining_send_jobs
			status=$?
			set -e
			printf 'status=%s\n' "$status"
		)
	)

	assertContains "Supervised teardown should preserve tracked job-id collection failures." \
		"$output" "status=37"
}

test_zxfer_terminate_remaining_send_jobs_returns_failure_when_supervised_abort_fails() {
	output=$(
		(
			zxfer_register_supervised_send_job "job-1" 101
			zxfer_abort_background_job() {
				return 38
			}
			set +e
			zxfer_terminate_remaining_send_jobs
			status=$?
			set -e
			printf 'status=%s\n' "$status"
		)
	)

	assertContains "Supervised teardown should preserve tracked supervised-job abort failures." \
		"$output" "status=38"
}

test_zxfer_terminate_remaining_send_jobs_continues_after_supervised_abort_failures_and_preserves_first_message() {
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	queue_dir="$g_zxfer_run_tmp_root/terminate-supervised-abort-queue"
	mkdir "$queue_dir"
	queue_path=$queue_dir/queue
	: >"$queue_path"

	output=$(
		(
			zxfer_register_supervised_send_job "job-1" 101
			zxfer_register_supervised_send_job "job-2" 202
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_path=$queue_path
			g_zfs_send_job_queue_dir=$queue_dir
			zxfer_abort_background_job() {
				printf 'abort:%s:%s\n' "$1" "$2"
				if [ "$1" = "job-1" ]; then
					g_zxfer_background_job_abort_failure_message="first supervised abort failed"
					return 39
				fi
				return 0
			}
			set +e
			zxfer_terminate_remaining_send_jobs
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'message=%s\n' "${g_zxfer_background_job_abort_failure_message:-}"
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_supervisor_records:-}"
			printf 'queue_open=%s\n' "${g_zfs_send_job_queue_open:-0}"
			printf 'dir_exists=%s\n' "$([ -e "$queue_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Supervised teardown should still attempt to abort the first tracked job when the aggregate pass fails." \
		"$output" "abort:job-1:TERM"
	assertContains "Supervised teardown should continue aborting later tracked jobs after an earlier abort failure." \
		"$output" "abort:job-2:TERM"
	assertContains "Supervised teardown should preserve the first abort failure status after the aggregate pass." \
		"$output" "status=39"
	assertContains "Supervised teardown should preserve the first abort failure message after the aggregate pass." \
		"$output" "message=first supervised abort failed"
	assertContains "Supervised teardown should keep only the failed job tracked after later jobs abort successfully." \
		"$output" "count=1"
	assertContains "Supervised teardown should preserve only the failed job pid after later jobs abort successfully." \
		"$output" "pids=<101>"
	assertContains "Supervised teardown should preserve only the failed supervisor record after later jobs abort successfully." \
		"$output" "records=<job-1"
	assertContains "Supervised teardown should still close the rolling queue after an aggregate abort failure." \
		"$output" "queue_open=0"
	assertContains "Supervised teardown should still remove the rolling queue directory after an aggregate abort failure." \
		"$output" "dir_exists=no"
}

test_zxfer_terminate_remaining_send_jobs_preserves_first_defensive_pid_abort_failure() {
	output=$(
		(
			g_zfs_send_job_pids="101 202"
			g_count_zfs_send_jobs=2
			IFS=:
			set -f
			zxfer_abort_cleanup_pid() {
				if [ "$1" = "101" ]; then
					g_zxfer_cleanup_pid_abort_failure_message="first send-job abort failed"
					return 40
				fi
				return 0
			}
			zxfer_close_send_job_completion_queue() {
				printf '%s\n' "queue-closed"
			}
			zxfer_terminate_remaining_send_jobs
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "$g_zxfer_cleanup_pid_abort_failure_message"
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'ifs=%s\n' "$IFS"
			printf 'flags=%s\n' "$-"
		)
	)

	assertContains "Defensive PID teardown should preserve the first validated cleanup abort failure status." \
		"$output" "status=40"
	assertContains "Defensive PID teardown should preserve the first validated cleanup abort failure message." \
		"$output" "message=first send-job abort failed"
	assertContains "Defensive PID teardown should still close the rolling completion queue after an abort failure." \
		"$output" "queue-closed"
	assertContains "Defensive PID teardown should still clear the tracked state after an abort failure." \
		"$output" "count=0"
	assertContains "Defensive PID teardown should still clear the tracked pid list after an abort failure." \
		"$output" "pids=<>"
	assertContains "Defensive PID teardown should preserve a caller-defined IFS." \
		"$output" "ifs=:"
	assertContains "Defensive PID teardown should preserve disabled globbing." \
		"$(printf '%s\n' "$output" | sed -n 's/^flags=//p')" "f"
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_succeeds_for_the_last_tracked_job() {
	queue_file="$TEST_TMPDIR/supervised_wait_success.queue"
	printf '%s\n' "job-1" >"$queue_file"

	output=$(
		(
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_success.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_zfs_send_job_queue_path=$queue_file
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="101"
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=0
				g_zxfer_background_job_wait_report_failure=""
			}
			zxfer_note_destination_dataset_exists() {
				printf 'noted=%s\n' "$1"
			}
			zxfer_invalidate_destination_property_mutation_cache() {
				printf 'properties=%s\n' "$1"
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_supervisor_records:-}"
			printf 'queue_open=%s\n' "${g_zfs_send_job_queue_open:-0}"
		)
	)

	assertContains "Successful supervised rolling waits should repair the destination-existence cache for the completed destination dataset." \
		"$output" "noted=backup/dst"
	assertContains "Successful supervised rolling waits should invalidate destination property caches for the completed destination dataset." \
		"$output" "properties=backup/dst"
	assertContains "Supervised rolling waits should decrement the tracked job count after a successful completion." \
		"$output" "count=0"
	assertContains "Supervised rolling waits should clear the tracked runner pid list after the last job completes." \
		"$output" "pids=<>"
	assertContains "Supervised rolling waits should clear the tracked supervisor registry after the last job completes." \
		"$output" "records=<>"
	assertContains "Supervised rolling waits should close the rolling queue after the last job completes." \
		"$output" "queue_open=0"
}

test_zxfer_wait_for_supervised_zfs_send_jobs_batch_repairs_destination_state_on_success() {
	output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=0
				g_zxfer_background_job_wait_report_failure=""
			}
			zxfer_note_destination_dataset_exists() {
				printf 'noted=%s\n' "$1"
			}
			zxfer_invalidate_destination_property_mutation_cache() {
				printf 'properties=%s\n' "$1"
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_supervisor_records:-}"
		)
	)

	assertContains "Successful supervised batch waits should repair the destination-existence cache for the completed destination dataset." \
		"$output" "noted=backup/dst"
	assertContains "Successful supervised batch waits should invalidate destination property caches for the completed destination dataset." \
		"$output" "properties=backup/dst"
	assertContains "Successful supervised batch waits should clear the tracked job count after draining the batch." \
		"$output" "count=0"
	assertContains "Successful supervised batch waits should clear the tracked runner pid list after draining the batch." \
		"$output" "pids=<>"
	assertContains "Successful supervised batch waits should clear the tracked supervisor registry after draining the batch." \
		"$output" "records=<>"
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_rejects_blank_notifications() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_blank.queue"
			printf '\n' >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_blank.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Supervised rolling waits should fail closed when a completion notification is blank." \
		1 "$status"
	assertContains "Malformed supervised notifications should terminate remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Malformed supervised notifications should surface the documented parse failure." \
		"$output" "Failed to parse a completed zfs send/receive job notification."
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_unknown_job_ids() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_unknown.queue"
			printf '%s\n' "job-missing" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_unknown.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Supervised rolling waits should fail closed when a completed job id is not tracked." \
		1 "$status"
	assertContains "Unknown supervised completion ids should terminate remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Unknown supervised completion ids should surface the documented matching failure." \
		"$output" "Failed to match a completed zfs send/receive job to a tracked PID."
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_metadata_failures() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_metadata_fail.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_metadata_fail.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				return 1
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Supervised rolling waits should fail closed when completion metadata cannot be read." \
		1 "$status"
	assertContains "Supervised metadata read failures should terminate remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Supervised metadata read failures should preserve the dedicated operator-facing error." \
		"$output" "Failed to read zfs send/receive completion metadata for [tank/src@snap2 -> backup/dst]."
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_failure_markers_and_nonzero_exits() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	queue_write_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_queue_write.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_queue_write.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="queue_write"
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	queue_write_status=$?
	completion_write_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_completion_write.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_completion_write.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="completion_write"
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	completion_write_status=$?
	exit_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_nonzero.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_nonzero.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=9
				g_zxfer_background_job_wait_report_failure=""
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	exit_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Supervised rolling waits should fail closed when a job records a queue-write failure marker." \
		1 "$queue_write_status"
	assertContains "Queue-write failures should preserve the publish-failure error." \
		"$queue_write_output" "Failed to publish zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 101, exit 7)."
	assertEquals "Supervised rolling waits should fail closed when a job records a completion-write failure marker." \
		1 "$completion_write_status"
	assertContains "Completion-write failures should preserve the completion-report error." \
		"$completion_write_output" "Failed to report zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 101, exit 7)."
	assertEquals "Supervised rolling waits should fail closed when the completed job exits nonzero." \
		1 "$exit_status"
	assertContains "Nonzero supervised job exits should preserve the operator-facing failure." \
		"$exit_output" "zfs send/receive job failed for [tank/src@snap2 -> backup/dst] (PID 101, exit 9)."
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_when_queue_is_unavailable() {
	output=$(
		(
			g_count_zfs_send_jobs=1
			g_zfs_send_job_queue_open=0
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_zfs_send_jobs() {
				printf 'fallback=%s\n' "$1"
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)

	assertContains "Supervised rolling waits should fall back to the batch wait path when the rolling queue is unavailable." \
		"$output" "fallback=unit"
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_to_batch_wait_on_queue_read_failure() {
	queue_dir="$TEST_TMPDIR/supervised_wait_read_fail.dir"
	queue_file="$queue_dir/completion.queue"
	mkdir -p "$queue_dir"
	: >"$queue_file"

	output=$(
		(
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_read_fail.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_zfs_send_job_queue_path=$queue_file
			g_zfs_send_job_queue_dir=$queue_dir
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_supervised_zfs_send_jobs_batch() {
				printf 'supervised-batch\n'
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
			printf 'unavailable=%s\n' "${g_zfs_send_job_queue_unavailable:-0}"
		)
	)

	assertContains "Supervised rolling waits should fall back to the supervised batch path when the queue reader hits EOF." \
		"$output" "supervised-batch"
	assertContains "Queue reader failures should mark the rolling queue unavailable." \
		"$output" "unavailable=1"
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_write_failed_notifications_and_abort_failures() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	record_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_completion_marker.queue"
			printf '%s\n' "completion_write_failed	job-1	bad" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_completion_marker.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=0
				g_zxfer_background_job_wait_report_failure=""
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	record_status=$?
	abort_failure_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_abort_failure.queue"
			printf '\n' >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			g_zxfer_background_job_abort_failure_message="abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	abort_failure_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Supervised rolling waits should fail closed when the runner reports a completion-write failure marker." \
		1 "$record_status"
	assertContains "Completion-write failure notifications should terminate remaining jobs before aborting." \
		"$record_output" "terminated"
	assertContains "Completion-write failure notifications should normalize malformed marker statuses to 125." \
		"$record_output" "Failed to record zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 101, exit 125)."
	assertEquals "Supervised rolling waits should surface supervisor abort failures when cleanup itself fails." \
		1 "$abort_failure_status"
	assertContains "Supervisor abort failures should preserve the dedicated abort failure message." \
		"$abort_failure_output" "abort failed"
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_unknown_job_errors() {
	set +e
	output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_unknown_abort_failure.queue"
			printf '%s\n' "job-missing" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_unknown_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	status=$?
	set -e

	assertEquals "Supervised rolling waits should surface supervisor cleanup-abort failures before unknown-job errors." \
		1 "$status"
	assertContains "Supervised rolling waits should preserve the supervisor cleanup-abort failure message before the unknown-job error." \
		"$output" "supervised cleanup abort failed"
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_metadata_and_failure_markers() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	metadata_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_metadata_abort_failure.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_metadata_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			zxfer_wait_for_background_job() {
				return 1
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	metadata_status=$?
	record_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_completion_marker_abort_failure.queue"
			printf '%s\n' "completion_write_failed	job-1	7" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_completion_marker_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=0
				g_zxfer_background_job_wait_report_failure=""
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	record_status=$?
	queue_write_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_queue_write_abort_failure.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_queue_write_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="queue_write"
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	queue_write_status=$?
	completion_write_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_completion_write_abort_failure.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_completion_write_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="completion_write"
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	completion_write_status=$?
	exit_output=$(
		(
			queue_file="$TEST_TMPDIR/supervised_wait_nonzero_abort_failure.queue"
			printf '%s\n' "job-1" >"$queue_file"
			exec 8<"$queue_file"
			exec 9>"$TEST_TMPDIR/supervised_wait_nonzero_abort_failure.writer"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_count_zfs_send_jobs=1
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=9
				g_zxfer_background_job_wait_report_failure=""
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
		)
	)
	exit_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Supervised rolling waits should surface supervisor cleanup-abort failures before metadata-read errors." \
		1 "$metadata_status"
	assertContains "Supervised rolling waits should preserve the supervisor cleanup-abort failure message before metadata-read errors." \
		"$metadata_output" "supervised cleanup abort failed"
	assertEquals "Supervised rolling waits should surface supervisor cleanup-abort failures before completion-write marker errors." \
		1 "$record_status"
	assertContains "Supervised rolling waits should preserve the supervisor cleanup-abort failure message before completion-write marker errors." \
		"$record_output" "supervised cleanup abort failed"
	assertEquals "Supervised rolling waits should surface supervisor cleanup-abort failures before queue-write marker errors." \
		1 "$queue_write_status"
	assertContains "Supervised rolling waits should preserve the supervisor cleanup-abort failure message before queue-write marker errors." \
		"$queue_write_output" "supervised cleanup abort failed"
	assertEquals "Supervised rolling waits should surface supervisor cleanup-abort failures before completion-report errors." \
		1 "$completion_write_status"
	assertContains "Supervised rolling waits should preserve the supervisor cleanup-abort failure message before completion-report errors." \
		"$completion_write_output" "supervised cleanup abort failed"
	assertEquals "Supervised rolling waits should surface supervisor cleanup-abort failures before nonzero-exit errors." \
		1 "$exit_status"
	assertContains "Supervised rolling waits should preserve the supervisor cleanup-abort failure message before nonzero-exit errors." \
		"$exit_output" "supervised cleanup abort failed"
}

test_zxfer_wait_for_supervised_zfs_send_jobs_batch_collects_ids_and_reports_failures() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	collect_output=$(
		(
			zxfer_collect_supervised_send_job_ids() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	collect_status=$?
	wait_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				return 1
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	wait_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Batch supervised waits should fail closed when they cannot collect the tracked job ids." \
		1 "$collect_status"
	assertContains "Batch supervised waits should preserve the collection failure message." \
		"$collect_output" "Failed to collect supervised send/receive job ids."
	assertEquals "Batch supervised waits should fail closed when a tracked job's completion metadata cannot be read." \
		1 "$wait_status"
	assertContains "Batch supervised waits should terminate remaining jobs before aborting on metadata failures." \
		"$wait_output" "terminated"
	assertContains "Batch supervised waits should preserve the metadata-read failure message." \
		"$wait_output" "Failed to read zfs send/receive completion metadata for [tank/src@snap2 -> backup/dst]."
}

test_zxfer_wait_for_supervised_zfs_send_jobs_batch_reports_failure_markers_and_nonzero_exits() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	queue_write_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="queue_write"
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	queue_write_status=$?
	completion_write_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="completion_write"
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	completion_write_status=$?
	exit_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101	tank/src@snap2	backup/dst	"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=9
				g_zxfer_background_job_wait_report_failure=""
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	exit_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Batch supervised waits should fail closed when a job reports a queue-write failure marker." \
		1 "$queue_write_status"
	assertContains "Batch queue-write failures should preserve the publish failure message." \
		"$queue_write_output" "Failed to publish zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 101, exit 7)."
	assertEquals "Batch supervised waits should fail closed when a job reports a completion-write failure marker." \
		1 "$completion_write_status"
	assertContains "Batch completion-write failures should preserve the completion report failure message." \
		"$completion_write_output" "Failed to report zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 101, exit 7)."
	assertEquals "Batch supervised waits should fail closed when a job exits nonzero." \
		1 "$exit_status"
	assertContains "Batch nonzero exits should preserve the operator-facing failure." \
		"$exit_output" "zfs send/receive job failed for [tank/src@snap2 -> backup/dst] (PID 101, exit 9)."
}

test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_failure_markers() {
	set +e
	queue_write_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="queue_write"
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	queue_write_status=$?
	exit_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=9
				g_zxfer_background_job_wait_report_failure=""
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	exit_status=$?
	set -e

	assertEquals "Batch supervised waits should surface supervisor cleanup-abort failures before queue-write marker errors." \
		1 "$queue_write_status"
	assertContains "Batch supervised waits should preserve the supervisor cleanup-abort failure message before the queue-write marker error." \
		"$queue_write_output" "supervised cleanup abort failed"
	assertEquals "Batch supervised waits should surface supervisor cleanup-abort failures before nonzero-exit errors." \
		1 "$exit_status"
	assertContains "Batch supervised waits should preserve the supervisor cleanup-abort failure message before the nonzero-exit error." \
		"$exit_output" "supervised cleanup abort failed"
}

test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_metadata_and_completion_report_errors() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	metadata_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				return 1
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	metadata_status=$?
	completion_write_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101"
			g_zfs_send_job_pids="101"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=7
				g_zxfer_background_job_wait_report_failure="completion_write"
			}
			g_zxfer_background_job_abort_failure_message="supervised cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
		)
	)
	completion_write_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Batch supervised waits should surface supervisor cleanup-abort failures before metadata-read errors." \
		1 "$metadata_status"
	assertContains "Batch supervised waits should preserve the supervisor cleanup-abort failure message before metadata-read errors." \
		"$metadata_output" "supervised cleanup abort failed"
	assertEquals "Batch supervised waits should surface supervisor cleanup-abort failures before completion-report errors." \
		1 "$completion_write_status"
	assertContains "Batch supervised waits should preserve the supervisor cleanup-abort failure message before completion-report errors." \
		"$completion_write_output" "supervised cleanup abort failed"
}

test_zxfer_wait_for_zfs_send_jobs_dispatches_rolling_and_batch_supervisor_paths() {
	output=$(
		(
			g_zfs_send_job_pids="101 202"
			g_zfs_send_job_supervisor_records="job-1	101
job-2	202"
			g_zfs_send_job_queue_open=1
			g_count_zfs_send_jobs=2
			zxfer_wait_for_next_supervised_zfs_send_job_completion() {
				printf 'supervised_next:%s\n' "$1"
				g_count_zfs_send_jobs=$((g_count_zfs_send_jobs - 1))
			}
			zxfer_close_send_job_completion_queue() {
				printf 'closed_supervised\n'
				g_zfs_send_job_queue_open=0
			}
			zxfer_wait_for_zfs_send_jobs "unit"
			g_zfs_send_job_pids="505"
			g_zfs_send_job_supervisor_records="job-5	505"
			g_zfs_send_job_queue_open=0
			g_count_zfs_send_jobs=1
			zxfer_wait_for_supervised_zfs_send_jobs_batch() {
				printf 'supervised_batch\n'
			}
			zxfer_wait_for_zfs_send_jobs "unit"
		)
	)

	assertContains "Queued supervised waits should drain through the rolling single-job helper." \
		"$output" "supervised_next:"
	assertContains "Queued supervised waits should close the queue after draining." \
		"$output" "closed_supervised"
	assertContains "Non-queued supervised waits should fall back to the batch helper." \
		"$output" "supervised_batch"
}
