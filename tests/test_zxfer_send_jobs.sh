#!/bin/sh
#
# shunit2 tests for the send/receive job supervision in src/zxfer_send_jobs.sh.
# The background-shell helpers it runs jobs in (src/zxfer_exec.sh) are tested
# in tests/suites/zxfer_exec_background_shell_tests.sh.
#
# Pins: per-job status propagation (success, failure, a job shell that dies
# before recording its status), the -j job limit, destination-ancestry
# serialization, foreground receives finishing in the scheduler, abort
# teardown of the whole job pipeline (TERM before KILL, first failure kept),
# and that a job whose status is written never has its recycled PID
# signalled.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/send_job_fixtures.sh
. "$TESTS_DIR/helpers/send_job_fixtures.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_plan.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_send_jobs"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_send_job_fixture_setup
}

# Stub the post-receive hooks so reap-time bookkeeping is observable without
# live snapshot state.
bgjob_test_stub_finalize_hooks() {
	zxfer_note_destination_receive_completed() {
		printf 'completed %s\n' "$1" >>"${BGJOB_HOOK_LOG:?}"
	}
	zxfer_invalidate_destination_property_mutation_cache() {
		printf 'invalidate %s\n' "$*" >>"${BGJOB_HOOK_LOG:?}"
	}
	zxfer_verify_converged_destination_after_receive() {
		printf 'verify %s\n' "$1" >>"${BGJOB_HOOK_LOG:?}"
	}
}

# Every group probe and signal uses `kill -SIG -PGID`: dash rejects
# `kill -s SIG -PGID`, and BusyBox ash reads the `--` of
# `kill -s SIG -- -PGID` as a PID, signals the group, and exits 1.
test_process_group_kills_use_the_signal_first_form() {
	status_file="$TEST_TMPDIR/group-kill-form.status"
	printf '0\n' >"$status_file"
	output=$(
		kill() {
			printf 'kill'
			for l_test_kill_arg; do
				printf ' <%s>' "$l_test_kill_arg"
			done
			printf '\n'
			return 1
		}
		zxfer_signal_background_shell 4242 pgid TERM
		zxfer_register_cleanup_pid 4243 "group fixture" pgid
		g_zxfer_cleanup_pid_records="4244	producer fixture	pgid"
		zxfer_kill_reaped_producer_group 4244
		zxfer_signal_send_job 4245 pgid "$status_file" KILL
	)

	assertContains "A group signal names the signal first." \
		"$output" "kill <-TERM> <-4242>"
	assertContains "A group probe uses the same form." \
		"$output" "kill <-0> <-4242>"
	assertContains "Registration probes the group in the same form." \
		"$output" "kill <-0> <-4243>"
	assertContains "A reaped producer's group gets KILL in the same form." \
		"$output" "kill <-KILL> <-4244>"
	assertContains "An exited job's group gets its signal in the same form." \
		"$output" "kill <-KILL> <-4245>"
	assertNotContains "No group kill passes -- after a signal option." \
		"$output" "<-->"
}

test_send_job_conflicts_with_destination_matches_equal_and_nested_paths() {
	g_zxfer_send_jobs="job	401	backup/a	/status/a	tank/a@snap	pgid"
	zxfer_send_job_conflicts_with_destination backup/a
	assertEquals "equal paths conflict and publish the active destination" \
		"0 backup/a" "$? $g_zxfer_send_job_conflict_dest_dataset"
	assertTrue "a descendant conflicts with its active ancestor" \
		"zxfer_send_job_conflicts_with_destination backup/a/b"
	assertFalse "siblings do not conflict" \
		"zxfer_send_job_conflicts_with_destination backup/b"
	assertFalse "a name prefix without a separator does not conflict" \
		"zxfer_send_job_conflicts_with_destination backup/ab"
	assertFalse "an empty destination never conflicts" \
		"zxfer_send_job_conflicts_with_destination ''"
	g_zxfer_send_jobs="job	401	backup/a/b/c	/status/a	tank/a@snap	pgid"
	assertTrue "an ancestor conflicts with its active descendant" \
		"zxfer_send_job_conflicts_with_destination backup/a"
	g_zxfer_send_jobs=""
	assertFalse "no active job never conflicts" \
		"zxfer_send_job_conflicts_with_destination backup/a"
}

test_spawn_send_job_records_status_and_finalizes_on_success() {
	hook_log="$TEST_TMPDIR/bgjob_success.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_spawn_send_job "printf ok >/dev/null" "tank/a@snap2" "backup/a"
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			zxfer_wait_for_zfs_send_jobs "unit"
			printf 'count=%s jobs=<%s>\n' "$g_count_zfs_send_jobs" "$g_zxfer_send_jobs"
		)
	)

	assertEquals "A spawned job counts while active and the list is empty after reaping." \
		"count=1
count=0 jobs=<>" "$output"
	assertEquals "A successful receive should run the three post-receive hooks for its destination." \
		"completed backup/a
invalidate backup/a exact
verify backup/a" "$(cat "$hook_log")"
}

# ksh93 (illumos /bin/sh) reports a signal death as 256+N; the job shell
# records the usual 128+N, so the reaper reports the signal instead of a job
# shell that died before recording its status.
test_spawn_send_job_records_signal_deaths_as_128_plus_n_under_ksh93() {
	case $(command -v ksh 2>/dev/null) in
	/*) ;;
	*)
		startSkipping
		assertTrue "No ksh is installed." true
		endSkipping
		return
		;;
	esac
	status_file="$TEST_TMPDIR/bgjob_ksh.status"
	rm -f "$status_file"
	job_shell=$(
		zxfer_spawn_background_shell() { printf '%s' "$1"; }
		zxfer_spawn_send_job 'sh -c "kill -s KILL \$\$"' tank/a@snap backup/a
	)

	ksh -c "$job_shell" zxfer-job "$status_file" 2>/dev/null

	assertEquals "A pipeline killed by KILL records status 137." 137 "$(cat "$status_file")"
}

test_reap_send_job_failure_aborts_remaining_jobs_and_throws_structured_error() {
	hook_log="$TEST_TMPDIR/bgjob_failure.hooks"
	pid_file="$TEST_TMPDIR/bgjob_failure.sleeper"
	: >"$hook_log"
	rm -f "$pid_file"
	set +e
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			g_option_T_target_host="operator@target"
			zxfer_throw_error() {
				printf 'message=%s\nstatus=%s\nstage=%s\n' "$1" "${2:-1}" "$g_zxfer_failure_stage"
				printf 'jobs=<%s> count=%s\n' "$g_zxfer_send_jobs" "$g_count_zfs_send_jobs"
				exit "${2:-1}"
			}
			zxfer_spawn_send_job "sh -c 'echo \$\$ >'\"$pid_file\"'; exec sleep 30'" "tank/b@snap2" "backup/b"
			zxfer_spawn_send_job "exit 3" "tank/a@snap2" "backup/a"
			zxfer_wait_for_zfs_send_jobs "unit"
		)
	)
	status=$?
	set +e

	assertEquals "The job's exit status becomes the thrown status." 3 "$status"
	assertContains "The failure names the snapshot, destination, target, pid, and exit status." \
		"$output" "message=zfs send/receive job failed for [tank/a@snap2 -> backup/a] on target [operator@target] (PID "
	assertContains "The failure carries the job's exit status." "$output" ", exit 3)."
	assertContains "The failure is reported at the send/receive stage." "$output" "stage=send/receive"
	assertContains "Every other job is torn down before the throw." "$output" "jobs=<> count=0"
	assertEquals "A failed job runs no post-receive hook." "" "$(cat "$hook_log")"
	sleeper_pid=$(cat "$pid_file" 2>/dev/null || :)
	assertNotNull "The sleeping pipeline must have started before the abort." "$sleeper_pid"
	assertTrue "The sleeping pipeline of the other job must be torn down." \
		"bgjob_test_wait_for_pid_exit '$sleeper_pid'"
}

test_reap_send_job_reports_a_job_shell_that_died_before_recording_status() {
	hook_log="$TEST_TMPDIR/bgjob_abnormal.hooks"
	: >"$hook_log"
	set +e
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				exit "${2:-1}"
			}
			# $$ inside the pipeline is the job shell itself.
			zxfer_spawn_send_job 'kill -s KILL "$$"' "tank/a@snap2" "backup/a"
			zxfer_wait_for_zfs_send_jobs "unit"
		)
	)
	status=$?
	set +e

	assertEquals "A job shell that never records its status fails closed with 125." 125 "$status"
	assertContains "The abnormal death is reported as a send/receive job failure." \
		"$output" "message=zfs send/receive job failed for [tank/a@snap2 -> backup/a] (PID "
	assertContains "The abnormal death carries the fail-closed status." "$output" ", exit 125)."
}

test_wait_for_any_send_job_reaps_the_finished_job_and_keeps_the_rest() {
	hook_log="$TEST_TMPDIR/bgjob_any.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_spawn_send_job "sleep 30" "tank/slow@snap2" "backup/slow"
			zxfer_spawn_send_job "exit 0" "tank/fast@snap2" "backup/fast"
			zxfer_wait_for_any_send_job "job limit"
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			if zxfer_send_job_conflicts_with_destination backup/slow; then
				printf 'slow=active\n'
			else
				printf 'slow=missing\n'
			fi
			zxfer_abort_all_send_jobs
			printf 'after_abort=%s\n' "$g_count_zfs_send_jobs"
		)
	)

	assertEquals "Waiting for any job reaps only the finished one and abort clears the rest." \
		"count=1
slow=active
after_abort=0" "$output"
	assertEquals "Only the finished job runs its post-receive hooks." \
		"completed backup/fast
invalidate backup/fast exact
verify backup/fast" "$(cat "$hook_log")"
}

test_schedule_send_receive_pipeline_runs_and_finishes_foreground_receives() {
	hook_log="$TEST_TMPDIR/bgjob_foreground.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_execute_rendered_shell_command() {
				printf 'exec %s\n' "$1" >>"$BGJOB_HOOK_LOG"
			}
			g_is_performed_send_destroy=0
			g_option_j_jobs=1
			zxfer_schedule_send_receive_pipeline "send | recv" "tank/a@snap" "backup/a" 1
			g_option_j_jobs=3
			zxfer_schedule_send_receive_pipeline "send | recv" "tank/b@snap" "backup/b" 0
			printf 'count=%s performed=%s\n' "$g_count_zfs_send_jobs" "$g_is_performed_send_destroy"
		)
	)

	assertEquals "Single-job and disallowed pipelines run in the foreground and mark the pass." \
		"count=0 performed=1" "$output"
	assertEquals "Each foreground pipeline runs once, then its receive is finished in place." \
		"exec send | recv
completed backup/a
invalidate backup/a exact
verify backup/a
exec send | recv
completed backup/b
invalidate backup/b exact
verify backup/b" "$(cat "$hook_log")"
}

test_schedule_send_receive_pipeline_waits_at_the_job_limit() {
	hook_log="$TEST_TMPDIR/bgjob_limit.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_echov() { printf '%s\n' "$1"; }
			g_option_j_jobs=2
			zxfer_schedule_send_receive_pipeline "sleep 1" "tank/a@snap" "backup/a" 1
			zxfer_schedule_send_receive_pipeline "sleep 1" "tank/b@snap" "backup/b" 1
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			zxfer_schedule_send_receive_pipeline "exit 0" "tank/c@snap" "backup/c" 1
			if zxfer_send_job_conflicts_with_destination backup/c; then
				printf 'third_started=yes\n'
			fi
			zxfer_wait_for_zfs_send_jobs "unit"
		)
	)

	assertContains "Reaching -j must wait before the next background job starts." \
		"$output" "Max jobs reached [2]. Waiting for jobs to complete."
	assertContains "Two jobs fill a -j 2 pool." "$output" "count=2"
	assertContains "The third job starts once a slot is free." \
		"$output" "third_started=yes"
	assertContains "Every job, the third included, is reaped." \
		"$(cat "$hook_log")" "completed backup/c"
}

test_schedule_send_receive_pipeline_waits_for_destination_ancestry_conflicts() {
	hook_log="$TEST_TMPDIR/bgjob_ancestry.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_echov() { printf '%s\n' "$1"; }
			g_option_j_jobs=4
			zxfer_schedule_send_receive_pipeline "sleep 1" "tank/a@snap" "backup/a" 1
			zxfer_schedule_send_receive_pipeline "exit 0" "tank/b@snap" "backup/b" 1
			printf 'independent_count=%s\n' "$g_count_zfs_send_jobs"
			zxfer_schedule_send_receive_pipeline "exit 0" "tank/a/child@snap" "backup/a/child" 1
			printf 'child_count=%s\n' "$g_count_zfs_send_jobs"
			zxfer_wait_for_zfs_send_jobs "unit"
		)
	)

	assertContains "An independent sibling starts without waiting." \
		"$output" "independent_count=2"
	assertContains "A descendant of an active destination waits for the ancestry conflict." \
		"$output" "Waiting for conflicting zfs send/receive ancestry to finish for destination [backup/a/child]; active destination [backup/a] is still running."
	assertNotContains "Ancestry waits are not job-limit waits." \
		"$output" "Max jobs reached"
	# The child exits at once while its parent sleeps, so only the ancestry
	# wait can finish the parent's receive first.
	assertEquals "The parent receive is reaped before its child starts." \
		"backup/a
backup/a/child" "$(awk '$1 == "completed" && $2 ~ /^backup\/a/ { print $2 }' "$hook_log")"
}

test_abort_all_send_jobs_tears_down_the_whole_pipeline_process_group() {
	zxfer_init_background_shell_spawn_mode
	if [ "$g_zxfer_background_shell_spawn_mode" = wrapper ]; then
		startSkipping
	fi
	first_pid_file="$TEST_TMPDIR/bgjob_group.first"
	last_pid_file="$TEST_TMPDIR/bgjob_group.last"
	rm -f "$first_pid_file" "$last_pid_file"
	output=$(
		(
			zxfer_spawn_send_job \
				"sh -c 'echo \$\$ >'\"$first_pid_file\"'; exec sleep 30' | sh -c 'echo \$\$ >'\"$last_pid_file\"'; exec sleep 30'" \
				"tank/a@snap" "backup/a"
			l_tries=0
			while { [ ! -s "$first_pid_file" ] || [ ! -s "$last_pid_file" ]; } &&
				[ "$l_tries" -lt 100 ]; do
				sleep 0.1 2>/dev/null || sleep 1
				l_tries=$((l_tries + 1))
			done
			zxfer_abort_all_send_jobs
			printf 'count=%s jobs=<%s>\n' "$g_count_zfs_send_jobs" "$g_zxfer_send_jobs"
		)
	)

	assertEquals "Abort clears the job list." "count=0 jobs=<>" "$output"
	first_pid=$(cat "$first_pid_file" 2>/dev/null || :)
	last_pid=$(cat "$last_pid_file" 2>/dev/null || :)
	assertNotNull "The pipeline's first stage must have started." "$first_pid"
	assertNotNull "The pipeline's last stage must have started." "$last_pid"
	assertTrue "The first pipeline stage is torn down with the job's process group." \
		"bgjob_test_wait_for_pid_exit '$first_pid'"
	assertTrue "The last pipeline stage is torn down with the job's process group." \
		"bgjob_test_wait_for_pid_exit '$last_pid'"
}

test_wrapper_spawn_mode_terminates_every_pipeline_stage() {
	first_pid_file="$TEST_TMPDIR/bgjob_wrapper.first"
	last_pid_file="$TEST_TMPDIR/bgjob_plain.last"
	rm -f "$first_pid_file" "$last_pid_file"
	output=$(
		(
			g_zxfer_background_shell_spawn_mode=wrapper
			zxfer_spawn_send_job \
				"sh -c 'echo \$\$ >'\"$first_pid_file\"'; exec sleep 30' | sh -c 'echo \$\$ >'\"$last_pid_file\"'; exec sleep 30'" \
				"tank/a@snap" "backup/a"
			l_tries=0
			while { [ ! -s "$first_pid_file" ] || [ ! -s "$last_pid_file" ]; } && [ "$l_tries" -lt 100 ]; do
				sleep 0.1 2>/dev/null || sleep 1
				l_tries=$((l_tries + 1))
			done
			printf 'scope=%s\n' "${g_zxfer_send_jobs##*	}"
			zxfer_abort_all_send_jobs
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
		)
	)

	assertEquals "Fallback spawns record wrapper scope and abort clears the list." \
		"scope=wrapper
count=0" "$output"
	first_pid=$(cat "$first_pid_file" 2>/dev/null || :)
	last_pid=$(cat "$last_pid_file" 2>/dev/null || :)
	assertNotNull "The pipeline's last stage must have started." "$last_pid"
	assertTrue "The fallback wrapper stops the first pipeline stage without a process group." \
		"bgjob_test_wait_for_pid_exit '$first_pid'"
	assertTrue "The fallback wrapper stops the last pipeline stage without a process group." \
		"bgjob_test_wait_for_pid_exit '$last_pid'"
}

test_abort_all_send_jobs_preserves_first_failure_without_waiting_on_survivors() {
	output=$(
		(
			g_zxfer_send_jobs="first	401	backup/a	/status/a	tank/a@snap	pgid
second	402	backup/b	/status/b	tank/b@snap	pgid"
			g_count_zfs_send_jobs=2
			g_zxfer_send_job_abort_grace_seconds=0
			g_zxfer_send_job_poll_seconds=1
			zxfer_signal_background_shell() {
				[ "$3" != KILL ] || [ "$1" != 401 ] || return 17
			}
			wait() { printf 'wait=%s\n' "$1"; }
			zxfer_abort_all_send_jobs
			printf 'status=%s\ncount=%s\nmessage=%s\n' "$?" "$g_count_zfs_send_jobs" "$g_zxfer_send_job_abort_failure_message"
		)
	)
	assertContains "Abort preserves the first failed KILL status." "$output" "status=17"
	assertContains "Abort retains the failed job for trap retry." "$output" "count=1"
	assertContains "The diagnostic identifies the failed transfer." "$output" "tank/a@snap -> backup/a"
	assertContains "Successfully stopped siblings are reaped." "$output" "wait=402"
	assertNotContains "Abort never waits on an unkillable survivor." "$output" "wait=401"
}

test_wait_for_any_send_job_reaps_every_finished_job_in_one_scan() {
	hook_log="$TEST_TMPDIR/bgjob_scan.hooks"
	: >"$hook_log"
	printf '0\n' >"$TEST_TMPDIR/bgjob_scan.a"
	printf '0\n' >"$TEST_TMPDIR/bgjob_scan.b"
	: >"$TEST_TMPDIR/bgjob_scan.c"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			# Like the -T convergence check's ssh, this hook reads stdin.
			zxfer_verify_converged_destination_after_receive() {
				cat >/dev/null
				printf 'verify %s\n' "$1" >>"$BGJOB_HOOK_LOG"
			}
			g_zxfer_send_jobs="a	401	backup/a	$TEST_TMPDIR/bgjob_scan.a	tank/a@snap	pgid
c	403	backup/c	$TEST_TMPDIR/bgjob_scan.c	tank/c@snap	pgid
b	402	backup/b	$TEST_TMPDIR/bgjob_scan.b	tank/b@snap	pgid"
			g_count_zfs_send_jobs=3
			# Job c has no status yet and is still running.
			kill() { return 0; }
			wait() { :; }
			zxfer_wait_for_any_send_job "job limit"
			printf 'count=%s jobs=<%s>\n' "$g_count_zfs_send_jobs" "$g_zxfer_send_jobs"
		)
	)

	assertEquals "One scan reaps both finished jobs and keeps the running one." \
		"count=1 jobs=<c	403	backup/c	$TEST_TMPDIR/bgjob_scan.c	tank/c@snap	pgid>" "$output"
	assertContains "The first finished job is finished." "$(cat "$hook_log")" "verify backup/a"
	assertContains "The later finished job is finished in the same scan." "$(cat "$hook_log")" "verify backup/b"
}

test_reap_send_job_reports_teardown_failure_before_the_job_failure() {
	printf '3\n' >"$TEST_TMPDIR/bgjob_teardown.a"
	: >"$TEST_TMPDIR/bgjob_teardown.b"
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf 'message=%s\nstatus=%s\nstage=%s\n' "$1" "${2:-1}" "$g_zxfer_failure_stage"
				exit "${2:-1}"
			}
			g_zxfer_send_jobs="b	401	backup/b	$TEST_TMPDIR/bgjob_teardown.b	tank/b@snap2	pgid
a	402	backup/a	$TEST_TMPDIR/bgjob_teardown.a	tank/a@snap2	pgid"
			g_count_zfs_send_jobs=2
			# The survivor cannot be stopped: its final KILL fails.
			zxfer_signal_background_shell() {
				[ "$1" != 401 ] || [ "$3" != KILL ] || return 17
			}
			kill() { return 0; }
			wait() { :; }
			zxfer_reap_send_job a 402 backup/a "$TEST_TMPDIR/bgjob_teardown.a" tank/a@snap2
		)
	)
	status=$?

	assertEquals "The teardown failure status wins over the job status." 17 "$status"
	assertContains "The teardown failure names the job that could not be stopped." "$output" \
		"message=Failed to stop send/receive job [tank/b@snap2 -> backup/b] (PID 401)."
	assertContains "The teardown failure is reported at the send/receive stage." \
		"$output" "stage=send/receive"
	assertNotContains "The job failure is not reported over the teardown failure." \
		"$output" "zfs send/receive job failed"
}

test_abort_all_send_jobs_sends_term_before_kill() {
	output=$(
		(
			g_zxfer_send_jobs="first	401	backup/a	$TEST_TMPDIR/bgjob_order.none	tank/a@snap	pgid
second	402	backup/b	$TEST_TMPDIR/bgjob_order.none	tank/b@snap	wrapper"
			g_count_zfs_send_jobs=2
			zxfer_signal_background_shell() {
				printf '%s %s %s\n' "$3" "$1" "$2"
			}
			wait() { :; }
			zxfer_abort_all_send_jobs
			printf 'status=%s count=%s\n' "$?" "$g_count_zfs_send_jobs"
		)
	)

	assertEquals "Every running job gets TERM before any job gets KILL." \
		"TERM 401 pgid
TERM 402 wrapper
KILL 401 pgid
KILL 402 wrapper
status=0 count=0" "$output"
}

test_abort_all_send_jobs_never_signals_a_recycled_pid_after_its_status_is_written() {
	status_file="$TEST_TMPDIR/bgjob_recycled.status"
	printf '0\n' >"$status_file"
	output=$(
		(
			g_zxfer_send_jobs="group	4242	backup/a	$status_file	tank/a@snap	pgid
wrapped	4343	backup/b	$status_file	tank/b@snap	wrapper"
			g_count_zfs_send_jobs=2
			g_zxfer_send_job_abort_grace_seconds=1
			g_zxfer_send_job_poll_seconds=1
			# Both job shells exited; their PIDs now belong to live,
			# unrelated processes that lead no process group.
			kill() {
				printf 'kill %s\n' "$*"
				[ "$*" = "-s 0 4242" ] || [ "$*" = "-s 0 4343" ]
			}
			ps() {
				if [ "$4" = 4242 ] || [ "$4" = 4343 ]; then
					printf ' 9999\n'
				else
					printf ' 1000\n'
				fi
			}
			wait() { :; }
			zxfer_abort_all_send_jobs
			printf 'status=%s count=%s\n' "$?" "$g_count_zfs_send_jobs"
		)
	)

	assertContains "Exited jobs are cleared without a teardown failure." \
		"$output" "status=0 count=0"
	assertContains "An exited pgid job still gets a signal to its process group." \
		"$output" "kill -TERM -4242"
	for recycled_signal in TERM KILL STOP; do
		assertNotContains "A recycled pgid-job PID never gets $recycled_signal." \
			"$output" "kill -s $recycled_signal 4242"
		assertNotContains "A recycled wrapper-job PID never gets $recycled_signal." \
			"$output" "kill -s $recycled_signal 4343"
	done
	assertNotContains "A wrapper job has no process group to signal." \
		"$output" " -4343"
}

test_abort_all_send_jobs_never_evaluates_a_non_numeric_grace_knob() {
	status_file="$TEST_TMPDIR/bgjob_grace.status"
	marker="$TEST_TMPDIR/bgjob_grace.injected"
	printf '0\n' >"$status_file"
	rm -f "$marker"
	(
		g_zxfer_send_jobs="a	401	backup/a	$status_file	tank/a@snap	pgid"
		g_count_zfs_send_jobs=1
		g_zxfer_send_job_poll_seconds=0.2
		g_zxfer_send_job_abort_grace_seconds="x[\$(: >'$marker')]"
		kill() { return 0; }
		wait() { :; }
		zxfer_abort_all_send_jobs
	) 2>/dev/null

	assertFalse "The grace knob never reaches arithmetic as an expression." "[ -e '$marker' ]"
}

test_wait_for_zfs_send_jobs_clears_job_list_on_success() {
	output=$(
		(
			zxfer_reset_send_job_state
			zxfer_reset_send_receive_state
			zxfer_note_destination_receive_completed() { :; }
			zxfer_invalidate_destination_property_mutation_cache() { :; }
			zxfer_verify_converged_destination_after_receive() { :; }
			zxfer_spawn_send_job "sleep 1" "tank/a@snap" "backup/a"
			zxfer_spawn_send_job "sleep 1" "tank/b@snap" "backup/b"
			zxfer_wait_for_zfs_send_jobs "unit"
			printf 'jobs=<%s> count=%s\n' "$g_zxfer_send_jobs" "$g_count_zfs_send_jobs"
		)
	)
	assertEquals "Waiting for every send job should leave the job list empty." \
		"jobs=<> count=0" "$output"
}

test_wait_for_zfs_send_jobs_reports_failure() {
	(
		zxfer_reset_send_job_state
		zxfer_reset_send_receive_state
		g_zxfer_send_job_abort_grace_seconds=0
		zxfer_note_destination_receive_completed() { :; }
		zxfer_invalidate_destination_property_mutation_cache() { :; }
		zxfer_verify_converged_destination_after_receive() { :; }
		zxfer_throw_error() {
			echo "send failure"
			exit 1
		}
		zxfer_spawn_send_job "exit 0" "tank/a@snap" "backup/a"
		zxfer_spawn_send_job "exit 3" "tank/b@snap" "backup/b"
		zxfer_wait_for_zfs_send_jobs "failure"
	) >/dev/null 2>&1
	assertEquals "Job failures should surface via zxfer_throw_error." 1 "$?"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
