#!/bin/sh
#
# shunit2 tests for zxfer_background_job_runner.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_background_job_runner"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	TMPDIR="$TEST_TMPDIR"
	exec 9>&- 2>/dev/null || true
	ZXFER_BACKGROUND_JOB_NOTIFY_FD=""
	ZXFER_BACKGROUND_JOB_RUNNER_SOURCE_ONLY=1 \
		. "$ZXFER_ROOT/src/zxfer_background_job_runner.sh"
	unset ZXFER_BACKGROUND_JOB_RUNNER_SOURCE_ONLY
}

test_background_job_runner_main_writes_launch_completion_and_notifies_queue() {
	control_dir="$TEST_TMPDIR/runner_success_control"
	outfile="$TEST_TMPDIR/runner_success.out"
	errfile="$TEST_TMPDIR/runner_success.err"
	notify_file="$TEST_TMPDIR/runner_success.notify"
	mkdir -p "$control_dir"

	exec 9>"$notify_file"
	ZXFER_BACKGROUND_JOB_NOTIFY_FD=9
	zxfer_background_job_runner_main \
		"token-1" \
		"job-1" \
		"send_receive" \
		"$control_dir" \
		"printf '%s\n' payload" \
		"display payload" \
		"$outfile" \
		"$errfile"
	status=$?
	exec 9>&-

	assertEquals "Runner main should return the worker exit status for successful jobs." \
		0 "$status"
	assertEquals "Runner main should capture worker stdout in the requested file." \
		"payload" "$(tr -d '\n' <"$outfile")"
	assertEquals "Runner main should leave worker stderr empty when the worker is quiet." \
		0 "$(wc -c <"$errfile" | tr -d '[:space:]')"
	assertContains "Runner main should publish launch metadata for the job id." \
		"$(cat "$control_dir/launch.tsv")" "job_id	job-1"
	assertContains "Runner main should publish launch metadata for the runner token." \
		"$(cat "$control_dir/launch.tsv")" "runner_token	token-1"
	assertContains "Runner main should publish completion metadata for the zero exit status." \
		"$(cat "$control_dir/completion.tsv")" "status	0"
	assertEquals "Runner main should publish the completed job id to the notify fd." \
		"job-1" "$(tr -d '\r\n' <"$notify_file")"
}

test_background_job_runner_main_reports_launch_metadata_write_failures() {
	control_dir="$TEST_TMPDIR/runner_launch_failure_control"
	stdout_file="$TEST_TMPDIR/runner_launch_failure.stdout"
	stderr_file="$TEST_TMPDIR/runner_launch_failure.stderr"
	fake_bin_dir="$TEST_TMPDIR/runner_launch_failure_bin"
	abort_log="$TEST_TMPDIR/runner_launch_failure.abort"
	mkdir -p "$control_dir"
	mkdir -p "$fake_bin_dir"

	cat >"$fake_bin_dir/setsid" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$fake_bin_dir/setsid"

	# shellcheck disable=SC2016
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		zxfer_background_job_runner_get_pgid() {
			if [ "$1" = "$$" ]; then
				printf "%s\n" "901"
			else
				printf "%s\n" "902"
			fi
		}
			zxfer_background_job_runner_write_launch() {
				return 1
			}
			zxfer_background_job_runner_abort_worker_scope() {
				printf "%s\t%s\t%s\t%s\n" "$1" "$2" "$3" "$4" >"'"$abort_log"'"
				return 1
			}
		l_old_path=$PATH
		PATH="'"$fake_bin_dir"':$PATH"
		export PATH
		zxfer_background_job_runner_main \
			"token-2" \
			"job-2" \
			"send_receive" \
			"'"$control_dir"'" \
			":" \
			"display failure" \
			"" \
			""
		l_status=$?
		PATH=$l_old_path
		export PATH
		return "$l_status"
	'

	assertEquals "Runner main should fail closed when launch metadata cannot be written." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Launch-metadata write failures should preserve the dedicated operator-facing error." \
		"$(cat "$stderr_file")" "Failed to record launch metadata for background job [job-2]."
	assertContains "Launch-metadata write failures should report worker-scope teardown failures when they occur." \
		"$(cat "$stderr_file")" "Failed to tear down background job [job-2] worker scope after launch metadata failure."
	assertContains "Launch-metadata write failures should tear down the validated worker scope instead of only the wrapper pid." \
		"$(cat "$abort_log")" "process_group	TERM"
}

test_background_job_runner_main_reports_completion_write_failures() {
	control_dir="$TEST_TMPDIR/runner_completion_failure_control"
	notify_file="$TEST_TMPDIR/runner_completion_failure.notify"
	stdout_file="$TEST_TMPDIR/runner_completion_failure.stdout"
	stderr_file="$TEST_TMPDIR/runner_completion_failure.stderr"
	mkdir -p "$control_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		exec 9>"'"$notify_file"'"
		ZXFER_BACKGROUND_JOB_NOTIFY_FD=9
		zxfer_background_job_runner_write_completion() {
			return 1
		}
		zxfer_background_job_runner_main \
			"token-3" \
			"job-3" \
			"send_receive" \
			"'"$control_dir"'" \
			":" \
			"display completion failure" \
			"" \
			""
	'

	assertEquals "Runner main should fail closed when completion metadata cannot be written." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Completion-metadata write failures should preserve the dedicated operator-facing error." \
		"$(cat "$stderr_file")" "Failed to record background job completion"
	assertEquals "Completion-metadata write failures should publish the explicit failure notification record." \
		"completion_write_failed	job-3	0" "$(tr -d '\r\n' <"$notify_file")"
}

test_background_job_runner_main_reports_queue_publish_failures() {
	control_dir="$TEST_TMPDIR/runner_notify_failure_control"
	stdout_file="$TEST_TMPDIR/runner_notify_failure.stdout"
	stderr_file="$TEST_TMPDIR/runner_notify_failure.stderr"
	mkdir -p "$control_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		zxfer_background_job_runner_notify_completion() {
			return 1
		}
		zxfer_background_job_runner_main \
			"token-4" \
			"job-4" \
			"send_receive" \
			"'"$control_dir"'" \
			":" \
			"display notify failure" \
			"" \
			""
	'

	assertEquals "Runner main should fail closed when queue publication fails." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Queue-publication failures should preserve the dedicated operator-facing error." \
		"$(cat "$stderr_file")" "Failed to publish background job completion for [job-4]."
	assertContains "Queue-publication failures should rewrite the completion metadata with the queue_write marker." \
		"$(cat "$control_dir/completion.tsv")" "report_failure	queue_write"
}

test_background_job_runner_main_reports_queue_publication_rewrite_failures() {
	control_dir="$TEST_TMPDIR/runner_notify_rewrite_failure_control"
	stdout_file="$TEST_TMPDIR/runner_notify_rewrite_failure.stdout"
	stderr_file="$TEST_TMPDIR/runner_notify_rewrite_failure.stderr"
	mkdir -p "$control_dir"

	# shellcheck disable=SC2016
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		l_completion_write_count=0
		zxfer_background_job_runner_notify_completion() {
			return 1
		}
		zxfer_background_job_runner_write_completion() {
			l_completion_write_count=$((l_completion_write_count + 1))
			if [ "$l_completion_write_count" -gt 1 ]; then
				return 1
			fi
			command cat >"'"$control_dir"'/completion.tsv" <<-EOF
				status	0
				report_failure
			EOF
			return 0
		}
		zxfer_background_job_runner_main \
			"token-4b" \
			"job-4b" \
			"send_receive" \
			"'"$control_dir"'" \
			":" \
			"display notify rewrite failure" \
			"" \
			""
	'

	assertEquals "Runner main should fail closed when queue publication fails and the completion rewrite also fails." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Queue-publication rewrite failures should preserve the dedicated operator-facing error." \
		"$(cat "$stderr_file")" "Failed to record background job queue publication failure"
	assertFalse "Queue-publication rewrite failures should remove the stale completion file when the rewrite cannot be recorded." \
		"[ -f '$control_dir/completion.tsv' ]"
}

test_background_job_runner_main_uses_setsid_runner_when_available() {
	control_dir="$TEST_TMPDIR/runner_setsid_control"
	outfile="$TEST_TMPDIR/runner_setsid.out"
	errfile="$TEST_TMPDIR/runner_setsid.err"
	fake_bin_dir="$TEST_TMPDIR/runner_setsid_bin"
	setsid_log="$TEST_TMPDIR/runner_setsid.log"
	mkdir -p "$control_dir" "$fake_bin_dir"

	cat >"$fake_bin_dir/setsid" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$ZXFER_TEST_FAKE_SETSID_LOG"
exec "$@"
EOF
	chmod +x "$fake_bin_dir/setsid"

	l_old_path=$PATH
	PATH="$fake_bin_dir:$PATH"
	export PATH
	ZXFER_TEST_FAKE_SETSID_LOG=$setsid_log
	export ZXFER_TEST_FAKE_SETSID_LOG
	zxfer_background_job_runner_main \
		"token-setsid" \
		"job-setsid" \
		"send_receive" \
		"$control_dir" \
		"printf '%s\n' payload" \
		"display setsid payload" \
		"$outfile" \
		"$errfile"
	status=$?
	PATH=$l_old_path
	export PATH
	unset ZXFER_TEST_FAKE_SETSID_LOG

	assertEquals "Runner main should still return success when a setsid helper is available." \
		0 "$status"
	assertEquals "Runner main should still capture stdout when the worker is launched through setsid." \
		"payload" "$(tr -d '\n' <"$outfile")"
	assertContains "Runner main should route worker launch through the available setsid helper." \
		"$(cat "$setsid_log")" "sh -c"
	assertContains "Runner main should preserve the worker payload command when launching through the setsid helper." \
		"$(cat "$setsid_log")" "payload"
}

test_background_job_runner_main_uses_setsid_output_only_path_and_marks_process_group_teardown() {
	control_dir="$TEST_TMPDIR/runner_setsid_output_only_control"
	outfile="$TEST_TMPDIR/runner_setsid_output_only.out"
	fake_bin_dir="$TEST_TMPDIR/runner_setsid_output_only_bin"
	setsid_log="$TEST_TMPDIR/runner_setsid_output_only.log"
	mkdir -p "$control_dir" "$fake_bin_dir"

	cat >"$fake_bin_dir/setsid" <<'EOF'
#!/bin/sh
	printf '%s\n' "$*" >>"$ZXFER_TEST_FAKE_SETSID_LOG"
	exec "$@"
EOF
	chmod +x "$fake_bin_dir/setsid"

	zxfer_background_job_runner_get_pgid() {
		if [ "$1" = "$$" ]; then
			printf '%s\n' "701"
		else
			printf '%s\n' "801"
		fi
	}

	l_old_path=$PATH
	PATH="$fake_bin_dir:$PATH"
	export PATH
	ZXFER_TEST_FAKE_SETSID_LOG=$setsid_log
	export ZXFER_TEST_FAKE_SETSID_LOG
	zxfer_background_job_runner_main \
		"token-setsid-output-only" \
		"job-setsid-output-only" \
		"send_receive" \
		"$control_dir" \
		"printf '%s\n' output-only-setsid" \
		"display setsid output only" \
		"$outfile" \
		""
	status=$?
	PATH=$l_old_path
	export PATH
	unset ZXFER_TEST_FAKE_SETSID_LOG

	assertEquals "Runner main should preserve success through the setsid output-only path." \
		0 "$status"
	assertEquals "The setsid output-only path should still capture stdout." \
		"output-only-setsid" "$(tr -d '\n' <"$outfile")"
	assertContains "The setsid output-only path should route through the setsid helper." \
		"$(cat "$setsid_log")" "sh -c"
	assertContains "When setsid isolates the worker pgid, the runner should record process-group teardown mode." \
		"$(cat "$control_dir/launch.tsv")" "teardown_mode	process_group"
}

test_background_job_runner_main_uses_setsid_no_output_path_and_marks_process_group_teardown() {
	control_dir="$TEST_TMPDIR/runner_setsid_no_output_control"
	fake_bin_dir="$TEST_TMPDIR/runner_setsid_no_output_bin"
	setsid_log="$TEST_TMPDIR/runner_setsid_no_output.log"
	notify_file="$TEST_TMPDIR/runner_setsid_no_output.notify"
	mkdir -p "$control_dir" "$fake_bin_dir"

	cat >"$fake_bin_dir/setsid" <<'EOF'
#!/bin/sh
	printf '%s\n' "$*" >>"$ZXFER_TEST_FAKE_SETSID_LOG"
	exec "$@"
EOF
	chmod +x "$fake_bin_dir/setsid"

	zxfer_background_job_runner_get_pgid() {
		if [ "$1" = "$$" ]; then
			printf '%s\n' "702"
		else
			printf '%s\n' "802"
		fi
	}

	exec 9>"$notify_file"
	l_old_path=$PATH
	PATH="$fake_bin_dir:$PATH"
	export PATH
	ZXFER_TEST_FAKE_SETSID_LOG=$setsid_log
	export ZXFER_TEST_FAKE_SETSID_LOG
	ZXFER_BACKGROUND_JOB_NOTIFY_FD=9
	zxfer_background_job_runner_main \
		"token-setsid-no-output" \
		"job-setsid-no-output" \
		"send_receive" \
		"$control_dir" \
		":" \
		"display setsid no output" \
		"" \
		""
	status=$?
	exec 9>&-
	PATH=$l_old_path
	export PATH
	unset ZXFER_TEST_FAKE_SETSID_LOG

	assertEquals "Runner main should preserve success through the setsid no-output path." \
		0 "$status"
	assertContains "The setsid no-output path should still route through the setsid helper." \
		"$(cat "$setsid_log")" "sh -c"
	assertContains "The setsid no-output path should still publish queue completion." \
		"$(cat "$notify_file")" "job-setsid-no-output"
	assertContains "The no-output setsid path should record process-group teardown mode when the pgid differs." \
		"$(cat "$control_dir/launch.tsv")" "teardown_mode	process_group"
}

test_background_job_runner_main_handles_output_only_and_no_output_paths_without_setsid_helper() {
	control_dir="$TEST_TMPDIR/runner_output_variants_control"
	output_only_file="$TEST_TMPDIR/runner_output_only.out"
	notify_file="$TEST_TMPDIR/runner_output_variants.notify"
	fake_bin_dir="$TEST_TMPDIR/runner_output_variants_bin"
	real_sh=$(command -v sh)
	mkdir -p "$control_dir" "$fake_bin_dir"
	for l_tool in sh date sed mv chmod rm printf; do
		l_real_tool=$(command -v "$l_tool") || fail "Unable to resolve [$l_tool] for the no-setsid PATH fixture."
		ln -s "$l_real_tool" "$fake_bin_dir/$l_tool" ||
			fail "Unable to publish [$l_tool] into the no-setsid PATH fixture."
	done
	cat >"$fake_bin_dir/ps" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$fake_bin_dir/ps"

	exec 9>"$notify_file"
	l_old_path=$PATH
	PATH=$fake_bin_dir
	export PATH
	ZXFER_BACKGROUND_JOB_NOTIFY_FD=9
	zxfer_background_job_runner_main \
		"token-output-only" \
		"job-output-only" \
		"send_receive" \
		"$control_dir" \
		"printf '%s\n' output-only" \
		"display output only" \
		"$output_only_file" \
		""
	output_only_status=$?
	zxfer_background_job_runner_main \
		"token-no-output" \
		"job-no-output" \
		"send_receive" \
		"$control_dir" \
		":" \
		"display no output" \
		"" \
		""
	no_output_status=$?
	PATH=$l_old_path
	export PATH
	exec 9>&-

	assertEquals "Runner main should support output-only worker capture paths." \
		0 "$output_only_status"
	assertEquals "Output-only worker capture should preserve stdout." \
		"output-only" "$(tr -d '\n' <"$output_only_file")"
	assertEquals "Runner main should support worker launches without staged output files." \
		0 "$no_output_status"
	assertContains "Worker runs without staged output files should still notify completion." \
		"$(cat "$notify_file")" "job-no-output"
}

test_background_job_runner_main_preserves_child_set_teardown_when_worker_pgid_is_unreadable() {
	control_dir="$TEST_TMPDIR/runner_child_set_control"
	mkdir -p "$control_dir"

	(
		zxfer_background_job_runner_get_pgid() {
			if [ "$1" = "$$" ]; then
				printf '%s\n' "777"
			else
				printf '%s\n' ""
			fi
		}
		zxfer_background_job_runner_main \
			"token-child-set" \
			"job-child-set" \
			"send_receive" \
			"$control_dir" \
			":" \
			"display child set" \
			"" \
			""
	)

	assertContains "Unreadable worker pgids should keep the teardown mode on owned-child-set cleanup." \
		"$(cat "$control_dir/launch.tsv")" "teardown_mode	child_set"
}

test_background_job_runner_process_snapshot_and_pid_set_helpers_cover_current_shell_paths() {
	zxfer_test_capture_subshell "
		ps() {
			cat <<-'EOF'
				7100 7000
				7101 7100
			EOF
		}

		snapshot=\$(zxfer_background_job_runner_read_process_snapshot)
		printf 'snapshot=%s\n' \"\$snapshot\"
		l_restore_errexit=0

		case \$- in
		*e*)
			l_restore_errexit=1
			;;
		esac

		set +e
		zxfer_background_job_runner_get_pid_set \"\$snapshot\" bad >/dev/null 2>&1
		invalid_root_status=\$?
		zxfer_background_job_runner_snapshot_has_pid \"\$snapshot\" bad >/dev/null 2>&1
		invalid_pid_status=\$?
		zxfer_background_job_runner_snapshot_has_pid_with_parent \"\$snapshot\" bad 7000 >/dev/null 2>&1
		invalid_parent_pid_status=\$?
		zxfer_background_job_runner_snapshot_has_pid_with_parent \"\$snapshot\" 7101 bad >/dev/null 2>&1
		invalid_parent_status=\$?
		zxfer_background_job_runner_snapshot_has_pid_with_parent_and_pgid \"\$snapshot\" bad 7000 7100 >/dev/null 2>&1
		invalid_pgid_pid_status=\$?
		zxfer_background_job_runner_snapshot_has_pid_with_parent_and_pgid \"\$snapshot\" 7101 bad 7100 >/dev/null 2>&1
		invalid_pgid_parent_status=\$?
		zxfer_background_job_runner_snapshot_has_pid_with_parent_and_pgid \"\$snapshot\" 7101 7100 bad >/dev/null 2>&1
		invalid_pgid_status=\$?
		zxfer_background_job_runner_snapshot_has_pid \"\$snapshot\" 7101 >/dev/null 2>&1
		has_pid_status=\$?
		zxfer_background_job_runner_snapshot_has_pid \"\$snapshot\" 7999 >/dev/null 2>&1
		missing_pid_status=\$?
		if [ \"\$l_restore_errexit\" -eq 1 ]; then
			set -e
		fi
		printf 'invalid_root_status=%s\n' \"\$invalid_root_status\"
		printf 'invalid_pid_status=%s\n' \"\$invalid_pid_status\"
		printf 'invalid_parent_pid_status=%s\n' \"\$invalid_parent_pid_status\"
		printf 'invalid_parent_status=%s\n' \"\$invalid_parent_status\"
		printf 'invalid_pgid_pid_status=%s\n' \"\$invalid_pgid_pid_status\"
		printf 'invalid_pgid_parent_status=%s\n' \"\$invalid_pgid_parent_status\"
		printf 'invalid_pgid_status=%s\n' \"\$invalid_pgid_status\"
		printf 'has_pid_status=%s\n' \"\$has_pid_status\"
		printf 'missing_pid_status=%s\n' \"\$missing_pid_status\"
	"
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Process snapshot reads should preserve the current-shell ps rows." \
		"$output" "7100 7000"
	assertContains "Process snapshot reads should preserve later pid/ppid rows from the current shell." \
		"$output" "7101 7100"
	assertContains "PID-set derivation should reject invalid root pids." \
		"$output" "invalid_root_status=1"
	assertContains "Process snapshot helpers should reject invalid pid inputs." \
		"$output" "invalid_pid_status=1"
	assertContains "Parent snapshot helpers should reject invalid pid inputs." \
		"$output" "invalid_parent_pid_status=1"
	assertContains "Parent snapshot helpers should reject invalid parent pid inputs." \
		"$output" "invalid_parent_status=1"
	assertContains "Parent-and-pgid snapshot helpers should reject invalid pid inputs." \
		"$output" "invalid_pgid_pid_status=1"
	assertContains "Parent-and-pgid snapshot helpers should reject invalid parent pid inputs." \
		"$output" "invalid_pgid_parent_status=1"
	assertContains "Parent-and-pgid snapshot helpers should reject invalid pgid inputs." \
		"$output" "invalid_pgid_status=1"
	assertContains "Process snapshot helpers should report success when the requested pid is present." \
		"$output" "has_pid_status=0"
	assertContains "Process snapshot helpers should report failure when the requested pid is absent." \
		"$output" "missing_pid_status=1"
}

test_background_job_runner_signal_helpers_cover_current_shell_paths() {
	signal_log="$TEST_TMPDIR/runner_signal_helpers.log"

	(
		kill() {
			printf 'kill:%s\n' "$*" >>"$signal_log"
			return 0
		}

		zxfer_background_job_runner_signal_pid_set "3301

3302" "TERM"
		printf 'pid_set_status=%s\n' "$?" >>"$signal_log"
		set +e
		zxfer_background_job_runner_signal_process_group "bad" "TERM"
		printf 'invalid_pgid_status=%s\n' "$?" >>"$signal_log"
		set -e
		zxfer_background_job_runner_signal_process_group "4301" "TERM"
		printf 'process_group_status=%s\n' "$?" >>"$signal_log"
	)

	assertContains "PID-set signaling should forward the first pid with the requested signal." \
		"$(cat "$signal_log")" "kill:-TERM 3301"
	assertContains "PID-set signaling should forward later pids with the requested signal." \
		"$(cat "$signal_log")" "kill:-TERM 3302"
	assertContains "PID-set signaling should report success when all signal calls succeed." \
		"$(cat "$signal_log")" "pid_set_status=0"
	assertContains "Process-group signaling should reject invalid pgids." \
		"$(cat "$signal_log")" "invalid_pgid_status=1"
	assertContains "Process-group signaling should target the negative pgid when the pgid is valid." \
		"$(cat "$signal_log")" "kill:-TERM -4301"
	assertContains "Process-group signaling should report success when the signal call succeeds." \
		"$(cat "$signal_log")" "process_group_status=0"
}

test_background_job_runner_abort_worker_scope_prefers_process_group_cleanup() {
	abort_log="$TEST_TMPDIR/runner_abort_process_group.log"

	(
		zxfer_background_job_runner_signal_process_group() {
			printf 'pg:%s:%s\n' "$1" "$2" >>"$abort_log"
			return 0
		}
		zxfer_background_job_runner_signal_pid_set() {
			printf 'pidset:%s:%s\n' "$1" "$2" >>"$abort_log"
			return 0
		}
		zxfer_background_job_runner_read_process_snapshot() {
			printf '%s %s %s\n' "2201" "$$" "3201"
			return 0
		}
		kill() {
			printf 'kill:%s\n' "$*" >>"$abort_log"
			return 0
		}
		wait() {
			printf 'wait:%s\n' "$1" >>"$abort_log"
			return 0
		}

		zxfer_background_job_runner_abort_worker_scope "2201" "3201" "process_group" "TERM"
	)

	assertContains "Worker-scope aborts should signal the validated process group when one is available." \
		"$(cat "$abort_log")" "pg:3201:TERM"
	assertContains "Worker-scope aborts should still wait on the worker pid after process-group cleanup." \
		"$(cat "$abort_log")" "wait:2201"
	assertNotContains "Successful process-group cleanup should not fall back to owned-child-set signaling." \
		"$(cat "$abort_log")" "pidset:"
	assertNotContains "Successful process-group cleanup should not fall back to a bare worker-pid signal." \
		"$(cat "$abort_log")" "kill:"
}

test_background_job_runner_abort_worker_scope_falls_back_to_owned_child_set_cleanup() {
	abort_log="$TEST_TMPDIR/runner_abort_child_set.log"
	pid_set_file="$TEST_TMPDIR/runner_abort_child_set.pidset"

	(
		zxfer_background_job_runner_signal_process_group() {
			printf 'pg-failed:%s:%s\n' "$1" "$2" >>"$abort_log"
			return 1
		}
		zxfer_background_job_runner_read_process_snapshot() {
			cat <<-EOF
				2202 $$ 3202
				2203 2202 3202
				2204 2203 3202
			EOF
		}
		zxfer_background_job_runner_signal_pid_set() {
			printf '%s\n' "$1" >"$pid_set_file"
			printf 'pidset-signal:%s\n' "$2" >>"$abort_log"
			return 0
		}
		kill() {
			printf 'kill:%s\n' "$*" >>"$abort_log"
			return 0
		}
		wait() {
			printf 'wait:%s\n' "$1" >>"$abort_log"
			return 0
		}

		zxfer_background_job_runner_abort_worker_scope "2202" "3202" "process_group" "TERM"
	)

	assertContains "Failed process-group cleanup should fall back to owned-child-set signaling." \
		"$(cat "$abort_log")" "pidset-signal:TERM"
	assertEquals "Owned-child-set cleanup should signal the full worker subtree rather than only the wrapper pid." \
		"2202
2203
2204" "$(cat "$pid_set_file")"
	assertContains "Owned-child-set cleanup should still wait on the worker pid after signaling the subtree." \
		"$(cat "$abort_log")" "wait:2202"
	assertNotContains "Owned-child-set cleanup should not fall back to a bare worker-pid signal when subtree derivation succeeds." \
		"$(cat "$abort_log")" "kill:"
}

test_background_job_runner_abort_worker_scope_rejects_unowned_worker_before_signaling() {
	abort_log="$TEST_TMPDIR/runner_abort_unowned_worker.log"

	output=$(
		(
			zxfer_background_job_runner_signal_process_group() {
				printf 'pg:%s:%s\n' "$1" "$2" >>"$abort_log"
				return 0
			}
			zxfer_background_job_runner_signal_pid_set() {
				printf 'pidset:%s:%s\n' "$1" "$2" >>"$abort_log"
				return 0
			}
			zxfer_background_job_runner_read_process_snapshot() {
				cat <<-EOF
					2209 9999 3209
					2210 2209 3209
				EOF
			}
			wait() {
				printf 'wait:%s\n' "$1" >>"$abort_log"
				return 0
			}

			zxfer_background_job_runner_abort_worker_scope "2209" "3209" "process_group" "TERM"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Worker-scope aborts should fail closed when the worker pid is no longer owned by the runner." \
		"$output" "status=1"
	assertContains "Worker-scope aborts should still reap the recorded worker pid after ownership rejection." \
		"$(cat "$abort_log")" "wait:2209"
	assertNotContains "Worker-scope aborts should not signal a reused worker process group before proving ownership." \
		"$(cat "$abort_log")" "pg:"
	assertNotContains "Worker-scope aborts should not signal a child set rooted at an unowned worker pid." \
		"$(cat "$abort_log")" "pidset:"
}

test_background_job_runner_abort_worker_scope_ignores_invalid_worker_pid() {
	output=$(
		(
			zxfer_background_job_runner_abort_worker_scope "bad" "3204" "child_set" "TERM"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Worker-scope aborts should treat invalid worker pids as a no-op success." \
		"$output" "status=0"
}

test_background_job_runner_abort_worker_scope_fails_closed_when_snapshot_derivation_is_unavailable() {
	abort_log="$TEST_TMPDIR/runner_abort_pid_fallback.log"

	output=$(
		(
			zxfer_background_job_runner_read_process_snapshot() {
				return 1
			}
			kill() {
				printf 'kill:%s\n' "$*" >>"$abort_log"
				return 0
			}
			wait() {
				printf 'wait:%s\n' "$1" >>"$abort_log"
				return 0
			}

			zxfer_background_job_runner_abort_worker_scope "2205" "" "child_set" "TERM"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Worker-scope aborts should still wait on the worker pid after a snapshot-derivation failure." \
		"$(cat "$abort_log")" "wait:2205"
	assertContains "Worker-scope aborts should fail closed when subtree derivation is unavailable." \
		"$output" "status=1"
	assertFalse "Worker-scope aborts should not fall back to signaling a bare worker pid when ownership cannot be proven." \
		"[ -f '$abort_log' ] && grep -q 'kill:' '$abort_log'"
}

test_background_job_runner_abort_worker_scope_fails_closed_when_snapshot_omits_worker_pid() {
	abort_log="$TEST_TMPDIR/runner_abort_missing_worker.log"

	output=$(
		(
			zxfer_background_job_runner_read_process_snapshot() {
				cat <<-EOF
					3301 1201
					3302 3301
				EOF
			}
			zxfer_background_job_runner_signal_pid_set() {
				printf 'pidset:%s:%s\n' "$1" "$2" >>"$abort_log"
				return 0
			}
			wait() {
				printf 'wait:%s\n' "$1" >>"$abort_log"
				return 0
			}

			zxfer_background_job_runner_abort_worker_scope "2206" "" "child_set" "TERM"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Worker-scope aborts should fail closed when the worker pid is no longer present in the process snapshot." \
		"$output" "status=1"
	assertContains "Worker-scope aborts should still wait on the worker pid after the missing-worker path." \
		"$(cat "$abort_log")" "wait:2206"
	assertNotContains "Worker-scope aborts should not signal an owned child set when the worker pid is absent from the snapshot." \
		"$(cat "$abort_log")" "pidset:"
}

test_background_job_runner_abort_worker_scope_fails_closed_when_pid_set_derivation_fails_or_is_empty() {
	abort_log="$TEST_TMPDIR/runner_abort_pid_set_derivation.log"

	output=$(
		(
			zxfer_background_job_runner_read_process_snapshot() {
				printf '%s %s %s\n' "2211" "$$" "2211"
			}
			zxfer_background_job_runner_get_pid_set() {
				return 1
			}
			wait() {
				printf 'wait-fail:%s\n' "$1" >>"$abort_log"
			}
			zxfer_background_job_runner_abort_worker_scope "2211" "" "child_set" "TERM"
			printf 'derive_status=%s\n' "$?"
		)
		(
			zxfer_background_job_runner_read_process_snapshot() {
				printf '%s %s %s\n' "2212" "$$" "2212"
			}
			zxfer_background_job_runner_get_pid_set() {
				return 0
			}
			wait() {
				printf 'wait-empty:%s\n' "$1" >>"$abort_log"
			}
			zxfer_background_job_runner_abort_worker_scope "2212" "" "child_set" "TERM"
			printf 'empty_status=%s\n' "$?"
		)
	)

	assertContains "Worker-scope aborts should fail closed when owned-child-set derivation fails." \
		"$output" "derive_status=1"
	assertContains "Worker-scope aborts should fail closed when owned-child-set derivation returns empty." \
		"$output" "empty_status=1"
	assertContains "Worker-scope aborts should still wait on the worker after pid-set derivation failures." \
		"$(cat "$abort_log")" "wait-fail:2211"
	assertContains "Worker-scope aborts should still wait on the worker after empty pid-set derivation." \
		"$(cat "$abort_log")" "wait-empty:2212"
}

test_background_job_runner_abort_worker_scope_fails_closed_when_owned_child_set_signal_fails() {
	abort_log="$TEST_TMPDIR/runner_abort_child_signal_failure.log"

	output=$(
		(
			zxfer_background_job_runner_read_process_snapshot() {
				cat <<-EOF
					2207 $$ 2207
					2208 2207 2207
				EOF
			}
			zxfer_background_job_runner_signal_pid_set() {
				printf 'pidset:%s:%s\n' "$1" "$2" >>"$abort_log"
				return 1
			}
			wait() {
				printf 'wait:%s\n' "$1" >>"$abort_log"
				return 0
			}

			zxfer_background_job_runner_abort_worker_scope "2207" "" "child_set" "TERM"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Worker-scope aborts should report failure when signaling the validated owned child set fails." \
		"$output" "status=1"
	assertContains "Worker-scope aborts should still wait on the worker pid after an owned-child-set signal failure." \
		"$(cat "$abort_log")" "wait:2207"
}

test_background_job_runner_atomic_write_cleans_up_stage_files_on_write_and_rename_failures() {
	missing_dir_target="$TEST_TMPDIR/missing-dir/launch.tsv"
	rename_target="$TEST_TMPDIR/atomic_rename_fail.tsv"
	rename_stage="$rename_target.stage.$$"
	l_restore_errexit=0

	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	zxfer_background_job_runner_atomic_write "$missing_dir_target" "payload" >/dev/null 2>&1
	write_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi
	assertEquals "Atomic writes should fail closed when the stage file cannot be created." \
		1 "$write_status"
	assertFalse "Atomic writes should not leave a stage file behind after a stage-write failure." \
		"[ -e '$missing_dir_target.stage.$$' ]"

	set +e
	(
		mv() {
			return 1
		}
		zxfer_background_job_runner_atomic_write "$rename_target" "payload"
	)
	rename_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Atomic writes should fail closed when the stage rename fails." \
		1 "$rename_status"
	assertFalse "Atomic writes should remove the staged file after a rename failure." \
		"[ -e '$rename_stage' ]"
}

test_background_job_runner_notify_helpers_ignore_invalid_notify_fds() {
	output=$(
		(
			ZXFER_BACKGROUND_JOB_NOTIFY_FD="bad"
			zxfer_background_job_runner_notify_completion "job-5"
			printf 'notify_status=%s\n' "$?"
			zxfer_background_job_runner_notify_completion_write_failure "job-5" 125
			printf 'failure_status=%s\n' "$?"
		)
	)

	assertContains "Completion notification helpers should treat invalid notify fds as no-op success." \
		"$output" "notify_status=0"
	assertContains "Completion-write failure notification helpers should treat invalid notify fds as no-op success." \
		"$output" "failure_status=0"
}

test_background_job_runner_script_executes_main_when_run_directly() {
	control_dir="$TEST_TMPDIR/runner_direct_script_control"
	outfile="$TEST_TMPDIR/runner_direct_script.out"
	errfile="$TEST_TMPDIR/runner_direct_script.err"
	mkdir -p "$control_dir"

	/bin/sh "$ZXFER_ROOT/src/zxfer_background_job_runner.sh" \
		"token-direct" \
		"job-direct" \
		"send_receive" \
		"$control_dir" \
		"printf '%s\n' direct" \
		"display direct" \
		"$outfile" \
		"$errfile"
	status=$?

	assertEquals "Running the helper directly should execute the main entrypoint and preserve the worker status." \
		0 "$status"
	assertEquals "Direct helper execution should still capture worker stdout." \
		"direct" "$(tr -d '\n' <"$outfile")"
	assertContains "Direct helper execution should still write launch metadata." \
		"$(cat "$control_dir/launch.tsv")" "job_id	job-direct"
}

test_background_job_runner_source_entrypoint_executes_main_when_sourced_in_current_shell() {
	control_dir="$TEST_TMPDIR/runner_direct_source_control"
	outfile="$TEST_TMPDIR/runner_direct_source.out"
	errfile="$TEST_TMPDIR/runner_direct_source.err"
	mkdir -p "$control_dir"

	set +e
	(
		set -- \
			"token-source" \
			"job-source" \
			"send_receive" \
			"$control_dir" \
			"printf '%s\n' sourced" \
			"display sourced" \
			"$outfile" \
			"$errfile"
		unset ZXFER_BACKGROUND_JOB_RUNNER_SOURCE_ONLY
		. "$ZXFER_ROOT/src/zxfer_background_job_runner.sh"
	)
	status=$?
	set -e

	assertEquals "Sourcing the helper with source-only disabled should execute the main entrypoint and preserve the worker status." \
		0 "$status"
	assertEquals "Current-shell sourced helper execution should still capture worker stdout." \
		"sourced" "$(tr -d '\n' <"$outfile")"
	assertContains "Current-shell sourced helper execution should still write launch metadata." \
		"$(cat "$control_dir/launch.tsv")" "job_id	job-source"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
