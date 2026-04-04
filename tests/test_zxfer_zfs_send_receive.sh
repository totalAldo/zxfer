#!/bin/sh
#
# shunit2 tests for zxfer_zfs_send_receive.sh helpers.
#
# shellcheck disable=SC2030,SC2031

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_zfs_send_receive.sh
. "$ZXFER_ROOT/src/zxfer_zfs_send_receive.sh"

usage() {
	:
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_send_receive.XXXXXX)
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_D_display_progress_bar=""
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_z_compress=0
	g_option_j_jobs=1
	g_option_F_force_rollback=""
	g_cmd_zfs="/sbin/zfs"
	g_cmd_compress_safe="gzip"
	g_cmd_decompress_safe="gunzip"
	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=0
	g_is_performed_send_destroy=0
	g_zxfer_failure_last_command=""
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_failure_context "unit"
}

test_wrap_command_with_ssh_receive_direction_with_compression() {
	result=$(
		get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
		quote_host_spec_tokens() { printf '%s\n' "'target.example' 'doas'"; }
		split_host_spec_tokens() { printf '%s\n%s\n' "target.example" "doas"; }
		build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'gunzip | zfs receive tank/dst'"; }
		build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'gunzip | zfs receive tank/dst'"; }
		wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive
	)

	assertEquals "Receive-side compression should wrap the remote command in the documented direction." \
		"gzip | '/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'gunzip | zfs receive tank/dst'" "$result"
}

test_wrap_command_with_ssh_rejects_missing_safe_compression_commands() {
	set +e
	output=$(
		(
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_compress_safe=""
			g_cmd_decompress_safe=""
			wrap_command_with_ssh "zfs send tank/src@snap" "origin.example" 1 send
		)
	)
	status=$?

	assertEquals "Unsafe compression settings should abort wrapping." 1 "$status"
	assertContains "Missing safe compression commands should surface the validation error." \
		"$output" "Compression enabled but commands are not configured safely."
}

test_calculate_size_estimate_reports_incremental_probe_failures() {
	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				printf '%s\n' "probe failed"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
		)
	)
	status=$?

	assertEquals "Incremental size estimation failures should abort." 1 "$status"
	assertContains "Incremental estimate failures should mention the failed probe." \
		"$output" "Error calculating incremental estimate: probe failed"
}

test_calculate_size_estimate_reports_full_probe_failures() {
	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				printf '%s\n' "probe failed"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			calculate_size_estimate "tank/src@snap1" ""
		)
	)
	status=$?

	assertEquals "Full size estimation failures should abort." 1 "$status"
	assertContains "Full estimate failures should mention the failed probe." \
		"$output" "Error calculating estimate: probe failed"
}

test_handle_progress_bar_option_builds_passthrough_pipeline() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	result=$(
		calculate_size_estimate() {
			printf '%s\n' "4096"
		}
		handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
	)

	assertContains "Progress handling should preserve the progress passthrough helper." \
		"$result" "zxfer_progress_passthrough"
	assertContains "Progress handling should substitute the snapshot title." \
		"$result" "pv -s 4096 -N tank/src@snap2"
}

test_setup_progress_dialog_substitutes_estimate_and_snapshot_title() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	result=$(setup_progress_dialog "8192" "tank/src@snap9")

	assertEquals "Progress-dialog setup should substitute both the size estimate and snapshot title." \
		"pv -s 8192 -N tank/src@snap9" "$result"
}

test_wait_for_zfs_send_jobs_returns_immediately_when_empty() {
	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=5

	wait_for_zfs_send_jobs "unit"

	assertEquals "Waiting with no jobs should reset the running-job count." 0 "$g_count_zfs_send_jobs"
}

test_zxfer_progress_passthrough_falls_back_when_mktemp_fails() {
	log="$TEST_TMPDIR/progress_mktemp.log"
	output=$(
		printf 'payload\n' | (
			echoV() {
				printf '%s\n' "$1" >>"$log"
			}
			mktemp() {
				return 1
			}
			zxfer_progress_passthrough "cat >/dev/null"
		)
	)
	status=$?

	assertEquals "mktemp failures should fall back to a plain passthrough." 0 "$status"
	assertEquals "mktemp failure fallback should preserve stdin." "payload" "$output"
	assertContains "mktemp failure fallback should log the degraded path." \
		"$(cat "$log")" "Unable to create FIFO for progress bar"
}

test_zxfer_progress_passthrough_falls_back_when_mkfifo_fails() {
	log="$TEST_TMPDIR/progress_mkfifo.log"
	output=$(
		printf 'payload\n' | (
			echoV() {
				printf '%s\n' "$1" >>"$log"
			}
			mktemp() {
				printf '%s\n' "$TEST_TMPDIR/progress_fifo"
			}
			mkfifo() {
				return 1
			}
			zxfer_progress_passthrough "cat >/dev/null"
		)
	)
	status=$?

	assertEquals "mkfifo failures should fall back to a plain passthrough." 0 "$status"
	assertEquals "mkfifo failure fallback should preserve stdin." "payload" "$output"
	assertContains "mkfifo failure fallback should log the degraded path." \
		"$(cat "$log")" "Unable to mkfifo"
}

test_zxfer_progress_passthrough_falls_back_when_chmod_fails() {
	log="$TEST_TMPDIR/progress_chmod.log"
	output=$(
		printf 'payload\n' | (
			echoV() {
				printf '%s\n' "$1" >>"$log"
			}
			mktemp() {
				printf '%s\n' "$TEST_TMPDIR/progress_fifo"
			}
			mkfifo() {
				: >"$TEST_TMPDIR/progress_fifo"
			}
			chmod() {
				return 1
			}
			zxfer_progress_passthrough "cat >/dev/null"
		)
	)
	status=$?

	assertEquals "chmod failures should fall back to a plain passthrough." 0 "$status"
	assertEquals "chmod failure fallback should preserve stdin." "payload" "$output"
	assertContains "chmod failure fallback should log the degraded path." \
		"$(cat "$log")" "Unable to secure permissions"
}

test_zxfer_progress_passthrough_logs_progress_command_failures() {
	log="$TEST_TMPDIR/progress_status.log"
	output=$(
		printf 'payload\n' | (
			echoV() {
				printf '%s\n' "$1" >>"$log"
			}
			zxfer_progress_passthrough "cat >/dev/null; exit 7"
		)
	)
	status=$?

	assertEquals "Progress-command failures should preserve the tee exit status." 0 "$status"
	assertEquals "Progress-command failures should preserve the send stream." "payload" "$output"
	assertContains "Progress-command failures should be logged for operators." \
		"$(cat "$log")" "Progress bar command exited with status 7"
}

test_zfs_send_receive_runs_foreground_pipeline() {
	log="$TEST_TMPDIR/foreground_pipeline.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		printf 'performed=%s\n' "$g_is_performed_send_destroy" >>"$EXEC_LOG"
	)

	assertEquals "Foreground send/receive should execute a single pipeline." \
		"sendcmd | recvcmd
performed=1" "$(cat "$log")"
}

test_zfs_send_receive_backgrounds_pipeline_when_parallel_jobs_available() {
	log="$TEST_TMPDIR/background_pipeline.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_j_jobs=3
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		# shellcheck disable=SC2086
		set -- $g_zfs_send_job_pids
		wait "$@"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "$g_zfs_send_job_pids" >>"$EXEC_LOG"
	)

	assertContains "Background send/receive should execute the composed pipeline." \
		"$(cat "$log")" "sendcmd | recvcmd"
	assertContains "Background send/receive should increment the job count." \
		"$(cat "$log")" "count=1"
	assertContains "Background send/receive should track the spawned PID." \
		"$(cat "$log")" "pids="
}

test_zfs_send_receive_waits_at_job_limit_before_backgrounding() {
	log="$TEST_TMPDIR/job_limit.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		wait_for_zfs_send_jobs() {
			printf 'wait:%s\n' "$1" >>"$EXEC_LOG"
			g_count_zfs_send_jobs=0
		}
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_j_jobs=2
		g_count_zfs_send_jobs=2
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		# shellcheck disable=SC2086
		set -- $g_zfs_send_job_pids
		wait "$@"
	)

	assertEquals "Hitting the job limit should wait before spawning the next transfer." \
		"wait:job limit
sendcmd | recvcmd" "$(cat "$log")"
}

test_zfs_send_receive_invalid_job_limit_falls_back_to_single_job_mode() {
	log="$TEST_TMPDIR/job_limit_invalid.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_j_jobs="invalid"
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "${g_zfs_send_job_pids:-}" >>"$EXEC_LOG"
	)

	assertEquals "Invalid job limits should fall back to foreground execution without tracking background jobs." \
		"sendcmd | recvcmd
count=0
pids=" "$(cat "$log")"
}

test_zfs_send_receive_adds_remote_wrappers_and_progress_pipeline() {
	log="$TEST_TMPDIR/remote_progress.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		wrap_command_with_ssh() {
			printf '%s<%s:%s:%s>\n' "$1" "$2" "$3" "$4"
		}
		handle_progress_bar_option() {
			printf '%s\n' "| progress"
		}
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_option_z_compress=1
		g_option_D_display_progress_bar="pv"
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	assertEquals "Remote send/receive should wrap both ends and append the progress helper." \
		"sendcmd<origin.example:1:send> | progress | recvcmd<target.example:1:receive>" "$(cat "$log")"
}

. "$SHUNIT2_BIN"
