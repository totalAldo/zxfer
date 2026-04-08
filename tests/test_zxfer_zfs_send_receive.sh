#!/bin/sh
#
# shunit2 tests for zxfer_zfs_send_receive.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2317,SC2329

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=src/zxfer_common.sh
. "$ZXFER_ROOT/src/zxfer_common.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_secure_paths.sh
. "$ZXFER_ROOT/src/zxfer_secure_paths.sh"

# shellcheck source=src/zxfer_remote_cli.sh
. "$ZXFER_ROOT/src/zxfer_remote_cli.sh"

# shellcheck source=src/zxfer_backup_metadata.sh
. "$ZXFER_ROOT/src/zxfer_backup_metadata.sh"

# shellcheck source=src/zxfer_property_cache.sh
. "$ZXFER_ROOT/src/zxfer_property_cache.sh"

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
	g_option_w_raw_send=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_z_compress=0
	g_option_j_jobs=1
	g_option_F_force_rollback=""
	g_cmd_zfs="/sbin/zfs"
	g_cmd_compress_safe="gzip"
	g_cmd_decompress_safe="gunzip"
	g_origin_cmd_compress_safe="remote-gzip"
	g_origin_cmd_decompress_safe="remote-gunzip"
	g_target_cmd_compress_safe="target-gzip"
	g_target_cmd_decompress_safe="target-gunzip"
	g_zfs_send_job_pids=""
	g_zfs_send_job_records=""
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_unavailable=0
	g_count_zfs_send_jobs=0
	g_is_performed_send_destroy=0
	g_zxfer_failure_last_command=""
	g_delete_source_tmp_file=""
	g_delete_dest_tmp_file=""
	g_delete_snapshots_to_delete_tmp_file=""
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	zxfer_reset_destination_existence_cache
	TMPDIR="$TEST_TMPDIR"
	exec 8<&- 2>/dev/null || true
	zxfer_reset_failure_context "unit"
}

test_wrap_command_with_ssh_receive_direction_with_compression() {
	result=$(
		g_option_T_target_host="target.example doas"
		split_host_spec_tokens() { printf '%s\n%s\n' "target.example" "doas"; }
		build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'target-gunzip | zfs receive tank/dst'"; }
		build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'target-gunzip | zfs receive tank/dst'"; }
		wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive
	)

	assertEquals "Receive-side compression should wrap the remote command in the documented direction." \
		"gzip | '/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'target-gunzip | zfs receive tank/dst'" "$result"
}

test_wrap_command_with_ssh_without_compression_uses_remote_shell_wrapper_for_multi_token_hosts() {
	result=$(
		split_host_spec_tokens() { printf '%s\n%s\n' "origin.example" "pfexec"; }
		build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'zfs send tank/src@snap'"; }
		build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap'"; }
		wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 0 send
	)

	assertEquals "Non-compressed wrapper hosts should execute through a remote sh -c wrapper." \
		"'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap'" "$result"
}

test_wrap_command_with_ssh_send_direction_with_compression_and_wrapper_host() {
	result=$(
		g_option_O_origin_host="origin.example pfexec"
		split_host_spec_tokens() { printf '%s\n%s\n' "origin.example" "pfexec"; }
		build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'zfs send tank/src@snap | remote-gzip'"; }
		build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap | remote-gzip'"; }
		wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 1 send
	)

	assertEquals "Compressed send wrappers should compress remotely before piping back through the safe decompressor." \
		"'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap | remote-gzip' | gunzip" "$result"
}

test_wrap_command_with_ssh_send_direction_with_compression_and_simple_host() {
	result=$(
		g_option_O_origin_host="origin.example"
		split_host_spec_tokens() { printf '%s\n' "origin.example"; }
		build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'origin.example' 'zfs send tank/src@snap | remote-gzip'"; }
		wrap_command_with_ssh "zfs send tank/src@snap" "origin.example" 1 send
	)

	assertEquals "Compressed send wrappers on simple hosts should still append the safe local decompressor." \
		"'/usr/bin/ssh' 'origin.example' 'zfs send tank/src@snap | remote-gzip' | gunzip" "$result"
}

test_wrap_command_with_ssh_receive_direction_with_compression_and_simple_host() {
	result=$(
		g_option_T_target_host="target.example"
		split_host_spec_tokens() { printf '%s\n' "target.example"; }
		build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'target.example' 'target-gunzip | zfs receive tank/dst'"; }
		wrap_command_with_ssh "zfs receive tank/dst" "target.example" 1 receive
	)

	assertEquals "Compressed receive wrappers on simple hosts should stream through the safe compressor locally." \
		"gzip | '/usr/bin/ssh' 'target.example' 'target-gunzip | zfs receive tank/dst'" "$result"
}

test_wrap_command_with_ssh_rejects_missing_safe_compression_commands() {
	set +e
	output=$(
		(
			exec 8</dev/null
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

test_zxfer_progress_dialog_uses_size_estimate_detects_size_macro() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	if zxfer_progress_dialog_uses_size_estimate; then
		:
	else
		fail "Progress templates using %%size%% should request a size probe."
	fi

	g_option_D_display_progress_bar="pv -N %%title%%"
	if zxfer_progress_dialog_uses_size_estimate; then
		fail "Progress templates without %%size%% should skip size probing."
	fi
}

test_zxfer_should_use_fast_progress_estimate_prefers_remote_or_parallel_runs() {
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_j_jobs=1
	if zxfer_should_use_fast_progress_estimate; then
		fail "Local single-job runs should keep the exact progress estimate path."
	fi

	g_option_j_jobs=4
	if zxfer_should_use_fast_progress_estimate; then
		:
	else
		fail "Parallel runs should prefer the faster progress estimate path."
	fi

	g_option_j_jobs=1
	g_option_O_origin_host="origin.example"
	if zxfer_should_use_fast_progress_estimate; then
		:
	else
		fail "Remote origin runs should prefer the faster progress estimate path."
	fi

	g_option_O_origin_host=""
	g_option_T_target_host="target.example"
	if zxfer_should_use_fast_progress_estimate; then
		:
	else
		fail "Remote target runs should prefer the faster progress estimate path."
	fi
}

test_zxfer_should_use_fast_progress_estimate_treats_invalid_job_count_as_single_job() {
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_j_jobs="many"

	if zxfer_should_use_fast_progress_estimate; then
		fail "Invalid job counts should fall back to the exact local progress estimate path."
	fi
}

test_zxfer_extract_numeric_progress_estimate_rejects_nonnumeric_values() {
	set +e
	zxfer_extract_numeric_progress_estimate "not-a-number" >/dev/null 2>&1
	status=$?

	assertEquals "Non-numeric progress estimates should be rejected so callers can fall back safely." \
		"1" "$status"
}

test_zxfer_calculate_fast_incremental_size_estimate_rejects_non_snapshot_inputs() {
	set +e
	zxfer_calculate_fast_incremental_size_estimate "tank/src" "tank/src@snap1" >/dev/null 2>&1
	status=$?

	assertEquals "Fast incremental estimation should reject inputs that are not snapshot paths." \
		"1" "$status"
}

test_calculate_size_estimate_uses_fast_incremental_probe_when_requested() {
	log="$TEST_TMPDIR/fast_incremental_estimate.log"
	: >"$log"

	result=$(
		(
			LOG_FILE="$log"
			run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				printf '%s\n' "2048"
			}
			calculate_size_estimate "tank/src@snap2" "tank/src@snap1" 1
		)
	)

	assertEquals "Fast incremental estimation should return the cheaper written@snapshot value." \
		"2048" "$result"
	assertEquals "Fast incremental estimation should use one written@snapshot probe instead of an exact send estimate." \
		"get -Hpo value written@snap1 tank/src" "$(cat "$log")"
}

test_calculate_size_estimate_falls_back_to_exact_incremental_probe_when_fast_mode_fails() {
	log="$TEST_TMPDIR/fast_incremental_fallback.log"
	: >"$log"

	result=$(
		(
			LOG_FILE="$log"
			run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				if [ "$1" = "get" ]; then
					printf '%s\n' "unsupported"
					return 1
				elif [ "$1" = "send" ]; then
					printf 'size\t8192\n'
				fi
			}
			calculate_size_estimate "tank/src@snap2" "tank/src@snap1" 1
		)
	)

	assertEquals "Fast incremental estimation should fall back to the exact send estimate when the cheap probe is unavailable." \
		"8192" "$result"
	assertEquals "Fast incremental estimation should try the cheap probe first, then the exact send estimate." \
		"get -Hpo value written@snap1 tank/src
send -nPv -I tank/src@snap1 tank/src@snap2" "$(cat "$log")"
}

test_calculate_size_estimate_uses_fast_full_probe_when_requested() {
	log="$TEST_TMPDIR/fast_full_estimate.log"
	: >"$log"

	result=$(
		(
			LOG_FILE="$log"
			run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				printf '%s\n' "16384"
			}
			calculate_size_estimate "tank/src@snap2" "" 1
		)
	)

	assertEquals "Fast full estimation should return the cheaper referenced-space value." \
		"16384" "$result"
	assertEquals "Fast full estimation should use one referenced-size probe instead of an exact send estimate." \
		"list -Hp -o referenced tank/src@snap2" "$(cat "$log")"
}

test_calculate_size_estimate_falls_back_to_exact_full_probe_when_fast_mode_fails() {
	log="$TEST_TMPDIR/fast_full_fallback.log"
	: >"$log"

	result=$(
		(
			LOG_FILE="$log"
			g_option_V_very_verbose=1
			run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				if [ "$1" = "list" ]; then
					printf '%s\n' "unsupported"
					return 1
				fi
				printf 'size\t12288\n'
			}
			calculate_size_estimate "tank/src@snap2" "" 1
		) 2>&1
	)

	assertContains "Fast full estimation should log that it is falling back when the cheap probe fails." \
		"$result" "Falling back to exact full progress estimate for tank/src@snap2."
	assertContains "Fast full estimation should still return the exact send estimate after fallback." \
		"$result" "12288"
	assertEquals "Fast full estimation should try the cheap full probe before the exact send estimate." \
		"list -Hp -o referenced tank/src@snap2
send -nPv tank/src@snap2" "$(cat "$log")"
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

test_handle_progress_bar_option_skips_size_probe_when_size_macro_is_unused() {
	log="$TEST_TMPDIR/progress_no_size_probe.log"
	: >"$log"
	g_option_D_display_progress_bar="pv -N %%title%%"
	result=$(
		(
			LOG_FILE="$log"
			calculate_size_estimate() {
				printf '%s\n' "called" >>"$LOG_FILE"
				printf '%s\n' "4096"
			}
			handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
		)
	)

	assertEquals "Progress handling should skip size estimation when the dialog does not use %%size%%." \
		"" "$(cat "$log")"
	assertContains "Progress handling should still substitute the snapshot title when %%size%% is unused." \
		"$result" "pv -N tank/src@snap2"
}

test_handle_progress_bar_option_prefers_fast_estimate_for_remote_or_parallel_runs() {
	mode_log="$TEST_TMPDIR/progress_fast_mode.log"
	: >"$mode_log"
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	g_option_O_origin_host="origin.example"

	result=$(
		(
			MODE_LOG="$mode_log"
			calculate_size_estimate() {
				printf '%s\n' "$3" >"$MODE_LOG"
				printf '%s\n' "4096"
			}
			handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
		)
	)

	assertEquals "Remote progress handling should request the cheaper estimate mode." \
		"1" "$(cat "$mode_log")"
	assertContains "Fast-mode progress handling should still substitute the snapshot title and estimate." \
		"$result" "pv -s 4096 -N tank/src@snap2"
}

test_setup_progress_dialog_substitutes_estimate_and_snapshot_title() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	result=$(setup_progress_dialog "8192" "tank/src@snap9")

	assertEquals "Progress-dialog setup should substitute both the size estimate and snapshot title." \
		"pv -s 8192 -N tank/src@snap9" "$result"
}

test_get_send_command_display_includes_verbose_raw_flags_for_full_send() {
	g_option_V_very_verbose=1
	g_option_w_raw_send=1

	result=$(get_send_command "" "tank/src@snap9")

	assertEquals "Display-mode full sends should include verbose and raw flags when enabled." \
		"/sbin/zfs send -v -w tank/src@snap9" "$result"
}

test_wait_for_zfs_send_jobs_returns_immediately_when_empty() {
	g_zfs_send_job_pids=""
	g_count_zfs_send_jobs=5

	wait_for_zfs_send_jobs "unit"

	assertEquals "Waiting with no jobs should reset the running-job count." 0 "$g_count_zfs_send_jobs"
}

test_zxfer_open_send_job_completion_queue_marks_queue_unavailable_when_tempdir_setup_fails() {
	log="$TEST_TMPDIR/queue_tempdir_fail.log"

	(
		echoV() {
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
		echoV() {
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
		echoV() {
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
		echoV() {
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

test_zxfer_find_send_job_pid_by_status_file_returns_failure_for_unknown_status_files() {
	g_zfs_send_job_records="101	$TEST_TMPDIR/known_status"

	if zxfer_find_send_job_pid_by_status_file "$TEST_TMPDIR/unknown_status"; then
		fail "Unknown status files should not resolve to a tracked PID."
	fi
}

test_zxfer_unregister_send_job_removes_middle_pid_from_multi_pid_list() {
	status_one=$(mktemp "$TEST_TMPDIR/job_status_one.XXXXXX")
	status_two=$(mktemp "$TEST_TMPDIR/job_status_two.XXXXXX")
	status_three=$(mktemp "$TEST_TMPDIR/job_status_three.XXXXXX")
	g_zfs_send_job_pids="101 202 303"
	g_zfs_send_job_records="101	$status_one
202	$status_two
303	$status_three"
	g_count_zfs_send_jobs=3

	zxfer_unregister_send_job 202

	assertEquals "Unregistering a middle PID should preserve the remaining PID order." \
		"101 303" "$g_zfs_send_job_pids"
	assertEquals "Unregistering a tracked job should decrement the job count once." \
		2 "$g_count_zfs_send_jobs"
	assertFalse "Unregistering a tracked job should remove only its status file." \
		"[ -e '$status_two' ]"
	assertTrue "Unregistering a tracked job should leave unrelated status files intact." \
		"[ -e '$status_one' ]"
	assertTrue "Unregistering a tracked job should leave later status files intact." \
		"[ -e '$status_three' ]"
}

test_zxfer_run_background_pipeline_executes_command_and_reports_completion() {
	status_file=$(mktemp "$TEST_TMPDIR/bg_status.XXXXXX")
	queue_file=$(mktemp "$TEST_TMPDIR/bg_queue.XXXXXX")
	runner_status_file=$(mktemp "$TEST_TMPDIR/bg_runner_status.XXXXXX")
	log="$TEST_TMPDIR/bg_runner.log"
	: >"$log"

	(
		exec 8>>"$queue_file"
		g_zfs_send_job_queue_open=1
		echov() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_run_background_pipeline "printf 'runner-ok' >'$TEST_TMPDIR/bg_payload.txt'" "displaycmd" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	)
	status=$(tr -d '\r\n' <"$runner_status_file")
	completion_path=$(tr -d '\r\n' <"$queue_file")

	assertEquals "Background pipeline helper should return the eval exit status." 0 "$status"
	assertEquals "Background pipeline helper should write the child status to its status file." \
		"0" "$(tr -d '\r\n' <"$status_file")"
	assertEquals "Background pipeline helper should publish the completed status-file path into the queue." \
		"$status_file" "$completion_path"
	assertContains "Background pipeline helper should log the rendered display command." \
		"$(cat "$log")" "displaycmd"
	assertEquals "Background pipeline helper should execute the requested command body." \
		"runner-ok" "$(cat "$TEST_TMPDIR/bg_payload.txt")"
}

test_zxfer_run_background_pipeline_dry_run_skips_eval_and_returns_success() {
	status_file=$(mktemp "$TEST_TMPDIR/bg_dry_status.XXXXXX")
	runner_status_file=$(mktemp "$TEST_TMPDIR/bg_dry_runner_status.XXXXXX")
	log="$TEST_TMPDIR/bg_dry_runner.log"
	: >"$log"

	g_option_n_dryrun=1
	(
		echov() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_run_background_pipeline "touch '$TEST_TMPDIR/bg_dry_payload.txt'" "dry-display" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	)
	status=$(tr -d '\r\n' <"$runner_status_file")

	assertEquals "Dry-run background helpers should exit successfully." 0 "$status"
	assertEquals "Dry-run background helpers should still persist a zero status file." \
		"0" "$(tr -d '\r\n' <"$status_file")"
	assertFalse "Dry-run background helpers should not execute the eval command." \
		"[ -e '$TEST_TMPDIR/bg_dry_payload.txt' ]"
	assertContains "Dry-run background helpers should log the rendered dry-run command." \
		"$(cat "$log")" "Dry run: dry-display"
}

test_wait_for_next_zfs_send_job_completion_falls_back_when_queue_is_not_open() {
	log="$TEST_TMPDIR/wait_next_fallback.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		wait_for_zfs_send_jobs() {
			printf 'wait:%s\n' "$1" >>"$EXEC_LOG"
		}
		g_count_zfs_send_jobs=1
		g_zfs_send_job_queue_open=0
		g_zfs_send_job_records="101	$TEST_TMPDIR/status"
		wait_for_next_zfs_send_job_completion "unit"
	)

	assertEquals "Closed completion queues should fall back to the legacy wait helper." \
		"wait:unit" "$(cat "$log")"
}

test_wait_for_next_zfs_send_job_completion_uses_wait_status_when_status_file_is_nonnumeric() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_status.txt"
			queue_file="$TEST_TMPDIR/wait_next_queue.txt"
			printf '%s\n' "not-a-number" >"$status_file"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 7' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_close_send_job_completion_queue() {
				:
			}
			zxfer_terminate_remaining_send_jobs() {
				:
			}
			throw_error() {
				printf 'error:%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?

	assertEquals "Nonnumeric rolling-pool status files should fall back to the waited PID exit status." 1 "$status"
	assertContains "Rolling-pool fallback should preserve the waited PID in the failure message." \
		"$output" "error:zfs send/receive job failed (PID "
	assertContains "Rolling-pool fallback should preserve the waited exit status in the failure message." \
		"$output" "exit 7"
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

test_zxfer_progress_passthrough_uses_private_fifo_dir_under_physical_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/progress_real_tmp"
	link_tmp="$physical_tmpdir/progress_link_tmp"
	log="$TEST_TMPDIR/progress_private_dir.log"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	output=$(
		printf 'payload\n' | (
			LOG_FILE="$log"
			mkfifo() {
				printf '%s\n' "$1" >"$LOG_FILE"
				command mkfifo "$1"
			}
			zxfer_progress_passthrough "cat >/dev/null"
		)
	)
	status=$?
	recorded_fifo=$(cat "$log")

	assertEquals "Progress passthrough should preserve the send stream when using a private FIFO directory." 0 "$status"
	assertEquals "Progress passthrough should preserve stdin when using a private FIFO directory." "payload" "$output"
	case "$recorded_fifo" in
	"$real_tmp"/*/fifo) inside_real=0 ;;
	*) inside_real=1 ;;
	esac
	assertEquals "Progress FIFOs should be created under the physical TMPDIR target." 0 "$inside_real"
	assertFalse "Progress passthrough should clean up its private FIFO after use." "[ -e \"$recorded_fifo\" ]"
	assertFalse "Progress passthrough should remove the private parent directory after use." "[ -d \"${recorded_fifo%/*}\" ]"

	TMPDIR="$TEST_TMPDIR"
}

test_get_send_command_exec_treats_local_zfs_path_as_literal() {
	marker="$TEST_TMPDIR/send_exec_marker"
	old_cmd_zfs=$g_cmd_zfs
	g_cmd_zfs="/bin/echo; touch $marker #"

	cmd=$(get_send_command "" "tank/fs@snap1" "$g_cmd_zfs" "exec")

	if eval "$cmd" >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi
	g_cmd_zfs=$old_cmd_zfs

	: "$status"
	assertContains "Exec-mode send commands should quote the resolved zfs helper path." \
		"$cmd" "'/bin/echo; touch $marker #'"
	assertFalse "Exec-mode send commands should not execute shell metacharacters from the local zfs path." \
		"[ -e '$marker' ]"
}

test_get_receive_command_exec_treats_local_zfs_path_as_literal() {
	marker="$TEST_TMPDIR/recv_exec_marker"
	old_cmd_zfs=$g_cmd_zfs
	g_cmd_zfs="/bin/echo; touch $marker #"

	cmd=$(get_receive_command "tank/dst" "$g_cmd_zfs" "exec")

	if eval "$cmd" >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi
	g_cmd_zfs=$old_cmd_zfs

	: "$status"
	assertContains "Exec-mode receive commands should quote the resolved zfs helper path." \
		"$cmd" "'/bin/echo; touch $marker #'"
	assertFalse "Exec-mode receive commands should not execute shell metacharacters from the local zfs path." \
		"[ -e '$marker' ]"
}

test_zfs_send_receive_runs_foreground_pipeline() {
	log="$TEST_TMPDIR/foreground_pipeline.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echoV() { :; }
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

test_zfs_send_receive_invalidates_destination_cache_after_live_receive() {
	log="$TEST_TMPDIR/foreground_invalidation.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echoV() { :; }
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		execute_command() {
			printf 'exec=%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_invalidate_destination_property_cache() {
			printf 'invalidate=%s\n' "$1" >>"$EXEC_LOG"
		}
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	assertEquals "Successful live send/receive should invalidate the destination property cache for the receive dataset." \
		"exec=sendcmd | recvcmd
invalidate=backup/dst" "$(cat "$log")"
}

test_zfs_send_receive_marks_destination_hierarchy_exists_after_foreground_receive() {
	output=$(
		(
			echoV() { :; }
			get_send_command() { printf '%s\n' "sendcmd"; }
			get_receive_command() { printf '%s\n' "recvcmd"; }
			zxfer_mark_destination_root_missing_in_cache "backup"
			execute_command() {
				:
			}
			zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst/child" "0"
			printf 'root=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup")"
			printf 'parent=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'child=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"
			printf 'sibling=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/other")"
		)
	)

	assertContains "Foreground receives should mark the cache root as existing after success." \
		"$output" "root=1"
	assertContains "Foreground receives should mark parent datasets as existing after success." \
		"$output" "parent=1"
	assertContains "Foreground receives should mark the receive dataset as existing after success." \
		"$output" "child=1"
	assertContains "Unrelated descendants under the authoritative root should still be inferred missing." \
		"$output" "sibling=0"
}

test_zfs_send_receive_tracks_profile_counters_when_very_verbose() {
	log="$TEST_TMPDIR/foreground_pipeline_profile.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echoV() { :; }
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_V_very_verbose=1
		g_zxfer_profile_source_zfs_calls=0
		g_zxfer_profile_destination_zfs_calls=0
		g_zxfer_profile_zfs_send_calls=0
		g_zxfer_profile_zfs_receive_calls=0
		g_zxfer_profile_send_receive_pipeline_commands=0
		g_zxfer_profile_send_receive_background_pipeline_commands=0
		g_zxfer_profile_bucket_send_receive_setup=0
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		{
			printf 'source_zfs=%s\n' "${g_zxfer_profile_source_zfs_calls:-0}"
			printf 'destination_zfs=%s\n' "${g_zxfer_profile_destination_zfs_calls:-0}"
			printf 'send_calls=%s\n' "${g_zxfer_profile_zfs_send_calls:-0}"
			printf 'receive_calls=%s\n' "${g_zxfer_profile_zfs_receive_calls:-0}"
			printf 'pipelines=%s\n' "${g_zxfer_profile_send_receive_pipeline_commands:-0}"
			printf 'background=%s\n' "${g_zxfer_profile_send_receive_background_pipeline_commands:-0}"
			printf 'bucket=%s\n' "${g_zxfer_profile_bucket_send_receive_setup:-0}"
		} >>"$EXEC_LOG"
	)

	assertEquals "Very-verbose profiling should track foreground send/receive pipeline counts." \
		"sendcmd | recvcmd
source_zfs=1
destination_zfs=1
send_calls=1
receive_calls=1
pipelines=1
background=0
bucket=1" "$(cat "$log")"
}

test_zfs_send_receive_tracks_remote_ssh_profile_counters_when_very_verbose() {
	log="$TEST_TMPDIR/remote_pipeline_profile.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echoV() { :; }
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		wrap_command_with_ssh() {
			printf '%s\n' "$1 via $2"
		}
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_V_very_verbose=1
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_zxfer_profile_ssh_shell_invocations=0
		g_zxfer_profile_source_ssh_shell_invocations=0
		g_zxfer_profile_destination_ssh_shell_invocations=0
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		{
			printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
			printf 'source_ssh=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
			printf 'destination_ssh=%s\n' "${g_zxfer_profile_destination_ssh_shell_invocations:-0}"
		} >>"$EXEC_LOG"
	)

	assertEquals "Very-verbose profiling should count remote send/receive ssh hops once per side." \
		"sendcmd via origin.example | recvcmd via target.example
ssh=2
source_ssh=1
destination_ssh=1" "$(cat "$log")"
}

test_zfs_send_receive_tracks_remote_ssh_counters_when_origin_and_target_share_host_spec() {
	log="$TEST_TMPDIR/remote_pipeline_same_host_profile.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echoV() { :; }
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		wrap_command_with_ssh() {
			printf '%s\n' "$1 via $2"
		}
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_V_very_verbose=1
		g_option_O_origin_host="shared.example"
		g_option_T_target_host="shared.example"
		g_zxfer_profile_ssh_shell_invocations=0
		g_zxfer_profile_source_ssh_shell_invocations=0
		g_zxfer_profile_destination_ssh_shell_invocations=0
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		{
			printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
			printf 'source_ssh=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
			printf 'destination_ssh=%s\n' "${g_zxfer_profile_destination_ssh_shell_invocations:-0}"
		} >>"$EXEC_LOG"
	)

	assertEquals "Remote send/receive profiling should attribute source and destination ssh counts separately even when both ends share the same host spec." \
		"sendcmd via shared.example | recvcmd via shared.example
ssh=2
source_ssh=1
destination_ssh=1" "$(cat "$log")"
}

test_zfs_send_receive_dry_run_skips_actual_call_profile_counters() {
	log="$TEST_TMPDIR/dry_run_pipeline_profile.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echoV() { :; }
		echov() { :; }
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		g_option_n_dryrun=1
		g_option_V_very_verbose=1
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_zxfer_profile_source_zfs_calls=0
		g_zxfer_profile_destination_zfs_calls=0
		g_zxfer_profile_zfs_send_calls=0
		g_zxfer_profile_zfs_receive_calls=0
		g_zxfer_profile_ssh_shell_invocations=0
		g_zxfer_profile_send_receive_pipeline_commands=0
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		{
			printf 'source_zfs=%s\n' "${g_zxfer_profile_source_zfs_calls:-0}"
			printf 'destination_zfs=%s\n' "${g_zxfer_profile_destination_zfs_calls:-0}"
			printf 'send_calls=%s\n' "${g_zxfer_profile_zfs_send_calls:-0}"
			printf 'receive_calls=%s\n' "${g_zxfer_profile_zfs_receive_calls:-0}"
			printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
			printf 'pipelines=%s\n' "${g_zxfer_profile_send_receive_pipeline_commands:-0}"
		} >"$EXEC_LOG"
	)

	assertEquals "Dry-run send/receive should not claim actual zfs or ssh execution in the profile counters." \
		"source_zfs=0
destination_zfs=0
send_calls=0
receive_calls=0
ssh=0
pipelines=1" "$(cat "$log")"
}

test_zfs_send_receive_backgrounds_pipeline_when_parallel_jobs_available() {
	log="$TEST_TMPDIR/background_pipeline.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_run_background_pipeline() {
			printf '%s\n' "$2" >>"$EXEC_LOG"
			printf '0\n' >"$3"
			printf '%s\n' "$3" >&8
			exit 0
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

test_zfs_send_receive_appends_multiple_background_job_pids_and_logs_force_flag() {
	log="$TEST_TMPDIR/background_pipeline_multiple.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() {
			printf '%s\n' "sendcmd-$2"
		}
		get_receive_command() {
			printf '%s\n' "recvcmd"
		}
		echov() {
			printf 'verbose:%s\n' "$*" >>"$EXEC_LOG"
		}
		zxfer_run_background_pipeline() {
			printf '%s\n' "$2" >>"$EXEC_LOG"
			printf '0\n' >"$3"
			printf '%s\n' "$3" >&8
			exit 0
		}
		g_option_j_jobs=3
		g_option_F_force_rollback="-F"
		g_option_v_verbose=1
		zfs_send_receive "tank/src@snap0" "tank/src@snap1" "backup/dst" "1"
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		# shellcheck disable=SC2086
		set -- $g_zfs_send_job_pids
		wait "$@"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "$g_zfs_send_job_pids" >>"$EXEC_LOG"
	)

	assertContains "Background send/receive should log when the receive-side force flag is active." \
		"$(cat "$log")" "verbose:Receive-side force flag (-F) is active for destination [backup/dst]."
	assertContains "The first background transfer should still execute its composed pipeline." \
		"$(cat "$log")" "sendcmd-tank/src@snap1 | recvcmd"
	assertContains "The second background transfer should also execute its composed pipeline." \
		"$(cat "$log")" "sendcmd-tank/src@snap2 | recvcmd"
	assertContains "Launching multiple background transfers should append additional tracked PIDs instead of replacing the first one." \
		"$(cat "$log")" "count=2"
	assertContains "Launching multiple background transfers should retain the tracked PID list." \
		"$(cat "$log")" "pids="
}

test_zfs_send_receive_waits_at_job_limit_before_backgrounding() {
	log="$TEST_TMPDIR/job_limit.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			return 1
		}
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
		if [ -n "${g_zfs_send_job_pids:-}" ]; then
			# shellcheck disable=SC2086
			set -- $g_zfs_send_job_pids
			wait "$@"
		fi
	)

	assertEquals "Hitting the job limit should wait before spawning the next transfer." \
		"wait:job limit
sendcmd | recvcmd" "$(cat "$log")"
}

test_zfs_send_receive_falls_back_to_legacy_background_path_when_queue_is_unavailable() {
	log="$TEST_TMPDIR/job_limit_legacy_fallback.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		get_send_command() { printf '%s\n' "sendcmd"; }
		get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			return 1
		}
		execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_j_jobs=2
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		wait_for_zfs_send_jobs "final sync"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "$g_zfs_send_job_pids" >>"$EXEC_LOG"
	)

	assertEquals "Unavailable rolling queues should fall back to the legacy background path." \
		"sendcmd | recvcmd
count=0
pids=" "$(cat "$log")"
}

test_zfs_send_receive_uses_rolling_pool_when_a_job_finishes_early() {
	log="$TEST_TMPDIR/rolling_pool.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		echov() { :; }
		get_send_command() {
			printf '%s\n' "sendcmd-$2"
		}
		get_receive_command() {
			printf '%s\n' "recvcmd"
		}
		zxfer_run_background_pipeline() {
			l_exec_cmd=$1
			l_status_file=$3
			if echo "$l_exec_cmd" | grep -q 'sendcmd-tank/src@snap1'; then
				printf 'start:1\n' >>"$EXEC_LOG"
				sleep 1
				printf '0\n' >"$l_status_file"
				printf '%s\n' "$l_status_file" >&8
				printf 'end:1\n' >>"$EXEC_LOG"
				exit 0
			elif echo "$l_exec_cmd" | grep -q 'sendcmd-tank/src@snap2'; then
				printf 'start:2\n' >>"$EXEC_LOG"
				printf '0\n' >"$l_status_file"
				printf '%s\n' "$l_status_file" >&8
				printf 'end:2\n' >>"$EXEC_LOG"
				exit 0
			elif echo "$l_exec_cmd" | grep -q 'sendcmd-tank/src@snap3'; then
				printf 'start:3\n' >>"$EXEC_LOG"
				printf '0\n' >"$l_status_file"
				printf '%s\n' "$l_status_file" >&8
				printf 'end:3\n' >>"$EXEC_LOG"
				exit 0
			fi
			printf '99\n' >"$l_status_file"
			printf '%s\n' "$l_status_file" >&8
			exit 99
		}
		g_option_j_jobs=2
		zfs_send_receive "tank/src@base" "tank/src@snap1" "backup/dst" "1"
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		zfs_send_receive "tank/src@snap2" "tank/src@snap3" "backup/dst" "1"
		wait_for_zfs_send_jobs "final sync"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "${g_zfs_send_job_pids:-}" >>"$EXEC_LOG"
	)

	line_start3=$(grep -n '^start:3$' "$log" | cut -d: -f1)
	line_end1=$(grep -n '^end:1$' "$log" | cut -d: -f1)

	assertContains "Rolling background scheduling should run the second job to completion." \
		"$(cat "$log")" "end:2"
	assertContains "Rolling background scheduling should eventually run the third job too." \
		"$(cat "$log")" "end:3"
	assertTrue "The third job should start before the first slow job finishes when a slot frees up early." \
		"[ '$line_start3' -lt '$line_end1' ]"
	assertContains "Final waits should drain the rolling job pool and clear the count." \
		"$(cat "$log")" "count=0"
	assertContains "Final waits should clear the tracked PID list." \
		"$(cat "$log")" "pids="
}

test_zfs_send_receive_rolling_pool_fails_fast_and_kills_inflight_jobs() {
	log="$TEST_TMPDIR/rolling_pool_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			EXEC_LOG="$log"
			echov() { :; }
			get_send_command() {
				printf '%s\n' "sendcmd-$2"
			}
			get_receive_command() {
				printf '%s\n' "recvcmd"
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_run_background_pipeline() {
				l_exec_cmd=$1
				l_status_file=$3
				if echo "$l_exec_cmd" | grep -q 'sendcmd-tank/src@snap1'; then
					trap 'printf "killed:1\n" >>"$EXEC_LOG"; printf "143\n" >"$l_status_file"; printf "%s\n" "$l_status_file" >&8 2>/dev/null || :; exit 143' TERM
					printf 'start:1\n' >>"$EXEC_LOG"
					while :; do
						sleep 1
					done
				elif echo "$l_exec_cmd" | grep -q 'sendcmd-tank/src@snap2'; then
					printf 'start:2\n' >>"$EXEC_LOG"
					printf '7\n' >"$l_status_file"
					printf '%s\n' "$l_status_file" >&8
					exit 7
				elif echo "$l_exec_cmd" | grep -q 'sendcmd-tank/src@snap3'; then
					printf 'start:3\n' >>"$EXEC_LOG"
					printf '0\n' >"$l_status_file"
					printf '%s\n' "$l_status_file" >&8
					exit 0
				fi
				printf '99\n' >"$l_status_file"
				printf '%s\n' "$l_status_file" >&8
				exit 99
			}
			g_option_j_jobs=2
			zfs_send_receive "tank/src@base" "tank/src@snap1" "backup/dst" "1"
			zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
			zfs_send_receive "tank/src@snap2" "tank/src@snap3" "backup/dst" "1"
		)
	)
	status=$?
	set -e

	for _ in 1 2 3 4 5; do
		grep -q '^killed:1$' "$log" 2>/dev/null && break
		sleep 1
	done

	assertEquals "A failed background transfer should abort before scheduling more work." 1 "$status"
	assertContains "The failure should report the failing background PID and exit status." \
		"$output" "zfs send/receive job failed (PID "
	assertContains "The failure should report the non-zero child exit status." \
		"$output" "exit 7)."
	assertContains "The first inflight job should have started before the failure was observed." \
		"$(cat "$log")" "start:1"
	assertContains "The failing job should also have started." \
		"$(cat "$log")" "start:2"
	assertContains "Fail-fast handling should terminate the other inflight jobs." \
		"$(cat "$log")" "killed:1"
	assertNotContains "The rolling pool should stop scheduling new jobs after the first failure." \
		"$(cat "$log")" "start:3"
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

test_zfs_send_receive_uses_resolved_remote_zfs_paths() {
	log="$TEST_TMPDIR/remote_zfs_paths.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		execute_command() {
			printf 'exec=%s\n' "$1" >>"$EXEC_LOG"
			printf 'display=%s\n' "$3" >>"$EXEC_LOG"
		}
		g_cmd_ssh="/usr/bin/ssh"
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_origin_cmd_zfs="/remote/origin/zfs"
		g_target_cmd_zfs="/remote/target/zfs"
		zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)
	display_line=$(grep '^display=' "$log")

	assertContains "The exec pipeline should use the resolved origin-host zfs path." \
		"$(cat "$log")" "/remote/origin/zfs"
	assertContains "The exec pipeline should use the resolved target-host zfs path." \
		"$(cat "$log")" "/remote/target/zfs"
	assertContains "The display pipeline should also use the resolved origin-host zfs path." \
		"$display_line" "/remote/origin/zfs"
	assertContains "The display pipeline should also use the resolved target-host zfs path." \
		"$display_line" "/remote/target/zfs"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
