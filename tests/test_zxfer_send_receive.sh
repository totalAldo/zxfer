#!/bin/sh
#
# shunit2 tests for zxfer_send_receive.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_send_receive.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_send_receive"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
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
	g_cmd_ps=${g_cmd_ps:-$(command -v ps 2>/dev/null || printf '%s\n' ps)}
	g_zfs_send_job_pids=""
	g_zfs_send_job_records=""
	g_zfs_send_job_supervisor_records=""
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_unavailable=0
	g_zfs_send_job_queue_path=""
	g_zfs_send_job_queue_dir=""
	g_zfs_send_job_queue_writer_open=0
	g_zxfer_send_job_status_file_exit_status=""
	g_zxfer_send_job_status_file_report_failure=""
	g_zxfer_send_job_record_job_id=""
	g_zxfer_send_job_record_runner_pid=""
	g_zxfer_send_job_record_source_dataset=""
	g_zxfer_send_job_record_source_snapshot=""
	g_zxfer_send_job_record_dest_dataset=""
	g_zxfer_send_job_record_target_host=""
	g_zxfer_send_job_conflict_job_id=""
	g_zxfer_send_job_conflict_dest_dataset=""
	g_zxfer_send_job_conflict_target_host=""
	g_zxfer_send_job_error_context_result=""
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
	g_zxfer_progress_size_estimate_result=""
	g_zxfer_progress_bar_command_result=""
	zxfer_reset_destination_existence_cache
	zxfer_reset_background_job_state
	zxfer_reset_cleanup_pid_tracking
	TMPDIR="$TEST_TMPDIR"
	exec 8<&- 2>/dev/null || true
	exec 9<&- 2>/dev/null || true
	zxfer_reset_failure_context "unit"
}

test_wrap_command_with_ssh_receive_direction_with_compression() {
	result=$(
		g_option_T_target_host="target.example doas"
		zxfer_split_host_spec_tokens() { printf '%s\n%s\n' "target.example" "doas"; }
		zxfer_build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'target-gunzip | zfs receive tank/dst'"; }
		zxfer_build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'target-gunzip | zfs receive tank/dst'"; }
		zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive
	)

	assertEquals "Receive-side compression should wrap the remote command in the documented direction." \
		"gzip | '/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'target-gunzip | zfs receive tank/dst'" "$result"
}

test_wrap_command_with_ssh_without_compression_uses_remote_shell_wrapper_for_multi_token_hosts() {
	result=$(
		zxfer_split_host_spec_tokens() { printf '%s\n%s\n' "origin.example" "pfexec"; }
		zxfer_build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'zfs send tank/src@snap'"; }
		zxfer_build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 0 send
	)

	assertEquals "Non-compressed wrapper hosts should execute through a remote sh -c wrapper." \
		"'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap'" "$result"
}

test_wrap_command_with_ssh_send_direction_with_compression_and_wrapper_host() {
	result=$(
		g_option_O_origin_host="origin.example pfexec"
		zxfer_split_host_spec_tokens() { printf '%s\n%s\n' "origin.example" "pfexec"; }
		zxfer_build_remote_sh_c_command() { printf '%s\n' "'sh' '-c' 'zfs send tank/src@snap | remote-gzip'"; }
		zxfer_build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap | remote-gzip'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 1 send
	)

	assertEquals "Compressed send wrappers should compress remotely before piping back through the safe decompressor." \
		"'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap | remote-gzip' | gunzip" "$result"
}

test_wrap_command_with_ssh_send_direction_with_compression_and_simple_host() {
	result=$(
		g_option_O_origin_host="origin.example"
		zxfer_split_host_spec_tokens() { printf '%s\n' "origin.example"; }
		zxfer_build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'origin.example' 'zfs send tank/src@snap | remote-gzip'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example" 1 send
	)

	assertEquals "Compressed send wrappers on simple hosts should still append the safe local decompressor." \
		"'/usr/bin/ssh' 'origin.example' 'zfs send tank/src@snap | remote-gzip' | gunzip" "$result"
}

test_wrap_command_with_ssh_receive_direction_with_compression_and_simple_host() {
	result=$(
		g_option_T_target_host="target.example"
		zxfer_split_host_spec_tokens() { printf '%s\n' "target.example"; }
		zxfer_build_ssh_shell_command_for_host() { printf '%s\n' "'/usr/bin/ssh' 'target.example' 'target-gunzip | zfs receive tank/dst'"; }
		zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example" 1 receive
	)

	assertEquals "Compressed receive wrappers on simple hosts should stream through the safe compressor locally." \
		"gzip | '/usr/bin/ssh' 'target.example' 'target-gunzip | zfs receive tank/dst'" "$result"
}

test_wrap_command_with_ssh_rejects_missing_safe_compression_commands() {
	set +e
	output=$(
		(
			exec 8</dev/null
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_compress_safe=""
			g_cmd_decompress_safe=""
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example" 1 send
		)
	)
	status=$?

	assertEquals "Unsafe compression settings should abort wrapping." 1 "$status"
	assertContains "Missing safe compression commands should surface the validation error." \
		"$output" "Compression enabled but commands are not configured safely."
}

test_wrap_command_with_ssh_preserves_remote_wrapper_builder_status() {
	set +e
	output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "origin.example" "pfexec"
			}
			zxfer_build_remote_sh_c_command() {
				return 73
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 0 send
		)
	)
	status=$?

	assertEquals "SSH command wrapping should preserve the exact remote-shell wrapper builder status." \
		73 "$status"
	assertEquals "SSH command wrapping should not emit a partial wrapped command when remote-shell wrapper construction fails." \
		"" "$output"
}

test_wrap_command_with_ssh_preserves_compressed_builder_failures_for_all_host_shapes() {
	set +e
	send_wrapper_output=$(
		(
			g_option_O_origin_host="origin.example pfexec"
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "origin.example" "pfexec"
			}
			zxfer_build_remote_sh_c_command() {
				return 81
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 1 send
		)
	)
	send_wrapper_status=$?
	send_transport_output=$(
		(
			g_option_O_origin_host="origin.example pfexec"
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "origin.example" "pfexec"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "'sh' '-c' 'zfs send tank/src@snap | remote-gzip'"
			}
			zxfer_build_ssh_shell_command_for_host() {
				return 82
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 1 send
		)
	)
	send_transport_status=$?
	send_simple_output=$(
		(
			g_option_O_origin_host="origin.example"
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "origin.example"
			}
			zxfer_build_ssh_shell_command_for_host() {
				return 83
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example" 1 send
		)
	)
	send_simple_status=$?
	receive_wrapper_output=$(
		(
			g_option_T_target_host="target.example doas"
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "target.example" "doas"
			}
			zxfer_build_remote_sh_c_command() {
				return 84
			}
			zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive
		)
	)
	receive_wrapper_status=$?
	receive_transport_output=$(
		(
			g_option_T_target_host="target.example doas"
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "target.example" "doas"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "'sh' '-c' 'target-gunzip | zfs receive tank/dst'"
			}
			zxfer_build_ssh_shell_command_for_host() {
				return 85
			}
			zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive
		)
	)
	receive_transport_status=$?
	receive_simple_output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "target.example"
			}
			zxfer_build_ssh_shell_command_for_host() {
				return 86
			}
			zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example" 1 receive
		)
	)
	receive_simple_status=$?
	set -e

	assertEquals "Compressed send wrapping should preserve remote-shell wrapper failures for multi-token hosts." \
		81 "$send_wrapper_status"
	assertEquals "Compressed send wrapping should not emit partial output when remote-shell wrapper creation fails." \
		"" "$send_wrapper_output"
	assertEquals "Compressed send wrapping should preserve ssh wrapper failures for multi-token hosts." \
		82 "$send_transport_status"
	assertEquals "Compressed send wrapping should not emit partial output when ssh wrapper construction fails for multi-token hosts." \
		"" "$send_transport_output"
	assertEquals "Compressed send wrapping should preserve ssh wrapper failures for simple hosts." \
		83 "$send_simple_status"
	assertEquals "Compressed send wrapping should not emit partial output when ssh wrapper construction fails for simple hosts." \
		"" "$send_simple_output"
	assertEquals "Compressed receive wrapping should preserve remote-shell wrapper failures for multi-token hosts." \
		84 "$receive_wrapper_status"
	assertEquals "Compressed receive wrapping should not emit partial output when remote-shell wrapper creation fails." \
		"" "$receive_wrapper_output"
	assertEquals "Compressed receive wrapping should preserve ssh wrapper failures for multi-token hosts." \
		85 "$receive_transport_status"
	assertEquals "Compressed receive wrapping should not emit partial output when ssh wrapper construction fails for multi-token hosts." \
		"" "$receive_transport_output"
	assertEquals "Compressed receive wrapping should preserve ssh wrapper failures for simple hosts." \
		86 "$receive_simple_status"
	assertEquals "Compressed receive wrapping should not emit partial output when ssh wrapper construction fails for simple hosts." \
		"" "$receive_simple_output"
}

test_wrap_command_with_ssh_rethrows_host_spec_split_failures() {
	set +e
	output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid host spec"
				return 74
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 0 send
		)
	)
	status=$?

	assertEquals "SSH wrapping should fail closed when host-spec token splitting fails." \
		1 "$status"
	assertContains "SSH wrapping should preserve the host-spec split diagnostic." \
		"$output" "invalid host spec"
}

test_zxfer_reset_send_receive_state_clears_queue_and_progress_scratch() {
	g_count_zfs_send_jobs=3
	g_zfs_send_job_pids="111 222"
	g_zfs_send_job_records="111	$TEST_TMPDIR/status.111"
	g_zfs_send_job_queue_open=1
	g_zfs_send_job_queue_unavailable=1
	g_zfs_send_job_queue_path="$TEST_TMPDIR/queue"
	g_zfs_send_job_queue_dir="$TEST_TMPDIR/queue-dir"
	g_zfs_send_job_queue_writer_open=1
	g_zxfer_send_job_status_file_exit_status="9"
	g_zxfer_send_job_status_file_report_failure="queue_write"
	g_zxfer_progress_size_estimate_result="4096"
	g_zxfer_progress_probe_output_result="size	4096"
	g_zxfer_progress_bar_command_result="| pv"

	zxfer_reset_send_receive_state

	assertEquals "Resetting send/receive state should clear tracked job counts." \
		0 "${g_count_zfs_send_jobs:-0}"
	assertEquals "Resetting send/receive state should clear tracked background PIDs." \
		"" "$g_zfs_send_job_pids"
	assertEquals "Resetting send/receive state should clear tracked job status records." \
		"" "$g_zfs_send_job_records"
	assertEquals "Resetting send/receive state should close the queue state." \
		0 "${g_zfs_send_job_queue_open:-0}"
	assertEquals "Resetting send/receive state should clear queue unavailability scratch." \
		0 "${g_zfs_send_job_queue_unavailable:-0}"
	assertEquals "Resetting send/receive state should clear the queue path scratch." \
		"" "$g_zfs_send_job_queue_path"
	assertEquals "Resetting send/receive state should clear the queue directory scratch." \
		"" "$g_zfs_send_job_queue_dir"
	assertEquals "Resetting send/receive state should clear the queue writer-open marker." \
		0 "${g_zfs_send_job_queue_writer_open:-0}"
	assertEquals "Resetting send/receive state should clear staged send/receive status scratch." \
		"" "$g_zxfer_send_job_status_file_exit_status"
	assertEquals "Resetting send/receive state should clear staged send/receive failure scratch." \
		"" "$g_zxfer_send_job_status_file_report_failure"
	assertEquals "Resetting send/receive state should clear cached progress estimates." \
		"" "$g_zxfer_progress_size_estimate_result"
	assertEquals "Resetting send/receive state should clear cached progress probe output." \
		"" "$g_zxfer_progress_probe_output_result"
	assertEquals "Resetting send/receive state should clear the staged progress-wrapper command." \
		"" "$g_zxfer_progress_bar_command_result"
}

test_zxfer_read_progress_estimate_capture_file_trims_trailing_newlines_and_handles_blank_paths() {
	capture_file="$TEST_TMPDIR/progress_estimate_capture.txt"
	printf '%s\n' "size	4096" >"$capture_file"

	blank_output=$(zxfer_read_progress_estimate_capture_file "")
	blank_status=$?
	zxfer_read_progress_estimate_capture_file "$capture_file" >/dev/null
	status=$?

	assertEquals "Progress-estimate capture reads should accept blank paths as an empty no-op." \
		0 "$blank_status"
	assertEquals "Progress-estimate capture reads should keep stdout clean when no capture path is supplied." \
		"" "$blank_output"
	assertEquals "Progress-estimate capture reads should succeed for staged capture files." \
		0 "$status"
	assertEquals "Progress-estimate capture reads should trim the trailing newline from staged probe output." \
		"size	4096" "$g_zxfer_progress_probe_output_result"
}

test_calculate_size_estimate_reports_incremental_probe_failures() {
	l_stdout_file=$TEST_TMPDIR/size_estimate_incremental_probe.stdout
	l_stderr_file=$TEST_TMPDIR/size_estimate_incremental_probe.stderr

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell_split.
	zxfer_test_capture_subshell_split "$l_stdout_file" "$l_stderr_file" '
		zxfer_run_source_zfs_cmd() {
			l_restore_xtrace=0
			case $- in
			*x*)
				l_restore_xtrace=1
				set +x
				;;
			esac
			printf "%s\n" "probe failed"
			if [ "$l_restore_xtrace" -eq 1 ]; then
				set -x
			fi
			return 1
		}
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
	'

	assertEquals "Incremental size estimation failures should abort." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Incremental estimate failures should preserve the operator-facing error prefix." \
		"$(cat "$l_stdout_file")" "Error calculating incremental estimate:"
}

test_calculate_size_estimate_reports_full_probe_failures() {
	l_stdout_file=$TEST_TMPDIR/size_estimate_full_probe.stdout
	l_stderr_file=$TEST_TMPDIR/size_estimate_full_probe.stderr

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell_split.
	zxfer_test_capture_subshell_split "$l_stdout_file" "$l_stderr_file" '
		zxfer_run_source_zfs_cmd() {
			l_restore_xtrace=0
			case $- in
			*x*)
				l_restore_xtrace=1
				set +x
				;;
			esac
			printf "%s\n" "probe failed"
			if [ "$l_restore_xtrace" -eq 1 ]; then
				set -x
			fi
			return 1
		}
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_calculate_size_estimate "tank/src@snap1" ""
	'

	assertEquals "Full size estimation failures should abort." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Full estimate failures should preserve the operator-facing error prefix." \
		"$(cat "$l_stdout_file")" "Error calculating estimate:"
}

test_calculate_size_estimate_reports_incremental_parse_failures() {
	l_stdout_file=$TEST_TMPDIR/size_estimate_incremental_parse.stdout
	l_stderr_file=$TEST_TMPDIR/size_estimate_incremental_parse.stderr

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell_split.
	zxfer_test_capture_subshell_split "$l_stdout_file" "$l_stderr_file" '
		zxfer_run_source_zfs_cmd() {
			l_restore_xtrace=0
			case $- in
			*x*)
				l_restore_xtrace=1
				set +x
				;;
			esac
			printf "%s\n" "size	not-a-number"
			if [ "$l_restore_xtrace" -eq 1 ]; then
				set -x
			fi
		}
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
	'

	assertEquals "Incremental size estimation should fail closed when the exact probe output has no numeric size." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Incremental parse failures should preserve the operator-facing parse-error prefix." \
		"$(cat "$l_stdout_file")" "Error parsing incremental estimate:"
}

test_calculate_size_estimate_reports_full_parse_failures() {
	l_stdout_file=$TEST_TMPDIR/size_estimate_full_parse.stdout
	l_stderr_file=$TEST_TMPDIR/size_estimate_full_parse.stderr

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell_split.
	zxfer_test_capture_subshell_split "$l_stdout_file" "$l_stderr_file" '
		zxfer_run_source_zfs_cmd() {
			l_restore_xtrace=0
			case $- in
			*x*)
				l_restore_xtrace=1
				set +x
				;;
			esac
			printf "%s\n" "full\ttank/src@snap1\tinvalid"
			if [ "$l_restore_xtrace" -eq 1 ]; then
				set -x
			fi
		}
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_calculate_size_estimate "tank/src@snap1" ""
	'

	assertEquals "Full size estimation should fail closed when the exact probe output has no numeric size." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Full parse failures should preserve the operator-facing parse-error prefix." \
		"$(cat "$l_stdout_file")" "Error parsing estimate:"
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

test_zxfer_extract_numeric_progress_estimate_accepts_exact_send_probe_output() {
	result=$(zxfer_extract_numeric_progress_estimate "$(printf 'full\ttank/src@snap1\t13424\nsize\t13424\n')")

	assertEquals "Exact send-probe output should still yield the numeric size record." \
		"13424" "$result"
}

test_zxfer_extract_numeric_progress_estimate_parses_size_probe_footer_via_awk() {
	result=$(zxfer_extract_numeric_progress_estimate "$(printf '%s\n%s\n' \
		"full send estimate" \
		"size	8192")")

	assertEquals "Non-bare size probe output should still yield the numeric size footer through the awk fallback." \
		"8192" "$result"
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
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				printf '%s\n' "2048"
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1" 1
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
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				if [ "$1" = "get" ]; then
					printf '%s\n' "unsupported"
					return 1
				elif [ "$1" = "send" ]; then
					printf 'size\t8192\n'
				fi
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1" 1
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
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				printf '%s\n' "16384"
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "" 1
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
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				if [ "$1" = "list" ]; then
					printf '%s\n' "unsupported"
					return 1
				fi
				printf 'size\t12288\n'
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "" 1
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

test_calculate_size_estimate_accepts_incremental_probe_size_output_when_probe_status_is_nonzero() {
	result=$(
		(
			zxfer_capture_progress_estimate_probe_output() {
				g_zxfer_progress_probe_output_result=$(printf 'incremental\ttank/src@snap1\ttank/src@snap2\t2048\nsize\t2048\n')
				return 1
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
		)
	)

	assertEquals "Incremental size estimation should keep a usable size record even when the exact dry-run probe exits nonzero." \
		"2048" "$result"
}

test_calculate_size_estimate_accepts_full_probe_size_output_when_probe_status_is_nonzero() {
	result=$(
		(
			zxfer_capture_progress_estimate_probe_output() {
				g_zxfer_progress_probe_output_result=$(printf 'full\ttank/src@snap1\t13424\nsize\t13424\n')
				return 1
			}
			zxfer_calculate_size_estimate "tank/src@snap1" ""
		)
	)

	assertEquals "Full size estimation should keep a usable size record even when the exact dry-run probe exits nonzero." \
		"13424" "$result"
}

test_handle_progress_bar_option_builds_passthrough_pipeline() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	result=$(
		zxfer_calculate_size_estimate() {
			g_zxfer_progress_size_estimate_result="4096"
		}
		zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
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
			zxfer_calculate_size_estimate() {
				printf '%s\n' "called" >>"$LOG_FILE"
				printf '%s\n' "4096"
			}
			zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
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
			zxfer_calculate_size_estimate() {
				printf '%s\n' "$3" >"$MODE_LOG"
				g_zxfer_progress_size_estimate_result="4096"
			}
			zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
		)
	)

	assertEquals "Remote progress handling should request the cheaper estimate mode." \
		"1" "$(cat "$mode_log")"
	assertContains "Fast-mode progress handling should still substitute the snapshot title and estimate." \
		"$result" "pv -s 4096 -N tank/src@snap2"
}

test_handle_progress_bar_option_rethrows_size_estimate_failures() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	set +e
	output=$(
		(
			zxfer_calculate_size_estimate() {
				zxfer_throw_error "estimate failed"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
		) 2>&1
	)
	status=$?

	assertEquals "Progress handling should abort when the live size estimator fails." \
		1 "$status"
	assertContains "Progress handling should surface the live size-estimate failure instead of rendering an empty-size wrapper." \
		"$output" "estimate failed"
}

test_handle_progress_bar_option_throws_when_size_estimate_result_is_empty() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	set +e
	output=$(
		(
			zxfer_calculate_size_estimate() {
				g_zxfer_progress_size_estimate_result=""
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
		)
	)
	status=$?
	set -e

	assertEquals "Progress handling should fail closed when the size-estimate helper returns success without publishing an estimate." \
		1 "$status"
	assertContains "Empty size-estimate results should surface the dedicated progress size-estimate failure." \
		"$output" "Failed to calculate progress size estimate for tank/src@snap2."
}

test_handle_progress_bar_option_propagates_nonthrowing_size_estimate_failures() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	set +e
	(
		zxfer_calculate_size_estimate() {
			return 17
		}
		zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1" >/dev/null
	)
	status=$?

	assertEquals "Progress handling should preserve non-throwing size-estimate failures instead of converting them to success." \
		17 "$status"
}

test_handle_progress_bar_option_skips_size_probe_in_dry_run() {
	probe_log="$TEST_TMPDIR/progress_dry_run_probe.log"
	estimate_log="$TEST_TMPDIR/progress_dry_run_estimate.log"
	: >"$probe_log"
	: >"$estimate_log"
	g_option_n_dryrun=1
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	output=$(
		(
			PROBE_LOG="$probe_log"
			ESTIMATE_LOG="$estimate_log"
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_calculate_size_estimate() {
				printf '%s\n' "$*" >>"$ESTIMATE_LOG"
				return 1
			}
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$PROBE_LOG"
				printf '%s\n' "4096"
			}
			zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
		)
	)

	assertEquals "Dry-run progress handling should not probe the live source for %%size%%." \
		"" "$(cat "$probe_log")"
	assertEquals "Dry-run progress handling should not call the live size-estimator helper at all." \
		"" "$(cat "$estimate_log")"
	assertContains "Dry-run progress handling should explain that the live %%size%% probe is skipped." \
		"$output" "Dry run: skipping live %%size%% progress estimate discovery."
	assertContains "Dry-run progress handling should render an explicit unknown-size placeholder in the preview pipeline." \
		"$output" "pv -s UNKNOWN -N tank/src@snap2"
}

test_setup_progress_dialog_substitutes_estimate_and_snapshot_title() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	result=$(zxfer_setup_progress_dialog "8192" "tank/src@snap9")

	assertEquals "Progress-dialog setup should substitute both the size estimate and snapshot title." \
		"pv -s 8192 -N tank/src@snap9" "$result"
}

test_setup_progress_dialog_substitutes_estimate_and_snapshot_title_in_current_shell() {
	output_file="$TEST_TMPDIR/setup_progress_dialog_current_shell.out"
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"

	zxfer_setup_progress_dialog "8192" "tank/src@snap9" >"$output_file"

	assertEquals "Direct progress-dialog setup calls should still substitute both the size estimate and snapshot title." \
		"pv -s 8192 -N tank/src@snap9" "$(cat "$output_file")"
}

test_zxfer_capture_progress_estimate_probe_output_preserves_readback_failure_status() {
	set +e
	(
		zxfer_read_progress_estimate_capture_file() {
			return 23
		}
		zxfer_capture_progress_estimate_probe_output sh -c "printf '%s\n' 'size\t4096'"
	)
	status=$?

	assertEquals "Progress-estimate capture should preserve staged readback failures exactly." \
		23 "$status"
}

test_zxfer_capture_progress_estimate_probe_output_preserves_tempfile_allocation_failures() {
	set +e
	(
		zxfer_get_temp_file() {
			return 37
		}
		zxfer_capture_progress_estimate_probe_output sh -c "printf '%s\n' 'size\t4096'"
	)
	status=$?

	assertEquals "Progress-estimate capture should preserve temp-file allocation failures exactly." \
		37 "$status"
}

test_zxfer_read_progress_estimate_capture_file_preserves_runtime_readback_failures() {
	capture_file="$TEST_TMPDIR/progress-estimate-capture.txt"
	printf '%s\n' "size	4096" >"$capture_file"

	set +e
	(
		g_zxfer_progress_probe_output_result="stale"
		zxfer_read_runtime_artifact_file() {
			return 29
		}
		zxfer_read_progress_estimate_capture_file "$capture_file"
	)
	status=$?

	assertEquals "Progress-estimate capture-file reads should preserve runtime readback failures exactly." \
		29 "$status"
}

test_get_send_command_display_includes_verbose_raw_flags_for_full_send() {
	g_option_V_very_verbose=1
	g_option_w_raw_send=1

	result=$(zxfer_get_send_command "" "tank/src@snap9")

	assertEquals "Display-mode full sends should include verbose and raw flags when enabled." \
		"/sbin/zfs send -v -w tank/src@snap9" "$result"
}

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

test_zxfer_close_send_job_completion_queue_cleans_orphaned_queue_paths() {
	queue_path="$TEST_TMPDIR/orphaned-queue"
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

test_zxfer_register_send_job_tracks_legacy_pid_lists_and_lookup_records() {
	status_one="$TEST_TMPDIR/legacy_job_status_one"
	status_two="$TEST_TMPDIR/legacy_job_status_two"

	output=$(
		(
			zxfer_register_cleanup_pid() {
				printf 'cleanup:%s\n' "$1"
			}
			zxfer_register_send_job 101 "$status_one"
			zxfer_register_send_job 202 "$status_two"
			printf 'found=%s\n' "$(zxfer_find_send_job_pid_by_status_file "$status_two")"
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			printf 'pids=%s\n' "$g_zfs_send_job_pids"
			printf 'records=%s\n' "$(printf '%s\n' "$g_zfs_send_job_records" | sed 's/	/:/g')"
		)
	)

	assertContains "Registering legacy send jobs should register each PID for cleanup." \
		"$output" "cleanup:101"
	assertContains "Registering multiple legacy send jobs should register later PIDs for cleanup too." \
		"$output" "cleanup:202"
	assertContains "Legacy send-job lookup should resolve tracked status files back to their PID." \
		"$output" "found=202"
	assertContains "Registering legacy send jobs should increment the tracked job count." \
		"$output" "count=2"
	assertContains "Registering legacy send jobs should append to the tracked PID list." \
		"$output" "pids=101 202"
	assertContains "Registering legacy send jobs should preserve the PID-to-status-file registry." \
		"$output" "records=101:$status_one
202:$status_two"
}

test_zxfer_register_send_job_preserves_cleanup_registration_failures() {
	status_file="$TEST_TMPDIR/legacy_job_status_fail"

	output=$(
		(
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_register_send_job 101 "$status_file"
			printf 'status=%s\n' "$?"
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_records:-}"
		)
	)

	assertContains "Legacy send-job registration should fail closed when validated cleanup tracking cannot be published." \
		"$output" "status=1"
	assertContains "Legacy send-job registration failures should preserve the tracked job count." \
		"$output" "count=0"
	assertContains "Legacy send-job registration failures should leave the tracked PID list empty." \
		"$output" "pids=<>"
	assertContains "Legacy send-job registration failures should leave the PID-to-status-file registry empty." \
		"$output" "records=<>"
}

test_zxfer_find_send_job_pid_by_status_file_returns_failure_for_unknown_status_files() {
	g_zfs_send_job_records="101	$TEST_TMPDIR/known_status"

	if zxfer_find_send_job_pid_by_status_file "$TEST_TMPDIR/unknown_status"; then
		fail "Unknown status files should not resolve to a tracked PID."
	fi
}

test_zxfer_read_send_job_status_file_preserves_runtime_readback_failures() {
	status_file="$TEST_TMPDIR/send_job_status_failure"
	printf '%s\n' "status	0" >"$status_file"

	output=$(
		(
			g_zxfer_send_job_status_file_exit_status="stale-status"
			g_zxfer_send_job_status_file_report_failure="stale-report"
			zxfer_read_runtime_artifact_file() {
				return 19
			}
			zxfer_read_send_job_status_file "$status_file" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exit_status=<%s>\n' "$g_zxfer_send_job_status_file_exit_status"
			printf 'report_failure=<%s>\n' "$g_zxfer_send_job_status_file_report_failure"
		)
	)

	assertContains "Send-job status reads should preserve runtime readback failures exactly." \
		"$output" "status=19"
	assertContains "Send-job status readback failures should clear the parsed exit status scratch." \
		"$output" "exit_status=<>"
	assertContains "Send-job status readback failures should clear the parsed failure marker scratch." \
		"$output" "report_failure=<>"
}

test_zxfer_read_send_job_status_file_parses_status_and_failure_marker() {
	status_file="$TEST_TMPDIR/send_job_status_success"
	cat >"$status_file" <<'EOF'
status	7
report_failure	queue_write
EOF

	zxfer_read_send_job_status_file "$status_file" >/dev/null

	assertEquals "Readable send-job status files should preserve the recorded exit status." \
		7 "${g_zxfer_send_job_status_file_exit_status:-}"
	assertEquals "Readable send-job status files should preserve the recorded failure marker." \
		"queue_write" "${g_zxfer_send_job_status_file_report_failure:-}"
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
	assertEquals "Destination-ancestry conflict detection should identify the conflicting tracked job id." \
		"job-1" "$g_zxfer_send_job_conflict_job_id"
	assertEquals "Destination-ancestry conflict detection should expose the conflicting active destination dataset." \
		"backup/dst" "$g_zxfer_send_job_conflict_dest_dataset"

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

test_zxfer_run_background_pipeline_executes_command_and_reports_completion() {
	status_file=$(mktemp "$TEST_TMPDIR/bg_status.XXXXXX")
	queue_file=$(mktemp "$TEST_TMPDIR/bg_queue.XXXXXX")
	runner_status_file=$(mktemp "$TEST_TMPDIR/bg_runner_status.XXXXXX")
	log="$TEST_TMPDIR/bg_runner.log"
	: >"$log"

	(
		exec 9>>"$queue_file"
		g_zfs_send_job_queue_open=1
		zxfer_echov() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_run_background_pipeline "printf 'runner-ok' >'$TEST_TMPDIR/bg_payload.txt'" "displaycmd" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	)
	status=$(tr -d '\r\n' <"$runner_status_file")
	completion_path=$(tr -d '\r\n' <"$queue_file")

	assertEquals "Background pipeline helper should return the eval exit status." 0 "$status"
	assertEquals "Background pipeline helper should write the child status to its status file." \
		"status	0" "$(tr -d '\r' <"$status_file")"
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
		zxfer_echov() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_run_background_pipeline "touch '$TEST_TMPDIR/bg_dry_payload.txt'" "dry-display" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	)
	status=$(tr -d '\r\n' <"$runner_status_file")

	assertEquals "Dry-run background helpers should exit successfully." 0 "$status"
	assertEquals "Dry-run background helpers should still persist a zero status file." \
		"status	0" "$(tr -d '\r' <"$status_file")"
	assertFalse "Dry-run background helpers should not execute the eval command." \
		"[ -e '$TEST_TMPDIR/bg_dry_payload.txt' ]"
	assertContains "Dry-run background helpers should log the rendered dry-run command." \
		"$(cat "$log")" "Dry run: dry-display"
}

test_zxfer_run_background_pipeline_reports_status_file_write_failures() {
	status_file="$TEST_TMPDIR/bg_status_write_failed.txt"
	queue_file="$TEST_TMPDIR/bg_status_write_failed.queue"
	runner_status_file="$TEST_TMPDIR/bg_status_write_failed.status"
	stderr_file="$TEST_TMPDIR/bg_status_write_failed.stderr"
	: >"$queue_file"

	(
		exec 9>>"$queue_file"
		g_zfs_send_job_queue_open=1
		zxfer_write_send_job_status_file() {
			return 1
		}
		zxfer_run_background_pipeline ":" "displaycmd" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	) 2>"$stderr_file"
	status=$(tr -d '\r\n' <"$runner_status_file")

	assertEquals "Background pipeline helpers should fail closed when the status file cannot be written." \
		125 "$status"
	assertEquals "Status-file write failures should publish an explicit queue failure record when queue mode is enabled." \
		"status_write_failed	$status_file	0" "$(tr -d '\r\n' <"$queue_file")"
	assertContains "Status-file write failures should emit a specific operator-visible error." \
		"$(cat "$stderr_file")" "Failed to record zfs send/receive background status"
}

test_zxfer_run_background_pipeline_records_queue_write_failures_in_status_file() {
	status_file="$TEST_TMPDIR/bg_queue_write_failed.txt"
	runner_status_file="$TEST_TMPDIR/bg_queue_write_failed.status"
	stderr_file="$TEST_TMPDIR/bg_queue_write_failed.stderr"

	(
		g_zfs_send_job_queue_open=1
		exec 9>&-
		zxfer_run_background_pipeline ":" "displaycmd" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	) 2>"$stderr_file"
	status=$(tr -d '\r\n' <"$runner_status_file")

	assertEquals "Queue write failures should fail closed instead of returning the child pipeline status." \
		125 "$status"
	assertEquals "Queue write failures should preserve the job exit status and notification failure marker in the status file." \
		"status	0
report_failure	queue_write" "$(tr -d '\r' <"$status_file")"
	assertContains "Queue write failures should emit a specific operator-visible error." \
		"$(cat "$stderr_file")" "Failed to publish zfs send/receive background completion"
}

test_zxfer_run_background_pipeline_removes_stale_status_files_when_queue_failure_marking_fails() {
	status_file="$TEST_TMPDIR/bg_queue_write_marker_failed.txt"
	runner_status_file="$TEST_TMPDIR/bg_queue_write_marker_failed.status"
	stderr_file="$TEST_TMPDIR/bg_queue_write_marker_failed.stderr"

	(
		g_zfs_send_job_queue_open=1
		exec 9>&-
		zxfer_write_send_job_status_file() {
			if [ "${3:-}" = "queue_write" ]; then
				return 1
			fi
			printf 'status\t%s\n' "$2" >"$1"
		}
		zxfer_run_background_pipeline ":" "displaycmd" "$status_file"
		printf '%s\n' "$?" >"$runner_status_file"
	) 2>"$stderr_file"
	status=$(tr -d '\r\n' <"$runner_status_file")

	assertEquals "Queue write failures should still fail closed when the queue-failure marker cannot be written." \
		125 "$status"
	assertFalse "Queue write failures should remove stale success-only status files when the failure marker cannot be recorded." \
		"[ -e '$status_file' ]"
	assertContains "Queue write failures should surface the marker-write failure too." \
		"$(cat "$stderr_file")" "Failed to record zfs send/receive completion notification failure"
}

test_zxfer_write_send_job_status_file_reports_second_write_failures() {
	status_file="$TEST_TMPDIR/send_job_status_append_fail.txt"
	mkdir -p "$status_file" || fail "Unable to create a directory-backed status-file path fixture."

	set +e
	zxfer_write_send_job_status_file "$status_file" 0 "queue_write"
	status=$?

	assertEquals "Send-job status writes should fail closed when the report-failure marker cannot be appended." \
		1 "$status"
}

test_zxfer_write_send_job_status_file_preserves_append_failures_when_second_write_fails() {
	status_file="$TEST_TMPDIR/send_job_status_append_fail_injected.txt"

	output=$(
		(
			write_count=0
			printf() {
				write_count=$((write_count + 1))
				if [ "$write_count" -eq 2 ]; then
					return 1
				fi
				command printf "$@"
			}
			set +e
			zxfer_write_send_job_status_file "$status_file" 0 "queue_write"
			status=$?
			set -e
			command printf 'status=%s\n' "$status"
			command printf 'contents=%s\n' "$(cat "$status_file")"
		)
	)

	assertContains "Injected append failures should still fail the send-job status helper." \
		"$output" "status=1"
	assertContains "Injected append failures should leave the first status line intact for diagnostics." \
		"$output" "contents=status	0"
}

test_zxfer_get_send_job_completion_status_preserves_readback_failures_and_completion_markers() {
	status_file="$TEST_TMPDIR/send_job_completion_status.txt"
	printf 'status\t0\n' >"$status_file"

	output=$(
		(
			set +e
			zxfer_read_send_job_status_file() {
				return 41
			}
			zxfer_get_send_job_completion_status "$status_file" 125
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Completion-status reads should preserve status-file read failures exactly." \
		"$output" "status=41"

	output=$(
		(
			printf 'status\tnot-a-number\n' >"$status_file"
			zxfer_get_send_job_completion_status "$status_file" 125
			printf 'exit_status=%s\n' "$g_zxfer_send_job_status_file_exit_status"
			printf 'report_failure=%s\n' "$g_zxfer_send_job_status_file_report_failure"
		)
	)

	assertContains "Completion-status reads should fall back to the waited status when the status file is nonnumeric." \
		"$output" "exit_status=125"
	assertContains "Completion-status reads should infer a completion-write failure when the waited status was 125." \
		"$output" "report_failure=completion_write"
}

test_wait_for_next_zfs_send_job_completion_falls_back_when_queue_is_not_open() {
	log="$TEST_TMPDIR/wait_next_fallback.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait:%s\n' "$1" >>"$EXEC_LOG"
		}
		g_count_zfs_send_jobs=1
		g_zfs_send_job_queue_open=0
		g_zfs_send_job_records="101	$TEST_TMPDIR/status"
		zxfer_wait_for_next_zfs_send_job_completion "unit"
	)

	assertEquals "Closed completion queues should fall back to the legacy wait helper." \
		"wait:unit" "$(cat "$log")"
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
			zxfer_throw_error() {
				printf 'error:%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?

	assertEquals "Nonnumeric rolling-pool status files should fall back to the waited PID exit status." 1 "$status"
	assertContains "Rolling-pool fallback should preserve the waited PID in the failure message." \
		"$output" "error:zfs send/receive job failed (PID "
	assertContains "Rolling-pool fallback should preserve the waited exit status in the failure message." \
		"$output" "exit 7"
}

test_wait_for_next_zfs_send_job_completion_rejects_blank_status_write_notifications() {
	set +e
	output=$(
		(
			queue_file="$TEST_TMPDIR/wait_next_blank_status_write_failed.txt"
			printf '\n' >"$queue_file"
			exec 8<"$queue_file"
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="101	$TEST_TMPDIR/other.status"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?
	set -e

	assertEquals "Rolling completion waits should fail closed when a status-write notification omits its status-file path." \
		1 "$status"
	assertContains "Malformed rolling completion notifications should terminate the remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Malformed rolling completion notifications should surface the dedicated parse failure." \
		"$output" "Failed to parse a completed zfs send/receive job notification."
}

test_wait_for_next_zfs_send_job_completion_falls_back_to_legacy_waits_when_queue_notifications_are_missing() {
	log="$TEST_TMPDIR/wait_next_queue_eof.log"
	: >"$log"

	(
		queue_file="$TEST_TMPDIR/wait_next_empty_queue.txt"
		: >"$queue_file"
		exec 8<"$queue_file"
		zxfer_wait_for_zfs_send_jobs_legacy() {
			printf 'legacy:%s\n' "$1" >>"$log"
		}
		zxfer_close_send_job_completion_queue() {
			printf 'closed\n' >>"$log"
			g_zfs_send_job_queue_open=0
			g_zfs_send_job_queue_writer_open=0
		}
		g_zfs_send_job_queue_open=1
		g_zfs_send_job_records="101	$TEST_TMPDIR/status"
		g_count_zfs_send_jobs=1
		zxfer_wait_for_next_zfs_send_job_completion "unit"
		printf 'unavailable=%s\n' "$g_zfs_send_job_queue_unavailable" >>"$log"
	)

	assertEquals "Missing queue notifications should tear down queue mode and fall back to legacy waits." \
		"closed
legacy:unit
unavailable=1" "$(cat "$log")"
}

test_wait_for_next_zfs_send_job_completion_reports_status_write_failures() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_status_write_failed.txt"
			queue_file="$TEST_TMPDIR/wait_next_status_write_failed.queue"
			printf 'status_write_failed\t%s\t0\n' "$status_file" >"$queue_file"
			sh -c 'exit 125' &
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?

	assertEquals "Explicit status-write failure notifications should abort immediately." 1 "$status"
	assertContains "Status-write failure notifications should preserve the operator-facing error." \
		"$output" "Failed to record zfs send/receive background status"
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_status_write_failures() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_status_write_abort_failure.txt"
			queue_file="$TEST_TMPDIR/wait_next_status_write_abort_failure.queue"
			printf 'status_write_failed\t%s\t0\n' "$status_file" >"$queue_file"
			sh -c 'exit 125' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_terminate_remaining_send_jobs() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?

	assertEquals "Rolling completion waits should surface cleanup-abort failures before status-write failure errors." \
		1 "$status"
	assertContains "Cleanup-abort failures should preserve the validated abort failure message before the status-write failure error." \
		"$output" "validated cleanup abort failed"
}

test_wait_for_next_zfs_send_job_completion_normalizes_nonnumeric_status_write_failures() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_status_write_failed_non_numeric.txt"
			queue_file="$TEST_TMPDIR/wait_next_status_write_failed_non_numeric.queue"
			printf 'status_write_failed\t%s\tnot-a-number\n' "$status_file" >"$queue_file"
			sh -c 'exit 125' &
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?

	assertEquals "Malformed status-write failure notifications should still fail closed." \
		1 "$status"
	assertContains "Malformed status-write failure notifications should normalize the status to 125." \
		"$output" "exit 125)."
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_queue_write_and_nonzero_errors() {
	set +e
	queue_write_output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_queue_write_abort_failure.status"
			queue_file="$TEST_TMPDIR/wait_next_queue_write_abort_failure.queue"
			printf '%s\n' "status	0" >"$status_file"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 0' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				g_zxfer_send_job_status_file_exit_status=7
				g_zxfer_send_job_status_file_report_failure="queue_write"
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_close_send_job_completion_queue() {
				:
			}
			zxfer_terminate_remaining_send_jobs() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	queue_write_status=$?
	exit_output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_nonzero_abort_failure.status"
			queue_file="$TEST_TMPDIR/wait_next_nonzero_abort_failure.queue"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 9' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				g_zxfer_send_job_status_file_exit_status=9
				g_zxfer_send_job_status_file_report_failure=""
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_close_send_job_completion_queue() {
				:
			}
			zxfer_terminate_remaining_send_jobs() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	exit_status=$?

	assertEquals "Rolling completion waits should surface cleanup-abort failures before queue-write marker errors." \
		1 "$queue_write_status"
	assertContains "Cleanup-abort failures should preserve the validated abort failure message before the queue-write marker error." \
		"$queue_write_output" "validated cleanup abort failed"
	assertEquals "Rolling completion waits should surface cleanup-abort failures before nonzero-exit errors." \
		1 "$exit_status"
	assertContains "Cleanup-abort failures should preserve the validated abort failure message before the nonzero-exit error." \
		"$exit_output" "validated cleanup abort failed"
}

test_zxfer_wait_for_zfs_send_jobs_legacy_reports_generic_completion_write_failures() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/legacy_completion_write_failed.txt"
			sh -c 'exit 125' &
			job_pid=$!
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	status=$?

	assertEquals "Legacy background waits should fail closed when a job exits with the completion-wrapper failure status and no status file." \
		1 "$status"
	assertContains "Legacy background waits should surface the generic completion-report failure." \
		"$output" "Failed to report zfs send/receive background completion"
}

test_zxfer_wait_for_zfs_send_jobs_legacy_reports_queue_write_failure_markers() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/legacy_queue_write_failed.txt"
			cat >"$status_file" <<'EOF'
status	0
report_failure	queue_write
EOF
			sh -c 'exit 0' &
			job_pid=$!
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	status=$?
	set -e

	assertEquals "Legacy background waits should fail closed when a completed job recorded a queue-write failure marker." \
		1 "$status"
	assertContains "Legacy queue-write failures should terminate the remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Legacy queue-write failures should surface the publish-failure error." \
		"$output" "Failed to publish zfs send/receive background completion"
}

test_wait_for_next_zfs_send_job_completion_reports_status_read_failures_and_queue_write_markers() {
	set +e
	read_failure_output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_status_read_failure.txt"
			queue_file="$TEST_TMPDIR/wait_next_status_read_failure.queue"
			printf '%s\n' "status	0" >"$status_file"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 0' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				return 1
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	read_failure_status=$?
	queue_write_output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_queue_write_marker.txt"
			queue_file="$TEST_TMPDIR/wait_next_queue_write_marker.queue"
			printf '%s\n' "status	0" >"$status_file"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 0' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				g_zxfer_send_job_status_file_exit_status=7
				g_zxfer_send_job_status_file_report_failure="queue_write"
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_close_send_job_completion_queue() {
				:
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	queue_write_status=$?
	set -e

	assertEquals "Rolling completion waits should fail closed when the completed status file cannot be read." \
		1 "$read_failure_status"
	assertContains "Rolling status-file read failures should terminate remaining jobs before aborting." \
		"$read_failure_output" "terminated"
	assertContains "Rolling status-file read failures should preserve the documented operator-facing error." \
		"$read_failure_output" "Failed to read zfs send/receive job status file ["
	assertEquals "Rolling completion waits should fail closed when the completed status file records a queue-write marker." \
		1 "$queue_write_status"
	assertContains "Rolling queue-write markers should terminate remaining jobs before aborting." \
		"$queue_write_output" "terminated"
	assertContains "Rolling queue-write markers should preserve the publish-failure error." \
		"$queue_write_output" "Failed to publish zfs send/receive background completion"
}

test_wait_for_next_zfs_send_job_completion_reports_completion_write_markers() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_completion_write_marker.txt"
			queue_file="$TEST_TMPDIR/wait_next_completion_write_marker.queue"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 7' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				g_zxfer_send_job_status_file_exit_status=7
				g_zxfer_send_job_status_file_report_failure="completion_write"
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_close_send_job_completion_queue() {
				:
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?
	set -e

	assertEquals "Rolling completion waits should fail closed when the completed status file records a completion-write marker." \
		1 "$status"
	assertContains "Rolling completion-write markers should terminate remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Rolling completion-write markers should preserve the completion-report error." \
		"$output" "Failed to report zfs send/receive background completion (PID "
	assertContains "Rolling completion-write markers should preserve the waited exit status in the completion-report error." \
		"$output" ", exit 7)."
}

test_wait_for_next_zfs_send_job_completion_reports_unknown_completed_status_files() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_unmatched_status.txt"
			queue_file="$TEST_TMPDIR/wait_next_unmatched_queue.txt"
			printf '%s\n' 0 >"$status_file"
			printf '%s\n' "$status_file" >"$queue_file"
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				return 1
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="101	$TEST_TMPDIR/other_status"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?

	assertEquals "Rolling send/receive waits should fail closed when a completed status file does not match a tracked PID." \
		1 "$status"
	assertContains "Unknown completed status files should terminate the remaining jobs before aborting." \
		"$output" "terminated"
	assertContains "Unknown completed status files should preserve the documented matching failure." \
		"$output" "Failed to match a completed zfs send/receive job to a tracked PID."
}

test_zxfer_terminate_remaining_send_jobs_aborts_supervised_jobs_and_clears_state() {
	queue_dir=$(mktemp -d "$TEST_TMPDIR/terminate_supervised_queue.XXXXXX")
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
				return 1
			}
			set +e
			zxfer_terminate_remaining_send_jobs
			status=$?
			set -e
			printf 'status=%s\n' "$status"
		)
	)

	assertContains "Supervised teardown should fail closed when the tracked job-id collection fails." \
		"$output" "status=1"
}

test_zxfer_terminate_remaining_send_jobs_returns_failure_when_supervised_abort_fails() {
	output=$(
		(
			zxfer_register_supervised_send_job "job-1" 101
			zxfer_abort_background_job() {
				return 1
			}
			set +e
			zxfer_terminate_remaining_send_jobs
			status=$?
			set -e
			printf 'status=%s\n' "$status"
		)
	)

	assertContains "Supervised teardown should fail closed when aborting a tracked supervised job fails." \
		"$output" "status=1"
}

test_zxfer_terminate_remaining_send_jobs_continues_after_supervised_abort_failures_and_preserves_first_message() {
	queue_dir=$(mktemp -d "$TEST_TMPDIR/terminate_supervised_abort_queue.XXXXXX")
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
					return 1
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
		"$output" "status=1"
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

test_zxfer_terminate_remaining_send_jobs_kills_legacy_jobs_and_cleans_status_files() {
	status_one="$TEST_TMPDIR/terminate_legacy_status_one"
	status_two="$TEST_TMPDIR/terminate_legacy_status_two"
	printf '%s\n' "status	0" >"$status_one"
	printf '%s\n' "status	0" >"$status_two"

	output=$(
		(
			g_zfs_send_job_pids="101 202"
			g_zfs_send_job_records="101	$status_one
202	$status_two"
			g_count_zfs_send_jobs=2
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s:%s\n' "$1" "$2"
			}
			zxfer_terminate_remaining_send_jobs
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_records:-}"
		)
	)

	assertContains "Legacy teardown should abort the first tracked PID through the validated cleanup helper path." \
		"$output" "abort:101:TERM"
	assertContains "Legacy teardown should abort later tracked PIDs through the validated cleanup helper path too." \
		"$output" "abort:202:TERM"
	assertContains "Legacy teardown should clear the tracked job count." \
		"$output" "count=0"
	assertContains "Legacy teardown should clear the tracked PID list." \
		"$output" "pids=<>"
	assertContains "Legacy teardown should clear the tracked status-file registry." \
		"$output" "records=<>"
	assertFalse "Legacy teardown should remove the first tracked status file." \
		"[ -e '$status_one' ]"
	assertFalse "Legacy teardown should remove the second tracked status file." \
		"[ -e '$status_two' ]"
}

test_zxfer_terminate_remaining_send_jobs_preserves_first_abort_failure() {
	status_one="$TEST_TMPDIR/terminate_abort_failure_one.status"
	status_two="$TEST_TMPDIR/terminate_abort_failure_two.status"
	: >"$status_one"
	: >"$status_two"

	output=$(
		(
			g_zfs_send_job_pids="101 202"
			g_zfs_send_job_records="101	$status_one
202	$status_two"
			g_count_zfs_send_jobs=2
			zxfer_abort_cleanup_pid() {
				if [ "$1" = "101" ]; then
					g_zxfer_cleanup_pid_abort_failure_message="first send-job abort failed"
					return 1
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
			printf 'records=<%s>\n' "${g_zfs_send_job_records:-}"
		)
	)

	assertContains "Legacy send-job teardown should preserve the first validated cleanup abort failure status." \
		"$output" "status=1"
	assertContains "Legacy send-job teardown should preserve the first validated cleanup abort failure message." \
		"$output" "message=first send-job abort failed"
	assertContains "Legacy send-job teardown should still close the rolling completion queue after an abort failure." \
		"$output" "queue-closed"
	assertContains "Legacy send-job teardown should still clear the tracked state after an abort failure." \
		"$output" "count=0"
	assertContains "Legacy send-job teardown should still clear the tracked pid list after an abort failure." \
		"$output" "pids=<>"
	assertContains "Legacy send-job teardown should still clear the tracked pid-to-status-file registry after an abort failure." \
		"$output" "records=<>"
}

test_zxfer_throw_send_job_cleanup_failure_uses_validated_abort_message() {
	set +e
	output=$(
		(
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_throw_send_job_cleanup_failure
		)
	)
	status=$?
	set -e

	assertEquals "Validated send-job cleanup failure helper should fail closed through zxfer_throw_error." \
		1 "$status"
	assertContains "Validated send-job cleanup failure helper should preserve the validated abort failure message." \
		"$output" "validated cleanup abort failed"
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_on_parse_and_completion_write_paths() {
	set +e
	parse_output=$(
		(
			queue_file="$TEST_TMPDIR/wait_next_cleanup_abort_parse.queue"
			printf '\n' >"$queue_file"
			exec 8<"$queue_file"
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="101	$TEST_TMPDIR/other.status"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	parse_status=$?
	completion_write_output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_cleanup_abort_completion.status"
			queue_file="$TEST_TMPDIR/wait_next_cleanup_abort_completion.queue"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 0' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				g_zxfer_send_job_status_file_exit_status=7
				g_zxfer_send_job_status_file_report_failure="completion_write"
			}
			zxfer_unregister_send_job() {
				g_count_zfs_send_jobs=0
				g_zfs_send_job_pids=""
			}
			zxfer_close_send_job_completion_queue() {
				:
			}
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	completion_write_status=$?
	set -e

	assertEquals "Rolling completion waits should fail closed when cleanup itself fails during malformed queue-record teardown." \
		1 "$parse_status"
	assertContains "Rolling completion waits should surface validated cleanup abort failures before the malformed queue-record error." \
		"$parse_output" "validated cleanup abort failed"
	assertEquals "Rolling completion waits should fail closed when cleanup itself fails during completion-write teardown." \
		1 "$completion_write_status"
	assertContains "Rolling completion waits should surface validated cleanup abort failures before the completion-write error." \
		"$completion_write_output" "validated cleanup abort failed"
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_status_read_errors() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/wait_next_cleanup_abort_status_read.status"
			queue_file="$TEST_TMPDIR/wait_next_cleanup_abort_status_read.queue"
			printf '%s\n' "$status_file" >"$queue_file"
			sh -c 'exit 0' &
			job_pid=$!
			exec 8<"$queue_file"
			zxfer_find_send_job_pid_by_status_file() {
				printf '%s\n' "$job_pid"
			}
			zxfer_get_send_job_completion_status() {
				return 1
			}
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_records="record"
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids="$job_pid"
			zxfer_wait_for_next_zfs_send_job_completion "unit"
		)
	)
	status=$?
	set -e

	assertEquals "Rolling completion waits should fail closed when cleanup itself fails during status-read teardown." \
		1 "$status"
	assertContains "Rolling completion waits should surface validated cleanup abort failures before the status-read error." \
		"$output" "validated cleanup abort failed"
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
			zxfer_invalidate_destination_property_cache() {
				printf 'invalidated=%s\n' "$1"
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
	assertContains "Successful supervised rolling waits should invalidate the destination property cache for the completed destination dataset." \
		"$output" "invalidated=backup/dst"
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
			zxfer_invalidate_destination_property_cache() {
				printf 'invalidated=%s\n' "$1"
			}
			zxfer_wait_for_supervised_zfs_send_jobs_batch
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_supervisor_records:-}"
		)
	)

	assertContains "Successful supervised batch waits should repair the destination-existence cache for the completed destination dataset." \
		"$output" "noted=backup/dst"
	assertContains "Successful supervised batch waits should invalidate the destination property cache for the completed destination dataset." \
		"$output" "invalidated=backup/dst"
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

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_to_legacy_waits_on_queue_read_failure() {
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
			zxfer_wait_for_zfs_send_jobs_legacy() {
				printf 'legacy=%s\n' "$1"
			}
			zxfer_wait_for_next_supervised_zfs_send_job_completion "unit"
			printf 'unavailable=%s\n' "${g_zfs_send_job_queue_unavailable:-0}"
		)
	)

	assertContains "Supervised rolling waits should fall back to the legacy wait path when the queue reader hits EOF." \
		"$output" "legacy=unit"
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

test_wait_for_zfs_send_jobs_legacy_reports_missing_status_file_records() {
	set +e
	output=$(
		(
			sh -c 'exit 0' &
			job_pid=$!
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zxfer_runtime_artifact_read_result="stale-status"
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="999	$TEST_TMPDIR/other.status"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	status=$?

	assertEquals "Legacy background waits should fail closed when a tracked PID has no status-file record." \
		1 "$status"
	assertContains "Legacy background waits should terminate the remaining jobs before aborting on a missing status-file record." \
		"$output" "terminated"
	assertContains "Legacy background waits should preserve the missing status-file record error." \
		"$output" "Failed to match a tracked zfs send/receive job PID to a status file."
}

test_zxfer_wait_for_zfs_send_jobs_legacy_terminates_remaining_jobs_on_failure() {
	set +e
	output=$(
		(
			first_status_file="$TEST_TMPDIR/legacy_failure_first.status"
			second_status_file="$TEST_TMPDIR/legacy_failure_second.status"
			printf 'status\t7\n' >"$first_status_file"
			printf 'status\t0\n' >"$second_status_file"
			sh -c 'exit 7' &
			first_pid=$!
			sh -c 'exit 0' &
			second_pid=$!
			zxfer_abort_cleanup_pid() {
				printf 'aborted:%s:%s\n' "$1" "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$first_pid $second_pid"
			g_zfs_send_job_records="$first_pid	$first_status_file
$second_pid	$second_status_file"
			g_count_zfs_send_jobs=2
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	status=$?

	assertEquals "Legacy waits should fail closed when one background job exits nonzero." \
		1 "$status"
	assertContains "Legacy waits should terminate the remaining tracked jobs after the first failure." \
		"$output" "aborted:"
	assertContains "Legacy waits should preserve the failing job exit status in the operator-facing error." \
		"$output" "zfs send/receive job failed (PID "
}

test_zxfer_wait_for_zfs_send_jobs_legacy_surfaces_cleanup_abort_failures_before_record_and_completion_errors() {
	set +e
	record_output=$(
		(
			sh -c 'exit 0' &
			job_pid=$!
			g_zxfer_runtime_artifact_read_result="stale-status"
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="999	$TEST_TMPDIR/other.status"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	record_status=$?
	completion_write_output=$(
		(
			status_file="$TEST_TMPDIR/legacy_cleanup_abort_completion.status"
			cat >"$status_file" <<'EOF'
status	0
report_failure	completion_write
EOF
			sh -c 'exit 0' &
			job_pid=$!
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	completion_write_status=$?
	set -e

	assertEquals "Legacy waits should fail closed when cleanup itself fails while handling a missing status-file record." \
		1 "$record_status"
	assertContains "Legacy waits should surface validated cleanup abort failures before the missing-record error." \
		"$record_output" "validated cleanup abort failed"
	assertEquals "Legacy waits should fail closed when cleanup itself fails while handling a completion-write marker." \
		1 "$completion_write_status"
	assertContains "Legacy waits should surface validated cleanup abort failures before the completion-write error." \
		"$completion_write_output" "validated cleanup abort failed"
}

test_zxfer_wait_for_zfs_send_jobs_legacy_surfaces_cleanup_abort_failures_before_status_read_errors() {
	set +e
	output=$(
		(
			status_file="$TEST_TMPDIR/legacy_cleanup_abort_status_read.status"
			printf 'status\t0\n' >"$status_file"
			sh -c 'exit 0' &
			job_pid=$!
			zxfer_get_send_job_completion_status() {
				return 1
			}
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	status=$?
	set -e

	assertEquals "Legacy waits should fail closed when cleanup itself fails during status-read teardown." \
		1 "$status"
	assertContains "Legacy waits should surface validated cleanup abort failures before the status-read error." \
		"$output" "validated cleanup abort failed"
}

test_zxfer_wait_for_zfs_send_jobs_legacy_surfaces_cleanup_abort_failures_before_queue_write_and_nonzero_errors() {
	set +e
	queue_write_output=$(
		(
			status_file="$TEST_TMPDIR/legacy_cleanup_abort_queue_write.status"
			cat >"$status_file" <<'EOF'
status	0
report_failure	queue_write
EOF
			sh -c 'exit 0' &
			job_pid=$!
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	queue_write_status=$?
	exit_output=$(
		(
			status_file="$TEST_TMPDIR/legacy_cleanup_abort_nonzero.status"
			printf 'status\t9\n' >"$status_file"
			sh -c 'exit 9' &
			job_pid=$!
			g_zxfer_cleanup_pid_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	exit_status=$?
	set -e

	assertEquals "Legacy waits should surface cleanup-abort failures before queue-write marker errors." \
		1 "$queue_write_status"
	assertContains "Legacy waits should preserve the validated cleanup abort failure before the queue-write marker error." \
		"$queue_write_output" "validated cleanup abort failed"
	assertEquals "Legacy waits should surface cleanup-abort failures before nonzero-exit errors." \
		1 "$exit_status"
	assertContains "Legacy waits should preserve the validated cleanup abort failure before the nonzero-exit error." \
		"$exit_output" "validated cleanup abort failed"
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

test_zxfer_wait_for_zfs_send_jobs_dispatches_supervised_and_legacy_queue_paths() {
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
			g_zfs_send_job_pids="303 404"
			g_zfs_send_job_supervisor_records=""
			g_zfs_send_job_records="303	$TEST_TMPDIR/legacy.303
404	$TEST_TMPDIR/legacy.404"
			g_zfs_send_job_queue_open=1
			g_count_zfs_send_jobs=2
			zxfer_wait_for_next_zfs_send_job_completion() {
				printf 'legacy_next:%s\n' "$1"
				g_count_zfs_send_jobs=$((g_count_zfs_send_jobs - 1))
			}
			zxfer_close_send_job_completion_queue() {
				printf 'closed_legacy\n'
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
	assertContains "Queued legacy waits should drain through the rolling legacy helper." \
		"$output" "legacy_next:"
	assertContains "Queued legacy waits should close the queue after draining." \
		"$output" "closed_legacy"
	assertContains "Non-queued supervised waits should fall back to the batch helper." \
		"$output" "supervised_batch"
}

test_zxfer_wait_for_zfs_send_jobs_dispatches_to_legacy_helper_when_queue_is_closed() {
	output=$(
		(
			g_zfs_send_job_queue_open=0
			g_zfs_send_job_pids="101"
			g_zfs_send_job_supervisor_records=""
			zxfer_wait_for_zfs_send_jobs_legacy() {
				printf 'legacy:%s\n' "$1"
			}
			zxfer_wait_for_zfs_send_jobs "unit"
		)
	)

	assertContains "Non-queued legacy waits should dispatch through the legacy batch helper." \
		"$output" "legacy:unit"
}

test_zxfer_wait_for_zfs_send_jobs_legacy_clears_state_and_closes_queue_on_success() {
	queue_dir=$(mktemp -d "$TEST_TMPDIR/legacy_wait_success_queue.XXXXXX")
	queue_path=$queue_dir/queue
	: >"$queue_path"

	output=$(
		(
			sh -c 'exit 0' &
			job_pid=$!
			g_zfs_send_job_pids=$job_pid
			g_zfs_send_job_records=""
			g_count_zfs_send_jobs=1
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_path=$queue_path
			g_zfs_send_job_queue_dir=$queue_dir
			zxfer_close_send_job_completion_queue() {
				printf 'closed\n'
				g_zfs_send_job_queue_open=0
			}
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
			printf 'pids=<%s>\n' "${g_zfs_send_job_pids:-}"
			printf 'records=<%s>\n' "${g_zfs_send_job_records:-}"
			printf 'count=%s\n' "${g_count_zfs_send_jobs:-0}"
			printf 'queue_open=%s\n' "${g_zfs_send_job_queue_open:-0}"
		)
	)

	assertContains "Legacy wait success should clear the tracked pid list." \
		"$output" "pids=<>"
	assertContains "Legacy wait success should clear the tracked status-file records." \
		"$output" "records=<>"
	assertContains "Legacy wait success should clear the tracked job count." \
		"$output" "count=0"
	assertContains "Legacy wait success should close the rolling queue state." \
		"$output" "closed"
	assertContains "Legacy wait success should leave the queue marked closed." \
		"$output" "queue_open=0"
}

test_zxfer_wait_for_zfs_send_jobs_legacy_dispatches_to_supervised_batch_and_reports_status_read_failures() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	supervised_output=$(
		(
			g_zfs_send_job_supervisor_records="job-1	101"
			zxfer_wait_for_supervised_zfs_send_jobs_batch() {
				printf '%s\n' "supervised-batch"
			}
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	supervised_status=$?
	read_failure_output=$(
		(
			status_file="$TEST_TMPDIR/legacy_status_read_failure.status"
			printf 'status\t0\n' >"$status_file"
			sh -c 'exit 0' &
			job_pid=$!
			zxfer_get_send_job_completion_status() {
				return 1
			}
			zxfer_terminate_remaining_send_jobs() {
				printf '%s\n' "terminated"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zfs_send_job_pids="$job_pid"
			g_zfs_send_job_records="$job_pid	$status_file"
			g_count_zfs_send_jobs=1
			zxfer_wait_for_zfs_send_jobs_legacy "unit"
		)
	)
	read_failure_status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Legacy wait dispatch should defer to the supervised batch path when supervised records are tracked." \
		0 "$supervised_status"
	assertContains "Legacy wait dispatch should call the supervised batch wait helper when supervised records are tracked." \
		"$supervised_output" "supervised-batch"
	assertEquals "Legacy waits should fail closed when a tracked status file cannot be read." \
		1 "$read_failure_status"
	assertContains "Legacy status read failures should terminate remaining jobs before aborting." \
		"$read_failure_output" "terminated"
	assertContains "Legacy status read failures should preserve the documented operator-facing error." \
		"$read_failure_output" "Failed to read zfs send/receive job status file ["
}

test_zxfer_progress_passthrough_falls_back_when_mktemp_fails() {
	log="$TEST_TMPDIR/progress_mktemp.log"
	output=$(
		printf 'payload\n' | (
			zxfer_echoV() {
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
			zxfer_echoV() {
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
			zxfer_echoV() {
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

test_zxfer_progress_passthrough_falls_back_when_cleanup_wrapper_resolution_fails() {
	log="$TEST_TMPDIR/progress_wrapper_missing.log"

	output=$(
		printf 'payload\n' | (
			zxfer_echoV() {
				printf '%s\n' "$1" >>"$log"
			}
			zxfer_get_cleanup_child_wrapper_script_path() {
				return 1
			}
			zxfer_progress_passthrough "cat >/dev/null"
		)
	)
	status=$?

	assertEquals "Cleanup-wrapper lookup failures should fall back to a plain passthrough." \
		0 "$status"
	assertEquals "Cleanup-wrapper lookup failure fallback should preserve stdin." "payload" "$output"
	assertContains "Cleanup-wrapper lookup failure fallback should log the degraded path." \
		"$(cat "$log")" "Unable to resolve the cleanup wrapper for the progress dialog"
}

test_zxfer_progress_passthrough_falls_back_when_cleanup_registration_fails() {
	log="$TEST_TMPDIR/progress_register_fail.log"
	abort_log="$TEST_TMPDIR/progress_register_fail.abort.log"

	output=$(
		printf 'payload\n' | (
			zxfer_echoV() {
				printf '%s\n' "$1" >>"$log"
			}
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_direct_child_pid() {
				printf 'abort:%s:%s:%s\n' "$1" "$2" "$3" >>"$abort_log"
				kill -s TERM "$1" 2>/dev/null || :
				wait "$1" 2>/dev/null || :
				return 0
			}
			zxfer_progress_passthrough "sleep 30"
		)
	)
	status=$?

	assertEquals "Cleanup-registration failures should fall back to a plain passthrough when the spawned progress helper tree is reaped successfully." \
		0 "$status"
	assertEquals "Cleanup-registration failure fallback should preserve stdin." "payload" "$output"
	assertContains "Cleanup-registration failure fallback should log the degraded path." \
		"$(cat "$log")" "Unable to register validated cleanup metadata for the progress dialog"
	assertContains "Cleanup-registration failure fallback should route teardown through the validated direct-child abort helper." \
		"$(cat "$abort_log")" "abort:"
	assertContains "Cleanup-registration failure fallback should preserve the progress-helper purpose when invoking the validated direct-child abort helper." \
		"$(cat "$abort_log")" "progress dialog helper"
}

test_zxfer_progress_passthrough_fails_when_cleanup_registration_abort_fails() {
	abort_log="$TEST_TMPDIR/progress_register_abort_fail.log"
	l_restore_errexit=0

	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	output=$(
		printf 'payload\n' | (
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_direct_child_pid() {
				printf 'abort:%s:%s:%s\n' "$1" "$2" "$3" >>"$abort_log"
				kill -s TERM "$1" 2>/dev/null || :
				wait "$1" 2>/dev/null || :
				return 1
			}
			zxfer_progress_passthrough "sleep 30"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Progress passthrough should fail closed when cleanup-registration recovery cannot tear down the spawned helper." \
		1 "$status"
	assertEquals "Progress passthrough should not emit fallback output when cleanup-registration recovery itself fails." \
		"" "$output"
	assertContains "Cleanup-registration recovery failures should still route teardown through the validated direct-child abort helper." \
		"$(cat "$abort_log")" "progress dialog helper"
}

test_zxfer_progress_passthrough_logs_progress_command_failures() {
	log="$TEST_TMPDIR/progress_status.log"
	output=$(
		printf 'payload\n' | (
			zxfer_echoV() {
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

	cmd=$(zxfer_get_send_command "" "tank/fs@snap1" "$g_cmd_zfs" "exec")

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

	cmd=$(zxfer_get_receive_command "tank/dst" "$g_cmd_zfs" "exec")

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
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
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
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_execute_command() {
			printf 'exec=%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_invalidate_destination_property_cache() {
			printf 'invalidate=%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	assertEquals "Successful live send/receive should invalidate the destination property cache for the receive dataset." \
		"exec=sendcmd | recvcmd
invalidate=backup/dst" "$(cat "$log")"
}

test_zfs_send_receive_marks_destination_hierarchy_exists_after_foreground_receive() {
	output=$(
		(
			zxfer_echoV() { :; }
			zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
			zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
			zxfer_mark_destination_root_missing_in_cache "backup"
			zxfer_execute_command() {
				:
			}
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst/child" "0"
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
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_execute_command() {
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
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
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
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_wrap_command_with_ssh() {
			printf '%s\n' "$1 via $2"
		}
		zxfer_execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_V_very_verbose=1
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_zxfer_profile_ssh_shell_invocations=0
		g_zxfer_profile_source_ssh_shell_invocations=0
		g_zxfer_profile_destination_ssh_shell_invocations=0
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
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
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_wrap_command_with_ssh() {
			printf '%s\n' "$1 via $2"
		}
		zxfer_execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_V_very_verbose=1
		g_option_O_origin_host="shared.example"
		g_option_T_target_host="shared.example"
		g_zxfer_profile_ssh_shell_invocations=0
		g_zxfer_profile_source_ssh_shell_invocations=0
		g_zxfer_profile_destination_ssh_shell_invocations=0
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
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
		zxfer_echoV() { :; }
		zxfer_echov() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
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
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
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

test_zfs_send_receive_dry_run_emits_raw_incremental_pipeline_on_stdout() {
	output=$(
		(
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_V_very_verbose=1
			g_option_w_raw_send=1
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		)
	)

	assertContains "Dry-run send/receive should keep the operator-facing incremental raw-send pipeline on stdout." \
		"$output" "/sbin/zfs send -v -w -I tank/src@snap1 tank/src@snap2 | /sbin/zfs receive  backup/dst"
}

test_zfs_send_receive_dry_run_with_progress_template_skips_live_size_probe() {
	probe_log="$TEST_TMPDIR/dry_run_progress_pipeline_probe.log"
	estimate_log="$TEST_TMPDIR/dry_run_progress_pipeline_estimate.log"
	: >"$probe_log"
	: >"$estimate_log"

	output=$(
		(
			PROBE_LOG="$probe_log"
			ESTIMATE_LOG="$estimate_log"
			zxfer_echoV() {
				printf '%s\n' "$*" >&2
			}
			zxfer_echov() {
				printf '%s\n' "$*"
			}
			zxfer_calculate_size_estimate() {
				printf '%s\n' "$*" >>"$ESTIMATE_LOG"
				return 1
			}
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$PROBE_LOG"
				printf '%s\n' "4096"
			}
			g_option_n_dryrun=1
			g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		) 2>&1
	)

	assertEquals "Dry-run send/receive should not probe the live source even when the progress template uses %%size%%." \
		"" "$(cat "$probe_log")"
	assertEquals "Dry-run send/receive should not call the live size-estimator helper when the progress template uses %%size%%." \
		"" "$(cat "$estimate_log")"
	assertContains "Dry-run send/receive should explain that the live %%size%% probe is skipped." \
		"$output" "Dry run: skipping live %%size%% progress estimate discovery."
	assertContains "Dry-run send/receive should still render the progress passthrough pipeline with an explicit unknown-size placeholder." \
		"$output" "pv -s UNKNOWN -N tank/src@snap2"
}

test_zfs_send_receive_backgrounds_pipeline_when_parallel_jobs_available() {
	log="$TEST_TMPDIR/background_pipeline.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			return 0
		}
		zxfer_spawn_supervised_background_job() {
			printf 'spawn:%s|notify=%s\n' "$3" "${6:-}" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-1"
			g_zxfer_background_job_last_runner_pid=111
		}
		g_option_j_jobs=3
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		{
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			printf 'pids=%s\n' "$g_zfs_send_job_pids"
		} >>"$EXEC_LOG"
		printf 'records=%s\n' "$(printf '%s\n' "$g_zfs_send_job_supervisor_records" | sed 's/	/:/g')" >>"$EXEC_LOG"
	)

	assertContains "Background send/receive should execute the composed pipeline." \
		"$(cat "$log")" "spawn:sendcmd | recvcmd|notify=9"
	assertContains "Background send/receive should increment the job count." \
		"$(cat "$log")" "count=1"
	assertContains "Background send/receive should track the spawned PID." \
		"$(cat "$log")" "pids=111"
	assertContains "Background send/receive should track the supervised job id alongside dataset metadata for later conflict checks and failure reporting." \
		"$(cat "$log")" "records=job-1:111:tank/src@snap2:backup/dst:"
}

test_zfs_send_receive_passes_queue_notify_fd_to_supervised_background_job_when_rolling_pool_is_open() {
	log="$TEST_TMPDIR/background_pipeline_notify_fd.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			return 0
		}
		zxfer_spawn_supervised_background_job() {
			printf 'notify=%s queue_open=%s writer_open=%s\n' \
				"${6:-}" "${g_zfs_send_job_queue_open:-0}" "${g_zfs_send_job_queue_writer_open:-0}" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-1"
			g_zxfer_background_job_last_runner_pid=111
		}
		g_option_j_jobs=2
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
	)

	assertEquals "Rolling background scheduling should pass the queue writer fd through the supervisor spawn path." \
		"notify=9 queue_open=1 writer_open=1" "$(cat "$log")"
}

test_zfs_send_receive_appends_multiple_background_job_pids_and_logs_force_flag() {
	log="$TEST_TMPDIR/background_pipeline_multiple.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		l_spawn_count=0
		zxfer_get_send_command() {
			printf '%s\n' "sendcmd-$2"
		}
		zxfer_get_receive_command() {
			printf '%s\n' "recvcmd"
		}
		zxfer_echov() {
			printf 'verbose:%s\n' "$*" >>"$EXEC_LOG"
		}
		zxfer_open_send_job_completion_queue() {
			return 1
		}
		zxfer_spawn_supervised_background_job() {
			l_spawn_count=$((l_spawn_count + 1))
			printf 'spawn:%s\n' "$3" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-$l_spawn_count"
			g_zxfer_background_job_last_runner_pid=$((100 + l_spawn_count))
		}
		g_option_j_jobs=3
		g_option_F_force_rollback="-F"
		g_option_v_verbose=1
		zxfer_zfs_send_receive "tank/src@snap0" "tank/src@snap1" "backup/dst-one" "1"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst-two" "1"
		{
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			printf 'pids=%s\n' "$g_zfs_send_job_pids"
		} >>"$EXEC_LOG"
		printf 'records=%s\n' "$(printf '%s\n' "$g_zfs_send_job_supervisor_records" | sed 's/	/:/g')" >>"$EXEC_LOG"
	)

	assertContains "Background send/receive should log when the receive-side force flag is active." \
		"$(cat "$log")" "verbose:Receive-side force flag (-F) is active for destination [backup/dst-one]."
	assertContains "The first background transfer should still execute its composed pipeline." \
		"$(cat "$log")" "sendcmd-tank/src@snap1 | recvcmd"
	assertContains "The second background transfer should also execute its composed pipeline." \
		"$(cat "$log")" "sendcmd-tank/src@snap2 | recvcmd"
	assertContains "Launching multiple background transfers should append additional tracked PIDs instead of replacing the first one." \
		"$(cat "$log")" "count=2"
	assertContains "Launching multiple background transfers should retain the tracked PID list." \
		"$(cat "$log")" "pids=101 102"
	assertContains "Launching multiple background transfers should retain the tracked supervised job metadata for both datasets." \
		"$(cat "$log")" "records=job-1:101:tank/src@snap1:backup/dst-one:
job-2:102:tank/src@snap2:backup/dst-two:"
}

test_zfs_send_receive_appends_multiple_background_job_pids_in_current_shell() {
	output_file="$TEST_TMPDIR/background_pipeline_multiple_current_shell.out"

	(
		l_spawn_count=0
		zxfer_get_send_command() {
			printf '%s\n' "sendcmd-$2"
		}
		zxfer_get_receive_command() {
			printf '%s\n' "recvcmd"
		}
		zxfer_open_send_job_completion_queue() {
			return 1
		}
		zxfer_spawn_supervised_background_job() {
			l_spawn_count=$((l_spawn_count + 1))
			g_zxfer_background_job_last_id="job-$l_spawn_count"
			g_zxfer_background_job_last_runner_pid=$((200 + l_spawn_count))
		}
		g_option_j_jobs=3
		zxfer_zfs_send_receive "tank/src@snap0" "tank/src@snap1" "backup/dst-one" "1"
		first=$g_zfs_send_job_pids
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst-two" "1"
		second=$g_zfs_send_job_pids
		# shellcheck disable=SC2086
		set -- $g_zfs_send_job_pids
		printf 'argc=%s\n' "$#" >"$output_file"
		printf 'first=%s\n' "$first" >>"$output_file"
		printf 'second=%s\n' "$second" >>"$output_file"
	)

	assertContains "Launching multiple supervised background transfers should leave two tracked runner PIDs in the current shell." \
		"$(cat "$output_file")" "argc=2"
}

test_zfs_send_receive_waits_at_job_limit_before_backgrounding() {
	log="$TEST_TMPDIR/job_limit.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			return 1
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait:%s\n' "$1" >>"$EXEC_LOG"
			g_count_zfs_send_jobs=0
		}
		zxfer_spawn_supervised_background_job() {
			printf 'spawn:%s\n' "$3" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-1"
			g_zxfer_background_job_last_runner_pid=111
		}
		g_option_j_jobs=2
		g_count_zfs_send_jobs=2
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
	)

	assertEquals "Hitting the job limit should wait before spawning the next transfer." \
		"wait:job limit
spawn:sendcmd | recvcmd" "$(cat "$log")"
}

test_zfs_send_receive_waits_for_destination_ancestry_conflicts_before_backgrounding() {
	log="$TEST_TMPDIR/destination_ancestry_wait.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			return 1
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait:%s\n' "$1" >>"$EXEC_LOG"
			g_zfs_send_job_pids=""
			g_zfs_send_job_supervisor_records=""
			g_count_zfs_send_jobs=0
		}
		zxfer_spawn_supervised_background_job() {
			printf 'spawn:%s\n' "$3" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-new"
			g_zxfer_background_job_last_runner_pid=222
		}
		g_option_j_jobs=3
		g_count_zfs_send_jobs=1
		g_zfs_send_job_pids="111"
		g_zfs_send_job_supervisor_records="job-existing	111	tank/src@snap1	backup/dst	"
		zxfer_zfs_send_receive "tank/src/child@snap1" "tank/src/child@snap2" "backup/dst/child" "1"
	)

	assertEquals "Destination-ancestry conflicts should wait even when the numeric job limit still has free slots." \
		"wait:destination ancestry
spawn:sendcmd | recvcmd" "$(cat "$log")"
}

test_zfs_send_receive_reopens_rolling_queue_writer_after_job_limit_wait() {
	log="$TEST_TMPDIR/job_limit_rolling_reopen.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		l_open_count=0
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			l_open_count=$((l_open_count + 1))
			printf 'open:%s writer_before=%s\n' "$l_open_count" "${g_zfs_send_job_queue_writer_open:-0}" >>"$EXEC_LOG"
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			g_zfs_send_job_queue_path="$TEST_TMPDIR/reopen.queue"
			return 0
		}
		zxfer_wait_for_next_zfs_send_job_completion() {
			printf 'wait:%s writer_before=%s\n' "$1" "${g_zfs_send_job_queue_writer_open:-0}" >>"$EXEC_LOG"
			g_zfs_send_job_queue_writer_open=0
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids=111
			g_zfs_send_job_supervisor_records="job-existing	111	tank/src@snap1	backup/other	"
		}
		zxfer_spawn_supervised_background_job() {
			printf 'spawn:writer=%s queue_open=%s notify=%s\n' \
				"${g_zfs_send_job_queue_writer_open:-0}" "${g_zfs_send_job_queue_open:-0}" "${6:-}" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-new"
			g_zxfer_background_job_last_runner_pid=222
		}
		g_option_j_jobs=2
		g_count_zfs_send_jobs=2
		g_zfs_send_job_queue_open=0
		g_zfs_send_job_queue_writer_open=0
		g_option_F_force_rollback=""
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		{
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			printf 'pids=%s\n' "$g_zfs_send_job_pids"
		} >>"$EXEC_LOG"
	)

	assertContains "Rolling background scheduling should reopen the completion-queue writer after a job-limit wait before spawning the next job." \
		"$(cat "$log")" "open:2 writer_before=0"
	assertContains "Reopened rolling background scheduling should launch the new job with the queue writer open." \
		"$(cat "$log")" "spawn:writer=1 queue_open=1 notify=9"
	assertContains "Reopened rolling background scheduling should keep both the existing and new runner PIDs tracked." \
		"$(cat "$log")" "pids=111 222"
}

test_zfs_send_receive_drains_rolling_jobs_before_legacy_fallback_when_reopen_fails() {
	log="$TEST_TMPDIR/job_limit_rolling_reopen_failure.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		l_open_count=0
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			l_open_count=$((l_open_count + 1))
			printf 'open:%s writer_before=%s\n' "$l_open_count" "${g_zfs_send_job_queue_writer_open:-0}" >>"$EXEC_LOG"
			if [ "$l_open_count" -eq 1 ]; then
				g_zfs_send_job_queue_open=1
				g_zfs_send_job_queue_writer_open=1
				g_zfs_send_job_queue_path="$TEST_TMPDIR/reopen-failure.queue"
				return 0
			fi
			g_zfs_send_job_queue_open=0
			g_zfs_send_job_queue_writer_open=0
			return 1
		}
		zxfer_wait_for_next_zfs_send_job_completion() {
			printf 'wait_next:%s writer_before=%s\n' "$1" "${g_zfs_send_job_queue_writer_open:-0}" >>"$EXEC_LOG"
			g_zfs_send_job_queue_writer_open=0
			g_count_zfs_send_jobs=1
			g_zfs_send_job_pids=111
			g_zfs_send_job_supervisor_records="job-existing	111	tank/src@snap1	backup/dst	"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait_all:%s records=%s\n' "$1" "${g_zfs_send_job_supervisor_records:-}" >>"$EXEC_LOG"
			g_zfs_send_job_pids=""
			g_zfs_send_job_supervisor_records=""
			g_count_zfs_send_jobs=0
		}
		zxfer_spawn_supervised_background_job() {
			printf 'spawn:%s notify=<%s>\n' "$3" "${6:-}" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-new"
			g_zxfer_background_job_last_runner_pid=222
		}
		g_option_j_jobs=2
		g_count_zfs_send_jobs=2
		g_zfs_send_job_queue_open=0
		g_zfs_send_job_queue_writer_open=0
		g_option_F_force_rollback=""
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
	)

	assertContains "Rolling background scheduling should drain the remaining tracked rolling jobs before falling back to the legacy path when queue-writer reopen fails." \
		"$(cat "$log")" "wait_all:rolling queue recovery records=job-existing	111	tank/src@snap1	backup/dst	"
	assertContains "Rolling background scheduling should still spawn the transfer through the supervisor path after draining the rolling jobs." \
		"$(cat "$log")" "spawn:sendcmd | recvcmd notify=<>"
}

test_zfs_send_receive_rethrows_supervisor_spawn_failures() {
	log="$TEST_TMPDIR/background_pipeline_supervisor_failure.log"
	: >"$log"

	set +e
	output=$(
		(
			EXEC_LOG="$log"
			zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
			zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
			zxfer_open_send_job_completion_queue() {
				g_zfs_send_job_queue_open=1
				g_zfs_send_job_queue_writer_open=1
				return 0
			}
			zxfer_spawn_supervised_background_job() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			g_option_j_jobs=2
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		) 2>&1
	)
	status=$?

	assertEquals "Rolling background scheduling should fail closed when supervised background-job setup fails." \
		1 "$status"
	assertContains "Rolling background scheduling should preserve the supervisor spawn failure." \
		"$output" "Error creating temporary file."
	assertEquals "Rolling background scheduling should not leave a tracked background job after supervisor spawn failure." \
		"" "$(cat "$log")"
}

test_zfs_send_receive_uses_supervisor_background_path_when_queue_is_unavailable() {
	log="$TEST_TMPDIR/job_limit_legacy_fallback.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_open_send_job_completion_queue() {
			return 1
		}
		zxfer_spawn_supervised_background_job() {
			printf 'spawn:%s notify=<%s>\n' "$3" "${6:-}" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-1"
			g_zxfer_background_job_last_runner_pid=111
		}
		zxfer_wait_for_background_job() {
			printf 'wait:%s\n' "$1" >>"$EXEC_LOG"
			g_zxfer_background_job_wait_exit_status=0
			g_zxfer_background_job_wait_report_failure=""
		}
		g_option_j_jobs=2
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
		zxfer_wait_for_zfs_send_jobs "final sync"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "$g_zfs_send_job_pids" >>"$EXEC_LOG"
	)

	assertEquals "Unavailable rolling queues should still use the supervised background path without queue notifications." \
		"spawn:sendcmd | recvcmd notify=<>
wait:job-1
count=0
pids=" "$(cat "$log")"
}

test_zfs_send_receive_uses_rolling_pool_when_a_job_finishes_early() {
	log="$TEST_TMPDIR/rolling_pool.log"
	release_first="$TEST_TMPDIR/rolling_pool.release_first"
	: >"$log"
	rm -f "$release_first"

	(
		EXEC_LOG="$log"
		l_spawn_count=0
		zxfer_get_send_command() {
			printf '%s\n' "sendcmd-$2"
		}
		zxfer_get_receive_command() {
			printf '%s\n' "recvcmd"
		}
		zxfer_open_send_job_completion_queue() {
			g_zfs_send_job_queue_open=1
			g_zfs_send_job_queue_writer_open=1
			return 0
		}
		zxfer_spawn_supervised_background_job() {
			l_spawn_count=$((l_spawn_count + 1))
			printf 'start:%s\n' "$l_spawn_count" >>"$EXEC_LOG"
			g_zxfer_background_job_last_id="job-$l_spawn_count"
			g_zxfer_background_job_last_runner_pid=$((300 + l_spawn_count))
		}
		zxfer_wait_for_next_zfs_send_job_completion() {
			printf 'wait_next:%s\n' "$1" >>"$EXEC_LOG"
			zxfer_unregister_supervised_send_job "job-2"
		}
		zxfer_wait_for_zfs_send_jobs() {
			printf 'wait_all:%s\n' "$1" >>"$EXEC_LOG"
			g_zfs_send_job_pids=""
			g_zfs_send_job_supervisor_records=""
			g_count_zfs_send_jobs=0
		}
		g_option_j_jobs=2
		zxfer_zfs_send_receive "tank/src@base" "tank/src@snap1" "backup/dst-one" "1"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst-two" "1"
		zxfer_zfs_send_receive "tank/src@snap2" "tank/src@snap3" "backup/dst-three" "1"
		zxfer_wait_for_zfs_send_jobs "final sync"
		printf 'count=%s\n' "$g_count_zfs_send_jobs" >>"$EXEC_LOG"
		printf 'pids=%s\n' "${g_zfs_send_job_pids:-}" >>"$EXEC_LOG"
	)

	line_start3=$(grep -n '^start:3$' "$log" | cut -d: -f1)
	line_wait=$(grep -n '^wait_next:job limit$' "$log" | cut -d: -f1)

	assertContains "Rolling background scheduling should start the second job before the pool refills a freed slot." \
		"$(cat "$log")" "start:2"
	assertContains "Rolling background scheduling should eventually start the third job too." \
		"$(cat "$log")" "start:3"
	assertTrue "The third job should start immediately after a single rolling wait frees one slot instead of draining the entire batch first." \
		"[ '$line_wait' -lt '$line_start3' ]"
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
			l_spawn_count=0
			zxfer_get_send_command() {
				printf '%s\n' "sendcmd-$2"
			}
			zxfer_get_receive_command() {
				printf '%s\n' "recvcmd"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_open_send_job_completion_queue() {
				g_zfs_send_job_queue_open=1
				g_zfs_send_job_queue_writer_open=1
				return 0
			}
			zxfer_spawn_supervised_background_job() {
				l_spawn_count=$((l_spawn_count + 1))
				printf 'start:%s\n' "$l_spawn_count" >>"$EXEC_LOG"
				g_zxfer_background_job_last_id="job-$l_spawn_count"
				g_zxfer_background_job_last_runner_pid=$((400 + l_spawn_count))
			}
			zxfer_wait_for_next_zfs_send_job_completion() {
				printf 'killed:1\n' >>"$EXEC_LOG"
				zxfer_throw_error "zfs send/receive job failed (PID 402, exit 7)."
			}
			g_option_j_jobs=2
			zxfer_zfs_send_receive "tank/src@base" "tank/src@snap1" "backup/dst-one" "1"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst-two" "1"
			zxfer_zfs_send_receive "tank/src@snap2" "tank/src@snap3" "backup/dst-three" "1"
		)
	)
	status=$?
	set -e

	assertEquals "A failed background transfer should abort before scheduling more work." 1 "$status"
	assertContains "The failure should report the failing background PID and exit status." \
		"$output" "zfs send/receive job failed (PID 402, exit 7)."
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
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_j_jobs="invalid"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "1"
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
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_wrap_command_with_ssh() {
			printf '%s<%s:%s:%s>\n' "$1" "$2" "$3" "$4"
		}
		zxfer_handle_progress_bar_option() {
			g_zxfer_progress_bar_command_result="| progress"
		}
		zxfer_execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_option_z_compress=1
		g_option_D_display_progress_bar="pv"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	assertEquals "Remote send/receive should wrap both ends and append the progress helper." \
		"sendcmd<origin.example:1:send> | progress | recvcmd<target.example:1:receive>" "$(cat "$log")"
}

test_zfs_send_receive_rethrows_progress_wrapper_failures() {
	set +e
	output=$(
		(
			zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
			zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
			zxfer_handle_progress_bar_option() {
				zxfer_throw_error "progress wrapper failed"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		) 2>&1
	)
	status=$?

	assertEquals "Send/receive setup should abort when progress-wrapper construction fails." \
		1 "$status"
	assertContains "Send/receive setup should surface progress-wrapper failures instead of continuing with a malformed pipeline." \
		"$output" "progress wrapper failed"
}

test_zfs_send_receive_propagates_nonthrowing_progress_wrapper_failures() {
	set +e
	(
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
		zxfer_handle_progress_bar_option() {
			return 23
		}
		g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)
	status=$?

	assertEquals "Send/receive setup should preserve non-throwing progress-wrapper failures instead of converting them to success." \
		23 "$status"
}

test_zfs_send_receive_rethrows_empty_progress_wrapper_results() {
	set +e
	output=$(
		(
			zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
			zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
			zxfer_handle_progress_bar_option() {
				g_zxfer_progress_bar_command_result=""
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		) 2>&1
	)
	status=$?

	assertEquals "Send/receive setup should fail closed when the progress-wrapper helper returns success without a pipeline fragment." \
		1 "$status"
	assertContains "Empty progress-wrapper results should preserve the explicit setup failure." \
		"$output" "Failed to build progress wrapper for tank/src@snap2."
}

test_zfs_send_receive_uses_explicit_force_flag_argument() {
	log="$TEST_TMPDIR/explicit_force_flag.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() {
			printf 'force=%s\n' "$4" >>"$EXEC_LOG"
			printf '%s\n' "recvcmd"
		}
		zxfer_execute_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_F_force_rollback="-F"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0" ""
	)

	assertContains "Explicit force-flag arguments should override the global rollback flag even when passed as an empty string." \
		"$(cat "$log")" "force="
	assertNotContains "Explicit empty force-flag arguments should not silently fall back to the global rollback flag." \
		"$(cat "$log")" "force=-F"
}

test_zfs_send_receive_uses_resolved_remote_zfs_paths() {
	log="$TEST_TMPDIR/remote_zfs_paths.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_execute_command() {
			printf 'exec=%s\n' "$1" >>"$EXEC_LOG"
			printf 'display=%s\n' "$3" >>"$EXEC_LOG"
		}
		g_cmd_ssh="/usr/bin/ssh"
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_origin_cmd_zfs="/remote/origin/zfs"
		g_target_cmd_zfs="/remote/target/zfs"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
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
