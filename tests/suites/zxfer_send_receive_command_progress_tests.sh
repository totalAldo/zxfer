#!/bin/sh
# Send/receive command rendering, sizing, and progress behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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

test_wrap_command_with_ssh_rethrows_compressed_host_spec_split_failures() {
	set +e
	send_output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid send host"
				return 75
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 1 send
		)
	)
	send_status=$?
	receive_output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid receive host"
				return 76
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive
		)
	)
	receive_status=$?
	set -e

	assertEquals "Compressed send wrapping should fail closed when host-spec token splitting fails." \
		1 "$send_status"
	assertContains "Compressed send wrapping should preserve the host-spec split diagnostic." \
		"$send_output" "invalid send host"
	assertEquals "Compressed receive wrapping should fail closed when host-spec token splitting fails." \
		1 "$receive_status"
	assertContains "Compressed receive wrapping should preserve the host-spec split diagnostic." \
		"$receive_output" "invalid receive host"
}

test_zxfer_reset_send_receive_state_clears_queue_and_progress_scratch() {
	g_count_zfs_send_jobs=3
	g_zfs_send_job_pids="111 222"
	g_zfs_send_job_supervisor_records="job-1	111"
	g_zfs_send_job_queue_open=1
	g_zfs_send_job_queue_unavailable=1
	g_zfs_send_job_queue_path="$TEST_TMPDIR/queue"
	g_zfs_send_job_queue_dir="$TEST_TMPDIR/queue-dir"
	g_zfs_send_job_queue_writer_open=1
	g_zxfer_progress_size_estimate_result="4096"
	g_zxfer_progress_probe_output_result="size	4096"
	g_zxfer_progress_bar_command_result="| pv"

	zxfer_reset_send_job_state
	zxfer_reset_send_receive_state

	assertEquals "Resetting send/receive state should clear tracked job counts." \
		0 "${g_count_zfs_send_jobs:-0}"
	assertEquals "Resetting send/receive state should clear tracked background PIDs." \
		"" "$g_zfs_send_job_pids"
	assertEquals "Resetting send/receive state should clear tracked supervisor records." \
		"" "$g_zfs_send_job_supervisor_records"
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
	assertEquals "Resetting send/receive state should clear cached progress estimates." \
		"" "$g_zxfer_progress_size_estimate_result"
	assertEquals "Resetting send/receive state should clear cached progress probe output." \
		"" "$g_zxfer_progress_probe_output_result"
	assertEquals "Resetting send/receive state should clear the staged progress-wrapper command." \
		"" "$g_zxfer_progress_bar_command_result"
}

test_send_receive_source_has_one_supervised_background_job_path() {
	source_file="$TESTS_DIR/../src/zxfer_send_jobs.sh"
	spawn_count=$(grep -c 'zxfer_spawn_supervised_background_job' "$source_file")

	assertEquals "Send-job production code should have one supervised background-job spawn site." \
		1 "$spawn_count"
	if grep -q 'g_zfs_send_job_records' "$source_file"; then
		fail "Send-job production code must not restore the retired PID-to-status-file registry."
	fi
	if grep -q 'zxfer_wait_for_zfs_send_jobs_legacy' "$source_file"; then
		fail "Send-job production code must not restore the retired legacy wait path."
	fi
	if grep -q 'wait "[$]l_pid"' "$source_file"; then
		fail "Send-job completion must remain owned by the background-job supervisor."
	fi
}

test_profile_record_send_receive_pipeline_metrics_records_startup_latency_once() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_option_n_dryrun=0
			g_option_O_origin_host=""
			g_option_T_target_host=""
			g_zxfer_profile_has_data=0
			g_zxfer_profile_start_ms=1000
			g_zxfer_profile_startup_latency_ms=0
			g_zxfer_profile_startup_latency_recorded=0
			g_zxfer_profile_source_zfs_calls=0
			g_zxfer_profile_destination_zfs_calls=0
			g_zxfer_profile_zfs_send_calls=0
			g_zxfer_profile_zfs_receive_calls=0
			zxfer_profile_now_ms() {
				printf '%s\n' 1250
			}
			zxfer_profile_record_send_receive_pipeline_metrics
			zxfer_profile_now_ms() {
				printf '%s\n' 2000
			}
			zxfer_profile_record_send_receive_pipeline_metrics
			printf 'latency=%s\n' "$g_zxfer_profile_startup_latency_ms"
			printf 'recorded=%s\n' "$g_zxfer_profile_startup_latency_recorded"
			printf 'send_calls=%s\n' "$g_zxfer_profile_zfs_send_calls"
		)
	)

	assertContains "The first live send/receive pipeline should record startup latency." \
		"$output" "latency=250"
	assertContains "Startup latency should only be recorded once per zxfer run." \
		"$output" "recorded=1"
	assertContains "The existing send-call counter should still be incremented for each pipeline." \
		"$output" "send_calls=2"
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
	assertNotContains "Progress handling should not add lossy buffering commands ahead of the passthrough helper." \
		"$result" "dd obs="
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
	capture_file="$TEST_TMPDIR/progress-estimate-readback-failure.out"

	set +e
	(
		zxfer_create_runtime_artifact_file() {
			: >"$capture_file"
			g_zxfer_runtime_artifact_path_result=$capture_file
			return 0
		}
		zxfer_read_runtime_artifact_file() {
			g_zxfer_runtime_artifact_read_result=""
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
		zxfer_create_runtime_artifact_file() {
			return 37
		}
		zxfer_capture_progress_estimate_probe_output sh -c "printf '%s\n' 'size\t4096'"
	)
	status=$?

	assertEquals "Progress-estimate capture should preserve temp-file allocation failures exactly." \
		37 "$status"
}

test_zxfer_capture_progress_estimate_probe_output_preserves_failed_probe_output() {
	set +e
	output=$(
		(
			zxfer_capture_progress_estimate_probe_output sh -c "printf '%s\n' 'stdout-size'; printf '%s\n' 'stderr-detail' >&2; exit 41"
			status=$?
			printf 'status=%s\n' "$status"
			printf 'result=<%s>\n' "$g_zxfer_progress_probe_output_result"
			exit "$status"
		)
	)
	status=$?
	set -e

	assertEquals "Progress-estimate capture should preserve the probe command status after readback." \
		41 "$status"
	assertContains "Progress-estimate capture should publish stdout and stderr for failed probes." \
		"$output" "result=<stdout-size
stderr-detail>"
}

test_get_send_command_display_includes_verbose_raw_flags_for_full_send() {
	g_option_V_very_verbose=1
	g_option_w_raw_send=1

	result=$(zxfer_get_send_command "" "tank/src@snap9")

	assertEquals "Display-mode full sends should include verbose and raw flags when enabled." \
		"/sbin/zfs send -v -w tank/src@snap9" "$result"
}
