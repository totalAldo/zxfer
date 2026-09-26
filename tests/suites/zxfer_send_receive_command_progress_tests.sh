#!/bin/sh
# Send/receive command rendering, sizing, and progress behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_wrap_command_with_ssh_receive_direction_with_compression() {
	result=$(
		g_option_T_target_host="target.example doas"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { g_zxfer_prepared_ssh_shell_command_result="'/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' '$2'"; }
		zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example doas" 1 receive || exit
		printf '%s' "$g_zxfer_wrapped_command_result"
	)

	assertEquals "Receive-side compression should wrap the remote command in the documented direction." \
		"gzip | '/usr/bin/ssh' 'target.example' 'doas' 'sh' '-c' 'target-gunzip | zfs receive tank/dst'" "$result"
}

test_wrap_command_with_ssh_without_compression_uses_remote_shell_wrapper_for_multi_token_hosts() {
	result=$(
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { g_zxfer_prepared_ssh_shell_command_result="'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' '$2'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 0 send || exit
		printf '%s' "$g_zxfer_wrapped_command_result"
	)

	assertEquals "Non-compressed wrapper hosts should execute through a remote sh -c wrapper." \
		"'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap'" "$result"
}

test_wrap_command_with_ssh_send_direction_with_compression_and_wrapper_host() {
	result=$(
		g_option_O_origin_host="origin.example pfexec"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { g_zxfer_prepared_ssh_shell_command_result="'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' '$2'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 1 send || exit
		printf '%s' "$g_zxfer_wrapped_command_result"
	)

	assertEquals "Compressed send wrappers should compress remotely before piping back through the safe decompressor." \
		"'/usr/bin/ssh' 'origin.example' 'pfexec' 'sh' '-c' 'zfs send tank/src@snap | remote-gzip' | gunzip" "$result"
}

test_wrap_command_with_ssh_send_direction_with_compression_and_simple_host() {
	result=$(
		g_option_O_origin_host="origin.example"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { g_zxfer_prepared_ssh_shell_command_result="'/usr/bin/ssh' 'origin.example' '$2'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example" 1 send || exit
		printf '%s' "$g_zxfer_wrapped_command_result"
	)

	assertEquals "Compressed send wrappers on simple hosts should still append the safe local decompressor." \
		"'/usr/bin/ssh' 'origin.example' 'zfs send tank/src@snap | remote-gzip' | gunzip" "$result"
}

test_wrap_command_with_ssh_receive_direction_with_compression_and_simple_host() {
	result=$(
		g_option_T_target_host="target.example"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { g_zxfer_prepared_ssh_shell_command_result="'/usr/bin/ssh' 'target.example' '$2'"; }
		zxfer_wrap_command_with_ssh "zfs receive tank/dst" "target.example" 1 receive || exit
		printf '%s' "$g_zxfer_wrapped_command_result"
	)

	assertEquals "Compressed receive wrappers on simple hosts should stream through the safe compressor locally." \
		"gzip | '/usr/bin/ssh' 'target.example' 'target-gunzip | zfs receive tank/dst'" "$result"
}

# Regression: with byte-identical -O and -T specs the receive side used the
# origin slot's decompressor, which holds the LOCAL path.
test_wrap_command_with_ssh_picks_the_codec_by_role_when_origin_and_target_specs_match() {
	result=$(
		g_option_O_origin_host="nas.example"
		g_option_T_target_host="nas.example"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { g_zxfer_prepared_ssh_shell_command_result="'/usr/bin/ssh' 'nas.example' '$2'"; }
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "nas.example" 1 send || exit
		printf '%s\n' "$g_zxfer_wrapped_command_result"
		zxfer_wrap_command_with_ssh "zfs receive tank/dst" "nas.example" 1 receive || exit
		printf '%s\n' "$g_zxfer_wrapped_command_result"
	)

	assertEquals "The send side should compress with the origin codec and the receive side decompress with the target codec." \
		"'/usr/bin/ssh' 'nas.example' 'zfs send tank/src@snap | remote-gzip' | gunzip
gzip | '/usr/bin/ssh' 'nas.example' 'target-gunzip | zfs receive tank/dst'" "$result"
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
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
				return 73
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "origin.example pfexec" 0 send || exit
			printf '%s' "$g_zxfer_wrapped_command_result"
		)
	)
	status=$?

	assertEquals "SSH command wrapping should preserve the exact remote-shell wrapper builder status." \
		73 "$status"
	assertEquals "SSH command wrapping should not publish a partial wrapped command when remote-shell wrapper construction fails." \
		"" "$output"
}

# Runs one failing wrap in a subshell and prints "STATUS:RESULT".
zxfer_send_receive_test_wrap_failure() {
	(
		zxfer_wrap_command_with_ssh "$@"
		l_wrap_failure_status=$?
		printf '%s:%s' "$l_wrap_failure_status" "$g_zxfer_wrapped_command_result"
	)
}

test_wrap_command_with_ssh_preserves_compressed_builder_failures_for_all_host_shapes() {
	set +e
	send_wrapper=$(
		g_option_O_origin_host="origin.example pfexec"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { return 81; }
		zxfer_send_receive_test_wrap_failure "zfs send tank/src@snap" "origin.example pfexec" 1 send
	)
	send_simple=$(
		g_option_O_origin_host="origin.example"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { return 83; }
		zxfer_send_receive_test_wrap_failure "zfs send tank/src@snap" "origin.example" 1 send
	)
	receive_wrapper=$(
		g_option_T_target_host="target.example doas"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { return 84; }
		zxfer_send_receive_test_wrap_failure "zfs receive tank/dst" "target.example doas" 1 receive
	)
	receive_simple=$(
		g_option_T_target_host="target.example"
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() { return 86; }
		zxfer_send_receive_test_wrap_failure "zfs receive tank/dst" "target.example" 1 receive
	)

	assertEquals "Compressed send wrapping should preserve ssh wrapper failures for multi-token hosts." \
		"81:" "$send_wrapper"
	assertEquals "Compressed send wrapping should preserve ssh wrapper failures for simple hosts." \
		"83:" "$send_simple"
	assertEquals "Compressed receive wrapping should preserve ssh wrapper failures for multi-token hosts." \
		"84:" "$receive_wrapper"
	assertEquals "Compressed receive wrapping should preserve ssh wrapper failures for simple hosts." \
		"86:" "$receive_simple"
}

test_wrap_command_with_ssh_rethrows_host_spec_split_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" 'origin.example "pfexec"' 0 send
		)
	)
	status=$?

	assertEquals "SSH wrapping should fail closed when host-spec token splitting fails." \
		1 "$status"
	assertContains "SSH wrapping should preserve the host-spec split diagnostic." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only"
}

test_wrap_command_with_ssh_rethrows_compressed_host_spec_split_failures() {
	set +e
	send_output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" 'origin.example "pfexec"' 1 send
		)
	)
	send_status=$?
	receive_output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_wrap_command_with_ssh "zfs receive tank/dst" 'target.example "doas"' 1 receive
		)
	)
	receive_status=$?
	set -e

	assertEquals "Compressed send wrapping should fail closed when host-spec token splitting fails." \
		1 "$send_status"
	assertContains "Compressed send wrapping should preserve the host-spec split diagnostic." \
		"$send_output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only"
	assertEquals "Compressed receive wrapping should fail closed when host-spec token splitting fails." \
		1 "$receive_status"
	assertContains "Compressed receive wrapping should preserve the host-spec split diagnostic." \
		"$receive_output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only"
}

test_zxfer_reset_send_receive_state_clears_jobs_and_progress_scratch() {
	g_count_zfs_send_jobs=3
	g_zxfer_send_jobs="stale job"
	g_zxfer_send_job_abort_failure_message="stale failure"
	g_zxfer_progress_size_estimate_result=4096
	g_zxfer_progress_bar_command_result="| pv"
	g_zxfer_wrapped_command_result="ssh host cmd"
	zxfer_reset_send_job_state
	zxfer_reset_send_receive_state
	assertEquals "Reset clears the active job count." 0 "$g_count_zfs_send_jobs"
	assertEquals "Reset clears the active job records." "" "$g_zxfer_send_jobs"
	assertEquals "Reset clears the abort diagnostic." "" "$g_zxfer_send_job_abort_failure_message"
	assertEquals "Reset clears the cached estimate." "" "$g_zxfer_progress_size_estimate_result"
	assertEquals "Reset clears the rendered progress command." "" "$g_zxfer_progress_bar_command_result"
	assertEquals "Reset clears the wrapped command." "" "$g_zxfer_wrapped_command_result"
}

test_zfs_send_receive_records_startup_latency_once() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=0
			g_zxfer_profile_start_ms=1000
			g_zxfer_profile_startup_latency_ms=0
			g_zxfer_profile_startup_latency_recorded=0
			g_zxfer_profile_zfs_send_calls=0
			zxfer_echoV() { :; }
			zxfer_schedule_send_receive_pipeline() { :; }
			zxfer_profile_now_ms() {
				printf '%s\n' 1250
			}
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" 0
			zxfer_profile_now_ms() {
				printf '%s\n' 2000
			}
			zxfer_zfs_send_receive "tank/src@snap2" "tank/src@snap3" "backup/dst" 0
			printf 'latency=%s\n' "$g_zxfer_profile_startup_latency_ms"
			printf 'recorded=%s\n' "$g_zxfer_profile_startup_latency_recorded"
			printf 'send_calls=%s\n' "$g_zxfer_profile_zfs_send_calls"
		)
	)

	assertContains "The first live send/receive pipeline should record startup latency." \
		"$output" "latency=250"
	assertContains "Startup latency should only be recorded once per zxfer run." \
		"$output" "recorded=1"
	assertContains "The send-call counter should still be incremented for each pipeline." \
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
			printf "%s\n" "stderr-detail" >&2
			if [ "$l_restore_xtrace" -eq 1 ]; then
				set -x
			fi
			return 41
		}
		zxfer_throw_error() {
			printf "%s\nstatus=%s\n" "$1" "$2"
			exit "$2"
		}
		zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
	'

	assertEquals "Incremental size estimation failures should abort with the probe status." \
		41 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Incremental estimate failures should preserve the operator-facing error prefix." \
		"$(cat "$l_stdout_file")" "Error calculating incremental estimate: probe failed"
	assertContains "Incremental estimate failures should include the probe's stderr." \
		"$(cat "$l_stdout_file")" "stderr-detail"
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

# Prints the first zfs verb the size estimate issues for one -O/-T/-j setup.
zxfer_send_receive_test_first_size_probe_verb() {
	l_probe_verb_log="$TEST_TMPDIR/size_probe_verbs.log"
	: >"$l_probe_verb_log"
	(
		g_option_O_origin_host=$1
		g_option_T_target_host=$2
		g_option_j_jobs=$3
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "$1" >>"$l_probe_verb_log"
			printf '4096\n'
		}
		zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
	)
	head -n 1 "$l_probe_verb_log"
}

test_calculate_size_estimate_uses_the_fast_probe_only_for_remote_or_parallel_runs() {
	assertEquals "Local single-job runs should keep the exact send estimate." \
		send "$(zxfer_send_receive_test_first_size_probe_verb "" "" 1)"
	assertEquals "Parallel runs should try the cheap written@ probe first." \
		get "$(zxfer_send_receive_test_first_size_probe_verb "" "" 4)"
	assertEquals "Remote origin runs should try the cheap written@ probe first." \
		get "$(zxfer_send_receive_test_first_size_probe_verb "origin.example" "" 1)"
	assertEquals "Remote target runs should try the cheap written@ probe first." \
		get "$(zxfer_send_receive_test_first_size_probe_verb "" "target.example" 1)"
}

test_calculate_size_estimate_parses_every_exact_probe_output_shape() {
	for probe_case in \
		"13424|full	tank/src@snap1	13424
size	13424" \
		"8192|full send estimate
size	8192" \
		"2048|2048" \
		"4096|size	4096"; do
		probe_expected=${probe_case%%|*}
		probe_output=${probe_case#*|}
		result=$(
			PROBE_OUTPUT=$probe_output
			zxfer_run_source_zfs_cmd() {
				printf '%s\r\n' "$PROBE_OUTPUT"
			}
			zxfer_calculate_size_estimate "tank/src@snap1" "" || exit
			printf '%s' "$g_zxfer_progress_size_estimate_result"
		)
		assertEquals "The exact probe output <$probe_output> should yield its size." \
			"$probe_expected" "$result"
	done
}

test_calculate_size_estimate_uses_fast_incremental_probe_when_requested() {
	log="$TEST_TMPDIR/fast_incremental_estimate.log"
	: >"$log"

	result=$(
		(
			LOG_FILE="$log"
			g_option_j_jobs=2
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				printf '%s\n' "2048"
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
			printf '%s\n' "$g_zxfer_progress_size_estimate_result"
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
			g_option_j_jobs=2
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				if [ "$1" = "get" ]; then
					printf '%s\n' "unsupported"
					return 1
				elif [ "$1" = "send" ]; then
					printf 'size\t8192\n'
				fi
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1"
			printf '%s\n' "$g_zxfer_progress_size_estimate_result"
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
			g_option_j_jobs=2
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				printf '%s\n' "16384"
			}
			zxfer_calculate_size_estimate "tank/src@snap2" ""
			printf '%s\n' "$g_zxfer_progress_size_estimate_result"
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
			g_option_j_jobs=2
			g_option_V_very_verbose=1
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "$*" >>"$LOG_FILE"
				if [ "$1" = "list" ]; then
					printf '%s\n' "unsupported"
					return 1
				fi
				printf 'size\t12288\n'
			}
			zxfer_calculate_size_estimate "tank/src@snap2" ""
			printf '%s\n' "$g_zxfer_progress_size_estimate_result"
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

test_calculate_size_estimate_accepts_probe_size_output_when_probe_status_is_nonzero() {
	incremental=$(
		zxfer_run_source_zfs_cmd() {
			printf 'incremental\ttank/src@snap1\ttank/src@snap2\t2048\nsize\t2048\n'
			return 1
		}
		zxfer_calculate_size_estimate "tank/src@snap2" "tank/src@snap1" || exit
		printf '%s' "$g_zxfer_progress_size_estimate_result"
	)
	full=$(
		zxfer_run_source_zfs_cmd() {
			printf 'full\ttank/src@snap1\t13424\nsize\t13424\n'
			return 1
		}
		zxfer_calculate_size_estimate "tank/src@snap1" "" || exit
		printf '%s' "$g_zxfer_progress_size_estimate_result"
	)

	assertEquals "Incremental size estimation should keep a usable size record even when the exact dry-run probe exits nonzero." \
		"2048" "$incremental"
	assertEquals "Full size estimation should keep a usable size record even when the exact dry-run probe exits nonzero." \
		"13424" "$full"
}

# Renders the progress stage for SNAPSHOT with a 4096 estimate and prints it.
zxfer_send_receive_test_render_progress_stage() {
	(
		zxfer_calculate_size_estimate() {
			g_zxfer_progress_size_estimate_result="4096"
		}
		zxfer_handle_progress_bar_option "$1" "tank/src@snap1" || exit
		printf '%s' "$g_zxfer_progress_bar_command_result"
	)
}

test_handle_progress_bar_option_renders_a_plain_shell_stage() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	result=$(zxfer_send_receive_test_render_progress_stage "tank/src@snap2")
	wrapper="$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_cleanup_child_wrapper.sh"

	assertContains "The stage runs the substituted dialog under the cleanup wrapper." \
		"$result" "| { '/bin/sh' '$wrapper' 'pv -s 4096 -N tank/src@snap2' <'"
	assertContains "The stage tees the stream into the FIFO and keeps tee's status." \
		"$result" "& tee '"
	assertContains "The stage waits for the dialog and exits with tee's status." \
		"$result" "; l_tee=\$?; wait \$!; exit \$l_tee; }"
	assertNotContains "The stage calls no zxfer function, so a -j job shell can run it." \
		"$result" "zxfer_progress_passthrough"
	assertNotContains "Progress handling should not add lossy buffering commands." \
		"$result" "dd obs="
}

test_handle_progress_bar_option_substitutes_macros_in_template_order_literally() {
	g_option_D_display_progress_bar="%%title%%:%%size%%/%%title%% tail"
	# shellcheck disable=SC2016  # The literal $c is the point of the test.
	result=$(zxfer_send_receive_test_render_progress_stage 'tank/a&b$c@s/1')

	assertContains "Mixed templates keep their order and treat substituted values literally." \
		"$result" "'tank/a&b\$c@s/1:4096/tank/a&b\$c@s/1 tail'"
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
			}
			zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
			printf '%s' "$g_zxfer_progress_bar_command_result"
		)
	)

	assertEquals "Progress handling should skip size estimation when the dialog does not use %%size%%." \
		"" "$(cat "$log")"
	assertContains "Progress handling should still substitute the snapshot title when %%size%% is unused." \
		"$result" "'pv -N tank/src@snap2'"
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

test_zfs_send_receive_renders_verbose_raw_full_send() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_option_w_raw_send=1
			zxfer_echoV() { :; }
			zxfer_schedule_send_receive_pipeline() {
				printf '%s\n' "$1"
			}
			zxfer_zfs_send_receive "" "tank/src@snap9" "backup/dst" 0
		)
	)

	assertEquals "Full sends render -v and -w when enabled and each argument single-quoted." \
		"'/sbin/zfs' 'send' '-v' '-w' 'tank/src@snap9' | '/sbin/zfs' 'receive' 'backup/dst'" "$output"
}
