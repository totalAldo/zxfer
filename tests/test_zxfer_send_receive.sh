#!/bin/sh
#
# shunit2 tests for src/zxfer_send_receive.sh: what only a unit test can pin.
# tests/test_contract_send_receive.sh pins the pipeline through the real
# launcher: -D templates, size probes and their fallbacks and failures, the
# dialog's stdout and status, the FIFO failure, -z across each ssh hop, the
# composed pipeline in -j job shells, and the -V pipeline counters.
#
# Pinned here: the size-estimate parser (one table), the fast-probe choice,
# the FIFO's privacy and the stage in every spawn mode, the literal zfs path,
# per-endpoint zfs paths and -V counters that one mock host cannot tell
# apart, and fail-closed branches no mock can reach (unsafe codecs, builders
# that fail without throwing, a job that cannot start).
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_send_receive"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	zxfer_test_reset_all_owner_state || return
	TMPDIR="$TEST_TMPDIR"
	g_cmd_zfs="/sbin/zfs"
	# Distinct local and endpoint codecs show which one a rendered pipeline used.
	g_cmd_compress_safe="gzip"
	g_cmd_decompress_safe="gunzip"
	g_origin_cmd_compress_safe="remote-gzip"
	g_target_cmd_decompress_safe="target-gunzip"
	g_zxfer_send_job_abort_grace_seconds=0
}

# ---------------------------------------------------------------------------
# Size estimates for the -D %%size%% macro.

# Row: previous snapshot|probe output|probe stderr|probe status|verdict;
# cells spell LF as \n, TAB as \t and CR as \r. The exact `zfs send -nPv`
# estimate reads the "size" row, or a bare number on the last line, keeps a
# size that a failing probe still printed, and otherwise fails closed with
# the probe's status and output.
test_calculate_size_estimate_reads_every_probe_output_shape_and_fails_closed_otherwise() {
	while IFS='|' read -r row_previous row_output row_stderr row_status row_verdict; do
		[ -n "$row_verdict" ] || continue
		verdict=$(
			SIZE_PROBE_OUTPUT=$row_output
			SIZE_PROBE_STDERR=$row_stderr
			SIZE_PROBE_STATUS=$row_status
			g_option_O_origin_host=""
			g_option_T_target_host=""
			g_option_j_jobs=1
			zxfer_run_source_zfs_cmd() {
				printf '%b\n' "$SIZE_PROBE_OUTPUT"
				[ -z "$SIZE_PROBE_STDERR" ] || printf '%s\n' "$SIZE_PROBE_STDERR" >&2
				return "$SIZE_PROBE_STATUS"
			}
			zxfer_throw_error() {
				printf 'error=%s:%s\n' "${2:-1}" "$1"
				exit "${2:-1}"
			}
			zxfer_calculate_size_estimate "tank/src@snap2" "$row_previous"
			printf 'size=%s\n' "$g_zxfer_progress_size_estimate_result"
		)
		assertEquals "size estimate for [$row_previous|$row_output|$row_status]" \
			"$(printf '%b' "$row_verdict")" "$verdict"
	done <<'EOF'
|full\ttank/src@snap2\t13424\nsize\t13424||0|size=13424
|full send estimate\nsize\t8192\r||0|size=8192
|2048\r||0|size=2048
tank/src@snap1|size\t4096||0|size=4096
tank/src@snap1|incremental\ttank/src@snap1\ttank/src@snap2\t2048\nsize\t2048||1|size=2048
|full\ttank/src@snap2\t13424\nsize\t13424||1|size=13424
tank/src@snap1|probe failed|stderr-detail|41|error=41:Error calculating incremental estimate: probe failed\nstderr-detail
|probe failed||1|error=1:Error calculating estimate: probe failed
tank/src@snap1|size\tnot-a-number||0|error=1:Error parsing incremental estimate: size\tnot-a-number
|full\ttank/src@snap2\tinvalid||0|error=1:Error parsing estimate: full\ttank/src@snap2\tinvalid
EOF
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

# ---------------------------------------------------------------------------
# The -D stage's FIFO and its job shells.

# Renders the progress stage for DIALOG into $g_zxfer_progress_bar_command_result
# in the current shell.
zxfer_send_receive_test_prepare_progress_stage() {
	g_option_D_display_progress_bar=$1
	zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
}

test_progress_stage_delivers_the_stream_from_a_job_shell_in_every_spawn_mode() {
	zxfer_init_background_shell_spawn_mode
	for spawn_mode in "$g_zxfer_background_shell_spawn_mode" wrapper; do
		out_file="$TEST_TMPDIR/progress_job.$spawn_mode.out"
		err_file="$TEST_TMPDIR/progress_job.$spawn_mode.err"
		dialog_copy="$TEST_TMPDIR/progress_job.$spawn_mode.dialog"
		zxfer_send_receive_test_prepare_progress_stage "cat >'$dialog_copy'"
		g_zxfer_background_shell_spawn_mode=$spawn_mode
		# A fresh sh -c has no zxfer functions: the stage must be plain shell.
		zxfer_spawn_background_shell \
			"printf 'ZXFERMOCKSTREAM x\\n' $g_zxfer_progress_bar_command_result | wc -c" \
			"$out_file" "$err_file"
		wait "$g_last_background_pid"
		job_status=$?

		assertEquals "The $spawn_mode job exits 0; stderr: $(cat "$err_file")" 0 "$job_status"
		assertEquals "The $spawn_mode job's receiver gets every byte." \
			18 "$(tr -d ' ' <"$out_file")"
		assertEquals "The $spawn_mode job's dialog gets a full copy." \
			"ZXFERMOCKSTREAM x" "$(cat "$dialog_copy")"
	done
	zxfer_reset_background_shell_spawn_mode
}

test_progress_stage_uses_a_private_fifo_under_the_run_root() {
	zxfer_send_receive_test_prepare_progress_stage "cat >/dev/null"
	fifo=${g_zxfer_progress_bar_command_result#*"<'"}
	fifo=${fifo%%"'"*}

	case "$fifo" in
	"$g_zxfer_run_tmp_root"/zxfer-progress.*/fifo) inside_root=0 ;;
	*) inside_root=1 ;;
	esac
	assertEquals "The FIFO lives in its own directory under the run root: $fifo" 0 "$inside_root"
	assertTrue "The FIFO is a named pipe." "[ -p '$fifo' ]"
	case "$(ls -ld "${fifo%/*}")" in
	drwx------*) dir_private=0 ;;
	*) dir_private=1 ;;
	esac
	assertEquals "The FIFO directory is private (0700)." 0 "$dir_private"
	case "$(ls -l "$fifo")" in
	prw-------*) fifo_private=0 ;;
	*) fifo_private=1 ;;
	esac
	assertEquals "The FIFO itself is 0600." 0 "$fifo_private"
}

# ---------------------------------------------------------------------------
# Rendering one mock host cannot tell apart, and the -V counters.

test_zfs_send_receive_treats_local_zfs_path_as_literal() {
	marker="$TEST_TMPDIR/send_exec_marker"
	g_cmd_zfs="/bin/echo; touch $marker #"

	cmd=$(
		zxfer_echoV() { :; }
		zxfer_schedule_send_receive_pipeline() { printf '%s\n' "$1"; }
		zxfer_zfs_send_receive "" "tank/fs@snap1" "tank/dst" 0
	)
	eval "$cmd" >/dev/null 2>&1
	g_cmd_zfs="/sbin/zfs"

	assertEquals "The zfs helper path is quoted on both sides of the pipeline." \
		"'/bin/echo; touch $marker #' 'send' 'tank/fs@snap1' | '/bin/echo; touch $marker #' 'receive' 'tank/dst'" "$cmd"
	assertFalse "Shell metacharacters in the zfs helper path must never run." \
		"[ -e '$marker' ]"
}

test_zfs_send_receive_adds_remote_wrappers_and_progress_pipeline() {
	log="$TEST_TMPDIR/remote_progress.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_wrap_command_with_ssh() {
			g_zxfer_wrapped_command_result="<$1:$2:$3:$4>"
		}
		zxfer_handle_progress_bar_option() {
			g_zxfer_progress_bar_command_result="| progress"
		}
		zxfer_execute_rendered_shell_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_origin_cmd_zfs="/origin/zfs"
		g_target_cmd_zfs="/target/zfs"
		g_option_z_compress=1
		g_option_D_display_progress_bar="pv"
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	assertEquals "Remote send/receive should wrap both ends with their resolved zfs paths and append the progress stage after the origin hop." \
		"<'/origin/zfs' 'send' '-I' 'tank/src@snap1' 'tank/src@snap2':origin.example:1:send> | progress | <'/target/zfs' 'receive' 'backup/dst':target.example:1:receive>" "$(cat "$log")"
}

test_zfs_send_receive_tracks_profile_counters_when_very_verbose() {
	log="$TEST_TMPDIR/foreground_pipeline_profile.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_echoV() { :; }
		zxfer_execute_rendered_shell_command() {
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
		g_zxfer_profile_command_render_calls=0
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		{
			printf 'source_zfs=%s\n' "${g_zxfer_profile_source_zfs_calls:-0}"
			printf 'destination_zfs=%s\n' "${g_zxfer_profile_destination_zfs_calls:-0}"
			printf 'send_calls=%s\n' "${g_zxfer_profile_zfs_send_calls:-0}"
			printf 'receive_calls=%s\n' "${g_zxfer_profile_zfs_receive_calls:-0}"
			printf 'pipelines=%s\n' "${g_zxfer_profile_send_receive_pipeline_commands:-0}"
			printf 'background=%s\n' "${g_zxfer_profile_send_receive_background_pipeline_commands:-0}"
			printf 'bucket=%s\n' "${g_zxfer_profile_bucket_send_receive_setup:-0}"
			printf 'renders=%s\n' "${g_zxfer_profile_command_render_calls:-0}"
		} >>"$EXEC_LOG"
	)

	assertEquals "Very-verbose profiling should track foreground send/receive pipeline counts." \
		"'/sbin/zfs' 'send' '-v' '-I' 'tank/src@snap1' 'tank/src@snap2' | '/sbin/zfs' 'receive' 'backup/dst'
source_zfs=1
destination_zfs=1
send_calls=1
receive_calls=1
pipelines=1
background=0
bucket=1
renders=2" "$(cat "$log")"
}

test_zfs_send_receive_tracks_remote_ssh_profile_counters_when_very_verbose() {
	for ssh_hosts in "origin.example target.example" "shared.example shared.example"; do
		output=$(
			(
				zxfer_echoV() { :; }
				zxfer_wrap_command_with_ssh() {
					g_zxfer_wrapped_command_result="<$4 via $2>"
				}
				zxfer_execute_rendered_shell_command() {
					printf '%s\n' "$1"
				}
				g_option_V_very_verbose=1
				g_option_O_origin_host=${ssh_hosts% *}
				g_option_T_target_host=${ssh_hosts#* }
				g_zxfer_profile_source_ssh_shell_invocations=0
				g_zxfer_profile_destination_ssh_shell_invocations=0
				zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
				printf 'source_ssh=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
				printf 'destination_ssh=%s\n' "${g_zxfer_profile_destination_ssh_shell_invocations:-0}"
			)
		)

		assertEquals "Remote send/receive profiling counts one ssh hop per side for [$ssh_hosts]." \
			"<send via ${ssh_hosts% *}> | <receive via ${ssh_hosts#* }>
source_ssh=1
destination_ssh=1" "$output"
	done
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
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=1250
			}
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" 0
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=2000
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

# ---------------------------------------------------------------------------
# Fail-closed branches no mock can reach.

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

test_zfs_send_receive_stops_when_ssh_wrapping_fails_without_throwing() {
	set +e
	for side in send receive; do
		output=$(
			(
				zxfer_echoV() { :; }
				# A throw inside the builder's $() leaves no captured diagnostic.
				zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
					return 5
				}
				zxfer_throw_error() {
					printf 'throw=%s\n' "$1"
					exit "${2:-1}"
				}
				zxfer_schedule_send_receive_pipeline() {
					printf 'scheduled=%s\n' "$1"
				}
				if [ "$side" = send ]; then
					g_option_O_origin_host="origin.example"
				else
					g_option_T_target_host="target.example"
				fi
				zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
				printf 'returned=%s\n' "$?"
			)
		)
		status=$?

		if [ "$side" = send ]; then
			expected="throw=Failed to prepare the ssh send command for [tank/src@snap2]."
		else
			expected="throw=Failed to prepare the ssh receive command for [backup/dst]."
		fi
		assertEquals "A silent $side wrapping failure ends the run with its status." 5 "$status"
		assertEquals "A silent $side wrapping failure is thrown." "$expected" "$output"
	done
}

test_zfs_send_receive_throws_nonthrowing_progress_wrapper_failures() {
	set +e
	output=$(
		(
			zxfer_handle_progress_bar_option() {
				return 23
			}
			zxfer_throw_error() {
				printf 'throw=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_schedule_send_receive_pipeline() {
				printf 'scheduled=%s\n' "$1"
			}
			g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
			printf 'returned=%s\n' "$?"
		)
	)
	status=$?

	assertEquals "A progress stage that fails without throwing still ends the run with its status." \
		23 "$status"
	assertEquals "The failure is thrown before any pipeline is scheduled." \
		"throw=Failed to prepare the progress dialog for [tank/src@snap2]." "$output"
}

test_zfs_send_receive_propagates_background_spawn_failure() {
	set +e
	output=$(
		(
			zxfer_spawn_background_shell() { return 41; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			g_option_j_jobs=2
			zxfer_zfs_send_receive "tank/a@base" "tank/a@new" "backup/a" 1
		)
	)
	status=$?
	assertEquals "Failure to start the shell preserves its status." 41 "$status"
	assertEquals "Spawn failure names the transfer." \
		"Failed to start the send/receive job for [tank/a@new -> backup/a]." "$output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
