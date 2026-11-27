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
# shellcheck source=tests/helpers/send_job_fixtures.sh
. "$TESTS_DIR/helpers/send_job_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_send_receive"
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
		zxfer_execute_rendered_shell_command() { printf '%s\n' "$1"; }
		zxfer_finish_destination_receive() { :; }
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
			g_zxfer_shell_command_result="<$1:$2:$3:$4>"
		}
		zxfer_handle_progress_bar_option() {
			g_zxfer_shell_command_result="progress rendering scratch"
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
					g_zxfer_shell_command_result="<$4 via $2>"
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
			zxfer_execute_rendered_shell_command() { :; }
			zxfer_finish_destination_receive() { :; }
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
	zxfer_reset_send_receive_state
	assertEquals "Reset clears the active job count." 0 "$g_count_zfs_send_jobs"
	assertEquals "Reset clears the active job records." "" "$g_zxfer_send_jobs"
	assertEquals "Reset clears the abort diagnostic." "" "$g_zxfer_send_job_abort_failure_message"
	assertEquals "Reset clears the cached estimate." "" "$g_zxfer_progress_size_estimate_result"
	assertEquals "Reset clears the rendered progress command." "" "$g_zxfer_progress_bar_command_result"
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

test_wrap_command_with_ssh_retains_input_from_the_canonical_result() {
	l_wrap_output=$(
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
			printf 'input=%s\n' "$2"
			g_zxfer_shell_command_result="prepared ssh"
		}
		g_option_O_origin_host=origin.example
		g_zxfer_shell_command_result="zfs send tank/src@snap"
		zxfer_wrap_command_with_ssh "$g_zxfer_shell_command_result" \
			origin.example 1 send
		printf 'result=%s\n' "$g_zxfer_shell_command_result"
	)
	assertEquals "Wrapping retains its input and publishes the complete local decompression stage." \
		"input=zfs send tank/src@snap | remote-gzip
result=prepared ssh | gunzip" "$l_wrap_output"
}

test_wrap_command_with_ssh_clears_stale_results_on_rejected_input() {
	for l_wrap_empty_part in host command; do
		g_zxfer_shell_command_result="stale command"
		if [ "$l_wrap_empty_part" = host ]; then
			zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "" 0 send
		else
			zxfer_wrap_command_with_ssh "" origin.example 0 send
		fi
		l_wrap_reject_status=$?
		assertEquals "An empty $l_wrap_empty_part keeps the builder failure status." \
			1 "$l_wrap_reject_status"
		assertEquals "An empty $l_wrap_empty_part publishes no stale command." \
			"" "$g_zxfer_shell_command_result"
	done
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
				zxfer_execute_rendered_shell_command() {
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
			zxfer_execute_rendered_shell_command() {
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

bgjob_test_stub_finalize_hooks() {
	zxfer_render_shell_command_from_argv() {
		case $2 in
		send) g_zxfer_shell_command_result=${BGJOB_TEST_SEND:-sleep 1} ;;
		receive) g_zxfer_shell_command_result=${BGJOB_TEST_RECEIVE:-cat >/dev/null} ;;
		*) return 1 ;;
		esac
	}
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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

# ksh93 (illumos /bin/sh) reports a signal death as 256+N; the job shell
# records the usual 128+N, so the reaper reports the signal instead of a job
# shell that died before recording its status.
test_spawn_send_job_records_signal_deaths_as_128_plus_n_under_ksh93() {
	zxfer_test_send_job_fixture_setup
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

test_reap_send_job_reports_a_job_shell_that_died_before_recording_status() {
	zxfer_test_send_job_fixture_setup
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

test_zfs_send_receive_dispatch_runs_and_finishes_foreground_receives() {
	zxfer_test_send_job_fixture_setup
	hook_log="$TEST_TMPDIR/bgjob_foreground.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			BGJOB_TEST_SEND=send
			BGJOB_TEST_RECEIVE=recv
			bgjob_test_stub_finalize_hooks
			zxfer_execute_rendered_shell_command() {
				printf 'exec %s\n' "$1" >>"$BGJOB_HOOK_LOG"
			}
			g_is_performed_send_destroy=0
			g_option_j_jobs=1
			zxfer_zfs_send_receive "" "tank/a@snap" "backup/a" 1
			g_option_j_jobs=3
			zxfer_zfs_send_receive "" "tank/b@snap" "backup/b" 0
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

test_zfs_send_receive_dispatch_waits_at_the_job_limit() {
	zxfer_test_send_job_fixture_setup
	hook_log="$TEST_TMPDIR/bgjob_limit.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_echov() { printf '%s\n' "$1"; }
			g_option_j_jobs=2
			zxfer_zfs_send_receive "" "tank/a@snap" "backup/a" 1
			zxfer_zfs_send_receive "" "tank/b@snap" "backup/b" 1
			printf 'count=%s\n' "$g_count_zfs_send_jobs"
			BGJOB_TEST_SEND="exit 0"
			zxfer_zfs_send_receive "" "tank/c@snap" "backup/c" 1
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

test_zfs_send_receive_dispatch_waits_for_destination_ancestry_conflicts() {
	zxfer_test_send_job_fixture_setup
	hook_log="$TEST_TMPDIR/bgjob_ancestry.hooks"
	: >"$hook_log"
	output=$(
		(
			BGJOB_HOOK_LOG=$hook_log
			bgjob_test_stub_finalize_hooks
			zxfer_echov() { printf '%s\n' "$1"; }
			g_option_j_jobs=4
			zxfer_zfs_send_receive "" "tank/a@snap" "backup/a" 1
			BGJOB_TEST_SEND="exit 0"
			zxfer_zfs_send_receive "" "tank/b@snap" "backup/b" 1
			printf 'independent_count=%s\n' "$g_count_zfs_send_jobs"
			zxfer_zfs_send_receive "" "tank/a/child@snap" "backup/a/child" 1
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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
	zxfer_test_send_job_fixture_setup
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
