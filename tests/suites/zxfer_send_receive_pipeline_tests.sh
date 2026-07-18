#!/bin/sh
# Send/receive progress passthrough and pipeline execution behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
			mkfifo() {
				: >"$1"
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
				return 37
			}
			zxfer_progress_passthrough "sleep 30"
		)
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "Progress passthrough should fail closed when cleanup-registration recovery cannot tear down the spawned helper." \
		37 "$status"
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

test_zxfer_progress_passthrough_discards_progress_command_stdout() {
	output=$(
		printf 'payload\n' | zxfer_progress_passthrough "cat"
	)
	status=$?

	assertEquals "Progress passthrough should preserve the primary send stream." 0 "$status"
	assertEquals "Progress command stdout should not be allowed to duplicate or corrupt the receive stream." \
		"payload" "$output"
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
		zxfer_execute_rendered_shell_command() {
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
		zxfer_execute_rendered_shell_command() {
			printf 'exec=%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf 'properties=%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	# The receive only changed the destination dataset's own snapshots, so
	# property caches are invalidated for that dataset but the whole-tree
	# snapshot record cache (and its in-memory fallback) must survive for the
	# remaining datasets' -d delete planning.
	assertEquals "Successful live send/receive should invalidate destination property caches for the receive dataset without wiping the whole-tree snapshot record cache." \
		"exec=sendcmd | recvcmd
properties=backup/dst" "$(cat "$log")"
}

test_zfs_send_receive_marks_destination_hierarchy_exists_after_foreground_receive() {
	output=$(
		(
			zxfer_echoV() { :; }
			zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
			zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
			zxfer_mark_destination_root_missing_in_cache "backup"
			zxfer_execute_rendered_shell_command() {
				:
			}
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst/child" "0"
			printf 'root=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup")"
			printf 'parent=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'child=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"
			sibling_status=0
			sibling_state=$(zxfer_get_destination_existence_cache_entry "backup/other") ||
				sibling_status=$?
			printf 'sibling=%s status=%s\n' "$sibling_state" "$sibling_status"
		)
	)

	assertContains "Foreground receives should mark the cache root as existing after success." \
		"$output" "root=1"
	assertContains "Foreground receives should mark parent datasets as existing after success." \
		"$output" "parent=1"
	assertContains "Foreground receives should mark the receive dataset as existing after success." \
		"$output" "child=1"
	assertContains "Foreground receives should clear stale missing-root assumptions so unrelated descendants are live-probed." \
		"$output" "sibling= status=1"
}

test_zfs_send_receive_tracks_profile_counters_when_very_verbose() {
	log="$TEST_TMPDIR/foreground_pipeline_profile.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_echoV() { :; }
		zxfer_get_send_command() { printf '%s\n' "sendcmd"; }
		zxfer_get_receive_command() { printf '%s\n' "recvcmd"; }
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
		zxfer_execute_rendered_shell_command() {
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
		zxfer_execute_rendered_shell_command() {
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
		zxfer_note_destination_receive_completed() {
			printf 'note=%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf 'properties=%s\n' "$1" >>"$EXEC_LOG"
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
	assertNotContains "Background send/receive should wait for completion before publishing destination receive mutation state." \
		"$(cat "$log")" "properties=backup/dst"
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

test_zfs_send_receive_drains_rolling_jobs_before_batch_fallback_when_reopen_fails() {
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

	assertContains "Rolling background scheduling should drain the remaining tracked rolling jobs before falling back to the batch path when queue-writer reopen fails." \
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
	log="$TEST_TMPDIR/job_limit_batch_fallback.log"
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
		zxfer_execute_rendered_shell_command() {
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
		zxfer_execute_rendered_shell_command() {
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
		zxfer_execute_rendered_shell_command() {
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
		zxfer_execute_rendered_shell_command() {
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
