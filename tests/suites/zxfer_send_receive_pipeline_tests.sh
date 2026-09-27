#!/bin/sh
# Send/receive progress stage and pipeline execution behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Renders the progress stage for DIALOG into $g_zxfer_progress_bar_command_result
# in the current shell.
zxfer_send_receive_test_prepare_progress_stage() {
	g_option_D_display_progress_bar=$1
	zxfer_handle_progress_bar_option "tank/src@snap2" "tank/src@snap1"
}

test_progress_stage_passes_the_stream_through_eval_and_discards_dialog_stdout() {
	dialog_copy="$TEST_TMPDIR/progress_eval.dialog"
	receiver="$TEST_TMPDIR/progress_eval.receiver"
	rm -f "$dialog_copy" "$receiver"
	zxfer_send_receive_test_prepare_progress_stage "tee '$dialog_copy'"

	zxfer_execute_rendered_shell_command \
		"printf 'payload\\n' $g_zxfer_progress_bar_command_result | cat >'$receiver'"

	assertEquals "The receiver gets the stream exactly once; dialog stdout is discarded." \
		"payload" "$(cat "$receiver")"
	assertEquals "The dialog reads a full copy of the stream." \
		"payload" "$(cat "$dialog_copy")"
}

test_progress_stage_keeps_tee_status_when_the_dialog_fails_after_reading() {
	zxfer_send_receive_test_prepare_progress_stage "cat >/dev/null; exit 7"

	# The subshell keeps the stage's exit off this shell where the last
	# pipeline element runs in the current shell (ksh93).
	output=$(
		(eval "printf 'payload\\n' $g_zxfer_progress_bar_command_result")
		printf 'stage=%s\n' "$?"
	)

	assertEquals "A dialog that exits non-zero after EOF does not fail the stream." \
		"payload
stage=0" "$output"
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

test_progress_stage_fails_closed_when_the_fifo_cannot_be_created() {
	set +e
	output=$(
		(
			mkfifo() { return 1; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_send_receive_test_prepare_progress_stage "cat >/dev/null"
		)
	)
	status=$?

	assertEquals "A FIFO that cannot be created stops the transfer." 1 "$status"
	assertEquals "The failure names the snapshot." \
		"Failed to prepare the progress dialog FIFO for tank/src@snap2." "$output"
}

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

test_zfs_send_receive_runs_foreground_pipeline() {
	log="$TEST_TMPDIR/foreground_pipeline.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_echoV() { :; }
		zxfer_execute_rendered_shell_command() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
		}
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		printf 'performed=%s\n' "$g_is_performed_send_destroy" >>"$EXEC_LOG"
	)

	assertEquals "Foreground send/receive should execute a single pipeline." \
		"'/sbin/zfs' 'send' '-I' 'tank/src@snap1' 'tank/src@snap2' | '/sbin/zfs' 'receive' 'backup/dst'
performed=1" "$(cat "$log")"
}

test_zfs_send_receive_invalidates_destination_cache_after_live_receive() {
	log="$TEST_TMPDIR/foreground_invalidation.log"
	: >"$log"

	(
		EXEC_LOG="$log"
		zxfer_echoV() { :; }
		zxfer_execute_rendered_shell_command() {
			printf 'exec\n' >>"$EXEC_LOG"
		}
		zxfer_invalidate_destination_property_mutation_cache() {
			printf 'properties=%s %s\n' "$1" "$2" >>"$EXEC_LOG"
		}
		zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
	)

	# The receive only changed the destination dataset's own snapshots, so
	# property caches are invalidated for that exact dataset but the
	# whole-tree snapshot record cache (and its in-memory fallback) must
	# survive for the remaining datasets' -d delete planning.
	assertEquals "A foreground receive invalidates only the received dataset's property cache." \
		"exec
properties=backup/dst exact" "$(cat "$log")"
}

test_zfs_send_receive_marks_destination_hierarchy_exists_after_foreground_receive() {
	output=$(
		(
			zxfer_echoV() { :; }
			zxfer_mark_destination_root_missing_in_cache "backup"
			zxfer_execute_rendered_shell_command() {
				:
			}
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst/child" "0"
			printf 'root=%s\n' "$(zxfer_lookup_destination_existence_cache "backup" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'parent=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'child=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst/child" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			sibling_status=0
			sibling_state=$(zxfer_lookup_destination_existence_cache "backup/other" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result") ||
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

test_zfs_send_receive_stops_when_ssh_wrapping_throws() {
	set +e
	output=$(
		(
			zxfer_echoV() { :; }
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
				zxfer_throw_error "ssh prepare failed" 7
			}
			zxfer_throw_error() {
				printf 'throw=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_schedule_send_receive_pipeline() {
				printf 'scheduled=%s\n' "$1"
			}
			g_option_O_origin_host="origin.example"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0"
		)
	)
	status=$?

	assertEquals "An ssh wrapping failure ends the run with its status." 7 "$status"
	assertContains "The wrapping failure is thrown." "$output" "throw=ssh prepare failed"
	assertNotContains "No pipeline is scheduled after a wrapping failure." "$output" "scheduled="
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

test_zfs_send_receive_tracks_background_pipeline_and_defers_mutation_hooks() {
	log="$TEST_TMPDIR/background_pipeline.log"
	: >"$log"
	(
		EXEC_LOG=$log
		zxfer_spawn_background_shell() {
			printf 'spawn:%s\n' "$1" >>"$EXEC_LOG"
			g_last_background_pid=$((110 + g_count_zfs_send_jobs))
			g_zxfer_background_shell_scope=pgid
		}
		zxfer_note_destination_receive_completed() {
			printf 'premature-note:%s\n' "$1" >>"$EXEC_LOG"
		}
		g_option_j_jobs=3
		zxfer_zfs_send_receive "tank/a@base" "tank/a@new" "backup/a" 1
		zxfer_zfs_send_receive "tank/b@base" "tank/b@new" "backup/b" 1
		printf 'count=%s\nperformed=%s\nrecords=%s\n' "$g_count_zfs_send_jobs" \
			"$g_is_performed_send_destroy" "$g_zxfer_send_jobs" >>"$EXEC_LOG"
	)
	assertContains "The composed pipeline is handed to the background shell." "$(cat "$log")" \
		"'/sbin/zfs' 'send' '-I' 'tank/a@base' 'tank/a@new' | '/sbin/zfs' 'receive' 'backup/a' &"
	assertContains "Both scheduled jobs stay in the current-shell registry." "$(cat "$log")" "count=2"
	assertContains "Scheduling a job marks the pass as having sent." "$(cat "$log")" "performed=1"
	assertContains "The first destination is tracked." "$(cat "$log")" "backup/a"
	assertContains "The second destination is tracked." "$(cat "$log")" "backup/b"
	assertNotContains "Mutation hooks wait for successful completion." "$(cat "$log")" "premature-note:"
}

test_zfs_send_receive_hands_job_shells_plain_shell_only() {
	log="$TEST_TMPDIR/job_shell_commands.log"
	: >"$log"
	(
		EXEC_LOG=$log
		zxfer_echoV() { :; }
		zxfer_run_source_zfs_cmd() { printf '4096\n'; }
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
			g_zxfer_prepared_ssh_shell_command_result="'ssh' '$1' '$2'"
		}
		zxfer_spawn_send_job() {
			printf '%s\n' "$1" >>"$EXEC_LOG"
			g_count_zfs_send_jobs=0
		}
		g_option_j_jobs=3
		g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
		zxfer_zfs_send_receive "tank/a@base" "tank/a@new" "backup/a" 1
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example"
		g_option_z_compress=1
		zxfer_zfs_send_receive "tank/b@base" "tank/b@new" "backup/b" 1
	)
	wrapper="$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_cleanup_child_wrapper.sh"
	commands=$(cat "$log")

	assertEquals "Both pipelines reach the job spawner." 2 "$(grep -c . "$log")"
	assertContains "The progress stage runs the dialog under the wrapper." \
		"$commands" "'/bin/sh' '$wrapper' 'pv -s 4096 -N tank/a@new'"
	# Single-quoted text is data (paths, dialogs); a zxfer function call
	# would have to be an unquoted word.
	unquoted=$(sed "s/'[^']*'//g" "$log")
	assertNotContains "A job shell has no zxfer functions: $unquoted" "$unquoted" "zxfer_"
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

test_zfs_send_receive_rethrows_progress_wrapper_failures() {
	set +e
	output=$(
		(
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

test_zfs_send_receive_uses_explicit_force_flag_argument() {
	output=$(
		(
			zxfer_echoV() { :; }
			zxfer_schedule_send_receive_pipeline() { printf '%s\n' "$1"; }
			g_option_F_force_rollback="-F"
			zxfer_zfs_send_receive "tank/src@snap1" "tank/src@snap2" "backup/dst" "0" ""
			zxfer_zfs_send_receive "" "tank/src@snap2" "backup/new" "0" "-F"
		)
	)

	assertContains "An explicit empty force-flag argument overrides the global -F." \
		"$output" "| '/sbin/zfs' 'receive' 'backup/dst'"
	assertContains "An explicit -F argument forces the receive." \
		"$output" "| '/sbin/zfs' 'receive' '-F' 'backup/new'"
}
