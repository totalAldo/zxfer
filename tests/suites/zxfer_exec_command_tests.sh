#!/bin/sh
# Tests for src/zxfer_exec.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_exec_direct_load_does_not_require_transport_capability_or_snapshot_state() {
	/bin/sh -c '
		ZXFER_SOURCE_MODULES_ROOT=$1
		. "$1/src/zxfer_modules.sh"
		zxfer_load_modules zxfer_exec.sh || exit 1
		command -v zxfer_render_shell_command_from_argv >/dev/null 2>&1 || exit 2
		command -v zxfer_invoke_ssh_shell_command_for_host >/dev/null 2>&1 && exit 3
		command -v zxfer_ensure_remote_host_capabilities >/dev/null 2>&1 && exit 4
		command -v zxfer_reset_live_destination_view_state >/dev/null 2>&1 && exit 5
		exit 0
	' zxfer-exec-direct-load "$ZXFER_ROOT"

	assertEquals "Generic exec should load without transport, capability, or snapshot modules." \
		0 "$?"
}

test_execute_command_respects_dry_run_mode() {
	# With --dry-run enabled, zxfer_execute_rendered_shell_command should not run but still
	# describe the action, so no temp files should be created.
	temp_file="$TEST_TMPDIR/dry_run_output"
	g_option_n_dryrun=1

	zxfer_execute_rendered_shell_command "printf 'should not run' > '$temp_file'"

	assertFalse "Dry run should skip running the command." "[ -f \"$temp_file\" ]"
}

test_execute_command_runs_command_when_not_dry_run() {
	# When --dry-run is off, the helper must execute commands verbatim.
	temp_file="$TEST_TMPDIR/run_output"

	zxfer_execute_rendered_shell_command "printf 'ran' > '$temp_file'"

	assertTrue "Command should run when dry run is disabled." "[ -f \"$temp_file\" ]"
	assertEquals "ran" "$(cat "$temp_file")"
}

test_execute_background_cmd_writes_output_file() {
	# Background commands are used for option pipelines; ensure their stdout
	# still lands in the provided tempfile.
	temp_file="$TEST_TMPDIR/bg_output"
	g_last_background_pid=""

	zxfer_execute_rendered_background_shell_command "printf bg-data" "$temp_file"
	bg_pid=$g_last_background_pid
	wait "$bg_pid"

	assertTrue "zxfer_execute_rendered_background_shell_command should expose the spawned PID for callers." \
		"[ -n \"$bg_pid\" ]"
	assertTrue "Background output file should be created." "[ -f \"$temp_file\" ]"
	assertEquals "bg-data" "$(cat "$temp_file")"
}

test_execute_background_cmd_fails_closed_when_cleanup_registration_fails() {
	output=$(
		g_zxfer_cleanup_pid_abort_grace_seconds=0
		zxfer_spawn_background_shell() {
			g_last_background_pid=12345
			g_zxfer_background_shell_scope=pgid
		}
		zxfer_register_cleanup_pid() { return 1; }
		zxfer_signal_background_shell() {
			printf 'signal=%s:%s:%s\n' "$1" "$2" "$3"
		}
		zxfer_execute_rendered_background_shell_command \
			'printf x' "$TEST_TMPDIR/bg_register_fail_output"
		printf 'status=%s pid=<%s>\n' "$?" "$g_last_background_pid"
	)
	assertContains "Registration failure stays fatal after cleanup and clears the published PID." \
		"$output" 'status=1 pid=<>'
	assertContains "Failed registration signals TERM to the owned group." \
		"$output" 'signal=12345:pgid:TERM'
	assertContains "Failed registration escalates the group even when its leader exited on TERM." \
		"$output" 'signal=12345:pgid:KILL'
}

test_execute_background_cmd_preserves_abort_failures_when_cleanup_registration_fails() {
	for failure_signal in TERM KILL; do
		output=$(
			g_zxfer_cleanup_pid_abort_grace_seconds=0
			zxfer_spawn_background_shell() {
				g_last_background_pid=12345
				g_zxfer_background_shell_scope=pgid
			}
			zxfer_register_cleanup_pid() { return 1; }
			zxfer_signal_background_shell() { [ "$3" != "$failure_signal" ]; }
			zxfer_execute_rendered_background_shell_command \
				'printf x' "$TEST_TMPDIR/bg_abort_fail_output"
			printf 'status=%s pid=%s\n' "$?" "$g_last_background_pid"
			zxfer_find_cleanup_pid_record 12345
			printf 'retained=%s scope=%s purpose=%s\n' "$?" \
				"$g_zxfer_cleanup_pid_record_scope" "$g_zxfer_cleanup_pid_record_purpose"
		)
		assertContains "$failure_signal failure retains the published direct-child handle." \
			"$output" 'status=1 pid=12345'
		assertContains "$failure_signal failure retains scope and purpose for ordered trap retry." \
			"$output" 'retained=0 scope=pgid purpose=background command helper'
	done
}

test_signal_background_shell_covers_group_initialization_race() {
	for launch_case in launcher transition gone denied_launcher denied_group; do
		output=$(
			group_live=0
			launcher_live=1
			[ "$launch_case" != gone ] || launcher_live=0
			[ "$launch_case" != denied_group ] || group_live=1
			# The launcher is still in zxfer's own process group.
			ps() { printf ' 777\n'; }
			kill() {
				if [ "$1" != -s ]; then
					# kill -SIG -PGID reaches the process group.
					[ "$1" != -0 ] || return "$((1 - group_live))"
					printf 'group=%s\n' "${1#-}"
					[ "$launch_case" != denied_group ] || return 1
					[ "$group_live" -eq 1 ] || return 1
					group_live=0
				else
					[ "$2" != 0 ] || return "$((1 - launcher_live))"
					printf 'launcher=%s\n' "$2"
					[ "$launch_case" != denied_launcher ] || return 1
					launcher_live=0
					# setsid can establish the group immediately before the
					# direct signal reaches the still-owned launcher PID.
					[ "$launch_case" != transition ] || group_live=1
				fi
				return 0
			}
			zxfer_signal_background_shell 12345 pgid KILL
			printf 'status=%s launcher=%s group=%s\n' "$?" "$launcher_live" "$group_live"
		)
		case "$launch_case" in
		denied_launcher | denied_group)
			assertContains "$launch_case must preserve failed teardown for retry." "$output" 'status=1'
			;;
		*)
			assertContains "$launch_case must leave no owned launch or late-created group alive." \
				"$output" 'status=0 launcher=0 group=0'
			;;
		esac
		if [ "$launch_case" = launcher ] || [ "$launch_case" = transition ]; then
			assertContains "The live launcher must receive KILL before its group is established." \
				"$output" 'launcher=KILL'
		fi
	done
}

test_signal_background_shell_never_signals_a_recycled_pid_outside_its_own_group() {
	output=$(
		# No group exists for the PID, which now belongs to a live, unrelated
		# process in another process group.
		ps() {
			if [ "$4" = 12345 ]; then
				printf ' 9999\n'
			else
				printf ' 777\n'
			fi
		}
		kill() {
			printf 'kill %s\n' "$*"
			[ "$*" = "-s 0 12345" ]
		}
		zxfer_signal_background_shell 12345 pgid KILL
		printf 'status=%s\n' "$?"
	)

	assertContains "A recycled PID outside zxfer's group means the job is gone." \
		"$output" "status=0"
	assertNotContains "The recycled PID itself is never signalled." \
		"$output" "kill -s KILL 12345"
}

test_execute_background_cmd_respects_dry_run_mode() {
	temp_file="$TEST_TMPDIR/bg_dry_run_output"
	err_file="$TEST_TMPDIR/bg_dry_run_error"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			zxfer_execute_rendered_background_shell_command "printf bg-data" "$temp_file" "$err_file"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Dry-run background execution should render the skipped command." \
		"$output" "Dry run: printf bg-data"
	assertContains "Dry-run background execution should leave the background PID unset." \
		"$output" "pid="
	assertTrue "Dry-run background execution should still create the placeholder output file." \
		"[ -f \"$temp_file\" ]"
	assertTrue "Dry-run background execution should still create the placeholder error file." \
		"[ -f \"$err_file\" ]"
	assertEquals "Dry-run background execution should leave the placeholder output empty." \
		"" "$(cat "$temp_file")"
	assertEquals "Dry-run background execution should leave the placeholder error file empty." \
		"" "$(cat "$err_file")"
}

test_execute_background_cmd_dry_run_fails_closed_when_placeholder_creation_fails() {
	temp_dir="$TEST_TMPDIR/bg_dry_run_output_dir"
	err_dir="$TEST_TMPDIR/bg_dry_run_error_dir"
	mkdir -p "$temp_dir" "$err_dir"

	zxfer_test_capture_subshell '
		g_option_n_dryrun=1
		zxfer_execute_rendered_background_shell_command "printf bg-data" "'"$temp_dir"'" "'"$err_dir"'"
	'

	assertEquals "Dry-run background execution should return failure when placeholder file creation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_execute_background_cmd_dry_run_clears_stale_pid_and_partial_output_on_second_placeholder_failure() {
	zxfer_get_temp_file >/dev/null
	output_file=$g_zxfer_temp_file_result
	err_dir="$TEST_TMPDIR/bg_dry_run_partial_error_dir"
	mkdir -p "$err_dir"

	output=$(
		(
			g_option_n_dryrun=1
			g_last_background_pid=43210
			zxfer_execute_rendered_background_shell_command "printf bg-data" "$output_file" "$err_dir" 2>/dev/null
			printf 'status=%s\n' "$?"
			printf 'pid=<%s>\n' "${g_last_background_pid:-}"
			if [ -e "$output_file" ]; then
				printf 'output_exists=yes\n'
			else
				printf 'output_exists=no\n'
			fi
		)
	)

	assertContains "Dry-run placeholder failures should preserve the write failure status." \
		"$output" "status=1"
	assertContains "Dry-run placeholder failures should clear any stale background PID state." \
		"$output" "pid=<>"
	assertContains "Dry-run placeholder failures should not leave a partially published output placeholder behind." \
		"$output" "output_exists=no"
}

test_execute_command_records_last_command_string() {
	g_option_n_dryrun=1
	g_zxfer_failure_last_command=""

	zxfer_execute_rendered_shell_command "printf 'hello'"

	assertEquals "zxfer_execute_rendered_shell_command should redact the exact command string for failure reports by default." "[redacted]" "$g_zxfer_failure_last_command"
}
