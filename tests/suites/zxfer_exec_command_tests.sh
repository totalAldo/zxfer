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
		command -v zxfer_reset_live_destination_listing_state >/dev/null 2>&1 && exit 5
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

test_execute_command_records_last_command_string() {
	g_option_n_dryrun=1
	g_zxfer_failure_last_command=""

	zxfer_execute_rendered_shell_command "printf 'hello'"

	assertEquals "zxfer_execute_rendered_shell_command should redact the exact command string for failure reports by default." "[redacted]" "$g_zxfer_failure_last_command"
}
