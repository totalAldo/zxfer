#!/bin/sh
#
# shunit2 tests for src/zxfer_exec.sh: command execution, dry runs, background
# commands and background-shell spawn modes.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"
# shellcheck source=tests/helpers/send_job_fixtures.sh
. "$TESTS_DIR/helpers/send_job_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_shunit"
	TEST_ORIGINAL_PATH=$PATH
	zxfer_test_exec_fixture_one_time_setup
}

oneTimeTearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_exec_fixture_setup
}

tearDown() {
	relax_test_tmpdir_permissions
}

test_exec_direct_load_does_not_require_transport_capability_or_snapshot_state() {
	/bin/sh -c '
		ZXFER_SOURCE_MODULES_ROOT=$1
		. "$1/src/zxfer_modules.sh"
		zxfer_load_modules zxfer_exec.sh || exit 1
		command -v zxfer_render_shell_command_from_argv >/dev/null 2>&1 || exit 2
		command -v zxfer_invoke_ssh_shell_command_for_host >/dev/null 2>&1 && exit 3
		command -v zxfer_ensure_remote_host_capabilities >/dev/null 2>&1 && exit 4
		command -v zxfer_reset_destination_existence_cache >/dev/null 2>&1 && exit 5
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

test_spawn_mode_rejects_setsid_that_does_not_run_the_probe_child() {
	zxfer_test_send_job_fixture_setup
	probe_bin="$TEST_TMPDIR/empty-setsid-bin"
	mkdir -p "$probe_bin"
	printf '#!/bin/sh\nexit 0\n' >"$probe_bin/setsid"
	chmod +x "$probe_bin/setsid"
	mode=$(
		PATH="$probe_bin:$PATH"
		zxfer_reset_background_shell_spawn_mode
		zxfer_init_background_shell_spawn_mode
		printf '%s\n' "$g_zxfer_background_shell_spawn_mode"
	)
	assertNotEquals "A successful launcher status alone does not prove PID/group ownership." setsid "$mode"
}

# The mode is probed once per process but also used from subshells, where
# FreeBSD sh, dash and ksh93 ignore set -m. A pgid scope must name a real
# process group there too.
test_spawn_mode_isolation_holds_inside_subshells() {
	zxfer_test_send_job_fixture_setup
	zxfer_init_background_shell_spawn_mode
	started="$TEST_TMPDIR/spawn-isolation-started"
	rm -f "$started"
	isolation=$(
		zxfer_spawn_background_shell ': >"$1"; sleep 30' /dev/null /dev/null "$started"
		pid=$g_last_background_pid
		scope=$g_zxfer_background_shell_scope
		# setsid(1) creates the group just before it execs the job shell, so
		# check for the group only once the job shell has run.
		tries=0
		while [ ! -e "$started" ] && [ "$tries" -lt 20 ]; do
			sleep 1
			tries=$((tries + 1))
		done
		if [ "$scope" = wrapper ]; then
			printf 'wrapper\n'
		elif zxfer_signal_process_group 0 "$pid"; then
			printf 'group\n'
		else
			printf 'none\n'
		fi
		zxfer_signal_background_shell "$pid" "$scope" KILL ||
			kill -s KILL "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	)

	assertNotEquals "A subshell spawn must get the isolation its recorded scope claims." \
		none "$isolation"
}

# A process group outlives its leader, and the pgid probes must see that
# under every test shell. BusyBox ash's kill took the `--` of
# `kill -s 0 -- -PGID` for a PID and exited 1 even for a live group, so a
# group whose leader was already reaped was never registered for teardown.
test_group_outliving_its_reaped_leader_is_registered_and_stopped() {
	zxfer_test_send_job_fixture_setup
	zxfer_init_background_shell_spawn_mode
	if [ "$g_zxfer_background_shell_spawn_mode" = wrapper ]; then
		startSkipping
		return
	fi
	member_file="$TEST_TMPDIR/orphaned-group-member.pid"
	rm -f "$member_file"
	# The job shell leaves a member in its group and exits at once.
	zxfer_spawn_background_shell 'sleep 30 & printf "%s\n" "$!" >"$1"' \
		/dev/null /dev/null "$member_file"
	group_pid=$g_last_background_pid
	wait "$group_pid"
	member_pid=$(cat "$member_file" 2>/dev/null)
	zxfer_register_cleanup_pid "$group_pid" "orphaned group fixture" pgid
	registered=0
	zxfer_find_cleanup_pid_record "$group_pid" && registered=1
	zxfer_signal_background_shell "$group_pid" pgid KILL
	signal_status=$?
	bgjob_test_wait_for_pid_exit "$member_pid"
	member_status=$?
	zxfer_unregister_cleanup_pid "$group_pid"

	assertNotNull "The job shell recorded the member it left in its group." \
		"$member_pid"
	assertEquals "A live group stays registered after its leader was reaped." \
		1 "$registered"
	assertEquals "The leaderless group is signalled." 0 "$signal_status"
	assertEquals "The member left in the group is stopped." 0 "$member_status"
}

# Job shells are /bin/sh, so a secure PATH without sh (as the integration
# harness narrows ZXFER_SECURE_PATH) still starts them in every spawn mode.
test_spawn_background_shell_runs_bin_sh_without_sh_on_path() {
	zxfer_test_send_job_fixture_setup
	no_sh_path="$TEST_TMPDIR/path-without-sh"
	mkdir -p "$no_sh_path"
	# setsid mode still runs setsid from PATH.
	case $(command -v setsid 2>/dev/null) in
	/*) ln -sf "$(command -v setsid)" "$no_sh_path/setsid" ;;
	esac
	zxfer_init_background_shell_spawn_mode
	full_path_mode=$g_zxfer_background_shell_spawn_mode
	# Production probes lazily, after PATH is already the secure PATH.
	probed_mode=$(
		PATH=$no_sh_path
		zxfer_reset_background_shell_spawn_mode
		zxfer_init_background_shell_spawn_mode
		printf '%s\n' "$g_zxfer_background_shell_spawn_mode"
	)
	assertEquals "The spawn-mode probe does not need sh on PATH." \
		"$full_path_mode" "$probed_mode"
	for spawn_mode in "$probed_mode" wrapper; do
		out_file="$TEST_TMPDIR/spawn-without-sh.$spawn_mode.out"
		rm -f "$out_file"
		status=$(
			g_zxfer_background_shell_spawn_mode=$spawn_mode
			PATH=$no_sh_path
			# The ARG becomes the job shell's $1.
			zxfer_spawn_background_shell 'printf "%s\n" "$1"' "$out_file" "" ok
			wait "$g_last_background_pid"
			printf '%s\n' "$?"
		)
		assertEquals "A $spawn_mode job shell starts without sh on PATH." 0 "$status"
		assertEquals "A $spawn_mode job shell gets its arguments." ok "$(cat "$out_file" 2>/dev/null)"
	done
}

test_signal_background_shell_reports_success_for_exited_children() {
	zxfer_test_send_job_fixture_setup
	sh -c 'exit 0' &
	exited_pid=$!
	wait "$exited_pid"

	assertTrue "Signalling an exited pid scope is not a failure." \
		"zxfer_signal_background_shell '$exited_pid' pid TERM"
	assertTrue "Signalling an exited pgid scope is not a failure." \
		"zxfer_signal_background_shell '$exited_pid' pgid TERM"
	assertTrue "Non-numeric pids are ignored." \
		"zxfer_signal_background_shell 'not-a-pid' pid TERM"
}

test_spawn_background_shell_uses_the_fixed_wrapper_path_and_fails_closed_without_it() {
	zxfer_test_send_job_fixture_setup
	# An exited child: were the wrapper check skipped, teardown would find
	# nothing to stop and return 0.
	sh -c 'exit 0' &
	exited_pid=$!
	wait "$exited_pid"
	output=$(
		(
			g_zxfer_background_shell_spawn_mode=wrapper
			printf 'wrapper=%s\n' "${ZXFER_CLEANUP_CHILD_WRAPPER#"$ZXFER_ROOT"/}"
			ZXFER_CLEANUP_CHILD_WRAPPER="$TEST_TMPDIR/no-such-wrapper.sh"
			zxfer_spawn_background_shell 'exit 0'
			printf 'missing=%s pid=<%s>\n' "$?" "$g_last_background_pid"
			zxfer_kill_background_wrapper "$exited_pid"
			printf 'kill_missing=%s\n' "$?"
		)
	)

	assertEquals "The wrapper path is fixed beside the modules; without a readable wrapper neither spawning nor wrapper teardown runs." \
		"wrapper=src/zxfer_cleanup_child_wrapper.sh
missing=1 pid=<>
kill_missing=1" "$output"
}

test_signal_background_shell_uses_c_locale_and_trims_pgid_whitespace() {
	l_ps_dir="$TEST_TMPDIR/pgid-locale-ps"
	l_ps_log="$TEST_TMPDIR/pgid-locale-ps.log"
	mkdir -p "$l_ps_dir"
	: >"$l_ps_log"
	cat >"$l_ps_dir/ps" <<EOF
#!/bin/sh
printf 'LC_ALL=%s args=%s\n' "\${LC_ALL:-unset}" "\$*" >>"$l_ps_log"
printf '\t  777 \t \n'
EOF
	chmod +x "$l_ps_dir/ps"

	l_out=$(
		PATH="$l_ps_dir:$PATH"
		LC_ALL=fr_FR
		export LC_ALL
		l_launcher_live=1
		kill() {
			if [ "$1" != -s ]; then
				return 1
			fi
			[ "$2" != 0 ] || return "$((1 - l_launcher_live))"
			printf 'launcher=%s\n' "$2"
			l_launcher_live=0
			return 0
		}
		zxfer_signal_background_shell 12345 pgid KILL
		printf 'status=%s\n' "$?"
	)

	assertEquals "PGID comparison must trim leading/trailing spaces and tabs and signal the matching launcher." \
		"launcher=KILL
status=0" "$l_out"
	assertEquals "Both ps -o pgid= probes must run under LC_ALL=C." \
		2 "$(grep -c '^LC_ALL=C args=-o pgid= -p ' "$l_ps_log")"
}

test_spawn_background_shell_preserves_preexisting_monitor_mode() {
	zxfer_test_send_job_fixture_setup
	if ! (set -m 2>/dev/null); then
		startSkipping
		assertTrue "Shell does not allow set -m in a subshell; skipped." true
		endSkipping
		return 0
	fi
	l_out=$(
		(
			g_zxfer_background_shell_spawn_mode=monitor
			set +m
			zxfer_spawn_background_shell 'exit 0' /dev/null /dev/null
			wait "$g_last_background_pid" 2>/dev/null || :
			if [ "${-#*m}" != "$-" ]; then
				l_after_off=on
			else
				l_after_off=off
			fi

			set -m
			zxfer_spawn_background_shell 'exit 0' /dev/null /dev/null
			wait "$g_last_background_pid" 2>/dev/null || :
			if [ "${-#*m}" != "$-" ]; then
				l_after_on=on
			else
				l_after_on=off
			fi
			set +m
			printf 'from_off=%s from_on=%s\n' "$l_after_off" "$l_after_on"
		) 2>/dev/null
	)

	assertEquals "Monitor-mode spawning must only turn -m off if it enabled -m itself." \
		"from_off=off from_on=on" "$l_out"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
