#!/bin/sh
# Background-shell tests for src/zxfer_exec.sh: the spawn modes, process-group
# isolation and signalling of job shells. Run by tests/test_zxfer_exec.sh under
# the send-job fixture.
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_spawn_mode_rejects_setsid_that_does_not_run_the_probe_child() {
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
