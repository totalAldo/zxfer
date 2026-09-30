#!/bin/sh
#
# Black-box -O / -T remote transport suite for ./zxfer.
#
# Drives the real launcher against the canned zfs and the socket-aware mock
# ssh from tests/helpers/blackbox.sh and asserts on the zfs and ssh argv
# logs: master opens and closes, listings over the target master, wrapper
# host specs, the ssh policy, capability probes and their failures.
#
# The invariant each test pins:
#
#   -T destination discovery (ordinary listings over the target master)
#       test_remote_target_destination_listing_failure_fails_closed
#       → a failed snapshot listing on the -T host keeps the zfs exit status
#         and the local "Failed to retrieve snapshot list" report.
#       test_remote_target_ssh_failure_during_discovery_fails_closed
#       → an ssh failure of that listing exits 255 with a report.
#       test_remote_target_bootstraps_a_missing_destination_root
#       → a missing -T root whose pool the live probe lists is bootstrapped.
#       test_remote_target_missing_root_with_an_unlistable_pool_fails_closed
#       → the same root with an unlistable pool fails closed.
#
#   -T remote destination, -P property pass (role-routing fix, 2026-09)
#       test_remote_target_property_pass_reads_destination_properties_over_ssh
#       → every destination-side `zfs get` crosses the ssh transport and no
#         source-side `zfs get` does, even though zfs resolves to the same
#         path on both "hosts"; zero MUTATE / send / receive argv.
#
#   -O / -T ssh transport (pinned 2026-09)
#       test_remote_wrapper_host_specs_wrap_every_remote_command_of_their_role
#       → wrapper tokens after the host run every remote command of their
#         role, pipelines through `sh -c`; masters and closes get the whole
#         spec; -V labels and profiles each command on its role's side.
#       test_remote_shared_host_very_verbose_labels_and_profiles_each_command_by_its_role
#       → equal -O and -T specs: one origin/target master, and each command
#         labeled and profiled by the role it serves.
#       test_remote_host_spec_needing_shell_syntax_is_a_usage_error_before_any_ssh
#       → quotes or a backslash in a host spec exit 2 before ssh or zfs.
#       test_remote_ssh_policy_environment_shapes_every_ssh_argv
#       → ambient config drops the managed -o options; the managed policy
#         leads every master, command and close argv.
#       test_remote_hosts_ignore_inherited_transport_and_capability_state
#       → exported transport and capability globals never reach ssh.
#       test_remote_noop_under_a_short_tmpdir_keeps_its_socket_in_the_run_root
#       → a short TMPDIR keeps the socket in the run root; TMPDIR ends empty.
#       test_remote_master_close_at_exit_treats_a_gone_master_as_closed
#       → a stale master at exit is closed; another close failure fails the
#         run; the other role is closed either way.
#       test_remote_origin_master_failure_still_closes_the_target_master
#       → a failed origin master fails closed; the target master is closed.
#       test_remote_failed_capability_preload_is_probed_again
#       → a failed startup probe caches nothing; -v alone shows why.
#       test_remote_capability_answers_decide_between_direct_probes_and_dependency_errors
#       → a malformed answer falls back to direct probes; a missing or
#         unqueryable zfs stops the run with a dependency report.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# Invariant: -V must not change replication outcomes, and an ssh without
# control-socket support still replicates over direct connections, -T
# destination discovery included. Regression for -T destination discovery
# aborting under -V because a profiling recorder's non-zero status leaked into
# a discovery function's return value (zxfer_profile_record_zfs_call returned
# 1 for destination-side calls). The minimal mock ssh rejects -M.
test_remote_target_discovery_succeeds_with_very_verbose() {
	planning_setup_env
	zxfer_mockbin_write_minimal_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write minimal mock ssh."
	SSH_LOG="$CASE_DIR/ssh.log"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -V -O localhost -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_remote_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-V remote no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_noop_status"
	assertTrue "the -T destination snapshot listing must have run over ssh" \
		"grep -q \"'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'\" '$SSH_LOG'"
	for l_remote_noop_role in origin target; do
		assertContains "-V must explain the direct-connection fallback for the $l_remote_noop_role host" \
			"$(cat "$CASE_DIR/zxfer.stderr")" \
			"ssh client does not support control sockets; continuing without connection reuse for $l_remote_noop_role host."
	done
	assertFalse "without control-socket support no command may name a socket" \
		"grep -q -- '-S ' '$SSH_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: a clean remote-origin pull no-op opens the origin's ssh control
# master before its first remote command, runs the capability probe and the
# source listing over that socket, probes exactly ONCE, and closes the master
# once at exit.
test_remote_origin_pull_noop_opens_master_first_and_probes_once() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_pull_noop.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_pull_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O pull no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_pull_noop_status"
	planning_assert_ssh_commands_multiplexed 1
	assertTrue "the origin master must use the origin role socket" \
		"grep -q -- '-M -S [^ ]*/ssh-origin.sock -fN localhost' '$SSH_LOG'"
	assertEquals "a warmed origin host must cost exactly one capability probe round trip" \
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertTrue "the source listing must run over the origin master" \
		"grep -q -- 'ssh-origin.sock localhost .*snapshot' '$SSH_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: a clean -T push no-op opens the target's master before its first
# remote command, runs the capability probe and the destination snapshot
# listing over it, lists no destination dataset inventory (nothing on a no-op
# reads it), and closes the master once at exit.
test_remote_target_push_noop_opens_master_first_and_probes_once() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_push_noop.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_push_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-T push no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_push_noop_status"
	planning_assert_ssh_commands_multiplexed 1
	assertTrue "the target master must use the target role socket" \
		"grep -q -- '-M -S [^ ]*/ssh-target.sock -fN localhost' '$SSH_LOG'"
	assertEquals "the target host must cost exactly one capability probe round trip" \
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertEquals "the destination snapshot listing must run once over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"
	assertEquals "a no-op must not list the destination dataset inventory" \
		0 "$(grep -c -- "'filesystem,volume'" "$SSH_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: with distinct -O and -T host specs each role opens its own master
# before any remote command and sends every command over its own socket; each
# master closes once at exit. A -T spec equal to the -O spec shares the origin
# master, since commands for that spec already use the origin socket, and one
# capability probe, since both roles ask that host the same questions.
test_remote_origin_and_target_noop_open_one_master_per_host_spec() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_both_noop.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -T 127.0.0.1 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_both_noop_status=$?

	assertEquals "-O -T no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_both_noop_status"
	planning_assert_ssh_commands_multiplexed 2
	assertEquals "each host must cost exactly one capability probe round trip" \
		2 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertFalse "origin commands must never use the target socket" \
		"grep -q -- 'ssh-target.sock localhost' '$SSH_LOG'"
	assertFalse "target commands must never use the origin socket" \
		"grep -q -- 'ssh-origin.sock 127.0.0.1' '$SSH_LOG'"
	assertEquals "the destination snapshot listing must run once over the target master" \
		1 "$(grep -c -- "ssh-target.sock 127.0.0.1 .*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"

	: >"$SSH_LOG"
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_same_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O -T no-op to one host spec must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_same_noop_status"
	planning_assert_ssh_commands_multiplexed 1
	assertFalse "one host spec must not open a second master" \
		"grep -q -- 'ssh-target.sock' '$SSH_LOG'"
	assertEquals "one host spec must cost exactly one capability probe round trip" \
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: when a long TMPDIR would push the control-socket path past the
# sun_path limit, the master's socket lives in a short private directory
# under the default temp root instead: the socket path, with the suffix ssh
# adds to its temporary listener, stays under 104 bytes, and the run leaves
# neither that directory nor its run root behind. Root and other users alike
# accept the 0700 TMPDIR they made and a root-owned sticky default root.
test_remote_noop_under_a_long_tmpdir_removes_its_short_socket_directory() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_long_tmpdir.log"
	: >"$SSH_LOG"
	l_long_component="zxfer-long-tmpdir-component-00000000000000000000000000000"
	l_long_tmpdir="$CASE_DIR/$l_long_component/$l_long_component"
	mkdir -p "$l_long_tmpdir" || fail "Unable to create the long TMPDIR."
	chmod 700 "$l_long_tmpdir"

	# Export in a subshell: FreeBSD sh exports a prefix assignment on a
	# function call only when the name was already exported, and ksh93 not
	# even then, so zxfer would miss this TMPDIR and keep its sockets in a
	# short run root under the default temp root.
	(
		TMPDIR=$l_long_tmpdir
		PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		MOCK_SSH_LOG=$SSH_LOG
		export TMPDIR PATH MOCK_SSH_LOG
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	l_long_tmpdir_status=$?
	l_long_socket=$(awk '$1 == "-M" || index($0, " -M -S ") {
		for (i = 1; i < NF; i++) if ($i == "-S") { print $(i + 1); exit }
	}' "$SSH_LOG")
	l_long_socket_dir=${l_long_socket%/*}
	# ssh binds SOCKET plus a dot and 16 random characters, then renames it.
	l_long_socket_listener="$l_long_socket.0123456789abcdef"
	# The default temp roots zxfer may pick, by physical path (/tmp is
	# /private/tmp on macOS).
	l_long_socket_parent_is_default=no
	for l_long_default_root in /dev/shm /run/shm /tmp; do
		[ -d "$l_long_default_root" ] || continue
		[ "$(cd -P "$l_long_default_root" && pwd)" != "${l_long_socket_dir%/*}" ] ||
			l_long_socket_parent_is_default=yes
	done

	assertEquals "-O no-op under a long TMPDIR must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_long_tmpdir_status"
	planning_assert_ssh_commands_multiplexed 1
	assertNotNull "the origin master must name its control socket" \
		"$l_long_socket"
	assertTrue "the socket's temporary listener path must fit sun_path: $l_long_socket_listener" \
		"[ ${#l_long_socket_listener} -lt 104 ]"
	assertNotContains "the origin socket must not sit under the long TMPDIR" \
		"$l_long_socket_dir" "$l_long_component"
	assertContains "the origin socket should sit in a short zxfer.ssh directory" \
		"${l_long_socket_dir##*/}" "zxfer.ssh."
	assertEquals "the short socket directory must sit directly under a default temp root: $l_long_socket_dir" \
		yes "$l_long_socket_parent_is_default"
	assertFalse "the short socket directory must be gone after the run" \
		"[ -e '$l_long_socket_dir' ]"
	assertEquals "the run root under the long TMPDIR must be gone too" \
		"" "$(ls -A "$l_long_tmpdir")"
	planning_assert_no_mutations
}

# Purpose: Run a -T localhost push of STATE_DIR through the fault-injecting
# socket-aware mock ssh, logging ssh calls to $CASE_DIR/ssh.log (SSH_LOG).
# Usage: planning_run_remote_target_push; sets PLANNING_RUN_STATUS. Export
# any MOCK_FAIL_* variables first: a prefix assignment on a function call is
# not exported on FreeBSD sh.
planning_run_remote_target_push() {
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh.log"
	: >"$SSH_LOG"
	MOCK_SSH_LOG=$SSH_LOG
	export MOCK_SSH_LOG
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	PLANNING_RUN_STATUS=$?
	unset MOCK_SSH_LOG
}

# Invariant (-T discovery): a failed destination snapshot listing on the -T
# host fails closed like a local one: the zfs exit status, the snapshot
# discovery stage report, and zero mutating or send/receive argv.
test_remote_target_destination_listing_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" remote_dstsnapfail
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2

	planning_run_remote_target_push
	assertEquals "a failed -T destination snapshot listing must keep the zfs exit status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		2 "$PLANNING_RUN_STATUS"
	assertEquals "the listing must have run over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshot list from the destination."
}

# Invariant (-T discovery): when the ssh call carrying the destination
# snapshot listing fails, the run stops with ssh's exit status 255 and ssh's
# diagnostic, sends no existence probe over that connection (a lost one would
# turn the status into the probe's 1), and changes nothing.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the ssh argv glob
test_remote_target_ssh_failure_during_discovery_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" remote_sshfail
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	MOCK_FAIL_TOOL=ssh
	MOCK_FAIL_CALL=1
	MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
	MOCK_FAIL_MATCH="*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'"
	export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH

	planning_run_remote_target_push
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH
	assertEquals "an ssh failure during -T discovery must exit with ssh's status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		255 "$PLANNING_RUN_STATUS"
	assertEquals "exactly the listing's ssh call must have failed" \
		1 "$(grep -c '^fail	' "$SSH_LOG")"
	assertEquals "no existence probe may follow an undelivered listing" \
		0 "$(grep -c -- "'list' '-H' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"
	assertContains "ssh's diagnostic must reach stderr" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "Connection to localhost closed by remote host."
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshot list from the destination."
}

# Invariant (-T discovery): a missing -T destination root is bootstrapped
# only after the live pool probe, run over the target master, lists its pool;
# every dataset is then received. The dataset inventory that reported the
# root missing ran over the target master too, never locally.
test_remote_target_bootstraps_a_missing_destination_root() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" remote_missing_root
	l_missing_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	printf '%s\n' "$l_missing_pool" >"$STATE_DIR/dst_pool.list"
	printf 'list -H -o name %s\tdst_pool.list\t0\n' "$l_missing_pool" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the pool rule."
	planning_make_remote_destination_root_missing

	planning_run_remote_target_push
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
	assertEquals "a missing -T root whose pool exists must be bootstrapped; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$PLANNING_RUN_STATUS"
	assertEquals "the pool probe must run once, over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-H' '-o' 'name' '$l_missing_pool'" "$SSH_LOG")"
	assertEquals "the dataset inventory must run once, over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-t' 'filesystem,volume' '-Hr' '-o' 'name' '$ZXFER_MOCKBIN_DEST_ROOT'" "$SSH_LOG")"
	for l_missing_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_missing_suffix"
	done
	planning_assert_no_mutations
	assertNotContains "a bootstrap must not report a failure" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "zxfer: failure report begin"
}

# Invariant (-T discovery): a missing -T destination root whose pool cannot be
# listed fails closed in discovery, before any send or receive.
test_remote_target_missing_root_with_an_unlistable_pool_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" remote_missing_pool
	l_missing_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	printf 'list -H -o name %s\t-\t2\n' "$l_missing_pool" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the pool rule."
	planning_make_remote_destination_root_missing

	planning_run_remote_target_push
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
	assertEquals "an unlistable -T pool must keep the pool probe's status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		2 "$PLANNING_RUN_STATUS"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Destination dataset [$ZXFER_MOCKBIN_DEST_ROOT] is missing and destination pool [$l_missing_pool] could not be listed"
}

# Invariant: when the origin's control master cannot be opened the run fails
# closed before any other remote command or zfs call, with ssh's diagnostic
# and the structured socket error, and leaves no master to close.
test_remote_master_open_failure_fails_closed_before_any_remote_command() {
	planning_setup_env
	SSH_LOG="$CASE_DIR/ssh_master_failure.log"
	: >"$SSH_LOG"
	cat >"$MOCKBIN_DIR/ssh" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
for l_arg in "$@"; do
	[ "$l_arg" = -S ] || continue
	printf '%s\n' 'ssh: connect to host localhost port 22: Connection refused' >&2
	exit 255
done
exit 0
EOF
	chmod +x "$MOCKBIN_DIR/ssh"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/incremental" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_master_failure_status=$?
	unset MOCK_SSH_LOG

	assertEquals "a failed master open must fail the run" 1 "$l_master_failure_status"
	planning_assert_failure_report "cli validation" \
		"Error creating ssh control socket for origin host."
	assertContains "ssh's own diagnostic must reach the operator" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "Connection refused"
	assertEquals "only the support probe and the master open may reach ssh" \
		"-M -V
-o BatchMode=yes -o StrictHostKeyChecking=yes -M -S" \
		"$(sed 's/ -M -S .*/ -M -S/' "$SSH_LOG")"
	assertFalse "no zfs command may run" "[ -s '$ZFS_LOG' ]"
}

# Invariant: an invalid ZXFER_SSH_* policy fails the run at startup with the
# policy diagnostic and a structured report even without -V, before any ssh
# connection or zfs call.
test_remote_invalid_ssh_policy_fails_at_startup_without_very_verbose() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_invalid_policy.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"
	export ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative_known_hosts

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_invalid_policy_status=$?
	unset MOCK_SSH_LOG ZXFER_SSH_USER_KNOWN_HOSTS_FILE

	assertEquals "an invalid ssh policy must fail the run" 1 "$l_invalid_policy_status"
	planning_assert_failure_report "cli validation" \
		"ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
	assertEquals "only the control-socket support probe may reach ssh" \
		"-M -V" "$(cat "$SSH_LOG")"
	assertFalse "no zfs command may run" "[ -s '$ZFS_LOG' ]"
}

# Invariant: an incremental remote-origin pull opens the per-run ssh control
# master exactly ONCE, multiplexes later remote commands over that one
# socket, probes capabilities exactly once, and closes the master once at
# exit -- no per-command reconnect or per-command handshake regression.
test_remote_origin_pull_incremental_opens_master_once() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_pull_incr.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/incremental" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_pull_incr_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O pull incremental must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_pull_incr_status"
	for l_pull_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_pull_suffix"
	done
	assertEquals "an incremental pull must open the ssh control master exactly once" \
		1 "$(grep -c -- ' -M ' "$SSH_LOG")"
	assertEquals "a warmed origin host must cost exactly one capability probe round trip" \
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertEquals "the per-run ssh control master must be closed exactly once at exit" \
		1 "$(grep -c -- ' -O exit ' "$SSH_LOG")"
	l_master_socket=$(awk '/ -M /{for (i=1;i<NF;i++) if ($i=="-S") {print $(i+1); exit}}' "$SSH_LOG")
	assertNotNull "the master open must carry a -S control socket path" "$l_master_socket"
	l_multiplexed=$(grep -c -- "-S $l_master_socket" "$SSH_LOG")
	assertTrue "remote send commands must multiplex over the one opened master socket" \
		"[ ${l_multiplexed:-0} -ge 2 ]"
	planning_assert_no_mutations
}

# Invariant: with a remote destination (-T) every destination-side property
# read of the -P pass crosses the ssh transport and no source-side read does.
# Regression for the zfs command dispatcher routing by comparing the
# requested binary path against the source path first: with zfs installed at
# the same path on both hosts (as here, where both "hosts" resolve the one
# canned zfs) every destination `zfs get` silently ran against the LOCAL
# pool, so the property diff compared the source with itself.
test_remote_target_property_pass_reads_destination_properties_over_ssh() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	planning_clone_state "$FIXTURE_DIR/noop" remote_props
	planning_add_property_transfer_fixtures
	SSH_LOG="$CASE_DIR/ssh_remote_props.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -T localhost -P -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_remote_props_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-T -P no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_props_status"
	planning_assert_property_reads_routed_by_side
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# ---------------------------------------------------------------------------
# -O / -T ssh transport: host specs, ssh policy, masters and probes.

# Purpose: Run ./zxfer against a fixture state dir with the mock PATH and
# MOCK_SSH_LOG=$SSH_LOG exported in a subshell (a prefix assignment on a
# function call is not exported on FreeBSD sh).
# Usage: planning_run_remote_zxfer <state-dir> [zxfer-arg...]; returns
# zxfer's status.
planning_run_remote_zxfer() {
	(
		MOCK_SSH_LOG=$SSH_LOG
		PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		export MOCK_SSH_LOG PATH
		planning_run_zxfer "$@"
	)
}

# Purpose: Write a doas stand-in into MOCKBIN_DIR that appends its argv to
# DOAS_LOG and runs it, as doas runs a permitted command.
# Usage: planning_write_mock_doas; sets DOAS_LOG.
planning_write_mock_doas() {
	DOAS_LOG="$CASE_DIR/doas.log"
	: >"$DOAS_LOG"
	cat >"$MOCKBIN_DIR/doas" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$DOAS_LOG"
exec "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/doas"
}

# Purpose: Print how many ssh commands in SSH_LOG ran a direct probe, the
# per-question fallback for a host whose capability answer was unusable: the
# uname probe or a `command -v 'TOOL'` probe. `d` chunk boundaries are
# removed first, as planning_count_remote_script_marker does.
# Usage: planning_count_direct_remote_probes
planning_count_direct_remote_probes() {
	sed "s/' 'd//g" "$SSH_LOG" |
		grep -c -F -e 'export PATH; uname 2>/dev/null' -e "command -v '\\''" || :
}

# Purpose: Print the value of one -V profile counter from zxfer's stderr.
# Usage: planning_profile_counter <key>
planning_profile_counter() {
	sed -n "s/^zxfer profile: $1=//p" "$CASE_DIR/zxfer.stderr"
}

# Purpose: Fail unless every ssh argv in SSH_LOG but the control-socket
# support probe starts with the given ssh options.
# Usage: planning_assert_every_ssh_argv_leads_with <options>
planning_assert_every_ssh_argv_leads_with() {
	l_lead_total=$(grep -c -v -x -- '-M -V' "$SSH_LOG")
	l_lead_matching=$(awk -v lead="$1 -" 'index($0, lead) == 1' "$SSH_LOG" | awk 'END { print NR }')
	assertEquals "every ssh argv but the support probe must lead with: $1; ssh log: $(cat "$SSH_LOG")" \
		"$l_lead_total" "$l_lead_matching"
	assertNotEquals "zxfer must have run ssh" 0 "$l_lead_total"
}

# Invariant (wrapper host specs): the tokens after the host in -O/-T, such as
# "doas" or "env NAME=VALUE", run every remote command of that role, each
# token quoted apart: the capability probe, listings, and every send or
# receive pipeline, which crosses the wrapper whole through `sh -c`. The
# master open and its close hand ssh the whole host spec. Every answer comes
# from the one capability probe per host, and -V labels each role's commands
# with its host spec and profiles each once, on that role's side.
test_remote_wrapper_host_specs_wrap_every_remote_command_of_their_role() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	planning_write_mock_doas
	SSH_LOG="$CASE_DIR/ssh_wrapper.log"
	: >"$SSH_LOG"
	l_target_spec="127.0.0.1 env ZXFER_WRAPPED=1"

	planning_run_remote_zxfer "$FIXTURE_DIR/incremental" -V -O "localhost doas" \
		-T "$l_target_spec" -R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_wrapper_status=$?
	l_origin_commands=$(grep -c -F -- 'ssh-origin.sock localhost ' "$SSH_LOG")
	l_target_commands=$(grep -c -F -- 'ssh-target.sock 127.0.0.1 ' "$SSH_LOG")
	l_stderr=$(cat "$CASE_DIR/zxfer.stderr")

	assertEquals "wrapped -O -T incremental must exit 0; stderr: $l_stderr" \
		0 "$l_wrapper_status"
	for l_wrapper_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_wrapper_suffix"
	done
	planning_assert_ssh_commands_multiplexed 2
	assertEquals "the origin master open and its close must pass the whole host spec to ssh" \
		2 "$(grep -c -- '-S [^ ]*/ssh-origin.sock .* localhost doas$' "$SSH_LOG")"
	assertEquals "the target master open and its close must pass the whole host spec to ssh" \
		2 "$(grep -c -- '-S [^ ]*/ssh-target.sock .* 127.0.0.1 env ZXFER_WRAPPED=1$' "$SSH_LOG")"
	assertTrue "each role must have run remote commands" \
		"[ $l_origin_commands -gt 0 ] && [ $l_target_commands -gt 0 ]"
	assertEquals "every origin command must run under the quoted wrapper; ssh log: $(cat "$SSH_LOG")" \
		"$l_origin_commands" "$(grep -c -F -- "ssh-origin.sock localhost 'doas' " "$SSH_LOG")"
	assertEquals "every target command must run under the quoted wrapper tokens" \
		"$l_target_commands" \
		"$(grep -c -F -- "ssh-target.sock 127.0.0.1 'env' 'ZXFER_WRAPPED=1' " "$SSH_LOG")"
	# The mock ssh also runs a master's `-fN localhost doas` tail, which real
	# ssh never runs: that doas call has no command and logs an empty line.
	assertEquals "the wrapper must really have run every origin command" \
		"$l_origin_commands" "$(grep -c . "$DOAS_LOG")"
	assertEquals "every send pipeline must cross the origin wrapper whole through sh -c" \
		3 "$(grep -F -- "localhost 'doas' 'sh' '-c' " "$SSH_LOG" |
			grep -c -F -- "'\\''send'\\''")"
	assertEquals "every receive pipeline must cross the target wrapper whole through sh -c" \
		3 "$(grep -F -- "127.0.0.1 'env' 'ZXFER_WRAPPED=1' 'sh' '-c' " "$SSH_LOG" |
			grep -c -F -- "'\\''receive'\\''")"
	assertEquals "each host must cost exactly one capability probe round trip" \
		2 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertEquals "the capability probe must answer every OS and helper question" \
		0 "$(planning_count_direct_remote_probes)"
	assertEquals "-V must label the origin master with its host spec" 1 \
		"$(grep -c -F 'Opening ssh control socket [origin: localhost doas]: ' "$CASE_DIR/zxfer.stderr")"
	assertEquals "-V must label the target master with its host spec" 1 \
		"$(grep -c -F "Opening ssh control socket [target: $l_target_spec]: " "$CASE_DIR/zxfer.stderr")"
	assertEquals "-V must label each probe with its role and host spec" 2 \
		"$(grep -c -F -e 'Running remote probe [origin: localhost doas]: ' \
			-e "Running remote probe [target: $l_target_spec]: " "$CASE_DIR/zxfer.stderr")"
	assertContains "-V must print a target command with its label and full ssh argv" \
		"$l_stderr" "Running remote command [target: $l_target_spec]: '$MOCKBIN_DIR/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '"
	assertEquals "every -V remote command line must carry one of the two role labels" \
		"$(grep -c '^Running remote command \[' "$CASE_DIR/zxfer.stderr")" \
		"$(grep -c -F -e 'Running remote command [origin: localhost doas]: ' \
			-e "Running remote command [target: $l_target_spec]: " "$CASE_DIR/zxfer.stderr")"
	assertEquals "-V must profile each origin command once on the source side" \
		"$l_origin_commands" "$(planning_profile_counter source_ssh_shell_invocations)"
	assertEquals "-V must profile each target command once on the destination side" \
		"$l_target_commands" "$(planning_profile_counter destination_ssh_shell_invocations)"
	planning_assert_no_mutations
}

# Invariant (-V, one host for both roles): with equal -O and -T host specs
# the one master's open line names the host as origin/target, and every
# other remote command carries the role it serves: a destination command is
# labeled and profiled as target-side although its host spec also matches -O.
test_remote_shared_host_very_verbose_labels_and_profiles_each_command_by_its_role() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_shared_verbose.log"
	: >"$SSH_LOG"

	planning_run_remote_zxfer "$FIXTURE_DIR/incremental" -V -O localhost -T localhost \
		-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_shared_status=$?
	l_commands=$(grep -F -- 'ssh-origin.sock localhost ' "$SSH_LOG")
	l_source_commands=$(printf '%s\n' "$l_commands" | grep -c -F -- "$ZXFER_MOCKBIN_SOURCE_ROOT")
	l_destination_commands=$(printf '%s\n' "$l_commands" | grep -c -F -- "$ZXFER_MOCKBIN_DEST_ROOT")

	assertEquals "-V -O -T one-host incremental must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_shared_status"
	planning_assert_ssh_commands_multiplexed 1
	assertEquals "the one master must be labeled origin/target" 1 \
		"$(grep -c -F 'Opening ssh control socket [origin/target: localhost]: ' "$CASE_DIR/zxfer.stderr")"
	assertEquals "the one capability probe must run for the origin" 1 \
		"$(grep -c -F 'Running remote probe [origin: localhost]: ' "$CASE_DIR/zxfer.stderr")"
	assertNotEquals "the destination must have run a -V labeled command" 0 \
		"$(grep -c -F 'Running remote command [target: localhost]: ' "$CASE_DIR/zxfer.stderr")"
	assertEquals "no remote command may fall back to the shared label" 0 \
		"$(grep -c -F 'Running remote command [origin/target: ' "$CASE_DIR/zxfer.stderr")"
	assertEquals "-V must profile the probe and each source command on the source side" \
		"$((l_source_commands + 1))" "$(planning_profile_counter source_ssh_shell_invocations)"
	assertEquals "-V must profile each destination command on the destination side" \
		"$l_destination_commands" "$(planning_profile_counter destination_ssh_shell_invocations)"
	planning_assert_no_mutations
}

# Invariant: -O and -T host specs are split into literal tokens and never
# parsed by a shell, so a spec that needs shell quoting or escapes is a usage
# error with exit status 2 before zxfer runs ssh or zfs.
test_remote_host_spec_needing_shell_syntax_is_a_usage_error_before_any_ssh() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_quoted_spec.log"

	for l_spec_case in "-O|localhost \"doas\"" "-T|localhost 'doas'" "-T|localhost pf\\exec"; do
		l_spec_option=${l_spec_case%%|*}
		l_spec=${l_spec_case#*|}
		: >"$SSH_LOG"
		rm -f "$ZFS_LOG"
		planning_run_remote_zxfer "$FIXTURE_DIR/noop" "$l_spec_option" "$l_spec" \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		assertEquals "$l_spec_option [$l_spec] must be a usage error" 2 "$?"
		for l_spec_line in "failure_class: usage" "failure_stage: cli parse" \
			"message: Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."; do
			assertTrue "$l_spec_option [$l_spec] report must hold: $l_spec_line" \
				"grep -Fqx '$l_spec_line' '$CASE_DIR/zxfer.stderr'"
		done
		assertEquals "$l_spec_option [$l_spec] must not reach ssh" "" "$(cat "$SSH_LOG")"
		assertFalse "$l_spec_option [$l_spec] must not run zfs" "[ -s '$ZFS_LOG' ]"
	done
}

# Invariant (ssh policy): the ZXFER_SSH_* policy shapes every ssh argv zxfer
# builds: the masters, every remote command and pipeline, and the closes.
# ZXFER_SSH_USE_AMBIENT_CONFIG=yes drops the managed -o options, ignoring the
# other policy variables, and keeps control-socket reuse; otherwise
# BatchMode, StrictHostKeyChecking and an absolute UserKnownHostsFile lead
# every argv, in that order.
test_remote_ssh_policy_environment_shapes_every_ssh_argv() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_policy.log"
	: >"$SSH_LOG"
	l_known_hosts="$CASE_DIR/known_hosts"

	(
		ZXFER_SSH_USE_AMBIENT_CONFIG=yes
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/ignored_known_hosts
		export ZXFER_SSH_USE_AMBIENT_CONFIG ZXFER_SSH_USER_KNOWN_HOSTS_FILE
		planning_run_remote_zxfer "$FIXTURE_DIR/incremental" -O localhost \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "an ambient-config pull must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$?"
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	planning_assert_ssh_commands_multiplexed 1
	assertEquals "ambient ssh config must add no -o option to any argv; ssh log: $(cat "$SSH_LOG")" \
		0 "$(grep -c '^-o ' "$SSH_LOG")"

	: >"$SSH_LOG"
	: >"$ZFS_LOG"
	(
		ZXFER_SSH_BATCH_MODE=no
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING=accept-new
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=$l_known_hosts
		export ZXFER_SSH_BATCH_MODE ZXFER_SSH_STRICT_HOST_KEY_CHECKING \
			ZXFER_SSH_USER_KNOWN_HOSTS_FILE
		planning_run_remote_zxfer "$FIXTURE_DIR/incremental" -O localhost -T 127.0.0.1 \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "a managed-policy push and pull must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$?"
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	planning_assert_ssh_commands_multiplexed 2
	planning_assert_every_ssh_argv_leads_with \
		"-o BatchMode=no -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=$l_known_hosts"
}

# Invariant: zxfer starts from its own transport and capability state. Globals
# a caller exports under zxfer's names (each role's parsed host, wrapper and
# control socket, a validated ssh policy without options, filled capability
# slots) are reset before -O and -T are parsed, so they cannot redirect ssh,
# reuse a socket zxfer did not open, drop the ssh policy or skip a probe.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the planted wrappers
test_remote_hosts_ignore_inherited_transport_and_capability_state() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_inherited.log"
	: >"$SSH_LOG"
	l_records="zfs$(printf '\t')0$(printf '\t')/planted/zfs"

	(
		g_zxfer_ssh_origin_spec=localhost
		g_zxfer_ssh_origin_host=planted.invalid
		g_zxfer_ssh_origin_wrapper="'planted-wrapper'"
		g_ssh_origin_control_socket="$CASE_DIR/planted-origin.sock"
		g_zxfer_ssh_target_spec=127.0.0.1
		g_zxfer_ssh_target_host=planted.invalid
		g_zxfer_ssh_target_wrapper="'planted-wrapper'"
		g_ssh_target_control_socket="$CASE_DIR/planted-target.sock"
		g_zxfer_ssh_transport_ready=1
		g_zxfer_ssh_policy_options=""
		g_origin_remote_capabilities_host=localhost
		g_origin_remote_capabilities_tools=zfs
		g_origin_remote_capabilities_os=PlantedOS
		g_origin_remote_capabilities_zfs_status=0
		g_origin_remote_capabilities_tool_records=$l_records
		g_origin_remote_capabilities_response=planted
		g_target_remote_capabilities_host=127.0.0.1
		g_target_remote_capabilities_tools=zfs
		g_target_remote_capabilities_os=PlantedOS
		g_target_remote_capabilities_zfs_status=0
		g_target_remote_capabilities_tool_records=$l_records
		g_target_remote_capabilities_response=planted
		g_origin_cmd_zfs=/planted/zfs
		g_target_cmd_zfs=/planted/zfs
		export g_zxfer_ssh_origin_spec g_zxfer_ssh_origin_host \
			g_zxfer_ssh_origin_wrapper g_ssh_origin_control_socket \
			g_zxfer_ssh_target_spec g_zxfer_ssh_target_host \
			g_zxfer_ssh_target_wrapper g_ssh_target_control_socket \
			g_zxfer_ssh_transport_ready g_zxfer_ssh_policy_options \
			g_origin_remote_capabilities_host g_origin_remote_capabilities_tools \
			g_origin_remote_capabilities_os g_origin_remote_capabilities_zfs_status \
			g_origin_remote_capabilities_tool_records \
			g_origin_remote_capabilities_response \
			g_target_remote_capabilities_host g_target_remote_capabilities_tools \
			g_target_remote_capabilities_os g_target_remote_capabilities_zfs_status \
			g_target_remote_capabilities_tool_records \
			g_target_remote_capabilities_response g_origin_cmd_zfs g_target_cmd_zfs
		planning_run_remote_zxfer "$FIXTURE_DIR/noop" -O localhost -T 127.0.0.1 \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	l_inherited_status=$?

	assertEquals "-O -T no-op with planted globals must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_inherited_status"
	planning_assert_ssh_commands_multiplexed 2
	assertEquals "no planted value may reach ssh; ssh log: $(cat "$SSH_LOG")" \
		0 "$(grep -c planted "$SSH_LOG")"
	planning_assert_every_ssh_argv_leads_with \
		"-o BatchMode=yes -o StrictHostKeyChecking=yes"
	assertEquals "each host must be probed afresh, exactly once" \
		2 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	planning_assert_no_mutations
}

# Invariant: under a short TMPDIR each master's control socket lives directly
# in the private run root, so the root's removal at exit takes it along: no
# separate socket directory is made and TMPDIR ends empty.
test_remote_noop_under_a_short_tmpdir_keeps_its_socket_in_the_run_root() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_short_tmpdir.log"
	: >"$SSH_LOG"
	# A short private TMPDIR under /tmp keeps the socket path below the
	# sun_path limit whatever the suite's own temp root.
	l_short_tmpdir=$(mktemp -d /tmp/zxfer-contract.XXXXXX) ||
		fail "Unable to create the short TMPDIR."
	chmod 700 "$l_short_tmpdir"

	(
		TMPDIR=$l_short_tmpdir
		export TMPDIR
		planning_run_remote_zxfer "$FIXTURE_DIR/noop" -O localhost \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	l_short_status=$?
	l_short_socket=$(awk '$1 == "-M" || index($0, " -M -S ") {
		for (i = 1; i < NF; i++) if ($i == "-S") { print $(i + 1); exit }
	}' "$SSH_LOG")
	l_short_socket_root=${l_short_socket%/*}
	case ${l_short_socket_root##*/} in
	zxfer.ssh.*) l_short_root_kind="socket directory" ;;
	zxfer.*) l_short_root_kind="run root" ;;
	*) l_short_root_kind=unknown ;;
	esac

	assertEquals "-O no-op under a short TMPDIR must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_short_status"
	planning_assert_ssh_commands_multiplexed 1
	assertEquals "the origin socket must sit in a directory directly under TMPDIR: $l_short_socket" \
		"$(cd -P "$l_short_tmpdir" && pwd)" "${l_short_socket_root%/*}"
	assertEquals "that directory must be the run root: $l_short_socket" \
		"run root" "$l_short_root_kind"
	assertEquals "the run must leave TMPDIR empty" "" "$(ls -A "$l_short_tmpdir")"
	rm -rf "$l_short_tmpdir"
}

# Invariant (closing masters at exit): each master gets one `-O exit` at exit,
# and a failed close of the origin still closes the target. A close whose ssh
# reports the master already gone ("Control socket connect(...): Connection
# refused" and the like) counts as closed, so the run exits 0; any other
# close failure prints ssh's diagnostic and fails the run with a
# trap-cleanup report.
test_remote_master_close_at_exit_treats_a_gone_master_as_closed() {
	planning_setup_env
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_close.log"

	for l_close_case in gone failed; do
		: >"$SSH_LOG"
		: >"$ZFS_LOG"
		rm -rf "$CASE_DIR/fail_calls"
		mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
		(
			MOCK_FAIL_TOOL=ssh
			MOCK_FAIL_CALL=1
			MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
			MOCK_FAIL_MATCH='* -O exit *'
			export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH
			if [ "$l_close_case" = gone ]; then
				MOCK_FAIL_STDERR="Control socket connect($CASE_DIR/ssh-origin.sock): Connection refused"
				export MOCK_FAIL_STDERR
			fi
			planning_run_remote_zxfer "$FIXTURE_DIR/noop" -O localhost -T 127.0.0.1 \
				-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		)
		l_close_status=$?

		assertEquals "[$l_close_case] exactly the origin close must have failed; ssh log: $(cat "$SSH_LOG")" \
			1 "$(awk -F '\t' '$1 == "fail" && / -O exit localhost$/' "$SSH_LOG" | awk 'END { print NR }')"
		assertEquals "[$l_close_case] the target master must still be closed" \
			1 "$(awk -F '\t' '$1 == "control" && / -O exit 127\.0\.0\.1$/' "$SSH_LOG" | awk 'END { print NR }')"
		planning_assert_no_mutations
		if [ "$l_close_case" = gone ]; then
			assertEquals "a master already gone at exit must count as closed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
				0 "$l_close_status"
			assertNotContains "a clean close must not report a failure" \
				"$(cat "$CASE_DIR/zxfer.stderr")" "zxfer: failure report begin"
		else
			assertEquals "any other close failure must fail the run" 1 "$l_close_status"
			assertContains "ssh's diagnostic must reach the operator" \
				"$(cat "$CASE_DIR/zxfer.stderr")" "Connection to localhost closed by remote host."
			planning_assert_failure_report "trap cleanup" \
				"Failed to close one or more ssh control sockets during exit."
		fi
	done
}

# Invariant: under BatchMode=yes both masters start before either is waited
# for. When the origin's fails the run fails closed naming the origin, before
# any remote command or zfs call; the target master that came up is closed
# at exit and the failed origin socket is never used again.
test_remote_origin_master_failure_still_closes_the_target_master() {
	planning_setup_env
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_origin_master_failure.log"
	: >"$SSH_LOG"
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."

	(
		MOCK_FAIL_TOOL=ssh
		MOCK_FAIL_CALL=1
		MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
		MOCK_FAIL_MATCH='* -M -S * localhost'
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH
		planning_run_remote_zxfer "$FIXTURE_DIR/incremental" -O localhost -T 127.0.0.1 \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	l_master_status=$?

	assertEquals "a failed origin master must fail the run" 1 "$l_master_status"
	planning_assert_failure_report "cli validation" \
		"Error creating ssh control socket for origin host."
	assertContains "ssh's own diagnostic must reach the operator" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "Connection to localhost closed by remote host."
	assertEquals "the failed origin socket must never be used again; ssh log: $(cat "$SSH_LOG")" \
		1 "$(grep -c 'ssh-origin\.sock' "$SSH_LOG")"
	assertEquals "the target master must have been started with the origin's" \
		1 "$(awk -F '\t' '$1 == "master" && /ssh-target\.sock -fN 127\.0\.0\.1$/' "$SSH_LOG" | awk 'END { print NR }')"
	assertEquals "the target master must be closed at exit" \
		1 "$(awk -F '\t' '$1 == "control" && /ssh-target\.sock -O exit 127\.0\.0\.1$/' "$SSH_LOG" | awk 'END { print NR }')"
	assertEquals "no remote command may run" \
		0 "$(awk -F '\t' '$1 == "mux" || $1 == "direct"' "$SSH_LOG" | awk 'END { print NR }')"
	assertEquals "no zfs command may run" "" "$(grep -v '^FAIL ' "$ZFS_LOG")"
}

# Invariant: a capability probe that fails during startup is not fatal and
# caches nothing: the next lookup probes the host again and the run goes on.
# The probe's ssh diagnostic reaches stderr under -v and is suppressed
# otherwise.
test_remote_failed_capability_preload_is_probed_again() {
	planning_setup_env
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_preload.log"

	for l_preload_mode in quiet -v; do
		: >"$SSH_LOG"
		rm -rf "$CASE_DIR/fail_calls"
		mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
		set -- -O localhost -R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		[ "$l_preload_mode" = quiet ] || set -- "$l_preload_mode" "$@"
		# Calls 1 and 2 are the support probe and the master open.
		(
			MOCK_FAIL_TOOL=ssh
			MOCK_FAIL_CALL=3
			MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
			export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR
			planning_run_remote_zxfer "$FIXTURE_DIR/noop" "$@"
		)
		l_preload_status=$?

		assertEquals "[$l_preload_mode] a failed startup probe must not fail the run; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_preload_status"
		assertEquals "[$l_preload_mode] the failed third call must be the capability probe; ssh log: $(cat "$SSH_LOG")" \
			1 "$(sed -n '3p' "$SSH_LOG" | sed "s/' 'd//g" |
				awk -F '\t' '$1 == "fail" && index($0, "ZXFER_REMOTE_CAPS_V2")' | awk 'END { print NR }')"
		assertEquals "[$l_preload_mode] the host must be probed again after the failure" \
			2 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
		planning_assert_no_mutations
		if [ "$l_preload_mode" = quiet ]; then
			assertNotContains "a quiet run must not print the startup probe's diagnostic" \
				"$(cat "$CASE_DIR/zxfer.stderr")" "closed by remote host"
		else
			assertContains "-v must print the startup probe's diagnostic" \
				"$(cat "$CASE_DIR/zxfer.stderr")" "Connection to localhost closed by remote host."
		fi
	done
}

# Purpose: Put an ssh stand-in in front of the socket-aware mock ssh (moved
# to ssh.socket) that answers zxfer's capability probe with the contents of
# $CASE_DIR/capability_answer and logs it to SSH_LOG. The probe is found by
# its marker once the `d` chunk boundaries are removed.
# Usage: planning_write_capability_answering_ssh
planning_write_capability_answering_ssh() {
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh.socket" ||
		fail "Unable to write socket-aware mock ssh."
	cat >"$MOCKBIN_DIR/ssh" <<EOF
#!/bin/sh
for answer_arg in "\$@"; do
	answer_command=\$answer_arg
done
answer_script=\$(printf '%s' "\${answer_command:-}" | sed "s/' 'd//g")
case \$answer_script in
*ZXFER_REMOTE_CAPS_V2*)
	[ -z "\${MOCK_SSH_LOG:-}" ] || printf '%s\n' "\$*" >>"\$MOCK_SSH_LOG"
	cat "$CASE_DIR/capability_answer"
	exit 0
	;;
esac
exec "$MOCKBIN_DIR/ssh.socket" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/ssh"
}

# Invariant (capability answers): a malformed answer, here one in the retired
# V1 framing, is never used: zxfer asks the OS and zfs through direct probes
# under the secure PATH, and the run succeeds. A well-formed answer that
# lists zfs as missing (status 1) or unqueryable (another status) stops the
# run before any zfs call with a dependency report naming the host.
test_remote_capability_answers_decide_between_direct_probes_and_dependency_errors() {
	planning_setup_env
	planning_write_capability_answering_ssh
	SSH_LOG="$CASE_DIR/ssh_capability_answers.log"
	l_secure_path=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")

	for l_answer_case in retired missing unqueryable; do
		case $l_answer_case in
		retired) printf 'ZXFER_REMOTE_CAPS_V1\nos\tRemoteOS\ntool\tzfs\t0\t/remote/bin/zfs\nend\n' ;;
		missing) printf 'ZXFER_REMOTE_CAPS_V2\nos\tRemoteOS\ntool\tzfs\t1\t-\nend\n' ;;
		*) printf 'ZXFER_REMOTE_CAPS_V2\nos\tRemoteOS\ntool\tzfs\t2\t-\nend\n' ;;
		esac >"$CASE_DIR/capability_answer"
		: >"$SSH_LOG"
		: >"$ZFS_LOG"
		planning_run_remote_zxfer "$FIXTURE_DIR/noop" -O localhost \
			-R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		l_answer_status=$?

		if [ "$l_answer_case" = retired ]; then
			sed "s/' 'd//g" "$SSH_LOG" >"$CASE_DIR/ssh_capability_answers.joined"
			assertEquals "a malformed answer must fall back to direct probes; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
				0 "$l_answer_status"
			assertEquals "the OS must come from one direct uname probe under the secure PATH; ssh log: $(cat "$SSH_LOG")" \
				1 "$(grep -c -F -- "PATH='\\''$l_secure_path'\\''; export PATH; uname 2>/dev/null" \
					"$CASE_DIR/ssh_capability_answers.joined")"
			assertEquals "zfs must come from one direct probe under the secure PATH" \
				1 "$(grep -F -- "PATH='\\''$l_secure_path'\\''; export PATH; " \
					"$CASE_DIR/ssh_capability_answers.joined" |
					grep -c -F -- "command -v '\\''zfs'\\''")"
			assertEquals "the resolved zfs must list the source over the master" \
				1 "$(grep -c -F -- "'\\''$MOCKBIN_DIR/zfs'\\'' '\\''list'\\''" \
					"$CASE_DIR/ssh_capability_answers.joined")"
			continue
		fi
		if [ "$l_answer_case" = missing ]; then
			l_answer_message="Required dependency \"zfs\" not found on host localhost in secure PATH ($l_secure_path)."
		else
			l_answer_message="Failed to query dependency \"zfs\" on host localhost."
		fi
		assertEquals "[$l_answer_case] zfs must stop the run" 1 "$l_answer_status"
		for l_answer_line in "failure_class: dependency" "failure_stage: cli validation"; do
			assertTrue "[$l_answer_case] report must hold: $l_answer_line" \
				"grep -Fqx '$l_answer_line' '$CASE_DIR/zxfer.stderr'"
		done
		assertContains "[$l_answer_case] the report must name the host and the dependency" \
			"$(cat "$CASE_DIR/zxfer.stderr")" "message: $l_answer_message"
		assertEquals "[$l_answer_case] no direct probe may second-guess a well-formed answer" \
			0 "$(planning_count_direct_remote_probes)"
		assertFalse "[$l_answer_case] no zfs command may run" "[ -s '$ZFS_LOG' ]"
	done
}

. "$SHUNIT2_BIN"
