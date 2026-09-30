#!/bin/sh
# Connection tests for src/zxfer_ssh_transport.sh: the control-socket
# directory, opening masters, and control-socket actions, for the branches
# the black-box suites cannot reach (mktemp and path failures, planted
# sockets, overlapping handshakes). Run by tests/test_zxfer_ssh_transport.sh
# under the remote-host fixture. Masters, closes and wrapper specs are pinned
# black-box in tests/test_contract_remote.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Run zxfer_ensure_ssh_control_socket_dir in a subshell for one failure CASE
# of the short-directory fallback under ROOT, and print
# "CASE=STATUS handle=<SHORT_DIR> dir=<RESULT>".
# Usage: zxfer_test_socket_dir_failure_case CASE ROOT
zxfer_test_socket_dir_failure_case() {
	(
		l_case=$1
		l_root=$2
		g_zxfer_ssh_control_socket_dir_result=""
		g_zxfer_run_tmp_root="$l_root/long-run-root"
		zxfer_ensure_run_tmp_root() {
			[ "$l_case" != no_run_root ]
		}
		# Only paths under the run root are too long, unless every path is.
		zxfer_is_ssh_control_socket_path_short_enough() {
			[ "$l_case" != still_too_long ] &&
				[ "${1#"$g_zxfer_run_tmp_root"/}" = "$1" ]
		}
		zxfer_find_default_tmpdir() {
			[ "$l_case" != no_default_root ] || return 1
			g_zxfer_default_tmpdir_result=$l_root/default
			[ "$l_case" != mktemp_fails ] ||
				g_zxfer_default_tmpdir_result=$l_root/missing-parent
		}
		[ "$l_case" != foreign_answer ] || PATH="$l_root/fake-mktemp:$PATH"
		zxfer_ensure_ssh_control_socket_dir
		printf '%s=%s handle=<%s> dir=<%s>\n' "$l_case" "$?" \
			"$g_zxfer_ssh_control_socket_short_dir" \
			"$g_zxfer_ssh_control_socket_dir_result"
	)
}

# Run zxfer_open_ssh_control_sockets for -O origin.example in a subshell for
# one failure CASE under ROOT, and print what it threw and the origin socket
# it kept.
# Usage: zxfer_test_open_failure_case CASE ROOT
zxfer_test_open_failure_case() {
	(
		l_case=$1
		l_root=$2
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_ssh_supports_control_sockets=1
		g_option_O_origin_host=origin.example
		zxfer_ensure_ssh_control_socket_dir() {
			[ "$l_case" != no_socket_dir ] || return 1
			g_zxfer_ssh_control_socket_dir_result=$l_root/dir
			[ "$l_case" != planted_path ] ||
				g_zxfer_ssh_control_socket_dir_result=$l_root/planted
		}
		if [ "$l_case" = rejected_start ]; then
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_stderr="host spec rejected"
				return 1
			}
		fi
		zxfer_throw_error() {
			printf '%s: throw=%s socket=<%s>\n' "$l_case" "$1" \
				"$g_ssh_origin_control_socket"
			exit 1
		}
		zxfer_open_ssh_control_sockets
	)
}

test_ssh_control_socket_dir_fails_closed_without_a_short_private_directory() {
	l_root="$TEST_TMPDIR/socket_dir_failures"
	mkdir -p "$l_root/fake-mktemp" "$l_root/default"
	printf '#!/bin/sh\nexit 0\n' >"$l_root/fake-mktemp/mktemp"
	chmod 755 "$l_root/fake-mktemp/mktemp"
	l_long_suffix=""
	while [ "${#l_long_suffix}" -lt 150 ]; do
		l_long_suffix="${l_long_suffix}xxxxxxxxxx"
	done
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/zxfer-short/ssh-origin.sock"
	l_short_status=$?
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/$l_long_suffix/ssh-origin.sock"
	l_long_status=$?
	l_output=$(
		for l_case in no_run_root no_default_root mktemp_fails foreign_answer still_too_long; do
			zxfer_test_socket_dir_failure_case "$l_case" "$l_root"
		done
	)

	assertEquals "The sun_path check should accept a short socket path and reject a long one." \
		"0 1" "$l_short_status $l_long_status"
	assertEquals "Without a run root, a default temp root, a mktemp directory named from the template, or one short enough, no socket directory may be used." \
		"no_run_root=1 handle=<> dir=<>
no_default_root=1 handle=<> dir=<>
mktemp_fails=1 handle=<> dir=<>
foreign_answer=1 handle=<> dir=<>
still_too_long=1 handle=<> dir=<>" "$l_output"
	assertEquals "A short directory still too long should be removed at once." \
		"" "$(ls -A "$l_root/default")"
}

test_zxfer_remove_ssh_control_socket_dir_removes_only_ssh_names_and_fails_closed() {
	branch_root="$TEST_TMPDIR/ssh_socket_dir_removal"
	mkdir -p "$branch_root/zxfer.ssh.full" "$branch_root/zxfer.ssh.busy" \
		"$branch_root/link-target" "$branch_root/operator-dir"
	: >"$branch_root/zxfer.ssh.full/ssh-origin.sock"
	: >"$branch_root/zxfer.ssh.full/ssh-target.sock"
	: >"$branch_root/zxfer.ssh.full/ssh-target.sock.Mvij6x1tYLn6woxm"
	: >"$branch_root/zxfer.ssh.busy/ssh-origin.sock"
	: >"$branch_root/zxfer.ssh.busy/operator-file"
	: >"$branch_root/link-target/ssh-origin.sock"
	: >"$branch_root/operator-dir/ssh-origin.sock"
	ln -s "$branch_root/link-target" "$branch_root/zxfer.ssh.link"

	output=$(
		# No case statement: bash 3.2 as /bin/sh misparses unparenthesized
		# case patterns inside $( ).
		for l_case in none full busy link operator-dir missing; do
			g_zxfer_ssh_control_socket_short_dir="$branch_root/zxfer.ssh.$l_case"
			if [ "$l_case" = none ]; then
				g_zxfer_ssh_control_socket_short_dir=""
			elif [ "$l_case" = missing ]; then
				g_zxfer_ssh_control_socket_short_dir="$branch_root/zxfer.ssh.gone"
			elif [ "$l_case" = operator-dir ]; then
				g_zxfer_ssh_control_socket_short_dir="$branch_root/operator-dir"
			fi
			zxfer_remove_ssh_control_socket_dir
			printf '%s=%s handle=<%s>\n' "$l_case" "$?" \
				"${g_zxfer_ssh_control_socket_short_dir##*/}"
		done
	)

	assertEquals "Removal should succeed for no directory, a socket directory and one already gone, and keep the handle when it fails." \
		"none=0 handle=<>
full=0 handle=<>
busy=1 handle=<zxfer.ssh.busy>
link=1 handle=<zxfer.ssh.link>
operator-dir=1 handle=<operator-dir>
missing=0 handle=<>" "$output"
	assertFalse "The socket directory, sockets and ssh's leftover listener name should be gone." \
		"[ -e '$branch_root/zxfer.ssh.full' ]"
	assertTrue "Removal is not recursive: a file ssh did not create stays." \
		"[ -f '$branch_root/zxfer.ssh.busy/operator-file' ]"
	assertTrue "A symlink in place of the directory must not be followed." \
		"[ -f '$branch_root/link-target/ssh-origin.sock' ]"
	assertTrue "A directory this module did not name must not be touched." \
		"[ -f '$branch_root/operator-dir/ssh-origin.sock' ]"
}

test_open_ssh_control_sockets_fails_closed_without_trusting_the_socket_path() {
	l_root="$TEST_TMPDIR/open_failures"
	mkdir -p "$l_root/planted" "$l_root/dir"
	chmod 700 "$l_root/planted" "$l_root/dir"
	# Only this run writes the socket directory, so a path already there is
	# not a socket zxfer can trust.
	: >"$l_root/planted/ssh-origin.sock"
	l_log="$TEST_TMPDIR/open_failures_ssh.log"
	: >"$l_log"
	FAKE_SSH_LOG=$l_log
	export FAKE_SSH_LOG
	l_output=$(
		for l_case in planted_path no_socket_dir rejected_start; do
			zxfer_test_open_failure_case "$l_case" "$l_root" 2>&1
		done
	)
	unset FAKE_SSH_LOG

	assertEquals "A planted path, a missing socket directory and a rejected start should throw and forget the socket; a rejected start prints its diagnostic first." \
		"planted_path: throw=Error creating ssh control socket for origin host. socket=<>
no_socket_dir: throw=Error creating temporary directory for ssh control socket. socket=<>
host spec rejected
rejected_start: throw=Error creating ssh control socket for origin host. socket=<>" "$l_output"
	assertEquals "No master may be opened." "" "$(cat "$l_log")"
	assertTrue "The planted path must be left alone." "[ -f '$l_root/planted/ssh-origin.sock' ]"
}

test_ssh_control_socket_actions_reject_bad_input_as_errors() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
	zxfer_run_ssh_control_socket_action open "origin.example" "$TEST_TMPDIR/check.sock"
	policy_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr:$g_zxfer_ssh_control_socket_action_pid"
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	zxfer_run_ssh_control_socket_action exit 'origin.example "doas"' "$TEST_TMPDIR/check.sock"
	host_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr"
	zxfer_run_ssh_control_socket_action check "origin.example" "$TEST_TMPDIR/check.sock"
	check_status=$?
	zxfer_close_ssh_control_socket_for_role invalid
	role_status=$?

	assertEquals "An invalid ssh policy should be an action error with its diagnostic and start no ssh." \
		"1:error:ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path.:" "$policy_result"
	assertEquals "A host spec that needs shell quoting should be an action error with its diagnostic." \
		"1:error:Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$host_result"
	assertEquals "An unknown action, including the retired check, should fail closed." 1 "$check_status"
	assertEquals "Closing an unknown role should fail closed." 1 "$role_status"
}

test_ssh_control_socket_failure_helpers_classify_and_report() {
	l_socket="$TEST_TMPDIR/check.sock"
	# Each row: close stderr|status of the stale-master check. Only a
	# connect failure on the control socket itself means the master is gone.
	while IFS='|' read -r l_stderr l_expected_status; do
		zxfer_ssh_control_socket_failure_is_stale_master "$l_stderr"
		assertEquals "[$l_stderr] stale-master status" "$l_expected_status" "$?"
	done <<EOF
Control socket connect($l_socket): No such file or directory|0
Control socket connect($l_socket): Connection refused|0
Control socket connect($l_socket): Connection reset by peer|0
Control socket connect($l_socket): Broken pipe|0
Host key verification failed.|1
ssh: connect to host origin.example port 22: Connection refused|1
|1
EOF
	zxfer_reset_ssh_control_socket_action_state
	blank_output=$(zxfer_emit_ssh_control_socket_action_failure_message)
	blank_status=$?
	default_output=$(zxfer_emit_ssh_control_socket_action_failure_message "default action failure.")
	g_zxfer_ssh_control_socket_action_stderr="staged action failure"
	staged_output=$(zxfer_emit_ssh_control_socket_action_failure_message "ignored default")

	assertEquals "Without a staged stderr or a default nothing should print, successfully." \
		"0:" "$blank_status:$blank_output"
	assertEquals "Without a staged stderr the default should print." \
		"default action failure." "$default_output"
	assertEquals "A staged stderr should win over the default." \
		"staged action failure" "$staged_output"
}

test_open_ssh_control_sockets_overlaps_masters_only_in_batch_mode() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_open_order"
	order_log="$TEST_TMPDIR/open_order.log"
	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
for l_arg in "$@"; do
	l_host=$l_arg
done
printf 'start %s\n' "$l_host" >>"$ORDER_LOG"
sleep 1
printf 'end %s\n' "$l_host" >>"$ORDER_LOG"
EOF
	chmod +x "$fake_ssh"

	# Print the start/end order of both master opens under BatchMode=$1.
	zxfer_test_open_order() {
		: >"$order_log"
		(
			ORDER_LOG=$order_log
			export ORDER_LOG
			ZXFER_SSH_BATCH_MODE=$1
			g_cmd_ssh=$fake_ssh
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_open_ssh_control_sockets
		)
		tr '\n' ' ' <"$order_log"
	}
	order_yes=$(zxfer_test_open_order yes)
	order_no=$(zxfer_test_open_order no)

	assertContains "Under BatchMode=yes the origin handshake should start before either ends." \
		"${order_yes%%end*}" "start origin.example"
	assertContains "Under BatchMode=yes the target handshake should start before either ends." \
		"${order_yes%%end*}" "start target.example"
	assertEquals "Without BatchMode=yes a prompt could interleave, so the masters should open one at a time." \
		"start origin.example end origin.example start target.example end target.example " "$order_no"
}
