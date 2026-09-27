#!/bin/sh
# Connection tests for src/zxfer_ssh_transport.sh: local ssh resolution,
# control-socket directories, opening and closing masters, socket action
# results, and remote zfs command refresh. Run by
# tests/test_zxfer_ssh_transport.sh under the remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_zxfer_ensure_ssh_control_socket_dir_prefers_the_run_tmp_root() {
	output=$(
		(
			set +e
			short_root="$TEST_PRIVATE_DEFAULT_TMPDIR/zxfer.$$.run-root"
			mkdir -p "$short_root" || exit 90
			chmod 700 "$short_root" || exit 90
			g_zxfer_run_tmp_root=$short_root
			g_zxfer_ssh_control_socket_dir_result="stale"
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_ensure_ssh_control_socket_dir >"$TEST_TMPDIR/socket-dir.out"
			printf 'status=%s\n' "$?"
			printf 'dir=%s\n' "$g_zxfer_ssh_control_socket_dir_result"
			printf 'expected=%s\n' "$short_root"
		)
	)
	expected_root=$(printf '%s\n' "$output" | awk -F= '/^expected=/{print $2}')

	assertContains "Per-run socket directory resolution should succeed under a short run temp root." \
		"$output" "status=0"
	assertContains "Per-run sockets should live directly under the private run temp root." \
		"$output" "dir=$expected_root"
	assertFalse "Socket directory resolution should print nothing." \
		"[ -s '$TEST_TMPDIR/socket-dir.out' ]"
}

test_zxfer_ensure_ssh_control_socket_dir_falls_back_to_short_root_for_long_tmpdir() {
	long_component="zxfer-long-tmpdir-component-000000000000000000000000000000000000"
	long_tmpdir="$TEST_TMPDIR/$long_component/$long_component"
	mkdir -p "$long_tmpdir" || fail "Unable to create the long TMPDIR fixture."
	chmod 700 "$long_tmpdir" || fail "Unable to restrict the long TMPDIR fixture."

	output=$(
		(
			set +e
			TMPDIR=$long_tmpdir
			g_option_V_very_verbose=1
			zxfer_discard_runtime_cleanup_state
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			g_zxfer_run_tmp_root=""
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_ssh_control_socket_dir 2>"$TEST_TMPDIR/socket-dir-fallback.note"
			printf 'status=%s\n' "$?"
			socket_dir=$g_zxfer_ssh_control_socket_dir_result
			printf 'socket_dir=%s\n' "$socket_dir"
			# Prefix-strip instead of a case glob: bash 3.2 as /bin/sh
			# misparses unparenthesized case patterns inside $( ).
			if [ "${socket_dir#"$long_tmpdir"/}" != "$socket_dir" ]; then
				printf 'under_long_tmpdir=yes\n'
			else
				printf 'under_long_tmpdir=no\n'
			fi
			if [ -d "$socket_dir" ]; then
				printf 'socket_dir_mode=%s\n' "$(zxfer_get_path_mode_octal "$socket_dir")"
			fi
			[ "$g_zxfer_ssh_control_socket_short_dir" = "$socket_dir" ] &&
				printf '%s\n' 'short_dir_owned=yes'
			printf 'note=%s\n' "$(cat "$TEST_TMPDIR/socket-dir-fallback.note")"
			: >"$socket_dir/ssh-target.sock"
			zxfer_remove_ssh_control_socket_dir
			printf 'remove_status=%s handle=<%s>\n' "$?" \
				"$g_zxfer_ssh_control_socket_short_dir"
			[ -e "$socket_dir" ] || printf '%s\n' 'socket_dir=removed'
		)
	)

	assertContains "Long-TMPDIR socket directory resolution should still succeed." \
		"$output" "status=0"
	assertContains "Long-TMPDIR runs should not place control sockets under the long run temp root." \
		"$output" "under_long_tmpdir=no"
	assertContains "The fallback socket directory should be private to the effective user." \
		"$output" "socket_dir_mode=700"
	assertContains "Long-TMPDIR fallback should explain the shorter socket root under -V." \
		"$output" "for ssh control sockets; using shorter socket root"
	assertContains "The ssh transport should own the short directory it created." \
		"$output" "short_dir_owned=yes"
	assertContains "Removing the short directory should succeed and forget it." \
		"$output" "remove_status=0 handle=<>"
	assertContains "The short directory and its socket should be gone." \
		"$output" "socket_dir=removed"
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

test_zxfer_ensure_ssh_control_socket_dir_fails_closed_when_no_short_root_exists() {
	output=$(
		(
			set +e
			g_zxfer_run_tmp_root=""
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'no_root=%s\n' "$?"
		)
		(
			set +e
			long_component="zxfer-long-fallback-component-00000000000000000000000000000000"
			g_zxfer_run_tmp_root="/tmp/$long_component/$long_component"
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_find_default_tmpdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'no_fallback=%s\n' "$?"
		)
		(
			set +e
			long_component="zxfer-long-fallback-component-00000000000000000000000000000000"
			long_fallback_root="$TEST_TMPDIR/$long_component/$long_component"
			mkdir -p "$long_fallback_root" || exit 90
			g_zxfer_run_tmp_root="/tmp/$long_component/$long_component"
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_find_default_tmpdir() {
				g_zxfer_default_tmpdir_result=$long_fallback_root
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'fallback_too_long=%s\n' "$?"
		)
	)

	assertContains "Socket directory resolution should fail closed when the run temp root cannot be created." \
		"$output" "no_root=1"
	assertContains "Socket directory resolution should fail closed when no short fallback temp root exists." \
		"$output" "no_fallback=1"
	assertContains "Socket directory resolution should fail closed when even the fallback root exceeds sun_path limits." \
		"$output" "fallback_too_long=1"
}

test_zxfer_is_ssh_control_socket_path_short_enough_enforces_sun_path_limit() {
	long_suffix=""
	while [ "${#long_suffix}" -lt 150 ]; do
		long_suffix="${long_suffix}xxxxxxxxxx"
	done

	set +e
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/zxfer-short/ssh-origin.sock"
	short_status=$?
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/$long_suffix/ssh-origin.sock"
	long_status=$?

	assertEquals "SSH control-socket path-length checks should accept short socket paths." \
		0 "$short_status"
	assertEquals "SSH control-socket path-length checks should reject socket paths beyond the sun_path limit." \
		1 "$long_status"
}

test_ssh_control_socket_support_helper_covers_probe_success_and_failure() {
	fake_support_bin="$TEST_TMPDIR/fake_ssh_support"
	cat >"$fake_support_bin" <<'EOF'
#!/bin/sh
if [ "$1" = "-M" ] && [ "$2" = "-V" ]; then
	exit 0
fi
exit 1
EOF
	chmod +x "$fake_support_bin"

	g_cmd_ssh="$fake_support_bin"
	set +e
	zxfer_ssh_supports_control_sockets >/dev/null 2>&1
	support_status=$?
	g_cmd_ssh="$TEST_TMPDIR/missing_ssh"
	zxfer_ssh_supports_control_sockets >/dev/null 2>&1
	missing_status=$?

	assertEquals "SSH control-socket support helpers should detect a transport that accepts -M -V probes." \
		0 "$support_status"
	assertEquals "SSH control-socket support helpers should fail closed when the configured ssh helper cannot be probed." \
		"yes" "$(if [ "$missing_status" -ne 0 ]; then printf '%s' yes; else printf '%s' no; fi)"
}

test_open_ssh_control_sockets_opens_target_master_with_host_tokens() {
	log="$TEST_TMPDIR/open_target.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG

	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_supports_control_sockets=1
			g_option_T_target_host="target.example doas"
			zxfer_open_ssh_control_sockets
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'origin=<%s>\n' "$g_ssh_origin_control_socket"
			printf 'records=<%s>\n' "${g_zxfer_cleanup_pid_records:-}"
		)
	)

	unset FAKE_SSH_LOG

	socket=$(printf '%s\n' "$result" | awk -F= '/^socket=/{print $2}')
	assertContains "The target master should use the per-role socket name." \
		"$socket" "/ssh-target.sock"
	assertContains "A -T only run should open no origin master." \
		"$result" "origin=<>"
	assertContains "The finished open should leave no cleanup PID behind." \
		"$result" "records=<>"
	assertEquals "The target master open should preserve host token boundaries for ssh." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-M
-S
$socket
-fN
target.example
doas" "$(cat "$log")"
}

test_open_ssh_control_sockets_opens_one_master_per_host_spec_once() {
	log="$TEST_TMPDIR/open_per_spec.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG

	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_open_ssh_control_sockets
			zxfer_open_ssh_control_sockets
			printf 'distinct=%s %s\n' "${g_ssh_origin_control_socket##*/}" \
				"${g_ssh_target_control_socket##*/}"
			printf 'distinct_opens=%s\n' "$(grep -c '^-M$' "$log")"
			: >"$log"
			g_ssh_origin_control_socket=""
			g_ssh_target_control_socket=""
			g_option_T_target_host="origin.example"
			zxfer_open_ssh_control_sockets
			printf 'shared=%s <%s>\n' "${g_ssh_origin_control_socket##*/}" \
				"$g_ssh_target_control_socket"
			printf 'shared_opens=%s\n' "$(grep -c '^-M$' "$log")"
		)
	)

	unset FAKE_SSH_LOG

	assertContains "Distinct -O and -T specs should each get their own role socket." \
		"$result" "distinct=ssh-origin.sock ssh-target.sock"
	assertContains "Each role should open its master once, even when asked twice." \
		"$result" "distinct_opens=2"
	assertContains "A -T spec equal to the -O spec should share the origin master." \
		"$result" "shared=ssh-origin.sock <>"
	assertContains "One host spec should cost one master open." \
		"$result" "shared_opens=1"
}

test_open_ssh_control_sockets_logs_when_control_sockets_are_unavailable() {
	log="$TEST_TMPDIR/open_no_mux.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_supports_control_sockets=0
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			zxfer_open_ssh_control_sockets
			printf 'sockets=<%s><%s>\n' "$g_ssh_origin_control_socket" \
				"$g_ssh_target_control_socket"
		)
	)

	unset FAKE_SSH_LOG

	assertContains "The origin role should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for origin host."
	assertContains "The target role should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for target host."
	assertContains "Without control-socket support no role should record a socket." \
		"$output" "sockets=<><>"
	assertEquals "Without control-socket support no master should be opened." \
		"" "$(cat "$log")"
}

test_open_ssh_control_sockets_waits_for_both_masters_before_failing() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_open_by_host"
	open_log="$TEST_TMPDIR/open_by_host.log"
	: >"$open_log"
	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
for l_arg in "$@"; do
	l_host=$l_arg
done
printf '%s\n' "$l_host" >>"$OPEN_LOG"
if [ "$l_host" = "$FAIL_HOST" ]; then
	printf 'ssh: connect to host %s port 22: Connection refused\n' "$l_host" >&2
	exit 255
fi
exit 0
EOF
	chmod +x "$fake_ssh"

	set +e
	output=$(
		(
			OPEN_LOG=$open_log
			FAIL_HOST=origin.example
			export OPEN_LOG FAIL_HOST
			zxfer_throw_error() {
				printf 'throw=%s\n' "$1"
				printf 'sockets=<%s><%s>\n' "$g_ssh_origin_control_socket" \
					"${g_ssh_target_control_socket##*/}"
				printf 'records=<%s>\n' "${g_zxfer_cleanup_pid_records:-}"
				exit 1
			}
			g_cmd_ssh=$fake_ssh
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_open_ssh_control_sockets
		) 2>&1
	)
	status=$?

	assertEquals "A failed master open should fail closed." 1 "$status"
	assertContains "ssh's own diagnostic should reach the operator." \
		"$output" "ssh: connect to host origin.example port 22: Connection refused"
	assertContains "The failure should name the role whose master failed." \
		"$output" "throw=Error creating ssh control socket for origin host."
	assertContains "The failed role should forget its socket, and the opened one keep it for trap cleanup." \
		"$output" "sockets=<><ssh-target.sock>"
	assertContains "Both opens should be waited for and unregistered before the throw." \
		"$output" "records=<>"
	assertEquals "Both masters should have been started." \
		"origin.example
target.example" "$(sort "$open_log")"
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

test_open_ssh_control_sockets_fails_closed_on_a_preexisting_socket_path() {
	log="$TEST_TMPDIR/open_preexisting.log"
	: >"$log"
	socket_dir="$TEST_TMPDIR/open_preexisting_sockets"
	mkdir -p "$socket_dir"
	chmod 700 "$socket_dir"
	planted_socket="$socket_dir/ssh-origin.sock"
	: >"$planted_socket"

	set +e
	output=$(
		(
			FAKE_SSH_LOG=$log
			export FAKE_SSH_LOG
			zxfer_ensure_ssh_control_socket_dir() {
				g_zxfer_ssh_control_socket_dir_result=$socket_dir
			}
			zxfer_throw_error() {
				printf 'throw=%s\n' "$1"
				printf 'socket=<%s>\n' "$g_ssh_origin_control_socket"
				exit 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			zxfer_open_ssh_control_sockets
		)
	)
	status=$?
	rm -f "$planted_socket"

	assertEquals "A socket path this run did not create should fail closed." 1 "$status"
	assertContains "The planted path should be reported as a socket creation failure." \
		"$output" "throw=Error creating ssh control socket for origin host."
	assertContains "The planted path must not become the role socket." \
		"$output" "socket=<>"
	assertEquals "No master may be opened over a planted path." "" "$(cat "$log")"
}

test_close_origin_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_origin.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	: >"$g_ssh_origin_control_socket"

	zxfer_close_ssh_control_socket_for_role origin

	unset FAKE_SSH_LOG

	assertEquals "Origin socket path should be cleared after closing." "" "$g_ssh_origin_control_socket"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$TEST_TMPDIR/origin.sock
-O
exit
origin.example
pfexec" "$(cat "$log")"
}

test_close_target_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_target.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"
	: >"$g_ssh_target_control_socket"

	zxfer_close_ssh_control_socket_for_role target

	unset FAKE_SSH_LOG

	assertEquals "Target socket path should be cleared after closing." "" "$g_ssh_target_control_socket"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$TEST_TMPDIR/target.sock
-O
exit
target.example
doas" "$(cat "$log")"
}

test_zxfer_prepare_ssh_shell_command_context_reuses_the_role_spec_parse() {
	g_option_O_origin_host="origin.example pfexec"
	g_option_T_target_host=""
	zxfer_refresh_remote_zfs_commands
	# Only a fresh parse rewrites the raw token result.
	g_zxfer_ssh_host_spec_tokens_result=untouched

	zxfer_prepare_ssh_shell_command_context "origin.example pfexec" "echo one"
	first="$?|$g_zxfer_ssh_shell_host_result|$g_zxfer_ssh_shell_full_remote_command_result"
	zxfer_prepare_ssh_shell_command_context "origin.example pfexec" "echo two"
	second="$?|$g_zxfer_ssh_shell_host_result|$g_zxfer_ssh_shell_full_remote_command_result"
	role_tokens=$g_zxfer_ssh_host_spec_tokens_result
	zxfer_prepare_ssh_shell_command_context "elsewhere.example sudo" "echo three"
	other="$?|$g_zxfer_ssh_shell_host_result|$g_zxfer_ssh_shell_full_remote_command_result"

	assertEquals "A role spec should publish the host and wrap the command." \
		"0|origin.example|'pfexec' echo one" "$first"
	assertEquals "A second role-spec call should rewrap the new command identically." \
		"0|origin.example|'pfexec' echo two" "$second"
	assertEquals "Role specs should reuse the -O/-T parse instead of parsing again." \
		untouched "$role_tokens"
	assertEquals "Other specs should be parsed per call." \
		"0|elsewhere.example|'sudo' echo three" "$other"
	assertEquals "A per-call parse should publish the raw tokens." \
		"elsewhere.example
sudo" "$g_zxfer_ssh_host_spec_tokens_result"
}

test_ssh_supports_control_sockets_reflects_ssh_status() {
	g_cmd_ssh="$FAKE_SSH_BIN"

	FAKE_SSH_EXIT_STATUS=0
	export FAKE_SSH_EXIT_STATUS
	if zxfer_ssh_supports_control_sockets; then
		status_supported=0
	else
		status_supported=1
	fi

	FAKE_SSH_EXIT_STATUS=1
	export FAKE_SSH_EXIT_STATUS
	if zxfer_ssh_supports_control_sockets; then
		status_unsupported=0
	else
		status_unsupported=1
	fi

	unset FAKE_SSH_EXIT_STATUS

	assertEquals "zxfer_ssh_supports_control_sockets should succeed when ssh -M -V succeeds." 0 "$status_supported"
	assertEquals "zxfer_ssh_supports_control_sockets should fail when ssh -M -V fails." 1 "$status_unsupported"
}

test_select_ssh_control_socket_prefers_matching_control_socket() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	zxfer_select_ssh_control_socket "origin.example"
	assertEquals "The origin host should reuse the origin control socket." \
		"$TEST_TMPDIR/origin.sock" "$g_zxfer_ssh_control_socket_result"
	zxfer_select_ssh_control_socket "target.example"
	assertEquals "The target host should reuse the target control socket." \
		"$TEST_TMPDIR/target.sock" "$g_zxfer_ssh_control_socket_result"
	zxfer_select_ssh_control_socket "other.example"
	assertEquals "Unmatched hosts should use no control socket." \
		"" "$g_zxfer_ssh_control_socket_result"
	zxfer_select_ssh_control_socket ""
	assertEquals "An empty host spec should use no control socket." \
		"" "$g_zxfer_ssh_control_socket_result"
}

test_ssh_control_socket_open_renders_only_when_very_verbose() {
	quiet_output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_V_very_verbose=0
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_run_ssh_control_socket_action open \
				"other.example" "$TEST_TMPDIR/open.sock"
			wait "$g_zxfer_ssh_control_socket_action_pid"
		)
	)
	verbose_output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_V_very_verbose=1
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_run_ssh_control_socket_action open \
				"other.example" "$TEST_TMPDIR/open.sock"
			wait "$g_zxfer_ssh_control_socket_action_pid"
		)
	)

	assertEquals "Quiet runs should not render ssh control socket commands for display." \
		"" "$quiet_output"
	assertEquals "Very-verbose runs should keep the current control-socket operator line text." \
		"Opening ssh control socket [remote: other.example]: '$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-M' '-S' '$TEST_TMPDIR/open.sock' '-fN' 'other.example'" \
		"$verbose_output"
}

test_open_ssh_control_sockets_propagates_transport_policy_validation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_open_ssh_control_sockets
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "ssh control socket setup should propagate the underlying ssh policy validation message instead of a generic cache-dir error." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
	assertNotContains "ssh control socket setup should not mask transport-policy validation failures behind the generic tempdir message." \
		"$output" "Error creating temporary directory for ssh control socket."
}

test_ssh_control_socket_exit_classifies_closed_stale_and_error_results() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	socket="$TEST_TMPDIR/check.sock"
	stale_stderr="Control socket connect($socket): No such file or directory"
	results=""
	for action_case in \
		"exit|0||closed" \
		"exit|255|$stale_stderr|stale" \
		"exit|255|Host key verification failed.|error"; do
		action=${action_case%%|*}
		action_rest=${action_case#*|}
		FAKE_SSH_EXIT_STATUS=${action_rest%%|*}
		action_rest=${action_rest#*|}
		FAKE_SSH_STDERR=${action_rest%%|*}
		export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
		zxfer_run_ssh_control_socket_action "$action" "origin.example" "$socket"
		results="$results$action:$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr
"
	done
	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR

	assertEquals "A control-socket close should classify each ssh outcome and keep its stderr for the caller." \
		"exit:0:closed:
exit:1:stale:$stale_stderr
exit:1:error:Host key verification failed.
" "$results"
}

test_ssh_control_socket_action_reports_policy_and_host_spec_failures_as_errors() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
	zxfer_run_ssh_control_socket_action open "origin.example" "$TEST_TMPDIR/check.sock"
	policy_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr:$g_zxfer_ssh_control_socket_action_pid"
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	zxfer_run_ssh_control_socket_action exit 'origin.example "doas"' "$TEST_TMPDIR/check.sock"
	host_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr"
	zxfer_run_ssh_control_socket_action check "origin.example" "$TEST_TMPDIR/check.sock"
	check_status=$?

	assertEquals "An invalid ssh policy should be an action error with its diagnostic and start no ssh." \
		"1:error:ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path.:" "$policy_result"
	assertEquals "A host spec that needs shell quoting should be an action error with its diagnostic." \
		"1:error:Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$host_result"
	assertEquals "An unknown action, including the retired check, should fail closed." 1 "$check_status"
}

test_zxfer_ssh_control_socket_action_failure_helpers_cover_stale_classification_and_output() {
	zxfer_reset_ssh_control_socket_action_state
	blank_output=$(zxfer_emit_ssh_control_socket_action_failure_message)
	blank_status=$?
	default_output=$(zxfer_emit_ssh_control_socket_action_failure_message "default action failure.")
	default_status=$?
	g_zxfer_ssh_control_socket_action_stderr="staged action failure"
	staged_output=$(zxfer_emit_ssh_control_socket_action_failure_message "ignored default")
	staged_status=$?

	classification_output=$(
		(
			set +e
			zxfer_ssh_control_socket_failure_is_stale_master \
				"Control socket connect($TEST_TMPDIR/check.sock): No such file or directory"
			printf 'missing=%s\n' "$?"
			zxfer_ssh_control_socket_failure_is_stale_master \
				"Control socket connect($TEST_TMPDIR/check.sock): Broken pipe"
			printf 'broken_pipe=%s\n' "$?"
			zxfer_ssh_control_socket_failure_is_stale_master \
				"Host key verification failed."
			printf 'other=%s\n' "$?"
		)
	)

	assertEquals "ssh control socket action failure message emission should stay silent when no detail is staged and no default is supplied." \
		"" "$blank_output"
	assertEquals "ssh control socket action failure message emission should still succeed when no message is emitted." \
		0 "$blank_status"
	assertEquals "ssh control socket action failure message emission should print the default message when no detail is staged." \
		"default action failure." "$default_output"
	assertEquals "ssh control socket action failure message emission should succeed when printing the default message." \
		0 "$default_status"
	assertEquals "ssh control socket action failure message emission should prefer the staged stderr over the default message." \
		"staged action failure" "$staged_output"
	assertEquals "ssh control socket action failure message emission should succeed when printing the staged stderr." \
		0 "$staged_status"
	assertContains "ssh control socket stale-master detection should classify missing control sockets as stale masters." \
		"$classification_output" "missing=0"
	assertContains "ssh control socket stale-master detection should classify broken pipes as stale masters." \
		"$classification_output" "broken_pipe=0"
	assertContains "ssh control socket stale-master detection should not classify unrelated transport failures as stale masters." \
		"$classification_output" "other=1"
}

test_zxfer_close_all_ssh_control_sockets_prefers_origin_failure_and_uses_target_failure_when_origin_succeeds() {
	set +e
	output=$(
		(
			zxfer_close_ssh_control_socket_for_role() {
				[ "$1" != origin ] || return 7
				return 9
			}

			set +e
			zxfer_close_all_ssh_control_sockets
			printf 'origin_failure_status=%s\n' "$?"

			zxfer_close_ssh_control_socket_for_role() {
				[ "$1" != origin ] || return 0
				return 9
			}

			zxfer_close_all_ssh_control_sockets
			printf 'target_failure_status=%s\n' "$?"
		)
	)
	set -e

	assertContains "close-all socket cleanup should preserve the origin close status when origin cleanup fails first." \
		"$output" "origin_failure_status=7"
	assertContains "close-all socket cleanup should propagate the target close status when origin cleanup succeeds." \
		"$output" "target_failure_status=9"
}

test_zxfer_refresh_remote_zfs_commands_rejects_shell_quoted_host_specs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_O_origin_host='origin.example "pfexec -u zfs"'
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_refresh_remote_zfs_commands
		)
	)
	status=$?
	set -e

	assertEquals "Remote host-spec refresh should fail closed when the configured host spec relies on shell quoting." \
		2 "$status"
	assertContains "Rejected remote host specs should explain the literal-token requirement." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_zxfer_local_ssh_resolution_helpers_cover_success_and_failure_paths() {
	output=$(
		(
			set +e
			g_cmd_ssh=""
			zxfer_find_required_tool() {
				if [ "$1" = "ssh" ]; then
					g_zxfer_required_tool_result=$FAKE_SSH_BIN
					return 0
				fi
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_success=%s:%s:%s\n' "$?" "$g_cmd_ssh" "$g_zxfer_resolved_local_ssh_command_result"

			g_cmd_ssh=""
			zxfer_find_required_tool() {
				g_zxfer_required_tool_result="missing ssh"
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_failure=%s:%s\n' "$?" "$g_zxfer_resolved_local_ssh_command_result"
		)
	)

	assertContains "Lazy local ssh resolution should cache the resolved ssh helper on success." \
		"$output" "ensure_success=0:$FAKE_SSH_BIN:$FAKE_SSH_BIN"
	assertContains "Lazy local ssh resolution should preserve the dependency diagnostic when ssh lookup fails." \
		"$output" "ensure_failure=1:missing ssh"
}

test_close_ssh_control_socket_for_role_returns_early_without_state() {
	output=$(
		(
			zxfer_close_ssh_control_socket_for_role origin
			printf 'origin=%s\n' "$?"
			zxfer_close_ssh_control_socket_for_role target
			printf 'target=%s\n' "$?"
		)
	)

	assertContains "Origin ssh control socket close should return early without state." \
		"$output" "origin=0"
	assertContains "Target ssh control socket close should return early without state." \
		"$output" "target=0"
}

test_zxfer_ssh_open_and_close_error_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_setup_close_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/close-error.sock"
			: >"$g_ssh_origin_control_socket"
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="exit action failed"
				g_zxfer_ssh_control_socket_action_command="$1 $2 $3"
				return 1
			}
			zxfer_close_ssh_control_socket_for_role origin 2>"$branch_root/close-error.err"
			printf 'close_error_status=%s\n' "$?"
			printf 'close_error_err=%s\n' "$(cat "$branch_root/close-error.err")"
			printf 'close_error_state=%s\n' "$g_ssh_origin_control_socket"
			if [ -e "$branch_root/close-error.sock" ]; then
				printf 'close_error_socket=kept\n'
			else
				printf 'close_error_socket=removed\n'
			fi
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/close-stale.sock"
			: >"$g_ssh_origin_control_socket"
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_result=stale
				g_zxfer_ssh_control_socket_action_command="$1 $2 $3"
				return 1
			}
			zxfer_close_ssh_control_socket_for_role origin 2>"$branch_root/close-stale.err"
			printf 'close_stale_status=%s\n' "$?"
			printf 'close_stale_state=<%s>\n' "$g_ssh_origin_control_socket"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_supports_control_sockets=1
			zxfer_ensure_ssh_control_socket_dir() {
				return 1
			}
			zxfer_throw_error() {
				printf 'open_dir_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_open_ssh_control_sockets
			)
			printf 'open_dir_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_ssh_transport() {
				g_zxfer_ssh_transport_error="transport policy failure"
				return 1
			}
			zxfer_throw_error() {
				printf 'open_transport_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_open_ssh_control_sockets
			)
			printf 'open_transport_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_supports_control_sockets=1
			zxfer_ensure_ssh_control_socket_dir() {
				g_zxfer_ssh_control_socket_dir_result=$branch_root
			}
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="host spec rejected"
				return 1
			}
			zxfer_throw_error() {
				printf 'open_action_throw=%s\n' "$1"
				printf 'open_action_state=<%s>\n' "$g_ssh_origin_control_socket"
				exit 9
			}
			(
				zxfer_open_ssh_control_sockets
			) 2>"$branch_root/open-action.err"
			printf 'open_action_status=%s\n' "$?"
			printf 'open_action_err=%s\n' "$(cat "$branch_root/open-action.err")"
		)
	)

	assertContains "Socket close should fail closed when the exit action reports a non-stale error." \
		"$output" "close_error_status=1"
	assertContains "Socket close should surface the exit action diagnostic." \
		"$output" "close_error_err=exit action failed"
	assertContains "Socket close should preserve the role state when the exit action fails." \
		"$output" "close_error_state=$branch_root/close-error.sock"
	assertContains "Socket close should keep the socket path for trap-time retry when the exit action fails." \
		"$output" "close_error_socket=kept"
	assertContains "Socket close should treat a stale master as already closed." \
		"$output" "close_stale_status=0"
	assertContains "Socket close should clear the role state after a stale master." \
		"$output" "close_stale_state=<>"
	assertContains "Opening should fail closed when the per-run socket directory cannot be created." \
		"$output" "open_dir_throw=Error creating temporary directory for ssh control socket."
	assertContains "Opening should route transport policy failures through throw_error." \
		"$output" "open_transport_throw=transport policy failure"
	assertContains "Opening should route a rejected master start through throw_error." \
		"$output" "open_action_throw=Error creating ssh control socket for origin host."
	assertContains "A rejected master start should forget the role socket." \
		"$output" "open_action_state=<>"
	assertContains "A rejected master start should surface the action diagnostic first." \
		"$output" "open_action_err=host spec rejected"
}

test_zxfer_ssh_transport_directory_and_quoting_failure_branches_fail_closed() {
	branch_root="$TEST_TMPDIR/ssh_transport_directory_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			zxfer_parse_ssh_host_spec 'invalid "host"'
			printf 'quote_status=%s\n' "$?"
			printf 'quote_output=%s\n' "$g_zxfer_ssh_shell_context_error_result"
		)
		(
			set +e
			g_zxfer_ssh_control_socket_dir_result=""
			g_zxfer_run_tmp_root="$branch_root/long-run-root"
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_is_ssh_control_socket_path_short_enough() {
				return 1
			}
			zxfer_find_default_tmpdir() {
				g_zxfer_default_tmpdir_result=$branch_root/missing-parent
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'create_status=%s handle=<%s>\n' "$?" \
				"$g_zxfer_ssh_control_socket_short_dir"
		)
		(
			set +e
			g_zxfer_ssh_control_socket_dir_result=""
			g_zxfer_run_tmp_root="$branch_root/long-run-root"
			fake_mktemp_dir="$branch_root/fake-mktemp"
			mkdir -p "$fake_mktemp_dir"
			printf '#!/bin/sh\nexit 0\n' >"$fake_mktemp_dir/mktemp"
			chmod 755 "$fake_mktemp_dir/mktemp"
			PATH="$fake_mktemp_dir:$PATH"
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_is_ssh_control_socket_path_short_enough() {
				[ "${1#"$g_zxfer_run_tmp_root"/}" = "$1" ]
			}
			zxfer_find_default_tmpdir() {
				g_zxfer_default_tmpdir_result=$branch_root
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'foreign_status=%s handle=<%s> dir=<%s>\n' "$?" \
				"$g_zxfer_ssh_control_socket_short_dir" \
				"$g_zxfer_ssh_control_socket_dir_result"
		)
		(
			set +e
			g_zxfer_ssh_control_socket_dir_result=""
			g_zxfer_run_tmp_root="$branch_root/long-run-root"
			long_parent="$branch_root/long-parent"
			mkdir -p "$long_parent"
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_is_ssh_control_socket_path_short_enough() {
				return 1
			}
			zxfer_find_default_tmpdir() {
				g_zxfer_default_tmpdir_result=$long_parent
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'too_long_status=%s handle=<%s>\n' "$?" \
				"$g_zxfer_ssh_control_socket_short_dir"
			printf 'too_long_leftovers=<%s>\n' "$(ls -A "$long_parent")"
		)
	)

	assertContains "Host-spec quoting should reject specs that need shell quoting." \
		"$output" "quote_status=1"
	assertContains "Host-spec quoting should keep the literal-token diagnostic." \
		"$output" "quote_output=Host spec (-O/-T) must use literal whitespace-delimited tokens only"
	assertContains "A short socket directory mktemp cannot create should fail closed without a handle." \
		"$output" "create_status=1 handle=<>"
	assertContains "An empty or foreign mktemp answer should fail closed without a handle." \
		"$output" "foreign_status=1 handle=<> dir=<>"
	assertContains "A short socket directory still too long should fail closed without a handle." \
		"$output" "too_long_status=1 handle=<>"
	assertContains "A short socket directory still too long should be removed at once." \
		"$output" "too_long_leftovers=<>"
}

test_zxfer_ssh_transport_owner_guards_fail_closed() {
	output=$(
		(
			set +e
			zxfer_close_ssh_control_socket_for_role invalid
			printf 'close_invalid_role_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host=""
			g_option_T_target_host='invalid "target"'
			zxfer_throw_usage_error() {
				printf 'target_usage_throw=%s\n' "$1"
				exit "$2"
			}
			zxfer_refresh_remote_zfs_commands
			printf 'target_usage_status=%s\n' "$?"
		)
	)

	assertContains "Socket close dispatch should reject unknown roles." \
		"$output" "close_invalid_role_status=1"
	assertContains "Target host quoting failures should retain usage-error handling." \
		"$output" "target_usage_throw=Host spec (-O/-T) must use literal whitespace-delimited tokens only"
}
