#!/bin/sh
# SSH control-socket directory, lifecycle and capability behavior tests.
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
			printf 'note=%s\n' "$(cat "$TEST_TMPDIR/socket-dir-fallback.note")"
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

test_zxfer_ensure_remote_host_capabilities_fills_memory_from_one_live_probe() {
	probe_count_file="$TEST_TMPDIR/ensure-live-probe-count"
	printf '0\n' >"$probe_count_file"
	g_option_O_origin_host="origin.example"
	zxfer_fetch_remote_host_capabilities_live() {
		l_count=$(($(cat "$probe_count_file") + 1))
		printf '%s\n' "$l_count" >"$probe_count_file"
		g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
		zxfer_parse_remote_capability_response "$g_zxfer_remote_capability_response_result"
	}

	first=$(zxfer_ensure_remote_host_capabilities "origin.example" source)
	first_status=$?
	# Plain (non-command-substitution) call so the in-memory store persists in
	# this shell, mirroring the preload flow.
	zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	second=$(zxfer_ensure_remote_host_capabilities "origin.example" source)
	second_status=$?
	probe_count=$(cat "$probe_count_file")

	unset -f zxfer_fetch_remote_host_capabilities_live
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "The first capability bootstrap should succeed from the live probe." 0 "$first_status"
	assertContains "The first capability bootstrap should publish the live payload." \
		"$first" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "Memory-backed lookups should succeed after the warm-up call." 0 "$second_status"
	assertContains "Memory-backed lookups should replay the stored payload." \
		"$second" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "One warmed host should cost exactly two live probes before the memory tier fills (one per command-substituted call) and zero after." \
		2 "$probe_count"
}

test_zxfer_ensure_remote_host_capabilities_preserves_live_probe_diagnostic() {
	set +e
	output=$(
		(
			zxfer_fetch_remote_host_capabilities_live() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability ensure should fail when the live capability probe fails." 1 "$status"
	assertContains "Remote capability ensure should preserve the underlying live-probe transport diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_ensure_remote_host_capabilities_never_treats_failed_probe_as_empty() {
	set +e
	output=$(
		(
			zxfer_fetch_remote_host_capabilities_live() {
				return 37
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
			printf 'status=%s\n' "$?"
			printf 'stored=<%s|%s>\n' "${g_origin_remote_capabilities_host:-}" \
				"${g_origin_remote_capabilities_response:-}"
		)
	)

	assertContains "Remote capability ensure should propagate the live probe failure status." \
		"$output" "status=37"
	assertContains "A failed probe must never populate the in-memory capability state." \
		"$output" "stored=<|>"
}

test_zxfer_reset_remote_host_state_resets_capability_and_resolved_tool_state() {
	result=$(
		(
			g_cmd_zfs="/stub/zfs"
			g_origin_remote_capabilities_host="origin.example"
			g_origin_remote_capabilities_response="dirty-origin"
			g_origin_remote_capabilities_os="DirtyOriginOS"
			g_target_remote_capabilities_tools="zfs cat"
			g_target_remote_capabilities_response="dirty-target"
			g_target_remote_capabilities_tool_records="dirty-target-tools"
			g_zxfer_remote_probe_capture_failed=1
			g_origin_cmd_zfs="/dirty/origin-zfs"

			zxfer_reset_remote_host_state
			printf 'origin=<%s|%s|%s>\n' "$g_origin_remote_capabilities_host" \
				"$g_origin_remote_capabilities_response" "$g_origin_remote_capabilities_os"
			printf 'target=<%s|%s|%s>\n' "$g_target_remote_capabilities_tools" \
				"$g_target_remote_capabilities_response" "$g_target_remote_capabilities_tool_records"
			printf 'capture_failed=%s\n' "$g_zxfer_remote_probe_capture_failed"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		)
	)

	assertContains "Remote-host reset should empty the origin capability slot." \
		"$result" "origin=<||>"
	assertContains "Remote-host reset should empty the target capability slot." \
		"$result" "target=<||>"
	assertContains "Remote-host reset should clear remote capture failure state." \
		"$result" "capture_failed=0"
	assertContains "Remote-host reset should restore origin zfs to the local default." \
		"$result" "origin_zfs=/stub/zfs"
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
