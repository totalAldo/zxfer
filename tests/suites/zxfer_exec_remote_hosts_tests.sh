#!/bin/sh
# Tests for src/zxfer_remote_hosts.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

fake_remote_capability_response_missing_zfs() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	1	-
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
end
EOF
}

fake_remote_capability_response_missing_parallel() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	0	/remote/bin/cat
end
EOF
}

# Run the remote tool resolver and print its published result, keeping its
# status.
exec_remote_hosts_test_resolve() {
	zxfer_resolve_remote_required_tool "$@"
	l_test_resolve_status=$?
	printf '%s\n' "$g_zxfer_required_tool_result"
	return "$l_test_resolve_status"
}

test_resolve_remote_required_tool_uses_shell_probe_for_wrapped_hosts() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_secure_path="/opt/openzfs/bin:/usr/sbin"
	remote_log="$TEST_TMPDIR/resolve_remote_required_tool.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(exec_remote_hosts_test_resolve "backup@example.com pfexec -p 2222" zfs zfs source)

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote tool lookup should return the resolved absolute path." "/remote/bin/zfs" "$result"
	assertEquals "ssh should force batch mode before the wrapped-host probe target." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the wrapped-host probe target." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before the wrapped-host probe target." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the wrapped-host probe target." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "Host token should remain the ssh target." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Privilege wrapper should be preserved inside the remote command string." "$log_line_remote_cmd" "'pfexec'"
	assertContains "Wrapper flags should be preserved inside the remote command string." "$log_line_remote_cmd" "'-p'"
	assertContains "Wrapper flag values should be preserved inside the remote command string." "$log_line_remote_cmd" "'2222'"
	assertContains "Remote capability discovery should execute via sh -c for wrapped hosts." "$log_line_remote_cmd" "'sh' '-c'"
	assertContains "Remote capability discovery should pin the secure PATH inside the shell probe." "$log_line_remote_cmd" "/opt/openzfs/bin:/usr/sbin"
	assertContains "Remote capability discovery should query uname in the single handshake." "$log_line_remote_cmd" "uname"
	assertContains "Remote capability discovery should query zfs in the single handshake." "$log_line_remote_cmd" "zfs"
}

test_resolve_remote_required_tool_handles_realistic_ssh_command_joining() {
	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_join_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_join_exec.log"
	remote_bin_dir="$TEST_TMPDIR/remote_bins"

	mkdir -p "$remote_bin_dir"
	create_fake_ssh_join_exec_bin "$realistic_ssh_bin"
	cat >"$remote_bin_dir/zfs" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$remote_bin_dir/zfs"

	g_cmd_ssh="$realistic_ssh_bin"
	g_zxfer_secure_path="$remote_bin_dir:/usr/bin"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	result=$(exec_remote_hosts_test_resolve "backup@example.com" zfs zfs destination)

	unset FAKE_SSH_LOG

	assertEquals "Remote lookup should survive ssh joining the remote capability handshake into a shell string." "$remote_bin_dir/zfs" "$result"
	assertContains "The realistic ssh emulation should receive the expected remote shell command." \
		"$(cat "$realistic_ssh_log")" "command -v"
	assertContains "The realistic ssh emulation should include the requested tool name." \
		"$(cat "$realistic_ssh_log")" "zfs"
	assertContains "The realistic ssh emulation should also include the uname probe from the combined handshake." \
		"$(cat "$realistic_ssh_log")" "uname"
}

test_resolve_remote_required_tool_reports_remote_probe_failures() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_SUPPRESS_STDOUT=1
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	set +e
	result=$(exec_remote_hosts_test_resolve "backup@example.com" zfs "zfs" source)
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote lookup should fail when ssh cannot execute the probe." 1 "$status"
	assertEquals "Remote lookup failures should not be misreported as missing binaries." \
		"Failed to query dependency \"zfs\" on host backup@example.com." "$result"
}

test_resolve_remote_required_tool_reports_missing_remote_dependency() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_secure_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response_missing_zfs)
	export FAKE_SSH_STDOUT_OVERRIDE

	set +e
	result=$(exec_remote_hosts_test_resolve "backup@example.com" zfs "zfs" source)
	status=$?

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote lookup should fail when the secure PATH probe returns no result." 1 "$status"
	assertEquals "Missing remote tools should mention the secure PATH guidance." \
		"Required dependency \"zfs\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$result"
}

test_resolve_remote_required_tool_maps_missing_tool_from_capability_handshake_to_missing_dependency() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_secure_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response_missing_parallel)
	export FAKE_SSH_STDOUT_OVERRIDE

	set +e
	result=$(exec_remote_hosts_test_resolve "backup@example.com" parallel "GNU parallel" source)
	status=$?

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote lookup should treat handshake-reported missing tools as missing dependencies." 1 "$status"
	assertEquals "Handshake-reported missing tools should map to the user-facing missing dependency guidance." \
		"Required dependency \"GNU parallel\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$result"
}

test_resolve_remote_required_tool_rejects_relative_remote_path() {
	set +e
	result=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_run_remote_probe_script() {
				g_zxfer_remote_probe_stdout=zfs
			}
			exec_remote_hosts_test_resolve "backup@example.com" zfs "zfs" source
		)
	)
	status=$?

	assertEquals "Remote lookup should fail when the remote probe returns a non-absolute path." 1 "$status"
	assertEquals "Relative remote tool paths should be rejected explicitly." \
		"Required dependency \"zfs\" on host backup@example.com resolved to \"zfs\", but zxfer requires an absolute path." \
		"$result"
}

test_resolve_remote_required_tool_supports_remote_cat_from_handshake() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
	export FAKE_SSH_STDOUT_OVERRIDE

	result=$(exec_remote_hosts_test_resolve "backup@example.com" cat "cat" destination)

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote restore-mode cat lookups should reuse the combined capability handshake." \
		"/remote/bin/cat" "$result"
}

test_resolve_remote_required_tool_probes_tools_missing_from_the_handshake_directly() {
	fake_ssh="$TEST_TMPDIR/direct_probe_ssh"
	remote_log="$TEST_TMPDIR/direct_probe_ssh.log"
	# The last argument is the remote command. The capability probe answers
	# without a record for the tool; the direct probe prints a path or exits 10.
	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
for l_arg in "$@"; do
	l_remote_cmd=$l_arg
done
printf '%s\n' "$l_remote_cmd" >>"$FAKE_DIRECT_PROBE_LOG"
case $l_remote_cmd in
*ZXFER_REMOTE_CAPS_V2*)
	printf 'ZXFER_REMOTE_CAPS_V2\nos\tRemoteOS\ntool\tzfs\t0\t/remote/bin/zfs\nend\n'
	exit 0
	;;
esac
[ "$FAKE_DIRECT_PROBE_CASE" = found ] || exit 10
printf '%s\n' /remote/bin/lz4
EOF
	chmod +x "$fake_ssh"
	: >"$remote_log"
	g_cmd_ssh=$fake_ssh
	g_zxfer_secure_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_DIRECT_PROBE_LOG=$remote_log
	export FAKE_DIRECT_PROBE_LOG

	set +e
	found=$(
		FAKE_DIRECT_PROBE_CASE=found
		export FAKE_DIRECT_PROBE_CASE
		exec_remote_hosts_test_resolve "backup@example.com" lz4 "lz4" source
	)
	found_status=$?
	missing=$(
		FAKE_DIRECT_PROBE_CASE=missing
		export FAKE_DIRECT_PROBE_CASE
		exec_remote_hosts_test_resolve "backup@example.com" lz4 "lz4" source
	)
	missing_status=$?

	unset FAKE_DIRECT_PROBE_LOG

	assertEquals "A direct probe that finds the tool should succeed." 0 "$found_status"
	assertEquals "A direct probe should print the validated absolute path." \
		"/remote/bin/lz4" "$found"
	assertEquals "A direct probe that exits 10 should report a missing dependency." 1 "$missing_status"
	assertEquals "Exit 10 from the direct probe should map to the secure PATH guidance." \
		"Required dependency \"lz4\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$missing"
	assertContains "The direct probe should look the tool up by name." \
		"$(cat "$remote_log")" "command -v '\\''lz4'\\''"
}

test_get_os_handles_local_and_remote_invocations() {
	local_result=$(zxfer_get_os "")
	if remote_result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
			export FAKE_SSH_STDOUT_OVERRIDE
			zxfer_get_os "backup@example.com pfexec" source
		)
	); then
		remote_status=0
	else
		remote_status=$?
	fi
	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Local OS detection should match uname output." "$(uname)" "$local_result"
	assertEquals "Remote OS detection should succeed through the ssh helper path." 0 "$remote_status"
	assertEquals "Remote OS detection should execute uname through the ssh helper path." "RemoteOS" "$remote_result"
}

test_get_os_fails_when_the_remote_host_is_unreachable() {
	set +e
	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			FAKE_SSH_SUPPRESS_STDOUT=1
			FAKE_SSH_EXIT_STATUS=255
			export FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS
			zxfer_get_os "backup@example.com" destination 2>/dev/null
		)
	)
	status=$?

	assertEquals "Remote OS detection should fail when both probes fail." 1 "$status"
	assertEquals "Failed remote OS detection should not print a payload." "" "$result"
}

test_get_os_treats_local_ssh_path_as_literal() {
	marker="$TEST_TMPDIR/get_os_ssh_marker"
	old_cmd_ssh=${g_cmd_ssh:-}
	g_cmd_ssh="/bin/echo; touch $marker #"

	if zxfer_get_os "backup@example.com" >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi
	g_cmd_ssh=$old_cmd_ssh

	: "$status"
	assertFalse "Local ssh helper paths should not execute shell metacharacters during OS detection." \
		"[ -e '$marker' ]"
}
