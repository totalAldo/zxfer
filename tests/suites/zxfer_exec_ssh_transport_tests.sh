#!/bin/sh
# Tests for src/zxfer_ssh_transport.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_parse_ssh_host_spec_splits_host_and_wrapper_tokens() {
	# Host specs may append privilege wrappers like "pfexec" or ssh options.
	zxfer_parse_ssh_host_spec "user@host pfexec -p 2222"
	status=$?

	assertEquals "A literal host spec should parse." 0 "$status"
	assertEquals "The raw tokens should be published one per line." \
		"$(printf '%s\n' user@host pfexec -p 2222)" "$g_zxfer_ssh_host_spec_tokens_result"
	assertEquals "The first token should be the ssh host." "user@host" "$g_zxfer_ssh_shell_host_result"
	assertEquals "The wrapper tokens should be quoted one by one." \
		"'pfexec' '-p' '2222'" "$g_zxfer_ssh_wrapper_result"
}

test_parse_ssh_host_spec_rejects_shell_quotes_and_backslashes() {
	zxfer_parse_ssh_host_spec 'user@host "ZFS Admin"'
	status=$?

	assertEquals "Host-spec parsing should fail closed when the input requires shell quoting semantics." \
		1 "$status"
	assertEquals "Rejected host specs should explain the literal-token requirement." \
		"Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$g_zxfer_ssh_shell_context_error_result"
	assertEquals "A rejected host spec should publish no host." "" "$g_zxfer_ssh_shell_host_result"
}

test_parse_ssh_host_spec_keeps_metacharacters_inside_quoted_tokens() {
	# Characters such as semicolons end a token but never start a new command.
	zxfer_parse_ssh_host_spec "backup.example.com; touch /tmp/pwn"

	assertEquals "A semicolon should stay part of the host token." \
		"backup.example.com;" "$g_zxfer_ssh_shell_host_result"
	assertEquals "The remaining tokens should be quoted as wrapper arguments." \
		"'touch' '/tmp/pwn'" "$g_zxfer_ssh_wrapper_result"
}

test_parse_ssh_host_spec_publishes_nothing_for_blank_specs() {
	for blank_spec in "" "   "; do
		zxfer_parse_ssh_host_spec "$blank_spec"
		status=$?

		assertEquals "A blank host spec [$blank_spec] should parse." 0 "$status"
		assertEquals "A blank host spec [$blank_spec] should publish no host." "" "$g_zxfer_ssh_shell_host_result"
		assertEquals "A blank host spec [$blank_spec] should publish no wrapper." "" "$g_zxfer_ssh_wrapper_result"
	done
}

test_run_zfs_cmd_for_role_routes_destination_even_when_both_sides_share_one_zfs_path() {
	# Regression: dispatch used to compare the requested binary path against
	# the source zfs path first, so with zfs installed at the same path on both
	# hosts (the common case) every destination read silently ran through the
	# source runner and, under -T, against the local pool.
	log_file="$TEST_TMPDIR/role_destination_same_path.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_target_cmd_zfs="/sbin/zfs"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket=""

	zxfer_run_zfs_cmd_for_role destination get name tank/dst

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "Role destination should run over ssh on the -T host even when both sides share one zfs path." \
		"target.example
'/sbin/zfs' 'get' 'name' 'tank/dst'" "$(sed -n '5,6p' "$log_file")"
}

test_run_zfs_cmd_for_role_local_executes_configured_zfs_directly() {
	tool="$TEST_TMPDIR/echo_tool"
	cat >"$tool" <<'EOF'
#!/bin/sh
echo "$@"
EOF
	chmod +x "$tool"

	# shellcheck disable=SC2030,SC2031
	result=$(
		g_cmd_zfs=$tool
		zxfer_run_source_zfs_cmd() { printf 'source %s\n' "$1"; }
		zxfer_run_destination_zfs_cmd() { printf 'destination %s\n' "$1"; }
		zxfer_run_zfs_cmd_for_role local alpha beta
	)

	assertEquals "Role local should execute the configured zfs binary directly." "alpha beta" "$result"
}

test_run_zfs_cmd_for_role_rejects_unknown_roles_without_running_anything() {
	# shellcheck disable=SC2030,SC2031
	output=$(
		g_cmd_zfs=/bin/echo
		zxfer_run_zfs_cmd_for_role /sbin/zfs list tank/fs 2>&1
	)
	status=$?

	assertEquals "Unknown zfs command roles should fail closed." 1 "$status"
	assertEquals "Unknown zfs command roles should name the rejected role and reach neither runner." \
		"zxfer: unknown zfs command role [/sbin/zfs]." "$output"
}

test_run_zfs_cmd_for_role_local_tracks_other_profile_counter_when_very_verbose() {
	tool="$TEST_TMPDIR/profile_other_zfs"
	cat >"$tool" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$tool"

	g_option_V_very_verbose=1
	g_cmd_zfs=$tool
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_get_calls=0

	zxfer_run_zfs_cmd_for_role local get name tank/other >/dev/null

	assertEquals "Very-verbose profiling should count local-role zfs calls in the other bucket." \
		1 "$g_zxfer_profile_other_zfs_calls"
	assertEquals "Very-verbose profiling should still classify the local-role verb." \
		1 "$g_zxfer_profile_zfs_get_calls"
}

test_run_source_zfs_cmd_tracks_profile_counters_when_very_verbose() {
	tool="$TEST_TMPDIR/profile_source_zfs"
	cat >"$tool" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$tool"

	g_option_V_very_verbose=1
	g_zxfer_failure_stage="snapshot discovery"
	g_cmd_zfs="$tool"
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_bucket_source_inspection=0

	zxfer_run_source_zfs_cmd list tank/src >/dev/null

	assertEquals "Very-verbose profiling should count source-side zfs calls." \
		1 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Very-verbose profiling should count list verbs separately." \
		1 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Snapshot discovery source calls should contribute to the source-inspection bucket." \
		1 "$g_zxfer_profile_bucket_source_inspection"
}

test_invoke_ssh_shell_command_for_host_tracks_profile_counters_when_very_verbose() {
	FAKE_SSH_LOG="$TEST_TMPDIR/ssh_profile.log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0

	zxfer_invoke_ssh_shell_command_for_host "origin.example" "'/bin/true'" >/dev/null \
		2>/dev/null

	unset FAKE_SSH_LOG

	assertEquals "Very-verbose profiling should count ssh shell invocations." \
		1 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Very-verbose profiling should attribute origin-host ssh invocations to the source side." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_invoke_ssh_shell_command_for_host_tracks_explicit_profile_side_when_origin_and_target_match() {
	FAKE_SSH_LOG="$TEST_TMPDIR/ssh_profile_same_host.log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0

	zxfer_invoke_ssh_shell_command_for_host "shared.example" "'/bin/true'" source >/dev/null \
		2>/dev/null
	zxfer_invoke_ssh_shell_command_for_host "shared.example" "'/bin/true'" destination >/dev/null \
		2>/dev/null

	unset FAKE_SSH_LOG

	assertEquals "Explicit profile sides should still count total ssh invocations." \
		2 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Explicit source-side attribution should remain correct when origin and target share the same host spec." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
	assertEquals "Explicit destination-side attribution should remain correct when origin and target share the same host spec." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
}

test_invoke_ssh_shell_command_for_host_emits_very_verbose_remote_prefix() {
	log_file="$TEST_TMPDIR/invoke_cmd_verbose.log"
	stderr_file="$TEST_TMPDIR/invoke_cmd_verbose.err"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"

	zxfer_invoke_ssh_shell_command_for_host "backup@example.com pfexec" "zfs list -H tank/src" \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/origin.sock" "backup@example.com" \
		"'pfexec' zfs list -H tank/src")

	assertContains "Very-verbose ssh shell execution should prefix origin-host remote commands." \
		"$(cat "$stderr_file")" "Running remote command [origin: backup@example.com pfexec]:"
	assertContains "Very-verbose ssh shell execution should print the full rendered ssh command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_invoke_ssh_shell_command_for_host_skips_remote_render_when_quiet() {
	log_file="$TEST_TMPDIR/invoke_cmd_quiet.log"
	stderr_file="$TEST_TMPDIR/invoke_cmd_quiet.err"
	render_count_file="$TEST_TMPDIR/invoke_cmd_quiet.renders"
	: >"$log_file"
	printf '%s\n' 0 >"$render_count_file"

	(
		FAKE_SSH_LOG="$log_file"
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
		RENDER_COUNT_FILE="$render_count_file"
		zxfer_render_command_for_report() {
			printf '%s\n' 1 >>"$RENDER_COUNT_FILE"
			printf '%s\n' "rendered"
		}
		g_option_v_verbose=0
		g_option_V_very_verbose=0
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host="backup@example.com"
		g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
		zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "zfs list -H tank/src" \
			>/dev/null 2>"$stderr_file"
	)

	assertEquals "Quiet ssh shell execution should not render the remote command for display." \
		"0" "$(cat "$render_count_file")"
	assertEquals "Quiet ssh shell execution should emit no very-verbose output." \
		"" "$(cat "$stderr_file")"
	assertContains "Quiet ssh shell execution should still invoke the remote command." \
		"$(cat "$log_file")" "zfs list -H tank/src"
}

test_select_ssh_control_socket_reads_role_control_socket_at_call_time() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	zxfer_refresh_remote_zfs_commands
	zxfer_select_ssh_control_socket "target.example"
	before=$g_zxfer_ssh_control_socket_result
	g_ssh_target_control_socket="$TEST_TMPDIR/target-late.sock"
	zxfer_select_ssh_control_socket "target.example"
	after=$g_zxfer_ssh_control_socket_result
	zxfer_select_ssh_control_socket "origin.example"
	origin=$g_zxfer_ssh_control_socket_result

	assertEquals "No socket applies before the role opens one." "" "$before"
	assertEquals "A control socket opened after the host specs were parsed should apply." \
		"$TEST_TMPDIR/target-late.sock" "$after"
	assertEquals "The origin role should not borrow the target control socket." "" "$origin"
}

test_invoke_ssh_shell_command_for_host_honors_explicit_ambient_policy_opt_out() {
	log_file="$TEST_TMPDIR/invoke_cmd_ambient.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_shell_command_for_host "backup.example" "/bin/true"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' "-S" "$TEST_TMPDIR/origin.sock" "backup.example" "/bin/true")

	assertEquals "Ambient-policy opt-out should suppress zxfer-managed ssh -o options on the live invocation path while preserving control-socket reuse." \
		"$expected" "$(cat "$log_file")"
}

test_ssh_shell_command_render_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_ssh_shell_command_for_host render 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_ssh_host_spec_helpers_reject_invalid_literal_token_strings() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_ssh_shell_command_for_host render 'backup.example \"pfexec -u zfs\"' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."

	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_invoke_ssh_shell_command_for_host 'backup.example \"pfexec -u zfs\"' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command execution should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command execution should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_invoke_ssh_shell_command_for_host_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_invoke_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command execution should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command execution should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_invoke_ssh_shell_command_for_host_includes_explicit_known_hosts_override() {
	log_file="$TEST_TMPDIR/invoke_shell_known_hosts.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_shell_command_for_host "backup.example" "'sh' '-c' 'printf ok >/dev/null'"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' \
		"-o" "BatchMode=yes" \
		"-o" "StrictHostKeyChecking=yes" \
		"-o" "UserKnownHostsFile=$TEST_TMPDIR/known_hosts" \
		"backup.example" "'sh' '-c' 'printf ok >/dev/null'")

	assertEquals "ssh shell-command execution should pass the explicit managed known-hosts override through the live argv path." \
		"$expected" "$(cat "$log_file")"
}

test_invoke_ssh_shell_command_for_host_emits_explicit_very_verbose_remote_prefix() {
	log_file="$TEST_TMPDIR/invoke_shell_verbose.log"
	stderr_file="$TEST_TMPDIR/invoke_shell_verbose.err"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	zxfer_invoke_ssh_shell_command_for_host \
		"shared.example" "'sh' '-c' 'printf ok >/dev/null'" destination \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/target.sock" "shared.example" \
		"'sh' '-c' 'printf ok >/dev/null'")

	assertContains "Very-verbose ssh shell execution should honor the explicit target-side prefix when hosts match." \
		"$(cat "$stderr_file")" "Running remote command [target: shared.example]:"
	assertContains "Very-verbose ssh shell execution should print the full rendered ssh command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_run_source_zfs_cmd_uses_remote_ssh_when_origin_specified() {
	old_cmd_ssh=$g_cmd_ssh
	old_cmd_zfs=$g_cmd_zfs
	old_origin_cmd_zfs=${g_origin_cmd_zfs:-}
	old_origin_host=$g_option_O_origin_host

	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs="/usr/sbin/zfs"
	g_option_O_origin_host="backup@example.com pfexec -p 2222"
	g_ssh_origin_control_socket=""

	remote_log="$TEST_TMPDIR/zxfer_run_source_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_run_source_zfs_cmd list tank/fs@snap

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "ssh should force batch mode before connecting to the origin host." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the origin host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before connecting to the origin host." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the origin host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should target the origin host without literal quotes." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	arg_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Privilege wrappers must remain quoted inside the remote shell command." "$arg_remote_cmd" "'pfexec' '-p' '2222'"
	assertContains "zfs binary should use the origin-host path." "$arg_remote_cmd" "'$g_origin_cmd_zfs'"
	assertContains "Remote command should preserve requested subcommand." "$arg_remote_cmd" "'list'"
	assertContains "Dataset argument should remain a single remote-shell token." "$arg_remote_cmd" "'tank/fs@snap'"

	g_cmd_ssh=$old_cmd_ssh
	g_cmd_zfs=$old_cmd_zfs
	g_origin_cmd_zfs=$old_origin_cmd_zfs
	g_option_O_origin_host=$old_origin_host
}

test_run_source_zfs_cmd_uses_default_local_zfs_when_wrapper_is_unset() {
	fake_zfs="$TEST_TMPDIR/default_source_zfs"
	outfile="$TEST_TMPDIR/default_source_zfs.out"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$fake_zfs"
	g_option_O_origin_host=""
	g_cmd_zfs="$fake_zfs"

	zxfer_run_source_zfs_cmd list -H tank/src >"$outfile"

	assertEquals "The default local source path should execute the resolved zfs binary directly." \
		"list -H tank/src" "$(cat "$outfile")"
	assertEquals "Default local source execution should redact the last command." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_run_destination_zfs_cmd_uses_remote_ssh_when_target_specified() {
	old_cmd_ssh=$g_cmd_ssh
	old_cmd_zfs=$g_cmd_zfs
	old_target_cmd_zfs=${g_target_cmd_zfs:-}
	old_target_host=$g_option_T_target_host

	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_target_cmd_zfs="/usr/sbin/zfs"
	g_option_T_target_host="target@example.com doas"
	g_ssh_target_control_socket=""

	remote_log="$TEST_TMPDIR/zxfer_run_destination_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_run_destination_zfs_cmd get -H name tank/dst

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "ssh should force batch mode before connecting to the target host." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the target host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before connecting to the target host." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the target host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should connect to the target host without stray quotes." "target@example.com" "$(sed -n '5p' "$remote_log")"
	targ_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Additional host-spec tokens must survive inside the remote shell command." "$targ_remote_cmd" "'doas'"
	assertContains "Remote call should include the target-host zfs path." "$targ_remote_cmd" "'$g_target_cmd_zfs'"
	assertContains "Command verb should pass through untouched." "$targ_remote_cmd" "'get'"
	assertContains "Original flags should be preserved." "$targ_remote_cmd" "'-H'"
	assertContains "Property argument should pass through verbatim." "$targ_remote_cmd" "'name'"
	assertContains "Dataset argument should remain literal." "$targ_remote_cmd" "'tank/dst'"

	g_cmd_ssh=$old_cmd_ssh
	g_cmd_zfs=$old_cmd_zfs
	g_target_cmd_zfs=$old_target_cmd_zfs
	g_option_T_target_host=$old_target_host
}

test_run_destination_zfs_cmd_uses_default_local_zfs_when_wrapper_is_unset() {
	fake_zfs="$TEST_TMPDIR/default_dest_zfs"
	outfile="$TEST_TMPDIR/default_dest_zfs.out"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$fake_zfs"
	g_option_T_target_host=""
	g_cmd_zfs="$fake_zfs"

	zxfer_run_destination_zfs_cmd get name tank/dst >"$outfile"

	assertEquals "The default local destination path should execute the resolved zfs binary directly." \
		"get name tank/dst" "$(cat "$outfile")"
	assertEquals "Default local destination execution should redact the last command." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_run_source_zfs_cmd_records_local_command_in_unsafe_mode() {
	g_option_O_origin_host=""
	g_cmd_zfs="/bin/echo"
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null

	assertEquals "Direct local ZFS commands should be shell-quoted in the last-command field when unsafe mode is enabled." \
		"'/bin/echo' 'list' '-H' 'tank/src'" "$g_zxfer_failure_last_command"
}

test_invoke_ssh_shell_command_for_host_records_remote_command_in_unsafe_mode() {
	FAKE_SSH_STDOUT_OVERRIDE="ok"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "zfs list -H tank/src" >/dev/null

	assertEquals "Unsafe SSH command recording should preserve every token boundary." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock' 'backup@example.com' 'zfs list -H tank/src'" \
		"$g_zxfer_failure_last_command"
}

test_ssh_shell_command_for_host_record_mode_stores_the_argv_without_running_ssh() {
	output=$(
		(
			ssh_log="$TEST_TMPDIR/record_mode_ssh.log"
			: >"$ssh_log"
			FAKE_SSH_LOG=$ssh_log
			export FAKE_SSH_LOG
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_V_very_verbose=1
			g_zxfer_profile_ssh_shell_invocations=0
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			zxfer_ssh_shell_command_for_host record "backup@example.com doas" "zfs list" source
			printf 'status=%s\n' "$?"
			printf 'last=<%s>\n' "$g_zxfer_failure_last_command"
			printf 'ssh_count=%s\n' "$g_zxfer_profile_ssh_shell_invocations"
			printf 'ssh_log=<%s>\n' "$(cat "$ssh_log")"
		) 2>&1
	)

	assertContains "record mode should succeed." "$output" "status=0"
	assertContains "record mode should store the same argv that run mode executes." "$output" \
		"last=<'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup@example.com' ''"
	assertContains "record mode should not count an ssh invocation." "$output" "ssh_count=0"
	assertContains "record mode should not run ssh." "$output" "ssh_log=<>"
	assertNotContains "record mode should not print the -V remote command line." "$output" "Running remote command"
}

test_zxfer_remote_command_context_helpers_cover_remaining_role_labels() {
	output=$(
		(
			g_option_O_origin_host="shared.example"
			g_option_T_target_host="shared.example"
			printf 'other=%s\n' "$(zxfer_get_remote_command_context_label "other.example" other)"
			printf 'shared=%s\n' "$(zxfer_get_remote_command_context_label "shared.example")"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			printf 'target=%s\n' "$(zxfer_get_remote_command_context_label "target.example")"
			g_option_V_very_verbose=1
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_echoV_remote_command_for_host "misc.example doas" other /bin/echo hello
		)
	)

	assertContains "Remote command context labels should render the explicit other profile side as remote." \
		"$output" "other=remote: other.example"
	assertEquals "Remote command context labels should fall back to a bare remote label when no host is provided." \
		"remote" "$(zxfer_get_remote_command_context_label "")"
	assertContains "Remote command context labels should render shared origin and target hosts as origin/target." \
		"$output" "shared=origin/target: shared.example"
	assertContains "Remote command context labels should infer the target role when only the target host matches." \
		"$output" "target=target: target.example"
	assertContains "Very-verbose remote command rendering should include the resolved remote context label." \
		"$output" "Running remote command [remote: misc.example doas]: '/bin/echo' 'hello'"
}

test_zxfer_echoV_remote_command_for_host_covers_current_shell_render_path() {
	trace_file="$TEST_TMPDIR/echoV_remote_command_current_shell.log"

	(
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example doas"
		g_option_V_very_verbose=1
		zxfer_echoV() {
			printf '%s\n' "$*" >"$trace_file"
		}
		zxfer_echoV_remote_command_for_host "target.example doas" "" /bin/echo current-shell
	)

	assertEquals "Very-verbose remote command rendering should keep the current-shell target-context path shell-quoted exactly once." \
		"Running remote command [target: target.example doas]: '/bin/echo' 'current-shell'" \
		"$(cat "$trace_file")"
}

test_zxfer_render_destination_zfs_command_uses_remote_target_tool_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="backup@example.com"
	g_target_cmd_zfs="/remote/bin/zfs"

	rendered=$(zxfer_render_destination_zfs_command list -H backup/target)

	assertContains "Remote destination zfs rendering should route through ssh." \
		"$rendered" "'$FAKE_SSH_BIN'"
	assertContains "Remote destination zfs rendering should target the configured host." \
		"$rendered" "'backup@example.com'"
	assertContains "Remote destination zfs rendering should mention the resolved remote zfs path." \
		"$rendered" "/remote/bin/zfs"
	assertContains "Remote destination zfs rendering should preserve the requested subcommand and dataset." \
		"$rendered" "backup/target"
}

test_zxfer_render_zfs_command_for_role_routes_destination_and_local_commands() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="backup@example.com"
	g_target_cmd_zfs="/remote/bin/zfs"
	# Same zfs path on both sides: routing must still be decided by role.
	g_cmd_zfs="/local/bin/zfs"

	zxfer_render_zfs_command_for_role destination list -H backup/target
	destination_rendered=$g_zxfer_shell_command_result
	zxfer_render_zfs_command_for_role local list -H backup/target
	local_rendered=$g_zxfer_shell_command_result
	unknown_rendered=$(zxfer_render_zfs_command_for_role /local/bin/zfs list -H backup/target 2>&1)
	unknown_status=$?

	assertContains "Role destination should reuse the destination render helper even when both sides share one zfs path." \
		"$destination_rendered" "/remote/bin/zfs"
	assertContains "Role destination should preserve the requested dataset argument." \
		"$destination_rendered" "backup/target"
	assertEquals "Role local should render the configured zfs binary as direct shell-quoted argv." \
		"'/local/bin/zfs' 'list' '-H' 'backup/target'" "$local_rendered"
	assertEquals "Unknown roles should fail closed instead of rendering a guessed command." \
		1 "$unknown_status"
	assertEquals "Unknown roles should name the rejected role." \
		"zxfer: unknown zfs command role [/local/bin/zfs]." "$unknown_rendered"
}

test_ssh_shell_command_render_quotes_control_socket_path_for_eval() {
	marker_rel="control_socket_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	log_file="$TEST_TMPDIR/control_socket_eval.log"
	socket_path="$TEST_TMPDIR/socket.\$(touch $marker_rel)"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	: >"$log_file"
	rm -f "$marker"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$socket_path"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_ssh_shell_command_for_host render "backup@example.com" "$safe_cmd"
	cmd=$g_zxfer_shell_command_result
	(
		cd "$TEST_TMPDIR" || exit 1
		zxfer_execute_rendered_shell_command "$cmd"
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertFalse "Control-socket paths should stay literal when ssh commands are eval-rendered." \
		"[ -e '$marker' ]"
	assertEquals "Rendered ssh commands should pass the control socket as a single argv token." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$socket_path
backup@example.com
'sh' '-c' 'printf ok >/dev/null'" "$(cat "$log_file")"
}

test_build_remote_sh_c_command_preserves_multiline_scripts_as_one_c_argument() {
	log_file="$TEST_TMPDIR/remote_sh_multiline.log"
	ssh_bin="$TEST_TMPDIR/fake_ssh_join_multiline"
	create_fake_ssh_join_exec_bin "$ssh_bin"
	: >"$log_file"
	g_cmd_ssh="$ssh_bin"
	g_option_O_origin_host="backup@example.com"
	FAKE_SSH_LOG="$log_file"
	export FAKE_SSH_LOG

	remote_cmd=$(zxfer_build_remote_sh_c_command "l_value=ok
printf '%s\n' \"\$l_value\"")
	output=$(zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "$remote_cmd")

	unset FAKE_SSH_LOG

	assertEquals "Remote sh -c builders should preserve multiline scripts as one command argument." \
		"ok" "$output"
	assertContains "Remote sh -c builders should still target the requested host." \
		"$(cat "$log_file")" "backup@example.com"
	assertContains "Remote sh -c builders should keep the entire multiline script inside the single -c payload." \
		"$(cat "$log_file")" "l_value=ok"
}

test_publish_prepared_ssh_shell_command_wraps_only_wrapper_hosts() {
	log_file="$TEST_TMPDIR/prepared_ssh_shell_command.log"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw \
		"backup@example.com" "zfs list tank/src | cat"
	simple_status=$?
	: >"$log_file"
	zxfer_execute_rendered_shell_command "$g_zxfer_prepared_ssh_shell_command_result"
	simple_argv=$(cat "$log_file")

	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw \
		"backup@example.com pfexec -p 2222" "zfs list tank/src | cat"
	wrapped_status=$?
	: >"$log_file"
	zxfer_execute_rendered_shell_command "$g_zxfer_prepared_ssh_shell_command_result"
	wrapped_argv=$(cat "$log_file")
	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Simple host specs should publish successfully." 0 "$simple_status"
	assertEquals "Simple host specs should pass the remote command as one ssh argument." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
backup@example.com
zfs list tank/src | cat" "$simple_argv"
	assertEquals "Wrapper host specs should publish successfully." 0 "$wrapped_status"
	assertEquals "Wrapper host specs should run the whole remote pipeline under the wrapper through sh -c." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
backup@example.com
'pfexec' '-p' '2222' 'sh' '-c' 'zfs list tank/src | cat'" "$wrapped_argv"
}

test_publish_prepared_ssh_shell_command_rejects_quoted_host_specs_and_empty_commands() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_publish_prepared_ssh_shell_command_for_host_or_throw \
			'backup.example \"pfexec -u zfs\"' 'zfs list'
	"
	quoted_status=$ZXFER_TEST_CAPTURE_STATUS
	quoted_output=$ZXFER_TEST_CAPTURE_OUTPUT

	g_zxfer_prepared_ssh_shell_command_result=stale
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "backup.example" ""
	empty_status=$?

	assertEquals "Host specs that need shell quoting should throw." 1 "$quoted_status"
	assertContains "Rejected host specs should keep the literal-token message." \
		"$quoted_output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only"
	assertEquals "An empty remote command should return 1 without throwing." 1 "$empty_status"
	assertEquals "An empty remote command should clear the published command." \
		"" "$g_zxfer_prepared_ssh_shell_command_result"
}

test_prepare_ssh_shell_command_context_extracts_host_and_wrapper_command() {
	zxfer_prepare_ssh_shell_command_context "backup@example.com pfexec -u root" "'sh' '-c' 'zfs list tank/src'"
	status=$?

	assertEquals "SSH shell context preparation should succeed for wrapper host specs." \
		0 "$status"
	assertEquals "SSH shell context preparation should publish the first host-spec token as the ssh host." \
		"backup@example.com" "$g_zxfer_ssh_shell_host_result"
	assertEquals "SSH shell context preparation should prefix the remote command with safely quoted wrapper tokens." \
		"'pfexec' '-u' 'root' 'sh' '-c' 'zfs list tank/src'" "$g_zxfer_ssh_shell_full_remote_command_result"
}

test_ssh_shell_command_helpers_return_one_for_an_empty_host_spec() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	log_file="$TEST_TMPDIR/empty_host_spec.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	export FAKE_SSH_LOG

	build_output=$(zxfer_ssh_shell_command_for_host render "" "zfs list tank/src")
	build_status=$?
	zxfer_invoke_ssh_shell_command_for_host "" "zfs list tank/src" source >/dev/null
	invoke_status=$?

	unset FAKE_SSH_LOG
	assertEquals "Rendering for an empty host spec should return 1." 1 "$build_status"
	assertEquals "Rendering for an empty host spec should print nothing." "" "$build_output"
	assertEquals "Running for an empty host spec should return 1." 1 "$invoke_status"
	assertEquals "Running for an empty host spec should never start ssh." "" "$(cat "$log_file")"
}

test_ssh_shell_command_render_honors_explicit_ambient_policy_opt_out() {
	log_file="$TEST_TMPDIR/build_shell_ambient.log"
	socket_path="$TEST_TMPDIR/ambient.sock"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	: >"$log_file"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$socket_path"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_ssh_shell_command_for_host render "backup@example.com" "$safe_cmd"
	cmd=$g_zxfer_shell_command_result
	zxfer_execute_rendered_shell_command "$cmd"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Ambient-policy opt-out should suppress managed ssh -o options in shell-command rendering while preserving control-socket reuse." \
		"-S
$socket_path
backup@example.com
'sh' '-c' 'printf ok >/dev/null'" "$(cat "$log_file")"
}

test_ssh_shell_command_render_fuzzes_wrapper_specs_and_control_socket_paths() {
	marker_rel="control_socket_fuzz_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	case_file="$TEST_TMPDIR/control_socket_fuzz_cases.txt"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	cat >"$case_file" <<EOF
backup@example.com doas|$TEST_TMPDIR/socket,comma
backup@example.com pfexec -u root|$TEST_TMPDIR/socket=equals
backup@example.com env LC_ALL=C doas|$TEST_TMPDIR/socket:semicolon;literal
backup@example.com doas|$TEST_TMPDIR/socket.\$(touch $marker_rel)
EOF

	case_index=0
	while IFS='|' read -r host_spec socket_path || [ -n "$host_spec$socket_path" ]; do
		[ -n "$host_spec" ] || continue
		case_index=$((case_index + 1))
		log_file="$TEST_TMPDIR/control_socket_fuzz_$case_index.log"
		: >"$log_file"
		rm -f "$marker"
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host=$host_spec
		g_ssh_origin_control_socket=$socket_path
		FAKE_SSH_LOG="$log_file"
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		zxfer_ssh_shell_command_for_host render "$host_spec" "$safe_cmd"
		cmd=$g_zxfer_shell_command_result
		(
			cd "$TEST_TMPDIR" || exit 1
			zxfer_execute_rendered_shell_command "$cmd"
		)

		unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		assertFalse "Control-socket fuzz case $case_index should not execute command substitutions from the socket path." \
			"[ -e '$marker' ]"
		assertEquals "Control-socket fuzz case $case_index should force batch mode first." "-o" "$(sed -n '1p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass BatchMode=yes as the first managed transport option." "BatchMode=yes" "$(sed -n '2p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should force strict host-key checking next." "-o" "$(sed -n '3p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass StrictHostKeyChecking=yes as the second managed transport option." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass -S separately." "-S" "$(sed -n '5p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should preserve the literal control-socket path." \
			"$socket_path" "$(sed -n '6p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should keep the ssh host token separate from wrappers." \
			"backup@example.com" "$(sed -n '7p' "$log_file")"
		log_line_remote_cmd=$(sed -n '8p' "$log_file")
		assertContains "Control-socket fuzz case $case_index should preserve the quoted remote command payload." \
			"$log_line_remote_cmd" "'sh' '-c' 'printf ok >/dev/null'"

		case "$host_spec" in
		*" doas"*)
			assertContains "Control-socket fuzz case $case_index should keep doas in the remote wrapper chain." \
				"$log_line_remote_cmd" "'doas'"
			;;
		esac
		case "$host_spec" in
		*"pfexec -u root"*)
			assertContains "Control-socket fuzz case $case_index should keep pfexec wrapper tokens quoted." \
				"$log_line_remote_cmd" "'pfexec' '-u' 'root'"
			;;
		esac
		case "$host_spec" in
		*"LC_ALL=C doas"*)
			assertContains "Control-socket fuzz case $case_index should keep env-style wrapper tokens quoted." \
				"$log_line_remote_cmd" "'env' 'LC_ALL=C' 'doas'"
			;;
		esac
	done <"$case_file"
}

test_zxfer_load_ssh_transport_policy_covers_ambient_managed_and_invalid_options() {
	nl='
'
	zxfer_test_capture_subshell "
		ZXFER_SSH_USE_AMBIENT_CONFIG=1
		zxfer_load_ssh_transport_policy
		printf '<%s>' \"\$g_zxfer_ssh_policy_options\"
	"
	ambient="$ZXFER_TEST_CAPTURE_STATUS:$ZXFER_TEST_CAPTURE_OUTPUT"
	zxfer_test_capture_subshell "
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=/etc/zxfer/known_hosts
		zxfer_load_ssh_transport_policy
		printf '%s' \"\$g_zxfer_ssh_policy_options\"
	"
	managed="$ZXFER_TEST_CAPTURE_STATUS:$ZXFER_TEST_CAPTURE_OUTPUT"

	assertEquals "Ambient ssh policy should add no options." "0:<>" "$ambient"
	assertEquals "Managed ssh policy should publish its options one per line." \
		"0:-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-o
UserKnownHostsFile=/etc/zxfer/known_hosts" "$managed"

	for invalid_case in \
		"ZXFER_SSH_BATCH_MODE|bad${nl}value|ZXFER_SSH_BATCH_MODE must be a single-line non-empty value." \
		"ZXFER_SSH_STRICT_HOST_KEY_CHECKING|bad${nl}policy|ZXFER_SSH_STRICT_HOST_KEY_CHECKING must be a single-line non-empty value." \
		"ZXFER_SSH_USER_KNOWN_HOSTS_FILE|/bad${nl}path|ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be a single-line non-empty value." \
		"ZXFER_SSH_USER_KNOWN_HOSTS_FILE|relative/known_hosts|ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."; do
		invalid_name=${invalid_case%%|*}
		invalid_rest=${invalid_case#*|}
		invalid_value=${invalid_rest%%|*}
		invalid_message=${invalid_rest#*|}
		ZXFER_SSH_BATCH_MODE=yes
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING=yes
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=""
		case $invalid_name in
		ZXFER_SSH_BATCH_MODE) ZXFER_SSH_BATCH_MODE=$invalid_value ;;
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING) ZXFER_SSH_STRICT_HOST_KEY_CHECKING=$invalid_value ;;
		*) ZXFER_SSH_USER_KNOWN_HOSTS_FILE=$invalid_value ;;
		esac
		zxfer_load_ssh_transport_policy
		status=$?
		assertEquals "Invalid $invalid_name values should fail the policy closed." 1 "$status"
		assertEquals "Invalid $invalid_name values should explain the problem." \
			"$invalid_message" "$g_zxfer_ssh_policy_error"
		assertEquals "Invalid $invalid_name values should publish no options." \
			"" "$g_zxfer_ssh_policy_options"
	done
	unset ZXFER_SSH_BATCH_MODE ZXFER_SSH_STRICT_HOST_KEY_CHECKING ZXFER_SSH_USER_KNOWN_HOSTS_FILE
}
