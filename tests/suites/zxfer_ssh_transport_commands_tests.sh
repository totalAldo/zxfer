#!/bin/sh
# Remote-command tests for src/zxfer_ssh_transport.sh: host-spec parsing,
# zfs routing by role, the fail-closed ssh command modes, last-command
# recording, -V labels, and the ssh policy. Run by
# tests/test_zxfer_ssh_transport.sh under the exec fixture. The black-box
# pins live in tests/test_contract_remote.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Run one ssh command MODE (render, run, record or publish) for HOST_SPEC and
# REMOTE_CMD in a subshell whose zxfer_throw_error prints "throw: MESSAGE"
# and exits 3.
# Usage: zxfer_test_run_ssh_command_mode MODE HOST_SPEC REMOTE_CMD
zxfer_test_run_ssh_command_mode() {
	(
		zxfer_throw_error() {
			printf 'throw: %s\n' "$1"
			exit 3
		}
		if [ "$1" = run ]; then
			zxfer_invoke_ssh_shell_command_for_host "$2" "$3"
		elif [ "$1" = publish ]; then
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$2" "$3"
		else
			zxfer_ssh_shell_command_for_host "$1" "$2" "$3"
		fi
	)
}

test_parse_ssh_host_spec_splits_literal_tokens_and_rejects_shell_syntax() {
	# Each row: spec|status|ssh host|quoted wrapper. Tokens split on
	# whitespace without a shell parser, so ";" stays inside its token.
	while IFS='|' read -r l_spec l_expected_status l_expected_host l_expected_wrapper; do
		zxfer_parse_ssh_host_spec "$l_spec"
		l_status=$?
		assertEquals "[$l_spec] status" "$l_expected_status" "$l_status"
		assertEquals "[$l_spec] ssh host" "$l_expected_host" "$g_zxfer_ssh_shell_host_result"
		assertEquals "[$l_spec] wrapper" "$l_expected_wrapper" "$g_zxfer_ssh_wrapper_result"
	done <<'EOF'
user@host pfexec -p 2222|0|user@host|'pfexec' '-p' '2222'
backup.example.com; touch /tmp/pwn|0|backup.example.com;|'touch' '/tmp/pwn'
|0||
   |0||
user@host "ZFS Admin"|1||
user@host 'doas'|1||
user@host pf\exec|1||
EOF
	zxfer_parse_ssh_host_spec "user@host pfexec -p 2222"
	assertEquals "The raw tokens should be published one per line." \
		"$(printf '%s\n' user@host pfexec -p 2222)" "$g_zxfer_ssh_host_spec_tokens_result"
	zxfer_parse_ssh_host_spec 'user@host "ZFS Admin"'
	assertEquals "A rejected spec should explain the literal-token requirement." \
		"Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$g_zxfer_ssh_shell_context_error_result"
}

test_zfs_role_commands_run_each_role_where_its_zfs_lives() {
	l_local_zfs="$TEST_TMPDIR/role_local_zfs"
	printf '#!/bin/sh\nprintf "local %%s\\n" "$*"\n' >"$l_local_zfs"
	chmod +x "$l_local_zfs"
	l_ssh_log="$TEST_TMPDIR/role_ssh.log"
	l_out="$TEST_TMPDIR/role.out"
	: >"$l_ssh_log"
	FAKE_SSH_LOG=$l_ssh_log
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs=$l_local_zfs
	g_origin_cmd_zfs=/origin/sbin/zfs
	g_target_cmd_zfs=/target/usr/sbin/zfs
	g_option_O_origin_host=origin.example
	g_option_T_target_host=target.example
	g_option_V_very_verbose=1
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_get_calls=0

	# Each role runs its own zfs: over ssh for an -O/-T host, else directly.
	zxfer_run_zfs_cmd_for_role source get name tank/src >"$l_out" 2>/dev/null
	l_source="$?|$(tr '\n' ' ' <"$l_ssh_log")"
	: >"$l_ssh_log"
	zxfer_run_zfs_cmd_for_role destination get name tank/dst >"$l_out" 2>/dev/null
	l_destination="$?|$(tr '\n' ' ' <"$l_ssh_log")"
	: >"$l_ssh_log"
	g_option_T_target_host=""
	zxfer_run_zfs_cmd_for_role destination get name tank/dst >"$l_out"
	l_local_destination="$?|$(cat "$l_out")"
	zxfer_run_zfs_cmd_for_role local get name tank/other >"$l_out"
	l_local="$?|$(cat "$l_out")"
	zxfer_run_zfs_cmd_for_role /sbin/zfs get name tank/fs >"$l_out" 2>"$TEST_TMPDIR/role.err"
	l_unknown="$?|$(cat "$l_out" "$TEST_TMPDIR/role.err")"
	# Rendering routes the same way; the rendered -T command runs the same argv.
	g_option_T_target_host=target.example
	zxfer_render_zfs_command_for_role destination list -H backup/target
	zxfer_execute_rendered_shell_command "$g_zxfer_shell_command_result"
	l_rendered_destination=$(tr '\n' ' ' <"$l_ssh_log")
	zxfer_render_zfs_command_for_role local list -H backup/target
	l_rendered_local="$?|$g_zxfer_shell_command_result"
	l_rendered_unknown=$(zxfer_render_zfs_command_for_role /sbin/zfs list 2>&1)
	l_rendered_unknown="$?|$l_rendered_unknown"
	# An ssh command run without a side counts on the side of its host's role.
	g_zxfer_profile_source_ssh_shell_invocations=0
	zxfer_invoke_ssh_shell_command_for_host origin.example "'true'" 2>/dev/null
	l_sideless_ssh=$g_zxfer_profile_source_ssh_shell_invocations
	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "The source role should run the -O host's zfs over ssh." \
		"0|-o BatchMode=yes -o StrictHostKeyChecking=yes origin.example '/origin/sbin/zfs' 'get' 'name' 'tank/src' " \
		"$l_source"
	assertEquals "The destination role should run the -T host's zfs over ssh." \
		"0|-o BatchMode=yes -o StrictHostKeyChecking=yes target.example '/target/usr/sbin/zfs' 'get' 'name' 'tank/dst' " \
		"$l_destination"
	assertEquals "A destination without -T should run the local zfs directly." \
		"0|local get name tank/dst" "$l_local_destination"
	assertEquals "The local role should run the local zfs directly." \
		"0|local get name tank/other" "$l_local"
	assertEquals "An unknown role should fail closed, name the role and run nothing." \
		"1|zxfer: unknown zfs command role [/sbin/zfs]." "$l_unknown"
	assertEquals "A rendered -T command should run the -T host's zfs over ssh." \
		"-o BatchMode=yes -o StrictHostKeyChecking=yes target.example '/target/usr/sbin/zfs' 'list' '-H' 'backup/target' " \
		"$l_rendered_destination"
	assertEquals "The local role should render the local zfs argv." \
		"0|'$l_local_zfs' 'list' '-H' 'backup/target'" "$l_rendered_local"
	assertEquals "Rendering an unknown role should fail closed and name the role." \
		"1|zxfer: unknown zfs command role [/sbin/zfs]." "$l_rendered_unknown"
	assertEquals "-V should count each run on its role's side, local destinations included." \
		"source=1 destination=2 other=1 get=4" \
		"source=$g_zxfer_profile_source_zfs_calls destination=$g_zxfer_profile_destination_zfs_calls other=$g_zxfer_profile_other_zfs_calls get=$g_zxfer_profile_zfs_get_calls"
	assertEquals "-V should count a side-less ssh command to the -O host on the source side." \
		1 "$l_sideless_ssh"
}

test_zfs_dispatch_preserves_run_status_and_render_does_not_record_a_call() {
	output=$(
		(
			g_cmd_zfs=/bin/sh
			g_cmd_ssh=$FAKE_SSH_BIN
			g_option_V_very_verbose=1
			g_option_O_origin_host=""
			g_zxfer_profile_source_zfs_calls=0
			zxfer_run_source_zfs_cmd -c 'exit 23' >/dev/null
			printf 'local=%s\n' "$?"
			g_option_O_origin_host='origin.example doas'
			FAKE_SSH_EXIT_STATUS=37
			FAKE_SSH_SUPPRESS_STDOUT=1
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_SUPPRESS_STDOUT
			zxfer_run_source_zfs_cmd list tank/src >/dev/null 2>/dev/null
			printf 'remote=%s calls=%s\n' "$?" "$g_zxfer_profile_source_zfs_calls"
			zxfer_render_zfs_command_for_role source list tank/src
			printf 'render=%s calls=%s\n' "$?" "$g_zxfer_profile_source_zfs_calls"
		)
	)
	status=$?
	assertEquals "The dispatch check should complete normally." 0 "$status"
	assertEquals "Runs preserve local and ssh statuses; rendering adds no recorded call." \
		"local=23
remote=37 calls=2
render=0 calls=2" "$output"
}

test_ssh_command_modes_fail_closed_before_ssh_runs() {
	l_ssh_log="$TEST_TMPDIR/fail_closed_ssh.log"
	: >"$l_ssh_log"
	FAKE_SSH_LOG=$l_ssh_log
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	l_policy_message="throw: ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
	l_quoted_message="throw: Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
	l_results=""
	for l_mode in render run record publish; do
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
		export ZXFER_SSH_USER_KNOWN_HOSTS_FILE
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" backup.example "'zfs' 'list'" 2>&1)
		l_results="$l_results$l_mode policy=$?|$l_output
"
		unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" 'backup.example "pfexec -u zfs"' "'zfs' 'list'" 2>&1)
		l_results="$l_results$l_mode quoted=$?|$l_output
"
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" "" "'zfs' 'list'" 2>&1)
		l_results="$l_results$l_mode empty_host=$?|$l_output
"
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" backup.example "" 2>&1)
		l_results="$l_results$l_mode empty_command=$?|$l_output
"
	done
	g_zxfer_prepared_ssh_shell_command_result=stale
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw backup.example ""
	l_published="$?|$g_zxfer_prepared_ssh_shell_command_result"
	unset FAKE_SSH_LOG

	l_expected=""
	for l_mode in render run record publish; do
		l_expected="$l_expected$l_mode policy=3|$l_policy_message
$l_mode quoted=3|$l_quoted_message
$l_mode empty_host=1|
$l_mode empty_command=1|
"
	done
	assertEquals "Every mode should throw a bad policy or a host spec that needs shell quoting, and return 1 for an empty host or command." \
		"$l_expected" "$l_results"
	assertEquals "No failure may start ssh." "" "$(cat "$l_ssh_log")"
	assertEquals "An empty command should clear the published command." "1|" "$l_published"
}

test_zfs_and_ssh_runs_record_their_argv_as_the_last_command() {
	l_ssh_log="$TEST_TMPDIR/record_ssh.log"
	: >"$l_ssh_log"
	FAKE_SSH_LOG=$l_ssh_log
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs=/bin/echo
	g_option_O_origin_host=""

	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null
	l_safe=$g_zxfer_failure_last_command
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null
	l_local=$g_zxfer_failure_last_command
	g_option_O_origin_host=backup@example.com
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	zxfer_invoke_ssh_shell_command_for_host backup@example.com "zfs list -H tank/src" >/dev/null
	l_run=$g_zxfer_failure_last_command
	: >"$l_ssh_log"
	# Record mode stores the argv for a caller that runs ssh in a subshell.
	g_option_V_very_verbose=1
	g_zxfer_profile_source_ssh_shell_invocations=0
	zxfer_ssh_shell_command_for_host record "backup@example.com doas" "zfs list" source \
		2>"$TEST_TMPDIR/record.err"
	l_record="$?|$g_zxfer_failure_last_command"
	l_record_side_effects="ssh=<$(cat "$l_ssh_log")> count=$g_zxfer_profile_source_ssh_shell_invocations stderr=<$(cat "$TEST_TMPDIR/record.err")>"
	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS

	assertEquals "Safe report mode should redact the last command." "[redacted]" "$l_safe"
	assertEquals "Unsafe report mode should quote a local zfs argv." \
		"'/bin/echo' 'list' '-H' 'tank/src'" "$l_local"
	assertEquals "Unsafe report mode should keep every ssh token boundary." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock' 'backup@example.com' 'zfs list -H tank/src'" \
		"$l_run"
	assertEquals "Record mode should store the argv run mode would execute." \
		"0|'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup@example.com' ''\"'\"'doas'\"'\"' zfs list'" \
		"$l_record"
	assertEquals "Record mode should neither run ssh, count it nor print the -V line." \
		"ssh=<> count=0 stderr=<>" "$l_record_side_effects"
}

test_remote_command_context_label_names_the_role() {
	g_option_O_origin_host=origin.example
	g_option_T_target_host="target.example doas"
	# Each row: host spec|side|label. Without a side the -O/-T match decides.
	while IFS='|' read -r l_host l_side l_expected_label; do
		assertEquals "[$l_host|$l_side] label" "$l_expected_label" \
			"$(zxfer_get_remote_command_context_label "$l_host" "$l_side")"
	done <<'EOF'
origin.example|source|origin: origin.example
target.example doas|destination|target: target.example doas
misc.example|other|remote: misc.example
origin.example||origin: origin.example
target.example doas||target: target.example doas
misc.example||remote: misc.example
||remote
EOF
	g_option_T_target_host=origin.example
	assertEquals "A spec that is both -O and -T should be origin/target without a side." \
		"origin/target: origin.example" "$(zxfer_get_remote_command_context_label origin.example)"
	g_option_V_very_verbose=1
	assertEquals "-V should print the labeled command, quoted once." \
		"Running remote command [remote: misc.example doas]: '/bin/echo' 'hello'" \
		"$(zxfer_echoV_remote_command_for_host "misc.example doas" other /bin/echo hello 2>&1)"
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

test_zxfer_load_ssh_transport_policy_validates_each_variable() {
	nl='
'
	# ZXFER_SSH_USE_AMBIENT_CONFIG takes 1, yes, true or on in any case and
	# then ignores the other variables; any other value keeps the policy.
	for l_ambient in 1 yes YES True on; do
		ZXFER_SSH_USE_AMBIENT_CONFIG=$l_ambient
		ZXFER_SSH_BATCH_MODE="bad${nl}value"
		zxfer_load_ssh_transport_policy
		assertEquals "Ambient config [$l_ambient] should add no options." \
			"0:" "$?:$g_zxfer_ssh_policy_options"
	done
	unset ZXFER_SSH_BATCH_MODE
	for l_ambient in 0 no off; do
		ZXFER_SSH_USE_AMBIENT_CONFIG=$l_ambient
		zxfer_load_ssh_transport_policy
		assertEquals "[$l_ambient] should keep the managed default options." \
			"0:-o
BatchMode=yes
-o
StrictHostKeyChecking=yes" "$?:$g_zxfer_ssh_policy_options"
	done
	unset ZXFER_SSH_USE_AMBIENT_CONFIG

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

test_zxfer_prepare_ssh_transport_validates_the_policy_once_per_run() {
	output=$(
		(
			g_cmd_ssh=/usr/bin/ssh
			ZXFER_SSH_BATCH_MODE='bad
value'
			zxfer_prepare_ssh_transport
			printf 'invalid=%s ready=<%s> error=%s\n' "$?" \
				"$g_zxfer_ssh_transport_ready" "$g_zxfer_ssh_transport_error"
			ZXFER_SSH_BATCH_MODE=no
			zxfer_prepare_ssh_transport
			printf 'valid=%s ready=<%s>\n' "$?" "$g_zxfer_ssh_transport_ready"
			load_calls=0
			zxfer_load_ssh_transport_policy() {
				load_calls=$((load_calls + 1))
				return 1
			}
			zxfer_prepare_ssh_transport
			printf 'again=%s load_calls=%s\n' "$?" "$load_calls"
			zxfer_reset_ssh_transport_state
			zxfer_prepare_ssh_transport
			printf 'after_reset=%s load_calls=%s ready=<%s>\n' "$?" "$load_calls" \
				"$g_zxfer_ssh_transport_ready"
			zxfer_load_ssh_transport_policy() { return 0; }
			g_cmd_ssh=""
			g_zxfer_secure_path="$TEST_TMPDIR/no-ssh-here"
			zxfer_prepare_ssh_transport
			printf 'no_ssh=%s ready=<%s> error=%s\n' "$?" \
				"$g_zxfer_ssh_transport_ready" "$g_zxfer_ssh_transport_error"
		)
	)

	assertEquals "A failed policy or ssh lookup is checked again; the first success holds until the session reset." \
		"invalid=1 ready=<> error=ZXFER_SSH_BATCH_MODE must be a single-line non-empty value.
valid=0 ready=<1>
again=0 load_calls=0
after_reset=1 load_calls=1 ready=<>
no_ssh=1 ready=<> error=Required dependency \"ssh\" not found in secure PATH ($TEST_TMPDIR/no-ssh-here). Set ZXFER_SECURE_PATH or install the binary." "$output"
}
