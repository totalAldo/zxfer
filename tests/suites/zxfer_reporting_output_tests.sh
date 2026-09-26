#!/bin/sh
# Operator-output tests for src/zxfer_reporting.sh: the -v/-V printers, beep,
# report quoting, failure-report rendering, launcher usage errors and the
# throw helpers. Run by tests/test_zxfer_reporting.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_echov_outputs_only_when_verbose_enabled() {
	# zxfer_echov should emit text only when -v/--verbose is set.
	g_option_v_verbose=1
	output=$(zxfer_echov "verbose message")

	assertEquals "verbose message" "$output"

	g_option_v_verbose=0
	output=$(zxfer_echov "hidden message")

	assertEquals "" "$output"
}

test_echoV_outputs_only_when_very_verbose_enabled() {
	# zxfer_echoV uses -V/--very-verbose, so it should stay quiet unless the
	# highest verbosity level is requested.
	g_option_V_very_verbose=1
	output=$(zxfer_echoV "debug message" 2>&1)

	assertEquals "debug message" "$output"

	g_option_V_very_verbose=0
	output=$(zxfer_echoV "hidden debug" 2>&1)

	assertEquals "" "$output"
}

test_echov_and_echoV_print_backslash_escapes_as_written() {
	# echo in dash and macOS /bin/sh expands \033 and \n and stops at \c; the
	# verbose printers must print the text as written plus one newline.
	l_text='lit=\033[31m \c cut \n \\ \x1B end'
	g_option_v_verbose=1
	g_option_V_very_verbose=1

	assertEquals "zxfer_echov prints backslashes as written." "$l_text
." "$(
		zxfer_echov "$l_text"
		printf '.'
	)"
	assertEquals "zxfer_echoV prints backslashes as written." "$l_text
." "$(
		{
			zxfer_echoV "$l_text"
			printf '.' >&2
		} 2>&1
	)"
	assertEquals "A lone -n is text, not an echo option." "-n" "$(zxfer_echov -n)"
}

test_echoV_escaped_prints_escaped_value_only_when_very_verbose() {
	l_value=$(printf 'ok\033[2J\033]0;x\007 lit=\\033 \\c cut\r\nforged: line')

	g_option_V_very_verbose=0
	assertEquals "zxfer_echoV_escaped stays quiet without -V." \
		"" "$(zxfer_echoV_escaped label "$l_value" 2>&1)"

	g_option_V_very_verbose=1
	assertEquals "Control bytes and backslashes print escaped on one line." \
		'label: ok\x1B[2J\x1B]0;x\x07 lit=\\033 \\c cut\r\nforged: line' \
		"$(zxfer_echoV_escaped label "$l_value" 2>&1)"
	assertEquals "An empty value keeps the label and separator." \
		"label: " "$(zxfer_echoV_escaped label "" 2>&1)"
	assertEquals "zxfer_echoV_escaped writes to stderr only." \
		"" "$(zxfer_echoV_escaped label "$l_value" 2>/dev/null)"
}

test_beep_skips_on_non_freebsd_hosts() {
	output=$(
		(
			uname() {
				printf '%s\n' "Linux"
			}
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "Non-FreeBSD hosts should skip beep handling with a debug message." \
		"$output" "Beep requested but unsupported on Linux; skipping."
}

test_beep_skips_when_speaker_device_is_missing() {
	output=$(
		(
			uname() {
				printf '%s\n' "FreeBSD"
			}
			kldstat() {
				printf '%s\n' "speaker.ko"
			}
			kldload() {
				return 0
			}
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without /dev/speaker should skip beep handling with a debug message." \
		"$output" "Beep requested but /dev/speaker missing; skipping."
}

test_beep_skips_when_speaker_tools_are_missing() {
	fake_bin_dir="$TEST_TMPDIR/no_speaker_tools"
	fake_uname_bin="$fake_bin_dir/uname"
	mkdir -p "$fake_bin_dir"
	cat >"$fake_uname_bin" <<'EOF'
#!/bin/sh
printf '%s\n' "FreeBSD"
EOF
	chmod +x "$fake_uname_bin"

	output=$(
		(
			PATH="$fake_bin_dir"
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without speaker helper tools should skip beep handling with a debug message." \
		"$output" "Beep requested but speaker tools are missing; skipping."
}

test_zxfer_quote_command_argv_escapes_control_chars_and_apostrophes() {
	l_newline_arg=$(printf 'line1\nline2')

	result=$(zxfer_quote_command_argv "./zxfer" "value with space" "$l_newline_arg" "apost'rophe")
	expected="'./zxfer' 'value with space' 'line1\\nline2' 'apost'\"'\"'rophe'"

	assertEquals "Quoted argv should remain one-line and shell-safe for reports." "$expected" "$result"
}

test_zxfer_render_command_for_report_appends_quoted_argv_to_prefix() {
	result=$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs" "create" "-o" "compression=lz4")
	expected="/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4'"

	assertEquals "Report rendering should preserve the shell-ready prefix and quote appended argv tokens." \
		"$expected" "$result"
}

test_zxfer_render_command_for_report_returns_prefix_when_no_argv_are_provided() {
	result=$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs")

	assertEquals "Report rendering should return the prefix unchanged when no argv tokens are appended." \
		"/usr/bin/ssh 'host' /sbin/zfs" "$result"
}

test_zxfer_render_command_for_report_quotes_argv_when_prefix_is_empty() {
	result=$(zxfer_render_command_for_report "" "zfs" "list" "tank/src")

	assertEquals "Report rendering should still quote argv tokens when no shell prefix is supplied." \
		"'zfs' 'list' 'tank/src'" "$result"
}

test_zxfer_render_failure_report_includes_context_fields() {
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=1
	g_option_Y_yield_iterations=8
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_zxfer_original_invocation="'./zxfer' '-R' 'tank/src' 'backup/dst'"
	g_zxfer_failure_class="runtime"
	g_zxfer_failure_stage="send/receive"
	g_zxfer_failure_message="replication failed"
	g_zxfer_failure_source_root="tank/src"
	g_zxfer_failure_current_source="tank/src/child"
	g_zxfer_failure_destination_root="backup/dst"
	g_zxfer_failure_current_destination="backup/dst/child"
	g_zxfer_failure_last_command="'/sbin/zfs' 'send' 'tank/src@snap1'"

	report=$(zxfer_render_failure_report 1)

	assertContains "$report" "zxfer: failure report begin"
	assertContains "$report" "failure_stage: send/receive"
	assertContains "$report" "source_root: tank/src"
	assertContains "$report" "current_destination: backup/dst/child"
	assertContains "$report" "invocation: [redacted]"
	assertContains "$report" "last_command: [redacted]"
	assertContains "$report" "zxfer: failure report end"
}

test_zxfer_usage_error_failure_report_redacts_invocation_by_default() {
	secure_path_dir="$TEST_TMPDIR/usage_redaction_secure_path"
	stdout_file="$TEST_TMPDIR/usage_redaction.stdout"
	stderr_file="$TEST_TMPDIR/usage_redaction.stderr"
	secret_source="tank/secret-source"

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -R "$secret_source" >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Usage-error launcher runs should still exit with usage status when failure-report command redaction is enabled by default." \
		2 "$status"
	assertContains "Default failure-report command redaction should replace the launcher-captured invocation in stderr." \
		"$(cat "$stderr_file")" "invocation: [redacted]"
	assertNotContains "Default failure-report command redaction should keep secret-bearing usage arguments out of stderr." \
		"$(cat "$stderr_file")" "$secret_source"
}

test_zxfer_usage_error_failure_report_escapes_control_bytes_in_invocation_in_unsafe_mode() {
	secure_path_dir="$TEST_TMPDIR/usage_escape_secure_path"
	stdout_file="$TEST_TMPDIR/usage_escape.stdout"
	stderr_file="$TEST_TMPDIR/usage_escape.stderr"
	esc=$(printf '\033')
	bell=$(printf '\007')
	control_source=$(printf 'tank/ctrl%s[31m%s' "$esc" "$bell")

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "$control_source" >"$stdout_file" 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/ctrl\\x1B[31m\\x07'" "$stderr_file" >/dev/null 2>&1
	escaped_esc_status=$?
	grep -F "\\\\x1B" "$stderr_file" >/dev/null 2>&1
	double_esc_status=$?
	grep -F "\\x07" "$stderr_file" >/dev/null 2>&1
	escaped_bell_status=$?
	grep -F "$esc" "$stderr_file" >/dev/null 2>&1
	raw_esc_status=$?
	grep -F "$bell" "$stderr_file" >/dev/null 2>&1
	raw_bell_status=$?

	assertEquals "Unsafe usage-error launcher runs should still exit with usage status when invocation control bytes are escaped." \
		2 "$status"
	assertEquals "Unsafe failure reports should render ESC bytes from the launcher-captured invocation as escaped text." \
		0 "$escaped_esc_status"
	assertEquals "Unsafe failure reports should not double-escape control-byte markers from the launcher-captured invocation." \
		1 "$double_esc_status"
	assertEquals "Unsafe failure reports should render BEL bytes from the launcher-captured invocation as escaped text." \
		0 "$escaped_bell_status"
	assertEquals "Unsafe failure reports should not contain raw ESC bytes from the launcher-captured invocation." \
		1 "$raw_esc_status"
	assertEquals "Unsafe failure reports should not contain raw BEL bytes from the launcher-captured invocation." \
		1 "$raw_bell_status"
}

test_zxfer_usage_error_failure_report_preserves_trailing_newline_in_invocation_in_unsafe_mode() {
	secure_path_dir="$TEST_TMPDIR/usage_trailing_newline_secure_path"
	stdout_file="$TEST_TMPDIR/usage_trailing_newline.stdout"
	stderr_file="$TEST_TMPDIR/usage_trailing_newline.stderr"
	trailing_source=$(printf 'tank/trailing-source\n_')
	trailing_source=${trailing_source%_}

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "$trailing_source" >"$stdout_file" 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/trailing-source\\n'" "$stderr_file" >/dev/null 2>&1
	trailing_newline_status=$?

	assertEquals "Unsafe usage-error launcher runs should still exit with usage status when invocation newline markers are preserved." \
		2 "$status"
	assertEquals "Unsafe failure reports should preserve trailing newline markers from the launcher-captured invocation." \
		0 "$trailing_newline_status"
}

test_zxfer_render_failure_report_omits_empty_optional_fields() {
	g_zxfer_version="test-version"
	g_zxfer_failure_class="runtime"
	g_zxfer_failure_stage="snapshot discovery"
	g_zxfer_failure_message="missing snapshot"

	report=$(zxfer_render_failure_report 3)

	assertContains "$report" "failure_stage: snapshot discovery"
	assertNotContains "$report" "current_source:"
	assertNotContains "$report" "current_destination:"
	assertNotContains "$report" "invocation:"
}

test_zxfer_render_failure_report_defaults_runtime_class_for_nonusage_exit() {
	report_file="$TEST_TMPDIR/runtime_default.report"
	zxfer_reset_failure_context "unit-test"
	g_zxfer_failure_class=""
	g_zxfer_failure_message=""
	g_zxfer_failure_stage=""

	zxfer_render_failure_report 1 >"$report_file"

	report=$(cat "$report_file")
	assertContains "Non-usage exits should default to runtime failures." \
		"$report" "failure_class: runtime"
	assertContains "Missing failure messages should fall back to the exit status summary." \
		"$report" "message: zxfer exited with status 1."
}

test_zxfer_render_failure_defaults_cover_usage_mode() {
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive="tank/src"
	zxfer_reset_failure_context "unit"

	report=$(zxfer_render_failure_report 2)

	assertContains "Failure reports should default exit status 2 to usage errors." \
		"$report" "failure_class: usage"
	assertContains "Failure reports should default missing messages to the exit-status text." \
		"$report" "message: zxfer exited with status 2."
	assertContains "Failure reports should identify nonrecursive mode when -N is set." \
		"$report" "mode: nonrecursive"
}

test_throw_error_writes_message_to_stderr() {
	stdout_file="$TEST_TMPDIR/zxfer_throw_error.stdout"
	stderr_file="$TEST_TMPDIR/zxfer_throw_error.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error "boom" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_error should preserve the requested exit status." 3 "$status"
	assertEquals "zxfer_throw_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "boom"
}

test_throw_usage_error_defaults_to_the_usage_status() {
	stdout_file="$TEST_TMPDIR/throw_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_usage_error "bad option"
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_usage_error should exit with usage status 2." 2 "$status"
	assertEquals "zxfer_throw_usage_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: bad option"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}

test_throw_error_with_usage_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/zxfer_throw_error_with_usage.stdout"
	stderr_file="$TEST_TMPDIR/zxfer_throw_error_with_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error_with_usage "boom with usage" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_error_with_usage should preserve the requested exit status." 3 "$status"
	assertEquals "zxfer_throw_error_with_usage should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: boom with usage"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}
