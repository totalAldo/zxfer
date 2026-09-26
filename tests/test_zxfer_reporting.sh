#!/bin/sh
#
# shunit2 tests for the structured failure reports, verbose printers and
# failure-context helpers in src/zxfer_reporting.sh. The ZXFER_ERROR_LOG
# mirror is tested in tests/test_zxfer_error_log.sh.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# The usage-error cases run the launcher against a stand-in secure PATH.
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

zxfer_source_runtime_modules_through "zxfer_error_log.sh"

# zxfer_throw_usage_error prints this after its message; the usage cases look
# for "usage: zxfer" or "usage output".
zxfer_usage() {
	printf '%s\n' "usage: zxfer (usage output)"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_reporting"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	TMPDIR=$TEST_TMPDIR
	export TMPDIR
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_R_recursive="tank/src"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_option_Y_yield_iterations=3
	g_zxfer_version="test-version"
	g_zxfer_original_invocation="'./zxfer' 'backup/dst'"
	g_zxfer_secure_staging_dir_result=""
	g_zxfer_runtime_artifact_cleanup_paths=""
	unset ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the reporting test run root."
	zxfer_reset_failure_context "unit"
	# The output fragment's cases render reports with no recorded invocation.
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_reporting_output_tests.sh"; then
		g_zxfer_original_invocation=""
	fi
}

test_zxfer_render_failure_report_redacts_command_fields_by_default() {
	zxfer_set_failure_roots "tank/src" "backup/dst"
	zxfer_set_current_dataset_context "tank/src/child" "backup/dst/child"
	zxfer_record_last_command_string "zfs send tank/src@snap1"
	g_zxfer_failure_message="boom"

	report=$(zxfer_render_failure_report 1)

	assertContains "Failure report should include the selected stage." \
		"$report" "failure_stage: unit"
	assertContains "Failure report should include the current source dataset." \
		"$report" "current_source: tank/src/child"
	assertContains "Failure reports should redact the invocation by default." \
		"$report" "invocation: [redacted]"
	assertContains "Failure reports should redact the last command by default." \
		"$report" "last_command: [redacted]"
}

test_zxfer_record_last_command_helpers_store_redaction_marker_by_default() {
	zxfer_record_last_command_string "printf '%s' super-secret"
	assertEquals "String-based last-command tracking should store the redaction marker by default." \
		"[redacted]" "$g_zxfer_failure_last_command"

	zxfer_record_last_command_argv "/usr/bin/ssh" "backup.example" "super-secret"
	assertEquals "Argv-based last-command tracking should store the redaction marker by default." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_zxfer_record_last_command_helpers_preserve_empty_input_semantics_by_default() {
	zxfer_record_last_command_string ""
	assertEquals "String-based last-command tracking should keep empty command strings empty by default." \
		"" "$g_zxfer_failure_last_command"

	zxfer_record_last_command_argv
	assertEquals "Argv-based last-command tracking should keep empty argv lists empty by default." \
		"" "$g_zxfer_failure_last_command"
}

test_zxfer_command_display_render_enabled_tracks_display_consumers() {
	quiet_status=$(
		(
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			zxfer_command_display_render_enabled
			printf '%s\n' "$?"
		)
	)
	verbose_status=$(
		(
			g_option_v_verbose=1
			g_option_V_very_verbose=0
			zxfer_command_display_render_enabled
			printf '%s\n' "$?"
		)
	)
	very_verbose_status=$(
		(
			g_option_v_verbose=0
			g_option_V_very_verbose=1
			zxfer_command_display_render_enabled
			printf '%s\n' "$?"
		)
	)
	unsafe_status=$(
		(
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			zxfer_command_display_render_enabled
			printf '%s\n' "$?"
		)
	)

	assertEquals "Quiet runs should skip display command rendering." "1" "$quiet_status"
	assertEquals "Verbose (-v) runs should render display commands." "0" "$verbose_status"
	assertEquals "Very-verbose (-V) runs should render display commands." "0" "$very_verbose_status"
	assertEquals "Unsafe failure-report mode should render commands for failure context." "0" "$unsafe_status"
}

test_zxfer_render_failure_report_preserves_command_fields_in_unsafe_mode() {
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
	zxfer_set_failure_roots "tank/src" "backup/dst"
	g_zxfer_original_invocation="'./zxfer' '-Z' 'super-secret-token' 'backup/dst'"
	g_zxfer_failure_last_command="'/usr/bin/ssh' 'backup.example' 'super-secret-token'"
	g_zxfer_failure_message="boom"

	report=$(zxfer_render_failure_report 1)

	assertContains "Unsafe failure-report mode should preserve the original invocation." \
		"$report" "invocation: './zxfer' '-Z' 'super-secret-token' 'backup/dst'"
	assertContains "Unsafe failure-report mode should preserve the last command." \
		"$report" "last_command: '/usr/bin/ssh' 'backup.example' 'super-secret-token'"
}

test_zxfer_render_failure_report_keeps_missing_last_command_omitted_by_default() {
	g_zxfer_original_invocation="'./zxfer' '-R' 'tank/src' 'backup/dst'"
	g_zxfer_failure_message="boom"

	report=$(zxfer_render_failure_report 1)

	assertContains "Default failure-report mode should still redact the invocation when present." \
		"$report" "invocation: [redacted]"
	assertNotContains "Default failure-report mode should keep an unset last-command field omitted." \
		"$report" "last_command:"
}

test_zxfer_emit_failure_report_redacts_command_fields_in_stderr_and_log_by_default() {
	log_path="$TEST_TMPDIR/redacted_failure.log"
	stdout_file="$TEST_TMPDIR/redacted_failure.stdout"
	stderr_file="$TEST_TMPDIR/redacted_failure.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		g_zxfer_failure_report_emitted=0
		g_zxfer_original_invocation=\"'./zxfer' '-D' 'api-token=super-secret-token'\"
		g_zxfer_failure_last_command=\"'/usr/bin/ssh' 'backup.example' 'super-secret-token'\"
		g_zxfer_failure_message='boom'
		zxfer_emit_failure_report 1
	"

	assertEquals "Default failure-report emission should succeed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Default failure-report emission should redact the invocation in stderr." \
		"$(cat "$stderr_file")" "invocation: [redacted]"
	assertContains "Default failure-report emission should redact the last command in stderr." \
		"$(cat "$stderr_file")" "last_command: [redacted]"
	assertNotContains "Default failure-report emission should keep secrets out of stderr." \
		"$(cat "$stderr_file")" "super-secret-token"
	assertContains "Default failure-report emission should also redact the invocation in ZXFER_ERROR_LOG." \
		"$(cat "$log_path")" "invocation: [redacted]"
	assertContains "Default failure-report emission should also redact the last command in ZXFER_ERROR_LOG." \
		"$(cat "$log_path")" "last_command: [redacted]"
	assertNotContains "Default failure-report emission should keep secrets out of ZXFER_ERROR_LOG." \
		"$(cat "$log_path")" "super-secret-token"
}

test_zxfer_render_failure_report_escapes_raw_control_bytes_in_unsafe_mode() {
	esc=$(printf '\033')
	bell=$(printf '\007')
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	g_zxfer_original_invocation=$(zxfer_quote_command_argv "./zxfer" "-D" "$(printf 'token%swarn' "$esc")")
	zxfer_record_last_command_argv "/usr/bin/printf" "$(printf 'line%sbell' "$bell")"
	g_zxfer_failure_message="boom"

	report=$(zxfer_render_failure_report 1)
	printf '%s\n' "$report" >"$TEST_TMPDIR/control_escape_report.txt"
	grep -F -x "invocation: './zxfer' '-D' 'token\\x1Bwarn'" "$TEST_TMPDIR/control_escape_report.txt" >/dev/null 2>&1
	escaped_invocation_status=$?
	grep -F -x "last_command: '/usr/bin/printf' 'line\\x07bell'" "$TEST_TMPDIR/control_escape_report.txt" >/dev/null 2>&1
	escaped_last_command_status=$?
	grep -F "\\\\x1B" "$TEST_TMPDIR/control_escape_report.txt" >/dev/null 2>&1
	double_esc_status=$?
	grep -F "$esc" "$TEST_TMPDIR/control_escape_report.txt" >/dev/null 2>&1
	raw_esc_status=$?
	grep -F "$bell" "$TEST_TMPDIR/control_escape_report.txt" >/dev/null 2>&1
	raw_bell_status=$?

	assertEquals "Unsafe failure reports should escape ESC bytes in the invocation field." \
		0 "$escaped_invocation_status"
	assertEquals "Unsafe failure reports should escape BEL bytes in the last-command field." \
		0 "$escaped_last_command_status"
	assertEquals "Unsafe failure reports should not double-escape control-byte markers in command fields." \
		1 "$double_esc_status"
	assertEquals "Unsafe failure reports should not contain raw ESC bytes in command fields." \
		1 "$raw_esc_status"
	assertEquals "Unsafe failure reports should not contain raw BEL bytes in command fields." \
		1 "$raw_bell_status"
}

test_zxfer_emit_failure_report_escapes_raw_control_bytes_in_stderr_and_log_in_unsafe_mode() {
	log_path="$TEST_TMPDIR/control_escaped_failure.log"
	stdout_file="$TEST_TMPDIR/control_escaped_failure.stdout"
	stderr_file="$TEST_TMPDIR/control_escaped_failure.stderr"
	esc=$(printf '\033')
	bell=$(printf '\007')

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
		g_zxfer_failure_report_emitted=0
		g_zxfer_original_invocation=\$(zxfer_quote_command_argv './zxfer' '-D' \"\$(printf 'token%swarn' '$esc')\")
		zxfer_record_last_command_argv '/usr/bin/printf' \"\$(printf 'line%sbell' '$bell')\"
		g_zxfer_failure_message='boom'
		zxfer_emit_failure_report 1
	"
	grep -F -x "invocation: './zxfer' '-D' 'token\\x1Bwarn'" "$stderr_file" >/dev/null 2>&1
	stderr_invocation_status=$?
	grep -F -x "last_command: '/usr/bin/printf' 'line\\x07bell'" "$stderr_file" >/dev/null 2>&1
	stderr_last_command_status=$?
	grep -F "\\\\x1B" "$stderr_file" >/dev/null 2>&1
	stderr_double_esc_status=$?
	grep -F "$esc" "$stderr_file" >/dev/null 2>&1
	stderr_raw_esc_status=$?
	grep -F "$bell" "$stderr_file" >/dev/null 2>&1
	stderr_raw_bell_status=$?
	grep -F -x "invocation: './zxfer' '-D' 'token\\x1Bwarn'" "$log_path" >/dev/null 2>&1
	log_invocation_status=$?
	grep -F -x "last_command: '/usr/bin/printf' 'line\\x07bell'" "$log_path" >/dev/null 2>&1
	log_last_command_status=$?
	grep -F "\\\\x1B" "$log_path" >/dev/null 2>&1
	log_double_esc_status=$?
	grep -F "$esc" "$log_path" >/dev/null 2>&1
	log_raw_esc_status=$?
	grep -F "$bell" "$log_path" >/dev/null 2>&1
	log_raw_bell_status=$?

	assertEquals "Unsafe control-byte escaping failure-report emission should succeed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Unsafe stderr failure reports should escape ESC bytes in invocation." \
		0 "$stderr_invocation_status"
	assertEquals "Unsafe stderr failure reports should escape BEL bytes in last_command." \
		0 "$stderr_last_command_status"
	assertEquals "Unsafe stderr failure reports should not double-escape control-byte markers." \
		1 "$stderr_double_esc_status"
	assertEquals "Unsafe stderr failure reports should not contain raw ESC bytes." \
		1 "$stderr_raw_esc_status"
	assertEquals "Unsafe stderr failure reports should not contain raw BEL bytes." \
		1 "$stderr_raw_bell_status"
	assertEquals "Unsafe ZXFER_ERROR_LOG mirrors should escape ESC bytes in invocation." \
		0 "$log_invocation_status"
	assertEquals "Unsafe ZXFER_ERROR_LOG mirrors should escape BEL bytes in last_command." \
		0 "$log_last_command_status"
	assertEquals "Unsafe ZXFER_ERROR_LOG mirrors should not double-escape control-byte markers." \
		1 "$log_double_esc_status"
	assertEquals "Unsafe ZXFER_ERROR_LOG mirrors should not contain raw ESC bytes." \
		1 "$log_raw_esc_status"
	assertEquals "Unsafe ZXFER_ERROR_LOG mirrors should not contain raw BEL bytes." \
		1 "$log_raw_bell_status"
}

test_zxfer_record_last_command_argv_preserves_trailing_newlines_in_unsafe_mode() {
	trailing_arg=$(printf 'line-with-trailing-newline\n_')
	trailing_arg=${trailing_arg%_}
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_record_last_command_argv "/usr/bin/printf" "$trailing_arg"
	g_zxfer_failure_message="boom"

	report=$(zxfer_render_failure_report 1)
	printf '%s\n' "$report" >"$TEST_TMPDIR/trailing_newline_report.txt"
	grep -F -x "last_command: '/usr/bin/printf' 'line-with-trailing-newline\\n'" "$TEST_TMPDIR/trailing_newline_report.txt" >/dev/null 2>&1
	trailing_newline_status=$?

	assertEquals "Unsafe argv-based failure-report command capture should preserve trailing newline markers." \
		0 "$trailing_newline_status"
}

test_zxfer_failure_context_setters_ignore_empty_values_and_succeed() {
	# The setters are often a caller's last statement, so an ignored empty
	# value must not become a non-zero return.
	zxfer_set_failure_stage "replication"
	zxfer_set_failure_roots "tank/src" "backup/dst"
	zxfer_set_current_dataset_context "tank/src/a" "backup/dst/a"
	l_failures=""
	zxfer_set_failure_stage "" || l_failures="$l_failures stage"
	zxfer_set_failure_roots "" || l_failures="$l_failures roots"
	zxfer_set_current_dataset_context "tank/src/b" || l_failures="$l_failures dataset"

	assertEquals "Failure-context setters should return 0 for empty values." "" "$l_failures"
	assertEquals "An empty stage should keep the previous stage." \
		"replication" "$g_zxfer_failure_stage"
	assertEquals "An empty source root should keep the previous root." \
		"tank/src" "$g_zxfer_failure_source_root"
	assertEquals "A missing destination root should keep the previous root." \
		"backup/dst" "$g_zxfer_failure_destination_root"
	assertEquals "A given source dataset should replace the previous one." \
		"tank/src/b" "$g_zxfer_failure_current_source"
	assertEquals "A missing destination dataset should keep the previous one." \
		"backup/dst/a" "$g_zxfer_failure_current_destination"
}

test_throw_usage_error_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/throw_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_usage.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		zxfer_throw_usage_error "boom" 2
	'

	assertEquals "zxfer_throw_usage_error should preserve the requested exit status." 2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "zxfer_throw_usage_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "zxfer_throw_usage_error should write the error message to stderr." \
		"$(cat "$stderr_file")" "Error: boom"
	assertContains "zxfer_throw_usage_error should print usage to stderr." \
		"$(cat "$stderr_file")" "usage output"
}

test_throw_error_with_usage_keeps_runtime_class_and_skips_blank_message() {
	stdout_file="$TEST_TMPDIR/throw_with_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_with_usage.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		zxfer_emit_failure_report() {
			printf "class=%s message=<%s>\n" "$g_zxfer_failure_class" "$g_zxfer_failure_message" >&2
		}
		trap "zxfer_emit_failure_report \$?" EXIT
		zxfer_throw_error_with_usage ""
	'

	assertEquals "zxfer_throw_error_with_usage should default to exit status 1." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertNotContains "A blank message should not print an Error: line." \
		"$(cat "$stderr_file")" "Error:"
	assertContains "zxfer_throw_error_with_usage should print usage to stderr." \
		"$(cat "$stderr_file")" "usage output"
	assertContains "zxfer_throw_error_with_usage should classify the failure as runtime and keep the message empty." \
		"$(cat "$stderr_file")" "class=runtime message=<>"
}

test_throw_error_keeps_an_earlier_failure_class() {
	zxfer_test_capture_subshell '
		zxfer_set_failure_class dependency
		trap "printf \"class=%s message=%s\n\" \"\$g_zxfer_failure_class\" \"\$g_zxfer_failure_message\"" EXIT
		zxfer_throw_error "missing tool" 3
	'

	assertEquals "zxfer_throw_error should exit with the requested status." 3 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "zxfer_throw_error should print the message as-is." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "missing tool"
	assertContains "zxfer_throw_error should keep a class set before the throw and record the message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "class=dependency message=missing tool"
}

test_zxfer_emit_failure_report_marks_the_report_emitted_before_mirroring() {
	output=$(
		(
			zxfer_append_failure_report_to_log() {
				printf 'mirror emitted=%s\n' "$g_zxfer_failure_report_emitted"
				return 1
			}
			g_zxfer_failure_message=boom
			zxfer_emit_failure_report 0
			printf 'after_success=%s\n' "$g_zxfer_failure_report_emitted"
			zxfer_emit_failure_report 4
			printf 'after_failure=%s\n' "$?"
			zxfer_emit_failure_report 4
		) 2>&1
	)

	assertContains "A zero exit status should not emit a report." "$output" "after_success=0"
	assertContains "The report should be marked emitted before it is mirrored." "$output" "mirror emitted=1"
	assertContains "A failed mirror should not change the emit status." "$output" "after_failure=0"
	assertEquals "A second emit should print nothing." \
		1 "$(printf '%s\n' "$output" | grep -c '^zxfer: failure report begin$')"
}

test_zxfer_command_trace_helpers_follow_V_and_unsafe_mode() {
	output=$(
		(
			g_option_v_verbose=1
			g_option_V_very_verbose=0
			zxfer_command_trace_enabled
			printf 'v_only=%s\n' "$?"
			g_option_V_very_verbose=1
			zxfer_command_trace_enabled
			printf 'V=%s\n' "$?"
			zxfer_trace_rendered_command "Running command" "'zfs' 'list'"
			printf 'V_last=<%s>\n' "$g_zxfer_failure_last_command"
			g_option_V_very_verbose=0
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=yes
			zxfer_command_trace_enabled
			printf 'unsafe=%s\n' "$?"
			zxfer_trace_rendered_command "Running command" "'zfs' 'get'"
			printf 'unsafe_last=<%s>\n' "$g_zxfer_failure_last_command"
		) 2>&1
	)

	assertContains "Plain -v should not need rendered trace commands." "$output" "v_only=1"
	assertContains "-V should need rendered trace commands." "$output" "V=0"
	assertContains "-V should print the labeled command to stderr." "$output" "Running command: 'zfs' 'list'"
	assertContains "Safe mode should record only the redaction marker." "$output" "V_last=<[redacted]>"
	assertContains "Unsafe report mode should need rendered trace commands." "$output" "unsafe=0"
	assertNotContains "Without -V the trace should not be printed." "$output" "Running command: 'zfs' 'get'"
	assertContains "Unsafe report mode should record the rendered command." "$output" "unsafe_last=<'zfs' 'get'>"
}

test_zxfer_set_original_invocation_redacts_unless_unsafe_mode() {
	tab=$(printf '\t')

	zxfer_set_original_invocation ./zxfer -R "tank/it's"
	safe_invocation=$g_zxfer_original_invocation
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
	zxfer_set_original_invocation ./zxfer -R "tank/it's" "a${tab}b"
	unsafe_invocation=$g_zxfer_original_invocation

	assertEquals "Safe mode should store only the redaction marker." \
		"[redacted]" "$safe_invocation"
	assertEquals "Unsafe mode should store every argument as an escaped report word." \
		"'./zxfer' '-R' 'tank/it'\"'\"'s' 'a\\tb'" "$unsafe_invocation"
}

test_zxfer_report_quoting_skips_awk_and_sed_for_plain_tokens() {
	helper_log="$TEST_TMPDIR/report-quoting-helpers.log"
	rm -f "$helper_log"
	# posh cannot parse a single quote inside a nested "$(...)", so pass it
	# through a variable.
	quote_input="it's"
	output=$(
		(
			g_cmd_awk=zxfer_test_logging_awk
			zxfer_test_logging_awk() {
				printf '%s\n' awk >>"$helper_log"
				command awk "$@"
			}
			sed() {
				printf '%s\n' sed >>"$helper_log"
				command sed "$@"
			}
			printf 'escape=<%s>\n' "$(zxfer_escape_report_value 'tank/src@snap 1')"
			printf 'quote=<%s>\n' "$(zxfer_quote_token_for_report 'tank/src@snap')"
			printf 'argv=<%s>\n' "$(zxfer_quote_command_argv zfs list 'a b')"
			printf 'plain_helpers=<%s>\n' "$(cat "$helper_log" 2>/dev/null)"
			printf 'slow_quote=<%s>\n' "$(zxfer_quote_token_for_report "$quote_input")"
			printf 'slow_escape=<%s>\n' "$(zxfer_escape_report_value 'back\slash')"
		)
	)

	assertContains "Plain values should be returned unchanged." "$output" "escape=<tank/src@snap 1>"
	assertContains "Plain tokens should be single-quoted as-is." "$output" "quote=<'tank/src@snap'>"
	assertContains "Plain argv should be quoted word by word." "$output" "argv=<'zfs' 'list' 'a b'>"
	assertContains "Plain values should not run awk or sed." "$output" "plain_helpers=<>"
	assertContains "Single quotes should still take the sed path." "$output" "slow_quote=<'it'\"'\"'s'>"
	assertEquals "Backslashes should still take the awk path." \
		'slow_escape=<back\\slash>' "$(printf '%s\n' "$output" | sed -n '/^slow_escape=/p')"
	assertEquals "Only the two slow-path values should run helpers." \
		"sed
awk" "$(cat "$helper_log")"
}

test_zxfer_report_quoting_takes_the_slow_path_without_print_class_support() {
	helper_log="$TEST_TMPDIR/report-quoting-no-class.log"
	rm -f "$helper_log"
	output=$(
		(
			g_zxfer_report_fast_path=0
			g_cmd_awk=zxfer_test_logging_awk
			zxfer_test_logging_awk() {
				printf '%s\n' awk >>"$helper_log"
				command awk "$@"
			}
			sed() {
				printf '%s\n' sed >>"$helper_log"
				command sed "$@"
			}
			printf 'escape=<%s>\n' "$(zxfer_escape_report_value 'tank/src')"
			printf 'quote=<%s>\n' "$(zxfer_quote_token_for_report 'tank/src')"
			printf 'argv=<%s>\n' "$(zxfer_quote_command_argv zfs 'a b')"
		)
	)

	assertContains "The slow path should return plain values unchanged." "$output" "escape=<tank/src>"
	assertContains "The slow path should quote plain tokens the same way." "$output" "quote=<'tank/src'>"
	assertContains "The slow path should quote plain argv the same way." "$output" "argv=<'zfs' 'a b'>"
	assertEquals "Without [[:print:]] support every word should run awk, and quoting also sed." \
		"awk
awk
sed
awk
sed
awk
sed" "$(cat "$helper_log")"
}

# A UTF-8 sed rejects invalid multibyte input and prints nothing, which used
# to render such tokens as ''. The \001 forces the slow path in every shell;
# the "\377z" token takes the fast path where the shell treats 0xFF as
# printable and the slow path elsewhere, and must render the same either way.
test_zxfer_report_quoting_renders_invalid_multibyte_bytes_under_utf8() {
	utf8_locale=$(locale -a 2>/dev/null | grep -i -E '^(C|en_US)\.utf-?8$' | head -n 1)
	if [ "$utf8_locale" = "" ]; then
		startSkipping
	fi
	stderr_log="$TEST_TMPDIR/report-quoting-utf8.err"
	byte_ff=$(printf '\377')
	slow_token=$(printf 'a\377\001')
	fast_token=$(printf '\377z')

	quote_output=$(
		LC_ALL=$utf8_locale
		export LC_ALL
		zxfer_quote_token_for_report "$slow_token" 2>"$stderr_log"
	)
	quote_stderr=$(cat "$stderr_log")
	argv_output=$(
		LC_ALL=$utf8_locale
		export LC_ALL
		zxfer_quote_command_argv "$slow_token" "$fast_token" 2>"$stderr_log"
	)
	argv_stderr=$(cat "$stderr_log")
	fast_output=$(
		LC_ALL=$utf8_locale
		export LC_ALL
		zxfer_quote_token_for_report "$fast_token" 2>"$stderr_log"
	)
	fast_stderr=$(cat "$stderr_log")

	assertEquals "An invalid UTF-8 byte should pass through while control bytes are escaped." \
		"'a${byte_ff}\\x01'" "$quote_output"
	assertEquals "Rendering an invalid UTF-8 byte should not warn." "" "$quote_stderr"
	assertEquals "Argv rendering should keep invalid UTF-8 bytes in every word." \
		"'a${byte_ff}\\x01' '${byte_ff}z'" "$argv_output"
	assertEquals "Argv rendering of invalid UTF-8 bytes should not warn." "" "$argv_stderr"
	assertEquals "A printable-looking invalid UTF-8 token should render unchanged." \
		"'${byte_ff}z'" "$fast_output"
	assertEquals "Rendering a printable-looking invalid UTF-8 token should not warn." "" "$fast_stderr"
}

# zxfer-test-fragment: suites/zxfer_reporting_output_tests.sh
# shellcheck source=tests/suites/zxfer_reporting_output_tests.sh
. "$TESTS_DIR/suites/zxfer_reporting_output_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_reporting.sh" \
		"$TESTS_DIR/suites/zxfer_reporting_output_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
