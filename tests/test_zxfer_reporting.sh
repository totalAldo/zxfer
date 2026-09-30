#!/bin/sh
#
# shunit2 tests for the structured failure reports, verbose printers and
# failure-context helpers in src/zxfer_reporting.sh. The ZXFER_ERROR_LOG
# mirror is tested in tests/suites/zxfer_reporting_error_log_tests.sh.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# The concurrent error-log case runs the launcher against a stand-in secure
# PATH.
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

zxfer_source_runtime_modules_through "zxfer_reporting.sh"

# The usage throws print this after their message; the throw case looks for it.
zxfer_usage() {
	printf '%s\n' "usage: zxfer (usage output)"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_reporting"
}

oneTimeTearDown() {
	# Error-log cases make log parents read-only; restore them for removal.
	chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || true
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
	unset ZXFER_ERROR_LOG ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the reporting test run root."
	zxfer_reset_failure_context "unit"
	# The output fragment's cases render reports with no recorded invocation.
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_reporting_output_tests.sh"; then
		g_zxfer_original_invocation=""
	fi
}

# By default a report keeps every context field but redacts the command
# fields, and leaves out a field that has no value.
test_zxfer_render_failure_report_redacts_command_fields_and_omits_empty_ones_by_default() {
	g_zxfer_failure_stage="send/receive"
	g_zxfer_failure_message="replication failed"
	zxfer_set_failure_roots "tank/src" "backup/dst"
	zxfer_set_current_dataset_context "tank/src/child" "backup/dst/child"
	g_zxfer_original_invocation="'./zxfer' '-Z' 'super-secret-token' 'backup/dst'"
	g_zxfer_failure_last_command="'/usr/bin/ssh' 'backup.example' 'super-secret-token'"

	report=$(zxfer_render_failure_report 1)

	for l_line in "zxfer: failure report begin" "failure_stage: send/receive" \
		"source_root: tank/src" "current_source: tank/src/child" \
		"destination_root: backup/dst" "current_destination: backup/dst/child" \
		"origin_host: origin.example" "target_host: target.example" \
		"invocation: [redacted]" "last_command: [redacted]" \
		"zxfer: failure report end"; do
		assertContains "A default report should hold [$l_line]." "$report" "$l_line"
	done
	assertNotContains "A default report should keep secrets out." \
		"$report" "super-secret-token"

	g_zxfer_failure_current_source=""
	g_zxfer_failure_current_destination=""
	g_zxfer_original_invocation=""
	g_zxfer_failure_last_command=""
	report=$(zxfer_render_failure_report 1)

	for l_field in current_source current_destination invocation last_command; do
		assertNotContains "An empty $l_field should be left out." "$report" "$l_field:"
	done
}

# Command fields hold the redaction marker unless unsafe report mode is on,
# which keeps each argument as an escaped report word; an empty command
# records nothing.
test_zxfer_command_field_recorders_redact_by_default_and_escape_in_unsafe_mode() {
	tab=$(printf '\t')
	trailing_arg=$(printf 'line-with-trailing-newline\n_')
	trailing_arg=${trailing_arg%_}

	zxfer_record_last_command_string "printf '%s' super-secret"
	assertEquals "A command string should be recorded as the redaction marker by default." \
		"[redacted]" "$g_zxfer_failure_last_command"
	zxfer_record_last_command_argv "/usr/bin/ssh" "backup.example" "super-secret"
	assertEquals "An argv should be recorded as the redaction marker by default." \
		"[redacted]" "$g_zxfer_failure_last_command"
	zxfer_set_original_invocation ./zxfer -R "tank/it's"
	assertEquals "The invocation should be recorded as the redaction marker by default." \
		"[redacted]" "$g_zxfer_original_invocation"
	zxfer_record_last_command_string ""
	assertEquals "An empty command string should stay empty." "" "$g_zxfer_failure_last_command"
	zxfer_record_last_command_argv
	assertEquals "An empty argv should stay empty." "" "$g_zxfer_failure_last_command"

	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
	zxfer_record_last_command_argv "/usr/bin/printf" "$trailing_arg"
	assertEquals "Unsafe mode should keep a trailing newline as an escaped marker." \
		"'/usr/bin/printf' 'line-with-trailing-newline\\n'" "$g_zxfer_failure_last_command"
	zxfer_set_original_invocation ./zxfer -R "tank/it's" "a${tab}b"
	assertEquals "Unsafe mode should store every argument as an escaped report word." \
		"'./zxfer' '-R' 'tank/it'\"'\"'s' 'a\\tb'" "$g_zxfer_original_invocation"
}

# -v, -V and unsafe report mode each read rendered display commands; rendered
# trace commands only have -V, which prints them, and unsafe report mode,
# which records them. Unsafe mode takes 1, yes, true or on in any case.
test_zxfer_command_render_predicates_and_trace_follow_their_readers() {
	output=$(
		(
			while read -r l_v l_V l_unsafe; do
				g_option_v_verbose=$l_v
				g_option_V_very_verbose=$l_V
				ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=$l_unsafe
				zxfer_command_display_render_enabled
				l_display=$?
				zxfer_command_trace_enabled
				printf 'v=%s V=%s unsafe=%s: display=%s trace=%s\n' \
					"$l_v" "$l_V" "$l_unsafe" "$l_display" "$?"
			done <<'ROWS'
0 0 off
1 0 off
0 1 off
0 0 1
0 0 yes
0 0 TRUE
0 0 On
ROWS
			g_option_v_verbose=1
			g_option_V_very_verbose=1
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=off
			zxfer_trace_rendered_command "Running command" "'zfs' 'list'"
			printf 'V_last=<%s>\n' "$g_zxfer_failure_last_command"
			g_option_V_very_verbose=0
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=yes
			zxfer_trace_rendered_command "Running command" "'zfs' 'get'"
			printf 'unsafe_last=<%s>\n' "$g_zxfer_failure_last_command"
		) 2>&1
	)

	assertEquals "Each reader should enable its predicates, and a trace should print only under -V." \
		"v=0 V=0 unsafe=off: display=1 trace=1
v=1 V=0 unsafe=off: display=0 trace=1
v=0 V=1 unsafe=off: display=0 trace=0
v=0 V=0 unsafe=1: display=0 trace=0
v=0 V=0 unsafe=yes: display=0 trace=0
v=0 V=0 unsafe=TRUE: display=0 trace=0
v=0 V=0 unsafe=On: display=0 trace=0
Running command: 'zfs' 'list'
V_last=<[redacted]>
unsafe_last=<'zfs' 'get'>" "$output"
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

test_throw_helpers_exit_with_the_requested_status_and_keep_an_earlier_class() {
	stdout_file="$TEST_TMPDIR/throw.stdout"
	stderr_file="$TEST_TMPDIR/throw.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		zxfer_set_failure_class dependency
		trap "printf \"class=%s message=%s\n\" \"\$g_zxfer_failure_class\" \"\$g_zxfer_failure_message\" >&2" EXIT
		zxfer_throw_error "missing tool" 3
	'

	assertEquals "zxfer_throw_error should exit with the requested status." 3 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "zxfer_throw_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertEquals "zxfer_throw_error should print the message as-is and keep a class set before the throw." \
		"missing tool
class=dependency message=missing tool" "$(cat "$stderr_file")"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" '
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error_with_usage "boom with usage" 3
	'

	assertEquals "zxfer_throw_error_with_usage should preserve the requested exit status." \
		3 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "zxfer_throw_error_with_usage should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "zxfer_throw_error_with_usage should print the error to stderr." \
		"$(cat "$stderr_file")" "Error: boom with usage"
	assertContains "zxfer_throw_error_with_usage should print usage to stderr." \
		"$(cat "$stderr_file")" "usage: zxfer (usage output)"

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
	assertContains "A blank message should still print usage to stderr." \
		"$(cat "$stderr_file")" "usage output"
	assertContains "zxfer_throw_error_with_usage should classify the failure as runtime and keep the message empty." \
		"$(cat "$stderr_file")" "class=runtime message=<>"
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

test_zxfer_report_quoting_skips_awk_and_sed_for_plain_tokens() {
	helper_log="$TEST_TMPDIR/report-quoting-helpers.log"
	rm -f "$helper_log"
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
# zxfer-test-fragment: suites/zxfer_reporting_error_log_tests.sh
# shellcheck source=tests/suites/zxfer_reporting_error_log_tests.sh
. "$TESTS_DIR/suites/zxfer_reporting_error_log_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_reporting.sh" \
		"$TESTS_DIR/suites/zxfer_reporting_output_tests.sh" \
		"$TESTS_DIR/suites/zxfer_reporting_error_log_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
