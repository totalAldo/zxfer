#!/bin/sh
#
# shunit2 tests for structured failure reports, ZXFER_ERROR_LOG mirroring,
# verbose printers, and failure-context helpers in src/zxfer_reporting.sh.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# The concurrent error-log case runs the launcher against a stand-in secure
# PATH.
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

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

test_report_escape_preserves_each_newline_and_returns_awk_status() {
	l_ctrl=$(printf '\001')
	assertEquals 'one\ntwo\n\n\n' \
		"$(zxfer_escape_report_value "one${ZXFER_LF}two${ZXFER_LF}${ZXFER_LF}${ZXFER_LF}")"
	assertEquals '\n\n' "$(zxfer_escape_report_value "$ZXFER_LF$ZXFER_LF")"
	(
		g_cmd_awk=false
		zxfer_escape_report_value "$l_ctrl"
	)
	assertEquals "A failed escape must return the awk status." 1 "$?"
}

test_quote_token_for_report_escapes_multibyte_trail_quotes_and_preserves_lc_all() {
	l_byte_81=$(printf '\201')
	l_byte_01=$(printf '\001')
	l_lead_quote="${l_byte_81}'c"
	l_ctrl_lead_quote="${l_byte_01}${l_byte_81}'c"
	l_lead_backslash="${l_byte_81}\\c"
	l_expected_quote="'${l_byte_81}'\"'\"'c'"
	l_expected_ctrl_quote="'\\x01${l_byte_81}'\"'\"'c'"
	l_expected_backslash="'${l_byte_81}\\\\c'"

	l_lc_output=$(
		LC_ALL=POSIX
		l_q1=$(zxfer_quote_token_for_report "$l_lead_quote")
		l_q2=$(zxfer_quote_token_for_report "$l_ctrl_lead_quote")
		l_q3=$(zxfer_quote_token_for_report "$l_lead_backslash")
		printf 'set=%s\nq1=%s\nq2=%s\nq3=%s\n' "${LC_ALL-unset}" "$l_q1" "$l_q2" "$l_q3"
		unset LC_ALL
		l_q4=$(zxfer_quote_token_for_report "it's")
		printf 'unset=%s\nq4=%s\n' "${LC_ALL-unset}" "$l_q4"
	)

	assertEquals "Report quoting must byte-escape 0x27 and 0x5C after high bytes and preserve LC_ALL state." \
		"set=POSIX
q1=$l_expected_quote
q2=$l_expected_ctrl_quote
q3=$l_expected_backslash
unset=unset
q4='it'\"'\"'s'" "$l_lc_output"
}

test_zxfer_render_failure_report_escapes_raw_control_bytes_in_unsafe_mode() {
	esc=$(printf '\033')
	bell=$(printf '\007')
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_quote_command_argv_into_result "./zxfer" "-D" "$(printf 'token%swarn' "$esc")"
	g_zxfer_original_invocation=$g_zxfer_quoted_command_argv_result
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

	capture_case() {
		zxfer_set_failure_class dependency
		trap "printf \"class=%s message=%s\n\" \"\$g_zxfer_failure_class\" \"\$g_zxfer_failure_message\" >&2" EXIT
		zxfer_throw_error "missing tool" 3
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case

	assertEquals "zxfer_throw_error should exit with the requested status." 3 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "zxfer_throw_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertEquals "zxfer_throw_error should print the message as-is and keep a class set before the throw." \
		"missing tool
class=dependency message=missing tool" "$(cat "$stderr_file")"

	capture_case() {
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error_with_usage "boom with usage" 3
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case

	assertEquals "zxfer_throw_error_with_usage should preserve the requested exit status." \
		3 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "zxfer_throw_error_with_usage should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "zxfer_throw_error_with_usage should print the error to stderr." \
		"$(cat "$stderr_file")" "Error: boom with usage"
	assertContains "zxfer_throw_error_with_usage should print usage to stderr." \
		"$(cat "$stderr_file")" "usage: zxfer (usage output)"

	capture_case() {
		zxfer_emit_failure_report() {
			printf "class=%s message=<%s>\n" "$g_zxfer_failure_class" "$g_zxfer_failure_message" >&2
		}
		trap "zxfer_emit_failure_report \$?" EXIT
		zxfer_throw_error_with_usage ""
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case

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
	ctrl_input=$(printf 'ctrl\001byte')
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
			zxfer_quote_command_argv_into_result zfs list 'a b'
			printf 'argv=<%s>\n' "$g_zxfer_quoted_command_argv_result"
			printf 'fast_bs=<%s>\n' "$(zxfer_escape_report_value 'back\slash')"
			printf 'plain_helpers=<%s>\n' "$(cat "$helper_log" 2>/dev/null)"
			printf 'slow_quote=<%s>\n' "$(zxfer_quote_token_for_report "$quote_input")"
			printf 'slow_escape=<%s>\n' "$(zxfer_escape_report_value "$ctrl_input")"
		)
	)

	assertContains "Plain values should be returned unchanged." "$output" "escape=<tank/src@snap 1>"
	assertContains "Plain tokens should be single-quoted as-is." "$output" "quote=<'tank/src@snap'>"
	assertContains "Plain argv should be quoted word by word." "$output" "argv=<'zfs' 'list' 'a b'>"
	assertEquals "Short backslash values should escape in-shell without awk." \
		'fast_bs=<back\\slash>' "$(printf '%s\n' "$output" | sed -n '/^fast_bs=/p')"
	assertContains "Plain and short backslash values should not run awk or sed." "$output" "plain_helpers=<>"
	assertContains "Single quotes should be escaped in shell without sed." "$output" "slow_quote=<'it'\"'\"'s'>"
	assertEquals "Control bytes should still take the awk slow path." \
		'slow_escape=<ctrl\x01byte>' "$(printf '%s\n' "$output" | sed -n '/^slow_escape=/p')"
	assertEquals "Only control/non-fast values should run awk, and sed is never needed." \
		"awk" "$(cat "$helper_log")"
}

test_zxfer_escape_report_value_in_shell_fast_path_fallbacks_and_lc_all() {
	helper_log="$TEST_TMPDIR/report-escape-fast-path.log"
	rm -f "$helper_log"
	l_mixed="a\\b${ZXFER_TAB}c${ZXFER_CR}d${ZXFER_LF}\\t*?[x]"
	l_c0=$(printf 'a\\b\t\r\n\001z')
	l_del=$(printf 'a\\b\177z')
	# Build a >4096-byte value containing a backslash so the length cap triggers awk.
	l_long='0123456789abcdef'
	while [ "${#l_long}" -le 4096 ]; do
		l_long=$l_long$l_long
	done
	l_long_bs="${l_long}\\tail"

	output=$(
		(
			g_cmd_awk=zxfer_test_logging_awk
			zxfer_test_logging_awk() {
				printf '%s\n' awk >>"$helper_log"
				command awk "$@"
			}

			LC_ALL=POSIX
			zxfer_escape_report_value_into_result "$l_mixed"
			printf 'mixed_status=%s lc=%s val=<%s>\n' "$?" "${LC_ALL-unset}" \
				"$g_zxfer_escaped_report_value_result"
			printf 'fast_helpers=<%s>\n' "$(cat "$helper_log" 2>/dev/null)"

			unset LC_ALL
			zxfer_escape_report_value_into_result "$l_c0"
			printf 'c0_status=%s lc=%s val=<%s>\n' "$?" "${LC_ALL-unset}" \
				"$g_zxfer_escaped_report_value_result"

			LC_ALL=""
			zxfer_escape_report_value_into_result "$l_del"
			printf 'del_status=%s lc=<%s> set=%s val=<%s>\n' "$?" "${LC_ALL-unset}" \
				"${LC_ALL+set}" "$g_zxfer_escaped_report_value_result"

			LC_ALL=POSIX
			zxfer_escape_report_value_into_result "$l_long_bs"
			l_long_status=$?
			l_long_len=${#g_zxfer_escaped_report_value_result}
			l_long_tail=${g_zxfer_escaped_report_value_result#"$l_long"}
			printf 'long_status=%s lc=%s len=%s tail=<%s>\n' "$l_long_status" \
				"${LC_ALL-unset}" "$l_long_len" "$l_long_tail"
		)
	)

	assertContains "Backslash, TAB, CR, and LF should escape in-shell with backslash escaped first and LC_ALL preserved." \
		"$output" 'mixed_status=0 lc=POSIX val=<a\\b\tc\rd\n\\t*?[x]>'
	assertContains "Bounded \\, TAB, CR, LF escaping must not invoke awk." \
		"$output" "fast_helpers=<>"
	assertContains "C0 bytes must fall back to awk on the original value without double-escaping and keep LC_ALL unset." \
		"$output" 'c0_status=0 lc=unset val=<a\\b\t\r\n\x01z>'
	assertContains "DEL bytes must fall back to awk on the original value and preserve an empty LC_ALL." \
		"$output" 'del_status=0 lc=<> set=set val=<a\\b\x7Fz>'
	assertContains "Values longer than 4096 bytes with escapes must fall back to awk and preserve LC_ALL." \
		"$output" "long_status=0 lc=POSIX len=$((${#l_long} + 6)) tail=<\\\\tail>"
	assertEquals "Exactly C0, DEL, and >4096B values should take the awk fallback." \
		"awk
awk
awk" "$(cat "$helper_log")"
}

test_zxfer_report_command_into_result_helpers_publish_and_reset_cleanly() {
	l_ctrl=$(printf 'bad\001arg')
	output=$(
		(
			zxfer_quote_command_argv_into_result "zfs" "it's" "a${ZXFER_TAB}b"
			printf 'argv_status=%s val=<%s>\n' "$?" "$g_zxfer_quoted_command_argv_result"

			zxfer_quote_command_argv_into_result
			printf 'empty_argv_status=%s val=<%s>\n' "$?" "$g_zxfer_quoted_command_argv_result"

			zxfer_render_command_for_report_into_result "/usr/bin/ssh 'host' /sbin/zfs" "create" "-o" "a=b"
			printf 'prefixed_status=%s val=<%s>\n' "$?" "$g_zxfer_rendered_report_command_result"

			zxfer_render_command_for_report_into_result "/usr/bin/ssh 'host' /sbin/zfs"
			printf 'prefix_only_status=%s val=<%s>\n' "$?" "$g_zxfer_rendered_report_command_result"

			zxfer_render_command_for_report_into_result "" "zfs" "list" "tank/src"
			printf 'empty_prefix_status=%s val=<%s>\n' "$?" "$g_zxfer_rendered_report_command_result"

			g_zxfer_quoted_command_argv_result="stale-argv"
			g_zxfer_rendered_report_command_result="stale-cmd"
			zxfer_reset_failure_context "unit"
			printf 'reset=<%s><%s>\n' "$g_zxfer_quoted_command_argv_result" \
				"$g_zxfer_rendered_report_command_result"

			g_zxfer_quoted_command_argv_result="stale-argv"
			g_zxfer_rendered_report_command_result="stale-cmd"
			g_cmd_awk=false
			zxfer_render_command_for_report_into_result "prefix" "$l_ctrl"
			printf 'fail_status=%s argv=<%s> cmd=<%s>\n' "$?" \
				"$g_zxfer_quoted_command_argv_result" "$g_zxfer_rendered_report_command_result"
		)
	)

	assertContains "zxfer_quote_command_argv_into_result should publish quoted report argv in-shell." \
		"$output" "argv_status=0 val=<'zfs' 'it'\"'\"'s' 'a\\tb'>"
	assertContains "zxfer_quote_command_argv_into_result should publish empty string for empty argv." \
		"$output" "empty_argv_status=0 val=<>"
	assertContains "zxfer_render_command_for_report_into_result should join prefix and quoted argv." \
		"$output" "prefixed_status=0 val=</usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'a=b'>"
	assertContains "zxfer_render_command_for_report_into_result should publish prefix alone when argv is empty." \
		"$output" "prefix_only_status=0 val=</usr/bin/ssh 'host' /sbin/zfs>"
	assertContains "zxfer_render_command_for_report_into_result should publish quoted argv when prefix is empty." \
		"$output" "empty_prefix_status=0 val=<'zfs' 'list' 'tank/src'>"
	assertContains "zxfer_reset_failure_context should clear both new report command result globals." \
		"$output" "reset=<><>"
	assertContains "A failed render should preserve status and clear both result globals." \
		"$output" "fail_status=1 argv=<> cmd=<>"
}

test_quote_report_token_into_result_restores_lc_all_across_fast_slow_and_error_paths() {
	l_ctrl=$(printf 'bad\001'\''arg')
	output=$(
		(
			unset LC_ALL
			zxfer_quote_report_token_into_result "it's\\ok"
			printf 'fast_unset=%s lc=%s val=<%s>\n' "$?" "${LC_ALL-unset}" \
				"$g_zxfer_quoted_report_token_result"

			LC_ALL=""
			zxfer_quote_report_token_into_result "$l_ctrl"
			printf 'slow_empty=%s lc=<%s> set=%s val=<%s>\n' "$?" "${LC_ALL-unset}" \
				"${LC_ALL+set}" "$g_zxfer_quoted_report_token_result"

			unset LC_ALL
			g_zxfer_escaped_report_value_result="stale-esc"
			g_zxfer_quoted_report_token_result="stale-tok"
			g_cmd_awk=false
			zxfer_quote_report_token_into_result "$l_ctrl"
			printf 'fail_unset=%s lc=%s esc=<%s> tok=<%s>\n' "$?" "${LC_ALL-unset}" \
				"$g_zxfer_escaped_report_value_result" "$g_zxfer_quoted_report_token_result"

			LC_ALL=POSIX
			g_zxfer_report_fast_path=0
			g_cmd_awk="awk"
			zxfer_quote_report_token_into_result "no-fast'quote"
			printf 'nofast_posix=%s lc=%s val=<%s>\n' "$?" "${LC_ALL-unset}" \
				"$g_zxfer_quoted_report_token_result"
		)
	)

	assertContains "Fast-path quoting should preserve unset LC_ALL." \
		"$output" "fast_unset=0 lc=unset val=<'it'\"'\"'s\\\\ok'>"
	assertContains "Slow-path quoting should preserve empty LC_ALL." \
		"$output" "slow_empty=0 lc=<> set=set val=<'bad\\x01'\"'\"'arg'>"
	assertContains "Failed slow-path quoting should restore LC_ALL and clear both results." \
		"$output" "fail_unset=1 lc=unset esc=<> tok=<>"
	assertContains "Slow-path quoting with fast path disabled should restore LC_ALL." \
		"$output" "nofast_posix=0 lc=POSIX val=<'no-fast'\"'\"'quote'>"
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
			zxfer_quote_command_argv_into_result zfs 'a b'
			printf 'argv=<%s>\n' "$g_zxfer_quoted_command_argv_result"
		)
	)

	assertContains "The slow path should return plain values unchanged." "$output" "escape=<tank/src>"
	assertContains "The slow path should quote plain tokens the same way." "$output" "quote=<'tank/src'>"
	assertContains "The slow path should quote plain argv the same way." "$output" "argv=<'zfs' 'a b'>"
	assertEquals "Without [[:print:]] support every word should run awk once without sed." \
		"awk
awk
awk
awk" "$(cat "$helper_log")"
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
		zxfer_quote_command_argv_into_result "$slow_token" "$fast_token" 2>"$stderr_log"
		printf '%s\n' "$g_zxfer_quoted_command_argv_result"
	)
	argv_stderr=$(cat "$stderr_log")
	fast_output=$(
		LC_ALL=$utf8_locale
		export LC_ALL
		zxfer_quote_token_for_report "$fast_token" 2>"$stderr_log"
	)
	fast_stderr=$(cat "$stderr_log")
	mb_awk_log="$TEST_TMPDIR/report-quoting-utf8-mb-awk.log"
	rm -f "$mb_awk_log"
	mb_euro=$(printf '\342\202\254')
	mb_lang_output=$(
		unset LC_ALL
		LANG=$utf8_locale
		export LANG
		g_cmd_awk=zxfer_test_logging_mb_awk
		zxfer_test_logging_mb_awk() {
			printf 'awk\n' >>"$mb_awk_log"
			command awk "$@"
		}
		zxfer_escape_report_value_into_result "$mb_euro"
		printf 'lc=%s val=<%s>\n' "${LC_ALL-unset}" "$g_zxfer_escaped_report_value_result"
	)

	assertEquals "An invalid UTF-8 byte should pass through while control bytes are escaped." \
		"'a${byte_ff}\\x01'" "$quote_output"
	assertEquals "Rendering an invalid UTF-8 byte should not warn." "" "$quote_stderr"
	assertEquals "Argv rendering should keep invalid UTF-8 bytes in every word." \
		"'a${byte_ff}\\x01' '${byte_ff}z'" "$argv_output"
	assertEquals "Argv rendering of invalid UTF-8 bytes should not warn." "" "$argv_stderr"
	assertEquals "A printable-looking invalid UTF-8 token should render unchanged." \
		"'${byte_ff}z'" "$fast_output"
	assertEquals "Rendering a printable-looking invalid UTF-8 token should not warn." "" "$fast_stderr"
	assertEquals "Printable UTF-8 multibyte text under exported LANG must still honor exported LC_ALL=C and leave LC_ALL unset." \
		"lc=unset val=<$mb_euro>" "$mb_lang_output"
	assertEquals "Bytes >= 0x80 under exported LANG must take the awk fallback under exported LC_ALL=C." \
		"awk" "$(cat "$mb_awk_log" 2>/dev/null)"
}

test_verbose_printers_print_text_as_written_only_at_their_level() {
	g_zxfer_original_invocation=""
	l_text='lit=\033[31m \c cut \n \\ \x1B end'
	l_value=$(printf 'ok\033[2J\033]0;x\007 lit=\\033 \\c cut\r\nforged: line')

	g_option_v_verbose=0
	g_option_V_very_verbose=0
	assertEquals "zxfer_echov stays quiet without -v." "" "$(zxfer_echov "hidden message")"
	assertEquals "zxfer_echoV stays quiet without -V." "" "$(zxfer_echoV "hidden debug" 2>&1)"
	assertEquals "zxfer_echoV_escaped stays quiet without -V." \
		"" "$(zxfer_echoV_escaped label "$l_value" 2>&1)"

	g_option_v_verbose=1
	g_option_V_very_verbose=1
	assertEquals "zxfer_echov prints backslashes as written." "$l_text
." "$(
		zxfer_echov "$l_text"
		printf '.'
	)"
	assertEquals "A lone -n is text, not an echo option." "-n" "$(zxfer_echov -n)"
	assertEquals "zxfer_echoV prints backslashes as written, to stderr only." "$l_text
." "$(
		{
			zxfer_echoV "$l_text"
			printf '.' >&2
		} 2>&1 >/dev/null
	)"
	assertEquals "zxfer_echoV_escaped prints control bytes and backslashes escaped on one line." \
		'label: ok\x1B[2J\x1B]0;x\x07 lit=\\033 \\c cut\r\nforged: line' \
		"$(zxfer_echoV_escaped label "$l_value" 2>&1)"
	assertEquals "An empty value keeps the label and separator." \
		"label: " "$(zxfer_echoV_escaped label "" 2>&1)"
	assertEquals "zxfer_echoV_escaped writes to stderr only." \
		"" "$(zxfer_echoV_escaped label "$l_value" 2>/dev/null)"
}

# Only FreeBSD with the speaker tools and /dev/speaker beeps; any other host
# skips with a -V note, so replication continues.
test_beep_skips_with_a_V_note_where_the_speaker_is_unavailable() {
	g_zxfer_original_invocation=""
	fake_bin_dir="$TEST_TMPDIR/no_speaker_tools"
	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/uname" <<'UNAME'
#!/bin/sh
printf '%s\n' "FreeBSD"
UNAME
	chmod +x "$fake_bin_dir/uname"

	output=$(
		(
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			(
				uname() {
					printf '%s\n' "Linux"
				}
				zxfer_beep 1
			)
			(
				PATH="$fake_bin_dir"
				zxfer_beep 1
			)
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
				zxfer_beep 1
			)
		) 2>&1
	)

	assertEquals "A non-FreeBSD host, missing speaker tools and a missing /dev/speaker should each skip with a -V note." \
		"Beep requested but unsupported on Linux; skipping.
Beep requested but speaker tools are missing; skipping.
Beep requested but /dev/speaker missing; skipping." "$output"
}

test_zxfer_report_command_rendering_quotes_each_argument_on_one_line() {
	g_zxfer_original_invocation=""
	l_newline_arg=$(printf 'line1\nline2')
	zxfer_quote_command_argv_into_result "./zxfer" "value with space" "$l_newline_arg" "apost'rophe"
	result=$g_zxfer_quoted_command_argv_result

	assertEquals "Quoted argv should remain one-line and shell-safe for reports." \
		"'./zxfer' 'value with space' 'line1\\nline2' 'apost'\"'\"'rophe'" "$result"
	assertEquals "Report rendering should keep a shell-ready prefix and quote the argv after it." \
		"/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4'" \
		"$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs" "create" "-o" "compression=lz4")"
	assertEquals "Report rendering should return a prefix without argv unchanged." \
		"/usr/bin/ssh 'host' /sbin/zfs" \
		"$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs")"
	assertEquals "Report rendering should quote the argv when the prefix is empty." \
		"'zfs' 'list' 'tank/src'" \
		"$(zxfer_render_command_for_report "" "zfs" "list" "tank/src")"
}

# The exit status picks the class and message when none was recorded: 2 is a
# usage failure, anything else a runtime one.
test_zxfer_render_failure_report_defaults_class_message_and_stage() {
	g_zxfer_original_invocation=""
	g_zxfer_failure_class=""
	g_zxfer_failure_message=""
	g_zxfer_failure_stage=""
	runtime_report=$(zxfer_render_failure_report 1)
	g_option_R_recursive=""
	g_option_N_nonrecursive="tank/src"
	usage_report=$(zxfer_render_failure_report 2)

	assertContains "Non-usage exits should default to runtime failures." \
		"$runtime_report" "failure_class: runtime"
	assertContains "Missing failure messages should fall back to the exit status summary." \
		"$runtime_report" "message: zxfer exited with status 1."
	assertContains "A missing stage should fall back to startup." \
		"$runtime_report" "failure_stage: startup"
	assertContains "Failure reports should default exit status 2 to usage errors." \
		"$usage_report" "failure_class: usage"
	assertContains "Failure reports should default missing messages to the exit-status text." \
		"$usage_report" "message: zxfer exited with status 2."
	assertContains "Failure reports should identify nonrecursive mode when -N is set." \
		"$usage_report" "mode: nonrecursive"
}

make_error_log_parent_read_only() {
	chmod 500 "$1" || fail "Unable to make $1 read-only."
	if [ -w "$1" ]; then
		chmod 700 "$1"
		startSkipping
		return 1
	fi
}

test_zxfer_append_failure_report_to_log_rejects_direct_symlink_target_when_component_scan_does_not_fire() {
	log_target="$TEST_TMPDIR/failure-direct-target.log"
	log_symlink="$TEST_TMPDIR/failure-direct-link.log"

	: >"$log_target"
	ln -s "$log_target" "$log_symlink"

	capture_case() {
		zxfer_find_symlink_path_component() {
			return 1
		}
		ZXFER_ERROR_LOG="$log_symlink" \
			zxfer_append_failure_report_to_log "report"
	}
	zxfer_test_capture_subshell capture_case

	assertEquals "Direct symlinked error-log targets should still be rejected when path-component scanning does not catch them first." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Direct symlinked error-log target rejection should explain the refusal." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "because it is a symlink"
}

test_zxfer_append_failure_report_to_log_warns_when_the_write_fails() {
	log_path="$TEST_TMPDIR/append-failure.log"
	stdout_file="$TEST_TMPDIR/append_failure.stdout"
	stderr_file="$TEST_TMPDIR/append_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	# The report is written by the awk in g_cmd_awk; false stands in for an
	# awk whose write or close fails.
	capture_case() {
		ZXFER_ERROR_LOG="$log_path"
		g_cmd_awk=false
		zxfer_append_failure_report_to_log "append-failure-report"
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case

	assertEquals "Append failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Append failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
	assertEquals "Append failures should not write partial report data." \
		"" "$(cat "$log_path")"
}

test_zxfer_append_failure_report_to_log_appends_existing_file_when_parent_is_not_writable() {
	log_dir="$TEST_TMPDIR/nonwritable-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_parent.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_parent.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	capture_case() {
		ZXFER_ERROR_LOG="$log_path"
		zxfer_append_failure_report_to_log "message: appended-report"
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case
	chmod 700 "$log_dir"

	assertEquals "Existing secure ZXFER_ERROR_LOG files should still append cleanly when the trusted parent is not writable." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Non-writable trusted-parent appends should not emit warnings." \
		"" "$(cat "$stderr_file")"
	assertContains "Non-writable trusted-parent appends should preserve prior contents." \
		"$(cat "$log_path")" "existing: keep-me"
	assertContains "Non-writable trusted-parent appends should add the new report payload." \
		"$(cat "$log_path")" "message: appended-report"
}

test_zxfer_append_failure_report_to_log_warns_when_nonwritable_parent_needs_create() {
	log_dir="$TEST_TMPDIR/nonwritable-create-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_create.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_create.stderr"

	mkdir -p "$log_dir"
	make_error_log_parent_read_only "$log_dir" || return 0

	capture_case() {
		ZXFER_ERROR_LOG="$log_path"
		zxfer_append_failure_report_to_log "message: appended-report"
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case
	chmod 700 "$log_dir"

	assertEquals "Missing ZXFER_ERROR_LOG files should fail closed when the trusted parent is not writable." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing ZXFER_ERROR_LOG files in non-writable trusted parents should warn that creation is not possible." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_rejects_unsafe_paths() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	untrusted_dir="$physical_tmpdir/untrusted_error_log_parent"
	real_dir="$physical_tmpdir/real_parent"
	link_dir="$physical_tmpdir/link_parent"
	real_path="$physical_tmpdir/real_failure.log"
	link_path="$physical_tmpdir/failure_link.log"
	dir_target="$physical_tmpdir/failure_dir"
	mkdir -p "$untrusted_dir" "$real_dir" "$dir_target"
	chmod 0777 "$untrusted_dir"
	ln -s "$real_dir" "$link_dir"
	: >"$real_path"
	chmod 600 "$real_path"
	ln -s "$real_path" "$link_path"

	set +e
	while IFS='|' read -r l_case l_log_path l_warning l_absent_path; do
		stderr_file="$TEST_TMPDIR/error_log_$l_case.stderr"
		ZXFER_ERROR_LOG=$l_log_path
		zxfer_append_failure_report_to_log "report" \
			>"$TEST_TMPDIR/error_log_$l_case.stdout" 2>"$stderr_file"
		assertEquals "[$l_case] should be rejected." 1 "$?"
		assertContains "[$l_case] should emit a refusal warning." \
			"$(cat "$stderr_file")" "$l_warning"
		if [ -n "$l_absent_path" ]; then
			assertFalse "[$l_case] must not create $l_absent_path." \
				"[ -e '$l_absent_path' ]"
		fi
	done <<EOF
relative|relative.log|refusing ZXFER_ERROR_LOG path "relative.log" because it is not absolute|$TEST_TMPDIR/relative.log
missing_parent|$physical_tmpdir/missing/subdir/failure.log|parent directory "$physical_tmpdir/missing/subdir" does not exist|
untrusted_parent|$untrusted_dir/failure.log|writable by others without sticky-bit protection|
symlink_parent|$link_dir/failure.log|path component "$link_dir" is a symlink|$real_dir/failure.log
symlink_target|$link_path|path component "$link_path" is a symlink|
non_regular|$dir_target|path "$dir_target" because it is not a regular file|
EOF
	chmod 0700 "$untrusted_dir"
}

# Each row: a fault in a 0600 log owned by this user in a trusted parent, and
# the warning its refusal must give. A log whose mode or owner is wrong, or
# whose owner, mode or link count cannot be read, is refused unwritten.
test_zxfer_append_failure_report_to_log_refuses_a_log_it_cannot_prove_private() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)

	while IFS='|' read -r l_fault l_warning; do
		log_path="$physical_tmpdir/refused_$l_fault.log"
		stderr_file="$TEST_TMPDIR/refused_$l_fault.stderr"
		: >"$log_path"
		chmod 600 "$log_path"

		set +e
		(
			[ "$l_fault" != wider_mode ] || chmod 644 "$log_path"
			zxfer_read_path_metadata() {
				zxfer_parse_path_metadata_line "$(command ls -ldin "$1")" || return 1
				[ "$1" = "$log_path" ] || return 0
				case $l_fault in
				foreign_owner) g_zxfer_path_owner_uid_result=1234 ;;
				unknown_owner) return 1 ;;
				unknown_mode) g_zxfer_path_mode_result="" ;;
				unknown_links) g_zxfer_path_link_count_result="" ;;
				esac
			}
			ZXFER_ERROR_LOG=$log_path
			zxfer_append_failure_report_to_log "message: should-not-append"
		) </dev/null >/dev/null 2>"$stderr_file"
		status=$?

		assertEquals "A log with a $l_fault should be refused." 1 "$status"
		assertContains "The $l_fault refusal should explain it." \
			"$(cat "$stderr_file")" "$l_warning"
		assertEquals "A log refused for a $l_fault must not receive the report." \
			"" "$(cat "$log_path")"
	done <<'ROWS'
wider_mode|because its permissions (644) are not 0600
foreign_owner|because it is owned by UID 1234 instead of
unknown_owner|because its owner could not be determined
unknown_mode|because its permissions could not be determined
unknown_links|because its link count could not be determined
ROWS
}

test_zxfer_append_failure_report_to_log_rejects_a_log_with_another_hard_link() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	other_path="$physical_tmpdir/hard_link_other"
	log_path="$physical_tmpdir/hard_link.log"
	stderr_file="$TEST_TMPDIR/error_log_hard_link.stderr"
	printf '%s\n' "other: keep-me" >"$other_path"
	chmod 600 "$other_path"
	ln "$other_path" "$log_path" || fail "Unable to create the hard-link fixture."
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: should-not-append" \
		>"$TEST_TMPDIR/error_log_hard_link.stdout" 2>"$stderr_file"
	status=$?

	assertEquals "A log that is also reachable by another name should be rejected." 1 "$status"
	assertContains "The hard-link rejection should give the link count." \
		"$(cat "$stderr_file")" "because it has 2 hard links"
	assertEquals "The file's other name must not receive the report." \
		"other: keep-me" "$(cat "$other_path")"
}

test_zxfer_append_failure_report_to_log_warns_when_the_log_cannot_be_created() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	stderr_file="$TEST_TMPDIR/error_log_create_failure.stderr"
	# No supported file system accepts a 300-byte name, so the create fails
	# even for root, which a read-only parent cannot stop.
	ZXFER_ERROR_LOG="$physical_tmpdir/$(printf '%0300d' 0)"

	set +e
	zxfer_append_failure_report_to_log "message: create-failed" \
		>"$TEST_TMPDIR/error_log_create_failure.stdout" 2>"$stderr_file"
	status=$?

	assertEquals "A log that cannot be created should be reported as a failure." 1 "$status"
	assertContains "A log that cannot be created should produce the documented warning." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG file"
}

# A default ACL on the parent can give a new log more than the umask allows;
# the umask stub stands in for one.
test_zxfer_append_failure_report_to_log_narrows_a_wider_new_log_to_0600() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/wider_new.log"
	stderr_file="$TEST_TMPDIR/error_log_wider_new.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		umask() {
			command umask 022
		}
		zxfer_append_failure_report_to_log "message: wider-new-log"
	) >"$TEST_TMPDIR/error_log_wider_new.stdout" 2>"$stderr_file"
	status=$?
	perms=$(zxfer_get_path_mode_octal "$log_path") || perms=""

	assertEquals "A new log created wider than 0600 should still take the report." 0 "$status"
	assertEquals "zxfer should narrow a new log to mode 600." "600" "$perms"
	assertEquals "The new log should hold the report." \
		"message: wider-new-log" "$(cat "$log_path")"
	assertEquals "Narrowing a new log should not warn." "" "$(cat "$stderr_file")"
}

# CREATED 1 stands for a create step that opened what another user put at the
# name; the chmod for a new log must not reach that entry's target.
test_zxfer_validate_existing_error_log_file_never_changes_a_planted_entry() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	target_path="$physical_tmpdir/planted_target"
	symlink_path="$physical_tmpdir/planted_symlink.log"
	hard_link_path="$physical_tmpdir/planted_hard_link.log"
	printf '%s\n' "target: keep-me" >"$target_path"
	chmod 644 "$target_path"
	ln -s "$target_path" "$symlink_path"
	ln "$target_path" "$hard_link_path" || fail "Unable to create the hard-link fixture."

	set +e
	zxfer_validate_existing_error_log_file "$symlink_path" 1 \
		2>"$TEST_TMPDIR/planted_symlink.stderr"
	symlink_status=$?
	zxfer_validate_existing_error_log_file "$hard_link_path" 1 \
		2>"$TEST_TMPDIR/planted_hard_link.stderr"
	hard_link_status=$?
	perms=$(zxfer_get_path_mode_octal "$target_path") || perms=""

	assertEquals "A symlink at the new log's name should be refused." 1 "$symlink_status"
	assertContains "The symlink refusal should explain it." \
		"$(cat "$TEST_TMPDIR/planted_symlink.stderr")" "because it is a symlink"
	assertEquals "A hard link at the new log's name should be refused." 1 "$hard_link_status"
	assertContains "The hard-link refusal should give the link count." \
		"$(cat "$TEST_TMPDIR/planted_hard_link.stderr")" "because it has 2 hard links"
	assertEquals "The planted entries' target must keep its mode." "644" "$perms"
}

test_error_log_parent_and_created_mode_checks_work_in_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	created_log="$physical_tmpdir/created_meta_fail.log"
	: >"$created_log"
	chmod 600 "$created_log"

	parent_output=$(
		(
			zxfer_find_symlink_path_component() { return 1; }
			zxfer_get_path_parent_dir() {
				printf 'unexpected-parent-helper\n'
				return 99
			}
			zxfer_validate_temp_root_candidate() {
				printf 'parent=%s\n' "$1" >&2
				return 0
			}
			zxfer_validate_error_log_parent "/top.log"
			zxfer_validate_error_log_parent "/"
			zxfer_validate_error_log_parent "$physical_tmpdir/sub.log"
		) 2>&1
	)
	assertEquals "Error-log parent validation should derive / and directory parents in-shell." \
		"parent=/
parent=/
parent=$physical_tmpdir" "$parent_output"

	mode_output=$(
		(
			l_meta_calls=0
			zxfer_get_path_mode_octal() {
				printf 'unexpected-mode-helper\n' >&2
				return 99
			}
			zxfer_read_path_metadata() {
				l_meta_calls=$((l_meta_calls + 1))
				if [ "$l_meta_calls" -eq 1 ]; then
					zxfer_parse_path_metadata_line "$(command ls -ldin "$1")"
					return
				fi
				g_zxfer_path_mode_result=""
				return 1
			}
			zxfer_validate_existing_error_log_file "$created_log" 1
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	assertContains "Created-log post-chmod verification should refuse when metadata read fails." \
		"$mode_output" "because its permissions could not be determined."
	assertContains "Created-log post-chmod metadata failure should return 1." \
		"$mode_output" "status=1"
	assertNotContains "Created-log post-chmod verification should not call zxfer_get_path_mode_octal." \
		"$mode_output" "unexpected-mode-helper"
}

# Opening a FIFO for writing blocks until someone reads it, which would hang
# zxfer's EXIT trap; a FIFO at the log path must be refused without an open.
test_zxfer_append_failure_report_to_log_refuses_a_fifo_without_opening_it() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/fifo.log"
	stderr_file="$TEST_TMPDIR/error_log_fifo.stderr"
	status_file="$TEST_TMPDIR/error_log_fifo.status"
	mkfifo "$log_path" || fail "Unable to create the FIFO fixture."
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_append_failure_report_to_log "message: should-not-append" \
			>/dev/null 2>"$stderr_file"
		printf '%s\n' "$?" >"$status_file"
	) &
	append_pid=$!
	l_tries=0
	while [ ! -s "$status_file" ] && [ "$l_tries" -lt 50 ]; do
		sleep 0.1 2>/dev/null || sleep 1
		l_tries=$((l_tries + 1))
	done
	blocked=0
	if [ ! -s "$status_file" ]; then
		# A read-write open of the FIFO releases a writer blocked in its open.
		blocked=1
		(true <>"$log_path")
	fi
	wait "$append_pid"

	assertEquals "zxfer must not open a FIFO at the log path." 0 "$blocked"
	assertEquals "A FIFO at the log path should be refused." "1" "$(cat "$status_file")"
	assertContains "The FIFO refusal should explain it." \
		"$(cat "$stderr_file")" "because it is not a regular file"
}

# Linux caps one exec argument or environment string at 128 KiB, so a report
# this size arrives whole only through the pipe to awk.
test_zxfer_append_failure_report_to_log_mirrors_a_report_larger_than_an_exec_string() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/large_report.log"
	expected_file="$TEST_TMPDIR/large_report.expected"
	awk 'BEGIN {
		printf "zxfer: failure report begin\nlast_command: "
		for (i = 0; i < 300000; i++)
			printf "x"
		printf "\nzxfer: failure report end\n"
	}' >"$expected_file"
	report=$(cat "$expected_file")
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "$report" 2>"$TEST_TMPDIR/large_report.stderr"
	status=$?

	assertEquals "A report larger than an exec string should be mirrored." 0 "$status"
	assertTrue "The log should hold the whole report." \
		"cmp -s '$expected_file' '$log_path'"
}

# Purpose: Run the launcher to a usage failure that mirrors its report to
# LOG_PATH, with verbatim command fields so the report names TAG's source.
# Usage: zxfer_test_error_log_usage_failure SECURE_PATH_DIR LOG_PATH TAG;
# writes TEST_TMPDIR/concurrent_TAG.stdout and .stderr.
zxfer_test_error_log_usage_failure() {
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$1:/sbin:/bin:/usr/sbin:/usr/bin" \
		ZXFER_ERROR_LOG="$2" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "tank/$3" \
		>"$TEST_TMPDIR/concurrent_$3.stdout" 2>"$TEST_TMPDIR/concurrent_$3.stderr"
}

# Purpose: Print each failure-report block of FILE... as one tab-joined line,
# sorted, so logs holding the same reports in any order compare equal and an
# interleaved block does not.
# Usage: zxfer_test_error_log_blocks_as_lines FILE...
zxfer_test_error_log_blocks_as_lines() {
	awk '
		{ block = block (block == "" ? "" : "\t") $0 }
		$0 == "zxfer: failure report end" { print block; block = "" }
		END { if (block != "") print "unterminated\t" block }
	' "$@" | LC_ALL=C sort
}

# Failing runs that start together race to create the log and then append at
# the same time. Each report must land whole and once, in any order, and
# nothing but the log may be left beside it. Four runs, not two: bash
# line-buffers its printf, and a printf writer broke about one round in
# twenty with two runs but most rounds with four.
test_concurrent_failing_runs_append_intact_reports() {
	secure_path_dir="$TEST_TMPDIR/concurrent_secure_path"
	log_dir="$TEST_TMPDIR/concurrent_log_parent"
	log_path="$log_dir/failure.log"
	create_launcher_usage_secure_path "$secure_path_dir" || return
	mkdir -p "$log_dir"

	set +e
	l_pids=""
	for l_tag in one two three four; do
		zxfer_test_error_log_usage_failure "$secure_path_dir" "$log_path" "$l_tag" &
		l_pids="$l_pids $!"
	done
	l_statuses=""
	for l_pid in $l_pids; do
		wait "$l_pid"
		l_statuses="$l_statuses $?"
	done
	l_reports=""
	for l_tag in one two three four; do
		sed -n '/^zxfer: failure report begin$/,/^zxfer: failure report end$/p' \
			"$TEST_TMPDIR/concurrent_$l_tag.stderr" >"$TEST_TMPDIR/concurrent_$l_tag.report"
		l_reports="$l_reports $TEST_TMPDIR/concurrent_$l_tag.report"
	done
	# shellcheck disable=SC2086 # One word per report file.
	expected_blocks=$(zxfer_test_error_log_blocks_as_lines $l_reports)

	assertEquals "Every run should exit with the usage status." " 2 2 2 2" "$l_statuses"
	assertEquals "Each run's stderr should hold one report naming its own source." \
		"1 1 1 1" "$(for l_tag in one two three four; do
			grep -c "'tank/$l_tag'" "$TEST_TMPDIR/concurrent_$l_tag.report"
		done | tr '\n' ' ' | sed 's/ $//')"
	assertEquals "The log should hold every stderr report, each whole and once." \
		"$expected_blocks" "$(zxfer_test_error_log_blocks_as_lines "$log_path")"
	assertNotContains "No run should warn." \
		"$(cat "$TEST_TMPDIR"/concurrent_*.stderr)" "warning:"
	assertEquals "Only the log should remain in its parent." \
		"failure.log" "$(ls -A "$log_dir")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
