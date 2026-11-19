#!/bin/sh
#
# shunit2 tests for the coverage runner script.
# Quoted commands run in an isolated shell and shunit2 discovers tests by name.
# shellcheck disable=SC2016,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_coverage"
	RUN_COVERAGE_BIN="$ZXFER_ROOT/tests/run_coverage.sh"
}

run_coverage_helper() {
	l_command=$1
	shift
	env -i \
		PATH="${PATH:-/usr/bin:/bin}" \
		TMPDIR="${TMPDIR:-/tmp}" \
		TEST_TMPDIR="$TEST_TMPDIR" \
		ZXFER_RUN_COVERAGE_SOURCE_ONLY=1 \
		/bin/sh -c '. "$0"; '"$l_command" "$RUN_COVERAGE_BIN" "$@"
}

# Each report case writes its own source and trace under a separate root.
prepare_coverage_fixture() {
	l_fake_root="$TEST_TMPDIR/$1"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$l_fake_root/targets.list"
	l_trace_file="$l_fake_root/merged.trace"
	l_summary_file="$l_fake_root/summary.tsv"
	l_missing_file="$l_fake_root/missing.txt"
	if ! mkdir -p "$l_fake_root/src" || ! printf '%s\n' "$l_source_file" >"$l_target_list_file"; then
		fail "Unable to prepare coverage fixture $1."
	fi
}

render_coverage_fixture() {
	run_coverage_helper 'ZXFER_ROOT=$1; shift; render_bash_xtrace_report "$@"' \
		"$l_fake_root" "$l_target_list_file" "$l_trace_file" "$l_summary_file" "$l_missing_file" || return "$?"
	cat "$l_summary_file"
	printf '%s\n' '---'
	cat "$l_missing_file"
}

# Bash 4.1 introduced BASH_XTRACEFD. Check that prerequisite independently of
# the capture helper so a regression in the helper still fails its tests.
zxfer_test_bash_supports_xtracefd() {
	l_test_bash_bin=$1
	"$l_test_bash_bin" --noprofile --norc -c '
		[ "${BASH_VERSINFO[0]}" -gt 4 ] ||
			{ [ "${BASH_VERSINFO[0]}" -eq 4 ] &&
				[ "${BASH_VERSINFO[1]}" -ge 1 ]; }
	' >/dev/null 2>&1
}

test_run_coverage_rejects_the_removed_enforce_option_and_accepts_report_only() {
	output=$("$RUN_COVERAGE_BIN" --enforce 2>&1)
	status=$?

	assertEquals "The removed --enforce option must fail instead of silently running report-only coverage." \
		1 "$status"
	assertContains "The rejection should name the unknown option." \
		"$output" "Unknown argument: --enforce"

	output=$("$RUN_COVERAGE_BIN" --report-only --help)

	assertContains "The compatibility --report-only flag should still be accepted." \
		"$output" "Coverage is report-only"
}

test_run_coverage_default_suite_resolution_includes_coverage_overlays() {
	output=$(run_coverage_helper '
		ZXFER_COVERAGE_MODE=bash-xtrace
		COVERAGE_DIR="$TEST_TMPDIR/default-suites"
		run_with_bash_xtrace() { shift; printf "%s\n" "$@"; }
		main
	')

	assertContains "The default coverage run should include the send/receive suite covering pipeline and job supervision." \
		"$output" "tests/test_zxfer_send_receive.sh"
	assertContains "The default coverage run should include the remote host suite." \
		"$output" "tests/test_zxfer_remote_hosts.sh"
	assertContains "The default coverage run should include the property transfer suite that exercises the in-memory property tables." \
		"$output" "tests/test_zxfer_property_transfer.sh"
	assertContains "The default coverage run should include the destination state suite." \
		"$output" "tests/test_zxfer_destination_state.sh"
	assertNotContains "The default coverage run should not execute shared test scaffolding as a suite." \
		"$output" "tests/test_helper.sh"
}

test_collectors_keep_each_suite_and_merge_directory_as_one_argument() {
	suite_path="$TEST_TMPDIR/test [a].sh"
	second_suite="$TEST_TMPDIR/test-extra.sh"
	touch "$suite_path" "$second_suite" "$TEST_TMPDIR/test a.sh"
	output=$(run_coverage_helper '
		COVERAGE_DIR="$TEST_TMPDIR/coverage results"
		ZXFER_COVERAGE_MODE=bash-xtrace
		run_with_bash_xtrace() { shift; printf "suite=<%s>\n" "$@"; }
		main "$@"
	' "$suite_path" "$second_suite")
	assertEquals 0 "$?"
	assertEquals "suite=<$suite_path>
suite=<$second_suite>" "$output"

	output=$(run_coverage_helper '
		COVERAGE_DIR="$TEST_TMPDIR/coverage results"
		ZXFER_COVERAGE_MODE=kcov
		kcov() {
			if [ "$1" = --merge ]; then
				shift 2
				printf "merge=<%s>\n" "$@" >>"$TEST_TMPDIR/kcov-argv"
			else
				shift 2
				printf "suite=<%s>\n" "$@" >>"$TEST_TMPDIR/kcov-argv"
			fi
		}
		: >"$TEST_TMPDIR/kcov-argv"
		main "$@" >/dev/null && cat "$TEST_TMPDIR/kcov-argv"
	' "$suite_path" "$second_suite")
	assertEquals 0 "$?"
	assertEquals "suite=<$suite_path>
suite=<$second_suite>
merge=<$TEST_TMPDIR/coverage results/kcov/test [a]>
merge=<$TEST_TMPDIR/coverage results/kcov/test-extra>" "$output"
}

test_run_coverage_xtrace_shell_keeps_tracing_after_a_suite_closes_fd_9() {
	l_bash_bin=${ZXFER_COVERAGE_BASH_BIN:-}
	if [ -z "$l_bash_bin" ]; then
		l_bash_bin=$(command -v bash 2>/dev/null || true)
	fi
	if [ -z "$l_bash_bin" ] || [ ! -x "$l_bash_bin" ]; then
		return 0
	fi
	if ! zxfer_test_bash_supports_xtracefd "$l_bash_bin"; then
		startSkipping
		assertTrue "The available Bash predates BASH_XTRACEFD; descriptor-isolation coverage skipped." true
		endSkipping
		return 0
	fi
	l_trace_dir="$TEST_TMPDIR/xtrace-fd9"
	rm -rf "$l_trace_dir"
	mkdir -p "$l_trace_dir/probe"
	if ! run_coverage_helper 'bash_supports_xtrace_line_numbers "$@"' "$l_bash_bin" "$l_trace_dir/probe" >/dev/null 2>&1; then
		fail "The selected Bash should support the line-number trace format used by coverage."
		return 0
	fi
	l_script_file="$TEST_TMPDIR/trace-survives-fd9-close.sh"
	cat >"$l_script_file" <<'EOS'
#!/bin/sh
before=1
exec 9<&- 2>/dev/null || true
after=1
set -u
EOS

	run_coverage_helper '
		write_bash_xtrace_shell "$1" "$2" "$2/xtrace-shell"
		"$2/xtrace-shell" "$3"
	' "$l_bash_bin" "$l_trace_dir" "$l_script_file" >/dev/null 2>&1
	assertEquals "The traced suite should succeed." 0 "$?"
	output=$(cat "$l_trace_dir/trace-survives-fd9-close.trace")

	assertContains "The xtrace shell should keep tracing after a suite closes fd 9 for its own descriptor management." \
		"$output" "after=1"
	assertContains "Trace lines should name the suite file and line." \
		"$output" "+$l_script_file:4: after=1"
}

test_run_coverage_zxfer_wrapper_traces_the_launcher_and_its_modules() {
	l_bash_bin=${ZXFER_COVERAGE_BASH_BIN:-}
	if [ -z "$l_bash_bin" ]; then
		l_bash_bin=$(command -v bash 2>/dev/null || true)
	fi
	if [ -z "$l_bash_bin" ] || ! zxfer_test_bash_supports_xtracefd "$l_bash_bin"; then
		startSkipping
		assertTrue "No Bash with BASH_XTRACEFD; launcher tracing coverage skipped." true
		endSkipping
		return 0
	fi
	l_root="$TEST_TMPDIR/wrapped-root"
	l_trace_dir="$TEST_TMPDIR/wrapped-traces"
	rm -rf "$l_root" "$l_trace_dir"
	mkdir -p "$l_root/src" "$l_trace_dir"
	cat >"$l_root/zxfer" <<'EOS'
#!/bin/sh
. "${0%/*}/src/zxfer_mod.sh"
zxfer_mod_report "$0" "$@"
exit 3
EOS
	cat >"$l_root/src/zxfer_mod.sh" <<'EOS'
zxfer_mod_report() {
	printf 'launcher=%s args=%s|%s\n' "$1" "$2" "$3"
}
EOS
	chmod 700 "$l_root/zxfer"

	output=$(run_coverage_helper '
		ZXFER_ROOT=$1
		write_bash_xtrace_shell "$2" "$3" "$3/xtrace-shell"
		write_bash_xtrace_zxfer "$3/xtrace-zxfer"
		"$ZXFER_COVERAGE_ZXFER_BIN" "one arg" two
	' "$l_root" "$l_bash_bin" "$l_trace_dir")
	assertEquals "The wrapper should return the launcher's exit status." 3 "$?"
	trace=$(cat "$l_trace_dir"/zxfer.*.trace)

	assertContains "The wrapped run should see the real launcher as \$0 and keep each argument whole." \
		"$output" "launcher=$l_root/zxfer args=one arg|two"
	assertContains "The trace should hold the launcher's own lines." \
		"$trace" "+$l_root/zxfer:3: zxfer_mod_report"
	assertContains "The trace should hold the lines of the modules the launcher sources." \
		"$trace" "+$l_root/src/zxfer_mod.sh:2: printf"
}

test_run_coverage_appends_total_summary_row() {
	l_summary_file="$TEST_TMPDIR/summary.tsv"
	cat >"$l_summary_file" <<'EOF'
80.00	10	8	2	src/a.sh
50.00	4	2	2	src/b.sh
EOF

	output=$(run_coverage_helper 'append_total_summary_row "$1"; cat "$1"' "$l_summary_file")

	assertContains "The total-row helper should preserve the existing per-file entries." \
		"$output" "80.00	10	8	2	src/a.sh"
	assertContains "The total-row helper should append an aggregate TOTAL row." \
		"$output" "71.43	14	10	4	TOTAL"
}

test_run_coverage_appends_total_summary_row_replaces_existing_total() {
	l_summary_file="$TEST_TMPDIR/summary-existing-total.tsv"
	cat >"$l_summary_file" <<'EOF'
80.00	10	8	2	src/a.sh
50.00	4	2	2	src/b.sh
71.43	14	10	4	TOTAL
EOF

	output=$(run_coverage_helper 'append_total_summary_row "$1"; cat "$1"' "$l_summary_file")
	total_count=$(printf '%s\n' "$output" | grep -c 'TOTAL$')

	assertEquals "The total-row helper should keep only one aggregate TOTAL row when rerun." \
		"1" "$total_count"
	assertContains "The recomputed TOTAL row should still reflect only per-file rows." \
		"$output" "71.43	14	10	4	TOTAL"
}

test_run_coverage_render_bash_xtrace_report_uses_repo_relative_paths() {
	prepare_coverage_fixture 'relative paths'
	cat >"$l_source_file" <<'EOF'
#!/bin/sh
printf '%s\n' one
printf '%s\n' two
EOF
	printf '+%s/tests/../src/fake.sh:2: printf '\''%%s\\n'\'' one\n' "$l_fake_root" >"$l_trace_file"
	printf '+%s:2: repeated hit\n' "$l_source_file" >>"$l_trace_file"

	output=$(render_coverage_fixture)

	assertContains "The rendered summary should normalize target paths to repo-relative labels even when the trace path contains tests/../ segments." \
		"$output" "50.00	2	1	1	src/fake.sh"
	assertContains "The missing-line report should also use repo-relative headings." \
		"$output" "src/fake.sh"
	assertContains "The missing-line report should retain the uncovered source line." \
		"$output" "  3:printf '%s"
}

test_render_report_rejects_unreadable_inputs_and_accepts_empty_files() {
	for input in targets source trace; do
		prepare_coverage_fixture "missing-$input"
		touch "$l_source_file" "$l_trace_file"
		case $input in
		targets) l_missing_input=$l_target_list_file ;;
		source) l_missing_input=$l_source_file ;;
		trace) l_missing_input=$l_trace_file ;;
		esac
		rm "$l_missing_input"
		output=$(render_coverage_fixture 2>&1)
		status=$?
		assertNotEquals "A missing $input must fail report generation." 0 "$status"
		assertContains "The error should identify the unreadable input." "$output" "$l_missing_input"
		assertEquals "No successful per-file result should be published." "" "$(cat "$l_summary_file")"
	done

	prepare_coverage_fixture empty-inputs
	touch "$l_source_file" "$l_trace_file"
	output=$(render_coverage_fixture)
	assertEquals 0 "$?"
	assertContains "$output" "100.00	0	0	0	src/fake.sh"
	printf '%s\n' 'echo uncovered' >"$l_source_file"
	output=$(render_coverage_fixture)
	assertEquals 0 "$?"
	assertContains "$output" "0.00	1	0	1	src/fake.sh"
}

test_runner_reports_a_missing_target_after_the_suite_passes() {
	l_suite_dir="$TEST_TMPDIR/successful-suite"
	mkdir -p "$l_suite_dir"
	printf '#!/bin/sh\nexit 0\n' >"$l_suite_dir/run_shunit_tests.sh"
	chmod +x "$l_suite_dir/run_shunit_tests.sh"
	output=$(run_coverage_helper '
		TEST_DIR=$1
		COVERAGE_DIR="$TEST_TMPDIR/missing-target-report"
		ZXFER_COVERAGE_MODE=bash-xtrace
		ZXFER_COVERAGE_BASH_BIN=/bin/sh
		bash_supports_xtrace_line_numbers() { return 0; }
		write_target_file_list() { printf "%s\n" "$TEST_DIR/disappeared.sh" >"$1"; }
		main "$TEST_DIR/run_shunit_tests.sh"
	' "$l_suite_dir" 2>&1)
	status=$?
	assertNotEquals "A successful suite cannot mask a report read error." 0 "$status"
	assertContains "$output" "$l_suite_dir/disappeared.sh"
	assertNotContains "$output" "Coverage summary:"
}

test_run_coverage_render_bash_xtrace_report_ignores_untraceable_shell_syntax() {
	prepare_coverage_fixture syntax
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
(
printf '%s\n' one
)
case "$1" in
foo)
printf '%s\n' foo
;;
esac
message="line one
line two"
{
printf '%s\n' block
} <<EOF
payload
EOF
cat <<EOF >/dev/null
cat payload
EOF
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' one
+$l_source_file:13: printf '%s\n' block
+$l_source_file:17: cat
+$l_source_file:20: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "The bash-xtrace fallback should ignore case labels, heredoc bodies, grouping parens, and multiline string bodies when counting coverable lines." \
		"$output" "80.00	5	4	1	src/fake.sh"
	assertContains "Only the truly uncovered executable line should remain in the missing-line report." \
		"$output" "  7:printf '%s"
	assertNotContains "Case labels should not be treated as missing executable lines." \
		"$output" "foo)"
	assertNotContains "Here-doc bodies should not be treated as missing executable lines." \
		"$output" "payload"
	assertNotContains "Command here-doc bodies should not be treated as missing executable lines." \
		"$output" "cat payload"
	assertNotContains "Multiline string bodies should not be treated as missing executable lines." \
		"$output" "line two"
}

test_run_coverage_render_bash_xtrace_report_ignores_quoted_heredoc_payloads() {
	prepare_coverage_fixture quoted-heredoc
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
{
printf '%s\n' block
} <<-'QUOTED'
	if payload-were-counted; then
		this-would-be-a-miss
	fi
QUOTED
cat <<\ESCAPED
another payload miss
ESCAPED
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' block
+$l_source_file:9: cat
+$l_source_file:12: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "Single-quoted and backslash-quoted heredoc delimiters should exclude their payloads from the bash-xtrace denominator." \
		"$output" "100.00	3	3	0	src/fake.sh"
	assertNotContains "A quoted heredoc payload should not be reported as uncovered shell code." \
		"$output" "payload-were-counted"
	assertNotContains "Control-flow terminators carrying quoted heredocs should not be counted as executable misses." \
		"$output" "} <<-'QUOTED'"
}

test_run_coverage_render_bash_xtrace_report_ignores_compound_control_delimiters() {
	prepare_coverage_fixture control-delimiters
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
if (
printf '%s\n' condition
); then
printf '%s\n' branch
fi
if ! (
false
); then
printf '%s\n' negated
fi
while IFS= read -r line; do
printf '%s\n' "$line"
done <"$1"
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' condition
+$l_source_file:5: printf '%s\n' branch
+$l_source_file:8: false
+$l_source_file:10: printf '%s\n' negated
+$l_source_file:12: IFS= read -r line
+$l_source_file:13: printf '%s\n' payload
+$l_source_file:15: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "Subshell conditions and redirected loop terminators should not add syntax-only lines to the bash-xtrace denominator." \
		"$output" "100.00	7	7	0	src/fake.sh"
	assertNotContains "An if-subshell opener should not be reported as an uncovered command." \
		"$output" "if ("
	assertNotContains "A redirected loop terminator should not be reported as an uncovered command." \
		"$output" "done <\"\$1\""
}

test_run_coverage_render_bash_xtrace_report_ignores_multiline_command_substitutions() {
	prepare_coverage_fixture command-subst
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
captured=$(
printf '%s\n' one
)
printf '%s\n' "$captured"
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:5: printf '%s\n' "\$captured"
TRACE

	output=$(render_coverage_fixture)

	assertContains "The bash-xtrace fallback should ignore multiline command-substitution bodies that bash does not trace with line numbers." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "Multiline command-substitution bodies should not be treated as missing executable lines." \
		"$output" "captured=\$("
	assertNotContains "The inner command-substitution body should not appear as uncovered shell code." \
		"$output" "one"
}

test_run_coverage_render_bash_xtrace_report_ignores_unattributable_command_substitution_openers() {
	prepare_coverage_fixture command-subst-openers
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
continued=$(render_value \
	'one')
piped=$(printf '%s\n' input |
	sed 's/input/output/')
same_line=$(printf '%s\n' same)
printf '%s\n' "$continued:$piped:$same_line"
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: render_value one
+$l_source_file:3: continued=one
+$l_source_file:5: printf '%s\n' input
+$l_source_file:5: sed s/input/output/
+$l_source_file:5: piped=output
+$l_source_file:6: printf '%s\n' same
+$l_source_file:6: same_line=same
+$l_source_file:7: printf '%s\n' one:output:same
TRACE

	output=$(render_coverage_fixture)

	assertContains "Command-substitution opener lines that Bash attributes to a later physical line should not become structural coverage misses." \
		"$output" "100.00	3	3	0	src/fake.sh"
	# shellcheck disable=SC1003,SC2016  # Exact shell source fragments, not expansions.
	assertNotContains "A backslash-continued command-substitution opener should not appear as uncovered shell code." \
		"$output" 'continued=$(render_value \'
	assertNotContains "A pipeline command-substitution opener should not appear as uncovered shell code." \
		"$output" "piped=\$(printf"
}

test_run_coverage_render_bash_xtrace_report_ignores_quoted_closes_in_command_substitution_openers() {
	prepare_coverage_fixture command-subst-quoted-close
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
continued=$(printf "%s)" \
	input)
printf '%s\n' "$continued"
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s)' input
+$l_source_file:3: continued='input)'
+$l_source_file:4: printf '%s\n' 'input)'
TRACE

	output=$(render_coverage_fixture)

	assertContains "A close parenthesis inside a command-substitution string literal should not expose the opener as an uncovered command." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "The command-substitution opener should remain structural when Bash attributes it to the continued line." \
		"$output" 'continued=$(printf'
}

test_run_coverage_render_bash_xtrace_report_balances_nested_parentheses_in_command_substitution_openers() {
	prepare_coverage_fixture command-subst-nested-opener
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
continued=$( (printf '%s\n' one) |
	sed 's/one/two/')
printf '%s\n' "$continued"
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' one
+$l_source_file:3: sed s/one/two/
+$l_source_file:3: continued=two
+$l_source_file:4: printf '%s\n' two
TRACE

	output=$(render_coverage_fixture)

	assertContains "Nested subshell parentheses should not close the surrounding command substitution early." \
		"$output" "100.00	2	2	0	src/fake.sh"
	assertNotContains "A command-substitution opener with a nested subshell should not become an uncovered command." \
		"$output" 'continued=$( (printf'
}

test_run_coverage_render_bash_xtrace_report_ignores_command_substitution_close_lines_with_redirections() {
	prepare_coverage_fixture command-subst-redir
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
captured=$(
printf '%s\n' one
) 2>/dev/null
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:5: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "A command-substitution close line with redirections should end the ignored multiline body." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "The multiline command-substitution opening line should not be reported as missing shell code." \
		"$output" "captured=\$("
	assertNotContains "The command-substitution body should not appear as uncovered shell code." \
		"$output" "one"
}

test_run_coverage_render_bash_xtrace_report_tracks_nested_scopes_inside_multiline_command_substitutions() {
	prepare_coverage_fixture command-subst-nested
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
captured=$(
(
printf '%s\n' one
) 2>/dev/null
printf '%s\n' two
)
printf '%s\n' "$captured"
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:8: printf '%s\n' "\$captured"
TRACE

	output=$(render_coverage_fixture)

	assertContains "Nested subshell closes inside a multiline command substitution should not terminate the ignored body early." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "Lines that still belong to the multiline command substitution body should not appear as uncovered shell code after an inner subshell close." \
		"$output" "two"
}

test_run_coverage_render_bash_xtrace_report_ignores_multiline_single_quoted_bodies() {
	prepare_coverage_fixture single-quote
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
awk '
BEGIN {
	print "hello"
}
' "$1"
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:7: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "The bash-xtrace fallback should ignore multiline single-quoted command bodies such as embedded awk programs." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "The opening awk quote should not be treated as missing shell code." \
		"$output" "awk '"
	assertNotContains "Inner awk-program lines should not appear as uncovered shell code." \
		"$output" "print \"hello\""
}

test_run_coverage_ignores_multiline_single_quote_openers_with_inline_data() {
	prepare_coverage_fixture inline-single-quote
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
MANIFEST='first-item
second-item
third-item'
printf '%s\n' "$MANIFEST"
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:5: printf '%s\n' "\$MANIFEST"
TRACE

	output=$(render_coverage_fixture)

	assertContains "Inline data after an opening single quote should still start a non-coverable multiline body." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "Manifest data lines should not be reported as executable misses." \
		"$output" "second-item"
}

test_run_coverage_single_quoted_double_quotes_do_not_hide_following_executable_lines() {
	prepare_coverage_fixture single-quoted-double-quote
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
MANIFEST='first " item
second-item
third-item'
printf '%s\n' still-coverable
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:6: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "Double quotes inside a single-quoted multiline value must not hide later executable lines from the denominator." \
		"$output" "50.00	2	1	1	src/fake.sh"
	assertContains "The executable line after the single-quoted value should remain visible as a miss." \
		"$output" "  5:printf '%s"
	assertNotContains "The genuine multiline value body should remain excluded." \
		"$output" "second-item"
}

# shellcheck disable=SC1003  # The assertion matches literal shell source.
test_run_coverage_does_not_treat_escaped_or_double_quoted_apostrophes_as_multiline_openers() {
	prepare_coverage_fixture literal-apostrophe
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
escaped=${escaped#*\'}
quoted="an apostrophe isn't a shell quote here"
printf '%s\n' before-comment;# operator isn't a shell quote
printf '%s\n' still-coverable
MANIFEST='first-item
second-item\'
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:8: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "Escaped and double-quoted apostrophes must not hide the executable lines that follow them from the coverage denominator." \
		"$output" "20.00	5	1	4	src/fake.sh"
	assertContains "A parameter-pattern apostrophe should remain a coverable shell assignment." \
		"$output" '  2:escaped=${escaped#*\'"'"'}'
	assertContains "Executable lines following literal apostrophes should remain visible as misses." \
		"$output" "  5:printf '%s"
	assertNotContains "A genuine multiline single-quoted body should remain excluded." \
		"$output" "second-item"
}

test_run_coverage_render_bash_xtrace_report_ignores_multiline_single_quoted_bodies_started_on_backslash_continuations() {
	prepare_coverage_fixture single-quote-continuation
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
awk \
	-v mode=1 '
BEGIN {
	print "hello"
}
' "$1"
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:2: awk -v mode=1 ...
+$l_source_file:8: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "The bash-xtrace fallback should keep ignoring multiline single-quoted bodies when the opening quote starts on a backslash-continuation line." \
		"$output" "100.00	2	2	0	src/fake.sh"
	assertNotContains "The embedded awk body should not reappear as uncovered shell code when its opening quote follows a continuation line." \
		"$output" "print \"hello\""
}

test_run_coverage_render_bash_xtrace_report_ignores_backslash_continuation_lines() {
	prepare_coverage_fixture continuation
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
rm -f "$1" \
	"$2" \
	"$3"
printf '%s\n' done
SCRIPT
	cat >"$l_trace_file" <<TRACE
+$l_source_file:2: rm -f "$1" "$2" "$3"
+$l_source_file:5: printf '%s\n' done
TRACE

	output=$(render_coverage_fixture)

	assertContains "The bash-xtrace fallback should count only the first line of a backslash-continued shell command." \
		"$output" "100.00	2	2	0	src/fake.sh"
	assertNotContains "Backslash continuation payload lines should not appear as uncovered shell code." \
		"$output" "\"\$2\" \\"
}

test_run_coverage_render_bash_xtrace_report_overwrites_existing_outputs() {
	prepare_coverage_fixture overwrite
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
printf '%s\n' one
printf '%s\n' two
SCRIPT
	printf '+%s:2: printf '\''%%s\\n'\'' one\n' "$l_source_file" >"$l_trace_file"
	cat >"$l_summary_file" <<'EOF'
99.00	1	1	0	src/stale.sh
99.00	1	1	0	TOTAL
EOF
	cat >"$l_missing_file" <<'EOF'
src/stale.sh
  1:stale
EOF

	output=$(render_coverage_fixture)

	assertContains "The renderer should replace stale summary content with the current target set." \
		"$output" "50.00	2	1	1	src/fake.sh"
	assertNotContains "The renderer should not append to stale summary rows from prior runs." \
		"$output" "src/stale.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
