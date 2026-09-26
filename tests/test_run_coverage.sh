#!/bin/sh
#
# shunit2 tests for the coverage runner script.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_coverage"
	RUN_COVERAGE_BIN="$ZXFER_ROOT/tests/run_coverage.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
run_coverage_helper() {
	l_command=$1
	env -i \
		PATH="${PATH:-/usr/bin:/bin}" \
		TMPDIR="${TMPDIR:-/tmp}" \
		TEST_TMPDIR="$TEST_TMPDIR" \
		RUN_COVERAGE_BIN="$RUN_COVERAGE_BIN" \
		ZXFER_RUN_COVERAGE_SOURCE_ONLY=1 \
		/bin/sh -c ". \"$RUN_COVERAGE_BIN\"; $l_command"
}

# Bash 4.1 introduced BASH_XTRACEFD. Check that prerequisite independently of
# the capture helper so a regression in the helper still fails its tests.
# shellcheck disable=SC2016,SC2329  # Bash expands this; shunit tests call the helper indirectly.
zxfer_test_bash_supports_xtracefd() {
	l_test_bash_bin=$1
	"$l_test_bash_bin" --noprofile --norc -c '
		[ "${BASH_VERSINFO[0]}" -gt 4 ] ||
			{ [ "${BASH_VERSINFO[0]}" -eq 4 ] &&
				[ "${BASH_VERSINFO[1]}" -ge 1 ]; }
	' >/dev/null 2>&1
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_rejects_the_removed_enforce_option_and_accepts_report_only() {
	set +e
	output=$("$RUN_COVERAGE_BIN" --enforce 2>&1)
	status=$?
	set -e

	assertEquals "The removed --enforce option must fail instead of silently running report-only coverage." \
		1 "$status"
	assertContains "The rejection should name the unknown option." \
		"$output" "Unknown argument: --enforce"

	output=$("$RUN_COVERAGE_BIN" --report-only --help)

	assertContains "The compatibility --report-only flag should still be accepted." \
		"$output" "Coverage is report-only"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Invoked indirectly by shunit2; command expands inside the helper shell.
test_run_coverage_default_suite_resolution_includes_coverage_overlays() {
	output=$(run_coverage_helper 'ZXFER_ROOT=$(cd "$(dirname "$RUN_COVERAGE_BIN")/.." && pwd); TEST_DIR="$ZXFER_ROOT/tests"; resolve_suites | while IFS= read -r suite; do case "$suite" in "$ZXFER_ROOT"/*) printf "%s\n" "${suite#$ZXFER_ROOT/}" ;; *) printf "%s\n" "$suite" ;; esac; done')

	assertContains "The default coverage run should include the send-job coverage suite." \
		"$output" "tests/test_zxfer_send_jobs.sh"
	assertContains "The default coverage run should include the remote host suite." \
		"$output" "tests/test_zxfer_remote_hosts.sh"
	assertContains "The default coverage run should include the property reconcile suite that exercises the in-memory property tables." \
		"$output" "tests/test_zxfer_property_reconcile.sh"
	assertContains "The default coverage run should include the snapshot state suite that protects transform readback coverage." \
		"$output" "tests/test_zxfer_snapshot_state.sh"
	assertNotContains "The default coverage run should not execute shared test scaffolding as a suite." \
		"$output" "tests/test_helper.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_capture_bash_xtrace_to_file_survives_fd_9_closure() {
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
	l_support_status=$(run_coverage_helper \
		"if bash_supports_xtrace_line_numbers \"$l_bash_bin\" >/dev/null 2>&1; then printf '%s' 0; else printf '%s' 1; fi")
	if [ "$l_support_status" != "0" ]; then
		fail "The selected Bash should support the line-number trace format used by coverage."
		return 0
	fi
	l_script_file="$TEST_TMPDIR/trace-survives-fd9-close.sh"
	l_trace_file="$TEST_TMPDIR/trace-survives-fd9-close.trace"

	cat >"$l_script_file" <<'EOF'
#!/bin/sh
before=1
exec 9<&- 2>/dev/null || true
after=1
set -u
EOF

	output=$(run_coverage_helper \
		"if capture_bash_xtrace_to_file \"$l_bash_bin\" \"$l_trace_file\" \"$l_script_file\" >/dev/null 2>&1; then l_capture_status=0; else l_capture_status=\$?; fi; printf 'capture_status=%s\\n' \"\$l_capture_status\"; cat \"$l_trace_file\"")

	assertContains "The bash-xtrace capture helper should report a successful traced process." \
		"$output" "capture_status=0"
	if ! printf '%s\n' "$output" | grep -F -- 'after=1' >/dev/null; then
		fail "The bash-xtrace capture helper should keep tracing after a suite closes fd 9 for its own descriptor management. Output: $output"
	fi
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_refuses_to_signal_a_reused_descendant_pid() {
	output=$(run_coverage_helper '
		coverage_get_process_start_token() {
			printf "%s\n" "lstart:new-process"
		}
		coverage_send_signal_to_pid() {
			printf "signal=%s pid=%s\n" "$1" "$2"
		}
		tab=$(printf "\t")
		record="43210${tab}lstart:original-process"
		coverage_signal_process_tree TERM "$record"
	')

	assertEquals "A changed process-start token must prevent TERM/KILL from touching a reused PID." \
		"" "$output"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_refuses_to_signal_a_reused_root_pid() {
	output=$(run_coverage_helper '
		coverage_get_process_start_token() {
			printf "%s\n" "lstart:new-process"
		}
		coverage_send_signal_to_pid() {
			printf "signal=%s pid=%s\n" "$1" "$2"
		}
		coverage_signal_tracked_process TERM 43210 "lstart:original-process"
	')

	assertEquals "A changed root process-start token must prevent TERM/KILL from touching a reused PID." \
		"" "$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_term_exits_and_reaps_the_active_suite() {
	l_bash_bin=${ZXFER_COVERAGE_BASH_BIN:-}
	if [ -z "$l_bash_bin" ]; then
		l_bash_bin=$(command -v bash 2>/dev/null || true)
	fi
	if [ -z "$l_bash_bin" ] || [ ! -x "$l_bash_bin" ]; then
		startSkipping
		assertTrue "No executable Bash is available; coverage-runner TERM cleanup skipped." true
		endSkipping
		return 0
	fi
	if ! zxfer_test_bash_supports_xtracefd "$l_bash_bin"; then
		startSkipping
		assertTrue "The available Bash predates BASH_XTRACEFD; coverage-runner TERM cleanup skipped." true
		endSkipping
		return 0
	fi
	l_support_status=$(run_coverage_helper \
		"if bash_supports_xtrace_line_numbers \"$l_bash_bin\" >/dev/null 2>&1; then printf '%s' 0; else printf '%s' 1; fi")
	if [ "$l_support_status" != "0" ]; then
		fail "The selected Bash should support the line-number trace format used by coverage."
		return 0
	fi
	l_suite_file="$TEST_TMPDIR/coverage-term-suite.sh"
	l_suite_pid_file="$TEST_TMPDIR/coverage-term-suite.pid"
	l_child_pid_file="$TEST_TMPDIR/coverage-term-child.pid"
	l_grandchild_pid_file="$TEST_TMPDIR/coverage-term-grandchild.pid"
	l_output_file="$TEST_TMPDIR/coverage-term-runner.out"
	l_coverage_dir="$TEST_TMPDIR/coverage-term-output"
	l_fake_bin="$TEST_TMPDIR/coverage-term-bin"
	mkdir -p "$l_fake_bin"
	cat >"$l_fake_bin/pgrep" <<'EOF'
#!/bin/sh
[ "$#" -eq 2 ] && [ "$1" = "-P" ] || exit 2
l_parent_pid=$2
l_suite_pid=$(cat "${COVERAGE_TERM_SUITE_PID_FILE:?}" 2>/dev/null || :)
l_child_pid=$(cat "${COVERAGE_TERM_CHILD_PID_FILE:?}" 2>/dev/null || :)
if [ -n "$l_suite_pid" ] && [ "$l_parent_pid" = "$l_suite_pid" ]; then
	cat "${COVERAGE_TERM_CHILD_PID_FILE:?}"
	check_status=$?
	[ "$check_status" -eq 0 ] || exit "$check_status"
	exit 0
fi
if [ -n "$l_child_pid" ] && [ "$l_parent_pid" = "$l_child_pid" ]; then
	cat "${COVERAGE_TERM_GRANDCHILD_PID_FILE:?}"
	check_status=$?
	[ "$check_status" -eq 0 ] || exit "$check_status"
	exit 0
fi
exit 1
EOF
	chmod +x "$l_fake_bin/pgrep"
	cat >"$l_fake_bin/ps" <<'EOF'
#!/bin/sh
if [ "$#" -eq 4 ] && [ "$1" = "-p" ] && [ "$3" = "-o" ]; then
	case "$4" in
	lstart=)
		printf '%s\n' 'Fri Jul 17 12:00:00 2026'
		exit 0
		;;
	stime=)
		printf '%s\n' '12:00:00'
		exit 0
		;;
	esac
fi
exec /bin/ps "$@"
EOF
	chmod +x "$l_fake_bin/ps"
	cat >"$l_suite_file" <<'EOF'
#!/bin/sh
printf '%s\n' "$$" >"${COVERAGE_TERM_SUITE_PID_FILE:?}"
(
	trap '' TERM
	sh -c 'trap "" TERM; while :; do sleep 1; done' &
	printf '%s\n' "$!" >"${COVERAGE_TERM_GRANDCHILD_PID_FILE:?}"
	wait
) &
printf '%s\n' "$!" >"${COVERAGE_TERM_CHILD_PID_FILE:?}"
trap '' TERM
while :; do
	sleep 1
done
EOF
	chmod +x "$l_suite_file"

	COVERAGE_TERM_SUITE_PID_FILE="$l_suite_pid_file" \
		COVERAGE_TERM_CHILD_PID_FILE="$l_child_pid_file" \
		COVERAGE_TERM_GRANDCHILD_PID_FILE="$l_grandchild_pid_file" \
		COVERAGE_SIGNAL_SHUTDOWN_GRACE_SECONDS=1 \
		ZXFER_COVERAGE_MODE=bash-xtrace \
		ZXFER_COVERAGE_BASH_BIN="$l_bash_bin" \
		COVERAGE_DIR="$l_coverage_dir" \
		PATH="$l_fake_bin:${PATH:-/usr/bin:/bin}" \
		"$RUN_COVERAGE_BIN" --report-only "$l_suite_file" >"$l_output_file" 2>&1 &
	l_runner_pid=$!
	l_wait_count=0
	while { [ ! -s "$l_suite_pid_file" ] || [ ! -s "$l_child_pid_file" ] || [ ! -s "$l_grandchild_pid_file" ]; } &&
		[ "$l_wait_count" -lt 10 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -s "$l_suite_pid_file" ] || [ ! -s "$l_child_pid_file" ] || [ ! -s "$l_grandchild_pid_file" ]; then
		kill -s KILL "$l_runner_pid" >/dev/null 2>&1 || :
		wait "$l_runner_pid" >/dev/null 2>&1 || :
		fail "The coverage runner did not start its selected suite within the bounded wait. Output: $(cat "$l_output_file" 2>/dev/null || :)"
		return
	fi
	l_suite_pid=$(cat "$l_suite_pid_file")
	l_child_pid=$(cat "$l_child_pid_file")
	l_grandchild_pid=$(cat "$l_grandchild_pid_file")

	kill -s TERM "$l_runner_pid"
	set +e
	wait "$l_runner_pid"
	l_runner_status=$?
	set -e

	assertEquals "TERM should end the coverage runner with the conventional signal-derived status." \
		143 "$l_runner_status"
	for l_reaped_record in \
		"suite:$l_suite_pid" \
		"child:$l_child_pid" \
		"grandchild:$l_grandchild_pid"; do
		l_reaped_role=${l_reaped_record%%:*}
		l_reaped_pid=${l_reaped_record#*:}
		set +e
		run_coverage_helper "coverage_process_running_p '$l_reaped_pid'" >/dev/null 2>&1
		l_suite_running_status=$?
		set -e
		assertNotEquals "TERM should not leave the selected suite or any captured descendant live after the coverage runner exits ($l_reaped_role pid $l_reaped_pid)." \
			0 "$l_suite_running_status"
	done
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_appends_total_summary_row() {
	l_summary_file="$TEST_TMPDIR/summary.tsv"
	cat >"$l_summary_file" <<'EOF'
80.00	10	8	2	src/a.sh
50.00	4	2	2	src/b.sh
EOF

	output=$(run_coverage_helper "append_total_summary_row \"$l_summary_file\"; cat \"$l_summary_file\"")

	assertContains "The total-row helper should preserve the existing per-file entries." \
		"$output" "80.00	10	8	2	src/a.sh"
	assertContains "The total-row helper should append an aggregate TOTAL row." \
		"$output" "71.43	14	10	4	TOTAL"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_appends_total_summary_row_replaces_existing_total() {
	l_summary_file="$TEST_TMPDIR/summary-existing-total.tsv"
	cat >"$l_summary_file" <<'EOF'
80.00	10	8	2	src/a.sh
50.00	4	2	2	src/b.sh
71.43	14	10	4	TOTAL
EOF

	output=$(run_coverage_helper "append_total_summary_row \"$l_summary_file\"; cat \"$l_summary_file\"")
	total_count=$(printf '%s\n' "$output" | grep -c 'TOTAL$')

	assertEquals "The total-row helper should keep only one aggregate TOTAL row when rerun." \
		"1" "$total_count"
	assertContains "The recomputed TOTAL row should still reflect only per-file rows." \
		"$output" "71.43	14	10	4	TOTAL"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_uses_repo_relative_paths() {
	l_fake_root="$TEST_TMPDIR/fake-root"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets.list"
	l_trace_file="$TEST_TMPDIR/merged.trace"
	l_summary_file="$TEST_TMPDIR/render-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'EOF'
#!/bin/sh
printf '%s\n' one
printf '%s\n' two
EOF
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	printf '+%s/tests/../src/fake.sh:2: printf '\''%%s\\n'\'' one\n' "$l_fake_root" >"$l_trace_file"

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\")\"")

	assertContains "The rendered summary should normalize target paths to repo-relative labels even when the trace path contains tests/../ segments." \
		"$output" "50.00	2	1	1	src/fake.sh"
	assertContains "The missing-line report should also use repo-relative headings." \
		"$output" "src/fake.sh"
	assertContains "The missing-line report should retain the uncovered source line." \
		"$output" "  3:printf '%s"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_untraceable_shell_syntax() {
	l_fake_root="$TEST_TMPDIR/fake-root-syntax"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-syntax.list"
	l_trace_file="$TEST_TMPDIR/merged-syntax.trace"
	l_summary_file="$TEST_TMPDIR/render-syntax-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-syntax-missing.txt"

	mkdir -p "$l_fake_root/src"
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
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' one
+$l_source_file:13: printf '%s\n' block
+$l_source_file:17: cat
+$l_source_file:20: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\")\"")

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

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_quoted_heredoc_payloads() {
	l_fake_root="$TEST_TMPDIR/fake-root-quoted-heredoc"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-quoted-heredoc.list"
	l_trace_file="$TEST_TMPDIR/merged-quoted-heredoc.trace"
	l_summary_file="$TEST_TMPDIR/render-quoted-heredoc-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-quoted-heredoc-missing.txt"

	mkdir -p "$l_fake_root/src"
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
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' block
+$l_source_file:9: cat
+$l_source_file:12: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Single-quoted and backslash-quoted heredoc delimiters should exclude their payloads from the bash-xtrace denominator." \
		"$output" "100.00	3	3	0	src/fake.sh"
	assertNotContains "A quoted heredoc payload should not be reported as uncovered shell code." \
		"$output" "payload-were-counted"
	assertNotContains "Control-flow terminators carrying quoted heredocs should not be counted as executable misses." \
		"$output" "} <<-'QUOTED'"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_compound_control_delimiters() {
	l_fake_root="$TEST_TMPDIR/fake-root-control-delimiters"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-control-delimiters.list"
	l_trace_file="$TEST_TMPDIR/merged-control-delimiters.trace"
	l_summary_file="$TEST_TMPDIR/render-control-delimiters-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-control-delimiters-missing.txt"

	mkdir -p "$l_fake_root/src"
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
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' condition
+$l_source_file:5: printf '%s\n' branch
+$l_source_file:8: false
+$l_source_file:10: printf '%s\n' negated
+$l_source_file:12: IFS= read -r line
+$l_source_file:13: printf '%s\n' payload
+$l_source_file:15: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Subshell conditions and redirected loop terminators should not add syntax-only lines to the bash-xtrace denominator." \
		"$output" "100.00	7	7	0	src/fake.sh"
	assertNotContains "An if-subshell opener should not be reported as an uncovered command." \
		"$output" "if ("
	assertNotContains "A redirected loop terminator should not be reported as an uncovered command." \
		"$output" "done <\"\$1\""
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_multiline_command_substitutions() {
	l_fake_root="$TEST_TMPDIR/fake-root-command-subst"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-command-subst.list"
	l_trace_file="$TEST_TMPDIR/merged-command-subst.trace"
	l_summary_file="$TEST_TMPDIR/render-command-subst-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-command-subst-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
captured=$(
printf '%s\n' one
)
printf '%s\n' "$captured"
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:5: printf '%s\n' "\$captured"
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "The bash-xtrace fallback should ignore multiline command-substitution bodies that bash does not trace with line numbers." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "Multiline command-substitution bodies should not be treated as missing executable lines." \
		"$output" "captured=\$("
	assertNotContains "The inner command-substitution body should not appear as uncovered shell code." \
		"$output" "one"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_unattributable_command_substitution_openers() {
	l_fake_root="$TEST_TMPDIR/fake-root-command-subst-openers"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-command-subst-openers.list"
	l_trace_file="$TEST_TMPDIR/merged-command-subst-openers.trace"
	l_summary_file="$TEST_TMPDIR/render-command-subst-openers-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-command-subst-openers-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
continued=$(render_value \
	'one')
piped=$(printf '%s\n' input |
	sed 's/input/output/')
same_line=$(printf '%s\n' same)
printf '%s\n' "$continued:$piped:$same_line"
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
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

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Command-substitution opener lines that Bash attributes to a later physical line should not become structural coverage misses." \
		"$output" "100.00	3	3	0	src/fake.sh"
	# shellcheck disable=SC1003,SC2016  # Exact shell source fragments, not expansions.
	assertNotContains "A backslash-continued command-substitution opener should not appear as uncovered shell code." \
		"$output" 'continued=$(render_value \'
	assertNotContains "A pipeline command-substitution opener should not appear as uncovered shell code." \
		"$output" "piped=\$(printf"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal fixture text; invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_quoted_closes_in_command_substitution_openers() {
	l_fake_root="$TEST_TMPDIR/fake-root-command-subst-quoted-close"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-command-subst-quoted-close.list"
	l_trace_file="$TEST_TMPDIR/merged-command-subst-quoted-close.trace"
	l_summary_file="$TEST_TMPDIR/render-command-subst-quoted-close-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-command-subst-quoted-close-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
continued=$(printf "%s)" \
	input)
printf '%s\n' "$continued"
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s)' input
+$l_source_file:3: continued='input)'
+$l_source_file:4: printf '%s\n' 'input)'
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "A close parenthesis inside a command-substitution string literal should not expose the opener as an uncovered command." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "The command-substitution opener should remain structural when Bash attributes it to the continued line." \
		"$output" 'continued=$(printf'
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal fixture text; invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_balances_nested_parentheses_in_command_substitution_openers() {
	l_fake_root="$TEST_TMPDIR/fake-root-command-subst-nested-opener"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-command-subst-nested-opener.list"
	l_trace_file="$TEST_TMPDIR/merged-command-subst-nested-opener.trace"
	l_summary_file="$TEST_TMPDIR/render-command-subst-nested-opener-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-command-subst-nested-opener-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
continued=$( (printf '%s\n' one) |
	sed 's/one/two/')
printf '%s\n' "$continued"
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' one
+$l_source_file:3: sed s/one/two/
+$l_source_file:3: continued=two
+$l_source_file:4: printf '%s\n' two
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Nested subshell parentheses should not close the surrounding command substitution early." \
		"$output" "100.00	2	2	0	src/fake.sh"
	assertNotContains "A command-substitution opener with a nested subshell should not become an uncovered command." \
		"$output" 'continued=$( (printf'
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_command_substitution_close_lines_with_redirections() {
	l_fake_root="$TEST_TMPDIR/fake-root-command-subst-redir"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-command-subst-redir.list"
	l_trace_file="$TEST_TMPDIR/merged-command-subst-redir.trace"
	l_summary_file="$TEST_TMPDIR/render-command-subst-redir-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-command-subst-redir-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
captured=$(
printf '%s\n' one
) 2>/dev/null
printf '%s\n' done
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:5: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "A command-substitution close line with redirections should end the ignored multiline body." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "The multiline command-substitution opening line should not be reported as missing shell code." \
		"$output" "captured=\$("
	assertNotContains "The command-substitution body should not appear as uncovered shell code." \
		"$output" "one"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_tracks_nested_scopes_inside_multiline_command_substitutions() {
	l_fake_root="$TEST_TMPDIR/fake-root-command-subst-nested"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-command-subst-nested.list"
	l_trace_file="$TEST_TMPDIR/merged-command-subst-nested.trace"
	l_summary_file="$TEST_TMPDIR/render-command-subst-nested-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-command-subst-nested-missing.txt"

	mkdir -p "$l_fake_root/src"
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
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:8: printf '%s\n' "\$captured"
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Nested subshell closes inside a multiline command substitution should not terminate the ignored body early." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "Lines that still belong to the multiline command substitution body should not appear as uncovered shell code after an inner subshell close." \
		"$output" "two"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_multiline_single_quoted_bodies() {
	l_fake_root="$TEST_TMPDIR/fake-root-single-quote"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-single-quote.list"
	l_trace_file="$TEST_TMPDIR/merged-single-quote.trace"
	l_summary_file="$TEST_TMPDIR/render-single-quote-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-single-quote-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
awk '
BEGIN {
	print "hello"
}
' "$1"
printf '%s\n' done
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:7: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "The bash-xtrace fallback should ignore multiline single-quoted command bodies such as embedded awk programs." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "The opening awk quote should not be treated as missing shell code." \
		"$output" "awk '"
	assertNotContains "Inner awk-program lines should not appear as uncovered shell code." \
		"$output" "print \"hello\""
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_ignores_multiline_single_quote_openers_with_inline_data() {
	l_fake_root="$TEST_TMPDIR/fake-root-inline-single-quote"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-inline-single-quote.list"
	l_trace_file="$TEST_TMPDIR/merged-inline-single-quote.trace"
	l_summary_file="$TEST_TMPDIR/render-inline-single-quote-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-inline-single-quote-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
MANIFEST='first-item
second-item
third-item'
printf '%s\n' "$MANIFEST"
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:5: printf '%s\n' "\$MANIFEST"
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Inline data after an opening single quote should still start a non-coverable multiline body." \
		"$output" "100.00	1	1	0	src/fake.sh"
	assertNotContains "Manifest data lines should not be reported as executable misses." \
		"$output" "second-item"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_single_quoted_double_quotes_do_not_hide_following_executable_lines() {
	l_fake_root="$TEST_TMPDIR/fake-root-single-quoted-double-quote"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-single-quoted-double-quote.list"
	l_trace_file="$TEST_TMPDIR/merged-single-quoted-double-quote.trace"
	l_summary_file="$TEST_TMPDIR/render-single-quoted-double-quote-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-single-quoted-double-quote-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
MANIFEST='first " item
second-item
third-item'
printf '%s\n' still-coverable
printf '%s\n' done
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:6: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Double quotes inside a single-quoted multiline value must not hide later executable lines from the denominator." \
		"$output" "50.00	2	1	1	src/fake.sh"
	assertContains "The executable line after the single-quoted value should remain visible as a miss." \
		"$output" "  5:printf '%s"
	assertNotContains "The genuine multiline value body should remain excluded." \
		"$output" "second-item"
}

# shellcheck disable=SC1003,SC2016,SC2317,SC2329  # Literal shell source; invoked indirectly by shunit2.
test_run_coverage_does_not_treat_escaped_or_double_quoted_apostrophes_as_multiline_openers() {
	l_fake_root="$TEST_TMPDIR/fake-root-literal-apostrophe"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-literal-apostrophe.list"
	l_trace_file="$TEST_TMPDIR/merged-literal-apostrophe.trace"
	l_summary_file="$TEST_TMPDIR/render-literal-apostrophe-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-literal-apostrophe-missing.txt"

	mkdir -p "$l_fake_root/src"
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
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:8: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "Escaped and double-quoted apostrophes must not hide the executable lines that follow them from the coverage denominator." \
		"$output" "20.00	5	1	4	src/fake.sh"
	assertContains "A parameter-pattern apostrophe should remain a coverable shell assignment." \
		"$output" '  2:escaped=${escaped#*\'"'"'}'
	assertContains "Executable lines following literal apostrophes should remain visible as misses." \
		"$output" "  5:printf '%s"
	assertNotContains "A genuine multiline single-quoted body should remain excluded." \
		"$output" "second-item"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_multiline_single_quoted_bodies_started_on_backslash_continuations() {
	l_fake_root="$TEST_TMPDIR/fake-root-single-quote-continuation"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-single-quote-continuation.list"
	l_trace_file="$TEST_TMPDIR/merged-single-quote-continuation.trace"
	l_summary_file="$TEST_TMPDIR/render-single-quote-continuation-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-single-quote-continuation-missing.txt"

	mkdir -p "$l_fake_root/src"
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
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:2: awk -v mode=1 ...
+$l_source_file:8: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "The bash-xtrace fallback should keep ignoring multiline single-quoted bodies when the opening quote starts on a backslash-continuation line." \
		"$output" "100.00	2	2	0	src/fake.sh"
	assertNotContains "The embedded awk body should not reappear as uncovered shell code when its opening quote follows a continuation line." \
		"$output" "print \"hello\""
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_ignores_backslash_continuation_lines() {
	l_fake_root="$TEST_TMPDIR/fake-root-continuation"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-continuation.list"
	l_trace_file="$TEST_TMPDIR/merged-continuation.trace"
	l_summary_file="$TEST_TMPDIR/render-continuation-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-continuation-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
rm -f "$1" \
	"$2" \
	"$3"
printf '%s\n' done
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:2: rm -f "$1" "$2" "$3"
+$l_source_file:5: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\" 2>/dev/null || :)\"")

	assertContains "The bash-xtrace fallback should count only the first line of a backslash-continued shell command." \
		"$output" "100.00	2	2	0	src/fake.sh"
	assertNotContains "Backslash continuation payload lines should not appear as uncovered shell code." \
		"$output" "\"\$2\" \\"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_render_bash_xtrace_report_overwrites_existing_outputs() {
	l_fake_root="$TEST_TMPDIR/fake-root-overwrite"
	l_source_file="$l_fake_root/src/fake.sh"
	l_target_list_file="$TEST_TMPDIR/targets-overwrite.list"
	l_trace_file="$TEST_TMPDIR/merged-overwrite.trace"
	l_summary_file="$TEST_TMPDIR/render-overwrite-summary.tsv"
	l_missing_file="$TEST_TMPDIR/render-overwrite-missing.txt"

	mkdir -p "$l_fake_root/src"
	cat >"$l_source_file" <<'SCRIPT'
#!/bin/sh
printf '%s\n' one
printf '%s\n' two
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	printf '+%s:2: printf '\''%%s\\n'\'' one\n' "$l_source_file" >"$l_trace_file"
	cat >"$l_summary_file" <<'EOF'
99.00	1	1	0	src/stale.sh
99.00	1	1	0	TOTAL
EOF
	cat >"$l_missing_file" <<'EOF'
src/stale.sh
  1:stale
EOF

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\")\"")

	assertContains "The renderer should replace stale summary content with the current target set." \
		"$output" "50.00	2	1	1	src/fake.sh"
	assertNotContains "The renderer should not append to stale summary rows from prior runs." \
		"$output" "src/stale.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
