#!/bin/sh
#
# shunit2 tests for the coverage runner script.
#

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_run_coverage.XXXXXX)
	RUN_COVERAGE_BIN="$ZXFER_ROOT/tests/run_coverage.sh"
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
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
printf '%s\n' done
SCRIPT
	printf '%s\n' "$l_source_file" >"$l_target_list_file"
	cat >"$l_trace_file" <<TRACE
+$l_source_file:3: printf '%s\n' one
+$l_source_file:13: printf '%s\n' block
+$l_source_file:17: printf '%s\n' done
TRACE

	output=$(run_coverage_helper \
		"ZXFER_ROOT=\"$l_fake_root\"; render_bash_xtrace_report \"$l_target_list_file\" \"$l_trace_file\" \"$l_summary_file\" \"$l_missing_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_summary_file\")\" \"\$(cat \"$l_missing_file\")\"")

	assertContains "The bash-xtrace fallback should ignore case labels, heredoc bodies, grouping parens, and multiline string bodies when counting coverable lines." \
		"$output" "75.00	4	3	1	src/fake.sh"
	assertContains "Only the truly uncovered executable line should remain in the missing-line report." \
		"$output" "  7:printf '%s"
	assertNotContains "Case labels should not be treated as missing executable lines." \
		"$output" "foo)"
	assertNotContains "Here-doc bodies should not be treated as missing executable lines." \
		"$output" "payload"
	assertNotContains "Multiline string bodies should not be treated as missing executable lines." \
		"$output" "line two"
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

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_policy_accepts_matching_summary() {
	l_summary_file="$TEST_TMPDIR/policy-pass-summary.tsv"
	l_policy_file="$TEST_TMPDIR/policy-pass.tsv"
	l_baseline_file="$TEST_TMPDIR/policy-pass-baseline.tsv"
	l_report_file="$TEST_TMPDIR/policy-pass-report.txt"
	l_failures_file="$TEST_TMPDIR/policy-pass-failures.tsv"

	cat >"$l_summary_file" <<'EOF'
80.00	10	8	2	src/a.sh
71.43	14	10	4	TOTAL
EOF
	cat >"$l_policy_file" <<'EOF'
TOTAL	70.00
src/a.sh	75.00
EOF
	cat >"$l_baseline_file" <<'EOF'
79.50	10	8	2	src/a.sh
70.00	14	10	4	TOTAL
EOF

	output=$(run_coverage_helper \
		"COVERAGE_POLICY_FILE=\"$l_policy_file\"; COVERAGE_BASELINE_SUMMARY_FILE=\"$l_baseline_file\"; enforce_bash_xtrace_policy \"$l_summary_file\" \"$l_report_file\" \"$l_failures_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_report_file\")\" \"\$(cat \"$l_failures_file\")\"")

	assertContains "A matching coverage summary should pass the policy gate." \
		"$output" "Coverage policy passed."
	assertContains "The successful policy check should still emit the failures TSV header." \
		"$output" "type	target	current_pct	required_pct	note"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_policy_reports_regressions_and_missing_policy_entries() {
	l_summary_file="$TEST_TMPDIR/policy-fail-summary.tsv"
	l_policy_file="$TEST_TMPDIR/policy-fail.tsv"
	l_baseline_file="$TEST_TMPDIR/policy-fail-baseline.tsv"
	l_report_file="$TEST_TMPDIR/policy-fail-report.txt"
	l_failures_file="$TEST_TMPDIR/policy-fail-failures.tsv"

	cat >"$l_summary_file" <<'EOF'
70.00	10	7	3	src/a.sh
69.23	13	9	4	TOTAL
EOF
	cat >"$l_policy_file" <<'EOF'
TOTAL	69.00
EOF
	cat >"$l_baseline_file" <<'EOF'
80.00	10	8	2	src/a.sh
69.23	13	9	4	TOTAL
EOF

	set +e
	output=$(run_coverage_helper \
		"COVERAGE_POLICY_FILE=\"$l_policy_file\"; COVERAGE_BASELINE_SUMMARY_FILE=\"$l_baseline_file\"; set +e; enforce_bash_xtrace_policy \"$l_summary_file\" \"$l_report_file\" \"$l_failures_file\"; status=\$?; set -e; printf '%s\n---\n%s\n' \"\$(cat \"$l_report_file\")\" \"\$(cat \"$l_failures_file\")\"; exit \"\$status\"")
	status=$?
	set -e

	assertEquals "A regressed or unpoliced target should fail the coverage policy gate." 1 "$status"
	assertContains "The report should explain that the target is missing from the policy file." \
		"$output" "missing-policy	src/a.sh"
	assertContains "The report should record the baseline regression for the target." \
		"$output" "regression	src/a.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
