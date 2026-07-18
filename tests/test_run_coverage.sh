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

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_full_runs_are_report_only_without_explicit_enforcement() {
	output=$(run_coverage_helper \
		'COVERAGE_POLICY_MODE=auto; configure_coverage_policy_enforcement 0; printf "%s %s\n" "$ZXFER_COVERAGE_ENFORCE_POLICY" "$COVERAGE_RUN_SCOPE"')

	assertEquals "A full coverage run should stay report-only unless enforcement is explicit." \
		"0 full" "$output"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_targeted_runs_are_report_only_by_default() {
	output=$(run_coverage_helper \
		'COVERAGE_POLICY_MODE=auto; configure_coverage_policy_enforcement 1; printf "%s %s\n" "$ZXFER_COVERAGE_ENFORCE_POLICY" "$COVERAGE_RUN_SCOPE"')

	assertEquals "A targeted suite trace should not be compared with the full-tree policy unless enforcement is requested." \
		"0 targeted" "$output"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_rejects_conflicting_explicit_policy_options() {
	set +e
	output=$(run_coverage_helper '
		select_explicit_coverage_policy_mode 1
		select_explicit_coverage_policy_mode 0
	' 2>&1)
	status=$?
	set -e

	assertEquals "Conflicting explicit policy options must fail instead of silently letting the last option win." \
		1 "$status"
	assertContains "The conflict error should name both incompatible options." \
		"$output" "--enforce and --report-only cannot be used together"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_enforcement_requires_a_full_run() {
	output=$(run_coverage_helper '
		COVERAGE_POLICY_MODE=1
		configure_coverage_policy_enforcement 0
		printf "full=%s\n" "$ZXFER_COVERAGE_ENFORCE_POLICY"
		COVERAGE_POLICY_MODE=0
		configure_coverage_policy_enforcement 0
		printf "report=%s\n" "$ZXFER_COVERAGE_ENFORCE_POLICY"
	')

	assertContains "Explicit enforcement should apply to a full coverage run." \
		"$output" "full=1"
	assertContains "Explicit report-only mode should also work for full coverage runs." \
		"$output" "report=0"

	set +e
	output=$(run_coverage_helper '
		COVERAGE_POLICY_MODE=1
		configure_coverage_policy_enforcement 1
	' 2>&1)
	status=$?
	set -e

	assertEquals "A targeted trace should reject policy enforcement instead of comparing partial coverage to full-tree thresholds." \
		1 "$status"
	assertContains "The targeted-enforcement error should explain the full-run requirement." \
		"$output" "targeted suites are always report-only"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_enforcement_selects_bash_xtrace_and_rejects_kcov() {
	output=$(run_coverage_helper '
		ZXFER_COVERAGE_ENFORCE_POLICY=1
		ZXFER_COVERAGE_MODE=auto
		resolve_coverage_collector_mode
	')

	assertEquals "An enforced auto-mode run should select the collector that implements repository thresholds." \
		"bash-xtrace" "$output"

	set +e
	output=$(run_coverage_helper '
		ZXFER_COVERAGE_ENFORCE_POLICY=1
		ZXFER_COVERAGE_MODE=kcov
		resolve_coverage_collector_mode
	' 2>&1)
	status=$?
	set -e

	assertEquals "Explicit kcov mode should fail rather than silently skipping requested policy enforcement." \
		1 "$status"
	assertContains "The collector error should name the required enforcement mode." \
		"$output" "requires ZXFER_COVERAGE_MODE=bash-xtrace"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Invoked indirectly by shunit2; command expands inside the helper shell.
test_run_coverage_default_suite_resolution_includes_coverage_overlays() {
	output=$(run_coverage_helper 'ZXFER_ROOT=$(cd "$(dirname "$RUN_COVERAGE_BIN")/.." && pwd); TEST_DIR="$ZXFER_ROOT/tests"; resolve_suites | while IFS= read -r suite; do case "$suite" in "$ZXFER_ROOT"/*) printf "%s\n" "${suite#$ZXFER_ROOT/}" ;; *) printf "%s\n" "$suite" ;; esac; done')

	assertContains "The default coverage run should include the background job coverage suite that protects the committed baseline." \
		"$output" "tests/test_zxfer_background_jobs.sh"
	assertContains "The default coverage run should include the remote host overlay suite that protects the committed baseline." \
		"$output" "tests/test_zxfer_remote_hosts_coverage.sh"
	assertContains "The default coverage run should include the property reconcile suite that exercises the in-memory property tables." \
		"$output" "tests/test_zxfer_property_reconcile.sh"
	assertContains "The default coverage run should include the snapshot state suite that protects transform readback coverage." \
		"$output" "tests/test_zxfer_snapshot_state.sh"
	assertNotContains "The default coverage run should not execute shared test scaffolding as a suite." \
		"$output" "tests/test_helper.sh"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Expands in helper shell; invoked indirectly by shunit2.
test_run_coverage_repository_policy_and_baseline_cover_every_production_target() {
	set +e
	output=$(run_coverage_helper '
		ZXFER_ROOT=$(cd "$(dirname "$RUN_COVERAGE_BIN")/.." && pwd)
		ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=0
		COVERAGE_POLICY_FILE="$ZXFER_ROOT/tests/coverage_policy.tsv"
		COVERAGE_BASELINE_SUMMARY_FILE="$ZXFER_ROOT/tests/coverage_baseline/bash-xtrace/summary.tsv"
		check_coverage_policy_target_inventory
	' 2>&1)
	status=$?
	set -e

	assertEquals "Every production coverage target and TOTAL must have exactly one policy and baseline row. Output: $output" \
		0 "$status" || :
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_repository_policy_and_baseline_have_measured_production_rows() {
	l_invalid_policy=$(awk -F '\t' '
		$1 ~ /^src\// && ($2 + 0) <= 0 { print $1 }
	' "$ZXFER_ROOT/tests/coverage_policy.tsv")
	l_invalid_baseline=$(awk -F '\t' '
		$5 ~ /^src\// && ($2 + 0) <= 0 { print $5 }
	' "$ZXFER_ROOT/tests/coverage_baseline/bash-xtrace/summary.tsv")

	assertEquals "Every production coverage target must have a nonzero enforced minimum." \
		"" "$l_invalid_policy"
	assertEquals "Every production coverage target must have measured full-run baseline evidence." \
		"" "$l_invalid_baseline"
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

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_targeted_missing_report_skips_the_full_baseline_diff() {
	l_missing_file="$TEST_TMPDIR/targeted-missing.txt"
	l_baseline_file="$TEST_TMPDIR/targeted-baseline-missing.txt"
	l_diff_file="$TEST_TMPDIR/targeted-missing.diff"
	printf '%s\n' "src/current.sh" >"$l_missing_file"
	printf '%s\n' "src/baseline.sh" >"$l_baseline_file"

	output=$(run_coverage_helper \
		"COVERAGE_BASELINE_MISSING_FILE=\"$l_baseline_file\"; write_missing_diff_file \"$l_missing_file\" \"$l_diff_file\" targeted; cat \"$l_diff_file\"")

	assertEquals "A targeted trace should not render a misleading diff against the full-run missing-line baseline." \
		"Full-baseline missing-line diff skipped for targeted coverage run." "$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_full_missing_report_still_compares_the_committed_baseline() {
	l_missing_file="$TEST_TMPDIR/full-missing.txt"
	l_baseline_file="$TEST_TMPDIR/full-baseline-missing.txt"
	l_diff_file="$TEST_TMPDIR/full-missing.diff"
	printf '%s\n' "src/current.sh" >"$l_missing_file"
	printf '%s\n' "src/baseline.sh" >"$l_baseline_file"

	output=$(run_coverage_helper \
		"COVERAGE_BASELINE_MISSING_FILE=\"$l_baseline_file\"; write_missing_diff_file \"$l_missing_file\" \"$l_diff_file\" full; cat \"$l_diff_file\"")

	assertContains "A full trace should retain the committed missing-line baseline diff." \
		"$output" "src/baseline.sh"
	assertContains "The full-run diff should include the current missing-line report." \
		"$output" "src/current.sh"
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
test_run_coverage_policy_accepts_small_hit_regressions_within_tolerance() {
	l_summary_file="$TEST_TMPDIR/policy-tolerance-summary.tsv"
	l_policy_file="$TEST_TMPDIR/policy-tolerance.tsv"
	l_baseline_file="$TEST_TMPDIR/policy-tolerance-baseline.tsv"
	l_report_file="$TEST_TMPDIR/policy-tolerance-report.txt"
	l_failures_file="$TEST_TMPDIR/policy-tolerance-failures.tsv"

	cat >"$l_summary_file" <<'EOF'
70.00	10	7	3	src/a.sh
69.23	13	9	4	TOTAL
EOF
	cat >"$l_policy_file" <<'EOF'
TOTAL	69.00
src/a.sh	69.00
EOF
	cat >"$l_baseline_file" <<'EOF'
80.00	10	8	2	src/a.sh
76.92	13	10	3	TOTAL
EOF

	output=$(run_coverage_helper \
		"COVERAGE_POLICY_FILE=\"$l_policy_file\"; COVERAGE_BASELINE_SUMMARY_FILE=\"$l_baseline_file\"; ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE=1; ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE=1; enforce_bash_xtrace_policy \"$l_summary_file\" \"$l_report_file\" \"$l_failures_file\"; printf '%s\n---\n%s\n' \"\$(cat \"$l_report_file\")\" \"\$(cat \"$l_failures_file\")\"")

	assertContains "Small baseline hit regressions within the configured tolerance should still pass the no-regression gate." \
		"$output" "Coverage policy passed."
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
		"COVERAGE_POLICY_FILE=\"$l_policy_file\"; COVERAGE_BASELINE_SUMMARY_FILE=\"$l_baseline_file\"; ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE=0; ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE=0; set +e; enforce_bash_xtrace_policy \"$l_summary_file\" \"$l_report_file\" \"$l_failures_file\"; status=\$?; set -e; printf '%s\n---\n%s\n' \"\$(cat \"$l_report_file\")\" \"\$(cat \"$l_failures_file\")\"; exit \"\$status\"")
	status=$?
	set -e

	assertEquals "A regressed or unpoliced target should fail the coverage policy gate." 1 "$status"
	assertContains "The report should explain that the target is missing from the policy file." \
		"$output" "missing-policy	src/a.sh"
	assertContains "The report should record the baseline regression for the target." \
		"$output" "regression	src/a.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_coverage_policy_reports_regressions_beyond_hit_tolerance() {
	l_summary_file="$TEST_TMPDIR/policy-hit-fail-summary.tsv"
	l_policy_file="$TEST_TMPDIR/policy-hit-fail.tsv"
	l_baseline_file="$TEST_TMPDIR/policy-hit-fail-baseline.tsv"
	l_report_file="$TEST_TMPDIR/policy-hit-fail-report.txt"
	l_failures_file="$TEST_TMPDIR/policy-hit-fail-failures.tsv"

	cat >"$l_summary_file" <<'EOF'
70.00	10	7	3	src/a.sh
69.23	13	9	4	TOTAL
EOF
	cat >"$l_policy_file" <<'EOF'
TOTAL	69.00
src/a.sh	69.00
EOF
	cat >"$l_baseline_file" <<'EOF'
90.00	10	9	1	src/a.sh
84.62	13	11	2	TOTAL
EOF

	set +e
	output=$(run_coverage_helper \
		"COVERAGE_POLICY_FILE=\"$l_policy_file\"; COVERAGE_BASELINE_SUMMARY_FILE=\"$l_baseline_file\"; ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE=1; ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE=1; set +e; enforce_bash_xtrace_policy \"$l_summary_file\" \"$l_report_file\" \"$l_failures_file\"; status=\$?; set -e; printf '%s\n---\n%s\n' \"\$(cat \"$l_report_file\")\" \"\$(cat \"$l_failures_file\")\"; exit \"\$status\"")
	status=$?
	set -e

	assertEquals "Regressions that exceed the configured baseline hit tolerance should still fail the coverage gate." \
		1 "$status"
	assertContains "The failures TSV should still record the regression once it exceeds tolerance." \
		"$output" "regression	src/a.sh"
	assertContains "The TOTAL row should also fail when the overall hit regression exceeds tolerance." \
		"$output" "regression	TOTAL"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
