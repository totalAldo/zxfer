#!/bin/sh
#
# shunit2 tests for zxfer_reporting.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_reporting.sh"

zxfer_usage() {
	printf '%s\n' "usage output"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_reporting"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_R_recursive="tank/src"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_option_Y_yield_iterations=3
	g_zxfer_version="test-version"
	g_zxfer_original_invocation="'./zxfer' 'backup/dst'"
	zxfer_reset_failure_context "unit"
}

test_zxfer_render_failure_report_includes_context_fields() {
	zxfer_set_failure_roots "tank/src" "backup/dst"
	zxfer_set_current_dataset_context "tank/src/child" "backup/dst/child"
	zxfer_record_last_command_string "zfs send tank/src@snap1"
	g_zxfer_failure_message="boom"

	report=$(zxfer_render_failure_report 1)

	assertContains "Failure report should include the selected stage." \
		"$report" "failure_stage: unit"
	assertContains "Failure report should include the current source dataset." \
		"$report" "current_source: tank/src/child"
	assertContains "Failure report should include the invocation string." \
		"$report" "invocation: './zxfer' 'backup/dst'"
	assertContains "Failure report should include the last command." \
		"$report" "last_command: zfs send tank/src@snap1"
}

test_zxfer_append_failure_report_to_log_rejects_relative_path() {
	zxfer_test_capture_subshell '
		ZXFER_ERROR_LOG="relative.log" \
			zxfer_append_failure_report_to_log "report"
	'

	assertEquals "Relative error-log paths should be rejected." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative path rejection should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "because it is not absolute"
}

test_zxfer_append_failure_report_to_log_rejects_symlink_target() {
	log_target="$TEST_TMPDIR/failure-target.log"
	log_symlink="$TEST_TMPDIR/failure-link.log"

	: >"$log_target"
	ln -s "$log_target" "$log_symlink"

	zxfer_test_capture_subshell "
		ZXFER_ERROR_LOG=\"$log_symlink\" \\
			zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Symlinked error-log targets should be rejected." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Symlinked error-log targets should explain the refusal." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "is a symlink"
}

test_zxfer_append_failure_report_to_log_rejects_direct_symlink_target_when_component_scan_does_not_fire() {
	log_target="$TEST_TMPDIR/failure-direct-target.log"
	log_symlink="$TEST_TMPDIR/failure-direct-link.log"

	: >"$log_target"
	ln -s "$log_target" "$log_symlink"

	zxfer_test_capture_subshell "
		zxfer_find_symlink_path_component() {
			return 1
		}
		ZXFER_ERROR_LOG=\"$log_symlink\" \\
			zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Direct symlinked error-log targets should still be rejected when path-component scanning does not catch them first." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Direct symlinked error-log target rejection should explain the refusal." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "because it is a symlink"
}

test_zxfer_append_failure_report_to_log_warns_when_append_fails() {
	log_path="$TEST_TMPDIR/append-failure.log"
	stdout_file="$TEST_TMPDIR/append_failure.stdout"
	stderr_file="$TEST_TMPDIR/append_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		printf() {
			if [ \"\$2\" = \"append-failure-report\" ]; then
				return 1
			fi
			command printf \"\$@\"
		}
		zxfer_append_failure_report_to_log \"append-failure-report\"
	"

	assertEquals "Append failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Append failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
	assertEquals "Append failures should not write partial report data." \
		"" "$(cat "$log_path")"
}

test_zxfer_profile_now_ms_returns_failure_when_date_is_unavailable() {
	zxfer_test_capture_subshell '
		date() {
			return 1
		}
		zxfer_profile_now_ms
	'

	assertEquals "Profile timestamps should fail cleanly when date cannot provide either format." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed profile timestamp lookups should not emit a value." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_profile_add_elapsed_ms_ignores_failed_clock_lookups_in_current_shell() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=0
			g_test_profile_elapsed_ms=7
			zxfer_profile_now_ms() {
				return 1
			}
			zxfer_profile_add_elapsed_ms g_test_profile_elapsed_ms 10
			printf 'counter=%s\n' "$g_test_profile_elapsed_ms"
			printf 'has_data=%s\n' "${g_zxfer_profile_has_data:-0}"
		)
	)

	assertEquals "Failed clock lookups should leave existing counters unchanged." \
		"counter=7
has_data=0" "$output"
}

test_zxfer_profile_add_elapsed_ms_normalizes_invalid_existing_counter_values() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_test_profile_elapsed_ms="bogus"

	zxfer_profile_add_elapsed_ms g_test_profile_elapsed_ms 10 15

	assertEquals "Elapsed-timing helpers should normalize invalid stored counter values before adding elapsed milliseconds." \
		5 "$g_test_profile_elapsed_ms"
	assertEquals "Successful elapsed-timing updates should mark that profiling data exists." \
		1 "$g_zxfer_profile_has_data"
}

test_zxfer_profile_add_elapsed_ms_ignores_empty_counter_names_and_invalid_end_values() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=0
			g_test_profile_elapsed_ms=9
			zxfer_profile_add_elapsed_ms "" 10 15
			zxfer_profile_add_elapsed_ms g_test_profile_elapsed_ms 10 "bad-end"
			printf 'counter=%s\n' "$g_test_profile_elapsed_ms"
			printf 'has_data=%s\n' "${g_zxfer_profile_has_data:-0}"
		)
	)

	assertEquals "Elapsed-timing helpers should ignore empty counter names and invalid end timestamps without mutating state." \
		"counter=9
has_data=0" "$output"
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
