#!/bin/sh
#
# Golden-output CLI contract suite for the zxfer launcher.
#
# Drives ./zxfer black-box through a mock secure PATH (fail-loud zfs/zstd
# stand-ins resolve first, so any unexpected helper execution breaks the
# pinned transcript) and compares exit status, stdout, and stderr byte for
# byte against the fixtures in tests/golden/cli_*.golden.
#
# Volatile fields are masked by zxfer_golden_normalize_stream() before the
# comparison: failure-report timestamp/hostname/version values, the launcher
# path token inside unsafe invocation fields, and the shell-owned getopts
# diagnostic line for unknown flags. The field names themselves stay pinned,
# so a real failure-report format change still fails the diff.
#
# This suite never regenerates fixtures. To refresh one intentionally, rerun
# the failing case, review the unified diff that the failure prints, and copy
# the reviewed "actual" transcript (left at $TEST_TMPDIR/case.actual while the
# test runs) over the golden file by hand.
#
# shellcheck disable=SC1090,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

ZXFER_TEST_GOLDEN_DIR="$TESTS_DIR/golden"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_cli_golden"

	g_golden_zxfer_bin="$ZXFER_ROOT/zxfer"
	g_golden_mock_bin_dir="$TEST_TMPDIR/mock_bin"
	g_golden_mock_tool_log="$TEST_TMPDIR/mock_tool_invocations.log"
	g_golden_scratch_tmpdir="$TEST_TMPDIR/scratch_tmp"
	g_golden_stdout_file="$TEST_TMPDIR/case.stdout"
	g_golden_stderr_file="$TEST_TMPDIR/case.stderr"
	g_golden_actual_file="$TEST_TMPDIR/case.actual"

	# Mirror the launcher's built-in default secure PATH with the mock dir
	# first so zfs/zstd resolve to the fail-loud stand-ins while awk/sed/etc
	# still resolve to the real host tools.
	g_golden_secure_path="$g_golden_mock_bin_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	zxfer_golden_write_mock_tool "$g_golden_mock_bin_dir/zfs"
	zxfer_golden_write_mock_tool "$g_golden_mock_bin_dir/zstd"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	rm -f "$g_golden_mock_tool_log" \
		"$g_golden_stdout_file" \
		"$g_golden_stderr_file" \
		"$g_golden_actual_file"
	rm -rf "$g_golden_scratch_tmpdir"
	mkdir -p "$g_golden_scratch_tmpdir"

	# Per-case environment overrides; tests opt in before invoking zxfer.
	g_golden_case_error_log=""
	g_golden_case_unsafe_commands=""
	g_golden_case_tmpdir=""
}

# Purpose: Write one fail-loud helper stand-in into the mock secure PATH dir.
# Usage: Called from oneTimeSetUp for zfs and zstd. Golden CLI cases must
# abort before executing either helper; if one runs anyway it records the
# invocation and pollutes stderr so both the log assert and the pinned
# transcript fail.
zxfer_golden_write_mock_tool() {
	l_tool_path=$1

	mkdir -p "$(dirname "$l_tool_path")"
	cat >"$l_tool_path" <<'EOF'
#!/bin/sh
if [ -n "${MOCK_TOOL_LOG:-}" ]; then
	printf '%s %s\n' "$0" "$*" >>"$MOCK_TOOL_LOG"
fi
echo "mock helper executed unexpectedly: $0 $*" >&2
exit 1
EOF
	chmod +x "$l_tool_path"
}

# Purpose: Run the real ./zxfer launcher with a fully pinned environment.
# Usage: Called by every golden case. Captures stdout/stderr to the shared
# case files and publishes the exit status in g_golden_exit_status. Per-case
# knobs (error log path, unsafe report mode, private TMPDIR) come from the
# g_golden_case_* globals reset in setUp.
zxfer_golden_invoke_zxfer() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	ZXFER_SECURE_PATH="$g_golden_secure_path" \
		ZXFER_SECURE_PATH_APPEND='' \
		ZXFER_ERROR_LOG="$g_golden_case_error_log" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS="$g_golden_case_unsafe_commands" \
		MOCK_TOOL_LOG="$g_golden_mock_tool_log" \
		TMPDIR="${g_golden_case_tmpdir:-$g_golden_scratch_tmpdir}" \
		"$g_golden_zxfer_bin" "$@" \
		>"$g_golden_stdout_file" 2>"$g_golden_stderr_file"
	g_golden_exit_status=$?
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
}

# Purpose: Mask the volatile fields of a CLI transcript stream.
# Usage: Applied to captured stdout/stderr and to mirrored error-log contents
# before any golden comparison. Only field *values* that legitimately vary per
# run/host are masked; renamed or missing fields still drift the diff. The
# Error:/message: branch guards keep zxfer's own "Invalid option provided."
# text pinned while the shell-owned getopts diagnostic line (wording varies by
# /bin/sh implementation and embeds the launcher path) collapses to one token.
zxfer_golden_normalize_stream() {
	sed \
		-e 's/^timestamp: ..*$/timestamp: [normalized]/' \
		-e 's/^hostname: ..*$/hostname: [normalized]/' \
		-e 's/^zxfer_version: ..*$/zxfer_version: [normalized]/' \
		-e "s/^invocation: '[^']*'/invocation: '[zxfer-path]'/" \
		-e '/^Error:/b' \
		-e '/^message:/b' \
		-e 's/^.*[Ii]llegal option.*$/[shell-option-diagnostic]/' \
		-e 's/^.*[Ii]nvalid option.*$/[shell-option-diagnostic]/' \
		-e 's/^.*[Uu]nknown option.*$/[shell-option-diagnostic]/'
}

# Purpose: Render the normalized exit-status/stdout/stderr transcript for the
# most recent zxfer_golden_invoke_zxfer run.
# Returns: Transcript text on stdout.
zxfer_golden_render_transcript() {
	printf 'exit_status: %s\n' "$g_golden_exit_status"
	printf '%s\n' '=== stdout ==='
	zxfer_golden_normalize_stream <"$g_golden_stdout_file"
	printf '%s\n' '=== stderr ==='
	zxfer_golden_normalize_stream <"$g_golden_stderr_file"
}

# Purpose: Compare one already-normalized actual file against its golden
# fixture byte for byte and fail with a unified diff on drift.
zxfer_golden_assert_file_matches_golden() {
	l_case_name=$1
	l_actual_file=$2
	l_golden_file="$ZXFER_TEST_GOLDEN_DIR/${l_case_name}.golden"

	if [ ! -f "$l_golden_file" ]; then
		fail "Missing golden fixture $l_golden_file for case $l_case_name."
		return 1
	fi

	if cmp -s "$l_golden_file" "$l_actual_file"; then
		return 0
	fi

	echo "Golden transcript drift for case $l_case_name (golden vs actual):" >&2
	diff -u "$l_golden_file" "$l_actual_file" >&2
	fail "CLI output for case $l_case_name no longer matches $l_golden_file."
	return 1
}

# Purpose: Fail the case when a mocked zfs/zstd helper was executed.
# Usage: Golden CLI paths must abort before any zfs interaction; this guards
# the host-safety contract for every pinned case.
zxfer_golden_assert_mock_tools_not_executed() {
	l_case_name=$1

	if [ -e "$g_golden_mock_tool_log" ]; then
		fail "Case $l_case_name executed mocked helpers unexpectedly: $(cat "$g_golden_mock_tool_log")"
		return 1
	fi
	return 0
}

# Purpose: Run one zxfer invocation and pin its full transcript to a fixture.
# Usage: zxfer_golden_assert_case_matches_golden <case_name> [zxfer args...]
zxfer_golden_assert_case_matches_golden() {
	l_run_case_name=$1
	shift

	zxfer_golden_invoke_zxfer "$@"
	zxfer_golden_render_transcript >"$g_golden_actual_file"

	zxfer_golden_assert_file_matches_golden "$l_run_case_name" "$g_golden_actual_file" || return 1
	zxfer_golden_assert_mock_tools_not_executed "$l_run_case_name"
}

test_help_flag_prints_usage_to_stdout_and_exits_zero() {
	zxfer_golden_assert_case_matches_golden cli_help -h

	assertEquals "zxfer -h must exit with status 0." 0 "$g_golden_exit_status"
}

test_usage_error_missing_destination_pins_redacted_report() {
	zxfer_golden_assert_case_matches_golden cli_usage_missing_destination -R tank/src

	assertEquals "Usage failures must exit with status 2." 2 "$g_golden_exit_status"
	assertTrue "The default failure report must redact the invocation field." \
		"grep -q '^invocation: \[redacted\]$' '$g_golden_stderr_file'"
}

test_usage_error_choosing_both_n_and_r() {
	zxfer_golden_assert_case_matches_golden cli_usage_n_with_r \
		-N tank/a -R tank/b backup/dest

	assertEquals "Combining -N and -R must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_invalid_job_count() {
	zxfer_golden_assert_case_matches_golden cli_usage_invalid_job_count \
		-j twelve -R tank/src backup/dest

	assertEquals "A non-numeric -j value must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_zero_job_count() {
	zxfer_golden_assert_case_matches_golden cli_usage_zero_job_count \
		-j 0 -R tank/src backup/dest

	assertEquals "-j 0 must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_backup_and_restore_conflict() {
	zxfer_golden_assert_case_matches_golden cli_usage_backup_restore_conflict \
		-e -k -R tank/src backup/dest

	assertEquals "Combining -e and -k must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_compression_without_remote_host() {
	zxfer_golden_assert_case_matches_golden cli_usage_compress_without_remote \
		-z -R tank/src backup/dest

	assertEquals "-z without -O/-T must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_unknown_flag() {
	zxfer_golden_assert_case_matches_golden cli_usage_unknown_flag \
		-q -R tank/src backup/dest

	assertEquals "An unknown flag must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_both_beep_modes() {
	zxfer_golden_assert_case_matches_golden cli_usage_both_beep_modes \
		-b -B -R tank/src backup/dest

	assertEquals "Combining -b and -B must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_invalid_grandfather_days() {
	zxfer_golden_assert_case_matches_golden cli_usage_invalid_grandfather_days \
		-g zero -R tank/src backup/dest

	assertEquals "A non-numeric -g value must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_missing_source() {
	zxfer_golden_assert_case_matches_golden cli_usage_missing_source backup/dest

	assertEquals "A run without -N or -R must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_absolute_source_path() {
	zxfer_golden_assert_case_matches_golden cli_usage_absolute_source_path \
		-R /tank/src backup/dest

	assertEquals "A source beginning with / must exit with status 2." 2 "$g_golden_exit_status"
}

test_failure_report_unsafe_mode_populates_invocation_field() {
	g_golden_case_unsafe_commands=1
	zxfer_golden_assert_case_matches_golden cli_report_missing_destination_unsafe \
		-R tank/src

	assertEquals "Usage failures in unsafe report mode must still exit with status 2." \
		2 "$g_golden_exit_status"
	assertTrue "Unsafe report mode must populate the invocation field with the quoted arguments." \
		"grep -q \"^invocation: '.*' '-R' 'tank/src'$\" '$g_golden_stderr_file'"
	assertFalse "Unsafe report mode must not print the redaction marker." \
		"grep -q '^invocation: \[redacted\]$' '$g_golden_stderr_file'"
}

test_error_log_mirrors_failure_report_and_cleans_lock_dir() {
	l_log_dir="$TEST_TMPDIR/error_log_dir"
	rm -rf "$l_log_dir"
	mkdir -p "$l_log_dir"
	g_golden_case_error_log="$l_log_dir/log"

	zxfer_golden_invoke_zxfer -R tank/src

	assertEquals "A usage failure with ZXFER_ERROR_LOG set must still exit with status 2." \
		2 "$g_golden_exit_status"
	assertTrue "A failing run must create the ZXFER_ERROR_LOG file." \
		"[ -f '$l_log_dir/log' ]"

	# The mirrored log must hold exactly the stderr failure-report block.
	sed -n '/^zxfer: failure report begin$/,/^zxfer: failure report end$/p' \
		"$g_golden_stderr_file" |
		zxfer_golden_normalize_stream >"$TEST_TMPDIR/report_from_stderr.normalized"
	zxfer_golden_normalize_stream <"$l_log_dir/log" \
		>"$TEST_TMPDIR/report_from_log.normalized"

	if ! cmp -s "$TEST_TMPDIR/report_from_stderr.normalized" \
		"$TEST_TMPDIR/report_from_log.normalized"; then
		echo "ZXFER_ERROR_LOG contents diverged from the stderr failure report:" >&2
		diff -u "$TEST_TMPDIR/report_from_stderr.normalized" \
			"$TEST_TMPDIR/report_from_log.normalized" >&2
		fail "ZXFER_ERROR_LOG must mirror the stderr failure report exactly."
	fi

	zxfer_golden_assert_file_matches_golden cli_error_log_report \
		"$TEST_TMPDIR/report_from_log.normalized"

	# The per-log lock dir (.zxfer-error-log.lock.<name>) and any staging
	# residue must be gone; only the log file itself may remain.
	assertEquals "The error-log lock dir must be cleaned up after mirroring the report." \
		"log" "$(ls -A "$l_log_dir")"

	case "$(ls -ld "$l_log_dir/log")" in
	-rw-------*) ;;
	*)
		fail "ZXFER_ERROR_LOG file must keep 0600 permissions; got: $(ls -ld "$l_log_dir/log")"
		;;
	esac

	zxfer_golden_assert_mock_tools_not_executed cli_error_log_report
}

test_failing_usage_invocation_leaves_tmpdir_empty() {
	l_leak_tmpdir="$TEST_TMPDIR/leak_tmp"
	rm -rf "$l_leak_tmpdir"
	mkdir -p "$l_leak_tmpdir"
	g_golden_case_tmpdir="$l_leak_tmpdir"

	zxfer_golden_invoke_zxfer -j 0 -R tank/src backup/dest

	assertEquals "The failing usage invocation must exit with status 2." \
		2 "$g_golden_exit_status"
	assertEquals "A failing usage invocation must not leak temp files into TMPDIR." \
		"" "$(ls -A "$l_leak_tmpdir")"
	zxfer_golden_assert_mock_tools_not_executed cli_tmpdir_leak
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
