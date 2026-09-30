#!/bin/sh
#
# Golden-output CLI contract suite for the zxfer launcher.
#
# Drives ./zxfer black-box through a mock secure PATH (fail-loud zfs, zstd
# and ssh stand-ins resolve first, so any unexpected helper execution or
# connection breaks the pinned transcript) and compares exit status, stdout,
# and stderr byte for byte against the fixtures in tests/golden/cli_*.golden.
#
# Volatile fields are masked by zxfer_golden_normalize_stream() before the
# comparison: failure-report timestamp/hostname/version values, the launcher
# path token inside unsafe invocation fields, and the shell-owned getopts
# diagnostic line for unknown flags. The field names themselves stay pinned,
# so a real failure-report format change still fails the diff.
#
# A normal run never rewrites fixtures. To refresh them after an intentional
# change, run the suite with ZXFER_UPDATE_GOLDEN=1, then review the fixture
# diff with git before committing it.
#
# shellcheck disable=SC1090,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

ZXFER_TEST_GOLDEN_DIR="$TESTS_DIR/golden"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_cli_golden"

	g_golden_zxfer_bin=$ZXFER_TEST_ZXFER_BIN
	g_golden_mock_bin_dir="$TEST_TMPDIR/mock_bin"
	g_golden_mock_tool_log="$TEST_TMPDIR/mock_tool_invocations.log"
	g_golden_scratch_tmpdir="$TEST_TMPDIR/scratch_tmp"
	g_golden_stdout_file="$TEST_TMPDIR/case.stdout"
	g_golden_stderr_file="$TEST_TMPDIR/case.stderr"
	g_golden_actual_file="$TEST_TMPDIR/case.actual"

	# Mirror the launcher's built-in default secure PATH with the mock dir
	# first so zfs/zstd/ssh resolve to the fail-loud stand-ins while
	# awk/sed/etc still resolve to the real host tools.
	g_golden_secure_path="$g_golden_mock_bin_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	zxfer_golden_write_mock_tool "$g_golden_mock_bin_dir/zfs"
	zxfer_golden_write_mock_tool "$g_golden_mock_bin_dir/zstd"
	zxfer_golden_write_mock_tool "$g_golden_mock_bin_dir/ssh"
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
	g_golden_case_secure_path=""
}

# Purpose: Write one fail-loud helper stand-in into the mock secure PATH dir.
# Usage: Called from oneTimeSetUp for zfs, zstd and ssh. Golden CLI cases
# must abort before executing any of them; if one runs anyway it records the
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
# knobs (error log path, unsafe report mode, private TMPDIR, secure PATH) come
# from the g_golden_case_* globals reset in setUp.
zxfer_golden_invoke_zxfer() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	ZXFER_SECURE_PATH="${g_golden_case_secure_path:-$g_golden_secure_path}" \
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
# Usage: zxfer_golden_assert_file_matches_golden CASE ACTUAL_FILE; with
# ZXFER_UPDATE_GOLDEN=1 it rewrites the fixture from ACTUAL_FILE instead.
zxfer_golden_assert_file_matches_golden() {
	l_case_name=$1
	l_actual_file=$2
	l_golden_file="$ZXFER_TEST_GOLDEN_DIR/${l_case_name}.golden"

	if [ "${ZXFER_UPDATE_GOLDEN:-0}" = 1 ]; then
		if ! cp "$l_actual_file" "$l_golden_file"; then
			fail "Could not update golden fixture $l_golden_file for case $l_case_name."
			return 1
		fi
		echo "Updated golden fixture $l_golden_file." >&2
		return 0
	fi

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

test_usage_error_repeated_override_property() {
	zxfer_golden_assert_case_matches_golden cli_usage_repeated_override_property \
		-o compression=lz4,compression=gzip -R tank/src backup/dest

	assertEquals "A property named twice in -o must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_invalid_override_syntax() {
	zxfer_golden_assert_case_matches_golden cli_usage_invalid_override_syntax \
		-o compression -R tank/src backup/dest

	assertEquals "An -o item without a property name must exit with status 2." 2 "$g_golden_exit_status"
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

test_error_log_mirrors_failure_report_and_leaves_only_the_log() {
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

	# Appends take no lock and stage nothing: only the log file may appear.
	assertEquals "Mirroring the report must leave only the log in its directory." \
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

test_golden_update_mode_rewrites_the_fixture_from_the_actual_transcript() {
	l_update_dir="$TEST_TMPDIR/update_golden"
	mkdir -p "$l_update_dir"
	printf '%s\n' stale >"$l_update_dir/update_case.golden"
	printf '%s\n' fresh >"$TEST_TMPDIR/update_case.actual"

	(
		ZXFER_TEST_GOLDEN_DIR=$l_update_dir
		ZXFER_UPDATE_GOLDEN=1
		zxfer_golden_assert_file_matches_golden update_case \
			"$TEST_TMPDIR/update_case.actual" 2>/dev/null
	)
	l_update_status=$?

	assertEquals "ZXFER_UPDATE_GOLDEN=1 should rewrite the golden fixture from the actual transcript." \
		"status=0 golden=fresh" "status=$l_update_status golden=$(cat "$l_update_dir/update_case.golden")"
}

# Purpose: Like zxfer_golden_assert_case_matches_golden, for a transcript that
# names a per-run directory: each occurrence of PATH becomes TOKEN first.
# Usage: zxfer_golden_assert_masked_case_matches_golden <case_name> <path>
# <token> [zxfer args...]
zxfer_golden_assert_masked_case_matches_golden() {
	l_masked_case_name=$1
	l_masked_path=$2
	l_masked_token=$3
	shift 3

	zxfer_golden_invoke_zxfer "$@"
	# index() matches the path literally, whatever characters it holds.
	zxfer_golden_render_transcript |
		awk -v path="$l_masked_path" -v token="$l_masked_token" '{
			line = ""
			while ((i = index($0, path)) > 0) {
				line = line substr($0, 1, i - 1) token
				$0 = substr($0, i + length(path))
			}
			print line $0
		}' >"$g_golden_actual_file"

	zxfer_golden_assert_file_matches_golden "$l_masked_case_name" "$g_golden_actual_file" || return 1
	zxfer_golden_assert_mock_tools_not_executed "$l_masked_case_name"
}

test_invalid_secure_path_is_a_dependency_failure_before_parsing() {
	g_golden_case_secure_path=$(printf '/bin\t/untrusted')
	zxfer_golden_assert_case_matches_golden cli_dependency_invalid_secure_path \
		-R tank/src backup/dest

	assertEquals "A secure PATH with a tab must exit with status 1." 1 "$g_golden_exit_status"
}

test_missing_zfs_on_the_secure_path_is_a_dependency_failure() {
	l_awk_only_dir="$TEST_TMPDIR/awk_only_bin"
	mkdir -p "$l_awk_only_dir"
	[ -e "$l_awk_only_dir/awk" ] || ln -s "$(command -v awk)" "$l_awk_only_dir/awk" ||
		fail "Unable to stage awk alone on a secure PATH."
	g_golden_case_secure_path=$l_awk_only_dir
	zxfer_golden_assert_masked_case_matches_golden cli_dependency_missing_zfs \
		"$l_awk_only_dir" "[secure-path]" -R tank/src backup/dest

	assertEquals "A secure PATH without zfs must exit with status 1." 1 "$g_golden_exit_status"
}

test_blank_compression_command_is_a_usage_error() {
	zxfer_golden_assert_case_matches_golden cli_usage_blank_compression_command \
		-Z '   ' -R tank/src backup/dest

	assertEquals "A blank -Z command must exit with status 2." 2 "$g_golden_exit_status"
}

test_unresolvable_compression_command_is_a_dependency_failure() {
	zxfer_golden_assert_masked_case_matches_golden cli_dependency_missing_compression_command \
		"$g_golden_mock_bin_dir" "[mock-bin]" \
		-Z 'zxfer-golden-missing-codec -3' -R tank/src backup/dest

	assertEquals "A -Z command whose head is not on the secure PATH must exit with status 1." \
		1 "$g_golden_exit_status"
}

test_usage_error_zero_grandfather_days() {
	zxfer_golden_assert_case_matches_golden cli_usage_zero_grandfather_days \
		-g 0 -R tank/src backup/dest

	assertEquals "-g 0 must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_migration_with_a_remote_host() {
	zxfer_golden_assert_case_matches_golden cli_usage_migrate_with_remote_host \
		-m -O origin.example -R tank/src backup/dest

	assertEquals "Combining -m and -O must exit with status 2." 2 "$g_golden_exit_status"
}

test_usage_error_shell_quoted_compression_command() {
	zxfer_golden_assert_case_matches_golden cli_usage_quoted_compression_command \
		-Z '"/opt/zstd dir/zstd" -3' -O origin.example -R tank/src backup/dest

	assertEquals "A shell-quoted -Z command must exit with status 2." 2 "$g_golden_exit_status"
}

# The prescan finds -h only before the first operand; after an option
# argument the full parser prints the same usage.
test_help_after_an_option_argument_prints_usage_and_exits_zero() {
	zxfer_golden_assert_case_matches_golden cli_help -R tank/src -h backup/dest

	assertEquals "zxfer -R SRC -h must exit with status 0." 0 "$g_golden_exit_status"
}

# -h prints the usage before any module loads: it needs no helper on the
# secure PATH and runs no awk or sed planted on the caller's PATH.
test_help_needs_no_helper_and_runs_no_tool_planted_on_path() {
	l_planted_dir="$TEST_TMPDIR/planted_path"
	l_planted_log="$TEST_TMPDIR/planted_path.log"
	l_empty_dir="$TEST_TMPDIR/empty_secure_path"
	rm -rf "$l_planted_dir" "$l_planted_log" "$l_empty_dir"
	mkdir -p "$l_planted_dir" "$l_empty_dir"
	for l_tool in awk sed; do
		printf '#!/bin/sh\nprintf "%%s\\n" %s >>"%s"\nexec "%s" "$@"\n' \
			"$l_tool" "$l_planted_log" "$(command -v "$l_tool")" \
			>"$l_planted_dir/$l_tool"
		chmod +x "$l_planted_dir/$l_tool"
	done

	g_golden_exit_status=0
	PATH="$l_planted_dir:$PATH" ZXFER_SECURE_PATH=$l_empty_dir \
		ZXFER_SECURE_PATH_APPEND='' MOCK_TOOL_LOG="$g_golden_mock_tool_log" \
		TMPDIR="$g_golden_scratch_tmpdir" "$g_golden_zxfer_bin" -h \
		>"$g_golden_stdout_file" 2>"$g_golden_stderr_file" ||
		g_golden_exit_status=$?
	zxfer_golden_render_transcript >"$g_golden_actual_file"

	zxfer_golden_assert_file_matches_golden cli_help "$g_golden_actual_file"
	assertFalse "No tool planted on the caller's PATH may run: $(cat "$l_planted_log" 2>/dev/null)" \
		"[ -e '$l_planted_log' ]"
}

# -z resolves the local codec while the options are parsed, so with a remote
# origin a missing codec stops the run before any connection: the ssh
# stand-in would log one.
test_missing_compression_command_with_a_remote_origin_fails_before_any_connection() {
	zxfer_golden_invoke_zxfer -Z zxfer-missing-codec -O origin.example \
		-R tank/src backup/dest

	assertEquals "A missing -Z command must exit with status 1." 1 "$g_golden_exit_status"
	assertTrue "The run must stop while its options are parsed." \
		"grep -Fqx 'failure_stage: cli parse' '$g_golden_stderr_file'"
	zxfer_golden_assert_mock_tools_not_executed cli_missing_compression_command
}

# A relative ZXFER_ERROR_LOG is refused with a warning after the report; the
# failure keeps its exit status and nothing lands in the working directory.
test_relative_error_log_is_refused_without_changing_the_exit_status() {
	l_cwd="$TEST_TMPDIR/relative_log_cwd"
	rm -rf "$l_cwd"
	mkdir -p "$l_cwd"
	case $g_golden_zxfer_bin in
	/*) l_bin=$g_golden_zxfer_bin ;;
	*) l_bin=$PWD/$g_golden_zxfer_bin ;;
	esac
	g_golden_case_error_log=relative.log

	g_golden_exit_status=0
	(
		cd "$l_cwd" || exit 97
		g_golden_zxfer_bin=$l_bin
		zxfer_golden_invoke_zxfer -R tank/src
		exit "$g_golden_exit_status"
	) || g_golden_exit_status=$?
	zxfer_golden_render_transcript >"$g_golden_actual_file"

	zxfer_golden_assert_file_matches_golden cli_error_log_relative_path \
		"$g_golden_actual_file"
	assertEquals "Nothing may be written to the working directory." \
		"" "$(ls -A "$l_cwd")"
	zxfer_golden_assert_mock_tools_not_executed cli_error_log_relative_path
}

# The version a failure report names is the release the package declares.
test_failure_report_names_the_packaged_release_version() {
	l_spec_version=$(sed -n 's/^Version:[[:space:]]*//p' "$ZXFER_ROOT/packaging/zxfer.spec")

	zxfer_golden_invoke_zxfer -R tank/src

	assertNotNull "packaging/zxfer.spec must declare a Version." "$l_spec_version"
	assertEquals "The failure report must name the packaged release." \
		"zxfer_version: $l_spec_version" \
		"$(grep '^zxfer_version: ' "$g_golden_stderr_file")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
