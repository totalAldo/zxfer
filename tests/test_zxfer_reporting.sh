#!/bin/sh
#
# shunit2 tests for zxfer_reporting.sh, the ZXFER_ERROR_LOG mirror and lock in
# zxfer_error_log.sh, and zxfer_profile.sh.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_error_log.sh"

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
}

# Make a log parent read-only for this user. When the user can still write
# it (root, or an ACL), restore it and skip the test, which needs a parent
# that refuses the atomic-rename path.
make_error_log_parent_read_only() {
	chmod 500 "$1" || fail "Unable to make $1 read-only."
	if [ -w "$1" ]; then
		chmod 700 "$1"
		startSkipping
		return 1
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

test_zxfer_append_failure_report_to_log_appends_existing_file_when_parent_is_not_writable() {
	log_dir="$TEST_TMPDIR/nonwritable-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_parent.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_parent.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
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

test_zxfer_append_failure_report_to_log_rechecks_existence_under_lock_before_creating() {
	# A concurrent winner can create the log while this run waits on the
	# error-log lock; the stale pre-lock existence answer must not route the
	# waiting run onto the create path, which would clobber the winner's
	# freshly published report with an empty staged file.
	log_path="$TEST_TMPDIR/recheck-under-lock.log"
	rm -f "$log_path"

	output=$(
		(
			ZXFER_ERROR_LOG="$log_path"
			zxfer_acquire_error_log_lock() {
				# Simulate the concurrent winner publishing the log while
				# this run was blocked on lock acquisition.
				printf 'winner: report-kept\n' >"$log_path"
				chmod 600 "$log_path"
				return 0
			}
			set +e
			zxfer_append_failure_report_to_log "loser: report-appended"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Appending after a lock wait should still succeed." \
		"$output" "status=0"
	assertContains "The concurrent winner's report must survive the recheck (no create-path clobber)." \
		"$(cat "$log_path")" "winner: report-kept"
	assertContains "The waiting run's report must append behind the winner's content." \
		"$(cat "$log_path")" "loser: report-appended"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_system_tmp_fallback_chain() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			case "$1" in
			"/unsafe-tmpdir"|"/dev/shm"|"/run/shm")
				return 1
				;;
			"/tmp")
				printf "%s\n" "/tmp"
				return 0
				;;
			esac
			return 1
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "/tmp/.zxfer-error-log.lock.d/prepared/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /tmp is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the prepared exact lock path under the first safe tmpdir candidate." \
		"/tmp/.zxfer-error-log.lock.d/prepared/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_dev_shm_fallback_when_available() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			case "$1" in
			"/dev/shm"|"/run/shm"|"/tmp")
				printf "%s\n" "$1"
				return 0
				;;
			esac
			return 1
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "$1/.zxfer-error-log.lock.d/prepared-for:$2/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /dev/shm is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should prepare the lock for the log under /dev/shm when TMPDIR is unsafe." \
		"/dev/shm/.zxfer-error-log.lock.d/prepared-for:/tmp/failure.log/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_run_shm_fallback_when_dev_shm_is_unavailable() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			case "$1" in
			"/run/shm"|"/tmp")
				printf "%s\n" "$1"
				return 0
				;;
			esac
			return 1
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "$1/.zxfer-error-log.lock.d/prepared/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /run/shm is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the prepared exact lock path under /run/shm when /dev/shm is unavailable." \
		"/run/shm/.zxfer-error-log.lock.d/prepared/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_prefers_a_trusted_tmpdir() {
	zxfer_test_capture_subshell '
		TMPDIR="/trusted tmp"
		zxfer_validate_temp_root_candidate() {
			printf "%s\n" "/physical$1"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "$1/.zxfer-error-log.lock.d/prepared/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when TMPDIR is trusted." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the validated physical TMPDIR, spaces intact, before system candidates." \
		"/physical/trusted tmp/.zxfer-error-log.lock.d/prepared/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_skips_an_empty_tmpdir() {
	candidates_log="$TEST_TMPDIR/empty-tmpdir-candidates.log"
	rm -f "$candidates_log"

	zxfer_test_capture_subshell "
		TMPDIR=''
		zxfer_validate_temp_root_candidate() {
			printf 'candidate=<%s>\n' \"\$1\" >>'$candidates_log'
			[ \"\$1\" = /tmp ] || return 1
			printf '%s\n' \"\$1\"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf '%s\n' \"\$1/lock\"
		}
		zxfer_get_error_log_fallback_lock_dir /tmp/failure.log
	"

	assertEquals "Fallback lock-dir lookup should succeed through the system candidates." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "An empty TMPDIR should not be validated as a candidate." \
		"candidate=</dev/shm>
candidate=</run/shm>
candidate=</tmp>" "$(cat "$candidates_log")"
}

test_zxfer_get_error_log_fallback_lock_dir_returns_failure_when_no_safe_tmpdir_exists() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should fail closed when no safe temp-root candidate exists." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed fallback lock-dir lookups should not emit a path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_returns_failure_when_lock_path_prepare_fails() {
	zxfer_test_capture_subshell '
		TMPDIR="/safe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			printf "%s\n" "/safe-tmpdir"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should fail when the exact lock path cannot be prepared." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed fallback lock-dir lookups should not emit a partial path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_ensure_error_log_fallback_lock_component_dir_rejects_symlinks() {
	component_target="$TEST_TMPDIR/error-log-component-target"
	component_link="$TEST_TMPDIR/error-log-component-link"
	mkdir "$component_target"
	ln -s "$component_target" "$component_link"

	set +e
	zxfer_ensure_error_log_fallback_lock_component_dir "$component_link"
	component_status=$?

	assertEquals "Fallback error-log lock components must reject symlinks before changing their mode or contents." \
		1 "$component_status"
}

test_zxfer_error_log_lock_identity_hex_fails_when_hex_encoding_is_empty() {
	output=$(
		(
			set +e
			od() {
				:
			}
			zxfer_error_log_lock_identity_hex "/tmp/failure.log" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Error-log lock identity derivation should fail closed when exact hex encoding produces no output." \
		"$output" "status=1"
}

test_zxfer_error_log_lock_identity_hex_uses_exact_hex_in_current_shell() {
	od() {
		printf ' 66 6f 6f 0a \n'
	}
	identity_hex=$(zxfer_error_log_lock_identity_hex "/tmp/failure.log")
	unset -f od

	assertEquals "Current-shell error-log lock identity should use exact lowercase hex from od." \
		"666f6f0a" "$identity_hex"
}

test_zxfer_prepare_error_log_fallback_lock_dir_distinguishes_known_legacy_cksum_collision_paths() {
	path_one="/var/log/zxfer-3kzpfymt.log"
	path_two="/var/log/zxfer-amu2x4ex.log"

	lock_one=$(zxfer_prepare_error_log_fallback_lock_dir "$TEST_TMPDIR" "$path_one") ||
		fail "Expected fallback lock preparation to succeed for the first legacy collision path."
	lock_two=$(zxfer_prepare_error_log_fallback_lock_dir "$TEST_TMPDIR" "$path_two") ||
		fail "Expected fallback lock preparation to succeed for the second legacy collision path."

	assertNotEquals "Fallback error-log lock paths should not collapse known legacy cksum-collision log paths." \
		"$lock_one" "$lock_two"
	assertTrue "Fallback lock preparation should create the exact lock parent directory." \
		"[ -d \"${lock_one%/lock}\" ]"
}

test_zxfer_acquire_error_log_lock_retries_before_failing() {
	zxfer_test_capture_subshell '
		g_test_sleep_calls=0
		mkdir() {
			return 1
		}
		sleep() {
			g_test_sleep_calls=$((g_test_sleep_calls + 1))
			return 0
		}
		zxfer_acquire_error_log_lock "/tmp/lock-dir"
		l_status=$?
		printf "status=%s\n" "$l_status"
		printf "sleeps=%s\n" "$g_test_sleep_calls"
		[ "$l_status" -eq 1 ]
	'

	assertEquals "Repeated lock-dir creation failures should eventually return a non-zero status." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Lock acquisition should report the expected failure status after exhausting retries." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
	assertContains "Lock acquisition should sleep between failed retries before giving up." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "sleeps=2"
}

test_zxfer_acquire_error_log_lock_reaps_stale_lock_and_retries_successfully() {
	zxfer_test_capture_subshell '
		g_test_create_calls=0
		g_test_reap_calls=0
		zxfer_create_owned_lock_dir() {
			g_test_create_calls=$((g_test_create_calls + 1))
			if [ "$g_test_create_calls" -eq 1 ]; then
				mkdir -p "$1"
				return 1
			fi
			return 0
		}
		zxfer_try_reap_stale_owned_lock_dir() {
			g_test_reap_calls=$((g_test_reap_calls + 1))
			rm -rf "$1"
			return 0
		}
		zxfer_acquire_error_log_lock "'"$TEST_TMPDIR"'/reapable.lock"
		printf "status=%s\n" "$?"
		printf "creates=%s\n" "$g_test_create_calls"
		printf "reaps=%s\n" "$g_test_reap_calls"
	'

	assertContains "Error-log lock acquisition should succeed after reaping one stale lock directory." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=0"
	assertContains "Error-log lock acquisition should retry lock creation after a successful stale-lock reap." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "creates=2"
	assertContains "Error-log lock acquisition should attempt exactly one stale-lock reap in this path." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "reaps=1"
}

test_zxfer_acquire_error_log_lock_defers_corrupt_reap_until_recheck_round() {
	# A 0700 lock dir without metadata models a live winner inside its
	# mkdir-to-metadata publish window: the first sighting must be treated as
	# busy (lock preserved across the sleep) and the corrupt reap may only
	# happen after the recheck still reports corrupt metadata.
	lock_dir="$TEST_TMPDIR/midpublish.lock"
	mkdir -m 700 "$lock_dir" || fail "Unable to create the mid-publish lock fixture."
	g_test_sleep_calls=0
	g_test_lock_present_at_sleep=no
	sleep() {
		g_test_sleep_calls=$((g_test_sleep_calls + 1))
		if [ -d "$lock_dir" ]; then
			g_test_lock_present_at_sleep=yes
		fi
		return 0
	}

	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?
	unset -f sleep

	assertEquals "Acquisition should still succeed after the corrupt recheck round reaps the metadata-less lock." \
		0 "$status"
	assertEquals "The metadata-less lock must survive the first sighting (treated as busy, not reaped)." \
		yes "$g_test_lock_present_at_sleep"
	assertEquals "Exactly one recheck sleep should separate the corrupt sighting from the corrupt reap." \
		1 "$g_test_sleep_calls"
	assertTrue "The acquired lock should carry this process's published metadata." \
		"[ -f \"$lock_dir/metadata\" ]"

	zxfer_release_error_log_lock "$lock_dir" ||
		fail "Unable to release the acquired error-log lock fixture."
}

test_zxfer_acquire_error_log_lock_fails_closed_when_stale_reap_errors() {
	zxfer_test_capture_subshell '
		lock_dir="'"$TEST_TMPDIR"'/reap_error.lock"
		mkdir -p "$lock_dir"
		zxfer_create_owned_lock_dir() {
			return 1
		}
		zxfer_try_reap_stale_owned_lock_dir() {
			return 1
		}
		zxfer_acquire_error_log_lock "$lock_dir"
	'

	assertEquals "Error-log lock acquisition should fail closed when stale-lock reaping errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_acquire_error_log_lock_covers_stale_reap_error_in_current_shell() {
	lock_dir="$TEST_TMPDIR/reap_error_current.lock"
	mkdir -p "$lock_dir" || fail "Unable to create stale error-log lock directory."
	g_test_sleep_calls=0

	zxfer_create_owned_lock_dir() {
		return 1
	}
	zxfer_try_reap_stale_owned_lock_dir() {
		return 1
	}
	sleep() {
		g_test_sleep_calls=$((g_test_sleep_calls + 1))
		return 0
	}

	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?
	sleep_calls=$g_test_sleep_calls
	unset -f zxfer_create_owned_lock_dir zxfer_try_reap_stale_owned_lock_dir sleep
	zxfer_source_runtime_modules_through "zxfer_error_log.sh"

	assertEquals "Current-shell error-log lock acquisition should fail closed when stale-lock reaping errors." \
		1 "$status"
	assertEquals "Current-shell error-log lock acquisition should not retry hard stale-lock reap errors." \
		0 "$sleep_calls"
}

test_zxfer_release_error_log_lock_warns_and_returns_failure() {
	log_path="$TEST_TMPDIR/release_failure.log"
	lock_dir="$TEST_TMPDIR/release_failure.lock"
	stdout_file="$TEST_TMPDIR/release_failure.stdout"
	stderr_file="$TEST_TMPDIR/release_failure.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		zxfer_release_owned_lock_dir() {
			[ \"\$1\" = '$lock_dir' ] || return 99
			return 23
		}
		zxfer_release_error_log_lock '$log_path' '$lock_dir'
	"

	assertEquals "Error-log lock release should fail closed when the owned-lock release fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Error-log lock release failures should emit the documented warning with the owned-lock status." \
		"$(cat "$stderr_file")" "unable to release ZXFER_ERROR_LOG lock for \"$log_path\" (status 23)"
}

test_zxfer_release_error_log_lock_is_silent_on_success() {
	log_path="$TEST_TMPDIR/release_success.log"
	lock_dir="$TEST_TMPDIR/release_success.lock"
	stdout_file="$TEST_TMPDIR/release_success.stdout"
	stderr_file="$TEST_TMPDIR/release_success.stderr"
	rm -rf "$lock_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		zxfer_create_owned_lock_dir '$lock_dir' >/dev/null || exit 9
		zxfer_release_error_log_lock '$log_path' '$lock_dir'
	"

	assertEquals "Releasing a held error-log lock should succeed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A successful release should not warn." "" "$(cat "$stderr_file")"
	assertFalse "A successful release should remove the lock directory." "[ -e '$lock_dir' ]"
}

test_zxfer_append_failure_report_to_log_keeps_the_append_failure_when_release_also_fails() {
	log_path="$TEST_TMPDIR/append-and-release-failure.log"
	stdout_file="$TEST_TMPDIR/append_and_release_failure.stdout"
	stderr_file="$TEST_TMPDIR/append_and_release_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG='$log_path'
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_release_owned_lock_dir() {
			return 5
		}
		zxfer_append_failure_report_to_log report
	"

	assertEquals "The append failure should stay the reported status when lock release also fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The primary staging failure should still be reported." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG staging directory"
	assertContains "The secondary release failure should be reported as a warning." \
		"$(cat "$stderr_file")" "unable to release ZXFER_ERROR_LOG lock for \"$log_path\" (status 5)"
}

test_zxfer_append_failure_report_to_log_warns_when_nonwritable_parent_needs_create() {
	log_dir="$TEST_TMPDIR/nonwritable-create-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_create.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_create.stderr"

	mkdir -p "$log_dir"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Missing ZXFER_ERROR_LOG files should fail closed when the trusted parent is not writable." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing ZXFER_ERROR_LOG files in non-writable trusted parents should warn that creation is not possible." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_fallback_lock_lookup_fails() {
	log_dir="$TEST_TMPDIR/nonwritable-lock-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_lock.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_lock.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_get_error_log_fallback_lock_dir() {
			return 1
		}
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Fallback lock-path lookup failures should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Fallback lock-path lookup failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to acquire ZXFER_ERROR_LOG lock"
}

test_zxfer_append_failure_report_to_log_warns_when_direct_append_fails_in_nonwritable_parent() {
	log_dir="$TEST_TMPDIR/nonwritable-append-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_append.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_append.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		printf() {
			if [ \"\$2\" = \"message: appended-report\" ]; then
				return 1
			fi
			command printf \"\$@\"
		}
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Direct append failures in the non-writable-parent fallback path should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Direct append failures in the non-writable-parent fallback path should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_lock_acquisition_fails() {
	log_path="$TEST_TMPDIR/lock-failure.log"
	stdout_file="$TEST_TMPDIR/lock_failure.stdout"
	stderr_file="$TEST_TMPDIR/lock_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_acquire_error_log_lock() {
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Lock-acquisition failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Lock-acquisition failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to acquire ZXFER_ERROR_LOG lock"
}

test_zxfer_append_failure_report_to_log_warns_when_staging_dir_creation_fails() {
	log_path="$TEST_TMPDIR/stage-failure.log"
	stdout_file="$TEST_TMPDIR/stage_failure.stdout"
	stderr_file="$TEST_TMPDIR/stage_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Staging-dir creation failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Staging-dir creation failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG staging directory"
}

test_zxfer_create_secure_staging_dir_for_path_registers_and_cleanup_unregisters_error_log_stage_dirs() {
	log_path="$TEST_TMPDIR/runtime-cleanup.log"
	zxfer_reset_runtime_artifact_state
	zxfer_create_secure_staging_dir_for_path "$log_path" "zxfer-error-log" >/dev/null
	status=$?
	stage_dir=$g_zxfer_secure_staging_dir_result

	assertEquals "Secure error-log staging should succeed for writable parents." 0 "$status"
	assertTrue "Secure error-log staging should create the stage directory." \
		"[ -d \"$stage_dir\" ]"
	assertContains "Secure error-log staging should register its stage directory for abort cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$stage_dir"

	zxfer_cleanup_runtime_artifact_path "$stage_dir"

	assertFalse "Runtime artifact cleanup should remove the error-log stage directory." \
		"[ -e \"$stage_dir\" ]"
	assertNotContains "Runtime artifact cleanup should unregister the error-log stage directory." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$stage_dir"
}

test_zxfer_get_error_log_fallback_lock_dir_does_not_try_later_roots_after_prepare_fails() {
	prepare_log="$TEST_TMPDIR/prepare-once.log"
	rm -f "$prepare_log"

	zxfer_test_capture_subshell "
		TMPDIR=/first-trusted
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' \"\$1\"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf 'prepare=<%s>\n' \"\$1\" >>'$prepare_log'
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir /tmp/failure.log
	"

	assertEquals "Fallback lock-dir lookup should fail closed when the first trusted root cannot hold the lock." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should not fall through to later roots after a prepare failure." \
		"prepare=</first-trusted>" "$(cat "$prepare_log")"
}

test_zxfer_create_error_log_file_cleans_up_stage_dir_when_write_or_move_fails() {
	write_stage_dir="$TEST_TMPDIR/.zxfer-error-log.write.$$"
	move_stage_dir="$TEST_TMPDIR/.zxfer-error-log.move.$$"
	write_output=$(
		(
			set +e
			mkdir -p "$write_stage_dir"
			zxfer_register_runtime_artifact_path "$write_stage_dir" || exit 90
			zxfer_create_secure_staging_dir_for_path() {
				g_zxfer_secure_staging_dir_result="$write_stage_dir"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 1
			}
			zxfer_create_error_log_file "$TEST_TMPDIR/write_failure.log"
			printf 'status=%s\n' "$?"
			printf 'stage_exists=%s\n' "$([ -e "$write_stage_dir" ] && printf yes || printf no)"
		)
	)
	move_output=$(
		(
			set +e
			mkdir -p "$move_stage_dir"
			zxfer_register_runtime_artifact_path "$move_stage_dir" || exit 90
			zxfer_create_secure_staging_dir_for_path() {
				g_zxfer_secure_staging_dir_result="$move_stage_dir"
				return 0
			}
			mv() {
				return 1
			}
			zxfer_create_error_log_file "$TEST_TMPDIR/move_failure.log"
			printf 'status=%s\n' "$?"
			printf 'stage_exists=%s\n' "$([ -e "$move_stage_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Error-log file creation should fail when the staged file cannot be written." \
		"$write_output" "status=1"
	assertContains "Error-log file creation should remove the stage directory when the staged write fails." \
		"$write_output" "stage_exists=no"
	assertContains "Error-log file creation should fail when the staged file cannot be moved into place." \
		"$move_output" "status=1"
	assertContains "Error-log file creation should remove the stage directory when the final move fails." \
		"$move_output" "stage_exists=no"
}

test_zxfer_create_error_log_file_helpers_cover_current_shell_paths() {
	create_fail_target="$TEST_TMPDIR/error_log_create_fail.log"
	create_success_target="$TEST_TMPDIR/error_log_create_success.log"
	create_success_stage="$TEST_TMPDIR/.zxfer-error-log.success.$$"

	zxfer_test_capture_subshell "
		set +e
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_create_error_log_file \"$create_fail_target\" >/dev/null
		printf 'fail=%s\\n' \"\$?\"
		unset -f zxfer_create_secure_staging_dir_for_path

		mkdir -p \"$create_success_stage\" || exit 91
		zxfer_register_runtime_artifact_path \"$create_success_stage\" || exit 92
		zxfer_create_secure_staging_dir_for_path() {
			g_zxfer_secure_staging_dir_result=\"$create_success_stage\"
			return 0
		}
		zxfer_create_error_log_file \"$create_success_target\" >/dev/null
		printf 'success=%s\\n' \"\$?\"
		unset -f zxfer_create_secure_staging_dir_for_path
		printf 'target=%s\\n' \"\$([ -f \"$create_success_target\" ] && printf yes || printf no)\"
		printf 'contents=<%s>\\n' \"\$([ -f \"$create_success_target\" ] && cat \"$create_success_target\")\"
		printf 'stage=%s\\n' \"\$([ -e \"$create_success_stage\" ] && printf yes || printf no)\"
	"

	assertEquals "Current-shell error-log creation helper coverage should complete the subshell cleanly." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Current-shell error-log creation should preserve staging-dir allocation failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "fail=1"
	assertContains "Current-shell error-log creation should succeed when staging and publish both succeed." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "success=0"
	assertContains "Current-shell error-log creation should publish the target log file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "target=yes"
	assertContains "Current-shell error-log creation should create an empty secure log file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "contents=<>"
	assertContains "Current-shell error-log creation should remove the staging directory after publishing the file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "stage=no"
}

test_zxfer_acquire_error_log_lock_rejects_symlink_and_reap_validation_failures() {
	lock_target="$TEST_TMPDIR/error_log_lock_target"
	lock_symlink="$TEST_TMPDIR/error_log_lock_symlink"
	lock_dir="$TEST_TMPDIR/error_log_lock_dir"
	mkdir -p "$lock_target" "$lock_dir" || fail "Unable to create error-log lock fixtures."
	ln -s "$lock_target" "$lock_symlink" || fail "Unable to create the error-log lock symlink fixture."

	symlink_output=$(
		(
			set +e
			zxfer_create_owned_lock_dir() {
				return 1
			}
			zxfer_acquire_error_log_lock "$lock_symlink"
			printf 'status=%s\n' "$?"
		)
	)
	reap_output=$(
		(
			set +e
			zxfer_create_owned_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir() {
				return 1
			}
			zxfer_acquire_error_log_lock "$lock_dir"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Error-log lock acquisition should fail closed when the target path is a symlink." \
		"$symlink_output" "status=1"
	assertContains "Error-log lock acquisition should fail closed when stale-lock reaping reports a validation failure." \
		"$reap_output" "status=1"
}

test_zxfer_acquire_error_log_lock_reports_reap_validation_failures_in_current_shell() {
	lock_dir="$TEST_TMPDIR/error_log_lock_reap_current"
	mkdir -p "$lock_dir" || fail "Unable to create the current-shell error-log lock fixture."

	zxfer_create_owned_lock_dir() {
		return 1
	}
	zxfer_try_reap_stale_owned_lock_dir() {
		return 1
	}

	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?

	zxfer_source_runtime_modules_through "zxfer_error_log.sh"
	setUp

	assertEquals "Current-shell error-log lock acquisition should fail closed when stale-lock reaping returns a validation failure." \
		1 "$status"
}

test_zxfer_append_failure_report_to_log_warns_when_snapshot_link_fails() {
	log_path="$TEST_TMPDIR/snapshot-link-failure.log"
	stdout_file="$TEST_TMPDIR/snapshot_link_failure.stdout"
	stderr_file="$TEST_TMPDIR/snapshot_link_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		ln() {
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Snapshot-link failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Snapshot-link failures should emit the append warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_snapshot_validation_fails() {
	log_path="$TEST_TMPDIR/snapshot-validation-failure.log"
	stdout_file="$TEST_TMPDIR/snapshot_validation_failure.stdout"
	stderr_file="$TEST_TMPDIR/snapshot_validation_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		g_test_validation_calls=0
		zxfer_validate_existing_error_log_file() {
			g_test_validation_calls=\$((g_test_validation_calls + 1))
			if [ \"\$g_test_validation_calls\" -eq 1 ]; then
				return 0
			fi
			printf '%s\n' \"zxfer: warning: refusing ZXFER_ERROR_LOG file \\\"\$2\\\" because its permissions could not be determined.\" >&2
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Snapshot-validation failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Snapshot-validation failures should preserve the validation warning." \
		"$(cat "$stderr_file")" "permissions could not be determined"
}

test_zxfer_append_failure_report_to_log_warns_when_snapshot_copy_fails() {
	log_path="$TEST_TMPDIR/snapshot-copy-failure.log"
	stdout_file="$TEST_TMPDIR/snapshot_copy_failure.stdout"
	stderr_file="$TEST_TMPDIR/snapshot_copy_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		cat() {
			case \"\$1\" in
			*/log.snapshot)
				return 1
				;;
			esac
			command cat \"\$@\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Snapshot-copy failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Snapshot-copy failures should emit the append warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_atomic_move_fails() {
	log_path="$TEST_TMPDIR/move-failure.log"
	stdout_file="$TEST_TMPDIR/move_failure.stdout"
	stderr_file="$TEST_TMPDIR/move_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		mv() {
			case \"\$1:\$2\" in
			-f:*/log.write | */log.write:*)
				return 1
				;;
			esac
			case \"\$1\" in
			*/log.write)
				return 1
				;;
			esac
			command mv \"\$@\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Atomic-move failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Atomic-move failures should emit the append warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_returns_failure_when_created_file_fails_validation() {
	log_path="$TEST_TMPDIR/create-validation-failure.log"
	stdout_file="$TEST_TMPDIR/create_validation_failure.stdout"
	stderr_file="$TEST_TMPDIR/create_validation_failure.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_validate_existing_error_log_file() {
			printf '%s\n' \"zxfer: warning: refusing ZXFER_ERROR_LOG file \\\"\$2\\\" because its permissions could not be determined.\" >&2
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Validation failures after secure file creation should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Validation failures after secure file creation should preserve the validation warning." \
		"$(cat "$stderr_file")" "permissions could not be determined"
}

test_zxfer_append_failure_report_to_log_warns_when_staged_log_chmod_fails() {
	log_path="$TEST_TMPDIR/staged-chmod-failure.log"
	stdout_file="$TEST_TMPDIR/staged_chmod_failure.stdout"
	stderr_file="$TEST_TMPDIR/staged_chmod_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		chmod() {
			case \"\$2\" in
			*/log.write)
				return 1
				;;
			esac
			command chmod \"\$@\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Staged-log chmod failures should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Staged-log chmod failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to chmod ZXFER_ERROR_LOG file"
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
