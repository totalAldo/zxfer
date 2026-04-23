#!/bin/sh
#
# shunit2 tests for zxfer_reporting.sh helpers.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_runtime.sh"

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
	zxfer_reset_failure_context "unit"
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

test_zxfer_append_failure_report_to_log_appends_existing_file_when_parent_is_not_writable() {
	log_dir="$TEST_TMPDIR/nonwritable-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_parent.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_parent.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	chmod 500 "$log_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_error_log_parent_is_writable() {
			return 1
		}
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
		zxfer_error_log_lock_key() {
			printf "%s\n" "kfallback"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /tmp is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the derived lock key under the first safe tmpdir candidate." \
		"/tmp/.zxfer-error-log.lock.kfallback" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_dev_shm_fallback_when_available() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_capture_reporting_helper_output() {
			l_result_var=$1
			l_helper_name=$2
			l_helper_arg=$3
			case "$l_helper_name:$l_helper_arg" in
			"zxfer_validate_temp_root_candidate:/unsafe-tmpdir")
				return 1
				;;
			"zxfer_validate_temp_root_candidate:/dev/shm")
				eval "$l_result_var=/dev/shm"
				return 0
				;;
			"zxfer_error_log_lock_key:/tmp/failure.log")
				eval "$l_result_var=kdevshm"
				return 0
				;;
			esac
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /dev/shm is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the derived lock key under /dev/shm when that candidate validates." \
		"/dev/shm/.zxfer-error-log.lock.kdevshm" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_run_shm_fallback_when_dev_shm_is_unavailable() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_capture_reporting_helper_output() {
			l_result_var=$1
			l_helper_name=$2
			l_helper_arg=$3
			case "$l_helper_name:$l_helper_arg" in
			"zxfer_validate_temp_root_candidate:/unsafe-tmpdir"|\
			"zxfer_validate_temp_root_candidate:/dev/shm")
				return 1
				;;
			"zxfer_validate_temp_root_candidate:/run/shm")
				eval "$l_result_var=/run/shm"
				return 0
				;;
			"zxfer_error_log_lock_key:/tmp/failure.log")
				eval "$l_result_var=krunshm"
				return 0
				;;
			esac
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /run/shm is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the derived lock key under /run/shm when /dev/shm is unavailable." \
		"/run/shm/.zxfer-error-log.lock.krunshm" "$ZXFER_TEST_CAPTURE_OUTPUT"
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

test_zxfer_get_error_log_fallback_lock_dir_returns_failure_when_lock_key_lookup_fails() {
	zxfer_test_capture_subshell '
		TMPDIR="/safe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			printf "%s\n" "/safe-tmpdir"
		}
		zxfer_error_log_lock_key() {
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should fail when the lock key cannot be derived." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed fallback lock-dir lookups should not emit a partial path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_error_log_lock_key_falls_back_to_hex_and_zero_key_when_cksum_is_unusable() {
	output=$(
		(
			set +e
			cksum() {
				return 1
			}
			od() {
				printf ' 61 62 63 64 \n'
			}
			printf 'hex=%s\n' "$(zxfer_error_log_lock_key '/tmp/failure.log')"
			od() {
				printf '\n'
			}
			printf 'zero=%s\n' "$(zxfer_error_log_lock_key '/tmp/failure.log')"
		)
	)

	assertContains "Error-log lock keys should fall back to a hex digest when cksum is unusable." \
		"$output" "hex=k61626364"
	assertContains "Error-log lock keys should fall back to k00 when both cksum and hex derivation are unusable." \
		"$output" "zero=k00"
}

test_zxfer_error_log_lock_key_falls_back_to_hex_and_zero_key_in_current_shell() {
	cksum() {
		return 1
	}
	od() {
		printf ' 66 6f 6f 0a \n'
	}
	hex_key=$(zxfer_error_log_lock_key "/tmp/failure.log")
	od() {
		printf '\n'
	}
	zero_key=$(zxfer_error_log_lock_key "/tmp/failure.log")
	unset -f cksum od

	assertEquals "Current-shell error-log lock keys should fall back to a hex digest when cksum is unusable." \
		"k666f6f0a" "$hex_key"
	assertEquals "Current-shell error-log lock keys should fall back to k00 when both cksum and hex derivation are unusable." \
		"k00" "$zero_key"
}

test_zxfer_capture_reporting_helper_output_preserves_readback_failures_and_cleans_up() {
	capture_file="$TEST_TMPDIR/reporting_capture_failure.out"

	zxfer_test_capture_subshell "
		set +e
		zxfer_create_runtime_artifact_file() {
			: >\"$capture_file\"
			g_zxfer_runtime_artifact_path_result=\"$capture_file\"
			return 0
		}
		zxfer_read_runtime_artifact_file() {
			g_zxfer_runtime_artifact_read_result=''
			return 17
		}
		zxfer_capture_reporting_helper_output l_result printf '%s\\n' 'captured'
		printf 'status=%s\\n' \"\$?\"
		printf 'exists=%s\\n' \"\$([ -e \"$capture_file\" ] && printf yes || printf no)\"
	"

	assertContains "Reporting-helper captures should preserve staged readback failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=17"
	assertContains "Reporting-helper captures should clean up the staged capture file after readback failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "exists=no"
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

test_zxfer_release_error_log_lock_warn_only_warns_and_returns_success() {
	log_path="$TEST_TMPDIR/release_warn_only.log"
	lock_dir="$TEST_TMPDIR/release_warn_only.lock"
	stdout_file="$TEST_TMPDIR/release_warn_only.stdout"
	stderr_file="$TEST_TMPDIR/release_warn_only.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		zxfer_release_error_log_lock() {
			return 17
		}
		zxfer_release_error_log_lock_warn_only \"$log_path\" \"$lock_dir\"
	"

	assertEquals "Warn-only error-log lock release should still return success." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Warn-only error-log lock release should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to release ZXFER_ERROR_LOG lock for \"$log_path\" (status 17)"
}

test_zxfer_release_error_log_lock_checked_warns_and_fails_closed() {
	log_path="$TEST_TMPDIR/release_checked.log"
	lock_dir="$TEST_TMPDIR/release_checked.lock"
	stdout_file="$TEST_TMPDIR/release_checked.stdout"
	stderr_file="$TEST_TMPDIR/release_checked.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		zxfer_release_error_log_lock() {
			return 23
		}
		zxfer_release_error_log_lock_checked \"$log_path\" \"$lock_dir\"
	"

	assertEquals "Checked error-log lock release should fail closed when the shared release helper fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Checked error-log lock release should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to release ZXFER_ERROR_LOG lock for \"$log_path\" (status 23)"
}

test_zxfer_append_failure_report_to_log_warns_when_nonwritable_parent_needs_create() {
	log_dir="$TEST_TMPDIR/nonwritable-create-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_create.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_create.stderr"

	mkdir -p "$log_dir"
	chmod 500 "$log_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_error_log_parent_is_writable() {
			return 1
		}
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
	chmod 500 "$log_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_error_log_parent_is_writable() {
			return 1
		}
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
	chmod 500 "$log_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_error_log_parent_is_writable() {
			return 1
		}
		zxfer_append_failure_report_to_existing_log_directly() {
			return 1
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

	zxfer_cleanup_error_log_stage_dir "$stage_dir"

	assertFalse "Error-log stage-dir cleanup should remove the created stage directory." \
		"[ -e \"$stage_dir\" ]"
	assertNotContains "Error-log stage-dir cleanup should unregister the stage directory from runtime cleanup state." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$stage_dir"
}

test_zxfer_cleanup_error_log_stage_dir_falls_back_without_runtime_cleanup_helper_in_current_shell() {
	stage_dir="$TEST_TMPDIR/error_log_stage_dir_fallback"
	mkdir -p "$stage_dir" || fail "Unable to create the fallback error-log stage directory."
	: >"$stage_dir/log.snapshot"
	: >"$stage_dir/log.write"

	unset -f zxfer_cleanup_runtime_artifact_path
	zxfer_cleanup_error_log_stage_dir "$stage_dir"

	assertFalse "Error-log stage-dir cleanup should remove staged files even when the runtime cleanup helper is unavailable." \
		"[ -e \"$stage_dir\" ]"

	zxfer_source_runtime_modules_through "zxfer_runtime.sh"
	setUp
}

test_zxfer_get_error_log_fallback_lock_dir_reports_lock_key_capture_failures_in_current_shell() {
	TMPDIR="$TEST_TMPDIR"

	zxfer_capture_reporting_helper_output() {
		l_result_var=$1
		l_helper_name=$2
		l_helper_arg=$3
		case "$l_helper_name:$l_helper_arg" in
		"zxfer_validate_temp_root_candidate:$TEST_TMPDIR")
			eval "$l_result_var=\$TEST_TMPDIR"
			return 0
			;;
		"zxfer_error_log_lock_key:/tmp/failure.log")
			return 1
			;;
		esac
		return 1
	}

	set +e
	zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log" >/dev/null
	status=$?
	set -e
	unset -f zxfer_capture_reporting_helper_output

	assertEquals "Current-shell fallback lock-dir lookup should fail closed when the derived lock key cannot be captured." \
		1 "$status"
}

test_zxfer_create_error_log_file_cleans_up_stage_dir_when_write_or_move_fails() {
	write_stage_dir="$TEST_TMPDIR/error_log_write_stage"
	move_stage_dir="$TEST_TMPDIR/error_log_move_stage"
	write_output=$(
		(
			set +e
			mkdir -p "$write_stage_dir"
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
	create_success_stage="$TEST_TMPDIR/error_log_create_success.stage"

	zxfer_test_capture_subshell "
		set +e
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_create_error_log_file \"$create_fail_target\" >/dev/null
		printf 'fail=%s\\n' \"\$?\"
		unset -f zxfer_create_secure_staging_dir_for_path

		mkdir -p \"$create_success_stage\" || exit 91
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

	set +e
	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?
	set -e

	zxfer_source_runtime_modules_through "zxfer_runtime.sh"
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
		zxfer_chmod_error_log_file() {
			case \"\$1\" in
			*/log.write)
				return 1
				;;
			esac
			command chmod 600 \"\$1\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Staged-log chmod failures should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Staged-log chmod failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to chmod ZXFER_ERROR_LOG file"
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
