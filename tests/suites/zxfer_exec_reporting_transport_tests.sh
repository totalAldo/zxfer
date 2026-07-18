#!/bin/sh
# Reporting, transport rendering, and path-security behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_zxfer_quote_command_argv_escapes_control_chars_and_apostrophes() {
	l_newline_arg=$(printf 'line1\nline2')

	result=$(zxfer_quote_command_argv "./zxfer" "value with space" "$l_newline_arg" "apost'rophe")
	expected="'./zxfer' 'value with space' 'line1\\nline2' 'apost'\"'\"'rophe'"

	assertEquals "Quoted argv should remain one-line and shell-safe for reports." "$expected" "$result"
}

test_zxfer_render_command_for_report_appends_quoted_argv_to_prefix() {
	result=$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs" "create" "-o" "compression=lz4")
	expected="/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4'"

	assertEquals "Report rendering should preserve the shell-ready prefix and quote appended argv tokens." \
		"$expected" "$result"
}

test_zxfer_render_command_for_report_returns_prefix_when_no_argv_are_provided() {
	result=$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs")

	assertEquals "Report rendering should return the prefix unchanged when no argv tokens are appended." \
		"/usr/bin/ssh 'host' /sbin/zfs" "$result"
}

test_zxfer_render_command_for_report_quotes_argv_when_prefix_is_empty() {
	result=$(zxfer_render_command_for_report "" "zfs" "list" "tank/src")

	assertEquals "Report rendering should still quote argv tokens when no shell prefix is supplied." \
		"'zfs' 'list' 'tank/src'" "$result"
}

test_zxfer_render_failure_report_includes_context_fields() {
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=1
	g_option_Y_yield_iterations=8
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_zxfer_original_invocation="'./zxfer' '-R' 'tank/src' 'backup/dst'"
	g_zxfer_failure_class="runtime"
	g_zxfer_failure_stage="send/receive"
	g_zxfer_failure_message="replication failed"
	g_zxfer_failure_source_root="tank/src"
	g_zxfer_failure_current_source="tank/src/child"
	g_zxfer_failure_destination_root="backup/dst"
	g_zxfer_failure_current_destination="backup/dst/child"
	g_zxfer_failure_last_command="'/sbin/zfs' 'send' 'tank/src@snap1'"

	report=$(zxfer_render_failure_report 1)

	assertContains "$report" "zxfer: failure report begin"
	assertContains "$report" "failure_stage: send/receive"
	assertContains "$report" "source_root: tank/src"
	assertContains "$report" "current_destination: backup/dst/child"
	assertContains "$report" "invocation: [redacted]"
	assertContains "$report" "last_command: [redacted]"
	assertContains "$report" "zxfer: failure report end"
}

test_zxfer_usage_error_failure_report_redacts_invocation_by_default() {
	secure_path_dir="$TEST_TMPDIR/usage_redaction_secure_path"
	stdout_file="$TEST_TMPDIR/usage_redaction.stdout"
	stderr_file="$TEST_TMPDIR/usage_redaction.stderr"
	secret_source="tank/secret-source"

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -R "$secret_source" >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Usage-error launcher runs should still exit with usage status when failure-report command redaction is enabled by default." \
		2 "$status"
	assertContains "Default failure-report command redaction should replace the launcher-captured invocation in stderr." \
		"$(cat "$stderr_file")" "invocation: [redacted]"
	assertNotContains "Default failure-report command redaction should keep secret-bearing usage arguments out of stderr." \
		"$(cat "$stderr_file")" "$secret_source"
}

test_zxfer_usage_error_failure_report_escapes_control_bytes_in_invocation_in_unsafe_mode() {
	secure_path_dir="$TEST_TMPDIR/usage_escape_secure_path"
	stdout_file="$TEST_TMPDIR/usage_escape.stdout"
	stderr_file="$TEST_TMPDIR/usage_escape.stderr"
	esc=$(printf '\033')
	bell=$(printf '\007')
	control_source=$(printf 'tank/ctrl%s[31m%s' "$esc" "$bell")

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "$control_source" >"$stdout_file" 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/ctrl\\x1B[31m\\x07'" "$stderr_file" >/dev/null 2>&1
	escaped_esc_status=$?
	grep -F "\\\\x1B" "$stderr_file" >/dev/null 2>&1
	double_esc_status=$?
	grep -F "\\x07" "$stderr_file" >/dev/null 2>&1
	escaped_bell_status=$?
	grep -F "$esc" "$stderr_file" >/dev/null 2>&1
	raw_esc_status=$?
	grep -F "$bell" "$stderr_file" >/dev/null 2>&1
	raw_bell_status=$?

	assertEquals "Unsafe usage-error launcher runs should still exit with usage status when invocation control bytes are escaped." \
		2 "$status"
	assertEquals "Unsafe failure reports should render ESC bytes from the launcher-captured invocation as escaped text." \
		0 "$escaped_esc_status"
	assertEquals "Unsafe failure reports should not double-escape control-byte markers from the launcher-captured invocation." \
		1 "$double_esc_status"
	assertEquals "Unsafe failure reports should render BEL bytes from the launcher-captured invocation as escaped text." \
		0 "$escaped_bell_status"
	assertEquals "Unsafe failure reports should not contain raw ESC bytes from the launcher-captured invocation." \
		1 "$raw_esc_status"
	assertEquals "Unsafe failure reports should not contain raw BEL bytes from the launcher-captured invocation." \
		1 "$raw_bell_status"
}

test_zxfer_usage_error_failure_report_preserves_trailing_newline_in_invocation_in_unsafe_mode() {
	secure_path_dir="$TEST_TMPDIR/usage_trailing_newline_secure_path"
	stdout_file="$TEST_TMPDIR/usage_trailing_newline.stdout"
	stderr_file="$TEST_TMPDIR/usage_trailing_newline.stderr"
	trailing_source=$(printf 'tank/trailing-source\n_')
	trailing_source=${trailing_source%_}

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "$trailing_source" >"$stdout_file" 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/trailing-source\\n'" "$stderr_file" >/dev/null 2>&1
	trailing_newline_status=$?

	assertEquals "Unsafe usage-error launcher runs should still exit with usage status when invocation newline markers are preserved." \
		2 "$status"
	assertEquals "Unsafe failure reports should preserve trailing newline markers from the launcher-captured invocation." \
		0 "$trailing_newline_status"
}

test_zxfer_render_failure_report_omits_empty_optional_fields() {
	g_zxfer_version="test-version"
	g_zxfer_failure_class="runtime"
	g_zxfer_failure_stage="snapshot discovery"
	g_zxfer_failure_message="missing snapshot"

	report=$(zxfer_render_failure_report 3)

	assertContains "$report" "failure_stage: snapshot discovery"
	assertNotContains "$report" "current_source:"
	assertNotContains "$report" "current_destination:"
	assertNotContains "$report" "invocation:"
}

test_zxfer_render_failure_report_defaults_runtime_class_for_nonusage_exit() {
	report_file="$TEST_TMPDIR/runtime_default.report"
	zxfer_reset_failure_context "unit-test"
	g_zxfer_failure_class=""
	g_zxfer_failure_message=""
	g_zxfer_failure_stage=""

	zxfer_render_failure_report 1 >"$report_file"

	report=$(cat "$report_file")
	assertContains "Non-usage exits should default to runtime failures." \
		"$report" "failure_class: runtime"
	assertContains "Missing failure messages should fall back to the exit status summary." \
		"$report" "message: zxfer exited with status 1."
}

test_zxfer_render_failure_defaults_cover_usage_mode() {
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive="tank/src"
	zxfer_reset_failure_context "unit"

	report=$(zxfer_render_failure_report 2)

	assertContains "Failure reports should default exit status 2 to usage errors." \
		"$report" "failure_class: usage"
	assertContains "Failure reports should default missing messages to the exit-status text." \
		"$report" "message: zxfer exited with status 2."
	assertContains "Failure reports should identify nonrecursive mode when -N is set." \
		"$report" "mode: nonrecursive"
}

test_throw_error_writes_message_to_stderr() {
	stdout_file="$TEST_TMPDIR/zxfer_throw_error.stdout"
	stderr_file="$TEST_TMPDIR/zxfer_throw_error.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error "boom" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_error should preserve the requested exit status." 3 "$status"
	assertEquals "zxfer_throw_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "boom"
}

test_throw_usage_error_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/throw_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_usage_error "bad option"
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_usage_error should exit with usage status 2." 2 "$status"
	assertEquals "zxfer_throw_usage_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: bad option"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}

test_zxfer_help_bypasses_dependency_init() {
	secure_path_dir="$TEST_TMPDIR/help_secure_path"
	hostile_path_dir="$TEST_TMPDIR/help_hostile_path"
	marker_file="$TEST_TMPDIR/help_hostile_path.marker"
	stdout_file="$TEST_TMPDIR/help.stdout"
	stderr_file="$TEST_TMPDIR/help.stderr"
	real_awk=$(command -v awk 2>/dev/null || :)
	real_sed=$(command -v sed 2>/dev/null || :)
	mkdir -p "$secure_path_dir"
	mkdir -p "$hostile_path_dir"

	if [ -z "$real_awk" ] || [ -z "$real_sed" ]; then
		fail "Host test requires awk and sed on the local system PATH."
	fi

	cat >"$hostile_path_dir/awk" <<EOF
#!/bin/sh
printf '%s\n' "awk" >>"$marker_file"
exec "$real_awk" "\$@"
EOF
	cat >"$hostile_path_dir/sed" <<EOF
#!/bin/sh
printf '%s\n' "sed" >>"$marker_file"
exec "$real_sed" "\$@"
EOF
	chmod +x "$hostile_path_dir/awk" "$hostile_path_dir/sed"

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="$hostile_path_dir:/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -h >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Help output should succeed even when the secure PATH lacks required tools." 0 "$status"
	assertContains "$(cat "$stdout_file")" "usage:"
	assertContains "Help output should advertise the standalone -c service list option." \
		"$(cat "$stdout_file")" "[-c FMRI|pattern[ FMRI|pattern]...]"
	assertContains "Help output should advertise the migration flag separately from -c." \
		"$(cat "$stdout_file")" "[-m]"
	assertContains "Help output should advertise the unsupported-property skip flag." \
		"$(cat "$stdout_file")" "[-U]"
	assertEquals "Help prescan should bypass dependency initialization errors." "" "$(cat "$stderr_file")"
	if [ -f "$marker_file" ]; then
		marker_contents=$(cat "$marker_file")
	else
		marker_contents=""
	fi
	assertEquals "Early invocation capture should not execute PATH-injected awk/sed helpers." "" "$marker_contents"
}

test_zxfer_usage_error_with_very_verbose_does_not_emit_profile_summary() {
	secure_path_dir="$TEST_TMPDIR/usage_secure_path"
	stdout_file="$TEST_TMPDIR/usage.stdout"
	stderr_file="$TEST_TMPDIR/usage.stderr"

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -V >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Very-verbose usage errors should still exit with usage status." 2 "$status"
	assertEquals "Usage errors should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: Need a destination."
	assertNotContains "Usage-mode very-verbose exits should not emit profiling counters." \
		"$(cat "$stderr_file")" "zxfer profile:"
}

test_trap_exit_emits_failure_report_once() {
	set +e
	output=$(
		(
			g_zxfer_failure_class="runtime"
			g_zxfer_failure_stage="unit"
			g_zxfer_failure_message="trap failure"
			g_delete_source_tmp_file=""
			g_delete_dest_tmp_file=""
			g_delete_snapshots_to_delete_tmp_file=""
			g_services_need_relaunch=0
			false
			zxfer_trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	count=$(printf '%s\n' "$output" | grep -c "^zxfer: failure report begin$")
	assertEquals "zxfer_trap_exit helper path should preserve the failing exit status." 1 "$status"
	assertEquals "zxfer_trap_exit should emit the failure report only once even when EXIT re-triggers cleanup." 1 "$count"
}

test_trap_exit_emits_profile_summary_once_in_very_verbose_mode() {
	set +e
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_summary_emitted=0
			g_zxfer_profile_start_epoch=$(($(date +%s) - 3))
			g_zxfer_profile_startup_latency_ms=99
			g_zxfer_profile_cleanup_ms=55
			g_zxfer_profile_ssh_setup_ms=111
			g_zxfer_profile_source_snapshot_listing_ms=222
			g_zxfer_profile_destination_snapshot_listing_ms=333
			g_zxfer_profile_snapshot_diff_sort_ms=444
			g_zxfer_profile_source_zfs_calls=3
			g_zxfer_profile_destination_zfs_calls=4
			g_zxfer_profile_ssh_shell_invocations=2
			g_zxfer_profile_source_snapshot_list_commands=1
			g_zxfer_profile_send_receive_pipeline_commands=2
			g_zxfer_profile_exists_destination_calls=5
			g_zxfer_profile_normalized_property_reads_source=6
			g_zxfer_profile_normalized_property_reads_destination=7
			g_zxfer_profile_required_property_backfill_gets=1
			g_zxfer_profile_parent_destination_property_reads=2
			g_zxfer_profile_bucket_source_inspection=8
			g_zxfer_profile_bucket_destination_inspection=9
			g_zxfer_profile_bucket_property_reconciliation=10
			g_zxfer_profile_bucket_send_receive_setup=11
			g_delete_source_tmp_file=""
			g_delete_dest_tmp_file=""
			g_delete_snapshots_to_delete_tmp_file=""
			g_services_need_relaunch=0
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when only emitting profiling output." 0 "$status"
	assertContains "Very-verbose exits should emit the source zfs profile counter." \
		"$output" "zxfer profile: source_zfs_calls=3"
	assertContains "Very-verbose exits should emit startup latency timing." \
		"$output" "zxfer profile: startup_latency_ms=99"
	assertContains "Very-verbose exits should emit cleanup timing." \
		"$output" "zxfer profile: cleanup_ms="
	assertContains "Very-verbose exits should emit the accumulated ssh setup stage timing." \
		"$output" "zxfer profile: ssh_setup_ms=111"
	assertContains "Very-verbose exits should emit the accumulated snapshot diff/sort stage timing." \
		"$output" "zxfer profile: snapshot_diff_sort_ms=444"
	assertContains "Very-verbose exits should emit the property-read profile counter." \
		"$output" "zxfer profile: normalized_property_reads_destination=7"
	assertContains "Very-verbose exits should emit the send/receive bucket counter." \
		"$output" "zxfer profile: bucket_send_receive_setup=11"
	count=$(printf '%s\n' "$output" | grep -c "^zxfer profile: source_zfs_calls=3$")
	assertEquals "zxfer_trap_exit should emit the profile summary only once." 1 "$count"
}

test_zxfer_profile_emit_summary_returns_without_output_when_already_emitted() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_summary_emitted=1
			zxfer_profile_emit_summary
		) 2>&1
	)
	status=$?

	assertEquals "An already-emitted profile summary should return success." 0 "$status"
	assertEquals "An already-emitted profile summary should not emit duplicate output." "" "$output"
}

test_zxfer_profile_increment_counter_normalizes_blank_and_invalid_inputs_in_current_shell() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_zxfer_profile_test_counter="bogus"

	zxfer_profile_increment_counter ""
	assertEquals "Blank profile counter names should be ignored without marking profile data present." \
		0 "$g_zxfer_profile_has_data"

	zxfer_profile_increment_counter g_zxfer_profile_test_counter "bogus"

	assertEquals "Profile counter updates should mark that profile data exists." 1 "$g_zxfer_profile_has_data"
	assertEquals "Invalid increment amounts and counter values should be normalized before incrementing." \
		1 "$g_zxfer_profile_test_counter"
}

test_zxfer_profile_now_ms_falls_back_to_second_resolution_when_millisecond_format_is_unavailable() {
	output=$(
		(
			date() {
				if [ "$1" = "+%s%3N" ]; then
					printf '%s\n' "not-supported"
				else
					printf '%s\n' "42"
				fi
			}
			zxfer_profile_now_ms
		)
	)
	status=$?

	assertEquals "Profile millisecond timestamps should still succeed when date lacks %N-style support." \
		0 "$status"
	assertEquals "Second-resolution fallbacks should be normalized into millisecond units." \
		42000 "$output"
}

test_zxfer_profile_add_elapsed_ms_accumulates_only_valid_positive_durations_in_current_shell() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_zxfer_profile_test_elapsed_ms=5

	zxfer_profile_add_elapsed_ms g_zxfer_profile_test_elapsed_ms 10 25
	zxfer_profile_add_elapsed_ms g_zxfer_profile_test_elapsed_ms bogus 30
	zxfer_profile_add_elapsed_ms g_zxfer_profile_test_elapsed_ms 40 35

	assertEquals "Elapsed stage timings should accumulate onto existing millisecond totals." \
		20 "$g_zxfer_profile_test_elapsed_ms"
	assertEquals "Elapsed stage timings should mark that profiling data exists." \
		1 "$g_zxfer_profile_has_data"
}

test_zxfer_append_failure_report_to_log_creates_secure_file() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure.log"
	ZXFER_ERROR_LOG="$log_path"
	report_contents=$(printf 'zxfer: failure report begin\nmessage: failed\nzxfer: failure report end\n')

	set +e
	zxfer_append_failure_report_to_log "$report_contents"
	status=$?
	if [ -f "$log_path" ]; then
		file_exists=1
	else
		file_exists=0
	fi
	perms=$(stat -c '%a' "$log_path" 2>/dev/null || stat -f '%Lp' "$log_path" 2>/dev/null)
	perms_status=$?
	grep -F "message: failed" "$log_path" >/dev/null 2>&1
	grep_status=$?

	assertEquals "ZXFER_ERROR_LOG appends should succeed for valid absolute paths." 0 "$status"
	assertEquals "Failure log should be created when ZXFER_ERROR_LOG is valid." 1 "$file_exists"
	assertEquals "Log file mode should be readable for assertions." 0 "$perms_status"
	assertEquals "ZXFER_ERROR_LOG files should be created with mode 600." "600" "$perms"
	assertEquals "Failure log should contain the rendered report payload." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_preserves_existing_contents() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure_append.log"
	ZXFER_ERROR_LOG="$log_path"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: appended-report"
	status=$?
	grep -F "existing: keep-me" "$log_path" >/dev/null 2>&1
	existing_status=$?
	grep -F "message: appended-report" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files should still accept appended reports." 0 "$status"
	assertEquals "Atomic ZXFER_ERROR_LOG appends should preserve prior log contents." 0 "$existing_status"
	assertEquals "Atomic ZXFER_ERROR_LOG appends should add the new report payload." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_relative_path() {
	stderr_file="$TEST_TMPDIR/error_log.stderr"
	ZXFER_ERROR_LOG="relative.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log.stdout" 2>"$stderr_file"
	status=$?
	grep -F "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	if [ -e "$TEST_TMPDIR/relative.log" ]; then
		file_exists=1
	else
		file_exists=0
	fi

	assertEquals "Relative ZXFER_ERROR_LOG paths should be rejected." 1 "$status"
	assertEquals "Relative ZXFER_ERROR_LOG rejection should emit a warning." 0 "$grep_status"
	assertEquals "Relative ZXFER_ERROR_LOG should not create a local file." 0 "$file_exists"
}

test_zxfer_append_failure_report_to_log_rejects_missing_parent_dir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	stderr_file="$TEST_TMPDIR/error_log_parent.stderr"
	ZXFER_ERROR_LOG="$physical_tmpdir/missing/subdir/failure.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_parent.stdout" 2>"$stderr_file"
	status=$?
	grep -F "parent directory \"$physical_tmpdir/missing/subdir\" does not exist" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Missing parent directories should be rejected for ZXFER_ERROR_LOG." 1 "$status"
	assertEquals "Missing parent directory rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_untrusted_parent_dir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_dir="$physical_tmpdir/untrusted_error_log_parent"
	stderr_file="$TEST_TMPDIR/error_log_untrusted_parent.stderr"
	mkdir -p "$log_dir"
	chmod 0777 "$log_dir"
	ZXFER_ERROR_LOG="$log_dir/failure.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_untrusted_parent.stdout" 2>"$stderr_file"
	status=$?
	grep -F "writable by others without sticky-bit protection" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	chmod 0700 "$log_dir"

	assertEquals "ZXFER_ERROR_LOG parents that are writable by others without sticky-bit protection should be rejected." 1 "$status"
	assertEquals "Untrusted ZXFER_ERROR_LOG parent rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_symlinked_parent_component() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/real_parent"
	link_dir="$physical_tmpdir/link_parent"
	log_path="$link_dir/failure.log"
	stderr_file="$TEST_TMPDIR/error_log_symlink.stderr"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_symlink.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path component \"$link_dir\" is a symlink" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	if [ -e "$real_dir/failure.log" ]; then
		file_exists=1
	else
		file_exists=0
	fi

	assertEquals "Symlinked parent components should be rejected for ZXFER_ERROR_LOG." 1 "$status"
	assertEquals "Symlinked parent component rejection should emit a warning." 0 "$grep_status"
	assertEquals "Symlinked parent component rejection should not create the target file." 0 "$file_exists"
}

test_zxfer_append_failure_report_to_log_rejects_symlink_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_path="$physical_tmpdir/real_failure.log"
	log_path="$physical_tmpdir/failure_link.log"
	stderr_file="$TEST_TMPDIR/error_log_target_symlink.stderr"
	: >"$real_path"
	chmod 600 "$real_path"
	ln -s "$real_path" "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_target_symlink.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path component \"$log_path\" is a symlink" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Symlinked ZXFER_ERROR_LOG targets should be rejected." 1 "$status"
	assertEquals "Symlinked target rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_non_regular_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure_dir"
	stderr_file="$TEST_TMPDIR/error_log_nonregular.stderr"
	mkdir -p "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_nonregular.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path \"$log_path\" because it is not a regular file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Non-regular ZXFER_ERROR_LOG targets should be rejected." 1 "$status"
	assertEquals "Non-regular target rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_existing_insecure_mode() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/insecure_mode.log"
	stderr_file="$TEST_TMPDIR/error_log_mode.stderr"
	: >"$log_path"
	chmod 644 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: should-not-append" >"$TEST_TMPDIR/error_log_mode.stdout" 2>"$stderr_file"
	status=$?
	grep -F "permissions (644) are not 0600" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing insecure ZXFER_ERROR_LOG files should be rejected." 1 "$status"
	assertEquals "Insecure mode rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected insecure log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_existing_insecure_owner() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/insecure_owner.log"
	stderr_file="$TEST_TMPDIR/error_log_owner.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1"
		}
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock_warn_only() {
			:
		}
		zxfer_get_path_owner_uid() { printf '%s\n' "1234"; }
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_owner.stdout" 2>"$stderr_file"
	status=$?
	grep -F "owned by UID 1234 instead of" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with insecure owners should be rejected." 1 "$status"
	assertEquals "Insecure owner rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected insecure-owner log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_owner() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_owner.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_owner.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1"
		}
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock_warn_only() {
			:
		}
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_owner.stdout" 2>"$stderr_file"
	status=$?
	grep -F "owner could not be determined" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with unknown owners should be rejected." 1 "$status"
	assertEquals "Unknown-owner rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected unknown-owner log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_mode() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_mode.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_mode.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock_warn_only() {
			:
		}
		zxfer_get_path_owner_uid() {
			printf '%s\n' "0"
		}
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_mode.stdout" 2>"$stderr_file"
	status=$?
	grep -F "permissions could not be determined" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with unknown modes should be rejected." 1 "$status"
	assertEquals "Unknown-mode rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected unknown-mode log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_warns_when_file_creation_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/create_failure.log"
	stderr_file="$TEST_TMPDIR/error_log_create_failure.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_create_error_log_file() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: create-failed"
	) >"$TEST_TMPDIR/error_log_create_failure.stdout" 2>"$stderr_file"
	status=$?
	grep -F "unable to create ZXFER_ERROR_LOG file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	stderr_contents=$(cat "$stderr_file" 2>/dev/null || true)

	assertEquals "ZXFER_ERROR_LOG creation failures should be reported without succeeding. status=$status stderr=$stderr_contents" 1 "$status"
	assertEquals "ZXFER_ERROR_LOG creation failures should emit a warning. status=$status stderr=$stderr_contents" 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_warns_when_chmod_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/chmod_failure.log"
	stderr_file="$TEST_TMPDIR/error_log_chmod_failure.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_chmod_error_log_file() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: chmod-failed"
	) >"$TEST_TMPDIR/error_log_chmod_failure.stdout" 2>"$stderr_file"
	status=$?
	grep -F "unable to chmod ZXFER_ERROR_LOG file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	stderr_contents=$(cat "$stderr_file" 2>/dev/null || true)

	assertEquals "ZXFER_ERROR_LOG chmod failures should be reported without succeeding. status=$status stderr=$stderr_contents" 1 "$status"
	assertEquals "ZXFER_ERROR_LOG chmod failures should emit a warning. status=$status stderr=$stderr_contents" 0 "$grep_status"
}

test_trap_exit_preserves_failure_status_when_error_log_warning_fails() {
	set +e
	output=$(
		(
			ZXFER_ERROR_LOG="relative.log"
			g_zxfer_failure_class="runtime"
			g_zxfer_failure_stage="unit"
			g_zxfer_failure_message="trap failure"
			g_delete_source_tmp_file=""
			g_delete_dest_tmp_file=""
			g_delete_snapshots_to_delete_tmp_file=""
			g_services_need_relaunch=0
			false
			zxfer_trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	assertEquals "Failure-report log warnings must not replace the original exit status." 1 "$status"
	assertContains "zxfer_trap_exit should still emit the report before warning about the log sink." "$output" "zxfer: failure report begin"
	assertContains "zxfer_trap_exit should warn when ZXFER_ERROR_LOG is invalid." \
		"$output" "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute"
}

test_zxfer_kill_registered_cleanup_pids_only_terminates_registered_pids() {
	output=$(
		(
			unrelated_pid=60101
			g_zxfer_cleanup_pids="50101"
			g_zxfer_cleanup_pid_records="50101	registered cleanup helper"
			g_test_cleanup_abort_calls=""
			zxfer_abort_cleanup_pid() {
				g_test_cleanup_abort_calls="${g_test_cleanup_abort_calls}${g_test_cleanup_abort_calls:+ }$1:$2"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'abort_calls=<%s>\n' "$g_test_cleanup_abort_calls"
			printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pids"
			printf 'unrelated=<%s>\n' "$unrelated_pid"
		)
	)

	assertContains "Cleanup should delegate validated teardown only for tracked helper PIDs." \
		"$output" "abort_calls=<50101:TERM>"
	assertNotContains "Cleanup should not delegate teardown for unrelated helper PIDs." \
		"$output" "60101:"
	assertContains "Cleanup PID tracking should be cleared after termination." \
		"$output" "remaining=<>"
}

test_zxfer_cleanup_pid_helpers_ignore_invalid_inputs_in_current_shell() {
	sleep 30 &
	tracked_pid=$!
	zxfer_register_cleanup_pid "$tracked_pid" "tracked cleanup helper"

	zxfer_register_cleanup_pid ""
	zxfer_register_cleanup_pid "abc"
	assertEquals "Cleanup PID registration should ignore empty and non-numeric inputs." \
		"$tracked_pid" "$g_zxfer_cleanup_pids"

	zxfer_unregister_cleanup_pid ""
	zxfer_unregister_cleanup_pid "abc"
	zxfer_unregister_cleanup_pid "$tracked_pid"
	assertEquals "Cleanup PID unregistration should ignore invalid inputs and preserve the remaining list order." \
		"" "$g_zxfer_cleanup_pids"

	output=$(
		(
			l_stub_pid=7001
			g_zxfer_cleanup_pids="abc $l_stub_pid $$"
			g_zxfer_cleanup_pid_records="$l_stub_pid	tracked cleanup helper"
			g_test_cleanup_abort_calls=""
			zxfer_abort_cleanup_pid() {
				g_test_cleanup_abort_calls="${g_test_cleanup_abort_calls}${g_test_cleanup_abort_calls:+ }$1:$2"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'abort_calls=<%s>\n' "$g_test_cleanup_abort_calls"
			printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pids"
		)
	)

	kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
	wait "$tracked_pid" 2>/dev/null || true
	assertContains "Cleanup termination should still delegate teardown for the validated helper when invalid entries are present in the PID list." \
		"$output" "abort_calls=<7001:TERM>"
	assertNotContains "Cleanup termination should ignore non-numeric entries in the tracked PID list." \
		"$output" "abc:"
	assertContains "Cleanup termination should remove the processed helper without treating malformed or self entries as signal targets." \
		"$output" "remaining=<abc $$>"
}

test_execute_command_records_last_command_string() {
	g_option_n_dryrun=1
	g_zxfer_failure_last_command=""

	zxfer_execute_rendered_shell_command "printf 'hello'"

	assertEquals "zxfer_execute_rendered_shell_command should redact the exact command string for failure reports by default." "[redacted]" "$g_zxfer_failure_last_command"
}

test_run_source_zfs_cmd_records_local_command_in_unsafe_mode() {
	g_option_O_origin_host=""
	g_cmd_zfs="/bin/echo"
	g_LZFS="$g_cmd_zfs"
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null

	assertEquals "Direct local ZFS commands should be shell-quoted in the last-command field when unsafe mode is enabled." \
		"'/bin/echo' 'list' '-H' 'tank/src'" "$g_zxfer_failure_last_command"
}

test_invoke_ssh_shell_command_for_host_records_remote_command_in_unsafe_mode() {
	FAKE_SSH_STDOUT_OVERRIDE="ok"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "zfs list -H tank/src" >/dev/null

	assertEquals "Unsafe SSH command recording should preserve every token boundary." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock' 'backup@example.com' 'zfs list -H tank/src'" \
		"$g_zxfer_failure_last_command"
}

test_zxfer_remote_command_context_helpers_cover_remaining_role_labels() {
	output=$(
		(
			g_option_O_origin_host="shared.example"
			g_option_T_target_host="shared.example"
			printf 'other=%s\n' "$(zxfer_get_remote_command_context_label "other.example" other)"
			printf 'shared=%s\n' "$(zxfer_get_remote_command_context_label "shared.example")"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			printf 'target=%s\n' "$(zxfer_get_remote_command_context_label "target.example")"
			g_option_V_very_verbose=1
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_echoV_remote_command_for_host "misc.example doas" other /bin/echo hello
		)
	)

	assertContains "Remote command context labels should render the explicit other profile side as remote." \
		"$output" "other=remote: other.example"
	assertEquals "Remote command context labels should fall back to a bare remote label when no host is provided." \
		"remote" "$(zxfer_get_remote_command_context_label "")"
	assertContains "Remote command context labels should render shared origin and target hosts as origin/target." \
		"$output" "shared=origin/target: shared.example"
	assertContains "Remote command context labels should infer the target role when only the target host matches." \
		"$output" "target=target: target.example"
	assertContains "Very-verbose remote command rendering should include the resolved remote context label." \
		"$output" "Running remote command [remote: misc.example doas]: '/bin/echo' 'hello'"
}

test_zxfer_echoV_remote_command_for_host_covers_current_shell_render_path() {
	trace_file="$TEST_TMPDIR/echoV_remote_command_current_shell.log"

	(
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example doas"
		g_option_V_very_verbose=1
		zxfer_echoV() {
			printf '%s\n' "$*" >"$trace_file"
		}
		zxfer_echoV_remote_command_for_host "target.example doas" "" /bin/echo current-shell
	)

	assertEquals "Very-verbose remote command rendering should keep the current-shell target-context path shell-quoted exactly once." \
		"Running remote command [target: target.example doas]: '/bin/echo' 'current-shell'" \
		"$(cat "$trace_file")"
}

test_zxfer_render_destination_zfs_command_uses_remote_target_tool_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="backup@example.com"
	g_option_T_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
	g_target_cmd_zfs="/remote/bin/zfs"

	rendered=$(zxfer_render_destination_zfs_command list -H backup/target)

	assertContains "Remote destination zfs rendering should route through ssh." \
		"$rendered" "'$FAKE_SSH_BIN'"
	assertContains "Remote destination zfs rendering should target the configured host." \
		"$rendered" "'backup@example.com'"
	assertContains "Remote destination zfs rendering should mention the resolved remote zfs path." \
		"$rendered" "/remote/bin/zfs"
	assertContains "Remote destination zfs rendering should preserve the requested subcommand and dataset." \
		"$rendered" "backup/target"
}

test_zxfer_render_zfs_command_for_spec_routes_destination_and_literal_commands() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="backup@example.com"
	g_option_T_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
	g_target_cmd_zfs="/remote/bin/zfs"
	g_LZFS="mock_source_spec"
	g_RZFS="mock_destination_spec"

	destination_rendered=$(zxfer_render_zfs_command_for_spec "$g_RZFS" list -H backup/target)
	literal_rendered=$(zxfer_render_zfs_command_for_spec "/bin/echo" hello world)

	assertContains "Destination command specs should reuse the destination render helper." \
		"$destination_rendered" "/remote/bin/zfs"
	assertContains "Destination command specs should preserve the requested dataset argument." \
		"$destination_rendered" "backup/target"
	assertEquals "Literal command specs should be rendered as direct shell-quoted argv." \
		"'/bin/echo' 'hello' 'world'" "$literal_rendered"
}

test_build_ssh_shell_command_for_host_quotes_control_socket_path_for_eval() {
	marker_rel="control_socket_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	log_file="$TEST_TMPDIR/control_socket_eval.log"
	socket_path="$TEST_TMPDIR/socket.\$(touch $marker_rel)"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	: >"$log_file"
	rm -f "$marker"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$socket_path"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	cmd=$(zxfer_build_ssh_shell_command_for_host "backup@example.com" "$safe_cmd")
	(
		cd "$TEST_TMPDIR" || exit 1
		zxfer_execute_rendered_shell_command "$cmd"
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertFalse "Control-socket paths should stay literal when ssh commands are eval-rendered." \
		"[ -e '$marker' ]"
	assertEquals "Rendered ssh commands should pass the control socket as a single argv token." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$socket_path
backup@example.com
'sh' '-c' 'printf ok >/dev/null'" "$(cat "$log_file")"
}

test_build_remote_sh_c_command_preserves_multiline_scripts_as_one_c_argument() {
	log_file="$TEST_TMPDIR/remote_sh_multiline.log"
	ssh_bin="$TEST_TMPDIR/fake_ssh_join_multiline"
	create_fake_ssh_join_exec_bin "$ssh_bin"
	: >"$log_file"
	g_cmd_ssh="$ssh_bin"
	g_option_O_origin_host="backup@example.com"
	FAKE_SSH_LOG="$log_file"
	export FAKE_SSH_LOG

	remote_cmd=$(zxfer_build_remote_sh_c_command "l_value=ok
printf '%s\n' \"\$l_value\"")
	output=$(zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "$remote_cmd")

	unset FAKE_SSH_LOG

	assertEquals "Remote sh -c builders should preserve multiline scripts as one command argument." \
		"ok" "$output"
	assertContains "Remote sh -c builders should still target the requested host." \
		"$(cat "$log_file")" "backup@example.com"
	assertContains "Remote sh -c builders should keep the entire multiline script inside the single -c payload." \
		"$(cat "$log_file")" "l_value=ok"
}

test_prepare_remote_shell_command_for_host_wraps_only_wrapper_hosts() {
	zxfer_prepare_remote_shell_command_for_host "backup@example.com" "zfs list tank/src"
	simple_status=$?
	simple_result=$g_zxfer_remote_shell_command_for_host_result

	zxfer_prepare_remote_shell_command_for_host "backup@example.com pfexec -p 2222" "zfs list tank/src"
	wrapped_status=$?
	wrapped_result=$g_zxfer_remote_shell_command_for_host_result

	assertEquals "Simple host specs should prepare without an extra remote shell wrapper." \
		0 "$simple_status"
	assertEquals "Simple host specs should preserve the original remote command." \
		"zfs list tank/src" "$simple_result"
	assertEquals "Wrapper host specs should prepare successfully." \
		0 "$wrapped_status"
	assertContains "Wrapper host specs should render a remote sh command." \
		"$wrapped_result" "'sh' '-c'"
	assertContains "Wrapper host specs should preserve the remote command inside the sh payload." \
		"$wrapped_result" "zfs list tank/src"
}

test_prepare_remote_shell_command_for_host_preserves_split_and_wrapper_failures() {
	output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid host spec"
				return 41
			}
			zxfer_prepare_remote_shell_command_for_host "bad host" "zfs list" >/dev/null
			printf 'split_status=%s\n' "$?"
			printf 'split_result=<%s>\n' "$g_zxfer_remote_shell_command_for_host_result"
		)
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "backup@example.com" "pfexec"
			}
			zxfer_build_remote_sh_c_command() {
				return 42
			}
			zxfer_prepare_remote_shell_command_for_host "backup@example.com pfexec" "zfs list" >/dev/null
			printf 'wrapper_status=%s\n' "$?"
		)
	)

	assertContains "Remote shell preparation should preserve host-token split failures." \
		"$output" "split_status=41"
	assertContains "Remote shell preparation should expose host-token split diagnostics to current-shell callers." \
		"$output" "split_result=<invalid host spec>"
	assertContains "Remote shell preparation should preserve remote sh builder failures." \
		"$output" "wrapper_status=42"
}

test_prepare_ssh_shell_command_context_extracts_host_and_wrapper_command() {
	zxfer_prepare_ssh_shell_command_context "backup@example.com pfexec -u root" "'sh' '-c' 'zfs list tank/src'"
	status=$?

	assertEquals "SSH shell context preparation should succeed for wrapper host specs." \
		0 "$status"
	assertEquals "SSH shell context preparation should publish the first host-spec token as the ssh host." \
		"backup@example.com" "$g_zxfer_ssh_shell_host_result"
	assertEquals "SSH shell context preparation should prefix the remote command with safely quoted wrapper tokens." \
		"'pfexec' '-u' 'root' 'sh' '-c' 'zfs list tank/src'" "$g_zxfer_ssh_shell_full_remote_command_result"
}

test_build_prepared_ssh_shell_command_for_host_centralizes_prepare_and_render() {
	output=$(
		(
			zxfer_prepare_remote_shell_command_for_host() {
				g_zxfer_remote_shell_command_for_host_result="'sh' '-c' '$2'"
				return 0
			}
			zxfer_build_ssh_shell_command_for_host() {
				printf 'host=<%s> cmd=<%s>' "$1" "$2"
			}
			zxfer_build_prepared_ssh_shell_command_for_host "backup@example.com pfexec" "zfs list tank/src"
			printf '\nresult=<%s>\n' "$g_zxfer_prepared_ssh_shell_command_result"
		)
	)

	assertContains "Prepared SSH shell rendering should pass the host spec to the final SSH renderer." \
		"$output" "host=<backup@example.com pfexec>"
	assertContains "Prepared SSH shell rendering should pass the prepared remote command to the final SSH renderer." \
		"$output" "cmd=<'sh' '-c' 'zfs list tank/src'>"
	assertContains "Prepared SSH shell rendering should publish the rendered shell command for current-shell callers." \
		"$output" "result=<host=<backup@example.com pfexec> cmd=<'sh' '-c' 'zfs list tank/src'>>"
}

test_ssh_shell_context_callers_preserve_empty_context_failures() {
	output=$(
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				g_zxfer_ssh_shell_context_error_result=""
				return 47
			}
			zxfer_build_ssh_shell_command_for_host "backup@example.com" "zfs list tank/src" >/dev/null
			printf 'build_status=%s\n' "$?"
		)
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				g_zxfer_ssh_shell_context_error_result=""
				return 48
			}
			zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "zfs list tank/src" source >/dev/null
			printf 'invoke_status=%s\n' "$?"
		)
	)

	assertContains "SSH shell rendering should preserve context-preparation failures even without diagnostics." \
		"$output" "build_status=47"
	assertContains "SSH shell invocation should preserve context-preparation failures even without diagnostics." \
		"$output" "invoke_status=48"
}

test_build_prepared_ssh_shell_command_for_host_preserves_render_diagnostics() {
	output=$(
		(
			zxfer_prepare_remote_shell_command_for_host() {
				g_zxfer_remote_shell_command_for_host_result="'sh' '-c' '$2'"
				return 0
			}
			zxfer_build_ssh_shell_command_for_host() {
				printf '%s\n' "render diagnostic"
				return 43
			}
			zxfer_build_prepared_ssh_shell_command_for_host "backup@example.com" "zfs list tank/src" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'error=<%s>\n' "$g_zxfer_prepared_ssh_shell_command_error_result"
		)
	)

	assertContains "Prepared SSH shell rendering should preserve final renderer status." \
		"$output" "status=43"
	assertContains "Prepared SSH shell rendering should publish final renderer diagnostics for callers that rethrow outside command substitutions." \
		"$output" "error=<render diagnostic>"
}

test_build_ssh_shell_command_for_host_honors_explicit_ambient_policy_opt_out() {
	log_file="$TEST_TMPDIR/build_shell_ambient.log"
	socket_path="$TEST_TMPDIR/ambient.sock"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	: >"$log_file"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$socket_path"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	cmd=$(zxfer_build_ssh_shell_command_for_host "backup@example.com" "$safe_cmd")
	zxfer_execute_rendered_shell_command "$cmd"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Ambient-policy opt-out should suppress managed ssh -o options in shell-command rendering while preserving control-socket reuse." \
		"-S
$socket_path
backup@example.com
'sh' '-c' 'printf ok >/dev/null'" "$(cat "$log_file")"
}

test_build_ssh_shell_command_for_host_fuzzes_wrapper_specs_and_control_socket_paths() {
	marker_rel="control_socket_fuzz_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	case_file="$TEST_TMPDIR/control_socket_fuzz_cases.txt"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	cat >"$case_file" <<EOF
backup@example.com doas|$TEST_TMPDIR/socket,comma
backup@example.com pfexec -u root|$TEST_TMPDIR/socket=equals
backup@example.com env LC_ALL=C doas|$TEST_TMPDIR/socket:semicolon;literal
backup@example.com doas|$TEST_TMPDIR/socket.\$(touch $marker_rel)
EOF

	case_index=0
	while IFS='|' read -r host_spec socket_path || [ -n "$host_spec$socket_path" ]; do
		[ -n "$host_spec" ] || continue
		case_index=$((case_index + 1))
		log_file="$TEST_TMPDIR/control_socket_fuzz_$case_index.log"
		: >"$log_file"
		rm -f "$marker"
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host=$host_spec
		g_ssh_origin_control_socket=$socket_path
		FAKE_SSH_LOG="$log_file"
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		cmd=$(zxfer_build_ssh_shell_command_for_host "$host_spec" "$safe_cmd")
		(
			cd "$TEST_TMPDIR" || exit 1
			zxfer_execute_rendered_shell_command "$cmd"
		)

		unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		assertFalse "Control-socket fuzz case $case_index should not execute command substitutions from the socket path." \
			"[ -e '$marker' ]"
		assertEquals "Control-socket fuzz case $case_index should force batch mode first." "-o" "$(sed -n '1p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass BatchMode=yes as the first managed transport option." "BatchMode=yes" "$(sed -n '2p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should force strict host-key checking next." "-o" "$(sed -n '3p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass StrictHostKeyChecking=yes as the second managed transport option." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass -S separately." "-S" "$(sed -n '5p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should preserve the literal control-socket path." \
			"$socket_path" "$(sed -n '6p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should keep the ssh host token separate from wrappers." \
			"backup@example.com" "$(sed -n '7p' "$log_file")"
		log_line_remote_cmd=$(sed -n '8p' "$log_file")
		assertContains "Control-socket fuzz case $case_index should preserve the quoted remote command payload." \
			"$log_line_remote_cmd" "'sh' '-c' 'printf ok >/dev/null'"

		case "$host_spec" in
		*" doas"*)
			assertContains "Control-socket fuzz case $case_index should keep doas in the remote wrapper chain." \
				"$log_line_remote_cmd" "'doas'"
			;;
		esac
		case "$host_spec" in
		*"pfexec -u root"*)
			assertContains "Control-socket fuzz case $case_index should keep pfexec wrapper tokens quoted." \
				"$log_line_remote_cmd" "'pfexec' '-u' 'root'"
			;;
		esac
		case "$host_spec" in
		*"LC_ALL=C doas"*)
			assertContains "Control-socket fuzz case $case_index should keep env-style wrapper tokens quoted." \
				"$log_line_remote_cmd" "'env' 'LC_ALL=C' 'doas'"
			;;
		esac
	done <"$case_file"
}

test_read_remote_backup_file_rejects_insecure_remote_owner() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() { return 95; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort on insecure remote ownership." 1 "$status"
	assertContains "Insecure remote ownership should use the documented error." \
		"$output" "Refusing to use backup metadata /tmp/backup.meta on backup@example.com because it is not owned by root or the ssh user."
}

test_read_remote_backup_file_rejects_insecure_remote_mode() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() { return 96; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort on insecure remote permissions." 1 "$status"
	assertContains "Insecure remote permissions should use the documented error." \
		"$output" "Refusing to use backup metadata /tmp/backup.meta on backup@example.com because its permissions are not 0600."
}

test_read_remote_backup_file_rejects_unknown_remote_security_metadata() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() { return 97; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort when remote ownership or mode cannot be determined." 1 "$status"
	assertContains "Unknown remote security metadata should use the documented error." \
		"$output" "Cannot determine ownership or permissions for backup metadata /tmp/backup.meta on backup@example.com."
}

test_read_remote_backup_file_allows_trusted_absolute_root_symlink_components() {
	backup_file=$(mktemp /tmp/read_remote_trusted.XXXXXX)
	outfile="$TEST_TMPDIR/read_remote_trusted.out"
	printf '%s\n' "backup-data" >"$backup_file"
	chmod 600 "$backup_file"
	g_cmd_cat="/bin/cat"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
	) >"$outfile"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup reads, which keeps default /var- or /tmp-backed remote roots working on macOS." 0 "$status"
	assertEquals "Trusted absolute symlink components should still allow the secure metadata contents to be read." \
		"backup-data" "$(cat "$outfile")"

	rm -f "$backup_file"
}

test_throw_error_with_usage_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/zxfer_throw_error_with_usage.stdout"
	stderr_file="$TEST_TMPDIR/zxfer_throw_error_with_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error_with_usage "boom with usage" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_error_with_usage should preserve the requested exit status." 3 "$status"
	assertEquals "zxfer_throw_error_with_usage should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: boom with usage"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}

test_get_os_handles_local_and_remote_invocations() {
	local_result=$(zxfer_get_os "")
	if remote_result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
			export FAKE_SSH_STDOUT_OVERRIDE
			zxfer_get_os "backup@example.com pfexec"
		)
	); then
		remote_status=0
	else
		remote_status=$?
	fi
	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Local OS detection should match uname output." "$(uname)" "$local_result"
	assertEquals "Remote OS detection should succeed through the ssh helper path." 0 "$remote_status"
	assertEquals "Remote OS detection should execute uname through the ssh helper path." "RemoteOS" "$remote_result"
}

test_get_os_fails_when_remote_helper_is_unavailable() {
	set +e
	result=$(
		(
			unset -f zxfer_get_remote_host_operating_system 2>/dev/null || :
			zxfer_get_os "backup@example.com" 2>/dev/null
		)
	)
	status=$?

	assertEquals "Remote OS detection should preserve missing-helper status." 127 "$status"
	assertEquals "Failed remote OS detection should not print a payload." "" "$result"
}

test_get_os_treats_local_ssh_path_as_literal() {
	marker="$TEST_TMPDIR/get_os_ssh_marker"
	old_cmd_ssh=${g_cmd_ssh:-}
	g_cmd_ssh="/bin/echo; touch $marker #"

	if zxfer_get_os "backup@example.com" >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi
	g_cmd_ssh=$old_cmd_ssh

	: "$status"
	assertFalse "Local ssh helper paths should not execute shell metacharacters during OS detection." \
		"[ -e '$marker' ]"
}

test_execute_command_continue_on_fail_reports_noncritical_error() {
	g_option_n_dryrun=0

	output=$(zxfer_execute_rendered_shell_command "false" 1)
	status=$?

	assertEquals "Continue-on-fail commands should not abort the caller." 0 "$status"
	assertContains "Continue-on-fail commands should report the non-critical failure." \
		"$output" "Non-critical error when executing command. Continuing."
}

test_zxfer_get_path_parent_dir_handles_root_and_relative_inputs() {
	assertEquals "Absolute paths should return their containing directory." \
		"/var/log" "$(zxfer_get_path_parent_dir "/var/log/zxfer.log")"
	assertEquals "Paths without a slash should fall back to root for parent-dir validation." \
		"/" "$(zxfer_get_path_parent_dir "zxfer.log")"
}

test_run_source_zfs_cmd_uses_local_wrapper_command_when_configured() {
	wrapper="$TEST_TMPDIR/local_source_wrapper"
	cat >"$wrapper" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$wrapper"
	result=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			g_option_O_origin_host=""
			g_cmd_zfs="/sbin/zfs"
			g_LZFS="$wrapper"
			zxfer_run_source_zfs_cmd list -H tank/src
			printf 'last=%s\n' "$g_zxfer_failure_last_command"
		)
	)

	assertContains "Local source wrappers should execute directly when configured." \
		"$result" "list -H tank/src"
	assertContains "Local source wrappers should be recorded in the last-command field." \
		"$result" "last='$wrapper' 'list' '-H' 'tank/src'"
}

test_run_destination_zfs_cmd_uses_local_wrapper_command_when_configured() {
	wrapper="$TEST_TMPDIR/local_dest_wrapper"
	cat >"$wrapper" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$wrapper"
	result=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_RZFS="$wrapper"
			zxfer_run_destination_zfs_cmd get name tank/dst
			printf 'last=%s\n' "$g_zxfer_failure_last_command"
		)
	)

	assertContains "Local destination wrappers should execute directly when configured." \
		"$result" "get name tank/dst"
	assertContains "Local destination wrappers should be recorded in the last-command field." \
		"$result" "last='$wrapper' 'get' 'name' 'tank/dst'"
}

test_strip_trailing_slashes_preserves_all_slash_input_in_current_shell() {
	outfile="$TEST_TMPDIR/all_slash_path.out"

	zxfer_strip_trailing_slashes "///" >"$outfile"

	assertEquals "Inputs made only of slash characters should remain unchanged." "///" "$(cat "$outfile")"
}

test_zxfer_find_symlink_path_component_detects_nested_symlink() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/real_dir"
	link_dir="$physical_tmpdir/link_dir"
	mkdir -p "$real_dir/subdir"
	ln -s "$real_dir" "$link_dir"

	result=$(zxfer_find_symlink_path_component "$link_dir/subdir/file")
	status=$?

	assertEquals "Nested symlink detection should succeed when any path component is a symlink." 0 "$status"
	assertEquals "Nested symlink detection should return the offending path component." "$link_dir" "$result"
}

test_zxfer_find_symlink_path_component_detects_relative_symlink() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/relative_real_dir"
	link_dir="$physical_tmpdir/relative_link_dir"
	mkdir -p "$real_dir/subdir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	result=$(zxfer_find_symlink_path_component "./relative_link_dir/subdir/file")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative paths should be scanned for nested symlink components." 0 "$status"
	assertEquals "Relative symlink checks should return the offending relative path component." "./relative_link_dir" "$result"
}

test_zxfer_find_symlink_path_component_ignores_trusted_absolute_root_symlink() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	result=$(zxfer_find_symlink_path_component "$trusted_root_symlink/zxfer-trusted-root-symlink-probe/subdir/file")
	status=$?

	assertEquals "Trusted top-level system symlink components should be ignored regardless of platform-specific root layout." 1 "$status"
	assertEquals "Trusted absolute symlink components should not be reported as unsafe." "" "$result"
}

test_zxfer_is_trusted_symlink_path_component_accepts_known_root_symlink() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Known trusted root-level symlinks should be accepted by the trust check when the current host exposes one." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Trusted root-symlink checks should stay silent on success." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_owner_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the symlink owner lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Owner-lookup failures should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_owner_lookup_failures_for_absolute_nonroot_symlinks() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_owner_lookup_failure"
	symlink_target="$symlink_parent/target"
	symlink_path="$symlink_parent/link"
	mkdir -p "$symlink_target"
	ln -sf "$symlink_target" "$symlink_path"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$symlink_path\") return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$symlink_path\"
	"

	assertEquals "Trusted-symlink checks should fail closed when the symlink owner lookup fails for absolute non-root symlinks." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Absolute non-root symlink owner-lookup failures should not emit a trusted result payload." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_parent_owner_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$trusted_root_symlink\") printf '%s\n' '0' ;;
			/) return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the root-parent owner lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Parent-owner lookup failures should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_parent_owner_lookup_failures_for_absolute_nonroot_symlinks() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_parent_lookup_failure"
	symlink_target="$symlink_parent/target"
	symlink_path="$symlink_parent/link"
	mkdir -p "$symlink_target"
	ln -sf "$symlink_target" "$symlink_path"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$symlink_path\") printf '%s\n' '0' ;;
			\"$symlink_parent\") return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$symlink_path\"
	"

	assertEquals "Trusted-symlink checks should fail closed when the parent owner lookup fails for absolute non-root symlinks." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Absolute non-root parent-owner lookup failures should not emit a trusted result payload." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_ls_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			return 1
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the root permission lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed root-permission lookups should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_unparseable_root_permissions() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'bad-perms'
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should reject malformed root permission strings." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Malformed root-permission strings should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_world_writable_root_without_sticky_bit() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'drwxrwxrwx 1 0 0 0 Jan 1 00:00 /'
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should reject world-writable root parents without a sticky bit." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Untrusted root-permission layouts should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_find_symlink_path_component_returns_empty_for_relative_non_symlink_path() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/relative_plain_dir/subdir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	result=$(zxfer_find_symlink_path_component "./relative_plain_dir/subdir/file")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative paths without symlink components should still return failure." 1 "$status"
	assertEquals "Relative non-symlink checks should not report a component." "" "$result"
}

test_zxfer_require_backup_metadata_path_without_symlinks_rejects_symlink_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_file="$physical_tmpdir/backup.meta.real"
	link_file="$physical_tmpdir/backup.meta.link"
	: >"$real_file"
	ln -s "$real_file" "$link_file"

	zxfer_test_capture_subshell "
		zxfer_require_backup_metadata_path_without_symlinks \"$link_file\"
	"

	assertEquals "Exact backup metadata symlink paths should be rejected." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Exact backup metadata symlink rejections should identify the symlink itself." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Refusing to use backup metadata $link_file because it is a symlink."
}

test_zxfer_get_path_mode_octal_returns_failure_when_ls_fallback_cannot_map_permissions() {
	zxfer_test_capture_subshell "
		cd \"$TEST_TMPDIR\" || exit 1
		: >\"mode_unknown\"
		stat() {
			return 1
		}
		ls() {
			printf '%s\n' '-rw-r----- 1 0 0 0 Jan 1 00:00 ./mode_unknown'
		}
		zxfer_get_path_mode_octal \"mode_unknown\"
	"

	assertEquals "Mode lookups should fail when the ls fallback cannot map permissions to an octal value." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed ls-mode fallbacks should not emit a value." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_try_get_socket_cache_tmpdir_normalizes_dot_segment_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	socket_tmp="$physical_tmpdir/socket_cache_tmpdir"
	dot_tmp="$socket_tmp/./"
	mkdir -p "$socket_tmp"
	TMPDIR="$dot_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_socket_cache_tmpdir)
	status=$?

	assertEquals "Socket-cache tempdir selection should still succeed when TMPDIR includes dot segments." \
		0 "$status"
	assertEquals "Dot-segment TMPDIR values should resolve to the physical directory instead of preserving the raw dotted path." \
		"$socket_tmp" "$result"

	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_try_get_effective_tmpdir_resolves_symlinked_tmpdir_to_physical_path() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/effective_tmp_real"
	link_tmp="$physical_tmpdir/effective_tmp_link"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_effective_tmpdir)
	status=$?

	assertEquals "Symlinked TMPDIR values should still resolve successfully when their physical target is trusted." 0 "$status"
	assertEquals "Effective TMPDIR resolution should return the physical directory path." "$real_tmp" "$result"
	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_try_get_effective_tmpdir_prefers_memory_backed_default_candidates_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	ram_tmp="$physical_tmpdir/default_tmp_ram"
	disk_tmp="$physical_tmpdir/default_tmp_disk"
	mkdir -p "$ram_tmp" "$disk_tmp"
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			output_file="$TEST_TMPDIR/effective_tmp_default_current_shell.out"

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "$disk_tmp"
			}

			zxfer_try_get_effective_tmpdir >"$output_file" || exit $?
			result=$(cat "$output_file")
			printf 'result=%s\n' "$result"
			printf 'request=%s\n' "$g_zxfer_effective_tmpdir_requested"
		)
	)
	status=$?

	assertEquals "Unset TMPDIR should prefer the first validated default temp-root candidate, which lets zxfer prefer memory-backed roots when available." \
		0 "$status"
	assertContains "Unset TMPDIR should resolve to the preferred memory-backed default candidate." \
		"$output" "result=$ram_tmp"
	assertContains "Default-tempdir selections should cache under the synthetic default request key." \
		"$output" "request=__ZXFER_DEFAULT_TMPDIR__"
}

test_zxfer_try_get_effective_tmpdir_prefers_explicit_tmpdir_over_default_candidates_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	explicit_tmp="$physical_tmpdir/effective_tmp_explicit"
	ram_tmp="$physical_tmpdir/effective_tmp_default_ram"
	mkdir -p "$explicit_tmp" "$ram_tmp"
	output=$(
		(
			TMPDIR="$explicit_tmp"
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			output_file="$TEST_TMPDIR/effective_tmp_explicit_current_shell.out"

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "/tmp"
			}

			zxfer_try_get_effective_tmpdir >"$output_file" || exit $?
			result=$(cat "$output_file")
			printf 'result=%s\n' "$result"
			printf 'request=%s\n' "$g_zxfer_effective_tmpdir_requested"
		)
	)
	status=$?

	assertEquals "A valid explicit TMPDIR should still win over the default memory-backed candidate list." \
		0 "$status"
	assertContains "A valid explicit TMPDIR should remain the effective temp root." \
		"$output" "result=$explicit_tmp"
	assertContains "The cache key should still reflect the explicit TMPDIR request." \
		"$output" "request=$explicit_tmp"
}

test_zxfer_try_get_effective_tmpdir_falls_back_to_preferred_default_candidate_when_tmpdir_is_unsafe() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure_preferred"
	ram_tmp="$physical_tmpdir/effective_tmp_fallback_ram"
	disk_tmp="$physical_tmpdir/effective_tmp_fallback_disk"
	mkdir -p "$insecure_tmp" "$ram_tmp" "$disk_tmp"
	chmod 0777 "$insecure_tmp"
	output=$(
		(
			TMPDIR="$insecure_tmp"
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "$disk_tmp"
			}

			result=$(zxfer_try_get_effective_tmpdir) || exit $?
			printf 'result=%s\n' "$result"
		)
	)
	status=$?
	chmod 0700 "$insecure_tmp"

	assertEquals "Unsafe TMPDIR values should still resolve cleanly by falling back to the preferred validated default temp root." \
		0 "$status"
	assertContains "Unsafe TMPDIR values should fall back to the preferred validated default candidate before disk-backed fallbacks." \
		"$output" "result=$ram_tmp"
}

test_zxfer_unsafe_tmpdir_fallback_note_is_held_until_option_parsing_emits_it() {
	# The eager run temp root decides TMPDIR safety in zxfer_init_globals,
	# BEFORE -V parsing; the advisory must be held and replayed once option
	# parsing knows the verbosity state instead of being silently dropped.
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_note_insecure"
	safe_tmp="$physical_tmpdir/effective_tmp_note_safe"
	mkdir -p "$insecure_tmp" "$safe_tmp"
	chmod 0777 "$insecure_tmp"
	pre_parse_stderr="$TEST_TMPDIR/tmpdir_note_pre_parse.stderr"
	post_parse_stderr="$TEST_TMPDIR/tmpdir_note_post_parse.stderr"
	immediate_stderr="$TEST_TMPDIR/tmpdir_note_immediate.stderr"
	(
		TMPDIR="$insecure_tmp"
		g_option_V_very_verbose=0
		g_zxfer_effective_tmpdir=""
		g_zxfer_effective_tmpdir_requested=""
		g_zxfer_tmpdir_fallback_note=""
		zxfer_list_default_tmpdir_candidates() {
			printf '%s\n' "$safe_tmp"
		}
		zxfer_try_get_effective_tmpdir >/dev/null 2>"$pre_parse_stderr" || exit $?
		g_option_V_very_verbose=1
		zxfer_emit_pending_tmpdir_fallback_note 2>"$post_parse_stderr"
		# A second replay must stay silent: the note is consumed on emission.
		zxfer_emit_pending_tmpdir_fallback_note 2>>"$post_parse_stderr"
	)
	held_status=$?
	(
		TMPDIR="$insecure_tmp"
		g_option_V_very_verbose=1
		g_zxfer_effective_tmpdir=""
		g_zxfer_effective_tmpdir_requested=""
		g_zxfer_tmpdir_fallback_note=""
		zxfer_list_default_tmpdir_candidates() {
			printf '%s\n' "$safe_tmp"
		}
		zxfer_try_get_effective_tmpdir >/dev/null 2>"$immediate_stderr" || exit $?
	)
	immediate_status=$?
	chmod 0700 "$insecure_tmp"

	assertEquals "The held-advisory fallback path should still resolve the temp root cleanly." \
		0 "$held_status"
	assertEquals "No advisory should print while -V state is still unknown." \
		"" "$(cat "$pre_parse_stderr")"
	assertEquals "The held advisory should replay exactly once under -V after option parsing." \
		"Ignoring unsafe TMPDIR $insecure_tmp; using $safe_tmp instead." \
		"$(cat "$post_parse_stderr")"
	assertEquals "The immediate-advisory fallback path should still resolve the temp root cleanly." \
		0 "$immediate_status"
	assertEquals "The advisory should print at decision time when -V is already live." \
		"Ignoring unsafe TMPDIR $insecure_tmp; using $safe_tmp instead." \
		"$(cat "$immediate_stderr")"
}

test_zxfer_try_get_effective_tmpdir_rejects_non_sticky_world_writable_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure"
	mkdir -p "$insecure_tmp"
	chmod 0777 "$insecure_tmp"
	TMPDIR="$insecure_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_effective_tmpdir)
	status=$?

	assertEquals "Unsafe world-writable TMPDIR values should still resolve by falling back to the system temp root." 0 "$status"
	assertNotEquals "Unsafe world-writable TMPDIR values should not remain selected." "$insecure_tmp" "$result"

	chmod 0700 "$insecure_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_validate_temp_root_candidate_returns_failure_when_ls_lookup_fails() {
	candidate="$TEST_TMPDIR/validate_tmp_root_ls_failure"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			return 1
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should fail closed when directory permission lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed temp-root validation should not emit a physical directory path." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_nonroot_owned_dir_when_effective_uid_lookup_fails() {
	candidate="$TEST_TMPDIR/validate_tmp_root_effective_uid_failure"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '1234'
		}
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should fail closed when a non-root directory cannot be matched to the effective uid." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed effective-uid validation should not emit a physical directory path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_non_sticky_world_writable_dir_directly() {
	candidate="$TEST_TMPDIR/validate_tmp_root_insecure_mode"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'drwxrwxrwx 1 0 0 0 Jan 1 00:00 $candidate'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should reject world-writable directories without a sticky bit." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Rejected insecure temp-root candidates should not emit a physical directory path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_relative_physical_pwd_output() {
	candidate="$TEST_TMPDIR/validate_tmp_root_relative_pwd"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		pwd() {
			printf '%s\n' 'relative-path'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should reject non-absolute physical-directory results." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Rejected relative physical-directory results should not emit a temp-root path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_create_secure_staging_dir_for_path_returns_failure_when_parent_lookup_fails() {
	stage_path="$TEST_TMPDIR/create_secure_staging_parent_lookup/backup.meta"

	zxfer_test_capture_subshell "
		zxfer_get_path_parent_dir() {
			return 1
		}
		zxfer_create_secure_staging_dir_for_path \"$stage_path\" >/dev/null
	"

	assertEquals "Secure same-directory staging should fail closed when the parent-path lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_create_secure_staging_dir_for_path_returns_failure_when_parent_validation_fails() {
	stage_root="$TEST_TMPDIR/create_secure_staging_parent_validation"
	stage_path="$stage_root/backup.meta"
	mkdir -p "$stage_root"

	zxfer_test_capture_subshell "
		zxfer_validate_temp_root_candidate() {
			return 1
		}
		zxfer_create_secure_staging_dir_for_path \"$stage_path\" >/dev/null
	"

	assertEquals "Secure same-directory staging should fail closed when the parent directory is not a trusted temp-root candidate." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_create_secure_staging_dir_for_path_uses_unpredictable_mktemp_names() {
	# Staging parents may be shared sticky directories, so the staged name
	# must be mktemp-randomized: predictable pid+attempt slots are squat-able
	# by a local process-table reader.
	stage_root=$(cd -P "$TEST_TMPDIR" && pwd)/create_secure_staging_random
	stage_path="$stage_root/backup.meta"
	mkdir -p "$stage_root"

	zxfer_create_secure_staging_dir_for_path "$stage_path" >/dev/null
	stage_status=$?
	stage_dir=$g_zxfer_secure_staging_dir_result
	zxfer_create_secure_staging_dir_for_path "$stage_path" >/dev/null
	second_stage_dir=$g_zxfer_secure_staging_dir_result

	case "${stage_dir##*/}" in
	".zxfer.stage.$$."*)
		stage_name_randomized=no
		;;
	.zxfer.stage.??????)
		stage_name_randomized=yes
		;;
	*)
		stage_name_randomized=no
		;;
	esac

	assertEquals "Secure same-directory staging should succeed under a validated parent." \
		0 "$stage_status"
	assertEquals "Secure same-directory staging should use the randomized mktemp template, not pid+attempt slots." \
		yes "$stage_name_randomized"
	assertTrue "Secure same-directory staging should create the staged directory." \
		"[ -d \"$stage_dir\" ]"
	assertNotEquals "Consecutive staging directories should never reuse a name." \
		"$stage_dir" "$second_stage_dir"
}

test_zxfer_try_get_effective_tmpdir_reuses_cached_value_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	cached_tmp="$physical_tmpdir/effective_tmp_cached"
	mkdir -p "$cached_tmp"
	TMPDIR="$cached_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	first_out="$TEST_TMPDIR/effective_tmp_first.out"
	second_out="$TEST_TMPDIR/effective_tmp_second.out"
	zxfer_try_get_effective_tmpdir >"$first_out"
	first_status=$?
	zxfer_try_get_effective_tmpdir >"$second_out"
	second_status=$?

	assertEquals "The first effective TMPDIR lookup should succeed for a trusted directory." \
		0 "$first_status"
	assertEquals "Repeated effective TMPDIR lookups should reuse the cached value." \
		0 "$second_status"
	assertEquals "The first lookup should return the trusted TMPDIR path." \
		"$cached_tmp" "$(cat "$first_out")"
	assertEquals "The cached lookup should return the same TMPDIR path." \
		"$cached_tmp" "$(cat "$second_out")"
	assertEquals "The cached TMPDIR path should remain stored in the current shell." \
		"$cached_tmp" "$g_zxfer_effective_tmpdir"
	assertEquals "The cached TMPDIR request key should remain stored in the current shell." \
		"$cached_tmp" "$g_zxfer_effective_tmpdir_requested"

	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_create_private_temp_dir_returns_failure_when_effective_tmpdir_lookup_fails_in_current_shell() {
	zxfer_try_get_effective_tmpdir() {
		return 1
	}

	zxfer_create_private_temp_dir "zxfer_private_tmp" >"$TEST_TMPDIR/private_temp_dir.out" 2>/dev/null
	status=$?

	unset -f zxfer_try_get_effective_tmpdir

	assertEquals "Private temp directory creation should fail when the effective temp root cannot be determined." \
		1 "$status"
}

test_zxfer_render_ssh_transport_policy_identity_covers_ambient_and_invalid_options() {
	output=$(
		(
			set +e
			ZXFER_SSH_USE_AMBIENT_CONFIG=1
			identity=$(zxfer_render_ssh_transport_policy_identity)
			printf 'ambient_status=%s\n' "$?"
			printf 'ambient=%s\n' "$identity"
		)
		(
			set +e
			ZXFER_SSH_BATCH_MODE="bad
value"
			zxfer_render_ssh_transport_policy_identity >/dev/null
			printf 'invalid_status=%s\n' "$?"
			invalid_message=$(zxfer_render_ssh_transport_policy_identity)
			printf 'invalid_message=%s\n' "$invalid_message"
		)
	)

	assertContains "Ambient ssh policy identity rendering should succeed." \
		"$output" "ambient_status=0"
	assertContains "Ambient ssh policy identity should render as the ambient marker." \
		"$output" "ambient=ambient"
	assertContains "Invalid managed ssh options should fail policy identity rendering closed." \
		"$output" "invalid_status=1"
	assertContains "Invalid managed ssh options should surface the validation diagnostic." \
		"$output" "invalid_message=ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_get_ssh_transport_tokens_for_host_serves_warm_target_memo() {
	output=$(
		(
			set +e
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_ssh_origin_control_socket=""
			g_ssh_target_control_socket=""
			zxfer_refresh_ssh_transport_tokens_for_role target
			printf 'target_set=%s\n' "${g_zxfer_ssh_transport_tokens_target_set:-0}"
			rendered=$(zxfer_render_ssh_transport_tokens_for_host "target.example")
			memo=$(zxfer_get_ssh_transport_tokens_for_host "target.example")
			if [ "$memo" = "$rendered" ]; then
				printf 'memo_matches_render=yes\n'
			else
				printf 'memo_matches_render=no\n'
			fi
			zxfer_render_ssh_transport_tokens_for_host() {
				printf 'fresh-after-socket-change\n'
			}
			g_ssh_target_control_socket="$TEST_TMPDIR/target-memo.sock"
			stale=$(zxfer_get_ssh_transport_tokens_for_host "target.example")
			printf 'stale=%s\n' "$stale"
		)
	)

	assertContains "Refreshing the target role should warm the target transport memo." \
		"$output" "target_set=1"
	assertContains "A warm target memo must replay the rendered transport tokens byte for byte." \
		"$output" "memo_matches_render=yes"
	assertContains "A target control-socket change must bypass the stale target memo." \
		"$output" "stale=fresh-after-socket-change"
}
