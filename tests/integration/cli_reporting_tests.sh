#!/bin/sh
#
# Integration tests for CLI validation, structured failures, diagnostics, and error-log behavior.
# Sourced by tests/run_integration_zxfer.sh; the registry owns execution order.

usage_error_tests() {
	log "Starting usage error tests"

	assert_usage_error_case "Missing destination" "Need a destination." -R tank/src
	assert_usage_error_case "Missing -N/-R source flag" "You must specify a source with either -N or -R." backup/target
	assert_usage_error_case "Conflicting -N and -R flags" \
		"You must choose either -N to transfer a single filesystem or -R to transfer a single filesystem and its children recursively, but not both -N and -R at the same time." \
		-N tank/src -R tank/src backup/target

	log "Usage error tests passed"
}

usage_error_failure_report_test() {
	log "Starting usage error failure report test"

	stdout_log="$WORKDIR/usage_failure.stdout"
	stderr_log="$WORKDIR/usage_failure.stderr"
	safe_rm_f "$stdout_log" "$stderr_log"

	set +e
	"$ZXFER_BIN" -R tank/src >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage failure report test expected exit status 2, got $status. See $stderr_log."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Usage failure report should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -q "^zxfer: failure report begin$" "$stderr_log"; then
		fail "Usage failure report block missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_class: usage" "$stderr_log"; then
		fail "Usage failure report class missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "message: Need a destination\\." "$stderr_log"; then
		fail "Usage failure report message missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "^invocation: \[redacted\]$" "$stderr_log"; then
		fail "Usage failure report should redact the invocation by default. Output: $(cat "$stderr_log")"
	fi
	if grep -F "tank/src" "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report should not leak the source argument by default. Output: $(cat "$stderr_log")"
	fi

	safe_rm_f "$stdout_log" "$stderr_log"

	log "Usage error failure report test passed"
}

usage_error_failure_report_unsafe_commands_test() {
	log "Starting usage error failure report unsafe command test"

	secret_source="tank/secret-source"
	stdout_log="$WORKDIR/usage_failure_unsafe.stdout"
	stderr_log="$WORKDIR/usage_failure_unsafe.stderr"
	safe_rm_f "$stdout_log" "$stderr_log"

	set +e
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 "$ZXFER_BIN" -R "$secret_source" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage failure report unsafe command test expected exit status 2, got $status. See $stderr_log."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Usage failure report unsafe command test should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -Eq "invocation: .*'tank/secret-source'" "$stderr_log"; then
		fail "Usage failure report unsafe command test should preserve the secret-bearing source argument when explicitly enabled. Output: $(cat "$stderr_log")"
	fi

	safe_rm_f "$stdout_log" "$stderr_log"

	log "Usage error failure report unsafe command test passed"
}

usage_error_failure_report_control_character_escaping_test() {
	log "Starting usage error failure report unsafe control-character escaping test"

	esc=$(printf '\033')
	bell=$(printf '\007')
	control_source=$(printf 'tank/src%s[31m%s' "$esc" "$bell")
	stdout_log="$WORKDIR/usage_failure_control_chars.stdout"
	stderr_log="$WORKDIR/usage_failure_control_chars.stderr"
	safe_rm_f "$stdout_log" "$stderr_log"

	set +e
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 "$ZXFER_BIN" -R "$control_source" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage failure report unsafe control-character escaping test expected exit status 2, got $status. See $stderr_log."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Usage failure report unsafe control-character escaping test should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -F -x "invocation: '$ZXFER_BIN' '-R' 'tank/src\\x1B[31m\\x07'" "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report unsafe control-character escaping test missing escaped ESC sequence. Output: $(cat "$stderr_log")"
	fi
	if ! grep -F '\x07' "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report unsafe control-character escaping test missing escaped BEL sequence. Output: $(cat "$stderr_log")"
	fi
	if grep -F '\\x1B' "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report unsafe control-character escaping test double-escaped the ESC marker. Output: $(cat "$stderr_log")"
	fi
	if grep -F "$esc" "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report unsafe control-character escaping test leaked a raw ESC byte. Output: $(cat "$stderr_log")"
	fi
	if grep -F "$bell" "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report unsafe control-character escaping test leaked a raw BEL byte. Output: $(cat "$stderr_log")"
	fi

	safe_rm_f "$stdout_log" "$stderr_log"

	log "Usage error failure report control-character escaping test passed"
}

usage_error_failure_report_trailing_newline_preservation_test() {
	log "Starting usage error failure report unsafe trailing-newline preservation test"

	trailing_source=$(printf 'tank/src\n_')
	trailing_source=${trailing_source%_}
	stdout_log="$WORKDIR/usage_failure_trailing_newline.stdout"
	stderr_log="$WORKDIR/usage_failure_trailing_newline.stderr"
	safe_rm_f "$stdout_log" "$stderr_log"

	set +e
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 "$ZXFER_BIN" -R "$trailing_source" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage failure report unsafe trailing-newline preservation test expected exit status 2, got $status. See $stderr_log."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Usage failure report unsafe trailing-newline preservation test should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -F -x "invocation: '$ZXFER_BIN' '-R' 'tank/src\\n'" "$stderr_log" >/dev/null 2>&1; then
		fail "Usage failure report unsafe trailing-newline preservation test missing escaped newline marker. Output: $(cat "$stderr_log")"
	fi

	safe_rm_f "$stdout_log" "$stderr_log"

	log "Usage error failure report unsafe trailing-newline preservation test passed"
}

failure_handling_tests() {
	log "Starting missing dataset error tests"

	# Ensure destination exists so the source failure path is hit.
	dest_root="$DEST_POOL/failure_dest"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"

	assert_error_case "Missing source dataset" \
		"Failed to retrieve snapshots from the source" \
		1 \
		-R "$SRC_POOL/no_such_dataset" "$dest_root"

	log "Missing dataset error tests passed"
}

runtime_failure_report_test() {
	log "Starting runtime failure report test"

	dest_root="$DEST_POOL/failure_report_dest"
	stdout_log="$WORKDIR/runtime_failure.stdout"
	stderr_log="$WORKDIR/runtime_failure.stderr"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"

	set +e
	"$ZXFER_BIN" -R "$SRC_POOL/no_such_dataset" "$dest_root" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 1 ]; then
		fail "Runtime failure report test expected exit status 1, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ -s "$stdout_log" ]; then
		fail "Runtime failure report should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -q "^zxfer: failure report begin$" "$stderr_log"; then
		fail "Runtime failure report block missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_stage: snapshot discovery" "$stderr_log"; then
		fail "Runtime failure report stage missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "source_root: $SRC_POOL/no_such_dataset" "$stderr_log"; then
		fail "Runtime failure report source_root missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "destination_root: $dest_root" "$stderr_log"; then
		fail "Runtime failure report destination_root missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "^last_command: " "$stderr_log"; then
		fail "Runtime failure report last_command missing. Output: $(cat "$stderr_log")"
	fi

	log "Runtime failure report test passed"
}

runtime_failure_report_redaction_test() {
	log "Starting runtime failure report default redaction test"

	dest_root="$DEST_POOL/failure_report_redacted_dest"
	progress_secret="printf runtime-secret-token"
	log_path="$WORKDIR/runtime_failure_redacted.report"
	stdout_log="$WORKDIR/runtime_failure_redacted.stdout"
	stderr_log="$WORKDIR/runtime_failure_redacted.stderr"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"
	safe_rm_f "$log_path" "$stdout_log" "$stderr_log"

	set +e
	ZXFER_ERROR_LOG="$log_path" \
		"$ZXFER_BIN" -D "$progress_secret" -R "$SRC_POOL/no_such_dataset" "$dest_root" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 1 ]; then
		fail "Runtime failure report default redaction test expected exit status 1, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ -s "$stdout_log" ]; then
		fail "Runtime failure report default redaction test should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if [ ! -f "$log_path" ]; then
		fail "Runtime failure report default redaction test expected ZXFER_ERROR_LOG output."
	fi
	if ! grep -q "^invocation: \[redacted\]$" "$stderr_log"; then
		fail "Runtime failure report default redaction test missing redacted invocation on stderr. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "^last_command: \[redacted\]$" "$stderr_log"; then
		fail "Runtime failure report default redaction test missing redacted last_command on stderr. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "^invocation: \[redacted\]$" "$log_path"; then
		fail "Runtime failure report default redaction test missing redacted invocation in ZXFER_ERROR_LOG. Output: $(cat "$log_path")"
	fi
	if ! grep -q "^last_command: \[redacted\]$" "$log_path"; then
		fail "Runtime failure report default redaction test missing redacted last_command in ZXFER_ERROR_LOG. Output: $(cat "$log_path")"
	fi
	if grep -F "$progress_secret" "$stderr_log" >/dev/null 2>&1; then
		fail "Runtime failure report default redaction test leaked the secret-bearing progress command on stderr. Output: $(cat "$stderr_log")"
	fi
	if grep -F "$progress_secret" "$log_path" >/dev/null 2>&1; then
		fail "Runtime failure report default redaction test leaked the secret-bearing progress command in ZXFER_ERROR_LOG. Output: $(cat "$log_path")"
	fi

	log "Runtime failure report default redaction test passed"
}

runtime_failure_report_unsafe_commands_test() {
	log "Starting runtime failure report unsafe command test"

	dest_root="$DEST_POOL/failure_report_unsafe_dest"
	progress_secret="printf runtime-secret-token"
	log_path="$WORKDIR/runtime_failure_unsafe.report"
	stdout_log="$WORKDIR/runtime_failure_unsafe.stdout"
	stderr_log="$WORKDIR/runtime_failure_unsafe.stderr"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"
	safe_rm_f "$log_path" "$stdout_log" "$stderr_log"

	set +e
	ZXFER_ERROR_LOG="$log_path" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_BIN" -D "$progress_secret" -R "$SRC_POOL/no_such_dataset" "$dest_root" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 1 ]; then
		fail "Runtime failure report unsafe command test expected exit status 1, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ -s "$stdout_log" ]; then
		fail "Runtime failure report unsafe command test should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if [ ! -f "$log_path" ]; then
		fail "Runtime failure report unsafe command test expected ZXFER_ERROR_LOG output."
	fi
	if ! grep -F "$progress_secret" "$stderr_log" >/dev/null 2>&1; then
		fail "Runtime failure report unsafe command test should preserve the secret-bearing progress command on stderr when explicitly enabled. Output: $(cat "$stderr_log")"
	fi
	if ! grep -F "$progress_secret" "$log_path" >/dev/null 2>&1; then
		fail "Runtime failure report unsafe command test should preserve the secret-bearing progress command in ZXFER_ERROR_LOG when explicitly enabled. Output: $(cat "$log_path")"
	fi
	if ! grep -q "^last_command: " "$stderr_log"; then
		fail "Runtime failure report unsafe command test should preserve last_command on stderr when explicitly enabled. Output: $(cat "$stderr_log")"
	fi
	if grep -q "^last_command: \[redacted\]$" "$stderr_log"; then
		fail "Runtime failure report unsafe command test should not redact last_command on stderr when explicitly enabled. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "^last_command: " "$log_path"; then
		fail "Runtime failure report unsafe command test should preserve last_command in ZXFER_ERROR_LOG when explicitly enabled. Output: $(cat "$log_path")"
	fi
	if grep -q "^last_command: \[redacted\]$" "$log_path"; then
		fail "Runtime failure report unsafe command test should not redact last_command in ZXFER_ERROR_LOG when explicitly enabled. Output: $(cat "$log_path")"
	fi

	log "Runtime failure report unsafe command test passed"
}

extended_usage_error_tests() {
	log "Starting extended usage error tests"

	# Test sources/destinations starting with / (should fail validation)
	assert_usage_error_case "Source starting with /" \
		"Source and destination must not begin with \"/\"." \
		-R /tank/src backup/target

	assert_usage_error_case "Destination starting with /" \
		"Source and destination must not begin with \"/\"." \
		-R tank/src /backup/target

	# Test snapshot source (not supported for recursive/non-recursive flags in this way usually,
	# or at least zxfer often expects filesystems.
	# Based on code reading, zxfer_check_snapshot should reject if it looks like a snapshot but we wanted a fs)
	# Actually zxfer_replication.sh:303 checks if source is a snapshot and fails if so for normal mode.
	assert_error_case "Source is a snapshot" \
		"Snapshots are not allowed as a source." \
		1 \
		-R tank/src@snap backup/target

	# Test -c without -m
	assert_error_case "-c without -m" \
		"When using -c, -m needs to be specified as well." \
		1 \
		-c svc:/network/ssh -R tank/src backup/target

	log "Extended usage error tests passed"
}

consistency_option_validation_tests() {
	log "Starting option consistency tests"

	assert_usage_error_case "Backup and restore properties together" \
		"You cannot bac(k)up and r(e)store properties at the same time." \
		-k -e -R tank/src backup/target

	assert_usage_error_case "Both beep modes" \
		"You cannot use both beep modes at the same time." \
		-b -B -R tank/src backup/target

	assert_usage_error_case "Compression without remote" \
		"-z option can only be used with -O or -T option" \
		-z -R tank/src backup/target

	assert_usage_error_case "Empty compression command" \
		"Compression command (-Z) cannot be empty." \
		-Z "" -R tank/src backup/target

	assert_usage_error_case "Zero job count" \
		"The -j option requires a job count of at least 1." \
		-j 0 -R tank/src backup/target

	assert_usage_error_case "Non-numeric job count" \
		"The -j option requires a positive integer job count, but received \"abc\"." \
		-j abc -R tank/src backup/target

	log "Option consistency tests passed"
}

error_log_mirror_test() {
	log "Starting ZXFER_ERROR_LOG mirror test"

	dest_root="$DEST_POOL/error_log_dest"
	log_path="$WORKDIR/runtime_failure.report"
	stderr_log="$WORKDIR/error_log.stderr"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"
	safe_rm_f "$log_path"

	set +e
	ZXFER_ERROR_LOG="$log_path" "$ZXFER_BIN" -R "$SRC_POOL/no_such_dataset" "$dest_root" >/dev/null 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 1 ]; then
		fail "ZXFER_ERROR_LOG mirror test expected exit status 1, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ ! -f "$log_path" ]; then
		fail "ZXFER_ERROR_LOG mirror file was not created."
	fi
	if ! grep -q "^zxfer: failure report begin$" "$log_path"; then
		fail "ZXFER_ERROR_LOG mirror file missing report block. Output: $(cat "$log_path")"
	fi
	if ! grep -q "message: Failed to retrieve snapshots from the source" "$log_path"; then
		fail "ZXFER_ERROR_LOG mirror file missing failure message. Output: $(cat "$log_path")"
	fi

	log "ZXFER_ERROR_LOG mirror test passed"
}

usage_error_log_mirror_test() {
	log "Starting usage ZXFER_ERROR_LOG mirror test"

	log_path="$WORKDIR/usage_failure.report"
	stderr_log="$WORKDIR/usage_error_log.stderr"
	safe_rm_f "$log_path" "$stderr_log"

	set +e
	ZXFER_ERROR_LOG="$log_path" "$ZXFER_BIN" -R tank/src >/dev/null 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage ZXFER_ERROR_LOG mirror test expected exit status 2, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ ! -f "$log_path" ]; then
		fail "Usage ZXFER_ERROR_LOG mirror file was not created."
	fi
	if ! grep -q "^zxfer: failure report begin$" "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing report block. Output: $(cat "$log_path")"
	fi
	if ! grep -q "failure_class: usage" "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing usage class. Output: $(cat "$log_path")"
	fi
	if ! grep -q "exit_status: 2" "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing exit status 2. Output: $(cat "$log_path")"
	fi
	if ! grep -q "message: Need a destination." "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing usage message. Output: $(cat "$log_path")"
	fi

	log "Usage ZXFER_ERROR_LOG mirror test passed"
}

invalid_error_log_warning_test() {
	log "Starting invalid ZXFER_ERROR_LOG warning test"

	dest_root="$DEST_POOL/error_log_warning_dest"
	stderr_log="$WORKDIR/error_log_warning.stderr"
	relative_log="relative_failure_report.log"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"
	safe_rm_f "$WORKDIR/$relative_log"
	zxfer_abs=$(cd "$(dirname "$ZXFER_BIN")" && pwd)/$(basename "$ZXFER_BIN")

	set +e
	(
		cd "$WORKDIR"
		ZXFER_ERROR_LOG="$relative_log" "$zxfer_abs" -R "$SRC_POOL/no_such_dataset" "$dest_root" >/dev/null 2>"$stderr_log"
	)
	status=$?
	set -e

	if [ "$status" -ne 1 ]; then
		fail "Invalid ZXFER_ERROR_LOG warning test expected exit status 1, got $status. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "warning: refusing ZXFER_ERROR_LOG path \"$relative_log\" because it is not absolute" "$stderr_log"; then
		fail "ZXFER_ERROR_LOG warning missing from stderr. Output: $(cat "$stderr_log")"
	fi
	if [ -e "$WORKDIR/$relative_log" ]; then
		fail "Relative ZXFER_ERROR_LOG should not create a file in $WORKDIR."
	fi

	log "Invalid ZXFER_ERROR_LOG warning test passed"
}

error_log_email_example_self_test() {
	log "Starting error-log email example self-test"

	set +e
	output=$(sh ./examples/error-log-email-notify.sh --self-test 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Error-log email example self-test failed with status $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "self-test passed"; then
		fail "Error-log email example self-test did not report success. Output: $output"
	fi

	log "Error-log email example self-test passed"
}

verbose_debug_logging_test() {
	log "Starting verbose/debug logging test"

	src_dataset="$SRC_POOL/verbose_src"
	dest_root="$DEST_POOL/verbose_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	stdout_log="$WORKDIR/verbose_stdout.log"
	stderr_log="$WORKDIR/verbose_stderr.log"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_f "$stdout_log" "$stderr_log"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs snap -r "$src_dataset@vlog1"

	set +e
	"$ZXFER_BIN" -v -V -n -R "$src_dataset" "$dest_root" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Verbose/debug dry run should succeed. See $stdout_log and $stderr_log."
	fi
	if ! grep -q "Dry run: skipping live replication-state validation and command planning." "$stderr_log"; then
		fail "zxfer_echoV dry-run debug output missing from stderr."
	fi
	if grep -q "Dry run: skipping live replication-state validation and command planning." "$stdout_log"; then
		fail "zxfer_echoV dry-run debug output should not appear on stdout."
	fi
	if ! grep -q "Checking source snapshot." "$stderr_log"; then
		fail "Verbose output missing expected stderr status message."
	fi
	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Dry run should not create destination dataset $dest_dataset."
	fi

	log "Verbose/debug logging test passed"
}
