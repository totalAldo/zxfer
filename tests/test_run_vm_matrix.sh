#!/bin/sh
#
# shunit2 tests for the VM-backed integration runner.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_vm_matrix"
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	VM_MATRIX_BIN="$ZXFER_ROOT/tests/run_vm_matrix.sh"
	VM_MATRIX_LIB="$ZXFER_ROOT/tests/vm/lib.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	unset ZXFER_VM_JOBS ZXFER_VM_STREAM_GUEST_OUTPUT ZXFER_VM_FAILED_TESTS_ONLY ZXFER_VM_ONLY_TESTS ZXFER_VM_TEST_LAYER ZXFER_VM_PERF_PROFILE ZXFER_VM_PERF_BASELINE_REF
	# shellcheck source=tests/vm/lib.sh
	. "$VM_MATRIX_LIB"
	zxfer_vm_reset_state
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_profile_local_selects_ubuntu_and_freebsd() {
	zxfer_vm_select_guests

	assertEquals "The local profile should run the Ubuntu and FreeBSD guests by default." \
		"ubuntu freebsd" "$ZXFER_VM_SELECTED_GUESTS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_profile_full_includes_omnios() {
	ZXFER_VM_PROFILE=full

	zxfer_vm_select_guests

	assertEquals "The full profile should include Ubuntu, FreeBSD, and OmniOS." \
		"ubuntu freebsd omnios" "$ZXFER_VM_SELECTED_GUESTS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_parse_args_accepts_jobs_and_stream_guest_output() {
	zxfer_vm_parse_args --jobs 2 --stream-guest-output

	assertEquals "The VM runner should accept an explicit guest concurrency." \
		"2" "$ZXFER_VM_JOBS"
	assertEquals "The VM runner should enable live guest output streaming when requested." \
		"1" "$ZXFER_VM_STREAM_GUEST_OUTPUT"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_parse_args_accepts_shunit2_test_layer() {
	zxfer_vm_parse_args --test-layer shunit2

	assertEquals "The VM runner should accept an opt-in shunit2 guest test layer." \
		"shunit2" "$ZXFER_VM_TEST_LAYER"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_parse_args_accepts_perf_test_layer() {
	zxfer_vm_parse_args --test-layer perf

	assertEquals "The VM runner should accept an opt-in performance test layer." \
		"perf" "$ZXFER_VM_TEST_LAYER"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_parse_args_accepts_perf_compare_test_layer() {
	zxfer_vm_parse_args --test-layer perf-compare

	assertEquals "The VM runner should accept the performance comparison test layer." \
		"perf-compare" "$ZXFER_VM_TEST_LAYER"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_shell_quote_preserves_single_shell_argument() {
	quoted=$(zxfer_vm_shell_quote "feature/has'quote")

	assertEquals "The VM shell quoting helper should preserve embedded single quotes." \
		"'feature/has'\\''quote'" "$quoted"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_parse_args_accepts_failed_tests_only() {
	zxfer_vm_parse_args --failed-tests-only

	assertEquals "The VM runner should accept failure-only guest harness output mode." \
		"1" "$ZXFER_VM_FAILED_TESTS_ONLY"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_parse_args_accepts_only_test_lists() {
	zxfer_vm_parse_args --only-test basic_replication_test,force_rollback_test --only-test usage_error_tests

	assertEquals "The VM runner should accept comma-delimited and repeated in-guest --only-test selectors." \
		"basic_replication_test force_rollback_test usage_error_tests" "$ZXFER_VM_ONLY_TESTS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_failed_tests_only_auto_enables_stream_guest_output() {
	zxfer_vm_parse_args --failed-tests-only
	zxfer_vm_normalize_output_modes

	assertEquals "Failure-only VM runs should force live guest output so progress markers stay visible." \
		"1" "$ZXFER_VM_STREAM_GUEST_OUTPUT"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_validate_options_rejects_invalid_jobs() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_JOBS=0
		zxfer_vm_validate_options
	"

	assertEquals "The VM runner should reject zero guest jobs." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The jobs validation should explain the constraint." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--jobs must be a positive integer"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_validate_options_rejects_unknown_test_layer() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_TEST_LAYER=nosuchlayer
		zxfer_vm_validate_options
	"

	assertEquals "Unknown VM test layers should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Unknown test-layer validation should identify the bad value." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Unsupported test layer: nosuchlayer"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_validate_options_rejects_unknown_perf_profile() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_TEST_LAYER=perf
		ZXFER_VM_PERF_PROFILE=fast
		zxfer_vm_validate_options
	"

	assertEquals "Unknown VM perf profiles should fail before rendering guest shell." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should identify the unsupported perf profile." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Unsupported VM performance profile: fast"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_backend_validate_selection_rejects_ci_managed_perf_compare() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_BACKEND=ci-managed
		ZXFER_VM_TEST_LAYER=perf-compare
		ZXFER_VM_SELECTED_GUESTS=ubuntu
		zxfer_vm_backend_validate_selection
	"

	assertEquals "perf-compare should require the qemu backend because qemu exports the baseline ref." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should explain the backend requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "perf-compare test layer currently requires the qemu backend"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_ref_archive_rejects_unknown_perf_baseline_ref() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_qemu_write_ref_archive refs/heads/zxfer-nosuch-perf-baseline-ref >/dev/null
	"

	assertEquals "The qemu backend should validate the baseline ref on the host before guest copy." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should name ZXFER_VM_PERF_BASELINE_REF." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_VM_PERF_BASELINE_REF does not name a tree"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_ref_archive_rejects_option_like_perf_baseline_ref() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_qemu_write_ref_archive --output=/tmp/zxfer-perf-baseline.tar >/dev/null
	"

	assertEquals "The qemu backend should reject option-like baseline refs before invoking git." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should reject option-like ZXFER_VM_PERF_BASELINE_REF values." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "must not begin with '-'"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_validate_options_rejects_only_test_outside_integration_layer() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_TEST_LAYER=shunit2
		ZXFER_VM_ONLY_TESTS=basic_replication_test
		zxfer_vm_validate_options
	"

	assertEquals "Shunit2 guest runs should reject integration-only test selectors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should point callers back to the integration layer." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--only-test is only supported with --test-layer integration"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_validate_options_rejects_failed_tests_only_outside_integration_layer() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_TEST_LAYER=shunit2
		ZXFER_VM_FAILED_TESTS_ONLY=1
		zxfer_vm_validate_options
	"

	assertEquals "Shunit2 guest runs should reject integration-only failure filtering." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should identify the integration-only flag." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--failed-tests-only is only supported with --test-layer integration"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_should_show_progress_bar_requires_serial_tty() {
	ZXFER_VM_STDERR_IS_TTY=1
	ZXFER_VM_JOBS=1

	assertEquals "Interactive serial runs should allow a host-side progress bar." \
		0 "$(
			zxfer_vm_should_show_progress_bar
			printf '%s' "$?"
		)"

	ZXFER_VM_JOBS=2
	assertEquals "Parallel runs should suppress host-side progress bars to avoid interleaved output." \
		1 "$(
			zxfer_vm_should_show_progress_bar
			printf '%s' "$?"
		)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_download_and_verify_file_marks_cached_state() {
	download_file="$TEST_TMPDIR/cached-download.img"
	printf '%s\n' "payload" >"$download_file"
	expected_sha=$(zxfer_vm_sha256_file "$download_file")

	zxfer_vm_download_and_verify_file "https://example.invalid/cached.img" "$download_file" "$expected_sha"

	assertEquals "Cache hits should be reported distinctly from fresh downloads." \
		"cached" "$ZXFER_VM_LAST_DOWNLOAD_ACTION"
	assertEquals "Cache hit state should record the on-disk file size." \
		"8" "$ZXFER_VM_LAST_DOWNLOAD_SIZE_BYTES"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_refresh_cached_download_uses_cached_copy_when_refresh_fails() {
	mock_bin="$TEST_TMPDIR/mock-bin-refresh-cached"
	checksum_file="$TEST_TMPDIR/SHA256SUMS"
	mkdir -p "$mock_bin"
	printf '%s\n' "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789 ubuntu-24.04-server-cloudimg-arm64.img" >"$checksum_file"

	cat <<'EOF' >"$mock_bin/curl"
#!/bin/sh
exit 6
EOF
	chmod 700 "$mock_bin/curl"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\"
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_refresh_cached_download 'https://example.invalid/SHA256SUMS' \"$checksum_file\" 'Ubuntu 24.04/arm64' 'checksum manifest SHA256SUMS'
		cat \"$checksum_file\"
	"

	assertEquals "Cached checksum manifests should allow offline VM reruns when refresh fails." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The refresh helper should warn when it reuses a cached manifest after a download failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "reusing cached copy"
	assertContains "The refresh helper should preserve the existing manifest contents when it falls back to cache." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ubuntu-24.04-server-cloudimg-arm64.img"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_refresh_cached_download_fails_without_cached_copy() {
	mock_bin="$TEST_TMPDIR/mock-bin-refresh-miss"
	checksum_file="$TEST_TMPDIR/missing.SHA256SUMS"
	mkdir -p "$mock_bin"

	cat <<'EOF' >"$mock_bin/curl"
#!/bin/sh
exit 6
EOF
	chmod 700 "$mock_bin/curl"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\"
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_refresh_cached_download 'https://example.invalid/SHA256SUMS' \"$checksum_file\" 'Ubuntu 24.04/arm64' 'checksum manifest SHA256SUMS'
	"

	assertEquals "Checksum refresh should still fail closed when no cached manifest exists." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The refresh helper should keep the original download failure when it cannot reuse a cache." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to download https://example.invalid/SHA256SUMS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_decompress_archive_marks_cached_state() {
	base_image="$TEST_TMPDIR/base-image.qcow2"
	printf '%s\n' "base-image" >"$base_image"

	zxfer_vm_decompress_archive "$TEST_TMPDIR/archive.qcow2" "$base_image" none

	assertEquals "Prepared base-image cache hits should be reported distinctly." \
		"cached" "$ZXFER_VM_LAST_DECOMPRESS_ACTION"
	assertEquals "Prepared base-image cache hits should record the on-disk file size." \
		"11" "$ZXFER_VM_LAST_DECOMPRESS_SIZE_BYTES"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_qemu_preferred_arch_prefers_arm64_on_arm_hosts() {
	ZXFER_VM_HOST_ARCH=arm64

	assertEquals "Apple Silicon and other arm64 hosts should prefer Ubuntu arm64 images when available." \
		"arm64" "$(zxfer_vm_guest_qemu_preferred_arch ubuntu)"
	assertEquals "Apple Silicon and other arm64 hosts should prefer FreeBSD arm64 images when available." \
		"arm64" "$(zxfer_vm_guest_qemu_preferred_arch freebsd)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_qemu_preferred_arch_falls_back_to_amd64_when_arm64_is_unavailable() {
	ZXFER_VM_HOST_ARCH=arm64

	assertEquals "Guests without an arm64 image should fall back to amd64 on arm64 hosts." \
		"amd64" "$(zxfer_vm_guest_qemu_preferred_arch omnios)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_qemu_seed_transport_uses_cidata_for_freebsd() {
	assertEquals "FreeBSD qemu guests should use a cidata config-drive seed transport." \
		"disk-cidata" "$(zxfer_vm_guest_qemu_seed_transport freebsd)"
	assertEquals "Ubuntu qemu guests should keep the nocloud-net SMBIOS transport." \
		"smbios-nocloud-net" "$(zxfer_vm_guest_qemu_seed_transport ubuntu)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_qemu_ssh_ready_probe_count_uses_single_probe_for_supported_guests() {
	assertEquals "OmniOS qemu guests should use a shorter but still stable SSH readiness threshold under first-boot host-key churn." \
		"3" "$(zxfer_vm_guest_qemu_ssh_ready_probe_count omnios)"
	assertEquals "Ubuntu qemu guests should keep the default one-probe SSH readiness threshold." \
		"1" "$(zxfer_vm_guest_qemu_ssh_ready_probe_count ubuntu)"
	assertEquals "FreeBSD qemu guests should keep the default one-probe SSH readiness threshold." \
		"1" "$(zxfer_vm_guest_qemu_ssh_ready_probe_count freebsd)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_wait_for_ssh_resets_stability_when_host_key_changes() {
	mock_bin="$TEST_TMPDIR/mock-bin"
	known_hosts_file="$TEST_TMPDIR/known_hosts"
	keyscan_count_file="$TEST_TMPDIR/keyscan-count"
	ssh_count_file="$TEST_TMPDIR/ssh-count"
	mkdir -p "$mock_bin"

	cat <<EOF >"$mock_bin/ssh-keyscan"
#!/bin/sh
count=0
if [ -r "$keyscan_count_file" ]; then
	count=\$(cat "$keyscan_count_file")
fi
count=\$((count + 1))
	printf '%s\n' "\$count" >"$keyscan_count_file"
if [ "\$count" -eq 1 ]; then
	printf '%s\n' "[127.0.0.1]:2222 ssh-ed25519 KEY_A"
else
	printf '%s\n' "[127.0.0.1]:2222 ssh-ed25519 KEY_B"
fi
EOF
	chmod 700 "$mock_bin/ssh-keyscan"

	cat <<EOF >"$mock_bin/ssh"
#!/bin/sh
count=0
if [ -r "$ssh_count_file" ]; then
	count=\$(cat "$ssh_count_file")
fi
count=\$((count + 1))
printf '%s\n' "\$count" >"$ssh_count_file"
exit 0
EOF
	chmod 700 "$mock_bin/ssh"

	cat <<'EOF' >"$mock_bin/sleep"
#!/bin/sh
exit 0
EOF
	chmod 700 "$mock_bin/sleep"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\"
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_qemu_wait_for_ssh 127.0.0.1 2222 \"$known_hosts_file\" \"$TEST_TMPDIR/id_ed25519\" 30 \"OmniOS r151056/amd64\" 2
		printf 'ssh-count=%s\n' \"\$(cat \"$ssh_count_file\")\"
	"

	assertEquals "The readiness probe should succeed once the SSH host key stays stable." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A host-key change during readiness should reset the consecutive-success counter." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ssh-count=3"
	assertContains "The final known_hosts file should keep the stable replacement host key." \
		"$(cat "$known_hosts_file")" "KEY_B"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_prepare_remote_ssh_step_reuses_existing_known_hosts_when_keyscan_flakes() {
	mock_bin="$TEST_TMPDIR/mock-bin-prepare-step"
	known_hosts_file="$TEST_TMPDIR/known_hosts.prepare-step"
	ssh_count_file="$TEST_TMPDIR/ssh-count.prepare-step"
	mkdir -p "$mock_bin"
	printf '%s\n' "[127.0.0.1]:2222 ssh-ed25519 KEY_A" >"$known_hosts_file"

	cat <<'EOF' >"$mock_bin/ssh-keyscan"
#!/bin/sh
exit 1
EOF
	chmod 700 "$mock_bin/ssh-keyscan"

	cat <<EOF >"$mock_bin/ssh"
#!/bin/sh
count=0
if [ -r "$ssh_count_file" ]; then
	count=\$(cat "$ssh_count_file")
fi
count=\$((count + 1))
printf '%s\n' "\$count" >"$ssh_count_file"
exit 0
EOF
	chmod 700 "$mock_bin/ssh"

	cat <<'EOF' >"$mock_bin/sleep"
#!/bin/sh
exit 0
EOF
	chmod 700 "$mock_bin/sleep"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\"
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_qemu_prepare_remote_ssh_step 127.0.0.1 2222 \"$known_hosts_file\" \"$TEST_TMPDIR/id_ed25519\" 'FreeBSD 15.0/arm64' 'the selected guest test layer' 15
		printf 'ssh-count=%s\n' \"\$(cat \"$ssh_count_file\")\"
	"

	assertEquals "Transient keyscan failures should not abort a remote step when the existing known_hosts entry still works." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The helper should warn when it falls back to the existing validated known_hosts entry." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "reusing the existing validated known_hosts entry"
	assertContains "The fallback path should still prove the guest is reachable over SSH." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ssh-count=1"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_prepare_remote_ssh_step_retries_until_keyscan_recovers() {
	mock_bin="$TEST_TMPDIR/mock-bin-prepare-step-retry"
	known_hosts_file="$TEST_TMPDIR/known_hosts.prepare-step-retry"
	keyscan_count_file="$TEST_TMPDIR/keyscan-count.prepare-step-retry"
	mkdir -p "$mock_bin"

	cat <<EOF >"$mock_bin/ssh-keyscan"
#!/bin/sh
count=0
if [ -r "$keyscan_count_file" ]; then
	count=\$(cat "$keyscan_count_file")
fi
count=\$((count + 1))
printf '%s\n' "\$count" >"$keyscan_count_file"
if [ "\$count" -eq 1 ]; then
	exit 1
fi
printf '%s\n' "[127.0.0.1]:2222 ssh-ed25519 KEY_B"
EOF
	chmod 700 "$mock_bin/ssh-keyscan"

	cat <<'EOF' >"$mock_bin/ssh"
#!/bin/sh
exit 1
EOF
	chmod 700 "$mock_bin/ssh"

	cat <<'EOF' >"$mock_bin/sleep"
#!/bin/sh
exit 0
EOF
	chmod 700 "$mock_bin/sleep"

	zxfer_test_capture_subshell "
		PATH=\"$mock_bin:\$PATH\"
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_qemu_prepare_remote_ssh_step 127.0.0.1 2222 \"$known_hosts_file\" \"$TEST_TMPDIR/id_ed25519\" 'FreeBSD 15.0/arm64' 'guest preparation' 15
		printf 'keyscan-count=%s\n' \"\$(cat \"$keyscan_count_file\")\"
	"

	assertEquals "Step-level SSH preparation should retry until ssh-keyscan succeeds again." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The helper should keep retrying transient keyscan failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "keyscan-count=2"
	assertContains "Recovered keyscan retries should log that the host-key refresh succeeded before the remote step continues." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "SSH host-key refresh recovered for guest preparation"
	assertContains "The recovered refresh should publish the new host key." \
		"$(cat "$known_hosts_file")" "KEY_B"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_report_guest_command_failure_surfaces_shunit_suite_failures() {
	stdout_file="$TEST_TMPDIR/harness.stdout"
	stderr_file="$TEST_TMPDIR/harness.stderr"
	cat <<'EOF' >"$stdout_file"
Ran 3 tests.
FAILED
EOF
	cat <<'EOF' >"$stderr_file"
!! Suite failed: /root/zxfer/tests/test_zxfer_runtime.sh (exit status 1)
!! Suite failed: /root/zxfer/tests/test_zxfer_snapshot_state.sh (exit status 2)
EOF

	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_report_guest_command_failure 'FreeBSD 15.0/arm64' 'shunit2 runner' 1 \"$stdout_file\" \"$stderr_file\"
	"

	assertEquals "Guest command failure summaries should not fail the reporting helper itself." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The summary should identify the failing guest step and exit status." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "shunit2 runner failed with exit status 1"
	assertContains "The summary should surface shunit2 suite failures from harness stderr." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "!! Suite failed: /root/zxfer/tests/test_zxfer_runtime.sh"
	assertContains "Artifact-only summaries should still point operators to the captured guest logs." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "guest logs: $stdout_file, $stderr_file"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_report_guest_command_failure_falls_back_to_assert_lines() {
	stdout_file="$TEST_TMPDIR/assert-only.stdout"
	stderr_file="$TEST_TMPDIR/assert-only.stderr"
	cat <<'EOF' >"$stdout_file"
ASSERT:Remote -j discovery should preserve the validation failure without requiring verbose mode. Not found:<remote parallel validation failed>
EOF
	: >"$stderr_file"

	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		zxfer_vm_report_guest_command_failure 'FreeBSD 15.0/arm64' 'shunit2 runner' 1 \"$stdout_file\" \"$stderr_file\"
	"

	assertEquals "Assertion-only harness failures should still render through the shared guest failure reporter." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "When suite summaries are unavailable, the reporter should surface the first assertion line from stdout." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ASSERT:Remote -j discovery should preserve the validation failure without requiring verbose mode."
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_render_cloud_init_uses_freebsd_cloud_config_root_bootstrap() {
	seed_dir="$TEST_TMPDIR/freebsd-seed"
	pubkey_file="$TEST_TMPDIR/freebsd-key.pub"
	printf '%s\n' "ssh-ed25519 AAAATESTKEY freebsd" >"$pubkey_file"

	zxfer_vm_qemu_render_cloud_init freebsd "$seed_dir" "$pubkey_file"

	assertContains "FreeBSD seed data should use the documented cloud-config user-data path." \
		"$(sed -n '1,20p' "$seed_dir/user-data")" "#cloud-config"
	assertContains "FreeBSD seed data should also provision the default user with the generated SSH key." \
		"$(cat "$seed_dir/user-data")" "ssh_authorized_keys:"
	assertContains "FreeBSD seed data should install the SSH key for root." \
		"$(cat "$seed_dir/user-data")" "/root/.ssh/authorized_keys"
	assertContains "FreeBSD seed data should enable explicit root SSH logins for the bootstrap key path." \
		"$(cat "$seed_dir/user-data")" "PermitRootLogin yes"
	assertContains "FreeBSD seed data should restart sshd after applying the root login bootstrap." \
		"$(cat "$seed_dir/user-data")" "service sshd restart"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_select_guests_rejects_unknown_guest() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_GUEST_FILTERS=nosuchguest
		zxfer_vm_select_guests
	"

	assertEquals "Unknown guest filters should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The error should identify the unknown guest." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Unknown guest: nosuchguest"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_resolve_backend_uses_ci_managed_guest_override() {
	ZXFER_VM_CI_MANAGED_GUEST=freebsd
	ZXFER_VM_BACKEND=auto

	zxfer_vm_resolve_backend

	assertEquals "Backend auto should become ci-managed when a CI-managed guest override is present." \
		"ci-managed" "$ZXFER_VM_BACKEND"
	assertEquals "The override should narrow the guest list to the current guest." \
		"freebsd" "$ZXFER_VM_GUEST_FILTERS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_backend_validation_rejects_multiple_ci_managed_guests() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_BACKEND=ci-managed
		ZXFER_VM_SELECTED_GUESTS='ubuntu freebsd'
		zxfer_vm_backend_validate_selection
	"

	assertEquals "The ci-managed backend should reject multi-guest runs." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The ci-managed backend should explain why the selection is invalid." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "The ci-managed backend can only run one guest at a time"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_detect_host_platform_marks_wsl2() {
	printf '%s\n' "6.6.87.2-microsoft-standard-WSL2" >"$TEST_TMPDIR/osrelease"

	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_UNAME_S=Linux
		ZXFER_VM_UNAME_M=x86_64
		ZXFER_VM_PROC_OSRELEASE_FILE=\"$TEST_TMPDIR/osrelease\"
		zxfer_vm_detect_host_platform
		printf '%s/%s\n' \"\$ZXFER_VM_HOST_OS\" \"\$ZXFER_VM_HOST_ARCH\"
	"

	assertEquals "The WSL2 detection helper should identify the Linux guest environment correctly." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "The WSL2 detection helper should classify the host as wsl2/amd64." \
		"wsl2/amd64" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_checksum_parser_supports_bsd_format() {
	checksum_file="$TEST_TMPDIR/freebsd.CHECKSUM.SHA256"
	cat <<'EOF' >"$checksum_file"
SHA256 (FreeBSD-15.0-RELEASE-amd64-BASIC-CLOUDINIT-zfs.qcow2.xz) = abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
EOF

	result=$(zxfer_vm_resolve_expected_checksum "$checksum_file" "FreeBSD-15.0-RELEASE-amd64-BASIC-CLOUDINIT-zfs.qcow2.xz")

	assertEquals "The checksum parser should support FreeBSD-style SHA256 manifests." \
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_checksum_parser_supports_gnu_format() {
	checksum_file="$TEST_TMPDIR/ubuntu.SHA256SUMS"
	cat <<'EOF' >"$checksum_file"
abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789 ubuntu-24.04-server-cloudimg-amd64.img
EOF

	result=$(zxfer_vm_resolve_expected_checksum "$checksum_file" "ubuntu-24.04-server-cloudimg-amd64.img")

	assertEquals "The checksum parser should support GNU-style checksum manifests." \
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_checksum_parser_supports_gnu_binary_format() {
	checksum_file="$TEST_TMPDIR/ubuntu-binary.SHA256SUMS"
	cat <<'EOF' >"$checksum_file"
abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789 *ubuntu-24.04-server-cloudimg-arm64.img
EOF

	result=$(zxfer_vm_resolve_expected_checksum "$checksum_file" "ubuntu-24.04-server-cloudimg-arm64.img")

	assertEquals "The checksum parser should support GNU binary-mode checksum manifests." \
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_checksum_parser_supports_single_hash_files() {
	checksum_file="$TEST_TMPDIR/omnios.sha256"
	cat <<'EOF' >"$checksum_file"
abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
EOF

	result=$(zxfer_vm_resolve_expected_checksum "$checksum_file" "omnios-r151056.cloud.qcow2")

	assertEquals "The checksum parser should support one-line sha256 files." \
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_guest_rejects_strict_isolation_without_acceleration() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_HOST_OS=darwin
		ZXFER_VM_HOST_ARCH=arm64
		ZXFER_VM_HOST_CAN_ACCELERATE_ARM64=0
		ZXFER_VM_STRICT_ISOLATION_REQUIRED=1
		ZXFER_VM_BACKEND=qemu
		zxfer_vm_backend_qemu_run_guest ubuntu \"$TEST_TMPDIR/artifacts\"
	"

	assertEquals "Strict qemu lanes should fail closed when only TCG is available." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The qemu backend should explain the strict-isolation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "requires hardware virtualization for strict isolation"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_integration_harness_extra_args_adds_failed_tests_only_flag() {
	ZXFER_VM_FAILED_TESTS_ONLY=1

	assertEquals "The VM runner should pass --failed-tests-only through to the guest harness when requested." \
		"--failed-tests-only" "$(zxfer_vm_integration_harness_extra_args)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_integration_harness_extra_args_adds_only_test_flags() {
	ZXFER_VM_ONLY_TESTS="basic_replication_test force_rollback_test"

	assertEquals "The VM runner should pass named in-guest test filters through to the integration harness." \
		"--only-test basic_replication_test --only-test force_rollback_test" "$(zxfer_vm_integration_harness_extra_args)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_render_guest_test_script_defaults_to_integration_harness() {
	script_body=$(zxfer_vm_render_guest_test_script ubuntu /root/zxfer /var/tmp/zxfer-vm-matrix)

	assertContains "The default guest test layer should keep using the integration harness." \
		"$script_body" "./tests/run_integration_zxfer.sh --yes --keep-going"
	assertNotContains "The default guest test layer should not switch to the shunit2 runner." \
		"$script_body" "./tests/run_shunit_tests.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_render_guest_test_script_uses_shunit2_runner_when_requested() {
	ZXFER_VM_TEST_LAYER=shunit2

	script_body=$(zxfer_vm_render_guest_test_script ubuntu /root/zxfer /var/tmp/zxfer-vm-matrix)

	assertContains "Opt-in shunit2 guest runs should invoke the shunit2 suite runner." \
		"$script_body" "./tests/run_shunit_tests.sh --jobs 4"
	assertNotContains "Opt-in shunit2 guest runs should not invoke the integration harness." \
		"$script_body" "./tests/run_integration_zxfer.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_render_guest_test_script_uses_perf_runner_when_requested() {
	ZXFER_VM_TEST_LAYER=perf
	ZXFER_VM_PERF_PROFILE=standard

	script_body=$(zxfer_vm_render_guest_test_script ubuntu /root/zxfer /var/tmp/zxfer-vm-matrix)

	assertContains "Opt-in performance guest runs should invoke the perf runner." \
		"$script_body" "./tests/run_perf_tests.sh --yes --profile \"standard\""
	assertContains "Performance guest runs should keep artifacts under the guest temp root." \
		"$script_body" "ZXFER_PERF_OUTPUT_DIR=\"/var/tmp/zxfer-vm-matrix/perf-artifacts\""
	assertNotContains "Opt-in performance guest runs should not invoke the integration harness." \
		"$script_body" "./tests/run_integration_zxfer.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_render_guest_test_script_uses_perf_compare_runner_when_requested() {
	ZXFER_VM_TEST_LAYER=perf-compare
	ZXFER_VM_PERF_PROFILE=standard
	ZXFER_VM_PERF_BASELINE_REF=upstream-compat-final

	script_body=$(zxfer_vm_render_guest_test_script ubuntu /root/zxfer /var/tmp/zxfer-vm-matrix)

	assertContains "Performance comparison guest runs should invoke the comparator." \
		"$script_body" "./tests/run_perf_compare.sh --yes --profile \"standard\""
	assertContains "Performance comparison guest runs should use the archived baseline checkout beside the candidate." \
		"$script_body" "--baseline-bin \"/root/zxfer-baseline/zxfer\""
	assertContains "Performance comparison guest runs should label the baseline ref." \
		"$script_body" "--baseline-label 'upstream-compat-final'"
	assertContains "Performance comparison guest runs should measure the current checkout as candidate." \
		"$script_body" "--candidate-bin \"/root/zxfer/zxfer\""
	assertNotContains "Performance comparison guest runs should not rely on git inside the guest." \
		"$script_body" "git "
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_render_guest_test_script_shell_quotes_perf_compare_baseline_label() {
	ZXFER_VM_TEST_LAYER=perf-compare
	ZXFER_VM_PERF_PROFILE=smoke
	ZXFER_VM_PERF_BASELINE_REF="feature/has'quote"

	script_body=$(zxfer_vm_render_guest_test_script ubuntu /root/zxfer /var/tmp/zxfer-vm-matrix)

	assertContains "Performance comparison guest scripts should shell-quote the baseline label." \
		"$script_body" "--baseline-label 'feature/has'\\''quote'"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_render_guest_test_script_wraps_omnios_shunit2_with_bash_posix() {
	ZXFER_VM_TEST_LAYER=shunit2

	script_body=$(zxfer_vm_render_guest_test_script omnios /root/zxfer /var/tmp/zxfer-vm-matrix)

	assertContains "OmniOS shunit2 guest runs should resolve bash explicitly." \
		"$script_body" "bash_bin=\$(command -v bash)"
	assertContains "OmniOS shunit2 guest runs should export a bash --posix wrapper for the suites." \
		"$script_body" "export ZXFER_TEST_SHELL="
	assertContains "OmniOS shunit2 guest runs should preserve suite positional arguments in the generated bash wrapper." \
		"$script_body" '\$@'
	assertContains "OmniOS shunit2 guest runs should invoke the shunit2 runner through bash." \
		"$script_body" "\"\$bash_bin\" ./tests/run_shunit_tests.sh --jobs 2"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_prepare_script_skips_integration_packages_for_ubuntu_shunit2() {
	script_body=$(zxfer_vm_guest_prepare_script ubuntu qemu shunit2)

	assertEquals "Ubuntu shunit2 guest preparation should stay minimal." \
		":" "$script_body"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_prepare_script_installs_zfs_tools_for_ubuntu_perf() {
	script_body=$(zxfer_vm_guest_prepare_script ubuntu qemu perf)

	assertContains "Ubuntu perf guest preparation should install OpenZFS tooling." \
		"$script_body" "apt-get install -y csh zfsutils-linux parallel zstd"
	assertContains "Ubuntu perf guest preparation should load the ZFS module before running sparse-pool fixtures." \
		"$script_body" "modprobe zfs"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_prepare_script_installs_zfs_tools_for_ubuntu_perf_compare() {
	script_body=$(zxfer_vm_guest_prepare_script ubuntu qemu perf-compare)

	assertContains "Ubuntu perf-compare guest preparation should install OpenZFS tooling." \
		"$script_body" "apt-get install -y csh zfsutils-linux parallel zstd"
	assertContains "Ubuntu perf-compare guest preparation should load the ZFS module before running sparse-pool fixtures." \
		"$script_body" "modprobe zfs"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_prepare_script_installs_bash_for_freebsd_shunit2() {
	script_body=$(zxfer_vm_guest_prepare_script freebsd qemu shunit2)

	assertContains "FreeBSD shunit2 guest preparation should install bash for the coverage fallback suite." \
		"$script_body" "pkg install -y bash"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_prepare_script_installs_perf_tools_for_freebsd_perf() {
	script_body=$(zxfer_vm_guest_prepare_script freebsd qemu perf)

	assertContains "FreeBSD perf guest preparation should install parallel and zstd for perf fixtures." \
		"$script_body" "pkg install -y parallel zstd"
	assertContains "FreeBSD perf guest preparation should load the ZFS module when needed." \
		"$script_body" "kldload zfs || true"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_guest_prepare_script_installs_bash_for_omnios_shunit2() {
	script_body=$(zxfer_vm_guest_prepare_script omnios qemu shunit2)

	assertContains "OmniOS shunit2 guest preparation should install bash for the POSIX wrapper path." \
		"$script_body" "pkg install bash"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_run_selected_guests_parallel_propagates_failures() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_BACKEND=qemu
		ZXFER_VM_ARTIFACT_ROOT=\"$TEST_TMPDIR/artifacts\"
		ZXFER_VM_SELECTED_GUESTS='ubuntu freebsd'
		ZXFER_VM_JOBS=2
		zxfer_vm_backend_run_guest() {
			l_guest=\$1
			l_artifact_dir=\$2
			mkdir -p \"\$l_artifact_dir\"
			case \"\$l_guest\" in
			ubuntu)
				sleep 1
				return 0
				;;
			freebsd)
				sleep 1
				return 3
				;;
			esac
		}
		if zxfer_vm_run_selected_guests; then
			printf 'status=0\n'
		else
			printf 'status=%s\n' \"\$?\"
		fi
		printf 'failed=%s\n' \"\$ZXFER_VM_FAILED_GUESTS\"
	"

	assertEquals "Parallel guest execution should preserve nonzero guest status." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The parallel scheduler should surface the failing guest exit status." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=3"
	assertContains "The parallel scheduler should record the failing guest in the summary state." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "failed=freebsd"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_handle_signal_stops_background_guests_and_exits_with_signal_status() {
	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_ACTIVE_BACKGROUND_PIDS='101 202'
		zxfer_vm_stop_active_background_guests() {
			printf 'stop=%s:%s\\n' \"\$1\" \"\$ZXFER_VM_ACTIVE_BACKGROUND_PIDS\"
		}
		zxfer_vm_wait_for_active_background_guests() {
			printf 'wait=%s\\n' \"\$ZXFER_VM_ACTIVE_BACKGROUND_PIDS\"
			ZXFER_VM_ACTIVE_BACKGROUND_PIDS=
		}
		zxfer_vm_cleanup_runner_state() {
			printf '%s\\n' 'cleanup=1'
		}
		zxfer_vm_handle_signal INT
	"

	assertEquals "Signal handling should exit with the conventional SIGINT status." \
		130 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Signal handling should stop the tracked guest workers with TERM." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "stop=TERM:101 202"
	assertContains "Signal handling should wait for the tracked guest workers before exiting." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "wait=101 202"
	assertContains "Signal handling should still clean up runner state before exiting." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "cleanup=1"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_run_command_with_captured_output_streams_when_enabled() {
	stdout_capture="$TEST_TMPDIR/stream.capture.stdout"
	stderr_capture="$TEST_TMPDIR/stream.capture.stderr"

	zxfer_test_capture_subshell_split "$stdout_capture" "$stderr_capture" "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_STREAM_GUEST_OUTPUT=1
		zxfer_vm_run_command_with_captured_output \
			\"$TEST_TMPDIR/stream.stdout\" \
			\"$TEST_TMPDIR/stream.stderr\" \
			'[stream stdout]' \
			'[stream stderr]' \
			sh -c 'printf \"%s\\n\" stdout-line; printf \"%s\\n\" stderr-line >&2'
		printf 'stdout-file=%s\n' \"\$(cat \"$TEST_TMPDIR/stream.stdout\")\"
		printf 'stderr-file=%s\n' \"\$(cat \"$TEST_TMPDIR/stream.stderr\")\"
	"

	assertEquals "Streaming guest output should not fail the helper on successful commands." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The helper should mirror stdout lines to the console with a prefix." \
		"$(cat "$stdout_capture")" "[stream stdout] stdout-line"
	assertContains "The helper should mirror stderr lines to the console with a prefix." \
		"$(cat "$stderr_capture")" "[stream stderr] stderr-line"
	assertContains "The helper should still write stdout to the artifact file." \
		"$(cat "$stdout_capture")" "stdout-file=stdout-line"
	assertContains "The helper should still write stderr to the artifact file." \
		"$(cat "$stdout_capture")$(cat "$stderr_capture")" "stderr-file=stderr-line"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_main_does_not_fall_back_to_direct_host_harness_when_qemu_is_missing() {
	FAKE_BIN_DIR="$TEST_TMPDIR/fake-bin"
	mkdir -p "$FAKE_BIN_DIR"
	cat <<'EOF' >"$FAKE_BIN_DIR/uname"
#!/bin/sh
case "$1" in
-s) printf '%s\n' "Linux" ;;
-m) printf '%s\n' "x86_64" ;;
*) printf '%s\n' "Linux" ;;
esac
EOF
	chmod +x "$FAKE_BIN_DIR/uname"
	for cmd in curl python3 qemu-img ssh ssh-keygen ssh-keyscan tar; do
		cat <<'EOF' >"$FAKE_BIN_DIR/$cmd"
#!/bin/sh
exit 0
EOF
		chmod +x "$FAKE_BIN_DIR/$cmd"
	done

	zxfer_test_capture_subshell "
		PATH=\"$FAKE_BIN_DIR:/usr/bin:/bin\"
		ZXFER_VM_UNAME_S=Linux
		ZXFER_VM_UNAME_M=x86_64
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_main --profile smoke --backend qemu
	"

	assertEquals "The runner should fail when qemu-system-x86_64 is unavailable." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The error should identify the missing qemu host dependency." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Required command not found: qemu-system-x86_64"
	assertNotContains "The VM runner must not silently fall back to the direct host integration harness." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "run_integration_zxfer.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_main_requires_qemu_system_aarch64_for_arm64_guests() {
	FAKE_BIN_DIR="$TEST_TMPDIR/fake-bin-arm64"
	mkdir -p "$FAKE_BIN_DIR"
	cat <<'EOF' >"$FAKE_BIN_DIR/uname"
#!/bin/sh
case "$1" in
-s) printf '%s\n' "Darwin" ;;
-m) printf '%s\n' "arm64" ;;
*) printf '%s\n' "Darwin" ;;
esac
EOF
	chmod +x "$FAKE_BIN_DIR/uname"
	for cmd in curl python3 qemu-img ssh ssh-keygen ssh-keyscan tar; do
		cat <<'EOF' >"$FAKE_BIN_DIR/$cmd"
#!/bin/sh
exit 0
EOF
		chmod +x "$FAKE_BIN_DIR/$cmd"
	done

	zxfer_test_capture_subshell "
		PATH=\"$FAKE_BIN_DIR:/usr/bin:/bin\" \"$VM_MATRIX_BIN\" --profile smoke --backend qemu
	"

	assertEquals "Apple Silicon smoke runs should require qemu-system-aarch64 for arm64 guests." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The missing arm64 qemu dependency should be reported directly." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Required command not found: qemu-system-aarch64"
	assertNotContains "The VM runner must not silently fall back to the direct host integration harness." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "run_integration_zxfer.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_aarch64_efi_prefers_explicit_override() {
	firmware_file="$TEST_TMPDIR/QEMU_EFI.fd"
	printf '%s\n' "firmware" >"$firmware_file"
	ZXFER_VM_QEMU_AARCH64_EFI=$firmware_file

	assertEquals "An explicit arm64 EFI override should take precedence over auto-detection." \
		"$firmware_file" "$(zxfer_vm_qemu_resolve_aarch64_efi)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_machine_arg_keeps_highmem_enabled_for_arm64_guests() {
	assertEquals "Apple Silicon arm64 guests should keep QEMU's default highmem layout enabled." \
		"virt,accel=hvf" "$(zxfer_vm_qemu_machine_arg arm64 hvf)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vm_qemu_write_repo_archive_disables_macos_metadata_flags() {
	fake_bin_dir="$TEST_TMPDIR/fake-bin-tar"
	recorded_args="$TEST_TMPDIR/tar-args.txt"
	mkdir -p "$fake_bin_dir"
	cat <<EOF >"$fake_bin_dir/tar"
#!/bin/sh
printf '%s\n' "\$*" >"$recorded_args"
exit 0
EOF
	chmod +x "$fake_bin_dir/tar"

	zxfer_test_capture_subshell "
		. \"$VM_MATRIX_LIB\"
		zxfer_vm_reset_state
		ZXFER_VM_HOST_OS=darwin
		PATH=\"$fake_bin_dir:/usr/bin:/bin\"
		zxfer_vm_qemu_write_repo_archive >/dev/null
	"

	assertEquals "The macOS archive helper should succeed with the expected tar flags." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The macOS archive helper should disable Apple metadata entries explicitly." \
		"$(cat "$recorded_args")" "--no-mac-metadata"
	assertContains "The macOS archive helper should disable xattrs explicitly." \
		"$(cat "$recorded_args")" "--no-xattrs"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
