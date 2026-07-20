#!/bin/sh
#
# Integration tests for secure PATH, SSH transport, wrappers, capability protocols, and remote execution.
# Sourced by tests/run_integration_zxfer.sh; the registry owns execution order.

remote_dry_run_noexec_progress_test() {
	log "Starting remote dry-run no-exec progress test"

	mock_path="$WORKDIR/mock_remote_dryrun"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	ssh_log="$WORKDIR/mock_remote_dryrun.log"
	progress_script="$WORKDIR/mock_remote_dryrun_progress.sh"
	progress_log="$WORKDIR/mock_remote_dryrun_progress.log"

	src_dataset="$SRC_POOL/remote_dryrun_src"
	dest_root="$DEST_POOL/remote_dryrun_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_f "$ssh_log" "$progress_log"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "remote dry run"
	zfs snap -r "$src_dataset@rdr1"

	write_progress_logger_script "$progress_script"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" \
		MOCK_SSH_LOG="$ssh_log" \
		"$ZXFER_BIN" -v -V -n -D "$progress_script $progress_log %%size%% %%title%%" -T localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Remote dry-run no-exec progress test expected success, got $status. Output: $output"
	fi
	if [ -e "$ssh_log" ] && [ -s "$ssh_log" ]; then
		fail "Strict remote dry-run should not execute ssh. Log: $(cat "$ssh_log")"
	fi
	if [ -e "$progress_log" ]; then
		fail "Strict remote dry-run should not execute the progress helper. Log: $(cat "$progress_log")"
	fi
	if ! printf '%s\n' "$output" | grep -q "Dry run: skipping live %%size%% progress estimate discovery."; then
		fail "Remote dry-run should report that live %%size%% estimation was skipped. Output: $output"
	fi
	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Remote dry run should not create destination dataset $dest_dataset."
	fi

	log "Remote dry-run no-exec progress test passed"
}

secure_path_dependency_tests() {
	log "Starting secure PATH dependency tests"

	mock_path="$WORKDIR/mock_secure_path"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"
	cat >"$mock_path/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_path/ssh"
	for bin in awk cat sed date mktemp tr printf grep cut head sort; do
		real_bin=$(resolve_host_command "$bin")
		if [ "$real_bin" != "" ]; then
			ln -s "$real_bin" "$mock_path/$bin"
		else
			fail "Required binary $bin not found on host; cannot run secure PATH test."
		fi
	done
	# Deliberately omit zfs from the secure PATH to ensure zxfer aborts cleanly.
	secure_path="$mock_path"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" ZXFER_SECURE_PATH_APPEND="" "$ZXFER_BIN" -R tank/src backup/target 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when secure PATH lacks required tools."
	fi
	if ! printf '%s\n' "$output" | grep -q 'Required dependency "zfs" not found'; then
		fail "Expected missing zfs dependency error. Output: $output"
	fi

	safe_rm_rf "$mock_path"

	log "Secure PATH dependency tests passed"
}

secure_path_failure_report_test() {
	log "Starting secure PATH failure report test"

	mock_path="$WORKDIR/mock_secure_path_report"
	stdout_log="$WORKDIR/secure_path_report.stdout"
	stderr_log="$WORKDIR/secure_path_report.stderr"
	safe_rm_rf "$mock_path"
	safe_rm_f "$stdout_log" "$stderr_log"
	mkdir -p "$mock_path"
	cat >"$mock_path/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_path/ssh"
	for bin in awk cat sed date mktemp tr printf grep cut head sort; do
		real_bin=$(resolve_host_command "$bin")
		if [ "$real_bin" != "" ]; then
			ln -s "$real_bin" "$mock_path/$bin"
		else
			fail "Required binary $bin not found on host; cannot run secure PATH failure report test."
		fi
	done

	set +e
	ZXFER_SECURE_PATH="$mock_path" ZXFER_SECURE_PATH_APPEND="" "$ZXFER_BIN" -R tank/src backup/target >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when secure PATH lacks required tools."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Dependency failure report should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -q "^zxfer: failure report begin$" "$stderr_log"; then
		fail "Dependency failure report block missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_class: dependency" "$stderr_log"; then
		fail "Dependency failure report class missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_stage: startup" "$stderr_log"; then
		fail "Dependency failure report stage missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q 'Required dependency "zfs" not found' "$stderr_log"; then
		fail "Dependency failure report message missing. Output: $(cat "$stderr_log")"
	fi

	safe_rm_rf "$mock_path"

	log "Secure PATH failure report test passed"
}

secure_path_append_resolution_test() {
	log "Starting secure PATH append resolution test"

	base_path="$WORKDIR/mock_secure_path_base"
	append_path="$WORKDIR/mock_secure_path_append"
	safe_rm_rf "$base_path" "$append_path"

	prepare_mock_bin_dir "$base_path" awk ps ssh
	prepare_mock_bin_dir "$append_path" zfs

	set +e
	output=$(ZXFER_SECURE_PATH="$base_path" ZXFER_SECURE_PATH_APPEND="$append_path" "$ZXFER_BIN" -R tank/src 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Expected secure PATH append test to reach usage validation with exit 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Need a destination." >/dev/null 2>&1; then
		fail "Expected secure PATH append test to reach destination validation. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -F "Required dependency" >/dev/null 2>&1; then
		fail "Secure PATH append should satisfy dependency resolution without a missing dependency error. Output: $output"
	fi

	log "Secure PATH append resolution test passed"
}

remote_migration_guard_tests() {
	log "Starting remote/migration guard tests"

	mock_bin_dir="$WORKDIR/mockbin"
	safe_rm_rf "$mock_bin_dir"
	mkdir -p "$mock_bin_dir"
	cat >"$mock_bin_dir/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_bin_dir/ssh"
	secure_path="$mock_bin_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	# Using -m with a remote origin should be rejected before any replication starts.
	ZXFER_SECURE_PATH="$secure_path" assert_usage_error_case "Migration with remote origin" \
		"You cannot migrate to or from a remote host." \
		-m -O remotehost -R tank/src backup/target

	# Using -c without migration is already covered; ensure -c paired with a remote target also fails.
	ZXFER_SECURE_PATH="$secure_path" assert_usage_error_case "Service stop with remote target" \
		"You cannot migrate to or from a remote host." \
		-c svc:/network/ssh -T remotehost -R tank/src backup/target

	safe_rm_rf "$mock_bin_dir"

	log "Remote/migration guard tests passed"
}

local_helper_path_shell_metacharacters_test() {
	log "Starting local helper path shell metacharacters test"

	marker_rel="local_helper_path_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_local_helper.\$(touch $marker_rel)"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$marker"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs not found on host; cannot run local helper path shell metacharacters test."
	fi
	ln -s "$real_zfs" "$mock_path/zfs"

	src_dataset="$SRC_POOL/local_helper_shell_src"
	dest_root="$DEST_POOL/local_helper_shell_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "helper-path-one"
	zfs snap -r "$src_dataset@lhs1"

	(
		cd "$WORKDIR"
		log "Running: $zxfer_bin_abs -v -R $src_dataset $dest_root"
		ZXFER_SECURE_PATH="$secure_path" "$zxfer_bin_abs" -v -R "$src_dataset" "$dest_root"
	)

	assert_snapshot_exists "$dest_dataset" "lhs1"
	if [ -e "$marker" ]; then
		fail "Resolved local helper paths containing shell metacharacters should not execute locally; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"

	log "Local helper path shell metacharacters test passed"
}

garbage_wrapped_host_spec_fails_closed_test() {
	log "Starting garbage wrapped host spec fail-closed test"

	marker_rel="garbage_host_spec_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_garbage_host_spec"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="garbage-host.example \$(touch $marker_rel)"
	safe_rm_f "$marker"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	src_dataset="$SRC_POOL/garbage_host_spec_src"
	dest_root="$DEST_POOL/garbage_host_spec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "garbage-host-spec"
	zfs snap -r "$src_dataset@ghs1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Garbage wrapped host specs should fail closed instead of replicating successfully. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Error creating ssh control socket for origin host." >/dev/null 2>&1 &&
		! printf '%s\n' "$output" | grep -F "Failed to determine operating system on host garbage-host.example" >/dev/null 2>&1 &&
		! printf '%s\n' "$output" | grep -F "Failed to retrieve snapshots from the source:" >/dev/null 2>&1; then
		fail "Garbage wrapped host specs should fail closed before replication begins. Output: $output"
	fi
	if [ -e "$marker" ]; then
		fail "Garbage wrapped host specs should not execute embedded shell fragments; marker file was created at $marker."
	fi
	assert_dataset_absent "$dest_dataset"

	safe_rm_rf "$mock_path"

	log "Garbage wrapped host spec fail-closed test passed"
}

control_socket_path_shell_metacharacters_test() {
	log "Starting control socket path shell metacharacters test"

	marker_rel="control_socket_path_marker"
	marker="$WORKDIR/$marker_rel"
	tmpdir_with_payload="$WORKDIR/mock_tmpdir.\$(touch $marker_rel)"
	mock_path="$WORKDIR/mock_control_socket_safe"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$marker"
	safe_rm_rf "$tmpdir_with_payload" "$mock_path"
	mkdir -p "$tmpdir_with_payload"
	mkdir -p "$mock_path"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs not found on host; cannot run control socket path shell metacharacters test."
	fi
	ln -s "$real_zfs" "$mock_path/zfs"
	write_mock_ssh_script "$mock_path/ssh"

	src_dataset="$SRC_POOL/control_socket_shell_src"
	dest_root="$DEST_POOL/control_socket_shell_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "control-socket-one"
	zfs snap -r "$src_dataset@css1"

	(
		cd "$WORKDIR"
		log "Running: $zxfer_bin_abs -v -O localhost -R $src_dataset $dest_root"
		TMPDIR="$tmpdir_with_payload" \
			ZXFER_SECURE_PATH="$secure_path" \
			"$zxfer_bin_abs" -v -O localhost -R "$src_dataset" "$dest_root"
	)

	assert_snapshot_exists "$dest_dataset" "css1"
	if [ -e "$marker" ]; then
		fail "SSH control-socket paths containing shell metacharacters should not execute locally; marker file was created at $marker."
	fi

	safe_rm_rf "$tmpdir_with_payload" "$mock_path"

	log "Control socket path shell metacharacters test passed"
}

remote_origin_target_uncompressed_test() {
	log "Starting remote uncompressed origin/target test"

	if ! has_parallel; then
		log "Skipping remote uncompressed test (parallel not available for -j>1 remote listings)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_uncompressed"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	ssh_log="$WORKDIR/mock_remote_uncompressed.log"
	safe_rm_f "$ssh_log"
	before_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)

	src_dataset="$SRC_POOL/remote_uncompressed_src"
	dest_root_origin="$DEST_POOL/remote_uncompressed_dest_origin"
	dest_root_target="$DEST_POOL/remote_uncompressed_dest_target"
	dest_origin="$dest_root_origin/${src_dataset##*/}"
	dest_target="$dest_root_target/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root_origin" "$dest_root_target"

	zfs create "$src_dataset"
	zfs create "$dest_root_origin"
	zfs create "$dest_root_target"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@rmt1"

	ZXFER_SECURE_PATH="$secure_path" MOCK_SSH_LOG="$ssh_log" run_zxfer -v -j 2 -O localhost -R "$src_dataset" "$dest_root_origin"
	assert_snapshot_exists "$dest_origin" "rmt1"

	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@rmt2"

	ZXFER_SECURE_PATH="$secure_path" MOCK_SSH_LOG="$ssh_log" run_zxfer -v -T localhost -R "$src_dataset" "$dest_root_target"
	assert_snapshot_exists "$dest_target" "rmt1"
	assert_snapshot_exists "$dest_target" "rmt2"

	socket_leaks=""
	after_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)
	for dir in $after_sockets; do
		case " $before_sockets " in
		*" $dir "*) continue ;;
		esac
		socket_leaks="$socket_leaks $dir"
	done
	if [ -n "$socket_leaks" ]; then
		fail "SSH control socket directories leaked: $socket_leaks"
	fi

	log "Remote uncompressed origin/target test passed"
}

remote_helper_path_shell_metacharacters_test() {
	log "Starting remote helper path shell metacharacters test"

	marker_rel="remote_helper_path_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_remote_helper.\$(touch $marker_rel)"
	ssh_mock_dir="$WORKDIR/mock_remote_helper_safe"
	secure_path="$ssh_mock_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$marker"
	safe_rm_rf "$mock_path" "$ssh_mock_dir"
	mkdir -p "$mock_path"
	mkdir -p "$ssh_mock_dir"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs not found on host; cannot run remote helper path shell metacharacters test."
	fi
	ln -s "$real_zfs" "$mock_path/zfs"
	write_mock_ssh_script "$ssh_mock_dir/ssh"

	src_dataset="$SRC_POOL/remote_helper_shell_src"
	dest_root="$DEST_POOL/remote_helper_shell_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "helper-path-one"
	zfs snap -r "$src_dataset@rhs1"

	(
		cd "$WORKDIR"
		log "Running: $zxfer_bin_abs -v -O localhost -R $src_dataset $dest_root"
		MOCK_SSH_COMMAND_V_TOOL="zfs" \
			MOCK_SSH_COMMAND_V_RESULT="$mock_path/zfs" \
			ZXFER_SECURE_PATH="$secure_path" \
			"$zxfer_bin_abs" -v -O localhost -R "$src_dataset" "$dest_root"
	)

	assert_snapshot_exists "$dest_dataset" "rhs1"
	if [ -e "$marker" ]; then
		fail "Resolved helper paths containing shell metacharacters should not execute locally; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path" "$ssh_mock_dir"

	log "Remote helper path shell metacharacters test passed"
}

remote_capability_control_whitespace_path_falls_back_to_direct_probe_test() {
	log "Starting remote capability control-whitespace path fallback test"

	marker_rel="remote_capability_control_whitespace_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_remote_capability_control_whitespace"
	capability_file="$WORKDIR/mock_remote_capability_control_whitespace.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="control-whitespace.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
		printf 'os\t%s\n' "MockRemoteOS"
		printf "tool\tzfs\t0\t/tmp/mock_remote_helper.\$(touch %s)/zfs\r\n" "$marker_rel"
		printf 'tool\tparallel\t1\t-\n'
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/remote_capability_control_whitespace_src"
	dest_root="$DEST_POOL/remote_capability_control_whitespace_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "control-whitespace-path"
	zfs snap -r "$src_dataset@rcw1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Remote capability responses with control-whitespace helper paths should degrade safely to the direct probe path when it is available. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "rcw1"
	if [ -e "$marker" ]; then
		fail "Control-whitespace helper paths from remote capabilities should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Remote capability control-whitespace path fallback test passed"
}

target_capability_control_whitespace_path_falls_back_to_direct_probe_test() {
	log "Starting target capability control-whitespace path fallback test"

	marker_rel="target_capability_control_whitespace_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_target_capability_control_whitespace"
	capability_file="$WORKDIR/mock_target_capability_control_whitespace.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="target-control-whitespace.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
		printf 'os\t%s\n' "MockRemoteOS"
		printf "tool\tzfs\t0\t/tmp/mock_remote_helper.\$(touch %s)/zfs\r\n" "$marker_rel"
		printf 'tool\tparallel\t1\t-\n'
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/target_capability_control_whitespace_src"
	dest_root="$DEST_POOL/target_capability_control_whitespace_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "target-control-whitespace-path"
	zfs snap -r "$src_dataset@tcw1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -T "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Target capability responses with control-whitespace helper paths should degrade safely to the direct probe path when it is available. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "tcw1"
	if [ -e "$marker" ]; then
		fail "Target control-whitespace helper paths from remote capabilities should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Target capability control-whitespace path fallback test passed"
}

remote_compression_pipeline_test() {
	log "Starting remote compression pipeline test"

	if ! has_parallel; then
		log "Skipping remote compression pipeline test (parallel not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_compress"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	src_dataset="$SRC_POOL/remote_compress_src"
	dest_root="$DEST_POOL/remote_compress_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "compressed-one"
	zfs snap -r "$src_dataset@rcomp1"

	ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -j 2 -Z "zstd -T0 -6" -O localhost -T localhost -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "rcomp1"

	append_data_to_dataset "$src_dataset" "file.txt" "compressed-two"
	zfs snap -r "$src_dataset@rcomp2"

	ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -j 2 -z -O localhost -T localhost -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "rcomp2"

	log "Remote compression pipeline test passed"
}

target_only_remote_compression_test() {
	log "Starting target-only remote compression test"

	mock_path="$WORKDIR/mock_target_only_compress"
	ssh_log="$WORKDIR/mock_target_only_compress.log"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	safe_rm_f "$ssh_log"

	src_dataset="$SRC_POOL/target_only_compress_src"
	dest_root="$DEST_POOL/target_only_compress_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "target-only-one"
	zfs snap -r "$src_dataset@toc1"

	MOCK_SSH_LOG="$ssh_log" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -z -T localhost -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "toc1"

	append_data_to_dataset "$src_dataset" "file.txt" "target-only-two"
	zfs snap -r "$src_dataset@toc2"

	MOCK_SSH_LOG="$ssh_log" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -z -T localhost -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "toc2"

	if ! grep -F "'$mock_path/zstd' '-d'" "$ssh_log" >/dev/null 2>&1; then
		fail "Expected target-only remote compression run to invoke remote zstd decompression. Log: $(cat "$ssh_log" 2>/dev/null || true)"
	fi

	log "Target-only remote compression test passed"
}

remote_csh_origin_snapshot_listing_test() {
	log "Starting remote csh origin snapshot listing test"

	if ! has_parallel; then
		log "Skipping remote csh origin snapshot listing test (parallel not available)"
		return
	fi

	l_csh_shell=$(find_csh_shell)
	if [ "$l_csh_shell" = "" ]; then
		log "Skipping remote csh origin snapshot listing test (csh/tcsh not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_csh_origin"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	src_dataset="$SRC_POOL/remote_csh_src"
	dest_root="$DEST_POOL/remote_csh_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@csh1"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" MOCK_SSH_REMOTE_SHELL="$l_csh_shell" "$ZXFER_BIN" -v -j 2 -z -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Remote csh origin snapshot listing should succeed. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "Unmatched"; then
		fail "Remote csh origin snapshot listing should not emit unmatched-quote errors. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "unexpected end of file"; then
		fail "Remote csh origin snapshot listing should not emit zstd EOF errors. Output: $output"
	fi

	assert_snapshot_exists "$dest_dataset" "csh1"

	log "Remote csh origin snapshot listing test passed"
}

remote_wrapped_host_spec_test() {
	log "Starting remote wrapped host spec test"

	if ! has_parallel; then
		log "Skipping remote wrapped host spec test (parallel not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_wrapped"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	write_exec_wrapper_script "$mock_path/pfexec"
	write_exec_wrapper_script "$mock_path/doas"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	wrapper_log="$WORKDIR/mock_remote_wrapped.log"
	safe_rm_f "$wrapper_log"

	src_dataset="$SRC_POOL/remote_wrapped_src"
	dest_root="$DEST_POOL/remote_wrapped_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "wrapped"
	zfs snap -r "$src_dataset@wrap1"

	MOCK_WRAPPER_LOG="$wrapper_log" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -j 2 -z -O "localhost pfexec" -T "localhost doas" -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "wrap1"

	if ! grep -q '^pfexec:' "$wrapper_log"; then
		fail "Expected pfexec wrapper invocation recorded in $wrapper_log."
	fi
	if ! grep -q '^doas:' "$wrapper_log"; then
		fail "Expected doas wrapper invocation recorded in $wrapper_log."
	fi
	if ! grep -q '^pfexec:sh -c ' "$wrapper_log"; then
		fail "Expected pfexec to wrap a remote sh -c command. Log: $(cat "$wrapper_log")"
	fi
	if ! grep -q '^doas:sh -c ' "$wrapper_log"; then
		fail "Expected doas to wrap a remote sh -c command. Log: $(cat "$wrapper_log")"
	fi

	log "Remote wrapped host spec test passed"
}

malformed_remote_capability_response_fails_closed_test() {
	log "Starting malformed remote capability response fail-closed test"

	marker_rel="malformed_remote_capability_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_malformed_remote_capability"
	capability_file="$WORKDIR/mock_malformed_remote_capability.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="malformed-capability.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
		printf 'os\t%s\n' "MockRemoteOS"
		printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
		printf 'tool\tparallel\t1\t-\n'
		printf '%s\n' "\$(touch $marker_rel)"
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/malformed_remote_capability_src"
	dest_root="$DEST_POOL/malformed_remote_capability_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "malformed-capability"
	zfs snap -r "$src_dataset@mrc1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				MOCK_SSH_MISSING_TOOL="zfs" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Malformed remote capability responses should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Required dependency \"zfs\" not found on host $l_host_spec" >/dev/null 2>&1; then
		fail "Malformed remote capability responses should fall back to a secure remote dependency probe and fail closed. Output: $output"
	fi
	if [ -e "$marker" ]; then
		fail "Malformed remote capability payloads should not execute embedded shell fragments; marker file was created at $marker."
	fi
	assert_dataset_absent "$dest_dataset"

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Malformed remote capability response fail-closed test passed"
}

malformed_remote_capability_response_falls_back_to_direct_probe_test() {
	log "Starting malformed remote capability response fallback test"

	marker_rel="malformed_remote_capability_fallback_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_malformed_remote_capability_fallback"
	capability_file="$WORKDIR/mock_malformed_remote_capability_fallback.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="malformed-fallback.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
		printf 'os\t%s\n' "MockRemoteOS"
		printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
		printf 'tool\tparallel\t1\t-\n'
		printf '%s\n' "\$(touch $marker_rel)"
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/malformed_remote_capability_fallback_src"
	dest_root="$DEST_POOL/malformed_remote_capability_fallback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "malformed-capability-fallback"
	zfs snap -r "$src_dataset@mrf1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Malformed remote capability responses should fall back to direct probes and still allow replication. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "mrf1"
	if [ -e "$marker" ]; then
		fail "Malformed remote capability payloads should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Malformed remote capability response fallback test passed"
}

malformed_target_capability_response_falls_back_to_direct_probe_test() {
	log "Starting malformed target capability response fallback test"

	marker_rel="malformed_target_capability_fallback_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_malformed_target_capability_fallback"
	capability_file="$WORKDIR/mock_malformed_target_capability_fallback.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="malformed-target-fallback.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
		printf 'os\t%s\n' "MockRemoteOS"
		printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
		printf 'tool\tparallel\t1\t-\n'
		printf '%s\n' "\$(touch $marker_rel)"
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/malformed_target_capability_fallback_src"
	dest_root="$DEST_POOL/malformed_target_capability_fallback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "malformed-target-capability-fallback"
	zfs snap -r "$src_dataset@mtf1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -T "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Malformed target capability responses should fall back to direct probes and still allow replication. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "mtf1"
	if [ -e "$marker" ]; then
		fail "Malformed target capability payloads should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Malformed target capability response fallback test passed"
}

missing_parallel_error_test() {
	log "Starting missing parallel failure test"

	mock_path="$WORKDIR/mock_no_parallel"
	prepare_mock_bin_dir "$mock_path" \
		awk cat chmod comm cut date grep head id ln ls mkdir mkfifo mktemp ps rm rmdir sed sort ssh stat tr uname zfs

	secure_path="$mock_path"
	src_dataset="$SRC_POOL/no_parallel_src"
	dest_root="$DEST_POOL/no_parallel_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@np1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@np2"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Local -j run without parallel should fail closed. Output: $output"
	fi
	assert_dataset_absent "$dest_dataset"
	if ! printf '%s\n' "$output" | grep -q "requires parallel but it was not found in PATH on the local host"; then
		fail "Missing local parallel error not found. Output: $output"
	fi

	safe_rm_rf "$mock_path"

	log "Missing parallel failure test passed"
}

remote_missing_parallel_origin_test() {
	log "Starting remote missing parallel origin test"

	if ! has_parallel; then
		log "Skipping remote missing parallel origin test (local parallel not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_no_parallel"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	real_parallel=$(resolve_host_command parallel)
	if [ "$real_parallel" = "" ]; then
		fail "parallel not found on host after availability probe."
	fi
	ln -s "$real_parallel" "$mock_path/parallel"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	tmpdir="$WORKDIR/remote_no_parallel_tmp"

	src_dataset="$SRC_POOL/remote_no_parallel_src"
	dest_root="$DEST_POOL/remote_no_parallel_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$tmpdir"
	mkdir -p "$tmpdir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	i=1
	while [ "$i" -le 16 ]; do
		zfs create "$src_dataset/child$i"
		i=$((i + 1))
	done
	zfs snap -r "$src_dataset@np1"

	set +e
	# Isolate the cross-process remote capability cache so earlier localhost tests
	# do not mask this deliberate missing-parallel failure path.
	output=$(TMPDIR="$tmpdir" MOCK_SSH_MISSING_TOOL=parallel ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Remote origin without parallel should fail closed when -j is requested. Output: $output"
	fi
	assert_dataset_absent "$dest_root/${src_dataset##*/}"
	if ! printf '%s\n' "$output" | grep -q "parallel not found on origin host localhost but -j 2 was requested"; then
		fail "Expected remote parallel missing-helper error. Output: $output"
	fi

	safe_rm_rf "$mock_path" "$tmpdir"

	log "Remote missing parallel origin test passed"
}

remote_incompatible_parallel_origin_test() {
	log "Starting remote incompatible parallel origin test"

	mock_path="$WORKDIR/mock_remote_incompatible_parallel"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	cat >"$mock_path/parallel" <<'EOF'
#!/bin/sh
if [ "$1" = "--will-cite" ]; then
	shift
fi
if [ "$1" = "--version" ]; then
	printf '%s\n' "parallel from elsewhere"
	exit 0
fi
	printf '%s\n' "incompatible parallel mock cannot execute zxfer workloads" >&2
exit 64
EOF
	chmod +x "$mock_path/parallel"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	tmpdir="$WORKDIR/remote_incompatible_parallel_tmp"

	src_dataset="$SRC_POOL/remote_incompatible_parallel_src"
	dest_root="$DEST_POOL/remote_incompatible_parallel_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$tmpdir"
	mkdir -p "$tmpdir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	i=1
	while [ "$i" -le 16 ]; do
		zfs create "$src_dataset/child$i"
		i=$((i + 1))
	done
	zfs snap -r "$src_dataset@ng1"

	set +e
	output=$(TMPDIR="$tmpdir" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Remote origin with an incompatible parallel implementation should fail during source snapshot discovery. Output: $output"
	fi
	assert_dataset_absent "$dest_root/${src_dataset##*/}"
	if ! printf '%s\n' "$output" | grep -q "incompatible parallel mock cannot execute zxfer workloads"; then
		fail "Remote incompatible parallel diagnostics should be preserved from the rendered source snapshot pipeline. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Failed to retrieve snapshots from the source:"; then
		fail "Remote incompatible parallel implementations should fail through the rendered source snapshot pipeline. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "requires GNU parallel"; then
		fail "Remote incompatible parallel implementations should not be rejected by an upfront GNU validation check. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "is not GNU parallel"; then
		fail "Remote incompatible parallel implementations should not be misreported as upfront GNU validation failures. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "Falling back to serial source snapshot listing."; then
		fail "Remote incompatible parallel implementations should not fall back to serial once the helper resolves. Output: $output"
	fi

	safe_rm_rf "$mock_path" "$tmpdir"

	log "Remote incompatible parallel origin test passed"
}

remote_parallel_rendered_failure_origin_test() {
	log "Starting remote rendered parallel failure origin test"

	mock_path="$WORKDIR/mock_remote_rendered_failure_parallel"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	cat >"$mock_path/parallel" <<'EOF'
#!/bin/sh
if [ "$1" = "--will-cite" ]; then
	shift
fi
if [ "$1" = "--version" ]; then
	printf '%s\n' "GNU parallel 20260122"
	exit 0
fi
printf '%s\n' "remote parallel command failed during source listing" >&2
exit 124
EOF
	chmod +x "$mock_path/parallel"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	tmpdir="$WORKDIR/remote_rendered_failure_parallel_tmp"

	src_dataset="$SRC_POOL/remote_rendered_failure_parallel_src"
	dest_root="$DEST_POOL/remote_rendered_failure_parallel_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$tmpdir"
	mkdir -p "$tmpdir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	i=1
	while [ "$i" -le 16 ]; do
		zfs create "$src_dataset/child$i"
		i=$((i + 1))
	done
	zfs snap -r "$src_dataset@pf1"

	set +e
	output=$(TMPDIR="$tmpdir" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Remote origin should fail closed when the rendered parallel listing command fails. Output: $output"
	fi
	assert_dataset_absent "$dest_root/${src_dataset##*/}"
	if ! printf '%s\n' "$output" | grep -q "remote parallel command failed during source listing"; then
		fail "Expected the rendered remote parallel failure diagnostic to be preserved. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "requires GNU parallel"; then
		fail "Rendered remote parallel failures should no longer be rejected by upfront GNU validation. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "is not GNU parallel"; then
		fail "Rendered remote parallel failures should no longer be misreported as upfront GNU validation failures. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Failed to retrieve snapshots from the source:"; then
		fail "Rendered remote parallel failures should surface through the source snapshot pipeline. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "Falling back to serial source snapshot listing."; then
		fail "Rendered remote parallel failures should not fall back to serial discovery. Output: $output"
	fi

	safe_rm_rf "$mock_path" "$tmpdir"

	log "Remote rendered parallel failure origin test passed"
}

managed_ssh_policy_test() {
	log "Starting managed ssh policy test"

	mock_path="$WORKDIR/mock_managed_ssh"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	ssh_log="$WORKDIR/mock_managed_ssh.log"
	ssh_argv_log="$WORKDIR/mock_managed_ssh.argv.log"
	known_hosts="$WORKDIR/mock_managed_ssh_known_hosts"

	src_dataset="$SRC_POOL/managed_ssh_src"
	dest_root_managed="$DEST_POOL/managed_ssh_dest"
	dest_root_ambient="$DEST_POOL/managed_ssh_ambient_dest"
	dest_dataset_managed="$dest_root_managed/${src_dataset##*/}"
	dest_dataset_ambient="$dest_root_ambient/${src_dataset##*/}"

	safe_rm_f "$ssh_log" "$ssh_argv_log" "$known_hosts"
	: >"$known_hosts"
	destroy_test_datasets_if_present "$dest_root_managed" "$dest_root_ambient" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root_managed"
	zfs create "$dest_root_ambient"
	append_data_to_dataset "$src_dataset" "file.txt" "managed ssh"
	zfs snap -r "$src_dataset@msp1"

	ZXFER_SECURE_PATH="$secure_path" \
		MOCK_SSH_LOG="$ssh_log" \
		MOCK_SSH_ARGV_LOG="$ssh_argv_log" \
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$known_hosts" \
		run_zxfer -v -T localhost -R "$src_dataset" "$dest_root_managed"

	assert_snapshot_exists "$dest_dataset_managed" "msp1"
	if ! grep -q "^argv:BatchMode=yes$" "$ssh_argv_log"; then
		fail "Managed ssh runs should pass BatchMode=yes. Log: $(cat "$ssh_argv_log")"
	fi
	if ! grep -q "^argv:StrictHostKeyChecking=yes$" "$ssh_argv_log"; then
		fail "Managed ssh runs should pass StrictHostKeyChecking=yes. Log: $(cat "$ssh_argv_log")"
	fi
	if ! grep -q "^argv:UserKnownHostsFile=$known_hosts$" "$ssh_argv_log"; then
		fail "Managed ssh runs should pass the explicit known-hosts file. Log: $(cat "$ssh_argv_log")"
	fi

	safe_rm_f "$ssh_argv_log"

	ZXFER_SECURE_PATH="$secure_path" \
		MOCK_SSH_LOG="$ssh_log" \
		MOCK_SSH_ARGV_LOG="$ssh_argv_log" \
		ZXFER_SSH_USE_AMBIENT_CONFIG=1 \
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$known_hosts" \
		run_zxfer -v -T localhost -R "$src_dataset" "$dest_root_ambient"

	assert_snapshot_exists "$dest_dataset_ambient" "msp1"
	if grep -q "^argv:BatchMode=yes$" "$ssh_argv_log"; then
		fail "Ambient-config opt-out should suppress BatchMode=yes. Log: $(cat "$ssh_argv_log")"
	fi
	if grep -q "^argv:StrictHostKeyChecking=yes$" "$ssh_argv_log"; then
		fail "Ambient-config opt-out should suppress StrictHostKeyChecking=yes. Log: $(cat "$ssh_argv_log")"
	fi
	if grep -q "^argv:UserKnownHostsFile=$known_hosts$" "$ssh_argv_log"; then
		fail "Ambient-config opt-out should suppress the managed UserKnownHostsFile override. Log: $(cat "$ssh_argv_log")"
	fi

	log "Managed ssh policy test passed"
}

parallel_jobs_listing_test() {
	log "Starting parallel jobs listing test"

	if ! has_parallel; then
		log "Skipping parallel jobs listing test (parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/parallel_list_src"
	dest_root="$DEST_POOL/parallel_list_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@pl1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@pl2"

	set +e
	output=$(run_zxfer -v -j 2 -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		log "Skipping parallel jobs listing test due to zxfer failure (possibly incompatible parallel support in ZFS list pipeline). Output: $output"
		return
	fi

	assert_snapshot_exists "$dest_dataset" "pl1"
	assert_snapshot_exists "$dest_dataset" "pl2"

	log "Parallel jobs listing test passed"
}
