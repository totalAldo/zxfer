#!/bin/sh
# Tests for src/zxfer_dependencies.sh and src/zxfer_ssh_transport.sh local ssh
# resolution, run by tests/test_zxfer_remote_hosts.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_zxfer_local_ssh_resolution_helpers_cover_success_and_failure_paths() {
	output=$(
		(
			set +e
			g_cmd_ssh=""
			zxfer_find_required_tool() {
				if [ "$1" = "ssh" ]; then
					g_zxfer_required_tool_result=$FAKE_SSH_BIN
					return 0
				fi
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_success=%s:%s:%s\n' "$?" "$g_cmd_ssh" "$g_zxfer_resolved_local_ssh_command_result"

			g_cmd_ssh=""
			zxfer_find_required_tool() {
				g_zxfer_required_tool_result="missing ssh"
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_failure=%s:%s\n' "$?" "$g_zxfer_resolved_local_ssh_command_result"
		)
	)

	assertContains "Lazy local ssh resolution should cache the resolved ssh helper on success." \
		"$output" "ensure_success=0:$FAKE_SSH_BIN:$FAKE_SSH_BIN"
	assertContains "Lazy local ssh resolution should preserve the dependency diagnostic when ssh lookup fails." \
		"$output" "ensure_failure=1:missing ssh"
}

test_zxfer_find_required_tool_reports_missing_dependency() {
	empty_path="$TEST_TMPDIR/empty_path"
	mkdir -p "$empty_path"
	g_zxfer_secure_path="$empty_path"

	zxfer_find_required_tool definitely_missing "missing-tool"
	status=$?

	assertEquals "Missing dependencies should fail lookup." 1 "$status"
	assertEquals "Missing dependencies should mention the secure PATH guidance." \
		"Required dependency \"missing-tool\" not found in secure PATH ($empty_path). Set ZXFER_SECURE_PATH or install the binary." \
		"$g_zxfer_required_tool_result"
}

test_zxfer_find_required_tool_ignores_shell_functions() {
	empty_path="$TEST_TMPDIR/function_only_path"
	mkdir -p "$empty_path"

	set +e
	result=$(
		(
			mocktool() {
				:
			}
			g_zxfer_secure_path="$empty_path"
			zxfer_find_required_tool mocktool "mocktool"
			l_status=$?
			printf '%s\n' "$g_zxfer_required_tool_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "A shell function must never satisfy a helper lookup." 1 "$status"
	assertEquals "A name that is only a shell function should be reported as missing from the secure PATH." \
		"Required dependency \"mocktool\" not found in secure PATH ($empty_path). Set ZXFER_SECURE_PATH or install the binary." \
		"$result"
}

test_zxfer_find_required_tool_returns_absolute_path_from_secure_path() {
	tool_dir="$TEST_TMPDIR/required_tool_path"
	mkdir -p "$tool_dir"
	cat >"$tool_dir/mocktool" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$tool_dir/mocktool"
	g_zxfer_secure_path="$tool_dir"

	zxfer_find_required_tool mocktool "mocktool"

	assertEquals "Required tool lookup should return the resolved absolute path from the secure PATH." \
		"$tool_dir/mocktool" "$g_zxfer_required_tool_result"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace() {
	tab=$(printf '\t')

	zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool"
	status=$?

	assertEquals "Resolved tool paths with control whitespace should be rejected." 1 "$status"
	assertContains "Rejected tool paths should explain the control-whitespace requirement." \
		"$g_zxfer_required_tool_result" "single-line absolute path without control whitespace"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace_with_scope() {
	tab=$(printf '\t')

	zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool" "host origin.example"
	status=$?

	assertEquals "Scoped control-whitespace tool paths should be rejected." 1 "$status"
	assertContains "Scoped control-whitespace failures should mention the host scope." \
		"$g_zxfer_required_tool_result" "Required dependency \"mocktool\" on host origin.example resolved to"
}

test_refresh_compression_commands_resolves_local_helpers_when_enabled() {
	result=$(
		(
			zxfer_find_required_tool() {
				if [ "$1" = "zstd" ]; then
					g_zxfer_required_tool_result=/secure/bin/zstd
				else
					g_zxfer_required_tool_result="unexpected tool"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
			printf 'compress=%s\n' "$g_cmd_compress_safe"
			printf 'decompress=%s\n' "$g_cmd_decompress_safe"
		)
	)

	assertContains "Enabled compression should resolve the compressor head token through the secure local path." \
		"$result" "compress='/secure/bin/zstd' '-T0' '-9'"
	assertContains "Enabled compression should resolve the decompressor head token through the secure local path." \
		"$result" "decompress='/secure/bin/zstd' '-d'"
}

test_zxfer_resolve_cli_command_safe_rejects_blank_local_commands() {
	zxfer_resolve_cli_command_safe "" "   " "compression command"

	assertEquals "Blank local CLI commands should be rejected." 1 "$?"
	assertEquals "Blank local CLI command failures should use the documented validation message." \
		"Required dependency \"compression command\" must not be empty or whitespace-only." \
		"$g_zxfer_resolved_cli_command_result"
}

test_zxfer_resolve_cli_command_safe_surfaces_local_lookup_failures() {
	output=$(
		(
			zxfer_find_required_tool() {
				g_zxfer_required_tool_result="missing helper"
				return 1
			}
			zxfer_resolve_cli_command_safe "" "zstd -T0 -9" "compression command"
			printf 'status=%s %s\n' "$?" "$g_zxfer_resolved_cli_command_result"
		)
	)

	assertEquals "Local CLI command resolution should surface the dependency lookup failure verbatim." \
		"status=1 missing helper" "$output"
}

test_refresh_compression_commands_rejects_empty_compression_command() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress=""
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when the configured compression command is empty." 2 "$status"
	assertContains "Empty compression commands should use the documented usage error." \
		"$output" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_whitespace_only_compression_command() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress="   "
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only compression commands as empty." 2 "$status"
	assertContains "Whitespace-only compression commands should use the documented usage error." \
		"$output" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_missing_decompress_command() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress=""
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when no decompressor can be derived." 1 "$status"
	assertContains "Missing decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_refresh_compression_commands_rejects_whitespace_only_decompress_command() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="   "
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only decompression commands as missing." 1 "$status"
	assertContains "Whitespace-only decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}
