#!/bin/sh
# Tool-resolution tests for src/zxfer_dependencies.sh: required tools on the
# secure PATH, resolved-path validation, compression helpers and CLI command
# heads, locally and on remote hosts. Run by tests/test_zxfer_dependencies.sh
# under the remote-host fixture.
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

test_zxfer_resolve_cli_command_safe_resolves_remote_first_token_and_preserves_args() {
	result=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "/remote/bin/zstd"
			}
			zxfer_resolve_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source
			printf '%s\n' "$g_zxfer_resolved_cli_command_result"
		)
	)

	assertEquals "Remote CLI command resolution should replace only the first token and keep the remaining arguments intact." \
		"'/remote/bin/zstd' '-T0' '-9'" "$result"
}

test_zxfer_resolve_cli_command_safe_delegates_the_remote_head_to_the_tool_resolver() {
	log_file="$TEST_TMPDIR/resolve_remote_cli_head.log"

	result=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s:%s:%s:%s\n' "$1" "$2" "$3" "$4" >"$log_file"
				g_zxfer_required_tool_result=/remote/bin/xz
			}
			zxfer_resolve_cli_command_safe "target.example" "xz -d -T0" "decompression command" destination
			printf '%s\n' "$g_zxfer_resolved_cli_command_result"
		)
	)

	assertEquals "The command head should be resolved with the host, label and side." \
		"target.example:xz:decompression command:destination" "$(cat "$log_file")"
	assertEquals "The resolved head should be requoted with the remaining arguments." \
		"'/remote/bin/xz' '-d' '-T0'" "$result"
}

test_zxfer_resolve_cli_command_safe_reuses_the_preloaded_host_scope() {
	probe_count_file="$TEST_TMPDIR/resolve_remote_cli_scope.count"
	printf '0\n' >"$probe_count_file"

	result=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_j_jobs=4
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_option_V_very_verbose=1
			g_zxfer_profile_remote_cli_tool_direct_probes=0
			zxfer_capture_remote_probe_output() {
				printf '%s\n' "$(($(cat "$probe_count_file") + 1))" >"$probe_count_file"
				g_zxfer_remote_probe_stdout='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	zstd	0	/remote/bin/zstd
end'
				g_zxfer_remote_probe_stderr=""
			}
			zxfer_preload_remote_host_capabilities origin.example source
			zxfer_resolve_cli_command_safe \
				"origin.example" "zstd -T0 -9" "compression command" source
			printf '%s\n' "$g_zxfer_resolved_cli_command_result"
			printf 'direct_probes=%s\n' "$g_zxfer_profile_remote_cli_tool_direct_probes"
		)
	)

	assertContains "The compression head should resolve from the preloaded capabilities." \
		"$result" "'/remote/bin/zstd' '-T0' '-9'"
	assertContains "No direct probe should be needed." "$result" "direct_probes=0"
	assertEquals "The preload should be the only probe." 1 "$(cat "$probe_count_file")"
}

test_zxfer_resolve_cli_command_safe_reports_a_missing_remote_head() {
	output=$(
		(
			g_zxfer_secure_path="/secure/bin"
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 10
			}
			zxfer_resolve_cli_command_safe "origin.example" "zstd -3" "compression command" source
			printf 'status=%s\n%s\n' "$?" "$g_zxfer_resolved_cli_command_result"
		)
	)

	assertEquals "A missing command head should fail with the documented secure-PATH guidance." \
		"status=1
Required dependency \"compression command\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." "$output"
}

test_zxfer_resolve_cli_command_safe_rejects_blank_commands_and_surfaces_remote_lookup_failures() {
	zxfer_resolve_cli_command_safe "origin.example" "   " "compression command" source
	blank_status=$?
	blank_message=$g_zxfer_resolved_cli_command_result

	lookup_output=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="remote helper lookup failed"
				return 1
			}
			zxfer_resolve_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source
			printf 'status=%s %s\n' "$?" "$g_zxfer_resolved_cli_command_result"
		)
	)

	assertEquals "Blank remote CLI commands should be rejected." 1 "$blank_status"
	assertEquals "Blank remote CLI command failures should use the documented validation message." \
		"Required dependency \"compression command\" must not be empty or whitespace-only." "$blank_message"
	assertEquals "Remote CLI command resolution should surface the remote helper lookup failure verbatim." \
		"status=1 remote helper lookup failed" "$lookup_output"
}

test_zxfer_resolve_cli_command_safe_rejects_shell_quoting_before_any_remote_lookup() {
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "unexpected remote lookup"
			}
			zxfer_resolve_cli_command_safe \
				"origin.example" \
				'"/opt/zstd dir/zstd" -T0 -9' \
				"compression command" \
				source
			printf 'status=%s %s\n' "$?" "$g_zxfer_resolved_cli_command_result"
		)
	)

	assertEquals "Remote CLI command resolution should report only the literal-token diagnostic before any remote lookup." \
		"status=1 compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." "$output"
}
