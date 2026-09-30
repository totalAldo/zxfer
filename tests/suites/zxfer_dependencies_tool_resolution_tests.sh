#!/bin/sh
# Tool-resolution tests for src/zxfer_dependencies.sh: shell functions never
# satisfy a helper lookup, the decompressor check, and CLI command heads
# resolved on remote hosts. Run by tests/test_zxfer_dependencies.sh under the
# remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

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

# No option sets the decompressor, so only a changed default reaches this
# runtime error; -Z blank is a usage error pinned by the golden CLI suite.
test_refresh_compression_commands_rejects_a_missing_or_blank_decompress_command() {
	for l_decompress in "" "   "; do
		set +e
		output=$(
			(
				zxfer_test_stub_throw_error_to_stdout
				g_option_z_compress=1
				g_cmd_compress="zstd -3"
				g_cmd_decompress=$l_decompress
				zxfer_refresh_compression_commands
			)
		)
		status=$?

		assertEquals "Compression validation should fail for the decompressor [$l_decompress]." \
			1 "$status"
		assertContains "A missing decompressor should use the documented runtime error." \
			"$output" "Compression requested but decompression command missing."
	done
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
