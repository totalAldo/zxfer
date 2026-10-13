#!/bin/sh
# Local and remote CLI command-head resolution for src/zxfer_remote_hosts.sh.
# Run by tests/test_zxfer_remote_hosts.sh under the remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
