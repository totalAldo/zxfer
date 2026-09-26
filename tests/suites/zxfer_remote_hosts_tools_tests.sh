#!/bin/sh
# Tests for src/zxfer_remote_hosts.sh remote CLI tool resolution and
# src/zxfer_ssh_transport.sh zfs-command refresh,
# run by tests/test_zxfer_remote_hosts.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_zxfer_reset_remote_host_state_resets_capability_and_resolved_tool_state() {
	result=$(
		(
			g_cmd_zfs="/stub/zfs"
			g_origin_remote_capabilities_host="origin.example"
			g_origin_remote_capabilities_response="dirty-origin"
			g_origin_remote_capabilities_os="DirtyOriginOS"
			g_target_remote_capabilities_tools="zfs cat"
			g_target_remote_capabilities_response="dirty-target"
			g_target_remote_capabilities_tool_records="dirty-target-tools"
			g_zxfer_remote_probe_capture_failed=1
			g_origin_cmd_zfs="/dirty/origin-zfs"

			zxfer_reset_remote_host_state
			printf 'origin=<%s|%s|%s>\n' "$g_origin_remote_capabilities_host" \
				"$g_origin_remote_capabilities_response" "$g_origin_remote_capabilities_os"
			printf 'target=<%s|%s|%s>\n' "$g_target_remote_capabilities_tools" \
				"$g_target_remote_capabilities_response" "$g_target_remote_capabilities_tool_records"
			printf 'capture_failed=%s\n' "$g_zxfer_remote_probe_capture_failed"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		)
	)

	assertContains "Remote-host reset should empty the origin capability slot." \
		"$result" "origin=<||>"
	assertContains "Remote-host reset should empty the target capability slot." \
		"$result" "target=<||>"
	assertContains "Remote-host reset should clear remote capture failure state." \
		"$result" "capture_failed=0"
	assertContains "Remote-host reset should restore origin zfs to the local default." \
		"$result" "origin_zfs=/stub/zfs"
}

test_zxfer_refresh_remote_zfs_commands_rejects_shell_quoted_host_specs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_O_origin_host='origin.example "pfexec -u zfs"'
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_refresh_remote_zfs_commands
		)
	)
	status=$?
	set -e

	assertEquals "Remote host-spec refresh should fail closed when the configured host spec relies on shell quoting." \
		2 "$status"
	assertContains "Rejected remote host specs should explain the literal-token requirement." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
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
