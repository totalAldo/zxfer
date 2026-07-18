#!/bin/sh
#
# shunit2 tests for src/zxfer_ssh_transport.sh ownership and direct loading.
#
# shellcheck disable=SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_ssh_transport.sh"

test_transport_boundary_loads_without_capability_or_snapshot_modules() {
	assertTrue "The direct transport boundary should define SSH rendering." \
		"command -v zxfer_build_ssh_shell_command_for_host >/dev/null 2>&1"
	assertTrue "The direct transport boundary should define control-socket lifecycle." \
		"command -v zxfer_close_all_ssh_control_sockets >/dev/null 2>&1"
	assertFalse "The direct transport boundary should not load remote capability negotiation." \
		"command -v zxfer_ensure_remote_host_capabilities >/dev/null 2>&1"
	assertFalse "The direct transport boundary should not load snapshot state." \
		"command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1"
}

test_zxfer_reset_ssh_transport_state_clears_owned_state() {
	g_cmd_zfs=/stub/zfs
	g_cmd_ssh=""
	g_ssh_origin_control_socket=/dirty/origin.sock
	g_zxfer_ssh_transport_tokens_target_set=1
	g_zxfer_ssh_shell_context_memo_origin_spec=dirty
	g_zxfer_prepared_ssh_shell_command_result=dirty
	g_LZFS=/dirty/origin-zfs

	zxfer_reset_ssh_transport_state

	assertEquals "Transport reset should clear the origin control socket." \
		"" "$g_ssh_origin_control_socket"
	assertEquals "Transport reset should clear target-token memos." \
		0 "$g_zxfer_ssh_transport_tokens_target_set"
	assertEquals "Transport reset should clear host parsing memos." \
		"" "$g_zxfer_ssh_shell_context_memo_origin_spec"
	assertEquals "Transport reset should clear prepared render results." \
		"" "$g_zxfer_prepared_ssh_shell_command_result"
	assertEquals "Transport reset should restore local source ZFS routing." \
		/stub/zfs "$g_LZFS"
	assertEquals "Transport reset should keep control sockets disabled until SSH resolves." \
		0 "$g_ssh_supports_control_sockets"
}

test_wrapper_host_spec_parsing_preserves_host_and_remote_wrapper_argv() {
	zxfer_prepare_ssh_shell_command_context \
		"backup@example.com pfexec -u root" \
		"'zfs' 'list' 'tank/src'"

	assertEquals "Only the first host-spec token should become the SSH host argv." \
		"backup@example.com" "$g_zxfer_ssh_shell_host_result"
	assertEquals "Wrapper tokens should remain quoted inside the remote command channel." \
		"'pfexec' '-u' 'root' 'zfs' 'list' 'tank/src'" \
		"$g_zxfer_ssh_shell_full_remote_command_result"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
