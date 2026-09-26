#!/bin/sh
# Tests for src/zxfer_cli.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_read_command_line_switches_skips_control_socket_when_ssh_lacks_support() {
	remote_log="$TEST_TMPDIR/unsupported_control_socket.log"
	result_file="$TEST_TMPDIR/unsupported_control_socket.out"
	stderr_file="$TEST_TMPDIR/unsupported_control_socket.err"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		: >"$remote_log"
		FAKE_SSH_LOG="$remote_log"
		export FAKE_SSH_LOG
		OPTIND=1
		g_option_z_compress=0
		g_cmd_compress="zstd -3"
		g_cmd_decompress="zstd -d"
		g_option_O_origin_host=""
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_cmd_zfs="/sbin/zfs"
		g_ssh_supports_control_sockets=0
		g_ssh_origin_control_socket=""
		zxfer_read_command_line_switches -O "backup@example.com"
		printf 'origin=%s\n' "$g_option_O_origin_host"
		printf 'socket=%s\n' "$g_ssh_origin_control_socket"
	) >"$result_file" 2>"$stderr_file"
	status=$?

	result=$(cat "$result_file")
	assertNotEquals "Skipping unsupported control sockets should still leave observable parser state." "" "$result"
	assertEquals "Unsupported ssh clients should not be asked to create control sockets." "" "$(cat "$remote_log")"
	assertEquals "Parsing should not emit stderr noise when multiplexing is unavailable." "" "$(cat "$stderr_file")"
	assertContains "$result" "origin=backup@example.com"
	assertContains "$result" "socket="
}
