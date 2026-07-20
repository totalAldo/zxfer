#!/bin/sh
#
# shunit2 tests for src/zxfer_ssh_transport.sh ownership and direct loading.
#
# shellcheck disable=SC2016,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_ssh_transport.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_ssh_transport"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

zxfer_test_max_rendered_shell_word_bytes() {
	LC_ALL=C awk '
		function finish_word() {
			if (in_word && word_bytes > max_bytes) max_bytes = word_bytes
			in_word = 0
			word_bytes = 0
		}
		BEGIN { single_quote = sprintf("%c", 39) }
		{
			for (i = 1; i <= length($0); i++) {
				c = substr($0, i, 1)
				if (!in_single && !in_double && (c == " " || c == "\t")) {
					finish_word()
					continue
				}
				in_word = 1
				word_bytes++
				if (escaped) {
					escaped = 0
					continue
				}
				if (!in_single && c == "\\") {
					escaped = 1
					continue
				}
				if (!in_double && c == single_quote) in_single = !in_single
				else if (!in_single && c == "\"") in_double = !in_double
			}
		}
		END {
			finish_word()
			if (in_single || in_double || escaped) exit 2
			print max_bytes + 0
		}
	'
}

zxfer_test_write_remote_sh_capture_bin() {
	l_zxfer_test_remote_sh_capture_path=$1
	cat >"$l_zxfer_test_remote_sh_capture_path" <<'EOF'
#!/bin/sh
[ "${1:-}" = "-c" ] || exit 126
case ${2:-} in
'l_nl=$(printf "\\nx")'*)
	exec "$ZXFER_TEST_REAL_SH" "$@"
	;;
esac
printf '%s' "$2" >"$ZXFER_TEST_SCRIPT_CAPTURE" || exit $?
if IFS= read -r l_zxfer_test_stdin; then
	printf '%s' "$l_zxfer_test_stdin" >"$ZXFER_TEST_STDIN_CAPTURE" || exit $?
else
	: >"$ZXFER_TEST_STDIN_CAPTURE" || exit $?
fi
exit "${ZXFER_TEST_INNER_STATUS:-0}"
EOF
	chmod +x "$l_zxfer_test_remote_sh_capture_path"
}

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

test_build_remote_sh_c_command_keeps_short_rendering_stable() {
	l_short_rendered="$TEST_TMPDIR/short-rendered"
	l_short_expected="$TEST_TMPDIR/short-expected"
	zxfer_build_remote_sh_c_command 'exit 7' >"$l_short_rendered"
	printf '%s' "'sh' '-c' 'exit 7'" >"$l_short_expected"

	assertEquals "Short remote scripts should retain the established rendered argv." \
		"'sh' '-c' 'exit 7'" \
		"$(cat "$l_short_rendered")"
	assertTrue "Short remote scripts should retain the established exact stdout without a trailing newline." \
		"cmp '$l_short_expected' '$l_short_rendered' >/dev/null 2>&1"
}

test_build_remote_sh_c_command_preserves_renderer_failure_output_and_status() {
	l_failure_rendered="$TEST_TMPDIR/failure-rendered"
	if (
		zxfer_build_shell_command_from_argv() {
			printf '%s' 'render diagnostic'
			return 43
		}
		zxfer_build_remote_sh_c_command 'exit 7' >"$l_failure_rendered"
	); then
		l_failure_status=0
	else
		l_failure_status=$?
	fi

	assertEquals "Remote sh rendering failures should retain the underlying renderer status." \
		43 "$l_failure_status"
	assertEquals "Remote sh rendering failures should retain the underlying renderer diagnostic." \
		"render diagnostic" "$(cat "$l_failure_rendered")"
}

test_build_remote_sh_c_command_chunks_long_scripts_below_illumos_csh_limit() {
	l_long_script=""
	l_padding_line="# quote ' double \" dollar \$ parens () semicolon ; wildcard * question ?"
	l_padding_count=0
	while [ "$l_padding_count" -lt 90 ]; do
		l_long_script=$l_long_script$l_padding_line'
'
		l_padding_count=$((l_padding_count + 1))
	done
	l_long_script=$l_long_script'IFS= read -r l_input || exit 91;
printf "%s\n" "$l_input";
exit 37;
'

	l_rendered_command=$(zxfer_build_remote_sh_c_command "$l_long_script")
	l_rendered_lines=$(printf '%s\n' "$l_rendered_command" | wc -l | tr -d '[:space:]')
	l_max_word_bytes=$(printf '%s\n' "$l_rendered_command" |
		zxfer_test_max_rendered_shell_word_bytes)

	assertEquals "Chunked remote sh commands should remain one physical login-shell line." \
		1 "$l_rendered_lines"
	assertTrue "Every rendered word should stay below illumos csh's 1020-byte lexical limit (maximum $l_max_word_bytes)." \
		"[ '$l_max_word_bytes' -lt 1020 ]"
	assertContains "Long scripts should use the fixed positional-argument bootstrap." \
		"$l_rendered_command" 'for l_part do case $l_part in'
}

test_build_remote_sh_c_command_preserves_script_bytes_stdin_and_status() {
	l_capture_sh="$TEST_TMPDIR/sh"
	l_script_capture="$TEST_TMPDIR/remote-script.capture"
	l_stdin_capture="$TEST_TMPDIR/remote-stdin.capture"
	l_expected_script="$TEST_TMPDIR/remote-script.expected"
	zxfer_test_write_remote_sh_capture_bin "$l_capture_sh"

	l_long_script=""
	l_padding_line="# preserve quote ' double \" dollar \$ parens () semicolon ; wildcard *"
	l_padding_count=0
	while [ "$l_padding_count" -lt 90 ]; do
		l_long_script=$l_long_script$l_padding_line'
'
		l_padding_count=$((l_padding_count + 1))
	done
	l_long_script=$l_long_script'printf "%s\n" reached;
'
	printf '%s' "$l_long_script" >"$l_expected_script"
	l_rendered_command=$(zxfer_build_remote_sh_c_command "$l_long_script")

	ZXFER_TEST_REAL_SH=/bin/sh \
		ZXFER_TEST_SCRIPT_CAPTURE=$l_script_capture \
		ZXFER_TEST_STDIN_CAPTURE=$l_stdin_capture \
		ZXFER_TEST_INNER_STATUS=37 \
		PATH="$TEST_TMPDIR:$PATH" \
		/bin/sh -c "$l_rendered_command" <<'EOF'
stdin-through-bootstrap
EOF
	l_status=$?

	assertEquals "The inner sh status should pass through the exec bootstrap unchanged." \
		37 "$l_status"
	assertTrue "The chunk bootstrap should reconstruct quotes, metacharacters, and trailing newlines byte-for-byte." \
		"cmp '$l_expected_script' '$l_script_capture' >/dev/null 2>&1"
	assertEquals "The chunk bootstrap should leave the original stdin attached to the inner script." \
		"stdin-through-bootstrap" "$(cat "$l_stdin_capture")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
