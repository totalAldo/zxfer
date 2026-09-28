#!/bin/sh
#
# shunit2 tests for src/zxfer_ssh_transport.sh: host-spec parsing, the
# managed ssh policy, rendered remote commands and control sockets.
#
# The fragments keep the fixture they were written for: the exec fixture for
# the remote-command cases and the remote-host fixture for the connection
# cases. The cases in this file use none.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_ssh_transport"
	zxfer_test_exec_fixture_one_time_setup
	zxfer_test_remote_host_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_ssh_transport_commands_tests.sh"; then
		zxfer_test_exec_fixture_setup
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_ssh_transport_connection_tests.sh"; then
		zxfer_test_remote_host_fixture_setup
	fi
}

tearDown() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_ssh_transport_commands_tests.sh"; then
		relax_test_tmpdir_permissions
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_ssh_transport_connection_tests.sh"; then
		zxfer_test_remote_host_fixture_teardown
	fi
}

# Write a zfs stand-in that logs its argument count and each argument in
# brackets to $ZXFER_TEST_ZFS_ARGV_LOG.
zxfer_test_write_argv_logging_zfs() {
	cat >"$1" <<'EOF'
#!/bin/sh
{
	printf 'argc=%s\n' "$#"
	for l_arg in "$@"; do
		printf '[%s]\n' "$l_arg"
	done
} >>"$ZXFER_TEST_ZFS_ARGV_LOG"
EOF
	chmod +x "$1"
}

# Route destination zfs calls to target.example through an ssh stand-in that
# joins the remote argv and runs it under /bin/sh, or under CSH_SHELL.
# Usage: zxfer_test_use_remote_argv_zfs NAME [CSH_SHELL]
zxfer_test_use_remote_argv_zfs() {
	create_fake_ssh_join_exec_bin "$TEST_TMPDIR/$1.ssh" "${2:-}"
	zxfer_test_write_argv_logging_zfs "$TEST_TMPDIR/$1.zfs"
	ZXFER_TEST_ZFS_ARGV_LOG="$TEST_TMPDIR/$1.argv"
	: >"$ZXFER_TEST_ZFS_ARGV_LOG"
	export ZXFER_TEST_ZFS_ARGV_LOG
	g_cmd_ssh="$TEST_TMPDIR/$1.ssh"
	g_cmd_zfs=/nonexistent/local/zfs
	g_target_cmd_zfs="$TEST_TMPDIR/$1.zfs"
	g_option_O_origin_host=""
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	zxfer_refresh_remote_zfs_commands
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

test_zxfer_reset_ssh_transport_state_clears_owned_state() {
	g_cmd_zfs=/stub/zfs
	g_cmd_ssh=""
	g_ssh_origin_control_socket=/dirty/origin.sock
	g_zxfer_ssh_target_spec=dirty
	g_zxfer_ssh_origin_wrapper=dirty
	g_zxfer_prepared_ssh_shell_command_result=dirty

	zxfer_reset_ssh_transport_state

	assertEquals "Transport reset should clear the origin control socket." \
		"" "$g_ssh_origin_control_socket"
	assertEquals "Transport reset should forget the parsed -T host spec." \
		"" "$g_zxfer_ssh_target_spec"
	assertEquals "Transport reset should forget the parsed -O wrapper." \
		"" "$g_zxfer_ssh_origin_wrapper"
	assertEquals "Transport reset should clear prepared render results." \
		"" "$g_zxfer_prepared_ssh_shell_command_result"
	assertEquals "Transport reset should keep control sockets disabled until SSH resolves." \
		0 "$g_ssh_supports_control_sockets"
}

test_refresh_remote_zfs_commands_parses_each_host_spec_once() {
	g_option_O_origin_host="backup@example.com pfexec -u root"
	g_option_T_target_host="target.example"
	zxfer_refresh_remote_zfs_commands

	assertEquals "The -O host should be the first host-spec token." \
		"backup@example.com" "$g_zxfer_ssh_origin_host"
	assertEquals "The -O wrapper tokens should be quoted once for the remote shell." \
		"'pfexec' '-u' 'root'" "$g_zxfer_ssh_origin_wrapper"
	assertEquals "A plain -T host should have no wrapper." \
		"target.example:" "$g_zxfer_ssh_target_host:$g_zxfer_ssh_target_wrapper"

	zxfer_test_capture_subshell '
		zxfer_throw_usage_error() {
			printf "%s status=%s\n" "$1" "$2"
			exit "$2"
		}
		g_option_T_target_host="target.example \"doas\""
		zxfer_refresh_remote_zfs_commands
	'
	assertEquals "A host spec that needs shell quoting should be a usage error." \
		2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "The usage error should keep the literal-token message." \
		"Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported. status=2" \
		"$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_run_destination_zfs_cmd_preserves_multiline_and_empty_arguments_over_target_host() {
	zxfer_test_use_remote_argv_zfs lf_sh

	zxfer_run_destination_zfs_cmd set 'user:multi=a
sharenfs=rw' backup/dst
	set_status=$?
	zxfer_run_destination_zfs_cmd list -H "" backup/dst
	empty_status=$?
	zxfer_run_destination_zfs_cmd create -o compression=lz4 -o 'user:note=a b' backup/dst/new
	create_status=$?

	assertEquals "Remote zfs calls should succeed." "0 0 0" \
		"$set_status $empty_status $create_status"
	assertEquals "Every remote zfs argument should keep its boundary, embedded newlines and empty values included." \
		"argc=3
[set]
[user:multi=a
sharenfs=rw]
[backup/dst]
argc=4
[list]
[-H]
[]
[backup/dst]
argc=6
[create]
[-o]
[compression=lz4]
[-o]
[user:note=a b]
[backup/dst/new]" "$(cat "$ZXFER_TEST_ZFS_ARGV_LOG")"
}

test_run_destination_zfs_cmd_preserves_multiline_arguments_through_a_csh_login_shell() {
	l_csh_shell=$(find_csh_shell_for_tests)
	if [ "$l_csh_shell" = "" ]; then
		startSkipping
		assertTrue "csh and tcsh are unavailable; csh login-shell regression skipped." true
		endSkipping
		return 0
	fi
	zxfer_test_use_remote_argv_zfs lf_csh "$l_csh_shell"

	zxfer_run_destination_zfs_cmd set 'user:multi=a
sharenfs=rw' backup/dst

	assertEquals "A csh login shell should receive one command line and keep the argument count." \
		"argc=3
[set]
[user:multi=a
sharenfs=rw]
[backup/dst]" "$(cat "$ZXFER_TEST_ZFS_ARGV_LOG")"
}

test_render_destination_zfs_command_keeps_multiline_arguments_on_one_line() {
	zxfer_test_use_remote_argv_zfs lf_render

	rendered=$(zxfer_render_destination_zfs_command set 'user:multi=a
sharenfs=rw' backup/dst)
	rendered_lines=$(printf '%s\n' "$rendered" | wc -l | tr -d '[:space:]')
	zxfer_execute_rendered_shell_command "$rendered"

	assertEquals "A rendered remote command should stay one shell line." 1 "$rendered_lines"
	assertEquals "The rendered command should deliver the same remote argv." \
		"argc=3
[set]
[user:multi=a
sharenfs=rw]
[backup/dst]" "$(cat "$ZXFER_TEST_ZFS_ARGV_LOG")"
}

test_run_destination_zfs_cmd_counts_remote_calls_like_local_ones_when_very_verbose() {
	zxfer_test_use_remote_argv_zfs profile_remote
	g_option_V_very_verbose=1
	g_zxfer_failure_stage="property transfer"
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_bucket_property_reconciliation=0

	zxfer_run_destination_zfs_cmd set user:note=x backup/dst 2>/dev/null
	zxfer_run_destination_zfs_cmd inherit user:note backup/dst 2>/dev/null
	g_option_T_target_host=""
	g_cmd_zfs="$TEST_TMPDIR/profile_remote.zfs"
	zxfer_run_destination_zfs_cmd set user:note=x backup/dst

	g_option_V_very_verbose=0
	g_zxfer_failure_stage=""
	assertEquals "Remote and local property calls should all count as destination zfs calls." \
		3 "$g_zxfer_profile_destination_zfs_calls"
	assertEquals "Remote and local property calls should all count toward property reconciliation." \
		3 "$g_zxfer_profile_bucket_property_reconciliation"
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

test_build_remote_sh_c_command_restores_lc_all_and_publishes_its_result() {
	l_lc_all_output=$(
		LC_ALL=POSIX
		zxfer_build_remote_sh_c_command 'exit 7' >/dev/null
		printf 'set=%s result=%s\n' "${LC_ALL-unset}" "$g_zxfer_remote_sh_c_command_result"
		unset LC_ALL
		zxfer_build_remote_sh_c_command 'a
b' >/dev/null
		printf 'unset=%s\n' "${LC_ALL-unset}"
	)

	assertEquals "The renderer should restore a set LC_ALL, clear an unset one, and publish its result in the current shell." \
		"set=POSIX result='sh' '-c' 'exit 7'
unset=unset" "$l_lc_all_output"
}

test_build_remote_sh_c_command_pins_the_chunked_transport_bytes() {
	l_x32='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	l_x128=$l_x32$l_x32$l_x32$l_x32
	# shellcheck disable=SC2016 # The remote bootstrap expands these.
	l_bootstrap='l_nl=$(printf "\\nx") || exit $?; l_nl=${l_nl%x}; l_script=; for l_part do case $l_part in d*) l_script=$l_script${l_part#d} ;; n) l_script=$l_script$l_nl ;; *) exit 125 ;; esac; done; exec sh -c "$l_script"'

	zxfer_build_remote_sh_c_command "a'b

${l_x128}xx
" >/dev/null

	assertEquals "Chunks should be quoted words: a quote escaped as '\\'', one n word per newline, and at most 128 script bytes per d word." \
		"'sh' '-c' '$l_bootstrap' 'sh' 'da'\\''b' 'n' 'n' 'd$l_x128' 'dxx' 'n'" \
		"$g_zxfer_remote_sh_c_command_result"
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

# zxfer-test-fragment: suites/zxfer_ssh_transport_commands_tests.sh
# shellcheck source=tests/suites/zxfer_ssh_transport_commands_tests.sh
. "$TESTS_DIR/suites/zxfer_ssh_transport_commands_tests.sh"
# zxfer-test-fragment: suites/zxfer_ssh_transport_connection_tests.sh
# shellcheck source=tests/suites/zxfer_ssh_transport_connection_tests.sh
. "$TESTS_DIR/suites/zxfer_ssh_transport_connection_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_ssh_transport.sh" \
		"$TESTS_DIR/suites/zxfer_ssh_transport_commands_tests.sh" \
		"$TESTS_DIR/suites/zxfer_ssh_transport_connection_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
