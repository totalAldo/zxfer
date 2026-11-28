#!/bin/sh
#
# shunit2 tests for src/zxfer_ssh_transport.sh: host-spec parsing, the
# managed ssh policy, rendered remote commands and control sockets.
#
# Remote-command cases initialize the exec fixture and connection cases
# initialize the remote-host fixture explicitly; pure helpers use neither.
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

tearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_remote_host_fixture_teardown
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

zxfer_test_find_utf8_locale() {
	l_found_utf8_locale=$(locale -a 2>/dev/null | grep -i -E '^(C|en_US)\.utf-?8$' | head -n 1)
	if [ -z "$l_found_utf8_locale" ]; then
		l_found_utf8_locale=$(locale -a 2>/dev/null | grep -i -E '(^|\.)utf-?8$' | head -n 1)
	fi
	printf '%s\n' "${l_found_utf8_locale:-POSIX}"
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

test_zfs_command_for_role_chunks_long_single_args_but_keeps_multi_arg_commands_plain() {
	zxfer_test_exec_fixture_setup
	l_x32='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	l_x256=$l_x32$l_x32$l_x32$l_x32$l_x32$l_x32$l_x32$l_x32
	l_x800=$l_x256$l_x256$l_x256$l_x32
	l_mb_char=$(printf '\342\202\254')
	l_mb8=$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char
	l_mb64=$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8
	# 264 UTF-8 3-byte chars = 792 bytes (>768) while character length is <768.
	l_mb264=$l_mb64$l_mb64$l_mb64$l_mb64$l_mb8
	l_q8="''''''''"
	l_q64=$l_q8$l_q8$l_q8$l_q8$l_q8$l_q8$l_q8$l_q8
	# 256 single quotes = 1024 escaped bytes (>768) while raw length is 256.
	l_q256=$l_q64$l_q64$l_q64$l_q64

	l_out_file="$TEST_TMPDIR/long_single_arg.out"
	l_utf8_locale=$(zxfer_test_find_utf8_locale)
	(
		g_cmd_ssh=/usr/bin/ssh
		g_target_cmd_zfs=/target/sbin/zfs
		g_option_T_target_host=target.example
		zxfer_refresh_remote_zfs_commands

		# 15 short properties push the total command past 768 bytes, but every
		# individual word stays well below 768 bytes and must remain plain.
		LC_ALL=$l_utf8_locale
		export LC_ALL
		zxfer_render_zfs_command_for_role destination set \
			"user:p01=$l_x32$l_x32" "user:p02=$l_x32$l_x32" "user:p03=$l_x32$l_x32" \
			"user:p04=$l_x32$l_x32" "user:p05=$l_x32$l_x32" "user:p06=$l_x32$l_x32" \
			"user:p07=$l_x32$l_x32" "user:p08=$l_x32$l_x32" "user:p09=$l_x32$l_x32" \
			"user:p10=$l_x32$l_x32" "user:p11=$l_x32$l_x32" "user:p12=$l_x32$l_x32" \
			"user:p13=$l_x32$l_x32" "user:p14=$l_x32$l_x32" "user:p15=$l_x32$l_x32" \
			backup/dst
		case $g_zxfer_shell_command_result in
		*"'sh' '-c'"*) l_multi_form=chunked ;;
		*) l_multi_form=plain ;;
		esac
		printf 'multi=%s|lc=%s\n' "$l_multi_form" "${LC_ALL-unset}"

		# A >768-byte ASCII arg, a >768-byte multibyte arg, and a quote-heavy
		# arg whose escaped word exceeds 768 bytes must all route via sh -c.
		unset LC_ALL
		LANG=$l_utf8_locale
		export LANG
		for l_case_tag in ascii multibyte quotes; do
			case $l_case_tag in
			ascii) l_val=$l_x800 ;;
			multibyte) l_val=$l_mb264 ;;
			quotes) l_val=$l_q256 ;;
			esac
			zxfer_render_zfs_command_for_role destination set "user:big=$l_val" backup/dst
			l_remote_cmd=$(/bin/sh -c "set -- $g_zxfer_shell_command_result; shift \$(( \$# - 1 )); printf '%s' \"\$1\"")
			l_max_word=$(printf '%s\n' "$l_remote_cmd" | zxfer_test_max_rendered_shell_word_bytes)
			case $l_remote_cmd in
			"'sh' '-c' 'l_nl="*) l_form=chunked ;;
			*) l_form=plain ;;
			esac
			printf '%s=%s|under1020=%s|lc=%s\n' "$l_case_tag" "$l_form" \
				"$([ "$l_max_word" -lt 1020 ] && printf yes || printf no)" "${LC_ALL-unset}"
		done
	) >"$l_out_file"
	l_out=$(cat "$l_out_file")

	assertEquals "Normal multi-property commands stay plain; long single arguments route via chunked sh -c while restoring LC_ALL." \
		"multi=plain|lc=$l_utf8_locale
ascii=chunked|under1020=yes|lc=unset
multibyte=chunked|under1020=yes|lc=unset
quotes=chunked|under1020=yes|lc=unset" "$l_out"

	# Verify end-to-end execution delivers a 1100-byte single argument intact
	# (through csh when available, else /bin/sh).
	l_long_shell=$(find_csh_shell_for_tests)
	zxfer_test_use_remote_argv_zfs long_arg "$l_long_shell"
	l_x1100=$l_x800$l_x256$l_x32"123456789012"
	zxfer_run_destination_zfs_cmd set "user:long=$l_x1100" backup/dst
	assertEquals "A >1020-byte single argument must reach remote zfs byte-for-byte intact." \
		"argc=3
[set]
[user:long=$l_x1100]
[backup/dst]" "$(cat "$ZXFER_TEST_ZFS_ARGV_LOG")"
}

test_build_remote_sh_c_command_pins_the_plain_and_chunked_renderings() {
	l_x32='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
	l_x128=$l_x32$l_x32$l_x32$l_x32
	# shellcheck disable=SC2016 # The remote bootstrap expands these.
	l_bootstrap='l_nl=$(printf "\\nx") || exit $?; l_nl=${l_nl%x}; l_script=; for l_part do case $l_part in d*) l_script=$l_script${l_part#d} ;; n) l_script=$l_script$l_nl ;; *) exit 125 ;; esac; done; exec sh -c "$l_script"'
	zxfer_build_remote_sh_c_command 'exit 7' >"$TEST_TMPDIR/render-stdout"
	l_plain_rendered=$g_zxfer_shell_command_result

	zxfer_build_remote_sh_c_command "a'b

${l_x128}xx
" >>"$TEST_TMPDIR/render-stdout"

	assertEquals "A short one-line script should keep the plain sh -c form." \
		"'sh' '-c' 'exit 7'" "$l_plain_rendered"
	assertEquals "A multi-line script should become quoted chunk words: a quote escaped as '\\'', one n word per newline, and at most 128 script bytes per d word." \
		"'sh' '-c' '$l_bootstrap' 'sh' 'da'\\''b' 'n' 'n' 'd$l_x128' 'dxx' 'n'" \
		"$g_zxfer_shell_command_result"
	assertFalse "Both forms should publish only the result global, without stdout." \
		"[ -s '$TEST_TMPDIR/render-stdout' ]"
}

test_build_remote_sh_c_command_restores_lc_all_and_publishes_its_result() {
	l_utf8_locale=$(zxfer_test_find_utf8_locale)
	l_mb_char=$(printf '\342\202\254')
	l_mb8=$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char
	l_mb64=$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8$l_mb8
	l_mb264=$l_mb64$l_mb64$l_mb64$l_mb64$l_mb8
	l_lc_all_file="$TEST_TMPDIR/build_remote_sh_c_lc_all.out"
	(
		LC_ALL=POSIX
		export LC_ALL
		zxfer_build_remote_sh_c_command 'exit 7'
		printf 'set=%s result=%s\n' "${LC_ALL-unset}" "$g_zxfer_shell_command_result"
		unset LC_ALL
		LANG=$l_utf8_locale
		export LANG
		zxfer_build_remote_sh_c_command "printf '%s' '$l_mb264'"
		case $g_zxfer_shell_command_result in
		"'sh' '-c' 'l_nl="*) l_mb_form=chunked ;;
		*) l_mb_form=plain ;;
		esac
		printf 'unset=%s mb=%s\n' "${LC_ALL-unset}" "$l_mb_form"
	) >"$l_lc_all_file"
	l_lc_all_output=$(cat "$l_lc_all_file")

	assertEquals "The renderer should restore a set LC_ALL, chunk multibyte >768B scripts under exported LANG while keeping LC_ALL unset, and publish its result in the current shell." \
		"set=POSIX result='sh' '-c' 'exit 7'
unset=unset mb=chunked" "$l_lc_all_output"
}

test_build_remote_sh_c_command_retains_input_from_the_canonical_result() {
	# The chunked script must keep stdin and its status even when the input
	# arrives through the same result channel that rendering overwrites.
	g_zxfer_shell_command_result='IFS= read -r l_input;
printf "%s\n" "$l_input";
exit 7;'
	zxfer_build_remote_sh_c_command "$g_zxfer_shell_command_result"
	printf '%s\n' "kept 'quoted' input" |
		/bin/sh -c "$g_zxfer_shell_command_result" >"$TEST_TMPDIR/canonical-input"
	l_canonical_status=$?
	assertEquals "Rendering from the canonical channel keeps the script's status." \
		7 "$l_canonical_status"
	assertEquals "The remote bootstrap keeps the script and stdin intact." \
		"kept 'quoted' input" "$(cat "$TEST_TMPDIR/canonical-input")"
}

test_build_remote_sh_c_command_chunks_long_scripts_that_run_intact() {
	l_capture_sh="$TEST_TMPDIR/sh"
	l_script_capture="$TEST_TMPDIR/remote-script.capture"
	l_stdin_capture="$TEST_TMPDIR/remote-stdin.capture"
	l_expected_script="$TEST_TMPDIR/remote-script.expected"
	zxfer_test_write_remote_sh_capture_bin "$l_capture_sh"

	l_long_script=""
	l_padding_line="# quote ' double \" dollar \$ parens () semicolon ; wildcard * question ?"
	l_padding_count=0
	while [ "$l_padding_count" -lt 90 ]; do
		l_long_script=$l_long_script$l_padding_line'
'
		l_padding_count=$((l_padding_count + 1))
	done
	l_long_script=$l_long_script'printf "%s\n" reached;
'
	printf '%s' "$l_long_script" >"$l_expected_script"
	zxfer_build_remote_sh_c_command "$l_long_script"
	l_rendered_command=$g_zxfer_shell_command_result
	l_rendered_lines=$(printf '%s\n' "$l_rendered_command" | wc -l | tr -d '[:space:]')
	l_max_word_bytes=$(printf '%s\n' "$l_rendered_command" |
		zxfer_test_max_rendered_shell_word_bytes)

	ZXFER_TEST_REAL_SH=/bin/sh \
		ZXFER_TEST_SCRIPT_CAPTURE=$l_script_capture \
		ZXFER_TEST_STDIN_CAPTURE=$l_stdin_capture \
		ZXFER_TEST_INNER_STATUS=37 \
		PATH="$TEST_TMPDIR:$PATH" \
		/bin/sh -c "$l_rendered_command" <<'EOF'
stdin-through-bootstrap
EOF
	l_status=$?

	assertEquals "A chunked command should remain one physical login-shell line." \
		1 "$l_rendered_lines"
	assertTrue "Every rendered word should stay below illumos csh's 1020-byte lexical limit (maximum $l_max_word_bytes)." \
		"[ '$l_max_word_bytes' -lt 1020 ]"
	assertEquals "The inner sh status should pass through the exec bootstrap unchanged." \
		37 "$l_status"
	assertTrue "The chunk bootstrap should reconstruct quotes, metacharacters, and trailing newlines byte-for-byte." \
		"cmp '$l_expected_script' '$l_script_capture' >/dev/null 2>&1"
	assertEquals "The chunk bootstrap should leave the original stdin attached to the inner script." \
		"stdin-through-bootstrap" "$(cat "$l_stdin_capture")"
}

zxfer_test_run_ssh_command_mode() {
	(
		zxfer_throw_error() {
			printf 'throw: %s\n' "$1"
			exit 3
		}
		if [ "$1" = run ]; then
			zxfer_invoke_ssh_shell_command_for_host "$2" "$3"
		elif [ "$1" = publish ]; then
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$2" "$3"
		else
			zxfer_ssh_shell_command_for_host "$1" "$2" "$3"
		fi
	)
}

test_parse_ssh_host_spec_splits_literal_tokens_and_rejects_shell_syntax() {
	zxfer_test_exec_fixture_setup
	# Each row: spec|status|ssh host|quoted wrapper. Tokens split on
	# whitespace without a shell parser, so ";" stays inside its token.
	while IFS='|' read -r l_spec l_expected_status l_expected_host l_expected_wrapper; do
		zxfer_parse_ssh_host_spec "$l_spec"
		l_status=$?
		assertEquals "[$l_spec] status" "$l_expected_status" "$l_status"
		assertEquals "[$l_spec] ssh host" "$l_expected_host" "$g_zxfer_ssh_shell_host_result"
		assertEquals "[$l_spec] wrapper" "$l_expected_wrapper" "$g_zxfer_ssh_wrapper_result"
	done <<'EOF'
user@host pfexec -p 2222|0|user@host|'pfexec' '-p' '2222'
backup.example.com; touch /tmp/pwn|0|backup.example.com;|'touch' '/tmp/pwn'
|0||
   |0||
user@host "ZFS Admin"|1||
user@host 'doas'|1||
user@host pf\exec|1||
EOF
	zxfer_parse_ssh_host_spec "user@host pfexec -p 2222"
	assertEquals "The raw tokens should be published one per line." \
		"$(printf '%s\n' user@host pfexec -p 2222)" "$g_zxfer_ssh_host_spec_tokens_result"
	zxfer_parse_ssh_host_spec 'user@host "ZFS Admin"'
	assertEquals "A rejected spec should explain the literal-token requirement." \
		"Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$g_zxfer_ssh_shell_context_error_result"
}

test_zfs_role_commands_run_each_role_where_its_zfs_lives() {
	zxfer_test_exec_fixture_setup
	l_local_zfs="$TEST_TMPDIR/role_local_zfs"
	printf '#!/bin/sh\nprintf "local %%s\\n" "$*"\n' >"$l_local_zfs"
	chmod +x "$l_local_zfs"
	l_ssh_log="$TEST_TMPDIR/role_ssh.log"
	l_out="$TEST_TMPDIR/role.out"
	: >"$l_ssh_log"
	FAKE_SSH_LOG=$l_ssh_log
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs=$l_local_zfs
	g_origin_cmd_zfs=/origin/sbin/zfs
	g_target_cmd_zfs=/target/usr/sbin/zfs
	g_option_O_origin_host=origin.example
	g_option_T_target_host=target.example
	g_option_V_very_verbose=1
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_get_calls=0

	# Each role runs its own zfs: over ssh for an -O/-T host, else directly.
	zxfer_run_zfs_cmd_for_role source get name tank/src >"$l_out" 2>/dev/null
	l_source="$?|$(tr '\n' ' ' <"$l_ssh_log")"
	: >"$l_ssh_log"
	zxfer_run_zfs_cmd_for_role destination get name tank/dst >"$l_out" 2>/dev/null
	l_destination="$?|$(tr '\n' ' ' <"$l_ssh_log")"
	: >"$l_ssh_log"
	g_option_T_target_host=""
	zxfer_run_zfs_cmd_for_role destination get name tank/dst >"$l_out"
	l_local_destination="$?|$(cat "$l_out")"
	zxfer_run_zfs_cmd_for_role local get name tank/other >"$l_out"
	l_local="$?|$(cat "$l_out")"
	zxfer_run_zfs_cmd_for_role /sbin/zfs get name tank/fs >"$l_out" 2>"$TEST_TMPDIR/role.err"
	l_unknown="$?|$(cat "$l_out" "$TEST_TMPDIR/role.err")"
	# Rendering routes the same way; the rendered -T command runs the same argv.
	g_option_T_target_host=target.example
	zxfer_render_zfs_command_for_role destination list -H backup/target
	zxfer_execute_rendered_shell_command "$g_zxfer_shell_command_result"
	l_rendered_destination=$(tr '\n' ' ' <"$l_ssh_log")
	zxfer_render_zfs_command_for_role local list -H backup/target
	l_rendered_local="$?|$g_zxfer_shell_command_result"
	l_rendered_unknown=$(zxfer_render_zfs_command_for_role /sbin/zfs list 2>&1)
	l_rendered_unknown="$?|$l_rendered_unknown"
	# An ssh command run without a side counts on the side of its host's role.
	g_zxfer_profile_source_ssh_shell_invocations=0
	zxfer_invoke_ssh_shell_command_for_host origin.example "'true'" 2>/dev/null
	l_sideless_ssh=$g_zxfer_profile_source_ssh_shell_invocations
	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "The source role should run the -O host's zfs over ssh." \
		"0|-o BatchMode=yes -o StrictHostKeyChecking=yes origin.example '/origin/sbin/zfs' 'get' 'name' 'tank/src' " \
		"$l_source"
	assertEquals "The destination role should run the -T host's zfs over ssh." \
		"0|-o BatchMode=yes -o StrictHostKeyChecking=yes target.example '/target/usr/sbin/zfs' 'get' 'name' 'tank/dst' " \
		"$l_destination"
	assertEquals "A destination without -T should run the local zfs directly." \
		"0|local get name tank/dst" "$l_local_destination"
	assertEquals "The local role should run the local zfs directly." \
		"0|local get name tank/other" "$l_local"
	assertEquals "An unknown role should fail closed, name the role and run nothing." \
		"1|zxfer: unknown zfs command role [/sbin/zfs]." "$l_unknown"
	assertEquals "A rendered -T command should run the -T host's zfs over ssh." \
		"-o BatchMode=yes -o StrictHostKeyChecking=yes target.example '/target/usr/sbin/zfs' 'list' '-H' 'backup/target' " \
		"$l_rendered_destination"
	assertEquals "The local role should render the local zfs argv." \
		"0|'$l_local_zfs' 'list' '-H' 'backup/target'" "$l_rendered_local"
	assertEquals "Rendering an unknown role should fail closed and name the role." \
		"1|zxfer: unknown zfs command role [/sbin/zfs]." "$l_rendered_unknown"
	assertEquals "-V should count each run on its role's side, local destinations included." \
		"source=1 destination=2 other=1 get=4" \
		"source=$g_zxfer_profile_source_zfs_calls destination=$g_zxfer_profile_destination_zfs_calls other=$g_zxfer_profile_other_zfs_calls get=$g_zxfer_profile_zfs_get_calls"
	assertEquals "-V should count a side-less ssh command to the -O host on the source side." \
		1 "$l_sideless_ssh"
}

test_zfs_dispatch_preserves_run_status_and_render_does_not_record_a_call() {
	zxfer_test_exec_fixture_setup
	output=$(
		(
			g_cmd_zfs=/bin/sh
			g_cmd_ssh=$FAKE_SSH_BIN
			g_option_V_very_verbose=1
			g_option_O_origin_host=""
			g_zxfer_profile_source_zfs_calls=0
			zxfer_run_source_zfs_cmd -c 'exit 23' >/dev/null
			printf 'local=%s\n' "$?"
			g_option_O_origin_host='origin.example doas'
			FAKE_SSH_EXIT_STATUS=37
			FAKE_SSH_SUPPRESS_STDOUT=1
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_SUPPRESS_STDOUT
			zxfer_run_source_zfs_cmd list tank/src >/dev/null 2>/dev/null
			printf 'remote=%s calls=%s\n' "$?" "$g_zxfer_profile_source_zfs_calls"
			zxfer_render_zfs_command_for_role source list tank/src
			printf 'render=%s calls=%s\n' "$?" "$g_zxfer_profile_source_zfs_calls"
		)
	)
	status=$?
	assertEquals "The dispatch check should complete normally." 0 "$status"
	assertEquals "Runs preserve local and ssh statuses; rendering adds no recorded call." \
		"local=23
remote=37 calls=2
render=0 calls=2" "$output"
}

test_ssh_command_modes_fail_closed_before_ssh_runs() {
	zxfer_test_exec_fixture_setup
	l_ssh_log="$TEST_TMPDIR/fail_closed_ssh.log"
	: >"$l_ssh_log"
	FAKE_SSH_LOG=$l_ssh_log
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	l_policy_message="throw: ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
	l_quoted_message="throw: Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
	l_results=""
	for l_mode in render run record publish; do
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
		export ZXFER_SSH_USER_KNOWN_HOSTS_FILE
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" backup.example "'zfs' 'list'" 2>&1)
		l_results="$l_results$l_mode policy=$?|$l_output
"
		unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" 'backup.example "pfexec -u zfs"' "'zfs' 'list'" 2>&1)
		l_results="$l_results$l_mode quoted=$?|$l_output
"
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" "" "'zfs' 'list'" 2>&1)
		l_results="$l_results$l_mode empty_host=$?|$l_output
"
		l_output=$(zxfer_test_run_ssh_command_mode "$l_mode" backup.example "" 2>&1)
		l_results="$l_results$l_mode empty_command=$?|$l_output
"
	done
	g_zxfer_shell_command_result=stale
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw backup.example ""
	l_published="$?|$g_zxfer_shell_command_result"
	unset FAKE_SSH_LOG

	l_expected=""
	for l_mode in render run record publish; do
		l_expected="$l_expected$l_mode policy=3|$l_policy_message
$l_mode quoted=3|$l_quoted_message
$l_mode empty_host=1|
$l_mode empty_command=1|
"
	done
	assertEquals "Every mode should throw a bad policy or a host spec that needs shell quoting, and return 1 for an empty host or command." \
		"$l_expected" "$l_results"
	assertEquals "No failure may start ssh." "" "$(cat "$l_ssh_log")"
	assertEquals "An empty command should clear the published command." "1|" "$l_published"
}

# The renderer shares quoting's result, so retain an input passed through that
# same channel before any helper rewrites it; failed builders publish no text.
test_prepared_ssh_renderer_keeps_input_and_clears_partial_results_on_failure() {
	zxfer_test_exec_fixture_setup
	for builder_status in 0 37; do
		output=$(
			g_zxfer_shell_command_result="'zfs' 'list' 'tank/data'"
			zxfer_ssh_shell_command_for_host() {
				printf 'input=%s\n' "$3"
				g_zxfer_shell_command_result='complete ssh argv'
				return "$builder_status"
			}
			status=0
			zxfer_publish_prepared_ssh_shell_command_for_host_or_throw backup.example \
				"$g_zxfer_shell_command_result" || status=$?
			printf 'status=%s result=<%s>\n' "$status" "$g_zxfer_shell_command_result"
		)
		expected_result='complete ssh argv'
		[ "$builder_status" -eq 0 ] || expected_result=""
		assertEquals "The canonical result retains only successful renderer output." \
			"input='zfs' 'list' 'tank/data'
status=$builder_status result=<$expected_result>" "$output"
	done
}

test_zfs_and_ssh_runs_record_their_argv_as_the_last_command() {
	zxfer_test_exec_fixture_setup
	l_ssh_log="$TEST_TMPDIR/record_ssh.log"
	: >"$l_ssh_log"
	FAKE_SSH_LOG=$l_ssh_log
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs=/bin/echo
	g_option_O_origin_host=""

	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null
	l_safe=$g_zxfer_failure_last_command
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null
	l_local=$g_zxfer_failure_last_command
	g_option_O_origin_host=backup@example.com
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	zxfer_invoke_ssh_shell_command_for_host backup@example.com "zfs list -H tank/src" >/dev/null
	l_run=$g_zxfer_failure_last_command
	: >"$l_ssh_log"
	# Record mode stores the argv for a caller that runs ssh in a subshell.
	g_option_V_very_verbose=1
	g_zxfer_profile_source_ssh_shell_invocations=0
	zxfer_ssh_shell_command_for_host record "backup@example.com doas" "zfs list" source \
		2>"$TEST_TMPDIR/record.err"
	l_record="$?|$g_zxfer_failure_last_command"
	l_record_side_effects="ssh=<$(cat "$l_ssh_log")> count=$g_zxfer_profile_source_ssh_shell_invocations stderr=<$(cat "$TEST_TMPDIR/record.err")>"
	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS

	assertEquals "Safe report mode should redact the last command." "[redacted]" "$l_safe"
	assertEquals "Unsafe report mode should quote a local zfs argv." \
		"'/bin/echo' 'list' '-H' 'tank/src'" "$l_local"
	assertEquals "Unsafe report mode should keep every ssh token boundary." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock' 'backup@example.com' 'zfs list -H tank/src'" \
		"$l_run"
	assertEquals "Record mode should store the argv run mode would execute." \
		"0|'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup@example.com' ''\"'\"'doas'\"'\"' zfs list'" \
		"$l_record"
	assertEquals "Record mode should neither run ssh, count it nor print the -V line." \
		"ssh=<> count=0 stderr=<>" "$l_record_side_effects"
}

test_remote_command_context_label_names_the_role() {
	zxfer_test_exec_fixture_setup
	g_option_O_origin_host=origin.example
	g_option_T_target_host="target.example doas"
	# Each row: host spec|side|label. Without a side the -O/-T match decides.
	while IFS='|' read -r l_host l_side l_expected_label; do
		zxfer_get_remote_command_context_label_into_result "$l_host" "$l_side"
		assertEquals "[$l_host|$l_side] label" "$l_expected_label" \
			"$g_zxfer_remote_command_context_label_result"
	done <<'EOF'
origin.example|source|origin: origin.example
target.example doas|destination|target: target.example doas
misc.example|other|remote: misc.example
origin.example||origin: origin.example
target.example doas||target: target.example doas
misc.example||remote: misc.example
||remote
EOF
	g_option_T_target_host=origin.example
	zxfer_get_remote_command_context_label_into_result origin.example
	assertEquals "A spec that is both -O and -T should be origin/target without a side." \
		"origin/target: origin.example" "$g_zxfer_remote_command_context_label_result"
	g_option_V_very_verbose=1
	assertEquals "-V should print the labeled command, quoted once." \
		"Running remote command [remote: misc.example doas]: '/bin/echo' 'hello'" \
		"$(zxfer_echoV_remote_command_for_host "misc.example doas" other /bin/echo hello 2>&1)"
}

test_remote_command_context_label_into_result_publishes_in_current_shell_and_resets() {
	zxfer_test_exec_fixture_setup
	l_out=$(
		g_option_O_origin_host=origin.example
		g_option_T_target_host="target.example doas"
		l_stdout_file="$TEST_TMPDIR/context-label-into-result.stdout"
		: >"$l_stdout_file"
		while IFS='|' read -r l_host l_side; do
			zxfer_get_remote_command_context_label_into_result "$l_host" "$l_side" >>"$l_stdout_file"
			printf '%s\n' "$g_zxfer_remote_command_context_label_result"
		done <<'EOF'
origin.example|source
target.example doas|destination
misc.example|other
origin.example|
target.example doas|
misc.example|
|
EOF
		g_option_T_target_host=origin.example
		zxfer_get_remote_command_context_label_into_result origin.example >>"$l_stdout_file"
		printf '%s\n' "$g_zxfer_remote_command_context_label_result"

		g_option_V_very_verbose=1
		g_zxfer_remote_command_context_label_result=""
		g_zxfer_rendered_report_command_result=""
		l_echo_err="$TEST_TMPDIR/context-label-echoV.err"
		zxfer_echoV_remote_command_for_host "misc.example doas" other /bin/echo "a'b" 2>"$l_echo_err"
		printf 'echoV=%s\n' "$(cat "$l_echo_err")"
		printf 'in_shell=%s|%s\n' \
			"$g_zxfer_remote_command_context_label_result" \
			"$g_zxfer_rendered_report_command_result"

		g_cmd_ssh=/usr/bin/true
		l_open_err="$TEST_TMPDIR/context-label-openV.err"
		zxfer_run_ssh_control_socket_action open origin.example "$TEST_TMPDIR/openV.sock" 2>"$l_open_err"
		wait "$g_zxfer_ssh_control_socket_action_pid" 2>/dev/null || :
		printf 'openV=%s\n' "$(cat "$l_open_err")"
		printf 'open_in_shell=%s\n' "$g_zxfer_remote_command_context_label_result"

		zxfer_reset_ssh_transport_state
		printf 'after_reset=<%s>\n' "$g_zxfer_remote_command_context_label_result"
		printf 'stdout_bytes=%s\n' "$(wc -c <"$l_stdout_file" | tr -d ' ')"
	)

	assertEquals "The in-shell context-label helper should publish all role forms with no stdout, feed -V formatting in-shell, and clear on reset." \
		"origin: origin.example
target: target.example doas
remote: misc.example
origin: origin.example
target: target.example doas
remote: misc.example
remote
origin/target: origin.example
echoV=Running remote command [remote: misc.example doas]: '/bin/echo' 'a'\"'\"'b'
in_shell=remote: misc.example doas|'/bin/echo' 'a'\"'\"'b'
openV=Opening ssh control socket [origin/target: origin.example]: '/usr/bin/true' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-M' '-S' '$TEST_TMPDIR/openV.sock' '-fN' 'origin.example'
open_in_shell=origin/target: origin.example
after_reset=<>
stdout_bytes=0" "$l_out"
}

test_ssh_shell_command_render_fuzzes_wrapper_specs_and_control_socket_paths() {
	zxfer_test_exec_fixture_setup
	marker_rel="control_socket_fuzz_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	case_file="$TEST_TMPDIR/control_socket_fuzz_cases.txt"
	zxfer_build_remote_sh_c_command "printf ok >/dev/null"
	safe_cmd=$g_zxfer_shell_command_result
	cat >"$case_file" <<EOF
backup@example.com doas|$TEST_TMPDIR/socket,comma
backup@example.com pfexec -u root|$TEST_TMPDIR/socket=equals
backup@example.com env LC_ALL=C doas|$TEST_TMPDIR/socket:semicolon;literal
backup@example.com doas|$TEST_TMPDIR/socket.\$(touch $marker_rel)
EOF

	case_index=0
	while IFS='|' read -r host_spec socket_path || [ -n "$host_spec$socket_path" ]; do
		[ -n "$host_spec" ] || continue
		case_index=$((case_index + 1))
		log_file="$TEST_TMPDIR/control_socket_fuzz_$case_index.log"
		: >"$log_file"
		rm -f "$marker"
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host=$host_spec
		g_ssh_origin_control_socket=$socket_path
		FAKE_SSH_LOG="$log_file"
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		zxfer_ssh_shell_command_for_host render "$host_spec" "$safe_cmd"
		cmd=$g_zxfer_shell_command_result
		(
			cd "$TEST_TMPDIR" || exit 1
			zxfer_execute_rendered_shell_command "$cmd"
		)

		unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		assertFalse "Control-socket fuzz case $case_index should not execute command substitutions from the socket path." \
			"[ -e '$marker' ]"
		assertEquals "Control-socket fuzz case $case_index should force batch mode first." "-o" "$(sed -n '1p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass BatchMode=yes as the first managed transport option." "BatchMode=yes" "$(sed -n '2p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should force strict host-key checking next." "-o" "$(sed -n '3p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass StrictHostKeyChecking=yes as the second managed transport option." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass -S separately." "-S" "$(sed -n '5p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should preserve the literal control-socket path." \
			"$socket_path" "$(sed -n '6p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should keep the ssh host token separate from wrappers." \
			"backup@example.com" "$(sed -n '7p' "$log_file")"
		log_line_remote_cmd=$(sed -n '8p' "$log_file")
		assertContains "Control-socket fuzz case $case_index should preserve the quoted remote command payload." \
			"$log_line_remote_cmd" "'sh' '-c' 'printf ok >/dev/null'"

		case "$host_spec" in
		*" doas"*)
			assertContains "Control-socket fuzz case $case_index should keep doas in the remote wrapper chain." \
				"$log_line_remote_cmd" "'doas'"
			;;
		esac
		case "$host_spec" in
		*"pfexec -u root"*)
			assertContains "Control-socket fuzz case $case_index should keep pfexec wrapper tokens quoted." \
				"$log_line_remote_cmd" "'pfexec' '-u' 'root'"
			;;
		esac
		case "$host_spec" in
		*"LC_ALL=C doas"*)
			assertContains "Control-socket fuzz case $case_index should keep env-style wrapper tokens quoted." \
				"$log_line_remote_cmd" "'env' 'LC_ALL=C' 'doas'"
			;;
		esac
	done <"$case_file"
}

test_zxfer_load_ssh_transport_policy_validates_each_variable() {
	zxfer_test_exec_fixture_setup
	nl='
'
	# ZXFER_SSH_USE_AMBIENT_CONFIG takes 1, yes, true or on in any case and
	# then ignores the other variables; any other value keeps the policy.
	for l_ambient in 1 yes YES True on; do
		ZXFER_SSH_USE_AMBIENT_CONFIG=$l_ambient
		ZXFER_SSH_BATCH_MODE="bad${nl}value"
		zxfer_load_ssh_transport_policy
		assertEquals "Ambient config [$l_ambient] should add no options." \
			"0:" "$?:$g_zxfer_ssh_policy_options"
	done
	unset ZXFER_SSH_BATCH_MODE
	for l_ambient in 0 no off; do
		ZXFER_SSH_USE_AMBIENT_CONFIG=$l_ambient
		zxfer_load_ssh_transport_policy
		assertEquals "[$l_ambient] should keep the managed default options." \
			"0:-o
BatchMode=yes
-o
StrictHostKeyChecking=yes" "$?:$g_zxfer_ssh_policy_options"
	done
	unset ZXFER_SSH_USE_AMBIENT_CONFIG

	for invalid_case in \
		"ZXFER_SSH_BATCH_MODE|bad${nl}value|ZXFER_SSH_BATCH_MODE must be a single-line non-empty value." \
		"ZXFER_SSH_STRICT_HOST_KEY_CHECKING|bad${nl}policy|ZXFER_SSH_STRICT_HOST_KEY_CHECKING must be a single-line non-empty value." \
		"ZXFER_SSH_USER_KNOWN_HOSTS_FILE|/bad${nl}path|ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be a single-line non-empty value." \
		"ZXFER_SSH_USER_KNOWN_HOSTS_FILE|relative/known_hosts|ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."; do
		invalid_name=${invalid_case%%|*}
		invalid_rest=${invalid_case#*|}
		invalid_value=${invalid_rest%%|*}
		invalid_message=${invalid_rest#*|}
		ZXFER_SSH_BATCH_MODE=yes
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING=yes
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE=""
		case $invalid_name in
		ZXFER_SSH_BATCH_MODE) ZXFER_SSH_BATCH_MODE=$invalid_value ;;
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING) ZXFER_SSH_STRICT_HOST_KEY_CHECKING=$invalid_value ;;
		*) ZXFER_SSH_USER_KNOWN_HOSTS_FILE=$invalid_value ;;
		esac
		zxfer_load_ssh_transport_policy
		status=$?
		assertEquals "Invalid $invalid_name values should fail the policy closed." 1 "$status"
		assertEquals "Invalid $invalid_name values should explain the problem." \
			"$invalid_message" "$g_zxfer_ssh_policy_error"
		assertEquals "Invalid $invalid_name values should publish no options." \
			"" "$g_zxfer_ssh_policy_options"
	done
	unset ZXFER_SSH_BATCH_MODE ZXFER_SSH_STRICT_HOST_KEY_CHECKING ZXFER_SSH_USER_KNOWN_HOSTS_FILE
}

test_zxfer_prepare_ssh_transport_validates_the_policy_once_per_run() {
	zxfer_test_exec_fixture_setup
	output=$(
		(
			g_cmd_ssh=/usr/bin/ssh
			ZXFER_SSH_BATCH_MODE='bad
value'
			zxfer_prepare_ssh_transport
			printf 'invalid=%s ready=<%s> error=%s\n' "$?" \
				"$g_zxfer_ssh_transport_ready" "$g_zxfer_ssh_transport_error"
			ZXFER_SSH_BATCH_MODE=no
			zxfer_prepare_ssh_transport
			printf 'valid=%s ready=<%s>\n' "$?" "$g_zxfer_ssh_transport_ready"
			load_calls=0
			zxfer_load_ssh_transport_policy() {
				load_calls=$((load_calls + 1))
				return 1
			}
			zxfer_prepare_ssh_transport
			printf 'again=%s load_calls=%s\n' "$?" "$load_calls"
			zxfer_reset_ssh_transport_state
			zxfer_prepare_ssh_transport
			printf 'after_reset=%s load_calls=%s ready=<%s>\n' "$?" "$load_calls" \
				"$g_zxfer_ssh_transport_ready"
			zxfer_load_ssh_transport_policy() { return 0; }
			g_cmd_ssh=""
			g_zxfer_secure_path="$TEST_TMPDIR/no-ssh-here"
			zxfer_prepare_ssh_transport
			printf 'no_ssh=%s ready=<%s> error=%s\n' "$?" \
				"$g_zxfer_ssh_transport_ready" "$g_zxfer_ssh_transport_error"
		)
	)

	assertEquals "A failed policy or ssh lookup is checked again; the first success holds until the session reset." \
		"invalid=1 ready=<> error=ZXFER_SSH_BATCH_MODE must be a single-line non-empty value.
valid=0 ready=<1>
again=0 load_calls=0
after_reset=1 load_calls=1 ready=<>
no_ssh=1 ready=<> error=Required dependency \"ssh\" not found in secure PATH ($TEST_TMPDIR/no-ssh-here). Set ZXFER_SECURE_PATH or install the binary." "$output"
}

test_local_ssh_lookup_publishes_paths_and_errors_without_an_alias() {
	zxfer_test_remote_host_fixture_setup || return
	result=$(
		g_cmd_ssh=""
		l_ssh_lookup_test_calls=0
		zxfer_find_required_tool() {
			l_ssh_lookup_test_calls=$((l_ssh_lookup_test_calls + 1))
			if [ "$l_ssh_lookup_test_calls" -eq 2 ]; then
				g_zxfer_required_tool_result="ssh lookup failed"
				return 37
			fi
			g_zxfer_required_tool_result="/trusted/ssh"
		}
		g_zxfer_required_tool_result=stale
		zxfer_ensure_local_ssh_command || exit 11
		printf 'lookup=%s|%s|%s\n' "$g_zxfer_required_tool_result" "$g_cmd_ssh" "$l_ssh_lookup_test_calls"
		g_zxfer_required_tool_result="another helper"
		zxfer_ensure_local_ssh_command || exit 12
		printf 'cached=%s|%s\n' "$g_zxfer_required_tool_result" "$l_ssh_lookup_test_calls"
		g_cmd_ssh=""
		g_zxfer_required_tool_result=stale
		zxfer_ensure_local_ssh_command
		printf 'failed=%s|%s|%s|%s\n' "$?" "$g_zxfer_required_tool_result" "$g_cmd_ssh" "$l_ssh_lookup_test_calls"
		zxfer_ensure_local_ssh_command || exit 13
		printf 'retry=%s|%s|%s\n' "$g_zxfer_required_tool_result" "$g_cmd_ssh" "$l_ssh_lookup_test_calls"
	)
	status=$?

	assertEquals "Successful lookups and cache hits should publish paths; failure should preserve its diagnostic and allow retry." \
		"0|lookup=/trusted/ssh|/trusted/ssh|1
cached=/trusted/ssh|1
failed=1|ssh lookup failed||2
retry=/trusted/ssh|/trusted/ssh|3" "$status|$result"
}

# Run zxfer_ensure_ssh_control_socket_dir in a subshell for one failure CASE
# of the short-directory fallback under ROOT, and print
# "CASE=STATUS handle=<SHORT_DIR> dir=<RESULT>".
# Usage: zxfer_test_socket_dir_failure_case CASE ROOT
zxfer_test_socket_dir_failure_case() {
	(
		l_case=$1
		l_root=$2
		g_zxfer_ssh_control_socket_dir_result=""
		g_zxfer_run_tmp_root="$l_root/long-run-root"
		zxfer_ensure_run_tmp_root() {
			[ "$l_case" != no_run_root ]
		}
		# Only paths under the run root are too long, unless every path is.
		zxfer_is_ssh_control_socket_path_short_enough() {
			[ "$l_case" != still_too_long ] &&
				[ "${1#"$g_zxfer_run_tmp_root"/}" = "$1" ]
		}
		zxfer_find_default_tmpdir() {
			[ "$l_case" != no_default_root ] || return 1
			g_zxfer_temp_root_candidate_result=$l_root/default
			[ "$l_case" != mktemp_fails ] ||
				g_zxfer_temp_root_candidate_result=$l_root/missing-parent
		}
		[ "$l_case" != foreign_answer ] || PATH="$l_root/fake-mktemp:$PATH"
		[ "$l_case" != slash_suffix ] || PATH="$l_root/fake-mktemp-slash:$PATH"
		[ "$l_case" != newline_suffix ] || PATH="$l_root/fake-mktemp-nl:$PATH"
		zxfer_ensure_ssh_control_socket_dir
		printf '%s=%s handle=<%s> dir=<%s>\n' "$l_case" "$?" \
			"$g_zxfer_ssh_control_socket_short_dir" \
			"$g_zxfer_ssh_control_socket_dir_result"
	)
}

# Run zxfer_open_ssh_control_sockets for -O origin.example in a subshell for
# one failure CASE under ROOT, and print what it threw and the origin socket
# it kept.
# Usage: zxfer_test_open_failure_case CASE ROOT
zxfer_test_open_failure_case() {
	(
		l_case=$1
		l_root=$2
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_ssh_supports_control_sockets=1
		g_option_O_origin_host=origin.example
		zxfer_ensure_ssh_control_socket_dir() {
			[ "$l_case" != no_socket_dir ] || return 1
			g_zxfer_ssh_control_socket_dir_result=$l_root/dir
			[ "$l_case" != planted_path ] ||
				g_zxfer_ssh_control_socket_dir_result=$l_root/planted
		}
		if [ "$l_case" = rejected_start ]; then
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_stderr="host spec rejected"
				return 1
			}
		fi
		zxfer_throw_error() {
			printf '%s: throw=%s socket=<%s>\n' "$l_case" "$1" \
				"$g_ssh_origin_control_socket"
			exit 1
		}
		zxfer_open_ssh_control_sockets
	)
}

test_ssh_control_socket_dir_fails_closed_without_a_short_private_directory() {
	zxfer_test_remote_host_fixture_setup || return
	l_root="$TEST_TMPDIR/socket_dir_failures"
	mkdir -p "$l_root/fake-mktemp" "$l_root/fake-mktemp-slash" "$l_root/fake-mktemp-nl" "$l_root/default"
	printf '#!/bin/sh\nexit 0\n' >"$l_root/fake-mktemp/mktemp"
	printf '#!/bin/sh\nprintf "%%s/zxfer.ssh.abc/nested\\n" "%s"\n' "$l_root/default" >"$l_root/fake-mktemp-slash/mktemp"
	printf '#!/bin/sh\nprintf "%%s/zxfer.ssh.abc\\nextra\\n" "%s"\n' "$l_root/default" >"$l_root/fake-mktemp-nl/mktemp"
	chmod 755 "$l_root/fake-mktemp/mktemp" "$l_root/fake-mktemp-slash/mktemp" "$l_root/fake-mktemp-nl/mktemp"
	l_long_suffix=""
	while [ "${#l_long_suffix}" -lt 150 ]; do
		l_long_suffix="${l_long_suffix}xxxxxxxxxx"
	done
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/zxfer-short/ssh-origin.sock"
	l_short_status=$?
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/$l_long_suffix/ssh-origin.sock"
	l_long_status=$?
	l_output=$(
		for l_case in no_run_root no_default_root mktemp_fails foreign_answer slash_suffix newline_suffix still_too_long; do
			zxfer_test_socket_dir_failure_case "$l_case" "$l_root"
		done
	)

	assertEquals "The sun_path check should accept a short socket path and reject a long one." \
		"0 1" "$l_short_status $l_long_status"
	assertEquals "Without a run root, a default temp root, a direct mktemp directory named from the template, or one short enough, no socket directory may be used." \
		"no_run_root=1 handle=<> dir=<>
no_default_root=1 handle=<> dir=<>
mktemp_fails=1 handle=<> dir=<>
foreign_answer=1 handle=<> dir=<>
slash_suffix=1 handle=<> dir=<>
newline_suffix=1 handle=<> dir=<>
still_too_long=1 handle=<> dir=<>" "$l_output"
	assertEquals "A short directory still too long should be removed at once." \
		"" "$(ls -A "$l_root/default")"
}

test_ssh_control_socket_path_length_check_counts_utf8_bytes_and_restores_lc_all() {
	l_mb_char=$(printf '\342\202\254')
	l_mb5=$l_mb_char$l_mb_char$l_mb_char$l_mb_char$l_mb_char
	# 30 UTF-8 3-byte chars = 90 bytes; with /tmp/ (5) + /s (2) + 17-byte suffix
	# = 114 bytes (>= 104) while character length is only 54 (< 104).
	l_mb_long="/tmp/$l_mb5$l_mb5$l_mb5$l_mb5$l_mb5$l_mb5/s"
	# 20 UTF-8 3-byte chars = 60 bytes; total = 84 bytes (< 104).
	l_mb_short="/tmp/$l_mb5$l_mb5$l_mb5$l_mb5/s"

	l_utf8_locale=$(zxfer_test_find_utf8_locale)
	l_out=$(
		LC_ALL=$l_utf8_locale
		export LC_ALL
		zxfer_is_ssh_control_socket_path_short_enough "$l_mb_long"
		l_long_set=$?
		l_lc_after_set=${LC_ALL-unset}

		unset LC_ALL
		LANG=$l_utf8_locale
		export LANG
		zxfer_is_ssh_control_socket_path_short_enough "$l_mb_long"
		l_long_unset=$?
		zxfer_is_ssh_control_socket_path_short_enough "$l_mb_short"
		l_short_unset=$?
		l_lc_after_unset=${LC_ALL-unset}

		printf 'long_set=%s|lc_set=%s|long_unset=%s|short_unset=%s|lc_unset=%s\n' \
			"$l_long_set" "$l_lc_after_set" "$l_long_unset" "$l_short_unset" "$l_lc_after_unset"
	)

	assertEquals "sun_path validation must bound multibyte paths by UTF-8 byte length and restore LC_ALL." \
		"long_set=1|lc_set=$l_utf8_locale|long_unset=1|short_unset=0|lc_unset=unset" "$l_out"
}

test_zxfer_remove_ssh_control_socket_dir_removes_only_ssh_names_and_fails_closed() {
	zxfer_test_remote_host_fixture_setup || return
	branch_root="$TEST_TMPDIR/ssh_socket_dir_removal"
	mkdir -p "$branch_root/zxfer.ssh.full" "$branch_root/zxfer.ssh.busy" \
		"$branch_root/link-target" "$branch_root/operator-dir"
	: >"$branch_root/zxfer.ssh.full/ssh-origin.sock"
	: >"$branch_root/zxfer.ssh.full/ssh-target.sock"
	: >"$branch_root/zxfer.ssh.full/ssh-target.sock.Mvij6x1tYLn6woxm"
	: >"$branch_root/zxfer.ssh.busy/ssh-origin.sock"
	: >"$branch_root/zxfer.ssh.busy/operator-file"
	: >"$branch_root/link-target/ssh-origin.sock"
	: >"$branch_root/operator-dir/ssh-origin.sock"
	ln -s "$branch_root/link-target" "$branch_root/zxfer.ssh.link"

	output=$(
		# No case statement: bash 3.2 as /bin/sh misparses unparenthesized
		# case patterns inside $( ).
		for l_case in none full busy link operator-dir missing; do
			g_zxfer_ssh_control_socket_short_dir="$branch_root/zxfer.ssh.$l_case"
			if [ "$l_case" = none ]; then
				g_zxfer_ssh_control_socket_short_dir=""
			elif [ "$l_case" = missing ]; then
				g_zxfer_ssh_control_socket_short_dir="$branch_root/zxfer.ssh.gone"
			elif [ "$l_case" = operator-dir ]; then
				g_zxfer_ssh_control_socket_short_dir="$branch_root/operator-dir"
			fi
			zxfer_remove_ssh_control_socket_dir
			printf '%s=%s handle=<%s>\n' "$l_case" "$?" \
				"${g_zxfer_ssh_control_socket_short_dir##*/}"
		done
	)

	assertEquals "Removal should succeed for no directory, a socket directory and one already gone, and keep the handle when it fails." \
		"none=0 handle=<>
full=0 handle=<>
busy=1 handle=<zxfer.ssh.busy>
link=1 handle=<zxfer.ssh.link>
operator-dir=1 handle=<operator-dir>
missing=0 handle=<>" "$output"
	assertFalse "The socket directory, sockets and ssh's leftover listener name should be gone." \
		"[ -e '$branch_root/zxfer.ssh.full' ]"
	assertTrue "Removal is not recursive: a file ssh did not create stays." \
		"[ -f '$branch_root/zxfer.ssh.busy/operator-file' ]"
	assertTrue "A symlink in place of the directory must not be followed." \
		"[ -f '$branch_root/link-target/ssh-origin.sock' ]"
	assertTrue "A directory this module did not name must not be touched." \
		"[ -f '$branch_root/operator-dir/ssh-origin.sock' ]"
}

test_open_ssh_control_sockets_fails_closed_without_trusting_the_socket_path() {
	zxfer_test_remote_host_fixture_setup || return
	l_root="$TEST_TMPDIR/open_failures"
	mkdir -p "$l_root/planted" "$l_root/dir"
	chmod 700 "$l_root/planted" "$l_root/dir"
	# Only this run writes the socket directory, so a path already there is
	# not a socket zxfer can trust.
	: >"$l_root/planted/ssh-origin.sock"
	l_log="$TEST_TMPDIR/open_failures_ssh.log"
	: >"$l_log"
	FAKE_SSH_LOG=$l_log
	export FAKE_SSH_LOG
	l_output=$(
		for l_case in planted_path no_socket_dir rejected_start; do
			zxfer_test_open_failure_case "$l_case" "$l_root" 2>&1
		done
	)
	unset FAKE_SSH_LOG

	assertEquals "A planted path, a missing socket directory and a rejected start should throw and forget the socket; a rejected start prints its diagnostic first." \
		"planted_path: throw=Error creating ssh control socket for origin host. socket=<>
no_socket_dir: throw=Error creating temporary directory for ssh control socket. socket=<>
host spec rejected
rejected_start: throw=Error creating ssh control socket for origin host. socket=<>" "$l_output"
	assertEquals "No master may be opened." "" "$(cat "$l_log")"
	assertTrue "The planted path must be left alone." "[ -f '$l_root/planted/ssh-origin.sock' ]"
}

test_ssh_control_socket_actions_reject_bad_input_as_errors() {
	zxfer_test_remote_host_fixture_setup || return
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
	zxfer_run_ssh_control_socket_action open "origin.example" "$TEST_TMPDIR/check.sock"
	policy_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr:$g_zxfer_ssh_control_socket_action_pid"
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	zxfer_run_ssh_control_socket_action exit 'origin.example "doas"' "$TEST_TMPDIR/check.sock"
	host_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr"
	zxfer_run_ssh_control_socket_action check "origin.example" "$TEST_TMPDIR/check.sock"
	check_status=$?
	zxfer_close_ssh_control_socket_for_role invalid
	role_status=$?

	assertEquals "An invalid ssh policy should be an action error with its diagnostic and start no ssh." \
		"1:error:ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path.:" "$policy_result"
	assertEquals "A host spec that needs shell quoting should be an action error with its diagnostic." \
		"1:error:Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$host_result"
	assertEquals "An unknown action, including the retired check, should fail closed." 1 "$check_status"
	assertEquals "Closing an unknown role should fail closed." 1 "$role_status"
}

test_ssh_control_socket_failure_helpers_classify_and_report() {
	zxfer_test_remote_host_fixture_setup || return
	l_socket="$TEST_TMPDIR/check.sock"
	# Each row: close stderr|status of the stale-master check. Only a
	# connect failure on the control socket itself means the master is gone.
	while IFS='|' read -r l_stderr l_expected_status; do
		zxfer_ssh_control_socket_failure_is_stale_master "$l_stderr"
		assertEquals "[$l_stderr] stale-master status" "$l_expected_status" "$?"
	done <<EOF
Control socket connect($l_socket): No such file or directory|0
Control socket connect($l_socket): Connection refused|0
Control socket connect($l_socket): Connection reset by peer|0
Control socket connect($l_socket): Broken pipe|0
Host key verification failed.|1
ssh: connect to host origin.example port 22: Connection refused|1
|1
EOF
	zxfer_reset_ssh_control_socket_action_state
	blank_output=$(zxfer_emit_ssh_control_socket_action_failure_message)
	blank_status=$?
	default_output=$(zxfer_emit_ssh_control_socket_action_failure_message "default action failure.")
	g_zxfer_ssh_control_socket_action_stderr="staged action failure"
	staged_output=$(zxfer_emit_ssh_control_socket_action_failure_message "ignored default")

	assertEquals "Without a staged stderr or a default nothing should print, successfully." \
		"0:" "$blank_status:$blank_output"
	assertEquals "Without a staged stderr the default should print." \
		"default action failure." "$default_output"
	assertEquals "A staged stderr should win over the default." \
		"staged action failure" "$staged_output"
}

test_open_ssh_control_sockets_overlaps_masters_only_in_batch_mode() {
	zxfer_test_remote_host_fixture_setup || return
	fake_ssh="$TEST_TMPDIR/fake_ssh_open_order"
	order_log="$TEST_TMPDIR/open_order.log"
	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
for l_arg in "$@"; do
	l_host=$l_arg
done
printf 'start %s\n' "$l_host" >>"$ORDER_LOG"
sleep 1
printf 'end %s\n' "$l_host" >>"$ORDER_LOG"
EOF
	chmod +x "$fake_ssh"

	# Print the start/end order of both master opens under BatchMode=$1.
	zxfer_test_open_order() {
		: >"$order_log"
		(
			ORDER_LOG=$order_log
			export ORDER_LOG
			ZXFER_SSH_BATCH_MODE=$1
			g_cmd_ssh=$fake_ssh
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_open_ssh_control_sockets
		)
		tr '\n' ' ' <"$order_log"
	}
	order_yes=$(zxfer_test_open_order yes)
	order_no=$(zxfer_test_open_order no)

	assertContains "Under BatchMode=yes the origin handshake should start before either ends." \
		"${order_yes%%end*}" "start origin.example"
	assertContains "Under BatchMode=yes the target handshake should start before either ends." \
		"${order_yes%%end*}" "start target.example"
	assertEquals "Without BatchMode=yes a prompt could interleave, so the masters should open one at a time." \
		"start origin.example end origin.example start target.example end target.example " "$order_no"
}

test_trace_zfs_command_for_role_redacts_stale_last_command_on_render_failure() {
	l_trace_out=$(
		zxfer_reset_session_state
		g_cmd_zfs=/sbin/zfs
		g_cmd_ssh=/usr/bin/ssh
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
		export ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS

		zxfer_record_last_command_string "stale-command-1"
		g_option_T_target_host="   "
		zxfer_trace_zfs_command_for_role "Running command" destination list -H backup/dst 2>/dev/null
		l_after_ws=$g_zxfer_failure_last_command

		zxfer_record_last_command_string "stale-command-2"
		g_option_T_target_host=""
		zxfer_trace_zfs_command_for_role "Running command" invalid-role list -H backup/dst 2>/dev/null
		l_after_bad_role=$g_zxfer_failure_last_command

		printf 'ws=<%s> role=<%s>\n' "$l_after_ws" "$l_after_bad_role"
	)

	assertEquals "Render failures during command tracing must overwrite stale last_command with [redacted]." \
		"ws=<[redacted]> role=<[redacted]>" "$l_trace_out"
}

test_run_zfs_cmd_capturing_stderr_routes_very_verbose_trace_to_caller_stderr() {
	zxfer_test_exec_fixture_setup
	l_fake_ssh="$TEST_TMPDIR/capture_stderr_ssh"
	cat >"$l_fake_ssh" <<'EOF'
#!/bin/sh
[ -z "${FAKE_CAP_STDOUT:-}" ] || printf '%s\n' "$FAKE_CAP_STDOUT"
[ -z "${FAKE_CAP_STDERR:-}" ] || printf '%s\n' "$FAKE_CAP_STDERR" >&2
if [ "${FAKE_CAP_CHECK_FD6:-0}" -eq 1 ]; then
	if { printf 'fd6-leaked\n' >&6; } 2>/dev/null; then
		printf 'fd6=leaked\n'
	else
		printf 'fd6=closed\n'
	fi
fi
exit "${FAKE_CAP_STATUS:-0}"
EOF
	chmod +x "$l_fake_ssh"

	l_merge_trace="$TEST_TMPDIR/capture_merge.trace"
	l_stderr_only_trace="$TEST_TMPDIR/capture_stderr_only.trace"
	l_file_trace="$TEST_TMPDIR/capture_file.trace"
	l_file_out="$TEST_TMPDIR/capture_file.out"
	l_file_err="$TEST_TMPDIR/capture_file.err"
	l_quiet_err="$TEST_TMPDIR/capture_quiet.err"
	l_nested_trace="$TEST_TMPDIR/capture_nested.trace"
	l_badpath_trace="$TEST_TMPDIR/capture_badpath.trace"
	l_out=$(
		g_cmd_ssh=$l_fake_ssh
		g_target_cmd_zfs=/target/sbin/zfs
		g_option_T_target_host=target.example
		g_option_V_very_verbose=1
		zxfer_refresh_remote_zfs_commands

		FAKE_CAP_STDOUT="remote-stdout"
		FAKE_CAP_STDERR="remote-stderr"
		FAKE_CAP_STATUS=37
		FAKE_CAP_CHECK_FD6=1
		export FAKE_CAP_STDOUT FAKE_CAP_STDERR FAKE_CAP_STATUS FAKE_CAP_CHECK_FD6
		g_zxfer_profile_command_render_calls=0
		l_merge_status=0
		l_merge_val=$(zxfer_run_zfs_cmd_capturing_stderr destination merge list -H backup/dst 2>"$l_merge_trace") ||
			l_merge_status=$?
		printf 'merge=%s|<%s>|fd6_flag=%s\n' "$l_merge_status" "$l_merge_val" "${g_zxfer_ssh_echoV_to_fd6:-0}"

		# Run in the current shell to verify single-pass command rendering (no double render).
		FAKE_CAP_STDOUT="clean-stdout"
		FAKE_CAP_STDERR=""
		FAKE_CAP_STATUS=0
		FAKE_CAP_CHECK_FD6=0
		g_zxfer_profile_command_render_calls=0
		zxfer_run_zfs_cmd_capturing_stderr destination "$l_file_err" list -H backup/dst >"$l_file_out" 2>"$l_file_trace"
		printf 'file_clean=%s|out=<%s>|err_bytes=%s|renders=%s|fd6_flag=%s\n' "$?" \
			"$(cat "$l_file_out")" "$(wc -c <"$l_file_err" | tr -d ' ')" \
			"$g_zxfer_profile_command_render_calls" "${g_zxfer_ssh_echoV_to_fd6:-0}"

		FAKE_CAP_STDOUT="remote-stdout"
		FAKE_CAP_STDERR="remote-stderr"
		FAKE_CAP_STATUS=37
		l_only_status=0
		l_only_val=$(zxfer_run_zfs_cmd_capturing_stderr destination stderr-only list -H backup/dst 2>"$l_stderr_only_trace") ||
			l_only_status=$?
		printf 'only=%s|<%s>\n' "$l_only_status" "$l_only_val"

		# Bad-path redirection failure must still restore g_zxfer_ssh_echoV_to_fd6 to 0.
		l_badpath_status=0
		zxfer_run_zfs_cmd_capturing_stderr destination "$TEST_TMPDIR/no-such-dir/err" list -H backup/dst 2>"$l_badpath_trace" ||
			l_badpath_status=$?
		printf 'badpath_nonzero=%s|fd6_flag=%s\n' \
			"$([ "$l_badpath_status" -ne 0 ] && printf yes || printf no)" "${g_zxfer_ssh_echoV_to_fd6:-0}"

		# Nested captures keep the outermost caller stderr on FD 6 and do not
		# leak traces or clobber outer return status.
		l_nested_out=$(
			(
				zxfer_run_destination_zfs_cmd() {
					if [ "$1" = outer ]; then
						l_inner_st=0
						l_inner_out=$(zxfer_run_zfs_cmd_capturing_stderr destination merge list -H backup/inner) ||
							l_inner_st=$?
						printf 'inner(%s:%s)\n' "$l_inner_st" "$l_inner_out"
						zxfer_run_zfs_cmd_for_role destination list -H backup/outer
						return 0
					fi
					zxfer_run_zfs_cmd_for_role destination "$@"
				}
				FAKE_CAP_STDOUT="ok"
				FAKE_CAP_STDERR="err"
				FAKE_CAP_STATUS=19
				zxfer_run_zfs_cmd_capturing_stderr destination merge outer
				printf 'outer_st=%s|outer_fd6=%s\n' "$?" "${g_zxfer_ssh_echoV_to_fd6:-0}"
			) 2>"$l_nested_trace"
		)
		printf 'nested=<%s>|nested_traces=%s\n' "$l_nested_out" \
			"$(grep -c -F "Running remote command [target: target.example]: '$l_fake_ssh'" "$l_nested_trace")"

		# Local and non-verbose captures must not touch FD 6 or set g_zxfer_ssh_echoV_to_fd6.
		l_fd6_caller_file="$TEST_TMPDIR/capture_caller_fd6.out"
		: >"$l_fd6_caller_file"
		{
			l_local_zfs="$TEST_TMPDIR/capture_local_zfs"
			printf '#!/bin/sh\nprintf "local-out\\n"\nprintf "caller-fd6-ok\\n" >&6\n' >"$l_local_zfs"
			chmod +x "$l_local_zfs"
			g_cmd_zfs=$l_local_zfs
			l_local_val=$(zxfer_run_zfs_cmd_capturing_stderr local merge list -H tank/local)
			g_option_V_very_verbose=0
			FAKE_CAP_STDOUT="quiet-out"
			FAKE_CAP_STDERR=""
			FAKE_CAP_STATUS=0
			FAKE_CAP_CHECK_FD6=0
			l_nonv_val=$(zxfer_run_zfs_cmd_capturing_stderr destination merge list -H backup/dst)
			g_option_V_very_verbose=1
		} 6>"$l_fd6_caller_file"
		printf 'local_fd6=%s|nonv=%s|caller_fd6=<%s>\n' \
			"$l_local_val" "$l_nonv_val" "$(cat "$l_fd6_caller_file")"

		# Retired echoV mode must fail closed; reset clears g_zxfer_ssh_echoV_to_fd6.
		zxfer_zfs_command_for_role echoV destination list -H backup/dst 2>/dev/null
		l_retired_status=$?
		g_zxfer_ssh_echoV_to_fd6=1
		zxfer_reset_ssh_transport_state
		printf 'retired=%s|after_reset=%s\n' "$l_retired_status" "$g_zxfer_ssh_echoV_to_fd6"

		g_cmd_ssh=$l_fake_ssh
		g_option_T_target_host=target.example
		zxfer_refresh_remote_zfs_commands
		FAKE_CAP_STDOUT=""
		FAKE_CAP_STDERR=""
		FAKE_CAP_STATUS=0
		zxfer_invoke_ssh_shell_command_for_host target.example "'true'" destination quiet >/dev/null 2>"$l_quiet_err"
		printf 'quiet_status=%s|quiet_bytes=%s\n' "$?" "$(wc -c <"$l_quiet_err" | tr -d ' ')"

		zxfer_run_zfs_cmd_capturing_stderr destination "" list -H backup/dst
		l_empty_status=$?
		l_bad_role=$(zxfer_run_zfs_cmd_capturing_stderr unknown merge list -H backup/dst 2>&1)
		printf 'empty=%s|bad=%s:%s\n' "$l_empty_status" "$?" "$l_bad_role"
	)

	assertEquals "Captured stderr modes must route -V traces via FD 6 without double rendering, close FD 6 for ssh, handle nesting and bad paths cleanly, and keep run_quiet silent." \
		"merge=37|<remote-stdout
remote-stderr
fd6=closed>|fd6_flag=0
file_clean=0|out=<clean-stdout>|err_bytes=0|renders=1|fd6_flag=0
only=37|<remote-stderr>
badpath_nonzero=yes|fd6_flag=0
nested=<inner(19:ok
err)
ok
err
outer_st=0|outer_fd6=0>|nested_traces=2
local_fd6=local-out|nonv=quiet-out|caller_fd6=<caller-fd6-ok>
retired=1|after_reset=0
quiet_status=0|quiet_bytes=0
empty=1|bad=1:zxfer: unknown zfs command role [unknown]." "$l_out"
	for l_trace_file in "$l_merge_trace" "$l_stderr_only_trace" "$l_file_trace"; do
		assertEquals "Each captured mode must emit exactly one -V remote command line on caller stderr ($l_trace_file)." \
			1 "$(grep -c -F "Running remote command [target: target.example]: '$l_fake_ssh'" "$l_trace_file")"
	done
	assertNotContains "FD 6 must be closed before executing ssh so ssh cannot write to caller stderr through it." \
		"$(cat "$l_merge_trace")" "fd6-leaked"
}

test_ssh_control_socket_action_command_renders_only_for_very_verbose_exit() {
	zxfer_test_remote_host_fixture_setup || return
	l_stub_ssh="$TEST_TMPDIR/cmd_gate_ssh"
	printf '#!/bin/sh\nexit 0\n' >"$l_stub_ssh"
	chmod +x "$l_stub_ssh"
	l_out=$(
		g_cmd_ssh=$l_stub_ssh
		g_option_V_very_verbose=1
		zxfer_run_ssh_control_socket_action open origin.example "$TEST_TMPDIR/cmd_gate.sock" 2>/dev/null
		wait "$g_zxfer_ssh_control_socket_action_pid" 2>/dev/null || :
		printf 'open_V=<%s>\n' "$g_zxfer_ssh_control_socket_action_command"

		g_option_V_very_verbose=0
		zxfer_run_ssh_control_socket_action exit origin.example "$TEST_TMPDIR/cmd_gate.sock" 2>/dev/null
		printf 'exit_quiet=<%s>\n' "$g_zxfer_ssh_control_socket_action_command"

		g_option_V_very_verbose=1
		zxfer_run_ssh_control_socket_action exit origin.example "$TEST_TMPDIR/cmd_gate.sock" 2>/dev/null
		printf 'exit_V=<%s>\n' "$g_zxfer_ssh_control_socket_action_command"
	)

	assertEquals "g_zxfer_ssh_control_socket_action_command must be rendered only for exit under -V." \
		"open_V=<>
exit_quiet=<>
exit_V=<'$l_stub_ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/cmd_gate.sock' '-O' 'exit' 'origin.example'>" "$l_out"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
