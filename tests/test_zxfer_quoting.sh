#!/bin/sh
#
# shunit2 tests for the fork-free primitives in src/zxfer_quoting.sh.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_quoting.sh"

QUOTING_TEST_REJECTION_SUFFIX="must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_quoting"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	g_option_V_very_verbose=0
	g_zxfer_profile_command_render_calls=0
}

# Reference copy of the renderer that zxfer_render_shell_command_from_argv
# replaced. Its bytes are the compatibility contract for rendered commands.
quoting_test_reference_escape() {
	case $1 in
	*\'*) ;;
	*)
		printf '%s' "$1"
		return 0
		;;
	esac

	l_ref_escape_rest=$1
	l_ref_escape_out=""
	while :; do
		case $l_ref_escape_rest in
		*\'*)
			l_ref_escape_out="$l_ref_escape_out${l_ref_escape_rest%%\'*}'\\''"
			l_ref_escape_rest=${l_ref_escape_rest#*\'}
			;;
		*)
			l_ref_escape_out="$l_ref_escape_out$l_ref_escape_rest"
			break
			;;
		esac
	done
	printf '%s' "$l_ref_escape_out"
}

quoting_test_reference_build() {
	l_ref_separator=""
	for l_ref_arg in "$@"; do
		printf "%s'" "$l_ref_separator"
		quoting_test_reference_escape "$l_ref_arg"
		printf "'"
		l_ref_separator=" "
	done
}

# Compare the old and new renderers byte for byte on one argument list,
# through files so trailing newlines survive.
quoting_test_assert_render_matches_reference() {
	quoting_test_reference_build "$@" >"$TEST_TMPDIR/render.ref"
	zxfer_render_shell_command_from_argv "$@" >"$TEST_TMPDIR/render.stdout"
	printf '%s' "$g_zxfer_shell_command_result" >"$TEST_TMPDIR/render.global"

	assertTrue "g_zxfer_shell_command_result should match the reference bytes for [$*]." \
		"cmp -s '$TEST_TMPDIR/render.ref' '$TEST_TMPDIR/render.global'"
	assertEquals "zxfer_render_shell_command_from_argv should print nothing." \
		0 "$(wc -c <"$TEST_TMPDIR/render.stdout" | tr -d ' ')"
}

test_line_control_constants_hold_single_bytes() {
	assertEquals "ZXFER_TAB, ZXFER_CR and ZXFER_LF should hold one tab, CR and LF byte." \
		"090d0a" "$(printf '%s%s%s' "$ZXFER_TAB" "$ZXFER_CR" "$ZXFER_LF" | od -An -tx1 | tr -d ' \n')"
}

test_value_is_single_line_rejects_line_control_bytes_silently() {
	for l_value in "" "plain" "two words" "x$(printf '\303\251')y" "a;b|c"; do
		zxfer_value_is_single_line "$l_value" >"$TEST_TMPDIR/single.out" 2>&1
		assertEquals "A value without tab, CR or LF should be single-line: [$l_value]." \
			0 "$?"
		assertEquals "The single-line predicate should print nothing." \
			"" "$(cat "$TEST_TMPDIR/single.out")"
	done
	for l_value in "a${ZXFER_TAB}b" "a${ZXFER_CR}b" "a${ZXFER_LF}b" "tail$ZXFER_LF" \
		"$ZXFER_CR" "${ZXFER_TAB}lead"; do
		zxfer_value_is_single_line "$l_value" >"$TEST_TMPDIR/single.out" 2>&1
		assertEquals "A value with a line-control byte should be rejected." \
			1 "$?"
		assertEquals "A rejected value should leave the error text to the caller." \
			"" "$(cat "$TEST_TMPDIR/single.out")"
	done
}

test_is_uint_accepts_only_nonempty_digit_strings() {
	for l_value in 0 7 42 007 18446744073709551616; do
		assertTrue "[$l_value] should be an unsigned integer." \
			"zxfer_is_uint '$l_value'"
	done
	for l_value in "" "-1" "+1" "1.5" " 1" "1 " "a" "1a" "0x1" "1$ZXFER_LF" "*"; do
		assertFalse "[$l_value] should not be an unsigned integer." \
			"zxfer_is_uint '$l_value'"
	done
}

test_split_begin_and_end_restore_unset_ifs_and_enabled_globbing() {
	(
		unset IFS
		set +f
		zxfer_split_begin
		[ "$IFS" = " $ZXFER_TAB$ZXFER_LF" ] && echo "inside_ifs=default"
		case $- in *f*) echo "inside_noglob=on" ;; esac
		zxfer_split_end
		[ "${IFS+set}" = set ] || echo "after_ifs=unset"
		case $- in *f*) ;; *) echo "after_noglob=off" ;; esac
	) >"$TEST_TMPDIR/split_state.out" 2>&1

	assertEquals "split_begin should set default IFS and noglob; split_end should restore unset IFS and globbing." \
		"inside_ifs=default
inside_noglob=on
after_ifs=unset
after_noglob=off" "$(cat "$TEST_TMPDIR/split_state.out")"
}

test_split_begin_and_end_restore_set_ifs_and_disabled_globbing() {
	(
		IFS=":"
		set -f
		zxfer_split_begin ";"
		[ "$IFS" = ";" ] && echo "inside_ifs=semicolon"
		zxfer_split_end
		printf 'after_ifs=<%s>\n' "$IFS"
		case $- in *f*) echo "after_noglob=on" ;; esac

		IFS=""
		zxfer_split_begin ""
		[ "${IFS+set}" = set ] && [ -z "$IFS" ] && echo "inside_ifs=empty"
		zxfer_split_end
		[ "${IFS+set}" = set ] && [ -z "$IFS" ] && echo "after_ifs=empty"
	) >"$TEST_TMPDIR/split_state.out" 2>&1

	assertEquals "split_begin/end should restore a set IFS, an empty IFS, and enabled noglob." \
		"inside_ifs=semicolon
after_ifs=<:>
after_noglob=on
inside_ifs=empty
after_ifs=empty" "$(cat "$TEST_TMPDIR/split_state.out")"
}

test_split_tokens_into_result_publishes_tokens_without_output() {
	(
		IFS=":"
		set +f
		cd "$TEST_TMPDIR" || exit 1
		: >"glob-match"
		zxfer_split_tokens_into_result "  zstd   -3${ZXFER_TAB}*  ${ZXFER_LF}"
		printf 'result=<%s>\n' "$g_zxfer_split_tokens_result"
		printf 'after_ifs=<%s>\n' "$IFS"
		case $- in *f*) ;; *) echo "after_noglob=off" ;; esac
		zxfer_split_tokens_into_result "cmd;rm -rf|grep foo&echo done"
		printf 'meta=<%s>\n' "$g_zxfer_split_tokens_result"
		zxfer_split_tokens_into_result " $ZXFER_TAB$ZXFER_LF "
		printf 'blank=<%s>\n' "$g_zxfer_split_tokens_result"
		# ksh93 once matched a backslash in [;\|\&]; pin the portable split.
		zxfer_split_tokens_into_result 'a\;b a;\*'
		printf 'backslash=<%s>\n' "$g_zxfer_split_tokens_result"
	) >"$TEST_TMPDIR/split_result.out" 2>&1

	assertEquals "split_tokens_into_result should publish literal newline-joined tokens and restore shell state." \
		"result=<zstd
-3
*>
after_ifs=<:>
after_noglob=off
meta=<cmd;
rm
-rf|
grep
foo&
echo
done>
blank=<>
backslash=<a\\;
b
a;
\\*>" "$(cat "$TEST_TMPDIR/split_result.out")"
}

test_render_shell_command_matches_reference_bytes() {
	l_lf=$ZXFER_LF
	set -- "" " " "plain" "two words" "a${l_lf}b" "x$l_lf" "x$l_lf$l_lf" "$l_lf" \
		"'" "''" "a'b'c" "trailing'" "'leading" "it's a 'test'" \
		'back\slash' "trail\\" "\\" '$HOME' '$(id)' '`id`' '%s' '%%' '-n' '-e' '--' \
		"tab${ZXFER_TAB}here" "cr${ZXFER_CR}here" "*" "a?[b]" "a;b|c&d" \
		"$(printf 'caf\303\251 \346\227\245')" "$(printf 'bad\377byte')" \
		"$(printf '\303\251')'x" "\"double\""

	for l_corpus_arg in "$@"; do
		quoting_test_assert_render_matches_reference "$l_corpus_arg"
	done
	quoting_test_assert_render_matches_reference "$@"
	quoting_test_assert_render_matches_reference
	quoting_test_assert_render_matches_reference "" ""
}

test_render_shell_command_counts_one_render_for_the_profile() {
	g_option_V_very_verbose=1
	g_zxfer_profile_command_render_calls=0

	zxfer_render_shell_command_from_argv "zfs" "list"
	zxfer_render_shell_command_from_argv "zfs"

	assertEquals "Each render should count exactly one render in the current shell." \
		2 "$g_zxfer_profile_command_render_calls"

	g_option_V_very_verbose=0
	zxfer_render_shell_command_from_argv "zfs"
	assertEquals "Renders should not be counted without -V." \
		2 "$g_zxfer_profile_command_render_calls"
}

test_escape_single_quotes_into_result_escapes_apostrophes() {
	# Single-quoted contexts require reopening the quotes around apostrophes,
	# so ensure the helper inserts the standard '\''' sequence.
	zxfer_escape_single_quotes_into_result "needs'single'quotes"

	assertEquals "Input should be properly escaped for single quotes." \
		"needs'\\''single'\\''quotes" "$g_zxfer_escaped_single_quotes_result"
}

test_shell_command_render_preserves_exact_argument_bytes() {
	value="a'b

"
	# shellcheck disable=SC2016 # Deliberate shell syntax must stay literal.
	literal='$(exit 19); * \ end'
	zxfer_render_shell_command_from_argv printf '<%s>\n' "" "$value" "$literal"
	output=$(sh -c "$g_zxfer_shell_command_result")

	assertEquals "Quoting must preserve empty values, apostrophes, trailing newlines, and literal shell syntax." \
		"<>
<$value>
<$literal>" "$output"
}

test_check_literal_token_string_publishes_only_the_rejection_message() {
	zxfer_check_literal_token_string "zstd -3" "compression command" >"$TEST_TMPDIR/check.out" 2>&1
	assertEquals "A literal token string should pass the check." 0 "$?"
	assertEquals "An accepted token string should publish no message." \
		"" "$g_zxfer_literal_token_error_result"

	for l_value in 'zstd -T0\ -3' 'a "b"' "a 'b'"; do
		zxfer_check_literal_token_string "$l_value" "compression command" >"$TEST_TMPDIR/check.out" 2>&1
		assertEquals "Quotes and backslashes should fail the check: [$l_value]." 1 "$?"
		assertEquals "The check should publish the rejection message." \
			"compression command $QUOTING_TEST_REJECTION_SUFFIX" "$g_zxfer_literal_token_error_result"
		assertFalse "The check should print nothing." "[ -s '$TEST_TMPDIR/check.out' ]"
	done

	zxfer_check_literal_token_string 'a\b'
	assertEquals "The check should default its label to command." \
		"command $QUOTING_TEST_REJECTION_SUFFIX" "$g_zxfer_literal_token_error_result"
}

test_quote_cli_tokens_renders_nothing_for_blank_or_tokenless_input() {
	g_option_V_very_verbose=1
	g_zxfer_profile_command_render_calls=0

	for l_value in "" "   " " $ZXFER_TAB$ZXFER_LF "; do
		g_zxfer_shell_command_result=stale
		zxfer_quote_cli_tokens "$l_value" >"$TEST_TMPDIR/quote_cli.out"
		assertEquals "Blank or tokenless CLI strings should succeed." 0 "$?"
		assertEquals "Blank or tokenless CLI strings should not render placeholder quotes." \
			"" "$g_zxfer_shell_command_result"
	done
	assertEquals "Blank or tokenless CLI strings should not count a render." \
		0 "$g_zxfer_profile_command_render_calls"

	zxfer_quote_cli_tokens "zstd -3;x" >"$TEST_TMPDIR/quote_cli.out"
	assertEquals "A literal CLI string should render its quoted tokens." \
		"'zstd' '-3;' 'x'" "$g_zxfer_shell_command_result"
	assertEquals "A rendered CLI string should count one render." \
		1 "$g_zxfer_profile_command_render_calls"
	assertFalse "quote_cli_tokens should print nothing." "[ -s '$TEST_TMPDIR/quote_cli.out' ]"

	zxfer_quote_cli_tokens "a 'b'"
	assertEquals "quote_cli_tokens should fail on shell quotes." 1 "$?"
	assertEquals "quote_cli_tokens should default its label to CLI command." \
		"CLI command $QUOTING_TEST_REJECTION_SUFFIX" "$g_zxfer_literal_token_error_result"
}

test_quote_cli_tokens_preserves_argument_boundaries() {
	# Compression commands should behave like arrays, preserving each argument.
	zxfer_quote_cli_tokens "zstd -3 --long=27"

	assertEquals "CLI tokens should be individually quoted." \
		"'zstd' '-3' '--long=27'" "$g_zxfer_shell_command_result"
}

test_quote_cli_tokens_blocks_shell_metacharacters() {
	# Metacharacters such as ';' or '|' must be neutralized instead of being
	# interpreted as new commands or pipelines.
	zxfer_quote_cli_tokens "zstd -3; touch /tmp/pwn | cat"

	assertEquals "CLI tokens should remain literal even with metacharacters." \
		"'zstd' '-3;' 'touch' '/tmp/pwn' '|' 'cat'" "$g_zxfer_shell_command_result"
}

test_quote_cli_tokens_preserves_validation_failures() {
	zxfer_quote_cli_tokens '"/opt/zstd dir/zstd" -3' "compression command"
	status=$?

	assertEquals "CLI quoting should fail closed when token validation rejects the input." \
		1 "$status"
	assertEquals "CLI quoting should publish the literal-token validation message." \
		"compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$g_zxfer_literal_token_error_result"
}

test_strip_trailing_slashes_publishes_the_result() {
	for l_case in "pool/dst///:pool/dst" "a//b/:a//b" "pool:pool" "///:///" "/:/" ":"; do
		l_input=${l_case%%:*}
		l_expected=${l_case#*:}
		zxfer_strip_trailing_slashes "$l_input" >"$TEST_TMPDIR/strip.out"
		assertEquals "strip_trailing_slashes should publish [$l_expected] for [$l_input]." \
			"$l_expected" "$g_zxfer_stripped_path_result"
		assertFalse "strip_trailing_slashes should print nothing for [$l_input]." \
			"[ -s '$TEST_TMPDIR/strip.out' ]"
	done
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
