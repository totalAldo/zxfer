#!/bin/sh
# Tests for src/zxfer_quoting.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
