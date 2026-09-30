#!/bin/sh
# Operator-output tests for src/zxfer_reporting.sh: the -v/-V printers, beep,
# report command rendering and failure-report defaults. Run by
# tests/test_zxfer_reporting.sh; the launcher's usage and dependency failure
# transcripts are pinned by tests/test_contract_cli_golden.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# echo in dash and macOS /bin/sh expands \033 and \n, stops at \c and reads a
# lone -n as an option; the printers must print the text as written, each only
# at its own level and on its own stream.
test_verbose_printers_print_text_as_written_only_at_their_level() {
	l_text='lit=\033[31m \c cut \n \\ \x1B end'
	l_value=$(printf 'ok\033[2J\033]0;x\007 lit=\\033 \\c cut\r\nforged: line')

	g_option_v_verbose=0
	g_option_V_very_verbose=0
	assertEquals "zxfer_echov stays quiet without -v." "" "$(zxfer_echov "hidden message")"
	assertEquals "zxfer_echoV stays quiet without -V." "" "$(zxfer_echoV "hidden debug" 2>&1)"
	assertEquals "zxfer_echoV_escaped stays quiet without -V." \
		"" "$(zxfer_echoV_escaped label "$l_value" 2>&1)"

	g_option_v_verbose=1
	g_option_V_very_verbose=1
	assertEquals "zxfer_echov prints backslashes as written." "$l_text
." "$(
		zxfer_echov "$l_text"
		printf '.'
	)"
	assertEquals "A lone -n is text, not an echo option." "-n" "$(zxfer_echov -n)"
	assertEquals "zxfer_echoV prints backslashes as written, to stderr only." "$l_text
." "$(
		{
			zxfer_echoV "$l_text"
			printf '.' >&2
		} 2>&1 >/dev/null
	)"
	assertEquals "zxfer_echoV_escaped prints control bytes and backslashes escaped on one line." \
		'label: ok\x1B[2J\x1B]0;x\x07 lit=\\033 \\c cut\r\nforged: line' \
		"$(zxfer_echoV_escaped label "$l_value" 2>&1)"
	assertEquals "An empty value keeps the label and separator." \
		"label: " "$(zxfer_echoV_escaped label "" 2>&1)"
	assertEquals "zxfer_echoV_escaped writes to stderr only." \
		"" "$(zxfer_echoV_escaped label "$l_value" 2>/dev/null)"
}

# Only FreeBSD with the speaker tools and /dev/speaker beeps; any other host
# skips with a -V note, so replication continues.
test_beep_skips_with_a_V_note_where_the_speaker_is_unavailable() {
	fake_bin_dir="$TEST_TMPDIR/no_speaker_tools"
	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/uname" <<'UNAME'
#!/bin/sh
printf '%s\n' "FreeBSD"
UNAME
	chmod +x "$fake_bin_dir/uname"

	output=$(
		(
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			(
				uname() {
					printf '%s\n' "Linux"
				}
				zxfer_beep 1
			)
			(
				PATH="$fake_bin_dir"
				zxfer_beep 1
			)
			(
				uname() {
					printf '%s\n' "FreeBSD"
				}
				kldstat() {
					printf '%s\n' "speaker.ko"
				}
				kldload() {
					return 0
				}
				zxfer_beep 1
			)
		) 2>&1
	)

	assertEquals "A non-FreeBSD host, missing speaker tools and a missing /dev/speaker should each skip with a -V note." \
		"Beep requested but unsupported on Linux; skipping.
Beep requested but speaker tools are missing; skipping.
Beep requested but /dev/speaker missing; skipping." "$output"
}

test_zxfer_report_command_rendering_quotes_each_argument_on_one_line() {
	l_newline_arg=$(printf 'line1\nline2')
	result=$(zxfer_quote_command_argv "./zxfer" "value with space" "$l_newline_arg" "apost'rophe")

	assertEquals "Quoted argv should remain one-line and shell-safe for reports." \
		"'./zxfer' 'value with space' 'line1\\nline2' 'apost'\"'\"'rophe'" "$result"
	assertEquals "Report rendering should keep a shell-ready prefix and quote the argv after it." \
		"/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4'" \
		"$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs" "create" "-o" "compression=lz4")"
	assertEquals "Report rendering should return a prefix without argv unchanged." \
		"/usr/bin/ssh 'host' /sbin/zfs" \
		"$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs")"
	assertEquals "Report rendering should quote the argv when the prefix is empty." \
		"'zfs' 'list' 'tank/src'" \
		"$(zxfer_render_command_for_report "" "zfs" "list" "tank/src")"
}

# The exit status picks the class and message when none was recorded: 2 is a
# usage failure, anything else a runtime one.
test_zxfer_render_failure_report_defaults_class_message_and_stage() {
	g_zxfer_failure_class=""
	g_zxfer_failure_message=""
	g_zxfer_failure_stage=""
	runtime_report=$(zxfer_render_failure_report 1)
	g_option_R_recursive=""
	g_option_N_nonrecursive="tank/src"
	usage_report=$(zxfer_render_failure_report 2)

	assertContains "Non-usage exits should default to runtime failures." \
		"$runtime_report" "failure_class: runtime"
	assertContains "Missing failure messages should fall back to the exit status summary." \
		"$runtime_report" "message: zxfer exited with status 1."
	assertContains "A missing stage should fall back to startup." \
		"$runtime_report" "failure_stage: startup"
	assertContains "Failure reports should default exit status 2 to usage errors." \
		"$usage_report" "failure_class: usage"
	assertContains "Failure reports should default missing messages to the exit-status text." \
		"$usage_report" "message: zxfer exited with status 2."
	assertContains "Failure reports should identify nonrecursive mode when -N is set." \
		"$usage_report" "mode: nonrecursive"
}
