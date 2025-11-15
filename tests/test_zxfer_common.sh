#!/bin/sh
#
# Basic shunit2 tests for zxfer_common.sh helpers.
#

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_shunit.XXXXXX)
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	g_option_n_dryrun=0
	if [ -n "${TEST_TMPDIR:-}" ]; then
		rm -f "$TEST_TMPDIR"/*
	fi
}

test_escape_for_double_quotes_escapes_special_chars() {
	input="text\"with\\\`special\$chars\\and normal"
	expected="text\\\"with\\\`special\\\$chars\\\\and normal"

	result=$(escape_for_double_quotes "$input")

	assertEquals "Input should be properly escaped for double quotes." "$expected" "$result"
}

test_execute_command_respects_dry_run_mode() {
	temp_file="$TEST_TMPDIR/dry_run_output"
	g_option_n_dryrun=1

	execute_command "printf 'should not run' > '$temp_file'"

	assertFalse "Dry run should skip running the command." "[ -f \"$temp_file\" ]"
}

test_execute_command_runs_command_when_not_dry_run() {
	temp_file="$TEST_TMPDIR/run_output"

	execute_command "printf 'ran' > '$temp_file'"

	assertTrue "Command should run when dry run is disabled." "[ -f \"$temp_file\" ]"
	assertEquals "ran" "$(cat "$temp_file")"
}

test_get_temp_file_creates_unique_file() {
	file_one=$(get_temp_file)
	file_two=$(get_temp_file)

	assertTrue "First temp file should exist." "[ -f \"$file_one\" ]"
	assertTrue "Second temp file should exist." "[ -f \"$file_two\" ]"
	assertNotEquals "Two consecutive temp file names should be unique." "$file_one" "$file_two"

	rm -f "$file_one" "$file_two"
}

. "$SHUNIT2_BIN"
