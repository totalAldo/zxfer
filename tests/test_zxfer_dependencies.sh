#!/bin/sh
#
# shunit2 tests for zxfer_dependencies.sh helpers.
#
# shellcheck disable=SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_dependencies.sh"

setUp() {
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	unset g_cmd_awk
	g_zxfer_secure_path=""
	g_zxfer_dependency_path=""
	g_zxfer_runtime_path=""
}

test_zxfer_compute_secure_path_defaults_to_allowlist() {
	assertEquals "The default secure PATH should use the built-in allowlist." \
		"/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin" \
		"$(zxfer_compute_secure_path)"
}

test_merge_path_allowlists_deduplicates_entries() {
	assertEquals "Merged allowlists should preserve order while deduplicating entries." \
		"/sbin:/bin:/usr/bin" \
		"$(zxfer_merge_path_allowlists "/sbin:/bin" "/bin:/usr/bin")"
}

test_zxfer_validate_resolved_tool_path_rejects_relative_path() {
	zxfer_test_capture_subshell '
		zxfer_validate_resolved_tool_path "awk" "awk"
	'

	assertEquals "Relative tool paths should be rejected." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative path rejection should require an absolute path." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "requires an absolute path"
}

test_zxfer_validate_resolved_tool_path_accepts_shell_quoted_absolute_path() {
	quoted_path="'/tmp/mocktool.\$(touch marker)'"

	result=$(zxfer_validate_resolved_tool_path "$quoted_path" "mocktool")
	status=$?

	assertEquals "Shell-quoted absolute paths from command -v should remain valid after normalization." 0 "$status"
	assertEquals "Shell-quoted absolute paths from command -v should be normalized before validation." \
		"/tmp/mocktool.\$(touch marker)" "$result"
}

test_zxfer_validate_resolved_tool_path_accepts_double_quoted_absolute_path() {
	quoted_path="\"/tmp/mocktool.\$(touch marker)\""

	result=$(zxfer_validate_resolved_tool_path "$quoted_path" "mocktool")
	status=$?

	assertEquals "Double-quoted absolute paths from command -v should remain valid after normalization." 0 "$status"
	assertEquals "Double-quoted absolute paths from command -v should be normalized before validation." \
		"/tmp/mocktool.\$(touch marker)" "$result"
}

test_zxfer_initialize_dependency_defaults_sets_runtime_path_and_awk() {
	unset g_cmd_awk
	zxfer_initialize_dependency_defaults

	assertEquals "Dependency bootstrap should set the secure path." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$g_zxfer_secure_path"
	assertNotEquals "Dependency bootstrap should resolve an awk helper." \
		"" "$g_cmd_awk"
}

test_zxfer_initialize_dependency_defaults_falls_back_to_plain_awk_when_secure_path_has_no_awk() {
	unset g_cmd_awk
	ZXFER_SECURE_PATH="$TEST_TMPDIR/no-awk-here"

	zxfer_initialize_dependency_defaults

	assertEquals "Dependency bootstrap should fall back to plain awk when secure-path lookup finds no absolute awk binary." \
		"awk" "$g_cmd_awk"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
