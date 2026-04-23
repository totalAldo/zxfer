#!/bin/sh
#
# shunit2 tests for zxfer_dependencies.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_dependencies.sh"

setUp() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	unset g_cmd_awk
	g_zxfer_secure_path=""
	g_zxfer_dependency_path=""
	g_zxfer_runtime_path=""
}

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

test_zxfer_compute_secure_path_defaults_to_allowlist() {
	assertEquals "The default secure PATH should use the built-in allowlist." \
		"/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin" \
		"$(zxfer_compute_secure_path)"
}

test_zxfer_get_effective_dependency_path_refreshes_from_environment() {
	result=$(
		(
			g_zxfer_secure_path="/stale/secure/path"
			g_zxfer_dependency_path="/stale/dependency/path"
			ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
			ZXFER_SECURE_PATH_APPEND="/custom/bin"
			zxfer_get_effective_dependency_path
		)
	)

	assertEquals "Effective dependency-path lookups should recompute from ZXFER_SECURE_PATH when the environment overrides the secure path." \
		"/fresh/secure/path:/usr/bin:/custom/bin" "$result"
}

test_zxfer_get_effective_dependency_path_prefers_cached_dependency_path_without_env_override() {
	g_zxfer_secure_path="/cached/secure/path"
	g_zxfer_dependency_path="/cached/dependency/path"

	assertEquals "Effective dependency-path lookups should prefer the cached dependency path when no environment override is active." \
		"/cached/dependency/path" "$(zxfer_get_effective_dependency_path)"
}

test_zxfer_get_effective_dependency_path_falls_back_to_cached_secure_path_and_default() {
	result=$(
		(
			g_zxfer_dependency_path=""
			g_zxfer_secure_path="/cached/secure/path"
			printf 'cached=%s\n' "$(zxfer_get_effective_dependency_path)"
			g_zxfer_secure_path=""
			printf 'default=%s\n' "$(zxfer_get_effective_dependency_path)"
		)
	)

	assertContains "Effective dependency-path lookups should fall back to the cached secure path when the dependency path cache is empty." \
		"$result" "cached=/cached/secure/path"
	assertContains "Effective dependency-path lookups should fall back to the built-in default when no cached path state exists." \
		"$result" "default=$ZXFER_DEFAULT_SECURE_PATH"
}

test_merge_path_allowlists_deduplicates_entries() {
	assertEquals "Merged allowlists should preserve order while deduplicating entries." \
		"/sbin:/bin:/usr/bin" \
		"$(zxfer_merge_path_allowlists "/sbin:/bin" "/bin:/usr/bin")"
}

test_zxfer_apply_secure_path_keeps_runtime_path_equal_to_secure_allowlist() {
	result=$(
		(
			ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/sbin"
			ZXFER_SECURE_PATH_APPEND="/custom/bin"
			zxfer_apply_secure_path
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'runtime=%s\n' "$g_zxfer_runtime_path"
			printf 'path=%s\n' "$PATH"
		)
	)

	assertContains "zxfer_apply_secure_path should honor the configured secure PATH." \
		"$result" "secure=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Runtime PATH should now remain equal to the computed secure allowlist." \
		"$result" "runtime=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Exported PATH should match the strict runtime PATH." \
		"$result" "path=/opt/zfs/bin:/usr/sbin:/custom/bin"
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

test_zxfer_resolve_local_cli_command_safe_rejects_quoted_token_strings() {
	set +e
	output=$(zxfer_resolve_local_cli_command_safe '"/opt/zstd dir/zstd" -3' "compression command")
	status=$?
	set -e

	assertEquals "Local CLI command resolution should fail closed when the configured command relies on shell quoting." \
		1 "$status"
	assertContains "Rejected local CLI commands should explain the literal-token requirement." \
		"$output" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_zxfer_requote_cli_command_with_resolved_head_surfaces_split_failures() {
	set +e
	output=$(zxfer_requote_cli_command_with_resolved_head '"/opt/zstd dir/zstd" -3' "/resolved/zstd" "compression command")
	status=$?
	set -e

	assertEquals "Requoting local CLI commands should fail when the original command cannot be split safely." \
		1 "$status"
	assertContains "Requoted local CLI command failures should preserve the splitter diagnostic." \
		"$output" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_zxfer_initialize_dependency_defaults_sets_runtime_path_and_awk() {
	result=$(
		(
			unset g_cmd_awk
			original_path=$PATH
			zxfer_initialize_dependency_defaults
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'runtime=%s\n' "$g_zxfer_runtime_path"
			printf 'path=%s\n' "$PATH"
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'original=%s\n' "$original_path"
		)
	)

	assertContains "Dependency bootstrap should set the secure path." \
		"$result" "secure=$ZXFER_DEFAULT_SECURE_PATH"
	assertContains "Dependency bootstrap should track the strict runtime PATH value." \
		"$result" "runtime=$ZXFER_DEFAULT_SECURE_PATH"
	assertContains "Dependency bootstrap should leave the caller PATH unchanged until runtime init applies it." \
		"$result" "path=$TEST_ORIGINAL_PATH"
	assertContains "Dependency bootstrap should preserve the original caller PATH for later helper fallbacks." \
		"$result" "original=$TEST_ORIGINAL_PATH"
	assertContains "Dependency bootstrap should resolve an absolute awk helper when the secure PATH contains one." \
		"$result" "awk=/"
}

test_zxfer_initialize_dependency_defaults_falls_back_to_plain_awk_when_secure_path_has_no_awk() {
	result=$(
		(
			unset g_cmd_awk
			ZXFER_SECURE_PATH="$TEST_TMPDIR/no-awk-here"
			zxfer_initialize_dependency_defaults
			printf 'path=%s\n' "$PATH"
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'tokens=%s\n' "$(zxfer_split_tokens_on_whitespace "alpha beta" | tr '\n' ' ')"
		)
	)

	assertContains "Dependency bootstrap should not clobber the caller PATH when the secure PATH lacks awk." \
		"$result" "path=$TEST_ORIGINAL_PATH"
	assertContains "Dependency bootstrap should fall back to plain awk when secure-path lookup finds no absolute awk binary." \
		"$result" "awk=awk"
	assertContains "The plain awk fallback should remain usable before runtime init exports the strict secure PATH." \
		"$result" "tokens=alpha beta "
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
