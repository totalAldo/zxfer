#!/bin/sh
#
# shunit2 tests for src/zxfer_dependencies.sh: the secure PATH, required and
# optional helper lookup, and compression command resolution.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"

zxfer_source_runtime_modules_through "zxfer_dependencies.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_dependencies"
	zxfer_test_remote_host_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	zxfer_test_cleanup_tmpdir
}

setUp() {
	# The tool-resolution cases were written for the remote-host fixture.
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_dependencies_tool_resolution_tests.sh"; then
		zxfer_test_remote_host_fixture_setup
		return
	fi
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	unset g_cmd_awk
	g_zxfer_secure_path=""
}

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

# Purpose: Create executable stand-ins for helper names in one directory.
# Usage: dependencies_test_make_tools DIR TOOL...
dependencies_test_make_tools() {
	l_tools_dir=$1
	shift
	mkdir -p "$l_tools_dir"
	for l_tool in "$@"; do
		printf '#!/bin/sh\nexit 0\n' >"$l_tools_dir/$l_tool"
		chmod 755 "$l_tools_dir/$l_tool"
	done
}

test_zxfer_refresh_secure_path_state_defaults_to_allowlist() {
	zxfer_refresh_secure_path_state

	assertEquals "The default secure PATH should use the built-in allowlist." \
		"/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin" \
		"$g_zxfer_secure_path"
}

test_zxfer_refresh_secure_path_state_preserves_custom_ifs_and_enabled_globbing() {
	secure_fixture_root="$TEST_TMPDIR/secure-glob"
	mkdir -p "$secure_fixture_root/secure-one" "$secure_fixture_root/secure-two"

	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		IFS="|"
		set +f
		ZXFER_SECURE_PATH="$secure_fixture_root/secure-*:/usr/bin"
		zxfer_refresh_secure_path_state
		printf "path=<%s>\n" "$g_zxfer_secure_path"
		printf "ifs=<%s>\n" "$IFS"
		case $- in
		*f*) printf "%s\n" "globbing=disabled" ;;
		*) printf "%s\n" "globbing=enabled" ;;
		esac
	'

	assertContains "Secure PATH parsing should keep wildcard characters literal instead of expanding them against the filesystem." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "path=<$secure_fixture_root/secure-*:/usr/bin>"
	assertContains "Secure PATH parsing should restore a caller-defined IFS exactly." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ifs=<|>"
	assertContains "Secure PATH parsing should leave caller-enabled globbing enabled." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "globbing=enabled"
}

test_zxfer_refresh_secure_path_state_preserves_unset_ifs_and_disabled_globbing() {
	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		unset IFS
		set -f
		ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/bin"
		zxfer_refresh_secure_path_state >/dev/null
		if [ "${IFS+set}" = "set" ]; then
			printf "%s\n" "ifs=set"
		else
			printf "%s\n" "ifs=unset"
		fi
		case $- in
		*f*) printf "%s\n" "globbing=disabled" ;;
		*) printf "%s\n" "globbing=enabled" ;;
		esac
	'

	assertContains "Secure PATH parsing should restore an originally unset IFS." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ifs=unset"
	assertContains "Secure PATH parsing should preserve a caller's disabled-globbing state." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "globbing=disabled"
}

test_zxfer_refresh_secure_path_state_rejects_control_whitespace_without_mutating_shell_state() {
	control_rejection_result=$(
		IFS="|"
		set -f
		ZXFER_SECURE_PATH=$(printf "/opt/trusted/bin\n/opt/translated/bin")
		if zxfer_refresh_secure_path_state; then
			path_status=0
		else
			path_status=$?
		fi
		printf "path-status=%s\n" "$path_status"
		printf "path-result=<%s>\n" "$g_zxfer_secure_path"
		printf "ifs=<%s>\n" "$IFS"
		control_shell_flags=$-
		if [ "${control_shell_flags#*f}" != "$control_shell_flags" ]; then
			printf "%s\n" "globbing=disabled"
		else
			printf "%s\n" "globbing=enabled"
		fi
		ZXFER_SECURE_PATH="/usr/bin"
		ZXFER_SECURE_PATH_APPEND=$(printf "/opt/append\t/opt/translated")
		if zxfer_refresh_secure_path_state; then
			append_status=0
		else
			append_status=$?
		fi
		printf "append-status=%s\n" "$append_status"
	)

	assertContains "Newline-bearing secure-PATH entries must fail closed before remote rendering can translate them." \
		"$control_rejection_result" "path-status=1"
	assertContains "Rejected secure-PATH input must not publish a partial path." \
		"$control_rejection_result" "path-result=<>"
	assertContains "Secure-PATH rejection should preserve a caller-defined IFS." \
		"$control_rejection_result" "ifs=<|>"
	assertContains "Secure-PATH rejection should preserve disabled globbing." \
		"$control_rejection_result" "globbing=disabled"
	assertContains "Control whitespace in ZXFER_SECURE_PATH_APPEND must also fail closed." \
		"$control_rejection_result" "append-status=1"
}

test_zxfer_refresh_secure_path_state_publishes_only_the_secure_path() {
	result=$(
		(
			ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/sbin"
			ZXFER_SECURE_PATH_APPEND="/custom/bin"
			zxfer_refresh_secure_path_state
			printf 'status=%s\n' "$?"
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'path=%s\n' "$PATH"
		)
	)

	assertContains "Refreshing should succeed for a single-line secure PATH." \
		"$result" "status=0"
	assertContains "Refreshing should publish the configured secure PATH." \
		"$result" "secure=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Refreshing should leave the live PATH alone until zxfer_apply_secure_path." \
		"$result" "path=$TEST_ORIGINAL_PATH"
}

test_zxfer_refresh_secure_path_state_fails_closed_through_the_rejection_owner() {
	result=$(
		(
			g_zxfer_secure_path="/kept/secure/path"
			ZXFER_SECURE_PATH=$(printf '/opt/trusted/bin\n/opt/translated/bin')
			zxfer_set_failure_context_if_empty() {
				printf 'context=%s:%s\n' "$1" "$2"
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_refresh_secure_path_state
			printf 'refresh=%s secure=%s\n' "$?" "$g_zxfer_secure_path"
			zxfer_reject_invalid_secure_path_configuration
			printf '%s\n' "not reached"
		)
	)
	status=$?

	assertEquals "Rejecting a control-whitespace-bearing secure PATH should fail closed." \
		1 "$status"
	assertContains "A rejected refresh should keep the previous secure PATH." \
		"$result" "refresh=1 secure=/kept/secure/path"
	assertContains "The secure-PATH owner should classify rejected configuration before throwing." \
		"$result" "context=dependency:secure PATH validation"
	assertContains "The secure-PATH owner should preserve the stable rejection diagnostic." \
		"$result" "single-line absolute path without control whitespace"
	assertNotContains "The rejection should stop the run." "$result" "not reached"
}

test_zxfer_apply_secure_path_exports_the_secure_path() {
	result=$(
		(
			g_zxfer_secure_path="/opt/zfs/bin:/usr/sbin:/custom/bin"
			zxfer_apply_secure_path
			printf 'path=%s\n' "$PATH"
			child_env=$(/usr/bin/env)
			[ "${child_env#*PATH=/opt/zfs/bin:/usr/sbin:/custom/bin}" = "$child_env" ] ||
				printf '%s\n' "exported"
		)
	)

	assertContains "zxfer_apply_secure_path should narrow PATH to the secure PATH." \
		"$result" "path=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "zxfer_apply_secure_path should export the narrowed PATH to children." \
		"$result" "exported"
}

test_zxfer_apply_secure_path_refuses_an_empty_secure_path() {
	result=$(
		(
			g_zxfer_secure_path=""
			PATH=/usr/bin:/bin
			zxfer_throw_error() {
				printf 'throw=%s\n' "$1"
				exit 7
			}
			zxfer_apply_secure_path
			printf 'path=%s\n' "$PATH"
		)
	)
	status=$?

	assertEquals "An empty secure PATH should fail closed." 7 "$status"
	assertEquals "An empty secure PATH should report the secure PATH rejection and never export PATH." \
		"throw=$ZXFER_INVALID_SECURE_PATH_MESSAGE" "$result"
}

test_zxfer_find_tool_in_path_returns_the_first_executable_regular_file() {
	tools="$TEST_TMPDIR/find_tool"
	mkdir -p "$tools/dir-entry/mocktool" "$tools/noexec"
	: >"$tools/noexec/mocktool"
	chmod 644 "$tools/noexec/mocktool"
	dependencies_test_make_tools "$tools/exec" mocktool
	dependencies_test_make_tools "$tools/later" mocktool

	zxfer_find_tool_in_path mocktool \
		"relative:$tools/dir-entry:$tools/noexec::$tools/exec:$tools/later"
	status=$?

	assertEquals "A tool on the list should be found." 0 "$status"
	assertEquals "Lookup should skip relative entries, directories and non-executable files." \
		"$tools/exec/mocktool" "$g_zxfer_tool_path_result"
}

test_zxfer_find_tool_in_path_checks_slash_paths_as_given_and_misses_cleanly() {
	tools="$TEST_TMPDIR/find_tool_slash"
	dependencies_test_make_tools "$tools/exec" mocktool
	mkdir -p "$tools/noexec"
	: >"$tools/noexec/mocktool"

	zxfer_find_tool_in_path "$tools/exec/mocktool" ""
	assertEquals "An executable path containing a slash should be accepted as given." \
		"0:$tools/exec/mocktool" "$?:$g_zxfer_tool_path_result"

	zxfer_find_tool_in_path "$tools/noexec/mocktool" "$tools/exec"
	assertEquals "A slash path is never searched for on the list." \
		"1:" "$?:$g_zxfer_tool_path_result"

	zxfer_find_tool_in_path missingtool "$tools/exec"
	assertEquals "A miss should clear the result." "1:" "$?:$g_zxfer_tool_path_result"

	zxfer_find_tool_in_path "" "$tools/exec"
	assertEquals "An empty tool name should never match." "1:" "$?:$g_zxfer_tool_path_result"

	zxfer_find_tool_in_path mocktool "$tools/exec/"
	assertEquals "A trailing slash on a list entry should not double the separator." \
		"0:$tools/exec/mocktool" "$?:$g_zxfer_tool_path_result"
}

test_zxfer_validate_resolved_tool_path_rejects_relative_path() {
	zxfer_validate_resolved_tool_path "awk" "awk"

	assertEquals "Relative tool paths should be rejected." 1 "$?"
	assertContains "Relative path rejection should require an absolute path." \
		"$g_zxfer_required_tool_result" "requires an absolute path"
}

test_zxfer_validate_resolved_tool_path_accepts_shell_quoted_absolute_path() {
	quoted_path="'/tmp/mocktool.\$(touch marker)'"

	zxfer_validate_resolved_tool_path "$quoted_path" "mocktool"
	status=$?

	assertEquals "Shell-quoted absolute paths from command -v should remain valid after normalization." 0 "$status"
	assertEquals "Shell-quoted absolute paths from command -v should be normalized before validation." \
		"/tmp/mocktool.\$(touch marker)" "$g_zxfer_required_tool_result"
}

test_zxfer_validate_resolved_tool_path_accepts_double_quoted_absolute_path() {
	quoted_path="\"/tmp/mocktool.\$(touch marker)\""

	zxfer_validate_resolved_tool_path "$quoted_path" "mocktool"
	status=$?

	assertEquals "Double-quoted absolute paths from command -v should remain valid after normalization." 0 "$status"
	assertEquals "Double-quoted absolute paths from command -v should be normalized before validation." \
		"/tmp/mocktool.\$(touch marker)" "$g_zxfer_required_tool_result"
}

test_zxfer_validate_resolved_tool_path_drops_trailing_newlines_and_publishes_the_result() {
	zxfer_validate_resolved_tool_path "/opt/bin/mocktool

" "mocktool" >"$TEST_TMPDIR/validate.out"
	status=$?

	assertEquals "Trailing newlines, as remote probe output carries, should not fail validation." \
		0 "$status"
	assertEquals "Validation should publish the path without its trailing newlines." \
		"/opt/bin/mocktool" "$g_zxfer_required_tool_result"
	assertEquals "Validation should print nothing." \
		"" "$(cat "$TEST_TMPDIR/validate.out")"

	zxfer_validate_resolved_tool_path "/opt/bin/mock
tool" "mocktool" >/dev/null
	assertEquals "An embedded newline should still be rejected." 1 "$?"
	assertEquals "The rejection should be published." \
		"Required dependency \"mocktool\" resolved to \"/opt/bin/mock
tool\", but zxfer requires a single-line absolute path without control whitespace." \
		"$g_zxfer_required_tool_result"
}

test_zxfer_require_tool_publishes_the_absolute_path() {
	tools="$TEST_TMPDIR/require_tool"
	dependencies_test_make_tools "$tools" mocktool
	g_zxfer_secure_path="relative:$tools"

	zxfer_require_tool mocktool

	assertEquals "A found helper should be published as its absolute path." \
		"$tools/mocktool" "$g_zxfer_required_tool_result"
}

test_zxfer_require_tool_throws_a_dependency_failure_when_missing() {
	mkdir -p "$TEST_TMPDIR/require_empty"

	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		g_zxfer_secure_path="$TEST_TMPDIR/require_empty"
		zxfer_throw_error() {
			printf "class=%s message=%s\n" "$g_zxfer_failure_class" "$1"
			exit 1
		}
		zxfer_require_tool mocktool "mock tool"
		printf "%s\n" "not reached"
	'

	assertEquals "A missing helper should stop the run." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A missing helper should be a dependency failure with the secure-PATH guidance." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "class=dependency message=Required dependency \"mock tool\" not found in secure PATH ($TEST_TMPDIR/require_empty). Set ZXFER_SECURE_PATH or install the binary."
	assertNotContains "The failure should not return to the caller." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "not reached"
}

test_zxfer_resolve_cli_command_safe_rejects_quoted_token_strings() {
	zxfer_resolve_cli_command_safe "" '"/opt/zstd dir/zstd" -3' "compression command"
	status=$?

	assertEquals "CLI command resolution should fail closed when the configured command relies on shell quoting." \
		1 "$status"
	assertEquals "Rejected CLI commands should explain the literal-token requirement." \
		"compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$g_zxfer_resolved_cli_command_result"
}

test_zxfer_resolve_cli_command_safe_requotes_the_local_head_and_keeps_arguments() {
	tools="$TEST_TMPDIR/cli_resolve"
	dependencies_test_make_tools "$tools" zstd
	g_zxfer_secure_path=$tools

	zxfer_resolve_cli_command_safe "" "zstd  -T0 -9;x" "compression command"

	assertEquals "A local CLI command should resolve its head on the secure PATH." 0 "$?"
	assertEquals "The requoted command should keep every argument as one quoted token." \
		"'$tools/zstd' '-T0' '-9;' 'x'" "$g_zxfer_resolved_cli_command_result"
}

test_zxfer_reset_dependency_state_drops_inherited_commands_and_secure_path() {
	result=$(
		(
			g_zxfer_secure_path=/inherited
			g_cmd_awk=/inherited/awk
			g_cmd_cat=/inherited/cat
			g_cmd_parallel=/inherited/parallel
			g_cmd_ps=/inherited/ps
			g_cmd_ssh=/inherited/ssh
			g_cmd_zfs=/inherited/zfs
			g_cmd_compress="evil -9"
			g_cmd_decompress="evil -d"
			g_cmd_compress_safe="'evil'"
			g_cmd_decompress_safe="'evil'"
			g_origin_cmd_compress_safe="'evil'"
			g_target_cmd_decompress_safe="'evil'"
			zxfer_reset_dependency_state
			printf '<%s>' "$g_zxfer_secure_path" \
				"$g_cmd_awk" "$g_cmd_cat" "$g_cmd_parallel" "$g_cmd_ps" \
				"$g_cmd_ssh" "$g_cmd_zfs" "$g_cmd_compress_safe" \
				"$g_cmd_decompress_safe" "$g_origin_cmd_compress_safe" \
				"$g_target_cmd_decompress_safe"
			printf '\ncompress=%s decompress=%s\n' "$g_cmd_compress" "$g_cmd_decompress"
		)
	)

	assertContains "The reset should clear the secure PATH and every helper command." \
		"$result" "<><><><><><><><><><><>"
	assertContains "The reset should restore the default compression commands." \
		"$result" "compress=zstd -3 decompress=zstd -d"
}

test_zxfer_initialize_dependency_reporting_defaults_uses_the_built_in_path_awk() {
	expected_awk='awk'
	for awk_dir in /sbin /bin /usr/sbin /usr/bin /usr/local/sbin /usr/local/bin; do
		if [ -f "$awk_dir/awk" ] && [ -x "$awk_dir/awk" ]; then
			expected_awk=$awk_dir/awk
			break
		fi
	done
	result=$(
		(
			g_cmd_awk="$TEST_TMPDIR/inherited-untrusted-awk"
			ZXFER_SECURE_PATH="$TEST_TMPDIR/operator-secure-path"
			zxfer_initialize_dependency_reporting_defaults
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "The reporting awk should come from the built-in secure PATH, never an inherited value." \
		"$expected_awk" "$result"
}

test_zxfer_initialize_dependency_reporting_defaults_falls_back_to_plain_awk() {
	result=$(
		(
			g_cmd_awk="$TEST_TMPDIR/inherited-untrusted-awk"
			ZXFER_DEFAULT_SECURE_PATH="$TEST_TMPDIR/no-awk-here"
			zxfer_initialize_dependency_reporting_defaults
			printf 'awk=%s\n' "$g_cmd_awk"
			zxfer_split_tokens_into_result "alpha beta"
			printf 'tokens=%s\n' "$g_zxfer_split_tokens_result" | tr '\n' ' '
		)
	)

	assertContains "Without an awk on the built-in path the reporting awk should be plain awk." \
		"$result" "awk=awk"
	assertContains "The plain awk fallback should remain usable before the secure PATH is applied." \
		"$result" "tokens=alpha beta "
}

test_zxfer_init_dependency_tool_defaults_resolves_helpers_on_the_secure_path() {
	tools="$TEST_TMPDIR/tool_defaults"
	dependencies_test_make_tools "$tools" awk zfs ps

	result=$(
		(
			g_zxfer_secure_path="$tools"
			g_cmd_parallel="/inherited/parallel"
			zxfer_init_dependency_tool_defaults
			printf 'awk=%s zfs=%s ps=%s parallel=<%s>\n' \
				"$g_cmd_awk" "$g_cmd_zfs" "$g_cmd_ps" "$g_cmd_parallel"
			dependencies_test_make_tools "$tools" parallel
			zxfer_init_dependency_tool_defaults
			printf 'optional=%s\n' "$g_cmd_parallel"
		)
	)

	assertContains "Dependency defaults should resolve awk, zfs and ps on the secure PATH and leave a missing parallel unset." \
		"$result" "awk=$tools/awk zfs=$tools/zfs ps=$tools/ps parallel=<>"
	assertContains "Dependency defaults should resolve an optional parallel that is present." \
		"$result" "optional=$tools/parallel"
}

test_zxfer_init_dependency_tool_defaults_reports_a_missing_zfs() {
	tools="$TEST_TMPDIR/tool_defaults_no_zfs"
	dependencies_test_make_tools "$tools" awk ps

	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		g_zxfer_secure_path="$tools"
		zxfer_throw_error() {
			printf "class=%s message=%s\n" "$g_zxfer_failure_class" "$1"
			exit 1
		}
		zxfer_init_dependency_tool_defaults
	'

	assertEquals "A missing zfs should stop startup." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A missing zfs should keep its dependency classification and message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "class=dependency message=Required dependency \"zfs\" not found in secure PATH ($tools). Set ZXFER_SECURE_PATH or install the binary."
}

test_zxfer_refresh_secure_path_state_filters_relative_entries() {
	result=$(
		ZXFER_SECURE_PATH="./bin:/tmp/bin:relative:/usr/sbin"
		ZXFER_SECURE_PATH_APPEND=""
		zxfer_refresh_secure_path_state
		printf '%s\n' "$g_zxfer_secure_path"
	)

	assertEquals "Relative path segments must be dropped from the secure PATH." "/tmp/bin:/usr/sbin" "$result"
}

test_zxfer_refresh_secure_path_state_appends_extra_entries() {
	result=$(
		ZXFER_SECURE_PATH="/sbin:/bin"
		ZXFER_SECURE_PATH_APPEND=":/opt/zfs/bin:./malicious"
		zxfer_refresh_secure_path_state
		printf '%s\n' "$g_zxfer_secure_path"
	)

	assertEquals "ZXFER_SECURE_PATH_APPEND should only add absolute directories to the allowlist." "/sbin:/bin:/opt/zfs/bin" "$result"
}

test_zxfer_refresh_secure_path_state_uses_append_when_default_is_empty() {
	result=$(
		ZXFER_DEFAULT_SECURE_PATH=""
		ZXFER_SECURE_PATH=""
		ZXFER_SECURE_PATH_APPEND="/opt/trusted/bin"
		zxfer_refresh_secure_path_state
		printf '%s\n' "$g_zxfer_secure_path"
	)

	assertEquals "Append-only secure-path configuration should still work when the built-in allowlist is empty." \
		"/opt/trusted/bin" "$result"
}

test_zxfer_refresh_secure_path_state_falls_back_to_default_when_all_entries_are_filtered() {
	result=$(
		ZXFER_SECURE_PATH="relative:.:./bin"
		ZXFER_SECURE_PATH_APPEND="also-relative:./still-bad"
		zxfer_refresh_secure_path_state
		printf '%s\n' "$g_zxfer_secure_path"
	)

	assertEquals "When every configured secure-PATH entry is filtered out, zxfer should fall back to the built-in allowlist." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$result"
}

test_refresh_compression_commands_tokenizes_custom_pipeline() {
	# A -Z command is resolved and quoted token by token, so the shell never
	# runs the raw string.
	zstd_dir="$TEST_TMPDIR/custom_pipeline_bin"
	mkdir -p "$zstd_dir"
	printf '#!/bin/sh\nexit 0\n' >"$zstd_dir/zstd"
	chmod 755 "$zstd_dir/zstd"

	result=$(
		g_zxfer_secure_path=$zstd_dir
		g_option_z_compress=1
		g_cmd_compress="zstd -3;touch /tmp/pwn"
		g_cmd_decompress="zstd -d"
		zxfer_refresh_compression_commands
		printf '%s\n' "$g_cmd_compress_safe"
	)

	assertEquals "Compression command tokens should be quoted." \
		"'$zstd_dir/zstd' '-3;' 'touch' '/tmp/pwn'" "$result"
}

# zxfer-test-fragment: suites/zxfer_dependencies_tool_resolution_tests.sh
# shellcheck source=tests/suites/zxfer_dependencies_tool_resolution_tests.sh
. "$TESTS_DIR/suites/zxfer_dependencies_tool_resolution_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_dependencies.sh" \
		"$TESTS_DIR/suites/zxfer_dependencies_tool_resolution_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
