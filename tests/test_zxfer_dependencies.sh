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

# Each row: ZXFER_SECURE_PATH|ZXFER_SECURE_PATH_APPEND|built-in default (the
# module's own, or empty)|secure PATH. Only absolute entries survive, and a
# list left empty falls back to the built-in allowlist.
test_zxfer_refresh_secure_path_state_keeps_only_absolute_entries() {
	result=$(
		(
			l_builtin=$ZXFER_DEFAULT_SECURE_PATH
			while IFS='|' read -r l_secure l_append l_default l_expected; do
				ZXFER_SECURE_PATH=$l_secure
				ZXFER_SECURE_PATH_APPEND=$l_append
				ZXFER_DEFAULT_SECURE_PATH=$l_builtin
				[ "$l_default" = builtin ] || ZXFER_DEFAULT_SECURE_PATH=""
				zxfer_refresh_secure_path_state
				l_status=$?
				[ "$l_status:$g_zxfer_secure_path" = "0:$l_expected" ] ||
					printf 'row <%s|%s|%s>: status=%s secure=%s\n' "$l_secure" \
						"$l_append" "$l_default" "$l_status" "$g_zxfer_secure_path"
			done <<'EOF'
||builtin|/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin
/opt/zfs/bin:/usr/sbin|/custom/bin|builtin|/opt/zfs/bin:/usr/sbin:/custom/bin
./bin:/tmp/bin:relative:/usr/sbin||builtin|/tmp/bin:/usr/sbin
/sbin:/bin|:/opt/zfs/bin:./malicious|builtin|/sbin:/bin:/opt/zfs/bin
|/opt/trusted/bin|empty|/opt/trusted/bin
relative:.:./bin|also-relative:./still-bad|builtin|/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin
EOF
			printf 'path=%s\n' "$PATH"
		)
	)

	assertEquals "Every row should publish its secure PATH and leave the live PATH alone until zxfer_apply_secure_path." \
		"path=$TEST_ORIGINAL_PATH" "$result"
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

test_zxfer_refresh_secure_path_state_rejects_control_whitespace_without_mutating_shell_state() {
	control_rejection_result=$(
		IFS="|"
		set -f
		g_zxfer_secure_path=/kept/secure/path
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
	assertContains "Rejected secure-PATH input must keep the previous secure PATH, never publish a partial one." \
		"$control_rejection_result" "path-result=</kept/secure/path>"
	assertContains "Secure-PATH rejection should preserve a caller-defined IFS." \
		"$control_rejection_result" "ifs=<|>"
	assertContains "Secure-PATH rejection should preserve disabled globbing." \
		"$control_rejection_result" "globbing=disabled"
	assertContains "Control whitespace in ZXFER_SECURE_PATH_APPEND must also fail closed." \
		"$control_rejection_result" "append-status=1"
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

# command -v quoting (OmniOS) and the trailing newlines of remote probe output
# are normalized away, never evaluated; anything but one absolute line is
# refused with the reason, naming the host scope when there is one.
test_zxfer_validate_resolved_tool_path_publishes_one_absolute_line_or_the_refusal() {
	tab=$(printf '\t')

	zxfer_validate_resolved_tool_path "'/tmp/mocktool.\$(touch marker)'" mocktool
	assertEquals "A single-quoted absolute path should be unquoted." \
		"0:/tmp/mocktool.\$(touch marker)" "$?:$g_zxfer_required_tool_result"
	zxfer_validate_resolved_tool_path "\"/tmp/mocktool.\$(touch marker)\"" mocktool
	assertEquals "A double-quoted absolute path should be unquoted." \
		"0:/tmp/mocktool.\$(touch marker)" "$?:$g_zxfer_required_tool_result"
	zxfer_validate_resolved_tool_path "/opt/bin/mocktool$ZXFER_LF$ZXFER_LF" mocktool \
		>"$TEST_TMPDIR/validate.out"
	assertEquals "Trailing newlines should be dropped." \
		"0:/opt/bin/mocktool" "$?:$g_zxfer_required_tool_result"
	assertEquals "Validation should print nothing." "" "$(cat "$TEST_TMPDIR/validate.out")"

	zxfer_validate_resolved_tool_path awk awk
	assertEquals "A relative path should be refused." \
		"1:Required dependency \"awk\" resolved to \"awk\", but zxfer requires an absolute path." \
		"$?:$g_zxfer_required_tool_result"
	zxfer_validate_resolved_tool_path "'/tmp/it's'" mocktool
	assertEquals "A quote inside a quoted path should keep the quotes, and so be refused." \
		"1:Required dependency \"mocktool\" resolved to \"'/tmp/it's'\", but zxfer requires an absolute path." \
		"$?:$g_zxfer_required_tool_result"
	zxfer_validate_resolved_tool_path "/opt/bin/mock${ZXFER_LF}tool" mocktool
	assertEquals "An embedded newline should be refused." \
		"1:Required dependency \"mocktool\" resolved to \"/opt/bin/mock${ZXFER_LF}tool\", but zxfer requires a single-line absolute path without control whitespace." \
		"$?:$g_zxfer_required_tool_result"
	zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" mocktool "host origin.example"
	assertEquals "A tab should be refused, naming the host scope." \
		"1:Required dependency \"mocktool\" on host origin.example resolved to \"/tmp/mock${tab}tool\", but zxfer requires a single-line absolute path without control whitespace." \
		"$?:$g_zxfer_required_tool_result"
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

test_refresh_compression_commands_resolves_each_head_and_quotes_every_token() {
	# A -Z command is resolved and quoted token by token, so the shell never
	# runs the raw string; the decompressor resolves the same way.
	zstd_dir="$TEST_TMPDIR/custom_pipeline_bin"
	dependencies_test_make_tools "$zstd_dir" zstd

	result=$(
		g_zxfer_secure_path=$zstd_dir
		g_option_z_compress=1
		g_cmd_compress="zstd -3;touch /tmp/pwn"
		g_cmd_decompress="zstd -d"
		zxfer_refresh_compression_commands
		printf 'compress=%s\n' "$g_cmd_compress_safe"
		printf 'decompress=%s\n' "$g_cmd_decompress_safe"
	)

	assertEquals "Both heads should resolve on the secure PATH, and every token should be quoted." \
		"compress='$zstd_dir/zstd' '-3;' 'touch' '/tmp/pwn'
decompress='$zstd_dir/zstd' '-d'" "$result"
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
