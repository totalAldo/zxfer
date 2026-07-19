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

test_zxfer_compute_secure_path_preserves_custom_ifs_and_enabled_globbing() {
	secure_fixture_root=$(mktemp -d -t zxfer-dependencies-secure.XXXXXX) ||
		fail "Unable to create secure-PATH fixture directory."
	mkdir -p "$secure_fixture_root/secure-one" "$secure_fixture_root/secure-two"
	path_output_file="$secure_fixture_root/secure-path-custom-ifs.out"

	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		IFS="|"
		set +f
		ZXFER_SECURE_PATH="$secure_fixture_root/secure-*:/usr/bin"
		zxfer_compute_secure_path >"$secure_fixture_root/secure-path-custom-ifs.out"
		printf "ifs=<%s>\n" "$IFS"
		case $- in
		*f*) printf "%s\n" "globbing=disabled" ;;
		*) printf "%s\n" "globbing=enabled" ;;
		esac
	'

	assertEquals "Secure PATH parsing should keep wildcard characters literal instead of expanding them against the filesystem." \
		"$secure_fixture_root/secure-*:/usr/bin" "$(cat "$path_output_file")"
	assertContains "Secure PATH parsing should restore a caller-defined IFS exactly." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ifs=<|>"
	assertContains "Secure PATH parsing should leave caller-enabled globbing enabled." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "globbing=enabled"

	rm -rf "$secure_fixture_root"
}

test_zxfer_compute_secure_path_preserves_unset_ifs_and_disabled_globbing() {
	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		unset IFS
		set -f
		ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/bin"
		zxfer_compute_secure_path >/dev/null
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

test_zxfer_compute_secure_path_rejects_control_whitespace_without_mutating_shell_state() {
	control_output_file=$(mktemp -t zxfer-dependencies-control.XXXXXX) ||
		fail "Unable to create secure-PATH rejection capture file."
	control_rejection_result=$(
		IFS="|"
		set -f
		ZXFER_SECURE_PATH=$(printf "/opt/trusted/bin\n/opt/translated/bin")
		if zxfer_compute_secure_path >"$control_output_file"; then
			path_status=0
		else
			path_status=$?
		fi
		printf "path-status=%s\n" "$path_status"
		printf "path-output=<%s>\n" \
			"$(cat "$control_output_file")"
		printf "ifs=<%s>\n" "$IFS"
		control_shell_flags=$-
		if [ "${control_shell_flags#*f}" != "$control_shell_flags" ]; then
			printf "%s\n" "globbing=disabled"
		else
			printf "%s\n" "globbing=enabled"
		fi
		ZXFER_SECURE_PATH="/usr/bin"
		ZXFER_SECURE_PATH_APPEND=$(printf "/opt/append\t/opt/translated")
		if zxfer_compute_secure_path >/dev/null; then
			append_status=0
		else
			append_status=$?
		fi
		printf "append-status=%s\n" "$append_status"
	)
	rm -f "$control_output_file"

	assertContains "Newline-bearing secure-PATH entries must fail closed before remote rendering can translate them." \
		"$control_rejection_result" "path-status=1"
	assertContains "Rejected secure-PATH input must not publish a partial path." \
		"$control_rejection_result" "path-output=<>"
	assertContains "Secure-PATH rejection should preserve a caller-defined IFS." \
		"$control_rejection_result" "ifs=<|>"
	assertContains "Secure-PATH rejection should preserve disabled globbing." \
		"$control_rejection_result" "globbing=disabled"
	assertContains "Control whitespace in ZXFER_SECURE_PATH_APPEND must also fail closed." \
		"$control_rejection_result" "append-status=1"
}

test_zxfer_initialize_dependency_defaults_reports_invalid_secure_path() {
	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		ZXFER_SECURE_PATH=$(printf "/opt/trusted/bin\n/opt/translated/bin")
		zxfer_initialize_dependency_defaults
	'

	assertEquals "Dependency bootstrap should stop on a secure PATH that cannot survive remote rendering." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Dependency bootstrap should explain the control-whitespace restriction." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "single-line absolute path without control whitespace"
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

test_zxfer_apply_secure_path_rejects_invalid_configuration_through_owner_path() {
	set +e
	result=$(
		(
			ZXFER_SECURE_PATH=$(printf '/opt/trusted/bin\n/opt/translated/bin')
			zxfer_set_failure_context_if_empty() {
				printf 'context=%s:%s\n' "$1" "$2"
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_apply_secure_path
		)
	)
	status=$?
	set -e

	assertEquals "Applying a control-whitespace-bearing secure PATH should fail closed." \
		1 "$status"
	assertContains "The secure-PATH owner should classify rejected configuration before throwing." \
		"$result" "context=dependency:secure PATH validation"
	assertContains "The secure-PATH owner should preserve the stable rejection diagnostic." \
		"$result" "single-line absolute path without control whitespace"
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

test_zxfer_assign_required_tool_rejects_untrusted_assignment_target_before_lookup() {
	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		g_dependency_lookup_calls=0
		g_dependency_assignment_injected=0
		zxfer_find_required_tool() {
			g_dependency_lookup_calls=$((g_dependency_lookup_calls + 1))
			printf "%s\n" "/opt/mock/mocktool"
		}
		zxfer_throw_error() {
			printf "class=%s\n" "$g_zxfer_failure_class"
			printf "message=%s\n" "$1"
			printf "lookups=%s\n" "$g_dependency_lookup_calls"
			printf "injected=%s\n" "$g_dependency_assignment_injected"
			exit 1
		}
		zxfer_assign_required_tool "g_cmd_safe=ignored; g_dependency_assignment_injected" mocktool "mocktool"
	'

	assertEquals "Invalid dependency assignment targets should fail closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Invalid dependency assignment targets should use dependency failure classification." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "class=dependency"
	assertContains "Invalid dependency assignment targets should use a stable internal-error diagnostic." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "message=Invalid internal dependency assignment target."
	assertContains "Dependency assignment targets should be validated before helper lookup." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "lookups=0"
	assertContains "Rejected dependency targets should never be evaluated." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "injected=0"
}

test_zxfer_assign_required_tool_rejects_valid_names_outside_command_prefix() {
	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_assign_required_tool g_dependency_result mocktool "mocktool"
	'

	assertEquals "Dependency assignment should reject valid shell names outside the g_cmd_ namespace." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Out-of-namespace dependency targets should use the stable internal-error diagnostic." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Invalid internal dependency assignment target."
}

test_zxfer_set_dependency_command_publishes_each_supported_command_slot() {
	zxfer_set_dependency_command g_cmd_compress_safe "'/opt/bin/zstd' '-3'"
	zxfer_set_dependency_command g_cmd_decompress "/opt/bin/zstd -d"
	zxfer_set_dependency_command g_cmd_decompress_safe "'/opt/bin/zstd' '-d'"
	zxfer_set_dependency_command g_cmd_parallel "/opt/bin/parallel"
	zxfer_set_dependency_command g_cmd_ps "/bin/ps"
	zxfer_set_dependency_command g_cmd_zfs "/sbin/zfs"

	assertEquals "The dependency owner should publish the safe compression command." \
		"'/opt/bin/zstd' '-3'" "$g_cmd_compress_safe"
	assertEquals "The dependency owner should publish the decompression command." \
		"/opt/bin/zstd -d" "$g_cmd_decompress"
	assertEquals "The dependency owner should publish the safe decompression command." \
		"'/opt/bin/zstd' '-d'" "$g_cmd_decompress_safe"
	assertEquals "The dependency owner should publish the optional parallel command." \
		"/opt/bin/parallel" "$g_cmd_parallel"
	assertEquals "The dependency owner should publish the process-inspection command." \
		"/bin/ps" "$g_cmd_ps"
	assertEquals "The dependency owner should publish the ZFS command." \
		"/sbin/zfs" "$g_cmd_zfs"
}

test_zxfer_set_endpoint_compression_command_rejects_unknown_selector() {
	set +e
	zxfer_set_endpoint_compression_command other compress "'/opt/bin/zstd' '-3'"
	status=$?

	assertEquals "Endpoint compression publication should reject unknown role and codec selectors." \
		2 "$status"
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

test_zxfer_initialize_dependency_defaults_replaces_inherited_awk_command() {
	g_cmd_awk="$TEST_TMPDIR/inherited-untrusted-awk"

	zxfer_initialize_dependency_defaults

	assertNotEquals "Dependency bootstrap must not preserve an inherited internal awk command." \
		"$TEST_TMPDIR/inherited-untrusted-awk" "$g_cmd_awk"
	case $g_cmd_awk in
	/* | awk) l_awk_is_bootstrap_safe=0 ;;
	*) l_awk_is_bootstrap_safe=1 ;;
	esac
	assertEquals "The replacement awk command should be an absolute secure-PATH result or the safe early-PATH fallback." \
		0 "$l_awk_is_bootstrap_safe"
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

test_zxfer_init_dependency_tool_defaults_owns_command_state() {
	result=$(
		(
			g_zxfer_dependency_path=""
			zxfer_assign_required_tool() {
				eval "$1=/stub/$2"
			}
			zxfer_refresh_compression_commands() {
				printf '%s\n' "compression_refreshed=yes"
			}

			zxfer_init_dependency_tool_defaults
			printf 'zfs=%s\n' "$g_cmd_zfs"
			printf 'ssh=<%s>\n' "$g_cmd_ssh"
			printf 'compress=%s\n' "$g_cmd_compress"
			printf 'decompress=%s\n' "$g_cmd_decompress"
			printf 'ps=%s\n' "$g_cmd_ps"
			printf 'parallel=<%s>\n' "$g_cmd_parallel"
		)
	)

	assertContains "Dependency defaults should resolve the required zfs helper." \
		"$result" "zfs=/stub/zfs"
	assertContains "Dependency defaults should keep ssh lazy for local-only runs." \
		"$result" "ssh=<>"
	assertContains "Dependency defaults should retain the established compression command." \
		"$result" "compress=zstd -3"
	assertContains "Dependency defaults should retain the established decompression command." \
		"$result" "decompress=zstd -d"
	assertContains "Dependency defaults should resolve ps for process identity checks." \
		"$result" "ps=/stub/ps"
	assertContains "Dependency defaults should leave missing optional parallel unset." \
		"$result" "parallel=<>"
	assertContains "Dependency defaults should refresh the safe compression renderings." \
		"$result" "compression_refreshed=yes"
}

test_zxfer_init_dependency_tool_defaults_rejects_nonabsolute_parallel_resolution() {
	set +e
	result=$(
		(
			g_zxfer_dependency_path=/definitely/missing
			parallel() {
				:
			}
			zxfer_assign_required_tool() {
				zxfer_set_dependency_command "$1" "/stub/$2"
			}
			zxfer_refresh_compression_commands() {
				:
			}
			zxfer_set_failure_class() {
				g_zxfer_failure_class=$1
			}
			zxfer_throw_error() {
				printf 'class=%s\n' "$g_zxfer_failure_class"
				printf 'message=%s\n' "$1"
				printf 'status=%s\n' "${2:-1}"
				exit "${2:-1}"
			}
			zxfer_init_dependency_tool_defaults
		)
	)
	status=$?
	set -e

	assertEquals "Dependency initialization should preserve validation failure status for a nonabsolute optional parallel path." \
		1 "$status"
	assertContains "Nonabsolute optional parallel paths should retain dependency failure classification." \
		"$result" "class=dependency"
	assertContains "Nonabsolute optional parallel paths should retain the absolute-path diagnostic." \
		"$result" "zxfer requires an absolute path"
	assertContains "Nonabsolute optional parallel paths should preserve the lower-level validation status." \
		"$result" "status=1"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
