#!/bin/sh
#
# shunit2 tests for zxfer launcher module loading.
#
# shellcheck disable=SC1090,SC2016,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_launcher"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# Purpose: Build a launcher tree with an empty src file for every manifest
# module and a zxfer_main stub, so a test sees only what ./zxfer itself does.
# Usage: zxfer_test_create_launcher_fixture DIR; each run appends
# "main ARGS prescan=<VALUE>" to $ZXFER_TEST_LOG.
zxfer_test_create_launcher_fixture() {
	mkdir -p "$1/src"
	cp "$ZXFER_ROOT/zxfer" "$1/zxfer"
	cp "$ZXFER_ROOT/src/zxfer_modules.sh" "$1/src/zxfer_modules.sh"
	for l_fixture_module in $ZXFER_SOURCE_MODULE_MANIFEST; do
		: >"$1/src/$l_fixture_module"
	done
	cat >>"$1/src/zxfer_session.sh" <<'EOF'
zxfer_set_original_invocation() {
	:
}
zxfer_main() {
	printf 'main %s prescan=<%s>\n' "$*" "${g_zxfer_profile_prescan-unset}" >>"${ZXFER_TEST_LOG:?}"
}
EOF
}

test_module_loader_has_no_source_time_initialization() {
	zxfer_test_capture_subshell "
		unset -f zxfer_set_failure_stage
		ZXFER_SOURCE_MODULES_ROOT=\"$ZXFER_ROOT\"
		. \"$ZXFER_ROOT/src/zxfer_modules.sh\"
		if command -v zxfer_set_failure_stage >/dev/null 2>&1; then
			exit 9
		fi
		zxfer_load_modules
		command -v zxfer_set_failure_stage >/dev/null 2>&1
	"

	assertEquals "Sourcing the manifest should define only loader functions until zxfer_load_modules is called." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Loading every module should not emit output." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_module_loader_does_not_publish_a_default_root_at_source_time() {
	zxfer_test_capture_subshell "
		unset ZXFER_SOURCE_MODULES_ROOT
		. \"$ZXFER_ROOT/src/zxfer_modules.sh\"
		[ \"\${ZXFER_SOURCE_MODULES_ROOT+set}\" != set ]
	"

	assertEquals "Sourcing the pure loader must not mutate module-root state." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_canonical_module_name_validator_rejects_unknown_name() {
	set +e
	zxfer_is_source_module_name not-a-module.sh
	status=$?

	assertEquals "The canonical manifest validator should reject unknown module names." \
		1 "$status"
}

test_canonical_module_loader_rejects_unknown_boundary() {
	set +e
	output=$(zxfer_load_modules not-a-module.sh 2>&1)
	status=$?

	assertEquals "The canonical loader should reject an unknown boundary before sourcing." \
		2 "$status"
	assertContains "The canonical loader should identify the invalid boundary." \
		"$output" "unknown source module boundary: not-a-module.sh"
}

test_module_loader_stops_after_a_valid_boundary() {
	zxfer_test_capture_subshell "
		unset -f zxfer_split_begin zxfer_set_failure_stage
		ZXFER_SOURCE_MODULES_ROOT=\"$ZXFER_ROOT\"
		. \"$ZXFER_ROOT/src/zxfer_modules.sh\"
		zxfer_load_modules zxfer_quoting.sh || exit 8
		command -v zxfer_split_begin >/dev/null 2>&1 || exit 9
		command -v zxfer_set_failure_stage >/dev/null 2>&1 && exit 10
		exit 0
	"

	assertEquals "A partial load should source the boundary module and stop before the next one." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_module_loader_rejects_unknown_boundary_before_sourcing_modules() {
	zxfer_test_capture_subshell "
		unset -f zxfer_set_failure_stage
		ZXFER_SOURCE_MODULES_ROOT=\"$ZXFER_ROOT\"
		. \"$ZXFER_ROOT/src/zxfer_modules.sh\"
		zxfer_load_modules not-a-module.sh
		l_status=\$?
		command -v zxfer_set_failure_stage >/dev/null 2>&1 && exit 9
		exit \"\$l_status\"
	"

	assertEquals "An unknown partial-load boundary should fail before any modules are sourced." \
		2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The loader should identify the invalid boundary." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unknown source module boundary: not-a-module.sh"
}

test_module_loader_rejects_multiline_boundary_before_sourcing_modules() {
	zxfer_test_capture_subshell '
		ZXFER_SOURCE_MODULES_ROOT="'"$ZXFER_ROOT"'"
		. "'"$ZXFER_ROOT"'/src/zxfer_modules.sh"
		zxfer_source_module() {
			printf "%s\n" unexpected-source
			return 99
		}
		l_boundary="zxfer_path_security.sh
zxfer_quoting.sh"
		zxfer_load_modules "$l_boundary"
	'

	assertEquals "A multiline boundary spanning adjacent manifest entries must be rejected." \
		2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertNotContains "Invalid boundaries must fail before the loader sources a module." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unexpected-source"
}

test_module_loader_preserves_caller_ifs_and_globbing_state() {
	zxfer_test_capture_subshell "
		ZXFER_SOURCE_MODULES_ROOT=\"$ZXFER_ROOT\"
		IFS=:
		set -f
		. \"$ZXFER_ROOT/src/zxfer_modules.sh\"
		zxfer_load_modules || exit 8
		[ \"\$IFS\" = : ] || exit 9
		case \$- in
		*f*) ;;
		*) exit 10 ;;
		esac
	"

	assertEquals "Loading modules should not alter a caller's IFS or globbing mode." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_module_manifest_covers_every_runtime_source_module_once() {
	expected_modules=$(
		for module_path in "$ZXFER_ROOT"/src/*.sh; do
			printf '%s\n' "${module_path##*/}"
		done |
			sed -e '/^zxfer_modules[.]sh$/d' \
				-e '/^zxfer_cleanup_child_wrapper[.]sh$/d' |
			sort
	)
	actual_modules=$(printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" | sort)

	assertEquals "The canonical manifest should list every sourceable runtime module exactly once." \
		"$expected_modules" "$actual_modules"
}

test_launcher_invalid_secure_path_preserves_structured_failure_reporting() {
	invalid_path=$(printf '/bin\t/untrusted')
	error_log="$TEST_TMPDIR/invalid-secure-path.log"
	rm -f "$error_log"

	set +e
	output=$(
		ZXFER_SECURE_PATH="$invalid_path" \
			ZXFER_ERROR_LOG="$error_log" \
			"$ZXFER_ROOT/zxfer" backup/dst 2>&1
	)
	status=$?

	assertEquals "Invalid secure-PATH configuration should preserve the dependency failure status." \
		1 "$status"
	assertContains "Invalid secure-PATH configuration should still emit the structured failure envelope." \
		"$output" "zxfer: failure report begin"
	assertContains "Invalid secure-PATH configuration should retain dependency classification." \
		"$output" "failure_class: dependency"
	assertContains "Invalid secure-PATH configuration should retain its validation stage." \
		"$output" "failure_stage: secure PATH validation"
	assertTrue "Invalid secure-PATH configuration should still mirror the structured report to ZXFER_ERROR_LOG." \
		"[ -f '$error_log' ]"
	assertContains "The mirrored startup failure should retain dependency classification." \
		"$(cat "$error_log" 2>/dev/null || :)" "failure_class: dependency"
}

test_launcher_finds_modules_when_invoked_without_a_directory_prefix() {
	fixture_dir="$TEST_TMPDIR/launcher-relative"
	rm -rf "$fixture_dir"
	zxfer_test_create_launcher_fixture "$fixture_dir"
	log_path="$fixture_dir/launcher.log"

	zxfer_test_capture_subshell "
		cd \"$fixture_dir\" || exit 97
		ZXFER_TEST_LOG=\"$log_path\" sh zxfer backup/dst || exit
		ZXFER_TEST_LOG=\"$log_path\" ./zxfer backup/dst
	"

	assertEquals "The launcher should load ./src when \$0 has no directory part or a ./ prefix." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Both relative invocations should reach zxfer_main with their arguments." \
		2 "$(grep -c '^main backup/dst ' "$log_path")"
	assertEquals "Relative invocations should not print errors." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_launcher_prescans_argv_for_very_verbose_before_main() {
	fixture_dir="$TEST_TMPDIR/launcher-profile-prescan"
	rm -rf "$fixture_dir"
	zxfer_test_create_launcher_fixture "$fixture_dir"
	log_path="$fixture_dir/launcher.log"

	zxfer_test_capture_subshell "
		export ZXFER_TEST_LOG=\"$log_path\"
		g_zxfer_profile_prescan=1 \"$fixture_dir/zxfer\" backup/dst || exit
		\"$fixture_dir/zxfer\" -vV backup/dst || exit
		\"$fixture_dir/zxfer\" -R tank/src -V backup/dst || exit
		\"$fixture_dir/zxfer\" -O userV@host backup/dst
	"

	assertEquals "The fixture launcher runs should succeed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "The prescan should always assign 0 or 1, overriding an inherited value, and find V in any short-option cluster." \
		"prescan=<0>
prescan=<1>
prescan=<1>
prescan=<0>" "$(sed 's/^.* prescan=/prescan=/' "$log_path")"
}

test_launcher_renders_the_unsafe_invocation_with_every_argument_escaped() {
	invalid_path=$(printf '/bin\t/untrusted')
	stderr_file="$TEST_TMPDIR/launcher-invocation.stderr"
	tab=$(printf '\t')
	esc=$(printf '\033')

	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 ZXFER_SECURE_PATH="$invalid_path" \
		"$ZXFER_ROOT/zxfer" -R "tank/it's" 'dq"x' "t${tab}ab" "nl
x" "e${esc}[31m" 'back\slash' >/dev/null 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/it'\"'\"'s' 'dq\"x' 't\\tab' 'nl\\nx' 'e\\x1B[31m' 'back\\\\slash'" \
		"$stderr_file" >/dev/null 2>&1
	invocation_status=$?

	assertEquals "The forced startup failure should keep its dependency status." 1 "$status"
	assertEquals "The unsafe invocation should quote and escape every argument." \
		0 "$invocation_status"
}

test_launcher_never_runs_an_inherited_awk_command() {
	invalid_path=$(printf '/bin\t/untrusted')
	fake_awk="$TEST_TMPDIR/inherited-awk"
	marker="$TEST_TMPDIR/inherited-awk.ran"
	stderr_file="$TEST_TMPDIR/launcher-inherited-awk.stderr"
	rm -f "$marker"
	cat >"$fake_awk" <<EOF
#!/bin/sh
: >"$marker"
EOF
	chmod 700 "$fake_awk"

	g_cmd_awk=$fake_awk ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		ZXFER_SECURE_PATH="$invalid_path" \
		"$ZXFER_ROOT/zxfer" -R "$(printf 'tank\001src')" >/dev/null 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank\\x01src'" "$stderr_file" >/dev/null 2>&1
	invocation_status=$?

	assertEquals "The forced startup failure should keep its dependency status." 1 "$status"
	assertFalse "An exported g_cmd_awk must never be executed." "[ -e '$marker' ]"
	assertEquals "The invocation should still be escaped by the awk on the pinned PATH." \
		0 "$invocation_status"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
