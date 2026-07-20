#!/bin/sh
#
# shunit2 tests for the direct integration harness control flow.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	INTEGRATION_HARNESS="$ZXFER_ROOT/tests/run_integration_zxfer.sh"
	INTEGRATION_REGISTRY="$ZXFER_ROOT/tests/integration_test_registry.tsv"
	INTEGRATION_FRAGMENT_MANIFEST="$ZXFER_ROOT/tests/integration_fragment_manifest.tsv"
	INTEGRATION_REGISTRY_HELPER="$ZXFER_ROOT/tests/helpers/integration_test_registry.sh"
	l_integration_load_sentinel=zxfer-integration-source-load-complete
	l_integration_load_status=0
	l_integration_load_output=$(
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1 \
			ZXFER_INTEGRATION_TESTS_DIR="$ZXFER_ROOT/tests" \
			INTEGRATION_HARNESS="$INTEGRATION_HARNESS" \
			/bin/sh -c '
				. "$INTEGRATION_HARNESS" || exit $?
				printf "%s\n" "zxfer-integration-source-load-complete"
			' 2>&1
	) || l_integration_load_status=$?
	if [ "$l_integration_load_status" -ne 0 ] ||
		[ "$l_integration_load_output" != "$l_integration_load_sentinel" ]; then
		printf 'Source-only integration load did not reach its sentinel (status %s): %s\n' \
			"$l_integration_load_status" "$l_integration_load_output" >&2
		return 1
	fi
	zxfer_test_create_tmpdir "zxfer_run_integration"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	unset ZXFER_INTEGRATION_REGISTRY_FILE
	unset ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE
	ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
	ZXFER_INTEGRATION_TESTS_DIR="$TESTS_DIR"
	# shellcheck source=tests/run_integration_zxfer.sh
	. "$INTEGRATION_HARNESS"
	ZXFER_LIST_FAILED_TESTS_ONLY=0
	ZXFER_SKIP_TESTS=""
	ZXFER_ONLY_TESTS=""
	ZXFER_KEEP_GOING=0
	ZXFER_ABORT_REQUESTED=0
	ZXFER_FAILED_TESTS=""
	WORKDIR="$TEST_TMPDIR/workdir"
	rm -rf "$WORKDIR"
	mkdir -p "$WORKDIR"
	WORKDIR=$(cd -P "$WORKDIR" && pwd)
}

tearDown() {
	rm -rf "$WORKDIR"
}

# shellcheck disable=SC2329  # Invoked by assertions through command substitution.
zxfer_test_integration_fragment_corpus() {
	l_test_integration_fragment_files=$(zxfer_integration_fragment_files) || return 1
	while IFS= read -r l_test_integration_fragment_file; do
		[ -n "$l_test_integration_fragment_file" ] || continue
		cat "$l_test_integration_fragment_file" || return $?
	done <<-EOF
		$l_test_integration_fragment_files
	EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_parse_args_accepts_failed_tests_only() {
	parse_args --failed-tests-only

	assertEquals "The integration harness should accept failure-only output mode." \
		"1" "$ZXFER_LIST_FAILED_TESTS_ONLY"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_parse_args_accepts_only_test_lists() {
	parse_args --only-test basic_replication_test,force_rollback_test --only-test usage_error_tests

	assertEquals "The integration harness should accept comma-delimited and repeated --only-test selectors." \
		"basic_replication_test force_rollback_test usage_error_tests" "$ZXFER_ONLY_TESTS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_parse_args_preserves_confirmation_default_and_yes_override() {
	assertEquals "Direct integration runs should confirm each wrapped modifying command by default." \
		1 "$ZXFER_CONFIRM_EACH_COMMAND"

	parse_args --yes

	assertEquals "The explicit --yes flag should remain the only CLI bypass for per-command confirmation." \
		0 "$ZXFER_CONFIRM_EACH_COMMAND"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_help_returns_before_fragment_or_registry_loading() {
	bad_manifest="$TEST_TMPDIR/help-invalid-fragment-manifest.tsv"
	printf '%s\n' '# invalid header' >"$bad_manifest"

	status=0
	output=$(ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$bad_manifest \
		"$INTEGRATION_HARNESS" --help 2>&1) || status=$?

	assertEquals "Help should retain its early zero-status path without loading test fragments." 0 "$status"
	assertContains "Help should retain the integration harness usage synopsis." \
		"$output" "usage: ./tests/run_integration_zxfer.sh"
	assertNotContains "Help should not expose an unrelated fragment validation failure." \
		"$output" "Invalid integration fragment manifest"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_source_only_loading_preserves_caller_options_and_positionals() {
	status=0
	output=$(
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1 \
			ZXFER_INTEGRATION_TESTS_DIR="$ZXFER_ROOT/tests" \
			INTEGRATION_HARNESS="$INTEGRATION_HARNESS" \
			/bin/sh -c '
				set +e
				set +u
				set -f
				set -- "first value" "second*value" ""
				options_before=$-
				. "$INTEGRATION_HARNESS" || exit $?
				options_after=$-
				printf "options_before=%s\n" "$options_before"
				printf "options_after=%s\n" "$options_after"
				printf "argument_count=%s\n" "$#"
				for argument do
					printf "argument=<%s>\n" "$argument"
				done
			' 2>&1
	) || status=$?
	options_before=$(printf '%s\n' "$output" | sed -n 's/^options_before=//p')
	options_after=$(printf '%s\n' "$output" | sed -n 's/^options_after=//p')

	assertEquals "Source-only loading should succeed. Output: $output" 0 "$status"
	assertContains "The caller fixture should begin with globbing disabled." \
		"$options_before" "f"
	assertEquals "Source-only loading must preserve the caller's shell-option flags." \
		"$options_before" "$options_after"
	assertContains "Source-only loading must preserve the caller's positional count." \
		"$output" "argument_count=3"
	assertContains "Source-only loading must preserve positional whitespace." \
		"$output" "argument=<first value>"
	assertContains "Source-only loading must preserve positional glob characters." \
		"$output" "argument=<second*value>"
	assertContains "Source-only loading must preserve an empty trailing positional argument." \
		"$output" "argument=<>"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_build_requested_test_sequence_filters_to_named_tests() {
	TEST_SEQUENCE="usage_error_tests basic_replication_test force_rollback_test"
	ZXFER_ONLY_TESTS="force_rollback_test,usage_error_tests"

	assertEquals "Requested integration tests should preserve the suite's declared order." \
		"usage_error_tests force_rollback_test" "$(build_requested_test_sequence)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_build_requested_test_sequence_accepts_multiline_declared_test_lists() {
	TEST_SEQUENCE="usage_error_tests \
basic_replication_test \
force_rollback_test"
	ZXFER_ONLY_TESTS="force_rollback_test,usage_error_tests"

	assertEquals "Requested integration tests should validate and preserve order when the declared suite list spans multiple lines." \
		"usage_error_tests force_rollback_test" "$(build_requested_test_sequence)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_build_requested_test_sequence_rejects_unknown_test_names() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
		. \"$INTEGRATION_HARNESS\"
		TEST_SEQUENCE='usage_error_tests basic_replication_test'
		ZXFER_ONLY_TESTS='nosuchtest'
		build_requested_test_sequence
	"

	assertEquals "Unknown --only-test names should fail closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The integration harness should identify the unknown requested test." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Unknown integration test requested via --only-test: nosuchtest"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_registry_preserves_exact_existing_test_and_group_order() {
	actual=$(zxfer_integration_registry_names)
	expected='usage_error_tests
usage_error_failure_report_test
usage_error_failure_report_unsafe_commands_test
usage_error_failure_report_control_character_escaping_test
usage_error_failure_report_trailing_newline_preservation_test
basic_replication_test
non_recursive_replication_test
generate_tests_replication
idempotent_replication_test
auto_snapshot_replication_test
auto_snapshot_nonrecursive_test
trailing_slash_destination_test
exclude_filter_test
missing_destination_error_test
invalid_override_property_test
dry_run_replication_test
remote_dry_run_noexec_progress_test
yield_loop_dryrun_iteration_test
force_rollback_test
failure_handling_tests
runtime_failure_report_test
runtime_failure_report_redaction_test
runtime_failure_report_unsafe_commands_test
extended_usage_error_tests
consistency_option_validation_tests
snapshot_deletion_test
snapshot_name_mismatch_deletion_test
snapshot_name_prefix_collision_deletion_test
send_command_dryrun_test
raw_send_replication_test
backup_dir_symlink_guard_test
relative_backup_dir_rejection_test
missing_backup_metadata_error_test
grandfather_protection_test
migration_unmounted_guard_test
property_backup_restore_test
chained_property_backup_provenance_test
remote_property_backup_restore_test
property_creation_with_zvol_test
property_override_and_ignore_test
escaped_comma_override_test
unsupported_property_skip_test
must_create_property_error_test
delete_dest_only_snapshot_test
existing_empty_destination_seed_test
dry_run_deletion_test
progress_wrapper_test
progress_placeholder_passthrough_test
job_limit_enforcement_test
background_receive_ancestry_serialization_test
background_send_failure_test
secure_path_dependency_tests
secure_path_failure_report_test
secure_path_append_resolution_test
error_log_mirror_test
usage_error_log_mirror_test
invalid_error_log_warning_test
error_log_email_example_self_test
remote_migration_guard_tests
local_helper_path_shell_metacharacters_test
garbage_wrapped_host_spec_fails_closed_test
control_socket_path_shell_metacharacters_test
remote_origin_target_uncompressed_test
remote_helper_path_shell_metacharacters_test
remote_capability_control_whitespace_path_falls_back_to_direct_probe_test
target_capability_control_whitespace_path_falls_back_to_direct_probe_test
remote_compression_pipeline_test
target_only_remote_compression_test
remote_csh_origin_snapshot_listing_test
remote_wrapped_host_spec_test
malformed_remote_capability_response_fails_closed_test
malformed_remote_capability_response_falls_back_to_direct_probe_test
malformed_target_capability_response_falls_back_to_direct_probe_test
trap_exit_cleanup_test
missing_parallel_error_test
remote_missing_parallel_origin_test
remote_incompatible_parallel_origin_test
remote_parallel_rendered_failure_origin_test
managed_ssh_policy_test
parallel_jobs_listing_test
migration_service_success_test
migration_service_failure_test
get_os_detection_test
verbose_debug_logging_test
legacy_backup_layout_rejected_test
unsupported_backup_format_version_rejected_test
remote_legacy_backup_layout_rejected_test
insecure_backup_metadata_guard_test
beep_handling_test'

	assertEquals "The registry should preserve all 89 integration tests and groups in their existing order." \
		"$expected" "$actual"
	assertEquals "usage_error_tests should remain the sole pre-pool check and still appear in the main sequence." \
		"usage_error_tests" "$(zxfer_integration_registry_pre_pool_names)"
	group_names=$(awk -F '	' 'NR > 1 && $2 == "group" { print $1 }' "$INTEGRATION_REGISTRY" |
		awk 'BEGIN { separator = "" } { printf "%s%s", separator, $0; separator = " " } END { print "" }')
	assertEquals "Grouped integration functions should remain explicit registry entries." \
		"usage_error_tests generate_tests_replication failure_handling_tests extended_usage_error_tests consistency_option_validation_tests secure_path_dependency_tests remote_migration_guard_tests" \
		"$group_names"
	assertNotContains "The integration registry loader must remain a non-eval data path." \
		"$(cat "$INTEGRATION_REGISTRY_HELPER")" "eval"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_fragment_manifest_preserves_fixed_concern_order_and_complete_registry_coverage() {
	expected_paths='integration/cli_reporting_tests.sh
integration/snapshot_replication_tests.sh
integration/property_backup_tests.sh
integration/remote_security_tests.sh
integration/jobs_platform_tests.sh'
	actual_paths=$(zxfer_integration_fragment_paths)
	definition_rows=$(zxfer_integration_fragment_definition_rows)
	definition_count=$(printf '%s\n' "$definition_rows" | awk 'NF { count++ } END { print count + 0 }')
	unique_definition_count=$(printf '%s\n' "$definition_rows" |
		awk -F '\t' 'NF { names[$1] = 1 } END { for (name in names) count++; print count + 0 }')
	definition_tab=$(printf '\t')
	runner_definition_status=0
	runner_definition_rows=$(awk -v headers_only=1 \
		-f "$ZXFER_ROOT/tests/measure_shell_complexity.awk" "$INTEGRATION_HARNESS") ||
		runner_definition_status=$?
	registered_runner_status=0
	registered_runner_definitions=$(printf '%s\n' "$runner_definition_rows" |
		awk -F "$definition_tab" '
			FILENAME == ARGV[1] {
				if (FNR > 1) registered[$1] = 1
				next
			}
			$2 in registered { print $2 }
		' "$INTEGRATION_REGISTRY" -) || registered_runner_status=$?

	assertEquals "The fragment manifest should retain the fixed concern-loading order." \
		"$expected_paths" "$actual_paths"
	assertEquals "Every one of the 89 registered tests and groups should have one fragment definition." \
		89 "$definition_count"
	assertEquals "Integration function definitions should remain unique across fragments." \
		89 "$unique_definition_count"
	assertEquals "The runner definition scan should complete successfully." \
		0 "$runner_definition_status"
	assertEquals "The registered-definition intersection should complete successfully." \
		0 "$registered_runner_status"
	assertEquals "The composition runner must not define any registered integration behavior body." \
		"" "$registered_runner_definitions"
	assertContains "Source-only loading should publish manifest-listed functions into the current shell." \
		"$(command -v basic_replication_test)" "basic_replication_test"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_shell_function_check_accepts_loaded_function_and_rejects_external_sort() {
	function_path="$TEST_TMPDIR/function-tools"
	function_path_command="$function_path/collision_probe"
	mkdir "$function_path"
	printf '%s\n' '#!/bin/sh' 'exit 0' >"$function_path_command"
	chmod +x "$function_path_command"
	loaded_function_status=0
	zxfer_integration_shell_function_p basic_replication_test || loaded_function_status=$?
	external_sort_lookup_status=0
	external_sort_description=$(LC_ALL=C command -V sort 2>&1) ||
		external_sort_lookup_status=$?
	external_sort_status=0
	zxfer_integration_shell_function_p sort || external_sort_status=$?
	path_collision_status=0
	PATH="$function_path:$PATH" zxfer_integration_shell_function_p collision_probe ||
		path_collision_status=$?

	assertEquals "A manifest-loaded integration function should satisfy the callable contract." \
		0 "$loaded_function_status"
	assertEquals "The callable collision fixture requires an installed external sort command." \
		0 "$external_sort_lookup_status"
	assertNotContains "The sort collision fixture must resolve to an external command." \
		"$external_sort_description" "function"
	assertEquals "An external executable must not satisfy the integration function contract." \
		1 "$external_sort_status"
	assertEquals "The word function in an executable path must not satisfy the shell-function contract." \
		1 "$path_collision_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_fragment_manifest_rejects_schema_duplicates_unsafe_paths_missing_files_and_symlinks() {
	bad_header="$TEST_TMPDIR/integration-fragments-bad-header.tsv"
	duplicate="$TEST_TMPDIR/integration-fragments-duplicate.tsv"
	unsafe_path="$TEST_TMPDIR/integration-fragments-unsafe-path.tsv"
	missing="$TEST_TMPDIR/integration-fragments-missing.tsv"
	fixture_root="$TEST_TMPDIR/integration-fragment-symlink-root"
	symlink_manifest="$fixture_root/manifest.tsv"
	parent_symlink_root="$TEST_TMPDIR/integration-fragment-parent-symlink-root"
	parent_symlink_target="$TEST_TMPDIR/integration-fragment-parent-symlink-target"
	parent_symlink_manifest="$parent_symlink_root/manifest.tsv"

	printf '%s\n' "# invalid header" >"$bad_header"
	cp "$INTEGRATION_FRAGMENT_MANIFEST" "$duplicate"
	sed -n '2p' "$INTEGRATION_FRAGMENT_MANIFEST" >>"$duplicate"
	printf '%s\n%s\n' '# path' 'integration/../run_integration_zxfer.sh' >"$unsafe_path"
	printf '%s\n%s\n' '# path' 'integration/missing_tests.sh' >"$missing"
	mkdir -p "$fixture_root/integration"
	ln -s "$INTEGRATION_HARNESS" "$fixture_root/integration/symlink_tests.sh"
	printf '%s\n%s\n' '# path' 'integration/symlink_tests.sh' >"$symlink_manifest"
	mkdir -p "$parent_symlink_root" "$parent_symlink_target"
	printf '%s\n' 'registered_test() {' ':' '}' \
		>"$parent_symlink_target/parent_tests.sh"
	ln -s "$parent_symlink_target" "$parent_symlink_root/integration"
	printf '%s\n%s\n' '# path' 'integration/parent_tests.sh' \
		>"$parent_symlink_manifest"

	bad_header_status=0
	bad_header_output=$(zxfer_validate_integration_fragment_manifest_file "$bad_header" 2>&1) ||
		bad_header_status=$?
	duplicate_status=0
	duplicate_output=$(zxfer_validate_integration_fragment_manifest_file "$duplicate" 2>&1) ||
		duplicate_status=$?
	unsafe_status=0
	unsafe_output=$(zxfer_validate_integration_fragment_manifest_file "$unsafe_path" 2>&1) ||
		unsafe_status=$?
	missing_status=0
	missing_output=$(zxfer_validate_integration_fragment_manifest_file "$missing" 2>&1) ||
		missing_status=$?
	symlink_status=0
	symlink_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			zxfer_validate_integration_fragment_manifest_file "$symlink_manifest"
		) 2>&1
	) || symlink_status=$?
	parent_symlink_status=0
	parent_symlink_output=$(
		(
			INTEGRATION_TESTS_DIR=$parent_symlink_root
			zxfer_validate_integration_fragment_manifest_file \
				"$parent_symlink_manifest"
		) 2>&1
	) || parent_symlink_status=$?

	assertEquals "Changed fragment-manifest schemas should fail closed." 1 "$bad_header_status"
	assertContains "Schema failures should identify the manifest header contract." \
		"$bad_header_output" "header does not match the one-field manifest schema"
	assertEquals "Duplicate fragment paths should fail closed." 1 "$duplicate_status"
	assertContains "Duplicate failures should name the repeated fragment." \
		"$duplicate_output" "duplicates fragment [integration/cli_reporting_tests.sh]"
	assertEquals "Traversal-shaped fragment paths should fail closed." 1 "$unsafe_status"
	assertContains "Unsafe path failures should identify the invalid row." \
		"$unsafe_output" "has an invalid fragment path"
	assertEquals "Missing manifest-listed fragments should fail closed." 1 "$missing_status"
	assertContains "Missing fragment failures should identify the unreadable path." \
		"$missing_output" "fragment [integration/missing_tests.sh] is not readable"
	assertEquals "Symbolic-link fragments should fail closed." 1 "$symlink_status"
	assertContains "Symlink failures should retain the no-indirection contract." \
		"$symlink_output" "must not be a symbolic link"
	assertEquals "A symlinked integration directory must fail closed." \
		1 "$parent_symlink_status"
	assertContains "Parent-symlink failures should identify the directory boundary." \
		"$parent_symlink_output" "integration directory must not be a symbolic link"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_registry_rejects_duplicate_and_unlisted_fragment_definitions() {
	fixture_root="$TEST_TMPDIR/integration-definition-root"
	fixture_registry="$fixture_root/registry.tsv"
	fixture_manifest="$fixture_root/manifest.tsv"
	mkdir -p "$fixture_root/integration"
	cp "$ZXFER_ROOT/tests/measure_shell_complexity.awk" \
		"$fixture_root/measure_shell_complexity.awk"
	printf '# name\tkind\tpre_pool\nregistered_test\ttest\tyes\n' >"$fixture_registry"
	printf '%s\n%s\n%s\n' '# path' \
		'integration/one_tests.sh' 'integration/two_tests.sh' >"$fixture_manifest"
	printf '%s\n' 'registered_test() {' ':' '}' >"$fixture_root/integration/one_tests.sh"
	printf '%s\n' 'registered_test() {' ':' '}' >"$fixture_root/integration/two_tests.sh"

	duplicate_status=0
	duplicate_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || duplicate_status=$?

	printf '%s\n' 'unlisted_test() {' ':' '}' >"$fixture_root/integration/two_tests.sh"
	unlisted_status=0
	unlisted_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || unlisted_status=$?

	assertEquals "Definitions duplicated across fragments should fail closed." 1 "$duplicate_status"
	assertContains "Duplicate-definition failures should name the collision." \
		"$duplicate_output" "function [registered_test] is defined by multiple integration fragments"
	assertEquals "Manifest fragments must not define functions absent from the registry." 1 "$unlisted_status"
	assertContains "Unlisted-definition failures should name the unexpected function." \
		"$unlisted_output" "fragment function [unlisted_test] is not listed in the registry"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_definition_scan_ignores_heredoc_payloads_and_detects_whitespace_variants() {
	fixture_root="$TEST_TMPDIR/integration-definition-syntax-root"
	fixture_registry="$fixture_root/registry.tsv"
	fixture_manifest="$fixture_root/manifest.tsv"
	mkdir -p "$fixture_root/integration"
	cp "$ZXFER_ROOT/tests/measure_shell_complexity.awk" \
		"$fixture_root/measure_shell_complexity.awk"
	printf '# name\tkind\tpre_pool\nregistered_test\ttest\tyes\n' >"$fixture_registry"
	printf '%s\n%s\n' '# path' 'integration/one_tests.sh' >"$fixture_manifest"
	cat >"$fixture_root/integration/one_tests.sh" <<'EOF'
registered_test() {
	cat <<'PAYLOAD'
payload_only_test() {
PAYLOAD
}
EOF

	heredoc_status=0
	heredoc_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || heredoc_status=$?

	printf '%s\n' 'integration/two_tests.sh' >>"$fixture_manifest"
	cat >"$fixture_root/integration/two_tests.sh" <<'EOF'
registered_test ( )
{
	:
}
EOF
	duplicate_status=0
	duplicate_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || duplicate_status=$?

	cat >"$fixture_root/integration/two_tests.sh" <<'EOF'
registered_test\
() {
	:
}
EOF
	continued_status=0
	continued_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || continued_status=$?

	assertEquals "Function-shaped heredoc data must not become an unlisted integration definition. Output: $heredoc_output" \
		0 "$heredoc_status"
	assertEquals "A valid POSIX whitespace variant must not bypass duplicate-definition validation." \
		1 "$duplicate_status"
	assertContains "Whitespace-variant collisions should name the duplicated registered function." \
		"$duplicate_output" "function [registered_test] is defined by multiple integration fragments"
	assertEquals "A backslash-newline function header must not bypass duplicate-definition validation." \
		1 "$continued_status"
	assertContains "Continued-header collisions should name the duplicated registered function." \
		"$continued_output" "function [registered_test] is defined by multiple integration fragments"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_fragments_reject_top_level_execution_and_nested_definitions() {
	fixture_root="$TEST_TMPDIR/integration-definition-only-root"
	fixture_registry="$fixture_root/registry.tsv"
	fixture_manifest="$fixture_root/manifest.tsv"
	fixture_fragment="$fixture_root/integration/one_tests.sh"
	mkdir -p "$fixture_root/integration"
	cp "$ZXFER_ROOT/tests/measure_shell_complexity.awk" \
		"$fixture_root/measure_shell_complexity.awk"
	printf '%s\n%s\n' '# path' 'integration/one_tests.sh' >"$fixture_manifest"
	printf '# name\tkind\tpre_pool\nregistered_test\ttest\tyes\n' >"$fixture_registry"

	cat >"$fixture_fragment" <<'EOF'
registered_test() {
	:
}
exit 0
EOF
	exit_status=0
	exit_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_REGISTRY_FILE=$fixture_registry
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_load_integration_test_fragments
		) 2>&1
	) || exit_status=$?

	cat >"$fixture_fragment" <<'EOF'
registered_test() {
	:
}
FRAGMENT_MUTATION=changed
EOF
	mutation_status=0
	mutation_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_REGISTRY_FILE=$fixture_registry
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_load_integration_test_fragments
		) 2>&1
	) || mutation_status=$?

	printf '# name\tkind\tpre_pool\nsort\ttest\tyes\n' >"$fixture_registry"
	cat >"$fixture_fragment" <<'EOF'
if false; then
	sort() {
		:
	}
fi
EOF
	conditional_status=0
	conditional_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_REGISTRY_FILE=$fixture_registry
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_load_integration_test_fragments
		) 2>&1
	) || conditional_status=$?

	printf '# name\tkind\tpre_pool\nouter_test\ttest\tyes\nnested_test\ttest\tno\n' \
		>"$fixture_registry"
	cat >"$fixture_fragment" <<'EOF'
outer_test() {
	nested_test() {
		:
	}
}
EOF
	nested_status=0
	nested_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			ZXFER_INTEGRATION_REGISTRY_FILE=$fixture_registry
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$fixture_manifest
			zxfer_load_integration_test_fragments
		) 2>&1
	) || nested_status=$?

	assertEquals "Top-level exit must be rejected before a fragment can silently end the caller." \
		1 "$exit_status"
	assertContains "Top-level exit rejection should identify executable fragment code." \
		"$exit_output" "executable top-level shell code"
	assertEquals "Top-level state mutation must be rejected before sourcing." \
		1 "$mutation_status"
	assertContains "State-mutation rejection should identify executable fragment code." \
		"$mutation_output" "executable top-level shell code"
	assertEquals "A conditional definition must not let an external executable satisfy the registry." \
		1 "$conditional_status"
	assertContains "Conditional definitions should fail the definition-only boundary." \
		"$conditional_output" "executable top-level shell code"
	assertEquals "Nested registered definitions must fail closed." 1 "$nested_status"
	assertContains "Nested-definition rejection should identify the structural violation." \
		"$nested_output" "nested function definition"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_harness_declares_remote_parallel_rendered_failure_case() {
	fragment_contents=$(zxfer_test_integration_fragment_corpus)
	registry_contents=$(cat "$INTEGRATION_REGISTRY")

	assertContains "A manifest-listed integration fragment should define the rendered remote parallel failure integration case." \
		"$fragment_contents" "remote_parallel_rendered_failure_origin_test()"
	assertContains "The integration harness should keep the rendered remote parallel failure case in the declared test sequence." \
		"$registry_contents" "remote_parallel_rendered_failure_origin_test"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_get_os_detection_probe_loads_the_remote_host_owner_module() {
	status=0
	output=$(
		(
			cd "$ZXFER_ROOT" || exit 1
			get_os_detection_test
		) 2>&1
	) || status=$?

	assertEquals "The host-safe OS probe should load zxfer_get_os from its owning module. Output: $output" \
		0 "$status"
	assertContains "The OS probe should complete both its local and mock-remote checks." \
		"$output" "Get_os detection test passed"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_registry_rejects_bad_schema_duplicates_and_undefined_functions() {
	bad_header="$TEST_TMPDIR/integration-registry-bad-header.tsv"
	duplicate="$TEST_TMPDIR/integration-registry-duplicate.tsv"
	undefined="$TEST_TMPDIR/integration-registry-undefined.tsv"
	external_collision="$TEST_TMPDIR/integration-registry-external-collision.tsv"
	printf '%s\n' "# invalid header" >"$bad_header"
	cp "$INTEGRATION_REGISTRY" "$duplicate"
	sed -n '2p' "$INTEGRATION_REGISTRY" >>"$duplicate"
	printf '# name\tkind\tpre_pool\nnosuch_integration_function\ttest\tyes\n' >"$undefined"
	printf '# name\tkind\tpre_pool\nsort\ttest\tyes\n' >"$external_collision"

	header_status=0
	header_output=$(zxfer_validate_integration_registry_file "$bad_header" 2>&1) || header_status=$?
	duplicate_status=0
	duplicate_output=$(zxfer_validate_integration_registry_file "$duplicate" 2>&1) || duplicate_status=$?
	undefined_status=0
	undefined_output=$(
		(
			ZXFER_INTEGRATION_REGISTRY_FILE=$undefined
			zxfer_validate_integration_registry
		) 2>&1
	) || undefined_status=$?
	external_collision_status=0
	external_collision_output=$(
		(
			ZXFER_INTEGRATION_REGISTRY_FILE=$external_collision
			zxfer_validate_integration_registry
		) 2>&1
	) || external_collision_status=$?

	assertEquals "Registry files with a changed schema should fail closed." 1 "$header_status"
	assertContains "Schema failures should identify the registry header contract." \
		"$header_output" "header does not match the 3-field registry schema"
	assertEquals "Duplicate integration function names should fail closed." 1 "$duplicate_status"
	assertContains "Duplicate failures should identify the repeated function." \
		"$duplicate_output" "duplicates function [usage_error_tests]"
	assertEquals "Registry entries without a defined harness function should fail closed." 1 "$undefined_status"
	assertContains "Undefined-function failures should identify the missing callable." \
		"$undefined_output" "function [nosuch_integration_function] is not defined"
	assertEquals "An installed executable must not satisfy the registry's function contract." \
		1 "$external_collision_status"
	assertContains "Executable-name collisions should be reported as undefined integration functions." \
		"$external_collision_output" "function [sort] is not defined"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_main_rejects_invalid_registry_before_any_zpool_lookup() {
	bad_registry="$TEST_TMPDIR/integration-registry-main-invalid.tsv"
	tool_lookup_log="$TEST_TMPDIR/tool-lookups-for-invalid-registry"
	printf '%s\n' "# invalid header" >"$bad_registry"
	rm -f "$tool_lookup_log"

	status=0
	output=$(
		(
			ZXFER_INTEGRATION_REGISTRY_FILE=$bad_registry
			require_cmd() {
				printf '%s\n' "$1" >>"$tool_lookup_log"
				exit 97
			}
			main
		) 2>&1
	) || status=$?

	assertEquals "Invalid registries should stop the harness." 1 "$status"
	assertContains "The early failure should report the registry schema error." \
		"$output" "header does not match the 3-field registry schema"
	tool_lookup_status=0
	[ ! -s "$tool_lookup_log" ] || tool_lookup_status=1
	assertEquals "Registry validation must complete before any dependency lookup." \
		0 "$tool_lookup_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_main_rejects_invalid_fragment_manifest_before_any_zpool_lookup() {
	bad_manifest="$TEST_TMPDIR/integration-fragment-main-invalid.tsv"
	tool_lookup_log="$TEST_TMPDIR/tool-lookups-for-invalid-fragment"
	printf '%s\n' "# invalid header" >"$bad_manifest"
	rm -f "$tool_lookup_log"

	status=0
	output=$(
		(
			ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE=$bad_manifest
			require_cmd() {
				printf '%s\n' "$1" >>"$tool_lookup_log"
				exit 97
			}
			main
		) 2>&1
	) || status=$?

	assertEquals "Invalid fragment manifests should stop the harness." 1 "$status"
	assertContains "The early failure should report the fragment manifest schema error." \
		"$output" "header does not match the one-field manifest schema"
	tool_lookup_status=0
	[ ! -s "$tool_lookup_log" ] || tool_lookup_status=1
	assertEquals "Fragment validation must complete before any dependency lookup." \
		0 "$tool_lookup_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_pool_fixture_destroy_guard_refuses_unowned_pool_without_destroying() {
	status=0
	output=$(
		(
			pool_belongs_to_test_run() { return 1; }
			zpool() {
				if [ "$1" = "list" ]; then
					return 0
				fi
				if [ "$1" = "destroy" ]; then
					printf '%s\n' "unexpected destroy"
					return 0
				fi
				return 1
			}
			destroy_test_pool_if_owned source zxfer_src_guard 1 "$WORKDIR/source.img"
		) 2>&1
	) || status=$?

	assertEquals "An ownership mismatch should make pool cleanup fail closed." 1 "$status"
	assertContains "The guard should explain why it refused destruction." \
		"$output" "does not match this test run's safety markers"
	assertNotContains "The guarded path must never reach zpool destroy." \
		"$output" "unexpected destroy"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_pool_fixture_workdir_guard_rejects_traversal_and_symlink_escape() {
	escape_root="$TEST_TMPDIR/outside-workdir"
	mkdir -p "$escape_root"
	ln -s "$escape_root" "$WORKDIR/escape-link"

	traversal_status=0
	is_safe_workdir_path "$WORKDIR/../outside-workdir" || traversal_status=$?
	symlink_status=0
	is_safe_workdir_path "$WORKDIR/escape-link/file" || symlink_status=$?

	assertEquals "Parent traversal should remain outside the removable WORKDIR boundary." 1 "$traversal_status"
	assertEquals "A symlinked parent outside WORKDIR should remain outside the removable boundary." 1 "$symlink_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_harness_does_not_skip_child_property_assertions_on_darwin() {
	fragment_contents=$(zxfer_test_integration_fragment_corpus)

	assertContains "The integration harness should still assert inherited child atime after initial replication." \
		"$fragment_contents" "Expected atime=off on \$dest_child, got \$child_atime."
	assertContains "The integration harness should still assert child atime after an explicit property pass." \
		"$fragment_contents" "Expected atime=off to be set on \$dest_child after property pass."
	assertNotContains "Darwin should not bypass supported child property reconciliation assertions." \
		"$fragment_contents" "Skipping child atime assertion on Darwin"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_find_backup_metadata_file_for_exact_pair_matches_v2_root_headers_and_row() {
	backup_root="$WORKDIR/v2_lookup"
	backup_dir="$backup_root/tank/src"
	backup_file="$backup_dir/.zxfer_backup_info.src.kcurrent"
	mkdir -p "$backup_dir"
	printf '%s\n%s\n%s\n%s\n%s\n' \
		"#zxfer property backup file" \
		"#format_version:2" \
		"#source_root:tank/src" \
		"#destination_root:backup/dst/src" \
		".	compression=lz4=local" >"$backup_file"

	result=$(find_backup_metadata_file_for_exact_pair "$backup_root" "tank/src" "backup/dst/src")
	wrong_destination_result=$(find_backup_metadata_file_for_exact_pair "$backup_root" "tank/src" "backup/dst")

	assertEquals "The integration harness should locate current v2 metadata by source_root, destination_root, and relative root row." \
		"$backup_file" "$result"
	assertEquals "The v2 metadata lookup should not match stale destination roots." \
		"" "$wrong_destination_result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_find_backup_metadata_file_for_exact_pair_ignores_v1_body_rows() {
	backup_root="$WORKDIR/v1_lookup"
	backup_dir="$backup_root/tank/src"
	backup_file="$backup_dir/.zxfer_backup_info.src.klegacy"
	mkdir -p "$backup_dir"
	printf '%s\n%s\n%s\n' \
		"#zxfer property backup file" \
		"#format_version:1" \
		"tank/src,backup/dst/src,compression=lz4=local" >"$backup_file"

	result=$(find_backup_metadata_file_for_exact_pair "$backup_root" "tank/src" "backup/dst/src")

	assertEquals "The integration harness should not locate retired v1 source,destination,properties body rows." \
		"" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_mock_ssh_fixture_matches_controls_across_transport_chunk_boundaries() {
	mock_dir="$WORKDIR/chunked-mock-ssh"
	mock_ssh="$mock_dir/ssh"
	capability_response="$WORKDIR/chunked-capability-response"
	mkdir -p "$mock_dir"
	write_mock_ssh_script "$mock_ssh"
	printf '%s\n' "fixture-capability-response" >"$capability_response"

	prefix=""
	prefix_count=0
	while [ "$prefix_count" -lt 124 ]; do
		prefix=${prefix}x
		prefix_count=$((prefix_count + 1))
	done
	suffix=""
	suffix_count=0
	while [ "$suffix_count" -lt 800 ]; do
		suffix=${suffix}y
		suffix_count=$((suffix_count + 1))
	done

	capability_script="#$prefix ZXFER_REMOTE_CAPS_V2 '$suffix'"
	capability_command=$(
		(
			zxfer_source_runtime_modules_through "zxfer_ssh_transport.sh"
			zxfer_build_remote_sh_c_command "$capability_script"
		)
	)
	capability_output=$(
		MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_response" \
			"$mock_ssh" "fixture.example" "$capability_command"
	)

	assertContains "The fixture regression must exercise the bounded long-script transport." \
		"$capability_command" "for l_part do case"
	assertNotContains "The fixture regression must split the protocol marker across data chunks." \
		"$capability_command" "ZXFER_REMOTE_CAPS_V2"
	assertEquals "Capability-response controls should match when the protocol marker crosses a data-chunk boundary." \
		"fixture-capability-response" "$capability_output"
	wrapped_capability_command="'pfexec' '-u' 'root' $capability_command"
	wrapped_capability_output=$(
		MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_response" \
			"$mock_ssh" "fixture.example" "$wrapped_capability_command"
	)
	assertEquals "Capability-response controls should match chunked commands after wrapper argv." \
		"fixture-capability-response" "$wrapped_capability_output"

	missing_probe_script="#$prefix command -v zfs '$suffix'"
	missing_probe_command=$(
		(
			zxfer_source_runtime_modules_through "zxfer_ssh_transport.sh"
			zxfer_build_remote_sh_c_command "$missing_probe_script"
		)
	)
	if MOCK_SSH_MISSING_TOOL=zfs \
		"$mock_ssh" "fixture.example" "$missing_probe_command" \
		>/dev/null 2>&1; then
		missing_probe_status=0
	else
		missing_probe_status=$?
	fi

	assertNotContains "The fixture regression must split command -v across data chunks." \
		"$missing_probe_command" "command -v zfs"
	assertEquals "Missing-tool controls should retain their synthetic status when command -v crosses a data-chunk boundary." \
		10 "$missing_probe_status"
	wrapped_missing_probe_command="'doas' $missing_probe_command"
	if MOCK_SSH_MISSING_TOOL=zfs \
		"$mock_ssh" "fixture.example" "$wrapped_missing_probe_command" \
		>/dev/null 2>&1; then
		wrapped_missing_probe_status=0
	else
		wrapped_missing_probe_status=$?
	fi
	assertEquals "Missing-tool controls should match chunked commands after wrapper argv." \
		10 "$wrapped_missing_probe_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_run_test_suppresses_passing_output_in_failed_tests_only_mode() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
		. \"$INTEGRATION_HARNESS\"
		ZXFER_LIST_FAILED_TESTS_ONLY=1
		ZXFER_KEEP_GOING=1
		WORKDIR=\"$TEST_TMPDIR/workdir-pass\"
		rm -rf \"\$WORKDIR\"
		mkdir -p \"\$WORKDIR\"
		passing_test() {
			log 'starting synthetic pass'
			printf '%s\n' 'pass-stdout'
			printf '%s\n' 'pass-stderr' >&2
			return 0
		}
		run_test 1 1 passing_test
	"

	assertEquals "Passing tests should still succeed in failure-only mode." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Passing tests should emit the compact completed-status line with the test name in failure-only mode." \
		"[1/1] PASS passing_test" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_run_test_replays_failing_output_in_failed_tests_only_mode() {
	zxfer_test_capture_subshell "
		ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1
		. \"$INTEGRATION_HARNESS\"
		ZXFER_LIST_FAILED_TESTS_ONLY=1
		ZXFER_KEEP_GOING=1
		WORKDIR=\"$TEST_TMPDIR/workdir-fail\"
		rm -rf \"\$WORKDIR\"
		mkdir -p \"\$WORKDIR\"
		failing_test() {
			log 'starting synthetic failure'
			printf '%s\n' 'fail-stdout'
			printf '%s\n' 'fail-stderr' >&2
			return 7
		}
		run_test 2 3 failing_test
		printf 'failed=%s\n' \"\$ZXFER_FAILED_TESTS\"
	"

	assertEquals "Failure-only mode should still let keep-going runs return success from run_test itself." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Failure-only mode should still identify the failing test function." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "[2/3] FAIL"
	assertContains "Failure-only mode should label the replayed stdout block with the failing test name." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--- failing_test stdout ---"
	assertContains "Failure-only mode should replay captured stdout for failing tests." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "fail-stdout"
	assertContains "Failure-only mode should label the replayed stderr block with the failing test name." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--- failing_test stderr ---"
	assertContains "Failure-only mode should replay captured stderr for failing tests." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "fail-stderr"
	assertContains "Failure-only mode should still append the failing test to the summary state." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "failed=failing_test"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
