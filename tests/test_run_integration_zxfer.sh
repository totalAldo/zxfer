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

# Purpose: Make a tests directory whose helpers are the real ones and whose
# integration/ directory is empty, for fragment-loading fixtures.
# Usage: make_integration_fixture_dir DIR; the caller adds fragments to
# DIR/integration.
# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
make_integration_fixture_dir() {
	rm -rf "$1"
	mkdir -p "$1/integration"
	ln -s "$ZXFER_ROOT/tests/helpers" "$1/helpers"
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
	fixture_dir="$TEST_TMPDIR/help-invalid-fragments"
	make_integration_fixture_dir "$fixture_dir"
	printf '%s\n' ':' >"$fixture_dir/integration/Bad_tests.sh"

	status=0
	output=$(ZXFER_INTEGRATION_TESTS_DIR=$fixture_dir \
		"$INTEGRATION_HARNESS" --help 2>&1) || status=$?

	assertEquals "Help should retain its early zero-status path without loading test fragments." 0 "$status"
	assertContains "Help should retain the integration harness usage synopsis." \
		"$output" "usage: ./tests/run_integration_zxfer.sh"
	assertNotContains "Help should not expose an unrelated fragment validation failure." \
		"$output" "Invalid integration fragments"
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
beep_handling_test
hostile_dataset_names_replication_test
hostile_dataset_names_delete_test
hostile_property_values_test
hostile_property_override_test
hostile_property_backup_restore_test
hostile_property_record_shaped_value_test
hostile_property_dash_name_test'

	assertEquals "The registry should preserve all 96 integration tests and groups in their existing order." \
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
test_integration_fragments_load_in_sorted_order_with_complete_registry_coverage() {
	expected_paths='integration/cli_reporting_tests.sh
integration/hostile_names_tests.sh
integration/jobs_platform_tests.sh
integration/property_backup_tests.sh
integration/remote_security_tests.sh
integration/snapshot_replication_tests.sh'
	actual_paths=$(zxfer_integration_fragment_paths)
	definition_rows=$(zxfer_integration_fragment_definition_rows)
	definition_count=$(printf '%s\n' "$definition_rows" | awk 'NF { count++ } END { print count + 0 }')
	unique_definition_count=$(printf '%s\n' "$definition_rows" |
		awk -F '\t' 'NF { names[$1] = 1 } END { for (name in names) count++; print count + 0 }')
	definition_tab=$(printf '\t')
	runner_definition_status=0
	runner_definition_rows=$(zxfer_scan_integration_fragment headers "$INTEGRATION_HARNESS") ||
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

	assertEquals "Every integration/*_tests.sh fragment should load, in C sort order." \
		"$expected_paths" "$actual_paths"
	assertEquals "Every one of the 96 registered tests and groups should have one fragment definition." \
		96 "$definition_count"
	assertEquals "Integration function definitions should remain unique across fragments." \
		96 "$unique_definition_count"
	assertEquals "The runner definition scan should complete successfully." \
		0 "$runner_definition_status"
	assertEquals "The registered-definition intersection should complete successfully." \
		0 "$registered_runner_status"
	assertEquals "The composition runner must not define any registered integration behavior body." \
		"" "$registered_runner_definitions"
	assertContains "Source-only loading should publish the fragments' functions into the current shell." \
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

	assertEquals "A loaded integration function should satisfy the callable contract." \
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
test_integration_fragments_reject_bad_names_symlinks_and_an_empty_directory() {
	fixture_dir="$TEST_TMPDIR/integration-fragment-checks"

	make_integration_fixture_dir "$fixture_dir"
	printf '%s\n' 'upper_test() {' ':' '}' >"$fixture_dir/integration/Upper_tests.sh"
	bad_name_status=0
	bad_name_output=$(INTEGRATION_TESTS_DIR=$fixture_dir zxfer_integration_fragment_paths 2>&1) ||
		bad_name_status=$?

	make_integration_fixture_dir "$fixture_dir"
	ln -s "$INTEGRATION_HARNESS" "$fixture_dir/integration/symlink_tests.sh"
	symlink_status=0
	symlink_output=$(INTEGRATION_TESTS_DIR=$fixture_dir zxfer_integration_fragment_paths 2>&1) ||
		symlink_status=$?

	make_integration_fixture_dir "$fixture_dir"
	rmdir "$fixture_dir/integration"
	mkdir -p "$TEST_TMPDIR/integration-fragment-target"
	printf '%s\n' 'target_test() {' ':' '}' >"$TEST_TMPDIR/integration-fragment-target/target_tests.sh"
	ln -s "$TEST_TMPDIR/integration-fragment-target" "$fixture_dir/integration"
	dir_symlink_status=0
	dir_symlink_output=$(INTEGRATION_TESTS_DIR=$fixture_dir zxfer_integration_fragment_paths 2>&1) ||
		dir_symlink_status=$?

	make_integration_fixture_dir "$fixture_dir"
	empty_status=0
	empty_output=$(INTEGRATION_TESTS_DIR=$fixture_dir zxfer_integration_fragment_paths 2>&1) ||
		empty_status=$?

	assertEquals "A fragment name outside lower-case name_tests.sh should fail closed." 1 "$bad_name_status"
	assertContains "The name failure should name the fragment." \
		"$bad_name_output" "fragment [Upper_tests.sh] must be named like name_tests.sh in lower case"
	assertEquals "Symbolic-link fragments should fail closed." 1 "$symlink_status"
	assertContains "Symlink failures should retain the no-indirection contract." \
		"$symlink_output" "fragment [symlink_tests.sh] must not be a symbolic link"
	assertEquals "A symlinked integration directory must fail closed." 1 "$dir_symlink_status"
	assertContains "Directory-symlink failures should identify the directory boundary." \
		"$dir_symlink_output" "the directory must not be a symbolic link"
	assertEquals "An integration directory without fragments should fail closed." 1 "$empty_status"
	assertContains "The empty-directory failure should say what is missing." \
		"$empty_output" "no NAME_tests.sh fragment found"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_registry_rejects_duplicate_and_unlisted_fragment_definitions() {
	fixture_root="$TEST_TMPDIR/integration-definition-root"
	fixture_registry="$fixture_root/registry.tsv"
	make_integration_fixture_dir "$fixture_root"
	printf '# name\tkind\tpre_pool\nregistered_test\ttest\tyes\n' >"$fixture_registry"
	printf '%s\n' 'registered_test() {' ':' '}' >"$fixture_root/integration/one_tests.sh"
	printf '%s\n' 'registered_test() {' ':' '}' >"$fixture_root/integration/two_tests.sh"

	duplicate_status=0
	duplicate_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || duplicate_status=$?

	printf '%s\n' 'unlisted_test() {' ':' '}' >"$fixture_root/integration/two_tests.sh"
	unlisted_status=0
	unlisted_output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_root
			zxfer_validate_integration_registry_definitions "$fixture_registry"
		) 2>&1
	) || unlisted_status=$?

	assertEquals "Definitions duplicated across fragments should fail closed." 1 "$duplicate_status"
	assertContains "Duplicate-definition failures should name the collision." \
		"$duplicate_output" "function [registered_test] is defined by multiple integration fragments"
	assertEquals "Fragments must not define functions absent from the registry." 1 "$unlisted_status"
	assertContains "Unlisted-definition failures should name the unexpected function." \
		"$unlisted_output" "fragment function [unlisted_test] is not listed in the registry"
}

# The definition-only scan relies on the shfmt layout, so any other spelling
# of a function header, and function-shaped text inside a body, fails closed.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_definition_scan_rejects_other_header_spellings_and_function_shaped_bodies() {
	fixture_file="$TEST_TMPDIR/integration-definition-syntax.sh"

	printf '%s\n' 'registered_test ( )' '{' '	:' '}' >"$fixture_file"
	spaced_status=0
	spaced_output=$(zxfer_scan_integration_fragment definitions "$fixture_file") ||
		spaced_status=$?

	printf '%s\n' 'registered_test\' '() {' '	:' '}' >"$fixture_file"
	continued_status=0
	continued_output=$(zxfer_scan_integration_fragment definitions "$fixture_file") ||
		continued_status=$?

	cat >"$fixture_file" <<'EOF'
registered_test() {
	cat <<'PAYLOAD'
payload_only_test() {
PAYLOAD
}
EOF
	heredoc_status=0
	heredoc_output=$(zxfer_scan_integration_fragment definitions "$fixture_file") ||
		heredoc_status=$?

	assertEquals "A spaced header with its brace on the next line should fail closed." 1 "$spaced_status"
	assertContains "The spaced header should be reported as top-level code." \
		"$spaced_output" "executable top-level shell code at line 1."
	assertEquals "A backslash-continued header should fail closed." 1 "$continued_status"
	assertContains "The continued header should be reported as top-level code." \
		"$continued_output" "executable top-level shell code at line 1."
	assertEquals "Function-shaped heredoc text should fail closed." 1 "$heredoc_status"
	assertContains "Function-shaped heredoc text should read as a nested definition." \
		"$heredoc_output" "nested function definition at line 3."
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_fragments_reject_top_level_execution_and_nested_definitions() {
	fixture_root="$TEST_TMPDIR/integration-definition-only-root"
	fixture_registry="$fixture_root/registry.tsv"
	make_integration_fixture_dir "$fixture_root"
	fixture_fragment="$fixture_root/integration/one_tests.sh"
	printf '# name\tkind\tpre_pool\nregistered_test\ttest\tyes\n' >"$fixture_registry"

	for fixture_case in exit mutation brace conditional nested; do
		case "$fixture_case" in
		exit) printf '%s\n' 'registered_test() {' '	:' '}' 'exit 0' ;;
		mutation) printf '%s\n' 'registered_test() {' '	:' '}' 'FRAGMENT_MUTATION=changed' ;;
		brace) printf '%s\n' 'registered_test() {' '	:' '} && FRAGMENT_MUTATION=changed' ;;
		conditional) printf '%s\n' 'if false; then' '	registered_test() {' '		:' '	}' 'fi' ;;
		nested) printf '%s\n' 'registered_test() {' '	nested_test() {' '		:' '	}' '}' ;;
		esac >"$fixture_fragment"
		fixture_status=0
		fixture_output=$(
			(
				INTEGRATION_TESTS_DIR=$fixture_root
				ZXFER_INTEGRATION_REGISTRY_FILE=$fixture_registry
				zxfer_load_integration_test_fragments || exit "$?"
				printf '%s\n' "mutation=${FRAGMENT_MUTATION:-unset}"
			) 2>&1
		) || fixture_status=$?
		assertEquals "The $fixture_case fragment must be rejected before sourcing. Output: $fixture_output" \
			1 "$fixture_status"
		assertNotContains "The $fixture_case fragment must never run." \
			"$fixture_output" "mutation=changed"
		case "$fixture_case" in
		brace)
			assertContains "Code after a closing brace should be named." \
				"$fixture_output" "code after a function closing brace at line 3."
			;;
		nested)
			assertContains "A nested definition should be named." \
				"$fixture_output" "nested function definition at line 2."
			;;
		*)
			assertContains "The $fixture_case fragment should be reported as top-level code." \
				"$fixture_output" "executable top-level shell code"
			;;
		esac
	done
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_fragment_scanner_accepts_inner_groups_and_rejects_hidden_definitions() {
	grouped_fragment="$TEST_TMPDIR/scanner-grouped.sh"
	hash_fragment="$TEST_TMPDIR/scanner-hashes.sh"
	subshell_fragment="$TEST_TMPDIR/scanner-subshell-body.sh"
	unterminated_fragment="$TEST_TMPDIR/scanner-unterminated.sh"
	cat >"$grouped_fragment" <<'EOF'
grouped_test() {
	false || {
		:
	}
	while false; do
		:
	done
}
EOF
	cat >"$hash_fragment" <<'EOF'
trim_test() {
	l_value=${1#prefix}; nested_trim() { :; }
}
EOF
	printf 'setUp()\n(\n\t:\n)\n' >"$subshell_fragment"
	printf '%s\n' 'open_test() {' '	:' >"$unterminated_fragment"

	grouped_status=0
	grouped_output=$(zxfer_scan_integration_fragment definitions "$grouped_fragment") ||
		grouped_status=$?
	hash_status=0
	hash_output=$(zxfer_scan_integration_fragment definitions "$hash_fragment") ||
		hash_status=$?
	subshell_status=0
	subshell_output=$(zxfer_scan_integration_fragment definitions "$subshell_fragment") ||
		subshell_status=$?
	unterminated_status=0
	unterminated_output=$(zxfer_scan_integration_fragment definitions "$unterminated_fragment") ||
		unterminated_status=$?

	assertEquals "An indented inner group must not end its function early. Output: $grouped_output" \
		0 "$grouped_status"
	assertEquals "A definition after a parameter trim must fail closed." 1 "$hash_status"
	assertContains "A parameter-trim hash must not start a comment." \
		"$hash_output" "nested function definition at line 2."
	assertEquals "A subshell-bodied function must fail closed." 1 "$subshell_status"
	assertContains "A subshell body should read as top-level code." \
		"$subshell_output" "executable top-level shell code at line 1."
	assertEquals "An unterminated function must fail closed." 1 "$unterminated_status"
	assertContains "The unterminated function should be named by its header line." \
		"$unterminated_output" "unterminated function at line 1."
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_fragment_scanner_is_quiet_under_gnu_awk() {
	gawk_bin=$(command -v gawk 2>/dev/null) || {
		startSkipping
		assertTrue "gawk is unavailable; GNU awk diagnostic regression skipped." true
		endSkipping
		return 0
	}
	heredoc_fragment="$TEST_TMPDIR/scanner-heredoc.sh"
	gawk_stderr="$TEST_TMPDIR/scanner-heredoc.stderr"
	cat >"$heredoc_fragment" <<'EOF'
heredoc_test() {
	cat <<'PAYLOAD'
literal payload
PAYLOAD
}
EOF

	gawk_status=0
	gawk_output=$(
		awk() { "$gawk_bin" "$@"; }
		zxfer_scan_integration_fragment headers "$heredoc_fragment" 2>"$gawk_stderr"
	) || gawk_status=$?

	assertEquals "GNU awk should accept the fragment scanner." 0 "$gawk_status"
	assertEquals "The scanner must not emit GNU awk warnings." \
		"" "$(cat "$gawk_stderr")"
	assertEquals "The header scan should report the one top-level function." \
		"$(printf '%s\theredoc_test\t1' "$heredoc_fragment")" "$gawk_output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_integration_harness_declares_remote_parallel_rendered_failure_case() {
	fragment_contents=$(zxfer_test_integration_fragment_corpus)
	registry_contents=$(cat "$INTEGRATION_REGISTRY")

	assertContains "An integration fragment should define the rendered remote parallel failure integration case." \
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
test_integration_main_rejects_invalid_fragments_before_any_zpool_lookup() {
	fixture_dir="$TEST_TMPDIR/integration-fragment-main-invalid"
	tool_lookup_log="$TEST_TMPDIR/tool-lookups-for-invalid-fragment"
	make_integration_fixture_dir "$fixture_dir"
	printf '%s\n' ':' >"$fixture_dir/integration/Bad_tests.sh"
	rm -f "$tool_lookup_log"

	status=0
	output=$(
		(
			INTEGRATION_TESTS_DIR=$fixture_dir
			ZXFER_INTEGRATION_REGISTRY_FILE=$INTEGRATION_REGISTRY
			require_cmd() {
				printf '%s\n' "$1" >>"$tool_lookup_log"
				exit 97
			}
			main
		) 2>&1
	) || status=$?

	assertEquals "Invalid fragments should stop the harness." 1 "$status"
	assertContains "The early failure should report the fragment name error." \
		"$output" "fragment [Bad_tests.sh] must be named like name_tests.sh in lower case"
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
