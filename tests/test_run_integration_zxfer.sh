#!/bin/sh
#
# shunit2 tests for the direct integration harness control flow.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_integration"
	ZXFER_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)
	INTEGRATION_HARNESS="$ZXFER_ROOT/tests/run_integration_zxfer.sh"
	INTEGRATION_REGISTRY="$ZXFER_ROOT/tests/integration_test_registry.tsv"
	INTEGRATION_REGISTRY_HELPER="$ZXFER_ROOT/tests/helpers/integration_test_registry.sh"
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
test_integration_harness_declares_remote_parallel_rendered_failure_case() {
	harness_contents=$(cat "$INTEGRATION_HARNESS")
	registry_contents=$(cat "$INTEGRATION_REGISTRY")

	assertContains "The integration harness should define the rendered remote parallel failure integration case." \
		"$harness_contents" "remote_parallel_rendered_failure_origin_test()"
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
	zpool_marker="$TEST_TMPDIR/zpool-called"
	printf '%s\n' "# invalid header" >"$bad_registry"

	status=0
	output=$(
		(
			ZXFER_INTEGRATION_REGISTRY_FILE=$bad_registry
			zpool() {
				printf '%s\n' "called" >"$zpool_marker"
				return 1
			}
			main
		) 2>&1
	) || status=$?

	assertEquals "Invalid registries should stop the harness." 1 "$status"
	assertContains "The early failure should report the registry schema error." \
		"$output" "header does not match the 3-field registry schema"
	zpool_marker_status=0
	[ ! -e "$zpool_marker" ] || zpool_marker_status=1
	assertEquals "Registry validation must complete before the harness touches zpool." \
		0 "$zpool_marker_status"
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
	harness_contents=$(cat "$INTEGRATION_HARNESS")

	assertContains "The integration harness should still assert inherited child atime after initial replication." \
		"$harness_contents" "Expected atime=off on \$dest_child, got \$child_atime."
	assertContains "The integration harness should still assert child atime after an explicit property pass." \
		"$harness_contents" "Expected atime=off to be set on \$dest_child after property pass."
	assertNotContains "Darwin should not bypass supported child property reconciliation assertions." \
		"$harness_contents" "Skipping child atime assertion on Darwin"
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
