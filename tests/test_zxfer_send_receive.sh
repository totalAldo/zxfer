#!/bin/sh
#
# shunit2 tests for zxfer_send_receive.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_reconcile.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_send_receive"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

zxfer_send_receive_test_reset_option_and_command_state() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_D_display_progress_bar=""
	g_option_w_raw_send=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_z_compress=0
	g_option_j_jobs=1
	g_option_F_force_rollback=""
	g_cmd_zfs="/sbin/zfs"
	g_cmd_compress_safe="gzip"
	g_cmd_decompress_safe="gunzip"
	g_origin_cmd_compress_safe="remote-gzip"
	g_origin_cmd_decompress_safe="remote-gunzip"
	g_target_cmd_compress_safe="target-gzip"
	g_target_cmd_decompress_safe="target-gunzip"
	g_cmd_ps=${g_cmd_ps:-$(command -v ps 2>/dev/null || printf '%s\n' ps)}
}

zxfer_send_receive_test_reset_job_state() {
	g_zfs_send_job_pids=""
	g_zfs_send_job_supervisor_records=""
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_unavailable=0
	g_zfs_send_job_queue_path=""
	g_zfs_send_job_queue_dir=""
	g_zfs_send_job_queue_writer_open=0
	g_zxfer_send_job_record_runner_pid=""
	g_zxfer_send_job_record_source_dataset=""
	g_zxfer_send_job_record_source_snapshot=""
	g_zxfer_send_job_record_dest_dataset=""
	g_zxfer_send_job_record_target_host=""
	g_zxfer_send_job_conflict_dest_dataset=""
	g_count_zfs_send_jobs=0
}

zxfer_send_receive_test_reset_runtime_state() {
	g_is_performed_send_destroy=0
	g_zxfer_failure_last_command=""
	g_zxfer_snapshot_delete_source_identities_file=""
	g_zxfer_snapshot_delete_destination_identities_file=""
	g_zxfer_snapshot_delete_difference_file=""
	g_ssh_origin_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	g_ssh_target_control_socket=""
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_progress_size_estimate_result=""
	g_zxfer_progress_bar_command_result=""
	TMPDIR="$TEST_TMPDIR"
	if [ -n "${g_zxfer_run_tmp_root:-}" ] &&
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_run_tmp_root"; then
		zxfer_reset_runtime_artifact_state || return "$?"
	else
		zxfer_discard_runtime_cleanup_state
	fi
	zxfer_reset_destination_existence_cache
	zxfer_reset_background_job_state
	zxfer_reset_cleanup_pid_tracking
	exec 8<&- 2>/dev/null || true
	exec 9<&- 2>/dev/null || true
	zxfer_reset_failure_context "unit"
}

setUp() {
	set +e
	zxfer_send_receive_test_reset_option_and_command_state
	zxfer_send_receive_test_reset_job_state
	zxfer_send_receive_test_reset_runtime_state
}

# Behavior-focused fragments keep this stable suite entry point while
# bounding the amount of test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_send_receive_command_progress_tests.sh
# shellcheck source=tests/suites/zxfer_send_receive_command_progress_tests.sh
. "$TESTS_DIR/suites/zxfer_send_receive_command_progress_tests.sh"
# zxfer-test-fragment: suites/zxfer_send_receive_supervision_tests.sh
# shellcheck source=tests/suites/zxfer_send_receive_supervision_tests.sh
. "$TESTS_DIR/suites/zxfer_send_receive_supervision_tests.sh"
# zxfer-test-fragment: suites/zxfer_send_receive_pipeline_tests.sh
# shellcheck source=tests/suites/zxfer_send_receive_pipeline_tests.sh
. "$TESTS_DIR/suites/zxfer_send_receive_pipeline_tests.sh"

# Compatibility test-name aliases stay outside the fragment registrar so
# named dispatch and --list-tests retain behavior-equivalent pre-refactor names
# without duplicating them in an unfiltered run. Names that covered the deleted
# status-file/wait implementation dispatch to the equivalent supervisor-owned
# queue, metadata, abort, and reap behavior.
test_zfs_send_receive_drains_rolling_jobs_before_legacy_fallback_when_reopen_fails() {
	test_zfs_send_receive_drains_rolling_jobs_before_batch_fallback_when_reopen_fails
}

test_zxfer_terminate_remaining_send_jobs_preserves_first_abort_failure() {
	test_zxfer_terminate_remaining_send_jobs_preserves_first_defensive_pid_abort_failure
}

test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_to_legacy_waits_on_queue_read_failure() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_to_batch_wait_on_queue_read_failure
}

test_zxfer_wait_for_zfs_send_jobs_dispatches_supervised_and_legacy_queue_paths() {
	test_zxfer_wait_for_zfs_send_jobs_dispatches_rolling_and_batch_supervisor_paths
}

test_wait_for_next_zfs_send_job_completion_falls_back_to_legacy_waits_when_queue_notifications_are_missing() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_when_queue_is_unavailable
}

test_wait_for_next_zfs_send_job_completion_falls_back_when_queue_is_not_open() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_falls_back_when_queue_is_unavailable
}

test_wait_for_next_zfs_send_job_completion_normalizes_nonnumeric_status_write_failures() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_write_failed_notifications_and_abort_failures
}

test_wait_for_next_zfs_send_job_completion_rejects_blank_status_write_notifications() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_rejects_blank_notifications
}

test_wait_for_next_zfs_send_job_completion_reports_completion_write_markers() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_write_failed_notifications_and_abort_failures
}

test_wait_for_next_zfs_send_job_completion_reports_status_read_failures_and_queue_write_markers() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_metadata_failures
}

test_wait_for_next_zfs_send_job_completion_reports_status_write_failures() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_write_failed_notifications_and_abort_failures
}

test_wait_for_next_zfs_send_job_completion_reports_unknown_completed_status_files() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_unknown_job_ids
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_queue_write_and_nonzero_errors() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_metadata_and_failure_markers
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_status_read_errors() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_metadata_and_failure_markers
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_before_status_write_failures() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_write_failed_notifications_and_abort_failures
}

test_wait_for_next_zfs_send_job_completion_surfaces_cleanup_abort_failures_on_parse_and_completion_write_paths() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_write_failed_notifications_and_abort_failures
}

test_wait_for_next_zfs_send_job_completion_uses_wait_status_when_status_file_is_nonnumeric() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_failure_markers_and_nonzero_exits
}

test_wait_for_zfs_send_jobs_legacy_reports_missing_status_file_records() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_collects_ids_and_reports_failures
}

test_zxfer_find_send_job_pid_by_status_file_returns_failure_for_unknown_status_files() {
	test_supervised_send_job_helpers_collect_unregister_and_report_missing_jobs
}

test_zxfer_get_send_job_completion_status_preserves_readback_failures_and_completion_markers() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_metadata_failures
}

test_zxfer_read_send_job_status_file_parses_status_and_failure_marker() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_failure_markers_and_nonzero_exits
}

test_zxfer_read_send_job_status_file_preserves_runtime_readback_failures() {
	test_zxfer_wait_for_next_supervised_zfs_send_job_completion_reports_completion_metadata_failures
}

test_zxfer_terminate_remaining_send_jobs_kills_legacy_jobs_and_cleans_status_files() {
	test_zxfer_terminate_remaining_send_jobs_aborts_supervised_jobs_and_clears_state
}

test_zxfer_unregister_send_job_removes_middle_pid_from_multi_pid_list() {
	test_supervised_send_job_helpers_track_metadata_conflicts_and_render_context
}

test_zxfer_wait_for_zfs_send_jobs_dispatches_to_legacy_helper_when_queue_is_closed() {
	test_zxfer_wait_for_zfs_send_jobs_dispatches_rolling_and_batch_supervisor_paths
}

test_zxfer_wait_for_zfs_send_jobs_legacy_clears_state_and_closes_queue_on_success() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_repairs_destination_state_on_success
}

test_zxfer_wait_for_zfs_send_jobs_legacy_dispatches_to_supervised_batch_and_reports_status_read_failures() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_metadata_and_completion_report_errors
}

test_zxfer_wait_for_zfs_send_jobs_legacy_reports_generic_completion_write_failures() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_metadata_and_completion_report_errors
}

test_zxfer_wait_for_zfs_send_jobs_legacy_reports_queue_write_failure_markers() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_metadata_and_completion_report_errors
}

test_zxfer_wait_for_zfs_send_jobs_legacy_surfaces_cleanup_abort_failures_before_queue_write_and_nonzero_errors() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_failure_markers
}

test_zxfer_wait_for_zfs_send_jobs_legacy_surfaces_cleanup_abort_failures_before_record_and_completion_errors() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_metadata_and_completion_report_errors
}

test_zxfer_wait_for_zfs_send_jobs_legacy_surfaces_cleanup_abort_failures_before_status_read_errors() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_surfaces_cleanup_abort_failures_before_metadata_and_completion_report_errors
}

test_zxfer_wait_for_zfs_send_jobs_legacy_terminates_remaining_jobs_on_failure() {
	test_zxfer_wait_for_supervised_zfs_send_jobs_batch_collects_ids_and_reports_failures
}

test_zxfer_throw_send_job_error_after_cleanup_uses_validated_abort_message() {
	set +e
	output=$(
		(
			g_zxfer_background_job_abort_failure_message="validated cleanup abort failed"
			zxfer_terminate_remaining_send_jobs() {
				return 41
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_throw_supervised_send_job_error_after_cleanup "primary failure message"
		)
	)
	status=$?
	set -e

	assertEquals "Send-job cleanup failures should take precedence and preserve their cleanup status through zxfer_throw_error." \
		41 "$status"
	assertContains "Send-job cleanup failures should preserve the validated supervisor abort failure message." \
		"$output" "validated cleanup abort failed"
}

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/suites/zxfer_send_receive_command_progress_tests.sh" \
		"$TESTS_DIR/suites/zxfer_send_receive_supervision_tests.sh" \
		"$TESTS_DIR/suites/zxfer_send_receive_pipeline_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
