#!/bin/sh
#
# shunit2 tests for zxfer_backup_metadata.sh restore/write helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

create_fake_ssh_bin() {
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

zxfer_test_ensure_parent_dir() {
	l_path=$1
	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ] || [ "$l_parent" = "" ]; then
		l_parent=.
	fi
	mkdir -p "$l_parent"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_backup_metadata"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	OPTIND=1
	unset -f zxfer_ensure_local_backup_dir
	unset -f zxfer_ensure_remote_backup_dir
	unset -f zxfer_get_backup_metadata_filename
	unset -f zxfer_get_backup_storage_dir_for_dataset_tree
	unset -f zxfer_invoke_ssh_shell_command_for_host
	unset -f zxfer_read_local_backup_file
	unset -f zxfer_read_remote_backup_file
	unset -f zxfer_resolve_remote_cli_command_safe
	unset -f zxfer_run_destination_zfs_cmd
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_throw_error
	unset -f zxfer_throw_error_with_usage
	unset -f cksum
	unset -f od
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset ZXFER_BACKUP_DIR
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"
	TMPDIR="$TEST_TMPDIR"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_z_compress=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_g_grandfather_protection=""
	g_option_j_jobs=1
	g_option_m_migrate=0
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_ssh_origin_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	g_ssh_target_control_socket=""
	g_ssh_supports_control_sockets=0
	g_zxfer_remote_capability_cache_wait_retries=5
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_backup_storage_root=""
	g_backup_file_extension=""
	g_backup_file_contents=""
	g_pending_backup_file_contents=""
	g_zxfer_backup_metadata_record_list_result=""
	g_zxfer_rendered_backup_metadata_contents=""
	g_zxfer_backup_file_read_result=""
	g_zxfer_backup_restore_candidate_path_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_forwarded_backup_properties=""
	g_restored_backup_file_contents=""
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_destination="backup/dst"
	g_actual_dest="backup/dst"
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	create_fake_ssh_bin
}

# Behavior-focused fragments keep this stable suite entry point while
# bounding the amount of test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_backup_metadata_records_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_records_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_records_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_metadata_restore_tests.sh
# shellcheck source=tests/suites/zxfer_backup_metadata_restore_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_metadata_restore_tests.sh"
# zxfer-test-fragment: suites/zxfer_backup_storage_io_tests.sh
# shellcheck source=tests/suites/zxfer_backup_storage_io_tests.sh
. "$TESTS_DIR/suites/zxfer_backup_storage_io_tests.sh"

# Compatibility alias for focused invocations recorded before runtime became
# the sole owner of recursive artifact cleanup. Keep it outside suite() so the
# default run executes the behavior once, under its current descriptive name.
test_cleanup_backup_metadata_stage_dir_falls_back_to_rm_when_runtime_helpers_are_unavailable() {
	test_create_backup_metadata_stage_dir_for_path_registers_and_unregisters_runtime_cleanup_state
}

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/suites/zxfer_backup_metadata_records_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_metadata_restore_tests.sh" \
		"$TESTS_DIR/suites/zxfer_backup_storage_io_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
