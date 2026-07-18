#!/bin/sh
#
# Stable shunit2 entry point for property state, policy, reconciliation, and
# transfer behavior. Test definitions live in ordered behavior fragments below.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

zxfer_property_test_report_globbing_state() {
	l_globbing_label=$1
	case $- in
	*f*) printf '%s_globbing=disabled\n' "$l_globbing_label" ;;
	*) printf '%s_globbing=enabled\n' "$l_globbing_label" ;;
	esac
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_transfer_props"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

zxfer_property_test_reset_option_state() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_option_I_ignore_properties=""
	g_destination_operating_system=""
	g_source_operating_system=""
	ZXFER_BASE_READONLY_PROPERTIES="readonly,mountpoint"
	ZXFER_FREEBSD_READONLY_PROPERTIES="aclmode"
}

zxfer_property_test_reset_command_context() {
	g_RZFS="/sbin/zfs"
	g_LZFS="/sbin/zfs"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	g_backup_file_contents=""
	g_restored_backup_file_contents=""
	g_ensure_writable=0
	g_dest_seed_requires_property_reconcile=0
	g_destination="backup/dst"
	g_option_T_target_host=""
	g_target_cmd_zfs=""
	g_cmd_ssh=$(command -v ssh 2>/dev/null || printf '%s\n' ssh)
	unset FAKE_REMOTE_PATH
	unset FAKE_SSH_LOG
	unset ZXFER_REMOTE_ZFS_LOG
}

zxfer_property_test_reset_cache_and_profile_state() {
	zxfer_reset_destination_existence_cache
	zxfer_reset_property_iteration_caches
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_profile_parent_destination_property_reads=0
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
	g_zxfer_property_stage_file_read_result=""
	zxfer_reset_failure_context "unit"
}

setUp() {
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	zxfer_property_test_reset_option_state
	zxfer_property_test_reset_command_context
	zxfer_property_test_reset_cache_and_profile_state
}

# Keep fragment markers aligned with source and registrar order so listing,
# named dispatch, and unfiltered shunit2 execution share one 270-test contract.
# zxfer-test-fragment: suites/zxfer_property_state_cache_tests.sh
# shellcheck source=tests/suites/zxfer_property_state_cache_tests.sh
. "$TESTS_DIR/suites/zxfer_property_state_cache_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_policy_tests.sh
# shellcheck source=tests/suites/zxfer_property_policy_tests.sh
. "$TESTS_DIR/suites/zxfer_property_policy_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_reconcile_apply_tests.sh
# shellcheck source=tests/suites/zxfer_property_reconcile_apply_tests.sh
. "$TESTS_DIR/suites/zxfer_property_reconcile_apply_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_transfer_tests.sh
# shellcheck source=tests/suites/zxfer_property_transfer_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_tests.sh"

# Compatibility test-name alias retained outside the default registrar so
# named dispatch and --list-tests preserve the pre-split suite contract without
# executing the same behavior twice during an unfiltered run.
test_zxfer_property_reconcile_state_helpers_cover_current_shell_paths() {
	test_zxfer_property_owner_operations_and_state_helpers_cover_current_shell_paths
}

suite() {
	zxfer_test_add_property_state_cache_tests
	zxfer_test_add_property_policy_tests
	zxfer_test_add_property_reconcile_apply_tests
	zxfer_test_add_property_transfer_tests
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
