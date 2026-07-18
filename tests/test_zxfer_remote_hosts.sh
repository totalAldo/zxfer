#!/bin/sh
#
# Stable shunit2 entry point for remote capability, dependency, transport,
# backup-path security, and SSH control-socket behavior.
#
# Test definitions live in ordered behavior fragments below. Keep the fragment
# markers aligned with source order so listing and execution preserve the
# 216-test contract.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_session.sh"

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

create_fake_ssh_bin() {
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
if [ -n "${FAKE_SSH_STDOUT:-}" ] && [ -z "${FAKE_SSH_SUPPRESS_STDOUT:-}" ]; then
	printf '%s' "$FAKE_SSH_STDOUT"
fi
if [ -n "${FAKE_SSH_STDERR:-}" ]; then
	printf '%s' "$FAKE_SSH_STDERR" >&2
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

find_csh_shell_for_tests() {
	command -v csh 2>/dev/null || command -v tcsh 2>/dev/null || true
}

create_fake_ssh_join_csh_exec_bin() {
	l_path=$1
	l_csh_shell=$2
	cat >"$l_path" <<EOF
#!/bin/sh
while [ \$# -gt 0 ]; do
	case "\$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=\$1
shift
remote_cmd=""
for arg in "\$@"; do
	if [ "\$remote_cmd" = "" ]; then
		remote_cmd=\$arg
	else
		remote_cmd="\$remote_cmd \$arg"
	fi
done
if [ -n "\${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "\$host" >>"\$FAKE_SSH_LOG"
	printf '%s\n' "\$remote_cmd" >>"\$FAKE_SSH_LOG"
fi
"$l_csh_shell" -fc "\$remote_cmd"
EOF
	chmod +x "$l_path"
}

fake_remote_capability_response() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
end
EOF
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rh.XXXXXX) || {
		echo "Unable to create private remote-host test temp root." >&2
		exit 1
	}
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"
	zxfer_test_cleanup_tmpdir
}

# Reset process environment and temp-root fixtures before each remote-host case.
zxfer_test_reset_remote_host_environment_fixture() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	mkdir -p "$TEST_PRIVATE_DEFAULT_TMPDIR"
	OPTIND=1
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset FAKE_SSH_STDOUT
	unset FAKE_SSH_STDERR
	unset FAKE_SSH_SUPPRESS_STDOUT
	unset ZXFER_BACKUP_DIR
	unset ZXFER_SSH_BATCH_MODE
	unset ZXFER_SSH_STRICT_HOST_KEY_CHECKING
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	unset ZXFER_SSH_USE_AMBIENT_CONFIG
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	TMPDIR="$TEST_TMPDIR"
	zxfer_list_default_tmpdir_candidates() {
		printf '%s\n' "$TEST_PRIVATE_DEFAULT_TMPDIR"
	}
}

# Reset parsed options and primary command paths before each remote-host case.
zxfer_test_reset_remote_host_option_fixture() {
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
	g_option_o_override_property=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_s_make_snapshot=0
	g_option_U_skip_unsupported_properties=0
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
}

# Reset capability and remote-probe scratch state before each remote-host case.
zxfer_test_reset_remote_host_capability_fixture() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_zxfer_remote_capability_response_result=""
	g_zxfer_backup_file_read_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
}

# Reset SSH control-socket and transport memo state before each remote-host case.
zxfer_test_reset_remote_host_transport_fixture() {
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	g_zxfer_ssh_transport_tokens_origin=""
	g_zxfer_ssh_transport_tokens_origin_socket=""
	g_zxfer_ssh_transport_tokens_origin_set=0
	g_zxfer_ssh_transport_tokens_target=""
	g_zxfer_ssh_transport_tokens_target_socket=""
	g_zxfer_ssh_transport_tokens_target_set=0
	g_zxfer_ssh_shell_context_memo_origin_spec=""
	g_zxfer_ssh_shell_context_memo_origin_host=""
	g_zxfer_ssh_shell_context_memo_origin_wrapper=""
	g_zxfer_ssh_shell_context_memo_target_spec=""
	g_zxfer_ssh_shell_context_memo_target_host=""
	g_zxfer_ssh_shell_context_memo_target_wrapper=""
	g_ssh_supports_control_sockets=0
}

# Reset runtime paths and snapshot scratch state before each remote-host case.
zxfer_test_reset_remote_host_runtime_fixture() {
	g_test_max_yield_iterations=8
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_temp_prefix=""
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	zxfer_get_max_yield_iterations() {
		printf '%s\n' "$g_test_max_yield_iterations"
	}
}

setUp() {
	zxfer_test_reset_remote_host_environment_fixture
	zxfer_test_reset_remote_host_option_fixture
	zxfer_test_reset_remote_host_capability_fixture
	zxfer_test_reset_remote_host_transport_fixture
	zxfer_test_reset_remote_host_runtime_fixture
	zxfer_init_temp_artifacts
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the remote-host test run root."
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	if command -v zxfer_reset_owned_lock_tracking >/dev/null 2>&1; then
		zxfer_reset_owned_lock_tracking
	fi
	create_fake_ssh_bin
}

# Behavior-focused fragments keep this stable suite entry point while bounding
# the amount of remote-host test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_remote_hosts_capability_probe_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_capability_probe_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_capability_probe_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_initialization_dependency_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_initialization_dependency_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_initialization_dependency_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_transport_runtime_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_transport_runtime_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_transport_runtime_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_backup_path_security_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_backup_path_security_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_backup_path_security_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_control_socket_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_control_socket_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_control_socket_tests.sh"

# Compatibility test-name alias retained outside the fragment registrar so
# named dispatch and --list-tests preserve the pre-split suite contract without
# executing the replacement behavior twice during an unfiltered run. Full
# session loading now makes the migration-service dependency unconditional.
test_trap_exit_logs_when_relaunch_is_unavailable() {
	test_trap_exit_has_loaded_migration_relaunch_dependency
}

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/suites/zxfer_remote_hosts_capability_probe_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_initialization_dependency_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_transport_runtime_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_backup_path_security_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_control_socket_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
