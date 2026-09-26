#!/bin/sh
#
# shunit2 entry point for the tests/suites/zxfer_exec_*_tests.sh fragments,
# with the fixtures they share.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Exec behavior includes property-backup serialization cases.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

create_fake_parallel_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
if [ "$1" = "--version" ]; then
	printf '%s\n' "GNU parallel (fake)"
	exit 0
fi
exit 0
EOF
	chmod +x "$l_path"
}

create_launcher_usage_secure_path() {
	l_secure_path_dir=$1
	l_real_awk=$(command -v awk 2>/dev/null || :)

	mkdir -p "$l_secure_path_dir"

	if [ -z "$l_real_awk" ]; then
		fail "Host test requires awk on the local system PATH."
		return 1
	fi

	ln -s "$l_real_awk" "$l_secure_path_dir/awk"
	cat >"$l_secure_path_dir/ps" <<'EOF'
#!/bin/sh
exit 0
EOF
	cat >"$l_secure_path_dir/zfs" <<'EOF'
#!/bin/sh
exit 0
EOF
	cat >"$l_secure_path_dir/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$l_secure_path_dir/ps" "$l_secure_path_dir/zfs" "$l_secure_path_dir/ssh"
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

zxfer_usage() {
	printf '%s\n' "usage: zxfer"
}

# Some macOS sandboxes report sysconf(_SC_ARG_MAX) failures when invoking
# /usr/bin/xargs without arguments. Provide a shell stub for the shunit2 lookup
# that mirrors the behavior needed by _shunit_extractTestFunctions().
# shellcheck disable=SC2120
xargs() {
	if command [ "$#" -eq 0 ]; then
		tr '\n' ' ' | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//'
	else
		command xargs "$@"
	fi
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_shunit"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_ORIGINAL_PATH=$PATH
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	FAKE_PARALLEL_BIN="$TEST_TMPDIR/fake_parallel"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN" echo
	create_fake_parallel_bin "$FAKE_PARALLEL_BIN"
}

relax_test_tmpdir_permissions() {
	if [ -n "${TEST_TMPDIR:-}" ] && [ -d "$TEST_TMPDIR" ]; then
		chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || true
	fi
}

oneTimeTearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	# Remove a run root an earlier case allocated before the suite directory
	# is emptied, so later allocators never inherit a path removed under them.
	# A root the case left unremovable is simply dropped.
	zxfer_test_reset_all_owner_state >/dev/null 2>&1
	relax_test_tmpdir_permissions
	rm -rf "${TEST_TMPDIR:?}/"*
	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT \
		FAKE_SSH_EXIT_STATUS ZXFER_ERROR_LOG ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS \
		ZXFER_SSH_BATCH_MODE ZXFER_SSH_STRICT_HOST_KEY_CHECKING \
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE ZXFER_SSH_USE_AMBIENT_CONFIG \
		ZXFER_SECURE_PATH ZXFER_SECURE_PATH_APPEND
	PATH=$TEST_ORIGINAL_PATH
	TMPDIR="$TEST_TMPDIR"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN" echo
	create_fake_parallel_bin "$FAKE_PARALLEL_BIN"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_compress_safe="'zstd' '-3'"
	g_cmd_decompress_safe="'zstd' '-d'"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store"
	g_zxfer_original_invocation=""
	# Owned-lock behavior is covered independently. Keep this broad suite
	# deterministic on restricted hosts where ps cannot inspect the test shell.
	g_zxfer_own_process_start_token="lstart:zxfer exec test"
}

tearDown() {
	relax_test_tmpdir_permissions
}

# Each fragment holds the tests of the src modules named in its header. They
# load and run in src/zxfer_modules.sh order.
# zxfer-test-fragment: suites/zxfer_exec_reporting_tests.sh
# shellcheck source=tests/suites/zxfer_exec_reporting_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_reporting_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_command_tests.sh
# shellcheck source=tests/suites/zxfer_exec_command_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_command_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_dependencies_tests.sh
# shellcheck source=tests/suites/zxfer_exec_dependencies_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_dependencies_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_runtime_tests.sh
# shellcheck source=tests/suites/zxfer_exec_runtime_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_runtime_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_ssh_transport_tests.sh
# shellcheck source=tests/suites/zxfer_exec_ssh_transport_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_ssh_transport_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_remote_hosts_tests.sh
# shellcheck source=tests/suites/zxfer_exec_remote_hosts_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_remote_hosts_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_cli_tests.sh
# shellcheck source=tests/suites/zxfer_exec_cli_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_cli_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_snapshot_state_tests.sh
# shellcheck source=tests/suites/zxfer_exec_snapshot_state_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_snapshot_state_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_backup_metadata_tests.sh
# shellcheck source=tests/suites/zxfer_exec_backup_metadata_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_backup_metadata_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_snapshot_producers_tests.sh
# shellcheck source=tests/suites/zxfer_exec_snapshot_producers_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_snapshot_producers_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_send_receive_tests.sh
# shellcheck source=tests/suites/zxfer_exec_send_receive_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_send_receive_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_session_tests.sh
# shellcheck source=tests/suites/zxfer_exec_session_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_session_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_exec.sh" \
		"$TESTS_DIR/suites/zxfer_exec_reporting_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_command_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_dependencies_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_runtime_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_ssh_transport_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_remote_hosts_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_cli_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_snapshot_state_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_backup_metadata_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_snapshot_producers_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_send_receive_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_session_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
