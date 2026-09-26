#!/bin/sh
#
# Stable shunit2 entry point for remote capability, dependency, transport,
# backup-path security, and SSH control-socket behavior.
#
# Test definitions live in the behavior fragments below. Each fragment has a
# "zxfer-test-fragment" marker, a source line and a path in suite(); keep the
# three in the same order so listing and execution agree.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Remote fake executables are loaded only by suites that exercise transport.
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
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

# Publish a mocked capability response through the same parsed-result channel
# that production ensure calls guarantee to their OS and tool consumers.
zxfer_test_accept_remote_capability_response() {
	l_test_capability_response=$1
	g_zxfer_remote_capability_response_result=$l_test_capability_response
	zxfer_parse_remote_capability_response "$l_test_capability_response" || return 1
	printf '%s\n' "$l_test_capability_response"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rh.XXXXXX) || {
		echo "Unable to create private remote-host test temp root." >&2
		exit 1
	}
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
}

oneTimeTearDown() {
	rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"
	zxfer_test_cleanup_tmpdir
}

setUp() {
	# Some cases end with set -e; clear it so fragment order cannot matter.
	set +e
	zxfer_test_reset_all_owner_state || return
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	OPTIND=1
	unset FAKE_SSH_LOG FAKE_SSH_EXIT_STATUS FAKE_SSH_STDOUT FAKE_SSH_STDERR \
		FAKE_SSH_SUPPRESS_STDOUT ZXFER_BACKUP_DIR ZXFER_SSH_BATCH_MODE \
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING ZXFER_SSH_USER_KNOWN_HOSTS_FILE \
		ZXFER_SSH_USE_AMBIENT_CONFIG ZXFER_SECURE_PATH ZXFER_SECURE_PATH_APPEND
	TMPDIR="$TEST_TMPDIR"
	mkdir -p "$TEST_PRIVATE_DEFAULT_TMPDIR"
	zxfer_list_default_tmpdir_candidates() {
		printf '%s\n' "$TEST_PRIVATE_DEFAULT_TMPDIR"
	}
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs
	g_cmd_ssh="$FAKE_SSH_BIN"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the remote-host test run root."
}

# Behavior-focused fragments keep this stable suite entry point while bounding
# the amount of remote-host test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_remote_hosts_capability_probe_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_capability_probe_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_capability_probe_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_dependencies_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_dependencies_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_dependencies_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_cli_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_cli_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_cli_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_session_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_session_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_session_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_tools_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_tools_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_tools_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_transport_runtime_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_transport_runtime_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_transport_runtime_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_backup_path_security_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_backup_path_security_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_backup_path_security_tests.sh"

# zxfer-test-fragment: suites/zxfer_remote_hosts_control_socket_tests.sh
# shellcheck source=tests/suites/zxfer_remote_hosts_control_socket_tests.sh
. "$TESTS_DIR/suites/zxfer_remote_hosts_control_socket_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_remote_hosts.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_capability_probe_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_dependencies_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_cli_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_session_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_tools_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_transport_runtime_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_backup_path_security_tests.sh" \
		"$TESTS_DIR/suites/zxfer_remote_hosts_control_socket_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
