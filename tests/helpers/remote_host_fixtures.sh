#!/bin/sh
# Shared unit fixture for remote-host capability, transport, session, and
# backup-path tests: a private default temp root offered by
# zxfer_list_default_tmpdir_candidates, a once-created env-driven ssh stand-in,
# fixed zfs/ssh command globals, and a genuine run root per case.
# shellcheck disable=SC2034,SC2317,SC2329

# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

# Purpose: Create the private default temp root and the ssh stand-in once
# TEST_TMPDIR exists.
# Usage: zxfer_test_remote_host_fixture_one_time_setup, from oneTimeSetUp
# after zxfer_test_create_tmpdir. Set TEST_ORIGINAL_PATH=$PATH at the top of
# the suite before sourcing tests/test_helper.sh.
zxfer_test_remote_host_fixture_one_time_setup() {
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rh.XXXXXX) || {
		echo "Unable to create private remote-host test temp root." >&2
		exit 1
	}
	mkdir -p "$TEST_TMPDIR/.remote_tools"
	FAKE_SSH_BIN="$TEST_TMPDIR/.remote_tools/fake_ssh"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
}

# Purpose: Remove the private default temp root.
# Usage: zxfer_test_remote_host_fixture_one_time_teardown, from
# oneTimeTearDown before zxfer_test_cleanup_tmpdir.
zxfer_test_remote_host_fixture_one_time_teardown() {
	rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"
}

# Purpose: Reset owner state and the ssh environment, stub the default temp
# candidates, and allocate a run root.
# Usage: zxfer_test_remote_host_fixture_setup || return, from setUp or a test.
zxfer_test_remote_host_fixture_setup() {
	# Some cases end with set -e; clear it so case order cannot matter.
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
	FAKE_SSH_BIN="$TEST_TMPDIR/.remote_tools/fake_ssh"
	g_cmd_ssh="$FAKE_SSH_BIN"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the remote-host test run root."
}

# Purpose: Restore the PATH a case may have narrowed.
# Usage: zxfer_test_remote_host_fixture_teardown, from tearDown.
zxfer_test_remote_host_fixture_teardown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}
