#!/bin/sh
# The unit fixture of tests/test_zxfer_remote_hosts.sh, shared with the
# fragments that were written for it and now live in their own module's home:
# a private default temp root that zxfer_list_default_tmpdir_candidates is
# stubbed to offer, an env-driven ssh stand-in in FAKE_SSH_BIN, fixed zfs and
# ssh command globals, and a genuine run root per case. An entry whose own
# cases need another fixture applies this one to those fragments only,
# through zxfer_test_running_test_is_in in its setUp. (The integration and
# performance harnesses use tests/helpers/zxfer_remote_fixtures.sh instead.)
# shellcheck disable=SC2034,SC2317,SC2329

# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

# Purpose: Publish a mocked capability response through the same parsed-result
# channel that production ensure calls guarantee to their OS and tool
# consumers.
# Usage: zxfer_test_accept_remote_capability_response RESPONSE
zxfer_test_accept_remote_capability_response() {
	l_test_capability_response=$1
	g_zxfer_remote_capability_response_result=$l_test_capability_response
	zxfer_parse_remote_capability_response "$l_test_capability_response" || return 1
	printf '%s\n' "$l_test_capability_response"
}

# Purpose: Create the private default temp root and the ssh stand-in once
# TEST_TMPDIR exists.
# Usage: zxfer_test_remote_host_fixture_one_time_setup, from oneTimeSetUp
# after zxfer_test_create_tmpdir. Set TEST_ORIGINAL_PATH=$PATH at the top of
# the entry before sourcing tests/test_helper.sh.
zxfer_test_remote_host_fixture_one_time_setup() {
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rh.XXXXXX) || {
		echo "Unable to create private remote-host test temp root." >&2
		exit 1
	}
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
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
# Usage: zxfer_test_remote_host_fixture_setup || return, from setUp.
zxfer_test_remote_host_fixture_setup() {
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
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	g_cmd_ssh="$FAKE_SSH_BIN"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the remote-host test run root."
}

# Purpose: Restore the PATH a case may have narrowed.
# Usage: zxfer_test_remote_host_fixture_teardown, from tearDown.
zxfer_test_remote_host_fixture_teardown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}
