#!/bin/sh
# Shared unit fixture for tests/test_zxfer_exec.sh and command-rendering tests:
# a TEST_TMPDIR emptied before each case, once-created echo-mode ssh and
# GNU-parallel stand-ins under $TEST_TMPDIR/.exec_tools, and fixed zfs,
# compression and backup-root globals.
# shellcheck disable=SC2034,SC2317,SC2329

# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

# Purpose: Let a later case remove what an earlier one left read-only.
# Usage: relax_test_tmpdir_permissions
relax_test_tmpdir_permissions() {
	if [ -n "${TEST_TMPDIR:-}" ] && [ -d "$TEST_TMPDIR" ]; then
		chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || true
	fi
}

# Purpose: Record the fixture paths and write stand-ins once TEST_TMPDIR exists.
# Usage: zxfer_test_exec_fixture_one_time_setup, from oneTimeSetUp after
# zxfer_test_create_tmpdir.
zxfer_test_exec_fixture_one_time_setup() {
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_ORIGINAL_PATH=${TEST_ORIGINAL_PATH:-$PATH}
	mkdir -p "$TEST_TMPDIR/.exec_tools"
	FAKE_SSH_BIN="$TEST_TMPDIR/.exec_tools/fake_ssh"
	FAKE_PARALLEL_BIN="$TEST_TMPDIR/.exec_tools/fake_parallel"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN" echo
	create_parallel_bin "$FAKE_PARALLEL_BIN"
}

# Purpose: Reset owner state, empty non-dot scratch in TEST_TMPDIR, and set
# the fixture globals without rewriting the stand-in tools.
# Usage: zxfer_test_exec_fixture_setup, from setUp or per-case setup.
zxfer_test_exec_fixture_setup() {
	set +e
	# Remove a run root an earlier case allocated before the suite directory
	# is emptied, so later allocators never inherit a path removed under them.
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
	FAKE_SSH_BIN="$TEST_TMPDIR/.exec_tools/fake_ssh"
	FAKE_PARALLEL_BIN="$TEST_TMPDIR/.exec_tools/fake_parallel"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_compress_safe="'zstd' '-3'"
	g_cmd_decompress_safe="'zstd' '-d'"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store"
	g_zxfer_original_invocation=""
}
