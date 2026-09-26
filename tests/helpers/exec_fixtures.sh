#!/bin/sh
# The unit fixture of tests/test_zxfer_exec.sh, shared with the fragments that
# were written for it and now live in their own module's home: a TEST_TMPDIR
# emptied before each case, an echo-mode ssh stand-in in FAKE_SSH_BIN, a
# GNU-parallel stand-in in FAKE_PARALLEL_BIN, and fixed zfs, compression,
# backup-root and start-token globals. An entry whose own cases need another
# fixture applies this one to those fragments only, through
# zxfer_test_running_test_is_in in its setUp.
# shellcheck disable=SC2034,SC2317,SC2329

# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

# Purpose: Write a GNU-parallel stand-in that answers --version and exits 0.
# Usage: create_fake_parallel_bin PATH
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

# Purpose: Let a later case remove what an earlier one left read-only.
# Usage: relax_test_tmpdir_permissions
relax_test_tmpdir_permissions() {
	if [ -n "${TEST_TMPDIR:-}" ] && [ -d "$TEST_TMPDIR" ]; then
		chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || true
	fi
}

# Purpose: Record the fixture paths once TEST_TMPDIR exists.
# Usage: zxfer_test_exec_fixture_one_time_setup, from oneTimeSetUp after
# zxfer_test_create_tmpdir.
zxfer_test_exec_fixture_one_time_setup() {
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_ORIGINAL_PATH=${TEST_ORIGINAL_PATH:-$PATH}
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	FAKE_PARALLEL_BIN="$TEST_TMPDIR/fake_parallel"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN" echo
	create_fake_parallel_bin "$FAKE_PARALLEL_BIN"
}

# Purpose: Reset owner state, empty TEST_TMPDIR, and set the fixture globals.
# Usage: zxfer_test_exec_fixture_setup, from setUp.
zxfer_test_exec_fixture_setup() {
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
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN" echo
	create_fake_parallel_bin "$FAKE_PARALLEL_BIN"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_compress_safe="'zstd' '-3'"
	g_cmd_decompress_safe="'zstd' '-d'"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store"
	g_zxfer_original_invocation=""
	# Owned-lock behavior is covered independently. Keep this broad fixture
	# deterministic on restricted hosts where ps cannot inspect the test shell.
	g_zxfer_own_process_start_token="lstart:zxfer exec test"
}
