#!/bin/sh
#
# Basic shunit2 tests for zxfer_exec.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Exec behavior includes property-backup serialization cases.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"

zxfer_source_runtime_modules_through "zxfer_session.sh"

create_fake_ssh_bin() {
	# Re-create the fake ssh helper after each cleanup so setUp() can freely
	# truncate the temp directory without leaving a stale interpreter. The helper
	# echoes both argv[0] and all arguments so tests can assert the full command
	# line.
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
if [ -n "${FAKE_SSH_STDOUT_OVERRIDE:-}" ]; then
	printf '%s\n' "$FAKE_SSH_STDOUT_OVERRIDE"
	exit "${FAKE_SSH_EXIT_STATUS:-0}"
fi
if [ "${FAKE_SSH_SUPPRESS_STDOUT:-0}" = "1" ]; then
	exit "${FAKE_SSH_EXIT_STATUS:-0}"
fi
printf '%s\n' "$0"
printf '%s\n' "$@"
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

create_fake_ssh_join_exec_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
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
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
/bin/sh -c "$remote_cmd"
EOF
	chmod +x "$l_path"
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

create_passthrough_zstd() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	--) shift
		break
		;;
	-*) shift
		;;
	*) break
		;;
	esac
done
cat
EOF
	chmod +x "$l_path"
}

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

fake_remote_capability_response_missing_zfs() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	1	-
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
end
EOF
}

fake_remote_capability_response_missing_parallel() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	0	/remote/bin/cat
end
EOF
}

fake_remote_capability_response_relative_zfs() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
end
EOF
}

find_trusted_root_symlink_for_tests() {
	for l_candidate in /tmp /bin /sbin /lib /lib64 /home /var/run /var/lock /*; do
		[ -L "$l_candidate" ] || [ -h "$l_candidate" ] || continue
		if zxfer_is_trusted_symlink_path_component "$l_candidate" >/dev/null 2>&1; then
			printf '%s\n' "$l_candidate"
			return 0
		fi
	done

	return 1
}

require_trusted_root_symlink_for_tests() {
	trusted_root_symlink=$(find_trusted_root_symlink_for_tests) || {
		startSkipping
		return 1
	}

	return 0
}

create_fake_parallel_exec_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
if [ "$1" = "--version" ] ||
	{ [ "$1" = "--will-cite" ] && [ "$2" = "--version" ]; }; then
	printf '%s\n' "GNU parallel (fake)"
	exit 0
fi

while [ $# -gt 0 ]; do
	case "$1" in
	--will-cite)
		shift
		;;
	-j)
		shift 2
		;;
	--line-buffer)
		shift
		;;
	--)
		shift
		break
		;;
	*)
		break
		;;
	esac
done

l_template=$1
[ -n "$l_template" ] || exit 1
shift

while IFS= read -r l_item || [ -n "$l_item" ]; do
	l_cmd=$(printf '%s\n' "$l_template" | sed "s|{}|$l_item|g")
	sh -c "$l_cmd" || exit $?
done
EOF
	chmod +x "$l_path"
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
	create_fake_ssh_bin
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

zxfer_exec_test_reset_option_and_temp_state() {
	# A prior case may have allocated the genuine per-run root. Retire that
	# ownership before the disposable suite directory is cleared so later
	# allocators never inherit a path that the fixture just removed.
	if [ -n "${g_zxfer_run_tmp_root:-}" ] &&
		zxfer_run_tmp_root_has_safe_owned_shape "$g_zxfer_run_tmp_root"; then
		zxfer_reset_runtime_artifact_state >/dev/null 2>&1 ||
			zxfer_discard_runtime_cleanup_state
	else
		zxfer_discard_runtime_cleanup_state
	fi
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_j_jobs=1
	g_option_O_origin_host=""
	g_option_O_origin_host_safe=""
	g_option_T_target_host=""
	g_option_T_target_host_safe=""
	g_option_e_restore_property_mode=0
	g_backup_file_contents=""
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store"
	TMPDIR="$TEST_TMPDIR"
	if [ -n "${TEST_TMPDIR:-}" ]; then
		relax_test_tmpdir_permissions
		rm -rf "${TEST_TMPDIR:?}/"*
	fi
}

zxfer_exec_test_reset_environment_and_capabilities() {
	unset FAKE_SSH_LOG
	unset FAKE_SSH_STDOUT_OVERRIDE
	unset FAKE_SSH_SUPPRESS_STDOUT
	unset FAKE_SSH_EXIT_STATUS
	unset ZXFER_ERROR_LOG
	unset ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
	unset ZXFER_SSH_BATCH_MODE
	unset ZXFER_SSH_STRICT_HOST_KEY_CHECKING
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	unset ZXFER_SSH_USE_AMBIENT_CONFIG
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	PATH=$TEST_ORIGINAL_PATH
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
}

zxfer_exec_test_reset_command_state() {
	create_fake_ssh_bin
	create_fake_parallel_bin "$FAKE_PARALLEL_BIN"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_cat=""
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe="'zstd' '-3'"
	g_cmd_decompress_safe="'zstd' '-d'"
	g_origin_cmd_zfs=""
	g_target_cmd_zfs=""
	g_origin_parallel_cmd=""
	g_origin_cmd_compress_safe=""
	g_origin_cmd_decompress_safe=""
	g_target_cmd_compress_safe=""
	g_target_cmd_decompress_safe=""
	g_LZFS=""
	g_RZFS=""
	g_option_z_compress=0
}

zxfer_exec_test_reset_runtime_state() {
	g_ssh_origin_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	# Owned-lock behavior is covered independently. Keep this broad suite
	# deterministic on restricted hosts where ps cannot inspect the test shell.
	g_zxfer_own_process_start_token="lstart:zxfer exec test"
	zxfer_reset_destination_existence_cache
	g_ssh_target_control_socket=""
	g_zxfer_original_invocation=""
	g_option_Y_yield_iterations=1
	zxfer_reset_cleanup_pid_tracking
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	zxfer_init_temp_artifacts
	zxfer_reset_failure_context "unit"
}

setUp() {
	# Keep fixture phases explicit so individual fragments share one short,
	# deterministic lifecycle under every supported POSIX test shell.
	set +e
	zxfer_exec_test_reset_option_and_temp_state
	zxfer_exec_test_reset_environment_and_capabilities
	zxfer_exec_test_reset_command_state
	zxfer_exec_test_reset_runtime_state
}

tearDown() {
	relax_test_tmpdir_permissions
}

fake_zfs_mountpoint_cmd() {
	if [ "$1" = "get" ]; then
		printf '%s\n' "$FAKE_ZFS_MOUNTPOINT"
		return 0
	fi

	return 1
}

read_backup_file_with_mocked_security() {
	l_path=$1

	(
		zxfer_get_path_owner_uid() { printf '%s\n' "0"; }
		zxfer_get_path_mode_octal() { printf '%s\n' "600"; }
		zxfer_read_local_backup_file "$l_path"
	)
}

fake_property_set_runner() {
	FAKE_SET_CALLS="${FAKE_SET_CALLS}${1}@${2};"
}

fake_property_inherit_runner() {
	FAKE_INHERIT_CALLS="${FAKE_INHERIT_CALLS}${1}@${2};"
}

property_set_logger() {
	[ -n "${PROPERTY_LOG:-}" ] || return 1
	printf 'set %s %s\n' "$1" "$2" >>"$PROPERTY_LOG"
}

property_inherit_logger() {
	[ -n "${PROPERTY_LOG:-}" ] || return 1
	printf 'inherit %s %s\n' "$1" "$2" >>"$PROPERTY_LOG"
}

sort_property_list() {
	l_list=$1
	echo "$l_list" | tr ',' '\n' | sort | tr '\n' ',' | sed 's/,$//'
}

# Behavior-focused fragments keep this stable suite entry point while
# bounding the amount of test code a contributor must load at once.
# zxfer-test-fragment: suites/zxfer_exec_core_tests.sh
# shellcheck source=tests/suites/zxfer_exec_core_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_core_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_replication_tests.sh
# shellcheck source=tests/suites/zxfer_exec_replication_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_replication_tests.sh"
# zxfer-test-fragment: suites/zxfer_exec_reporting_transport_tests.sh
# shellcheck source=tests/suites/zxfer_exec_reporting_transport_tests.sh
. "$TESTS_DIR/suites/zxfer_exec_reporting_transport_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/suites/zxfer_exec_core_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_replication_tests.sh" \
		"$TESTS_DIR/suites/zxfer_exec_reporting_transport_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
