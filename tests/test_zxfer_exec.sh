#!/bin/sh
#
# Basic shunit2 tests for zxfer_exec.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_reconcile.sh"

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
EOF
}

fake_remote_capability_response_missing_zfs() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	1	-
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
EOF
}

fake_remote_capability_response_missing_parallel() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	0	/remote/bin/cat
EOF
}

fake_remote_capability_response_relative_zfs() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
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

setUp() {
	# Reset the global option flags before each test so we always start from a
	# consistent CLI state, and isolate each test to its own temp directory.
	set +e
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
	g_origin_remote_capabilities_dependency_path=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_dependency_path=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
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
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_lease_file=""
	zxfer_reset_destination_existence_cache
	g_ssh_origin_control_socket_dir=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_lease_file=""
	g_ssh_target_control_socket_dir=""
	g_zxfer_original_invocation=""
	g_option_Y_yield_iterations=1
	zxfer_reset_cleanup_pid_tracking
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	zxfer_init_temp_artifacts
	zxfer_reset_failure_context "unit"
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

test_zxfer_compute_secure_path_defaults_to_allowlist() {
	result=$(ZXFER_SECURE_PATH="" ZXFER_SECURE_PATH_APPEND="" zxfer_compute_secure_path)

	assertEquals "Default secure PATH should only include trusted system directories." "$ZXFER_DEFAULT_SECURE_PATH" "$result"
}

test_zxfer_compute_secure_path_filters_relative_entries() {
	result=$(ZXFER_SECURE_PATH="./bin:/tmp/bin:relative:/usr/sbin" ZXFER_SECURE_PATH_APPEND="" zxfer_compute_secure_path)

	assertEquals "Relative path segments must be dropped from the secure PATH." "/tmp/bin:/usr/sbin" "$result"
}

test_zxfer_compute_secure_path_appends_extra_entries() {
	result=$(ZXFER_SECURE_PATH="/sbin:/bin" ZXFER_SECURE_PATH_APPEND=":/opt/zfs/bin:./malicious" zxfer_compute_secure_path)

	assertEquals "ZXFER_SECURE_PATH_APPEND should only add absolute directories to the allowlist." "/sbin:/bin:/opt/zfs/bin" "$result"
}

test_zxfer_compute_secure_path_uses_append_when_default_is_empty() {
	old_default_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	old_secure_path=${ZXFER_SECURE_PATH-}
	old_secure_path_append=${ZXFER_SECURE_PATH_APPEND-}
	outfile="$TEST_TMPDIR/secure_path_append_only.out"
	ZXFER_DEFAULT_SECURE_PATH=""
	ZXFER_SECURE_PATH=""
	ZXFER_SECURE_PATH_APPEND="/opt/trusted/bin"

	zxfer_compute_secure_path >"$outfile"

	ZXFER_DEFAULT_SECURE_PATH=$old_default_secure_path
	ZXFER_SECURE_PATH=$old_secure_path
	ZXFER_SECURE_PATH_APPEND=$old_secure_path_append

	assertEquals "Append-only secure-path configuration should still work when the built-in allowlist is empty." \
		"/opt/trusted/bin" "$(cat "$outfile")"
}

test_zxfer_compute_secure_path_falls_back_to_default_when_all_entries_are_filtered() {
	result=$(ZXFER_SECURE_PATH="relative:.:./bin" ZXFER_SECURE_PATH_APPEND="also-relative:./still-bad" zxfer_compute_secure_path)

	assertEquals "When every configured secure-PATH entry is filtered out, zxfer should fall back to the built-in allowlist." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$result"
}

test_escape_for_single_quotes_escapes_apostrophes() {
	# Single-quoted contexts require reopening the quotes around apostrophes,
	# so ensure the helper inserts the standard '\''' sequence.
	input=$(printf "%s" "needs'single'quotes")
	expected=$(printf "%s" "needs'\\''single'\\''quotes")

	result=$(zxfer_escape_for_single_quotes "$input")

	assertEquals "Input should be properly escaped for single quotes." "$expected" "$result"
}

test_split_host_spec_tokens_handles_multi_word_hosts() {
	# Host specs may append privilege wrappers like "pfexec" or ssh options.
	result=$(zxfer_split_host_spec_tokens "user@host pfexec -p 2222")
	expected=$(printf "%s\n" "user@host" "pfexec" "-p" "2222")

	assertEquals "Host spec should be split into whitespace-delimited tokens." "$expected" "$result"
}

test_split_host_spec_tokens_rejects_shell_quotes_and_backslashes() {
	set +e
	output=$(zxfer_split_host_spec_tokens 'user@host "ZFS Admin"')
	status=$?
	set -e

	assertEquals "Host-spec tokenization should fail closed when the input requires shell quoting semantics." \
		1 "$status"
	assertContains "Rejected host specs should explain the literal-token requirement." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_quote_host_spec_tokens_neutralizes_metacharacters() {
	# Ensure characters such as semicolons are quoted so they cannot escape
	# into new local commands when eval'd later.
	result=$(zxfer_quote_host_spec_tokens "backup.example.com; touch /tmp/pwn")
	expected="'backup.example.com;' 'touch' '/tmp/pwn'"

	assertEquals "Host spec should be rendered as safely quoted tokens." "$expected" "$result"
}

test_quote_cli_tokens_preserves_argument_boundaries() {
	# Compression commands should behave like arrays, preserving each argument.
	result=$(zxfer_quote_cli_tokens "zstd -3 --long=27")
	expected="'zstd' '-3' '--long=27'"

	assertEquals "CLI tokens should be individually quoted." "$expected" "$result"
}

test_split_cli_tokens_rejects_shell_quotes_and_backslashes() {
	set +e
	output=$(zxfer_split_cli_tokens 'zstd -T0\ -3' "compression command")
	status=$?
	set -e

	assertEquals "CLI tokenization should fail closed when the input relies on shell escaping." \
		1 "$status"
	assertContains "Rejected CLI command strings should explain the literal-token requirement." \
		"$output" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_quote_cli_tokens_blocks_shell_metacharacters() {
	# Metacharacters such as ';' or '|' must be neutralized instead of being
	# interpreted as new commands or pipelines.
	result=$(zxfer_quote_cli_tokens "zstd -3; touch /tmp/pwn | cat")
	expected="'zstd' '-3;' 'touch' '/tmp/pwn' '|' 'cat'"

	assertEquals "CLI tokens should remain literal even with metacharacters." "$expected" "$result"
}

test_quote_cli_tokens_preserves_validation_failures() {
	set +e
	output=$(zxfer_quote_cli_tokens '"/opt/zstd dir/zstd" -3' "compression command")
	status=$?
	set -e

	assertEquals "CLI quoting should fail closed when token validation rejects the input." \
		1 "$status"
	assertContains "CLI quoting should preserve the literal-token validation message." \
		"$output" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_split_tokens_on_whitespace_breaks_metacharacters() {
	result=$(zxfer_split_tokens_on_whitespace "cmd;rm -rf|grep foo&echo done")
	expected=$(printf '%s\n' "cmd;" "rm" "-rf|" "grep" "foo&" "echo" "done")

	assertEquals "Tokenizer should break arguments on whitespace while leaving metacharacters literal." "$expected" "$result"
}

test_split_tokens_on_whitespace_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/split_tokens_empty.out"

	zxfer_split_tokens_on_whitespace "" >"$outfile"

	assertEquals "Blank token streams should produce no output." "" "$(cat "$outfile")"
}

test_quote_token_stream_preserves_each_token() {
	tokens=$(printf '%s\n' "alpha" "beta value" "" "gamma")
	result=$(zxfer_quote_token_stream "$tokens")
	expected="'alpha' 'beta value' 'gamma'"

	assertEquals "Token stream quoting should ignore blank lines and wrap each entry." "$expected" "$result"
}

test_quote_token_stream_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_token_stream_empty.out"

	zxfer_quote_token_stream "" >"$outfile"

	assertEquals "Blank token streams should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_host_spec_tokens_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_host_empty.out"

	zxfer_quote_host_spec_tokens "" >"$outfile"

	assertEquals "Blank host specs should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_cli_tokens_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_cli_empty.out"

	zxfer_quote_cli_tokens "" >"$outfile"

	assertEquals "Blank CLI strings should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_host_spec_tokens_returns_empty_when_splitter_yields_no_tokens() {
	zxfer_test_capture_subshell '
		zxfer_split_host_spec_tokens() {
			:
		}
		zxfer_quote_host_spec_tokens "backup.example"
	'

	assertEquals "Host-spec quoting should stay empty when tokenization succeeds but yields no tokens." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Empty host-token streams should not render placeholder quotes." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_quote_cli_tokens_returns_empty_when_splitter_yields_no_tokens() {
	zxfer_test_capture_subshell '
		zxfer_split_cli_tokens() {
			:
		}
		zxfer_quote_cli_tokens "zstd -3"
	'

	assertEquals "CLI quoting should stay empty when tokenization succeeds but yields no tokens." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Empty CLI-token streams should not render placeholder quotes." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_ssh_option_value_rejects_control_whitespace() {
	invalid_value=$(printf 'bad\nvalue')

	set +e
	output=$(zxfer_validate_ssh_option_value "$invalid_value" "ZXFER_SSH_BATCH_MODE")
	status=$?

	assertEquals "SSH transport option values should reject control whitespace." 1 "$status"
	assertContains "Rejected ssh transport option values should explain the single-line requirement." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_validate_ssh_option_path_preserves_single_line_validation_failure() {
	invalid_path=$(printf 'bad\npath')

	set +e
	output=$(zxfer_validate_ssh_option_path "$invalid_path" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE")
	status=$?

	assertEquals "SSH known-hosts path validation should fail closed on control-whitespace input." \
		1 "$status"
	assertContains "SSH known-hosts path validation should preserve the underlying single-line validation message." \
		"$output" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be a single-line non-empty value."
}

test_run_zfs_cmd_for_spec_routes_to_source_runner() {
	# shellcheck disable=SC2030,SC2031
	result=$(
		g_LZFS="/sbin/zfs"
		g_RZFS="/usr/sbin/zfs"
		zxfer_run_source_zfs_cmd() { printf 'source %s %s\n' "$1" "$2"; }
		zxfer_run_destination_zfs_cmd() { printf 'destination %s\n' "$1"; }
		zxfer_run_zfs_cmd_for_spec "/sbin/zfs" list tank/fs
	)

	assertEquals "Spec matching g_LZFS should call zxfer_run_source_zfs_cmd." "source list tank/fs" "$result"
}

test_run_zfs_cmd_for_spec_routes_to_destination_runner() {
	# shellcheck disable=SC2030,SC2031
	result=$(
		g_LZFS="/sbin/zfs"
		g_RZFS="/usr/sbin/zfs"
		zxfer_run_source_zfs_cmd() { printf 'source %s\n' "$1"; }
		zxfer_run_destination_zfs_cmd() { printf 'destination %s %s\n' "$1" "$2"; }
		zxfer_run_zfs_cmd_for_spec "/usr/sbin/zfs" get name tank/dst
	)

	assertEquals "Spec matching g_RZFS should call zxfer_run_destination_zfs_cmd." "destination get name" "$result"
}

test_run_zfs_cmd_for_spec_executes_literal_command_when_not_wrapped() {
	tool="$TEST_TMPDIR/echo_tool"
	cat >"$tool" <<'EOF'
#!/bin/sh
echo "$@"
EOF
	chmod +x "$tool"

	result=$(zxfer_run_zfs_cmd_for_spec "$tool" alpha beta)

	assertEquals "Arbitrary command specs should be executed directly." "alpha beta" "$result"
}

test_run_zfs_cmd_for_spec_tracks_other_profile_counter_when_very_verbose() {
	tool="$TEST_TMPDIR/profile_other_zfs"
	cat >"$tool" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$tool"

	g_option_V_very_verbose=1
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_get_calls=0

	zxfer_run_zfs_cmd_for_spec "$tool" get name tank/other >/dev/null

	assertEquals "Very-verbose profiling should count direct-spec zfs calls in the other bucket." \
		1 "$g_zxfer_profile_other_zfs_calls"
	assertEquals "Very-verbose profiling should still classify the direct-spec verb." \
		1 "$g_zxfer_profile_zfs_get_calls"
}

test_run_source_zfs_cmd_tracks_profile_counters_when_very_verbose() {
	tool="$TEST_TMPDIR/profile_source_zfs"
	cat >"$tool" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$tool"

	g_option_V_very_verbose=1
	g_zxfer_failure_stage="snapshot discovery"
	g_cmd_zfs="$tool"
	g_LZFS="$tool"
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_bucket_source_inspection=0

	zxfer_run_source_zfs_cmd list tank/src >/dev/null

	assertEquals "Very-verbose profiling should count source-side zfs calls." \
		1 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Very-verbose profiling should count list verbs separately." \
		1 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Snapshot discovery source calls should contribute to the source-inspection bucket." \
		1 "$g_zxfer_profile_bucket_source_inspection"
}

test_invoke_ssh_shell_command_for_host_tracks_profile_counters_when_very_verbose() {
	FAKE_SSH_LOG="$TEST_TMPDIR/ssh_profile.log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0

	zxfer_invoke_ssh_shell_command_for_host "origin.example" "'/bin/true'" >/dev/null \
		2>/dev/null

	unset FAKE_SSH_LOG

	assertEquals "Very-verbose profiling should count ssh shell invocations." \
		1 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Very-verbose profiling should attribute origin-host ssh invocations to the source side." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_invoke_ssh_shell_command_for_host_tracks_explicit_profile_side_when_origin_and_target_match() {
	FAKE_SSH_LOG="$TEST_TMPDIR/ssh_profile_same_host.log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0

	zxfer_invoke_ssh_shell_command_for_host "shared.example" "'/bin/true'" source >/dev/null \
		2>/dev/null
	zxfer_invoke_ssh_shell_command_for_host "shared.example" "'/bin/true'" destination >/dev/null \
		2>/dev/null

	unset FAKE_SSH_LOG

	assertEquals "Explicit profile sides should still count total ssh invocations." \
		2 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Explicit source-side attribution should remain correct when origin and target share the same host spec." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
	assertEquals "Explicit destination-side attribution should remain correct when origin and target share the same host spec." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
}

test_zxfer_profile_record_ssh_invocation_tracks_other_and_inferred_sides() {
	g_option_V_very_verbose=1
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0

	zxfer_profile_record_ssh_invocation "wrapper.example" other
	zxfer_profile_record_ssh_invocation "target.example"
	zxfer_profile_record_ssh_invocation "unknown.example"

	assertEquals "Explicit other-side attribution should still count toward total ssh invocations." \
		3 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Inferred destination-side attribution should count target-host ssh invocations." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
	assertEquals "Explicit other-side attribution and unknown hosts should both count toward the other-side ssh bucket." \
		2 "$g_zxfer_profile_other_ssh_shell_invocations"
	assertEquals "Origin-side attribution should remain unchanged when only other and destination paths are exercised." \
		0 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_zxfer_profile_record_zfs_call_tracks_remaining_verbs_and_buckets() {
	g_option_V_very_verbose=1
	g_zxfer_failure_stage=""
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0

	zxfer_profile_record_bucket destination_inspection
	zxfer_profile_record_bucket property_reconciliation

	g_zxfer_failure_stage="property transfer"
	zxfer_profile_record_zfs_call destination send

	g_zxfer_failure_stage="send/receive"
	zxfer_profile_record_zfs_call destination receive
	zxfer_profile_record_zfs_call destination list
	zxfer_profile_record_zfs_call source get

	assertEquals "Destination-side zfs calls should include send, receive, and list verbs." \
		3 "$g_zxfer_profile_destination_zfs_calls"
	assertEquals "Source-side zfs calls should include the source get verb." \
		1 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Send verbs should increment the send counter." \
		1 "$g_zxfer_profile_zfs_send_calls"
	assertEquals "Receive verbs should increment the receive counter." \
		1 "$g_zxfer_profile_zfs_receive_calls"
	assertEquals "List verbs should increment the list counter." \
		1 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Get verbs should increment the get counter." \
		1 "$g_zxfer_profile_zfs_get_calls"
	assertEquals "Destination-inspection bucket accounting should include the direct bucket hit and send/receive destination list probes." \
		2 "$g_zxfer_profile_bucket_destination_inspection"
	assertEquals "Property-reconciliation bucket accounting should include the direct hit and property-transfer send probe." \
		2 "$g_zxfer_profile_bucket_property_reconciliation"
	assertEquals "Send/receive setup bucket accounting should include receive-side send/receive probes." \
		1 "$g_zxfer_profile_bucket_send_receive_setup"
	assertEquals "Source-inspection bucket accounting should include source-side get probes during send/receive setup." \
		1 "$g_zxfer_profile_bucket_source_inspection"
}

test_get_backup_storage_dir_for_dataset_tree_derives_source_relative_layout() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"
	result=$(zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/child")
	expected="$g_backup_storage_root/tank/src/child"
	slash_prefixed_result=$(zxfer_get_backup_storage_dir_for_dataset_tree "/tank/src/child/")

	assertEquals "Backup metadata storage should now derive only from the source dataset tree." \
		"$expected" "$result"
	assertEquals "Backup metadata storage should normalize slash-prefixed dataset inputs without introducing duplicate separators." \
		"$expected" "$slash_prefixed_result"
}

test_get_backup_storage_dir_for_dataset_tree_treats_rootlike_inputs_as_dataset_placeholder_in_current_shell() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"
	root_output="$TEST_TMPDIR/get_backup_storage_dir_root.out"
	blank_output="$TEST_TMPDIR/get_backup_storage_dir_blank.out"

	zxfer_get_backup_storage_dir_for_dataset_tree "/" >"$root_output"
	zxfer_get_backup_storage_dir_for_dataset_tree "" >"$blank_output"

	assertEquals "Rootlike dataset-tree lookups should collapse to the dataset placeholder bucket." \
		"$g_backup_storage_root/dataset" "$(cat "$root_output")"
	assertEquals "Blank dataset-tree lookups should also collapse to the dataset placeholder bucket." \
		"$g_backup_storage_root/dataset" "$(cat "$blank_output")"
}

test_zxfer_backup_metadata_file_key_fails_when_identity_hex_is_empty_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_metadata_file_key_current_shell.out"
	status_file="$TEST_TMPDIR/backup_metadata_file_key_current_shell.status"

	(
		od() {
			:
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
		printf '%s\n' "$?" >"$status_file"
	)

	assertEquals "Backup metadata keys should fail closed when the lossless identity hex cannot be derived." \
		1 "$(cat "$status_file")"
	assertEquals "Failed backup metadata key derivation should not emit a placeholder key." \
		"" "$(cat "$output_file")"
}

test_zxfer_backup_metadata_file_key_uses_identity_hex_output() {
	output_file="$TEST_TMPDIR/backup_metadata_file_key_od_hex.out"

	(
		od() {
			printf ' 61 62 63 64\n'
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
	)

	assertEquals "Backup metadata keys should use the exact od-derived identity hex." \
		"h/61626364" "$(cat "$output_file")"
}

test_zxfer_get_backup_metadata_filename_propagates_key_lookup_failures() {
	zxfer_test_capture_subshell '
		zxfer_backup_metadata_file_key() {
			return 1
		}
		zxfer_get_backup_metadata_filename "tank/src" "backup/dst"
	'

	assertEquals "Backup metadata filename generation should fail when the keyed suffix cannot be computed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed backup metadata filename generation should not emit a partial filename." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_backup_owner_uid_is_allowed_accepts_root_and_effective_uid() {
	result_root=$(
		zxfer_get_effective_user_uid() { printf '%s\n' 1000; }
		if zxfer_backup_owner_uid_is_allowed 0; then echo ok; else echo fail; fi
	)
	assertEquals "Root must always be allowed." "ok" "$result_root"

	result_user=$(
		zxfer_get_effective_user_uid() { printf '%s\n' 4242; }
		if zxfer_backup_owner_uid_is_allowed 4242; then echo ok; else echo fail; fi
	)
	assertEquals "Effective UID should be permitted when matching the owner." "ok" "$result_user"
}

test_describe_expected_backup_owner_includes_effective_uid_when_non_root() {
	result=$(
		zxfer_get_effective_user_uid() { printf '%s\n' 9999; }
		zxfer_describe_expected_backup_owner
	)
	assertEquals "root (UID 0) or UID 9999" "$result"
}

test_require_secure_backup_file_rejects_non_0600_permissions() {
	tmp_file="$TEST_TMPDIR/insecure_backup"
	: >"$tmp_file"
	(
		zxfer_throw_error() {
			printf 'ERROR:%s' "$1"
			exit "${2:-1}"
		}
		zxfer_get_path_owner_uid() { printf '%s\n' 0; }
		zxfer_get_path_mode_octal() { printf '%s\n' 644; }
		zxfer_require_secure_backup_file "$tmp_file"
	) >/dev/null 2>&1
	status=$?
	assertEquals "Insecure permissions should trigger an error." 1 "$status"
}

test_require_secure_backup_file_accepts_secure_metadata() {
	tmp_file="$TEST_TMPDIR/secure_backup"
	: >"$tmp_file"
	(
		zxfer_throw_error() {
			echo "unexpected"
			exit "${2:-1}"
		}
		zxfer_get_path_owner_uid() { printf '%s\n' 0; }
		zxfer_get_path_mode_octal() { printf '%s\n' 600; }
		zxfer_require_secure_backup_file "$tmp_file"
	)
	status=$?
	assertEquals "Secure metadata should pass validation." 0 "$status"
}

test_ensure_local_backup_dir_creates_secure_directory() {
	l_dir=$(cd -P "$TEST_TMPDIR" && pwd)/local_backup
	rm -rf "$l_dir"
	zxfer_ensure_local_backup_dir "$l_dir"
	assertTrue "Secure directory should be created." "[ -d '$l_dir' ]"
	perms=$(stat -c '%a' "$l_dir" 2>/dev/null || stat -f '%Lp' "$l_dir" 2>/dev/null)
	assertEquals "Backup directory must be chmod 700." "700" "$perms"
}

test_invoke_ssh_command_for_host_preserves_argument_boundaries() {
	host_spec="backup@example.com pfexec doas"
	log_file="$TEST_TMPDIR/invoke_cmd.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="$host_spec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"

	zxfer_invoke_ssh_command_for_host "$host_spec" "--" "cmd arg" "with spaces" "umask 077; cat > /tmp/backup"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" "-S" "$TEST_TMPDIR/origin.sock" "backup@example.com" "pfexec" "doas" "--" "cmd arg" "with spaces" "umask 077; cat > /tmp/backup")
	result=$(cat "$log_file")

	assertEquals "ssh helper should keep control-socket, multi-word host specs, and remote commands intact." "$expected" "$result"
}

test_invoke_ssh_command_for_host_emits_very_verbose_remote_prefix() {
	log_file="$TEST_TMPDIR/invoke_cmd_verbose.log"
	stderr_file="$TEST_TMPDIR/invoke_cmd_verbose.err"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"

	zxfer_invoke_ssh_command_for_host "backup@example.com pfexec" /sbin/zfs list -H tank/src \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/origin.sock" "backup@example.com" "pfexec" \
		"/sbin/zfs" "list" "-H" "tank/src")

	assertContains "Very-verbose ssh argv execution should prefix origin-host remote commands." \
		"$(cat "$stderr_file")" "Running remote command [origin: backup@example.com pfexec]:"
	assertContains "Very-verbose ssh argv execution should print the full rendered ssh command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_invoke_ssh_command_for_host_runs_without_remote_args() {
	outfile="$TEST_TMPDIR/invoke_ssh_noargs.out"
	g_cmd_ssh="$FAKE_SSH_BIN"

	zxfer_invoke_ssh_command_for_host "" >"$outfile"

	assertEquals "ssh helpers should still invoke the base command when no host or remote argv is provided." \
		"$FAKE_SSH_BIN
-o
BatchMode=yes
-o
StrictHostKeyChecking=yes" "$(cat "$outfile")"
}

test_get_ssh_base_transport_tokens_preserves_local_ssh_resolution_failures() {
	set +e
	output=$(
		(
			zxfer_get_managed_ssh_option_tokens() {
				printf '%s\n' "-o\nBatchMode=yes"
			}
			zxfer_ensure_local_ssh_command() {
				g_zxfer_resolved_local_ssh_command_result="ssh lookup failed"
				return 1
			}
			zxfer_get_ssh_base_transport_tokens
		)
	)
	status=$?

	assertEquals "SSH base transport token discovery should fail when local ssh resolution fails." \
		1 "$status"
	assertEquals "SSH base transport token discovery should preserve the local ssh resolution diagnostic." \
		"ssh lookup failed" "$output"
}

test_invoke_ssh_command_for_host_includes_explicit_known_hosts_override() {
	log_file="$TEST_TMPDIR/invoke_cmd_known_hosts.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_command_for_host "backup.example" "/bin/true"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' \
		"-o" "BatchMode=yes" \
		"-o" "StrictHostKeyChecking=yes" \
		"-o" "UserKnownHostsFile=$TEST_TMPDIR/known_hosts" \
		"backup.example" "/bin/true")

	assertEquals "ssh invocation helpers should pass the explicit managed known-hosts override through the live argv path." \
		"$expected" "$(cat "$log_file")"
}

test_invoke_ssh_command_for_host_honors_explicit_ambient_policy_opt_out() {
	log_file="$TEST_TMPDIR/invoke_cmd_ambient.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_command_for_host "backup.example" "/bin/true"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' "-S" "$TEST_TMPDIR/origin.sock" "backup.example" "/bin/true")

	assertEquals "Ambient-policy opt-out should suppress zxfer-managed ssh -o options on the live invocation path while preserving control-socket reuse." \
		"$expected" "$(cat "$log_file")"
}

test_get_ssh_cmd_for_host_adds_managed_transport_policy_by_default() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"

	assertEquals "Managed ssh transports should enforce batch mode and strict host-key checks before the control socket." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock'" \
		"$(zxfer_get_ssh_cmd_for_host "origin.example")"
}

test_get_ssh_cmd_for_host_allows_explicit_known_hosts_file_override() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	assertEquals "Managed ssh transports should allow pinning a specific known-hosts file." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-o' 'UserKnownHostsFile=$TEST_TMPDIR/known_hosts'" \
		"$(zxfer_get_ssh_cmd_for_host "backup.example")"
}

test_get_ssh_cmd_for_host_allows_explicit_ambient_policy_opt_out() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1

	assertEquals "Ambient-policy opt-out should suppress zxfer-managed ssh -o transport flags." \
		"'$FAKE_SSH_BIN' '-S' '$TEST_TMPDIR/origin.sock'" \
		"$(zxfer_get_ssh_cmd_for_host "origin.example")"
}

test_get_ssh_cmd_for_host_rejects_relative_known_hosts_override() {
	set +e
	output=$(
		(
			exec 8</dev/null
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_USER_KNOWN_HOSTS_FILE="relative-known-hosts"
			zxfer_get_ssh_cmd_for_host "backup.example"
		)
	)
	status=$?

	assertEquals "Relative known-hosts overrides should fail closed before ssh command rendering." 1 "$status"
	assertContains "Rejected known-hosts overrides should explain the absolute-path requirement." \
		"$output" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
	assertNotContains "Rejected known-hosts overrides should not leak partial ssh argv tokens into the diagnostic." \
		"$output" "$FAKE_SSH_BIN"
	assertNotContains "Rejected known-hosts overrides should not leak the internal managed-policy identity prefix into the diagnostic." \
		"$output" "managed"
}

test_zxfer_get_managed_ssh_option_tokens_rejects_invalid_batch_mode() {
	zxfer_test_capture_subshell "
		ZXFER_SSH_BATCH_MODE=\$(printf 'bad\nmode')
		zxfer_get_managed_ssh_option_tokens
	"

	assertEquals "Managed ssh transport tokens should fail closed when ZXFER_SSH_BATCH_MODE is malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed ZXFER_SSH_BATCH_MODE values should preserve the specific validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_get_managed_ssh_option_tokens_rejects_invalid_strict_host_key_checking() {
	zxfer_test_capture_subshell "
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING=\$(printf 'bad\npolicy')
		zxfer_get_managed_ssh_option_tokens
	"

	assertEquals "Managed ssh transport tokens should fail closed when ZXFER_SSH_STRICT_HOST_KEY_CHECKING is malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed ZXFER_SSH_STRICT_HOST_KEY_CHECKING values should preserve the specific validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_STRICT_HOST_KEY_CHECKING must be a single-line non-empty value."
}

test_invoke_ssh_command_for_host_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_invoke_ssh_command_for_host 'backup.example' '/bin/true'
	"

	assertEquals "ssh argv execution helpers should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh argv execution helpers should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_build_ssh_shell_command_for_host_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_build_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_build_ssh_shell_command_for_host_preserves_transport_token_status() {
	zxfer_test_capture_subshell "
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' 'custom transport failure'
			return 73
		}
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit \"\${2:-1}\"
		}
		zxfer_build_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should preserve the exact transport-token failure status." \
		73 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should preserve the transport-token failure text." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "custom transport failure"
}

test_ssh_host_spec_helpers_reject_invalid_literal_token_strings() {
	zxfer_test_capture_subshell "
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' '/usr/bin/ssh'
		}
		zxfer_invoke_ssh_command_for_host 'backup.example \"pfexec -u zfs\"' '/bin/true'
	"

	assertEquals "ssh argv execution helpers should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh argv execution helpers should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."

	zxfer_test_capture_subshell "
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' '/usr/bin/ssh'
		}
		zxfer_build_ssh_shell_command_for_host 'backup.example \"pfexec -u zfs\"' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."

	zxfer_test_capture_subshell "
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' '/usr/bin/ssh'
		}
		zxfer_invoke_ssh_shell_command_for_host 'backup.example \"pfexec -u zfs\"' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command execution should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command execution should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_invoke_ssh_shell_command_for_host_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_invoke_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command execution should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command execution should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_invoke_ssh_shell_command_for_host_includes_explicit_known_hosts_override() {
	log_file="$TEST_TMPDIR/invoke_shell_known_hosts.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_shell_command_for_host "backup.example" "'sh' '-c' 'printf ok >/dev/null'"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' \
		"-o" "BatchMode=yes" \
		"-o" "StrictHostKeyChecking=yes" \
		"-o" "UserKnownHostsFile=$TEST_TMPDIR/known_hosts" \
		"backup.example" "'sh' '-c' 'printf ok >/dev/null'")

	assertEquals "ssh shell-command execution should pass the explicit managed known-hosts override through the live argv path." \
		"$expected" "$(cat "$log_file")"
}

test_invoke_ssh_shell_command_for_host_emits_explicit_very_verbose_remote_prefix() {
	log_file="$TEST_TMPDIR/invoke_shell_verbose.log"
	stderr_file="$TEST_TMPDIR/invoke_shell_verbose.err"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	zxfer_invoke_ssh_shell_command_for_host \
		"shared.example" "'sh' '-c' 'printf ok >/dev/null'" destination \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/target.sock" "shared.example" \
		"'sh' '-c' 'printf ok >/dev/null'")

	assertContains "Very-verbose ssh shell execution should honor the explicit target-side prefix when hosts match." \
		"$(cat "$stderr_file")" "Running remote command [target: shared.example]:"
	assertContains "Very-verbose ssh shell execution should print the full rendered ssh command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_run_source_zfs_cmd_uses_remote_ssh_when_origin_specified() {
	old_cmd_ssh=$g_cmd_ssh
	old_cmd_zfs=$g_cmd_zfs
	old_origin_cmd_zfs=${g_origin_cmd_zfs:-}
	old_origin_host=$g_option_O_origin_host

	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs="/usr/sbin/zfs"
	g_option_O_origin_host="backup@example.com pfexec -p 2222"
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""

	remote_log="$TEST_TMPDIR/zxfer_run_source_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_run_source_zfs_cmd list tank/fs@snap

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "ssh should force batch mode before connecting to the origin host." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the origin host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before connecting to the origin host." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the origin host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should target the origin host without literal quotes." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	arg_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Privilege wrappers must remain quoted inside the remote shell command." "$arg_remote_cmd" "'pfexec' '-p' '2222'"
	assertContains "zfs binary should use the origin-host path." "$arg_remote_cmd" "'$g_origin_cmd_zfs'"
	assertContains "Remote command should preserve requested subcommand." "$arg_remote_cmd" "'list'"
	assertContains "Dataset argument should remain a single remote-shell token." "$arg_remote_cmd" "'tank/fs@snap'"

	g_cmd_ssh=$old_cmd_ssh
	g_cmd_zfs=$old_cmd_zfs
	g_origin_cmd_zfs=$old_origin_cmd_zfs
	g_option_O_origin_host=$old_origin_host
	g_option_O_origin_host_safe=""
}

test_run_source_zfs_cmd_uses_default_local_zfs_when_wrapper_is_unset() {
	fake_zfs="$TEST_TMPDIR/default_source_zfs"
	outfile="$TEST_TMPDIR/default_source_zfs.out"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$fake_zfs"
	g_option_O_origin_host=""
	g_cmd_zfs="$fake_zfs"
	g_LZFS="$fake_zfs"

	zxfer_run_source_zfs_cmd list -H tank/src >"$outfile"

	assertEquals "The default local source path should execute the resolved zfs binary directly." \
		"list -H tank/src" "$(cat "$outfile")"
	assertEquals "Default local source execution should redact the last command." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_run_destination_zfs_cmd_uses_remote_ssh_when_target_specified() {
	old_cmd_ssh=$g_cmd_ssh
	old_cmd_zfs=$g_cmd_zfs
	old_target_cmd_zfs=${g_target_cmd_zfs:-}
	old_target_host=$g_option_T_target_host

	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_target_cmd_zfs="/usr/sbin/zfs"
	g_option_T_target_host="target@example.com doas"
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""

	remote_log="$TEST_TMPDIR/zxfer_run_destination_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_run_destination_zfs_cmd get -H name tank/dst

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "ssh should force batch mode before connecting to the target host." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the target host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before connecting to the target host." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the target host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should connect to the target host without stray quotes." "target@example.com" "$(sed -n '5p' "$remote_log")"
	targ_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Additional host-spec tokens must survive inside the remote shell command." "$targ_remote_cmd" "'doas'"
	assertContains "Remote call should include the target-host zfs path." "$targ_remote_cmd" "'$g_target_cmd_zfs'"
	assertContains "Command verb should pass through untouched." "$targ_remote_cmd" "'get'"
	assertContains "Original flags should be preserved." "$targ_remote_cmd" "'-H'"
	assertContains "Property argument should pass through verbatim." "$targ_remote_cmd" "'name'"
	assertContains "Dataset argument should remain literal." "$targ_remote_cmd" "'tank/dst'"

	g_cmd_ssh=$old_cmd_ssh
	g_cmd_zfs=$old_cmd_zfs
	g_target_cmd_zfs=$old_target_cmd_zfs
	g_option_T_target_host=$old_target_host
	g_option_T_target_host_safe=""
}

test_run_destination_zfs_cmd_uses_default_local_zfs_when_wrapper_is_unset() {
	fake_zfs="$TEST_TMPDIR/default_dest_zfs"
	outfile="$TEST_TMPDIR/default_dest_zfs.out"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$fake_zfs"
	g_option_T_target_host=""
	g_cmd_zfs="$fake_zfs"
	g_RZFS="$fake_zfs"

	zxfer_run_destination_zfs_cmd get name tank/dst >"$outfile"

	assertEquals "The default local destination path should execute the resolved zfs binary directly." \
		"get name tank/dst" "$(cat "$outfile")"
	assertEquals "Default local destination execution should redact the last command." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_refresh_compression_commands_tokenizes_custom_pipeline() {
	# When -Z supplies a custom command, ensure zxfer stores
	# the quoted representation so eval never executes the raw string.
	if [ "${g_cmd_compress+x}" = x ]; then
		old_g_cmd_compress=$g_cmd_compress
		old_g_cmd_compress_set=1
	else
		old_g_cmd_compress_set=0
	fi
	if [ "${g_cmd_decompress+x}" = x ]; then
		old_g_cmd_decompress=$g_cmd_decompress
		old_g_cmd_decompress_set=1
	else
		old_g_cmd_decompress_set=0
	fi
	if [ "${g_cmd_compress_safe+x}" = x ]; then
		old_g_cmd_compress_safe=$g_cmd_compress_safe
		old_g_cmd_compress_safe_set=1
	else
		old_g_cmd_compress_safe_set=0
	fi
	if [ "${g_cmd_decompress_safe+x}" = x ]; then
		old_g_cmd_decompress_safe=$g_cmd_decompress_safe
		old_g_cmd_decompress_safe_set=1
	else
		old_g_cmd_decompress_safe_set=0
	fi
	if [ "${g_option_z_compress+x}" = x ]; then
		old_g_option_z_compress=$g_option_z_compress
		old_g_option_z_compress_set=1
	else
		old_g_option_z_compress_set=0
	fi

	g_cmd_compress="zstd -3;touch /tmp/pwn"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	g_option_z_compress=0

	zxfer_refresh_compression_commands

	assertEquals "Compression command tokens should be quoted." "'zstd' '-3;' 'touch' '/tmp/pwn'" "$g_cmd_compress_safe"

	if [ "$old_g_cmd_compress_set" -eq 1 ]; then
		g_cmd_compress=$old_g_cmd_compress
	else
		unset g_cmd_compress
	fi
	if [ "$old_g_cmd_decompress_set" -eq 1 ]; then
		g_cmd_decompress=$old_g_cmd_decompress
	else
		unset g_cmd_decompress
	fi
	if [ "$old_g_option_z_compress_set" -eq 1 ]; then
		g_option_z_compress=$old_g_option_z_compress
	else
		unset g_option_z_compress
	fi
	if [ "$old_g_cmd_compress_safe_set" -eq 1 ]; then
		g_cmd_compress_safe=$old_g_cmd_compress_safe
	else
		unset g_cmd_compress_safe
	fi
	if [ "$old_g_cmd_decompress_safe_set" -eq 1 ]; then
		g_cmd_decompress_safe=$old_g_cmd_decompress_safe
	else
		unset g_cmd_decompress_safe
	fi
}

test_derive_override_lists_handles_overrides_only() {
	result=$(zxfer_derive_override_lists "compression=lz4=local" "compression=lzjb" 0 "filesystem")

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$result
EOF

	assertEquals "Override list should reflect -o values with override sources." "compression=lzjb=override" "$override_pvs"
	assertEquals "Creation list should keep explicit overrides for source-local properties." "compression=lzjb=override" "$creation_pvs"
}

test_derive_override_lists_includes_local_props_for_creation() {
	source_pvs="compression=lz4=local,refreservation=4G=received,quota=none=local"
	override_opts="quota=8G"
	result=$(zxfer_derive_override_lists "$source_pvs" "$override_opts" 1 "volume")

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$result
EOF

	expected_override="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Overrides should include source properties with user overrides applied." "$(sort_property_list "$expected_override")" "$(sort_property_list "$override_pvs")"
	expected_creation="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Creation list should keep local props, explicit local overrides, and zvol refreservation even if not local." "$(sort_property_list "$expected_creation")" "$(sort_property_list "$creation_pvs")"
}

test_diff_properties_separates_set_and_inherit_lists() {
	override_pvs="compression=lz4=local,atime=off=received"
	dest_pvs="compression=lzjb=local,atime=on=local"
	result=$(zxfer_diff_properties "$override_pvs" "$dest_pvs" "casesensitivity,normalization,jailed,utf8only")

	{
		IFS= read -r initial_set_list
		IFS= read -r set_list
		IFS= read -r inherit_list
	} <<EOF
$result
EOF

	assertEquals "Initial pass should require setting every diverging property." "compression=lz4,atime=off" "$initial_set_list"
	assertEquals "Child dataset should only set properties sourced locally on the parent." "compression=lz4" "$set_list"
	assertEquals "Child dataset should inherit properties whose source is not local." "atime=off" "$inherit_list"
}

test_apply_property_changes_skips_inherit_for_initial_source() {
	FAKE_SET_CALLS=""
	FAKE_INHERIT_CALLS=""

	zxfer_apply_property_changes "pool/src" 1 "compression=lz4" "" "" fake_property_set_runner fake_property_inherit_runner

	assertEquals "Initial source should call the set runner once with the full initial diff list." "compression=lz4@pool/src;" "$FAKE_SET_CALLS"
	assertEquals "Initial source should not inherit properties." "" "$FAKE_INHERIT_CALLS"
}

test_apply_property_changes_invokes_inherit_runner_for_children() {
	FAKE_SET_CALLS=""
	FAKE_INHERIT_CALLS=""

	zxfer_apply_property_changes "pool/src" 0 "" "compression=lz4" "atime=off" fake_property_set_runner fake_property_inherit_runner

	assertEquals "Child dataset should apply the full child set list in one runner call." "compression=lz4@pool/src;" "$FAKE_SET_CALLS"
	assertEquals "Child dataset should inherit requested properties." "atime@pool/src;" "$FAKE_INHERIT_CALLS"
}

test_strip_trailing_slashes_trims_dataset_suffixes() {
	# Datasets may be provided with a trailing slash; ensure we drop all trailing
	# separators so concatenated child names never gain a double slash.
	result=$(zxfer_strip_trailing_slashes "pool/dst///")
	assertEquals "Trailing slashes should be removed." "pool/dst" "$result"

	result=$(zxfer_strip_trailing_slashes "pool/dst")
	assertEquals "Paths without trailing slashes should be unchanged." "pool/dst" "$result"
}

test_strip_trailing_slashes_preserves_absolute_placeholders() {
	# Absolute paths are rejected later, so inputs that consist entirely of
	# slashes must be passed through untouched.
	result=$(zxfer_strip_trailing_slashes "/")
	assertEquals "Single slash inputs should be preserved." "/" "$result"

	result=$(zxfer_strip_trailing_slashes "")
	assertEquals "Empty inputs should stay empty." "" "$result"
}

test_execute_command_respects_dry_run_mode() {
	# With --dry-run enabled, zxfer_execute_command should not run but still
	# describe the action, so no temp files should be created.
	temp_file="$TEST_TMPDIR/dry_run_output"
	g_option_n_dryrun=1

	zxfer_execute_command "printf 'should not run' > '$temp_file'"

	assertFalse "Dry run should skip running the command." "[ -f \"$temp_file\" ]"
}

test_execute_command_runs_command_when_not_dry_run() {
	# When --dry-run is off, the helper must execute commands verbatim.
	temp_file="$TEST_TMPDIR/run_output"

	zxfer_execute_command "printf 'ran' > '$temp_file'"

	assertTrue "Command should run when dry run is disabled." "[ -f \"$temp_file\" ]"
	assertEquals "ran" "$(cat "$temp_file")"
}

test_get_temp_file_creates_unique_file() {
	# zxfer_get_temp_file should provide unique temp files so concurrent options do
	# not collide or overwrite each other.
	file_one=$(zxfer_get_temp_file)
	file_two=$(zxfer_get_temp_file)

	assertTrue "First temp file should exist." "[ -f \"$file_one\" ]"
	assertTrue "Second temp file should exist." "[ -f \"$file_two\" ]"
	assertNotEquals "Two consecutive temp file names should be unique." "$file_one" "$file_two"

	rm -f "$file_one" "$file_two"
}

test_get_temp_file_honors_tmpdir_variable() {
	# Honor the TMPDIR override so tests or CLI invocations can direct
	# scratch files to a specific filesystem, but use the validated
	# physical directory path rather than a logical symlinked alias.
	custom_tmp="$TEST_TMPDIR/custom"
	mkdir -p "$custom_tmp"
	physical_custom_tmp=$(cd -P "$custom_tmp" && pwd)
	TMPDIR="$custom_tmp"

	file=$(zxfer_get_temp_file)

	case "$file" in
	"$physical_custom_tmp"/*) inside=0 ;;
	*) inside=1 ;;
	esac

	assertEquals "Temp file should be created inside the validated TMPDIR root." 0 "$inside"
	assertTrue "Temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_uses_physical_tmpdir_for_symlinked_tmpdir_paths() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/tmp_real"
	link_tmp="$physical_tmpdir/tmp_link"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)

	case "$file" in
	"$real_tmp"/*) inside_real=0 ;;
	*) inside_real=1 ;;
	esac
	case "$file" in
	"$link_tmp"/*) inside_link=0 ;;
	*) inside_link=1 ;;
	esac

	assertEquals "Temp files should use the physical TMPDIR target instead of the symlinked path." 0 "$inside_real"
	assertEquals "Temp files should not be created through the symlinked TMPDIR path itself." 1 "$inside_link"
	assertTrue "Temp file should exist under the physical TMPDIR path." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_rejects_non_sticky_world_writable_tmpdir_and_falls_back_to_system_tmp() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/insecure_tmp"
	mkdir -p "$insecure_tmp"
	chmod 0777 "$insecure_tmp"
	TMPDIR="$insecure_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)
	status=$?

	assertEquals "Non-sticky world-writable TMPDIR values should not prevent temporary file creation." 0 "$status"
	case "$file" in
	"$insecure_tmp"/*) inside_insecure=0 ;;
	*) inside_insecure=1 ;;
	esac
	assertEquals "Non-sticky world-writable TMPDIR values should be rejected." 1 "$inside_insecure"
	assertTrue "Fallback temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	chmod 0700 "$insecure_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_allows_sticky_world_writable_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	sticky_tmp="$physical_tmpdir/sticky_tmp"
	mkdir -p "$sticky_tmp"
	chmod 1777 "$sticky_tmp"
	TMPDIR="$sticky_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)

	case "$file" in
	"$sticky_tmp"/*) inside_sticky=0 ;;
	*) inside_sticky=1 ;;
	esac

	assertEquals "Sticky world-writable TMPDIR values should remain usable." 0 "$inside_sticky"
	assertTrue "Sticky TMPDIR temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	chmod 0700 "$sticky_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_ignores_relative_tmpdir_and_falls_back_to_system_tmp() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/relative_tmp_root"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."
	TMPDIR="relative_tmp_root"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative TMPDIR values should not prevent temporary file creation." 0 "$status"
	case "$file" in
	"$physical_tmpdir"/relative_tmp_root/*) inside_relative=0 ;;
	*) inside_relative=1 ;;
	esac
	assertEquals "Relative TMPDIR values should be ignored instead of being used directly." 1 "$inside_relative"
	assertTrue "Fallback temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_throws_when_mktemp_fails() {
	set +e
	output=$(
		(
			mktemp() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_temp_file
		)
	)
	status=$?

	assertEquals "Temporary-file allocation failures should abort." 1 "$status"
	assertContains "Temporary-file allocation failures should use the documented error." \
		"$output" "Error creating temporary file."
}

test_echov_outputs_only_when_verbose_enabled() {
	# zxfer_echov should emit text only when -v/--verbose is set.
	g_option_v_verbose=1
	output=$(zxfer_echov "verbose message")

	assertEquals "verbose message" "$output"

	g_option_v_verbose=0
	output=$(zxfer_echov "hidden message")

	assertEquals "" "$output"
}

test_echoV_outputs_only_when_very_verbose_enabled() {
	# zxfer_echoV uses -V/--very-verbose, so it should stay quiet unless the
	# highest verbosity level is requested.
	g_option_V_very_verbose=1
	output=$(zxfer_echoV "debug message" 2>&1)

	assertEquals "debug message" "$output"

	g_option_V_very_verbose=0
	output=$(zxfer_echoV "hidden debug" 2>&1)

	assertEquals "" "$output"
}

test_beep_skips_on_non_freebsd_hosts() {
	output=$(
		(
			uname() {
				printf '%s\n' "Linux"
			}
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "Non-FreeBSD hosts should skip beep handling with a debug message." \
		"$output" "Beep requested but unsupported on Linux; skipping."
}

test_beep_skips_when_speaker_device_is_missing() {
	output=$(
		(
			uname() {
				printf '%s\n' "FreeBSD"
			}
			kldstat() {
				printf '%s\n' "speaker.ko"
			}
			kldload() {
				return 0
			}
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without /dev/speaker should skip beep handling with a debug message." \
		"$output" "Beep requested but /dev/speaker missing; skipping."
}

test_beep_skips_when_speaker_tools_are_missing() {
	fake_bin_dir="$TEST_TMPDIR/no_speaker_tools"
	fake_uname_bin="$fake_bin_dir/uname"
	mkdir -p "$fake_bin_dir"
	cat >"$fake_uname_bin" <<'EOF'
#!/bin/sh
printf '%s\n' "FreeBSD"
EOF
	chmod +x "$fake_uname_bin"

	output=$(
		(
			PATH="$fake_bin_dir"
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without speaker helper tools should skip beep handling with a debug message." \
		"$output" "Beep requested but speaker tools are missing; skipping."
}

test_execute_background_cmd_writes_output_file() {
	# Background commands are used for option pipelines; ensure their stdout
	# still lands in the provided tempfile.
	temp_file="$TEST_TMPDIR/bg_output"
	g_last_background_pid=""

	zxfer_execute_background_cmd "printf bg-data" "$temp_file"
	bg_pid=$g_last_background_pid
	wait "$bg_pid"

	assertTrue "zxfer_execute_background_cmd should expose the spawned PID for callers." \
		"[ -n \"$bg_pid\" ]"
	assertTrue "Background output file should be created." "[ -f \"$temp_file\" ]"
	assertEquals "bg-data" "$(cat "$temp_file")"
}

test_execute_background_cmd_fails_closed_when_cleanup_registration_fails() {
	temp_file="$TEST_TMPDIR/bg_register_fail_output"

	output=$(
		(
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_direct_child_pid() {
				printf 'abort:%s:%s:%s\n' "$1" "$2" "$3"
				kill -s TERM "$1" 2>/dev/null || :
				wait "$1" 2>/dev/null || :
				return 0
			}
			zxfer_execute_background_cmd \
				"sleep 30" \
				"$temp_file"
			printf 'status=%s\n' "$?"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Background helper registration failures should fail closed." \
		"$output" "status=1"
	assertContains "Background helper registration failures should clear the published PID." \
		"$output" "pid="
	assertContains "Background helper registration failures should route teardown through the validated direct-child abort helper." \
		"$output" "abort:"
	assertContains "Background helper registration failures should preserve the cleanup-helper purpose when invoking the validated direct-child abort helper." \
		"$output" "background command helper"
}

test_execute_background_cmd_fails_closed_when_cleanup_wrapper_lookup_fails() {
	temp_file="$TEST_TMPDIR/bg_wrapper_lookup_fail_output"

	output=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "cleanup wrapper lookup failed"
				return 1
			}
			zxfer_execute_background_cmd "sleep 30" "$temp_file"
			printf 'status=%s\n' "$?"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Background execution should fail closed when the cleanup-wrapper lookup fails." \
		"$output" "status=1"
	assertContains "Cleanup-wrapper lookup failures should not publish a background PID." \
		"$output" "pid="
}

test_execute_background_cmd_preserves_abort_failures_when_cleanup_registration_fails() {
	temp_file="$TEST_TMPDIR/bg_abort_fail_output"

	output=$(
		(
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_direct_child_pid() {
				printf 'abort:%s:%s:%s\n' "$1" "$2" "$3"
				return 1
			}
			zxfer_execute_background_cmd \
				"sleep 30" \
				"$temp_file"
			printf 'status=%s\n' "$?"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Background helper registration failures should preserve validated abort failures." \
		"$output" "status=1"
	assertContains "Background helper registration failures should still clear the published PID when aborting the helper fails." \
		"$output" "pid="
	assertContains "Background helper registration failures should still route teardown through the validated direct-child abort helper before returning the abort failure." \
		"$output" "abort:"
}

test_execute_background_cmd_respects_dry_run_mode() {
	temp_file="$TEST_TMPDIR/bg_dry_run_output"
	err_file="$TEST_TMPDIR/bg_dry_run_error"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			zxfer_execute_background_cmd "printf bg-data" "$temp_file" "$err_file"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Dry-run background execution should render the skipped command." \
		"$output" "Dry run: printf bg-data"
	assertContains "Dry-run background execution should leave the background PID unset." \
		"$output" "pid="
	assertTrue "Dry-run background execution should still create the placeholder output file." \
		"[ -f \"$temp_file\" ]"
	assertTrue "Dry-run background execution should still create the placeholder error file." \
		"[ -f \"$err_file\" ]"
	assertEquals "Dry-run background execution should leave the placeholder output empty." \
		"" "$(cat "$temp_file")"
	assertEquals "Dry-run background execution should leave the placeholder error file empty." \
		"" "$(cat "$err_file")"
}

test_execute_background_cmd_dry_run_fails_closed_when_placeholder_creation_fails() {
	temp_dir="$TEST_TMPDIR/bg_dry_run_output_dir"
	err_dir="$TEST_TMPDIR/bg_dry_run_error_dir"
	mkdir -p "$temp_dir" "$err_dir"

	zxfer_test_capture_subshell '
		g_option_n_dryrun=1
		zxfer_execute_background_cmd "printf bg-data" "'"$temp_dir"'" "'"$err_dir"'"
	'

	assertEquals "Dry-run background execution should return failure when placeholder file creation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_execute_background_cmd_dry_run_clears_stale_pid_and_partial_output_on_second_placeholder_failure() {
	output_file="$TEST_TMPDIR/bg_dry_run_partial_output"
	err_dir="$TEST_TMPDIR/bg_dry_run_partial_error_dir"
	mkdir -p "$err_dir"

	output=$(
		(
			g_option_n_dryrun=1
			g_last_background_pid=43210
			zxfer_execute_background_cmd "printf bg-data" "$output_file" "$err_dir" 2>/dev/null
			printf 'status=%s\n' "$?"
			printf 'pid=<%s>\n' "${g_last_background_pid:-}"
			if [ -e "$output_file" ]; then
				printf 'output_exists=yes\n'
			else
				printf 'output_exists=no\n'
			fi
		)
	)

	assertContains "Dry-run placeholder failures should preserve the write failure status." \
		"$output" "status=1"
	assertContains "Dry-run placeholder failures should clear any stale background PID state." \
		"$output" "pid=<>"
	assertContains "Dry-run placeholder failures should not leave a partially published output placeholder behind." \
		"$output" "output_exists=no"
}

test_exists_destination_returns_one_on_success() {
	# zxfer_exists_destination checks the remote ZFS command stored in g_RZFS;
	# when the check succeeds, the helper returns 1.
	# shellcheck disable=SC2031
	old_g_RZFS=${g_RZFS-}
	g_RZFS=true

	result=$(zxfer_exists_destination "pool/fs")

	assertEquals "Destination should exist when command succeeds." "1" "$result"

	g_RZFS=$old_g_RZFS
}

test_exists_destination_returns_zero_when_dataset_is_missing() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'pool/fs': dataset does not exist" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Explicit missing-dataset errors should map to destination absent." 0 "$status"
	assertEquals "Missing destinations should still return 0." "0" "$result"
}

test_exists_destination_returns_zero_when_dataset_is_missing_with_stdout_only_error() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'pool/fs': dataset does not exist"
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Missing-dataset probes should still map to destination absent when the platform reports the error on stdout." 0 "$status"
	assertEquals "Stdout-only missing-dataset probes should still return 0." "0" "$result"
}

test_exists_destination_returns_zero_when_dataset_is_missing_with_omnios_error() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'pool/fs': no such pool or dataset" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "OmniOS-style missing-dataset probes should still map to destination absent." 0 "$status"
	assertEquals "OmniOS-style missing-dataset probes should still return 0." "0" "$result"
}

test_exists_destination_reports_probe_failures() {
	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "ssh: permission denied" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Operational probe failures should return non-zero." 1 "$status"
	assertContains "Operational probe failures should preserve the destination context." \
		"$output" "Failed to determine whether destination dataset [pool/fs] exists: ssh: permission denied"
}

test_exists_destination_reports_probe_failures_without_stderr() {
	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Silent destination probe failures should still return non-zero." 1 "$status"
	assertContains "Silent destination probe failures should still report the destination dataset." \
		"$output" "Failed to determine whether destination dataset [pool/fs] exists."
}

test_exists_destination_uses_cached_exact_result_without_reprobing() {
	output=$(
		(
			zxfer_set_destination_existence_cache_entry "pool/fs" 1
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)

	assertEquals "Exact cached destination results should be returned without another zfs probe." \
		"1" "$output"
}

test_exists_destination_infers_missing_descendants_from_seeded_tree() {
	output=$(
		(
			zxfer_seed_destination_existence_cache_from_recursive_list "backup/dst" "backup/dst
backup/dst/existing"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "backup/dst/missing"
		)
	)

	assertEquals "Datasets omitted from a seeded destination subtree should be treated as missing without another zfs probe." \
		"0" "$output"
}

test_exists_destination_live_bypasses_cache_and_refreshes_exact_entry() {
	output=$(
		(
			l_result_file="$TEST_TMPDIR/exists_destination_cache_refresh.out"
			zxfer_mark_destination_root_missing_in_cache "backup/dst"
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			zxfer_exists_destination "backup/dst/child" live >"$l_result_file"
			l_live_result=$(cat "$l_result_file")
			printf 'live=%s\n' "$l_live_result"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "backup/dst/child" >"$l_result_file"
			l_cached_result=$(cat "$l_result_file")
			printf 'cached=%s\n' "$l_cached_result"
		)
	)

	assertContains "Live destination probes should bypass cached subtree-missing state." \
		"$output" "live=1"
	assertContains "Successful live probes should refresh the exact cache entry for later callers." \
		"$output" "cached=1"
}

test_exists_destination_uses_parent_recursive_listing_for_ambiguous_omnios_child_probes() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					printf '%s\n' "backup/dst/src/child"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Ambiguous OmniOS child probes should fall back to a parent recursive listing." 0 "$status"
	assertEquals "A parent recursive listing that contains the child should report that it exists." \
		"1" "$output"
}

test_exists_destination_uses_parent_recursive_listing_to_confirm_missing_omnios_child() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Ambiguous OmniOS child probes should still return successfully when the parent listing proves the child is missing." \
		0 "$status"
	assertEquals "A parent recursive listing that omits the child should report it missing." \
		"0" "$output"
}

test_exists_destination_reports_parent_recursive_listing_failures_for_ambiguous_omnios_child_probe() {
	set +e
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "permission denied" >&2
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Ambiguous OmniOS child probes should still fail closed when the parent recursive fallback errors." \
		1 "$status"
	assertContains "Fallback failures should preserve the child dataset context." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] failed: permission denied"
}

test_exists_destination_reports_parent_recursive_listing_without_parent_dataset_for_ambiguous_omnios_child_probe() {
	set +e
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/other"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "A parent recursive fallback that does not list the parent dataset should fail closed." \
		1 "$status"
	assertContains "The missing-parent fallback failure should identify the child and parent datasets." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] did not contain the parent dataset."
}

test_exists_destination_parent_recursive_listing_treats_missing_parent_as_missing_child_for_ambiguous_omnios_probe() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "cannot open 'backup/dst/src': no such pool or dataset" >&2
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "A missing parent discovered through the recursive fallback should map to a missing child." \
		0 "$status"
	assertEquals "Missing-parent fallback should report the child as absent." \
		"0" "$output"
}

test_exists_destination_parent_recursive_listing_treats_silent_missing_parent_as_missing_child_when_ancestor_confirms_absence() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "A silent SunOS parent-listing failure should map to missing only when an ancestor listing proves the parent is absent." \
		0 "$status"
	assertEquals "Confirmed silent missing-parent fallback should report the child as absent." \
		"0" "$output"
}

test_exists_destination_reports_silent_parent_recursive_listing_failures_for_ambiguous_omnios_child_probe() {
	set +e
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst" ]; then
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Silent parent recursive fallback failures should still fail closed." 1 "$status"
	assertContains "Silent parent fallback failures should emit the dedicated recursive-listing error." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] failed."
}

test_exists_destination_cached_hits_do_not_increment_probe_counter() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_exists_destination_calls=0
			zxfer_set_destination_existence_cache_entry "pool/fs" 1
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs" >/dev/null
			printf 'cached_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			zxfer_exists_destination "pool/other" live >/dev/null
			printf 'live_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
		)
	)

	assertContains "Cached destination answers should not count as live destination probes." \
		"$output" "cached_calls=0"
	assertContains "Live destination probes should still increment the destination-probe profile counter." \
		"$output" "live_calls=1"
}

test_write_backup_properties_treats_backup_data_as_literal() {
	# Property backups must never interpret dataset-controlled data as shell
	# commands. Ensure values containing command substitutions are written
	# verbatim and do not execute locally.
	mount_dir="$TEST_TMPDIR/mnt"
	mkdir -p "$mount_dir"
	FAKE_ZFS_MOUNTPOINT="$mount_dir"
	old_g_RZFS=${g_RZFS-}
	g_RZFS=fake_zfs_mountpoint_cmd

	g_initial_source="pool/src"
	g_destination="pool/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_T_target_host=""
	g_option_n_dryrun=0

	sentinel_file="$TEST_TMPDIR/sentinel_touch"
	rm -f "$sentinel_file"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "user:note=\$(touch $sentinel_file)")

	zxfer_write_backup_properties

	secure_dir=$(zxfer_get_backup_storage_dir_for_dataset_tree "$g_initial_source")
	backup_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	backup_file="$secure_dir/$backup_name"

	assertTrue "Backup property file should be written." "[ -f \"$backup_file\" ]"
	assertFalse "Backup file must not be written into dataset mountpoints." "[ -f \"$mount_dir/$backup_name\" ]"
	assertFalse "Command substitutions within properties must not run." "[ -f \"$sentinel_file\" ]"

	backup_contents=$(cat "$backup_file")
	needle="\$(touch $sentinel_file)"
	case "$backup_contents" in
	*"$needle"*) found=0 ;;
	*) found=1 ;;
	esac
	assertEquals "Backup file should contain literal property data." 0 "$found"

	if [ -n "${old_g_RZFS-}" ]; then
		g_RZFS=$old_g_RZFS
	else
		unset g_RZFS
	fi
	unset FAKE_ZFS_MOUNTPOINT
	rm -f "$backup_file"
}

test_write_backup_properties_skips_when_no_data() {
	old_g_backup_storage_root=${g_backup_storage_root-}
	g_backup_storage_root="$TEST_TMPDIR/backup-skip"
	rm -rf "$g_backup_storage_root"

	g_initial_source="pool/src"
	g_destination="pool/dst"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents=""

	zxfer_write_backup_properties

	assertFalse "Backup metadata should not be written when no properties were collected." "[ -d \"$g_backup_storage_root\" ]"

	if [ -n "${old_g_backup_storage_root-}" ]; then
		g_backup_storage_root=$old_g_backup_storage_root
	else
		unset g_backup_storage_root
	fi
}

test_read_local_backup_file_refuses_non_root_owned_metadata() {
	# zxfer_read_local_backup_file must refuse to parse metadata when the file is
	# not root-owned to prevent tampering from less-privileged users. Stub
	# the ownership/mode helpers so the test does not rely on the invoking
	# user's UID or default umask.
	backup_file="$TEST_TMPDIR_PHYSICAL/insecure_backup"
	printf '%s\n' "tampered" >"$backup_file"
	chmod 600 "$backup_file"

	if output=$(
		(
			zxfer_get_path_owner_uid() { printf '%s\n' "1234"; }
			zxfer_get_path_mode_octal() { printf '%s\n' "600"; }
			zxfer_read_local_backup_file "$backup_file"
		) 2>&1
	); then
		status=0
	else
		status=$?
	fi

	assertEquals "Reading non-root metadata should exit with an error." 1 "$status"

	expected_owner_desc="root (UID 0)"
	if command -v id >/dev/null 2>&1; then
		if current_uid=$(id -u 2>/dev/null); then
			if [ "$current_uid" != "0" ]; then
				expected_owner_desc="$expected_owner_desc or UID $current_uid"
			fi
		fi
	fi

	case "$output" in
	*"Refusing to use backup metadata $backup_file because it is owned by UID 1234 instead of $expected_owner_desc."*) ;;
	*)
		fail "zxfer_read_local_backup_file did not report an insecure owner: $output"
		;;
	esac

	rm -f "$backup_file"
}

test_read_local_backup_file_returns_contents_when_secure() {
	# When metadata ownership and permissions pass validation, the helper
	# should return the literal on-disk contents.
	backup_file="$TEST_TMPDIR_PHYSICAL/secure_backup"
	printf '%s\n' "trusted" >"$backup_file"

	result=$(read_backup_file_with_mocked_security "$backup_file")

	assertEquals "trusted" "$result"
	rm -f "$backup_file"
}

test_read_local_backup_file_reads_from_staged_snapshot_path() {
	backup_file="$TEST_TMPDIR_PHYSICAL/secure_backup_snapshot"
	cat_arg_log="$TEST_TMPDIR/read_local_backup_snapshot_arg"
	printf '%s\n' "trusted" >"$backup_file"
	chmod 600 "$backup_file"

	result=$(
		(
			zxfer_get_path_owner_uid() { printf '%s\n' "0"; }
			zxfer_get_path_mode_octal() { printf '%s\n' "600"; }
			cat() {
				printf '%s\n' "$1" >"$cat_arg_log"
				command cat "$1"
			}
			zxfer_read_local_backup_file "$backup_file"
		)
	)

	assertEquals "Staged local backup reads should still return the payload." "trusted" "$result"
	assertNotEquals "Secure local backup reads should read through the staged snapshot path instead of reopening the original pathname." \
		"$backup_file" "$(cat "$cat_arg_log")"
	assertContains "Staged local backup reads should use the dedicated sibling staging directory." \
		"$(cat "$cat_arg_log")" ".zxfer-backup-read"
	rm -f "$backup_file"
}

test_read_local_backup_file_returns_failure_when_cat_fails_after_security_checks() {
	backup_file="$TEST_TMPDIR_PHYSICAL/secure_backup_cat_fail"
	printf '%s\n' "trusted" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	status=$(
		(
			zxfer_require_backup_metadata_path_without_symlinks() {
				return 0
			}
			zxfer_check_secure_backup_file() {
				return 0
			}
			cat() {
				return 1
			}
			zxfer_read_local_backup_file "$backup_file" >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Secure local backup reads should surface literal cat failures after security checks pass." \
		1 "$status"
	rm -f "$backup_file"
}

test_read_local_backup_file_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/read_local_backup_real"
	link_dir="$physical_tmpdir/read_local_backup_link"
	backup_file="$link_dir/backup.meta"
	mkdir -p "$real_dir"
	printf '%s\n' "trusted" >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
	ln -s "$real_dir" "$link_dir"

	set +e
	output=$(
		(
			zxfer_read_local_backup_file "$backup_file"
		) 2>&1
	)
	status=$?

	assertEquals "Backup metadata reads should reject symlinked parent components." 1 "$status"
	assertContains "Nested symlink reads should identify the offending path component." \
		"$output" "Refusing to use backup metadata $backup_file because path component $link_dir is a symlink."
}

test_build_source_snapshot_list_cmd_serial_returns_direct_list() {
	g_LZFS="/sbin/zfs"
	g_initial_source="tank/data"
	g_option_j_jobs=1

	result=$(zxfer_build_source_snapshot_list_cmd)

	assertEquals "Serial snapshot listing should render a shell-safe direct zfs command." \
		"'/sbin/zfs' 'list' '-Hr' '-o' 'name' '-s' 'creation' '-t' 'snapshot' 'tank/data'" "$result"
}

test_build_source_snapshot_list_cmd_parallel_local_includes_parallel_runner() {
	g_LZFS="/sbin/zfs"
	g_initial_source="tank/home"
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_option_O_origin_host=""
	g_option_z_compress=0

	result=$(
		(
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Local -j listing should enumerate source datasets directly." \
		"$result" "'/sbin/zfs' 'list' '-Hr' '-t' 'filesystem,volume' '-o' 'name' 'tank/home'"
	assertContains "GNU parallel invocation should include the job count." "$result" "'$g_cmd_parallel' -j 4 --line-buffer"
	assertContains "Local parallel snapshot listing should embed the per-dataset runner command." "$result" "'snapshot'"
	assertContains "Local parallel snapshot listing should preserve the dataset placeholder." "$result" "{}"
	assertNotContains "Local -j listing should not inline prefetched dataset lists." "$result" "'printf'"
	assertNotContains "Local parallel snapshot listing should not reintroduce a sh -c wrapper." "$result" "sh -c"
}

test_build_source_snapshot_list_cmd_remote_with_compression_sets_ssh_pipeline() {
	g_LZFS="/sbin/zfs"
	g_cmd_zfs="/usr/sbin/zfs"
	g_origin_cmd_zfs="/opt/openzfs/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"
	g_initial_source="tank/src"
	g_option_j_jobs=8
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_option_O_origin_host="backup@example.com pfexec -p 2222"
	g_option_O_origin_host_safe=""
	g_option_z_compress=1
	g_cmd_ssh="/usr/bin/ssh"

	result=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/opt/bin/parallel"
			}
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "GNU parallel (fake)"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Remote listing should start with ssh." "$result" "$g_cmd_ssh"
	assertContains "The ssh target host should remain a standalone local argument." "$result" "'backup@example.com'"
	assertContains "Wrapper tokens should remain inside the remote command string." "$result" "'pfexec'"
	assertContains "Wrapper flags should remain inside the remote command string." "$result" "'-p'"
	assertContains "Wrapper flag values should remain inside the remote command string." "$result" "'2222'"
	assertContains "Remote -j listing should enumerate source datasets on the origin host." \
		"$result" "filesystem,volume"
	assertContains "Remote -j listing should preserve the configured source root inside the remote dataset enumeration command." \
		"$result" "tank/src"
	assertContains "Remote GNU parallel path should be used." "$result" "/opt/bin/parallel"
	assertContains "Remote GNU parallel invocation should preserve the job count." "$result" "-j 8 --line-buffer"
	assertContains "Remote listing should use the origin host zfs path." "$result" "$g_origin_cmd_zfs"
	assertContains "Remote metadata discovery should include the resolved remote compressor path." "$result" "/remote/bin/zstd"
	assertContains "Remote metadata discovery should include the local decompression stage." "$result" "/local/bin/zstd"
	assertContains "Remote command should use GNU parallel's direct dataset placeholder runner." \
		"$result" "{}"
	assertNotContains "Remote -j listing should not inline prefetched dataset lists." "$result" "'printf'"
}

test_build_source_snapshot_list_cmd_remote_helper_path_does_not_execute_locally() {
	marker="$TEST_TMPDIR/remote_helper_marker"
	outfile="$TEST_TMPDIR/remote_helper.out"
	errfile="$TEST_TMPDIR/remote_helper.err"
	remote_log="$TEST_TMPDIR/remote_helper.log"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs="/bin/echo; touch $marker #"
	g_option_O_origin_host="backup@example.com"
	g_option_O_origin_host_safe=""
	g_option_j_jobs=1
	g_initial_source="tank/src"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	l_cmd=$(zxfer_build_source_snapshot_list_cmd)
	zxfer_execute_background_cmd "$l_cmd" "$outfile" "$errfile"
	wait "$g_last_background_pid"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertFalse "Resolved remote helper paths should not execute locally when snapshot listing is eval'd." \
		"[ -e '$marker' ]"
	assertEquals "ssh should force batch mode before the remote helper host token." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the remote helper host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before the remote helper host token." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the remote helper host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should still target the requested host." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "The malicious helper path should be quoted as one remote-shell token." \
		"$log_line_remote_cmd" "'/bin/echo; touch $marker #'"
}

test_resolve_remote_required_tool_uses_shell_probe_for_wrapped_hosts() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	remote_log="$TEST_TMPDIR/resolve_remote_required_tool.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(zxfer_resolve_remote_required_tool "backup@example.com pfexec -p 2222" zfs)

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote tool lookup should return the resolved absolute path." "/remote/bin/zfs" "$result"
	assertEquals "ssh should force batch mode before the wrapped-host probe target." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the wrapped-host probe target." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before the wrapped-host probe target." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the wrapped-host probe target." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "Host token should remain the ssh target." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Privilege wrapper should be preserved inside the remote command string." "$log_line_remote_cmd" "'pfexec'"
	assertContains "Wrapper flags should be preserved inside the remote command string." "$log_line_remote_cmd" "'-p'"
	assertContains "Wrapper flag values should be preserved inside the remote command string." "$log_line_remote_cmd" "'2222'"
	assertContains "Remote capability discovery should execute via sh -c for wrapped hosts." "$log_line_remote_cmd" "'sh' '-c'"
	assertContains "Remote capability discovery should pin the secure PATH inside the shell probe." "$log_line_remote_cmd" "/opt/openzfs/bin:/usr/sbin"
	assertContains "Remote capability discovery should query uname in the single handshake." "$log_line_remote_cmd" "uname"
	assertContains "Remote capability discovery should query zfs in the single handshake." "$log_line_remote_cmd" "zfs"
}

test_resolve_remote_required_tool_handles_realistic_ssh_command_joining() {
	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_join_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_join_exec.log"
	remote_bin_dir="$TEST_TMPDIR/remote_bins"

	mkdir -p "$remote_bin_dir"
	create_fake_ssh_join_exec_bin "$realistic_ssh_bin"
	cat >"$remote_bin_dir/zfs" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$remote_bin_dir/zfs"

	g_cmd_ssh="$realistic_ssh_bin"
	g_zxfer_dependency_path="$remote_bin_dir:/usr/bin"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	result=$(zxfer_resolve_remote_required_tool "backup@example.com" zfs)

	unset FAKE_SSH_LOG

	assertEquals "Remote lookup should survive ssh joining the remote capability handshake into a shell string." "$remote_bin_dir/zfs" "$result"
	assertContains "The realistic ssh emulation should receive the expected remote shell command." \
		"$(cat "$realistic_ssh_log")" "command -v"
	assertContains "The realistic ssh emulation should include the requested tool name." \
		"$(cat "$realistic_ssh_log")" "zfs"
	assertContains "The realistic ssh emulation should also include the uname probe from the combined handshake." \
		"$(cat "$realistic_ssh_log")" "uname"
}

test_resolve_remote_required_tool_reports_remote_probe_failures() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_SUPPRESS_STDOUT=1
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	set +e
	result=$(zxfer_resolve_remote_required_tool "backup@example.com" zfs "zfs")
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote lookup should fail when ssh cannot execute the probe." 1 "$status"
	assertEquals "Remote lookup failures should not be misreported as missing binaries." \
		"Failed to query dependency \"zfs\" on host backup@example.com." "$result"
}

test_resolve_remote_required_tool_reports_missing_remote_dependency() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response_missing_zfs)
	export FAKE_SSH_STDOUT_OVERRIDE

	set +e
	result=$(zxfer_resolve_remote_required_tool "backup@example.com" zfs "zfs")
	status=$?

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote lookup should fail when the secure PATH probe returns no result." 1 "$status"
	assertEquals "Missing remote tools should mention the secure PATH guidance." \
		"Required dependency \"zfs\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$result"
}

test_resolve_remote_required_tool_maps_missing_tool_from_capability_handshake_to_missing_dependency() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response_missing_parallel)
	export FAKE_SSH_STDOUT_OVERRIDE

	set +e
	result=$(zxfer_resolve_remote_required_tool "backup@example.com" parallel "GNU parallel")
	status=$?

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote lookup should treat handshake-reported missing tools as missing dependencies." 1 "$status"
	assertEquals "Handshake-reported missing tools should map to the user-facing missing dependency guidance." \
		"Required dependency \"GNU parallel\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$result"
}

test_resolve_remote_required_tool_rejects_relative_remote_path() {
	set +e
	result=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				zxfer_validate_resolved_tool_path "zfs" "zfs" "host backup@example.com"
			}
			zxfer_resolve_remote_required_tool "backup@example.com" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Remote lookup should fail when the remote probe returns a non-absolute path." 1 "$status"
	assertEquals "Relative remote tool paths should be rejected explicitly." \
		"Required dependency \"zfs\" on host backup@example.com resolved to \"zfs\", but zxfer requires an absolute path." \
		"$result"
}

test_resolve_remote_required_tool_uses_fresh_capability_cache_file_before_ssh() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	requested_tools=$(zxfer_get_remote_capability_requested_tools_for_tool parallel)
	if ! cache_path=$(zxfer_remote_capability_cache_path \
		"backup@example.com" \
		"$requested_tools"); then
		fail "Expected a cache path for remote capability caching."
	fi
	if ! cache_identity_hex=$(zxfer_remote_capability_cache_identity_hex_for_host \
		"backup@example.com" "$requested_tools"); then
		fail "Expected a remote capability cache identity for fixture metadata."
	fi
	zxfer_write_cache_object_file_atomically \
		"$cache_path" \
		"$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" \
		"created_epoch=$(date '+%s')
identity_hex=$cache_identity_hex" \
		"$(fake_remote_capability_response)" >/dev/null ||
		fail "Expected a writable remote capability cache fixture."
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_EXIT_STATUS

	result=$(zxfer_resolve_remote_required_tool "backup@example.com" parallel "GNU parallel")

	unset FAKE_SSH_EXIT_STATUS

	assertEquals "Fresh remote capability cache files should satisfy later remote helper lookups without ssh." \
		"/opt/bin/parallel" "$result"
}

test_resolve_remote_required_tool_supports_remote_cat_from_handshake() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
	export FAKE_SSH_STDOUT_OVERRIDE

	result=$(zxfer_resolve_remote_required_tool "backup@example.com" cat "cat")

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote restore-mode cat lookups should reuse the combined capability handshake." \
		"/remote/bin/cat" "$result"
}

test_resolve_remote_required_tool_rejects_unknown_tools_after_handshake() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
	export FAKE_SSH_STDOUT_OVERRIDE

	set +e
	result=$(zxfer_resolve_remote_required_tool "backup@example.com" not-a-real-tool "not-a-real-tool")
	status=$?

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Unknown remote helper lookups should fail cleanly after the handshake." 1 "$status"
	assertEquals "Unknown remote helper lookups should surface the generic query failure." \
		"Failed to query dependency \"not-a-real-tool\" on host backup@example.com." "$result"
}

test_init_variables_resolves_remote_tool_paths_and_restore_cat() {
	result=$(
		zxfer_get_os() {
			if [ "$1" = "" ]; then
				printf '%s\n' "LocalOS"
			else
				printf '%s\n' "RemoteOS"
			fi
		}
		zxfer_resolve_remote_required_tool() {
			if [ "$1:$2" = "origin.example pfexec:zfs" ]; then
				printf '%s\n' "/remote/origin/zfs"
			elif [ "$1:$2" = "target.example doas:zfs" ]; then
				printf '%s\n' "/remote/target/zfs"
			elif [ "$1:$2" = "origin.example pfexec:cat" ]; then
				printf '%s\n' "/remote/origin/cat"
			else
				return 1
			fi
		}
		g_option_z_compress=0
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_zfs="/sbin/zfs"
		g_option_O_origin_host="origin.example pfexec"
		g_option_O_origin_host_safe="'origin.example' 'pfexec'"
		g_option_T_target_host="target.example doas"
		g_option_T_target_host_safe="'target.example' 'doas'"
		g_option_e_restore_property_mode=1
		zxfer_init_variables
		printf 'source_os=%s\n' "$g_source_operating_system"
		printf 'dest_os=%s\n' "$g_destination_operating_system"
		printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
		printf 'lzfs=%s\n' "$g_LZFS"
		printf 'rzfs=%s\n' "$g_RZFS"
		printf 'cat=%s\n' "$g_cmd_cat"
	)

	assertContains "Origin OS should be populated from remote zxfer_get_os()." "$result" "source_os=RemoteOS"
	assertContains "Destination OS should be populated from remote zxfer_get_os()." "$result" "dest_os=RemoteOS"
	assertContains "Origin zfs path should use the remote lookup result." "$result" "origin_zfs=/remote/origin/zfs"
	assertContains "Target zfs path should use the remote lookup result." "$result" "target_zfs=/remote/target/zfs"
	assertContains "g_LZFS should track the resolved remote origin zfs path." "$result" "lzfs=/remote/origin/zfs"
	assertContains "g_RZFS should track the resolved remote target zfs path." "$result" "rzfs=/remote/target/zfs"
	assertContains "Restore mode should resolve cat on the origin host." "$result" "cat=/remote/origin/cat"
}

test_init_variables_passes_explicit_profile_sides_when_origin_and_target_match() {
	log_file="$TEST_TMPDIR/init_variables_profile_sides.log"
	: >"$log_file"

	(
		zxfer_get_os() {
			printf 'os:%s:%s\n' "$1" "${2:-}" >>"$log_file"
			printf '%s\n' "RemoteOS"
		}
		zxfer_resolve_remote_required_tool() {
			printf 'tool:%s:%s:%s:%s\n' "$1" "$2" "$3" "${4:-}" >>"$log_file"
			case "$2" in
			zfs)
				printf '%s\n' "/remote/$2"
				;;
			cat)
				printf '%s\n' "/remote/$2"
				;;
			esac
		}
		g_option_z_compress=0
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_zfs="/sbin/zfs"
		g_option_O_origin_host="shared.example"
		g_option_O_origin_host_safe="'shared.example'"
		g_option_T_target_host="shared.example"
		g_option_T_target_host_safe="'shared.example'"
		g_option_e_restore_property_mode=1
		zxfer_init_variables
	)

	result=$(cat "$log_file")
	assertContains "Origin OS probes should be tagged as source-side even when origin and target share the same host spec." \
		"$result" "os:shared.example:source"
	assertContains "Target OS probes should be tagged as destination-side even when origin and target share the same host spec." \
		"$result" "os:shared.example:destination"
	assertContains "Origin zfs dependency probes should be tagged as source-side." \
		"$result" "tool:shared.example:zfs:zfs:source"
	assertContains "Target zfs dependency probes should be tagged as destination-side." \
		"$result" "tool:shared.example:zfs:zfs:destination"
	assertContains "Origin restore-metadata cat probes should be tagged as source-side." \
		"$result" "tool:shared.example:cat:cat:source"
}

test_init_variables_marks_remote_zfs_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_cmd_zfs="/sbin/zfs"
			g_option_O_origin_host="origin.example"
			g_option_O_origin_host_safe="'origin.example'"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote zfs lookup failures should abort zxfer_init_variables." 1 "$status"
	assertContains "Remote zfs lookup failures should be classified as dependency errors." "$output" "class=dependency"
	assertContains "Remote zfs lookup failures should surface the lookup message." "$output" "msg=lookup failed"
}

test_ensure_parallel_remote_fetches_remote_parallel_path() {
	result_file="$TEST_TMPDIR/ensure_parallel_remote_fetch.out"
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	remote_log="$TEST_TMPDIR/remote_parallel_probe.log"
	socket_path="$TEST_TMPDIR/origin.sock"
	: >"$remote_log"
	: >"$socket_path"

	(
		zxfer_get_remote_resolved_tool_version_output() {
			printf '%s\n' "GNU parallel (fake)"
		}
		g_origin_parallel_cmd=""
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_ssh_origin_control_socket="$socket_path"
		g_ssh_origin_control_socket_dir="$TEST_TMPDIR/origin.sock.d"

		FAKE_SSH_LOG="$remote_log"
		FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT

		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		{
			printf 'parallel=%s\n' "$g_origin_parallel_cmd"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
		} >"$result_file"

		unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT
	)
	status=$?

	assertEquals "Remote GNU parallel path should be detected via ssh." 0 "$status"
	assertContains "Remote GNU parallel path should be detected via ssh." "$(cat "$result_file")" "parallel=/opt/bin/parallel"
	assertEquals "ssh should force batch mode for managed remote probes." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes as the first managed transport option." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking for managed remote probes." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes as the second managed transport option." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should reuse the established control socket." "-S" "$(sed -n '5p' "$remote_log")"
	assertEquals "SSH must pass the control socket path as the next argument." "$(sed -n '2p' "$result_file" | sed 's/^socket=//')" "$(sed -n '6p' "$remote_log")"
	assertEquals "ssh should direct probes at the origin host." "$g_option_O_origin_host" "$(sed -n '7p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '8,$p' "$remote_log")
	assertContains "Remote capability discovery should execute via sh -c so wrapper host specs stay valid." "$log_line_remote_cmd" "'sh' '-c'"
	assertContains "Remote capability discovery should pin the secure PATH inside the shell probe." "$log_line_remote_cmd" "$g_zxfer_dependency_path"
	assertContains "Remote capability discovery should include the requested parallel probe." "$log_line_remote_cmd" "parallel"
	assertContains "Remote capability discovery should include uname in the combined probe." "$log_line_remote_cmd" "uname"
}

test_ensure_parallel_available_for_source_jobs_reports_remote_probe_failures() {
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_option_O_origin_host="aldo@172.16.0.4 pfexec"
	g_origin_parallel_cmd=""
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_SUPPRESS_STDOUT=1
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote parallel probe failures should abort the helper." 1 "$status"
	assertContains "Remote parallel probe failures should preserve the query failure message." \
		"$output" "Failed to query dependency \"parallel\" on host aldo@172.16.0.4 pfexec."
}

test_ensure_parallel_available_for_source_jobs_reports_missing_remote_parallel() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "Required dependency \"parallel\" not found on host origin.example in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_j_jobs=4
			g_cmd_parallel="$FAKE_PARALLEL_BIN"
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""
			zxfer_ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Missing remote parallel should abort the helper." 1 "$status"
	assertContains "Missing remote parallel should be translated into the user-facing guidance." \
		"$output" "parallel not found on origin host origin.example but -j 4 was requested. Install parallel remotely or rerun without -j."
}

test_read_remote_backup_file_uses_resolved_remote_cat_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_cat="/remote/bin/cat"
	remote_log="$TEST_TMPDIR/zxfer_read_remote_backup_file.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(zxfer_read_remote_backup_file "backup@example.com pfexec" "/tmp/backup.meta")
	status=$?

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote backup reads should succeed when the ssh probe succeeds." 0 "$status"
	assertEquals "Remote backup reads should forward the remote payload." "payload" "$result"
	assertEquals "Remote backup reads should force batch mode before the host token." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "Remote backup reads should pass BatchMode=yes before the host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "Remote backup reads should force strict host-key checking before the host token." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "Remote backup reads should pass StrictHostKeyChecking=yes before the host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "Remote backup reads should keep the host token separate." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Remote backup reads should keep wrapper tokens in the remote command string." "$log_line_remote_cmd" "'pfexec'"
	assertContains "Remote backup reads should use the resolved remote cat path." "$log_line_remote_cmd" "/remote/bin/cat"
	assertContains "Remote backup reads should read through the staged snapshot path after validation." "$log_line_remote_cmd" "backup.snapshot"
	assertContains "Remote backup reads should preserve the requested remote metadata path." "$log_line_remote_cmd" "/tmp/backup.meta"
}

test_read_remote_backup_file_accepts_ssh_user_owned_metadata() {
	realistic_ssh_bin="$TEST_TMPDIR/read_remote_backup_exec_ssh"
	remote_file="$TEST_TMPDIR_PHYSICAL/remote_backup.meta"
	printf '%s\n' "payload" >"$remote_file"
	chmod 600 "$remote_file"
	create_fake_ssh_join_exec_bin "$realistic_ssh_bin"
	g_cmd_ssh="$realistic_ssh_bin"
	g_cmd_cat="/bin/cat"

	result=$(zxfer_read_remote_backup_file "backup@example.com" "$remote_file")
	status=$?

	assertEquals "Remote backup reads should accept secure metadata owned by the remote ssh user." 0 "$status"
	assertEquals "Remote backup reads should pass through the payload for ssh-user-owned secure metadata." \
		"payload" "$result"
}

test_read_remote_backup_file_quotes_resolved_remote_cat_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	marker="$TEST_TMPDIR/read_remote_backup_marker"
	g_cmd_cat="/remote/bin/cat; touch $marker #"
	remote_log="$TEST_TMPDIR/read_remote_backup_quoted.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta")
	status=$?

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote backup reads should still succeed when the resolved helper path contains metacharacters." 0 "$status"
	assertEquals "payload" "$result"
	assertFalse "Resolved remote cat paths should not execute locally when rendered into the remote shell helper." \
		"[ -e '$marker' ]"
	assertEquals "Remote backup reads should force batch mode before the host token." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "Remote backup reads should pass BatchMode=yes before the host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "Remote backup reads should force strict host-key checking before the host token." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "Remote backup reads should pass StrictHostKeyChecking=yes before the host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "Remote backup reads should keep the host token separate." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "The resolved remote cat path should be quoted as one token in the remote helper script." \
		"$log_line_remote_cmd" "'/remote/bin/cat; touch $marker #'"
}

test_read_remote_backup_file_returns_missing_status_when_remote_file_is_absent() {
	set +e
	status=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 94
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/missing.meta" >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Remote backup reads should map the explicit remote missing-file status to the local missing sentinel." \
		4 "$status"
}

test_read_remote_backup_file_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/read_remote_backup_real"
	link_dir="$physical_tmpdir/read_remote_backup_link"
	backup_file="$link_dir/backup.meta"
	mkdir -p "$real_dir"
	printf '%s\n' "trusted" >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
	ln -s "$real_dir" "$link_dir"

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup reads should reject symlinked parent components before cat runs." 1 "$status"
	assertContains "Remote nested symlink reads should identify the offending path component." \
		"$output" "Refusing to use backup metadata $backup_file because path component $link_dir is a symlink."
}

test_read_remote_backup_file_rejects_root_owned_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/read_remote_backup_root_real"
	link_dir="$physical_tmpdir/read_remote_backup_root_link"
	backup_file="$link_dir/backup.meta"
	fake_bin="$physical_tmpdir/read_remote_backup_root_bin"
	mkdir -p "$real_dir" "$fake_bin"
	printf '%s\n' "trusted" >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
	ln -s "$real_dir" "$link_dir"
	cat >"$fake_bin/stat" <<'EOF'
#!/bin/sh
case "$1 $2" in
	"-c %u"|"-f %u")
		printf '0\n'
		exit 0
		;;
	"-c %a"|"-f %OLp")
		printf '600\n'
		exit 0
		;;
esac
exit 1
EOF
	cat >"$fake_bin/ls" <<'EOF'
#!/bin/sh
for last_arg do :; done
	printf '%s\n' "-rw------- 1 0 0 0 Jan  1 00:00 $last_arg"
EOF
	cat >"$fake_bin/id" <<'EOF'
#!/bin/sh
if [ "${1-}" = "-u" ]; then
	printf '1000\n'
	exit 0
fi
exit 1
EOF
	chmod +x "$fake_bin/stat" "$fake_bin/ls" "$fake_bin/id"

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				PATH="$fake_bin:$PATH" sh -c "$2"
			}
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup reads should reject nested symlink components even when remote ownership probes report a secure root-owned path." 1 "$status"
	assertContains "Root-owned nested symlink reads should still identify the offending path component." \
		"$output" "Refusing to use backup metadata $backup_file because path component $link_dir is a symlink."
}

test_read_command_line_switches_skips_control_socket_when_ssh_lacks_support() {
	remote_log="$TEST_TMPDIR/unsupported_control_socket.log"
	result_file="$TEST_TMPDIR/unsupported_control_socket.out"
	stderr_file="$TEST_TMPDIR/unsupported_control_socket.err"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		: >"$remote_log"
		FAKE_SSH_LOG="$remote_log"
		export FAKE_SSH_LOG
		OPTIND=1
		g_option_z_compress=0
		g_cmd_compress="zstd -3"
		g_cmd_decompress="zstd -d"
		g_option_O_origin_host=""
		g_option_O_origin_host_safe=""
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_cmd_zfs="/sbin/zfs"
		g_ssh_supports_control_sockets=0
		g_ssh_origin_control_socket=""
		g_ssh_origin_control_socket_dir=""
		zxfer_read_command_line_switches -O "backup@example.com"
		printf 'origin=%s\n' "$g_option_O_origin_host"
		printf 'socket=%s\n' "$g_ssh_origin_control_socket"
		printf 'lzfs=%s\n' "$g_LZFS"
	) >"$result_file" 2>"$stderr_file"
	status=$?

	result=$(cat "$result_file")
	assertNotEquals "Skipping unsupported control sockets should still leave observable parser state." "" "$result"
	assertEquals "Unsupported ssh clients should not be asked to create control sockets." "" "$(cat "$remote_log")"
	assertEquals "Parsing should not emit stderr noise when multiplexing is unavailable." "" "$(cat "$stderr_file")"
	assertContains "$result" "origin=backup@example.com"
	assertContains "$result" "socket="
	assertContains "$result" "lzfs=/sbin/zfs"
}

test_remote_snapshot_listing_pipeline_handles_cli_flow() {
	g_option_j_jobs=4
	g_option_z_compress=1
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_cmd_zfs="/usr/sbin/zfs"
	g_origin_cmd_zfs="$g_cmd_zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	g_initial_source="zroot"

	FAKE_SSH_SUPPRESS_STDOUT=1 zxfer_setup_ssh_control_socket "$g_option_O_origin_host" "origin"
	unset FAKE_SSH_SUPPRESS_STDOUT

	fake_zstd="$TEST_TMPDIR/zstd"
	create_passthrough_zstd "$fake_zstd"
	g_cmd_decompress_safe="'$fake_zstd' '-d'"
	g_origin_cmd_compress_safe="'$fake_zstd' '-9'"

	l_cmd=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/opt/bin/parallel"
			}
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "GNU parallel (fake)"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)

	remote_log="$TEST_TMPDIR/remote_snapshot_list.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT

	eval "$l_cmd" >"$TEST_TMPDIR/source_snapshot_list.log"
	status=$?

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Remote snapshot listing pipeline should execute without syntax errors." 0 "$status"
	assertEquals "payload" "$(cat "$TEST_TMPDIR/source_snapshot_list.log")"
	assertEquals "ssh should force batch mode for managed snapshot-listing pipelines." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes to the snapshot-listing transport." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking for managed snapshot-listing pipelines." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes to the snapshot-listing transport." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should reuse the established control socket." "-S" "$(sed -n '5p' "$remote_log")"
	assertEquals "SSH must pass the control socket path as the next argument." "$g_ssh_origin_control_socket" "$(sed -n '6p' "$remote_log")"
	assertEquals "ssh should connect to the requested origin host." "$g_option_O_origin_host" "$(sed -n '7p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '8p' "$remote_log")
	assertContains "Remote command should force the remote pipeline through sh -c." "$log_line_remote_cmd" "'sh' '-c'"
	assertContains "Remote command should include the source dataset path." "$log_line_remote_cmd" "zroot"
	assertContains "Remote command should include the dataset listing helper." "$log_line_remote_cmd" "/usr/sbin/zfs"
	assertContains "Remote command should include GNU parallel." "$log_line_remote_cmd" "/opt/bin/parallel"
	assertContains "Remote command should preserve the parallel job count." "$log_line_remote_cmd" "-j 4 --line-buffer"
	assertContains "Remote command should preserve the per-dataset snapshot placeholder." "$log_line_remote_cmd" "{}"
	assertContains "Remote metadata discovery should keep the compressor helper in the rendered ssh pipeline." "$log_line_remote_cmd" "$fake_zstd"
}

test_remote_snapshot_listing_pipeline_executes_parallel_runner_for_each_dataset() {
	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_join_exec_pipeline"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_pipeline.log"
	fake_remote_zfs="$TEST_TMPDIR/fake_remote_zfs_exec"
	fake_parallel="$TEST_TMPDIR/fake_parallel_exec"
	fake_zstd="$TEST_TMPDIR/zstd"

	create_fake_ssh_join_exec_bin "$realistic_ssh_bin"
	create_fake_parallel_exec_bin "$fake_parallel"
	create_passthrough_zstd "$fake_zstd"
	cat >"$fake_remote_zfs" <<'EOF'
#!/bin/sh
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-t" ] && [ "$4" = "filesystem,volume" ] &&
	[ "$5" = "-o" ] && [ "$6" = "name" ] && [ "$7" = "zroot" ]; then
	printf '%s\n' "zroot"
	printf '%s\n' "zroot/usr"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot" ]; then
	printf '%s\n' "zroot@snap1"
	printf '%s\n' "zroot@snap2"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot/usr" ]; then
	printf '%s\n' "zroot/usr@snap1"
	exit 0
fi
printf 'unexpected argv:' >&2
printf ' [%s]' "$@" >&2
printf '\n' >&2
exit 64
EOF
	chmod +x "$fake_remote_zfs"

	g_option_j_jobs=2
	g_option_z_compress=1
	g_cmd_parallel="$fake_parallel"
	g_origin_parallel_cmd="$fake_parallel"
	g_cmd_zfs="$fake_remote_zfs"
	g_origin_cmd_zfs="$fake_remote_zfs"
	g_cmd_decompress_safe="'$fake_zstd' '-d'"
	g_origin_cmd_compress_safe="'$fake_zstd' '-9'"
	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	g_initial_source="zroot"

	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	l_cmd=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "$fake_parallel"
			}
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "GNU parallel (fake)"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)
	eval "$l_cmd" >"$TEST_TMPDIR/remote_snapshot_exec.out" 2>"$TEST_TMPDIR/remote_snapshot_exec.err"
	status=$?

	unset FAKE_SSH_LOG
	PATH=$old_path

	assertEquals "Remote snapshot listing should execute the GNU parallel runner without malformed zfs argv." 0 "$status"
	assertEquals "The executed remote pipeline should return all source snapshots." \
		"zroot@snap1
zroot@snap2
zroot/usr@snap1" "$(cat "$TEST_TMPDIR/remote_snapshot_exec.out")"
	assertEquals "The executed remote pipeline should not emit zfs usage or malformed-argv errors." \
		"" "$(cat "$TEST_TMPDIR/remote_snapshot_exec.err")"
}

test_local_snapshot_listing_pipeline_executes_direct_parallel_runner_for_each_dataset() {
	fake_local_zfs="$TEST_TMPDIR/fake_local_zfs_exec"
	fake_parallel="$TEST_TMPDIR/fake_parallel_exec_local"

	create_fake_parallel_exec_bin "$fake_parallel"
	cat >"$fake_local_zfs" <<'EOF'
#!/bin/sh
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-t" ] && [ "$4" = "filesystem,volume" ] &&
	[ "$5" = "-o" ] && [ "$6" = "name" ] && [ "$7" = "tank/home" ]; then
	printf '%s\n' "tank/home"
	printf '%s\n' "tank/home/usr"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "tank/home" ]; then
	printf '%s\n' "tank/home@snap1"
	printf '%s\n' "tank/home@snap2"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "tank/home/usr" ]; then
	printf '%s\n' "tank/home/usr@snap1"
	exit 0
fi
	printf 'unexpected argv:' >&2
	printf ' [%s]' "$@" >&2
	printf '\n' >&2
	exit 64
EOF
	chmod +x "$fake_local_zfs"

	g_option_j_jobs=2
	g_option_z_compress=0
	g_cmd_parallel="$fake_parallel"
	g_cmd_zfs="$fake_local_zfs"
	g_LZFS="$fake_local_zfs"
	g_initial_source="tank/home"

	l_cmd=$(
		(
			zxfer_build_source_snapshot_list_cmd
		)
	)
	eval "$l_cmd" >"$TEST_TMPDIR/local_snapshot_exec.out" 2>"$TEST_TMPDIR/local_snapshot_exec.err"
	status=$?

	assertEquals "Local snapshot listing should execute the GNU parallel runner without malformed zfs argv." 0 "$status"
	assertEquals "The executed local pipeline should return all source snapshots." \
		"tank/home@snap1
tank/home@snap2
tank/home/usr@snap1" "$(cat "$TEST_TMPDIR/local_snapshot_exec.out")"
	assertEquals "The executed local pipeline should not emit zfs usage or malformed-argv errors." \
		"" "$(cat "$TEST_TMPDIR/local_snapshot_exec.err")"
}

test_remote_snapshot_listing_pipeline_handles_csh_remote_shell() {
	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_join_csh_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_join_csh_exec.log"
	fake_remote_zfs="$TEST_TMPDIR/fake_remote_zfs"
	fake_zstd="$TEST_TMPDIR/zstd"
	l_csh_shell=$(find_csh_shell_for_tests)

	if [ "$l_csh_shell" = "" ]; then
		return 0
	fi

	create_fake_ssh_join_csh_exec_bin "$realistic_ssh_bin" "$l_csh_shell"
	create_passthrough_zstd "$fake_zstd"
	cat >"$fake_remote_zfs" <<'EOF'
#!/bin/sh
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-t" ] && [ "$4" = "filesystem,volume" ] &&
	[ "$5" = "-o" ] && [ "$6" = "name" ] && [ "$7" = "zroot" ]; then
	printf '%s\n' "zroot"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot" ]; then
	printf '%s\n' "zroot@snap1"
	exit 0
fi
exit 0
EOF
	chmod +x "$fake_remote_zfs"

	g_option_j_jobs=4
	g_option_z_compress=1
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="$fake_remote_zfs"
	g_origin_cmd_zfs="$fake_remote_zfs"
	g_cmd_decompress_safe="'$fake_zstd' '-d'"
	g_origin_cmd_compress_safe="'$fake_zstd' '-9'"
	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	g_initial_source="zroot"

	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	l_cmd=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "$FAKE_PARALLEL_BIN"
			}
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "GNU parallel (fake)"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)
	eval "$l_cmd" >"$TEST_TMPDIR/remote_snapshot_csh.out" 2>"$TEST_TMPDIR/remote_snapshot_csh.err"
	status=$?

	unset FAKE_SSH_LOG
	PATH=$old_path

	assertEquals "Remote snapshot listing should succeed even when ssh routes through csh on the origin host." 0 "$status"
	assertNotContains "The csh-backed ssh emulation should not report unmatched-quote syntax errors." \
		"$(cat "$TEST_TMPDIR/remote_snapshot_csh.err")" "Unmatched"
	assertContains "The csh-backed ssh emulation should receive a remote sh -c wrapper." \
		"$(cat "$realistic_ssh_log")" "'sh' '-c'"
}

test_normalize_destination_snapshot_list_maps_destination_prefix_to_source() {
	input_file="$TEST_TMPDIR/dest_snaps.txt"
	output_file="$TEST_TMPDIR/normalized_snaps.txt"
	cat <<'EOF' >"$input_file"
tank/backup/app@snap2
tank/backup/app@snap1
EOF
	g_initial_source="tank/src/app"
	g_initial_source_had_trailing_slash=0

	zxfer_normalize_destination_snapshot_list "tank/backup/app" "$input_file" "$output_file"

	result=$(cat "$output_file")
	expected="tank/src/app@snap1
tank/src/app@snap2"
	assertEquals "Destination snapshot paths should be rewritten to match the source dataset." "$expected" "$result"
}

test_normalize_destination_snapshot_list_keeps_dataset_when_trailing_slash_requested() {
	input_file="$TEST_TMPDIR/dest_snaps_trailing.txt"
	output_file="$TEST_TMPDIR/normalized_snaps_trailing.txt"
	cat <<'EOF' >"$input_file"
tank/dst@snapB
tank/dst@snapA
EOF
	g_initial_source="tank/dst"
	g_initial_source_had_trailing_slash=1

	zxfer_normalize_destination_snapshot_list "tank/dst" "$input_file" "$output_file"

	result=$(cat "$output_file")
	expected="tank/dst@snapA
tank/dst@snapB"
	assertEquals "Trailing slash semantics should only sort the destination list." "$expected" "$result"
}

test_diff_snapshot_lists_supports_source_and_destination_modes() {
	source_file="$TEST_TMPDIR/source_snaps.txt"
	dest_file="$TEST_TMPDIR/dest_snaps_diff.txt"
	cat <<'EOF' >"$source_file"
pool/src@app
pool/src@bpp
pool/src@cpp
EOF
	cat <<'EOF' >"$dest_file"
pool/src@app
pool/src@cpp
pool/src@dpp
EOF

	result_missing=$(zxfer_diff_snapshot_lists "$source_file" "$dest_file" "source_minus_destination")
	assertEquals "pool/src@bpp" "$result_missing"

	result_extra=$(zxfer_diff_snapshot_lists "$source_file" "$dest_file" "destination_minus_source")
	assertEquals "pool/src@dpp" "$result_extra"
}

test_get_last_common_snapshot_requires_matching_guid() {
	l_source_snaps=$(
		cat <<'EOF'
tank/doET/tank@zxfer_2	222
tank/doET/tank@zxfer_1	111
EOF
	)
	l_dest_snaps=$(
		cat <<'EOF'
tank/backups/nucbackup/tank/doET/tank@zxfer_2	999
tank/backups/nucbackup/tank/doET/tank@zxfer_1	111
EOF
	)

	result=$(zxfer_get_last_common_snapshot "$l_source_snaps" "$l_dest_snaps")

	assertEquals "Common-snapshot detection should require matching guid identity, not just snapshot name." \
		"tank/doET/tank@zxfer_1	111" "$result"
}

test_get_last_common_snapshot_returns_empty_when_no_snapshot_match() {
	# If the destination never reported the snapshot name, the helper must
	# return an empty string so zxfer performs a full send.
	l_source_snaps="tank/doET/tank@zxfer_2
tank/doET/tank@zxfer_1"
	l_dest_snaps="tank/backups/nucbackup/tank/doET/tank@zxfer_3"

	result=$(zxfer_get_last_common_snapshot "$l_source_snaps" "$l_dest_snaps")

	assertEquals "" "$result"
}

test_inspect_delete_snap_filters_exact_dataset_matches() {
	g_option_d_delete_destination_snapshots=0
	g_delete_source_tmp_file=$(mktemp -t zxfer_src.XXXXXX)
	g_delete_dest_tmp_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_delete_snapshots_to_delete_tmp_file=$(mktemp -t zxfer_diff.XXXXXX)
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/zfsbackup/doCGA/tank@zxfer_30473_20251114214157
tank/zfsbackup/doET/tank@zxfer_98767_20251117000001
tank/zfsbackup/doET/tank@zxfer_30473_20251114214157
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
tank/backups/nucbackup/tank/zfsbackup/doCGA/tank@zxfer_30473_20251114214157
tank/backups/nucbackup/tank/zfsbackup/doET/tank@zxfer_30473_20251114214157
EOF
	)
	g_actual_dest="tank/backups/nucbackup/tank/zfsbackup/doET/tank"

	zxfer_get_snapshot_identity_records_for_dataset() {
		case "$1:$2" in
		source:tank/zfsbackup/doET/tank)
			printf '%s\n' \
				"tank/zfsbackup/doET/tank@zxfer_98767_20251117000001" \
				"tank/zfsbackup/doET/tank@zxfer_30473_20251114214157"
			;;
		destination:tank/backups/nucbackup/tank/zfsbackup/doET/tank)
			printf '%s\n' "tank/backups/nucbackup/tank/zfsbackup/doET/tank@zxfer_30473_20251114214157"
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_inspect_delete_snap 0 "tank/zfsbackup/doET/tank"

	assertEquals "tank/zfsbackup/doET/tank@zxfer_30473_20251114214157" "$g_last_common_snap"
	unset -f zxfer_get_snapshot_identity_records_for_dataset
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
}

test_get_dest_snapshots_to_delete_per_dataset_returns_extra_dest_entries() {
	g_delete_source_tmp_file=$(mktemp -t zxfer_src.XXXXXX)
	g_delete_dest_tmp_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_delete_snapshots_to_delete_tmp_file=$(mktemp -t zxfer_diff.XXXXXX)
	source_list=$(printf '%s\n%s' "tank/fs@s1" "tank/fs@s2")
	dest_list=$(printf '%s\n%s' "tank/fs@s1" "tank/fs@s3")
	result=$(zxfer_get_dest_snapshots_to_delete_per_dataset "$source_list" "$dest_list")
	assertEquals "tank/fs@s3" "$result"
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
}

test_get_dest_snapshots_to_delete_per_dataset_treats_guid_mismatches_as_extra() {
	g_delete_source_tmp_file=$(mktemp -t zxfer_src.XXXXXX)
	g_delete_dest_tmp_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_delete_snapshots_to_delete_tmp_file=$(mktemp -t zxfer_diff.XXXXXX)
	source_list=$(
		cat <<'EOF'
tank/fs@s1	111
EOF
	)
	dest_list=$(
		cat <<'EOF'
tank/fs@s1	999
EOF
	)
	result=$(zxfer_get_dest_snapshots_to_delete_per_dataset "$source_list" "$dest_list")
	assertEquals "Same-named destination snapshots with a different guid should be treated as divergent extras." \
		"tank/fs@s1" "$result"
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
}

test_set_src_snapshot_transfer_list_collects_newer_snapshots() {
	g_last_common_snap="tank/fs@snap1"
	zxfer_set_src_snapshot_transfer_list "tank/fs@snap3 tank/fs@snap2 tank/fs@snap1" "tank/fs"
	expected=$(printf '%s\n%s' "tank/fs@snap2" "tank/fs@snap3")
	assertEquals "$expected" "$g_src_snapshot_transfer_list"
}

test_delete_snaps_invokes_destroy_for_missing_snapshots() {
	log="$TEST_TMPDIR/delete_snap_cmd.log"
	g_delete_source_tmp_file=$(mktemp -t zxfer_src.XXXXXX)
	g_delete_dest_tmp_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_delete_snapshots_to_delete_tmp_file=$(mktemp -t zxfer_diff.XXXXXX)
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	(
		g_RZFS="/sbin/zfs"
		g_option_n_dryrun=0
		zxfer_run_destination_zfs_cmd() { printf '%s %s %s\n' "$g_RZFS" "$1" "$2" >"$log"; }
		zxfer_delete_snaps "$source_list" "$dest_list"
	)
	result=$(cat "$log")
	assertEquals "/sbin/zfs destroy tank/fs@snap3" "$result"
	rm -f "$log" "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
}

test_grandfather_test_allows_young_snapshots() {
	g_option_g_grandfather_protection=30
	current=$(date +%s)
	old=$((current - 3 * 86400))
	result=$(
		zxfer_run_destination_zfs_cmd() {
			if [ "$5" = "-p" ]; then
				printf '%s\n' "$old"
			else
				printf '%s\n' "Mon Jan  1 00:00:00 UTC 2024"
			fi
		}
		zxfer_grandfather_test "tank/fs@snap"
		echo "ok"
	)
	assertEquals "ok" "$result"
}

test_grandfather_test_blocks_old_snapshots() {
	g_option_g_grandfather_protection=1
	current=$(date +%s)
	very_old=$((current - 10 * 86400))
	set +e
	ZXFER_TEST_VERY_OLD=$very_old ZXFER_TEST_ROOT=$ZXFER_ROOT /bin/sh <<'EOF' >/dev/null 2>&1
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_snapshot_reconcile.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_g_grandfather_protection=1
zxfer_throw_usage_error() {
	echo "grandfather:$1"
	exit 2
}
zxfer_run_destination_zfs_cmd() {
	if [ "$5" = "-p" ]; then
		printf '%s\n' "$ZXFER_TEST_VERY_OLD"
	else
		printf '%s\n' "Sun Jan  1 00:00:00 UTC 2023"
	fi
}
zxfer_grandfather_test "tank/fs@ancient"
EOF
	status=$?
	assertEquals "Grandfather violations should exit with status 2." 2 "$status"
}

test_remove_sources_strips_source_suffix() {
	l_oldifs=$IFS
	IFS=","
	zxfer_remove_sources "compression=lz4=local,atime=off=override"
	IFS=$l_oldifs
	assertEquals "compression=lz4,atime=off" "$g_zxfer_new_rmvs_pv"
}

test_remove_properties_drops_requested_entries() {
	l_oldifs=$IFS
	IFS=","
	zxfer_remove_properties "compression=lz4=local,atime=off=local" "atime"
	IFS=$l_oldifs
	assertEquals "compression=lz4=local" "$g_zxfer_new_rmv_pvs"
}

test_resolve_human_vars_prefers_human_overrides() {
	zxfer_resolve_human_vars "compression=lz4=local,atime=on=local" "compression=lz4,atime=none"
	assertEquals "compression=lz4=local,atime=none=local" "$human_results"
}

test_validate_override_properties_rejects_unknown_property() {
	set +e
	ZXFER_TEST_ROOT=$ZXFER_ROOT /bin/sh <<'EOF' >/dev/null 2>&1
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
zxfer_throw_usage_error() {
	echo "invalid"
	exit 2
}
zxfer_validate_override_properties "copies=2" "compression=lz4=local"
EOF
	status=$?
	assertEquals "Unknown overrides should raise a usage error." 2 "$status"
}

test_validate_override_properties_accepts_known_property() {
	zxfer_validate_override_properties "compression=lz4" "compression=lz4=local"
	assertEquals 0 "$?"
}

test_derive_override_lists_with_transfer_all_preserves_sources() {
	result=$(zxfer_derive_override_lists "compression=lz4=local,atime=off=local" "compression=lz4" 1 filesystem)
	{
		IFS= read -r override_line
		IFS= read -r creation_line
	} <<EOF
$result
EOF
	assertEquals "compression=lz4=override,atime=off=local" "$override_line"
	assertEquals "compression=lz4=override,atime=off=local" "$creation_line"
}

test_derive_override_lists_without_transfer_all_uses_overrides_only() {
	result=$(zxfer_derive_override_lists "compression=lz4=local" "atime=off" 0 filesystem)
	{
		IFS= read -r override_line
		IFS= read -r creation_line
	} <<EOF
$result
EOF
	assertEquals "atime=off=override" "$override_line"
	assertEquals "atime=off=override" "$creation_line"
}

test_sanitize_property_list_removes_readonly_and_ignored_sets() {
	list="compression=lz4=local,atime=off=local"
	readonly="compression"
	ignore="atime"
	result=$(zxfer_sanitize_property_list "$list" "$readonly" "$ignore")
	assertEquals "" "$result"
}

test_strip_unsupported_properties_removes_matching_entries() {
	result=$(zxfer_strip_unsupported_properties "compression=lz4=local,checksum=sha256=local" "checksum")
	assertEquals "compression=lz4=local" "$result"
}

test_diff_properties_returns_expected_set_and_inherit_lists() {
	result=$(zxfer_diff_properties "compression=lz4=local,atime=off=received" "compression=lz4=local,atime=on=local" "")
	{
		IFS= read -r init_list
		IFS= read -r set_list
		IFS= read -r inherit_list
	} <<EOF
$result
EOF
	assertEquals "atime=off" "$init_list"
	assertEquals "" "$set_list"
	assertEquals "atime=off" "$inherit_list"
}

test_apply_property_changes_uses_initial_set_list_for_root_dataset() {
	log="$TEST_TMPDIR/property_apply_initial.log"
	PROPERTY_LOG="$log" zxfer_apply_property_changes "tank/dst" 1 "compression=lz4,atime=off" "copies=2" "checksum" property_set_logger property_inherit_logger
	result=$(cat "$log")
	expected="set compression=lz4,atime=off tank/dst"
	assertEquals "$expected" "$result"
	rm -f "$log"
}

test_apply_property_changes_sets_and_inherits_on_children() {
	log="$TEST_TMPDIR/property_apply_child.log"
	PROPERTY_LOG="$log" zxfer_apply_property_changes "tank/dst/child" 0 "compression=lz4" "atime=off" "encryption" property_set_logger property_inherit_logger
	result=$(cat "$log")
	expected="set atime=off tank/dst/child
inherit encryption tank/dst/child"
	assertEquals "$expected" "$result"
	rm -f "$log"
}

test_write_destination_snapshot_list_to_files_normalizes_destination_path() {
	full_file="$TEST_TMPDIR/dest_snapshots.txt"
	norm_file="$TEST_TMPDIR/dest_snapshots_normalized.txt"
	# shellcheck disable=SC2030,SC2031
	(
		g_initial_source="tank/src"
		g_destination="backup/dst"
		g_initial_source_had_trailing_slash=0
		g_RZFS="$TEST_TMPDIR/fake_rzfs"
		cat >"$g_RZFS" <<'EOF'
#!/bin/sh
cat <<'DATA'
backup/dst/src@snapA
backup/dst/src@snapB
DATA
EOF
		chmod +x "$g_RZFS"
		zxfer_exists_destination() { echo 1; }
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	)
	result=$(cat "$norm_file")
	expected="tank/src@snapA
tank/src@snapB"
	assertEquals "Destination snapshots should be rewritten to match the source prefix." "$expected" "$result"
}

test_set_g_recursive_source_list_updates_dataset_caches() {
	source_tmp=$(mktemp -t zxfer_srcsnap.XXXXXX)
	dest_tmp=$(mktemp -t zxfer_dstsnap.XXXXXX)
	cat <<'EOF' >"$source_tmp"
tank/src@a
tank/src@b
tank/src/child@a
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@a
EOF
	g_cmd_awk=${g_cmd_awk:-$(command -v awk)}
	g_option_x_exclude_datasets=""
	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
	expected_list=$(printf '%s\n%s' "tank/src" "tank/src/child")
	assertEquals "Missing datasets should be identified for replication." "$expected_list" "$g_recursive_source_list"
	expected_datasets=$(printf '%s\n%s' "tank/src" "tank/src/child")
	assertEquals "Dataset cache should include every source filesystem." "$expected_datasets" "$g_recursive_source_dataset_list"
	rm -f "$source_tmp" "$dest_tmp"
}

test_calculate_size_estimate_uses_incremental_send_probe() {
	result=$(
		zxfer_run_source_zfs_cmd() { printf 'size\t2048\n'; }
		zxfer_calculate_size_estimate "tank/fs@snap2" "tank/fs@snap1"
	)
	assertEquals "2048" "$result"
}

test_calculate_size_estimate_handles_full_send_estimate() {
	result=$(
		zxfer_run_source_zfs_cmd() { printf 'size\t1024\n'; }
		zxfer_calculate_size_estimate "tank/fs@snap1" ""
	)
	assertEquals "1024" "$result"
}

test_setup_progress_dialog_substitutes_placeholders() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	result=$(zxfer_setup_progress_dialog 4096 "tank/fs@snap")
	assertEquals "pv -s 4096 -N tank/fs@snap" "$result"
}

test_wrap_command_with_ssh_without_compression_quotes_command() {
	result=$(
		g_cmd_ssh="/usr/bin/ssh"
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "backup@example.com" 0 send
	)
	assertEquals "'/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup@example.com' 'zfs send tank/src@snap'" "$result"
}

test_wrap_command_with_ssh_streams_compression_on_send() {
	result=$(
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_compress_safe="gzip"
		g_cmd_decompress_safe="gunzip"
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "backup" 1 send
	)
	assertEquals "'/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup' 'zfs send tank/src@snap | gzip' | gunzip" "$result"
}

test_get_send_command_generates_incremental_streams_with_flags() {
	g_cmd_zfs="/sbin/zfs"
	g_option_V_very_verbose=1
	g_option_w_raw_send=1
	result=$(zxfer_get_send_command "tank/fs@snap1" "tank/fs@snap2")
	assertEquals "/sbin/zfs send -v -w -I tank/fs@snap1 tank/fs@snap2" "$result"
}

test_get_send_command_emits_full_stream_when_no_common_snapshot() {
	g_cmd_zfs="/sbin/zfs"
	g_option_V_very_verbose=0
	g_option_w_raw_send=0
	result=$(zxfer_get_send_command "" "tank/fs@snap1")
	assertEquals "/sbin/zfs send   tank/fs@snap1" "$result"
}

test_get_receive_command_honors_force_flag() {
	g_cmd_zfs="/sbin/zfs"
	g_option_F_force_rollback="-F"
	result=$(zxfer_get_receive_command "tank/dst")
	assertEquals "/sbin/zfs receive -F tank/dst" "$result"
}

test_wait_for_zfs_send_jobs_clears_pid_list_on_success() {
	sleep 0.05 &
	pid1=$!
	sleep 0.05 &
	pid2=$!
	g_zfs_send_job_pids="$pid1 $pid2"
	g_count_zfs_send_jobs=2
	zxfer_wait_for_zfs_send_jobs "unit"
	assertEquals "" "$g_zfs_send_job_pids"
	assertEquals 0 "$g_count_zfs_send_jobs"
}

test_wait_for_zfs_send_jobs_reports_failure() {
	(
		zxfer_throw_error() {
			echo "send failure"
			exit 1
		}
		sleep 1 &
		ok_pid=$!
		sh -c 'exit 3' &
		bad_pid=$!
		g_zfs_send_job_pids="$ok_pid $bad_pid"
		g_count_zfs_send_jobs=2
		zxfer_wait_for_zfs_send_jobs "failure"
	) >/dev/null 2>&1
	assertEquals "Job failures should surface via zxfer_throw_error." 1 "$?"
}

test_zxfer_quote_command_argv_escapes_control_chars_and_apostrophes() {
	l_newline_arg=$(printf 'line1\nline2')

	result=$(zxfer_quote_command_argv "./zxfer" "value with space" "$l_newline_arg" "apost'rophe")
	expected="'./zxfer' 'value with space' 'line1\\nline2' 'apost'\"'\"'rophe'"

	assertEquals "Quoted argv should remain one-line and shell-safe for reports." "$expected" "$result"
}

test_zxfer_render_command_for_report_appends_quoted_argv_to_prefix() {
	result=$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs" "create" "-o" "compression=lz4")
	expected="/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4'"

	assertEquals "Report rendering should preserve the shell-ready prefix and quote appended argv tokens." \
		"$expected" "$result"
}

test_zxfer_render_command_for_report_returns_prefix_when_no_argv_are_provided() {
	result=$(zxfer_render_command_for_report "/usr/bin/ssh 'host' /sbin/zfs")

	assertEquals "Report rendering should return the prefix unchanged when no argv tokens are appended." \
		"/usr/bin/ssh 'host' /sbin/zfs" "$result"
}

test_zxfer_render_command_for_report_quotes_argv_when_prefix_is_empty() {
	result=$(zxfer_render_command_for_report "" "zfs" "list" "tank/src")

	assertEquals "Report rendering should still quote argv tokens when no shell prefix is supplied." \
		"'zfs' 'list' 'tank/src'" "$result"
}

test_zxfer_render_failure_report_includes_context_fields() {
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src"
	g_option_n_dryrun=1
	g_option_Y_yield_iterations=8
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_zxfer_original_invocation="'./zxfer' '-R' 'tank/src' 'backup/dst'"
	g_zxfer_failure_class="runtime"
	g_zxfer_failure_stage="send/receive"
	g_zxfer_failure_message="replication failed"
	g_zxfer_failure_source_root="tank/src"
	g_zxfer_failure_current_source="tank/src/child"
	g_zxfer_failure_destination_root="backup/dst"
	g_zxfer_failure_current_destination="backup/dst/child"
	g_zxfer_failure_last_command="'/sbin/zfs' 'send' 'tank/src@snap1'"

	report=$(zxfer_render_failure_report 1)

	assertContains "$report" "zxfer: failure report begin"
	assertContains "$report" "failure_stage: send/receive"
	assertContains "$report" "source_root: tank/src"
	assertContains "$report" "current_destination: backup/dst/child"
	assertContains "$report" "invocation: [redacted]"
	assertContains "$report" "last_command: [redacted]"
	assertContains "$report" "zxfer: failure report end"
}

test_zxfer_usage_error_failure_report_redacts_invocation_by_default() {
	secure_path_dir="$TEST_TMPDIR/usage_redaction_secure_path"
	stdout_file="$TEST_TMPDIR/usage_redaction.stdout"
	stderr_file="$TEST_TMPDIR/usage_redaction.stderr"
	secret_source="tank/secret-source"

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -R "$secret_source" >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Usage-error launcher runs should still exit with usage status when failure-report command redaction is enabled by default." \
		2 "$status"
	assertContains "Default failure-report command redaction should replace the launcher-captured invocation in stderr." \
		"$(cat "$stderr_file")" "invocation: [redacted]"
	assertNotContains "Default failure-report command redaction should keep secret-bearing usage arguments out of stderr." \
		"$(cat "$stderr_file")" "$secret_source"
}

test_zxfer_usage_error_failure_report_escapes_control_bytes_in_invocation_in_unsafe_mode() {
	secure_path_dir="$TEST_TMPDIR/usage_escape_secure_path"
	stdout_file="$TEST_TMPDIR/usage_escape.stdout"
	stderr_file="$TEST_TMPDIR/usage_escape.stderr"
	esc=$(printf '\033')
	bell=$(printf '\007')
	control_source=$(printf 'tank/ctrl%s[31m%s' "$esc" "$bell")

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "$control_source" >"$stdout_file" 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/ctrl\\x1B[31m\\x07'" "$stderr_file" >/dev/null 2>&1
	escaped_esc_status=$?
	grep -F "\\\\x1B" "$stderr_file" >/dev/null 2>&1
	double_esc_status=$?
	grep -F "\\x07" "$stderr_file" >/dev/null 2>&1
	escaped_bell_status=$?
	grep -F "$esc" "$stderr_file" >/dev/null 2>&1
	raw_esc_status=$?
	grep -F "$bell" "$stderr_file" >/dev/null 2>&1
	raw_bell_status=$?

	assertEquals "Unsafe usage-error launcher runs should still exit with usage status when invocation control bytes are escaped." \
		2 "$status"
	assertEquals "Unsafe failure reports should render ESC bytes from the launcher-captured invocation as escaped text." \
		0 "$escaped_esc_status"
	assertEquals "Unsafe failure reports should not double-escape control-byte markers from the launcher-captured invocation." \
		1 "$double_esc_status"
	assertEquals "Unsafe failure reports should render BEL bytes from the launcher-captured invocation as escaped text." \
		0 "$escaped_bell_status"
	assertEquals "Unsafe failure reports should not contain raw ESC bytes from the launcher-captured invocation." \
		1 "$raw_esc_status"
	assertEquals "Unsafe failure reports should not contain raw BEL bytes from the launcher-captured invocation." \
		1 "$raw_bell_status"
}

test_zxfer_usage_error_failure_report_preserves_trailing_newline_in_invocation_in_unsafe_mode() {
	secure_path_dir="$TEST_TMPDIR/usage_trailing_newline_secure_path"
	stdout_file="$TEST_TMPDIR/usage_trailing_newline.stdout"
	stderr_file="$TEST_TMPDIR/usage_trailing_newline.stderr"
	trailing_source=$(printf 'tank/trailing-source\n_')
	trailing_source=${trailing_source%_}

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "$trailing_source" >"$stdout_file" 2>"$stderr_file"
	status=$?
	grep -F -x "invocation: '$ZXFER_ROOT/zxfer' '-R' 'tank/trailing-source\\n'" "$stderr_file" >/dev/null 2>&1
	trailing_newline_status=$?

	assertEquals "Unsafe usage-error launcher runs should still exit with usage status when invocation newline markers are preserved." \
		2 "$status"
	assertEquals "Unsafe failure reports should preserve trailing newline markers from the launcher-captured invocation." \
		0 "$trailing_newline_status"
}

test_zxfer_render_failure_report_omits_empty_optional_fields() {
	g_zxfer_version="test-version"
	g_zxfer_failure_class="runtime"
	g_zxfer_failure_stage="snapshot discovery"
	g_zxfer_failure_message="missing snapshot"

	report=$(zxfer_render_failure_report 3)

	assertContains "$report" "failure_stage: snapshot discovery"
	assertNotContains "$report" "current_source:"
	assertNotContains "$report" "current_destination:"
	assertNotContains "$report" "invocation:"
}

test_zxfer_render_failure_report_defaults_runtime_class_for_nonusage_exit() {
	report_file="$TEST_TMPDIR/runtime_default.report"
	zxfer_reset_failure_context "unit-test"
	g_zxfer_failure_class=""
	g_zxfer_failure_message=""
	g_zxfer_failure_stage=""

	zxfer_render_failure_report 1 >"$report_file"

	report=$(cat "$report_file")
	assertContains "Non-usage exits should default to runtime failures." \
		"$report" "failure_class: runtime"
	assertContains "Missing failure messages should fall back to the exit status summary." \
		"$report" "message: zxfer exited with status 1."
}

test_zxfer_render_failure_defaults_cover_usage_mode() {
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive="tank/src"
	zxfer_reset_failure_context "unit"

	report=$(zxfer_render_failure_report 2)

	assertContains "Failure reports should default exit status 2 to usage errors." \
		"$report" "failure_class: usage"
	assertContains "Failure reports should default missing messages to the exit-status text." \
		"$report" "message: zxfer exited with status 2."
	assertContains "Failure reports should identify nonrecursive mode when -N is set." \
		"$report" "mode: nonrecursive"
}

test_throw_error_writes_message_to_stderr() {
	stdout_file="$TEST_TMPDIR/zxfer_throw_error.stdout"
	stderr_file="$TEST_TMPDIR/zxfer_throw_error.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error "boom" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_error should preserve the requested exit status." 3 "$status"
	assertEquals "zxfer_throw_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "boom"
}

test_throw_usage_error_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/throw_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_usage_error "bad option"
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_usage_error should exit with usage status 2." 2 "$status"
	assertEquals "zxfer_throw_usage_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: bad option"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}

test_zxfer_help_bypasses_dependency_init() {
	secure_path_dir="$TEST_TMPDIR/help_secure_path"
	hostile_path_dir="$TEST_TMPDIR/help_hostile_path"
	marker_file="$TEST_TMPDIR/help_hostile_path.marker"
	stdout_file="$TEST_TMPDIR/help.stdout"
	stderr_file="$TEST_TMPDIR/help.stderr"
	real_awk=$(command -v awk 2>/dev/null || :)
	real_sed=$(command -v sed 2>/dev/null || :)
	mkdir -p "$secure_path_dir"
	mkdir -p "$hostile_path_dir"

	if [ -z "$real_awk" ] || [ -z "$real_sed" ]; then
		fail "Host test requires awk and sed on the local system PATH."
	fi

	cat >"$hostile_path_dir/awk" <<EOF
#!/bin/sh
printf '%s\n' "awk" >>"$marker_file"
exec "$real_awk" "\$@"
EOF
	cat >"$hostile_path_dir/sed" <<EOF
#!/bin/sh
printf '%s\n' "sed" >>"$marker_file"
exec "$real_sed" "\$@"
EOF
	chmod +x "$hostile_path_dir/awk" "$hostile_path_dir/sed"

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="$hostile_path_dir:/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -h >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Help output should succeed even when the secure PATH lacks required tools." 0 "$status"
	assertContains "$(cat "$stdout_file")" "usage:"
	assertContains "Help output should advertise the standalone -c service list option." \
		"$(cat "$stdout_file")" "[-c FMRI|pattern[ FMRI|pattern]...]"
	assertContains "Help output should advertise the migration flag separately from -c." \
		"$(cat "$stdout_file")" "[-m]"
	assertContains "Help output should advertise the unsupported-property skip flag." \
		"$(cat "$stdout_file")" "[-U]"
	assertEquals "Help prescan should bypass dependency initialization errors." "" "$(cat "$stderr_file")"
	if [ -f "$marker_file" ]; then
		marker_contents=$(cat "$marker_file")
	else
		marker_contents=""
	fi
	assertEquals "Early invocation capture should not execute PATH-injected awk/sed helpers." "" "$marker_contents"
}

test_zxfer_usage_error_with_very_verbose_does_not_emit_profile_summary() {
	secure_path_dir="$TEST_TMPDIR/usage_secure_path"
	stdout_file="$TEST_TMPDIR/usage.stdout"
	stderr_file="$TEST_TMPDIR/usage.stderr"

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -V >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Very-verbose usage errors should still exit with usage status." 2 "$status"
	assertEquals "Usage errors should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: Need a destination."
	assertNotContains "Usage-mode very-verbose exits should not emit profiling counters." \
		"$(cat "$stderr_file")" "zxfer profile:"
}

test_trap_exit_emits_failure_report_once() {
	set +e
	output=$(
		(
			g_zxfer_failure_class="runtime"
			g_zxfer_failure_stage="unit"
			g_zxfer_failure_message="trap failure"
			g_delete_source_tmp_file=""
			g_delete_dest_tmp_file=""
			g_delete_snapshots_to_delete_tmp_file=""
			g_services_need_relaunch=0
			false
			zxfer_trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	count=$(printf '%s\n' "$output" | grep -c "^zxfer: failure report begin$")
	assertEquals "zxfer_trap_exit helper path should preserve the failing exit status." 1 "$status"
	assertEquals "zxfer_trap_exit should emit the failure report only once even when EXIT re-triggers cleanup." 1 "$count"
}

test_trap_exit_emits_profile_summary_once_in_very_verbose_mode() {
	set +e
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_summary_emitted=0
			g_zxfer_profile_start_epoch=$(($(date +%s) - 3))
			g_zxfer_profile_startup_latency_ms=99
			g_zxfer_profile_cleanup_ms=55
			g_zxfer_profile_ssh_setup_ms=111
			g_zxfer_profile_source_snapshot_listing_ms=222
			g_zxfer_profile_destination_snapshot_listing_ms=333
			g_zxfer_profile_snapshot_diff_sort_ms=444
			g_zxfer_profile_source_zfs_calls=3
			g_zxfer_profile_destination_zfs_calls=4
			g_zxfer_profile_ssh_shell_invocations=2
			g_zxfer_profile_source_snapshot_list_commands=1
			g_zxfer_profile_send_receive_pipeline_commands=2
			g_zxfer_profile_exists_destination_calls=5
			g_zxfer_profile_normalized_property_reads_source=6
			g_zxfer_profile_normalized_property_reads_destination=7
			g_zxfer_profile_required_property_backfill_gets=1
			g_zxfer_profile_parent_destination_property_reads=2
			g_zxfer_profile_bucket_source_inspection=8
			g_zxfer_profile_bucket_destination_inspection=9
			g_zxfer_profile_bucket_property_reconciliation=10
			g_zxfer_profile_bucket_send_receive_setup=11
			g_delete_source_tmp_file=""
			g_delete_dest_tmp_file=""
			g_delete_snapshots_to_delete_tmp_file=""
			g_services_need_relaunch=0
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when only emitting profiling output." 0 "$status"
	assertContains "Very-verbose exits should emit the source zfs profile counter." \
		"$output" "zxfer profile: source_zfs_calls=3"
	assertContains "Very-verbose exits should emit startup latency timing." \
		"$output" "zxfer profile: startup_latency_ms=99"
	assertContains "Very-verbose exits should emit cleanup timing." \
		"$output" "zxfer profile: cleanup_ms="
	assertContains "Very-verbose exits should emit the accumulated ssh setup stage timing." \
		"$output" "zxfer profile: ssh_setup_ms=111"
	assertContains "Very-verbose exits should emit the accumulated snapshot diff/sort stage timing." \
		"$output" "zxfer profile: snapshot_diff_sort_ms=444"
	assertContains "Very-verbose exits should emit the property-read profile counter." \
		"$output" "zxfer profile: normalized_property_reads_destination=7"
	assertContains "Very-verbose exits should emit the send/receive bucket counter." \
		"$output" "zxfer profile: bucket_send_receive_setup=11"
	count=$(printf '%s\n' "$output" | grep -c "^zxfer profile: source_zfs_calls=3$")
	assertEquals "zxfer_trap_exit should emit the profile summary only once." 1 "$count"
}

test_zxfer_profile_emit_summary_returns_without_output_when_already_emitted() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_summary_emitted=1
			zxfer_profile_emit_summary
		) 2>&1
	)
	status=$?

	assertEquals "An already-emitted profile summary should return success." 0 "$status"
	assertEquals "An already-emitted profile summary should not emit duplicate output." "" "$output"
}

test_zxfer_profile_increment_counter_normalizes_blank_and_invalid_inputs_in_current_shell() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_test_profile_counter="bogus"

	zxfer_profile_increment_counter ""
	assertEquals "Blank profile counter names should be ignored without marking profile data present." \
		0 "$g_zxfer_profile_has_data"

	zxfer_profile_increment_counter g_test_profile_counter "bogus"

	assertEquals "Profile counter updates should mark that profile data exists." 1 "$g_zxfer_profile_has_data"
	assertEquals "Invalid increment amounts and counter values should be normalized before incrementing." \
		1 "$g_test_profile_counter"
}

test_zxfer_profile_now_ms_falls_back_to_second_resolution_when_millisecond_format_is_unavailable() {
	output=$(
		(
			date() {
				if [ "$1" = "+%s%3N" ]; then
					printf '%s\n' "not-supported"
				else
					printf '%s\n' "42"
				fi
			}
			zxfer_profile_now_ms
		)
	)
	status=$?

	assertEquals "Profile millisecond timestamps should still succeed when date lacks %N-style support." \
		0 "$status"
	assertEquals "Second-resolution fallbacks should be normalized into millisecond units." \
		42000 "$output"
}

test_zxfer_profile_add_elapsed_ms_accumulates_only_valid_positive_durations_in_current_shell() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_test_profile_elapsed_ms=5

	zxfer_profile_add_elapsed_ms g_test_profile_elapsed_ms 10 25
	zxfer_profile_add_elapsed_ms g_test_profile_elapsed_ms bogus 30
	zxfer_profile_add_elapsed_ms g_test_profile_elapsed_ms 40 35

	assertEquals "Elapsed stage timings should accumulate onto existing millisecond totals." \
		20 "$g_test_profile_elapsed_ms"
	assertEquals "Elapsed stage timings should mark that profiling data exists." \
		1 "$g_zxfer_profile_has_data"
}

test_zxfer_append_failure_report_to_log_creates_secure_file() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure.log"
	ZXFER_ERROR_LOG="$log_path"
	report_contents=$(printf 'zxfer: failure report begin\nmessage: failed\nzxfer: failure report end\n')

	set +e
	zxfer_append_failure_report_to_log "$report_contents"
	status=$?
	if [ -f "$log_path" ]; then
		file_exists=1
	else
		file_exists=0
	fi
	perms=$(stat -c '%a' "$log_path" 2>/dev/null || stat -f '%Lp' "$log_path" 2>/dev/null)
	perms_status=$?
	grep -F "message: failed" "$log_path" >/dev/null 2>&1
	grep_status=$?

	assertEquals "ZXFER_ERROR_LOG appends should succeed for valid absolute paths." 0 "$status"
	assertEquals "Failure log should be created when ZXFER_ERROR_LOG is valid." 1 "$file_exists"
	assertEquals "Log file mode should be readable for assertions." 0 "$perms_status"
	assertEquals "ZXFER_ERROR_LOG files should be created with mode 600." "600" "$perms"
	assertEquals "Failure log should contain the rendered report payload." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_preserves_existing_contents() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure_append.log"
	ZXFER_ERROR_LOG="$log_path"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: appended-report"
	status=$?
	grep -F "existing: keep-me" "$log_path" >/dev/null 2>&1
	existing_status=$?
	grep -F "message: appended-report" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files should still accept appended reports." 0 "$status"
	assertEquals "Atomic ZXFER_ERROR_LOG appends should preserve prior log contents." 0 "$existing_status"
	assertEquals "Atomic ZXFER_ERROR_LOG appends should add the new report payload." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_relative_path() {
	stderr_file="$TEST_TMPDIR/error_log.stderr"
	ZXFER_ERROR_LOG="relative.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log.stdout" 2>"$stderr_file"
	status=$?
	grep -F "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	if [ -e "$TEST_TMPDIR/relative.log" ]; then
		file_exists=1
	else
		file_exists=0
	fi

	assertEquals "Relative ZXFER_ERROR_LOG paths should be rejected." 1 "$status"
	assertEquals "Relative ZXFER_ERROR_LOG rejection should emit a warning." 0 "$grep_status"
	assertEquals "Relative ZXFER_ERROR_LOG should not create a local file." 0 "$file_exists"
}

test_zxfer_append_failure_report_to_log_rejects_missing_parent_dir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	stderr_file="$TEST_TMPDIR/error_log_parent.stderr"
	ZXFER_ERROR_LOG="$physical_tmpdir/missing/subdir/failure.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_parent.stdout" 2>"$stderr_file"
	status=$?
	grep -F "parent directory \"$physical_tmpdir/missing/subdir\" does not exist" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Missing parent directories should be rejected for ZXFER_ERROR_LOG." 1 "$status"
	assertEquals "Missing parent directory rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_untrusted_parent_dir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_dir="$physical_tmpdir/untrusted_error_log_parent"
	stderr_file="$TEST_TMPDIR/error_log_untrusted_parent.stderr"
	mkdir -p "$log_dir"
	chmod 0777 "$log_dir"
	ZXFER_ERROR_LOG="$log_dir/failure.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_untrusted_parent.stdout" 2>"$stderr_file"
	status=$?
	grep -F "writable by others without sticky-bit protection" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	chmod 0700 "$log_dir"

	assertEquals "ZXFER_ERROR_LOG parents that are writable by others without sticky-bit protection should be rejected." 1 "$status"
	assertEquals "Untrusted ZXFER_ERROR_LOG parent rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_symlinked_parent_component() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/real_parent"
	link_dir="$physical_tmpdir/link_parent"
	log_path="$link_dir/failure.log"
	stderr_file="$TEST_TMPDIR/error_log_symlink.stderr"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_symlink.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path component \"$link_dir\" is a symlink" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	if [ -e "$real_dir/failure.log" ]; then
		file_exists=1
	else
		file_exists=0
	fi

	assertEquals "Symlinked parent components should be rejected for ZXFER_ERROR_LOG." 1 "$status"
	assertEquals "Symlinked parent component rejection should emit a warning." 0 "$grep_status"
	assertEquals "Symlinked parent component rejection should not create the target file." 0 "$file_exists"
}

test_zxfer_append_failure_report_to_log_rejects_symlink_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_path="$physical_tmpdir/real_failure.log"
	log_path="$physical_tmpdir/failure_link.log"
	stderr_file="$TEST_TMPDIR/error_log_target_symlink.stderr"
	: >"$real_path"
	chmod 600 "$real_path"
	ln -s "$real_path" "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_target_symlink.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path component \"$log_path\" is a symlink" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Symlinked ZXFER_ERROR_LOG targets should be rejected." 1 "$status"
	assertEquals "Symlinked target rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_non_regular_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure_dir"
	stderr_file="$TEST_TMPDIR/error_log_nonregular.stderr"
	mkdir -p "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_nonregular.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path \"$log_path\" because it is not a regular file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Non-regular ZXFER_ERROR_LOG targets should be rejected." 1 "$status"
	assertEquals "Non-regular target rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_existing_insecure_mode() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/insecure_mode.log"
	stderr_file="$TEST_TMPDIR/error_log_mode.stderr"
	: >"$log_path"
	chmod 644 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: should-not-append" >"$TEST_TMPDIR/error_log_mode.stdout" 2>"$stderr_file"
	status=$?
	grep -F "permissions (644) are not 0600" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing insecure ZXFER_ERROR_LOG files should be rejected." 1 "$status"
	assertEquals "Insecure mode rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected insecure log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_existing_insecure_owner() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/insecure_owner.log"
	stderr_file="$TEST_TMPDIR/error_log_owner.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1"
		}
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock_warn_only() {
			:
		}
		zxfer_get_path_owner_uid() { printf '%s\n' "1234"; }
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_owner.stdout" 2>"$stderr_file"
	status=$?
	grep -F "owned by UID 1234 instead of" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with insecure owners should be rejected." 1 "$status"
	assertEquals "Insecure owner rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected insecure-owner log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_owner() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_owner.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_owner.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1"
		}
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock_warn_only() {
			:
		}
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_owner.stdout" 2>"$stderr_file"
	status=$?
	grep -F "owner could not be determined" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with unknown owners should be rejected." 1 "$status"
	assertEquals "Unknown-owner rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected unknown-owner log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_mode() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_mode.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_mode.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock_warn_only() {
			:
		}
		zxfer_get_path_owner_uid() {
			printf '%s\n' "0"
		}
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_mode.stdout" 2>"$stderr_file"
	status=$?
	grep -F "permissions could not be determined" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with unknown modes should be rejected." 1 "$status"
	assertEquals "Unknown-mode rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected unknown-mode log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_warns_when_file_creation_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/create_failure.log"
	stderr_file="$TEST_TMPDIR/error_log_create_failure.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_create_error_log_file() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: create-failed"
	) >"$TEST_TMPDIR/error_log_create_failure.stdout" 2>"$stderr_file"
	status=$?
	grep -F "unable to create ZXFER_ERROR_LOG file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	stderr_contents=$(cat "$stderr_file" 2>/dev/null || true)

	assertEquals "ZXFER_ERROR_LOG creation failures should be reported without succeeding. status=$status stderr=$stderr_contents" 1 "$status"
	assertEquals "ZXFER_ERROR_LOG creation failures should emit a warning. status=$status stderr=$stderr_contents" 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_warns_when_chmod_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/chmod_failure.log"
	stderr_file="$TEST_TMPDIR/error_log_chmod_failure.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_chmod_error_log_file() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: chmod-failed"
	) >"$TEST_TMPDIR/error_log_chmod_failure.stdout" 2>"$stderr_file"
	status=$?
	grep -F "unable to chmod ZXFER_ERROR_LOG file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	stderr_contents=$(cat "$stderr_file" 2>/dev/null || true)

	assertEquals "ZXFER_ERROR_LOG chmod failures should be reported without succeeding. status=$status stderr=$stderr_contents" 1 "$status"
	assertEquals "ZXFER_ERROR_LOG chmod failures should emit a warning. status=$status stderr=$stderr_contents" 0 "$grep_status"
}

test_trap_exit_preserves_failure_status_when_error_log_warning_fails() {
	set +e
	output=$(
		(
			ZXFER_ERROR_LOG="relative.log"
			g_zxfer_failure_class="runtime"
			g_zxfer_failure_stage="unit"
			g_zxfer_failure_message="trap failure"
			g_delete_source_tmp_file=""
			g_delete_dest_tmp_file=""
			g_delete_snapshots_to_delete_tmp_file=""
			g_services_need_relaunch=0
			false
			zxfer_trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	assertEquals "Failure-report log warnings must not replace the original exit status." 1 "$status"
	assertContains "zxfer_trap_exit should still emit the report before warning about the log sink." "$output" "zxfer: failure report begin"
	assertContains "zxfer_trap_exit should warn when ZXFER_ERROR_LOG is invalid." \
		"$output" "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute"
}

test_zxfer_kill_registered_cleanup_pids_only_terminates_registered_pids() {
	output=$(
		(
			unrelated_pid=60101
			g_zxfer_cleanup_pids="50101"
			g_zxfer_cleanup_pid_records="50101	registered cleanup helper	start-token	unit-host	700	child_set"
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s\n' "$1"
				zxfer_unregister_cleanup_pid "$1"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pids"
			printf 'unrelated=<%s>\n' "$unrelated_pid"
		)
	)

	assertContains "Cleanup should delegate validated teardown only for tracked helper PIDs." \
		"$output" "abort:50101"
	assertNotContains "Cleanup should not delegate teardown for unrelated helper PIDs." \
		"$output" "abort:60101"
	assertContains "Cleanup PID tracking should be cleared after termination." \
		"$output" "remaining=<>"
}

test_zxfer_cleanup_pid_helpers_ignore_invalid_inputs_in_current_shell() {
	sleep 30 &
	tracked_pid=$!
	zxfer_register_cleanup_pid "$tracked_pid" "tracked cleanup helper"

	zxfer_register_cleanup_pid ""
	zxfer_register_cleanup_pid "abc"
	assertEquals "Cleanup PID registration should ignore empty and non-numeric inputs." \
		"$tracked_pid" "$g_zxfer_cleanup_pids"

	zxfer_unregister_cleanup_pid ""
	zxfer_unregister_cleanup_pid "abc"
	zxfer_unregister_cleanup_pid "$tracked_pid"
	assertEquals "Cleanup PID unregistration should ignore invalid inputs and preserve the remaining list order." \
		"" "$g_zxfer_cleanup_pids"

	output=$(
		(
			l_stub_pid=7001
			g_zxfer_cleanup_pids="abc $l_stub_pid $$"
			g_zxfer_cleanup_pid_records="$l_stub_pid	tracked cleanup helper	start-token	unit-host	700	child_set"
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s\n' "$1"
				zxfer_unregister_cleanup_pid "$1"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pids"
		)
	)

	kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
	wait "$tracked_pid" 2>/dev/null || true
	assertContains "Cleanup termination should still delegate teardown for the validated helper when invalid entries are present in the PID list." \
		"$output" "abort:7001"
	assertNotContains "Cleanup termination should ignore non-numeric entries in the tracked PID list." \
		"$output" "abort:abc"
	assertContains "Cleanup PID tracking should be cleared after termination." \
		"$output" "remaining=<>"
}

test_execute_command_records_last_command_string() {
	g_option_n_dryrun=1
	g_zxfer_failure_last_command=""

	zxfer_execute_command "printf 'hello'"

	assertEquals "zxfer_execute_command should redact the exact command string for failure reports by default." "[redacted]" "$g_zxfer_failure_last_command"
}

test_run_source_zfs_cmd_records_local_command_in_unsafe_mode() {
	g_option_O_origin_host=""
	g_cmd_zfs="/bin/echo"
	g_LZFS="$g_cmd_zfs"
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_run_source_zfs_cmd list -H tank/src >/dev/null

	assertEquals "Direct local ZFS commands should be shell-quoted in the last-command field when unsafe mode is enabled." \
		"'/bin/echo' 'list' '-H' 'tank/src'" "$g_zxfer_failure_last_command"
}

test_invoke_ssh_command_for_host_records_remote_command_in_unsafe_mode() {
	FAKE_SSH_STDOUT_OVERRIDE="ok"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1

	zxfer_invoke_ssh_command_for_host "backup@example.com pfexec" /sbin/zfs list -H tank/src >/dev/null

	assertEquals "Unsafe SSH command recording should preserve every token boundary." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock' 'backup@example.com' 'pfexec' '/sbin/zfs' 'list' '-H' 'tank/src'" \
		"$g_zxfer_failure_last_command"
}

test_zxfer_remote_command_context_helpers_cover_remaining_role_labels() {
	output=$(
		(
			g_option_O_origin_host="shared.example"
			g_option_T_target_host="shared.example"
			printf 'other=%s\n' "$(zxfer_get_remote_command_context_label "other.example" other)"
			printf 'shared=%s\n' "$(zxfer_get_remote_command_context_label "shared.example")"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			printf 'target=%s\n' "$(zxfer_get_remote_command_context_label "target.example")"
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_echoV_remote_command_for_host "misc.example doas" other /bin/echo hello
		)
	)

	assertContains "Remote command context labels should render the explicit other profile side as remote." \
		"$output" "other=remote: other.example"
	assertContains "Remote command context labels should render shared origin and target hosts as origin/target." \
		"$output" "shared=origin/target: shared.example"
	assertContains "Remote command context labels should infer the target role when only the target host matches." \
		"$output" "target=target: target.example"
	assertContains "Very-verbose remote command rendering should include the resolved remote context label." \
		"$output" "Running remote command [remote: misc.example doas]: '/bin/echo' 'hello'"
}

test_zxfer_echoV_remote_command_for_host_covers_current_shell_render_path() {
	trace_file="$TEST_TMPDIR/echoV_remote_command_current_shell.log"

	(
		g_option_O_origin_host="origin.example"
		g_option_T_target_host="target.example doas"
		zxfer_echoV() {
			printf '%s\n' "$*" >"$trace_file"
		}
		zxfer_echoV_remote_command_for_host "target.example doas" "" /bin/echo current-shell
	)

	assertEquals "Very-verbose remote command rendering should keep the current-shell target-context path shell-quoted exactly once." \
		"Running remote command [target: target.example doas]: '/bin/echo' 'current-shell'" \
		"$(cat "$trace_file")"
}

test_zxfer_render_destination_zfs_command_uses_remote_target_tool_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="backup@example.com"
	g_option_T_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
	g_target_cmd_zfs="/remote/bin/zfs"

	rendered=$(zxfer_render_destination_zfs_command list -H backup/target)

	assertContains "Remote destination zfs rendering should route through ssh." \
		"$rendered" "'$FAKE_SSH_BIN'"
	assertContains "Remote destination zfs rendering should target the configured host." \
		"$rendered" "'backup@example.com'"
	assertContains "Remote destination zfs rendering should mention the resolved remote zfs path." \
		"$rendered" "/remote/bin/zfs"
	assertContains "Remote destination zfs rendering should preserve the requested subcommand and dataset." \
		"$rendered" "backup/target"
}

test_zxfer_render_zfs_command_for_spec_routes_destination_and_literal_commands() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="backup@example.com"
	g_option_T_target_host_safe=$(zxfer_quote_host_spec_tokens "$g_option_T_target_host")
	g_target_cmd_zfs="/remote/bin/zfs"
	g_LZFS="mock_source_spec"
	g_RZFS="mock_destination_spec"

	destination_rendered=$(zxfer_render_zfs_command_for_spec "$g_RZFS" list -H backup/target)
	literal_rendered=$(zxfer_render_zfs_command_for_spec "/bin/echo" hello world)

	assertContains "Destination command specs should reuse the destination render helper." \
		"$destination_rendered" "/remote/bin/zfs"
	assertContains "Destination command specs should preserve the requested dataset argument." \
		"$destination_rendered" "backup/target"
	assertEquals "Literal command specs should be rendered as direct shell-quoted argv." \
		"'/bin/echo' 'hello' 'world'" "$literal_rendered"
}

test_build_ssh_shell_command_for_host_quotes_control_socket_path_for_eval() {
	marker_rel="control_socket_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	log_file="$TEST_TMPDIR/control_socket_eval.log"
	socket_path="$TEST_TMPDIR/socket.\$(touch $marker_rel)"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	: >"$log_file"
	rm -f "$marker"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$socket_path"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	cmd=$(zxfer_build_ssh_shell_command_for_host "backup@example.com" "$safe_cmd")
	(
		cd "$TEST_TMPDIR" || exit 1
		zxfer_execute_command "$cmd"
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertFalse "Control-socket paths should stay literal when ssh commands are eval-rendered." \
		"[ -e '$marker' ]"
	assertEquals "Rendered ssh commands should pass the control socket as a single argv token." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$socket_path
backup@example.com
'sh' '-c' 'printf ok >/dev/null'" "$(cat "$log_file")"
}

test_build_remote_sh_c_command_preserves_multiline_scripts_as_one_c_argument() {
	log_file="$TEST_TMPDIR/remote_sh_multiline.log"
	ssh_bin="$TEST_TMPDIR/fake_ssh_join_multiline"
	create_fake_ssh_join_exec_bin "$ssh_bin"
	: >"$log_file"
	g_cmd_ssh="$ssh_bin"
	g_option_O_origin_host="backup@example.com"
	FAKE_SSH_LOG="$log_file"
	export FAKE_SSH_LOG

	remote_cmd=$(zxfer_build_remote_sh_c_command "l_value=ok
printf '%s\n' \"\$l_value\"")
	output=$(zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "$remote_cmd")

	unset FAKE_SSH_LOG

	assertEquals "Remote sh -c builders should preserve multiline scripts as one command argument." \
		"ok" "$output"
	assertContains "Remote sh -c builders should still target the requested host." \
		"$(cat "$log_file")" "backup@example.com"
	assertContains "Remote sh -c builders should keep the entire multiline script inside the single -c payload." \
		"$(cat "$log_file")" "l_value=ok"
}

test_prepare_remote_shell_command_for_host_wraps_only_wrapper_hosts() {
	zxfer_prepare_remote_shell_command_for_host "backup@example.com" "zfs list tank/src"
	simple_status=$?
	simple_result=$g_zxfer_remote_shell_command_for_host_result

	zxfer_prepare_remote_shell_command_for_host "backup@example.com pfexec -p 2222" "zfs list tank/src"
	wrapped_status=$?
	wrapped_result=$g_zxfer_remote_shell_command_for_host_result

	assertEquals "Simple host specs should prepare without an extra remote shell wrapper." \
		0 "$simple_status"
	assertEquals "Simple host specs should preserve the original remote command." \
		"zfs list tank/src" "$simple_result"
	assertEquals "Wrapper host specs should prepare successfully." \
		0 "$wrapped_status"
	assertContains "Wrapper host specs should render a remote sh command." \
		"$wrapped_result" "'sh' '-c'"
	assertContains "Wrapper host specs should preserve the remote command inside the sh payload." \
		"$wrapped_result" "zfs list tank/src"
}

test_prepare_remote_shell_command_for_host_preserves_split_and_wrapper_failures() {
	output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid host spec"
				return 41
			}
			zxfer_prepare_remote_shell_command_for_host "bad host" "zfs list" >/dev/null
			printf 'split_status=%s\n' "$?"
			printf 'split_result=<%s>\n' "$g_zxfer_remote_shell_command_for_host_result"
		)
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n%s\n' "backup@example.com" "pfexec"
			}
			zxfer_build_remote_sh_c_command() {
				return 42
			}
			zxfer_prepare_remote_shell_command_for_host "backup@example.com pfexec" "zfs list" >/dev/null
			printf 'wrapper_status=%s\n' "$?"
		)
	)

	assertContains "Remote shell preparation should preserve host-token split failures." \
		"$output" "split_status=41"
	assertContains "Remote shell preparation should expose host-token split diagnostics to current-shell callers." \
		"$output" "split_result=<invalid host spec>"
	assertContains "Remote shell preparation should preserve remote sh builder failures." \
		"$output" "wrapper_status=42"
}

test_prepare_ssh_shell_command_context_extracts_host_and_wrapper_command() {
	zxfer_prepare_ssh_shell_command_context "backup@example.com pfexec -u root" "'sh' '-c' 'zfs list tank/src'"
	status=$?

	assertEquals "SSH shell context preparation should succeed for wrapper host specs." \
		0 "$status"
	assertEquals "SSH shell context preparation should publish the first host-spec token as the ssh host." \
		"backup@example.com" "$g_zxfer_ssh_shell_host_result"
	assertEquals "SSH shell context preparation should prefix the remote command with safely quoted wrapper tokens." \
		"'pfexec' '-u' 'root' 'sh' '-c' 'zfs list tank/src'" "$g_zxfer_ssh_shell_full_remote_command_result"
}

test_build_prepared_ssh_shell_command_for_host_centralizes_prepare_and_render() {
	output=$(
		(
			zxfer_prepare_remote_shell_command_for_host() {
				g_zxfer_remote_shell_command_for_host_result="'sh' '-c' '$2'"
				return 0
			}
			zxfer_build_ssh_shell_command_for_host() {
				printf 'host=<%s> cmd=<%s>' "$1" "$2"
			}
			zxfer_build_prepared_ssh_shell_command_for_host "backup@example.com pfexec" "zfs list tank/src"
			printf '\nresult=<%s>\n' "$g_zxfer_prepared_ssh_shell_command_result"
		)
	)

	assertContains "Prepared SSH shell rendering should pass the host spec to the final SSH renderer." \
		"$output" "host=<backup@example.com pfexec>"
	assertContains "Prepared SSH shell rendering should pass the prepared remote command to the final SSH renderer." \
		"$output" "cmd=<'sh' '-c' 'zfs list tank/src'>"
	assertContains "Prepared SSH shell rendering should publish the rendered shell command for current-shell callers." \
		"$output" "result=<host=<backup@example.com pfexec> cmd=<'sh' '-c' 'zfs list tank/src'>>"
}

test_ssh_shell_context_callers_preserve_empty_context_failures() {
	output=$(
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				g_zxfer_ssh_shell_context_error_result=""
				return 47
			}
			zxfer_build_ssh_shell_command_for_host "backup@example.com" "zfs list tank/src" >/dev/null
			printf 'build_status=%s\n' "$?"
		)
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				g_zxfer_ssh_shell_context_error_result=""
				return 48
			}
			zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "zfs list tank/src" source >/dev/null
			printf 'invoke_status=%s\n' "$?"
		)
	)

	assertContains "SSH shell rendering should preserve context-preparation failures even without diagnostics." \
		"$output" "build_status=47"
	assertContains "SSH shell invocation should preserve context-preparation failures even without diagnostics." \
		"$output" "invoke_status=48"
}

test_build_prepared_ssh_shell_command_for_host_preserves_render_diagnostics() {
	output=$(
		(
			zxfer_prepare_remote_shell_command_for_host() {
				g_zxfer_remote_shell_command_for_host_result="'sh' '-c' '$2'"
				return 0
			}
			zxfer_build_ssh_shell_command_for_host() {
				printf '%s\n' "render diagnostic"
				return 43
			}
			zxfer_build_prepared_ssh_shell_command_for_host "backup@example.com" "zfs list tank/src" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'error=<%s>\n' "$g_zxfer_prepared_ssh_shell_command_error_result"
		)
	)

	assertContains "Prepared SSH shell rendering should preserve final renderer status." \
		"$output" "status=43"
	assertContains "Prepared SSH shell rendering should publish final renderer diagnostics for callers that rethrow outside command substitutions." \
		"$output" "error=<render diagnostic>"
}

test_build_ssh_shell_command_for_host_honors_explicit_ambient_policy_opt_out() {
	log_file="$TEST_TMPDIR/build_shell_ambient.log"
	socket_path="$TEST_TMPDIR/ambient.sock"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	: >"$log_file"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com"
	g_ssh_origin_control_socket="$socket_path"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	cmd=$(zxfer_build_ssh_shell_command_for_host "backup@example.com" "$safe_cmd")
	zxfer_execute_command "$cmd"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Ambient-policy opt-out should suppress managed ssh -o options in shell-command rendering while preserving control-socket reuse." \
		"-S
$socket_path
backup@example.com
'sh' '-c' 'printf ok >/dev/null'" "$(cat "$log_file")"
}

test_build_ssh_shell_command_for_host_fuzzes_wrapper_specs_and_control_socket_paths() {
	marker_rel="control_socket_fuzz_marker"
	marker="$TEST_TMPDIR/$marker_rel"
	case_file="$TEST_TMPDIR/control_socket_fuzz_cases.txt"
	safe_cmd=$(zxfer_build_remote_sh_c_command "printf ok >/dev/null")
	cat >"$case_file" <<EOF
backup@example.com doas|$TEST_TMPDIR/socket,comma
backup@example.com pfexec -u root|$TEST_TMPDIR/socket=equals
backup@example.com env LC_ALL=C doas|$TEST_TMPDIR/socket:semicolon;literal
backup@example.com doas|$TEST_TMPDIR/socket.\$(touch $marker_rel)
EOF

	case_index=0
	while IFS='|' read -r host_spec socket_path || [ -n "$host_spec$socket_path" ]; do
		[ -n "$host_spec" ] || continue
		case_index=$((case_index + 1))
		log_file="$TEST_TMPDIR/control_socket_fuzz_$case_index.log"
		: >"$log_file"
		rm -f "$marker"
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host=$host_spec
		g_ssh_origin_control_socket=$socket_path
		FAKE_SSH_LOG="$log_file"
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		cmd=$(zxfer_build_ssh_shell_command_for_host "$host_spec" "$safe_cmd")
		(
			cd "$TEST_TMPDIR" || exit 1
			zxfer_execute_command "$cmd"
		)

		unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

		assertFalse "Control-socket fuzz case $case_index should not execute command substitutions from the socket path." \
			"[ -e '$marker' ]"
		assertEquals "Control-socket fuzz case $case_index should force batch mode first." "-o" "$(sed -n '1p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass BatchMode=yes as the first managed transport option." "BatchMode=yes" "$(sed -n '2p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should force strict host-key checking next." "-o" "$(sed -n '3p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass StrictHostKeyChecking=yes as the second managed transport option." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should pass -S separately." "-S" "$(sed -n '5p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should preserve the literal control-socket path." \
			"$socket_path" "$(sed -n '6p' "$log_file")"
		assertEquals "Control-socket fuzz case $case_index should keep the ssh host token separate from wrappers." \
			"backup@example.com" "$(sed -n '7p' "$log_file")"
		log_line_remote_cmd=$(sed -n '8p' "$log_file")
		assertContains "Control-socket fuzz case $case_index should preserve the quoted remote command payload." \
			"$log_line_remote_cmd" "'sh' '-c' 'printf ok >/dev/null'"

		case "$host_spec" in
		*" doas"*)
			assertContains "Control-socket fuzz case $case_index should keep doas in the remote wrapper chain." \
				"$log_line_remote_cmd" "'doas'"
			;;
		esac
		case "$host_spec" in
		*"pfexec -u root"*)
			assertContains "Control-socket fuzz case $case_index should keep pfexec wrapper tokens quoted." \
				"$log_line_remote_cmd" "'pfexec' '-u' 'root'"
			;;
		esac
		case "$host_spec" in
		*"LC_ALL=C doas"*)
			assertContains "Control-socket fuzz case $case_index should keep env-style wrapper tokens quoted." \
				"$log_line_remote_cmd" "'env' 'LC_ALL=C' 'doas'"
			;;
		esac
	done <"$case_file"
}

test_read_remote_backup_file_rejects_insecure_remote_owner() {
	set +e
	output=$(
		(
			zxfer_get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
			zxfer_invoke_ssh_shell_command_for_host() { return 95; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort on insecure remote ownership." 1 "$status"
	assertContains "Insecure remote ownership should use the documented error." \
		"$output" "Refusing to use backup metadata /tmp/backup.meta on backup@example.com because it is not owned by root or the ssh user."
}

test_read_remote_backup_file_rejects_insecure_remote_mode() {
	set +e
	output=$(
		(
			zxfer_get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
			zxfer_invoke_ssh_shell_command_for_host() { return 96; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort on insecure remote permissions." 1 "$status"
	assertContains "Insecure remote permissions should use the documented error." \
		"$output" "Refusing to use backup metadata /tmp/backup.meta on backup@example.com because its permissions are not 0600."
}

test_read_remote_backup_file_rejects_unknown_remote_security_metadata() {
	set +e
	output=$(
		(
			zxfer_get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
			zxfer_invoke_ssh_shell_command_for_host() { return 97; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort when remote ownership or mode cannot be determined." 1 "$status"
	assertContains "Unknown remote security metadata should use the documented error." \
		"$output" "Cannot determine ownership or permissions for backup metadata /tmp/backup.meta on backup@example.com."
}

test_read_remote_backup_file_allows_trusted_absolute_root_symlink_components() {
	backup_file=$(mktemp /tmp/read_remote_trusted.XXXXXX)
	outfile="$TEST_TMPDIR/read_remote_trusted.out"
	printf '%s\n' "backup-data" >"$backup_file"
	chmod 600 "$backup_file"
	g_cmd_cat="/bin/cat"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
	) >"$outfile"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup reads, which keeps default /var- or /tmp-backed remote roots working on macOS." 0 "$status"
	assertEquals "Trusted absolute symlink components should still allow the secure metadata contents to be read." \
		"backup-data" "$(cat "$outfile")"

	rm -f "$backup_file"
}

test_throw_error_with_usage_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/zxfer_throw_error_with_usage.stdout"
	stderr_file="$TEST_TMPDIR/zxfer_throw_error_with_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_throw_error_with_usage "boom with usage" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "zxfer_throw_error_with_usage should preserve the requested exit status." 3 "$status"
	assertEquals "zxfer_throw_error_with_usage should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: boom with usage"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}

test_get_os_handles_local_and_remote_invocations() {
	local_result=$(zxfer_get_os "")
	if remote_result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			FAKE_SSH_STDOUT_OVERRIDE=$(fake_remote_capability_response)
			export FAKE_SSH_STDOUT_OVERRIDE
			zxfer_get_os "backup@example.com pfexec"
		)
	); then
		remote_status=0
	else
		remote_status=$?
	fi
	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Local OS detection should match uname output." "$(uname)" "$local_result"
	assertEquals "Remote OS detection should succeed through the ssh helper path." 0 "$remote_status"
	assertEquals "Remote OS detection should execute uname through the ssh helper path." "RemoteOS" "$remote_result"
}

test_get_os_fails_when_remote_helper_is_unavailable() {
	set +e
	result=$(
		(
			unset -f zxfer_get_remote_host_operating_system 2>/dev/null || :
			zxfer_get_os "backup@example.com" 2>/dev/null
		)
	)
	status=$?

	assertEquals "Remote OS detection should preserve missing-helper status." 127 "$status"
	assertEquals "Failed remote OS detection should not print a payload." "" "$result"
}

test_get_os_treats_local_ssh_path_as_literal() {
	marker="$TEST_TMPDIR/get_os_ssh_marker"
	old_cmd_ssh=${g_cmd_ssh:-}
	g_cmd_ssh="/bin/echo; touch $marker #"

	if zxfer_get_os "backup@example.com" >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi
	g_cmd_ssh=$old_cmd_ssh

	: "$status"
	assertFalse "Local ssh helper paths should not execute shell metacharacters during OS detection." \
		"[ -e '$marker' ]"
}

test_execute_command_continue_on_fail_reports_noncritical_error() {
	g_option_n_dryrun=0

	output=$(zxfer_execute_command "false" 1)
	status=$?

	assertEquals "Continue-on-fail commands should not abort the caller." 0 "$status"
	assertContains "Continue-on-fail commands should report the non-critical failure." \
		"$output" "Non-critical error when executing command. Continuing."
}

test_zxfer_get_error_log_parent_dir_handles_root_and_relative_inputs() {
	assertEquals "Absolute paths should return their containing directory." \
		"/var/log" "$(zxfer_get_error_log_parent_dir "/var/log/zxfer.log")"
	assertEquals "Paths without a slash should fall back to root for parent-dir validation." \
		"/" "$(zxfer_get_error_log_parent_dir "zxfer.log")"
}

test_run_source_zfs_cmd_uses_local_wrapper_command_when_configured() {
	wrapper="$TEST_TMPDIR/local_source_wrapper"
	cat >"$wrapper" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$wrapper"
	result=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			g_option_O_origin_host=""
			g_cmd_zfs="/sbin/zfs"
			g_LZFS="$wrapper"
			zxfer_run_source_zfs_cmd list -H tank/src
			printf 'last=%s\n' "$g_zxfer_failure_last_command"
		)
	)

	assertContains "Local source wrappers should execute directly when configured." \
		"$result" "list -H tank/src"
	assertContains "Local source wrappers should be recorded in the last-command field." \
		"$result" "last='$wrapper' 'list' '-H' 'tank/src'"
}

test_run_destination_zfs_cmd_uses_local_wrapper_command_when_configured() {
	wrapper="$TEST_TMPDIR/local_dest_wrapper"
	cat >"$wrapper" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$wrapper"
	result=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_RZFS="$wrapper"
			zxfer_run_destination_zfs_cmd get name tank/dst
			printf 'last=%s\n' "$g_zxfer_failure_last_command"
		)
	)

	assertContains "Local destination wrappers should execute directly when configured." \
		"$result" "get name tank/dst"
	assertContains "Local destination wrappers should be recorded in the last-command field." \
		"$result" "last='$wrapper' 'get' 'name' 'tank/dst'"
}

test_strip_trailing_slashes_preserves_all_slash_input_in_current_shell() {
	outfile="$TEST_TMPDIR/all_slash_path.out"

	zxfer_strip_trailing_slashes "///" >"$outfile"

	assertEquals "Inputs made only of slash characters should remain unchanged." "///" "$(cat "$outfile")"
}

test_zxfer_find_symlink_path_component_detects_nested_symlink() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/real_dir"
	link_dir="$physical_tmpdir/link_dir"
	mkdir -p "$real_dir/subdir"
	ln -s "$real_dir" "$link_dir"

	result=$(zxfer_find_symlink_path_component "$link_dir/subdir/file")
	status=$?

	assertEquals "Nested symlink detection should succeed when any path component is a symlink." 0 "$status"
	assertEquals "Nested symlink detection should return the offending path component." "$link_dir" "$result"
}

test_zxfer_find_symlink_path_component_detects_relative_symlink() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/relative_real_dir"
	link_dir="$physical_tmpdir/relative_link_dir"
	mkdir -p "$real_dir/subdir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	result=$(zxfer_find_symlink_path_component "./relative_link_dir/subdir/file")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative paths should be scanned for nested symlink components." 0 "$status"
	assertEquals "Relative symlink checks should return the offending relative path component." "./relative_link_dir" "$result"
}

test_zxfer_find_symlink_path_component_ignores_trusted_absolute_root_symlink() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	result=$(zxfer_find_symlink_path_component "$trusted_root_symlink/zxfer-trusted-root-symlink-probe/subdir/file")
	status=$?

	assertEquals "Trusted top-level system symlink components should be ignored regardless of platform-specific root layout." 1 "$status"
	assertEquals "Trusted absolute symlink components should not be reported as unsafe." "" "$result"
}

test_zxfer_is_trusted_symlink_path_component_accepts_known_root_symlink() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Known trusted root-level symlinks should be accepted by the trust check when the current host exposes one." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Trusted root-symlink checks should stay silent on success." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_owner_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the symlink owner lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Owner-lookup failures should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_owner_lookup_failures_for_absolute_nonroot_symlinks() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_owner_lookup_failure"
	symlink_target="$symlink_parent/target"
	symlink_path="$symlink_parent/link"
	mkdir -p "$symlink_target"
	ln -sf "$symlink_target" "$symlink_path"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$symlink_path\") return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$symlink_path\"
	"

	assertEquals "Trusted-symlink checks should fail closed when the symlink owner lookup fails for absolute non-root symlinks." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Absolute non-root symlink owner-lookup failures should not emit a trusted result payload." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_parent_owner_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$trusted_root_symlink\") printf '%s\n' '0' ;;
			/) return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the root-parent owner lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Parent-owner lookup failures should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_parent_owner_lookup_failures_for_absolute_nonroot_symlinks() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_parent_lookup_failure"
	symlink_target="$symlink_parent/target"
	symlink_path="$symlink_parent/link"
	mkdir -p "$symlink_target"
	ln -sf "$symlink_target" "$symlink_path"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$symlink_path\") printf '%s\n' '0' ;;
			\"$symlink_parent\") return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$symlink_path\"
	"

	assertEquals "Trusted-symlink checks should fail closed when the parent owner lookup fails for absolute non-root symlinks." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Absolute non-root parent-owner lookup failures should not emit a trusted result payload." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_ls_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			return 1
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the root permission lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed root-permission lookups should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_unparseable_root_permissions() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'bad-perms'
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should reject malformed root permission strings." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Malformed root-permission strings should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_world_writable_root_without_sticky_bit() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'drwxrwxrwx 1 0 0 0 Jan 1 00:00 /'
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should reject world-writable root parents without a sticky bit." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Untrusted root-permission layouts should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_find_symlink_path_component_returns_empty_for_relative_non_symlink_path() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/relative_plain_dir/subdir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	result=$(zxfer_find_symlink_path_component "./relative_plain_dir/subdir/file")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative paths without symlink components should still return failure." 1 "$status"
	assertEquals "Relative non-symlink checks should not report a component." "" "$result"
}

test_zxfer_require_backup_metadata_path_without_symlinks_rejects_symlink_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_file="$physical_tmpdir/backup.meta.real"
	link_file="$physical_tmpdir/backup.meta.link"
	: >"$real_file"
	ln -s "$real_file" "$link_file"

	zxfer_test_capture_subshell "
		zxfer_require_backup_metadata_path_without_symlinks \"$link_file\"
	"

	assertEquals "Exact backup metadata symlink paths should be rejected." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Exact backup metadata symlink rejections should identify the symlink itself." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Refusing to use backup metadata $link_file because it is a symlink."
}

test_zxfer_get_path_mode_octal_returns_failure_when_ls_fallback_cannot_map_permissions() {
	zxfer_test_capture_subshell "
		cd \"$TEST_TMPDIR\" || exit 1
		: >\"mode_unknown\"
		stat() {
			return 1
		}
		ls() {
			printf '%s\n' '-rw-r----- 1 0 0 0 Jan 1 00:00 ./mode_unknown'
		}
		zxfer_get_path_mode_octal \"mode_unknown\"
	"

	assertEquals "Mode lookups should fail when the ls fallback cannot map permissions to an octal value." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed ls-mode fallbacks should not emit a value." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_try_get_socket_cache_tmpdir_normalizes_dot_segment_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	socket_tmp="$physical_tmpdir/socket_cache_tmpdir"
	dot_tmp="$socket_tmp/./"
	mkdir -p "$socket_tmp"
	TMPDIR="$dot_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_socket_cache_tmpdir)
	status=$?

	assertEquals "Socket-cache tempdir selection should still succeed when TMPDIR includes dot segments." \
		0 "$status"
	assertEquals "Dot-segment TMPDIR values should resolve to the physical directory instead of preserving the raw dotted path." \
		"$socket_tmp" "$result"

	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_try_get_effective_tmpdir_resolves_symlinked_tmpdir_to_physical_path() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/effective_tmp_real"
	link_tmp="$physical_tmpdir/effective_tmp_link"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_effective_tmpdir)
	status=$?

	assertEquals "Symlinked TMPDIR values should still resolve successfully when their physical target is trusted." 0 "$status"
	assertEquals "Effective TMPDIR resolution should return the physical directory path." "$real_tmp" "$result"
	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_try_get_effective_tmpdir_prefers_memory_backed_default_candidates_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	ram_tmp="$physical_tmpdir/default_tmp_ram"
	disk_tmp="$physical_tmpdir/default_tmp_disk"
	mkdir -p "$ram_tmp" "$disk_tmp"
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			output_file="$TEST_TMPDIR/effective_tmp_default_current_shell.out"

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "$disk_tmp"
			}

			zxfer_try_get_effective_tmpdir >"$output_file" || exit $?
			result=$(cat "$output_file")
			printf 'result=%s\n' "$result"
			printf 'request=%s\n' "$g_zxfer_effective_tmpdir_requested"
		)
	)
	status=$?

	assertEquals "Unset TMPDIR should prefer the first validated default temp-root candidate, which lets zxfer prefer memory-backed roots when available." \
		0 "$status"
	assertContains "Unset TMPDIR should resolve to the preferred memory-backed default candidate." \
		"$output" "result=$ram_tmp"
	assertContains "Default-tempdir selections should cache under the synthetic default request key." \
		"$output" "request=__ZXFER_DEFAULT_TMPDIR__"
}

test_zxfer_try_get_effective_tmpdir_prefers_explicit_tmpdir_over_default_candidates_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	explicit_tmp="$physical_tmpdir/effective_tmp_explicit"
	ram_tmp="$physical_tmpdir/effective_tmp_default_ram"
	mkdir -p "$explicit_tmp" "$ram_tmp"
	output=$(
		(
			TMPDIR="$explicit_tmp"
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			output_file="$TEST_TMPDIR/effective_tmp_explicit_current_shell.out"

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "/tmp"
			}

			zxfer_try_get_effective_tmpdir >"$output_file" || exit $?
			result=$(cat "$output_file")
			printf 'result=%s\n' "$result"
			printf 'request=%s\n' "$g_zxfer_effective_tmpdir_requested"
		)
	)
	status=$?

	assertEquals "A valid explicit TMPDIR should still win over the default memory-backed candidate list." \
		0 "$status"
	assertContains "A valid explicit TMPDIR should remain the effective temp root." \
		"$output" "result=$explicit_tmp"
	assertContains "The cache key should still reflect the explicit TMPDIR request." \
		"$output" "request=$explicit_tmp"
}

test_zxfer_try_get_effective_tmpdir_falls_back_to_preferred_default_candidate_when_tmpdir_is_unsafe() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure_preferred"
	ram_tmp="$physical_tmpdir/effective_tmp_fallback_ram"
	disk_tmp="$physical_tmpdir/effective_tmp_fallback_disk"
	mkdir -p "$insecure_tmp" "$ram_tmp" "$disk_tmp"
	chmod 0777 "$insecure_tmp"
	output=$(
		(
			TMPDIR="$insecure_tmp"
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""

			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$ram_tmp"
				printf '%s\n' "$disk_tmp"
			}

			result=$(zxfer_try_get_effective_tmpdir) || exit $?
			printf 'result=%s\n' "$result"
		)
	)
	status=$?
	chmod 0700 "$insecure_tmp"

	assertEquals "Unsafe TMPDIR values should still resolve cleanly by falling back to the preferred validated default temp root." \
		0 "$status"
	assertContains "Unsafe TMPDIR values should fall back to the preferred validated default candidate before disk-backed fallbacks." \
		"$output" "result=$ram_tmp"
}

test_zxfer_try_get_effective_tmpdir_rejects_non_sticky_world_writable_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/effective_tmp_insecure"
	mkdir -p "$insecure_tmp"
	chmod 0777 "$insecure_tmp"
	TMPDIR="$insecure_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	result=$(zxfer_try_get_effective_tmpdir)
	status=$?

	assertEquals "Unsafe world-writable TMPDIR values should still resolve by falling back to the system temp root." 0 "$status"
	assertNotEquals "Unsafe world-writable TMPDIR values should not remain selected." "$insecure_tmp" "$result"

	chmod 0700 "$insecure_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_validate_temp_root_candidate_returns_failure_when_ls_lookup_fails() {
	candidate="$TEST_TMPDIR/validate_tmp_root_ls_failure"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			return 1
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should fail closed when directory permission lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed temp-root validation should not emit a physical directory path." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_nonroot_owned_dir_when_effective_uid_lookup_fails() {
	candidate="$TEST_TMPDIR/validate_tmp_root_effective_uid_failure"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '1234'
		}
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should fail closed when a non-root directory cannot be matched to the effective uid." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed effective-uid validation should not emit a physical directory path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_non_sticky_world_writable_dir_directly() {
	candidate="$TEST_TMPDIR/validate_tmp_root_insecure_mode"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'drwxrwxrwx 1 0 0 0 Jan 1 00:00 $candidate'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should reject world-writable directories without a sticky bit." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Rejected insecure temp-root candidates should not emit a physical directory path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_relative_physical_pwd_output() {
	candidate="$TEST_TMPDIR/validate_tmp_root_relative_pwd"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		pwd() {
			printf '%s\n' 'relative-path'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should reject non-absolute physical-directory results." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Rejected relative physical-directory results should not emit a temp-root path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_create_secure_staging_dir_for_path_returns_failure_when_parent_lookup_fails() {
	stage_path="$TEST_TMPDIR/create_secure_staging_parent_lookup/backup.meta"

	zxfer_test_capture_subshell "
		zxfer_get_path_parent_dir() {
			return 1
		}
		zxfer_create_secure_staging_dir_for_path \"$stage_path\" >/dev/null
	"

	assertEquals "Secure same-directory staging should fail closed when the parent-path lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_create_secure_staging_dir_for_path_returns_failure_when_parent_validation_fails() {
	stage_root="$TEST_TMPDIR/create_secure_staging_parent_validation"
	stage_path="$stage_root/backup.meta"
	mkdir -p "$stage_root"

	zxfer_test_capture_subshell "
		zxfer_validate_temp_root_candidate() {
			return 1
		}
		zxfer_create_secure_staging_dir_for_path \"$stage_path\" >/dev/null
	"

	assertEquals "Secure same-directory staging should fail closed when the parent directory is not a trusted temp-root candidate." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_try_get_effective_tmpdir_reuses_cached_value_in_current_shell() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	cached_tmp="$physical_tmpdir/effective_tmp_cached"
	mkdir -p "$cached_tmp"
	TMPDIR="$cached_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	first_out="$TEST_TMPDIR/effective_tmp_first.out"
	second_out="$TEST_TMPDIR/effective_tmp_second.out"
	zxfer_try_get_effective_tmpdir >"$first_out"
	first_status=$?
	zxfer_try_get_effective_tmpdir >"$second_out"
	second_status=$?

	assertEquals "The first effective TMPDIR lookup should succeed for a trusted directory." \
		0 "$first_status"
	assertEquals "Repeated effective TMPDIR lookups should reuse the cached value." \
		0 "$second_status"
	assertEquals "The first lookup should return the trusted TMPDIR path." \
		"$cached_tmp" "$(cat "$first_out")"
	assertEquals "The cached lookup should return the same TMPDIR path." \
		"$cached_tmp" "$(cat "$second_out")"
	assertEquals "The cached TMPDIR path should remain stored in the current shell." \
		"$cached_tmp" "$g_zxfer_effective_tmpdir"
	assertEquals "The cached TMPDIR request key should remain stored in the current shell." \
		"$cached_tmp" "$g_zxfer_effective_tmpdir_requested"

	TMPDIR="$TEST_TMPDIR"
}

test_zxfer_create_private_temp_dir_returns_failure_when_effective_tmpdir_lookup_fails_in_current_shell() {
	zxfer_try_get_effective_tmpdir() {
		return 1
	}

	zxfer_create_private_temp_dir "zxfer_private_tmp" >"$TEST_TMPDIR/private_temp_dir.out" 2>/dev/null
	status=$?

	unset -f zxfer_try_get_effective_tmpdir

	assertEquals "Private temp directory creation should fail when the effective temp root cannot be determined." \
		1 "$status"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
