#!/bin/sh
#
# Basic shunit2 tests for zxfer_common.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2317,SC2329

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_inspect_delete_snap.sh
. "$ZXFER_ROOT/src/zxfer_inspect_delete_snap.sh"

# shellcheck source=src/zxfer_transfer_properties.sh
. "$ZXFER_ROOT/src/zxfer_transfer_properties.sh"

# shellcheck source=src/zxfer_get_zfs_list.sh
. "$ZXFER_ROOT/src/zxfer_get_zfs_list.sh"

# shellcheck source=src/zxfer_zfs_send_receive.sh
. "$ZXFER_ROOT/src/zxfer_zfs_send_receive.sh"

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

create_fake_parallel_exec_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
if [ "$1" = "--version" ]; then
	printf '%s\n' "GNU parallel (fake)"
	exit 0
fi

while [ $# -gt 0 ]; do
	case "$1" in
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

usage() {
	printf '%s\n' "usage: zxfer"
}

# Some macOS sandboxes report sysconf(_SC_ARG_MAX) failures when invoking
# /usr/bin/xargs without arguments. Provide a shell stub for the shunit2 lookup
# that mirrors the behavior needed by _shunit_extractTestFunctions().
xargs() {
	if command [ "$#" -eq 0 ]; then
		tr '\n' ' ' | sed 's/[[:space:]]\+/ /g; s/^ //; s/ $//'
	else
		command xargs "$@"
	fi
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_shunit.XXXXXX)
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
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	# Reset the global option flags before each test so we always start from a
	# consistent CLI state, and isolate each test to its own temp directory.
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
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
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
	create_fake_ssh_bin
	create_fake_parallel_bin "$FAKE_PARALLEL_BIN"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_cat=""
	g_origin_cmd_zfs=""
	g_target_cmd_zfs=""
	g_origin_parallel_cmd=""
	g_LZFS=""
	g_RZFS=""
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_zxfer_original_invocation=""
	g_option_Y_yield_iterations=1
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
		get_path_owner_uid() { printf '%s\n' "0"; }
		get_path_mode_octal() { printf '%s\n' "600"; }
		read_local_backup_file "$l_path"
	)
}

fake_property_set_runner() {
	FAKE_SET_CALLS="${FAKE_SET_CALLS}${1}=${2}@${3};"
}

fake_property_inherit_runner() {
	FAKE_INHERIT_CALLS="${FAKE_INHERIT_CALLS}${1}@${2};"
}

property_set_logger() {
	[ -n "${PROPERTY_LOG:-}" ] || return 1
	printf 'set %s=%s %s\n' "$1" "$2" "$3" >>"$PROPERTY_LOG"
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

	result=$(escape_for_single_quotes "$input")

	assertEquals "Input should be properly escaped for single quotes." "$expected" "$result"
}

test_split_host_spec_tokens_handles_multi_word_hosts() {
	# Host specs may append privilege wrappers like "pfexec" or ssh options.
	result=$(split_host_spec_tokens "user@host pfexec -p 2222")
	expected=$(printf "%s\n" "user@host" "pfexec" "-p" "2222")

	assertEquals "Host spec should be split into whitespace-delimited tokens." "$expected" "$result"
}

test_quote_host_spec_tokens_neutralizes_metacharacters() {
	# Ensure characters such as semicolons are quoted so they cannot escape
	# into new local commands when eval'd later.
	result=$(quote_host_spec_tokens "backup.example.com; touch /tmp/pwn")
	expected="'backup.example.com;' 'touch' '/tmp/pwn'"

	assertEquals "Host spec should be rendered as safely quoted tokens." "$expected" "$result"
}

test_quote_cli_tokens_preserves_argument_boundaries() {
	# Compression commands should behave like arrays, preserving each argument.
	result=$(quote_cli_tokens "zstd -3 --long=27")
	expected="'zstd' '-3' '--long=27'"

	assertEquals "CLI tokens should be individually quoted." "$expected" "$result"
}

test_quote_cli_tokens_blocks_shell_metacharacters() {
	# Metacharacters such as ';' or '|' must be neutralized instead of being
	# interpreted as new commands or pipelines.
	result=$(quote_cli_tokens "zstd -3; touch /tmp/pwn | cat")
	expected="'zstd' '-3;' 'touch' '/tmp/pwn' '|' 'cat'"

	assertEquals "CLI tokens should remain literal even with metacharacters." "$expected" "$result"
}

test_split_tokens_on_whitespace_breaks_metacharacters() {
	result=$(split_tokens_on_whitespace "cmd;rm -rf|grep foo&echo done")
	expected=$(printf '%s\n' "cmd;" "rm" "-rf|" "grep" "foo&" "echo" "done")

	assertEquals "Tokenizer should break arguments on whitespace while leaving metacharacters literal." "$expected" "$result"
}

test_split_tokens_on_whitespace_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/split_tokens_empty.out"

	split_tokens_on_whitespace "" >"$outfile"

	assertEquals "Blank token streams should produce no output." "" "$(cat "$outfile")"
}

test_quote_token_stream_preserves_each_token() {
	tokens=$(printf '%s\n' "alpha" "beta value" "" "gamma")
	result=$(quote_token_stream "$tokens")
	expected="'alpha' 'beta value' 'gamma'"

	assertEquals "Token stream quoting should ignore blank lines and wrap each entry." "$expected" "$result"
}

test_quote_host_spec_tokens_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_host_empty.out"

	quote_host_spec_tokens "" >"$outfile"

	assertEquals "Blank host specs should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_cli_tokens_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_cli_empty.out"

	quote_cli_tokens "" >"$outfile"

	assertEquals "Blank CLI strings should remain blank after quoting." "" "$(cat "$outfile")"
}

test_run_zfs_cmd_for_spec_routes_to_source_runner() {
	# shellcheck disable=SC2030,SC2031
	result=$(
		g_LZFS="/sbin/zfs"
		g_RZFS="/usr/sbin/zfs"
		run_source_zfs_cmd() { printf 'source %s %s\n' "$1" "$2"; }
		run_destination_zfs_cmd() { printf 'destination %s\n' "$1"; }
		run_zfs_cmd_for_spec "/sbin/zfs" list tank/fs
	)

	assertEquals "Spec matching g_LZFS should call run_source_zfs_cmd." "source list tank/fs" "$result"
}

test_run_zfs_cmd_for_spec_routes_to_destination_runner() {
	# shellcheck disable=SC2030,SC2031
	result=$(
		g_LZFS="/sbin/zfs"
		g_RZFS="/usr/sbin/zfs"
		run_source_zfs_cmd() { printf 'source %s\n' "$1"; }
		run_destination_zfs_cmd() { printf 'destination %s %s\n' "$1" "$2"; }
		run_zfs_cmd_for_spec "/usr/sbin/zfs" get name tank/dst
	)

	assertEquals "Spec matching g_RZFS should call run_destination_zfs_cmd." "destination get name" "$result"
}

test_run_zfs_cmd_for_spec_executes_literal_command_when_not_wrapped() {
	tool="$TEST_TMPDIR/echo_tool"
	cat >"$tool" <<'EOF'
#!/bin/sh
echo "$@"
EOF
	chmod +x "$tool"

	result=$(run_zfs_cmd_for_spec "$tool" alpha beta)

	assertEquals "Arbitrary command specs should be executed directly." "alpha beta" "$result"
}

test_sanitize_backup_component_replaces_invalid_characters() {
	result=$(sanitize_backup_component "../unsafe path!")
	assertEquals ".._unsafe_path_" "$result"
}

test_sanitize_dataset_relpath_normalizes_nested_segments() {
	result=$(sanitize_dataset_relpath "/tank/src//child-")
	assertEquals "tank/src/child-" "$result"
}

test_get_backup_storage_dir_derives_detached_layout() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"
	result=$(get_backup_storage_dir "-" "tank/src/child")
	expected="$g_backup_storage_root/detached/tank/src/child"

	assertEquals "Detached mountpoints should include sanitized dataset path." "$expected" "$result"
}

test_backup_owner_uid_is_allowed_accepts_root_and_effective_uid() {
	result_root=$(
		get_effective_user_uid() { printf '%s\n' 1000; }
		if backup_owner_uid_is_allowed 0; then echo ok; else echo fail; fi
	)
	assertEquals "Root must always be allowed." "ok" "$result_root"

	result_user=$(
		get_effective_user_uid() { printf '%s\n' 4242; }
		if backup_owner_uid_is_allowed 4242; then echo ok; else echo fail; fi
	)
	assertEquals "Effective UID should be permitted when matching the owner." "ok" "$result_user"
}

test_describe_expected_backup_owner_includes_effective_uid_when_non_root() {
	result=$(
		get_effective_user_uid() { printf '%s\n' 9999; }
		describe_expected_backup_owner
	)
	assertEquals "root (UID 0) or UID 9999" "$result"
}

test_require_secure_backup_file_rejects_non_0600_permissions() {
	tmp_file="$TEST_TMPDIR/insecure_backup"
	: >"$tmp_file"
	(
		throw_error() {
			printf 'ERROR:%s' "$1"
			exit "${2:-1}"
		}
		get_path_owner_uid() { printf '%s\n' 0; }
		get_path_mode_octal() { printf '%s\n' 644; }
		require_secure_backup_file "$tmp_file"
	) >/dev/null 2>&1
	status=$?
	assertEquals "Insecure permissions should trigger an error." 1 "$status"
}

test_require_secure_backup_file_accepts_secure_metadata() {
	tmp_file="$TEST_TMPDIR/secure_backup"
	: >"$tmp_file"
	(
		throw_error() {
			echo "unexpected"
			exit "${2:-1}"
		}
		get_path_owner_uid() { printf '%s\n' 0; }
		get_path_mode_octal() { printf '%s\n' 600; }
		require_secure_backup_file "$tmp_file"
	)
	status=$?
	assertEquals "Secure metadata should pass validation." 0 "$status"
}

test_ensure_local_backup_dir_creates_secure_directory() {
	l_dir="$TEST_TMPDIR/local_backup"
	rm -rf "$l_dir"
	ensure_local_backup_dir "$l_dir"
	assertTrue "Secure directory should be created." "[ -d '$l_dir' ]"
	perms=$(stat -c '%a' "$l_dir" 2>/dev/null || stat -f '%Lp' "$l_dir" 2>/dev/null)
	assertEquals "Backup directory must be chmod 700." "700" "$perms"
}

test_invoke_ssh_command_for_host_preserves_argument_boundaries() {
	fake_cmd="$FAKE_SSH_BIN -q -p 2222"
	host_spec="backup@example.com pfexec doas"
	log_file="$TEST_TMPDIR/invoke_cmd.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	invoke_ssh_command_for_host "$fake_cmd" "$host_spec" "--" "cmd arg" "with spaces" "umask 077; cat > /tmp/backup"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' "-q" "-p" "2222" "backup@example.com" "pfexec" "doas" "--" "cmd arg" "with spaces" "umask 077; cat > /tmp/backup")
	result=$(cat "$log_file")

	assertEquals "ssh helper should keep multi-word host specs and remote commands intact." "$expected" "$result"
}

test_invoke_ssh_command_for_host_runs_without_remote_args() {
	outfile="$TEST_TMPDIR/invoke_ssh_noargs.out"

	invoke_ssh_command_for_host "$FAKE_SSH_BIN" "" >"$outfile"

	assertEquals "ssh helpers should still invoke the base command when no host or remote argv is provided." \
		"$FAKE_SSH_BIN" "$(cat "$outfile")"
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

	remote_log="$TEST_TMPDIR/run_source_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	run_source_zfs_cmd list tank/fs@snap

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	{
		IFS= read -r arg1
		IFS= read -r arg2
		IFS= read -r arg3
		IFS= read -r arg4
		IFS= read -r arg5
		IFS= read -r arg6
		IFS= read -r arg7
	} <"$remote_log"

	assertEquals "ssh should target the origin host without literal quotes." "backup@example.com" "$arg1"
	assertEquals "Privilege wrappers must remain separate tokens." "pfexec" "$arg2"
	assertEquals "Port flag should pass through literally." "-p" "$arg3"
	assertEquals "Port argument should remain intact." "2222" "$arg4"
	assertEquals "zfs binary should use the origin-host path." "$g_origin_cmd_zfs" "$arg5"
	assertEquals "Remote command should preserve requested subcommand." "list" "$arg6"
	assertEquals "Dataset argument should not be re-quoted locally." "tank/fs@snap" "$arg7"

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

	run_source_zfs_cmd list -H tank/src >"$outfile"

	assertEquals "The default local source path should execute the resolved zfs binary directly." \
		"list -H tank/src" "$(cat "$outfile")"
	assertEquals "Default local source execution should still record the last command." \
		"'$fake_zfs' 'list' '-H' 'tank/src'" "$g_zxfer_failure_last_command"
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

	remote_log="$TEST_TMPDIR/run_destination_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	run_destination_zfs_cmd get -H name tank/dst

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	{
		IFS= read -r targ1
		IFS= read -r targ2
		IFS= read -r targ3
		IFS= read -r targ4
		IFS= read -r targ5
		IFS= read -r targ6
		IFS= read -r targ7
	} <"$remote_log"

	assertEquals "ssh should connect to the target host without stray quotes." "target@example.com" "$targ1"
	assertEquals "Additional host-spec tokens must survive argument splitting." "doas" "$targ2"
	assertEquals "Remote call should include the target-host zfs path." "$g_target_cmd_zfs" "$targ3"
	assertEquals "Command verb should pass through untouched." "get" "$targ4"
	assertEquals "Original flags should be preserved." "-H" "$targ5"
	assertEquals "Property argument should pass through verbatim." "name" "$targ6"
	assertEquals "Dataset argument should remain literal." "tank/dst" "$targ7"

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

	run_destination_zfs_cmd get name tank/dst >"$outfile"

	assertEquals "The default local destination path should execute the resolved zfs binary directly." \
		"get name tank/dst" "$(cat "$outfile")"
	assertEquals "Default local destination execution should still record the last command." \
		"'$fake_zfs' 'get' 'name' 'tank/dst'" "$g_zxfer_failure_last_command"
}

test_refresh_compression_commands_tokenizes_custom_pipeline() {
	# When -Z/ZXFER_COMPRESSION supplies a custom command, ensure zxfer stores
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

	refresh_compression_commands

	assertEquals "Compression command tokens should be quoted." "'zstd' '-3;' 'touch' '/tmp/pwn'" "$g_cmd_compress_safe"

	if [ $old_g_cmd_compress_set -eq 1 ]; then
		g_cmd_compress=$old_g_cmd_compress
	else
		unset g_cmd_compress
	fi
	if [ $old_g_cmd_decompress_set -eq 1 ]; then
		g_cmd_decompress=$old_g_cmd_decompress
	else
		unset g_cmd_decompress
	fi
	if [ $old_g_option_z_compress_set -eq 1 ]; then
		g_option_z_compress=$old_g_option_z_compress
	else
		unset g_option_z_compress
	fi
	if [ $old_g_cmd_compress_safe_set -eq 1 ]; then
		g_cmd_compress_safe=$old_g_cmd_compress_safe
	else
		unset g_cmd_compress_safe
	fi
	if [ $old_g_cmd_decompress_safe_set -eq 1 ]; then
		g_cmd_decompress_safe=$old_g_cmd_decompress_safe
	else
		unset g_cmd_decompress_safe
	fi
}

test_derive_override_lists_handles_overrides_only() {
	result=$(derive_override_lists "compression=lz4=local" "compression=lzjb" 0 "filesystem")

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$result
EOF

	assertEquals "Override list should reflect -o values with override sources." "compression=lzjb=override" "$override_pvs"
	assertEquals "Creation list should stay empty when only -o is supplied." "" "$creation_pvs"
}

test_derive_override_lists_includes_local_props_for_creation() {
	source_pvs="compression=lz4=local,refreservation=4G=received,quota=none=local"
	override_opts="quota=8G"
	result=$(derive_override_lists "$source_pvs" "$override_opts" 1 "volume")

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$result
EOF

	expected_override="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Overrides should include source properties with user overrides applied." "$(sort_property_list "$expected_override")" "$(sort_property_list "$override_pvs")"
	assertEquals "Creation list should keep local props and zvol refreservation even if not local." "compression=lz4=local,refreservation=4G=received" "$creation_pvs"
}

test_diff_properties_separates_set_and_inherit_lists() {
	override_pvs="compression=lz4=local,atime=off=received"
	dest_pvs="compression=lzjb=local,atime=on=local"
	result=$(diff_properties "$override_pvs" "$dest_pvs" "casesensitivity,normalization,jailed,utf8only")

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

	apply_property_changes "pool/src" 1 "compression=lz4" "" "" fake_property_set_runner fake_property_inherit_runner

	assertEquals "Initial source should call the set runner with initial diff list." "compression=lz4@pool/src;" "$FAKE_SET_CALLS"
	assertEquals "Initial source should not inherit properties." "" "$FAKE_INHERIT_CALLS"
}

test_apply_property_changes_invokes_inherit_runner_for_children() {
	FAKE_SET_CALLS=""
	FAKE_INHERIT_CALLS=""

	apply_property_changes "pool/src" 0 "" "compression=lz4" "atime=off" fake_property_set_runner fake_property_inherit_runner

	assertEquals "Child dataset should apply child set list." "compression=lz4@pool/src;" "$FAKE_SET_CALLS"
	assertEquals "Child dataset should inherit requested properties." "atime@pool/src;" "$FAKE_INHERIT_CALLS"
}

test_strip_trailing_slashes_trims_dataset_suffixes() {
	# Datasets may be provided with a trailing slash; ensure we drop all trailing
	# separators so concatenated child names never gain a double slash.
	result=$(strip_trailing_slashes "pool/dst///")
	assertEquals "Trailing slashes should be removed." "pool/dst" "$result"

	result=$(strip_trailing_slashes "pool/dst")
	assertEquals "Paths without trailing slashes should be unchanged." "pool/dst" "$result"
}

test_strip_trailing_slashes_preserves_absolute_placeholders() {
	# Absolute paths are rejected later, so inputs that consist entirely of
	# slashes must be passed through untouched.
	result=$(strip_trailing_slashes "/")
	assertEquals "Single slash inputs should be preserved." "/" "$result"

	result=$(strip_trailing_slashes "")
	assertEquals "Empty inputs should stay empty." "" "$result"
}

test_execute_command_respects_dry_run_mode() {
	# With --dry-run enabled, execute_command should not run but still
	# describe the action, so no temp files should be created.
	temp_file="$TEST_TMPDIR/dry_run_output"
	g_option_n_dryrun=1

	execute_command "printf 'should not run' > '$temp_file'"

	assertFalse "Dry run should skip running the command." "[ -f \"$temp_file\" ]"
}

test_execute_command_runs_command_when_not_dry_run() {
	# When --dry-run is off, the helper must execute commands verbatim.
	temp_file="$TEST_TMPDIR/run_output"

	execute_command "printf 'ran' > '$temp_file'"

	assertTrue "Command should run when dry run is disabled." "[ -f \"$temp_file\" ]"
	assertEquals "ran" "$(cat "$temp_file")"
}

test_get_temp_file_creates_unique_file() {
	# get_temp_file should provide unique temp files so concurrent options do
	# not collide or overwrite each other.
	file_one=$(get_temp_file)
	file_two=$(get_temp_file)

	assertTrue "First temp file should exist." "[ -f \"$file_one\" ]"
	assertTrue "Second temp file should exist." "[ -f \"$file_two\" ]"
	assertNotEquals "Two consecutive temp file names should be unique." "$file_one" "$file_two"

	rm -f "$file_one" "$file_two"
}

test_get_temp_file_honors_tmpdir_variable() {
	# Honor the TMPDIR override so tests or CLI invocations can direct
	# scratch files to a specific filesystem.
	custom_tmp="$TEST_TMPDIR/custom"
	mkdir -p "$custom_tmp"
	TMPDIR="$custom_tmp"

	file=$(get_temp_file)

	case "$file" in
	"$custom_tmp"/*) inside=0 ;;
	*) inside=1 ;;
	esac

	assertEquals "Temp file should be created inside TMPDIR." 0 "$inside"
	assertTrue "Temp file should exist." "[ -f \"$file\" ]"

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
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			get_temp_file
		)
	)
	status=$?

	assertEquals "Temporary-file allocation failures should abort." 1 "$status"
	assertContains "Temporary-file allocation failures should use the documented error." \
		"$output" "Error creating temporary file."
}

test_echov_outputs_only_when_verbose_enabled() {
	# echov should emit text only when -v/--verbose is set.
	g_option_v_verbose=1
	output=$(echov "verbose message")

	assertEquals "verbose message" "$output"

	g_option_v_verbose=0
	output=$(echov "hidden message")

	assertEquals "" "$output"
}

test_echoV_outputs_only_when_very_verbose_enabled() {
	# echoV uses -V/--very-verbose, so it should stay quiet unless the
	# highest verbosity level is requested.
	g_option_V_very_verbose=1
	output=$(echoV "debug message" 2>&1)

	assertEquals "debug message" "$output"

	g_option_V_very_verbose=0
	output=$(echoV "hidden debug" 2>&1)

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
			beep 1
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
			beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without /dev/speaker should skip beep handling with a debug message." \
		"$output" "Beep requested but /dev/speaker missing; skipping."
}

test_beep_skips_when_speaker_tools_are_missing() {
	output=$(
		(
			uname() {
				printf '%s\n' "FreeBSD"
			}
			mkdir -p "$TEST_TMPDIR/no_speaker_tools"
			# shellcheck disable=SC2123
			PATH="$TEST_TMPDIR/no_speaker_tools"
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without speaker helper tools should skip beep handling with a debug message." \
		"$output" "Beep requested but speaker tools are missing; skipping."
}

test_execute_background_cmd_writes_output_file() {
	# Background commands are used for option pipelines; ensure their stdout
	# still lands in the provided tempfile.
	temp_file="$TEST_TMPDIR/bg_output"

	execute_background_cmd "printf bg-data" "$temp_file"
	bg_pid=$!
	wait "$bg_pid"

	assertTrue "Background output file should be created." "[ -f \"$temp_file\" ]"
	assertEquals "bg-data" "$(cat "$temp_file")"
}

test_exists_destination_returns_one_on_success() {
	# exists_destination checks the remote ZFS command stored in g_RZFS;
	# when the check succeeds, the helper returns 1.
	# shellcheck disable=SC2031
	old_g_RZFS=${g_RZFS-}
	g_RZFS=true

	result=$(exists_destination "pool/fs")

	assertEquals "Destination should exist when command succeeds." "1" "$result"

	g_RZFS=$old_g_RZFS
}

test_exists_destination_returns_zero_on_failure() {
	# When the remote ZFS check fails, the helper should return 0 so callers
	# can detect that the destination needs to be created.
	# shellcheck disable=SC2031
	old_g_RZFS=${g_RZFS-}
	g_RZFS=false

	result=$(exists_destination "pool/fs")

	assertEquals "Destination should not exist when command fails." "0" "$result"

	g_RZFS=$old_g_RZFS
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

	initial_source="pool/src"
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
	g_backup_file_contents=";$initial_source,$g_destination,user:note=\$(touch $sentinel_file)"

	write_backup_properties

	l_tail=${initial_source##*/}
	secure_dir=$(get_backup_storage_dir "$mount_dir" "$g_destination")
	backup_file="$secure_dir/$g_backup_file_extension.$l_tail"

	assertTrue "Backup property file should be written." "[ -f \"$backup_file\" ]"
	assertFalse "Backup file must not be written into dataset mountpoints." "[ -f \"$mount_dir/$g_backup_file_extension.$l_tail\" ]"
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

	initial_source="pool/src"
	g_destination="pool/dst"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents=""

	write_backup_properties

	assertFalse "Backup metadata should not be written when no properties were collected." "[ -d \"$g_backup_storage_root\" ]"

	if [ -n "${old_g_backup_storage_root-}" ]; then
		g_backup_storage_root=$old_g_backup_storage_root
	else
		unset g_backup_storage_root
	fi
}

test_read_local_backup_file_refuses_non_root_owned_metadata() {
	# read_local_backup_file must refuse to parse metadata when the file is
	# not root-owned to prevent tampering from less-privileged users. Stub
	# the ownership/mode helpers so the test does not rely on the invoking
	# user's UID or default umask.
	backup_file="$TEST_TMPDIR/insecure_backup"
	printf '%s\n' "tampered" >"$backup_file"
	chmod 600 "$backup_file"

	if output=$(
		(
			get_path_owner_uid() { printf '%s\n' "1234"; }
			get_path_mode_octal() { printf '%s\n' "600"; }
			read_local_backup_file "$backup_file"
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
		fail "read_local_backup_file did not report an insecure owner: $output"
		;;
	esac

	rm -f "$backup_file"
}

test_read_local_backup_file_returns_contents_when_secure() {
	# When metadata ownership and permissions pass validation, the helper
	# should return the literal on-disk contents.
	backup_file="$TEST_TMPDIR/secure_backup"
	printf '%s\n' "trusted" >"$backup_file"

	result=$(read_backup_file_with_mocked_security "$backup_file")

	assertEquals "trusted" "$result"
	rm -f "$backup_file"
}

test_build_source_snapshot_list_cmd_serial_returns_direct_list() {
	g_LZFS="/sbin/zfs"
	initial_source="tank/data"
	g_option_j_jobs=1

	result=$(build_source_snapshot_list_cmd)

	assertEquals "Serial snapshot listing should call zfs directly." "$g_LZFS list -Hr -o name -s creation -t snapshot $initial_source" "$result"
}

test_build_source_snapshot_list_cmd_parallel_local_includes_parallel_runner() {
	g_LZFS="/sbin/zfs"
	initial_source="tank/home"
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_option_O_origin_host=""
	g_option_z_compress=0

	result=$(build_source_snapshot_list_cmd)

	assertContains "Parallel listing should include dataset enumeration." "$result" "$g_LZFS list -Hr -o name $initial_source |"
	assertContains "GNU parallel invocation should include the job count." "$result" "\"$g_cmd_parallel\" -j 4 --line-buffer"
	assertEquals "Local parallel snapshot listing should use the direct GNU parallel runner from development." \
		"$g_LZFS list -Hr -o name $initial_source | \"$g_cmd_parallel\" -j 4 --line-buffer '$g_LZFS list -H -o name -s creation -d 1 -t snapshot {}'" "$result"
	assertNotContains "Local parallel snapshot listing should not reintroduce a sh -c wrapper." "$result" "sh -c"
}

test_build_source_snapshot_list_cmd_remote_with_compression_sets_ssh_pipeline() {
	g_LZFS="/sbin/zfs"
	g_cmd_zfs="/usr/sbin/zfs"
	g_origin_cmd_zfs="/opt/openzfs/bin/zfs"
	initial_source="tank/src"
	g_option_j_jobs=8
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_option_O_origin_host="backup@example.com pfexec -p 2222"
	g_option_O_origin_host_safe=""
	g_option_z_compress=1
	g_cmd_ssh="/usr/bin/ssh"

	result=$(build_source_snapshot_list_cmd)

	assertContains "Remote listing should start with ssh." "$result" "$g_cmd_ssh"
	assertContains "The ssh target host should remain a standalone local argument." "$result" "'backup@example.com'"
	assertContains "Wrapper tokens should remain inside the remote command string." "$result" "'pfexec'"
	assertContains "Wrapper flags should remain inside the remote command string." "$result" "'-p'"
	assertContains "Wrapper flag values should remain inside the remote command string." "$result" "'2222'"
	assertContains "Remote GNU parallel path should be used." "$result" "\"/opt/bin/parallel\" -j 8 --line-buffer"
	assertContains "Remote listing should use the origin host zfs path." "$result" "$g_origin_cmd_zfs"
	assertContains "Compression should be applied when requested (zstd -9 present)." "$result" "zstd -9"
	assertContains "Compression should be applied when requested (zstd -d present)." "$result" "zstd -d"
	assertContains "Remote command should use GNU parallel's direct dataset placeholder runner." \
		"$result" "$g_origin_cmd_zfs list -H -o name -s creation -d 1 -t snapshot {}"
}

test_resolve_remote_required_tool_uses_shell_probe_for_wrapped_hosts() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	remote_log="$TEST_TMPDIR/resolve_remote_required_tool.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="/remote/bin/zfs"
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(resolve_remote_required_tool "backup@example.com pfexec -p 2222" zfs)

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote tool lookup should return the resolved absolute path." "/remote/bin/zfs" "$result"
	{
		IFS= read -r log_line1
		IFS= read -r log_line2
	} <"$remote_log"

	assertEquals "Host token should remain the ssh target." "backup@example.com" "$log_line1"
	assertContains "Privilege wrapper should be preserved inside the remote command string." "$log_line2" "'pfexec'"
	assertContains "Wrapper flags should be preserved inside the remote command string." "$log_line2" "'-p'"
	assertContains "Wrapper flag values should be preserved inside the remote command string." "$log_line2" "'2222'"
	assertContains "Remote lookup should execute via sh -c for wrapped hosts." "$log_line2" "'sh' '-c'"
	assertContains "Remote lookup should pin the secure PATH inside the shell probe." "$log_line2" "/opt/openzfs/bin:/usr/sbin"
	assertContains "Remote lookup should query the requested tool directly." "$log_line2" "command -v"
	assertContains "Remote lookup should include the requested tool name in the remote shell command." "$log_line2" "zfs"
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

	result=$(resolve_remote_required_tool "backup@example.com" zfs)

	unset FAKE_SSH_LOG

	assertEquals "Remote lookup should survive ssh joining the remote command into a shell string." "$remote_bin_dir/zfs" "$result"
	assertContains "The realistic ssh emulation should receive the expected remote shell command." \
		"$(cat "$realistic_ssh_log")" "command -v"
	assertContains "The realistic ssh emulation should include the requested tool name." \
		"$(cat "$realistic_ssh_log")" "zfs"
}

test_resolve_remote_required_tool_reports_remote_probe_failures() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_SUPPRESS_STDOUT=1
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	set +e
	result=$(resolve_remote_required_tool "backup@example.com" zfs "zfs")
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote lookup should fail when ssh cannot execute the probe." 1 "$status"
	assertEquals "Remote lookup failures should not be misreported as missing binaries." \
		"Failed to query dependency \"zfs\" on host backup@example.com." "$result"
}

test_resolve_remote_required_tool_reports_missing_remote_dependency() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_SUPPRESS_STDOUT

	set +e
	result=$(resolve_remote_required_tool "backup@example.com" zfs "zfs")
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Remote lookup should fail when the secure PATH probe returns no result." 1 "$status"
	assertEquals "Missing remote tools should mention the secure PATH guidance." \
		"Required dependency \"zfs\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$result"
}

test_resolve_remote_required_tool_treats_exit_1_without_output_as_missing_dependency() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_dependency_path="/opt/openzfs/bin:/usr/sbin"
	FAKE_SSH_SUPPRESS_STDOUT=1
	FAKE_SSH_EXIT_STATUS=1
	export FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	set +e
	result=$(resolve_remote_required_tool "backup@example.com" parallel "GNU parallel")
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote lookup should treat exit status 1 without output as a missing dependency." 1 "$status"
	assertEquals "Exit-1 command -v probes should map to the user-facing missing dependency guidance." \
		"Required dependency \"GNU parallel\" not found on host backup@example.com in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary." \
		"$result"
}

test_resolve_remote_required_tool_rejects_relative_remote_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_STDOUT_OVERRIDE="zfs"
	export FAKE_SSH_STDOUT_OVERRIDE

	set +e
	result=$(resolve_remote_required_tool "backup@example.com" zfs "zfs")
	status=$?

	unset FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote lookup should fail when the remote probe returns a non-absolute path." 1 "$status"
	assertEquals "Relative remote tool paths should be rejected explicitly." \
		"Required dependency \"zfs\" on host backup@example.com resolved to \"zfs\", but zxfer requires an absolute path." \
		"$result"
}

test_init_variables_resolves_remote_tool_paths_and_restore_cat() {
	result=$(
		get_os() {
			if [ "$1" = "" ]; then
				printf '%s\n' "LocalOS"
			else
				printf '%s\n' "RemoteOS"
			fi
		}
		resolve_remote_required_tool() {
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
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_zfs="/sbin/zfs"
		g_option_O_origin_host="origin.example pfexec"
		g_option_O_origin_host_safe="'origin.example' 'pfexec'"
		g_option_T_target_host="target.example doas"
		g_option_T_target_host_safe="'target.example' 'doas'"
		g_option_e_restore_property_mode=1
		init_variables
		printf 'source_os=%s\n' "$g_source_operating_system"
		printf 'dest_os=%s\n' "$g_destination_operating_system"
		printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
		printf 'lzfs=%s\n' "$g_LZFS"
		printf 'rzfs=%s\n' "$g_RZFS"
		printf 'cat=%s\n' "$g_cmd_cat"
	)

	assertContains "Origin OS should be populated from remote get_os()." "$result" "source_os=RemoteOS"
	assertContains "Destination OS should be populated from remote get_os()." "$result" "dest_os=RemoteOS"
	assertContains "Origin zfs path should use the remote lookup result." "$result" "origin_zfs=/remote/origin/zfs"
	assertContains "Target zfs path should use the remote lookup result." "$result" "target_zfs=/remote/target/zfs"
	assertContains "g_LZFS should incorporate the remote origin zfs path." "$result" "lzfs=/usr/bin/ssh 'origin.example' 'pfexec' /remote/origin/zfs"
	assertContains "g_RZFS should incorporate the remote target zfs path." "$result" "rzfs=/usr/bin/ssh 'target.example' 'doas' /remote/target/zfs"
	assertContains "Restore mode should resolve cat on the origin host." "$result" "cat=/remote/origin/cat"
}

test_init_variables_marks_remote_zfs_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			get_os() {
				printf '%s\n' "RemoteOS"
			}
			resolve_remote_required_tool() {
				printf '%s\n' "lookup failed"
				return 1
			}
			throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_cmd_zfs="/sbin/zfs"
			g_option_O_origin_host="origin.example"
			g_option_O_origin_host_safe="'origin.example'"
			init_variables
		)
	)
	status=$?

	assertEquals "Remote zfs lookup failures should abort init_variables." 1 "$status"
	assertContains "Remote zfs lookup failures should be classified as dependency errors." "$output" "class=dependency"
	assertContains "Remote zfs lookup failures should surface the lookup message." "$output" "msg=lookup failed"
}

test_ensure_parallel_remote_fetches_remote_parallel_path() {
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	g_origin_parallel_cmd=""
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""

	FAKE_SSH_SUPPRESS_STDOUT=1 setup_ssh_control_socket "$g_option_O_origin_host" "origin"
	unset FAKE_SSH_SUPPRESS_STDOUT

	remote_log="$TEST_TMPDIR/remote_parallel_probe.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="/opt/bin/parallel"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT

	ensure_parallel_available_for_source_jobs

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT

	assertEquals "Remote GNU parallel path should be detected via ssh." "/opt/bin/parallel" "$g_origin_parallel_cmd"
	{
		IFS= read -r log_line1
		IFS= read -r log_line2
		IFS= read -r log_line3
		IFS= read -r log_line4
	} <"$remote_log"

	assertEquals "ssh should reuse the established control socket." "-S" "$log_line1"
	assertEquals "SSH must pass the control socket path as the next argument." "$g_ssh_origin_control_socket" "$log_line2"
	assertEquals "ssh should direct probes at the origin host." "$g_option_O_origin_host" "$log_line3"
	assertContains "Remote probe should execute via sh -c so wrapper host specs stay valid." "$log_line4" "'sh' '-c'"
	assertContains "Remote probe should pin the secure PATH inside the shell probe." "$log_line4" "$g_zxfer_dependency_path"
	assertContains "Remote probe should look up the requested tool directly." "$log_line4" "command -v"
	assertContains "Remote probe should include the requested tool name." "$log_line4" "parallel"
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
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote GNU parallel probe failures should abort the helper." 1 "$status"
	assertContains "Remote GNU parallel probe failures should preserve the query failure message." \
		"$output" "Failed to query dependency \"GNU parallel\" on host aldo@172.16.0.4 pfexec."
}

test_ensure_parallel_available_for_source_jobs_reports_missing_remote_parallel() {
	set +e
	output=$(
		(
			resolve_remote_required_tool() {
				printf '%s\n' "Required dependency \"GNU parallel\" not found on host origin.example in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_j_jobs=4
			g_cmd_parallel="$FAKE_PARALLEL_BIN"
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""
			ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Missing remote GNU parallel should abort the helper." 1 "$status"
	assertContains "Missing remote GNU parallel should be translated into the user-facing guidance." \
		"$output" "GNU parallel not found on origin host origin.example but -j 4 was requested. Install GNU parallel remotely or rerun without -j."
}

test_read_remote_backup_file_uses_resolved_remote_cat_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_cat="/remote/bin/cat"
	remote_log="$TEST_TMPDIR/read_remote_backup_file.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(read_remote_backup_file "backup@example.com pfexec" "/tmp/backup.meta")
	status=$?

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote backup reads should succeed when the ssh probe succeeds." 0 "$status"
	assertEquals "Remote backup reads should forward the remote payload." "payload" "$result"
	{
		IFS= read -r log_line1
		IFS= read -r log_line2
	} <"$remote_log"

	assertEquals "Remote backup reads should keep the host token separate." "backup@example.com" "$log_line1"
	assertContains "Remote backup reads should keep wrapper tokens in the remote command string." "$log_line2" "'pfexec'"
	assertContains "Remote backup reads should use the resolved remote cat path." "$log_line2" "/remote/bin/cat"
	assertContains "Remote backup reads should preserve the requested remote metadata path." "$log_line2" "/tmp/backup.meta"
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
		read_command_line_switches -O "backup@example.com"
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
	assertContains "$result" "lzfs=$FAKE_SSH_BIN 'backup@example.com' /sbin/zfs"
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
	initial_source="zroot"

	FAKE_SSH_SUPPRESS_STDOUT=1 setup_ssh_control_socket "$g_option_O_origin_host" "origin"
	unset FAKE_SSH_SUPPRESS_STDOUT

	l_cmd=$(build_source_snapshot_list_cmd)

	remote_log="$TEST_TMPDIR/remote_snapshot_list.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT

	fake_zstd="$TEST_TMPDIR/zstd"
	create_passthrough_zstd "$fake_zstd"
	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"

	eval "$l_cmd" >"$TEST_TMPDIR/source_snapshot_list.log"
	status=$?

	PATH=$old_path
	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE FAKE_SSH_SUPPRESS_STDOUT
	rm -f "$fake_zstd"

	assertEquals "Remote snapshot listing pipeline should execute without syntax errors." 0 "$status"
	assertEquals "payload" "$(cat "$TEST_TMPDIR/source_snapshot_list.log")"

	{
		IFS= read -r log_line1
		IFS= read -r log_line2
		IFS= read -r log_line3
		IFS= read -r log_line4
	} <"$remote_log"

	assertEquals "ssh should reuse the established control socket." "-S" "$log_line1"
	assertEquals "SSH must pass the control socket path as the next argument." "$g_ssh_origin_control_socket" "$log_line2"
	assertEquals "ssh should connect to the requested origin host." "$g_option_O_origin_host" "$log_line3"
	assertContains "Remote command should force the remote pipeline through sh -c." "$log_line4" "'sh' '-c'"
	assertContains "Remote command should include the dataset listing pipeline." "$log_line4" '/usr/sbin/zfs list -Hr -o name zroot | "/opt/bin/parallel" -j 4 --line-buffer'
	assertContains "Remote command should use GNU parallel's direct dataset placeholder runner." \
		"$log_line4" "/usr/sbin/zfs list -H -o name -s creation -d 1 -t snapshot {}"
	assertContains "Compression pipeline should be preserved in the remote command." "$log_line4" "| zstd -9"
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
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "zroot" ]; then
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
	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	initial_source="zroot"

	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	l_cmd=$(build_source_snapshot_list_cmd)
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
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "tank/home" ]; then
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
	initial_source="tank/home"

	l_cmd=$(build_source_snapshot_list_cmd)
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
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "zroot" ]; then
	printf '%s\n' "zroot"
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
	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_option_O_origin_host_safe=""
	initial_source="zroot"

	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	l_cmd=$(build_source_snapshot_list_cmd)
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
	initial_source="tank/src/app"
	g_initial_source_had_trailing_slash=0

	normalize_destination_snapshot_list "tank/backup/app" "$input_file" "$output_file"

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
	initial_source="tank/dst"
	g_initial_source_had_trailing_slash=1

	normalize_destination_snapshot_list "tank/dst" "$input_file" "$output_file"

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

	result_missing=$(diff_snapshot_lists "$source_file" "$dest_file" "source_minus_destination")
	assertEquals "pool/src@bpp" "$result_missing"

	result_extra=$(diff_snapshot_lists "$source_file" "$dest_file" "destination_minus_source")
	assertEquals "pool/src@dpp" "$result_extra"
}

test_get_last_common_snapshot_matches_snapshot_name_only() {
	# Destination snapshot names use the destination dataset prefix, so the
	# helper must compare on the snapshot component rather than the full path.
	l_source_snaps="tank/doET/tank@zxfer_2
tank/doET/tank@zxfer_1"
	l_dest_snaps="tank/backups/nucbackup/tank/doET/tank@zxfer_1"

	result=$(get_last_common_snapshot "$l_source_snaps" "$l_dest_snaps")

	assertEquals "tank/doET/tank@zxfer_1" "$result"
}

test_get_last_common_snapshot_returns_empty_when_no_snapshot_match() {
	# If the destination never reported the snapshot name, the helper must
	# return an empty string so zxfer performs a full send.
	l_source_snaps="tank/doET/tank@zxfer_2
tank/doET/tank@zxfer_1"
	l_dest_snaps="tank/backups/nucbackup/tank/doET/tank@zxfer_3"

	result=$(get_last_common_snapshot "$l_source_snaps" "$l_dest_snaps")

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

	inspect_delete_snap 0 "tank/zfsbackup/doET/tank"

	assertEquals "tank/zfsbackup/doET/tank@zxfer_30473_20251114214157" "$g_last_common_snap"
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
}

test_get_dest_snapshots_to_delete_per_dataset_returns_extra_dest_entries() {
	g_delete_source_tmp_file=$(mktemp -t zxfer_src.XXXXXX)
	g_delete_dest_tmp_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_delete_snapshots_to_delete_tmp_file=$(mktemp -t zxfer_diff.XXXXXX)
	source_list=$(printf '%s\n%s' "tank/fs@s1" "tank/fs@s2")
	dest_list=$(printf '%s\n%s' "tank/fs@s1" "tank/fs@s3")
	result=$(get_dest_snapshots_to_delete_per_dataset "$source_list" "$dest_list")
	assertEquals "tank/fs@s3" "$result"
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
}

test_set_src_snapshot_transfer_list_collects_newer_snapshots() {
	g_last_common_snap="tank/fs@snap1"
	set_src_snapshot_transfer_list "tank/fs@snap3 tank/fs@snap2 tank/fs@snap1" "tank/fs"
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
		run_destination_zfs_cmd() { printf '%s %s %s\n' "$g_RZFS" "$1" "$2" >"$log"; }
		delete_snaps "$source_list" "$dest_list"
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
		run_destination_zfs_cmd() {
			if [ "$5" = "-p" ]; then
				printf '%s\n' "$old"
			else
				printf '%s\n' "Mon Jan  1 00:00:00 UTC 2024"
			fi
		}
		grandfather_test "tank/fs@snap"
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
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_inspect_delete_snap.sh"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_g_grandfather_protection=1
throw_usage_error() {
	echo "grandfather:$1"
	exit 2
}
run_destination_zfs_cmd() {
	if [ "$5" = "-p" ]; then
		printf '%s\n' "$ZXFER_TEST_VERY_OLD"
	else
		printf '%s\n' "Sun Jan  1 00:00:00 UTC 2023"
	fi
}
grandfather_test "tank/fs@ancient"
EOF
	status=$?
	assertEquals "Grandfather violations should exit with status 2." 2 "$status"
}

test_remove_sources_strips_source_suffix() {
	l_oldifs=$IFS
	IFS=","
	remove_sources "compression=lz4=local,atime=off=override"
	IFS=$l_oldifs
	assertEquals "compression=lz4,atime=off" "$m_new_rmvs_pv"
}

test_remove_properties_drops_requested_entries() {
	l_oldifs=$IFS
	IFS=","
	remove_properties "compression=lz4=local,atime=off=local" "atime"
	IFS=$l_oldifs
	assertEquals "compression=lz4=local" "$m_new_rmv_pvs"
}

test_resolve_human_vars_prefers_human_overrides() {
	resolve_human_vars "compression=lz4=local,atime=on=local" "compression=lz4,atime=none"
	assertEquals "compression=lz4=local,atime=none=local" "$human_results"
}

test_validate_override_properties_rejects_unknown_property() {
	set +e
	ZXFER_TEST_ROOT=$ZXFER_ROOT /bin/sh <<'EOF' >/dev/null 2>&1
. "$ZXFER_TEST_ROOT/src/zxfer_common.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_globals.sh"
. "$ZXFER_TEST_ROOT/src/zxfer_transfer_properties.sh"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
throw_usage_error() {
	echo "invalid"
	exit 2
}
validate_override_properties "copies=2" "compression=lz4=local"
EOF
	status=$?
	assertEquals "Unknown overrides should raise a usage error." 2 "$status"
}

test_validate_override_properties_accepts_known_property() {
	validate_override_properties "compression=lz4" "compression=lz4=local"
	assertEquals 0 "$?"
}

test_derive_override_lists_with_transfer_all_preserves_sources() {
	result=$(derive_override_lists "compression=lz4=local,atime=off=local" "compression=lz4" 1 filesystem)
	{
		IFS= read -r override_line
		IFS= read -r creation_line
	} <<EOF
$result
EOF
	assertEquals "compression=lz4=override,atime=off=local" "$override_line"
	assertEquals "atime=off=local" "$creation_line"
}

test_derive_override_lists_without_transfer_all_uses_overrides_only() {
	result=$(derive_override_lists "compression=lz4=local" "atime=off" 0 filesystem)
	{
		IFS= read -r override_line
		IFS= read -r creation_line
	} <<EOF
$result
EOF
	assertEquals "atime=off=override" "$override_line"
	assertEquals "" "$creation_line"
}

test_sanitize_property_list_removes_readonly_and_ignored_sets() {
	list="compression=lz4=local,atime=off=local"
	readonly="compression"
	ignore="atime"
	result=$(sanitize_property_list "$list" "$readonly" "$ignore")
	assertEquals "" "$result"
}

test_strip_unsupported_properties_removes_matching_entries() {
	unsupported_properties="checksum"
	result=$(strip_unsupported_properties "compression=lz4=local,checksum=sha256=local" "checksum")
	assertEquals "compression=lz4=local" "$result"
}

test_diff_properties_returns_expected_set_and_inherit_lists() {
	result=$(diff_properties "compression=lz4=local,atime=off=received" "compression=lz4=local,atime=on=local" "")
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
	PROPERTY_LOG="$log" apply_property_changes "tank/dst" 1 "compression=lz4,atime=off" "copies=2" "checksum" property_set_logger property_inherit_logger
	result=$(cat "$log")
	expected="set compression=lz4 tank/dst
set atime=off tank/dst"
	assertEquals "$expected" "$result"
	rm -f "$log"
}

test_apply_property_changes_sets_and_inherits_on_children() {
	log="$TEST_TMPDIR/property_apply_child.log"
	PROPERTY_LOG="$log" apply_property_changes "tank/dst/child" 0 "compression=lz4" "atime=off" "encryption" property_set_logger property_inherit_logger
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
		initial_source="tank/src"
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
		exists_destination() { echo 1; }
		write_destination_snapshot_list_to_files "$full_file" "$norm_file"
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
	set_g_recursive_source_list "$source_tmp" "$dest_tmp"
	expected_list=$(printf '%s\n%s' "tank/src" "tank/src/child")
	assertEquals "Missing datasets should be identified for replication." "$expected_list" "$g_recursive_source_list"
	expected_datasets=$(printf '%s\n%s' "tank/src" "tank/src/child")
	assertEquals "Dataset cache should include every source filesystem." "$expected_datasets" "$g_recursive_source_dataset_list"
	rm -f "$source_tmp" "$dest_tmp"
}

test_calculate_size_estimate_uses_incremental_send_probe() {
	result=$(
		run_source_zfs_cmd() { printf 'size\t2048\n'; }
		calculate_size_estimate "tank/fs@snap2" "tank/fs@snap1"
	)
	assertEquals "2048" "$result"
}

test_calculate_size_estimate_handles_full_send_estimate() {
	result=$(
		run_source_zfs_cmd() { printf 'size\t1024\n'; }
		calculate_size_estimate "tank/fs@snap1" ""
	)
	assertEquals "1024" "$result"
}

test_setup_progress_dialog_substitutes_placeholders() {
	g_option_D_display_progress_bar="pv -s %%size%% -N %%title%%"
	result=$(setup_progress_dialog 4096 "tank/fs@snap")
	assertEquals "pv -s 4096 -N tank/fs@snap" "$result"
}

test_wrap_command_with_ssh_without_compression_quotes_command() {
	result=$(
		get_ssh_cmd_for_host() { echo "/usr/bin/ssh"; }
		quote_host_spec_tokens() { echo "'backup@example.com'"; }
		wrap_command_with_ssh "zfs send tank/src@snap" "backup@example.com" 0 send
	)
	assertEquals "/usr/bin/ssh 'backup@example.com' \"zfs send tank/src@snap\"" "$result"
}

test_wrap_command_with_ssh_streams_compression_on_send() {
	result=$(
		g_cmd_compress_safe="gzip"
		g_cmd_decompress_safe="gunzip"
		get_ssh_cmd_for_host() { echo "/usr/bin/ssh"; }
		quote_host_spec_tokens() { echo "'backup'"; }
		wrap_command_with_ssh "zfs send tank/src@snap" "backup" 1 send
	)
	assertEquals "/usr/bin/ssh 'backup' \"zfs send tank/src@snap | gzip\" | gunzip" "$result"
}

test_get_send_command_generates_incremental_streams_with_flags() {
	g_cmd_zfs="/sbin/zfs"
	g_option_V_very_verbose=1
	g_option_w_raw_send=1
	result=$(get_send_command "tank/fs@snap1" "tank/fs@snap2")
	assertEquals "/sbin/zfs send -v -w -I tank/fs@snap1 tank/fs@snap2" "$result"
}

test_get_send_command_emits_full_stream_when_no_common_snapshot() {
	g_cmd_zfs="/sbin/zfs"
	g_option_V_very_verbose=0
	g_option_w_raw_send=0
	result=$(get_send_command "" "tank/fs@snap1")
	assertEquals "/sbin/zfs send   tank/fs@snap1" "$result"
}

test_get_receive_command_honors_force_flag() {
	g_cmd_zfs="/sbin/zfs"
	g_option_F_force_rollback="-F"
	result=$(get_receive_command "tank/dst")
	assertEquals "/sbin/zfs receive -F tank/dst" "$result"
}

test_wait_for_zfs_send_jobs_clears_pid_list_on_success() {
	sleep 0.05 &
	pid1=$!
	sleep 0.05 &
	pid2=$!
	g_zfs_send_job_pids="$pid1 $pid2"
	g_count_zfs_send_jobs=2
	wait_for_zfs_send_jobs "unit"
	assertEquals "" "$g_zfs_send_job_pids"
	assertEquals 0 "$g_count_zfs_send_jobs"
}

test_wait_for_zfs_send_jobs_reports_failure() {
	(
		throw_error() {
			echo "send failure"
			exit 1
		}
		sleep 1 &
		ok_pid=$!
		sh -c 'exit 3' &
		bad_pid=$!
		g_zfs_send_job_pids="$ok_pid $bad_pid"
		g_count_zfs_send_jobs=2
		wait_for_zfs_send_jobs "failure"
	) >/dev/null 2>&1
	assertEquals "Job failures should surface via throw_error." 1 "$?"
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
	assertContains "$report" "invocation: './zxfer' '-R' 'tank/src' 'backup/dst'"
	assertContains "$report" "last_command: '/sbin/zfs' 'send' 'tank/src@snap1'"
	assertContains "$report" "zxfer: failure report end"
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
	stdout_file="$TEST_TMPDIR/throw_error.stdout"
	stderr_file="$TEST_TMPDIR/throw_error.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		throw_error "boom" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "throw_error should preserve the requested exit status." 3 "$status"
	assertEquals "throw_error should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "boom"
}

test_throw_usage_error_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/throw_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		throw_usage_error "bad option"
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "throw_usage_error should exit with usage status 2." 2 "$status"
	assertEquals "throw_usage_error should not write to stdout." "" "$(cat "$stdout_file")"
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
	PATH="$hostile_path_dir:/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -h >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Help output should succeed even when the secure PATH lacks required tools." 0 "$status"
	assertContains "$(cat "$stdout_file")" "usage:"
	assertEquals "Help prescan should bypass dependency initialization errors." "" "$(cat "$stderr_file")"
	if [ -f "$marker_file" ]; then
		marker_contents=$(cat "$marker_file")
	else
		marker_contents=""
	fi
	assertEquals "Early invocation capture should not execute PATH-injected awk/sed helpers." "" "$marker_contents"
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
			trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	count=$(printf '%s\n' "$output" | grep -c "^zxfer: failure report begin$")
	assertEquals "trap_exit helper path should preserve the failing exit status." 1 "$status"
	assertEquals "trap_exit should emit the failure report only once even when EXIT re-triggers cleanup." 1 "$count"
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
		get_path_owner_uid() { printf '%s\n' "1234"; }
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
		get_path_owner_uid() {
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
		get_path_owner_uid() {
			printf '%s\n' "0"
		}
		get_path_mode_octal() {
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
			trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	assertEquals "Failure-report log warnings must not replace the original exit status." 1 "$status"
	assertContains "trap_exit should still emit the report before warning about the log sink." "$output" "zxfer: failure report begin"
	assertContains "trap_exit should warn when ZXFER_ERROR_LOG is invalid." \
		"$output" "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute"
}

test_execute_command_records_last_command_string() {
	g_option_n_dryrun=1

	execute_command "printf 'hello'"

	assertEquals "execute_command should record the exact command string for failure reports." "printf 'hello'" "$g_zxfer_failure_last_command"
}

test_run_source_zfs_cmd_records_local_command() {
	g_option_O_origin_host=""
	g_cmd_zfs="/bin/echo"
	g_LZFS="$g_cmd_zfs"

	run_source_zfs_cmd list -H tank/src >/dev/null

	assertEquals "Direct local ZFS commands should be shell-quoted in the last-command field." \
		"'/bin/echo' 'list' '-H' 'tank/src'" "$g_zxfer_failure_last_command"
}

test_invoke_ssh_command_for_host_records_remote_command() {
	FAKE_SSH_STDOUT_OVERRIDE="ok"

	invoke_ssh_command_for_host "$FAKE_SSH_BIN -q -p 2222" "backup@example.com pfexec" /sbin/zfs list -H tank/src >/dev/null

	assertEquals "SSH command recording should preserve every token boundary." \
		"'$FAKE_SSH_BIN' '-q' '-p' '2222' 'backup@example.com' 'pfexec' '/sbin/zfs' 'list' '-H' 'tank/src'" \
		"$g_zxfer_failure_last_command"
}

test_read_remote_backup_file_rejects_insecure_remote_owner() {
	set +e
	output=$(
		(
			get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
			invoke_ssh_shell_command_for_host() { return 95; }
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort on insecure remote ownership." 1 "$status"
	assertContains "Insecure remote ownership should use the documented error." \
		"$output" "Refusing to use backup metadata /tmp/backup.meta on backup@example.com because it is not owned by root."
}

test_read_remote_backup_file_rejects_insecure_remote_mode() {
	set +e
	output=$(
		(
			get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
			invoke_ssh_shell_command_for_host() { return 96; }
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
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
			get_ssh_cmd_for_host() { printf '%s\n' "/usr/bin/ssh"; }
			invoke_ssh_shell_command_for_host() { return 97; }
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	assertEquals "Remote backup reads should abort when remote ownership or mode cannot be determined." 1 "$status"
	assertContains "Unknown remote security metadata should use the documented error." \
		"$output" "Cannot determine ownership or permissions for backup metadata /tmp/backup.meta on backup@example.com."
}

test_throw_error_with_usage_writes_message_and_usage_to_stderr() {
	stdout_file="$TEST_TMPDIR/throw_error_with_usage.stdout"
	stderr_file="$TEST_TMPDIR/throw_error_with_usage.stderr"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		throw_error_with_usage "boom with usage" 3
	) >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "throw_error_with_usage should preserve the requested exit status." 3 "$status"
	assertEquals "throw_error_with_usage should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: boom with usage"
	assertContains "$(cat "$stderr_file")" "usage: zxfer"
}

test_get_os_handles_local_and_remote_invocations() {
	local_result=$(get_os "")
	remote_result=$(
		(
			fake_remote() {
				printf '%s\n' "RemoteOS"
			}
			get_os fake_remote
		)
	)

	assertEquals "Local OS detection should match uname output." "$(uname)" "$local_result"
	assertEquals "Remote OS detection should eval the supplied remote command prefix." "RemoteOS" "$remote_result"
}

test_execute_command_continue_on_fail_reports_noncritical_error() {
	g_option_n_dryrun=0

	output=$(execute_command "false" 1)
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
			g_option_O_origin_host=""
			g_cmd_zfs="/sbin/zfs"
			g_LZFS="$wrapper"
			run_source_zfs_cmd list -H tank/src
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
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_RZFS="$wrapper"
			run_destination_zfs_cmd get name tank/dst
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

	strip_trailing_slashes "///" >"$outfile"

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

test_zxfer_find_symlink_path_component_rejects_relative_paths() {
	result=$(zxfer_find_symlink_path_component "relative/path")
	status=$?

	assertEquals "Relative paths should not be scanned for symlink components." 1 "$status"
	assertEquals "Relative path checks should not report a symlink component." "" "$result"
}

. "$SHUNIT2_BIN"
