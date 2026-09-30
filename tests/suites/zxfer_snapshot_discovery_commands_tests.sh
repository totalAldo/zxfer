#!/bin/sh
# Source listing command cases for src/zxfer_snapshot_discovery.sh that need
# the exec fixture's recording ssh: a hostile resolved origin helper path
# stays one remote token, and the -j -z listing survives a csh login shell on
# the origin. Run by tests/test_zxfer_snapshot_discovery.sh under the exec
# fixture.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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

test_build_source_snapshot_list_cmd_remote_helper_path_does_not_execute_locally() {
	marker="$TEST_TMPDIR/remote_helper_marker"
	outfile="$TEST_TMPDIR/remote_helper.out"
	errfile="$TEST_TMPDIR/remote_helper.err"
	remote_log="$TEST_TMPDIR/remote_helper.log"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs="/bin/echo; touch $marker #"
	g_option_O_origin_host="backup@example.com"
	g_option_j_jobs=1
	g_initial_source="tank/src"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	l_cmd=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd)
	zxfer_execute_source_snapshot_list_background_cmd_with_sort "$l_cmd" \
		"$outfile" "$errfile" "$TEST_TMPDIR/remote_helper.sorted"
	wait "$g_last_background_pid"
	zxfer_unregister_cleanup_pid "$g_last_background_pid"

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
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot" ]; then
	printf '%s\t%s\n' "zroot@snap1" "guid-1"
	exit 0
fi
exit 0
EOF
	chmod +x "$fake_remote_zfs"

	g_option_j_jobs=4
	g_option_z_compress=1
	g_cmd_compress="zstd -9"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="$fake_remote_zfs"
	g_origin_cmd_zfs="$fake_remote_zfs"
	g_cmd_decompress_safe="'$fake_zstd' '-d'"
	g_origin_cmd_compress_safe="'$fake_zstd' '-9'"
	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_initial_source="zroot"

	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	l_cmd=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="$FAKE_PARALLEL_BIN"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
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
