#!/bin/sh
# Listing-command tests for src/zxfer_snapshot_producers.sh: serial, parallel
# and remote source listings, the remote parallel lookup, listing pipelines
# (csh remote shells included), and destination-list normalization. Run by
# tests/test_zxfer_snapshot_producers.sh under the exec fixture.
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
	l_cmd=$(printf '%s\n' "$l_template" | sed "s|{}|'$l_item'|g")
	sh -c "$l_cmd" || exit $?
done
EOF
	chmod +x "$l_path"
}

test_build_source_snapshot_list_cmd_serial_returns_direct_list() {
	g_cmd_zfs="/sbin/zfs"
	g_initial_source="tank/data"
	g_option_j_jobs=1

	result=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd)

	assertEquals "Serial snapshot listing should render a shell-safe direct zfs command." \
		"'/sbin/zfs' 'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' 'tank/data'" "$result"
}

test_build_source_snapshot_list_cmd_parallel_local_includes_parallel_runner() {
	g_cmd_zfs="/sbin/zfs"
	g_initial_source="tank/home"
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_option_O_origin_host=""
	g_option_z_compress=0

	result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
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
	g_cmd_zfs="/usr/sbin/zfs"
	g_origin_cmd_zfs="/opt/openzfs/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"
	g_cmd_compress="zstd -T0 -9"
	g_initial_source="tank/src"
	g_option_j_jobs=8
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_option_O_origin_host="backup@example.com pfexec -p 2222"
	g_option_z_compress=1
	g_cmd_ssh="/usr/bin/ssh"

	result=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="/opt/bin/parallel"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
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
	g_option_j_jobs=1
	g_initial_source="tank/src"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	l_cmd=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd)
	zxfer_execute_rendered_background_shell_command "$l_cmd" "$outfile" "$errfile"
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

test_ensure_parallel_remote_fetches_remote_parallel_path() {
	result_file="$TEST_TMPDIR/ensure_parallel_remote_fetch.out"
	g_option_j_jobs=4
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_option_O_origin_host="aldo@172.16.0.4"
	remote_log="$TEST_TMPDIR/remote_parallel_probe.log"
	socket_path="$TEST_TMPDIR/origin.sock"
	: >"$remote_log"
	: >"$socket_path"

	(
		g_origin_parallel_cmd=""
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_ssh_origin_control_socket="$socket_path"

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
	assertContains "Remote capability discovery should pin the secure PATH inside the shell probe." "$log_line_remote_cmd" "$g_zxfer_secure_path"
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
				printf 'throw:%s\n' "$1"
				exit 1
			}
			zxfer_ensure_parallel_available_for_source_jobs 2>/dev/null
			l_status=$?
			printf 'reason=%s\n' "$g_zxfer_parallel_source_job_check_result"
			exit "$l_status"
		)
	)
	status=$?

	unset FAKE_SSH_SUPPRESS_STDOUT FAKE_SSH_EXIT_STATUS

	assertEquals "Remote parallel probe failures should abort the helper." 1 "$status"
	assertEquals "Remote parallel probe failures should publish the query failure message without throwing." \
		"reason=Failed to query dependency \"parallel\" on host aldo@172.16.0.4 pfexec." "$output"
}

test_ensure_parallel_available_for_source_jobs_reports_missing_remote_parallel() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="Required dependency \"parallel\" not found on host origin.example in secure PATH (/opt/openzfs/bin:/usr/sbin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_throw_error() {
				printf 'throw:%s\n' "$1"
				exit 1
			}
			g_option_j_jobs=4
			g_cmd_parallel="$FAKE_PARALLEL_BIN"
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""
			zxfer_ensure_parallel_available_for_source_jobs
			l_status=$?
			printf 'reason=%s\n' "$g_zxfer_parallel_source_job_check_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Missing remote parallel should abort the helper." 1 "$status"
	assertEquals "Missing remote parallel should be translated into the user-facing guidance without throwing." \
		"reason=parallel not found on origin host origin.example but -j 4 was requested. Install parallel remotely or rerun without -j." "$output"
}

test_build_source_snapshot_list_cmd_surfaces_throws_from_the_remote_parallel_lookup() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				zxfer_throw_error "remote lookup failed" 1
			}
			zxfer_throw_error() {
				printf 'throw:%s\n' "$1" >&2
				exit "${2:-1}"
			}
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""
			zxfer_build_source_snapshot_list_cmd
		) 2>&1
	)
	status=$?

	assertEquals "A throw inside the -O -j parallel lookup should end the run with its status." \
		1 "$status"
	assertContains "A throw inside the -O -j parallel lookup should reach stderr." \
		"$output" "throw:remote lookup failed"
}

test_remote_snapshot_listing_pipeline_handles_cli_flow() {
	g_option_j_jobs=4
	g_option_z_compress=1
	g_cmd_compress="zstd -9"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_cmd_zfs="/usr/sbin/zfs"
	g_origin_cmd_zfs="$g_cmd_zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_initial_source="zroot"

	g_ssh_supports_control_sockets=1
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_SUPPRESS_STDOUT
	zxfer_open_ssh_control_sockets
	unset FAKE_SSH_SUPPRESS_STDOUT

	fake_zstd="$TEST_TMPDIR/zstd"
	create_passthrough_zstd "$fake_zstd"
	g_cmd_decompress_safe="'$fake_zstd' '-d'"
	g_origin_cmd_compress_safe="'$fake_zstd' '-9'"

	l_cmd=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="/opt/bin/parallel"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	remote_log="$TEST_TMPDIR/remote_snapshot_list.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	# The canned remote output must end with the discovery success sentinel:
	# the local pipeline strips it and fails the listing when it is missing.
	FAKE_SSH_STDOUT_OVERRIDE="payload
$ZXFER_SOURCE_DISCOVERY_SENTINEL"
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
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot" ]; then
	printf '%s\t%s\n' "zroot@snap1" "guid-1"
	printf '%s\t%s\n' "zroot@snap2" "guid-2"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot/usr" ]; then
	printf '%s\t%s\n' "zroot/usr@snap1" "guid-3"
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
	g_cmd_compress="zstd -9"
	g_cmd_parallel="$fake_parallel"
	g_origin_parallel_cmd="$fake_parallel"
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
				g_zxfer_required_tool_result="$fake_parallel"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	eval "$l_cmd" >"$TEST_TMPDIR/remote_snapshot_exec.out" 2>"$TEST_TMPDIR/remote_snapshot_exec.err"
	status=$?

	unset FAKE_SSH_LOG
	PATH=$old_path

	assertEquals "Remote snapshot listing should execute the GNU parallel runner without malformed zfs argv." 0 "$status"
	assertEquals "The executed remote pipeline should return all source snapshots." \
		"zroot@snap1	guid-1
zroot@snap2	guid-2
zroot/usr@snap1	guid-3" "$(cat "$TEST_TMPDIR/remote_snapshot_exec.out")"
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
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "tank/home" ]; then
	printf '%s\t%s\n' "tank/home@snap1" "guid-1"
	printf '%s\t%s\n' "tank/home@snap2" "guid-2"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "tank/home/usr" ]; then
	printf '%s\t%s\n' "tank/home/usr@snap1" "guid-3"
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
	g_initial_source="tank/home"

	l_cmd=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	eval "$l_cmd" >"$TEST_TMPDIR/local_snapshot_exec.out" 2>"$TEST_TMPDIR/local_snapshot_exec.err"
	status=$?

	assertEquals "Local snapshot listing should execute the GNU parallel runner without malformed zfs argv." 0 "$status"
	assertEquals "The executed local pipeline should return all source snapshots." \
		"tank/home@snap1	guid-1
tank/home@snap2	guid-2
tank/home/usr@snap1	guid-3" "$(cat "$TEST_TMPDIR/local_snapshot_exec.out")"
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

test_normalize_destination_snapshot_list_keeps_already_aligned_trailing_slash_paths() {
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
	assertEquals "Trailing-slash normalization should leave already source-aligned destination paths unchanged apart from sorting." "$expected" "$result"
}

test_write_destination_snapshot_list_to_files_normalizes_destination_path() {
	full_file="$TEST_TMPDIR/dest_snapshots.txt"
	norm_file="$TEST_TMPDIR/dest_snapshots_normalized.txt"
	# shellcheck disable=SC2030,SC2031
	(
		g_initial_source="tank/src"
		g_destination="backup/dst"
		g_initial_source_had_trailing_slash=0
		g_cmd_zfs="$TEST_TMPDIR/fake_rzfs"
		cat >"$g_cmd_zfs" <<'EOF'
#!/bin/sh
cat <<'DATA'
backup/dst/src@snapA
backup/dst/src@snapB
DATA
EOF
		chmod +x "$g_cmd_zfs"
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	)
	result=$(cat "$norm_file")
	expected="tank/src@snapA
tank/src@snapB"
	assertEquals "Destination snapshots should be rewritten to match the source prefix." "$expected" "$result"
}
