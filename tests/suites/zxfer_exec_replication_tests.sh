#!/bin/sh
# Backup, discovery, property, and send/receive behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
		"'/sbin/zfs' 'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' 'tank/data'" "$result"
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
	g_cmd_compress="zstd -T0 -9"
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

	result=$(
		(
			# This case verifies helper-token quoting before transport; keep the
			# csh-safe transport chunker out of its string assertion.
			zxfer_build_remote_sh_c_command() {
				printf '%s' "$1"
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
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
	g_cmd_compress="zstd -9"
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
			zxfer_build_source_snapshot_list_cmd
		)
	)

	remote_log="$TEST_TMPDIR/remote_snapshot_list.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	# The canned remote output must end with the discovery success sentinel:
	# the local pipeline strips it and fails the listing when it is missing.
	FAKE_SSH_STDOUT_OVERRIDE="payload
$(zxfer_get_source_discovery_sentinel_line)"
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
			zxfer_build_source_snapshot_list_cmd
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
	g_zxfer_snapshot_delete_source_identities_file=$(mktemp -t zxfer_src.XXXXXX)
	g_zxfer_snapshot_delete_destination_identities_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_zxfer_snapshot_delete_difference_file=$(mktemp -t zxfer_diff.XXXXXX)
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
	rm -f "$g_zxfer_snapshot_delete_source_identities_file" "$g_zxfer_snapshot_delete_destination_identities_file" "$g_zxfer_snapshot_delete_difference_file"
}

test_get_dest_snapshots_to_delete_per_dataset_returns_extra_dest_entries() {
	g_zxfer_snapshot_delete_source_identities_file=$(mktemp -t zxfer_src.XXXXXX)
	g_zxfer_snapshot_delete_destination_identities_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_zxfer_snapshot_delete_difference_file=$(mktemp -t zxfer_diff.XXXXXX)
	source_list=$(printf '%s\n%s' "tank/fs@s1" "tank/fs@s2")
	dest_list=$(printf '%s\n%s' "tank/fs@s1" "tank/fs@s3")
	result=$(zxfer_get_dest_snapshots_to_delete_per_dataset "$source_list" "$dest_list")
	assertEquals "tank/fs@s3" "$result"
	rm -f "$g_zxfer_snapshot_delete_source_identities_file" "$g_zxfer_snapshot_delete_destination_identities_file" "$g_zxfer_snapshot_delete_difference_file"
}

test_get_dest_snapshots_to_delete_per_dataset_treats_guid_mismatches_as_extra() {
	g_zxfer_snapshot_delete_source_identities_file=$(mktemp -t zxfer_src.XXXXXX)
	g_zxfer_snapshot_delete_destination_identities_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_zxfer_snapshot_delete_difference_file=$(mktemp -t zxfer_diff.XXXXXX)
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
	rm -f "$g_zxfer_snapshot_delete_source_identities_file" "$g_zxfer_snapshot_delete_destination_identities_file" "$g_zxfer_snapshot_delete_difference_file"
}

test_set_src_snapshot_transfer_list_collects_newer_snapshots() {
	g_last_common_snap="tank/fs@snap1"
	zxfer_set_src_snapshot_transfer_list "tank/fs@snap3 tank/fs@snap2 tank/fs@snap1" "tank/fs"
	expected=$(printf '%s\n%s' "tank/fs@snap2" "tank/fs@snap3")
	assertEquals "$expected" "$g_src_snapshot_transfer_list"
}

test_delete_snaps_invokes_destroy_for_missing_snapshots() {
	log="$TEST_TMPDIR/delete_snap_cmd.log"
	g_zxfer_snapshot_delete_source_identities_file=$(mktemp -t zxfer_src.XXXXXX)
	g_zxfer_snapshot_delete_destination_identities_file=$(mktemp -t zxfer_dst.XXXXXX)
	g_zxfer_snapshot_delete_difference_file=$(mktemp -t zxfer_diff.XXXXXX)
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
	rm -f "$log" "$g_zxfer_snapshot_delete_source_identities_file" "$g_zxfer_snapshot_delete_destination_identities_file" "$g_zxfer_snapshot_delete_difference_file"
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
	zxfer_reset_background_job_state
	zxfer_reset_send_receive_state
	zxfer_spawn_supervised_background_job "send_receive" "sleep 1" "sleep 1"
	zxfer_register_supervised_send_job \
		"$g_zxfer_background_job_last_id" \
		"$g_zxfer_background_job_last_runner_pid"
	zxfer_spawn_supervised_background_job "send_receive" "sleep 1" "sleep 1"
	zxfer_register_supervised_send_job \
		"$g_zxfer_background_job_last_id" \
		"$g_zxfer_background_job_last_runner_pid"
	zxfer_wait_for_zfs_send_jobs "unit"
	assertEquals "" "$g_zfs_send_job_pids"
	assertEquals 0 "$g_count_zfs_send_jobs"
}

test_wait_for_zfs_send_jobs_reports_failure() {
	(
		zxfer_reset_background_job_state
		zxfer_reset_send_receive_state
		zxfer_throw_error() {
			echo "send failure"
			exit 1
		}
		zxfer_spawn_supervised_background_job "send_receive" "exit 0" "exit 0"
		zxfer_register_supervised_send_job \
			"$g_zxfer_background_job_last_id" \
			"$g_zxfer_background_job_last_runner_pid"
		zxfer_spawn_supervised_background_job "send_receive" "exit 3" "exit 3"
		zxfer_register_supervised_send_job \
			"$g_zxfer_background_job_last_id" \
			"$g_zxfer_background_job_last_runner_pid"
		zxfer_wait_for_zfs_send_jobs "failure"
	) >/dev/null 2>&1
	assertEquals "Job failures should surface via zxfer_throw_error." 1 "$?"
}
