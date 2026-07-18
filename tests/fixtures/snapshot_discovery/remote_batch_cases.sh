#!/bin/sh
# shellcheck shell=sh
# Remote destination batching, staged status, and orchestration failure cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_get_zfs_list_remote_target_batches_destination_discovery() {
	ssh_log="$TEST_TMPDIR/get_zfs_remote_batch_success.ssh"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG="$ssh_log"
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'host=%s side=%s\n' "$1" "$3" >>"$SSH_LOG"
				printf 'cmd=%s\n' "$2" >>"$SSH_LOG"
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf '%s\n' "backup/dst/src"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf '%s\t%s\n' "backup/dst/src@snapA" "guid-a"
				printf '%s\t%s\n' "backup/dst/src/child@snapB" "guid-b"
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unexpected-destination-zfs" >>"$SSH_LOG"
				return 99
			}
			zxfer_set_g_recursive_source_list() {
				printf 'normalized=%s\n' "$(cat "$2")"
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'root_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'snapshot_dataset_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/src")"
			printf 'raw=%s\n' "$g_rzfs_list_hr_snap"
		)
	)

	assertEquals "Remote destination discovery should use one target SSH invocation." \
		"1" "$(grep -c '^host=target.example side=destination$' "$ssh_log")"
	assertContains "Remote destination discovery should render dataset inventory in the batch script." \
		"$(cat "$ssh_log")" "filesystem,volume"
	assertContains "Remote destination discovery should render snapshot listing in the batch script." \
		"$(cat "$ssh_log")" "list -Hr -o name,guid -t snapshot"
	assertNotContains "Remote destination discovery should not fall back to separate destination zfs helper calls." \
		"$(cat "$ssh_log")" "unexpected-destination-zfs"
	assertContains "Remote destination discovery should publish the recursive destination inventory." \
		"$output" "dest=backup/dst
backup/dst/src"
	assertContains "Remote destination discovery should seed the destination root existence cache." \
		"$output" "root_cache=1"
	assertContains "Remote destination discovery should seed the destination snapshot dataset existence cache." \
		"$output" "snapshot_dataset_cache=1"
	assertContains "Remote destination discovery should preserve the raw destination snapshot cache." \
		"$output" "raw=backup/dst/src@snapA	guid-a
backup/dst/src/child@snapB	guid-b"
	assertContains "Remote destination discovery should normalize and byte-sort destination snapshot paths for source-side diffing." \
		"$output" "normalized=tank/src/child@snapB	guid-b
tank/src@snapA	guid-a"
}

test_build_remote_destination_discovery_batch_script_matches_golden_output() {
	actual_script="$TEST_TMPDIR/remote_destination_discovery_batch_script.actual"
	expected_script="$TESTS_DIR/golden/remote_destination_discovery_batch_script.golden"

	(
		unset ZXFER_SECURE_PATH ZXFER_SECURE_PATH_APPEND
		g_target_cmd_zfs=/opt/zfs/bin/zfs
		g_zxfer_dependency_path=/secure/sbin:/secure/bin
		zxfer_build_remote_destination_discovery_batch_script \
			backup/dst backup/dst/src backup >"$actual_script"
	)

	golden_status=0
	cmp -s "$expected_script" "$actual_script" || golden_status=$?
	if [ "$golden_status" -ne 0 ]; then
		diff -u "$expected_script" "$actual_script" >&2 || :
	fi
	assertEquals "Remote destination batch rendering should remain byte-for-byte stable." \
		0 "$golden_status"
}

test_build_remote_destination_discovery_batch_script_streams_snapshot_stdout_directly() {
	fake_zfs="$TEST_TMPDIR/remote_batch_stream_zfs"
	zfs_log="$TEST_TMPDIR/remote_batch_stream_zfs.log"
	: >"$zfs_log"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$ZXFER_FAKE_ZFS_LOG"
case "$*" in
"list -t filesystem,volume -Hr -o name backup/dst")
	printf '%s\n' "backup/dst"
	printf '%s\n' "backup/dst/src"
	printf '%s\n' "backup/dst/other"
	;;
"list -Hr -o name,guid -t snapshot backup/dst/src")
	printf '%s\t%s\n' "backup/dst/src@snapA" "guid-a"
	;;
*)
	printf 'unexpected zfs args: %s\n' "$*" >&2
	exit 99
	;;
esac
EOF
	chmod +x "$fake_zfs"
	g_target_cmd_zfs=$fake_zfs

	script=$(zxfer_build_remote_destination_discovery_batch_script "backup/dst" "backup/dst/src" "backup")

	assertNotContains "Remote batch should not buffer recursive destination inventory stdout in a shell variable." \
		"$script" "l_inventory_stdout=\$("
	assertNotContains "Remote batch should not buffer destination snapshot stdout in a shell variable." \
		"$script" "l_snapshot_stdout=\$("
	assertContains "Remote batch should stage destination inventory stdout in a target-side temp file." \
		"$script" 'zxfer.destination-discovery.inventory.XXXXXX'
	assertNotContains "Remote batch should not stage destination snapshot stdout in a target-side temp file." \
		"$script" 'zxfer.destination-discovery.snapshots.XXXXXX'
	assertContains "Remote batch should still stage compact destination snapshot stderr diagnostics." \
		"$script" 'zxfer.destination-discovery.snapshots-stderr.XXXXXX'
	assertContains "Remote batch should stream staged section bodies instead of expanding payload variables." \
		"$script" "cat \"\$l_section_file\""
	assertContains "Remote batch should stream snapshot stdout directly from zfs." \
		"$script" "\"\$l_zfs_cmd\" list -Hr -o name,guid -t snapshot \"\$l_destination_snapshot_dataset\" 2>\"\$l_snapshot_stderr_file\""
	assertContains "Remote batch should clean target-side temp files on shell exit." \
		"$script" "trap 'zxfer_cleanup_destination_discovery_batch' 0"
	assertContains "Remote batch should use an exact fixed-string scan for the destination snapshot dataset." \
		"$script" "grep -F -x -e \"\$l_destination_snapshot_dataset\" \"\$l_inventory_stdout_file\""

	set +e
	output=$(ZXFER_FAKE_ZFS_LOG="$zfs_log" TMPDIR="$TEST_TMPDIR" sh -c "$script" 2>&1)
	status=$?
	set -e

	assertEquals "Generated remote batch script should execute successfully with target-side temp files." \
		0 "$status"
	assertContains "Generated remote batch should emit the destination inventory section." \
		"$output" "$(printf 'BEGIN\tinventory_stdout')"
	assertContains "Generated remote batch should stream destination inventory rows." \
		"$output" "backup/dst/src"
	assertContains "Generated remote batch should stream destination snapshot rows." \
		"$output" "backup/dst/src@snapA	guid-a"
	assertContains "Generated remote batch should report that snapshot listing ran." \
		"$output" "$(printf 'STATUS\tsnapshot_ran\t1')"
	assertContains "Generated remote batch should report snapshot status after streaming stdout." \
		"$output" "$(printf 'STATUS\tsnapshot\t0')"
	assertEquals "Generated remote batch should run inventory and snapshot zfs lists without a pool fallback." \
		"2" "$(wc -l <"$zfs_log" | tr -d '[:space:]')"
	assertEquals "Generated remote batch should remove its target-side temp files." \
		"" "$(find "$TEST_TMPDIR" -name 'zxfer.destination-discovery.*' -print)"
}

test_destination_discovery_batch_status_loader_rejects_malformed_sidecars() {
	status_file="$TEST_TMPDIR/remote_batch_status_bad.out"

	printf '%s\n' "not-tab-separated" >"$status_file"
	set +e
	zxfer_load_destination_discovery_batch_status_file "$status_file" >/dev/null 2>&1
	status=$?
	set -e
	assertEquals "Status sidecars without tab-separated fields should fail closed." \
		1 "$status"

	{
		printf 'inventory\t0\n'
		printf 'pool\t\n'
		printf 'snapshot\t0\n'
		printf 'unexpected\t0\n'
	} >"$status_file"
	set +e
	zxfer_load_destination_discovery_batch_status_file "$status_file" >/dev/null 2>&1
	status=$?
	set -e
	assertEquals "Status sidecars with unknown status names should fail closed." \
		1 "$status"
}

test_destination_discovery_batch_status_loader_accepts_complete_sidecars() {
	status_file="$TEST_TMPDIR/remote_batch_status_good.out"
	{
		printf 'inventory\t0\n'
		printf 'pool\t\n'
		printf 'snapshot\t0\n'
		printf 'snapshot_ran\t1\n'
	} >"$status_file"

	zxfer_load_destination_discovery_batch_status_file "$status_file"
	invalid_status=0
	zxfer_destination_discovery_batch_status_is_numeric "not-a-status" ||
		invalid_status=$?

	assertEquals "Complete batch status sidecars should publish the inventory status." \
		0 "$g_zxfer_destination_discovery_batch_inventory_status"
	assertEquals "Complete batch status sidecars should publish the snapshot status." \
		0 "$g_zxfer_destination_discovery_batch_snapshot_status"
	assertEquals "Complete batch status sidecars should publish the snapshot_ran marker." \
		1 "$g_zxfer_destination_discovery_batch_snapshot_ran"
	assertEquals "Non-numeric batch statuses should be rejected." \
		1 "$invalid_status"
}

test_read_snapshot_discovery_status_file_defaults_empty_sidecars() {
	status_file="$TEST_TMPDIR/snapshot_discovery_empty_status.out"
	: >"$status_file"

	zxfer_read_snapshot_discovery_status_file "$status_file" 37
	status=$?

	assertEquals "Empty snapshot discovery status files should be accepted as the supplied default." \
		0 "$status"
	assertEquals "Empty snapshot discovery status files should publish the supplied default." \
		37 "$g_zxfer_snapshot_discovery_status_file_result"
}

test_get_zfs_list_remote_target_batches_missing_destination_root_fallback() {
	ssh_log="$TEST_TMPDIR/get_zfs_remote_batch_missing.ssh"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG="$ssh_log"
			g_option_T_target_host="target.example"
			g_option_V_very_verbose=1
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'host=%s side=%s\n' "$1" "$3" >>"$SSH_LOG"
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t1\n'
				printf 'STATUS\tpool\t0\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t0\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf '%s\n' "cannot open 'backup/dst': no such pool or dataset"
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unexpected-pool-probe" >>"$SSH_LOG"
				return 99
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_get_zfs_list
			printf 'dest=<%s>\n' "$g_recursive_dest_list"
			printf 'root_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'child_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/src")"
			printf 'raw=<%s>\n' "$g_rzfs_list_hr_snap"
		)
	)

	assertEquals "Missing-root remote discovery should still use one target SSH invocation." \
		"1" "$(grep -c '^host=target.example side=destination$' "$ssh_log")"
	assertNotContains "Remote missing-root fallback should use the batch pool status instead of a second local destination helper probe." \
		"$(cat "$ssh_log")" "unexpected-pool-probe"
	assertContains "Remote missing-root fallback should treat the recursive destination inventory as empty." \
		"$output" "dest=<>"
	assertContains "Remote missing-root fallback should mark the destination root missing." \
		"$output" "root_cache=0"
	assertContains "Remote missing-root fallback should infer descendants under the missing root as absent." \
		"$output" "child_cache=0"
	assertContains "Remote missing-root fallback should stage an empty destination snapshot list." \
		"$output" "raw=<>"
}

test_get_zfs_list_remote_target_batches_inventory_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t13\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t0\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf '%s\n' "permission denied"
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote destination inventory failures should preserve the target-side status." \
		13 "$status"
	assertContains "Remote destination inventory failures should include the target-side diagnostic." \
		"$output" "Failed to retrieve list of datasets from the destination: permission denied"
}

test_get_zfs_list_remote_target_batches_snapshot_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t17\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf '%s\n' "backup/dst/src"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf '%s\n' "snapshot list failed"
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote destination snapshot failures should preserve the target-side status." \
		17 "$status"
	assertContains "Remote destination snapshot failures should preserve the existing snapshot-list failure message." \
		"$output" "Failed to retrieve snapshot list from the destination."
	assertContains "Remote destination snapshot failures should preserve target-side stderr diagnostics." \
		"$output" "snapshot list failed"
}

test_get_zfs_list_remote_target_batches_malformed_payloads_fail_closed() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Malformed remote destination discovery batches should fail closed." \
		1 "$status"
	assertContains "Malformed remote destination discovery batches should report the malformed batch context." \
		"$output" "Malformed destination discovery batch response."

	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf '%s\n' "backup/dst/src"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf '%s\n' "backup/dst/src@snapA	101"
				printf 'END\tsnapshot_stdout\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote destination discovery batches with missing sections should fail closed." \
		1 "$status"
	assertContains "Missing remote batch sections should report the malformed batch context." \
		"$output" "Malformed destination discovery batch response."
}

test_run_remote_destination_discovery_batch_preserves_setup_and_transport_failures() {
	output=$(
		set +e
		dest_file="$TEST_TMPDIR/remote_batch_failure.dest"
		err_file="$TEST_TMPDIR/remote_batch_failure.err"
		snap_file="$TEST_TMPDIR/remote_batch_failure.snap"
		snap_err_file="$TEST_TMPDIR/remote_batch_failure.snap.err"
		: >"$dest_file"
		: >"$err_file"
		: >"$snap_file"
		: >"$snap_err_file"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_get_temp_file() {
				return 31
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'temp=%s\n' "$?"

		(
			g_destination="backup/dst"
			zxfer_build_remote_destination_discovery_batch_script() {
				return 32
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'build=%s\n' "$?"

		(
			g_destination="backup/dst"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				return 33
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'command=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "transport-policy-failed"
				return 35
			}
			zxfer_throw_error() {
				printf 'transport_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'transport=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				g_zxfer_ssh_shell_context_error_result="wrapper setup failed"
				return 38
			}
			zxfer_throw_error() {
				printf 'context_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'context=%s\n' "$?"

		(
			pool_log="$TEST_TMPDIR/remote_batch_rootless_pool.log"
			g_destination="backup"
			g_option_T_target_host="target.example"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "$3" >"$pool_log"
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 39
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
			l_rootless_status=$?
			printf 'rootless_pool=%s\n' "$(cat "$pool_log")"
			exit "$l_rootless_status"
		)
		printf 'context_nomsg=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_get_temp_file() {
				if [ "${remote_batch_second_temp_count:-0}" = 1 ]; then
					return 36
				fi
				remote_batch_second_temp_count=1
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_second_temp_$remote_batch_second_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'second_temp=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_get_temp_file() {
				if [ "${remote_batch_third_temp_count:-0}" = 0 ]; then
					remote_batch_third_temp_count=1
				elif [ "$remote_batch_third_temp_count" = 1 ]; then
					remote_batch_third_temp_count=2
				else
					return 37
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_third_temp_$remote_batch_third_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'third_temp=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_ssh_temp_count:-0}" = 0 ]; then
					remote_batch_ssh_temp_count=1
				elif [ "$remote_batch_ssh_temp_count" = 1 ]; then
					remote_batch_ssh_temp_count=2
				else
					remote_batch_ssh_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_ssh_$remote_batch_ssh_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 34
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'ssh_cleanup=%s\n' "$1"
				rm -f "$1"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'ssh=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_status_read_temp_count:-0}" = 0 ]; then
					remote_batch_status_read_temp_count=1
				elif [ "$remote_batch_status_read_temp_count" = 1 ]; then
					remote_batch_status_read_temp_count=2
				else
					remote_batch_status_read_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_status_read_$remote_batch_status_read_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "ZXFER_DESTINATION_DISCOVERY_BATCH_V1"
			}
			zxfer_read_snapshot_discovery_capture_file() {
				return 41
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'status_read=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_malformed_status_temp_count:-0}" = 0 ]; then
					remote_batch_malformed_status_temp_count=1
				elif [ "$remote_batch_malformed_status_temp_count" = 1 ]; then
					remote_batch_malformed_status_temp_count=2
				else
					remote_batch_malformed_status_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_malformed_status_$remote_batch_malformed_status_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "ZXFER_DESTINATION_DISCOVERY_BATCH_V1"
			}
			zxfer_read_snapshot_discovery_capture_file() {
				g_zxfer_snapshot_discovery_file_read_result="not-a-number"
				return 0
			}
			zxfer_throw_error() {
				printf 'malformed_status_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'malformed_status=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_stderr_read_temp_count:-0}" = 0 ]; then
					remote_batch_stderr_read_temp_count=1
				elif [ "$remote_batch_stderr_read_temp_count" = 1 ]; then
					remote_batch_stderr_read_temp_count=2
				else
					remote_batch_stderr_read_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_stderr_read_$remote_batch_stderr_read_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 34
			}
			zxfer_read_snapshot_discovery_capture_file() {
				if [ "$1" = "$TEST_TMPDIR/remote_batch_stderr_read_2.out" ]; then
					return 42
				fi
				IFS= read -r g_zxfer_snapshot_discovery_file_read_result <"$1" || return "$?"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'stderr_read=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_err_write_temp_count:-0}" = 0 ]; then
					remote_batch_err_write_temp_count=1
				elif [ "$remote_batch_err_write_temp_count" = 1 ]; then
					remote_batch_err_write_temp_count=2
				else
					remote_batch_err_write_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_err_write_$remote_batch_err_write_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "transport stderr" >&2
				return 34
			}
			zxfer_write_runtime_artifact_file() {
				if [ "$1" = "$err_file" ] && [ $# -gt 1 ] && [ "$2" != "" ]; then
					return 43
				fi
				: >"$1" || return "$?"
				if [ $# -gt 1 ]; then
					printf '%s' "$2" >"$1"
				fi
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'err_write=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_parse_temp_count:-0}" = 0 ]; then
					remote_batch_parse_temp_count=1
				elif [ "$remote_batch_parse_temp_count" = 1 ]; then
					remote_batch_parse_temp_count=2
				else
					remote_batch_parse_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_parse_$remote_batch_parse_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "ZXFER_DESTINATION_DISCOVERY_BATCH_V1"
			}
			zxfer_throw_error() {
				printf 'parse_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'parse_cleanup=%s\n' "$1"
				rm -f "$1"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'parse=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				if [ "${remote_batch_status_load_temp_count:-0}" = 0 ]; then
					remote_batch_status_load_temp_count=1
				elif [ "$remote_batch_status_load_temp_count" = 1 ]; then
					remote_batch_status_load_temp_count=2
				else
					remote_batch_status_load_temp_count=3
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_status_load_$remote_batch_status_load_temp_count.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_load_destination_discovery_batch_status_file() {
				return 44
			}
			zxfer_throw_error() {
				printf 'load_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'load=%s\n' "$?"
	)

	assertContains "Remote batch temp allocation failures should preserve status." \
		"$output" "temp=31"
	assertContains "Remote batch script render failures should preserve status." \
		"$output" "build=32"
	assertContains "Remote batch command render failures should preserve status." \
		"$output" "command=33"
	assertContains "Remote batch transport token failures should preserve status." \
		"$output" "transport=35"
	assertContains "Remote batch transport token failures should preserve diagnostics." \
		"$output" "transport_error=transport-policy-failed"
	assertContains "Remote batch wrapper setup failures should preserve diagnostics." \
		"$output" "context_error=wrapper setup failed"
	assertContains "Remote batch wrapper setup failures without diagnostics should preserve status." \
		"$output" "context_nomsg=39"
	assertContains "Remote batch rootless destination roots should be passed through as the pool probe name." \
		"$output" "rootless_pool=backup"
	assertContains "Remote batch second temp allocation failures should preserve status." \
		"$output" "second_temp=36"
	assertContains "Remote batch third temp allocation failures should preserve status." \
		"$output" "third_temp=37"
	assertContains "Remote batch SSH failures should preserve transport status." \
		"$output" "ssh=34"
	assertContains "Remote batch transport status read failures should preserve status." \
		"$output" "status_read=41"
	assertContains "Remote batch malformed transport status should report context." \
		"$output" "malformed_status_error=Malformed destination discovery transport status."
	assertContains "Remote batch malformed transport status should fail closed." \
		"$output" "malformed_status=1"
	assertContains "Remote batch transport stderr read failures should preserve status." \
		"$output" "stderr_read=42"
	assertContains "Remote batch transport stderr stage failures should preserve status." \
		"$output" "err_write=43"
	assertContains "Remote batch parse failures should report malformed batch context." \
		"$output" "parse_error=Malformed destination discovery batch response."
	assertContains "Remote batch parse failures should preserve parser status." \
		"$output" "parse=1"
	assertContains "Remote batch status load failures should report malformed batch context." \
		"$output" "load_error=Malformed destination discovery batch response."
	assertContains "Remote batch status load failures should preserve status." \
		"$output" "load=44"
	assertContains "Remote batch SSH failures should clean the transport status sidecar." \
		"$output" "ssh_cleanup=$TEST_TMPDIR/remote_batch_ssh_1.out"
	assertContains "Remote batch SSH failures should clean the transport stderr sidecar." \
		"$output" "ssh_cleanup=$TEST_TMPDIR/remote_batch_ssh_2.out"
	assertContains "Remote batch SSH failures should clean the batch status sidecar." \
		"$output" "ssh_cleanup=$TEST_TMPDIR/remote_batch_ssh_3.out"
	assertContains "Remote batch parse failures should clean the transport status sidecar." \
		"$output" "parse_cleanup=$TEST_TMPDIR/remote_batch_parse_1.out"
	assertContains "Remote batch parse failures should clean the transport stderr sidecar." \
		"$output" "parse_cleanup=$TEST_TMPDIR/remote_batch_parse_2.out"
	assertContains "Remote batch parse failures should clean the batch status sidecar." \
		"$output" "parse_cleanup=$TEST_TMPDIR/remote_batch_parse_3.out"
}

test_get_zfs_list_local_destination_discovery_does_not_use_remote_batch() {
	ssh_log="$TEST_TMPDIR/get_zfs_local_batch_guard.ssh"
	zfs_log="$TEST_TMPDIR/get_zfs_local_batch_guard.zfs"
	: >"$ssh_log"
	: >"$zfs_log"

	output=$(
		(
			SSH_LOG="$ssh_log"
			ZFS_LOG="$zfs_log"
			g_option_T_target_host=""
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "unexpected-ssh" >>"$SSH_LOG"
				return 99
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$ZFS_LOG"
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ]; then
					printf '%s\t%s\n' "backup/dst/src@snapA" "guid-a"
					return 0
				fi
				return 99
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'raw=%s\n' "$g_rzfs_list_hr_snap"
		)
	)

	assertEquals "Local destination discovery should not invoke the remote batch path." \
		"" "$(cat "$ssh_log")"
	assertContains "Local destination discovery should keep using the direct recursive dataset inventory command." \
		"$(cat "$zfs_log")" "list -t filesystem,volume -Hr -o name backup/dst"
	assertContains "Local destination discovery should keep using the direct unsorted destination snapshot command." \
		"$(cat "$zfs_log")" "list -Hr -o name,guid -t snapshot backup/dst/src"
	assertContains "Local destination discovery should still publish the recursive destination inventory." \
		"$output" "dest=backup/dst
backup/dst/src"
	assertContains "Local destination discovery should still publish the raw destination snapshot cache." \
		"$output" "raw=backup/dst/src@snapA	guid-a"
}

test_get_zfs_list_tracks_stage_timings_when_very_verbose() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_profile.counter"
			now_counter_file="$TEST_TMPDIR/get_zfs_profile.now.counter"
			printf '%s\n' 0 >"$counter_file"
			printf '%s\n' 0 >"$now_counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_profile.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_profile.$idx"
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 1000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 1500
				elif [ "$idx" = "3" ]; then
					printf '%s\n' 1900
				elif [ "$idx" = "4" ]; then
					printf '%s\n' 2600
				elif [ "$idx" = "5" ]; then
					printf '%s\n' 3000
				elif [ "$idx" = "6" ]; then
					printf '%s\n' 3550
				fi
			}
			zxfer_echoV() {
				:
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_reverse_file_lines() {
				cat "$1"
			}
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_ms=%s\n' "${g_zxfer_profile_source_snapshot_listing_ms:-0}"
			printf 'destination_ms=%s\n' "${g_zxfer_profile_destination_snapshot_listing_ms:-0}"
			printf 'diff_ms=%s\n' "${g_zxfer_profile_snapshot_diff_sort_ms:-0}"
		)
	)

	assertContains "Very-verbose snapshot discovery should accumulate source snapshot listing timings." \
		"$output" "source_ms=1600"
	assertContains "Very-verbose snapshot discovery should accumulate destination listing timings." \
		"$output" "destination_ms=400"
	assertContains "Very-verbose snapshot discovery should accumulate diff/sort timings." \
		"$output" "diff_ms=550"
}

test_get_zfs_list_throws_when_source_snapshot_list_is_empty() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_empty.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_empty.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_empty.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Empty source snapshot listings should abort with zxfer's direct invariant failure status." 1 "$status"
	assertContains "Empty source snapshot listings should surface the retrieval failure." \
		"$output" "Failed to retrieve snapshots from the source"
}

test_get_zfs_list_restores_source_last_command_when_background_snapshot_listing_fails() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			counter_file="$TEST_TMPDIR/get_zfs_fail.counter"
			dest_cache_stage_path=""
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/get_zfs_fail.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_fail.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing command" >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'printf \"%s\\n\" \"missing command\" >&2; exit 37'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=37
				g_zxfer_background_job_wait_report_failure=""
				return 0
			}
			zxfer_exists_destination() {
				printf '%s\n' 0
			}
			zxfer_write_destination_snapshot_list_to_files() {
				dest_cache_stage_path=$1
				: >"$1"
				: >"$2"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "backup"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'dst_cache=<%s>\n' "${g_zxfer_destination_snapshot_record_cache_file:-}"
				if [ -n "$dest_cache_stage_path" ] && [ -e "$dest_cache_stage_path" ]; then
					printf 'dst_cache_exists=yes\n'
				else
					printf 'dst_cache_exists=no\n'
				fi
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source snapshot listing failures should propagate the exact worker status." 37 "$status"
	assertContains "Failure handling should restore the source snapshot command before reporting." \
		"$output" "cmd=sh -c 'printf \"%s"
	assertContains "The restored command should still reference the failing source snapshot probe." \
		"$output" "\"missing command\" >&2; exit 37'"
	assertContains "Background source snapshot listing failures should clear the remembered destination snapshot cache path before reporting." \
		"$output" "dst_cache=<>"
	assertContains "Background source snapshot listing failures should remove the staged destination snapshot cache file before reporting." \
		"$output" "dst_cache_exists=no"
	assertContains "Failure handling should still emit the source snapshot error." \
		"$output" "msg=Failed to retrieve snapshots from the source: missing command"
}

test_get_zfs_list_reports_generic_source_failure_when_background_snapshot_listing_has_no_stderr() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			counter_file="$TEST_TMPDIR/get_zfs_fail_blank.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_fail_blank.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_fail_blank.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=1
				g_zxfer_background_job_wait_report_failure=""
				return 0
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source snapshot failures without stderr should still propagate the exact worker status." 1 "$status"
	assertContains "Failure handling should still restore the last attempted source snapshot command." \
		"$output" "cmd=sh -c 'exit 1'"
	assertContains "Failure handling should fall back to the generic source snapshot retrieval error when stderr is empty." \
		"$output" "msg=Failed to retrieve snapshots from the source"
}

test_get_zfs_list_reports_supervisor_completion_failures_before_source_stderr_handling() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_completion_write.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_completion_write.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 125'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=125
				g_zxfer_background_job_wait_report_failure="completion_write"
				return 0
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Supervisor completion-write failures during source snapshot discovery should keep the generic failure exit status." \
		1 "$status"
	assertContains "Source snapshot discovery should report supervisor completion-write failures directly instead of misattributing them to source stderr." \
		"$output" "msg=Failed to report source snapshot discovery completion."
}

test_get_zfs_list_reports_supervisor_queue_publish_failures_before_source_stderr_handling() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_queue_write.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_queue_write.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 125'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=125
				g_zxfer_background_job_wait_report_failure="queue_write"
				return 0
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Supervisor queue-publication failures during source snapshot discovery should keep the generic failure exit status." \
		1 "$status"
	assertContains "Source snapshot discovery should report supervisor queue-publication failures directly instead of misattributing them to source stderr." \
		"$output" "msg=Failed to publish source snapshot discovery completion."
}

test_get_zfs_list_reports_source_stderr_readback_failures_after_background_failure() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing stderr capture" >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=1
				g_zxfer_background_job_wait_report_failure=""
				return 0
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				return 31
			}
			zxfer_throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source stderr readback failures should preserve the readback status." 31 "$status"
	assertContains "Background source stderr readback failures should still restore the source snapshot command context." \
		"$output" "cmd=sh -c 'exit 1'"
	assertContains "Background source stderr readback failures should report the staged stderr context." \
		"$output" "msg=Failed to read staged source snapshot stderr."
}
