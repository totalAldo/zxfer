#!/bin/sh
# shellcheck shell=sh
# Remote destination batching, staged status, and orchestration failure cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Emit the target renderer's exact destination-discovery wire order. Keeping
# valid fixtures here makes reordered protocol rows stand out as adversarial.
zxfer_test_emit_remote_destination_discovery_batch() {
	l_test_remote_batch_inventory_status=$1
	l_test_remote_batch_pool_status=$2
	l_test_remote_batch_snapshot_status=$3
	l_test_remote_batch_snapshot_ran=$4
	l_test_remote_batch_inventory_stdout=$5
	l_test_remote_batch_inventory_stderr=$6
	l_test_remote_batch_snapshot_stdout=$7
	l_test_remote_batch_snapshot_stderr=$8

	printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
	printf 'BEGIN\tsnapshot_stdout\n'
	[ -z "$l_test_remote_batch_snapshot_stdout" ] ||
		printf '%s\n' "$l_test_remote_batch_snapshot_stdout"
	printf 'END\tsnapshot_stdout\n'
	printf 'STATUS\tinventory\t%s\n' "$l_test_remote_batch_inventory_status"
	printf 'STATUS\tpool\t%s\n' "$l_test_remote_batch_pool_status"
	printf 'STATUS\tsnapshot_ran\t%s\n' "$l_test_remote_batch_snapshot_ran"
	printf 'BEGIN\tinventory_stdout\n'
	[ -z "$l_test_remote_batch_inventory_stdout" ] ||
		printf '%s\n' "$l_test_remote_batch_inventory_stdout"
	printf 'END\tinventory_stdout\n'
	printf 'BEGIN\tinventory_stderr\n'
	[ -z "$l_test_remote_batch_inventory_stderr" ] ||
		printf '%s\n' "$l_test_remote_batch_inventory_stderr"
	printf 'END\tinventory_stderr\n'
	printf 'BEGIN\tpool_stderr\n'
	printf 'END\tpool_stderr\n'
	printf 'STATUS\tsnapshot\t%s\n' "$l_test_remote_batch_snapshot_status"
	printf 'BEGIN\tsnapshot_stderr\n'
	[ -z "$l_test_remote_batch_snapshot_stderr" ] ||
		printf '%s\n' "$l_test_remote_batch_snapshot_stderr"
	printf 'END\tsnapshot_stderr\n'
	printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
}

# Allocate the four direct run-root children accepted by the publication API.
zxfer_test_allocate_remote_destination_batch_outputs() {
	zxfer_create_temp_file_group 4 >/dev/null || return "$?"
	{
		IFS= read -r g_test_remote_batch_inventory_file
		IFS= read -r g_test_remote_batch_inventory_error_file
		IFS= read -r g_test_remote_batch_snapshot_file
		IFS= read -r g_test_remote_batch_snapshot_error_file
	} <<-EOF
		$g_zxfer_temp_file_group_result
	EOF
}

zxfer_test_seed_remote_destination_batch_outputs() {
	printf '%s' 'old-inventory' >"$g_test_remote_batch_inventory_file"
	printf '%s' 'old-inventory-error' >"$g_test_remote_batch_inventory_error_file"
	printf '%s' 'old-snapshot' >"$g_test_remote_batch_snapshot_file"
	printf '%s' 'old-snapshot-error' >"$g_test_remote_batch_snapshot_error_file"
}

zxfer_test_print_remote_destination_batch_outputs() {
	printf 'inventory=%s\n' "$(cat "$g_test_remote_batch_inventory_file")"
	printf 'inventory_error=%s\n' "$(cat "$g_test_remote_batch_inventory_error_file")"
	printf 'snapshot=%s\n' "$(cat "$g_test_remote_batch_snapshot_file")"
	printf 'snapshot_error=%s\n' "$(cat "$g_test_remote_batch_snapshot_error_file")"
}

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
				zxfer_test_emit_remote_destination_discovery_batch \
					0 "" 0 1 \
					"backup/dst
backup/dst/src" "" \
					"backup/dst/src@snapA	guid-a
backup/dst/src/child@snapB	guid-b" ""
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

test_build_remote_destination_discovery_batch_script_preserves_dependency_path_failures() {
	set +e
	output=$(
		(
			zxfer_get_effective_dependency_path() {
				return 48
			}
			zxfer_build_remote_destination_discovery_batch_script \
				backup/dst backup/dst/src backup
		)
	)
	status=$?
	set -e

	assertEquals "Remote destination rendering should preserve secure PATH validation failures." \
		48 "$status"
	assertEquals "A failed secure PATH stage should not emit a partial remote script." \
		"" "$output"
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

	injection_marker="$TEST_TMPDIR/remote-batch-render-injected"
	injected_dataset="backup/dst/src'; : >'$injection_marker'; #"
	injected_script=$(zxfer_build_remote_destination_discovery_batch_script \
		"backup/dst" "$injected_dataset" "backup")
	ZXFER_FAKE_ZFS_LOG="$zfs_log" TMPDIR="$TEST_TMPDIR" \
		sh -c "$injected_script" >/dev/null 2>&1 || :
	if [ -e "$injection_marker" ]; then
		injection_status=0
	else
		injection_status=1
	fi
	assertEquals "Quoted remote dataset values must not execute inserted shell syntax." \
		1 "$injection_status"
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
				zxfer_test_emit_remote_destination_discovery_batch \
					1 0 0 0 "" \
					"cannot open 'backup/dst': no such pool or dataset" \
					"" ""
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
				zxfer_test_emit_remote_destination_discovery_batch \
					13 "" 0 0 "" "permission denied" "" ""
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

test_get_zfs_list_remote_target_transport_failures_preserve_diagnostic_and_status() {
	set +e
	output=$(
		(
			g_option_T_target_host=target.example
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' 'tank/src@snapA' >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ssh transport timed out' >&2
				return 34
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

	assertEquals "Remote destination transport failures should preserve SSH status." \
		34 "$status"
	assertContains "Remote destination transport failures should preserve staged SSH diagnostics." \
		"$output" 'Failed to retrieve list of datasets from the destination: ssh transport timed out'
}

test_remote_destination_failure_staging_preserves_original_status_and_diagnostic() {
	output=$(
		set +e
		(
			g_zxfer_full_remote_destination_list_error_file=unused-error-stage
			zxfer_run_remote_destination_discovery_batch_to_files() {
				g_zxfer_remote_destination_discovery_failure_kind=transport
				g_zxfer_remote_destination_discovery_transport_stderr_result='ssh transport timed out'
				return 37
			}
			zxfer_stage_full_remote_destination_failure_error() {
				return 44
			}
			zxfer_cleanup_failed_full_remote_destination_snapshot_discovery() {
				printf '%s\n' cleanup=complete
			}
			zxfer_throw_error() {
				printf 'error=%s\nerror_status=%s\n' "$1" "${2:-1}"
				return 0
			}
			zxfer_run_and_publish_full_remote_destination_discovery_batch \
				backup/dst/src
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Failure-diagnostic staging errors should preserve the original batch status." \
		"$output" 'status=37'
	assertContains "Failure-diagnostic staging errors should preserve validated SSH diagnostics." \
		"$output" 'error=Failed to retrieve list of datasets from the destination: ssh transport timed out'
	assertContains "Failure-diagnostic staging errors should report the original status." \
		"$output" 'error_status=37'
	assertContains "Failure-diagnostic staging errors should clean discovery state before reporting." \
		"$output" 'cleanup=complete'
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
				zxfer_test_emit_remote_destination_discovery_batch \
					0 "" 17 1 \
					"backup/dst
backup/dst/src" "" "" "snapshot list failed"
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
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs

	output=$(
		set +e
		(
			g_destination=backup/dst
			zxfer_build_remote_destination_discovery_batch_script() {
				return 32
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
		)
		printf 'build=%s\n' "$?"

		(
			g_destination=backup/dst
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' batch-script
			}
			zxfer_build_remote_sh_c_command() {
				return 33
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
		)
		printf 'command=%s\n' "$?"

		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' batch-script
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' remote-command
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' transport-policy-failed
				return 35
			}
			zxfer_throw_error() {
				printf 'transport_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
		)
		printf 'transport_policy=%s\n' "$?"

		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' batch-script
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' remote-command
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' ssh
			}
			zxfer_prepare_ssh_shell_command_context() {
				g_zxfer_ssh_shell_context_error_result='wrapper setup failed'
				return 38
			}
			zxfer_throw_error() {
				printf 'context_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
		)
		printf 'context=%s\n' "$?"

		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' batch-script
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' remote-command
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' ssh
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_create_private_temp_dir() {
				return 31
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
		)
		printf 'workspace=%s\n' "$?"

		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' batch-script
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' remote-command
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' ssh
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'transport stderr' >&2
				return 34
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			l_test_remote_transport_status=$?
			printf 'transport_status=%s\n' "$l_test_remote_transport_status"
			printf 'transport_stderr=%s\n' \
				"$(zxfer_get_remote_destination_discovery_transport_stderr)"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)

	assertContains "Remote batch script render failures should preserve status." \
		"$output" 'build=32'
	assertContains "Remote batch command render failures should preserve status." \
		"$output" 'command=33'
	assertContains "Remote transport-policy failures should preserve status." \
		"$output" 'transport_policy=35'
	assertContains "Remote transport-policy failures should preserve diagnostics." \
		"$output" 'transport_error=transport-policy-failed'
	assertContains "Wrapper-context failures should preserve status and context." \
		"$output" 'context_error=wrapper setup failed'
	assertContains "Wrapper-context diagnostics should preserve the legacy reporter status." \
		"$output" 'context=1'
	assertContains "Workspace allocation failures should preserve status." \
		"$output" 'workspace=31'
	assertContains "SSH failures should preserve the exact transport status." \
		"$output" 'transport_status=34'
	assertContains "SSH failures should retain the checked diagnostic channel." \
		"$output" 'transport_stderr=transport stderr'
	assertContains "SSH failures must leave inventory output untouched." \
		"$output" 'inventory=old-inventory'
	assertContains "SSH failures must leave inventory stderr untouched." \
		"$output" 'inventory_error=old-inventory-error'
	assertContains "SSH failures must leave snapshot output untouched." \
		"$output" 'snapshot=old-snapshot'
	assertContains "SSH failures must leave snapshot stderr untouched." \
		"$output" 'snapshot_error=old-snapshot-error'
}

test_prepare_remote_destination_discovery_batch_preserves_rootless_pool_and_wrapper_status() {
	pool_file="$TEST_TMPDIR/remote-batch-rootless-pool"
	set +e
	(
		g_destination=backup
		g_option_T_target_host=target.example
		g_zxfer_ssh_shell_context_error_result=""
		zxfer_build_remote_destination_discovery_batch_script() {
			printf '%s\n' "$3" >"$pool_file"
			printf '%s\n' batch-script
		}
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' remote-command
		}
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' ssh
		}
		zxfer_prepare_ssh_shell_command_context() {
			return 39
		}
		zxfer_prepare_remote_destination_discovery_batch_command backup/src
	)
	status=$?
	set -e

	assertEquals "Rootless destinations should pass their full name as the pool probe." \
		backup "$(cat "$pool_file")"
	assertEquals "Wrapper-context failures without diagnostics should preserve status." \
		39 "$status"
}

test_run_remote_destination_discovery_batch_preserves_transport_sidecar_read_failures() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs

	output=$(
		set +e
		g_destination=backup/dst
		g_option_T_target_host=target.example
		zxfer_prepare_remote_destination_discovery_batch_command() {
			g_zxfer_remote_destination_discovery_command_result=remote-command
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
		}
		zxfer_read_snapshot_discovery_capture_file() {
			return 41
		}
		zxfer_run_remote_destination_discovery_batch_to_files \
			backup/dst/src \
			"$g_test_remote_batch_inventory_file" \
			"$g_test_remote_batch_inventory_error_file" \
			"$g_test_remote_batch_snapshot_file" \
			"$g_test_remote_batch_snapshot_error_file"
		printf 'status_read=%s\n' "$?"

		g_destination=backup/dst
		g_option_T_target_host=target.example
		zxfer_prepare_remote_destination_discovery_batch_command() {
			g_zxfer_remote_destination_discovery_command_result=remote-command
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			return 34
		}
		zxfer_read_snapshot_discovery_capture_file() {
			if [ "${1##*/}" = transport.stderr ]; then
				return 42
			fi
			zxfer_read_runtime_artifact_file "$1" >/dev/null || return "$?"
			g_zxfer_snapshot_discovery_file_read_result=$g_zxfer_runtime_artifact_read_result
		}
		zxfer_run_remote_destination_discovery_batch_to_files \
			backup/dst/src \
			"$g_test_remote_batch_inventory_file" \
			"$g_test_remote_batch_inventory_error_file" \
			"$g_test_remote_batch_snapshot_file" \
			"$g_test_remote_batch_snapshot_error_file"
		printf 'stderr_read=%s\n' "$?"
	)

	assertContains "Transport-status read failures should preserve exact status." \
		"$output" 'status_read=41'
	assertContains "Transport-stderr read failures should preserve exact status." \
		"$output" 'stderr_read=42'
	assertEquals "Transport sidecar read failures must leave inventory untouched." \
		old-inventory "$(cat "$g_test_remote_batch_inventory_file")"
	assertEquals "Transport sidecar read failures must leave snapshots untouched." \
		old-snapshot "$(cat "$g_test_remote_batch_snapshot_file")"
}

test_run_remote_destination_discovery_batch_uses_one_workspace_and_one_ssh() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs
	ssh_log="$TEST_TMPDIR/remote-batch-workspace.ssh"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG=$ssh_log
			g_destination=backup/dst
			g_option_T_target_host=target.example
			l_test_remote_workspace="$g_zxfer_run_tmp_root/remote-batch-workspace"
			l_test_remote_cleanup_count=0
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' batch-script
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' remote-command
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' ssh
			}
			zxfer_prepare_ssh_shell_command_context() {
				return 0
			}
			zxfer_create_private_temp_dir() {
				mkdir -m 700 "$l_test_remote_workspace" || return "$?"
				g_zxfer_runtime_artifact_path_result=$l_test_remote_workspace
				printf '%s\n' "$l_test_remote_workspace"
			}
			zxfer_cleanup_runtime_artifact_path() {
				[ "$1" = "$l_test_remote_workspace" ] || return 91
				l_test_remote_cleanup_count=$((l_test_remote_cleanup_count + 1))
				rm -rf "$l_test_remote_workspace"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' ssh >>"$SSH_LOG"
				zxfer_test_emit_remote_destination_discovery_batch \
					0 "" 0 1 \
					"backup/dst
backup/dst/src" "" \
					"backup/dst/src@snapA	guid-a" ""
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'status=%s\n' "$?"
			printf 'cleanup_count=%s\n' "$l_test_remote_cleanup_count"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)

	assertContains "Valid batches should succeed." "$output" 'status=0'
	assertEquals "One remote batch should invoke SSH exactly once." \
		1 "$(wc -l <"$ssh_log" | tr -d '[:space:]')"
	assertContains "One remote batch should clean its workspace exactly once." \
		"$output" 'cleanup_count=1'
	assertContains "Valid batches should publish complete inventory output." \
		"$output" 'inventory=backup/dst
backup/dst/src'
	assertContains "Valid batches should publish complete snapshot output." \
		"$output" 'snapshot=backup/dst/src@snapA	guid-a'
	assertEquals "Normal cleanup should remove the contained workspace." \
		"" "$(find "$g_zxfer_run_tmp_root" -type d -name 'remote-batch-workspace' -print)"
}

test_run_remote_destination_discovery_batch_rejects_truncated_and_reordered_protocols() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs

	output=$(
		set +e
		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_prepare_remote_destination_discovery_batch_command() {
				g_zxfer_remote_destination_discovery_command_result=remote-command
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf '%s\n' 'backup/dst/src@snapA	guid-a'
			}
			zxfer_throw_error() {
				printf 'truncated_error=%s\n' "$1"
				return 0
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'truncated_status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)
	assertContains "Truncated protocols should preserve parser status." \
		"$output" 'truncated_status=1'
	assertContains "Truncated protocols should report malformed context." \
		"$output" 'truncated_error=Malformed destination discovery batch response.'
	assertContains "Truncated protocols must leave all outputs on the old generation." \
		"$output" 'snapshot_error=old-snapshot-error'

	zxfer_test_seed_remote_destination_batch_outputs
	output=$(
		set +e
		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_prepare_remote_destination_discovery_batch_command() {
				g_zxfer_remote_destination_discovery_command_result=remote-command
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
			}
			zxfer_throw_error() {
				return 0
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'reordered_status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)
	assertContains "Reordered protocols should fail closed." \
		"$output" 'reordered_status=1'
	assertContains "Reordered protocols must leave inventory on the old generation." \
		"$output" 'inventory=old-inventory'
	assertContains "Reordered protocols must leave snapshot on the old generation." \
		"$output" 'snapshot=old-snapshot'
}

test_run_remote_destination_discovery_batch_preserves_stage_and_readback_failures() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs
	ssh_log="$TEST_TMPDIR/remote-batch-stage-failure.ssh"
	: >"$ssh_log"

	output=$(
		set +e
		(
			SSH_LOG=$ssh_log
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_prepare_remote_destination_discovery_batch_command() {
				g_zxfer_remote_destination_discovery_command_result=remote-command
			}
			zxfer_initialize_remote_destination_discovery_workspace_files() {
				return 45
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' ssh >>"$SSH_LOG"
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'stage_status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)
	assertContains "Workspace stage failures should preserve exact status." \
		"$output" 'stage_status=45'
	assertEquals "Workspace stage failures should happen before SSH." \
		"" "$(cat "$ssh_log")"
	assertContains "Workspace stage failures must preserve output state." \
		"$output" 'inventory=old-inventory'

	zxfer_test_seed_remote_destination_batch_outputs
	output=$(
		set +e
		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_prepare_remote_destination_discovery_batch_command() {
				g_zxfer_remote_destination_discovery_command_result=remote-command
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				zxfer_test_emit_remote_destination_discovery_batch \
					0 "" 0 1 'backup/dst' "" "" ""
			}
			zxfer_validate_remote_destination_discovery_workspace_files() {
				return 46
			}
			zxfer_throw_error() {
				return 0
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'readback_status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)
	assertContains "Readback validation failures should preserve exact status." \
		"$output" 'readback_status=46'
	assertContains "Readback failures must preserve snapshot output state." \
		"$output" 'snapshot=old-snapshot'
}

test_run_remote_destination_discovery_batch_rolls_back_late_publish_failures() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs

	output=$(
		set +e
		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_prepare_remote_destination_discovery_batch_command() {
				g_zxfer_remote_destination_discovery_command_result=remote-command
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				zxfer_test_emit_remote_destination_discovery_batch \
					0 "" 0 1 'backup/dst' "" \
					'backup/dst/src@snapA	guid-a' ""
			}
			zxfer_publish_remote_destination_discovery_staged_files() {
				mv -f \
					"$g_zxfer_remote_destination_discovery_inventory_stage_file" \
					"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file" \
					"$g_zxfer_run_tmp_root" || return "$?"
				return 47
			}
			zxfer_throw_error() {
				printf 'publish_error=%s\n' "$1"
				return 0
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'publish_status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)

	assertContains "Late publish failures should preserve their exact status." \
		"$output" 'publish_status=47'
	assertContains "Late publish failures should report malformed batch context." \
		"$output" 'publish_error=Malformed destination discovery batch response.'
	assertContains "Late publish failures should roll inventory back." \
		"$output" 'inventory=old-inventory'
	assertContains "Late publish failures should roll inventory stderr back." \
		"$output" 'inventory_error=old-inventory-error'
	assertContains "Late publish failures should roll snapshots back." \
		"$output" 'snapshot=old-snapshot'
	assertContains "Late publish failures should roll snapshot stderr back." \
		"$output" 'snapshot_error=old-snapshot-error'
}

test_remote_destination_discovery_transaction_restores_partial_backup() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs

	output=$(
		set +e
		(
			zxfer_allocate_remote_destination_discovery_workspace \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file" || exit "$?"
			printf '%s' new-inventory >"$g_zxfer_remote_destination_discovery_inventory_stage_file"
			printf '%s' new-inventory-error >"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file"
			printf '%s' new-snapshot >"$g_zxfer_remote_destination_discovery_snapshot_stage_file"
			printf '%s' new-snapshot-error >"$g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file"
			zxfer_backup_remote_destination_discovery_publish_targets() {
				mv -f "$g_zxfer_remote_destination_discovery_inventory_target_file" \
					"$g_zxfer_remote_destination_discovery_rollback_dir" || return "$?"
				return 48
			}
			zxfer_publish_remote_destination_discovery_workspace_files
			printf 'status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
			zxfer_cleanup_remote_destination_discovery_workspace
		)
	)

	assertContains "Partial backup failures should preserve their original status." \
		"$output" 'status=48'
	assertContains "Partial backup failures should restore inventory." \
		"$output" 'inventory=old-inventory'
	assertContains "Partial backup failures should retain inventory stderr." \
		"$output" 'inventory_error=old-inventory-error'
	assertContains "Partial backup failures should retain snapshots." \
		"$output" 'snapshot=old-snapshot'
	assertContains "Partial backup failures should retain snapshot stderr." \
		"$output" 'snapshot_error=old-snapshot-error'
}

test_remote_destination_discovery_transaction_clears_all_targets_when_rollback_fails() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs

	output=$(
		set +e
		(
			zxfer_allocate_remote_destination_discovery_workspace \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file" || exit "$?"
			printf '%s' new-inventory >"$g_zxfer_remote_destination_discovery_inventory_stage_file"
			printf '%s' new-inventory-error >"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file"
			printf '%s' new-snapshot >"$g_zxfer_remote_destination_discovery_snapshot_stage_file"
			printf '%s' new-snapshot-error >"$g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file"
			zxfer_publish_remote_destination_discovery_staged_files() {
				mv -f \
					"$g_zxfer_remote_destination_discovery_inventory_stage_file" \
					"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file" \
					"$g_zxfer_run_tmp_root" || return "$?"
				return 49
			}
			zxfer_restore_remote_destination_discovery_publish_targets() {
				return 1
			}
			zxfer_publish_remote_destination_discovery_workspace_files
			printf 'status=%s\n' "$?"
			zxfer_test_print_remote_destination_batch_outputs
			zxfer_cleanup_remote_destination_discovery_workspace
		)
	)

	assertEquals "A failed rollback should preserve the publication status and clear every caller target." \
		"status=49
inventory=
inventory_error=
snapshot=
snapshot_error=" "$output"
}

test_remote_destination_discovery_workspace_rejects_untrusted_publish_targets() {
	zxfer_test_allocate_remote_destination_batch_outputs
	outside_target="$TEST_TMPDIR/outside-remote-batch-target"
	workspace_log="$TEST_TMPDIR/untrusted-remote-batch-workspace.log"
	: >"$outside_target"
	: >"$workspace_log"

	output=$(
		set +e
		(
			WORKSPACE_LOG=$workspace_log
			zxfer_create_private_temp_dir() {
				printf '%s\n' called >>"$WORKSPACE_LOG"
				return 90
			}
			zxfer_allocate_remote_destination_discovery_workspace \
				"$outside_target" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'outside_status=%s\n' "$?"
			zxfer_allocate_remote_destination_discovery_workspace \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'duplicate_status=%s\n' "$?"
			mv "$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_file.real" || exit "$?"
			ln -s "$g_test_remote_batch_inventory_file.real" \
				"$g_test_remote_batch_inventory_file" || exit "$?"
			zxfer_allocate_remote_destination_discovery_workspace \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'symlink_status=%s\n' "$?"
		)
	)

	assertContains "Workspace allocation should reject targets outside the private run root." \
		"$output" 'outside_status=1'
	assertContains "Workspace allocation should reject duplicate publish targets." \
		"$output" 'duplicate_status=1'
	assertContains "Workspace allocation should reject symlink publish targets." \
		"$output" 'symlink_status=1'
	assertEquals "Rejected targets should fail before allocating a workspace." \
		"" "$(cat "$workspace_log")"
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

test_prepare_remote_destination_discovery_batch_preserves_transport_token_failure_after_reporting() {
	set +e
	output=$(
		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example invalid-wrapper"
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "sh -c batch"
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "invalid wrapper transport"
				return 44
			}
			zxfer_throw_error() {
				printf 'reported=%s status=%s\n' "$1" "$2"
				return 0
			}
			zxfer_prepare_remote_destination_discovery_batch_command "backup/dst/src"
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "The prepared-command test wrapper should finish after publishing the preserved status." \
		0 "$status"
	assertContains "Transport-token failures should pass their diagnostic and status through the reporter." \
		"$output" "reported=invalid wrapper transport status=44"
	assertContains "A returning reporter should not replace the transport-token failure status." \
		"$output" "status=44"
}

test_remote_destination_discovery_publish_target_rejects_reserved_workspace_names() {
	reserved_target="$g_zxfer_run_tmp_root/rollback"
	zxfer_write_runtime_artifact_file "$reserved_target" "old payload"

	set +e
	zxfer_remote_destination_discovery_publish_target_is_valid "$reserved_target"
	status=$?
	set -e

	assertEquals "Caller-visible discovery files should reject names reserved for the contained workspace transaction." \
		1 "$status"
}

test_load_remote_destination_discovery_transport_status_rejects_nonnumeric_content() {
	status_file="$g_zxfer_run_tmp_root/remote-discovery-transport-status"
	zxfer_write_runtime_artifact_file "$status_file" "not-a-status"
	g_zxfer_remote_destination_discovery_transport_status_file=$status_file
	g_zxfer_remote_destination_discovery_failure_kind=""
	g_zxfer_remote_destination_discovery_failure_status=""

	set +e
	zxfer_load_remote_destination_discovery_transport_status
	status=$?
	set -e

	assertEquals "Nonnumeric SSH status sidecars should fail closed." 1 "$status"
	assertEquals "Nonnumeric SSH status sidecars should retain the malformed-status failure kind." \
		"transport_status_malformed" "$g_zxfer_remote_destination_discovery_failure_kind"
	assertEquals "Nonnumeric SSH status sidecars should publish the stable malformed-status value." \
		1 "$g_zxfer_remote_destination_discovery_failure_status"
}

test_restore_remote_destination_discovery_publish_targets_reports_each_partial_move_failure() {
	rollback_dir="$g_zxfer_run_tmp_root/remote-discovery-rollback"
	mkdir "$rollback_dir"
	g_zxfer_remote_destination_discovery_rollback_dir=$rollback_dir
	g_zxfer_remote_destination_discovery_inventory_target_file="$g_zxfer_run_tmp_root/inventory-target"
	g_zxfer_remote_destination_discovery_inventory_stderr_target_file="$g_zxfer_run_tmp_root/inventory-stderr-target"
	g_zxfer_remote_destination_discovery_snapshot_target_file="$g_zxfer_run_tmp_root/snapshot-target"
	g_zxfer_remote_destination_discovery_snapshot_stderr_target_file="$g_zxfer_run_tmp_root/snapshot-stderr-target"
	zxfer_write_runtime_artifact_file "$rollback_dir/inventory-target" "old inventory"
	zxfer_write_runtime_artifact_file "$rollback_dir/inventory-stderr-target" "old inventory stderr"
	zxfer_write_runtime_artifact_file "$rollback_dir/snapshot-target" "old snapshot"
	# Keep the set partial so the owner takes its checked one-by-one restore path.

	set +e
	output=$(
		(
			mv() {
				return 1
			}
			zxfer_restore_remote_destination_discovery_publish_targets
			printf 'status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "The partial-restore test wrapper should finish after recording the owner status." \
		0 "$status"
	assertContains "Any failed partial rollback move should make the whole four-file restore fail." \
		"$output" "status=1"

	# Exercise the fourth fixed pair separately; a complete four-file set takes
	# the optimized multi-file restore branch instead of the partial path.
	rm -f "$rollback_dir/inventory-target" \
		"$rollback_dir/inventory-stderr-target" \
		"$rollback_dir/snapshot-target"
	zxfer_write_runtime_artifact_file "$rollback_dir/snapshot-stderr-target" \
		"old snapshot stderr"
	set +e
	fourth_output=$(
		(
			mv() {
				return 1
			}
			zxfer_restore_remote_destination_discovery_publish_targets
			printf 'status=%s\n' "$?"
		)
	)
	fourth_status=$?
	set -e

	assertEquals "The fourth-pair restore test wrapper should finish after recording the owner status." \
		0 "$fourth_status"
	assertContains "A failed snapshot-stderr rollback should make the fixed-pair restore fail." \
		"$fourth_output" "status=1"
}

test_clear_remote_destination_discovery_publish_targets_reports_clear_failure() {
	g_zxfer_remote_destination_discovery_inventory_target_file="$g_zxfer_run_tmp_root/clear-inventory"
	g_zxfer_remote_destination_discovery_inventory_stderr_target_file="$g_zxfer_run_tmp_root/clear-inventory-stderr"
	g_zxfer_remote_destination_discovery_snapshot_target_file="$g_zxfer_run_tmp_root/clear-snapshot"
	g_zxfer_remote_destination_discovery_snapshot_stderr_target_file="$g_zxfer_run_tmp_root/clear-snapshot-stderr"

	set +e
	output=$(
		(
			zxfer_write_runtime_artifact_file() {
				return 27
			}
			zxfer_clear_remote_destination_discovery_publish_targets
			printf 'status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "The clear-failure test wrapper should finish after recording the owner status." \
		0 "$status"
	assertContains "A failed fail-closed target clear should remain observable to its coordinator." \
		"$output" "status=1"
}

test_publish_remote_destination_discovery_workspace_clears_after_backup_restore_failure() {
	set +e
	output=$(
		(
			zxfer_backup_remote_destination_discovery_publish_targets() {
				return 31
			}
			zxfer_restore_remote_destination_discovery_publish_targets() {
				printf '%s\n' restore
				return 1
			}
			zxfer_clear_remote_destination_discovery_publish_targets() {
				printf '%s\n' clear
				return 0
			}
			zxfer_publish_remote_destination_discovery_workspace_files
			printf 'status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "The publication-failure test wrapper should finish after recording the owner status." \
		0 "$status"
	assertContains "A partial backup that cannot be restored should clear every caller-visible target." \
		"$output" "clear"
	assertContains "Publication should preserve the original backup failure over cleanup failures." \
		"$output" "status=31"
}

test_process_remote_destination_discovery_workspace_classifies_status_loader_failure() {
	set +e
	output=$(
		(
			g_zxfer_remote_destination_discovery_parser_status_result=0
			g_zxfer_remote_destination_discovery_transport_status_result=0
			g_zxfer_remote_destination_discovery_batch_status_file="batch.status"
			zxfer_execute_remote_destination_discovery_batch_pipeline() {
				return 0
			}
			zxfer_load_remote_destination_discovery_transport_status() {
				g_zxfer_remote_destination_discovery_transport_status_result=0
				return 0
			}
			zxfer_load_destination_discovery_batch_status_file() {
				return 23
			}
			zxfer_process_remote_destination_discovery_workspace
			printf 'status=%s\n' "$?"
			printf 'kind=%s\n' "$g_zxfer_remote_destination_discovery_failure_kind"
			printf 'failure_status=%s\n' "$g_zxfer_remote_destination_discovery_failure_status"
		)
	)
	status=$?
	set -e

	assertEquals "The status-loader test wrapper should finish after recording the failure channel." \
		0 "$status"
	assertContains "Malformed compact status sidecars should use the batch-status failure kind." \
		"$output" "kind=batch_status"
	assertContains "Compact status-sidecar failures should preserve their exact stage status." \
		"$output" "failure_status=23"
	assertContains "The workspace processor should return the compact status-sidecar failure." \
		"$output" "status=23"
}

test_report_remote_destination_discovery_failure_handles_malformed_and_unknown_kinds() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf 'error=%s\n' "$1"
				return 0
			}
			g_zxfer_remote_destination_discovery_failure_kind=transport_status_malformed
			g_zxfer_remote_destination_discovery_failure_status=77
			zxfer_report_remote_destination_discovery_failure
			printf 'malformed_status=%s\n' "$?"
			g_zxfer_remote_destination_discovery_failure_kind=unknown
			unset g_zxfer_remote_destination_discovery_failure_status
			zxfer_report_remote_destination_discovery_failure
			printf 'unknown_status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "The failure-reporter test wrapper should finish after recording both stable statuses." \
		0 "$status"
	assertContains "Malformed transport status should retain its operator-visible error text." \
		"$output" "error=Malformed destination discovery transport status."
	assertContains "Malformed transport status should return the stable generic status after a returning reporter." \
		"$output" "malformed_status=1"
	assertContains "Unknown failure kinds should retain the stable generic fallback status." \
		"$output" "unknown_status=1"
}
