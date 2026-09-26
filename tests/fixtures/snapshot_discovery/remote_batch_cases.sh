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
			printf 'root_cache=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'snapshot_dataset_cache=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst/src" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'raw=%s\n' "$(cat "$g_zxfer_destination_snapshot_record_cache_file")"
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
		g_target_cmd_zfs=/opt/zfs/bin/zfs
		g_zxfer_secure_path=/secure/sbin:/secure/bin
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
	assertEquals "Remote batch should allocate exactly one private target-side workspace." \
		1 "$(printf '%s\n' "$script" | grep -c 'mktemp')"
	assertContains "Remote batch should allocate that workspace with mktemp -d." \
		"$script" "mktemp -d \"\$l_tmpdir/zxfer.destination-discovery.XXXXXX\""
	assertEquals "Remote batch should remove the workspace with exactly one rm." \
		1 "$(printf '%s\n' "$script" | grep -c 'rm ')"
	assertContains "Remote batch should stage destination inventory stdout in the workspace." \
		"$script" "l_inventory_stdout_file=\$l_workspace/inventory"
	assertContains "Remote batch should still stage compact destination snapshot stderr diagnostics." \
		"$script" "l_snapshot_stderr_file=\$l_workspace/snapshot-stderr"
	assertContains "Remote batch should stream staged section bodies instead of expanding payload variables." \
		"$script" "cat \"\$l_section_file\""
	assertContains "Remote batch should stream snapshot stdout directly from zfs." \
		"$script" "\"\$l_zfs_cmd\" list -Hr -o name,guid -t snapshot \"\$l_destination_snapshot_dataset\" 2>\"\$l_snapshot_stderr_file\""
	assertContains "Remote batch should clean the target-side workspace on shell exit." \
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

# Run the rendered discovery batch under TMPDIR=$1 with a fake zfs whose
# behavior is selected by MODE ($2) and that touches MARKER ($3) when set;
# print the script status. The fake reads both from its environment, and only
# assignments before an external command are exported everywhere (FreeBSD sh
# does not export them for a function call).
zxfer_test_run_discovery_batch_script() {
	l_test_batch_tmpdir=$1
	l_test_batch_script=$(zxfer_build_remote_destination_discovery_batch_script \
		"backup/dst" "backup/dst/src" "backup")
	l_test_batch_status=0
	BATCH_ZFS_MODE=$2 BATCH_ZFS_MARKER=${3:-} TMPDIR="$l_test_batch_tmpdir" \
		sh -c "$l_test_batch_script" \
		>"$TEST_TMPDIR/discovery_batch_workspace.out" 2>&1 ||
		l_test_batch_status=$?
	printf '%s\n' "$l_test_batch_status"
}

test_remote_destination_discovery_batch_script_removes_its_workspace_on_every_exit() {
	fake_zfs="$TEST_TMPDIR/remote_batch_workspace_zfs"
	batch_tmpdir="$TEST_TMPDIR/remote_batch_workspace_tmp"
	mkdir -p "$batch_tmpdir"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
[ -z "${BATCH_ZFS_MARKER:-}" ] || : >"$BATCH_ZFS_MARKER"
case "$BATCH_ZFS_MODE:$*" in
missing:"list -H -o name backup")
	printf '%s\n' "backup"
	;;
missing:*)
	printf '%s\n' "cannot open 'backup/dst': dataset does not exist" >&2
	exit 1
	;;
term:"list -Hr -o name,guid -t snapshot backup/dst/src")
	kill -TERM "$PPID"
	;;
*)
	printf '%s\n' "backup/dst/src"
	;;
esac
EOF
	chmod +x "$fake_zfs"
	g_target_cmd_zfs=$fake_zfs

	missing_status=$(zxfer_test_run_discovery_batch_script "$batch_tmpdir" missing)
	missing_output=$(cat "$TEST_TMPDIR/discovery_batch_workspace.out")
	missing_left=$(find "$batch_tmpdir" -mindepth 1 -print)
	term_status=$(zxfer_test_run_discovery_batch_script "$batch_tmpdir" term)
	term_left=$(find "$batch_tmpdir" -mindepth 1 -print)
	# mktemp implementations disagree about a TMPDIR that does not exist, so
	# a stand-in makes the workspace allocation fail the same way everywhere.
	# A second stand-in succeeds without printing a path, which must not
	# stage the side files at the target's filesystem root.
	failing_mktemp_bin="$TEST_TMPDIR/remote_batch_failing_mktemp_bin"
	empty_mktemp_bin="$TEST_TMPDIR/remote_batch_empty_mktemp_bin"
	empty_marker="$TEST_TMPDIR/remote_batch_empty_mktemp_zfs_ran"
	mkdir -p "$failing_mktemp_bin" "$empty_mktemp_bin"
	printf '%s\n' '#!/bin/sh' 'exit 1' >"$failing_mktemp_bin/mktemp"
	printf '%s\n' '#!/bin/sh' 'exit 0' >"$empty_mktemp_bin/mktemp"
	chmod +x "$failing_mktemp_bin/mktemp" "$empty_mktemp_bin/mktemp"
	mktemp_status=$(g_zxfer_secure_path="$failing_mktemp_bin:${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}" \
		zxfer_test_run_discovery_batch_script "$batch_tmpdir" ok)
	mktemp_output=$(cat "$TEST_TMPDIR/discovery_batch_workspace.out")
	empty_status=$(g_zxfer_secure_path="$empty_mktemp_bin:${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}" \
		zxfer_test_run_discovery_batch_script "$batch_tmpdir" ok "$empty_marker")

	assertEquals "A missing destination root should still end the batch cleanly." 0 "$missing_status"
	assertContains "A missing destination root should report the pool probe status." \
		"$missing_output" "$(printf 'STATUS\tpool\t0')"
	assertEquals "The workspace should be removed after a missing-root batch." "" "$missing_left"
	assertEquals "A TERM during the snapshot listing should end the batch with status 1." 1 "$term_status"
	assertEquals "The workspace should be removed after a TERM." "" "$term_left"
	assertNotEquals "A workspace that cannot be created should fail the batch." 0 "$mktemp_status"
	assertNotContains "A batch without a workspace should print no protocol header." \
		"$mktemp_output" "ZXFER_DESTINATION_DISCOVERY_BATCH_V1"
	assertEquals "An empty workspace path should fail the batch with status 1." 1 "$empty_status"
	empty_zfs_ran=0
	[ ! -e "$empty_marker" ] || empty_zfs_ran=1
	assertEquals "An empty workspace path should stop the batch before any zfs command." \
		0 "$empty_zfs_ran"
}

test_run_remote_destination_discovery_batch_validates_statuses_in_the_parser() {
	zxfer_test_allocate_remote_destination_batch_outputs

	output=$(
		set +e
		g_destination=backup/dst
		g_option_T_target_host=target.example
		zxfer_prepare_remote_destination_discovery_batch_command() {
			g_zxfer_remote_destination_discovery_command_result=remote-command
		}
		zxfer_throw_error() {
			printf 'error=%s\n' "$1"
			return 0
		}
		for status_case in \
			"0||0|1|accepted" \
			"0|3|0|1|accepted" \
			"0	1||0|1|tab" \
			"0||0|yes|word" \
			"||0|1|empty"; do
			l_test_inventory=${status_case%%|*}
			l_test_rest=${status_case#*|}
			l_test_pool=${l_test_rest%%|*}
			l_test_rest=${l_test_rest#*|}
			l_test_snapshot=${l_test_rest%%|*}
			l_test_rest=${l_test_rest#*|}
			l_test_ran=${l_test_rest%%|*}
			l_test_label=${l_test_rest#*|}
			zxfer_invoke_ssh_shell_command_for_host() {
				zxfer_test_emit_remote_destination_discovery_batch \
					"$l_test_inventory" "$l_test_pool" "$l_test_snapshot" "$l_test_ran" \
					backup/dst "" "" ""
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf '%s=%s|%s|%s|%s|%s\n' "$l_test_label" "$?" \
				"$g_zxfer_destination_discovery_batch_inventory_status" \
				"$g_zxfer_destination_discovery_batch_pool_status" \
				"$g_zxfer_destination_discovery_batch_snapshot_status" \
				"$g_zxfer_destination_discovery_batch_snapshot_ran"
		done
	)

	assertContains "Numeric statuses with an empty pool status should be accepted." \
		"$output" "accepted=0|0||0|1"
	assertContains "A numeric pool status should be published." \
		"$output" "accepted=0|0|3|0|1"
	assertContains "A status holding a tab should fail the batch." \
		"$output" "tab=1|"
	assertContains "A non-numeric status should fail the batch." \
		"$output" "word=1|"
	assertContains "An empty inventory status should fail the batch." \
		"$output" "empty=1|"
	assertContains "Rejected statuses should report a malformed batch response." \
		"$output" "error=Malformed destination discovery batch response."
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
			printf 'root_cache=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'child_cache=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst/src" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'raw=<%s>\n' "$(cat "$g_zxfer_destination_snapshot_record_cache_file")"
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
			g_option_T_target_host=target.example
			ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
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
		zxfer_test_print_remote_destination_batch_outputs | sed 's/^/setup_/'

		(
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf '%s\n' 'backup/dst/src@partial	guid-p'
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
			zxfer_remote_destination_discovery_failure_is_transport &&
				printf 'transport_kind=yes\n'
			zxfer_test_print_remote_destination_batch_outputs
		)
	)

	assertContains "Remote transport-policy failures should throw their diagnostic." \
		"$output" 'transport_error=ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path.'
	assertContains "Remote transport-policy failures should keep the reporter status." \
		"$output" 'transport_policy=1'
	assertContains "Wrapper-context failures should preserve status and context." \
		"$output" 'context_error=wrapper setup failed'
	assertContains "Wrapper-context diagnostics should preserve the legacy reporter status." \
		"$output" 'context=1'
	assertContains "Setup failures should leave the caller outputs untouched." \
		"$output" 'setup_inventory=old-inventory'
	assertContains "SSH failures should preserve the exact transport status." \
		"$output" 'transport_status=34'
	assertContains "SSH failures should keep the ssh stderr for the caller." \
		"$output" 'transport_stderr=transport stderr'
	assertContains "SSH failures should be classified as transport failures." \
		"$output" 'transport_kind=yes'
	assertContains "SSH failures must leave no partial inventory output." \
		"$output" 'inventory=
'
	assertContains "SSH failures must leave no partial snapshot output." \
		"$output" 'snapshot=
'
	assertContains "SSH failures must empty the snapshot stderr output." \
		"$output" 'snapshot_error='
}

test_run_remote_destination_discovery_batch_publishes_no_statuses_after_late_failures() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs
	fake_awk="$TEST_TMPDIR/remote-batch-silent-awk"
	printf '#!/bin/sh\ncat >/dev/null\nexit 0\n' >"$fake_awk"
	chmod +x "$fake_awk"

	output=$(
		set +e
		g_destination=backup/dst
		g_option_T_target_host=target.example
		zxfer_prepare_remote_destination_discovery_batch_command() {
			g_zxfer_remote_destination_discovery_command_result=remote-command
		}
		zxfer_throw_error() {
			printf 'error=%s\n' "$1"
			return 0
		}
		print_batch_result() {
			printf '%s=%s|%s|%s|%s|%s|%s\n' "$1" "$2" \
				"$g_zxfer_destination_discovery_batch_inventory_status" \
				"$g_zxfer_destination_discovery_batch_pool_status" \
				"$g_zxfer_destination_discovery_batch_snapshot_status" \
				"$g_zxfer_destination_discovery_batch_snapshot_ran" \
				"$g_zxfer_remote_destination_discovery_failure_kind"
		}
		l_test_run_root_count=$(find "$g_zxfer_run_tmp_root" | wc -l)

		# ssh fails after the target wrote a complete, valid stream.
		zxfer_invoke_ssh_shell_command_for_host() {
			zxfer_test_emit_remote_destination_discovery_batch \
				1 0 0 1 "" "cannot open 'backup/dst': dataset does not exist" "" ""
			printf '%s\n' 'connection reset' >&2
			return 255
		}
		zxfer_run_remote_destination_discovery_batch_to_files \
			backup/dst/src \
			"$g_test_remote_batch_inventory_file" \
			"$g_test_remote_batch_inventory_error_file" \
			"$g_test_remote_batch_snapshot_file" \
			"$g_test_remote_batch_snapshot_error_file"
		print_batch_result late_ssh "$?"
		zxfer_test_print_remote_destination_batch_outputs

		# awk exits 0 without writing its status line.
		zxfer_invoke_ssh_shell_command_for_host() {
			zxfer_test_emit_remote_destination_discovery_batch \
				0 "" 0 1 backup/dst "" "" ""
		}
		g_cmd_awk=$fake_awk
		zxfer_run_remote_destination_discovery_batch_to_files \
			backup/dst/src \
			"$g_test_remote_batch_inventory_file" \
			"$g_test_remote_batch_inventory_error_file" \
			"$g_test_remote_batch_snapshot_file" \
			"$g_test_remote_batch_snapshot_error_file"
		print_batch_result silent_awk "$?"
		printf 'run_root_growth=%s\n' \
			"$(($(find "$g_zxfer_run_tmp_root" | wc -l) - l_test_run_root_count))"
	)

	assertContains "A late ssh failure must publish no statuses." \
		"$output" 'late_ssh=255|||||transport'
	assertContains "A late ssh failure must leave no partial inventory output." \
		"$output" 'inventory=
'
	assertContains "An awk that exits 0 without its status line must fail the batch." \
		"$output" 'silent_awk=1|||||batch_parse'
	assertContains "A missing status line should report a malformed batch response." \
		"$output" 'error=Malformed destination discovery batch response.'
	assertContains "Each batch should remove its ssh status and stderr files." \
		"$output" 'run_root_growth=0'
}

test_get_zfs_list_remote_target_late_transport_failure_reprobes_the_pool_live() {
	probe_log="$TEST_TMPDIR/remote-batch-late-failure.probe"
	: >"$probe_log"

	set +e
	output=$(
		(
			PROBE_LOG=$probe_log
			g_option_T_target_host=target.example
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' 'tank/src@snapA' >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				zxfer_test_emit_remote_destination_discovery_batch \
					1 0 0 1 "" "cannot open 'backup/dst': dataset does not exist" "" ""
				printf '%s\n' "cannot open 'backup/dst': dataset does not exist" >&2
				return 255
			}
			zxfer_run_destination_zfs_cmd() {
				printf 'probe=%s\n' "$*" >>"$PROBE_LOG"
				printf '%s\n' 'cannot open pool: I/O error' >&2
				return 1
			}
			zxfer_throw_error() {
				printf 'error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "A late ssh failure must not skip the live destination pool probe." \
		"probe=list -H -o name backup" "$(cat "$probe_log")"
	assertEquals "A pool probe failure after a late ssh failure should fail the run." \
		1 "$status"
	assertContains "A late ssh failure must not treat the destination root as missing." \
		"$output" 'error=Destination dataset [backup/dst] is missing and destination pool [backup] could not be listed: cannot open pool: I/O error'
}

test_prepare_remote_destination_discovery_batch_preserves_rootless_pool_and_wrapper_status() {
	set +e
	output=$(
		(
			g_destination=backup
			g_option_T_target_host=target.example
			g_zxfer_ssh_shell_context_error_result=""
			zxfer_prepare_ssh_shell_command_context() {
				return 39
			}
			zxfer_prepare_remote_destination_discovery_batch_command backup/src
			printf 'status=%s\n' "$?"
			printf '%s\n' "$g_zxfer_remote_snapshot_discovery_batch_script_result"
		)
	)
	set -e

	assertContains "Rootless destinations should pass their full name as the pool probe." \
		"$output" "l_destination_pool='backup'"
	assertContains "Wrapper-context failures without diagnostics should preserve status." \
		"$output" "status=39"
}

test_run_remote_destination_discovery_batch_rejects_an_unreadable_transport_status() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs
	status_dir="$TEST_TMPDIR/remote-batch-status-dir"
	mkdir -p "$status_dir"

	output=$(
		set +e
		g_destination=backup/dst
		g_option_T_target_host=target.example
		l_test_temp_count=0
		zxfer_prepare_remote_destination_discovery_batch_command() {
			g_zxfer_remote_destination_discovery_command_result=remote-command
		}
		# Hand out a directory as the status file so the ssh status is never
		# written or read back.
		zxfer_get_temp_file() {
			l_test_temp_count=$((l_test_temp_count + 1))
			if [ "$l_test_temp_count" -eq 1 ]; then
				g_zxfer_temp_file_result=$status_dir
			else
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote-batch-status-dir.stderr"
			fi
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			zxfer_test_emit_remote_destination_discovery_batch \
				0 "" 0 1 backup/dst "" "backup/dst/src@snapA	guid-a" ""
		}
		zxfer_throw_error() {
			printf 'error=%s\n' "$1"
			return 0
		}
		zxfer_run_remote_destination_discovery_batch_to_files \
			backup/dst/src \
			"$g_test_remote_batch_inventory_file" \
			"$g_test_remote_batch_inventory_error_file" \
			"$g_test_remote_batch_snapshot_file" \
			"$g_test_remote_batch_snapshot_error_file" 2>/dev/null
		printf 'status=%s\n' "$?"
		zxfer_test_print_remote_destination_batch_outputs
	)

	assertContains "A missing ssh status should fail closed." "$output" 'status=1'
	assertContains "A missing ssh status should report the malformed transport status." \
		"$output" 'error=Malformed destination discovery transport status.'
	assertContains "A missing ssh status must not publish the parsed snapshots." \
		"$output" 'snapshot=
'
}

test_run_remote_destination_discovery_batch_streams_into_caller_files_with_one_ssh() {
	zxfer_test_allocate_remote_destination_batch_outputs
	zxfer_test_seed_remote_destination_batch_outputs
	ssh_log="$TEST_TMPDIR/remote-batch-stream.ssh"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG=$ssh_log
			g_destination=backup/dst
			g_option_T_target_host=target.example
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'host=%s side=%s\n' "$1" "$3" >>"$SSH_LOG"
				zxfer_test_emit_remote_destination_discovery_batch \
					0 "" 0 1 \
					"backup/dst
backup/dst/src" "" \
					"backup/dst/src@snapA	guid-a" "snapshot warning"
			}
			zxfer_run_remote_destination_discovery_batch_to_files \
				backup/dst/src \
				"$g_test_remote_batch_inventory_file" \
				"$g_test_remote_batch_inventory_error_file" \
				"$g_test_remote_batch_snapshot_file" \
				"$g_test_remote_batch_snapshot_error_file"
			printf 'status=%s\n' "$?"
			printf 'statuses=%s|%s|%s|%s\n' \
				"$g_zxfer_destination_discovery_batch_inventory_status" \
				"$g_zxfer_destination_discovery_batch_pool_status" \
				"$g_zxfer_destination_discovery_batch_snapshot_status" \
				"$g_zxfer_destination_discovery_batch_snapshot_ran"
			zxfer_test_print_remote_destination_batch_outputs
		)
	)

	assertContains "Valid batches should succeed." "$output" 'status=0'
	assertEquals "One remote batch should invoke ssh exactly once, on the target." \
		"host=target.example side=destination" "$(cat "$ssh_log")"
	assertContains "Valid batches should publish their statuses." \
		"$output" 'statuses=0||0|1'
	assertContains "Valid batches should replace the inventory output." \
		"$output" 'inventory=backup/dst
backup/dst/src'
	assertContains "An empty section should still empty its output file." \
		"$output" 'inventory_error=
'
	assertContains "Valid batches should replace the snapshot output." \
		"$output" 'snapshot=backup/dst/src@snapA	guid-a'
	assertContains "Valid batches should replace the snapshot stderr output." \
		"$output" 'snapshot_error=snapshot warning'
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
	assertContains "Truncated protocols must not leave the partially streamed snapshots behind." \
		"$output" 'snapshot=
'
	assertContains "Truncated protocols must empty every output." \
		"$output" 'inventory=
'

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
	assertContains "Reordered protocols must empty the inventory output." \
		"$output" 'inventory=
'
	assertContains "Reordered protocols must empty the snapshot stderr output." \
		"$output" 'snapshot_error='
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
			printf 'raw=%s\n' "$(cat "$g_zxfer_destination_snapshot_record_cache_file")"
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
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing command" >"$2"
				sh -c 'exit 37' &
				g_source_snapshot_list_pid=$!
				g_source_snapshot_list_job_id=""
				g_source_snapshot_list_cmd="sh -c 'printf \"%s\\n\" \"missing command\" >&2; exit 37'"
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
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				sh -c 'exit 1' &
				g_source_snapshot_list_pid=$!
				g_source_snapshot_list_job_id=""
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
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

test_get_zfs_list_reports_source_stderr_readback_failures_after_background_failure() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing stderr capture" >"$2"
				sh -c 'exit 1' &
				g_source_snapshot_list_pid=$!
				g_source_snapshot_list_job_id=""
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
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

test_prepare_remote_destination_discovery_batch_fails_closed_after_a_returning_reporter() {
	set +e
	output=$(
		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			ZXFER_SSH_BATCH_MODE="bad
mode"
			zxfer_throw_error() {
				printf 'reported=%s status=%s\n' "$1" "${2:-1}"
				return 0
			}
			zxfer_prepare_remote_destination_discovery_batch_command "backup/dst/src"
			printf 'status=%s\n' "$?"
			printf 'command=<%s>\n' "$g_zxfer_remote_destination_discovery_command_result"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "The prepared-command test wrapper should finish after publishing the preserved status." \
		0 "$status"
	assertContains "Transport policy failures should pass their diagnostic through the reporter." \
		"$output" "reported=ZXFER_SSH_BATCH_MODE must be a single-line non-empty value. status=1"
	assertContains "A returning reporter should still fail the preparation." \
		"$output" "status=1"
	assertContains "A failed preparation should publish no remote command." \
		"$output" "command=<>"
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
