#!/bin/sh
#
# shunit2 tests for zxfer_runtime.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# zxfer_init_globals() now delegates reset to the owner helpers that live
# through the replication layer, so source the full runtime stack that defines
# those helpers.
zxfer_source_runtime_modules_through "zxfer_replication.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_runtime"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_BACKUP_DIR
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_runtime_artifact_state
	zxfer_reset_background_job_state
	zxfer_reset_cleanup_pid_tracking
	zxfer_reset_failure_context "unit"
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
}

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

test_refresh_backup_storage_root_rejects_relative_override() {
	zxfer_test_capture_subshell '
		ZXFER_BACKUP_DIR="relative-backups"
		zxfer_refresh_backup_storage_root
	'

	assertEquals "Relative ZXFER_BACKUP_DIR overrides should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative backup-root errors should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_BACKUP_DIR must be an absolute path"
}

test_get_temp_file_creates_unique_paths() {
	file_one=$(zxfer_get_temp_file)
	file_two=$(zxfer_get_temp_file)

	assertNotEquals "Each temp-file request should return a unique path." \
		"$file_one" "$file_two"
	assertTrue "The first temp file should exist." '[ -f "$file_one" ]'
	assertTrue "The second temp file should exist." '[ -f "$file_two" ]'
}

test_zxfer_create_temp_file_group_publishes_requested_paths() {
	group_output_file="$TEST_TMPDIR/runtime-temp-group.out"

	zxfer_create_temp_file_group 3 >"$group_output_file"
	group_status=$?
	group_count=$(printf '%s\n' "$g_zxfer_temp_file_group_result" | awk 'NF {count++} END {print count + 0}')
	missing_count=0
	while IFS= read -r group_path || [ -n "$group_path" ]; do
		[ -n "$group_path" ] || continue
		if [ ! -f "$group_path" ]; then
			missing_count=$((missing_count + 1))
		fi
	done <<EOF
$g_zxfer_temp_file_group_result
EOF

	assertEquals "Temp-file group allocation should succeed for a valid count." \
		0 "$group_status"
	assertEquals "Temp-file group allocation should publish one path per requested file." \
		3 "$group_count"
	assertEquals "Temp-file group allocation should track the number of allocated files." \
		3 "$g_zxfer_temp_file_group_allocated_count"
	assertEquals "Temp-file group allocation should print the same newline-delimited paths it stores." \
		"$g_zxfer_temp_file_group_result" "$(cat "$group_output_file")"
	assertEquals "Temp-file group allocation should create every published file." \
		0 "$missing_count"
}

test_zxfer_create_temp_file_group_cleans_partial_allocations_on_failure() {
	first_path="$TEST_TMPDIR/runtime-temp-group-partial-one"
	second_path="$TEST_TMPDIR/runtime-temp-group-partial-two"
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result=$first_path
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			g_zxfer_temp_file_result=$second_path
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		esac
		return 73
	}

	set +e
	zxfer_create_temp_file_group 4 >/dev/null
	group_status=$?
	allocated_count=$g_zxfer_temp_file_group_allocated_count
	group_result=$g_zxfer_temp_file_group_result
	if [ -e "$first_path" ]; then
		first_exists=yes
	else
		first_exists=no
	fi
	if [ -e "$second_path" ]; then
		second_exists=yes
	else
		second_exists=no
	fi

	unset -f zxfer_get_temp_file
	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Temp-file group allocation should preserve the failed allocation status." \
		73 "$group_status"
	assertEquals "Temp-file group allocation should report how many files were allocated before failure." \
		2 "$allocated_count"
	assertEquals "Temp-file group allocation should not publish a complete group on failure." \
		"" "$group_result"
	assertFalse "Temp-file group allocation should clean the first partial file on failure." \
		"[ \"$first_exists\" = yes ]"
	assertFalse "Temp-file group allocation should clean the second partial file on failure." \
		"[ \"$second_exists\" = yes ]"
}

test_zxfer_create_temp_file_group_rejects_invalid_counts() {
	g_zxfer_temp_file_group_result="stale-group"
	g_zxfer_temp_file_group_allocated_count=9

	set +e
	zxfer_create_temp_file_group 0 >/dev/null
	group_status=$?

	assertEquals "Temp-file group allocation should reject zero as an invalid group size." \
		1 "$group_status"
	assertEquals "Temp-file group allocation should clear stale group results on invalid input." \
		"" "$g_zxfer_temp_file_group_result"
	assertEquals "Temp-file group allocation should reset the allocated count on invalid input." \
		0 "$g_zxfer_temp_file_group_allocated_count"
}

test_zxfer_cleanup_pid_helpers_cover_current_shell_paths() {
	sleep 30 &
	first_pid=$!
	sleep 30 &
	second_pid=$!

	output=$(
		(
			zxfer_register_cleanup_pid ""
			zxfer_register_cleanup_pid "$first_pid" "unit cleanup helper"
			zxfer_register_cleanup_pid "$second_pid" "unit cleanup helper"
			zxfer_register_cleanup_pid "$second_pid" "unit cleanup helper"
			printf 'registered=<%s>\n' "$g_zxfer_cleanup_pids"

			zxfer_unregister_cleanup_pid "$first_pid"
			printf 'after_unregister=<%s>\n' "$g_zxfer_cleanup_pids"

			zxfer_register_cleanup_pid "$$" "current shell"
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s\n' "$1"
				zxfer_unregister_cleanup_pid "$1"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'after_kill=<%s>\n' "$g_zxfer_cleanup_pids"
		)
	)

	kill -s TERM "$first_pid" >/dev/null 2>&1 || true
	kill -s TERM "$second_pid" >/dev/null 2>&1 || true
	wait "$first_pid" 2>/dev/null || true
	wait "$second_pid" 2>/dev/null || true

	assertContains "Cleanup PID registration should keep unique live helper PIDs." \
		"$output" "registered=<$first_pid $second_pid>"
	assertContains "Cleanup PID unregistration should remove only the requested helper PID." \
		"$output" "after_unregister=<$second_pid>"
	assertContains "Cleanup PID teardown should delegate teardown for the remaining helper PID." \
		"$output" "abort:$second_pid"
	assertContains "Cleanup PID teardown should clear the registered helper PID list after delegated teardown." \
		"$output" "after_kill=<>"
}

test_zxfer_register_cleanup_pid_tracks_direct_children_without_identity_captures() {
	sleep 30 &
	tracked_pid=$!

	zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
	register_status=$?
	zxfer_find_cleanup_pid_record "$tracked_pid"
	find_status=$?

	kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
	wait "$tracked_pid" 2>/dev/null || true

	assertEquals "Registering a live direct child should succeed." 0 "$register_status"
	assertEquals "Registered helpers should be findable by PID." 0 "$find_status"
	assertEquals "Registered rows should carry only the PID and purpose." \
		"$tracked_pid	unit cleanup helper" "$g_zxfer_cleanup_pid_records"
	assertEquals "Record lookups should publish the stored purpose." \
		"unit cleanup helper" "$g_zxfer_cleanup_pid_record_purpose"
}

test_zxfer_register_cleanup_pid_rejects_invalid_self_and_dead_pids() {
	zxfer_register_cleanup_pid "not-a-pid" "unit cleanup helper"
	invalid_status=$?
	zxfer_register_cleanup_pid "$$" "current shell"
	self_status=$?
	sh -c 'exit 0' &
	dead_pid=$!
	wait "$dead_pid" 2>/dev/null
	zxfer_register_cleanup_pid "$dead_pid" "already exited helper"
	dead_status=$?

	assertEquals "Non-numeric PIDs should be ignored without error." 0 "$invalid_status"
	assertEquals "The current shell PID should be ignored without error." 0 "$self_status"
	assertEquals "Already-exited helpers should be ignored without error." 0 "$dead_status"
	assertEquals "No registry rows should exist after rejected registrations." \
		"" "$g_zxfer_cleanup_pid_records"
	assertEquals "No tracked PIDs should exist after rejected registrations." \
		"" "$g_zxfer_cleanup_pids"
}

test_zxfer_register_cleanup_pid_fails_closed_when_purpose_normalization_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		tracked_pid=$!
		zxfer_normalize_owned_lock_text_field() {
			return 1
		}
		zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
		printf "status=%s\n" "$?"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
		wait "$tracked_pid" 2>/dev/null || true
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Registration should fail closed when purpose normalization fails." \
		"$output" "status=1"
	assertContains "Failed registrations should not leave registry rows behind." \
		"$output" "records=<>"
}

test_zxfer_abort_cleanup_pid_signals_and_unregisters_live_tracked_children() {
	sleep 30 &
	tracked_pid=$!

	zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
	zxfer_abort_cleanup_pid "$tracked_pid" TERM
	abort_status=$?
	wait "$tracked_pid" 2>/dev/null
	reaped_status=$?

	assertEquals "Aborting a live tracked helper should succeed." 0 "$abort_status"
	assertEquals "Aborting should leave no failure message." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
	assertEquals "Aborting should remove the registry row." "" "$g_zxfer_cleanup_pid_records"
	assertEquals "Aborting should remove the tracked PID." "" "$g_zxfer_cleanup_pids"
	assertEquals "The aborted helper should have died from the TERM signal." \
		143 "$reaped_status"
}

test_zxfer_abort_cleanup_pid_handles_untracked_and_already_exited_helpers() {
	zxfer_abort_cleanup_pid 99999 TERM
	untracked_status=$?

	sh -c 'exit 0' &
	dead_pid=$!
	g_zxfer_cleanup_pid_records="$dead_pid	already exited helper"
	g_zxfer_cleanup_pids=$dead_pid
	wait "$dead_pid" 2>/dev/null
	zxfer_abort_cleanup_pid "$dead_pid" TERM
	dead_status=$?

	assertEquals "Aborting an untracked PID should be a no-op success." 0 "$untracked_status"
	assertEquals "Aborting a tracked helper that already exited should succeed." 0 "$dead_status"
	assertEquals "Already-exited helpers should be unregistered during abort." \
		"" "$g_zxfer_cleanup_pid_records"
	assertEquals "Already-exited helpers should leave no failure message." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
}

test_zxfer_abort_cleanup_pid_fails_closed_when_signalling_a_live_helper_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		tracked_pid=$!
		zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
		kill() {
			case "$2" in
			0)
				command kill -0 "$3" 2>/dev/null
				return $?
				;;
			esac
			return 1
		}
		zxfer_abort_cleanup_pid "$tracked_pid" TERM
		printf "status=%s\n" "$?"
		printf "message=%s\n" "$g_zxfer_cleanup_pid_abort_failure_message"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
		unset -f kill
		kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
		wait "$tracked_pid" 2>/dev/null || true
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Aborting should fail closed when the signal cannot be delivered to a live helper." \
		"$output" "status=1"
	assertContains "Failed aborts should explain which helper could not be signalled." \
		"$output" "message=Failed to signal cleanup helper [unit cleanup helper] (PID "
	assertNotContains "Failed aborts should preserve the registry row for a later retry." \
		"$output" "records=<>"
}

test_zxfer_abort_direct_child_pid_signals_unreaped_direct_children() {
	sleep 30 &
	child_pid=$!

	zxfer_abort_direct_child_pid "$child_pid" TERM "unit direct helper"
	abort_status=$?
	wait "$child_pid" 2>/dev/null
	reaped_status=$?

	assertEquals "Signalling a live direct child should succeed." 0 "$abort_status"
	assertEquals "Signalling should leave no failure message." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
	assertEquals "The signalled child should have died from the TERM signal." \
		143 "$reaped_status"
}

test_zxfer_abort_direct_child_pid_rejects_invalid_self_and_dead_pids() {
	zxfer_abort_direct_child_pid "" TERM "unit direct helper"
	empty_status=$?
	zxfer_abort_direct_child_pid "not-a-pid" TERM "unit direct helper"
	invalid_status=$?
	zxfer_abort_direct_child_pid "$$" TERM "unit direct helper"
	self_status=$?
	sh -c 'exit 0' &
	dead_pid=$!
	wait "$dead_pid" 2>/dev/null
	zxfer_abort_direct_child_pid "$dead_pid" TERM "unit direct helper"
	dead_status=$?

	assertEquals "Empty PIDs should be a no-op success." 0 "$empty_status"
	assertEquals "Non-numeric PIDs should be a no-op success." 0 "$invalid_status"
	assertEquals "The current shell PID must be refused." 1 "$self_status"
	assertEquals "Already-exited children should be a no-op success." 0 "$dead_status"
}

test_zxfer_abort_direct_child_pid_fails_closed_when_purpose_normalization_fails() {
	zxfer_test_capture_subshell '
		sleep 30 &
		child_pid=$!
		zxfer_normalize_owned_lock_text_field() {
			return 1
		}
		zxfer_abort_direct_child_pid "$child_pid" TERM "unit direct helper"
		printf "status=%s\n" "$?"
		kill -s TERM "$child_pid" >/dev/null 2>&1 || true
		wait "$child_pid" 2>/dev/null || true
	'

	assertContains "Direct-child aborts should fail closed when purpose normalization fails." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
}

test_zxfer_kill_registered_cleanup_pids_preserves_first_failure_message_and_rebuilds_tracked_pids() {
	zxfer_test_capture_subshell '
		set +e
		g_zxfer_cleanup_pids="401 402"
		g_zxfer_cleanup_pid_records="401	first helper
402	second helper"
		zxfer_abort_cleanup_pid() {
			if [ "$1" = "401" ]; then
				g_zxfer_cleanup_pid_abort_failure_message="first cleanup abort failed"
				return 1
			fi
			return 0
		}

		zxfer_kill_registered_cleanup_pids
		printf "status=%s\n" "$?"
		printf "message=%s\n" "$g_zxfer_cleanup_pid_abort_failure_message"
		printf "remaining=<%s>\n" "$g_zxfer_cleanup_pids"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Cleanup-helper shutdown should preserve the first abort failure status." \
		"$output" "status=1"
	assertContains "Cleanup-helper shutdown should preserve the first abort failure message." \
		"$output" "message=first cleanup abort failed"
	assertContains "Cleanup-helper shutdown should rebuild the tracked pid list from the remaining records after a failed aggregate pass." \
		"$output" "remaining=<401 402>"
}

test_runtime_init_default_helpers_cover_current_shell_paths() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}

			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress_safe="gzip"
			g_cmd_decompress_safe="gunzip"

			zxfer_init_runtime_metadata
			zxfer_init_option_defaults
			zxfer_init_transport_remote_defaults
			zxfer_init_runtime_state_defaults
			zxfer_init_temp_artifacts

			printf 'version=%s\n' "$g_zxfer_version"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'origin_caps=<%s>\n' "$g_origin_remote_capabilities_response"
			printf 'control_sockets=%s\n' "$g_ssh_supports_control_sockets"
			printf 'local_zfs=%s\n' "$g_LZFS"
			printf 'backup_root=%s\n' "$g_backup_storage_root"
			printf 'backup_ext=%s\n' "$g_backup_file_extension"
			printf 'delete_source=<%s>\n' "$g_delete_source_tmp_file"
			printf 'temp_prefix=%s\n' "$g_zxfer_temp_prefix"
		)
	)

	assertContains "Runtime metadata initialization should set the current zxfer version string." \
		"$output" "version=2.0.0-20260611"
	assertContains "Option default initialization should restore the single-job default." \
		"$output" "jobs=1"
	assertContains "Transport runtime defaults should clear cached remote capability payloads." \
		"$output" "origin_caps=<>"
	assertContains "Transport runtime defaults should publish the ssh control-socket support marker in current-shell state." \
		"$output" "control_sockets="
	assertContains "Transport runtime defaults should seed the local zfs helpers from the base zfs path." \
		"$output" "local_zfs=/sbin/zfs"
	assertContains "Runtime state defaults should restore the default backup metadata root." \
		"$output" "backup_root=/var/db/zxfer"
	assertContains "Runtime state defaults should restore the secure backup-file suffix." \
		"$output" "backup_ext=.zxfer_backup_info"
	assertContains "Temporary artifact initialization should leave delete-planning scratch paths unset until needed." \
		"$output" "delete_source=<>"
	assertContains "Temporary artifact initialization should publish the current run temp prefix." \
		"$output" "temp_prefix=zxfer."
}

test_zxfer_init_globals_applies_secure_path_after_reset_helpers() {
	output=$(
		(
			reset_replication_calls=0
			zxfer_refresh_secure_path_state() {
				printf '%s\n' "refresh"
			}
			zxfer_reset_replication_runtime_state() {
				reset_replication_calls=$((reset_replication_calls + 1))
			}
			zxfer_init_dependency_tool_defaults() {
				printf '%s\n' "deps"
			}
			zxfer_init_transport_remote_defaults() {
				printf '%s\n' "transport"
			}
			zxfer_init_temp_artifacts() {
				printf '%s\n' "temp"
			}
			zxfer_apply_secure_path() {
				g_zxfer_runtime_path="/secure/path"
				printf '%s\n' "apply"
			}
			zxfer_init_globals
			printf 'runtime=<%s>\n' "${g_zxfer_runtime_path:-}"
			printf 'replication_resets=%s\n' "$reset_replication_calls"
		)
	)

	assertContains "Global runtime initialization should refresh the secure-path state before rebuilding defaults." \
		"$output" "refresh"
	assertContains "Global runtime initialization should reset replication state through the public helper when it is available." \
		"$output" "replication_resets=1"
	assertContains "Global runtime initialization should still run the dependency, transport, and temp default helpers." \
		"$output" "deps"
	assertContains "Global runtime initialization should still reapply the secure runtime PATH after rebuilding defaults." \
		"$output" "apply"
	assertContains "Global runtime initialization should leave the secure runtime PATH published in current-shell state." \
		"$output" "runtime=</secure/path>"
}

test_runtime_execution_context_init_helpers_cover_local_and_dry_run_remote_paths() {
	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$1"
			}
			zxfer_get_os() {
				if [ -n "$1" ]; then
					printf '%s\n' "RemoteOS"
				else
					printf '%s\n' "LocalOS"
				fi
			}
			zxfer_assign_required_tool() {
				eval "$1='/usr/bin/$2'"
			}
			zxfer_quote_cli_tokens() {
				printf 'quoted<%s>\n' "$1"
			}

			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="local-compress"
			g_cmd_decompress_safe="local-decompress"
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			g_origin_cmd_zfs=""
			g_target_cmd_zfs=""
			g_cmd_cat=""

			zxfer_init_transfer_command_context
			printf 'transfer_origin=%s\n' "$g_origin_cmd_compress_safe"
			printf 'transfer_target=%s\n' "$g_target_cmd_decompress_safe"

			g_option_e_restore_property_mode=1
			g_option_O_origin_host=""
			zxfer_init_restore_property_helpers
			printf 'local_cat=%s\n' "$g_cmd_cat"

			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_option_n_dryrun=1
			g_option_z_compress=1
			g_cmd_cat=""
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			zxfer_init_source_execution_context
			zxfer_init_destination_execution_context
			zxfer_init_restore_property_helpers

			printf 'source_os=<%s>\n' "$g_source_operating_system"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
			printf 'origin_compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'dest_os=<%s>\n' "$g_destination_operating_system"
			printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
			printf 'target_decompress=%s\n' "$g_target_cmd_decompress_safe"
			printf 'remote_cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Transfer command context initialization should copy the local compression helper to the origin transport defaults." \
		"$output" "transfer_origin=local-compress"
	assertContains "Transfer command context initialization should copy the local decompression helper to the target transport defaults." \
		"$output" "transfer_target=local-decompress"
	assertContains "Restore-helper initialization should resolve the local cat helper when restore mode is enabled without an origin host." \
		"$output" "local_cat=/usr/bin/cat"
	assertContains "Dry-run remote source initialization should skip live OS probing and leave the cached source OS blank." \
		"$output" "source_os=<>"
	assertContains "Dry-run remote source initialization should still seed the origin zfs helper from the local zfs path." \
		"$output" "origin_zfs=/sbin/zfs"
	assertContains "Dry-run remote source initialization should quote the remote compression command when compression is enabled." \
		"$output" "origin_compress=quoted<zstd -3>"
	assertContains "Dry-run remote destination initialization should skip live OS probing and leave the cached destination OS blank." \
		"$output" "dest_os=<>"
	assertContains "Dry-run remote destination initialization should still seed the target zfs helper from the local zfs path." \
		"$output" "target_zfs=/sbin/zfs"
	assertContains "Dry-run remote destination initialization should quote the remote decompression command when compression is enabled." \
		"$output" "target_decompress=quoted<zstd -d>"
	assertContains "Dry-run remote restore-helper initialization should fall back to a literal cat helper." \
		"$output" "remote_cat=cat"
}

test_runtime_artifact_allocators_use_the_per_run_temp_root_for_files_and_dirs() {
	zxfer_create_runtime_artifact_file "runtime-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-dir" >/dev/null
	dir_status=$?
	dir_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "Runtime artifact file allocation should succeed under the per-run temp root." \
		0 "$file_status"
	assertEquals "Runtime artifact directory allocation should succeed under the per-run temp root." \
		0 "$dir_status"
	assertNotEquals "Runtime artifact allocation should publish the per-run temp root." \
		"" "$g_zxfer_run_tmp_root"
	assertContains "The per-run temp root should live under the validated temp root." \
		"$g_zxfer_run_tmp_root" "$TEST_TMPDIR/"
	assertEquals "The per-run temp root should be private to the current user (0700)." \
		"700" "$(zxfer_get_path_mode_octal "$g_zxfer_run_tmp_root")"
	assertContains "Runtime artifact files should be allocated under the per-run temp root." \
		"$file_path" "$g_zxfer_run_tmp_root/"
	assertContains "Runtime artifact directories should be allocated under the per-run temp root." \
		"$dir_path" "$g_zxfer_run_tmp_root/"
	assertTrue "Runtime artifact file allocation should create the requested file." \
		"[ -f \"$file_path\" ]"
	assertEquals "Runtime artifact files should be created owner-only (0600)." \
		"600" "$(zxfer_get_path_mode_octal "$file_path")"
	assertTrue "Runtime artifact directory allocation should create the requested directory." \
		"[ -d \"$dir_path\" ]"
	assertEquals "Runtime artifact directories should be created owner-only (0700)." \
		"700" "$(zxfer_get_path_mode_octal "$dir_path")"
	assertEquals "Per-run-root allocations should not register per-file cleanup bookkeeping." \
		"" "${g_zxfer_runtime_artifact_cleanup_paths:-}"
}

test_runtime_artifact_allocators_skip_pre_seeded_counter_names_in_current_shell() {
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	g_zxfer_run_tmp_counter=0
	: >"$g_zxfer_run_tmp_root/skip-file.1"
	zxfer_create_runtime_artifact_file "skip-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	mkdir -m 700 "$g_zxfer_run_tmp_root/skip-dir.3"
	zxfer_create_runtime_artifact_dir "skip-dir" >/dev/null
	dir_status=$?
	dir_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "File allocation should succeed after skipping a taken counter name." \
		0 "$file_status"
	assertEquals "File allocation should advance past the taken counter name." \
		"$g_zxfer_run_tmp_root/skip-file.2" "$file_path"
	assertEquals "Directory allocation should succeed after skipping a taken counter name." \
		0 "$dir_status"
	assertEquals "Directory allocation should advance past the taken counter name." \
		"$g_zxfer_run_tmp_root/skip-dir.4" "$dir_path"
}

test_runtime_in_parent_allocators_use_unpredictable_mktemp_names() {
	# The allocators publish paths under the validated physical parent.
	# Staging parents may be shared sticky directories, so the names must be
	# mktemp-randomized: predictable pid+attempt slots are squat-able by a
	# local process-table reader.
	parent_dir=$(cd -P "$TEST_TMPDIR" && pwd)/runtime-parent-staging
	mkdir -p "$parent_dir"

	zxfer_create_runtime_artifact_file_in_parent "$parent_dir" "stage-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_file_in_parent "$parent_dir" "stage-file" >/dev/null
	second_file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_cache_object_stage_dir_in_parent "$parent_dir" "stage-dir" >/dev/null
	stage_status=$?
	stage_path=$g_zxfer_runtime_artifact_path_result

	case "${file_path##*/}" in
	"stage-file.$$."*)
		file_name_randomized=no
		;;
	stage-file.??????)
		file_name_randomized=yes
		;;
	*)
		file_name_randomized=no
		;;
	esac
	case "${stage_path##*/}" in
	".stage-dir.$$."*)
		stage_name_randomized=no
		;;
	.stage-dir.??????)
		stage_name_randomized=yes
		;;
	*)
		stage_name_randomized=no
		;;
	esac

	assertEquals "Path-adjacent file staging should succeed under a validated parent." \
		0 "$file_status"
	assertEquals "Path-adjacent file staging should use the randomized mktemp template, not pid+attempt slots." \
		yes "$file_name_randomized"
	assertTrue "Path-adjacent file staging should create the staged file." \
		"[ -f \"$file_path\" ]"
	assertNotEquals "Consecutive staged files should never reuse a name." \
		"$file_path" "$second_file_path"
	assertEquals "Cache-object stage dirs should succeed under a validated parent." \
		0 "$stage_status"
	assertEquals "Cache-object stage dirs should use the randomized mktemp template, not pid+attempt slots." \
		yes "$stage_name_randomized"
	assertTrue "Cache-object stage dirs should create the staged directory." \
		"[ -d \"$stage_path\" ]"
	assertContains "Cache-object stage dirs should register for trap cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$stage_path"
}

test_runtime_artifact_allocators_fail_closed_when_the_target_dir_is_unwritable() {
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	# Owner-only without write: passes safety validation, rejects creation.
	chmod 500 "$g_zxfer_run_tmp_root"
	zxfer_create_runtime_artifact_file "unwritable-file" >/dev/null 2>&1
	file_status=$?
	zxfer_create_runtime_artifact_dir "unwritable-dir" >/dev/null 2>&1
	dir_status=$?
	chmod 700 "$g_zxfer_run_tmp_root"

	parent_dir=$(cd -P "$TEST_TMPDIR" && pwd)/runtime-unwritable-parent
	mkdir -p "$parent_dir"
	chmod 500 "$parent_dir"
	zxfer_create_runtime_artifact_file_in_parent "$parent_dir" "unwritable" >/dev/null 2>&1
	parent_file_status=$?
	zxfer_create_cache_object_stage_dir_in_parent "$parent_dir" "unwritable" >/dev/null 2>&1
	stage_dir_status=$?
	chmod 700 "$parent_dir"

	assertEquals "File allocation should fail closed when the run root rejects writes." \
		1 "$file_status"
	assertEquals "Directory allocation should fail closed when the run root rejects writes." \
		1 "$dir_status"
	assertEquals "Path-adjacent file staging should fail closed when the parent rejects writes." \
		1 "$parent_file_status"
	assertEquals "Cache-object stage dirs should fail closed when the parent rejects writes." \
		1 "$stage_dir_status"
}

test_runtime_artifact_allocators_skip_taken_names_after_subshell_allocations() {
	# Allocate through command substitutions so the counter bumps never reach
	# this shell; the allocator must still hand out unique, existing paths.
	first_path=$(zxfer_get_temp_file)
	printf 'first payload\n' >"$first_path"
	second_path=$(zxfer_get_temp_file)

	assertNotEquals "Subshell allocations should never reuse a taken temp path." \
		"$first_path" "$second_path"
	assertEquals "Subshell allocations should never truncate earlier allocations." \
		"first payload" "$(cat "$first_path")"
	assertTrue "Subshell allocations should create the later temp file." \
		"[ -f \"$second_path\" ]"
}

test_runtime_artifact_file_allocator_in_parent_uses_validated_parent_and_registers_path() {
	parent_dir="$TEST_TMPDIR/runtime-parent"
	mkdir -p "$parent_dir"

	zxfer_create_runtime_artifact_file_in_parent "$parent_dir" "runtime-parent-file" >/dev/null
	status=$?
	file_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "Parent-scoped runtime artifact allocation should succeed for validated directories." \
		0 "$status"
	assertContains "Parent-scoped runtime artifact allocation should create files in the requested directory." \
		"$file_path" "$parent_dir/"
	assertTrue "Parent-scoped runtime artifact allocation should create the requested file." \
		"[ -f \"$file_path\" ]"
	assertContains "Parent-scoped runtime artifact allocation should register the file for cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
}

test_zxfer_reset_runtime_artifact_state_cleans_registered_artifacts() {
	zxfer_create_runtime_artifact_file "runtime-reset-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-reset-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result

	zxfer_reset_runtime_artifact_state

	assertFalse "Resetting runtime artifact state should remove registered runtime files." \
		"[ -e \"$file_path\" ]"
	assertFalse "Resetting runtime artifact state should remove registered runtime directories." \
		"[ -e \"$dir_path\" ]"
	assertEquals "Resetting runtime artifact state should clear the registered cleanup path list." \
		"" "$g_zxfer_runtime_artifact_cleanup_paths"
	assertEquals "Resetting runtime artifact state should clear the shared path scratch result." \
		"" "$g_zxfer_runtime_artifact_path_result"
	assertEquals "Resetting runtime artifact state should clear the shared readback scratch result." \
		"" "$g_zxfer_runtime_artifact_read_result"
}

test_zxfer_reset_runtime_artifact_state_preserves_failed_cleanup_registrations() {
	artifact_path="$TEST_TMPDIR/runtime-reset-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			zxfer_ensure_run_tmp_root || exit 90
			g_zxfer_runtime_artifact_path_result="stale-path"
			g_zxfer_runtime_artifact_read_result="stale-read"
			rm() {
				return 1
			}
			zxfer_reset_runtime_artifact_state
			status=$?
			printf 'status=%s\n' "$status"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
			printf 'path_result=<%s>\n' "$g_zxfer_runtime_artifact_path_result"
			printf 'read_result=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'root_retained=<%s>\n' "$([ -n "$g_zxfer_run_tmp_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Resetting runtime artifact state should preserve cleanup failures." \
		"$output" "status=1"
	assertContains "Resetting runtime artifact state should keep undeleted artifacts registered for later cleanup." \
		"$output" "registered=<$artifact_path>"
	assertContains "Resetting runtime artifact state should still clear the shared path scratch result after cleanup failures." \
		"$output" "path_result=<>"
	assertContains "Resetting runtime artifact state should still clear the shared readback scratch result after cleanup failures." \
		"$output" "read_result=<>"
	assertTrue "Resetting runtime artifact state should leave undeleted artifacts in place when cleanup fails." \
		"[ -e \"$artifact_path\" ]"
	assertContains "Resetting runtime artifact state should keep the undeleted run root tracked for later cleanup." \
		"$output" "root_retained=<yes>"
}

test_zxfer_trap_exit_cleans_registered_runtime_artifacts() {
	registered_file="$TEST_TMPDIR/registered-runtime-file"
	registered_dir="$TEST_TMPDIR/registered-runtime-dir"
	: >"$registered_file"
	mkdir -p "$registered_dir/subdir"
	: >"$registered_dir/subdir/payload"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_register_runtime_artifact_path "$registered_dir"
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success after removing registered runtime artifacts." \
		0 "$status"
	assertEquals "zxfer_trap_exit should keep stdout clean while removing registered runtime artifacts." \
		"" "$output"
	assertFalse "zxfer_trap_exit should remove registered runtime files." \
		"[ -e \"$registered_file\" ]"
	assertFalse "zxfer_trap_exit should remove registered runtime directories." \
		"[ -e \"$registered_dir\" ]"
}

test_zxfer_trap_exit_removes_the_per_run_temp_root_on_success_and_failure_paths() {
	success_output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_get_temp_file >/dev/null
			printf 'root=%s\n' "$g_zxfer_run_tmp_root" >&2
			true
			zxfer_trap_exit
		) 2>&1
	)
	success_status=$?
	success_root=${success_output#root=}

	assertEquals "zxfer_trap_exit should preserve success after removing the per-run temp root." \
		0 "$success_status"
	assertNotEquals "The per-run temp root should exist before the trap runs." \
		"" "$success_root"
	assertFalse "zxfer_trap_exit should remove the per-run temp root and everything below it." \
		"[ -e \"$success_root\" ]"

	failure_root_file="$TEST_TMPDIR/trap-failure-root.path"
	(
		zxfer_close_all_ssh_control_sockets() {
			:
		}
		zxfer_echoV() {
			:
		}
		zxfer_get_temp_file >/dev/null
		printf '%s\n' "$g_zxfer_run_tmp_root" >"$failure_root_file"
		false
		zxfer_trap_exit
	) 2>/dev/null
	failure_status=$?
	failure_root=$(cat "$failure_root_file")

	assertEquals "zxfer_trap_exit should preserve the failing exit status while removing the per-run temp root." \
		1 "$failure_status"
	assertFalse "zxfer_trap_exit should remove the per-run temp root on failure paths too." \
		"[ -e \"$failure_root\" ]"
}

test_zxfer_trap_exit_surfaces_failed_run_tmp_root_removal_as_trap_cleanup_failure() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			zxfer_get_temp_file >/dev/null
			rm() {
				return 1
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when the per-run temp root cannot be removed." \
		1 "$status"
	assertContains "Failed run-root removal should surface as a runtime trap-cleanup failure." \
		"$output" "class=runtime"
	assertContains "Failed run-root removal should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Failed run-root removal should report the runtime temp-artifact cleanup message." \
		"$output" "message=Failed to remove one or more runtime temp artifacts during exit."
}

test_run_tmp_root_is_removed_when_the_process_is_terminated_mid_run() {
	leak_tmpdir="$TEST_TMPDIR/sigterm-leak-tmp"
	ready_flag="$TEST_TMPDIR/sigterm-child.ready"
	child_script="$TEST_TMPDIR/sigterm-child.sh"
	child_stderr="$TEST_TMPDIR/sigterm-child.stderr"
	rm -rf "$leak_tmpdir" "$ready_flag"
	mkdir -p "$leak_tmpdir"

	cat >"$child_script" <<EOF
#!/bin/sh
ZXFER_SOURCE_MODULES_ROOT="$ZXFER_ROOT" \\
	ZXFER_SOURCE_MODULES_THROUGH=zxfer_replication.sh \\
	. "$ZXFER_ROOT/src/zxfer_modules.sh"
TMPDIR="$leak_tmpdir"
export TMPDIR
zxfer_reset_failure_context "sigterm-test"
zxfer_reset_runtime_artifact_state
zxfer_reset_background_job_state
zxfer_reset_cleanup_pid_tracking
zxfer_reset_owned_lock_tracking
g_option_Y_yield_iterations=1
zxfer_register_runtime_traps
zxfer_get_temp_file >/dev/null
: >"$ready_flag"
# An interruptible wait so the TERM trap runs promptly.
sleep 30 &
wait \$!
EOF

	/bin/sh "$child_script" >/dev/null 2>"$child_stderr" &
	child_pid=$!
	wait_count=0
	while [ ! -e "$ready_flag" ] && [ "$wait_count" -lt 100 ]; do
		wait_count=$((wait_count + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	assertTrue "The SIGTERM fixture child should reach its ready state." \
		"[ -e \"$ready_flag\" ]"
	assertNotEquals "The SIGTERM fixture child should allocate under the per-run temp root before the signal." \
		"" "$(ls -A "$leak_tmpdir")"

	kill -s TERM "$child_pid" 2>/dev/null
	wait "$child_pid" 2>/dev/null

	assertEquals "A SIGTERM mid-run must not leak any temp state into TMPDIR." \
		"" "$(ls -A "$leak_tmpdir")"
}

test_zxfer_trap_exit_aborts_supervised_background_jobs_before_legacy_pid_cleanup() {
	cleanup_log="$TEST_TMPDIR/trap_supervisor_cleanup.log"
	: >"$cleanup_log"

	output=$(
		(
			CLEANUP_LOG="$cleanup_log"
			zxfer_abort_all_background_jobs() {
				printf '%s\n' "abort" >>"$CLEANUP_LOG"
			}
			zxfer_kill_registered_cleanup_pids() {
				printf '%s\n' "legacy" >>"$CLEANUP_LOG"
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when supervised background cleanup succeeds." \
		0 "$status"
	assertEquals "zxfer_trap_exit should run supervised background cleanup before legacy bare-PID cleanup." \
		"abort
legacy" "$(cat "$cleanup_log")"
	assertEquals "zxfer_trap_exit should keep stdout clean when cleanup succeeds." \
		"" "$output"
}

test_zxfer_trap_exit_fails_closed_when_supervised_background_cleanup_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_abort_all_background_jobs() {
				g_zxfer_background_job_abort_failure_message="validated abort failed"
				return 17
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve supervised background cleanup failure status." \
		17 "$status"
	assertContains "Supervised background cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Supervised background cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Supervised background cleanup failures should preserve the validated abort failure message." \
		"$output" "message=validated abort failed"
}

test_zxfer_trap_exit_fails_closed_when_validated_cleanup_helper_abort_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_kill_registered_cleanup_pids() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup helper abort failed"
				return 23
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve validated cleanup-helper teardown failure status." \
		23 "$status"
	assertContains "Validated cleanup-helper teardown failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Validated cleanup-helper teardown failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Validated cleanup-helper teardown failures should preserve the validated abort failure message." \
		"$output" "message=validated cleanup helper abort failed"
}

test_zxfer_trap_exit_fails_closed_when_ssh_socket_cleanup_fails_after_success() {
	registered_file="$TEST_TMPDIR/trap-close-failure-artifact"
	: >"$registered_file"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_close_all_ssh_control_sockets() {
				printf '%s\n' "close failed" >&2
				return 19
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when ssh socket cleanup fails after an otherwise successful run." \
		19 "$status"
	assertContains "zxfer_trap_exit should preserve ssh socket cleanup diagnostics on stderr." \
		"$output" "close failed"
	assertContains "ssh socket cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "ssh socket cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "ssh socket cleanup failures should preserve the cleanup-specific failure message." \
		"$output" "message=Failed to close one or more ssh control sockets during exit."
	assertFalse "zxfer_trap_exit should continue removing registered runtime artifacts after ssh socket cleanup failures." \
		"[ -e \"$registered_file\" ]"
}

test_zxfer_cleanup_runtime_artifact_path_preserves_registration_when_delete_fails() {
	artifact_path="$TEST_TMPDIR/runtime-cleanup-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			rm() {
				return 1
			}
			zxfer_cleanup_runtime_artifact_path "$artifact_path"
			status=$?
			printf 'status=%s\n' "$status"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
		)
	)

	assertContains "Runtime artifact cleanup should preserve failure when an artifact cannot be deleted." \
		"$output" "status=1"
	assertContains "Runtime artifact cleanup should keep undeleted artifacts registered for later cleanup." \
		"$output" "registered=<$artifact_path>"
	assertTrue "Runtime artifact cleanup failures should leave the undeleted artifact in place." \
		"[ -e \"$artifact_path\" ]"
}

test_zxfer_cleanup_runtime_artifact_paths_removes_and_unregisters_multiple_paths() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-cleanup-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result

	zxfer_cleanup_runtime_artifact_paths "$file_path" "$dir_path"
	cleanup_status=$?

	assertEquals "Multi-path runtime artifact cleanup should succeed when every registered path can be deleted." \
		0 "$cleanup_status"
	assertFalse "Multi-path runtime artifact cleanup should remove registered files." \
		"[ -e \"$file_path\" ]"
	assertFalse "Multi-path runtime artifact cleanup should remove registered directories." \
		"[ -e \"$dir_path\" ]"
	assertNotContains "Multi-path runtime artifact cleanup should unregister deleted files." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertNotContains "Multi-path runtime artifact cleanup should unregister deleted directories." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
}

test_zxfer_cleanup_runtime_artifact_paths_preserves_failures_when_one_path_cannot_be_removed() {
	output_file="$TEST_TMPDIR/runtime_cleanup_paths_failure.out"

	(
		zxfer_cleanup_runtime_artifact_path() {
			case "$1" in
			fail-path) return 1 ;;
			esac
			command printf 'cleaned=%s\n' "$1"
			return 0
		}
		set +e
		zxfer_cleanup_runtime_artifact_paths "fail-path" "ok-path"
		status=$?
		set -e
		command printf 'status=%s\n' "$status"
	) >"$output_file"
	output=$(cat "$output_file")

	assertContains "Multi-path runtime artifact cleanup should still attempt later paths after an earlier failure." \
		"$output" "cleaned=ok-path"
	assertContains "Multi-path runtime artifact cleanup should return failure when any one path cannot be removed." \
		"$output" "status=1"
}

test_zxfer_cleanup_runtime_artifact_path_list_removes_newline_delimited_paths() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-list-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-cleanup-list-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result
	path_list=$(printf '%s\n%s\n' "$file_path" "$dir_path")

	zxfer_cleanup_runtime_artifact_path_list "$path_list"
	cleanup_status=$?

	assertEquals "List-based runtime artifact cleanup should succeed when every listed artifact is removed." \
		0 "$cleanup_status"
	assertFalse "List-based runtime artifact cleanup should remove listed files." \
		"[ -e \"$file_path\" ]"
	assertFalse "List-based runtime artifact cleanup should remove listed directories." \
		"[ -e \"$dir_path\" ]"
	assertNotContains "List-based runtime artifact cleanup should unregister listed files." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertNotContains "List-based runtime artifact cleanup should unregister listed directories." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
}

test_zxfer_cleanup_runtime_artifact_path_list_and_return_preserves_original_status() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-list-return" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result

	zxfer_cleanup_runtime_artifact_path_list_and_return 37 "$file_path"
	cleanup_status=$?

	assertEquals "List cleanup return helper should preserve the caller's original status." \
		37 "$cleanup_status"
	assertFalse "List cleanup return helper should still remove listed artifacts." \
		"[ -e \"$file_path\" ]"
}

test_zxfer_write_and_read_runtime_artifact_file_preserve_multiline_payloads() {
	read_output_file="$TEST_TMPDIR/runtime-readback.out"
	zxfer_create_runtime_artifact_file "runtime-readback" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	payload=$(printf '%s\n' \
		"line one" \
		"line two")

	zxfer_write_runtime_artifact_file "$artifact_path" "$payload"
	write_status=$?
	zxfer_read_runtime_artifact_file "$artifact_path" >"$read_output_file"
	read_status=$?
	read_output=$(cat "$read_output_file")

	assertEquals "Runtime artifact writes should succeed for multiline payloads." \
		0 "$write_status"
	assertEquals "Runtime artifact reads should succeed for multiline payloads." \
		0 "$read_status"
	assertEquals "Runtime artifact reads should reproduce the exact multiline payload on stdout." \
		"$payload" "$read_output"
	assertEquals "Runtime artifact reads should publish the exact multiline payload in shared scratch state." \
		"$payload" "$g_zxfer_runtime_artifact_read_result"
}

test_zxfer_read_runtime_artifact_file_preserves_trailing_blank_lines_exactly() {
	read_output_file="$TEST_TMPDIR/runtime-readback-trailing.out"
	scratch_output_file="$TEST_TMPDIR/runtime-readback-trailing.scratch"
	expected_hex="6c696e65206f6e650a0a0a"
	zxfer_create_runtime_artifact_file "runtime-readback-trailing" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	printf 'line one\n\n\n' >"$artifact_path"

	zxfer_read_runtime_artifact_file "$artifact_path" >"$read_output_file"
	read_status=$?
	printf '%s' "$g_zxfer_runtime_artifact_read_result" >"$scratch_output_file"
	read_output_hex=$(od -An -tx1 -v "$read_output_file" | tr -d ' \n')
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Runtime artifact reads should preserve trailing blank lines on stdout." \
		0 "$read_status"
	assertEquals "Runtime artifact reads should preserve trailing blank lines in stdout payloads." \
		"$expected_hex" "$read_output_hex"
	assertEquals "Runtime artifact reads should preserve trailing blank lines in shared scratch state." \
		"$expected_hex" "$scratch_output_hex"
}

test_zxfer_read_runtime_artifact_file_preserves_nonzero_status_and_clears_scratch() {
	artifact_path="$TEST_TMPDIR/runtime-readback-failure"
	: >"$artifact_path"

	output=$(
		(
			g_zxfer_runtime_artifact_read_result="stale-runtime-readback"
			cat() {
				return 26
			}
			zxfer_read_runtime_artifact_file "$artifact_path" >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)

	assertContains "Runtime artifact readback failures should preserve the original nonzero status." \
		"$output" "status=26"
	assertContains "Runtime artifact readback failures should clear the shared readback scratch state." \
		"$output" "scratch=<>"
}

test_zxfer_read_runtime_artifact_file_trimmed_strips_one_trailing_newline_from_scratch() {
	read_output_file="$TEST_TMPDIR/runtime-readback-trimmed.out"
	scratch_output_file="$TEST_TMPDIR/runtime-readback-trimmed.scratch"
	expected_stdout_hex="6c696e65206f6e650a0a0a"
	expected_scratch_hex="6c696e65206f6e650a0a"
	zxfer_create_runtime_artifact_file "runtime-readback-trimmed" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	printf 'line one\n\n\n' >"$artifact_path"

	zxfer_read_runtime_artifact_file_trimmed "$artifact_path" >"$read_output_file"
	read_status=$?
	printf '%s' "$g_zxfer_runtime_artifact_read_result" >"$scratch_output_file"
	read_output_hex=$(od -An -tx1 -v "$read_output_file" | tr -d ' \n')
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Trimmed runtime artifact reads should preserve a successful status." \
		0 "$read_status"
	assertEquals "Trimmed runtime artifact reads should emit the trimmed payload plus the helper's output newline." \
		"$expected_stdout_hex" "$read_output_hex"
	assertEquals "Trimmed runtime artifact reads should strip exactly one trailing newline in shared scratch state." \
		"$expected_scratch_hex" "$scratch_output_hex"
}

test_zxfer_read_runtime_artifact_file_trimmed_preserves_read_failures() {
	artifact_path="$TEST_TMPDIR/runtime-readback-trimmed-failure"
	: >"$artifact_path"

	output=$(
		(
			g_zxfer_runtime_artifact_read_result="stale-runtime-readback"
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result=""
				return 29
			}
			zxfer_read_runtime_artifact_file_trimmed "$artifact_path" >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)

	assertContains "Trimmed runtime artifact reads should preserve lower-level read failures." \
		"$output" "status=29"
	assertContains "Trimmed runtime artifact reads should leave the lower-level failure scratch state intact." \
		"$output" "scratch=<>"
}

test_zxfer_capture_runtime_artifact_command_output_reads_and_cleans_capture_file() {
	capture_file="$TEST_TMPDIR/runtime-capture-command.out"

	output=$(
		(
			zxfer_create_runtime_artifact_file() {
				: >"$capture_file"
				g_zxfer_runtime_artifact_path_result=$capture_file
				return 0
			}
			zxfer_emit_runtime_capture_fixture() {
				printf '%s\n%s\n' "line one" "line two"
			}
			zxfer_capture_runtime_artifact_command_output "runtime-capture" zxfer_emit_runtime_capture_fixture
			printf 'status=%s\n' "$?"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Runtime command captures should preserve successful command output in readback scratch." \
		"$output" "scratch=<line one
line two>"
	assertContains "Runtime command captures should return success for successful commands and readbacks." \
		"$output" "status=0"
	assertContains "Runtime command captures should remove the temporary capture file after readback." \
		"$output" "exists=no"
}

test_zxfer_capture_runtime_artifact_command_output_preserves_command_and_read_failures() {
	capture_file="$TEST_TMPDIR/runtime-capture-command-failure.out"

	output=$(
		(
			zxfer_create_runtime_artifact_file() {
				: >"$capture_file"
				g_zxfer_runtime_artifact_path_result=$capture_file
				return 0
			}
			zxfer_failing_runtime_capture_fixture() {
				printf '%s\n' "partial"
				return 37
			}
			g_zxfer_runtime_artifact_read_result="stale"
			zxfer_capture_runtime_artifact_command_output "runtime-capture" zxfer_failing_runtime_capture_fixture
			printf 'command_status=%s\n' "$?"
			printf 'command_scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'command_exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
		(
			zxfer_create_runtime_artifact_file() {
				: >"$capture_file"
				g_zxfer_runtime_artifact_path_result=$capture_file
				return 0
			}
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result=""
				return 38
			}
			zxfer_capture_runtime_artifact_command_output "runtime-capture" printf '%s\n' "captured"
			printf 'read_status=%s\n' "$?"
			printf 'read_exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Runtime command captures should preserve command failures." \
		"$output" "command_status=37"
	assertContains "Runtime command captures should not publish partial command output after command failures." \
		"$output" "command_scratch=<>"
	assertContains "Runtime command captures should remove temp files after command failures." \
		"$output" "command_exists=no"
	assertContains "Runtime command captures should preserve readback failures." \
		"$output" "read_status=38"
	assertContains "Runtime command captures should remove temp files after readback failures." \
		"$output" "read_exists=no"
}

test_zxfer_capture_runtime_artifact_combined_command_output_preserves_status_after_readback() {
	capture_file="$TEST_TMPDIR/runtime-capture-combined-command.out"

	output=$(
		(
			zxfer_create_runtime_artifact_file() {
				: >"$capture_file"
				g_zxfer_runtime_artifact_path_result=$capture_file
				return 0
			}
			zxfer_failing_runtime_combined_capture_fixture() {
				printf '%s\n' "stdout-line"
				printf '%s\n' "stderr-line" >&2
				return 43
			}
			zxfer_capture_runtime_artifact_combined_command_output "runtime-combined" zxfer_failing_runtime_combined_capture_fixture
			printf 'status=%s\n' "$?"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Combined runtime command captures should preserve the command status after successful readback." \
		"$output" "status=43"
	assertContains "Combined runtime command captures should publish stdout and stderr after command failures." \
		"$output" "scratch=<stdout-line
stderr-line>"
	assertContains "Combined runtime command captures should remove temp files after readback." \
		"$output" "exists=no"
}

test_zxfer_capture_runtime_artifact_combined_command_output_preserves_readback_failures() {
	capture_file="$TEST_TMPDIR/runtime-capture-combined-read-failure.out"

	output=$(
		(
			zxfer_create_runtime_artifact_file() {
				: >"$capture_file"
				g_zxfer_runtime_artifact_path_result=$capture_file
				return 0
			}
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result=""
				return 44
			}
			zxfer_capture_runtime_artifact_combined_command_output "runtime-combined" printf '%s\n' "captured"
			printf 'status=%s\n' "$?"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
			printf 'exists=%s\n' "$([ -e "$capture_file" ] && printf yes || printf no)"
		)
	)

	assertContains "Combined runtime command captures should preserve readback failures." \
		"$output" "status=44"
	assertContains "Combined runtime command captures should leave failed readback scratch empty." \
		"$output" "scratch=<>"
	assertContains "Combined runtime command captures should remove temp files after readback failures." \
		"$output" "exists=no"
}

test_zxfer_write_runtime_artifact_file_creates_empty_files_without_caller_truncation() {
	artifact_path="$TEST_TMPDIR/runtime-empty-payload"

	zxfer_write_runtime_artifact_file "$artifact_path" ""
	write_status=$?

	assertEquals "Runtime artifact writes should succeed when asked to create an empty file." \
		0 "$write_status"
	assertTrue "Runtime artifact writes should create the destination file for empty payloads." \
		"[ -f \"$artifact_path\" ]"
	assertTrue "Runtime artifact writes should leave empty payload files at zero bytes." \
		"[ ! -s \"$artifact_path\" ]"
}

test_zxfer_write_runtime_artifact_file_suppresses_shell_redirection_stderr() {
	artifact_path="$TEST_TMPDIR/runtime-missing-parent/payload"

	output=$(
		(
			zxfer_write_runtime_artifact_file "$artifact_path" "payload"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertEquals "Runtime artifact write failures should stay silent so callers control the operator-facing error." \
		"status=1" "$output"
}

test_runtime_artifact_parent_and_stage_helpers_reject_invalid_parent_contexts() {
	set +e
	zxfer_create_runtime_artifact_file_in_parent "relative-parent" "runtime-parent-file" >/dev/null 2>&1
	parent_status=$?
	stage_output=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			zxfer_stage_runtime_artifact_file_for_path "$TEST_TMPDIR/runtime-target" >/dev/null 2>&1
			printf 'stage_status=%s\n' "$?"
		)
	)
	set -e

	assertEquals "Runtime artifact files staged in explicit parents should reject unvalidated parent directories." \
		1 "$parent_status"
	assertContains "Runtime artifact staging should preserve parent-directory lookup failures." \
		"$stage_output" "stage_status=1"
}

test_zxfer_write_runtime_artifact_file_preserves_non_redirection_failure_status() {
	artifact_path="$TEST_TMPDIR/runtime-nonredirection-failure"

	output=$(
		(
			printf() {
				return 7
			}
			set +e
			zxfer_write_runtime_artifact_file "$artifact_path" "payload"
			status=$?
			set -e
			command printf 'status=%s\n' "$status"
		)
	)

	assertContains "Runtime artifact writes should preserve non-redirection shell failures from the payload writer." \
		"$output" "status=7"
}

test_zxfer_write_cache_object_contents_to_path_rejects_symlinked_targets() {
	object_path="$TEST_TMPDIR/cache-object-open-failure"
	object_target_dir="$TEST_TMPDIR/cache-object-target-dir"
	mkdir -p "$object_target_dir" || fail "Unable to create the cache-object fixture directory."
	ln -s "$object_target_dir" "$object_path" || fail "Unable to create the cache-object redirection failure fixture."

	set +e
	zxfer_write_cache_object_contents_to_path "$object_path" "demo-kind" "" "payload" >/dev/null 2>&1
	object_write_status=$?
	set -e
	if [ -e "$object_path" ] && [ ! -L "$object_path" ]; then
		object_partial_exists=yes
	else
		object_partial_exists=no
	fi

	assertEquals "Cache-object content writes should fail closed when the destination path cannot be opened for writing." \
		1 "$object_write_status"
	assertEquals "Failed cache-object content writes should not leave a partially published target behind." \
		no "$object_partial_exists"
}

test_get_os_handles_local_and_remote_invocations() {
	assertEquals "A local zxfer_get_os call should match uname." \
		"$(uname)" "$(zxfer_get_os "")"

	remote_os=$(
		zxfer_get_remote_host_operating_system() {
			printf '%s\n' "RemoteOS"
		}
		zxfer_get_os "origin.example" source
	)

	assertEquals "A remote zxfer_get_os call should delegate to the remote helper." \
		"RemoteOS" "$remote_os"
}

test_init_globals_initializes_dependency_state_and_temp_files() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			g_zxfer_services_to_restart="stale-service"
			g_backup_file_contents="stale-backup"
			g_restored_backup_file_contents="stale-restore"
			g_zxfer_remote_capability_response_result="stale-caps"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_ssh_control_socket_action_result="stale-action"
			g_zxfer_ssh_control_socket_action_stderr="stale-stderr"
			g_recursive_source_list="stale-source"
			g_last_common_snap="stale@snap"
			g_zfs_send_job_pids="123 456"
			g_zxfer_background_job_records="stale-job	kind	111	/tmp/bg	/runner	token"
			g_zxfer_background_job_wait_job_id="stale-job"
			g_zxfer_property_table_lookup_result="stale-lookup"
			g_zxfer_source_pvs_raw="stale=property=local"
			g_zxfer_property_stage_file_read_result="stale-stage-read"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_init_globals
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'path=%s\n' "$PATH"
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'ps=%s\n' "$g_cmd_ps"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'tmp_source=%s\n' "$g_delete_source_tmp_file"
			printf 'tmp_dest=%s\n' "$g_delete_dest_tmp_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'backup=<%s>\n' "$g_backup_file_contents"
			printf 'restored=<%s>\n' "$g_restored_backup_file_contents"
			printf 'remote_caps=<%s>\n' "$g_zxfer_remote_capability_response_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'socket_action=<%s>\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'socket_stderr=<%s>\n' "$g_zxfer_ssh_control_socket_action_stderr"
			printf 'recursive=<%s>\n' "$g_recursive_source_list"
			printf 'last_common=<%s>\n' "$g_last_common_snap"
			printf 'send_pids=<%s>\n' "$g_zfs_send_job_pids"
			printf 'background_records=<%s>\n' "$g_zxfer_background_job_records"
			printf 'background_wait_job=<%s>\n' "$g_zxfer_background_job_wait_job_id"
			printf 'table_lookup=<%s>\n' "$g_zxfer_property_table_lookup_result"
			printf 'source_pvs=<%s>\n' "$g_zxfer_source_pvs_raw"
			printf 'property_stage_read=<%s>\n' "$g_zxfer_property_stage_file_read_result"
		)
	)

	assertContains "zxfer_init_globals should initialize the secure path." \
		"$output" "secure=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should export the strict runtime PATH once runtime startup begins." \
		"$output" "path=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should resolve the awk helper." \
		"$output" "awk=/usr/bin/awk"
	assertContains "zxfer_init_globals should resolve the ps helper for supervised background-job validation." \
		"$output" "ps=/usr/bin/ps"
	assertContains "zxfer_init_globals should record ssh control-socket support." \
		"$output" "control=1"
	assertContains "zxfer_init_globals should leave snapshot-delete temp paths empty until delete planning needs them." \
		"$output" "tmp_source="
	assertContains "zxfer_init_globals should leave the paired snapshot-delete temp path empty until delete planning needs it." \
		"$output" "tmp_dest="
	assertContains "zxfer_init_globals should reset orchestration restart scratch state." \
		"$output" "restart=<>"
	assertContains "zxfer_init_globals should reset backup-metadata accumulation state." \
		"$output" "backup=<>"
	assertContains "zxfer_init_globals should reset restored backup scratch state." \
		"$output" "restored=<>"
	assertContains "zxfer_init_globals should reset remote capability handshake scratch state." \
		"$output" "remote_caps=<>"
	assertContains "zxfer_init_globals should reset remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "zxfer_init_globals should reset ssh control-socket action classification state." \
		"$output" "socket_action=<>"
	assertContains "zxfer_init_globals should reset ssh control-socket action stderr scratch state." \
		"$output" "socket_stderr=<>"
	assertContains "zxfer_init_globals should reset snapshot-discovery scratch state." \
		"$output" "recursive=<>"
	assertContains "zxfer_init_globals should reset snapshot-reconcile scratch state." \
		"$output" "last_common=<>"
	assertContains "zxfer_init_globals should reset send/receive PID tracking state." \
		"$output" "send_pids=<>"
	assertContains "zxfer_init_globals should reset supervised background-job registry state." \
		"$output" "background_records=<>"
	assertContains "zxfer_init_globals should reset supervised background-job wait scratch state." \
		"$output" "background_wait_job=<>"
	assertContains "zxfer_init_globals should reset property-table lookup scratch state." \
		"$output" "table_lookup=<>"
	assertContains "zxfer_init_globals should reset property-reconcile source scratch state." \
		"$output" "source_pvs=<>"
	assertContains "zxfer_init_globals should reset staged property-file read scratch state." \
		"$output" "property_stage_read=<>"
}

test_init_globals_defers_strict_path_export_until_startup_helpers_finish() {
	secure_path_dir="$TEST_TMPDIR/narrow-secure-path"
	mkdir -p "$secure_path_dir"

	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_SECURE_PATH="$secure_path_dir"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_init_globals
			status=$?
			printf 'status=%s\n' "$status"
			printf 'path=%s\n' "$PATH"
			printf 'tmp_source=%s\n' "$g_delete_source_tmp_file"
		) 2>&1
	)

	assertContains "zxfer_init_globals should still finish startup when ZXFER_SECURE_PATH omits date/mktemp directories." \
		"$output" "status=0"
	assertContains "zxfer_init_globals should export the narrow secure PATH after startup completes." \
		"$output" "path=$secure_path_dir"
	assertContains "zxfer_init_globals should still finish startup before switching to the strict runtime PATH even when delete tempfiles are deferred." \
		"$output" "tmp_source="
	assertNotContains "Startup should not trip over missing bootstrap utilities when the strict PATH is applied at the end of init." \
		"$output" "command not found"
}

test_ensure_snapshot_delete_temp_artifacts_allocates_paths_lazily_in_current_shell() {
	output=$(
		(
			counter=0
			zxfer_reset_delete_temp_artifacts
			zxfer_get_temp_file() {
				counter=$((counter + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/delete.$counter"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only.$counter"
			}

			zxfer_ensure_snapshot_delete_temp_artifacts
			first_source=$g_delete_source_tmp_file
			first_dest=$g_delete_dest_tmp_file
			first_diff=$g_delete_snapshots_to_delete_tmp_file

			zxfer_ensure_snapshot_delete_temp_artifacts

			printf 'source=%s\n' "$g_delete_source_tmp_file"
			printf 'dest=%s\n' "$g_delete_dest_tmp_file"
			printf 'diff=%s\n' "$g_delete_snapshots_to_delete_tmp_file"
			printf 'reused=%s\n' \
				"$([ "$first_source" = "$g_delete_source_tmp_file" ] &&
					[ "$first_dest" = "$g_delete_dest_tmp_file" ] &&
					[ "$first_diff" = "$g_delete_snapshots_to_delete_tmp_file" ] &&
					printf yes || printf no)"
			printf 'count=%s\n' "$counter"
		)
	)

	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the source path." \
		"$output" "source=$TEST_TMPDIR/delete.1"
	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the destination path." \
		"$output" "dest=$TEST_TMPDIR/delete.2"
	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the diff path." \
		"$output" "diff=$TEST_TMPDIR/delete.3"
	assertContains "Lazy snapshot-delete tempfile setup should reuse already-assigned paths on later calls." \
		"$output" "reused=yes"
	assertContains "Lazy snapshot-delete tempfile setup should allocate exactly once per required path." \
		"$output" "count=3"
}

test_ensure_snapshot_delete_temp_artifacts_preserves_allocation_failures_without_publishing_paths() {
	output=$(
		(
			zxfer_reset_delete_temp_artifacts
			zxfer_get_temp_file() {
				return 71
			}

			set +e
			zxfer_ensure_snapshot_delete_temp_artifacts
			status=$?
			set -e

			printf 'status=%s\n' "$status"
			printf 'source=<%s>\n' "${g_delete_source_tmp_file:-}"
			printf 'dest=<%s>\n' "${g_delete_dest_tmp_file:-}"
			printf 'diff=<%s>\n' "${g_delete_snapshots_to_delete_tmp_file:-}"
		)
	)

	assertContains "Lazy snapshot-delete tempfile setup should preserve the first allocation failure status." \
		"$output" "status=71"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a source temp path when allocation fails." \
		"$output" "source=<>"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a destination temp path when allocation fails." \
		"$output" "dest=<>"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a diff temp path when allocation fails." \
		"$output" "diff=<>"
}

test_ensure_snapshot_delete_temp_artifacts_cleans_up_after_second_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/delete_temp_cleanup_second.log"
	zxfer_reset_delete_temp_artifacts
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-second-source"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			return 71
			;;
		esac
		return 72
	}
	zxfer_cleanup_runtime_artifact_paths() {
		printf '%s\n' "$*" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_ensure_snapshot_delete_temp_artifacts >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	source_path=${g_delete_source_tmp_file:-}
	dest_path=${g_delete_dest_tmp_file:-}
	diff_path=${g_delete_snapshots_to_delete_tmp_file:-}

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell delete-temp setup should preserve the second allocation failure status." \
		71 "$status"
	assertEquals "Current-shell delete-temp setup should clean up the already allocated source tempfile when the second allocation fails." \
		"$TEST_TMPDIR/delete-second-source" "$cleanup_paths"
	assertEquals "Current-shell delete-temp setup should not publish the source tempfile after the second allocation fails." \
		"" "$source_path"
	assertEquals "Current-shell delete-temp setup should not publish the destination tempfile after the second allocation fails." \
		"" "$dest_path"
	assertEquals "Current-shell delete-temp setup should not publish the diff tempfile after the second allocation fails." \
		"" "$diff_path"
}

test_ensure_snapshot_delete_temp_artifacts_cleans_up_after_third_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/delete_temp_cleanup_third.log"
	zxfer_reset_delete_temp_artifacts
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-third-source"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-third-dest"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		3)
			return 72
			;;
		esac
		return 73
	}
	zxfer_cleanup_runtime_artifact_paths() {
		printf '%s\n' "$*" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_ensure_snapshot_delete_temp_artifacts >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	source_path=${g_delete_source_tmp_file:-}
	dest_path=${g_delete_dest_tmp_file:-}
	diff_path=${g_delete_snapshots_to_delete_tmp_file:-}

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell delete-temp setup should preserve the third allocation failure status." \
		72 "$status"
	assertEquals "Current-shell delete-temp setup should clean up both already allocated tempfiles when the third allocation fails." \
		"$TEST_TMPDIR/delete-third-source $TEST_TMPDIR/delete-third-dest" "$cleanup_paths"
	assertEquals "Current-shell delete-temp setup should not publish the source tempfile after the third allocation fails." \
		"" "$source_path"
	assertEquals "Current-shell delete-temp setup should not publish the destination tempfile after the third allocation fails." \
		"" "$dest_path"
	assertEquals "Current-shell delete-temp setup should not publish the diff tempfile after the third allocation fails." \
		"" "$diff_path"
}

test_init_globals_calls_owner_reset_helpers() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			reset_log="$TEST_TMPDIR/init_globals_resets.log"
			: >"$reset_log"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_reset_replication_runtime_state() {
				printf 'replication\n' >>"$reset_log"
			}
			zxfer_reset_send_receive_state() {
				printf 'send_receive\n' >>"$reset_log"
			}
			zxfer_reset_background_job_state() {
				printf 'background_jobs\n' >>"$reset_log"
			}
			zxfer_reset_destination_existence_cache() {
				printf 'destination_cache\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_record_indexes() {
				printf 'snapshot_indexes\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_discovery_state() {
				printf 'snapshot_discovery\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_reconcile_state() {
				printf 'snapshot_reconcile\n' >>"$reset_log"
			}
			zxfer_reset_backup_metadata_state() {
				printf 'backup_metadata\n' >>"$reset_log"
			}
			zxfer_reset_property_runtime_state() {
				printf 'property_runtime\n' >>"$reset_log"
			}
			zxfer_reset_property_iteration_caches() {
				printf 'property_cache\n' >>"$reset_log"
			}
			zxfer_reset_property_reconcile_state() {
				printf 'property_reconcile\n' >>"$reset_log"
			}

			zxfer_init_globals
			cat "$reset_log"
		)
	)

	assertContains "zxfer_init_globals should delegate replication scratch reset to the replication owner helper." \
		"$output" "replication"
	assertContains "zxfer_init_globals should delegate send/receive scratch reset to the send/receive owner helper." \
		"$output" "send_receive"
	assertContains "zxfer_init_globals should delegate supervised background-job scratch reset to the background-job owner helper." \
		"$output" "background_jobs"
	assertContains "zxfer_init_globals should delegate destination cache reset to the snapshot-state owner helper." \
		"$output" "destination_cache"
	assertContains "zxfer_init_globals should delegate snapshot index reset to the snapshot-state owner helper." \
		"$output" "snapshot_indexes"
	assertContains "zxfer_init_globals should delegate snapshot discovery reset to the snapshot-discovery owner helper." \
		"$output" "snapshot_discovery"
	assertContains "zxfer_init_globals should delegate snapshot reconcile reset to the snapshot-reconcile owner helper." \
		"$output" "snapshot_reconcile"
	assertContains "zxfer_init_globals should delegate backup metadata reset to the backup owner helper." \
		"$output" "backup_metadata"
	assertContains "zxfer_init_globals should delegate run-wide property state reset to the property owner helper." \
		"$output" "property_runtime"
	assertContains "zxfer_init_globals should delegate property-cache reset to the property-cache owner helper." \
		"$output" "property_cache"
	assertContains "zxfer_init_globals should delegate per-call property reconcile reset to the property owner helper." \
		"$output" "property_reconcile"
}

test_init_globals_reinitializes_property_module_scratch_state_when_reinvoked() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}

			zxfer_init_globals

			g_zxfer_source_property_table="tank/src	compression=stale=local"
			g_zxfer_destination_property_table="backup/dst	compression=stale=local"
			g_zxfer_property_table_memo_side="source"
			g_zxfer_property_table_memo_dataset="tank/src"
			g_zxfer_property_table_memo_payload="compression=stale=local"
			g_zxfer_required_properties_result="stale-required"
			g_zxfer_adjusted_set_list="compression=lz4"
			g_zxfer_adjusted_inherit_list="mountpoint"
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result="compression=lz4=local"
			g_zxfer_property_stage_file_read_result="stale-stage-read"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_destination_property_tree_prefetch_state=2
			g_zxfer_unsupported_filesystem_properties="compression"
			g_zxfer_unsupported_volume_properties="volblocksize"

			zxfer_init_globals

			printf 'required=<%s>\n' "$g_zxfer_required_properties_result"
			printf 'source_table=<%s>\n' "${g_zxfer_source_property_table:-}"
			printf 'destination_table=<%s>\n' "${g_zxfer_destination_property_table:-}"
			printf 'memo_dataset=<%s>\n' "${g_zxfer_property_table_memo_dataset:-}"
			printf 'adjusted_set=<%s>\n' "$g_zxfer_adjusted_set_list"
			printf 'adjusted_inherit=<%s>\n' "$g_zxfer_adjusted_inherit_list"
			printf 'override_result=<%s>\n' "$g_zxfer_override_pvs_result"
			printf 'creation_result=<%s>\n' "$g_zxfer_creation_pvs_result"
			printf 'property_stage_read=<%s>\n' "$g_zxfer_property_stage_file_read_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'prefetch_state=%s\n' "$g_zxfer_destination_property_tree_prefetch_state"
			printf 'unsupported_fs=<%s>\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'unsupported_vol=<%s>\n' "$g_zxfer_unsupported_volume_properties"
		)
	)

	assertContains "Re-running zxfer_init_globals should clear required-property scratch results." \
		"$output" "required=<>"
	assertContains "Re-running zxfer_init_globals should clear the in-memory source property table." \
		"$output" "source_table=<>"
	assertContains "Re-running zxfer_init_globals should clear the in-memory destination property table." \
		"$output" "destination_table=<>"
	assertContains "Re-running zxfer_init_globals should clear the property-table memo." \
		"$output" "memo_dataset=<>"
	assertContains "Re-running zxfer_init_globals should clear adjusted set scratch state." \
		"$output" "adjusted_set=<>"
	assertContains "Re-running zxfer_init_globals should clear adjusted inherit scratch state." \
		"$output" "adjusted_inherit=<>"
	assertContains "Re-running zxfer_init_globals should clear derived override scratch state." \
		"$output" "override_result=<>"
	assertContains "Re-running zxfer_init_globals should clear derived creation-property scratch state." \
		"$output" "creation_result=<>"
	assertContains "Re-running zxfer_init_globals should clear staged property-file read scratch state." \
		"$output" "property_stage_read=<>"
	assertContains "Re-running zxfer_init_globals should clear remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "Re-running zxfer_init_globals should rearm destination property prefetch state." \
		"$output" "prefetch_state=0"
	assertContains "Re-running zxfer_init_globals should clear filesystem unsupported-property cache state." \
		"$output" "unsupported_fs=<>"
	assertContains "Re-running zxfer_init_globals should clear volume unsupported-property cache state." \
		"$output" "unsupported_vol=<>"
}

test_zxfer_cache_object_file_round_trip_preserves_metadata_and_payload() {
	object_path="$TEST_TMPDIR/cache-object-round-trip.entry"
	output_file="$TEST_TMPDIR/cache-object-round-trip.out"
	metadata=$(printf '%s\n' \
		"created_epoch=123" \
		"side=source")
	payload=$(printf '%s\n' \
		"line one" \
		"line two")

	zxfer_write_cache_object_file_atomically \
		"$object_path" "demo-kind" "$metadata" "$payload" >/dev/null
	write_status=$?
	zxfer_read_cache_object_file "$object_path" "demo-kind" >"$output_file"
	read_status=$?

	assertEquals "Atomic cache-object writes should publish a readable cache object." \
		0 "$write_status"
	assertEquals "Cache-object reads should succeed for valid published objects." \
		0 "$read_status"
	assertEquals "Valid cache-object reads should reproduce the original payload on stdout." \
		"$payload" "$(cat "$output_file")"
	assertEquals "Valid cache-object reads should publish the parsed object kind in shared scratch state." \
		"demo-kind" "$g_zxfer_cache_object_kind_result"
	assertEquals "Valid cache-object reads should preserve metadata lines in shared scratch state." \
		"$metadata" "$g_zxfer_cache_object_metadata_result"
	assertEquals "Valid cache-object reads should preserve the full payload in shared scratch state." \
		"$payload" "$g_zxfer_cache_object_payload_result"
}

test_cache_object_metadata_helpers_cover_invalid_lines_missing_keys_and_max_yield_constant() {
	set +e
	zxfer_validate_cache_object_metadata_lines "broken-metadata-line" >/dev/null 2>&1
	metadata_status=$?
	zxfer_get_cache_object_metadata_value "kind=demo" "missing" >/dev/null 2>&1
	missing_key_status=$?
	set -e
	max_yield=$(zxfer_get_max_yield_iterations)

	assertEquals "Cache-object metadata validation should fail closed on lines without key separators." \
		1 "$metadata_status"
	assertEquals "Cache-object metadata lookup should fail when the requested key is absent." \
		1 "$missing_key_status"
	assertEquals "Runtime max-yield helpers should return the exported runtime constant." \
		"$ZXFER_MAX_YIELD_ITERATIONS" "$max_yield"
}

test_zxfer_read_cache_object_file_rejects_missing_end_marker() {
	object_path="$TEST_TMPDIR/cache-object-missing-end.entry"
	output_file="$TEST_TMPDIR/cache-object-missing-end.out"

	cat >"$object_path" <<-EOF
		$ZXFER_CACHE_OBJECT_HEADER_LINE
		kind=demo-kind

		payload
	EOF

	g_zxfer_cache_object_kind_result="stale-kind"
	g_zxfer_cache_object_metadata_result="stale=metadata"
	g_zxfer_cache_object_payload_result="stale-payload"
	set +e
	zxfer_read_cache_object_file "$object_path" "demo-kind" >"$output_file"
	status=$?

	assertEquals "Cache-object reads should fail closed when the end marker is missing." \
		1 "$status"
	assertEquals "Rejected cache objects should not emit a payload." \
		"" "$(cat "$output_file")"
	assertEquals "Rejected cache objects should clear the kind scratch result." \
		"" "$g_zxfer_cache_object_kind_result"
	assertEquals "Rejected cache objects should clear the metadata scratch result." \
		"" "$g_zxfer_cache_object_metadata_result"
	assertEquals "Rejected cache objects should clear the payload scratch result." \
		"" "$g_zxfer_cache_object_payload_result"
}

test_zxfer_read_cache_object_file_rejects_wrong_kind() {
	object_path="$TEST_TMPDIR/cache-object-wrong-kind.entry"
	output_file="$TEST_TMPDIR/cache-object-wrong-kind.out"

	zxfer_write_cache_object_file_atomically \
		"$object_path" "actual-kind" "" "payload" >/dev/null ||
		fail "Unable to create a valid cache object fixture."

	g_zxfer_cache_object_kind_result="stale-kind"
	g_zxfer_cache_object_payload_result="stale-payload"
	set +e
	zxfer_read_cache_object_file "$object_path" "expected-kind" >"$output_file"
	status=$?

	assertEquals "Cache-object reads should fail closed when the published object kind does not match the expected kind." \
		1 "$status"
	assertEquals "Wrong-kind cache objects should not emit a payload." \
		"" "$(cat "$output_file")"
	assertEquals "Wrong-kind cache objects should clear the cached kind scratch state." \
		"" "$g_zxfer_cache_object_kind_result"
	assertEquals "Wrong-kind cache objects should clear the cached payload scratch state." \
		"" "$g_zxfer_cache_object_payload_result"
}

test_zxfer_read_cache_object_file_rejects_runtime_read_failures() {
	unreadable_path="$TEST_TMPDIR/cache-object-unreadable.entry"
	unreadable_output="$TEST_TMPDIR/cache-object-unreadable.out"
	output=$(
		(
			zxfer_write_cache_object_file_atomically \
				"$unreadable_path" "demo-kind" "" "payload" >/dev/null ||
				fail "Unable to create a cache object fixture for readback failure coverage."

			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result="stale-runtime-read"
				return 1
			}

			g_zxfer_cache_object_kind_result="stale-kind"
			g_zxfer_cache_object_metadata_result="stale=metadata"
			g_zxfer_cache_object_payload_result="stale-payload"
			set +e
			zxfer_read_cache_object_file "$unreadable_path" "demo-kind" >"$unreadable_output"
			unreadable_status=$?
			set -e

			printf 'status=%s\n' "$unreadable_status"
			printf 'payload=<%s>\n' "$(cat "$unreadable_output")"
			printf 'kind=<%s>\n' "$g_zxfer_cache_object_kind_result"
			printf 'metadata=<%s>\n' "$g_zxfer_cache_object_metadata_result"
			printf 'cache_payload=<%s>\n' "$g_zxfer_cache_object_payload_result"
		)
	)

	assertContains "Cache-object reads should fail closed when the staged runtime read helper fails." \
		"$output" "status=1"
	assertContains "Runtime read failures should not emit a payload." \
		"$output" "payload=<>"
	assertContains "Runtime read failures should clear the cached kind scratch state." \
		"$output" "kind=<>"
	assertContains "Runtime read failures should clear the cached metadata scratch state." \
		"$output" "metadata=<>"
	assertContains "Runtime read failures should clear the cached payload scratch state." \
		"$output" "cache_payload=<>"
}

test_zxfer_read_cache_object_file_rejects_invalid_kind_and_metadata_lines() {
	invalid_kind_path="$TEST_TMPDIR/cache-object-invalid-kind"
	invalid_metadata_path="$TEST_TMPDIR/cache-object-invalid-metadata"
	printf '%s\n%s\n\npayload\n%s\n' \
		"$ZXFER_CACHE_OBJECT_HEADER_LINE" \
		"broken" \
		"$ZXFER_CACHE_OBJECT_END_LINE" >"$invalid_kind_path"
	printf '%s\n%s\n%s\n\npayload\n%s\n' \
		"$ZXFER_CACHE_OBJECT_HEADER_LINE" \
		"kind=demo-kind" \
		"broken-metadata-line" \
		"$ZXFER_CACHE_OBJECT_END_LINE" >"$invalid_metadata_path"

	set +e
	zxfer_read_cache_object_file "$invalid_kind_path" "demo-kind" >/dev/null 2>&1
	invalid_kind_status=$?
	zxfer_read_cache_object_file "$invalid_metadata_path" "demo-kind" >/dev/null 2>&1
	invalid_metadata_status=$?
	set -e

	assertEquals "Cache-object reads should fail closed when the kind header is malformed." \
		1 "$invalid_kind_status"
	assertEquals "Cache-object reads should fail closed when metadata lines are malformed." \
		1 "$invalid_metadata_status"
}

test_zxfer_read_cache_object_file_rejects_empty_payloads() {
	empty_path="$TEST_TMPDIR/cache-object-empty.entry"
	empty_output="$TEST_TMPDIR/cache-object-empty.out"

	cat >"$empty_path" <<-EOF
		$ZXFER_CACHE_OBJECT_HEADER_LINE
		kind=demo-kind

		$ZXFER_CACHE_OBJECT_END_LINE
	EOF

	set +e
	zxfer_read_cache_object_file "$empty_path" "demo-kind" >"$empty_output"
	empty_status=$?

	assertEquals "Cache-object reads should fail closed when the published payload is empty." \
		1 "$empty_status"
	assertEquals "Empty-payload cache objects should not emit a payload." \
		"" "$(cat "$empty_output")"
}

test_zxfer_write_cache_object_file_atomically_cleans_up_stage_dirs_on_write_readback_and_rename_failures() {
	stage_root="$TEST_TMPDIR/cache-object-stage-cleanup"
	write_target="$stage_root/write-failure.entry"
	readback_target="$stage_root/readback-failure.entry"
	rename_target="$stage_root/rename-failure.entry"
	mkdir -p "$stage_root" || fail "Unable to create cache-object stage root."

	set +e
	(
		zxfer_write_cache_object_contents_to_path() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$write_target" "demo-kind" "" "payload"
	)
	write_status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		write_stage_count=$#
	else
		write_stage_count=0
	fi

	set +e
	(
		mv() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$rename_target" "demo-kind" "" "payload"
	)
	rename_status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		rename_stage_count=$#
	else
		rename_stage_count=0
	fi

	assertEquals "Atomic cache-object writes should fail closed when the staged payload cannot be written." \
		1 "$write_status"
	assertFalse "Failed staged payload writes should not leave a published cache object behind." \
		"[ -e \"$write_target\" ]"
	assertEquals "Failed staged payload writes should clean up their private stage directory." \
		0 "$write_stage_count"

	set +e
	(
		zxfer_read_cache_object_file() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$readback_target" "demo-kind" "" "payload"
	)
	readback_status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		readback_stage_count=$#
	else
		readback_stage_count=0
	fi

	assertEquals "Atomic cache-object writes should fail closed when the staged object cannot be read back for validation." \
		1 "$readback_status"
	assertFalse "Failed staged readback validation should not leave a published cache object behind." \
		"[ -e \"$readback_target\" ]"
	assertEquals "Failed staged readback validation should clean up their private stage directory." \
		0 "$readback_stage_count"

	assertEquals "Atomic cache-object writes should fail closed when the staged object cannot be renamed into place." \
		1 "$rename_status"
	assertFalse "Failed cache-object renames should not leave a published cache object behind." \
		"[ -e \"$rename_target\" ]"
	assertEquals "Failed cache-object renames should clean up their private stage directory." \
		0 "$rename_stage_count"
}

test_zxfer_write_cache_object_file_atomically_cleans_up_stage_dirs_when_rmdir_would_fail() {
	stage_root="$TEST_TMPDIR/cache-object-stage-rmdir-failure"
	target_path="$stage_root/published.entry"
	mkdir -p "$stage_root" || fail "Unable to create cache-object publish root."

	set +e
	(
		rmdir() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$target_path" "demo-kind" "" "payload"
	)
	status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		stage_count=$#
	else
		stage_count=0
	fi

	assertEquals "Successful cache-object publishes should not depend on a direct rmdir cleanup path." \
		0 "$status"
	assertTrue "Successful cache-object publishes should still create the published target." \
		"[ -f \"$target_path\" ]"
	assertEquals "Successful cache-object publishes should clean up their private stage directory even when rmdir would fail." \
		0 "$stage_count"
}

test_zxfer_write_cache_object_file_atomically_reports_stage_dir_creation_failures() {
	set +e
	stage_output=$(
		(
			zxfer_create_cache_object_stage_dir_for_path() {
				return 1
			}
			zxfer_write_cache_object_file_atomically \
				"$TEST_TMPDIR/cache-object-stage-dir-failure" "demo-kind" "" "payload" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertEquals "Atomic cache-object writes should fail closed when the stage directory cannot be allocated." \
		"status=1" "$stage_output"
}

test_zxfer_create_cache_object_stage_dir_for_path_preserves_parent_lookup_failures() {
	output=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			set +e
			zxfer_create_cache_object_stage_dir_for_path "$TEST_TMPDIR/cache-object-parent-lookup" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Cache-object stage-dir creation should preserve target-parent lookup failures." \
		"$output" "status=1"
}

test_zxfer_write_cache_object_file_atomically_registers_stage_dirs_in_current_shell_before_failures() {
	stage_root="$TEST_TMPDIR/cache-object-stage-current-shell"
	target_path="$stage_root/published.entry"
	trace_file="$TEST_TMPDIR/cache-object-stage-current-shell.trace"
	mkdir -p "$stage_root" || fail "Unable to create the cache-object stage root."

	output=$(
		(
			zxfer_write_cache_object_contents_to_path() {
				printf 'registered=<%s>\n' "${g_zxfer_runtime_artifact_cleanup_paths:-}" >"$trace_file"
				return 1
			}
			set +e
			zxfer_write_cache_object_file_atomically \
				"$target_path" "demo-kind" "" "payload" >/dev/null
			status=$?
			set -e
			printf 'status=%s\n' "$status"
		)
	)
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		stage_count=$#
	else
		stage_count=0
	fi

	assertContains "Atomic cache-object writes should still fail closed when the staged payload helper fails." \
		"$output" "status=1"
	assertContains "Atomic cache-object writes should register their private stage dir in current-shell cleanup state before helper failures." \
		"$(cat "$trace_file")" "/.zxfer-cache-object."
	assertEquals "Atomic cache-object writes should still clean up their private stage directory after helper failures." \
		0 "$stage_count"
}

test_zxfer_write_cache_object_contents_to_path_rejects_invalid_metadata_and_failed_writes() {
	write_failure_target="$TEST_TMPDIR/cache-object-write-failure.entry"

	set +e
	zxfer_write_cache_object_contents_to_path \
		"$TEST_TMPDIR/cache-object-invalid-metadata.entry" \
		"demo-kind" "broken-metadata-line" "payload" >/dev/null 2>&1
	metadata_status=$?
	write_failure_output=$(
		(
			printf() {
				return 1
			}
			set +e
			zxfer_write_cache_object_contents_to_path \
				"$write_failure_target" \
				"demo-kind" "kind=demo" "payload" >/dev/null 2>&1
			command printf 'status=%s\n' "$?"
		)
	)

	assertEquals "Cache-object content writes should fail closed when metadata lines are malformed." \
		1 "$metadata_status"
	assertContains "Cache-object content writes should fail closed when the staged write operation fails." \
		"$write_failure_output" "status=1"
	assertFalse "Failed cache-object content writes should not create the destination path when the staged write operation fails." \
		"[ -e \"$write_failure_target\" ]"
}

test_try_get_effective_tmpdir_fails_cleanly_when_no_safe_default_exists() {
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			zxfer_try_get_default_tmpdir() {
				return 1
			}
			set +e
			zxfer_try_get_effective_tmpdir >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'requested=%s\n' "${g_zxfer_effective_tmpdir_requested:-}"
			printf 'effective=<%s>\n' "${g_zxfer_effective_tmpdir:-}"
		)
	)

	assertEquals "Temp-root resolution should fail cleanly when both TMPDIR and the built-in defaults are unavailable." \
		"status=1
requested=__ZXFER_DEFAULT_TMPDIR__
effective=<>" "$output"
}

test_zxfer_register_runtime_traps_installs_exit_handler() {
	output=$(
		(
			zxfer_register_runtime_traps
			trap
		)
	)

	assertContains "Runtime trap registration should install the shared zxfer_trap_exit handler." \
		"$output" "zxfer_trap_exit"
}

test_zxfer_init_destination_execution_context_reports_remote_decompress_resolution_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			g_option_z_compress=1
			g_cmd_decompress="zstd -d"
			g_cmd_zfs="/sbin/zfs"
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/remote/bin/$2"
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "decompress lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_init_destination_execution_context
		)
	)
	status=$?

	assertEquals "Destination execution-context initialization should fail closed when the remote decompressor cannot be resolved safely." \
		1 "$status"
	assertContains "Remote decompressor resolution failures should preserve the dependency error." \
		"$output" "decompress lookup failed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
