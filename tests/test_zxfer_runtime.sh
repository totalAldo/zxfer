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

test_zxfer_cleanup_pid_helpers_cover_current_shell_paths() {
	kill_log="$TEST_TMPDIR/runtime_cleanup_pids.log"
	: >"$kill_log"

	(
		kill() {
			printf '%s\n' "$1" >>"$kill_log"
		}

		g_zxfer_cleanup_pids=""
		zxfer_register_cleanup_pid ""
		zxfer_register_cleanup_pid 101
		zxfer_register_cleanup_pid 202
		zxfer_register_cleanup_pid 202
		printf 'registered=<%s>\n' "$g_zxfer_cleanup_pids"
		zxfer_unregister_cleanup_pid 101
		printf 'remaining=<%s>\n' "$g_zxfer_cleanup_pids"
		zxfer_register_cleanup_pid "$$"
		zxfer_kill_registered_cleanup_pids
		printf 'after_kill=<%s>\n' "$g_zxfer_cleanup_pids"
	) >"$TEST_TMPDIR/runtime_cleanup_pids.out"
	output=$(cat "$TEST_TMPDIR/runtime_cleanup_pids.out")

	assertContains "Cleanup PID registration should keep unique non-empty PIDs." \
		"$output" "registered=<101 202>"
	assertContains "Cleanup PID unregistration should remove only the requested PID." \
		"$output" "remaining=<202>"
	assertContains "Cleanup PID teardown should clear the registered PID list after issuing kills." \
		"$output" "after_kill=<>"
	assertEquals "Cleanup PID teardown should only signal non-self registered PIDs." \
		"202" "$(tr -d '\n' <"$kill_log")"
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
		"$output" "version=2.0.0-20260411"
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

test_runtime_artifact_allocators_use_validated_temp_root_for_files_and_dirs() {
	zxfer_create_runtime_artifact_file "runtime-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-dir" >/dev/null
	dir_status=$?
	dir_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "Runtime artifact file allocation should succeed under the validated temp root." \
		0 "$file_status"
	assertEquals "Runtime artifact directory allocation should succeed under the validated temp root." \
		0 "$dir_status"
	assertContains "Runtime artifact files should be allocated under the validated temp root." \
		"$file_path" "$TEST_TMPDIR/"
	assertContains "Runtime artifact directories should be allocated under the validated temp root." \
		"$dir_path" "$TEST_TMPDIR/"
	assertTrue "Runtime artifact file allocation should create the requested file." \
		"[ -f \"$file_path\" ]"
	assertTrue "Runtime artifact directory allocation should create the requested directory." \
		"[ -d \"$dir_path\" ]"
	assertContains "Runtime artifact allocation should register the created file for cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertContains "Runtime artifact allocation should register the created directory for cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
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

test_zxfer_write_runtime_cache_file_atomically_cleans_up_on_write_and_publish_failures() {
	stage_root="$TEST_TMPDIR/runtime-cache-stage-cleanup"
	write_target="$stage_root/write-failure.entry"
	publish_target="$stage_root/publish-failure.entry"
	mkdir -p "$stage_root" || fail "Unable to create runtime cache stage root."

	set +e
	(
		zxfer_write_runtime_artifact_file() {
			return 1
		}
		zxfer_write_runtime_cache_file_atomically \
			"$write_target" "payload" "zxfer-runtime-cache-test"
	)
	write_status=$?
	set -- "$stage_root"/.zxfer-runtime-cache-test.*
	if [ -e "$1" ]; then
		write_stage_count=$#
	else
		write_stage_count=0
	fi

	set +e
	(
		zxfer_publish_runtime_artifact_file() {
			return 1
		}
		zxfer_write_runtime_cache_file_atomically \
			"$publish_target" "payload" "zxfer-runtime-cache-test"
	)
	publish_status=$?
	set -- "$stage_root"/.zxfer-runtime-cache-test.*
	if [ -e "$1" ]; then
		publish_stage_count=$#
	else
		publish_stage_count=0
	fi

	assertEquals "Atomic runtime cache writes should fail closed when the staged payload cannot be written." \
		1 "$write_status"
	assertFalse "Failed runtime cache writes should not leave a published cache target behind." \
		"[ -e \"$write_target\" ]"
	assertEquals "Failed runtime cache writes should clean up their staged artifact files." \
		0 "$write_stage_count"
	assertEquals "Atomic runtime cache writes should fail closed when the staged payload cannot be published." \
		1 "$publish_status"
	assertFalse "Failed runtime cache publishes should not leave a published cache target behind." \
		"[ -e \"$publish_target\" ]"
	assertEquals "Failed runtime cache publishes should clean up their staged artifact files." \
		0 "$publish_stage_count"
}

test_zxfer_write_runtime_cache_file_atomically_requires_existing_parent_dir() {
	missing_parent="$TEST_TMPDIR/runtime-cache-missing-parent"
	target_path="$missing_parent/cache.entry"

	set +e
	zxfer_write_runtime_cache_file_atomically "$target_path" "payload" "zxfer-runtime-cache-test"
	status=$?

	assertEquals "Atomic runtime cache writes should fail closed when the target parent directory is missing." \
		1 "$status"
	assertFalse "Atomic runtime cache writes should not create a missing parent directory implicitly." \
		"[ -d \"$missing_parent\" ]"
	assertFalse "Atomic runtime cache writes should not leave a published cache target behind when the parent is missing." \
		"[ -e \"$target_path\" ]"
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
			g_zxfer_property_cache_path="/tmp/stale-cache"
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
			printf 'cache_path=<%s>\n' "$g_zxfer_property_cache_path"
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
	assertContains "zxfer_init_globals should reset property-cache path scratch state." \
		"$output" "cache_path=<>"
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

			stale_cache_dir="$TEST_TMPDIR/stale-property-cache"
			mkdir -p "$stale_cache_dir/normalized/source"
			: >"$stale_cache_dir/normalized/source/entry"
			g_zxfer_property_cache_dir=$stale_cache_dir
			g_zxfer_required_properties_result="stale-required"
			g_zxfer_property_cache_key="stale-key"
			g_zxfer_adjusted_set_list="compression=lz4"
			g_zxfer_adjusted_inherit_list="mountpoint"
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result="compression=lz4=local"
			g_zxfer_property_stage_file_read_result="stale-stage-read"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_destination_property_tree_prefetch_state=2
			g_unsupported_properties="compression"
			g_zxfer_unsupported_filesystem_properties="compression"
			g_zxfer_unsupported_volume_properties="volblocksize"

			zxfer_init_globals

			printf 'required=<%s>\n' "$g_zxfer_required_properties_result"
			printf 'cache_key=<%s>\n' "$g_zxfer_property_cache_key"
			printf 'adjusted_set=<%s>\n' "$g_zxfer_adjusted_set_list"
			printf 'adjusted_inherit=<%s>\n' "$g_zxfer_adjusted_inherit_list"
			printf 'override_result=<%s>\n' "$g_zxfer_override_pvs_result"
			printf 'creation_result=<%s>\n' "$g_zxfer_creation_pvs_result"
			printf 'property_stage_read=<%s>\n' "$g_zxfer_property_stage_file_read_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'cache_dir=<%s>\n' "$g_zxfer_property_cache_dir"
			printf 'prefetch_state=%s\n' "$g_zxfer_destination_property_tree_prefetch_state"
			printf 'unsupported=<%s>\n' "$g_unsupported_properties"
			printf 'unsupported_fs=<%s>\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'unsupported_vol=<%s>\n' "$g_zxfer_unsupported_volume_properties"
			if [ -d "$stale_cache_dir" ]; then
				printf 'stale_dir_exists=1\n'
			else
				printf 'stale_dir_exists=0\n'
			fi
		)
	)

	assertContains "Re-running zxfer_init_globals should clear required-property scratch results." \
		"$output" "required=<>"
	assertContains "Re-running zxfer_init_globals should clear property-cache key scratch state." \
		"$output" "cache_key=<>"
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
	assertContains "Re-running zxfer_init_globals should reset the cache directory pointer." \
		"$output" "cache_dir=<>"
	assertContains "Re-running zxfer_init_globals should rearm destination property prefetch state." \
		"$output" "prefetch_state=0"
	assertContains "Re-running zxfer_init_globals should clear run-wide unsupported-property scratch state." \
		"$output" "unsupported=<>"
	assertContains "Re-running zxfer_init_globals should clear filesystem unsupported-property cache state." \
		"$output" "unsupported_fs=<>"
	assertContains "Re-running zxfer_init_globals should clear volume unsupported-property cache state." \
		"$output" "unsupported_vol=<>"
	assertContains "Re-running zxfer_init_globals should remove stale property cache directories." \
		"$output" "stale_dir_exists=0"
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

test_zxfer_write_cache_object_file_atomically_cleans_up_stage_dirs_on_write_and_rename_failures() {
	stage_root="$TEST_TMPDIR/cache-object-stage-cleanup"
	write_target="$stage_root/write-failure.entry"
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

test_zxfer_write_cache_object_file_atomically_reports_stage_dir_creation_failures_and_publish_dir_rejections() {
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
	publish_output=$(
		(
			stage_dir="$TEST_TMPDIR/publish-cache-object-stage"
			mkdir -p "$stage_dir" || exit 1
			relative_parent="relative-publish-parent"
			rm -rf "$relative_parent"
			mkdir -p "$relative_parent" || exit 1
			set +e
			zxfer_publish_cache_object_directory "$stage_dir" "$relative_parent/object-dir" >/dev/null 2>&1
			status=$?
			rm -rf "$relative_parent"
			set -e
			printf 'status=%s\n' "$status"
		)
	)
	set -e

	assertContains "Atomic cache-object writes should fail closed when the stage directory cannot be allocated." \
		"$stage_output" "status=1"
	assertContains "Publishing cache-object directories should reject existing relative parents that are outside the validated temp-root rules." \
		"$publish_output" "status=1"
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
