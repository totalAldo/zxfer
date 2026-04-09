#!/bin/sh
#
# shunit2 tests for zxfer_runtime.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

TESTS_DIR=$(dirname "$0")

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
	TMPDIR="$TEST_TMPDIR"
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
}

test_get_temp_file_creates_unique_paths() {
	file_one=$(zxfer_get_temp_file)
	file_two=$(zxfer_get_temp_file)

	assertNotEquals "Each temp-file request should return a unique path." \
		"$file_one" "$file_two"
	assertTrue "The first temp file should exist." '[ -f "$file_one" ]'
	assertTrue "The second temp file should exist." '[ -f "$file_two" ]'
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
			g_recursive_source_list="stale-source"
			g_last_common_snap="stale@snap"
			g_zfs_send_job_pids="123 456"
			g_zxfer_property_cache_path="/tmp/stale-cache"
			g_zxfer_source_pvs_raw="stale=property=local"
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
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'tmp_source=%s\n' "$g_delete_source_tmp_file"
			printf 'tmp_dest=%s\n' "$g_delete_dest_tmp_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'backup=<%s>\n' "$g_backup_file_contents"
			printf 'restored=<%s>\n' "$g_restored_backup_file_contents"
			printf 'recursive=<%s>\n' "$g_recursive_source_list"
			printf 'last_common=<%s>\n' "$g_last_common_snap"
			printf 'send_pids=<%s>\n' "$g_zfs_send_job_pids"
			printf 'cache_path=<%s>\n' "$g_zxfer_property_cache_path"
			printf 'source_pvs=<%s>\n' "$g_zxfer_source_pvs_raw"
		)
	)

	assertContains "zxfer_init_globals should initialize the secure path." \
		"$output" "secure=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should resolve the awk helper." \
		"$output" "awk=/usr/bin/awk"
	assertContains "zxfer_init_globals should record ssh control-socket support." \
		"$output" "control=1"
	assertContains "zxfer_init_globals should allocate the source temp file." \
		"$output" "tmp_source="
	assertContains "zxfer_init_globals should allocate the destination temp file." \
		"$output" "tmp_dest="
	assertContains "zxfer_init_globals should reset orchestration restart scratch state." \
		"$output" "restart=<>"
	assertContains "zxfer_init_globals should reset backup-metadata accumulation state." \
		"$output" "backup=<>"
	assertContains "zxfer_init_globals should reset restored backup scratch state." \
		"$output" "restored=<>"
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
			g_zxfer_destination_property_tree_prefetch_state=2
			g_unsupported_properties="compression"

			zxfer_init_globals

			printf 'required=<%s>\n' "$g_zxfer_required_properties_result"
			printf 'cache_key=<%s>\n' "$g_zxfer_property_cache_key"
			printf 'adjusted_set=<%s>\n' "$g_zxfer_adjusted_set_list"
			printf 'adjusted_inherit=<%s>\n' "$g_zxfer_adjusted_inherit_list"
			printf 'cache_dir=<%s>\n' "$g_zxfer_property_cache_dir"
			printf 'prefetch_state=%s\n' "$g_zxfer_destination_property_tree_prefetch_state"
			printf 'unsupported=<%s>\n' "$g_unsupported_properties"
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
	assertContains "Re-running zxfer_init_globals should reset the cache directory pointer." \
		"$output" "cache_dir=<>"
	assertContains "Re-running zxfer_init_globals should rearm destination property prefetch state." \
		"$output" "prefetch_state=0"
	assertContains "Re-running zxfer_init_globals should clear run-wide unsupported-property scratch state." \
		"$output" "unsupported=<>"
	assertContains "Re-running zxfer_init_globals should remove stale property cache directories." \
		"$output" "stale_dir_exists=0"
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
