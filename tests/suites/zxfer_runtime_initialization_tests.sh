#!/bin/sh
# Runtime initialization, reset, trap-registration, and remote-context tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

test_runtime_artifact_registry_helpers_cover_rejected_and_missing_entries() {
	set +e
	zxfer_runtime_artifact_registration_path_has_safe_shape "relative-stage"
	relative_status=$?
	nested_child_status=$(
		(
			g_zxfer_run_tmp_root="$TEST_TMPDIR/zxfer.runtime-shape"
			zxfer_run_tmp_root_is_current_private_dir() {
				return 0
			}
			zxfer_runtime_artifact_path_is_run_root_child \
				"$g_zxfer_run_tmp_root/nested/child"
			printf '%s\n' "$?"
		)
	)

	g_zxfer_runtime_artifact_cleanup_dir_identities=""
	zxfer_get_registered_runtime_artifact_directory_identity \
		"$TEST_TMPDIR/zxfer.missing-stage"
	missing_identity_status=$?

	assertEquals "Runtime artifact registration should reject non-absolute paths." \
		1 "$relative_status"
	assertEquals "A contained runtime artifact must be one direct run-root child, never a nested path." \
		1 "$nested_child_status"
	assertEquals "Runtime artifact identity lookup should fail for an unregistered directory." \
		1 "$missing_identity_status"
	assertEquals "Missing runtime artifact identity lookup should clear the owner result channel." \
		"" "$g_zxfer_runtime_artifact_directory_identity_result"
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

			g_zxfer_source_property_table="tank/src\tcompression=stale=local"
			g_zxfer_destination_property_table="backup/dst\tcompression=stale=local"
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

test_try_get_effective_tmpdir_fails_cleanly_when_no_safe_default_exists() {
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			# A candidate list with no safe entry exhausts the fallback walk.
			zxfer_list_default_tmpdir_candidates() {
				printf '%s\n' "$TEST_TMPDIR/no-such-default-candidate"
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
