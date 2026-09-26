#!/bin/sh
# Runtime initialization, reset, trap-registration, and remote-context tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

test_runtime_artifact_registry_helpers_cover_rejected_and_missing_entries() {
	set +e
	zxfer_runtime_artifact_registration_path_has_safe_shape "relative-stage"
	relative_status=$?
	child_statuses=$(
		(
			zxfer_ensure_run_tmp_root || exit 90
			zxfer_runtime_artifact_path_is_run_root_child \
				"$g_zxfer_run_tmp_root/child"
			printf 'direct=%s ' "$?"
			zxfer_runtime_artifact_path_is_run_root_child \
				"$g_zxfer_run_tmp_root/nested/child"
			printf 'nested=%s\n' "$?"
			zxfer_remove_run_tmp_root
		)
	)

	g_zxfer_runtime_artifact_cleanup_paths=""
	zxfer_runtime_artifact_path_is_registered \
		"$TEST_TMPDIR/zxfer.missing-stage"
	missing_identity_status=$?

	assertEquals "Runtime artifact registration should reject non-absolute paths." \
		1 "$relative_status"
	assertEquals "A contained runtime artifact must be one direct run-root child, never a nested path." \
		"direct=0 nested=1" "$child_statuses"
	assertEquals "Runtime artifact identity lookup should fail for an unregistered directory." \
		1 "$missing_identity_status"
	assertEquals "Missing runtime artifact identity lookup should clear the owner result channel." \
		"" "$g_zxfer_runtime_artifact_directory_identity_result"
}

test_runtime_artifact_registry_keeps_one_identity_path_pair_per_entry() {
	stage_file="$TEST_TMPDIR/zxfer.registry-file"
	stage_dir="$TEST_TMPDIR/.zxfer-registry-dir"
	glob_dir="$TEST_TMPDIR/.zxfer-registry-[glob]*"
	: >"$stage_file"
	mkdir -p "$stage_dir" "$glob_dir"
	output=$(
		(
			g_zxfer_runtime_artifact_cleanup_paths=""
			zxfer_register_runtime_artifact_path "$stage_file" || exit 90
			zxfer_register_runtime_artifact_path "$stage_dir" || exit 91
			zxfer_register_runtime_artifact_path "$glob_dir" || exit 92
			zxfer_register_runtime_artifact_path "$stage_file" || exit 93
			dir_identity=$(zxfer_get_path_device_inode "$stage_dir") || exit 94
			printf 'pairs=%s\n' "$(printf '%s\n' "$g_zxfer_runtime_artifact_cleanup_paths" | wc -l | tr -d ' ')"
			zxfer_runtime_artifact_path_is_registered "$stage_file"
			printf 'file_identity_status=%s result=<%s>\n' "$?" \
				"$g_zxfer_runtime_artifact_directory_identity_result"
			zxfer_runtime_artifact_path_is_registered "$stage_dir"
			[ "$g_zxfer_runtime_artifact_directory_identity_result" = "$dir_identity" ] &&
				printf '%s\n' 'dir_identity=current'
			zxfer_runtime_artifact_path_is_registered "${stage_dir#/}"
			printf 'relative_lookup=%s\n' "$?"
			zxfer_runtime_artifact_path_is_registered "$TEST_TMPDIR/.zxfer-registry-*"
			printf 'pattern_lookup=%s\n' "$?"
			zxfer_runtime_artifact_path_is_registered "$stage_dir" drop
			printf 'after_drop=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
			zxfer_runtime_artifact_path_is_registered "$glob_dir" drop
			zxfer_runtime_artifact_path_is_registered "$stage_file" drop
			printf 'emptied=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
		)
	)

	assertContains "A duplicate registration should not add a second pair." "$output" "pairs=6"
	assertContains "A registered file should publish - as its identity." \
		"$output" "file_identity_status=0 result=<->"
	assertContains "A registered directory should publish the identity it had at registration." \
		"$output" "dir_identity=current"
	assertContains "Registry lookups should reject relative paths." "$output" "relative_lookup=1"
	assertContains "Registry lookups should match paths literally, never as patterns." \
		"$output" "pattern_lookup=1"
	assertContains "Dropping one entry should keep every other pair in order." \
		"$output" "after_drop=<-
$stage_file
$(zxfer_get_path_device_inode "$glob_dir")
$glob_dir>"
	assertContains "Dropping every entry should empty the registry." "$output" "emptied=<>"
}

test_session_init_reinitializes_property_module_scratch_state_when_reinvoked() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			zxfer_init_dependency_tool_defaults() {
				:
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}

			zxfer_reset_session_state
			zxfer_init_session_environment

			g_zxfer_source_property_table="tank/src\tcompression=stale=local"
			g_zxfer_destination_property_table="backup/dst\tcompression=stale=local"
			g_zxfer_required_properties_result="stale-required"
			g_zxfer_adjusted_set_list="compression=lz4"
			g_zxfer_adjusted_inherit_list="mountpoint"
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result="compression=lz4=local"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_destination_property_tree_prefetch_state=2
			g_zxfer_unsupported_filesystem_properties="compression"
			g_zxfer_unsupported_volume_properties="volblocksize"

			zxfer_reset_session_state
			zxfer_init_session_environment

			printf 'required=<%s>\n' "$g_zxfer_required_properties_result"
			printf 'source_table=<%s>\n' "${g_zxfer_source_property_table:-}"
			printf 'destination_table=<%s>\n' "${g_zxfer_destination_property_table:-}"
			printf 'adjusted_set=<%s>\n' "$g_zxfer_adjusted_set_list"
			printf 'adjusted_inherit=<%s>\n' "$g_zxfer_adjusted_inherit_list"
			printf 'override_result=<%s>\n' "$g_zxfer_override_pvs_result"
			printf 'creation_result=<%s>\n' "$g_zxfer_creation_pvs_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'prefetch_state=%s\n' "$g_zxfer_destination_property_tree_prefetch_state"
			printf 'unsupported_fs=<%s>\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'unsupported_vol=<%s>\n' "$g_zxfer_unsupported_volume_properties"
		)
	)

	assertContains "Re-running session initialization should clear required-property scratch results." \
		"$output" "required=<>"
	assertContains "Re-running session initialization should clear the in-memory source property table." \
		"$output" "source_table=<>"
	assertContains "Re-running session initialization should clear the in-memory destination property table." \
		"$output" "destination_table=<>"
	assertContains "Re-running session initialization should clear adjusted set scratch state." \
		"$output" "adjusted_set=<>"
	assertContains "Re-running session initialization should clear adjusted inherit scratch state." \
		"$output" "adjusted_inherit=<>"
	assertContains "Re-running session initialization should clear derived override scratch state." \
		"$output" "override_result=<>"
	assertContains "Re-running session initialization should clear derived creation-property scratch state." \
		"$output" "creation_result=<>"
	assertContains "Re-running session initialization should clear remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "Re-running session initialization should rearm destination property prefetch state." \
		"$output" "prefetch_state=0"
	assertContains "Re-running session initialization should clear filesystem unsupported-property cache state." \
		"$output" "unsupported_fs=<>"
	assertContains "Re-running session initialization should clear volume unsupported-property cache state." \
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

# Purpose: Rewrite `trap` listing lines as "SIGNAL action", dropping the SIG
# prefix and quoting that differ between shells.
# Usage: trap | zxfer_test_normalize_trap_listing
zxfer_test_normalize_trap_listing() {
	awk -v quote="'" '{
		signal = $NF
		sub(/^SIG/, "", signal)
		action = $0
		sub(/^trap -- /, "", action)
		sub(/ [^ ]*$/, "", action)
		gsub(quote, "", action)
		print signal " " action
	}'
}

# EXIT runs the plain handler; each signal passes its 128+signo status. A
# signal ignored on entry (INT and QUIT in an async suite) cannot be trapped,
# so the expectation covers only the signals a probe trap shows this shell
# accepts.
test_zxfer_session_initialize_maps_each_signal_to_its_exit_status() {
	trappable=$(
		(
			trap 'zxfer_trap_probe' HUP INT QUIT TERM
			trap
		) | zxfer_test_normalize_trap_listing |
			awk '$2 == "zxfer_trap_probe" { print $1 }'
	)
	expected="EXIT zxfer_trap_exit"
	for signal in $trappable; do
		case $signal in
		HUP) expected="$expected
HUP zxfer_trap_exit 129" ;;
		INT) expected="$expected
INT zxfer_trap_exit 130" ;;
		QUIT) expected="$expected
QUIT zxfer_trap_exit 131" ;;
		TERM) expected="$expected
TERM zxfer_trap_exit 143" ;;
		esac
	done
	output=$(
		(
			zxfer_init_session_environment() {
				:
			}
			zxfer_session_initialize
			trap
			trap - EXIT HUP INT QUIT TERM
		) | zxfer_test_normalize_trap_listing | grep ' zxfer_trap_exit' | sort
	)

	assertContains "TERM must be trappable in the test shell." "$trappable" "TERM"
	assertEquals "Session startup should route EXIT and each signal through zxfer_trap_exit with its exit status." \
		"$(printf '%s\n' "$expected" | sort)" "$output"
}

test_zxfer_init_endpoint_execution_context_reports_remote_decompress_resolution_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			g_option_z_compress=1
			g_cmd_decompress="zstd -d"
			g_cmd_zfs="/sbin/zfs"
			zxfer_get_os() {
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="/remote/bin/$2"
			}
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result="decompress lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_init_endpoint_execution_context target
		)
	)
	status=$?

	assertEquals "Target execution-context initialization should fail closed when the remote decompressor cannot be resolved safely." \
		1 "$status"
	assertContains "Remote decompressor resolution failures should preserve the dependency error." \
		"$output" "decompress lookup failed"
}
