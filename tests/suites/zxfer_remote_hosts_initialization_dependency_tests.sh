#!/bin/sh
# Remote-host initialization, CLI parsing, dependency, and compression behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_zxfer_reset_remote_host_state_resets_capability_and_resolved_tool_state() {
	result=$(
		(
			g_cmd_zfs="/stub/zfs"
			g_origin_remote_capabilities_response="dirty-origin"
			g_origin_remote_capabilities_parsed_identity="dirty-origin-identity"
			g_origin_remote_capabilities_os="DirtyOriginOS"
			g_target_remote_capabilities_response="dirty-target"
			g_target_remote_capabilities_parsed_identity="dirty-target-identity"
			g_target_remote_capabilities_tool_records="dirty-target-tools"
			g_zxfer_remote_probe_capture_failed=1
			g_origin_cmd_zfs="/dirty/origin-zfs"

			zxfer_reset_remote_host_state
			printf 'origin=<%s>\n' "$g_origin_remote_capabilities_response"
			printf 'origin_parsed=<%s>\n' "$g_origin_remote_capabilities_parsed_identity"
			printf 'origin_os=<%s>\n' "$g_origin_remote_capabilities_os"
			printf 'target=<%s>\n' "$g_target_remote_capabilities_response"
			printf 'target_parsed=<%s>\n' "$g_target_remote_capabilities_parsed_identity"
			printf 'target_tools=<%s>\n' "$g_target_remote_capabilities_tool_records"
			printf 'capture_failed=%s\n' "$g_zxfer_remote_probe_capture_failed"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		)
	)

	assertContains "Remote-host reset should clear origin capability payloads." \
		"$result" "origin=<>"
	assertContains "Remote-host reset should clear origin parsed capability identities." \
		"$result" "origin_parsed=<>"
	assertContains "Remote-host reset should clear origin parsed operating-system state." \
		"$result" "origin_os=<>"
	assertContains "Remote-host reset should clear target capability payloads." \
		"$result" "target=<>"
	assertContains "Remote-host reset should clear target parsed capability identities." \
		"$result" "target_parsed=<>"
	assertContains "Remote-host reset should clear target parsed tool records." \
		"$result" "target_tools=<>"
	assertContains "Remote-host reset should clear remote capture failure state." \
		"$result" "capture_failed=0"
	assertContains "Remote-host reset should restore origin zfs to the local default." \
		"$result" "origin_zfs=/stub/zfs"
}

test_init_globals_initializes_defaults_and_temp_files() {
	real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	result=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_init_globals.counter"
			printf '%s\n' 0 >"$counter_file"
			g_zxfer_services_to_restart="stale-service"
			g_zxfer_property_table_lookup_result="stale-lookup"
			zxfer_get_temp_file() {
				temp_index=$(cat "$counter_file")
				temp_index=$((temp_index + 1))
				printf '%s\n' "$temp_index" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/tmp.$temp_index"
			}
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					eval "$1=$(command -v awk 2>/dev/null || printf '%s\n' awk)"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				[ -n "${g_cmd_ssh:-}" ]
			}
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/backup_root"
			zxfer_init_globals
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'zfs=%s\n' "$g_cmd_zfs"
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'backup=%s\n' "$g_backup_storage_root"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'tmp1=%s\n' "$g_zxfer_snapshot_delete_source_identities_file"
			printf 'tmp2=%s\n' "$g_zxfer_snapshot_delete_destination_identities_file"
			printf 'tmp3=%s\n' "$g_zxfer_snapshot_delete_difference_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'table_lookup=<%s>\n' "$g_zxfer_property_table_lookup_result"
		)
	)

	assertContains "zxfer_init_globals should resolve awk through the helper." "$result" "awk=$real_awk"
	assertContains "zxfer_init_globals should resolve zfs through the helper." "$result" "zfs=/stub/zfs"
	assertContains "zxfer_init_globals should defer ssh resolution until remote transport is actually needed." "$result" "ssh="
	assertContains "zxfer_init_globals should honor ZXFER_BACKUP_DIR when set." "$result" "backup=$TEST_TMPDIR/backup_root"
	assertContains "zxfer_init_globals should leave control-socket support disabled until ssh is resolved on demand." "$result" "control=0"
	assertContains "Yield iterations should default to 1." "$result" "yield=1"
	assertContains "Delete source temp file path should stay empty until delete planning needs it." "$result" "tmp1="
	assertContains "Delete destination temp file path should stay empty until delete planning needs it." "$result" "tmp2="
	assertContains "Delete diff temp file path should stay empty until delete planning needs it." "$result" "tmp3="
	assertContains "Runtime init should clear stale service restart state." "$result" "restart=<>"
	assertContains "Runtime init should clear stale property-table lookup state." "$result" "table_lookup=<>"
}

test_prepare_remote_host_connections_resolves_ssh_on_demand() {
	log="$TEST_TMPDIR/prepare_remote_hosts_resolve_ssh.log"
	: >"$log"

	result=$(
		(
			zxfer_find_required_tool() {
				if [ "$1" = "ssh" ]; then
					printf '%s\n' "$FAKE_SSH_BIN"
					return 0
				fi
				printf '%s\n' "/stub/$1"
			}
			zxfer_ssh_supports_control_sockets() {
				[ "${g_cmd_ssh:-}" = "$FAKE_SSH_BIN" ]
			}
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			g_cmd_ssh=""
			g_option_O_origin_host="origin.example pfexec"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			zxfer_prepare_remote_host_connections
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'lzfs=%s\n' "$g_LZFS"
		)
	)

	assertContains "Remote preparation should resolve ssh on demand when a remote host is configured." \
		"$result" "ssh=$FAKE_SSH_BIN"
	assertContains "Remote preparation should refresh control-socket capability after lazy ssh resolution." \
		"$result" "control=1"
	assertNotContains "Remote capability preparation should not open an SSH control socket before replication work exists." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Origin capability preload should still run after lazy ssh resolution." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Origin zfs rendering should still refresh after lazy ssh resolution." \
		"$result" "lzfs=/remote/origin/zfs"
}

test_zxfer_local_ssh_resolution_helpers_cover_success_and_failure_paths() {
	output=$(
		(
			set +e
			g_cmd_ssh=""
			zxfer_find_required_tool() {
				if [ "$1" = "ssh" ]; then
					printf '%s\n' "$FAKE_SSH_BIN"
					return 0
				fi
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_success=%s:%s:%s\n' "$?" "$g_cmd_ssh" "$g_zxfer_resolved_local_ssh_command_result"

			g_cmd_ssh=""
			zxfer_find_required_tool() {
				printf '%s\n' "missing ssh"
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_failure=%s:%s\n' "$?" "$g_zxfer_resolved_local_ssh_command_result"
		)
	)

	assertContains "Lazy local ssh resolution should cache the resolved ssh helper on success." \
		"$output" "ensure_success=0:$FAKE_SSH_BIN:$FAKE_SSH_BIN"
	assertContains "Lazy local ssh resolution should preserve the dependency diagnostic when ssh lookup fails." \
		"$output" "ensure_failure=1:missing ssh"
}

test_init_globals_rejects_relative_backup_dir_override() {
	set +e
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_BACKUP_DIR="relative-backups"
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					eval "$1=$(command -v awk 2>/dev/null || printf '%s\n' awk)"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_init_globals
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Relative ZXFER_BACKUP_DIR overrides should abort startup." 1 "$status"
	assertContains "Startup should report that ZXFER_BACKUP_DIR must be absolute." \
		"$output" "ZXFER_BACKUP_DIR must be an absolute path"
}

test_zxfer_find_required_tool_reports_missing_dependency() {
	empty_path="$TEST_TMPDIR/empty_path"
	mkdir -p "$empty_path"
	g_zxfer_secure_path="$empty_path"
	g_zxfer_dependency_path="$empty_path"

	set +e
	result=$(zxfer_find_required_tool definitely_missing "missing-tool")
	status=$?

	assertEquals "Missing dependencies should fail lookup." 1 "$status"
	assertEquals "Missing dependencies should mention the secure PATH guidance." \
		"Required dependency \"missing-tool\" not found in secure PATH ($empty_path). Set ZXFER_SECURE_PATH or install the binary." \
		"$result"
}

test_zxfer_find_required_tool_rejects_relative_resolution() {
	set +e
	result=$(
		(
			mocktool() {
				:
			}
			g_zxfer_secure_path="$ZXFER_DEFAULT_SECURE_PATH"
			g_zxfer_dependency_path="$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_find_required_tool mocktool "mocktool"
		)
	)
	status=$?

	assertEquals "Relative command -v results should be rejected." 1 "$status"
	assertEquals "Relative paths should be rejected explicitly." \
		"Required dependency \"mocktool\" resolved to \"mocktool\", but zxfer requires an absolute path." \
		"$result"
}

test_zxfer_find_required_tool_returns_absolute_path_from_secure_path() {
	tool_dir="$TEST_TMPDIR/required_tool_path"
	mkdir -p "$tool_dir"
	cat >"$tool_dir/mocktool" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$tool_dir/mocktool"
	g_zxfer_secure_path="$tool_dir"
	g_zxfer_dependency_path="$tool_dir"

	result=$(zxfer_find_required_tool mocktool "mocktool")

	assertEquals "Required tool lookup should return the resolved absolute path from the secure PATH." \
		"$tool_dir/mocktool" "$result"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace() {
	tab=$(printf '\t')

	set +e
	result=$(zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool")
	status=$?

	assertEquals "Resolved tool paths with control whitespace should be rejected." 1 "$status"
	assertContains "Rejected tool paths should explain the control-whitespace requirement." \
		"$result" "single-line absolute path without control whitespace"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace_with_scope() {
	tab=$(printf '\t')

	set +e
	result=$(zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool" "host origin.example")
	status=$?

	assertEquals "Scoped control-whitespace tool paths should be rejected." 1 "$status"
	assertContains "Scoped control-whitespace failures should mention the host scope." \
		"$result" "Required dependency \"mocktool\" on host origin.example resolved to"
}

test_zxfer_assign_required_tool_marks_dependency_failures() {
	set +e
	output=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_assign_required_tool g_cmd_cat mocktool "mocktool"
		)
	)
	status=$?

	assertEquals "zxfer_assign_required_tool should abort when lookup fails." 1 "$status"
	assertContains "Dependency lookup failures should be classified correctly." "$output" "class=dependency"
	assertContains "Dependency lookup failures should preserve the lookup message." "$output" "msg=lookup failed"
}

test_zxfer_assign_required_tool_sets_target_variable_on_success() {
	result=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "/opt/mock/mocktool"
			}
			g_cmd_cat=""
			zxfer_assign_required_tool g_cmd_cat mocktool "mocktool"
			printf '%s\n' "$g_cmd_cat"
		)
	)

	assertEquals "Successful tool assignment should populate the requested variable." "/opt/mock/mocktool" "$result"
}

test_init_globals_rejects_control_whitespace_in_optional_parallel_path() {
	tab=$(printf '\t')
	parallel_dir="$TEST_TMPDIR/parallel${tab}bin"
	mkdir -p "$parallel_dir"
	cat >"$parallel_dir/parallel" <<'EOF'
#!/bin/sh
printf '%s\n' "parallel (fake)"
exit 0
EOF
	chmod +x "$parallel_dir/parallel"

	set +e
	output=$(
		(
			ZXFER_SECURE_PATH="$parallel_dir:/usr/bin:/bin:/usr/sbin:/sbin"
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					# shellcheck disable=SC2034
					l_real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
					eval "$1=\$l_real_awk"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_get_temp_file() {
				printf '%s\n' "$TEST_TMPDIR/tmp"
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_init_globals
		)
	)
	status=$?

	assertEquals "zxfer_init_globals should fail when optional parallel resolves to a path with control whitespace." 1 "$status"
	assertContains "Invalid optional parallel resolutions should be classified as dependency failures." \
		"$output" "class=dependency"
	assertContains "Invalid optional parallel resolutions should explain the path validation failure." \
		"$output" "single-line absolute path without control whitespace"
}

test_extract_snapshot_identity_returns_empty_for_non_snapshot_path() {
	result=$(zxfer_extract_snapshot_identity "tank/src")

	assertEquals "Snapshot identities should be empty when the record does not include a snapshot suffix." \
		"" "$result"
}

test_extract_snapshot_dataset_and_guid_detection_helpers() {
	assertEquals "Snapshot dataset extraction should strip the snapshot suffix from guid-bearing records." \
		"tank/src" "$(zxfer_extract_snapshot_dataset "tank/src@snap1	123")"
	assertEquals "Snapshot dataset extraction should return empty for non-snapshot records." \
		"" "$(zxfer_extract_snapshot_dataset "tank/src")"
	assertTrue "Guid detection should report true when a snapshot record includes a guid field." \
		'zxfer_snapshot_record_list_contains_guid "tank/src@snap1	123"'
	assertFalse "Guid detection should report false for name-only snapshot records." \
		'zxfer_snapshot_record_list_contains_guid "tank/src@snap1"'
}

test_zxfer_reverse_snapshot_record_list_and_name_overlap_helpers() {
	reversed=$(zxfer_reverse_snapshot_record_list "tank/src@snap1	111
tank/src@snap2	222
tank/src@snap3	333")

	assertEquals "Snapshot-record reversal should preserve full records while reversing their order." \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" "$reversed"

	set +e
	zxfer_snapshot_record_lists_share_snapshot_name "tank/src@snap2
tank/src@snap1" "backup/dst@snap9
backup/dst@snap1"
	status=$?
	assertEquals "Snapshot-name overlap detection should succeed when both sides share any snapshot name." \
		0 "$status"

	zxfer_snapshot_record_lists_share_snapshot_name "tank/src@snap2
tank/src@snap1" "backup/dst@other"
	status=$?
	assertEquals "Snapshot-name overlap detection should fail when the lists do not share any snapshot name." \
		1 "$status"
}

test_zxfer_filter_snapshot_identity_records_to_reference_paths_preserves_identity_order() {
	result=$(zxfer_filter_snapshot_identity_records_to_reference_paths \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" \
		"tank/src@snap2
tank/src@snap1")

	assertEquals "Reference-path filtering should keep only matching identity records in their original identity-record order." \
		"tank/src@snap2	222
tank/src@snap1	111" "$result"
}

test_zxfer_get_source_snapshot_identity_records_for_dataset_reverses_creation_order() {
	result=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' \
					"tank/src@snap1	111" \
					"tank/src@snap2	222" \
					"tank/src@snap3	333"
			}

			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src"
		)
	)

	assertEquals "Source identity-record retrieval should reverse creation-ordered zfs output into newest-first order." \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" "$result"
}

test_zxfer_get_destination_snapshot_identity_records_for_dataset_filters_descendants() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' \
					"backup/dst@snap1	111" \
					"backup/dst/child@snap1	211" \
					"backup/dst@snap2	222"
			}

			zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst"
		)
	)

	assertEquals "Destination identity-record retrieval should keep only the exact dataset snapshots and drop descendant records." \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$result"
}

test_zxfer_get_snapshot_identity_records_for_dataset_dispatches_and_filters_reference_records() {
	result=$(
		(
			zxfer_get_source_snapshot_identity_records_for_dataset() {
				printf '%s\n' \
					"tank/src@snap3	333" \
					"tank/src@snap2	222" \
					"tank/src@snap1	111"
			}
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n' \
					"backup/dst@snap2	222" \
					"backup/dst@snap1	111"
			}

			zxfer_get_snapshot_identity_records_for_dataset source "tank/src" "tank/src@snap2
tank/src@snap1"
		)
	)

	assertEquals "Generic identity-record lookup should dispatch to the requested side and honor reference-path filtering." \
		"tank/src@snap2	222
tank/src@snap1	111" "$result"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset invalid "tank/src"
		)
	)
	status=$?

	assertEquals "Generic identity-record lookup should reject unknown lookup sides." 1 "$status"
	assertEquals "Rejected identity-record lookups should not emit an output payload." "" "$output"
}

test_zxfer_snapshot_identity_record_helpers_report_lookup_failures_and_destination_dispatch() {
	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				return 1
			}

			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src"
		)
	)
	status=$?
	assertEquals "Source identity-record lookup should fail cleanly when the zfs query fails." 1 "$status"
	assertEquals "Failed source identity lookups should not emit a payload." "" "$output"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}

			zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst"
		)
	)
	status=$?
	assertEquals "Destination identity-record lookup should fail cleanly when the zfs query fails." 1 "$status"
	assertEquals "Failed destination identity lookups should not emit a payload." "" "$output"

	output=$(
		(
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n' "backup/dst@snap2	222"
			}

			zxfer_get_snapshot_identity_records_for_dataset destination "backup/dst"
		)
	)
	status=$?
	assertEquals "Generic identity-record lookup should support the destination side without requiring reference filters." 0 "$status"
	assertEquals "Destination-side identity dispatch should return the destination helper payload unchanged when no reference filter is supplied." \
		"backup/dst@snap2	222" "$output"
}

test_read_command_line_switches_sets_options_and_remote_paths() {
	log="$TEST_TMPDIR/read_switches.log"
	: >"$log"
	result=$(
		(
			zxfer_refresh_compression_commands() {
				printf 'refresh\n' >>"$log"
				g_cmd_compress_safe="zstd -9"
				g_cmd_decompress_safe="zstd -d"
			}
			g_ssh_supports_control_sockets=1
			g_cmd_zfs="/sbin/zfs"
			g_test_max_yield_iterations=8
			OPTIND=1
			zxfer_read_command_line_switches \
				-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
				-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
				-N "tank/nonrecursive" -o "atime=off" -O "origin.example pfexec" \
				-P -R "tank/src" -s -T "target.example doas" -U -v -V -w \
				-x "child" -Y -z -Z "zstd -9"
			printf 'origin=%s\n' "$g_option_O_origin_host"
			printf 'target=%s\n' "$g_option_T_target_host"
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'compress=%s\n' "$g_cmd_compress"
			printf 'props=%s\n' "$g_option_P_transfer_property"
			printf 'verbose=%s/%s\n' "$g_option_v_verbose" "$g_option_V_very_verbose"
		)
	)

	assertContains "Origin host should be recorded from -O." "$result" "origin=origin.example pfexec"
	assertContains "Target host should be recorded from -T." "$result" "target=target.example doas"
	assertContains "Origin zfs spec should remain the resolved zfs path until remote execution is rendered." "$result" "lzfs=/sbin/zfs"
	assertContains "Target zfs spec should remain the resolved zfs path until remote execution is rendered." "$result" "rzfs=/sbin/zfs"
	assertContains "Parallel job count should come from -j." "$result" "jobs=4"
	assertContains "Yield iterations should expand to the max when -Y is set." "$result" "yield=8"
	assertContains "Custom compression should be recorded from -Z." "$result" "compress=zstd -9"
	assertContains "Property transfer should be enabled by -e/-k/-m/-P." "$result" "props=1"
	assertContains "Very verbose mode should imply verbose mode." "$result" "verbose=1/1"
	assertContains "Compression refresh should run after parsing options." "$(cat "$log")" "refresh"
}

test_zxfer_refresh_remote_zfs_commands_rejects_shell_quoted_host_specs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_O_origin_host='origin.example "pfexec -u zfs"'
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_refresh_remote_zfs_commands
		)
	)
	status=$?
	set -e

	assertEquals "Remote host-spec refresh should fail closed when the configured host spec relies on shell quoting." \
		2 "$status"
	assertContains "Rejected remote host specs should explain the literal-token requirement." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_prepare_remote_host_connections_preloads_capabilities_without_opening_control_sockets() {
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	now_counter_file="$TEST_TMPDIR/prepare_remote_hosts.now.counter"
	: >"$log"
	printf '%s\n' 0 >"$now_counter_file"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 1000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 1250
				fi
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertNotContains "Origin control socket setup should be deferred until replication work exists." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertNotContains "Target control socket setup should be deferred until replication work exists." \
		"$(cat "$log")" "setup target.example doas target"
	assertContains "Origin capability discovery should be preloaded during remote preparation." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Target capability discovery should be preloaded during remote preparation." \
		"$(cat "$log")" "preload target.example doas destination"
	assertContains "Origin zfs spec should refresh to the resolved origin helper path." \
		"$result" "lzfs=/remote/origin/zfs"
	assertContains "Target zfs spec should refresh to the resolved target helper path." \
		"$result" "rzfs=/remote/target/zfs"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_ssh_control_sockets_for_active_hosts_sets_up_control_sockets_after_validation() {
	log="$TEST_TMPDIR/prepare_active_control_sockets.log"
	now_counter_file="$TEST_TMPDIR/prepare_active_control_sockets.now.counter"
	: >"$log"
	printf '%s\n' 0 >"$now_counter_file"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
				if [ "$2" = "origin" ]; then
					g_ssh_origin_control_socket="/tmp/origin.sock"
				elif [ "$2" = "target" ]; then
					g_ssh_target_control_socket="/tmp/target.sock"
				fi
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 2000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 2250
				fi
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertContains "Origin control socket setup should happen once replication work exists." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Target control socket setup should happen once replication work exists." \
		"$(cat "$log")" "setup target.example doas target"
	assertEquals "Active control-socket preparation should not replace sockets that are already ready." \
		"2" "$(wc -l <"$log" | tr -d '[:space:]')"
	assertContains "Origin zfs spec should refresh after deferred socket setup." \
		"$result" "lzfs=/remote/origin/zfs"
	assertContains "Target zfs spec should refresh after deferred socket setup." \
		"$result" "rzfs=/remote/target/zfs"
	assertContains "Very-verbose deferred socket preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_ssh_control_sockets_for_active_hosts_logs_when_control_sockets_are_unavailable() {
	log="$TEST_TMPDIR/prepare_remote_hosts_no_mux.log"
	: >"$log"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=0
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertContains "Origin active socket preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for origin host."
	assertContains "Target active socket preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for target host."
	assertEquals "Deferred socket setup should not preload remote capabilities." "" "$(cat "$log")"
	assertContains "Remote zfs specs should still refresh even without control socket support." \
		"$output" "lzfs=/remote/origin/zfs"
	assertContains "Remote zfs specs should still refresh target commands even without control socket support." \
		"$output" "rzfs=/remote/target/zfs"
}

test_prepare_remote_host_connections_surfaces_verbose_preload_failures() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_setup_ssh_control_socket() {
				:
			}
			zxfer_preload_remote_host_capabilities() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			g_option_v_verbose=1
			g_option_O_origin_host="origin.example pfexec"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
		) 2>&1
	)

	assertContains "Verbose remote preparation should surface opportunistic preload diagnostics instead of discarding them." \
		"$output" "Host key verification failed."
}

test_prepare_remote_host_connections_skips_live_setup_in_dry_run() {
	log="$TEST_TMPDIR/prepare_remote_hosts_dry_run.log"
	: >"$log"

	output=$(
		(
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertEquals "Dry-run remote preparation should not open control sockets or preload capabilities." \
		"" "$(cat "$log")"
	assertContains "Dry-run remote preparation should explain that origin ssh preflight is skipped." \
		"$output" "Dry run: skipping ssh control-socket setup and remote capability preload for origin host."
	assertContains "Dry-run remote preparation should explain that target ssh preflight is skipped." \
		"$output" "Dry run: skipping ssh control-socket setup and remote capability preload for target host."
	assertContains "Dry-run remote preparation should still refresh the origin zfs render command." \
		"$output" "lzfs=/remote/origin/zfs"
	assertContains "Dry-run remote preparation should still refresh the target zfs render command." \
		"$output" "rzfs=/remote/target/zfs"
}

test_read_command_line_switches_sets_flags_in_current_shell() {
	OPTIND=1
	g_cmd_ssh="/usr/bin/ssh"
	g_cmd_zfs="/sbin/zfs"
	g_test_max_yield_iterations=9
	g_ssh_supports_control_sockets=0
	zxfer_refresh_compression_commands() {
		:
	}

	zxfer_read_command_line_switches \
		-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
		-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
		-N "tank/nonrecursive" -o "atime=off" -V \
		-O "origin.example pfexec" -P -R "tank/src" -s \
		-T "target.example doas" -U -w -x "child" -Y -z -Z "zstd -9"

	assertEquals "Beep-always should be enabled by -b." "1" "$g_option_b_beep_always"
	assertEquals "Beep-on-success should be enabled by -B." "1" "$g_option_B_beep_on_success"
	assertEquals "Service list should be captured from -c." "svc:/network/nfs/server" "$g_option_c_services"
	assertEquals "Snapshot deletion should be enabled by -d." "1" "$g_option_d_delete_destination_snapshots"
	assertEquals "Progress display command should be captured from -D." "pv -N %%title%%" "$g_option_D_display_progress_bar"
	assertEquals "Grandfather protection should be captured from -g." "7" "$g_option_g_grandfather_protection"
	assertEquals "Ignore-properties list should be captured from -I." "mountpoint" "$g_option_I_ignore_properties"
	assertEquals "Parallel job count should be captured from -j." "4" "$g_option_j_jobs"
	assertEquals "Nonrecursive source should be captured from -N." "tank/nonrecursive" "$g_option_N_nonrecursive"
	assertEquals "Override property should be captured from -o." "atime=off" "$g_option_o_override_property"
	# zxfer_read_command_line_switches runs in the current shell here; the SC2031
	# warning is triggered by separate subshell-based coverage elsewhere.
	# shellcheck disable=SC2031
	assertEquals "Origin host should be captured from -O." "origin.example pfexec" "$g_option_O_origin_host"
	assertEquals "Recursive source should be captured from -R." "tank/src" "$g_option_R_recursive"
	# shellcheck disable=SC2031
	assertEquals "Target host should be captured from -T." "target.example doas" "$g_option_T_target_host"
	assertEquals "Exclude list should be captured from -x." "child" "$g_option_x_exclude_datasets"
	assertEquals "Very-verbose mode should imply verbose mode." "1" "$g_option_v_verbose"
	assertEquals "Very-verbose mode should be enabled by -V." "1" "$g_option_V_very_verbose"
	assertEquals "Raw-send mode should be enabled by -w." "1" "$g_option_w_raw_send"
	assertEquals "Unsupported-property skipping should be enabled by -U." "1" "$g_option_U_skip_unsupported_properties"
	assertEquals "Compression should be enabled by -z/-Z." "1" "$g_option_z_compress"
	assertEquals "Yield iterations should expand to the configured maximum." "9" "$g_option_Y_yield_iterations"
	assertEquals "The parser should preserve the custom compression command from -Z." "zstd -9" "$g_cmd_compress"
	assertEquals "Property transfer should be enabled by property-affecting switches." "1" "$g_option_P_transfer_property"
	assertEquals "Origin zfs spec should remain the resolved zfs path after parsing." \
		"/sbin/zfs" "$g_LZFS"
	assertEquals "Target zfs spec should remain the resolved zfs path after parsing." \
		"/sbin/zfs" "$g_RZFS"

	unset -f zxfer_refresh_compression_commands
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"
}

test_read_command_line_switches_rejects_invalid_option() {
	set +e
	output=$(
		(
			zxfer_refresh_compression_commands() {
				:
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			OPTIND=1
			zxfer_read_command_line_switches -Q 2>/dev/null
		)
	)
	status=$?

	assertEquals "Invalid options should exit with usage status." 2 "$status"
	assertContains "Invalid options should use the generic usage error." "$output" "Invalid option provided."
}

test_read_command_line_switches_exits_zero_for_help() {
	set +e
	output=$(
		(
			zxfer_usage() {
				printf '%s\n' "usage output"
			}
			OPTIND=1
			zxfer_read_command_line_switches -h
			printf '%s\n' "after-help"
		)
	)
	status=$?

	assertEquals "The help switch should exit successfully." 0 "$status"
	assertEquals "The help switch should print usage and stop parsing immediately." "usage output" "$output"
}

test_consistency_check_rejects_non_numeric_jobs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=abc
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Non-numeric job counts should fail validation." 2 "$status"
	assertContains "The validation error should mention the invalid job count." \
		"$output" "The -j option requires a positive integer job count"
}

test_consistency_check_rejects_zero_jobs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=0
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Zero job counts should fail validation." 2 "$status"
	assertContains "The validation error should require at least one job." \
		"$output" "requires a job count of at least 1"
}

test_consistency_check_rejects_remote_migration_conflicts() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_O_origin_host="origin.example"
			g_option_m_migrate=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Remote migration should be rejected." 2 "$status"
	assertContains "Remote migration conflicts should use the documented error." \
		"$output" "You cannot migrate to or from a remote host."
}

test_consistency_check_rejects_compression_without_remote_host() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_z_compress=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Compression without -O/-T should be rejected." 2 "$status"
	assertContains "Compression validation should point to the missing remote host." \
		"$output" "-z option can only be used with -O or -T option"
}

test_init_variables_uses_gawk_on_sunos_when_available() {
	gawk_dir="$TEST_TMPDIR/gawk_path"
	mkdir -p "$gawk_dir"
	cat >"$gawk_dir/gawk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$gawk_dir/gawk"

	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_dependency_path="$gawk_dir"
			zxfer_init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "FreeBSD"
			}
			zxfer_assign_required_tool() {
				if [ "$2" = "cat" ]; then
					eval "$1=/bin/cat"
				else
					eval "$1=/stub/$2"
				fi
			}
			g_option_e_restore_property_mode=1
			zxfer_init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat through the required-tool helper." \
		"$result" "cat=/bin/cat"
}

test_refresh_compression_commands_resolves_local_helpers_when_enabled() {
	result=$(
		(
			zxfer_find_required_tool() {
				if [ "$1" = "zstd" ]; then
					printf '%s\n' "/secure/bin/zstd"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
			printf 'compress=%s\n' "$g_cmd_compress_safe"
			printf 'decompress=%s\n' "$g_cmd_decompress_safe"
		)
	)

	assertContains "Enabled compression should resolve the compressor head token through the secure local path." \
		"$result" "compress='/secure/bin/zstd' '-T0' '-9'"
	assertContains "Enabled compression should resolve the decompressor head token through the secure local path." \
		"$result" "decompress='/secure/bin/zstd' '-d'"
}

test_zxfer_resolve_remote_cli_command_safe_resolves_first_token_and_preserves_args() {
	result=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "/remote/bin/zstd"
			}
			zxfer_resolve_remote_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source
		)
	)

	assertEquals "Remote CLI command resolution should replace only the first token and keep the remaining arguments intact." \
		"'/remote/bin/zstd' '-T0' '-9'" "$result"
}

test_zxfer_resolve_remote_cli_command_safe_uses_cached_capability_tool_for_generic_heads() {
	result_file="$TEST_TMPDIR/resolve_remote_cli_cached_generic.out"
	probe_file="$TEST_TMPDIR/resolve_remote_cli_cached_generic.probes"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_cached_generic.direct"

	(
		g_option_O_origin_host="origin.example"
		g_option_j_jobs=4
		g_option_z_compress=1
		g_cmd_compress="zstd -T0 -9"
		g_zxfer_profile_remote_cli_tool_direct_probes=0
		zxfer_ensure_remote_host_capabilities() {
			zxfer_test_accept_remote_capability_response 'ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	zstd	0	/remote/bin/zstd
end'
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			return 1
		}
		zxfer_resolve_remote_cli_command_safe \
			"origin.example" "zstd -T0 -9" "compression command" source >"$result_file"
		printf '%s\n' "${g_zxfer_profile_remote_cli_tool_direct_probes:-0}" >"$probe_file"
	)
	status=$?

	assertEquals "Remote CLI command resolution should reuse cached capability tool records for generic helper heads." \
		0 "$status"
	assertEquals "Cached generic helper resolution should replace only the first command token." \
		"'/remote/bin/zstd' '-T0' '-9'" "$(cat "$result_file")"
	assertEquals "Cached generic helper resolution should not fall back to a direct remote helper probe when the capability payload already advertises the tool." \
		"" "$(cat "$direct_log" 2>/dev/null)"
	assertEquals "Cached generic helper resolution should leave the direct-probe counter at zero when no probe is needed." \
		"0" "$(cat "$probe_file")"
}

test_zxfer_resolve_remote_cli_tool_prefers_prewarmed_host_scope_for_generic_heads() {
	result_file="$TEST_TMPDIR/resolve_remote_cli_host_scope.out"
	log_file="$TEST_TMPDIR/resolve_remote_cli_host_scope.log"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_host_scope.direct"

	(
		LOG_PATH="$log_file"
		g_option_O_origin_host="origin.example"
		g_option_j_jobs=4
		g_option_e_restore_property_mode=1
		g_option_z_compress=1
		g_cmd_compress="zstd -T0 -9"
		zxfer_ensure_remote_host_capabilities() {
			printf '%s\n' "${3:-}" >"$LOG_PATH"
			zxfer_test_accept_remote_capability_response 'ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
tool	zstd	0	/remote/bin/zstd
end'
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			return 1
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$result_file"
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should succeed when the broader host scope already advertises the helper." \
		0 "$status"
	assertEquals "Generic remote CLI tool resolution should return the parsed helper path from the broader host scope." \
		"/remote/bin/zstd" "$(cat "$result_file")"
	assertContains "Generic remote CLI tool resolution should reuse the broader host-scoped preload identity for parallel when -j is active." \
		"$(cat "$log_file")" "parallel"
	assertContains "Generic remote CLI tool resolution should reuse the broader host-scoped preload identity for cat when restore-property mode is active." \
		"$(cat "$log_file")" "cat"
	assertContains "Generic remote CLI tool resolution should still include the requested generic helper in the reused host scope." \
		"$(cat "$log_file")" "zstd"
	assertEquals "Generic remote CLI tool resolution should not reopen a direct probe when the broader host scope already advertises the helper." \
		"" "$(cat "$direct_log" 2>/dev/null)"
}

test_zxfer_resolve_local_cli_command_safe_rejects_blank_commands_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_local_cli_blank.out"

	(
		zxfer_resolve_local_cli_command_safe "   " "compression command" >"$output_file"
	)
	status=$?

	assertEquals "Blank local CLI commands should be rejected." 1 "$status"
	assertContains "Blank local CLI command failures should use the documented validation message." \
		"$(cat "$output_file")" "Required dependency \"compression command\" must not be empty or whitespace-only."
}

test_zxfer_resolve_local_cli_command_safe_surfaces_lookup_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_local_cli_lookup_failure.out"

	(
		zxfer_find_required_tool() {
			printf '%s\n' "missing helper"
			return 1
		}
		zxfer_resolve_local_cli_command_safe "zstd -T0 -9" "compression command" >"$output_file"
	)
	status=$?

	assertEquals "Local CLI command resolution should fail when the head token cannot be resolved." 1 "$status"
	assertEquals "Local CLI command resolution should surface the dependency lookup failure verbatim." \
		"missing helper" "$(cat "$output_file")"
}

test_zxfer_resolve_remote_cli_tool_delegates_known_tools_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_tool_known.out"
	log_file="$TEST_TMPDIR/resolve_remote_cli_tool_known.log"

	(
		zxfer_resolve_remote_required_tool() {
			printf '%s:%s:%s:%s\n' "$1" "$2" "$3" "$4" >"$log_file"
			printf '%s\n' "/remote/bin/zfs"
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zfs" "source zfs" source >"$output_file"
	)
	status=$?

	assertEquals "Known remote CLI tools should delegate to zxfer_resolve_remote_required_tool." 0 "$status"
	assertEquals "Known remote CLI tool delegation should preserve the host, tool, label, and profile side." \
		"origin.example:zfs:source zfs:source" "$(cat "$log_file")"
	assertEquals "Known remote CLI tool delegation should return the resolved remote helper path." \
		"/remote/bin/zfs" "$(cat "$output_file")"
}

test_zxfer_resolve_remote_cli_tool_reports_missing_and_query_failures_in_current_shell() {
	missing_output="$TEST_TMPDIR/resolve_remote_cli_tool_missing.out"
	missing_log="$TEST_TMPDIR/resolve_remote_cli_tool_missing.log"
	error_output="$TEST_TMPDIR/resolve_remote_cli_tool_error.out"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			printf '%s\n' "$2" >"$missing_log"
			return 10
		}
		g_zxfer_dependency_path="/stale/secure/path"
		ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$missing_output"
	)
	missing_status=$?

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			return 77
		}
		g_zxfer_dependency_path="/secure/bin"
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$error_output"
	)
	error_status=$?

	assertEquals "Missing remote CLI tools should return failure." 1 "$missing_status"
	assertContains "Missing remote CLI tool probes should refresh the secure PATH from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$(cat "$missing_log")" "/fresh/secure/path:/usr/bin"
	assertNotContains "Missing remote CLI tool probes should not keep using a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$(cat "$missing_log")" "/stale/secure/path"
	assertContains "Missing remote CLI tools should use the documented secure-PATH guidance." \
		"$(cat "$missing_output")" "Required dependency \"compression command\" not found on host origin.example in secure PATH (/fresh/secure/path:/usr/bin)."
	assertEquals "Remote CLI probe errors should return failure." 1 "$error_status"
	assertContains "Remote CLI probe errors should use the documented generic failure message." \
		"$(cat "$error_output")" "Failed to query dependency \"compression command\" on host origin.example."
}

test_zxfer_resolve_remote_cli_tool_falls_back_to_direct_probe_when_generic_tool_is_absent_from_capabilities() {
	result_file="$TEST_TMPDIR/resolve_remote_cli_absent_fallback.out"
	probe_file="$TEST_TMPDIR/resolve_remote_cli_absent_fallback.probes"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_absent_fallback.direct"

	(
		g_option_V_very_verbose=1
		g_zxfer_profile_remote_cli_tool_direct_probes=0
		zxfer_ensure_remote_host_capabilities() {
			zxfer_test_accept_remote_capability_response \
				"$(fake_remote_capability_response)"
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			zxfer_profile_increment_counter g_zxfer_profile_remote_cli_tool_direct_probes
			printf '%s\n' "/remote/bin/zstd"
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$result_file"
		printf '%s\n' "${g_zxfer_profile_remote_cli_tool_direct_probes:-0}" >"$probe_file"
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should fall back to a direct probe when the capability payload does not advertise the requested tool." \
		0 "$status"
	assertEquals "Generic remote CLI tool fallback should return the direct-probe helper path." \
		"/remote/bin/zstd" "$(cat "$result_file")"
	assertEquals "Generic remote CLI tool fallback should call the direct-probe helper when the capability payload omits the requested tool." \
		"direct-probe-called" "$(cat "$direct_log")"
	assertEquals "Generic remote CLI tool fallback should make the direct-probe counter visible when it has to probe." \
		"1" "$(cat "$probe_file")"
}

test_zxfer_resolve_remote_cli_tool_reports_missing_generic_dependency_from_capabilities_without_direct_probe() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_cached_missing.out"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_cached_missing.direct"

	set +e
	(
		zxfer_ensure_remote_host_capabilities() {
			zxfer_test_accept_remote_capability_response 'ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	zstd	1	-
end'
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			return 1
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$output_file"
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should fail closed when the cached capability payload reports the helper missing." \
		1 "$status"
	assertContains "Generic remote CLI tool resolution should surface the documented secure-PATH guidance directly from the cached capability payload." \
		"$(cat "$output_file")" "Required dependency \"compression command\" not found on host origin.example in secure PATH"
	assertEquals "Generic remote CLI tool resolution should not fall back to a direct probe when the cached capability payload already reports the helper missing." \
		"" "$(cat "$direct_log" 2>/dev/null)"
}

test_zxfer_resolve_remote_cli_command_safe_rejects_blank_commands_and_surfaces_lookup_failures_in_current_shell() {
	blank_output="$TEST_TMPDIR/resolve_remote_cli_blank.out"
	lookup_output="$TEST_TMPDIR/resolve_remote_cli_lookup.out"

	(
		zxfer_resolve_remote_cli_command_safe "origin.example" "   " "compression command" source >"$blank_output"
	)
	blank_status=$?

	(
		zxfer_resolve_remote_cli_tool() {
			printf '%s\n' "remote helper lookup failed"
			return 1
		}
		zxfer_resolve_remote_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source >"$lookup_output"
	)
	lookup_status=$?

	assertEquals "Blank remote CLI commands should be rejected." 1 "$blank_status"
	assertContains "Blank remote CLI command failures should use the documented validation message." \
		"$(cat "$blank_output")" "Required dependency \"compression command\" must not be empty or whitespace-only."
	assertEquals "Remote CLI command resolution should fail when the head token cannot be resolved." 1 "$lookup_status"
	assertEquals "Remote CLI command resolution should surface the remote helper lookup failure verbatim." \
		"remote helper lookup failed" "$(cat "$lookup_output")"
}

test_zxfer_extract_remote_cli_command_head_surfaces_split_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/extract_remote_cli_head_failure.out"

	(
		zxfer_extract_remote_cli_command_head '"/opt/parallel dir/parallel" --jobs 4' "parallel command" >"$output_file"
	)
	status=$?

	assertEquals "Remote CLI head extraction should fail when the configured command relies on shell quoting." \
		1 "$status"
	assertContains "Remote CLI head extraction should preserve the splitter diagnostic." \
		"$(cat "$output_file")" "parallel command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_zxfer_resolve_remote_cli_command_safe_surfaces_split_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_split_failure.out"

	(
		zxfer_resolve_remote_cli_command_safe \
			"origin.example" \
			'"/opt/zstd dir/zstd" -T0 -9' \
			"compression command" \
			source >"$output_file"
	)
	status=$?

	assertEquals "Remote CLI command resolution should fail when the configured command relies on shell quoting." \
		1 "$status"
	assertContains "Remote CLI command resolution should preserve splitter diagnostics before remote lookup begins." \
		"$(cat "$output_file")" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_init_variables_resolves_remote_compression_helpers() {
	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "/remote/target/zfs"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				if [ "$1:$2" = "origin.example:zstd -T0 -9" ]; then
					printf '%s\n' "'/remote/origin/zstd' '-T0' '-9'"
				elif [ "$1:$2" = "target.example:zstd -d" ]; then
					printf '%s\n' "'/remote/target/zstd' '-d'"
				else
					printf '%s\n' "unexpected compression command"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
			printf 'origin-compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'origin-decompress=%s\n' "$g_origin_cmd_decompress_safe"
			printf 'target-compress=%s\n' "$g_target_cmd_compress_safe"
			printf 'target-decompress=%s\n' "$g_target_cmd_decompress_safe"
		)
	)

	assertContains "Origin initialization should resolve the remote compression helper." \
		"$result" "origin-compress='/remote/origin/zstd' '-T0' '-9'"
	assertContains "Origin initialization should leave the unused remote decompression helper on the local safe default." \
		"$result" "origin-decompress='/local/bin/zstd' '-d'"
	assertContains "Target initialization should leave the unused remote compression helper on the local safe default." \
		"$result" "target-compress='/local/bin/zstd' '-T0' '-9'"
	assertContains "Target initialization should resolve the remote decompression helper." \
		"$result" "target-decompress='/remote/target/zstd' '-d'"
}

test_init_variables_marks_remote_compression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "remote compression lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote compression lookup failures should abort initialization." 1 "$status"
	assertContains "Remote compression lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote compression lookup failures should preserve the failing message." \
		"$output" "msg=remote compression lookup failed"
}

test_init_variables_marks_remote_target_zfs_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "target zfs lookup failed"
					return 1
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Target-side remote zfs lookup failures should abort initialization." 1 "$status"
	assertContains "Target-side remote zfs lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Target-side remote zfs lookup failures should preserve the failing message." \
		"$output" "msg=target zfs lookup failed"
}

test_init_variables_marks_remote_source_os_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote source OS lookup failures should abort initialization." 1 "$status"
	assertContains "Remote source OS lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote source OS lookup failures should use the documented host-scoped message." \
		"$output" "msg=Failed to determine operating system on host origin.example."
}

test_init_variables_marks_remote_destination_os_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				if [ "$1" = "target.example" ]; then
					return 1
				fi
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/resolved/$2"
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote destination OS lookup failures should abort initialization." 1 "$status"
	assertContains "Remote destination OS lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote destination OS lookup failures should use the documented host-scoped message." \
		"$output" "msg=Failed to determine operating system on host target.example."
}

test_init_variables_marks_remote_target_decompression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "/remote/target/zfs"
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "target decompression lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-3'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Target-side remote decompression lookup failures should abort initialization." 1 "$status"
	assertContains "Target-side remote decompression lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Target-side remote decompression lookup failures should preserve the failing message." \
		"$output" "msg=target decompression lookup failed"
}

test_init_variables_marks_remote_restore_cat_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "origin.example:cat" ]; then
					printf '%s\n' "remote cat lookup failed"
					return 1
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_e_restore_property_mode=1
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote restore-mode cat lookup failures should abort initialization." 1 "$status"
	assertContains "Remote restore-mode cat lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote restore-mode cat lookup failures should preserve the failing message." \
		"$output" "msg=remote cat lookup failed"
}

test_init_variables_skips_remote_dependency_validation_in_dry_run() {
	log="$TEST_TMPDIR/init_variables_dry_run.log"
	: >"$log"

	output=$(
		(
			LOG_FILE="$log"
			zxfer_get_os() {
				printf 'get_os %s\n' "$1" >>"$LOG_FILE"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf 'resolve-tool %s %s\n' "$1" "$2" >>"$LOG_FILE"
				printf '%s\n' "/remote/$2"
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf 'resolve-cli %s %s\n' "$1" "$2" >>"$LOG_FILE"
				printf '%s\n' "'/remote/zstd' '-d'"
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			g_option_z_compress=1
			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_option_e_restore_property_mode=1
			g_cmd_cat=""
			zxfer_init_variables
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
			printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
			printf 'origin_compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'target_decompress=%s\n' "$g_target_cmd_decompress_safe"
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertNotContains "Dry-run variable initialization should not probe the origin host operating system." \
		"$(cat "$log")" "get_os origin.example"
	assertNotContains "Dry-run variable initialization should not probe the target host operating system." \
		"$(cat "$log")" "get_os target.example"
	assertNotContains "Dry-run variable initialization should not resolve any remote helper paths." \
		"$(cat "$log")" "resolve-tool "
	assertNotContains "Dry-run variable initialization should not resolve any remote CLI helper commands." \
		"$(cat "$log")" "resolve-cli "
	assertContains "Dry-run variable initialization should explain that origin helper validation is skipped." \
		"$output" "Dry run: skipping live remote source helper validation."
	assertContains "Dry-run variable initialization should explain that target helper validation is skipped." \
		"$output" "Dry run: skipping live remote destination helper validation."
	assertContains "Dry-run restore initialization should explain that remote cat validation is skipped." \
		"$output" "Dry run: skipping live remote backup-restore helper validation."
	assertContains "Dry-run variable initialization should keep the unresolved origin zfs render helper." \
		"$output" "origin_zfs=/sbin/zfs"
	assertContains "Dry-run variable initialization should keep the unresolved target zfs render helper." \
		"$output" "target_zfs=/sbin/zfs"
	assertContains "Dry-run variable initialization should preserve the local safe compression command for rendering." \
		"$output" "origin_compress='/local/bin/zstd' '-T0' '-9'"
	assertContains "Dry-run variable initialization should preserve the local safe decompression command for rendering." \
		"$output" "target_decompress='/local/bin/zstd' '-d'"
	assertContains "Dry-run restore initialization should fall back to a plain cat helper name for rendering." \
		"$output" "cat=cat"
}

test_refresh_compression_commands_rejects_empty_compression_command() {
	set +e
	output=$(
		(
			zxfer_quote_cli_tokens() {
				if [ "$1" = "" ]; then
					printf '%s' ""
				else
					printf "'%s'\n" "$1"
				fi
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress=""
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when the configured compression command is empty." 2 "$status"
	assertContains "Empty compression commands should use the documented usage error." \
		"$output" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_whitespace_only_compression_command() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress="   "
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only compression commands as empty." 2 "$status"
	assertContains "Whitespace-only compression commands should use the documented usage error." \
		"$output" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_missing_decompress_command() {
	set +e
	output=$(
		(
			zxfer_quote_cli_tokens() {
				if [ "$1" = "zstd -3" ]; then
					printf '%s\n' "'zstd' '-3'"
				else
					printf '%s' ""
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress=""
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when no decompressor can be derived." 1 "$status"
	assertContains "Missing decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_refresh_compression_commands_rejects_whitespace_only_decompress_command() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="   "
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only decompression commands as missing." 1 "$status"
	assertContains "Whitespace-only decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}
