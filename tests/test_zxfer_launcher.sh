#!/bin/sh
#
# shunit2 tests for zxfer launcher module loading.
#
# shellcheck disable=SC1090,SC2016,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_launcher"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

create_minimal_launcher_fixture() {
	l_fixture_dir=$1

	mkdir -p "$l_fixture_dir/src"
	cp "$ZXFER_ROOT/zxfer" "$l_fixture_dir/zxfer"
	cp "$ZXFER_ROOT/src/zxfer_modules.sh" "$l_fixture_dir/src/zxfer_modules.sh"
	cp "$ZXFER_ROOT/src/zxfer_session.sh" "$l_fixture_dir/src/zxfer_session.sh"
	chmod +x "$l_fixture_dir/zxfer"

	cat >"$l_fixture_dir/src/zxfer_reporting.sh" <<'EOF'
#!/bin/sh
zxfer_set_failure_stage() {
	:
}
zxfer_set_failure_roots() {
	:
}
zxfer_reset_failure_context() {
	:
}
zxfer_set_original_invocation() {
	:
}
zxfer_throw_usage_error() {
	printf '%s\n' "$1" >&2
	exit "${2:-2}"
}
zxfer_beep() {
	:
}
zxfer_echoV() {
	:
}
zxfer_emit_failure_report() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_profile.sh" <<'EOF'
#!/bin/sh
zxfer_reset_profile_state() {
	:
}
zxfer_profile_metrics_enabled() {
	return 1
}
zxfer_profile_add_elapsed_ms() {
	:
}
zxfer_profile_emit_summary() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_exec.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_dependencies.sh" <<'EOF'
#!/bin/sh
zxfer_initialize_dependency_defaults() {
	:
}
zxfer_refresh_secure_path_state() {
	:
}
zxfer_init_dependency_tool_defaults() {
	g_cmd_zfs=/stub/zfs
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
}
zxfer_reset_endpoint_compression_commands() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_runtime.sh" <<'EOF'
#!/bin/sh
zxfer_init_runtime_state_defaults() {
	:
}
zxfer_discard_runtime_cleanup_state() {
	:
}
zxfer_init_temp_artifacts() {
	:
}
zxfer_ensure_run_tmp_root() {
	:
}
zxfer_apply_secure_path() {
	:
}
zxfer_refresh_backup_storage_root() {
	:
}
zxfer_init_backup_storage_root() {
	:
}
zxfer_get_os() {
	printf '%s\n' "zxfer_init_variables" >>"${ZXFER_TEST_LOG:?}"
	printf '%s\n' Linux
}
zxfer_kill_registered_cleanup_pids() {
	:
}
zxfer_cleanup_registered_runtime_artifacts() {
	:
}
zxfer_remove_run_tmp_root() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_background_jobs.sh" <<'EOF'
#!/bin/sh
zxfer_reset_background_job_state() {
	:
}
zxfer_discard_background_job_cleanup_state() {
	:
}
zxfer_abort_all_background_jobs() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_ssh_transport.sh" <<'EOF'
#!/bin/sh
zxfer_reset_ssh_transport_state() {
	:
}
zxfer_discard_ssh_cleanup_state() {
	:
}
zxfer_refresh_remote_zfs_commands() {
	printf '%s\n' "zxfer_refresh_remote_zfs_commands" >>"${ZXFER_TEST_LOG:?}"
}
zxfer_refresh_ssh_transport_tokens_for_role() {
	:
}
zxfer_close_all_ssh_control_sockets() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_remote_hosts.sh" <<'EOF'
#!/bin/sh
zxfer_reset_remote_host_state() {
	:
}
zxfer_preload_remote_host_capabilities() {
	:
}
zxfer_publish_endpoint_runtime_context() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_cli.sh" <<'EOF'
#!/bin/sh
zxfer_init_cli_option_defaults() {
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_option_z_compress=0
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
}
zxfer_read_command_line_switches() {
	OPTIND=1
}
zxfer_consistency_check() {
	:
}
zxfer_set_destination_argument() {
	g_destination=${1:-}
}
EOF

	cat >"$l_fixture_dir/src/zxfer_snapshot_state.sh" <<'EOF'
#!/bin/sh
zxfer_reset_destination_existence_cache() {
	:
}
zxfer_reset_snapshot_record_indexes() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_property_reconcile.sh" <<'EOF'
#!/bin/sh
zxfer_reset_property_runtime_state() {
	:
}
zxfer_reset_property_iteration_caches() {
	:
}
zxfer_reset_property_reconcile_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_backup_metadata.sh" <<'EOF'
#!/bin/sh
zxfer_reset_backup_metadata_state() {
	:
}
zxfer_write_backup_properties() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_replication.sh" <<'EOF'
#!/bin/sh
zxfer_reset_replication_runtime_state() {
	:
}
zxfer_run_zfs_mode_loop() {
	printf '%s\n' "zxfer_run_zfs_mode_loop" >>"${ZXFER_TEST_LOG:?}"
}
EOF

	for helper in \
		zxfer_path_security.sh \
		zxfer_quoting.sh \
		zxfer_locking.sh \
		zxfer_secure_staging.sh \
		zxfer_error_log.sh \
		zxfer_backup_storage.sh \
		zxfer_property_state.sh \
		zxfer_property_policy.sh \
		zxfer_remote_snapshot_discovery.sh; do
		cat >"$l_fixture_dir/src/$helper" <<'EOF'
		#!/bin/sh
EOF
	done

	cat >"$l_fixture_dir/src/zxfer_snapshot_producers.sh" <<'EOF'
#!/bin/sh
zxfer_reset_snapshot_producer_session_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_quoting.sh" <<'EOF'
#!/bin/sh
zxfer_reset_quoting_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_migration_services.sh" <<'EOF'
#!/bin/sh
zxfer_reset_migration_service_state() {
	:
}
zxfer_relaunch() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_send_jobs.sh" <<'EOF'
#!/bin/sh
zxfer_reset_send_job_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_operation_state.sh" <<'EOF'
#!/bin/sh
zxfer_reset_operation_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_locking.sh" <<'EOF'
#!/bin/sh
zxfer_reset_owned_lock_tracking() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_snapshot_discovery.sh" <<'EOF'
#!/bin/sh
zxfer_reset_snapshot_discovery_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_send_receive.sh" <<'EOF'
#!/bin/sh
zxfer_reset_send_receive_state() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_snapshot_reconcile.sh" <<'EOF'
#!/bin/sh
zxfer_reset_snapshot_reconcile_state() {
	:
}
EOF
}

test_module_loader_has_no_source_time_initialization() {
	fixture_dir="$TEST_TMPDIR/loader-pure"
	rm -rf "$fixture_dir"
	mkdir -p "$fixture_dir"
	create_minimal_launcher_fixture "$fixture_dir"

	zxfer_test_capture_subshell "
		unset -f zxfer_set_failure_stage 2>/dev/null || :
		ZXFER_SOURCE_MODULES_ROOT=\"$fixture_dir\"
		. \"$fixture_dir/src/zxfer_modules.sh\"
		if command -v zxfer_set_failure_stage >/dev/null 2>&1; then
			exit 9
		fi
		zxfer_load_modules zxfer_dependencies.sh
		command -v zxfer_set_failure_stage >/dev/null 2>&1
	"

	assertEquals "Sourcing the manifest should define only loader functions until zxfer_load_modules is called." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "The pure loader should not emit output for a valid partial load." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_module_loader_does_not_publish_a_default_root_at_source_time() {
	zxfer_test_capture_subshell "
		unset ZXFER_SOURCE_MODULES_ROOT
		. \"$ZXFER_ROOT/src/zxfer_modules.sh\"
		[ \"\${ZXFER_SOURCE_MODULES_ROOT+set}\" != set ]
	"

	assertEquals "Sourcing the pure loader must not mutate module-root state." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_canonical_module_name_validator_rejects_unknown_name() {
	set +e
	zxfer_is_source_module_name not-a-module.sh
	status=$?

	assertEquals "The canonical manifest validator should reject unknown module names." \
		1 "$status"
}

test_canonical_module_loader_rejects_unknown_boundary() {
	set +e
	output=$(zxfer_load_modules not-a-module.sh 2>&1)
	status=$?

	assertEquals "The canonical loader should reject an unknown boundary before sourcing." \
		2 "$status"
	assertContains "The canonical loader should identify the invalid boundary." \
		"$output" "unknown source module boundary: not-a-module.sh"
}

test_module_loader_rejects_unknown_boundary_before_sourcing_modules() {
	fixture_dir="$TEST_TMPDIR/loader-invalid"
	rm -rf "$fixture_dir"
	mkdir -p "$fixture_dir"
	create_minimal_launcher_fixture "$fixture_dir"

	zxfer_test_capture_subshell "
		ZXFER_SOURCE_MODULES_ROOT=\"$fixture_dir\"
		. \"$fixture_dir/src/zxfer_modules.sh\"
		zxfer_load_modules not-a-module.sh
	"

	assertEquals "An unknown partial-load boundary should fail before any modules are sourced." \
		2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The loader should identify the invalid boundary." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unknown source module boundary: not-a-module.sh"
}

test_module_loader_rejects_multiline_boundary_before_sourcing_modules() {
	fixture_dir="$TEST_TMPDIR/loader-multiline-boundary"
	rm -rf "$fixture_dir"
	mkdir -p "$fixture_dir"
	create_minimal_launcher_fixture "$fixture_dir"

	zxfer_test_capture_subshell '
		ZXFER_SOURCE_MODULES_ROOT="'"$fixture_dir"'"
		. "'"$fixture_dir"'/src/zxfer_modules.sh"
		zxfer_source_module() {
			printf "%s\n" unexpected-source
			return 99
		}
		l_boundary="zxfer_path_security.sh
zxfer_quoting.sh"
		zxfer_load_modules "$l_boundary"
	'

	assertEquals "A multiline boundary spanning adjacent manifest entries must be rejected." \
		2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertNotContains "Invalid boundaries must fail before the loader sources a module." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unexpected-source"
}

test_module_loader_preserves_caller_ifs_and_globbing_state() {
	fixture_dir="$TEST_TMPDIR/loader-shell-state"
	rm -rf "$fixture_dir"
	mkdir -p "$fixture_dir"
	create_minimal_launcher_fixture "$fixture_dir"

	zxfer_test_capture_subshell "
		ZXFER_SOURCE_MODULES_ROOT=\"$fixture_dir\"
		IFS=:
		set -f
		. \"$fixture_dir/src/zxfer_modules.sh\"
		zxfer_load_modules zxfer_dependencies.sh || exit 8
		[ \"\$IFS\" = : ] || exit 9
		case \$- in
		*f*) ;;
		*) exit 10 ;;
		esac
	"

	assertEquals "Loading modules should not alter a caller's IFS or globbing mode." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_module_manifest_covers_every_runtime_source_module_once() {
	expected_modules=$(
		for module_path in "$ZXFER_ROOT"/src/*.sh; do
			printf '%s\n' "${module_path##*/}"
		done |
			sed -e '/^zxfer_modules[.]sh$/d' -e '/^zxfer_cleanup_child_wrapper[.]sh$/d' |
			sort
	)
	actual_modules=$(printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" | sort)

	assertEquals "The canonical manifest should list every sourceable runtime module exactly once." \
		"$expected_modules" "$actual_modules"
}

test_module_manifest_orders_property_state_before_policy_and_reconcile() {
	property_modules=$(printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" |
		sed -n '/^zxfer_property_/p')

	assertEquals "Property modules should load from state ownership through policy into destination reconciliation." \
		"zxfer_property_state.sh
zxfer_property_policy.sh
zxfer_property_reconcile.sh" "$property_modules"
}

test_module_manifest_orders_snapshot_producers_before_remote_and_orchestration() {
	discovery_modules=$(printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" |
		sed -n \
			-e '/^zxfer_snapshot_producers[.]sh$/p' \
			-e '/^zxfer_remote_snapshot_discovery[.]sh$/p' \
			-e '/^zxfer_snapshot_discovery[.]sh$/p')

	assertEquals "Snapshot discovery should load producers, remote batch handling, then orchestration." \
		"zxfer_snapshot_producers.sh
zxfer_remote_snapshot_discovery.sh
zxfer_snapshot_discovery.sh" "$discovery_modules"
}

test_module_manifest_orders_backup_storage_before_metadata() {
	backup_modules=$(printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" |
		sed -n '/^zxfer_backup_/p')

	assertEquals "Secure backup storage should load before metadata policy and orchestration." \
		"zxfer_backup_storage.sh
zxfer_backup_metadata.sh" "$backup_modules"
}

test_module_manifest_orders_ssh_transport_before_remote_capabilities() {
	remote_modules=$(printf '%s\n' "$ZXFER_SOURCE_MODULE_MANIFEST" |
		sed -n -e '/^zxfer_ssh_transport[.]sh$/p' -e '/^zxfer_remote_hosts[.]sh$/p')

	assertEquals "SSH transport should load before remote capability negotiation." \
		"zxfer_ssh_transport.sh
zxfer_remote_hosts.sh" "$remote_modules"
}

test_launcher_prepares_remote_connections_before_runtime_initialization() {
	fixture_dir="$TEST_TMPDIR/launcher-remote-hosts"
	rm -rf "$fixture_dir"
	mkdir -p "$fixture_dir"
	create_minimal_launcher_fixture "$fixture_dir"
	log_path="$fixture_dir/launcher.log"

	zxfer_test_capture_subshell "
		ZXFER_TEST_LOG=\"$log_path\" \
			\"$fixture_dir/zxfer\" backup/dst
	"

	assertEquals "The launcher should succeed when the required remote hosts module is present." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Remote connection preparation should refresh transport routing before execution-context initialization." \
		"zxfer_refresh_remote_zfs_commands" "$(sed -n '1p' "$log_path")"
	assertContains "The launcher should continue into zxfer_init_variables()." \
		"$(cat "$log_path")" "zxfer_init_variables"
	assertContains "The launcher should continue into replication." \
		"$(cat "$log_path")" "zxfer_run_zfs_mode_loop"
	assertEquals "The launcher should not emit a spurious command-not-found error when the required module is present." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
