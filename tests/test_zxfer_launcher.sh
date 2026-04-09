#!/bin/sh
#
# shunit2 tests for zxfer launcher module loading.
#
# shellcheck disable=SC1090,SC2317,SC2329

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
	chmod +x "$l_fixture_dir/zxfer"

	cat >"$l_fixture_dir/src/zxfer_modules.sh" <<'EOF'
#!/bin/sh
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_path_security.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_reporting.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_exec.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_dependencies.sh"
zxfer_initialize_dependency_defaults
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_runtime.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_remote_hosts.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_cli.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_snapshot_state.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_backup_metadata.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_property_cache.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_property_reconcile.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_snapshot_discovery.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_send_receive.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_snapshot_reconcile.sh"
. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_replication.sh"
EOF

	cat >"$l_fixture_dir/src/zxfer_reporting.sh" <<'EOF'
#!/bin/sh
zxfer_set_failure_stage() {
	:
}
zxfer_set_failure_roots() {
	:
}
zxfer_throw_usage_error() {
	printf '%s\n' "$1" >&2
	exit "${2:-2}"
}
zxfer_beep() {
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
EOF

	cat >"$l_fixture_dir/src/zxfer_runtime.sh" <<'EOF'
#!/bin/sh
zxfer_init_globals() {
	g_option_k_backup_property_mode=0
}
zxfer_register_runtime_traps() {
	:
}
zxfer_init_variables() {
	printf '%s\n' "zxfer_init_variables" >>"${ZXFER_TEST_LOG:?}"
}
EOF

	cat >"$l_fixture_dir/src/zxfer_path_security.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_remote_hosts.sh" <<'EOF'
#!/bin/sh
zxfer_refresh_remote_zfs_commands() {
	printf '%s\n' "zxfer_refresh_remote_zfs_commands" >>"${ZXFER_TEST_LOG:?}"
}
zxfer_prepare_remote_host_connections() {
	printf '%s\n' "zxfer_prepare_remote_host_connections" >>"${ZXFER_TEST_LOG:?}"
	zxfer_refresh_remote_zfs_commands
}
EOF

	cat >"$l_fixture_dir/src/zxfer_cli.sh" <<'EOF'
#!/bin/sh
zxfer_read_command_line_switches() {
	OPTIND=1
}
zxfer_consistency_check() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_property_cache.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_snapshot_state.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_property_reconcile.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_backup_metadata.sh" <<'EOF'
#!/bin/sh
zxfer_write_backup_properties() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_replication.sh" <<'EOF'
#!/bin/sh
zxfer_run_zfs_mode_loop() {
	printf '%s\n' "zxfer_run_zfs_mode_loop" >>"${ZXFER_TEST_LOG:?}"
}
EOF

	for helper in zxfer_snapshot_discovery.sh zxfer_send_receive.sh zxfer_snapshot_reconcile.sh; do
		cat >"$l_fixture_dir/src/$helper" <<'EOF'
#!/bin/sh
EOF
	done
}

test_launcher_runs_remote_connection_prep_from_remote_hosts_module() {
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
	assertContains "The launcher should invoke remote connection preparation from the dedicated remote hosts module." \
		"$(cat "$log_path")" "zxfer_prepare_remote_host_connections"
	assertContains "Remote connection preparation should refresh remote wrappers." \
		"$(cat "$log_path")" "zxfer_refresh_remote_zfs_commands"
	assertContains "The launcher should continue into zxfer_init_variables()." \
		"$(cat "$log_path")" "zxfer_init_variables"
	assertContains "The launcher should continue into replication." \
		"$(cat "$log_path")" "zxfer_run_zfs_mode_loop"
	assertEquals "The launcher should not emit a spurious command-not-found error when the required module is present." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
