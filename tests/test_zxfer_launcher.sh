#!/bin/sh
#
# shunit2 tests for zxfer launcher compatibility.
#
# shellcheck disable=SC1090,SC2317,SC2329

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_launcher.XXXXXX)
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

create_minimal_launcher_fixture() {
	l_fixture_dir=$1

	mkdir -p "$l_fixture_dir/src"
	cp "$ZXFER_ROOT/zxfer" "$l_fixture_dir/zxfer"
	chmod +x "$l_fixture_dir/zxfer"

	cat >"$l_fixture_dir/src/zxfer_common.sh" <<'EOF'
#!/bin/sh
init_globals() {
	g_option_k_backup_property_mode=0
}
zxfer_set_failure_stage() {
	:
}
zxfer_set_failure_roots() {
	:
}
consistency_check() {
	:
}
throw_usage_error() {
	printf '%s\n' "$1" >&2
	exit "${2:-2}"
}
beep() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_globals.sh" <<'EOF'
#!/bin/sh
read_command_line_switches() {
	OPTIND=1
}
init_variables() {
	printf '%s\n' "init_variables" >>"${ZXFER_TEST_LOG:?}"
}
EOF

	cat >"$l_fixture_dir/src/zxfer_secure_paths.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_remote_cli.sh" <<'EOF'
#!/bin/sh
refresh_remote_zfs_commands() {
	printf '%s\n' "refresh_remote_zfs_commands" >>"${ZXFER_TEST_LOG:?}"
}
prepare_remote_host_connections() {
	printf '%s\n' "prepare_remote_host_connections" >>"${ZXFER_TEST_LOG:?}"
	refresh_remote_zfs_commands
}
EOF

	cat >"$l_fixture_dir/src/zxfer_property_cache.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_transfer_properties.sh" <<'EOF'
#!/bin/sh
EOF

	cat >"$l_fixture_dir/src/zxfer_backup_metadata.sh" <<'EOF'
#!/bin/sh
write_backup_properties() {
	:
}
EOF

	cat >"$l_fixture_dir/src/zxfer_zfs_mode.sh" <<'EOF'
#!/bin/sh
run_zfs_mode_loop() {
	printf '%s\n' "run_zfs_mode_loop" >>"${ZXFER_TEST_LOG:?}"
}
EOF

	for helper in zxfer_get_zfs_list.sh zxfer_zfs_send_receive.sh zxfer_inspect_delete_snap.sh; do
		cat >"$l_fixture_dir/src/$helper" <<'EOF'
#!/bin/sh
EOF
	done
}

test_launcher_runs_remote_connection_prep_from_remote_cli_module() {
	fixture_dir="$TEST_TMPDIR/launcher-remote-cli"
	rm -rf "$fixture_dir"
	mkdir -p "$fixture_dir"
	create_minimal_launcher_fixture "$fixture_dir"
	log_path="$fixture_dir/launcher.log"

	set +e
	output=$(
		ZXFER_TEST_LOG="$log_path" \
			"$fixture_dir/zxfer" backup/dst 2>&1
	)
	status=$?
	set -e

	assertEquals "The launcher should succeed when the required remote CLI module is present." \
		0 "$status"
	assertContains "The launcher should invoke remote connection preparation from the dedicated remote CLI module." \
		"$(cat "$log_path")" "prepare_remote_host_connections"
	assertContains "Remote connection preparation should refresh remote wrappers." \
		"$(cat "$log_path")" "refresh_remote_zfs_commands"
	assertContains "The launcher should continue into init_variables()." \
		"$(cat "$log_path")" "init_variables"
	assertContains "The launcher should continue into replication." \
		"$(cat "$log_path")" "run_zfs_mode_loop"
	assertEquals "The launcher should not emit a spurious command-not-found error when the required module is present." \
		"" "$output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
