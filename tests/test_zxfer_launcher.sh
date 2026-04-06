#!/bin/sh
#
# shunit2 tests for zxfer launcher compatibility.
#

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

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
refresh_remote_zfs_commands() {
	printf '%s\n' "refresh_remote_zfs_commands" >>"${ZXFER_TEST_LOG:?}"
}
init_variables() {
	printf '%s\n' "init_variables" >>"${ZXFER_TEST_LOG:?}"
}
EOF

	cat >"$l_fixture_dir/src/zxfer_transfer_properties.sh" <<'EOF'
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

test_launcher_falls_back_when_prepare_remote_host_connections_is_missing() {
	fixture_dir="$TEST_TMPDIR/mixed-version"
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

	assertEquals "The launcher should still succeed when the sourced globals lack prepare_remote_host_connections()." \
		0 "$status"
	assertContains "The compatibility shim should still refresh remote wrappers." \
		"$(cat "$log_path")" "refresh_remote_zfs_commands"
	assertContains "The launcher should continue into init_variables()." \
		"$(cat "$log_path")" "init_variables"
	assertContains "The launcher should continue into replication." \
		"$(cat "$log_path")" "run_zfs_mode_loop"
	assertEquals "The mixed-version fallback should avoid a spurious command-not-found error." \
		"" "$output"
}

. "$SHUNIT2_BIN"
