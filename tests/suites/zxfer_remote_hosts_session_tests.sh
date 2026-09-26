#!/bin/sh
# Tests for src/zxfer_session.sh, run by tests/test_zxfer_remote_hosts.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_session_init_initializes_defaults_and_temp_files() {
	tools="$TEST_TMPDIR/session_init_tools"
	mkdir -p "$tools"
	ln -sf "$(command -v awk)" "$tools/awk"
	ln -sf "$(command -v ps)" "$tools/ps"
	printf '#!/bin/sh\nexit 0\n' >"$tools/zfs"
	chmod 755 "$tools/zfs"
	result=$(
		(
			counter_file="$TEST_TMPDIR/session_init.counter"
			printf '%s\n' 0 >"$counter_file"
			g_zxfer_services_to_restart="stale-service"
			g_zxfer_property_table_lookup_result="stale-lookup"
			g_zxfer_snapshot_plan_file="$TEST_TMPDIR/stale-plan"
			zxfer_get_temp_file() {
				temp_index=$(cat "$counter_file")
				temp_index=$((temp_index + 1))
				printf '%s\n' "$temp_index" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/tmp.$temp_index"
			}
			zxfer_ssh_supports_control_sockets() {
				[ -n "${g_cmd_ssh:-}" ]
			}
			ZXFER_SECURE_PATH=$tools
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/backup_root"
			zxfer_reset_session_state
			zxfer_init_session_environment
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'zfs=%s\n' "$g_cmd_zfs"
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'backup=%s\n' "$g_backup_storage_root"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'plan_file=<%s>\n' "$g_zxfer_snapshot_plan_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'table_lookup=<%s>\n' "$g_zxfer_property_table_lookup_result"
		)
	)

	assertContains "Session initialization should resolve awk on the secure PATH." "$result" "awk=$tools/awk"
	assertContains "Session initialization should resolve zfs on the secure PATH." "$result" "zfs=$tools/zfs"
	assertContains "Session initialization should defer ssh resolution until remote transport is actually needed." "$result" "ssh="
	assertContains "Session initialization should honor ZXFER_BACKUP_DIR when set." "$result" "backup=$TEST_TMPDIR/backup_root"
	assertContains "Session initialization should leave control-socket support disabled until ssh is resolved on demand." "$result" "control=0"
	assertContains "Yield iterations should default to 1." "$result" "yield=1"
	assertContains "The snapshot plan scratch file should stay unallocated until planning needs it." "$result" "plan_file=<>"
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
					g_zxfer_required_tool_result="$FAKE_SSH_BIN"
					return 0
				fi
				g_zxfer_required_tool_result="/stub/$1"
			}
			zxfer_ssh_supports_control_sockets() {
				[ "${g_cmd_ssh:-}" = "$FAKE_SSH_BIN" ]
			}
			zxfer_open_ssh_control_sockets() {
				printf 'open ssh=%s control=%s\n' "$g_cmd_ssh" \
					"$g_ssh_supports_control_sockets" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			g_cmd_ssh=""
			g_option_O_origin_host="origin.example pfexec"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			zxfer_prepare_remote_host_connections
		)
	)

	assertEquals "Remote preparation should resolve ssh and probe control-socket support before it opens the masters, then preload capabilities over them." \
		"open ssh=$FAKE_SSH_BIN control=1
preload origin.example pfexec source" "$(cat "$log")"
}

test_prepare_remote_host_connections_fails_with_dependency_class_without_ssh() {
	set +e
	output=$(
		(
			zxfer_ensure_local_ssh_command() {
				g_zxfer_resolved_local_ssh_command_result="ssh dependency missing"
				return 1
			}
			zxfer_open_ssh_control_sockets() {
				printf '%s\n' "open"
			}
			zxfer_throw_error() {
				printf 'class=%s throw=%s\n' "$g_zxfer_failure_class" "$1"
				exit 9
			}
			g_option_T_target_host="target.example"
			zxfer_prepare_remote_host_connections
		)
	)
	status=$?

	assertEquals "A missing local ssh should stop remote preparation." 9 "$status"
	assertEquals "A missing local ssh should be a dependency failure with the lookup diagnostic, before any master opens." \
		"class=dependency throw=ssh dependency missing" "$output"
}

test_session_init_rejects_relative_backup_dir_override() {
	set +e
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_BACKUP_DIR="relative-backups"
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_reset_session_state
			zxfer_init_session_environment
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Relative ZXFER_BACKUP_DIR overrides should abort startup." 1 "$status"
	assertContains "Startup should report that ZXFER_BACKUP_DIR must be absolute." \
		"$output" "ZXFER_BACKUP_DIR must be an absolute path"
}

test_session_init_rejects_control_whitespace_in_a_secure_path_entry() {
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
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_reset_session_state
			zxfer_init_session_environment
		)
	)
	status=$?

	assertEquals "Session initialization should fail when a secure PATH entry holds control whitespace." 1 "$status"
	assertContains "A rejected secure PATH should be classified as a dependency failure." \
		"$output" "class=dependency"
	assertContains "A rejected secure PATH should explain the path validation failure." \
		"$output" "single-line absolute path without control whitespace"
}

test_prepare_remote_host_connections_opens_masters_before_preloading_capabilities() {
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	now_counter_file="$TEST_TMPDIR/prepare_remote_hosts.now.counter"
	: >"$log"
	printf '%s\n' 0 >"$now_counter_file"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_open_ssh_control_sockets() {
				printf 'open %s %s\n' "$g_option_O_origin_host" "$g_option_T_target_host" >>"$log"
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
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertEquals "Both masters should open before either capability probe runs over them." \
		"open origin.example pfexec target.example doas
preload origin.example pfexec source
preload target.example doas destination" "$(cat "$log")"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_remote_host_connections_surfaces_verbose_preload_failures() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_open_ssh_control_sockets() {
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
			zxfer_open_ssh_control_sockets() {
				printf "open\n" >>"$log"
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
		)
	)

	assertEquals "Dry-run remote preparation should not open control sockets or preload capabilities." \
		"" "$(cat "$log")"
	assertContains "Dry-run remote preparation should explain that origin ssh preflight is skipped." \
		"$output" "Dry run: skipping ssh control-socket setup and remote capability preload for origin host."
	assertContains "Dry-run remote preparation should explain that target ssh preflight is skipped." \
		"$output" "Dry run: skipping ssh control-socket setup and remote capability preload for target host."
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
				g_zxfer_os_result="SunOS"
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_secure_path="$gawk_dir"
			zxfer_init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	cat_dir="$TEST_TMPDIR/restore_cat_path"
	mkdir -p "$cat_dir"
	printf '#!/bin/sh\nexit 0\n' >"$cat_dir/cat"
	chmod 755 "$cat_dir/cat"

	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="FreeBSD"
				printf '%s\n' "FreeBSD"
			}
			g_zxfer_secure_path=$cat_dir
			g_option_e_restore_property_mode=1
			zxfer_init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat on the secure PATH." \
		"$result" "cat=$cat_dir/cat"
}

test_init_variables_resolves_remote_compression_helpers() {
	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/target/zfs"
				else
					g_zxfer_required_tool_result="unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_cli_command_safe() {
				if [ "$1:$2" = "origin.example:zstd -T0 -9" ]; then
					g_zxfer_resolved_cli_command_result="'/remote/origin/zstd' '-T0' '-9'"
				elif [ "$1:$2" = "target.example:zstd -d" ]; then
					g_zxfer_resolved_cli_command_result="'/remote/target/zstd' '-d'"
				else
					g_zxfer_resolved_cli_command_result="unexpected compression command"
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
			printf 'target-decompress=%s\n' "$g_target_cmd_decompress_safe"
		)
	)

	assertContains "Origin initialization should resolve the remote compression helper." \
		"$result" "origin-compress='/remote/origin/zstd' '-T0' '-9'"
	assertContains "Target initialization should resolve the remote decompression helper." \
		"$result" "target-decompress='/remote/target/zstd' '-d'"
}

test_init_variables_marks_remote_compression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/origin/zfs"
				else
					g_zxfer_required_tool_result="unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result="remote compression lookup failed"
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
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					g_zxfer_required_tool_result="target zfs lookup failed"
					return 1
				else
					g_zxfer_required_tool_result="/resolved/$2"
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
				[ "$1" != "origin.example" ] || return 1
				g_zxfer_os_result="LocalOS"
				printf '%s\n' "LocalOS"
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
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="/resolved/$2"
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
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/target/zfs"
				else
					g_zxfer_required_tool_result="/resolved/$2"
				fi
			}
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result="target decompression lookup failed"
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
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					g_zxfer_required_tool_result="/remote/origin/zfs"
				elif [ "$1:$2" = "origin.example:cat" ]; then
					g_zxfer_required_tool_result="remote cat lookup failed"
					return 1
				else
					g_zxfer_required_tool_result="/resolved/$2"
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
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf 'resolve-tool %s %s\n' "$1" "$2" >>"$LOG_FILE"
				g_zxfer_required_tool_result="/remote/$2"
			}
			zxfer_resolve_cli_command_safe() {
				printf 'resolve-cli %s %s\n' "$1" "$2" >>"$LOG_FILE"
				g_zxfer_resolved_cli_command_result="'/remote/zstd' '-d'"
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
