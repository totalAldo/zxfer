#!/bin/sh
#
# shunit2 tests for zxfer_globals.sh helpers.
#
# shellcheck disable=SC2030,SC2317,SC2329

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

usage() {
	:
}

tearDown() {
	:
}

create_fake_ssh_bin() {
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_globals.XXXXXX)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	OPTIND=1
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset ZXFER_BACKUP_DIR
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_z_compress=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_g_grandfather_protection=""
	g_option_j_jobs=1
	g_option_m_migrate=0
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_supports_control_sockets=0
	g_MAX_YIELD_ITERATIONS=8
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	zxfer_reset_failure_context "unit"
	create_fake_ssh_bin
}

test_init_globals_initializes_defaults_and_temp_files() {
	real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	result=$(
		(
			counter_file="$TEST_TMPDIR/init_globals.counter"
			printf '%s\n' 0 >"$counter_file"
			get_temp_file() {
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
			ssh_supports_control_sockets() {
				return 0
			}
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/backup_root"
			init_globals
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'zfs=%s\n' "$g_cmd_zfs"
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'backup=%s\n' "$g_backup_storage_root"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'tmp1=%s\n' "$g_delete_source_tmp_file"
			printf 'tmp2=%s\n' "$g_delete_dest_tmp_file"
			printf 'tmp3=%s\n' "$g_delete_snapshots_to_delete_tmp_file"
		)
	)

	assertContains "init_globals should resolve awk through the helper." "$result" "awk=$real_awk"
	assertContains "init_globals should resolve zfs through the helper." "$result" "zfs=/stub/zfs"
	assertContains "init_globals should resolve ssh through the helper." "$result" "ssh=/stub/ssh"
	assertContains "init_globals should honor ZXFER_BACKUP_DIR when set." "$result" "backup=$TEST_TMPDIR/backup_root"
	assertContains "init_globals should enable control sockets when ssh supports them." "$result" "control=1"
	assertContains "Yield iterations should default to 1." "$result" "yield=1"
	assertContains "Delete source temp file should be initialized." "$result" "tmp1=$TEST_TMPDIR/tmp.1"
	assertContains "Delete destination temp file should be initialized." "$result" "tmp2=$TEST_TMPDIR/tmp.2"
	assertContains "Delete diff temp file should be initialized." "$result" "tmp3=$TEST_TMPDIR/tmp.3"
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

test_zxfer_assign_required_tool_marks_dependency_failures() {
	set +e
	output=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "lookup failed"
				return 1
			}
			throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_assign_required_tool g_cmd_test mocktool "mocktool"
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
			g_cmd_mock=""
			zxfer_assign_required_tool g_cmd_mock mocktool "mocktool"
			printf '%s\n' "$g_cmd_mock"
		)
	)

	assertEquals "Successful tool assignment should populate the requested variable." "/opt/mock/mocktool" "$result"
}

test_read_command_line_switches_sets_options_and_remote_paths() {
	log="$TEST_TMPDIR/read_switches.log"
	: >"$log"
	result=$(
		(
			get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			refresh_compression_commands() {
				printf 'refresh\n' >>"$log"
				g_cmd_compress_safe="zstd -9"
				g_cmd_decompress_safe="zstd -d"
			}
			g_ssh_supports_control_sockets=1
			g_cmd_zfs="/sbin/zfs"
			g_MAX_YIELD_ITERATIONS=8
			OPTIND=1
			read_command_line_switches \
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
	assertContains "Origin zfs wrapper should use the quoted host spec." "$result" "lzfs=/usr/bin/ssh 'origin.example' 'pfexec' /sbin/zfs"
	assertContains "Target zfs wrapper should use the quoted host spec." "$result" "rzfs=/usr/bin/ssh 'target.example' 'doas' /sbin/zfs"
	assertContains "Parallel job count should come from -j." "$result" "jobs=4"
	assertContains "Yield iterations should expand to the max when -Y is set." "$result" "yield=8"
	assertContains "Custom compression should be recorded from -Z." "$result" "compress=zstd -9"
	assertContains "Property transfer should be enabled by -e/-k/-m/-P." "$result" "props=1"
	assertContains "Very verbose mode should imply verbose mode." "$result" "verbose=1/1"
	assertContains "Compression refresh should run after parsing options." "$(cat "$log")" "refresh"
}

test_prepare_remote_host_connections_sets_up_control_sockets_after_validation() {
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	: >"$log"

	result=$(
		(
			setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertContains "Origin control socket setup should happen during remote preparation." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Target control socket setup should happen during remote preparation." \
		"$(cat "$log")" "setup target.example doas target"
	assertContains "Origin zfs wrapper should be refreshed after socket setup." \
		"$result" "lzfs=/usr/bin/ssh 'origin.example' 'pfexec' /remote/origin/zfs"
	assertContains "Target zfs wrapper should be refreshed after socket setup." \
		"$result" "rzfs=/usr/bin/ssh 'target.example' 'doas' /remote/target/zfs"
}

test_read_command_line_switches_sets_flags_in_current_shell() {
	OPTIND=1
	g_cmd_ssh="/usr/bin/ssh"
	g_cmd_zfs="/sbin/zfs"
	g_MAX_YIELD_ITERATIONS=9
	g_ssh_supports_control_sockets=0
	refresh_compression_commands() {
		:
	}

	read_command_line_switches \
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
	# read_command_line_switches runs in the current shell here; the SC2031
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
	assertEquals "Origin zfs wrapper should include the quoted host spec and local zfs path." \
		"/usr/bin/ssh 'origin.example' 'pfexec' /sbin/zfs" "$g_LZFS"
	assertEquals "Target zfs wrapper should include the quoted host spec and local zfs path." \
		"/usr/bin/ssh 'target.example' 'doas' /sbin/zfs" "$g_RZFS"

	unset -f refresh_compression_commands
	. "$ZXFER_ROOT/src/zxfer_globals.sh"
}

test_read_command_line_switches_rejects_invalid_option() {
	set +e
	output=$(
		(
			refresh_compression_commands() {
				:
			}
			throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			OPTIND=1
			read_command_line_switches -Q 2>/dev/null
		)
	)
	status=$?

	assertEquals "Invalid options should exit with usage status." 2 "$status"
	assertContains "Invalid options should use the generic usage error." "$output" "Invalid option provided."
}

test_consistency_check_rejects_non_numeric_jobs() {
	set +e
	output=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=abc
			consistency_check
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
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=0
			consistency_check
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
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_O_origin_host="origin.example"
			g_option_m_migrate=1
			consistency_check
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
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_z_compress=1
			consistency_check
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
			get_os() {
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_dependency_path="$gawk_dir"
			init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	result=$(
		(
			get_os() {
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
			init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat through the required-tool helper." \
		"$result" "cat=/bin/cat"
}

test_refresh_compression_commands_rejects_empty_compression_command() {
	set +e
	output=$(
		(
			quote_cli_tokens() {
				if [ "$1" = "" ]; then
					printf '%s' ""
				else
					printf "'%s'\n" "$1"
				fi
			}
			throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress=""
			g_cmd_decompress="zstd -d"
			refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when the configured compression command is empty." 2 "$status"
	assertContains "Empty compression commands should use the documented usage error." \
		"$output" "Compression command (-Z/ZXFER_COMPRESSION) cannot be empty."
}

test_refresh_compression_commands_rejects_missing_decompress_command() {
	set +e
	output=$(
		(
			quote_cli_tokens() {
				if [ "$1" = "zstd -3" ]; then
					printf '%s\n' "'zstd' '-3'"
				else
					printf '%s' ""
				fi
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress=""
			refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when no decompressor can be derived." 1 "$status"
	assertContains "Missing decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_close_origin_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_origin.log"
	socket_dir="$TEST_TMPDIR/origin_socket_dir"
	mkdir -p "$socket_dir"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_origin_control_socket_dir="$socket_dir"

	close_origin_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Origin socket path should be cleared after closing." "" "$g_ssh_origin_control_socket"
	assertEquals "Origin socket directory should be cleared after closing." "" "$g_ssh_origin_control_socket_dir"
	assertFalse "Origin socket directory should be removed during cleanup." "[ -d \"$socket_dir\" ]"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-S
$TEST_TMPDIR/origin.sock
-O
exit
origin.example
pfexec" "$(cat "$log")"
}

test_get_path_owner_uid_falls_back_to_ls_for_dash_prefixed_paths() {
	result=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >"-owner_file"
			chmod 600 "./-owner_file"
			stat() {
				return 1
			}
			get_path_owner_uid "-owner_file"
		)
	)

	assertEquals "LS fallback should recover the owner for dash-prefixed paths." "$(id -u)" "$result"
}

test_get_path_mode_octal_falls_back_to_ls_for_dash_prefixed_paths() {
	result=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >"-mode_file"
			chmod 600 "./-mode_file"
			stat() {
				return 1
			}
			ls() {
				printf '%s\n' "-rw------- 1 0 0 0 Jan 1 00:00 ./-mode_file"
			}
			get_path_mode_octal "-mode_file"
		)
	)

	assertEquals "LS fallback should recover 0600 permissions for dash-prefixed paths." "600" "$result"
}

test_merge_path_allowlists_deduplicates_entries() {
	result=$(merge_path_allowlists "/sbin:/bin:/usr/bin" "/bin:/usr/local/bin:/usr/bin")

	assertEquals "Merged PATH allowlists should keep first-seen ordering and drop duplicates." \
		"/sbin:/bin:/usr/bin:/usr/local/bin" "$result"
}

test_zxfer_apply_secure_path_exports_runtime_path() {
	result=$(
		(
			ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/sbin"
			ZXFER_SECURE_PATH_APPEND="/custom/bin"
			zxfer_apply_secure_path
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'runtime=%s\n' "$g_zxfer_runtime_path"
			printf 'path=%s\n' "$PATH"
		)
	)

	assertContains "zxfer_apply_secure_path should honor the configured secure PATH." \
		"$result" "secure=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Runtime PATH should append the built-in allowlist without duplicates." \
		"$result" "runtime=/opt/zfs/bin:/usr/sbin:/custom/bin:/sbin:/bin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "Exported PATH should match the computed runtime PATH." \
		"$result" "path=/opt/zfs/bin:/usr/sbin:/custom/bin:/sbin:/bin:/usr/bin:/usr/local/sbin:/usr/local/bin"
}

test_ssh_supports_control_sockets_reflects_ssh_status() {
	g_cmd_ssh="$FAKE_SSH_BIN"

	FAKE_SSH_EXIT_STATUS=0
	export FAKE_SSH_EXIT_STATUS
	if ssh_supports_control_sockets; then
		status_supported=0
	else
		status_supported=1
	fi

	FAKE_SSH_EXIT_STATUS=1
	export FAKE_SSH_EXIT_STATUS
	if ssh_supports_control_sockets; then
		status_unsupported=0
	else
		status_unsupported=1
	fi

	unset FAKE_SSH_EXIT_STATUS

	assertEquals "ssh_supports_control_sockets should succeed when ssh -M -V succeeds." 0 "$status_supported"
	assertEquals "ssh_supports_control_sockets should fail when ssh -M -V fails." 1 "$status_unsupported"
}

test_get_ssh_cmd_for_host_prefers_matching_control_socket() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	assertEquals "Origin host ssh command should reuse the origin control socket." \
		"$FAKE_SSH_BIN -S $TEST_TMPDIR/origin.sock" "$(get_ssh_cmd_for_host "origin.example")"
	assertEquals "Target host ssh command should reuse the target control socket." \
		"$FAKE_SSH_BIN -S $TEST_TMPDIR/target.sock" "$(get_ssh_cmd_for_host "target.example")"
	assertEquals "Unmatched hosts should use the base ssh command." \
		"$FAKE_SSH_BIN" "$(get_ssh_cmd_for_host "other.example")"
}

test_close_target_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_target.log"
	socket_dir="$TEST_TMPDIR/target_socket_dir"
	mkdir -p "$socket_dir"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"
	g_ssh_target_control_socket_dir="$socket_dir"

	close_target_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Target socket path should be cleared after closing." "" "$g_ssh_target_control_socket"
	assertEquals "Target socket directory should be cleared after closing." "" "$g_ssh_target_control_socket_dir"
	assertFalse "Target socket directory should be removed during cleanup." "[ -d \"$socket_dir\" ]"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-S
$TEST_TMPDIR/target.sock
-O
exit
target.example
doas" "$(cat "$log")"
}

test_trap_exit_relaunches_services_when_requested() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			close_all_ssh_control_sockets() {
				:
			}
			echoV() {
				printf '%s\n' "$*"
			}
			relaunch() {
				printf 'relaunch\n'
			}
			true
			trap_exit
		)
	)
	status=$?

	assertEquals "trap_exit should preserve a successful exit status when cleanup relaunch succeeds." 0 "$status"
	assertContains "trap_exit should log that it is restarting stopped services." \
		"$output" "zxfer exiting early; restarting stopped services."
	assertContains "trap_exit should invoke relaunch when services are still marked for restart." \
		"$output" "relaunch"
}

test_trap_exit_logs_when_relaunch_is_unavailable() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			close_all_ssh_control_sockets() {
				:
			}
			echoV() {
				printf '%s\n' "$*"
			}
			true
			trap_exit
		)
	)
	status=$?

	assertEquals "trap_exit should preserve a successful exit status when relaunch is unavailable." 0 "$status"
	assertContains "trap_exit should log when stopped services cannot be restarted because relaunch() is missing." \
		"$output" "zxfer exiting with services still stopped; relaunch() unavailable."
}

test_setup_ssh_control_socket_replaces_existing_target_socket_state() {
	log="$TEST_TMPDIR/setup_target.log"
	FAKE_SSH_LOG="$log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	result=$(
		(
			close_target_ssh_control_socket() {
				printf 'closed\n'
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_target_control_socket="$TEST_TMPDIR/old_target.sock"
			g_ssh_target_control_socket_dir="$TEST_TMPDIR/old_target_dir"
			setup_ssh_control_socket "target.example doas" "target"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'dir=%s\n' "$g_ssh_target_control_socket_dir"
		)
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertContains "Replacing an existing target control socket should close the old socket first." \
		"$result" "closed"
	assertContains "Target socket setup should store the new control socket path." "$result" "socket="
	assertContains "Target socket setup should store the new control socket directory." "$result" "dir="
	assertEquals "New target control socket setup should preserve host token boundaries for ssh." \
		"-M
-S
$(printf '%s\n' "$result" | awk -F= '/^socket=/{print $2}')
-fN
target.example
doas" "$(cat "$log")"
}

test_consistency_check_rejects_backup_and_restore_modes_together() {
	set +e
	output=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_k_backup_property_mode=1
			g_option_e_restore_property_mode=1
			consistency_check
		)
	)
	status=$?

	assertEquals "Backup and restore mode conflicts should fail validation." 2 "$status"
	assertContains "Backup and restore mode conflicts should use the documented error." \
		"$output" "You cannot bac(k)up and r(e)store properties at the same time."
}

test_consistency_check_rejects_dual_beep_modes() {
	set +e
	output=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_b_beep_always=1
			g_option_B_beep_on_success=1
			consistency_check
		)
	)
	status=$?

	assertEquals "Conflicting beep modes should fail validation." 2 "$status"
	assertContains "Conflicting beep modes should use the documented error." \
		"$output" "You cannot use both beep modes at the same time."
}

test_consistency_check_rejects_invalid_grandfather_values() {
	set +e
	output_non_numeric=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="abc"
			consistency_check
		)
	)
	status_non_numeric=$?

	output_zero=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="0"
			consistency_check
		)
	)
	status_zero=$?

	assertEquals "Non-numeric grandfather values should fail validation." 2 "$status_non_numeric"
	assertContains "Non-numeric grandfather errors should mention the received value." \
		"$output_non_numeric" "grandfather protection requires a positive integer; received \"abc\"."
	assertEquals "Zero-day grandfather values should fail validation." 2 "$status_zero"
	assertContains "Zero-day grandfather errors should require days greater than zero." \
		"$output_zero" "grandfather protection requires days greater than 0; received \"0\"."
}

test_sanitize_backup_helpers_cover_empty_root_and_legacy_cases() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"

	assertEquals "Empty backup components should normalize to an underscore." "_" "$(sanitize_backup_component "")"
	assertEquals "Empty dataset paths should normalize to the dataset placeholder." "dataset" "$(sanitize_dataset_relpath "/")"
	assertEquals "Root mountpoints should map to the root backup directory." \
		"$g_backup_storage_root/root" "$(get_backup_storage_dir "/" "tank/src")"
	assertEquals "Legacy mountpoints should append the sanitized dataset path." \
		"$g_backup_storage_root/legacy/tank/src" "$(get_backup_storage_dir "legacy" "tank/src")"
}

test_get_ssh_cmd_for_host_returns_base_command_for_empty_host() {
	g_cmd_ssh="/usr/bin/ssh"

	assertEquals "Hosts omitted from wrapper lookups should return the base ssh command." \
		"/usr/bin/ssh" "$(get_ssh_cmd_for_host "")"
}

test_get_effective_user_uid_returns_failure_when_id_is_unavailable() {
	empty_path="$TEST_TMPDIR/no_id_path"
	mkdir -p "$empty_path"
	old_path=$PATH
	PATH="$empty_path"
	outfile="$TEST_TMPDIR/effective_uid.out"

	get_effective_user_uid >"$outfile"
	status=$?
	PATH=$old_path

	assertEquals "Missing id binaries should make effective-UID detection fail cleanly." 1 "$status"
	assertEquals "Failed effective-UID detection should not emit output." "" "$(cat "$outfile")"
}

test_get_backup_storage_dir_handles_detached_none_and_blank_mountpoints() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"

	assertEquals "Blank mountpoints should use the detached layout with the dataset suffix appended." \
		"$g_backup_storage_root/detached/tank/src" "$(get_backup_storage_dir "" "tank/src")"
	assertEquals "\"none\" mountpoints should use the detached layout with the dataset suffix appended." \
		"$g_backup_storage_root/none/tank/src" "$(get_backup_storage_dir "none" "tank/src")"
	assertEquals "\"-\" mountpoints should use the detached layout with the dataset suffix appended." \
		"$g_backup_storage_root/detached/tank/src" "$(get_backup_storage_dir "-" "tank/src")"
}

test_get_path_owner_uid_and_mode_use_stat_when_available() {
	owned_file="$TEST_TMPDIR/stat_owned_file"
	: >"$owned_file"

	owner_result=$(
		(
			stat() {
				if [ "$1" = "-f" ] && [ "$2" = "%u" ]; then
					printf '%s\n' "4242"
					return 0
				fi
				return 1
			}
			get_path_owner_uid "$owned_file"
		)
	)

	mode_result=$(
		(
			stat() {
				if [ "$1" = "-f" ] && [ "$2" = "%OLp" ]; then
					printf '%s\n' "600"
					return 0
				fi
				return 1
			}
			get_path_mode_octal "$owned_file"
		)
	)

	assertEquals "Owner lookup should use stat when available." "4242" "$owner_result"
	assertEquals "Mode lookup should use stat when available." "600" "$mode_result"
}

test_ensure_local_backup_dir_rejects_symlink_and_non_directory_targets() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_real"
	symlink_dir="$physical_tmpdir/ensure_local_link"
	non_dir="$physical_tmpdir/ensure_local_file"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$symlink_dir"
	: >"$non_dir"

	set +e
	symlink_output=$(
		(
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_local_backup_dir "$symlink_dir"
		)
	)
	symlink_status=$?

	non_dir_output=$(
		(
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_local_backup_dir "$non_dir"
		)
	)
	non_dir_status=$?

	assertEquals "Symlinked backup directories should be rejected." 1 "$symlink_status"
	assertContains "Symlinked backup directories should use the documented error." \
		"$symlink_output" "Refusing to use backup directory $symlink_dir because it is a symlink."
	assertEquals "Non-directory backup paths should be rejected." 1 "$non_dir_status"
	assertContains "Non-directory backup paths should use the documented error." \
		"$non_dir_output" "Refusing to use backup directory $non_dir because it is not a directory."
}

test_ensure_local_backup_dir_rejects_unknown_or_disallowed_owner() {
	backup_dir="$TEST_TMPDIR/ensure_local_owner"
	mkdir -p "$backup_dir"

	set +e
	unknown_owner_output=$(
		(
			get_path_owner_uid() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_local_backup_dir "$backup_dir"
		)
	)
	unknown_owner_status=$?

	disallowed_owner_output=$(
		(
			get_path_owner_uid() {
				printf '%s\n' "1234"
			}
			backup_owner_uid_is_allowed() {
				return 1
			}
			describe_expected_backup_owner() {
				printf '%s\n' "root (UID 0)"
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_local_backup_dir "$backup_dir"
		)
	)
	disallowed_owner_status=$?

	assertEquals "Backup directories with unknown owners should be rejected." 1 "$unknown_owner_status"
	assertContains "Unknown owner failures should use the documented error." \
		"$unknown_owner_output" "Cannot determine the owner of backup directory $backup_dir."
	assertEquals "Backup directories owned by other UIDs should be rejected." 1 "$disallowed_owner_status"
	assertContains "Disallowed owner failures should identify the unexpected UID." \
		"$disallowed_owner_output" "Refusing to use backup directory $backup_dir because it is owned by UID 1234 instead of root (UID 0)."
}

test_ensure_local_backup_dir_reports_chmod_failures_in_current_shell() {
	backup_dir="$TEST_TMPDIR/ensure_local_chmod_fail"
	fake_bin="$TEST_TMPDIR/ensure_local_chmod_bin"
	mkdir -p "$backup_dir" "$fake_bin"
	cat >"$fake_bin/chmod" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin/chmod"
	old_path=$PATH
	PATH="$fake_bin:$PATH"
	THROW_MSG=""
	throw_error() {
		THROW_MSG=$1
		return 1
	}

	ensure_local_backup_dir "$backup_dir"
	status=$?

	unset -f throw_error
	PATH=$old_path

	assertEquals "chmod failures should cause ensure_local_backup_dir to fail." 1 "$status"
	assertContains "chmod failures should use the documented backup-directory error." \
		"$THROW_MSG" "Error securing backup directory $backup_dir."
}

test_ensure_remote_backup_dir_skips_without_host_and_reports_ssh_failures() {
	if ensure_remote_backup_dir "$TEST_TMPDIR/remote_backup" ""; then
		empty_host_status=0
	else
		empty_host_status=1
	fi

	set +e
	ssh_failure_output=$(
		(
			get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			invoke_ssh_shell_command_for_host() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_remote_backup_dir "-remote_backup" "backup@example.com"
		)
	)
	ssh_failure_status=$?

	assertEquals "Remote backup directory preparation should no-op when no host is provided." 0 "$empty_host_status"
	assertEquals "Remote backup directory ssh failures should abort the helper." 1 "$ssh_failure_status"
	assertContains "Remote backup directory ssh failures should use the documented error." \
		"$ssh_failure_output" "Error preparing backup directory on backup@example.com."
}

test_ensure_remote_backup_dir_quotes_dash_prefixed_paths() {
	ssh_log="$TEST_TMPDIR/ensure_remote_dash.log"
	ssh_bin="$TEST_TMPDIR/ensure_remote_dash_ssh"
	cat >"$ssh_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >"$ssh_log"
exit 0
EOF
	chmod +x "$ssh_bin"
	g_cmd_ssh="$ssh_bin"

	ensure_remote_backup_dir "-remote_backup" "backup@example.com"

	assertContains "Dash-prefixed remote backup paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup"
}

test_read_remote_backup_file_quotes_dash_prefixed_paths() {
	ssh_log="$TEST_TMPDIR/read_remote_dash.log"
	ssh_bin="$TEST_TMPDIR/read_remote_dash_ssh"
	outfile="$TEST_TMPDIR/read_remote_dash.out"
	cat >"$ssh_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >"$ssh_log"
printf '%s\n' "backup-data"
exit 0
EOF
	chmod +x "$ssh_bin"
	g_cmd_ssh="$ssh_bin"
	g_cmd_cat="/bin/cat"

	read_remote_backup_file "backup@example.com" "-remote_backup_file" >"$outfile"
	status=$?

	assertEquals "Successful remote backup reads should preserve the ssh exit status." 0 "$status"
	assertEquals "Successful remote backup reads should pass through the remote file contents." \
		"backup-data" "$(cat "$outfile")"
	assertContains "Dash-prefixed remote metadata paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup_file"
}

test_write_backup_properties_renders_remote_dry_run_command() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example doas"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	initial_source="tank/src"

	result=$(
		(
			get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			quote_host_spec_tokens() {
				printf '%s\n' "'target.example' 'doas'"
			}
			run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			write_backup_properties
		)
	)

	assertContains "Remote dry-run backup writes should render the ssh command prefix." \
		"$result" "/usr/bin/ssh 'target.example' 'doas'"
	assertContains "Remote dry-run backup writes should render the remote cat pipeline." \
		"$result" ".zxfer_backup_info.src"
}

test_write_backup_properties_renders_local_dry_run_command() {
	g_option_n_dryrun=1
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	initial_source="tank/src"

	result=$(
		(
			run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			write_backup_properties
		)
	)

	assertContains "Local dry-run backup writes should render a local redirection command." \
		"$result" "umask 077; printf '%s'"
	assertContains "Local dry-run backup writes should target the secure backup path." \
		"$result" ".zxfer_backup_info.src"
}

test_get_backup_properties_reads_legacy_local_backup_and_warns() {
	mount_dir="$TEST_TMPDIR/legacy_mount"
	mkdir -p "$mount_dir"
	legacy_backup="$mount_dir/.zxfer_backup_info.child"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$legacy_backup"
	chmod 600 "$legacy_backup"
	initial_source="tank/src/child"
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
	stderr_file="$TEST_TMPDIR/legacy_backup.stderr"

	run_source_zfs_cmd() {
		printf '%s\n' "$mount_dir"
	}

	get_backup_properties 2>"$stderr_file"

	assertEquals "Legacy backup reads should restore the backup file contents." \
		"tank/src/child,backup/dst,compression=lz4" "$g_restored_backup_file_contents"
	assertContains "Legacy backup reads should emit a warning about the hardened storage path." \
		"$(cat "$stderr_file")" "Warning: read legacy backup metadata from $legacy_backup."
}

test_get_backup_properties_uses_find_fallback_under_backup_root() {
	g_backup_storage_root="$TEST_TMPDIR/fallback_store"
	fallback_dir="$g_backup_storage_root/unexpected/layout"
	mkdir -p "$fallback_dir"
	fallback_file="$fallback_dir/.zxfer_backup_info.child"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$fallback_file"
	chmod 600 "$fallback_file"
	initial_source="tank/src/child"
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"

	run_source_zfs_cmd() {
		printf '%s\n' "/mnt/backups"
	}

	get_backup_properties

	assertEquals "Backup-property discovery should fall back to searching under the backup root." \
		"tank/src/child,backup/dst,compression=lz4" "$g_restored_backup_file_contents"
}

test_get_backup_properties_rejects_ambiguous_find_fallback_matches() {
	g_backup_storage_root="$TEST_TMPDIR/ambiguous_store"
	first_dir="$g_backup_storage_root/layout/one"
	second_dir="$g_backup_storage_root/layout/two"
	mkdir -p "$first_dir" "$second_dir"
	first_file="$first_dir/.zxfer_backup_info.child"
	second_file="$second_dir/.zxfer_backup_info.child"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$first_file"
	printf '%s\n' "tank/src/child,backup/dst,compression=off" >"$second_file"
	chmod 600 "$first_file" "$second_file"
	initial_source="tank/src/child"
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"

	run_source_zfs_cmd() {
		printf '%s\n' "/mnt/backups"
	}

	set +e
	output=$(
		(
			throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			get_backup_properties
		)
	)
	status=$?

	assertEquals "Ambiguous backup-root fallback matches should abort instead of choosing one arbitrarily." 1 "$status"
	assertContains "Ambiguous fallback failures should identify the filename." \
		"$output" "Multiple backup property files named .zxfer_backup_info.child"
}

test_get_backup_properties_walks_up_to_parent_filesystem() {
	g_backup_storage_root="$TEST_TMPDIR/ancestor_store"
	parent_secure_dir="$g_backup_storage_root/mnt/parent"
	mkdir -p "$parent_secure_dir"
	parent_backup="$parent_secure_dir/.zxfer_backup_info.child"
	printf '%s\n' "tank/parent/child,backup/dst,compression=lz4" >"$parent_backup"
	chmod 600 "$parent_backup"
	initial_source="tank/parent/child"
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"

	run_source_zfs_cmd() {
		case "$6" in
		tank/parent/child)
			printf '%s\n' "/mnt/child"
			;;
		tank/parent)
			printf '%s\n' "/mnt/parent"
			;;
		*)
			return 1
			;;
		esac
	}

	get_backup_properties

	assertEquals "Backup-property discovery should walk up to ancestor datasets when the child has no metadata file." \
		"tank/parent/child,backup/dst,compression=lz4" "$g_restored_backup_file_contents"
}

test_get_backup_properties_reads_remote_legacy_backup_and_warns() {
	initial_source="tank/src/child"
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/remote_backup_store"
	stderr_file="$TEST_TMPDIR/remote_legacy_backup.stderr"
	legacy_backup="/mnt/remote/.zxfer_backup_info.child"

	run_source_zfs_cmd() {
		printf '%s\n' "/mnt/remote"
	}

	read_remote_backup_file() {
		if [ "$2" = "$legacy_backup" ]; then
			printf '%s\n' "tank/src/child,backup/dst,compression=lz4"
			return 0
		fi
		return 1
	}

	get_backup_properties 2>"$stderr_file"

	assertEquals "Remote legacy backup reads should restore the backup file contents." \
		"tank/src/child,backup/dst,compression=lz4" "$g_restored_backup_file_contents"
	assertContains "Remote legacy backup reads should emit a warning about the hardened storage path." \
		"$(cat "$stderr_file")" "Warning: read legacy backup metadata from $legacy_backup."
}

test_get_backup_properties_reports_missing_backup_file() {
	initial_source="tank"
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/missing_store"

	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				printf '%s\n' "-"
			}
			throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			get_backup_properties
		)
	)
	status=$?

	assertEquals "Missing backup metadata should abort with an error." 1 "$status"
	assertContains "Missing backup metadata should use the documented guidance." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_require_secure_backup_file_reports_unknown_owner_and_mode() {
	backup_file="$TEST_TMPDIR/secure_meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	owner_output=$(
		(
			get_path_owner_uid() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			require_secure_backup_file "$backup_file"
		)
	)
	owner_status=$?

	mode_output=$(
		(
			get_path_owner_uid() {
				printf '%s\n' "0"
			}
			get_path_mode_octal() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			require_secure_backup_file "$backup_file"
		)
	)
	mode_status=$?

	assertEquals "Unknown backup-file owners should be rejected." 1 "$owner_status"
	assertContains "Unknown owner failures should mention the metadata path." \
		"$owner_output" "Cannot determine the owner of backup metadata $backup_file."
	assertEquals "Unknown backup-file permissions should be rejected." 1 "$mode_status"
	assertContains "Unknown mode failures should mention the metadata path." \
		"$mode_output" "Cannot determine the permissions for backup metadata $backup_file."
}

test_write_backup_properties_reports_local_write_failure() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	initial_source="tank/src"

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			get_backup_storage_dir() {
				printf '%s\n' "$TEST_TMPDIR/missing/secure/path"
			}
			ensure_local_backup_dir() {
				:
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			write_backup_properties 2>/dev/null
		)
	)
	status=$?

	assertEquals "Local backup writes should abort when the secure file cannot be created." 1 "$status"
	assertContains "Local backup write failures should mention the mounted-filesystem guidance." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

test_write_backup_properties_reports_remote_write_failure() {
	g_option_n_dryrun=0
	g_option_T_target_host="target.example"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	initial_source="tank/src"

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			get_backup_storage_dir() {
				printf '%s\n' "/var/db/zxfer/mnt/backups"
			}
			ensure_remote_backup_dir() {
				:
			}
			get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			invoke_ssh_shell_command_for_host() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			write_backup_properties
		)
	)
	status=$?

	assertEquals "Remote backup writes should abort when the remote write command fails." 1 "$status"
	assertContains "Remote backup write failures should mention the mounted-filesystem guidance." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

. "$SHUNIT2_BIN"
