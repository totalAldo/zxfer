#!/bin/sh
# Switch-parsing and consistency-check tests for src/zxfer_cli.sh that need
# the real remote-host and compression helpers. Run by tests/test_zxfer_cli.sh
# under the remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

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
			OPTIND=1
			zxfer_read_command_line_switches \
				-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
				-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
				-N "tank/nonrecursive" -o "atime=off" -O "origin.example pfexec" \
				-P -R "tank/src" -s -T "target.example doas" -U -v -V -w \
				-x "child" -Y -z -Z "zstd -9"
			printf 'origin=%s\n' "$g_option_O_origin_host"
			printf 'target=%s\n' "$g_option_T_target_host"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'compress=%s\n' "$g_cmd_compress"
			printf 'props=%s\n' "$g_option_P_transfer_property"
			printf 'verbose=%s/%s\n' "$g_option_v_verbose" "$g_option_V_very_verbose"
		)
	)

	assertContains "Origin host should be recorded from -O." "$result" "origin=origin.example pfexec"
	assertContains "Target host should be recorded from -T." "$result" "target=target.example doas"
	assertContains "Parallel job count should come from -j." "$result" "jobs=4"
	assertContains "Yield iterations should expand to the max when -Y is set." "$result" "yield=$ZXFER_MAX_YIELD_ITERATIONS"
	assertContains "Custom compression should be recorded from -Z." "$result" "compress=zstd -9"
	assertContains "Property transfer should be enabled by -e/-k/-m/-P." "$result" "props=1"
	assertContains "Very verbose mode should imply verbose mode." "$result" "verbose=1/1"
	assertContains "Compression refresh should run after parsing options." "$(cat "$log")" "refresh"
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

test_read_command_line_switches_skips_control_socket_when_ssh_lacks_support() {
	remote_log="$TEST_TMPDIR/unsupported_control_socket.log"
	result_file="$TEST_TMPDIR/unsupported_control_socket.out"
	stderr_file="$TEST_TMPDIR/unsupported_control_socket.err"

	set +e
	(
		trap - EXIT INT TERM HUP QUIT
		: >"$remote_log"
		FAKE_SSH_LOG="$remote_log"
		export FAKE_SSH_LOG
		OPTIND=1
		g_option_z_compress=0
		g_cmd_compress="zstd -3"
		g_cmd_decompress="zstd -d"
		g_option_O_origin_host=""
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_cmd_zfs="/sbin/zfs"
		g_ssh_supports_control_sockets=0
		g_ssh_origin_control_socket=""
		zxfer_read_command_line_switches -O "backup@example.com"
		printf 'origin=%s\n' "$g_option_O_origin_host"
		printf 'socket=%s\n' "$g_ssh_origin_control_socket"
	) >"$result_file" 2>"$stderr_file"
	status=$?

	result=$(cat "$result_file")
	assertNotEquals "Skipping unsupported control sockets should still leave observable parser state." "" "$result"
	assertEquals "Unsupported ssh clients should not be asked to create control sockets." "" "$(cat "$remote_log")"
	assertEquals "Parsing should not emit stderr noise when multiplexing is unavailable." "" "$(cat "$stderr_file")"
	assertContains "$result" "origin=backup@example.com"
	assertContains "$result" "socket="
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

test_consistency_check_rejects_backup_and_restore_modes_together() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_k_backup_property_mode=1
			g_option_e_restore_property_mode=1
			zxfer_consistency_check
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
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_b_beep_always=1
			g_option_B_beep_on_success=1
			zxfer_consistency_check
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
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="abc"
			zxfer_consistency_check
		)
	)
	status_non_numeric=$?

	output_zero=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="0"
			zxfer_consistency_check
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
