#!/bin/sh
#
# shunit2 tests for zxfer_cli.sh helpers.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_cli.sh"

zxfer_usage() {
	printf '%s\n' "usage output"
}

setUp() {
	OPTIND=1
	g_test_max_yield_iterations=8
	zxfer_init_cli_option_defaults
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	zxfer_resolve_local_cli_command_safe() {
		printf '%s\n' "$1"
	}
	zxfer_refresh_remote_zfs_commands() {
		:
	}
	zxfer_get_max_yield_iterations() {
		printf '%s\n' "$g_test_max_yield_iterations"
	}
}

test_zxfer_init_cli_option_defaults_resets_complete_owned_state() {
	g_option_b_beep_always=9
	g_option_j_jobs=9
	g_option_O_origin_host="dirty-origin"
	g_option_Y_yield_iterations=9
	zxfer_init_cli_option_defaults

	boolean_defaults="$g_option_b_beep_always:$g_option_B_beep_on_success:$g_option_d_delete_destination_snapshots:$g_option_e_restore_property_mode:$g_option_k_backup_property_mode:$g_option_P_transfer_property:$g_option_m_migrate:$g_option_n_dryrun:$g_option_s_make_snapshot:$g_option_U_skip_unsupported_properties:$g_option_v_verbose:$g_option_V_very_verbose:$g_option_w_raw_send:$g_option_z_compress"
	string_defaults="$g_option_c_services$g_option_D_display_progress_bar$g_option_F_force_rollback$g_option_g_grandfather_protection$g_option_I_ignore_properties$g_option_o_override_property$g_option_O_origin_host$g_option_R_recursive$g_option_N_nonrecursive$g_option_T_target_host$g_option_x_exclude_datasets"

	assertEquals "Every boolean CLI option should reset to disabled." \
		"0:0:0:0:0:0:0:0:0:0:0:0:0:0" "$boolean_defaults"
	assertEquals "Every string CLI option should reset to empty." "" "$string_defaults"
	assertEquals "Parallelism should retain the safe single-job default." 1 "$g_option_j_jobs"
	assertEquals "Yield retries should retain the one-iteration default." 1 "$g_option_Y_yield_iterations"
}

test_read_command_line_switches_sets_flags_in_current_shell() {
	zxfer_read_command_line_switches -v -n -z -j 3 -O origin.example -T target.example -Y

	assertEquals "Verbose mode should be enabled." 1 "$g_option_v_verbose"
	assertEquals "Dry-run mode should be enabled." 1 "$g_option_n_dryrun"
	assertEquals "Compression should be enabled." 1 "$g_option_z_compress"
	assertEquals "Job count should be updated." 3 "$g_option_j_jobs"
	assertEquals "Origin host should be captured." "origin.example" "$g_option_O_origin_host"
	assertEquals "Target host should be captured." "target.example" "$g_option_T_target_host"
	assertEquals "Yield mode should expand to the configured max iterations." \
		8 "$g_option_Y_yield_iterations"
}

test_read_command_line_switches_preserves_override_escape_sequences() {
	zxfer_read_command_line_switches -o 'user:note=value\,with\,commas=and;semi'

	assertEquals "Quoted -o values should keep literal-comma escape sequences for the downstream override parser." \
		'user:note=value\,with\,commas=and;semi' "$g_option_o_override_property"
}

test_consistency_check_rejects_zero_jobs() {
	zxfer_test_capture_subshell '
		zxfer_throw_usage_error() {
			printf "%s\n" "$1"
			exit "${2:-2}"
		}
		g_option_j_jobs=0
		zxfer_consistency_check
	'

	assertEquals "A zero job count should fail validation." 2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Zero-job validation should explain the lower bound." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "job count of at least 1"
}

test_refresh_compression_commands_rejects_empty_command() {
	zxfer_test_capture_subshell '
		zxfer_throw_usage_error() {
			printf "%s\n" "$1"
			exit "${2:-2}"
		}
		g_option_z_compress=1
		g_cmd_compress=""
		zxfer_refresh_compression_commands
	'

	assertEquals "An empty compression command should fail validation." 2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Compression validation should explain the empty command." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_shell_quoted_compression_command() {
	zxfer_test_capture_subshell '
		zxfer_throw_usage_error() {
			printf "%s\n" "$1"
			exit "${2:-2}"
		}
		g_option_z_compress=1
		g_cmd_compress="\"/opt/zstd dir/zstd\" -3"
		g_cmd_decompress="zstd -d"
		zxfer_refresh_compression_commands
	'

	assertEquals "Quoted compression commands should fail validation instead of being silently re-tokenized." \
		2 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Quoted compression command failures should explain the literal-token requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Compression command (-Z) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_refresh_compression_commands_marks_dependency_failure_for_compression_lookup() {
	zxfer_test_capture_subshell '
		zxfer_throw_error() {
			printf "class=%s\n" "${g_zxfer_failure_class:-}"
			printf "msg=%s\n" "$1"
			exit "${2:-1}"
		}
		zxfer_resolve_local_cli_command_safe() {
			printf "%s\n" "compression lookup failed"
			return 1
		}
		g_option_z_compress=1
		g_cmd_compress="zstd -3"
		g_cmd_decompress="zstd -d"
		zxfer_refresh_compression_commands
	'

	assertEquals "Compression-helper lookup failures should abort command refresh." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Compression-helper lookup failures should be classified as dependency errors." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "class=dependency"
	assertContains "Compression-helper lookup failures should preserve the lookup error." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "msg=compression lookup failed"
}

test_refresh_compression_commands_marks_dependency_failure_for_decompression_lookup() {
	zxfer_test_capture_subshell '
		zxfer_throw_error() {
			printf "class=%s\n" "${g_zxfer_failure_class:-}"
			printf "msg=%s\n" "$1"
			exit "${2:-1}"
		}
		zxfer_resolve_local_cli_command_safe() {
			if [ "$2" = "decompression command" ]; then
				printf "%s\n" "decompression lookup failed"
				return 1
			fi
			printf "%s\n" "$1"
		}
		g_option_z_compress=1
		g_cmd_compress="zstd -3"
		g_cmd_decompress="zstd -d"
		zxfer_refresh_compression_commands
	'

	assertEquals "Decompression-helper lookup failures should abort command refresh." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Decompression-helper lookup failures should be classified as dependency errors." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "class=dependency"
	assertContains "Decompression-helper lookup failures should preserve the lookup error." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "msg=decompression lookup failed"
}

test_refresh_compression_commands_rejects_shell_quoted_decompression_command() {
	zxfer_test_capture_subshell '
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit "${2:-1}"
		}
		g_option_z_compress=1
		g_cmd_compress="zstd -3"
		g_cmd_decompress="\"/opt/zstd dir/zstd\" -d"
		zxfer_refresh_compression_commands
	'

	assertEquals "Quoted decompression commands should fail validation instead of being silently re-tokenized." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Quoted decompression command failures should explain the literal-token requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Decompression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_refresh_compression_commands_rejects_invalid_noncompress_compression_command_quoting() {
	zxfer_test_capture_subshell '
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit "${2:-1}"
		}
		zxfer_quote_cli_tokens() {
			if [ "$2" = "Compression command" ]; then
				printf "%s\n" "compression quote failed"
				return 1
			fi
			printf "%s\n" "$1"
		}
		g_option_z_compress=0
		zxfer_refresh_compression_commands
	'

	assertEquals "Non-compress refreshes should fail when safe compression quoting fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Non-compress refreshes should preserve compression quoting failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "compression quote failed"
}

test_refresh_compression_commands_rejects_invalid_noncompress_decompression_command_quoting() {
	zxfer_test_capture_subshell '
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit "${2:-1}"
		}
		zxfer_quote_cli_tokens() {
			if [ "$2" = "Decompression command" ]; then
				printf "%s\n" "decompression quote failed"
				return 1
			fi
			printf "%s\n" "$1"
		}
		g_option_z_compress=0
		zxfer_refresh_compression_commands
	'

	assertEquals "Non-compress refreshes should fail when safe decompression quoting fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Non-compress refreshes should preserve decompression quoting failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "decompression quote failed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
