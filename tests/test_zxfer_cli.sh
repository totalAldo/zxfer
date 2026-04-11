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
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_e_restore_property_mode=0
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_I_ignore_properties=""
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_w_raw_send=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
