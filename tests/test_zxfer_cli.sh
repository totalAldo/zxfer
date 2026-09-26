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
	zxfer_init_cli_option_defaults
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	zxfer_resolve_cli_command_safe() {
		g_zxfer_resolved_cli_command_result=$2
	}
	zxfer_refresh_remote_zfs_commands() {
		:
	}
}

# Purpose: Parse one switch set from the option defaults and print every
# parsed option as a <name=value> line.
# Usage: cli_test_parse_options ARG...
cli_test_parse_options() {
	(
		zxfer_init_cli_option_defaults
		OPTIND=1
		zxfer_read_command_line_switches "$@"
		printf '<%s>\n' \
			"b=$g_option_b_beep_always" "B=$g_option_B_beep_on_success" \
			"c=$g_option_c_services" "d=$g_option_d_delete_destination_snapshots" \
			"D=$g_option_D_display_progress_bar" "e=$g_option_e_restore_property_mode" \
			"F=$g_option_F_force_rollback" "g=$g_option_g_grandfather_protection" \
			"I=$g_option_I_ignore_properties" "j=$g_option_j_jobs" \
			"k=$g_option_k_backup_property_mode" "m=$g_option_m_migrate" \
			"n=$g_option_n_dryrun" "N=$g_option_N_nonrecursive" \
			"o=$g_option_o_override_property" "O=$g_option_O_origin_host" \
			"P=$g_option_P_transfer_property" "R=$g_option_R_recursive" \
			"s=$g_option_s_make_snapshot" "T=$g_option_T_target_host" \
			"U=$g_option_U_skip_unsupported_properties" "v=$g_option_v_verbose" \
			"V=$g_option_V_very_verbose" "w=$g_option_w_raw_send" \
			"x=$g_option_x_exclude_datasets" "Y=$g_option_Y_yield_iterations" \
			"z=$g_option_z_compress" "compress=$g_cmd_compress"
	)
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

# Each row parses one switch from the defaults: flag|argument|expected option.
# A flag with several effects has one row per effect.
test_read_command_line_switches_sets_each_flag_on_its_own() {
	while IFS='|' read -r cli_flag cli_value cli_expected; do
		if [ -n "$cli_value" ]; then
			cli_output=$(cli_test_parse_options "$cli_flag" "$cli_value")
		else
			cli_output=$(cli_test_parse_options "$cli_flag")
		fi
		assertContains "zxfer $cli_flag $cli_value should leave <$cli_expected>." \
			"$cli_output" "<$cli_expected>"
	done <<EOF
-b||b=1
-B||B=1
-c|svc:/network/nfs/server|c=svc:/network/nfs/server
-d||d=1
-D|pv -N %%title%%|D=pv -N %%title%%
-e||e=1
-e||P=1
-F||F=-F
-g|7|g=7
-I|mountpoint|I=mountpoint
-j|4|j=4
-k||k=1
-k||P=1
-m||m=1
-m||s=1
-m||P=1
-n||n=1
-N|tank/nonrecursive|N=tank/nonrecursive
-o|user:note=value\,with\,commas=and;semi|o=user:note=value\,with\,commas=and;semi
-O|origin.example pfexec|O=origin.example pfexec
-P||P=1
-R|tank/src|R=tank/src
-s||s=1
-s||m=0
-s||P=0
-T|target.example doas|T=target.example doas
-U||U=1
-v||v=1
-V||V=1
-V||v=1
-w||w=1
-x|child|x=child
-Y||Y=$ZXFER_MAX_YIELD_ITERATIONS
-z||z=1
-Z|zstd -9|z=1
-Z|zstd -9|compress=zstd -9
EOF
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

test_refresh_compression_commands_clears_stale_safe_commands_without_z() {
	g_option_z_compress=0
	g_cmd_compress_safe="evil"
	g_cmd_decompress_safe="evil"

	zxfer_refresh_compression_commands

	assertEquals "Without -z no safe compression command should survive a refresh." \
		"<>|<>" "<$g_cmd_compress_safe>|<$g_cmd_decompress_safe>"
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
		zxfer_resolve_cli_command_safe() {
			g_zxfer_resolved_cli_command_result="compression lookup failed"
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
		zxfer_resolve_cli_command_safe() {
			if [ "$3" = "decompression command" ]; then
				g_zxfer_resolved_cli_command_result="decompression lookup failed"
				return 1
			fi
			g_zxfer_resolved_cli_command_result=$2
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
