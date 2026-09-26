#!/bin/sh
#
# shunit2 tests for the -V profiling counters and summary in
# src/zxfer_profile.sh.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_profile"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_reset_all_owner_state
	TMPDIR=$TEST_TMPDIR
	export TMPDIR
}

test_zxfer_profile_record_ssh_invocation_tracks_other_and_inferred_sides() {
	g_option_V_very_verbose=1
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0

	zxfer_profile_record_ssh_invocation "wrapper.example" other
	zxfer_profile_record_ssh_invocation "target.example"
	zxfer_profile_record_ssh_invocation "unknown.example"

	assertEquals "Explicit other-side attribution should still count toward total ssh invocations." \
		3 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Inferred destination-side attribution should count target-host ssh invocations." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
	assertEquals "Explicit other-side attribution and unknown hosts should both count toward the other-side ssh bucket." \
		2 "$g_zxfer_profile_other_ssh_shell_invocations"
	assertEquals "Origin-side attribution should remain unchanged when only other and destination paths are exercised." \
		0 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_zxfer_profile_record_zfs_call_tracks_remaining_verbs_and_buckets() {
	g_option_V_very_verbose=1
	g_zxfer_failure_stage=""
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0

	zxfer_profile_increment_counter g_zxfer_profile_bucket_destination_inspection
	zxfer_profile_increment_counter g_zxfer_profile_bucket_property_reconciliation

	g_zxfer_failure_stage="property transfer"
	zxfer_profile_record_zfs_call destination send

	g_zxfer_failure_stage="send/receive"
	zxfer_profile_record_zfs_call destination receive
	zxfer_profile_record_zfs_call destination list
	zxfer_profile_record_zfs_call source get

	assertEquals "Destination-side zfs calls should include send, receive, and list verbs." \
		3 "$g_zxfer_profile_destination_zfs_calls"
	assertEquals "Source-side zfs calls should include the source get verb." \
		1 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Send verbs should increment the send counter." \
		1 "$g_zxfer_profile_zfs_send_calls"
	assertEquals "Receive verbs should increment the receive counter." \
		1 "$g_zxfer_profile_zfs_receive_calls"
	assertEquals "List verbs should increment the list counter." \
		1 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Get verbs should increment the get counter." \
		1 "$g_zxfer_profile_zfs_get_calls"
	assertEquals "Destination-inspection bucket accounting should include the direct bucket hit and send/receive destination list probes." \
		2 "$g_zxfer_profile_bucket_destination_inspection"
	assertEquals "Property-reconciliation bucket accounting should include the direct hit and property-transfer send probe." \
		2 "$g_zxfer_profile_bucket_property_reconciliation"
	assertEquals "Send/receive setup bucket accounting should include receive-side send/receive probes." \
		1 "$g_zxfer_profile_bucket_send_receive_setup"
	assertEquals "Source-inspection bucket accounting should include source-side get probes during send/receive setup." \
		1 "$g_zxfer_profile_bucket_source_inspection"
}

test_zxfer_profile_emit_summary_returns_without_output_when_already_emitted() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_summary_emitted=1
			zxfer_profile_emit_summary
		) 2>&1
	)
	status=$?

	assertEquals "An already-emitted profile summary should return success." 0 "$status"
	assertEquals "An already-emitted profile summary should not emit duplicate output." "" "$output"
}

test_zxfer_profile_increment_counter_normalizes_blank_and_invalid_inputs_in_current_shell() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_zxfer_profile_command_render_calls="bogus"

	zxfer_profile_increment_counter ""
	assertEquals "Blank profile counter names should be ignored without marking profile data present." \
		0 "$g_zxfer_profile_has_data"

	zxfer_profile_increment_counter g_zxfer_profile_command_render_calls "bogus"

	assertEquals "Profile counter updates should mark that profile data exists." 1 "$g_zxfer_profile_has_data"
	assertEquals "Invalid increment amounts and counter values should be normalized before incrementing." \
		1 "$g_zxfer_profile_command_render_calls"
}

test_zxfer_profile_now_ms_falls_back_to_second_resolution_when_millisecond_format_is_unavailable() {
	output=$(
		(
			# BSD date prints %3N literally after the seconds field.
			date() {
				printf '%s\n' "42 423N"
			}
			zxfer_profile_now_ms
		)
	)
	status=$?

	assertEquals "Profile millisecond timestamps should still succeed when date lacks %N-style support." \
		0 "$status"
	assertEquals "Second-resolution fallbacks should be normalized into millisecond units." \
		42000 "$output"
}

test_zxfer_profile_add_elapsed_ms_accumulates_only_valid_positive_durations_in_current_shell() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_zxfer_profile_snapshot_diff_sort_ms=5

	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms 10 25
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms bogus 30
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms 40 35

	assertEquals "Elapsed stage timings should accumulate onto existing millisecond totals." \
		20 "$g_zxfer_profile_snapshot_diff_sort_ms"
	assertEquals "Elapsed stage timings should mark that profiling data exists." \
		1 "$g_zxfer_profile_has_data"
}

test_zxfer_reset_profile_state_clears_owned_timing_and_counter_state() {
	g_zxfer_profile_has_data=1
	g_zxfer_profile_summary_emitted=1
	g_zxfer_profile_cleanup_ms=999
	g_zxfer_profile_ssh_shell_invocations=999
	g_zxfer_profile_runtime_artifact_files_created=999
	g_zxfer_profile_live_destination_snapshot_rechecks=999
	g_zxfer_profile_diverged_snapshot_warnings=999

	zxfer_reset_profile_state

	assertEquals "Profile reset should clear the data marker." 0 "$g_zxfer_profile_has_data"
	assertEquals "Profile reset should rearm summary emission." 0 "$g_zxfer_profile_summary_emitted"
	assertEquals "Profile reset should clear cleanup timing." 0 "$g_zxfer_profile_cleanup_ms"
	assertEquals "Profile reset should clear ssh counters." 0 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Profile reset should clear runtime artifact counters." \
		0 "$g_zxfer_profile_runtime_artifact_files_created"
	assertEquals "Profile reset should clear destination-recheck counters." \
		0 "$g_zxfer_profile_live_destination_snapshot_rechecks"
	assertEquals "Profile reset should clear diverged-snapshot counters." \
		0 "$g_zxfer_profile_diverged_snapshot_warnings"
}

# Every -V summary key, in order. Keys marked "retired" lost their producers
# and print a literal 0; command_render_calls belongs to zxfer_quoting.sh.
ZXFER_TEST_PROFILE_KEYS="elapsed_seconds startup_latency_ms cleanup_ms ssh_setup_ms source_snapshot_listing_ms destination_snapshot_listing_ms snapshot_diff_sort_ms ssh_control_socket_lock_wait_count ssh_control_socket_lock_wait_ms remote_capability_cache_wait_count remote_capability_cache_wait_ms remote_capability_bootstrap_live remote_capability_bootstrap_cache remote_capability_bootstrap_memory remote_cli_tool_direct_probes source_zfs_calls destination_zfs_calls other_zfs_calls zfs_list_calls zfs_get_calls zfs_send_calls zfs_receive_calls ssh_shell_invocations source_ssh_shell_invocations destination_ssh_shell_invocations other_ssh_shell_invocations source_snapshot_list_commands source_snapshot_list_parallel_commands send_receive_pipeline_commands send_receive_background_pipeline_commands exists_destination_calls normalized_property_reads_source normalized_property_reads_destination normalized_property_reads_other required_property_backfill_gets parent_destination_property_reads bucket_source_inspection bucket_destination_inspection bucket_property_reconciliation bucket_send_receive_setup runtime_artifact_files_created runtime_artifact_dirs_created runtime_artifact_paths_cleaned runtime_cache_object_writes runtime_cache_object_readbacks command_render_calls live_destination_snapshot_rechecks diverged_snapshot_warnings"
ZXFER_TEST_PROFILE_RETIRED_KEYS="ssh_control_socket_lock_wait_count ssh_control_socket_lock_wait_ms remote_capability_cache_wait_count remote_capability_cache_wait_ms remote_capability_bootstrap_cache runtime_cache_object_writes runtime_cache_object_readbacks"

test_zxfer_profile_emit_summary_prints_every_key_once_in_order() {
	output=$(
		(
			g_option_V_very_verbose=1
			zxfer_reset_profile_state
			g_zxfer_profile_has_data=1
			g_zxfer_profile_zfs_send_calls=4
			g_zxfer_profile_command_render_calls=2
			g_zxfer_profile_ssh_control_socket_lock_wait_count=9
			g_zxfer_profile_runtime_cache_object_readbacks=9
			zxfer_profile_emit_summary
		) 2>&1
	)
	keys=$(printf '%s\n' "$output" | sed -n 's/^zxfer profile: \([a-z_]*\)=.*$/\1/p' | tr '\n' ' ')

	assertEquals "The -V summary should print every stable key once, in order, and nothing else." \
		"$ZXFER_TEST_PROFILE_KEYS " "$keys"
	assertEquals "Every summary line should be a profile key line." \
		"" "$(printf '%s\n' "$output" | grep -v '^zxfer profile: [a-z_]*=')"
	assertContains "Counter values should be printed from their globals." \
		"$output" "zxfer profile: zfs_send_calls=4"
	assertContains "The quoting-owned render counter should be printed." \
		"$output" "zxfer profile: command_render_calls=2"
	assertContains "Retired keys should print a literal 0 even when a same-named global is set." \
		"$output" "zxfer profile: ssh_control_socket_lock_wait_count=0"
	assertContains "Retired cache readback keys should print a literal 0." \
		"$output" "zxfer profile: runtime_cache_object_readbacks=0"
	assertTrue "Elapsed seconds should be a whole number when the start time was recorded." \
		"printf '%s\n' \"\$output\" | grep -q '^zxfer profile: elapsed_seconds=[0-9][0-9]*\$'"
}

test_zxfer_profile_increment_counter_covers_every_live_summary_counter() {
	live_keys=""
	for key in $ZXFER_TEST_PROFILE_KEYS; do
		case " elapsed_seconds command_render_calls $ZXFER_TEST_PROFILE_RETIRED_KEYS " in
		*" $key "*) ;;
		*) live_keys="$live_keys $key" ;;
		esac
	done
	output=$(
		(
			g_option_V_very_verbose=1
			zxfer_reset_profile_state
			for key in $live_keys; do
				zxfer_profile_increment_counter "g_zxfer_profile_$key" 3
			done
			for key in $ZXFER_TEST_PROFILE_RETIRED_KEYS; do
				zxfer_profile_increment_counter "g_zxfer_profile_$key" 3
			done
			zxfer_profile_emit_summary
		) 2>&1
	)

	for key in $live_keys; do
		assertContains "Every live summary counter should be in the increment table: $key." \
			"$output" "zxfer profile: $key=3"
	done
	for key in $ZXFER_TEST_PROFILE_RETIRED_KEYS; do
		assertContains "Retired keys should not be counted: $key." \
			"$output" "zxfer profile: $key=0"
	done
}

test_zxfer_profile_read_clock_ms_parses_one_date_reading() {
	output=$(
		(
			date() {
				[ "$1" = "+%s %s%3N" ] || return 9
				printf '%s\n' "42 42123"
			}
			zxfer_profile_read_clock_ms
			printf 'gnu=%s:%s\n' "$?" "$g_zxfer_profile_clock_ms"
			date() {
				printf '%s\n' "42 423N"
			}
			zxfer_profile_read_clock_ms
			printf 'bsd=%s:%s\n' "$?" "$g_zxfer_profile_clock_ms"
			date() {
				printf '%s\n' "42123"
			}
			zxfer_profile_read_clock_ms
			printf 'one_field=%s:<%s>\n' "$?" "$g_zxfer_profile_clock_ms"
			date() {
				return 1
			}
			zxfer_profile_read_clock_ms
			printf 'missing=%s:<%s>\n' "$?" "$g_zxfer_profile_clock_ms"
		)
	)

	assertContains "GNU date should yield the millisecond field." "$output" "gnu=0:42123"
	assertContains "A literal %3N should fall back to seconds * 1000." "$output" "bsd=0:42000"
	assertContains "Output without both fields should fail and clear the result." "$output" "one_field=1:<>"
	assertContains "A failing date should fail and clear the result." "$output" "missing=1:<>"
}

test_zxfer_reset_profile_state_reads_the_clock_only_when_the_launcher_saw_V() {
	date_log="$TEST_TMPDIR/profile-reset-date.log"
	rm -f "$date_log"
	output=$(
		(
			date() {
				printf '%s\n' called >>"$date_log"
				printf '%s\n' "100 100250"
			}
			g_zxfer_profile_prescan=0
			zxfer_reset_profile_state
			printf 'quiet_start=<%s>\n' "$g_zxfer_profile_start_ms"
			printf 'quiet_dates=<%s>\n' "$(cat "$date_log" 2>/dev/null)"
			g_zxfer_profile_prescan=1
			zxfer_reset_profile_state
			printf 'verbose_start=<%s>\n' "$g_zxfer_profile_start_ms"
			unset g_zxfer_profile_prescan
			zxfer_reset_profile_state
			printf 'direct_start=<%s>\n' "$g_zxfer_profile_start_ms"
		)
	)

	assertContains "Without -V on the command line, profile reset should leave the start time empty." \
		"$output" "quiet_start=<>"
	assertContains "Without -V on the command line, profile reset should not run date." \
		"$output" "quiet_dates=<>"
	assertContains "With -V on the command line, profile reset should record the start time in ms." \
		"$output" "verbose_start=<100250>"
	assertContains "Callers that bypass the launcher prescan should still get a start time." \
		"$output" "direct_start=<100250>"
}

test_zxfer_profile_now_ms_returns_failure_when_date_is_unavailable() {
	zxfer_test_capture_subshell '
		date() {
			return 1
		}
		zxfer_profile_now_ms
	'

	assertEquals "Profile timestamps should fail cleanly when date cannot provide either format." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed profile timestamp lookups should not emit a value." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_profile_add_elapsed_ms_ignores_failed_clock_lookups_in_current_shell() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=0
			g_zxfer_profile_snapshot_diff_sort_ms=7
			zxfer_profile_now_ms() {
				return 1
			}
			zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms 10
			printf 'counter=%s\n' "$g_zxfer_profile_snapshot_diff_sort_ms"
			printf 'has_data=%s\n' "${g_zxfer_profile_has_data:-0}"
		)
	)

	assertEquals "Failed clock lookups should leave existing counters unchanged." \
		"counter=7
has_data=0" "$output"
}

test_zxfer_profile_add_elapsed_ms_normalizes_invalid_existing_counter_values() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_zxfer_profile_snapshot_diff_sort_ms="bogus"

	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms 10 15

	assertEquals "Elapsed-timing helpers should normalize invalid stored counter values before adding elapsed milliseconds." \
		5 "$g_zxfer_profile_snapshot_diff_sort_ms"
	assertEquals "Successful elapsed-timing updates should mark that profiling data exists." \
		1 "$g_zxfer_profile_has_data"
}

test_zxfer_profile_add_elapsed_ms_ignores_empty_counter_names_and_invalid_end_values() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=0
			g_zxfer_profile_snapshot_diff_sort_ms=9
			zxfer_profile_add_elapsed_ms "" 10 15
			zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms 10 "bad-end"
			printf 'counter=%s\n' "$g_zxfer_profile_snapshot_diff_sort_ms"
			printf 'has_data=%s\n' "${g_zxfer_profile_has_data:-0}"
		)
	)

	assertEquals "Elapsed-timing helpers should ignore empty counter names and invalid end timestamps without mutating state." \
		"counter=9
has_data=0" "$output"
}

test_zxfer_profile_helpers_ignore_untrusted_indirect_assignment_targets() {
	g_option_V_very_verbose=1
	g_zxfer_profile_has_data=0
	g_zxfer_profile_assignment_injected=0
	g_zxfer_profile_unknown_counter=4
	l_untrusted_counter_name='g_zxfer_profile_probe:-0}; g_zxfer_profile_assignment_injected=1; l_counter_value=${g_zxfer_profile_probe'

	zxfer_profile_increment_counter "$l_untrusted_counter_name"
	zxfer_profile_add_elapsed_ms "$l_untrusted_counter_name" 10 20
	zxfer_profile_increment_counter g_zxfer_profile_unknown_counter
	zxfer_profile_add_elapsed_ms g_zxfer_profile_unknown_counter 10 20

	assertEquals "Rejected profile targets should not mark profile data present." \
		0 "$g_zxfer_profile_has_data"
	assertEquals "Rejected profile targets should never be evaluated." \
		0 "$g_zxfer_profile_assignment_injected"
	assertEquals "Syntactically valid but unowned profile counters should remain unchanged." \
		4 "$g_zxfer_profile_unknown_counter"
}

test_zxfer_profile_recorders_always_return_success() {
	# Regression: profiling recorders are often the final statement of a
	# caller, so a non-zero recorder status leaks into replication control
	# flow. With -V active, zxfer_profile_record_zfs_call previously returned
	# 1 for destination-side calls because a trailing "[ side = source ] &&"
	# guard failed; that aborted batched -T destination discovery.
	l_failures=""
	for l_verbose in 0 1; do
		g_option_V_very_verbose=$l_verbose
		for l_stage in "" "snapshot discovery" "send/receive" "property transfer"; do
			g_zxfer_failure_stage=$l_stage
			for l_side in source destination other; do
				for l_verb in list get send receive; do
					if ! zxfer_profile_record_zfs_call "$l_side" "$l_verb"; then
						l_failures="$l_failures zfs_call:$l_verbose:$l_stage:$l_side:$l_verb"
					fi
				done
			done
		done
		if ! zxfer_profile_record_ssh_invocation "user@host" ""; then
			l_failures="$l_failures ssh:$l_verbose"
		fi
	done
	g_zxfer_failure_stage=""
	g_option_V_very_verbose=0

	assertEquals "Profiling recorders must never return a non-zero status." \
		"" "$l_failures"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
