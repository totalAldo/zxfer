#!/bin/sh
#
# shunit2 tests for the -V profiling counters and summary in
# src/zxfer_profile.sh.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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

# Every -V summary key, in order. Keys marked "retired" lost their producers
# and print a literal 0; ssh_shell_invocations is derived from the per-side
# counters and elapsed_seconds from the start clock.
ZXFER_TEST_PROFILE_KEYS="elapsed_seconds startup_latency_ms cleanup_ms ssh_setup_ms source_snapshot_listing_ms destination_snapshot_listing_ms snapshot_diff_sort_ms ssh_control_socket_lock_wait_count ssh_control_socket_lock_wait_ms remote_capability_cache_wait_count remote_capability_cache_wait_ms remote_capability_bootstrap_live remote_capability_bootstrap_cache remote_capability_bootstrap_memory remote_cli_tool_direct_probes source_zfs_calls destination_zfs_calls other_zfs_calls zfs_list_calls zfs_get_calls zfs_send_calls zfs_receive_calls ssh_shell_invocations source_ssh_shell_invocations destination_ssh_shell_invocations other_ssh_shell_invocations source_snapshot_list_commands source_snapshot_list_parallel_commands send_receive_pipeline_commands send_receive_background_pipeline_commands exists_destination_calls normalized_property_reads_source normalized_property_reads_destination normalized_property_reads_other required_property_backfill_gets parent_destination_property_reads bucket_source_inspection bucket_destination_inspection bucket_property_reconciliation bucket_send_receive_setup runtime_artifact_files_created runtime_artifact_dirs_created runtime_artifact_paths_cleaned runtime_cache_object_writes runtime_cache_object_readbacks command_render_calls live_destination_snapshot_rechecks diverged_snapshot_warnings"
ZXFER_TEST_PROFILE_RETIRED_KEYS="ssh_control_socket_lock_wait_count ssh_control_socket_lock_wait_ms remote_capability_cache_wait_count remote_capability_cache_wait_ms remote_capability_bootstrap_cache normalized_property_reads_other runtime_cache_object_writes runtime_cache_object_readbacks"

# Purpose: Print the summary keys that print their own global.
# Usage: profile_test_counter_keys
profile_test_counter_keys() {
	for l_key in $ZXFER_TEST_PROFILE_KEYS; do
		case " elapsed_seconds ssh_shell_invocations $ZXFER_TEST_PROFILE_RETIRED_KEYS " in
		*" $l_key "*) ;;
		*) printf '%s\n' "$l_key" ;;
		esac
	done
}

test_zxfer_profile_record_ssh_invocation_tracks_other_and_inferred_sides() {
	g_option_V_very_verbose=1
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"

	zxfer_profile_record_ssh_invocation "wrapper.example" other
	zxfer_profile_record_ssh_invocation "target.example"
	zxfer_profile_record_ssh_invocation "unknown.example"
	zxfer_profile_record_ssh_invocation "target.example" source

	assertEquals "Inferred destination-side attribution should count target-host ssh invocations." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
	assertEquals "Explicit other-side attribution and unknown hosts should both count toward the other-side ssh bucket." \
		2 "$g_zxfer_profile_other_ssh_shell_invocations"
	assertEquals "An explicit side should win over the host-spec match." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"

	g_option_V_very_verbose=0
	zxfer_profile_record_ssh_invocation "origin.example"
	assertEquals "Without -V the recorder should count nothing." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_zxfer_profile_record_zfs_call_tracks_sides_verbs_and_buckets() {
	g_option_V_very_verbose=1

	g_zxfer_failure_stage="property transfer"
	zxfer_profile_record_zfs_call destination send

	g_zxfer_failure_stage="send/receive"
	zxfer_profile_record_zfs_call destination receive
	zxfer_profile_record_zfs_call destination list
	zxfer_profile_record_zfs_call source get
	zxfer_profile_record_zfs_call other list
	zxfer_profile_record_zfs_call local destroy

	g_zxfer_failure_stage="snapshot discovery"
	zxfer_profile_record_zfs_call source snapshot

	assertEquals "Destination-side zfs calls should include send, receive, and list verbs." \
		3 "$g_zxfer_profile_destination_zfs_calls"
	assertEquals "Source-side zfs calls should include every verb." \
		2 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Any other side should count as other." \
		2 "$g_zxfer_profile_other_zfs_calls"
	assertEquals "Send verbs should increment the send counter." \
		1 "$g_zxfer_profile_zfs_send_calls"
	assertEquals "Receive verbs should increment the receive counter." \
		1 "$g_zxfer_profile_zfs_receive_calls"
	assertEquals "List verbs should increment the list counter." \
		2 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Get verbs should increment the get counter." \
		1 "$g_zxfer_profile_zfs_get_calls"
	assertEquals "Destination list probes outside property transfer count as destination inspection." \
		1 "$g_zxfer_profile_bucket_destination_inspection"
	assertEquals "Every call during property transfer counts toward property reconciliation." \
		1 "$g_zxfer_profile_bucket_property_reconciliation"
	assertEquals "Send and receive verbs during send/receive count toward its setup bucket." \
		1 "$g_zxfer_profile_bucket_send_receive_setup"
	assertEquals "Source get probes and every snapshot-discovery call count as source inspection." \
		2 "$g_zxfer_profile_bucket_source_inspection"

	g_option_V_very_verbose=0
	zxfer_profile_record_zfs_call source list
	assertEquals "Without -V the recorder should count nothing." \
		2 "$g_zxfer_profile_source_zfs_calls"
}

test_zxfer_profile_timers_measure_only_valid_forward_readings_under_V() {
	output=$(
		(
			l_clock_log="$TEST_TMPDIR/timer-clock.log"
			: >"$l_clock_log"
			zxfer_profile_read_clock_ms() {
				printf '%s\n' read >>"$l_clock_log"
				g_zxfer_profile_clock_ms=$ZXFER_TEST_CLOCK_MS
			}
			g_option_V_very_verbose=0
			ZXFER_TEST_CLOCK_MS=10
			zxfer_profile_start_timer
			printf 'quiet_start=<%s>\n' "$g_zxfer_profile_clock_ms"
			zxfer_profile_stop_timer 5
			printf 'quiet_elapsed=%s has_data=%s reads=%s\n' "$g_zxfer_profile_elapsed_ms" \
				"$g_zxfer_profile_has_data" "$(wc -l <"$l_clock_log" | tr -d ' ')"

			g_option_V_very_verbose=1
			zxfer_profile_start_timer
			printf 'start=<%s>\n' "$g_zxfer_profile_clock_ms"
			ZXFER_TEST_CLOCK_MS=25
			zxfer_profile_stop_timer 10
			printf 'elapsed=%s has_data=%s\n' "$g_zxfer_profile_elapsed_ms" "$g_zxfer_profile_has_data"
			g_zxfer_profile_has_data=0
			zxfer_profile_stop_timer bogus
			printf 'bogus=%s\n' "$g_zxfer_profile_elapsed_ms"
			zxfer_profile_stop_timer ""
			printf 'empty=%s\n' "$g_zxfer_profile_elapsed_ms"
			zxfer_profile_stop_timer 40
			printf 'backwards=%s has_data=%s\n' "$g_zxfer_profile_elapsed_ms" "$g_zxfer_profile_has_data"
			zxfer_profile_stop_timer 25
			printf 'zero=%s has_data=%s\n' "$g_zxfer_profile_elapsed_ms" "$g_zxfer_profile_has_data"
		)
	)

	assertContains "Without -V the start timer should leave no start and read no clock." \
		"$output" "quiet_start=<>"
	assertContains "Without -V the stop timer should measure nothing and read no clock." \
		"$output" "quiet_elapsed=0 has_data=0 reads=0"
	assertContains "Under -V the start timer should publish the clock reading." \
		"$output" "start=<10>"
	assertContains "Under -V the stop timer should publish the milliseconds since the start and mark data." \
		"$output" "elapsed=15 has_data=1"
	assertContains "A non-numeric start should measure 0." "$output" "bogus=0"
	assertContains "An empty start (no -V at the start) should measure 0." "$output" "empty=0"
	assertContains "A clock that went backwards should measure 0 and mark nothing." \
		"$output" "backwards=0 has_data=0"
	assertContains "A 0 ms stage is still a measurement that marks data." \
		"$output" "zero=0 has_data=1"
}

test_zxfer_profile_timers_ignore_failed_clock_readings() {
	output=$(
		(
			g_option_V_very_verbose=1
			date() {
				return 1
			}
			zxfer_profile_start_timer
			printf 'start_status=%s start=<%s>\n' "$?" "$g_zxfer_profile_clock_ms"
			zxfer_profile_stop_timer 10
			printf 'stop_status=%s elapsed=%s has_data=%s\n' "$?" \
				"$g_zxfer_profile_elapsed_ms" "$g_zxfer_profile_has_data"
		)
	)

	assertContains "A failed clock read should leave no start and still return 0." \
		"$output" "start_status=0 start=<>"
	assertContains "A failed clock read should measure 0, mark nothing, and return 0." \
		"$output" "stop_status=0 elapsed=0 has_data=0"
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

test_zxfer_profile_emit_summary_stays_silent_until_V_timed_or_counted_something() {
	output=$(
		(
			g_option_V_very_verbose=1
			zxfer_profile_emit_summary
			printf 'nothing_status=%s\n' "$?"
			g_zxfer_profile_command_render_calls=5
			g_option_V_very_verbose=0
			zxfer_profile_emit_summary
			printf 'quiet_status=%s\n' "$?"
			g_option_V_very_verbose=1
			zxfer_profile_emit_summary
		) 2>&1
	)
	timed_output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			zxfer_profile_emit_summary
		) 2>&1
	)

	assertEquals "A -V run that counted nothing should print no summary." \
		"nothing_status=0" "$(printf '%s\n' "$output" | sed -n 1p)"
	assertEquals "Without -V a counted run should print no summary." \
		"quiet_status=0" "$(printf '%s\n' "$output" | sed -n 2p)"
	assertEquals "One counted value should print the summary under -V, once." \
		1 "$(printf '%s\n' "$output" | grep -c '^zxfer profile: elapsed_seconds=')"
	assertContains "A counted value should reach the summary." \
		"$output" "zxfer profile: command_render_calls=5"
	assertContains "A timed stage should print the summary even when every count is 0." \
		"$timed_output" "zxfer profile: command_render_calls=0"
}

test_zxfer_reset_profile_state_clears_owned_timing_and_counter_state() {
	g_zxfer_profile_has_data=1
	g_zxfer_profile_summary_emitted=1
	g_zxfer_profile_startup_latency_recorded=1
	g_zxfer_profile_cleanup_ms=999
	g_zxfer_profile_source_ssh_shell_invocations=999
	g_zxfer_profile_runtime_artifact_files_created=999
	g_zxfer_profile_live_destination_snapshot_rechecks=999
	g_zxfer_profile_diverged_snapshot_warnings=999

	zxfer_reset_profile_state

	assertEquals "Profile reset should clear the data marker." 0 "$g_zxfer_profile_has_data"
	assertEquals "Profile reset should rearm summary emission." 0 "$g_zxfer_profile_summary_emitted"
	assertEquals "Profile reset should rearm the startup latency reading." \
		0 "$g_zxfer_profile_startup_latency_recorded"
	assertEquals "Profile reset should clear cleanup timing." 0 "$g_zxfer_profile_cleanup_ms"
	assertEquals "Profile reset should clear ssh counters." 0 "$g_zxfer_profile_source_ssh_shell_invocations"
	assertEquals "Profile reset should clear runtime artifact counters." \
		0 "$g_zxfer_profile_runtime_artifact_files_created"
	assertEquals "Profile reset should clear destination-recheck counters." \
		0 "$g_zxfer_profile_live_destination_snapshot_rechecks"
	assertEquals "Profile reset should clear diverged-snapshot counters." \
		0 "$g_zxfer_profile_diverged_snapshot_warnings"
}

test_zxfer_reset_profile_state_zeroes_every_printed_counter_against_inherited_values() {
	# Producers bump counters with plain arithmetic, which bash and ksh would
	# evaluate as an expression; the reset must neutralize every inherited
	# value first. export NAME=VALUE assigns a computed name without eval.
	injected_file="$TEST_TMPDIR/profile-injected"
	rm -f "$injected_file"
	output=$(
		(
			for l_key in $(profile_test_counter_keys); do
				export "g_zxfer_profile_$l_key=x[\$(: >$injected_file)]"
			done
			g_option_V_very_verbose=1
			zxfer_reset_profile_state
			zxfer_render_shell_command_from_argv zfs list
			g_zxfer_profile_has_data=1
			zxfer_profile_emit_summary
		) 2>&1
	)

	for key in $(profile_test_counter_keys); do
		if [ "$key" = command_render_calls ]; then
			assertContains "An inline producer should count from 0 after the reset." \
				"$output" "zxfer profile: $key=1"
		else
			assertContains "The reset should zero the inherited $key counter." \
				"$output" "zxfer profile: $key=0"
		fi
	done
	assertFalse "No inherited counter value may be evaluated." "[ -e '$injected_file' ]"
}

test_zxfer_profile_emit_summary_prints_every_key_once_in_order() {
	output=$(
		(
			g_option_V_very_verbose=1
			zxfer_reset_profile_state
			g_zxfer_profile_zfs_send_calls=4
			g_zxfer_profile_command_render_calls=2
			g_zxfer_profile_source_ssh_shell_invocations=1
			g_zxfer_profile_destination_ssh_shell_invocations=2
			g_zxfer_profile_other_ssh_shell_invocations=3
			g_zxfer_profile_ssh_shell_invocations=9
			g_zxfer_profile_ssh_control_socket_lock_wait_count=9
			g_zxfer_profile_normalized_property_reads_other=9
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
	assertContains "The ssh total should be the sum of the three sides, whatever a same-named global holds." \
		"$output" "zxfer profile: ssh_shell_invocations=6"
	assertContains "Retired keys should print a literal 0 even when a same-named global is set." \
		"$output" "zxfer profile: ssh_control_socket_lock_wait_count=0"
	assertContains "The unreachable other-side property read key should print a literal 0." \
		"$output" "zxfer profile: normalized_property_reads_other=0"
	assertContains "Retired cache readback keys should print a literal 0." \
		"$output" "zxfer profile: runtime_cache_object_readbacks=0"
	assertTrue "Elapsed seconds should be a whole number when the start time was recorded." \
		"printf '%s\n' \"\$output\" | grep -q '^zxfer profile: elapsed_seconds=[0-9][0-9]*\$'"
}

test_zxfer_profile_emit_summary_prints_every_counter_from_its_own_global() {
	output=$(
		(
			g_option_V_very_verbose=1
			zxfer_reset_profile_state
			for l_key in $(profile_test_counter_keys); do
				export "g_zxfer_profile_$l_key=3"
			done
			zxfer_profile_emit_summary
		) 2>&1
	)

	for key in $(profile_test_counter_keys); do
		assertContains "The summary should print $key from g_zxfer_profile_$key." \
			"$output" "zxfer profile: $key=3"
	done
	assertContains "The ssh total should add up the three per-side counters." \
		"$output" "zxfer profile: ssh_shell_invocations=9"
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
			for l_side in source destination other local; do
				for l_verb in list get send receive destroy; do
					if ! zxfer_profile_record_zfs_call "$l_side" "$l_verb"; then
						l_failures="$l_failures zfs_call:$l_verbose:$l_stage:$l_side:$l_verb"
					fi
				done
			done
		done
		for l_side in "" source destination other; do
			if ! zxfer_profile_record_ssh_invocation "user@host" "$l_side"; then
				l_failures="$l_failures ssh:$l_verbose:$l_side"
			fi
		done
		zxfer_profile_start_timer || l_failures="$l_failures start_timer:$l_verbose"
		for l_start in "" bogus 99999999999999 0; do
			if ! zxfer_profile_stop_timer "$l_start"; then
				l_failures="$l_failures stop_timer:$l_verbose:$l_start"
			fi
		done
	done
	g_zxfer_failure_stage=""
	g_option_V_very_verbose=0

	assertEquals "Profiling recorders and timers must never return a non-zero status." \
		"" "$l_failures"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
