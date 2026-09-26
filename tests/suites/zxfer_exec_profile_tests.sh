#!/bin/sh
# Tests for src/zxfer_profile.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
