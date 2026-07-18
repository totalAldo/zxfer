#!/bin/sh
# Compatibility aliases for runtime tests whose cleanup ownership and explicit
# wait boundaries changed while their externally selectable names remain
# supported.

test_zxfer_abort_cleanup_pid_signals_and_unregisters_live_tracked_children() {
	test_zxfer_abort_cleanup_pid_signals_live_tracked_children_until_waited
}

test_zxfer_kill_registered_cleanup_pids_preserves_first_failure_message_and_rebuilds_tracked_pids() {
	test_zxfer_kill_registered_cleanup_pids_reaps_successes_and_preserves_failures
}
