#!/bin/sh
# Tests for src/zxfer_send_receive.sh and src/zxfer_send_jobs.sh, run by
# tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_calculate_size_estimate_uses_incremental_send_probe() {
	result=$(
		zxfer_run_source_zfs_cmd() { printf 'size\t2048\n'; }
		zxfer_calculate_size_estimate "tank/fs@snap2" "tank/fs@snap1"
		printf '%s' "$g_zxfer_progress_size_estimate_result"
	)
	assertEquals "2048" "$result"
}

test_calculate_size_estimate_handles_full_send_estimate() {
	result=$(
		zxfer_run_source_zfs_cmd() { printf 'size\t1024\n'; }
		zxfer_calculate_size_estimate "tank/fs@snap1" ""
		printf '%s' "$g_zxfer_progress_size_estimate_result"
	)
	assertEquals "1024" "$result"
}

test_wrap_command_with_ssh_without_compression_quotes_command() {
	result=$(
		g_cmd_ssh="/usr/bin/ssh"
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "backup@example.com" 0 send
		printf '%s' "$g_zxfer_wrapped_command_result"
	)
	assertEquals "'/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup@example.com' 'zfs send tank/src@snap'" "$result"
}

test_wrap_command_with_ssh_streams_compression_on_send() {
	result=$(
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_compress_safe="gzip"
		g_cmd_decompress_safe="gunzip"
		zxfer_wrap_command_with_ssh "zfs send tank/src@snap" "backup" 1 send
		printf '%s' "$g_zxfer_wrapped_command_result"
	)
	assertEquals "'/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'backup' 'zfs send tank/src@snap | gzip' | gunzip" "$result"
}

test_zfs_send_receive_renders_incremental_raw_verbose_send_and_forced_receive() {
	result=$(
		g_cmd_zfs="/sbin/zfs"
		g_option_V_very_verbose=1
		g_option_w_raw_send=1
		g_option_F_force_rollback="-F"
		zxfer_echoV() { :; }
		zxfer_schedule_send_receive_pipeline() { printf '%s' "$1"; }
		zxfer_zfs_send_receive "tank/fs@snap1" "tank/fs@snap2" "tank/dst" 0
	)
	assertEquals "'/sbin/zfs' 'send' '-v' '-w' '-I' 'tank/fs@snap1' 'tank/fs@snap2' | '/sbin/zfs' 'receive' '-F' 'tank/dst'" "$result"
}

test_wait_for_zfs_send_jobs_clears_job_list_on_success() {
	output=$(
		(
			zxfer_reset_send_job_state
			zxfer_reset_send_receive_state
			zxfer_note_destination_receive_completed() { :; }
			zxfer_invalidate_destination_property_mutation_cache() { :; }
			zxfer_mark_live_destination_dataset_dirty() { :; }
			zxfer_verify_converged_destination_after_receive() { :; }
			zxfer_spawn_send_job "sleep 1" "tank/a@snap" "backup/a"
			zxfer_spawn_send_job "sleep 1" "tank/b@snap" "backup/b"
			zxfer_wait_for_zfs_send_jobs "unit"
			printf 'jobs=<%s> count=%s\n' "$g_zxfer_send_jobs" "$g_count_zfs_send_jobs"
		)
	)
	assertEquals "Waiting for every send job should leave the job list empty." \
		"jobs=<> count=0" "$output"
}

test_wait_for_zfs_send_jobs_reports_failure() {
	(
		zxfer_reset_send_job_state
		zxfer_reset_send_receive_state
		g_zxfer_send_job_abort_grace_seconds=0
		zxfer_note_destination_receive_completed() { :; }
		zxfer_invalidate_destination_property_mutation_cache() { :; }
		zxfer_mark_live_destination_dataset_dirty() { :; }
		zxfer_verify_converged_destination_after_receive() { :; }
		zxfer_throw_error() {
			echo "send failure"
			exit 1
		}
		zxfer_spawn_send_job "exit 0" "tank/a@snap" "backup/a"
		zxfer_spawn_send_job "exit 3" "tank/b@snap" "backup/b"
		zxfer_wait_for_zfs_send_jobs "failure"
	) >/dev/null 2>&1
	assertEquals "Job failures should surface via zxfer_throw_error." 1 "$?"
}
