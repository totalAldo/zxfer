#!/bin/sh
# shellcheck shell=sh
# Destination listing, normalization and no-op-proof producer cases for
# src/zxfer_snapshot_discovery.sh that only a unit test can reach: the
# snapshot record awk's rewrite and exclude rules, awk/sort and staging
# failures, and producer registration and abort edges. Run by
# tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Destination listing stand-in for the writer tests: the recursive snapshot
# listing prints DEST_LIST_STDOUT and DEST_LIST_STDERR and exits
# DEST_LIST_STATUS; the exact `zfs list -H DATASET` existence probe prints
# DEST_PROBE_OUTPUT to stderr and exits DEST_PROBE_STATUS. Every call is
# logged to DEST_LIST_LOG.
zxfer_test_stub_destination_listing() {
	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$DEST_LIST_LOG"
		if [ "$2" = "-Hr" ]; then
			[ -z "${DEST_LIST_STDOUT:-}" ] || printf '%s\n' "$DEST_LIST_STDOUT"
			[ -z "${DEST_LIST_STDERR:-}" ] || printf '%s\n' "$DEST_LIST_STDERR" >&2
			return "${DEST_LIST_STATUS:-0}"
		fi
		[ -z "${DEST_PROBE_OUTPUT:-}" ] || printf '%s\n' "$DEST_PROBE_OUTPUT" >&2
		return "${DEST_PROBE_STATUS:-0}"
	}
}

test_write_destination_snapshot_list_to_files_reports_empty_stage_failures_when_destination_missing() {
	full_file="$TEST_TMPDIR/dest_missing_stage_fail_full.txt"
	norm_file="$TEST_TMPDIR/dest_missing_stage_fail_norm.txt"
	DEST_LIST_LOG="$TEST_TMPDIR/dest_missing_stage_fail.log"
	: >"$DEST_LIST_LOG"

	zxfer_test_capture_subshell "
		zxfer_test_stub_destination_listing
		DEST_LIST_STATUS=1
		DEST_PROBE_OUTPUT='dataset does not exist'
		DEST_PROBE_STATUS=1
		zxfer_write_runtime_artifact_file() {
			return 42
		}
		zxfer_write_destination_snapshot_list_to_files '$full_file' '$norm_file'
	"

	assertEquals "Destination discovery should fail closed when staging an empty missing-destination snapshot list fails." \
		42 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing destination staging failures should preserve the empty-list staging context." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to stage empty destination snapshot list."
}

# A destination proof producer whose cleanup registration fails is stopped
# and forgotten, and the start fails closed.
test_start_destination_snapshot_name_sorted_fifo_producer_stops_an_unregistered_producer() {
	registration_output=$(
		(
			zxfer_get_temp_file >/dev/null || exit 1
			test_output=$g_zxfer_temp_file_result
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst/src@snapA"
			}
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_fast_noop_background_pid() {
				kill "$1" 2>/dev/null || :
				return 0
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer \
				"$test_output" \
				"$TEST_TMPDIR/dest_fifo_registration.err" \
				"$TEST_TMPDIR/dest_fifo_registration.status" \
				"$TEST_TMPDIR/dest_fifo_registration.raw"
			printf 'status=%s pid=<%s>\n' "$?" "$g_last_background_pid"
		)
	)

	assertEquals "Destination FIFO producer should fail closed and forget the producer when cleanup registration fails." \
		"status=1 pid=<>" "$registration_output"
}

test_abort_fast_noop_background_pid_covers_invalid_and_fallback_paths() {
	zxfer_abort_fast_noop_background_pid "" "invalid"
	invalid_status=$?
	log="$TEST_TMPDIR/fast_noop_abort.log"
	: >"$log"

	(
		(exit 0) &
		dead_pid=$!
		wait "$dead_pid" 2>/dev/null || :
		zxfer_abort_fast_noop_background_pid "$dead_pid" "finished proof helper"
		printf 'dead_status=%s\n' "$?" >>"$log"
	)
	(
		LOG_FILE="$log"
		zxfer_abort_cleanup_pid() {
			printf 'cleanup=%s\n' "$1" >>"$LOG_FILE"
			return 1
		}
		zxfer_abort_direct_child_pid() {
			printf 'registered_direct=%s:%s\n' "$1" "$2" >>"$LOG_FILE"
			return 1
		}
		sleep 5 &
		child_pid=$!
		g_zxfer_cleanup_pid_records="$child_pid	registered proof helper"
		zxfer_abort_fast_noop_background_pid "$child_pid" "test proof helper"
		printf 'registered_status=%s\n' "$?" >>"$LOG_FILE"
		command kill -s TERM "$child_pid" >/dev/null 2>&1 || :
		wait "$child_pid" 2>/dev/null || :
	)
	(
		LOG_FILE="$log"
		zxfer_abort_direct_child_pid() {
			printf 'direct=%s:%s\n' "$1" "$2" >>"$LOG_FILE"
			return 1
		}
		sleep 5 &
		child_pid=$!
		zxfer_abort_fast_noop_background_pid \
			"$child_pid" "unregistered proof helper"
		printf 'direct_status=%s\n' "$?" >>"$LOG_FILE"
		command kill -s TERM "$child_pid" >/dev/null 2>&1 || :
		wait "$child_pid" 2>/dev/null || :
	)

	assertEquals "Invalid fast no-op abort pids should be ignored." 0 "$invalid_status"
	assertContains "Fast no-op abort should accept untracked helpers that already exited." \
		"$(cat "$log")" "dead_status=0"
	assertContains "Fast no-op abort should try the registered cleanup helper first." \
		"$(cat "$log")" "cleanup="
	assertContains "A failed identity-aware registered abort should remain a failure." \
		"$(cat "$log")" "registered_status=1"
	assertNotContains "A registered helper must not fall back to a weaker direct-child signal path." \
		"$(cat "$log")" "registered_direct="
	assertContains "Unregistered helpers should use only the identity-aware direct-child path." \
		"$(cat "$log")" "direct="
	assertContains "A failed identity-aware direct-child abort should remain a failure." \
		"$(cat "$log")" "direct_status=1"
}

# ZXFER_SNAPSHOT_RECORD_AWK through its three callers: the exclude filter
# drops records by their dataset only (never by snapshot name or guid), the
# destination normalizer rewrites the destination root prefix (and only that
# exact prefix) to the source's and sorts, and the no-op proof's stream
# normalizer rewrites before it drops excluded datasets.
test_snapshot_record_awk_rewrites_destination_paths_and_drops_excluded_datasets() {
	input_file="$TEST_TMPDIR/record_awk_input.txt"
	output_file="$TEST_TMPDIR/record_awk_output.txt"
	# caller|destination|initial source|exclude|records|expected; records are
	# comma-separated and "=" stands for the tab before a guid.
	while IFS='|' read -r l_caller l_destination l_source l_exclude l_records l_expected; do
		printf '%s\n' "$l_records" | tr ',=' '\n\t' >"$input_file"
		(
			g_initial_source=$l_source
			g_option_x_exclude_datasets=$l_exclude
			case $l_caller in
			filter)
				zxfer_filter_snapshot_file_with_excludes "$input_file" "$output_file"
				;;
			normalize)
				zxfer_normalize_destination_snapshot_list "$l_destination" \
					"$input_file" "$output_file"
				;;
			stream)
				zxfer_normalize_destination_snapshot_stream_for_noop_proof \
					"$l_destination" <"$input_file" >"$output_file"
				;;
			esac
		)
		assertEquals "The record awk [$l_caller: $l_records]." \
			"$(printf '%s\n' "$l_expected" | tr ',=' '\n\t')" "$(cat "$output_file")"
	done <<'EOF'
filter|||/replica$|tank/src/replica@snapA,tank/src@snap-replica,tank/src@snapA=guidA|tank/src@snap-replica,tank/src@snapA=guidA
filter||||tank/src/app@snap2,tank/src/app@snap1|tank/src/app@snap2,tank/src/app@snap1
normalize|tank/backup/app|tank/src/app||tank/backup/app@snap2,tank/backup/app@snap1|tank/src/app@snap1,tank/src/app@snap2
normalize|backup/dst|tank/src||backup/dst/child@snap2,backup/dst@snap1|tank/src/child@snap2,tank/src@snap1
normalize|tank/dst|tank/dst||tank/dst@snapB,tank/dst@snapA|tank/dst@snapA,tank/dst@snapB
normalize|backup/dst|tank/src||backup/dst/backup/dst/child@snap2=222,backup/dst@snap1=111|tank/src/backup/dst/child@snap2=222,tank/src@snap1=111
normalize|backup/dst|tank/src||backup/dst-old@snap1,backup/dst@snap1|backup/dst-old@snap1,tank/src@snap1
stream|backup/dst/src|tank/src|/replica$|backup/dst/src@snapA,backup/dst/src/replica@snapB|tank/src@snapA
stream|backup/dst|tank/src||backup/dst@snapA|tank/src@snapA
stream|backup/dst|tank/src|/replica$|backup/dst@snapA,backup/dst/replica@snapB|tank/src@snapA
EOF
}

test_normalize_destination_snapshot_list_preserves_awk_and_sort_failures() {
	input_file="$TEST_TMPDIR/dest_normalize_failure_input.txt"
	output_file="$TEST_TMPDIR/dest_normalize_failure_output.txt"
	fake_awk="$TEST_TMPDIR/dest_normalize_failure_awk.sh"
	printf '%s\n' "backup/dst@snap1" >"$input_file"
	cat >"$fake_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "normalize awk failed" >&2
exit 42
EOF
	chmod +x "$fake_awk"

	output=$(
		(
			g_cmd_awk=$fake_awk
			zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"
			printf 'awk=%s\n' "$?"
		) 2>&1
		(
			sort() {
				return 63
			}
			zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"
			printf 'sort=%s\n' "$?"
		)
	)

	assertContains "Destination normalization should preserve awk diagnostics." \
		"$output" "normalize awk failed"
	assertContains "Destination normalization should preserve awk exit status." \
		"$output" "awk=42"
	assertContains "Destination normalization should preserve sort failures." \
		"$output" "sort=63"
}
