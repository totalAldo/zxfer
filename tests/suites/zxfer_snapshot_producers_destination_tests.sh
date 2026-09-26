#!/bin/sh
# shellcheck shell=sh
# Destination snapshot listing, normalization and no-op-proof stream cases for
# src/zxfer_snapshot_producers.sh. Run by tests/test_zxfer_snapshot_producers.sh.
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

test_write_destination_snapshot_list_to_files_lists_without_an_existence_probe() {
	set +e
	full_file="$TEST_TMPDIR/dest_existing_full.txt"
	norm_file="$TEST_TMPDIR/dest_existing_norm.txt"
	DEST_LIST_LOG="$TEST_TMPDIR/dest_existing.log"
	stderr_file="$TEST_TMPDIR/dest_existing.err"
	: >"$DEST_LIST_LOG"

	output=$(
		(
			zxfer_test_stub_destination_listing
			DEST_LIST_STDOUT="backup/dst/src@snap1	111"
			DEST_LIST_STDERR="zfs: listing warning"
			zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
			printf 'status=%s\n' "$?"
			zxfer_lookup_destination_existence_cache "backup/dst/src"
			printf 'cache=%s\n' "$g_zxfer_destination_existence_cache_entry_result"
		) 2>"$stderr_file"
	)

	assertContains "A successful destination listing should succeed." "$output" "status=0"
	assertEquals "A successful listing should pass its warnings on to stderr." \
		"zfs: listing warning" "$(cat "$stderr_file")"
	assertEquals "A successful listing should be the only destination zfs call." \
		"list -Hr -o name,guid -t snapshot backup/dst/src" "$(cat "$DEST_LIST_LOG")"
	assertContains "A successful listing should record that the destination root exists." \
		"$output" "cache=1"
	assertEquals "The raw listing should be staged unchanged." \
		"backup/dst/src@snap1	111" "$(cat "$full_file")"
	assertEquals "The normalized listing should use source paths." \
		"tank/src@snap1	111" "$(cat "$norm_file")"
}

test_write_destination_snapshot_list_to_files_outputs_empty_when_destination_missing() {
	set +e
	full_file="$TEST_TMPDIR/dest_missing_full.txt"
	norm_file="$TEST_TMPDIR/dest_missing_norm.txt"
	DEST_LIST_LOG="$TEST_TMPDIR/dest_missing.log"
	: >"$DEST_LIST_LOG"

	(
		zxfer_test_stub_destination_listing
		DEST_LIST_STDERR="cannot open 'backup/dst/src': dataset does not exist"
		DEST_LIST_STATUS=1
		DEST_PROBE_OUTPUT=$DEST_LIST_STDERR
		DEST_PROBE_STATUS=1
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	)

	assertContains "A failed listing should be classified by an exact existence probe of the root." \
		"$(cat "$DEST_LIST_LOG")" "list -H backup/dst/src"
	assertEquals "Missing destination datasets should yield an empty raw snapshot file." "" "$(cat "$full_file")"
	assertEquals "Missing destination datasets should yield an empty normalized snapshot file." "" "$(cat "$norm_file")"
}

test_write_destination_snapshot_list_to_files_reports_destination_probe_failures() {
	full_file="$TEST_TMPDIR/dest_probe_fail_full.txt"
	norm_file="$TEST_TMPDIR/dest_probe_fail_norm.txt"
	DEST_LIST_LOG="$TEST_TMPDIR/dest_probe_fail.log"
	: >"$DEST_LIST_LOG"

	set +e
	output=$(
		(
			zxfer_test_stub_destination_listing
			DEST_LIST_STATUS=1
			DEST_PROBE_OUTPUT="permission denied"
			DEST_PROBE_STATUS=1
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
		)
	)
	status=$?

	assertEquals "Destination snapshot discovery should fail closed when destination existence checks fail." 1 "$status"
	assertContains "Destination snapshot discovery should surface the destination probe failure." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src] exists: permission denied"
}

test_write_destination_snapshot_list_to_files_reports_snapshot_listing_failures() {
	full_file="$TEST_TMPDIR/dest_list_fail_full.txt"
	norm_file="$TEST_TMPDIR/dest_list_fail_norm.txt"
	DEST_LIST_LOG="$TEST_TMPDIR/dest_list_fail.log"
	: >"$DEST_LIST_LOG"

	set +e
	output=$(
		(
			zxfer_test_stub_destination_listing
			# A missing child in the listing's stderr must not pass for a
			# missing root while the exact probe finds the root.
			DEST_LIST_STDERR="cannot open 'backup/dst/src/child': dataset does not exist"
			DEST_LIST_STATUS=1
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
		) 2>&1
	)
	status=$?

	assertEquals "Destination snapshot discovery should abort when listing snapshots fails." 1 "$status"
	assertContains "Destination snapshot listing failures should surface the listing stderr." \
		"$output" "cannot open 'backup/dst/src/child': dataset does not exist"
	assertContains "Destination snapshot listing failures should surface the generic destination snapshot-list error." \
		"$output" "Failed to retrieve snapshot list from the destination."
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

test_write_destination_snapshot_list_to_files_uses_destination_root_for_trailing_slash_sources() {
	full_file="$TEST_TMPDIR/dest_trailing_existing_full.txt"
	norm_file="$TEST_TMPDIR/dest_trailing_existing_norm.txt"
	cmd_file="$TEST_TMPDIR/dest_trailing_existing_cmd.txt"
	g_initial_source_had_trailing_slash=1
	g_initial_source="tank/src"
	g_destination="backup/dst"

	(
		g_option_V_very_verbose=1
		zxfer_record_last_command_string() {
			:
		}
		# shellcheck disable=SC2317,SC2329  # Invoked indirectly via g_cmd_zfs.
		fake_rzfs() {
			printf '%s\n' "$*" >"$cmd_file"
			printf '%s\n' "backup/dst/child@snap2" "backup/dst@snap1"
		}
		g_cmd_zfs="fake_rzfs"
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file" 2>/dev/null
	)

	assertContains "Trailing-slash replication should list snapshots from the destination root dataset, not a child suffix." \
		"$(cat "$cmd_file")" "snapshot backup/dst"
	assertNotContains "Trailing-slash replication should not append the source basename to the destination root." \
		"$(cat "$cmd_file")" "backup/dst/src"
	assertEquals "Trailing-slash replication should normalize destination snapshots into source-path form for recursive diffing." \
		"tank/src/child@snap2
tank/src@snap1" "$(cat "$norm_file")"
}

test_start_destination_snapshot_name_sorted_fifo_producer_streams_statuses_and_handles_registration_failures() {
	zxfer_get_temp_file >/dev/null || fail "temp output setup failed"
	destination_output=$g_zxfer_temp_file_result
	err_file="$TEST_TMPDIR/dest_fifo.err"
	status_file="$TEST_TMPDIR/dest_fifo.status"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >"$TEST_TMPDIR/dest_fifo.cmd"
		printf '%s\n' "backup/dst/src@snapA"
		printf '%s\n' "backup/dst/src/child@snapB"
	}
	# Run very-verbose so the lazily gated display render path is exercised.
	g_option_V_very_verbose=1
	raw_file="$TEST_TMPDIR/dest_fifo.raw"
	zxfer_start_destination_snapshot_name_sorted_fifo_producer \
		"$destination_output" "$err_file" "$status_file" "$raw_file" 2>/dev/null
	g_option_V_very_verbose=0
	producer_pid=$g_last_background_pid
	wait "$producer_pid"
	producer_status=$?
	zxfer_unregister_cleanup_pid "$producer_pid"

	registration_status=$(
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
			set +e
			zxfer_start_destination_snapshot_name_sorted_fifo_producer \
				"$test_output" \
				"$TEST_TMPDIR/dest_fifo_registration.err" \
				"$TEST_TMPDIR/dest_fifo_registration.status" \
				"$TEST_TMPDIR/dest_fifo_registration.raw"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Destination snapshot producer should complete successfully." 0 "$producer_status"
	assertContains "Destination FIFO producer should keep the identity-aware unsorted snapshot query." \
		"$(cat "$TEST_TMPDIR/dest_fifo.cmd")" "list -Hr -o name,guid -t snapshot backup/dst/src"
	assertEquals "Destination FIFO producer should normalize and byte-sort destination paths." \
		"tank/src/child@snapB
tank/src@snapA" "$(cat "$destination_output")"
	assertEquals "Destination FIFO producer should keep the raw listing for full discovery." \
		"backup/dst/src@snapA
backup/dst/src/child@snapB" "$(cat "$raw_file")"
	assertEquals "Destination FIFO producer should record the list, normalize and sort statuses on one line." \
		"0 0 0" "$(cat "$status_file")"
	assertEquals "Destination FIFO producer should fail closed when cleanup registration fails." \
		1 "$registration_status"
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

test_normalize_destination_snapshot_list_rewrites_trailing_slash_destination_to_source_paths() {
	input_file="$TEST_TMPDIR/dest_trailing_input.txt"
	output_file="$TEST_TMPDIR/dest_trailing_output.txt"
	g_initial_source_had_trailing_slash=1
	g_initial_source="tank/src"
	cat <<'EOF' >"$input_file"
backup/dst/child@snap2
backup/dst@snap1
EOF

	# Run very-verbose so the lazily gated display render path is exercised.
	g_option_V_very_verbose=1
	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file" 2>/dev/null

	assertEquals "Trailing-slash destinations should be sorted after source-prefix rewriting." \
		"tank/src/child@snap2
tank/src@snap1" "$(cat "$output_file")"
}

test_normalize_destination_snapshot_list_treats_temp_paths_as_literal() {
	marker="$TEST_TMPDIR/normalize_temp_path_marker"
	input_file="$TEST_TMPDIR/input.\$(touch normalize_temp_path_marker)"
	output_file="$TEST_TMPDIR/output.\$(touch normalize_temp_path_marker)"
	rm -f "$marker" "$input_file" "$output_file"
	printf '%s\n%s\n' "backup/dst@b" "backup/dst@a" >"$input_file"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"

	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"

	assertEquals "Normalization should still rewrite and sort snapshot names when temp paths contain metacharacters." \
		"tank/src@a
tank/src@b" "$(cat "$output_file")"
	assertFalse "Normalization should not execute command substitutions embedded in temp file paths." "[ -e '$marker' ]"
}

test_normalize_destination_snapshot_list_rewrites_only_leading_destination_prefix() {
	input_file="$TEST_TMPDIR/dest_repeated_prefix_input.txt"
	output_file="$TEST_TMPDIR/dest_repeated_prefix_output.txt"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
	{
		printf '%s\t%s\n' "backup/dst/backup/dst/child@snap2" "222"
		printf '%s\t%s\n' "backup/dst@snap1" "111"
	} >"$input_file"

	# Run very-verbose so the lazily gated display render path is exercised.
	g_option_V_very_verbose=1
	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file" 2>/dev/null

	expected=$(printf '%s\t%s\n%s\t%s' \
		"tank/src/backup/dst/child@snap2" "222" \
		"tank/src@snap1" "111")
	assertEquals "Destination normalization should rewrite only the leading destination root prefix." \
		"$expected" "$(cat "$output_file")"
}

test_normalize_destination_snapshot_list_does_not_rewrite_similar_dataset_prefixes() {
	input_file="$TEST_TMPDIR/dest_similar_prefix_input.txt"
	output_file="$TEST_TMPDIR/dest_similar_prefix_output.txt"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
	cat <<'EOF' >"$input_file"
backup/dst-old@snap1
backup/dst@snap1
EOF

	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"

	expected=$(printf '%s\n%s' "backup/dst-old@snap1" "tank/src@snap1")
	assertEquals "Destination normalization should not rewrite datasets that only share a text prefix." \
		"$expected" "$(cat "$output_file")"
}

test_normalize_destination_snapshot_list_preserves_sort_failures() {
	input_file="$TEST_TMPDIR/dest_normalize_sort_failure_input.txt"
	output_file="$TEST_TMPDIR/dest_normalize_sort_failure_output.txt"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
	printf '%s\n' "backup/dst@snap1" >"$input_file"

	status=$(
		(
			sort() {
				return 63
			}
			set +e
			zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Destination normalization should preserve sort failures." \
		63 "$status"
}

test_normalize_destination_snapshot_list_preserves_awk_failures() {
	input_file="$TEST_TMPDIR/dest_normalize_awk_failure_input.txt"
	output_file="$TEST_TMPDIR/dest_normalize_awk_failure_output.txt"
	fake_awk="$TEST_TMPDIR/dest_normalize_awk_failure.sh"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
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
			set +e
			zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertContains "Destination normalization should preserve awk diagnostics." \
		"$output" "normalize awk failed"
	assertContains "Destination normalization should preserve awk exit status." \
		"$output" "status=42"
}

test_normalize_destination_snapshot_stream_for_noop_proof_rewrites_and_filters() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_option_x_exclude_datasets='/replica$'

	output=$(
		printf '%s\n' \
			"backup/dst/src@snapA" \
			"backup/dst/src/replica@snapB" |
			zxfer_normalize_destination_snapshot_stream_for_noop_proof "backup/dst/src"
	)

	assertEquals "Streaming destination normalization should rewrite prefixes and filter excluded datasets." \
		"tank/src@snapA" "$output"
}

test_normalize_destination_snapshot_stream_for_noop_proof_handles_trailing_slash_streams() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_option_x_exclude_datasets=""

	pass_output=$(
		printf '%s\n' "backup/dst@snapA" |
			zxfer_normalize_destination_snapshot_stream_for_noop_proof "backup/dst"
	)
	g_option_x_exclude_datasets='/replica$'
	filter_output=$(
		printf '%s\n' "backup/dst@snapA" "backup/dst/replica@snapB" |
			zxfer_normalize_destination_snapshot_stream_for_noop_proof "backup/dst"
	)

	assertEquals "Trailing-slash stream normalization without excludes should rewrite into source-path form." \
		"tank/src@snapA" "$pass_output"
	assertEquals "Trailing-slash stream normalization should rewrite before filtering excluded datasets." \
		"tank/src@snapA" "$filter_output"
}
