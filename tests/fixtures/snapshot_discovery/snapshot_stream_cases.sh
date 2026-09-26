#!/bin/sh
# shellcheck shell=sh
# Snapshot stream diffing, normalization, reversal, and recursive-state cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_write_snapshot_delta_files_splits_both_diff_directions() {
	source_file="$TEST_TMPDIR/source_delta_split.txt"
	dest_file="$TEST_TMPDIR/dest_delta_split.txt"
	missing_file="$TEST_TMPDIR/source_delta_missing.txt"
	extra_file="$TEST_TMPDIR/destination_delta_extra.txt"
	scratch_file="$TEST_TMPDIR/delta_split.scratch"
	cat <<'EOF' >"$source_file"
tank/src@same	111
tank/src@source-only	222
EOF
	cat <<'EOF' >"$dest_file"
tank/src@dest-only	333
tank/src@same	999
EOF
	sort "$source_file" -o "$source_file"
	sort "$dest_file" -o "$dest_file"

	zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"

	assertEquals "Single-pass recursive diff should preserve source-only and GUID-divergent source records." \
		"tank/src@same	111
tank/src@source-only	222" "$(cat "$missing_file")"
	assertEquals "Single-pass recursive diff should strip only the comm prefix from destination-only records." \
		"tank/src@dest-only	333
tank/src@same	999" "$(cat "$extra_file")"
}

test_write_snapshot_delta_files_preserves_comm_and_splitter_failures() {
	set +e
	source_file="$TEST_TMPDIR/source_delta_splitter_fail.txt"
	dest_file="$TEST_TMPDIR/dest_delta_splitter_fail.txt"
	missing_file="$TEST_TMPDIR/source_delta_splitter_fail_missing.txt"
	extra_file="$TEST_TMPDIR/destination_delta_splitter_fail_extra.txt"
	scratch_file="$TEST_TMPDIR/delta_splitter_fail.scratch"
	fake_awk="$TEST_TMPDIR/delta_splitter_awk_fail.sh"
	printf '%s\n' "tank/src@source-only" >"$source_file"
	: >"$dest_file"
	cat >"$fake_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "awk failed" >&2
exit 15
EOF
	chmod +x "$fake_awk"

	output=$(
		(
			g_cmd_awk="$fake_awk"
			zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
			printf 'splitter=%s\n' "$?"
			comm() {
				return 16
			}
			zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
			printf 'comm=%s\n' "$?"
		) 2>&1
	)

	assertContains "Single-pass recursive diff should preserve splitter failures." \
		"$output" "splitter=15"
	assertContains "Splitter failures should preserve awk diagnostics." \
		"$output" "awk failed"
	assertContains "Single-pass recursive diff should preserve comm failures." \
		"$output" "comm=16"
}

test_reverse_file_lines_uses_linear_reverse_for_small_inputs() {
	input_file="$TEST_TMPDIR/reverse_file_lines_input.txt"
	cat <<'EOF' >"$input_file"
tank/src@snap-a
tank/src@snap-million
tank/src@snap-b
EOF
	output=$(zxfer_reverse_file_lines "$input_file")

	assertEquals "zxfer_reverse_file_lines should reverse small inputs without depending on numbered-sort formatting." \
		"tank/src@snap-b
tank/src@snap-million
tank/src@snap-a" "$output"
}

test_reverse_file_lines_falls_back_to_sort_for_large_inputs() {
	input_file="$TEST_TMPDIR/reverse_file_lines_fallback_input.txt"
	cat <<'EOF' >"$input_file"
tank/src@snap-a
tank/src@snap-b
tank/src@snap-million
tank/src@snap-c
EOF
	expected="tank/src@snap-c
tank/src@snap-million
tank/src@snap-b
tank/src@snap-a"
	output=$(
		(
			g_zxfer_linear_reverse_max_lines=1
			zxfer_reverse_file_lines "$input_file"
		)
	)
	bogus_output=$(
		(
			g_zxfer_linear_reverse_max_lines="bogus"
			zxfer_reverse_file_lines "$input_file"
		)
	)

	assertEquals "zxfer_reverse_file_lines should retain the sort-based fallback for larger inputs to avoid unbounded awk memory growth." \
		"$expected" "$output"
	assertEquals "A non-numeric reverse threshold should take the bounded sort fallback." \
		"$expected" "$bogus_output"
}

test_reverse_file_lines_preserves_awk_and_fallback_failures() {
	set +e
	input_file="$TEST_TMPDIR/reverse_failure_input.txt"
	failing_awk="$TEST_TMPDIR/reverse_awk_fails"
	printf '%s\n' "tank/src@snap-a" "tank/src@snap-b" >"$input_file"
	cat >"$failing_awk" <<'EOF'
#!/bin/sh
exit 37
EOF
	chmod +x "$failing_awk"

	output=$(
		(
			g_cmd_awk=$failing_awk
			zxfer_reverse_file_lines "$input_file"
			printf 'awk=%s\n' "$?"
		)
		(
			g_zxfer_linear_reverse_max_lines=1
			zxfer_get_temp_file() {
				return 45
			}
			zxfer_reverse_file_lines "$input_file"
			printf 'tempfile=%s\n' "$?"
		)
		(
			g_zxfer_linear_reverse_max_lines=1
			cat() {
				if [ "$1" = "-n" ]; then
					return 1
				fi
				command cat "$@"
			}
			zxfer_reverse_file_lines "$input_file"
			printf 'numbering=%s\n' "$?"
		)
	)

	assertContains "Reverse awk failures should return the exact underlying status." \
		"$output" "awk=37"
	assertContains "The sort fallback should preserve temp-file allocation failures." \
		"$output" "tempfile=45"
	assertContains "The sort fallback should fail cleanly when numbering the file fails." \
		"$output" "numbering=1"
}

test_set_g_recursive_source_list_applies_exclude_filter_and_verbose_output() {
	source_tmp="$TEST_TMPDIR/source_snapshots.txt"
	dest_tmp="$TEST_TMPDIR/dest_snapshots.txt"
	cat <<'EOF' >"$source_tmp"
tank/src@a
tank/src/child@a
tank/src@b
tank/src/child@b
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@a
tank/src/child@a
tank/src/extra@z
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_x_exclude_datasets="^tank/src/child$"
	g_option_V_very_verbose=1
	verbose_file="$TEST_TMPDIR/set_recursive_source.verbose"
	zxfer_reset_runtime_artifact_state

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$verbose_file" 2>&1
	output=$(cat "$verbose_file")

	assertEquals "Excluded datasets should be removed from the transfer list." "tank/src" "$g_recursive_source_list"
	assertEquals "Excluded datasets should also be removed from the dataset cache." "tank/src" "$g_recursive_source_dataset_list"
	assertEquals "Successful recursive source-list discovery should not leave stale runtime-artifact cleanup registrations behind." \
		"" "${g_zxfer_runtime_artifact_cleanup_paths:-}"
	assertContains "Very-verbose mode should print the missing-source snapshot heading." \
		"$output" "Snapshots present in source but missing in destination"
	assertContains "Very-verbose mode should print the extra-destination snapshot heading." \
		"$output" "Extra Destination snapshots not in source"
}

test_set_g_recursive_source_list_accepts_leading_dash_exclude_patterns() {
	source_tmp="$TEST_TMPDIR/source_dash_pattern_snapshots.txt"
	dest_tmp="$TEST_TMPDIR/dest_dash_pattern_snapshots.txt"
	output_file="$TEST_TMPDIR/dash_pattern_output.txt"
	cat <<'EOF' >"$source_tmp"
tank/src@a
tank/src/child-exclude@a
tank/src@b
tank/src/child-exclude@b
EOF
	: >"$dest_tmp"
	sort "$source_tmp" -o "$source_tmp"
	g_option_x_exclude_datasets="-exclude$"

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$output_file" 2>&1
	output=$(cat "$output_file")

	assertEquals "Leading-dash regex patterns should still exclude matching datasets." \
		"tank/src" "$g_recursive_source_list"
	assertEquals "Leading-dash regex patterns should also filter the dataset cache." \
		"tank/src" "$g_recursive_source_dataset_list"
	assertNotContains "Leading-dash patterns should be treated as regexes, not grep options." \
		"$output" "illegal option"
	assertNotContains "Leading-dash patterns should not trigger grep usage errors on GNU systems either." \
		"$output" "invalid option"
}

test_set_g_recursive_source_list_filters_excluded_snapshots_before_noop_compare() {
	source_tmp="$TEST_TMPDIR/source_excluded_noop_snapshots.txt"
	dest_tmp="$TEST_TMPDIR/dest_excluded_noop_snapshots.txt"
	output_file="$TEST_TMPDIR/excluded_noop_output.txt"
	cat <<'EOF' >"$source_tmp"
tank/src/replica@source-only
tank/src@a
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src/replica@destination-only
tank/src@a
EOF
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_R_recursive="tank/src"
	g_option_x_exclude_datasets='/replica$'

	(
		zxfer_write_snapshot_delta_files() {
			printf '%s\n' "unexpected-diff"
			return 99
		}
		zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		printf 'source=%s\n' "$g_recursive_source_list"
		printf 'dest=%s\n' "$g_recursive_destination_extra_dataset_list"
		printf 'datasets=%s\n' "$g_recursive_source_dataset_list"
	) >"$output_file"
	status=$?
	output=$(cat "$output_file")

	assertEquals "Excluded source and destination snapshot rows should be removed before the exact no-op comparison." \
		0 "$status"
	assertNotContains "Excluded-only differences should not fall through to the full comm/splitter path." \
		"$output" "unexpected-diff"
	assertContains "Excluded-only differences should leave no source datasets queued for transfer." \
		"$output" "source="
	assertContains "Excluded-only differences should leave no destination datasets queued for deletion." \
		"$output" "dest="
	assertContains "Recursive no-op runs without property work should still avoid whole-tree source dataset inventory." \
		"$output" "datasets="
}

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

test_set_g_recursive_source_list_treats_trailing_slash_rewritten_destination_snapshots_as_common() {
	source_tmp="$TEST_TMPDIR/source_trailing_common.txt"
	dest_full_tmp="$TEST_TMPDIR/dest_trailing_common_full.txt"
	dest_norm_tmp="$TEST_TMPDIR/dest_trailing_common_norm.txt"
	g_initial_source_had_trailing_slash=1
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_option_x_exclude_datasets=""
	cat <<'EOF' >"$source_tmp"
tank/src@snap1	111
tank/src/child@snap2	222
EOF
	cat <<'EOF' >"$dest_full_tmp"
backup/dst@snap1	111
backup/dst/child@snap2	222
EOF

	zxfer_normalize_destination_snapshot_list "backup/dst" "$dest_full_tmp" "$dest_norm_tmp"
	LC_ALL=C sort "$source_tmp" -o "$source_tmp"
	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_norm_tmp" "$source_tmp"

	assertEquals "Trailing-slash destination snapshots with matching GUIDs should not be queued as missing source work." \
		"" "$g_recursive_source_list"
	assertEquals "Trailing-slash destination snapshots with matching GUIDs should not be queued as destination-only deletes." \
		"" "$g_recursive_destination_extra_dataset_list"
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

test_failed_source_discovery_wait_stops_descendants_after_group_leader_exit() {
	zxfer_init_background_shell_spawn_mode
	if [ "$g_zxfer_background_shell_spawn_mode" = wrapper ]; then
		startSkipping
		assertTrue "This regression requires verified process-group isolation." true
		endSkipping
		return
	fi
	for wait_mode in fast full; do
		child_file="$TEST_TMPDIR/failed_source_$wait_mode.child"
		group_file="$TEST_TMPDIR/failed_source_$wait_mode.group"
		release_file="$TEST_TMPDIR/failed_source_$wait_mode.release"
		rm -f "$release_file"
		output=$(
			(
				# The failed leader leaves one bounded, quiet descendant in its
				# group. Cleanup must use the group even after wait reaps it.
				# shellcheck disable=SC2016 # This command runs in the fixture child.
				zxfer_spawn_background_shell \
					'sleep 30 </dev/null >/dev/null 2>&1 & printf "%s\n" "$!" >"$1"; tries=0; while [ ! -f "$2" ] && [ "$tries" -lt 30 ]; do sleep 0.1 2>/dev/null || sleep 1; tries=$((tries + 1)); done; exit 37' \
					/dev/null /dev/null "$child_file" "$release_file"
				source_pid=$g_last_background_pid
				printf '%s\n' "$source_pid" >"$group_file"
				zxfer_register_cleanup_pid "$source_pid" "failed source fixture" "$g_zxfer_background_shell_scope"
				: >"$release_file"
				zxfer_throw_error() {
					printf 'status=%s records=<%s>\n' "${2:-1}" "$g_zxfer_cleanup_pid_records"
					exit "${2:-1}"
				}
				if [ "$wait_mode" = fast ]; then
					g_zxfer_snapshot_discovery_fast_noop_source_pid=$source_pid
					(exit 0) &
					g_zxfer_snapshot_discovery_fast_noop_destination_pid=$!
					zxfer_wait_for_fast_recursive_noop_discovery
					printf 'status=%s records=<%s>\n' \
						"$g_zxfer_snapshot_discovery_fast_noop_source_wait_status" "$g_zxfer_cleanup_pid_records"
				else
					g_source_snapshot_list_pid=$source_pid
					g_zxfer_full_source_snapshot_file="$g_zxfer_run_tmp_root/source"
					g_zxfer_full_source_snapshot_error_file="$g_zxfer_run_tmp_root/error"
					g_zxfer_full_destination_snapshot_file="$g_zxfer_run_tmp_root/destination"
					g_zxfer_full_destination_snapshot_sorted_file="$g_zxfer_run_tmp_root/sorted"
					printf 'source listing failed\n' >"$g_zxfer_full_source_snapshot_error_file"
					zxfer_wait_for_full_source_snapshot_discovery
				fi
			) 2>&1
		)
		wait_result=$?
		if [ "$wait_mode" = full ]; then
			assertEquals "Full discovery keeps the exact failed producer status." 37 "$wait_result"
		else
			assertEquals "The fast wait leaves status validation to its caller." 0 "$wait_result"
		fi
		assertContains "The completed cleanup releases its registered scope: $wait_mode" \
			"$output" "status=37 records=<>"
		child_pid=$(cat "$child_file")
		tries=0
		while kill -s 0 "$child_pid" 2>/dev/null && [ "$tries" -lt 20 ]; do
			sleep 0.1 2>/dev/null || sleep 1
			tries=$((tries + 1))
		done
		if kill -s 0 "$child_pid" 2>/dev/null; then
			# Always stop the fixture on a regression; its descriptors do not
			# keep the test output pipe open even if the assertion fails.
			command kill -KILL "-$(cat "$group_file")" 2>/dev/null || :
			fail "$wait_mode left a descendant alive after its group leader failed."
		fi
	done
}

# A reaped producer's number may already belong to an unrelated process. Here
# a live non-leader that ignores TERM stands in for that process: after the
# producer's wait, none of the three teardown sites may signal its bare PID
# (or snapshot the process table for it), whatever the recorded scope.
test_failed_source_discovery_never_signals_a_reaped_producer_pid() {
	(
		trap '' TERM
		exec sleep 30
	) </dev/null >/dev/null 2>&1 &
	victim_pid=$!
	if command kill -0 "-$victim_pid" 2>/dev/null; then
		command kill -s KILL "$victim_pid" 2>/dev/null || :
		wait "$victim_pid" 2>/dev/null || :
		startSkipping
		assertTrue "The stand-in must not lead its own process group." true
		endSkipping
		return
	fi
	for site in fast full deststart; do
		for scope in pgid wrapper pid; do
			log="$TEST_TMPDIR/reaped_producer_$site.$scope.log"
			: >"$log"
			(
				LOG=$log
				VICTIM=$victim_pid
				g_zxfer_cleanup_pid_abort_grace_seconds=0
				zxfer_register_cleanup_pid "$VICTIM" "recycled producer fixture" "$scope"
				reaped=0
				wait() {
					reaped=1
					return 37
				}
				# Liveness and group probes reach the real kill; any other
				# signal is only logged, and flagged once the producer is reaped.
				kill() {
					case "$*" in
					"-s 0 $VICTIM" | "-0 -$VICTIM" | -[A-Z]*" -$VICTIM")
						command kill "$@" 2>/dev/null
						return
						;;
					esac
					[ "$reaped" -eq 0 ] || printf 'VICTIM kill %s\n' "$*" >>"$LOG"
					return 0
				}
				ps() {
					[ "$reaped" -eq 0 ] || printf 'VICTIM ps %s\n' "$*" >>"$LOG"
					return 1
				}
				zxfer_throw_error() {
					printf 'throw %s\n' "$1" >>"$LOG"
					exit "${2:-1}"
				}
				case $site in
				fast)
					g_zxfer_snapshot_discovery_fast_noop_source_pid=$VICTIM
					g_zxfer_snapshot_discovery_fast_noop_destination_pid=""
					zxfer_wait_for_fast_recursive_noop_discovery
					;;
				full)
					g_source_snapshot_list_pid=$VICTIM
					g_zxfer_full_source_snapshot_file="$g_zxfer_run_tmp_root/source"
					g_zxfer_full_source_snapshot_error_file="$g_zxfer_run_tmp_root/error"
					g_zxfer_full_destination_snapshot_file="$g_zxfer_run_tmp_root/destination"
					g_zxfer_full_destination_snapshot_sorted_file="$g_zxfer_run_tmp_root/sorted"
					: >"$g_zxfer_full_source_snapshot_error_file"
					zxfer_wait_for_full_source_snapshot_discovery
					;;
				deststart)
					zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
						return 7
					}
					g_zxfer_snapshot_discovery_fast_noop_source_pid=$VICTIM
					zxfer_start_fast_recursive_noop_destination_discovery
					;;
				esac
				printf 'returned %s\n' "$?" >>"$LOG"
			) >/dev/null 2>&1
			assertNotContains "$site/$scope must not signal a reaped producer PID." \
				"$(cat "$log")" "VICTIM"
		done
	done
	assertTrue "The unrelated stand-in must survive every teardown site." \
		"command kill -s 0 $victim_pid"
	command kill -s KILL "$victim_pid" 2>/dev/null || :
	wait "$victim_pid" 2>/dev/null || :
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

test_filter_snapshot_file_with_excludes_filters_by_snapshot_dataset() {
	input_file="$TEST_TMPDIR/snapshot_exclude_filter_input.txt"
	output_file="$TEST_TMPDIR/snapshot_exclude_filter_output.txt"
	cat <<'EOF' >"$input_file"
tank/src/replica@snapA
tank/src@snap-replica
tank/src@snapA	guidA
EOF
	g_option_x_exclude_datasets='/replica$'

	zxfer_filter_snapshot_file_with_excludes "$input_file" "$output_file"

	assertEquals "Snapshot-list exclude filtering should match dataset names, not snapshot names or GUID fields." \
		"tank/src@snap-replica
tank/src@snapA	guidA" "$(cat "$output_file")"
}

test_filter_snapshot_file_with_excludes_copies_input_without_patterns() {
	input_file="$TEST_TMPDIR/snapshot_exclude_passthrough_input.txt"
	output_file="$TEST_TMPDIR/snapshot_exclude_passthrough_output.txt"
	cat <<'EOF' >"$input_file"
tank/src/app@snap2
tank/src/app@snap1
EOF
	g_option_x_exclude_datasets=""

	zxfer_filter_snapshot_file_with_excludes "$input_file" "$output_file"

	assertEquals "Snapshot-list exclude filtering should copy records unchanged when no exclude pattern is configured." \
		"tank/src/app@snap2
tank/src/app@snap1" "$(cat "$output_file")"
}

test_snapshot_discovery_need_helpers_cover_recursive_shortcuts() {
	output=$(
		(
			set +e
			g_option_R_recursive="-R"
			g_option_P_transfer_property=1
			zxfer_snapshot_discovery_needs_source_dataset_inventory
			printf 'source_props=%s\n' "$?"
			g_option_P_transfer_property=0
			g_option_U_skip_unsupported_properties=1
			g_recursive_source_list=""
			zxfer_snapshot_discovery_needs_source_dataset_inventory
			printf 'source_unsupported_noop=%s\n' "$?"
			g_recursive_source_list="tank/src"
			zxfer_snapshot_discovery_needs_source_dataset_inventory
			printf 'source_unsupported_work=%s\n' "$?"
			g_option_U_skip_unsupported_properties=0

			g_recursive_source_list="tank/src"
			zxfer_snapshot_discovery_needs_record_caches
			printf 'record_source=%s\n' "$?"
			g_recursive_source_list=""
			g_option_d_delete_destination_snapshots=1
			g_recursive_destination_extra_dataset_list="tank/src"
			zxfer_snapshot_discovery_needs_record_caches
			printf 'record_delete=%s\n' "$?"
			g_recursive_destination_extra_dataset_list=""
			g_option_d_delete_destination_snapshots=0
			g_option_o_override_property="compression=lz4"
			zxfer_snapshot_discovery_needs_record_caches
			printf 'record_props=%s\n' "$?"

			g_recursive_source_list="tank/src"
			zxfer_snapshot_discovery_needs_destination_dataset_inventory
			printf 'dest_source=%s\n' "$?"
			g_recursive_source_list=""
			g_option_o_override_property=""
			g_option_d_delete_destination_snapshots=1
			g_recursive_destination_extra_dataset_list="tank/src"
			zxfer_snapshot_discovery_needs_destination_dataset_inventory
			printf 'dest_delete=%s\n' "$?"
			g_recursive_destination_extra_dataset_list=""
			g_option_d_delete_destination_snapshots=0
			g_option_P_transfer_property=1
			zxfer_snapshot_discovery_needs_destination_dataset_inventory
			printf 'dest_props=%s\n' "$?"
		)
	)

	assertContains "Property transfer should require source dataset inventory." \
		"$output" "source_props=0"
	assertContains "Unsupported-property scanning should not require source dataset inventory after recursive no-op discovery." \
		"$output" "source_unsupported_noop=1"
	assertContains "Unsupported-property scanning should require source dataset inventory when source work may need create filtering." \
		"$output" "source_unsupported_work=0"
	assertContains "Pending transfers should retain snapshot record caches." \
		"$output" "record_source=0"
	assertContains "Pending delete inspection should retain snapshot record caches." \
		"$output" "record_delete=0"
	assertContains "Property work should retain snapshot record caches." \
		"$output" "record_props=0"
	assertContains "Pending transfers should require destination dataset inventory." \
		"$output" "dest_source=0"
	assertContains "Pending destination deletes should require destination dataset inventory." \
		"$output" "dest_delete=0"
	assertContains "Property work should require destination dataset inventory." \
		"$output" "dest_props=0"
}

test_set_g_recursive_source_list_logs_when_no_new_snapshots_exist() {
	source_tmp="$TEST_TMPDIR/source_same_snapshots.txt"
	dest_tmp="$TEST_TMPDIR/dest_same_snapshots.txt"
	output_file="$TEST_TMPDIR/source_same_output.txt"
	cat <<'EOF' >"$source_tmp"
tank/src@a
tank/src@b
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@a
tank/src@b
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_v_verbose=1
	g_option_x_exclude_datasets=""
	output=$(
		(
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$output_file"
			printf 'source=%s\n' "$g_recursive_source_list"
			printf 'datasets=%s\n' "$g_recursive_source_dataset_list"
			printf 'dest=%s\n' "$g_recursive_destination_extra_dataset_list"
		)
	)

	assertContains "Matching source and destination snapshots should leave no datasets queued for transfer after guid validation." \
		"$output" "source="
	assertContains "Dataset caches should still reflect the source datasets even when nothing needs transfer." \
		"$output" "datasets=tank/src"
	assertContains "Matching source and destination snapshots should leave no datasets queued for delete-only inspection after guid validation." \
		"$output" "dest="
	assertNotContains "Recursive delta planning should not leak current-shell temp file paths into stdout when no datasets differ." \
		"$output" "$TEST_TMPDIR/zxfer."
	assertContains "Verbose mode should explain when no new snapshots need transfer." \
		"$(cat "$output_file")" "No new snapshots to transfer."
}

test_set_g_recursive_source_list_uses_existing_presorted_source_sidecar() {
	source_tmp="$TEST_TMPDIR/source_presorted_unused_raw.txt"
	presorted_tmp="$TEST_TMPDIR/source_presorted_existing.txt"
	dest_tmp="$TEST_TMPDIR/dest_presorted_existing.txt"
	cat <<'EOF' >"$presorted_tmp"
tank/src@a
tank/src@b
EOF
	cp "$presorted_tmp" "$dest_tmp"
	rm -f "$source_tmp"

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" "$presorted_tmp"

	assertEquals "Recursive planning should use an existing sorted source sidecar without reading the raw source list." \
		"" "$g_recursive_source_list"
}

test_set_g_recursive_source_list_reports_missing_presorted_source_sidecar() {
	source_tmp="$TEST_TMPDIR/source_presorted_missing_raw.txt"
	dest_tmp="$TEST_TMPDIR/dest_presorted_missing.txt"
	presorted_tmp="$TEST_TMPDIR/source_presorted_missing.txt"
	: >"$source_tmp"
	: >"$dest_tmp"
	rm -f "$presorted_tmp"

	zxfer_test_capture_subshell "
		zxfer_set_g_recursive_source_list '$source_tmp' '$dest_tmp' '$presorted_tmp'
	"

	assertEquals "Recursive planning should fail closed when the advertised sorted source sidecar is missing." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing sorted source sidecar failures should preserve recursive delta context." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to locate staged sorted source snapshots for recursive delta planning."
}

test_set_g_recursive_source_list_tracks_destination_only_snapshot_datasets() {
	source_tmp="$TEST_TMPDIR/source_delete_delta.txt"
	dest_tmp="$TEST_TMPDIR/dest_delete_delta.txt"
	cat <<'EOF' >"$source_tmp"
tank/src@a
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@a
tank/src/child@extra
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_x_exclude_datasets=""

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"

	assertEquals "Destination-only snapshot datasets should be tracked separately for delete-only inspection." \
		"tank/src/child" "$g_recursive_destination_extra_dataset_list"
}

test_set_g_recursive_source_list_queues_name_identical_guid_divergence_from_initial_records() {
	source_tmp="$TEST_TMPDIR/source_guid_divergence.txt"
	dest_tmp="$TEST_TMPDIR/dest_guid_divergence.txt"
	output_file="$TEST_TMPDIR/source_guid_divergence.out"
	cat <<'EOF' >"$source_tmp"
tank/src@same	111
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@same	999
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_x_exclude_datasets=""

	(
		zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		printf 'source=%s\n' "$g_recursive_source_list"
		printf 'dest=%s\n' "$g_recursive_destination_extra_dataset_list"
	) >"$output_file"

	assertContains "Initial identity-aware discovery should queue same-name source snapshots with different GUIDs for transfer planning." \
		"$(cat "$output_file")" "source=tank/src"
	assertContains "Initial identity-aware discovery should queue same-name destination snapshots with different GUIDs for delete/common-snapshot inspection." \
		"$(cat "$output_file")" "dest=tank/src"
}

test_set_g_recursive_source_list_verbose_summarizes_dirty_recursive_delta() {
	source_tmp="$TEST_TMPDIR/source_verbose_delta.txt"
	dest_tmp="$TEST_TMPDIR/dest_verbose_delta.txt"
	cat <<'EOF' >"$source_tmp"
tank/src/app@snapA	111
tank/src/db@snapB	222
tank/src@snap0	000
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src/old@snapZ	999
tank/src@snap0	000
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_v_verbose=1
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""

	output=$(zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" "$source_tmp")

	assertContains "Verbose recursive delta output should show compact source/destination dirty counts." \
		"$output" "Recursive snapshot delta summary: source_missing_snapshots=2 destination_extra_snapshots=1 source_datasets=2 destination_extra_datasets=1"
	assertContains "Verbose recursive delta output should name source datasets queued for transfer." \
		"$output" "  tank/src/app"
	assertContains "Verbose recursive delta output should name every source-delta dataset." \
		"$output" "  tank/src/db"
	assertContains "Verbose recursive delta output should name destination-only datasets queued for delete inspection." \
		"$output" "  tank/src/old"
}

test_report_recursive_snapshot_delta_counts_lists_larger_than_argument_limits() {
	missing_file="$TEST_TMPDIR/large_delta_missing.txt"
	extra_file="$TEST_TMPDIR/large_delta_extra.txt"
	stderr_file="$TEST_TMPDIR/large_delta.err"
	printf '%s\n' "tank/src/a@s1" "tank/src/a@s2" "tank/src/b@s1" >"$missing_file"
	printf '%s\n' "tank/src/old@s0" >"$extra_file"
	# About 1.3 MiB of names: past Linux's 128 KiB limit for one argv or
	# environment string and macOS's 1 MiB limit for all of them.
	g_recursive_source_list=$(awk 'BEGIN {
		for (i = 1; i <= 20000; i++)
			printf "tank/src/dataset-name-padded-past-the-argument-size-limits-%05d\n", i
	}')
	g_recursive_destination_extra_dataset_list=$(printf '%s\n%s' "tank/src/old" "tank/src/older")
	g_option_v_verbose=1
	g_option_V_very_verbose=1

	set +e
	output=$(zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file" 2>"$stderr_file")
	status=$?

	assertEquals "Reporting a large recursive delta should succeed." 0 "$status"
	assertContains "Verbose output should keep the delta summary for a source list larger than the argument limits." \
		"$output" "Recursive snapshot delta summary: source_missing_snapshots=3 destination_extra_snapshots=1 source_datasets=20000 destination_extra_datasets=2"
	assertContains "Very verbose output should count every source dataset." \
		"$output" "Source dataset count: 20000"
	assertEquals "Reporting a large recursive delta should not write to stderr." \
		"" "$(cat "$stderr_file")"
}

test_report_recursive_snapshot_delta_fails_closed_when_counting_fails() {
	missing_file="$TEST_TMPDIR/delta_count_failure_missing.txt"
	extra_file="$TEST_TMPDIR/delta_count_failure_extra.txt"
	fake_awk="$TEST_TMPDIR/delta_count_failure_awk.sh"
	printf '%s\n' "tank/src@s1" >"$missing_file"
	: >"$extra_file"
	cat >"$fake_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "awk failed" >&2
exit 6
EOF
	chmod +x "$fake_awk"

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"
			g_option_v_verbose=1
			g_recursive_source_list="tank/src"
			g_recursive_destination_extra_dataset_list=""

			zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
		) 2>&1
	)
	status=$?

	assertEquals "A failed delta count should fail closed with awk's status." 6 "$status"
	assertContains "A failed delta count should report a specific error." \
		"$output" "Failed to count the recursive snapshot delta."
	assertNotContains "A failed delta count should not print a zeroed summary." \
		"$output" "Recursive snapshot delta summary"
}

test_set_g_recursive_source_list_treats_tmpdir_derived_paths_as_literal() {
	old_tmpdir=${TMPDIR:-}
	marker="$TEST_TMPDIR/source_sort_marker"
	tmpdir_with_payload="$TEST_TMPDIR/tmpdir.\$(touch source_sort_marker)"
	source_tmp="$TEST_TMPDIR/source_sort_input.txt"
	dest_tmp="$TEST_TMPDIR/dest_sort_input.txt"
	rm -f "$marker"
	rm -rf "$tmpdir_with_payload"
	mkdir -p "$tmpdir_with_payload"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	TMPDIR=$tmpdir_with_payload

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"

	TMPDIR=$old_tmpdir

	assertEquals "Sorting source snapshots should still identify the missing dataset when TMPDIR contains metacharacters." \
		"tank/src" "$g_recursive_source_list"
	assertFalse "Sorting source snapshots should not execute command substitutions embedded in TMPDIR-derived temp paths." \
		"[ -e '$marker' ]"
}

test_set_g_recursive_source_list_reports_recursive_snapshot_diff_failures() {
	source_tmp="$TEST_TMPDIR/recursive_diff_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_diff_failure_dest.txt"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			comm() {
				if [ "$1" = "-3" ]; then
					return 6
				fi
				command comm "$@"
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when the source-minus-destination diff fails." \
		6 "$status"
	assertContains "Recursive delta planning should preserve a specific transfer-planning diff error." \
		"$output" "Failed to diff source and destination snapshots for recursive delta planning."
}

test_set_g_recursive_source_list_reports_recursive_source_dataset_transfer_awk_failures() {
	source_tmp="$TEST_TMPDIR/recursive_source_transfer_awk_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_source_transfer_awk_failure_dest.txt"
	fake_awk="$TEST_TMPDIR/recursive_source_transfer_awk_fail.sh"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 8

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when deriving the source transfer dataset list fails before sort notices." \
		8 "$status"
	assertContains "Recursive delta planning should preserve the upstream awk failure from the source transfer dataset derivation." \
		"$output" "awk failed"
	assertContains "Recursive delta planning should report a specific source transfer dataset derivation error." \
		"$output" "Failed to derive recursive source dataset transfer list."
}

test_set_g_recursive_source_list_reports_recursive_destination_dataset_delete_awk_failures() {
	source_tmp="$TEST_TMPDIR/recursive_destination_delete_awk_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_destination_delete_awk_failure_dest.txt"
	fake_awk="$TEST_TMPDIR/recursive_destination_delete_awk_fail.sh"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n%s\n' "tank/src/child@extra" "tank/src@snap1" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 9

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delete-only planning should fail closed when deriving destination delete datasets fails before sort notices." \
		9 "$status"
	assertContains "Recursive delete-only planning should preserve the upstream awk failure from the destination delete dataset derivation." \
		"$output" "awk failed"
	assertContains "Recursive delete-only planning should report a specific destination delete dataset derivation error." \
		"$output" "Failed to derive recursive destination dataset delete list."
}

test_set_g_recursive_source_list_reports_recursive_source_dataset_inventory_failures() {
	source_tmp="$TEST_TMPDIR/recursive_source_inventory_error_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_source_inventory_error_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			sort() {
				if [ "$1" = "-u" ]; then
					return 7
				fi
				command sort "$@"
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when source dataset inventory derivation fails." \
		7 "$status"
	assertContains "Recursive delta planning should preserve a specific source dataset inventory error." \
		"$output" "Failed to derive recursive source dataset inventory."
}

test_set_g_recursive_source_list_reports_recursive_source_dataset_inventory_awk_failures() {
	source_tmp="$TEST_TMPDIR/recursive_source_inventory_awk_error_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_source_inventory_awk_error_dest.txt"
	fake_awk="$TEST_TMPDIR/recursive_source_inventory_awk_fail.sh"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 10

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when source dataset inventory derivation fails before sort notices." \
		10 "$status"
	assertContains "Recursive delta planning should preserve the upstream awk failure from source dataset inventory derivation." \
		"$output" "awk failed"
	assertContains "Recursive delta planning should report a specific source dataset inventory error." \
		"$output" "Failed to derive recursive source dataset inventory."
}

test_set_g_recursive_source_list_reports_invalid_exclude_pattern_failures() {
	source_tmp="$TEST_TMPDIR/recursive_exclude_pattern_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_exclude_pattern_failure_dest.txt"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src/child@snap1" >"$source_tmp"
	: >"$dest_tmp"
	g_option_x_exclude_datasets='['

	set +e
	output=$(
		(
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when exclude filtering uses an invalid pattern." \
		2 "$status"
	assertContains "Recursive delta planning should report the specific pre-diff snapshot exclude-filter context." \
		"$output" "Failed to filter source snapshots against exclude patterns for recursive delta planning."
}

test_set_g_recursive_source_list_fuzzes_tmpdir_derived_paths_with_odd_characters() {
	old_tmpdir=${TMPDIR:-}
	marker="$TEST_TMPDIR/source_sort_marker_fuzz"
	case_file="$TEST_TMPDIR/tmpdir_fuzz_cases.txt"
	source_tmp="$TEST_TMPDIR/source_sort_fuzz_input.txt"
	dest_tmp="$TEST_TMPDIR/dest_sort_fuzz_input.txt"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	cat >"$case_file" <<EOF
tmpdir,comma
tmpdir=equals
tmpdir:semicolon;literal
tmpdir.\$(touch source_sort_marker_fuzz)
EOF

	case_index=0
	rm -f "$marker"
	while IFS= read -r tmpdir_tail || [ -n "$tmpdir_tail" ]; do
		[ -n "$tmpdir_tail" ] || continue
		case_index=$((case_index + 1))
		tmpdir_case="$TEST_TMPDIR/$tmpdir_tail"
		rm -rf "$tmpdir_case"
		mkdir -p "$tmpdir_case"
		TMPDIR=$tmpdir_case
		g_recursive_source_list=""
		g_recursive_source_dataset_list=""

		zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"

		assertEquals "TMPDIR fuzz case $case_index should still identify the missing dataset." \
			"tank/src" "$g_recursive_source_list"
	done <"$case_file"

	if [ -n "${old_tmpdir+set}" ]; then
		TMPDIR=$old_tmpdir
	else
		unset TMPDIR
	fi

	assertFalse "TMPDIR fuzz cases should not execute command substitutions embedded in derived temp paths." \
		"[ -e '$marker' ]"
}
