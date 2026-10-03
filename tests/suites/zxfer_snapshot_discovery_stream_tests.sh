#!/bin/sh
# shellcheck shell=sh
# Snapshot delta, reversal and recursive work-list cases for
# src/zxfer_snapshot_discovery.sh that only a unit test can reach: helper
# edge cases, awk/sort/comm failures, argument-size limits and producer
# teardown. Run by tests/test_zxfer_snapshot_discovery.sh.
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

test_reverse_file_lines_reverses_small_and_large_inputs() {
	input_file="$TEST_TMPDIR/reverse_file_lines_input.txt"
	printf '%s\n' "tank/src@snap-a" "tank/src@snap-b" "tank/src@snap-million" \
		"tank/src@snap-c" >"$input_file"
	expected="tank/src@snap-c
tank/src@snap-million
tank/src@snap-b
tank/src@snap-a"

	# No limit uses the linear awk reverse; a limit below the line count, or
	# one that is not a number, takes the bounded numbered-sort fallback.
	for l_reverse_limit in "" 1 bogus; do
		output=$(
			zxfer_reverse_file_lines "$input_file" "$l_reverse_limit"
		)
		assertEquals "The lines should come out last first [limit:$l_reverse_limit]." \
			"$expected" "$output"
	done
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
			zxfer_get_temp_file() {
				return 45
			}
			zxfer_reverse_file_lines "$input_file" 1
			printf 'tempfile=%s\n' "$?"
		)
		(
			cat() {
				if [ "$1" = "-n" ]; then
					return 1
				fi
				command cat "$@"
			}
			zxfer_reverse_file_lines "$input_file" 1
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
					(exit 0) &
					destination_pid=$!
					fast_wait_status=0
					zxfer_wait_for_snapshot_discovery_producer "$source_pid" "" source || fast_wait_status=$?
					zxfer_wait_for_snapshot_discovery_producer "$destination_pid" "" destination
					printf 'status=%s records=<%s>\n' \
						"$fast_wait_status" "$g_zxfer_cleanup_pid_records"
				else
					g_source_snapshot_list_pid=$source_pid
					source_file="$g_zxfer_run_tmp_root/source"
					error_file="$g_zxfer_run_tmp_root/error"
					destination_file="$g_zxfer_run_tmp_root/destination"
					sorted_file="$g_zxfer_run_tmp_root/sorted"
					printf 'source listing failed\n' >"$error_file"
					zxfer_wait_for_full_source_snapshot_discovery "$source_file" "$error_file" ""
					full_wait_status=$?
					printf 'status=%s records=<%s> diagnostic=<%s>\n' \
						"$full_wait_status" "$g_zxfer_cleanup_pid_records" "$g_zxfer_snapshot_discovery_failure_result"
					exit "$full_wait_status"
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
					zxfer_wait_for_snapshot_discovery_producer "$VICTIM" "" source
					;;
				full)
					g_source_snapshot_list_pid=$VICTIM
					source_file="$g_zxfer_run_tmp_root/source"
					error_file="$g_zxfer_run_tmp_root/error"
					destination_file="$g_zxfer_run_tmp_root/destination"
					sorted_file="$g_zxfer_run_tmp_root/sorted"
					: >"$error_file"
					zxfer_wait_for_full_source_snapshot_discovery "$source_file" "$error_file" ""
					;;
				deststart)
					zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
						return 7
					}
					zxfer_start_fast_recursive_noop_destination_discovery \
						"" "" "" "" "$VICTIM"
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

# Each dataset list comes from its delta file through awk -F@ and sort -u, and
# then through the exclude filter; a failure at any step stops delta planning
# with that step's status and names the list. The fixture decides which list
# is the first to reach the failing step.
test_set_g_recursive_source_list_fails_closed_when_a_dataset_list_cannot_be_derived_or_filtered() {
	source_tmp="$TEST_TMPDIR/dataset_list_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/dataset_list_failure_dest.txt"
	fake_awk="$TEST_TMPDIR/dataset_list_failure_awk.sh"

	# step|status|source records|destination records|filtered list|message;
	# records and list lines are comma-separated.
	while IFS='|' read -r l_step l_status l_source l_dest l_filtered l_message; do
		printf '%s\n' "$l_source" | tr ',' '\n' | LC_ALL=C sort >"$source_tmp"
		printf '%s\n' "$l_dest" | tr ',' '\n' | LC_ALL=C sort >"$dest_tmp"
		[ "$l_step" != awk ] || create_selective_awk_failure_bin "$fake_awk" "$l_status"
		output=$(
			(
				FAIL_STATUS=$l_status
				FAIL_LIST=$(printf '%s' "$l_filtered" | tr ',' '\n')
				# if, not case: bash 3.2 misparses case patterns inside $(...).
				if [ "$l_step" = awk ]; then
					g_cmd_awk=$fake_awk
				elif [ "$l_step" = sort ]; then
					sort() {
						[ "$1" != "-u" ] || return "$FAIL_STATUS"
						command sort "$@"
					}
				else
					zxfer_filter_recursive_dataset_list_with_excludes() {
						[ "$1" != "$FAIL_LIST" ] || return "$FAIL_STATUS"
						g_zxfer_recursive_dataset_list_result=$1
					}
				fi
				zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
			) 2>&1
		)
		l_row_status=$?

		assertEquals "Delta planning should stop with the failing step's status [$l_step: $l_message]." \
			"$l_status" "$l_row_status"
		assertContains "Delta planning should name the list [$l_step]." \
			"$output" "$l_message"
		if [ "$l_step" = awk ]; then
			assertContains "Delta planning should keep awk's diagnostic." \
				"$output" "awk failed"
		fi
	done <<'EOF'
awk|8|tank/src@snap1,tank/src@snap2|tank/src@snap1||Failed to derive recursive source dataset transfer list.
awk|9|tank/src@snap1|tank/src/child@extra,tank/src@snap1||Failed to derive recursive destination dataset delete list.
awk|10|tank/src@snap1|tank/src@snap1||Failed to derive recursive source dataset inventory.
sort|7|tank/src@snap1|tank/src@snap1||Failed to derive recursive source dataset inventory.
filter|2|tank/src@snap1|tank/src/child@extra,tank/src@snap1|tank/src/child|Failed to filter recursive destination dataset delete list against exclude patterns.
filter|2|tank/src@snap1,tank/src/child@snap2|tank/src@snap1|tank/src,tank/src/child|Failed to filter recursive source dataset inventory against exclude patterns.
EOF
}
