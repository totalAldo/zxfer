#!/bin/sh
#
# shunit2 tests for src/zxfer_snapshot_discovery.sh: the fast no-op proof,
# full discovery, deltas and record files, source and destination listing
# commands, parallel discovery, staged capture files and normalization.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/snapshot_discovery_fixtures.sh
. "$TESTS_DIR/helpers/snapshot_discovery_fixtures.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_snapshot_discovery"
	zxfer_test_exec_fixture_one_time_setup
	zxfer_test_snapshot_discovery_fixture_write_tools
}

oneTimeTearDown() {
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_snapshot_discovery_fixture_setup
}

tearDown() {
	relax_test_tmpdir_permissions
}

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
	assertEquals "Single-pass recursive diff should publish split counts and file-pair provenance." \
		"2|2|$missing_file|$extra_file" \
		"$g_zxfer_delta_missing_count|$g_zxfer_delta_extra_count|$g_zxfer_delta_count_missing_file|$g_zxfer_delta_count_extra_file"

	# Both outputs are truncated even when one side has zero records.
	printf '%s\n' "tank/src@only-source	111" >"$source_file"
	: >"$dest_file"
	zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
	assertEquals "Missing-only diff should truncate stale extra records." "" "$(cat "$extra_file")"
	assertEquals "Missing-only diff should publish 1/0 counts." \
		"1|0" "$g_zxfer_delta_missing_count|$g_zxfer_delta_extra_count"
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
	printf '%s\n' "tank/src@snap-a" "" "tank/src@snap:b" "tank/src@snap-million" \
		"tank/src@snap-c" >"$input_file"
	expected="tank/src@snap-c
tank/src@snap-million
tank/src@snap:b

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
			grep() { return 44; }
			zxfer_reverse_file_lines "$input_file" 1
			printf 'numbering=%s\n' "$?"
		)
		(
			sort() { return 42; }
			zxfer_reverse_file_lines "$input_file" 1
			printf 'sort=%s\n' "$?"
		)
		(
			cut() { return 43; }
			zxfer_reverse_file_lines "$input_file" 1
			printf 'cut=%s\n' "$?"
		)
	)

	assertContains "Reverse awk failures should return the exact underlying status." \
		"$output" "awk=37"
	assertContains "The sort fallback should preserve temp-file allocation failures." \
		"$output" "tempfile=45"
	assertContains "The sort fallback should fail cleanly when numbering the file fails." \
		"$output" "numbering=44"
	assertContains "The fallback should propagate a failed sort." "$output" "sort=42"
	assertContains "The fallback should propagate a failed cut." "$output" "cut=43"
	assertNotContains "A failed sort must not publish snapshot records." "$output" "tank/src@"
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
					zxfer_write_source_snapshot_list_to_file() {
						g_source_snapshot_list_pid=$source_pid
						printf 'source listing failed\n' >"$2"
					}
					zxfer_write_destination_snapshot_list_to_files() { :; }
					zxfer_run_full_snapshot_discovery
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
					"-s 0 $VICTIM" | "-0 -$VICTIM" | "-TERM -$VICTIM" | "-KILL -$VICTIM" | "-STOP -$VICTIM" | "-CONT -$VICTIM" | "-INT -$VICTIM" | "-HUP -$VICTIM")
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
					zxfer_write_source_snapshot_list_to_file() {
						g_source_snapshot_list_pid=$VICTIM
					}
					zxfer_write_destination_snapshot_list_to_files() { :; }
					zxfer_run_full_snapshot_discovery
					;;
				deststart)
					g_option_R_recursive=tank/src
					zxfer_build_source_snapshot_name_list_cmd() {
						g_zxfer_shell_command_result=":"
					}
					zxfer_start_source_snapshot_producer() {
						g_last_background_pid=$VICTIM
						return 0
					}
					zxfer_start_fast_noop_destination_producer() {
						return 7
					}
					zxfer_try_fast_recursive_noop_discovery
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
			zxfer_pass_has_replication_work
			printf 'dest_source=%s\n' "$?"
			g_recursive_source_list=""
			g_option_o_override_property=""
			g_option_d_delete_destination_snapshots=1
			g_recursive_destination_extra_dataset_list="tank/src"
			zxfer_pass_has_replication_work
			printf 'dest_delete=%s\n' "$?"
			g_recursive_destination_extra_dataset_list=""
			g_option_d_delete_destination_snapshots=0
			g_option_P_transfer_property=1
			zxfer_pass_has_replication_work
			printf 'dest_props=%s\n' "$?"
		)
	)

	assertContains "Property transfer should require source dataset inventory." \
		"$output" "source_props=0"
	assertContains "Unsupported-property scanning should not require source dataset inventory after recursive no-op discovery." \
		"$output" "source_unsupported_noop=1"
	assertContains "Unsupported-property scanning should require source dataset inventory when source work may need create filtering." \
		"$output" "source_unsupported_work=0"
	assertContains "Pending transfers should require destination dataset inventory." \
		"$output" "dest_source=0"
	assertContains "Pending destination deletes should require destination dataset inventory." \
		"$output" "dest_delete=0"
	assertContains "Property work should require destination dataset inventory." \
		"$output" "dest_props=0"
}

test_set_g_recursive_source_list_reports_missing_presorted_source_sidecar() {
	dest_tmp="$TEST_TMPDIR/dest_presorted_missing.txt"
	presorted_tmp="$TEST_TMPDIR/source_presorted_missing.txt"
	: >"$dest_tmp"
	rm -f "$presorted_tmp"

	zxfer_test_capture_subshell zxfer_set_g_recursive_source_list "$presorted_tmp" "$dest_tmp"

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
		"$output" "Recursive snapshot delta summary: snapshots_missing_on_destination=3 snapshots_missing_on_source=1 source_datasets=20000 destination_extra_datasets=2"
	assertContains "Very verbose output should count every source dataset." \
		"$output" "Source dataset count: 20000"
	assertEquals "Reporting a large recursive delta should not write to stderr." \
		"" "$(cat "$stderr_file")"
}

test_report_recursive_snapshot_delta_preserves_literal_names_guids_and_destination_mapping() {
	missing_file="$TEST_TMPDIR/literal_delta_missing.txt"
	extra_file="$TEST_TMPDIR/literal_delta_extra.txt"
	l_snapshot_tail='child with spaces@snap\c%literal'
	printf 'tank/src/%s\t18446744073709551615\n' "$l_snapshot_tail" >"$missing_file"
	printf 'tank/src/%s\t18446744073709551614\n' "$l_snapshot_tail" >"$extra_file"
	g_recursive_source_list='tank/src/child with spaces'
	g_recursive_destination_extra_dataset_list=$g_recursive_source_list
	g_option_v_verbose=1

	for l_trailing_slash in 0 1; do
		g_initial_source_had_trailing_slash=$l_trailing_slash
		l_destination_root=backup/dst
		[ "$l_trailing_slash" -eq 1 ] || l_destination_root=$l_destination_root/src
		output=$(zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file")
		assertContains "The normal verbose summary must name both missing sides and count GUID conflicts." \
			"$output" "Recursive snapshot delta summary: snapshots_missing_on_destination=1 snapshots_missing_on_source=1 source_datasets=1 destination_extra_datasets=1"
		assertContains "Source names and maximum-width GUIDs must print literally." \
			"$output" "  tank/src/$l_snapshot_tail (GUID: 18446744073709551615)"
		assertContains "Destination records must show the actual mapped path [trailing slash:$l_trailing_slash]." \
			"$output" "  $l_destination_root/$l_snapshot_tail (GUID: 18446744073709551614)"
	done
}

test_nonrecursive_delta_skip_reasons_respect_exact_destination_cleanup_queue_membership() {
	missing_file="$TEST_TMPDIR/nonrecursive_missing.txt"
	extra_file="$TEST_TMPDIR/nonrecursive_extra.txt"
	printf 'tank/src/child@s1\t1\n' >"$missing_file"
	printf 'tank/src/child@s0\t0\n' >"$extra_file"
	g_option_v_verbose=1
	g_option_d_delete_destination_snapshots=1
	g_recursive_source_list=tank/src/child
	for l_cleanup_dataset in tank/src/child tank/src/childish tank/src; do
		g_recursive_destination_extra_dataset_list=$l_cleanup_dataset
		output=$(zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file")
		if [ "$l_cleanup_dataset" = tank/src/child ]; then
			assertNotContains "A descendant that cleanup will visit must not be labeled skipped." \
				"$output" "Not scheduled: nonrecursive mode"
		else
			assertContains "Cleanup membership must use literal full dataset names [$l_cleanup_dataset]." \
				"$output" "Not scheduled: nonrecursive mode"
		fi
	done
}

test_report_recursive_snapshot_delta_fails_closed_when_counted_records_disappear() {
	missing_file="$TEST_TMPDIR/delta_read_failure_missing.txt"
	extra_file="$TEST_TMPDIR/delta_read_failure_extra.txt"
	fake_awk="$TEST_TMPDIR/delta_read_failure_awk.sh"
	stdout_file="$TEST_TMPDIR/delta_read_failure.stdout"
	stderr_file="$TEST_TMPDIR/delta_read_failure.stderr"
	printf 'tank/src@s1\t123\n' >"$missing_file"
	: >"$extra_file"
	cat >"$fake_awk" <<'EOF'
#!/bin/sh
rm "$2" || exit 7
printf '1 0 1 0\n'
EOF
	chmod +x "$fake_awk"
	capture_case() {
		g_cmd_awk="$fake_awk"
		g_option_v_verbose=1
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_case
	assertEquals "A delta file lost after counting must fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "An unreadable delta must report which side failed on stderr." \
		"$(cat "$stderr_file")" "Failed to read the recursive source snapshot delta."
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

test_report_recursive_snapshot_delta_reuses_split_counts_and_fast_paths_empty_files() {
	source_file="$TEST_TMPDIR/p2_source.txt"
	dest_file="$TEST_TMPDIR/p2_dest.txt"
	missing_file="$TEST_TMPDIR/p2_missing.txt"
	extra_file="$TEST_TMPDIR/p2_extra.txt"
	scratch_file="$TEST_TMPDIR/p2_scratch.txt"
	printf 'tank/src@s1\t1\ntank/src/a@s2\t2\n' | LC_ALL=C sort >"$source_file"
	printf 'tank/src@extra\t9\n' >"$dest_file"

	# Provenanced split counts let delta reporting skip awk while counting the
	# final post-filter/-prune dataset lists in-shell (matching awk NF).
	output=$(
		zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
		g_cmd_awk=false
		g_option_v_verbose=1
		g_option_V_very_verbose=0
		g_recursive_source_list="tank/src${ZXFER_LF}   ${ZXFER_LF}${ZXFER_TAB}${ZXFER_LF}tank/src/a"
		g_recursive_destination_extra_dataset_list="tank/src"
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
		printf 'provenance=<%s|%s>\n' \
			"$g_zxfer_delta_count_missing_file" "$g_zxfer_delta_count_extra_file"
	)
	assertEquals "Provenanced delta report should succeed without spawning awk." 0 "$?"
	assertContains "Provenanced delta report should emit split snapshot counts and final in-shell dataset counts." \
		"$output" "Recursive snapshot delta summary: snapshots_missing_on_destination=2 snapshots_missing_on_source=1 source_datasets=2 destination_extra_datasets=1"
	assertContains "Consuming provenanced counts should clear provenance." \
		"$output" "provenance=<|>"

	# Empty readable delta files fast-path to 0/0 without awk and still emit -V.
	: >"$missing_file"
	: >"$extra_file"
	empty_output=$(
		g_cmd_awk=false
		g_option_v_verbose=0
		g_option_V_very_verbose=1
		g_recursive_source_list=""
		g_recursive_destination_extra_dataset_list=""
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
	)
	assertEquals "Empty readable delta files should fast-path without awk." 0 "$?"
	assertContains "Empty fast-path must still emit the -V source dataset count." \
		"$empty_output" "Source dataset count: 0"
	assertContains "Empty fast-path must still emit the -V extra section." \
		"$empty_output" "====== Extra Destination snapshots not in source ======"
}

test_report_recursive_snapshot_delta_clears_provenance_and_checks_readability() {
	source_file="$TEST_TMPDIR/p2_prov_source.txt"
	dest_file="$TEST_TMPDIR/p2_prov_dest.txt"
	missing_file="$TEST_TMPDIR/p2_prov_missing.txt"
	extra_file="$TEST_TMPDIR/p2_prov_extra.txt"
	scratch_file="$TEST_TMPDIR/p2_prov_scratch.txt"
	stdout_file="$TEST_TMPDIR/p2_prov_stdout.txt"
	stderr_file="$TEST_TMPDIR/p2_prov_stderr.txt"
	printf 'tank/src@s1\t1\n' >"$source_file"
	printf 'tank/src@s1\t1\n' >"$dest_file"

	# (a1) Empty-file fast path clears 0/0 provenance so a later non-empty write
	# to the same paths is recounted instead of reusing stale 0/0 counts.
	empty_recount_output=$(
		zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
		g_option_v_verbose=1
		g_option_V_very_verbose=0
		g_recursive_source_list=""
		g_recursive_destination_extra_dataset_list=""
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
		printf 'after_empty=<%s|%s|%s|%s>\n' \
			"$g_zxfer_delta_missing_count" "$g_zxfer_delta_extra_count" \
			"$g_zxfer_delta_count_missing_file" "$g_zxfer_delta_count_extra_file"
		printf 'tank/src@s2\t2\n' >"$missing_file"
		g_recursive_source_list="tank/src"
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
	)
	l_empty_recount_status=$?
	assertEquals "Recount after empty-file delta report should succeed." 0 "$l_empty_recount_status"
	assertContains "Empty-file delta report must clear all four provenance globals." \
		"$empty_recount_output" "after_empty=<|||>"
	assertContains "Subsequent non-empty write to same paths must be recounted accurately." \
		"$empty_recount_output" "Recursive snapshot delta summary: snapshots_missing_on_destination=1 snapshots_missing_on_source=0 source_datasets=1 destination_extra_datasets=0"
	assertContains "Recounted delta must list the newly written snapshot identity." \
		"$empty_recount_output" "tank/src@s2 (GUID: 2)"

	# (a2) Quiet early return (-v=0 -V=0) also clears provenance so a later verbose
	# call on overwritten files recounts accurately.
	quiet_recount_output=$(
		zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
		g_option_v_verbose=0
		g_option_V_very_verbose=0
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
		printf 'after_quiet=<%s|%s|%s|%s>\n' \
			"$g_zxfer_delta_missing_count" "$g_zxfer_delta_extra_count" \
			"$g_zxfer_delta_count_missing_file" "$g_zxfer_delta_count_extra_file"
		printf 'tank/src@s2\t2\ntank/src@s3\t3\n' >"$missing_file"
		g_option_v_verbose=1
		g_recursive_source_list="tank/src"
		g_recursive_destination_extra_dataset_list=""
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
	)
	l_quiet_recount_status=$?
	assertEquals "Recount after quiet delta report should succeed." 0 "$l_quiet_recount_status"
	assertContains "Quiet delta report must clear all four provenance globals." \
		"$quiet_recount_output" "after_quiet=<|||>"
	assertContains "Verbose delta report after quiet call must recount updated files." \
		"$quiet_recount_output" "Recursive snapshot delta summary: snapshots_missing_on_destination=2 snapshots_missing_on_source=0 source_datasets=1 destination_extra_datasets=0"

	# (b) A vanished or unreadable file under matching 0/0 provenance must fail
	# closed through the awk branch instead of trusting cached 0/0 counts.
	capture_removed_provenanced_case() {
		zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
		rm -f "$missing_file"
		g_option_v_verbose=1
		g_option_V_very_verbose=0
		g_recursive_source_list=""
		g_recursive_destination_extra_dataset_list=""
		zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
	}
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_removed_provenanced_case
	assertNotEquals "A removed delta file under matching 0/0 provenance must fail closed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A removed provenanced delta file must report the counting failure." \
		"$(cat "$stderr_file")" "Failed to count the recursive snapshot delta."
	assertNotContains "A removed provenanced delta file must not emit a summary." \
		"$(cat "$stdout_file")" "Recursive snapshot delta summary"

	if [ "$(id -u)" -ne 0 ]; then
		capture_unreadable_provenanced_case() {
			zxfer_write_snapshot_delta_files "$source_file" "$dest_file" "$missing_file" "$extra_file" "$scratch_file"
			chmod 000 "$extra_file"
			g_option_v_verbose=1
			g_option_V_very_verbose=0
			g_recursive_source_list=""
			g_recursive_destination_extra_dataset_list=""
			zxfer_report_recursive_snapshot_delta "$missing_file" "$extra_file"
		}
		zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" capture_unreadable_provenanced_case
		chmod 600 "$extra_file" 2>/dev/null || :
		assertNotEquals "An unreadable delta file under matching 0/0 provenance must fail closed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
		assertContains "An unreadable provenanced delta file must report the counting failure." \
			"$(cat "$stderr_file")" "Failed to count the recursive snapshot delta."
	fi
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

test_collect_destination_dataset_inventory_preserves_setup_and_publish_failures() {
	temp_status=$(
		(
			zxfer_create_runtime_artifact_workspace() {
				return 66
			}
			set +e
			zxfer_collect_destination_dataset_inventory
			printf '%s\n' "$?"
		)
	)
	publish_status=$(
		(
			l_one="$TEST_TMPDIR/dest_inventory_publish.one"
			l_two="$TEST_TMPDIR/dest_inventory_publish.two"
			g_option_V_very_verbose=1
			zxfer_create_runtime_artifact_workspace() {
				: >"$l_one"
				: >"$l_one.error"
				g_zxfer_runtime_artifact_path_result=$l_one
				g_zxfer_temp_file_group_result=$(printf '%s\n%s' "$l_one" "$l_one.error")
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_publish_destination_dataset_inventory_from_stage() {
				return 67
			}
			set +e
			zxfer_collect_destination_dataset_inventory 2>/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Destination inventory should preserve temp-file group allocation failures." \
		66 "$temp_status"
	assertEquals "Destination inventory should preserve publish failures after cleanup." \
		67 "$publish_status"
}

test_get_zfs_list_reports_destination_inventory_readback_failures() {
	set +e
	output=$(
		(
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/existing"
					return 0
				fi
				return 1
			}
			zxfer_read_runtime_artifact_file() {
				return 27
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Destination inventory readback failures should preserve the staged read status." \
		27 "$status"
	assertContains "Destination inventory readback failures should report the staged destination inventory context." \
		"$output" "Failed to read staged destination dataset inventory."
}

test_get_zfs_list_reports_destination_inventory_stderr_readback_failures() {
	set +e
	probe_log="$TEST_TMPDIR/get_zfs_destination_inventory_stderr_probe.log"
	: >"$probe_log"
	output=$(
		(
			PROBE_LOG="$probe_log"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_destination_probe_reports_missing() {
				printf '%s\n' "called" >>"$PROBE_LOG"
				return 0
			}
			zxfer_read_runtime_artifact_file() {
				return 28
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Destination inventory stderr readback failures should preserve the staged stderr read status." \
		28 "$status"
	assertContains "Destination inventory stderr readback failures should report the staged stderr context." \
		"$output" "Failed to read staged destination dataset inventory stderr."
	assertFalse "Destination inventory stderr readback failures should not continue into missing-destination fallback checks." \
		"[ -s '$probe_log' ]"
}

test_get_zfs_list_reports_source_snapshot_record_cache_stage_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			l_temp_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
				printf '%s\n' "tank/src@snapA" >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_read_runtime_artifact_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_runtime_artifact_read_result="backup/dst"
				else
					return 1
				fi
				return 0
			}
			zxfer_get_temp_file() {
				l_temp_count=$((l_temp_count + 1))
				g_zxfer_runtime_artifact_path_result="$TEST_TMPDIR/get-zfs-source-cache-stage-$l_temp_count.tmp"
				: >"$g_zxfer_runtime_artifact_path_result"
				return 0
			}
			zxfer_reverse_file_lines() {
				return 1
			}
			zxfer_throw_error() {
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?
	set -e

	assertEquals "Source snapshot record-cache staging failures should abort snapshot discovery." \
		1 "$status"
	assertContains "Source snapshot record-cache staging failures should report the staged source-cache context." \
		"$output" "msg=Failed to stage source snapshot record cache."
}

# Probe zxfer_fast_recursive_noop_options_are_eligible with a clean option
# state plus the supplied overrides; prints the helper's exit status.
# Errexit-safe so the suite's set -e tests cannot abort the caller.
zxfer_test_noop_proof_eligibility_status() {
	l_eligibility_status=0
	(
		g_option_O_origin_host=""
		g_option_T_target_host=""
		g_option_R_recursive="tank/src"
		g_option_s_make_snapshot=0
		g_option_m_migrate=0
		g_option_P_transfer_property=0
		g_option_o_override_property=""
		g_option_e_restore_property_mode=0
		g_option_k_backup_property_mode=0
		for l_eligibility_override in "$@"; do
			eval "$l_eligibility_override"
		done
		zxfer_fast_recursive_noop_options_are_eligible
	) || l_eligibility_status=$?
	printf '%s\n' "$l_eligibility_status"
	return 0
}

test_fast_recursive_noop_discovery_eligibility_gates() {
	# Local sources are eligible since Phase 8: -O is no longer consulted.
	# Every other gate stays: -R required, -T absent, and the no-op-unsafe
	# options (-s/-m/-P/-o/-e/-k) must all be off.
	assertEquals "A plain local recursive run must be proof-eligible." \
		0 "$(zxfer_test_noop_proof_eligibility_status)"
	assertEquals "A remote-origin recursive run must stay proof-eligible." \
		0 "$(zxfer_test_noop_proof_eligibility_status \
			"g_option_O_origin_host=origin.example")"
	assertEquals "Non-recursive runs must stay ineligible." \
		1 "$(zxfer_test_noop_proof_eligibility_status \
			"g_option_R_recursive=")"
	assertEquals "-T target-host runs must stay ineligible." \
		1 "$(zxfer_test_noop_proof_eligibility_status \
			"g_option_T_target_host=target.example")"
	for l_eligibility_gate in \
		"g_option_s_make_snapshot=1" \
		"g_option_m_migrate=1" \
		"g_option_P_transfer_property=1" \
		"g_option_o_override_property=copies=2" \
		"g_option_e_restore_property_mode=1" \
		"g_option_k_backup_property_mode=1"; do
		assertEquals "Runs with $l_eligibility_gate must stay ineligible for the no-op proof." \
			1 "$(zxfer_test_noop_proof_eligibility_status "$l_eligibility_gate")"
	done
	# A no-op cannot need -U, -g or -d work, so they keep the proof.
	for l_eligibility_safe in \
		"g_option_U_skip_unsupported_properties=1" \
		"g_option_g_grandfather_protection=30" \
		"g_option_d_delete_destination_snapshots=1"; do
		assertEquals "Runs with $l_eligibility_safe must stay proof-eligible." \
			0 "$(zxfer_test_noop_proof_eligibility_status "$l_eligibility_safe")"
	done
}

test_try_fast_recursive_noop_discovery_preserves_setup_failures() {
	temp_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_create_runtime_artifact_workspace() {
				return 42
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	build_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				return 43
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	execute_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_source_snapshot_producer() {
				return 44
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	destination_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_source_snapshot_producer() {
				sleep 5 &
				g_last_background_pid=$!
				zxfer_register_cleanup_pid "$g_last_background_pid" "background source snapshot no-op proof helper" || :
				return 0
			}
			zxfer_start_fast_noop_destination_producer() {
				return 45
			}
			zxfer_abort_direct_child_pid() {
				kill "$1" 2>/dev/null || :
				return 0
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)

	assertEquals "Fast no-op proof should preserve temp-file allocation failures." \
		42 "$temp_status"
	assertEquals "Fast no-op proof should preserve source command render failures." \
		43 "$build_status"
	assertEquals "Fast no-op proof should preserve background source launch failures." \
		44 "$execute_status"
	assertEquals "Fast no-op proof should preserve destination discovery failures and abort the background source proof." \
		45 "$destination_status"
}

# A failed stderr readback stops the proof before any source diagnostic.
test_try_fast_recursive_noop_discovery_reports_source_failures() {
	stderr_read_status=0
	stderr_read_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="sh -c 'exit 17'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			# The staged stderr is the first file this proof reads back.
			zxfer_read_runtime_artifact_file() {
				return 68
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || stderr_read_status=$?
	assertContains "Fast no-op proof should report staged stderr readback failures before surfacing source failure context." \
		"$stderr_read_output" "throw:Failed to read staged source snapshot stderr.:68"
	assertEquals "Fast no-op proof should preserve staged stderr readback failure status." \
		68 "$stderr_read_status"
}

# A producer may omit the line terminator on its final record. Compare all
# identity bytes, including the GUID, even when it is the only source record.
test_fast_recursive_noop_compares_the_whole_unterminated_source_record() {
	for destination_guid in 123 1234; do
		proof_status=0
		(
			g_option_R_recursive=tank/src
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s' 'tank/src@snapA${ZXFER_TAB}123'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA${ZXFER_TAB}$destination_guid"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_try_fast_recursive_noop_discovery
		) || proof_status=$?
		expected_status=0
		[ "$destination_guid" = 123 ] || expected_status=1
		assertEquals "The proof accepts only a complete identity match for GUID $destination_guid." \
			"$expected_status" "$proof_status"
	done
}

# The proof's destination producer writes its listing exit status to the status
# file before normalizing: a malformed status file fails the proof closed, a
# failed stderr readback keeps its own status, and a failed producer exit keeps
# its status.
test_try_fast_recursive_noop_discovery_reports_destination_fifo_status_failures() {
	malformed_status=0
	malformed_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS="bad"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			zxfer_try_fast_recursive_noop_discovery
		)
	) || malformed_status=$?
	assertContains "Fast no-op proof should fail closed on a malformed destination listing status." \
		"$malformed_output" "throw:Failed to validate destination snapshot status for recursive no-op proof.:1"
	assertEquals "Fast no-op proof should return failure for a malformed destination listing status." \
		1 "$malformed_status"
	destination_stderr_read_status=0
	destination_stderr_read_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS=17
				ZXFER_TEST_FAST_NOOP_DESTINATION_STDERR="permission denied"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			# The staged destination stderr is the first file read back.
			zxfer_read_runtime_artifact_file() {
				return 70
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || destination_stderr_read_status=$?
	sort_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_STREAM_STATUS=23
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)
	destination_wait_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				l_fifo=$1
				l_err_file=$2
				l_status_file=$3
				(
					printf '%s\n' "tank/src@snapA" >"$l_fifo"
					: >"$l_err_file"
					printf '%s\n' "0" >"$l_status_file"
					exit 31
				) &
				g_last_background_pid=$!
				zxfer_register_cleanup_pid "$g_last_background_pid" "test destination snapshot no-op proof helper"
			}
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertContains "Fast no-op proof should report destination stderr readback failures before surfacing destination snapshot context." \
		"$destination_stderr_read_output" "throw:Failed to read staged destination snapshot stderr.:70"
	assertEquals "Fast no-op proof should preserve destination stderr readback failure status." \
		70 "$destination_stderr_read_status"
	assertEquals "Fast no-op proof should preserve destination stream failures." \
		23 "$sort_status"
	assertEquals "Fast no-op proof should preserve destination producer wait failures." \
		31 "$destination_wait_status"
}

test_try_fast_recursive_noop_discovery_reports_compare_failures() {
	compare_status=0
	compare_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			cmp() {
				return 2
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		)
	) || compare_status=$?

	assertContains "Fast no-op proof should report compare failures with no-op proof context." \
		"$compare_output" "throw:Failed to compare source and destination snapshots for recursive no-op proof.:2"
	assertEquals "Fast no-op proof should preserve compare failure status." \
		2 "$compare_status"
}

test_get_zfs_list_throws_on_stage_failures_that_did_not_throw() {
	set +e
	output=$(
		(
			zxfer_try_fast_recursive_noop_discovery() {
				return 58
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "unexpected full discovery"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_get_zfs_list
		)
		printf 'fast_status=%s\n' "$?"
		(
			zxfer_try_fast_recursive_noop_discovery() {
				return 1
			}
			zxfer_write_source_snapshot_list_to_file() {
				:
			}
			zxfer_write_destination_snapshot_list_to_files() {
				return 9
			}
			zxfer_wait_for_snapshot_discovery_producer() {
				printf '%s\n' "unexpected source wait"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_get_zfs_list
		)
		printf 'full_status=%s\n' "$?"
	)

	assertNotContains "A fast no-op proof hard failure should not continue into full discovery." \
		"$output" "unexpected"
	assertContains "Its callers ignore the status, so discovery should throw a fast proof hard failure." \
		"$output" "throw:Failed to discover source and destination snapshots.:58"
	assertContains "The thrown failure should keep the fast proof status." \
		"$output" "fast_status=58"
	assertContains "Discovery should throw a full-discovery stage failure that returned without throwing." \
		"$output" "throw:Failed to discover source and destination snapshots.:9"
	assertContains "The thrown failure should keep the stage status." \
		"$output" "full_status=9"
}

test_get_zfs_list_tracks_stage_timings_when_very_verbose() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_profile.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_runtime_artifact_path_result="$TEST_TMPDIR/get_zfs_profile.$idx"
				: >"$g_zxfer_runtime_artifact_path_result"
			}
			# One reading per clock read, in this shell: source start,
			# destination start and end, source end, diff start and end.
			clock_readings="1000 1500 1900 2600 3000 3550"
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=${clock_readings%% *}
				clock_readings=${clock_readings#* }
			}
			zxfer_echoV() {
				:
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_reverse_file_lines() {
				cat "$1"
			}
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_ms=%s\n' "${g_zxfer_profile_source_snapshot_listing_ms:-0}"
			printf 'destination_ms=%s\n' "${g_zxfer_profile_destination_snapshot_listing_ms:-0}"
			printf 'diff_ms=%s\n' "${g_zxfer_profile_snapshot_diff_sort_ms:-0}"
		)
	)

	assertContains "Very-verbose snapshot discovery should accumulate source snapshot listing timings." \
		"$output" "source_ms=1600"
	assertContains "Very-verbose snapshot discovery should accumulate destination listing timings." \
		"$output" "destination_ms=400"
	assertContains "Very-verbose snapshot discovery should accumulate diff/sort timings." \
		"$output" "diff_ms=550"
}

# Status is a completed-producer result, consumed by both raw-list handoff and
# proof validation. A second parse would make their view of one operation differ.
test_fast_recursive_noop_reads_destination_status_once() {
	output=$(
		(
			g_option_R_recursive="-R"
			status_reads=0
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			read() {
				if [ "$*" = '-r l_fast_noop_owner_list_status' ]; then
					status_reads=$((status_reads + 1))
				fi
				# shellcheck disable=SC2162 # Forward the production read arguments.
				command read "$@"
			}
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf 'status=%s reads=%s\n' "$?" "$status_reads"
		)
	)
	assertEquals "A successful proof consumes one completed status record." \
		"status=0 reads=1" "$output"
}

# The operation owner must retain the failure status and diagnostic before it
# clears staged state. Cleanup deliberately returns a different status here.
test_fast_recursive_noop_owner_reports_failure_after_releasing_scratch() {
	status=0
	output=$(
		(
			g_option_R_recursive="-R"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'original source failure' >&2; exit 37"
			}
			zxfer_start_fast_noop_destination_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			release_called=0
			zxfer_cleanup_runtime_artifact_path_list() {
				release_called=1
				return 91
			}
			zxfer_throw_error() {
				printf 'status=%s diagnostic=<%s> released=%s\n' \
					"$2" "$1" "$release_called"
				exit "$2"
			}
			zxfer_try_fast_recursive_noop_discovery
		)
	) || status=$?
	assertEquals "Cleanup cannot overwrite the original failed producer status." 37 "$status"
	assertEquals "The diagnostic survives scratch reset and is reported afterwards." \
		"status=37 diagnostic=<Failed to retrieve snapshots from the source: original source failure> released=1" "$output"
}

# Allocation scratch and result channels may change while the producers run.
# The owner must still wait with the right timers, release its proof
# files and retain the successful raw listing when the streams differ.
test_fast_recursive_noop_owner_keeps_paths_pids_and_timers_across_helper_scratch_mutation() {
	output=$(
		(
			g_option_R_recursive="-R"
			owner_clock=0
			owner_timers=""
			zxfer_profile_start_timer() {
				owner_clock=$((owner_clock + 1))
				g_zxfer_profile_clock_ms=$owner_clock
			}
			zxfer_profile_stop_timer() {
				owner_timers=${owner_timers:+$owner_timers,}$1
				g_zxfer_profile_elapsed_ms=0
				g_last_background_pid=clobbered
			}
			zxfer_build_source_snapshot_name_list_cmd() {
				owner_proof_paths=$g_zxfer_temp_file_group_result
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_fast_noop_destination_producer() {
				owner_sorted_path=$1
				owner_raw_path=$4
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapB"
				ZXFER_TEST_FAST_NOOP_DESTINATION_RAW="tank/dst@snapB"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
				l_fast_noop_owner_source_file=clobbered
				l_fast_noop_owner_status_file=clobbered
				g_zxfer_runtime_artifact_path_result=clobbered
				g_zxfer_temp_file_group_result=clobbered
			}
			status=0
			zxfer_try_fast_recursive_noop_discovery || status=$?
			printf 'status=%s timers=%s\n' "$status" "$owner_timers"
			while IFS= read -r owner_proof_path; do
				[ "$owner_proof_path" != "$owner_raw_path" ] || continue
				[ "$owner_proof_path" != "$owner_sorted_path" ] || continue
				[ ! -e "$owner_proof_path" ] || printf 'unreleased=%s\n' "$owner_proof_path"
			done <<EOF
$owner_proof_paths
EOF
			if [ "$g_zxfer_snapshot_discovery_destination_listing_file" = "$owner_raw_path" ] &&
				[ "$(cat "$owner_raw_path")" = "tank/dst@snapB" ]; then
				printf 'raw retained\n'
			fi
			if [ "$g_zxfer_snapshot_discovery_destination_sorted_file" = "$owner_sorted_path" ] &&
				[ "$(cat "$owner_sorted_path")" = "tank/src@snapB" ]; then
				printf 'sorted retained\n'
			fi
			printf 'producer records=<%s>\n' "$g_zxfer_cleanup_pid_records"
		)
	)
	assertEquals "Fallback retains its raw and pre-sorted listings and releases reaped proof resources despite helper scratch changes." \
		"status=1 timers=1,2,3
raw retained
sorted retained
producer records=<>" "$output"
}

test_fast_noop_fallback_reuses_sorted_destination_only_when_unfiltered_and_clean() {
	# Unfiltered clean fallback reuses .destination without re-normalizing,
	# sets the destination existence cache, retains .raw as record cache when
	# work is found, and releases .destination on both success and failure.
	reuse_output=$(
		(
			g_option_R_recursive="-R"
			g_option_x_exclude_datasets=""
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA${ZXFER_TAB}101' 'tank/src@snapB${ZXFER_TAB}102'"
			}
			zxfer_start_fast_noop_destination_producer() {
				reused_sorted_path=$1
				reused_raw_path=$4
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA${ZXFER_TAB}101"
				ZXFER_TEST_FAST_NOOP_DESTINATION_RAW="backup/dst/src@snapA${ZXFER_TAB}101"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			proof_status=0
			zxfer_try_fast_recursive_noop_discovery || proof_status=$?
			zxfer_write_source_snapshot_list_to_file() {
				printf 'tank/src@snapA\t101\ntank/src@snapB\t102\n' >"$1"
				: >"$2"
				printf 'tank/src@snapA\t101\ntank/src@snapB\t102\n' >"$3"
				g_source_snapshot_list_pid=""
			}
			zxfer_normalize_destination_snapshot_list() {
				printf 'unexpected normalize\n'
				return 99
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf 'unexpected dest list\n'
				return 98
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst" "backup/dst/src"
			}
			full_status=0
			zxfer_run_full_snapshot_discovery || full_status=$?
			sorted_exists=0
			[ ! -e "$reused_sorted_path" ] || sorted_exists=1
			raw_retained=0
			[ "$g_zxfer_destination_snapshot_record_cache_file" != "$reused_raw_path" ] ||
				[ ! -f "$reused_raw_path" ] || raw_retained=1
			cache_exists=0
			zxfer_destination_exists backup/dst/src cache && cache_exists=1 || :
			printf 'proof=%s full=%s list=<%s> cache=%s sorted_exists=%s raw_retained=%s handoff=<%s|%s>\n' \
				"$proof_status" "$full_status" "$g_recursive_source_list" "$cache_exists" \
				"$sorted_exists" "$raw_retained" \
				"$g_zxfer_snapshot_discovery_destination_listing_file" \
				"$g_zxfer_snapshot_discovery_destination_sorted_file"
		)
	)
	assertEquals "Unfiltered clean fallback should skip destination re-normalization and release the reused sorted file." \
		"proof=1 full=0 list=<tank/src> cache=1 sorted_exists=0 raw_retained=1 handoff=<|>" "$reuse_output"

	# On full-discovery failure, both handed-off .raw and .destination are removed.
	failure_output=$(
		(
			g_option_R_recursive="-R"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA${ZXFER_TAB}101'"
			}
			zxfer_start_fast_noop_destination_producer() {
				fail_sorted_path=$1
				fail_raw_path=$4
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapB${ZXFER_TAB}102"
				ZXFER_TEST_FAST_NOOP_DESTINATION_RAW="backup/dst/src@snapB${ZXFER_TAB}102"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_try_fast_recursive_noop_discovery || :
			zxfer_write_source_snapshot_list_to_file() { return 53; }
			fail_status=0
			zxfer_run_full_snapshot_discovery || fail_status=$?
			leftover=0
			[ ! -e "$fail_sorted_path" ] && [ ! -e "$fail_raw_path" ] || leftover=1
			printf 'status=%s leftover=%s\n' "$fail_status" "$leftover"
		)
	)
	assertEquals "Full-discovery failure must release both handed-off proof files." \
		"status=53 leftover=0" "$failure_output"

	# When -x is set or destination normalization failed, .destination must NOT
	# be handed off (only .raw may survive).
	for skip_case in exclude dest_error; do
		skip_output=$(
			(
				g_option_R_recursive="-R"
				[ "$skip_case" != exclude ] || g_option_x_exclude_datasets="/skip$"
				zxfer_build_source_snapshot_name_list_cmd() {
					g_zxfer_shell_command_result="printf '%s\n' 'tank/src@snapA${ZXFER_TAB}101'"
				}
				zxfer_start_fast_noop_destination_producer() {
					skip_sorted_path=$1
					skip_raw_path=$4
					ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapB${ZXFER_TAB}102"
					ZXFER_TEST_FAST_NOOP_DESTINATION_RAW="backup/dst/src@snapB${ZXFER_TAB}102"
					[ "$skip_case" != dest_error ] || ZXFER_TEST_FAST_NOOP_DESTINATION_STREAM_STATUS=19
					zxfer_test_start_fast_noop_destination_fifo_producer "$@"
				}
				proof_status=0
				zxfer_try_fast_recursive_noop_discovery || proof_status=$?
				sorted_exists=0
				[ ! -e "$skip_sorted_path" ] || sorted_exists=1
				raw_match=0
				[ "$g_zxfer_snapshot_discovery_destination_listing_file" != "$skip_raw_path" ] || raw_match=1
				printf 'proof=%s raw=%s sorted_global=<%s> sorted_exists=%s\n' \
					"$proof_status" "$raw_match" \
					"$g_zxfer_snapshot_discovery_destination_sorted_file" "$sorted_exists"
			)
		)
		assertEquals "Fast proof [$skip_case] must not hand off .destination." \
			"proof=1 raw=1 sorted_global=<> sorted_exists=0" "$skip_output"
	done

	# Reset clears the pre-sorted handoff and delta provenance globals.
	g_zxfer_snapshot_discovery_destination_sorted_file="/tmp/stale.sorted"
	g_zxfer_delta_missing_count=3
	g_zxfer_delta_extra_count=4
	g_zxfer_delta_count_missing_file="/tmp/stale.missing"
	g_zxfer_delta_count_extra_file="/tmp/stale.extra"
	zxfer_reset_snapshot_discovery_state
	assertEquals "Discovery reset should clear pre-sorted handoff and delta count provenance." \
		"||||" \
		"$g_zxfer_snapshot_discovery_destination_sorted_file|$g_zxfer_delta_missing_count|$g_zxfer_delta_extra_count|$g_zxfer_delta_count_missing_file|$g_zxfer_delta_count_extra_file"
}

# A destination/setup failure can reach release before source wait. Keep the
# unreaped producer's files for the immediate failure throw's ordered EXIT trap.
test_full_discovery_release_preserves_unreaped_producer_files() {
	for producer_pending in 1 0; do
		output=$(
			zxfer_write_source_snapshot_list_to_file() {
				source_file=$1
				error_file=$2
				printf 'tank/src@snapA\n' >"$1"
				# An ownership marker avoids an extra child just for this guard.
				[ "$producer_pending" -eq 0 ] || g_source_snapshot_list_pid=12345
				return 0
			}
			zxfer_write_destination_snapshot_list_to_files() { return 29; }
			status=0
			zxfer_run_full_snapshot_discovery || status=$?
			retained=0
			[ ! -f "$source_file" ] || retained=1
			printf 'status=%s retained=%s\n' "$status" "$retained"
			[ "$producer_pending" -eq 0 ] || [ -f "$error_file" ] || printf 'error file lost\n'
		)
		assertEquals "Only an unreaped producer keeps its stages for trap teardown." \
			"status=29 retained=$producer_pending" "$output"
	done
}

# Helper scratch and shared allocator result channels must not become the
# operation's file registry: stages can overwrite them without losing ownership.
test_full_discovery_keeps_owned_paths_across_helper_scratch_mutation() {
	output=$(
		(
			g_option_R_recursive="-R"
			zxfer_write_source_snapshot_list_to_file() {
				source_seen=$1
				error_seen=$2
				sorted_seen=$3
				printf 'tank/src@snapA\n' >"$1"
				: >"$2"
				printf 'tank/src@snapA\n' >"$3"
				l_full_owner_source_file=clobbered
				l_full_owner_sorted_file=clobbered
				g_zxfer_temp_file_group_result=clobbered
			}
			zxfer_write_destination_snapshot_list_to_files() {
				destination_seen=$1
				destination_sorted_seen=$2
				: >"$1"
				: >"$2"
				g_zxfer_runtime_artifact_path_result=clobbered
			}
			zxfer_set_g_recursive_source_list() {
				[ "$#" -eq 2 ] && [ "$1" = "$sorted_seen" ] && [ "$2" = "$destination_sorted_seen" ] || return 73
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_full_snapshot_discovery
			printf 'status=%s\n' "$?"
			for owned_path in "$source_seen" "$error_seen" "$sorted_seen" \
				"$destination_seen" "$destination_sorted_seen"; do
				[ ! -e "$owned_path" ] || printf 'unreleased=%s\n' "$owned_path"
			done
		)
	)
	assertEquals "The owner retains and releases its real paths after helpers mutate scratch." \
		"status=0" "$output"
}

# A failed best-effort release leaves artifacts for the run-root trap, while
# the owner returns the discovery failure that made release necessary.
test_full_discovery_cleanup_failure_keeps_the_operation_status() {
	output=$(
		zxfer_write_source_snapshot_list_to_file() { printf 'tank/src@snapA\n' >"$1"; }
		zxfer_write_destination_snapshot_list_to_files() { return 37; }
		zxfer_cleanup_runtime_artifact_path_list() { return 43; }
		zxfer_cleanup_snapshot_record_cache_files() { printf 'caches discarded\n'; }
		status=0
		zxfer_run_full_snapshot_discovery || status=$?
		printf 'status=%s\n' "$status"
	)
	assertEquals "Release failure cannot replace discovery's failure or skip cache disposal." \
		"caches discarded
status=37" "$output"
}

# Published records outlive full discovery even when a producer changes the
# allocator result; transient files are still released before returning.
test_full_discovery_retains_only_published_record_caches() {
	output=$(
		zxfer_write_source_snapshot_list_to_file() {
			transient_source=$1
			printf 'tank/src@snapA\t101\n' >"$1"
		}
		zxfer_write_destination_snapshot_list_to_files() {
			retained_destination=$1
			printf 'backup/dst@snapA\t101\n' >"$1"
			g_zxfer_runtime_artifact_path_result=clobbered
		}
		zxfer_set_g_recursive_source_list() { g_recursive_source_list=""; }
		zxfer_run_full_snapshot_discovery || exit "$?"
		[ ! -e "$transient_source" ] || printf 'source stage leaked\n'
		[ "$retained_destination" = "$g_zxfer_destination_snapshot_record_cache_file" ] || printf 'destination cache lost\n'
		cat "$g_zxfer_source_snapshot_record_cache_file" "$g_zxfer_destination_snapshot_record_cache_file"
	)
	assertEquals "Planning retains the validated records, with transient source staging released." \
		"$(printf 'tank/src@snapA\t101\nbackup/dst@snapA\t101')" "$output"
}

# Exercise retention through full discovery, using real delta planning,
# inventory publication and cache staging over canned endpoint records.
test_full_discovery_retention_follows_dataset_work_and_nonrecursive_mode() {
	zxfer_test_run_full_discovery_retention_case() {
		g_option_R_recursive="tank/src"
		case $retention_case in
		nonrecursive | nonrecursive_transfer) g_option_R_recursive="" ;;
		delete) g_option_d_delete_destination_snapshots=1 ;;
		property) g_option_P_transfer_property=1 ;;
		override) g_option_o_override_property="compression=lz4" ;;
		esac
		zxfer_write_source_snapshot_list_to_file() {
			printf 'tank/src@snapA\t101\n' >"$1"
			printf 'tank/src@snapA\t101\n' >"$3"
		}
		zxfer_write_destination_snapshot_list_to_files() {
			: >"$1"
			case $retention_case in
			transfer | nonrecursive_transfer) ;;
			*) printf 'backup/dst/src@snapA\t101\n' >"$1" ;;
			esac
			case $retention_case in
			delete | ignored_extra) printf 'backup/dst/src@extra\t102\n' >>"$1" ;;
			esac
			zxfer_normalize_destination_snapshot_list backup/dst/src "$1" "$2"
		}
		zxfer_run_source_zfs_cmd() { printf '%s\n' tank/src; }
		zxfer_run_destination_zfs_cmd() { printf '%s\n' backup/dst backup/dst/src; }
		zxfer_run_full_snapshot_discovery || exit "$?"
		inventory_present=0
		[ -z "$g_recursive_dest_list" ] || inventory_present=1
		records_present=0
		if [ -f "$g_zxfer_source_snapshot_record_cache_file" ] &&
			[ -f "$g_zxfer_destination_snapshot_record_cache_file" ]; then
			records_present=1
		fi
		printf 'inventory=%s records=%s\n' "$inventory_present" "$records_present"
	}
	for retention_case in noop nonrecursive nonrecursive_transfer transfer delete ignored_extra property override; do
		output=$(zxfer_test_run_full_discovery_retention_case)
		case $retention_case in
		noop | ignored_extra) expected_retention="inventory=0 records=0" ;;
		nonrecursive) expected_retention="inventory=0 records=1" ;;
		*) expected_retention="inventory=1 records=1" ;;
		esac
		assertEquals "$retention_case retains only the inventories and records its work needs." \
			"$expected_retention" "$output"
	done
}

test_full_discovery_inventory_failure_never_publishes_record_caches() {
	for retention_mode in recursive nonrecursive; do
		output=$(
			g_option_R_recursive="tank/src"
			[ "$retention_mode" != nonrecursive ] || g_option_R_recursive=""
			zxfer_write_source_snapshot_list_to_file() {
				printf 'tank/src@snapA\t101\n' >"$1"
				printf 'tank/src@snapA\t101\n' >"$3"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_collect_destination_dataset_inventory() { return 37; }
			status=0
			zxfer_run_full_snapshot_discovery || status=$?
			printf 'status=%s source=<%s> destination=<%s>\n' "$status" \
				"$g_zxfer_source_snapshot_record_cache_file" "$g_zxfer_destination_snapshot_record_cache_file"
		)
		assertEquals "$retention_mode cannot publish record files after a failed inventory." \
			"status=37 source=<> destination=<>" "$output"
	done
}

test_snapshot_dataset_lists_extract_sort_and_deduplicate_each_role() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file.txt"
	printf '%s\n' "tank/src/child@snap2" "tank/src/child@snap3" "tank/src/space name@snap4" "tank/src@snap1" \
		>"$snapshot_records_file"
	extra_records_file="$TEST_TMPDIR/extra_snapshot_file.txt"
	printf '%s\n' "tank/src@snap5" "tank/src@snap6" "tank/srcmore@snap7" >"$extra_records_file"

	zxfer_capture_snapshot_dataset_lists "$snapshot_records_file" "$extra_records_file" \
		"$TEST_TMPDIR/recursive_snapshot_file.scratch" "$snapshot_records_file"

	assertEquals "Capture should extract, sort and deduplicate the dataset names." \
		"tank/src
tank/src/child
tank/src/space name" "$g_recursive_source_list"
	assertEquals "The inventory includes the full source, independently of the transfer list." \
		"$g_recursive_source_list" "$g_recursive_source_dataset_list"
	assertEquals "A dataset present on both sides stays in each independently sorted list." \
		"tank/src
tank/srcmore" "$g_recursive_destination_extra_dataset_list"
	assertEquals "Only one row per adjacent dataset in each role is staged for the sort." \
		8 "$(grep -c '^' "$TEST_TMPDIR/recursive_snapshot_file.scratch")"
}

test_recursive_dataset_list_filter_keeps_existing_exclusion_semantics() {
	# pattern|list|filtered list|status; lists are comma-separated. grep's
	# no-match status 1 empties the list, while its hard failure (2, an
	# invalid BRE) fails closed with no list.
	while IFS='|' read -r l_pattern l_list l_expected l_status; do
		g_option_x_exclude_datasets=$l_pattern
		zxfer_filter_recursive_dataset_list_with_excludes \
			"$(printf '%s' "$l_list" | tr ',' '\n')" 2>/dev/null
		assertEquals "The filter status [pattern:$l_pattern]." "$l_status" "$?"
		assertEquals "The filtered list [pattern:$l_pattern]." \
			"$(printf '%s' "$l_expected" | tr ',' '\n')" "$g_zxfer_recursive_dataset_list_result"
	done <<'EOF'
|tank/src,tank/src/child|tank/src,tank/src/child|0
/exclude$|tank/src,tank/src/exclude,tank/src/child,tank/src/child/exclude|tank/src,tank/src/child|0
^tank/src|tank/src||0
\(|tank/src||2
EOF
}

test_capture_snapshot_dataset_lists_preserves_awk_and_sort_failures() {
	set +e
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file_failure.txt"
	scratch_file="$TEST_TMPDIR/recursive_snapshot_file_failure.scratch"
	failing_awk="$TEST_TMPDIR/recursive_snapshot_file_awk_fails"
	printf '%s\n' "tank/src@snap1" >"$snapshot_records_file"
	cat >"$failing_awk" <<'EOF'
#!/bin/sh
exit 23
EOF
	chmod +x "$failing_awk"

	output=$(
		(
			g_recursive_source_list=stale
			g_recursive_destination_extra_dataset_list=stale
			g_recursive_source_dataset_list=stale
			g_cmd_awk=$failing_awk
			zxfer_capture_snapshot_dataset_lists "$snapshot_records_file" "$snapshot_records_file" "$scratch_file"
			printf 'awk=%s missing=<%s> extra=<%s> inventory=<%s>\n' "$?" \
				"$g_recursive_source_list" "$g_recursive_destination_extra_dataset_list" "$g_recursive_source_dataset_list"
		)
		(
			g_recursive_source_list=stale
			g_recursive_destination_extra_dataset_list=stale
			g_recursive_source_dataset_list=stale
			sort() {
				printf '%s\n' "partial"
				return 24
			}
			zxfer_capture_snapshot_dataset_lists "$snapshot_records_file" "$snapshot_records_file" "$scratch_file"
			printf 'sort=%s missing=<%s> extra=<%s> inventory=<%s>\n' "$?" \
				"$g_recursive_source_list" "$g_recursive_destination_extra_dataset_list" "$g_recursive_source_dataset_list"
		)
	)

	assertContains "Dataset-list capture should preserve the awk failure and clear stale results." \
		"$output" "awk=23 missing=<> extra=<> inventory=<>"
	assertContains "Dataset-list capture should preserve the sort failure and publish nothing." \
		"$output" "sort=24 missing=<> extra=<> inventory=<>"
}

test_capture_snapshot_dataset_lists_skips_empty_work_and_omits_unneeded_inventory() {
	empty_file="$TEST_TMPDIR/empty-dataset-work"
	: >"$empty_file"
	output=$(
		g_recursive_source_list=stale
		g_recursive_destination_extra_dataset_list=stale
		g_recursive_source_dataset_list=stale
		awk() {
			printf 'unexpected awk\n'
			return 27
		}
		g_cmd_awk='awk'
		sort() {
			printf 'unexpected sort\n'
			return 28
		}
		status=0
		zxfer_capture_snapshot_dataset_lists "$empty_file" "$empty_file" "$empty_file.scratch" || status=$?
		printf 'status=%s missing=<%s> extra=<%s> inventory=<%s>\n' "$status" \
			"$g_recursive_source_list" "$g_recursive_destination_extra_dataset_list" "$g_recursive_source_dataset_list"
	)
	assertEquals "A recursive no-op clears every list without spawning extraction tools." \
		"status=0 missing=<> extra=<> inventory=<>" "$output"
	printf 'tank/src@snap1\n' >"$empty_file"
	g_recursive_source_dataset_list=stale
	zxfer_capture_snapshot_dataset_lists "$empty_file" "$empty_file" "$empty_file.scratch"
	assertEquals "No optional source file means no retained source inventory." "" "$g_recursive_source_dataset_list"
}

test_recursive_delta_workspace_allocates_filter_files_only_for_exclusions() {
	source_tmp="$TEST_TMPDIR/sorted_source.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"

	for g_option_x_exclude_datasets in '' 'excluded$'; do
		before_files=${g_zxfer_profile_runtime_artifact_files_created:-0}
		zxfer_set_g_recursive_source_list "$source_tmp" "$source_tmp"
		expected_files=3
		[ -z "$g_option_x_exclude_datasets" ] || expected_files=5
		assertEquals "Delta workspace allocation with exclude [$g_option_x_exclude_datasets]." \
			"$expected_files" "$((g_zxfer_profile_runtime_artifact_files_created - before_files))"
	done
}

test_set_g_recursive_source_list_reports_destination_snapshot_exclude_filter_failures() {
	source_tmp="$TEST_TMPDIR/destination_snap_filter_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/destination_snap_filter_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	g_option_x_exclude_datasets='exclude$'

	set +e
	output=$(
		(
			snapshot_filter_call_count=0
			zxfer_filter_snapshot_file_with_excludes() {
				snapshot_filter_call_count=$((snapshot_filter_call_count + 1))
				if [ "$snapshot_filter_call_count" -eq 2 ]; then
					return 1
				fi
				cat "$1" >"$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when filtering the destination snapshot file fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the destination snapshot exclude-filter failure context." \
		"$output" "Failed to filter destination snapshots against exclude patterns for recursive delta planning."
}

test_set_g_recursive_source_list_reports_snapshot_compare_failures() {
	source_tmp="$TEST_TMPDIR/snapshot_compare_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/snapshot_compare_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	g_option_x_exclude_datasets=""

	set +e
	output=$(
		(
			cmp() {
				return 2
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when the snapshot list comparison itself fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the snapshot comparison failure context." \
		"$output" "Failed to compare source and destination snapshots for recursive delta planning."
}

# Every staging file discovery allocates is checked: a failed allocation stops
# discovery with its own status wherever it happens. zxfer_get_temp_file
# throws on its own failures, so only a stub reaches these ladders.
test_get_zfs_list_keeps_the_status_of_each_failed_temp_file_allocation() {
	# call:status for all five workspace files, five delta files with -x, two inventory
	# files and the source record cache. No failed sibling can be ignored.
	for l_temp_case in 1:9 2:11 3:12 4:13 5:23 6:24 7:25 8:26 9:27 10:28 11:29 12:37 13:38; do
		(
			g_option_x_exclude_datasets='excluded$'
			FAIL_AT=${l_temp_case%:*}
			FAIL_STATUS=${l_temp_case#*:}
			temp_calls=0
			# shellcheck disable=SC2120 # Workspaces call this fault stub with names.
			zxfer_get_temp_file() {
				temp_calls=$((temp_calls + 1))
				[ "$temp_calls" -ne "$FAIL_AT" ] || return "$FAIL_STATUS"
				# Stage cleanup removes only paths under the run root.
				if [ -n "${2:-}" ]; then
					g_zxfer_runtime_artifact_path_result="$g_zxfer_run_tmp_root/$1.$2"
				else
					g_zxfer_runtime_artifact_path_result="$g_zxfer_run_tmp_root/${1:-get_zfs_temp}.$temp_calls"
				fi
				: >"$g_zxfer_runtime_artifact_path_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				printf '%s\n' "tank/src@snapA" >"$3"
				: >"$2"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst"
			}
			zxfer_get_zfs_list >/dev/null 2>&1
		)
		assertEquals "Discovery should stop with the status of failed allocation ${l_temp_case%:*}." \
			"${l_temp_case#*:}" "$?"
	done
}

# Shared proof for the collapsed status-ladder forms used across src/ modules:
# 'cmd || return "$?"' and 'cmd || { l_status=$?; cleanup; return "$l_status"; }'
# must both return the failed command's original status. The capture must
# happen before the cleanup call because $? after cleanup reflects the cleanup
# command, not the failure being propagated.
test_collapsed_status_ladder_forms_preserve_original_failure_status() {
	output=$(
		(
			fail_with_status_27() {
				return 27
			}
			plain_collapse() {
				fail_with_status_27 || return "$?"
				echo "unreachable"
			}
			cleanup_collapse() {
				fail_with_status_27 || {
					l_status=$?
					: cleanup that succeeds and would clobber a bare \$?
					return "$l_status"
				}
				echo "unreachable"
			}
			set +e
			plain_collapse
			plain_status=$?
			cleanup_collapse
			cleanup_status=$?
			set -e
			printf 'plain=%s cleanup=%s\n' "$plain_status" "$cleanup_status"
		)
	)

	assertEquals "The 'cmd || return \"\$?\"' collapse must propagate the failed command's exact status." \
		"plain=27 cleanup=27" "$output"
}

# Both discovery paths use this validator. A successful wait needs no stderr
# read; a failed wait retains the source command and only its first ten lines.
test_source_snapshot_wait_validation_preserves_status_command_and_bounded_stderr() {
	# shellcheck disable=SC2119 # Use the default unnamed allocation.
	zxfer_get_temp_file
	source_error_file=$g_zxfer_runtime_artifact_path_result
	expected_error=""
	{
		for error_line in 1 2 3 4 5 6 7 8 9 10 11 12; do
			printf 'source error %s\n' "$error_line"
			[ "$error_line" -gt 10 ] ||
				expected_error=${expected_error:+$expected_error$ZXFER_LF}"source error $error_line"
		done
	} >"$source_error_file"
	for wait_status in 0 37; do
		output=$(
			g_source_snapshot_list_cmd="original source command"
			g_zxfer_snapshot_discovery_failure_result=""
			recorded_command=""
			zxfer_record_last_command_string() { recorded_command=$1; }
			capture_file=$source_error_file
			[ "$wait_status" -ne 0 ] || capture_file=$source_error_file.missing
			status=0
			zxfer_validate_source_snapshot_wait_status "$wait_status" "$capture_file" || status=$?
			printf 'status=%s command=<%s> diagnostic=<%s>\n' \
				"$status" "$recorded_command" "$g_zxfer_snapshot_discovery_failure_result"
		)
		if [ "$wait_status" -eq 0 ]; then
			expected='status=0 command=<> diagnostic=<>'
		else
			expected="status=37 command=<original source command> diagnostic=<Failed to retrieve snapshots from the source: $expected_error>"
		fi
		assertEquals "Source wait $wait_status keeps its status and bounded diagnostic." "$expected" "$output"
	done

	output=$(
		g_source_snapshot_list_cmd="original source command"
		recorded_command=""
		zxfer_record_last_command_string() { recorded_command=$1; }
		zxfer_read_runtime_artifact_file() { return 71; }
		status=0
		zxfer_validate_source_snapshot_wait_status 37 "$source_error_file" || status=$?
		printf 'status=%s command=<%s> diagnostic=<%s>\n' \
			"$status" "$recorded_command" "$g_zxfer_snapshot_discovery_failure_result"
	)
	assertEquals "A failed stderr read takes precedence while retaining the source command." \
		'status=71 command=<original source command> diagnostic=<Failed to read staged source snapshot stderr.>' "$output"
}

test_get_zfs_list_reports_source_stderr_readback_failures_after_background_failure() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing stderr capture" >"$2"
				sh -c 'exit 1' &
				g_source_snapshot_list_pid=$!
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_read_runtime_artifact_file() {
				l_read_count=$((l_read_count + 1))
				return 31
			}
			zxfer_throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source stderr readback failures should preserve the readback status." 31 "$status"
	assertContains "Background source stderr readback failures should still restore the source snapshot command context." \
		"$output" "cmd=sh -c 'exit 1'"
	assertContains "Background source stderr readback failures should report the staged stderr context." \
		"$output" "msg=Failed to read staged source snapshot stderr."
}

test_zxfer_limit_snapshot_discovery_capture_lines_keeps_the_first_lines() {
	l_capture=$(printf '%s\n' line1 line2 line3)
	assertEquals "A limit should keep that many lines." \
		"line1
line2" "$(zxfer_limit_snapshot_discovery_capture_lines "$l_capture" 2)"
	for l_limit in invalid 0 ""; do
		assertEquals "A limit that is not a positive number should fall back to ten lines [limit:$l_limit]." \
			"$l_capture" "$(zxfer_limit_snapshot_discovery_capture_lines "$l_capture" "$l_limit")"
	done
	assertEquals "The default limit should be ten lines." \
		"$(printf 'l%s\n' 1 2 3 4 5 6 7 8 9 10)" \
		"$(zxfer_limit_snapshot_discovery_capture_lines "$(printf 'l%s\n' 1 2 3 4 5 6 7 8 9 10 11 12)")"
}

# Run zxfer_ensure_parallel_available_for_source_jobs with -j JOBS, -O ORIGIN,
# the local parallel LOCAL and the origin helper CACHED cached for
# CACHED_HOST, over a stale reason; the stubbed remote lookup answers RESULT
# with STATUS. Prints the status, the reason, the cached origin helper and
# host, and each lookup made.
# Usage: zxfer_test_ensure_parallel_outcome JOBS ORIGIN LOCAL CACHED CACHED_HOST RESULT STATUS
zxfer_test_ensure_parallel_outcome() {
	(
		g_option_j_jobs=$1
		g_option_O_origin_host=$2
		g_cmd_parallel=$3
		g_origin_parallel_cmd=$4
		g_origin_parallel_cmd_host=$5
		LOOKUP_RESULT=$6
		LOOKUP_STATUS=$7
		g_zxfer_parallel_source_job_check_result="stale reason"
		l_test_lookups=""
		zxfer_resolve_remote_required_tool() {
			l_test_lookups="$l_test_lookups $1:$2"
			g_zxfer_required_tool_result=$LOOKUP_RESULT
			return "$LOOKUP_STATUS"
		}
		zxfer_ensure_parallel_available_for_source_jobs
		printf 'status=%s reason=<%s> cmd=<%s@%s> lookups=<%s>\n' "$?" \
			"$g_zxfer_parallel_source_job_check_result" "$g_origin_parallel_cmd" \
			"$g_origin_parallel_cmd_host" "${l_test_lookups# }"
	)
}

test_ensure_parallel_available_for_source_jobs_decides_by_jobs_host_and_cache() {
	assertEquals "-j 1 needs no parallel anywhere." \
		"status=0 reason=<> cmd=<@> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 1 "" "" "" "" "" 0)"
	assertEquals "A local -j run trusts the local parallel without a version probe." \
		"status=0 reason=<> cmd=<@> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 2 "" "$PARALLEL_BIN" "" "" "" 0)"
	assertEquals "A local -j run without parallel fails with the local-host reason." \
		"status=1 reason=<The -j option requires parallel but it was not found in PATH on the local host.> cmd=<@> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 2 "" "" "" "" "" 0)"
	assertEquals "An -O run resolves the origin's parallel, trusts it and caches it for that host." \
		"status=0 reason=<> cmd=</opt/bin/parallel@origin.example> lookups=<origin.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" "" "" /opt/bin/parallel 0)"
	assertEquals "A helper cached for the same origin is reused without a lookup." \
		"status=0 reason=<> cmd=</opt/bin/parallel@origin.example> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" /opt/bin/parallel origin.example /usr/bin/parallel 0)"
	assertEquals "A helper cached for another origin is resolved again." \
		"status=0 reason=<> cmd=</usr/local/bin/parallel@origin-b.example> lookups=<origin-b.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin-b.example "" /opt/bin/parallel origin-a.example /usr/local/bin/parallel 0)"
	assertEquals "A parallel the origin lacks becomes the -j guidance." \
		"status=1 reason=<parallel not found on origin host origin.example but -j 2 was requested. Install parallel remotely or rerun without -j.> cmd=<@> lookups=<origin.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" "" "" 'Required dependency "parallel" not found on host origin.example in secure PATH (/usr/sbin).' 1)"
	assertEquals "Any other lookup failure keeps its message and status." \
		"status=3 reason=<Failed to query dependency \"parallel\" on host origin.example.> cmd=<@> lookups=<origin.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" "" "" 'Failed to query dependency "parallel" on host origin.example.' 3)"
}

# A -j listing never falls back to the serial one: when the parallel check
# fails, the builder returns the check's status and publishes its reason, or
# a generic one, instead of a command.
test_build_source_snapshot_list_cmd_fails_closed_when_the_parallel_check_fails() {
	g_option_j_jobs=2
	for l_check_case in "68|parallel check failed" "69|"; do
		l_check_reason=${l_check_case#*|}
		output=$(
			CHECK_STATUS=${l_check_case%%|*}
			CHECK_REASON=$l_check_reason
			zxfer_ensure_parallel_available_for_source_jobs() {
				# Helpers share one variable namespace; clobbering the
				# builder's scratch name must not change its result.
				l_list_status=0
				g_zxfer_parallel_source_job_check_result=$CHECK_REASON
				return "$CHECK_STATUS"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'status=%s\n' "$?"
		)
		assertEquals "A failed check should publish its reason, or the generic one, and keep its status [check:$l_check_case]." \
			"${l_check_reason:-Failed to prepare parallel source discovery.}
status=${l_check_case%%|*}" "$output"
	done
	g_cmd_parallel=""
	assertEquals "The real check should fail a local -j run without parallel, with no serial fallback." \
		"The -j option requires parallel but it was not found in PATH on the local host.
status=1" "$(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'status=%s\n' "$?"
		)"
}

# A builder whose rendering fails returns that status and publishes no
# command: the serial listing's role render, the origin's ssh wrapper, and an
# -O -z listing whose origin compressor is unresolved, for the proof's name
# listing and the -j listing alike.
test_source_listing_builders_fail_closed_when_rendering_fails() {
	output=$(
		(
			zxfer_render_zfs_command_for_role() {
				g_zxfer_shell_command_result="partial serial command"
				return 67
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'serial render=%s\n' "$?"
		)
		g_option_j_jobs=2
		g_option_O_origin_host="origin.example"
		g_origin_cmd_zfs="/remote/bin/zfs"
		g_origin_parallel_cmd="/opt/bin/parallel"
		g_origin_parallel_cmd_host="origin.example"
		for l_builder in zxfer_build_source_snapshot_name_list_cmd \
			zxfer_build_source_snapshot_list_cmd; do
			(
				zxfer_ssh_shell_command_for_host() {
					g_zxfer_shell_command_result="partial SSH command"
					return 34
				}
				zxfer_test_print_source_listing "$l_builder"
				printf '%s ssh=%s\n' "$l_builder" "$?"
			)
			(
				g_option_z_compress=1
				g_origin_cmd_compress_safe=""
				zxfer_test_print_source_listing "$l_builder"
				printf '%s compressor=%s\n' "$l_builder" "$?"
			)
		done
	)

	assertEquals "Each failed render should keep its status and publish only a reason, never a partial command." \
		"serial render=67
zxfer_build_source_snapshot_name_list_cmd ssh=34
The origin host compression command is not resolved.
zxfer_build_source_snapshot_name_list_cmd compressor=1
zxfer_build_source_snapshot_list_cmd ssh=34
The origin host compression command is not resolved.
zxfer_build_source_snapshot_list_cmd compressor=1" "$output"
}

# The origin renderer must retain an input supplied through its own output
# channel before clearing it or rendering the remote wrapper.
test_origin_listing_renderer_retains_input_from_the_canonical_result() {
	output=$(
		g_option_O_origin_host="origin.example"
		g_zxfer_shell_command_result="printf '%s\\n' 'input retained'"
		zxfer_ssh_shell_command_for_host() {
			zxfer_execute_rendered_shell_command "$3" || return
			g_zxfer_shell_command_result="wrapped command"
		}
		zxfer_render_origin_listing_command "$g_zxfer_shell_command_result" 0
		printf 'result=%s\n' "$g_zxfer_shell_command_result"
	)
	assertEquals "Remote preparation must receive the original pipeline before replacing the shared result." \
		"input retained
result=wrapped command" "$output"
}

# Both launchers keep the launched command for a later producer failure even
# after producer preparation renders another command through the shared result.
test_source_listing_launchers_retain_commands_across_producer_rendering() {
	for listing_mode in fast full; do
		output=$(
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_shell_command_result="printf snapshot"
			}
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_shell_command_result="printf snapshot"
			}
			zxfer_start_source_snapshot_producer() {
				g_last_background_pid=""
				zxfer_render_shell_command_from_argv ignored scratch
				zxfer_execute_rendered_shell_command "$2"
			}
			if [ "$listing_mode" = fast ]; then
				g_option_R_recursive=tank/src
				zxfer_start_fast_noop_destination_producer() { return 1; }
				zxfer_try_fast_recursive_noop_discovery || :
			else
				zxfer_write_source_snapshot_list_to_file out err sorted
			fi
			printf '\nretained=%s\n' "$g_source_snapshot_list_cmd"
		)
		assertEquals "$listing_mode keeps its original command across later rendering." \
			"snapshot
retained=printf snapshot" "$output"
	done
}

# A -j no-op proof is still one recursive listing; only the full producer
# fans out. Pin both counters with and without -V through the real builders.
test_source_listing_launchers_account_parallel_by_listing_mode() {
	for listing_mode in fast full; do
		for listing_jobs in 1 4; do
			for listing_verbose in 0 1; do
				output=$(
					g_option_j_jobs=$listing_jobs
					g_option_V_very_verbose=$listing_verbose
					zxfer_start_source_snapshot_producer() {
						g_last_background_pid=""
						return 0
					}
					if [ "$listing_mode" = fast ]; then
						g_option_R_recursive=tank/src
						zxfer_start_fast_noop_destination_producer() { return 1; }
						zxfer_try_fast_recursive_noop_discovery || :
					else
						zxfer_write_source_snapshot_list_to_file out err sorted
					fi >/dev/null
					printf '%s %s\n' "$g_zxfer_profile_source_snapshot_list_commands" \
						"$g_zxfer_profile_source_snapshot_list_parallel_commands"
				)
				parallel_count=0
				if [ "$listing_mode:$listing_jobs" = full:4 ]; then
					parallel_count=1
				fi
				assertEquals "$listing_mode -j $listing_jobs -V $listing_verbose accounts only its selected producer." \
					"1 $parallel_count" "$output"
			done
		done
	done
}

# A failed build throws the builder's message, or a generic one with the
# build's status when the builder published none.
test_write_source_snapshot_list_to_file_throws_the_builder_message_or_a_generic_one() {
	outfile="$TEST_TMPDIR/source_build_failure.out"
	for l_build_case in "builder failed|1" "|7"; do
		output=$(
			(
				BUILD_MESSAGE=${l_build_case%|*}
				BUILD_STATUS=${l_build_case#*|}
				zxfer_build_source_snapshot_list_cmd() {
					g_zxfer_shell_command_result=$BUILD_MESSAGE
					return "$BUILD_STATUS"
				}
				zxfer_throw_error() {
					printf '<%s:%s>' "$1" "$2"
					exit 1
				}
				zxfer_write_source_snapshot_list_to_file "$outfile" "$outfile.err" "$outfile.sorted"
			)
		)
		l_build_message=${l_build_case%|*}
		assertEquals "A failed build should throw its message and status [build:$l_build_case]." \
			"<${l_build_message:-Failed to build source snapshot discovery command.}:${l_build_case#*|}>" \
			"$output"
	done
}

# A failed launch keeps its status; all output paths stay with the operation
# owner, including when a pipeline status file cannot be allocated.
test_write_source_snapshot_list_to_file_keeps_the_status_of_a_failed_launch() {
	outfile="$TEST_TMPDIR/source_launch_failure.out"
	errfile="$TEST_TMPDIR/source_launch_failure.err"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' snap"
			}
			zxfer_start_source_snapshot_producer() {
				return 32
			}
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile" "$outfile.sorted"
			printf 'launch=%s\n' "$?"
		)
		(
			temp_calls=0
			zxfer_get_temp_file() {
				temp_calls=$((temp_calls + 1))
				[ "$temp_calls" -eq 1 ] || return 33
				g_zxfer_runtime_artifact_path_result="$TEST_TMPDIR/source_launch_failure.sorted"
				: >"$g_zxfer_runtime_artifact_path_result"
			}
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_shell_command_result="printf '%s\n' snap"
			}
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile" "$outfile.sorted"
			printf 'status file=%s calls=%s\n' "$?" "$temp_calls"
		)
	)

	assertEquals "A failed launch should keep its status with caller-owned output paths." \
		"launch=32
status file=33 calls=2" "$output"
}

# A producer that cannot allocate its status files never starts and keeps
# the allocation's status: the full listing's group, and the proof listing's
# listing and optional exclusion-filter status files.
test_source_snapshot_producers_keep_the_status_of_a_failed_setup() {
	sorted_file="$TEST_TMPDIR/producer_setup.sorted"

	output=$(
		(
			zxfer_create_runtime_artifact_workspace() {
				return 14
			}
			zxfer_start_source_snapshot_producer full "printf x" \
				"$sorted_file" "" "$TEST_TMPDIR/producer_setup.out"
			printf 'records=%s\n' "$?"
		)
		for l_setup_case in 1:23 2:57; do
			(
				FAIL_AT=${l_setup_case%:*}
				FAIL_STATUS=${l_setup_case#*:}
				temp_calls=0
				zxfer_get_temp_file() {
					temp_calls=$((temp_calls + 1))
					[ "$temp_calls" -ne "$FAIL_AT" ] || return "$FAIL_STATUS"
					g_zxfer_runtime_artifact_path_result="$TEST_TMPDIR/producer_setup.$temp_calls"
					: >"$g_zxfer_runtime_artifact_path_result"
				}
				g_option_x_exclude_datasets=skip
				zxfer_start_source_snapshot_producer fast "printf x" "$sorted_file"
				printf 'names %s=%s\n' "$FAIL_AT" "$?"
			)
		done
	)

	assertEquals "Each failed status-file allocation should keep its status before any launch." \
		"records=14
names 1=23
names 2=57" "$output"
}

test_source_snapshot_producers_preserve_spawn_failure_without_registering_stale_pid() {
	for producer in names records; do
		status=0
		output=$(
			g_last_background_pid=12345
			zxfer_spawn_background_shell() { return 79; }
			zxfer_register_cleanup_pid() { printf 'registered stale PID'; }
			if [ "$producer" = names ]; then
				zxfer_start_source_snapshot_producer fast \
					'printf x' "$TEST_TMPDIR/failed-spawn.sorted"
			else
				zxfer_start_source_snapshot_producer full \
					'printf x' "$TEST_TMPDIR/failed-spawn.sorted" "" \
					"$TEST_TMPDIR/failed-spawn.out"
			fi
		) || status=$?

		assertEquals "$producer discovery must preserve the spawn failure." 79 "$status"
		assertEquals "$producer discovery must not register a previous helper PID after a failed spawn." \
			"" "$output"
	done
}

# Exercise the rendered pipelines themselves: full discovery keeps creation
# order, the proof filters before sorting, and the first failed stage wins
# even if sort also fails. Paths containing shell syntax stay literal.
test_source_snapshot_producers_preserve_stage_order_and_failure_precedence() {
	producer_path="$TEST_TMPDIR/producer ' \$(touch forbidden)"
	filter_wrapper="$TEST_TMPDIR/producer-filter"
	real_awk=$(command -v awk)
	# shellcheck disable=SC2016 # The fixture script expands its own arguments.
	printf '#!/bin/sh\n"%s" "$@" || exit $?\nexit "$FILTER_STATUS"\n' \
		"$real_awk" >"$filter_wrapper"
	chmod +x "$filter_wrapper"
	# Install overrides before entering a subshell: ksh93 retains the original
	# binding when a subshell alone replaces an already defined function.
	zxfer_spawn_background_shell() { RENDERED_PRODUCER=$1; }
	zxfer_register_background_producer() { return 0; }
	tee() {
		command tee "$@"
		return "$FILTER_STATUS"
	}
	sort() {
		command sort "$@"
		return "$SORT_STATUS"
	}
	for producer_case in full:0:0:0:0 full:37:38:39:37 full:0:38:39:38 full:0:0:39:39 \
		fast:0:0:0:0 fast:37:38:39:37 fast:0:38:39:38 fast:0:0:39:39 empty:0:0:0:0; do
		(
			IFS=: read -r MODE LIST_STATUS FILTER_STATUS SORT_STATUS EXPECTED <<EOF
$producer_case
EOF
			export FILTER_STATUS
			g_cmd_awk=$filter_wrapper
			g_option_x_exclude_datasets=skip
			producer_cmd="printf '%s\n' 'tank/src@z${ZXFER_TAB}2' 'tank/skip@a${ZXFER_TAB}1' 'tank/src@a${ZXFER_TAB}0'; exit $LIST_STATUS"
			[ "$MODE" != empty ] || producer_cmd=:
			if [ "$MODE" = full ]; then
				zxfer_start_source_snapshot_producer full \
					"$producer_cmd" "$producer_path.sorted" "" "$producer_path.raw"
			else
				zxfer_start_source_snapshot_producer fast \
					"$producer_cmd" "$producer_path.sorted"
			fi
			(eval "$RENDERED_PRODUCER")
			printf 'status=%s\n' "$?"
			command cat "$producer_path.sorted"
			if [ "$MODE" = full ]; then
				printf 'raw:\n'
				command cat "$producer_path.raw"
			fi
		) >"$TEST_TMPDIR/producer-result"
		output=$(command cat "$TEST_TMPDIR/producer-result")
		assertEquals "The first failing stage wins [$producer_case]." \
			"status=${producer_case##*:}" "${output%%"$ZXFER_LF"*}"
		case $producer_case in
		full:*)
			assertContains "Full discovery keeps the excluded dataset and raw creation order." \
				"$output" "raw:
tank/src@z	2
tank/skip@a	1
tank/src@a	0"
			;;
		fast:*)
			assertContains "The proof byte-sorts the retained records." "$output" "tank/src@a	0
tank/src@z	2"
			assertNotContains "The proof filters before sort." "$output" tank/skip
			;;
		empty:*) assertEquals "An empty stream leaves the sorted file empty." "status=0" "$output" ;;
		esac
	done
	unset -f sort tee
	zxfer_source_modules_for_tests "$ZXFER_ROOT"
}

# A producer whose cleanup registration fails is stopped at once: TERM, then
# KILL to its whole owned scope, and a failed KILL keeps that scope
# registered for the trap's ordered retry. Both producers share this path; the
# proof's producer also runs it for real, in the probed spawn mode and the
# wrapper, with a descendant that ignores TERM.
test_source_snapshot_producers_stop_an_unregistered_producer_and_its_descendants() {
	child_file="$TEST_TMPDIR/source_register.child"
	sorted_file="$TEST_TMPDIR/source_register.sorted"
	outfile="$TEST_TMPDIR/source_register.out"
	errfile="$TEST_TMPDIR/source_register.err"
	for producer_mode in names records; do
		output=$(
			g_zxfer_cleanup_pid_abort_grace_seconds=0
			zxfer_spawn_background_shell() {
				g_last_background_pid=12345
				g_zxfer_background_shell_scope=pgid
			}
			zxfer_register_cleanup_pid() { return 1; }
			zxfer_signal_background_shell() {
				printf 'signal=%s:%s:%s\n' "$1" "$2" "$3"
				[ "$3" = TERM ]
			}
			if [ "$producer_mode" = names ]; then
				zxfer_start_source_snapshot_producer fast \
					'printf x' "$sorted_file" "$errfile"
			else
				zxfer_start_source_snapshot_producer full \
					'printf x' "$sorted_file" "$errfile" "$outfile"
			fi
			printf 'status=%s pid=%s\n' "$?" "$g_last_background_pid"
			zxfer_find_cleanup_pid_record 12345
			printf 'retained=%s scope=%s\n' "$?" "$g_zxfer_cleanup_pid_record_scope"
		)
		assertContains "$producer_mode preserves the registration failure and published PID on failed KILL." \
			"$output" "status=1 pid=12345"
		assertContains "$producer_mode escalates the entire owned scope after TERM." \
			"$output" "signal=12345:pgid:KILL"
		assertContains "$producer_mode retains the failed scope for ordered trap cleanup." \
			"$output" "retained=0 scope=pgid"
	done

	# The producer's shell exits on TERM; this bounded descendant ignores it.
	# Both the probed spawn mode and the cleanup wrapper must stop it.
	# shellcheck disable=SC2016 # The fixture child expands its own PID/path.
	zxfer_render_shell_command_from_argv sh -c \
		'trap "" TERM; printf "%s\n" "$$" >"$1"; exec sleep 30' \
		zxfer-test "$child_file"
	producer_cmd=$g_zxfer_shell_command_result
	zxfer_init_background_shell_spawn_mode
	spawn_modes=$g_zxfer_background_shell_spawn_mode
	[ "$spawn_modes" = wrapper ] || spawn_modes="$spawn_modes wrapper"
	for spawn_mode in $spawn_modes; do
		rm -f "$child_file"
		output=$(
			g_zxfer_background_shell_spawn_mode=$spawn_mode
			g_zxfer_cleanup_pid_abort_grace_seconds=0
			zxfer_register_cleanup_pid() {
				tries=0
				while [ ! -s "$child_file" ] && [ "$tries" -lt 30 ]; do
					sleep 0.1 2>/dev/null || sleep 1
					tries=$((tries + 1))
				done
				return 1
			}
			zxfer_start_source_snapshot_producer fast \
				"$producer_cmd" "$sorted_file" "$errfile"
			printf 'status=%s pid=<%s> records=<%s>\n' \
				"$?" "$g_last_background_pid" "$g_zxfer_cleanup_pid_records"
		)
		assertContains "$spawn_mode completes failed-registration cleanup and clears ownership." \
			"$output" 'status=1 pid=<> records=<>'
		assertTrue "$spawn_mode starts the TERM-ignoring descendant before cleanup." \
			"[ -s '$child_file' ]"
		child_pid=$(cat "$child_file" 2>/dev/null)
		[ -n "$child_pid" ] || continue
		tries=0
		while kill -s 0 "$child_pid" 2>/dev/null && [ "$tries" -lt 20 ]; do
			sleep 0.1 2>/dev/null || sleep 1
			tries=$((tries + 1))
		done
		if kill -s 0 "$child_pid" 2>/dev/null; then
			command kill -s KILL "$child_pid" 2>/dev/null || :
			fail "$spawn_mode forgot a TERM-ignoring descendant after registration failed."
		fi
	done
}

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

	capture_case() {
		zxfer_test_stub_destination_listing
		DEST_LIST_STATUS=1
		DEST_PROBE_OUTPUT='dataset does not exist'
		DEST_PROBE_STATUS=1
		zxfer_write_runtime_artifact_file() {
			return 42
		}
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	}
	zxfer_test_capture_subshell capture_case

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
			test_output=$g_zxfer_runtime_artifact_path_result
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
			zxfer_start_fast_noop_destination_producer \
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
				zxfer_normalize_destination_snapshot_list "$l_destination" \
					"$input_file" "$output_file" "$l_exclude"
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

create_passthrough_zstd() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	--) shift
		break
		;;
	-*) shift
		;;
	*) break
		;;
	esac
done
cat
EOF
	chmod +x "$l_path"
}

test_build_source_snapshot_list_cmd_remote_helper_path_does_not_execute_locally() {
	zxfer_source_modules_for_tests "$ZXFER_ROOT"
	zxfer_test_exec_fixture_setup
	marker="$TEST_TMPDIR/remote_helper_marker"
	outfile="$TEST_TMPDIR/remote_helper.out"
	errfile="$TEST_TMPDIR/remote_helper.err"
	remote_log="$TEST_TMPDIR/remote_helper.log"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs="/bin/echo; touch $marker #"
	g_option_O_origin_host="backup@example.com"
	g_option_j_jobs=1
	g_initial_source="tank/src"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	l_cmd=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd)
	zxfer_start_source_snapshot_producer full "$l_cmd" \
		"$TEST_TMPDIR/remote_helper.sorted" "$errfile" "$outfile"
	wait "$g_last_background_pid"
	zxfer_unregister_cleanup_pid "$g_last_background_pid"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertFalse "Resolved remote helper paths should not execute locally when snapshot listing is eval'd." \
		"[ -e '$marker' ]"
	assertEquals "ssh should force batch mode before the remote helper host token." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the remote helper host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before the remote helper host token." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the remote helper host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should still target the requested host." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "The malicious helper path should be quoted as one remote-shell token." \
		"$log_line_remote_cmd" "'/bin/echo; touch $marker #'"
}

test_remote_snapshot_listing_pipeline_handles_csh_remote_shell() {
	zxfer_source_modules_for_tests "$ZXFER_ROOT"
	zxfer_test_exec_fixture_setup
	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_join_csh_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_join_csh_exec.log"
	fake_remote_zfs="$TEST_TMPDIR/fake_remote_zfs"
	fake_zstd="$TEST_TMPDIR/zstd"
	l_csh_shell=$(find_csh_shell_for_tests)

	if [ "$l_csh_shell" = "" ]; then
		return 0
	fi

	create_fake_ssh_join_csh_exec_bin "$realistic_ssh_bin" "$l_csh_shell"
	create_passthrough_zstd "$fake_zstd"
	cat >"$fake_remote_zfs" <<'EOF'
#!/bin/sh
if [ "$1" = "list" ] && [ "$2" = "-Hr" ] && [ "$3" = "-t" ] && [ "$4" = "filesystem,volume" ] &&
	[ "$5" = "-o" ] && [ "$6" = "name" ] && [ "$7" = "zroot" ]; then
	printf '%s\n' "zroot"
	exit 0
fi
if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name,guid" ] &&
	[ "$5" = "-s" ] && [ "$6" = "creation" ] && [ "$7" = "-d" ] && [ "$8" = "1" ] &&
	[ "$9" = "-t" ] && [ "${10}" = "snapshot" ] && [ "${11}" = "zroot" ]; then
	printf '%s\t%s\n' "zroot@snap1" "guid-1"
	exit 0
fi
exit 0
EOF
	chmod +x "$fake_remote_zfs"

	g_option_j_jobs=4
	g_option_z_compress=1
	g_cmd_compress="zstd -9"
	g_cmd_parallel="$FAKE_PARALLEL_BIN"
	g_origin_parallel_cmd="$FAKE_PARALLEL_BIN"
	g_cmd_zfs="$fake_remote_zfs"
	g_origin_cmd_zfs="$fake_remote_zfs"
	g_cmd_decompress_safe="'$fake_zstd' '-d'"
	g_origin_cmd_compress_safe="'$fake_zstd' '-9'"
	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="aldo@172.16.0.4"
	g_initial_source="zroot"

	old_path=$PATH
	PATH="$(dirname "$fake_zstd"):$PATH"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	l_cmd=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="$FAKE_PARALLEL_BIN"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	eval "$l_cmd" >"$TEST_TMPDIR/remote_snapshot_csh.out" 2>"$TEST_TMPDIR/remote_snapshot_csh.err"
	status=$?

	unset FAKE_SSH_LOG
	PATH=$old_path

	assertEquals "Remote snapshot listing should succeed even when ssh routes through csh on the origin host." 0 "$status"
	assertNotContains "The csh-backed ssh emulation should not report unmatched-quote syntax errors." \
		"$(cat "$TEST_TMPDIR/remote_snapshot_csh.err")" "Unmatched"
	assertContains "The csh-backed ssh emulation should receive a remote sh -c wrapper." \
		"$(cat "$realistic_ssh_log")" "'sh' '-c'"
}

test_report_recursive_snapshot_delta_emits_source_and_destination_under_ksh_source_alias() {
	l_missing_file="$TEST_TMPDIR/report_delta.missing"
	l_extra_file="$TEST_TMPDIR/report_delta.extra"
	printf 'tank/src@snap2\tguid-s2\ntank/src/child@snap2\tguid-c2\n' >"$l_missing_file"
	printf 'tank/src@extra1\tguid-e1\n' >"$l_extra_file"

	l_out=$(
		g_option_v_verbose=1
		g_option_V_very_verbose=0
		g_option_R_recursive=""
		g_option_d_delete_destination_snapshots=0
		g_initial_source="tank/src"
		g_destination="backup/dst"
		g_initial_source_had_trailing_slash=0
		g_recursive_source_list="tank/src${ZXFER_LF}tank/src/child"
		g_recursive_destination_extra_dataset_list="tank/src"
		zxfer_report_recursive_snapshot_delta "$l_missing_file" "$l_extra_file"
	)
	assertContains "Delta report must include the missing-on-destination header." \
		"$l_out" "Recursive source snapshots absent from destination (name/GUID):"
	assertContains "Delta report must include the root missing snapshot." \
		"$l_out" "  tank/src@snap2 (GUID: guid-s2)"
	assertContains "Delta report must explain nonrecursive descendant exclusion." \
		"$l_out" "    Not scheduled: nonrecursive mode (-N) selects only [tank/src]; this descendant is outside the dataset queue."
	assertContains "Delta report must include the absent-from-source header." \
		"$l_out" "Recursive destination snapshots absent from source (name/GUID):"
	assertContains "Delta report must map destination snapshot names." \
		"$l_out" "  backup/dst/src@extra1 (GUID: guid-e1)"

	l_ksh_bin=$(command -v ksh 2>/dev/null || :)
	if [ -n "$l_ksh_bin" ]; then
		l_ksh_out=$("$l_ksh_bin" -c '
			ZXFER_SOURCE_MODULES_ROOT=$1
			. "$1/src/zxfer_quoting.sh"
			. "$1/src/zxfer_reporting.sh"
			. "$1/src/zxfer_destination_state.sh"
			# Source zxfer_snapshot_discovery.sh directly with ksh93 default aliases
			# still active to verify the quoted for-loop words resist source="command .".
			. "$1/src/zxfer_snapshot_discovery.sh"
			g_option_v_verbose=1
			g_option_V_very_verbose=0
			g_option_R_recursive=""
			g_option_d_delete_destination_snapshots=0
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_initial_source_had_trailing_slash=0
			g_recursive_source_list="tank/src
tank/src/child"
			g_recursive_destination_extra_dataset_list=""
			zxfer_report_recursive_snapshot_delta "$2" "$3"
		' ksh "$ZXFER_ROOT" "$l_missing_file" "$l_extra_file")
		assertEquals "Direct ksh93 execution of delta report must succeed." 0 "$?"
		assertContains "Direct ksh93 parse must not alias-expand source in the delta side loop." \
			"$l_ksh_out" "Recursive source snapshots absent from destination (name/GUID):"
	fi
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
