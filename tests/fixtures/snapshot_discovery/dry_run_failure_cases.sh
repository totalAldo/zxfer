#!/bin/sh
# shellcheck shell=sh
# Current-shell seam and injected failure-propagation cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_zxfer_read_snapshot_discovery_capture_file_reads_multiline_results_in_current_shell() {
	capture_file="$TEST_TMPDIR/snapshot_discovery_capture.txt"
	expected_capture='first line
second line
'
	cat >"$capture_file" <<'EOF'
first line
second line
EOF

	zxfer_read_snapshot_discovery_capture_file "$capture_file"

	# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
	assertEquals "Snapshot-discovery capture-file reads should preserve multiline staged command content in current-shell scratch." \
		"$expected_capture" "$g_zxfer_snapshot_discovery_file_read_result"
}

test_zxfer_read_snapshot_discovery_capture_file_fails_closed_on_redirection_errors_in_current_shell() {
	capture_dir="$TEST_TMPDIR/snapshot_discovery_capture_dir"
	mkdir -p "$capture_dir"
	g_zxfer_snapshot_discovery_file_read_result="stale-capture"

	set +e
	zxfer_read_snapshot_discovery_capture_file "$capture_dir" 2>/dev/null
	status=$?
	set -e

	assertNotEquals "Snapshot-discovery capture-file reads should fail when the staged capture path cannot be opened for reading." \
		0 "$status"
	assertEquals "Snapshot-discovery capture-file reads should not publish stale or partial scratch on redirection failure." \
		"" "$g_zxfer_snapshot_discovery_file_read_result"
}

test_ensure_parallel_available_for_source_jobs_clears_a_stale_reason() {
	output=$(
		(
			g_zxfer_parallel_source_job_check_result="stale-parallel-check"
			g_option_j_jobs=2
			g_option_O_origin_host=""
			g_cmd_parallel="$PARALLEL_BIN"
			set +e
			zxfer_ensure_parallel_available_for_source_jobs
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
			printf 'result=<%s>\n' "${g_zxfer_parallel_source_job_check_result:-}"
		)
	)

	assertEquals "A passing parallel check should clear a stale reason and print nothing." \
		"status=0
result=<>" "$output"
}

test_build_source_snapshot_list_cmd_publishes_the_parallel_check_reason() {
	output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				# Helpers share one variable namespace; clobbering the
				# builder's scratch name must not change its result.
				l_list_status=0
				g_zxfer_parallel_source_job_check_result="nested remote validation failed"
				return 1
			}
			g_option_j_jobs=2
			set +e
			zxfer_build_source_snapshot_list_cmd
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'result=<%s>\n' "$g_zxfer_source_snapshot_list_cmd_result"
		)
	)

	assertEquals "The source listing builder should return the parallel check's status and publish its reason." \
		"status=1
result=<nested remote validation failed>" "$output"
}

test_build_source_snapshot_list_cmd_allocates_no_scratch_when_the_parallel_check_fails() {
	output=$(
		(
			tempfile_log="$TEST_TMPDIR/parallel-check-tempfile.log"
			cleanup_log="$TEST_TMPDIR/parallel-check-cleanup.log"
			zxfer_get_temp_file() {
				printf '%s\n' "called" >"$tempfile_log"
				return 1
			}
			zxfer_ensure_parallel_available_for_source_jobs() {
				return 27
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf '%s\n' "$1" >"$cleanup_log"
				return 0
			}
			g_option_j_jobs=2
			set +e
			zxfer_build_source_snapshot_list_cmd
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'result=<%s>\n' "$g_zxfer_source_snapshot_list_cmd_result"
			printf 'tempfile_called=<%s>\n' "$(cat "$tempfile_log" 2>/dev/null)"
			printf 'cleanup=<%s>\n' "$(cat "$cleanup_log" 2>/dev/null)"
		)
	)

	assertContains "A silent parallel check failure should keep its status." \
		"$output" "status=27"
	assertContains "A silent parallel check failure should publish the generic message." \
		"$output" "result=<Failed to prepare parallel source discovery.>"
	assertContains "A failed parallel check should allocate no scratch file." \
		"$output" "tempfile_called=<>"
	assertContains "A failed parallel check should clean up nothing." \
		"$output" "cleanup=<>"
}

test_build_source_snapshot_list_cmd_preserves_remote_parallel_resolution_from_current_shell() {
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result="sh -c $1"
				printf '%s\n' "sh -c $1"
			}
			zxfer_ssh_shell_command_for_host() {
				g_zxfer_shell_command_result="ssh $2 $3"
			}
			zxfer_ensure_parallel_available_for_source_jobs() {
				g_origin_parallel_cmd="/opt/bin/parallel"
				return 0
			}
			g_option_j_jobs=4
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_initial_source="tank/src"
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'resolved=%s\n' "$g_origin_parallel_cmd"
		)
	)

	assertContains "Remote source snapshot planning should retain the helper path resolved during the current-shell availability check." \
		"$output" "'/opt/bin/parallel' -j 4 --line-buffer"
	assertContains "Remote source snapshot planning should preserve the direct remote dataset enumeration command." \
		"$output" "'/remote/bin/zfs' 'list' '-Hr' '-t' 'filesystem,volume' '-o' 'name' 'tank/src'"
	assertContains "Remote source snapshot planning should preserve the resolved origin-host parallel helper after command rendering." \
		"$output" "resolved=/opt/bin/parallel"
}

test_capture_recursive_dataset_list_from_snapshot_file_extracts_sorted_unique_datasets() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file.txt"
	cat >"$snapshot_records_file" <<'EOF'
tank/src/child@snap2
tank/src@snap1
tank/src/child@snap3
EOF

	zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file" \
		"$TEST_TMPDIR/recursive_snapshot_file.scratch"

	# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
	assertEquals "Recursive dataset-list capture from snapshot files should extract, sort, and deduplicate dataset names." \
		"tank/src
tank/src/child" "$g_zxfer_recursive_dataset_list_result"
}

test_capture_recursive_dataset_list_from_snapshot_file_preserves_awk_and_sort_failures() {
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
			g_zxfer_recursive_dataset_list_result="stale-datasets"
			g_cmd_awk=$failing_awk
			zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file" "$scratch_file"
			printf 'awk=%s result=<%s>\n' "$?" "$g_zxfer_recursive_dataset_list_result"
		)
		(
			g_zxfer_recursive_dataset_list_result="stale-datasets"
			sort() {
				printf '%s\n' "partial"
				return 24
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file" "$scratch_file"
			printf 'sort=%s result=<%s>\n' "$?" "$g_zxfer_recursive_dataset_list_result"
		)
	)

	assertContains "Dataset-list capture should preserve the awk failure and clear stale results." \
		"$output" "awk=23 result=<>"
	assertContains "Dataset-list capture should preserve the sort failure and publish nothing." \
		"$output" "sort=24 result=<>"
}

test_filter_recursive_dataset_list_with_excludes_passthrough_without_patterns_in_current_shell() {
	input_list=$(printf '%s\n%s' "tank/src" "tank/src/child")
	g_option_x_exclude_datasets=""

	zxfer_filter_recursive_dataset_list_with_excludes "$input_list"

	# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
	assertEquals "Recursive dataset-list filtering should pass the original dataset list through unchanged when no exclude pattern is configured." \
		"$input_list" "$g_zxfer_recursive_dataset_list_result"
}

test_filter_recursive_dataset_list_with_excludes_filters_matching_entries_in_current_shell() {
	g_option_x_exclude_datasets='/exclude$'

	zxfer_filter_recursive_dataset_list_with_excludes "$(
		cat <<'EOF'
tank/src
tank/src/exclude
tank/src/child
tank/src/child/exclude
EOF
	)"

	# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
	assertEquals "Recursive dataset-list filtering should remove datasets matching the configured exclude pattern." \
		"tank/src
tank/src/child" "$g_zxfer_recursive_dataset_list_result"

	g_option_x_exclude_datasets='^tank/src'
	zxfer_filter_recursive_dataset_list_with_excludes "tank/src"
	filter_status=$?

	assertEquals "grep's no-match status must not fail a list whose datasets are all excluded." \
		0 "$filter_status"
	assertEquals "A list whose datasets are all excluded should become empty." \
		"" "$g_zxfer_recursive_dataset_list_result"
}

test_write_source_snapshot_list_to_file_reports_tempfile_failures() {
	outfile="$TEST_TMPDIR/source_tempfile_failure.out"
	errfile="$TEST_TMPDIR/source_tempfile_failure.err"

	output=$(
		(
			zxfer_create_runtime_artifact_file() {
				printf '%s\n' "unexpected staging file"
				return 17
			}
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\\n' staged-free"
			}
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			wait "$g_source_snapshot_list_pid"
			printf 'status=%s payload=%s\n' "$?" "$(cat "$outfile")"
		)
	)

	assertEquals "A serial source listing should start without allocating a staging file for its command." \
		"status=0 payload=staged-free" "$output"
}

test_write_source_snapshot_list_to_file_runs_each_pass_own_command() {
	outfile="$TEST_TMPDIR/source_command_reuse.out"

	output=$(
		(
			pass=0
			zxfer_build_source_snapshot_list_cmd() {
				pass=$((pass + 1))
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\\n' pass-$pass"
			}
			zxfer_write_source_snapshot_list_to_file "$outfile"
			wait "$g_source_snapshot_list_pid"
			printf 'first=%s\n' "$(cat "$outfile")"
			zxfer_write_source_snapshot_list_to_file "$outfile"
			wait "$g_source_snapshot_list_pid"
			printf 'second=%s\n' "$(cat "$outfile")"
		)
	)

	assertContains "The first discovery pass should run the builder's command." \
		"$output" "first=pass-1"
	assertContains "A second discovery pass should run the command its own build published." \
		"$output" "second=pass-2"
}

test_set_g_recursive_source_list_reports_source_sort_failures() {
	source_tmp="$TEST_TMPDIR/source_sort_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/source_sort_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			sort() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when the source snapshot sort fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the source snapshot sort failure context." \
		"$output" "Failed to sort source snapshots for recursive delta planning."
}

test_set_g_recursive_source_list_reports_recursive_delete_diff_failures() {
	source_tmp="$TEST_TMPDIR/delete_diff_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/delete_diff_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	: >"$dest_tmp"

	set +e
	output=$(
		(
			comm() {
				if [ "$1" = "-3" ]; then
					return 7
				fi
				command comm "$@"
			}
			zxfer_throw_error() {
				printf '%s:%s\n' "$1" "$2"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when the destination-minus-source diff fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the recursive delete diff failure context and status." \
		"$output" "Failed to diff source and destination snapshots for recursive delta planning.:7"
}

test_set_g_recursive_source_list_reports_recursive_destination_exclude_failures() {
	source_tmp="$TEST_TMPDIR/destination_exclude_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/destination_exclude_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n%s\n' "tank/src/child@extra" "tank/src@snap1" >"$dest_tmp"
	g_option_x_exclude_datasets='exclude$'

	set +e
	output=$(
		(
			zxfer_filter_recursive_dataset_list_with_excludes() {
				[ "$1" != "tank/src/child" ] || return 2
				g_zxfer_recursive_dataset_list_result=$1
				return 0
			}
			zxfer_throw_error() {
				printf '%s:%s\n' "$1" "$2"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when filtering the destination delete dataset list fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the destination delete exclude-filter failure context and status." \
		"$output" "Failed to filter recursive destination dataset delete list against exclude patterns.:2"
}

test_set_g_recursive_source_list_reports_recursive_source_inventory_exclude_failures() {
	source_tmp="$TEST_TMPDIR/source_inventory_exclude_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/source_inventory_exclude_failure_dest.txt"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src/child@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	g_option_x_exclude_datasets='exclude$'

	set +e
	output=$(
		(
			# Only the two-dataset inventory list fails.
			zxfer_filter_recursive_dataset_list_with_excludes() {
				[ "${1#*"$ZXFER_LF"}" = "$1" ] || return 2
				g_zxfer_recursive_dataset_list_result=$1
				return 0
			}
			zxfer_throw_error() {
				printf '%s:%s\n' "$1" "$2"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when filtering the source inventory dataset list fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the source inventory exclude-filter failure context and status." \
		"$output" "Failed to filter recursive source dataset inventory against exclude patterns.:2"
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

test_filter_recursive_dataset_list_with_excludes_preserves_grep_hard_failures() {
	# An invalid BRE makes the exclude grep itself fail (status 2) instead of
	# merely matching nothing (status 1), which must fail closed.
	g_option_x_exclude_datasets='\('

	set +e
	output=$(
		(
			zxfer_filter_recursive_dataset_list_with_excludes "tank/src"
		) 2>/dev/null
	)
	status=$?

	assertEquals "Recursive dataset-list filtering should preserve hard grep failures instead of treating them as no-match." \
		2 "$status"
	assertEquals "Recursive dataset-list filtering should not publish a dataset list when the exclude grep fails." \
		"" "$output"
}

test_execute_source_snapshot_name_list_background_sort_cmd_preserves_count_status_tempfile_failures() {
	set +e
	output=$(
		(
			temp_call_count=0
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				if [ "$temp_call_count" -ge 2 ]; then
					return 57
				fi
				g_zxfer_temp_file_result="$TEST_TMPDIR/count-temp-$temp_call_count.tmp"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"echo snapshots" \
				"$TEST_TMPDIR/count-temp-sorted.out" \
				"" \
				"$TEST_TMPDIR/count-temp.count"
		)
	)
	status=$?

	assertEquals "The no-op proof source launcher should preserve count status-file allocation failures exactly." \
		57 "$status"
	assertEquals "The no-op proof source launcher should not emit output for count status-file allocation failures." \
		"" "$output"
}

test_get_zfs_list_reports_initial_tempfile_failures() {
	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 9
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Snapshot discovery should preserve the exact tempfile allocation failure status when the first source staging tempfile cannot be allocated." \
		9 "$status"
	assertEquals "Snapshot discovery should not emit output for first source staging tempfile failures." \
		"" "$output"
}

test_get_zfs_list_reports_second_source_tempfile_failures() {
	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-source-1.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 11
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Snapshot discovery should preserve the exact tempfile allocation failure status when the source stderr staging tempfile cannot be allocated." \
		11 "$status"
	assertEquals "Snapshot discovery should not emit output for source stderr staging tempfile failures." \
		"" "$output"
}

test_get_zfs_list_reports_destination_list_tempfile_failures() {
	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 2 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-dest-$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 12
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Snapshot discovery should preserve the exact tempfile allocation failure status when the destination dataset inventory tempfile cannot be allocated." \
		12 "$status"
	assertEquals "Snapshot discovery should not emit output for destination dataset inventory tempfile failures." \
		"" "$output"
}

test_get_zfs_list_reports_destination_list_errfile_tempfile_failures() {
	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 3 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-dest-err-$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 13
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Snapshot discovery should preserve the exact tempfile allocation failure status when the destination dataset inventory stderr tempfile cannot be allocated." \
		13 "$status"
	assertEquals "Snapshot discovery should not emit output for destination dataset inventory stderr tempfile failures." \
		"" "$output"
}

test_get_zfs_list_propagates_recursive_source_list_failures() {
	set +e
	output=$(
		(
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
				return 23
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
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Snapshot discovery should propagate recursive source-list planning failures instead of continuing with empty planning state." \
		23 "$status"
	assertEquals "Recursive source-list planning failures without their own diagnostic should not emit extra output." \
		"" "$output"
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
