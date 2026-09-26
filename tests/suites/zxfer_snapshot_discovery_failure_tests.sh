#!/bin/sh
# shellcheck shell=sh
# Current-shell seam, injected failure-propagation and source-listing failure
# cases for src/zxfer_snapshot_discovery.sh. Run by
# tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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

test_get_zfs_list_throws_when_source_snapshot_list_is_empty() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_empty.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_empty.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Empty source snapshot listings should abort with zxfer's direct invariant failure status." 1 "$status"
	assertContains "Empty source snapshot listings should surface the retrieval failure." \
		"$output" "Failed to retrieve snapshots from the source"
}

test_get_zfs_list_restores_source_last_command_when_background_snapshot_listing_fails() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			counter_file="$TEST_TMPDIR/get_zfs_fail.counter"
			dest_cache_stage_path=""
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/get_zfs_fail.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing command" >"$2"
				sh -c 'exit 37' &
				g_source_snapshot_list_pid=$!
				g_source_snapshot_list_job_id=""
				g_source_snapshot_list_cmd="sh -c 'printf \"%s\\n\" \"missing command\" >&2; exit 37'"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				dest_cache_stage_path=$1
				: >"$1"
				: >"$2"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "backup"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'dst_cache=<%s>\n' "${g_zxfer_destination_snapshot_record_cache_file:-}"
				if [ -n "$dest_cache_stage_path" ] && [ -e "$dest_cache_stage_path" ]; then
					printf 'dst_cache_exists=yes\n'
				else
					printf 'dst_cache_exists=no\n'
				fi
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source snapshot listing failures should propagate the exact worker status." 37 "$status"
	assertContains "Failure handling should restore the source snapshot command before reporting." \
		"$output" "cmd=sh -c 'printf \"%s"
	assertContains "The restored command should still reference the failing source snapshot probe." \
		"$output" "\"missing command\" >&2; exit 37'"
	assertContains "Background source snapshot listing failures should clear the remembered destination snapshot cache path before reporting." \
		"$output" "dst_cache=<>"
	assertContains "Background source snapshot listing failures should remove the staged destination snapshot cache file before reporting." \
		"$output" "dst_cache_exists=no"
	assertContains "Failure handling should still emit the source snapshot error." \
		"$output" "msg=Failed to retrieve snapshots from the source: missing command"
}

test_get_zfs_list_reports_generic_source_failure_when_background_snapshot_listing_has_no_stderr() {
	set +e
	output=$(
		(
			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			counter_file="$TEST_TMPDIR/get_zfs_fail_blank.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_fail_blank.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				sh -c 'exit 1' &
				g_source_snapshot_list_pid=$!
				g_source_snapshot_list_job_id=""
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
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
			zxfer_throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source snapshot failures without stderr should still propagate the exact worker status." 1 "$status"
	assertContains "Failure handling should still restore the last attempted source snapshot command." \
		"$output" "cmd=sh -c 'exit 1'"
	assertContains "Failure handling should fall back to the generic source snapshot retrieval error when stderr is empty." \
		"$output" "msg=Failed to retrieve snapshots from the source"
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
				g_source_snapshot_list_job_id=""
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
			zxfer_read_snapshot_discovery_capture_file() {
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
