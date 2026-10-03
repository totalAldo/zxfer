#!/bin/sh
# shellcheck shell=sh
# Dataset-list helpers and the failures of snapshot discovery that the fault
# injector cannot reach (temp-file allocation, sort, cmp, awk and staged-file
# readback) for src/zxfer_snapshot_discovery.sh. Run by
# tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_recursive_dataset_list_helpers_extract_and_filter_dataset_names() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file.txt"
	printf '%s\n' "tank/src/child@snap2" "tank/src@snap1" "tank/src/child@snap3" \
		>"$snapshot_records_file"

	zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file" \
		"$TEST_TMPDIR/recursive_snapshot_file.scratch"

	assertEquals "Capture should extract, sort and deduplicate the dataset names." \
		"tank/src
tank/src/child" "$g_zxfer_recursive_dataset_list_result"

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
	# call:status for the source stage's three files, the destination's two,
	# the delta stage's group, the inventory's group and the record file.
	for l_temp_case in 1:9 2:11 3:12 4:13 5:23 11:24 12:37; do
		(
			FAIL_AT=${l_temp_case%:*}
			FAIL_STATUS=${l_temp_case#*:}
			temp_calls=0
			zxfer_get_temp_file() {
				temp_calls=$((temp_calls + 1))
				[ "$temp_calls" -ne "$FAIL_AT" ] || return "$FAIL_STATUS"
				# Stage cleanup removes only paths under the run root.
				g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/get_zfs_temp.$temp_calls"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
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
