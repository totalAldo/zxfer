#!/bin/sh
# shellcheck shell=sh
# Dataset-list helpers and the failures of snapshot discovery that the fault
# injector cannot reach (temp-file allocation, sort, cmp, awk and staged-file
# readback) for src/zxfer_snapshot_discovery.sh. Run by
# tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_snapshot_dataset_lists_extract_sort_and_deduplicate_each_role() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file.txt"
	printf '%s\n' "tank/src/child@snap2" "tank/src@snap1" "tank/src/child@snap3" "tank/src/space name@snap4" \
		>"$snapshot_records_file"
	extra_records_file="$TEST_TMPDIR/extra_snapshot_file.txt"
	printf '%s\n' "tank/src/child@snap5" "tank/src/extra@snap1" "tank/src/child@snap6" >"$extra_records_file"

	zxfer_capture_snapshot_dataset_lists "$snapshot_records_file" "$extra_records_file" \
		"$TEST_TMPDIR/recursive_snapshot_file.scratch" "$snapshot_records_file"

	assertEquals "Capture should extract, sort and deduplicate the dataset names." \
		"tank/src
tank/src/child
tank/src/space name" "$g_recursive_source_list"
	assertEquals "The inventory includes the full source, independently of the transfer list." \
		"$g_recursive_source_list" "$g_recursive_source_dataset_list"
	assertEquals "A dataset present on both sides stays in each independently sorted list." \
		"tank/src/child
tank/src/extra" "$g_recursive_destination_extra_dataset_list"
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
	# call:status for all five workspace files, six delta files, two inventory
	# files and the source record cache. No failed sibling can be ignored.
	for l_temp_case in 1:9 2:11 3:12 4:13 5:23 6:24 7:25 8:26 9:27 10:28 11:29 12:37 13:38 14:39; do
		(
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
