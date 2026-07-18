#!/bin/sh
# shellcheck shell=sh
# Dry-run, current-shell seam, and injected failure-propagation cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_write_destination_snapshot_list_to_files_skips_live_validation_in_dry_run() {
	records_file="$TEST_TMPDIR/destination_dry_run.records"
	sorted_file="$TEST_TMPDIR/destination_dry_run.sorted"
	log="$TEST_TMPDIR/destination_dry_run.log"
	: >"$log"

	output=$(
		(
			LOG_FILE="$log"
			zxfer_echoV() {
				printf '%s\n' "$*" >>"$LOG_FILE"
			}
			zxfer_exists_destination() {
				printf '%s\n' "exists-called" >>"$LOG_FILE"
				printf '%s\n' 1
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "destination-cmd-called" >>"$LOG_FILE"
				return 0
			}
			zxfer_normalize_destination_snapshot_list() {
				printf '%s\n' "normalize-called" >>"$LOG_FILE"
			}
			g_option_n_dryrun=1
			g_initial_source="tank/src"
			g_initial_source_had_trailing_slash=0
			g_destination="backup/dst"
			zxfer_write_destination_snapshot_list_to_files "$records_file" "$sorted_file"
			printf 'records_exists=%s\n' "$([ -f "$records_file" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'sorted_exists=%s\n' "$([ -f "$sorted_file" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'records_size=%s\n' "$(wc -c <"$records_file" 2>/dev/null | tr -d '[:space:]' || printf '%s' missing)"
			printf 'sorted_size=%s\n' "$(wc -c <"$sorted_file" 2>/dev/null | tr -d '[:space:]' || printf '%s' missing)"
		)
	)

	assertNotContains "Dry-run destination snapshot discovery should not probe destination dataset existence." \
		"$(cat "$log")" "exists-called"
	assertNotContains "Dry-run destination snapshot discovery should not run the live destination zfs helper." \
		"$(cat "$log")" "destination-cmd-called"
	assertNotContains "Dry-run destination snapshot discovery should not run the normalization helper." \
		"$(cat "$log")" "normalize-called"
	assertContains "Dry-run destination snapshot discovery should render the skipped destination listing command." \
		"$(cat "$log")" "Dry run:"
	assertContains "Dry-run destination snapshot discovery should create the raw destination snapshot tempfile." \
		"$output" "records_exists=1"
	assertContains "Dry-run destination snapshot discovery should create the normalized destination snapshot tempfile." \
		"$output" "sorted_exists=1"
	assertContains "Dry-run destination snapshot discovery should leave the raw destination snapshot tempfile empty." \
		"$output" "records_size=0"
	assertContains "Dry-run destination snapshot discovery should leave the normalized destination snapshot tempfile empty." \
		"$output" "sorted_size=0"
}

test_write_destination_snapshot_list_to_files_reports_preview_render_failures_in_dry_run() {
	records_file="$TEST_TMPDIR/destination_dry_run_error.records"
	sorted_file="$TEST_TMPDIR/destination_dry_run_error.sorted"

	zxfer_test_capture_subshell "
		zxfer_render_destination_zfs_command() {
			printf '%s\n' 'destination preview render failed'
			return 1
		}
		g_option_n_dryrun=1
		g_initial_source='tank/src'
		g_destination='backup/dst'
		zxfer_write_destination_snapshot_list_to_files '$records_file' '$sorted_file'
	"

	assertEquals "Dry-run destination snapshot discovery should fail closed when preview rendering fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Dry-run destination snapshot discovery should surface the preview render failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "destination preview render failed"
}

test_write_destination_snapshot_list_to_files_preserves_record_stage_failures_in_dry_run() {
	records_file="$TEST_TMPDIR/destination_dry_run_stage_failure.records"
	sorted_file="$TEST_TMPDIR/destination_dry_run_stage_failure.sorted"

	output=$(
		(
			write_call_count=0
			zxfer_write_runtime_artifact_file() {
				write_call_count=$((write_call_count + 1))
				printf 'write=%s:%s\n' "$write_call_count" "$1"
				return 31
			}
			g_option_n_dryrun=1
			g_initial_source="tank/src"
			g_destination="backup/dst"
			set +e
			zxfer_write_destination_snapshot_list_to_files "$records_file" "$sorted_file"
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'calls=%s\n' "$write_call_count"
		)
	)

	assertContains "Dry-run destination snapshot discovery should preserve raw-record staging failures." \
		"$output" "status=31"
	assertContains "Dry-run destination snapshot discovery should stop after the raw-record stage fails." \
		"$output" "calls=1"
	assertContains "Dry-run destination snapshot discovery should fail on the raw destination snapshot stage first." \
		"$output" "write=1:$records_file"
}

test_write_destination_snapshot_list_to_files_preserves_sorted_stage_failures_in_dry_run() {
	records_file="$TEST_TMPDIR/destination_dry_run_sorted_stage_failure.records"
	sorted_file="$TEST_TMPDIR/destination_dry_run_sorted_stage_failure.sorted"

	output=$(
		(
			write_call_count=0
			zxfer_write_runtime_artifact_file() {
				write_call_count=$((write_call_count + 1))
				printf 'write=%s:%s\n' "$write_call_count" "$1"
				if [ "$write_call_count" -eq 1 ]; then
					return 0
				fi
				return 37
			}
			g_option_n_dryrun=1
			g_initial_source="tank/src"
			g_destination="backup/dst"
			set +e
			zxfer_write_destination_snapshot_list_to_files "$records_file" "$sorted_file"
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'calls=%s\n' "$write_call_count"
		)
	)

	assertContains "Dry-run destination snapshot discovery should preserve normalized-list staging failures." \
		"$output" "status=37"
	assertContains "Dry-run destination snapshot discovery should attempt the normalized stage after the raw-record stage succeeds." \
		"$output" "calls=2"
	assertContains "Dry-run destination snapshot discovery should still stage the raw destination snapshot list first." \
		"$output" "write=1:$records_file"
	assertContains "Dry-run destination snapshot discovery should surface the normalized destination snapshot staging failure second." \
		"$output" "write=2:$sorted_file"
}

test_get_zfs_list_skips_live_snapshot_discovery_in_dry_run() {
	log="$TEST_TMPDIR/get_zfs_dry_run.log"
	: >"$log"

	output=$(
		(
			LOG_FILE="$log"
			zxfer_echoV() {
				printf '%s\n' "$*" >>"$LOG_FILE"
			}
			zxfer_get_temp_file() {
				printf '%s\n' "$TEST_TMPDIR/get_zfs_dry_run.tmp"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "source-called" >>"$LOG_FILE"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "destination-called" >>"$LOG_FILE"
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "dest-zfs-called" >>"$LOG_FILE"
				return 0
			}
			g_option_n_dryrun=1
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_recursive_source_list="stale-source"
			g_recursive_source_dataset_list="stale-source
stale-source/child"
			g_recursive_destination_extra_dataset_list="stale-extra"
			g_recursive_dest_list="stale-dest"
			zxfer_get_zfs_list
			printf 'source=%s\n' "${g_lzfs_list_hr_snap:-}"
			printf 'source_list=%s\n' "${g_recursive_source_list:-}"
			printf 'source_datasets=%s\n' "${g_recursive_source_dataset_list:-}"
			printf 'dest_extra=%s\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'dest=%s\n' "${g_recursive_dest_list:-}"
		)
	)

	assertContains "Dry-run snapshot discovery should explain that live discovery is skipped." \
		"$(cat "$log")" "Dry run: skipping live snapshot discovery for tank/src -> backup/dst."
	assertNotContains "Dry-run snapshot discovery should not start the source snapshot helper." \
		"$(cat "$log")" "source-called"
	assertNotContains "Dry-run snapshot discovery should not start the destination snapshot helper." \
		"$(cat "$log")" "destination-called"
	assertNotContains "Dry-run snapshot discovery should not execute any destination zfs listing." \
		"$(cat "$log")" "dest-zfs-called"
	assertContains "Dry-run snapshot discovery should leave the cached source snapshot list empty." \
		"$output" "source="
	assertContains "Dry-run snapshot discovery should clear any stale recursive source list." \
		"$output" "source_list="
	assertContains "Dry-run snapshot discovery should clear any stale recursive source dataset cache." \
		"$output" "source_datasets="
	assertContains "Dry-run snapshot discovery should clear any stale destination-extra dataset cache." \
		"$output" "dest_extra="
	assertContains "Dry-run snapshot discovery should leave the cached destination dataset list empty." \
		"$output" "dest="
}

test_get_zfs_list_dry_run_ignores_stale_background_completion_failure_state() {
	log="$TEST_TMPDIR/get_zfs_dry_run_stale_completion.log"
	: >"$log"

	output=$(
		(
			LOG_FILE="$log"
			zxfer_echoV() {
				printf '%s\n' "$*" >>"$LOG_FILE"
			}
			zxfer_throw_error() {
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			g_option_n_dryrun=1
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_zxfer_background_job_wait_report_failure="completion_write"
			zxfer_get_zfs_list
			printf 'source=%s\n' "${g_lzfs_list_hr_snap:-}"
		)
	)
	status=$?

	assertEquals "Dry-run snapshot discovery should not reuse stale supervisor completion state when no background job was awaited." \
		0 "$status"
	assertContains "Dry-run snapshot discovery should still complete normally when the wait scratch contains stale completion data." \
		"$output" "source="
	assertNotContains "Dry-run snapshot discovery should not report a stale supervisor completion failure." \
		"$output" "Failed to report source snapshot discovery completion."
}

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

test_zxfer_check_parallel_source_jobs_in_current_shell_preserves_nested_validation_status_without_tempfile_staging() {
	output=$(
		(
			g_zxfer_parallel_source_job_check_result="stale-parallel-check"
			g_zxfer_parallel_source_job_check_kind="stale-kind"
			zxfer_ensure_parallel_available_for_source_jobs() {
				return 23
			}
			set +e
			zxfer_check_parallel_source_jobs_in_current_shell
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
			printf 'result=<%s>\n' "${g_zxfer_parallel_source_job_check_result:-}"
			# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
			printf 'kind=<%s>\n' "${g_zxfer_parallel_source_job_check_kind:-}"
		)
	)

	assertContains "Current-shell parallel validation should preserve the nested availability-check status without staging through a temp file." \
		"$output" "status=23"
	assertContains "Current-shell parallel validation should clear stale fallback scratch before invoking the nested availability check." \
		"$output" "result=<>"
	assertContains "Current-shell parallel validation should also clear stale reason-kind scratch before invoking the nested availability check." \
		"$output" "kind=<>"
}

test_zxfer_check_parallel_source_jobs_in_current_shell_preserves_current_shell_reason_when_nested_validation_reuses_generic_scratch() {
	output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				# Nested POSIX shell helpers share one variable namespace, so this
				# intentionally reuses a generic scratch name that remote-helper
				# helpers also use in production.
				l_capture_path="$TEST_TMPDIR/nested-remote-probe-stderr"
				g_zxfer_parallel_source_job_check_result="nested remote validation failed"
				g_zxfer_parallel_source_job_check_kind="origin_probe_failed"
				return 1
			}
			set +e
			zxfer_check_parallel_source_jobs_in_current_shell
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
			printf 'result=<%s>\n' "${g_zxfer_parallel_source_job_check_result:-}"
			# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
			printf 'kind=<%s>\n' "${g_zxfer_parallel_source_job_check_kind:-}"
		)
	)

	assertContains "Current-shell parallel validation should preserve the nested availability-check status even when nested helpers reuse generic scratch variable names." \
		"$output" "status=1"
	assertContains "Current-shell parallel validation should preserve the nested failure reason directly from current-shell globals even when nested helpers reuse generic scratch variable names." \
		"$output" "result=<nested remote validation failed"
	assertContains "Current-shell parallel validation should preserve the machine-readable reason kind from the nested availability check." \
		"$output" "kind=<origin_probe_failed>"
}

test_zxfer_check_parallel_source_jobs_in_current_shell_avoids_tempfile_allocation_and_cleanup() {
	output=$(
		(
			tempfile_log="$TEST_TMPDIR/parallel-check-tempfile.log"
			cleanup_log="$TEST_TMPDIR/parallel-check-cleanup.log"
			zxfer_get_temp_file() {
				printf '%s\n' "called" >"$tempfile_log"
				return 1
			}
			zxfer_ensure_parallel_available_for_source_jobs() {
				g_zxfer_parallel_source_job_check_result="parallel validation failed"
				g_zxfer_parallel_source_job_check_kind="origin_probe_failed"
				return 27
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf '%s\n' "$1" >"$cleanup_log"
				return 0
			}
			set +e
			zxfer_check_parallel_source_jobs_in_current_shell
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
			printf 'result=<%s>\n' "${g_zxfer_parallel_source_job_check_result:-}"
			printf 'tempfile_called=<%s>\n' "$(cat "$tempfile_log" 2>/dev/null)"
			printf 'cleanup=<%s>\n' "$(cat "$cleanup_log" 2>/dev/null)"
		)
	)

	assertContains "Current-shell parallel validation should preserve nested availability-check failure statuses without temp-file staging." \
		"$output" "status=27"
	assertContains "Current-shell parallel validation should preserve the fallback reason published directly by the nested availability check." \
		"$output" "result=<parallel validation failed>"
	assertContains "Current-shell parallel validation should not allocate a staging temp file now that the availability check publishes current-shell globals directly." \
		"$output" "tempfile_called=<>"
	assertContains "Current-shell parallel validation should not attempt temp-path cleanup when no staging artifact is created." \
		"$output" "cleanup=<>"
}

test_build_source_snapshot_list_cmd_preserves_remote_parallel_resolution_from_current_shell() {
	output=$(
		(
			zxfer_build_shell_command_from_argv() {
				printf '%s\n' "$*"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "sh -c $1"
			}
			zxfer_build_ssh_shell_command_for_host() {
				printf '%s\n' "ssh $1 $2"
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
			zxfer_build_source_snapshot_list_cmd
			printf 'resolved=%s\n' "$g_origin_parallel_cmd"
		)
	)

	assertContains "Remote source snapshot planning should retain the helper path resolved during the current-shell availability check." \
		"$output" "/opt/bin/parallel -j 4 --line-buffer"
	assertContains "Remote source snapshot planning should preserve the direct remote dataset enumeration command." \
		"$output" "/remote/bin/zfs list -Hr -t filesystem,volume -o name tank/src"
	assertContains "Remote source snapshot planning should preserve the resolved origin-host parallel helper after command rendering." \
		"$output" "resolved=/opt/bin/parallel"
}

test_capture_recursive_dataset_list_from_lines_file_sorts_unique_entries_in_current_shell() {
	dataset_lines_file="$TEST_TMPDIR/recursive_dataset_lines.txt"
	cat >"$dataset_lines_file" <<'EOF'
tank/src/child
tank/src
tank/src/child
EOF

	zxfer_capture_recursive_dataset_list_from_lines_file "$dataset_lines_file"

	assertEquals "Recursive dataset-list capture from plain lines should sort and deduplicate datasets in current-shell scratch." \
		"tank/src
tank/src/child" "$g_zxfer_recursive_dataset_list_result"
}

test_capture_recursive_dataset_list_from_lines_file_reports_tempfile_failures() {
	dataset_lines_file="$TEST_TMPDIR/recursive_dataset_lines_temp_failure.txt"
	printf '%s\n' "tank/src" >"$dataset_lines_file"

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_lines_file "$dataset_lines_file"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from plain lines should fail closed when it cannot allocate a sort staging file." \
		1 "$status"
	assertEquals "Recursive dataset-list capture from plain lines should not emit output for tempfile failures." \
		"" "$output"
}

test_capture_recursive_dataset_list_from_lines_file_reports_staged_read_failures() {
	dataset_lines_file="$TEST_TMPDIR/recursive_dataset_lines_read_failure.txt"
	sorted_file="$g_zxfer_run_tmp_root/recursive_dataset_lines_read_failure.sorted"
	printf '%s\n' "tank/src/child" >"$dataset_lines_file"
	printf '%s\n' "tank/src" >>"$dataset_lines_file"

	output=$(
		(
			g_zxfer_recursive_dataset_list_result="stale-datasets"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$sorted_file"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_read_snapshot_discovery_capture_file() {
				return 41
			}
			set +e
			zxfer_capture_recursive_dataset_list_from_lines_file "$dataset_lines_file"
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'sorted_exists=%s\n' "$([ -e "$sorted_file" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'result=<%s>\n' "${g_zxfer_recursive_dataset_list_result:-}"
		)
	)

	assertContains "Recursive dataset-list capture from plain lines should preserve staged readback failures." \
		"$output" "status=41"
	assertContains "Recursive dataset-list capture from plain lines should clean up the sorted staging file after a readback failure." \
		"$output" "sorted_exists=0"
	assertContains "Recursive dataset-list capture from plain lines should clear stale current-shell results before surfacing readback failures." \
		"$output" "result=<>"
}

test_capture_recursive_dataset_list_from_snapshot_file_extracts_sorted_unique_datasets() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file.txt"
	cat >"$snapshot_records_file" <<'EOF'
tank/src/child@snap2
tank/src@snap1
tank/src/child@snap3
EOF

	zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file"

	# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
	assertEquals "Recursive dataset-list capture from snapshot files should extract, sort, and deduplicate dataset names." \
		"tank/src
tank/src/child" "$g_zxfer_recursive_dataset_list_result"
}

test_capture_recursive_dataset_list_from_snapshot_file_reports_line_capture_failures() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file_line_failure.txt"
	printf '%s\n' "tank/src@snap1" >"$snapshot_records_file"

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_lines_file() {
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from snapshot files should fail closed when recursive line capture fails." \
		1 "$status"
	assertEquals "Recursive dataset-list capture from snapshot files should not emit output for recursive line-capture failures." \
		"" "$output"
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
}

test_filter_recursive_dataset_list_with_excludes_reports_second_tempfile_failures() {
	g_option_x_exclude_datasets='^tank/src/exclude$'

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/recursive_filter_input.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_filter_recursive_dataset_list_with_excludes "tank/src"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list filtering should fail closed when the filtered-output tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Recursive dataset-list filtering should not emit output for second-tempfile failures." \
		"" "$output"
}

test_filter_recursive_dataset_list_with_excludes_reports_staged_read_failures() {
	input_file="$g_zxfer_run_tmp_root/recursive_filter_read_failure_input.tmp"
	filtered_file="$g_zxfer_run_tmp_root/recursive_filter_read_failure_filtered.tmp"
	g_option_x_exclude_datasets='exclude'

	output=$(
		(
			call_count=0
			g_zxfer_recursive_dataset_list_result="stale-filtered-datasets"
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$input_file"
				else
					g_zxfer_temp_file_result="$filtered_file"
				fi
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_read_snapshot_discovery_capture_file() {
				return 43
			}
			set +e
			zxfer_filter_recursive_dataset_list_with_excludes "$(printf '%s\n%s\n' "tank/src" "tank/src/exclude")"
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'input_exists=%s\n' "$([ -e "$input_file" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'filtered_exists=%s\n' "$([ -e "$filtered_file" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'result=<%s>\n' "${g_zxfer_recursive_dataset_list_result:-}"
		)
	)

	assertContains "Recursive dataset-list filtering should preserve staged readback failures." \
		"$output" "status=43"
	assertContains "Recursive dataset-list filtering should clean up the input staging file after a readback failure." \
		"$output" "input_exists=0"
	assertContains "Recursive dataset-list filtering should clean up the filtered staging file after a readback failure." \
		"$output" "filtered_exists=0"
	assertContains "Recursive dataset-list filtering should clear stale current-shell results before surfacing readback failures." \
		"$output" "result=<>"
}

test_write_source_snapshot_list_to_file_reports_tempfile_failures() {
	outfile="$TEST_TMPDIR/source_tempfile_failure.out"
	errfile="$TEST_TMPDIR/source_tempfile_failure.err"

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 17
			}
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		)
	)
	status=$?

	assertEquals "Source snapshot discovery should preserve the exact tempfile allocation failure status when the staged command tempfile cannot be allocated." \
		17 "$status"
	assertEquals "Source snapshot discovery should not emit output for staged command tempfile failures." \
		"" "$output"
}

test_capture_recursive_dataset_list_from_snapshot_file_reports_tempfile_failures() {
	snapshot_records_file="$TEST_TMPDIR/recursive_snapshot_file_temp_failure.txt"
	printf '%s\n' "tank/src@snap1" >"$snapshot_records_file"

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_file "$snapshot_records_file"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from snapshot files should fail closed when the first tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Recursive dataset-list capture from snapshot files should not emit output for first-tempfile failures." \
		"" "$output"
}

test_filter_recursive_dataset_list_with_excludes_reports_initial_tempfile_failures() {
	g_option_x_exclude_datasets='exclude$'

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_filter_recursive_dataset_list_with_excludes "tank/src"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list filtering should fail closed when the first tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Recursive dataset-list filtering should not emit output for first-tempfile failures." \
		"" "$output"
}

test_filter_recursive_dataset_list_with_excludes_reports_input_write_failures() {
	g_option_x_exclude_datasets='exclude$'

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/filter-$call_count.tmp"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 73
			}
			zxfer_filter_recursive_dataset_list_with_excludes "tank/src"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list filtering should fail closed when the input staging file cannot be written." \
		73 "$status"
	assertEquals "Recursive dataset-list filtering should not publish shell noise when the input staging file cannot be written." \
		"" "$output"
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
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when the destination-minus-source diff fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the recursive delete diff failure context." \
		"$output" "Failed to diff source and destination snapshots for recursive delta planning."
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
			filter_call_count=0
			zxfer_filter_recursive_dataset_list_with_excludes() {
				filter_call_count=$((filter_call_count + 1))
				if [ "$filter_call_count" -eq 2 ]; then
					return 1
				fi
				g_zxfer_recursive_dataset_list_result=$1
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when filtering the destination delete dataset list fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the destination delete exclude-filter failure context." \
		"$output" "Failed to filter recursive destination dataset delete list against exclude patterns."
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
			filter_call_count=0
			zxfer_filter_recursive_dataset_list_with_excludes() {
				filter_call_count=$((filter_call_count + 1))
				if [ "$filter_call_count" -eq 3 ]; then
					return 1
				fi
				g_zxfer_recursive_dataset_list_result=$1
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when filtering the source inventory dataset list fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the source inventory exclude-filter failure context." \
		"$output" "Failed to filter recursive source dataset inventory against exclude patterns."
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

test_set_g_recursive_source_list_reports_empty_source_delta_stage_failures() {
	source_tmp="$TEST_TMPDIR/empty_source_delta_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/empty_source_delta_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	g_option_x_exclude_datasets=""

	set +e
	output=$(
		(
			zxfer_write_runtime_artifact_file() {
				return 77
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when staging the empty source delta fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the empty source delta staging failure context." \
		"$output" "Failed to stage empty recursive source snapshot delta."
}

test_set_g_recursive_source_list_reports_empty_destination_delta_stage_failures() {
	source_tmp="$TEST_TMPDIR/empty_destination_delta_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/empty_destination_delta_failure_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	g_option_x_exclude_datasets=""

	set +e
	output=$(
		(
			empty_delta_write_call_count=0
			zxfer_write_runtime_artifact_file() {
				empty_delta_write_call_count=$((empty_delta_write_call_count + 1))
				if [ "$empty_delta_write_call_count" -ge 2 ]; then
					return 78
				fi
				: >"$1"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when staging the empty destination delta fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the empty destination delta staging failure context." \
		"$output" "Failed to stage empty recursive destination snapshot delta."
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

test_recursive_snapshot_delta_stage_helpers_preserve_failure_statuses_when_reporter_returns() {
	raw_source_file="$TEST_TMPDIR/delta_stage_status_raw_source.txt"
	presorted_source_file="$TEST_TMPDIR/delta_stage_status_presorted_source.txt"
	destination_file="$TEST_TMPDIR/delta_stage_status_destination.txt"
	sorted_source_file="$TEST_TMPDIR/delta_stage_status_sorted_source.txt"
	filtered_source_file="$TEST_TMPDIR/delta_stage_status_filtered_source.txt"
	filtered_destination_file="$TEST_TMPDIR/delta_stage_status_filtered_destination.txt"
	missing_file="$TEST_TMPDIR/delta_stage_status_missing.txt"
	extra_file="$TEST_TMPDIR/delta_stage_status_extra.txt"
	printf '%s\n' "tank/src@snap1" >"$raw_source_file"
	cp "$raw_source_file" "$presorted_source_file"
	cp "$raw_source_file" "$destination_file"

	output=$(
		(
			zxfer_cleanup_runtime_artifact_path_list() { :; }
			zxfer_throw_error() {
				printf 'reported=%s\n' "$1"
				return 0
			}

			status=0
			zxfer_prepare_recursive_snapshot_delta_inputs \
				"$raw_source_file" "$destination_file" \
				"$TEST_TMPDIR/missing-presorted-source.txt" \
				"$sorted_source_file" "$filtered_source_file" \
				"$filtered_destination_file" "delta-stage-files" || status=$?
			printf 'missing_presorted=%s\n' "$status"

			sort() { return 41; }
			status=0
			zxfer_prepare_recursive_snapshot_delta_inputs \
				"$raw_source_file" "$destination_file" "" \
				"$sorted_source_file" "$filtered_source_file" \
				"$filtered_destination_file" "delta-stage-files" || status=$?
			printf 'sort=%s\n' "$status"

			g_option_x_exclude_datasets="excluded"
			zxfer_filter_snapshot_file_with_excludes() { return 42; }
			status=0
			zxfer_prepare_recursive_snapshot_delta_inputs \
				"$raw_source_file" "$destination_file" "$presorted_source_file" \
				"$sorted_source_file" "$filtered_source_file" \
				"$filtered_destination_file" "delta-stage-files" || status=$?
			printf 'source_filter=%s\n' "$status"

			filter_call_count=0
			zxfer_filter_snapshot_file_with_excludes() {
				filter_call_count=$((filter_call_count + 1))
				[ "$filter_call_count" -eq 1 ] || return 43
				return 0
			}
			status=0
			zxfer_prepare_recursive_snapshot_delta_inputs \
				"$raw_source_file" "$destination_file" "$presorted_source_file" \
				"$sorted_source_file" "$filtered_source_file" \
				"$filtered_destination_file" "delta-stage-files" || status=$?
			printf 'destination_filter=%s\n' "$status"

			zxfer_write_runtime_artifact_file() { return 44; }
			status=0
			zxfer_materialize_recursive_snapshot_delta_files \
				"$raw_source_file" "$destination_file" "$missing_file" \
				"$extra_file" "delta-stage-files" || status=$?
			printf 'empty_source=%s\n' "$status"

			write_call_count=0
			zxfer_write_runtime_artifact_file() {
				write_call_count=$((write_call_count + 1))
				[ "$write_call_count" -eq 1 ] || return 45
				: >"$1"
			}
			status=0
			zxfer_materialize_recursive_snapshot_delta_files \
				"$raw_source_file" "$destination_file" "$missing_file" \
				"$extra_file" "delta-stage-files" || status=$?
			printf 'empty_destination=%s\n' "$status"

			cmp() { return 46; }
			status=0
			zxfer_materialize_recursive_snapshot_delta_files \
				"$raw_source_file" "$destination_file" "$missing_file" \
				"$extra_file" "delta-stage-files" || status=$?
			printf 'compare=%s\n' "$status"

			cmp() { return 1; }
			zxfer_write_snapshot_delta_files() { return 47; }
			status=0
			zxfer_materialize_recursive_snapshot_delta_files \
				"$raw_source_file" "$destination_file" "$missing_file" \
				"$extra_file" "delta-stage-files" || status=$?
			printf 'diff=%s\n' "$status"
		)
	)

	assertContains "Missing presorted inputs should retain their validation status when the reporter returns." \
		"$output" "missing_presorted=1"
	assertContains "Source sort failures should retain the sort status when the reporter returns." \
		"$output" "sort=41"
	assertContains "Source filter failures should retain the filter status when the reporter returns." \
		"$output" "source_filter=42"
	assertContains "Destination filter failures should retain the filter status when the reporter returns." \
		"$output" "destination_filter=43"
	assertContains "Empty source-delta staging should retain its write status when the reporter returns." \
		"$output" "empty_source=44"
	assertContains "Empty destination-delta staging should retain its write status when the reporter returns." \
		"$output" "empty_destination=45"
	assertContains "Snapshot comparison failures should retain the comparison status when the reporter returns." \
		"$output" "compare=46"
	assertContains "Snapshot diff failures should retain the diff status when the reporter returns." \
		"$output" "diff=47"
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

test_execute_source_snapshot_name_list_background_sort_cmd_preserves_count_file_quoting_failures() {
	set +e
	output=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "$TEST_TMPDIR/cleanup-wrapper.sh"
			}
			# Command substitutions run the stub in subshells, so key the
			# injected failure off the argument instead of a call counter.
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "$TEST_TMPDIR/count-quote.count" ]; then
					return 53
				fi
				printf "'%s'\n" "$1"
			}
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"echo snapshots" \
				"$TEST_TMPDIR/count-quote-sorted.out" \
				"" \
				"$TEST_TMPDIR/count-quote.count"
		)
	)
	status=$?

	assertEquals "The no-op proof source launcher should preserve count-file quoting failures exactly." \
		53 "$status"
	assertEquals "The no-op proof source launcher should not emit output for count-file quoting failures." \
		"" "$output"
}

test_execute_source_snapshot_name_list_background_sort_cmd_preserves_count_status_tempfile_failures() {
	set +e
	output=$(
		(
			temp_call_count=0
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "$TEST_TMPDIR/cleanup-wrapper.sh"
			}
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
