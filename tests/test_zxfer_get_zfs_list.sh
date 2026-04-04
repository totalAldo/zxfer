#!/bin/sh
#
# shunit2 tests for zxfer_get_zfs_list.sh helpers.
#
# shellcheck disable=SC2030

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_get_zfs_list.sh
. "$ZXFER_ROOT/src/zxfer_get_zfs_list.sh"

usage() {
	:
}

create_parallel_bin() {
	l_path=$1
	l_version_line=$2
	cat >"$l_path" <<EOF
#!/bin/sh
if [ "\$1" = "--version" ]; then
	printf '%s\n' "$l_version_line"
	exit 0
fi
exit 0
EOF
	chmod +x "$l_path"
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_get_list.XXXXXX)
	GNU_PARALLEL_BIN="$TEST_TMPDIR/gnu_parallel"
	NONGNU_PARALLEL_BIN="$TEST_TMPDIR/non_gnu_parallel"
	create_parallel_bin "$GNU_PARALLEL_BIN" "GNU parallel (fake)"
	create_parallel_bin "$NONGNU_PARALLEL_BIN" "parallel from elsewhere"
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	TMPDIR="$TEST_TMPDIR"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_j_jobs=1
	g_option_O_origin_host=""
	g_option_x_exclude_datasets=""
	g_initial_source_had_trailing_slash=0
	initial_source="tank/src"
	g_destination="backup/dst"
	g_cmd_parallel="$GNU_PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_RZFS="/sbin/zfs"
	g_LZFS="/sbin/zfs"
	g_cmd_zfs="/sbin/zfs"
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_lzfs_list_hr_S_snap=""
	g_recursive_dest_list=""
	g_last_background_pid=""
	g_source_snapshot_list_pid=""
	zxfer_reset_failure_context "unit"
}

test_ensure_parallel_available_for_source_jobs_requires_local_parallel() {
	set +e
	output=$(
		(
			g_option_j_jobs=2
			g_cmd_parallel=""
			ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Parallel listing should fail fast when GNU parallel is missing locally." 1 "$status"
	assertContains "The local-missing error should mention GNU parallel and the local host." \
		"$output" "requires GNU parallel but it was not found in PATH on the local host"
}

test_ensure_parallel_available_for_source_jobs_rejects_non_gnu_parallel() {
	set +e
	output=$(
		(
			g_option_j_jobs=2
			g_cmd_parallel="$NONGNU_PARALLEL_BIN"
			ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Parallel listing should fail when the local binary is not GNU parallel." 1 "$status"
	assertContains "The validation error should mention the non-GNU binary path." \
		"$output" "\"$NONGNU_PARALLEL_BIN\" is not GNU parallel"
}

test_ensure_parallel_available_for_source_jobs_reports_missing_remote_parallel_in_current_shell() {
	set +e
	output=$(
		(
			ssh_bin="$TEST_TMPDIR/missing_remote_parallel_ssh"
			cat >"$ssh_bin" <<'EOF'
#!/bin/sh
exit 0
EOF
			chmod +x "$ssh_bin"
			g_cmd_ssh="$ssh_bin"
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""

			ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Missing remote GNU parallel should fail source-job setup." 1 "$status"
	assertContains "The remote-missing error should identify the origin host." \
		"$output" "GNU parallel not found on origin host origin.example"
}

test_write_source_snapshot_list_to_file_uses_execute_background_cmd_when_serial() {
	log="$TEST_TMPDIR/source_serial.log"
	outfile="$TEST_TMPDIR/source_serial.out"
	errfile="$TEST_TMPDIR/source_serial.err"
	: >"$log"

	(
		SOURCE_LOG="$log"
		build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-serial'"
		}
		execute_background_cmd() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$SOURCE_LOG"
			g_last_background_pid=4242
		}
		g_option_j_jobs=1
		write_source_snapshot_list_to_file "$outfile" "$errfile"
		printf '%s\n' "$g_source_snapshot_list_pid" >>"$SOURCE_LOG"
	)

	assertEquals "Serial snapshot listing should delegate to execute_background_cmd." \
		"printf 'snap-serial'|$outfile|$errfile
4242" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_backgrounds_parallel_command() {
	outfile="$TEST_TMPDIR/source_parallel.out"
	lastcmd_file="$TEST_TMPDIR/source_parallel.lastcmd"
	g_option_j_jobs=3

	(
		build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-parallel'"
		}
		zxfer_record_last_command_string() {
			printf '%s\n' "$1" >"$lastcmd_file"
		}
		write_source_snapshot_list_to_file "$outfile"
		wait
	)

	assertEquals "Parallel snapshot listing should execute the built command in the background." \
		"snap-parallel" "$(cat "$outfile")"
	assertEquals "Parallel snapshot listing should record the last attempted command." \
		"printf 'snap-parallel'" "$(cat "$lastcmd_file")"
}

test_write_source_snapshot_list_to_file_surfaces_parallel_validation_errors() {
	g_option_j_jobs=2
	g_cmd_parallel=""

	set +e
	output=$(
		(
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			write_source_snapshot_list_to_file "$TEST_TMPDIR/source_parallel_error.out"
		)
	)
	status=$?

	assertEquals "Parallel validation failures should abort snapshot list generation." 1 "$status"
	assertContains "Parallel validation failures should preserve the local GNU parallel error." \
		"$output" "requires GNU parallel but it was not found in PATH on the local host"
}

test_diff_snapshot_lists_rejects_unknown_mode() {
	source_file="$TEST_TMPDIR/source_diff.txt"
	dest_file="$TEST_TMPDIR/dest_diff.txt"
	: >"$source_file"
	: >"$dest_file"

	set +e
	output=$(
		(
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			diff_snapshot_lists "$source_file" "$dest_file" "unknown_mode"
		)
	)
	status=$?

	assertEquals "Unknown diff modes should abort." 1 "$status"
	assertContains "Unknown diff modes should include the requested mode." \
		"$output" "Unknown snapshot diff mode: unknown_mode"
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

	set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$verbose_file" 2>&1
	output=$(cat "$verbose_file")

	assertEquals "Excluded datasets should be removed from the transfer list." "tank/src" "$g_recursive_source_list"
	assertEquals "Excluded datasets should also be removed from the dataset cache." "tank/src" "$g_recursive_source_dataset_list"
	assertContains "Very-verbose mode should print the missing-source snapshot heading." \
		"$output" "Snapshots present in source but missing in destination"
	assertContains "Very-verbose mode should print the extra-destination snapshot heading." \
		"$output" "Extra Destination snapshots not in source"
}

test_write_destination_snapshot_list_to_files_outputs_empty_when_destination_missing() {
	full_file="$TEST_TMPDIR/dest_missing_full.txt"
	norm_file="$TEST_TMPDIR/dest_missing_norm.txt"

	(
		exists_destination() {
			printf '0\n'
		}
		write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	)

	assertEquals "Missing destination datasets should yield an empty raw snapshot file." "" "$(cat "$full_file")"
	assertEquals "Missing destination datasets should yield an empty normalized snapshot file." "" "$(cat "$norm_file")"
}

test_normalize_destination_snapshot_list_preserves_destination_when_trailing_slash_requested() {
	input_file="$TEST_TMPDIR/dest_trailing_input.txt"
	output_file="$TEST_TMPDIR/dest_trailing_output.txt"
	g_initial_source_had_trailing_slash=1
	initial_source="tank/src"
	cat <<'EOF' >"$input_file"
backup/dst/child@snap2
backup/dst@snap1
EOF

	normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"

	assertEquals "Trailing-slash destinations should be sorted without source-prefix rewriting." \
		"backup/dst/child@snap2
backup/dst@snap1" "$(cat "$output_file")"
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

	set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$output_file"

	assertEquals "Matching source and destination snapshots should leave no datasets queued for transfer." \
		"" "$g_recursive_source_list"
	assertEquals "Dataset caches should still reflect the source datasets even when nothing needs transfer." \
		"tank/src" "$g_recursive_source_dataset_list"
	assertContains "Verbose mode should explain when no new snapshots need transfer." \
		"$(cat "$output_file")" "No new snapshots to transfer."
}

test_get_zfs_list_bootstraps_missing_destination_dataset_when_pool_exists() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_list.counter"
			printf '%s\n' 0 >"$counter_file"
			get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_list.$idx"
			}
			write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snapA
tank/src@snapB
EOF
			}
			write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "dataset does not exist" >&2
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "backup"
					return 0
				fi
				return 1
			}
			get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'source=%s\n' "$g_lzfs_list_hr_S_snap"
		)
	)

	assertContains "Bootstrap path should treat the missing destination dataset as an empty recursive list." "$output" "dest="
	assertContains "Source snapshots should still be collected and reversed for send planning." \
		"$output" "source=tank/src@snapB
tank/src@snapA"
}

test_get_zfs_list_throws_when_source_snapshot_list_is_empty() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_empty.counter"
			printf '%s\n' 0 >"$counter_file"
			get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_empty.$idx"
			}
			write_source_snapshot_list_to_file() {
				: >"$1"
			}
			write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst"
			}
			throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			get_zfs_list
		)
	)
	status=$?

	assertEquals "Empty source snapshot listings should abort with status 3." 3 "$status"
	assertContains "Empty source snapshot listings should surface the retrieval failure." \
		"$output" "Failed to retrieve snapshots from the source"
}

test_get_zfs_list_restores_source_last_command_when_background_snapshot_listing_fails() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_fail.counter"
			printf '%s\n' 0 >"$counter_file"
			get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_fail.$idx"
			}
			build_source_snapshot_list_cmd() {
				printf '%s\n' "sh -c 'printf \"%s\\n\" \"missing command\" >&2; exit 3'"
			}
			exists_destination() {
				printf '%s\n' 0
			}
			run_destination_zfs_cmd() {
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
			throw_error() {
				printf 'cmd=%s\n' "$g_zxfer_failure_last_command"
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			get_zfs_list
		)
	)
	status=$?

	assertEquals "Background source snapshot listing failures should keep exit status 3." 3 "$status"
	assertContains "Failure handling should restore the source snapshot command before reporting." \
		"$output" "cmd=sh -c 'printf \"%s"
	assertContains "The restored command should still reference the failing source snapshot probe." \
		"$output" "\"missing command\" >&2; exit 3'"
	assertContains "Failure handling should still emit the source snapshot error." \
		"$output" "msg=Failed to retrieve snapshots from the source: missing command"
}

. "$SHUNIT2_BIN"
