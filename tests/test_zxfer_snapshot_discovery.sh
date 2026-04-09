#!/bin/sh
#
# shunit2 tests for zxfer_snapshot_discovery.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_discovery.sh"

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

create_fake_ssh_handshake_bin() {
	l_path=$1
	l_parallel_status=$2
	cat >"$l_path" <<EOF
#!/bin/sh
cat <<'INNER_EOF'
ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	$l_parallel_status	$([ "$l_parallel_status" = "0" ] && printf '%s' /opt/bin/parallel || printf '%s' -)
tool	cat	0	/remote/bin/cat
INNER_EOF
EOF
	chmod +x "$l_path"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_get_list"
	GNU_PARALLEL_BIN="$TEST_TMPDIR/gnu_parallel"
	NONGNU_PARALLEL_BIN="$TEST_TMPDIR/non_gnu_parallel"
	create_parallel_bin "$GNU_PARALLEL_BIN" "GNU parallel (fake)"
	create_parallel_bin "$NONGNU_PARALLEL_BIN" "parallel from elsewhere"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
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
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_cmd_parallel="$GNU_PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_RZFS="/sbin/zfs"
	g_LZFS="/sbin/zfs"
	g_cmd_zfs="/sbin/zfs"
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_recursive_dest_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_linear_reverse_max_lines=""
	g_last_background_pid=""
	g_source_snapshot_list_pid=""
	zxfer_reset_destination_existence_cache
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
}

test_zxfer_get_source_snapshot_parallel_dataset_threshold_scales_locally() {
	g_option_j_jobs=5

	result=$(zxfer_get_source_snapshot_parallel_dataset_threshold)

	assertEquals "Local adaptive source discovery should scale with the configured job count." \
		"10" "$result"
}

test_zxfer_get_source_snapshot_parallel_dataset_threshold_treats_invalid_jobs_as_single_job_in_current_shell() {
	output_file="$TEST_TMPDIR/source_parallel_threshold.out"
	g_option_j_jobs="invalid"

	zxfer_get_source_snapshot_parallel_dataset_threshold >"$output_file"

	assertEquals "Invalid job counts should fall back to the single-job adaptive discovery threshold." \
		"8" "$(cat "$output_file")"
}

test_zxfer_get_source_snapshot_parallel_dataset_threshold_biases_remote_warm_startup_lower() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_remote_capabilities_bootstrap_source="cache"
	g_ssh_supports_control_sockets=1
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.socket"

	result=$(zxfer_get_source_snapshot_parallel_dataset_threshold)

	assertEquals "Warm cached remote startup should allow a lower dataset threshold before parallel discovery is used." \
		"6" "$result"
}

test_zxfer_get_source_snapshot_parallel_dataset_threshold_biases_remote_cold_startup_higher() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_remote_capabilities_bootstrap_source="live"
	g_ssh_supports_control_sockets=0
	g_ssh_origin_control_socket=""

	result=$(zxfer_get_source_snapshot_parallel_dataset_threshold)

	assertEquals "Cold remote startup without control-socket reuse should require a larger dataset tree before parallel discovery is used." \
		"14" "$result"
}

test_zxfer_count_source_snapshot_discovery_datasets_counts_entries_and_empty_input() {
	result=$(zxfer_count_source_snapshot_discovery_datasets "tank/src
tank/src/child")
	empty_result=$(zxfer_count_source_snapshot_discovery_datasets "")

	assertEquals "Source snapshot discovery dataset counting should return the number of newline-delimited datasets." \
		"2" "$result"
	assertEquals "Source snapshot discovery dataset counting should treat empty input as zero datasets." \
		"0" "$empty_result"
}

test_zxfer_count_source_snapshot_discovery_datasets_rejects_nonnumeric_awk_output_in_current_shell() {
	fake_awk="$TEST_TMPDIR/fake_bad_awk"
	orig_cmd_awk=$g_cmd_awk
	cat >"$fake_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "not-a-number"
EOF
	chmod +x "$fake_awk"
	g_cmd_awk="$fake_awk"

	set +e
	zxfer_count_source_snapshot_discovery_datasets "tank/src"
	status=$?
	g_cmd_awk=$orig_cmd_awk

	assertEquals "Source snapshot discovery dataset counting should fail when awk does not return a numeric line count." \
		1 "$status"
}

test_zxfer_build_source_snapshot_dataset_list_printf_cmd_preserves_dataset_boundaries() {
	cmd=$(zxfer_build_source_snapshot_dataset_list_printf_cmd "tank/src
tank/src/child")

	result=$(eval "$cmd")

	assertEquals "Inlined source dataset lists should round-trip each dataset as a separate line." \
		"tank/src
tank/src/child" "$result"
}

test_destination_snapshot_dataset_helpers_map_root_and_child_datasets() {
	assertEquals "Non-trailing-slash recursive replication should append the source root name under the destination root." \
		"backup/dst/src" "$(zxfer_get_destination_snapshot_root_dataset)"
	assertEquals "Non-trailing-slash recursive replication should map child datasets beneath the derived destination root." \
		"backup/dst/src/child" "$(zxfer_get_destination_dataset_for_source_dataset "tank/src/child")"

	g_initial_source_had_trailing_slash=1
	assertEquals "Trailing-slash recursive replication should keep the destination root unchanged." \
		"backup/dst" "$(zxfer_get_destination_snapshot_root_dataset)"
	assertEquals "Trailing-slash recursive replication should map child datasets directly beneath the requested destination." \
		"backup/dst/child" "$(zxfer_get_destination_dataset_for_source_dataset "tank/src/child")"
}

test_destination_snapshot_dataset_helpers_cover_exact_root_and_fallback_mappings() {
	assertEquals "Non-trailing-slash mapping should fall back to the destination root when a dataset does not extend the initial source path." \
		"backup/dst/src" "$(zxfer_get_destination_dataset_for_source_dataset "otherpool/unrelated")"

	g_initial_source_had_trailing_slash=1
	assertEquals "Trailing-slash mapping should keep the destination root unchanged for the exact source dataset." \
		"backup/dst" "$(zxfer_get_destination_dataset_for_source_dataset "tank/src")"
}

test_zxfer_write_snapshot_identity_file_from_records_normalizes_and_sorts_identities() {
	output_file="$TEST_TMPDIR/snapshot_identities.txt"

	zxfer_write_snapshot_identity_file_from_records \
		"tank/src@snap2	222
tank/src@snap1	111
tank/src@snap2	222" \
		"$output_file"

	assertEquals "Snapshot identity file generation should normalize, deduplicate, and sort the extracted identities." \
		"snap1	111
snap2	222" "$(cat "$output_file")"
}

test_zxfer_should_inline_source_snapshot_dataset_list_rejects_large_inputs() {
	large_dataset_list=$(awk 'BEGIN { for (i = 1; i <= 80; i++) print "tank/src/child" }')

	assertTrue "Moderate dataset lists should stay inline to avoid a second discovery pass." \
		"zxfer_should_inline_source_snapshot_dataset_list 'tank/src
tank/src/child' 2"
	assertFalse "Very large dataset lists should fall back to a streamed dataset enumeration command." \
		"zxfer_should_inline_source_snapshot_dataset_list '$large_dataset_list' 80"
}

test_zxfer_should_inline_source_snapshot_dataset_list_rejects_invalid_numeric_inputs_in_current_shell() {
	set +e
	zxfer_should_inline_source_snapshot_dataset_list "tank/src" "not-a-number"
	invalid_count_status=$?

	(
		wc() {
			printf '%s\n' "not-a-number"
		}
		zxfer_should_inline_source_snapshot_dataset_list "tank/src" 1
	)
	invalid_size_status=$?

	assertEquals "Inline dataset-list selection should reject non-numeric dataset counts." \
		1 "$invalid_count_status"
	assertEquals "Inline dataset-list selection should reject non-numeric byte counts." \
		1 "$invalid_size_status"
}

test_build_source_snapshot_list_cmd_reports_adaptive_helper_failures_in_current_shell() {
	g_option_j_jobs=2
	output_file="$TEST_TMPDIR/source_snapshot_cmd.out"

	set +e
	(
		zxfer_get_source_snapshot_parallel_dataset_threshold() {
			return 1
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
	)
	threshold_status=$?
	threshold_output=$(cat "$output_file")

	(
		zxfer_get_source_snapshot_parallel_dataset_threshold() {
			printf '%s\n' "8"
		}
		zxfer_get_source_snapshot_discovery_dataset_list() {
			return 1
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
	)
	dataset_list_status=$?
	dataset_list_output=$(cat "$output_file")

	(
		zxfer_get_source_snapshot_parallel_dataset_threshold() {
			printf '%s\n' "8"
		}
		zxfer_get_source_snapshot_discovery_dataset_list() {
			printf '%s\n' "tank/src"
		}
		zxfer_count_source_snapshot_discovery_datasets() {
			return 1
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
	)
	dataset_count_status=$?
	dataset_count_output=$(cat "$output_file")

	assertEquals "Adaptive source snapshot command construction should fail when the discovery threshold cannot be determined." \
		1 "$threshold_status"
	assertContains "Threshold failures should report the adaptive discovery threshold error." \
		"$threshold_output" "Failed to determine the adaptive source snapshot discovery threshold."
	assertEquals "Adaptive source snapshot command construction should fail when the dataset prepass cannot be retrieved." \
		1 "$dataset_list_status"
	assertContains "Dataset-list failures should report the adaptive discovery dataset error." \
		"$dataset_list_output" "Failed to retrieve the source dataset list for adaptive snapshot discovery."
	assertEquals "Adaptive source snapshot command construction should fail when the dataset prepass cannot be counted." \
		1 "$dataset_count_status"
	assertContains "Dataset-count failures should report the adaptive discovery counting error." \
		"$dataset_count_output" "Failed to count source datasets for adaptive snapshot discovery."
}

test_ensure_parallel_available_for_source_jobs_requires_local_parallel() {
	set +e
	output=$(
		(
			g_option_j_jobs=2
			g_cmd_parallel=""
			zxfer_ensure_parallel_available_for_source_jobs
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
			zxfer_ensure_parallel_available_for_source_jobs
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
			create_fake_ssh_handshake_bin "$ssh_bin" 1
			g_cmd_ssh="$ssh_bin"
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_origin_parallel_cmd=""

			zxfer_ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Missing remote GNU parallel should fail source-job setup." 1 "$status"
	assertContains "The remote-missing error should identify the origin host." \
		"$output" "GNU parallel not found on origin host origin.example"
}

test_ensure_parallel_available_for_source_jobs_returns_success_when_parallel_is_not_requested() {
	g_option_j_jobs=1
	g_cmd_parallel=""
	g_origin_parallel_cmd=""

	zxfer_ensure_parallel_available_for_source_jobs
	status=$?

	assertEquals "Serial snapshot listing should not require GNU parallel." 0 "$status"
	assertEquals "Serial snapshot listing should leave the remote parallel path unset." "" "$g_origin_parallel_cmd"
}

test_ensure_parallel_available_for_source_jobs_skips_local_parallel_for_remote_runs() {
	g_option_j_jobs=2
	g_cmd_parallel=""
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd="/opt/bin/parallel"

	zxfer_ensure_parallel_available_for_source_jobs
	status=$?

	assertEquals "Remote source-job setup should not require a local GNU parallel binary when only the origin-host branch will execute it." \
		0 "$status"
}

test_build_source_snapshot_list_cmd_requires_local_parallel_before_dataset_prepass() {
	g_option_j_jobs=2
	g_cmd_parallel=""
	g_option_O_origin_host=""

	set +e
	result=$(zxfer_build_source_snapshot_list_cmd 2>&1)
	status=$?
	set -e

	assertEquals "Local -j runs should fail fast when GNU parallel is unavailable." 1 "$status"
	assertContains "The local missing-parallel error should surface before adaptive dataset discovery runs." \
		"$result" "requires GNU parallel but it was not found in PATH on the local host"
}

test_build_source_snapshot_list_cmd_uses_serial_local_discovery_below_threshold() {
	g_option_j_jobs=2
	g_cmd_parallel="$GNU_PARALLEL_BIN"
	g_option_O_origin_host=""

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 9
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertEquals "Small local trees should stay on the direct recursive snapshot listing path even when -j is set." \
		"'$g_LZFS' 'list' '-Hr' '-o' 'name' '-s' 'creation' '-t' 'snapshot' '$g_initial_source'" "$result"
}

test_build_source_snapshot_list_cmd_uses_parallel_local_discovery_at_threshold() {
	g_option_j_jobs=2
	g_cmd_parallel="$GNU_PARALLEL_BIN"
	g_option_O_origin_host=""

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
				printf '%s\n' "tank/src/child"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Parallel local discovery should inline the moderate dataset list instead of recounting it inside the shell command." \
		"$result" "'printf'"
	assertContains "Parallel local discovery should preserve the prefetched dataset order inside the inline list." \
		"$result" "'tank/src' 'tank/src/child'"
	assertContains "Parallel local discovery should still use GNU parallel once the threshold is met." \
		"$result" "'$g_cmd_parallel' -j 2 --line-buffer"
	assertNotContains "Parallel local discovery should no longer defer the branch decision into a shell-side dataset counter." \
		"$result" "l_dataset_count"
}

test_build_source_snapshot_list_cmd_uses_parallel_local_discovery_with_streamed_dataset_list() {
	g_option_j_jobs=2
	g_cmd_parallel="$GNU_PARALLEL_BIN"
	g_option_O_origin_host=""
	large_dataset_list=$(awk 'BEGIN { for (i = 1; i <= 65; i++) print "tank/src/child" i }')

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "$large_dataset_list"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertNotContains "Large local trees should not inline the prefetched dataset list into the parallel command." \
		"$result" "'printf'"
	assertContains "Large local trees should fall back to the streamed dataset enumeration command." \
		"$result" "'$g_LZFS' 'list' '-Hr' '-t' 'filesystem,volume' '-o' 'name' '$g_initial_source'"
	assertContains "Large local trees should still run GNU parallel once the adaptive threshold is met." \
		"$result" "'$g_cmd_parallel' -j 2 --line-buffer"
}

test_build_source_snapshot_list_cmd_uses_serial_remote_discovery_below_threshold_without_parallel_validation() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=1
	g_cmd_parallel=""
	g_origin_parallel_cmd=""
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 6
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
			}
			zxfer_build_source_snapshot_list_cmd
			printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}"
			printf 'skip=%s\n' "${g_source_snapshot_list_skipped_metadata_compression:-0}"
		)
	)

	assertContains "Small remote trees should stay on the recursive remote snapshot listing path." \
		"$result" "/remote/bin/zfs"
	assertNotContains "Remote serial discovery should now skip metadata compression on multi-job runs." \
		"$result" "/remote/bin/zstd"
	assertNotContains "Remote serial discovery should not add local decompression when metadata compression is skipped." \
		"$result" "/local/bin/zstd"
	assertNotContains "Remote serial discovery should not require GNU parallel when the serial path wins." \
		"$result" "parallel"
	assertContains "Adaptive remote serial discovery should record that metadata compression was intentionally skipped." \
		"$result" "meta=0"
	assertContains "Adaptive remote serial discovery should record the explicit metadata-compression skip state." \
		"$result" "skip=1"
}

test_build_source_snapshot_list_cmd_uses_serial_remote_discovery_with_metadata_compression_when_skip_is_disabled() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=1
	g_cmd_parallel=""
	g_origin_parallel_cmd=""
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 6
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
			}
			zxfer_should_skip_remote_snapshot_discovery_compression() {
				return 1
			}
			zxfer_build_source_snapshot_list_cmd
			printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}"
			printf 'skip=%s\n' "${g_source_snapshot_list_skipped_metadata_compression:-0}"
		)
	)

	assertContains "Remote serial discovery should append the resolved remote metadata compressor when the adaptive skip path is disabled." \
		"$result" "/remote/bin/zstd"
	assertContains "Remote serial discovery should append the resolved local metadata decompressor when metadata compression is enabled." \
		"$result" "/local/bin/zstd"
	assertContains "Remote serial discovery should record that metadata compression was used." \
		"$result" "meta=1"
	assertContains "Remote serial discovery with active metadata compression should leave the skip flag clear." \
		"$result" "skip=0"
}

test_build_source_snapshot_list_cmd_uses_serial_remote_discovery_with_metadata_compression_in_current_shell() {
	output_file="$TEST_TMPDIR/source_snapshot_cmd_remote_serial_current_shell.out"
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=1
	g_cmd_parallel=""
	g_origin_parallel_cmd=""
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"

	(
		zxfer_get_source_snapshot_parallel_dataset_threshold() {
			printf '%s\n' 6
		}
		zxfer_get_source_snapshot_discovery_dataset_list() {
			printf '%s\n' "tank/src"
		}
		zxfer_should_skip_remote_snapshot_discovery_compression() {
			return 1
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
		printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}" >>"$output_file"
	)

	assertContains "Direct serial remote discovery should still build the remote zfs listing command." \
		"$(cat "$output_file")" "/remote/bin/zfs"
	assertContains "Direct serial remote discovery should still append the resolved local decompressor when metadata compression is enabled." \
		"$(cat "$output_file")" "/local/bin/zstd"
	assertContains "Direct serial remote discovery should still record active metadata compression." \
		"$(cat "$output_file")" "meta=1"
}

test_build_source_snapshot_list_cmd_uses_parallel_remote_discovery_with_streamed_dataset_list() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=0
	g_cmd_parallel=""
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_cmd_zfs="/remote/bin/zfs"
	large_dataset_list=$(awk 'BEGIN { for (i = 1; i <= 65; i++) print "tank/src/child" i }')

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "$large_dataset_list"
			}
			zxfer_build_source_snapshot_list_cmd
			printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}"
			printf 'skip=%s\n' "${g_source_snapshot_list_skipped_metadata_compression:-0}"
		)
	)

	assertNotContains "Large remote trees should not inline the dataset list when the streamed remote branch wins." \
		"$result" "'printf'"
	assertNotContains "Large remote trees should not embed the prefetched dataset list inside the remote command string when the streamed branch wins." \
		"$result" "tank/src/child65"
	assertContains "Large remote trees should still use GNU parallel on the origin host." \
		"$result" "/opt/bin/parallel"
	assertContains "Remote discovery without -z should keep metadata compression disabled." \
		"$result" "meta=0"
	assertContains "Remote discovery without -z should not mark the adaptive metadata-compression skip state." \
		"$result" "skip=0"
}

test_build_source_snapshot_list_cmd_uses_parallel_remote_discovery_with_streamed_dataset_list_in_current_shell() {
	output_file="$TEST_TMPDIR/source_snapshot_cmd_remote_parallel_current_shell.out"
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=0
	g_cmd_parallel=""
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_cmd_zfs="/remote/bin/zfs"
	large_dataset_list=$(awk 'BEGIN { for (i = 1; i <= 65; i++) print "tank/src/child" i }')

	(
		zxfer_get_source_snapshot_parallel_dataset_threshold() {
			printf '%s\n' 2
		}
		zxfer_get_source_snapshot_discovery_dataset_list() {
			printf '%s\n' "$large_dataset_list"
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
	)

	assertContains "Direct parallel remote discovery should still use GNU parallel on the origin host." \
		"$(cat "$output_file")" "/opt/bin/parallel"
	assertContains "Direct parallel remote discovery should fall back to the streamed remote dataset prepass for large trees." \
		"$(cat "$output_file")" "filesystem,volume"
	assertContains "Direct parallel remote discovery should still enumerate the configured recursive source root when the streamed prepass wins." \
		"$(cat "$output_file")" "tank/src"
}

test_build_source_snapshot_list_cmd_uses_parallel_remote_discovery_without_metadata_compression() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=1
	g_cmd_parallel=""
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
				printf '%s\n' "tank/src/child"
			}
			zxfer_build_source_snapshot_list_cmd
			printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}"
			printf 'skip=%s\n' "${g_source_snapshot_list_skipped_metadata_compression:-0}"
		)
	)

	assertContains "Adaptive remote source discovery should retain the remote GNU parallel path for larger trees." \
		"$result" "/opt/bin/parallel"
	assertContains "Adaptive remote source discovery should inline the prefetched dataset list for moderate trees." \
		"$result" "'printf'"
	assertContains "Adaptive remote source discovery should include the first prefetched dataset in the inline list." \
		"$result" "tank/src"
	assertContains "Adaptive remote source discovery should include the second prefetched dataset in the inline list." \
		"$result" "tank/src/child"
	assertNotContains "Adaptive remote source discovery should skip metadata compression on multi-job remote runs." \
		"$result" "/remote/bin/zstd"
	assertNotContains "Adaptive remote source discovery should not add local decompression when metadata compression is skipped." \
		"$result" "/local/bin/zstd"
	assertContains "Adaptive remote source discovery should keep the metadata-compression flag clear when compression is skipped." \
		"$result" "meta=0"
	assertContains "Adaptive remote source discovery should mark the explicit metadata-compression skip state." \
		"$result" "skip=1"
}

test_build_source_snapshot_list_cmd_uses_parallel_remote_discovery_with_metadata_compression_when_skip_is_disabled() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=1
	g_cmd_parallel=""
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"

	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
				printf '%s\n' "tank/src/child"
			}
			zxfer_should_skip_remote_snapshot_discovery_compression() {
				return 1
			}
			zxfer_build_source_snapshot_list_cmd
			printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}"
			printf 'skip=%s\n' "${g_source_snapshot_list_skipped_metadata_compression:-0}"
		)
	)

	assertContains "Parallel remote discovery should append the resolved remote metadata compressor when the adaptive skip path is disabled." \
		"$result" "/remote/bin/zstd"
	assertContains "Parallel remote discovery should append the resolved local metadata decompressor when metadata compression is enabled." \
		"$result" "/local/bin/zstd"
	assertContains "Parallel remote discovery with active metadata compression should record the compressor flag." \
		"$result" "meta=1"
	assertContains "Parallel remote discovery with active metadata compression should leave the skip flag clear." \
		"$result" "skip=0"
}

test_zxfer_should_skip_remote_snapshot_discovery_compression_requires_remote_multi_job_compressed_run() {
	g_option_z_compress=1
	g_option_O_origin_host="origin.example"
	g_option_j_jobs=2

	set +e
	zxfer_should_skip_remote_snapshot_discovery_compression
	status=$?
	set -e

	assertEquals "Remote multi-job discovery should skip metadata compression." \
		0 "$status"

	set +e
	g_option_j_jobs=1
	zxfer_should_skip_remote_snapshot_discovery_compression
	status=$?
	set -e

	assertEquals "Single-job discovery should not claim the adaptive remote skip path." \
		1 "$status"

	set +e
	g_option_j_jobs=2
	g_option_O_origin_host=""
	zxfer_should_skip_remote_snapshot_discovery_compression
	status=$?
	set -e

	assertEquals "Local discovery should not use the remote metadata compression skip path." \
		1 "$status"

	set +e
	g_option_O_origin_host="origin.example"
	g_option_z_compress=0
	zxfer_should_skip_remote_snapshot_discovery_compression
	status=$?
	set -e

	assertEquals "Discovery should not skip metadata compression when -z is disabled." \
		1 "$status"
}

test_zxfer_should_skip_remote_snapshot_discovery_compression_treats_invalid_jobs_as_single_job_in_current_shell() {
	g_option_z_compress=1
	g_option_O_origin_host="origin.example"
	g_option_j_jobs="invalid"

	set +e
	zxfer_should_skip_remote_snapshot_discovery_compression
	status=$?
	set -e

	assertEquals "Invalid job counts should fall back to single-job behavior and avoid the remote metadata-compression skip path." \
		1 "$status"
}

test_build_source_snapshot_list_cmd_reports_remote_parallel_validation_failures_after_threshold() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd=""
	g_cmd_parallel=""

	set +e
	result=$(
		(
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
				printf '%s\n' "tank/src/child"
			}
			zxfer_ensure_parallel_available_for_source_jobs() {
				printf '%s\n' "remote parallel validation failed"
				return 1
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?
	set -e

	assertEquals "Remote adaptive discovery should abort when origin-host GNU parallel validation fails after the dataset prepass." \
		1 "$status"
	assertContains "Remote adaptive discovery should preserve the origin-host GNU parallel validation failure." \
		"$result" "remote parallel validation failed"
}

test_write_source_snapshot_list_to_file_uses_execute_background_cmd_when_serial() {
	log="$TEST_TMPDIR/source_serial.log"
	outfile="$TEST_TMPDIR/source_serial.out"
	errfile="$TEST_TMPDIR/source_serial.err"
	: >"$log"

	(
		SOURCE_LOG="$log"
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-serial'"
		}
		zxfer_execute_background_cmd() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$SOURCE_LOG"
			g_last_background_pid=4242
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		printf '%s\n' "$g_source_snapshot_list_pid" >>"$SOURCE_LOG"
	)

	assertEquals "Serial snapshot listing should delegate to zxfer_execute_background_cmd." \
		"printf 'snap-serial'|$outfile|$errfile
4242" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_tracks_profile_counters_when_very_verbose() {
	log="$TEST_TMPDIR/source_profile.log"
	outfile="$TEST_TMPDIR/source_profile.out"
	errfile="$TEST_TMPDIR/source_profile.err"
	: >"$log"

	(
		zxfer_echoV() {
			:
		}
		zxfer_build_source_snapshot_list_cmd() {
			g_source_snapshot_list_uses_parallel=1
			printf '%s\n' "printf 'snap-profile'"
		}
		g_option_V_very_verbose=1
		g_option_j_jobs=2
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		wait "$g_source_snapshot_list_pid"
		printf '%s\n' "$(cat "$outfile")" >"$log"
		{
			printf 'commands=%s\n' "${g_zxfer_profile_source_snapshot_list_commands:-0}"
			printf 'parallel=%s\n' "${g_zxfer_profile_source_snapshot_list_parallel_commands:-0}"
			printf 'bucket=%s\n' "${g_zxfer_profile_bucket_source_inspection:-0}"
		} >>"$log"
	)

	assertEquals "Very-verbose profiling should track source snapshot list command counts." \
		"snap-profile
commands=1
parallel=1
bucket=1" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_tracks_remote_ssh_profile_counter_when_very_verbose() {
	log="$TEST_TMPDIR/source_remote_profile.log"
	outfile="$TEST_TMPDIR/source_remote_profile.out"
	errfile="$TEST_TMPDIR/source_remote_profile.err"
	: >"$log"

	(
		zxfer_echoV() {
			:
		}
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'remote-snap-profile'"
		}
		zxfer_execute_background_cmd() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >"$log"
			g_last_background_pid=3131
		}
		g_option_V_very_verbose=1
		g_option_j_jobs=1
		g_option_O_origin_host="origin.example"
		g_zxfer_profile_ssh_shell_invocations=0
		g_zxfer_profile_source_ssh_shell_invocations=0
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		{
			printf 'pid=%s\n' "$g_source_snapshot_list_pid"
			printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
			printf 'source_ssh=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
		} >>"$log"
	)

	assertEquals "Very-verbose profiling should count the remote ssh hop used for source snapshot discovery." \
		"printf 'remote-snap-profile'|$outfile|$errfile
pid=3131
ssh=1
source_ssh=1" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_logs_when_remote_metadata_compression_is_skipped() {
	log="$TEST_TMPDIR/source_remote_skip_compress.log"
	outfile="$TEST_TMPDIR/source_remote_skip_compress.out"
	errfile="$TEST_TMPDIR/source_remote_skip_compress.err"
	: >"$log"

	(
		zxfer_echoV() {
			printf '%s\n' "$*" >>"$log"
		}
		zxfer_build_source_snapshot_list_cmd() {
			g_source_snapshot_list_skipped_metadata_compression=1
			printf '%s\n' "printf 'remote-snap-profile'"
		}
		zxfer_execute_background_cmd() {
			printf '%s\n' "run" >>"$log"
			g_last_background_pid=3131
		}
		g_option_V_very_verbose=1
		g_option_j_jobs=2
		g_option_z_compress=1
		g_option_O_origin_host="origin.example"
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
	)

	assertContains "Very-verbose remote discovery should explain when adaptive metadata compression is skipped." \
		"$(cat "$log")" "Skipping remote source snapshot-list compression for adaptive metadata discovery."
}

test_write_source_snapshot_list_to_file_does_not_log_metadata_skip_for_single_job_remote_listing() {
	log="$TEST_TMPDIR/source_remote_no_skip.log"
	outfile="$TEST_TMPDIR/source_remote_no_skip.out"
	errfile="$TEST_TMPDIR/source_remote_no_skip.err"
	: >"$log"

	(
		zxfer_echoV() {
			printf '%s\n' "$*" >>"$log"
		}
		zxfer_build_source_snapshot_list_cmd() {
			g_source_snapshot_list_skipped_metadata_compression=0
			printf '%s\n' "printf 'remote-snap-profile'"
		}
		zxfer_execute_background_cmd() {
			printf '%s\n' "run" >>"$log"
			g_last_background_pid=3131
		}
		g_option_V_very_verbose=1
		g_option_j_jobs=1
		g_option_z_compress=1
		g_option_O_origin_host="origin.example"
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
	)

	assertNotContains "Single-job remote listing should not log the adaptive metadata-compression skip message." \
		"$(cat "$log")" "Skipping remote source snapshot-list compression for adaptive metadata discovery."
}

test_write_source_snapshot_list_to_file_backgrounds_parallel_command() {
	outfile="$TEST_TMPDIR/source_parallel.out"
	lastcmd_file="$TEST_TMPDIR/source_parallel.lastcmd"
	g_option_j_jobs=3

	(
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-parallel'"
		}
		zxfer_record_last_command_string() {
			printf '%s\n' "$1" >"$lastcmd_file"
		}
		zxfer_write_source_snapshot_list_to_file "$outfile"
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
			zxfer_get_source_snapshot_parallel_dataset_threshold() {
				printf '%s\n' 2
			}
			zxfer_get_source_snapshot_discovery_dataset_list() {
				printf '%s\n' "tank/src"
				printf '%s\n' "tank/src/child"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_source_snapshot_list_to_file "$TEST_TMPDIR/source_parallel_error.out"
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_diff_snapshot_lists "$source_file" "$dest_file" "unknown_mode"
		)
	)
	status=$?

	assertEquals "Unknown diff modes should abort." 1 "$status"
	assertContains "Unknown diff modes should include the requested mode." \
		"$output" "Unknown snapshot diff mode: unknown_mode"
}

test_reverse_numbered_line_stream_preserves_full_payload_for_seven_digit_line_numbers() {
	output=$(
		cat <<'EOF' | zxfer_reverse_numbered_line_stream
     2	tank/src@snap-b
1000000	tank/src@snap-million
     1	tank/src@snap-a
EOF
	)

	assertEquals "Reversing numbered snapshot listings should preserve the full payload once line numbers grow past six digits." \
		"tank/src@snap-million
tank/src@snap-b
tank/src@snap-a" "$output"
}

test_reverse_numbered_line_stream_uses_linear_reverse_for_monotonic_numbered_input() {
	output=$(
		(
			zxfer_reverse_numbered_file_lines_with_sort() {
				printf '%s\n' "sort-fallback-should-not-run"
				return 23
			}
			cat <<'EOF' | zxfer_reverse_numbered_line_stream
     1	tank/src@snap-a
     2	tank/src@snap-b
1000000	tank/src@snap-million
EOF
		)
	)

	assertEquals "Monotonic numbered input should use the linear reverse path instead of the sort fallback." \
		"tank/src@snap-million
tank/src@snap-b
tank/src@snap-a" "$output"
}

test_reverse_numbered_line_stream_preserves_full_payload_when_falling_back_to_sort() {
	output=$(
		(
			g_zxfer_linear_reverse_max_lines=1
			cat <<'EOF' | zxfer_reverse_numbered_line_stream
     2	tank/src@snap-b
1000000	tank/src@snap-million
     1	tank/src@snap-a
EOF
		)
	)

	assertEquals "The large-input fallback should preserve the full payload once line numbers grow past six digits." \
		"tank/src@snap-million
tank/src@snap-b
tank/src@snap-a" "$output"
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
	output=$(
		(
			g_zxfer_linear_reverse_max_lines=1
			zxfer_reverse_file_lines "$input_file"
		)
	)

	assertEquals "zxfer_reverse_file_lines should retain the sort-based fallback for larger inputs to avoid unbounded awk memory growth." \
		"tank/src@snap-c
tank/src@snap-million
tank/src@snap-b
tank/src@snap-a" "$output"
}

test_zxfer_should_use_linear_reverse_for_file_rejects_non_numeric_threshold() {
	input_file="$TEST_TMPDIR/reverse_threshold_input.txt"
	printf '%s\n' "tank/src@snap-a" >"$input_file"

	output=$(
		(
			g_zxfer_linear_reverse_max_lines="bogus"
			zxfer_should_use_linear_reverse_for_file "$input_file"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "Non-numeric reverse thresholds should disable the linear awk fast path." \
		"status=1" "$output"
}

test_zxfer_should_use_linear_reverse_for_file_rejects_non_numeric_wc_output() {
	input_file="$TEST_TMPDIR/reverse_wc_input.txt"
	printf '%s\n' "tank/src@snap-a" >"$input_file"

	output=$(
		(
			wc() {
				printf '%s\n' "bogus"
			}
			zxfer_should_use_linear_reverse_for_file "$input_file"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "Invalid line-count output should disable the linear awk fast path." \
		"status=1" "$output"
}

test_reverse_numbered_line_stream_returns_failure_when_buffering_fails() {
	output=$(
		(
			cat() {
				return 1
			}
			printf '%s\n' "     1	tank/src@snap-a" | zxfer_reverse_numbered_line_stream
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "zxfer_reverse_numbered_line_stream should fail cleanly when it cannot buffer stdin." \
		"status=1" "$output"
}

test_zxfer_reverse_plain_file_lines_with_sort_returns_failure_when_numbering_fails() {
	input_file="$TEST_TMPDIR/reverse_sort_failure_input.txt"
	printf '%s\n' "tank/src@snap-a" >"$input_file"

	output=$(
		(
			cat() {
				if [ "$1" = "-n" ]; then
					return 1
				fi
				command cat "$@"
			}
			zxfer_reverse_plain_file_lines_with_sort "$input_file"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "The sort fallback should fail cleanly when numbering the file fails." \
		"status=1" "$output"
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

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$verbose_file" 2>&1
	output=$(cat "$verbose_file")

	assertEquals "Excluded datasets should be removed from the transfer list." "tank/src" "$g_recursive_source_list"
	assertEquals "Excluded datasets should also be removed from the dataset cache." "tank/src" "$g_recursive_source_dataset_list"
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

test_write_destination_snapshot_list_to_files_outputs_empty_when_destination_missing() {
	full_file="$TEST_TMPDIR/dest_missing_full.txt"
	norm_file="$TEST_TMPDIR/dest_missing_norm.txt"

	(
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	)

	assertEquals "Missing destination datasets should yield an empty raw snapshot file." "" "$(cat "$full_file")"
	assertEquals "Missing destination datasets should yield an empty normalized snapshot file." "" "$(cat "$norm_file")"
}

test_write_destination_snapshot_list_to_files_reports_destination_probe_failures() {
	full_file="$TEST_TMPDIR/dest_probe_fail_full.txt"
	norm_file="$TEST_TMPDIR/dest_probe_fail_norm.txt"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/dst/src] exists: permission denied"
				return 1
			}
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

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_record_last_command_string() {
				:
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "ssh timeout" >&2
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
		)
	)
	status=$?

	assertEquals "Destination snapshot discovery should abort when listing snapshots fails." 1 "$status"
	assertContains "Destination snapshot listing failures should surface the generic destination snapshot-list error." \
		"$output" "Failed to retrieve snapshot list from the destination."
}

test_write_destination_snapshot_list_to_files_uses_destination_root_for_trailing_slash_sources() {
	full_file="$TEST_TMPDIR/dest_trailing_existing_full.txt"
	norm_file="$TEST_TMPDIR/dest_trailing_existing_norm.txt"
	arg_file="$TEST_TMPDIR/dest_trailing_existing_arg.txt"
	cmd_file="$TEST_TMPDIR/dest_trailing_existing_cmd.txt"
	g_initial_source_had_trailing_slash=1
	g_initial_source="tank/src"
	g_destination="backup/dst"

	(
		zxfer_exists_destination() {
			printf '%s\n' "$1" >"$arg_file"
			printf '%s\n' 1
		}
		zxfer_record_last_command_string() {
			:
		}
		# shellcheck disable=SC2317,SC2329  # Invoked indirectly via g_RZFS in eval-built test command.
		fake_rzfs() {
			printf '%s\n' "$*" >"$cmd_file"
			printf '%s\n' "backup/dst/child@snap2" "backup/dst@snap1"
		}
		g_RZFS="fake_rzfs"
		zxfer_write_destination_snapshot_list_to_files "$full_file" "$norm_file"
	)

	assertEquals "Trailing-slash replication should probe the destination root dataset directly." \
		"backup/dst" "$(cat "$arg_file")"
	assertContains "Trailing-slash replication should list snapshots from the destination root dataset, not a child suffix." \
		"$(cat "$cmd_file")" "snapshot backup/dst"
	assertNotContains "Trailing-slash replication should not append the source basename to the destination root." \
		"$(cat "$cmd_file")" "backup/dst/src"
	assertEquals "Trailing-slash replication should leave normalized destination snapshots rooted at the destination dataset." \
		"backup/dst/child@snap2
backup/dst@snap1" "$(cat "$norm_file")"
}

test_normalize_destination_snapshot_list_preserves_destination_when_trailing_slash_requested() {
	input_file="$TEST_TMPDIR/dest_trailing_input.txt"
	output_file="$TEST_TMPDIR/dest_trailing_output.txt"
	g_initial_source_had_trailing_slash=1
	g_initial_source="tank/src"
	cat <<'EOF' >"$input_file"
backup/dst/child@snap2
backup/dst@snap1
EOF

	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"

	assertEquals "Trailing-slash destinations should be sorted without source-prefix rewriting." \
		"backup/dst/child@snap2
backup/dst@snap1" "$(cat "$output_file")"
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
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@a	111" "tank/src@b	222"
					return 0
				fi
				if [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@a	111" "backup/dst/src@b	222"
					return 0
				fi
				return 1
			}

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
	assertContains "Verbose mode should explain when no new snapshots need transfer." \
		"$(cat "$output_file")" "No new snapshots to transfer."
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

	zxfer_get_snapshot_identity_records_for_dataset() {
		if [ "$1:$2" = "source:tank/src" ]; then
			printf '%s\n' "tank/src@a	111"
			return 0
		fi
		if [ "$1:$2" = "destination:backup/dst/src" ]; then
			printf '%s\n' "backup/dst/src@a	111"
			return 0
		fi
		return 1
	}

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"

	assertEquals "Destination-only snapshot datasets should be tracked separately for delete-only inspection." \
		"tank/src/child" "$g_recursive_destination_extra_dataset_list"
	unset -f zxfer_get_snapshot_identity_records_for_dataset
}

test_set_g_recursive_source_list_marks_guid_divergence_when_name_only_lists_match() {
	source_tmp="$TEST_TMPDIR/source_guid_divergence.txt"
	dest_tmp="$TEST_TMPDIR/dest_guid_divergence.txt"
	output_file="$TEST_TMPDIR/source_guid_divergence.out"
	cat <<'EOF' >"$source_tmp"
tank/src@same
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@same
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_x_exclude_datasets=""

	(
		zxfer_get_snapshot_identity_records_for_dataset() {
			case "$1:$2" in
			source:tank/src)
				printf '%s\n' "tank/src@same	111"
				;;
			destination:backup/dst/src)
				printf '%s\n' "backup/dst/src@same	999"
				;;
			*)
				return 1
				;;
			esac
		}

		zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		printf 'source=%s\n' "$g_recursive_source_list"
		printf 'dest=%s\n' "$g_recursive_destination_extra_dataset_list"
	) >"$output_file"

	assertContains "Datasets that only differ by guid should still be marked for transfer after lazy identity validation." \
		"$(cat "$output_file")" "source=tank/src"
	assertContains "Datasets that only differ by guid should still be marked for delete inspection after lazy identity validation." \
		"$(cat "$output_file")" "dest=tank/src"
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_updates_lists_in_current_shell() {
	source_tmp="$TEST_TMPDIR/refine_guid_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_guid_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_recursive_source_list=""
	g_recursive_destination_extra_dataset_list=""

	zxfer_get_snapshot_identity_records_for_dataset() {
		case "$1:$2" in
		source:tank/src)
			printf '%s\n' "tank/src@same	111"
			;;
		destination:backup/dst/src)
			printf '%s\n' "backup/dst/src@same	999"
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"

	assertEquals "Lazy identity refinement should add guid-divergent datasets back into the source-delta list in the current shell." \
		"tank/src" "$g_recursive_source_list"
	assertEquals "Lazy identity refinement should add guid-divergent datasets back into the destination-extra list in the current shell." \
		"tank/src" "$g_recursive_destination_extra_dataset_list"
	unset -f zxfer_get_snapshot_identity_records_for_dataset
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_identity_probe_failures() {
	source_tmp="$TEST_TMPDIR/refine_guid_error_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_guid_error_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				return 1
			}

			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when a source identity probe fails." 1 "$status"
	assertContains "Source identity probe failures should surface the dataset that could not be validated." \
		"$output" "Failed to retrieve source snapshot identities for [tank/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_destination_identity_probe_failures() {
	source_tmp="$TEST_TMPDIR/refine_guid_dest_error_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_guid_dest_error_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					return 1
				fi
			}

			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when a destination identity probe fails." 1 "$status"
	assertContains "Destination identity probe failures should surface the mapped destination dataset." \
		"$output" "Failed to retrieve destination snapshot identities for [backup/dst/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_merges_existing_lists_in_current_shell() {
	source_tmp="$TEST_TMPDIR/refine_guid_merge_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_guid_merge_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_recursive_source_list="tank/already"
	g_recursive_destination_extra_dataset_list="tank/extra"

	zxfer_get_snapshot_identity_records_for_dataset() {
		case "$1:$2" in
		source:tank/src)
			printf '%s\n' "tank/src@same	111"
			;;
		destination:backup/dst/src)
			printf '%s\n' "backup/dst/src@same	999"
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"

	assertEquals "Guid-divergent datasets should be merged into the existing source-delta list." \
		"tank/already
tank/src" "$g_recursive_source_list"
	assertEquals "Guid-divergent datasets should be merged into the existing destination-extra list." \
		"tank/extra
tank/src" "$g_recursive_destination_extra_dataset_list"
	unset -f zxfer_get_snapshot_identity_records_for_dataset
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

test_get_zfs_list_bootstraps_missing_destination_dataset_when_pool_exists() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_get_zfs_list.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/zxfer_get_zfs_list.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snapA
tank/src@snapB
EOF
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
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'source_reversed_before=%s\n' "${g_lzfs_list_hr_S_snap:-}"
			printf 'source=%s\n' "$(zxfer_get_snapshot_records_for_dataset source "tank/src")"
		)
	)

	assertContains "Bootstrap path should treat the missing destination dataset as an empty recursive list." "$output" "dest="
	assertContains "Snapshot discovery should leave the reversed source cache unset until a later consumer asks for per-dataset records." \
		"$output" "source_reversed_before="
	assertContains "Per-dataset source lookups should still lazily return newest-first records for send planning." \
		"$output" "source=tank/src@snapB
tank/src@snapA"
}

test_get_zfs_list_bootstraps_missing_destination_dataset_when_omnios_reports_no_such_pool_or_dataset() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_get_zfs_list_omnios.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/zxfer_get_zfs_list_omnios.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snapA
tank/src@snapB
EOF
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
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "cannot open 'backup/tank/src': no such pool or dataset" >&2
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "backup"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'source=%s\n' "$(zxfer_get_snapshot_records_for_dataset source "tank/src")"
		)
	)

	assertContains "OmniOS-style missing destination errors should still bootstrap the recursive destination list as empty." \
		"$output" "dest="
	assertContains "OmniOS-style destination bootstrap should still preserve the source snapshot planning list." \
		"$output" "source=tank/src@snapB
tank/src@snapA"
}

test_get_zfs_list_reports_pool_lookup_failure_when_destination_root_has_no_slash() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_list_root_missing.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_list_root_missing.$idx"
			}
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
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "dataset does not exist" >&2
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "pool lookup failed" >&2
					return 1
				fi
				return 1
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_destination="backup"
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Missing destination roots without a slash should still fail closed when the pool lookup fails." 1 "$status"
	assertContains "Destination-root lookup failures should preserve the documented dataset-list message." \
		"$output" "Failed to retrieve list of datasets from the destination"
}

test_get_zfs_list_seeds_destination_existence_cache_from_recursive_dataset_list() {
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
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_reverse_file_lines() {
				cat "$1"
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
			zxfer_get_zfs_list
			printf 'root=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'existing=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/existing")"
			printf 'missing=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/missing")"
		)
	)

	assertContains "Destination discovery should seed the root dataset into the existence cache." \
		"$output" "root=1"
	assertContains "Destination discovery should seed known descendants into the existence cache." \
		"$output" "existing=1"
	assertContains "Destination discovery should let later callers infer missing descendants without another probe." \
		"$output" "missing=0"
}

test_get_zfs_list_lazily_builds_per_dataset_snapshot_indexes() {
	output=$(
		(
			source_root_file="$TEST_TMPDIR/get_zfs_lazy_source_root.records"
			source_child_file="$TEST_TMPDIR/get_zfs_lazy_source_child.records"
			dest_root_file="$TEST_TMPDIR/get_zfs_lazy_dest_root.records"
			dest_child_file="$TEST_TMPDIR/get_zfs_lazy_dest_child.records"
			zxfer_write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snap1
tank/src/child@child1
tank/src@snap2
EOF
			}
			zxfer_write_destination_snapshot_list_to_files() {
				cat <<'EOF' >"$1"
backup/dst@snap2
backup/dst@legacy1
backup/dst/child@child1
EOF
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list=$(printf '%s\n%s' "tank/src" "tank/src/child")
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/child"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_ready_before=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready_before=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
			zxfer_get_snapshot_records_for_dataset source "tank/src" >"$source_root_file"
			zxfer_get_snapshot_records_for_dataset source "tank/src/child" >"$source_child_file"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst" >"$dest_root_file"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst/child" >"$dest_child_file"
			printf 'source_root=%s\n' "$(cat "$source_root_file")"
			printf 'source_child=%s\n' "$(cat "$source_child_file")"
			printf 'dest_root=%s\n' "$(cat "$dest_root_file")"
			printf 'dest_child=%s\n' "$(cat "$dest_child_file")"
			printf 'source_ready_after=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready_after=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
		)
	)

	assertContains "Snapshot discovery should leave the source per-dataset index unset until a consumer needs it." \
		"$output" "source_ready_before=0"
	assertContains "Snapshot discovery should leave the destination per-dataset index unset until a consumer needs it." \
		"$output" "dest_ready_before=0"
	assertContains "Snapshot discovery should cache newest-first source snapshots for the root dataset." \
		"$output" "source_root=tank/src@snap2
tank/src@snap1"
	assertContains "Snapshot discovery should cache source snapshots for child datasets separately." \
		"$output" "source_child=tank/src/child@child1"
	assertContains "Snapshot discovery should cache destination snapshots in live destination order." \
		"$output" "dest_root=backup/dst@snap2
backup/dst@legacy1"
	assertContains "Snapshot discovery should cache destination child snapshots separately." \
		"$output" "dest_child=backup/dst/child@child1"
	assertContains "The source per-dataset index should be built lazily on first lookup." \
		"$output" "source_ready_after=1"
	assertContains "The destination per-dataset index should be built lazily on first lookup." \
		"$output" "dest_ready_after=1"
}

test_get_zfs_list_tracks_stage_timings_when_very_verbose() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_profile.counter"
			now_counter_file="$TEST_TMPDIR/get_zfs_profile.now.counter"
			printf '%s\n' 0 >"$counter_file"
			printf '%s\n' 0 >"$now_counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_profile.$idx"
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 1000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 1500
				elif [ "$idx" = "3" ]; then
					printf '%s\n' 1900
				elif [ "$idx" = "4" ]; then
					printf '%s\n' 2600
				elif [ "$idx" = "5" ]; then
					printf '%s\n' 3000
				elif [ "$idx" = "6" ]; then
					printf '%s\n' 3550
				fi
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
			zxfer_build_snapshot_record_index() {
				:
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
				printf '%s\n' "$TEST_TMPDIR/get_zfs_empty.$idx"
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
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_fail.$idx"
			}
			zxfer_build_source_snapshot_list_cmd() {
				printf '%s\n' "sh -c 'printf \"%s\\n\" \"missing command\" >&2; exit 3'"
			}
			zxfer_exists_destination() {
				printf '%s\n' 0
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
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
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

test_get_zfs_list_reports_generic_source_failure_when_background_snapshot_listing_has_no_stderr() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_fail_blank.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/get_zfs_fail_blank.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				sh -c 'exit 1' &
				g_source_snapshot_list_pid=$!
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

	assertEquals "Background source snapshot failures without stderr should still keep exit status 3." 3 "$status"
	assertContains "Failure handling should still restore the last attempted source snapshot command." \
		"$output" "cmd=sh -c 'exit 1'"
	assertContains "Failure handling should fall back to the generic source snapshot retrieval error when stderr is empty." \
		"$output" "msg=Failed to retrieve snapshots from the source"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
