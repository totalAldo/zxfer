#!/bin/sh
# shellcheck shell=sh
# Source command production, parallel discovery, staged capture and status
# files, and producer execution cases for src/zxfer_snapshot_discovery.sh. The
# two discovery-state reset cases stay first: later cases read the producer
# state their reset leaves. Run by tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_zxfer_reset_snapshot_discovery_state_preserves_remote_parallel_state() {
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"
	g_zxfer_snapshot_discovery_file_read_result="printf 'snap'"
	g_zxfer_recursive_dataset_list_result="tank/src"
	g_zxfer_source_snapshot_record_cache_file="$g_zxfer_run_tmp_root/source_cache.raw"
	g_zxfer_destination_snapshot_record_cache_file="$g_zxfer_run_tmp_root/destination_cache.raw"
	g_zxfer_full_source_snapshot_sorted_file="$g_zxfer_run_tmp_root/source_sorted.raw"
	source_cache_file=$g_zxfer_source_snapshot_record_cache_file
	destination_cache_file=$g_zxfer_destination_snapshot_record_cache_file
	sorted_source_file=$g_zxfer_full_source_snapshot_sorted_file
	printf '%s\n' "tank/src@snap1" >"$g_zxfer_source_snapshot_record_cache_file"
	printf '%s\n' "backup/dst/src@snap1" >"$g_zxfer_destination_snapshot_record_cache_file"
	printf '%s\n' "tank/src@snap1" >"$g_zxfer_full_source_snapshot_sorted_file"
	zxfer_reset_snapshot_discovery_state

	assertEquals "Resetting snapshot discovery state should preserve the cached remote parallel helper path for later discovery passes in the same run." \
		"/opt/bin/parallel" "$g_origin_parallel_cmd"
	assertEquals "Resetting snapshot discovery state should preserve the host paired with the cached remote parallel helper path." \
		"origin.example" "$g_origin_parallel_cmd_host"
	assertEquals "Resetting snapshot discovery state should clear staged snapshot-discovery file-read scratch." \
		"" "$g_zxfer_snapshot_discovery_file_read_result"
	assertEquals "Resetting snapshot discovery state should clear recursive dataset-list scratch." \
		"" "$g_zxfer_recursive_dataset_list_result"
	assertEquals "Resetting snapshot discovery state should clear the staged source snapshot-record cache file path." \
		"" "${g_zxfer_source_snapshot_record_cache_file:-}"
	assertEquals "Resetting snapshot discovery state should clear the staged destination snapshot-record cache file path." \
		"" "${g_zxfer_destination_snapshot_record_cache_file:-}"
	assertEquals "Resetting snapshot discovery state should clear the staged sorted source snapshot file path." \
		"" "${g_zxfer_full_source_snapshot_sorted_file:-}"
	assertFalse "Resetting snapshot discovery state should remove the staged source snapshot-record cache file." \
		"[ -e '$source_cache_file' ]"
	assertFalse "Resetting snapshot discovery state should remove the staged destination snapshot-record cache file." \
		"[ -e '$destination_cache_file' ]"
	assertFalse "Resetting snapshot discovery state should remove the staged sorted source snapshot file." \
		"[ -e '$sorted_source_file' ]"
}

test_zxfer_reset_snapshot_discovery_state_preserves_remote_parallel_reuse_across_discovery_passes() {
	log_file="$TEST_TMPDIR/reset_snapshot_discovery_parallel_reuse.log"

	(
		LOG_FILE="$log_file"
		g_cmd_parallel=""
		g_option_j_jobs=4
		g_option_O_origin_host="origin.example"
		g_origin_parallel_cmd=""
		g_origin_parallel_cmd_host=""
		zxfer_resolve_remote_required_tool() {
			printf '%s\n' "resolve:$1" >>"$LOG_FILE"
			g_zxfer_required_tool_result="/opt/bin/parallel"
		}

		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		zxfer_reset_snapshot_discovery_state
		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		[ "$g_origin_parallel_cmd" = "/opt/bin/parallel" ] || exit 1
		[ "$g_origin_parallel_cmd_host" = "origin.example" ] || exit 1
	)
	status=$?

	assertEquals "Resetting snapshot discovery state should not force a second origin-host parallel resolution during the same zxfer run." \
		0 "$status"
	assertEquals "Resetting snapshot discovery state should preserve the cached remote helper so later discovery passes resolve it only once." \
		"1" "$(wc -l <"$log_file" | tr -d '[:space:]')"
}

test_zxfer_limit_snapshot_discovery_capture_lines_defaults_invalid_limits_in_current_shell() {
	output_file="$TEST_TMPDIR/snapshot_capture_limit.out"

	zxfer_limit_snapshot_discovery_capture_lines \
		"line1
line2
line3" "invalid" >"$output_file"

	assertEquals "Snapshot discovery stderr limiting should fall back to the default line limit when the requested limit is invalid." \
		"line1
line2
line3" "$(cat "$output_file")"
}

test_build_source_snapshot_list_cmd_reports_parallel_helper_failures_in_current_shell() {
	g_option_j_jobs=2
	output_file="$TEST_TMPDIR/source_snapshot_cmd.out"

	set +e
	(
		zxfer_ensure_parallel_available_for_source_jobs() {
			g_zxfer_parallel_source_job_check_result="parallel unavailable"
			return 1
		}
		zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd >"$output_file"
	)
	reason_status=$?
	reason_output=$(cat "$output_file")

	(
		zxfer_ensure_parallel_available_for_source_jobs() {
			return 1
		}
		zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd >"$output_file"
	)
	generic_status=$?
	generic_output=$(cat "$output_file")

	assertEquals "Parallel source snapshot command construction should fail when parallel setup fails." \
		1 "$reason_status"
	assertContains "Parallel source snapshot command construction should preserve the staged parallel failure reason." \
		"$reason_output" "parallel unavailable"
	assertEquals "Parallel source snapshot command construction should still fail when no staged parallel reason is available." \
		1 "$generic_status"
	assertContains "Parallel source snapshot command construction should emit a generic parallel setup error when no staged reason exists." \
		"$generic_output" "Failed to prepare parallel source discovery."
}

test_ensure_parallel_available_for_source_jobs_requires_local_parallel() {
	set +e
	output=$(
		(
			g_option_j_jobs=2
			g_cmd_parallel=""
			zxfer_ensure_parallel_available_for_source_jobs
			l_status=$?
			printf 'reason=%s\n' "$g_zxfer_parallel_source_job_check_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Parallel listing should fail fast when parallel is missing locally." 1 "$status"
	assertEquals "The local-missing error should mention parallel and the local host, and print nothing else." \
		"reason=The -j option requires parallel but it was not found in PATH on the local host." "$output"
}

test_ensure_parallel_available_for_source_jobs_trusts_available_local_parallel() {
	set +e
	output=$(
		(
			g_option_j_jobs=2
			g_cmd_parallel="$ALT_PARALLEL_BIN"
			zxfer_ensure_parallel_available_for_source_jobs
		)
	)
	status=$?

	assertEquals "Parallel listing should trust an available local parallel helper without version probing." 0 "$status"
	assertEquals "Trusted local parallel setup should not print validation output." "" "$output"
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
			l_status=$?
			printf 'reason=%s\n' "$g_zxfer_parallel_source_job_check_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Missing remote parallel should fail source-job setup." 1 "$status"
	assertContains "The remote-missing error should identify the origin host." \
		"$output" "parallel not found on origin host origin.example"
}

test_ensure_parallel_available_for_source_jobs_returns_success_when_parallel_is_not_requested() {
	g_option_j_jobs=1
	g_cmd_parallel=""
	g_origin_parallel_cmd=""

	zxfer_ensure_parallel_available_for_source_jobs
	status=$?

	assertEquals "Serial snapshot listing should not require parallel." 0 "$status"
	assertEquals "Serial snapshot listing should leave the remote parallel path unset." "" "$g_origin_parallel_cmd"
}

test_ensure_parallel_available_for_source_jobs_skips_local_parallel_for_remote_runs() {
	g_option_j_jobs=2
	g_cmd_parallel=""
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"

	zxfer_resolve_remote_required_tool() {
		g_zxfer_required_tool_result="/opt/bin/parallel"
	}

	zxfer_ensure_parallel_available_for_source_jobs
	status=$?

	assertEquals "Remote source-job setup should not require a local parallel binary when only the origin-host branch will execute it." \
		0 "$status"
}

test_ensure_parallel_available_for_source_jobs_accepts_resolved_remote_parallel_after_resolution() {
	log_file="$TEST_TMPDIR/remote_parallel_resolution.log"
	: >"$log_file"

	(
		LOG_FILE="$log_file"
		zxfer_resolve_remote_required_tool() {
			printf 'resolve:%s\n' "$1" >>"$LOG_FILE"
			g_zxfer_required_tool_result="/opt/bin/parallel"
		}
		g_option_j_jobs=2
		g_option_O_origin_host="origin.example"
		g_cmd_parallel=""
		g_origin_parallel_cmd=""

		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		[ "$g_origin_parallel_cmd" = "/opt/bin/parallel" ] || exit 1
	)
	status=$?

	assertEquals "Remote source-job setup should succeed once the origin-host helper resolves." \
		0 "$status"
	assertContains "Remote source-job setup should still resolve the helper on the origin host." \
		"$(cat "$log_file")" "resolve:origin.example"
	assertNotContains "Remote source-job setup should not version-probe the resolved origin-host helper before publishing it." \
		"$(cat "$log_file")" "version:"
}

test_ensure_parallel_available_for_source_jobs_reuses_cached_remote_parallel_path_for_same_host_and_path() {
	log_file="$TEST_TMPDIR/remote_parallel_reuse.log"
	: >"$log_file"

	(
		LOG_FILE="$log_file"
		zxfer_resolve_remote_required_tool() {
			printf 'resolve:%s\n' "$1" >>"$LOG_FILE"
			g_zxfer_required_tool_result="/opt/bin/parallel"
		}
		g_option_j_jobs=2
		g_option_O_origin_host="origin.example"
		g_cmd_parallel=""
		g_origin_parallel_cmd=""
		g_origin_parallel_cmd_host=""

		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		[ "$g_origin_parallel_cmd" = "/opt/bin/parallel" ] || exit 1
		[ "$g_origin_parallel_cmd_host" = "origin.example" ] || exit 1
	)
	status=$?

	assertEquals "Remote source-job setup should succeed when it reuses a previously resolved origin-host parallel helper." \
		0 "$status"
	assertEquals "Remote source-job setup should skip re-resolving or revalidating the helper once the same host/path is cached." \
		"resolve:origin.example" "$(cat "$log_file")"
}

test_ensure_parallel_available_for_source_jobs_trusts_resolved_remote_parallel_without_banner_probe() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="/opt/bin/parallel"
			}
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_cmd_parallel=""
			g_origin_parallel_cmd=""

			zxfer_ensure_parallel_available_for_source_jobs
			l_status=$?
			printf 'cached=%s\n' "${g_origin_parallel_cmd:-}"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Remote source-job setup should trust a resolved origin-host parallel helper without probing its version banner." \
		0 "$status"
	assertContains "Trusted remote parallel setup should cache the resolved helper for command rendering." \
		"$output" "cached=/opt/bin/parallel"
}

test_ensure_parallel_available_for_source_jobs_preserves_remote_parallel_resolution_failures() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result='Failed to query dependency "parallel" on host origin.example.'
				return 1
			}
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_cmd_parallel=""
			g_origin_parallel_cmd=""

			zxfer_ensure_parallel_available_for_source_jobs
			l_status=$?
			printf 'reason=%s\n' "$g_zxfer_parallel_source_job_check_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Remote source-job setup should preserve remote parallel resolution failures." \
		1 "$status"
	assertContains "Remote parallel resolution failures should preserve the underlying diagnostic." \
		"$output" 'Failed to query dependency "parallel" on host origin.example.'
}

test_ensure_parallel_available_for_source_jobs_refreshes_remote_parallel_path_when_origin_host_changes() {
	result_file="$TEST_TMPDIR/remote_parallel_refresh.out"
	log_file="$TEST_TMPDIR/remote_parallel_refresh.log"
	: >"$log_file"

	(
		LOG_FILE="$log_file"
		zxfer_resolve_remote_required_tool() {
			printf 'resolve:%s\n' "$1" >>"$LOG_FILE"
			case "$1" in
			origin-a.example)
				g_zxfer_required_tool_result="/opt/bin/parallel"
				;;
			origin-b.example)
				g_zxfer_required_tool_result="/usr/local/bin/parallel"
				;;
			esac
		}
		g_option_j_jobs=2
		g_cmd_parallel=""
		g_origin_parallel_cmd=""

		g_option_O_origin_host="origin-a.example"
		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		printf 'first=%s\n' "$g_origin_parallel_cmd" >"$result_file"

		g_option_O_origin_host="origin-b.example"
		zxfer_ensure_parallel_available_for_source_jobs || exit 1
		printf 'second=%s\n' "$g_origin_parallel_cmd" >>"$result_file"
	)
	status=$?

	assertEquals "Remote source-job setup should refresh the resolved parallel helper when the origin host changes." \
		0 "$status"
	assertContains "Remote source-job setup should keep the first host's resolved helper path." \
		"$(cat "$result_file")" "first=/opt/bin/parallel"
	assertContains "Remote source-job setup should replace the cached helper path when the origin host changes." \
		"$(cat "$result_file")" "second=/usr/local/bin/parallel"
	assertContains "Remote source-job setup should re-resolve the helper for the new origin host." \
		"$(cat "$log_file")" "resolve:origin-b.example"
	assertNotContains "Remote source-job setup should not validate the helper for the new origin host." \
		"$(cat "$log_file")" "version:"
}

test_build_source_snapshot_list_cmd_fails_closed_when_local_parallel_is_unavailable() {
	g_option_j_jobs=2
	g_cmd_parallel=""
	g_option_O_origin_host=""

	result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?

	assertEquals "Local -j runs should fail closed when parallel is unavailable." \
		1 "$status"
	assertContains "Local failure should explain that parallel was not found." \
		"$result" "not found in PATH on the local host"
	assertNotContains "Local -j failures should not silently render the serial source snapshot listing." \
		"$result" "'$g_cmd_zfs' 'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' '$g_initial_source'"
}

test_build_source_snapshot_list_cmd_uses_serial_local_discovery_when_parallel_jobs_are_disabled() {
	g_option_j_jobs=1
	g_option_O_origin_host=""

	result=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd)

	assertEquals "Source snapshot discovery should use the direct serial listing command when parallel jobs are disabled." \
		"'$g_cmd_zfs' 'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' '$g_initial_source'" "$result"
	assertEquals "Source snapshot discovery should leave the parallel marker cleared when -j is disabled." \
		0 "$g_source_snapshot_list_uses_parallel"
}

test_build_source_snapshot_list_cmd_preserves_serial_render_status() {
	set +e
	output=$(
		{
			zxfer_render_zfs_command_for_role() {
				return 67
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		}
	)
	status=$?

	assertEquals "Serial source snapshot command rendering should preserve the exact render-helper status." \
		67 "$status"
	assertEquals "Serial source snapshot command rendering should not emit a partial command when rendering fails." \
		"" "$output"
}

test_build_source_snapshot_list_cmd_uses_parallel_local_discovery_directly() {
	g_option_j_jobs=2
	g_cmd_parallel="$PARALLEL_BIN"
	g_option_O_origin_host=""

	result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Local -j discovery should enumerate source datasets directly instead of using the serial snapshot list." \
		"$result" "'$g_cmd_zfs' 'list' '-Hr' '-t' 'filesystem,volume' '-o' 'name' '$g_initial_source'"
	assertContains "Local -j discovery should use parallel with the requested job count." \
		"$result" "'$g_cmd_parallel' -j 2 --line-buffer"
	assertContains "Local -j discovery should leave the placeholder bare so GNU parallel's own quoting keeps each dataset one argument." \
		"$result" "'$g_cmd_zfs' 'list' '-H' '-o' 'name,guid' '-s' 'creation' '-d' '1' '-t' 'snapshot' {}\""
	assertNotContains "Local -j discovery should not inline a prefetched dataset list." \
		"$result" "'printf'"
}

test_build_source_snapshot_list_cmd_guards_local_parallel_discovery_with_sentinel() {
	g_option_j_jobs=2
	g_cmd_parallel="$PARALLEL_BIN"
	g_option_O_origin_host=""

	result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Local -j discovery should capture enumeration failures instead of masking them in the pipeline." \
		"$result" "|| exit 70"
	assertContains "Local -j discovery should only emit the success sentinel when parallel reports success." \
		"$result" "&& printf"
	assertContains "Local -j discovery should reference the success sentinel constant." \
		"$result" "$ZXFER_SOURCE_DISCOVERY_SENTINEL"
	assertContains "Local -j discovery should verify and strip the sentinel with the local filter." \
		"$result" "sentinel_line="
	assertContains "Local -j discovery should fail the pipeline when the sentinel is missing." \
		"$result" "exit 65"
}

test_build_source_snapshot_list_cmd_guards_remote_parallel_discovery_with_sentinel() {
	g_option_j_jobs=3
	g_option_O_origin_host="origin.example"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"

	g_option_z_compress=0
	uncompressed_result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	compressed_result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Remote -j discovery should capture remote enumeration failures explicitly." \
		"$uncompressed_result" "|| exit 70"
	assertContains "Remote -j discovery should gate the success sentinel on parallel success." \
		"$uncompressed_result" "&& printf"
	assertContains "Remote -j discovery should append the local sentinel filter." \
		"$uncompressed_result" "sentinel_line="
	assertContains "Compressed remote -j discovery should keep the sentinel inside the compressed stream." \
		"$compressed_result" "/remote/bin/zstd"
	assertContains "Compressed remote -j discovery should place the sentinel filter after local decompression." \
		"$compressed_result" "'/local/bin/zstd' '-d' | "
	assertContains "Compressed remote -j discovery should still append the local sentinel filter." \
		"$compressed_result" "sentinel_line="
}

test_build_source_snapshot_name_list_cmd_guards_compressed_remote_listing_with_sentinel() {
	g_option_O_origin_host="origin.example"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_option_j_jobs=1

	g_option_z_compress=0
	uncompressed_result=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd)

	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	compressed_result=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd)

	assertNotContains "Uncompressed remote no-op proof listings propagate the zfs exit through ssh and need no sentinel." \
		"$uncompressed_result" "$ZXFER_SOURCE_DISCOVERY_SENTINEL"
	assertContains "Compressed remote no-op proof listings must gate a success sentinel on the listing because zstd masks its exit status." \
		"$compressed_result" "$ZXFER_SOURCE_DISCOVERY_SENTINEL"
	assertContains "Compressed remote no-op proof listings must verify and strip the sentinel locally." \
		"$compressed_result" "sentinel_line="
	assertContains "The sentinel filter must run after local decompression." \
		"$compressed_result" "'/local/bin/zstd' '-d' | "
}

test_local_parallel_discovery_pipeline_strips_sentinel_on_success() {
	fake_zfs="$TEST_TMPDIR/discovery_fake_zfs"
	functional_parallel="$TEST_TMPDIR/discovery_functional_parallel"
	create_discovery_fake_zfs_bin "$fake_zfs"
	create_functional_parallel_bin "$functional_parallel"

	g_option_j_jobs=2
	g_cmd_parallel="$functional_parallel"
	g_option_O_origin_host=""
	g_cmd_zfs="$fake_zfs"

	built_cmd=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	set +e
	output=$(
		(
			eval "$built_cmd"
		)
	)
	status=$?
	set -e

	assertEquals "A fully successful parallel discovery pipeline should exit zero." \
		0 "$status"
	assertEquals "A successful parallel discovery pipeline should emit exactly the snapshot records with the sentinel stripped." \
		"tank/src@s1	111
tank/src/a@s1	222
tank/src/b@s1	333" "$output"
}

test_local_parallel_discovery_pipeline_fails_when_sub_listing_fails() {
	fake_zfs="$TEST_TMPDIR/discovery_fake_zfs"
	functional_parallel="$TEST_TMPDIR/discovery_functional_parallel"
	create_discovery_fake_zfs_bin "$fake_zfs"
	create_functional_parallel_bin "$functional_parallel"

	g_option_j_jobs=2
	g_cmd_parallel="$functional_parallel"
	g_option_O_origin_host=""
	g_cmd_zfs="$fake_zfs"

	built_cmd=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	set +e
	output=$(
		(
			FAKE_ZFS_FAIL_SUBLISTING=1
			export FAKE_ZFS_FAIL_SUBLISTING
			eval "$built_cmd"
		) 2>/dev/null
	)
	status=$?
	set -e

	assertEquals "A failed per-dataset sub-listing must fail the discovery pipeline instead of passing a partial list." \
		65 "$status"
}

test_local_parallel_discovery_pipeline_fails_when_enumeration_fails() {
	fake_zfs="$TEST_TMPDIR/discovery_fake_zfs"
	functional_parallel="$TEST_TMPDIR/discovery_functional_parallel"
	create_discovery_fake_zfs_bin "$fake_zfs"
	create_functional_parallel_bin "$functional_parallel"

	g_option_j_jobs=2
	g_cmd_parallel="$functional_parallel"
	g_option_O_origin_host=""
	g_cmd_zfs="$fake_zfs"

	built_cmd=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	set +e
	output=$(
		(
			FAKE_ZFS_FAIL_ENUMERATION=1
			export FAKE_ZFS_FAIL_ENUMERATION
			eval "$built_cmd"
		) 2>/dev/null
	)
	status=$?
	set -e

	assertEquals "A failed source dataset enumeration must fail the discovery pipeline immediately." \
		70 "$status"
}

test_build_source_snapshot_list_cmd_preserves_local_parallel_builder_statuses() {
	g_option_j_jobs=2
	g_cmd_parallel="$PARALLEL_BIN"
	g_option_O_origin_host=""

	set +e
	check_output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				g_zxfer_parallel_source_job_check_result="parallel check failed"
				return 68
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	check_status=$?
	silent_output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				g_zxfer_parallel_source_job_check_result=""
				return 69
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	silent_status=$?

	assertEquals "Local parallel source snapshot planning should preserve the parallel check status." \
		68 "$check_status"
	assertEquals "Local parallel source snapshot planning should publish the parallel check reason." \
		"parallel check failed" "$check_output"
	assertEquals "A silent parallel check failure should keep its status." \
		69 "$silent_status"
	assertEquals "A silent parallel check failure should publish the generic reason." \
		"Failed to prepare parallel source discovery." "$silent_output"
}

test_build_source_snapshot_list_cmd_uses_parallel_remote_discovery_with_metadata_compression() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_option_z_compress=1
	g_cmd_compress="zstd -T0 -9"
	g_cmd_parallel=""
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-T0' '-9'"

	result=$(
		(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Remote -j discovery should stream the origin dataset inventory directly." \
		"$result" "/remote/bin/zfs"
	assertContains "Remote -j discovery should use parallel on the origin host." \
		"$result" "/opt/bin/parallel"
	assertContains "Remote -j discovery should append the resolved remote metadata compressor." \
		"$result" "/remote/bin/zstd"
	assertContains "Remote -j discovery should append the resolved local metadata decompressor." \
		"$result" "/local/bin/zstd"
	assertContains "Remote -j discovery should preserve the per-dataset remote snapshot runner." \
		"$result" "/remote/bin/zfs"
}

test_build_source_snapshot_name_list_cmd_uses_the_resolved_origin_compressor() {
	g_option_z_compress=1
	g_option_O_origin_host="origin.example"
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"

	result=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd)

	assertContains "Remote snapshot-list metadata should use the configured compressor cost instead of silently strengthening it." \
		"$result" "/remote/bin/zstd"
	assertNotContains "Remote snapshot-list metadata should not pick another compression level." \
		"$result" "-9"
}

test_build_source_snapshot_name_list_cmd_fails_closed_without_a_resolved_origin_compressor() {
	output=$(
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
			g_option_z_compress=0
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
			printf 'disabled=%s\n' "$?"

			g_option_z_compress=1
			g_origin_cmd_compress_safe=""
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
			printf 'unresolved=%s\n' "$?"
		)
	)

	assertContains "Without -z the listing should render." \
		"$output" "disabled=0"
	assertNotContains "Without -z the listing should not name the compressor." \
		"$output" "/remote/bin/zstd"
	assertContains "An unresolved origin compressor should fail closed." \
		"$output" "unresolved=1"
	assertContains "An unresolved origin compressor should be named in the failure." \
		"$output" "The origin host compression command is not resolved."
}

test_build_source_snapshot_name_list_cmd_covers_local_and_remote_rendering() {
	local_result=$(
		(
			g_option_O_origin_host=""
			g_option_j_jobs=4
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
			printf 'parallel=%s\n' "${g_source_snapshot_list_uses_parallel:-unset}"
		)
	)
	remote_result=$(
		(
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_parallel_cmd="/opt/bin/parallel"
			g_origin_parallel_cmd_host="origin.example"
			g_option_j_jobs=6
			g_option_z_compress=0
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
			printf 'parallel=%s\n' "${g_source_snapshot_list_uses_parallel:-unset}"
		)
	)
	compressed_result=$(
		(
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_parallel_cmd="/opt/bin/parallel"
			g_origin_parallel_cmd_host="origin.example"
			g_option_j_jobs=6
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
		)
	)

	assertContains "Local identity-aware no-op proof discovery should render a direct source snapshot list." \
		"$local_result" "'$g_cmd_zfs' 'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$g_initial_source'"
	assertNotContains "Local identity-aware no-op proof discovery should not fan out through parallel before work is proven." \
		"$local_result" "$PARALLEL_BIN"
	assertContains "Local identity-aware no-op proof discovery should record that source fanout was not used." \
		"$local_result" "parallel=0"
	assertNotContains "Local identity-aware discovery should not decompress the listing." \
		"$local_result" "zstd"
	assertContains "Remote identity-aware no-op proof discovery should render the resolved remote zfs path." \
		"$remote_result" "/remote/bin/zfs"
	assertContains "Remote identity-aware discovery should use ssh for the origin host." \
		"$remote_result" "origin.example"
	assertContains "Remote serial identity-aware discovery should request recursive source snapshots." \
		"$remote_result" "-Hr"
	assertNotContains "Remote identity-aware no-op proof discovery should not fan out through origin-host GNU parallel before work is proven." \
		"$remote_result" "/opt/bin/parallel"
	assertNotContains "Remote identity-aware no-op proof discovery should not feed a recursive dataset inventory into parallel." \
		"$remote_result" "filesystem,volume"
	# Check only the command sent to the origin: the fake ssh path before the
	# host lives under the test temp root, which may itself contain "-d".
	assertNotContains "Remote identity-aware no-op proof discovery should not render per-dataset snapshot commands." \
		"${remote_result#*"'origin.example'"}" "-d"
	assertNotContains "Remote identity-aware discovery should not pay for creation-order sorting on the origin." \
		"$remote_result" "creation"
	assertContains "Remote identity-aware no-op proof discovery should record that source fanout was not used." \
		"$remote_result" "parallel=0"
	assertNotContains "Uncompressed remote identity-aware discovery should not decompress the listing." \
		"${remote_result#*"'origin.example'"}" "zstd"
	assertContains "Compressed remote identity-aware discovery should use the resolved metadata compressor." \
		"$compressed_result" "/remote/bin/zstd"
	assertContains "Compressed remote identity-aware discovery should preserve the configured metadata compression level." \
		"$compressed_result" "-3"
	assertContains "Compressed remote identity-aware discovery should append the local decompressor." \
		"$compressed_result" "/local/bin/zstd"
	assertNotContains "Compressed remote identity-aware discovery should still defer parallel fanout." \
		"$compressed_result" "/opt/bin/parallel"
}

test_build_source_snapshot_name_list_cmd_covers_current_shell_success_paths() {
	local_out="$TEST_TMPDIR/source_name_list_local_serial.out"
	remote_serial_out="$TEST_TMPDIR/source_name_list_remote_serial.out"
	remote_compressed_out="$TEST_TMPDIR/source_name_list_remote_compressed.out"

	g_option_O_origin_host=""
	g_option_j_jobs=3
	g_cmd_parallel="$PARALLEL_BIN"
	zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd >"$local_out"
	local_status=$?

	g_option_O_origin_host="origin.example"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_option_j_jobs=1
	g_option_z_compress=0
	zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd >"$remote_serial_out"
	remote_serial_status=$?

	g_option_j_jobs=3
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"
	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd >"$remote_compressed_out"
	remote_compressed_status=$?

	assertEquals "Local no-op proof rendering should succeed in the current shell." \
		0 "$local_status"
	assertContains "Local no-op proof rendering should use one recursive source snapshot query." \
		"$(cat "$local_out")" "-Hr"
	assertNotContains "Local no-op proof rendering should not use parallel when -j is set." \
		"$(cat "$local_out")" "$PARALLEL_BIN"
	assertEquals "Remote serial no-op proof rendering should succeed in the current shell." \
		0 "$remote_serial_status"
	assertContains "Remote serial no-op proof rendering should use a recursive source snapshot query." \
		"$(cat "$remote_serial_out")" "-Hr"
	assertEquals "Remote compressed no-op proof rendering should succeed in the current shell." \
		0 "$remote_compressed_status"
	assertNotContains "Remote compressed no-op proof rendering should not use parallel when -j is set." \
		"$(cat "$remote_compressed_out")" "/opt/bin/parallel"
	assertContains "Remote compressed no-op proof rendering should append metadata compression." \
		"$(cat "$remote_compressed_out")" "/remote/bin/zstd"
	assertContains "Remote compressed no-op proof rendering should append local decompression." \
		"$(cat "$remote_compressed_out")" "/local/bin/zstd"
}

test_build_source_snapshot_name_list_cmd_keeps_source_side_excludes_local_when_jobs_are_configured() {
	local_result=$(
		(
			g_option_O_origin_host=""
			g_option_j_jobs=4
			g_option_x_exclude_datasets='replica$'
			g_cmd_parallel="$PARALLEL_BIN"
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
		)
	)
	remote_result=$(
		(
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_parallel_cmd="/opt/bin/parallel"
			g_origin_parallel_cmd_host="origin.example"
			g_option_j_jobs=6
			g_option_x_exclude_datasets='replica$'
			g_option_z_compress=0
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="unexpected-remote-awk"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
		)
	)

	assertContains "Local no-op proof discovery should use one recursive source snapshot query when excludes are configured." \
		"$local_result" "-Hr"
	assertNotContains "Local no-op proof discovery should not use parallel before work is proven when excludes are configured." \
		"$local_result" "$PARALLEL_BIN"
	assertNotContains "Local no-op proof discovery should leave exclude filtering to the local sort/filter wrapper." \
		"$local_result" "exclude_pattern=replica$"
	assertContains "Remote no-op proof discovery should use one recursive source snapshot query when excludes are configured." \
		"$remote_result" "-Hr"
	assertNotContains "Remote no-op proof discovery should not use parallel before work is proven when excludes are configured." \
		"$remote_result" "/opt/bin/parallel"
	assertNotContains "Remote no-op proof discovery should not feed fanout from the recursive source dataset list when excludes are configured." \
		"$remote_result" "filesystem,volume"
	assertNotContains "Remote no-op proof discovery should not resolve remote awk for the source-side proof filter." \
		"$remote_result" "unexpected-remote-awk"
}

test_build_source_snapshot_name_list_cmd_preserves_render_failures() {
	set +e
	ssh_output=$(
		(
			g_option_O_origin_host="origin.example"
			zxfer_ssh_shell_command_for_host() {
				return 34
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
			printf 'status=%s\n' "$?"
		)
	)
	compress_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_z_compress=1
			g_origin_cmd_compress_safe=""
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Name-only remote snapshot command rendering should preserve ssh wrapper failures without a partial command." \
		"status=34" "$ssh_output"
	assertEquals "Name-only remote snapshot command rendering should fail closed without an origin compressor." \
		1 "$compress_output"
}

test_build_source_snapshot_name_list_cmd_does_not_require_remote_awk_for_excludes() {
	output=$(
		(
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_parallel_cmd="/opt/bin/parallel"
			g_origin_parallel_cmd_host="origin.example"
			g_option_j_jobs=2
			g_option_x_exclude_datasets='replica$'
			g_option_z_compress=0
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="unexpected-remote-awk"
				return 35
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
		)
	)

	assertContains "Remote no-op proof discovery should render the recursive source snapshot query without remote awk." \
		"$output" "/remote/bin/zfs"
	assertNotContains "Remote no-op proof discovery should not use source-side fanout for the identity-aware proof." \
		"$output" "/opt/bin/parallel"
	assertNotContains "Remote no-op proof discovery should not resolve remote awk for source exclude filtering." \
		"$output" "unexpected-remote-awk"
}

test_build_source_snapshot_name_list_cmd_does_not_probe_parallel_before_work_is_proven() {
	set +e
	local_result=$(
		(
			g_option_j_jobs=2
			g_option_O_origin_host=""
			g_cmd_parallel=""
			zxfer_ensure_parallel_available_for_source_jobs() {
				printf '%s\n' "unexpected-local-parallel-check"
				return 5
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
		)
	)
	local_status=$?
	remote_result=$(
		(
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_parallel_cmd=""
			zxfer_ensure_parallel_available_for_source_jobs() {
				printf '%s\n' "unexpected-remote-parallel-check"
				return 1
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd
		)
	)
	remote_status=$?

	assertEquals "Local fast no-op proof should not require parallel when -j is configured." \
		0 "$local_status"
	assertNotContains "Local fast no-op proof should not run the parallel setup check before work is proven." \
		"$local_result" "unexpected-local-parallel-check"
	assertEquals "Remote fast no-op proof should not require origin parallel when -j is configured." \
		0 "$remote_status"
	assertNotContains "Remote fast no-op proof should not run the origin parallel setup check before work is proven." \
		"$remote_result" "unexpected-remote-parallel-check"
}

test_build_source_snapshot_name_list_cmd_preserves_local_recursive_render_failures_when_jobs_requested() {
	g_option_j_jobs=2
	g_option_O_origin_host=""
	g_cmd_parallel="/usr/local/bin/parallel"
	g_cmd_zfs="/sbin/zfs"

	output=$(zxfer_test_print_source_listing zxfer_build_source_snapshot_name_list_cmd)
	status=$?

	assertEquals "Local identity-aware no-op proof should render without parallel when -j was requested." \
		0 "$status"
	assertEquals "Local identity-aware no-op proof should render one recursive listing." \
		"'/sbin/zfs' 'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$g_initial_source'" "$output"
}

test_build_source_snapshot_list_cmd_fails_closed_when_remote_parallel_is_unavailable() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd=""
	g_cmd_parallel=""
	g_origin_cmd_zfs="/remote/bin/zfs"

	result=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				g_zxfer_parallel_source_job_check_result='parallel not found on origin host origin.example but -j 2 was requested. Install parallel remotely or rerun without -j.'
				return 1
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?

	assertEquals "Remote -j discovery should fail closed when origin-host parallel is unavailable." \
		1 "$status"
	assertContains "Remote -j discovery should preserve the origin-host parallel failure reason when it aborts." \
		"$result" 'parallel not found on origin host origin.example but -j 2 was requested. Install parallel remotely or rerun without -j.'
	assertNotContains "Remote -j discovery should not silently render the serial remote snapshot listing." \
		"$result" "/remote/bin/zfs"
}

test_build_source_snapshot_list_cmd_preserves_remote_ssh_wrapper_status() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_cmd_zfs="/remote/bin/zfs"

	set +e
	output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				return 0
			}
			zxfer_ssh_shell_command_for_host() {
				return 79
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?

	assertEquals "Remote source snapshot command rendering should preserve the exact ssh wrapper builder status." \
		79 "$status"
	assertEquals "Remote source snapshot command rendering should not emit a partial command when ssh wrapper rendering fails." \
		"" "$output"
}

test_build_source_snapshot_list_cmd_preserves_remote_parallel_builder_statuses() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_option_z_compress=1
	g_origin_cmd_compress_safe=""

	set +e
	output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				return 0
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?

	assertEquals "Remote parallel source snapshot planning should fail closed without an origin compressor." \
		1 "$status"
	assertEquals "Remote parallel source snapshot planning should publish only the failure, not a partial command." \
		"The origin host compression command is not resolved." "$output"
}

test_build_source_snapshot_list_cmd_preserves_local_parallel_dataset_input_render_failure() {
	g_option_j_jobs=2
	g_option_O_origin_host=""
	g_cmd_parallel="/usr/local/bin/parallel"
	g_cmd_zfs="/sbin/zfs"

	output=$(
		(
			zxfer_ensure_parallel_available_for_source_jobs() {
				return 0
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Local parallel source command rendering should stop with exit 70 when the dataset list fails." \
		"$output" "zxfer_discovery_datasets=\$('/sbin/zfs' 'list' '-Hr' '-t' 'filesystem,volume' '-o' 'name' '$g_initial_source') || exit 70;"
}

test_write_source_snapshot_list_to_file_starts_the_sorting_background_runner() {
	log="$TEST_TMPDIR/source_serial.log"
	outfile="$TEST_TMPDIR/source_serial.out"
	errfile="$TEST_TMPDIR/source_serial.err"
	: >"$log"

	(
		SOURCE_LOG="$log"
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result="printf 'snap-serial'"
		}
		zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$SOURCE_LOG"
			printf 'sorted_arg=%s\n' "$4" >>"$SOURCE_LOG"
			g_last_background_pid=4242
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		printf '%s\n' "$g_source_snapshot_list_pid" >>"$SOURCE_LOG"
		printf 'sorted_published=%s\n' "$g_zxfer_full_source_snapshot_sorted_file" >>"$SOURCE_LOG"
	)

	sorted_arg=$(sed -n 's/^sorted_arg=//p' "$log")
	assertNotNull "Every source listing should get a byte-sorted sidecar." "$sorted_arg"
	assertEquals "Source snapshot listing should start the sorting background runner and publish its PID and sidecar." \
		"printf 'snap-serial'|$outfile|$errfile
sorted_arg=$sorted_arg
4242
sorted_published=$sorted_arg" "$(cat "$log")"
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
			g_zxfer_source_snapshot_list_cmd_result="printf 'snap-profile'"
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
			g_zxfer_source_snapshot_list_cmd_result="printf 'remote-snap-profile'"
		}
		zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
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

test_write_source_snapshot_list_to_file_backgrounds_parallel_command() {
	outfile="$TEST_TMPDIR/source_parallel.out"
	lastcmd_file="$TEST_TMPDIR/source_parallel.lastcmd"
	g_option_j_jobs=3

	(
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result="printf 'snap-parallel'"
		}
		zxfer_record_last_command_string() {
			printf '%s\n' "$1" >>"$lastcmd_file"
		}
		zxfer_write_source_snapshot_list_to_file "$outfile"
		wait
	)

	assertEquals "Parallel snapshot listing should execute the built command in the background." \
		"snap-parallel" "$(cat "$outfile")"
	assertEquals "Parallel snapshot listing should first record the built command for failure reports." \
		"printf 'snap-parallel'" "$(sed -n 1p "$lastcmd_file")"
}

test_write_source_snapshot_list_to_file_uses_current_shell_temp_file_result() {
	outfile="$TEST_TMPDIR/source_current_shell.out"
	errfile="$TEST_TMPDIR/source_current_shell.err"
	log="$TEST_TMPDIR/source_current_shell.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result="$TEST_TMPDIR/source_current_shell.sorted"
			: >"$g_zxfer_temp_file_result"
		}
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result="printf 'snap-current-shell'"
		}
		zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
			printf '%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" >>"$LOG_FILE"
			g_last_background_pid=5151
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
	)

	assertEquals "Source snapshot discovery should take the sorted sidecar from the current-shell temp-file result instead of stdout." \
		"printf 'snap-current-shell'|$outfile|$errfile|$TEST_TMPDIR/source_current_shell.sorted" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_starts_published_command() {
	outfile="$TEST_TMPDIR/source_read_scratch.out"
	errfile="$TEST_TMPDIR/source_read_scratch.err"
	log="$TEST_TMPDIR/source_read_scratch.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result="printf 'snap-read-scratch'"
		}
		zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			g_last_background_pid=6161
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
	)

	assertEquals "Source snapshot discovery should start the command the builder published in the current shell." \
		"printf 'snap-read-scratch'|$outfile|$errfile" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_throws_generic_message_when_build_fails_silently() {
	outfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_failure.out"
	errfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_failure.err"

	zxfer_test_capture_subshell "
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result=''
			return 7
		}
		zxfer_throw_error() {
			printf '<%s:%s>' \"\$1\" \"\$2\"
			exit 1
		}
		zxfer_write_source_snapshot_list_to_file '$outfile' '$errfile'
	"

	assertEquals "Source snapshot discovery should fail closed when the build fails without a message." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A build failure without a message should throw the generic message with the build status." \
		"<Failed to build source snapshot discovery command.:7>" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_write_source_snapshot_list_to_file_throws_builder_message() {
	outfile="$TEST_TMPDIR/source_cmd_trim_build_failure.out"
	errfile="$TEST_TMPDIR/source_cmd_trim_build_failure.err"

	zxfer_test_capture_subshell "
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result='builder failed'
			return 1
		}
		zxfer_throw_error() {
			printf '<%s>' \"\$1\"
			exit 1
		}
		zxfer_write_source_snapshot_list_to_file '$outfile' '$errfile'
	"

	assertEquals "Source snapshot discovery should fail closed when the build fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A build failure should throw the builder's message verbatim." \
		"<builder failed>" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_write_source_snapshot_list_to_file_starts_built_command_without_readback() {
	outfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_success.out"
	errfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_success.err"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf 'snap-build-success'"
			}
			zxfer_read_runtime_artifact_file() {
				printf '%s\n' "unexpected readback"
				return 1
			}
			zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
				printf 'run=%s\n' "$1"
				g_last_background_pid=7171
			}
			g_option_j_jobs=1
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'cmd=%s\n' "$g_source_snapshot_list_cmd"
		)
	)

	assertEquals "A successful build should start the published command without reading anything back." \
		"run=printf 'snap-build-success'
cmd=printf 'snap-build-success'" "$output"
}

test_write_source_snapshot_list_to_file_preserves_background_sort_setup_failures() {
	outfile="$TEST_TMPDIR/source_background_sort_setup_failure.out"
	errfile="$TEST_TMPDIR/source_background_sort_setup_failure.err"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' snap"
			}
			zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
				return 32
			}
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'status=%s\n' "$?"
			printf 'sorted=%s\n' "${g_zxfer_full_source_snapshot_sorted_file:-}"
		)
	)

	assertContains "Source discovery should preserve background-sort setup failures." \
		"$output" "status=32"
	assertContains "Source discovery should clear the sorted sidecar path when background-sort setup fails." \
		"$output" "sorted="
}

test_write_source_snapshot_list_to_file_preserves_background_sort_temp_failures() {
	outfile="$TEST_TMPDIR/source_background_sort_temp_failure.out"
	errfile="$TEST_TMPDIR/source_background_sort_temp_failure.err"

	output=$(
		(
			temp_calls=0
			zxfer_get_temp_file() {
				temp_calls=$((temp_calls + 1))
				if [ "$temp_calls" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/source_background_sort_temp_failure.sorted"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 33
			}
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' snap"
			}
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'status=%s\n' "$?"
			printf 'calls=%s\n' "$temp_calls"
		)
	)

	assertContains "Source discovery should preserve status-file tempfile failures before launching the background job." \
		"$output" "status=33"
	assertContains "Source discovery should allocate the sorted sidecar, then fail on the first status file." \
		"$output" "calls=2"
}

test_write_source_snapshot_list_to_file_runs_builder_output_under_a_waitable_pid() {
	outfile="$TEST_TMPDIR/source_builder_output.out"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' builder-output"
			}
			zxfer_write_source_snapshot_list_to_file "$outfile"
			printf 'pid=<%s>\n' "$g_source_snapshot_list_pid"
			wait "$g_source_snapshot_list_pid"
			printf 'wait=%s\n' "$?"
			printf 'payload=%s\n' "$(cat "$outfile")"
		) 2>&1
	)

	assertNotContains "Snapshot-list execution should publish the background producer PID." \
		"$output" "pid=<>"
	assertContains "The published producer PID should be waitable and succeed." \
		"$output" "wait=0"
	assertContains "Snapshot-list execution should run the builder's command output in the background." \
		"$output" "payload=builder-output"
}

test_write_source_snapshot_list_to_file_can_sort_inside_background_job() {
	outfile="$TEST_TMPDIR/source_background_sort.out"
	errfile="$TEST_TMPDIR/source_background_sort.err"
	sorted_path_file="$TEST_TMPDIR/source_background_sort.path"

	(
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' tank/src@b tank/src@a"
		}
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		wait "$g_source_snapshot_list_pid"
		printf '%s\n' "$g_zxfer_full_source_snapshot_sorted_file" >"$sorted_path_file"
	)
	status=$?
	sorted_file=$(cat "$sorted_path_file")

	assertEquals "Background source snapshot discovery with an internal sort should complete successfully." \
		0 "$status"
	assertEquals "Background source snapshot discovery should preserve the raw creation-order output." \
		"tank/src@b
tank/src@a" "$(cat "$outfile")"
	assertEquals "Background source snapshot discovery should publish a sorted sidecar for recursive diff planning." \
		"tank/src@a
tank/src@b" "$(cat "$sorted_file")"
	assertEquals "Background source snapshot discovery should leave stderr empty on success." \
		"" "$(cat "$errfile")"
	zxfer_cleanup_runtime_artifact_path "$sorted_file"
}

test_write_source_snapshot_list_to_file_preserves_source_failure_when_streaming_background_sort() {
	outfile="$TEST_TMPDIR/source_background_sort_failure.out"
	errfile="$TEST_TMPDIR/source_background_sort_failure.err"
	sorted_path_file="$TEST_TMPDIR/source_background_sort_failure.path"

	(
		zxfer_build_source_snapshot_list_cmd() {
			g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' tank/src@partial; exit 37"
		}
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		wait "$g_source_snapshot_list_pid"
		printf 'status=%s\n' "$?" >"$sorted_path_file"
		printf 'sorted=%s\n' "$g_zxfer_full_source_snapshot_sorted_file" >>"$sorted_path_file"
	)
	result=$(cat "$sorted_path_file")
	sorted_file=$(printf '%s\n' "$result" | sed -n 's/^sorted=//p')

	assertContains "Streaming background sort should preserve the source-list command status." \
		"$result" "status=37"
	assertEquals "Streaming background sort should preserve partial raw output for diagnostics." \
		"tank/src@partial" "$(cat "$outfile")"
	zxfer_cleanup_runtime_artifact_path "$sorted_file"
}

test_execute_source_snapshot_list_background_cmd_with_sort_preserves_setup_failures() {
	outfile="$TEST_TMPDIR/source_background_sort_setup.out"
	errfile="$TEST_TMPDIR/source_background_sort_setup.err"
	sorted_file="$TEST_TMPDIR/source_background_sort_setup.sorted"

	group_status=$(
		(
			zxfer_create_temp_file_group() {
				return 14
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Background sort setup should preserve status-tempfile allocation failures." \
		14 "$group_status"
}

test_source_snapshot_producers_fail_with_the_first_failing_stage_status() {
	outfile="$TEST_TMPDIR/source_stage_status.out"
	sorted_file="$TEST_TMPDIR/source_stage_status.sorted"
	count_file="$TEST_TMPDIR/source_stage_status.count"

	output=$(
		(
			zxfer_execute_source_snapshot_list_background_cmd_with_sort \
				"printf '%s\\n' b a; exit 7" "$outfile" "" "$sorted_file"
			wait "$g_last_background_pid"
			printf 'records=%s sorted=%s\n' "$?" "$(tr '\n' ',' <"$sorted_file")"
			g_option_x_exclude_datasets='^tank/skip'
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"printf '%s\\n' tank/skip@s tank/src@s" "$sorted_file" "" "$count_file"
			wait "$g_last_background_pid"
			printf 'names=%s sorted=%s count=%s\n' "$?" "$(cat "$sorted_file")" "$(cat "$count_file")"
		)
	)

	assertContains "A failed source command should fail the background sort with its own status." \
		"$output" "records=7 sorted=a,b,"
	assertContains "The no-op proof producer should filter excluded records, count, and sort." \
		"$output" "names=0 sorted=tank/src@s count=1"
}

test_source_snapshot_producers_preserve_spawn_failure_without_registering_stale_pid() {
	for producer in names records; do
		status=0
		output=$(
			g_last_background_pid=12345
			zxfer_spawn_background_shell() { return 79; }
			zxfer_register_cleanup_pid() { printf 'registered stale PID'; }
			if [ "$producer" = names ]; then
				zxfer_execute_source_snapshot_name_list_background_sort_cmd \
					'printf x' "$TEST_TMPDIR/failed-spawn.sorted"
			else
				zxfer_execute_source_snapshot_list_background_cmd_with_sort \
					'printf x' "$TEST_TMPDIR/failed-spawn.out" "" \
					"$TEST_TMPDIR/failed-spawn.sorted"
			fi
		) || status=$?

		assertEquals "$producer discovery must preserve the spawn failure." 79 "$status"
		assertEquals "$producer discovery must not register a previous helper PID after a failed spawn." \
			"" "$output"
	done
}

# Exercise both producer entry points against real cleanup, in the probed
# spawn mode and the wrapper, and an injected KILL failure. The latter must
# retain scope for ordered trap retry.
zxfer_test_assert_source_producer_registration_cleanup() {
	producer_mode=$1
	child_file="$TEST_TMPDIR/source_${producer_mode}_register.child"
	sorted_file="$TEST_TMPDIR/source_${producer_mode}_register.sorted"
	outfile="$TEST_TMPDIR/source_${producer_mode}_register.out"
	errfile="$TEST_TMPDIR/source_${producer_mode}_register.err"
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
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				'printf x' "$sorted_file" "$errfile"
		else
			zxfer_execute_source_snapshot_list_background_cmd_with_sort \
				'printf x' "$outfile" "$errfile" "$sorted_file"
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
			if [ "$producer_mode" = names ]; then
				zxfer_execute_source_snapshot_name_list_background_sort_cmd \
					"$producer_cmd" "$sorted_file" "$errfile"
			else
				zxfer_execute_source_snapshot_list_background_cmd_with_sort \
					"$producer_cmd" "$outfile" "$errfile" "$sorted_file"
			fi
			printf 'status=%s pid=<%s> records=<%s>\n' \
				"$?" "$g_last_background_pid" "$g_zxfer_cleanup_pid_records"
		)
		assertContains "$producer_mode/$spawn_mode completes failed-registration cleanup and clears ownership." \
			"$output" 'status=1 pid=<> records=<>'
		assertTrue "$producer_mode/$spawn_mode starts the TERM-ignoring descendant before cleanup." \
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
			fail "$producer_mode/$spawn_mode forgot a TERM-ignoring descendant after registration failed."
		fi
	done
}

test_execute_source_snapshot_list_background_cmd_with_sort_aborts_child_when_registration_fails() {
	zxfer_test_assert_source_producer_registration_cleanup records
}

test_execute_source_snapshot_name_list_background_sort_cmd_runs_without_error_file() {
	sorted_file="$TEST_TMPDIR/source_name_background_sort.sorted"

	(
		zxfer_execute_source_snapshot_name_list_background_sort_cmd \
			"printf '%s\n' zeta alpha" "$sorted_file" || exit "$?"
		l_pid=$g_last_background_pid
		wait "$l_pid" || exit "$?"
		zxfer_unregister_cleanup_pid "$l_pid"
	)
	status=$?

	assertEquals "Identity-aware background sort should complete successfully without a stderr capture file." \
		0 "$status"
	assertEquals "Identity-aware background sort should write sorted source snapshot records." \
		"alpha
zeta" "$(cat "$sorted_file")"
}

test_execute_source_snapshot_name_list_background_sort_cmd_filters_excluded_snapshots_before_sort() {
	sorted_file="$TEST_TMPDIR/source_name_background_sort_filtered.sorted"

	(
		g_option_x_exclude_datasets='/replica$'
		zxfer_execute_source_snapshot_name_list_background_sort_cmd \
			"printf '%s\n' tank/src/replica@snap2 tank/src/app@snap2 tank/src/app@snap1" \
			"$sorted_file" || exit "$?"
		l_pid=$g_last_background_pid
		wait "$l_pid" || exit "$?"
		zxfer_unregister_cleanup_pid "$l_pid"
	)
	status=$?

	assertEquals "Name-only background sort should complete successfully when exclude filtering is active." \
		0 "$status"
	assertEquals "Name-only background sort should remove excluded datasets before sorting the no-op proof list." \
		"tank/src/app@snap1
tank/src/app@snap2" "$(cat "$sorted_file")"
}

test_execute_source_snapshot_name_list_background_sort_cmd_preserves_setup_failures() {
	sorted_file="$TEST_TMPDIR/source_name_background_sort_setup.sorted"

	temp_status=$(
		(
			zxfer_get_temp_file() {
				return 23
			}
			set +e
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"printf x" "$sorted_file"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Name-only background sort setup should preserve temp-file allocation failures." \
		23 "$temp_status"
}

test_execute_source_snapshot_name_list_background_sort_cmd_aborts_child_when_registration_fails() {
	zxfer_test_assert_source_producer_registration_cleanup names
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

test_read_snapshot_discovery_status_file_defaults_empty_sidecars() {
	status_file="$TEST_TMPDIR/snapshot_discovery_empty_status.out"
	: >"$status_file"

	zxfer_read_snapshot_discovery_status_file "$status_file" 37
	status=$?

	assertEquals "Empty snapshot discovery status files should be accepted as the supplied default." \
		0 "$status"
	assertEquals "Empty snapshot discovery status files should publish the supplied default." \
		37 "$g_zxfer_snapshot_discovery_status_file_result"
}
