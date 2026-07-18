#!/bin/sh
# shellcheck shell=sh
# Source command production, parallel discovery, and producer execution cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_zxfer_reset_snapshot_discovery_state_preserves_remote_parallel_state() {
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"
	g_zxfer_snapshot_discovery_file_read_result="printf 'snap'"
	g_zxfer_parallel_source_job_check_kind="origin_missing"
	g_zxfer_recursive_dataset_list_result="tank/src"
	g_zxfer_source_snapshot_record_cache_file="$g_zxfer_run_tmp_root/source_cache.raw"
	g_zxfer_destination_snapshot_record_cache_file="$g_zxfer_run_tmp_root/destination_cache.raw"
	g_source_snapshot_list_sorted_file="$g_zxfer_run_tmp_root/source_sorted.raw"
	source_cache_file=$g_zxfer_source_snapshot_record_cache_file
	destination_cache_file=$g_zxfer_destination_snapshot_record_cache_file
	sorted_source_file=$g_source_snapshot_list_sorted_file
	g_source_snapshot_list_background_sort_requested=1
	printf '%s\n' "tank/src@snap1" >"$g_zxfer_source_snapshot_record_cache_file"
	printf '%s\n' "backup/dst/src@snap1" >"$g_zxfer_destination_snapshot_record_cache_file"
	printf '%s\n' "tank/src@snap1" >"$g_source_snapshot_list_sorted_file"
	zxfer_reset_snapshot_discovery_state

	assertEquals "Resetting snapshot discovery state should preserve the cached remote parallel helper path for later discovery passes in the same run." \
		"/opt/bin/parallel" "$g_origin_parallel_cmd"
	assertEquals "Resetting snapshot discovery state should preserve the host paired with the cached remote parallel helper path." \
		"origin.example" "$g_origin_parallel_cmd_host"
	assertEquals "Resetting snapshot discovery state should clear staged snapshot-discovery file-read scratch." \
		"" "$g_zxfer_snapshot_discovery_file_read_result"
	assertEquals "Resetting snapshot discovery state should clear staged parallel-check kind scratch." \
		"" "$g_zxfer_parallel_source_job_check_kind"
	assertEquals "Resetting snapshot discovery state should clear recursive dataset-list scratch." \
		"" "$g_zxfer_recursive_dataset_list_result"
	assertEquals "Resetting snapshot discovery state should clear the staged source snapshot-record cache file path." \
		"" "${g_zxfer_source_snapshot_record_cache_file:-}"
	assertEquals "Resetting snapshot discovery state should clear the staged destination snapshot-record cache file path." \
		"" "${g_zxfer_destination_snapshot_record_cache_file:-}"
	assertEquals "Resetting snapshot discovery state should clear the staged sorted source snapshot file path." \
		"" "${g_source_snapshot_list_sorted_file:-}"
	assertEquals "Resetting snapshot discovery state should clear the background source sort request flag." \
		"0" "${g_source_snapshot_list_background_sort_requested:-0}"
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
			printf '%s\n' "/opt/bin/parallel"
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

test_destination_snapshot_dataset_helpers_treat_regex_significant_source_names_as_literal_paths() {
	g_initial_source="tank/app.v1"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=0

	assertEquals "Non-trailing-slash mapping should preserve dots in the source root as literal path components." \
		"backup/dst/app.v1/releases.2026" "$(zxfer_get_destination_dataset_for_source_dataset "tank/app.v1/releases.2026")"

	g_initial_source_had_trailing_slash=1
	assertEquals "Trailing-slash mapping should still preserve dotted child names as literal path components." \
		"backup/dst/releases.2026" "$(zxfer_get_destination_dataset_for_source_dataset "tank/app.v1/releases.2026")"
}

test_build_source_snapshot_list_cmd_reports_parallel_helper_failures_in_current_shell() {
	g_option_j_jobs=2
	output_file="$TEST_TMPDIR/source_snapshot_cmd.out"

	set +e
	(
		zxfer_check_parallel_source_jobs_in_current_shell() {
			g_zxfer_parallel_source_job_check_result="parallel unavailable"
			return 1
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
	)
	reason_status=$?
	reason_output=$(cat "$output_file")

	(
		zxfer_check_parallel_source_jobs_in_current_shell() {
			return 1
		}
		zxfer_build_source_snapshot_list_cmd >"$output_file"
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
		)
	)
	status=$?

	assertEquals "Parallel listing should fail fast when parallel is missing locally." 1 "$status"
	assertContains "The local-missing error should mention parallel and the local host." \
		"$output" "requires parallel but it was not found in PATH on the local host"
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
			printf 'kind=%s\n' "${g_zxfer_parallel_source_job_check_kind:-}"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Missing remote parallel should fail source-job setup." 1 "$status"
	assertContains "The remote-missing error should identify the origin host." \
		"$output" "parallel not found on origin host origin.example"
	assertContains "Missing remote parallel should set a machine-readable reason kind for downstream fallback decisions." \
		"$output" "kind=origin_missing"
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
		printf '%s\n' "/opt/bin/parallel"
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
			printf '%s\n' "/opt/bin/parallel"
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
			printf '%s\n' "/opt/bin/parallel"
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
				printf '%s\n' "/opt/bin/parallel"
			}
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_cmd_parallel=""
			g_origin_parallel_cmd=""

			zxfer_ensure_parallel_available_for_source_jobs
			l_status=$?
			printf 'kind=%s\n' "${g_zxfer_parallel_source_job_check_kind:-}"
			printf 'cached=%s\n' "${g_origin_parallel_cmd:-}"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Remote source-job setup should trust a resolved origin-host parallel helper without probing its version banner." \
		0 "$status"
	assertContains "Trusted remote parallel setup should cache the resolved helper for command rendering." \
		"$output" "cached=/opt/bin/parallel"
	assertContains "Trusted remote parallel setup should not publish a validation failure kind." \
		"$output" "kind="
}

test_ensure_parallel_available_for_source_jobs_preserves_remote_parallel_resolution_failures() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' 'Failed to query dependency "parallel" on host origin.example.'
				return 1
			}
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_cmd_parallel=""
			g_origin_parallel_cmd=""

			zxfer_ensure_parallel_available_for_source_jobs
			l_status=$?
			printf 'kind=%s\n' "${g_zxfer_parallel_source_job_check_kind:-}"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Remote source-job setup should preserve remote parallel resolution failures." \
		1 "$status"
	assertContains "Remote parallel resolution failures should preserve the underlying diagnostic." \
		"$output" 'Failed to query dependency "parallel" on host origin.example.'
	assertContains "Remote parallel resolution failures should classify the rejection as a probe failure." \
		"$output" "kind=origin_probe_failed"
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
				printf '%s\n' "/opt/bin/parallel"
				;;
			origin-b.example)
				printf '%s\n' "/usr/local/bin/parallel"
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
			zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?

	assertEquals "Local -j runs should fail closed when parallel is unavailable." \
		1 "$status"
	assertContains "Local failure should explain that parallel was not found." \
		"$result" "not found in PATH on the local host"
	assertNotContains "Local -j failures should not silently render the serial source snapshot listing." \
		"$result" "'$g_LZFS' 'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' '$g_initial_source'"
}

test_build_source_snapshot_list_cmd_uses_serial_local_discovery_when_parallel_jobs_are_disabled() {
	g_option_j_jobs=1
	g_option_O_origin_host=""

	result=$(zxfer_build_source_snapshot_list_cmd)

	assertEquals "Source snapshot discovery should use the direct serial listing command when parallel jobs are disabled." \
		"'$g_LZFS' 'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' '$g_initial_source'" "$result"
	assertEquals "Source snapshot discovery should leave the parallel marker cleared when -j is disabled." \
		0 "$g_source_snapshot_list_uses_parallel"
}

test_build_source_snapshot_list_cmd_preserves_serial_render_status() {
	set +e
	output=$(
		{
			zxfer_render_zfs_command_for_spec() {
				return 67
			}
			zxfer_build_source_snapshot_list_cmd
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
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Local -j discovery should enumerate source datasets directly instead of using the serial snapshot list." \
		"$result" "'$g_LZFS' 'list' '-Hr' '-t' 'filesystem,volume' '-o' 'name' '$g_initial_source'"
	assertContains "Local -j discovery should use parallel with the requested job count." \
		"$result" "'$g_cmd_parallel' -j 2 --line-buffer"
	assertContains "Local -j discovery should preserve the per-dataset snapshot runner." \
		"$result" "'$g_LZFS' 'list' '-H' '-o' 'name,guid' '-s' 'creation' '-d' '1' '-t' 'snapshot' '{}'"
	assertNotContains "Local -j discovery should not inline a prefetched dataset list." \
		"$result" "'printf'"
}

test_build_source_snapshot_list_cmd_guards_local_parallel_discovery_with_sentinel() {
	g_option_j_jobs=2
	g_cmd_parallel="$PARALLEL_BIN"
	g_option_O_origin_host=""

	result=$(
		(
			zxfer_build_source_snapshot_list_cmd
		)
	)

	assertContains "Local -j discovery should capture enumeration failures instead of masking them in the pipeline." \
		"$result" "|| exit 70"
	assertContains "Local -j discovery should only emit the success sentinel when parallel reports success." \
		"$result" "&& printf"
	assertContains "Local -j discovery should reference the success sentinel constant." \
		"$result" "$(zxfer_get_source_discovery_sentinel_line)"
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
			zxfer_build_source_snapshot_list_cmd
		)
	)

	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	compressed_result=$(
		(
			zxfer_build_source_snapshot_list_cmd
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
	uncompressed_result=$(zxfer_build_source_snapshot_name_list_cmd)

	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	compressed_result=$(zxfer_build_source_snapshot_name_list_cmd)

	assertNotContains "Uncompressed remote no-op proof listings propagate the zfs exit through ssh and need no sentinel." \
		"$uncompressed_result" "$(zxfer_get_source_discovery_sentinel_line)"
	assertContains "Compressed remote no-op proof listings must gate a success sentinel on the listing because zstd masks its exit status." \
		"$compressed_result" "$(zxfer_get_source_discovery_sentinel_line)"
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
	g_LZFS="$fake_zfs"
	g_cmd_zfs="$fake_zfs"

	built_cmd=$(
		(
			zxfer_build_source_snapshot_list_cmd
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
	g_LZFS="$fake_zfs"
	g_cmd_zfs="$fake_zfs"

	built_cmd=$(
		(
			zxfer_build_source_snapshot_list_cmd
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
	g_LZFS="$fake_zfs"
	g_cmd_zfs="$fake_zfs"

	built_cmd=$(
		(
			zxfer_build_source_snapshot_list_cmd
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
	runner_output=$(
		{
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_render_zfs_command_for_spec() {
				if [ "$3" = "-H" ]; then
					return 68
				fi
				printf '%s\n' "unexpected"
			}
			zxfer_build_source_snapshot_list_cmd
		}
	)
	runner_status=$?
	parallel_output=$(
		{
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_render_zfs_command_for_spec() {
				printf '%s\n' "rendered"
			}
			zxfer_build_shell_command_from_argv() {
				return 69
			}
			zxfer_build_source_snapshot_list_cmd
		}
	)
	parallel_status=$?
	dataset_output=$(
		{
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_render_zfs_command_for_spec() {
				if [ "$3" = "-H" ]; then
					printf '%s\n' "runner"
					return 0
				fi
				if [ "$3" = "-Hr" ]; then
					return 70
				fi
				printf '%s\n' "unexpected"
			}
			zxfer_build_shell_command_from_argv() {
				printf '%s\n' "parallel"
			}
			zxfer_build_source_snapshot_list_cmd
		}
	)
	dataset_status=$?

	assertEquals "Local parallel source snapshot planning should preserve runner-render failures." \
		68 "$runner_status"
	assertEquals "Local parallel source snapshot planning should not emit a partial command when runner rendering fails." \
		"" "$runner_output"
	assertEquals "Local parallel source snapshot planning should preserve parallel shell-render failures." \
		69 "$parallel_status"
	assertEquals "Local parallel source snapshot planning should not emit a partial command when parallel shell rendering fails." \
		"" "$parallel_output"
	assertEquals "Local parallel source snapshot planning should preserve dataset-input render failures." \
		70 "$dataset_status"
	assertEquals "Local parallel source snapshot planning should not emit a partial command when dataset-input rendering fails." \
		"" "$dataset_output"
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
			zxfer_build_source_snapshot_list_cmd
			printf 'meta=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-0}"
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
	assertContains "Remote -j discovery should record that metadata compression was used." \
		"$result" "meta=1"
}

test_get_origin_metadata_compress_safe_uses_configured_default_for_snapshot_metadata() {
	g_option_z_compress=1
	g_option_O_origin_host="origin.example"
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"

	result=$(zxfer_get_origin_metadata_compress_safe)

	assertEquals "Remote snapshot-list metadata should use the configured compressor cost instead of silently strengthening it." \
		"'/remote/bin/zstd' '-3'" "$result"
}

test_get_origin_metadata_compress_safe_covers_disabled_and_custom_resolution() {
	output=$(
		(
			set +e
			g_option_z_compress=0
			zxfer_get_origin_metadata_compress_safe >/dev/null
			printf 'disabled=%s\n' "$?"

			g_option_z_compress=1
			g_option_O_origin_host="origin.example"
			g_cmd_compress="gzip -1"
			g_origin_cmd_compress_safe=""
			zxfer_resolve_remote_cli_command_safe() {
				printf 'resolve:%s:%s:%s:%s\n' "$1" "$2" "$3" "$4"
			}
			zxfer_get_origin_metadata_compress_safe
		)
	)

	assertContains "Metadata compression lookup should fail closed when compression is disabled." \
		"$output" "disabled=1"
	assertContains "Custom metadata compression should resolve through the remote helper path." \
		"$output" "resolve:origin.example:gzip -1:metadata compression command:source"
}

test_build_source_snapshot_name_list_cmd_covers_local_and_remote_rendering() {
	local_result=$(
		(
			g_option_O_origin_host=""
			g_option_j_jobs=4
			zxfer_build_source_snapshot_name_list_cmd
			printf 'parallel=%s\n' "${g_source_snapshot_list_uses_parallel:-unset}"
			printf 'compressed=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-unset}"
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
			zxfer_build_source_snapshot_name_list_cmd
			printf 'parallel=%s\n' "${g_source_snapshot_list_uses_parallel:-unset}"
			printf 'compressed=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-unset}"
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
			zxfer_build_source_snapshot_name_list_cmd
			printf 'compressed=%s\n' "${g_source_snapshot_list_uses_metadata_compression:-unset}"
		)
	)

	assertContains "Local identity-aware no-op proof discovery should render a direct source snapshot list." \
		"$local_result" "'$g_LZFS' 'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$g_initial_source'"
	assertNotContains "Local identity-aware no-op proof discovery should not fan out through parallel before work is proven." \
		"$local_result" "$PARALLEL_BIN"
	assertContains "Local identity-aware no-op proof discovery should record that source fanout was not used." \
		"$local_result" "parallel=0"
	assertContains "Local identity-aware discovery should leave the metadata compression marker cleared." \
		"$local_result" "compressed=0"
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
	assertNotContains "Remote identity-aware no-op proof discovery should not render per-dataset snapshot commands." \
		"$remote_result" "-d"
	assertNotContains "Remote identity-aware discovery should not pay for creation-order sorting on the origin." \
		"$remote_result" "creation"
	assertContains "Remote identity-aware no-op proof discovery should record that source fanout was not used." \
		"$remote_result" "parallel=0"
	assertContains "Uncompressed remote identity-aware discovery should leave the compression marker cleared." \
		"$remote_result" "compressed=0"
	assertContains "Compressed remote identity-aware discovery should use the resolved metadata compressor." \
		"$compressed_result" "/remote/bin/zstd"
	assertContains "Compressed remote identity-aware discovery should preserve the configured metadata compression level." \
		"$compressed_result" "-3"
	assertContains "Compressed remote identity-aware discovery should append the local decompressor." \
		"$compressed_result" "/local/bin/zstd"
	assertNotContains "Compressed remote identity-aware discovery should still defer parallel fanout." \
		"$compressed_result" "/opt/bin/parallel"
	assertContains "Compressed remote identity-aware discovery should record the metadata compression marker." \
		"$compressed_result" "compressed=1"
}

test_build_source_snapshot_name_list_cmd_covers_current_shell_success_paths() {
	local_out="$TEST_TMPDIR/source_name_list_local_serial.out"
	remote_serial_out="$TEST_TMPDIR/source_name_list_remote_serial.out"
	remote_compressed_out="$TEST_TMPDIR/source_name_list_remote_compressed.out"

	g_option_O_origin_host=""
	g_option_j_jobs=3
	g_cmd_parallel="$PARALLEL_BIN"
	zxfer_build_source_snapshot_name_list_cmd >"$local_out"
	local_status=$?

	g_option_O_origin_host="origin.example"
	g_origin_cmd_zfs="/remote/bin/zfs"
	g_option_j_jobs=1
	g_option_z_compress=0
	zxfer_build_source_snapshot_name_list_cmd >"$remote_serial_out"
	remote_serial_status=$?

	g_option_j_jobs=3
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"
	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_origin_cmd_compress_safe="'/remote/bin/zstd' '-3'"
	g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
	zxfer_build_source_snapshot_name_list_cmd >"$remote_compressed_out"
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
			zxfer_build_source_snapshot_name_list_cmd
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
			zxfer_resolve_remote_cli_tool() {
				printf '%s\n' "unexpected-remote-awk"
			}
			zxfer_build_source_snapshot_name_list_cmd
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
	remote_zfs_status=$(
		(
			g_option_O_origin_host="origin.example"
			zxfer_build_shell_command_from_argv() {
				return 31
			}
			zxfer_build_source_snapshot_name_list_cmd >/dev/null
			printf '%s\n' "$?"
		)
	)
	compress_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_z_compress=1
			zxfer_get_origin_metadata_compress_safe() {
				return 32
			}
			zxfer_build_source_snapshot_name_list_cmd >/dev/null
			printf '%s\n' "$?"
		)
	)
	remote_shell_status=$(
		(
			g_option_O_origin_host="origin.example"
			zxfer_build_remote_sh_c_command() {
				return 33
			}
			zxfer_build_source_snapshot_name_list_cmd >/dev/null
			printf '%s\n' "$?"
		)
	)
	ssh_status=$(
		(
			g_option_O_origin_host="origin.example"
			zxfer_build_ssh_shell_command_for_host() {
				return 34
			}
			zxfer_build_source_snapshot_name_list_cmd >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Name-only remote snapshot command rendering should preserve remote zfs command render failures." \
		31 "$remote_zfs_status"
	assertEquals "Name-only remote snapshot command rendering should preserve metadata compression lookup failures." \
		32 "$compress_status"
	assertEquals "Name-only remote snapshot command rendering should preserve remote shell wrapping failures." \
		33 "$remote_shell_status"
	assertEquals "Name-only remote snapshot command rendering should preserve ssh wrapper failures." \
		34 "$ssh_status"
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
			zxfer_resolve_remote_cli_tool() {
				printf '%s\n' "unexpected-remote-awk"
				return 35
			}
			zxfer_build_source_snapshot_name_list_cmd
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
			zxfer_check_parallel_source_jobs_in_current_shell() {
				printf '%s\n' "unexpected-local-parallel-check"
				return 5
			}
			zxfer_build_source_snapshot_name_list_cmd
		)
	)
	local_status=$?
	remote_result=$(
		(
			g_option_j_jobs=2
			g_option_O_origin_host="origin.example"
			g_origin_cmd_zfs="/remote/bin/zfs"
			g_origin_parallel_cmd=""
			zxfer_check_parallel_source_jobs_in_current_shell() {
				printf '%s\n' "unexpected-remote-parallel-check"
				return 1
			}
			zxfer_build_source_snapshot_name_list_cmd
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

	set +e
	output=$(
		(
			zxfer_render_zfs_command_for_spec() {
				if [ "$3" = "-Hr" ]; then
					return 41
				fi
				printf '%s\n' "rendered:$*"
			}
			zxfer_build_source_snapshot_name_list_cmd
		)
	)
	status=$?

	assertEquals "Local identity-aware no-op proof should preserve recursive snapshot-list render failures when -j was requested." \
		41 "$status"
	assertEquals "Local identity-aware no-op proof should not emit a partial command when recursive rendering fails." \
		"" "$output"
}

test_build_source_snapshot_list_cmd_fails_closed_when_remote_parallel_is_unavailable() {
	g_option_j_jobs=2
	g_option_O_origin_host="origin.example"
	g_origin_parallel_cmd=""
	g_cmd_parallel=""
	g_origin_cmd_zfs="/remote/bin/zfs"

	result=$(
		(
			zxfer_check_parallel_source_jobs_in_current_shell() {
				g_zxfer_parallel_source_job_check_result='parallel not found on origin host origin.example but -j 2 was requested. Install parallel remotely or rerun without -j.'
				return 1
			}
			zxfer_build_source_snapshot_list_cmd
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
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_build_ssh_shell_command_for_host() {
				return 79
			}
			zxfer_build_source_snapshot_list_cmd
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

	set +e
	runner_output=$(
		{
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "/opt/bin/parallel" ]; then
					printf '%s\n' "/opt/bin/parallel"
					return 0
				fi
				if [ "$1" = "/remote/bin/zfs" ] && [ "$3" = "-H" ]; then
					return 71
				fi
				printf '%s\n' "unexpected"
			}
			zxfer_build_source_snapshot_list_cmd
		}
	)
	runner_status=$?
	dataset_output=$(
		{
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "/opt/bin/parallel" ]; then
					printf '%s\n' "/opt/bin/parallel"
					return 0
				fi
				if [ "$1" = "/remote/bin/zfs" ] && [ "$3" = "-H" ]; then
					printf '%s\n' "/remote/bin/zfs list -H -o name,guid -s creation -d 1 -t snapshot {}"
					return 0
				fi
				if [ "$1" = "/remote/bin/zfs" ] && [ "$3" = "-Hr" ]; then
					return 72
				fi
				printf '%s\n' "unexpected"
			}
			zxfer_build_source_snapshot_list_cmd
		}
	)
	dataset_status=$?
	remote_shell_output=$(
		{
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_build_shell_command_from_argv() {
				printf '%s\n' "$*"
			}
			zxfer_build_remote_sh_c_command() {
				return 73
			}
			zxfer_build_source_snapshot_list_cmd
		}
	)
	remote_shell_status=$?

	assertEquals "Remote parallel source snapshot planning should preserve remote runner shell-render failures." \
		71 "$runner_status"
	assertEquals "Remote parallel source snapshot planning should not emit a partial command when remote runner shell rendering fails." \
		"" "$runner_output"
	assertEquals "Remote parallel source snapshot planning should preserve remote dataset-input shell-render failures." \
		72 "$dataset_status"
	assertEquals "Remote parallel source snapshot planning should not emit a partial command when remote dataset-input shell rendering fails." \
		"" "$dataset_output"
	assertEquals "Remote parallel source snapshot planning should preserve remote sh -c wrapper failures." \
		73 "$remote_shell_status"
	assertEquals "Remote parallel source snapshot planning should not emit a partial command when remote sh -c wrapper rendering fails." \
		"" "$remote_shell_output"
}

test_build_source_snapshot_list_cmd_preserves_local_parallel_dataset_input_render_failure() {
	g_option_j_jobs=2
	g_option_O_origin_host=""
	g_cmd_parallel="/usr/local/bin/parallel"

	set +e
	output=$(
		(
			zxfer_check_parallel_source_jobs_in_current_shell() {
				return 0
			}
			zxfer_render_zfs_command_for_spec() {
				if [ "$3" = "-Hr" ] && [ "$4" = "-o" ]; then
					printf '%s\n' "serial"
					return 0
				fi
				if [ "$3" = "-H" ]; then
					printf '%s\n' "runner"
					return 0
				fi
				if [ "$3" = "-Hr" ] && [ "$4" = "-t" ]; then
					return 41
				fi
				return 99
			}
			zxfer_build_shell_command_from_argv() {
				printf '%s\n' "$1"
			}
			zxfer_build_source_snapshot_list_cmd
		)
	)
	status=$?

	assertEquals "Local parallel source command rendering should preserve dataset-input render failures." \
		41 "$status"
	assertEquals "Local parallel source command rendering should not emit a partial command when dataset input rendering fails." \
		"" "$output"
}

test_write_source_snapshot_list_to_file_uses_direct_background_runner_when_serial() {
	log="$TEST_TMPDIR/source_serial.log"
	outfile="$TEST_TMPDIR/source_serial.out"
	errfile="$TEST_TMPDIR/source_serial.err"
	: >"$log"

	(
		SOURCE_LOG="$log"
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-serial'"
		}
		zxfer_execute_rendered_background_shell_command() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$SOURCE_LOG"
			g_last_background_pid=4242
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		printf '%s\n' "$g_source_snapshot_list_pid" >>"$SOURCE_LOG"
		printf '%s\n' "$g_source_snapshot_list_job_id" >>"$SOURCE_LOG"
	)

	assertEquals "Serial snapshot listing should delegate to the direct background execution helper." \
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
		zxfer_execute_rendered_background_shell_command() {
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
			printf 'job=%s\n' "$g_source_snapshot_list_job_id"
			printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
			printf 'source_ssh=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
		} >>"$log"
	)

	assertEquals "Very-verbose profiling should count the remote ssh hop used for source snapshot discovery." \
		"printf 'remote-snap-profile'|$outfile|$errfile
pid=3131
job=
ssh=1
source_ssh=1" "$(cat "$log")"
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

test_write_source_snapshot_list_to_file_uses_current_shell_temp_file_result() {
	outfile="$TEST_TMPDIR/source_current_shell.out"
	errfile="$TEST_TMPDIR/source_current_shell.err"
	log="$TEST_TMPDIR/source_current_shell.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result="$TEST_TMPDIR/source_current_shell.cmd"
			: >"$g_zxfer_temp_file_result"
			printf '%s\n' "$TEST_TMPDIR/stdout-only-source-current-shell"
		}
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-current-shell'"
		}
		zxfer_execute_rendered_background_shell_command() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			g_last_background_pid=5151
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
	)

	assertEquals "Source snapshot discovery should stage the built command through the current-shell temp-file result instead of stdout." \
		"printf 'snap-current-shell'|$outfile|$errfile" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_uses_current_shell_read_scratch() {
	outfile="$TEST_TMPDIR/source_read_scratch.out"
	errfile="$TEST_TMPDIR/source_read_scratch.err"
	log="$TEST_TMPDIR/source_read_scratch.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result="$TEST_TMPDIR/source_read_scratch.cmd"
			: >"$g_zxfer_temp_file_result"
			printf '%s\n' "$TEST_TMPDIR/stdout-only-source-read-scratch"
		}
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf 'snap-read-scratch'"
		}
		zxfer_read_snapshot_discovery_capture_file() {
			g_zxfer_snapshot_discovery_file_read_result="printf 'snap-read-scratch'"
			return 0
		}
		zxfer_execute_rendered_background_shell_command() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			g_last_background_pid=6161
		}
		g_option_j_jobs=1
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
	)

	assertEquals "Source snapshot discovery should use the current-shell staged-command read scratch instead of stdout from the file-read helper." \
		"printf 'snap-read-scratch'|$outfile|$errfile" "$(cat "$log")"
}

test_write_source_snapshot_list_to_file_reports_staged_command_read_failures_after_build_failure() {
	outfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_failure.out"
	errfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_failure.err"
	cmd_tmp="$TEST_TMPDIR/source_cmd_read_fail_after_build_failure.cmd"

	zxfer_test_capture_subshell "
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result='$cmd_tmp'
			: >\"\$g_zxfer_temp_file_result\"
			printf '%s\n' '$TEST_TMPDIR/stdout-only-source-cmd-read-failure'
		}
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' 'builder failed'
			return 1
		}
		zxfer_read_snapshot_discovery_capture_file() {
			return 1
		}
		zxfer_write_source_snapshot_list_to_file '$outfile' '$errfile'
	"

	assertEquals "Source snapshot discovery should fail closed when the staged command cannot be read back after build failure." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Source snapshot discovery should report the staged-command read failure after build failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to read staged source snapshot discovery command after build failure."
}

test_write_source_snapshot_list_to_file_trims_staged_build_failure_newline() {
	outfile="$TEST_TMPDIR/source_cmd_trim_build_failure.out"
	errfile="$TEST_TMPDIR/source_cmd_trim_build_failure.err"
	cmd_tmp="$TEST_TMPDIR/source_cmd_trim_build_failure.cmd"

	zxfer_test_capture_subshell "
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result='$cmd_tmp'
			: >\"\$g_zxfer_temp_file_result\"
			printf '%s\n' '$TEST_TMPDIR/stdout-only-source-cmd-trim-build-failure'
		}
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' 'builder failed'
			return 1
		}
		zxfer_throw_error() {
			printf '<%s>' \"\$1\"
			exit 1
		}
		zxfer_write_source_snapshot_list_to_file '$outfile' '$errfile'
	"

	assertEquals "Source snapshot discovery should fail closed when staged command construction fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Source snapshot discovery should trim the formatter newline before surfacing staged build-failure output." \
		"<builder failed>" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_write_source_snapshot_list_to_file_reports_staged_command_read_failures_after_build_success() {
	outfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_success.out"
	errfile="$TEST_TMPDIR/source_cmd_read_fail_after_build_success.err"
	cmd_tmp="$TEST_TMPDIR/source_cmd_read_fail_after_build_success.cmd"

	zxfer_test_capture_subshell "
		zxfer_get_temp_file() {
			g_zxfer_temp_file_result='$cmd_tmp'
			: >\"\$g_zxfer_temp_file_result\"
			printf '%s\n' '$TEST_TMPDIR/stdout-only-source-cmd-read-success'
		}
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' \"printf 'snap-build-success'\"
		}
		zxfer_read_snapshot_discovery_capture_file() {
			return 1
		}
		zxfer_write_source_snapshot_list_to_file '$outfile' '$errfile'
	"

	assertEquals "Source snapshot discovery should fail closed when the staged command cannot be read back after a successful build." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Source snapshot discovery should report the staged-command read failure after successful build staging." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to read staged source snapshot discovery command."
}

test_write_source_snapshot_list_to_file_skips_execution_in_dry_run() {
	outfile="$TEST_TMPDIR/source_dry_run.out"
	errfile="$TEST_TMPDIR/source_dry_run.err"
	log="$TEST_TMPDIR/source_dry_run.log"
	: >"$log"

	output=$(
		(
			LOG_FILE="$log"
			zxfer_echoV() {
				printf '%s\n' "$*" >>"$LOG_FILE"
			}
			zxfer_build_source_snapshot_list_cmd() {
				printf '%s\n' "build-source-command-called" >>"$LOG_FILE"
				printf '%s\n' "printf 'snap-dry-run'"
			}
			zxfer_execute_rendered_background_shell_command() {
				printf '%s\n' "execute-background-called" >>"$LOG_FILE"
			}
			g_option_n_dryrun=1
			g_option_j_jobs=3
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'pid=%s\n' "${g_source_snapshot_list_pid:-}"
			printf 'outfile_exists=%s\n' "$([ -f "$outfile" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'errfile_exists=%s\n' "$([ -f "$errfile" ] && printf '%s' 1 || printf '%s' 0)"
			printf 'outfile_size=%s\n' "$(wc -c <"$outfile" 2>/dev/null | tr -d '[:space:]' || printf '%s' missing)"
			printf 'errfile_size=%s\n' "$(wc -c <"$errfile" 2>/dev/null | tr -d '[:space:]' || printf '%s' missing)"
		)
	)

	assertNotContains "Dry-run source snapshot discovery should not invoke the background execution helper." \
		"$(cat "$log")" "execute-background-called"
	assertNotContains "Dry-run source snapshot discovery should not enter parallel command planning." \
		"$(cat "$log")" "build-source-command-called"
	assertContains "Dry-run source snapshot discovery should render the skipped command." \
		"$(cat "$log")" "'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' 'tank/src'"
	assertContains "Dry-run source snapshot discovery should leave the background PID unset." \
		"$output" "pid="
	assertContains "Dry-run source snapshot discovery should create the snapshot tempfile placeholder." \
		"$output" "outfile_exists=1"
	assertContains "Dry-run source snapshot discovery should create the stderr tempfile placeholder." \
		"$output" "errfile_exists=1"
	assertContains "Dry-run source snapshot discovery should leave the snapshot tempfile empty." \
		"$output" "outfile_size=0"
	assertContains "Dry-run source snapshot discovery should leave the stderr tempfile empty." \
		"$output" "errfile_size=0"
}

test_write_source_snapshot_list_to_file_reports_preview_render_failures_in_dry_run() {
	outfile="$TEST_TMPDIR/source_dry_run_error.out"
	errfile="$TEST_TMPDIR/source_dry_run_error.err"

	zxfer_test_capture_subshell "
		zxfer_render_zfs_command_for_spec() {
			printf '%s\n' 'preview render failed'
			return 1
		}
		g_option_n_dryrun=1
		zxfer_write_source_snapshot_list_to_file '$outfile' '$errfile'
	"

	assertEquals "Dry-run source snapshot discovery should fail closed when preview rendering fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Dry-run source snapshot discovery should surface the preview render failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "preview render failed"
}

test_write_source_snapshot_list_to_file_preserves_outfile_stage_failures_in_dry_run() {
	outfile="$TEST_TMPDIR/source_dry_run_stage_failure.out"
	errfile="$TEST_TMPDIR/source_dry_run_stage_failure.err"

	output=$(
		(
			write_call_count=0
			zxfer_write_runtime_artifact_file() {
				write_call_count=$((write_call_count + 1))
				printf 'write=%s:%s\n' "$write_call_count" "$1"
				return 23
			}
			g_option_n_dryrun=1
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'calls=%s\n' "$write_call_count"
		)
	)

	assertContains "Dry-run source snapshot discovery should preserve outfile staging failures." \
		"$output" "status=23"
	assertContains "Dry-run source snapshot discovery should stop after the outfile stage fails." \
		"$output" "calls=1"
	assertContains "Dry-run source snapshot discovery should fail on the snapshot outfile stage first." \
		"$output" "write=1:$outfile"
}

test_write_source_snapshot_list_to_file_preserves_errfile_stage_failures_in_dry_run() {
	outfile="$TEST_TMPDIR/source_dry_run_err_stage_failure.out"
	errfile="$TEST_TMPDIR/source_dry_run_err_stage_failure.err"

	output=$(
		(
			write_call_count=0
			zxfer_write_runtime_artifact_file() {
				write_call_count=$((write_call_count + 1))
				printf 'write=%s:%s\n' "$write_call_count" "$1"
				if [ "$write_call_count" -eq 1 ]; then
					return 0
				fi
				return 29
			}
			g_option_n_dryrun=1
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'calls=%s\n' "$write_call_count"
		)
	)

	assertContains "Dry-run source snapshot discovery should preserve stderr staging failures." \
		"$output" "status=29"
	assertContains "Dry-run source snapshot discovery should attempt the stderr stage after the outfile stage succeeds." \
		"$output" "calls=2"
	assertContains "Dry-run source snapshot discovery should still stage the snapshot outfile before surfacing the stderr failure." \
		"$output" "write=1:$outfile"
	assertContains "Dry-run source snapshot discovery should report the stderr staging failure from the second write." \
		"$output" "write=2:$errfile"
}

test_write_source_snapshot_list_to_file_stages_sorted_sidecar_in_dry_run_when_requested() {
	outfile="$TEST_TMPDIR/source_dry_run_sorted.out"
	errfile="$TEST_TMPDIR/source_dry_run_sorted.err"
	temp_counter_file="$TEST_TMPDIR/source_dry_run_sorted.counter"
	printf '%s\n' 0 >"$temp_counter_file"

	output=$(
		(
			COUNTER_FILE="$temp_counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$COUNTER_FILE")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$COUNTER_FILE"
				g_zxfer_temp_file_result="$TEST_TMPDIR/source_dry_run_sorted.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-source-dry-run-sorted.$idx"
			}
			g_option_n_dryrun=1
			g_source_snapshot_list_background_sort_requested=1
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'sorted=%s\n' "$g_source_snapshot_list_sorted_file"
			printf 'sorted_size=%s\n' "$(wc -c <"$g_source_snapshot_list_sorted_file" 2>/dev/null | tr -d '[:space:]' || printf '%s' missing)"
		)
	)

	assertContains "Dry-run source discovery should publish an empty sorted sidecar when the caller requested background sorting." \
		"$output" "sorted=$TEST_TMPDIR/source_dry_run_sorted.1"
	assertContains "Dry-run source discovery should keep the requested sorted sidecar empty." \
		"$output" "sorted_size=0"
}

test_write_source_snapshot_list_to_file_reports_sorted_sidecar_temp_failures_in_dry_run() {
	outfile="$TEST_TMPDIR/source_dry_run_sorted_tempfail.out"
	errfile="$TEST_TMPDIR/source_dry_run_sorted_tempfail.err"

	output=$(
		(
			g_option_n_dryrun=1
			g_source_snapshot_list_background_sort_requested=1
			zxfer_get_temp_file() {
				return 31
			}
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Dry-run source discovery should preserve sorted-sidecar tempfile allocation failures." \
		"$output" "status=31"
}

test_write_source_snapshot_list_to_file_preserves_background_sort_setup_failures() {
	outfile="$TEST_TMPDIR/source_background_sort_setup_failure.out"
	errfile="$TEST_TMPDIR/source_background_sort_setup_failure.err"

	output=$(
		(
			g_source_snapshot_list_background_sort_requested=1
			zxfer_build_source_snapshot_list_cmd() {
				printf '%s\n' "printf '%s\n' snap"
			}
			zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
				return 32
			}
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'status=%s\n' "$?"
			printf 'sorted=%s\n' "${g_source_snapshot_list_sorted_file:-}"
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
			g_source_snapshot_list_background_sort_requested=1
			zxfer_get_temp_file() {
				temp_calls=$((temp_calls + 1))
				if [ "$temp_calls" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/source_background_sort_temp_failure.cmd"
					: >"$g_zxfer_temp_file_result"
					printf '%s\n' "$TEST_TMPDIR/stdout-only-source-background-sort-temp-failure"
					return 0
				fi
				return 33
			}
			zxfer_build_source_snapshot_list_cmd() {
				printf '%s\n' "printf '%s\n' snap"
			}
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'status=%s\n' "$?"
			printf 'calls=%s\n' "$temp_calls"
		)
	)

	assertContains "Source discovery should preserve sorted-sidecar tempfile failures before launching the background job." \
		"$output" "status=33"
	assertContains "Source discovery should attempt command and sorted-sidecar tempfile allocation." \
		"$output" "calls=2"
}

test_write_source_snapshot_list_to_file_preserves_direct_background_execution_failures() {
	outfile="$TEST_TMPDIR/source_background_direct_failure.out"
	errfile="$TEST_TMPDIR/source_background_direct_failure.err"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				printf '%s\n' "printf '%s\n' snap"
			}
			zxfer_execute_rendered_background_shell_command() {
				return 34
			}
			set +e
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Source discovery should preserve direct background execution failures." \
		"$output" "status=34"
}

test_write_source_snapshot_list_to_file_runs_serial_builder_output_when_jobs_remain_configured() {
	g_option_j_jobs=2
	outfile="$TEST_TMPDIR/source_parallel_fallback.out"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				printf '%s\n' "printf '%s\n' serial-fallback"
			}
			zxfer_write_source_snapshot_list_to_file "$outfile"
			wait "$g_source_snapshot_list_pid"
			printf 'payload=%s\n' "$(cat "$outfile")"
			printf 'job=%s\n' "${g_source_snapshot_list_job_id:-}"
		) 2>&1
	)
	status=$?

	assertEquals "Snapshot-list execution should still succeed when the builder returns a serial command string while -j remains configured." \
		0 "$status"
	assertContains "Snapshot-list execution should run the builder's serial command output through the background eval path." \
		"$output" "payload=serial-fallback"
	assertContains "Snapshot-list execution should use direct PID waiting instead of a supervised job id." \
		"$output" "job="
}

test_write_source_snapshot_list_to_file_can_sort_inside_background_job() {
	outfile="$TEST_TMPDIR/source_background_sort.out"
	errfile="$TEST_TMPDIR/source_background_sort.err"
	sorted_path_file="$TEST_TMPDIR/source_background_sort.path"

	(
		g_source_snapshot_list_background_sort_requested=1
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf '%s\n' tank/src@b tank/src@a"
		}
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		wait "$g_source_snapshot_list_pid"
		printf '%s\n' "$g_source_snapshot_list_sorted_file" >"$sorted_path_file"
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
		g_source_snapshot_list_background_sort_requested=1
		zxfer_build_source_snapshot_list_cmd() {
			printf '%s\n' "printf '%s\n' tank/src@partial; exit 37"
		}
		zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile"
		wait "$g_source_snapshot_list_pid"
		printf 'status=%s\n' "$?" >"$sorted_path_file"
		printf 'sorted=%s\n' "$g_source_snapshot_list_sorted_file" >>"$sorted_path_file"
	)
	result=$(cat "$sorted_path_file")
	sorted_file=$(printf '%s\n' "$result" | sed -n 's/^sorted=//p')

	assertContains "Streaming background sort should preserve the source-list command status." \
		"$result" "status=37"
	assertEquals "Streaming background sort should preserve partial raw output for diagnostics." \
		"tank/src@partial" "$(cat "$outfile")"
	zxfer_cleanup_runtime_artifact_path "$sorted_file"
}

test_execute_source_snapshot_list_background_cmd_with_sort_delegates_when_no_sorted_file_requested() {
	log="$TEST_TMPDIR/source_background_sort_delegate.log"
	outfile="$TEST_TMPDIR/source_background_sort_delegate.out"
	errfile="$TEST_TMPDIR/source_background_sort_delegate.err"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_execute_rendered_background_shell_command() {
			printf '%s|%s|%s\n' "$1" "$2" "$3" >"$LOG_FILE"
			g_last_background_pid=7171
			return 0
		}
		zxfer_execute_source_snapshot_list_background_cmd_with_sort \
			"printf '%s\n' delegated" "$outfile" "$errfile" ""
		printf 'pid=%s\n' "$g_last_background_pid" >>"$LOG_FILE"
	)
	status=$?

	assertEquals "Background sort execution should delegate to the direct background helper when no sorted sidecar is requested." \
		0 "$status"
	assertEquals "Delegated background execution should preserve command and file arguments." \
		"printf '%s\n' delegated|$outfile|$errfile
pid=7171" "$(cat "$log")"
}

test_execute_source_snapshot_list_background_cmd_with_sort_preserves_setup_failures() {
	outfile="$TEST_TMPDIR/source_background_sort_setup.out"
	errfile="$TEST_TMPDIR/source_background_sort_setup.err"
	sorted_file="$TEST_TMPDIR/source_background_sort_setup.sorted"
	status_files="$TEST_TMPDIR/source_background_sort_setup.status1
$TEST_TMPDIR/source_background_sort_setup.status2"

	wrapper_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				return 11
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	output_quote_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_build_shell_command_from_argv() {
				return 12
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	sorted_quote_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "$outfile" ]; then
					printf '%s\n' "$outfile"
					return 0
				fi
				return 13
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	group_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_create_temp_file_group() {
				return 14
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	source_status_quote_status=$(
		(
			STATUS_FILES="$status_files"
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_create_temp_file_group() {
				g_zxfer_temp_file_group_result=$STATUS_FILES
				return 0
			}
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "$outfile" ] || [ "$1" = "$sorted_file" ]; then
					printf '%s\n' "$1"
					return 0
				fi
				return 15
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	tee_status_quote_status=$(
		(
			STATUS_FILES="$status_files"
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_create_temp_file_group() {
				g_zxfer_temp_file_group_result=$STATUS_FILES
				return 0
			}
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "$outfile" ] ||
					[ "$1" = "$sorted_file" ] ||
					[ "$1" = "$TEST_TMPDIR/source_background_sort_setup.status1" ]; then
					printf '%s\n' "$1"
					return 0
				fi
				return 16
			}
			set +e
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" "$outfile" "$errfile" "$sorted_file"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Background sort setup should fail closed when cleanup-wrapper lookup fails." \
		1 "$wrapper_status"
	assertEquals "Background sort setup should preserve raw-output quote failures." \
		12 "$output_quote_status"
	assertEquals "Background sort setup should preserve sorted-output quote failures." \
		13 "$sorted_quote_status"
	assertEquals "Background sort setup should preserve status-tempfile allocation failures." \
		14 "$group_status"
	assertEquals "Background sort setup should preserve source-status quote failures." \
		15 "$source_status_quote_status"
	assertEquals "Background sort setup should preserve tee-status quote failures." \
		16 "$tee_status_quote_status"
}

test_execute_source_snapshot_list_background_cmd_with_sort_aborts_child_when_registration_fails() {
	outfile="$TEST_TMPDIR/source_background_sort_register.out"
	errfile="$TEST_TMPDIR/source_background_sort_register.err"
	sorted_file="$TEST_TMPDIR/source_background_sort_register.sorted"
	log="$TEST_TMPDIR/source_background_sort_register.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_register_cleanup_pid() {
			printf 'register=%s:%s\n' "$1" "$2" >>"$LOG_FILE"
			return 1
		}
		zxfer_abort_direct_child_pid() {
			printf 'abort=%s:%s:%s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			kill "$1" 2>/dev/null || :
			return 0
		}
		set +e
		zxfer_execute_source_snapshot_list_background_cmd_with_sort \
			"sleep 5" "$outfile" "$errfile" "$sorted_file"
		printf 'status=%s\n' "$?" >>"$LOG_FILE"
	)

	assertContains "Background sort registration failures should attempt to abort the launched helper." \
		"$(cat "$log")" "abort="
	assertContains "Background sort registration failures should return failure." \
		"$(cat "$log")" "status=1"
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
	status_file="$g_zxfer_run_tmp_root/source_name_background_sort_setup.status"

	wrapper_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				return 21
			}
			set +e
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"printf x" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	output_quote_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_build_shell_command_from_argv() {
				return 22
			}
			set +e
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"printf x" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	temp_status=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_get_temp_file() {
				return 23
			}
			set +e
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"printf x" "$sorted_file"
			printf '%s\n' "$?"
		)
	)
	status_quote_status=$(
		(
			STATUS_FILE="$status_file"
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "/bin/sh"
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$STATUS_FILE
				: >"$STATUS_FILE"
				return 0
			}
			zxfer_build_shell_command_from_argv() {
				if [ "$1" = "$sorted_file" ]; then
					printf '%s\n' "$1"
					return 0
				fi
				return 24
			}
			set +e
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"printf x" "$sorted_file"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Name-only background sort setup should fail closed when cleanup-wrapper lookup fails." \
		1 "$wrapper_status"
	assertEquals "Name-only background sort setup should preserve sorted-output quote failures." \
		22 "$output_quote_status"
	assertEquals "Name-only background sort setup should preserve temp-file allocation failures." \
		23 "$temp_status"
	assertEquals "Name-only background sort setup should preserve status-file quote failures." \
		24 "$status_quote_status"
	assertFalse "Name-only background sort setup should clean up the status file after quote failures." \
		"[ -e '$status_file' ]"
}

test_execute_source_snapshot_name_list_background_sort_cmd_aborts_child_when_registration_fails() {
	sorted_file="$TEST_TMPDIR/source_name_background_sort_register.sorted"
	log="$TEST_TMPDIR/source_name_background_sort_register.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_register_cleanup_pid() {
			printf 'register=%s:%s\n' "$1" "$2" >>"$LOG_FILE"
			return 1
		}
		zxfer_abort_direct_child_pid() {
			printf 'abort=%s:%s:%s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			kill "$1" 2>/dev/null || :
			return 0
		}
		set +e
		zxfer_execute_source_snapshot_name_list_background_sort_cmd \
			"sleep 5" "$sorted_file"
		printf 'status=%s\n' "$?" >>"$LOG_FILE"
	)

	assertContains "Name-only background sort registration failures should abort the launched helper." \
		"$(cat "$log")" "abort="
	assertContains "Name-only background sort registration failures should return failure." \
		"$(cat "$log")" "status=1"
}
