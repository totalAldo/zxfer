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

create_selective_awk_failure_bin() {
	l_path=$1
	l_exit_status=$2
	l_real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	cat >"$l_path" <<EOF
#!/bin/sh
if [ "\$1" = "-F@" ]; then
	printf '%s\n' "awk failed" >&2
	exit $l_exit_status
fi
exec "$l_real_awk" "\$@"
EOF
	chmod +x "$l_path"
}

create_fake_ssh_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
if [ "$1" = "-M" ] && [ "$2" = "-V" ]; then
	exit 1
fi
printf '%s\n' "$@"
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
ZXFER_REMOTE_CAPS_V2
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
	PARALLEL_BIN="$TEST_TMPDIR/parallel"
	ALT_PARALLEL_BIN="$TEST_TMPDIR/alt_parallel"
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_parallel_bin "$PARALLEL_BIN" "parallel (fake)"
	create_parallel_bin "$ALT_PARALLEL_BIN" "parallel from elsewhere"
	create_fake_ssh_bin "$FAKE_SSH_BIN"
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
	g_origin_remote_capabilities_dependency_path=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_dependency_path=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_cmd_parallel="$PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_RZFS="/sbin/zfs"
	g_LZFS="/sbin/zfs"
	g_cmd_zfs="/sbin/zfs"
	g_target_cmd_zfs=""
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_recursive_dest_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_file_read_result=""
	g_zxfer_parallel_source_job_check_kind=""
	g_zxfer_recursive_dataset_list_result=""
	g_zxfer_linear_reverse_max_lines=""
	g_cmd_ps=${g_cmd_ps:-$(command -v ps 2>/dev/null || printf '%s\n' ps)}
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_last_background_pid=""
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_job_id=""
	g_zxfer_temp_file_result=""
	zxfer_reset_background_job_state
	zxfer_reset_destination_existence_cache
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
}

test_zxfer_reset_snapshot_discovery_state_preserves_remote_parallel_state() {
	g_origin_parallel_cmd="/opt/bin/parallel"
	g_origin_parallel_cmd_host="origin.example"
	g_zxfer_snapshot_discovery_file_read_result="printf 'snap'"
	g_zxfer_parallel_source_job_check_kind="origin_missing"
	g_zxfer_recursive_dataset_list_result="tank/src"
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/source_cache.raw"
	g_zxfer_destination_snapshot_record_cache_file="$TEST_TMPDIR/destination_cache.raw"
	printf '%s\n' "tank/src@snap1" >"$g_zxfer_source_snapshot_record_cache_file"
	printf '%s\n' "backup/dst/src@snap1" >"$g_zxfer_destination_snapshot_record_cache_file"

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
	assertFalse "Resetting snapshot discovery state should remove the staged source snapshot-record cache file." \
		"[ -e '$TEST_TMPDIR/source_cache.raw' ]"
	assertFalse "Resetting snapshot discovery state should remove the staged destination snapshot-record cache file." \
		"[ -e '$TEST_TMPDIR/destination_cache.raw' ]"
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
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "unexpected version probe"
				return 42
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
		zxfer_execute_background_cmd() {
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
		zxfer_execute_background_cmd() {
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
		zxfer_execute_background_cmd() {
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
			zxfer_execute_background_cmd() {
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
		zxfer_render_source_snapshot_list_preview_cmd() {
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

test_zxfer_should_use_linear_reverse_for_file_preserves_line_count_failures() {
	input_file="$TEST_TMPDIR/reverse_wc_input.txt"
	failing_awk="$TEST_TMPDIR/reverse_count_awk_fails"
	printf '%s\n' "tank/src@snap-a" >"$input_file"
	cat >"$failing_awk" <<'EOF'
#!/bin/sh
exit 37
EOF
	chmod +x "$failing_awk"

	output=$(
		(
			g_cmd_awk=$failing_awk
			zxfer_should_use_linear_reverse_for_file "$input_file"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "Line-count helper failures should return the exact underlying status." \
		"status=37" "$output"
}

test_zxfer_should_use_linear_reverse_for_file_rejects_malformed_line_counts() {
	input_file="$TEST_TMPDIR/reverse_malformed_count_input.txt"
	malformed_awk="$TEST_TMPDIR/reverse_count_awk_malformed"
	printf '%s\n' "tank/src@snap-a" >"$input_file"
	cat >"$malformed_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "not-a-number"
EOF
	chmod +x "$malformed_awk"

	output=$(
		(
			g_cmd_awk=$malformed_awk
			zxfer_should_use_linear_reverse_for_file "$input_file"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "Malformed line-count helper output should disable the linear awk fast path." \
		"status=1" "$output"
}

test_reverse_numbered_line_stream_reports_tempfile_allocation_failures() {
	output=$(
		(
			zxfer_get_temp_file() {
				return 44
			}
			printf '%s\n' "     1	tank/src@snap-a" | zxfer_reverse_numbered_line_stream
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "zxfer_reverse_numbered_line_stream should preserve temp-file allocation failures." \
		"status=44" "$output"
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

test_reverse_numbered_line_stream_uses_current_shell_temp_file_result() {
	output=$(
		(
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/reverse_numbered_current_shell.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-reverse-numbered"
			}
			cat <<'EOF' | zxfer_reverse_numbered_line_stream
     1	tank/src@snap-a
     2	tank/src@snap-b
EOF
		)
	)

	assertEquals "Reversing numbered snapshot lines should use the current-shell temp-file result instead of stdout." \
		"tank/src@snap-b
tank/src@snap-a" "$output"
}

test_zxfer_reverse_plain_file_lines_with_sort_reports_tempfile_allocation_failures() {
	input_file="$TEST_TMPDIR/reverse_sort_temp_failure_input.txt"
	printf '%s\n' "tank/src@snap-a" >"$input_file"

	output=$(
		(
			zxfer_get_temp_file() {
				return 45
			}
			zxfer_reverse_plain_file_lines_with_sort "$input_file"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "zxfer_reverse_plain_file_lines_with_sort should preserve temp-file allocation failures." \
		"status=45" "$output"
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

test_zxfer_reverse_plain_file_lines_with_sort_uses_current_shell_temp_file_result() {
	input_file="$TEST_TMPDIR/reverse_plain_current_shell_input.txt"
	printf '%s\n' "tank/src@snap-a" >"$input_file"
	printf '%s\n' "tank/src@snap-b" >>"$input_file"

	output=$(
		(
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/reverse_plain_current_shell.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-reverse-plain"
			}
			zxfer_reverse_plain_file_lines_with_sort "$input_file"
		)
	)

	assertEquals "Plain-file reverse fallback should use the current-shell temp-file result instead of stdout." \
		"tank/src@snap-b
tank/src@snap-a" "$output"
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
	zxfer_reset_runtime_artifact_state

	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp" >"$verbose_file" 2>&1
	output=$(cat "$verbose_file")

	assertEquals "Excluded datasets should be removed from the transfer list." "tank/src" "$g_recursive_source_list"
	assertEquals "Excluded datasets should also be removed from the dataset cache." "tank/src" "$g_recursive_source_dataset_list"
	assertEquals "Successful recursive source-list discovery should not leave stale runtime-artifact cleanup registrations behind." \
		"" "${g_zxfer_runtime_artifact_cleanup_paths:-}"
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

test_normalize_destination_snapshot_list_rewrites_only_leading_destination_prefix() {
	input_file="$TEST_TMPDIR/dest_repeated_prefix_input.txt"
	output_file="$TEST_TMPDIR/dest_repeated_prefix_output.txt"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
	{
		printf '%s\t%s\n' "backup/dst/backup/dst/child@snap2" "222"
		printf '%s\t%s\n' "backup/dst@snap1" "111"
	} >"$input_file"

	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"

	expected=$(printf '%s\t%s\n%s\t%s' \
		"tank/src/backup/dst/child@snap2" "222" \
		"tank/src@snap1" "111")
	assertEquals "Destination normalization should rewrite only the leading destination root prefix." \
		"$expected" "$(cat "$output_file")"
}

test_normalize_destination_snapshot_list_does_not_rewrite_similar_dataset_prefixes() {
	input_file="$TEST_TMPDIR/dest_similar_prefix_input.txt"
	output_file="$TEST_TMPDIR/dest_similar_prefix_output.txt"
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
	cat <<'EOF' >"$input_file"
backup/dst-old@snap1
backup/dst@snap1
EOF

	zxfer_normalize_destination_snapshot_list "backup/dst" "$input_file" "$output_file"

	expected=$(printf '%s\n%s' "backup/dst-old@snap1" "tank/src@snap1")
	assertEquals "Destination normalization should not rewrite datasets that only share a text prefix." \
		"$expected" "$(cat "$output_file")"
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
	assertNotContains "Recursive delta planning should not leak current-shell temp file paths into stdout when no datasets differ." \
		"$output" "$TEST_TMPDIR/zxfer."
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

test_set_g_recursive_source_list_queues_name_identical_guid_divergence_from_initial_records() {
	source_tmp="$TEST_TMPDIR/source_guid_divergence.txt"
	dest_tmp="$TEST_TMPDIR/dest_guid_divergence.txt"
	output_file="$TEST_TMPDIR/source_guid_divergence.out"
	cat <<'EOF' >"$source_tmp"
tank/src@same	111
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@same	999
EOF
	sort "$source_tmp" -o "$source_tmp"
	sort "$dest_tmp" -o "$dest_tmp"
	g_option_x_exclude_datasets=""

	(
		zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		printf 'source=%s\n' "$g_recursive_source_list"
		printf 'dest=%s\n' "$g_recursive_destination_extra_dataset_list"
	) >"$output_file"

	assertContains "Initial identity-aware discovery should queue same-name source snapshots with different GUIDs for transfer planning." \
		"$(cat "$output_file")" "source=tank/src"
	assertContains "Initial identity-aware discovery should queue same-name destination snapshots with different GUIDs for delete/common-snapshot inspection." \
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

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_source_identity_diff_failures() {
	source_tmp="$TEST_TMPDIR/refine_guid_source_diff_error_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_guid_source_diff_error_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same\t111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same\t111"
				fi
			}
			zxfer_diff_snapshot_lists() {
				if [ "$3" = "source_minus_destination" ]; then
					return 4
				fi
				return 0
			}

			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve source-minus-destination identity diff failures." \
		4 "$status"
	assertContains "Source identity diff failures should surface the recursive dataset being validated." \
		"$output" "Failed to diff source and destination snapshot identities for [tank/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_destination_identity_diff_failures() {
	source_tmp="$TEST_TMPDIR/refine_guid_destination_diff_error_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_guid_destination_diff_error_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same\t111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same\t111"
				fi
			}
			zxfer_diff_snapshot_lists() {
				if [ "$3" = "destination_minus_source" ]; then
					return 5
				fi
				return 0
			}

			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve destination-minus-source identity diff failures." \
		5 "$status"
	assertContains "Destination identity diff failures should surface the recursive dataset being validated." \
		"$output" "Failed to diff destination and source snapshot identities for [tank/src]."
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

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_common_dataset_derivation_awk_failures() {
	source_tmp="$TEST_TMPDIR/refine_common_dataset_awk_error_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_common_dataset_awk_error_dest.txt"
	fake_awk="$TEST_TMPDIR/refine_common_dataset_awk_fail.sh"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 11

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"

			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve common recursive dataset derivation failures." \
		11 "$status"
	assertContains "Common recursive dataset derivation failures should preserve the upstream awk failure." \
		"$output" "awk failed"
	assertContains "Common recursive dataset derivation failures should report a specific identity-validation error." \
		"$output" "Failed to derive recursive common dataset list for snapshot identity validation."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_invalid_exclude_pattern_failures() {
	source_tmp="$TEST_TMPDIR/refine_invalid_exclude_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_invalid_exclude_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_option_x_exclude_datasets='['

	set +e
	output=$(
		(
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve invalid exclude-filter statuses." \
		2 "$status"
	assertContains "Identity-validation exclude filter failures should report the candidate-dataset context." \
		"$output" "Failed to filter recursive candidate dataset list against exclude patterns during snapshot identity validation."
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

test_set_g_recursive_source_list_reports_recursive_snapshot_diff_failures() {
	source_tmp="$TEST_TMPDIR/recursive_diff_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_diff_failure_dest.txt"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_diff_snapshot_lists() {
				if [ "$3" = "source_minus_destination" ]; then
					return 6
				fi
				return 0
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when the source-minus-destination diff fails." \
		6 "$status"
	assertContains "Recursive delta planning should preserve a specific transfer-planning diff error." \
		"$output" "Failed to diff source and destination snapshots for recursive transfer planning."
}

test_set_g_recursive_source_list_reports_recursive_source_dataset_transfer_awk_failures() {
	source_tmp="$TEST_TMPDIR/recursive_source_transfer_awk_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_source_transfer_awk_failure_dest.txt"
	fake_awk="$TEST_TMPDIR/recursive_source_transfer_awk_fail.sh"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src@snap2" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 8

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
				return 0
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when deriving the source transfer dataset list fails before sort notices." \
		8 "$status"
	assertContains "Recursive delta planning should preserve the upstream awk failure from the source transfer dataset derivation." \
		"$output" "awk failed"
	assertContains "Recursive delta planning should report a specific source transfer dataset derivation error." \
		"$output" "Failed to derive recursive source dataset transfer list."
}

test_set_g_recursive_source_list_reports_recursive_destination_dataset_delete_awk_failures() {
	source_tmp="$TEST_TMPDIR/recursive_destination_delete_awk_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_destination_delete_awk_failure_dest.txt"
	fake_awk="$TEST_TMPDIR/recursive_destination_delete_awk_fail.sh"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n%s\n' "tank/src/child@extra" "tank/src@snap1" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 9

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
				return 0
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delete-only planning should fail closed when deriving destination delete datasets fails before sort notices." \
		9 "$status"
	assertContains "Recursive delete-only planning should preserve the upstream awk failure from the destination delete dataset derivation." \
		"$output" "awk failed"
	assertContains "Recursive delete-only planning should report a specific destination delete dataset derivation error." \
		"$output" "Failed to derive recursive destination dataset delete list."
}

test_set_g_recursive_source_list_reports_recursive_source_dataset_inventory_failures() {
	source_tmp="$TEST_TMPDIR/recursive_source_inventory_error_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_source_inventory_error_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_diff_snapshot_lists() {
				return 0
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
				return 0
			}
			sort() {
				if [ "$1" = "-u" ]; then
					return 7
				fi
				command sort "$@"
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when source dataset inventory derivation fails." \
		7 "$status"
	assertContains "Recursive delta planning should preserve a specific source dataset inventory error." \
		"$output" "Failed to derive recursive source dataset inventory."
}

test_set_g_recursive_source_list_reports_recursive_source_dataset_inventory_awk_failures() {
	source_tmp="$TEST_TMPDIR/recursive_source_inventory_awk_error_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_source_inventory_awk_error_dest.txt"
	fake_awk="$TEST_TMPDIR/recursive_source_inventory_awk_fail.sh"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"
	create_selective_awk_failure_bin "$fake_awk" 10

	set +e
	output=$(
		(
			g_cmd_awk="$fake_awk"
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
				return 0
			}

			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when source dataset inventory derivation fails before sort notices." \
		10 "$status"
	assertContains "Recursive delta planning should preserve the upstream awk failure from source dataset inventory derivation." \
		"$output" "awk failed"
	assertContains "Recursive delta planning should report a specific source dataset inventory error." \
		"$output" "Failed to derive recursive source dataset inventory."
}

test_set_g_recursive_source_list_reports_invalid_exclude_pattern_failures() {
	source_tmp="$TEST_TMPDIR/recursive_exclude_pattern_failure_source.txt"
	dest_tmp="$TEST_TMPDIR/recursive_exclude_pattern_failure_dest.txt"
	printf '%s\n%s\n' "tank/src@snap1" "tank/src/child@snap1" >"$source_tmp"
	: >"$dest_tmp"
	g_option_x_exclude_datasets='['

	set +e
	output=$(
		(
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		) 2>&1
	)
	status=$?

	assertEquals "Recursive delta planning should fail closed when exclude filtering uses an invalid pattern." \
		2 "$status"
	assertContains "Recursive delta planning should report the specific exclude-filter failure context." \
		"$output" "Failed to filter recursive source dataset transfer list against exclude patterns."
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/zxfer_get_zfs_list.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-zxfer_get_zfs_list.$idx"
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/zxfer_get_zfs_list_omnios.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-zxfer_get_zfs_list_omnios.$idx"
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_list_root_missing.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_list_root_missing.$idx"
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
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
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
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
			zxfer_read_snapshot_discovery_capture_file() {
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
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_destination_probe_reports_missing() {
				printf '%s\n' "called" >>"$PROBE_LOG"
				return 0
			}
			zxfer_read_snapshot_discovery_capture_file() {
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

test_get_zfs_list_reports_empty_destination_inventory_readbacks() {
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
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
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
			zxfer_read_snapshot_discovery_capture_file() {
				g_zxfer_snapshot_discovery_file_read_result=""
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Empty staged destination inventory readbacks should abort snapshot discovery." \
		1 "$status"
	assertContains "Empty staged destination inventory readbacks should report the specific empty-inventory context." \
		"$output" "Staged destination dataset inventory was empty."
}

test_get_zfs_list_reports_destination_snapshot_list_readback_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
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
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/existing"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst
backup/dst/existing"
					return 0
				fi
				if [ "$l_read_count" -eq 2 ]; then
					return 28
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Destination snapshot-list readback failures should preserve the readback status." \
		28 "$status"
	assertContains "Destination snapshot-list readback failures should report the staged destination snapshot context." \
		"$output" "Failed to read staged destination snapshot list."
}

test_get_zfs_list_reports_source_snapshot_list_readback_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
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
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/existing"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst
backup/dst/existing"
					return 0
				fi
				if [ "$l_read_count" -eq 2 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst@snapA"
					return 0
				fi
				if [ "$l_read_count" -eq 3 ]; then
					return 29
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Source snapshot-list readback failures should preserve the readback status." \
		29 "$status"
	assertContains "Source snapshot-list readback failures should report the staged source snapshot context." \
		"$output" "Failed to read staged source snapshot list."
}

test_get_zfs_list_preserves_source_snapshot_record_cache_tempfile_failures() {
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
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
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
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst"
				elif [ "$l_read_count" -eq 2 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst@snapA"
				elif [ "$l_read_count" -eq 3 ]; then
					g_zxfer_snapshot_discovery_file_read_result="tank/src@snapA"
				else
					return 1
				fi
				return 0
			}
			zxfer_get_temp_file() {
				l_temp_count=$((l_temp_count + 1))
				if [ "$l_temp_count" -le 6 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-source-cache-$l_temp_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 37
			}
			zxfer_get_zfs_list
		)
	)
	status=$?
	set -e

	assertEquals "Snapshot discovery should preserve the exact tempfile allocation failure status when the staged source snapshot-record cache tempfile cannot be allocated." \
		37 "$status"
	assertEquals "Snapshot discovery should not emit output for staged source snapshot-record cache tempfile failures." \
		"" "$output"
}

test_get_zfs_list_reports_source_snapshot_record_cache_stage_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			l_temp_count=0
			cleanup_log="$TEST_TMPDIR/get-zfs-source-cache-stage.cleanup"
			cache_cleanup_log="$TEST_TMPDIR/get-zfs-source-cache-stage.cache-cleanup"
			: >"$cleanup_log"
			: >"$cache_cleanup_log"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
				printf '%s\n' "tank/src@snapA" >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
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
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst"
				elif [ "$l_read_count" -eq 2 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst@snapA"
				elif [ "$l_read_count" -eq 3 ]; then
					g_zxfer_snapshot_discovery_file_read_result="tank/src@snapA"
				else
					return 1
				fi
				return 0
			}
			zxfer_get_temp_file() {
				l_temp_count=$((l_temp_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-source-cache-stage-$l_temp_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_cleanup_runtime_artifact_paths() {
				printf '%s\n' "$*" >>"$cleanup_log"
				return 0
			}
			zxfer_cleanup_snapshot_record_cache_files() {
				printf '%s\n' "cache-cleanup" >>"$cache_cleanup_log"
				return 0
			}
			zxfer_reverse_file_lines() {
				return 1
			}
			zxfer_throw_error() {
				printf 'cleanup=%s\n' "$(cat "$cleanup_log" 2>/dev/null || :)"
				printf 'cache_cleanup=%s\n' "$(cat "$cache_cleanup_log" 2>/dev/null || :)"
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
	assertContains "Source snapshot record-cache staging failures should clean up the staged source snapshot list file." \
		"$output" "get-zfs-source-cache-stage-1.tmp"
	assertContains "Source snapshot record-cache staging failures should clean up the staged source snapshot stderr file." \
		"$output" "get-zfs-source-cache-stage-2.tmp"
	assertContains "Source snapshot record-cache staging failures should clean up the staged destination snapshot diff file." \
		"$output" "get-zfs-source-cache-stage-6.tmp"
	assertContains "Source snapshot record-cache staging failures should clean up the staged source snapshot-record cache file." \
		"$output" "get-zfs-source-cache-stage-7.tmp"
	assertContains "Source snapshot record-cache staging failures should run the snapshot-record cache cleanup helper." \
		"$output" "cache_cleanup=cache-cleanup"
	assertContains "Source snapshot record-cache staging failures should report the staged source-cache context." \
		"$output" "msg=Failed to stage source snapshot record cache."
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

test_get_zfs_list_remote_target_batches_destination_discovery() {
	ssh_log="$TEST_TMPDIR/get_zfs_remote_batch_success.ssh"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG="$ssh_log"
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\t%s\n' "tank/src@snapA" "guidA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'host=%s side=%s\n' "$1" "$3" >>"$SSH_LOG"
				printf 'cmd=%s\n' "$2" >>"$SSH_LOG"
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf '%s\n' "backup/dst/src"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf '%s\t%s\n' "backup/dst/src@snapA" "guidA"
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unexpected-destination-zfs" >>"$SSH_LOG"
				return 99
			}
			zxfer_set_g_recursive_source_list() {
				printf 'normalized=%s\n' "$(cat "$2")"
				g_recursive_source_list=""
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'root_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'snapshot_dataset_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/src")"
			printf 'raw=%s\n' "$g_rzfs_list_hr_snap"
		)
	)

	assertEquals "Remote destination discovery should use one target SSH invocation." \
		"1" "$(grep -c '^host=target.example side=destination$' "$ssh_log")"
	assertContains "Remote destination discovery should render dataset inventory in the batch script." \
		"$(cat "$ssh_log")" "filesystem,volume"
	assertContains "Remote destination discovery should render snapshot listing in the batch script." \
		"$(cat "$ssh_log")" "name,guid"
	assertNotContains "Remote destination discovery should not fall back to separate destination zfs helper calls." \
		"$(cat "$ssh_log")" "unexpected-destination-zfs"
	assertContains "Remote destination discovery should publish the recursive destination inventory." \
		"$output" "dest=backup/dst
backup/dst/src"
	assertContains "Remote destination discovery should seed the destination root existence cache." \
		"$output" "root_cache=1"
	assertContains "Remote destination discovery should seed the destination snapshot dataset existence cache." \
		"$output" "snapshot_dataset_cache=1"
	assertContains "Remote destination discovery should preserve the raw destination snapshot cache." \
		"$output" "raw=backup/dst/src@snapA	guidA"
	assertContains "Remote destination discovery should normalize destination snapshot paths for source-side diffing." \
		"$output" "normalized=tank/src@snapA	guidA"
}

test_build_remote_destination_discovery_batch_script_streams_snapshot_stdout_directly() {
	fake_zfs="$TEST_TMPDIR/remote_batch_stream_zfs"
	zfs_log="$TEST_TMPDIR/remote_batch_stream_zfs.log"
	: >"$zfs_log"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$ZXFER_FAKE_ZFS_LOG"
case "$*" in
"list -t filesystem,volume -Hr -o name backup/dst")
	printf '%s\n' "backup/dst"
	printf '%s\n' "backup/dst/src"
	printf '%s\n' "backup/dst/other"
	;;
"list -Hr -o name,guid -t snapshot backup/dst/src")
	printf '%s\t%s\n' "backup/dst/src@snapA" "guidA"
	;;
*)
	printf 'unexpected zfs args: %s\n' "$*" >&2
	exit 99
	;;
esac
EOF
	chmod +x "$fake_zfs"
	g_target_cmd_zfs=$fake_zfs

	script=$(zxfer_build_remote_destination_discovery_batch_script "backup/dst" "backup/dst/src" "backup")

	assertNotContains "Remote batch should not buffer recursive destination inventory stdout in a shell variable." \
		"$script" "l_inventory_stdout=\$("
	assertNotContains "Remote batch should not buffer destination snapshot stdout in a shell variable." \
		"$script" "l_snapshot_stdout=\$("
	assertContains "Remote batch should stage destination inventory stdout in a target-side temp file." \
		"$script" 'zxfer.destination-discovery.inventory.XXXXXX'
	assertNotContains "Remote batch should not stage destination snapshot stdout in a target-side temp file." \
		"$script" 'zxfer.destination-discovery.snapshots.XXXXXX'
	assertContains "Remote batch should still stage compact destination snapshot stderr diagnostics." \
		"$script" 'zxfer.destination-discovery.snapshots-stderr.XXXXXX'
	assertContains "Remote batch should stream staged section bodies instead of expanding payload variables." \
		"$script" "cat \"\$l_section_file\""
	assertContains "Remote batch should stream snapshot stdout directly from zfs." \
		"$script" "\"\$l_zfs_cmd\" list -Hr -o name,guid -t snapshot \"\$l_destination_snapshot_dataset\" 2>\"\$l_snapshot_stderr_file\""
	assertContains "Remote batch should clean target-side temp files on shell exit." \
		"$script" "trap 'zxfer_cleanup_destination_discovery_batch' 0"
	assertContains "Remote batch should use an exact fixed-string scan for the destination snapshot dataset." \
		"$script" "grep -F -x -e \"\$l_destination_snapshot_dataset\" \"\$l_inventory_stdout_file\""

	set +e
	output=$(ZXFER_FAKE_ZFS_LOG="$zfs_log" TMPDIR="$TEST_TMPDIR" sh -c "$script" 2>&1)
	status=$?
	set -e

	assertEquals "Generated remote batch script should execute successfully with target-side temp files." \
		0 "$status"
	assertContains "Generated remote batch should emit the destination inventory section." \
		"$output" "$(printf 'BEGIN\tinventory_stdout')"
	assertContains "Generated remote batch should stream destination inventory rows." \
		"$output" "backup/dst/src"
	assertContains "Generated remote batch should stream destination snapshot rows." \
		"$output" "backup/dst/src@snapA	guidA"
	assertContains "Generated remote batch should report that snapshot listing ran." \
		"$output" "$(printf 'STATUS\tsnapshot_ran\t1')"
	assertContains "Generated remote batch should report snapshot status after streaming stdout." \
		"$output" "$(printf 'STATUS\tsnapshot\t0')"
	assertEquals "Generated remote batch should run inventory and snapshot zfs lists without a pool fallback." \
		"2" "$(wc -l <"$zfs_log" | tr -d '[:space:]')"
	assertEquals "Generated remote batch should remove its target-side temp files." \
		"" "$(find "$TEST_TMPDIR" -name 'zxfer.destination-discovery.*' -print)"
}

test_parse_remote_destination_discovery_batch_output_file_splits_sections_in_bulk() {
	batch_file="$TEST_TMPDIR/remote_batch_parse.out"
	dest_file="$TEST_TMPDIR/remote_batch_parse.dest"
	dest_err_file="$TEST_TMPDIR/remote_batch_parse.dest.err"
	snap_file="$TEST_TMPDIR/remote_batch_parse.snap"
	snap_err_file="$TEST_TMPDIR/remote_batch_parse.snap.err"
	{
		printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
		printf 'STATUS\tinventory\t0\n'
		printf 'STATUS\tpool\t\n'
		printf 'STATUS\tsnapshot_ran\t1\n'
		printf 'BEGIN\tinventory_stdout\n'
		printf '%s\n' 'backup/dst'
		printf '%s\n' 'backup/dst/src'
		printf 'END\tinventory_stdout\n'
		printf 'BEGIN\tinventory_stderr\n'
		printf 'END\tinventory_stderr\n'
		printf 'BEGIN\tpool_stderr\n'
		printf 'END\tpool_stderr\n'
		printf 'BEGIN\tsnapshot_stdout\n'
		printf 'backup/dst/src@snapA\tguidA\n'
		printf 'backup/dst/src@snapB\tguidB\n'
		printf 'END\tsnapshot_stdout\n'
		printf 'STATUS\tsnapshot\t0\n'
		printf 'BEGIN\tsnapshot_stderr\n'
		printf 'END\tsnapshot_stderr\n'
		printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
	} >"$batch_file"
	: >"$dest_file"
	: >"$dest_err_file"
	: >"$snap_file"
	: >"$snap_err_file"
	parse_output=$(
		(
			zxfer_read_snapshot_discovery_capture_file() {
				return 99
			}
			zxfer_parse_remote_destination_discovery_batch_output_file \
				"$batch_file" \
				"$dest_file" \
				"$dest_err_file" \
				"$snap_file" \
				"$snap_err_file"
			printf 'inventory_status=%s\n' "$g_zxfer_destination_discovery_batch_inventory_status"
			printf 'snapshot_status=%s\n' "$g_zxfer_destination_discovery_batch_snapshot_status"
			printf 'snapshot_ran=%s\n' "$g_zxfer_destination_discovery_batch_snapshot_ran"
		)
	)

	assertEquals "Batch parser should split inventory stdout into the destination inventory stage." \
		"backup/dst
backup/dst/src" "$(cat "$dest_file")"
	assertEquals "Batch parser should split snapshot stdout into the destination snapshot stage." \
		"backup/dst/src@snapA	guidA
backup/dst/src@snapB	guidB" "$(cat "$snap_file")"
	assertEquals "Batch parser should leave empty inventory stderr empty." \
		"" "$(cat "$dest_err_file")"
	assertEquals "Batch parser should leave empty snapshot stderr empty." \
		"" "$(cat "$snap_err_file")"
	assertEquals "Batch parser should load inventory status from the compact sidecar." \
		"inventory_status=0" "$(printf '%s\n' "$parse_output" | sed -n '/^inventory_status=/p')"
	assertEquals "Batch parser should load delayed snapshot status from the compact sidecar." \
		"snapshot_status=0" "$(printf '%s\n' "$parse_output" | sed -n '/^snapshot_status=/p')"
	assertEquals "Batch parser should load snapshot_ran status from the compact sidecar." \
		"snapshot_ran=1" "$(printf '%s\n' "$parse_output" | sed -n '/^snapshot_ran=/p')"
}

test_destination_discovery_batch_status_loader_rejects_malformed_sidecars() {
	status_file="$TEST_TMPDIR/remote_batch_status_bad.out"

	printf '%s\n' "not-tab-separated" >"$status_file"
	set +e
	zxfer_load_destination_discovery_batch_status_file "$status_file" >/dev/null 2>&1
	status=$?
	set -e
	assertEquals "Status sidecars without tab-separated fields should fail closed." \
		1 "$status"

	{
		printf 'inventory\t0\n'
		printf 'pool\t\n'
		printf 'snapshot\t0\n'
		printf 'unexpected\t0\n'
	} >"$status_file"
	set +e
	zxfer_load_destination_discovery_batch_status_file "$status_file" >/dev/null 2>&1
	status=$?
	set -e
	assertEquals "Status sidecars with unknown status names should fail closed." \
		1 "$status"
}

test_parse_remote_destination_discovery_batch_output_file_preserves_status_tempfile_failures() {
	batch_file="$TEST_TMPDIR/remote_batch_parse_temp_failure.out"
	dest_file="$TEST_TMPDIR/remote_batch_parse_temp_failure.dest"
	dest_err_file="$TEST_TMPDIR/remote_batch_parse_temp_failure.dest.err"
	snap_file="$TEST_TMPDIR/remote_batch_parse_temp_failure.snap"
	snap_err_file="$TEST_TMPDIR/remote_batch_parse_temp_failure.snap.err"
	: >"$batch_file"
	: >"$dest_file"
	: >"$dest_err_file"
	: >"$snap_file"
	: >"$snap_err_file"

	status=$(
		(
			set +e
			zxfer_get_temp_file() {
				return 42
			}
			zxfer_parse_remote_destination_discovery_batch_output_file \
				"$batch_file" \
				"$dest_file" \
				"$dest_err_file" \
				"$snap_file" \
				"$snap_err_file"
			printf '%s\n' "$?"
		)
	)

	assertEquals "Batch parser should preserve compact status tempfile allocation failures." \
		"42" "$status"
}

test_get_zfs_list_remote_target_batches_missing_destination_root_fallback() {
	ssh_log="$TEST_TMPDIR/get_zfs_remote_batch_missing.ssh"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG="$ssh_log"
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'host=%s side=%s\n' "$1" "$3" >>"$SSH_LOG"
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t1\n'
				printf 'STATUS\tpool\t0\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t0\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf '%s\n' "cannot open 'backup/dst': no such pool or dataset"
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unexpected-pool-probe" >>"$SSH_LOG"
				return 99
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_get_zfs_list
			printf 'dest=<%s>\n' "$g_recursive_dest_list"
			printf 'root_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'child_cache=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/src")"
			printf 'raw=<%s>\n' "$g_rzfs_list_hr_snap"
		)
	)

	assertEquals "Missing-root remote discovery should still use one target SSH invocation." \
		"1" "$(grep -c '^host=target.example side=destination$' "$ssh_log")"
	assertNotContains "Remote missing-root fallback should use the batch pool status instead of a second local destination helper probe." \
		"$(cat "$ssh_log")" "unexpected-pool-probe"
	assertContains "Remote missing-root fallback should treat the recursive destination inventory as empty." \
		"$output" "dest=<>"
	assertContains "Remote missing-root fallback should mark the destination root missing." \
		"$output" "root_cache=0"
	assertContains "Remote missing-root fallback should infer descendants under the missing root as absent." \
		"$output" "child_cache=0"
	assertContains "Remote missing-root fallback should stage an empty destination snapshot list." \
		"$output" "raw=<>"
}

test_get_zfs_list_remote_target_batches_inventory_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t13\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t0\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf '%s\n' "permission denied"
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote destination inventory failures should preserve the target-side status." \
		13 "$status"
	assertContains "Remote destination inventory failures should preserve the existing inventory failure message." \
		"$output" "Failed to retrieve list of datasets from the destination"
}

test_get_zfs_list_remote_target_batches_snapshot_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t17\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf '%s\n' "backup/dst/src"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf 'END\tsnapshot_stdout\n'
				printf 'BEGIN\tsnapshot_stderr\n'
				printf '%s\n' "snapshot list failed"
				printf 'END\tsnapshot_stderr\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote destination snapshot failures should preserve the target-side status." \
		17 "$status"
	assertContains "Remote destination snapshot failures should preserve the existing snapshot-list failure message." \
		"$output" "Failed to retrieve snapshot list from the destination."
	assertContains "Remote destination snapshot failures should preserve target-side stderr diagnostics." \
		"$output" "snapshot list failed"
}

test_get_zfs_list_remote_target_batches_malformed_payloads_fail_closed() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Malformed remote destination discovery batches should fail closed." \
		1 "$status"
	assertContains "Malformed remote destination discovery batches should report the malformed batch context." \
		"$output" "Malformed destination discovery batch response."

	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
				printf 'STATUS\tinventory\t0\n'
				printf 'STATUS\tpool\t\n'
				printf 'STATUS\tsnapshot\t0\n'
				printf 'STATUS\tsnapshot_ran\t1\n'
				printf 'BEGIN\tinventory_stdout\n'
				printf '%s\n' "backup/dst"
				printf '%s\n' "backup/dst/src"
				printf 'END\tinventory_stdout\n'
				printf 'BEGIN\tinventory_stderr\n'
				printf 'END\tinventory_stderr\n'
				printf 'BEGIN\tpool_stderr\n'
				printf 'END\tpool_stderr\n'
				printf 'BEGIN\tsnapshot_stdout\n'
				printf '%s\n' "backup/dst/src@snapA	101"
				printf 'END\tsnapshot_stdout\n'
				printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote destination discovery batches with missing sections should fail closed." \
		1 "$status"
	assertContains "Missing remote batch sections should report the malformed batch context." \
		"$output" "Malformed destination discovery batch response."
}

test_run_remote_destination_discovery_batch_preserves_setup_and_transport_failures() {
	output=$(
		set +e
		dest_file="$TEST_TMPDIR/remote_batch_failure.dest"
		err_file="$TEST_TMPDIR/remote_batch_failure.err"
		snap_file="$TEST_TMPDIR/remote_batch_failure.snap"
		snap_err_file="$TEST_TMPDIR/remote_batch_failure.snap.err"
		: >"$dest_file"
		: >"$err_file"
		: >"$snap_file"
		: >"$snap_err_file"

		(
			g_destination="backup"
			zxfer_get_temp_file() {
				return 31
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'temp=%s\n' "$?"

		(
			g_destination="backup/dst"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_build.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				return 32
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'build_cleanup=%s\n' "$1"
				rm -f "$1"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'build=%s\n' "$?"

		(
			g_destination="backup/dst"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_command.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				return 33
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'command_cleanup=%s\n' "$1"
				rm -f "$1"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'command=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_ssh.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 34
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'ssh_cleanup=%s\n' "$1"
				rm -f "$1"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'ssh=%s\n' "$?"

		(
			g_destination="backup/dst"
			g_option_T_target_host="target.example"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/remote_batch_parse.out"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_remote_destination_discovery_batch_script() {
				printf '%s\n' "batch-script"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "remote-cmd"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "ZXFER_DESTINATION_DISCOVERY_BATCH_V1"
			}
			zxfer_throw_error() {
				printf 'parse_error=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_cleanup_runtime_artifact_path() {
				printf 'parse_cleanup=%s\n' "$1"
				rm -f "$1"
			}
			zxfer_run_remote_destination_discovery_batch_to_files "backup/dst/src" "$dest_file" "$err_file" "$snap_file" "$snap_err_file"
		)
		printf 'parse=%s\n' "$?"
	)

	assertContains "Remote batch temp allocation failures should preserve status." \
		"$output" "temp=31"
	assertContains "Remote batch script render failures should preserve status." \
		"$output" "build=32"
	assertContains "Remote batch command render failures should preserve status." \
		"$output" "command=33"
	assertContains "Remote batch SSH failures should preserve transport status." \
		"$output" "ssh=34"
	assertContains "Remote batch parse failures should report malformed batch context." \
		"$output" "parse_error=Malformed destination discovery batch response."
	assertContains "Remote batch parse failures should preserve parser status." \
		"$output" "parse=1"
	assertContains "Remote batch script render failures should clean their batch output file." \
		"$output" "build_cleanup=$TEST_TMPDIR/remote_batch_build.out"
	assertContains "Remote batch command render failures should clean their batch output file." \
		"$output" "command_cleanup=$TEST_TMPDIR/remote_batch_command.out"
	assertContains "Remote batch SSH failures should clean their batch output file." \
		"$output" "ssh_cleanup=$TEST_TMPDIR/remote_batch_ssh.out"
	assertContains "Remote batch parse failures should clean their batch output file." \
		"$output" "parse_cleanup=$TEST_TMPDIR/remote_batch_parse.out"
}

test_get_zfs_list_local_destination_discovery_does_not_use_remote_batch() {
	ssh_log="$TEST_TMPDIR/get_zfs_local_batch_guard.ssh"
	zfs_log="$TEST_TMPDIR/get_zfs_local_batch_guard.zfs"
	: >"$ssh_log"
	: >"$zfs_log"

	output=$(
		(
			SSH_LOG="$ssh_log"
			ZFS_LOG="$zfs_log"
			g_option_T_target_host=""
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "unexpected-ssh" >>"$SSH_LOG"
				return 99
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$ZFS_LOG"
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				if [ "$1" = "list" ] && [ "$2" = "-Hr" ]; then
					printf '%s\n' "backup/dst/src@snapA"
					return 0
				fi
				return 99
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'raw=%s\n' "$g_rzfs_list_hr_snap"
		)
	)

	assertEquals "Local destination discovery should not invoke the remote batch path." \
		"" "$(cat "$ssh_log")"
	assertContains "Local destination discovery should keep using the direct recursive dataset inventory command." \
		"$(cat "$zfs_log")" "list -t filesystem,volume -Hr -o name backup/dst"
	assertContains "Local destination discovery should keep using the direct destination snapshot command." \
		"$(cat "$zfs_log")" "list -Hr -o name,guid -t snapshot backup/dst/src"
	assertContains "Local destination discovery should still publish the recursive destination inventory." \
		"$output" "dest=backup/dst
backup/dst/src"
	assertContains "Local destination discovery should still publish the raw destination snapshot cache." \
		"$output" "raw=backup/dst/src@snapA"
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_profile.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_profile.$idx"
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_empty.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_empty.$idx"
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_fail.$idx"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_fail.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				printf '%s\n' "missing command" >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'printf \"%s\\n\" \"missing command\" >&2; exit 37'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=37
				g_zxfer_background_job_wait_report_failure=""
				return 0
			}
			zxfer_exists_destination() {
				printf '%s\n' 0
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
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_fail_blank.$idx"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=1
				g_zxfer_background_job_wait_report_failure=""
				return 0
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

test_get_zfs_list_reports_supervisor_completion_failures_before_source_stderr_handling() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_completion_write.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_completion_write.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 125'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=125
				g_zxfer_background_job_wait_report_failure="completion_write"
				return 0
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
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Supervisor completion-write failures during source snapshot discovery should keep the generic failure exit status." \
		1 "$status"
	assertContains "Source snapshot discovery should report supervisor completion-write failures directly instead of misattributing them to source stderr." \
		"$output" "msg=Failed to report source snapshot discovery completion."
}

test_get_zfs_list_reports_supervisor_queue_publish_failures_before_source_stderr_handling() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_queue_write.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_queue_write.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				: >"$1"
				: >"$2"
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 125'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=125
				g_zxfer_background_job_wait_report_failure="queue_write"
				return 0
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
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Supervisor queue-publication failures during source snapshot discovery should keep the generic failure exit status." \
		1 "$status"
	assertContains "Source snapshot discovery should report supervisor queue-publication failures directly instead of misattributing them to source stderr." \
		"$output" "msg=Failed to publish source snapshot discovery completion."
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
				g_source_snapshot_list_pid=4242
				g_source_snapshot_list_job_id="job-source"
				g_source_snapshot_list_cmd="sh -c 'exit 1'"
			}
			zxfer_wait_for_background_job() {
				g_zxfer_background_job_wait_exit_status=1
				g_zxfer_background_job_wait_report_failure=""
				return 0
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
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst"
					return 0
				fi
				if [ "$l_read_count" -eq 2 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst@snapA"
					return 0
				fi
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
	sorted_file="$TEST_TMPDIR/recursive_dataset_lines_read_failure.sorted"
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

test_capture_recursive_dataset_list_from_snapshot_records_extracts_sorted_unique_datasets() {
	zxfer_capture_recursive_dataset_list_from_snapshot_records "$(
		cat <<'EOF'
tank/src/child@snap2
tank/src@snap1
tank/src/child@snap3
EOF
	)"

	# shellcheck disable=SC2031  # Current-shell scratch is asserted directly in tests.
	assertEquals "Recursive dataset-list capture from snapshot records should extract, sort, and deduplicate dataset names." \
		"tank/src
tank/src/child" "$g_zxfer_recursive_dataset_list_result"
}

test_capture_recursive_dataset_list_from_snapshot_records_reports_second_tempfile_failures() {
	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/recursive_snapshot_records.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records "tank/src@snap1"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from snapshot records should fail closed when the dataset-lines tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Recursive dataset-list capture from snapshot records should not emit output for second-tempfile failures." \
		"" "$output"
}

test_capture_recursive_dataset_list_from_snapshot_records_reports_line_capture_failures() {
	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_lines_file() {
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records "tank/src@snap1"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from snapshot records should fail closed when recursive line capture fails." \
		1 "$status"
	assertEquals "Recursive dataset-list capture from snapshot records should not emit output for recursive line-capture failures." \
		"" "$output"
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

test_write_recursive_dataset_list_result_to_file_writes_empty_results() {
	output_file="$TEST_TMPDIR/recursive_dataset_list_empty.txt"
	g_zxfer_recursive_dataset_list_result=""

	zxfer_write_recursive_dataset_list_result_to_file "$output_file"
	status=$?

	assertEquals "Recursive dataset-list result writes should succeed for an empty list." \
		0 "$status"
	assertEquals "Recursive dataset-list result writes should create an empty file for an empty list." \
		"" "$(cat "$output_file")"
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
					g_zxfer_temp_file_result="$TEST_TMPDIR/recursive_filter_input.tmp"
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
	input_file="$TEST_TMPDIR/recursive_filter_read_failure_input.tmp"
	filtered_file="$TEST_TMPDIR/recursive_filter_read_failure_filtered.tmp"
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

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_common_snapshot_comm_failures() {
	source_tmp="$TEST_TMPDIR/refine_common_comm_error_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_common_comm_error_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			comm() {
				return 9
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when deriving the common snapshot intersection fails." \
		1 "$status"
	assertContains "Common snapshot intersection failures should report the recursive identity-validation context." \
		"$output" "Failed to derive recursive common snapshot list for snapshot identity validation."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_already_changed_dataset_derivation_failures() {
	source_tmp="$TEST_TMPDIR/refine_already_changed_derivation_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_already_changed_derivation_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_recursive_source_list="tank/already"

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when deriving the already-changed dataset list fails." \
		1 "$status"
	assertContains "Already-changed dataset derivation failures should report the recursive identity-validation context." \
		"$output" "Failed to derive already-changed recursive dataset list for snapshot identity validation."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_already_changed_dataset_stage_failures() {
	source_tmp="$TEST_TMPDIR/refine_already_changed_stage_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_already_changed_stage_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_recursive_source_list="tank/already"

	set +e
	output=$(
		(
			write_call_count=0
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records() {
				g_zxfer_recursive_dataset_list_result="tank/already"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				write_call_count=$((write_call_count + 1))
				if [ "$write_call_count" -eq 2 ]; then
					return 1
				fi
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when staging the already-changed dataset list fails." \
		1 "$status"
	assertContains "Already-changed dataset staging failures should report the recursive identity-validation context." \
		"$output" "Failed to stage already-changed recursive dataset list for snapshot identity validation."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_empty_already_changed_stage_failures() {
	source_tmp="$TEST_TMPDIR/refine_empty_already_changed_stage_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_empty_already_changed_stage_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_recursive_source_list=""
	g_recursive_destination_extra_dataset_list=""

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_write_runtime_artifact_file() {
				return 42
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				printf 'status=%s\n' "$2"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when staging an empty already-changed dataset list fails." \
		1 "$status"
	assertContains "Empty already-changed dataset staging failures should report the recursive identity-validation context." \
		"$output" "Failed to stage empty already-changed recursive dataset list for snapshot identity validation."
	assertContains "Empty already-changed dataset staging failures should preserve the runtime artifact write status." \
		"$output" "status=42"
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_candidate_dataset_load_failures_when_filtering() {
	source_tmp="$TEST_TMPDIR/refine_candidate_load_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_candidate_load_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_option_x_exclude_datasets='^tank/src/exclude$'

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_capture_recursive_dataset_list_from_lines_file() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when loading candidate datasets for exclude filtering fails." \
		1 "$status"
	assertContains "Candidate dataset load failures should report the recursive identity-validation filtering context." \
		"$output" "Failed to load recursive candidate dataset list for snapshot identity validation filtering."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_filtered_candidate_dataset_stage_failures() {
	source_tmp="$TEST_TMPDIR/refine_candidate_stage_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_candidate_stage_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"
	g_option_x_exclude_datasets='^tank/src/exclude$'

	set +e
	output=$(
		(
			write_call_count=0
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_capture_recursive_dataset_list_from_lines_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_filter_recursive_dataset_list_with_excludes() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				write_call_count=$((write_call_count + 1))
				if [ "$write_call_count" -eq 2 ]; then
					return 1
				fi
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when staging the filtered candidate dataset list fails." \
		1 "$status"
	assertContains "Filtered candidate dataset staging failures should report the recursive identity-validation context." \
		"$output" "Failed to stage filtered recursive candidate dataset list for snapshot identity validation."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_source_identity_stage_tempfile_failures() {
	source_tmp="$TEST_TMPDIR/refine_source_identity_temp_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_source_identity_temp_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 4 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/refine_source_identity_stage_$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same	999"
				else
					return 1
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when source identity staging cannot allocate a tempfile." \
		1 "$status"
	assertContains "Source identity staging tempfile failures should report the dataset being validated." \
		"$output" "Failed to allocate source snapshot identity staging for [tank/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_destination_identity_stage_tempfile_failures() {
	source_tmp="$TEST_TMPDIR/refine_destination_identity_temp_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_destination_identity_temp_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 5 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/refine_destination_identity_stage_$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same	999"
				else
					return 1
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when destination identity staging cannot allocate a tempfile." \
		1 "$status"
	assertContains "Destination identity staging tempfile failures should report the mapped destination dataset being validated." \
		"$output" "Failed to allocate destination snapshot identity staging for [backup/dst/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_source_identity_write_failures() {
	source_tmp="$TEST_TMPDIR/refine_source_identity_write_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_source_identity_write_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same	999"
				else
					return 1
				fi
			}
			zxfer_write_snapshot_identity_file_from_records() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when source identity staging writes fail." \
		1 "$status"
	assertContains "Source identity staging write failures should report the dataset being validated." \
		"$output" "Failed to write source snapshot identities for [tank/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_destination_identity_write_failures() {
	source_tmp="$TEST_TMPDIR/refine_destination_identity_write_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_destination_identity_write_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			write_call_count=0
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same	999"
				else
					return 1
				fi
			}
			zxfer_write_snapshot_identity_file_from_records() {
				write_call_count=$((write_call_count + 1))
				if [ "$write_call_count" -eq 1 ]; then
					printf '%s\n' "same	111" >"$2"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when destination identity staging writes fail." \
		1 "$status"
	assertContains "Destination identity staging write failures should report the mapped destination dataset being validated." \
		"$output" "Failed to write destination snapshot identities for [backup/dst/src]."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_recursive_source_merge_failures() {
	source_tmp="$TEST_TMPDIR/refine_source_merge_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_source_merge_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same	999"
				else
					return 1
				fi
			}
			zxfer_diff_snapshot_lists() {
				if [ "$3" = "source_minus_destination" ]; then
					printf '%s\n' "same	111"
					return 0
				fi
				return 0
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when merging the source-side recursive dataset list fails." \
		1 "$status"
	assertContains "Source-side recursive dataset merge failures should report the identity-validation merge context." \
		"$output" "Failed to merge recursive source dataset list after snapshot identity validation."
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_recursive_destination_merge_failures() {
	source_tmp="$TEST_TMPDIR/refine_destination_merge_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_destination_merge_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				printf '%s\n' "$g_zxfer_recursive_dataset_list_result" >"$1"
			}
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@same	111"
				elif [ "$1:$2" = "destination:backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src@same	999"
				else
					return 1
				fi
			}
			zxfer_diff_snapshot_lists() {
				if [ "$3" = "destination_minus_source" ]; then
					printf '%s\n' "same	999"
					return 0
				fi
				return 0
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when merging the destination-side recursive dataset list fails." \
		1 "$status"
	assertContains "Destination-side recursive dataset merge failures should report the identity-validation merge context." \
		"$output" "Failed to merge recursive destination dataset list after snapshot identity validation."
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

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_first_tempfile_failures() {
	source_tmp="$TEST_TMPDIR/refine_first_temp_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_first_temp_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 21
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve the exact tempfile allocation failure status when the first staging tempfile cannot be allocated." \
		21 "$status"
	assertEquals "Lazy identity refinement should not emit output for first-tempfile failures." \
		"" "$output"
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_second_tempfile_failures() {
	source_tmp="$TEST_TMPDIR/refine_second_temp_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_second_temp_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/refine-second-temp-1.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 22
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve the exact tempfile allocation failure status when the second staging tempfile cannot be allocated." \
		22 "$status"
	assertEquals "Lazy identity refinement should not emit output for second-tempfile failures." \
		"" "$output"
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_third_tempfile_failures() {
	source_tmp="$TEST_TMPDIR/refine_third_temp_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_third_temp_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 2 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/refine-third-temp-$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 23
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve the exact tempfile allocation failure status when the third staging tempfile cannot be allocated." \
		23 "$status"
	assertEquals "Lazy identity refinement should not emit output for third-tempfile failures." \
		"" "$output"
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_fourth_tempfile_failures() {
	source_tmp="$TEST_TMPDIR/refine_fourth_temp_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_fourth_temp_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				if [ "$call_count" -le 3 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/refine-fourth-temp-$call_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 24
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should preserve the exact tempfile allocation failure status when the fourth staging tempfile cannot be allocated." \
		24 "$status"
	assertEquals "Lazy identity refinement should not emit output for fourth-tempfile failures." \
		"" "$output"
}

test_zxfer_refine_recursive_snapshot_deltas_with_identity_validation_reports_common_dataset_stage_failures() {
	source_tmp="$TEST_TMPDIR/refine_common_stage_source.txt"
	dest_tmp="$TEST_TMPDIR/refine_common_stage_dest.txt"
	printf '%s\n' "tank/src@same" >"$source_tmp"
	printf '%s\n' "tank/src@same" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_capture_recursive_dataset_list_from_snapshot_file() {
				g_zxfer_recursive_dataset_list_result="tank/src"
				return 0
			}
			zxfer_write_recursive_dataset_list_result_to_file() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Lazy identity refinement should fail closed when staging the common dataset list fails." \
		1 "$status"
	assertContains "Common dataset staging failures should report the recursive identity-validation context." \
		"$output" "Failed to stage recursive common dataset list for snapshot identity validation."
}

test_capture_recursive_dataset_list_from_snapshot_records_reports_initial_tempfile_failures() {
	set +e
	output=$(
		(
			zxfer_get_temp_file() {
				return 1
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records "tank/src@snap1"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from snapshot records should fail closed when the first tempfile cannot be allocated." \
		1 "$status"
	assertEquals "Recursive dataset-list capture from snapshot records should not emit output for first-tempfile failures." \
		"" "$output"
}

test_capture_recursive_dataset_list_from_snapshot_records_reports_snapshot_record_write_failures() {
	set +e
	output=$(
		(
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/records-$call_count.tmp"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 73
			}
			zxfer_capture_recursive_dataset_list_from_snapshot_records "tank/src@snap1"
		)
	)
	status=$?

	assertEquals "Recursive dataset-list capture from snapshot records should fail closed when the snapshot-record staging file cannot be written." \
		73 "$status"
	assertEquals "Recursive dataset-list capture from snapshot records should not publish shell noise when the snapshot-record staging file cannot be written." \
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
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_diff_snapshot_lists() {
				if [ "$3" = "destination_minus_source" ]; then
					return 7
				fi
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

	assertEquals "Recursive delta planning should fail closed when the destination-minus-source diff fails." \
		1 "$status"
	assertContains "Recursive delta planning should report the recursive delete diff failure context." \
		"$output" "Failed to diff destination and source snapshots for recursive delete planning."
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

test_set_g_recursive_source_list_does_not_call_whole_tree_identity_validation() {
	source_tmp="$TEST_TMPDIR/no_identity_validation_source.txt"
	dest_tmp="$TEST_TMPDIR/no_identity_validation_dest.txt"
	printf '%s\n' "tank/src@snap1" >"$source_tmp"
	printf '%s\n' "tank/src@snap1" >"$dest_tmp"

	set +e
	output=$(
		(
			zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
				printf '%s\n' "unexpected identity validation"
				return 7
			}
			zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
		)
	)
	status=$?

	assertEquals "Recursive delta planning should skip the whole-tree identity validation pass on name-identical inputs." \
		0 "$status"
	assertEquals "Recursive delta planning should not emit identity-validation output when no snapshot names differ." \
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
