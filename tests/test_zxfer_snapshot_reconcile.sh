#!/bin/sh
#
# shunit2 tests for zxfer_snapshot_reconcile.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_reconcile.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_inspect_delete"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_source_runtime_modules_through "zxfer_snapshot_reconcile.sh"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_g_grandfather_protection=""
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_RZFS="/sbin/zfs"
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_actual_dest=""
	g_last_common_snap=""
	g_src_snapshot_transfer_list=""
	g_dest_has_snapshots=0
	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0
	g_destination_snapshot_creation_cache=""
	g_zxfer_snapshot_record_capture_result=""
	g_delete_source_tmp_file=$(mktemp "$TEST_TMPDIR/delete_source.XXXXXX")
	g_delete_dest_tmp_file=$(mktemp "$TEST_TMPDIR/delete_dest.XXXXXX")
	g_delete_snapshots_to_delete_tmp_file=$(mktemp "$TEST_TMPDIR/delete_diff.XXXXXX")
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
}

tearDown() {
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
	zxfer_reset_snapshot_record_indexes
}

test_zxfer_snapshot_reconcile_state_helpers_cover_current_shell_paths() {
	identity_output_file="$TEST_TMPDIR/snapshot_reconcile_identities.out"
	path_output_file="$TEST_TMPDIR/snapshot_reconcile_paths.out"

	g_last_common_snap="backup/dst@snap1"
	g_dest_has_snapshots=1
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_src_snapshot_transfer_list="tank/src@snap2"
	g_destination_snapshot_creation_cache="backup/dst@snap1	123"
	zxfer_reset_snapshot_reconcile_state

	zxfer_write_snapshot_identities_to_file \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$identity_output_file"
	zxfer_write_destination_snapshot_paths_for_identity_file \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$identity_output_file" >"$path_output_file"

	assertEquals "Resetting snapshot-reconcile state should clear the last-common snapshot scratch." \
		"" "$g_last_common_snap"
	assertEquals "Resetting snapshot-reconcile state should clear the destination-has-snapshots marker." \
		0 "${g_dest_has_snapshots:-0}"
	assertEquals "Resetting snapshot-reconcile state should clear the destination-deletion marker." \
		0 "${g_did_delete_dest_snapshots:-0}"
	assertEquals "Resetting snapshot-reconcile state should clear the deleted-newer-snapshots marker." \
		0 "${g_deleted_dest_newer_snapshots:-0}"
	assertEquals "Resetting snapshot-reconcile state should clear the source snapshot transfer list." \
		"" "$g_src_snapshot_transfer_list"
	assertEquals "Resetting snapshot-reconcile state should clear the destination snapshot creation cache." \
		"" "$g_destination_snapshot_creation_cache"
	assertEquals "Snapshot identity staging should normalize, deduplicate, and sort snapshot identities." \
		"snap1	111
snap2	222" "$(cat "$identity_output_file")"
	assertEquals "Destination snapshot path mapping should restore the original destination snapshot paths for matching identities." \
		"backup/dst@snap1
backup/dst@snap2" "$(cat "$path_output_file")"
}

test_invalidate_destination_snapshot_record_cache_resets_creation_cache_when_loaded() {
	destination_cache_file="$TEST_TMPDIR/destination_snapshot_cache_with_creation.raw"
	printf '%s\n' "backup/dst@snap1	111" >"$destination_cache_file"
	g_zxfer_destination_snapshot_record_cache_file=$destination_cache_file
	g_rzfs_list_hr_snap="backup/dst@snap1	111"
	g_destination_snapshot_creation_cache="backup/dst@snap1	111"

	zxfer_build_snapshot_record_index destination "backup/dst@snap1	111" >/dev/null ||
		fail "Expected destination snapshot index fixture to build."
	zxfer_invalidate_destination_snapshot_record_cache

	assertEquals "Destination snapshot invalidation should clear destination snapshot creation cache when snapshot reconcile is loaded." \
		"" "${g_destination_snapshot_creation_cache:-}"
	assertEquals "Destination snapshot invalidation should clear destination snapshot index readiness." \
		0 "${g_zxfer_destination_snapshot_record_index_ready:-0}"
	assertEquals "Destination snapshot invalidation should clear the destination snapshot cache-file path." \
		"" "${g_zxfer_destination_snapshot_record_cache_file:-}"
	assertFalse "Destination snapshot invalidation should remove the stale snapshot cache file." \
		"[ -e \"$destination_cache_file\" ]"
}

test_delete_snaps_returns_when_nothing_needs_deletion() {
	log_file="$TEST_TMPDIR/delete_none.log"
	: >"$log_file"
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")

	(
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$log_file"
		}
		zxfer_delete_snaps "$source_list" "$dest_list"
	)

	assertEquals "No destroy command should run when destination snapshots already match the source." "" "$(cat "$log_file")"
}

test_delete_snaps_dry_run_prints_destroy_command() {
	g_option_n_dryrun=1
	g_option_v_verbose=1
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")

	output=$(zxfer_delete_snaps "$source_list" "$dest_list")

	assertContains "Dry-run snapshot deletion should print the rendered destroy command." \
		"$output" "Dry run: '/sbin/zfs' 'destroy' 'tank/fs@snap3'"
}

test_delete_snaps_invalidates_destination_snapshot_cache_after_live_destroy() {
	log_file="$TEST_TMPDIR/delete_invalidate_snapshot_cache.log"
	: >"$log_file"
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")

	zxfer_run_destination_zfs_cmd() {
		printf 'destroy=%s %s\n' "$1" "$2" >>"$log_file"
		return 0
	}
	zxfer_invalidate_destination_snapshot_record_cache() {
		printf 'invalidated=snapshots\n' >>"$log_file"
	}

	zxfer_delete_snaps "$source_list" "$dest_list"

	assertEquals "Successful destination snapshot destroys should invalidate destination snapshot caches after the live mutation." \
		"destroy=destroy tank/fs@snap3
invalidated=snapshots" "$(cat "$log_file")"
}

test_delete_snaps_throws_when_destroy_fails() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 37
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_invalidate_destination_snapshot_record_cache() {
				printf '%s\n' "invalidated"
			}
			zxfer_delete_snaps "$source_list" "$dest_list"
		)
	)
	status=$?

	assertEquals "Failed destination destroys should preserve the destroy status." 37 "$status"
	assertContains "Failed destination destroys should use the generic execution error." \
		"$output" "Error when executing command."
	assertNotContains "Failed destination destroys should not invalidate snapshot caches as if the mutation succeeded." \
		"$output" "invalidated"
}

test_grandfather_test_reports_detailed_context_for_old_snapshots() {
	g_option_g_grandfather_protection=1
	current_epoch=$(date +%s)
	old_epoch=$((current_epoch - 5 * 86400))

	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_run_destination_zfs_cmd() {
				[ "$5" = "-p" ] || return 99
				printf '%s\n' "$old_epoch"
			}
			zxfer_format_snapshot_creation_epoch_for_display() {
				printf '%s\n' "Sun Jan  1 00:00:00 UTC 2023"
			}
			zxfer_grandfather_test "tank/fs@ancient"
		)
	)
	status=$?

	assertEquals "Grandfather protection should fail old snapshot deletions with a usage error." 2 "$status"
	assertContains "Grandfather errors should include the offending snapshot name." \
		"$output" "Snapshot name: tank/fs@ancient"
	assertContains "Grandfather errors should include the computed age." \
		"$output" "Snapshot age : 5 days old"
	assertContains "Grandfather errors should include the rendered snapshot date without issuing a second live probe." \
		"$output" "Snapshot date: Sun Jan  1 00:00:00 UTC 2023."
	assertContains "Grandfather errors should explain how to recover." \
		"$output" "Either amend/remove option g, fix your system date, or manually"
}

test_grandfather_test_allows_recent_snapshots() {
	g_option_g_grandfather_protection=10
	current_epoch=$(date +%s)
	recent_epoch=$((current_epoch - 2 * 86400))
	outfile="$TEST_TMPDIR/grandfather_recent.out"

	zxfer_run_destination_zfs_cmd() {
		if [ "$5" = "-p" ]; then
			printf '%s\n' "$recent_epoch"
		else
			printf '%s\n' "Mon Jan  1 00:00:00 UTC 2024"
		fi
	}

	zxfer_grandfather_test "tank/fs@recent" >"$outfile"
	status=$?

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Recent snapshots should pass grandfather protection checks." 0 "$status"
	assertEquals "Passing grandfather checks should not emit output." "" "$(cat "$outfile")"
}

test_grandfather_test_reports_creation_probe_failures() {
	g_option_g_grandfather_protection=1

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$5" = "-p" ]; then
					printf '%s\n' "Host key verification failed." >&2
					return 1
				fi
				return 0
			}
			zxfer_grandfather_test "tank/fs@missing"
		) 2>&1
	)
	status=$?

	assertEquals "Grandfather protection should abort when creation-time lookup fails." 1 "$status"
	assertContains "Grandfather protection should preserve the underlying creation-time probe diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Creation-time lookup failures should be reported as a destination creation-time query failure." \
		"$output" "Failed to query creation time for destination snapshot tank/fs@missing. Review prior stderr for the transport or query error."
}

test_grandfather_test_falls_back_to_unix_epoch_when_local_date_rendering_fails() {
	g_option_g_grandfather_protection=1
	current_epoch=$(date +%s)
	old_epoch=$((current_epoch - 3 * 86400))

	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_run_destination_zfs_cmd() {
				[ "$5" = "-p" ] || return 99
				printf '%s\n' "$old_epoch"
			}
			zxfer_format_snapshot_creation_epoch_for_display() {
				return 1
			}
			zxfer_grandfather_test "tank/fs@epoch-only"
		)
	)
	status=$?

	assertEquals "Grandfather protection should still raise a usage error when local date rendering falls back to unix epoch text." 2 "$status"
	assertContains "Grandfather errors should fall back to the validated creation epoch when no human-readable formatter succeeds." \
		"$output" "Snapshot date: $old_epoch (unix epoch)."
}

test_format_snapshot_creation_epoch_for_display_falls_back_to_unix_epoch_when_date_conversion_is_unavailable() {
	output=$(
		(
			date() {
				return 1
			}
			zxfer_format_snapshot_creation_epoch_for_display 123
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should succeed with a unix-epoch fallback even when local date conversion fails." 0 "$status"
	assertEquals "Creation-epoch display formatting should fall back to explicit unix-epoch text when date conversion is unavailable." \
		"123 (unix epoch)" "$output"
}

test_set_src_snapshot_transfer_list_transfers_all_snapshots_when_no_common_snapshot_exists() {
	g_last_common_snap=""

	zxfer_set_src_snapshot_transfer_list "tank/fs@snap3 tank/fs@snap2 tank/fs@snap1" "tank/fs"

	expected=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	assertEquals "Without a common snapshot, every source snapshot should be scheduled for transfer." \
		"$expected" "$g_src_snapshot_transfer_list"
}

test_get_last_common_snapshot_matches_exact_names_without_prefix_collisions() {
	outfile="$TEST_TMPDIR/last_common_exact.out"

	zxfer_get_last_common_snapshot \
		"tank/fs@daily-10 tank/fs@daily-1" \
		"backup/fs@daily-1 backup/fs@daily-11" >"$outfile"

	assertEquals "Last-common detection should match exact snapshot names, not prefixes." \
		"tank/fs@daily-1" "$(cat "$outfile")"
}

test_get_last_common_snapshot_preserves_destination_normalization_failures() {
	output_file="$TEST_TMPDIR/last_common_normalize_failure.out"

	set +e
	(
		zxfer_read_normalized_snapshot_record_list() {
			return 53
		}
		zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Last-common snapshot lookup should preserve destination normalization failures." \
		53 "$status"
	assertEquals "Last-common snapshot lookup should not publish a fallback snapshot when normalization fails." \
		"" "$(cat "$output_file")"
}

test_get_last_common_snapshot_preserves_source_normalization_failures() {
	output_file="$TEST_TMPDIR/last_common_source_normalize_failure.out"

	set +e
	(
		g_test_normalize_call_count=0
		zxfer_read_normalized_snapshot_record_list() {
			g_test_normalize_call_count=$((g_test_normalize_call_count + 1))
			if [ "$g_test_normalize_call_count" -le 2 ]; then
				g_zxfer_runtime_artifact_read_result="backup/dst@snap1"
				return 0
			fi
			return 47
		}
		zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Last-common snapshot lookup should preserve source normalization failures." \
		47 "$status"
	assertEquals "Last-common snapshot lookup should not publish a fallback snapshot when source normalization fails." \
		"" "$(cat "$output_file")"
}

test_get_last_common_snapshot_preserves_destination_identity_stage_failures() {
	output_file="$TEST_TMPDIR/last_common_identity_stage_failure.out"

	set +e
	(
		g_test_normalize_call_count=0
		zxfer_read_normalized_snapshot_record_list() {
			g_test_normalize_call_count=$((g_test_normalize_call_count + 1))
			if [ "$g_test_normalize_call_count" -eq 1 ]; then
				g_zxfer_runtime_artifact_read_result="backup/dst@snap1"
				return 0
			fi
			return 48
		}
		zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Last-common snapshot lookup should preserve destination identity staging failures." \
		48 "$status"
	assertEquals "Last-common snapshot lookup should not publish a fallback snapshot when identity staging fails." \
		"" "$(cat "$output_file")"
}

test_zxfer_write_destination_snapshot_paths_for_identity_file_preserves_normalized_read_failures() {
	set +e
	(
		zxfer_read_normalized_snapshot_record_list() {
			return 33
		}
		zxfer_write_destination_snapshot_paths_for_identity_file \
			"backup/dst@snap1	111" "$TEST_TMPDIR/identity-file" >/dev/null
	)
	status=$?
	set -e

	assertEquals "Destination snapshot-path restoration should preserve normalized-record read failures exactly." \
		33 "$status"
}

test_zxfer_capture_snapshot_records_for_dataset_and_last_common_snapshot_preserve_stage_failures() {
	stage_create_output=$(
		(
			set +e
			zxfer_create_runtime_artifact_file() {
				return 31
			}
			zxfer_capture_snapshot_records_for_dataset source "tank/src"
			printf 'status=%s\n' "$?"
			printf 'result=%s\n' "${g_zxfer_snapshot_record_capture_result:-}"
		)
	)
	stage_read_output=$(
		(
			set +e
			zxfer_create_runtime_artifact_file() {
				g_zxfer_runtime_artifact_path_result="$TEST_TMPDIR/snapshot-record-stage"
				: >"$g_zxfer_runtime_artifact_path_result"
				return 0
			}
			zxfer_get_snapshot_records_for_dataset() {
				printf '%s\n' "tank/src@snap1"
			}
			zxfer_read_runtime_artifact_file() {
				return 32
			}
			zxfer_capture_snapshot_records_for_dataset source "tank/src"
			printf 'status=%s\n' "$?"
			printf 'result=%s\n' "${g_zxfer_snapshot_record_capture_result:-}"
		)
	)
	initial_tempfile_output=$(
		(
			set +e
			zxfer_read_normalized_snapshot_record_list() {
				g_zxfer_runtime_artifact_read_result="backup/dst@snap1"
				return 0
			}
			zxfer_get_temp_file() {
				return 44
			}
			zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Snapshot-record capture should preserve staging allocation failures." \
		"$stage_create_output" "status=31"
	assertContains "Snapshot-record capture should leave the scratch result empty when staging fails." \
		"$stage_create_output" "result="
	assertContains "Snapshot-record capture should preserve staged readback failures." \
		"$stage_read_output" "status=32"
	assertContains "Snapshot-record capture should leave the scratch result empty when readback fails." \
		"$stage_read_output" "result="
	assertContains "Last-common snapshot lookup should preserve first temp-file allocation failures." \
		"$initial_tempfile_output" "status=44"
}

test_zxfer_write_destination_snapshot_paths_for_identity_file_preserves_awk_failures() {
	failing_awk="$TEST_TMPDIR/failing_snapshot_path_restore_awk.sh"
	cat >"$failing_awk" <<'EOF'
#!/bin/sh
exit 46
EOF
	chmod +x "$failing_awk"

	set +e
	(
		g_cmd_awk=$failing_awk
		zxfer_read_normalized_snapshot_record_list() {
			g_zxfer_runtime_artifact_read_result="backup/dst@snap1	111"
			return 0
		}
		zxfer_write_destination_snapshot_paths_for_identity_file \
			"backup/dst@snap1	111" "$TEST_TMPDIR/identity-file" >/dev/null
	)
	status=$?
	set -e

	assertEquals "Destination snapshot-path restoration should preserve awk failures exactly." \
		46 "$status"
}

test_get_last_common_snapshot_preserves_tempfile_and_stage_write_failures() {
	output_file="$TEST_TMPDIR/last_common_tempfile_failure.out"

	set +e
	tempfile_output=$(
		(
			temp_call_count=0
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				if [ "$temp_call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/last-common-first.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 44
			}
			zxfer_read_normalized_snapshot_record_list() {
				g_zxfer_runtime_artifact_read_result="backup/dst@snap1"
				return 0
			}
			zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >"$output_file"
			printf 'status=%s\n' "$?"
		)
	)
	stage_write_output=$(
		(
			temp_call_count=0
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/last-common-stage-$temp_call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			normalize_call_count=0
			zxfer_read_normalized_snapshot_record_list() {
				normalize_call_count=$((normalize_call_count + 1))
				if [ "$normalize_call_count" -eq 1 ]; then
					g_zxfer_runtime_artifact_read_result="backup/dst@snap1"
				else
					g_zxfer_runtime_artifact_read_result="tank/src@snap1"
				fi
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 45
			}
			zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >"$output_file"
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertContains "Last-common snapshot lookup should preserve second temp-file allocation failures." \
		"$tempfile_output" "status=44"
	assertContains "Last-common snapshot lookup should preserve source stage-write failures." \
		"$stage_write_output" "status=45"
}

test_get_last_common_snapshot_preserves_identity_scan_failures() {
	output_file="$TEST_TMPDIR/last_common_awk_failure.out"
	failing_awk="$TEST_TMPDIR/failing_last_common_awk.sh"
	cat >"$failing_awk" <<'EOF'
#!/bin/sh
exit 61
EOF
	chmod +x "$failing_awk"

	set +e
	(
		g_cmd_awk=$failing_awk
		g_test_temp_file_index=0
		zxfer_get_temp_file() {
			g_test_temp_file_index=$((g_test_temp_file_index + 1))
			g_zxfer_temp_file_result="$TEST_TMPDIR/last_common_awk_failure.$g_test_temp_file_index"
			: >"$g_zxfer_temp_file_result"
			printf '%s\n' "$g_zxfer_temp_file_result"
		}
		zxfer_read_normalized_snapshot_record_list() {
			g_zxfer_runtime_artifact_read_result=$1
			return 0
		}
		zxfer_write_snapshot_identities_to_file() {
			printf '%s\n' "snap1" >"$2"
		}
		zxfer_get_last_common_snapshot "tank/src@snap1" "backup/dst@snap1" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Last-common snapshot lookup should preserve final identity-scan awk failures." \
		61 "$status"
	assertEquals "Last-common snapshot lookup should not publish a fallback snapshot when the identity scan fails." \
		"" "$(cat "$output_file")"
}

test_get_dest_snapshots_to_delete_per_dataset_matches_exact_names_without_prefix_collisions() {
	outfile="$TEST_TMPDIR/delete_exact.out"

	zxfer_get_dest_snapshots_to_delete_per_dataset \
		"tank/fs@daily-10" \
		"tank/fs@daily-10 tank/fs@daily-1" >"$outfile"

	assertEquals "Destination-only snapshot deletion should match exact names, not prefixes." \
		"tank/fs@daily-1" "$(cat "$outfile")"
}

test_write_destination_snapshot_paths_for_identity_file_matches_guid_identities_exactly() {
	identity_file="$TEST_TMPDIR/delete_guid_identities.txt"
	outfile="$TEST_TMPDIR/delete_guid_paths.out"
	printf 'snap2\t222\nsnap1\n' >"$identity_file"

	zxfer_write_destination_snapshot_paths_for_identity_file \
		"tank/fs@snap2	222
tank/fs@snap2	999
tank/fs@snap1
tank/fs@orphan	333" \
		"$identity_file" >"$outfile"

	assertEquals "Destination snapshot path extraction should keep only records whose exact name/guid identity appears in the delete set." \
		"tank/fs@snap2
tank/fs@snap1" "$(cat "$outfile")"
}

test_get_dest_snapshots_to_delete_per_dataset_returns_multiple_extra_snapshots_in_input_order() {
	outfile="$TEST_TMPDIR/delete_multiple.out"

	zxfer_get_dest_snapshots_to_delete_per_dataset \
		"tank/fs@snap1 tank/fs@snap3" \
		"tank/fs@snap1 tank/fs@zeta tank/fs@snap3 tank/fs@alpha" >"$outfile"

	assertEquals "Destination-only snapshot deletion should preserve each extra snapshot in destination order." \
		"tank/fs@zeta
tank/fs@alpha" "$(cat "$outfile")"
}

test_get_dest_snapshots_to_delete_per_dataset_preserves_destination_order_for_guid_divergence() {
	outfile="$TEST_TMPDIR/delete_guid_order.out"

	zxfer_get_dest_snapshots_to_delete_per_dataset \
		"tank/fs@snap1	111" \
		"tank/fs@snap1	999 tank/fs@alpha	222" >"$outfile"

	assertEquals "Guid-mismatched destination-only snapshots should still be returned in live destination order." \
		"tank/fs@snap1
tank/fs@alpha" "$(cat "$outfile")"
}

test_get_dest_snapshots_to_delete_per_dataset_reports_destination_identity_write_failures() {
	set +e
	output=$(
		(
			zxfer_write_snapshot_identities_to_file() {
				if [ "$2" = "$g_delete_dest_tmp_file" ]; then
					return 9
				fi
				printf '%s\n' "snap1" >"$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_dest_snapshots_to_delete_per_dataset \
				"tank/fs@snap1" \
				"tank/fs@snap1 tank/fs@snap2"
		)
	)
	status=$?

	assertEquals "Destination identity-write failures should abort snapshot delete planning." \
		1 "$status"
	assertContains "Destination identity-write failures should preserve the new delete-planning error." \
		"$output" "Failed to generate destination snapshot identities for delete planning."
}

test_get_dest_snapshots_to_delete_per_dataset_reports_source_identity_write_failures() {
	set +e
	output=$(
		(
			zxfer_write_snapshot_identities_to_file() {
				if [ "$2" = "$g_delete_source_tmp_file" ]; then
					return 7
				fi
				printf '%s\n' "snap1" >"$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_dest_snapshots_to_delete_per_dataset \
				"tank/fs@snap1" \
				"tank/fs@snap1 tank/fs@snap2"
		)
	)
	status=$?

	assertEquals "Source identity-write failures should abort snapshot delete planning." \
		1 "$status"
	assertContains "Source identity-write failures should preserve the new delete-planning error." \
		"$output" "Failed to generate source snapshot identities for delete planning."
}

test_get_dest_snapshots_to_delete_per_dataset_falls_back_to_serial_when_cleanup_registration_fails() {
	log_file="$TEST_TMPDIR/delete_plan_cleanup_register_fail.log"

	output=$(
		(
			LOG_FILE="$log_file"
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_unregister_cleanup_pid() {
				printf 'unregister:%s\n' "$1" >>"$LOG_FILE"
			}
			zxfer_write_snapshot_identities_to_file() {
				if [ "$2" = "$g_delete_source_tmp_file" ]; then
					sleep 1
					printf '%s\n' "snap1	111" >"$2"
					printf '%s\n' "source" >>"$LOG_FILE"
					return 0
				fi
				printf '%s\n' "snap1	111
snap2	222" >"$2"
				printf '%s\n' "dest" >>"$LOG_FILE"
			}
			zxfer_get_dest_snapshots_to_delete_per_dataset \
				"tank/fs@snap1	111" \
				"tank/fs@snap1	111
tank/fs@snap2	222"
		)
	)
	status=$?

	assertEquals "Delete planning should fall back to a serial identity-write path when validated cleanup registration fails." \
		0 "$status"
	assertEquals "Delete planning should still return the destination-only snapshot after the serial fallback." \
		"tank/fs@snap2" "$output"
	assertEquals "Delete-planning serial fallback should wait for the source identity writer before starting the destination pass." \
		"source
dest" "$(cat "$log_file")"
}

test_get_dest_snapshots_to_delete_per_dataset_preserves_temp_allocation_failures() {
	status=$(
		(
			zxfer_ensure_snapshot_delete_temp_artifacts() {
				return 73
			}
			zxfer_get_dest_snapshots_to_delete_per_dataset \
				"tank/fs@snap1" \
				"tank/fs@snap1 tank/fs@snap2" >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Snapshot delete planning should preserve snapshot-delete temp allocation failures." \
		73 "$status"
}

test_get_dest_snapshots_to_delete_per_dataset_reports_snapshot_identity_diff_failures() {
	set +e
	output=$(
		(
			comm() {
				return 4
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_dest_snapshots_to_delete_per_dataset \
				"tank/fs@snap1" \
				"tank/fs@snap1 tank/fs@snap2"
		)
	)
	status=$?

	assertEquals "Snapshot identity diff failures should abort snapshot delete planning." \
		1 "$status"
	assertContains "Snapshot identity diff failures should preserve the new delete-planning error." \
		"$output" "Failed to diff source and destination snapshot identities for delete planning."
}

test_get_dest_snapshots_to_delete_per_dataset_reports_snapshot_path_mapping_failures() {
	set +e
	output=$(
		(
			comm() {
				printf '%s\n' "snap2"
			}
			zxfer_write_destination_snapshot_paths_for_identity_file() {
				return 6
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_dest_snapshots_to_delete_per_dataset \
				"tank/fs@snap1" \
				"tank/fs@snap1 tank/fs@snap2"
		)
	)
	status=$?

	assertEquals "Snapshot path mapping failures should abort snapshot delete planning." \
		1 "$status"
	assertContains "Snapshot path mapping failures should preserve the new delete-planning error." \
		"$output" "Failed to map destination snapshot identities back to snapshot paths for delete planning."
}

test_delete_snaps_runs_grandfather_checks_before_destroying() {
	log_file="$TEST_TMPDIR/delete_grandfather.log"
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	g_option_g_grandfather_protection=7

	zxfer_grandfather_test() {
		printf 'grandfather %s\n' "$1" >>"$log_file"
	}
	zxfer_run_destination_zfs_cmd() {
		printf 'destroy %s\n' "$*" >>"$log_file"
		return 0
	}

	zxfer_delete_snaps "$source_list" "$dest_list"

	unset -f zxfer_grandfather_test
	unset -f zxfer_run_destination_zfs_cmd
	# Restore the real helper after replacing it with a test stub.
	# shellcheck source=src/zxfer_snapshot_reconcile.sh
	. "$ZXFER_ROOT/src/zxfer_snapshot_reconcile.sh"

	assertContains "Grandfather protection should be checked for each candidate deletion." \
		"$(cat "$log_file")" "grandfather tank/fs@snap3"
	assertContains "Unprotected snapshots should still be destroyed after grandfather checks pass." \
		"$(cat "$log_file")" "destroy destroy tank/fs@snap3"
	assertEquals "Successful deletions should set the destination-delete flag." 1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_propagates_snapshot_delete_plan_failures() {
	source_list=$(printf '%s\n' "tank/fs@snap1")
	dest_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")

	set +e
	output=$(
		(
			zxfer_get_dest_snapshots_to_delete_per_dataset() {
				zxfer_throw_error "Failed to diff source and destination snapshot identities for delete planning."
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_delete_snaps "$source_list" "$dest_list"
		) 2>&1
	)
	status=$?

	assertEquals "Snapshot delete-plan failures should abort zxfer_delete_snaps." \
		1 "$status"
	assertContains "Snapshot delete-plan failures should preserve the original delete-planning diagnostic." \
		"$output" "Failed to diff source and destination snapshot identities for delete planning."
	assertNotContains "Snapshot delete-plan failures should not degrade into an empty delete-set success path." \
		"$output" "No snapshots to delete."
}

test_delete_snaps_marks_rollback_eligible_when_deleting_newer_snapshots() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"

	zxfer_run_destination_zfs_cmd() {
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3")
			printf 'tank/fs@snap2\t200\n'
			printf 'tank/fs@snap3\t300\n'
			;;
		"get -H -o value -p creation tank/fs@snap2")
			printf '%s\n' 200
			;;
		"get -H -o value -p creation tank/fs@snap3")
			printf '%s\n' 300
			;;
		"destroy tank/fs@snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_delete_snaps "$source_list" "$dest_list"

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Deleting a destination snapshot newer than the last common snapshot should preserve rollback eligibility." \
		1 "$g_deleted_dest_newer_snapshots"
	assertEquals "Deleting a newer destination snapshot should still mark that a destroy was issued." \
		1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_batches_creation_time_reads_for_rollback_and_grandfather_checks() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3" "tank/fs@snap4")
	log_file="$TEST_TMPDIR/delete_creation_batch.log"
	current_epoch=$(date +%s)
	common_epoch=$((current_epoch - 10 * 86400))
	snap3_epoch=$((current_epoch - 2 * 86400))
	snap4_epoch=$((current_epoch - 86400))
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_option_g_grandfather_protection=999

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3 tank/fs@snap4")
			printf 'tank/fs@snap2\t%s\n' "$common_epoch"
			printf 'tank/fs@snap3\t%s\n' "$snap3_epoch"
			printf 'tank/fs@snap4\t%s\n' "$snap4_epoch"
			;;
		"destroy tank/fs@snap4,snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_delete_snaps "$source_list" "$dest_list"

	unset -f zxfer_run_destination_zfs_cmd

	batch_count=$("${g_cmd_awk:-awk}" '/^get -H -o name,value -p creation / { count++ } END { print count + 0 }' "$log_file")
	single_count=$("${g_cmd_awk:-awk}" '/^get -H -o value -p creation / { count++ } END { print count + 0 }' "$log_file")

	assertEquals "Delete planning should batch creation-time reads once per dataset before rollback and grandfather checks." \
		1 "$batch_count"
	assertEquals "Grandfather and rollback checks should reuse the cached batch results instead of issuing per-snapshot creation reads." \
		0 "$single_count"
	assertEquals "Deleting snapshots newer than the last common point should keep rollback eligibility." \
		1 "$g_deleted_dest_newer_snapshots"
}

test_delete_snaps_reports_creation_prefetch_failures_without_falling_back_to_single_probes() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	log_file="$TEST_TMPDIR/delete_creation_fallback.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$log_file"
				if [ "$*" = "get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3" ]; then
					printf '%s\n' "Permission denied (publickey)." >&2
					return 38
				fi
				if [ "$*" = "destroy tank/fs@snap3" ]; then
					printf '%s\n' "destroy should not run" >>"$log_file"
					return 0
				fi
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_delete_snaps "$source_list" "$dest_list"
		) 2>&1
	)
	status=$?

	unset -f zxfer_run_destination_zfs_cmd

	batch_count=$("${g_cmd_awk:-awk}" '/^get -H -o name,value -p creation / { count++ } END { print count + 0 }' "$log_file")
	single_count=$("${g_cmd_awk:-awk}" '/^get -H -o value -p creation / { count++ } END { print count + 0 }' "$log_file")

	assertEquals "Delete planning should preserve batched creation-time prefetch failures." \
		38 "$status"
	assertContains "Delete planning should preserve the underlying batched creation-time probe diagnostic." \
		"$output" "Permission denied (publickey)."
	assertContains "Delete planning should report batched creation-time probe failures as a destination creation-time query error." \
		"$output" "Failed to query destination snapshot creation times while planning snapshot deletions. Review prior stderr for the transport or query error."
	assertEquals "Delete planning should still attempt the batched creation-time lookup first." \
		1 "$batch_count"
	assertEquals "Delete planning should not fall back to per-snapshot creation probes after a batched transport/query failure." \
		0 "$single_count"
	assertNotContains "Delete planning should not issue a destroy after batched creation-time prefetch failures." \
		"$(cat "$log_file")" "destroy should not run"
}

test_delete_snaps_falls_back_to_single_creation_probe_when_batched_creation_value_is_malformed() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	log_file="$TEST_TMPDIR/delete_creation_malformed.log"
	current_epoch=$(date +%s)
	common_epoch=$((current_epoch - 10 * 86400))
	snap3_epoch=$((current_epoch - 86400))
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_option_g_grandfather_protection=999

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3")
			printf 'tank/fs@snap2\t%s\n' "$common_epoch"
			printf 'tank/fs@snap3\tunknown\n'
			;;
		"get -H -o value -p creation tank/fs@snap3")
			printf '%s\n' "$snap3_epoch"
			;;
		"destroy tank/fs@snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_delete_snaps "$source_list" "$dest_list"

	unset -f zxfer_run_destination_zfs_cmd

	assertContains "Delete planning should still use one batched creation-time lookup attempt first." \
		"$(cat "$log_file")" "get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3"
	assertContains "Malformed batched creation values should be ignored so later checks fall back to an exact single-snapshot read." \
		"$(cat "$log_file")" "get -H -o value -p creation tank/fs@snap3"
	assertEquals "Exact fallback after malformed batched data should still preserve rollback eligibility detection." \
		1 "$g_deleted_dest_newer_snapshots"
}

test_zxfer_prefetch_destination_snapshot_creation_paths_flushes_when_batch_limit_is_reached() {
	log_file="$TEST_TMPDIR/prefetch_creation_batches.log"
	snapshot_records=$(
		"${g_cmd_awk:-awk}" 'BEGIN {
			for (i = 1; i <= 129; i++) {
				printf "tank/fs@snap%d\n", i
			}
		}'
	)

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		shift 5
		for l_snapshot_path in "$@"; do
			printf '%s\t100\n' "$l_snapshot_path"
		done
	}

	zxfer_reset_destination_snapshot_creation_cache
	zxfer_prefetch_destination_snapshot_creation_paths "$snapshot_records"

	unset -f zxfer_run_destination_zfs_cmd

	batch_count=$("${g_cmd_awk:-awk}" '/^get -H -o name,value -p creation / { count++ } END { print count + 0 }' "$log_file")

	assertEquals "Large snapshot sets should flush one full creation-time batch and one trailing batch." \
		2 "$batch_count"
	assertContains "The first batch should include the earliest queued snapshot path." \
		"$g_destination_snapshot_creation_cache" "tank/fs@snap1	100"
	assertContains "The trailing batch should still cache the last snapshot path." \
		"$g_destination_snapshot_creation_cache" "tank/fs@snap129	100"
}

test_zxfer_prefetch_destination_snapshot_creation_paths_preserves_full_batch_probe_failures() {
	snapshot_records=$(
		"${g_cmd_awk:-awk}" 'BEGIN {
			for (i = 1; i <= 128; i++) {
				printf "tank/fs@snap%d\n", i
			}
		}'
	)
	log_file="$TEST_TMPDIR/prefetch_creation_full_batch_failure.log"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		return 28
	}

	zxfer_reset_destination_snapshot_creation_cache
	zxfer_prefetch_destination_snapshot_creation_paths "$snapshot_records" >/dev/null 2>&1
	status=$?

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Full snapshot-creation batches should preserve probe failures without continuing into trailing work." \
		28 "$status"
	assertEquals "Failed full-batch creation-time probes should not publish partial cache entries." \
		"" "$g_destination_snapshot_creation_cache"
	assertEquals "Failed full-batch creation-time probes should stop after the first batched lookup attempt." \
		1 "$("${g_cmd_awk:-awk}" 'END { print NR + 0 }' "$log_file")"
}

test_zxfer_prefetch_destination_snapshot_creation_paths_preserves_probe_failures() {
	snapshot_records=$(printf '%s\n%s\n' "tank/fs@snap1" "tank/fs@snap2")

	zxfer_run_destination_zfs_cmd() {
		return 27
	}

	zxfer_reset_destination_snapshot_creation_cache
	zxfer_prefetch_destination_snapshot_creation_paths "$snapshot_records" >/dev/null 2>&1
	status=$?

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Prefetch creation-time failures should preserve the underlying probe status." \
		27 "$status"
	assertEquals "Failed creation-time prefetches should not publish partial cache entries." \
		"" "$g_destination_snapshot_creation_cache"
}

test_zxfer_prefetch_delete_snapshot_creation_times_prefetches_last_common_when_delete_list_is_empty() {
	log_file="$TEST_TMPDIR/prefetch_last_common_only.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/src@snap2"

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2")
			printf 'tank/fs@snap2\t200\n'
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_reset_destination_snapshot_creation_cache
	zxfer_prefetch_delete_snapshot_creation_times ""

	unset -f zxfer_run_destination_zfs_cmd

	assertContains "An empty delete list should still prefetch the last common destination snapshot when available." \
		"$(cat "$log_file")" "get -H -o name,value -p creation tank/fs@snap2"
	assertContains "The prefetched last common snapshot should be cached for later rollback checks." \
		"$g_destination_snapshot_creation_cache" "tank/fs@snap2	200"
}

test_deleted_snapshots_include_newer_than_last_common_skips_older_only_deletions() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	zxfer_run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' 200
			;;
		backup/dst@old1)
			printf '%s\n' 100
			;;
		backup/dst@old2)
			printf '%s\n' 150
			;;
		*)
			return 1
			;;
		esac
	}

	if zxfer_deleted_snapshots_include_newer_than_last_common "$(printf '%s\n%s' "backup/dst@old1" "backup/dst@old2")"; then
		fail "Older-only destination deletions should not require rollback."
	fi
}

test_zxfer_get_destination_snapshot_creation_epoch_preserves_probe_failures() {
	zxfer_run_destination_zfs_cmd() {
		return 29
	}

	set +e
	output=$(zxfer_get_destination_snapshot_creation_epoch "tank/fs@snap1" 2>&1)
	status=$?
	set -e

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Creation-epoch lookups should preserve the underlying destination probe status." \
		29 "$status"
	assertEquals "Failed creation-epoch lookups should not publish output." \
		"" "$output"
}

test_deleted_snapshots_include_newer_than_last_common_detects_newer_deletions() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	zxfer_run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' 200
			;;
		backup/dst@old)
			printf '%s\n' 150
			;;
		backup/dst@newer)
			printf '%s\n' 300
			;;
		*)
			return 1
			;;
		esac
	}

	if ! zxfer_deleted_snapshots_include_newer_than_last_common "$(printf '%s\n%s' "backup/dst@old" "backup/dst@newer")"; then
		fail "Deleting a destination snapshot newer than the last common snapshot should keep rollback eligible."
	fi
}

test_deleted_snapshots_include_newer_than_last_common_treats_unknown_last_common_creation_as_rollback_eligible() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	zxfer_run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' "unknown"
			;;
		backup/dst@old)
			printf '%s\n' 100
			;;
		*)
			return 1
			;;
		esac
	}

	if ! zxfer_deleted_snapshots_include_newer_than_last_common "backup/dst@old"; then
		fail "Unknown last-common creation times should fail closed and keep rollback eligible."
	fi
}

test_deleted_snapshots_include_newer_than_last_common_treats_unknown_deleted_creation_as_rollback_eligible() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	zxfer_run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' 200
			;;
		backup/dst@old)
			printf '%s\n' "unknown"
			;;
		*)
			return 1
			;;
		esac
	}

	if ! zxfer_deleted_snapshots_include_newer_than_last_common "backup/dst@old"; then
		fail "Unknown deleted-snapshot creation times should fail closed and keep rollback eligible."
	fi
}

test_deleted_snapshots_include_newer_than_last_common_returns_query_failure_for_creation_probe_errors() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				if [ "$7" = "backup/dst@common" ]; then
					printf '%s\n' "Host key verification failed." >&2
					return 1
				fi
				return 1
			}
			zxfer_deleted_snapshots_include_newer_than_last_common "backup/dst@old"
		) 2>&1
	)
	status=$?

	assertEquals "Rollback-eligibility detection should return a distinct failure status when creation-time lookup fails." \
		2 "$status"
	assertContains "Rollback-eligibility detection should preserve the underlying creation-time probe diagnostic." \
		"$output" "Host key verification failed."
}

test_inspect_delete_snap_marks_destination_empty_when_no_matching_destination_dataset_exists() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3
tank/src@zxfer_2
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/other@zxfer_1
EOF
	)
	g_actual_dest="backup/dst"

	zxfer_inspect_delete_snap 0 "tank/src"

	expected_transfer=$(printf '%s\n%s' "tank/src@zxfer_2" "tank/src@zxfer_3")
	assertEquals "Missing destination datasets should be reported as having no snapshots." 0 "$g_dest_has_snapshots"
	assertEquals "No destination snapshots should yield an empty last common snapshot." "" "$g_last_common_snap"
	assertEquals "All source snapshots should be transferred when the destination dataset is absent." \
		"$expected_transfer" "$g_src_snapshot_transfer_list"
}

test_inspect_delete_snap_marks_destination_present_when_matching_dataset_exists() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
EOF
	)
	g_actual_dest="backup/dst"

	zxfer_inspect_delete_snap 0 "tank/src"

	assertEquals "Matching destination datasets should be marked as present." 1 "$g_dest_has_snapshots"
	assertEquals "The most recent common snapshot should be detected from the matching destination list." \
		"tank/src@zxfer_2	222" "$g_last_common_snap"
}

test_inspect_delete_snap_requires_matching_guid_for_common_snapshot_detection() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2	999
backup/dst@zxfer_1	111
EOF
	)
	g_actual_dest="backup/dst"

	zxfer_inspect_delete_snap 0 "tank/src"

	assertEquals "Same-named but unrelated destination snapshots should not be treated as the common base." \
		"tank/src@zxfer_1	111" "$g_last_common_snap"
	assertEquals "Transfer planning should keep the divergent source snapshot when the destination guid differs." \
		"tank/src@zxfer_2	222
tank/src@zxfer_3	333" "$g_src_snapshot_transfer_list"
}

test_inspect_delete_snap_fetches_identity_records_when_name_only_lists_overlap() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3
tank/src@zxfer_2
tank/src@zxfer_1
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2
backup/dst@zxfer_1
EOF
	)
	g_actual_dest="backup/dst"

	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' \
						"tank/src@zxfer_3	333" \
						"tank/src@zxfer_2	222" \
						"tank/src@zxfer_1	111"
					return 0
				fi
				if [ "$1:$2" = "destination:backup/dst" ]; then
					printf '%s\n' \
						"backup/dst@zxfer_2	999" \
						"backup/dst@zxfer_1	111"
					return 0
				fi
				return 1
			}

			zxfer_inspect_delete_snap 0 "tank/src"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'transfer=%s\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertContains "Name-only overlapping snapshot sets should still use guid validation to find the real common base." \
		"$output" "last=tank/src@zxfer_1	111"
	assertContains "Name-only overlapping snapshot sets should still keep the divergent source snapshot queued for transfer." \
		"$output" "transfer=tank/src@zxfer_2
tank/src@zxfer_3"
}

test_inspect_delete_snap_prefers_file_backed_destination_cache_when_global_list_is_incomplete() {
	cache_file="$TEST_TMPDIR/destination_snapshot_cache_for_reconcile.raw"
	cat >"$cache_file" <<'EOF'
backup/dst/late@autosnap_2026-04-11_12:45:00_frequently	111
backup/dst/late@autosnap_2026-04-11_13:00:03_frequently	222
EOF
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src/late@autosnap_2026-04-11_13:00:03_frequently	222
tank/src/late@autosnap_2026-04-11_12:45:00_frequently	111
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst/late@autosnap_2026-04-11_12:45:00_frequently	111
EOF
	)
	g_zxfer_destination_snapshot_record_cache_file=$cache_file
	g_actual_dest="backup/dst/late"

	zxfer_inspect_delete_snap 0 "tank/src/late"

	assertEquals "Per-dataset planning should keep the newest destination snapshot from the file-backed cache as the common anchor even when the in-memory list is incomplete." \
		"tank/src/late@autosnap_2026-04-11_13:00:03_frequently	222" "$g_last_common_snap"
	assertEquals "Per-dataset planning should leave no snapshots queued once the file-backed destination cache proves the final snapshot already exists." \
		"" "$g_src_snapshot_transfer_list"
}

test_inspect_delete_snap_reports_source_identity_probe_failures_when_name_only_lists_overlap() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_2
tank/src@zxfer_1
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_1
EOF
	)
	g_actual_dest="backup/dst"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					return 31
				fi
				printf '%s\n' "backup/dst@zxfer_1	111"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_inspect_delete_snap 0 "tank/src"
		)
	)
	status=$?

	assertEquals "Name-only overlapping snapshot sets should preserve source identity lookup failures." \
		31 "$status"
	assertContains "Source identity lookup failures should report the specific source dataset." \
		"$output" "Failed to retrieve source snapshot identities for [tank/src]."
}

test_inspect_delete_snap_reports_destination_identity_probe_failures_when_name_only_lists_overlap() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_2
tank/src@zxfer_1
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_1
EOF
	)
	g_actual_dest="backup/dst"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' \
						"tank/src@zxfer_2	222" \
						"tank/src@zxfer_1	111"
					return 0
				fi
				if [ "$1:$2" = "destination:backup/dst" ]; then
					return 32
				fi
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_inspect_delete_snap 0 "tank/src"
		)
	)
	status=$?

	assertEquals "Name-only overlapping snapshot sets should preserve destination identity lookup failures." \
		32 "$status"
	assertContains "Destination identity lookup failures should report the specific destination dataset." \
		"$output" "Failed to retrieve destination snapshot identities for [backup/dst]."
}

test_inspect_delete_snap_reports_source_snapshot_record_lookup_failures() {
	g_actual_dest="backup/dst"

	set +e
	output=$(
		(
			zxfer_get_snapshot_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					return 23
				fi
				printf '%s\n' "backup/dst@zxfer_1	111"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_inspect_delete_snap 0 "tank/src"
		)
	)
	status=$?
	set -e

	assertEquals "Delete planning should preserve source snapshot-record lookup failures." \
		23 "$status"
	assertContains "Source snapshot-record lookup failures should report the specific source dataset." \
		"$output" "Failed to retrieve source snapshot records for [tank/src]."
}

test_inspect_delete_snap_reports_destination_snapshot_record_lookup_failures() {
	g_actual_dest="backup/dst"

	set +e
	output=$(
		(
			zxfer_get_snapshot_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@zxfer_1	111"
					return 0
				fi
				if [ "$1:$2" = "destination:backup/dst" ]; then
					return 29
				fi
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_inspect_delete_snap 0 "tank/src"
		)
	)
	status=$?
	set -e

	assertEquals "Delete planning should preserve destination snapshot-record lookup failures." \
		29 "$status"
	assertContains "Destination snapshot-record lookup failures should report the specific destination dataset." \
		"$output" "Failed to retrieve destination snapshot records for [backup/dst]."
}

test_inspect_delete_snap_reports_last_common_snapshot_failures() {
	g_actual_dest="backup/dst"

	set +e
	output=$(
		(
			zxfer_get_snapshot_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' "tank/src@zxfer_1	111"
				elif [ "$1:$2" = "destination:backup/dst" ]; then
					printf '%s\n' "backup/dst@zxfer_1	111"
				else
					return 1
				fi
			}
			zxfer_get_last_common_snapshot() {
				return 41
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_inspect_delete_snap 0 "tank/src"
		)
	)
	status=$?
	set -e

	assertEquals "Delete planning should preserve last-common snapshot computation failures." \
		41 "$status"
	assertContains "Last-common snapshot failures should report both dataset sides." \
		"$output" "Failed to determine the last common snapshot for [tank/src] and [backup/dst]."
}

test_inspect_delete_snap_invokes_delete_path_when_requested() {
	log_file="$TEST_TMPDIR/inspect_delete.log"
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3
tank/src@zxfer_2
tank/src@zxfer_1
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2
backup/dst@zxfer_1
backup/dst@old_only
EOF
	)
	g_actual_dest="backup/dst"

	(
		zxfer_delete_snaps() {
			printf 'source=%s\n' "$1" >"$log_file"
			printf 'dest=%s\n' "$2" >>"$log_file"
		}
		zxfer_get_snapshot_identity_records_for_dataset() {
			if [ "$1:$2" = "source:tank/src" ]; then
				printf '%s\n' \
					"tank/src@zxfer_3	333" \
					"tank/src@zxfer_2	222" \
					"tank/src@zxfer_1	111"
				return 0
			fi
			if [ "$1:$2" = "destination:backup/dst" ]; then
				printf '%s\n' \
					"backup/dst@zxfer_2	222" \
					"backup/dst@zxfer_1	111" \
					"backup/dst@old_only	999"
				return 0
			fi
			return 1
		}
		zxfer_inspect_delete_snap 1 "tank/src"
	)

	assertContains "zxfer_inspect_delete_snap should pass the filtered source snapshot list into zxfer_delete_snaps." \
		"$(cat "$log_file")" "source=tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111"
	assertContains "zxfer_inspect_delete_snap should pass the matching destination dataset snapshots into zxfer_delete_snaps." \
		"$(cat "$log_file")" "dest=backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
backup/dst@old_only	999"
}

test_inspect_delete_snap_uses_indexed_snapshot_records_when_available() {
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_actual_dest="backup/dst"

	zxfer_build_snapshot_record_index source "$(
		cat <<'EOF'
tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111
EOF
	)"
	zxfer_build_snapshot_record_index destination "$(
		cat <<'EOF'
backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
EOF
	)"

	zxfer_inspect_delete_snap 0 "tank/src"

	assertEquals "Indexed source/destination snapshot records should still drive common-snapshot detection when the legacy global lists are unavailable." \
		"tank/src@zxfer_2	222" "$g_last_common_snap"
	assertEquals "Indexed snapshot records should still produce the correct transfer list." \
		"tank/src@zxfer_3	333" "$g_src_snapshot_transfer_list"
	assertEquals "Indexed destination snapshot records should still mark the destination as having snapshots." \
		1 "$g_dest_has_snapshots"
}

test_inspect_delete_snap_preserves_snapshot_index_state_in_current_shell() {
	output=$(
		(
			g_actual_dest="backup/dst"
			g_lzfs_list_hr_snap=$(printf '%s\n' \
				"tank/src@zxfer_1	111" \
				"tank/src@zxfer_2	222")
			g_rzfs_list_hr_snap=$(printf '%s\n' \
				"backup/dst@zxfer_1	111")

			zxfer_inspect_delete_snap 0 "tank/src" || exit $?
			printf 'index_dir=<%s>\n' "${g_zxfer_snapshot_index_dir:-}"
			if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
				printf 'index_dir_exists=yes\n'
			else
				printf 'index_dir_exists=no\n'
			fi
			printf 'source_ready=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
		)
	)
	status=$?

	assertEquals "Delete planning should keep running when snapshot-record lookups now preserve current-shell index state." \
		0 "$status"
	assertContains "Delete planning should preserve the snapshot-index temp root in current-shell state so trap cleanup can reap it later." \
		"$output" "index_dir=</"
	assertContains "Delete planning should build the snapshot-index temp root in the current shell." \
		"$output" "index_dir_exists=yes"
	assertContains "Delete planning should leave the source snapshot-index marked ready in current-shell state." \
		"$output" "source_ready=1"
	assertContains "Delete planning should leave the destination snapshot-index marked ready in current-shell state." \
		"$output" "dest_ready=1"
}

test_format_snapshot_creation_epoch_for_display_rejects_nonnumeric_input() {
	set +e
	output=$(
		(
			zxfer_format_snapshot_creation_epoch_for_display "not-a-number"
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should reject non-numeric epochs." \
		1 "$status"
	assertEquals "Creation-epoch display formatting should not emit output for non-numeric epochs." \
		"" "$output"
}

test_format_snapshot_creation_epoch_for_display_prefers_date_r_when_available() {
	output=$(
		(
			date() {
				if [ "$1" = "-r" ] && [ "$2" = "123" ]; then
					printf '%s\n' "date-r-rendered"
					return 0
				fi
				return 1
			}
			zxfer_format_snapshot_creation_epoch_for_display 123
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should succeed when date -r is available." \
		0 "$status"
	assertEquals "Creation-epoch display formatting should prefer the date -r result when available." \
		"date-r-rendered" "$output"
}

test_format_snapshot_creation_epoch_for_display_uses_date_d_fallback_when_date_r_is_unavailable() {
	output=$(
		(
			date() {
				if [ "$1" = "-r" ]; then
					return 1
				fi
				if [ "$1" = "-d" ] && [ "$2" = "@123" ]; then
					printf '%s\n' "date-d-rendered"
					return 0
				fi
				return 1
			}
			zxfer_format_snapshot_creation_epoch_for_display 123
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should succeed when the GNU date -d fallback is available." \
		0 "$status"
	assertEquals "Creation-epoch display formatting should use the GNU date -d fallback when date -r is unavailable." \
		"date-d-rendered" "$output"
}

test_grandfather_test_rejects_nonnumeric_creation_epochs() {
	g_option_g_grandfather_protection=1

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unknown"
			}
			zxfer_grandfather_test "tank/fs@unknown"
		)
	)
	status=$?

	assertEquals "Grandfather protection should fail closed when the creation epoch is non-numeric." \
		1 "$status"
	assertContains "Grandfather protection should report the destination snapshot whose creation time was invalid." \
		"$output" "Couldn't determine creation time for destination snapshot tank/fs@unknown."
}

test_deleted_snapshots_include_newer_than_last_common_returns_query_failure_for_deleted_snapshot_probe_errors() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				if [ "$7" = "backup/dst@common" ]; then
					printf '%s\n' 200
				elif [ "$7" = "backup/dst@old" ]; then
					printf '%s\n' "Permission denied." >&2
					return 1
				else
					return 1
				fi
			}
			zxfer_deleted_snapshots_include_newer_than_last_common "backup/dst@old"
		) 2>&1
	)
	status=$?

	assertEquals "Rollback-eligibility detection should return a distinct failure status when a deleted snapshot creation probe fails." \
		2 "$status"
	assertContains "Rollback-eligibility detection should preserve the underlying deleted-snapshot probe diagnostic." \
		"$output" "Permission denied."
}

test_delete_snaps_reports_rollback_eligibility_probe_failures() {
	set +e
	output=$(
		(
			zxfer_get_dest_snapshots_to_delete_per_dataset() {
				printf '%s\n' "tank/fs@snap3"
			}
			zxfer_prefetch_delete_snapshot_creation_times() {
				return 0
			}
			zxfer_deleted_snapshots_include_newer_than_last_common() {
				return 2
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_delete_snaps "tank/fs@snap1" "tank/fs@snap1 tank/fs@snap3"
		)
	)
	status=$?

	assertEquals "Snapshot deletion should fail closed when rollback-eligibility creation probes fail." \
		1 "$status"
	assertContains "Snapshot deletion should report the rollback-eligibility creation-time failure." \
		"$output" "Failed to query destination snapshot creation times while evaluating rollback eligibility."
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
