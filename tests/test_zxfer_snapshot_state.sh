#!/bin/sh
#
# shunit2 tests for zxfer_snapshot_state.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_snapshot_state"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_recursive_dest_list=""
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
	zxfer_reset_failure_context "unit"
}

test_destination_probe_helpers_load_with_snapshot_state_not_generic_exec() {
	# shellcheck disable=SC2016  # Module-root variables expand inside the clean child shell.
	ownership_output=$(
		ZXFER_SOURCE_MODULES_ROOT="$ZXFER_ROOT" /bin/sh -c '
			. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_modules.sh" || exit 1
			zxfer_load_modules zxfer_exec.sh || exit 1
			if command -v zxfer_exists_destination >/dev/null 2>&1; then
				printf "%s\n" "exec_has_destination_state=yes"
			else
				printf "%s\n" "exec_has_destination_state=no"
			fi

			zxfer_load_modules zxfer_snapshot_state.sh || exit 1
			if command -v zxfer_exists_destination >/dev/null 2>&1; then
				printf "%s\n" "snapshot_has_destination_state=yes"
			else
				printf "%s\n" "snapshot_has_destination_state=no"
			fi
			if command -v zxfer_get_live_destination_snapshots >/dev/null 2>&1; then
				printf "%s\n" "snapshot_has_live_view=yes"
			else
				printf "%s\n" "snapshot_has_live_view=no"
			fi
		'
	)
	ownership_status=$?

	assertEquals "Canonical partial loading should succeed across the exec and snapshot-state boundaries." \
		0 "$ownership_status"
	assertContains "Generic execution should not own destination snapshot-state probes." \
		"$ownership_output" "exec_has_destination_state=no"
	assertContains "Snapshot state should own destination existence probes." \
		"$ownership_output" "snapshot_has_destination_state=yes"
	assertContains "Snapshot state should own the complete live destination view." \
		"$ownership_output" "snapshot_has_live_view=yes"
}

test_zxfer_reset_destination_existence_cache_clears_root_and_completion_state() {
	g_destination_existence_cache="1	backup/dst"
	g_destination_existence_cache_root="backup/dst"
	g_destination_existence_cache_root_complete=1

	zxfer_reset_destination_existence_cache

	assertEquals "Resetting the destination existence cache should clear cached dataset states." \
		"" "$g_destination_existence_cache"
	assertEquals "Resetting the destination existence cache should clear the remembered cache root." \
		"" "$g_destination_existence_cache_root"
	assertEquals "Resetting the destination existence cache should clear the root-complete marker." \
		0 "${g_destination_existence_cache_root_complete:-0}"
}

test_zxfer_reset_snapshot_record_indexes_clears_derived_reversed_source_list() {
	g_lzfs_list_hr_S_snap="tank/src@snap2
tank/src@snap1"

	zxfer_reset_snapshot_record_indexes

	assertEquals "Resetting snapshot-record lookup state should clear the derived reversed source record list." \
		"" "${g_lzfs_list_hr_S_snap:-}"
}

test_zxfer_note_destination_receive_completed_clears_missing_subtree_assumption() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"
	zxfer_note_destination_receive_completed "backup/dst"

	root_state=$(zxfer_get_destination_existence_cache_entry "backup/dst")
	set +e
	child_state=$(zxfer_get_destination_existence_cache_entry "backup/dst/child")
	child_status=$?
	set -e

	assertEquals "Receive completion should mark the receive target as present." \
		1 "$root_state"
	assertEquals "Receive completion should clear stale missing-subtree defaults so descendants are live-probed." \
		1 "$child_status"
	assertEquals "Receive completion should not publish an absent descendant cache value after clearing the root-complete marker." \
		"" "$child_state"
}

test_zxfer_ensure_source_snapshot_record_cache_returns_failure_when_reverse_helper_fails() {
	g_lzfs_list_hr_snap=$(printf '%s\n' "tank/src@snap2" "tank/src@snap1")

	set +e
	(
		zxfer_reverse_snapshot_record_list() {
			return 27
		}
		zxfer_ensure_source_snapshot_record_cache
	) >/dev/null 2>&1
	status=$?

	assertEquals "Source snapshot cache population should preserve reverse-ordering helper failures." \
		27 "$status"
	assertEquals "A failed source snapshot cache rebuild should not populate the reversed cache." \
		"" "$g_lzfs_list_hr_S_snap"
}

test_zxfer_ensure_source_snapshot_record_cache_returns_failure_without_source_records() {
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""

	set +e
	zxfer_ensure_source_snapshot_record_cache >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Source snapshot cache population should fail when no source snapshot records exist to derive from." \
		1 "$status"
}

test_zxfer_filter_snapshot_record_file_for_dataset_matches_exact_dataset_prefixes_only() {
	cache_file="$TEST_TMPDIR/filter_snapshot_record_cache.raw"
	cat >"$cache_file" <<'EOF'
tank/a/b@snap1	111
tank/a/bc@snap1	222
tank/a/b@snap2	333
tank/a/b/child@snap1	444
EOF
	filtered_output=$(zxfer_filter_snapshot_record_file_for_dataset "$cache_file" "tank/a/b")

	set +e
	zxfer_filter_snapshot_record_file_for_dataset "$TEST_TMPDIR/missing_snapshot_record_cache.raw" "tank/src" >/dev/null 2>&1
	missing_status=$?
	set -e

	assertEquals "Snapshot-record file filtering should match exact dataset@ prefixes so sibling prefix datasets never collide." \
		"tank/a/b@snap1	111
tank/a/b@snap2	333" "$filtered_output"
	assertEquals "Snapshot-record file filtering should fail when the staged cache file is missing." \
		1 "$missing_status"
}

test_zxfer_get_snapshot_records_for_dataset_filters_staged_source_file() {
	cache_file="$TEST_TMPDIR/source_snapshot_record_cache.raw"
	cat >"$cache_file" <<'EOF'
tank/src/early@snap1
tank/src/late@snap1
tank/src/late@snap2
EOF
	g_zxfer_source_snapshot_record_cache_file=$cache_file

	output=$(zxfer_get_snapshot_records_for_dataset source "tank/src/late")
	reversed_after=${g_lzfs_list_hr_S_snap:-}

	assertEquals "Source snapshot-record lookups should filter the staged flat record file directly." \
		"tank/src/late@snap1
tank/src/late@snap2" "$output"
	assertEquals "File-backed source lookups should not derive the in-memory reversed source list." \
		"" "$reversed_after"
}

test_zxfer_get_snapshot_records_for_dataset_filters_staged_destination_file() {
	cache_file="$TEST_TMPDIR/destination_snapshot_record_cache.raw"
	cat >"$cache_file" <<'EOF'
backup/dst/early@snap1
backup/dst/late@snap1
backup/dst/late@snap2
EOF
	g_zxfer_destination_snapshot_record_cache_file=$cache_file

	output=$(zxfer_get_snapshot_records_for_dataset destination "backup/dst/late")

	assertEquals "Destination snapshot-record lookups should filter the staged flat record file directly." \
		"backup/dst/late@snap1
backup/dst/late@snap2" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_returns_empty_success_for_absent_dataset() {
	cache_file="$TEST_TMPDIR/absent_dataset_snapshot_record_cache.raw"
	output_file="$TEST_TMPDIR/absent_dataset_snapshot_records.out"
	cat >"$cache_file" <<'EOF'
backup/dst@snap1	111
backup/dst/child@snap1	222
EOF
	g_zxfer_destination_snapshot_record_cache_file=$cache_file

	set +e
	zxfer_get_snapshot_records_for_dataset destination "backup/missing" >"$output_file"
	status=$?
	set -e

	assertEquals "Snapshot-record lookups should succeed with empty output when the dataset is absent from the staged file." \
		0 "$status"
	assertEquals "Absent datasets should yield an empty snapshot-record payload." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_snapshot_records_for_dataset_prefers_staged_file_over_incomplete_global_list() {
	cache_file="$TEST_TMPDIR/destination_snapshot_record_preferred_cache.raw"
	cat >"$cache_file" <<'EOF'
backup/dst/early@snap1
backup/dst/late@snap1
backup/dst/late@snap2
EOF
	g_zxfer_destination_snapshot_record_cache_file=$cache_file
	g_rzfs_list_hr_snap=$(printf '%s\n' \
		"backup/dst/early@snap1" \
		"backup/dst/late@snap1")

	output=$(zxfer_get_snapshot_records_for_dataset destination "backup/dst/late")

	assertEquals "Destination snapshot-record lookups should prefer the staged file-backed cache when the in-memory list is incomplete." \
		"backup/dst/late@snap1
backup/dst/late@snap2" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_fails_closed_when_staged_source_file_is_unreadable() {
	set +e
	output=$(
		(
			g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/vanished_source_snapshot_cache.raw"
			g_lzfs_list_hr_snap="tank/src@snap1"
			zxfer_get_snapshot_records_for_dataset source "tank/src"
			printf 'unreachable\n'
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "An unreadable staged source snapshot record file should abort the run instead of degrading to an empty list." \
		1 "$status"
	assertContains "Unreadable staged source snapshot record files should raise the structured fail-closed error." \
		"$output" "Failed to read staged source snapshot record cache."
	assertNotContains "Unreadable staged source snapshot record files should not fall back to in-memory snapshot records." \
		"$output" "unreachable"
	assertNotContains "Unreadable staged source snapshot record files should never publish snapshot records." \
		"$output" "tank/src@snap1"
}

test_zxfer_get_snapshot_records_for_dataset_fails_closed_when_staged_destination_file_is_unreadable() {
	set +e
	output=$(
		(
			g_zxfer_destination_snapshot_record_cache_file="$TEST_TMPDIR/vanished_destination_snapshot_cache.raw"
			g_rzfs_list_hr_snap="backup/dst@snap1"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst"
			printf 'unreachable\n'
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "An unreadable staged destination snapshot record file should abort the run instead of degrading to an empty list." \
		1 "$status"
	assertContains "Unreadable staged destination snapshot record files should raise the structured fail-closed error." \
		"$output" "Failed to read staged destination snapshot record cache."
	assertNotContains "Unreadable staged destination snapshot record files should not fall back to in-memory snapshot records." \
		"$output" "unreachable"
}

test_zxfer_get_snapshot_records_for_dataset_source_uses_global_reversed_list_without_staged_file() {
	output=$(
		(
			source_root_file="$TEST_TMPDIR/lazy_source_root.records"
			g_lzfs_list_hr_snap=$(printf '%s\n%s\n%s\n%s' \
				"tank/a/b@snap1" \
				"tank/a/bc@snap1" \
				"tank/a/b/child@child1" \
				"tank/a/b@snap2")
			printf 'source_reversed_before=%s\n' "${g_lzfs_list_hr_S_snap:-}"
			zxfer_get_snapshot_records_for_dataset source "tank/a/b" >"$source_root_file"
			printf 'source_root=%s\n' "$(cat "$source_root_file")"
			printf 'source_reversed_after=%s\n' "${g_lzfs_list_hr_S_snap:-}"
		)
	)

	assertContains "Source record filtering should not precompute the reversed source cache before lookup." \
		"$output" "source_reversed_before="
	assertContains "Source record filtering should return newest-first records for exactly the requested dataset." \
		"$output" "source_root=tank/a/b@snap2
tank/a/b@snap1"
	assertContains "Source record filtering should populate the reversed cache only after a consumer requests source records." \
		"$output" "source_reversed_after=tank/a/b@snap2
tank/a/b/child@child1
tank/a/bc@snap1
tank/a/b@snap1"
}

test_zxfer_get_snapshot_records_for_dataset_destination_uses_global_list_without_staged_file() {
	g_rzfs_list_hr_snap=$(printf '%s\n%s\n%s' \
		"backup/dst@snap2" \
		"backup/dst@legacy1" \
		"backup/dst/child@child1")

	output=$(zxfer_get_snapshot_records_for_dataset destination "backup/dst")

	assertEquals "Destination snapshot-record lookups should filter the in-memory destination list when no record file is staged." \
		"backup/dst@snap2
backup/dst@legacy1" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_preserves_source_cache_failures() {
	set +e
	output=$(
		(
			g_lzfs_list_hr_snap="tank/src@snap1"
			zxfer_ensure_source_snapshot_record_cache() {
				return 41
			}
			zxfer_get_snapshot_records_for_dataset source "tank/src"
		)
	)
	status=$?
	set -e

	assertEquals "Source snapshot-record lookups should preserve source cache population failures." \
		41 "$status"
	assertEquals "Failed source snapshot-record lookups should not publish payload." \
		"" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_returns_failure_for_unknown_side() {
	set +e
	zxfer_get_snapshot_records_for_dataset nonsense "tank/src" >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Snapshot record lookup should reject unknown sides." 1 "$status"
}

test_zxfer_destination_hierarchy_helpers_cover_current_shell_paths() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"
	zxfer_mark_destination_hierarchy_exists "backup/dst/child/grandchild"
	root_state=$(zxfer_get_destination_existence_cache_entry "backup/dst")
	child_state=$(zxfer_get_destination_existence_cache_entry "backup/dst/child")
	grandchild_state=$(zxfer_get_destination_existence_cache_entry "backup/dst/child/grandchild")
	zxfer_note_destination_dataset_exists "backup/dst/newchild"
	recursive_after_first=$g_recursive_dest_list
	zxfer_note_destination_dataset_exists "backup/dst/newchild"
	recursive_after_duplicate=$g_recursive_dest_list
	set +e
	zxfer_note_destination_dataset_exists ""
	set -e

	assertEquals "Destination hierarchy marking should promote the cached root to present." \
		1 "$root_state"
	assertEquals "Destination hierarchy marking should populate intermediate descendants." \
		1 "$child_state"
	assertEquals "Destination hierarchy marking should populate the requested descendant." \
		1 "$grandchild_state"
	assertEquals "Destination dataset notes should append the first created dataset to the recursive destination list." \
		"backup/dst/newchild" "$recursive_after_first"
	assertEquals "Destination dataset notes should avoid duplicating datasets already present in the recursive destination list." \
		"$recursive_after_first" "$recursive_after_duplicate"
}

test_zxfer_snapshot_record_runtime_helpers_cover_current_shell_paths() {
	source_identity_output_file="$TEST_TMPDIR/source_snapshot_identities.out"
	destination_identity_output_file="$TEST_TMPDIR/destination_snapshot_identities.out"
	dispatch_output_file="$TEST_TMPDIR/dispatched_snapshot_identities.out"

	zxfer_read_normalized_snapshot_record_list "tank/src@snap2	two tank/src@snap1	one"
	normalized_records=$g_zxfer_runtime_artifact_read_result
	case "$normalized_records" in
	*'
')
		normalized_records=${normalized_records%?}
		;;
	esac
	zxfer_read_reversed_snapshot_record_list "tank/src@snap1	one
tank/src@snap2	two"
	reversed_records=$g_zxfer_runtime_artifact_read_result
	case "$reversed_records" in
	*'
')
		reversed_records=${reversed_records%?}
		;;
	esac
	guid_present_status=0
	if zxfer_snapshot_record_list_contains_guid "tank/src@snap1	111" >/dev/null 2>&1; then
		guid_present_status=0
	else
		guid_present_status=$?
	fi
	guid_missing_status=0
	if zxfer_snapshot_record_list_contains_guid "tank/src@snap1" >/dev/null 2>&1; then
		guid_missing_status=0
	else
		guid_missing_status=$?
	fi

	(
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' \
				"tank/src@snap1	111" \
				"tank/src@snap2	222"
		}
		zxfer_get_source_snapshot_identity_records_for_dataset "tank/src" >"$source_identity_output_file"
	)
	(
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' \
				"backup/dst@snap1	111" \
				"backup/dst/child@snapc	333" \
				"backup/dst@snap2	222"
		}
		zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst" >"$destination_identity_output_file"
	)
	(
		zxfer_get_source_snapshot_identity_records_for_dataset() {
			printf '%s\n' \
				"snap2	222" \
				"snap1	111"
		}
		zxfer_get_snapshot_identity_records_for_dataset source "tank/src" >"$dispatch_output_file"
	)

	assertEquals "Snapshot-record normalization helpers should split space-delimited records into newline-delimited records." \
		"tank/src@snap2	two
tank/src@snap1	one" "$normalized_records"
	assertEquals "Snapshot-record reversal helpers should reverse normalized record order." \
		"tank/src@snap2	two
tank/src@snap1	one" "$reversed_records"
	assertEquals "Snapshot-record GUID detection should succeed when a guid field is present." \
		0 "$guid_present_status"
	assertEquals "Snapshot-record GUID detection should fail when the record has no guid field." \
		1 "$guid_missing_status"
	assertEquals "Source snapshot identity helpers should reverse normalized source snapshot records into newest-first full-record order." \
		"tank/src@snap2	222
tank/src@snap1	111" "$(cat "$source_identity_output_file")"
	assertEquals "Destination snapshot identity helpers should keep only exact-dataset full records." \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$(cat "$destination_identity_output_file")"
	assertEquals "Snapshot identity dispatch should return unfiltered identities when no reference record list is supplied." \
		"snap2	222
snap1	111" "$(cat "$dispatch_output_file")"
}

test_zxfer_seed_destination_existence_cache_from_recursive_list_marks_root_and_children_present() {
	zxfer_seed_destination_existence_cache_from_recursive_list "backup/dst" "$(printf '%s\n%s' "backup/dst" "backup/dst/child")"

	assertEquals "Seeding the destination existence cache should remember the cache root." \
		"backup/dst" "$g_destination_existence_cache_root"
	assertEquals "Seeding the destination existence cache should mark the root dataset as present." \
		1 "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
	assertEquals "Seeding the destination existence cache should mark child datasets as present." \
		1 "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"
}

test_zxfer_mark_destination_root_missing_in_cache_marks_descendants_missing() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"

	assertEquals "Marking a destination root missing should remember the root dataset." \
		"backup/dst" "$g_destination_existence_cache_root"
	assertEquals "The missing-root cache should report the root dataset as absent." \
		0 "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
	assertEquals "The missing-root cache should report descendants as absent too." \
		0 "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"
}

test_zxfer_get_destination_existence_cache_entry_misses_outside_complete_root() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"

	set +e
	outside_state=$(zxfer_get_destination_existence_cache_entry "other/pool")
	outside_status=$?
	set -e

	assertEquals "Existence cache lookups outside the complete root should miss so callers live-probe." \
		1 "$outside_status"
	assertEquals "Existence cache misses should not publish a cached state value." \
		"" "$outside_state"
}

test_zxfer_set_destination_existence_cache_entry_newest_entry_shadows_older_entries() {
	zxfer_set_destination_existence_cache_entry "backup/dst" 0
	zxfer_set_destination_existence_cache_entry "backup/dst/child" 1
	zxfer_set_destination_existence_cache_entry "backup/dst" 1

	assertEquals "The newest existence cache entry for a dataset should shadow its older entries." \
		1 "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
	assertEquals "Updating an existence cache entry should preserve unrelated cached datasets." \
		1 "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"

	zxfer_set_destination_existence_cache_entry "backup/dst" 0

	assertEquals "A still-newer existence cache entry should shadow every earlier state for the dataset." \
		0 "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
}

test_zxfer_get_destination_existence_cache_entry_misses_unknown_and_prefix_sibling_datasets() {
	zxfer_set_destination_existence_cache_entry "backup/dst/ab" 1

	set +e
	unknown_state=$(zxfer_get_destination_existence_cache_entry "backup/dst/other")
	unknown_status=$?
	suffix_state=$(zxfer_get_destination_existence_cache_entry "b")
	suffix_status=$?
	set -e

	assertEquals "Unknown datasets should miss the existence cache so callers live-probe." \
		1 "$unknown_status"
	assertEquals "Existence cache misses for unknown datasets should not publish a state value." \
		"" "$unknown_state"
	assertEquals "Dataset-name suffixes of cached datasets should never match a cached row." \
		1 "$suffix_status"
	assertEquals "Existence cache misses for suffix lookups should not publish a state value." \
		"" "$suffix_state"
}

test_zxfer_note_destination_dataset_exists_appends_missing_dataset_to_recursive_list() {
	g_recursive_dest_list=$(printf '%s\n' "backup/dst/existing")

	zxfer_note_destination_dataset_exists "backup/dst/newchild"

	assertEquals "Noting a newly existing destination dataset should append it to the recursive destination list." \
		"backup/dst/existing
backup/dst/newchild" "$g_recursive_dest_list"
	assertEquals "Noting an existing destination dataset should mark the dataset as present in the existence cache." \
		1 "$(zxfer_get_destination_existence_cache_entry "backup/dst/newchild")"
}

test_zxfer_extract_snapshot_helpers_split_path_name_dataset_and_guid() {
	record=$(printf 'tank/src@snap1\t12345')

	assertEquals "Snapshot path extraction should strip the guid suffix from identity records." \
		"tank/src@snap1" "$(zxfer_extract_snapshot_path "$record")"
	assertEquals "Snapshot name extraction should return the snapshot component after @." \
		"snap1" "$(zxfer_extract_snapshot_name "$record")"
	assertEquals "Snapshot dataset extraction should return the dataset component before @." \
		"tank/src" "$(zxfer_extract_snapshot_dataset "$record")"
	assertEquals "Snapshot guid extraction should return the trailing guid field." \
		"12345" "$(zxfer_extract_snapshot_guid "$record")"
	assertEquals "Snapshot identity extraction should emit name plus guid when present." \
		"$(printf 'snap1\t12345')" "$(zxfer_extract_snapshot_identity "$record")"
}

test_zxfer_extract_snapshot_helpers_handle_name_only_and_nonsnapshot_inputs() {
	record_without_guid="tank/src@snap1"
	nonsnapshot_record="tank/src"

	assertEquals "Snapshot path extraction should return the original record when no guid field is present." \
		"tank/src@snap1" "$(zxfer_extract_snapshot_path "$record_without_guid")"
	assertEquals "Snapshot name extraction should return an empty string when the record is not a snapshot path." \
		"" "$(zxfer_extract_snapshot_name "$nonsnapshot_record")"
	assertEquals "Snapshot dataset extraction should return an empty string when the record is not a snapshot path." \
		"" "$(zxfer_extract_snapshot_dataset "$nonsnapshot_record")"
	assertEquals "Snapshot guid extraction should return an empty string when the record has no guid field." \
		"" "$(zxfer_extract_snapshot_guid "$record_without_guid")"
	assertEquals "Snapshot identity extraction should return an empty string when the record is not a snapshot path." \
		"" "$(zxfer_extract_snapshot_identity "$nonsnapshot_record")"
	assertEquals "Snapshot identity extraction should emit only the snapshot name when no guid field is present." \
		"snap1" "$(zxfer_extract_snapshot_identity "$record_without_guid")"
}

test_zxfer_snapshot_record_list_helpers_normalize_reverse_and_detect_guid_overlap() {
	normalized=$(zxfer_normalize_snapshot_record_list "tank/src@snap2 tank/src@snap1")
	reversed=$(zxfer_reverse_snapshot_record_list "$(printf '%s\n%s' "tank/src@snap1" "tank/src@snap2")")
	set +e
	zxfer_snapshot_record_list_contains_guid 'tank/src@snap1	111'
	guid_status=$?
	zxfer_snapshot_record_list_contains_guid 'tank/src@snap1'
	no_guid_status=$?
	zxfer_snapshot_record_lists_share_snapshot_name 'tank/src@snap2 tank/src@snap1' 'backup/dst@snap9 backup/dst@snap1'
	shared_name_status=$?
	zxfer_snapshot_record_lists_share_snapshot_name 'tank/src@snap2' 'backup/dst@other'
	no_shared_name_status=$?
	set -e

	assertEquals "Snapshot record normalization should split space-delimited lists into newline-delimited records." \
		"tank/src@snap2
tank/src@snap1" "$normalized"
	assertEquals "Snapshot record reversal should invert the order of newline-delimited records." \
		"tank/src@snap2
tank/src@snap1" "$reversed"
	assertEquals "Snapshot record guid detection should report true when a tab-delimited guid is present." \
		0 "$guid_status"
	assertEquals "Snapshot record guid detection should report false for name-only lists." \
		1 "$no_guid_status"
	assertEquals "Snapshot-name overlap detection should match on exact snapshot names even across different datasets." \
		0 "$shared_name_status"
	assertEquals "Snapshot-name overlap detection should return false when the two lists share no snapshot names." \
		1 "$no_shared_name_status"
}

test_zxfer_filter_snapshot_identity_records_to_reference_paths_keeps_only_matching_paths() {
	output=$(zxfer_filter_snapshot_identity_records_to_reference_paths \
		"$(printf '%s\n%s' 'tank/src@snap1	111' 'tank/src/child@snap1	222')" \
		"tank/src@snap1")

	assertEquals "Filtering snapshot identity records to reference paths should keep only records whose snapshot path appears in the reference set." \
		"tank/src@snap1	111" "$output"
}

test_zxfer_get_snapshot_identity_records_for_dataset_filters_reference_paths() {
	output=$(
		(
			zxfer_get_source_snapshot_identity_records_for_dataset() {
				printf '%s\n%s\n' "tank/src@snap2	222" "tank/src@snap1	111"
			}
			zxfer_get_snapshot_identity_records_for_dataset source "tank/src" "tank/src@snap1"
		)
	)

	assertEquals "Snapshot identity lookup should apply reference-path filtering when a reference list is supplied." \
		"tank/src@snap1	111" "$output"
}

test_zxfer_get_snapshot_identity_records_for_dataset_destination_returns_unfiltered_output() {
	output=$(
		(
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n%s\n' "backup/dst@snap2	222" "backup/dst@snap1	111"
			}
			zxfer_get_snapshot_identity_records_for_dataset destination "backup/dst"
		)
	)

	assertEquals "Snapshot identity dispatch should return unfiltered destination identities when no reference list is supplied." \
		"backup/dst@snap2	222
backup/dst@snap1	111" "$output"
}

test_zxfer_get_snapshot_identity_records_for_dataset_preserves_source_failures() {
	output_file="$TEST_TMPDIR/snapshot_identity_dispatch_source_failure.out"

	set +e
	(
		zxfer_get_source_snapshot_identity_records_for_dataset() {
			return 27
		}
		zxfer_get_snapshot_identity_records_for_dataset source "tank/src" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Snapshot identity dispatcher should preserve source-side helper failures." \
		27 "$status"
	assertEquals "Snapshot identity dispatcher should not publish partial source-side output on failure." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_snapshot_identity_records_for_dataset_preserves_destination_failures() {
	output_file="$TEST_TMPDIR/snapshot_identity_dispatch_destination_failure.out"

	set +e
	(
		zxfer_get_destination_snapshot_identity_records_for_dataset() {
			return 29
		}
		zxfer_get_snapshot_identity_records_for_dataset destination "backup/dst" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Snapshot identity dispatcher should preserve destination-side helper failures." \
		29 "$status"
	assertEquals "Snapshot identity dispatcher should not publish partial destination-side output on failure." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_source_snapshot_identity_records_for_dataset_preserves_normalization_failures() {
	output_file="$TEST_TMPDIR/source_snapshot_identity_failure.out"

	set +e
	(
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "tank/src@snap1	111"
		}
		zxfer_normalize_snapshot_record_list() {
			return 27
		}
		zxfer_get_source_snapshot_identity_records_for_dataset "tank/src" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Source snapshot identity lookups should preserve normalization failures." \
		27 "$status"
	assertEquals "Source snapshot identity lookups should not publish partial identities after normalization failures." \
		"" "$(cat "$output_file")"
}

test_zxfer_snapshot_record_read_helpers_preserve_tempfile_and_reverse_readback_failures() {
	set +e
	normalized_output=$(
		(
			zxfer_create_runtime_artifact_file() {
				return 61
			}
			zxfer_read_normalized_snapshot_record_list "tank/src@snap1 tank/src@snap2" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	reversed_output=$(
		(
			zxfer_create_runtime_artifact_file() {
				return 62
			}
			zxfer_read_reversed_snapshot_record_list "$(printf '%s\n' "tank/src@snap1" "tank/src@snap2")" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	source_identity_output=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "tank/src@snap1	111"
			}
			zxfer_read_normalized_snapshot_record_list() {
				g_zxfer_runtime_artifact_read_result="tank/src@snap1	111"
				return 0
			}
			zxfer_read_reversed_snapshot_record_list() {
				return 63
			}
			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertContains "Normalized snapshot-record reads should preserve temp-file allocation failures." \
		"$normalized_output" "status=61"
	assertContains "Reversed snapshot-record reads should preserve temp-file allocation failures." \
		"$reversed_output" "status=62"
	assertContains "Source snapshot identity helpers should preserve reverse-readback failures." \
		"$source_identity_output" "status=63"
}

test_zxfer_snapshot_record_transform_read_helper_rejects_invalid_modes() {
	g_zxfer_runtime_artifact_read_result="stale"

	set +e
	output=$(
		(
			g_zxfer_runtime_artifact_read_result="stale"
			zxfer_read_transformed_snapshot_record_list "tank/src@snap1" invalid >/dev/null
			printf 'status=%s\n' "$?"
			printf 'scratch=%s\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)
	status=$?
	set -e

	assertEquals "Invalid snapshot-record transform modes should not abort the test shell." \
		0 "$status"
	assertContains "Invalid snapshot-record transform modes should fail closed." \
		"$output" "status=1"
	assertContains "Invalid snapshot-record transform modes should clear read scratch." \
		"$output" "scratch="
}

test_zxfer_snapshot_record_read_helpers_preserve_readback_and_reverse_stage_failures() {
	set +e
	normalized_read_output=$(
		(
			normalized_tmp_file="$g_zxfer_run_tmp_root/normalized-readback-failure.records"
			zxfer_create_runtime_artifact_file() {
				: >"$normalized_tmp_file"
				g_zxfer_runtime_artifact_path_result=$normalized_tmp_file
				return 0
			}
			zxfer_read_runtime_artifact_file() {
				return 71
			}
			zxfer_read_normalized_snapshot_record_list "tank/src@snap1 tank/src@snap2" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$normalized_tmp_file" ] && printf '%s' yes || printf '%s' no)"
		)
	)
	reversed_stage_output=$(
		(
			reversed_stage_tmp_file="$g_zxfer_run_tmp_root/reversed-stage-failure.records"
			zxfer_create_runtime_artifact_file() {
				: >"$reversed_stage_tmp_file"
				g_zxfer_runtime_artifact_path_result=$reversed_stage_tmp_file
				return 0
			}
			zxfer_reverse_snapshot_record_list() {
				return 72
			}
			zxfer_read_reversed_snapshot_record_list "$(printf '%s\n' "tank/src@snap1" "tank/src@snap2")" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$reversed_stage_tmp_file" ] && printf '%s' yes || printf '%s' no)"
		)
	)
	reversed_read_output=$(
		(
			reversed_read_tmp_file="$g_zxfer_run_tmp_root/reversed-readback-failure.records"
			zxfer_create_runtime_artifact_file() {
				: >"$reversed_read_tmp_file"
				g_zxfer_runtime_artifact_path_result=$reversed_read_tmp_file
				return 0
			}
			zxfer_read_runtime_artifact_file() {
				return 73
			}
			zxfer_read_reversed_snapshot_record_list "$(printf '%s\n' "tank/src@snap1" "tank/src@snap2")" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$reversed_read_tmp_file" ] && printf '%s' yes || printf '%s' no)"
		)
	)
	set -e

	assertContains "Normalized snapshot-record reads should preserve readback failures after staging normalized records." \
		"$normalized_read_output" "status=71"
	assertContains "Normalized snapshot-record readback failures should clean up the staged temp file." \
		"$normalized_read_output" "exists=no"
	assertContains "Reversed snapshot-record reads should preserve reverse-helper failures after staging the temp file." \
		"$reversed_stage_output" "status=72"
	assertContains "Reversed snapshot-record stage failures should clean up the staged temp file." \
		"$reversed_stage_output" "exists=no"
	assertContains "Reversed snapshot-record reads should preserve readback failures after reversing records." \
		"$reversed_read_output" "status=73"
	assertContains "Reversed snapshot-record readback failures should clean up the staged temp file." \
		"$reversed_read_output" "exists=no"
}

test_zxfer_get_destination_snapshot_identity_records_for_dataset_preserves_normalization_failures() {
	output_file="$TEST_TMPDIR/destination_snapshot_identity_failure.out"

	set +e
	(
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "backup/dst@snap1	111"
		}
		zxfer_normalize_snapshot_record_list() {
			return 31
		}
		zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Destination snapshot identity lookups should preserve normalization failures." \
		31 "$status"
	assertEquals "Destination snapshot identity lookups should not publish partial identities after normalization failures." \
		"" "$(cat "$output_file")"
}

test_zxfer_snapshot_identity_helpers_preserve_transport_failures_and_reject_invalid_side() {
	source_output_file="$TEST_TMPDIR/source_snapshot_identity_transport_failure.out"
	destination_output_file="$TEST_TMPDIR/destination_snapshot_identity_transport_failure.out"

	set +e
	(
		zxfer_run_source_zfs_cmd() {
			return 74
		}
		zxfer_get_source_snapshot_identity_records_for_dataset "tank/src" >"$source_output_file"
	)
	source_status=$?
	(
		zxfer_run_destination_zfs_cmd() {
			return 75
		}
		zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst" >"$destination_output_file"
	)
	destination_status=$?
	zxfer_get_snapshot_identity_records_for_dataset nonsense "tank/src" >/dev/null 2>&1
	invalid_side_status=$?
	set -e

	assertEquals "Source snapshot identity helpers should preserve transport failures from the source snapshot probe." \
		74 "$source_status"
	assertEquals "Source snapshot identity helpers should not publish partial identities after a source transport failure." \
		"" "$(cat "$source_output_file")"
	assertEquals "Destination snapshot identity helpers should preserve transport failures from the destination snapshot probe." \
		75 "$destination_status"
	assertEquals "Destination snapshot identity helpers should not publish partial identities after a destination transport failure." \
		"" "$(cat "$destination_output_file")"
	assertEquals "Snapshot identity dispatch should reject unknown sides." \
		1 "$invalid_side_status"
}

# shellcheck source=tests/shunit2/shunit2
. "$TESTS_DIR/shunit2/shunit2"
