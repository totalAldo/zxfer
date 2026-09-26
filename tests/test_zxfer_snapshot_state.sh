#!/bin/sh
#
# shunit2 tests for zxfer_snapshot_state.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_recursive_dest_list=""
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
	g_initial_source=""
	g_initial_source_had_trailing_slash=0
	g_destination=""
	g_actual_dest=""
	g_option_R_recursive=""
	g_option_V_very_verbose=0
	g_destination_operating_system=""
	g_zxfer_live_destination_view_file=""
	g_zxfer_live_destination_view_root=""
	g_zxfer_live_destination_dirty_datasets=""
	g_zxfer_live_destination_listing_file=""
	zxfer_reset_failure_context "unit"
}

# Print the newest cached state for a dataset the slow, obvious way, as the
# reference for the linear lookup.
reference_newest_existence_state() {
	# shellcheck disable=SC2016  # awk program should see literal $1/$2.
	printf '%s\n' "$g_destination_existence_cache" |
		"$g_cmd_awk" -F '	' -v ds="$1" '$2 == ds { print $1; exit }'
}

# Print DATASET's cached existence state, or "miss", through the in-shell
# lookup.
cached_state() {
	if zxfer_lookup_destination_existence_cache "$1"; then
		printf '%s\n' "$g_zxfer_destination_existence_cache_entry_result"
	else
		printf '%s\n' miss
	fi
}

test_destination_probe_helpers_load_with_snapshot_state_not_generic_exec() {
	# shellcheck disable=SC2016  # Module-root variables expand inside the clean child shell.
	ownership_output=$(
		ZXFER_SOURCE_MODULES_ROOT="$ZXFER_ROOT" /bin/sh -c '
			. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_modules.sh" || exit 1
			zxfer_load_modules zxfer_exec.sh || exit 1
			if command -v zxfer_probe_destination_existence >/dev/null 2>&1; then
				printf "%s\n" "exec_has_destination_state=yes"
			else
				printf "%s\n" "exec_has_destination_state=no"
			fi

			zxfer_load_modules zxfer_snapshot_state.sh || exit 1
			if command -v zxfer_probe_destination_existence >/dev/null 2>&1; then
				printf "%s\n" "snapshot_has_destination_state=yes"
			else
				printf "%s\n" "snapshot_has_destination_state=no"
			fi
			if command -v zxfer_get_live_destination_record_file >/dev/null 2>&1; then
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

test_zxfer_note_destination_receive_completed_clears_missing_subtree_assumption() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"
	zxfer_note_destination_receive_completed "backup/dst"

	assertEquals "Receive completion should mark the receive target as present." \
		1 "$(cached_state "backup/dst")"
	assertEquals "Receive completion should clear stale missing-subtree defaults so descendants are live-probed." \
		miss "$(cached_state "backup/dst/child")"
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

test_zxfer_destination_hierarchy_helpers_cover_current_shell_paths() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"
	zxfer_mark_destination_hierarchy_exists "backup/dst/child/grandchild"
	root_state=$(cached_state "backup/dst")
	child_state=$(cached_state "backup/dst/child")
	grandchild_state=$(cached_state "backup/dst/child/grandchild")
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

test_zxfer_seed_destination_existence_cache_from_recursive_list_marks_root_and_children_present() {
	zxfer_seed_destination_existence_cache_from_recursive_list "backup/dst" "$(printf '%s\n%s' "backup/dst" "backup/dst/child")"

	assertEquals "Seeding the destination existence cache should remember the cache root." \
		"backup/dst" "$g_destination_existence_cache_root"
	assertEquals "Seeding the destination existence cache should mark the root dataset as present." \
		1 "$(cached_state "backup/dst")"
	assertEquals "Seeding the destination existence cache should mark child datasets as present." \
		1 "$(cached_state "backup/dst/child")"
	assertEquals "Datasets under the seeded root but not listed should read as missing." \
		0 "$(cached_state "backup/dst/unlisted")"
}

test_zxfer_mark_destination_root_missing_in_cache_marks_descendants_missing() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"

	assertEquals "Marking a destination root missing should remember the root dataset." \
		"backup/dst" "$g_destination_existence_cache_root"
	assertEquals "The missing-root cache should report the root dataset as absent." \
		0 "$(cached_state "backup/dst")"
	assertEquals "The missing-root cache should report descendants as absent too." \
		0 "$(cached_state "backup/dst/child")"
	assertEquals "Existence cache lookups outside the complete root should miss so callers live-probe." \
		miss "$(cached_state "other/pool")"
}

test_zxfer_set_destination_existence_cache_entry_newest_entry_shadows_older_entries() {
	zxfer_set_destination_existence_cache_entry "backup/dst" 0
	zxfer_set_destination_existence_cache_entry "backup/dst/child" 1
	zxfer_set_destination_existence_cache_entry "backup/dst" 1

	assertEquals "The newest existence cache entry for a dataset should shadow its older entries." \
		1 "$(cached_state "backup/dst")"
	assertEquals "Updating an existence cache entry should preserve unrelated cached datasets." \
		1 "$(cached_state "backup/dst/child")"

	zxfer_set_destination_existence_cache_entry "backup/dst" 0

	assertEquals "A still-newer existence cache entry should shadow every earlier state for the dataset." \
		0 "$(cached_state "backup/dst")"
}

test_zxfer_lookup_destination_existence_cache_misses_unknown_and_prefix_sibling_datasets() {
	zxfer_set_destination_existence_cache_entry "backup/dst/ab" 1

	assertEquals "Unknown datasets should miss the existence cache so callers live-probe." \
		miss "$(cached_state "backup/dst/other")"
	assertEquals "Dataset-name suffixes of cached datasets should never match a cached row." \
		miss "$(cached_state "b")"
}

test_zxfer_note_destination_dataset_exists_appends_missing_dataset_to_recursive_list() {
	g_recursive_dest_list=$(printf '%s\n' "backup/dst/existing")

	zxfer_note_destination_dataset_exists "backup/dst/newchild"

	assertEquals "Noting a newly existing destination dataset should append it to the recursive destination list." \
		"backup/dst/existing
backup/dst/newchild" "$g_recursive_dest_list"
	assertEquals "Noting an existing destination dataset should mark the dataset as present in the existence cache." \
		1 "$(cached_state "backup/dst/newchild")"
}

test_zxfer_lookup_destination_existence_cache_matches_newest_row_semantics() {
	zxfer_set_destination_existence_cache_entry "backup/dst" 1
	zxfer_set_destination_existence_cache_entry "backup/dst/a" 1
	zxfer_set_destination_existence_cache_entry "backup/dst/b" 0
	zxfer_set_destination_existence_cache_entry "backup/dst/c" 0
	zxfer_set_destination_existence_cache_entry "backup/dst/a" 0
	zxfer_set_destination_existence_cache_entry "backup/dst/b" 1
	zxfer_set_destination_existence_cache_entry "backup/dst/c" 1
	zxfer_set_destination_existence_cache_entry "backup/dst/c" 0
	zxfer_set_destination_existence_cache_entry "backup/dst/my data" 1

	for dataset in "backup/dst" "backup/dst/a" "backup/dst/b" "backup/dst/c" "backup/dst/my data"; do
		lookup_status=0
		zxfer_lookup_destination_existence_cache "$dataset" || lookup_status=$?
		assertEquals "The in-shell lookup should hit cached dataset [$dataset]." \
			0 "$lookup_status"
		assertEquals "The in-shell lookup should return the newest row for [$dataset]." \
			"$(reference_newest_existence_state "$dataset")" \
			"$g_zxfer_destination_existence_cache_entry_result"
	done

	for dataset in "backup/dst/d" "backup/ds" "dst/a" "backup/dst/a/child" "my data"; do
		lookup_status=0
		zxfer_lookup_destination_existence_cache "$dataset" || lookup_status=$?
		assertEquals "Datasets without a row of their own should miss: [$dataset]." \
			1 "$lookup_status"
		assertEquals "A miss should publish no state for [$dataset]." \
			"" "$g_zxfer_destination_existence_cache_entry_result"
	done

	g_destination_existence_cache_root="backup/dst"
	g_destination_existence_cache_root_complete=1
	zxfer_lookup_destination_existence_cache "backup/dst/d"
	assertEquals "Under a complete root, unlisted datasets should read as missing." \
		0 "$g_zxfer_destination_existence_cache_entry_result"
	zxfer_lookup_destination_existence_cache "backup/dst/b"
	assertEquals "Under a complete root, a cached row should still win." \
		1 "$g_zxfer_destination_existence_cache_entry_result"
}

test_zxfer_mark_destination_hierarchy_exists_does_not_grow_cache_for_known_datasets() {
	zxfer_mark_destination_hierarchy_exists "backup/dst/a/b"
	first_cache=$g_destination_existence_cache
	zxfer_mark_destination_hierarchy_exists "backup/dst/a/b"
	zxfer_mark_destination_hierarchy_exists "backup/dst/a"
	zxfer_note_destination_dataset_exists "backup/dst/a/b"

	assertEquals "The first mark should record the dataset and every ancestor once." \
		"1	backup
1	backup/dst
1	backup/dst/a
1	backup/dst/a/b" "$first_cache"
	assertEquals "Marking datasets whose newest row already says 1 should not grow the cache." \
		"$first_cache" "$g_destination_existence_cache"

	zxfer_set_destination_existence_cache_entry "backup/dst" 0
	zxfer_mark_destination_hierarchy_exists "backup/dst/a/b"
	assertEquals "A stale missing row for an ancestor should still be corrected." \
		1 "$(cached_state "backup/dst")"

	g_destination_existence_cache=""
	g_destination_existence_cache_root="backup/dst"
	zxfer_mark_destination_hierarchy_exists "backup/dst/x/y"
	assertEquals "Marking should stop at the cache root." \
		"1	backup/dst
1	backup/dst/x
1	backup/dst/x/y" "$g_destination_existence_cache"
}

test_zxfer_probe_destination_existence_publishes_results_and_cache_in_current_shell() {
	probe_log="$TEST_TMPDIR/probe_existence.log"
	counter_log="$TEST_TMPDIR/probe_existence_counter.log"
	: >"$probe_log"
	: >"$counter_log"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$probe_log"
				if [ "$3" = "backup/present" ]; then
					printf '%s\n' "backup/present"
				elif [ "$3" = "backup/missing" ]; then
					printf '%s\n' "cannot open 'backup/missing': dataset does not exist" >&2
					return 1
				else
					printf '%s\n' "permission denied" >&2
					return 1
				fi
			}
			zxfer_profile_increment_counter() {
				printf '%s\n' "$1" >>"$counter_log"
			}
			zxfer_probe_destination_existence "backup/present"
			printf 'present=%s\n' "$g_zxfer_destination_exists_result"
			zxfer_probe_destination_existence "backup/present"
			printf 'cached=%s\n' "$g_zxfer_destination_exists_result"
			zxfer_probe_destination_existence "backup/present" live
			zxfer_probe_destination_existence "backup/missing"
			printf 'missing=%s\n' "$g_zxfer_destination_exists_result"
			printf 'missing_cached=%s\n' "$(cached_state "backup/missing")"
			broken_status=0
			zxfer_probe_destination_existence "backup/broken" || broken_status=$?
			printf 'broken_status=%s\n' "$broken_status"
			printf 'broken_result=<%s>\n' "$g_zxfer_destination_exists_result"
			printf 'broken_error=%s\n' "$g_zxfer_destination_exists_error"
		)
	)

	assertContains "A present dataset should publish 1." "$output" "present=1"
	assertContains "The second lookup should come from the in-shell cache." "$output" "cached=1"
	assertEquals "Cache hits must not run zfs; live mode must." \
		"list -H backup/present
list -H backup/present
list -H backup/missing
list -H backup/broken" "$(cat "$probe_log")"
	assertEquals "Every real probe, and no cache hit, should bump the exists counter in the caller's shell." \
		"g_zxfer_profile_exists_destination_calls
g_zxfer_profile_exists_destination_calls
g_zxfer_profile_exists_destination_calls
g_zxfer_profile_exists_destination_calls" "$(cat "$counter_log")"
	assertContains "A missing dataset should publish 0." "$output" "missing=0"
	assertContains "Probe results should persist in the caller's existence cache." \
		"$output" "missing_cached=0"
	assertContains "A failed probe should return 1." "$output" "broken_status=1"
	assertContains "A failed probe should publish no result." "$output" "broken_result=<>"
	assertContains "A failed probe should publish the operator message." "$output" \
		"broken_error=Failed to determine whether destination dataset [backup/broken] exists: permission denied"
}

test_zxfer_probe_destination_existence_resolves_ambiguous_sunos_probes_in_current_shell() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			# Exact probes fail without a diagnostic (ambiguous on SunOS).
			zxfer_run_destination_zfs_cmd() {
				if [ "$*" = "list -H -r -o name backup/dst" ]; then
					printf '%s\n' "backup/dst" "backup/dst/old"
				elif [ "$*" = "list -H -r -o name backup/gone" ]; then
					printf '%s\n' "backup/other"
				else
					return 1
				fi
			}
			zxfer_probe_destination_existence "backup/dst/new"
			printf 'new=%s\n' "$g_zxfer_destination_exists_result"
			printf 'new_cached=%s\n' "$(cached_state "backup/dst/new")"
			printf 'parent_cached=%s\n' "$(cached_state "backup/dst")"
			gone_status=0
			zxfer_probe_destination_existence "backup/gone/new" || gone_status=$?
			printf 'gone_status=%s\n' "$gone_status"
			printf 'gone_error=%s\n' "$g_zxfer_destination_exists_error"
		)
	)

	assertContains "A parent listing without the dataset should prove it missing." "$output" "new=0"
	assertContains "The fallback should cache the missing dataset in the caller's shell." \
		"$output" "new_cached=0"
	assertContains "The fallback should cache the listed parent as present." \
		"$output" "parent_cached=1"
	assertContains "A parent listing that lacks the parent itself should fail closed." \
		"$output" "gone_status=1"
	assertContains "The fallback failure should keep its operator message." "$output" \
		"gone_error=Failed to determine whether destination dataset [backup/gone/new] exists: parent recursive listing for [backup/gone] did not contain the parent dataset."
}

test_zxfer_get_live_destination_record_file_serves_view_or_depth_one_listing() {
	zfs_log="$TEST_TMPDIR/live_record_file_zfs.log"
	listing_copy="$TEST_TMPDIR/live_record_file_listing.copy"
	: >"$zfs_log"
	sentinel="$TEST_TMPDIR/listing_operator_file"
	printf '%s\n' "operator data" >"$sentinel"
	ln -s "$sentinel" "$TEST_TMPDIR/inherited_listing_link"

	output=$(
		(
			g_zxfer_live_destination_listing_file="$TEST_TMPDIR/inherited_listing_link"
			g_initial_source="tank/src"
			g_destination="backup"
			g_option_R_recursive="tank/src"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$zfs_log"
				if [ "$*" = "list -Hr -o name,guid -t snapshot backup/src" ]; then
					printf 'backup/src@s1\t1\nbackup/src/my child@s1\t2\n'
				elif [ "$*" = "list -H -d 1 -o name,guid -t snapshot backup/src/my child" ]; then
					printf 'backup/src/my child@s1\t2\nbackup/src/my child@s2\t3\n'
				else
					printf '%s\n' "ssh: broken pipe" >&2
					return 42
				fi
			}
			zxfer_get_live_destination_record_file "backup/src/my child"
			if [ "$g_zxfer_live_destination_record_file_result" = "$g_zxfer_live_destination_view_file" ]; then
				printf 'clean=view\n'
			fi
			zxfer_mark_live_destination_dataset_dirty "backup/src/my child"
			zxfer_get_live_destination_record_file "backup/src/my child"
			if [ "${g_zxfer_live_destination_record_file_result#"$g_zxfer_run_tmp_root"/}" != "$g_zxfer_live_destination_record_file_result" ]; then
				printf 'listing_under_root=yes\n'
			fi
			cp "$g_zxfer_live_destination_record_file_result" "$listing_copy"
			failure_status=0
			zxfer_get_live_destination_record_file "other/dataset" || failure_status=$?
			printf 'failure_status=%s\n' "$failure_status"
			printf 'failure_error=%s\n' "$g_zxfer_live_destination_record_file_error"
		)
	)

	assertContains "A clean dataset under the root should be served by the batched view." \
		"$output" "clean=view"
	assertContains "The depth-1 listing file should live under the run root." \
		"$output" "listing_under_root=yes"
	assertEquals "An inherited listing path must never be written through." \
		"operator data" "$(cat "$sentinel")"
	assertEquals "A dirty dataset should be served by a fresh depth-1 listing." \
		"backup/src/my child@s1	2
backup/src/my child@s2	3" "$(cat "$listing_copy")"
	assertContains "A failed depth-1 listing should return its status." \
		"$output" "failure_status=42"
	assertContains "A failed depth-1 listing should publish its output for the error report." \
		"$output" "failure_error=ssh: broken pipe"
	assertEquals "The view should be captured once, then each depth-1 listing runs live." \
		"list -Hr -o name,guid -t snapshot backup/src
list -H -d 1 -o name,guid -t snapshot backup/src/my child
list -H -d 1 -o name,guid -t snapshot other/dataset" "$(cat "$zfs_log")"
}

test_zxfer_map_destination_dataset_maps_roots_children_and_outsiders() {
	while IFS='|' read -r mapping_initial mapping_slash mapping_destination \
		mapping_source mapping_expected; do
		g_initial_source=$mapping_initial
		g_initial_source_had_trailing_slash=$mapping_slash
		g_destination=$mapping_destination
		zxfer_map_destination_dataset "$mapping_source"
		assertEquals "Mapping [$mapping_source] from [$mapping_initial] (slash $mapping_slash) to [$mapping_destination]." \
			"$mapping_expected" "$g_zxfer_destination_dataset_result"
	done <<'EOF'
tank/src|0|backup||backup/src
tank/src|0|backup|tank/src|backup/src
tank/src|0|backup|tank/src/child|backup/src/child
tank/src|0|backup|tank/src/child/grand child|backup/src/child/grand child
tank/src|0|backup|tank/src1|backup/src
tank/src|0|backup|other/pool|backup/src
tank/src|0|backup|tank|backup/src
tank/src|1|backup/dst||backup/dst
tank/src|1|backup/dst|tank/src|backup/dst
tank/src|1|backup/dst|tank/src/child|backup/dst/child
tank/src|1|backup/dst|tank/src1|backup/dst
pool|0|backup|pool/child|backup/pool/child
tank/my data|0|back|tank/my data/child|back/my data/child
EOF
}

test_zxfer_refresh_live_destination_view_ignores_an_inherited_view_file() {
	sentinel="$TEST_TMPDIR/operator_file"
	printf '%s\n' "operator data" >"$sentinel"
	ln -s "$sentinel" "$TEST_TMPDIR/inherited_view_link"

	output=$(
		(
			g_zxfer_live_destination_view_file="$TEST_TMPDIR/inherited_view_link"
			export g_zxfer_live_destination_view_file
			g_initial_source="srcpool/data"
			g_destination="dstpool/back"
			g_option_R_recursive="srcpool/data"
			zxfer_run_destination_zfs_cmd() {
				printf 'dstpool/back/data@snap1\t111\ndstpool/back/data/c@snap1\t222\n'
			}
			zxfer_get_live_destination_record_file "dstpool/back/data/c"
			if [ "${g_zxfer_live_destination_view_file#"$g_zxfer_run_tmp_root"/}" != "$g_zxfer_live_destination_view_file" ]; then
				printf 'under_run_root=yes\n'
			fi
			printf 'serves=%s\n' "$g_zxfer_live_destination_view_serves_current_dataset"
			printf 'rows=%s\n' "$(zxfer_filter_snapshot_record_file_for_dataset \
				"$g_zxfer_live_destination_record_file_result" "dstpool/back/data/c")"
		)
	)

	assertEquals "An inherited view path must never be written through." \
		"operator data" "$(cat "$sentinel")"
	assertContains "The view should be reallocated under the run root." \
		"$output" "under_run_root=yes"
	assertContains "The fresh view should serve the dataset." "$output" "serves=1"
	assertContains "The fresh view should hold the captured listing." \
		"$output" "rows=dstpool/back/data/c@snap1	222"
}

# shellcheck source=tests/shunit2/shunit2
. "$TESTS_DIR/shunit2/shunit2"
