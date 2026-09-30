#!/bin/sh
#
# shunit2 tests for src/zxfer_destination_state.sh: the destination existence
# cache, the probe's cache and live modes, destination dataset mapping, and
# the live depth-1 listing. The probe's platform fallbacks (the SunOS
# recursive listings) and its messages are pinned black-box in
# tests/test_contract_planning.sh.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_destination_state.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_destination_state"
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

test_zxfer_note_destination_receive_completed_clears_missing_subtree_assumption() {
	zxfer_mark_destination_root_missing_in_cache "backup/dst"
	zxfer_note_destination_receive_completed "backup/dst"

	assertEquals "Receive completion should mark the receive target as present." \
		1 "$(cached_state "backup/dst")"
	assertEquals "Receive completion should clear stale missing-subtree defaults so descendants are live-probed." \
		miss "$(cached_state "backup/dst/child")"
}

test_zxfer_seed_destination_existence_cache_from_recursive_list_marks_root_and_children_present() {
	g_recursive_dest_list="stale/dst"
	zxfer_seed_destination_existence_cache_from_recursive_list "backup/dst" "$(printf '%s\n%s' "backup/dst" "backup/dst/child")"

	assertEquals "Seeding should publish the listing as the destination dataset inventory." \
		"backup/dst
backup/dst/child" "$g_recursive_dest_list"
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
	g_recursive_dest_list="stale/dst"
	zxfer_mark_destination_root_missing_in_cache "backup/dst"

	assertEquals "A missing root leaves the destination dataset inventory empty." \
		"" "$g_recursive_dest_list"

	assertEquals "Marking a destination root missing should remember the root dataset." \
		"backup/dst" "$g_destination_existence_cache_root"
	assertEquals "The missing-root cache should report the root dataset as absent." \
		0 "$(cached_state "backup/dst")"
	assertEquals "The missing-root cache should report descendants as absent too." \
		0 "$(cached_state "backup/dst/child")"
	assertEquals "Existence cache lookups outside the complete root should miss so callers live-probe." \
		miss "$(cached_state "other/pool")"
}

test_zxfer_note_destination_dataset_exists_appends_each_dataset_once_and_marks_it_present() {
	zxfer_note_destination_dataset_exists "backup/dst"
	assertEquals "The first noted dataset should start the destination dataset inventory." \
		"backup/dst" "$g_recursive_dest_list"

	zxfer_note_destination_dataset_exists "backup/dst/child"
	zxfer_note_destination_dataset_exists "backup/dst/child"
	zxfer_note_destination_dataset_exists "backup/dst/ch"
	zxfer_note_destination_dataset_exists "" || :
	assertEquals "Later datasets should be appended once each, as whole newline-delimited names." \
		"backup/dst
backup/dst/child
backup/dst/ch" "$g_recursive_dest_list"
	assertEquals "A noted dataset should read as present in the existence cache." \
		1 "$(cached_state "backup/dst/child")"
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
	: >"$probe_log"

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
			g_zxfer_profile_exists_destination_calls=0
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
			printf 'exists_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
		)
	)

	assertContains "A present dataset should publish 1." "$output" "present=1"
	assertContains "The second lookup should come from the in-shell cache." "$output" "cached=1"
	assertEquals "Cache hits must not run zfs; live mode must." \
		"list -H backup/present
list -H backup/present
list -H backup/missing
list -H backup/broken" "$(cat "$probe_log")"
	assertContains "Every real probe, and no cache hit, should bump the exists counter in the caller's shell." \
		"$output" "exists_calls=4"
	assertContains "A missing dataset should publish 0." "$output" "missing=0"
	assertContains "Probe results should persist in the caller's existence cache." \
		"$output" "missing_cached=0"
	assertContains "A failed probe should return 1." "$output" "broken_status=1"
	assertContains "A failed probe should publish no result." "$output" "broken_result=<>"
	assertContains "A failed probe should publish the operator message." "$output" \
		"broken_error=Failed to determine whether destination dataset [backup/broken] exists: permission denied"
}

test_zxfer_get_live_destination_record_file_lists_the_dataset_at_depth_one() {
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
				if [ "$*" = "list -H -d 1 -o name,guid -t snapshot backup/src/my child" ]; then
					printf 'backup/src/my child@s1\t2\nbackup/src/my child@s2\t3\n'
				else
					printf '%s\n' "ssh: broken pipe" >&2
					return 42
				fi
			}
			zxfer_get_live_destination_record_file "backup/src/my child"
			if [ "${g_zxfer_live_destination_record_file_result#"$g_zxfer_run_tmp_root"/}" != "$g_zxfer_live_destination_record_file_result" ]; then
				printf 'listing_under_root=yes\n'
			fi
			cp "$g_zxfer_live_destination_record_file_result" "$listing_copy"
			failure_status=0
			zxfer_get_live_destination_record_file "other/dataset" || failure_status=$?
			printf 'failure_status=%s\n' "$failure_status"
			printf 'failure_result=<%s>\n' "$g_zxfer_live_destination_record_file_result"
			printf 'failure_error=%s\n' "$g_zxfer_live_destination_record_file_error"
		)
	)

	assertContains "The depth-1 listing file should live under the run root." \
		"$output" "listing_under_root=yes"
	assertEquals "An inherited listing path must never be written through." \
		"operator data" "$(cat "$sentinel")"
	assertEquals "The listing file should hold the dataset's live depth-1 rows." \
		"backup/src/my child@s1	2
backup/src/my child@s2	3" "$(cat "$listing_copy")"
	assertContains "A failed depth-1 listing should return its status." \
		"$output" "failure_status=42"
	assertContains "A failed depth-1 listing should publish no record file." \
		"$output" "failure_result=<>"
	assertContains "A failed depth-1 listing should publish its output for the error report." \
		"$output" "failure_error=ssh: broken pipe"
	assertEquals "Every call should be one depth-1 listing of the named dataset; nothing lists the tree." \
		"list -H -d 1 -o name,guid -t snapshot backup/src/my child
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
tank/app.v1|0|backup/dst|tank/app.v1/releases.2026|backup/dst/app.v1/releases.2026
tank/app.v1|1|backup/dst|tank/app.v1/releases.2026|backup/dst/releases.2026
tank/app.v1|0|backup/dst|tank/appXv1/releases.2026|backup/dst/app.v1
EOF
}

# shellcheck source=tests/shunit2/shunit2
. "$TESTS_DIR/shunit2/shunit2"
