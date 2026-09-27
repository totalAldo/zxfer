#!/bin/sh
# Destination dataset mapping cases for src/zxfer_destination_state.sh, written
# for the snapshot-discovery fixture (tank/src replicated to backup/dst). Run by
# tests/test_zxfer_destination_state.sh under that fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_destination_snapshot_dataset_helpers_map_root_and_child_datasets() {
	zxfer_map_destination_dataset
	assertEquals "Non-trailing-slash recursive replication should append the source root name under the destination root." \
		"backup/dst/src" "$g_zxfer_destination_dataset_result"
	zxfer_map_destination_dataset "tank/src/child"
	assertEquals "Non-trailing-slash recursive replication should map child datasets beneath the derived destination root." \
		"backup/dst/src/child" "$g_zxfer_destination_dataset_result"

	g_initial_source_had_trailing_slash=1
	zxfer_map_destination_dataset
	assertEquals "Trailing-slash recursive replication should keep the destination root unchanged." \
		"backup/dst" "$g_zxfer_destination_dataset_result"
	zxfer_map_destination_dataset "tank/src/child"
	assertEquals "Trailing-slash recursive replication should map child datasets directly beneath the requested destination." \
		"backup/dst/child" "$g_zxfer_destination_dataset_result"
}

test_destination_snapshot_dataset_helpers_cover_exact_root_and_fallback_mappings() {
	zxfer_map_destination_dataset "otherpool/unrelated"
	assertEquals "Non-trailing-slash mapping should fall back to the destination root when a dataset does not extend the initial source path." \
		"backup/dst/src" "$g_zxfer_destination_dataset_result"

	g_initial_source_had_trailing_slash=1
	zxfer_map_destination_dataset "tank/src"
	assertEquals "Trailing-slash mapping should keep the destination root unchanged for the exact source dataset." \
		"backup/dst" "$g_zxfer_destination_dataset_result"
}

test_destination_snapshot_dataset_helpers_treat_regex_significant_source_names_as_literal_paths() {
	g_initial_source="tank/app.v1"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=0

	zxfer_map_destination_dataset "tank/app.v1/releases.2026"
	assertEquals "Non-trailing-slash mapping should preserve dots in the source root as literal path components." \
		"backup/dst/app.v1/releases.2026" "$g_zxfer_destination_dataset_result"

	g_initial_source_had_trailing_slash=1
	zxfer_map_destination_dataset "tank/app.v1/releases.2026"
	assertEquals "Trailing-slash mapping should still preserve dotted child names as literal path components." \
		"backup/dst/releases.2026" "$g_zxfer_destination_dataset_result"
}
