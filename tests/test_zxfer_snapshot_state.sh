#!/bin/sh
#
# shunit2 tests for zxfer_snapshot_state.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

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
	TMPDIR="$TEST_TMPDIR"
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	zxfer_reset_snapshot_record_indexes
}

test_zxfer_reset_snapshot_record_indexes_removes_directory_and_resets_state() {
	g_zxfer_snapshot_index_dir="$TEST_TMPDIR/index-reset"
	mkdir -p "$g_zxfer_snapshot_index_dir"
	printf '%s\n' "stale" >"$g_zxfer_snapshot_index_dir/source.records"
	g_zxfer_snapshot_index_unavailable=1
	g_zxfer_source_snapshot_record_index="tank/src	$g_zxfer_snapshot_index_dir/source.records"
	g_zxfer_source_snapshot_record_index_ready=1

	zxfer_reset_snapshot_record_indexes

	assertFalse "Reset should remove the on-disk snapshot index directory." '[ -d "$g_zxfer_snapshot_index_dir" ]'
	assertEquals "Reset should clear the unavailable flag." 0 "$g_zxfer_snapshot_index_unavailable"
	assertEquals "Reset should clear the source index cache." "" "$g_zxfer_source_snapshot_record_index"
}

test_zxfer_get_snapshot_records_for_dataset_lazily_builds_indexes() {
	g_lzfs_list_hr_snap=$(printf '%s\n%s\n%s' \
		"tank/src@snap1	111" \
		"tank/src@snap2	222" \
		"tank/other@snap9	999")

	records_file="$TEST_TMPDIR/source.records"
	zxfer_get_snapshot_records_for_dataset source "tank/src" >"$records_file"
	records=$(cat "$records_file")

	assertEquals "The source dataset should return its matching snapshot records in reverse order." \
		"tank/src@snap2	222
tank/src@snap1	111" "$records"
	assertTrue "Lazy index construction should create an index directory." '[ -n "$g_zxfer_snapshot_index_dir" ]'
}

test_extract_snapshot_identity_returns_name_and_guid() {
	assertEquals "Snapshot identities should retain the name and guid." \
		"snap1	1234" "$(zxfer_extract_snapshot_identity "tank/src@snap1	1234")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
