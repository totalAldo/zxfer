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
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_recursive_dest_list=""
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_zxfer_snapshot_index_dir=""
	g_zxfer_snapshot_index_unavailable=0
	g_zxfer_source_snapshot_record_index_dir=""
	g_zxfer_source_snapshot_record_index=""
	g_zxfer_source_snapshot_record_index_ready=0
	g_zxfer_destination_snapshot_record_index_dir=""
	g_zxfer_destination_snapshot_record_index=""
	g_zxfer_destination_snapshot_record_index_ready=0
	zxfer_reset_failure_context "unit"
}

test_zxfer_reset_destination_existence_cache_clears_root_and_completion_state() {
	g_destination_existence_cache="backup/dst	1"
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

test_zxfer_reset_snapshot_record_indexes_removes_index_dir_and_clears_index_state() {
	index_dir="$TEST_TMPDIR/snapshot-index-dir"
	mkdir -p "$index_dir/source"
	printf '%s\n' "stale" >"$index_dir/source/1.records"
	g_zxfer_snapshot_index_dir="$index_dir"
	g_zxfer_snapshot_index_unavailable=1
	g_zxfer_source_snapshot_record_index_dir="$index_dir/source.1.obj"
	g_zxfer_source_snapshot_record_index="tank/src	$index_dir/source/1.records"
	g_zxfer_source_snapshot_record_index_ready=1
	g_zxfer_destination_snapshot_record_index_dir="$index_dir/destination.1.obj"
	g_zxfer_destination_snapshot_record_index="backup/dst	$index_dir/source/1.records"
	g_zxfer_destination_snapshot_record_index_ready=1

	zxfer_reset_snapshot_record_indexes

	assertFalse "Resetting snapshot record indexes should remove the on-disk index directory." \
		"[ -d '$index_dir' ]"
	assertEquals "Resetting snapshot record indexes should clear the index directory path." \
		"" "$g_zxfer_snapshot_index_dir"
	assertEquals "Resetting snapshot record indexes should clear the unavailable marker." \
		0 "${g_zxfer_snapshot_index_unavailable:-0}"
	assertEquals "Resetting snapshot record indexes should clear the source generation directory." \
		"" "$g_zxfer_source_snapshot_record_index_dir"
	assertEquals "Resetting snapshot record indexes should clear the destination generation directory." \
		"" "$g_zxfer_destination_snapshot_record_index_dir"
	assertEquals "Resetting snapshot record indexes should clear the source record map." \
		"" "$g_zxfer_source_snapshot_record_index"
	assertEquals "Resetting snapshot record indexes should clear the destination record map." \
		"" "$g_zxfer_destination_snapshot_record_index"
}

test_zxfer_ensure_snapshot_index_dir_marks_unavailable_when_tmpdir_lookup_fails() {
	set +e
	(
		zxfer_try_get_effective_tmpdir() {
			return 1
		}
		zxfer_ensure_snapshot_index_dir
		l_status=$?
		printf 'status=%s\n' "$l_status"
		printf 'dir=%s\n' "${g_zxfer_snapshot_index_dir:-}"
		printf 'unavailable=%s\n' "${g_zxfer_snapshot_index_unavailable:-0}"
		exit "$l_status"
	) >"$TEST_TMPDIR/index_dir_fail.out"
	status=$?

	assertEquals "Snapshot index directory setup should fail when no trusted temp root is available." \
		1 "$status"
	assertContains "Snapshot index directory failures should mark the index as unavailable." \
		"$(cat "$TEST_TMPDIR/index_dir_fail.out")" "unavailable=1"
}

test_zxfer_ensure_snapshot_index_dir_returns_failure_when_marked_unavailable() {
	g_zxfer_snapshot_index_unavailable=1
	g_zxfer_snapshot_index_dir="$TEST_TMPDIR/stale-snapshot-index-dir"

	set +e
	zxfer_ensure_snapshot_index_dir >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Snapshot index directory setup should fail closed when the unavailable marker is already set." \
		1 "$status"
	assertEquals "The unavailable shortcut should leave the remembered snapshot index directory unchanged." \
		"$TEST_TMPDIR/stale-snapshot-index-dir" "$g_zxfer_snapshot_index_dir"
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

test_zxfer_build_snapshot_record_index_and_lookup_round_trip_source_records() {
	records=$(printf '%s\n' \
		"tank/src@b	222" \
		"tank/src@a	111" \
		"tank/src/child@c	333")

	zxfer_build_snapshot_record_index source "$records"
	output=$(zxfer_get_indexed_snapshot_records_for_dataset source "tank/src")

	assertEquals "Source snapshot index lookups should return the original records for the requested dataset." \
		"tank/src@b	222
tank/src@a	111" "$output"
	assertEquals "Building the source snapshot index should mark the source side as ready." \
		1 "${g_zxfer_source_snapshot_record_index_ready:-0}"
}

test_zxfer_snapshot_record_index_state_helpers_cover_destination_and_invalid_sides() {
	index_dir="$TEST_TMPDIR/destination-index-helper"
	mkdir -p "$index_dir" || fail "Unable to create a destination snapshot-index directory fixture."

	zxfer_set_snapshot_record_index_state_for_side \
		destination "$index_dir" "backup/dst	$index_dir/records/1.records"
	clear_output=$(
		(
			zxfer_clear_snapshot_record_index_state_for_side destination
			printf 'dir=%s\n' "${g_zxfer_destination_snapshot_record_index_dir:-}"
			printf 'map=%s\n' "${g_zxfer_destination_snapshot_record_index:-}"
			printf 'ready=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
		)
	)

	assertContains "Destination snapshot-index state should remember the destination generation path before clearing." \
		"$clear_output" "dir="
	assertContains "Clearing destination snapshot-index state should remove the remembered generation path." \
		"$clear_output" "dir="
	assertContains "Clearing destination snapshot-index state should remove the remembered dataset map." \
		"$clear_output" "map="
	assertContains "Clearing destination snapshot-index state should reset the destination ready flag." \
		"$clear_output" "ready=0"

	assertFalse "Clearing destination snapshot-index state should remove the on-disk generation directory." \
		"[ -d '$index_dir' ]"

	set +e
	zxfer_clear_snapshot_record_index_state_for_side invalid >/dev/null 2>&1
	clear_status=$?
	zxfer_set_snapshot_record_index_state_for_side invalid "$TEST_TMPDIR/ignored" "ignored" >/dev/null 2>&1
	set_status=$?
	set -e

	assertEquals "Snapshot-index clearing should reject unknown sides." 1 "$clear_status"
	assertEquals "Snapshot-index state writes should reject unknown sides." 1 "$set_status"
}

test_zxfer_snapshot_record_index_state_helpers_cover_source_current_shell_paths() {
	index_dir="$TEST_TMPDIR/source-index-helper"
	mkdir -p "$index_dir" || fail "Unable to create a source snapshot-index directory fixture."

	set +e
	zxfer_set_snapshot_record_index_state_for_side \
		source "$index_dir" "tank/src	$index_dir/records/1.records"
	set_status=$?
	set -e

	assertEquals "Source snapshot-index state writes should remember the source generation path in the current shell." \
		"$index_dir" "${g_zxfer_source_snapshot_record_index_dir:-}"
	assertContains "Source snapshot-index state writes should remember the source dataset map in the current shell." \
		"${g_zxfer_source_snapshot_record_index:-}" "tank/src"
	assertEquals "Source snapshot-index state writes should mark the source side as ready in the current shell." \
		1 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Source snapshot-index state writes should succeed for the source side." \
		0 "$set_status"

	set +e
	zxfer_clear_snapshot_record_index_state_for_side source 0
	clear_status=$?
	set -e

	assertEquals "Source snapshot-index clearing without directory removal should clear the remembered generation path." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
	assertEquals "Source snapshot-index clearing without directory removal should clear the remembered dataset map." \
		"" "${g_zxfer_source_snapshot_record_index:-}"
	assertEquals "Source snapshot-index clearing without directory removal should reset the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Source snapshot-index clearing without directory removal should succeed for the source side." \
		0 "$clear_status"
	assertEquals "Source snapshot-index clearing without directory removal should preserve the on-disk generation directory." \
		yes "$(if [ -d "$index_dir" ]; then printf '%s' yes; else printf '%s' no; fi)"
}

test_zxfer_validate_snapshot_record_index_for_side_clears_source_state_when_dir_is_missing_in_current_shell() {
	g_zxfer_source_snapshot_record_index_dir=""
	g_zxfer_source_snapshot_record_index="tank/src	$TEST_TMPDIR/missing-source-index/records/1.records"
	g_zxfer_source_snapshot_record_index_ready=1

	set +e
	zxfer_validate_snapshot_record_index_for_side source >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Source snapshot-index validation should fail closed when the remembered generation directory is missing." \
		1 "$status"
	assertEquals "Source snapshot-index validation should clear the remembered generation path when the directory is missing." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
	assertEquals "Source snapshot-index validation should clear the remembered dataset map when the directory is missing." \
		"" "${g_zxfer_source_snapshot_record_index:-}"
	assertEquals "Source snapshot-index validation should reset the source ready flag when the directory is missing." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-0}"
}

test_zxfer_validate_snapshot_record_index_manifest_file_preserves_runtime_readback_failures() {
	index_dir="$TEST_TMPDIR/manifest-readback-index"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir" || fail "Unable to create a manifest readback fixture directory."
	printf '%s\n' "backup/dst	records/1.records" >"$manifest_path"

	set +e
	(
		zxfer_read_runtime_artifact_file() {
			return 41
		}
		zxfer_validate_snapshot_record_index_manifest_file "$index_dir" "$manifest_path" 1 >/dev/null
	)
	status=$?
	set -e

	assertEquals "Snapshot-index manifest validation should preserve runtime readback failures exactly." \
		41 "$status"
}

test_zxfer_snapshot_record_cache_file_for_side_reports_expected_paths() {
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/source_snapshot_cache.raw"
	g_zxfer_destination_snapshot_record_cache_file="$TEST_TMPDIR/destination_snapshot_cache.raw"

	source_output=$(zxfer_snapshot_record_cache_file_for_side source)
	destination_output=$(zxfer_snapshot_record_cache_file_for_side destination)
	set +e
	zxfer_snapshot_record_cache_file_for_side nonsense >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Snapshot-record cache lookups should return the remembered source cache path." \
		"$TEST_TMPDIR/source_snapshot_cache.raw" "$source_output"
	assertEquals "Snapshot-record cache lookups should return the remembered destination cache path." \
		"$TEST_TMPDIR/destination_snapshot_cache.raw" "$destination_output"
	assertEquals "Snapshot-record cache lookups should reject unknown sides." \
		1 "$status"
}

test_zxfer_validate_snapshot_record_index_manifest_file_rejects_duplicate_dataset_rows() {
	index_dir="$TEST_TMPDIR/manifest-duplicate-dataset-index"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir/records" || fail "Unable to create a duplicate-dataset manifest fixture directory."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/1.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "backup/dst@snap1	111" >/dev/null ||
		fail "Unable to publish the first snapshot-record cache object fixture."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/2.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "backup/dst@snap2	222" >/dev/null ||
		fail "Unable to publish the second snapshot-record cache object fixture."
	cat >"$manifest_path" <<'EOF'
backup/dst	records/1.records
backup/dst	records/2.records
EOF

	set +e
	zxfer_validate_snapshot_record_index_manifest_file "$index_dir" "$manifest_path" 2 >/dev/null
	status=$?
	set -e

	assertEquals "Snapshot-index manifest validation should fail closed on duplicate dataset rows." \
		1 "$status"
}

test_zxfer_validate_snapshot_record_index_manifest_file_rejects_duplicate_relpaths_and_bad_relpaths() {
	index_dir="$TEST_TMPDIR/manifest-duplicate-relpath-index"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir/records" || fail "Unable to create a duplicate-relpath manifest fixture directory."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/1.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "backup/dst@snap1	111" >/dev/null ||
		fail "Unable to publish the snapshot-record cache object fixture."
	cat >"$manifest_path" <<'EOF'
backup/dst	records/1.records
backup/other	records/1.records
EOF

	set +e
	zxfer_validate_snapshot_record_index_manifest_file "$index_dir" "$manifest_path" 2 >/dev/null
	duplicate_status=$?
	printf '%s\n' "backup/dst	invalid.records" >"$manifest_path"
	zxfer_validate_snapshot_record_index_manifest_file "$index_dir" "$manifest_path" 1 >/dev/null
	bad_relpath_status=$?
	set -e

	assertEquals "Snapshot-index manifest validation should fail closed on duplicate record-object relpaths." \
		1 "$duplicate_status"
	assertEquals "Snapshot-index manifest validation should fail closed on non-record relative paths." \
		1 "$bad_relpath_status"
}

test_zxfer_destination_snapshot_record_index_round_trip_and_invalid_record_object_cleanup() {
	records=$(printf '%s\n' \
		"backup/dst@snap2	222" \
		"backup/dst@snap1	111")
	output_file="$TEST_TMPDIR/destination_snapshot_index_round_trip.out"

	g_rzfs_list_hr_snap=$records
	zxfer_build_snapshot_record_index destination "$records"
	zxfer_get_snapshot_records_for_dataset destination "backup/dst" >"$output_file"
	valid_output=$(cat "$output_file")
	ready_before_invalid_lookup=${g_zxfer_destination_snapshot_record_index_ready:-0}
	printf '%s\n' "not a cache object" >"$g_zxfer_destination_snapshot_record_index_dir/records/1.records"

	set +e
	zxfer_get_indexed_snapshot_records_for_dataset destination "backup/dst" >"$output_file"
	invalid_status=$?
	set -e

	assertEquals "Destination snapshot-index lookups should return indexed records when a valid destination generation exists." \
		"backup/dst@snap2	222
backup/dst@snap1	111" "$valid_output"
	assertEquals "Building the destination snapshot index should mark the destination side as ready." \
		1 "$ready_before_invalid_lookup"
	assertEquals "Invalid destination snapshot record objects should fail closed during indexed lookups." \
		1 "$invalid_status"
	assertEquals "Invalid destination snapshot record objects should clear the destination ready flag." \
		0 "${g_zxfer_destination_snapshot_record_index_ready:-0}"
}

test_zxfer_build_snapshot_record_index_rejects_empty_stage_maps_and_preserves_runtime_readback_failures() {
	records=$(printf '%s\n' "backup/dst@snap1	111")
	empty_awk="$TEST_TMPDIR/mock-empty-stage-map-awk.sh"

	cat >"$empty_awk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod 700 "$empty_awk" || fail "Unable to publish the empty-stage-map awk fixture."

	set +e
	(
		g_cmd_awk="$empty_awk"
		zxfer_build_snapshot_record_index destination "$records" >/dev/null
	)
	empty_status=$?
	(
		zxfer_read_runtime_artifact_file() {
			return 55
		}
		zxfer_build_snapshot_record_index destination "$records" >/dev/null
	)
	read_status=$?
	set -e

	assertEquals "Snapshot-index builds should fail closed when stage-map generation produces no manifest payload." \
		1 "$empty_status"
	assertEquals "Snapshot-index builds should preserve raw-record readback failures exactly." \
		55 "$read_status"
}

test_zxfer_build_snapshot_record_index_rejects_malformed_stage_maps_and_meta_write_failures() {
	records=$(printf '%s\n' "tank/src@snap1	111")
	malformed_awk="$TEST_TMPDIR/mock-inline-stage-map-awk.sh"

	cat >"$malformed_awk" <<'EOF'
#!/bin/sh
case "${ZXFER_MOCK_STAGE_ROW_MODE:-}" in
missing-relpath)
	printf 'tank/src\t\t/tmp/mock.raw\n'
	;;
missing-raw)
	printf 'tank/src\trecords/1.records\t\n'
	;;
*)
	exit 1
	;;
esac
EOF
	chmod 700 "$malformed_awk" || fail "Unable to publish the malformed inline stage-map awk fixture."

	set +e
	g_cmd_awk=false
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	stage_map_status=$?
	g_cmd_awk="$malformed_awk"
	ZXFER_MOCK_STAGE_ROW_MODE=missing-relpath
	export ZXFER_MOCK_STAGE_ROW_MODE
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	missing_relpath_status=$?
	ZXFER_MOCK_STAGE_ROW_MODE=missing-raw
	export ZXFER_MOCK_STAGE_ROW_MODE
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	missing_raw_status=$?
	unset ZXFER_MOCK_STAGE_ROW_MODE
	unset g_cmd_awk
	meta_write_output=$(
		(
			write_call_count=0
			zxfer_write_cache_object_contents_to_path() {
				write_call_count=$((write_call_count + 1))
				if [ "$write_call_count" -eq 1 ]; then
					return 0
				fi
				return 1
			}
			zxfer_read_cache_object_file() {
				g_zxfer_cache_object_payload_result="ready"
				g_zxfer_cache_object_metadata_result="manifest=manifest.tsv
entries=1
side=source"
				return 0
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertEquals "Snapshot-index builds should fail closed when inline stage-map generation errors." \
		1 "$stage_map_status"
	assertEquals "Snapshot-index builds should fail closed when the staged map omits the record-object relpath." \
		1 "$missing_relpath_status"
	assertEquals "Snapshot-index builds should fail closed when the staged map omits the raw record path." \
		1 "$missing_raw_status"
	assertContains "Snapshot-index builds should fail closed when metadata objects cannot be written after inline staging succeeds." \
		"$meta_write_output" "status=1"
}

test_zxfer_build_snapshot_record_index_cleans_up_published_dirs_for_publish_and_state_failures_in_current_shell() {
	records=$(printf '%s\n' "tank/src@snap1	111")
	index_root="$TEST_TMPDIR/current_shell_inline_publish_index"
	stage_dir="$TEST_TMPDIR/current_shell_inline_publish.stage.publish-token"
	state_stage_dir="$TEST_TMPDIR/current_shell_inline_state.stage.state-token"
	published_dir="$index_root/source.state-token.obj"
	mkdir -p "$index_root" || fail "Unable to create the inline publish snapshot-index root."

	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$stage_dir
		command mkdir -p "$stage_dir"
		return 0
	}
	g_zxfer_snapshot_index_dir="$index_root"
	set +e
	zxfer_publish_cache_object_directory() {
		return 1
	}
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	publish_status=$?
	set -e
	publish_exists=$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)

	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp
	mkdir -p "$index_root" || fail "Unable to recreate the inline publish snapshot-index root."
	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$state_stage_dir
		command mkdir -p "$state_stage_dir"
		return 0
	}
	zxfer_publish_cache_object_directory() {
		command mv "$1" "$2"
	}
	zxfer_set_snapshot_record_index_state_for_side() {
		return 1
	}
	g_zxfer_snapshot_index_dir="$index_root"
	set +e
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	state_status=$?
	set -e
	state_exists=$([ -e "$published_dir" ] && printf '%s' yes || printf '%s' no)

	assertEquals "Current-shell snapshot-index builds should fail closed when the published directory cannot be installed." \
		1 "$publish_status"
	assertEquals "Current-shell snapshot-index builds should clean up the staged directory when publish fails." \
		"no" "$publish_exists"
	assertEquals "Current-shell snapshot-index builds should fail closed when the published index state cannot be recorded." \
		1 "$state_status"
	assertEquals "Current-shell snapshot-index builds should remove the published generation directory when state recording fails." \
		"no" "$state_exists"

	unset g_cmd_awk
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp
}

test_zxfer_build_snapshot_record_index_rejects_invalid_side_and_cleans_up_stage_dir_when_records_dir_creation_fails() {
	records=$(printf '%s\n' "backup/dst@snap1	111")
	stage_output=$(
		(
			stage_dir="$TEST_TMPDIR/build_snapshot_record_index_records_dir_failure"
			set +e
			zxfer_build_snapshot_record_index invalid "$records" >/dev/null 2>&1
			printf 'invalid=%s\n' "$?"

			zxfer_ensure_snapshot_index_dir() {
				return 0
			}
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result=$stage_dir
				command mkdir -p "$stage_dir"
				return 0
			}
			mkdir() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)"
		)
	)

	assertContains "Snapshot-index builds should reject unknown sides before staging." \
		"$stage_output" "invalid=1"
	assertContains "Snapshot-index builds should fail closed when the per-generation records directory cannot be created." \
		"$stage_output" "status=1"
	assertContains "Snapshot-index builds should clean up the stage directory when records-directory creation fails." \
		"$stage_output" "exists=no"
}

test_zxfer_validate_snapshot_record_index_for_side_covers_shortcuts_and_missing_dirs() {
	output=$(
		(
			g_zxfer_source_snapshot_record_index_ready=1
			g_zxfer_source_snapshot_record_index_dir=""
			g_zxfer_source_snapshot_record_index="stale"
			set +e
			zxfer_validate_snapshot_record_index_for_side source >/dev/null 2>&1
			source_missing_status=$?
			zxfer_validate_snapshot_record_index_for_side invalid >/dev/null 2>&1
			invalid_status=$?
			set -e
			printf 'source_missing=%s\n' "$source_missing_status"
			printf 'source_ready=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'source_map=<%s>\n' "${g_zxfer_source_snapshot_record_index:-}"

			g_zxfer_destination_snapshot_record_index_ready=1
			g_zxfer_destination_snapshot_record_index_dir="$TEST_TMPDIR/missing-destination-index"
			g_zxfer_destination_snapshot_record_index="backup/dst	$TEST_TMPDIR/missing-destination-index/records/1.records"
			set +e
			zxfer_validate_snapshot_record_index_for_side destination >/dev/null 2>&1
			destination_missing_status=$?
			set -e
			printf 'destination_missing=%s\n' "$destination_missing_status"
			printf 'destination_ready=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
			printf 'destination_dir=<%s>\n' "${g_zxfer_destination_snapshot_record_index_dir:-}"
			printf 'invalid=%s\n' "$invalid_status"
		)
	)

	assertContains "Snapshot-index validation should fail when a ready source index is missing its directory path." \
		"$output" "source_missing=1"
	assertContains "Snapshot-index validation should clear the source ready flag when the remembered source directory is empty." \
		"$output" "source_ready=0"
	assertContains "Snapshot-index validation should clear the source dataset map when the remembered source directory is empty." \
		"$output" "source_map=<>"
	assertContains "Snapshot-index validation should fail closed when a remembered destination index directory no longer exists." \
		"$output" "destination_missing=1"
	assertContains "Snapshot-index validation should clear the destination ready flag when the remembered index directory is invalid." \
		"$output" "destination_ready=0"
	assertContains "Snapshot-index validation should clear the remembered destination index directory when validation fails." \
		"$output" "destination_dir=<>"
	assertContains "Snapshot-index validation should reject unknown sides." \
		"$output" "invalid=1"
}

test_zxfer_validate_snapshot_record_index_object_dir_rejects_missing_meta_fields() {
	index_dir="$TEST_TMPDIR/index-object-meta-fixture"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir/records" || fail "Unable to create a snapshot-index object fixture directory."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/1.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "tank/src@snap1	111" >/dev/null ||
		fail "Unable to publish the snapshot-record payload fixture."
	printf '%s\n' "tank/src	records/1.records" >"$manifest_path"

	zxfer_write_cache_object_file_atomically \
		"$index_dir/meta" "$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" "entries=1
side=source" "ready" >/dev/null ||
		fail "Unable to publish the missing-manifest snapshot-index metadata fixture."
	set +e
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	missing_manifest_status=$?
	zxfer_write_cache_object_file_atomically \
		"$index_dir/meta" "$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" "manifest=manifest.tsv
entries=not-a-number
side=source" "ready" >/dev/null ||
		fail "Unable to publish the invalid-entry-count snapshot-index metadata fixture."
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	invalid_entries_status=$?
	zxfer_write_cache_object_file_atomically \
		"$index_dir/meta" "$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" "manifest=manifest.tsv
entries=1" "ready" >/dev/null ||
		fail "Unable to publish the missing-side snapshot-index metadata fixture."
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	missing_side_status=$?
	set -e

	assertEquals "Snapshot-index object validation should fail when manifest metadata is missing." \
		1 "$missing_manifest_status"
	assertEquals "Snapshot-index object validation should fail when entry-count metadata is nonnumeric." \
		1 "$invalid_entries_status"
	assertEquals "Snapshot-index object validation should fail when side metadata is missing for side-specific validation." \
		1 "$missing_side_status"
}

test_zxfer_validate_snapshot_record_index_object_dir_fails_closed_when_metadata_lookups_fail() {
	index_dir="$TEST_TMPDIR/index-object-metadata-lookup-fixture"
	mkdir -p "$index_dir" || fail "Unable to create a snapshot-index metadata lookup fixture directory."

	output=$(
		(
			zxfer_read_cache_object_file() {
				g_zxfer_cache_object_payload_result="ready"
				g_zxfer_cache_object_metadata_result="meta"
				return 0
			}

			zxfer_get_cache_object_metadata_value() {
				if [ "$2" = "manifest" ]; then
					return 1
				fi
			}
			set +e
			zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
			printf 'manifest=%s\n' "$?"

			zxfer_get_cache_object_metadata_value() {
				if [ "$2" = "manifest" ]; then
					printf '%s\n' "manifest.tsv"
				elif [ "$2" = "entries" ]; then
					return 1
				elif [ "$2" = "side" ]; then
					printf '%s\n' "source"
				fi
			}
			zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
			printf 'entries=%s\n' "$?"

			zxfer_get_cache_object_metadata_value() {
				if [ "$2" = "manifest" ]; then
					printf '%s\n' "manifest.tsv"
				elif [ "$2" = "entries" ]; then
					printf '%s\n' "1"
				elif [ "$2" = "side" ]; then
					return 1
				fi
			}
			zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
			printf 'side=%s\n' "$?"

			zxfer_get_cache_object_metadata_value() {
				if [ "$2" = "manifest" ]; then
					printf '%s\n' "manifest.tsv"
				elif [ "$2" = "entries" ]; then
					printf '%s\n' "1"
				elif [ "$2" = "side" ]; then
					printf '%s\n' "destination"
				fi
			}
			zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
			printf 'mismatch=%s\n' "$?"
		)
	)

	assertContains "Snapshot-index object validation should fail closed when manifest metadata lookup fails." \
		"$output" "manifest=1"
	assertContains "Snapshot-index object validation should fail closed when entry-count metadata lookup fails." \
		"$output" "entries=1"
	assertContains "Snapshot-index object validation should fail closed when side metadata lookup fails." \
		"$output" "side=1"
	assertContains "Snapshot-index object validation should fail closed when the remembered side does not match the requested side." \
		"$output" "mismatch=1"
}

test_zxfer_validate_snapshot_record_index_object_dir_preserves_metadata_lookup_failures_in_current_shell() {
	index_dir="$TEST_TMPDIR/index-object-metadata-lookup-current-shell"
	mkdir -p "$index_dir" || fail "Unable to create the current-shell snapshot-index fixture directory."

	zxfer_read_cache_object_file() {
		g_zxfer_cache_object_payload_result="ready"
		g_zxfer_cache_object_metadata_result="meta"
		return 0
	}

	zxfer_get_cache_object_metadata_value() {
		case "$2" in
		manifest)
			return 1
			;;
		esac
	}
	set +e
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	manifest_status=$?
	set -e

	zxfer_get_cache_object_metadata_value() {
		case "$2" in
		manifest)
			printf '%s\n' "manifest.tsv"
			;;
		entries)
			return 1
			;;
		side)
			printf '%s\n' "source"
			;;
		esac
	}
	set +e
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	entries_status=$?
	set -e

	zxfer_get_cache_object_metadata_value() {
		case "$2" in
		manifest)
			printf '%s\n' "manifest.tsv"
			;;
		entries)
			printf '%s\n' "1"
			;;
		side)
			return 1
			;;
		esac
	}
	set +e
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	side_status=$?
	set -e

	assertEquals "Current-shell snapshot-index object validation should fail closed when manifest metadata lookup fails." \
		1 "$manifest_status"
	assertEquals "Current-shell snapshot-index object validation should fail closed when entry-count metadata lookup fails." \
		1 "$entries_status"
	assertEquals "Current-shell snapshot-index object validation should fail closed when side metadata lookup fails." \
		1 "$side_status"
}

test_zxfer_validate_snapshot_record_index_object_dir_accepts_valid_metadata_in_current_shell() {
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp

	index_dir="$TEST_TMPDIR/index-object-valid-current-shell"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir/records" || fail "Unable to create the valid current-shell snapshot-index fixture directory."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/1.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "tank/src@snap1	111" >/dev/null ||
		fail "Unable to publish the valid current-shell snapshot-record fixture."
	printf '%s\n' "tank/src	records/1.records" >"$manifest_path"
	zxfer_write_cache_object_file_atomically \
		"$index_dir/meta" "$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" "manifest=manifest.tsv
entries=1
side=source" "ready" >/dev/null ||
		fail "Unable to publish the valid current-shell snapshot-index metadata fixture."

	set +e
	output=$(zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source")
	status=$?
	set -e

	assertEquals "Current-shell snapshot-index object validation should accept valid metadata and manifest contents." \
		0 "$status"
	assertEquals "Current-shell snapshot-index object validation should return the dataset-to-record map on success." \
		"tank/src	$index_dir/records/1.records" "$output"
}

test_zxfer_validate_snapshot_record_index_for_side_preserves_valid_source_index_in_current_shell() {
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp

	records=$(printf '%s\n' "tank/src@snap1	111" "tank/src@snap2	222")

	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1 ||
		fail "Unable to build the current-shell source-side snapshot-index fixture."
	remembered_dir=$g_zxfer_source_snapshot_record_index_dir

	zxfer_validate_snapshot_record_index_for_side source
	status=$?

	assertEquals "Current-shell source-side snapshot-index validation should preserve valid remembered state." \
		0 "$status"
	assertEquals "Current-shell source-side snapshot-index validation should keep the remembered generation directory." \
		"$remembered_dir" "$g_zxfer_source_snapshot_record_index_dir"
	assertContains "Current-shell source-side snapshot-index validation should keep the remembered dataset in the active map." \
		"$g_zxfer_source_snapshot_record_index" "tank/src	$g_zxfer_source_snapshot_record_index_dir/records/1.records"
	assertEquals "Current-shell source-side snapshot-index validation should keep the ready flag set." \
		1 "${g_zxfer_source_snapshot_record_index_ready:-0}"
}

test_zxfer_validate_snapshot_record_index_object_dir_rejects_side_mismatch_in_current_shell() {
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp

	index_dir="$TEST_TMPDIR/index-object-side-mismatch-current-shell"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir/records" || fail "Unable to create the side-mismatch snapshot-index fixture directory."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/1.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "tank/src@snap1	111" >/dev/null ||
		fail "Unable to publish the side-mismatch snapshot-record fixture."
	printf '%s\n' "tank/src	records/1.records" >"$manifest_path"
	zxfer_write_cache_object_file_atomically \
		"$index_dir/meta" "$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" "manifest=manifest.tsv
entries=1
side=destination" "ready" >/dev/null ||
		fail "Unable to publish the side-mismatch snapshot-index metadata fixture."

	set +e
	zxfer_validate_snapshot_record_index_object_dir "$index_dir" "source" >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Current-shell snapshot-index object validation should reject side metadata that does not match the expected side." \
		1 "$status"
}

test_zxfer_validate_snapshot_record_index_manifest_file_accepts_valid_manifest_in_current_shell() {
	index_dir="$TEST_TMPDIR/manifest-valid-current-shell"
	manifest_path="$index_dir/manifest.tsv"
	mkdir -p "$index_dir/records" || fail "Unable to create the valid manifest fixture directory."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/1.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "tank/src@snap1	111" >/dev/null ||
		fail "Unable to publish the first valid manifest snapshot-record fixture."
	zxfer_write_cache_object_file_atomically \
		"$index_dir/records/2.records" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "tank/src/child@snap1	222" >/dev/null ||
		fail "Unable to publish the second valid manifest snapshot-record fixture."
	printf 'tank/src\trecords/1.records\ntank/src/child\trecords/2.records\n' >"$manifest_path"

	set +e
	output=$(zxfer_validate_snapshot_record_index_manifest_file "$index_dir" "$manifest_path" 2)
	status=$?
	set -e

	assertEquals "Current-shell snapshot-index manifest validation should accept valid dataset-to-record mappings." \
		0 "$status"
	assertEquals "Current-shell snapshot-index manifest validation should return the normalized absolute record map." \
		"tank/src	$index_dir/records/1.records
tank/src/child	$index_dir/records/2.records" "$output"
}

test_zxfer_build_snapshot_record_index_preserves_stage_and_validation_failures() {
	records=$(printf '%s\n' "tank/src@snap1	111")

	set +e
	stage_dir_output=$(
		(
			zxfer_create_cache_object_stage_dir_in_parent() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	record_object_output=$(
		(
			zxfer_write_cache_object_contents_to_path() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	manifest_output=$(
		(
			zxfer_validate_snapshot_record_index_manifest_file() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	object_dir_output=$(
		(
			zxfer_validate_snapshot_record_index_object_dir() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set_state_output=$(
		(
			zxfer_set_snapshot_record_index_state_for_side() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertContains "Snapshot-index builds should fail closed when the stage directory cannot be allocated." \
		"$stage_dir_output" "status=1"
	assertContains "Snapshot-index builds should fail closed when per-dataset record objects cannot be written." \
		"$record_object_output" "status=1"
	assertContains "Snapshot-index builds should fail closed when the staged manifest cannot be validated." \
		"$manifest_output" "status=1"
	assertContains "Snapshot-index builds should fail closed when the published object directory does not validate." \
		"$object_dir_output" "status=1"
	assertContains "Snapshot-index builds should fail closed when the new index state cannot be recorded after publish." \
		"$set_state_output" "status=1"
}

test_zxfer_build_snapshot_record_index_preserves_empty_stage_map_and_meta_write_failures_in_current_shell() {
	records=$(printf '%s\n' "tank/src@snap1	111")
	empty_awk="$TEST_TMPDIR/mock-empty-inline-stage-map-awk.sh"
	stage_dir="$TEST_TMPDIR/current-shell-inline-empty-stage"
	meta_stage_dir="$TEST_TMPDIR/current-shell-inline-meta-stage"
	index_root="$TEST_TMPDIR/current-shell-inline-index"
	mkdir -p "$index_root" || fail "Unable to create the inline snapshot-index root."
	cat >"$empty_awk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod 700 "$empty_awk" || fail "Unable to publish the empty inline stage-map awk fixture."

	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$stage_dir
		command mkdir -p "$stage_dir"
		return 0
	}
	g_cmd_awk="$empty_awk"
	g_zxfer_snapshot_index_dir="$index_root"
	set +e
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	empty_status=$?
	set -e

	assertEquals "Current-shell snapshot-index builds should fail closed when the staged map is empty." \
		1 "$empty_status"
	assertEquals "Current-shell snapshot-index builds should clean up the stage directory when the staged map is empty." \
		"no" "$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)"

	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$meta_stage_dir
		command mkdir -p "$meta_stage_dir"
		return 0
	}
	zxfer_write_cache_object_contents_to_path() {
		l_path=$1
		case "$l_path" in
		*/meta)
			return 1
			;;
		esac
		command mkdir -p "$(dirname "$l_path")" || return 1
		printf '%s\n' "mock cache object" >"$l_path" || return 1
		return 0
	}
	zxfer_read_cache_object_file() {
		g_zxfer_cache_object_payload_result="ready"
		g_zxfer_cache_object_metadata_result="manifest=manifest.tsv
entries=1
side=source"
		return 0
	}
	zxfer_validate_snapshot_record_index_manifest_file() {
		printf '%s\n' "tank/src	$meta_stage_dir/records/1.records"
		return 0
	}
	g_cmd_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	set +e
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	meta_status=$?
	set -e

	assertEquals "Current-shell snapshot-index builds should fail closed when metadata objects cannot be written after staging." \
		1 "$meta_status"
	assertEquals "Current-shell snapshot-index builds should clean up the stage directory when metadata writes fail." \
		"no" "$([ -e "$meta_stage_dir" ] && printf '%s' yes || printf '%s' no)"
}

test_zxfer_build_snapshot_record_index_from_file_rejects_invalid_inputs_and_malformed_stage_maps() {
	records_file="$TEST_TMPDIR/source_snapshot_records.raw"
	malformed_awk="$TEST_TMPDIR/mock-file-stage-map-awk.sh"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	cat >"$malformed_awk" <<'EOF'
#!/bin/sh
if [ "${ZXFER_MOCK_STAGE_ROW_MODE:-}" = "missing-relpath" ]; then
	printf 'tank/src\t\t/tmp/mock.raw\n'
elif [ "${ZXFER_MOCK_STAGE_ROW_MODE:-}" = "missing-raw" ]; then
	printf 'tank/src\trecords/1.records\t\n'
fi
EOF
	chmod 700 "$malformed_awk" || fail "Unable to publish the malformed file-backed stage-map awk fixture."

	set +e
	zxfer_build_snapshot_record_index_from_file invalid "$records_file" >/dev/null 2>&1
	invalid_side_status=$?
	zxfer_build_snapshot_record_index_from_file source "$TEST_TMPDIR/missing-records.raw" >/dev/null 2>&1
	missing_file_status=$?
	missing_relpath_output=$(
		(
			g_cmd_awk="$malformed_awk"
			ZXFER_MOCK_STAGE_ROW_MODE=missing-relpath
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	missing_raw_output=$(
		(
			g_cmd_awk="$malformed_awk"
			ZXFER_MOCK_STAGE_ROW_MODE=missing-raw
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertEquals "File-backed snapshot-index builds should reject unknown sides." \
		1 "$invalid_side_status"
	assertEquals "File-backed snapshot-index builds should reject source record files that are missing or unreadable." \
		1 "$missing_file_status"
	assertContains "File-backed snapshot-index builds should fail closed when the staged map omits the record-object relpath." \
		"$missing_relpath_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when the staged map omits the raw record path." \
		"$missing_raw_output" "status=1"
}

test_zxfer_build_snapshot_record_index_from_file_cleans_up_stage_dir_for_malformed_stage_rows_in_current_shell() {
	records_file="$TEST_TMPDIR/current_shell_source_snapshot_records.raw"
	malformed_awk="$TEST_TMPDIR/current_shell_file_stage_map_awk.sh"
	index_root="$TEST_TMPDIR/current_shell_file_stage_index"
	relpath_stage_dir="$TEST_TMPDIR/current_shell_file_missing_relpath.stage.relpath-token"
	raw_stage_dir="$TEST_TMPDIR/current_shell_file_missing_raw.stage.raw-token"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	mkdir -p "$index_root" || fail "Unable to create the file-backed malformed-row snapshot-index root."
	cat >"$malformed_awk" <<'EOF'
#!/bin/sh
case "${ZXFER_MOCK_STAGE_ROW_MODE:-}" in
missing-relpath)
	printf 'tank/src\t\t/tmp/mock.raw\n'
	;;
missing-raw)
	printf 'tank/src\trecords/1.records\t\n'
	;;
*)
	exit 1
	;;
esac
EOF
	chmod 700 "$malformed_awk" || fail "Unable to publish the current-shell malformed file-backed stage-map awk fixture."

	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$relpath_stage_dir
		command mkdir -p "$relpath_stage_dir"
		return 0
	}
	g_zxfer_snapshot_index_dir="$index_root"
	g_cmd_awk="$malformed_awk"
	ZXFER_MOCK_STAGE_ROW_MODE=missing-relpath
	export ZXFER_MOCK_STAGE_ROW_MODE
	set +e
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	missing_relpath_status=$?
	set -e
	missing_relpath_exists=$([ -e "$relpath_stage_dir" ] && printf '%s' yes || printf '%s' no)

	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp
	mkdir -p "$index_root" || fail "Unable to recreate the file-backed malformed-row snapshot-index root."
	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$raw_stage_dir
		command mkdir -p "$raw_stage_dir"
		return 0
	}
	g_zxfer_snapshot_index_dir="$index_root"
	g_cmd_awk="$malformed_awk"
	ZXFER_MOCK_STAGE_ROW_MODE=missing-raw
	export ZXFER_MOCK_STAGE_ROW_MODE
	set +e
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	missing_raw_status=$?
	set -e
	missing_raw_exists=$([ -e "$raw_stage_dir" ] && printf '%s' yes || printf '%s' no)
	unset ZXFER_MOCK_STAGE_ROW_MODE

	assertEquals "Current-shell file-backed snapshot-index builds should fail closed when the staged map omits the record-object relpath." \
		1 "$missing_relpath_status"
	assertEquals "Current-shell file-backed snapshot-index builds should clean up the stage directory when the file-backed staged map omits the relpath." \
		"no" "$missing_relpath_exists"
	assertEquals "Current-shell file-backed snapshot-index builds should fail closed when the staged map omits the raw record path." \
		1 "$missing_raw_status"
	assertEquals "Current-shell file-backed snapshot-index builds should clean up the stage directory when the file-backed staged map omits the raw path." \
		"no" "$missing_raw_exists"

	unset ZXFER_MOCK_STAGE_ROW_MODE
	unset g_cmd_awk
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp
}

test_zxfer_build_snapshot_record_index_from_file_preserves_stage_and_publish_failures() {
	records_file="$TEST_TMPDIR/source_snapshot_records_for_file_build.raw"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"

	set +e
	stage_dir_output=$(
		(
			zxfer_create_cache_object_stage_dir_in_parent() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	readback_output=$(
		(
			zxfer_read_runtime_artifact_file() {
				return 64
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	record_object_output=$(
		(
			zxfer_write_cache_object_contents_to_path() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	record_read_output=$(
		(
			zxfer_write_cache_object_contents_to_path() {
				return 0
			}
			zxfer_read_cache_object_file() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	meta_write_output=$(
		(
			write_call_count=0
			zxfer_write_cache_object_contents_to_path() {
				write_call_count=$((write_call_count + 1))
				if [ "$write_call_count" -eq 1 ]; then
					return 0
				fi
				return 1
			}
			zxfer_read_cache_object_file() {
				g_zxfer_cache_object_payload_result="ready"
				g_zxfer_cache_object_metadata_result="manifest=manifest.tsv
entries=1
side=source"
				return 0
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	manifest_output=$(
		(
			zxfer_validate_snapshot_record_index_manifest_file() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	object_dir_output=$(
		(
			zxfer_validate_snapshot_record_index_object_dir() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	publish_output=$(
		(
			zxfer_publish_cache_object_directory() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set_state_output=$(
		(
			zxfer_set_snapshot_record_index_state_for_side() {
				return 1
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertContains "File-backed snapshot-index builds should fail closed when the stage directory cannot be allocated." \
		"$stage_dir_output" "status=1"
	assertContains "File-backed snapshot-index builds should preserve raw-record readback failures exactly." \
		"$readback_output" "status=64"
	assertContains "File-backed snapshot-index builds should fail closed when per-dataset record objects cannot be written." \
		"$record_object_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when staged record objects cannot be re-read after writing." \
		"$record_read_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when metadata objects cannot be written after staging." \
		"$meta_write_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when the staged manifest cannot be validated." \
		"$manifest_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when the staged object directory does not validate." \
		"$object_dir_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when the published directory cannot be installed." \
		"$publish_output" "status=1"
	assertContains "File-backed snapshot-index builds should fail closed when the new file-backed index state cannot be recorded after publish." \
		"$set_state_output" "status=1"
}

test_zxfer_build_snapshot_record_index_from_file_cleans_up_published_dirs_for_publish_and_state_failures_in_current_shell() {
	records_file="$TEST_TMPDIR/current_shell_file_publish_records.raw"
	index_root="$TEST_TMPDIR/current_shell_file_publish_index"
	stage_dir="$TEST_TMPDIR/current_shell_file_publish.stage.publish-token"
	state_stage_dir="$TEST_TMPDIR/current_shell_file_state.stage.state-token"
	published_dir="$index_root/source.state-token.obj"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	mkdir -p "$index_root" || fail "Unable to create the file-backed publish snapshot-index root."

	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$stage_dir
		command mkdir -p "$stage_dir"
		return 0
	}
	zxfer_publish_cache_object_directory() {
		return 1
	}
	g_zxfer_snapshot_index_dir="$index_root"
	set +e
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	publish_status=$?
	set -e
	publish_exists=$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)

	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	mkdir -p "$index_root" || fail "Unable to recreate the file-backed publish snapshot-index root."
	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$state_stage_dir
		command mkdir -p "$state_stage_dir"
		return 0
	}
	zxfer_publish_cache_object_directory() {
		command mv "$1" "$2"
	}
	zxfer_set_snapshot_record_index_state_for_side() {
		return 1
	}
	g_zxfer_snapshot_index_dir="$index_root"
	set +e
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	state_status=$?
	set -e
	state_exists=$([ -e "$published_dir" ] && printf '%s' yes || printf '%s' no)

	assertEquals "Current-shell file-backed snapshot-index builds should fail closed when the published directory cannot be installed." \
		1 "$publish_status"
	assertEquals "Current-shell file-backed snapshot-index builds should clean up the staged directory when publish fails." \
		"no" "$publish_exists"
	assertEquals "Current-shell file-backed snapshot-index builds should fail closed when the published index state cannot be recorded." \
		1 "$state_status"
	assertEquals "Current-shell file-backed snapshot-index builds should remove the published generation directory when state recording fails." \
		"no" "$state_exists"

	unset g_cmd_awk
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	setUp
}

test_zxfer_build_snapshot_record_index_from_file_preserves_empty_stage_map_and_meta_write_failures_in_current_shell() {
	records_file="$TEST_TMPDIR/current-shell-file-backed-records.raw"
	empty_awk="$TEST_TMPDIR/mock-empty-file-stage-map-awk.sh"
	stage_dir="$TEST_TMPDIR/current-shell-file-empty-stage"
	meta_stage_dir="$TEST_TMPDIR/current-shell-file-meta-stage"
	index_root="$TEST_TMPDIR/current-shell-file-index"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	mkdir -p "$index_root" || fail "Unable to create the file-backed snapshot-index root."
	cat >"$empty_awk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod 700 "$empty_awk" || fail "Unable to publish the empty file-backed stage-map awk fixture."

	zxfer_ensure_snapshot_index_dir() {
		return 0
	}
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$stage_dir
		command mkdir -p "$stage_dir"
		return 0
	}
	g_cmd_awk="$empty_awk"
	g_zxfer_snapshot_index_dir="$index_root"
	set +e
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	empty_status=$?
	set -e

	assertEquals "Current-shell file-backed snapshot-index builds should fail closed when the staged map is empty." \
		1 "$empty_status"
	assertEquals "Current-shell file-backed snapshot-index builds should clean up the stage directory when the staged map is empty." \
		"no" "$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)"

	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$meta_stage_dir
		command mkdir -p "$meta_stage_dir"
		return 0
	}
	zxfer_write_cache_object_contents_to_path() {
		l_path=$1
		case "$l_path" in
		*/meta)
			return 1
			;;
		esac
		command mkdir -p "$(dirname "$l_path")" || return 1
		printf '%s\n' "mock cache object" >"$l_path" || return 1
		return 0
	}
	zxfer_read_cache_object_file() {
		g_zxfer_cache_object_payload_result="ready"
		g_zxfer_cache_object_metadata_result="manifest=manifest.tsv
entries=1
side=source"
		return 0
	}
	zxfer_validate_snapshot_record_index_manifest_file() {
		printf '%s\n' "tank/src	$meta_stage_dir/records/1.records"
		return 0
	}
	g_cmd_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	set +e
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	meta_status=$?
	set -e

	assertEquals "Current-shell file-backed snapshot-index builds should fail closed when metadata objects cannot be written after staging." \
		1 "$meta_status"
	assertEquals "Current-shell file-backed snapshot-index builds should clean up the stage directory when metadata writes fail." \
		"no" "$([ -e "$meta_stage_dir" ] && printf '%s' yes || printf '%s' no)"
}

test_zxfer_build_snapshot_record_index_from_file_cleans_up_stage_dir_when_records_dir_creation_fails() {
	records_file="$TEST_TMPDIR/source_snapshot_records_records_dir_failure.raw"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"

	output=$(
		(
			stage_dir="$TEST_TMPDIR/build_snapshot_record_index_from_file_records_dir_failure"
			zxfer_ensure_snapshot_index_dir() {
				return 0
			}
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result=$stage_dir
				command mkdir -p "$stage_dir"
				return 0
			}
			mkdir() {
				return 1
			}
			set +e
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)"
		)
	)

	assertContains "File-backed snapshot-index builds should fail closed when the staged records directory cannot be created." \
		"$output" "status=1"
	assertContains "File-backed snapshot-index builds should clean up the stage directory when staged records-directory creation fails." \
		"$output" "exists=no"
}

test_zxfer_build_snapshot_record_index_helpers_cover_awk_and_manifest_write_failures_in_current_shell() {
	records=$(printf '%s\n' "tank/src@snap1	111")
	records_file="$TEST_TMPDIR/source_snapshot_records_manifest_failure.raw"
	failing_awk="$TEST_TMPDIR/failing_snapshot_index_awk.sh"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	cat >"$failing_awk" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod 700 "$failing_awk" || fail "Unable to publish the failing snapshot-index awk fixture."

	set +e
	manifest_status_probe_dir="$TEST_TMPDIR/snapshot_index_manifest_status_probe"
	command mkdir -p "$manifest_status_probe_dir/manifest.tsv"
	(: >"$manifest_status_probe_dir/manifest.tsv") >/dev/null 2>&1
	expected_manifest_write_status=$?
	rm -rf "$manifest_status_probe_dir"

	g_cmd_awk="$failing_awk"
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	build_awk_status=$?
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	build_file_awk_status=$?
	g_cmd_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)

	stage_dir="$TEST_TMPDIR/direct_snapshot_index_manifest_failure"
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$stage_dir
		command mkdir -p "$stage_dir/records" "$stage_dir/manifest.tsv"
		return 0
	}
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	manifest_status=$?
	manifest_exists=$([ -e "$stage_dir" ] && printf yes || printf no)
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"

	stage_dir="$TEST_TMPDIR/file_snapshot_index_manifest_failure"
	zxfer_create_cache_object_stage_dir_in_parent() {
		g_zxfer_runtime_artifact_path_result=$stage_dir
		command mkdir -p "$stage_dir/records" "$stage_dir/manifest.tsv"
		return 0
	}
	zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
	file_manifest_status=$?
	file_manifest_exists=$([ -e "$stage_dir" ] && printf yes || printf no)
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"
	set -e

	assertEquals "Snapshot-index builds should fail closed when the staged-map awk command fails in the current shell." \
		1 "$build_awk_status"
	assertEquals "File-backed snapshot-index builds should fail closed when the staged-map awk command fails in the current shell." \
		1 "$build_file_awk_status"
	assertNotEquals "Directory-backed manifest redirection should fail in the active test shell." \
		0 "$expected_manifest_write_status"
	assertEquals "Snapshot-index builds should fail closed when the staged manifest path cannot be opened for writing in the current shell." \
		"$expected_manifest_write_status" "$manifest_status"
	assertEquals "Snapshot-index builds should clean up the stage directory when staged manifest writes fail in the current shell." \
		"no" "$manifest_exists"
	assertEquals "File-backed snapshot-index builds should fail closed when the staged manifest path cannot be opened for writing in the current shell." \
		"$expected_manifest_write_status" "$file_manifest_status"
	assertEquals "File-backed snapshot-index builds should clean up the stage directory when staged manifest writes fail in the current shell." \
		"no" "$file_manifest_exists"
}

test_zxfer_build_snapshot_record_index_preserves_stage_entry_count_failures() {
	records=$(printf '%s\n' "tank/src@snap1	111")
	raw_record="$TEST_TMPDIR/inline_snapshot_index_stage_count.raw"
	stage_dir="$TEST_TMPDIR/inline_snapshot_index_stage_count.stage"
	count_file="$TEST_TMPDIR/inline_snapshot_index_stage_count.count"
	mock_awk="$TEST_TMPDIR/mock_inline_snapshot_index_stage_count_awk.sh"
	printf '%s\n' "tank/src@snap1	111" >"$raw_record"
	cat >"$mock_awk" <<'EOF'
#!/bin/sh
count=$(cat "$ZXFER_MOCK_AWK_COUNT_FILE" 2>/dev/null || printf '0')
count=$((count + 1))
printf '%s\n' "$count" >"$ZXFER_MOCK_AWK_COUNT_FILE"
if [ "$count" -eq 1 ]; then
	printf 'tank/src\trecords/1.records\t%s\n' "$ZXFER_MOCK_RAW_RECORD_PATH"
	exit 0
fi
exit 1
EOF
	chmod 700 "$mock_awk" || fail "Unable to publish the inline stage-count awk fixture."

	output=$(
		(
			ZXFER_MOCK_AWK_COUNT_FILE="$count_file"
			ZXFER_MOCK_RAW_RECORD_PATH="$raw_record"
			export ZXFER_MOCK_AWK_COUNT_FILE ZXFER_MOCK_RAW_RECORD_PATH
			g_cmd_awk="$mock_awk"
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result="$stage_dir"
				command mkdir -p "$stage_dir"
				return 0
			}
			set +e
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			status=$?
			set -e
			printf 'status=%s\n' "$status"
			printf 'exists=%s\n' "$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)"
		)
	)

	assertContains "Snapshot-index builds should fail closed when staged entry counting fails after inline map generation succeeds." \
		"$output" "status=1"
	assertContains "Snapshot-index builds should clean up the stage directory when staged entry counting fails." \
		"$output" "exists=no"
}

test_zxfer_build_snapshot_record_index_from_file_rejects_empty_stage_maps_and_preserves_stage_entry_count_failures() {
	records_file="$TEST_TMPDIR/source_snapshot_records_stage_count_failure.raw"
	raw_record="$TEST_TMPDIR/file_snapshot_index_stage_count.raw"
	empty_stage_dir="$TEST_TMPDIR/file_snapshot_index_empty_stage.stage"
	count_stage_dir="$TEST_TMPDIR/file_snapshot_index_stage_count.stage"
	count_file="$TEST_TMPDIR/file_snapshot_index_stage_count.count"
	empty_awk="$TEST_TMPDIR/mock_file_snapshot_index_empty_awk.sh"
	count_awk="$TEST_TMPDIR/mock_file_snapshot_index_stage_count_awk.sh"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	printf '%s\n' "tank/src@snap1	111" >"$raw_record"
	cat >"$empty_awk" <<'EOF'
#!/bin/sh
exit 0
EOF
	cat >"$count_awk" <<'EOF'
#!/bin/sh
count=$(cat "$ZXFER_MOCK_AWK_COUNT_FILE" 2>/dev/null || printf '0')
count=$((count + 1))
printf '%s\n' "$count" >"$ZXFER_MOCK_AWK_COUNT_FILE"
if [ "$count" -eq 1 ]; then
	printf 'tank/src\trecords/1.records\t%s\n' "$ZXFER_MOCK_RAW_RECORD_PATH"
	exit 0
fi
exit 1
EOF
	chmod 700 "$empty_awk" "$count_awk" || fail "Unable to publish the file-backed stage-count awk fixtures."

	output=$(
		(
			g_cmd_awk="$empty_awk"
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result="$empty_stage_dir"
				command mkdir -p "$empty_stage_dir"
				return 0
			}
			set +e
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			empty_status=$?
			set -e
			printf 'empty_status=%s\n' "$empty_status"
			printf 'empty_exists=%s\n' "$([ -e "$empty_stage_dir" ] && printf '%s' yes || printf '%s' no)"

			ZXFER_MOCK_AWK_COUNT_FILE="$count_file"
			ZXFER_MOCK_RAW_RECORD_PATH="$raw_record"
			export ZXFER_MOCK_AWK_COUNT_FILE ZXFER_MOCK_RAW_RECORD_PATH
			g_cmd_awk="$count_awk"
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result="$count_stage_dir"
				command mkdir -p "$count_stage_dir"
				return 0
			}
			set +e
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			count_status=$?
			set -e
			printf 'count_status=%s\n' "$count_status"
			printf 'count_exists=%s\n' "$([ -e "$count_stage_dir" ] && printf '%s' yes || printf '%s' no)"
		)
	)

	assertContains "File-backed snapshot-index builds should fail closed when stage-map generation produces no manifest payload." \
		"$output" "empty_status=1"
	assertContains "File-backed snapshot-index builds should clean up the stage directory when file-backed stage-map generation produces no manifest payload." \
		"$output" "empty_exists=no"
	assertContains "File-backed snapshot-index builds should fail closed when staged entry counting fails after file-backed map generation succeeds." \
		"$output" "count_status=1"
	assertContains "File-backed snapshot-index builds should clean up the stage directory when file-backed staged entry counting fails." \
		"$output" "count_exists=no"
}

test_zxfer_build_snapshot_record_index_from_file_round_trip_replaces_previous_generation() {
	initial_records=$(printf '%s\n' \
		"backup/dst/seed@snap0" \
		"backup/dst/seed@snap1")
	cache_file="$TEST_TMPDIR/destination_snapshot_record_index_from_file.raw"
	cat >"$cache_file" <<'EOF'
backup/dst/late@snap2
backup/dst/late@snap1
backup/dst/other@snap9
EOF

	set +e
	zxfer_build_snapshot_record_index destination "$initial_records" >/dev/null 2>&1
	initial_status=$?
	previous_dir=$g_zxfer_destination_snapshot_record_index_dir

	zxfer_build_snapshot_record_index_from_file destination "$cache_file" >/dev/null 2>&1
	build_status=$?
	output=$(zxfer_get_indexed_snapshot_records_for_dataset destination "backup/dst/late")
	lookup_status=$?
	if [ -d "$g_zxfer_destination_snapshot_record_index_dir" ]; then
		new_dir_exists=1
	else
		new_dir_exists=0
	fi
	if [ -n "$previous_dir" ] && [ -d "$previous_dir" ]; then
		previous_dir_exists=1
	else
		previous_dir_exists=0
	fi
	set -e

	assertEquals "Building the initial destination snapshot index fixture should succeed." \
		0 "$initial_status"
	assertEquals "Building destination snapshot indexes from a staged file should succeed." \
		0 "$build_status"
	assertEquals "Looking up records from a destination snapshot index built from a staged file should succeed." \
		0 "$lookup_status"
	assertEquals "Building destination snapshot indexes from a staged file should preserve the requested dataset records." \
		"backup/dst/late@snap2
backup/dst/late@snap1" "$output"
	assertEquals "Building destination snapshot indexes from a staged file should leave the destination side ready." \
		1 "${g_zxfer_destination_snapshot_record_index_ready:-0}"
	assertEquals "Building destination snapshot indexes from a staged file should publish a readable generation directory." \
		1 "$new_dir_exists"
	assertEquals "Building destination snapshot indexes from a staged file should reap the previous destination generation directory." \
		0 "$previous_dir_exists"
}

test_zxfer_ensure_snapshot_record_index_for_side_reuses_valid_source_and_destination_indexes() {
	source_records=$(printf '%s\n' "tank/src@snap2	222" "tank/src@snap1	111")
	destination_records=$(printf '%s\n' "backup/dst@snap2	222" "backup/dst@snap1	111")

	zxfer_build_snapshot_record_index source "$source_records"
	source_dir_before=$g_zxfer_source_snapshot_record_index_dir
	zxfer_build_snapshot_record_index destination "$destination_records"
	destination_dir_before=$g_zxfer_destination_snapshot_record_index_dir

	zxfer_ensure_snapshot_record_index_for_side source >/dev/null
	source_status=$?
	zxfer_ensure_snapshot_record_index_for_side destination >/dev/null
	destination_status=$?

	assertEquals "Source snapshot-index preparation should return early when an existing source index already validates." \
		0 "$source_status"
	assertEquals "Destination snapshot-index preparation should return early when an existing destination index already validates." \
		0 "$destination_status"
	assertEquals "Reused source snapshot indexes should keep the published generation directory unchanged." \
		"$source_dir_before" "$g_zxfer_source_snapshot_record_index_dir"
	assertEquals "Reused destination snapshot indexes should keep the published generation directory unchanged." \
		"$destination_dir_before" "$g_zxfer_destination_snapshot_record_index_dir"
}

test_zxfer_ensure_snapshot_record_index_for_side_source_prefers_file_backed_cache_when_present() {
	cache_file="$TEST_TMPDIR/source_snapshot_record_cache_for_index.raw"
	printf '%s\n' "tank/src@snap1" >"$cache_file"

	output=$(
		(
			g_zxfer_source_snapshot_record_cache_file=$cache_file
			zxfer_build_snapshot_record_index_from_file() {
				printf 'side=%s\n' "$1"
				printf 'file=%s\n' "$2"
				return 48
			}
			set +e
			zxfer_ensure_snapshot_record_index_for_side source
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Source snapshot-index preparation should use the staged source cache file when it is already available." \
		"$output" "side=source"
	assertContains "Source snapshot-index preparation should pass the staged source cache path through to the file-backed builder." \
		"$output" "file=$cache_file"
	assertContains "Source snapshot-index preparation should preserve file-backed builder failures." \
		"$output" "status=48"
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

test_zxfer_get_indexed_snapshot_records_for_dataset_clears_ready_state_when_manifest_is_invalid() {
	records=$(printf '%s\n' \
		"tank/src@b	222" \
		"tank/src@a	111")
	output_file="$TEST_TMPDIR/invalid_snapshot_index_manifest.out"

	zxfer_build_snapshot_record_index source "$records"
	printf '%s\n' "tank/src	../escape.records" >"$g_zxfer_source_snapshot_record_index_dir/manifest.tsv"

	set +e
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/src" >"$output_file"
	status=$?
	set -e
	output=$(cat "$output_file")

	assertEquals "Invalid snapshot-index manifests should cause indexed lookups to fail." \
		1 "$status"
	assertEquals "Invalid snapshot-index manifests should not produce a payload." \
		"" "$output"
	assertEquals "Invalid snapshot-index manifests should clear the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Invalid snapshot-index manifests should clear the remembered source generation path." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
}

test_zxfer_get_indexed_snapshot_records_for_dataset_clears_ready_state_when_manifest_is_unreadable() {
	records=$(printf '%s\n' \
		"tank/src@b	222" \
		"tank/src@a	111")
	output_file="$TEST_TMPDIR/unreadable_snapshot_index_manifest.out"
	mock_bin="$TEST_TMPDIR/mock-bin-snapshot-index-unreadable"
	previous_path=$PATH

	zxfer_build_snapshot_record_index source "$records"
	mkdir -p "$mock_bin" || fail "Unable to create a mock command directory for snapshot-index readback failures."
	cat <<'EOF' >"$mock_bin/cat"
#!/bin/sh
exit 1
EOF
	chmod 700 "$mock_bin/cat" || fail "Unable to publish the mock cat helper for snapshot-index readback failures."

	set +e
	PATH="$mock_bin:$PATH"
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/src" >"$output_file"
	status=$?
	PATH=$previous_path
	set -e

	assertEquals "Unreadable snapshot-index manifests should cause indexed lookups to fail." \
		1 "$status"
	assertEquals "Unreadable snapshot-index manifests should clear the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Unreadable snapshot-index manifests should clear the remembered source generation path." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
	assertEquals "Unreadable snapshot-index manifests should not produce a payload." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_snapshot_records_for_dataset_rebuilds_after_missing_record_object() {
	g_lzfs_list_hr_snap=$(printf '%s\n' \
		"tank/src@snap1" \
		"tank/src@snap2")
	first_output_file="$TEST_TMPDIR/snapshot_records_first.out"
	second_output_file="$TEST_TMPDIR/snapshot_records_second.out"

	zxfer_get_snapshot_records_for_dataset source "tank/src" >"$first_output_file"
	first_dir=$g_zxfer_source_snapshot_record_index_dir
	rm -f "$g_zxfer_source_snapshot_record_index_dir/records/1.records"

	zxfer_get_snapshot_records_for_dataset source "tank/src" >"$second_output_file"
	first_output=$(cat "$first_output_file")
	second_output=$(cat "$second_output_file")

	assertEquals "Initial indexed source lookups should still return the expected records." \
		"tank/src@snap2
tank/src@snap1" "$first_output"
	assertEquals "Missing record-object files should trigger a rebuild and still return the requested records." \
		"tank/src@snap2
tank/src@snap1" "$second_output"
	assertEquals "Rebuilt source indexes should still leave the side marked ready." \
		1 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertNotEquals "Rebuilding after a missing record object should publish a fresh generation directory." \
		"$first_dir" "$g_zxfer_source_snapshot_record_index_dir"
}

test_zxfer_get_indexed_snapshot_records_for_dataset_clears_ready_state_when_record_object_read_fails() {
	records=$(printf '%s\n' \
		"tank/src@b	222" \
		"tank/src@a	111")
	mock_bin="$TEST_TMPDIR/mock_record_object_read_cat"
	output_file="$TEST_TMPDIR/indexed_snapshot_record_read_failure.out"
	cat_bin=$(command -v cat)

	zxfer_build_snapshot_record_index source "$records"
	mkdir -p "$mock_bin" || fail "Unable to create the record-object read failure helper directory."
	cat >"$mock_bin/cat" <<EOF
#!/bin/sh
case "\$1" in
*/records/*.records)
	exit 1
	;;
esac
exec "$cat_bin" "\$@"
EOF
	chmod 700 "$mock_bin/cat" || fail "Unable to publish the record-object read failure helper."

	previous_path=$PATH
	set +e
	PATH="$mock_bin:$PATH"
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/src" >"$output_file"
	status=$?
	PATH=$previous_path
	set -e

	assertEquals "Unreadable staged record objects should cause indexed lookups to fail." \
		1 "$status"
	assertEquals "Unreadable staged record objects should clear the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Unreadable staged record objects should clear the remembered source generation directory." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
	assertEquals "Unreadable staged record objects should not produce a payload." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_snapshot_records_for_dataset_preserves_source_cache_failures() {
	output_file="$TEST_TMPDIR/source_snapshot_record_cache_failure.out"

	set +e
	(
		zxfer_get_indexed_snapshot_records_for_dataset() {
			return 1
		}
		zxfer_ensure_snapshot_record_index_for_side() {
			return 1
		}
		zxfer_ensure_source_snapshot_record_cache() {
			return 73
		}
		zxfer_get_snapshot_records_for_dataset source "tank/src" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Source snapshot-record lookups should preserve source cache rebuild failures." \
		73 "$status"
	assertEquals "Source snapshot-record lookups should not publish a payload when the source cache rebuild fails." \
		"" "$(cat "$output_file")"
}

test_zxfer_ensure_snapshot_record_index_for_side_preserves_source_cache_failures() {
	set +e
	(
		zxfer_ensure_source_snapshot_record_cache() {
			return 31
		}
		zxfer_ensure_snapshot_record_index_for_side source
	) >/dev/null 2>&1
	status=$?
	set -e

	assertEquals "Source snapshot-index preparation should preserve source cache failures." \
		31 "$status"
}

test_zxfer_build_snapshot_record_index_keeps_previous_generation_when_replacement_fails() {
	records=$(printf '%s\n' \
		"tank/src@b	222" \
		"tank/src@a	111")

	zxfer_build_snapshot_record_index source "$records"
	previous_dir=$g_zxfer_source_snapshot_record_index_dir
	previous_output=$(zxfer_get_indexed_snapshot_records_for_dataset source "tank/src")
	previous_awk=$g_cmd_awk
	g_cmd_awk=false

	set +e
	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
	status=$?
	set -e
	g_cmd_awk=$previous_awk

	assertEquals "Failed replacement snapshot-index builds should return failure." \
		1 "$status"
	assertEquals "Failed replacement snapshot-index builds should keep the prior generation path." \
		"$previous_dir" "$g_zxfer_source_snapshot_record_index_dir"
	assertEquals "Failed replacement snapshot-index builds should keep the source side ready." \
		1 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Failed replacement snapshot-index builds should not permanently disable later rebuild attempts." \
		0 "${g_zxfer_snapshot_index_unavailable:-0}"
	assertEquals "The previous snapshot-index generation should remain readable after a failed replacement build." \
		"$previous_output" "$(zxfer_get_indexed_snapshot_records_for_dataset source "tank/src")"

	zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1 ||
		fail "A later snapshot-index rebuild should still be allowed after a transient replacement failure."
	assertNotEquals "A later successful snapshot-index rebuild should publish a fresh generation directory." \
		"$previous_dir" "$g_zxfer_source_snapshot_record_index_dir"
}

test_zxfer_get_snapshot_records_for_dataset_destination_uses_global_list_without_index() {
	g_rzfs_list_hr_snap=$(printf '%s\n' \
		"backup/dst@keep1" \
		"backup/dst/child@skip" \
		"backup/dst@keep2")

	output=$(zxfer_get_snapshot_records_for_dataset destination "backup/dst")

	assertEquals "Destination snapshot record lookup should fall back to the legacy global list when no index exists." \
		"backup/dst@keep1
backup/dst@keep2" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_source_uses_global_reversed_list_after_index_failures() {
	output=$(
		(
			g_lzfs_list_hr_S_snap=$(printf '%s\n' \
				"tank/src@snap2" \
				"tank/src/child@snapc" \
				"tank/src@snap1")
			zxfer_get_indexed_snapshot_records_for_dataset() {
				return 1
			}
			zxfer_ensure_snapshot_record_index_for_side() {
				return 1
			}
			zxfer_ensure_source_snapshot_record_cache() {
				return 0
			}
			zxfer_get_snapshot_records_for_dataset source "tank/src"
		)
	)

	assertEquals "Source snapshot-record lookups should fall back to the in-memory reversed source list when index paths fail and no file-backed cache exists." \
		"tank/src@snap2
tank/src@snap1" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_destination_falls_back_after_index_failures() {
	output_file="$TEST_TMPDIR/destination_snapshot_record_fallback.out"

	set +e
	(
		g_rzfs_list_hr_snap=$(printf '%s\n' \
			"backup/dst@snap2" \
			"backup/dst/child@snapc" \
			"backup/dst@snap1")
		zxfer_get_indexed_snapshot_records_for_dataset() {
			return 1
		}
		zxfer_ensure_snapshot_record_index_for_side() {
			return 1
		}
		zxfer_get_snapshot_records_for_dataset destination "backup/dst" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Destination snapshot-record lookups should still succeed from the global list when indexed lookups are unavailable." \
		0 "$status"
	assertEquals "Destination snapshot-record fallbacks should keep only exact-dataset records from the global list." \
		"backup/dst@snap2
backup/dst@snap1" "$(cat "$output_file")"
}

test_zxfer_filter_snapshot_record_file_for_dataset_and_source_file_cache_cover_exact_paths() {
	cache_file="$TEST_TMPDIR/source_snapshot_record_cache.raw"
	cat >"$cache_file" <<'EOF'
tank/src/early@snap1
tank/src/late@snap1
tank/src/late@snap2
EOF
	filtered_output=$(zxfer_filter_snapshot_record_file_for_dataset "$cache_file" "tank/src/late")

	set +e
	zxfer_filter_snapshot_record_file_for_dataset "$TEST_TMPDIR/missing_snapshot_record_cache.raw" "tank/src" >/dev/null 2>&1
	missing_status=$?
	set -e

	source_output=$(
		(
			g_zxfer_source_snapshot_record_cache_file="$cache_file"
			g_lzfs_list_hr_snap=$(printf '%s\n' \
				"tank/src/early@snap1" \
				"tank/src/late@snap1")
			zxfer_get_indexed_snapshot_records_for_dataset() {
				return 1
			}
			zxfer_ensure_snapshot_record_index_for_side() {
				return 1
			}
			zxfer_get_snapshot_records_for_dataset source "tank/src/late"
		)
	)

	assertEquals "Snapshot-record file filtering should keep only exact-dataset records from file-backed caches." \
		"tank/src/late@snap1
tank/src/late@snap2" "$filtered_output"
	assertEquals "Snapshot-record file filtering should fail closed when the staged cache file is missing." \
		1 "$missing_status"
	assertEquals "Source snapshot-record lookups should prefer the staged file-backed cache when indexed lookups are unavailable." \
		"tank/src/late@snap1
tank/src/late@snap2" "$source_output"
}

test_zxfer_get_snapshot_records_for_dataset_destination_uses_file_backed_cache_when_index_paths_fail() {
	cache_file="$TEST_TMPDIR/destination_snapshot_record_fallback_cache.raw"
	output_file="$TEST_TMPDIR/destination_snapshot_record_file_fallback.out"
	cat >"$cache_file" <<'EOF'
backup/dst/early@snap1
backup/dst/late@snap1
backup/dst/late@snap2
EOF

	set +e
	(
		g_zxfer_destination_snapshot_record_cache_file=$cache_file
		zxfer_get_indexed_snapshot_records_for_dataset() {
			return 1
		}
		zxfer_ensure_snapshot_record_index_for_side() {
			return 1
		}
		zxfer_get_snapshot_records_for_dataset destination "backup/dst/late" >"$output_file"
	)
	status=$?
	set -e

	assertEquals "Destination snapshot-record lookups should still succeed from the staged file-backed cache when index paths fail." \
		0 "$status"
	assertEquals "Destination snapshot-record file fallbacks should keep only exact-dataset records from the staged cache file." \
		"backup/dst/late@snap1
backup/dst/late@snap2" "$(cat "$output_file")"
}

test_zxfer_get_snapshot_records_for_dataset_destination_prefers_file_backed_cache_over_incomplete_global_list() {
	cache_file="$TEST_TMPDIR/destination_snapshot_record_cache.raw"
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

test_zxfer_get_snapshot_records_for_dataset_returns_failure_for_unknown_side() {
	set +e
	zxfer_get_snapshot_records_for_dataset nonsense "tank/src" >/dev/null 2>&1
	status=$?

	assertEquals "Snapshot record lookup should reject unknown sides." 1 "$status"
}

test_zxfer_get_indexed_snapshot_records_for_dataset_returns_empty_when_dataset_is_absent_and_cache_miss_fails_closed() {
	records=$(printf '%s\n' \
		"tank/src@snap2	222" \
		"tank/src@snap1	111")
	output_file="$TEST_TMPDIR/absent_snapshot_index_lookup.out"

	zxfer_build_snapshot_record_index source "$records"

	set +e
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/missing" >"$output_file"
	index_status=$?
	zxfer_get_destination_existence_cache_entry "backup/missing" >/dev/null 2>&1
	cache_status=$?
	set -e

	assertEquals "Indexed snapshot-record lookups should return success with no payload when the dataset is absent from a valid index." \
		0 "$index_status"
	assertEquals "Indexed snapshot-record lookups should not emit payload when the requested dataset is absent." \
		"" "$(cat "$output_file")"
	assertEquals "Destination existence cache lookups should fail closed when no exact or rooted cache entry matches." \
		1 "$cache_status"
}

test_zxfer_get_indexed_snapshot_records_for_dataset_clears_ready_state_when_record_object_is_unreadable() {
	records=$(printf '%s\n' \
		"tank/src@snap2	222" \
		"tank/src@snap1	111")
	output_file="$TEST_TMPDIR/unreadable_snapshot_index_record.out"

	zxfer_build_snapshot_record_index source "$records"
	printf '%s\n' "not a cache object" >"$g_zxfer_source_snapshot_record_index_dir/records/1.records"

	set +e
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/src" >"$output_file"
	status=$?
	set -e

	assertEquals "Indexed snapshot-record lookups should fail closed when the per-dataset record object cannot be re-read." \
		1 "$status"
	assertEquals "Indexed snapshot-record lookups should not emit payload when the record object is unreadable." \
		"" "$(cat "$output_file")"
	assertEquals "Unreadable indexed record objects should clear the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-0}"
	assertEquals "Unreadable indexed record objects should clear the remembered source generation directory." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
}

test_zxfer_get_indexed_snapshot_records_for_dataset_clears_ready_state_when_record_object_read_fails_in_current_shell() {
	index_dir="$TEST_TMPDIR/current-shell-index-read-failure.obj"
	record_path="$index_dir/records/1.records"
	mkdir -p "$index_dir/records" || fail "Unable to create the current-shell indexed-record read-failure fixture directory."
	g_zxfer_source_snapshot_record_index_dir=$index_dir
	g_zxfer_source_snapshot_record_index=$(printf '%s\t%s\n' "tank/src" "$record_path")
	g_zxfer_source_snapshot_record_index_ready=1

	zxfer_validate_snapshot_record_index_object_dir() {
		printf '%s\t%s\n' "tank/src" "$record_path"
	}
	zxfer_read_cache_object_file() {
		return 1
	}

	set +e
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/src" >/dev/null 2>&1
	status=$?
	set -e
	ready_state=${g_zxfer_source_snapshot_record_index_ready:-0}
	index_dir_state=${g_zxfer_source_snapshot_record_index_dir:-}
	zxfer_source_runtime_modules_through "zxfer_snapshot_state.sh"

	assertEquals "Current-shell indexed snapshot-record lookups should fail closed when staged record-object reads fail." \
		1 "$status"
	assertEquals "Current-shell indexed snapshot-record read failures should clear the ready flag." \
		0 "$ready_state"
	assertEquals "Current-shell indexed snapshot-record read failures should clear the remembered generation directory." \
		"" "$index_dir_state"
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
			zxfer_get_temp_file() {
				return 61
			}
			zxfer_read_normalized_snapshot_record_list "tank/src@snap1 tank/src@snap2" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	reversed_output=$(
		(
			zxfer_get_temp_file() {
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

test_zxfer_snapshot_record_read_helpers_preserve_readback_and_reverse_stage_failures() {
	set +e
	normalized_read_output=$(
		(
			normalized_tmp_file="$TEST_TMPDIR/normalized-readback-failure.records"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$normalized_tmp_file
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
			reversed_stage_tmp_file="$TEST_TMPDIR/reversed-stage-failure.records"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$reversed_stage_tmp_file
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
			reversed_read_tmp_file="$TEST_TMPDIR/reversed-readback-failure.records"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$reversed_read_tmp_file
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

test_zxfer_snapshot_index_inline_cleanup_branches_cover_current_shell_paths() {
	records=$(printf '%s\n' "tank/src@snap1	111")
	branch_root="$TEST_TMPDIR/snapshot_index_inline_cleanup_branches"
	relpath_stage_dir="$branch_root/missing_relpath.stage.relpath-token"
	raw_stage_dir="$branch_root/missing_raw.stage.raw-token"
	read_stage_dir="$branch_root/read_failure.stage.read-token"
	file_relpath_stage_dir="$branch_root/file_missing_relpath.stage.file-relpath-token"
	malformed_awk="$branch_root/malformed-inline-awk.sh"
	raw_record="$branch_root/raw.records"
	records_file="$branch_root/source.records"
	mkdir -p "$branch_root" || fail "Unable to create the snapshot-index branch coverage root."
	printf '%s\n' "tank/src@snap1	111" >"$raw_record"
	printf '%s\n' "tank/src@snap1	111" >"$records_file"
	cat >"$malformed_awk" <<'EOF'
#!/bin/sh
if [ "${ZXFER_MOCK_STAGE_ROW_MODE:-}" = "missing-relpath" ]; then
	printf 'tank/src\t\t%s\n' "$ZXFER_MOCK_RAW_RECORD_PATH"
elif [ "${ZXFER_MOCK_STAGE_ROW_MODE:-}" = "missing-raw" ]; then
	printf 'tank/src\trecords/1.records\t\n'
else
	printf 'tank/src\trecords/1.records\t%s\n' "$ZXFER_MOCK_RAW_RECORD_PATH"
fi
EOF
	chmod 700 "$malformed_awk" || fail "Unable to publish the inline cleanup branch awk fixture."

	output=$(
		(
			set +e
			g_zxfer_snapshot_index_dir="$branch_root/index"
			g_cmd_awk="$malformed_awk"
			ZXFER_MOCK_STAGE_ROW_MODE=missing-relpath
			ZXFER_MOCK_RAW_RECORD_PATH=$raw_record
			export ZXFER_MOCK_STAGE_ROW_MODE ZXFER_MOCK_RAW_RECORD_PATH
			zxfer_ensure_snapshot_index_dir() {
				mkdir -p "$g_zxfer_snapshot_index_dir"
				return 0
			}
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result=$relpath_stage_dir
				mkdir -p "$relpath_stage_dir"
				return 0
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'missing_relpath_status=%s\n' "$?"
			printf 'missing_relpath_exists=%s\n' "$([ -e "$relpath_stage_dir" ] && printf yes || printf no)"
		)
		(
			set +e
			g_zxfer_snapshot_index_dir="$branch_root/index"
			g_cmd_awk="$malformed_awk"
			ZXFER_MOCK_STAGE_ROW_MODE=missing-raw
			ZXFER_MOCK_RAW_RECORD_PATH=$raw_record
			export ZXFER_MOCK_STAGE_ROW_MODE ZXFER_MOCK_RAW_RECORD_PATH
			zxfer_ensure_snapshot_index_dir() {
				mkdir -p "$g_zxfer_snapshot_index_dir"
				return 0
			}
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result=$raw_stage_dir
				mkdir -p "$raw_stage_dir"
				return 0
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'missing_raw_status=%s\n' "$?"
			printf 'missing_raw_exists=%s\n' "$([ -e "$raw_stage_dir" ] && printf yes || printf no)"
		)
		(
			set +e
			g_zxfer_snapshot_index_dir="$branch_root/index"
			g_cmd_awk="$malformed_awk"
			ZXFER_MOCK_STAGE_ROW_MODE=valid
			ZXFER_MOCK_RAW_RECORD_PATH=$raw_record
			export ZXFER_MOCK_STAGE_ROW_MODE ZXFER_MOCK_RAW_RECORD_PATH
			zxfer_ensure_snapshot_index_dir() {
				mkdir -p "$g_zxfer_snapshot_index_dir"
				return 0
			}
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result=$read_stage_dir
				mkdir -p "$read_stage_dir"
				return 0
			}
			zxfer_write_cache_object_contents_to_path() {
				return 0
			}
			zxfer_read_cache_object_file() {
				return 1
			}
			zxfer_build_snapshot_record_index source "$records" >/dev/null 2>&1
			printf 'record_object_read_status=%s\n' "$?"
			printf 'record_object_read_exists=%s\n' "$([ -e "$read_stage_dir" ] && printf yes || printf no)"
		)
		(
			set +e
			g_zxfer_snapshot_index_dir="$branch_root/index"
			g_cmd_awk="$malformed_awk"
			ZXFER_MOCK_STAGE_ROW_MODE=missing-relpath
			ZXFER_MOCK_RAW_RECORD_PATH=$raw_record
			export ZXFER_MOCK_STAGE_ROW_MODE ZXFER_MOCK_RAW_RECORD_PATH
			zxfer_ensure_snapshot_index_dir() {
				mkdir -p "$g_zxfer_snapshot_index_dir"
				return 0
			}
			zxfer_create_cache_object_stage_dir_in_parent() {
				g_zxfer_runtime_artifact_path_result=$file_relpath_stage_dir
				mkdir -p "$file_relpath_stage_dir"
				return 0
			}
			zxfer_build_snapshot_record_index_from_file source "$records_file" >/dev/null 2>&1
			printf 'file_missing_relpath_status=%s\n' "$?"
			printf 'file_missing_relpath_exists=%s\n' "$([ -e "$file_relpath_stage_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Inline snapshot-index builds should fail closed when the stage map omits the record relpath." \
		"$output" "missing_relpath_status=1"
	assertContains "Inline snapshot-index builds should clean up after a missing record relpath." \
		"$output" "missing_relpath_exists=no"
	assertContains "Inline snapshot-index builds should fail closed when the stage map omits the raw record path." \
		"$output" "missing_raw_status=1"
	assertContains "Inline snapshot-index builds should clean up after a missing raw record path." \
		"$output" "missing_raw_exists=no"
	assertContains "Inline snapshot-index builds should fail closed when a staged record object cannot be reread." \
		"$output" "record_object_read_status=1"
	assertContains "Inline snapshot-index builds should clean up after staged record object readback fails." \
		"$output" "record_object_read_exists=no"
	assertContains "File-backed snapshot-index builds should fail closed when the stage map omits the record relpath." \
		"$output" "file_missing_relpath_status=1"
	assertContains "File-backed snapshot-index builds should clean up after a missing record relpath." \
		"$output" "file_missing_relpath_exists=no"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
