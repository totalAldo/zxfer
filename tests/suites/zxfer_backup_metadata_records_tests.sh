#!/bin/sh
# Backup metadata row buffering, the record-list and file-format checks, and
# the write boundary's validation. The -k/-e behavior around them is pinned
# black-box by the backup_mode_* and restore_mode_* cases of
# tests/test_contract_backup.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Point the module at a private backup root under the physical temp dir and
# publish the exact-pair and forwarded file paths for tank/src -> backup/dst.
zxfer_backup_test_use_private_root() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/$1"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src"
	g_option_N_nonrecursive=""
	BACKUP_TEST_PRIMARY_FILE="$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src backup/dst)"
	BACKUP_TEST_FORWARDED_FILE="$g_backup_storage_root/backup/dst/src/$(zxfer_get_backup_metadata_filename backup/dst/src backup/dst/src)"
}

# Write one metadata file (0600 inside 0700 directories) with the given
# contents so a restore lookup can read it back.
zxfer_backup_test_write_file() {
	l_backup_test_path=$1
	l_backup_test_contents=$2

	(umask 077 && mkdir -p "${l_backup_test_path%/*}") || fail "Unable to create $l_backup_test_path parent."
	printf '%s\n' "$l_backup_test_contents" >"$l_backup_test_path"
	chmod 600 "$l_backup_test_path"
}

test_append_backup_metadata_record_keys_literal_rows_relative_to_the_source_root() {
	g_backup_file_contents=""
	zxfer_append_backup_metadata_record "tank/src" "user:note=a\\nb=local"
	zxfer_append_backup_metadata_record "tank/src/child" "user:path=C:\\temp=local"

	assertEquals "Rows are keyed by the source-root-relative path (a dot for the root) and appended without interpreting backslashes." \
		"$(zxfer_test_backup_metadata_row "." "user:note=a\\nb=local")
$(zxfer_test_backup_metadata_row "child" "user:path=C:\\temp=local")" "$g_backup_file_contents"

	output=$(
		(
			zxfer_append_backup_metadata_record "other/pool" "compression=lz4=local"
		) 2>&1
	)
	assertEquals "Datasets outside the source root must be refused." 1 "$?"
	assertContains "$output" "Backup metadata source dataset [other/pool] is outside source root [tank/src]."
}

test_validate_backup_metadata_record_list_keeps_the_newest_row_per_key_and_rejects_malformed_rows() {
	buffered="$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")
$(zxfer_test_backup_metadata_row "." "compression=off=local")"

	assertEquals "Duplicate keys collapse to the newest row while keeping first-appearance order." \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")" \
		"$(zxfer_validate_backup_metadata_record_list "$buffered")"

	for malformed in "no-tab-row" "$(zxfer_test_backup_metadata_row "." "")" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local,")" \
		"$(zxfer_test_backup_metadata_row "" "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression")" \
		"$(zxfer_test_backup_metadata_row "." "=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=")"; do
		output=$(zxfer_validate_backup_metadata_record_list "$malformed" 2>&1)
		assertEquals "Malformed row [$malformed] must fail validation." 1 "$?"
		assertEquals "A failed validation prints no partial list." "" "$output"
	done
}

test_backup_metadata_extract_properties_for_dataset_pair_validates_the_file_and_resolves_relative_rows() {
	header=$ZXFER_BACKUP_METADATA_HEADER_LINE
	roots="#source_root:tank/src
#destination_root:backup/dst"
	row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	file="$header
#format_version:2
$roots
$row
$(zxfer_test_backup_metadata_row "child" "atime=off=local")"
	# expected status and output|source|destination|contents
	for case_spec in \
		"0 compression=lz4=local|tank/src|backup/dst|$file" \
		"0 atime=off=local|tank/src/child|backup/dst/child|$file" \
		"0 compression=lz4=local|tank/src|backup/dst|$header
#version:x
$roots
#format_version:2
$row" \
		"3 |tank/src|backup/other|$file" \
		"3 |tank/src/child|backup/dst/other|$file" \
		"8 |tank/src/missing|backup/dst/missing|$file" \
		"9 |tank/src|backup/dst|$file
$(zxfer_test_backup_metadata_row "." "compression=off=local")" \
		"4 |tank/src|backup/dst|$file
tank/src,backup/dst,legacy=row" \
		"4 |tank/src|backup/dst|$file
$(zxfer_test_backup_metadata_row "/abs" "compression=lz4=local")" \
		"4 |tank/src|backup/dst|$file
$(zxfer_test_backup_metadata_row "child/" "compression=lz4=local")" \
		"4 |tank/src|backup/dst|$header
#format_version:2
$row" \
		"6 |tank/src|backup/dst|" \
		"6 |tank/src|backup/dst|$row" \
		"6 |tank/src|backup/dst|
$file" \
		"6 |tank/src|backup/dst|$header
$roots
$row
#format_version:2" \
		"6 |tank/src|backup/dst|$file
$header" \
		"6 |tank/src|backup/dst|$header
#format_version:2
$roots
broken-row
$header" \
		"7 |tank/src|backup/dst|$header" \
		"7 |tank/src|backup/dst|$header
$roots" \
		"7 |tank/src|backup/dst|$header
#format_version:1
$roots
$row" \
		"7 |tank/src|backup/dst|$header
#format_version:2
#format_version:2
$roots
$row" \
		"7 |tank/src|backup/dst|$header
#format_version:2
$roots
broken-row
#format_version:3"; do
		expected=${case_spec%%|*}
		rest=${case_spec#*|}
		pair_source=${rest%%|*}
		rest=${rest#*|}
		pair_destination=${rest%%|*}
		contents=${rest#*|}
		output=$(zxfer_backup_metadata_extract_properties_for_dataset_pair "$contents" \
			"$pair_source" "$pair_destination")
		assertEquals "Contents [$contents] for $pair_source -> $pair_destination." \
			"$expected" "$? $output"
	done
}

test_write_backup_properties_rejects_malformed_rows_before_touching_the_store() {
	zxfer_backup_test_use_private_root write_malformed
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents="broken-row"

	output=$(
		(
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Failed to validate buffered backup metadata records"
	assertFalse "No directory or file may be created for malformed rows." "[ -e '$g_backup_storage_root' ]"
}
