#!/bin/sh
# -e row lookup, candidate selection and failure messages, and the local read
# protections. The -e runs themselves are pinned black-box by the
# restore_mode_* cases of tests/test_contract_backup.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Render current-format contents for the tank/src -> backup/dst/src pair.
zxfer_backup_test_exact_pair_contents() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst/src"
	zxfer_test_render_current_backup_metadata_contents "$@"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
}

test_find_restored_backup_properties_returns_lookup_statuses() {
	output=$(
		(
			printf 'none=%s\n' "$(
				zxfer_find_restored_backup_properties tank/src backup/dst/src
				printf '%s' "$?"
			)"
			zxfer_test_load_backup_restore_rows tank/src backup/dst/src \
				"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
				"$(zxfer_test_backup_metadata_row "a b/[c]*" "atime=off=local")" \
				"$(zxfer_test_backup_metadata_row "dup" "a=1=local")" \
				"$(zxfer_test_backup_metadata_row "dup" "a=2=local")" \
				"$(zxfer_test_backup_metadata_row "lost" "a=1=local")"
			printf 'load=%s\n' "$?"
			# The row file of "lost" disappears after the load.
			zxfer_find_property_row "$g_zxfer_backup_restore_index" lost
			rm -f "$g_zxfer_property_row_dir/$g_zxfer_property_row_result"
			for l_pair in \
				"tank/src|backup/dst/src" \
				"tank/src/a b/[c]*|backup/dst/src/a b/[c]*" \
				"tank/src/a b/xc|backup/dst/src/a b/xc" \
				"tank/src/dup|backup/dst/src/dup" \
				"tank/src/lost|backup/dst/src/lost" \
				"tank/src/child|backup/dst/src/other" \
				"tank/other|backup/dst/src" \
				"tank/src|backup/dst" \
				"tank/src/|backup/dst/src/"; do
				zxfer_find_restored_backup_properties "${l_pair%|*}" "${l_pair#*|}"
				printf '%s %s <%s>\n' "$l_pair" "$?" "$g_zxfer_backup_restore_properties_result"
			done
		) 2>&1
	)
	assertEquals "none=3
load=0
tank/src|backup/dst/src 0 <compression=lz4=local>
tank/src/a b/[c]*|backup/dst/src/a b/[c]* 0 <atime=off=local>
tank/src/a b/xc|backup/dst/src/a b/xc 8 <>
tank/src/dup|backup/dst/src/dup 9 <>
tank/src/lost|backup/dst/src/lost 5 <>
tank/src/child|backup/dst/src/other 3 <>
tank/other|backup/dst/src 3 <>
tank/src|backup/dst 3 <>
tank/src/|backup/dst/src/ 3 <>" "$output"
}

test_get_backup_properties_reports_a_failed_row_load_as_a_read_failure() {
	zxfer_backup_test_use_private_root restore_load_failure
	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")"
	output=$(
		(
			zxfer_load_backup_restore_rows() { return 1; }
			zxfer_get_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Failed to read backup property file $BACKUP_TEST_PRIMARY_FILE."
}

test_try_backup_restore_candidate_maps_read_format_and_row_statuses() {
	zxfer_backup_test_use_private_root candidate_statuses
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst/src"
	valid=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
	legacy_file="$g_backup_storage_root/tank/src/.zxfer_backup_info.src.k1537737481.19"

	# current:legacy read statuses -> candidate status; the last case leaves
	# the legacy path as the one examined last.
	for case_spec in "4:4|1" "1:0|5" "0:0|0" "4:1|5"; do
		statuses=${case_spec%%|*}
		expected=${case_spec#*|}
		current_status=${statuses%%:*}
		legacy_status=${statuses#*:}
		result=$(
			(
				zxfer_read_local_backup_file() {
					g_zxfer_backup_file_read_result=$valid
					# No case statement here: bash 3.2 (macOS /bin/sh)
					# mis-parses case patterns inside command substitution.
					if [ "$1" = "$BACKUP_TEST_PRIMARY_FILE" ]; then
						return "$current_status"
					fi
					return "$legacy_status"
				}
				zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
					tank/src backup/dst tank/src backup/dst/src
				printf '%s %s\n' "$?" "$g_zxfer_backup_restore_candidate_path_result"
			)
		)
		assertEquals "Read statuses current=$current_status legacy=$legacy_status map to $expected." \
			"$expected" "${result%% *}"
	done
	assertEquals "The path examined last is reported when the legacy fallback is the one read." \
		"$legacy_file" "${result#* }"

	result=$(
		(
			zxfer_read_local_backup_file() {
				g_zxfer_backup_file_read_result=$valid
				return 0
			}
			zxfer_backup_metadata_extract_properties_for_dataset_pair() { return 99; }
			zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
				tank/src backup/dst tank/src backup/dst/src
			printf '%s\n' "$?"
		)
	)
	assertEquals "Unexpected row statuses fail closed as read failures." 5 "$result"

	# A valid file without a row for the pair (8) keeps its contents for the
	# forwarded-alias loader; a pair outside its roots stays 3.
	for case_spec in "8 contents|tank/src/other|backup/dst/src/other" "3 |tank/src|backup/other"; do
		expected=${case_spec%%|*}
		pair=${case_spec#*|}
		result=$(
			(
				zxfer_read_local_backup_file() {
					g_zxfer_backup_file_read_result=$valid
					return 0
				}
				zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
					tank/src backup/dst "${pair%|*}" "${pair#*|}"
				printf '%s %s\n' "$?" "${g_zxfer_backup_restore_candidate_contents_result:+contents}"
			)
		)
		assertEquals "Pair ${pair%|*} -> ${pair#*|} maps to status and contents [$expected]." \
			"$expected" "$result"
	done

	result=$(
		(
			g_cmd_awk=false
			zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
				tank/src backup/dst tank/src backup/dst/src
			printf '%s\n' "$?"
		)
	)
	assertEquals "An underivable filename is reported distinctly." 11 "$result"
}

test_throw_backup_candidate_failure_routes_usage_and_plain_errors() {
	for case_spec in \
		"1|usage|1|Cannot find backup property file." \
		"9|usage|1|Backup property file /p contains multiple relative rows for source dataset tank/src." \
		"3|usage|1|Backup property file /p does not contain a current-format relative row for source dataset tank/src." \
		"8|usage|1|Backup property file /p does not contain a current-format relative row for source dataset tank/src." \
		"4|usage|1|Backup property file /p is malformed." \
		"6|usage|1|Backup property file /p does not start with the required zxfer backup metadata header." \
		"7|usage|1|Backup property file /p does not declare supported zxfer backup metadata format version #format_version:2." \
		"5|usage|1|Failed to read backup property file /p." \
		"11|usage|1|Failed to derive backup metadata filename for source dataset [tank/src]." \
		"7||1|Forwarded backup property file /p does not declare supported zxfer backup metadata format version #format_version:2." \
		"5||1|Failed to read forwarded backup property file /p."; do
		status=${case_spec%%|*}
		rest=${case_spec#*|}
		usage=${rest%%|*}
		rest=${rest#*|}
		expected_status=${rest%%|*}
		expected_message=${rest#*|}
		label="Backup property file"
		[ -n "$usage" ] || label="Forwarded backup property file"
		output=$(
			(
				zxfer_throw_backup_candidate_failure "$status" /p tank/src "$label" "$usage"
			) 2>&1
		)
		assertEquals "Status $status should exit $expected_status." "$expected_status" "$?"
		assertContains "$output" "$expected_message"
	done
}

test_read_local_backup_file_returns_missing_and_refuses_symlinked_components() {
	real_dir="$TEST_TMPDIR_PHYSICAL/read_local_real"
	link_dir="$TEST_TMPDIR_PHYSICAL/read_local_link"
	mkdir -p "$real_dir"
	printf 'trusted\n' >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
	ln -s "$real_dir" "$link_dir"
	ln -s "$real_dir/backup.meta" "$real_dir/backup.link"

	zxfer_read_local_backup_file "$real_dir/missing.meta" >/dev/null
	assertEquals "A missing file is status 4." 4 "$?"

	output=$(zxfer_read_local_backup_file "$link_dir/backup.meta" 2>&1)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $link_dir/backup.meta because path component $link_dir is a symlink."

	output=$(zxfer_read_local_backup_file "$real_dir/backup.link" 2>&1)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $real_dir/backup.link because it is a symlink."
}

test_read_local_backup_file_rejects_insecure_owner_mode_and_shared_directory() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/read_local_secure"
	mkdir -p "$backup_dir"
	chmod 700 "$backup_dir"
	backup_file="$backup_dir/backup.meta"
	printf 'trusted\n' >"$backup_file"
	chmod 600 "$backup_file"

	output=$(
		(
			zxfer_get_path_owner_uid() { printf '4321\n'; }
			zxfer_read_local_backup_file "$backup_file"
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $backup_file because it is owned by UID 4321 instead of"

	chmod 644 "$backup_file"
	output=$(
		(
			zxfer_read_local_backup_file "$backup_file"
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $backup_file because its permissions (644) are not 0600."
	chmod 600 "$backup_file"

	chmod 777 "$backup_dir"
	output=$(
		(
			zxfer_read_local_backup_file "$backup_file"
		) 2>&1
	)
	assertEquals "A directory other users can modify could swap the file between the check and the read." 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $backup_file because its directory $backup_dir is not a private directory owned by root or the current user."
	chmod 700 "$backup_dir"

	assertEquals "A private 0700 directory with a 0600 file reads back its contents." \
		"trusted" "$(zxfer_read_local_backup_file "$backup_file")"
	assertEquals "trusted" "$(
		zxfer_read_local_backup_file "$backup_file" >/dev/null
		printf '%s' "$g_zxfer_backup_file_read_result"
	)"
}

test_read_local_backup_file_returns_cat_failures_after_security_checks() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/read_local_cat"
	mkdir -p "$backup_dir"
	backup_file="$backup_dir/backup.meta"
	printf 'trusted\n' >"$backup_file"
	chmod 600 "$backup_file"

	status=$(
		(
			cat() { return 3; }
			zxfer_read_local_backup_file "$backup_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	assertEquals "The literal cat status is preserved." 3 "$status"
}
