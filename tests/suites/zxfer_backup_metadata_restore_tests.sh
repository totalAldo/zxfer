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
			rm -f "$g_zxfer_backup_restore_row_dir/$g_zxfer_property_row_result"
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
				g_zxfer_property_row_payload_result=stale
				zxfer_find_restored_backup_properties "${l_pair%|*}" "${l_pair#*|}"
				printf '%s %s <%s>\n' "$l_pair" "$?" "$g_zxfer_property_row_payload_result"
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
			zxfer_prepare_property_read_files || exit
			zxfer_create_private_temp_dir() { return 1; }
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
					zxfer_write_runtime_artifact_file "$2" "$valid" || return
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
				zxfer_write_runtime_artifact_file "$2" "$valid" || return
				return 0
			}
			zxfer_load_backup_metadata_view() { return 99; }
			zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
				tank/src backup/dst tank/src backup/dst/src
			printf '%s\n' "$?"
		)
	)
	assertEquals "Unexpected row statuses fail closed as read failures." 5 "$result"

	# A valid alias without a row for the pair (8) keeps its view; a pair
	# outside its roots stays 3.
	for case_spec in "8 view|tank/src/other|backup/dst/src/other" "3 |tank/src|backup/other"; do
		expected=${case_spec%%|*}
		pair=${case_spec#*|}
		result=$(
			(
				zxfer_read_local_backup_file() {
					zxfer_write_runtime_artifact_file "$2" "$valid" || return
					return 0
				}
				zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
					tank/src backup/dst "${pair%|*}" "${pair#*|}" '' source tank/src
				printf '%s %s\n' "$?" "${g_zxfer_backup_view_result:+view}"
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
	zxfer_create_runtime_artifact_file backup-test
	staging=$g_zxfer_runtime_artifact_path_result
	real_dir="$TEST_TMPDIR_PHYSICAL/read_local_real"
	link_dir="$TEST_TMPDIR_PHYSICAL/read_local_link"
	mkdir -p "$real_dir"
	printf 'trusted\n' >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
	ln -s "$real_dir" "$link_dir"
	ln -s "$real_dir/backup.meta" "$real_dir/backup.link"

	zxfer_read_local_backup_file "$real_dir/missing.meta" "$staging" >/dev/null
	assertEquals "A missing file is status 4." 4 "$?"

	output=$(zxfer_read_local_backup_file "$link_dir/backup.meta" "$staging" 2>&1)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $link_dir/backup.meta because path component $link_dir is a symlink."

	output=$(zxfer_read_local_backup_file "$real_dir/backup.link" "$staging" 2>&1)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $real_dir/backup.link because it is a symlink."
}

test_read_local_backup_file_rejects_insecure_owner_mode_and_shared_directory() {
	zxfer_create_runtime_artifact_file backup-test
	staging=$g_zxfer_runtime_artifact_path_result
	backup_dir="$TEST_TMPDIR_PHYSICAL/read_local_secure"
	mkdir -p "$backup_dir"
	chmod 700 "$backup_dir"
	backup_file="$backup_dir/backup.meta"
	printf 'trusted\n' >"$backup_file"
	chmod 600 "$backup_file"

	output=$(
		(
			zxfer_get_path_owner_uid() { printf '4321\n'; }
			zxfer_read_local_backup_file "$backup_file" "$staging"
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $backup_file because it is owned by UID 4321 instead of"

	chmod 644 "$backup_file"
	output=$(
		(
			zxfer_read_local_backup_file "$backup_file" "$staging"
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $backup_file because its permissions (644) are not 0600."
	chmod 600 "$backup_file"

	chmod 777 "$backup_dir"
	output=$(
		(
			zxfer_read_local_backup_file "$backup_file" "$staging"
		) 2>&1
	)
	assertEquals "A directory other users can modify could swap the file between the check and the read." 1 "$?"
	assertContains "$output" "Refusing to use backup metadata $backup_file because its directory $backup_dir is not a private directory owned by root or the current user."
	chmod 700 "$backup_dir"

	zxfer_read_local_backup_file "$backup_file" "$staging"
	status=$?
	assertEquals "A private 0700 directory with a 0600 file reads successfully." 0 "$status"
	assertEquals "trusted" "$(cat "$staging")"

}

test_read_local_backup_file_returns_cat_failures_after_security_checks() {
	zxfer_create_runtime_artifact_file backup-test
	staging=$g_zxfer_runtime_artifact_path_result
	backup_dir="$TEST_TMPDIR_PHYSICAL/read_local_cat"
	mkdir -p "$backup_dir"
	backup_file="$backup_dir/backup.meta"
	printf 'trusted\n' >"$backup_file"
	chmod 600 "$backup_file"

	status=$(
		(
			cat() { return 3; }
			zxfer_read_local_backup_file "$backup_file" "$staging" >/dev/null
			printf '%s\n' "$?"
		)
	)
	assertEquals "The literal cat status is preserved." 3 "$status"
}

test_metadata_view_preserves_format_priority_and_clears_failed_publications() {
	zxfer_prepare_property_read_files || return
	zxfer_create_runtime_artifact_file metadata-test
	staging=$g_zxfer_runtime_artifact_path_result
	valid=$(zxfer_backup_test_exact_pair_contents "$(zxfer_test_backup_metadata_row . compression=lz4=local)")
	for case_spec in 'valid|5' 'bad-version|7' 'bad-header|6' 'missing-row|8'; do
		kind=${case_spec%|*}
		expected=${case_spec#*|}
		contents=$valid
		[ "$kind" != bad-version ] || contents="$ZXFER_BACKUP_METADATA_HEADER_LINE
#format_version:1"
		[ "$kind" != bad-header ] || contents="broken"
		zxfer_write_runtime_artifact_file "$staging" "$contents"
		output=$(
			(
				g_zxfer_backup_view_result=stale
				zxfer_create_private_temp_dir() { return 17; }
				if [ "$kind" = missing-row ]; then
					zxfer_load_backup_metadata_view "$staging" tank/src/missing backup/dst/src/missing
				else
					zxfer_load_backup_metadata_view "$staging" tank/src backup/dst/src
				fi
				printf '%s <%s>' "$?" "$g_zxfer_backup_view_result"
			)
		)
		assertEquals "Format failures precede row-directory allocation [$kind]." "$expected <>" "$output"
	done

	zxfer_write_runtime_artifact_file "$staging" "$valid"
	output=$(
		(
			g_cmd_awk="awk"
			# Export before the function prefix for FreeBSD sh/ksh93.
			ZXFER_AWK_ROW_DIR=""
			export ZXFER_AWK_ROW_DIR
			awk() {
				if [ -n "${ZXFER_AWK_ROW_DIR:-}" ]; then
					printf partial >"$ZXFER_AWK_ROW_DIR/r1"
					return 13
				fi
				command awk "$@"
			}
			zxfer_load_backup_metadata_view "$staging" tank/src backup/dst/src
			printf '%s <%s>' "$?" "$g_zxfer_backup_view_result"
		)
	)
	assertEquals "A partial row write publishes no view." '5 <>' "$output"
}

test_candidate_discards_partial_reads_and_allocation_failures() {
	zxfer_backup_test_use_private_root candidate_partial
	valid=$(zxfer_backup_test_exact_pair_contents "$(zxfer_test_backup_metadata_row . compression=lz4=local)")
	for failure in read allocation; do
		output=$(
			(
				zxfer_read_local_backup_file() {
					zxfer_write_runtime_artifact_file "$2" "$valid"
					return 12
				}
				zxfer_ensure_runtime_artifact_file() {
					[ "$failure" != allocation ] || return 17
					zxfer_create_runtime_artifact_file "$2"
				}
				g_zxfer_backup_view_result=stale
				zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
					tank/src backup/dst tank/src backup/dst/src
				printf '%s <%s>' "$?" "$g_zxfer_backup_view_result"
			)
		)
		assertEquals "$failure failure must not publish partial metadata." '5 <>' "$output"
	done
}

test_forwarded_views_survive_pass_resets_and_restore_refreshes() {
	zxfer_backup_test_use_private_root view_lifetimes
	alias="$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src)"
	ZXFER_TEST_BACKUP_SOURCE_ROOT=tank/src
	ZXFER_TEST_BACKUP_DESTINATION_ROOT=tank/src
	zxfer_backup_test_write_file "$alias" "$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row . compression=gzip=local)" \
		"$(zxfer_test_backup_metadata_row child atime=off=local)")"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
	zxfer_resolve_forwarded_backup_metadata tank/src/child
	status=$?
	assertEquals 0 "$status"
	assertEquals atime=off=local "$g_zxfer_backup_forwarded_properties"
	assertNotContains "The alias index contains no property payload." "$g_zxfer_backup_forwarded_rows" 'atime='

	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row . compression=lz4=local)")"
	zxfer_get_backup_properties
	old_dir=$g_zxfer_backup_restore_row_dir
	read_file=$g_zxfer_backup_read_file
	zxfer_reset_property_iteration_caches
	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row . compression=off=local)")"
	zxfer_get_backup_properties
	assertFalse "Refresh retires the previous restore directory." "[ -d '$old_dir' ]"
	assertEquals "Reads reuse one staging file." "$read_file" "$g_zxfer_backup_read_file"
	zxfer_find_restored_backup_properties tank/src backup/dst/src
	status=$?
	assertEquals 0 "$status"
	assertEquals compression=off=local "$g_zxfer_property_row_payload_result"
	# A cached alias does not get reread after its file changes or disappears.
	rm -f "$alias"
	zxfer_resolve_forwarded_backup_metadata tank/src/child
	status=$?
	assertEquals 0 "$status"
	assertEquals atime=off=local "$g_zxfer_backup_forwarded_properties"
}

test_empty_forwarded_alias_and_negative_memo_do_not_forge_rows() {
	zxfer_backup_test_use_private_root empty_alias
	alias="$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src)"
	ZXFER_TEST_BACKUP_SOURCE_ROOT=tank/src
	ZXFER_TEST_BACKUP_DESTINATION_ROOT=tank/src
	zxfer_backup_test_write_file "$alias" "$(zxfer_test_render_current_backup_metadata_contents)"
	zxfer_resolve_forwarded_backup_metadata tank/src/child
	status=$?
	assertEquals "An empty alias is valid but supplies no dataset." 1 "$status"
	assertEquals '' "$g_zxfer_backup_forwarded_rows"
	assertEquals '' "$g_zxfer_backup_forwarded_properties"
	child_alias="$g_backup_storage_root/tank/src/child/$(zxfer_get_backup_metadata_filename tank/src/child tank/src/child)"
	ZXFER_TEST_BACKUP_SOURCE_ROOT=tank/src/child
	ZXFER_TEST_BACKUP_DESTINATION_ROOT=tank/src/child
	zxfer_backup_test_write_file "$child_alias" "$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row . compression=gzip=local)")"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
	zxfer_resolve_forwarded_backup_metadata tank/src/child
	status=$?
	assertEquals "Absence is memoized for the session." 1 "$status"
}

test_forwarded_view_refuses_missing_and_truncated_selected_rows() {
	zxfer_backup_test_use_private_root forwarded_row_failure
	alias="$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src)"
	ZXFER_TEST_BACKUP_SOURCE_ROOT=tank/src
	ZXFER_TEST_BACKUP_DESTINATION_ROOT=tank/src
	zxfer_backup_test_write_file "$alias" "$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row . compression=gzip=local)")"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
	zxfer_resolve_forwarded_backup_metadata tank/src
	view_dir=${g_zxfer_backup_forwarded_roots##*"$ZXFER_TAB"}
	for failure in truncated missing; do
		printf 'compression=forged=local' >"$view_dir/r1"
		[ "$failure" != missing ] || rm -f "$view_dir/r1"
		output=$(zxfer_resolve_forwarded_backup_metadata tank/src 2>&1)
		status=$?
		assertEquals "$failure rows fail closed." 1 "$status"
		assertContains "$output" "Failed to read forwarded backup property file $alias."
	done
}

test_metadata_view_accepts_late_roots_and_literal_payload_bytes() {
	contents="$ZXFER_BACKUP_METADATA_HEADER_LINE
#format_version:2
$(zxfer_test_backup_metadata_row . 'user:note=a\\nb=local')
$(zxfer_test_backup_metadata_row 'a b/[c]*' 'user:note=C:\\temp=local')
#source_root:tank/src
#destination_root:backup/dst/src"
	output=$(zxfer_test_check_metadata_pair "$contents" 'tank/src/a b/[c]*' 'backup/dst/src/a b/[c]*')
	status=$?
	assertEquals 0 "$status"
	assertEquals 'user:note=C:\\temp=local' "$output"
}
