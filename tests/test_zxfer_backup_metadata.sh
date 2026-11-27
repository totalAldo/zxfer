#!/bin/sh
#
# shunit2 tests for src/zxfer_backup_metadata.sh: -k metadata records,
# -e restore, local and remote storage, backup directories and paths.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_backup_metadata"
	zxfer_test_remote_host_fixture_one_time_setup
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	zxfer_test_reset_all_owner_state || return
	OPTIND=1
	unset FAKE_SSH_LOG FAKE_SSH_EXIT_STATUS FAKE_SSH_STDOUT FAKE_SSH_STDERR \
		FAKE_SSH_SUPPRESS_STDOUT ZXFER_BACKUP_DIR ZXFER_SECURE_PATH \
		ZXFER_SECURE_PATH_APPEND
	TMPDIR="$TEST_TMPDIR"
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$TEST_TMPDIR/.remote_tools/fake_ssh"
	g_backup_storage_root=""
	g_backup_file_extension=""
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return
}

tearDown() {
	zxfer_test_remote_host_fixture_teardown
}

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

	for malformed in "$buffered
no-tab-row" "$(zxfer_test_backup_metadata_row "." "")" \
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

test_backup_metadata_view_validates_the_file_and_resolves_relative_rows() {
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
		output=$(zxfer_test_check_metadata_pair "$contents" \
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
				printf '%s %s\n' "$?" "${g_zxfer_backup_view_dir_result:+view}"
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
			zxfer_read_path_metadata() {
				zxfer_parse_path_metadata_line "$(command ls -ldin "$1")" || return 1
				g_zxfer_path_owner_uid_result=4321
			}
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
				g_zxfer_backup_view_dir_result=stale
				g_zxfer_backup_view_source_root_result=stale
				g_zxfer_backup_view_destination_root_result=stale
				g_zxfer_backup_view_index_result=stale
				zxfer_create_private_temp_dir() { return 17; }
				if [ "$kind" = missing-row ]; then
					zxfer_load_backup_metadata_view "$staging" tank/src/missing backup/dst/src/missing
				else
					zxfer_load_backup_metadata_view "$staging" tank/src backup/dst/src
				fi
				printf '%s <%s|%s|%s|%s>' "$?" \
					"$g_zxfer_backup_view_dir_result" "$g_zxfer_backup_view_source_root_result" \
					"$g_zxfer_backup_view_destination_root_result" "$g_zxfer_backup_view_index_result"
			)
		)
		assertEquals "Format failures precede row-directory allocation [$kind]." "$expected <|||>" "$output"
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
			g_zxfer_backup_view_dir_result=stale
			g_zxfer_backup_view_source_root_result=stale
			g_zxfer_backup_view_destination_root_result=stale
			g_zxfer_backup_view_index_result=stale
			zxfer_load_backup_metadata_view "$staging" tank/src backup/dst/src
			printf '%s <%s|%s|%s|%s>' "$?" \
				"$g_zxfer_backup_view_dir_result" "$g_zxfer_backup_view_source_root_result" \
				"$g_zxfer_backup_view_destination_root_result" "$g_zxfer_backup_view_index_result"
		)
	)
	assertEquals "A partial row write publishes no view." '5 <|||>' "$output"
}

test_metadata_view_publishes_named_fields_and_preserves_backslash_roots() {
	zxfer_prepare_property_read_files || return
	zxfer_create_runtime_artifact_file metadata-named
	staging=$g_zxfer_runtime_artifact_path_result

	# G2: backslashes in source/destination names travel via ENVIRON, never awk -v.
	l_bs_src='tank/a\nb'
	l_bs_dst='backup/c\td'
	l_bs_contents="$ZXFER_BACKUP_METADATA_HEADER_LINE
#format_version:2
#source_root:$l_bs_src
#destination_root:$l_bs_dst
$(zxfer_test_backup_metadata_row '.' 'compression=lz4=local')
$(zxfer_test_backup_metadata_row 'sub\x' 'atime=off=local')"
	zxfer_write_runtime_artifact_file "$staging" "$l_bs_contents"
	zxfer_load_backup_metadata_view "$staging" "$l_bs_src/sub\\x" "$l_bs_dst/sub\\x"
	l_status=$?
	assertEquals "Backslash dataset roots validate cleanly via ENVIRON." 0 "$l_status"
	assertNotNull "Named view dir is published." "$g_zxfer_backup_view_dir_result"
	assertEquals "Named source root keeps literal backslashes." "$l_bs_src" "$g_zxfer_backup_view_source_root_result"
	assertEquals "Named destination root keeps literal backslashes." "$l_bs_dst" "$g_zxfer_backup_view_destination_root_result"
	assertEquals "Named index holds raw newline-joined rows without surrounding LF." \
		"r1${ZXFER_TAB}.${ZXFER_LF}r2${ZXFER_TAB}sub\\x" "$g_zxfer_backup_view_index_result"

	# An empty alias publishes an empty index string (status 8) with no stray LF.
	l_empty_alias="$ZXFER_BACKUP_METADATA_HEADER_LINE
#format_version:2
#source_root:tank/src
#destination_root:tank/src"
	zxfer_write_runtime_artifact_file "$staging" "$l_empty_alias"
	zxfer_load_backup_metadata_view "$staging" tank/src tank/src tank/src
	l_status=$?
	assertEquals "Empty alias returns status 8 with a valid view." 8 "$l_status"
	assertEquals "Empty alias publishes empty index string." "" "$g_zxfer_backup_view_index_result"
	assertEquals "tank/src" "$g_zxfer_backup_view_source_root_result"
	assertEquals "tank/src" "$g_zxfer_backup_view_destination_root_result"

	zxfer_reset_backup_metadata_state
	assertEquals "Session reset clears every backup view result global." "|||" \
		"$g_zxfer_backup_view_dir_result|$g_zxfer_backup_view_source_root_result|$g_zxfer_backup_view_destination_root_result|$g_zxfer_backup_view_index_result"

	# P7b-heredoc: validating literal backslashes and shell syntax keeps bytes untouched.
	# shellcheck disable=SC2016  # Literal shell syntax must not expand here.
	l_literal_row=$(zxfer_test_backup_metadata_row "." 'user:note=a\nb $HOME `uname`=local')
	assertEquals "Heredoc record validation preserves literal bytes." "$l_literal_row" \
		"$(zxfer_validate_backup_metadata_record_list "$l_literal_row")"
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
				g_zxfer_backup_view_dir_result=stale
				g_zxfer_backup_view_source_root_result=stale
				g_zxfer_backup_view_destination_root_result=stale
				g_zxfer_backup_view_index_result=stale
				zxfer_try_backup_restore_candidate "$g_backup_storage_root/tank/src" \
					tank/src backup/dst tank/src backup/dst/src
				printf '%s <%s|%s|%s|%s>' "$?" \
					"$g_zxfer_backup_view_dir_result" "$g_zxfer_backup_view_source_root_result" \
					"$g_zxfer_backup_view_destination_root_result" "$g_zxfer_backup_view_index_result"
			)
		)
		assertEquals "$failure failure must not publish partial metadata." '5 <|||>' "$output"
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
	view_dir=$g_zxfer_backup_forwarded_alias_dir
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

zxfer_backup_test_stub_local_transport() {
	zxfer_build_remote_sh_c_command() {
		g_zxfer_shell_command_result=$1
	}
	zxfer_invoke_ssh_shell_command_for_host() { sh -c "$2"; }
}

# Run the real pair writer with a failed filesystem operation, locally or
# through the rendered SSH program. The wrappers affect only this subshell.
zxfer_backup_test_fail_pair_write() (
	BACKUP_TEST_PAIR_FAILURE=$1
	BACKUP_TEST_PAIR_BIN="$TEST_TMPDIR/pair-failure-bin"
	BACKUP_TEST_REAL_MV=$(command -v mv)
	BACKUP_TEST_REAL_MKTEMP=$(command -v mktemp)
	BACKUP_TEST_REAL_CAT=$(command -v cat)
	export BACKUP_TEST_PAIR_FAILURE BACKUP_TEST_PRIMARY_FILE BACKUP_TEST_FORWARDED_FILE
	export BACKUP_TEST_REAL_MV BACKUP_TEST_REAL_MKTEMP BACKUP_TEST_REAL_CAT
	mkdir -p "$BACKUP_TEST_PAIR_BIN"
	cat >"$BACKUP_TEST_PAIR_BIN/tool" <<'EOF'
#!/bin/sh
case ${0##*/} in
mktemp)
	if [ "$BACKUP_TEST_PAIR_FAILURE" = stage ] &&
		[ "${1%/*}" = "${BACKUP_TEST_FORWARDED_FILE%/*}" ]; then
		exit 1
	fi
	exec "$BACKUP_TEST_REAL_MKTEMP" "$@"
	;;
cat)
	if [ "$BACKUP_TEST_PAIR_FAILURE" = recovery_read ] &&
		[ "${1:-}" = "$BACKUP_TEST_PRIMARY_FILE" ]; then
		exit 1
	fi
	exec "$BACKUP_TEST_REAL_CAT" "$@"
	;;
mv)
	case $BACKUP_TEST_PAIR_FAILURE in
	primary)
		[ "$3" != "$BACKUP_TEST_PRIMARY_FILE" ] || exit 1
		;;
	forwarded | rollback)
		[ "$3" != "$BACKUP_TEST_FORWARDED_FILE" ] || exit 1
		if [ "$BACKUP_TEST_PAIR_FAILURE" = rollback ]; then
			case $2 in *.zxfer-backup-recovery.*) exit 1 ;; esac
		fi
		;;
	signal_primary | signal_forwarded)
		"$BACKUP_TEST_REAL_MV" "$@" || exit "$?"
		if { [ "$BACKUP_TEST_PAIR_FAILURE" = signal_primary ] && [ "$3" = "$BACKUP_TEST_PRIMARY_FILE" ]; } ||
			{ [ "$BACKUP_TEST_PAIR_FAILURE" = signal_forwarded ] && [ "$3" = "$BACKUP_TEST_FORWARDED_FILE" ]; }; then
			kill -s TERM "$PPID"
		fi
		exit 0
		;;
	esac
	exec "$BACKUP_TEST_REAL_MV" "$@"
	;;
esac
EOF
	chmod 700 "$BACKUP_TEST_PAIR_BIN/tool"
	for pair_tool in mv mktemp cat; do
		ln -sf tool "$BACKUP_TEST_PAIR_BIN/$pair_tool"
	done
	PATH="$BACKUP_TEST_PAIR_BIN:$PATH"
	export PATH
	if [ "$2" = remote ]; then
		g_option_T_target_host="target.example"
		g_zxfer_secure_path=$PATH
		zxfer_backup_test_stub_local_transport
		zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
	fi
	zxfer_write_backup_properties
)

test_backup_pair_failures_preserve_both_previous_files_locally_and_remotely() {
	for pair_mode in local remote; do
		for pair_failure in stage recovery_read primary forwarded; do
			zxfer_backup_test_use_private_root "pair_${pair_mode}_${pair_failure}"
			g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
			(umask 077 && mkdir -p "${BACKUP_TEST_PRIMARY_FILE%/*}" "${BACKUP_TEST_FORWARDED_FILE%/*}")
			printf 'old primary\n' >"$BACKUP_TEST_PRIMARY_FILE"
			printf 'old forwarded\n' >"$BACKUP_TEST_FORWARDED_FILE"
			output=$(zxfer_backup_test_fail_pair_write "$pair_failure" "$pair_mode" 2>&1)
			assertEquals "$pair_mode $pair_failure must report failure: $output" 1 "$?"
			assertEquals "$pair_mode $pair_failure preserves the primary." "old primary" "$(cat "$BACKUP_TEST_PRIMARY_FILE")"
			assertEquals "$pair_mode $pair_failure preserves the alias." "old forwarded" "$(cat "$BACKUP_TEST_FORWARDED_FILE")"
			assertEquals "Handled failure cleans its private staging and recovery files." \
				"" "$(find "$g_backup_storage_root" -name '.zxfer-backup-*')"
		done
	done
}

test_backup_pair_second_publish_failure_removes_a_new_primary() {
	for pair_mode in local remote; do
		zxfer_backup_test_use_private_root "pair_new_$pair_mode"
		g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
		output=$(zxfer_backup_test_fail_pair_write forwarded "$pair_mode" 2>&1)
		assertEquals "$pair_mode must report failure: $output" 1 "$?"
		assertFalse "A failed first publication must leave neither file live." "[ -e '$BACKUP_TEST_PRIMARY_FILE' ]"
		assertFalse "[ -e '$BACKUP_TEST_FORWARDED_FILE' ]"
	done
}

test_backup_pair_rollback_failure_keeps_private_recovery_and_reports_its_path() {
	for pair_mode in local remote; do
		zxfer_backup_test_use_private_root "pair_recovery_$pair_mode"
		g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
		(umask 077 && mkdir -p "${BACKUP_TEST_PRIMARY_FILE%/*}" "${BACKUP_TEST_FORWARDED_FILE%/*}")
		printf 'old primary\n' >"$BACKUP_TEST_PRIMARY_FILE"
		printf 'old forwarded\n' >"$BACKUP_TEST_FORWARDED_FILE"
		output=$(zxfer_backup_test_fail_pair_write rollback "$pair_mode" 2>&1)
		assertEquals "$pair_mode must report failure: $output" 1 "$?"
		recovery_file=$(find "$g_backup_storage_root" -name '.zxfer-backup-recovery.*')
		assertNotNull "A failed rollback preserves the old contents for recovery." "$recovery_file"
		assertEquals "old primary" "$(cat "$recovery_file")"
		assertContains "$output" "$recovery_file"
		assertContains "$output" "restoring backup metadata rollback state"
		assertFalse "A failed rollback must not leave the new primary looking authoritative." "[ -e '$BACKUP_TEST_PRIMARY_FILE' ]"
		assertEquals "old forwarded" "$(cat "$BACKUP_TEST_FORWARDED_FILE")"
		case "$(ls -ln "$recovery_file")" in
		-rw-------*) ;;
		*) fail "Recovery metadata must remain private." ;;
		esac
	done
}

test_backup_pair_defers_signals_until_publication_state_is_consistent() {
	for pair_mode in local remote; do
		for pair_signal in signal_primary signal_forwarded; do
			zxfer_backup_test_use_private_root "pair_${pair_mode}_${pair_signal}"
			g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
			(umask 077 && mkdir -p "${BACKUP_TEST_PRIMARY_FILE%/*}" "${BACKUP_TEST_FORWARDED_FILE%/*}")
			printf 'old primary\n' >"$BACKUP_TEST_PRIMARY_FILE"
			printf 'old forwarded\n' >"$BACKUP_TEST_FORWARDED_FILE"
			output=$(zxfer_backup_test_fail_pair_write "$pair_signal" "$pair_mode" 2>&1)
			assertEquals "$pair_mode $pair_signal reports interruption: $output" 1 "$?"
			if [ "$pair_signal" = signal_primary ]; then
				assertEquals "A signal after the first rename restores the old primary." "old primary" "$(cat "$BACKUP_TEST_PRIMARY_FILE")"
				assertEquals "old forwarded" "$(cat "$BACKUP_TEST_FORWARDED_FILE")"
			else
				assertContains "A signal after the second rename keeps the completed primary." \
					"$(cat "$BACKUP_TEST_PRIMARY_FILE")" "$g_backup_file_contents"
				assertContains "A signal after the second rename keeps the completed alias." \
					"$(cat "$BACKUP_TEST_FORWARDED_FILE")" "$g_backup_file_contents"
			fi
			assertEquals "Interrupted writes leave no disposable staging files." \
				"" "$(find "$g_backup_storage_root" -name '.zxfer-backup-*')"
		done
	done
}

test_get_backup_metadata_filename_renders_the_documented_identity_paths() {
	g_backup_file_extension=".zxfer_backup_info"
	long_source="tank/$(printf 'a%.0s' 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30)"
	# The man page example is the hex of "tank/src\nbackup/dst" in 48-character
	# chunks under the fixed v2 leaf name; a longer identity takes more chunks.
	# Retired writers keyed the name by cksum of "tank/src<LF>backup/dst" with
	# no final newline. source|destination|kind|name
	for case_spec in \
		"tank/src|backup/dst||.zxfer_backup_info.v2/h/74616e6b2f7372630a6261636b75702f647374/.zxfer_backup_info.v2" \
		"tank/my :src|backup/dst||.zxfer_backup_info.v2/h/74616e6b2f6d79203a7372630a6261636b75702f647374/.zxfer_backup_info.v2" \
		"$long_source|backup/dst||.zxfer_backup_info.v2/h/74616e6b2f61616161616161616161616161616161616161/61616161616161616161610a6261636b75702f647374/.zxfer_backup_info.v2" \
		"tank/src|backup/dst|legacy|.zxfer_backup_info.src.k1537737481.19"; do
		name_source=${case_spec%%|*}
		rest=${case_spec#*|}
		name_destination=${rest%%|*}
		rest=${rest#*|}
		name_kind=${rest%%|*}
		assertEquals "[$name_source] -> [$name_destination] ${name_kind:-current} name." \
			"${rest#*|}" \
			"$(zxfer_get_backup_metadata_filename "$name_source" "$name_destination" ${name_kind:+"$name_kind"})"
	done

	for name_kind in current legacy; do
		output=$(
			g_cmd_awk=false
			zxfer_get_backup_metadata_filename tank/src backup/dst "$name_kind"
		)
		assertEquals "A failed $name_kind identity pass fails closed without a partial name." \
			"1 " "$? $output"
	done
}

test_ensure_local_backup_dir_creates_private_directories_and_refuses_unsafe_paths() {
	base="$TEST_TMPDIR_PHYSICAL/ensure_local"
	mkdir -p "$base"
	zxfer_ensure_local_backup_dir "$base/new/nested"
	case "$(ls -ldn "$base/new/nested")" in
	drwx------*) ;;
	*) fail "Created directories must be 0700: $(ls -ldn "$base/new/nested")" ;;
	esac
	case "$(ls -ldn "$base/new")" in
	drwx------*) ;;
	*) fail "Intermediate directories are created under umask 077: $(ls -ldn "$base/new")" ;;
	esac

	ln -s "$base/new" "$base/link"
	: >"$base/file"
	failing_chmod_dir="$TEST_TMPDIR/ensure_local_failing_chmod"
	mkdir -p "$failing_chmod_dir"
	printf '#!/bin/sh\nexit 1\n' >"$failing_chmod_dir/chmod"
	chmod +x "$failing_chmod_dir/chmod"
	# directory|simulated failure|expected error
	for case_spec in \
		"$base/link||Refusing to use backup directory $base/link because it is a symlink." \
		"$base/link/child||Refusing to use backup directory $base/link/child because path component $base/link is a symlink." \
		"$base/file||Refusing to use backup directory $base/file because it is not a directory." \
		"$base/new|owner 4321|Refusing to use backup directory $base/new because it is owned by UID 4321 instead of" \
		"$base/new|owner unknown|Cannot determine the owner of backup directory $base/new." \
		"$base/missing|mkdir|Error creating secure backup directory $base/missing." \
		"$base/new|chmod|Error securing backup directory $base/new."; do
		dir=${case_spec%%|*}
		rest=${case_spec#*|}
		failure=${rest%%|*}
		output=$(
			(
				# No case statement here: bash 3.2 (macOS /bin/sh) mis-parses
				# case patterns inside command substitution.
				if [ "$failure" = "owner unknown" ]; then
					zxfer_read_path_metadata() { return 1; }
				elif [ "${failure%% *}" = owner ]; then
					zxfer_read_path_metadata() {
						g_zxfer_path_owner_uid_result=${failure#owner }
					}
				elif [ "$failure" = mkdir ]; then
					mkdir() { return 1; }
				elif [ "$failure" = chmod ]; then
					PATH="$failing_chmod_dir:$PATH"
				fi
				zxfer_ensure_local_backup_dir "$dir"
			) 2>&1
		)
		assertEquals "Directory [$dir] ${failure:+with $failure }must be refused." 1 "$?"
		assertContains "$output" "${rest#*|}"
	done
}

# The local pair writer runs /bin/sh, like job shells, so a secure PATH
# without sh (as the integration harness narrows ZXFER_SECURE_PATH) still
# publishes both files.
test_write_backup_properties_runs_bin_sh_without_sh_on_path() {
	zxfer_backup_test_use_private_root write_without_sh
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	no_sh_path="$TEST_TMPDIR/backup-path-without-sh"
	mkdir -p "$no_sh_path"
	for no_sh_tool in awk cat chmod date id ls mkdir mktemp mv rm stat; do
		case $(command -v "$no_sh_tool" 2>/dev/null) in
		/*) ln -sf "$(command -v "$no_sh_tool")" "$no_sh_path/$no_sh_tool" ;;
		esac
	done

	output=$(
		(
			PATH=$no_sh_path
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals "A local -k write needs no sh on PATH; output: $output" 0 "$?"
	for metadata_file in "$BACKUP_TEST_PRIMARY_FILE" "$BACKUP_TEST_FORWARDED_FILE"; do
		assertContains "The write publishes $metadata_file." \
			"$(cat "$metadata_file" 2>/dev/null)" "$g_backup_file_contents"
	done
}

test_write_backup_properties_reports_remote_dependency_write_and_transport_failures() {
	zxfer_backup_test_use_private_root write_remote_failures
	g_option_T_target_host="target.example"
	g_option_n_dryrun=0
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	empty_dir="$TEST_TMPDIR/write_remote_empty_bin"
	mkdir -p "$empty_dir"

	output=$(
		(
			g_zxfer_secure_path="$empty_dir"
			zxfer_backup_test_stub_local_transport
			zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
			zxfer_throw_error() {
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Required dependency \"mkdir\" not found on host target.example in secure PATH ($empty_dir)."
	assertContains "$output" "class=dependency Required remote backup-write helper dependency not found on host target.example in secure PATH ($empty_dir)."

	for case_spec in \
		"92||Error writing backup file. Is filesystem mounted?" \
		"255|ssh: connect refused|Failed to contact host target.example while writing backup metadata $BACKUP_TEST_PRIMARY_FILE." \
		"1||Error writing backup file. Is filesystem mounted?"; do
		remote_status=${case_spec%%|*}
		rest=${case_spec#*|}
		remote_stderr=${rest%%|*}
		expected=${rest#*|}
		output=$(
			(
				REMOTE_STATUS=$remote_status
				REMOTE_STDERR=$remote_stderr
				zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
				zxfer_invoke_ssh_shell_command_for_host() {
					[ -z "$REMOTE_STDERR" ] || printf '%s\n' "$REMOTE_STDERR" >&2
					return "$REMOTE_STATUS"
				}
				zxfer_write_backup_properties
			) 2>&1
		)
		assertEquals "Remote status $remote_status must fail the write." 1 "$?"
		assertContains "$output" "$expected"
		[ -z "$remote_stderr" ] || assertContains "Transport stderr is forwarded." "$output" "$remote_stderr"
	done

	output=$(
		(
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result='Required dependency "cat" not found on host target.example.'
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "class=dependency Required dependency \"cat\" not found on host target.example."
}

test_check_backup_storage_dir_if_needed_reports_remote_dependency_prepare_and_transport_failures() {
	empty_dir="$TEST_TMPDIR/ensure_remote_empty_bin"
	mkdir -p "$empty_dir"
	target_dir="$TEST_TMPDIR_PHYSICAL/ensure_remote_dir/child"
	g_option_k_backup_property_mode=1
	g_option_T_target_host="target.example"
	g_backup_storage_root=$target_dir

	(
		zxfer_backup_test_stub_local_transport
		zxfer_check_backup_storage_dir_if_needed
	)
	assertEquals 0 "$?"
	case "$(ls -ldn "$target_dir")" in
	drwx------*) ;;
	*) fail "The remote program creates the directory 0700: $(ls -ldn "$target_dir")" ;;
	esac

	output=$(
		(
			g_zxfer_secure_path="$empty_dir"
			zxfer_backup_test_stub_local_transport
			zxfer_throw_error() {
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Required dependency \"mkdir\" not found on host target.example in secure PATH ($empty_dir)."
	assertContains "$output" "class=dependency Required remote backup-directory helper dependency not found on host target.example in secure PATH ($empty_dir)."

	for case_spec in \
		"92|Backup path exists but is not a directory.|Error preparing backup directory on target.example." \
		"255|ssh: connect refused|Failed to contact host target.example while preparing backup directory $target_dir." \
		"1||Error preparing backup directory on target.example."; do
		remote_status=${case_spec%%|*}
		rest=${case_spec#*|}
		remote_stderr=${rest%%|*}
		expected=${rest#*|}
		output=$(
			(
				REMOTE_STATUS=$remote_status
				REMOTE_STDERR=$remote_stderr
				zxfer_invoke_ssh_shell_command_for_host() {
					[ -z "$REMOTE_STDERR" ] || printf '%s\n' "$REMOTE_STDERR" >&2
					return "$REMOTE_STATUS"
				}
				zxfer_check_backup_storage_dir_if_needed
			) 2>&1
		)
		assertEquals "Remote status $remote_status must fail directory preparation." 1 "$?"
		assertContains "$output" "$expected"
		[ -z "$remote_stderr" ] || assertContains "Remote stderr is forwarded." "$output" "$remote_stderr"
	done
}

test_run_remote_backup_script_feeds_stdin_and_reports_capture_failures() {
	marker="$TEST_TMPDIR/run_remote_script_payload"
	(
		zxfer_backup_test_stub_local_transport
		zxfer_run_remote_backup_script "target.example" "cat >'$marker';" destination "testing" backup-test 92 <<EOF
payload line
EOF
	)
	assertEquals 0 "$?"
	assertEquals "Standard input reaches the remote program." "payload line" "$(cat "$marker")"

	output=$(
		(
			zxfer_backup_test_stub_local_transport
			zxfer_read_runtime_artifact_file() {
				return 1
			}
			zxfer_run_remote_backup_script "target.example" "echo warning >&2; true;" destination "testing" backup-test 92
		) 2>&1
	)
	assertEquals "A truncated capture fails closed." 1 "$?"
	assertContains "$output" "Failed to reload local remote helper capture while testing on host target.example."
	assertContains "$output" "Failed to read remote probe stderr capture from local staging."
}

test_build_remote_backup_dir_prepare_cmd_runs_locally_and_rejects_symlinked_components() {
	base="$TEST_TMPDIR_PHYSICAL/remote_prepare"
	mkdir -p "$base/real"
	ln -s "$base/real" "$base/link"
	: >"$base/file"

	sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "$base/new/nested" "target.example")"
	assertEquals 0 "$?"
	case "$(ls -ldn "$base/new/nested")" in
	drwx------*) ;;
	*) fail "The rendered program creates 0700 directories: $(ls -ldn "$base/new/nested")" ;;
	esac

	for case_spec in \
		"$base/link|Refusing to use symlinked zxfer backup directory." \
		"$base/link/child|Refusing to use backup directory $base/link/child because path component $base/link is a symlink." \
		"$base/file|Backup path exists but is not a directory."; do
		output=$(sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "${case_spec%%|*}" "target.example")" 2>&1)
		assertEquals "Unsafe remote directory [${case_spec%%|*}] exits 92." 92 "$?"
		assertContains "$output" "${case_spec#*|}"
	done
	assertFalse "Nothing is created below a symlinked component." "[ -e '$base/real/child' ]"

	output=$(sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "-dashed/dir" "target.example")" 2>&1)
	assertEquals "Relative dash-prefixed paths are quoted for ls and still fail closed outside a real root." 92 "$?"
}

test_build_remote_backup_dir_prepare_cmd_checks_owner_before_chmod_and_prints_literal_paths() {
	base="$TEST_TMPDIR_PHYSICAL/remote_prepare_security"
	mkdir -p "$base/owned_other" "$base/real"
	chmod 755 "$base/owned_other"

	# Fake ls reports UID 4321 while fake id reports UID 1000, and fake chmod
	# logs any call so we prove chmod never mutates an unowned directory.
	fake_bin="$TEST_TMPDIR/remote_prepare_owner_bin"
	chmod_log="$TEST_TMPDIR/remote_prepare_chmod.log"
	mkdir -p "$fake_bin"
	: >"$chmod_log"
	cat >"$fake_bin/ls" <<'EOF'
#!/bin/sh
printf 'drwxr-xr-x 2 4321 4321 64 Jan 1 00:00 %s\n' "$2"
EOF
	cat >"$fake_bin/id" <<'EOF'
#!/bin/sh
printf '1000\n'
EOF
	cat >"$fake_bin/chmod" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$chmod_log"
exit 0
EOF
	chmod +x "$fake_bin/ls" "$fake_bin/id" "$fake_bin/chmod"
	other_owner_cmd=$(
		g_zxfer_secure_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
		zxfer_build_remote_backup_dir_prepare_cmd "$base/owned_other" "target.example"
	)
	output=$(sh -c "$other_owner_cmd" 2>&1)
	assertEquals "An unowned existing directory must fail with status 92." 92 "$?"
	assertContains "$output" "Backup directory must be owned by root or the ssh user."
	assertEquals "chmod must never run before the directory owner is verified." "" "$(cat "$chmod_log")"

	# A top-level symlink target (e.g. /tmp on macOS) is skipped by the prelude
	# component walk and must still be rejected directly by [ -L "$l_prepare_dir" ].
	if [ -L /tmp ]; then
		output=$(sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd /tmp "target.example")" 2>&1)
		assertEquals "Top-level symlink target must be refused with status 92." 92 "$?"
		assertEquals "Refusing to use symlinked zxfer backup directory." "$output"
	fi

	# Prelude symlink diagnostics must use printf so literal backslashes (such
	# as \n or \t) in variable paths are never expanded on dash or macOS /bin/sh.
	bs_link="$base/dir\\nlink"
	ln -s "$base/real" "$bs_link"
	output=$(sh -c "$(zxfer_build_remote_backup_dir_prepare_cmd "$bs_link/child" "target.example")" 2>&1)
	assertEquals "Symlinked backslash directory component exits 92." 92 "$?"
	assertEquals "Refusing to use backup directory $bs_link/child because path component $bs_link is a symlink." "$output"

	bs_meta="$base/meta\\nlink"
	ln -s "$base/real" "$bs_meta"
	output=$(sh -c "$(zxfer_build_remote_backup_read_cmd "$bs_meta" "origin.example")" 2>&1)
	assertEquals "Symlinked backslash metadata target exits 98." 98 "$?"
	assertEquals "Refusing to use backup metadata $bs_meta because it is a symlink." "$output"
}

test_build_backup_pair_write_cmd_handles_a_shared_target_without_following_symlinks() {
	dir="$TEST_TMPDIR_PHYSICAL/remote_write"
	target="$dir/backup.meta"
	decoy="$TEST_TMPDIR_PHYSICAL/remote_write_decoy"
	printf 'decoy\n' >"$decoy"
	(umask 077 && mkdir -p "$dir")

	printf '%s\n' "line one" "line two" |
		sh -c "$(zxfer_build_backup_pair_write_cmd "$target" "$target" tank/src cat)"
	assertEquals 0 "$?"
	assertEquals "line one
line two" "$(cat "$target")"
	case "$(ls -ldn "$target")" in
	-rw-------*) ;;
	*) fail "Remote writes publish 0600 files: $(ls -ldn "$target")" ;;
	esac

	rm -f "$target"
	ln -s "$decoy" "$target"
	output=$(printf 'attack\n' | sh -c "$(zxfer_build_backup_pair_write_cmd "$target" "$target" tank/src cat)" 2>&1)
	assertEquals 92 "$?"
	assertContains "$output" "Refusing to write backup metadata $target because it is a symlink."
	assertEquals "decoy" "$(cat "$decoy")"
	rm -f "$target"

	output=$(sh -c "$(zxfer_build_backup_pair_write_cmd "$target" "$target" tank/src false)" 2>&1)
	assertEquals "A failed payload helper exits 92." 92 "$?"
	assertFalse "[ -e '$target' ]"
	assertEquals "A failed payload leaves no stage file." "" "$(find "$dir" -name '.zxfer-backup-write.*')"
}

test_build_remote_backup_read_cmd_enforces_owner_mode_and_directory_checks() {
	dir="$TEST_TMPDIR_PHYSICAL/remote_read"
	mkdir -p "$dir"
	chmod 700 "$dir"
	file="$dir/backup.meta"
	printf 'payload\n' >"$file"
	chmod 600 "$file"
	g_cmd_cat="/bin/cat"

	assertEquals "payload" "$(sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")")"

	sh -c "$(zxfer_build_remote_backup_read_cmd "$dir/missing.meta" "origin.example")" >/dev/null
	assertEquals "Missing files exit 94." 94 "$?"

	chmod 644 "$file"
	sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")" >/dev/null
	assertEquals "Files that are not 0600 exit 96." 96 "$?"
	chmod 600 "$file"

	chmod 777 "$dir"
	sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")" >/dev/null
	assertEquals "A directory other users can modify exits 91." 91 "$?"
	chmod 1777 "$dir"
	assertEquals "A sticky shared directory cannot swap the entry and is accepted." \
		"payload" "$(sh -c "$(zxfer_build_remote_backup_read_cmd "$file" "origin.example")")"
	chmod 700 "$dir"

	ln -s "$dir" "$TEST_TMPDIR_PHYSICAL/remote_read_link"
	output=$(sh -c "$(zxfer_build_remote_backup_read_cmd "$TEST_TMPDIR_PHYSICAL/remote_read_link/backup.meta" "origin.example")" 2>&1)
	assertEquals "Symlinked components exit 98." 98 "$?"
	assertContains "$output" "Refusing to use backup metadata $TEST_TMPDIR_PHYSICAL/remote_read_link/backup.meta because path component $TEST_TMPDIR_PHYSICAL/remote_read_link is a symlink."

	# Root-owned directories are trusted, so the directory must belong to
	# neither root nor the ssh user; a fake id reports the ssh user as the
	# directory owner's uid plus one. The FreeBSD and OmniOS CI guests run the
	# suite as root: there the root-owned case is checked first, then the
	# directory is given to uid 4320.
	test_uid=$(id -u)
	dir_owner=$test_uid
	[ "$test_uid" -ne 0 ] || dir_owner=4320
	fake_bin="$TEST_TMPDIR/remote_read_other_owner"
	mkdir -p "$fake_bin"
	cat >"$fake_bin/id" <<EOF
#!/bin/sh
printf '%s\n' $((dir_owner + 1))
EOF
	chmod +x "$fake_bin/id"
	other_user_cmd=$(
		g_zxfer_secure_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
		zxfer_build_remote_backup_read_cmd "$file" "origin.example"
	)
	if [ "$test_uid" -eq 0 ]; then
		assertEquals "A root-owned directory and file are trusted for any ssh user." \
			"payload" "$(sh -c "$other_user_cmd")"
		chown "$dir_owner" "$dir" || fail "Could not give $dir to uid $dir_owner."
	fi
	sh -c "$other_user_cmd" >/dev/null
	assertEquals "When the ssh user is neither root nor the directory owner the directory check exits 91." 91 "$?"
}

test_build_remote_backup_storage_listing_cmd_lists_the_root_chain_and_descendants() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/listing_store"
	(umask 077 && mkdir -p "$g_backup_storage_root/tank/src/a/.zxfer_backup_info.v2/h/0a" \
		"$g_backup_storage_root/tank/src/b/c" "$g_backup_storage_root/other")
	: >"$g_backup_storage_root/tank/src/.zxfer_backup_info.src.k1.2"
	ln -s "$g_backup_storage_root/other" "$g_backup_storage_root/tank/src/link"

	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd tank/src origin.example)" | sort -u)
	assertEquals "The chain, descendant directories and symlinks are listed; metadata trees are not." \
		"tank
tank/src
tank/src/a
tank/src/b
tank/src/b/c
tank/src/link" "$output"

	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd tank/src/b/c/d origin.example)" | sort -u)
	assertEquals "Only existing directories of a deeper root's chain are listed." \
		"tank
tank/src
tank/src/b
tank/src/b/c" "$output"

	ln -s "$g_backup_storage_root/other" "$g_backup_storage_root/pool"
	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd pool/src origin.example)" 2>&1)
	assertEquals "A symlinked path component exits 92." 92 "$?"
	assertContains "$output" "path component $g_backup_storage_root/pool is a symlink."

	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/listing_store_missing"
	output=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd tank/src origin.example)")
	assertEquals "A missing store lists nothing and succeeds." 0 "$?"
	assertEquals "" "$output"
}

# An -O -k run lists the origin's store once. find(1) searches only the
# source root's own storage directory, so a store that is missing or holds
# only an ancestor needs no find; once the root has a directory, a missing
# find is the remote helper dependency error. Regression: every -O -k run,
# a first hop included, required find.
test_list_remote_backup_storage_dirs_needs_find_only_for_an_existing_root_directory() {
	no_find_bin="$TEST_TMPDIR/listing_no_find_bin"
	mkdir -p "$no_find_bin"
	g_initial_source="tank/src"
	g_option_O_origin_host="origin.example"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/listing_find_store"

	for store_dir in "" tank tank/src; do
		[ -z "$store_dir" ] || (umask 077 && mkdir -p "$g_backup_storage_root/$store_dir")
		output=$(
			(
				g_zxfer_secure_path=$no_find_bin
				zxfer_build_remote_sh_c_command() {
					g_zxfer_shell_command_result=$1
				}
				zxfer_invoke_ssh_shell_command_for_host() { sh -c "$2"; }
				zxfer_list_remote_backup_storage_dirs
				printf 'listed=%s listing=%s\n' "$g_zxfer_backup_forwarded_listed" \
					"$g_zxfer_backup_forwarded_listing"
			) 2>&1
		)
		status=$?
		if [ "$store_dir" != tank/src ]; then
			assertEquals "Store [${store_dir:-missing}] lists without find." 0 "$status"
			assertEquals "listed=1 listing=$store_dir" "$output"
			continue
		fi
		assertEquals "A root directory without find on the origin fails closed." 1 "$status"
		assertContains "$output" "Required dependency \"find\" not found on host origin.example in secure PATH ($no_find_bin)."
		assertContains "$output" "Required remote backup-metadata helper dependency not found on host origin.example in secure PATH ($no_find_bin)."
	done
}

test_read_remote_backup_file_maps_program_statuses_to_documented_errors() {
	zxfer_create_runtime_artifact_file backup-test
	staging=$g_zxfer_runtime_artifact_path_result
	# remote status | throw or return:<status> | expected message
	for case_spec in \
		"91|throw|Refusing to use backup metadata /tmp/backup.meta on origin.example because its directory is not a private directory owned by root or the ssh user." \
		"95|throw|Refusing to use backup metadata /tmp/backup.meta on origin.example because it is not owned by root or the ssh user." \
		"96|throw|Refusing to use backup metadata /tmp/backup.meta on origin.example because its permissions are not 0600." \
		"97|throw|Cannot determine ownership or permissions for backup metadata /tmp/backup.meta on origin.example." \
		"94|return:4|" \
		"98|return:1|Refusing to use backup metadata /tmp/backup.meta because path component /tmp/link is a symlink." \
		"255|throw|Failed to contact host origin.example while reading backup metadata /tmp/backup.meta." \
		"3|return:5|"; do
		remote_status=${case_spec%%|*}
		rest=${case_spec#*|}
		expected_status=${rest%%|*}
		expected_message=${rest#*|}
		output=$(
			(
				REMOTE_STATUS=$remote_status
				zxfer_invoke_ssh_shell_command_for_host() {
					# No case statement here: bash 3.2 (macOS /bin/sh)
					# mis-parses case patterns inside command substitution.
					if [ "$REMOTE_STATUS" -eq 98 ]; then
						printf 'Refusing to use backup metadata /tmp/backup.meta because path component /tmp/link is a symlink.\n' >&2
					elif [ "$REMOTE_STATUS" -eq 255 ]; then
						printf 'ssh: connect refused\n' >&2
					fi
					return "$REMOTE_STATUS"
				}
				zxfer_read_remote_backup_file "origin.example" "/tmp/backup.meta" source "$staging" >/dev/null
				printf 'status=%s\n' "$?"
			) 2>&1
		)
		case "$expected_status" in
		throw) assertEquals "Remote status $remote_status throws." 1 "$?" ;;
		*) assertContains "Remote status $remote_status maps to ${expected_status#return:}." "$output" "status=${expected_status#return:}" ;;
		esac
		[ -z "$expected_message" ] || assertContains "$output" "$expected_message"
	done

	result=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'remote payload'
			}
			zxfer_read_remote_backup_file "origin.example" "/tmp/backup.meta" source "$staging" >/dev/null
			cat "$staging"
		)
	)
	assertEquals "Successful reads publish the payload." "remote payload" "$result"
}

test_backup_script_body_emits_literal_bytes_and_preserves_renderer_state() {
	l_body_input="$TEST_TMPDIR/backup_body.input"
	l_body_output="$TEST_TMPDIR/backup_body.output"
	# shellcheck disable=SC2016  # Literal shell syntax must not expand here.
	printf '%s\n' '  $1; $(touch NEVER); `touch NEVER`;' '' 'printf '\''%s\\n'\'' "quoted";' >"$l_body_input"
	printf '%s' 'last line without LF;' >>"$l_body_input"
	g_zxfer_shell_command_result='caller-owned argv'
	zxfer_print_backup_script_body <"$l_body_input" >"$l_body_output"
	printf '\n' >>"$l_body_input"
	assertTrue "Blank lines, indentation, quotes and shell syntax stay literal." \
		"cmp -s '$l_body_input' '$l_body_output'"
	assertEquals 'caller-owned argv' "$g_zxfer_shell_command_result"
	zxfer_print_backup_script_body </dev/null >"$l_body_output"
	assertEquals "An empty body succeeds without printing a line." 0 "$?"
	assertFalse "An empty body has no bytes." "[ -s '$l_body_output' ]"
}

test_remote_backup_protocol_renderers_match_readable_golden_output() {
	actual_script="$TEST_TMPDIR/remote_backup_protocol_scripts.actual"
	golden_script="$ZXFER_ROOT/tests/golden/remote_backup_protocol_scripts.golden"

	(
		g_zxfer_secure_path="/secure/bin:/usr/bin"
		g_cmd_cat="/bin/cat"
		printf '%s\n' "### prelude-with-symlink-guard"
		zxfer_build_remote_backup_script_prelude "target.example doas" \
			"/var/db/zxfer/tank/src/backup.meta" metadata id ls awk
		printf '%s\n' "### directory-prepare"
		zxfer_build_remote_backup_dir_prepare_cmd \
			"/var/db/zxfer/tank/src" "target.example doas"
		# The -T write program exactly as zxfer_write_backup_properties
		# composes and sends it.
		printf '%s\n' "### write"
		(
			g_option_T_target_host="target.example doas"
			g_backup_storage_root="/var/db/zxfer"
			g_initial_source="tank/src"
			g_initial_source_had_trailing_slash=1
			g_destination="backup/dst"
			g_backup_file_contents=".	compression=lz4=local"
			zxfer_get_backup_metadata_filename() { printf '%s\n' backup.meta; }
			zxfer_resolve_cli_command_safe() { g_zxfer_resolved_cli_command_result="cat"; }
			zxfer_run_remote_backup_script() { printf '%s\n' "$2"; }
			zxfer_write_backup_properties
		)
		printf '%s\n' "### read"
		zxfer_build_remote_backup_read_cmd \
			"/var/db/zxfer/tank/src/backup.meta" "origin.example doas"
		printf '%s\n' "### storage-listing"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_build_remote_backup_storage_listing_cmd "tank/src" "origin.example doas"
	) >"$actual_script"

	if ! cmp -s "$golden_script" "$actual_script"; then
		diff -u "$golden_script" "$actual_script" >&2
		fail "Readable remote backup protocol renderers drifted from their exact-output golden."
	fi
	assertTrue "Every rendered program is valid POSIX sh." \
		"grep -v '^###' '$actual_script' | sh -n"
}

test_remote_backup_renderers_treat_host_and_path_metacharacters_as_data() {
	marker="$TEST_TMPDIR/renderer_metachar_marker"
	host="target.example; touch $marker #"
	path="$TEST_TMPDIR_PHYSICAL/quote' dir;*/it's.meta"
	helper="$TEST_TMPDIR_PHYSICAL/quote' cat helper"
	cat >"$helper" <<'EOF'
#!/bin/sh
exec /bin/cat "$@"
EOF
	chmod +x "$helper"
	g_cmd_cat=$helper
	zxfer_render_shell_command_from_argv "$helper"
	cat_command=$g_zxfer_shell_command_result

	rendered=$(
		zxfer_build_remote_backup_dir_prepare_cmd "${path%/*}" "$host" mktemp mv rm &&
			zxfer_build_backup_pair_write_cmd "$path" "$path" tank/src "$cat_command"
	)
	assertContains "Hosts are single-quoted data inside the rendered program." \
		"$rendered" "'target.example; touch $marker #'"
	assertContains "Paths with quotes are escaped for the remote shell." \
		"$rendered" "'$TEST_TMPDIR_PHYSICAL/quote'\\'' dir;*/it'\\''s.meta'"
	printf 'quoted payload\n' | sh -c "$rendered"
	assertEquals 0 "$?"
	assertEquals "quoted payload" "$(cat "$path")"
	assertFalse "Host metacharacters never execute." "[ -e '$marker' ]"
	assertEquals "The read program handles the same quoting." \
		"quoted payload" "$(sh -c "$(zxfer_build_remote_backup_read_cmd "$path" "$host")")"
}

test_backup_listing_and_collapsed_programs_keep_quoted_arguments_and_stdin() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/store' with spaces"
	root="tank/child' with spaces"
	path="$g_backup_storage_root/$root/backup.meta"
	rendered=$(
		zxfer_build_remote_backup_dir_prepare_cmd "${path%/*}" "target.example doas" mktemp mv rm &&
			zxfer_build_backup_pair_write_cmd "$path" "$path" "$root" cat
	)
	# Dry-run rendering removes newlines; the same program must still parse
	# and read payload stdin instead of consuming it while initializing argv.
	collapsed=$(printf '%s\n' "$rendered" | tr '\n' ' ')
	printf 'collapsed payload\n' | sh -c "$collapsed"
	status=$?
	assertEquals "The collapsed program publishes its stdin." 0 "$status"
	assertEquals "collapsed payload" "$(cat "$path")"
	listing=$(sh -c "$(zxfer_build_remote_backup_storage_listing_cmd "$root" origin.example)")
	status=$?
	assertEquals "Quoted ancestor arguments survive the listing prefix." 0 "$status"
	assertEquals "The chain precedes find's root entry." "tank
$root
$root" "$listing"
}

zxfer_backup_test_check_remote_dir() {
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_option_T_target_host="backup@example.com"
	g_backup_storage_root=$1
	zxfer_check_backup_storage_dir_if_needed
}

test_ensure_local_backup_dir_allows_trusted_absolute_root_symlink_components() {
	zxfer_test_remote_host_fixture_setup || return
	target_dir=$(mktemp -d /tmp/zxfer-local-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	zxfer_ensure_local_backup_dir "$target_dir"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block local backup directory creation, which keeps default /var- or /tmp-backed paths working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the secure backup directory to be created under the symlink target." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

test_check_backup_storage_dir_if_needed_remote_handles_csh_remote_login_shell() {
	zxfer_test_remote_host_fixture_setup || return
	l_csh_shell=$(find_csh_shell_for_tests)
	if [ "$l_csh_shell" = "" ]; then
		return 0
	fi

	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_backup_csh_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_backup_csh_exec.log"
	target_dir="$TEST_TMPDIR_PHYSICAL/ensure_remote_backup_csh/child"
	create_fake_ssh_join_csh_exec_bin "$realistic_ssh_bin" "$l_csh_shell"

	g_cmd_ssh="$realistic_ssh_bin"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	zxfer_backup_test_check_remote_dir "$target_dir"
	status=$?
	unset FAKE_SSH_LOG

	assertEquals "Remote backup directory preparation should succeed through csh/tcsh login shells." \
		0 "$status"
	assertTrue "The csh/tcsh remote handoff should create the requested secure backup directory." \
		"[ -d \"$target_dir\" ]"
	assertEquals "The csh/tcsh backup handoff should receive one physical command line after the host line." \
		2 "$(sed -n '$=' "$realistic_ssh_log")"
}

test_check_backup_storage_dir_if_needed_remote_allows_trusted_absolute_root_symlink_components() {
	zxfer_test_remote_host_fixture_setup || return
	target_dir=$(mktemp -d /tmp/zxfer-remote-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	(
		zxfer_build_remote_sh_c_command() {
			g_zxfer_shell_command_result=$1
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_backup_test_check_remote_dir "$target_dir"
	)
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup directory preparation, which keeps default /var- or /tmp-backed remote roots working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the remote backup directory helper to create the requested directory." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_render_failures() {
	zxfer_test_remote_host_fixture_setup || return
	capture_case() {
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_render_remote_backup_dry_run_shell_command() {
			return 46
		}
		zxfer_check_backup_storage_dir_if_needed
	}
	zxfer_test_capture_subshell capture_case

	assertEquals "Remote dry-run backup preflight should preserve prepared renderer failures." \
		46 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_remote_backup_preview_retains_canonical_input_and_clears_failed_results() {
	zxfer_test_remote_host_fixture_setup || return
	for l_preview_builder_status in 0 37; do
		l_preview_output=$(
			zxfer_ssh_shell_command_for_host() {
				printf 'input=%s\n' "$3"
				g_zxfer_shell_command_result="prepared ssh"
				return "$l_preview_builder_status"
			}
			g_zxfer_shell_command_result="printf '%s' value;

printf '%s' end;"
			l_preview_status=0
			zxfer_render_remote_backup_dry_run_shell_command target.example \
				"$g_zxfer_shell_command_result" || l_preview_status=$?
			printf 'status=%s result=<%s>\n' "$l_preview_status" "$g_zxfer_shell_command_result"
		)
		l_preview_expected_result="prepared ssh"
		[ "$l_preview_builder_status" -eq 0 ] || l_preview_expected_result=""
		assertEquals "A preview keeps its input and only publishes successful rendering." \
			"input=printf '%s' value; printf '%s' end;
status=$l_preview_builder_status result=<$l_preview_expected_result>" "$l_preview_output"
	done
}

test_remote_backup_preview_clears_stale_results_for_an_empty_program() {
	zxfer_test_remote_host_fixture_setup || return
	g_zxfer_shell_command_result="stale preview"
	zxfer_render_remote_backup_dry_run_shell_command target.example "$ZXFER_LF"
	l_preview_empty_status=$?
	assertEquals "An empty preview keeps its failure status." 1 "$l_preview_empty_status"
	assertEquals "An empty preview publishes no stale command." "" "$g_zxfer_shell_command_result"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_builder_failures() {
	zxfer_test_remote_host_fixture_setup || return
	l_builder_log="$TEST_TMPDIR/remote_backup_dry_run_builder_failure.log"
	: >"$l_builder_log"

	capture_case() {
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_build_remote_backup_dir_prepare_cmd() {
			return 45
		}
		zxfer_render_remote_backup_dry_run_shell_command() {
			printf "%s\n" unexpected-renderer-call >>"$l_builder_log"
		}
		zxfer_check_backup_storage_dir_if_needed
	}
	zxfer_test_capture_subshell capture_case

	assertEquals "Remote dry-run backup preflight should preserve directory-script builder failures." \
		45 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A failed directory-script build must stop before remote rendering." \
		"" "$(cat "$l_builder_log")"
}

test_forwarded_backup_alias_memo_preserves_control_bytes_in_tmpdir() {
	l_ctrl_tmp="$TEST_TMPDIR/tmp$(printf '\t')with$(printf '\r')controls"
	mkdir -p "$l_ctrl_tmp"
	chmod 700 "$l_ctrl_tmp"
	l_backup_root="$TEST_TMPDIR/forwarded_ctrl_backup"
	mkdir -p "$l_backup_root/tank/src"
	chmod 700 "$l_backup_root" "$l_backup_root/tank" "$l_backup_root/tank/src"

	g_backup_file_extension=".zxfer_backup_info"
	l_alias_rel=$(zxfer_get_backup_metadata_filename "tank/src" "tank/src")
	l_alias_file="$l_backup_root/tank/src/$l_alias_rel"
	mkdir -p "$(dirname "$l_alias_file")"
	chmod 700 "$l_backup_root/tank/src/.zxfer_backup_info.v2" \
		"$l_backup_root/tank/src/.zxfer_backup_info.v2/h" \
		"$(dirname "$l_alias_file")"
	cat >"$l_alias_file" <<'EOF'
#zxfer property backup file
#format_version:2
#source_root:tank/src
#destination_root:tank/src
.	compression=lz4=local
child	compression=zstd=local
EOF
	chmod 600 "$l_alias_file"

	l_output=$(
		TMPDIR="$l_ctrl_tmp"
		export TMPDIR
		zxfer_discard_runtime_cleanup_state
		zxfer_reset_path_security_state
		zxfer_reset_backup_metadata_state
		g_backup_storage_root=$l_backup_root
		g_option_O_origin_host=""
		zxfer_ensure_run_tmp_root || exit 91
		zxfer_resolve_forwarded_backup_metadata "tank/src" || exit 92
		l_root_props=$g_zxfer_backup_forwarded_properties
		zxfer_resolve_forwarded_backup_metadata "tank/src/child" || exit 93
		l_child_props=$g_zxfer_backup_forwarded_properties
		rm -rf "$g_zxfer_run_tmp_root"
		printf '%s|%s\n' "$l_root_props" "$l_child_props"
	)
	l_status=$?
	rm -rf "$l_ctrl_tmp"

	assertEquals "Forwarded alias lookup and memo hit must succeed under a TMPDIR with TAB/CR." \
		0 "$l_status"
	assertEquals "Both uncached and memoized forwarded lookups must return exact property payloads." \
		"compression=lz4=local|compression=zstd=local" "$l_output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
