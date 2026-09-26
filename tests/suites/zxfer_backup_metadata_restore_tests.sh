#!/bin/sh
# -e restore lookup, candidate selection, and local read protection tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Render current-format contents for the tank/src -> backup/dst/src pair.
zxfer_backup_test_exact_pair_contents() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst/src"
	zxfer_test_render_current_backup_metadata_contents "$@"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
}

# Run zxfer_get_backup_properties in a subshell and capture stderr plus the
# restored contents on success.
zxfer_backup_test_run_restore() {
	(
		zxfer_get_backup_properties
		printf 'restored=%s\n' "$g_restored_backup_file_contents"
	) 2>&1
}

test_get_backup_properties_reads_exact_pair_file_and_serves_relative_rows() {
	zxfer_backup_test_use_private_root restore_exact
	contents=$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "atime=off=local")")
	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$contents"

	output=$(zxfer_backup_test_run_restore)
	assertEquals "The exact-pair file is accepted." 0 "$?"
	assertContains "$output" "restored=$contents"
	assertEquals "Child datasets restore through relative rows of the one file." \
		"atime=off=local" "$(zxfer_backup_metadata_extract_properties_for_dataset_pair \
			"$contents" tank/src/child backup/dst/src/child)"
}

test_get_backup_properties_reads_retired_cksum_filename_read_only_when_current_file_is_absent() {
	zxfer_backup_test_use_private_root restore_legacy
	contents=$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	# Retired writers named the pair by cksum of "tank/src<LF>backup/dst".
	legacy_file="$g_backup_storage_root/tank/src/.zxfer_backup_info.src.k1537737481.19"
	zxfer_backup_test_write_file "$legacy_file" "$contents"

	output=$(zxfer_backup_test_run_restore)
	assertEquals "The retired cksum-keyed filename still restores." 0 "$?"
	assertContains "$output" "restored=$contents"
	assertFalse "Restore never writes the current filename." "[ -e '$BACKUP_TEST_PRIMARY_FILE' ]"
}

test_get_backup_properties_reports_missing_file_with_usage_error_before_any_dataset_work() {
	zxfer_backup_test_use_private_root restore_missing
	output=$(zxfer_backup_test_run_restore)
	assertEquals "Missing metadata is a usage error." 1 "$?"
	assertContains "$output" "Cannot find backup property file. Ensure that it"
	assertContains "$output" "exists under the source-dataset-relative tree inside ZXFER_BACKUP_DIR."
}

test_get_backup_properties_ignores_legacy_layouts_and_ancestor_files() {
	zxfer_backup_test_use_private_root restore_layouts
	contents=$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	# Retired mountpoint-local and tail-only names inside the tree, a v1
	# comma row file at the old flat path, and an exact-pair file for the
	# parent dataset must all be ignored: only the exact path counts.
	zxfer_backup_test_write_file "$TEST_TMPDIR_PHYSICAL/restore_layouts_mount/.zxfer_backup_info.src" "$contents"
	zxfer_backup_test_write_file "$g_backup_storage_root/tank/src/.zxfer_backup_info.src" "$contents"
	zxfer_backup_test_write_file "$g_backup_storage_root/tank/src/.zxfer_backup_info.v2" "$contents"
	zxfer_backup_test_write_file \
		"$g_backup_storage_root/tank/$(zxfer_get_backup_metadata_filename tank backup/dst)" \
		"$contents"

	output=$(zxfer_backup_test_run_restore)
	assertEquals 1 "$?"
	assertContains "$output" "Cannot find backup property file."
}

test_get_backup_properties_rejects_missing_header_unsupported_version_and_malformed_rows() {
	zxfer_backup_test_use_private_root restore_invalid
	good_row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "#format_version:2
#source_root:tank/src
#destination_root:backup/dst/src
$good_row"
	output=$(zxfer_backup_test_run_restore)
	assertEquals 1 "$?"
	assertContains "$output" "Backup property file $BACKUP_TEST_PRIMARY_FILE does not start with the required zxfer backup metadata header."

	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents "$good_row" |
		sed 's/^#format_version:2$/#format_version:999/')"
	output=$(zxfer_backup_test_run_restore)
	assertEquals 1 "$?"
	assertContains "$output" "Backup property file $BACKUP_TEST_PRIMARY_FILE does not declare supported zxfer backup metadata format version #format_version:2."

	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents "$good_row" "tank/src,backup/dst,legacy=row")"
	output=$(zxfer_backup_test_run_restore)
	assertEquals 1 "$?"
	assertContains "$output" "Backup property file $BACKUP_TEST_PRIMARY_FILE is malformed. Expected current-format relative-path and properties rows."
}

test_get_backup_properties_rejects_ambiguous_rows_and_files_without_the_root_row() {
	zxfer_backup_test_use_private_root restore_rows
	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "." "a=1=local")" "$(zxfer_test_backup_metadata_row "." "a=2=local")")"
	output=$(zxfer_backup_test_run_restore)
	assertEquals 1 "$?"
	assertContains "$output" "Backup property file $BACKUP_TEST_PRIMARY_FILE contains multiple relative rows for source dataset tank/src."

	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "child" "a=1=local")")"
	output=$(zxfer_backup_test_run_restore)
	assertEquals 1 "$?"
	assertContains "$output" "Backup property file $BACKUP_TEST_PRIMARY_FILE does not contain a current-format relative row for source dataset tank/src."

	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/other"
	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "a=1=local")")"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
	output=$(zxfer_backup_test_run_restore)
	assertEquals "A file recorded for another destination root never restores this pair." 1 "$?"
	assertContains "$output" "does not contain a current-format relative row for source dataset tank/src."
}

test_get_backup_properties_reads_the_exact_pair_file_through_the_origin_host() {
	zxfer_backup_test_use_private_root restore_remote
	contents=$(zxfer_backup_test_exact_pair_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	zxfer_backup_test_write_file "$BACKUP_TEST_PRIMARY_FILE" "$contents"
	g_option_O_origin_host="origin.example"
	g_cmd_cat="/bin/cat"

	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() { sh -c "$2"; }
			zxfer_get_backup_properties
			printf 'restored=%s\n' "$g_restored_backup_file_contents"
		) 2>&1
	)
	assertEquals "The origin-side read program returns the file." 0 "$?"
	assertContains "$output" "restored=$contents"

	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() { return 94; }
			zxfer_get_backup_properties
		) 2>&1
	)
	assertEquals "A remote missing status is the usage error, not a transport failure." 1 "$?"
	assertContains "$output" "Cannot find backup property file."

	empty_dir="$TEST_TMPDIR/restore_remote_empty_bin"
	mkdir -p "$empty_dir"
	output=$(
		(
			g_zxfer_secure_path="$empty_dir"
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() { sh -c "$2"; }
			zxfer_throw_error() {
				# The reader runs with stdout redirected; report on stderr
				# like the real helper does.
				printf 'class=%s %s\n' "${g_zxfer_failure_class:-}" "$1" >&2
				exit 1
			}
			zxfer_get_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "Missing remote helpers are dependency failures with the remote precheck text." \
		"$output" "Required dependency \"id\" not found on host origin.example in secure PATH ($empty_dir)."
	assertContains "$output" "class=dependency Required remote backup-metadata helper dependency not found on host origin.example in secure PATH ($empty_dir)."
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
		"2|usage|1|Backup property file /p contains multiple relative rows for source dataset tank/src." \
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
