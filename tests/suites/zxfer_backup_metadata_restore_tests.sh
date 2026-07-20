#!/bin/sh
# Backup metadata restore-selection and secure-read behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_try_backup_restore_candidate_set_stops_when_legacy_name_matches_current_name() {
	output=$(
		(
			zxfer_get_backup_metadata_filename() {
				printf '%s\n' "same-name"
			}
			zxfer_get_legacy_backup_metadata_filename() {
				printf '%s\n' "same-name"
			}
			zxfer_try_backup_restore_candidate() {
				printf 'probe=%s\n' "$1"
				return 1
			}

			set +e
			zxfer_try_backup_restore_candidate_set \
				"/backup" "tank/src" "backup/dst" "tank/src" "backup/dst"
			printf 'status=%s\n' "$?"
			printf 'candidate=%s\n' "$g_zxfer_backup_restore_candidate_path_result"
		)
	)

	assertContains "Restore-candidate fallback should probe the current filename before considering legacy aliases." \
		"$output" "probe=/backup/same-name"
	assertContains "Restore-candidate fallback should stop with a plain not-found status when the legacy filename is identical." \
		"$output" "status=1"
	assertContains "Restore-candidate fallback should leave the current candidate path selected when no distinct legacy alias exists." \
		"$output" "candidate=/backup/same-name"
}

test_try_backup_restore_candidate_maps_remote_transport_failures_to_transport_status() {
	set +e
	output=$(
		(
			zxfer_read_remote_backup_file() {
				printf '%s\n' "Permission denied (publickey)." >&2
				return 6
			}
			zxfer_try_backup_restore_candidate "/tmp/backup.meta" "tank/src" "backup/dst" "backup@example.com" source >/dev/null
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	set -e

	assertContains "Remote restore-candidate transport failures should preserve the ssh diagnostic from the exact keyed probe." \
		"$output" "Permission denied (publickey)."
	assertContains "Remote restore-candidate transport failures should map to the dedicated transport-validation status." \
		"$output" "status=8"
}

test_try_backup_restore_candidate_maps_remote_capture_failures_to_capture_status() {
	set +e
	output=$(
		(
			zxfer_read_remote_backup_file() {
				g_zxfer_remote_probe_stderr="Failed to read remote probe stderr capture from local staging."
				return 7
			}
			zxfer_try_backup_restore_candidate "/tmp/backup.meta" "tank/src" "backup/dst" "backup@example.com" source >/dev/null
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	set -e

	assertContains "Remote restore-candidate capture failures should map to the dedicated capture-validation status." \
		"$output" "status=9"
}

test_try_backup_restore_candidate_maps_unexpected_format_validation_status_to_generic_failure() {
	set +e
	output=$(
		(
			zxfer_read_local_backup_file() {
				g_zxfer_backup_file_read_result="#header"
				return 0
			}
			zxfer_validate_backup_metadata_format() {
				return 42
			}
			zxfer_try_backup_restore_candidate "/tmp/backup.meta" "tank/src" "backup/dst" >/dev/null
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	set -e

	assertContains "Restore-candidate validation should map unexpected metadata-format helper failures to the generic unreadable-candidate status." \
		"$output" "status=5"
}

test_try_backup_restore_candidate_maps_unexpected_match_status_to_generic_failure() {
	set +e
	output=$(
		(
			zxfer_read_local_backup_file() {
				g_zxfer_backup_file_read_result="#header"
				return 0
			}
			zxfer_validate_backup_metadata_format() {
				return 0
			}
			zxfer_backup_metadata_matches_source() {
				return 42
			}
			zxfer_try_backup_restore_candidate "/tmp/backup.meta" "tank/src" "backup/dst" >/dev/null
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	set -e

	assertContains "Restore-candidate validation should map unexpected source-match helper failures to the generic unreadable-candidate status." \
		"$output" "status=5"
}

test_try_backup_restore_candidate_uses_current_shell_read_scratch_for_remote_reads() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	backup_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	set +e
	output=$(
		(
			zxfer_read_remote_backup_file() {
				g_zxfer_backup_file_read_result=$backup_contents
				printf '%s\n' "invalid stdout payload"
				return 0
			}
			zxfer_try_backup_restore_candidate "/tmp/backup.meta" "tank/src" "backup/dst" "backup@example.com" source >/dev/null
			printf 'status=%s\n' "$?"
			printf 'restored=%s\n' "$g_restored_backup_file_contents"
		) 2>&1
	)
	set -e

	assertContains "Remote restore-candidate reads should succeed when the current-shell scratch contains valid backup metadata even if the helper stdout is ignored." \
		"$output" "status=0"
	assertContains "Remote restore-candidate reads should validate the scratch backup payload rather than stdout-only helper output." \
		"$output" "restored=$backup_contents"
}

test_get_backup_properties_reports_filename_derivation_failure() {
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_option_O_origin_host=""

	set +e
	output=$(
		(
			zxfer_get_backup_metadata_filename() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_backup_properties
		)
	)
	status=$?

	assertEquals "Restore-mode lookup should fail closed when the keyed backup filename cannot be derived." \
		1 "$status"
	assertContains "Filename-derivation failures should identify the source dataset that could not be keyed." \
		"$output" "Failed to derive backup metadata filename for source dataset [tank/src]."
}

test_get_backup_properties_reads_legacy_cksum_filename_when_current_file_is_missing_locally() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/legacy_cksum_exact_store"
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	backup_dir="$g_backup_storage_root/tank/src"
	current_file="$backup_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	legacy_file="$backup_dir/$(zxfer_get_legacy_backup_metadata_filename "$g_initial_source" "$g_destination")"
	mkdir -p "$backup_dir"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" >"$legacy_file"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$legacy_file"

	zxfer_get_backup_properties

	assertFalse "The compatibility restore test should exercise a missing current lossless-key path." \
		"[ -e \"$current_file\" ]"
	assertEquals "Restore lookup should remember the legacy candidate path that satisfied the read." \
		"$legacy_file" "$g_zxfer_backup_restore_candidate_path_result"
	assertContains "Restore lookup should load current-format metadata from the retired cksum filename when no current filename exists." \
		"$g_restored_backup_file_contents" ".	compression=lz4=local"
}

test_get_backup_properties_reads_legacy_cksum_filename_when_current_file_is_missing_remotely() {
	g_backup_storage_root="$TEST_TMPDIR/legacy_cksum_remote_store"
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	backup_dir="$g_backup_storage_root/tank/src"
	current_file="$backup_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	legacy_file="$backup_dir/$(zxfer_get_legacy_backup_metadata_filename "$g_initial_source" "$g_destination")"
	read_log="$TEST_TMPDIR/legacy_cksum_remote_reads.log"

	zxfer_read_remote_backup_file() {
		printf '%s\n' "$2" >>"$read_log"
		if [ "$2" = "$legacy_file" ]; then
			ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
			ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
			g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
				"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
			unset ZXFER_TEST_BACKUP_SOURCE_ROOT
			unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
			return 0
		fi
		return 4
	}

	zxfer_get_backup_properties

	assertContains "Remote restore lookup should try the current lossless-key path before the legacy cksum fallback." \
		"$(cat "$read_log")" "$current_file"
	assertContains "Remote restore lookup should try the retired cksum filename when the current file is absent." \
		"$(cat "$read_log")" "$legacy_file"
	assertEquals "Remote restore lookup should remember the legacy candidate path that satisfied the read." \
		"$legacy_file" "$g_zxfer_backup_restore_candidate_path_result"
	assertContains "Remote restore lookup should load current-format metadata from the retired cksum filename." \
		"$g_restored_backup_file_contents" ".	compression=lz4=local"
}

test_get_backup_properties_rejects_legacy_local_mountpoint_metadata_layout() {
	mount_dir="$TEST_TMPDIR_PHYSICAL/legacy_mount"
	mkdir -p "$mount_dir"
	legacy_backup="$mount_dir/.zxfer_backup_info.child"
	stdout_file="$TEST_TMPDIR/legacy_backup_local.out"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$legacy_backup"
	chmod 600 "$legacy_backup"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Legacy live-mountpoint backup metadata should now fail closed instead of being restored." 1 "$status"
	assertContains "Legacy live-mountpoint backup metadata should degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_rejects_tail_only_backup_filename_in_secure_tree() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/tail_only_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	tail_only_dir="$g_backup_storage_root/tank/src/child"
	tail_only_file="$tail_only_dir/.zxfer_backup_info.child"
	stdout_file="$TEST_TMPDIR/tail_only_restore.out"
	mkdir -p "$tail_only_dir"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$tail_only_file"
	chmod 600 "$tail_only_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Tail-only secure backup filenames should now fail closed instead of being recovered as compatibility candidates." \
		1 "$status"
	assertContains "Tail-only secure backup filename restores should use the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_rejects_broad_backup_root_fallback_scans() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/fallback_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	fallback_dir="$g_backup_storage_root/unexpected/layout"
	fallback_file="$fallback_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/broad_fallback_restore.out"
	zxfer_test_ensure_parent_dir "$fallback_file"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$fallback_file"
	chmod 600 "$fallback_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Backup restore should not search unrelated locations under ZXFER_BACKUP_DIR for matching metadata." \
		1 "$status"
	assertContains "Broad backup-root fallback scans should now degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_rejects_legacy_sanitized_mountpoint_backup_layout() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/legacy_sanitized_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	legacy_secure_dir="$g_backup_storage_root/mnt/foo_bar"
	legacy_backup="$legacy_secure_dir/.zxfer_backup_info.child"
	stdout_file="$TEST_TMPDIR/legacy_sanitized_restore.out"
	mkdir -p "$legacy_secure_dir"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$legacy_backup"
	chmod 600 "$legacy_backup"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Legacy sanitized mountpoint compatibility layouts should now fail closed instead of restoring metadata." \
		1 "$status"
	assertContains "Legacy sanitized mountpoint layouts should degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_rejects_exact_secure_file_without_matching_entry() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_nonmatching_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_nonmatching_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src/child"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/other"
	zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" >"$direct_file"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$direct_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Exact secure backup files that lack the requested relative row should fail closed instead of falling back to ancestors." \
		1 "$status"
	assertContains "Non-matching exact secure backup files should identify the exact keyed backup path." \
		"$output" "$direct_file"
	assertContains "Non-matching exact secure backup files should report the missing relative row." \
		"$output" "does not contain a current-format relative row for source dataset tank/src/child."
}

test_get_backup_properties_rejects_exact_secure_file_without_required_header() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_missing_header_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_missing_header_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$direct_file"
	chmod 600 "$direct_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Exact secure backup files without the required metadata header should fail closed before restore matching." \
		1 "$status"
	assertContains "Missing-header exact secure backup failures should identify the exact keyed backup path." \
		"$output" "$direct_file"
	assertContains "Missing-header exact secure backup failures should explain that the file must start with the required header." \
		"$output" "does not start with the required zxfer backup metadata header."
}

test_get_backup_properties_rejects_exact_secure_file_with_content_before_header() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_misordered_header_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_misordered_header_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	printf '%s\n%s\n%s\n%s\n%s\n' \
		"#legacy comment" \
		"#zxfer property backup file" \
		"#format_version:2" \
		"#version:test-version" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" >"$direct_file"
	chmod 600 "$direct_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Exact secure backup files with content before the zxfer header should fail closed before restore matching." \
		1 "$status"
	assertContains "Misordered-header exact secure backup failures should identify the exact keyed backup path." \
		"$output" "$direct_file"
	assertContains "Misordered-header exact secure backup failures should explain that the file must start with the zxfer header." \
		"$output" "does not start with the required zxfer backup metadata header."
}

test_get_backup_properties_rejects_exact_secure_file_with_unsupported_format_version() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_bad_format_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_bad_format_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	printf '%s\n%s\n%s\n%s\n' \
		"#zxfer property backup file" \
		"#format_version:999" \
		"#version:test-version" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" >"$direct_file"
	chmod 600 "$direct_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Exact secure backup files with unsupported metadata schema versions should fail closed before restore matching." \
		1 "$status"
	assertContains "Unsupported-format exact secure backup failures should identify the exact keyed backup path." \
		"$output" "$direct_file"
	assertContains "Unsupported-format exact secure backup failures should identify the expected schema marker." \
		"$output" "does not declare supported zxfer backup metadata format version #format_version:2."
}

test_get_backup_properties_rejects_malformed_exact_secure_file() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_malformed_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_malformed_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src/child"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	zxfer_test_render_current_backup_metadata_contents \
		"broken,row-without-tab" >"$direct_file"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$direct_file"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Malformed exact secure backup files should fail closed instead of degrading into missing-backup handling." \
		1 "$status"
	assertContains "Malformed exact secure backup files should identify the exact keyed backup path." \
		"$output" "$direct_file"
	assertContains "Malformed exact secure backup files should report the current-format parse expectation." \
		"$output" "is malformed. Expected current-format relative-path and properties rows."
}

test_get_backup_properties_rejects_ambiguous_relative_rows_in_direct_local_candidate() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_ambiguous_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_ambiguous_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src/child"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")" >"$direct_file"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$direct_file"

	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "/mnt/backups"
	}

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct secure backup candidates should fail closed when they contain duplicate relative rows." 1 "$status"
	assertContains "Direct local candidate failures should identify the exact secure backup file." \
		"$output" "$direct_file"
	assertContains "Direct local candidate failures should identify the ambiguous source dataset." \
		"$output" "contains multiple relative rows for source dataset tank/src/child."
}

test_get_backup_properties_rejects_ambiguous_relative_rows_in_direct_remote_candidate() {
	g_backup_storage_root="$TEST_TMPDIR/direct_remote_ambiguous_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	direct_file="$g_backup_storage_root/tank/src/child/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_ambiguous_remote.out"

	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "/mnt/backups"
	}

	zxfer_read_remote_backup_file() {
		if [ "$2" = "$direct_file" ]; then
			ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src/child"
			ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
			g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
				"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
				"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
			unset ZXFER_TEST_BACKUP_SOURCE_ROOT
			unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
			return 0
		fi
		return 1
	}

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct remote secure backup candidates should fail closed when they contain duplicate relative rows." 1 "$status"
	assertContains "Direct remote candidate failures should identify the exact secure backup file." \
		"$output" "$direct_file"
	assertContains "Direct remote candidate failures should identify the ambiguous source dataset." \
		"$output" "contains multiple relative rows for source dataset tank/src/child."
}

test_get_backup_properties_does_not_walk_up_to_parent_filesystem() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/ancestor_store"
	g_backup_file_extension=".zxfer_backup_info"
	parent_secure_dir="$g_backup_storage_root/tank/parent"
	parent_backup="$parent_secure_dir/$(zxfer_get_backup_metadata_filename "tank/parent" "backup/dst")"
	zxfer_test_ensure_parent_dir "$parent_backup"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/parent"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "child" "compression=lz4=local")" >"$parent_backup"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$parent_backup"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""

	set +e
	output=$(
		(
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_backup_properties
		)
	)
	status=$?
	set -e

	assertEquals "Backup-property discovery should require the exact current metadata file instead of walking to ancestor datasets." \
		1 "$status"
	assertContains "Ancestor-only metadata should degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_does_not_fallback_when_direct_local_read_fails() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_read_error_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/parent/child"
	parent_dir="$g_backup_storage_root/tank/parent"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$parent_dir/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_read_error_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	zxfer_test_ensure_parent_dir "$parent_file"
	printf '%s\n' "child-placeholder" >"$direct_file"
	printf '%s\n' "tank/parent/child,backup/dst,compression=lz4" >"$parent_file"
	chmod 600 "$direct_file" "$parent_file"

	set +e
	(
		zxfer_read_local_backup_file() {
			if [ "$1" = "$direct_file" ]; then
				return 5
			fi
			g_zxfer_backup_file_read_result=$(cat "$1")
			printf '%s' "$g_zxfer_backup_file_read_result"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct keyed local backup read failures should abort instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Direct keyed local backup read failures should identify the unreadable exact backup path." \
		"$output" "Failed to read backup property file $direct_file."
}

test_get_backup_properties_reports_local_stage_read_failures_distinctly() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_stage_read_error_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/parent/child"
	parent_dir="$g_backup_storage_root/tank/parent"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$parent_dir/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_stage_read_error_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	zxfer_test_ensure_parent_dir "$parent_file"
	printf '%s\n' "child-placeholder" >"$direct_file"
	printf '%s\n' "tank/parent/child,backup/dst,compression=lz4" >"$parent_file"
	chmod 600 "$direct_file" "$parent_file"

	set +e
	(
		zxfer_read_local_backup_file() {
			if [ "$1" = "$direct_file" ]; then
				g_zxfer_backup_local_read_failure_result=staging
				return 71
			fi
			g_zxfer_backup_file_read_result=$(cat "$1")
			printf '%s' "$g_zxfer_backup_file_read_result"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct keyed local backup staging failures should abort instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Direct keyed local backup staging failures should identify the exact backup path." \
		"$output" "Failed to stage local backup property file $direct_file for secure read."
}

test_get_backup_properties_rejects_insecure_exact_local_backup_file_without_ancestor_fallback() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_insecure_local_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/parent/child"
	parent_dir="$g_backup_storage_root/tank/parent"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$parent_dir/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_insecure_local.out"
	zxfer_test_ensure_parent_dir "$direct_file"
	zxfer_test_ensure_parent_dir "$parent_file"
	printf '%s\n' "tank/parent/child,backup/dst,compression=lz4" >"$direct_file"
	printf '%s\n' "tank/parent/child,backup/dst,compression=inherit" >"$parent_file"
	chmod 644 "$direct_file"
	chmod 600 "$parent_file"

	set +e
	(
		zxfer_throw_error() {
			printf '%s\n' "$1" >&2
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Insecure direct keyed local backup metadata should fail closed instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Insecure direct keyed local backup metadata should identify the exact backup path." \
		"$output" "$direct_file"
	assertContains "Insecure direct keyed local backup metadata should fail through the secure metadata guard instead of a generic missing-backup path." \
		"$output" "Refusing to use backup metadata $direct_file"
}

test_get_backup_properties_rejects_remote_legacy_mountpoint_metadata_layout() {
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/remote_backup_store"
	legacy_backup="/mnt/remote/.zxfer_backup_info.child"
	stdout_file="$TEST_TMPDIR/remote_legacy_backup.out"

	zxfer_read_remote_backup_file() {
		if [ "$2" = "$legacy_backup" ]; then
			g_zxfer_backup_file_read_result="tank/src/child,backup/dst,compression=lz4"
			printf '%s\n' "$g_zxfer_backup_file_read_result"
			return 0
		fi
		return 4
	}

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Remote legacy live-mountpoint backup metadata should now fail closed instead of being restored." \
		1 "$status"
	assertContains "Remote legacy live-mountpoint backup metadata should degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_does_not_read_remote_ancestor_dataset_tree() {
	g_backup_storage_root="$TEST_TMPDIR/remote_ancestor_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	parent_backup="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "backup/dst")"

	set +e
	output=$(
		(
			zxfer_read_remote_backup_file() {
				if [ "$2" = "$parent_backup" ]; then
					g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
						"$(zxfer_test_backup_metadata_row "child" "compression=lz4=local")")
					return 0
				fi
				return 4
			}
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_backup_properties
		)
	)
	status=$?
	set -e

	assertEquals "Remote backup-property discovery should require the exact current metadata file instead of walking to ancestor datasets." \
		1 "$status"
	assertContains "Remote ancestor-only metadata should degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_does_not_fallback_when_direct_remote_read_fails() {
	g_backup_storage_root="$TEST_TMPDIR/remote_direct_read_error_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	direct_file="$g_backup_storage_root/tank/parent/child/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_read_error_remote.out"

	set +e
	(
		zxfer_read_remote_backup_file() {
			case "$2" in
			"$direct_file")
				return 5
				;;
			"$parent_file")
				g_zxfer_backup_file_read_result="tank/parent/child,backup/dst,compression=lz4"
				printf '%s\n' "$g_zxfer_backup_file_read_result"
				return 0
				;;
			esac
			return 1
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct keyed remote backup read failures should abort instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Direct keyed remote backup read failures should identify the unreadable exact backup path." \
		"$output" "Failed to read backup property file $direct_file."
}

test_get_backup_properties_does_not_fallback_when_direct_remote_transport_fails() {
	g_backup_storage_root="$TEST_TMPDIR/remote_direct_transport_error_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	direct_file="$g_backup_storage_root/tank/parent/child/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_transport_error_remote.out"

	set +e
	(
		zxfer_read_remote_backup_file() {
			case "$2" in
			"$direct_file")
				printf '%s\n' "Host key verification failed." >&2
				return 6
				;;
			"$parent_file")
				g_zxfer_backup_file_read_result="tank/parent/child,backup/dst,compression=lz4"
				printf '%s\n' "$g_zxfer_backup_file_read_result"
				return 0
				;;
			esac
			return 1
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct keyed remote transport failures should abort instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Direct keyed remote transport failures should preserve the ssh diagnostic from the exact keyed probe." \
		"$output" "Host key verification failed."
	assertContains "Direct keyed remote transport failures should identify the unreadable exact backup path and host." \
		"$output" "Failed to contact source host backup@example.com while reading backup property file $direct_file."
}

test_get_backup_properties_does_not_fallback_when_direct_remote_capture_fails() {
	g_backup_storage_root="$TEST_TMPDIR/remote_direct_capture_error_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	direct_file="$g_backup_storage_root/tank/parent/child/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_capture_error_remote.out"

	set +e
	(
		zxfer_read_remote_backup_file() {
			case "$2" in
			"$direct_file")
				g_zxfer_remote_probe_stderr="Failed to read remote probe stderr capture from local staging."
				return 7
				;;
			"$parent_file")
				g_zxfer_backup_file_read_result="tank/parent/child,backup/dst,compression=lz4"
				printf '%s\n' "$g_zxfer_backup_file_read_result"
				return 0
				;;
			esac
			return 1
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct keyed remote capture failures should abort instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Direct keyed remote capture failures should preserve the staged-capture diagnostic from the exact keyed probe." \
		"$output" "Failed to read remote probe stderr capture from local staging."
	assertContains "Direct keyed remote capture failures should identify the exact backup path and host." \
		"$output" "Failed to reload local remote helper capture while reading backup property file $direct_file on host backup@example.com."
}

test_get_backup_properties_rejects_insecure_exact_remote_backup_file_without_ancestor_fallback() {
	g_backup_storage_root="$TEST_TMPDIR/remote_insecure_exact_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	direct_file="$g_backup_storage_root/tank/parent/child/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_insecure_remote.out"

	set +e
	(
		# This restore-selection case classifies the exact requested path; keep
		# transport chunking out of its string-matching stand-in.
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			case "$2" in
			*"$direct_file"*)
				return 96
				;;
			*"$parent_file"*)
				printf '%s\n' "tank/parent/child,backup/dst,compression=lz4"
				return 0
				;;
			esac
			return 94
		}
		zxfer_throw_error() {
			printf '%s\n' "$1" >&2
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Insecure direct keyed remote backup metadata should fail closed instead of falling back to ancestor metadata." \
		1 "$status"
	assertContains "Insecure direct keyed remote backup metadata should identify the exact backup path." \
		"$output" "$direct_file"
	assertContains "Insecure direct keyed remote backup metadata should fail through the secure metadata guard instead of a generic read failure." \
		"$output" "Refusing to use backup metadata $direct_file on backup@example.com"
}

test_get_backup_properties_rejects_remote_read_dependency_failures_without_generic_collapse() {
	g_backup_storage_root="$TEST_TMPDIR/remote_dependency_exact_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	direct_file="$g_backup_storage_root/tank/parent/child/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	parent_file="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_dependency_remote.out"

	set +e
	(
		zxfer_read_remote_backup_file() {
			case "$2" in
			"$direct_file")
				zxfer_throw_error "Required remote backup-metadata helper dependency not found on host backup@example.com in secure PATH (/tmp/secure-path). Review prior stderr for the missing tool name."
				;;
			"$parent_file")
				g_zxfer_backup_file_read_result="tank/parent/child,backup/dst,compression=lz4"
				printf '%s\n' "$g_zxfer_backup_file_read_result"
				return 0
				;;
			esac
			return 4
		}
		zxfer_throw_error() {
			printf '%s\n' "$1" >&2
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Direct keyed remote dependency failures should abort restore lookup instead of falling back or collapsing into a generic read error." \
		1 "$status"
	assertContains "Direct keyed remote dependency failures should preserve the exact dependency error from the keyed probe." \
		"$output" "Required remote backup-metadata helper dependency not found on host backup@example.com in secure PATH (/tmp/secure-path)."
	assertNotContains "Direct keyed remote dependency failures should not be rewritten into the generic unreadable-backup message." \
		"$output" "Failed to read backup property file $direct_file."
}

test_get_backup_properties_rejects_remote_broad_backup_root_fallback_scans() {
	g_backup_storage_root="$TEST_TMPDIR/remote_fallback_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	fallback_file="$g_backup_storage_root/layout/one/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/remote_broad_fallback_restore.out"

	zxfer_read_remote_backup_file() {
		if [ "$2" = "$fallback_file" ]; then
			g_zxfer_backup_file_read_result="tank/src/child,backup/dst,compression=lz4"
			printf '%s\n' "$g_zxfer_backup_file_read_result"
			return 0
		fi
		return 4
	}

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Remote restore should not search unrelated locations under ZXFER_BACKUP_DIR for matching metadata." \
		1 "$status"
	assertContains "Remote broad backup-root fallback scans should now degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_rejects_raw_mountpoint_compatibility_layout() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/raw_mount_fallback_store"
	fallback_dir="$g_backup_storage_root/mnt/safe"
	mkdir -p "$fallback_dir"
	fallback_file="$fallback_dir/.zxfer_backup_info.child"
	stdout_file="$TEST_TMPDIR/raw_mount_compat_restore.out"
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4" >"$fallback_file"
	chmod 600 "$fallback_file"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"

	set +e
	(
		zxfer_throw_error_with_usage() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_get_backup_properties
	) >"$stdout_file" 2>&1
	status=$?
	output=$(cat "$stdout_file")

	assertEquals "Raw mountpoint compatibility layouts should now fail closed instead of being canonicalized and restored." \
		1 "$status"
	assertContains "Raw mountpoint compatibility layouts should degrade into the documented missing-backup error." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_get_backup_properties_reports_missing_backup_file() {
	g_initial_source="tank"
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/missing_store"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "-"
			}
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_backup_properties
		)
	)
	status=$?

	assertEquals "Missing backup metadata should abort with an error." 1 "$status"
	assertContains "Missing backup metadata should use the documented guidance." \
		"$output" "Cannot find backup property file. Ensure that it"
}

test_check_secure_backup_file_reports_unknown_owner_and_mode() {
	backup_file="$TEST_TMPDIR/secure_meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	owner_output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_check_secure_backup_file "$backup_file"
		)
	)
	owner_status=$?

	mode_output=$(
		(
			zxfer_get_path_owner_uid() {
				printf '%s\n' "0"
			}
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_check_secure_backup_file "$backup_file"
		)
	)
	mode_status=$?

	assertEquals "Unknown backup-file owners should be rejected." 1 "$owner_status"
	assertContains "Unknown owner failures should mention the metadata path." \
		"$owner_output" "Cannot determine the owner of backup metadata $backup_file."
	assertEquals "Unknown backup-file permissions should be rejected." 1 "$mode_status"
	assertContains "Unknown mode failures should mention the metadata path." \
		"$mode_output" "Cannot determine the permissions for backup metadata $backup_file."
}

test_check_secure_backup_file_rejects_non_0600_permissions() {
	backup_file="$TEST_TMPDIR/insecure_meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 644 "$backup_file"

	set +e
	output=$(zxfer_check_secure_backup_file "$backup_file")
	status=$?

	assertEquals "Non-0600 backup metadata should be rejected." 1 "$status"
	assertContains "Non-0600 backup metadata failures should identify the observed mode." \
		"$output" "Refusing to use backup metadata $backup_file because its permissions (644) are not 0600."
}

test_require_backup_write_target_path_rejects_symlink_target() {
	real_file="$TEST_TMPDIR/backup_write_real.meta"
	link_file="$TEST_TMPDIR/backup_write_link.meta"
	: >"$real_file"
	ln -s "$real_file" "$link_file"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_require_backup_write_target_path "$link_file"
		)
	)
	status=$?

	assertEquals "Backup-write target validation should reject symlink targets." 1 "$status"
	assertContains "Symlinked backup-write targets should identify the exact path." \
		"$output" "Refusing to write backup metadata $link_file because it is a symlink."
}

test_require_backup_write_target_path_rejects_non_regular_target() {
	target_dir="$TEST_TMPDIR/backup_write_target_dir"
	mkdir -p "$target_dir"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_require_backup_write_target_path "$target_dir"
		)
	)
	status=$?

	assertEquals "Backup-write target validation should reject non-regular targets." 1 "$status"
	assertContains "Non-regular backup-write targets should identify the exact path." \
		"$output" "Refusing to write backup metadata $target_dir because it is not a regular file."
}

test_write_local_backup_file_atomically_reports_payload_write_failure() {
	backup_file="$TEST_TMPDIR/local_atomic_write_failure.meta"

	set +e
	status=$(
		(
			zxfer_prepare_local_backup_file_stage() {
				g_zxfer_backup_local_write_failure_result=staging
				return 1
			}
			zxfer_write_local_backup_file_atomically "$backup_file" "#header;payload" >/dev/null
			printf '%s\n' "$?"
		)
	)

	if [ -e "$backup_file" ]; then
		backup_exists=1
	else
		backup_exists=0
	fi

	assertEquals "Atomic local backup writes should fail when the staged payload cannot be prepared." \
		1 "$status"
	assertEquals "Failed atomic local backup writes should not leave a target file behind." \
		0 "$backup_exists"
}

test_write_local_backup_file_atomically_reports_staged_chmod_failure() {
	backup_file="$TEST_TMPDIR/local_atomic_chmod_failure.meta"

	set +e
	status=$(
		(
			chmod() {
				return 1
			}
			zxfer_write_local_backup_file_atomically "$backup_file" "#header;payload" >/dev/null
			printf '%s\n' "$?"
		)
	)
	leftovers=$(find "$TEST_TMPDIR" -maxdepth 1 -type d -name '.zxfer-backup-write.*' | wc -l | tr -d '[:space:]')
	if [ -e "$backup_file" ]; then
		backup_exists=1
	else
		backup_exists=0
	fi

	assertEquals "Atomic local backup writes should fail when securing the staged file fails." \
		1 "$status"
	assertEquals "Failed staged chmod paths should clean up their temporary backup-write directories." \
		0 "$leftovers"
	assertEquals "Failed staged chmod paths should not leave a target file behind." \
		0 "$backup_exists"
}

test_write_local_backup_file_atomically_preserves_prepare_failure_status() {
	backup_file="$TEST_TMPDIR/local_atomic_prepare_status.meta"

	set +e
	status=$(
		(
			zxfer_prepare_local_backup_file_stage() {
				return 37
			}
			zxfer_write_local_backup_file_atomically "$backup_file" "#header;payload" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Atomic local backup writes should preserve staged-prepare helper failures." \
		37 "$status"
}

test_write_local_backup_file_atomically_preserves_finalize_failure_status() {
	backup_file="$TEST_TMPDIR/local_atomic_finalize_status.meta"
	stage_dir="$TEST_TMPDIR/local_atomic_finalize_status.stage"
	stage_file="$stage_dir/backup.write"
	mkdir -p "$stage_dir"
	printf '%s' "payload" >"$stage_file"

	set +e
	status=$(
		(
			g_zxfer_backup_stage_dir_result=$stage_dir
			g_zxfer_backup_stage_file_result=$stage_file
			zxfer_prepare_local_backup_file_stage() {
				g_zxfer_backup_stage_dir_result=$stage_dir
				g_zxfer_backup_stage_file_result=$stage_file
				return 0
			}
			zxfer_commit_local_backup_file_stage() {
				g_zxfer_backup_commit_had_existing_target_result=1
				g_zxfer_backup_commit_rollback_file_result="$TEST_TMPDIR/local_atomic_finalize_status.rollback"
				printf '%s' "old" >"$g_zxfer_backup_commit_rollback_file_result"
				return 0
			}
			zxfer_finalize_local_backup_file_commit() {
				return 41
			}
			zxfer_write_local_backup_file_atomically "$backup_file" "#header;payload" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Atomic local backup writes should preserve rollback-file finalization failures." \
		41 "$status"
}

test_write_local_backup_file_atomically_registers_stale_rollback_for_runtime_cleanup_before_finalize() {
	backup_file="$TEST_TMPDIR/local_atomic_finalize_register.meta"
	stage_dir="$TEST_TMPDIR/local_atomic_finalize_register.stage"
	stage_file="$stage_dir/backup.write"
	# Model the path-adjacent rollback name produced by the live commit helper;
	# the runtime registry intentionally rejects arbitrary adjacent names.
	rollback_file="$TEST_TMPDIR/.zxfer-backup-rollback.test-register"
	trace_file="$TEST_TMPDIR/local_atomic_finalize_register.trace"
	mkdir -p "$stage_dir"
	printf '%s' "payload" >"$stage_file"
	printf '%s' "old" >"$rollback_file"

	output=$(
		(
			g_zxfer_runtime_artifact_cleanup_paths=""
			g_zxfer_backup_stage_dir_result=$stage_dir
			g_zxfer_backup_stage_file_result=$stage_file
			zxfer_prepare_local_backup_file_stage() {
				g_zxfer_backup_stage_dir_result=$stage_dir
				g_zxfer_backup_stage_file_result=$stage_file
				return 0
			}
			zxfer_commit_local_backup_file_stage() {
				g_zxfer_backup_commit_had_existing_target_result=1
				g_zxfer_backup_commit_rollback_file_result=$rollback_file
				return 0
			}
			zxfer_finalize_local_backup_file_commit() {
				printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths" >"$trace_file"
				return 41
			}
			zxfer_write_local_backup_file_atomically "$backup_file" "#header;payload" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	trace_output=$(cat "$trace_file")

	assertContains "Single-file local backup writes should register stale rollback files before finalization so trap cleanup can reap abort leftovers." \
		"$trace_output" "$rollback_file"
	assertContains "Single-file local backup writes should still preserve rollback finalization failures after registering the rollback path." \
		"$output" "status=41"
	g_zxfer_runtime_artifact_cleanup_paths=$rollback_file
	zxfer_cleanup_registered_runtime_artifacts
	assertFalse "The simulated trap cleanup should reap the registered stale rollback fixture after its registration has been asserted." \
		"[ -e '$rollback_file' ]"
}

test_create_backup_metadata_stage_dir_for_path_returns_failure_when_parent_is_missing() {
	stage_path="$TEST_TMPDIR/missing-parent/backup.meta"

	zxfer_create_backup_metadata_stage_dir_for_path "$stage_path" >/dev/null
	status=$?

	assertEquals "Backup stage-directory creation should fail when the target parent directory does not exist." \
		1 "$status"
}

test_create_backup_metadata_stage_dir_for_path_returns_failure_when_parent_lookup_fails() {
	stage_path="$TEST_TMPDIR/stage-parent-lookup-failure/backup.meta"

	set +e
	status=$(
		(
			zxfer_get_path_parent_dir() {
				return 57
			}
			zxfer_create_backup_metadata_stage_dir_for_path "$stage_path" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Backup stage-directory creation should fail closed when the target parent cannot be derived." \
		57 "$status"
}

test_create_backup_metadata_stage_dir_for_path_preserves_mktemp_failure_status() {
	stage_dir="$TEST_TMPDIR/stage-mktemp-status"
	stage_path="$stage_dir/backup.meta"
	mkdir -p "$stage_dir"

	set +e
	status=$(
		(
			mktemp() {
				return 59
			}
			zxfer_create_backup_metadata_stage_dir_for_path "$stage_path" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Backup stage-directory creation should preserve mktemp failures from same-directory staging." \
		59 "$status"
}

test_create_backup_metadata_stage_dir_for_path_registers_and_unregisters_runtime_cleanup_state() {
	stage_root="$TEST_TMPDIR/stage-runtime-registration"
	stage_path="$stage_root/backup.meta"
	mkdir -p "$stage_root"
	zxfer_reset_runtime_artifact_state

	zxfer_create_backup_metadata_stage_dir_for_path "$stage_path" >/dev/null
	status=$?
	stage_dir=$g_zxfer_backup_stage_dir_result

	assertEquals "Backup stage-directory creation should succeed for writable parents." \
		0 "$status"
	assertTrue "Backup stage-directory creation should create the stage directory." \
		"[ -d \"$stage_dir\" ]"
	assertContains "Backup stage-directory creation should register same-directory staging for trap cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$stage_dir"

	zxfer_cleanup_backup_metadata_stage_dir "$stage_dir"

	assertFalse "Backup stage-directory cleanup should remove the created stage directory." \
		"[ -e \"$stage_dir\" ]"
	assertNotContains "Backup stage-directory cleanup should unregister the stage directory from runtime cleanup state." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$stage_dir"
}

test_backup_metadata_path_uses_trusted_nonwritable_parent_returns_failure_when_parent_lookup_fails() {
	backup_file="$TEST_TMPDIR/nonwritable-parent-check/backup.meta"

	set +e
	status=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			zxfer_backup_metadata_path_uses_trusted_nonwritable_parent "$backup_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Trusted non-writable parent detection should fail closed when the backup-file parent cannot be derived." \
		1 "$status"
}

test_ensure_local_backup_dir_rejects_direct_symlink_when_symlink_scan_returns_no_component() {
	real_dir="$TEST_TMPDIR/direct_symlink_real"
	backup_dir="$TEST_TMPDIR/direct_symlink_backup"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$backup_dir"

	set +e
	output=$(
		(
			zxfer_find_symlink_path_component() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Local backup-directory preparation should still reject direct symlink targets when the component scan returns no earlier match." \
		1 "$status"
	assertContains "Direct symlink backup directories should surface the exact path in the error." \
		"$output" "Refusing to use backup directory $backup_dir because it is a symlink."
}

test_commit_local_backup_file_stage_rejects_symlink_target() {
	stage_file="$TEST_TMPDIR/commit_symlink_stage.write"
	target_file="$TEST_TMPDIR/commit_symlink_target.meta"
	target_link="$TEST_TMPDIR/commit_symlink_target.link"
	printf '%s' "payload" >"$stage_file"
	ln -s "$target_file" "$target_link"

	zxfer_commit_local_backup_file_stage "$target_link" "$stage_file" >/dev/null
	status=$?

	assertEquals "Local backup-file stage commits should reject symlink targets." \
		1 "$status"
}

test_commit_local_backup_file_stage_rejects_non_regular_target() {
	stage_file="$TEST_TMPDIR/commit_nonregular_stage.write"
	target_dir="$TEST_TMPDIR/commit_nonregular_target.dir"
	printf '%s' "payload" >"$stage_file"
	mkdir -p "$target_dir"

	zxfer_commit_local_backup_file_stage "$target_dir" "$stage_file" >/dev/null
	status=$?

	assertEquals "Local backup-file stage commits should reject non-regular targets." \
		1 "$status"
}

test_commit_local_backup_file_stage_reports_existing_target_parent_lookup_failure() {
	target_file="$TEST_TMPDIR/commit_parent_lookup_failure.meta"
	stage_file="$TEST_TMPDIR/commit_parent_lookup_failure.write"
	printf '%s' "old" >"$target_file"
	printf '%s' "new" >"$stage_file"

	set +e
	status=$(
		(
			zxfer_get_path_parent_dir() {
				return 43
			}
			zxfer_commit_local_backup_file_stage "$target_file" "$stage_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Local backup-file stage commits should fail closed when the existing target parent cannot be derived." \
		43 "$status"
	assertEquals "Failed existing-target parent lookups should leave the original target untouched." \
		"old" "$(cat "$target_file")"
}

test_commit_local_backup_file_stage_reports_existing_target_rollback_tempfile_creation_failure() {
	target_file="$TEST_TMPDIR/commit_rollback_tempfile_failure.meta"
	stage_file="$TEST_TMPDIR/commit_rollback_tempfile_failure.write"
	printf '%s' "old" >"$target_file"
	printf '%s' "new" >"$stage_file"

	set +e
	status=$(
		(
			mktemp() {
				return 67
			}
			zxfer_commit_local_backup_file_stage "$target_file" "$stage_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Local backup-file stage commits should fail closed when they cannot allocate a rollback file for an existing target." \
		67 "$status"
	assertEquals "Rollback-tempfile allocation failures should leave the original target untouched." \
		"old" "$(cat "$target_file")"
}

test_commit_local_backup_file_stage_cleans_up_rollback_file_when_existing_target_rename_fails() {
	target_file="$TEST_TMPDIR/commit_existing_target_rename_failure.meta"
	stage_file="$TEST_TMPDIR/commit_existing_target_rename_failure.write"
	rollback_file="$TEST_TMPDIR/commit_existing_target_rename_failure.rollback"
	printf '%s' "old" >"$target_file"
	printf '%s' "new" >"$stage_file"
	printf '%s' "placeholder" >"$rollback_file"

	set +e
	status=$(
		(
			mktemp() {
				printf '%s\n' "$rollback_file"
			}
			mv() {
				if [ "$1" = "-f" ] && [ "$2" = "$target_file" ] && [ "$3" = "$rollback_file" ]; then
					return 47
				fi
				command mv "$@"
			}
			zxfer_commit_local_backup_file_stage "$target_file" "$stage_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	if [ -e "$rollback_file" ]; then
		rollback_exists=1
	else
		rollback_exists=0
	fi

	assertEquals "Local backup-file stage commits should fail when they cannot move the existing target into rollback storage." \
		47 "$status"
	assertEquals "Existing-target rename failures should clean up the temporary rollback file path." \
		0 "$rollback_exists"
	assertEquals "Existing-target rename failures should leave the original target untouched." \
		"old" "$(cat "$target_file")"
}

test_commit_local_backup_file_stage_removes_target_when_stage_rename_fails_without_existing_target() {
	target_file="$TEST_TMPDIR/commit_stage_rename_failure.meta"
	stage_file="$TEST_TMPDIR/commit_stage_rename_failure.write"
	printf '%s' "new" >"$stage_file"

	set +e
	status=$(
		(
			mv() {
				if [ "$1" = "-f" ] && [ "$2" = "$stage_file" ] && [ "$3" = "$target_file" ]; then
					return 53
				fi
				command mv "$@"
			}
			zxfer_commit_local_backup_file_stage "$target_file" "$stage_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	if [ -e "$target_file" ]; then
		target_exists=1
	else
		target_exists=0
	fi

	assertEquals "Local backup-file stage commits should fail when the staged file cannot be renamed into place." \
		53 "$status"
	assertEquals "Failed stage renames without a prior target should not leave a partial destination file behind." \
		0 "$target_exists"
}

test_commit_local_backup_file_stage_preserves_rollback_file_when_restore_back_fails_after_stage_rename_failure() {
	target_file="$TEST_TMPDIR/commit_restore_back_failure.meta"
	stage_file="$TEST_TMPDIR/commit_restore_back_failure.write"
	rollback_file="$TEST_TMPDIR/commit_restore_back_failure.rollback"
	printf '%s' "old" >"$target_file"
	printf '%s' "new" >"$stage_file"

	set +e
	status=$(
		(
			mktemp() {
				printf '%s\n' "$rollback_file"
			}
			mv() {
				if [ "$1" = "-f" ] && [ "$2" = "$target_file" ] && [ "$3" = "$rollback_file" ]; then
					command mv "$@"
					return 0
				fi
				if [ "$1" = "-f" ] && [ "$2" = "$stage_file" ] && [ "$3" = "$target_file" ]; then
					return 61
				fi
				if [ "$1" = "-f" ] && [ "$2" = "$rollback_file" ] && [ "$3" = "$target_file" ]; then
					return 67
				fi
				command mv "$@"
			}
			zxfer_commit_local_backup_file_stage "$target_file" "$stage_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	if [ -e "$rollback_file" ]; then
		rollback_exists=1
	else
		rollback_exists=0
	fi
	if [ -e "$target_file" ]; then
		target_exists=1
	else
		target_exists=0
	fi

	assertEquals "Local backup-file stage commits should still return failure when the original target cannot be restored after a stage rename failure." \
		67 "$status"
	assertEquals "Failed rollback restoration inside stage-commit recovery should preserve the rollback file for manual recovery." \
		1 "$rollback_exists"
	assertEquals "Failed rollback restoration should leave the old metadata contents in the preserved rollback file." \
		"old" "$(cat "$rollback_file")"
	assertEquals "Failed rollback restoration inside stage-commit recovery should not leave a partial target behind." \
		0 "$target_exists"
}

test_rollback_local_backup_file_commit_returns_failure_when_restoring_existing_target_fails() {
	target_file="$TEST_TMPDIR/rollback_restore_failure.meta"
	rollback_file="$TEST_TMPDIR/rollback_restore_failure.rollback"
	printf '%s' "new" >"$target_file"
	printf '%s' "old" >"$rollback_file"

	set +e
	status=$(
		(
			mv() {
				return 1
			}
			if zxfer_rollback_local_backup_file_commit "$target_file" 1 "$rollback_file" >/dev/null; then
				l_status=0
			else
				l_status=$?
			fi
			printf '%s\n' "$l_status"
		)
	)
	set -e
	if [ -e "$target_file" ]; then
		target_exists=1
	else
		target_exists=0
	fi
	if [ -e "$rollback_file" ]; then
		rollback_exists=1
	else
		rollback_exists=0
	fi

	assertEquals "Backup-file rollback helpers should return failure when the original target cannot be restored." \
		1 "$status"
	assertEquals "Failed rollback restores should remove the new target so the rollback file remains authoritative." \
		0 "$target_exists"
	assertEquals "Failed rollback restores should preserve the rollback file for manual recovery." \
		1 "$rollback_exists"
}

test_prepare_local_backup_file_stage_cleans_up_when_stage_file_write_fails() {
	backup_file="$TEST_TMPDIR/prepare_stage_write_failure.meta"
	stage_dir="$g_zxfer_run_tmp_root/prepare_stage_write_failure.stage"
	mkdir -p "$stage_dir/backup.write" || fail "Unable to create the staged write-failure fixture."

	set +e
	output=$(
		(
			# The mocked stage directory is a direct child of this disposable
			# run root, matching the cleanup helper's ownership contract.
			zxfer_create_backup_metadata_stage_dir_for_path() {
				g_zxfer_backup_stage_dir_result=$stage_dir
				printf '%s\n' "$stage_dir"
				return 0
			}
			if zxfer_prepare_local_backup_file_stage "$backup_file" "#header;payload" >/dev/null 2>&1; then
				l_status=0
			else
				l_status=$?
			fi
			printf 'status=%s\n' "$l_status"
			printf 'failure=<%s>\n' "${g_zxfer_backup_local_write_failure_result:-}"
			printf 'stage_dir=<%s>\n' "${g_zxfer_backup_stage_dir_result:-}"
			printf 'stage_file=<%s>\n' "${g_zxfer_backup_stage_file_result:-}"
			printf 'exists=%s\n' "$([ -e "$stage_dir" ] && printf '%s' yes || printf '%s' no)"
		)
	)
	set -e
	stage_status=$(printf '%s\n' "$output" | awk -F= '/^status=/{print $2; exit}')

	assertContains "Preparing a local backup-file stage should report the staged write failure status." \
		"$output" "status="
	assertNotEquals "Preparing a local backup-file stage should fail closed when the staged file cannot be opened for writing." \
		0 "${stage_status:-}"
	assertContains "Preparing a local backup-file stage should classify staged write failures as staging errors." \
		"$output" "failure=<staging>"
	assertContains "Preparing a local backup-file stage should clear the published stage directory on staged write failure." \
		"$output" "stage_dir=<>"
	assertContains "Preparing a local backup-file stage should clear the published stage file on staged write failure." \
		"$output" "stage_file=<>"
	assertContains "Preparing a local backup-file stage should clean up the stage directory when staged writes fail." \
		"$output" "exists=no"
}

test_prepare_local_backup_file_stage_writes_multiline_stage_file_and_sets_results() {
	backup_file="$TEST_TMPDIR/prepare_stage_success.meta"

	zxfer_prepare_local_backup_file_stage "$backup_file" "#header;payload;trailer" >/dev/null

	stage_dir=$g_zxfer_backup_stage_dir_result
	stage_file=$g_zxfer_backup_stage_file_result

	assertEquals "Preparing a local backup-file stage should leave the local-write failure scratch empty on success." \
		"" "${g_zxfer_backup_local_write_failure_result:-}"
	assertNotEquals "Preparing a local backup-file stage should publish the stage directory on success." \
		"" "$stage_dir"
	assertNotEquals "Preparing a local backup-file stage should publish the stage file on success." \
		"" "$stage_file"
	assertTrue "Preparing a local backup-file stage should create the published stage directory." \
		"[ -d '$stage_dir' ]"
	assertTrue "Preparing a local backup-file stage should create the published staged backup file." \
		"[ -f '$stage_file' ]"
	assertEquals "Preparing a local backup-file stage should preserve newline-oriented payloads without semicolon translation." \
		"#header;payload;trailer" "$(cat "$stage_file")"
	assertEquals "Preparing a local backup-file stage should write staged files with secure 0600 permissions." \
		"600" "$(zxfer_get_path_mode_octal "$stage_file")"
}

test_rollback_local_backup_file_commit_preserves_remove_failure_status() {
	target_file="$TEST_TMPDIR/rollback_remove_failure.meta"
	rollback_file="$TEST_TMPDIR/rollback_remove_failure.rollback"
	printf '%s' "new" >"$target_file"
	printf '%s' "old" >"$rollback_file"

	set +e
	status=$(
		(
			zxfer_remove_local_backup_metadata_path_if_present() {
				return 47
			}
			zxfer_rollback_local_backup_file_commit "$target_file" 1 "$rollback_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Backup-file rollback helpers should preserve failures while removing the new target before rollback restore." \
		47 "$status"
	if [ ! -e "$rollback_file" ]; then
		fail "Backup-file rollback helpers should leave the rollback file in place when target removal fails before restore."
	fi
	return 0
}

test_rollback_local_backup_file_commit_removes_new_target_when_no_existing_target() {
	target_file="$TEST_TMPDIR/rollback_no_existing_target.meta"
	printf '%s' "new" >"$target_file"

	zxfer_rollback_local_backup_file_commit "$target_file" 0 ""
	status=$?
	if [ -e "$target_file" ]; then
		target_exists=1
	else
		target_exists=0
	fi

	assertEquals "Backup-file rollback helpers should succeed when removing a newly created target with no prior file to restore." \
		0 "$status"
	assertEquals "Rollback of a newly created target should remove the destination file." \
		0 "$target_exists"
}

test_finalize_local_backup_file_commit_removes_existing_rollback_file() {
	rollback_file="$TEST_TMPDIR/finalize_commit.rollback"
	printf '%s' "old" >"$rollback_file"

	zxfer_finalize_local_backup_file_commit 1 "$rollback_file"
	if [ -e "$rollback_file" ]; then
		rollback_exists=1
	else
		rollback_exists=0
	fi

	assertEquals "Finalizing a committed backup file should remove the now-stale rollback file." \
		0 "$rollback_exists"
}

test_finalize_local_backup_file_commit_unregisters_registered_rollback_file() {
	rollback_file="$TEST_TMPDIR/finalize_commit_registered.rollback"
	printf '%s' "old" >"$rollback_file"
	zxfer_reset_runtime_artifact_state
	zxfer_register_backup_metadata_runtime_artifact_path "$rollback_file"

	zxfer_finalize_local_backup_file_commit 1 "$rollback_file"

	assertNotContains "Committed backup finalization should unregister rollback files that were tracked for abort cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$rollback_file"
}

test_finalize_local_backup_file_commit_returns_failure_when_rollback_cleanup_fails() {
	rollback_file="$TEST_TMPDIR/finalize_commit_failure.rollback"
	printf '%s' "old" >"$rollback_file"

	set +e
	status=$(
		(
			rm() {
				return 43
			}
			zxfer_finalize_local_backup_file_commit 1 "$rollback_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	if [ -f "$rollback_file" ]; then
		rollback_exists=1
	else
		rollback_exists=0
	fi

	assertEquals "Committed backup finalization should preserve rollback cleanup failures." \
		43 "$status"
	assertEquals "Committed backup finalization should preserve the rollback file when cleanup fails." \
		1 "$rollback_exists"
}
