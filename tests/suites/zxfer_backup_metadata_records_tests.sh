#!/bin/sh
# Backup metadata record, capture, rendering, and keying behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_reset_backup_metadata_state_clears_accumulator_and_restore_cache() {
	g_backup_file_contents="stale-backup"
	g_pending_backup_file_contents="stale-pending"
	g_forwarded_backup_properties="stale-forwarded"
	g_restored_backup_file_contents="stale-restore"
	g_zxfer_backup_file_read_result="stale-read"
	g_zxfer_backup_restore_candidate_path_result="stale-candidate"

	zxfer_reset_backup_metadata_state

	assertEquals "The backup-metadata reset helper should clear the accumulation buffer." \
		"" "$g_backup_file_contents"
	assertEquals "The backup-metadata reset helper should clear deferred seeded backup rows." \
		"" "$g_pending_backup_file_contents"
	assertEquals "The backup-metadata reset helper should clear the record-list result scratch channel." \
		"" "$g_zxfer_backup_metadata_record_list_result"
	assertEquals "The backup-metadata reset helper should clear the rendered-metadata scratch channel." \
		"" "$g_zxfer_rendered_backup_metadata_contents"
	assertEquals "The backup-metadata reset helper should clear the backup-file read scratch channel." \
		"" "$g_zxfer_backup_file_read_result"
	assertEquals "The backup-metadata reset helper should clear the restore-candidate path scratch channel." \
		"" "$g_zxfer_backup_restore_candidate_path_result"
	assertEquals "The backup-metadata reset helper should clear forwarded provenance scratch state." \
		"" "$g_forwarded_backup_properties"
	assertEquals "The backup-metadata reset helper should clear restored backup contents." \
		"" "$g_restored_backup_file_contents"
}

test_reset_backup_storage_state_clears_storage_scratch_channels() {
	g_zxfer_remote_backup_dry_run_shell_command_result="stale-dry-run"
	g_zxfer_backup_file_read_result="stale-read"
	g_zxfer_backup_stage_dir_result="stale-stage-dir"
	g_zxfer_backup_stage_file_result="stale-stage-file"
	g_zxfer_backup_commit_had_existing_target_result="stale-target"
	g_zxfer_backup_commit_rollback_file_result="stale-rollback"
	g_zxfer_backup_local_read_failure_result="stale-read-failure"
	g_zxfer_backup_local_write_failure_result="stale-write-failure"

	zxfer_reset_backup_storage_state

	assertEquals "The backup-storage reset should clear remote dry-run rendering scratch." \
		"" "$g_zxfer_remote_backup_dry_run_shell_command_result"
	assertEquals "The backup-storage reset should clear secure read results." \
		"" "$g_zxfer_backup_file_read_result"
	assertEquals "The backup-storage reset should clear stage-directory results." \
		"" "$g_zxfer_backup_stage_dir_result"
	assertEquals "The backup-storage reset should clear stage-file results." \
		"" "$g_zxfer_backup_stage_file_result"
	assertEquals "The backup-storage reset should clear commit target-state results." \
		"" "$g_zxfer_backup_commit_had_existing_target_result"
	assertEquals "The backup-storage reset should clear rollback-file results." \
		"" "$g_zxfer_backup_commit_rollback_file_result"
	assertEquals "The backup-storage reset should clear local read failure classification." \
		"" "$g_zxfer_backup_local_read_failure_result"
	assertEquals "The backup-storage reset should clear local write failure classification." \
		"" "$g_zxfer_backup_local_write_failure_result"
}

test_backup_storage_partial_load_excludes_metadata_policy() {
	# shellcheck disable=SC2016  # Module-root variables expand inside the clean child shell.
	ownership_output=$(
		ZXFER_SOURCE_MODULES_ROOT="$ZXFER_ROOT" /bin/sh -c '
			. "$ZXFER_SOURCE_MODULES_ROOT/src/zxfer_modules.sh" || exit 1
			zxfer_load_modules zxfer_runtime.sh || exit 1
			if command -v zxfer_refresh_backup_storage_root >/dev/null 2>&1; then
				printf "%s\n" "runtime_has_backup_root=yes"
			else
				printf "%s\n" "runtime_has_backup_root=no"
			fi
			zxfer_load_modules zxfer_backup_storage.sh || exit 1
			if command -v zxfer_refresh_backup_storage_root >/dev/null 2>&1; then
				printf "%s\n" "storage_has_backup_root=yes"
			else
				printf "%s\n" "storage_has_backup_root=no"
			fi
			if command -v zxfer_read_local_backup_file >/dev/null 2>&1; then
				printf "%s\n" "storage_has_secure_read=yes"
			else
				printf "%s\n" "storage_has_secure_read=no"
			fi
			if command -v zxfer_validate_backup_metadata_format >/dev/null 2>&1; then
				printf "%s\n" "storage_has_metadata_policy=yes"
			else
				printf "%s\n" "storage_has_metadata_policy=no"
			fi
			zxfer_load_modules zxfer_backup_metadata.sh || exit 1
			if command -v zxfer_validate_backup_metadata_format >/dev/null 2>&1; then
				printf "%s\n" "metadata_has_format_policy=yes"
			else
				printf "%s\n" "metadata_has_format_policy=no"
			fi
		'
	)
	ownership_status=$?

	assertEquals "Canonical partial loading should succeed across the backup-storage and metadata boundaries." \
		0 "$ownership_status"
	assertContains "Generic runtime should not own backup-root configuration." \
		"$ownership_output" "runtime_has_backup_root=no"
	assertContains "Backup storage should own backup-root configuration." \
		"$ownership_output" "storage_has_backup_root=yes"
	assertContains "Backup storage should own secure metadata reads." \
		"$ownership_output" "storage_has_secure_read=yes"
	assertContains "Backup storage should not pull in metadata format policy." \
		"$ownership_output" "storage_has_metadata_policy=no"
	assertContains "Backup metadata should own format validation." \
		"$ownership_output" "metadata_has_format_policy=yes"
}

test_backup_metadata_constants_pin_source_values() {
	result=$(
		(
			# shellcheck source=src/zxfer_backup_storage.sh
			. "$TESTS_DIR/../src/zxfer_backup_storage.sh"
			# shellcheck source=src/zxfer_backup_metadata.sh
			. "$TESTS_DIR/../src/zxfer_backup_metadata.sh"
			printf 'header=%s\n' "$ZXFER_BACKUP_METADATA_HEADER_LINE"
			printf 'format=%s\n' "$ZXFER_BACKUP_METADATA_FORMAT_VERSION"
			printf 'split=%s\n' "$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE"
		)
	)

	assertContains "The backup metadata header getter should return the source-time header line." \
		"$result" "header=#zxfer property backup file"
	assertContains "The backup metadata format-version getter should return the source-time format version." \
		"$result" "format=2"
	assertContains "The backup metadata pair-split getter should return the current forwarded-backup split marker." \
		"$result" "split=__ZXFER_BACKUP_METADATA_PAIR_SPLIT__"
}

test_get_expected_backup_destination_for_source_treats_regex_significant_names_as_literal_paths() {
	g_initial_source="tank/app.v1"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=0

	assertEquals "Expected backup destination mapping should preserve dots in source dataset names as literal path components." \
		"backup/dst/app.v1/releases.2026" "$(zxfer_get_expected_backup_destination_for_source "tank/app.v1/releases.2026")"
}

test_append_backup_metadata_record_preserves_existing_newline_rows() {
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "existing" "quota=1G=local")
	l_expected_rows=$(printf '%s\n%s' \
		"$(zxfer_test_backup_metadata_row "existing" "quota=1G=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")

	zxfer_append_backup_metadata_record "tank/src" "compression=lz4=local"

	assertEquals "Backup-metadata appends should keep the v2 newline-oriented row format." \
		"$l_expected_rows" "$g_backup_file_contents"
}

test_append_backup_metadata_record_buffers_duplicate_rows_until_write_boundary() {
	g_backup_file_contents=$(printf '%s\n%s\n' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "other" "quota=1G=local")")
	l_expected_buffered_rows=$(printf '%s\n%s\n%s' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "other" "quota=1G=local")" \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")")
	l_expected_validated_rows=$(printf '%s\n%s' \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")" \
		"$(zxfer_test_backup_metadata_row "other" "quota=1G=local")")

	zxfer_append_backup_metadata_record "tank/src" "readonly=on=local"
	l_buffered_rows=$g_backup_file_contents
	l_validated_rows=$(zxfer_validate_backup_metadata_record_list "$g_backup_file_contents")

	assertEquals "Backup-metadata appends should buffer duplicate keys as plain rows instead of rebuilding the buffer per append." \
		"$l_expected_buffered_rows" "$l_buffered_rows"
	assertEquals "Write-boundary validation should collapse duplicate keys newest-row-wins in first-appearance order." \
		"$l_expected_validated_rows" "$l_validated_rows"
}

test_validate_backup_metadata_record_list_collapses_preexisting_duplicate_rows_newest_wins() {
	g_backup_file_contents=$(printf '%s\n%s\n' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "readonly=off=local")")

	zxfer_append_backup_metadata_record "tank/src" "readonly=on=local"

	assertEquals "Write-boundary validation should collapse buffered relative-path duplicates down to the newest row." \
		".	readonly=on=local" "$(zxfer_validate_backup_metadata_record_list "$g_backup_file_contents")"
}

test_append_backup_metadata_record_preserves_literal_backslashes() {
	g_backup_file_contents=$(printf '%s\n%s\n' \
		"$(zxfer_test_backup_metadata_row "." 'user:path=C:\\temp\\new=local')" \
		"$(zxfer_test_backup_metadata_row "other" 'user:path=E:\\keep\\me=local')")

	zxfer_append_backup_metadata_record "tank/src" 'user:path=D:\\archive\\more=local'

	l_expected_buffered_rows=$(printf '%s\n%s\n%s' \
		'.	user:path=C:\\temp\\new=local' \
		'other	user:path=E:\\keep\\me=local' \
		'.	user:path=D:\\archive\\more=local')
	l_expected_validated_rows=$(printf '%s\n%s' \
		'.	user:path=D:\\archive\\more=local' \
		'other	user:path=E:\\keep\\me=local')

	assertEquals "Buffered appends should preserve literal backslashes in newly appended rows." \
		"$l_expected_buffered_rows" "$g_backup_file_contents"
	assertEquals "Write-boundary validation should preserve literal backslashes in both winning and untouched rows." \
		"$l_expected_validated_rows" "$(zxfer_validate_backup_metadata_record_list "$g_backup_file_contents")"
}

test_append_backup_metadata_record_defers_malformed_row_rejection_to_write_boundary() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_backup_file_contents="broken,row-without-properties"
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			exit 1
		}
		zxfer_append_backup_metadata_record "tank/src" "readonly=on=local" || exit 9
		printf "buffered=<%s>\n" "$g_backup_file_contents" >&2
		zxfer_validate_backup_metadata_record_list "$g_backup_file_contents" >/dev/null
	'

	l_expected_buffered=$(printf 'buffered=<%s\n%s>' \
		"broken,row-without-properties" \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")")

	assertEquals "A malformed buffered row should abort at the write boundary instead of on the append that follows it." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Appends after a malformed buffered row should still buffer their own row before the write boundary." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "$l_expected_buffered"
	assertContains "Malformed buffered rows should surface the write-boundary validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to validate buffered backup metadata records for chained backup provenance."
}

test_validate_backup_metadata_record_list_preserves_relative_rows() {
	result=$(zxfer_validate_backup_metadata_record_list \
		"$(printf '%s\n%s\n' \
			"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
			"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")")")

	assertEquals "Forwarded-provenance rendering should preserve v2 relative rows because the forwarded header carries the destination identity." \
		".	compression=lz4=local
child	quota=1G=local" "$result"
}

test_validate_backup_metadata_record_list_reports_awk_failures() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_cmd_awk=false
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_validate_backup_metadata_record_list \
			";tank/src,backup/dst,compression=lz4=local"
	'

	assertEquals "Forwarded-provenance validation should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Forwarded-provenance validation should surface the buffered validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to validate buffered backup metadata records for chained backup provenance."
}

test_remove_backup_metadata_record_list_reports_awk_failures() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_cmd_awk=false
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_remove_backup_metadata_record_list \
			".	compression=lz4=local" \
			"tank/src"
	'

	assertEquals "Backup-metadata record removals should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Backup-metadata record removals should surface the buffered-remove failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to remove buffered backup metadata records."
}

test_get_buffered_backup_metadata_record_properties_returns_missing_without_mutating_scratch() {
	g_zxfer_backup_metadata_record_properties_result="stale"
	g_initial_source="tank"
	outfile="$TEST_TMPDIR/get_buffered_props_missing.out"

	set +e
	zxfer_get_buffered_backup_metadata_record_properties \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"tank/other" >"$outfile"
	status=$?
	set -e

	assertEquals "Buffered backup-metadata property lookups should return a plain missing status when no relative row exists." \
		1 "$status"
	assertEquals "Missing buffered backup-metadata property lookups should not emit a properties payload." \
		"" "$(cat "$outfile")"
	assertEquals "Missing buffered backup-metadata property lookups should clear the record-properties scratch channel." \
		"" "$g_zxfer_backup_metadata_record_properties_result"
}

test_get_buffered_backup_metadata_record_properties_reports_awk_failures() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_cmd_awk=/definitely-missing-zxfer-awk
		g_zxfer_backup_metadata_record_properties_result="stale"
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "scratch=<%s>\n" "$g_zxfer_backup_metadata_record_properties_result" >&2
			exit 1
		}
		zxfer_get_buffered_backup_metadata_record_properties \
			".	compression=lz4=local" \
			"tank/src"
	'

	assertEquals "Buffered backup-metadata property lookups should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Buffered backup-metadata property lookups should surface the inspection failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect buffered backup metadata records."
	assertContains "Buffered backup-metadata property lookups should not clobber the prior scratch channel before the failure is raised." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "scratch=<stale>"
}

test_get_buffered_backup_metadata_record_properties_returns_newest_duplicate_row() {
	g_initial_source="tank/src"

	result=$(zxfer_get_buffered_backup_metadata_record_properties \
		"$(printf '%s\n%s\n%s' \
			"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
			"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")" \
			"$(zxfer_test_backup_metadata_row "." "readonly=on=local")")" \
		"tank/src")
	status=$?

	assertEquals "Buffered backup-metadata property lookups should succeed when duplicate keys are buffered." \
		0 "$status"
	assertEquals "Buffered backup-metadata property lookups should return the newest buffered row for a duplicated key." \
		"readonly=on=local" "$result"
}

test_capture_backup_metadata_for_completed_transfer_buffers_live_rows_without_flushing() {
	log="$TEST_TMPDIR/capture_backup_live.log"
	: >"$log"
	g_option_k_backup_property_mode=1

	zxfer_write_backup_properties() {
		printf 'unexpected write\n' >>"$log"
	}

	zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "compression=lz4=local"

	unset -f zxfer_write_backup_properties

	assertEquals "Completed transfers should buffer the backup row in memory first." \
		".	compression=lz4=local" "$g_backup_file_contents"
	assertEquals "Completed-transfer buffering should not flush backup metadata by itself." \
		"" "$(cat "$log")"
}

test_capture_backup_metadata_for_completed_transfer_rethrows_forwarded_lookup_failures() {
	append_log="$TEST_TMPDIR/capture_forwarded_lookup_failure.log"
	: >"$append_log"

	set +e
	output=$(
		(
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=0
			g_backup_file_extension=".zxfer_backup_info"
			zxfer_get_forwarded_backup_properties_for_source() {
				zxfer_throw_error "forwarded lookup failed"
			}
			zxfer_append_backup_metadata_record() {
				printf 'unexpected append %s %s\n' "$1" "$2" >>"$append_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}

			zxfer_capture_backup_metadata_for_completed_transfer "backup/intermediate/src" "compression=off=local"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Completed-transfer backup capture should fail closed when forwarded provenance lookup throws an error." \
		1 "$status"
	assertContains "Forwarded provenance lookup failures should propagate their original error instead of silently falling back to intermediate live properties." \
		"$output" "forwarded lookup failed"
	assertEquals "Failed forwarded provenance lookups should not append fallback live-property backup rows." \
		"" "$(cat "$append_log")"
}

test_capture_backup_metadata_for_completed_transfer_rethrows_unexpected_forwarded_lookup_status() {
	append_log="$TEST_TMPDIR/capture_forwarded_lookup_status.log"
	: >"$append_log"

	set +e
	output=$(
		(
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=0
			g_backup_file_extension=".zxfer_backup_info"
			zxfer_get_forwarded_backup_properties_for_source() {
				return 2
			}
			zxfer_append_backup_metadata_record() {
				printf 'unexpected append %s %s\n' "$1" "$2" >>"$append_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}

			zxfer_capture_backup_metadata_for_completed_transfer "backup/intermediate/src" "compression=off=local"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Completed-transfer backup capture should fail closed when forwarded provenance lookup returns an unexpected non-missing status." \
		1 "$status"
	assertContains "Unexpected forwarded provenance lookup statuses should identify the source dataset whose provenance could not be derived." \
		"$output" "Failed to derive forwarded backup properties for source dataset [backup/intermediate/src]."
	assertEquals "Unexpected forwarded provenance lookup statuses should not append fallback live-property backup rows." \
		"" "$(cat "$append_log")"
}

test_capture_backup_metadata_for_completed_transfer_uses_forwarded_scratch_without_temp_file() {
	append_log="$TEST_TMPDIR/capture_forwarded_no_temp.log"
	: >"$append_log"
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_backup_file_extension=".zxfer_backup_info"

	zxfer_get_temp_file() {
		printf '%s\n' "unexpected temp file request" >&2
		exit 1
	}
	zxfer_get_forwarded_backup_properties_for_source() {
		g_forwarded_backup_properties="compression=lz4=local"
		printf '%s\n' "$g_forwarded_backup_properties"
	}
	zxfer_append_backup_metadata_record() {
		printf 'backup_append %s %s\n' "$1" "$2" >>"$append_log"
	}

	zxfer_capture_backup_metadata_for_completed_transfer "backup/intermediate/src" "compression=off=local"

	unset -f zxfer_get_temp_file
	unset -f zxfer_get_forwarded_backup_properties_for_source
	unset -f zxfer_append_backup_metadata_record

	assertContains "Forwarded provenance capture should use the helper-owned scratch value instead of a temp-file relay." \
		"$(cat "$append_log")" "backup_append backup/intermediate/src compression=lz4=local"
}

test_flush_captured_backup_metadata_if_live_flushes_and_restores_failure_stage() {
	log="$TEST_TMPDIR/flush_backup_live.log"
	: >"$log"
	g_option_k_backup_property_mode=1
	g_zxfer_failure_stage="property transfer"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	zxfer_write_backup_properties() {
		printf 'write stage=%s backup=%s\n' "$g_zxfer_failure_stage" "$g_backup_file_contents" >>"$log"
	}

	zxfer_flush_captured_backup_metadata_if_live

	unset -f zxfer_write_backup_properties

	assertEquals "Live backup flushes should write the already-buffered metadata." \
		"write stage=property transfer backup=.	compression=lz4=local" "$(cat "$log")"
	assertEquals "Successful live backup flushes should restore the caller failure stage." \
		"property transfer" "$g_zxfer_failure_stage"
}

test_flush_captured_backup_metadata_if_live_skips_dry_run_and_empty_buffers() {
	log="$TEST_TMPDIR/flush_backup_dryrun.log"
	: >"$log"
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=1
	g_zxfer_failure_stage="property transfer"

	zxfer_write_backup_properties() {
		printf 'unexpected write\n' >>"$log"
	}

	zxfer_flush_captured_backup_metadata_if_live
	g_option_n_dryrun=0
	zxfer_flush_captured_backup_metadata_if_live

	unset -f zxfer_write_backup_properties

	assertEquals "Dry-run or empty-buffer backup flush paths should keep the one-shot preview behavior and skip live writes." \
		"" "$(cat "$log")"
	assertEquals "Skipped backup flushes should leave the caller failure stage unchanged." \
		"property transfer" "$g_zxfer_failure_stage"
}

test_backup_metadata_capture_and_flush_helpers_treat_unset_mode_flags_as_disabled() {
	set +e
	output=$(
		(
			unset g_option_k_backup_property_mode g_option_n_dryrun
			zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "compression=lz4=local"
			zxfer_flush_captured_backup_metadata_if_live
		) 2>&1
	)
	status=$?

	assertEquals "Unset backup-mode flags should be treated as disabled no-op helpers." 0 "$status"
	assertEquals "Unset backup-mode flags should not emit integer-comparison warnings." "" "$output"
	assertEquals "Unset backup-mode flags should not mutate buffered metadata." "" "${g_backup_file_contents:-}"
}

test_defer_and_finalize_buffered_backup_metadata_records_move_seeded_rows_out_of_flushable_buffer() {
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_backup_file_contents=$(printf '%s\n%s\n' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")")
	l_final_rows=$(printf '%s\n%s' \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")

	zxfer_defer_buffered_backup_metadata_record "tank/src"

	assertEquals "Deferring a seeded dataset should remove its row from the flushable live buffer." \
		"child	quota=1G=local" "$g_backup_file_contents"
	assertEquals "Deferring a seeded dataset should move its row into the pending seeded buffer." \
		".	compression=lz4=local" "$g_pending_backup_file_contents"

	zxfer_finalize_deferred_backup_metadata_record "tank/src"

	assertEquals "Finalizing a seeded dataset should clear its pending seeded row." \
		"" "$g_pending_backup_file_contents"
	assertEquals "Finalizing a seeded dataset should restore the deferred pre-seed row into the flushable live buffer instead of overwriting it with later live properties." \
		"$l_final_rows" "$g_backup_file_contents"
}

test_defer_buffered_backup_metadata_record_rejects_missing_live_row() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents="child	quota=1G=local"
		g_pending_backup_file_contents="seed	readonly=on=local"
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when the live buffered row is missing." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Deferring buffered backup metadata should identify the missing live row instead of silently rebuilding it from later live properties." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Buffered backup metadata row for source dataset [tank/src] is missing."
	assertContains "Deferring buffered backup metadata should leave the flushable live buffer untouched when the row is missing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<child	quota=1G=local>"
	assertContains "Deferring buffered backup metadata should leave the pending seeded buffer untouched when the row is missing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<seed	readonly=on=local>"
}

test_defer_buffered_backup_metadata_record_moves_newest_duplicate_live_row() {
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_backup_file_contents=$(printf '%s\n%s\n%s' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")" \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")")

	zxfer_defer_buffered_backup_metadata_record "tank/src"

	assertEquals "Deferring a dataset with buffered duplicate keys should remove every duplicate from the live buffer." \
		"child	quota=1G=local" "$g_backup_file_contents"
	assertEquals "Deferring a dataset with buffered duplicate keys should move the newest buffered row into the pending buffer." \
		".	readonly=on=local" "$g_pending_backup_file_contents"
}

test_defer_buffered_backup_metadata_record_rejects_malformed_live_rows() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents="broken,row-without-properties
.	compression=lz4=local"
		g_pending_backup_file_contents="seed	readonly=on=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 3
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when the live buffered rows are malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed live buffered rows should identify the source dataset." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Buffered backup metadata rows are malformed while deferring source dataset [tank/src]."
	assertContains "Malformed live buffered rows should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<broken,row-without-properties"
	assertContains "Malformed live buffered rows should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<seed	readonly=on=local>"
}

test_defer_buffered_backup_metadata_record_rethrows_live_lookup_failures_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=".	compression=lz4=local
child	quota=1G=local"
		g_pending_backup_file_contents="seed	readonly=on=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 99
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when live-row inspection returns an unexpected status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Unexpected live-row inspection failures should identify the source dataset." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect buffered backup metadata row for source dataset [tank/src]."
	assertContains "Unexpected live-row inspection failures should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<.	compression=lz4=local"
	assertContains "Unexpected live-row inspection failures should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<seed	readonly=on=local>"
}

test_finalize_deferred_backup_metadata_record_rejects_missing_pending_row() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents="child	quota=1G=local"
		g_pending_backup_file_contents=""
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when the pending seeded row is missing." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing deferred backup rows should identify the source dataset." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Deferred backup metadata row for source dataset [tank/src] is missing."
}

test_finalize_deferred_backup_metadata_record_restores_newest_duplicate_pending_row() {
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_backup_file_contents="child	quota=1G=local"
	g_pending_backup_file_contents=$(printf '%s\n%s' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")")
	l_expected_live_rows=$(printf '%s\n%s' \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")" \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")")

	zxfer_finalize_deferred_backup_metadata_record "tank/src"

	assertEquals "Finalizing a dataset with duplicate pending keys should clear every pending duplicate." \
		"" "$g_pending_backup_file_contents"
	assertEquals "Finalizing a dataset with duplicate pending keys should restore the newest pending row into the live buffer." \
		"$l_expected_live_rows" "$g_backup_file_contents"
}

test_finalize_deferred_backup_metadata_record_rejects_malformed_pending_rows() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents="child	quota=1G=local"
		g_pending_backup_file_contents="broken,row-without-properties
.	compression=lz4=local"
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when the pending seeded rows are malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed deferred backup rows should identify the source dataset." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Deferred backup metadata rows are malformed while finalizing source dataset [tank/src]."
}

test_finalize_deferred_backup_metadata_record_rethrows_pending_lookup_failures_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_cmd_awk=/definitely-missing-zxfer-awk
		g_backup_file_contents="child	quota=1G=local"
		g_pending_backup_file_contents=".	compression=lz4=local"
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when pending-row inspection errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Pending-row inspection failures should surface the lower-level buffered-record inspection error that terminates the shell." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect buffered backup metadata records."
	assertContains "Pending-row inspection failures should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<child	quota=1G=local>"
	assertContains "Pending-row inspection failures should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<.	compression=lz4=local>"
}

test_finalize_deferred_backup_metadata_record_rethrows_unexpected_pending_lookup_status_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents="child	quota=1G=local"
		g_pending_backup_file_contents=".	compression=lz4=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 7
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed on unexpected pending-row inspection statuses." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Unexpected pending-row inspection statuses should identify the deferred source dataset." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect deferred backup metadata row for source dataset [tank/src]."
	assertContains "Unexpected pending-row inspection statuses should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<child	quota=1G=local>"
	assertContains "Unexpected pending-row inspection statuses should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<.	compression=lz4=local>"
}

test_render_backup_metadata_contents_preserves_write_format_without_mutating_accumulator() {
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src/child"
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src/child"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	rendered=$(zxfer_render_backup_metadata_contents)

	assertContains "Rendered backup metadata should include the current header and backup rows." \
		"$rendered" "#zxfer property backup file"
	assertContains "Rendered backup metadata should declare the dedicated backup-metadata format version." \
		"$rendered" "#format_version:2"
	assertContains "Rendered backup metadata should record the full source dataset root instead of only the tail component." \
		"$rendered" "#source_root:tank/src/child"
	assertContains "Rendered backup metadata should record the full destination dataset root for the run." \
		"$rendered" "#destination_root:backup/dst"
	assertNotContains "Rendered backup metadata should no longer emit the retired initial_source compatibility alias." \
		"$rendered" "#initial_source:"
	assertNotContains "Rendered backup metadata should no longer emit the retired destination compatibility alias." \
		"$rendered" "#destination:"
	assertContains "Rendered backup metadata should preserve the v2 relative property row payload." \
		"$rendered" ".	compression=lz4=local"
	assertEquals "Rendering backup metadata should not mutate the owner accumulator scratch state." \
		".	compression=lz4=local" "$g_backup_file_contents"
}

test_render_backup_metadata_contents_sets_render_scratch_in_current_shell() {
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	zxfer_render_backup_metadata_contents >/dev/null

	assertContains "Rendering backup metadata in the current shell should populate the rendered-content scratch channel." \
		"$g_zxfer_rendered_backup_metadata_contents" "#source_root:tank/src"
	assertContains "Current-shell rendering should preserve the relative backup row in scratch output too." \
		"$g_zxfer_rendered_backup_metadata_contents" ".	compression=lz4=local"
}

test_render_backup_metadata_contents_emits_stdout_and_sets_render_scratch_in_current_shell() {
	rendered_file="$TEST_TMPDIR/render_backup_metadata_current_shell.out"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	zxfer_render_backup_metadata_contents >"$rendered_file"

	assertContains "Current-shell backup-metadata rendering should still emit the rendered header on stdout." \
		"$(cat "$rendered_file")" "#source_root:tank/src"
	assertContains "Current-shell backup-metadata rendering should still emit the relative backup row on stdout." \
		"$(cat "$rendered_file")" ".	compression=lz4=local"
	assertEquals "Current-shell backup-metadata rendering should keep stdout and scratch output aligned." \
		"$(cat "$rendered_file")" "$g_zxfer_rendered_backup_metadata_contents"
}

test_render_forwarded_backup_metadata_contents_sets_render_scratch_in_current_shell() {
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst/src"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	zxfer_render_forwarded_backup_metadata_contents >/dev/null

	assertContains "Forwarded backup rendering in the current shell should advertise the destination tree as the forwarded source root." \
		"$g_zxfer_rendered_backup_metadata_contents" "#source_root:backup/dst/src"
	assertContains "Forwarded backup rendering should preserve the relative root row under the forwarded destination header." \
		"$g_zxfer_rendered_backup_metadata_contents" ".	compression=lz4=local"
}

test_render_forwarded_backup_metadata_contents_emits_stdout_and_sets_render_scratch_in_current_shell() {
	rendered_file="$TEST_TMPDIR/render_forwarded_backup_metadata_current_shell.out"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst/src"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")

	zxfer_render_forwarded_backup_metadata_contents >"$rendered_file"

	assertContains "Current-shell forwarded backup rendering should emit the forwarded source root on stdout." \
		"$(cat "$rendered_file")" "#source_root:backup/dst/src"
	assertContains "Current-shell forwarded backup rendering should emit the relative forwarded row on stdout." \
		"$(cat "$rendered_file")" ".	compression=lz4=local"
	assertEquals "Current-shell forwarded backup rendering should keep stdout and scratch output aligned." \
		"$(cat "$rendered_file")" "$g_zxfer_rendered_backup_metadata_contents"
}

test_render_current_backup_metadata_fixture_infers_header_roots_from_first_row_when_globals_do_not_match() {
	g_initial_source="tank/src"
	g_destination="backup/dst"

	rendered=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")

	assertContains "Current-format backup fixtures should use the ambient source root for v2 relative rows." \
		"$rendered" "#source_root:tank/src"
	assertContains "Current-format backup fixtures should use the ambient destination root for v2 relative rows." \
		"$rendered" "#destination_root:backup/dst"
	assertNotContains "Current-format backup fixtures should not emit the retired initial_source compatibility alias." \
		"$rendered" "#initial_source:"
	assertNotContains "Current-format backup fixtures should not emit the retired destination compatibility alias." \
		"$rendered" "#destination:"
	assertContains "Current-format backup fixtures should preserve the relative metadata row." \
		"$rendered" ".	compression=lz4=local"
}

test_backup_metadata_file_key_uses_lossless_source_destination_identity_hex() {
	outfile="$TEST_TMPDIR/backup_key_identity_hex.out"

	zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$outfile"
	result=$(cat "$outfile")

	assertEquals "Backup metadata key derivation should encode the exact source/destination identity instead of a 32-bit checksum." \
		"h/74616e6b2f7372630a6261636b75702f647374" "$result"
}

test_backup_metadata_file_key_fails_when_identity_hex_cannot_be_derived() {
	od() {
		return 1
	}

	zxfer_backup_metadata_file_key "tank/src" "backup/dst" >/dev/null
	status=$?

	assertEquals "Backup metadata key derivation should fail closed if the exact identity cannot be encoded." \
		1 "$status"
}

test_backup_metadata_legacy_file_key_preserves_caller_ifs_and_globbing() {
	custom_output=$TEST_TMPDIR/backup_legacy_key_custom.out
	custom_state=$TEST_TMPDIR/backup_legacy_key_custom.state
	unset_output=$TEST_TMPDIR/backup_legacy_key_unset.out
	unset_state=$TEST_TMPDIR/backup_legacy_key_unset.state

	(
		cksum() {
			printf '%s\n' '12345 67'
		}
		IFS=:
		set -f
		zxfer_backup_metadata_legacy_file_key 'tank/*' 'backup/dst' >"$custom_output"
		printf 'ifs=%s\n' "$IFS" >"$custom_state"
		case $- in
		*f*) printf '%s\n' 'globbing=disabled' >>"$custom_state" ;;
		*) printf '%s\n' 'globbing=enabled' >>"$custom_state" ;;
		esac
	)

	(
		cksum() {
			printf '%s\n' '54321 76'
		}
		unset IFS
		set +f
		zxfer_backup_metadata_legacy_file_key 'tank/*' 'backup/dst' >"$unset_output"
		if [ "${IFS+set}" = set ]; then
			printf '%s\n' 'ifs=set' >"$unset_state"
		else
			printf '%s\n' 'ifs=unset' >"$unset_state"
		fi
		case $- in
		*f*) printf '%s\n' 'globbing=disabled' >>"$unset_state" ;;
		*) printf '%s\n' 'globbing=enabled' >>"$unset_state" ;;
		esac
	)

	assertEquals "Legacy backup keys should parse cksum output independently of a custom caller IFS." \
		'k12345.67' "$(cat "$custom_output")"
	assertEquals "Legacy backup key parsing should preserve a custom IFS and disabled globbing." \
		"ifs=:
globbing=disabled" "$(cat "$custom_state")"
	assertEquals "Legacy backup keys should parse cksum output when caller IFS is unset." \
		'k54321.76' "$(cat "$unset_output")"
	assertEquals "Legacy backup key parsing should preserve unset IFS and enabled globbing." \
		"ifs=unset
globbing=enabled" "$(cat "$unset_state")"
}

test_backup_metadata_filenames_distinguish_known_legacy_cksum_collision_pairs() {
	g_backup_file_extension=".zxfer_backup_info"

	first_name=$(zxfer_get_backup_metadata_filename "tank/lixntn/src" "backup/0135l2/src")
	second_name=$(zxfer_get_backup_metadata_filename "tank/cp4hgv/src" "backup/8pnm4u/src")
	first_legacy_name=$(zxfer_get_legacy_backup_metadata_filename "tank/lixntn/src" "backup/0135l2/src")
	second_legacy_name=$(zxfer_get_legacy_backup_metadata_filename "tank/cp4hgv/src" "backup/8pnm4u/src")

	assertEquals "The direct repro pairs should still document the retired checksum filename collision." \
		"$first_legacy_name" "$second_legacy_name"
	assertNotEquals "Current exact-pair backup metadata filenames should distinguish source/destination pairs that collided under cksum." \
		"$first_name" "$second_name"
	assertContains "Current backup metadata filenames should carry the first pair's lossless identity key." \
		"$first_name" "h/74616e6b2f6c69786e746e2f7372630a6261636b75702f30"
	assertContains "Current backup metadata filenames should carry the rest of the first pair's lossless identity key." \
		"$first_name" "3133356c322f737263"
	assertContains "Current backup metadata filenames should carry the second pair's lossless identity key." \
		"$second_name" "h/74616e6b2f6370346867762f7372630a6261636b75702f38"
	assertContains "Current backup metadata filenames should carry the rest of the second pair's lossless identity key." \
		"$second_name" "706e6d34752f737263"
}

test_backup_metadata_filename_chunks_long_lossless_identity_components() {
	g_backup_file_extension=".zxfer_backup_info"
	long_source="tank/ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss"
	long_destination="backup/dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

	name=$(zxfer_get_backup_metadata_filename "$long_source" "$long_destination")
	longest_component=$(
		printf '%s\n' "$name" |
			awk -F/ '{
				max = 0
				for (i = 1; i <= NF; i++)
					if (length($i) > max)
						max = length($i)
				print max
			}'
	)

	assertContains "Current backup metadata filenames should place the lossless identity under the v2 metadata directory." \
		"$name" ".zxfer_backup_info.v2/h/"
	assertTrue "Current backup metadata filenames should avoid long path components." \
		"[ \"$longest_component\" -le 48 ]"
}

test_read_remote_backup_file_quotes_dash_prefixed_paths() {
	ssh_log="$TEST_TMPDIR/read_remote_dash.log"
	ssh_bin="$TEST_TMPDIR/read_remote_dash_ssh"
	outfile="$TEST_TMPDIR/read_remote_dash.out"
	cat >"$ssh_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >"$ssh_log"
printf '%s\n' "backup-data"
exit 0
EOF
	chmod +x "$ssh_bin"
	g_cmd_ssh="$ssh_bin"
	g_cmd_cat="/bin/cat"

	zxfer_read_remote_backup_file "backup@example.com" "-remote_backup_file" >"$outfile"
	status=$?

	assertEquals "Successful remote backup reads should preserve the ssh exit status." 0 "$status"
	assertEquals "Successful remote backup reads should pass through the remote file contents." \
		"backup-data" "$(cat "$outfile")"
	assertContains "Remote backup metadata reads should scope auxiliary tools to the secure dependency path before running the guard script." \
		"$(cat "$ssh_log")" "PATH='"
	assertContains "Dash-prefixed remote metadata paths should still be preserved in the staged remote helper command." \
		"$(cat "$ssh_log")" "-remote_backup_file"
	assertContains "Dash-prefixed remote metadata reads should now validate and read through the staged snapshot path." \
		"$(cat "$ssh_log")" "backup.snapshot"
}

test_build_remote_backup_symlink_guard_cmd_rejects_unknown_kind() {
	set +e
	output=$(zxfer_build_remote_backup_symlink_guard_cmd "/var/db/zxfer/backup.meta" 98 unknown)
	status=$?
	set -e

	assertEquals "Remote backup symlink guard rendering should reject unknown guard kinds." \
		1 "$status"
	assertEquals "Remote backup symlink guard rendering should not emit a partial command for unknown guard kinds." \
		"" "$output"
}

test_write_backup_properties_renders_remote_dry_run_command() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example doas"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_cmd_ssh="/usr/bin/ssh"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	expected_forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	expected_forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$expected_forwarded_root")

	result=$(
		(
			g_zxfer_dependency_path="/stale/secure/path"
			ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			zxfer_write_backup_properties
		)
	)

	assertContains "Remote dry-run backup writes should render the ssh command prefix." \
		"$result" "'/usr/bin/ssh'"
	assertContains "Remote dry-run backup writes should target the ssh host separately from wrapper tokens." \
		"$result" "'target.example'"
	assertContains "Remote dry-run backup writes should render the combined newline backup-content payload with the common argv formatter." \
		"$result" "'printf' '%s
%s
%s
'"
	assertContains "Remote dry-run backup writes should still include the secure-PATH wrapper in the rendered remote command." \
		"$result" "PATH="
	assertContains "Remote dry-run backup writes should refresh the secure-PATH wrapper from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$result" "/fresh/secure/path:/usr/bin"
	assertNotContains "Remote dry-run backup writes should not keep rendering a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$result" "/stale/secure/path"
	assertContains "Remote dry-run backup writes should preserve wrapper tokens in the rendered remote pipeline." \
		"$result" "doas"
	assertContains "Remote dry-run backup writes should preview the remote target guards that live writes now enforce." \
		"$result" "Refusing to write backup metadata because the target is a symlink."
	assertContains "Remote dry-run backup writes should stage remote writes through mktemp before the final rename." \
		"$result" "mktemp -d"
	assertEquals "Remote dry-run backup writes should now preview the primary metadata file and forwarded alias as one transactional command." \
		1 "$(printf '%s\n' "$result" | wc -l | tr -d '[:space:]')"
	assertContains "Remote dry-run backup writes should preview rollback staging for the forwarded alias and primary file." \
		"$result" ".zxfer-backup-rollback"
	assertContains "Remote dry-run backup writes should render the final secure backup path." \
		"$result" "$expected_name"
	assertContains "Remote dry-run backup writes should also preview the forwarded provenance alias path for chained backups." \
		"$result" "$expected_forwarded_name"
}

test_write_backup_properties_renders_local_dry_run_command() {
	g_option_n_dryrun=1
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	expected_forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	expected_forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$expected_forwarded_root")

	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			zxfer_write_backup_properties
		)
	)

	assertContains "Local dry-run backup writes should render the transactional local staging pipeline before the final renames." \
		"$result" "umask 077; l_primary_stage_dir=\$(mktemp -d"
	assertContains "Local dry-run backup writes should stage through mktemp before the final rename." \
		"$result" "mktemp -d"
	assertEquals "Local dry-run backup writes should now preview the primary metadata file and forwarded alias as one transactional command." \
		1 "$(printf '%s\n' "$result" | wc -l | tr -d '[:space:]')"
	assertContains "Local dry-run backup writes should preview rollback staging for the forwarded alias and primary file." \
		"$result" ".zxfer-backup-rollback"
	assertContains "Local dry-run backup writes should target the secure backup path." \
		"$result" "$expected_name"
	assertContains "Local dry-run backup writes should also preview the forwarded provenance alias path for chained backups." \
		"$result" "$expected_forwarded_name"
}

test_write_backup_properties_renders_single_file_local_dry_run_command_without_forwarded_alias() {
	g_option_n_dryrun=1
	g_option_T_target_host=""
	g_destination="tank/src"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")

	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			zxfer_write_backup_properties
		)
	)

	assertContains "Single-file local dry-run backup writes should render the local mktemp staging pipeline." \
		"$result" "umask 077; l_stage_dir=\$(mktemp -d"
	assertContains "Single-file local dry-run backup writes should render the final secure metadata path." \
		"$result" "$expected_name"
	assertNotContains "Single-file local dry-run backup writes should not preview pair-write rollback staging." \
		"$result" ".zxfer-backup-rollback"
	assertNotContains "Single-file local dry-run backup writes should not render the pair-write helper variable names." \
		"$result" "l_primary_stage_dir"
}

test_write_backup_properties_renders_single_file_remote_dry_run_command_without_forwarded_alias() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example doas"
	g_destination="tank/src"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_cmd_ssh="/usr/bin/ssh"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")

	result=$(
		(
			g_zxfer_dependency_path="/stale/secure/path"
			ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			zxfer_write_backup_properties
		)
	)

	assertContains "Single-file remote dry-run backup writes should render the ssh command prefix." \
		"$result" "'/usr/bin/ssh'"
	assertContains "Single-file remote dry-run backup writes should scope the helper command to the refreshed secure PATH." \
		"$result" "/fresh/secure/path:/usr/bin"
	assertContains "Single-file remote dry-run backup writes should render the final secure metadata path." \
		"$result" "$expected_name"
	assertContains "Single-file remote dry-run backup writes should render the remote write helper command." \
		"$result" "backup.write"
	assertNotContains "Single-file remote dry-run backup writes should not render the pair-write split marker." \
		"$result" "__ZXFER_BACKUP_METADATA_PAIR_SPLIT__"
}

test_write_backup_properties_rejects_remote_dry_run_when_single_file_host_spec_split_fails() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example"
	g_destination="tank/src"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1

	set +e
	output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid remote host spec"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Single-file remote dry-run backup rendering should fail closed when host tokenization fails." \
		1 "$status"
	assertContains "Single-file remote dry-run backup rendering should preserve the host-tokenization error." \
		"$output" "invalid remote host spec"
}

test_write_backup_properties_rejects_remote_dry_run_when_pair_host_spec_split_fails() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid remote host spec"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Transactional remote dry-run backup rendering should fail closed when host tokenization fails." \
		1 "$status"
	assertContains "Transactional remote dry-run backup rendering should preserve the host-tokenization error." \
		"$output" "invalid remote host spec"
}

test_render_remote_backup_dry_run_shell_command_preserves_prepare_failures() {
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw() {
		return 67
	}

	output=$(zxfer_render_remote_backup_dry_run_shell_command "target.example" "remote backup command")
	status=$?

	assertEquals "Remote backup dry-run rendering should preserve prepared-SSH helper failures." \
		67 "$status"
	assertEquals "Remote backup dry-run rendering should not publish partial command text on prepared-SSH failures." \
		"" "$output"
	assertEquals "Remote backup dry-run rendering should clear stale result scratch on prepared-SSH failures." \
		"" "$g_zxfer_remote_backup_dry_run_shell_command_result"
}

test_write_backup_properties_preserves_single_file_remote_dry_run_render_failures() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example"
	g_destination="tank/src"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/single_failure_backup_store"
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	zxfer_render_remote_backup_dry_run_shell_command() {
		return 72
	}

	output=$(zxfer_write_backup_properties)
	status=$?

	assertEquals "Single-file remote dry-run backup writes should preserve remote display-render failures." \
		72 "$status"
	assertEquals "Single-file remote dry-run backup writes should not emit partial dry-run command text after render failure." \
		"" "$output"
}

test_write_backup_properties_preserves_pair_remote_dry_run_render_failures() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR/pair_failure_backup_store"
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	zxfer_render_remote_backup_dry_run_shell_command() {
		return 73
	}

	output=$(zxfer_write_backup_properties)
	status=$?

	assertEquals "Pair remote dry-run backup writes should preserve remote display-render failures." \
		73 "$status"
	assertEquals "Pair remote dry-run backup writes should not emit partial dry-run command text after render failure." \
		"" "$output"
}

test_write_backup_properties_preserves_encoded_delimiter_heavy_payloads() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_write_encoded"
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local")
	g_initial_source="tank/src"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")

	zxfer_write_backup_properties

	written_file="$g_backup_storage_root/tank/src/$expected_name"

	assertTrue "Backup-property writes should create a metadata file under the secure backup root." \
		"[ -f \"$written_file\" ]"
	assertEquals "Backup-property writes should use the source dataset tree instead of the destination mountpoint tree." \
		"$g_backup_storage_root/tank/src/$expected_name" "$written_file"
	assertContains "Backup-property writes should record the full source dataset root in the metadata header." \
		"$(cat "$written_file")" "#source_root:tank/src"
	assertContains "Backup-property writes should record the full destination root in the metadata header." \
		"$(cat "$written_file")" "#destination_root:backup/dst"
	assertNotContains "Backup-property writes should no longer emit the retired initial_source compatibility alias." \
		"$(cat "$written_file")" "#initial_source:"
	assertNotContains "Backup-property writes should no longer emit the retired destination compatibility alias." \
		"$(cat "$written_file")" "#destination:"
	assertContains "Backup-property writes should preserve encoded delimiter-heavy property payloads as one metadata row." \
		"$(cat "$written_file")" ".	user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local"
}

test_write_backup_properties_collapses_buffered_duplicate_rows_newest_wins() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_write_duplicates"
	g_zxfer_version="test-version"
	g_backup_file_contents=$(printf '%s\n%s\n%s' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")" \
		"$(zxfer_test_backup_metadata_row "." "readonly=on=local")")
	g_initial_source="tank/src"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")

	zxfer_write_backup_properties

	written_file="$g_backup_storage_root/tank/src/$expected_name"

	assertContains "Write-boundary validation should publish the newest buffered row for a duplicated key." \
		"$(cat "$written_file")" ".	readonly=on=local"
	assertNotContains "Write-boundary validation should not publish shadowed duplicate rows." \
		"$(cat "$written_file")" ".	compression=lz4=local"
	assertEquals "Write-boundary validation should compact the buffered rows to their canonical newest-wins equivalent." \
		".	readonly=on=local
child	quota=1G=local" "$g_backup_file_contents"
}

test_write_backup_properties_rejects_malformed_buffered_rows_before_writing_any_files() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_write_malformed"
	g_zxfer_version="test-version"
	g_initial_source="tank/src"

	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_backup_file_contents=$(printf "%s\n%s" "broken,row-without-properties" ".	compression=lz4=local")
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			exit 1
		}
		zxfer_write_backup_properties
	'

	assertEquals "Backup-property writes should fail closed when a malformed buffered row reaches the write boundary." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed buffered rows should surface the write-boundary validation error text." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to validate buffered backup metadata records for chained backup provenance."
	assertTrue "Malformed buffered rows should stop the write before any metadata file is published." \
		"[ ! -d \"$g_backup_storage_root\" ] || [ -z \"\$(find \"$g_backup_storage_root\" -type f 2>/dev/null)\" ]"
}

test_write_backup_properties_writes_forwarded_provenance_alias_for_actual_destination_tree() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="backup/dst/src"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_forwarded_alias"
	g_zxfer_version="test-version"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_backup_file_contents=$(printf '%s\n%s\n' \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")")

	primary_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$forwarded_root")

	zxfer_write_backup_properties

	primary_file="$g_backup_storage_root/tank/src/$primary_name"
	forwarded_file="$g_backup_storage_root/$forwarded_root/$forwarded_name"

	assertTrue "Primary backup-property writes should still create the source-tree keyed metadata file." \
		"[ -f \"$primary_file\" ]"
	assertTrue "Backup-property writes should also create a forwarded provenance alias under the actual destination tree for later chained -k runs." \
		"[ -f \"$forwarded_file\" ]"
	assertContains "Forwarded provenance aliases should advertise the actual destination tree as both source_root and destination_root." \
		"$(cat "$forwarded_file")" "#source_root:backup/dst/src"
	assertContains "Forwarded provenance aliases should keep the actual destination tree as destination_root too." \
		"$(cat "$forwarded_file")" "#destination_root:backup/dst/src"
	assertContains "Forwarded provenance aliases should keep the relative root row under the forwarded destination header." \
		"$(cat "$forwarded_file")" ".	compression=lz4=local"
	assertContains "Forwarded provenance aliases should keep descendant rows relative for chained child restores." \
		"$(cat "$forwarded_file")" "child	quota=1G=local"
}

test_write_backup_properties_rethrows_forwarded_validation_failures_before_writing_any_files() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="backup/dst/src"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_forwarded_validation_failure"
	g_zxfer_version="test-version"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	primary_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$forwarded_root")
	primary_file="$g_backup_storage_root/tank/src/$primary_name"
	forwarded_file="$g_backup_storage_root/$forwarded_root/$forwarded_name"

	set +e
	output=$(
		(
			zxfer_validate_backup_metadata_record_list() {
				zxfer_throw_error "validation failed"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	write_status=$?
	set -e
	primary_exists=0
	forwarded_exists=0
	if [ -e "$primary_file" ]; then
		primary_exists=1
	fi
	if [ -e "$forwarded_file" ]; then
		forwarded_exists=1
	fi

	assertEquals "Backup-property writes should fail closed when forwarded-provenance validation errors." \
		1 "$write_status"
	assertContains "Forwarded-provenance validation failures should surface the original error instead of writing a header-only alias." \
		"$output" "validation failed"
	assertEquals "Forwarded-provenance validation failures should stop before writing the primary backup metadata file." \
		0 "$primary_exists"
	assertEquals "Forwarded-provenance validation failures should stop before writing the forwarded provenance alias file." \
		0 "$forwarded_exists"
}

test_write_backup_properties_and_get_backup_properties_share_dataset_tree_layout() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination/src"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_shared_layout"
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_option_O_origin_host=""

	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "/mnt/source"
	}

	set +e
	zxfer_write_backup_properties
	write_status=$?
	zxfer_get_backup_properties
	read_status=$?
	set -e

	assertEquals "Backup-property writes should succeed before restore re-reads the matching dataset-tree metadata file." \
		0 "$write_status"
	assertEquals "Backup-property restore should succeed when reading back the exact dataset-tree metadata layout written by the matching backup run." \
		0 "$read_status"

	assertContains "Backup-property restore should find the file written by the matching recursive backup run via the exact secure dataset-tree path." \
		"$g_restored_backup_file_contents" ".	compression=lz4=local"
}

test_write_backup_properties_routes_single_file_layout_through_single_file_store_helper() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="tank/src"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_single_file_route"
	g_zxfer_version="test-version"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	captured_args=""
	pair_called=0

	zxfer_write_backup_metadata_contents_to_store() {
		captured_args="$1|$2|$3"
	}
	zxfer_write_backup_metadata_pair_contents_to_store() {
		pair_called=1
	}

	zxfer_write_backup_properties

	assertContains "Single-file backup writes should route through the single-file storage helper when no forwarded alias is needed." \
		"$captured_args" "$g_backup_storage_root/tank/src"
	assertContains "Single-file backup writes should pass the v2 metadata payload to the single-file storage helper." \
		"$captured_args" "#source_root:tank/src"
	assertEquals "Single-file backup writes should not route through the transactional pair helper when no forwarded alias is needed." \
		0 "$pair_called"
}

test_get_forwarded_backup_properties_for_source_reads_ancestor_forwarded_metadata_alias() {
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_forwarded_lookup"
	g_option_O_origin_host=""
	current_source_root="backup/dst/src"
	current_source="backup/dst/src/child"
	forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$current_source_root")
	forwarded_dir="$g_backup_storage_root/$current_source_root"
	forwarded_file="$forwarded_dir/$forwarded_name"
	zxfer_test_ensure_parent_dir "$forwarded_file"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="$current_source_root"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="$current_source_root"
	zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")" >"$forwarded_file"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$forwarded_file"

	result=$(zxfer_get_forwarded_backup_properties_for_source "$current_source")

	assertEquals "Forwarded provenance lookup should reuse the ancestor destination-tree alias for child datasets in later chained -k runs." \
		"quota=1G=local" "$result"
}

test_get_forwarded_backup_properties_for_source_reads_legacy_cksum_forwarded_alias() {
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_forwarded_legacy_lookup"
	g_option_O_origin_host=""
	current_source_root="backup/dst/src"
	current_source="backup/dst/src/child"
	forwarded_name=$(zxfer_get_legacy_backup_metadata_filename "$current_source_root" "$current_source_root")
	forwarded_dir="$g_backup_storage_root/$current_source_root"
	result_file="$TEST_TMPDIR/forwarded_legacy_lookup.out"
	mkdir -p "$forwarded_dir"
	ZXFER_TEST_BACKUP_SOURCE_ROOT="$current_source_root"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="$current_source_root"
	zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "quota=1G=local")" >"$forwarded_dir/$forwarded_name"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT
	chmod 600 "$forwarded_dir/$forwarded_name"

	zxfer_get_forwarded_backup_properties_for_source "$current_source" >"$result_file"
	result=$(cat "$result_file")

	assertEquals "Forwarded provenance lookup should read retired cksum-keyed aliases when no current lossless-key alias exists." \
		"quota=1G=local" "$result"
	assertEquals "Forwarded provenance lookup should remember the legacy alias path that satisfied the read." \
		"$forwarded_dir/$forwarded_name" "$g_zxfer_backup_restore_candidate_path_result"
}

test_get_forwarded_backup_properties_for_source_returns_missing_and_restores_saved_cache() {
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_forwarded_missing"
	g_option_O_origin_host=""
	g_restored_backup_file_contents="saved-cache"

	set +e
	zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
	status=$?
	set -e

	assertEquals "Missing forwarded provenance aliases should return a plain not-found status." \
		1 "$status"
	assertEquals "Missing forwarded provenance lookups should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_returns_missing_when_forwarded_filename_derivation_fails() {
	output=$(
		(
			set +e
			g_restored_backup_file_contents="saved-cache"
			zxfer_get_backup_metadata_filename() {
				return 1
			}
			result=$(zxfer_get_forwarded_backup_properties_for_source "backup/dst/src")
			printf 'status=%s\n' "$?"
			printf 'result=<%s>\n' "$result"
			printf 'restored=<%s>\n' "${g_restored_backup_file_contents:-}"
			printf 'forwarded=<%s>\n' "${g_forwarded_backup_properties:-}"
		)
	)

	assertContains "Forwarded provenance lookups should return a plain not-found status when the forwarded filename cannot be derived." \
		"$output" "status=1"
	assertContains "Forwarded provenance lookups should not print a forwarded property payload when the forwarded filename cannot be derived." \
		"$output" "result=<>"
	assertContains "Forwarded provenance lookups should restore the prior restored-backup scratch state when forwarded filename derivation fails." \
		"$output" "restored=<saved-cache>"
	assertContains "Forwarded provenance lookups should leave the forwarded-properties scratch empty when forwarded filename derivation fails." \
		"$output" "forwarded=<>"
}

test_get_forwarded_backup_properties_for_source_rejects_invalid_forwarded_backup_header() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_invalid_header"
			}
			zxfer_try_backup_restore_candidate() {
				return 6
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookups should fail closed when the nearest forwarded alias has an invalid header." \
		1 "$status"
	assertContains "Invalid forwarded provenance headers should identify the exact forwarded metadata file." \
		"$output" "does not start with the required zxfer backup metadata header"
	assertEquals "Rejected forwarded provenance headers should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_root_forwarded_alias_without_exact_row() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_missing_exact_root"
			}
			zxfer_try_backup_restore_candidate() {
				return 3
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the dedicated current-source alias exists but lacks its own relative row." \
		1 "$status"
	assertContains "Missing relative rows in the dedicated current-source forwarded alias should identify the alias file." \
		"$output" "does not contain a current-format relative row"
	assertEquals "Rejected dedicated current-source forwarded aliases should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_ambiguous_forwarded_rows() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_ambiguous_rows"
			}
			zxfer_try_backup_restore_candidate() {
				return 2
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias contains ambiguous relative rows." \
		1 "$status"
	assertContains "Ambiguous forwarded provenance rows should identify the alias file." \
		"$output" "contains multiple relative rows for source dataset backup/dst/src."
	assertEquals "Rejected ambiguous forwarded provenance aliases should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_malformed_forwarded_rows() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_malformed_rows"
			}
			zxfer_try_backup_restore_candidate() {
				return 4
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias has malformed current-format rows." \
		1 "$status"
	assertContains "Malformed forwarded provenance rows should identify the alias file." \
		"$output" "is malformed. Expected current-format relative-path and properties rows."
	assertEquals "Rejected malformed forwarded provenance aliases should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_forwarded_read_failures() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_read_failure"
			}
			zxfer_try_backup_restore_candidate() {
				return 5
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias cannot be read securely." \
		1 "$status"
	assertContains "Forwarded provenance read failures should identify the exact alias file." \
		"$output" "Failed to read forwarded backup property file"
	assertEquals "Forwarded provenance read failures should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_local_stage_failures() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_stage_failure"
			}
			zxfer_try_backup_restore_candidate() {
				return 10
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias cannot stage a secure local read." \
		1 "$status"
	assertContains "Forwarded provenance stage failures should identify the exact alias file." \
		"$output" "Failed to stage local forwarded backup property file"
	assertEquals "Forwarded provenance stage failures should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_forwarded_transport_failures() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"
	g_option_O_origin_host="backup@example.com"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_transport_failure"
			}
			zxfer_try_backup_restore_candidate() {
				printf '%s\n' "Host key verification failed." >&2
				return 8
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the remote alias probe hits a transport failure." \
		1 "$status"
	assertContains "Forwarded provenance transport failures should preserve the remote ssh diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Forwarded provenance transport failures should identify the exact alias file and host." \
		"$output" "Failed to contact origin host backup@example.com while reading forwarded backup property file"
	assertEquals "Forwarded provenance transport failures should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_exact_property_extract_failures() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_extract_fail"
			}
			zxfer_try_backup_restore_candidate() {
				g_restored_backup_file_contents="candidate-cache"
				return 0
			}
			zxfer_backup_metadata_extract_properties_for_dataset_pair() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the exact property extract step fails after a successful candidate match." \
		1 "$status"
	assertContains "Forwarded provenance extract failures should identify the forwarded metadata file that matched." \
		"$output" "Failed to extract forwarded backup properties from"
	assertEquals "Failed forwarded provenance extracts should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_unsupported_forwarded_format_version() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_bad_format"
			}
			zxfer_try_backup_restore_candidate() {
				return 7
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias declares an unsupported format version." \
		1 "$status"
	assertContains "Unsupported forwarded provenance format versions should identify the expected schema marker." \
		"$output" "does not declare supported zxfer backup metadata format version #format_version:2."
	assertEquals "Rejected unsupported forwarded provenance versions should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}

test_get_forwarded_backup_properties_for_source_rejects_unexpected_forwarded_validation_failures() {
	g_backup_file_extension=".zxfer_backup_info"
	g_restored_backup_file_contents="saved-cache"

	set +e
	output=$(
		(
			zxfer_get_forwarded_backup_metadata_filename() {
				printf '%s\n' ".zxfer_backup_info.forwarded"
			}
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR_PHYSICAL/forwarded_unexpected_failure"
			}
			zxfer_try_backup_restore_candidate() {
				return 9
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_forwarded_backup_properties_for_source "backup/dst/src"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias returns an unexpected validation status." \
		1 "$status"
	assertContains "Unexpected forwarded provenance validation failures should identify the exact alias file." \
		"$output" "Failed to validate forwarded backup property file"
	assertEquals "Unexpected forwarded provenance validation failures should restore the prior restored-backup scratch state." \
		"saved-cache" "$g_restored_backup_file_contents"
}
