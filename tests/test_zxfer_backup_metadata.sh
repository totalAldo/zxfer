#!/bin/sh
#
# shunit2 tests for zxfer_backup_metadata.sh restore/write helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

tearDown() {
	if effective_uid=$(zxfer_get_effective_user_uid 2>/dev/null); then
		rm -rf "$TEST_TMPDIR"/*.remote-capabilities."$effective_uid".d
		rm -rf "$TEST_TMPDIR"/*.s."$effective_uid".d
	fi
}

create_fake_ssh_bin() {
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_backup_metadata"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	set +e
	OPTIND=1
	unset -f zxfer_ensure_local_backup_dir
	unset -f zxfer_ensure_remote_backup_dir
	unset -f zxfer_get_backup_metadata_filename
	unset -f zxfer_get_backup_storage_dir_for_dataset_tree
	unset -f zxfer_invoke_ssh_shell_command_for_host
	unset -f zxfer_read_local_backup_file
	unset -f zxfer_read_remote_backup_file
	unset -f zxfer_resolve_remote_cli_command_safe
	unset -f zxfer_run_destination_zfs_cmd
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_throw_error
	unset -f zxfer_throw_error_with_usage
	unset -f cksum
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset ZXFER_BACKUP_DIR
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"
	TMPDIR="$TEST_TMPDIR"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_z_compress=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_g_grandfather_protection=""
	g_option_j_jobs=1
	g_option_m_migrate=0
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_dependency_path=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_dependency_path=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	g_ssh_supports_control_sockets=0
	g_zxfer_remote_capability_cache_wait_retries=5
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_backup_storage_root=""
	g_backup_file_extension=""
	g_backup_file_contents=""
	g_pending_backup_file_contents=""
	g_zxfer_backup_metadata_record_list_result=""
	g_zxfer_rendered_backup_metadata_contents=""
	g_zxfer_backup_file_read_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_forwarded_backup_properties=""
	g_restored_backup_file_contents=""
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_destination="backup/dst"
	g_actual_dest="backup/dst"
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	create_fake_ssh_bin
}

test_reset_backup_metadata_state_clears_accumulator_and_restore_cache() {
	g_backup_file_contents="stale-backup"
	g_pending_backup_file_contents="stale-pending"
	g_forwarded_backup_properties="stale-forwarded"
	g_restored_backup_file_contents="stale-restore"
	g_zxfer_backup_file_read_result="stale-read"

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
	assertEquals "The backup-metadata reset helper should clear forwarded provenance scratch state." \
		"" "$g_forwarded_backup_properties"
	assertEquals "The backup-metadata reset helper should clear restored backup contents." \
		"" "$g_restored_backup_file_contents"
}

test_backup_metadata_constant_getters_return_source_constants() {
	result=$(
		(
			# shellcheck source=src/zxfer_backup_metadata.sh
			. "$TESTS_DIR/../src/zxfer_backup_metadata.sh"
			printf 'header=%s\n' "$(zxfer_get_backup_metadata_header_line)"
			printf 'format=%s\n' "$(zxfer_get_backup_metadata_format_version)"
			printf 'split=%s\n' "$(zxfer_get_backup_metadata_pair_split_line)"
		)
	)

	assertContains "The backup metadata header getter should return the source-time header line." \
		"$result" "header=#zxfer property backup file"
	assertContains "The backup metadata format-version getter should return the source-time format version." \
		"$result" "format=1"
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

test_append_backup_metadata_record_preserves_existing_serialized_format() {
	g_backup_file_contents="existing"

	zxfer_append_backup_metadata_record "tank/src" "backup/dst" "compression=lz4=local"

	assertEquals "Backup-metadata appends should keep the existing serialized semicolon-delimited row format." \
		"existing;tank/src,backup/dst,compression=lz4=local" "$g_backup_file_contents"
}

test_append_backup_metadata_record_replaces_existing_exact_pair_row() {
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/other,backup/other,quota=1G=local"

	zxfer_append_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"

	assertEquals "Backup-metadata appends should replace an existing exact source/destination row instead of appending an ambiguous duplicate." \
		";tank/src,backup/dst,readonly=on=local;tank/other,backup/other,quota=1G=local" "$g_backup_file_contents"
}

test_append_backup_metadata_record_deduplicates_existing_exact_pair_rows() {
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/src,backup/dst,readonly=off=local"

	zxfer_append_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"

	assertEquals "Backup-metadata appends should collapse pre-existing exact-pair duplicates down to one updated row." \
		";tank/src,backup/dst,readonly=on=local" "$g_backup_file_contents"
}

test_append_backup_metadata_record_preserves_literal_backslashes() {
	g_backup_file_contents=';tank/src,backup/dst,user:path=C:\\temp\\new=local;tank/other,backup/other,user:path=E:\\keep\\me=local'

	zxfer_append_backup_metadata_record "tank/src" "backup/dst" 'user:path=D:\\archive\\more=local'

	assertEquals "Backup-metadata appends should preserve literal backslashes in both replacement rows and untouched existing rows." \
		';tank/src,backup/dst,user:path=D:\\archive\\more=local;tank/other,backup/other,user:path=E:\\keep\\me=local' "$g_backup_file_contents"
}

test_append_backup_metadata_record_preserves_malformed_nonpair_segments() {
	g_backup_file_contents=";broken,row-without-properties;tank/src,backup/dst,compression=lz4=local"

	zxfer_append_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"

	assertEquals "Backup-metadata appends should preserve unrelated malformed segments while still replacing the exact source/destination row." \
		";broken,row-without-properties;tank/src,backup/dst,readonly=on=local" "$g_backup_file_contents"
}

test_append_backup_metadata_record_rethrows_update_failures_without_clearing_existing_buffer() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_backup_file_contents="existing"
		g_cmd_awk=false
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			exit 1
		}
		zxfer_append_backup_metadata_record "tank/src" "backup/dst" "compression=lz4=local"
	'

	assertEquals "Backup-metadata append helpers should fail closed when the inner record updater errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Backup-metadata append helpers should surface the buffered-update failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to update buffered backup metadata records."
	assertContains "Backup-metadata append helpers should leave the existing buffered rows untouched on updater failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<existing>"
}

test_rekey_backup_metadata_record_list_to_destination_identity_rekeys_exact_rows() {
	result=$(zxfer_rekey_backup_metadata_record_list_to_destination_identity \
		";tank/src,backup/dst/src,compression=lz4=local;tank/src/child,backup/dst/src/child,quota=1G=local")

	assertEquals "Forwarded-provenance rekeying should replace each source field with the destination identity while preserving the property payloads." \
		";backup/dst/src,backup/dst/src,compression=lz4=local;backup/dst/src/child,backup/dst/src/child,quota=1G=local" "$result"
}

test_rekey_backup_metadata_record_list_to_destination_identity_reports_awk_failures() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_cmd_awk=false
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_rekey_backup_metadata_record_list_to_destination_identity \
			";tank/src,backup/dst,compression=lz4=local"
	'

	assertEquals "Forwarded-provenance rekeying should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Forwarded-provenance rekeying should surface the buffered-rekey failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to rekey buffered backup metadata records for chained backup provenance."
}

test_update_backup_metadata_record_list_reports_awk_failures() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_cmd_awk=false
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_update_backup_metadata_record_list \
			";tank/src,backup/dst,compression=lz4=local" \
			"tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Backup-metadata record updates should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Backup-metadata record updates should surface the buffered-update failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to update buffered backup metadata records."
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
			";tank/src,backup/dst,compression=lz4=local" \
			"tank/src" "backup/dst"
	'

	assertEquals "Backup-metadata record removals should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Backup-metadata record removals should surface the buffered-remove failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to remove buffered backup metadata records."
}

test_get_buffered_backup_metadata_record_properties_returns_missing_without_mutating_scratch() {
	g_zxfer_backup_metadata_record_properties_result="stale"
	outfile="$TEST_TMPDIR/get_buffered_props_missing.out"

	set +e
	zxfer_get_buffered_backup_metadata_record_properties \
		";tank/src,backup/dst,compression=lz4=local" \
		"tank/other" "backup/other" >"$outfile"
	status=$?
	set -e

	assertEquals "Buffered backup-metadata property lookups should return a plain missing status when no exact row exists." \
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
			";tank/src,backup/dst,compression=lz4=local" \
			"tank/src" "backup/dst"
	'

	assertEquals "Buffered backup-metadata property lookups should fail closed when the awk helper errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Buffered backup-metadata property lookups should surface the inspection failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect buffered backup metadata records."
	assertContains "Buffered backup-metadata property lookups should not clobber the prior scratch channel before the failure is raised." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "scratch=<stale>"
}

test_capture_backup_metadata_for_completed_transfer_buffers_live_rows_without_flushing() {
	log="$TEST_TMPDIR/capture_backup_live.log"
	: >"$log"
	g_option_k_backup_property_mode=1

	zxfer_write_backup_properties() {
		printf 'unexpected write\n' >>"$log"
	}

	zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "backup/dst" "compression=lz4=local"

	unset -f zxfer_write_backup_properties

	assertEquals "Completed transfers should buffer the backup row in memory first." \
		";tank/src,backup/dst,compression=lz4=local" "$g_backup_file_contents"
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
				printf 'unexpected append %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}

			zxfer_capture_backup_metadata_for_completed_transfer "backup/intermediate/src" "backup/final/src" "compression=off=local"
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
				printf 'unexpected append %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}

			zxfer_capture_backup_metadata_for_completed_transfer "backup/intermediate/src" "backup/final/src" "compression=off=local"
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
		printf 'backup_append %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
	}

	zxfer_capture_backup_metadata_for_completed_transfer "backup/intermediate/src" "backup/final/src" "compression=off=local"

	unset -f zxfer_get_temp_file
	unset -f zxfer_get_forwarded_backup_properties_for_source
	unset -f zxfer_append_backup_metadata_record

	assertContains "Forwarded provenance capture should use the helper-owned scratch value instead of a temp-file relay." \
		"$(cat "$append_log")" "backup_append backup/intermediate/src backup/final/src compression=lz4=local"
}

test_flush_captured_backup_metadata_if_live_flushes_and_restores_failure_stage() {
	log="$TEST_TMPDIR/flush_backup_live.log"
	: >"$log"
	g_option_k_backup_property_mode=1
	g_zxfer_failure_stage="property transfer"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"

	zxfer_write_backup_properties() {
		printf 'write stage=%s backup=%s\n' "$g_zxfer_failure_stage" "$g_backup_file_contents" >>"$log"
	}

	zxfer_flush_captured_backup_metadata_if_live

	unset -f zxfer_write_backup_properties

	assertEquals "Live backup flushes should write the already-buffered metadata." \
		"write stage=property transfer backup=;tank/src,backup/dst,compression=lz4=local" "$(cat "$log")"
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
			zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "backup/dst" "compression=lz4=local"
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
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/child,backup/child,quota=1G=local"

	zxfer_defer_buffered_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"

	assertEquals "Deferring a seeded dataset should remove its row from the flushable live buffer." \
		";tank/child,backup/child,quota=1G=local" "$g_backup_file_contents"
	assertEquals "Deferring a seeded dataset should move its row into the pending seeded buffer." \
		";tank/src,backup/dst,compression=lz4=local" "$g_pending_backup_file_contents"

	zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"

	assertEquals "Finalizing a seeded dataset should clear its pending seeded row." \
		"" "$g_pending_backup_file_contents"
	assertEquals "Finalizing a seeded dataset should restore the deferred pre-seed row into the flushable live buffer instead of overwriting it with later live properties." \
		";tank/child,backup/child,quota=1G=local;tank/src,backup/dst,compression=lz4=local" "$g_backup_file_contents"
}

test_defer_buffered_backup_metadata_record_rejects_missing_live_row() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";seed/src,seed/dst,readonly=on=local"
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when the live buffered row is missing." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Deferring buffered backup metadata should identify the missing live row instead of silently rebuilding it from later live properties." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Buffered backup metadata row for source dataset [tank/src] and destination [backup/dst] is missing."
	assertContains "Deferring buffered backup metadata should leave the flushable live buffer untouched when the row is missing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/child,backup/child,quota=1G=local>"
	assertContains "Deferring buffered backup metadata should leave the pending seeded buffer untouched when the row is missing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;seed/src,seed/dst,readonly=on=local>"
}

test_defer_buffered_backup_metadata_record_rejects_ambiguous_live_rows() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/src,backup/dst,readonly=on=local"
		g_pending_backup_file_contents=";seed/src,seed/dst,readonly=on=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 2
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when the live buffered rows are ambiguous." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Ambiguous live buffered rows should identify the exact source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Buffered backup metadata rows for source dataset [tank/src] and destination [backup/dst] are ambiguous."
	assertContains "Ambiguous live buffered rows should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/src,backup/dst,compression=lz4=local;tank/src,backup/dst,readonly=on=local>"
	assertContains "Ambiguous live buffered rows should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;seed/src,seed/dst,readonly=on=local>"
}

test_defer_buffered_backup_metadata_record_rejects_malformed_live_rows() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";broken,row-without-properties;tank/src,backup/dst,compression=lz4=local"
		g_pending_backup_file_contents=";seed/src,seed/dst,readonly=on=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 3
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when the live buffered rows are malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed live buffered rows should identify the exact source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Buffered backup metadata rows are malformed while deferring source dataset [tank/src] and destination [backup/dst]."
	assertContains "Malformed live buffered rows should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;broken,row-without-properties;tank/src,backup/dst,compression=lz4=local>"
	assertContains "Malformed live buffered rows should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;seed/src,seed/dst,readonly=on=local>"
}

test_defer_buffered_backup_metadata_record_rethrows_live_lookup_failures_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";seed/src,seed/dst,readonly=on=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 99
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when live-row inspection returns an unexpected status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Unexpected live-row inspection failures should identify the exact source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect buffered backup metadata row for source dataset [tank/src] and destination [backup/dst]."
	assertContains "Unexpected live-row inspection failures should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/src,backup/dst,compression=lz4=local;tank/child,backup/child,quota=1G=local>"
	assertContains "Unexpected live-row inspection failures should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;seed/src,seed/dst,readonly=on=local>"
}

test_defer_buffered_backup_metadata_record_rethrows_pending_update_failures_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";seed/src,seed/dst,readonly=on=local"
		zxfer_remove_backup_metadata_record_list() {
			g_zxfer_backup_metadata_record_list_result=";tank/child,backup/child,quota=1G=local"
			printf "%s\n" "$g_zxfer_backup_metadata_record_list_result"
		}
		zxfer_update_backup_metadata_record_list() {
			zxfer_throw_error "pending update failed"
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_defer_buffered_backup_metadata_record "tank/src" "backup/dst" "compression=lz4=local"
	'

	assertEquals "Deferring buffered backup metadata should fail closed when the pending-buffer update errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Deferring buffered backup metadata should surface the pending-buffer update failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending update failed"
	assertContains "Deferring buffered backup metadata should not partially remove the live row before the pending update succeeds." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/src,backup/dst,compression=lz4=local;tank/child,backup/child,quota=1G=local>"
	assertContains "Deferring buffered backup metadata should leave the pending buffer untouched on update failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;seed/src,seed/dst,readonly=on=local>"
}

test_finalize_deferred_backup_metadata_record_rethrows_live_buffer_update_failures_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"
		zxfer_remove_backup_metadata_record_list() {
			g_zxfer_backup_metadata_record_list_result=""
			printf "%s\n" "$g_zxfer_backup_metadata_record_list_result"
		}
		zxfer_update_backup_metadata_record_list() {
			zxfer_throw_error "live buffer update failed"
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when the live-buffer update errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Finalizing deferred backup metadata should surface the live-buffer update failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "live buffer update failed"
	assertContains "Finalizing deferred backup metadata should leave the live buffer untouched on update failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/child,backup/child,quota=1G=local>"
	assertContains "Finalizing deferred backup metadata should not clear the pending row before the live update succeeds." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;tank/src,backup/dst,compression=lz4=local>"
}

test_finalize_deferred_backup_metadata_record_rejects_missing_pending_row() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=""
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when the pending seeded row is missing." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing deferred backup rows should identify the exact source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Deferred backup metadata row for source dataset [tank/src] and destination [backup/dst] is missing."
}

test_finalize_deferred_backup_metadata_record_rejects_ambiguous_pending_rows() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";tank/src,backup/dst,compression=lz4=local;tank/src,backup/dst,readonly=on=local"
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when the pending seeded rows are ambiguous." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Ambiguous deferred backup rows should identify the exact source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Deferred backup metadata rows for source dataset [tank/src] and destination [backup/dst] are ambiguous."
}

test_finalize_deferred_backup_metadata_record_rejects_malformed_pending_rows() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";broken,row-without-properties;tank/src,backup/dst,compression=lz4=local"
		zxfer_throw_error() {
			printf "%s\n" "$1"
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when the pending seeded rows are malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed deferred backup rows should identify the exact source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Deferred backup metadata rows are malformed while finalizing source dataset [tank/src] and destination [backup/dst]."
}

test_finalize_deferred_backup_metadata_record_rethrows_pending_lookup_failures_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_cmd_awk=/definitely-missing-zxfer-awk
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst" "readonly=on=local"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed when pending-row inspection errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Pending-row inspection failures should surface the lower-level buffered-record inspection error that terminates the shell." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect buffered backup metadata records."
	assertContains "Pending-row inspection failures should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/child,backup/child,quota=1G=local>"
	assertContains "Pending-row inspection failures should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;tank/src,backup/dst,compression=lz4=local>"
}

test_finalize_deferred_backup_metadata_record_rethrows_unexpected_pending_lookup_status_without_mutating_buffers() {
	# shellcheck disable=SC2016
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=0
		g_backup_file_contents=";tank/child,backup/child,quota=1G=local"
		g_pending_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"
		zxfer_get_buffered_backup_metadata_record_properties() {
			return 7
		}
		zxfer_throw_error() {
			printf "%s\n" "$1" >&2
			printf "backup=<%s>\n" "$g_backup_file_contents" >&2
			printf "pending=<%s>\n" "$g_pending_backup_file_contents" >&2
			exit 1
		}
		zxfer_finalize_deferred_backup_metadata_record "tank/src" "backup/dst"
	'

	assertEquals "Finalizing deferred backup metadata should fail closed on unexpected pending-row inspection statuses." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Unexpected pending-row inspection statuses should identify the exact deferred source and destination pair." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Failed to inspect deferred backup metadata row for source dataset [tank/src] and destination [backup/dst]."
	assertContains "Unexpected pending-row inspection statuses should leave the live buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "backup=<;tank/child,backup/child,quota=1G=local>"
	assertContains "Unexpected pending-row inspection statuses should leave the pending buffer untouched." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "pending=<;tank/src,backup/dst,compression=lz4=local>"
}

test_render_backup_metadata_contents_preserves_write_format_without_mutating_accumulator() {
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src/child"
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src/child"
	g_backup_file_contents=";tank/src/child,backup/dst/child,compression=lz4=local"

	rendered=$(zxfer_render_backup_metadata_contents)

	assertContains "Rendered backup metadata should include the current header and backup rows." \
		"$rendered" "#zxfer property backup file"
	assertContains "Rendered backup metadata should declare the dedicated backup-metadata format version." \
		"$rendered" "#format_version:1"
	assertContains "Rendered backup metadata should record the full source dataset root instead of only the tail component." \
		"$rendered" "#source_root:tank/src/child"
	assertContains "Rendered backup metadata should record the full destination dataset root for the run." \
		"$rendered" "#destination_root:backup/dst"
	assertNotContains "Rendered backup metadata should no longer emit the retired initial_source compatibility alias." \
		"$rendered" "#initial_source:"
	assertNotContains "Rendered backup metadata should no longer emit the retired destination compatibility alias." \
		"$rendered" "#destination:"
	assertContains "Rendered backup metadata should preserve the serialized property row payload." \
		"$rendered" ";tank/src/child,backup/dst/child,compression=lz4=local"
	assertEquals "Rendering backup metadata should not mutate the owner accumulator scratch state." \
		";tank/src/child,backup/dst/child,compression=lz4=local" "$g_backup_file_contents"
}

test_render_backup_metadata_contents_sets_render_scratch_in_current_shell() {
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"

	zxfer_render_backup_metadata_contents >/dev/null

	assertContains "Rendering backup metadata in the current shell should populate the rendered-content scratch channel." \
		"$g_zxfer_rendered_backup_metadata_contents" "#source_root:tank/src"
	assertContains "Current-shell rendering should preserve the serialized backup row in scratch output too." \
		"$g_zxfer_rendered_backup_metadata_contents" ";tank/src,backup/dst,compression=lz4=local"
}

test_render_backup_metadata_contents_emits_stdout_and_sets_render_scratch_in_current_shell() {
	rendered_file="$TEST_TMPDIR/render_backup_metadata_current_shell.out"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"

	zxfer_render_backup_metadata_contents >"$rendered_file"

	assertContains "Current-shell backup-metadata rendering should still emit the rendered header on stdout." \
		"$(cat "$rendered_file")" "#source_root:tank/src"
	assertContains "Current-shell backup-metadata rendering should still emit the serialized backup row on stdout." \
		"$(cat "$rendered_file")" ";tank/src,backup/dst,compression=lz4=local"
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
	g_backup_file_contents=";tank/src,backup/dst/src,compression=lz4=local"

	zxfer_render_forwarded_backup_metadata_contents >/dev/null

	assertContains "Forwarded backup rendering in the current shell should advertise the destination tree as the forwarded source root." \
		"$g_zxfer_rendered_backup_metadata_contents" "#source_root:backup/dst/src"
	assertContains "Forwarded backup rendering should rekey the root row to the forwarded destination identity in scratch output." \
		"$g_zxfer_rendered_backup_metadata_contents" ";backup/dst/src,backup/dst/src,compression=lz4=local"
}

test_render_forwarded_backup_metadata_contents_emits_stdout_and_sets_render_scratch_in_current_shell() {
	rendered_file="$TEST_TMPDIR/render_forwarded_backup_metadata_current_shell.out"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_initial_source="tank/src"
	g_destination="backup/dst"
	g_actual_dest="backup/dst/src"
	g_backup_file_contents=";tank/src,backup/dst/src,compression=lz4=local"

	zxfer_render_forwarded_backup_metadata_contents >"$rendered_file"

	assertContains "Current-shell forwarded backup rendering should emit the forwarded source root on stdout." \
		"$(cat "$rendered_file")" "#source_root:backup/dst/src"
	assertContains "Current-shell forwarded backup rendering should emit the rekeyed forwarded row on stdout." \
		"$(cat "$rendered_file")" ";backup/dst/src,backup/dst/src,compression=lz4=local"
	assertEquals "Current-shell forwarded backup rendering should keep stdout and scratch output aligned." \
		"$(cat "$rendered_file")" "$g_zxfer_rendered_backup_metadata_contents"
}

test_render_current_backup_metadata_fixture_infers_header_roots_from_first_row_when_globals_do_not_match() {
	g_initial_source="tank/src"
	g_destination="backup/dst"

	rendered=$(zxfer_test_render_current_backup_metadata_contents \
		"tank/parent/child,backup/other,compression=lz4=local")

	assertContains "Current-format backup fixtures should infer source_root from the first explicit metadata row instead of unrelated ambient globals." \
		"$rendered" "#source_root:tank/parent/child"
	assertContains "Current-format backup fixtures should infer destination_root from the first explicit metadata row instead of unrelated ambient globals." \
		"$rendered" "#destination_root:backup/other"
	assertNotContains "Current-format backup fixtures should not emit the retired initial_source compatibility alias." \
		"$rendered" "#initial_source:"
	assertNotContains "Current-format backup fixtures should not emit the retired destination compatibility alias." \
		"$rendered" "#destination:"
	assertNotContains "Current-format backup fixtures should not leak the ambient source global into the rendered header when the row describes a different dataset." \
		"$rendered" "#source_root:tank/src"
	assertNotContains "Current-format backup fixtures should not leak the ambient destination global into the rendered header when the row describes a different destination." \
		"$rendered" "#destination_root:backup/dst"
}

test_backup_metadata_file_key_falls_back_to_hex_when_cksum_unavailable_in_current_shell() {
	outfile="$TEST_TMPDIR/backup_key_fallback.out"
	cksum() {
		return 1
	}

	zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$outfile"
	result=$(cat "$outfile")

	assertContains "Backup metadata key derivation should fall back to the hex-encoding path when cksum is unavailable." \
		"$result" "k"
	assertNotEquals "Backup metadata key derivation should not return an empty fallback key." \
		"" "$result"
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

test_write_backup_properties_renders_remote_dry_run_command() {
	g_option_n_dryrun=1
	g_option_T_target_host="target.example doas"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
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
	assertContains "Remote dry-run backup writes should render the combined backup-content payload with the common argv formatter." \
		"$result" "'printf' '%s;%s;%s'"
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
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
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
	g_backup_file_contents=";tank/src,tank/src,compression=lz4=local"
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
	g_backup_file_contents=";tank/src,tank/src,compression=lz4=local"
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
	g_backup_file_contents=";tank/src,tank/src,compression=lz4=local"
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
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
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

test_write_backup_properties_preserves_encoded_delimiter_heavy_payloads() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_write_encoded"
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local"
	g_initial_source="tank/src"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")

	zxfer_write_backup_properties

	written_file=$(find "$g_backup_storage_root" -name "$expected_name" -type f | head -n 1)

	assertTrue "Backup-property writes should create a metadata file under the secure backup root." \
		"[ -n \"$written_file\" ]"
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
		"$(cat "$written_file")" "tank/src,backup/dst,user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local"
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
	g_backup_file_contents=";tank/src,backup/dst/src,compression=lz4=local;tank/src/child,backup/dst/src/child,quota=1G=local"

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
	assertContains "Forwarded provenance aliases should rekey the root dataset row to the destination identity while preserving the original properties." \
		"$(cat "$forwarded_file")" "backup/dst/src,backup/dst/src,compression=lz4=local"
	assertContains "Forwarded provenance aliases should also rekey descendant rows to destination identities for chained child restores." \
		"$(cat "$forwarded_file")" "backup/dst/src/child,backup/dst/src/child,quota=1G=local"
}

test_write_backup_properties_rethrows_forwarded_rekey_failures_before_writing_any_files() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="backup/dst/src"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_forwarded_rekey_failure"
	g_zxfer_version="test-version"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_backup_file_contents=";tank/src,backup/dst/src,compression=lz4=local"
	primary_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$forwarded_root")
	primary_file="$g_backup_storage_root/tank/src/$primary_name"
	forwarded_file="$g_backup_storage_root/$forwarded_root/$forwarded_name"

	set +e
	output=$(
		(
			zxfer_rekey_backup_metadata_record_list_to_destination_identity() {
				zxfer_throw_error "rekey failed"
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

	assertEquals "Backup-property writes should fail closed when forwarded-provenance rekeying errors." \
		1 "$write_status"
	assertContains "Forwarded-provenance rekey failures should surface the original error instead of writing a header-only alias." \
		"$output" "rekey failed"
	assertEquals "Forwarded-provenance rekey failures should stop before writing the primary backup metadata file." \
		0 "$primary_exists"
	assertEquals "Forwarded-provenance rekey failures should stop before writing the forwarded provenance alias file." \
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
	g_backup_file_contents=";tank/src,backup/dst/src,compression=lz4"
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
		"$g_restored_backup_file_contents" "tank/src,backup/dst/src,compression=lz4"
}

test_write_backup_properties_routes_single_file_layout_through_single_file_store_helper() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="tank/src"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/backup_store_single_file_route"
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,tank/src,compression=lz4=local"
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
	assertContains "Single-file backup writes should pass the serialized metadata payload to the single-file storage helper." \
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
	mkdir -p "$forwarded_dir"
	zxfer_test_render_current_backup_metadata_contents \
		"backup/dst/src,backup/dst/src,compression=lz4=local" \
		"backup/dst/src/child,backup/dst/src/child,quota=1G=local" >"$forwarded_dir/$forwarded_name"
	chmod 600 "$forwarded_dir/$forwarded_name"

	result=$(zxfer_get_forwarded_backup_properties_for_source "$current_source")

	assertEquals "Forwarded provenance lookup should reuse the ancestor destination-tree alias for child datasets in later chained -k runs." \
		"quota=1G=local" "$result"
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
			zxfer_get_forwarded_backup_metadata_filename() {
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

	assertEquals "Forwarded provenance lookup should fail closed when the dedicated current-source alias exists but lacks its own exact row." \
		1 "$status"
	assertContains "Missing exact rows in the dedicated current-source forwarded alias should identify the alias file." \
		"$output" "does not contain an exact current-format entry"
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

	assertEquals "Forwarded provenance lookup should fail closed when the matched alias contains ambiguous exact rows." \
		1 "$status"
	assertContains "Ambiguous forwarded provenance rows should identify the exact alias file." \
		"$output" "contains multiple entries for source dataset backup/dst/src and destination backup/dst/src."
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
	assertContains "Malformed forwarded provenance rows should identify the exact alias file." \
		"$output" "is malformed. Expected current-format source,destination,properties rows."
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
		"$output" "does not declare supported zxfer backup metadata format version #format_version:1."
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
	backup_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"tank/src,backup/dst,compression=lz4")

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
	mkdir -p "$fallback_dir"
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
	mkdir -p "$direct_dir"
	zxfer_test_render_current_backup_metadata_contents \
		"tank/src/child,backup/other,compression=lz4" >"$direct_file"
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

	assertEquals "Exact secure backup files that lack the requested source/destination row should fail closed instead of falling back to ancestors." \
		1 "$status"
	assertContains "Non-matching exact secure backup files should identify the exact keyed backup path." \
		"$output" "$direct_file"
	assertContains "Non-matching exact secure backup files should report the missing exact entry." \
		"$output" "does not contain an exact current-format entry for source dataset tank/src/child and destination backup/dst."
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
	mkdir -p "$direct_dir"
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
	mkdir -p "$direct_dir"
	printf '%s\n%s\n%s\n%s\n%s\n' \
		"#legacy comment" \
		"#zxfer property backup file" \
		"#format_version:1" \
		"#version:test-version" \
		"tank/src/child,backup/dst,compression=lz4" >"$direct_file"
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
	mkdir -p "$direct_dir"
	printf '%s\n%s\n%s\n%s\n' \
		"#zxfer property backup file" \
		"#format_version:999" \
		"#version:test-version" \
		"tank/src/child,backup/dst,compression=lz4" >"$direct_file"
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
		"$output" "does not declare supported zxfer backup metadata format version #format_version:1."
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
	mkdir -p "$direct_dir"
	zxfer_test_render_current_backup_metadata_contents \
		"tank/src/child,backup/dst,compression=lz4,extra" >"$direct_file"
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
		"$output" "is malformed. Expected current-format source,destination,properties rows."
}

test_get_backup_properties_rejects_ambiguous_exact_pair_rows_in_direct_local_candidate() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/direct_ambiguous_store"
	g_initial_source="tank/src/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""
	g_backup_file_extension=".zxfer_backup_info"
	direct_dir="$g_backup_storage_root/tank/src/child"
	direct_file="$direct_dir/$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")"
	stdout_file="$TEST_TMPDIR/direct_ambiguous_local.out"
	mkdir -p "$direct_dir"
	zxfer_test_render_current_backup_metadata_contents \
		"tank/src/child,backup/dst,compression=lz4" \
		"tank/src/child,backup/dst,compression=off" >"$direct_file"
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

	assertEquals "Direct secure backup candidates should fail closed when they contain duplicate exact source/destination rows." 1 "$status"
	assertContains "Direct local candidate failures should identify the exact secure backup file." \
		"$output" "$direct_file"
	assertContains "Direct local candidate failures should identify the exact source/destination pair." \
		"$output" "contains multiple entries for source dataset tank/src/child and destination backup/dst."
}

test_get_backup_properties_rejects_ambiguous_exact_pair_rows_in_direct_remote_candidate() {
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
			g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
				"tank/src/child,backup/dst,compression=lz4" \
				"tank/src/child,backup/dst,compression=off")
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

	assertEquals "Direct remote secure backup candidates should fail closed when they contain duplicate exact source/destination rows." 1 "$status"
	assertContains "Direct remote candidate failures should identify the exact secure backup file." \
		"$output" "$direct_file"
	assertContains "Direct remote candidate failures should identify the exact source/destination pair." \
		"$output" "contains multiple entries for source dataset tank/src/child and destination backup/dst."
}

test_get_backup_properties_walks_up_to_parent_filesystem() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/ancestor_store"
	g_backup_file_extension=".zxfer_backup_info"
	parent_secure_dir="$g_backup_storage_root/tank/parent"
	mkdir -p "$parent_secure_dir"
	parent_backup="$parent_secure_dir/$(zxfer_get_backup_metadata_filename "tank/parent" "backup/dst")"
	zxfer_test_render_current_backup_metadata_contents \
		"tank/parent/child,backup/dst,compression=lz4" >"$parent_backup"
	chmod 600 "$parent_backup"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""

	zxfer_get_backup_properties

	assertContains "Backup-property discovery should walk up to ancestor datasets when the child has no metadata file." \
		"$g_restored_backup_file_contents" "tank/parent/child,backup/dst,compression=lz4"
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
	mkdir -p "$direct_dir" "$parent_dir"
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
	mkdir -p "$direct_dir" "$parent_dir"
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
	mkdir -p "$direct_dir" "$parent_dir"
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

test_get_backup_properties_reads_remote_ancestor_dataset_tree() {
	g_backup_storage_root="$TEST_TMPDIR/remote_ancestor_store"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host="backup@example.com"
	g_backup_file_extension=".zxfer_backup_info"
	parent_backup="$g_backup_storage_root/tank/parent/$(zxfer_get_backup_metadata_filename "tank/parent" "backup/dst")"

	zxfer_read_remote_backup_file() {
		if [ "$2" = "$parent_backup" ]; then
			g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
				"tank/parent/child,backup/dst,compression=lz4")
			return 0
		fi
		return 4
	}

	zxfer_get_backup_properties

	assertContains "Remote backup-property discovery should walk up to the ancestor dataset tree using the exact secure keyed path." \
		"$g_restored_backup_file_contents" "tank/parent/child,backup/dst,compression=lz4"
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

test_require_secure_backup_file_reports_unknown_owner_and_mode() {
	backup_file="$TEST_TMPDIR/secure_meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	owner_output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_require_secure_backup_file "$backup_file"
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_require_secure_backup_file "$backup_file"
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

test_require_secure_backup_file_rejects_non_0600_permissions() {
	backup_file="$TEST_TMPDIR/insecure_meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 644 "$backup_file"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_require_secure_backup_file "$backup_file"
		)
	)
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
			tr() {
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

	assertEquals "Atomic local backup writes should fail when the staged payload cannot be rendered." \
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
	rollback_file="$TEST_TMPDIR/local_atomic_finalize_register.rollback"
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

test_cleanup_backup_metadata_stage_dir_falls_back_to_rm_when_runtime_helpers_are_unavailable() {
	stage_dir="$TEST_TMPDIR/backup-stage-cleanup-fallback"
	mkdir -p "$stage_dir"
	printf '%s\n' "snapshot" >"$stage_dir/backup.snapshot"
	printf '%s\n' "write" >"$stage_dir/backup.write"

	unset -f zxfer_cleanup_runtime_artifact_path

	zxfer_cleanup_backup_metadata_stage_dir "$stage_dir"

	assertFalse "Backup stage-directory cleanup should remove the staged snapshot file when the runtime cleanup helper is unavailable." \
		"[ -e \"$stage_dir/backup.snapshot\" ]"
	assertFalse "Backup stage-directory cleanup should remove the staged write file when the runtime cleanup helper is unavailable." \
		"[ -e \"$stage_dir/backup.write\" ]"
	assertFalse "Backup stage-directory cleanup should remove the stage directory when the runtime cleanup helper is unavailable." \
		"[ -e \"$stage_dir\" ]"
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

test_commit_local_backup_file_stage_removes_rollback_file_when_restore_back_fails_after_stage_rename_failure() {
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
	assertEquals "Failed rollback restoration inside stage-commit recovery should remove the rollback file once recovery is exhausted." \
		0 "$rollback_exists"
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
	stage_dir="$TEST_TMPDIR/prepare_stage_write_failure.stage"
	mkdir -p "$stage_dir/backup.write" || fail "Unable to create the staged write-failure fixture."

	set +e
	output=$(
		(
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
	assertEquals "Preparing a local backup-file stage should rewrite semicolon-delimited payloads as newline-delimited staged file contents." \
		"#header
payload
trailer" "$(cat "$stage_file")"
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

test_write_local_backup_file_pair_atomically_leaves_primary_unchanged_when_forwarded_commit_fails() {
	primary_file="$TEST_TMPDIR/local_pair_forwarded_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_forwarded_fail_alias.meta"
	printf '%s' "old-primary" >"$primary_file"
	printf '%s' "old-forwarded" >"$forwarded_file"
	chmod 600 "$primary_file" "$forwarded_file"

	set +e
	status=$(
		(
			mv() {
				if [ "$1" = "-f" ] && [ "$3" = "$forwarded_file" ] && [ "${2##*/}" = "backup.write" ]; then
					return 1
				fi
				command mv "$@"
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;new-primary" "$forwarded_file" "#header;new-forwarded" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	leftover_write=$(find "$TEST_TMPDIR" -maxdepth 1 -type d -name '.zxfer-backup-write.*' | wc -l | tr -d '[:space:]')
	leftover_rollback=$(find "$TEST_TMPDIR" -maxdepth 1 -type f -name '.zxfer-backup-rollback.*' | wc -l | tr -d '[:space:]')

	assertEquals "Transactional local pair writes should fail when the forwarded alias commit fails." \
		1 "$status"
	assertEquals "Transactional local pair writes should leave the primary metadata untouched when the forwarded alias commit fails." \
		"old-primary" "$(cat "$primary_file")"
	assertEquals "Transactional local pair writes should leave the forwarded alias untouched when its own commit fails." \
		"old-forwarded" "$(cat "$forwarded_file")"
	assertEquals "Failed forwarded alias commits should clean up staged backup-write directories." \
		0 "$leftover_write"
	assertEquals "Failed forwarded alias commits should not leave rollback files behind." \
		0 "$leftover_rollback"
}

test_write_local_backup_file_pair_atomically_rolls_back_forwarded_commit_when_primary_commit_fails() {
	primary_file="$TEST_TMPDIR/local_pair_primary_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_primary_fail_alias.meta"
	printf '%s' "old-primary" >"$primary_file"
	printf '%s' "old-forwarded" >"$forwarded_file"
	chmod 600 "$primary_file" "$forwarded_file"

	set +e
	status=$(
		(
			mv() {
				if [ "$1" = "-f" ] && [ "$3" = "$primary_file" ] && [ "${2##*/}" = "backup.write" ]; then
					return 1
				fi
				command mv "$@"
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;new-primary" "$forwarded_file" "#header;new-forwarded" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	leftover_write=$(find "$TEST_TMPDIR" -maxdepth 1 -type d -name '.zxfer-backup-write.*' | wc -l | tr -d '[:space:]')
	leftover_rollback=$(find "$TEST_TMPDIR" -maxdepth 1 -type f -name '.zxfer-backup-rollback.*' | wc -l | tr -d '[:space:]')

	assertEquals "Transactional local pair writes should fail when the primary metadata commit fails." \
		1 "$status"
	assertEquals "Transactional local pair writes should restore the original primary metadata when the primary commit fails." \
		"old-primary" "$(cat "$primary_file")"
	assertEquals "Transactional local pair writes should roll back the forwarded alias when the primary commit fails after the alias commit." \
		"old-forwarded" "$(cat "$forwarded_file")"
	assertEquals "Failed primary commits should clean up staged backup-write directories." \
		0 "$leftover_write"
	assertEquals "Failed primary commits should not leave rollback files behind." \
		0 "$leftover_rollback"
}

test_write_local_backup_file_pair_atomically_registers_stale_rollbacks_before_finalize() {
	primary_file="$TEST_TMPDIR/local_pair_finalize_register_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_finalize_register_alias.meta"
	primary_stage_dir="$TEST_TMPDIR/local_pair_finalize_register.primary.stage"
	forwarded_stage_dir="$TEST_TMPDIR/local_pair_finalize_register.alias.stage"
	primary_stage_file="$primary_stage_dir/backup.write"
	forwarded_stage_file="$forwarded_stage_dir/backup.write"
	primary_rollback_file="$TEST_TMPDIR/local_pair_finalize_register.primary.rollback"
	forwarded_rollback_file="$TEST_TMPDIR/local_pair_finalize_register.alias.rollback"
	trace_file="$TEST_TMPDIR/local_pair_finalize_register.trace"
	mkdir -p "$primary_stage_dir" "$forwarded_stage_dir"
	printf '%s' "primary" >"$primary_stage_file"
	printf '%s' "forwarded" >"$forwarded_stage_file"
	printf '%s' "old-primary" >"$primary_rollback_file"
	printf '%s' "old-forwarded" >"$forwarded_rollback_file"

	output=$(
		(
			g_zxfer_runtime_artifact_cleanup_paths=""
			g_test_commit_calls=0
			zxfer_prepare_local_backup_file_stage() {
				if [ "$1" = "$primary_file" ]; then
					g_zxfer_backup_stage_dir_result=$primary_stage_dir
					g_zxfer_backup_stage_file_result=$primary_stage_file
				else
					g_zxfer_backup_stage_dir_result=$forwarded_stage_dir
					g_zxfer_backup_stage_file_result=$forwarded_stage_file
				fi
				return 0
			}
			zxfer_commit_local_backup_file_stage() {
				g_test_commit_calls=$((g_test_commit_calls + 1))
				g_zxfer_backup_commit_had_existing_target_result=1
				if [ "$g_test_commit_calls" -eq 1 ]; then
					g_zxfer_backup_commit_rollback_file_result=$forwarded_rollback_file
				else
					g_zxfer_backup_commit_rollback_file_result=$primary_rollback_file
				fi
				return 0
			}
			zxfer_finalize_local_backup_file_commit() {
				printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths" >"$trace_file"
				return 43
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;primary" "$forwarded_file" "#header;forwarded" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	trace_output=$(cat "$trace_file")

	assertContains "Transactional local pair writes should register the forwarded rollback file once both commits have succeeded." \
		"$trace_output" "$forwarded_rollback_file"
	assertContains "Transactional local pair writes should register the primary rollback file once both commits have succeeded." \
		"$trace_output" "$primary_rollback_file"
	assertContains "Transactional local pair writes should still preserve finalization failures after registering stale rollback files." \
		"$output" "status=43"
}

test_write_local_backup_file_pair_atomically_cleans_up_stage_dirs_when_primary_finalize_fails_after_forwarded_finalize_success() {
	primary_file="$TEST_TMPDIR/local_pair_primary_finalize_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_primary_finalize_fail_alias.meta"
	primary_stage_dir="$TEST_TMPDIR/local_pair_primary_finalize_fail.primary.stage"
	forwarded_stage_dir="$TEST_TMPDIR/local_pair_primary_finalize_fail.alias.stage"
	primary_stage_file="$primary_stage_dir/backup.write"
	forwarded_stage_file="$forwarded_stage_dir/backup.write"
	primary_rollback_file="$TEST_TMPDIR/local_pair_primary_finalize_fail.primary.rollback"
	forwarded_rollback_file="$TEST_TMPDIR/local_pair_primary_finalize_fail.alias.rollback"
	mkdir -p "$primary_stage_dir" "$forwarded_stage_dir"
	printf '%s' "primary" >"$primary_stage_file"
	printf '%s' "forwarded" >"$forwarded_stage_file"
	printf '%s' "old-primary" >"$primary_rollback_file"
	printf '%s' "old-forwarded" >"$forwarded_rollback_file"

	set +e
	output=$(
		(
			g_test_commit_calls=0
			g_test_finalize_calls=0
			zxfer_prepare_local_backup_file_stage() {
				if [ "$1" = "$primary_file" ]; then
					g_zxfer_backup_stage_dir_result=$primary_stage_dir
					g_zxfer_backup_stage_file_result=$primary_stage_file
				else
					g_zxfer_backup_stage_dir_result=$forwarded_stage_dir
					g_zxfer_backup_stage_file_result=$forwarded_stage_file
				fi
				return 0
			}
			zxfer_commit_local_backup_file_stage() {
				g_test_commit_calls=$((g_test_commit_calls + 1))
				g_zxfer_backup_commit_had_existing_target_result=1
				if [ "$g_test_commit_calls" -eq 1 ]; then
					g_zxfer_backup_commit_rollback_file_result=$forwarded_rollback_file
				else
					g_zxfer_backup_commit_rollback_file_result=$primary_rollback_file
				fi
				return 0
			}
			zxfer_finalize_local_backup_file_commit() {
				g_test_finalize_calls=$((g_test_finalize_calls + 1))
				if [ "$g_test_finalize_calls" -eq 1 ]; then
					return 0
				fi
				return 43
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;primary" "$forwarded_file" "#header;forwarded" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'finalize_calls=%s\n' "$g_test_finalize_calls"
		)
	)
	set -e
	if [ -e "$primary_stage_dir" ]; then
		primary_stage_exists=1
	else
		primary_stage_exists=0
	fi
	if [ -e "$forwarded_stage_dir" ]; then
		forwarded_stage_exists=1
	else
		forwarded_stage_exists=0
	fi

	assertContains "Transactional local pair writes should preserve the primary finalization failure status after a forwarded finalization succeeds." \
		"$output" "status=43"
	assertContains "Transactional local pair writes should attempt both finalization steps before preserving the primary finalization failure." \
		"$output" "finalize_calls=2"
	assertEquals "Primary finalization failures should clean up the primary stage directory." \
		0 "$primary_stage_exists"
	assertEquals "Primary finalization failures should clean up the forwarded stage directory too." \
		0 "$forwarded_stage_exists"
}

test_write_local_backup_file_pair_atomically_cleans_up_primary_stage_when_forwarded_stage_creation_fails() {
	primary_file="$TEST_TMPDIR/local_pair_forwarded_stage_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_forwarded_stage_fail_alias.meta"
	primary_stage_dir="$TEST_TMPDIR/local_pair_forwarded_stage_fail.primary.stage"
	primary_stage_file="$primary_stage_dir/backup.write"
	mkdir -p "$primary_stage_dir"
	printf '%s' "primary" >"$primary_stage_file"

	set +e
	status=$(
		(
			zxfer_prepare_local_backup_file_stage() {
				if [ "$1" = "$primary_file" ]; then
					g_zxfer_backup_stage_dir_result=$primary_stage_dir
					g_zxfer_backup_stage_file_result=$primary_stage_file
					return 0
				fi
				return 47
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;primary" "$forwarded_file" "#header;forwarded" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	if [ -e "$primary_stage_dir" ]; then
		stage_dir_exists=1
	else
		stage_dir_exists=0
	fi

	assertEquals "Transactional local pair writes should preserve forwarded-stage preparation failures." \
		47 "$status"
	assertEquals "Forwarded-stage preparation failures should clean up the already prepared primary stage directory." \
		0 "$stage_dir_exists"
}

test_write_local_backup_file_pair_atomically_reports_failed_forwarded_rollback_after_primary_commit_failure() {
	primary_file="$TEST_TMPDIR/local_pair_rollback_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_rollback_fail_alias.meta"
	printf '%s' "old-primary" >"$primary_file"
	printf '%s' "old-forwarded" >"$forwarded_file"
	chmod 600 "$primary_file" "$forwarded_file"

	set +e
	status=$(
		(
			mv() {
				if [ "$1" = "-f" ] && [ "$3" = "$primary_file" ] && [ "${2##*/}" = "backup.write" ]; then
					return 1
				fi
				if [ "$1" = "-f" ] && [ "$3" = "$forwarded_file" ] && [ "${2##*/}" != "backup.write" ]; then
					return 1
				fi
				command mv "$@"
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;new-primary" "$forwarded_file" "#header;new-forwarded" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	leftover_rollback=$(find "$TEST_TMPDIR" -maxdepth 1 -type f -name '.zxfer-backup-rollback.*' | wc -l | tr -d '[:space:]')
	if [ -f "$primary_file" ]; then
		primary_contents=$(cat "$primary_file")
	else
		primary_contents=""
	fi
	if [ -e "$forwarded_file" ]; then
		forwarded_exists=1
	else
		forwarded_exists=0
	fi

	assertEquals "Transactional local pair writes should return the dedicated rollback-failure status when restoring the forwarded alias fails." \
		2 "$status"
	assertEquals "Transactional local pair writes should still restore the original primary metadata before reporting a forwarded-rollback failure." \
		"old-primary" "$primary_contents"
	assertEquals "Transactional local pair writes should remove the failed forwarded target so the preserved rollback file remains authoritative for recovery." \
		0 "$forwarded_exists"
	assertEquals "Transactional local pair writes should preserve the rollback file when forwarded alias restoration fails." \
		1 "$leftover_rollback"
}

test_write_local_backup_file_atomically_cleans_up_stage_dir_when_commit_fails() {
	backup_file="$TEST_TMPDIR/local_atomic_commit_failure.meta"
	stage_dir="$TEST_TMPDIR/local_atomic_commit_failure.stage"
	stage_file="$stage_dir/backup.write"
	mkdir -p "$stage_dir"
	printf '%s' "payload" >"$stage_file"

	set +e
	status=$(
		(
			zxfer_prepare_local_backup_file_stage() {
				g_zxfer_backup_stage_dir_result=$stage_dir
				g_zxfer_backup_stage_file_result=$stage_file
				return 0
			}
			zxfer_commit_local_backup_file_stage() {
				return 53
			}
			zxfer_write_local_backup_file_atomically "$backup_file" "#header;payload" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	if [ -e "$stage_dir" ]; then
		stage_dir_exists=1
	else
		stage_dir_exists=0
	fi

	assertEquals "Single-file local backup writes should preserve staged-file commit failures." \
		53 "$status"
	assertEquals "Single-file local backup writes should clean up the staged directory when commit fails." \
		0 "$stage_dir_exists"
}

test_write_backup_properties_reports_local_write_failure() {
	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "$TEST_TMPDIR/missing/secure/path"
			}
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_properties 2>/dev/null
		)
	)
	status=$?

	assertEquals "Local backup writes should abort when the secure file cannot be created." 1 "$status"
	assertContains "Local backup write failures caused by same-directory staging should surface the specific local staging error." \
		"$output" "Failed to stage local backup file "
	assertContains "Local backup write failures caused by same-directory staging should preserve the atomic-write staging context." \
		"$output" "for atomic write."
}

test_read_local_backup_file_returns_missing_when_snapshot_link_loses_target() {
	backup_file="$TEST_TMPDIR/read_local_snapshot_race.meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	status=$(
		(
			ln() {
				rm -f "$backup_file"
				return 1
			}
			zxfer_read_local_backup_file "$backup_file" >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Local backup reads should map a vanished target during snapshot-link staging to the missing-file sentinel." \
		4 "$status"
}

test_read_local_backup_file_preserves_snapshot_link_failure_when_target_survives() {
	backup_dir="$TEST_TMPDIR/read_local_snapshot_link_survives"
	backup_file="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	output=$(
		(
			ln() {
				return 73
			}
			zxfer_read_local_backup_file "$backup_file" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'staging=<%s>\n' "${g_zxfer_backup_local_read_failure_result:-}"
		)
	)
	set -e
	leftovers=$(find "$backup_dir" -maxdepth 1 -type d -name '.zxfer-backup-read.*' | wc -l | tr -d '[:space:]')

	assertContains "Local backup reads should preserve snapshot-link failures when the target file still exists." \
		"$output" "status=73"
	assertContains "Snapshot-link failures against a still-present local target should not be misclassified as local staging read failures." \
		"$output" "staging=<>"
	assertEquals "Snapshot-link failures against a still-present local target should clean up the staged backup-read directory." \
		0 "$leftovers"
}

test_read_local_backup_file_reads_existing_file_when_parent_is_not_writable() {
	backup_dir="$TEST_TMPDIR/read_local_nonwritable_parent"
	backup_file="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"
	chmod 500 "$backup_dir"

	set +e
	output=$(zxfer_read_local_backup_file "$backup_file" 2>&1)
	status=$?
	set -e
	chmod 700 "$backup_dir"

	assertEquals "Local backup reads should still succeed for existing secure files in trusted non-writable parent directories." \
		0 "$status"
	assertEquals "Local backup reads in trusted non-writable parent directories should return the file contents unchanged." \
		"payload" "$output"
}

test_read_local_backup_file_rejects_insecure_file_when_parent_is_not_writable() {
	backup_dir="$TEST_TMPDIR/read_local_nonwritable_insecure_parent"
	backup_file="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	printf '%s\n' "payload" >"$backup_file"
	chmod 644 "$backup_file"
	chmod 500 "$backup_dir"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_read_local_backup_file "$backup_file"
		) 2>&1
	)
	status=$?
	set -e
	chmod 700 "$backup_dir"

	assertEquals "Local backup reads should still reject insecure files in trusted non-writable parent directories." \
		1 "$status"
	assertContains "Direct local backup reads from trusted non-writable parent directories should preserve the secure-file validation error." \
		"$output" "its permissions (644) are not 0600"
}

test_read_local_backup_file_returns_failure_when_direct_read_cat_fails_in_nonwritable_parent() {
	backup_dir="$TEST_TMPDIR/read_local_nonwritable_cat_failure_parent"
	backup_file="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"
	chmod 500 "$backup_dir"

	set +e
	status=$(
		(
			cat() {
				return 1
			}
			zxfer_read_local_backup_file "$backup_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	chmod 700 "$backup_dir"

	assertEquals "Direct local backup reads from trusted non-writable parent directories should preserve direct staged-read failures." \
		1 "$status"
}

test_read_local_backup_file_preserves_stage_dir_creation_failure_status() {
	backup_file="$TEST_TMPDIR/read_local_stage_dir_failure.meta"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"

	set +e
	status=$(
		(
			zxfer_create_backup_metadata_stage_dir_for_path() {
				return 71
			}
			zxfer_read_local_backup_file "$backup_file" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e

	assertEquals "Local backup reads should preserve same-directory stage allocation failures." \
		71 "$status"
}

test_write_backup_properties_reports_remote_write_failure() {
	g_option_n_dryrun=0
	g_option_T_target_host="target.example"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_get_backup_storage_dir_for_dataset_tree() {
				printf '%s\n' "/var/db/zxfer/tank/src"
			}
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_properties
		)
	)
	status=$?

	assertEquals "Remote backup writes should abort when the remote write command fails." 1 "$status"
	assertContains "Remote backup write failures should mention the mounted-filesystem guidance." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

test_write_backup_metadata_contents_to_store_writes_local_file_atomically() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_store_root"
	backup_dir="$g_backup_storage_root/tank/src"
	backup_file="$backup_dir/.zxfer_backup_info.src"

	zxfer_write_backup_metadata_contents_to_store "$backup_dir" "$backup_file" "#header;payload"
	if [ -f "$backup_file" ]; then
		backup_exists=1
	else
		backup_exists=0
	fi

	assertEquals "Single-file local backup writes should create the target backup metadata file." \
		1 "$backup_exists"
	assertEquals "Single-file local backup writes should translate serialized semicolon separators into on-disk newlines." \
		"#header
payload" "$(cat "$backup_file")"
}

test_write_backup_metadata_contents_to_store_reports_local_atomic_write_failure() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_store_failure_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_write_local_backup_file_atomically() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "$g_backup_storage_root/tank/src" "$g_backup_storage_root/tank/src/.zxfer_backup_info.src" "#header;payload"
		)
	)
	status=$?

	assertEquals "Single-file local backup writes should abort when the atomic writer fails." \
		1 "$status"
	assertContains "Single-file local backup write failures should surface the mounted-filesystem guidance." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

test_write_backup_metadata_contents_to_store_reports_local_staging_failure_distinctly() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_store_stage_failure_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_write_local_backup_file_atomically() {
				g_zxfer_backup_local_write_failure_result=staging
				return 71
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "$g_backup_storage_root/tank/src" "$g_backup_storage_root/tank/src/.zxfer_backup_info.src" "#header;payload"
		)
	)
	status=$?

	assertEquals "Single-file local backup staging failures should abort when the atomic writer cannot stage the file." \
		1 "$status"
	assertContains "Single-file local backup staging failures should surface the local staging error instead of the generic mounted-filesystem guidance." \
		"$output" "Failed to stage local backup file $g_backup_storage_root/tank/src/.zxfer_backup_info.src for atomic write."
}

test_write_backup_metadata_pair_contents_to_store_reports_local_rollback_failure() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_pair_store_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_write_local_backup_file_pair_atomically() {
				return 2
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		)
	)
	status=$?

	assertEquals "Transactional pair writes should abort when restoring the forwarded alias fails locally." 1 "$status"
	assertContains "Transactional pair-write rollback failures should surface the dedicated recovery guidance locally." \
		"$output" "restoring forwarded provenance alias"
}

test_write_backup_metadata_pair_contents_to_store_reports_local_staging_failure_distinctly() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_pair_stage_store_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_write_local_backup_file_pair_atomically() {
				g_zxfer_backup_local_write_failure_result=staging
				return 71
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		)
	)
	status=$?

	assertEquals "Transactional pair writes should abort when local same-directory staging fails." 1 "$status"
	assertContains "Transactional pair-write local staging failures should surface the local staging error instead of the generic mounted-filesystem guidance." \
		"$output" "Failed to stage local backup file pair for atomic write."
}

test_write_backup_metadata_pair_contents_to_store_reports_generic_local_write_failure() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_pair_generic_store_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_require_backup_write_target_path() {
				:
			}
			zxfer_write_local_backup_file_pair_atomically() {
				return 73
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		)
	)
	status=$?

	assertEquals "Transactional pair writes should abort when the local atomic writer fails generically." 1 "$status"
	assertContains "Transactional pair-write local generic failures should surface the mounted-filesystem guidance when no staging or rollback classification applies." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

test_read_remote_backup_file_returns_failure_when_remote_read_fails() {
	set +e
	status=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 12
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/read-error.meta" >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Remote backup reads should map unexpected remote read failures to the generic read-failure sentinel." \
		5 "$status"
}

test_read_remote_backup_file_preserves_transport_failure_stderr() {
	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/read-transport-error.meta"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertContains "Remote backup reads should preserve ssh transport stderr when the remote probe fails before the helper script can report a synthetic status." \
		"$output" "Host key verification failed."
	assertContains "Remote backup reads should return the dedicated transport-failure sentinel for ssh/bootstrap errors." \
		"$output" "status=6"
}

test_read_remote_backup_file_maps_capture_reload_failures_to_capture_status() {
	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_capture_remote_probe_output() {
				g_zxfer_remote_probe_capture_failed=1
				g_zxfer_remote_probe_stderr="Failed to read remote backup helper stderr capture from local staging."
				return 12
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/read-capture-error.meta" >/dev/null
			printf 'status=%s\n' "$?"
		) 2>&1
	)
	set -e

	assertContains "Remote backup reads should map local remote-probe capture reload failures to the dedicated capture-failure sentinel." \
		"$output" "status=7"
}

test_read_remote_backup_file_cleans_up_stage_dir_when_uid_probe_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	backup_dir="$physical_tmpdir/read_remote_uid_probe"
	backup_file="$backup_dir/backup.meta"
	fake_bin="$physical_tmpdir/read_remote_uid_probe_bin"
	mkdir -p "$backup_dir" "$fake_bin"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"
	cat >"$fake_bin/id" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin/id"

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
		) 2>&1
	)
	status=$?
	leftovers=$(find "$backup_dir" -maxdepth 1 -type d -name '.zxfer-backup-read.*' | wc -l | tr -d '[:space:]')

	assertEquals "Remote backup reads should still fail closed when the remote UID probe fails." 1 "$status"
	assertContains "Remote UID-probe failures should preserve the ownership/permission error." \
		"$output" "Cannot determine ownership or permissions for backup metadata $backup_file on backup@example.com."
	assertEquals "Remote UID-probe failures should clean up staged backup-read directories." \
		0 "$leftovers"
}

test_read_remote_backup_file_marks_missing_secure_path_helpers_as_dependency_errors() {
	empty_dir="$TEST_TMPDIR/read_remote_missing_helper_bin"
	mkdir -p "$empty_dir"

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$empty_dir"
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup reads should fail closed when required secure-PATH helpers are missing." \
		1 "$status"
	assertContains "Missing remote backup-read helpers should surface the exact dependency name from the remote precheck." \
		"$output" "Required dependency \"id\" not found on host backup@example.com in secure PATH ($empty_dir)."
	assertContains "Missing remote backup-read helpers should be classified as dependency failures locally." \
		"$output" "class=dependency"
	assertContains "Missing remote backup-read helpers should use the dependency-specific local error." \
		"$output" "Required remote backup-metadata helper dependency not found on host backup@example.com in secure PATH ($empty_dir)."
}

test_read_remote_backup_file_reads_existing_file_when_parent_is_not_writable() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	backup_dir="$physical_tmpdir/read_remote_nonwritable_parent"
	backup_file="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	printf '%s\n' "payload" >"$backup_file"
	chmod 600 "$backup_file"
	chmod 500 "$backup_dir"

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
		) 2>&1
	)
	status=$?
	set -e
	chmod 700 "$backup_dir"

	assertEquals "Remote backup reads should still succeed for existing secure files in trusted non-writable parent directories." \
		0 "$status"
	assertEquals "Remote backup reads in trusted non-writable parent directories should return the file contents unchanged." \
		"payload" "$output"
}

test_write_backup_properties_uses_resolved_remote_cat_helper_for_live_writes() {
	g_option_n_dryrun=0
	g_option_T_target_host="target.example"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	g_initial_source="tank/src"
	log_file="$TEST_TMPDIR/remote_backup_write_helper.log"
	expected_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	expected_forwarded_root=$(zxfer_get_expected_backup_destination_for_source "$g_initial_source")
	expected_forwarded_name=$(zxfer_get_forwarded_backup_metadata_filename "$expected_forwarded_root")

	zxfer_get_backup_storage_dir_for_dataset_tree() {
		printf '/var/db/zxfer/%s\n' "$1"
	}

	zxfer_ensure_remote_backup_dir() {
		:
	}

	zxfer_resolve_remote_cli_command_safe() {
		printf '%s\n' "'/remote/bin/cat'"
	}

	zxfer_invoke_ssh_shell_command_for_host() {
		printf '%s\n' "$2" >>"$log_file"
		cat >/dev/null
		return 0
	}

	zxfer_write_backup_properties

	assertEquals "Transactional live remote backup writes should use one remote write invocation for the primary file and forwarded alias together." \
		1 "$(wc -l <"$log_file" | tr -d '[:space:]')"
	assertContains "Live remote backup writes should scope the remote staging helper to the secure dependency path." \
		"$(cat "$log_file")" "PATH='"
	assertContains "Live remote backup writes should stage the payload in a sibling temp directory before renaming it into place." \
		"$(cat "$log_file")" ".zxfer-backup-write"
	assertContains "Transactional live remote backup writes should stage rollback files so a later primary-write failure can restore the forwarded alias." \
		"$(cat "$log_file")" ".zxfer-backup-rollback"
	assertContains "Live remote backup writes should target the source dataset tree under ZXFER_BACKUP_DIR." \
		"$(cat "$log_file")" "/var/db/zxfer/tank/src/$expected_name"
	assertContains "Live remote backup writes should also write the forwarded provenance alias under the actual destination tree for chained backups." \
		"$(cat "$log_file")" "/var/db/zxfer/$expected_forwarded_root/$expected_forwarded_name"
}

test_write_backup_properties_marks_missing_remote_stage_helpers_as_dependency_errors() {
	empty_dir="$TEST_TMPDIR/write_remote_missing_helper_bin"
	mkdir -p "$empty_dir"
	g_option_n_dryrun=0
	g_option_T_target_host="target.example"
	g_destination="backup/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_storage_root=""
	g_zxfer_version="test-version"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4"
	g_initial_source="tank/src"

	zxfer_get_backup_storage_dir_for_dataset_tree() {
		printf '%s\n' "/var/db/zxfer/tank/src"
	}

	zxfer_ensure_remote_backup_dir() {
		:
	}

	zxfer_resolve_remote_cli_command_safe() {
		printf '%s\n' "'/bin/cat'"
	}

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$empty_dir"
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_properties
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup writes should fail closed when required secure-PATH helpers are missing." \
		1 "$status"
	assertContains "Missing remote backup-write helpers should surface the exact dependency name from the remote precheck." \
		"$output" "Required dependency \"mktemp\" not found on host target.example in secure PATH ($empty_dir)."
	assertContains "Missing remote backup-write helpers should be classified as dependency failures locally." \
		"$output" "class=dependency"
	assertContains "Missing remote backup-write helpers should use the dependency-specific local error." \
		"$output" "Required remote backup-write helper dependency not found on host target.example in secure PATH ($empty_dir)."
}

test_ensure_remote_backup_dir_preserves_transport_failure_stderr() {
	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "/var/db/zxfer/tank/src" "target.example" destination
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup-directory preparation should fail closed when ssh transport setup fails." \
		1 "$status"
	assertContains "Remote backup-directory transport failures should preserve the ssh diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Remote backup-directory transport failures should identify the remote host and directory context." \
		"$output" "Failed to contact target host target.example while preparing backup directory /var/db/zxfer/tank/src."
}

test_ensure_remote_backup_dir_reports_capture_failures() {
	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_capture_remote_probe_output() {
				g_zxfer_remote_probe_capture_failed=1
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "/var/db/zxfer/tank/src" "target.example" destination
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Remote backup-directory preparation should fail closed when stderr capture fails before a remote status is available." \
		1 "$status"
	assertContains "Remote backup-directory capture failures should identify the remote host and directory context." \
		"$output" "Failed to reload local remote helper capture while preparing backup directory /var/db/zxfer/tank/src on host target.example."
}

test_run_remote_backup_helper_with_payload_rethrows_temp_creation_failure() {
	set +e
	output=$(
		(
			zxfer_create_private_temp_dir() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "should-not-run"
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_run_remote_backup_helper_with_payload "target.example" "printf '%s\\n' ok" "payload" destination
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup helper payload staging should fail closed when local temp staging cannot be created." \
		1 "$status"
	assertContains "Remote backup helper payload staging should preserve the local temporary-file error instead of collapsing it into a later remote write failure." \
		"$output" "Error creating temporary file."
	assertNotContains "Remote backup helper payload staging should not attempt the remote ssh helper when local temp staging fails." \
		"$output" "should-not-run"
}

test_run_remote_backup_helper_with_payload_rethrows_transport_setup_failures_without_leaking_stage_dir() {
	l_stage_dir="$SHUNIT_TMPDIR/remote-backup-helper-stage"
	rm -rf "$l_stage_dir"

	set +e
	output=$(
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_create_private_temp_dir() {
				mkdir -p "$l_stage_dir" || return 1
				g_zxfer_runtime_artifact_path_result=$l_stage_dir
				printf '%s\n' "$l_stage_dir"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_run_remote_backup_helper_with_payload "target.example" "printf '%s\\n' ok" "payload" destination
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup helper payload staging should fail closed when ssh transport setup fails before the remote helper runs." \
		1 "$status"
	assertContains "Remote backup helper payload staging should preserve the transport setup error instead of collapsing it into a later write failure." \
		"$output" "Managed ssh policy invalid."
	assertFalse "Remote backup helper payload staging should not leak its staged temp directory when ssh transport setup fails before invocation." \
		"[ -e \"$l_stage_dir\" ]"
}

test_run_remote_backup_helper_with_payload_cleans_up_stage_dir_when_stdin_stage_write_fails() {
	l_stage_dir="$SHUNIT_TMPDIR/remote-backup-helper-stdin-stage"
	rm -rf "$l_stage_dir"

	set +e
	output=$(
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "ssh"
			}
			zxfer_create_private_temp_dir() {
				mkdir -p "$l_stage_dir" || return 1
				g_zxfer_runtime_artifact_path_result=$l_stage_dir
				printf '%s\n' "$l_stage_dir"
			}
			zxfer_write_runtime_artifact_file() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "should-not-run"
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_run_remote_backup_helper_with_payload "target.example" "printf '%s\\n' ok" "payload" destination
		) 2>&1
	)
	status=$?
	if [ -e "$l_stage_dir" ]; then
		stage_dir_exists=1
	else
		stage_dir_exists=0
	fi
	set -e

	assertEquals "Remote backup helper payload staging should fail closed when the staged stdin payload cannot be written locally." \
		1 "$status"
	assertContains "Remote backup helper stdin stage-write failures should preserve the temporary-file staging error." \
		"$output" "Error creating temporary file."
	assertNotContains "Remote backup helper stdin stage-write failures should stop before the ssh helper is invoked." \
		"$output" "should-not-run"
	assertEquals "Remote backup helper stdin stage-write failures should clean up the staged local helper directory." \
		0 "$stage_dir_exists"
}

test_run_remote_backup_helper_with_payload_counts_transport_setup_failures_in_ssh_profile() {
	set +e
	output=$(
		(
			g_zxfer_profile_ssh_shell_invocations=0
			g_zxfer_profile_source_ssh_shell_invocations=0
			g_zxfer_profile_destination_ssh_shell_invocations=0
			g_zxfer_profile_other_ssh_shell_invocations=0
			zxfer_profile_metrics_enabled() {
				return 0
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
				printf 'destination=%s\n' "${g_zxfer_profile_destination_ssh_shell_invocations:-0}"
				printf 'source=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
				printf 'other=%s\n' "${g_zxfer_profile_other_ssh_shell_invocations:-0}"
				exit 1
			}
			zxfer_run_remote_backup_helper_with_payload "target.example" "printf '%s\\n' ok" "payload" destination
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup helper payload staging should still fail closed when ssh transport setup fails." \
		1 "$status"
	assertContains "Remote backup helper transport preflight failures should still count as one destination ssh invocation in the profile summary." \
		"$output" "ssh=1"
	assertContains "Remote backup helper transport preflight failures should be attributed to the destination side when invoked for remote backup writes." \
		"$output" "destination=1"
	assertContains "Remote backup helper transport preflight failures should not increment unrelated source counters." \
		"$output" "source=0"
	assertContains "Remote backup helper transport preflight failures should not increment unrelated other-host counters." \
		"$output" "other=0"
}

test_run_remote_backup_helper_with_payload_reports_stderr_capture_failures() {
	l_stage_dir="$SHUNIT_TMPDIR/remote-backup-helper-capture"
	rm -rf "$l_stage_dir"

	set +e
	output=$(
		(
			zxfer_create_private_temp_dir() {
				mkdir -p "$l_stage_dir" || return 1
				g_zxfer_runtime_artifact_path_result=$l_stage_dir
				printf '%s\n' "$l_stage_dir"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				cat >/dev/null
				printf '%s\n' "helper-stdout"
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			cat() {
				if [ "$1" = "$l_stage_dir/stderr" ]; then
					printf '%s\n' "capture read failed" >&2
					return 9
				fi
				command cat "$@"
			}

			if zxfer_run_remote_backup_helper_with_payload "target.example" "printf '%s\\n' ok" "payload" destination; then
				l_status=0
			else
				l_status=$?
			fi

			printf 'status=%s\n' "$l_status"
			printf 'capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'stdout=<%s>\n' "$g_zxfer_remote_probe_stdout"
			printf 'stderr=<%s>\n' "$g_zxfer_remote_probe_stderr"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup-helper capture-failure tests should complete the subshell cleanly." \
		0 "$status"
	assertContains "Remote backup helpers should fail closed when the staged stderr payload cannot be reloaded." \
		"$output" "status=9"
	assertContains "Remote backup helpers should classify staged capture readback failures distinctly." \
		"$output" "capture_failed=1"
	assertContains "Remote backup helpers should preserve the underlying staged-read diagnostic." \
		"$output" "capture read failed"
	assertContains "Remote backup helpers should surface a specific staged stderr readback message." \
		"$output" "stderr=<Failed to read remote backup helper stderr capture from local staging.>"
	assertContains "Remote backup helpers should discard partial stdout payloads once capture reload fails." \
		"$output" "stdout=<>"
	assertFalse "Remote backup helpers should clean up the staged local directory after capture readback failures." \
		"[ -e \"$l_stage_dir\" ]"
}

test_write_backup_metadata_contents_to_store_runs_remote_helper_with_newline_payload_on_success() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"
	dir_log="$TEST_TMPDIR/remote_single_success_dirs.log"
	cmd_file="$TEST_TMPDIR/remote_single_success_cmd.txt"
	helper_cmd_file="$TEST_TMPDIR/remote_single_success_helper_cmd.txt"
	helper_side_file="$TEST_TMPDIR/remote_single_success_helper_side.txt"
	payload_file="$TEST_TMPDIR/remote_single_success_payload.txt"
	capture_file="$TEST_TMPDIR/remote_single_success_capture.txt"
	: >"$dir_log"

	(
		DIR_LOG="$dir_log"
		CMD_FILE="$cmd_file"
		HELPER_CMD_FILE="$helper_cmd_file"
		HELPER_SIDE_FILE="$helper_side_file"
		PAYLOAD_FILE="$payload_file"
		CAPTURE_FILE="$capture_file"
		zxfer_ensure_remote_backup_dir() {
			printf '%s\n' "$1" >>"$DIR_LOG"
		}
		zxfer_resolve_remote_cli_command_safe() {
			printf '%s\n' "'/remote/bin/cat'"
		}
		zxfer_get_remote_backup_helper_dependency_path() {
			printf '%s\n' "/secure/path"
		}
		zxfer_build_remote_backup_write_cmd() {
			printf '%s\n' "remote-write-cmd"
		}
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1" >"$CMD_FILE"
			printf '%s\n' "sh -c $1"
		}
		zxfer_run_remote_backup_helper_with_payload() {
			printf '%s\n' "$2" >"$HELPER_CMD_FILE"
			printf '%s\n' "$4" >"$HELPER_SIDE_FILE"
			printf '%s' "$3" >"$PAYLOAD_FILE"
			return 0
		}
		zxfer_throw_error() {
			printf '%s\n' "$1" >"$TEST_TMPDIR/remote_single_success_unexpected_throw.txt"
			exit 1
		}
		zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		printf '%s\n' "${g_zxfer_remote_probe_capture_failed:-0}" >"$CAPTURE_FILE"
	)
	status=$?

	assertEquals "Single-file remote backup writes should succeed when the staged remote helper completes successfully." \
		0 "$status"
	assertEquals "Single-file remote backup writes should ensure the secure root and target directory before launching the helper." \
		"/var/db/zxfer
/var/db/zxfer/tank/src" "$(cat "$dir_log")"
	assertEquals "Single-file remote backup writes should wrap the generated remote write command through the remote sh -c renderer." \
		"remote-write-cmd" "$(cat "$cmd_file")"
	assertEquals "Single-file remote backup writes should pass the rendered remote shell command to the helper runner." \
		"sh -c remote-write-cmd" "$(cat "$helper_cmd_file")"
	assertEquals "Single-file remote backup writes should classify the helper invocation as destination-side work." \
		"destination" "$(cat "$helper_side_file")"
	assertEquals "Single-file remote backup writes should rewrite semicolon-delimited metadata into newline-delimited helper payloads." \
		"#header
payload" "$(cat "$payload_file")"
	assertEquals "Single-file remote backup writes should leave capture-failure scratch cleared on helper success." \
		"0" "$(cat "$capture_file")"
}

test_write_backup_metadata_contents_to_store_marks_remote_cat_lookup_failures_as_dependency_errors() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "remote cat lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		)
	)
	status=$?

	assertEquals "Single-file remote backup writes should abort when the secure remote cat helper cannot be resolved." 1 "$status"
	assertContains "Single-file remote backup write helper lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Single-file remote backup write helper lookup failures should preserve the lookup message." \
		"$output" "msg=remote cat lookup failed"
}

test_write_backup_metadata_contents_to_store_marks_remote_write_dependency_status_as_dependency_error() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				cat >/dev/null
				return 99
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "${g_zxfer_failure_class:-}" "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		)
	)
	status=$?

	assertEquals "Single-file remote backup writes should abort when the remote stage helper reports a dependency failure status." \
		1 "$status"
	assertContains "Single-file remote backup write dependency failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Single-file remote backup write dependency failures should surface the dependency-specific local error." \
		"$output" "Required remote backup-write helper dependency not found on host target.example in secure PATH"
}

test_write_backup_metadata_contents_to_store_emits_probe_stderr_for_remote_failure_statuses() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	dependency_output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_run_remote_backup_helper_with_payload() {
				g_zxfer_remote_probe_stderr="missing dependency"
				return 99
			}
			zxfer_emit_remote_probe_failure_message() {
				printf '%s\n' "probe-stderr"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		) 2>&1
	)
	dependency_status=$?
	write_failure_output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_run_remote_backup_helper_with_payload() {
				g_zxfer_remote_probe_stderr="write failed"
				return 92
			}
			zxfer_emit_remote_probe_failure_message() {
				printf '%s\n' "probe-stderr"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		) 2>&1
	)
	write_failure_status=$?
	set -e

	assertEquals "Single-file remote backup writes should fail closed when the helper reports a dependency status and probe stderr is available." \
		1 "$dependency_status"
	assertContains "Single-file remote backup write dependency failures should emit the staged probe stderr before throwing." \
		"$dependency_output" "probe-stderr"
	assertContains "Single-file remote backup write dependency failures should still surface the dependency guidance after probe stderr." \
		"$dependency_output" "Required remote backup-write helper dependency not found on host target.example in secure PATH"
	assertEquals "Single-file remote backup writes should fail closed when the helper reports a write failure status and probe stderr is available." \
		1 "$write_failure_status"
	assertContains "Single-file remote backup write failures should emit the staged probe stderr before throwing." \
		"$write_failure_output" "probe-stderr"
	assertContains "Single-file remote backup write failures should still surface the mounted-filesystem guidance after probe stderr." \
		"$write_failure_output" "Error writing backup file. Is filesystem mounted?"
}

test_write_backup_metadata_contents_to_store_reports_remote_write_failure() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				cat >/dev/null
				return 7
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		)
	)
	status=$?

	assertEquals "Single-file remote backup writes should abort when the remote write command fails generically." \
		1 "$status"
	assertContains "Single-file remote backup write failures should surface the mounted-filesystem guidance." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

test_write_backup_metadata_contents_to_store_preserves_transport_failure_stderr() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				cat >/dev/null
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		) 2>&1
	)
	status=$?

	assertEquals "Single-file remote backup writes should fail closed when ssh transport setup fails." \
		1 "$status"
	assertContains "Single-file remote backup write transport failures should preserve the ssh diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Single-file remote backup write transport failures should identify the remote host and target path." \
		"$output" "Failed to contact target host target.example while writing backup metadata /var/db/zxfer/tank/src/.zxfer_backup_info.src."
}

test_write_backup_metadata_contents_to_store_reports_capture_failures_distinctly() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "'/remote/bin/cat'"
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_run_remote_backup_helper_with_payload() {
				g_zxfer_remote_probe_capture_failed=1
				g_zxfer_remote_probe_stderr="Failed to read remote backup helper stderr capture from local staging."
				return 9
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload"
		) 2>&1
	)
	status=$?

	assertEquals "Single-file remote backup writes should fail closed when local helper capture reload fails." \
		1 "$status"
	assertContains "Single-file remote backup writes should preserve the staged capture diagnostic." \
		"$output" "Failed to read remote backup helper stderr capture from local staging."
	assertContains "Single-file remote backup writes should report the local capture failure distinctly from transport errors." \
		"$output" "Failed to reload local remote helper capture while writing backup metadata /var/db/zxfer/tank/src/.zxfer_backup_info.src on host target.example."
	assertNotContains "Single-file remote backup writes should not misreport local capture failures as host-contact failures." \
		"$output" "Failed to contact target host target.example"
}

test_write_backup_metadata_pair_contents_to_store_runs_remote_helper_with_split_payload_on_success() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"
	dir_log="$TEST_TMPDIR/remote_pair_success_dirs.log"
	cmd_file="$TEST_TMPDIR/remote_pair_success_cmd.txt"
	helper_cmd_file="$TEST_TMPDIR/remote_pair_success_helper_cmd.txt"
	helper_side_file="$TEST_TMPDIR/remote_pair_success_helper_side.txt"
	payload_file="$TEST_TMPDIR/remote_pair_success_payload.txt"
	capture_file="$TEST_TMPDIR/remote_pair_success_capture.txt"
	pair_split_line=$(zxfer_get_backup_metadata_pair_split_line)
	: >"$dir_log"

	(
		DIR_LOG="$dir_log"
		CMD_FILE="$cmd_file"
		HELPER_CMD_FILE="$helper_cmd_file"
		HELPER_SIDE_FILE="$helper_side_file"
		PAYLOAD_FILE="$payload_file"
		CAPTURE_FILE="$capture_file"
		zxfer_ensure_remote_backup_dir() {
			printf '%s\n' "$1" >>"$DIR_LOG"
		}
		zxfer_build_remote_backup_pair_write_cmd() {
			printf '%s\n' "remote-pair-write-cmd"
		}
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1" >"$CMD_FILE"
			printf '%s\n' "sh -c $1"
		}
		zxfer_run_remote_backup_helper_with_payload() {
			printf '%s\n' "$2" >"$HELPER_CMD_FILE"
			printf '%s\n' "$4" >"$HELPER_SIDE_FILE"
			printf '%s' "$3" >"$PAYLOAD_FILE"
			return 0
		}
		zxfer_throw_error() {
			printf '%s\n' "$1" >"$TEST_TMPDIR/remote_pair_success_unexpected_throw.txt"
			exit 1
		}
		zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		printf '%s\n' "${g_zxfer_remote_probe_capture_failed:-0}" >"$CAPTURE_FILE"
	)
	status=$?

	assertEquals "Transactional remote pair writes should succeed when the staged remote helper completes successfully." \
		0 "$status"
	assertEquals "Transactional remote pair writes should ensure the secure root plus both target directories before launching the helper." \
		"/var/db/zxfer
/var/db/zxfer/tank/src
/var/db/zxfer/backup/dst/src" "$(cat "$dir_log")"
	assertEquals "Transactional remote pair writes should wrap the generated pair-write command through the remote sh -c renderer." \
		"remote-pair-write-cmd" "$(cat "$cmd_file")"
	assertEquals "Transactional remote pair writes should pass the rendered pair-write shell command to the helper runner." \
		"sh -c remote-pair-write-cmd" "$(cat "$helper_cmd_file")"
	assertEquals "Transactional remote pair writes should classify the helper invocation as destination-side work." \
		"destination" "$(cat "$helper_side_file")"
	assertEquals "Transactional remote pair writes should splice the primary and forwarded metadata payloads with the split marker before newline normalization." \
		"#header
payload
$pair_split_line
#header
forwarded" "$(cat "$payload_file")"
	assertEquals "Transactional remote pair writes should leave capture-failure scratch cleared on helper success." \
		"0" "$(cat "$capture_file")"
}

test_write_backup_metadata_pair_contents_to_store_reports_remote_rollback_failure() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 98
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		)
	)
	status=$?

	assertEquals "Transactional pair writes should abort when restoring the forwarded alias fails remotely." 1 "$status"
	assertContains "Transactional pair-write rollback failures should surface the dedicated recovery guidance remotely." \
		"$output" "restoring forwarded provenance alias"
}

test_write_backup_metadata_pair_contents_to_store_emits_probe_stderr_for_remote_write_failures() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_run_remote_backup_helper_with_payload() {
				g_zxfer_remote_probe_stderr="pair write failed"
				return 92
			}
			zxfer_emit_remote_probe_failure_message() {
				printf '%s\n' "probe-stderr"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Transactional remote pair writes should fail closed when the helper reports a write failure status and probe stderr is available." \
		1 "$status"
	assertContains "Transactional remote pair-write failures should emit the staged probe stderr before throwing." \
		"$output" "probe-stderr"
	assertContains "Transactional remote pair-write failures should still surface the mounted-filesystem guidance after probe stderr." \
		"$output" "Error writing backup file. Is filesystem mounted?"
}

test_write_backup_metadata_pair_contents_to_store_preserves_transport_failure_stderr() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				cat >/dev/null
				printf '%s\n' "Permission denied (publickey)." >&2
				return 255
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		) 2>&1
	)
	status=$?

	assertEquals "Transactional remote pair writes should fail closed when ssh transport setup fails." \
		1 "$status"
	assertContains "Transactional remote pair-write transport failures should preserve the ssh diagnostic." \
		"$output" "Permission denied (publickey)."
	assertContains "Transactional remote pair-write transport failures should identify the remote host and primary target path." \
		"$output" "Failed to contact target host target.example while writing backup metadata /var/db/zxfer/tank/src/.zxfer_backup_info.src."
}

test_write_backup_metadata_pair_contents_to_store_reports_capture_failures_distinctly() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"

	set +e
	output=$(
		(
			zxfer_ensure_remote_backup_dir() {
				:
			}
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_run_remote_backup_helper_with_payload() {
				g_zxfer_remote_probe_capture_failed=1
				g_zxfer_remote_probe_stderr="Failed to read remote backup helper stderr capture from local staging."
				return 9
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Transactional remote pair writes should fail closed when local helper capture reload fails." \
		1 "$status"
	assertContains "Transactional remote pair writes should preserve the staged capture diagnostic." \
		"$output" "Failed to read remote backup helper stderr capture from local staging."
	assertContains "Transactional remote pair writes should report the local capture failure distinctly from transport errors." \
		"$output" "Failed to reload local remote helper capture while writing backup metadata /var/db/zxfer/tank/src/.zxfer_backup_info.src on host target.example."
	assertNotContains "Transactional remote pair writes should not misreport local capture failures as host-contact failures." \
		"$output" "Failed to contact target host target.example"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
