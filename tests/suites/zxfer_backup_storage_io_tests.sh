#!/bin/sh
# Backup storage publication, rollback, and remote-I/O behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

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
	primary_stage_dir="$g_zxfer_run_tmp_root/local_pair_finalize_register.primary.stage"
	forwarded_stage_dir="$g_zxfer_run_tmp_root/local_pair_finalize_register.alias.stage"
	primary_stage_file="$primary_stage_dir/backup.write"
	forwarded_stage_file="$forwarded_stage_dir/backup.write"
	primary_rollback_file="$TEST_TMPDIR/.zxfer-backup-rollback.pair-register-primary"
	forwarded_rollback_file="$TEST_TMPDIR/.zxfer-backup-rollback.pair-register-forwarded"
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
	g_zxfer_runtime_artifact_cleanup_paths="$primary_rollback_file
$forwarded_rollback_file"
	zxfer_cleanup_registered_runtime_artifacts
}

test_write_local_backup_file_pair_atomically_cleans_up_stage_dirs_when_primary_finalize_fails_after_forwarded_finalize_success() {
	primary_file="$TEST_TMPDIR/local_pair_primary_finalize_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_primary_finalize_fail_alias.meta"
	primary_stage_dir="$g_zxfer_run_tmp_root/local_pair_primary_finalize_fail.primary.stage"
	forwarded_stage_dir="$g_zxfer_run_tmp_root/local_pair_primary_finalize_fail.alias.stage"
	primary_stage_file="$primary_stage_dir/backup.write"
	forwarded_stage_file="$forwarded_stage_dir/backup.write"
	primary_rollback_file="$TEST_TMPDIR/.zxfer-backup-rollback.primary-finalize-fail"
	forwarded_rollback_file="$TEST_TMPDIR/.zxfer-backup-rollback.forwarded-finalize-fail"
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
	g_zxfer_runtime_artifact_cleanup_paths="$primary_rollback_file
$forwarded_rollback_file"
	zxfer_cleanup_registered_runtime_artifacts
}

test_write_local_backup_file_pair_atomically_cleans_up_primary_stage_when_forwarded_stage_creation_fails() {
	primary_file="$TEST_TMPDIR/local_pair_forwarded_stage_fail_primary.meta"
	forwarded_file="$TEST_TMPDIR/local_pair_forwarded_stage_fail_alias.meta"
	primary_stage_dir="$g_zxfer_run_tmp_root/local_pair_forwarded_stage_fail.primary.stage"
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

test_write_local_backup_file_pair_atomically_preserves_primary_rollback_when_primary_restore_fails() {
	pair_dir="$TEST_TMPDIR/local_pair_primary_restore_fail"
	primary_file="$pair_dir/primary.meta"
	forwarded_file="$pair_dir/alias.meta"
	mkdir -p "$pair_dir"
	printf '%s' "old-primary" >"$primary_file"
	printf '%s' "old-forwarded" >"$forwarded_file"
	chmod 600 "$primary_file" "$forwarded_file"

	set +e
	status=$(
		(
			mv() {
				if [ "$1" = "-f" ] && [ "$3" = "$primary_file" ]; then
					l_mv_source_tail=${2##*/}
					if [ "$l_mv_source_tail" = "backup.write" ] ||
						[ "${l_mv_source_tail#.zxfer-backup-rollback.}" != "$l_mv_source_tail" ]; then
						return 77
					fi
				fi
				command mv "$@"
			}
			zxfer_write_local_backup_file_pair_atomically "$primary_file" "#header;new-primary" "$forwarded_file" "#header;new-forwarded" >/dev/null
			printf '%s\n' "$?"
		)
	)
	set -e
	leftover_rollback=$(find "$pair_dir" -maxdepth 1 -type f -name '.zxfer-backup-rollback.*' | wc -l | tr -d '[:space:]')
	if [ -f "$primary_file" ]; then
		primary_contents=$(cat "$primary_file")
	else
		primary_contents="__MISSING__"
	fi

	assertEquals "Transactional local pair writes should preserve the primary restore failure status." \
		77 "$status"
	assertEquals "Transactional local pair writes should roll back the forwarded alias even when primary restore fails." \
		"old-forwarded" "$(cat "$forwarded_file")"
	assertEquals "A failed primary restore should leave primary metadata absent so the preserved rollback remains authoritative." \
		"__MISSING__" "$primary_contents"
	assertEquals "Transactional local pair writes should preserve the primary rollback file for manual recovery." \
		1 "$leftover_rollback"
}

test_write_local_backup_file_atomically_cleans_up_stage_dir_when_commit_fails() {
	backup_file="$TEST_TMPDIR/local_atomic_commit_failure.meta"
	stage_dir="$g_zxfer_run_tmp_root/local_atomic_commit_failure.stage"
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
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
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
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
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
	assertEquals "Single-file local backup writes should preserve the rendered metadata payload without semicolon translation." \
		"#header;payload" "$(cat "$backup_file")"
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

test_write_backup_metadata_contents_to_store_reports_local_rollback_restore_failure() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_store_rollback_failure_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_write_local_backup_file_atomically() {
				g_zxfer_backup_local_write_failure_result=rollback
				return 77
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_contents_to_store "$g_backup_storage_root/tank/src" "$g_backup_storage_root/tank/src/.zxfer_backup_info.src" "#header;payload"
		)
	)
	status=$?

	assertEquals "Single-file local backup writes should abort when rollback restoration fails." \
		1 "$status"
	assertContains "Single-file local rollback failures should surface manual recovery guidance." \
		"$output" "restoring backup metadata rollback state"
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
		"$output" "restoring backup metadata rollback state"
}

test_write_backup_metadata_pair_contents_to_store_reports_local_primary_restore_failure() {
	g_option_T_target_host=""
	g_backup_storage_root="$TEST_TMPDIR/local_pair_restore_store_root"

	set +e
	output=$(
		(
			zxfer_ensure_local_backup_dir() {
				:
			}
			zxfer_write_local_backup_file_pair_atomically() {
				g_zxfer_backup_local_write_failure_result=rollback
				return 77
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_write_backup_metadata_pair_contents_to_store "/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/.zxfer_backup_info.src" "#header;payload" "/var/db/zxfer/backup/dst/src" "/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" "#header;forwarded"
		)
	)
	status=$?

	assertEquals "Transactional pair writes should abort when restoring the primary rollback file fails locally." 1 "$status"
	assertContains "Transactional pair primary-restore failures should surface the rollback recovery guidance locally." \
		"$output" "restoring backup metadata rollback state"
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
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
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
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
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
	l_stage_dir="$g_zxfer_run_tmp_root/remote-backup-helper-stage"
	rm -rf "$l_stage_dir"

	set +e
	output=$(
		(
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 43
			}
			zxfer_create_private_temp_dir() {
				mkdir -p "$l_stage_dir" || return 1
				g_zxfer_runtime_artifact_path_result=$l_stage_dir
				printf '%s\n' "$l_stage_dir"
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				printf 'throw_status=%s\n' "$2"
				exit "$2"
			}
			zxfer_run_remote_backup_helper_with_payload "target.example" "printf '%s\\n' ok" "payload" destination
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup helper payload staging should fail closed when ssh transport setup fails before the remote helper runs." \
		43 "$status"
	assertContains "Remote backup helper payload staging should preserve the transport setup error instead of collapsing it into a later write failure." \
		"$output" "Managed ssh policy invalid."
	assertContains "Remote backup helper payload staging should preserve the transport setup failure status after profiling the failed ssh invocation." \
		"$output" "throw_status=43"
	assertFalse "Remote backup helper payload staging should not leak its staged temp directory when ssh transport setup fails before invocation." \
		"[ -e \"$l_stage_dir\" ]"
}

test_run_remote_backup_helper_with_payload_cleans_up_stage_dir_when_stdin_stage_write_fails() {
	l_stage_dir="$g_zxfer_run_tmp_root/remote-backup-helper-stdin-stage"
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
	l_stage_dir="$g_zxfer_run_tmp_root/remote-backup-helper-capture"
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
	assertEquals "Single-file remote backup writes should pass the rendered metadata payload without semicolon translation." \
		"#header;payload" "$(cat "$payload_file")"
	assertEquals "Single-file remote backup writes should leave capture-failure scratch cleared on helper success." \
		"0" "$(cat "$capture_file")"
}

test_write_backup_metadata_contents_to_store_stops_after_remote_directory_prepare_failure() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"
	stage_log="$TEST_TMPDIR/remote_single_prepare_failure.log"
	: >"$stage_log"

	(
		STAGE_LOG="$stage_log"
		zxfer_ensure_remote_backup_dir() {
			printf 'prepare %s\n' "$1" >>"$STAGE_LOG"
			return 37
		}
		zxfer_resolve_remote_cli_command_safe() {
			printf '%s\n' unexpected-resolve >>"$STAGE_LOG"
		}

		zxfer_write_backup_metadata_contents_to_store \
			"/var/db/zxfer/tank/src" \
			"/var/db/zxfer/tank/src/.zxfer_backup_info.src" \
			"#header;payload"
	)
	status=$?

	assertEquals "Single-file remote backup writes should preserve directory-preparation failures." \
		37 "$status"
	assertEquals "Single-file remote backup writes should stop before helper resolution after directory preparation fails." \
		"prepare /var/db/zxfer" "$(cat "$stage_log")"
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
	pair_split_line=$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE
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
	assertEquals "Transactional remote pair writes should splice the primary and forwarded metadata payloads with the split marker without semicolon translation." \
		"#header;payload
$pair_split_line
#header;forwarded" "$(cat "$payload_file")"
	assertEquals "Transactional remote pair writes should leave capture-failure scratch cleared on helper success." \
		"0" "$(cat "$capture_file")"
}

test_write_backup_metadata_pair_contents_to_store_stops_after_remote_directory_prepare_failure() {
	g_option_T_target_host="target.example"
	g_backup_storage_root="/var/db/zxfer"
	stage_log="$TEST_TMPDIR/remote_pair_prepare_failure.log"
	: >"$stage_log"

	(
		STAGE_LOG="$stage_log"
		zxfer_ensure_remote_backup_dir() {
			printf 'prepare %s\n' "$1" >>"$STAGE_LOG"
			case "$1" in
			/var/db/zxfer/tank/src) return 38 ;;
			esac
		}
		zxfer_build_remote_backup_pair_write_cmd() {
			printf '%s\n' unexpected-render >>"$STAGE_LOG"
		}

		zxfer_write_backup_metadata_pair_contents_to_store \
			"/var/db/zxfer/tank/src" \
			"/var/db/zxfer/tank/src/.zxfer_backup_info.src" \
			"#header;payload" \
			"/var/db/zxfer/backup/dst/src" \
			"/var/db/zxfer/backup/dst/src/.zxfer_backup_info.src" \
			"#header;forwarded"
	)
	status=$?

	assertEquals "Transactional remote backup writes should preserve directory-preparation failures." \
		38 "$status"
	assertEquals "Transactional remote backup writes should stop before later directory and renderer stages after a preparation failure." \
		"prepare /var/db/zxfer
prepare /var/db/zxfer/tank/src" "$(cat "$stage_log")"
}

test_build_remote_backup_pair_write_cmd_rolls_back_forwarded_after_primary_restore_failure() {
	primary_dir="$TEST_TMPDIR/remote_pair_primary_restore_fail_primary"
	forwarded_dir="$TEST_TMPDIR/remote_pair_primary_restore_fail_forwarded"
	primary_file="$primary_dir/.zxfer_backup_info.src"
	forwarded_file="$forwarded_dir/.zxfer_backup_info.src"
	fake_bin="$TEST_TMPDIR/remote_pair_primary_restore_fail_bin"
	mv_log="$TEST_TMPDIR/remote_pair_primary_restore_fail_mv.log"
	pair_split_line=$ZXFER_BACKUP_METADATA_PAIR_SPLIT_LINE
	real_mv=$(command -v mv)

	mkdir -p "$primary_dir" "$forwarded_dir" "$fake_bin"
	printf '%s' "old-primary" >"$primary_file"
	printf '%s' "old-forwarded" >"$forwarded_file"
	chmod 600 "$primary_file" "$forwarded_file"
	for tool in mktemp chmod rm rmdir awk; do
		tool_path=$(command -v "$tool")
		ln -s "$tool_path" "$fake_bin/$tool"
	done
	cat >"$fake_bin/mv" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$ZXFER_TEST_MV_LOG"
if [ "$#" -ge 3 ] && [ "$1" = "-f" ] && [ "$3" = "$ZXFER_TEST_PRIMARY_FILE" ]; then
	case ${2##*/} in
	backup.write|.zxfer-backup-rollback.*)
		exit 1
		;;
	esac
fi
exec "$ZXFER_TEST_REAL_MV" "$@"
EOF
	chmod +x "$fake_bin/mv"

	ZXFER_SECURE_PATH=$fake_bin
	ZXFER_TEST_REAL_MV=$real_mv
	ZXFER_TEST_PRIMARY_FILE=$primary_file
	ZXFER_TEST_MV_LOG=$mv_log
	export ZXFER_SECURE_PATH ZXFER_TEST_REAL_MV ZXFER_TEST_PRIMARY_FILE ZXFER_TEST_MV_LOG
	remote_cmd=$(zxfer_build_remote_backup_pair_write_cmd \
		"$primary_dir" "$primary_file" \
		"$forwarded_dir" "$forwarded_file" \
		"target.example")

	set +e
	printf '%s\n%s\n%s\n' "#header;new-primary" "$pair_split_line" "#header;new-forwarded" |
		sh -c "$remote_cmd"
	status=$?
	set -e
	unset ZXFER_SECURE_PATH ZXFER_TEST_REAL_MV ZXFER_TEST_PRIMARY_FILE ZXFER_TEST_MV_LOG

	if [ -f "$primary_file" ]; then
		primary_contents=$(cat "$primary_file")
	else
		primary_contents="__MISSING__"
	fi
	leftover_rollbacks=$(find "$primary_dir" "$forwarded_dir" -maxdepth 1 -type f -name '.zxfer-backup-rollback.*' | wc -l | tr -d '[:space:]')

	assertEquals "Remote pair writes should report rollback failure when the primary restore fails after primary publish failure." \
		98 "$status"
	assertEquals "Remote pair writes should still roll back the forwarded alias after primary restore failure." \
		"old-forwarded" "$(cat "$forwarded_file")"
	assertEquals "The forced primary restore failure should leave primary metadata absent for manual recovery from rollback." \
		"__MISSING__" "$primary_contents"
	assertEquals "Remote pair writes should preserve the failed primary rollback file for manual recovery." \
		1 "$leftover_rollbacks"
	assertContains "The generated helper should attempt the forwarded rollback after the failed primary restore path." \
		"$(cat "$mv_log")" "$forwarded_file"
}

test_remote_backup_protocol_renderers_match_readable_golden_output() {
	actual_script="$TEST_TMPDIR/remote_backup_protocol_scripts.actual"
	golden_script="$ZXFER_ROOT/tests/golden/remote_backup_protocol_scripts.golden"

	(
		zxfer_get_remote_backup_helper_dependency_path() {
			printf '%s\n' "/secure/bin:/usr/bin"
		}
		printf '%s\n' "### symlink-guard"
		zxfer_build_remote_backup_symlink_guard_cmd \
			"/var/db/zxfer/tank/src/backup.meta" 98 metadata
		printf '%s\n' "### dependency-check"
		zxfer_build_remote_backup_helper_dependency_check_cmd \
			"target.example doas" 99 mktemp chmod mv
		printf '%s\n' "### directory-prepare"
		zxfer_build_remote_backup_dir_prepare_cmd \
			"/var/db/zxfer/tank/src" "target.example doas" 99 92
		printf '%s\n' "### single-write"
		zxfer_build_remote_backup_write_cmd \
			"/var/db/zxfer/tank/src" \
			"/var/db/zxfer/tank/src/backup.meta" \
			"target.example doas" "cat" 99 92
		printf '%s\n' "### pair-write"
		zxfer_build_remote_backup_pair_write_cmd \
			"/var/db/zxfer/tank/src" \
			"/var/db/zxfer/tank/src/backup.meta" \
			"/var/db/zxfer/backup/dst" \
			"/var/db/zxfer/backup/dst/forwarded.meta" \
			"target.example doas" 99 92
	) >"$actual_script"

	if ! cmp -s "$golden_script" "$actual_script"; then
		diff -u "$golden_script" "$actual_script" >&2
		fail "Readable remote backup protocol renderers drifted from their exact-output golden."
	fi
}

test_remote_backup_protocol_transport_collapse_preserves_script_and_one_line_contract() {
	readable_script=$(zxfer_build_remote_backup_pair_write_cmd \
		"/var/db/zxfer/tank/src" "/var/db/zxfer/tank/src/backup.meta" \
		"/var/db/zxfer/backup/dst" "/var/db/zxfer/backup/dst/forwarded.meta" \
		"target.example doas" 99 92)
	transport_script=$(zxfer_prepare_remote_backup_transport_script "$readable_script")

	assertTrue "Readable pair-write renderers should expose protocol stages on separate lines." \
		"[ \"$(printf '%s\n' "$readable_script" | wc -l | tr -d '[:space:]')\" -gt 20 ]"
	assertEquals "Remote backup transport should collapse the readable renderer for csh/tcsh-safe handoff." \
		1 "$(printf '%s\n' "$transport_script" | wc -l | tr -d '[:space:]')"
	assertContains "Transport collapse must preserve the atomic publication stage." \
		"$transport_script" "rollback_forwarded"
	assertContains "Transport collapse must preserve the dedicated rollback failure status." \
		"$transport_script" "exit 98"
	assertTrue "The collapsed pair-write transport must remain valid POSIX shell syntax." \
		"printf '%s\n' \"\$transport_script\" | sh -n"
}

test_remote_backup_root_rejects_newlines_before_transport_collapse() {
	# shellcheck disable=SC2016  # Expanded inside the isolated helper shell.
	zxfer_test_capture_subshell '
		g_backup_storage_root="/unchanged/root"
		ZXFER_BACKUP_DIR=$(printf "/tmp/first\nsecond")
		zxfer_refresh_backup_storage_root
		printf "root=%s\n" "$g_backup_storage_root"
	'

	assertEquals "A backup root whose bytes would change in one-line remote transport must fail closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Rejected backup roots should explain the single-line path contract without replaying unsafe path bytes." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "single-line absolute path without control whitespace"
	assertNotContains "Rejected backup roots must not continue after translating the embedded newline to a space." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "root=/tmp/first second"
}

test_remote_backup_dependency_renderer_treats_host_metacharacters_as_data() {
	injected_marker="$TEST_TMPDIR/remote_backup_host_injected"
	host_spec="target.example env ZXFER_NOTE=\$(touch\${IFS}$injected_marker)"
	remote_cmd=$(zxfer_build_remote_backup_helper_dependency_check_cmd \
		"$host_spec" 99 zxfer-definitely-missing-helper)

	set +e
	output=$(sh -c "$remote_cmd" 2>&1)
	status=$?

	assertEquals "Missing dependency renderers should preserve their established failure status for metacharacter host specs." \
		99 "$status"
	assertFalse "Host-spec command substitutions must remain diagnostic data inside the rendered remote helper." \
		"[ -e \"$injected_marker\" ]"
	assertContains "Missing dependency diagnostics should preserve the literal host spec without evaluating it." \
		"$output" "not found on host $host_spec in secure PATH"
}

test_build_remote_backup_write_cmd_executes_staged_atomic_publication() {
	backup_dir="$TEST_TMPDIR/remote_single_renderer_success"
	backup_path="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	remote_cmd=$(zxfer_build_remote_backup_write_cmd \
		"$backup_dir" "$backup_path" "target.example" "cat" 99 92)

	printf '%s\n' "rendered-payload" | sh -c "$remote_cmd"
	status=$?
	leftovers=$(find "$backup_dir" -maxdepth 1 -type d \
		-name '.zxfer-backup-write.*' | wc -l | tr -d '[:space:]')

	assertEquals "The readable single-write renderer should execute successfully under POSIX sh." \
		0 "$status"
	assertEquals "The single-write renderer should publish the exact stdin payload." \
		"rendered-payload" "$(cat "$backup_path")"
	assertEquals "Successful single writes should remove their staging directory." \
		0 "$leftovers"
}

test_build_remote_backup_write_cmd_quotes_metacharacter_target_without_execution() {
	backup_dir="$TEST_TMPDIR/remote_single_renderer_metachar"
	backup_path="$backup_dir/metadata'; touch zxfer_backup_injected; : '"
	injected_marker="$TEST_TMPDIR/zxfer_backup_injected"
	mkdir -p "$backup_dir"
	remote_cmd=$(zxfer_build_remote_backup_write_cmd \
		"$backup_dir" "$backup_path" "target.example" "cat" 99 92)

	(
		cd "$TEST_TMPDIR" || exit 1
		printf '%s\n' "quoted-payload" | sh -c "$remote_cmd"
	)
	status=$?

	assertEquals "Metacharacter target paths should remain data during remote script execution." \
		0 "$status"
	assertFalse "Quoted target paths must not execute an injected command." \
		"[ -e \"$injected_marker\" ]"
	assertEquals "Metacharacter target paths should receive the exact staged payload." \
		"quoted-payload" "$(cat "$backup_path")"
}

test_build_remote_backup_write_cmd_cleans_staging_and_returns_92_on_payload_failure() {
	backup_dir="$TEST_TMPDIR/remote_single_renderer_failure"
	backup_path="$backup_dir/backup.meta"
	mkdir -p "$backup_dir"
	remote_cmd=$(zxfer_build_remote_backup_write_cmd \
		"$backup_dir" "$backup_path" "target.example" "false" 99 92)

	set +e
	printf '%s\n' "unpublished-payload" | sh -c "$remote_cmd"
	status=$?
	leftovers=$(find "$backup_dir" -maxdepth 1 -type d \
		-name '.zxfer-backup-write.*' | wc -l | tr -d '[:space:]')

	assertEquals "Payload-helper failures should keep the established remote write status." \
		92 "$status"
	assertFalse "Payload-helper failures must not publish a partial metadata file." \
		"[ -e \"$backup_path\" ]"
	assertEquals "Payload-helper failures should remove their staging directory." \
		0 "$leftovers"
}

test_build_remote_backup_pair_write_cmd_rejects_truncated_payload_before_publication() {
	primary_dir="$TEST_TMPDIR/remote_pair_truncated_primary"
	forwarded_dir="$TEST_TMPDIR/remote_pair_truncated_forwarded"
	primary_path="$primary_dir/backup.meta"
	forwarded_path="$forwarded_dir/forwarded.meta"
	mkdir -p "$primary_dir" "$forwarded_dir"
	printf '%s\n' "old-primary" >"$primary_path"
	printf '%s\n' "old-forwarded" >"$forwarded_path"
	remote_cmd=$(zxfer_build_remote_backup_pair_write_cmd \
		"$primary_dir" "$primary_path" "$forwarded_dir" "$forwarded_path" \
		"target.example" 99 92)

	set +e
	printf '%s\n' "payload-without-split-sentinel" | sh -c "$remote_cmd"
	status=$?
	leftovers=$(find "$primary_dir" "$forwarded_dir" -maxdepth 1 \
		\( -name '.zxfer-backup-write.*' -o -name '.zxfer-backup-rollback.*' \) |
		wc -l | tr -d '[:space:]')

	assertEquals "Truncated pair payloads should retain the established remote write failure status." \
		92 "$status"
	assertEquals "A truncated pair payload must leave the primary metadata unchanged." \
		"old-primary" "$(cat "$primary_path")"
	assertEquals "A truncated pair payload must leave the forwarded metadata unchanged." \
		"old-forwarded" "$(cat "$forwarded_path")"
	assertEquals "A truncated pair payload should not leak staging or rollback artifacts." \
		0 "$leftovers"
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
		"$output" "restoring backup metadata rollback state"
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

test_backup_storage_key_fallbacks_cover_empty_and_unavailable_digest_helpers() {
	set +e
	output=$(
		(
			od() {
				return 0
			}
			zxfer_backup_metadata_file_key "" ""
			printf 'file_key_status=%s\n' "$?"

			cksum() {
				return 1
			}
			od() {
				printf '%s\n' " 61 62"
			}
			zxfer_backup_metadata_legacy_file_key "tank/src" "backup/dst"
			printf 'legacy_hex_status=%s\n' "$?"

			od() {
				return 0
			}
			zxfer_backup_metadata_legacy_file_key "" ""
			printf 'legacy_empty_status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "The key-fallback test wrapper should finish after recording all stable results." \
		0 "$status"
	assertContains "An empty exact identity should retain a nonempty lossless key when od emits no bytes." \
		"$output" "h/00"
	assertContains "Legacy key fallback should use a bounded hex digest when cksum is unavailable." \
		"$output" "k6162"
	assertContains "Legacy key fallback should retain the historical k00 sentinel when no digest bytes are available." \
		"$output" "k00"
	assertContains "Exact-key fallback should return success for the defined empty identity." \
		"$output" "file_key_status=0"
	assertContains "Legacy hex fallback should return success." \
		"$output" "legacy_hex_status=0"
	assertContains "Legacy empty fallback should return success." \
		"$output" "legacy_empty_status=0"
}

test_wrap_remote_backup_helper_preserves_secure_path_failure_status() {
	set +e
	output=$(
		(
			zxfer_get_remote_backup_helper_dependency_path() {
				return 39
			}
			zxfer_wrap_remote_backup_helper_with_secure_path "printf payload"
			printf 'status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "The secure-PATH failure test wrapper should finish after recording the stage status." \
		0 "$status"
	assertContains "Remote backup wrappers should preserve secure-PATH validation failure statuses." \
		"$output" "status=39"
	assertNotContains "A failed secure-PATH lookup should not render any partial remote helper payload." \
		"$output" "printf payload"
}

test_create_backup_metadata_stage_dir_cleans_unregistered_directory() {
	stage_target="$TEST_TMPDIR/unregistered-stage/backup.meta"
	mkdir -p "${stage_target%/*}"

	set +e
	output=$(
		(
			zxfer_register_backup_metadata_runtime_artifact_path() {
				return 1
			}
			zxfer_create_backup_metadata_stage_dir_for_path \
				"$stage_target" "zxfer-unregistered-stage"
			printf 'status=%s\n' "$?"
		)
	)
	status=$?
	set -e
	leftovers=$(find "${stage_target%/*}" -maxdepth 1 -type d \
		-name '.zxfer-unregistered-stage.*' | wc -l | tr -d '[:space:]')

	assertEquals "The registration-failure test wrapper should finish after recording the stage status." \
		0 "$status"
	assertContains "A stage directory that cannot be registered should fail closed." \
		"$output" "status=1"
	assertEquals "Registration failure should remove the newly allocated directory immediately." \
		0 "$leftovers"
}

test_commit_local_backup_file_stage_removes_stale_rollback_after_restore() {
	target_file="$TEST_TMPDIR/commit-stale-rollback.meta"
	stage_file="$TEST_TMPDIR/commit-stale-rollback.stage"
	printf '%s' "old" >"$target_file"
	printf '%s' "new" >"$stage_file"
	g_test_backup_move_calls=0
	g_test_backup_removed_path=""
	zxfer_move_local_backup_metadata_path() {
		g_test_backup_move_calls=$((g_test_backup_move_calls + 1))
		if [ "$g_test_backup_move_calls" -eq 2 ]; then
			return 55
		fi
		return 0
	}
	zxfer_remove_local_backup_metadata_path_if_present() {
		g_test_backup_removed_path=$1
		return 0
	}

	set +e
	zxfer_commit_local_backup_file_stage "$target_file" "$stage_file" >/dev/null
	status=$?
	set -e

	assertEquals "The failed staged publication should preserve its move status after a successful rollback." \
		55 "$status"
	assertEquals "Commit failure should attempt original backup, staged publish, and rollback restore in order." \
		3 "$g_test_backup_move_calls"
	assertContains "A rollback helper that reports success without consuming its file should trigger stale rollback cleanup." \
		"$g_test_backup_removed_path" ".zxfer-backup-rollback."
}
