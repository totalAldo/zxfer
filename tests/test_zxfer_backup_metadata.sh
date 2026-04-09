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
		rm -rf "$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
		rm -rf "$TEST_TMPDIR/zxfer-s.$effective_uid.d"
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
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset ZXFER_BACKUP_DIR
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
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
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
	g_backup_storage_root=""
	g_backup_file_extension=""
	g_backup_file_contents=""
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
	g_restored_backup_file_contents="stale-restore"

	zxfer_reset_backup_metadata_state

	assertEquals "The backup-metadata reset helper should clear the accumulation buffer." \
		"" "$g_backup_file_contents"
	assertEquals "The backup-metadata reset helper should clear restored backup contents." \
		"" "$g_restored_backup_file_contents"
}

test_append_backup_metadata_record_preserves_existing_serialized_format() {
	g_backup_file_contents="existing"

	zxfer_append_backup_metadata_record "tank/src" "backup/dst" "compression=lz4=local"

	assertEquals "Backup-metadata appends should keep the existing serialized semicolon-delimited row format." \
		"existing;tank/src,backup/dst,compression=lz4=local" "$g_backup_file_contents"
}

test_render_backup_metadata_contents_preserves_write_format_without_mutating_accumulator() {
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src"
	g_option_N_nonrecursive=""
	g_destination="backup/dst"
	g_initial_source="tank/src"
	g_backup_file_contents=";tank/src,backup/dst,compression=lz4=local"

	rendered=$(zxfer_render_backup_metadata_contents)

	assertContains "Rendered backup metadata should include the current header and backup rows." \
		"$rendered" "#zxfer property backup file"
	assertContains "Rendered backup metadata should preserve the serialized property row payload." \
		"$rendered" ";tank/src,backup/dst,compression=lz4=local"
	assertEquals "Rendering backup metadata should not mutate the owner accumulator scratch state." \
		";tank/src,backup/dst,compression=lz4=local" "$g_backup_file_contents"
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
	assertContains "Dash-prefixed remote metadata paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup_file"
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

	result=$(
		(
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
	assertContains "Remote dry-run backup writes should render the local backup-content command with the common argv formatter." \
		"$result" "'printf' '%s'"
	assertContains "Remote dry-run backup writes should preserve wrapper tokens in the rendered remote pipeline." \
		"$result" "doas"
	assertContains "Remote dry-run backup writes should render the remote cat pipeline." \
		"$result" "$expected_name"
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

	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "/mnt/backups"
			}
			zxfer_write_backup_properties
		)
	)

	assertContains "Local dry-run backup writes should render a local redirection command." \
		"$result" "umask 077; 'printf' '%s'"
	assertContains "Local dry-run backup writes should target the secure backup path." \
		"$result" "$expected_name"
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
	assertContains "Backup-property writes should preserve encoded delimiter-heavy property payloads as one metadata row." \
		"$(cat "$written_file")" "tank/src,backup/dst,user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local"
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

	zxfer_write_backup_properties
	zxfer_get_backup_properties

	assertContains "Backup-property restore should find the file written by the matching recursive backup run via the exact secure dataset-tree path." \
		"$g_restored_backup_file_contents" "tank/src,backup/dst/src,compression=lz4"
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
	printf '%s\n' "tank/src/child,backup/other,compression=lz4" >"$direct_file"
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
	printf '%s\n' "tank/src/child,backup/dst,compression=lz4,extra" >"$direct_file"
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
	printf '%s\n%s\n' \
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
			printf '%s\n%s\n' \
				"tank/src/child,backup/dst,compression=lz4" \
				"tank/src/child,backup/dst,compression=off"
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
	printf '%s\n' "tank/parent/child,backup/dst,compression=lz4" >"$parent_backup"
	chmod 600 "$parent_backup"
	g_initial_source="tank/parent/child"
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1
	g_option_O_origin_host=""

	zxfer_get_backup_properties

	assertEquals "Backup-property discovery should walk up to ancestor datasets when the child has no metadata file." \
		"tank/parent/child,backup/dst,compression=lz4" "$g_restored_backup_file_contents"
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
			cat "$1"
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
			printf '%s\n' "tank/src/child,backup/dst,compression=lz4"
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
			printf '%s\n' "tank/parent/child,backup/dst,compression=lz4"
			return 0
		fi
		return 4
	}

	zxfer_get_backup_properties

	assertEquals "Remote backup-property discovery should walk up to the ancestor dataset tree using the exact secure keyed path." \
		"tank/parent/child,backup/dst,compression=lz4" "$g_restored_backup_file_contents"
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
				printf '%s\n' "tank/parent/child,backup/dst,compression=lz4"
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
			printf '%s\n' "tank/src/child,backup/dst,compression=lz4"
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
	assertContains "Local backup write failures should mention the mounted-filesystem guidance." \
		"$output" "Error writing backup file. Is filesystem mounted?"
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
		printf '%s\n' "$2" >"$log_file"
		cat >/dev/null
		return 0
	}

	zxfer_write_backup_properties

	assertContains "Live remote backup writes should use the resolved remote cat helper instead of bare cat." \
		"$(cat "$log_file")" "/remote/bin/cat"
	assertContains "Live remote backup writes should target the source dataset tree under ZXFER_BACKUP_DIR." \
		"$(cat "$log_file")" "/var/db/zxfer/tank/src/$expected_name"
}

test_write_backup_properties_marks_remote_cat_lookup_failures_as_dependency_errors() {
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
				printf '%s\n' "remote cat lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_write_backup_properties
		)
	)
	status=$?

	assertEquals "Remote backup writes should abort when the secure remote cat helper cannot be resolved." 1 "$status"
	assertContains "Remote backup write helper lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote backup write helper lookup failures should preserve the lookup message." \
		"$output" "msg=remote cat lookup failed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
