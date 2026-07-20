#!/bin/sh
#
# Integration tests for property reconciliation plus backup metadata storage and restore behavior.
# Sourced by tests/run_integration_zxfer.sh; the registry owns execution order.

invalid_override_property_test() {
	log "Starting invalid override property test"

	src_dataset="$SRC_POOL/invalid_prop_src"
	dest_root="$DEST_POOL/invalid_prop_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@p1"
	zfs create "$dest_root"

	set +e
	output=$("$ZXFER_BIN" -v -o "definitelynotaproperty=on" -N "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Invalid override property should cause zxfer to fail."
	fi
	if [ "$status" -ne 2 ]; then
		fail "Invalid override property should exit with status 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Missing source property for -o override: definitelynotaproperty." >/dev/null 2>&1; then
		fail "Invalid override property error message missing. Output: $output"
	fi

	log "Invalid override property test passed"
}

backup_dir_symlink_guard_test() {
	log "Starting backup directory symlink guard test"

	src_dataset="$SRC_POOL/backup_symlink_src"
	dest_root="$DEST_POOL/backup_symlink_dest"
	backup_dir_link="$WORKDIR/backup_symlink"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_dir_link"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@symlinkguard1"

	ln -s /tmp "$backup_dir_link"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir_link" "$ZXFER_BIN" -v -k -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Backup write should fail when ZXFER_BACKUP_DIR is a symlink."
	fi
	if ! printf '%s\n' "$output" | grep -q "Refusing to use backup directory"; then
		fail "Expected symlink guard error. Output: $output"
	fi

	safe_rm_rf "$backup_dir_link"

	log "Backup directory symlink guard test passed"
}

relative_backup_dir_rejection_test() {
	log "Starting relative backup directory rejection test"

	src_dataset="$SRC_POOL/backup_relative_src"
	dest_root="$DEST_POOL/backup_relative_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@relativeguard1"

	set +e
	output=$(ZXFER_BACKUP_DIR="relative-backups" "$ZXFER_BIN" -v -k -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Backup write should fail when ZXFER_BACKUP_DIR is relative."
	fi
	if ! printf '%s\n' "$output" | grep -q "ZXFER_BACKUP_DIR must be an absolute path"; then
		fail "Expected relative backup-dir rejection message. Output: $output"
	fi

	log "Relative backup directory rejection test passed"
}

missing_backup_metadata_error_test() {
	log "Starting missing backup metadata error test"

	src_dataset="$SRC_POOL/no_backup_src"
	dest_root="$DEST_POOL/no_backup_dest"
	backup_dir="$WORKDIR/no_backup_dir"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_dir"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@missing"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" "$ZXFER_BIN" -v -e -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Restore (-e) should fail when no backup metadata exists."
	fi
	if ! printf '%s\n' "$output" | grep -q "Cannot find backup property file"; then
		fail "Expected missing backup metadata message. Output: $output"
	fi
	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Destination dataset should not be created when backup metadata is missing."
	fi

	log "Missing backup metadata error test passed"
}

property_backup_restore_test() {
	log "Starting property backup/restore test"

	src_dataset="$SRC_POOL/prop_backup_src"
	dest_root="$DEST_POOL/prop_backup_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	backup_dir="$WORKDIR/backup_props"

	safe_rm_rf "$backup_dir"
	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@propbackup1"

	# First run backs up properties into a hardened temp directory.
	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$dest_root"

	# Verify destination received the property and the backup file exists with a header.
	dest_prop=$(zfs get -H -o value test:prop "$dest_dataset")
	if [ "$dest_prop" != "one" ]; then
		fail "Destination property expected 'one', got '$dest_prop'."
	fi

	backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$dest_dataset")
	if [ "$backup_file" = "" ]; then
		fail "Current backup metadata file was not written under $backup_dir."
	fi
	if ! grep -q "^#zxfer property backup file" "$backup_file"; then
		fail "Backup metadata missing expected header."
	fi
	if ! grep -q "^#format_version:2$" "$backup_file"; then
		fail "Backup metadata missing expected format-version marker."
	fi
	if ! grep -q "^#source_root:$src_dataset$" "$backup_file"; then
		fail "Backup metadata missing the expected full source_root header."
	fi
	if ! grep -q "^#destination_root:$dest_dataset$" "$backup_file"; then
		fail "Backup metadata missing the expected full destination_root header."
	fi

	# Mutate destination then restore from backup metadata.
	zfs set test:prop=mutated "$dest_dataset"
	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -e -R "$src_dataset" "$dest_root"

	dest_prop_after=$(zfs get -H -o value test:prop "$dest_dataset")
	if [ "$dest_prop_after" != "one" ]; then
		fail "Property restore expected 'one', got '$dest_prop_after'."
	fi

	log "Property backup/restore test passed"
}

chained_property_backup_provenance_test() {
	log "Starting chained property-backup provenance test"

	src_dataset="$SRC_POOL/prop_chain_src"
	intermediate_root="$DEST_POOL/prop_chain_mid"
	intermediate_dataset="$intermediate_root/${src_dataset##*/}"
	final_root="$DEST_POOL/prop_chain_final"
	final_dataset="$final_root/${intermediate_dataset##*/}"
	backup_dir="$WORKDIR/prop_chain_backup"

	safe_rm_rf "$backup_dir"
	destroy_test_datasets_if_present "$final_root" "$intermediate_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$intermediate_root"
	zfs create "$final_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "chain provenance"
	zfs snap -r "$src_dataset@chain1"

	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$intermediate_root"

	zfs set test:prop=two "$intermediate_dataset"
	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$intermediate_dataset" "$final_root"

	final_prop=$(zfs get -H -o value test:prop "$final_dataset")
	if [ "$final_prop" != "two" ]; then
		fail "Chained backup expected the live intermediate property value 'two' on the final dataset, got '$final_prop'."
	fi

	zfs set test:prop=mutated "$final_dataset"
	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -e -R "$intermediate_dataset" "$final_root"

	final_restored_prop=$(zfs get -H -o value test:prop "$final_dataset")
	if [ "$final_restored_prop" != "one" ]; then
		fail "Chained backup restore expected the original upstream property value 'one', got '$final_restored_prop'."
	fi

	log "Chained property-backup provenance test passed"
}

remote_property_backup_restore_test() {
	log "Starting remote property backup/restore test"

	mock_path="$WORKDIR/mock_remote_backup"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	backup_dir="$WORKDIR/remote_backup_dir"

	src_dataset="$SRC_POOL/remote_prop_src"
	dest_remote_root="$DEST_POOL/remote_prop_remote_dest"
	dest_remote_dataset="$dest_remote_root/${src_dataset##*/}"
	restore_dest_root="$DEST_POOL/remote_prop_restore"
	restore_dest_dataset="$restore_dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_remote_root" "$restore_dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_remote_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "remote backup seed"
	zfs snap -r "$src_dataset@rpb1"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -T localhost -R "$src_dataset" "$dest_remote_root"

	assert_snapshot_exists "$dest_remote_dataset" "rpb1"
	dest_prop=$(zfs get -H -o value test:prop "$dest_remote_dataset")
	if [ "$dest_prop" != "one" ]; then
		fail "Remote destination property expected 'one', got '$dest_prop'."
	fi

	set_test_dataset_mountpoint "$dest_remote_dataset" "$WORKDIR/mnt/remote_prop_dest"

	remote_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$dest_remote_dataset")
	if [ "$remote_backup_file" = "" ]; then
		fail "Current remote backup metadata file not created under $backup_dir."
	fi
	remote_mode=$(get_file_mode_octal "$remote_backup_file" 2>/dev/null || echo "")
	if [ "$remote_mode" != "600" ]; then
		fail "Remote backup metadata permissions expected 600, got $remote_mode."
	fi
	if ! grep -q "^#format_version:2$" "$remote_backup_file"; then
		fail "Remote backup metadata missing expected format-version marker."
	fi
	if ! grep -q "^#source_root:$src_dataset$" "$remote_backup_file"; then
		fail "Remote backup metadata missing the expected full source_root header."
	fi
	if ! grep -q "^#destination_root:$dest_remote_dataset$" "$remote_backup_file"; then
		fail "Remote backup metadata missing the expected full destination_root header."
	fi

	# Exact-pair restore metadata is keyed by the current source and destination.
	# Capture that pair before mutating the remote source dataset so the later
	# -e run restores the original property value instead of the live mutated one.
	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -O localhost -R "$dest_remote_dataset" "$restore_dest_root"
	assert_snapshot_exists "$restore_dest_dataset" "rpb1"

	# Remove the seeded restore target so the upcoming restore run exercises a
	# fresh receive while still consuming the exact-pair backup metadata.
	destroy_test_datasets_if_present "$restore_dest_root"

	zfs set test:prop=mutated "$dest_remote_dataset"
	append_data_to_dataset "$dest_remote_dataset" "file.txt" "after remote backup"
	zfs snap -r "$dest_remote_dataset@rpb2"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -e -O localhost -R "$dest_remote_dataset" "$restore_dest_root"

	assert_snapshot_exists "$restore_dest_dataset" "rpb1"
	assert_snapshot_exists "$restore_dest_dataset" "rpb2"
	restore_prop=$(zfs get -H -o value test:prop "$restore_dest_dataset")
	if [ "$restore_prop" != "one" ]; then
		fail "Restored property expected 'one', got '$restore_prop'."
	fi

	log "Remote property backup/restore test passed"
}

property_creation_with_zvol_test() {
	log "Starting property creation with zvol test"

	src_parent="$SRC_POOL/prop_create_src"
	src_child="$src_parent/child"
	src_zvol="$src_parent/vol"
	dest_root="$DEST_POOL/prop_create_dest"
	dest_parent="$dest_root/${src_parent##*/}"
	dest_child="$dest_parent/${src_child##*/}"
	dest_zvol="$dest_parent/${src_zvol##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_parent"

	zfs create "$src_parent"
	zfs create "$src_child"
	zfs create -V 8M "$src_zvol"
	zfs set compression=lz4 "$src_parent"
	zfs set atime=off "$src_child"
	zfs snap -r "$src_parent@pc1"

	run_zxfer -v -P -R "$src_parent" "$dest_root"

	assert_snapshot_exists "$dest_parent" "pc1"
	assert_snapshot_exists "$dest_child" "pc1"
	if ! zfs list -t volume "$dest_zvol" >/dev/null 2>&1; then
		fail "Destination zvol $dest_zvol missing after replication."
	fi

	parent_compression=$(zfs get -H -o value compression "$dest_parent")
	if [ "$parent_compression" != "lz4" ]; then
		fail "Expected compression=lz4 on $dest_parent, got $parent_compression."
	fi

	child_atime=$(zfs get -H -o value atime "$dest_child")
	if [ "$child_atime" != "off" ]; then
		fail "Expected atime=off on $dest_child, got $child_atime."
	fi

	src_volsize=$(zfs get -H -o value volsize "$src_zvol")
	dest_volsize=$(zfs get -H -o value volsize "$dest_zvol")
	if [ "$src_volsize" != "$dest_volsize" ]; then
		fail "Destination zvol size $dest_volsize does not match source $src_volsize."
	fi

	log "Property creation with zvol test passed"
}

property_override_and_ignore_test() {
	log "Starting property override/ignore test"

	src_dataset="$SRC_POOL/prop_override_src"
	src_child="$src_dataset/child"
	dest_root="$DEST_POOL/prop_override_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	dest_child="$dest_dataset/${src_child##*/}"
	override_mount="$WORKDIR/override_mountpoint"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$override_mount"

	zfs create "$src_dataset"
	zfs create "$src_child"
	zfs set compression=lz4 "$src_dataset"
	zfs set checksum=sha256 "$src_dataset"
	zfs set atime=off "$src_child"
	zfs snap -r "$src_dataset@pov1"

	run_zxfer -v -P -R "$src_dataset" "$dest_root"

	dest_child_checksum_before_source=$(zfs get -H -o source checksum "$dest_child")
	if [ "$dest_child_checksum_before_source" = "local" ]; then
		fail "Checksum on $dest_child should be inherited after initial replication."
	fi

	zfs set compression=off "$dest_dataset"
	mkdir -p "$override_mount"
	zfs set mountpoint="$override_mount" "$dest_dataset"
	zfs set atime=on "$dest_child"
	zfs set checksum=fletcher4 "$dest_child"

	ZXFER_SECURE_PATH='' run_zxfer -v -P -o "quota=32M,checksum=sha256" -I "mountpoint,compression" -R "$src_dataset" "$dest_root"

	dest_compression_after=$(zfs get -H -o value compression "$dest_dataset")
	if [ "$dest_compression_after" != "off" ]; then
		fail "Ignored compression property should remain off on $dest_dataset; saw $dest_compression_after."
	fi

	dest_mount_after=$(zfs get -H -o value mountpoint "$dest_dataset")
	if [ "$dest_mount_after" != "$override_mount" ]; then
		fail "Ignored mountpoint should remain $override_mount on $dest_dataset; saw $dest_mount_after."
	fi

	child_atime_after=$(zfs get -H -o value atime "$dest_child")
	if [ "$child_atime_after" != "off" ]; then
		fail "Expected atime=off to be set on $dest_child after property pass."
	fi

	child_checksum_after=$(zfs get -H -o value checksum "$dest_child")
	if [ "$child_checksum_after" != "sha256" ]; then
		fail "Expected checksum on $dest_child to converge to sha256 after property pass; saw $child_checksum_after."
	fi

	parent_quota=$(zfs get -H -o value quota "$dest_dataset")
	child_quota=$(zfs get -H -o value quota "$dest_child")
	if [ "$parent_quota" != "32M" ] || [ "$child_quota" != "32M" ]; then
		fail "Override quota not applied to parent/child: parent=$parent_quota child=$child_quota."
	fi
	child_quota_source=$(zfs get -H -o source quota "$dest_child")
	if [ "$child_quota_source" != "local" ]; then
		fail "Non-inheritable override quota on $dest_child should remain local, got source=$child_quota_source."
	fi
	child_checksum_source=$(zfs get -H -o source checksum "$dest_child")
	if [ "$child_checksum_source" = "local" ]; then
		fail "Inheritable override checksum on $dest_child should inherit from the replicated parent, not remain local."
	fi

	parent_snap_count=$(list_exact_snapshot_names_for_dataset "$dest_dataset" | wc -l | tr -d ' ')
	if [ "$parent_snap_count" -ne 1 ]; then
		fail "Property-only pass should not create extra snapshots; found $parent_snap_count on $dest_dataset."
	fi

	log "Property override/ignore test passed"
}

escaped_comma_override_test() {
	log "Starting escaped comma override test"

	src_dataset="$SRC_POOL/escaped_override_src"
	dest_root="$DEST_POOL/escaped_override_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set user:note=source "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "escaped override"
	zfs snap -r "$src_dataset@eco1"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	run_zxfer -v -P -o 'user:note=value\,with\,commas=and;semi' -R "$src_dataset" "$dest_root"

	dest_note=$(zfs get -H -o value user:note "$dest_dataset")
	if [ "$dest_note" != "value,with,commas=and;semi" ]; then
		fail "Escaped-comma override expected 'value,with,commas=and;semi' on $dest_dataset, got '$dest_note'."
	fi

	log "Escaped comma override test passed"
}

unsupported_property_skip_test() {
	log "Starting unsupported property skip test"

	mock_path="$WORKDIR/mock_unsupported_props"
	mock_log="$WORKDIR/mock_unsupported_props.log"
	src_dataset="$SRC_POOL/unsupported_src"
	dest_root="$DEST_POOL/unsupported_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	prepare_mock_bin_dir "$mock_path" zfs
	real_zfs=$(resolve_host_command zfs)
	safe_rm_f "$mock_path/zfs"
	safe_rm_f "$mock_log"
	cat >"$mock_path/zfs" <<EOF
#!/bin/sh
[ -n "\${MOCK_UNSUPPORTED_LOG:-}" ] && printf '%s\n' "\$*" >>"\$MOCK_UNSUPPORTED_LOG"
if [ "\$1" = "get" ] && [ "\$2" = "-Hpo" ] && [ "\$3" = "property,value,source" ] && [ "\$4" = "compression" ]; then
	case "\$5" in
	"$DEST_POOL" | "$dest_root")
		printf '%s\n' "cannot get property: invalid property 'compression'" >&2
		exit 1
		;;
	esac
fi
exec "$real_zfs" "\$@"
EOF
	chmod +x "$mock_path/zfs"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs set compression=lz4 "$src_dataset"
	zfs snap -r "$src_dataset@u1"

	set +e
	output=$(MOCK_UNSUPPORTED_LOG="$mock_log" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -U -P -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Property transfer with -U failed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Destination does not support property compression=lz4"; then
		fail "Unsupported property skip message missing. Output: $output"
	fi
	if grep -q "compression=lz4" "$mock_log"; then
		fail "zxfer should not attempt to create or set compression=lz4 when the destination reports compression unsupported. Log: $(cat "$mock_log")"
	fi

	log "Unsupported property skip test passed"
}

must_create_property_error_test() {
	log "Starting must-create property error test"

	check_sensitive="$SRC_POOL/case_support_sensitive"
	check_insensitive="$SRC_POOL/case_support_insensitive"
	destroy_test_datasets_if_present "$check_sensitive" "$check_insensitive"
	if ! zfs create -o casesensitivity=sensitive "$check_sensitive" >/dev/null 2>&1 ||
		! zfs create -o casesensitivity=insensitive "$check_insensitive" >/dev/null 2>&1; then
		log "Skipping must-create property error test (casesensitivity property unsupported)"
		destroy_test_datasets_if_present "$check_sensitive" "$check_insensitive"
		return
	fi
	destroy_test_datasets_if_present "$check_sensitive" "$check_insensitive"

	src_dataset="$SRC_POOL/case_src"
	dest_root="$DEST_POOL/case_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create -o casesensitivity=sensitive "$src_dataset"
	zfs snap -r "$src_dataset@case1"
	zfs create "$dest_root"
	zfs create -o casesensitivity=insensitive "$dest_dataset"

	src_case=$(zfs get -H -o value casesensitivity "$src_dataset")
	dest_case=$(zfs get -H -o value casesensitivity "$dest_dataset")
	if [ "$src_case" = "$dest_case" ]; then
		fail "Must-create property test setup requires differing casesensitivity values. Source=$src_case Destination=$dest_case."
	fi

	set +e
	output=$("$ZXFER_BIN" -v -P -N "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when must-create property differs between source and destination. Source casesensitivity=$src_case destination casesensitivity=$dest_case. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "may only be set"; then
		fail "Must-create property error message missing. Output: $output"
	fi

	log "Must-create property error test passed"
}

legacy_backup_layout_rejected_test() {
	log "Starting legacy backup layout rejection test"

	src_dataset="$SRC_POOL/legacy_backup_src"
	dest_root="$DEST_POOL/legacy_backup_dest"
	restore_root="$DEST_POOL/legacy_backup_restore"
	backup_dir="$WORKDIR/legacy_backup_dir"

	destroy_test_datasets_if_present "$dest_root" "$restore_root" "$src_dataset"
	safe_rm_rf "$backup_dir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=legacy "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "legacy content"
	zfs snap -r "$src_dataset@legacy1"

	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$dest_root"

	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$restore_root"
	restore_dataset="$restore_root/${src_dataset##*/}"
	restore_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$restore_dataset")
	if [ "$restore_backup_file" = "" ]; then
		fail "Exact-pair restore backup metadata file not found for legacy rejection test."
	fi

	src_mount=$(get_mountpoint "$src_dataset")
	legacy_backup="$src_mount/.zxfer_backup_info.${src_dataset##*/}"
	mv "$restore_backup_file" "$legacy_backup"
	chmod 600 "$legacy_backup"

	destroy_test_datasets_if_present "$dest_root" "$restore_root"
	zfs create "$dest_root"
	zfs set test:prop=mutated "$src_dataset"
	zfs snap -r "$src_dataset@legacy2"
	safe_rm_rf "$backup_dir"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" "$ZXFER_BIN" -v -e -R "$src_dataset" "$restore_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Restore with legacy backup metadata should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Cannot find backup property file" >/dev/null 2>&1; then
		fail "Expected missing-backup failure for legacy backup metadata. Output: $output"
	fi
	if zfs list "$restore_dataset" >/dev/null 2>&1; then
		fail "Restore dataset should not be created when only legacy backup metadata is available."
	fi

	log "Legacy backup layout rejection test passed"
}

unsupported_backup_format_version_rejected_test() {
	log "Starting unsupported backup format-version rejection test"

	src_dataset="$SRC_POOL/unsupported_backup_format_src"
	restore_root="$DEST_POOL/unsupported_backup_format_restore"
	restore_dataset="$restore_root/${src_dataset##*/}"
	backup_dir="$WORKDIR/unsupported_backup_format_dir"
	invalid_backup_tmp="$WORKDIR/unsupported_backup_format.tmp"

	destroy_test_datasets_if_present "$restore_root" "$src_dataset"
	safe_rm_rf "$backup_dir"
	safe_rm_f "$invalid_backup_tmp"

	zfs create "$src_dataset"
	zfs create "$restore_root"
	zfs set test:prop=format "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "unsupported backup format"
	zfs snap -r "$src_dataset@format1"

	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$restore_root"

	restore_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$restore_dataset")
	if [ "$restore_backup_file" = "" ]; then
		fail "Exact-pair restore backup metadata file not found for unsupported format-version rejection test."
	fi

	if ! awk '
		BEGIN {
			rewritten = 0
		}
		/^#format_version:2$/ {
			print "#format_version:999"
			rewritten = 1
			next
		}
		{
			print
		}
		END {
			exit(rewritten ? 0 : 1)
		}
	' "$restore_backup_file" >"$invalid_backup_tmp"; then
		fail "Failed to rewrite backup metadata format version for rejection test."
	fi
	mv "$invalid_backup_tmp" "$restore_backup_file"
	chmod 600 "$restore_backup_file"

	destroy_test_datasets_if_present "$restore_root"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" "$ZXFER_BIN" -v -e -R "$src_dataset" "$restore_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Restore with unsupported backup metadata format version should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "does not declare supported zxfer backup metadata format version #format_version:2" >/dev/null 2>&1; then
		fail "Expected unsupported format-version failure. Output: $output"
	fi
	if zfs list "$restore_dataset" >/dev/null 2>&1; then
		fail "Restore dataset should not be created when backup metadata format version is unsupported."
	fi

	log "Unsupported backup format-version rejection test passed"
}

remote_legacy_backup_layout_rejected_test() {
	log "Starting remote legacy backup layout rejection test"

	mock_path="$WORKDIR/mock_remote_legacy_backup"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	backup_dir="$WORKDIR/remote_legacy_backup_dir"

	src_dataset="$SRC_POOL/remote_legacy_backup_src"
	dest_root="$DEST_POOL/remote_legacy_backup_dest"
	restore_root="$DEST_POOL/remote_legacy_backup_restore"

	destroy_test_datasets_if_present "$dest_root" "$restore_root" "$src_dataset"
	safe_rm_rf "$backup_dir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=legacy "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "remote legacy content"
	zfs snap -r "$src_dataset@remotelegacy1"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -T localhost -R "$src_dataset" "$dest_root"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -T localhost -R "$src_dataset" "$restore_root"
	restore_dataset="$restore_root/${src_dataset##*/}"
	restore_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$restore_dataset")
	if [ "$restore_backup_file" = "" ]; then
		fail "Exact-pair remote restore backup metadata file not found for legacy rejection test."
	fi

	src_mount=$(get_mountpoint "$src_dataset")
	legacy_backup="$src_mount/.zxfer_backup_info.${src_dataset##*/}"
	mv "$restore_backup_file" "$legacy_backup"
	chmod 600 "$legacy_backup"

	destroy_test_datasets_if_present "$dest_root" "$restore_root"
	zfs create "$dest_root"
	zfs set test:prop=mutated "$src_dataset"
	zfs snap -r "$src_dataset@remotelegacy2"
	safe_rm_rf "$backup_dir"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -e -O localhost -R "$src_dataset" "$restore_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Remote restore with legacy backup metadata should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Cannot find backup property file" >/dev/null 2>&1; then
		fail "Expected missing-backup failure for remote legacy backup metadata. Output: $output"
	fi
	if zfs list "$restore_dataset" >/dev/null 2>&1; then
		fail "Remote restore dataset should not be created when only legacy backup metadata is available."
	fi

	log "Remote legacy backup layout rejection test passed"
}

insecure_backup_metadata_guard_test() {
	log "Starting insecure backup metadata guard test"

	src_dataset="$SRC_POOL/insecure_backup_src"
	dest_root="$DEST_POOL/insecure_backup_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	backup_root="$WORKDIR/insecure_backup_dir"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_root"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@insecure1"

	ZXFER_BACKUP_DIR="$backup_root" run_zxfer -v -k -R "$src_dataset" "$dest_root"
	local_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_root" "$src_dataset" "$dest_dataset")
	if [ "$local_backup_file" = "" ]; then
		fail "Exact-pair local backup metadata file not found for insecure metadata guard test."
	fi
	chmod 644 "$local_backup_file"
	destroy_test_datasets_if_present "$dest_root"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_root" "$ZXFER_BIN" -v -e -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Insecure local backup metadata should cause restore to fail."
	fi
	if ! printf '%s\n' "$output" | grep -q "permissions" >/dev/null 2>&1; then
		fail "Expected permission rejection message for insecure local metadata. Output: $output"
	fi
	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Destination dataset should not be created when local backup metadata is rejected."
	fi

	mock_path="$WORKDIR/mock_insecure_remote"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	remote_backup_root="$WORKDIR/remote_insecure_backup_dir"
	safe_rm_rf "$remote_backup_root"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@insecure2"

	ZXFER_BACKUP_DIR="$remote_backup_root" run_zxfer -v -k -R "$src_dataset" "$dest_root"
	remote_backup_file=$(find_backup_metadata_file_for_exact_pair "$remote_backup_root" "$src_dataset" "$dest_dataset")
	if [ "$remote_backup_file" = "" ]; then
		fail "Exact-pair remote backup metadata file not found for insecure metadata guard test."
	fi
	remote_expected_error="not owned by root"
	chmod 600 "$remote_backup_file"
	if command -v chown >/dev/null 2>&1 && chown 1 "$remote_backup_file" >/dev/null 2>&1; then
		:
	else
		chmod 644 "$remote_backup_file"
		remote_expected_error="permissions"
	fi
	destroy_test_datasets_if_present "$dest_root"

	set +e
	output=$(ZXFER_BACKUP_DIR="$remote_backup_root" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -e -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Insecure remote backup metadata should cause restore to fail."
	fi
	if ! printf '%s\n' "$output" | grep -q "$remote_expected_error" >/dev/null 2>&1; then
		fail "Expected remote metadata rejection message [$remote_expected_error]. Output: $output"
	fi
	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Destination dataset should not be created when remote backup metadata is rejected."
	fi

	log "Insecure backup metadata guard test passed"
}
