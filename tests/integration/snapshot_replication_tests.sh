#!/bin/sh
#
# Integration tests for snapshot replication, retention, divergence, seeding, and convergence behavior.
# Sourced by tests/run_integration_zxfer.sh; the registry owns execution order.

basic_replication_test() {
	# Exercise zxfer's standard ZFS send/receive mode with -R so recursive
	# snapshots and incremental updates propagate from source to destination.
	log "Starting basic replication test"
	src_dataset="$SRC_POOL/srcdata"
	dest_root="$DEST_POOL/replica"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$DEST_POOL/replica" "$SRC_POOL/srcdata"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "snapshot one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "snapshot two"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	append_data_to_dataset "$src_dataset" "file.txt" "snapshot three"
	zfs snap -r "$src_dataset@snap3"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "snap3"

	log "Basic replication test passed"
}

non_recursive_replication_test() {
	log "Starting non-recursive replication test"

	src_dataset="$SRC_POOL/nonrec_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/nonrec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "root.txt" "root data 1"
	zfs snap "$src_dataset@rootsnap1"
	append_data_to_dataset "$src_dataset" "root.txt" "root data 2"
	zfs snap "$src_dataset@rootsnap2"

	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap "$child_dataset@childsnap1"

	run_zxfer -v -N "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "rootsnap1"
	assert_snapshot_exists "$dest_dataset" "rootsnap2"

	if zfs list "$dest_dataset/child" >/dev/null 2>&1; then
		fail "Child dataset should not be replicated when using -N."
	fi

	log "Non-recursive replication test passed"
}

generate_tests_replication() {
	# Exercise the historical multi-dataset replication layout using file-backed
	# integration pools instead of direct host datasets.
	log "Starting multi-dataset replication test"

	src_parent="$SRC_POOL/zxfer_tests"
	src_dataset="$src_parent/src"
	dest_root="$DEST_POOL/zxfer_tests"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_parent" "$dest_root"

	zfs create "$src_parent"
	zfs create "$src_dataset"
	zfs create "$dest_root"

	# Ensure the top-level dataset has at least one snapshot so zxfer creates
	# the destination parent before children are replicated.
	zfs snap "$src_dataset@root_snap"

	for child in 1 2 3; do
		child_dataset="$src_dataset/child$child"
		zfs create "$child_dataset"

		for snap in 1 2 3 4; do
			zfs snap -r "$child_dataset@snap$snap"
		done
	done

	zfs snap -r "$src_dataset/child1@snap1_1"
	zfs snap -r "$src_dataset/child1@snap2_1"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	for child in 1 2 3; do
		child_dest_dataset="$dest_dataset/child$child"
		for snap in 1 2 3 4; do
			assert_snapshot_exists "$child_dest_dataset" "snap$snap"
		done
	done

	assert_snapshot_exists "$dest_dataset/child1" "snap1_1"
	assert_snapshot_exists "$dest_dataset/child1" "snap2_1"
	assert_snapshot_exists "$dest_dataset" "root_snap"

	log "Multi-dataset replication test passed"
}

idempotent_replication_test() {
	# Verify that repeated zxfer runs converge on a stable replica.
	log "Starting idempotent replication test"

	src_dataset="$SRC_POOL/idempotent_src"
	dest_root="$DEST_POOL/idempotent_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "idem.txt" "initial data"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "idem.txt" "second snapshot"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	append_data_to_dataset "$src_dataset" "idem.txt" "third snapshot"
	zfs snap -r "$src_dataset@snap3"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	snapshots_before=$(list_exact_snapshot_names_for_dataset "$dest_dataset")

	run_zxfer -v -R "$src_dataset" "$dest_root"

	snapshots_after=$(list_exact_snapshot_names_for_dataset "$dest_dataset")

	if [ "$snapshots_before" != "$snapshots_after" ]; then
		fail "zxfer should be idempotent; destination snapshots changed after a no-op run."
	fi

	log "Idempotent replication test passed"
}

auto_snapshot_replication_test() {
	log "Starting auto-snapshot replication test"

	src_dataset="$SRC_POOL/newsnap_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/newsnap_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	dest_child="$dest_dataset/${child_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "parent.txt" "parent data"
	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap -r "$src_dataset@preseed"

	run_zxfer -v -s -R "$src_dataset" "$dest_root"

	src_snapshot_name=$(get_latest_snapshot_name_for_dataset "$src_dataset")
	snap_suffix=${src_snapshot_name#*@}

	if [ -z "$snap_suffix" ] || [ "$snap_suffix" = "$src_snapshot_name" ]; then
		fail "Auto snapshot was not created on source dataset $src_dataset."
	fi

	assert_snapshot_exists "$src_dataset" "$snap_suffix"
	assert_snapshot_exists "$child_dataset" "$snap_suffix"
	assert_snapshot_exists "$dest_dataset" "$snap_suffix"
	assert_snapshot_exists "$dest_child" "$snap_suffix"

	log "Auto-snapshot replication test passed"
}

auto_snapshot_nonrecursive_test() {
	log "Starting auto-snapshot non-recursive test"

	src_dataset="$SRC_POOL/newsnap_nonrec_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/newsnap_nonrec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "parent.txt" "parent data"
	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap "$src_dataset@preseed"

	run_zxfer -v -s -N "$src_dataset" "$dest_root"

	src_snapshot_name=$(get_latest_snapshot_name_for_dataset "$src_dataset")
	snap_suffix=${src_snapshot_name#*@}

	if [ -z "$snap_suffix" ] || [ "$snap_suffix" = "$src_snapshot_name" ]; then
		fail "Auto snapshot was not created on source dataset $src_dataset."
	fi

	assert_snapshot_exists "$src_dataset" "$snap_suffix"
	assert_snapshot_exists "$dest_dataset" "$snap_suffix"

	if zfs list -t snapshot "$child_dataset@$snap_suffix" >/dev/null 2>&1; then
		fail "Child dataset should not receive auto snapshot when using -s with -N."
	fi
	if zfs list "$dest_dataset/${child_dataset##*/}" >/dev/null 2>&1; then
		fail "Child dataset should not be replicated when using -N with auto snapshot."
	fi

	log "Auto-snapshot non-recursive test passed"
}

trailing_slash_destination_test() {
	log "Starting trailing slash destination test"

	# Without trailing slash: destination should contain the source basename
	src_dataset="$SRC_POOL/tslash_no"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/tslash_dest_no"
	dest_dataset="$dest_root/${src_dataset##*/}"
	dest_child="$dest_dataset/${child_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "data"
	zfs snap -r "$src_dataset@tsnap"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "tsnap"
	assert_snapshot_exists "$dest_child" "tsnap"

	# With trailing slash: destination should be written directly into dest_root
	src_dataset="$SRC_POOL/tslash_yes"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/tslash_dest_yes"
	dest_child="$dest_root/${child_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "data2"
	zfs snap -r "$src_dataset@tsnap"

	run_zxfer -v -F -R "$src_dataset/" "$dest_root"

	assert_snapshot_exists "$dest_root" "tsnap"
	assert_snapshot_exists "$dest_child" "tsnap"

	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Trailing slash should not create an extra child dataset under destination root."
	fi

	log "Trailing slash destination test passed"
}

exclude_filter_test() {
	log "Starting exclude filter test"

	src_parent="$SRC_POOL/exclude_src"
	include_child="$src_parent/include_ds"
	exclude_child="$src_parent/exclude_me"
	dest_root="$DEST_POOL/exclude_dest"
	dest_parent="$dest_root/${src_parent##*/}"
	dest_include="$dest_parent/${include_child##*/}"
	dest_exclude="$dest_parent/${exclude_child##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_parent"

	zfs create "$src_parent"
	zfs create "$include_child"
	zfs create "$exclude_child"
	zfs create "$dest_root"

	append_data_to_dataset "$src_parent" "parent.txt" "parent data"
	append_data_to_dataset "$include_child" "include.txt" "include data"
	append_data_to_dataset "$exclude_child" "exclude.txt" "exclude data"
	zfs snap -r "$src_parent@exsnap"

	run_zxfer -v -x "exclude_me" -R "$src_parent" "$dest_root"

	assert_snapshot_exists "$dest_parent" "exsnap"
	assert_snapshot_exists "$dest_include" "exsnap"

	if zfs list "$dest_exclude" >/dev/null 2>&1; then
		fail "Dataset matching exclude pattern should not be replicated."
	fi

	log "Exclude filter test passed"
}

missing_destination_error_test() {
	log "Starting missing destination error test"

	src_dataset="$SRC_POOL/missing_dest_src"

	destroy_test_datasets_if_present "$src_dataset"
	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@p1"

	set +e
	output=$("$ZXFER_BIN" -v -R "$src_dataset" nosuchdestpool/target 2>&1)
	status=$?
	set -e

	destroy_test_datasets_if_present "$src_dataset"

	if [ "$status" -eq 0 ]; then
		fail "Missing destination list should cause zxfer to fail."
	fi
	if [ "$status" -ne 1 ]; then
		fail "Missing destination list should preserve the destination lookup status 1, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Destination dataset [nosuchdestpool/target] is missing and destination pool [nosuchdestpool] could not be listed" >/dev/null 2>&1; then
		fail "Missing destination error message missing. Output: $output"
	fi

	log "Missing destination error test passed"
}

dry_run_replication_test() {
	log "Starting dry-run replication test"

	src_dataset="$SRC_POOL/dryrun_src"
	dest_root="$DEST_POOL/dryrun_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@dr1"
	zfs create "$dest_root"

	output=$("$ZXFER_BIN" -v -n -R "$src_dataset" "$dest_root" 2>&1)
	log "$output"

	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Dry run should not create destination dataset $dest_dataset."
	fi

	log "Dry-run replication test passed"
}

yield_loop_dryrun_iteration_test() {
	log "Starting yield loop dry-run iteration test"

	src_dataset="$SRC_POOL/yield_src"
	dest_root="$DEST_POOL/yield_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	zfs snap -r "$src_dataset@base"
	ZXFER_BACKUP_DIR='' run_zxfer -v -R "$src_dataset" "$dest_root"

	# With no new snapshots to send, -Y -n should perform a single iteration.
	set +e
	output=$("$ZXFER_BIN" -v -Y -n -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Expected zxfer -Y -n to exit successfully. Output: $output"
	fi

	iter_count=$(printf '%s\n' "$output" | grep -c "Begin Iteration")
	if [ "$iter_count" -ne 1 ]; then
		fail "Expected a single iteration under -Y -n; found $iter_count. Output: $output"
	fi

	assert_snapshot_exists "$dest_dataset" "base"

	log "Yield loop dry-run iteration test passed"
}

force_rollback_test() {
	log "Starting force rollback test"

	src_dataset="$SRC_POOL/rollback_src"
	dest_root="$DEST_POOL/rollback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "original"
	zfs snap "$src_dataset@snap1"

	run_zxfer -v -N "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	set_test_dataset_mountpoint "$dest_dataset" "$WORKDIR/mnt/rollback_dest"

	# Diverge destination with an extra snapshot.
	append_data_to_dataset "$dest_dataset" "file.txt" "dest divergence"
	zfs snap "$dest_dataset@destonly"

	# Advance source and create a new snapshot to send.
	append_data_to_dataset "$src_dataset" "file.txt" "source update"
	zfs snap "$src_dataset@snap2"

	run_zxfer -v -F -N "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "snap2"
	if zfs list -t snapshot "$dest_dataset@destonly" >/dev/null 2>&1; then
		fail "Force rollback should remove divergent destination snapshot destonly."
	fi

	log "Force rollback test passed"
}

snapshot_deletion_test() {
	log "Starting snapshot deletion test"

	src_dataset="$SRC_POOL/snapdel_src"
	dest_root="$DEST_POOL/snapdel_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	# Create initial state
	append_data_to_dataset "$src_dataset" "file.txt" "data1"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "data2"
	zfs snap -r "$src_dataset@snap2"

	# Replicate
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	# Delete snap1 on source
	destroy_test_dataset "$src_dataset@snap1"

	# Run without -d (snap1 should remain on dest)
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	# Run with -n -d (strict dry run, snap1 should remain and live delete
	# planning is intentionally skipped)
	set +e
	output=$("$ZXFER_BIN" -v -V -n -d -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e
	if [ "$status" -ne 0 ]; then
		fail "Strict dry-run snapshot deletion preview failed with status $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Dry run: skipping live replication-state validation and command planning."; then
		fail "Expected strict dry-run planning skip message for snapshot deletion preview. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Dry run: send/receive and property-reconcile commands require live snapshot discovery and are not rendered."; then
		fail "Expected strict dry-run no-render message for snapshot deletion preview. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "snap1"

	# Run with -d (snap1 should be deleted)
	run_zxfer -v -Y -d -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "snap1" 30
	wait_for_snapshot_absent "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	log "Snapshot deletion test passed"
}

snapshot_name_mismatch_deletion_test() {
	log "Starting snapshot name mismatch deletion test"

	src_dataset="$SRC_POOL/mismatch_src"
	dest_root="$DEST_POOL/mismatch_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@alpha"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@beta"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "alpha"
	assert_snapshot_exists "$dest_dataset" "beta"

	# Diverge source and destination with multiple differently named snapshots to
	# exercise the deletion comm/sort pipeline that relies on both temp files.
	# This resend path also needs -F: deleting snapshots alone does not roll the
	# live destination dataset back to the last common snapshot.
	destroy_test_dataset "$src_dataset@beta"
	append_data_to_dataset "$src_dataset" "file.txt" "three"
	zfs snap -r "$src_dataset@gamma"

	zfs snap -r "$dest_dataset@z-extra"
	zfs snap -r "$dest_dataset@doomed-beta"

	run_zxfer -v -Y -d -F -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "beta" 30
	wait_for_destroy_process_to_finish "$dest_dataset" "z-extra" 30
	wait_for_snapshot_absent "$dest_dataset" "beta"
	wait_for_snapshot_absent "$dest_dataset" "z-extra"
	wait_for_snapshot_absent "$dest_dataset" "doomed-beta"

	assert_snapshot_exists "$dest_dataset" "alpha"
	assert_snapshot_exists "$dest_dataset" "gamma"

	log "Snapshot name mismatch deletion test passed"
}

snapshot_name_prefix_collision_deletion_test() {
	log "Starting snapshot name prefix collision deletion test"

	src_dataset="$SRC_POOL/prefix_collision_src"
	dest_root="$DEST_POOL/prefix_collision_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap10"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap10"

	# Removing snap1 on the source should not cause -d to delete snap10 on the
	# destination just because the names share a prefix.
	destroy_test_dataset "$src_dataset@snap1"

	run_zxfer -v -Y -d -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "snap1" 30
	wait_for_snapshot_absent "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap10"

	log "Snapshot name prefix collision deletion test passed"
}

send_command_dryrun_test() {
	log "Starting send command dry-run test"

	src_dataset="$SRC_POOL/sendcmd_src"
	dest_root="$DEST_POOL/sendcmd_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap2"

	set +e
	output=$("$ZXFER_BIN" -v -V -w -n -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Dry-run send command test failed. Output: $output"
	fi

	if ! printf '%s\n' "$output" | grep -q "Dry run: send/receive and property-reconcile commands require live snapshot discovery and are not rendered."; then
		fail "Expected strict dry-run no-render message instead of a live send pipeline. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "send -v -w -I"; then
		fail "Strict dry-run should not render a live incremental send command anymore. Output: $output"
	fi

	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Dry run should not create destination dataset $dest_dataset."
	fi

	log "Send command dry-run test passed"
}

raw_send_replication_test() {
	log "Starting raw send replication test"

	keyfile="$WORKDIR/raw_keyfile"
	safe_rm_f "$keyfile"
	printf '%s\n' "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" >"$keyfile"

	src_dataset="$SRC_POOL/raw_send_src"
	dest_root="$DEST_POOL/raw_send_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	if ! zfs create -o encryption=on -o keyformat=hex -o keylocation="file://$keyfile" "$src_dataset" >/dev/null 2>&1; then
		log "Skipping raw send replication test (encryption/raw send unsupported on this host)"
		safe_rm_f "$keyfile"
		return
	fi
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "raw stream one"
	zfs snap -r "$src_dataset@raw1"
	append_data_to_dataset "$src_dataset" "file.txt" "raw stream two"
	zfs snap -r "$src_dataset@raw2"

	run_zxfer -v -w -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "raw1"
	assert_snapshot_exists "$dest_dataset" "raw2"
	dest_encryption=$(zfs get -H -o value encryption "$dest_dataset" 2>/dev/null || echo "")
	if [ "$dest_encryption" = "off" ] || [ "$dest_encryption" = "" ]; then
		fail "Raw send should preserve encryption on destination; got '$dest_encryption'."
	fi

	safe_rm_f "$keyfile"

	log "Raw send replication test passed"
}

grandfather_protection_test() {
	log "Starting grandfather protection test"

	src_dataset="$SRC_POOL/grand_src"
	dest_root="$DEST_POOL/grand_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@base"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "base"

	# With -g 0, any deletion attempt should be rejected before destroying snapshots.
	set +e
	output=$("$ZXFER_BIN" -v -g 0 -d -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Grandfather protection should fail when -g 0 blocks deletion."
	fi
	if ! printf '%s\n' "$output" | grep -q "grandfather"; then
		fail "Grandfather protection message missing. Output: $output"
	fi

	assert_snapshot_exists "$dest_dataset" "base"

	log "Grandfather protection test passed"
}

delete_dest_only_snapshot_test() {
	log "Starting destination-only snapshot delete test"

	src_dataset="$SRC_POOL/destonly_src"
	dest_root="$DEST_POOL/destonly_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	# This test only needs one common snapshot plus one destination-only
	# snapshot. Seed the common snapshot with the same single-snapshot zxfer
	# path used by other stable integration tests, then add a destination-only
	# snapshot on top.
	append_data_to_dataset "$src_dataset" "file.txt" "base"
	zfs snap -r "$src_dataset@base"

	set +e
	output=$(run_zxfer -v -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Initial zxfer seed run failed in delete_dest_only_snapshot_test. Output: $output"
	fi
	if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
		l_i=0
		while [ "$l_i" -lt 30 ]; do
			if zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
				break
			fi
			sleep 1
			l_i=$((l_i + 1))
		done
		if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
			l_source_snaps=$(zfs list -H -t snapshot -o name -r "$src_dataset" 2>/dev/null || true)
			l_dest_snaps=$(zfs list -H -t snapshot -o name -r "$dest_dataset" 2>/dev/null || true)
			fail "Initial zxfer seed run did not produce expected common snapshot $dest_dataset@base. zxfer output: $output. Source snapshots: ${l_source_snaps:-<none>}. Destination snapshots: ${l_dest_snaps:-<none>}."
		fi
	fi

	# Create a destination-only snapshot that should be removed by -d even when no new sends are pending.
	zfs snap -r "$dest_dataset@destonly"
	assert_snapshot_exists "$dest_dataset" "destonly"

	set +e
	output=$("$ZXFER_BIN" -v -d -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "zxfer -d run failed. Output: $output"
	fi

	wait_for_destroy_process_to_finish "$dest_dataset" "destonly" 30
	wait_for_snapshot_absent "$dest_dataset" "destonly"
	if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
		l_i=0
		while [ "$l_i" -lt 30 ]; do
			if zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
				break
			fi
			sleep 1
			l_i=$((l_i + 1))
		done
		if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
			l_source_snaps=$(zfs list -H -t snapshot -o name -r "$src_dataset" 2>/dev/null || true)
			l_dest_snaps=$(zfs list -H -t snapshot -o name -r "$dest_dataset" 2>/dev/null || true)
			fail "Delete-only run removed or failed to preserve common snapshot $dest_dataset@base. zxfer output: $output. Source snapshots after delete run: ${l_source_snaps:-<none>}. Destination snapshots after delete run: ${l_dest_snaps:-<none>}."
		fi
	fi

	log "Destination-only snapshot delete test passed"
}

existing_empty_destination_seed_test() {
	log "Starting existing empty destination seed test"

	src_dataset="$SRC_POOL/existing_empty_seed_src"
	dest_root="$DEST_POOL/existing_empty_seed_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs create "$dest_dataset"

	append_data_to_dataset "$src_dataset" "file.txt" "seed one"
	zfs snap "$src_dataset@seed1"

	set +e
	output=$(run_zxfer -v -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Existing empty destination seed run failed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "exists but has no snapshots. Seeding with [$src_dataset@seed1]" >/dev/null 2>&1; then
		fail "Expected non-creation seed branch message for existing empty destination. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "seed1"

	append_data_to_dataset "$src_dataset" "file.txt" "seed two"
	zfs snap "$src_dataset@seed2"
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "seed2"

	log "Existing empty destination seed test passed"
}

dry_run_deletion_test() {
	log "Starting dry-run deletion test"

	src_dataset="$SRC_POOL/dryrun_del_src"
	dest_root="$DEST_POOL/dryrun_del_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	destroy_test_dataset "$src_dataset@snap1"

	output=$("$ZXFER_BIN" -v -V -n -d -R "$src_dataset" "$dest_root" 2>&1)
	log "$output"

	if ! printf '%s\n' "$output" | grep -q "Dry run: skipping live replication-state validation and command planning."; then
		fail "Expected strict dry-run planning skip message for deletion preview. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Dry run: send/receive and property-reconcile commands require live snapshot discovery and are not rendered."; then
		fail "Expected strict dry-run no-render message for deletion preview. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	log "Dry-run deletion test passed"
}
