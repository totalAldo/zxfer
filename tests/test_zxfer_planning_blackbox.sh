#!/bin/sh
#
# Black-box argv-sequence planning suite for ./zxfer.
#
# Drives the REAL launcher against the canned zfs from
# tests/mock_toolchain_helper.sh and asserts on the MOCK_ZFS_LOG argv
# sequences. This suite pins the externally observable planning contract so
# internal-helper suites can be deleted or refactored later without losing
# behavioral coverage.
#
# GUID decision table — the invariant each test pins:
#
#   discovery argv shape
#       test_recursive_discovery_pins_guid_identity_listings
#       → source and destination snapshot listings request "-o name,guid":
#         snapshot identity is decided by guid, never by name alone.
#
#   src guid set == dst guid set
#       test_identical_source_and_destination_is_a_proven_noop
#       → proven no-op: exit 0, zero MUTATE / send / receive argv.
#
#   dst missing newest guid, -n
#       test_incremental_dryrun_issues_zero_zfs_argv_and_renders_no_plan
#       → dry run issues ZERO zfs argv and renders no send/receive plan
#         (current contract; -V explains the skip on stderr).
#
#   source listing exits non-zero
#       test_source_snapshot_listing_failure_fails_closed
#       → fail closed: non-zero exit, structured stderr failure report,
#         zero mutating argv.
#
#   dst existence check exits non-zero without "does not exist"
#       test_destination_existence_check_operational_failure_fails_closed
#       → operational error is NOT misread as a missing dataset: fail
#         closed instead of creating/sending.
#
#   dst snapshot listing exits non-zero
#       test_destination_snapshot_listing_failure_fails_closed
#       → fail closed with the zfs exit status preserved.
#
#   dst missing newest guid, live
#       test_live_recheck_runs_after_discovery_and_before_first_receive
#       → the depth-1 destination recheck lands after discovery and before
#         that dataset's send/receive pipeline starts.
#
#   same snapshot NAME on dst, different guid
#       test_same_name_divergent_guid_replans_incremental_send_not_noop
#       → NOT reported as a clean no-op: the divergent snapshot is treated
#         as absent and an incremental send is replanned from the newest
#         guid-matching ancestor.
#
#   dst-only snapshot, -d -n
#       test_delete_option_dryrun_issues_zero_zfs_argv
#       → dry run still issues ZERO zfs argv: no destroy is executed and no
#         destroy plan is rendered today.
#
#   dst-only snapshot, -d live
#       test_delete_option_live_destroys_only_extra_destination_snapshot
#       → exactly one "MUTATE destroy" of the extra snapshot and no sends.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/mock_toolchain_helper.sh
. "$TESTS_DIR/mock_toolchain_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_planning_blackbox"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	unset MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS \
		MOCK_SPAWN_LOG
	CASE_DIR=$(mktemp -d "$TEST_TMPDIR/case.XXXXXX") ||
		fail "Unable to create per-case temp directory."
}

tearDown() {
	if [ -n "${CASE_DIR:-}" ]; then
		rm -rf "$CASE_DIR"
	fi
	CASE_DIR=""
}

# Build the standard black-box environment in CASE_DIR: canned zfs in a mock
# bin dir plus the deterministic fixture tree (2 child datasets x 3 snaps).
planning_setup_env() {
	MOCKBIN_DIR="$CASE_DIR/mockbin"
	FIXTURE_DIR="$CASE_DIR/fixtures"
	ZFS_LOG="$CASE_DIR/zfs.log"

	mkdir -p "$MOCKBIN_DIR" || fail "Unable to create mock bin directory."
	zxfer_mockbin_write_canned_zfs "$MOCKBIN_DIR/zfs" ||
		fail "Unable to write canned zfs."
	zxfer_mockbin_build_fixture_tree "$FIXTURE_DIR" 2 3 ||
		fail "Unable to build fixture tree."
}

# Run ./zxfer black-box against one fixture state dir. Stdout/stderr land in
# $CASE_DIR/zxfer.stdout and $CASE_DIR/zxfer.stderr; status is returned.
planning_run_zxfer() {
	l_state_dir=$1
	shift

	zxfer_mockbin_run_zxfer "$MOCKBIN_DIR" "$l_state_dir" "$ZFS_LOG" "$@" \
		>"$CASE_DIR/zxfer.stdout" 2>"$CASE_DIR/zxfer.stderr"
}

# Copy one generated fixture state dir into a case-local scratch state so a
# test can rewrite manifest rules or fixture rows without touching the
# generated tree. Publishes the clone path in STATE_DIR.
planning_clone_state() {
	l_clone_source=$1
	l_clone_name=$2

	STATE_DIR="$CASE_DIR/state_$l_clone_name"
	mkdir -p "$STATE_DIR" || fail "Unable to create scratch state directory."
	cp "$l_clone_source"/* "$STATE_DIR/" ||
		fail "Unable to clone fixture state directory."
}

# Rewrite one manifest rule in STATE_DIR so the exact argv key answers with
# no output and the given non-zero exit status.
planning_force_manifest_failure() {
	l_force_key=$1
	l_force_status=$2

	awk -F'\t' -v key="$l_force_key" -v status="$l_force_status" '
		BEGIN { OFS = "\t" }
		$1 == key { print $1, "-", status; next }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to rewrite manifest rule."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install rewritten manifest."
}

planning_assert_log_has_line() {
	l_expected_line=$1

	grep -Fx "$l_expected_line" "$ZFS_LOG" >/dev/null 2>&1 ||
		fail "Expected zfs log line missing: $l_expected_line
zfs log: $(cat "$ZFS_LOG" 2>/dev/null)"
}

planning_assert_no_mutations() {
	if grep -q '^MUTATE ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero MUTATE lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

planning_assert_no_send_receive() {
	if grep -Eq '^(send|receive) ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero send/receive lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

# Print the 1-based line number of the first exact-match occurrence of the
# given argv line in the zfs log, or nothing when absent. Exact matching
# avoids prefix collisions such as "receive .../data" vs ".../data/child1".
planning_log_line_number() {
	awk -v needle="$1" '$0 == needle { print NR; exit }' "$ZFS_LOG"
}

# Assert the structured stderr failure report is present with the expected
# class, stage, and operator-facing message fragment.
planning_assert_failure_report() {
	l_report_stage=$1
	l_report_message=$2

	for l_report_line in \
		"zxfer: failure report begin" \
		"zxfer: failure report end" \
		"failure_class: runtime" \
		"failure_stage: $l_report_stage"; do
		grep -Fq "$l_report_line" "$CASE_DIR/zxfer.stderr" ||
			fail "Missing failure report line: $l_report_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	grep -Fq "$l_report_message" "$CASE_DIR/zxfer.stderr" ||
		fail "Missing failure report message: $l_report_message
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
}

# Give the destination root one extra snapshot (@snap9, guid unknown to the
# source) in both the recursive listing and the depth-1 recheck answer, and
# extend the manifest with the creation-time query that -d deletion planning
# issues for its destroy candidates.
planning_add_extra_destination_snapshot() {
	for l_extradst_fixture in dst_snapshots.list dst_d1_0.list; do
		printf '%s@snap9\t9999900009000000007\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
			>>"$STATE_DIR/$l_extradst_fixture" ||
			fail "Unable to append extra snapshot to $l_extradst_fixture."
	done
	printf '%s@snap3\t1700000003\n%s@snap9\t1700000009\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		>"$STATE_DIR/dst_creation.list" ||
		fail "Unable to write creation-time fixture."
	printf 'get -H -o name,value -p creation %s@*\tdst_creation.list\t0\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append creation-time manifest rule."
}

# Invariant: snapshot identity is guid-based. Both the source and the
# destination snapshot discovery listings must request "-o name,guid".
test_recursive_discovery_pins_guid_identity_listings() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/noop" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "recursive run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?

	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
}

# Invariant: identical source/destination guid sets are a proven no-op —
# exit 0 with zero mutating, send, or receive argv.
test_identical_source_and_destination_is_a_proven_noop() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/noop" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "no-op replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?

	assertTrue "no-op run must still have performed discovery" "[ -s '$ZFS_LOG' ]"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant (current dry-run contract, pinned 2026-06): -n issues ZERO zfs
# argv and renders NO send/receive plan on stdout. Planning would require
# live snapshot discovery, which the dry run skips entirely; with -V it
# explains the skip on stderr. A future change that renders a plan under -n
# must update this pin deliberately.
test_incremental_dryrun_issues_zero_zfs_argv_and_renders_no_plan() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -n -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "dry run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?
	assertFalse "dry run must not invoke zfs at all" "[ -s '$ZFS_LOG' ]"
	assertFalse "dry run renders no plan on stdout without -V" \
		"[ -s '$CASE_DIR/zxfer.stdout' ]"
	planning_assert_no_mutations

	planning_run_zxfer "$FIXTURE_DIR/incremental" -n -V -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "very verbose dry run should exit 0" 0 $?
	assertTrue "dry run should announce that planning is skipped" \
		"grep -q 'Dry run: send/receive and property-reconcile commands require live snapshot discovery' '$CASE_DIR/zxfer.stderr'"
	assertFalse "very verbose dry run still must not invoke zfs" \
		"[ -s '$ZFS_LOG' ]"
}

# Invariant: a failing source snapshot listing fails closed — the zfs exit
# status is preserved, a structured failure report lands on stderr, and no
# mutating argv is ever issued.
test_source_snapshot_listing_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" srcfail
	planning_force_manifest_failure \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" 2

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "source listing failure should preserve zfs exit status" 2 $?

	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshots from the source"
}

# Invariant: an operational destination existence-check failure (non-zero
# exit WITHOUT a "dataset does not exist" diagnostic) is not misread as a
# missing dataset — zxfer fails closed instead of creating or sending.
test_destination_existence_check_operational_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" dstexistfail
	planning_force_manifest_failure "list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "destination existence failure should exit 1" 1 $?

	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to determine whether destination dataset [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT] exists."
}

# Invariant: a failing destination snapshot listing fails closed with the
# zfs exit status preserved and zero mutating argv.
test_destination_snapshot_listing_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" dstsnapfail
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "destination snapshot listing failure should preserve zfs exit status" \
		2 $?

	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshot list from the destination."
}

# Invariant: immediately before mutating a dataset, zxfer re-checks that
# dataset's destination snapshots at depth 1. In the argv log the recheck
# must land AFTER all discovery listings and BEFORE that dataset's
# send/receive pipeline starts. Discovery log order is nondeterministic
# (background jobs), so ordering is asserted per line number, never
# positionally against the whole file.
test_live_recheck_runs_after_discovery_and_before_first_receive() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?
	planning_assert_no_mutations

	l_src_discovery=$(planning_log_line_number \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT")
	l_dst_exists=$(planning_log_line_number \
		"list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	l_dst_discovery=$(planning_log_line_number \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	assertNotNull "source discovery missing from log" "$l_src_discovery"
	assertNotNull "destination existence check missing from log" "$l_dst_exists"
	assertNotNull "destination discovery missing from log" "$l_dst_discovery"
	l_src_discovery=${l_src_discovery:-99999}
	l_dst_exists=${l_dst_exists:-99999}
	l_dst_discovery=${l_dst_discovery:-99999}

	for l_dataset_suffix in "" /child1 /child2; do
		l_recheck=$(planning_log_line_number \
			"list -H -d 1 -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_dataset_suffix")
		l_send=$(planning_log_line_number \
			"send -I $ZXFER_MOCKBIN_SOURCE_ROOT$l_dataset_suffix@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT$l_dataset_suffix@snap3")
		l_receive=$(planning_log_line_number \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_dataset_suffix")
		assertNotNull "live recheck missing for [$l_dataset_suffix]" "$l_recheck"
		assertNotNull "send missing for [$l_dataset_suffix]" "$l_send"
		assertNotNull "receive missing for [$l_dataset_suffix]" "$l_receive"
		l_recheck=${l_recheck:-0}
		l_send=${l_send:-0}
		l_receive=${l_receive:-0}

		assertTrue "recheck for [$l_dataset_suffix] must follow source discovery" \
			"[ $l_recheck -gt $l_src_discovery ]"
		assertTrue "recheck for [$l_dataset_suffix] must follow destination existence check" \
			"[ $l_recheck -gt $l_dst_exists ]"
		assertTrue "recheck for [$l_dataset_suffix] must follow destination discovery" \
			"[ $l_recheck -gt $l_dst_discovery ]"
		assertTrue "recheck for [$l_dataset_suffix] must precede its send" \
			"[ $l_recheck -lt $l_send ]"
		assertTrue "recheck for [$l_dataset_suffix] must precede its receive" \
			"[ $l_recheck -lt $l_receive ]"
	done
}

# Invariant (observed and pinned 2026-06): a destination snapshot that shares
# the source snapshot NAME but carries a different guid is NOT a clean no-op.
# Current zxfer treats the divergent snapshot as absent (guid identity) and
# replans an incremental send from the newest guid-matching ancestor
# (@snap2 -> @snap3) for that dataset only. It does NOT destroy the
# name-colliding destination snapshot first; on a real pool the collision
# would surface as a receive-time error. Untouched child datasets stay no-op.
test_same_name_divergent_guid_replans_incremental_send_not_noop() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" guiddiv

	# Same name, different guid for the root dataset's newest snapshot, in
	# both the recursive listing and the depth-1 live recheck answer.
	for l_guiddiv_fixture in dst_snapshots.list dst_d1_0.list; do
		awk -F'\t' 'BEGIN { OFS = "\t" }
			$1 == "dstpool/back/data@snap3" { $2 = "9999900003000000007" }
			{ print }
		' "$FIXTURE_DIR/noop/$l_guiddiv_fixture" \
			>"$STATE_DIR/$l_guiddiv_fixture" ||
			fail "Unable to rewrite $l_guiddiv_fixture with divergent guid."
	done

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "divergent-guid run should exit 0 against the canned zfs; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?

	# Work is planned for the diverged root dataset: NOT a clean no-op.
	planning_assert_log_has_line \
		"send -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "only the diverged dataset should be re-sent" 1 \
		"$(grep -c '^send ' "$ZFS_LOG")"
	# No destroy of the name-colliding snapshot is attempted today.
	planning_assert_no_mutations
}

# Invariant (current dry-run contract, pinned 2026-06): -d under -n issues
# ZERO zfs argv — deletion planning needs live discovery, so no destroy is
# executed and no destroy plan is rendered on stdout today.
test_delete_option_dryrun_issues_zero_zfs_argv() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" extradst_dryrun
	planning_add_extra_destination_snapshot

	planning_run_zxfer "$STATE_DIR" -d -n -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "-d dry run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?
	assertFalse "-d dry run must not invoke zfs at all" "[ -s '$ZFS_LOG' ]"
	assertFalse "-d dry run renders no destroy plan on stdout" \
		"[ -s '$CASE_DIR/zxfer.stdout' ]"
	planning_assert_no_mutations
}

# Invariant: with -d live, the destination-only snapshot is destroyed —
# exactly one MUTATE line, naming the extra snapshot — and nothing is sent
# because the destination is otherwise current. Deletion planning first
# queries the candidates' creation times.
test_delete_option_live_destroys_only_extra_destination_snapshot() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" extradst_live
	planning_add_extra_destination_snapshot

	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "-d live run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?

	planning_assert_log_has_line \
		"MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap9"
	assertEquals "exactly one destroy and no other mutations" 1 \
		"$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "an up-to-date destination must not be sent to" \
		"grep -q '^send ' '$ZFS_LOG'"
	assertTrue "deletion planning should query candidate creation times" \
		"grep -q '^get -H -o name,value -p creation $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@' '$ZFS_LOG'"
}

# Invariant: -V must not change replication outcomes. Regression for the
# batched -T destination discovery aborting under -V because a profiling
# recorder's non-zero status leaked into the batch function's return value
# (zxfer_profile_record_zfs_call returned 1 for destination-side calls).
test_remote_target_batch_discovery_succeeds_with_very_verbose() {
	planning_setup_env
	zxfer_mockbin_write_minimal_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write minimal mock ssh."
	SSH_LOG="$CASE_DIR/ssh.log"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -V -O localhost -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_remote_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-V remote no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_noop_status"
	assertTrue "the batched -T destination discovery must have run" \
		"grep -q 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1' '$SSH_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

. "$SHUNIT2_BIN"
