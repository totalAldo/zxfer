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
#         snapshot identity is decided by guid, never by name alone. Pinned
#         against the incremental fixture so the proof falls back and the
#         full creation-order discovery shape stays covered.
#
#   src guid set == dst guid set
#       test_identical_source_and_destination_is_a_proven_noop
#       → proven no-op via the fast recursive proof (local sources included
#         since Phase 8): exit 0, exactly the two sorted-FIFO identity
#         listings, no creation-order listing, no existence check, zero
#         MUTATE / send / receive argv.
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
#       test_live_recheck_batched_view_is_generation_gated
#       → live rechecks are served from ONE batched recursive destination
#         listing per destination-mutation generation: the batched listing
#         lands after discovery and before the first receive, and each
#         receive forces a fresh batched listing before the next dataset's
#         receive.
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

# Print the 1-based line number of the Nth exact-match occurrence of the
# given argv line, or nothing when fewer occurrences exist. Needed because
# discovery and the batched live destination view share one argv shape.
planning_log_nth_line_number() {
	awk -v needle="$1" -v n="$2" \
		'$0 == needle { c++; if (c == n) { print NR; exit } }' "$ZFS_LOG"
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
# Driven against the incremental fixture: the fast no-op proof attempts
# first (its identity listing also requests name,guid), mismatches, and the
# full creation-order discovery shape stays pinned on the fallback.
test_recursive_discovery_pins_guid_identity_listings() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "recursive run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?

	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
}

# Invariant: identical source/destination guid sets are a proven no-op —
# exit 0 with zero mutating, send, or receive argv. Since Phase 8 the fast
# recursive proof covers local sources too: a clean no-op is proven from the
# two sorted identity listings alone (one source, one destination) and never
# pays for the creation-order source listing or the destination existence
# check.
test_identical_source_and_destination_is_a_proven_noop() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/noop" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "no-op replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?

	assertTrue "no-op run must still have performed discovery" "[ -s '$ZFS_LOG' ]"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "a proven clean no-op costs exactly the two proof identity listings" \
		2 "$(wc -l <"$ZFS_LOG" | tr -d ' ')"
	assertFalse "a proven clean no-op must skip the creation-order source listing" \
		"grep -q -- '-s creation' '$ZFS_LOG'"
	assertFalse "a proven clean no-op must skip the destination existence check" \
		"grep -Fxq 'list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT' '$ZFS_LOG'"
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
# mutating argv is ever issued. Both source listing shapes are forced to
# fail: the proof's identity listing failure surfaces as a stream mismatch
# and falls back to full discovery, whose creation-order listing failure
# then fails the run closed.
test_source_snapshot_listing_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" srcfail
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" 2
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
# Cloned from the incremental fixture: the existence check only runs in full
# discovery, which a clean no-op would short-circuit via the proof path.
test_destination_existence_check_operational_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" dstexistfail
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

# Invariant: live rechecks are generation-gated. Planning is served from ONE
# batched recursive destination snapshot listing (same argv shape as
# discovery, so occurrences are counted) that is captured after discovery and
# refreshed only after this run mutates the destination: the first batched
# listing lands after discovery and before the first receive, each completed
# receive forces exactly one fresh batched listing before the next dataset's
# receive, no batched listing follows the final receive, and the old
# per-dataset depth-1 recheck never runs for covered datasets. Discovery log
# order is nondeterministic (background jobs), so ordering is asserted per
# line number, never positionally against the whole file.
test_live_recheck_batched_view_is_generation_gated() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?
	planning_assert_no_mutations

	l_view_key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_src_discovery=$(planning_log_line_number \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT")
	l_dst_exists=$(planning_log_line_number \
		"list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	assertNotNull "source discovery missing from log" "$l_src_discovery"
	assertNotNull "destination existence check missing from log" "$l_dst_exists"
	l_src_discovery=${l_src_discovery:-99999}
	l_dst_exists=${l_dst_exists:-99999}

	assertFalse "covered datasets must be served from the batched view, never per-dataset depth-1 rechecks" \
		"grep -q '^list -H -d 1 ' '$ZFS_LOG'"
	# Occurrence 1 is the fast no-op proof's destination identity listing
	# (it mismatches and falls back), occurrence 2 is discovery, then one
	# batched view per consumed destination mutation generation.
	assertEquals "one proof listing plus one discovery listing plus one batched view per consumed destination mutation generation" \
		5 "$(grep -cFx "$l_view_key" "$ZFS_LOG")"

	l_prev_receive=0
	l_view_occurrence=2
	for l_dataset_suffix in "" /child1 /child2; do
		l_view_occurrence=$((l_view_occurrence + 1))
		l_view=$(planning_log_nth_line_number "$l_view_key" "$l_view_occurrence")
		l_receive=$(planning_log_line_number \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_dataset_suffix")
		assertNotNull "batched view listing $l_view_occurrence missing for [$l_dataset_suffix]" "$l_view"
		assertNotNull "receive missing for [$l_dataset_suffix]" "$l_receive"
		l_view=${l_view:-0}
		l_receive=${l_receive:-99999}

		assertTrue "view listing for [$l_dataset_suffix] must follow source discovery" \
			"[ $l_view -gt $l_src_discovery ]"
		assertTrue "view listing for [$l_dataset_suffix] must follow the destination existence check" \
			"[ $l_view -gt $l_dst_exists ]"
		assertTrue "view listing for [$l_dataset_suffix] must follow the previous dataset's receive: a self-mutation invalidates the view" \
			"[ $l_view -gt $l_prev_receive ]"
		assertTrue "view listing for [$l_dataset_suffix] must precede its own receive" \
			"[ $l_view -lt $l_receive ]"
		l_prev_receive=$l_receive
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

	# The fast no-op proof must detect the guid divergence and fall back to
	# full creation-order discovery instead of declaring a clean no-op.
	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
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

# Write a minimal GNU-parallel stand-in: skip options through "--", then run
# the single command argument once per stdin line with every {} replaced by
# the line. Sequential execution is a valid serialization of parallel's
# interleaving, so the canned-zfs fixtures stay deterministic.
planning_write_mock_parallel() {
	l_parallel_path=$1

	cat >"$l_parallel_path" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	--)
		shift
		break
		;;
	*)
		shift
		;;
	esac
done
mock_parallel_cmd=$*
mock_parallel_status=0
while IFS= read -r mock_parallel_line || [ -n "$mock_parallel_line" ]; do
	[ -n "$mock_parallel_line" ] || continue
	mock_parallel_rest=$mock_parallel_cmd
	mock_parallel_run=""
	while :; do
		case "$mock_parallel_rest" in
		*"{}"*)
			mock_parallel_run="$mock_parallel_run${mock_parallel_rest%%"{}"*}$mock_parallel_line"
			mock_parallel_rest=${mock_parallel_rest#*"{}"}
			;;
		*)
			mock_parallel_run="$mock_parallel_run$mock_parallel_rest"
			break
			;;
		esac
	done
	sh -c "$mock_parallel_run" || mock_parallel_status=$?
done
exit $mock_parallel_status
EOF
	chmod +x "$l_parallel_path"
}

# Extend a cloned fixture state with the parallel source-discovery answers a
# -j run issues: the dataset enumeration plus one depth-1 creation-ordered
# snapshot listing per source dataset.
planning_add_parallel_source_discovery_fixtures() {
	{
		printf '%s\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
		printf '%s/child1\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
		printf '%s/child2\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
	} >"$STATE_DIR/src_datasets.list" ||
		fail "Unable to write the source dataset enumeration fixture."
	printf 'list -Hr -t filesystem,volume -o name %s\tsrc_datasets.list\t0\n' \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the source dataset enumeration manifest rule."

	l_parfix_index=0
	for l_parfix_suffix in "" /child1 /child2; do
		l_parfix_dataset="$ZXFER_MOCKBIN_SOURCE_ROOT$l_parfix_suffix"
		grep "^$l_parfix_dataset@" "$STATE_DIR/src_snapshots.list" \
			>"$STATE_DIR/src_d1_$l_parfix_index.list" ||
			fail "Unable to derive the depth-1 source listing for $l_parfix_dataset."
		printf 'list -H -o name,guid -s creation -d 1 -t snapshot %s\tsrc_d1_%s.list\t0\n' \
			"$l_parfix_dataset" "$l_parfix_index" >>"$STATE_DIR/manifest" ||
			fail "Unable to append the depth-1 source manifest rule for $l_parfix_dataset."
		l_parfix_index=$((l_parfix_index + 1))
	done
}

# Invariant: a -j 2 incremental run through the supervision-lite background
# job layer completes every per-dataset receive, exits 0, and leaves neither
# job processes nor per-job control files behind. The scheduling order itself
# (destination-ancestry serialization) is pinned by the send/receive unit
# suite; this pins the externally observable outcome.
test_parallel_jobs_incremental_completes_all_receives_without_leftovers() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" paralleljobs
	planning_add_parallel_source_discovery_fixtures
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."

	job_tmp_dir="$CASE_DIR/jobtmp"
	mkdir -p "$job_tmp_dir" || fail "Unable to create the per-run temp root."
	chmod 700 "$job_tmp_dir" || fail "Unable to restrict the per-run temp root."

	TMPDIR="$job_tmp_dir" planning_run_zxfer "$STATE_DIR" -j 2 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "-j 2 incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 $?
	planning_assert_no_mutations

	for l_parjobs_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_parjobs_suffix"
	done
	assertEquals "every dataset missing the newest snapshot should be sent exactly once" \
		3 "$(grep -c '^send ' "$ZFS_LOG")"

	leftover_files=$(find "$job_tmp_dir" -mindepth 1 2>/dev/null || :)
	assertEquals "the per-run temp root must hold no leftover background-job control files after exit" \
		"" "$leftover_files"
	# shellcheck disable=SC2009  # the assertion is about the raw process table
	leftover_processes=$(ps -axo command= 2>/dev/null |
		grep -F "$MOCKBIN_DIR/zfs" | grep -v grep || :)
	assertEquals "no canned-zfs job processes may survive the run" \
		"" "$leftover_processes"
	return 0
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

# Write a mock ssh that models real control-master semantics: `-M` creates
# the `-S` socket file, `-O check` answers from socket existence, `-O exit`
# removes it, and any remaining command runs locally through `sh -c` so
# helpers resolve from the inherited mock PATH. Argv is logged to
# $MOCK_SSH_LOG one invocation per line.
planning_write_socket_mock_ssh() {
	l_ssh_path=$1

	cat >"$l_ssh_path" <<'EOF'
#!/bin/sh
[ -n "${MOCK_SSH_LOG:-}" ] && printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
mock_socket=""
mock_op=""
while [ $# -gt 0 ]; do
	case "$1" in
	-M) shift ;;
	-S)
		mock_socket=$2
		shift 2
		;;
	-O)
		mock_op=$2
		shift 2
		;;
	-o) shift 2 ;;
	-*) shift ;;
	*) break ;;
	esac
done
if [ $# -gt 0 ]; then
	shift
fi
case "$mock_op" in
check)
	if [ -e "$mock_socket" ]; then
		exit 0
	fi
	printf 'Control socket connect(%s): No such file or directory\n' "$mock_socket" >&2
	exit 255
	;;
exit)
	rm -f "$mock_socket" 2>/dev/null
	exit 0
	;;
esac
if [ -n "$mock_socket" ] && [ ! -e "$mock_socket" ]; then
	: >"$mock_socket" 2>/dev/null || exit 255
fi
[ $# -gt 0 ] || exit 0
exec sh -c "$*"
EOF
	chmod +x "$l_ssh_path"
}

# Invariant: a clean remote-origin pull no-op never opens an ssh control
# master (deferred-socket behavior) and costs exactly ONE capability probe
# round trip for the origin host. The minimal mock ssh cannot service a
# master open, so this pin also fails loudly if a master is ever attempted.
test_remote_origin_pull_noop_defers_control_socket_and_probes_once() {
	planning_setup_env
	zxfer_mockbin_write_minimal_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write minimal mock ssh."
	SSH_LOG="$CASE_DIR/ssh_pull_noop.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_pull_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O pull no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_pull_noop_status"
	assertTrue "the pull no-op must have used the mock ssh transport" \
		"[ -s '$SSH_LOG' ]"
	assertFalse "a clean pull no-op must never open an ssh control master" \
		"grep -q -- ' -M ' '$SSH_LOG'"
	assertFalse "a clean pull no-op must never multiplex over a control socket" \
		"grep -q -- ' -S ' '$SSH_LOG'"
	assertEquals "a warmed origin host must cost exactly one capability probe round trip" \
		1 "$(grep -c 'ZXFER_REMOTE_CAPS_V2' "$SSH_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: an incremental remote-origin pull opens the per-run ssh control
# master exactly ONCE, multiplexes later remote commands over that one
# socket, probes capabilities exactly once, and closes the master once at
# exit -- no per-command reconnect or per-command handshake regression.
test_remote_origin_pull_incremental_opens_master_once() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_pull_incr.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/incremental" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_pull_incr_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O pull incremental must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_pull_incr_status"
	for l_pull_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_pull_suffix"
	done
	assertEquals "an incremental pull must open the ssh control master exactly once" \
		1 "$(grep -c -- ' -M ' "$SSH_LOG")"
	assertEquals "a warmed origin host must cost exactly one capability probe round trip" \
		1 "$(grep -c 'ZXFER_REMOTE_CAPS_V2' "$SSH_LOG")"
	assertEquals "the per-run ssh control master must be closed exactly once at exit" \
		1 "$(grep -c -- ' -O exit ' "$SSH_LOG")"
	l_master_socket=$(awk '/ -M /{for (i=1;i<NF;i++) if ($i=="-S") {print $(i+1); exit}}' "$SSH_LOG")
	assertNotNull "the master open must carry a -S control socket path" "$l_master_socket"
	l_multiplexed=$(grep -c -- "-S $l_master_socket" "$SSH_LOG")
	assertTrue "remote send commands must multiplex over the one opened master socket" \
		"[ ${l_multiplexed:-0} -ge 2 ]"
	planning_assert_no_mutations
}

. "$SHUNIT2_BIN"
