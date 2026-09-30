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
#   dst snapshot listing and existence check exit non-zero without
#   "does not exist"
#       test_destination_existence_check_operational_failure_fails_closed
#       → operational error is NOT misread as a missing dataset: fail
#         closed instead of creating/sending.
#
#   dst snapshot listing exits non-zero
#       test_destination_snapshot_listing_failure_fails_closed
#       → fail closed with the zfs exit status preserved.
#
#   dst unchanged by this run, or changed by its -d destroy
#       test_only_datasets_changed_by_this_run_are_listed_again
#       → a dataset this run did not change keeps the plan made from
#         discovery: after the one recursive destination listing no
#         destination snapshot listing runs. A dataset whose snapshots the
#         run destroyed is listed at depth 1 after its destroy and before
#         its rollback, and once more by the post-receive check.
#
#   same snapshot NAME on dst, different guid (divergence contract, 2026-06)
#       test_same_name_divergent_guid_fails_closed_without_d_and_f
#       → without BOTH -d and -F the diverged dataset fails closed with a
#         structured error and ZERO mutating/send argv; -d alone and -F
#         alone fail the same way.
#       test_divergence_with_d_and_f_warns_and_converges
#       → with -d -F the always-on stderr warning names the dataset, the
#         count, and both guids, then converges: destroy the diverged
#         destination snapshot, rollback to the last guid-matching common
#         snapshot, resend; post-receive verification passes once the live
#         listing heals ('once' manifest rules).
#       test_divergence_still_present_after_receive_fails_closed
#       → if the post-receive live listing STILL shows a name-match/guid-
#         mismatch snapshot, the run aborts with a structured "re-diverged
#         after convergence" error naming the snapshot.
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
#   dst-only snapshot newer than the anchor, -d live with sends pending
#       test_delete_without_force_never_rolls_back_before_a_send
#       → the destroy is the only mutation and every dataset is sent from
#         its anchor; the same run with -F also rolls the root back to its
#         anchor before the root's send.
#
#   property pass operator contract (-P / -o / -I / -U, pinned 2026-09)
#       test_property_pass_sets_differing_property_and_skips_identical_one
#       → a differing settable property is `MUTATE set` per dataset, an
#         identical one and a read-only one are never touched.
#       test_override_option_sets_override_value_and_children_inherit_it
#       → -o sets the override value on the root and children `inherit` it.
#       test_ignore_option_never_sets_ignored_property_even_when_it_differs
#       → -I keeps the ignored property out of every set.
#       test_property_local_on_destination_but_inherited_on_source_is_inherited
#       → local-on-destination / inherited-on-source becomes `inherit`.
#       test_skip_unsupported_option_skips_property_destination_does_not_support
#       → -U drops a property the destination reports as unknown; without -U
#         the same property is set.
#       test_missing_destination_child_is_created_with_creation_properties_before_receive
#       → a missing destination child is `create`d with its local and
#         creation-time properties (read-only ones dropped) before receive.
#       test_property_read_failure_fails_closed_without_mutations
#       → a failed source property read exits non-zero with a failure
#         report and zero MUTATE lines.
#
#   operator flags without another host-safe pin (pinned 2026-09)
#       test_nonrecursive_option_replicates_only_the_named_dataset
#       → -N sends and receives the named dataset only.
#       test_snapshot_option_creates_recursive_snapshot_before_sending
#       → -s takes one recursive zxfer_<pid>_<timestamp> snapshot before the
#         first send.
#       test_snapshot_option_fails_closed_when_date_fails
#       → -s stops before any snapshot, send or receive when date cannot
#         stamp the snapshot name.
#       test_migrate_option_fails_closed_before_unmounting_when_date_fails
#       → -m stops before it unmounts anything when date cannot stamp the
#         snapshot name.
#       test_raw_send_option_adds_w_to_every_send
#       → -w adds -w to every send.
#       test_exclude_option_skips_matching_child
#       → -x never sends or receives a matching dataset.
#       test_force_option_adds_F_to_every_receive
#       → -F adds -F to every receive.
#       test_yield_option_repeats_passes_until_limit
#       → -Y repeats a pass that did work up to the documented 8 passes.
#       test_guidless_source_row_fails_its_dataset_plan_closed
#       → a source row without a guid stops the run with exit 3 and a report
#         naming the dataset and failure_stage: replication, even after an
#         earlier dataset's send; nothing is received into it.
#       test_replan_failure_after_a_property_pass_reports_the_replication_stage
#       → a snapshot step after a dataset's -P pass (here its re-plan after a
#         -d destroy) reports failure_stage: replication.
#       test_yield_passes_plan_from_their_own_discovery
#       → each -Y pass plans from its own discovery: a second pass that finds
#         one dataset still behind sends that dataset alone.
#       test_yield_repeats_a_pass_whose_only_change_is_a_destroy
#       → a pass whose only change is a -d destroy counts as work for -Y.
#       test_grandfather_option_refuses_to_destroy_old_snapshot
#       → -d -g refuses to destroy a snapshot older than the limit: non-zero
#         exit and zero MUTATE lines.
#       test_grandfather_prepass_refuses_before_any_send
#       → a protected delete on the last dataset stops -d -g before any
#         send, receive or MUTATE on the earlier datasets, reported as
#         failure_stage: replication.
#       test_progress_option_passes_stream_through_dialog
#       → -D runs the dialog once per send with %%size%% and %%title%%
#         expanded and hands it a copy of the stream.
#
#   -j ancestry serialization
#       test_parallel_jobs_child_receive_starts_after_parent_receive_ends
#       → no child receive starts before the parent receive's END line.
#
#   interruption
#       test_parallel_jobs_term_tears_down_jobs_and_removes_run_tmp_root
#       → a TERM during a -j run exits 143 with one structured report,
#         leaving no job process or temp file behind.
#
#   snapshot discovery (no-op proof, listings, -x, -j, -Z, temp paths)
#       test_proof_source_listing_that_fails_after_the_destination_rows_never_proves_a_noop
#       → a proof listing that dies after the destination's rows fails
#         closed with its stderr and status; over -O -z the sentinel exposes
#         the truncated stream and full discovery sends every dataset.
#       test_proof_destination_listing_that_fails_after_its_rows_fails_closed_unless_the_root_is_missing
#       → the same on the destination side, unless it reports the root
#         missing, which declines the proof.
#       test_empty_source_listing_fails_closed_before_any_change
#       → an empty but successful source listing fails, proof or full.
#       test_exclude_option_proves_a_noop_when_only_excluded_datasets_differ
#       → -x differences are a proven no-op (no parallel, no pattern on the
#         origin); an all-excluded source falls back and finds no work.
#       test_invalid_exclude_pattern_fails_closed_before_any_change
#       → an exclude pattern awk rejects fails -R and -N runs closed.
#       test_parallel_jobs_dataset_enumeration_that_fails_partway_fails_closed
#       → a -j enumeration that fails partway exits 70 with no change.
#       test_parallel_jobs_very_verbose_reports_the_delta_profile_and_job_count
#       → -j -V reports the exact delta and listing counters, and parallel
#         gets -j N --line-buffer and the bare zfs runner.
#       test_remote_origin_compressed_parallel_discovery_replicates_every_dataset
#       → -O -Z -j compresses every origin stream with -Z's command and
#         decompresses it locally; the origin runs parallel with -j N.
#       test_destination_inventory_failures_fail_closed_with_the_listing_diagnostic
#       → a failed or empty inventory, or a missing root whose pool cannot
#         be listed, fails closed with the listing's diagnostic.
#       test_nonrecursive_destination_listing_diagnostics_are_passed_on
#       → full discovery passes listing warnings on, and a listing failure
#         is classified by an exact root probe, local status 255 included.
#       test_trailing_slash_source_maps_onto_the_destination_itself
#       → "SRC/" lists and replicates into the destination itself.
#       test_hostile_tmpdir_is_used_literally_by_discovery_pipelines
#       → temp paths stay literal words in the rendered pipelines.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# Invariant: snapshot identity is guid-based. Both the source and the
# destination snapshot discovery listings must request "-o name,guid".
# Driven against the incremental fixture: the fast no-op proof attempts
# first (its identity listing also requests name,guid), mismatches, and the
# full creation-order discovery shape stays pinned on the fallback.
test_recursive_discovery_pins_guid_identity_listings() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "recursive run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

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

	mkdir -m 700 "$CASE_DIR/runtime" || return 1
	TMPDIR="$CASE_DIR/runtime" planning_run_zxfer "$FIXTURE_DIR/noop" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "no-op replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

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
	assertEquals "The proven no-op must remove its complete run-private scratch tree at exit." \
		"" "$(find "$CASE_DIR/runtime" ! -path "$CASE_DIR/runtime" -print)"
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
	l_run_status=$?
	assertEquals "dry run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
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
# then fails the run closed. When the listing prints a diagnostic, the
# message carries it, and an unsafe report names the failed background
# listing as its last command, not a later foreground one.
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

	: >"$ZFS_LOG"
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	(
		MOCK_FAIL_TOOL=zfs
		MOCK_FAIL_CALL=1
		MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
		MOCK_FAIL_MATCH="list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
		MOCK_FAIL_STDERR="cannot iterate filesystems: I/O error"
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
			MOCK_FAIL_STDERR ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
		planning_run_zxfer "$FIXTURE_DIR/incremental" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "a failed listing with stderr keeps its status" 1 $?
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshots from the source: cannot iterate filesystems: I/O error"
	assertTrue "the report must name the failed listing as its last command; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep '^last_command: ' '$CASE_DIR/zxfer.stderr' | grep -Fq \"'list' '-Hr' '-o' 'name,guid' '-s' 'creation' '-t' 'snapshot' '$ZXFER_MOCKBIN_SOURCE_ROOT'\""
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: an operational destination existence-check failure (non-zero
# exit WITHOUT a "dataset does not exist" diagnostic) is not misread as a
# missing dataset — zxfer fails closed instead of creating or sending.
# Discovery checks existence only after its recursive destination listing
# fails, so both are forced to fail here.
test_destination_existence_check_operational_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" dstexistfail
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2
	planning_force_manifest_failure "list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "destination existence failure should exit 1" 1 $?

	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to determine whether destination dataset [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT] exists"
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

# Invariant (2026-09): only a dataset this run changed is listed again before
# its send; every other dataset keeps the plan made from discovery, and zfs
# receive still refuses an incremental whose base is gone. The incremental
# fixture changes nothing before its sends, so after the one recursive
# destination listing (the fast no-op proof's, which discovery reuses) no
# destination snapshot listing runs. In the -d -F convergence the root is
# listed at depth 1 after its destroy and before its rollback, then once more
# by the post-receive check. Discovery log order is nondeterministic
# (background jobs), so ordering is asserted per line number, never
# positionally against the whole file.
test_only_datasets_changed_by_this_run_are_listed_again() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_no_mutations

	assertFalse "a successful destination listing needs no separate existence check" \
		"grep -qFx 'list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT' '$ZFS_LOG'"
	assertFalse "no dataset is changed before its send here, so no depth-1 listing may run" \
		"grep -q '^list -H -d 1 ' '$ZFS_LOG'"
	assertEquals "the fast no-op proof's destination listing, reused by discovery, must be the only one" \
		1 "$(grep -cFx "list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZFS_LOG")"
	l_src_discovery=$(planning_log_line_number \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT")
	assertNotNull "source discovery missing from log" "$l_src_discovery"
	l_src_discovery=${l_src_discovery:-99999}
	for l_dataset_suffix in "" /child1 /child2; do
		l_receive=$(planning_log_line_number \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_dataset_suffix")
		assertNotNull "receive missing for [$l_dataset_suffix]" "$l_receive"
		l_receive=${l_receive:-0}
		assertTrue "the receive for [$l_dataset_suffix] must follow source discovery" \
			"[ $l_src_discovery -lt $l_receive ]"
	done

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" changed_relist
	planning_make_destination_diverged_until_receive
	planning_run_zxfer "$STATE_DIR" -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "diverged -d -F run should converge and exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

	l_relist_key="list -H -d 1 -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "the changed root is listed at depth 1 twice: the pre-send re-plan and the post-receive check" \
		2 "$(grep -cFx "$l_relist_key" "$ZFS_LOG")"
	assertEquals "only the changed root may be listed at depth 1" \
		2 "$(grep -c '^list -H -d 1 ' "$ZFS_LOG")"
	l_destroy=$(planning_log_line_number "MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap3")
	l_relist=$(planning_log_line_number "$l_relist_key")
	l_rollback=$(planning_log_line_number "MUTATE rollback -r $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap2")
	assertNotNull "destroy missing from log" "$l_destroy"
	assertNotNull "depth-1 re-list missing from log" "$l_relist"
	assertNotNull "rollback missing from log" "$l_rollback"
	assertTrue "the re-list must follow the destroy that changed the root" \
		"[ ${l_destroy:-99999} -lt ${l_relist:-0} ]"
	assertTrue "the re-list must precede the rollback and send it re-plans" \
		"[ ${l_relist:-99999} -lt ${l_rollback:-0} ]"
}

# Divergence contract (2026-06): a destination snapshot that shares the source
# snapshot NAME but carries a different guid is diverged data, and acting on
# it is destructive. Without BOTH -d and -F the dataset fails closed with a
# structured error and ZERO partial actions; -d alone and -F alone fail the
# same way. Before this contract, zxfer silently treated the divergent
# snapshot as absent and replanned an incremental send over it every run.
test_same_name_divergent_guid_fails_closed_without_d_and_f() {
	planning_setup_env

	for l_guiddiv_flags in "" "-d" "-F"; do
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/noop" "guiddiv${l_guiddiv_flags#-}"
		planning_make_destination_diverged

		# shellcheck disable=SC2086  # flag word is intentionally unquoted
		planning_run_zxfer "$STATE_DIR" $l_guiddiv_flags -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		assertEquals "divergence without both -d and -F must fail closed [flags:$l_guiddiv_flags]" \
			1 $?

		# The fast no-op proof must detect the guid divergence and fall back
		# to full creation-order discovery instead of declaring a clean no-op.
		planning_assert_log_has_line \
			"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
		planning_assert_no_mutations
		planning_assert_no_send_receive
		planning_assert_failure_report "divergence reconciliation" \
			"Destination dataset [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT] has diverged from source dataset [$ZXFER_MOCKBIN_SOURCE_ROOT]"
		assertTrue "the fail-closed error should state the -d -F remediation [flags:$l_guiddiv_flags]" \
			"grep -Fq 'Re-run with BOTH -d and -F' '$CASE_DIR/zxfer.stderr'"
	done
}

# Divergence contract: with BOTH -d and -F active the run warns on stderr
# (always-on, no -v/-V needed), then converges the diverged dataset: destroy
# the name-colliding destination snapshot, roll back to the last guid-matching
# common snapshot, and resend the range over it. The post-receive verification
# re-checks the live destination listing (healed here via 'once' rules) and
# the run completes cleanly. Untouched child datasets stay no-op.
test_divergence_with_d_and_f_warns_and_converges() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" guiddiv_converge
	planning_make_destination_diverged_until_receive

	planning_run_zxfer "$STATE_DIR" -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "diverged -d -F run should converge and exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

	assertTrue "the always-on divergence warning must name the diverged dataset and count" \
		"grep -q 'WARNING: destination dataset \[$ZXFER_MOCKBIN_DEST_MAPPED_ROOT\] has 1 snapshot' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the divergence warning must show both guids for the example snapshot" \
		"grep -Fq '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap3: source guid 1000000003000000007 vs destination guid 9999900003000000007' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the divergence warning must state the convergence action" \
		"grep -Fq 'converging: destroy + rollback + resend' '$CASE_DIR/zxfer.stderr'"

	planning_assert_log_has_line \
		"MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap3"
	planning_assert_log_has_line \
		"MUTATE rollback -r $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap2"
	assertEquals "convergence performs exactly the destroy and the rollback" 2 \
		"$(grep -c '^MUTATE ' "$ZFS_LOG")"
	planning_assert_log_has_line \
		"send -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"
	planning_assert_log_has_line "receive -F $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "only the diverged dataset should be re-sent" 1 \
		"$(grep -c '^send ' "$ZFS_LOG")"
}

# Divergence contract: -V planning transparency. Each PLANNED dataset gets
# one "Last common snapshot ...; diverged destination snapshots: N." line
# (datasets proven in sync are never planned, so they get no line) and the
# profile summary carries the diverged_snapshot_warnings counter.
test_divergence_very_verbose_reports_transparency_line_and_counter() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" guiddiv_verbose
	planning_make_destination_diverged_until_receive

	planning_run_zxfer "$STATE_DIR" -V -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-V diverged -d -F run should still exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

	assertTrue "-V planning should report the diverged dataset's transparency line" \
		"grep -Fq '; diverged destination snapshots: 1.' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the -V profile summary should count the warned dataset" \
		"grep -Fq 'zxfer profile: diverged_snapshot_warnings=1' '$CASE_DIR/zxfer.stderr'"

	# Planned-but-not-diverged datasets report a zero diverged count: the
	# incremental fixture plans every dataset (each misses @snap3) and none
	# of them carries a name-match/guid-mismatch snapshot.
	planning_run_zxfer "$FIXTURE_DIR/incremental" -V -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-V incremental run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "every planned in-sync dataset should report a zero diverged count" \
		3 "$(grep -cF '; diverged destination snapshots: 0.' "$CASE_DIR/zxfer.stderr")"
	assertTrue "an unwarned run should report a zero diverged counter" \
		"grep -Fq 'zxfer profile: diverged_snapshot_warnings=0' '$CASE_DIR/zxfer.stderr'"
}

# Divergence contract: if the post-receive live listing STILL shows the
# name-match/guid-mismatch snapshot (an external writer keeps re-diverging
# the destination), the run must abort with a structured error naming the
# snapshot instead of silently looping destroy + resend forever. The
# unconditional diverged listing rule models the external writer.
test_divergence_still_present_after_receive_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" guiddiv_rediverge
	planning_make_destination_diverged

	planning_run_zxfer "$STATE_DIR" -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a still-diverged post-receive listing must abort the run" 1 $?

	# Convergence itself ran: destroy, rollback, and the resend all happened
	# before the verification caught the re-divergence.
	planning_assert_log_has_line \
		"MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap3"
	planning_assert_log_has_line \
		"MUTATE rollback -r $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap2"
	planning_assert_log_has_line "receive -F $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_failure_report "post-receive divergence verification" \
		"Destination dataset [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT] re-diverged after convergence"
	assertTrue "the re-divergence error should name the snapshot and both guids" \
		"grep -Fq '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap3: source guid 1000000003000000007 vs destination guid 9999900003000000007' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the re-divergence error should blame an external writer" \
		"grep -Fq 'An external writer is modifying the destination' '$CASE_DIR/zxfer.stderr'"
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
	l_run_status=$?
	assertEquals "-d dry run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
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
	l_run_status=$?
	assertEquals "-d live run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

	planning_assert_log_has_line \
		"MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap9"
	assertEquals "exactly one destroy and no other mutations" 1 \
		"$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "an up-to-date destination must not be sent to" \
		"grep -q '^send ' '$ZFS_LOG'"
	assertTrue "deletion planning should query candidate creation times" \
		"grep -q '^get -H -o name,value -p creation $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@' '$ZFS_LOG'"
}

# Invariant (-d without -F): destroying a destination-only snapshot newer than
# the anchor never rolls the destination back; every dataset is still sent
# incrementally from its anchor. The same run with -F rolls the root back to
# its anchor (@snap2) before the root's send, which shows that the fixture
# qualifies for a rollback and only -F is missing.
test_delete_without_force_never_rolls_back_before_a_send() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" delete_newer
	planning_add_extra_destination_snapshot
	# The destination-only @snap9 is newer than the root's anchor @snap2.
	printf '%s@snap2\t1700000002\n%s@snap9\t1700000009\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		>"$STATE_DIR/dst_creation.list" ||
		fail "Unable to write the creation-time fixture."
	l_newer_destroy="MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap9"
	l_newer_rollback="MUTATE rollback -r $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap2"
	l_newer_root_send="send -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"

	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-d run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_log_has_line "$l_newer_destroy"
	assertEquals "without -F the destroy is the only mutation" \
		1 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	for l_newer_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"send -I $ZXFER_MOCKBIN_SOURCE_ROOT$l_newer_suffix@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT$l_newer_suffix@snap3"
	done

	: >"$ZFS_LOG"
	planning_run_zxfer "$STATE_DIR" -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-d -F run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_log_has_line "$l_newer_destroy"
	planning_assert_log_has_line "$l_newer_rollback"
	assertEquals "with -F the destroy and one rollback are the only mutations" \
		2 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	l_newer_rollback_line=$(planning_log_line_number "$l_newer_rollback")
	l_newer_send_line=$(planning_log_line_number "$l_newer_root_send")
	assertTrue "the rollback must precede the root's send (rollback line ${l_newer_rollback_line:-none}, send line ${l_newer_send_line:-none})" \
		"[ '${l_newer_send_line:-0}' -gt '${l_newer_rollback_line:-0}' ] && [ '${l_newer_rollback_line:-0}' -gt 0 ]"
}

# Invariant: a -j 2 incremental run through the supervision-lite background
# job layer completes every per-dataset receive, exits 0, and leaves neither
# job processes nor per-job control files behind. The scheduling order itself
# (destination-ancestry serialization) is pinned by the send/receive unit
# suite; this pins the externally observable outcome.
test_parallel_jobs_incremental_completes_all_receives_without_leftovers() {
	planning_setup_parallel_jobs_env paralleljobs

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 2 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-j 2 incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_no_mutations

	for l_parjobs_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_parjobs_suffix"
	done
	assertEquals "every dataset missing the newest snapshot should be sent exactly once" \
		3 "$(grep -c '^send ' "$ZFS_LOG")"
	planning_assert_no_parallel_job_leftovers
	return 0
}

# Invariant: -j 3 on the incremental fixture runs every per-dataset receive
# exactly once, exits 0, and leaves no job processes or control files.
test_parallel_jobs_three_completes_every_receive_exactly_once() {
	planning_setup_parallel_jobs_env paralleljobs3

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 3 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-j 3 incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_no_mutations

	for l_parjobs3_suffix in "" /child1 /child2; do
		assertEquals "receive of $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_parjobs3_suffix must run exactly once" \
			1 "$(grep -cx "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_parjobs3_suffix" "$ZFS_LOG")"
	done
	assertEquals "every dataset missing the newest snapshot should be sent exactly once" \
		3 "$(grep -c '^send ' "$ZFS_LOG")"
	planning_assert_no_parallel_job_leftovers
	return 0
}

# Invariant: a background receive that exits non-zero under -j 2 fails the
# run with a structured runtime failure report naming the destination
# dataset, and every other job is still reaped.
test_parallel_jobs_receive_failure_reports_dataset_and_reaps_every_job() {
	planning_setup_parallel_jobs_env paralleljobsfail
	printf 'receive %s/child1\t-\t1\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append the failing receive manifest rule."

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 2 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_parjobsfail_status=$?

	assertNotEquals "a failing background receive must fail the run" \
		0 "$l_parjobsfail_status"
	planning_assert_no_mutations
	for l_parjobsfail_line in \
		"zxfer: failure report begin" \
		"zxfer: failure report end" \
		"failure_class: runtime" \
		"failure_stage: send/receive" \
		"message: zfs send/receive job failed for [" \
		"-> $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1] (PID " \
		", exit 1)."; do
		grep -Fq -- "$l_parjobsfail_line" "$CASE_DIR/zxfer.stderr" ||
			fail "Missing failure report line: $l_parjobsfail_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	planning_assert_no_parallel_job_leftovers
	return 0
}

# Invariant: TERM delivered to zxfer while -j 2 receives are in flight (each
# receive sleeps in a case-local zfs wrapper, so zxfer sits in its job poll)
# exits 128+15 with exactly one structured failure report, tears down every
# job process, and removes the per-run temp root within a bounded time.
# Regression: the handler used to take $? from the interrupted poll sleep
# and exit 0 without a report.
test_parallel_jobs_term_tears_down_jobs_and_removes_run_tmp_root() {
	planning_setup_parallel_jobs_env paralleljobsterm
	planning_delay_canned_zfs_receive '*' 30

	(
		MOCK_ZFS_LOG="$ZFS_LOG"
		MOCK_ZFS_FIXTURE_DIR="$STATE_DIR"
		ZXFER_SECURE_PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		ZXFER_SECURE_PATH_APPEND=""
		TMPDIR="$JOB_TMP_DIR"
		export MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR ZXFER_SECURE_PATH \
			ZXFER_SECURE_PATH_APPEND TMPDIR
		exec "$ZXFER_TEST_ZXFER_BIN" -j 2 -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	) >"$CASE_DIR/zxfer.stdout" 2>"$CASE_DIR/zxfer.stderr" &
	l_parjobsterm_pid=$!

	l_parjobsterm_tries=0
	while ! grep -q '^send ' "$ZFS_LOG" 2>/dev/null &&
		[ "$l_parjobsterm_tries" -lt 200 ]; do
		l_parjobsterm_tries=$((l_parjobsterm_tries + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	assertTrue "the first send must start before the interruption; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep -q '^send ' '$ZFS_LOG'"
	assertNotEquals "the run must still be in flight when TERM is delivered" \
		"" "$(find "$JOB_TMP_DIR" -mindepth 1 -maxdepth 1 2>/dev/null)"

	kill -s TERM "$l_parjobsterm_pid" 2>/dev/null
	l_parjobsterm_tries=0
	while kill -s 0 "$l_parjobsterm_pid" 2>/dev/null &&
		[ "$l_parjobsterm_tries" -lt 150 ]; do
		l_parjobsterm_tries=$((l_parjobsterm_tries + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	assertFalse "zxfer must exit within the bounded window after TERM" \
		"kill -s 0 '$l_parjobsterm_pid' 2>/dev/null"
	wait "$l_parjobsterm_pid" 2>/dev/null
	assertEquals "a TERM must exit with status 128+15" 143 $?
	assertEquals "a TERM must emit exactly one structured failure report" \
		1 "$(grep -c '^zxfer: failure report begin$' "$CASE_DIR/zxfer.stderr")"
	planning_assert_failure_report signal \
		"message: zxfer was interrupted by a signal (exit status 143)."

	l_parjobsterm_tries=0
	# shellcheck disable=SC2009  # the assertion is about the raw process table
	while ps -axo command= 2>/dev/null | grep -F "$MOCKBIN_DIR/zfs" |
		grep -qv grep && [ "$l_parjobsterm_tries" -lt 50 ]; do
		l_parjobsterm_tries=$((l_parjobsterm_tries + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	planning_assert_no_parallel_job_leftovers
	return 0
}

# Invariant: with -j 3, ancestor/descendant destinations are serialized: no
# child receive starts until the parent receive (dstpool/back/data) has
# ended. The parent receive is held open for a second, so a child launched
# alongside it would log its receive before the parent's END line. Both the
# replication ready queue and the send-job scheduler defer a conflicting
# child, so this fails only when neither does.
test_parallel_jobs_child_receive_starts_after_parent_receive_ends() {
	planning_setup_parallel_jobs_env paralleljobsorder
	planning_delay_canned_zfs_receive "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 1

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 3 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-j 3 incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

	l_parjobsorder_parent_end=$(planning_log_line_number \
		"END receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	assertNotNull "the parent receive must end in the zfs log; zfs log: $(cat "$ZFS_LOG")" \
		"$l_parjobsorder_parent_end"
	for l_parjobsorder_suffix in /child1 /child2; do
		l_parjobsorder_child=$(planning_log_line_number \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_parjobsorder_suffix")
		assertNotNull "the child receive must appear in the zfs log" \
			"$l_parjobsorder_child"
		assertTrue "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_parjobsorder_suffix must not start receiving before the parent receive ends; zfs log: $(cat "$ZFS_LOG")" \
			"[ '${l_parjobsorder_parent_end:-99999}' -lt '${l_parjobsorder_child:-0}' ]"
	done
	planning_assert_no_parallel_job_leftovers
	return 0
}

# Invariant (-j names with spaces): the per-dataset -j source listing hands a
# dataset name containing a space to zfs as ONE argument, locally and over
# -O, and every dataset is replicated. Regression: the runner quoted the
# {} placeholder, so GNU parallel's own quoting of the name cancelled out and
# split it in two (the mock parallel quotes like GNU parallel).
test_parallel_jobs_keep_dataset_names_with_spaces_whole_locally_and_over_origin() {
	planning_use_fixture_roots "srcpool/my data" "$ZXFER_MOCKBIN_DEST_ROOT" \
		"$ZXFER_MOCKBIN_DEST_ROOT/my data"
	planning_setup_parallel_jobs_env parallelspaces
	planning_log_canned_zfs_argv
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."

	for l_spaces_origin in "" localhost; do
		: >"$ZFS_LOG"
		: >"$ARGV_LOG"
		set -- -j 2 -R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		[ -z "$l_spaces_origin" ] || set -- -O "$l_spaces_origin" "$@"
		TMPDIR="$JOB_TMP_DIR" PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
			planning_run_zxfer "$STATE_DIR" "$@"
		l_run_status=$?
		assertEquals "-j 2 over names with spaces should exit 0 [origin:$l_spaces_origin]; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_run_status"
		for l_spaces_suffix in "" /child1 /child2; do
			assertTrue "the depth-1 listing must pass $ZXFER_MOCKBIN_SOURCE_ROOT$l_spaces_suffix as one argument [origin:$l_spaces_origin]; argv: $(cat "$ARGV_LOG")" \
				"grep -Fxq '[list] [-H] [-o] [name,guid] [-s] [creation] [-d] [1] [-t] [snapshot] [$ZXFER_MOCKBIN_SOURCE_ROOT$l_spaces_suffix] ' '$ARGV_LOG'"
			planning_assert_log_has_line \
				"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_spaces_suffix"
		done
		assertFalse "no zfs call may see the name split at its space [origin:$l_spaces_origin]" \
			"grep -Fq '[srcpool/my] ' '$ARGV_LOG'"
		planning_assert_no_mutations
	done
	planning_assert_no_parallel_job_leftovers
	return 0
}

# Invariant (-P, differing plain property): a settable property whose source
# and destination values differ is `zfs set` on every dataset that holds it
# locally, an identical property is never touched, and a read-only property
# (mountpoint) is never set even when it differs.
test_property_pass_sets_differing_property_and_skips_identical_one() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prop_set
	l_src_rows=$(planning_property_default_rows)
	l_dst_rows=$(planning_property_rows_with "$l_src_rows" compression gzip local)
	l_dst_rows=$(planning_property_rows_with "$l_dst_rows" mountpoint /mnt/elsewhere local)
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_rows" "$l_dst_rows"

	planning_run_property_pass -P
	for l_prop_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set compression=lz4 $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prop_suffix"
	done
	assertEquals "exactly one set per dataset and nothing else mutates" \
		3 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "an identical property must never be set" \
		"grep -q 'atime=' '$ZFS_LOG'"
	assertFalse "a read-only property must never be set" \
		"grep -q 'mountpoint=' '$ZFS_LOG'"
	planning_assert_no_send_receive
}

# Invariant (-o override): the override value replaces the source value on
# the root dataset (`zfs set`), and existing children converge on the parent
# by `zfs inherit` rather than a local set; the source value never wins.
test_override_option_sets_override_value_and_children_inherit_it() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prop_override
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_rows" "$l_rows" "$l_rows" "$l_rows"

	planning_run_property_pass -o compression=gzip
	planning_assert_log_has_line \
		"MUTATE set compression=gzip $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	for l_prop_suffix in /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE inherit compression $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prop_suffix"
	done
	assertEquals "one root set plus one inherit per child" \
		3 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "the source value must never be applied under -o" \
		"grep -q 'compression=lz4' '$ZFS_LOG'"
	planning_assert_no_send_receive
}

# Invariant (-I ignore): an ignored property is never set even when it
# differs, while other differing properties are still reconciled.
test_ignore_option_never_sets_ignored_property_even_when_it_differs() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prop_ignore
	l_src_rows=$(planning_property_default_rows)
	l_dst_rows=$(planning_property_rows_with "$l_src_rows" compression gzip local)
	l_dst_rows=$(planning_property_rows_with "$l_dst_rows" atime on local)
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_rows" "$l_dst_rows"

	planning_run_property_pass -P -I compression
	for l_prop_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set atime=off $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prop_suffix"
	done
	assertEquals "only the non-ignored differing property is set" \
		3 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "an ignored property must never be set" \
		"grep -q 'compression=' '$ZFS_LOG'"
	planning_assert_no_send_receive
}

# Invariant (-P, inherited on source / local on destination): when the
# source child inherits a property from its parent but the destination child
# holds the same value locally, the destination child is `zfs inherit`ed so
# its source matches, and nothing is set.
test_property_local_on_destination_but_inherited_on_source_is_inherited() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prop_inherit
	l_rows=$(planning_property_default_rows)
	l_src_child_rows=$(planning_property_rows_with "$l_rows" compression lz4 \
		"inherited from $ZXFER_MOCKBIN_SOURCE_ROOT")
	planning_add_property_fixtures_for_rows "$l_rows" "$l_src_child_rows" \
		"$l_rows" "$l_rows"

	planning_run_property_pass -P
	for l_prop_suffix in /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE inherit compression $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prop_suffix"
	done
	assertEquals "one inherit per child and no set" \
		2 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "no property may be set when only the source differs" \
		"grep -q '^MUTATE set' '$ZFS_LOG'"
	planning_assert_no_send_receive
}

# Invariant (-U): a source property the destination reports as unknown is
# skipped by the property pass, while the same property is set without -U.
test_skip_unsupported_option_skips_property_destination_does_not_support() {
	planning_setup_env
	l_dst_rows=$(planning_property_default_rows)
	l_src_rows=$(planning_property_rows_with "$l_dst_rows" overlay on local)

	planning_clone_state "$FIXTURE_DIR/noop" prop_without_u
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_rows" "$l_dst_rows"
	planning_add_unsupported_property_fixtures "$l_src_rows" "$l_dst_rows"
	planning_run_property_pass -P
	planning_assert_log_has_line \
		"MUTATE set overlay=on $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "without -U the unsupported property is set on every dataset" \
		3 "$(grep -c 'overlay=' "$ZFS_LOG")"

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" prop_with_u
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_rows" "$l_dst_rows"
	planning_add_unsupported_property_fixtures "$l_src_rows" "$l_dst_rows"
	planning_run_property_pass -U -P
	assertFalse "with -U the destination-unsupported property must never be set" \
		"grep -q 'overlay=' '$ZFS_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant (-P, missing destination child): a source child whose destination
# does not exist is `zfs create`d with its local and creation-time
# properties (read-only ones dropped) before anything is received into it.
test_missing_destination_child_is_created_with_creation_properties_before_receive() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prop_create
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_rows" "$l_rows" "$l_rows" "$l_rows"
	l_missing_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	for l_create_fixture in dst_datasets.list dst_snapshots.list dst_props_tree.list; do
		grep -v "^$l_missing_child" "$STATE_DIR/$l_create_fixture" \
			>"$STATE_DIR/$l_create_fixture.new" || :
		mv "$STATE_DIR/$l_create_fixture.new" "$STATE_DIR/$l_create_fixture" ||
			fail "Unable to drop $l_missing_child from $l_create_fixture."
	done
	: >"$STATE_DIR/dst_d1_2.list"
	printf "cannot open '%s': dataset does not exist\n" "$l_missing_child" \
		>"$STATE_DIR/missing_child2.list"
	printf 'list -H %s\tmissing_child2.list\t1\n' "$l_missing_child" \
		>>"$STATE_DIR/manifest" || fail "Unable to append missing-child rule."

	planning_run_property_pass -P
	l_create_line="MUTATE create -o compression=lz4 -o readonly=off -o atime=off -o casesensitivity=sensitive -o normalization=none -o utf8only=off $l_missing_child"
	planning_assert_log_has_line "$l_create_line"
	assertEquals "exactly one create and no set/inherit for identical datasets" \
		1 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	l_create_at=$(planning_log_line_number "$l_create_line")
	l_receive_at=$(planning_log_line_number "receive $l_missing_child")
	assertNotNull "the created child must be received into; zfs log: $(cat "$ZFS_LOG")" \
		"$l_receive_at"
	assertTrue "the create must precede the first receive into the child" \
		"[ '$l_create_at' -lt '$l_receive_at' ]"
}

# Invariant (fail closed): when the source property read fails, the run
# exits non-zero with a structured failure report and performs zero
# mutations even though a differing destination property would otherwise
# have been set.
test_property_read_failure_fails_closed_without_mutations() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prop_read_failure
	l_src_rows=$(planning_property_default_rows)
	l_dst_rows=$(planning_property_rows_with "$l_src_rows" compression gzip local)
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_rows" "$l_dst_rows"
	planning_force_manifest_failure \
		"get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" 1
	planning_force_manifest_failure \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" 1

	planning_run_zxfer "$STATE_DIR" -P -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "a failed source property read must fail the run" 0 $?
	planning_assert_failure_report "property transfer" \
		"Failed to retrieve source properties for [$ZXFER_MOCKBIN_SOURCE_ROOT]."
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: a snapshot row without a guid fails its dataset's plan closed
# (the planner's exit 3) with a structured report naming the dataset, and
# nothing is received into that dataset. The row reaches the planner through
# the dataset's slice of the source record file. The report names the
# replication stage, not the root's earlier send (fixed 2026-09).
test_guidless_source_row_fails_its_dataset_plan_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" guidless_row
	awk -F'\t' -v row="$ZXFER_MOCKBIN_SOURCE_ROOT/child1@snap3" \
		'$1 == row { print $1; next } { print }' \
		"$FIXTURE_DIR/incremental/src_snapshots.list" >"$STATE_DIR/src_snapshots.list" ||
		fail "Unable to strip the guid from one source row."

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "a guid-less source row must stop the run with the planner's status" \
		3 "$l_run_status"
	assertTrue "the report must name the dataset whose plan failed" \
		"grep -Fq 'message: Failed to determine the last common snapshot for [$ZXFER_MOCKBIN_SOURCE_ROOT/child1] and [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1].' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the failure must be a structured runtime report" \
		"grep -Fq 'failure_class: runtime' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the report must name the planning stage, not the root's earlier send; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep -Fqx 'failure_stage: replication' '$CASE_DIR/zxfer.stderr'"
	assertFalse "nothing may be received into the dataset with the guid-less row" \
		"grep -q '^receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1\$' '$ZFS_LOG'"
	planning_assert_no_mutations
}

# Invariant (failure stage, 2026-09): the snapshot steps that follow a
# dataset's property pass (its pre-send re-plan, the rollback and the seed
# decision) report failure_stage: replication, not the property transfer that
# just finished. The root's -d destroy makes it re-plan from a depth-1
# listing before its send, and that listing fails here.
test_replan_failure_after_a_property_pass_reports_the_replication_stage() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" stage_after_properties
	planning_make_destination_diverged
	planning_add_property_transfer_fixtures
	planning_force_manifest_failure \
		"list -H -d 1 -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2

	planning_run_zxfer "$STATE_DIR" -d -F -P -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a failed re-plan listing must stop the run" 1 $?
	planning_assert_log_has_line \
		"MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap3"
	planning_assert_failure_report replication \
		"Failed to retrieve live destination snapshots for [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT]"
	planning_assert_no_send_receive
}

# Invariant: snapshot names are matched exactly, never as prefixes. With ten
# snapshots per dataset the names snap1 and snap10 coexist; the destination
# holds snap1..snap9, so the last common snapshot must be snap9 and every
# dataset must send exactly the snap9 -> snap10 increment. A prefix match
# (snap1 ~ snap10) would pick snap1 as the common base or plan a wrong tail.
test_prefix_snapshot_names_never_match_as_prefixes() {
	planning_setup_env
	PREFIX_FIXTURE_DIR="$CASE_DIR/fixtures10"
	zxfer_mockbin_build_fixture_tree "$PREFIX_FIXTURE_DIR" 2 10 ||
		fail "Unable to build the ten-snapshot fixture tree."

	planning_run_zxfer "$PREFIX_FIXTURE_DIR/incremental" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "ten-snapshot incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_no_mutations
	for l_prefix_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"send -I $ZXFER_MOCKBIN_SOURCE_ROOT$l_prefix_suffix@snap9 $ZXFER_MOCKBIN_SOURCE_ROOT$l_prefix_suffix@snap10"
	done
	assertEquals "every dataset sends exactly one snap9 -> snap10 increment" \
		3 "$(grep -c '^send -I ' "$ZFS_LOG")"
	assertFalse "snap1 must never be chosen as the incremental base for snap10" \
		"grep -q '^send -I [^ ]*@snap1 ' '$ZFS_LOG'"
}

# ---------------------------------------------------------------------------
# Operator flags without another host-safe pin. Each runs the incremental
# fixture (every dataset misses @snap3) unless stated otherwise.

# Invariant (-N): a non-recursive run replicates only the named dataset:
# exactly one send and one receive, both for the root.
test_nonrecursive_option_replicates_only_the_named_dataset() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-N run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	planning_assert_log_has_line \
		"send -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "-N must send only the named dataset" \
		1 "$(grep -c '^send ' "$ZFS_LOG")"
	assertEquals "-N must receive only the named dataset" \
		1 "$(grep -c '^receive ' "$ZFS_LOG")"
	planning_assert_no_mutations
}

# Invariant (-s without -m): zxfer takes one recursive snapshot of the source
# root, named zxfer_<pid>_<YYYYmmddHHMMSS>, before the first send.
test_snapshot_option_creates_recursive_snapshot_before_sending() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -s -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-s run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	l_snapshot_at=$(awk -v prefix="MUTATE snapshot -r $ZXFER_MOCKBIN_SOURCE_ROOT@zxfer_" \
		'index($0, prefix) == 1 { print NR; exit }' "$ZFS_LOG")
	l_first_send_at=$(awk '/^send / { print NR; exit }' "$ZFS_LOG")
	assertNotNull "-s must snapshot the source root recursively; zfs log: $(cat "$ZFS_LOG")" \
		"$l_snapshot_at"
	assertNotNull "-s must still send; zfs log: $(cat "$ZFS_LOG")" "$l_first_send_at"
	assertTrue "the snapshot must be taken before the first send" \
		"[ '${l_snapshot_at:-99999}' -lt '${l_first_send_at:-0}' ]"
	assertEquals "the snapshot is the only mutation and follows the naming scheme" \
		1 "$(grep -Ec "^MUTATE snapshot -r $ZXFER_MOCKBIN_SOURCE_ROOT@zxfer_[0-9]+_[0-9]{14}\$" "$ZFS_LOG")"
	assertEquals "-s must not mutate anything else" 1 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
}

# Purpose: Put a date in MOCKBIN_DIR that fails only for the -s/-m snapshot
# name format.
# Usage: planning_write_failing_snapshot_date
planning_write_failing_snapshot_date() {
	l_real_date=$(zxfer_mockbin_resolve_host_tool date) ||
		fail "Unable to resolve the host date."
	cat >"$MOCKBIN_DIR/date" <<EOF
#!/bin/sh
[ "\${1:-}" != "+%Y%m%d%H%M%S" ] || exit 1
exec '$l_real_date' "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/date"
}

# Invariant (-s, fail closed): when date cannot stamp the snapshot name, the
# run stops before any snapshot, send or receive instead of using a partial
# zxfer_<pid>_ name.
test_snapshot_option_fails_closed_when_date_fails() {
	planning_setup_env
	planning_write_failing_snapshot_date

	planning_run_zxfer "$FIXTURE_DIR/incremental" -s -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "-s must fail when the snapshot name cannot be stamped" 0 $?
	grep -q "Failed to read the date for the -s/-m snapshot name." "$CASE_DIR/zxfer.stderr" ||
		fail "expected the date failure; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant (-m, fail closed): when date cannot stamp the snapshot name, -m
# stops before it unmounts any source dataset.
test_migrate_option_fails_closed_before_unmounting_when_date_fails() {
	planning_setup_env
	planning_write_failing_snapshot_date
	planning_clone_state "$FIXTURE_DIR/incremental" migrate_date
	printf 'yes\n' >"$STATE_DIR/mounted_yes.list"
	printf '%s\t%s\t0\n' "get -Ho value mounted *" mounted_yes.list \
		"unmount *" - >>"$STATE_DIR/manifest" ||
		fail "Unable to append the -m manifest rules."

	planning_run_zxfer "$STATE_DIR" -m -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "-m must fail when the snapshot name cannot be stamped" 0 $?
	grep -q "Failed to read the date for the -s/-m snapshot name." "$CASE_DIR/zxfer.stderr" ||
		fail "expected the date failure; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertFalse "-m must not unmount before the name is stamped; zfs log: $(cat "$ZFS_LOG" 2>/dev/null)" \
		"grep -q '^unmount ' '$ZFS_LOG' 2>/dev/null"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant (-w): every send is a raw send.
test_raw_send_option_adds_w_to_every_send() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -w -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-w run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	for l_raw_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"send -w -I $ZXFER_MOCKBIN_SOURCE_ROOT$l_raw_suffix@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT$l_raw_suffix@snap3"
	done
	assertEquals "-w must not leave any non-raw send" \
		3 "$(grep -c '^send ' "$ZFS_LOG")"
}

# Invariant (-x): a dataset matching the exclude pattern is never sent to or
# received into; the others replicate normally. A pattern that starts with
# "-" is still a pattern, never a grep option. (It holds no "h": the
# launcher's early -h scan reads such an argument as an option cluster.)
test_exclude_option_skips_matching_child() {
	planning_setup_env

	for l_exclude_pattern in child1 '-*ild1$'; do
		: >"$ZFS_LOG"
		planning_run_zxfer "$FIXTURE_DIR/incremental" -x "$l_exclude_pattern" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		l_run_status=$?
		assertEquals "-x run should exit 0 [pattern:$l_exclude_pattern]; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_run_status"

		for l_exclude_suffix in "" /child2; do
			planning_assert_log_has_line \
				"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_exclude_suffix"
		done
		assertEquals "the excluded child must never be sent or received [pattern:$l_exclude_pattern]" \
			0 "$(grep -E '^(send|receive) ' "$ZFS_LOG" | grep -c 'child1')"
		assertEquals "the two remaining datasets are each sent once [pattern:$l_exclude_pattern]" \
			2 "$(grep -c '^send ' "$ZFS_LOG")"
		planning_assert_no_mutations
	done
}

# Invariant (-F): every receive forces a rollback of the destination.
test_force_option_adds_F_to_every_receive() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-F run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	for l_force_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive -F $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_force_suffix"
	done
	assertEquals "-F must not leave any receive without -F" \
		3 "$(grep -c '^receive ' "$ZFS_LOG")"
}

# Invariant (-Y): a pass that did work is repeated until the documented limit
# of 8 passes; without -Y there is one pass. The canned destination never
# records a receive, so every pass finds the same dataset to send.
test_yield_option_repeats_passes_until_limit() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-N run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "without -Y one pass sends once" 1 "$(grep -c '^send ' "$ZFS_LOG")"

	: >"$ZFS_LOG"
	planning_run_zxfer "$FIXTURE_DIR/incremental" -Y -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-Y -N run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "-Y repeats the pass up to its limit of 8" \
		8 "$(grep -c '^send ' "$ZFS_LOG")"
}

# Invariant (-Y, 2026-09): every pass plans from its own discovery and
# slices. Pass 1 sees every dataset miss @snap3; pass 2's discovery sees only
# child2 miss it, so pass 2 sends child2 alone; pass 3 is in sync and ends
# the loop. Two consumable rules answer the destination listing (one per pass,
# the fast no-op proof's, which discovery reuses) before the in-sync fixture.
test_yield_passes_plan_from_their_own_discovery() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" yield_rediscovery
	cp "$FIXTURE_DIR/incremental/dst_snapshots.list" "$STATE_DIR/dst_pass1.list" ||
		fail "Unable to stage the first pass's destination listing."
	grep -v "/child2@snap3" "$FIXTURE_DIR/noop/dst_snapshots.list" \
		>"$STATE_DIR/dst_pass2.list" ||
		fail "Unable to stage the second pass's destination listing."
	awk -F'\t' \
		-v key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" '
		BEGIN { OFS = "\t" }
		$1 == key {
			print key, "dst_pass1.list", 0, "once"
			print key, "dst_pass2.list", 0, "once"
		}
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the per-pass listing rules."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the per-pass listing rules."

	planning_run_zxfer "$STATE_DIR" -Y -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-Y -R run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_no_mutations

	assertEquals "pass 1 sends every dataset and pass 2 only child2" \
		4 "$(grep -c '^send ' "$ZFS_LOG")"
	assertEquals "child2 is sent in both passes" 2 "$(grep -cFx \
		"send -I $ZXFER_MOCKBIN_SOURCE_ROOT/child2@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT/child2@snap3" \
		"$ZFS_LOG")"
	for l_yield_suffix in "" /child1; do
		assertEquals "[$l_yield_suffix] is sent in pass 1 only" 1 "$(grep -cFx \
			"send -I $ZXFER_MOCKBIN_SOURCE_ROOT$l_yield_suffix@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT$l_yield_suffix@snap3" \
			"$ZFS_LOG")"
	done
	assertEquals "each pass lists the destination once, and pass 3 ends the loop" \
		3 "$(grep -cFx "list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZFS_LOG")"
}

# Invariant (-Y, -d): a pass whose only change is a -d destroy did work, so -Y
# repeats it. The canned destination never records the destroy, so every pass
# destroys the same extra snapshot: once without -Y, eight times (the limit)
# with it, and nothing is ever sent.
test_yield_repeats_a_pass_whose_only_change_is_a_destroy() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" yield_destroy_only
	planning_add_extra_destination_snapshot
	l_yield_destroy="MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap9"

	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-d run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "without -Y the pass destroys once" \
		1 "$(grep -cFx "$l_yield_destroy" "$ZFS_LOG")"

	: >"$ZFS_LOG"
	planning_run_zxfer "$STATE_DIR" -Y -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-Y -d run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "-Y repeats a destroy-only pass up to its limit of 8" \
		8 "$(grep -cFx "$l_yield_destroy" "$ZFS_LOG")"
	assertEquals "the destroy is the only mutation" \
		8 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	planning_assert_no_send_receive
}

# Invariant (-g): with -d, a destination-only snapshot older than the -g
# limit is never destroyed; the run fails and names grandfather protection.
# The extra @snap9 was created in Nov 2023, far beyond 30 days. The -d -g
# pre-pass refuses it before any change.
test_grandfather_option_refuses_to_destroy_old_snapshot() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" grandfather
	planning_add_extra_destination_snapshot

	planning_run_zxfer "$STATE_DIR" -d -g 30 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "-d -g must fail rather than destroy a grandfathered snapshot" 0 $?

	assertTrue "stderr must name grandfather protection; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep -qi 'grandfather' '$CASE_DIR/zxfer.stderr'"
	planning_assert_no_mutations
}

# Invariant (-d -g): the grandfather pre-pass plans every dataset before the
# first change, so a protected delete on child2 (the last dataset) stops the
# run before the root and child1 are sent.
test_grandfather_prepass_refuses_before_any_send() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" grandfather_prepass
	l_grandfather_child2="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	for l_grandfather_fixture in dst_snapshots.list dst_d1_2.list; do
		printf '%s@snap9\t9999900209000000007\n' "$l_grandfather_child2" \
			>>"$STATE_DIR/$l_grandfather_fixture" ||
			fail "Unable to append the old child2 snapshot to $l_grandfather_fixture."
	done
	printf '%s@snap2\t1700000002\n%s@snap9\t1700000009\n' \
		"$l_grandfather_child2" "$l_grandfather_child2" \
		>"$STATE_DIR/dst_child2_creation.list" ||
		fail "Unable to write the child2 creation-time fixture."
	printf 'get -H -o name,value -p creation %s@*\tdst_child2_creation.list\t0\n' \
		"$l_grandfather_child2" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the child2 creation-time manifest rule."

	planning_run_zxfer "$STATE_DIR" -d -g 30 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "-d -g must refuse a protected child2 delete" 0 $?

	assertTrue "stderr must name grandfather protection; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep -qi 'grandfather' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the refusal must report the replication stage; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep -Fqx 'failure_stage: replication' '$CASE_DIR/zxfer.stderr'"
	planning_assert_no_send_receive
	planning_assert_no_mutations
}

# Invariant (-F -g without -d): the -g pre-pass enforces the divergence
# contract for every dataset before the first change, so a diverged child2
# stops the run before the root's `receive -F` could destroy its
# destination-only @snap9. Regression: without -d the pre-pass was skipped.
test_grandfather_prepass_refuses_divergence_before_a_forced_receive_without_d() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" grandfather_diverged
	for l_grandfather_fixture in dst_snapshots.list dst_d1_0.list; do
		printf '%s@snap9\t9999900009000000007\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
			>>"$STATE_DIR/$l_grandfather_fixture" ||
			fail "Unable to append the destination-only root snapshot to $l_grandfather_fixture."
	done
	for l_grandfather_fixture in dst_snapshots.list dst_d1_2.list; do
		awk -F'\t' -v name="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2@snap2" '
			BEGIN { OFS = "\t" }
			$1 == name { $2 = "9999900202000000007" }
			{ print }
		' "$STATE_DIR/$l_grandfather_fixture" >"$STATE_DIR/$l_grandfather_fixture.new" ||
			fail "Unable to diverge child2@snap2 in $l_grandfather_fixture."
		mv "$STATE_DIR/$l_grandfather_fixture.new" "$STATE_DIR/$l_grandfather_fixture" ||
			fail "Unable to install the diverged $l_grandfather_fixture."
	done

	planning_run_zxfer "$STATE_DIR" -F -g 30 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "-F -g must refuse the diverged child2" 0 $?

	assertTrue "stderr must name the diverged child2; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		"grep -Fq 'Destination dataset [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2] has diverged' '$CASE_DIR/zxfer.stderr'"
	planning_assert_no_send_receive
	planning_assert_no_mutations
}

# Invariant (-D): each send pipes a copy of its stream into the progress
# dialog, started once per send with %%size%% replaced by the `send -nPv`
# estimate and %%title%% by the snapshot. -j 1 selects the exact estimate.
test_progress_option_passes_stream_through_dialog() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" progress
	printf 'size\t4096\n' >"$STATE_DIR/send_estimate.list"
	printf 'send -nPv *\tsend_estimate.list\t0\n' >>"$STATE_DIR/manifest" ||
		fail "Unable to append the send estimate manifest rule."
	cat >"$MOCKBIN_DIR/progress_dialog" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$CASE_DIR/dialog.argv"
cat >>"$CASE_DIR/dialog.stream"
EOF
	chmod +x "$MOCKBIN_DIR/progress_dialog"

	planning_run_zxfer "$STATE_DIR" -j 1 \
		-D "$MOCKBIN_DIR/progress_dialog -s %%size%% -t %%title%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-D run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	for l_progress_suffix in "" /child1 /child2; do
		l_progress_source="$ZXFER_MOCKBIN_SOURCE_ROOT$l_progress_suffix"
		planning_assert_log_has_line \
			"send -nPv -I $l_progress_source@snap2 $l_progress_source@snap3"
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_progress_suffix"
		assertTrue "the dialog must start with the expanded size and title for $l_progress_source" \
			"grep -Fxq -- '-s 4096 -t $l_progress_source@snap3' '$CASE_DIR/dialog.argv'"
		assertTrue "the dialog must receive the $l_progress_source stream" \
			"grep -Fxq 'ZXFERMOCKSTREAM send -I $l_progress_source@snap2 $l_progress_source@snap3' '$CASE_DIR/dialog.stream'"
	done
	assertEquals "the dialog runs once per send" \
		3 "$(wc -l <"$CASE_DIR/dialog.argv" | tr -d ' ')"
}

# ---------------------------------------------------------------------------
# Snapshot planning and destination state: deletes, -g, divergence, seeding
# and the SunOS existence probes. These cases took over the white-box pins of
# the snapshot-plan and destination-state unit suites.

# Purpose: Drop child1's rows from both source snapshot listings in
# STATE_DIR, so -d plans to delete every child1 destination snapshot, and
# answer the live source recheck of child1 with the given output and status.
# Usage: planning_empty_child1_source_listing <recheck-output> <status>
planning_empty_child1_source_listing() {
	l_empty_child="$ZXFER_MOCKBIN_SOURCE_ROOT/child1"
	for l_empty_fixture in src_snapshots.list src_snapshots_dataset.list; do
		grep -v "^$l_empty_child@" "$STATE_DIR/$l_empty_fixture" \
			>"$STATE_DIR/$l_empty_fixture.new" ||
			fail "Unable to drop $l_empty_child from $l_empty_fixture."
		mv "$STATE_DIR/$l_empty_fixture.new" "$STATE_DIR/$l_empty_fixture" ||
			fail "Unable to install the emptied $l_empty_fixture."
	done
	printf '%s' "$1" >"$STATE_DIR/src_recheck.list" ||
		fail "Unable to write the live source recheck fixture."
	printf 'list -H -d 1 -o name -t snapshot %s\tsrc_recheck.list\t%s\n' \
		"$l_empty_child" "$2" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the live source recheck rule."
}

# Invariant (-d, full wipe): when a dataset's cached source listing holds no
# snapshot, -d would delete every destination snapshot of it, so the source
# is first listed again live. A recheck that finds snapshots skips the delete
# with a warning; an empty one destroys them all in one comma-joined destroy
# with no creation-time query and no re-list (nothing is left to send), and
# transport noise in it is no snapshot; a failed recheck stops the run with
# its status before any destroy.
test_delete_option_rechecks_the_source_before_deleting_every_destination_snapshot() {
	planning_setup_env
	l_wipe_source="$ZXFER_MOCKBIN_SOURCE_ROOT/child1"
	l_wipe_destroy="MUTATE destroy $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1@snap3,snap2,snap1"

	planning_clone_state "$FIXTURE_DIR/noop" wipe_live
	planning_empty_child1_source_listing "$l_wipe_source@snap1" 0
	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "a skipped wipe should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line "list -H -d 1 -o name -t snapshot $l_wipe_source"
	assertTrue "the skip must warn that the cached source listing was incomplete" \
		"grep -Fq 'WARNING: skipping destination snapshot deletion for [$l_wipe_source]' '$CASE_DIR/zxfer.stderr'"
	planning_assert_no_mutations

	for l_wipe_case in empty noise; do
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/noop" "wipe_$l_wipe_case"
		if [ "$l_wipe_case" = empty ]; then
			planning_empty_child1_source_listing "" 0
		else
			planning_empty_child1_source_listing \
				"Warning: Permanently added 'src' (ED25519) to the list of known hosts." 0
		fi
		planning_run_zxfer "$STATE_DIR" -d -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		l_run_status=$?
		assertEquals "a confirmed wipe should exit 0 [$l_wipe_case]; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_run_status"
		planning_assert_log_has_line "$l_wipe_destroy"
		assertEquals "the one destroy is the only mutation [$l_wipe_case]" \
			1 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
		assertFalse "without a common snapshot or -g no creation time is read [$l_wipe_case]" \
			"grep -q '^get -H -o name,value -p creation ' '$ZFS_LOG'"
		assertFalse "with nothing left to send nothing is listed again [$l_wipe_case]" \
			"grep -q '^list -H -d 1 -o name,guid ' '$ZFS_LOG'"
	done

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" wipe_failed
	planning_empty_child1_source_listing "cannot open '$l_wipe_source': I/O error" 2
	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a failed recheck must stop the run with its status" 2 $?
	planning_assert_failure_report replication \
		"Failed to re-verify source snapshots for [$l_wipe_source] before deleting all destination snapshots: cannot open '$l_wipe_source': I/O error"
	planning_assert_no_mutations
}

# Invariant (-d, fail closed): a failed creation-time query or destroy stops
# the run with that zfs call's status and diagnostic, and no destroy starts.
test_delete_failures_keep_the_zfs_status_and_diagnostic() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" delete_failures
	planning_add_extra_destination_snapshot

	for l_delfail_case in \
		"get -H -o name,value -p creation *|5|Failed to query destination snapshot creation times while planning snapshot deletions." \
		"destroy *|6|Error when executing command."; do
		l_delfail_match=${l_delfail_case%%|*}
		l_delfail_rest=${l_delfail_case#*|}
		l_delfail_status=${l_delfail_rest%%|*}
		: >"$ZFS_LOG"
		rm -rf "$CASE_DIR/fail_calls"
		mkdir "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
		# Export in a subshell: FreeBSD sh does not export a prefix assignment
		# on a function call.
		(
			MOCK_FAIL_TOOL=zfs
			MOCK_FAIL_CALL=1
			MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
			MOCK_FAIL_MATCH=$l_delfail_match
			MOCK_FAIL_STATUS=$l_delfail_status
			MOCK_FAIL_STDERR="Permission denied (publickey)."
			export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
				MOCK_FAIL_STATUS MOCK_FAIL_STDERR
			planning_run_zxfer "$STATE_DIR" -d -R \
				"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		)
		l_run_status=$?
		assertEquals "a failed [$l_delfail_match] must keep its status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			"$l_delfail_status" "$l_run_status"
		planning_assert_failure_report replication "${l_delfail_rest#*|}"
		assertTrue "the zfs diagnostic must reach stderr [$l_delfail_match]" \
			"grep -Fqx 'Permission denied (publickey).' '$CASE_DIR/zxfer.stderr'"
		planning_assert_no_mutations
	done
}

# Invariant (-d, names with spaces): the creation-time query, the destroy and
# the re-list after it pass a dataset name holding a space as one argument.
test_delete_option_keeps_dataset_names_with_spaces_whole() {
	planning_use_fixture_roots "srcpool/my data" "$ZXFER_MOCKBIN_DEST_ROOT" \
		"$ZXFER_MOCKBIN_DEST_ROOT/my data"
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" delete_spaces
	planning_add_extra_destination_snapshot
	planning_log_canned_zfs_argv
	l_spaces_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT

	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-d over names with spaces should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	for l_spaces_argv in \
		"[get] [-H] [-o] [name,value] [-p] [creation] [$l_spaces_root@snap3] [$l_spaces_root@snap9] " \
		"[destroy] [$l_spaces_root@snap9] " \
		"[list] [-H] [-d] [1] [-o] [name,guid] [-t] [snapshot] [$l_spaces_root] "; do
		assertTrue "zfs must get [$l_spaces_argv]; argv: $(cat "$ARGV_LOG")" \
			"grep -Fxq '$l_spaces_argv' '$ARGV_LOG'"
	done
	assertEquals "the destroy is the only mutation" 1 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
}

# Invariant (-d -g report): refusing to destroy a protected snapshot is a
# usage error (exit 2) whose report names the -g limit and the snapshot with
# its age and creation date, and says how to recover.
test_grandfather_refusal_names_the_limit_and_the_protected_snapshot() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" grandfather_report
	planning_add_extra_destination_snapshot

	planning_run_zxfer "$STATE_DIR" -d -g 30 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a -g refusal is a usage error" 2 $?
	for l_grandfather_line in \
		"failure_class: usage" \
		"You have set grandfather protection at 30 days." \
		"Snapshot name: $ZXFER_MOCKBIN_DEST_MAPPED_ROOT@snap9" \
		"Snapshot date: " \
		"Either amend/remove option g, fix your system date, or manually"; do
		grep -Fq -- "$l_grandfather_line" "$CASE_DIR/zxfer.stderr" ||
			fail "Missing -g refusal line: $l_grandfather_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	assertTrue "the refusal must give the snapshot's age in days" \
		"grep -Eq 'Snapshot age : [0-9]+ days old' '$CASE_DIR/zxfer.stderr'"
	planning_assert_no_mutations
}

# Purpose: Diverge child1's newest destination snapshot (@snap2, guid
# 9999900102000000007) in an incremental STATE_DIR until its convergence
# receive: one 'once' rule serves the diverged recursive listing (the fast
# no-op proof's, which discovery reuses), one serves the post-destroy
# depth-1 listing to the re-plan, and the post-receive check falls through to
# the aligned listing. Adds the creation-time and existence rules that the
# destroy and the rollback read.
# Usage: planning_make_child1_diverged_until_receive
planning_make_child1_diverged_until_receive() {
	l_diverged_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	awk -F'\t' -v name="$l_diverged_child@snap2" 'BEGIN { OFS = "\t" }
		$1 == name { $2 = "9999900102000000007" }
		{ print }
	' "$STATE_DIR/dst_snapshots.list" >"$STATE_DIR/dst_snapshots_diverged.list" ||
		fail "Unable to write the diverged listing."
	grep -v "^$l_diverged_child@snap2" "$STATE_DIR/dst_d1_1.list" \
		>"$STATE_DIR/dst_d1_1_post_destroy.list" ||
		fail "Unable to write the post-destroy listing."
	printf '%s@snap1\t1700000001\n%s@snap2\t1700000002\n' \
		"$l_diverged_child" "$l_diverged_child" \
		>"$STATE_DIR/dst_child1_creation.list" ||
		fail "Unable to write the creation-time fixture."
	printf '%s\t96K\t1.0G\t24K\t/%s\n' "$l_diverged_child" "$l_diverged_child" \
		>"$STATE_DIR/dst_exists_child1.list" ||
		fail "Unable to write the existence fixture."
	awk -F'\t' \
		-v key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		-v d1_key="list -H -d 1 -o name,guid -t snapshot $l_diverged_child" '
		BEGIN { OFS = "\t" }
		$1 == key { print key, "dst_snapshots_diverged.list", 0, "once" }
		$1 == d1_key { print d1_key, "dst_d1_1_post_destroy.list", 0, "once" }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the consumable diverged listing rules."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the consumable diverged listing rules."
	printf '%s\t%s\t0\n' \
		"get -H -o name,value -p creation $l_diverged_child@*" dst_child1_creation.list \
		"list -H $l_diverged_child" dst_exists_child1.list >>"$STATE_DIR/manifest" ||
		fail "Unable to append the creation-time and existence rules."
}

# Invariant (-d -F -g, divergence): the -g pre-pass plans every dataset and
# the main pass plans it again, yet a diverged dataset warns and counts once.
# While child1 carries its convergence mark, the root's receive is not
# checked against a live listing: only child1 is listed at depth 1, before
# its rollback and after its receive. -V names each planned dataset's last
# common snapshot.
test_grandfather_prepass_warns_once_and_checks_only_the_diverged_child() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" prepass_diverged_child
	planning_make_child1_diverged_until_receive
	l_prepass_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	l_prepass_tab=$(printf '\t')

	planning_run_zxfer "$STATE_DIR" -V -d -F -g 36500 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-V -d -F -g over a diverged child should converge and exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the pre-pass and the main pass must warn about child1 once" \
		1 "$(grep -cF "WARNING: destination dataset [$l_prepass_child] has 1 snapshot" "$CASE_DIR/zxfer.stderr")"
	assertTrue "the -V profile must count the warned dataset once" \
		"grep -Fq 'zxfer profile: diverged_snapshot_warnings=1' '$CASE_DIR/zxfer.stderr'"
	assertEquals "only child1 is listed at depth 1: for its re-plan and its post-receive check" \
		"2 2" "$(grep -c '^list -H -d 1 ' "$ZFS_LOG") $(grep -cFx "list -H -d 1 -o name,guid -t snapshot $l_prepass_child" "$ZFS_LOG")"
	planning_assert_log_has_line "MUTATE rollback -r $l_prepass_child@snap1"
	for l_prepass_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive -F $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prepass_suffix"
	done
	assertTrue "-V must name each dataset's last common snapshot" \
		"grep -Fq 'Found last common snapshot: $ZXFER_MOCKBIN_SOURCE_ROOT@snap2${l_prepass_tab}1000000002000000007.' '$CASE_DIR/zxfer.stderr'"
}

# Invariant (post-receive check, fail closed): when the live listing that
# checks a converged dataset after its receive fails, the run stops with a
# report naming the dataset and carrying the listing's output.
test_post_receive_divergence_check_fails_closed_when_its_listing_fails() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" verify_listing_failure
	planning_make_destination_diverged_until_receive
	l_verify_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	printf "cannot open '%s': I/O error\n" "$l_verify_root" \
		>"$STATE_DIR/verify_failure.list" ||
		fail "Unable to write the listing failure fixture."
	# The re-plan keeps its 'once' rule; the check after the receive fails.
	awk -F'\t' -v key="list -H -d 1 -o name,guid -t snapshot $l_verify_root" '
		BEGIN { OFS = "\t" }
		$1 == key && $4 != "once" { print key, "verify_failure.list", 2; next }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the failing check rule."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the failing check rule."

	planning_run_zxfer "$STATE_DIR" -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a failed post-receive listing must stop the run" 1 $?
	planning_assert_log_has_line "receive -F $l_verify_root"
	for l_verify_line in \
		"zxfer: failure report begin" \
		"failure_class: runtime" \
		"message: Failed to retrieve live destination snapshots for [$l_verify_root] during post-receive divergence verification: cannot open '$l_verify_root': I/O error"; do
		grep -Fq -- "$l_verify_line" "$CASE_DIR/zxfer.stderr" ||
			fail "Missing failure report line: $l_verify_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
}

# Invariant: a destination snapshot row without a guid fails its dataset's
# plan closed like a source row does: the planner's exit 3, a report naming
# both datasets, and nothing received into that dataset.
test_guidless_destination_row_fails_its_dataset_plan_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" guidless_destination_row
	l_guidless_source="$ZXFER_MOCKBIN_SOURCE_ROOT/child1"
	l_guidless_dest="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	awk -F'\t' -v row="$l_guidless_dest@snap2" \
		'$1 == row { print $1; next } { print }' \
		"$FIXTURE_DIR/incremental/dst_snapshots.list" >"$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to strip the guid from one destination row."

	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "a guid-less destination row must stop the run with the planner's status" \
		3 "$l_run_status"
	planning_assert_failure_report replication \
		"message: Failed to determine the last common snapshot for [$l_guidless_source] and [$l_guidless_dest]."
	assertFalse "nothing may be received into the dataset with the guid-less row" \
		"grep -q '^receive $l_guidless_dest\$' '$ZFS_LOG'"
	planning_assert_no_mutations
}

# Invariant (seed): a destination dataset that exists without snapshots is
# seeded with every source snapshot, oldest first: a forced full receive of
# the oldest, then one increment to the newest. One whose snapshots share no
# guid with the source is refused before anything is sent.
test_snapshotless_destination_is_seeded_oldest_first_and_unrelated_snapshots_are_refused() {
	planning_setup_env
	l_seed_source="$ZXFER_MOCKBIN_SOURCE_ROOT/child2"
	l_seed_dest="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"

	planning_clone_state "$FIXTURE_DIR/noop" seed_empty
	grep -v "^$l_seed_dest@" "$FIXTURE_DIR/noop/dst_snapshots.list" \
		>"$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to drop the child2 destination snapshots."
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "seeding a snapshot-less child2 should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	# Either side of a send | receive pipeline may log first, so sends and
	# receives are compared as two sequences.
	assertEquals "child2 is seeded with its oldest snapshot, then sent one increment to the newest" \
		"send $l_seed_source@snap1
send -I $l_seed_source@snap1 $l_seed_source@snap3" "$(grep '^send ' "$ZFS_LOG")"
	assertEquals "the seed is received with -F and the increment without" \
		"receive -F $l_seed_dest
receive $l_seed_dest" "$(grep '^receive ' "$ZFS_LOG")"
	planning_assert_no_mutations

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" seed_unrelated
	# child2's snapshots become ones the source never had (other names and
	# guids), so no snapshot is common and none is diverged.
	awk -F'\t' -v prefix="$l_seed_dest@snap" 'BEGIN { OFS = "\t" }
		index($1, prefix) == 1 { sub(/@snap/, "@other", $1); $2 = "8888800201000000007" }
		{ print }
	' "$FIXTURE_DIR/noop/dst_snapshots.list" >"$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to replace the child2 destination snapshots."
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a destination with only unrelated snapshots must be refused" 1 $?
	planning_assert_failure_report replication \
		"Destination dataset [$l_seed_dest] has snapshots but none share a common guid with the source."
	planning_assert_no_send_receive
	planning_assert_no_mutations
}

# Invariant (environment): a convergence mark inherited from the environment
# cannot let a diverged dataset through without -d and -F: the run still
# fails closed in divergence reconciliation before any send.
test_inherited_convergence_mark_cannot_bypass_the_divergence_contract() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" inherited_mark
	planning_make_destination_diverged

	# Export in a subshell: FreeBSD sh does not export a prefix assignment on
	# a function call.
	(
		g_zxfer_diverged_converged_datasets=$(printf '%s\t%s' \
			"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_SOURCE_ROOT")
		export g_zxfer_diverged_converged_datasets
		planning_run_zxfer "$STATE_DIR" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "an inherited mark must not let divergence through" 1 $?
	planning_assert_failure_report "divergence reconciliation" \
		"Destination dataset [$ZXFER_MOCKBIN_DEST_MAPPED_ROOT] has diverged from source dataset [$ZXFER_MOCKBIN_SOURCE_ROOT]"
	planning_assert_no_send_receive
	planning_assert_no_mutations
}

# Purpose: Put a uname in MOCKBIN_DIR that names the given operating system
# ("mockhost" for -n), so zxfer takes that platform's code paths.
# Usage: planning_write_mock_uname <os>
planning_write_mock_uname() {
	cat >"$MOCKBIN_DIR/uname" <<EOF
#!/bin/sh
case "\${1:-}" in
-n) printf '%s\n' mockhost ;;
*) printf '%s\n' '$1' ;;
esac
EOF
	chmod +x "$MOCKBIN_DIR/uname"
}

# Purpose: Make the mapped destination root in STATE_DIR ambiguous the SunOS
# way: its snapshot listing and every exact `list -H` probe at or below it
# fail without a diagnostic, and the dataset inventory lists only its parent.
# The parent's recursive listing answers the first rows and status, the
# pool's the second ("-" for no output, "," between lines), and the root's
# lists the root, as it would after its seed receive.
# Usage: planning_make_destination_root_ambiguous <parent-rows> <status>
# <pool-rows> <status>
planning_make_destination_root_ambiguous() {
	l_ambiguous_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	l_ambiguous_parent=$ZXFER_MOCKBIN_DEST_ROOT
	l_ambiguous_parent_fixture=-
	l_ambiguous_pool_fixture=-
	if [ "$1" != - ]; then
		printf '%s\n' "$1" | tr ',' '\n' >"$STATE_DIR/parent_listing.list" ||
			fail "Unable to write the parent listing fixture."
		l_ambiguous_parent_fixture=parent_listing.list
	fi
	if [ "$3" != - ]; then
		printf '%s\n' "$3" | tr ',' '\n' >"$STATE_DIR/pool_listing.list" ||
			fail "Unable to write the pool listing fixture."
		l_ambiguous_pool_fixture=pool_listing.list
	fi
	printf '%s\n' "$l_ambiguous_root" >"$STATE_DIR/root_listing.list" ||
		fail "Unable to write the root listing fixture."
	printf '%s\n' "$l_ambiguous_parent" >"$STATE_DIR/dst_datasets.list" ||
		fail "Unable to write the dataset inventory fixture."
	{
		printf '%s\t-\t1\n' \
			"list -Hr -o name,guid -t snapshot $l_ambiguous_root" \
			"list -H $l_ambiguous_root*"
		printf '%s\t%s\t%s\n' \
			"list -H -r -o name $l_ambiguous_parent" "$l_ambiguous_parent_fixture" "$2" \
			"list -H -r -o name ${l_ambiguous_parent%%/*}" "$l_ambiguous_pool_fixture" "$4" \
			"list -H -r -o name $l_ambiguous_root" root_listing.list 0
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" ||
		fail "Unable to prepend the ambiguous probe rules."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the ambiguous probe rules."
}

# Invariant (SunOS existence probes): OmniOS zfs list fails without a
# diagnostic for a missing dataset, so on SunOS an ambiguous exact probe is
# settled by a recursive listing of the parent and, when that is ambiguous
# too, of each ancestor. A root proven missing is bootstrapped and seeded
# from its oldest snapshot; a root listed there exists, so its failed
# snapshot listing stops the run; anything unproven fails closed in
# discovery with nothing sent. Elsewhere the same silent probe fails closed
# at once. -V traces each fallback listing.
test_sunos_ambiguous_existence_probes_decide_from_recursive_listings() {
	planning_setup_env
	l_amb_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	l_amb_parent=$ZXFER_MOCKBIN_DEST_ROOT
	l_amb_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	l_amb_unknown="Failed to determine whether destination dataset [$l_amb_root] exists"
	l_amb_failed="$l_amb_unknown: parent recursive listing for [$l_amb_parent] failed"
	l_amb_row_count=0
	# os|parent rows|status|pool rows|status|outcome: "seed" or the message.
	for l_amb_row in \
		"SunOS|$l_amb_parent|0|-|0|seed" \
		"SunOS|$l_amb_parent,$l_amb_root|0|-|0|Failed to retrieve snapshot list from the destination." \
		"SunOS|otherpool/other|0|-|0|$l_amb_unknown: parent recursive listing for [$l_amb_parent] did not contain the parent dataset." \
		"SunOS|permission denied|1|-|0|$l_amb_failed: permission denied" \
		"SunOS|cannot open '$l_amb_parent': dataset does not exist|1|-|0|seed" \
		"SunOS|-|1|$l_amb_pool|0|seed" \
		"SunOS|-|1|-|1|$l_amb_failed." \
		"SunOS|-|1|$l_amb_pool,$l_amb_parent|0|$l_amb_failed." \
		"SunOS|-|1|otherpool|0|$l_amb_failed." \
		"SunOS|-|1|cannot open '$l_amb_pool': no such pool or dataset|1|seed" \
		"Linux|$l_amb_parent|0|-|0|$l_amb_unknown."; do
		l_amb_row_count=$((l_amb_row_count + 1))
		l_amb_os=${l_amb_row%%|*}
		l_amb_rest=${l_amb_row#*|}
		l_amb_parent_rows=${l_amb_rest%%|*}
		l_amb_rest=${l_amb_rest#*|}
		l_amb_parent_status=${l_amb_rest%%|*}
		l_amb_rest=${l_amb_rest#*|}
		l_amb_pool_rows=${l_amb_rest%%|*}
		l_amb_rest=${l_amb_rest#*|}
		l_amb_pool_status=${l_amb_rest%%|*}
		l_amb_outcome=${l_amb_rest#*|}
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/noop" "ambiguous_$l_amb_row_count"
		planning_write_mock_uname "$l_amb_os"
		planning_make_destination_root_ambiguous "$l_amb_parent_rows" \
			"$l_amb_parent_status" "$l_amb_pool_rows" "$l_amb_pool_status"

		planning_run_zxfer "$STATE_DIR" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		l_run_status=$?
		if [ "$l_amb_outcome" = seed ]; then
			assertEquals "a root proven missing is bootstrapped [$l_amb_row]; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
				0 "$l_run_status"
			planning_assert_log_has_line "send $ZXFER_MOCKBIN_SOURCE_ROOT@snap1"
			planning_assert_log_has_line "receive $l_amb_root"
		else
			assertEquals "an unproven or existing root stops the run [$l_amb_row]" \
				1 "$l_run_status"
			planning_assert_failure_report "snapshot discovery" "message: $l_amb_outcome"
			planning_assert_no_send_receive
		fi
		planning_assert_no_mutations
	done

	# -V traces the exact probe and both fallback listings.
	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" ambiguous_traced
	planning_write_mock_uname SunOS
	planning_make_destination_root_ambiguous - 1 "$l_amb_pool" 0
	planning_run_zxfer "$STATE_DIR" -V -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the traced ancestor walk should bootstrap the root; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	# Each trace is "LABEL: '<zfs path>' 'arg' ...".
	for l_amb_trace in \
		"Checking if destination exists|'list' '-H' '$l_amb_root'" \
		"Exact destination probe was ambiguous on SunOS; checking parent recursively|'list' '-H' '-r' '-o' 'name' '$l_amb_parent'" \
		"Parent recursive destination probe was ambiguous on SunOS; checking ancestor recursively|'list' '-H' '-r' '-o' 'name' '$l_amb_pool'"; do
		grep -F "${l_amb_trace%%|*}: '" "$CASE_DIR/zxfer.stderr" |
			grep -Fq "/zfs' ${l_amb_trace#*|}" ||
			fail "Missing -V trace line: $l_amb_trace
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
}

# ---------------------------------------------------------------------------
# Snapshot discovery: the fast no-op proof, full discovery's listings, -x,
# -j, -O -Z and the temp paths discovery renders into its pipelines.

# Invariant (no-op proof, source side): a source listing that dies after
# printing only the rows the destination already has never proves a no-op.
# Locally zfs's status reaches the proof, which fails closed with the
# listing's stderr and status (the bare message without stderr) before full
# discovery starts. Over -O with -z the origin's compressor masks that
# status, so only the success sentinel inside the compressed stream marks a
# listing complete: the truncated stream fails the local check, the proof
# declines, and full discovery sends every dataset.
test_proof_source_listing_that_fails_after_the_destination_rows_never_proves_a_noop() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" truncated_proof
	l_truncated_key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	# The listing stops before the newest snapshots, the ones the destination
	# lacks, so the rows it prints match the destination's exactly.
	grep -v '@snap3' "$FIXTURE_DIR/incremental/src_snapshots_dataset.list" \
		>"$STATE_DIR/src_snapshots_dataset.list" ||
		fail "Unable to write the truncated proof listing."

	for l_truncated_stderr in "cannot iterate filesystems: I/O error" ""; do
		: >"$ZFS_LOG"
		planning_fail_canned_zfs_after_output "$l_truncated_key" 1 "$l_truncated_stderr"
		planning_run_zxfer "$STATE_DIR" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		assertEquals "a proof listing that failed must fail the run with its status [stderr:$l_truncated_stderr]" \
			1 $?
		planning_assert_failure_report "snapshot discovery" \
			"message: Failed to retrieve snapshots from the source${l_truncated_stderr:+: $l_truncated_stderr}"
		assertTrue "the message must end where the listing's stderr does [stderr:$l_truncated_stderr]" \
			"grep -Fqx 'message: Failed to retrieve snapshots from the source${l_truncated_stderr:+: $l_truncated_stderr}' '$CASE_DIR/zxfer.stderr'"
		assertFalse "a failed proof must not continue into full discovery [stderr:$l_truncated_stderr]" \
			"grep -q -- '-s creation' '$ZFS_LOG'"
		planning_assert_no_mutations
		planning_assert_no_send_receive
	done

	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	planning_write_mock_zstd "$MOCKBIN_DIR/zstd" ||
		fail "Unable to write the mock zstd."
	: >"$ZFS_LOG"
	planning_fail_canned_zfs_after_output "$l_truncated_key" 1 \
		"cannot iterate filesystems: I/O error"
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -O localhost -z -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-O -z must fall back to full discovery and succeed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	for l_truncated_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_truncated_suffix"
	done
	planning_assert_no_mutations
}

# Invariant (-O -Z -j): every stream the origin sends, both discovery
# listings and each send, is compressed there with the configured command and
# decompressed locally; zxfer checks and strips the listing's success sentinel
# only after decompression. The compressed -j listing runs the origin's
# parallel with the requested job count. The stand-in zstd rewrites every
# line, so a stream that skipped either side of the codec would not parse.
# -V counts the parallel listing and the source ssh shell invocations.
test_remote_origin_compressed_parallel_discovery_replicates_every_dataset() {
	planning_setup_parallel_jobs_env compressed_parallel
	planning_log_mock_parallel_argv
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	planning_write_mock_zstd "$MOCKBIN_DIR/zstd" ||
		fail "Unable to write the mock zstd."
	SSH_LOG="$CASE_DIR/ssh_compressed.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	TMPDIR="$JOB_TMP_DIR" PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -V -O localhost -Z "zstd -5" -j 2 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	unset MOCK_SSH_LOG
	assertEquals "-O -Z -j must replicate the tree; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	for l_compressed_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_compressed_suffix"
	done
	assertEquals "the proof listing, the -j listing and three sends must compress with -Z's command" \
		"-5 -5 -5 -5 -5" "$(grep -vx -e -d "$MOCKBIN_DIR/zstd.argv" | tr '\n' ' ' | sed 's/ $//')"
	assertEquals "each compressed stream must be decompressed once" \
		5 "$(grep -cx -e -d "$MOCKBIN_DIR/zstd.argv")"
	assertEquals "the origin's parallel must get the job count, line buffering and the bare zfs runner" \
		"-j 2 --line-buffer -- '$MOCKBIN_DIR/zfs' 'list' '-H' '-o' 'name,guid' '-s' 'creation' '-d' '1' '-t' 'snapshot' {}" \
		"$(cat "$PARALLEL_ARGV_LOG")"
	# Every command ran over the origin's master, so each counts once.
	for l_compressed_counter in source_snapshot_list_parallel_commands=1 \
		"source_ssh_shell_invocations=$(grep -c '^mux' "$SSH_LOG")"; do
		assertTrue "-V must report $l_compressed_counter; stderr: $(grep '^zxfer profile: ' "$CASE_DIR/zxfer.stderr")" \
			"grep -Fqx 'zxfer profile: $l_compressed_counter' '$CASE_DIR/zxfer.stderr'"
	done
	planning_assert_no_mutations
	planning_assert_no_parallel_job_leftovers
}

# Invariant (no-op proof, destination side): a destination listing that
# prints every row and then fails is never trusted. An operational error
# fails the run closed with the listing's stderr and status before full
# discovery starts; a listing that reports the root itself missing declines
# the proof, and full discovery lists the destination again.
test_proof_destination_listing_that_fails_after_its_rows_fails_closed_unless_the_root_is_missing() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" proof_destination_failure
	l_proof_key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"

	planning_fail_canned_zfs_after_output "$l_proof_key" 1 \
		"cannot open '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1': permission denied"
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a failed destination listing must fail the run with its status" 1 $?
	assertContains "the listing's stderr must reach the operator" \
		"$(cat "$CASE_DIR/zxfer.stderr")" \
		"cannot open '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1': permission denied"
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshot list from the destination."
	assertFalse "a failed proof must not continue into full discovery" \
		"grep -q -- '-s creation' '$ZFS_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive

	: >"$ZFS_LOG"
	planning_fail_canned_zfs_after_output "$l_proof_key" 1 \
		"cannot open '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT': dataset does not exist"
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "a listing that reports the root missing must decline the proof, not fail; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	assertEquals "full discovery must list the destination again" \
		2 "$(grep -cFx "$l_proof_key" "$ZFS_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: a source listing that succeeds without a single snapshot is an
# error, never an empty source; with -d it would otherwise mark every
# destination snapshot for destruction. The no-op proof refuses an empty
# source stream even when the destination's is empty too, and full
# discovery refuses an empty creation-order listing.
test_empty_source_listing_fails_closed_before_any_change() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" empty_proof
	planning_answer_manifest_key_with_nothing \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_answer_manifest_key_with_nothing \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "an empty proof source listing must fail the run" 1 $?
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshots from the source"
	assertFalse "the proof must not fall back to full discovery" \
		"grep -q -- '-s creation' '$ZFS_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" empty_full
	# The destination-only @snap9 declines the proof.
	planning_add_extra_destination_snapshot
	planning_answer_manifest_key_with_nothing \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_run_zxfer "$STATE_DIR" -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "an empty creation-order listing must fail the run" 1 $?
	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshots from the source"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant (-x, no-op proof): both sides drop an excluded dataset's records
# before the proof compares them, so a run whose only difference is an
# excluded dataset is a proven no-op with the two identity listings alone,
# even under -j (the proof never runs parallel) and over -O (the origin lists
# without the pattern). An exclude that matches every dataset leaves nothing
# to prove, so the proof falls back to full discovery, which finds no work.
# Full discovery also drops excluded records before it diffs, so -v reports
# no delta for them.
test_exclude_option_proves_a_noop_when_only_excluded_datasets_differ() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" excluded_difference
	for l_excluded_fixture in dst_snapshots.list dst_d1_1.list; do
		grep -v "/child1@snap3" "$FIXTURE_DIR/noop/$l_excluded_fixture" \
			>"$STATE_DIR/$l_excluded_fixture" ||
			fail "Unable to drop child1@snap3 from $l_excluded_fixture."
	done
	# A parallel that must never run.
	cat >"$MOCKBIN_DIR/parallel" <<EOF_PARALLEL
#!/bin/sh
printf '%s\n' "\$*" >>"$CASE_DIR/parallel.log"
exit 1
EOF_PARALLEL
	chmod +x "$MOCKBIN_DIR/parallel" || fail "Unable to write the parallel stand-in."

	planning_run_zxfer "$STATE_DIR" -j 2 -x child1 -v -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "an excluded-only difference must be a proven no-op; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the proof must cost exactly the two identity listings; zfs log: $(cat "$ZFS_LOG")" \
		2 "$(wc -l <"$ZFS_LOG" | tr -d ' ')"
	assertTrue "-v must report that nothing needs transfer" \
		"grep -Fqx 'No new snapshots to transfer.' '$CASE_DIR/zxfer.stdout'"

	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_excluded.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"
	: >"$ZFS_LOG"
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -O localhost -j 2 -x child1 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	unset MOCK_SSH_LOG
	assertEquals "an -O excluded-only difference must be a proven no-op; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the -O proof must cost exactly the two identity listings; zfs log: $(cat "$ZFS_LOG")" \
		2 "$(wc -l <"$ZFS_LOG" | tr -d ' ')"
	l_origin_listing=$(grep -- "ssh-origin.sock localhost .*snapshot" "$SSH_LOG")
	assertEquals "the origin must list once; ssh log: $(cat "$SSH_LOG")" \
		1 "$(printf '%s\n' "$l_origin_listing" | grep -c .)"
	assertNotContains "the origin must list without the exclude pattern" \
		"$l_origin_listing" "child1"
	assertFalse "parallel must never run for the proof" "[ -e '$CASE_DIR/parallel.log' ]"

	: >"$ZFS_LOG"
	planning_run_zxfer "$FIXTURE_DIR/noop" -x "$ZXFER_MOCKBIN_SOURCE_ROOT" -v -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "an exclude that matches every dataset must succeed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	assertTrue "-v must report that nothing needs transfer after the fallback" \
		"grep -Fqx 'No new snapshots to transfer.' '$CASE_DIR/zxfer.stdout'"
	planning_assert_no_mutations
	planning_assert_no_send_receive

	: >"$ZFS_LOG"
	planning_run_zxfer "$STATE_DIR" -x child1 -v -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-N with an excluded-only difference must succeed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertFalse "full discovery must drop excluded records before it reports a delta; stdout: $(cat "$CASE_DIR/zxfer.stdout")" \
		"grep -q 'Recursive snapshot delta summary' '$CASE_DIR/zxfer.stdout'"
	assertTrue "-v must report that nothing needs transfer" \
		"grep -Fqx 'No new snapshots to transfer.' '$CASE_DIR/zxfer.stdout'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant (-j): the dataset enumeration that feeds parallel must succeed as
# a whole. One that prints some datasets and then fails would otherwise look
# like a shorter tree whose listing parallel completes: the missing datasets'
# destination snapshots would read as destination-only for -d. The listing
# stops with exit 70 instead, before any change.
test_parallel_jobs_dataset_enumeration_that_fails_partway_fails_closed() {
	planning_setup_parallel_jobs_env enumeration_partway
	printf '%s\n' "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_SOURCE_ROOT/child1" \
		>"$STATE_DIR/src_datasets_partial.list" ||
		fail "Unable to write the partial enumeration fixture."
	{
		printf 'list -Hr -t filesystem,volume -o name %s\tsrc_datasets_partial.list\t1\n' \
			"$ZXFER_MOCKBIN_SOURCE_ROOT"
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" ||
		fail "Unable to prepend the partial enumeration rule."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the partial enumeration rule."

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 2 -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "a failed enumeration must stop the -j listing with exit 70; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		70 "$l_run_status"
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshots from the source"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_no_parallel_job_leftovers
}

# Invariant: the destination dataset inventory later work reads must be
# complete. A failed inventory listing stops the run with the listing's
# status and stderr (the bare message without stderr), one that succeeds
# empty stops it too, and a missing root whose pool cannot be listed names
# the pool probe's stderr. Nothing is sent in any case.
test_destination_inventory_failures_fail_closed_with_the_listing_diagnostic() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" inventory_failures
	l_inventory_key="list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT"

	for l_inventory_case in "permission denied|13" "|14"; do
		l_inventory_stderr=${l_inventory_case%|*}
		rm -rf "$CASE_DIR/fail_calls"
		mkdir "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
		: >"$ZFS_LOG"
		(
			MOCK_FAIL_TOOL=zfs
			MOCK_FAIL_CALL=1
			MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
			MOCK_FAIL_MATCH=$l_inventory_key
			MOCK_FAIL_STDERR=$l_inventory_stderr
			MOCK_FAIL_STATUS=${l_inventory_case##*|}
			export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
				MOCK_FAIL_STDERR MOCK_FAIL_STATUS
			planning_run_zxfer "$STATE_DIR" -R \
				"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		)
		assertEquals "a failed inventory listing must keep its status [stderr:$l_inventory_stderr]" \
			"${l_inventory_case##*|}" $?
		assertTrue "the report must carry the listing's stderr, or none [stderr:$l_inventory_stderr]; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			"grep -Fqx 'message: Failed to retrieve list of datasets from the destination${l_inventory_stderr:+: $l_inventory_stderr}' '$CASE_DIR/zxfer.stderr'"
		planning_assert_no_mutations
		planning_assert_no_send_receive
	done

	: >"$ZFS_LOG"
	planning_answer_manifest_key_with_nothing "$l_inventory_key"
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "an empty inventory must fail the run" 1 $?
	planning_assert_failure_report "snapshot discovery" \
		"Staged destination dataset inventory was empty."
	planning_assert_no_mutations
	planning_assert_no_send_receive

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" inventory_missing_pool
	l_missing_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	printf 'list -H -o name %s\t-\t0\n' "$l_missing_pool" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the pool rule."
	rm -rf "$CASE_DIR/fail_calls"
	planning_make_remote_destination_root_missing
	planning_fail_canned_zfs_after_output "list -H -o name $l_missing_pool" 2 \
		"cannot open '$l_missing_pool': pool I/O is currently suspended"
	planning_run_zxfer "$STATE_DIR" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
	assertEquals "an unlistable pool must keep the pool probe's status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		2 "$l_run_status"
	planning_assert_failure_report "snapshot discovery" \
		"Destination dataset [$ZXFER_MOCKBIN_DEST_ROOT] is missing and destination pool [$l_missing_pool] could not be listed: cannot open '$l_missing_pool': pool I/O is currently suspended"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: discovery renders its temp paths into the background pipelines
# it runs through sh -c, so each path must stay one literal word. With a
# private TMPDIR whose name holds a command substitution, a space and single
# quotes, a -j -x run still replicates every dataset, runs nothing embedded
# in the name and leaves the TMPDIR empty.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the directory name
test_hostile_tmpdir_is_used_literally_by_discovery_pipelines() {
	planning_setup_parallel_jobs_env hostile_tmpdir
	l_hostile_tmpdir="$CASE_DIR/tmp \$(touch hostile-marker) 'q'"
	mkdir -m 700 "$l_hostile_tmpdir" || fail "Unable to create the hostile TMPDIR."
	l_hostile_zxfer=$(cd "${ZXFER_TEST_ZXFER_BIN%/*}" && pwd)/${ZXFER_TEST_ZXFER_BIN##*/}

	# A substitution that ran would touch the marker in zxfer's working
	# directory, so run from the case directory.
	(
		cd "$CASE_DIR" || exit 1
		ZXFER_TEST_ZXFER_BIN=$l_hostile_zxfer
		TMPDIR=$l_hostile_tmpdir
		export TMPDIR
		planning_run_zxfer "$STATE_DIR" -j 2 -x no-such-dataset -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	l_run_status=$?
	assertEquals "a hostile TMPDIR must not break the run; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	for l_hostile_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_hostile_suffix"
	done
	assertFalse "no command substitution in the TMPDIR name may run" \
		"[ -e '$CASE_DIR/hostile-marker' ]"
	assertEquals "the run must leave the TMPDIR empty" \
		"" "$(ls -A "$l_hostile_tmpdir")"
}

# Invariant (-j -V): -V reports the recursive delta exactly: the summary
# counts the three missing snapshots (the -j listing's success sentinel is
# stripped, never counted), names each queued dataset and dumps both delta
# directions; the profile counts the proof's and the full listing and the one
# parallel fan-out; and parallel gets the requested job count, line
# buffering and the bare depth-1 zfs runner.
test_parallel_jobs_very_verbose_reports_the_delta_profile_and_job_count() {
	planning_setup_parallel_jobs_env verbose_parallel
	planning_log_mock_parallel_argv

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 2 -V -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-j -V must replicate the tree; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the delta summary must count exactly the missing snapshots" \
		"Recursive snapshot delta summary: source_missing_snapshots=3 destination_extra_snapshots=0 source_datasets=3 destination_extra_datasets=0" \
		"$(grep '^Recursive snapshot delta summary' "$CASE_DIR/zxfer.stdout")"
	assertEquals "-v must name every queued dataset" \
		"Recursive source datasets queued for transfer:
  $ZXFER_MOCKBIN_SOURCE_ROOT
  $ZXFER_MOCKBIN_SOURCE_ROOT/child1
  $ZXFER_MOCKBIN_SOURCE_ROOT/child2" \
		"$(sed -n '/^Recursive source datasets queued/,/^  .*child2$/p' "$CASE_DIR/zxfer.stdout")"
	for l_verbose_heading in \
		"====== Snapshots present in source but missing in destination ======" \
		"====== Extra Destination snapshots not in source ======"; do
		assertTrue "-V must dump the delta under: $l_verbose_heading" \
			"grep -Fqx '$l_verbose_heading' '$CASE_DIR/zxfer.stdout'"
	done
	for l_verbose_counter in source_snapshot_list_commands=2 \
		source_snapshot_list_parallel_commands=1 bucket_source_inspection=1; do
		assertTrue "-V must report $l_verbose_counter; stderr: $(grep '^zxfer profile: ' "$CASE_DIR/zxfer.stderr")" \
			"grep -Fqx 'zxfer profile: $l_verbose_counter' '$CASE_DIR/zxfer.stderr'"
	done
	assertEquals "parallel must run once, with the job count, line buffering and the bare zfs runner" \
		"-j 2 --line-buffer -- '$MOCKBIN_DIR/zfs' 'list' '-H' '-o' 'name,guid' '-s' 'creation' '-d' '1' '-t' 'snapshot' {}" \
		"$(cat "$PARALLEL_ARGV_LOG")"
	planning_assert_no_mutations
	planning_assert_no_parallel_job_leftovers
}

# Invariant (full discovery's destination listing, run here under -N, where
# the no-op proof does not run): a successful listing passes its warnings on
# and needs no existence probe. A failed one is classified by an exact probe
# of the root whatever its stderr names: a missing child in that stderr is
# not a missing root, so the run fails closed and shows the stderr. A local
# listing that exits 255 is probed too; only an ssh failure under -T skips
# the probe.
test_nonrecursive_destination_listing_diagnostics_are_passed_on() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" nonrecursive_listing
	l_listing_key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_probe_line="list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"

	planning_fail_canned_zfs_after_output "$l_listing_key" 0 \
		"zfs: warning: listing is from a degraded pool"
	planning_run_zxfer "$STATE_DIR" -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "a listing with a warning must succeed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertContains "a successful listing must pass its warning on" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "zfs: warning: listing is from a degraded pool"
	assertFalse "a successful listing needs no existence probe" \
		"grep -Fqx '$l_probe_line' '$ZFS_LOG'"
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"

	for l_listing_case in \
		"cannot open '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1': dataset does not exist|1" \
		"|255"; do
		l_listing_stderr=${l_listing_case%|*}
		rm -rf "$CASE_DIR/fail_calls"
		mkdir "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
		: >"$ZFS_LOG"
		(
			MOCK_FAIL_TOOL=zfs
			MOCK_FAIL_CALL=1
			MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
			MOCK_FAIL_MATCH=$l_listing_key
			MOCK_FAIL_STATUS=${l_listing_case##*|}
			export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
				MOCK_FAIL_STATUS
			if [ -n "$l_listing_stderr" ]; then
				MOCK_FAIL_STDERR=$l_listing_stderr
				export MOCK_FAIL_STDERR
			fi
			planning_run_zxfer "$STATE_DIR" -N \
				"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		)
		assertEquals "a failed listing must keep its status [status:${l_listing_case##*|}]" \
			"${l_listing_case##*|}" $?
		planning_assert_log_has_line "$l_probe_line"
		planning_assert_failure_report "snapshot discovery" \
			"Failed to retrieve snapshot list from the destination."
		if [ -n "$l_listing_stderr" ]; then
			assertContains "the listing's stderr must reach the operator" \
				"$(cat "$CASE_DIR/zxfer.stderr")" "$l_listing_stderr"
		fi
		planning_assert_no_mutations
		planning_assert_no_send_receive
	done
}

# Invariant (trailing-slash source): "SRC/" replicates SRC's contents into the
# destination itself, so discovery lists the destination argument rather than
# a child named after the source, and rewrites its records to source paths: a
# matching tree is a proven no-op, and -N sends into the destination itself.
test_trailing_slash_source_maps_onto_the_destination_itself() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/noop" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_run_status=$?
	assertEquals "a trailing-slash no-op must succeed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "the proof must cost exactly the two identity listings; zfs log: $(cat "$ZFS_LOG")" \
		2 "$(wc -l <"$ZFS_LOG" | tr -d ' ')"
	planning_assert_no_mutations
	planning_assert_no_send_receive

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/incremental" trailing_slash
	printf 'list -t filesystem,volume -Hr -o name %s\tdst_datasets.list\t0\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the inventory rule."
	planning_run_zxfer "$STATE_DIR" -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_run_status=$?
	assertEquals "a trailing-slash -N run must succeed; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_log_has_line \
		"send -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"
	planning_assert_log_has_line "receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "-N must receive into the destination itself only" \
		1 "$(grep -c '^receive ' "$ZFS_LOG")"
	planning_assert_no_mutations
}

# Invariant (-x): an exclude pattern awk cannot compile stops the run before
# any change, whether the no-op proof's filters meet it (-R) or full
# discovery's filter does before it diffs (-N). Which of the proof's checks
# reports it depends on awk's exit status, so -R pins only the stage.
test_invalid_exclude_pattern_fails_closed_before_any_change() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -x '[' -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "an invalid pattern must fail the proof" 0 $?
	planning_assert_failure_report "snapshot discovery" "message: Failed to "
	planning_assert_no_mutations
	planning_assert_no_send_receive

	: >"$ZFS_LOG"
	planning_run_zxfer "$FIXTURE_DIR/incremental" -x '[' -N \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertNotEquals "an invalid pattern must fail full discovery" 0 $?
	planning_assert_failure_report "snapshot discovery" \
		"Failed to filter source snapshots against exclude patterns for recursive delta planning."
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

. "$SHUNIT2_BIN"
