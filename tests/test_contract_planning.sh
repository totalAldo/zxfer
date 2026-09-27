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
#   -T destination discovery (ordinary listings over the target master)
#       test_remote_target_destination_listing_failure_fails_closed
#       → a failed snapshot listing on the -T host keeps the zfs exit status
#         and the local "Failed to retrieve snapshot list" report.
#       test_remote_target_ssh_failure_during_discovery_fails_closed
#       → an ssh failure of that listing exits 255 with a report.
#       test_remote_target_bootstraps_a_missing_destination_root
#       → a missing -T root whose pool the live probe lists is bootstrapped.
#       test_remote_target_missing_root_with_an_unlistable_pool_fails_closed
#       → the same root with an unlistable pool fails closed.
#
#   -T remote destination, -P property pass (role-routing fix, 2026-09)
#       test_remote_target_property_pass_reads_destination_properties_over_ssh
#       → every destination-side `zfs get` crosses the ssh transport and no
#         source-side `zfs get` does, even though zfs resolves to the same
#         path on both "hosts"; zero MUTATE / send / receive argv.
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
#   -k / -e property backup metadata contract (pinned 2026-09)
#       test_backup_mode_writes_metadata_once_per_run
#       → `-k -P` writes ZXFER_BACKUP_DIR/<source>/.zxfer_backup_info.v2/h/
#         <identity-chunks>/.zxfer_backup_info.v2 (mode 0600, versioned
#         header, one relative row per dataset) plus the forwarded
#         provenance alias under the destination root, with exactly ONE
#         rename per file for the whole run (no per-dataset rewrites).
#       test_backup_mode_second_run_rewrites_metadata_in_place
#       → a second run replaces the file through one rename again and
#         leaves no stage files behind.
#       test_backup_mode_refuses_symlinked_backup_directory_and_target
#       → a symlinked ZXFER_BACKUP_DIR and a symlinked target file are both
#         refused with a non-zero exit and the link target untouched.
#       test_restore_mode_rejects_legacy_layout_and_unsupported_format_version
#       → `-e` fails closed before any zfs argv when only a legacy flat
#         layout exists or the header declares an unsupported version.
#       test_restore_mode_applies_recorded_properties
#       → `-e` reads the exact-pair file and `MUTATE set`s the recorded
#         value even when the live source has drifted.
#       test_remote_target_backup_mode_writes_metadata_through_ssh
#       → `-T -k` publishes both files through ONE rollback-capable pair
#         write script over the ssh transport, with the same layout and mode.
#       test_restore_mode_reads_a_current_file_under_the_retired_cksum_name
#       → `-e` still restores from the read-only retired name
#         .zxfer_backup_info.<tail>.k<cksum>.<length>.
#       test_backup_mode_refuses_a_v1_forwarded_alias_under_the_retired_name
#       → a v1 forwarded alias at the retired name fails `-k` closed and
#         writes nothing.
#       test_backup_mode_forwards_an_alias_below_the_source_root
#       → a forwarded alias for a child dataset supplies that child's row.
#       test_remote_origin_backup_mode_forwards_an_alias_below_the_source_root
#       → with -O the same alias is found through one listing of the
#         origin's storage directories.
#       test_dry_run_backup_mode_previews_only_the_backup_root
#       → `-n -v -k -P` prints the mkdir/chmod preview and the no-data
#         note, issues no zfs argv, and creates nothing.
#       test_backup_mode_failure_partway_keeps_the_previous_files
#       → a `-k` run that fails at a later dataset leaves the previous
#         complete files byte-identical.
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
		exec "$ZXFER_ROOT/zxfer" -j 2 -R \
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

# Invariant: -V must not change replication outcomes, and an ssh without
# control-socket support still replicates over direct connections, -T
# destination discovery included. Regression for -T destination discovery
# aborting under -V because a profiling recorder's non-zero status leaked into
# a discovery function's return value (zxfer_profile_record_zfs_call returned
# 1 for destination-side calls). The minimal mock ssh rejects -M.
test_remote_target_discovery_succeeds_with_very_verbose() {
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
	assertTrue "the -T destination snapshot listing must have run over ssh" \
		"grep -q \"'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'\" '$SSH_LOG'"
	for l_remote_noop_role in origin target; do
		assertContains "-V must explain the direct-connection fallback for the $l_remote_noop_role host" \
			"$(cat "$CASE_DIR/zxfer.stderr")" \
			"ssh client does not support control sockets; continuing without connection reuse for $l_remote_noop_role host."
	done
	assertFalse "without control-socket support no command may name a socket" \
		"grep -q -- '-S ' '$SSH_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: a clean remote-origin pull no-op opens the origin's ssh control
# master before its first remote command, runs the capability probe and the
# source listing over that socket, probes exactly ONCE, and closes the master
# once at exit.
test_remote_origin_pull_noop_opens_master_first_and_probes_once() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
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
	planning_assert_ssh_commands_multiplexed 1
	assertTrue "the origin master must use the origin role socket" \
		"grep -q -- '-M -S [^ ]*/ssh-origin.sock -fN localhost' '$SSH_LOG'"
	assertEquals "a warmed origin host must cost exactly one capability probe round trip" \
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertTrue "the source listing must run over the origin master" \
		"grep -q -- 'ssh-origin.sock localhost .*snapshot' '$SSH_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: a clean -T push no-op opens the target's master before its first
# remote command, runs the capability probe and the destination snapshot
# listing over it, lists no destination dataset inventory (nothing on a no-op
# reads it), and closes the master once at exit.
test_remote_target_push_noop_opens_master_first_and_probes_once() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_push_noop.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_push_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-T push no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_push_noop_status"
	planning_assert_ssh_commands_multiplexed 1
	assertTrue "the target master must use the target role socket" \
		"grep -q -- '-M -S [^ ]*/ssh-target.sock -fN localhost' '$SSH_LOG'"
	assertEquals "the target host must cost exactly one capability probe round trip" \
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertEquals "the destination snapshot listing must run once over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"
	assertEquals "a no-op must not list the destination dataset inventory" \
		0 "$(grep -c -- "'filesystem,volume'" "$SSH_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Invariant: with distinct -O and -T host specs each role opens its own master
# before any remote command and sends every command over its own socket; each
# master closes once at exit. A -T spec equal to the -O spec shares the origin
# master, since commands for that spec already use the origin socket.
test_remote_origin_and_target_noop_open_one_master_per_host_spec() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_both_noop.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -T 127.0.0.1 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_both_noop_status=$?

	assertEquals "-O -T no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_both_noop_status"
	planning_assert_ssh_commands_multiplexed 2
	assertEquals "each host must cost exactly one capability probe round trip" \
		2 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertFalse "origin commands must never use the target socket" \
		"grep -q -- 'ssh-target.sock localhost' '$SSH_LOG'"
	assertFalse "target commands must never use the origin socket" \
		"grep -q -- 'ssh-origin.sock 127.0.0.1' '$SSH_LOG'"
	assertEquals "the destination snapshot listing must run once over the target master" \
		1 "$(grep -c -- "ssh-target.sock 127.0.0.1 .*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"

	: >"$SSH_LOG"
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_same_noop_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O -T no-op to one host spec must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_same_noop_status"
	planning_assert_ssh_commands_multiplexed 1
	assertFalse "one host spec must not open a second master" \
		"grep -q -- 'ssh-target.sock' '$SSH_LOG'"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# Purpose: Run a -T localhost push of STATE_DIR through the fault-injecting
# socket-aware mock ssh, logging ssh calls to $CASE_DIR/ssh.log (SSH_LOG).
# Usage: planning_run_remote_target_push; sets PLANNING_RUN_STATUS. Export
# any MOCK_FAIL_* variables first: a prefix assignment on a function call is
# not exported on FreeBSD sh.
planning_run_remote_target_push() {
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh.log"
	: >"$SSH_LOG"
	MOCK_SSH_LOG=$SSH_LOG
	export MOCK_SSH_LOG
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	PLANNING_RUN_STATUS=$?
	unset MOCK_SSH_LOG
}

# Purpose: Make the -T destination root and its mapped datasets answer like
# missing datasets, while the pool rule is left to the caller. The recursive
# snapshot listing fails, each exact probe prints zfs's missing-dataset line
# (probes read stdout and stderr together) and the dataset inventory fails
# once with that line on stderr, through the zfs fault injector.
# Usage: planning_make_remote_destination_root_missing; exports the MOCK_FAIL_*
# variables, which the caller unsets.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the zfs message
planning_make_remote_destination_root_missing() {
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 1
	for l_missing_suffix in "" /child1 /child2; do
		l_missing_dataset=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_missing_suffix
		l_missing_fixture="missing_${l_missing_suffix#/}.list"
		printf "cannot open '%s': dataset does not exist\n" "$l_missing_dataset" \
			>"$STATE_DIR/$l_missing_fixture" ||
			fail "Unable to write the missing-dataset fixture."
		# The first matching rule wins, so these go first.
		{
			printf 'list -H %s\t%s\t1\n' "$l_missing_dataset" "$l_missing_fixture"
			cat "$STATE_DIR/manifest"
		} >"$STATE_DIR/manifest.new" ||
			fail "Unable to prepend the missing-dataset rule."
		mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
			fail "Unable to install the missing-dataset rule."
	done
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	MOCK_FAIL_TOOL=zfs
	MOCK_FAIL_CALL=1
	MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
	MOCK_FAIL_MATCH="list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT"
	MOCK_FAIL_STDERR="cannot open '$ZXFER_MOCKBIN_DEST_ROOT': dataset does not exist"
	MOCK_FAIL_STATUS=1
	export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
}

# Invariant (-T discovery): a failed destination snapshot listing on the -T
# host fails closed like a local one: the zfs exit status, the snapshot
# discovery stage report, and zero mutating or send/receive argv.
test_remote_target_destination_listing_failure_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" remote_dstsnapfail
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 2

	planning_run_remote_target_push
	assertEquals "a failed -T destination snapshot listing must keep the zfs exit status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		2 "$PLANNING_RUN_STATUS"
	assertEquals "the listing must have run over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshot list from the destination."
}

# Invariant (-T discovery): when the ssh call carrying the destination
# snapshot listing fails, the run stops with ssh's exit status 255 and ssh's
# diagnostic, sends no existence probe over that connection (a lost one would
# turn the status into the probe's 1), and changes nothing.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the ssh argv glob
test_remote_target_ssh_failure_during_discovery_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" remote_sshfail
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	MOCK_FAIL_TOOL=ssh
	MOCK_FAIL_CALL=1
	MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
	MOCK_FAIL_MATCH="*'list' '-Hr' '-o' 'name,guid' '-t' 'snapshot' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'"
	export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH

	planning_run_remote_target_push
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH
	assertEquals "an ssh failure during -T discovery must exit with ssh's status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		255 "$PLANNING_RUN_STATUS"
	assertEquals "exactly the listing's ssh call must have failed" \
		1 "$(grep -c '^fail	' "$SSH_LOG")"
	assertEquals "no existence probe may follow an undelivered listing" \
		0 "$(grep -c -- "'list' '-H' '$ZXFER_MOCKBIN_DEST_MAPPED_ROOT'" "$SSH_LOG")"
	assertContains "ssh's diagnostic must reach stderr" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "Connection to localhost closed by remote host."
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Failed to retrieve snapshot list from the destination."
}

# Invariant (-T discovery): a missing -T destination root is bootstrapped
# only after the live pool probe, run over the target master, lists its pool;
# every dataset is then received.
test_remote_target_bootstraps_a_missing_destination_root() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" remote_missing_root
	l_missing_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	printf '%s\n' "$l_missing_pool" >"$STATE_DIR/dst_pool.list"
	printf 'list -H -o name %s\tdst_pool.list\t0\n' "$l_missing_pool" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the pool rule."
	planning_make_remote_destination_root_missing

	planning_run_remote_target_push
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
	assertEquals "a missing -T root whose pool exists must be bootstrapped; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$PLANNING_RUN_STATUS"
	assertEquals "the pool probe must run once, over the target master" \
		1 "$(grep -c -- "ssh-target.sock localhost .*'list' '-H' '-o' 'name' '$l_missing_pool'" "$SSH_LOG")"
	for l_missing_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_missing_suffix"
	done
	planning_assert_no_mutations
	assertNotContains "a bootstrap must not report a failure" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "zxfer: failure report begin"
}

# Invariant (-T discovery): a missing -T destination root whose pool cannot be
# listed fails closed in discovery, before any send or receive.
test_remote_target_missing_root_with_an_unlistable_pool_fails_closed() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" remote_missing_pool
	l_missing_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	printf 'list -H -o name %s\t-\t2\n' "$l_missing_pool" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the pool rule."
	planning_make_remote_destination_root_missing

	planning_run_remote_target_push
	unset MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
	assertEquals "an unlistable -T pool must keep the pool probe's status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		2 "$PLANNING_RUN_STATUS"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_failure_report "snapshot discovery" \
		"Destination dataset [$ZXFER_MOCKBIN_DEST_ROOT] is missing and destination pool [$l_missing_pool] could not be listed"
}

# Invariant: when the origin's control master cannot be opened the run fails
# closed before any other remote command or zfs call, with ssh's diagnostic
# and the structured socket error, and leaves no master to close.
test_remote_master_open_failure_fails_closed_before_any_remote_command() {
	planning_setup_env
	SSH_LOG="$CASE_DIR/ssh_master_failure.log"
	: >"$SSH_LOG"
	cat >"$MOCKBIN_DIR/ssh" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
for l_arg in "$@"; do
	[ "$l_arg" = -S ] || continue
	printf '%s\n' 'ssh: connect to host localhost port 22: Connection refused' >&2
	exit 255
done
exit 0
EOF
	chmod +x "$MOCKBIN_DIR/ssh"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/incremental" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_master_failure_status=$?
	unset MOCK_SSH_LOG

	assertEquals "a failed master open must fail the run" 1 "$l_master_failure_status"
	planning_assert_failure_report "cli validation" \
		"Error creating ssh control socket for origin host."
	assertContains "ssh's own diagnostic must reach the operator" \
		"$(cat "$CASE_DIR/zxfer.stderr")" "Connection refused"
	assertEquals "only the support probe and the master open may reach ssh" \
		"-M -V
-o BatchMode=yes -o StrictHostKeyChecking=yes -M -S" \
		"$(sed 's/ -M -S .*/ -M -S/' "$SSH_LOG")"
	assertFalse "no zfs command may run" "[ -s '$ZFS_LOG' ]"
}

# Invariant: an invalid ZXFER_SSH_* policy fails the run at startup with the
# policy diagnostic and a structured report even without -V, before any ssh
# connection or zfs call.
test_remote_invalid_ssh_policy_fails_at_startup_without_very_verbose() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_invalid_policy.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"
	export ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative_known_hosts

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$FIXTURE_DIR/noop" -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_invalid_policy_status=$?
	unset MOCK_SSH_LOG ZXFER_SSH_USER_KNOWN_HOSTS_FILE

	assertEquals "an invalid ssh policy must fail the run" 1 "$l_invalid_policy_status"
	planning_assert_failure_report "cli validation" \
		"ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
	assertEquals "only the control-socket support probe may reach ssh" \
		"-M -V" "$(cat "$SSH_LOG")"
	assertFalse "no zfs command may run" "[ -s '$ZFS_LOG' ]"
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
		1 "$(planning_count_remote_script_marker 'ZXFER_REMOTE_CAPS_V2')"
	assertEquals "the per-run ssh control master must be closed exactly once at exit" \
		1 "$(grep -c -- ' -O exit ' "$SSH_LOG")"
	l_master_socket=$(awk '/ -M /{for (i=1;i<NF;i++) if ($i=="-S") {print $(i+1); exit}}' "$SSH_LOG")
	assertNotNull "the master open must carry a -S control socket path" "$l_master_socket"
	l_multiplexed=$(grep -c -- "-S $l_master_socket" "$SSH_LOG")
	assertTrue "remote send commands must multiplex over the one opened master socket" \
		"[ ${l_multiplexed:-0} -ge 2 ]"
	planning_assert_no_mutations
}

# Invariant: with a remote destination (-T) every destination-side property
# read of the -P pass crosses the ssh transport and no source-side read does.
# Regression for the zfs command dispatcher routing by comparing the
# requested binary path against the source path first: with zfs installed at
# the same path on both hosts (as here, where both "hosts" resolve the one
# canned zfs) every destination `zfs get` silently ran against the LOCAL
# pool, so the property diff compared the source with itself.
test_remote_target_property_pass_reads_destination_properties_over_ssh() {
	planning_setup_env
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	planning_clone_state "$FIXTURE_DIR/noop" remote_props
	planning_add_property_transfer_fixtures
	SSH_LOG="$CASE_DIR/ssh_remote_props.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_zxfer "$STATE_DIR" -T localhost -P -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_remote_props_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-T -P no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_props_status"
	planning_assert_property_reads_routed_by_side
	planning_assert_no_mutations
	planning_assert_no_send_receive
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
# received into; the others replicate normally.
test_exclude_option_skips_matching_child() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -x child1 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-x run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	for l_exclude_suffix in "" /child2; do
		planning_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_exclude_suffix"
	done
	assertEquals "the excluded child must never be sent or received" \
		0 "$(grep -E '^(send|receive) ' "$ZFS_LOG" | grep -c 'child1')"
	assertEquals "the two remaining datasets are each sent once" \
		2 "$(grep -c '^send ' "$ZFS_LOG")"
	planning_assert_no_mutations
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
# -k / -e property backup metadata contract. planning_backup_metadata_file
# (tests/helpers/blackbox.sh) derives the documented layout independently.

# Invariant (-k write-once): one live `-k -P` run over three datasets writes
# the exact-pair file and the forwarded alias exactly once each: two `mv`
# spawns for the whole run. The pre-2026-09 per-dataset flush cost 14+ mv
# and mktemp spawns on the same fixture.
test_backup_mode_writes_metadata_once_per_run() {
	planning_setup_backup_env k_write

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "each metadata file is published by exactly one rename per run" \
		2 "$(grep -c '^mv$' "$SPAWN_LOG")"
	case "$(ls -ldn "$BACKUP_ROOT")" in
	drwx------*) ;;
	*) fail "the backup root zxfer creates must be mode 0700: $(ls -ldn "$BACKUP_ROOT")" ;;
	esac
}

# Invariant (-k rewrite): a second run over the same pair replaces both files
# through one rename each and keeps the single-row-per-dataset contract.
test_backup_mode_second_run_rewrites_metadata_in_place() {
	planning_setup_backup_env k_rewrite

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "first -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	printf '#stale-marker\n' >>"$PRIMARY_FILE"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "second -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "the rewrite is again one rename per file" \
		2 "$(grep -c '^mv$' "$SPAWN_LOG")"
	assertFalse "the rewrite must replace the previous file rather than append to it" \
		"grep -q '#stale-marker' '$PRIMARY_FILE'"
}

# Invariant (symlink guards): a symlinked ZXFER_BACKUP_DIR is refused before
# anything is written into its target, and a symlink planted at the exact
# metadata path is refused without following it.
test_backup_mode_refuses_symlinked_backup_directory_and_target() {
	planning_setup_backup_env k_symlink
	l_real_root="$CASE_DIR/backup_real"
	mkdir -p "$l_real_root"
	BACKUP_ROOT="$CASE_DIR/backup_link"
	ln -s "$l_real_root" "$BACKUP_ROOT"

	planning_run_backup_zxfer -k -P
	assertNotEquals "a symlinked backup directory must fail the run" 0 $?
	grep -q "Refusing to use backup directory" "$CASE_DIR/zxfer.stderr" ||
		fail "expected the symlinked backup directory refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "nothing may be written through the symlinked root" \
		"" "$(find "$l_real_root" -type f)"
	planning_assert_no_mutations

	BACKUP_ROOT="$CASE_DIR/backup"
	PRIMARY_FILE=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT")
	l_decoy="$CASE_DIR/decoy"
	printf 'decoy\n' >"$l_decoy"
	mkdir -p "${PRIMARY_FILE%/*}"
	chmod 700 "${PRIMARY_FILE%/*}"
	ln -s "$l_decoy" "$PRIMARY_FILE"

	planning_run_backup_zxfer -k -P
	assertNotEquals "a symlinked metadata target must fail the run" 0 $?
	grep -q "Refusing to write backup metadata" "$CASE_DIR/zxfer.stderr" ||
		fail "expected the symlinked target refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the symlink target must be untouched" "decoy" "$(cat "$l_decoy")"
	assertTrue "the planted symlink must not be replaced" "[ -L '$PRIMARY_FILE' ]"
	assertEquals "no rename may run when the target is refused" \
		0 "$(grep -c '^mv$' "$SPAWN_LOG")"
}

# Invariant (-e fail-closed reads): a legacy flat-layout file is never
# consulted and an unsupported #format_version is rejected, both before any
# zfs argv is issued and with zero MUTATE lines.
test_restore_mode_rejects_legacy_layout_and_unsupported_format_version() {
	planning_setup_backup_env e_reject
	mkdir -p "$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT"
	printf '%s\n%s\n%s\n' "#zxfer property backup file" "#format_version:1" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT,$ZXFER_MOCKBIN_DEST_MAPPED_ROOT,compression=lz4" \
		>"$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data"
	chmod 600 "$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data"

	planning_run_backup_zxfer -e
	assertNotEquals "-e with only a legacy layout must fail" 0 $?
	grep -q "Cannot find backup property file" "$CASE_DIR/zxfer.stderr" ||
		fail "expected the missing-backup error; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the restore must fail before any zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	sed 's/^#format_version:2$/#format_version:999/' "$PRIMARY_FILE" >"$CASE_DIR/bad_version"
	cat "$CASE_DIR/bad_version" >"$PRIMARY_FILE"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -e
	assertNotEquals "-e with an unsupported format version must fail" 0 $?
	grep -q "does not declare supported zxfer backup metadata format version #format_version:2" \
		"$CASE_DIR/zxfer.stderr" ||
		fail "expected the unsupported-version error; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the rejected restore must fail before any zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
}

# Invariant (-e restore): the source side reads the exact-pair file and the
# plan applies the RECORDED value (compression=lz4) even though the live
# source and destination both report gzip now.
test_restore_mode_applies_recorded_properties() {
	planning_setup_backup_env e_restore

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	l_drifted_rows=$(planning_property_rows_with "$(planning_property_default_rows)" \
		compression gzip local)
	planning_add_property_fixtures_for_rows "$l_drifted_rows" "$l_drifted_rows" \
		"$l_drifted_rows" "$l_drifted_rows"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -e
	l_run_status=$?
	assertEquals "-e must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	for l_restore_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set compression=lz4 $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_restore_suffix"
	done
	assertEquals "the restore sets exactly the recorded property on each dataset" \
		3 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "the drifted live value must never be applied" \
		"grep -q 'compression=gzip' '$ZFS_LOG'"
	planning_assert_no_send_receive
}

# Invariant (-T -k): with a remote destination both metadata files are
# published together through one ssh write script, and land
# with the same layout and 0600 mode (the mock ssh runs the rendered script
# locally through `sh -c`).
test_remote_target_backup_mode_writes_metadata_through_ssh() {
	planning_setup_backup_env k_remote
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_backup.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_backup_zxfer -T localhost -k -P
	l_remote_backup_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-T -k -P no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_backup_status"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "one remote write script publishes the metadata pair" \
		1 "$(planning_count_remote_script_marker '.zxfer-backup-write')"
}

# Invariant (-e retired name): a current-format file under the retired name
# .zxfer_backup_info.<tail>.k<cksum>.<length>, where cksum hashes
# "srcpool/data<LF>dstpool/back" with no final newline, still restores.
test_restore_mode_reads_a_current_file_under_the_retired_cksum_name() {
	planning_setup_backup_env e_retired
	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	mv "$PRIMARY_FILE" "$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data.k1095302263.25" ||
		fail "Unable to rename the metadata file to its retired name."
	l_drifted_rows=$(planning_property_rows_with "$(planning_property_default_rows)" \
		compression gzip local)
	planning_add_property_fixtures_for_rows "$l_drifted_rows" "$l_drifted_rows" \
		"$l_drifted_rows" "$l_drifted_rows"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -e
	l_run_status=$?
	assertEquals "-e must restore from the retired name; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	for l_restore_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set compression=lz4 $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_restore_suffix"
	done
	assertFalse "restore never writes the current name" "[ -e '$PRIMARY_FILE' ]"
}

# Invariant (-k retired alias): a version-1 forwarded alias under the retired
# name (cksum of "srcpool/data<LF>srcpool/data") fails the chained -k run
# closed instead of silently recording live properties.
test_backup_mode_refuses_a_v1_forwarded_alias_under_the_retired_name() {
	planning_setup_backup_env k_retired_alias
	l_retired_alias="$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data.k2770155462.25"
	(umask 077 && mkdir -p "${l_retired_alias%/*}") ||
		fail "Unable to create the retired alias directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:1" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT,$ZXFER_MOCKBIN_SOURCE_ROOT,compression=gzip" \
		>"$l_retired_alias"
	chmod 600 "$l_retired_alias"

	planning_run_backup_zxfer -k -P
	assertNotEquals "a v1 alias must fail the -k run" 0 $?
	grep -q "Forwarded backup property file $l_retired_alias does not declare supported zxfer backup metadata format version #format_version:2." \
		"$CASE_DIR/zxfer.stderr" ||
		fail "expected the forwarded version refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertFalse "no metadata may be written" "[ -e '$PRIMARY_FILE' ]"
	assertFalse "no alias may be written" "[ -e '$FORWARDED_FILE' ]"
}

# Invariant (-k chained provenance): an alias left by an earlier hop for a
# child dataset (keyed srcpool/data/child1 on both sides) supplies that
# child's row; datasets it does not cover keep their live properties.
test_backup_mode_forwards_an_alias_below_the_source_root() {
	planning_setup_backup_env k_child_alias
	l_child=$ZXFER_MOCKBIN_SOURCE_ROOT/child1
	l_child_alias=$(planning_backup_metadata_file "$BACKUP_ROOT" "$l_child" "$l_child")
	(umask 077 && mkdir -p "${l_child_alias%/*}") ||
		fail "Unable to create the child alias directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$l_child" "#destination_root:$l_child" \
		".	compression=gzip=local" >"$l_child_alias"
	chmod 600 "$l_child_alias"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertTrue "child1 records the forwarded provenance: $(cat "$PRIMARY_FILE")" \
		"grep -Fxq 'child1	compression=gzip=local' '$PRIMARY_FILE'"
	for l_live_key in . child2; do
		grep -q "^$l_live_key	.*compression=lz4=local" "$PRIMARY_FILE" ||
			fail "row $l_live_key must keep the live properties: $(cat "$PRIMARY_FILE")"
	done
}

# Invariant (-O -k chained provenance): over ssh the same child alias is
# found through ONE listing of the origin's storage directories, and only the
# roots that listing reports are read.
test_remote_origin_backup_mode_forwards_an_alias_below_the_source_root() {
	planning_setup_backup_env k_remote_child_alias
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	l_child=$ZXFER_MOCKBIN_SOURCE_ROOT/child1
	l_child_alias=$(planning_backup_metadata_file "$BACKUP_ROOT" "$l_child" "$l_child")
	(umask 077 && mkdir -p "${l_child_alias%/*}") ||
		fail "Unable to create the child alias directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$l_child" "#destination_root:$l_child" \
		".	compression=gzip=local" >"$l_child_alias"
	chmod 600 "$l_child_alias"
	SSH_LOG="$CASE_DIR/ssh_backup_origin.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_backup_zxfer -O localhost -k -P
	l_remote_origin_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_origin_status"
	assertTrue "child1 records the forwarded provenance: $(cat "$PRIMARY_FILE")" \
		"grep -Fxq 'child1	compression=gzip=local' '$PRIMARY_FILE'"
	grep -q "^\.	.*compression=lz4=local" "$PRIMARY_FILE" ||
		fail "the root row must keep the live properties: $(cat "$PRIMARY_FILE")"
	assertEquals "one listing of the origin's storage directories" \
		1 "$(planning_count_remote_script_marker 'l_listing_dir')"
}

# Purpose: Run a first -k -P hop from SOURCE_ROOT into DEST_ROOT that excludes
# its own source root with -x, so zxfer itself writes the forwarded alias of
# MAPPED_ROOT with child rows (compression=gzip) and no "." row. Restores the
# default fixture roots for the next hop; BACKUP_ROOT stays the case's.
# Usage: planning_run_rootless_backup_hop SOURCE_ROOT DEST_ROOT MAPPED_ROOT
planning_run_rootless_backup_hop() {
	planning_use_fixture_roots "$1" "$2" "$3"
	planning_setup_backup_env rootless_hop
	l_hop_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_hop_rows" \
		"$(planning_property_rows_with "$l_hop_rows" compression gzip local)" \
		"$l_hop_rows" "$l_hop_rows"

	planning_run_backup_zxfer -k -P -x "^$1\$"
	l_run_status=$?
	assertEquals "hop 1 must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertTrue "hop 1 must write the forwarded alias of $3" "[ -f '$FORWARDED_FILE' ]"
	assertFalse "the alias must have no row for its own root: $(cat "$FORWARDED_FILE")" \
		"grep -q '^\.	' '$FORWARDED_FILE'"
	planning_use_fixture_roots "$planning_default_source_root" \
		"$planning_default_dest_root" "$planning_default_dest_mapped_root"
}

# Invariant (-k chained, root excluded below the source root): a hop that
# excluded its own root with -x leaves an alias without a "." row for
# srcpool/data/child1. A later -k of the parent reads it, finds no row for
# child1 there, and records every dataset. Regression: the run stopped at
# child1 with "does not contain a current-format relative row".
test_backup_mode_chains_through_an_alias_without_its_root_row_below_the_source_root() {
	planning_run_rootless_backup_hop qpool/child1 "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/child1"
	planning_setup_backup_env rootless_below

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the chained -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
}

# Invariant (-k chained, root excluded at the source root): the alias of
# srcpool/data itself has no "." row, so the root keeps its live properties
# while child1 and child2 record the forwarded gzip rows.
test_backup_mode_forwards_child_rows_from_an_alias_without_its_root_row() {
	planning_run_rootless_backup_hop qpool/data "${ZXFER_MOCKBIN_SOURCE_ROOT%/*}" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_setup_backup_env rootless_at_root

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the chained -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	grep -q "^\.	.*compression=lz4=local" "$PRIMARY_FILE" ||
		fail "the root row must keep the live properties: $(cat "$PRIMARY_FILE")"
	for l_forwarded_key in child1 child2; do
		grep -q "^$l_forwarded_key	.*compression=gzip=local" "$PRIMARY_FILE" ||
			fail "row $l_forwarded_key must record the forwarded properties: $(cat "$PRIMARY_FILE")"
	done
}

# Invariant (-n -k): a dry run previews only the backup-root preparation;
# with no property pass there is nothing to write, and nothing is created.
test_dry_run_backup_mode_previews_only_the_backup_root() {
	planning_setup_backup_env k_dry_run

	planning_run_backup_zxfer -n -v -k -P
	l_run_status=$?
	assertEquals "-n -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "the preview is the root preparation plus the no-data note" \
		"Dry run: umask 077; 'mkdir' '-p' '$BACKUP_ROOT'; 'chmod' '700' '$BACKUP_ROOT'
No property data collected; skipping backup write." "$(cat "$CASE_DIR/zxfer.stdout")"
	assertEquals "a dry run issues no zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
	assertFalse "a dry run creates no backup root" "[ -e '$BACKUP_ROOT' ]"
}

# Invariant (-k write boundary): rows are published only at run end, so a
# run that fails at a later dataset leaves the previous complete files
# byte-identical and no stage file behind.
test_backup_mode_failure_partway_keeps_the_previous_files() {
	planning_setup_backup_env k_partial
	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the first -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	cp "$PRIMARY_FILE" "$CASE_DIR/primary.before"
	cp "$FORWARDED_FILE" "$CASE_DIR/forwarded.before"

	planning_clone_state "$FIXTURE_DIR/incremental" k_partial_incremental
	planning_add_property_transfer_fixtures
	{
		printf 'receive*child2*\t-\t1\n'
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" ||
		fail "Unable to inject the child2 receive failure."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the rewritten manifest."
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -k -P
	assertNotEquals "the run must fail at child2" 0 $?
	planning_assert_log_has_line "END receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	assertTrue "the primary file is unchanged" "cmp -s '$CASE_DIR/primary.before' '$PRIMARY_FILE'"
	assertTrue "the forwarded alias is unchanged" "cmp -s '$CASE_DIR/forwarded.before' '$FORWARDED_FILE'"
	assertEquals "no stage file is left behind" \
		"" "$(find "$BACKUP_ROOT" -name '.zxfer-backup-*')"
}

. "$SHUNIT2_BIN"
