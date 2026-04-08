#!/bin/sh
#
# shunit2 tests for zxfer_inspect_delete_snap.sh helpers.
#
# shellcheck disable=SC1090,SC2034,SC2317,SC2329

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_inspect_delete_snap.sh"

usage() {
	:
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_inspect_delete.XXXXXX)
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_g_grandfather_protection=""
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_RZFS="/sbin/zfs"
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_actual_dest=""
	g_last_common_snap=""
	g_src_snapshot_transfer_list=""
	g_dest_has_snapshots=0
	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0
	g_destination_snapshot_creation_cache=""
	g_delete_source_tmp_file=$(mktemp "$TEST_TMPDIR/delete_source.XXXXXX")
	g_delete_dest_tmp_file=$(mktemp "$TEST_TMPDIR/delete_dest.XXXXXX")
	g_delete_snapshots_to_delete_tmp_file=$(mktemp "$TEST_TMPDIR/delete_diff.XXXXXX")
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
}

tearDown() {
	rm -f "$g_delete_source_tmp_file" "$g_delete_dest_tmp_file" "$g_delete_snapshots_to_delete_tmp_file"
	zxfer_reset_snapshot_record_indexes
}

test_delete_snaps_returns_when_nothing_needs_deletion() {
	log_file="$TEST_TMPDIR/delete_none.log"
	: >"$log_file"
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")

	(
		run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$log_file"
		}
		delete_snaps "$source_list" "$dest_list"
	)

	assertEquals "No destroy command should run when destination snapshots already match the source." "" "$(cat "$log_file")"
}

test_delete_snaps_dry_run_prints_destroy_command() {
	g_option_n_dryrun=1
	g_option_v_verbose=1
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")

	output=$(delete_snaps "$source_list" "$dest_list")

	assertContains "Dry-run snapshot deletion should print the rendered destroy command." \
		"$output" "Dry run: '/sbin/zfs' 'destroy' 'tank/fs@snap3'"
}

test_delete_snaps_throws_when_destroy_fails() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			delete_snaps "$source_list" "$dest_list"
		)
	)
	status=$?

	assertEquals "Failed destination destroys should abort snapshot deletion." 1 "$status"
	assertContains "Failed destination destroys should use the generic execution error." \
		"$output" "Error when executing command."
}

test_grandfather_test_reports_detailed_context_for_old_snapshots() {
	g_option_g_grandfather_protection=1
	current_epoch=$(date +%s)
	old_epoch=$((current_epoch - 5 * 86400))

	set +e
	output=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			run_destination_zfs_cmd() {
				if [ "$5" = "-p" ]; then
					printf '%s\n' "$old_epoch"
				else
					printf '%s\n' "Sun Jan  1 00:00:00 UTC 2023"
				fi
			}
			grandfather_test "tank/fs@ancient"
		)
	)
	status=$?

	assertEquals "Grandfather protection should fail old snapshot deletions with a usage error." 2 "$status"
	assertContains "Grandfather errors should include the offending snapshot name." \
		"$output" "Snapshot name: tank/fs@ancient"
	assertContains "Grandfather errors should include the computed age." \
		"$output" "Snapshot age : 5 days old"
	assertContains "Grandfather errors should explain how to recover." \
		"$output" "Either amend/remove option g, fix your system date, or manually"
}

test_grandfather_test_allows_recent_snapshots() {
	g_option_g_grandfather_protection=10
	current_epoch=$(date +%s)
	recent_epoch=$((current_epoch - 2 * 86400))
	outfile="$TEST_TMPDIR/grandfather_recent.out"

	run_destination_zfs_cmd() {
		if [ "$5" = "-p" ]; then
			printf '%s\n' "$recent_epoch"
		else
			printf '%s\n' "Mon Jan  1 00:00:00 UTC 2024"
		fi
	}

	grandfather_test "tank/fs@recent" >"$outfile"
	status=$?

	unset -f run_destination_zfs_cmd

	assertEquals "Recent snapshots should pass grandfather protection checks." 0 "$status"
	assertEquals "Passing grandfather checks should not emit output." "" "$(cat "$outfile")"
}

test_grandfather_test_reports_creation_probe_failures() {
	g_option_g_grandfather_protection=1

	set +e
	output=$(
		(
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			run_destination_zfs_cmd() {
				if [ "$5" = "-p" ]; then
					return 1
				fi
				return 0
			}
			grandfather_test "tank/fs@missing"
		) 2>&1
	)
	status=$?

	assertEquals "Grandfather protection should abort when creation-time lookup fails." 1 "$status"
	assertContains "Creation-time lookup failures should not be misreported as ancient snapshots." \
		"$output" "Couldn't determine creation time for destination snapshot tank/fs@missing."
}

test_set_src_snapshot_transfer_list_transfers_all_snapshots_when_no_common_snapshot_exists() {
	g_last_common_snap=""

	set_src_snapshot_transfer_list "tank/fs@snap3 tank/fs@snap2 tank/fs@snap1" "tank/fs"

	expected=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	assertEquals "Without a common snapshot, every source snapshot should be scheduled for transfer." \
		"$expected" "$g_src_snapshot_transfer_list"
}

test_get_last_common_snapshot_matches_exact_names_without_prefix_collisions() {
	outfile="$TEST_TMPDIR/last_common_exact.out"

	get_last_common_snapshot \
		"tank/fs@daily-10 tank/fs@daily-1" \
		"backup/fs@daily-1 backup/fs@daily-11" >"$outfile"

	assertEquals "Last-common detection should match exact snapshot names, not prefixes." \
		"tank/fs@daily-1" "$(cat "$outfile")"
}

test_get_dest_snapshots_to_delete_per_dataset_matches_exact_names_without_prefix_collisions() {
	outfile="$TEST_TMPDIR/delete_exact.out"

	get_dest_snapshots_to_delete_per_dataset \
		"tank/fs@daily-10" \
		"tank/fs@daily-10 tank/fs@daily-1" >"$outfile"

	assertEquals "Destination-only snapshot deletion should match exact names, not prefixes." \
		"tank/fs@daily-1" "$(cat "$outfile")"
}

test_get_dest_snapshots_to_delete_per_dataset_returns_multiple_extra_snapshots_in_input_order() {
	outfile="$TEST_TMPDIR/delete_multiple.out"

	get_dest_snapshots_to_delete_per_dataset \
		"tank/fs@snap1 tank/fs@snap3" \
		"tank/fs@snap1 tank/fs@zeta tank/fs@snap3 tank/fs@alpha" >"$outfile"

	assertEquals "Destination-only snapshot deletion should preserve each extra snapshot in destination order." \
		"tank/fs@zeta
tank/fs@alpha" "$(cat "$outfile")"
}

test_get_dest_snapshots_to_delete_per_dataset_preserves_destination_order_for_guid_divergence() {
	outfile="$TEST_TMPDIR/delete_guid_order.out"

	get_dest_snapshots_to_delete_per_dataset \
		"tank/fs@snap1	111" \
		"tank/fs@snap1	999 tank/fs@alpha	222" >"$outfile"

	assertEquals "Guid-mismatched destination-only snapshots should still be returned in live destination order." \
		"tank/fs@snap1
tank/fs@alpha" "$(cat "$outfile")"
}

test_delete_snaps_runs_grandfather_checks_before_destroying() {
	log_file="$TEST_TMPDIR/delete_grandfather.log"
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	g_option_g_grandfather_protection=7

	grandfather_test() {
		printf 'grandfather %s\n' "$1" >>"$log_file"
	}
	run_destination_zfs_cmd() {
		printf 'destroy %s\n' "$*" >>"$log_file"
		return 0
	}

	delete_snaps "$source_list" "$dest_list"

	unset -f grandfather_test
	unset -f run_destination_zfs_cmd
	# Restore the real helper after replacing it with a test stub.
	# shellcheck source=src/zxfer_inspect_delete_snap.sh
	. "$ZXFER_ROOT/src/zxfer_inspect_delete_snap.sh"

	assertContains "Grandfather protection should be checked for each candidate deletion." \
		"$(cat "$log_file")" "grandfather tank/fs@snap3"
	assertContains "Unprotected snapshots should still be destroyed after grandfather checks pass." \
		"$(cat "$log_file")" "destroy destroy tank/fs@snap3"
	assertEquals "Successful deletions should set the destination-delete flag." 1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_marks_rollback_eligible_when_deleting_newer_snapshots() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"

	run_destination_zfs_cmd() {
		case "$*" in
		"get -H -o value -p creation tank/fs@snap2")
			printf '%s\n' 200
			;;
		"get -H -o value -p creation tank/fs@snap3")
			printf '%s\n' 300
			;;
		"destroy tank/fs@snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	delete_snaps "$source_list" "$dest_list"

	unset -f run_destination_zfs_cmd

	assertEquals "Deleting a destination snapshot newer than the last common snapshot should preserve rollback eligibility." \
		1 "$g_deleted_dest_newer_snapshots"
	assertEquals "Deleting a newer destination snapshot should still mark that a destroy was issued." \
		1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_batches_creation_time_reads_for_rollback_and_grandfather_checks() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3" "tank/fs@snap4")
	log_file="$TEST_TMPDIR/delete_creation_batch.log"
	current_epoch=$(date +%s)
	common_epoch=$((current_epoch - 10 * 86400))
	snap3_epoch=$((current_epoch - 2 * 86400))
	snap4_epoch=$((current_epoch - 86400))
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_option_g_grandfather_protection=999

	run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3 tank/fs@snap4")
			printf 'tank/fs@snap2\t%s\n' "$common_epoch"
			printf 'tank/fs@snap3\t%s\n' "$snap3_epoch"
			printf 'tank/fs@snap4\t%s\n' "$snap4_epoch"
			;;
		"destroy tank/fs@snap4,snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	delete_snaps "$source_list" "$dest_list"

	unset -f run_destination_zfs_cmd

	batch_count=$("${g_cmd_awk:-awk}" '/^get -H -o name,value -p creation / { count++ } END { print count + 0 }' "$log_file")
	single_count=$("${g_cmd_awk:-awk}" '/^get -H -o value -p creation / { count++ } END { print count + 0 }' "$log_file")

	assertEquals "Delete planning should batch creation-time reads once per dataset before rollback and grandfather checks." \
		1 "$batch_count"
	assertEquals "Grandfather and rollback checks should reuse the cached batch results instead of issuing per-snapshot creation reads." \
		0 "$single_count"
	assertEquals "Deleting snapshots newer than the last common point should keep rollback eligibility." \
		1 "$g_deleted_dest_newer_snapshots"
}

test_delete_snaps_falls_back_to_single_creation_probes_when_batch_prefetch_fails() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	log_file="$TEST_TMPDIR/delete_creation_fallback.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"

	run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3")
			return 1
			;;
		"get -H -o value -p creation tank/fs@snap2")
			printf '%s\n' 200
			;;
		"get -H -o value -p creation tank/fs@snap3")
			printf '%s\n' 300
			;;
		"destroy tank/fs@snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	delete_snaps "$source_list" "$dest_list"

	unset -f run_destination_zfs_cmd

	batch_count=$("${g_cmd_awk:-awk}" '/^get -H -o name,value -p creation / { count++ } END { print count + 0 }' "$log_file")
	single_count=$("${g_cmd_awk:-awk}" '/^get -H -o value -p creation / { count++ } END { print count + 0 }' "$log_file")

	assertEquals "Delete planning should still attempt the batched creation-time lookup first." \
		1 "$batch_count"
	assertEquals "If the batched creation-time lookup fails, delete planning should fall back to per-snapshot reads." \
		2 "$single_count"
	assertEquals "Fallback per-snapshot creation reads should preserve rollback eligibility detection." \
		1 "$g_deleted_dest_newer_snapshots"
}

test_delete_snaps_falls_back_to_single_creation_probe_when_batched_creation_value_is_malformed() {
	source_list=$(printf '%s\n%s' "tank/fs@snap1" "tank/fs@snap2")
	dest_list=$(printf '%s\n%s\n%s' "tank/fs@snap1" "tank/fs@snap2" "tank/fs@snap3")
	log_file="$TEST_TMPDIR/delete_creation_malformed.log"
	current_epoch=$(date +%s)
	common_epoch=$((current_epoch - 10 * 86400))
	snap3_epoch=$((current_epoch - 86400))
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_option_g_grandfather_protection=999

	run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3")
			printf 'tank/fs@snap2\t%s\n' "$common_epoch"
			printf 'tank/fs@snap3\tunknown\n'
			;;
		"get -H -o value -p creation tank/fs@snap3")
			printf '%s\n' "$snap3_epoch"
			;;
		"destroy tank/fs@snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	delete_snaps "$source_list" "$dest_list"

	unset -f run_destination_zfs_cmd

	assertContains "Delete planning should still use one batched creation-time lookup attempt first." \
		"$(cat "$log_file")" "get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3"
	assertContains "Malformed batched creation values should be ignored so later checks fall back to an exact single-snapshot read." \
		"$(cat "$log_file")" "get -H -o value -p creation tank/fs@snap3"
	assertEquals "Exact fallback after malformed batched data should still preserve rollback eligibility detection." \
		1 "$g_deleted_dest_newer_snapshots"
}

test_zxfer_prefetch_destination_snapshot_creation_paths_flushes_when_batch_limit_is_reached() {
	log_file="$TEST_TMPDIR/prefetch_creation_batches.log"
	snapshot_records=$(
		"${g_cmd_awk:-awk}" 'BEGIN {
			for (i = 1; i <= 129; i++) {
				printf "tank/fs@snap%d\n", i
			}
		}'
	)

	run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		shift 5
		for l_snapshot_path in "$@"; do
			printf '%s\t100\n' "$l_snapshot_path"
		done
	}

	zxfer_reset_destination_snapshot_creation_cache
	zxfer_prefetch_destination_snapshot_creation_paths "$snapshot_records"

	unset -f run_destination_zfs_cmd

	batch_count=$("${g_cmd_awk:-awk}" '/^get -H -o name,value -p creation / { count++ } END { print count + 0 }' "$log_file")

	assertEquals "Large snapshot sets should flush one full creation-time batch and one trailing batch." \
		2 "$batch_count"
	assertContains "The first batch should include the earliest queued snapshot path." \
		"$g_destination_snapshot_creation_cache" "tank/fs@snap1	100"
	assertContains "The trailing batch should still cache the last snapshot path." \
		"$g_destination_snapshot_creation_cache" "tank/fs@snap129	100"
}

test_zxfer_prefetch_delete_snapshot_creation_times_prefetches_last_common_when_delete_list_is_empty() {
	log_file="$TEST_TMPDIR/prefetch_last_common_only.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/src@snap2"

	run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$log_file"
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2")
			printf 'tank/fs@snap2\t200\n'
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_reset_destination_snapshot_creation_cache
	zxfer_prefetch_delete_snapshot_creation_times ""

	unset -f run_destination_zfs_cmd

	assertContains "An empty delete list should still prefetch the last common destination snapshot when available." \
		"$(cat "$log_file")" "get -H -o name,value -p creation tank/fs@snap2"
	assertContains "The prefetched last common snapshot should be cached for later rollback checks." \
		"$g_destination_snapshot_creation_cache" "tank/fs@snap2	200"
}

test_deleted_snapshots_include_newer_than_last_common_skips_older_only_deletions() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' 200
			;;
		backup/dst@old1)
			printf '%s\n' 100
			;;
		backup/dst@old2)
			printf '%s\n' 150
			;;
		*)
			return 1
			;;
		esac
	}

	if deleted_snapshots_include_newer_than_last_common "$(printf '%s\n%s' "backup/dst@old1" "backup/dst@old2")"; then
		fail "Older-only destination deletions should not require rollback."
	fi
}

test_deleted_snapshots_include_newer_than_last_common_detects_newer_deletions() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' 200
			;;
		backup/dst@old)
			printf '%s\n' 150
			;;
		backup/dst@newer)
			printf '%s\n' 300
			;;
		*)
			return 1
			;;
		esac
	}

	if ! deleted_snapshots_include_newer_than_last_common "$(printf '%s\n%s' "backup/dst@old" "backup/dst@newer")"; then
		fail "Deleting a destination snapshot newer than the last common snapshot should keep rollback eligible."
	fi
}

test_deleted_snapshots_include_newer_than_last_common_treats_unknown_last_common_creation_as_rollback_eligible() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' "unknown"
			;;
		backup/dst@old)
			printf '%s\n' 100
			;;
		*)
			return 1
			;;
		esac
	}

	if ! deleted_snapshots_include_newer_than_last_common "backup/dst@old"; then
		fail "Unknown last-common creation times should fail closed and keep rollback eligible."
	fi
}

test_deleted_snapshots_include_newer_than_last_common_treats_unknown_deleted_creation_as_rollback_eligible() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common"

	run_destination_zfs_cmd() {
		case "$7" in
		backup/dst@common)
			printf '%s\n' 200
			;;
		backup/dst@old)
			printf '%s\n' "unknown"
			;;
		*)
			return 1
			;;
		esac
	}

	if ! deleted_snapshots_include_newer_than_last_common "backup/dst@old"; then
		fail "Unknown deleted-snapshot creation times should fail closed and keep rollback eligible."
	fi
}

test_inspect_delete_snap_marks_destination_empty_when_no_matching_destination_dataset_exists() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3
tank/src@zxfer_2
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/other@zxfer_1
EOF
	)
	g_actual_dest="backup/dst"

	inspect_delete_snap 0 "tank/src"

	expected_transfer=$(printf '%s\n%s' "tank/src@zxfer_2" "tank/src@zxfer_3")
	assertEquals "Missing destination datasets should be reported as having no snapshots." 0 "$g_dest_has_snapshots"
	assertEquals "No destination snapshots should yield an empty last common snapshot." "" "$g_last_common_snap"
	assertEquals "All source snapshots should be transferred when the destination dataset is absent." \
		"$expected_transfer" "$g_src_snapshot_transfer_list"
}

test_inspect_delete_snap_marks_destination_present_when_matching_dataset_exists() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
EOF
	)
	g_actual_dest="backup/dst"

	inspect_delete_snap 0 "tank/src"

	assertEquals "Matching destination datasets should be marked as present." 1 "$g_dest_has_snapshots"
	assertEquals "The most recent common snapshot should be detected from the matching destination list." \
		"tank/src@zxfer_2	222" "$g_last_common_snap"
}

test_inspect_delete_snap_requires_matching_guid_for_common_snapshot_detection() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2	999
backup/dst@zxfer_1	111
EOF
	)
	g_actual_dest="backup/dst"

	inspect_delete_snap 0 "tank/src"

	assertEquals "Same-named but unrelated destination snapshots should not be treated as the common base." \
		"tank/src@zxfer_1	111" "$g_last_common_snap"
	assertEquals "Transfer planning should keep the divergent source snapshot when the destination guid differs." \
		"tank/src@zxfer_2	222
tank/src@zxfer_3	333" "$g_src_snapshot_transfer_list"
}

test_inspect_delete_snap_fetches_identity_records_when_name_only_lists_overlap() {
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3
tank/src@zxfer_2
tank/src@zxfer_1
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
EOF
	)
	g_actual_dest="backup/dst"

	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset() {
				if [ "$1:$2" = "source:tank/src" ]; then
					printf '%s\n' \
						"tank/src@zxfer_3	333" \
						"tank/src@zxfer_2	222" \
						"tank/src@zxfer_1	111"
					return 0
				fi
				if [ "$1:$2" = "destination:backup/dst" ]; then
					printf '%s\n' \
						"backup/dst@zxfer_2	999" \
						"backup/dst@zxfer_1	111"
					return 0
				fi
				return 1
			}

			inspect_delete_snap 0 "tank/src"
			printf 'last=%s\n' "$g_last_common_snap"
			printf 'transfer=%s\n' "$g_src_snapshot_transfer_list"
		)
	)

	assertContains "Name-only overlapping snapshot sets should still use guid validation to find the real common base." \
		"$output" "last=tank/src@zxfer_1	111"
	assertContains "Name-only overlapping snapshot sets should still keep the divergent source snapshot queued for transfer." \
		"$output" "transfer=tank/src@zxfer_2
tank/src@zxfer_3"
}

test_inspect_delete_snap_invokes_delete_path_when_requested() {
	log_file="$TEST_TMPDIR/inspect_delete.log"
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/src@zxfer_3
tank/src@zxfer_2
tank/src@zxfer_1
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
backup/dst@zxfer_2
backup/dst@zxfer_1
backup/dst@old_only
EOF
	)
	g_actual_dest="backup/dst"

	(
		delete_snaps() {
			printf 'source=%s\n' "$1" >"$log_file"
			printf 'dest=%s\n' "$2" >>"$log_file"
		}
		zxfer_get_snapshot_identity_records_for_dataset() {
			if [ "$1:$2" = "source:tank/src" ]; then
				printf '%s\n' \
					"tank/src@zxfer_3	333" \
					"tank/src@zxfer_2	222" \
					"tank/src@zxfer_1	111"
				return 0
			fi
			if [ "$1:$2" = "destination:backup/dst" ]; then
				printf '%s\n' \
					"backup/dst@zxfer_2	222" \
					"backup/dst@zxfer_1	111" \
					"backup/dst@old_only	999"
				return 0
			fi
			return 1
		}
		inspect_delete_snap 1 "tank/src"
	)

	assertContains "inspect_delete_snap should pass the filtered source snapshot list into delete_snaps." \
		"$(cat "$log_file")" "source=tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111"
	assertContains "inspect_delete_snap should pass the matching destination dataset snapshots into delete_snaps." \
		"$(cat "$log_file")" "dest=backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
backup/dst@old_only	999"
}

test_inspect_delete_snap_uses_indexed_snapshot_records_when_available() {
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_actual_dest="backup/dst"

	zxfer_build_snapshot_record_index source "$(
		cat <<'EOF'
tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111
EOF
	)"
	zxfer_build_snapshot_record_index destination "$(
		cat <<'EOF'
backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
EOF
	)"

	inspect_delete_snap 0 "tank/src"

	assertEquals "Indexed source/destination snapshot records should still drive common-snapshot detection when the legacy global lists are unavailable." \
		"tank/src@zxfer_2	222" "$g_last_common_snap"
	assertEquals "Indexed snapshot records should still produce the correct transfer list." \
		"tank/src@zxfer_3	333" "$g_src_snapshot_transfer_list"
	assertEquals "Indexed destination snapshot records should still mark the destination as having snapshots." \
		1 "$g_dest_has_snapshots"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
