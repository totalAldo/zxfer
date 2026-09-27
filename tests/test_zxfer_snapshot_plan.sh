#!/bin/sh
#
# shunit2 tests for src/zxfer_snapshot_plan.sh: the per-dataset plan, the
# record slices, deletes and -g, the divergence contract, and the live
# re-plan of a dataset this run changed (fragment below).
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_snapshot_plan.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_snapshot_plan"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

reset_snapshot_plan_test_options() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_g_grandfather_protection=""
	g_option_d_delete_destination_snapshots=0
	g_option_F_force_rollback=""
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
}

reset_snapshot_plan_test_state() {
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_actual_dest=""
	g_zxfer_snapshot_plan_file=""
	g_zxfer_snapshot_creation_file=""
	zxfer_reset_snapshot_reconcile_state
}

# Stage the flat record files discovery publishes: source rows newest first,
# destination rows with real destination names.
stage_plan_record_files() {
	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/staged_source.records"
	g_zxfer_destination_snapshot_record_cache_file="$TEST_TMPDIR/staged_destination.records"
	printf '%s\n' "$1" >"$g_zxfer_source_snapshot_record_cache_file"
	printf '%s\n' "$2" >"$g_zxfer_destination_snapshot_record_cache_file"
}

setUp() {
	zxfer_source_runtime_modules_through "zxfer_snapshot_plan.sh"
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	reset_snapshot_plan_test_options
	reset_snapshot_plan_test_state
	zxfer_reset_failure_context "unit"
}

test_zxfer_snapshot_plan_file_reset_is_separate_from_dataset_state() {
	stage_plan_record_files "tank/src@s1	1" "backup/dst@s1	1"
	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
	plan_file=$g_zxfer_snapshot_plan_file

	zxfer_reset_snapshot_reconcile_state
	assertEquals "Per-dataset resets should keep the reusable plan file." \
		"$plan_file" "$g_zxfer_snapshot_plan_file"
	case "$plan_file" in
	"$g_zxfer_run_tmp_root"/?*) plan_file_under_root=yes ;;
	*) plan_file_under_root=no ;;
	esac
	assertEquals "The plan file should live under the run root." yes "$plan_file_under_root"

	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
	assertEquals "Later plans should reuse the same run-root plan file." \
		"$plan_file" "$g_zxfer_snapshot_plan_file"

	g_zxfer_snapshot_creation_file="$g_zxfer_run_tmp_root/creation"
	zxfer_reset_snapshot_delete_artifact_state
	assertEquals "The run-scoped reset should forget the plan file." \
		"" "$g_zxfer_snapshot_plan_file"
	assertEquals "The run-scoped reset should forget the creation-time file." \
		"" "$g_zxfer_snapshot_creation_file"
}

test_zxfer_reset_snapshot_reconcile_state_clears_plan_and_markers() {
	g_last_common_snap="tank/src@snap1"
	g_dest_has_snapshots=1
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_src_snapshot_transfer_list="tank/src@snap2"
	g_zxfer_plan_delete_snapshots="backup/dst@old"
	g_zxfer_diverged_converged_datasets="backup/dst	tank/src"

	zxfer_reset_snapshot_reconcile_state

	assertEquals "The last common snapshot should be cleared." "" "$g_last_common_snap"
	assertEquals "The destination-has-snapshots marker should be cleared." 0 "$g_dest_has_snapshots"
	assertEquals "The destination-deletion marker should be cleared." 0 "$g_did_delete_dest_snapshots"
	assertEquals "The deleted-newer marker should be cleared." 0 "$g_deleted_dest_newer_snapshots"
	assertEquals "The transfer list should be cleared." "" "$g_src_snapshot_transfer_list"
	assertEquals "The planned delete list should be cleared." "" "$g_zxfer_plan_delete_snapshots"
	assertEquals "The convergence markers should be cleared." "" "$g_zxfer_diverged_converged_datasets"
}

test_snapshot_plan_owner_operations_publish_and_validate_state() {
	zxfer_publish_snapshot_transfer_plan \
		"tank/src@snap1" "tank/src@snap2" 1

	assertEquals "Publishing a transfer plan should update the last common snapshot." \
		"tank/src@snap1" "$g_last_common_snap"
	assertEquals "Publishing a transfer plan should update the transfer list." \
		"tank/src@snap2" "$g_src_snapshot_transfer_list"
	assertEquals "Publishing a transfer plan should update destination snapshot presence." \
		1 "$g_dest_has_snapshots"

	invalid_plan_status=0
	zxfer_publish_snapshot_transfer_plan "" "" invalid || invalid_plan_status=$?
	assertEquals "Transfer-plan publication should reject invalid destination presence values." \
		2 "$invalid_plan_status"

}

test_delete_snaps_returns_when_nothing_needs_deletion() {
	log_file="$TEST_TMPDIR/delete_none.log"
	: >"$log_file"

	(
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$log_file"
		}
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "$*" >>"$log_file"
		}
		zxfer_delete_snaps "tank/fs" ""
	)

	assertEquals "An empty delete list should run no command." "" "$(cat "$log_file")"
}

test_delete_snaps_destroys_the_planned_snapshots_and_marks_the_dataset_changed() {
	log_file="$TEST_TMPDIR/delete_planned.log"
	: >"$log_file"
	g_zxfer_plan_source_count=2

	zxfer_run_destination_zfs_cmd() {
		printf 'destroy=%s %s\n' "$1" "$2" >>"$log_file"
		return 0
	}

	zxfer_delete_snaps "tank/fs" "backup/fs@snap3
backup/fs@snap4"

	assertEquals "The planned snapshots should be destroyed in one comma-joined target." \
		"destroy=destroy backup/fs@snap4,snap3" "$(cat "$log_file")"
	assertEquals "A destroy should set the destination-delete marker, which makes the pre-send recheck list the dataset live and marks the pass for -Y." \
		1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_throws_when_destroy_fails() {
	g_zxfer_plan_source_count=2

	status=0
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 37
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_delete_snaps "tank/fs" "tank/fs@snap3"
		)
	) || status=$?

	assertEquals "Failed destination destroys should preserve the destroy status." 37 "$status"
	assertContains "Failed destination destroys should use the generic execution error." \
		"$output" "Error when executing command."
}

test_delete_snaps_skips_full_wipe_when_live_source_recheck_shows_snapshots() {
	log_file="$TEST_TMPDIR/delete_guard_skip.log"
	warn_file="$TEST_TMPDIR/delete_guard_skip_warn.log"
	: >"$log_file"
	: >"$warn_file"
	g_zxfer_plan_source_count=0

	(
		zxfer_run_source_zfs_cmd() {
			printf 'source_probe=%s\n' "$*" >>"$log_file"
			printf '%s\n' "tank/fs@snap1"
		}
		zxfer_run_destination_zfs_cmd() {
			printf 'destination=%s\n' "$*" >>"$log_file"
		}
		zxfer_warn_stderr() {
			printf '%s\n' "$1" >>"$warn_file"
		}
		zxfer_delete_snaps "tank/fs" "tank/fs@snap1
tank/fs@snap2"
	)
	status=$?

	assertEquals "Skipping a suspicious full wipe should not be an error." 0 "$status"
	assertContains "An empty source plan must trigger a live source snapshot re-check before a full destination wipe." \
		"$(cat "$log_file")" "source_probe=list -H -d 1 -o name -t snapshot tank/fs"
	assertNotContains "No destination destroy may run when the live source re-check still shows snapshots." \
		"$(cat "$log_file")" "destroy"
	assertContains "Skipping the deletion should warn the operator about the incomplete cached listing." \
		"$(cat "$warn_file")" "skipping destination snapshot deletion for [tank/fs]"
}

test_delete_snaps_proceeds_with_full_wipe_when_live_source_recheck_confirms_empty() {
	log_file="$TEST_TMPDIR/delete_guard_proceed.log"
	: >"$log_file"
	g_zxfer_plan_source_count=0

	zxfer_run_source_zfs_cmd() {
		printf 'source_probe=%s\n' "$*" >>"$log_file"
		printf '%s' ""
	}
	zxfer_run_destination_zfs_cmd() {
		printf 'destination=%s %s\n' "$1" "$2" >>"$log_file"
		return 0
	}
	zxfer_delete_snaps "tank/fs" "tank/fs@snap1
tank/fs@snap2"

	assertContains "A genuinely snapshot-less source should still be re-checked live before a full destination wipe." \
		"$(cat "$log_file")" "source_probe=list -H -d 1 -o name -t snapshot tank/fs"
	assertContains "A live-confirmed empty source should preserve the existing delete-everything semantics." \
		"$(cat "$log_file")" "destination=destroy tank/fs@snap2,snap1"
}

test_delete_snaps_fails_closed_when_live_source_recheck_fails() {
	log_file="$TEST_TMPDIR/delete_guard_fail.log"
	: >"$log_file"
	g_zxfer_plan_source_count=0

	status=0
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "ssh timeout"
				return 43
			}
			zxfer_run_destination_zfs_cmd() {
				printf 'destination=%s\n' "$*" >>"$log_file"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_delete_snaps "tank/fs" "tank/fs@snap1
tank/fs@snap2"
		)
	) || status=$?

	assertEquals "A failed live source re-check must fail closed with the probe status." 43 "$status"
	assertContains "A failed live source re-check should explain what was being verified." \
		"$output" "Failed to re-verify source snapshots for [tank/fs] before deleting all destination snapshots"
	assertNotContains "No destination destroy may run when the live source re-check fails." \
		"$(cat "$log_file")" "destroy"
}

test_delete_snaps_ignores_probe_stderr_noise_when_deciding_full_wipe() {
	log_file="$TEST_TMPDIR/delete_guard_noise.log"
	: >"$log_file"
	g_zxfer_plan_source_count=0

	# The production capture merges stderr (2>&1): over ssh, benign noise such
	# as host-key notices or -V command echoes lands in the captured value on
	# a SUCCESSFUL probe of a genuinely snapshot-less source. Only lines that
	# are actually snapshots of the dataset may block the deletion.
	zxfer_run_source_zfs_cmd() {
		printf 'source_probe=%s\n' "$*" >>"$log_file"
		printf '%s\n' "Warning: Permanently added 'src' (ED25519) to the list of known hosts."
		printf '%s\n' "Running remote command [source aldo@src]: zfs list"
		return 0
	}
	zxfer_run_destination_zfs_cmd() {
		printf 'destination=%s %s\n' "$1" "$2" >>"$log_file"
		return 0
	}
	zxfer_delete_snaps "tank/fs" "tank/fs@snap1
tank/fs@snap2"

	assertContains "The guard should still live-probe the source before a full destination wipe." \
		"$(cat "$log_file")" "source_probe=list -H -d 1 -o name -t snapshot tank/fs"
	assertContains "Benign transport noise in the probe capture must not block a legitimate full deletion of destination snapshots." \
		"$(cat "$log_file")" "destination=destroy tank/fs@snap2,snap1"
}

test_delete_snaps_full_wipe_without_source_dataset_skips_the_live_recheck() {
	log_file="$TEST_TMPDIR/delete_guard_legacy.log"
	: >"$log_file"
	g_zxfer_plan_source_count=0

	zxfer_run_source_zfs_cmd() {
		printf 'source_probe=%s\n' "$*" >>"$log_file"
	}
	zxfer_run_destination_zfs_cmd() {
		printf 'destination=%s %s\n' "$1" "$2" >>"$log_file"
		return 0
	}
	zxfer_delete_snaps "" "tank/fs@snap1
tank/fs@snap2"

	assertNotContains "Callers that do not name the source dataset should not trigger a live source probe." \
		"$(cat "$log_file")" "source_probe"
	assertContains "Without a source dataset the delete-everything plan should still run." \
		"$(cat "$log_file")" "destination=destroy tank/fs@snap2,snap1"
}

# Stub the destination zfs runner for creation-time tests. `zfs get` prints
# CREATION_ROWS ("path<TAB>epoch" lines; leave a path out to model a missing
# row) and `zfs destroy` succeeds. Every argv is logged to CREATION_LOG.
stub_creation_rows() {
	CREATION_ROWS=$1
	CREATION_LOG=${2:-$TEST_TMPDIR/creation_rows.log}
	: >"$CREATION_LOG"
	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$*" >>"$CREATION_LOG"
		[ "$1" = get ] || return 0
		[ -z "$CREATION_ROWS" ] || printf '%s\n' "$CREATION_ROWS"
	}
}

test_delete_snaps_runs_grandfather_checks_before_destroying() {
	current_epoch=$(date +%s)
	g_option_g_grandfather_protection=7
	g_zxfer_plan_source_count=2
	stub_creation_rows "tank/fs@snap3	$((current_epoch - 86400))"

	zxfer_delete_snaps "tank/fs" "tank/fs@snap3"

	assertEquals "-g should read the creation time once, then destroy the unprotected snapshot." \
		"get -H -o name,value -p creation tank/fs@snap3
destroy tank/fs@snap3" "$(cat "$CREATION_LOG")"
	assertEquals "Successful deletions should set the destination-delete flag." 1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_marks_rollback_eligible_when_deleting_newer_snapshots() {
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_zxfer_plan_source_count=2

	zxfer_run_destination_zfs_cmd() {
		case "$*" in
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3")
			printf 'tank/fs@snap2\t200\n'
			printf 'tank/fs@snap3\t300\n'
			;;
		"destroy tank/fs@snap3")
			return 0
			;;
		*)
			return 1
			;;
		esac
	}

	zxfer_delete_snaps "tank/fs" "tank/fs@snap3"

	assertEquals "Deleting a destination snapshot newer than the last common snapshot should preserve rollback eligibility." \
		1 "$g_deleted_dest_newer_snapshots"
	assertEquals "Deleting a newer destination snapshot should still mark that a destroy was issued." \
		1 "$g_did_delete_dest_snapshots"
}

test_delete_snaps_batches_creation_time_reads_for_rollback_and_grandfather_checks() {
	log_file="$TEST_TMPDIR/delete_creation_batch.log"
	current_epoch=$(date +%s)
	common_epoch=$((current_epoch - 10 * 86400))
	snap3_epoch=$((current_epoch - 2 * 86400))
	snap4_epoch=$((current_epoch - 86400))
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_option_g_grandfather_protection=999
	g_zxfer_plan_source_count=2

	zxfer_run_destination_zfs_cmd() {
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

	zxfer_delete_snaps "tank/fs" "tank/fs@snap3
tank/fs@snap4"

	assertEquals "Delete planning should read every creation time in one batched query, then destroy." \
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3 tank/fs@snap4
destroy tank/fs@snap4,snap3" "$(cat "$log_file")"
	assertEquals "Deleting snapshots newer than the last common point should keep rollback eligibility." \
		1 "$g_deleted_dest_newer_snapshots"
}

test_delete_snaps_reports_creation_query_failures_before_any_destroy() {
	log_file="$TEST_TMPDIR/delete_creation_failure.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_zxfer_plan_source_count=2

	status=0
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$log_file"
				if [ "$*" = "get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3" ]; then
					printf '%s\n' "Permission denied (publickey)." >&2
					return 38
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_delete_snaps "tank/fs" "tank/fs@snap3"
		) 2>&1
	) || status=$?

	assertEquals "Delete planning should preserve the creation-time query status." \
		38 "$status"
	assertContains "Delete planning should keep the underlying query diagnostic." \
		"$output" "Permission denied (publickey)."
	assertContains "Delete planning should report the creation-time query failure." \
		"$output" "Failed to query destination snapshot creation times while planning snapshot deletions. Review prior stderr for the transport or query error."
	assertEquals "The failed query should be the only zfs command: no retry and no destroy." \
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3" "$(cat "$log_file")"
}

test_delete_snaps_treats_malformed_or_missing_creation_values_as_unknown() {
	current_epoch=$(date +%s)
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/fs@snap2"
	g_zxfer_plan_source_count=2
	stub_creation_rows "tank/fs@snap2	$((current_epoch - 10 * 86400))
tank/fs@snap3	unknown"

	zxfer_delete_snaps "tank/fs" "tank/fs@snap3
tank/fs@snap4"

	assertEquals "Unknown creation values should never trigger per-snapshot queries." \
		"get -H -o name,value -p creation tank/fs@snap2 tank/fs@snap3 tank/fs@snap4
destroy tank/fs@snap4,snap3" "$(cat "$CREATION_LOG")"
	assertEquals "Unknown creation values should keep rollback eligible (fail safe)." \
		1 "$g_deleted_dest_newer_snapshots"

	for unknown_case in "tank/fs@snap3	unknown" ""; do
		status=0
		output=$(
			(
				g_option_g_grandfather_protection=999
				stub_creation_rows "tank/fs@snap2	$((current_epoch - 10 * 86400))
$unknown_case"
				zxfer_throw_error() {
					printf '%s\n' "$1"
					exit 1
				}
				zxfer_delete_snaps "tank/fs" "tank/fs@snap3"
				printf 'unreachable\n'
			)
		) || status=$?

		assertEquals "-g must fail closed on an unknown creation time [$unknown_case]." 1 "$status"
		assertEquals "-g should name the snapshot whose creation time is unknown [$unknown_case]." \
			"Couldn't determine creation time for destination snapshot tank/fs@snap3." "$output"
		assertNotContains "No destroy may run after an unknown -g creation time [$unknown_case]." \
			"$(cat "$CREATION_LOG")" "destroy"
	done
}

test_prepare_snapshot_delete_creation_state_decides_rollback_eligibility() {
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common	111"
	creation_file=""

	# common epoch|delete list (space separated)|expected rollback eligibility
	for rollback_case in "200|backup/dst@old1 backup/dst@old2|0" \
		"200|backup/dst@old1 backup/dst@newer|1" \
		"unknown|backup/dst@old1|1" \
		"9e99|backup/dst@old1|1" \
		"200|backup/dst@unknown|1" \
		"200|backup/dst@unlisted|1"; do
		common_epoch=${rollback_case%%|*}
		rollback_rest=${rollback_case#*|}
		delete_list=$(printf '%s\n' "${rollback_rest%%|*}" | tr ' ' '\n')
		stub_creation_rows "backup/dst@common	$common_epoch
backup/dst@old1	100
backup/dst@old2	150
backup/dst@newer	300
backup/dst@unknown	-"

		zxfer_prepare_snapshot_delete_creation_state "$delete_list"

		assertEquals "Rollback eligibility for [$rollback_case]." \
			"${rollback_rest#*|}" "$g_deleted_dest_newer_snapshots"
		assertEquals "One batched query should serve the common snapshot and the deletes [$rollback_case]." \
			1 "$(grep -c '^get -H -o name,value -p creation backup/dst@common ' "$CREATION_LOG")"
		creation_file=${creation_file:-$g_zxfer_snapshot_creation_file}
		assertEquals "Every plan should reuse one creation-time file [$rollback_case]." \
			"$creation_file" "$g_zxfer_snapshot_creation_file"
	done
	case $creation_file in
	"$g_zxfer_run_tmp_root"/?*) creation_file_under_root=yes ;;
	*) creation_file_under_root=no ;;
	esac
	assertEquals "The creation-time file should live under the run root." yes "$creation_file_under_root"
}

test_prepare_snapshot_delete_creation_state_skips_the_query_when_nothing_needs_it() {
	stub_creation_rows ""

	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@common	111"
	g_deleted_dest_newer_snapshots=1
	zxfer_prepare_snapshot_delete_creation_state ""
	assertEquals "An empty delete list needs no creation time." \
		0 "$g_deleted_dest_newer_snapshots"

	g_last_common_snap=""
	zxfer_prepare_snapshot_delete_creation_state "backup/dst@old"
	assertEquals "Without a common snapshot and without -g rollback stays ineligible." \
		0 "$g_deleted_dest_newer_snapshots"

	g_last_common_snap="tank/src@common	111"
	g_actual_dest=""
	zxfer_prepare_snapshot_delete_creation_state "backup/dst@old"
	assertEquals "Without a destination dataset there is no common copy to compare against." \
		0 "$g_deleted_dest_newer_snapshots"

	assertEquals "None of these plans should query zfs." "" "$(cat "$CREATION_LOG")"
}

test_prepare_snapshot_delete_creation_state_batches_128_paths_per_query() {
	log_file="$TEST_TMPDIR/creation_batches.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/src@snap0	0"
	delete_list=$("${g_cmd_awk:-awk}" 'BEGIN { for (i = 1; i <= 128; i++) printf "tank/fs@snap%d\n", i }')

	# A zfs stand-in that drains stdin, as ssh would: it must not see the
	# delete list.
	zxfer_run_destination_zfs_cmd() {
		l_stdin=$(cat)
		printf '%s stdin=<%s>\n' "$#" "$l_stdin" >>"$log_file"
		shift 6
		for l_snapshot_path in "$@"; do
			printf '%s\t100\n' "$l_snapshot_path"
		done
	}

	# The caller's stdin carries data too (the -g pre-pass loops over a
	# here-doc): neither batch may read it.
	zxfer_prepare_snapshot_delete_creation_state "$delete_list" <<-EOF
		caller stdin must stay unread
	EOF

	# Each zfs argv is "get -H -o name,value -p creation" plus the paths.
	assertEquals "The common snapshot plus 128 deletes should take one full batch and one trailing batch, and never expose the delete list or the caller's stdin." \
		"134 stdin=<>
7 stdin=<>" "$(cat "$log_file")"
	assertEquals "Every path should be read, including the last batch's." \
		0 "$g_deleted_dest_newer_snapshots"
}

test_prepare_snapshot_delete_creation_state_stops_after_a_failed_full_batch() {
	log_file="$TEST_TMPDIR/creation_full_batch_failure.log"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/src@snap0	0"
	delete_list=$("${g_cmd_awk:-awk}" 'BEGIN { for (i = 1; i <= 200; i++) printf "tank/fs@snap%d\n", i }')

	status=0
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$#" >>"$log_file"
				return 28
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_prepare_snapshot_delete_creation_state "$delete_list"
		)
	) || status=$?

	assertEquals "A failed full batch should keep its status." 28 "$status"
	assertEquals "A failed full batch (6 zfs words plus 128 paths) should stop before the next batch." \
		134 "$(cat "$log_file")"
	assertContains "A failed full batch should report the query failure." \
		"$output" "Failed to query destination snapshot creation times while planning snapshot deletions."
}

test_prepare_snapshot_delete_creation_state_fails_closed_when_the_evaluation_fails() {
	failing_awk="$TEST_TMPDIR/failing_creation_awk.sh"
	printf '#!/bin/sh\nexit 57\n' >"$failing_awk"
	chmod +x "$failing_awk"
	g_actual_dest="tank/fs"
	g_last_common_snap="tank/src@snap2	2"

	status=0
	output=$(
		(
			stub_creation_rows "tank/fs@snap2	200
tank/fs@snap3	300"
			g_cmd_awk=$failing_awk
			zxfer_throw_error() {
				printf '%s newer=%s\n' "$1" "$g_deleted_dest_newer_snapshots"
				exit "${2:-1}"
			}
			zxfer_prepare_snapshot_delete_creation_state "tank/fs@snap3"
		)
	) || status=$?

	assertEquals "A failed evaluation should keep the awk status." 57 "$status"
	assertEquals "A failed evaluation should fail closed with rollback left eligible." \
		"Failed to evaluate destination snapshot creation times while planning snapshot deletions. newer=1" \
		"$output"
}

test_grandfather_policy_allows_young_snapshots_and_blocks_the_first_old_one_in_the_calling_shell() {
	g_option_g_grandfather_protection=5
	current_epoch=$(date +%s)
	young_epoch=$((current_epoch - 2 * 86400))
	old_epoch=$((current_epoch - 9 * 86400))
	creation_rows="backup/dst@young	$young_epoch
backup/dst@old	$old_epoch
backup/dst@older	$((old_epoch - 86400))"

	young_output=$(
		(
			stub_creation_rows "$creation_rows"
			zxfer_prepare_snapshot_delete_creation_state "backup/dst@young"
			printf 'allowed\n'
		)
	)
	old_status=0
	old_output=$(
		(
			stub_creation_rows "$creation_rows"
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_prepare_snapshot_delete_creation_state "backup/dst@young
backup/dst@old
backup/dst@older"
			printf 'unreachable\n'
		)
	) || old_status=$?

	assertEquals "Snapshots younger than -g days should pass the policy." "allowed" "$young_output"
	assertEquals "A snapshot older than -g days should stop the calling shell with a usage error." \
		2 "$old_status"
	assertContains "The grandfather error should name the first protected snapshot in list order." \
		"$old_output" "Snapshot name: backup/dst@old"
	assertContains "The grandfather error should give that snapshot's age (@older is 10 days old)." \
		"$old_output" "Snapshot age : 9 days old"
	assertNotContains "Nothing may run after a grandfather violation." "$old_output" "unreachable"

	# A snapshot exactly -g days old is already protected.
	edge_output=$(
		(
			stub_creation_rows "backup/dst@edge	$((current_epoch - 5 * 86400 - 60))"
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_prepare_snapshot_delete_creation_state "backup/dst@edge"
		)
	)
	assertContains "A snapshot exactly -g days old should be protected." \
		"$edge_output" "Snapshot age : 5 days old"

	g_option_g_grandfather_protection=""
	g_actual_dest=""
	zxfer_prepare_snapshot_delete_creation_state "backup/dst@old"
	assertEquals "Without -g or a common snapshot no query runs." 0 "$?"
}

test_grandfather_protection_error_reports_detailed_context() {
	g_option_g_grandfather_protection=1

	status=0
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_format_snapshot_creation_epoch_for_display() {
				printf '%s\n' "Sun Jan  1 00:00:00 UTC 2023"
			}
			zxfer_throw_grandfather_protection_error "tank/fs@ancient" 1672531200 5
		)
	) || status=$?

	assertEquals "Grandfather protection should fail old snapshot deletions with a usage error." 2 "$status"
	assertContains "Grandfather errors should include the -g setting." \
		"$output" "You have set grandfather protection at 1 days."
	assertContains "Grandfather errors should include the offending snapshot name." \
		"$output" "Snapshot name: tank/fs@ancient"
	assertContains "Grandfather errors should include the computed age." \
		"$output" "Snapshot age : 5 days old"
	assertContains "Grandfather errors should include the rendered snapshot date." \
		"$output" "Snapshot date: Sun Jan  1 00:00:00 UTC 2023."
	assertContains "Grandfather errors should explain how to recover." \
		"$output" "Either amend/remove option g, fix your system date, or manually"
}

test_grandfather_protection_error_falls_back_to_unix_epoch_when_local_date_rendering_fails() {
	g_option_g_grandfather_protection=1

	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_format_snapshot_creation_epoch_for_display() {
				return 1
			}
			zxfer_throw_grandfather_protection_error "tank/fs@epoch-only" 1672531200 3
		)
	)

	assertContains "Grandfather errors should fall back to the creation epoch when no formatter succeeds." \
		"$output" "Snapshot date: 1672531200 (unix epoch)."
}

test_grandfather_policy_fails_closed_when_the_current_time_is_unknown() {
	g_option_g_grandfather_protection=5

	status=0
	output=$(
		(
			stub_creation_rows "backup/dst@snap1	100"
			date() {
				printf '%s\n' "%s"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_snapshot_delete_creation_state "backup/dst@snap1"
			printf 'unreachable\n'
		)
	) || status=$?

	assertEquals "-g must fail closed when date +%s gives no epoch." 1 "$status"
	assertEquals "-g should say it could not read the current time." \
		"Failed to read the current time for grandfather protection (-g)." "$output"
}

test_format_snapshot_creation_epoch_for_display_falls_back_to_unix_epoch_when_date_conversion_is_unavailable() {
	output=$(
		(
			date() {
				return 1
			}
			zxfer_format_snapshot_creation_epoch_for_display 123
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should succeed with a unix-epoch fallback even when local date conversion fails." 0 "$status"
	assertEquals "Creation-epoch display formatting should fall back to explicit unix-epoch text when date conversion is unavailable." \
		"123 (unix epoch)" "$output"
}

test_format_snapshot_creation_epoch_for_display_rejects_nonnumeric_input() {
	set +e
	output=$(
		(
			zxfer_format_snapshot_creation_epoch_for_display "not-a-number"
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should reject non-numeric epochs." \
		1 "$status"
	assertEquals "Creation-epoch display formatting should not emit output for non-numeric epochs." \
		"" "$output"
}

test_format_snapshot_creation_epoch_for_display_prefers_date_r_when_available() {
	output=$(
		(
			date() {
				if [ "$1" = "-r" ] && [ "$2" = "123" ]; then
					printf '%s\n' "date-r-rendered"
					return 0
				fi
				return 1
			}
			zxfer_format_snapshot_creation_epoch_for_display 123
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should succeed when date -r is available." \
		0 "$status"
	assertEquals "Creation-epoch display formatting should prefer the date -r result when available." \
		"date-r-rendered" "$output"
}

test_format_snapshot_creation_epoch_for_display_uses_date_d_fallback_when_date_r_is_unavailable() {
	output=$(
		(
			date() {
				if [ "$1" = "-r" ]; then
					return 1
				fi
				if [ "$1" = "-d" ] && [ "$2" = "@123" ]; then
					printf '%s\n' "date-d-rendered"
					return 0
				fi
				return 1
			}
			zxfer_format_snapshot_creation_epoch_for_display 123
		)
	)
	status=$?

	assertEquals "Creation-epoch display formatting should succeed when the GNU date -d fallback is available." \
		0 "$status"
	assertEquals "Creation-epoch display formatting should use the GNU date -d fallback when date -r is unavailable." \
		"date-d-rendered" "$output"
}

test_record_diverged_destination_snapshots_caps_examples_at_three() {
	diverged_records=$(
		cat <<'EOF'
snapA	1	9
snapB	2	8
snapC	3	7
snapD	4	6
EOF
	)

	zxfer_record_diverged_destination_snapshots "$diverged_records"

	assertEquals "Every diverged snapshot should be counted." \
		4 "$g_zxfer_diverged_snapshot_count"
	assertEquals "Only the first three diverged snapshots should be kept as examples." \
		"snapA	1	9
snapB	2	8
snapC	3	7" "$g_zxfer_diverged_snapshot_examples"

	zxfer_record_diverged_destination_snapshots ""
	assertEquals "An empty scan should reset the diverged count." \
		0 "$g_zxfer_diverged_snapshot_count"
	assertEquals "An empty scan should reset the diverged examples." \
		"" "$g_zxfer_diverged_snapshot_examples"
}

test_diverged_converged_marker_find_and_unmark() {
	g_zxfer_diverged_converged_datasets="backup/a	tank/a
backup/a/nested	tank/a/nested"

	marker_lookup_status=0
	zxfer_find_diverged_converged_marker backup/a/nested || marker_lookup_status=$?
	assertEquals "Marker lookup should match the exact destination dataset." \
		0 "$marker_lookup_status"
	assertEquals "Marker lookup should publish the marker's source dataset." \
		"tank/a/nested" "$g_zxfer_diverged_converged_marker_source"
	assertFalse "Marker lookup must not prefix-match destination datasets." \
		"zxfer_find_diverged_converged_marker backup/a/nest"

	zxfer_unmark_diverged_converged_dataset "backup/a"
	assertEquals "Unmarking should drop only the named destination dataset." \
		"backup/a/nested	tank/a/nested" "$g_zxfer_diverged_converged_datasets"
	zxfer_unmark_diverged_converged_dataset "backup/a/nested"
	assertEquals "Unmarking the last dataset should empty the marker list." \
		"" "$g_zxfer_diverged_converged_datasets"
}

test_enforce_divergence_contract_emits_transparency_line_and_counts_once() {
	g_option_V_very_verbose=1
	g_option_d_delete_destination_snapshots=1
	g_option_F_force_rollback="-F"
	g_actual_dest="backup/dst"
	g_last_common_snap="tank/src@zxfer_1	111"
	zxfer_record_diverged_destination_snapshots "zxfer_2	222	999"
	g_zxfer_profile_diverged_snapshot_warnings=0

	contract_stderr_file="$TEST_TMPDIR/divergence_contract.err"
	zxfer_enforce_destination_divergence_contract "tank/src" 2>"$contract_stderr_file"
	contract_status=$?

	assertEquals "The convergence branch should return 0." 0 "$contract_status"
	assertTrue "The -V transparency line should name the last common snapshot and diverged count." \
		"grep -q 'Last common snapshot: tank/src@zxfer_1	111; diverged destination snapshots: 1.' '$contract_stderr_file'"
	assertEquals "The -V profile counter should count the warned dataset." \
		1 "$g_zxfer_profile_diverged_snapshot_warnings"

	# A second planning pass over the same dataset (e.g. the -g grandfather
	# pre-pass plus the main pass) must not warn or count again.
	zxfer_enforce_destination_divergence_contract "tank/src" 2>"$contract_stderr_file"
	assertEquals "A re-inspected marked dataset must not be counted twice." \
		1 "$g_zxfer_profile_diverged_snapshot_warnings"
	assertFalse "A re-inspected marked dataset must not warn twice." \
		"grep -q 'WARNING' '$contract_stderr_file'"

	# In-sync datasets still get the transparency line and nothing else.
	zxfer_record_diverged_destination_snapshots ""
	g_actual_dest="backup/clean"
	zxfer_enforce_destination_divergence_contract "tank/clean" 2>"$contract_stderr_file"
	assertTrue "In-sync datasets should still emit the transparency line." \
		"grep -q 'diverged destination snapshots: 0.' '$contract_stderr_file'"
	assertEquals "In-sync datasets must not be counted." \
		1 "$g_zxfer_profile_diverged_snapshot_warnings"
	assertFalse "In-sync datasets must not warn." \
		"grep -q 'WARNING' '$contract_stderr_file'"
}

test_verify_converged_destination_skips_unmarked_datasets() {
	g_zxfer_diverged_converged_datasets=""
	# No stubs installed: any capture or live-view call would fail loudly, so
	# returning 0 proves the unmarked path is a pure string test.
	zxfer_verify_converged_destination_after_receive "backup/dst"
	assertEquals "Unmarked datasets must skip post-receive verification." 0 $?
}

test_plan_dataset_snapshots_requires_a_guid_match_for_the_common_snapshot() {
	stage_plan_record_files "tank/doET/tank@zxfer_2	222
tank/doET/tank@zxfer_1	111" "backup/nuc/tank/doET/tank@zxfer_2	999
backup/nuc/tank/doET/tank@zxfer_1	111"

	zxfer_plan_dataset_snapshots "tank/doET/tank" "backup/nuc/tank/doET/tank"

	assertEquals "The common snapshot needs a matching name AND guid." \
		"tank/doET/tank@zxfer_1	111" "$g_zxfer_plan_common_snapshot"
	assertEquals "The same-named source snapshot with another guid should be sent again." \
		"tank/doET/tank@zxfer_2	222" "$g_zxfer_plan_transfer_list"
	assertEquals "The guid mismatch should be reported as divergence with both guids." \
		"zxfer_2	222	999" "$g_zxfer_plan_diverged_records"
	assertEquals "The guid-mismatched destination snapshot should be planned for deletion." \
		"backup/nuc/tank/doET/tank@zxfer_2" "$g_zxfer_plan_delete_snapshots"
	assertEquals "The destination has snapshots." 1 "$g_zxfer_plan_dest_has_snapshots"
	assertEquals "Both source records should be counted." 2 "$g_zxfer_plan_source_count"
}

test_plan_dataset_snapshots_sends_everything_when_nothing_matches() {
	stage_plan_record_files "tank/doET/tank@zxfer_2	222
tank/doET/tank@zxfer_1	111" "backup/doET/tank@zxfer_3	333"

	zxfer_plan_dataset_snapshots "tank/doET/tank" "backup/doET/tank"

	assertEquals "No shared snapshot should leave the common snapshot empty." \
		"" "$g_zxfer_plan_common_snapshot"
	assertEquals "Every source snapshot should be sent, oldest first." \
		"tank/doET/tank@zxfer_1	111
tank/doET/tank@zxfer_2	222" "$g_zxfer_plan_transfer_list"
	assertEquals "The destination-only snapshot should be planned for deletion." \
		"backup/doET/tank@zxfer_3" "$g_zxfer_plan_delete_snapshots"
	assertEquals "The destination still has snapshots." 1 "$g_zxfer_plan_dest_has_snapshots"

	stage_plan_record_files "tank/doET/tank@zxfer_1	111" "backup/other@zxfer_1	111"
	zxfer_plan_dataset_snapshots "tank/doET/tank" "backup/doET/tank"
	assertEquals "A destination without rows has no snapshots." 0 "$g_zxfer_plan_dest_has_snapshots"
	assertEquals "A destination without rows publishes no records." "" "$g_zxfer_plan_destination_records"
}

test_plan_dataset_snapshots_matches_exact_dataset_names_only() {
	stage_plan_record_files "tank/src1@s2	21
tank/src/c1@s2	31
tank/src@s2	2
tank/src@s1	1
tank/src1@s1	11" "backup/dst1@s9	19
backup/dst@s1	1
backup/dst/c1@s9	39"

	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"

	assertEquals "Sibling (tank/src1) and child (tank/src/c1) rows must be ignored." \
		"tank/src@s1	1" "$g_zxfer_plan_common_snapshot"
	assertEquals "Only the dataset's own newer snapshots should be sent." \
		"tank/src@s2	2" "$g_zxfer_plan_transfer_list"
	assertEquals "Rows of other destination datasets must never be deleted." \
		"" "$g_zxfer_plan_delete_snapshots"
	assertEquals "Only the dataset's own destination rows should be published." \
		"backup/dst@s1	1" "$g_zxfer_plan_destination_records"
	assertEquals "Only the dataset's own source rows should be counted." 2 "$g_zxfer_plan_source_count"
}

test_plan_dataset_snapshots_deletes_destination_only_and_guid_mismatched_rows_in_destination_order() {
	stage_plan_record_files "tank/fs@daily-10	10
tank/fs@snap3	333
tank/fs@snap1	111" "backup/fs@snap1	999
backup/fs@alpha	222
backup/fs@daily-1	1
backup/fs@snap3	333
backup/fs@zeta	444"

	zxfer_plan_dataset_snapshots "tank/fs" "backup/fs"

	assertEquals "Destination-only rows and guid mismatches should be deleted in destination order; prefix names never match." \
		"backup/fs@snap1
backup/fs@alpha
backup/fs@daily-1
backup/fs@zeta" "$g_zxfer_plan_delete_snapshots"
	assertEquals "The newest guid-exact match is the common snapshot." \
		"tank/fs@snap3	333" "$g_zxfer_plan_common_snapshot"
}

test_plan_dataset_snapshots_lists_newer_source_snapshots_oldest_first() {
	stage_plan_record_files "tank/fs@snap4	4
tank/fs@snap3	3
tank/fs@snap2	2
tank/fs@snap1	1" "backup/fs@snap1	1
backup/fs@snap2	2"

	zxfer_plan_dataset_snapshots "tank/fs" "backup/fs"

	assertEquals "The newest shared snapshot is the common one." \
		"tank/fs@snap2	2" "$g_zxfer_plan_common_snapshot"
	assertEquals "Only snapshots newer than the common one should be sent, oldest first." \
		"tank/fs@snap3	3
tank/fs@snap4	4" "$g_zxfer_plan_transfer_list"
}

test_plan_dataset_snapshots_reports_each_name_match_guid_mismatch() {
	stage_plan_record_files "tank/a/nested.b@autosnap_2026-06-12_06:00:02_frequently	2222000000000000001
tank/a/nested.b@zxfer_81150_20260612000001	1111000000000000001
tank/a/nested.b@hostile:colon-snap_%	3333000000000000001" "backup/a/nested.b@autosnap_2026-06-12_06:00:02_frequently	9999000000000000001
backup/a/nested.b@zxfer_81150_20260612000001	1111000000000000001
backup/a/nested.b@hostile:colon-snap_%	9999000000000000002"

	zxfer_plan_dataset_snapshots "tank/a/nested.b" "backup/a/nested.b"

	assertEquals "Every name-match/guid-mismatch pair should be reported with both guids, in source order; matching guids must not be." \
		"autosnap_2026-06-12_06:00:02_frequently	2222000000000000001	9999000000000000001
hostile:colon-snap_%	3333000000000000001	9999000000000000002" \
		"$g_zxfer_plan_diverged_records"

	stage_plan_record_files "tank/src@only_on_source	111" "backup/dst@only_on_dest	222"
	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
	assertEquals "Disjoint snapshot names should report no divergence." "" "$g_zxfer_plan_diverged_records"
}

test_plan_dataset_snapshots_fails_closed_on_records_without_guids() {
	for plan_case in "source" "destination"; do
		if [ "$plan_case" = source ]; then
			stage_plan_record_files "tank/src@snap2	222
tank/src@snap1" "backup/dst@snap1	111"
		else
			stage_plan_record_files "tank/src@snap1	111" "backup/dst@snap1"
		fi
		status=0
		output=$(
			(
				zxfer_throw_error() {
					printf '%s\n' "$1"
					exit "${2:-1}"
				}
				zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
				printf 'planned\n'
			)
		) || status=$?

		assertEquals "A guid-less $plan_case record must stop planning." 3 "$status"
		assertContains "The failure should name both datasets [$plan_case]." \
			"$output" "Failed to determine the last common snapshot for [tank/src] and [backup/dst]."
		assertNotContains "No plan may be used after a guid-less $plan_case record." "$output" "planned"
	done

	stage_plan_record_files "tank/other@guidless
tank/src@snap1	111" "backup/other@guidless
backup/dst@snap1	111"
	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
	assertEquals "Guid-less rows of other datasets are skipped before any check." \
		"tank/src@snap1	111" "$g_zxfer_plan_common_snapshot"
}

test_plan_dataset_snapshots_fails_closed_on_unreadable_record_files() {
	for plan_side in source destination; do
		stage_plan_record_files "tank/src@snap1	111" "backup/dst@snap1	111"
		if [ "$plan_side" = source ]; then
			g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/vanished_source.records"
		else
			g_zxfer_destination_snapshot_record_cache_file=""
		fi
		status=0
		output=$(
			(
				zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
				printf 'planned\n'
			) 2>&1
		) || status=$?

		assertEquals "A missing $plan_side record file must abort planning." 1 "$status"
		assertContains "The abort should name the missing $plan_side record file." \
			"$output" "Failed to read staged $plan_side snapshot record cache."
		assertNotContains "Planning must not continue without the $plan_side record file." \
			"$output" "planned"
	done
}

test_plan_dataset_snapshots_preserves_awk_failures() {
	failing_awk="$TEST_TMPDIR/failing_plan_awk.sh"
	printf '#!/bin/sh\nexit 61\n' >"$failing_awk"
	chmod +x "$failing_awk"
	stage_plan_record_files "tank/src@snap1	111" "backup/dst@snap1	111"

	status=0
	output=$(
		(
			g_cmd_awk=$failing_awk
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
			printf 'planned\n'
		)
	) || status=$?

	assertEquals "A failed plan pass must stop with the awk status." 61 "$status"
	assertContains "Plan failures should report both dataset sides." \
		"$output" "Failed to determine the last common snapshot for [tank/src] and [backup/dst]."
	assertNotContains "No plan may be used after a failed pass." "$output" "planned"
}

test_plan_dataset_snapshots_reads_an_explicit_destination_record_file() {
	stage_plan_record_files "tank/src@snap2	222
tank/src@snap1	111" "backup/dst@snap1	111"
	live_file="$TEST_TMPDIR/live_destination.records"
	printf '%s\n' "Warning: Permanently added 'dst' to the list of known hosts." \
		"backup/dst@snap1	111" "backup/dst@snap2	222" >"$live_file"

	zxfer_plan_dataset_snapshots "tank/src" "backup/dst" "$live_file"

	assertEquals "The explicit record file should replace the staged destination rows." \
		"tank/src@snap2	222" "$g_zxfer_plan_common_snapshot"
	assertEquals "Nothing is left to send once the live rows hold the newest snapshot." \
		"" "$g_zxfer_plan_transfer_list"
	assertEquals "Only the dataset's rows should be published; stderr noise is ignored." \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$g_zxfer_plan_destination_records"
}

test_plan_dataset_snapshots_leaves_the_published_plan_alone() {
	g_last_common_snap="tank/current@anchor	1"
	g_src_snapshot_transfer_list="tank/current@next	2"
	g_dest_has_snapshots=1
	stage_plan_record_files "tank/src@snap1	111" "backup/other@snap1	111"

	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"

	assertEquals "Planning must not overwrite the current dataset's common snapshot." \
		"tank/current@anchor	1" "$g_last_common_snap"
	assertEquals "Planning must not overwrite the current dataset's transfer list." \
		"tank/current@next	2" "$g_src_snapshot_transfer_list"
	assertEquals "Planning must not overwrite the current destination presence." \
		1 "$g_dest_has_snapshots"
}

# Stage record files for a split: source rows newest first and interleaved
# across datasets (a guid-less row and an unlisted dataset included),
# destination rows under backup/src with a sibling root, backup/src2, and a
# line that is no record.
stage_split_record_files() {
	stage_plan_record_files "tank/src/a/bc@s2	62
tank/src/a/b@s2	52
tank/src@s2	22
tank/other@s2	92
tank/src/a/b@s1	51
tank/src/a/bc@s1	61
tank/src@guidless
tank/src@s1	21" "backup/src@s1	21
backup/src2@s1	71
backup/src/a/b@s1	51
backup/src/a/bc@s1	61
backup/src/a/b@s0	50
no record here"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_destination="backup"
}

test_split_snapshot_records_writes_each_listed_dataset_rows_in_order() {
	stage_split_record_files

	zxfer_split_snapshot_records "1	tank/src
2	tank/src/a/b
3	tank/src/a/bc
4	tank/src/empty"
	split_status=$?
	base=$g_zxfer_snapshot_slice_base

	assertEquals "A complete split should succeed." 0 "$split_status"
	assertEquals "The split should record the record files it was cut from." \
		"$g_zxfer_source_snapshot_record_cache_file
$g_zxfer_destination_snapshot_record_cache_file" "$g_zxfer_snapshot_slice_records"
	assertEquals "The root's source slice should keep its rows newest first, the guid-less row included." \
		"tank/src@s2	22
tank/src@guidless
tank/src@s1	21" "$(cat "$base.1.s")"
	assertEquals "The root's destination slice should hold only the root's rows." \
		"backup/src@s1	21" "$(cat "$base.1.d")"
	assertEquals "A dataset must never take the rows of a sibling whose name it prefixes." \
		"tank/src/a/b@s2	52
tank/src/a/b@s1	51" "$(cat "$base.2.s")"
	assertEquals "Destination rows should keep their listing order." \
		"backup/src/a/b@s1	51
backup/src/a/b@s0	50" "$(cat "$base.2.d")"
	assertEquals "The prefixed sibling should get its own source rows." \
		"tank/src/a/bc@s2	62
tank/src/a/bc@s1	61" "$(cat "$base.3.s")"
	assertEquals "The prefixed sibling should get its own destination rows." \
		"backup/src/a/bc@s1	61" "$(cat "$base.3.d")"
	assertTrue "A listed dataset without rows should get empty slices." \
		"[ -f '$base.4.s' ] && [ ! -s '$base.4.s' ] && [ -f '$base.4.d' ] && [ ! -s '$base.4.d' ]"
	assertFalse "No slice should exist past the last listed position." "[ -e '$base.5.s' ]"
	assertTrue "The spent keyed copy should be emptied." "[ -f '$base' ] && [ ! -s '$base' ]"
	assertEquals "Slices should be private like every run-root file." \
		"$base.2.s" "$(find "$base.2.s" -perm 0600 2>/dev/null)"

	# An unlisted sibling whose name a listed dataset prefixes keeps its rows
	# out of every slice.
	zxfer_split_snapshot_records "1	tank/src/a/b"
	assertEquals "An unlisted prefixed sibling's source rows must be dropped." \
		"tank/src/a/b@s2	52
tank/src/a/b@s1	51" "$(cat "$base.1.s")"
	assertEquals "An unlisted prefixed sibling's destination rows must be dropped." \
		"backup/src/a/b@s1	51
backup/src/a/b@s0	50" "$(cat "$base.1.d")"
}

test_split_snapshot_records_maps_a_trailing_slash_destination_root() {
	stage_plan_record_files "tank/src/c@s1	31
tank/src@s1	21" "backup@s1	21
backup/c@s1	31
backup2/c@s1	41"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_destination="backup"

	zxfer_split_snapshot_records "1	tank/src
2	tank/src/c"

	assertEquals "The destination root itself should map to the source root." \
		"backup@s1	21" "$(cat "$g_zxfer_snapshot_slice_base.1.d")"
	assertEquals "Children should map below the destination root, never from a sibling root." \
		"backup/c@s1	31" "$(cat "$g_zxfer_snapshot_slice_base.2.d")"
}

test_split_snapshot_records_publishes_nothing_when_a_stage_fails() {
	stage_split_record_files
	real_awk=$g_cmd_awk
	failing_key_awk="$TEST_TMPDIR/failing_key_awk.sh"
	failing_write_awk="$TEST_TMPDIR/failing_write_awk.sh"
	printf '#!/bin/sh\nexit 44\n' >"$failing_key_awk"
	# shellcheck disable=SC2016  # the wrapper script expands these itself.
	printf '#!/bin/sh\n[ -z "${ZXFER_AWK_SLICE_BASE:-}" ] || exit 46\nexec "%s" "$@"\n' \
		"$real_awk" >"$failing_write_awk"
	chmod +x "$failing_key_awk" "$failing_write_awk"

	for split_stage in key sort write; do
		output=$(
			g_zxfer_snapshot_slice_records="stale"
			g_zxfer_snapshot_slice_key=9
			if [ "$split_stage" = key ]; then
				g_cmd_awk=$failing_key_awk
			elif [ "$split_stage" = sort ]; then
				sort() { return 45; }
			else
				g_cmd_awk=$failing_write_awk
			fi
			split_status=0
			zxfer_split_snapshot_records "1	tank/src" || split_status=$?
			printf 'status=%s records=<%s> key=<%s>\n' "$split_status" \
				"$g_zxfer_snapshot_slice_records" "$g_zxfer_snapshot_slice_key"
		)
		case $split_stage in
		key) expected_status=44 ;;
		sort) expected_status=45 ;;
		write) expected_status=46 ;;
		esac
		assertEquals "A failed $split_stage stage should return its status and publish no slices or selection." \
			"status=$expected_status records=<> key=<>" "$output"
	done
}

test_split_snapshot_records_skips_without_record_files_or_datasets() {
	g_zxfer_snapshot_slice_records="stale"
	zxfer_split_snapshot_records "1	tank/src"
	assertEquals "Without record files the split should succeed quietly." 0 "$?"
	assertEquals "Without record files no slices should be published." \
		"" "$g_zxfer_snapshot_slice_records"

	stage_split_record_files
	zxfer_split_snapshot_records ""
	assertEquals "An empty list should publish no slices." "" "$g_zxfer_snapshot_slice_records"
}

test_select_snapshot_slice_selects_only_numbered_positions_of_published_slices() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_destination="backup"

	g_zxfer_snapshot_slice_records=""
	zxfer_select_snapshot_slice 2 "tank/src/a"
	assertEquals "Without published slices nothing should be selected." "" "$g_zxfer_snapshot_slice_key"

	g_zxfer_snapshot_slice_records="published"
	zxfer_select_snapshot_slice "tank/src/a" "tank/src/a"
	assertEquals "A position that is not a number should select nothing." "" "$g_zxfer_snapshot_slice_key"

	zxfer_select_snapshot_slice 2 "tank/src/a"
	assertEquals "A position should select that dataset and its mapped destination." \
		"2|tank/src/a|backup/src/a" \
		"$g_zxfer_snapshot_slice_key|$g_zxfer_snapshot_slice_source|$g_zxfer_snapshot_slice_destination"
}

test_plan_dataset_snapshots_reads_only_the_selected_dataset_slices() {
	stage_split_record_files
	zxfer_split_snapshot_records "1	tank/src
2	tank/src/a/b"
	# The whole files change after the split: a slice-served plan never
	# reads them.
	printf '%s\n' "tank/src/a/b@s9	59" >"$g_zxfer_source_snapshot_record_cache_file"

	zxfer_select_snapshot_slice 2 "tank/src/a/b"
	zxfer_plan_dataset_snapshots "tank/src/a/b" "backup/src/a/b"
	assertEquals "The selected dataset should plan from its slices." \
		"tank/src/a/b@s1	51|tank/src/a/b@s2	52|backup/src/a/b@s0|2" \
		"$g_zxfer_plan_common_snapshot|$g_zxfer_plan_transfer_list|$g_zxfer_plan_delete_snapshots|$g_zxfer_plan_source_count"

	zxfer_plan_dataset_snapshots "tank/src" "backup/src"
	assertEquals "Another dataset should read the whole record files." \
		0 "$g_zxfer_plan_source_count"

	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/rediscovered_source.records"
	printf '%s\n' "tank/src/a/b@s9	59" >"$g_zxfer_source_snapshot_record_cache_file"
	zxfer_plan_dataset_snapshots "tank/src/a/b" "backup/src/a/b"
	assertEquals "Slices cut from earlier record files must never be used." \
		"tank/src/a/b@s9	59" "$g_zxfer_plan_transfer_list"
}

test_inspect_delete_snap_publishes_the_plan_for_the_current_dataset() {
	g_actual_dest="backup/dst"
	stage_plan_record_files "tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111" "backup/dst@zxfer_1	111
backup/dst@zxfer_2	222"

	zxfer_inspect_delete_snap 0 "tank/src"

	assertEquals "Inspection should publish the newest common snapshot." \
		"tank/src@zxfer_2	222" "$g_last_common_snap"
	assertEquals "Inspection should publish the transfer list." \
		"tank/src@zxfer_3	333" "$g_src_snapshot_transfer_list"
	assertEquals "Inspection should publish destination presence." 1 "$g_dest_has_snapshots"
	assertEquals "Inspection should leave the planned destination rows the live recheck compares against." \
		"backup/dst@zxfer_1	111
backup/dst@zxfer_2	222" "$g_zxfer_plan_destination_records"
}

test_inspect_delete_snap_marks_destination_empty_when_no_matching_destination_dataset_exists() {
	g_actual_dest="backup/dst"
	stage_plan_record_files "tank/src@zxfer_3	3
tank/src@zxfer_2	2" "backup/other@zxfer_1	1"

	zxfer_inspect_delete_snap 0 "tank/src"

	assertEquals "Missing destination datasets should be reported as having no snapshots." 0 "$g_dest_has_snapshots"
	assertEquals "No destination snapshots should yield an empty last common snapshot." "" "$g_last_common_snap"
	assertEquals "All source snapshots should be transferred when the destination dataset is absent." \
		"tank/src@zxfer_2	2
tank/src@zxfer_3	3" "$g_src_snapshot_transfer_list"
}

# Last-common selection through the real planning path: a snapshot name that
# is a prefix of another name (daily-1 vs daily-10 / daily-11) must never be
# mismatched.
test_inspect_delete_snap_matches_exact_names_without_prefix_collisions() {
	g_actual_dest="backup/fs"
	stage_plan_record_files "tank/fs@daily-10	10
tank/fs@daily-1	1" "backup/fs@daily-1	1
backup/fs@daily-11	11"

	zxfer_inspect_delete_snap 0 "tank/fs"

	assertEquals "Last-common detection should match exact snapshot names, not prefixes." \
		"tank/fs@daily-1	1" "$g_last_common_snap"
	assertEquals "Transfer planning should queue only the source snapshots newer than the exact-name common one." \
		"tank/fs@daily-10	10" "$g_src_snapshot_transfer_list"
}

test_inspect_delete_snap_reports_the_common_snapshot_in_very_verbose_mode() {
	g_option_V_very_verbose=1
	g_actual_dest="backup/dst"
	stage_plan_record_files "tank/src@zxfer_1	111" "backup/dst@zxfer_1	111"

	output=$(zxfer_inspect_delete_snap 0 "tank/src" 2>&1)

	assertContains "-V should print the common snapshot line." \
		"$output" "Found last common snapshot: tank/src@zxfer_1	111."
	assertContains "-V should print the divergence transparency line." \
		"$output" "diverged destination snapshots: 0."
}

# Divergence contract: planning over a name-match/guid-mismatch destination
# is only allowed to proceed when BOTH -d and -F are active; it must then
# warn on stderr (not gated on -v/-V), keep guid-based common-base selection,
# and mark the dataset for post-receive verification.
test_inspect_delete_snap_requires_matching_guid_for_common_snapshot_detection() {
	g_option_d_delete_destination_snapshots=1
	g_option_F_force_rollback="-F"
	g_actual_dest="backup/dst"
	stage_plan_record_files "tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111" "backup/dst@zxfer_2	999
backup/dst@zxfer_1	111"

	divergence_warning_file="$TEST_TMPDIR/divergence_warning.err"
	zxfer_inspect_delete_snap 0 "tank/src" 2>"$divergence_warning_file"

	assertEquals "Same-named but unrelated destination snapshots should not be treated as the common base." \
		"tank/src@zxfer_1	111" "$g_last_common_snap"
	assertEquals "Transfer planning should keep the divergent source snapshot when the destination guid differs." \
		"tank/src@zxfer_2	222
tank/src@zxfer_3	333" "$g_src_snapshot_transfer_list"
	assertTrue "The always-on divergence warning should name the diverged dataset." \
		"grep -q 'destination dataset \[backup/dst\] has 1 snapshot' '$divergence_warning_file'"
	assertTrue "The divergence warning should show both guids for the example snapshot." \
		"grep -q 'backup/dst@zxfer_2: source guid 222 vs destination guid 999' '$divergence_warning_file'"
	assertTrue "The divergence warning should state the convergence action." \
		"grep -q 'converging: destroy + rollback + resend' '$divergence_warning_file'"
	divergence_marker_status=0
	zxfer_find_diverged_converged_marker backup/dst || divergence_marker_status=$?
	assertEquals "The converged dataset should be marked for post-receive verification." \
		0 "$divergence_marker_status"
	assertEquals "The marker should remember the diverged dataset's source." \
		"tank/src" "$g_zxfer_diverged_converged_marker_source"
}

test_inspect_delete_snap_passes_the_planned_delete_list_to_delete_snaps() {
	log_file="$TEST_TMPDIR/inspect_delete.log"
	g_actual_dest="backup/dst"
	stage_plan_record_files "tank/src@zxfer_3	333
tank/src@zxfer_2	222
tank/src@zxfer_1	111" "backup/dst@zxfer_2	222
backup/dst@zxfer_1	111
backup/dst@old_only	999"

	(
		zxfer_delete_snaps() {
			printf 'source=%s\n' "$1" >"$log_file"
			printf 'delete=%s\n' "$2" >>"$log_file"
		}
		zxfer_inspect_delete_snap 1 "tank/src"
	)

	assertEquals "zxfer_inspect_delete_snap should hand the source dataset and the planned destination-only snapshots to zxfer_delete_snaps." \
		"source=tank/src
delete=backup/dst@old_only" "$(cat "$log_file")"
}

test_inspect_delete_snap_destroys_planned_snapshots_with_spaces_in_the_name() {
	log_file="$TEST_TMPDIR/inspect_delete_spaces.log"
	g_actual_dest="back/my data"
	stage_plan_record_files "tank/my data@s2	2
tank/my data@s1	1" "back/my data@s1	1
back/my data@s9	9"

	(
		zxfer_prepare_snapshot_delete_creation_state() { :; }
		zxfer_run_destination_zfs_cmd() {
			printf '%s' "$1" >>"$log_file"
			shift
			printf ' [%s]' "$@" >>"$log_file"
			printf '\n' >>"$log_file"
		}
		zxfer_inspect_delete_snap 1 "tank/my data"
		printf 'transfer=%s\n' "$g_src_snapshot_transfer_list" >>"$log_file"
	)

	assertEquals "A dataset name with a space must stay one destroy argument and one planned record." \
		"destroy [back/my data@s9]
transfer=tank/my data@s2	2" "$(cat "$log_file")"
}

test_inspect_delete_snap_stops_in_main_shell_on_grandfather_violation() {
	action_log="$TEST_TMPDIR/inspect_grandfather_stop.log"
	g_actual_dest="tank/fs"
	stage_plan_record_files "tank/src@snap1	1" "tank/fs@snap1	1
tank/fs@protected	2"

	status=0
	output=$(
		(
			g_option_g_grandfather_protection=7
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = get ]; then
					printf 'tank/fs@snap1\t100\ntank/fs@protected\t100\n'
					return 0
				fi
				printf '%s\n' "$1" >>"$action_log"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}

			zxfer_inspect_delete_snap 1 "tank/src"
			printf '%s\n' after-inspect >>"$action_log"
		) 2>&1
	) || status=$?

	assertEquals "Grandfather protection must terminate the owning inspect path, not only a render subshell." \
		2 "$status"
	assertContains "The protected-snapshot usage error should remain operator-visible." \
		"$output" "Snapshot name: tank/fs@protected"
	assertFalse "No destroy or later step may run after a grandfather violation." \
		"[ -e '$action_log' ]"
}

test_verify_converged_destination_clears_marker_on_aligned_live_view() {
	g_zxfer_diverged_converged_datasets="backup/dst	tank/src"
	stage_plan_record_files "tank/src@zxfer_2	222
tank/src@zxfer_1	111" "backup/dst@zxfer_2	999"
	live_file="$TEST_TMPDIR/verify_aligned.records"
	printf '%s\n' "backup/dst@zxfer_2	222" "backup/dst@zxfer_1	111" >"$live_file"

	verify_output=$(
		(
			zxfer_get_live_destination_record_file() {
				g_zxfer_live_destination_record_file_result=$live_file
			}
			zxfer_verify_converged_destination_after_receive "backup/dst" &&
				printf 'verified marker=[%s]\n' "$g_zxfer_diverged_converged_datasets"
		)
	)

	assertContains "An aligned live view should verify and clear the marker." \
		"$verify_output" "verified marker=[]"
}

test_verify_converged_destination_reports_live_listing_failures() {
	g_zxfer_diverged_converged_datasets="backup/dst	tank/src"

	verify_status=0
	verify_output=$(
		(
			zxfer_get_live_destination_record_file() {
				g_zxfer_live_destination_record_file_error="ssh: broken pipe"
				return 42
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_verify_converged_destination_after_receive "backup/dst"
		)
	) || verify_status=$?

	assertEquals "A failed live listing must abort verification." 1 "$verify_status"
	assertEquals "The failure should keep its post-receive context and the listing output." \
		"Failed to retrieve live destination snapshots for [backup/dst] during post-receive divergence verification: ssh: broken pipe" \
		"$verify_output"
}

test_verify_converged_destination_fails_closed_on_guidless_live_rows() {
	g_zxfer_diverged_converged_datasets="backup/dst	tank/src"
	stage_plan_record_files "tank/src@zxfer_1	111" "backup/dst@zxfer_1	111"
	live_file="$TEST_TMPDIR/verify_guidless.records"
	printf '%s\n' "backup/dst@zxfer_1" >"$live_file"

	verify_status=0
	verify_output=$(
		(
			zxfer_get_live_destination_record_file() {
				g_zxfer_live_destination_record_file_result=$live_file
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_verify_converged_destination_after_receive "backup/dst"
			printf 'verified\n'
		)
	) || verify_status=$?

	assertEquals "A guid-less live row must stop verification instead of passing it." 3 "$verify_status"
	assertNotContains "Verification must not pass on rows it cannot classify." "$verify_output" "verified"
}

test_verify_converged_destination_keeps_the_current_dataset_plan() {
	g_zxfer_diverged_converged_datasets="backup/done	tank/done"
	g_last_common_snap="tank/current@anchor	1"
	g_src_snapshot_transfer_list="tank/current@next	2"
	stage_plan_record_files "tank/done@s1	1" "backup/done@s1	1"
	live_file="$TEST_TMPDIR/verify_keeps_plan.records"
	printf '%s\n' "backup/done@s1	1" >"$live_file"

	zxfer_get_live_destination_record_file() {
		g_zxfer_live_destination_record_file_result=$live_file
	}
	zxfer_verify_converged_destination_after_receive "backup/done"

	assertEquals "A reap-time verification must not change the current dataset's common snapshot." \
		"tank/current@anchor	1" "$g_last_common_snap"
	assertEquals "A reap-time verification must not change the current dataset's transfer list." \
		"tank/current@next	2" "$g_src_snapshot_transfer_list"
	assertEquals "The verified dataset should be unmarked." "" "$g_zxfer_diverged_converged_datasets"
}

# zxfer-test-fragment: suites/zxfer_snapshot_plan_live_replan_tests.sh
# shellcheck source=tests/suites/zxfer_snapshot_plan_live_replan_tests.sh
. "$TESTS_DIR/suites/zxfer_snapshot_plan_live_replan_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_snapshot_plan.sh" \
		"$TESTS_DIR/suites/zxfer_snapshot_plan_live_replan_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
