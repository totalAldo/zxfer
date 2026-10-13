#!/bin/sh
#
# shunit2 tests for src/zxfer_snapshot_plan.sh: the planning decisions no
# contract case isolates (retention, rollback eligibility, -g, divergence
# records, the record slices, the live re-plan table in the fragment below)
# and the failures the fault injector cannot reach (awk, date, record files).
# tests/test_contract_planning.sh pins the deletes, the divergence contract
# and the re-plan end to end.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# The live re-plan cases stage source records with its helper.
# shellcheck source=tests/helpers/replication_fixtures.sh
. "$TESTS_DIR/helpers/replication_fixtures.sh"

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
	zxfer_reset_snapshot_delete_artifact_state
	zxfer_reset_snapshot_plan_state
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

test_snapshot_plan_owner_operations_publish_and_validate_state() {
	zxfer_publish_snapshot_transfer_plan \
		"tank/src@snap1" "tank/src@snap2" "tank/src@snap3" 1 "private-plan"

	assertEquals "Publishing a transfer plan should update the last common snapshot." \
		"tank/src@snap1" "$g_last_common_snap"
	assertEquals "Publishing a transfer plan should update the first pending record." \
		"tank/src@snap2" "$g_src_snapshot_transfer_first"
	assertEquals "Publishing a transfer plan should update destination snapshot presence." \
		1 "$g_dest_has_snapshots"

	invalid_plan_status=0
	zxfer_publish_snapshot_transfer_plan "" "" "" invalid "" || invalid_plan_status=$?
	assertEquals "Transfer-plan publication should reject invalid destination presence values." \
		2 "$invalid_plan_status"

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

# Stage tagged delete rows; creation policy consumes the same checked format as
# pass and live plans, including non-delete records that must be ignored.
prepare_test_delete_creation_state() {
	delete_plan_file=$TEST_TMPDIR/creation.plan
	printf 'send\ttank/src@ignored\t99\n' >"$delete_plan_file"
	printf '%s\n' "$1" | awk 'NF { print "delete\t" $0 }' >>"$delete_plan_file"
	zxfer_prepare_snapshot_delete_creation_state "$delete_plan_file"
}

plan_delete_records() {
	awk 'substr($0, 1, 7) == "delete\t" { print substr($0, 8) }' "$g_zxfer_plan_file"
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

		prepare_test_delete_creation_state "$delete_list"

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
	prepare_test_delete_creation_state "$delete_list" <<-EOF
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
			prepare_test_delete_creation_state "$delete_list"
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
			prepare_test_delete_creation_state "tank/fs@snap3"
		)
	) || status=$?

	assertEquals "A failed evaluation should keep the awk status." 57 "$status"
	assertEquals "A failed evaluation should fail closed with rollback left eligible." \
		"Failed to evaluate destination snapshot creation times while planning snapshot deletions. newer=1" \
		"$output"
}

test_grandfather_policy_allows_young_snapshots_and_blocks_the_first_old_or_unknown_one_in_the_calling_shell() {
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
			prepare_test_delete_creation_state "backup/dst@young"
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
			prepare_test_delete_creation_state "backup/dst@young
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
			prepare_test_delete_creation_state "backup/dst@edge"
		)
	)
	assertContains "A snapshot exactly -g days old should be protected." \
		"$edge_output" "Snapshot age : 5 days old"

	# An unknown creation time (a non-numeric value or no row) is protected.
	for unknown_rows in "backup/dst@snap1	unknown" ""; do
		unknown_status=0
		unknown_output=$(
			(
				stub_creation_rows "$unknown_rows"
				zxfer_throw_error() {
					printf '%s\n' "$1"
					exit 1
				}
				prepare_test_delete_creation_state "backup/dst@snap1"
				printf 'unreachable\n'
			)
		) || unknown_status=$?
		assertEquals "-g must fail closed on an unknown creation time [$unknown_rows]." \
			"1|Couldn't determine creation time for destination snapshot backup/dst@snap1." \
			"$unknown_status|$unknown_output"
	done

	g_option_g_grandfather_protection=""
	g_actual_dest=""
	prepare_test_delete_creation_state "backup/dst@old"
	assertEquals "Without -g or a common snapshot no query runs." 0 "$?"
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
			prepare_test_delete_creation_state "backup/dst@snap1"
			printf 'unreachable\n'
		)
	) || status=$?

	assertEquals "-g must fail closed when date +%s gives no epoch." 1 "$status"
	assertEquals "-g should say it could not read the current time." \
		"Failed to read the current time for grandfather protection (-g)." "$output"
}

# The -g error dates a snapshot with BSD date -r, else GNU date -d @EPOCH,
# else the raw epoch; a non-numeric epoch renders nothing.
test_creation_epoch_display_falls_back_from_bsd_to_gnu_date_to_the_raw_epoch() {
	for display_case in "r|123|date-r-rendered" "d|123|date-d-rendered" \
		"none|123|123 (unix epoch)" "none|not-a-number|status=1"; do
		display_rest=${display_case#*|}
		display_output=$(
			(
				DISPLAY_DATE=${display_case%%|*}
				# bash 3.2 misreads a case pattern's ")" inside $( ).
				date() {
					if [ "$DISPLAY_DATE:$*" = "r:-r 123" ]; then
						printf '%s\n' date-r-rendered
					elif [ "$DISPLAY_DATE:$*" = "d:-d @123" ]; then
						printf '%s\n' date-d-rendered
					else
						return 1
					fi
				}
				zxfer_format_snapshot_creation_epoch_for_display "${display_rest%%|*}" ||
					printf 'status=%s\n' "$?"
			)
		)
		assertEquals "Creation-epoch display [$display_case]." \
			"${display_rest#*|}" "$display_output"
	done

	display_output=$(
		(
			g_option_g_grandfather_protection=1
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
	assertContains "The -g error should fall back to the raw epoch when no date renders." \
		"$display_output" "Snapshot date: 1672531200 (unix epoch)."
}

test_snapshot_plan_caps_divergence_examples_at_three() {
	stage_plan_record_files "tank/src@snapA	1
tank/src@snapB	2
tank/src@snapC	3
tank/src@snapD	4" "backup/dst@snapA	9
backup/dst@snapB	8
backup/dst@snapC	7
backup/dst@snapD	6"
	zxfer_plan_dataset_snapshots tank/src backup/dst
	assertEquals "Every diverged snapshot should be counted." 4 "$g_zxfer_diverged_snapshot_count"
	assertEquals "Only the first three source-order examples should be retained." \
		"snapA	1	9
snapB	2	8
snapC	3	7" "$g_zxfer_diverged_snapshot_examples"
	stage_plan_record_files "" ""
	zxfer_plan_dataset_snapshots tank/src backup/dst
	assertEquals "An empty scan resets count and examples." "0|" \
		"$g_zxfer_diverged_snapshot_count|$g_zxfer_diverged_snapshot_examples"
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
		"tank/src@s2	2" "$g_zxfer_plan_transfer_first"
	assertEquals "Rows of other destination datasets must never be deleted." \
		"" "$(plan_delete_records)"
	assertEquals "The plan should report presence for the exact destination dataset." \
		1 "$g_zxfer_plan_dest_has_snapshots"
	assertEquals "Only the dataset's own source rows should be counted." 2 "$g_zxfer_plan_source_count"
}

test_plan_dataset_snapshots_destination_presence_resets_for_each_exact_dataset() {
	stage_plan_record_files "tank/src@s2	2" "backup/dst@s1	1
backup/dst@s0	0
backup/dst1@s1	11
backup/dst/child@s1	31"
	zxfer_plan_dataset_snapshots tank/src backup/dst >"$TEST_TMPDIR/plan.stdout"
	assertEquals "Validated destination records should remain in the input file, with no planner stdout copy." \
		"" "$(cat "$TEST_TMPDIR/plan.stdout")"
	assertEquals "Multiple validated snapshots still publish one presence value." \
		1 "$g_zxfer_plan_dest_has_snapshots"
	zxfer_plan_dataset_snapshots tank/src backup/missing
	assertEquals "A new plan without exact destination rows must discard the preceding presence." \
		0 "$g_zxfer_plan_dest_has_snapshots"
}

test_plan_dataset_snapshots_rejects_incomplete_or_invalid_summary_rows() {
	stage_plan_record_files "tank/src@s1	1" "backup/dst@s1	1"
	# Blank lines are real empty endpoints and an empty destroy target.
	valid_summary="__ZXFER_SNAPSHOT_SUMMARY__$ZXFER_LF$ZXFER_LF$ZXFER_LF${ZXFER_LF}1${ZXFER_LF}1${ZXFER_LF}0${ZXFER_LF}__ZXFER_SNAPSHOT_PLAN__"
	for count_rows in "" "sources	1" "destination_present	1" \
		"${valid_summary%__ZXFER_SNAPSHOT_PLAN__}" \
		"${valid_summary%__ZXFER_SNAPSHOT_PLAN__}wrong-marker" \
		"$valid_summary${ZXFER_LF}send	tank/src@s2	2" \
		"$valid_summary$ZXFER_LF$valid_summary" \
		"__ZXFER_SNAPSHOT_SUMMARY__$ZXFER_LF$ZXFER_LF$ZXFER_LF${ZXFER_LF}2${ZXFER_LF}1${ZXFER_LF}0${ZXFER_LF}__ZXFER_SNAPSHOT_PLAN__" \
		"__ZXFER_SNAPSHOT_SUMMARY__$ZXFER_LF$ZXFER_LF$ZXFER_LF${ZXFER_LF}1${ZXFER_LF}broken${ZXFER_LF}0${ZXFER_LF}__ZXFER_SNAPSHOT_PLAN__" \
		"__ZXFER_SNAPSHOT_SUMMARY__$ZXFER_LF$ZXFER_LF$ZXFER_LF${ZXFER_LF}1${ZXFER_LF}1${ZXFER_LF}bad${ZXFER_LF}__ZXFER_SNAPSHOT_PLAN__"; do
		output=$(
			(
				# Replace only the plan program; allocation, reporting and
				# owner-side validation remain in use.
				ZXFER_PLAN_DATASET_SNAPSHOTS_AWK='
BEGIN { print ENVIRON["ZXFER_TEST_PLAN_ROWS"] > ENVIRON["ZXFER_AWK_PLAN_FILE"]; exit }'
				ZXFER_TEST_PLAN_ROWS=$count_rows
				export ZXFER_TEST_PLAN_ROWS
				zxfer_plan_dataset_snapshots tank/src backup/dst
				printf 'planned\n'
			) 2>&1
		)
		assertEquals "Incomplete or invalid summary rows must fail closed [$count_rows]." 1 "$?"
		assertContains "$output" "Failed to determine the last common snapshot"
		assertNotContains "$output" planned
	done

	# Every truncated prefix fails, even one ending on an otherwise valid row.
	plan_file="$TEST_TMPDIR/truncated-summary"
	printf '%s\n' "$valid_summary" >"$plan_file.complete"
	for prefix in 1 2 3 4 5 6 7; do
		head -n "$prefix" "$plan_file.complete" >"$plan_file"
		status=0
		zxfer_load_snapshot_plan_summary "$plan_file" || status=$?
		assertEquals "A summary truncated after line $prefix must fail." 1 "$status"
	done
	printf '%s' "$valid_summary" >"$plan_file"
	status=0
	zxfer_load_snapshot_plan_summary "$plan_file" || status=$?
	assertEquals "An unterminated completion row must fail." 1 "$status"
	printf '%s\n%s' "$valid_summary" trailing-partial-row >"$plan_file"
	status=0
	zxfer_load_snapshot_plan_summary "$plan_file" || status=$?
	assertEquals "An unterminated row after completion must fail." 1 "$status"
}

test_snapshot_plan_summary_preserves_empty_and_literal_records_without_evaluation() {
	plan_file="$TEST_TMPDIR/literal-summary"
	# shellcheck disable=SC2016 # Shell-looking record bytes must stay literal.
	literal_record='tank/space \path $(touch forbidden) `touch forbidden` '\''quote'\''@snap'
	literal_record="$literal_record${ZXFER_TAB}123"
	printf '%s\n' "common${ZXFER_TAB}$literal_record" \
		__ZXFER_SNAPSHOT_SUMMARY__ "" "$literal_record" "" 1 2 0 \
		__ZXFER_SNAPSHOT_PLAN__ >"$plan_file"
	g_last_common_snap=active
	g_zxfer_plan_transfer_first=stale
	g_zxfer_plan_destroy_target=stale
	zxfer_load_snapshot_plan_summary "$plan_file"
	assertEquals "The summary loader must retain literal common-record bytes." \
		"$literal_record" "$g_zxfer_plan_common_snapshot"
	assertEquals "The summary loader must retain record tabs and backslashes." \
		"$literal_record" "$g_zxfer_plan_transfer_final"
	assertEquals "Empty endpoints remain empty." "" "$g_zxfer_plan_transfer_first"
	assertEquals "An empty destroy target remains empty." "" "$g_zxfer_plan_destroy_target"
	assertEquals "Summary loading never publishes active transfer state." active "$g_last_common_snap"
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
backup/fs@zeta" "$(plan_delete_records)"
	assertEquals "The newest guid-exact match is the common snapshot." \
		"tank/fs@snap3	333" "$g_zxfer_plan_common_snapshot"
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
		"$g_zxfer_diverged_snapshot_examples"

	stage_plan_record_files "tank/src@only_on_source	111" "backup/dst@only_on_dest	222"
	zxfer_plan_dataset_snapshots "tank/src" "backup/dst"
	assertEquals "Disjoint snapshot names should report no divergence." "" "$g_zxfer_diverged_snapshot_examples"
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
	# Losing the output must not replace the producer's meaningful status.
	# shellcheck disable=SC2016 # The fixture shell expands its output path.
	printf '#!/bin/sh\nrm -f "$ZXFER_AWK_PLAN_FILE"\nexit 61\n' >"$failing_awk"
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

test_snapshot_planner_preserves_the_original_at_field_interpretation() {
	# The old -F@ planner rejects a second @ before the GUID field. Batch
	# preparation must defer that same status, rather than accept the row.
	stage_plan_record_files "tank/src@bad@name	1" "backup/src@s1	1"
	g_initial_source=tank/src
	g_initial_source_had_trailing_slash=0
	g_destination=backup
	for mode in direct prepared; do
		output=$(
			(
				if [ "$mode" = prepared ]; then
					zxfer_prepare_snapshot_plans "1	tank/src"
					zxfer_select_snapshot_plan 1 tank/src
				fi
				zxfer_plan_dataset_snapshots tank/src backup/src
			) 2>&1
		)
		assertEquals "[$mode] preserves the original malformed-record status." 3 "$?"
		assertContains "$output" 'Failed to determine the last common snapshot'
	done
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

test_prepare_snapshot_plans_keeps_source_order_and_isolates_each_dataset() {
	stage_split_record_files
	zxfer_prepare_snapshot_plans "1	tank/src
2	tank/src/a/b
3	tank/src/a/bc
4	tank/src/empty"
	prepare_status=$?
	base=$g_zxfer_snapshot_slice_base

	assertEquals "Malformed identity is deferred to its dataset, not preparation." 0 "$prepare_status"
	assertEquals "Prepared plans retain the identity of their input files." \
		"$g_zxfer_source_snapshot_record_cache_file
$g_zxfer_destination_snapshot_record_cache_file" "$g_zxfer_snapshot_slice_records"
	assertEquals "Source slices retain newest-first order, including invalid identity for live rechecks." \
		"tank/src@s2	22
tank/src@guidless
tank/src@s1	21" "$(cat "$base.1.s")"
	assertContains "The invalid root plan records the original status." "$(cat "$base.1.p")" "error	3"
	assertEquals "A later dataset still has a complete independent plan." \
		"send	tank/src/a/b@s2	52
common	tank/src/a/b@s1	51
delete	backup/src/a/b@s0
__ZXFER_SNAPSHOT_SUMMARY__
tank/src/a/b@s2	52
tank/src/a/b@s2	52
backup/src/a/b@s0
1
2
0
__ZXFER_SNAPSHOT_PLAN__" "$(cat "$base.2.p")"
	assertEquals "A dataset must never take a sibling whose name it prefixes." \
		"send	tank/src/a/bc@s2	62
common	tank/src/a/bc@s1	61
__ZXFER_SNAPSHOT_SUMMARY__
tank/src/a/bc@s2	62
tank/src/a/bc@s2	62

1
2
0
__ZXFER_SNAPSHOT_PLAN__" "$(cat "$base.3.p")"
	assertEquals "An empty dataset gets a complete plan, with no leaked state." \
		"__ZXFER_SNAPSHOT_SUMMARY__



0
0
0
__ZXFER_SNAPSHOT_PLAN__" "$(cat "$base.4.p")"
	assertTrue "An empty dataset retains an empty source slice." "[ -f '$base.4.s' ] && [ ! -s '$base.4.s' ]"
	assertFalse "No slice exists beyond the queue." "[ -e '$base.5.s' ]"
	assertTrue "The spent keyed copy is emptied." "[ -f '$base' ] && [ ! -s '$base' ]"
	assertEquals "Prepared plans are private." "$base.2.p" "$(find "$base.2.p" -perm 0600 2>/dev/null)"
	assertEquals "Source slices are private." "$base.2.s" "$(find "$base.2.s" -perm 0600 2>/dev/null)"

	zxfer_prepare_snapshot_plans "1	tank/src/a/b"
	assertEquals "Unlisted siblings stay out of the source slice." \
		"tank/src/a/b@s2	52
tank/src/a/b@s1	51" "$(cat "$base.1.s")"
	assertContains "Destination-only rows keep their own dataset's plan." \
		"$(cat "$base.1.p")" "delete	backup/src/a/b@s0"
}

test_prepare_snapshot_plans_maps_a_trailing_slash_destination_root() {
	stage_plan_record_files "tank/src/c@s1	31
tank/src@s1	21" "backup@s1	21
backup/c@s1	31
backup2/c@s1	41"
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_destination="backup"
	zxfer_prepare_snapshot_plans "1	tank/src
2	tank/src/c"
	zxfer_select_snapshot_plan 1 tank/src
	zxfer_plan_dataset_snapshots tank/src backup
	assertEquals "The destination root itself maps to the source root." "tank/src@s1	21" "$g_zxfer_plan_common_snapshot"
	zxfer_select_snapshot_plan 2 tank/src/c
	zxfer_plan_dataset_snapshots tank/src/c backup/c
	assertEquals "Children map under the root without sibling-root identity." "tank/src/c@s1	31" "$g_zxfer_plan_common_snapshot"
	assertEquals "An unrelated destination root is never a delete candidate." "" "$(plan_delete_records)"
}

test_prepare_snapshot_plans_publishes_nothing_without_input_or_when_a_stage_fails() {
	stage_split_record_files
	real_awk=$g_cmd_awk
	failing_key_awk="$TEST_TMPDIR/failing_key_awk.sh"
	failing_write_awk="$TEST_TMPDIR/failing_write_awk.sh"
	printf '#!/bin/sh\nexit 44\n' >"$failing_key_awk"
	# shellcheck disable=SC2016  # the wrapper script expands these itself.
	printf '#!/bin/sh\n[ -z "${ZXFER_AWK_SLICE_BASE:-}" ] || exit 46\nexec "%s" "$@"\n' \
		"$real_awk" >"$failing_write_awk"
	chmod +x "$failing_key_awk" "$failing_write_awk"

	# Without record files or listed datasets the split succeeds quietly and
	# the planner reads (and reports on) the whole record files.
	for split_stage in allocation key sort write no_record_file no_dataset; do
		output=$(
			g_zxfer_snapshot_slice_records="stale"
			g_zxfer_snapshot_slice_key=9
			split_list="1	tank/src"
			if [ "$split_stage" = allocation ]; then
				zxfer_ensure_runtime_artifact_file() { return 43; }
			elif [ "$split_stage" = key ]; then
				g_cmd_awk=$failing_key_awk
			elif [ "$split_stage" = sort ]; then
				sort() { return 45; }
			elif [ "$split_stage" = write ]; then
				g_cmd_awk=$failing_write_awk
			elif [ "$split_stage" = no_record_file ]; then
				g_zxfer_destination_snapshot_record_cache_file=""
			else
				split_list=""
			fi
			split_status=0
			zxfer_prepare_snapshot_plans "$split_list" || split_status=$?
			printf 'status=%s records=<%s> key=<%s>\n' "$split_status" \
				"$g_zxfer_snapshot_slice_records" "$g_zxfer_snapshot_slice_key"
		)
		case $split_stage in
		allocation) expected_status=43 ;;
		key) expected_status=44 ;;
		sort) expected_status=45 ;;
		write) expected_status=46 ;;
		*) expected_status=0 ;;
		esac
		assertEquals "A split [$split_stage] should return $expected_status and publish no slices or selection." \
			"status=$expected_status records=<> key=<>" "$output"
	done
}

test_select_snapshot_plan_selects_only_numbered_positions_of_prepared_plans() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_destination="backup"

	g_zxfer_snapshot_slice_records=""
	zxfer_select_snapshot_plan 2 "tank/src/a"
	assertEquals "Without published slices nothing should be selected." "" "$g_zxfer_snapshot_slice_key"

	g_zxfer_snapshot_slice_records="published"
	zxfer_select_snapshot_plan "tank/src/a" "tank/src/a"
	assertEquals "A position that is not a number should select nothing." "" "$g_zxfer_snapshot_slice_key"

	zxfer_select_snapshot_plan 2 "tank/src/a"
	assertEquals "A position should select that dataset and its mapped destination." \
		"2|tank/src/a|backup/src/a" \
		"$g_zxfer_snapshot_slice_key|$g_zxfer_snapshot_slice_source|$g_zxfer_snapshot_slice_destination"
}

test_plan_dataset_snapshots_loads_prepared_plans_without_running_awk() {
	stage_split_record_files
	zxfer_prepare_snapshot_plans "1	tank/src
2	tank/src/a/b"
	# The whole files change after the split: a slice-served plan never
	# reads them.
	printf '%s\n' "tank/src/a/b@s9	59" >"$g_zxfer_source_snapshot_record_cache_file"

	zxfer_select_snapshot_plan 2 "tank/src/a/b"
	saved_awk=$g_cmd_awk
	g_cmd_awk=false
	zxfer_plan_dataset_snapshots "tank/src/a/b" "backup/src/a/b"
	g_cmd_awk=$saved_awk
	assertEquals "The selected dataset should plan from its slices." \
		"tank/src/a/b@s1	51|tank/src/a/b@s2	52|backup/src/a/b@s0|2" \
		"$g_zxfer_plan_common_snapshot|$g_zxfer_plan_transfer_first|$(plan_delete_records)|$g_zxfer_plan_source_count"

	zxfer_plan_dataset_snapshots "tank/src" "backup/src"
	assertEquals "Another dataset should read the whole record files." \
		0 "$g_zxfer_plan_source_count"

	g_zxfer_source_snapshot_record_cache_file="$TEST_TMPDIR/rediscovered_source.records"
	printf '%s\n' "tank/src/a/b@s9	59" >"$g_zxfer_source_snapshot_record_cache_file"
	zxfer_plan_dataset_snapshots "tank/src/a/b" "backup/src/a/b"
	assertEquals "Slices cut from earlier record files must never be used." \
		"tank/src/a/b@s9	59" "$g_zxfer_plan_transfer_first"
}

test_prepared_snapshot_plan_failures_never_publish_active_state() {
	for damage in missing incomplete malformed_error guidless; do
		stage_split_record_files
		zxfer_prepare_snapshot_plans "1	tank/src
2	tank/src/a/b"
		plan_file=$g_zxfer_snapshot_slice_base.2.p
		case $damage in
		missing) rm "$plan_file" ;;
		incomplete) printf 'destination_present\t1\n' >"$plan_file" ;;
		malformed_error) printf 'error\tbroken\n' >"$plan_file" ;;
		guidless) plan_file=$g_zxfer_snapshot_slice_base.1.p ;;
		esac
		output=$(
			(
				g_last_common_snap=active
				if [ "$damage" = guidless ]; then
					zxfer_select_snapshot_plan 1 tank/src
					zxfer_plan_dataset_snapshots tank/src backup/src
				else
					zxfer_select_snapshot_plan 2 tank/src/a/b
					zxfer_plan_dataset_snapshots tank/src/a/b backup/src/a/b
				fi
				printf 'published=%s\n' "$g_last_common_snap"
			) 2>&1
		)
		status=$?
		expected=1
		[ "$damage" != guidless ] || expected=3
		assertEquals "Prepared plan [$damage] preserves failure status." "$expected" "$status"
		assertContains "$output" 'Failed to determine the last common snapshot'
		assertNotContains "$output" 'published='
	done
}

# Both consumers must stop before planning from an owned file that now holds
# a partial capture. The retained path is storage, not a successful result.
test_live_destination_planning_stops_on_partial_listing_failures() {
	for operation in reconcile verify; do
		output=$(
			g_did_delete_dest_snapshots=1
			g_last_common_snap="tank/current@anchor	1"
			g_actual_dest=backup/current
			g_zxfer_diverged_converged_datasets="backup/current	tank/current"
			zxfer_run_destination_zfs_cmd() {
				printf 'backup/current@partial\t1\n'
				printf 'listing failed\n' >&2
				return 42
			}
			zxfer_plan_dataset_snapshots() { printf 'unexpected planning\n'; }
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			if [ "$operation" = reconcile ]; then
				zxfer_reconcile_live_destination_snapshot_state tank/current
			else
				zxfer_verify_converged_destination_after_receive backup/current
			fi
		)
		status=$?
		assertEquals "A failed live listing must stop $operation." 1 "$status"
		assertContains "The capture's diagnostic survives for $operation." "$output" "listing failed"
		assertNotContains "Partial rows cannot become a plan in $operation." "$output" "unexpected planning"
	done
}

test_verify_converged_destination_keeps_the_current_dataset_plan() {
	stage_plan_record_files "tank/current@next	2
tank/current@anchor	1
tank/done@s1	1" "backup/current@anchor	1
backup/done@s1	1"
	zxfer_plan_dataset_snapshots tank/current backup/current
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" \
		"$g_zxfer_plan_transfer_first" "$g_zxfer_plan_transfer_final" \
		1 "$g_zxfer_plan_file"
	active_file=$g_zxfer_transfer_plan_file
	active_records=$(cat "$active_file")
	g_zxfer_diverged_converged_datasets="backup/done	tank/done"
	live_file="$TEST_TMPDIR/verify_keeps_plan.records"
	printf '%s\n' "backup/done@s1	1" >"$live_file"
	zxfer_get_live_destination_record_file() {
		g_zxfer_live_destination_listing_file=$live_file
	}
	zxfer_verify_converged_destination_after_receive backup/done
	assertEquals "Reaping another dataset keeps the current range and eligibility file." \
		"tank/current@anchor	1|tank/current@next	2|tank/current@next	2|$active_file" \
		"$g_last_common_snap|$g_src_snapshot_transfer_first|$g_src_snapshot_transfer_final|$g_zxfer_transfer_plan_file"
	assertEquals "Verification must not overwrite the active identities." "$active_records" "$(cat "$active_file")"
	assertEquals "The verified dataset should be unmarked." "" "$g_zxfer_diverged_converged_datasets"
	probe_file=$g_zxfer_plan_file
	zxfer_plan_dataset_snapshots tank/done backup/done
	assertEquals "Repeated probes reuse the inactive scratch plan." "$probe_file" "$g_zxfer_plan_file"
	# Once that probe is active, the other scratch path becomes reusable.
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" "" "" 1 "$g_zxfer_plan_file"
	zxfer_plan_dataset_snapshots tank/current backup/current
	assertEquals "Alternating active plans need only two scratch files." "$active_file" "$g_zxfer_plan_file"
}

test_snapshot_recheck_allocation_failure_preserves_status_and_active_plan() {
	stage_plan_record_files "tank/src@s2	2
tank/src@s1	1" "backup/dst@s1	1"
	zxfer_plan_dataset_snapshots tank/src backup/dst
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" "$g_zxfer_plan_transfer_first" "$g_zxfer_plan_transfer_final" 1 "$g_zxfer_plan_file"
	active_file=$g_zxfer_transfer_plan_file
	active_records=$(cat "$active_file")
	status=0
	output=$(
		zxfer_ensure_runtime_artifact_file() { return 43; }
		zxfer_throw_error() {
			printf '%s active=%s\n' "$1" "$g_zxfer_transfer_plan_file"
			exit "${2:-1}"
		}
		zxfer_plan_dataset_snapshots tank/src backup/dst
	) || status=$?
	assertEquals "Allocating an inactive plan preserves the allocator's status." 43 "$status"
	assertEquals "The failed probe retains the current plan handle." "Failed to allocate the snapshot plan file. active=$active_file" "$output"
	assertEquals "The failed probe cannot truncate current eligibility." "$active_records" "$(cat "$active_file")"
}

test_snapshot_transfer_skip_reporting_preserves_literal_records_and_active_state() {
	# No interpretation of backslashes, percent signs or maximum-width GUIDs;
	# one real planner result exercises the diagnostic rows, not a hand-built tag.
	l_skip_name='old\c%literal'
	stage_plan_record_files "tank/src@common	18446744073709551615
tank/src@$l_skip_name	18446744073709551614" "backup/dst@common	18446744073709551615"
	g_option_v_verbose=1
	zxfer_plan_dataset_snapshots tank/src backup/dst
	zxfer_publish_snapshot_transfer_plan "$g_zxfer_plan_common_snapshot" "" "" 1 "$g_zxfer_plan_file"
	l_active_plan=$(cat "$g_zxfer_transfer_plan_file")
	l_active_state=$g_last_common_snap$ZXFER_LF$g_src_snapshot_transfer_first$ZXFER_LF$g_src_snapshot_transfer_final
	zxfer_report_snapshot_transfer_skips tank/src backup/dst "$g_zxfer_transfer_plan_file" "$g_last_common_snap" \
		>"$TEST_TMPDIR/skip-report.stdout"
	l_skip_output=$(cat "$TEST_TMPDIR/skip-report.stdout")
	assertContains "The reason must retain literal snapshot and GUID bytes." "$l_skip_output" \
		"tank/src@$l_skip_name (GUID: 18446744073709551614): before newest common snapshot tank/src@common (GUID: 18446744073709551615)"
	assertEquals "Reporting must not rewrite the plan file." "$l_active_plan" "$(cat "$g_zxfer_transfer_plan_file")"
	assertEquals "Reporting must not republish active transfer state." "$l_active_state" \
		"$g_last_common_snap$ZXFER_LF$g_src_snapshot_transfer_first$ZXFER_LF$g_src_snapshot_transfer_final"

	g_option_v_verbose=0
	zxfer_plan_dataset_snapshots tank/src backup/dst
	assertFalse "Quiet plans must not retain diagnostic-only rows." "grep -q '^skip' '$g_zxfer_plan_file'"
	assertEquals "Quiet reporting must not open diagnostic files." "" \
		"$(zxfer_report_snapshot_transfer_skips tank/src backup/dst /nonexistent 'common')"
}

test_live_replan_never_adopts_a_skipped_older_snapshot_as_anchor() {
	stage_plan_record_files "tank/src@next	3
tank/src@anchor	2
tank/src@older	1" "backup/dst@anchor	2"
	g_option_v_verbose=1
	g_actual_dest=backup/dst
	zxfer_inspect_delete_snap 0 tank/src
	l_original_plan=$g_zxfer_transfer_plan_file
	assertTrue "The plan must retain the diagnostic identity for the older gap." \
		"grep -q '^skip' '$l_original_plan'"
	l_live_file="$TEST_TMPDIR/skip-anchor-live.records"
	printf 'backup/dst@older\t1\n' >"$l_live_file"
	zxfer_get_live_destination_record_file() {
		g_zxfer_live_destination_listing_file=$l_live_file
	}
	g_did_delete_dest_snapshots=1
	zxfer_reconcile_live_destination_snapshot_state tank/src
	assertEquals "An older skipped identity must not become an eligible live anchor." "" "$g_last_common_snap"
	assertEquals "The original anchor and pending range must remain the only eligible identities." \
		"tank/src@anchor	2|tank/src@next	3|$l_original_plan" \
		"$g_src_snapshot_transfer_first|$g_src_snapshot_transfer_final|$g_zxfer_transfer_plan_file"
}

test_snapshot_transfer_skip_reporting_fails_closed_on_unreadable_plan() {
	zxfer_test_capture_subshell_split "$TEST_TMPDIR/skip.stdout" "$TEST_TMPDIR/skip.stderr" "
		g_option_v_verbose=1
		zxfer_report_snapshot_transfer_skips tank/src backup/dst '$TEST_TMPDIR/nonexistent-skip.plan' 'tank/src@common${ZXFER_TAB}123'
	"
	assertEquals "Unreadable diagnostic plans must fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The failure must identify the affected source dataset." "$(cat "$TEST_TMPDIR/skip.stderr")" \
		"Failed to read snapshot transfer skip reasons for [tank/src]."
}

test_snapshot_plan_preserves_range_endpoints_and_reverse_destroy_order() {
	stage_plan_record_files "tank/src@s4	4
tank/src@s3	3
tank/src@s2	2
tank/src@s1	1" "backup/dst@extraB	8
backup/dst@s1	1
backup/dst@extraA	9"
	zxfer_plan_dataset_snapshots tank/src backup/dst
	assertEquals "The range spans the oldest and newest pending identities." \
		"tank/src@s2	2|tank/src@s4	4" \
		"$g_zxfer_plan_transfer_first|$g_zxfer_plan_transfer_final"
	assertEquals "One destroy target retains reverse listing order." \
		"backup/dst@extraA,extraB" "$g_zxfer_plan_destroy_target"
	assertEquals "Deletion policy still sees original listing order." \
		"backup/dst@extraB
backup/dst@extraA" "$(plan_delete_records)"
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
