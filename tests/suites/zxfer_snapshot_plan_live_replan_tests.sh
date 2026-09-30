#!/bin/sh
# The live re-plan of src/zxfer_snapshot_plan.sh
# (zxfer_reconcile_live_destination_snapshot_state), run by
# tests/test_zxfer_snapshot_plan.sh. tests/test_contract_planning.sh pins
# that only a dataset whose -d destroy ran is listed again and re-planned;
# this table pins what the re-plan publishes when the live rows drifted.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Print one "PREFIX@name<TAB>guid" row per "name:guid" word of WORDS; a bare
# "name" prints a guid-less row and "-" prints nothing.
# Usage: replan_rows PREFIX WORDS
replan_rows() {
	# shellcheck disable=SC2086  # WORDS split into records on purpose.
	for l_replan_word in $2; do
		case $l_replan_word in
		-) ;;
		*:*) printf '%s@%s\t%s\n' "$1" "${l_replan_word%%:*}" "${l_replan_word#*:}" ;;
		*) printf '%s@%s\n' "$1" "$l_replan_word" ;;
		esac
	done
}

# Each row: the anchor and pending records that inspect published before the
# -d destroy, the live destination rows after it, and the anchor, pending
# records and destination presence the re-plan publishes. Only the anchor or
# a pending snapshot may become the anchor; otherwise the records stay
# queued without one, so the seed refuses a destination that still has
# snapshots and re-seeds an emptied one from the old anchor.
test_reconcile_live_destination_snapshot_state_republishes_the_plan_from_live_rows() {
	zxfer_test_stage_source_records "$(replan_rows tank/src "snap4:444 snap3:333 snap2:222 snap1:111")"
	zxfer_run_destination_zfs_cmd() {
		[ -z "$REPLAN_LIVE" ] || printf '%s\n' "$REPLAN_LIVE"
	}
	while IFS='|' read -r replan_anchor replan_pending replan_live \
		replan_want_anchor replan_want_pending replan_want_has; do
		g_did_delete_dest_snapshots=1
		g_actual_dest="backup/target/src"
		zxfer_publish_snapshot_transfer_plan "$(replan_rows tank/src "$replan_anchor")" \
			"$(replan_rows tank/src "$replan_pending")" 1
		REPLAN_LIVE=$(replan_rows backup/target/src "$replan_live")
		zxfer_reconcile_live_destination_snapshot_state tank/src </dev/null
		assertEquals "Live rows [$replan_live] after anchor [$replan_anchor] and pending [$replan_pending]." \
			"$(replan_rows tank/src "$replan_want_anchor")|$(replan_rows tank/src "$replan_want_pending")|$replan_want_has" \
			"$g_last_common_snap|$g_src_snapshot_transfer_list|$g_dest_has_snapshots"
	done <<'EOF'
snap2:222|snap3:333 snap4:444|snap1:111 snap2:222|snap2:222|snap3:333 snap4:444|1
snap1:111|snap2:222 snap3:333 snap4:444|snap4:444|snap4:444|-|1
-|snap1:111 snap2:222 snap3:333 snap4:444|snap1:111 snap3:333|snap3:333|snap4:444|1
snap2:222|snap3:333 snap4:444|snap1:111|-|snap2:222 snap3:333 snap4:444|1
snap1:111|snap2:222 snap3:333 snap4:444|unrelated:999|-|snap1:111 snap2:222 snap3:333 snap4:444|1
snap1:111|snap2:222 snap3:333 snap4:444|-|-|snap1:111 snap2:222 snap3:333 snap4:444|0
-|snap1 snap2 snap3 snap4|snap1:111 snap3:333|-|snap1 snap2 snap3 snap4|1
EOF
}
