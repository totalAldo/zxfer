#!/bin/sh
#
# Black-box replication and send/receive suite for ./zxfer.
#
# Drives the real launcher against the canned zfs of tests/helpers/blackbox.sh
# and asserts on its argv log, the exit status, the failure report and what
# each receive and progress dialog read. Pins, in file order:
#   - -D streams reach both the receive and the dialog under -j 1 (eval in
#     zxfer's shell) and -j 3 (job shells without zxfer functions), and -n
#     never sends;
#   - seeding: a missing child is live-probed, then fully received; an empty
#     one gets -F, which no later receive inherits; a single pending snapshot
#     is only seeded; a failed probe or a child whose snapshots share no guid
#     with the source stops the run before anything reaches that child;
#   - the re-plan after a -d destroy: a lost anchor stops the run, an emptied
#     destination is re-seeded from its anchor, and -F rolls back only for a
#     newer destroyed snapshot with a send pending;
#   - the pass: -n previews of -s and -m with every skipped live step named,
#     -s -N sending from the rediscovery, -m taking one snapshot after every
#     unmount, -s -Y taking its snapshot on the first pass only, and -Y
#     re-reading properties, keeping the newest -k rows, and counting sends
#     under -V;
#   - the post-seed property pass after every receive (-j 1, -j 2 and -T),
#     and remote runs over an ssh that reads its stdin;
#   - -j: a descendant waits only for its own ancestor within the job limit,
#     a failed job on -T keeps its status and stops its running sibling, and
#     a converged destination is checked only after its receive;
#   - the pipeline: -D templates, size probes, fallbacks and failures, the
#     dialog's stdout and status, and -z across every ssh hop.
#
# shellcheck disable=SC1090,SC2016,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# Purpose: Put a strict receive in front of the canned zfs: each receive
# appends "DATASET BYTES" to $CASE_DIR/receive.bytes and fails on 0 bytes.
# Usage: sendrecv_write_strict_receive_zfs
sendrecv_write_strict_receive_zfs() {
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.canned" ||
		fail "Unable to stage the canned zfs behind the strict receive."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
case "\${1:-}" in
receive | recv)
	for strict_arg in "\$@"; do
		strict_dataset=\$strict_arg
	done
	strict_bytes=\$(wc -c | tr -d ' ')
	printf '%s %s\n' "\$strict_dataset" "\$strict_bytes" >>"$CASE_DIR/receive.bytes"
	if [ "\$strict_bytes" -eq 0 ]; then
		echo "cannot receive: failed to read from stream" >&2
		exit 1
	fi
	exec "$MOCKBIN_DIR/zfs.canned" "\$@" </dev/null
	;;
esac
exec "$MOCKBIN_DIR/zfs.canned" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Build the -j incremental environment with the strict receive, both
# %%size%% probes answering 4096, and a dialog that keeps one stream copy per
# run in $CASE_DIR/dialog.<pid>.stream.
# Usage: sendrecv_setup_progress_env <state-name>
sendrecv_setup_progress_env() {
	planning_setup_parallel_jobs_env "$1"
	sendrecv_write_strict_receive_zfs
	printf '4096\n' >"$STATE_DIR/written.value"
	printf 'size\t4096\n' >"$STATE_DIR/send_estimate.list"
	printf 'get -Hpo value written@* *\twritten.value\t0\nsend -nPv *\tsend_estimate.list\t0\n' \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append the size estimate manifest rules."
	cat >"$MOCKBIN_DIR/progress_dialog" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$CASE_DIR/dialog.argv"
cat >"$CASE_DIR/dialog.\$\$.stream"
EOF
	chmod +x "$MOCKBIN_DIR/progress_dialog"
}

# Purpose: Assert a -D run exited 0 and every dataset's stream reached its
# receive and its dialog exactly once.
# Usage: sendrecv_assert_every_stream_delivered <status> <label>
sendrecv_assert_every_stream_delivered() {
	assertEquals "$2 should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$1"
	planning_assert_no_mutations
	for l_sendrecv_suffix in "" /child1 /child2; do
		l_sendrecv_source="$ZXFER_MOCKBIN_SOURCE_ROOT$l_sendrecv_suffix"
		l_sendrecv_dest="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_sendrecv_suffix"
		assertEquals "$2: $l_sendrecv_dest must be received exactly once" \
			1 "$(grep -cx "receive $l_sendrecv_dest" "$ZFS_LOG")"
		assertTrue "$2: the dialog must start with the title and size for $l_sendrecv_source" \
			"grep -Fxq -- '$l_sendrecv_source@snap3 4096' '$CASE_DIR/dialog.argv'"
		assertTrue "$2: a dialog must read the $l_sendrecv_source stream" \
			"grep -Fxq 'ZXFERMOCKSTREAM send -I $l_sendrecv_source@snap2 $l_sendrecv_source@snap3' $CASE_DIR/dialog.*.stream"
	done
	assertEquals "$2: every receive must have read a non-empty stream: $(cat "$CASE_DIR/receive.bytes")" \
		"3 0" "$(awk '{ n++ } $2 <= 0 { empty++ } END { print n + 0, empty + 0 }' "$CASE_DIR/receive.bytes")"
	planning_assert_no_parallel_job_leftovers
}

# Invariant: with -j 3 each pipeline runs in a job shell that has no zxfer
# functions; its plain-shell -D stage must still feed both the receive and
# the dialog. Regression: the stage called a zxfer function, so every
# receive got an empty stream.
test_progress_dialog_with_parallel_jobs_delivers_every_stream() {
	sendrecv_setup_progress_env progress_j3

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 3 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	sendrecv_assert_every_stream_delivered $? "-j 3 -D"
	planning_assert_log_has_line \
		"get -Hpo value written@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT"
}

# Invariant: the same stage run by eval in zxfer's own shell (-j 1) feeds
# both the receive and the dialog, sized by the exact send estimate.
test_progress_dialog_in_the_foreground_delivers_every_stream() {
	sendrecv_setup_progress_env progress_j1

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -j 1 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	sendrecv_assert_every_stream_delivered $? "-j 1 -D"
	planning_assert_log_has_line \
		"send -nPv -I $ZXFER_MOCKBIN_SOURCE_ROOT@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT@snap3"
}

# Invariant: -n with -j and -D never sends, receives, probes a size, or
# starts a dialog; the job scheduler is never reached in a dry run.
test_dry_run_with_parallel_jobs_and_progress_never_sends() {
	sendrecv_setup_progress_env progress_dryrun

	TMPDIR="$JOB_TMP_DIR" planning_run_zxfer "$STATE_DIR" -n -j 3 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	dry_run_status=$?
	assertEquals "-n -j 3 -D should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$dry_run_status"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	assertFalse "a dry run must not start the progress dialog" \
		"[ -e '$CASE_DIR/dialog.argv' ]"
	assertFalse "a dry run must not probe a size" \
		"grep -q 'written@' '$ZFS_LOG'"
}

# ---------------------------------------------------------------------------
# Replication order: seeding, the re-plan after a -d destroy, and refusals.
# Each case starts from a fixture of tests/helpers/blackbox.sh and edits one
# dataset; S is the source root and D its mapped destination.

S=$ZXFER_MOCKBIN_SOURCE_ROOT
D=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT

# Purpose: Print the send, receive and MUTATE lines of the zfs log, for
# checks that do not depend on how they interleave.
# Usage: sendrecv_transfer_lines
sendrecv_transfer_lines() {
	grep -E '^(send|receive|MUTATE) ' "$ZFS_LOG" || :
}

# Purpose: Print one kind of zfs log line in log order: the stream sends
# (size probes left out), the receives, or the MUTATE lines. Either side of
# a `zfs send | zfs receive` pipeline may log first, so sends and receives
# are compared as two sequences.
# Usage: sendrecv_log_lines send|receive|MUTATE
sendrecv_log_lines() {
	grep "^$1 " "$ZFS_LOG" | grep -v '^send -nPv ' || :
}

# Purpose: Run planning_run_zxfer from /dev/null with the mock bin first on
# PATH (so the mock ssh is found), ssh calls logged to $CASE_DIR/ssh.log, and
# TMPDIR set when given. They are exported in a subshell: ksh93 exports no
# prefix assignment on a function call, and FreeBSD sh only names that
# already were.
# Usage: sendrecv_run_zxfer TMPDIR|"" STATE_DIR [zxfer-arg...]
sendrecv_run_zxfer() {
	# shellcheck disable=SC2030  # The exports are for this one zxfer run.
	(
		if [ -n "$1" ]; then
			TMPDIR=$1
			export TMPDIR
		fi
		PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		MOCK_SSH_LOG="$CASE_DIR/ssh.log"
		export PATH MOCK_SSH_LOG
		shift
		planning_run_zxfer "$@" </dev/null
	)
}

# Purpose: Leave destination child N in STATE_DIR without snapshots: no
# recursive or depth-1 snapshot rows, while the dataset itself stays listed.
# Usage: sendrecv_make_destination_child_empty N
sendrecv_make_destination_child_empty() {
	grep -v "^$D/child$1@" "$STATE_DIR/dst_snapshots.list" \
		>"$STATE_DIR/dst_snapshots.list.new" || :
	mv "$STATE_DIR/dst_snapshots.list.new" "$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to empty destination child$1."
	: >"$STATE_DIR/dst_d1_$1.list"
}

# Purpose: Remove destination child N from STATE_DIR: no snapshots, not in
# the dataset inventory, and `zfs list -H` answers that it does not exist.
# Usage: sendrecv_make_destination_child_missing N
sendrecv_make_destination_child_missing() {
	sendrecv_make_destination_child_empty "$1"
	grep -vx "$D/child$1" "$STATE_DIR/dst_datasets.list" \
		>"$STATE_DIR/dst_datasets.list.new" || :
	mv "$STATE_DIR/dst_datasets.list.new" "$STATE_DIR/dst_datasets.list" ||
		fail "Unable to drop child$1 from the destination inventory."
	printf "cannot open '%s': dataset does not exist\n" "$D/child$1" \
		>"$STATE_DIR/missing_child$1.list"
	printf 'list -H %s\tmissing_child%s.list\t1\n' "$D/child$1" "$1" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the missing-child rule."
}

# Purpose: Keep only @snap1 of source child N in STATE_DIR's listings.
# Usage: sendrecv_keep_only_first_source_snapshot N
sendrecv_keep_only_first_source_snapshot() {
	for l_first_list in src_snapshots.list src_snapshots_dataset.list; do
		grep -v -e "^$S/child$1@snap2	" -e "^$S/child$1@snap3	" \
			"$STATE_DIR/$l_first_list" >"$STATE_DIR/$l_first_list.new" || :
		mv "$STATE_DIR/$l_first_list.new" "$STATE_DIR/$l_first_list" ||
			fail "Unable to trim source child$1 in $l_first_list."
	done
}

# Purpose: Make every `zfs list -H` of destination child N fail with an
# operational error, the answer zxfer must never read as "missing".
# Usage: sendrecv_fail_destination_child_probe N (after the missing rule)
sendrecv_fail_destination_child_probe() {
	printf "cannot open '%s': I/O error\n" "$D/child$1" >"$STATE_DIR/probe_error.list"
	{
		printf 'list -H %s\tprobe_error.list\t1\n' "$D/child$1"
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" || fail "Unable to prepend the probe failure."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the probe failure."
}

# Invariant: a destination child is seeded with its oldest pending snapshot
# before any incremental. A missing one gets a plain full receive, but only
# once a live `zfs list -H` confirms it is still missing (discovery may be
# minutes old); an existing empty one gets a full receive with -F that no
# later receive inherits. A single pending snapshot is seeded and never sent
# incrementally to itself. Only the missing child gets a creation-attempt
# notice on stderr, with or without -v; the later child still synchronizes.
test_missing_and_empty_destination_children_are_seeded_before_incrementals() {
	planning_setup_env
	for l_seed_verbose in "" -v; do
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/noop" "seeds$l_seed_verbose"
		sendrecv_keep_only_first_source_snapshot 1
		sendrecv_make_destination_child_missing 1
		sendrecv_make_destination_child_empty 2

		set -- -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
		[ -z "$l_seed_verbose" ] || set -- "$l_seed_verbose" "$@"
		planning_run_zxfer "$STATE_DIR" "$@"
		l_run_status=$?
		assertEquals "the seeding run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_run_status"
		assertEquals "each child is seeded with its oldest snapshot, and only child2 then gets an incremental" \
			"send $S/child1@snap1
send $S/child2@snap1
send -I $S/child2@snap1 $S/child2@snap3" "$(sendrecv_log_lines send)"
		assertEquals "the missing child gets a plain full receive, the empty one -F, which no later receive inherits" \
			"receive $D/child1
receive -F $D/child2
receive $D/child2" "$(sendrecv_log_lines receive)"
		planning_assert_no_mutations
		assertEquals "the missing child is probed live exactly once" \
			1 "$(grep -cFx "list -H $D/child1" "$ZFS_LOG")"
		assertTrue "the live probe must precede the full receive" \
			"[ '$(planning_log_line_number "list -H $D/child1")' -lt '$(planning_log_line_number "send $S/child1@snap1")' ]"
		assertFalse "no seeded child is listed again" "grep -q '^list -H -d 1 ' '$ZFS_LOG'"
		assertEquals "only the missing child gets a stderr notice, even without -v" \
			"zxfer: destination dataset [$D/child1] is missing; attempting creation before continuing recursive replication." \
			"$(cat "$CASE_DIR/zxfer.stderr")"
		if [ -z "$l_seed_verbose" ]; then
			assertFalse "the creation-attempt notice must not add quiet-run stdout" \
				"[ -s '$CASE_DIR/zxfer.stdout' ]"
			continue
		fi
		for l_seed_line in \
			"Destination dataset does not exist [$D/child1]. Sending first snapshot [$S/child1@snap1]" \
			"Destination dataset [$D/child2] exists but has no snapshots. Seeding with [$S/child2@snap1]" \
			"Temporarily enabling receive-side -F to seed existing empty destination dataset [$D/child2]."; do
			assertTrue "-v should explain the seed: $l_seed_line" \
				"grep -Fqx '$l_seed_line' '$CASE_DIR/zxfer.stdout'"
		done
	done
}

# Invariant: when the existence probe of a child to be seeded fails with
# anything but "does not exist", the run stops before any send or receive
# into it: whether the cache answered first and the live probe failed
# (nothing received before it), or the first probe itself failed (after the
# root's and child1's receives made the cached answer stale).
test_failed_existence_probe_of_a_child_to_seed_stops_before_its_receive() {
	planning_setup_env
	for l_probe_fixture in noop incremental; do
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/$l_probe_fixture" "probe_$l_probe_fixture"
		sendrecv_make_destination_child_missing 2
		sendrecv_fail_destination_child_probe 2

		planning_run_zxfer "$STATE_DIR" -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
		assertEquals "a failed probe must stop the run [$l_probe_fixture]" 1 $?
		planning_assert_failure_report replication \
			"Failed to determine whether destination dataset [$D/child2] exists: cannot open '$D/child2': I/O error"
		assertEquals "nothing may be sent to or received into child2 [$l_probe_fixture]" \
			"" "$(sendrecv_transfer_lines | grep 'child2' || :)"
		planning_assert_no_mutations
	done
}

# Invariant: an existing destination child whose snapshots share no guid
# with the source is never overwritten by a full receive: the run stops with
# a report before anything is sent to it.
test_child_with_only_unrelated_snapshots_is_never_fully_received() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" unrelated
	sendrecv_make_destination_child_empty 2
	printf '%s/child2@foreign\t9999900200000000007\n' "$D" |
		tee -a "$STATE_DIR/dst_snapshots.list" >>"$STATE_DIR/dst_d1_2.list" ||
		fail "Unable to add the unrelated snapshot."

	planning_run_zxfer "$STATE_DIR" -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "an unrelated snapshotted child must stop the run" 1 $?
	planning_assert_failure_report replication \
		"Destination dataset [$D/child2] has snapshots but none share a common guid with the source. Refusing to perform a full receive into an existing snapshotted dataset."
	assertEquals "nothing may be sent to or received into child2" \
		"" "$(sendrecv_transfer_lines | grep 'child2' || :)"
	planning_assert_no_mutations
}

# Purpose: Give the destination root in STATE_DIR a destination-only
# snapshot for -d to destroy, with the creation times of it and the anchor.
# Usage: sendrecv_add_destination_only_root_snapshot NAME CREATED ANCHOR
# ANCHOR_CREATED
sendrecv_add_destination_only_root_snapshot() {
	for l_extra_list in dst_snapshots.list dst_d1_0.list; do
		printf '%s@%s\t9999900009000000007\n' "$D" "$1" >>"$STATE_DIR/$l_extra_list" ||
			fail "Unable to add the destination-only snapshot to $l_extra_list."
	done
	printf '%s@%s\t%s\n%s@%s\t%s\n' "$D" "$3" "$4" "$D" "$1" "$2" \
		>"$STATE_DIR/dst_creation.list" || fail "Unable to write the creation times."
	printf 'get -H -o name,value -p creation %s@*\tdst_creation.list\t0\n' "$D" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the creation-time rule."
}

# Purpose: Answer the root's first depth-1 listing, the re-plan after its
# destroy, with the given rows (an outside writer changed it meanwhile).
# Usage: sendrecv_answer_replan_listing ROWS (empty for no snapshots)
sendrecv_answer_replan_listing() {
	printf '%s' "$1" >"$STATE_DIR/dst_replan.list"
	[ -z "$1" ] || printf '\n' >>"$STATE_DIR/dst_replan.list"
	awk -F'\t' -v key="list -H -d 1 -o name,guid -t snapshot $D" '
		BEGIN { OFS = "\t" }
		$1 == key { print key, "dst_replan.list", 0, "once" }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the re-plan listing."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the re-plan listing."
}

# Invariant: when the live re-plan after a -d destroy no longer finds the
# anchor (the last common snapshot), an older common snapshot never
# replaces it: the run stops with a report, rolls nothing back even under
# -F, and sends nothing.
test_replan_after_a_destroy_refuses_when_the_anchor_is_gone() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" anchor_gone
	sendrecv_add_destination_only_root_snapshot snap9 1700000009 snap3 1700000003
	sendrecv_answer_replan_listing "$(printf '%s@snap1\t1000000001000000007\n%s@snap2\t1000000002000000007' "$D" "$D")"

	planning_run_zxfer "$STATE_DIR" -d -F -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a lost anchor must stop the run" 1 $?
	planning_assert_failure_report replication \
		"Destination dataset [$D] has snapshots but none share a common guid with the source."
	assertEquals "the destroy is the only change: no rollback, send or receive" \
		"MUTATE destroy $D@snap9" "$(sendrecv_transfer_lines)"
}

# Invariant: when the re-plan after a -d destroy finds the destination
# emptied, it is re-seeded from its anchor with -F, never from the oldest
# source snapshot, and then sent incrementally from there as usual.
test_replan_after_a_destroy_reseeds_an_emptied_destination_from_its_anchor() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" emptied_in_sync
	sendrecv_add_destination_only_root_snapshot snap9 1700000009 snap3 1700000003
	sendrecv_answer_replan_listing ""

	planning_run_zxfer "$STATE_DIR" -d -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the in-sync re-seed should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the destroy is the only change" "MUTATE destroy $D@snap9" "$(sendrecv_log_lines MUTATE)"
	assertEquals "an emptied in-sync destination gets its anchor back and nothing else" \
		"send $S@snap3" "$(sendrecv_log_lines send)"
	assertEquals "the anchor is received with -F" "receive -F $D" "$(sendrecv_log_lines receive)"

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/incremental" emptied_behind
	sendrecv_add_destination_only_root_snapshot snap9 1700000009 snap2 1700000002
	sendrecv_answer_replan_listing ""

	planning_run_zxfer "$STATE_DIR" -d -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the re-seed with a pending snapshot should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the destroy is the only change" "MUTATE destroy $D@snap9" "$(sendrecv_log_lines MUTATE)"
	assertEquals "the root is re-seeded from its anchor, then sent incrementally; the children as usual" \
		"send $S@snap2
send -I $S@snap2 $S@snap3
send -I $S/child1@snap2 $S/child1@snap3
send -I $S/child2@snap2 $S/child2@snap3" "$(sendrecv_log_lines send)"
	assertEquals "only the re-seed is received with -F" \
		"receive -F $D
receive $D
receive $D/child1
receive $D/child2" "$(sendrecv_log_lines receive)"
}

# Invariant: under -d -F a destination is rolled back to its anchor only when
# a destroyed snapshot was newer than the anchor and something is left to
# send (the positive case is pinned in tests/test_contract_planning.sh): an
# older destroyed snapshot, or a newer one with nothing pending, leaves the
# destroy as the only change.
test_destroy_rolls_back_only_for_a_newer_snapshot_with_a_send_pending() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" older_destroyed
	sendrecv_add_destination_only_root_snapshot snap0 1700000001 snap2 1700000002

	planning_run_zxfer "$STATE_DIR" -d -F -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the older-destroy run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "an older destroyed snapshot needs no rollback" \
		"MUTATE destroy $D@snap0" "$(grep '^MUTATE ' "$ZFS_LOG")"
	assertEquals "every dataset is still sent" 3 "$(grep -c '^send -I ' "$ZFS_LOG")"

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" newer_nothing_pending
	sendrecv_add_destination_only_root_snapshot snap9 1700000009 snap3 1700000003

	planning_run_zxfer "$STATE_DIR" -d -F -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the nothing-pending run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "with nothing to send the destroy is the only change" \
		"MUTATE destroy $D@snap9" "$(sendrecv_transfer_lines)"
}

# ---------------------------------------------------------------------------
# The pass around the sends: -n previews, -s and -m snapshots, -Y passes.

# Purpose: Fail unless the named output file holds exactly one line equal to
# PREFIX followed by a zxfer_<pid>_<YYYYmmddHHMMSS> snapshot name and SUFFIX.
# Usage: sendrecv_assert_one_snapshot_line FILE PREFIX SUFFIX
sendrecv_assert_one_snapshot_line() {
	assertEquals "one line must be [$2<zxfer_pid_timestamp>$3]; output: $(cat "$1")" \
		1 "$(awk -v prefix="$2" -v suffix="$3" '
			index($0, prefix) == 1 {
				name = substr($0, length(prefix) + 1)
				if (length(suffix) && substr(name, length(name) - length(suffix) + 1) == suffix)
					name = substr(name, 1, length(name) - length(suffix))
				else if (length(suffix))
					next
				if (name ~ /^zxfer_[0-9]+_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$/)
					n++
			}
			END { print n + 0 }' "$1")"
}

# Invariant: a dry run touches no zfs command and previews only the
# explicitly requested dataset: -s prints its recursive snapshot, -m its
# unmount and snapshot, and -V says which live steps (-e, -U, a %%size%%
# estimate, planning and the sends) it skips.
test_dry_run_previews_snapshots_and_unmounts_and_names_every_skipped_step() {
	planning_setup_env

	planning_run_zxfer "$FIXTURE_DIR/incremental" -n -v -s -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-n -s should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertFalse "-n -s must not invoke zfs" "[ -s '$ZFS_LOG' ]"
	sendrecv_assert_one_snapshot_line "$CASE_DIR/zxfer.stdout" \
		"Dry run: '$MOCKBIN_DIR/zfs' 'snapshot' '-r' '$S@" "'"

	planning_run_zxfer "$FIXTURE_DIR/incremental" -n -V -m -e -U \
		-D "$MOCKBIN_DIR/progress_dialog %%size%%" -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-n -m should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertFalse "-n -m must not invoke zfs" "[ -s '$ZFS_LOG' ]"
	assertEquals "-n -m previews the unmount of the requested dataset only" \
		"Dry run: '$MOCKBIN_DIR/zfs' 'unmount' '$S'" \
		"$(grep "^Dry run: .*'unmount'" "$CASE_DIR/zxfer.stdout")"
	sendrecv_assert_one_snapshot_line "$CASE_DIR/zxfer.stdout" \
		"Dry run: '$MOCKBIN_DIR/zfs' 'snapshot' '-r' '$S@" "'"
	for l_skip_line in \
		"Dry run: recursive descendant discovery is skipped; previewing only the explicitly requested source dataset." \
		"Dry run: skipping live replication-state validation and command planning." \
		"Dry run: skipping live backup-metadata restore validation." \
		"Dry run: skipping live unsupported-property detection." \
		"Dry run: skipping live %%size%% progress estimate discovery." \
		"Dry run: send/receive and property-reconcile commands require live snapshot discovery and are not rendered."; do
		assertTrue "-V should say: $l_skip_line" \
			"grep -Fqx -- '$l_skip_line' '$CASE_DIR/zxfer.stderr'"
	done
}

# Invariant: -s without -R snapshots the source dataset alone (no -r), and
# the pass then plans from a new discovery: here the one that finds the new
# snapshot (@snap4 stands in for it), so the send reaches it.
test_snapshot_option_without_recursion_sends_from_the_rediscovery() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" snapshot_rediscovery
	{
		cat "$STATE_DIR/src_snapshots.list"
		printf '%s@snap4\t1000000004000000007\n' "$S"
	} >"$STATE_DIR/src_after_snapshot.list" || fail "Unable to stage the rediscovery."
	awk -F'\t' -v key="list -Hr -o name,guid -s creation -t snapshot $S" '
		BEGIN { OFS = "\t" }
		$1 == key { print key, "src_snapshots.list", 0, "once"; print key, "src_after_snapshot.list", 0; next }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the rediscovery rule."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the rediscovery rule."

	planning_run_zxfer "$STATE_DIR" -s -N "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-s -N should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	grep '^MUTATE ' "$ZFS_LOG" >"$CASE_DIR/mutations"
	sendrecv_assert_one_snapshot_line "$CASE_DIR/mutations" "MUTATE snapshot $S@" ""
	assertEquals "the snapshot is the only change" 1 "$(wc -l <"$CASE_DIR/mutations" | tr -d ' ')"
	assertEquals "the send plans from the discovery after the snapshot" \
		"send -I $S@snap2 $S@snap4" "$(sendrecv_log_lines send)"
	assertEquals "the source is received once" "receive $D" "$(sendrecv_log_lines receive)"
	assertEquals "the source is discovered before and after the snapshot" \
		2 "$(grep -cFx "list -Hr -o name,guid -s creation -t snapshot $S" "$ZFS_LOG")"
}

# Invariant: -m (with -s, which it replaces) checks every source dataset is
# mounted, unmounts each, takes exactly one recursive snapshot after the last
# unmount, rediscovers, and then replicates every dataset.
test_migrate_takes_one_snapshot_after_every_unmount_and_replicates_every_dataset() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" migrate
	planning_add_property_transfer_fixtures
	printf 'yes\n' >"$STATE_DIR/mounted_yes.list"
	printf '%s\t%s\t0\n' "get -Ho value mounted *" mounted_yes.list "unmount *" - \
		>>"$STATE_DIR/manifest" || fail "Unable to append the -m manifest rules."

	planning_run_zxfer "$STATE_DIR" -m -s -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-m -s should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	grep '^MUTATE ' "$ZFS_LOG" >"$CASE_DIR/mutations"
	sendrecv_assert_one_snapshot_line "$CASE_DIR/mutations" "MUTATE snapshot -r $S@" ""
	assertEquals "the -m snapshot is the only change" 1 "$(wc -l <"$CASE_DIR/mutations" | tr -d ' ')"
	l_snapshot_at=$(awk '/^MUTATE snapshot / { print NR; exit }' "$ZFS_LOG")
	for l_migrate_suffix in "" /child1 /child2; do
		l_unmount_at=$(planning_log_line_number "unmount $S$l_migrate_suffix")
		assertTrue "[$l_migrate_suffix] is unmounted before the snapshot" \
			"[ '${l_unmount_at:-99999}' -lt '${l_snapshot_at:-0}' ]"
		l_receive_at=$(planning_log_line_number "receive $D$l_migrate_suffix")
		assertTrue "[$l_migrate_suffix] is received after the snapshot" \
			"[ '${l_snapshot_at:-99999}' -lt '${l_receive_at:-0}' ]"
	done
	assertEquals "the source is rediscovered after the snapshot" \
		2 "$(grep -cFx "list -Hr -o name,guid -s creation -t snapshot $S" "$ZFS_LOG")"
}

# Invariant (2026-09-30): -s takes one snapshot per run. Only the first -Y
# pass takes it and rediscovers after it; every later pass sends from its own
# discovery and takes none. The canned destination never records a receive,
# so all 8 passes send, and a second snapshot fails here the way zfs refuses
# a snapshot name that already exists.
test_yield_passes_take_the_snapshot_option_once_per_run() {
	planning_setup_env
	mkdir -p "$CASE_DIR/snapshot_calls" || fail "Unable to create the fault counter."

	(
		MOCK_FAIL_TOOL=zfs
		MOCK_FAIL_CALL=2
		MOCK_FAIL_DIR="$CASE_DIR/snapshot_calls"
		MOCK_FAIL_MATCH="snapshot *"
		MOCK_FAIL_STDERR="cannot create snapshot: dataset already exists"
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
			MOCK_FAIL_STDERR
		planning_run_zxfer "$FIXTURE_DIR/incremental" -Y -s -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	l_run_status=$?
	assertEquals "-Y -s should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	grep '^MUTATE ' "$ZFS_LOG" >"$CASE_DIR/mutations"
	sendrecv_assert_one_snapshot_line "$CASE_DIR/mutations" "MUTATE snapshot -r $S@" ""
	assertEquals "the snapshot is the only change" 1 "$(wc -l <"$CASE_DIR/mutations" | tr -d ' ')"
	assertEquals "all 8 passes send the root" \
		8 "$(grep -cFx "send -I $S@snap2 $S@snap3" "$ZFS_LOG")"
	assertEquals "the first pass discovers before and after its snapshot, each later pass once" \
		9 "$(grep -cFx "list -Hr -o name,guid -s creation -t snapshot $S" "$ZFS_LOG")"
	l_snapshot_at=$(awk '/^MUTATE snapshot / { print NR; exit }' "$ZFS_LOG")
	l_send_at=$(awk '/^send / { print NR; exit }' "$ZFS_LOG")
	assertTrue "the snapshot must come before the first send (snapshot ${l_snapshot_at:-none}, send ${l_send_at:-none})" \
		"[ '${l_snapshot_at:-99999}' -lt '${l_send_at:-0}' ]"
}

# Invariant: each -Y pass reads the properties afresh, the -k rows of all
# passes collapse to one row per dataset that keeps the newest values, -V
# counts one send, one receive and one pipeline per transfer, and reaching
# the pass limit says how to finish in fewer passes. The canned destination
# never records a receive, so all 8 passes send every dataset; only the first
# pass reads compression=gzip on the source.
test_yield_passes_reread_properties_and_keep_the_newest_backup_rows() {
	planning_setup_backup_env yield_backup
	planning_clone_state "$FIXTURE_DIR/incremental" yield_backup_incremental
	planning_add_property_transfer_fixtures
	awk -F'\t' 'BEGIN { OFS = "\t" } $2 == "compression" { $3 = "gzip" } { print }' \
		"$STATE_DIR/src_props_tree.list" >"$STATE_DIR/src_props_tree_first.list" ||
		fail "Unable to write the first pass's source properties."
	for l_yield_view in -Hpo -Ho; do
		l_yield_key="get -r -t filesystem,volume $l_yield_view name,property,value,source all $S"
		awk -F'\t' -v key="$l_yield_key" '
			BEGIN { OFS = "\t" }
			$1 == key { print key, "src_props_tree_first.list", 0, "once" }
			{ print }
		' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
			fail "Unable to stage the first pass's property rule."
		mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
			fail "Unable to install the first pass's property rule."
	done

	planning_run_backup_zxfer -Y -V -k -P
	l_run_status=$?
	assertEquals "-Y -k -P should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "all 8 passes send the root, with -v under -V" \
		8 "$(grep -cFx "send -v -I $S@snap2 $S@snap3" "$ZFS_LOG")"
	assertEquals "every pass reads the source properties afresh" \
		8 "$(grep -cFx "get -r -t filesystem,volume -Hpo name,property,value,source all $S" "$ZFS_LOG")"
	assertEquals "only the first pass sets the drifted value" \
		3 "$(grep -c '^MUTATE set compression=gzip ' "$ZFS_LOG")"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" "$S" "$D"
	for l_yield_counter in zfs_send_calls=24 zfs_receive_calls=24 \
		send_receive_pipeline_commands=24 send_receive_background_pipeline_commands=0; do
		assertTrue "-V should report $l_yield_counter" \
			"grep -Fqx 'zxfer profile: $l_yield_counter' '$CASE_DIR/zxfer.stderr'"
	done
	assertTrue "the pass limit should print the tuning hint" \
		"grep -Fqx 'consider using compression, increasing bandwidth, increasing I/O or reducing snapshot frequency.' '$CASE_DIR/zxfer.stderr'"
}

# ---------------------------------------------------------------------------
# Remote transport details that change what a run replicates.

# Purpose: Write the socket-aware mock ssh of tests/helpers/blackbox.sh with
# OpenSSH's stdin handling: the remote command reads the local stdin, so a
# caller that lets ssh inherit a list it is still reading loses the rest.
# Usage: sendrecv_write_stdin_reading_ssh PATH
sendrecv_write_stdin_reading_ssh() {
	planning_write_socket_mock_ssh "$1" || fail "Unable to write the mock ssh."
	sed 's/^exec sh -c "\$\*"$/cat | sh -c "$*"/' "$1" >"$1.new" ||
		fail "Unable to make the mock ssh read stdin."
	if ! mv "$1.new" "$1" || ! chmod +x "$1"; then
		fail "Unable to install the mock ssh."
	fi
	grep -q '^cat | sh -c ' "$1" ||
		fail "The mock ssh no longer ends with the command it runs."
}

# Purpose: Build the post-seed environment on a noop STATE_DIR: property
# fixtures, both destination children missing (the -P pass creates them
# before their seed), and destination property reads that answer the tree
# as the receives left it (atime=on on both children) once the first read
# of the pass is spent.
# Usage: sendrecv_setup_post_seed_env NAME; a fresh environment and zfs log
# each time.
sendrecv_setup_post_seed_env() {
	rm -rf "$CASE_DIR/mockbin" "$CASE_DIR/fixtures" "$CASE_DIR/zfs.log"
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	planning_add_property_transfer_fixtures
	awk -F'\t' -v child="$D/child" 'BEGIN { OFS = "\t" }
		index($1, child) == 1 && $2 == "atime" { $3 = "on" }
		{ print }
	' "$STATE_DIR/dst_props_tree.list" >"$STATE_DIR/dst_props_tree_received.list" ||
		fail "Unable to write the received property tree."
	cut -f1,2 "$STATE_DIR/dst_props_tree_received.list" \
		>"$STATE_DIR/dst_props_tree_received.names"
	for l_post_seed_list in dst_props_tree.list dst_props_tree.names; do
		grep -v "^$D/child" "$STATE_DIR/$l_post_seed_list" \
			>"$STATE_DIR/$l_post_seed_list.new" || :
		mv "$STATE_DIR/$l_post_seed_list.new" "$STATE_DIR/$l_post_seed_list" ||
			fail "Unable to drop the children from $l_post_seed_list."
	done
	sendrecv_make_destination_child_missing 1
	sendrecv_make_destination_child_missing 2
	# One rule per recursive view: "VIEW|SUFFIX" of the tree fixtures.
	for l_post_seed_rule in "-Hpo name,property,value,source|list" \
		"-Ho name,property,value,source|list" "-Ho name,property|names"; do
		awk -F'\t' -v suffix="${l_post_seed_rule#*|}" \
			-v key="get -r -t filesystem,volume ${l_post_seed_rule%|*} all $ZXFER_MOCKBIN_DEST_ROOT" '
			BEGIN { OFS = "\t" }
			$1 == key {
				print key, "dst_props_tree." suffix, 0, "once"
				print key, "dst_props_tree_received." suffix, 0
				next
			}
			{ print }
		' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
			fail "Unable to stage the destination tree rules."
		mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
			fail "Unable to install the destination tree rules."
	done
}

# Purpose: Assert the post-seed outcome: each missing child created, seeded
# with -F and sent incrementally, then reconciled once with the property
# the receives changed, after the last receive of the run has ended.
# Usage: sendrecv_assert_post_seed_reconcile LABEL
sendrecv_assert_post_seed_reconcile() {
	for l_post_seed_child in child1 child2; do
		l_create_at=$(awk -v prefix="MUTATE create " -v name="$D/$l_post_seed_child" \
			'index($0, prefix) == 1 && $NF == name { print NR; exit }' "$ZFS_LOG")
		l_seed_at=$(planning_log_line_number "receive -F $D/$l_post_seed_child")
		assertTrue "$1: $l_post_seed_child is created before its seed receive; zfs log: $(cat "$ZFS_LOG")" \
			"[ '${l_create_at:-99999}' -lt '${l_seed_at:-0}' ]"
		assertEquals "$1: $l_post_seed_child is reconciled once after its seed" \
			1 "$(grep -cFx "MUTATE set atime=off $D/$l_post_seed_child" "$ZFS_LOG")"
	done
	l_last_end=$(awk '/^END receive / { n = NR } END { print n + 0 }' "$ZFS_LOG")
	l_first_set=$(awk '/^MUTATE set / { print NR; exit }' "$ZFS_LOG")
	assertTrue "$1: the reconcile waits for the last receive to end" \
		"[ '$l_last_end' -gt 0 ] && [ '$l_last_end' -lt '${l_first_set:-0}' ]"
	assertEquals "$1: only the two creates and the two reconciles change properties" \
		4 "$(grep -cE '^MUTATE (create|set|inherit) ' "$ZFS_LOG")"
}

# Invariant: a seed receive creates a dataset without the source's local
# properties, so every seeded dataset gets one more property pass after the
# run's receives have all ended (-j 2 included, with a slow last receive),
# reading the destination afresh; -v output stays on stdout. Over -T, the
# pass's ssh reads never consume the list of datasets still to reconcile.
test_post_seed_property_pass_reconciles_every_seeded_dataset_after_the_receives() {
	sendrecv_setup_post_seed_env post_seed
	planning_run_zxfer "$STATE_DIR" -v -P -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-P over missing children should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	sendrecv_assert_post_seed_reconcile "-j 1"
	assertTrue "-v output of the property pass stays on stdout" \
		"grep -Fq 'Creating destination filesystem \"$D/child2\" with specified properties.' '$CASE_DIR/zxfer.stdout'"

	sendrecv_setup_post_seed_env post_seed_jobs
	planning_add_parallel_source_discovery_fixtures
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."
	planning_delay_canned_zfs_receive "$D/child2" 1
	planning_run_zxfer "$STATE_DIR" -j 2 -P -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-j 2 -P over missing children should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	sendrecv_assert_post_seed_reconcile "-j 2"

	sendrecv_setup_post_seed_env post_seed_target
	sendrecv_write_stdin_reading_ssh "$MOCKBIN_DIR/ssh"
	sendrecv_run_zxfer "" "$STATE_DIR" -T localhost -P -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "-T -P over missing children should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	sendrecv_assert_post_seed_reconcile "-T"
}

# Invariant: a pull runs ssh inside each dataset's work; over an ssh that
# reads its stdin, as OpenSSH does, the run still replicates every dataset
# instead of losing the rest of its queue to the first one.
test_remote_pull_over_an_ssh_that_reads_stdin_replicates_every_dataset() {
	planning_setup_env
	sendrecv_write_stdin_reading_ssh "$MOCKBIN_DIR/ssh"
	sendrecv_run_zxfer "" "$FIXTURE_DIR/incremental" -O localhost -R "$S" \
		"$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the -O pull should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "every dataset is received once" \
		"receive $D
receive $D/child1
receive $D/child2" "$(grep '^receive ' "$ZFS_LOG")"
}

# ---------------------------------------------------------------------------
# -j scheduling, job failures, and work that must wait for a job.

# Purpose: Build a -j environment on a nested tree: the incremental state of
# a four-child fixture with child3 renamed child1/sub and child4 child2/sub on
# both sides, the root already in sync, and the -j source discovery answers.
# Usage: sendrecv_setup_nested_jobs_env NAME
sendrecv_setup_nested_jobs_env() {
	MOCKBIN_DIR="$CASE_DIR/mockbin"
	FIXTURE_DIR="$CASE_DIR/fixtures"
	ZFS_LOG="$CASE_DIR/zfs.log"
	mkdir -p "$MOCKBIN_DIR" || fail "Unable to create the mock bin directory."
	zxfer_mockbin_write_canned_zfs "$MOCKBIN_DIR/zfs" || fail "Unable to write the canned zfs."
	zxfer_mockbin_build_fixture_tree "$FIXTURE_DIR" 4 3 ||
		fail "Unable to build the four-child fixture tree."
	planning_clone_state "$FIXTURE_DIR/incremental" "$1"
	for l_nested_file in "$STATE_DIR"/*; do
		if ! sed -e 's#/child3#/child1/sub#g' -e 's#/child4#/child2/sub#g' \
			"$l_nested_file" >"$l_nested_file.new" ||
			! mv "$l_nested_file.new" "$l_nested_file"; then
			fail "Unable to nest the fixture in $l_nested_file."
		fi
	done
	printf '%s@snap3\t1000000003000000007\n' "$D" |
		tee -a "$STATE_DIR/dst_snapshots.list" >>"$STATE_DIR/dst_d1_0.list" ||
		fail "Unable to put the destination root in sync."
	printf '%s\n' "$S" "$S/child1" "$S/child2" "$S/child1/sub" "$S/child2/sub" \
		>"$STATE_DIR/src_datasets.list"
	printf 'list -Hr -t filesystem,volume -o name %s\tsrc_datasets.list\t0\n' "$S" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the source inventory rule."
	l_nested_index=0
	for l_nested_suffix in "" /child1 /child2 /child1/sub /child2/sub; do
		grep "^$S$l_nested_suffix@" "$STATE_DIR/src_snapshots.list" \
			>"$STATE_DIR/src_d1_$l_nested_index.list" ||
			fail "Unable to derive the depth-1 source listing of [$l_nested_suffix]."
		printf 'list -H -o name,guid -s creation -d 1 -t snapshot %s\tsrc_d1_%s.list\t0\n' \
			"$S$l_nested_suffix" "$l_nested_index" >>"$STATE_DIR/manifest" ||
			fail "Unable to append the depth-1 source rule of [$l_nested_suffix]."
		l_nested_index=$((l_nested_index + 1))
	done
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."
	JOB_TMP_DIR="$CASE_DIR/jobtmp"
	if ! mkdir -p "$JOB_TMP_DIR" || ! chmod 700 "$JOB_TMP_DIR"; then
		fail "Unable to create the per-run temp root."
	fi
}

# Invariant: with -j 2 a dataset whose ancestor's receive is still running
# waits for it, while a dataset whose own ancestor is done starts at once:
# child1's slow receive holds child1/sub back but not child2/sub, and no more
# than two transfers ever run at a time (a transfer runs from its send line
# to its receive's END line). Every dataset sends its own increment once.
test_parallel_jobs_start_a_descendant_as_soon_as_its_own_ancestor_is_done() {
	sendrecv_setup_nested_jobs_env nested_jobs
	planning_delay_canned_zfs_receive "$D/child1" 4

	sendrecv_run_zxfer "$JOB_TMP_DIR" "$STATE_DIR" -j 2 -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the nested -j 2 run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	for l_nested_suffix in /child1 /child2 /child1/sub /child2/sub; do
		assertEquals "[$l_nested_suffix] sends its own increment once" 1 \
			"$(grep -cFx "send -I $S$l_nested_suffix@snap2 $S$l_nested_suffix@snap3" "$ZFS_LOG")"
	done
	assertEquals "the in-sync root is not sent" 4 "$(grep -c '^send ' "$ZFS_LOG")"
	l_child1_end=$(planning_log_line_number "END receive $D/child1")
	l_child2_end=$(planning_log_line_number "END receive $D/child2")
	l_sub1_send=$(planning_log_line_number "send -I $S/child1/sub@snap2 $S/child1/sub@snap3")
	l_sub2_send=$(planning_log_line_number "send -I $S/child2/sub@snap2 $S/child2/sub@snap3")
	assertTrue "child1/sub waits for its ancestor's receive; zfs log: $(cat "$ZFS_LOG")" \
		"[ '${l_child1_end:-99999}' -lt '${l_sub1_send:-0}' ]"
	assertTrue "child2/sub waits for its own ancestor only; zfs log: $(cat "$ZFS_LOG")" \
		"[ '${l_child2_end:-99999}' -lt '${l_sub2_send:-0}' ] && [ '${l_sub2_send:-99999}' -lt '${l_child1_end:-0}' ]"
	assertEquals "no more than two transfers run at a time; zfs log: $(cat "$ZFS_LOG")" 2 \
		"$(awk '/^send / { n++; if (n > max) max = n } /^END receive / { n-- } END { print max + 0 }' "$ZFS_LOG")"
	planning_assert_no_parallel_job_leftovers
}

# Invariant: when a -j job fails on a -T target, the run ends with that job's
# exit status and a report naming the snapshot, destination and target, and
# does not wait for the sibling job still receiving: its whole pipeline is
# stopped, the run finishes well within the sibling's 30 seconds, and nothing
# is left behind.
test_parallel_job_failure_over_the_target_keeps_its_status_and_stops_the_sibling() {
	planning_setup_parallel_jobs_env job_failure_target
	printf 'receive %s/child1\t-\t3\n' "$D" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the failing receive rule."
	planning_delay_canned_zfs_receive "$D/child2" 30
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" || fail "Unable to write the mock ssh."

	l_failure_started=$(date '+%s')
	sendrecv_run_zxfer "$JOB_TMP_DIR" "$STATE_DIR" -j 2 -T localhost -R "$S" \
		"$ZXFER_MOCKBIN_DEST_ROOT"
	l_failure_status=$?
	l_failure_elapsed=$(($(date '+%s') - l_failure_started))
	assertEquals "the run ends with the failed job's status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		3 "$l_failure_status"
	planning_assert_failure_report send/receive \
		"message: zfs send/receive job failed for [$S/child1@snap3 -> $D/child1] on target [localhost] (PID "
	assertTrue "the report carries the job's exit status" \
		"grep -Fq ', exit 3).' '$CASE_DIR/zxfer.stderr'"
	assertTrue "the run must not wait for the sibling (took ${l_failure_elapsed}s)" \
		"[ '$l_failure_elapsed' -lt 20 ]"
	assertFalse "the sibling's receive never completes" \
		"grep -q '^END receive $D/child2\$' '$ZFS_LOG'"
	planning_assert_no_mutations
	planning_assert_no_parallel_job_leftovers
}

# Invariant: with -j a converged destination is checked again only after its
# own receive has ended (the receive is slow here), and -V counts that
# receive as one background pipeline.
test_parallel_jobs_check_a_converged_destination_only_after_its_receive() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" converge_jobs
	planning_make_destination_diverged_until_receive
	planning_add_parallel_source_discovery_fixtures
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."
	JOB_TMP_DIR="$CASE_DIR/jobtmp"
	if ! mkdir -p "$JOB_TMP_DIR" || ! chmod 700 "$JOB_TMP_DIR"; then
		fail "Unable to create the per-run temp root."
	fi
	planning_delay_canned_zfs_receive "$D" 1

	sendrecv_run_zxfer "$JOB_TMP_DIR" "$STATE_DIR" -V -j 2 -d -F -R "$S" \
		"$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the -j 2 convergence should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	l_converge_key="list -H -d 1 -o name,guid -t snapshot $D"
	assertEquals "the root is listed live before its send and once after it" \
		2 "$(grep -cFx "$l_converge_key" "$ZFS_LOG")"
	l_converge_end=$(planning_log_line_number "END receive $D")
	l_converge_check=$(awk -v key="$l_converge_key" '$0 == key { n = NR } END { print n + 0 }' "$ZFS_LOG")
	assertTrue "the post-receive check follows the receive's end; zfs log: $(cat "$ZFS_LOG")" \
		"[ '${l_converge_end:-99999}' -lt '$l_converge_check' ]"
	assertTrue "-V counts the background pipeline" \
		"grep -Fqx 'zxfer profile: send_receive_background_pipeline_commands=1' '$CASE_DIR/zxfer.stderr'"
	planning_assert_no_parallel_job_leftovers
}

# ---------------------------------------------------------------------------
# The pipeline around each stream: -D progress stages and -z compression.

# Purpose: Put a recorder in front of whatever zfs MOCKBIN_DIR holds: each
# receive appends its stream to $CASE_DIR/received/<dataset>, with "/"
# spelled "_", on its way to the wrapped zfs, whose status it keeps.
# Usage: sendrecv_record_received_streams
sendrecv_record_received_streams() {
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.recorded" ||
		fail "Unable to stage the zfs behind the stream recorder."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
case "\${1:-}" in
receive | recv)
	for recorded_arg in "\$@"; do
		recorded_dataset=\$recorded_arg
	done
	mkdir -p "$CASE_DIR/received" || exit 1
	tee -a "$CASE_DIR/received/\$(printf '%s' "\$recorded_dataset" | tr / _)" |
		"$MOCKBIN_DIR/zfs.recorded" "\$@"
	exit
	;;
esac
exec "$MOCKBIN_DIR/zfs.recorded" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Fail unless each dataset of the tree received exactly its own
# incremental stream once, with nothing added or lost.
# Usage: sendrecv_assert_streams_received_intact LABEL
sendrecv_assert_streams_received_intact() {
	for l_intact_suffix in "" /child1 /child2; do
		l_intact_file="$CASE_DIR/received/$(printf '%s' "$D$l_intact_suffix" | tr / _)"
		assertEquals "$1: $D$l_intact_suffix receives its stream exactly once" \
			"ZXFERMOCKSTREAM send -I $S$l_intact_suffix@snap2 $S$l_intact_suffix@snap3" \
			"$(cat "$l_intact_file" 2>/dev/null)"
	done
}

# Purpose: Write a dialog to MOCKBIN_DIR/progress_dialog that appends its
# argv to $CASE_DIR/dialog.argv and drains its stdin.
# Usage: sendrecv_write_argv_dialog
sendrecv_write_argv_dialog() {
	cat >"$MOCKBIN_DIR/progress_dialog" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$CASE_DIR/dialog.argv"
cat >/dev/null
EOF
	chmod +x "$MOCKBIN_DIR/progress_dialog"
}

# Invariant: -D replaces every %%size%% and %%title%% of the template in
# order and keeps the text around them; only a template with %%size%% makes
# zxfer estimate the stream size.
test_progress_template_expands_every_macro_in_order_and_sizes_only_on_request() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" progress_template
	printf 'size\t4096\n' >"$STATE_DIR/send_estimate.list"
	printf 'send -nPv *\tsend_estimate.list\t0\n' >>"$STATE_DIR/manifest" ||
		fail "Unable to append the send estimate rule."
	sendrecv_write_argv_dialog

	planning_run_zxfer "$STATE_DIR" \
		-D "$MOCKBIN_DIR/progress_dialog %%title%%:%%size%%/%%title%% tail" -N "$S" \
		"$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the sized template run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "every macro is expanded in template order" \
		"$S@snap3:4096/$S@snap3 tail" "$(cat "$CASE_DIR/dialog.argv")"
	planning_assert_log_has_line "send -nPv -I $S@snap2 $S@snap3"

	: >"$ZFS_LOG"
	: >"$CASE_DIR/dialog.argv"
	planning_run_zxfer "$STATE_DIR" -D "$MOCKBIN_DIR/progress_dialog %%title%% tail" \
		-N "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the unsized template run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "the title and the trailing text reach the dialog" \
		"$S@snap3 tail" "$(cat "$CASE_DIR/dialog.argv")"
	assertFalse "a template without %%size%% probes no size" \
		"grep -Eq '^send -nPv |written@|referenced' '$ZFS_LOG'"
}

# Invariant: the dialog's stdout never reaches the receive, and a dialog that
# fails after reading its copy of the stream does not fail the transfer: a
# dialog that copies the stream to stdout and exits 7 leaves every receive
# with exactly one copy, in zxfer's own shell (-j 1) and in job shells (-j 2).
test_progress_dialog_output_never_reaches_the_receive_and_its_status_is_ignored() {
	planning_setup_parallel_jobs_env progress_stdout
	sendrecv_record_received_streams

	for l_stdout_jobs in 1 2; do
		rm -rf "$CASE_DIR/received"
		: >"$ZFS_LOG"
		: >"$CASE_DIR/dialog.copy"
		sendrecv_run_zxfer "$JOB_TMP_DIR" "$STATE_DIR" -j "$l_stdout_jobs" \
			-D "tee -a '$CASE_DIR/dialog.copy'; exit 7" -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
		l_run_status=$?
		assertEquals "-j $l_stdout_jobs should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
		sendrecv_assert_streams_received_intact "-j $l_stdout_jobs"
		assertEquals "-j $l_stdout_jobs: the dialog read every stream once" \
			3 "$(grep -c '^ZXFERMOCKSTREAM send -I ' "$CASE_DIR/dialog.copy")"
	done
	planning_assert_no_parallel_job_leftovers
}

# Invariant: under -j the size comes first from the cheap written@ (for an
# incremental) or referenced (for a full send) property; when that probe
# fails, the exact `zfs send -nPv` estimate answers instead.
test_progress_size_falls_back_to_the_exact_estimate_for_full_and_incremental_sends() {
	planning_setup_parallel_jobs_env progress_fallback
	sendrecv_make_destination_child_missing 2
	printf 'size\t4096\n' >"$STATE_DIR/incremental_estimate.list"
	printf 'size\t8192\n' >"$STATE_DIR/full_estimate.list"
	printf '%s\t%s\t%s\n' \
		"get -Hpo value written@* *" - 1 \
		"list -Hp -o referenced *" - 1 \
		"send -nPv -I *" incremental_estimate.list 0 \
		"send -nPv *" full_estimate.list 0 >>"$STATE_DIR/manifest" ||
		fail "Unable to append the size probe rules."
	sendrecv_write_argv_dialog

	sendrecv_run_zxfer "$JOB_TMP_DIR" "$STATE_DIR" -j 2 \
		-D "$MOCKBIN_DIR/progress_dialog %%title%% %%size%%" -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "the fallback run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "every dialog gets the exact estimate of its send" \
		"$S/child1@snap3 4096
$S/child2@snap1 8192
$S/child2@snap3 4096
$S@snap3 4096" "$(LC_ALL=C sort "$CASE_DIR/dialog.argv")"
	for l_fallback_line in "get -Hpo value written@snap2 $S" "send -nPv -I $S@snap2 $S@snap3" \
		"list -Hp -o referenced $S/child2@snap1" "send -nPv $S/child2@snap1"; do
		planning_assert_log_has_line "$l_fallback_line"
	done
	planning_assert_no_parallel_job_leftovers
}

# Purpose: Print the stream sends and receives of the zfs log, which leaves
# out the `send -nPv` size probes.
# Usage: sendrecv_stream_lines
sendrecv_stream_lines() {
	grep -E '^(send|receive) ' "$ZFS_LOG" | grep -v '^send -nPv ' || :
}

# Invariant: a -D stage that cannot be prepared stops the run with a report
# before the send it was for: a failed size estimate keeps zfs's status, and
# an unparsable estimate or a FIFO mkfifo cannot create fail with status 1.
test_progress_stage_failures_stop_the_run_before_its_send() {
	planning_setup_env
	sendrecv_write_argv_dialog

	planning_clone_state "$FIXTURE_DIR/incremental" progress_probe_failure
	printf 'probe failed detail\n' >"$STATE_DIR/probe_failure.list"
	printf 'send -nPv *\tprobe_failure.list\t41\n' >>"$STATE_DIR/manifest" ||
		fail "Unable to append the failing estimate rule."
	planning_run_zxfer "$STATE_DIR" -D "$MOCKBIN_DIR/progress_dialog %%size%%" -N "$S" \
		"$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "a failed estimate keeps zfs's status" 41 $?
	planning_assert_failure_report send/receive \
		"Error calculating incremental estimate: probe failed detail"
	assertEquals "nothing is sent after a failed estimate" "" "$(sendrecv_stream_lines)"

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/incremental" progress_parse_failure
	printf 'size\tnot-a-number\n' >"$STATE_DIR/probe_garbage.list"
	printf 'send -nPv *\tprobe_garbage.list\t0\n' >>"$STATE_DIR/manifest" ||
		fail "Unable to append the unparsable estimate rule."
	planning_run_zxfer "$STATE_DIR" -D "$MOCKBIN_DIR/progress_dialog %%size%%" -N "$S" \
		"$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "an unparsable estimate stops the run" 1 $?
	planning_assert_failure_report send/receive "Error parsing incremental estimate: size"
	assertEquals "nothing is sent after an unparsable estimate" "" "$(sendrecv_stream_lines)"

	: >"$ZFS_LOG"
	zxfer_mockbin_write_counting_wrapper "$MOCKBIN_DIR/mkfifo" \
		"$(zxfer_mockbin_resolve_host_tool mkfifo)" || fail "Unable to wrap mkfifo."
	mkdir -p "$CASE_DIR/mkfifo_calls" || fail "Unable to create the fault counter."
	(
		MOCK_FAIL_TOOL='mkfifo'
		MOCK_FAIL_CALL=1
		MOCK_FAIL_DIR="$CASE_DIR/mkfifo_calls"
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR
		planning_run_zxfer "$FIXTURE_DIR/incremental" -D "$MOCKBIN_DIR/progress_dialog %%title%%" \
			-N "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "a FIFO that cannot be created stops the run" 1 $?
	planning_assert_failure_report send/receive \
		"Failed to prepare the progress dialog FIFO for $S@snap3."
	assertEquals "nothing is sent without its FIFO" "" "$(sendrecv_stream_lines)"
	assertFalse "no dialog ever started" "[ -s '$CASE_DIR/dialog.argv' ]"
}

# Purpose: Write a framing zstd to MOCKBIN_DIR: compressing prepends one
# ZXFERMOCKZSTD line, and decompressing fails unless it can strip one. A
# stream that skipped the compressor then fails, and one that skipped the
# decompressor reaches its receive framed.
# Usage: sendrecv_write_framing_zstd
sendrecv_write_framing_zstd() {
	cat >"$MOCKBIN_DIR/zstd" <<'EOF'
#!/bin/sh
case "${1:-}" in
-d)
	IFS= read -r framing_header || exit 1
	if [ "$framing_header" != ZXFERMOCKZSTD ]; then
		echo "zstd: stdin is not a mock frame" >&2
		exit 1
	fi
	exec cat
	;;
esac
printf '%s\n' ZXFERMOCKZSTD
exec cat
EOF
	chmod +x "$MOCKBIN_DIR/zstd"
}

# Invariant: -z compresses each stream on the sending side of its ssh hop and
# decompresses it on the receiving side, pulling (-O), pushing (-T) and when
# -O and -T name the same host (each side still picks the codec of its own
# role), so every receive gets the stream as sent. With -j 2 and -D as well,
# the whole composed pipeline runs in job shells and the dialog sees the
# decompressed stream.
test_compression_crosses_each_ssh_hop_and_leaves_every_stream_intact() {
	planning_setup_parallel_jobs_env compression
	sendrecv_record_received_streams
	sendrecv_write_framing_zstd
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" || fail "Unable to write the mock ssh."
	cat >"$MOCKBIN_DIR/progress_dialog" <<EOF
#!/bin/sh
cat >>"$CASE_DIR/dialog.copy"
EOF
	chmod +x "$MOCKBIN_DIR/progress_dialog"
	l_codec_send="'$MOCKBIN_DIR/zfs' 'send' '-I' '$S@snap2' '$S@snap3' | '$MOCKBIN_DIR/zstd' '-3'"
	l_codec_receive="'$MOCKBIN_DIR/zstd' '-d' | '$MOCKBIN_DIR/zfs' 'receive' '$D'"

	for l_codec_hosts in "-O localhost" "-T localhost" "-O localhost -T localhost"; do
		rm -rf "$CASE_DIR/received"
		: >"$ZFS_LOG"
		: >"$CASE_DIR/ssh.log"
		: >"$CASE_DIR/dialog.copy"
		set -- -z
		case $l_codec_hosts in
		*-T*) set -- "$@" -T localhost ;;
		esac
		case $l_codec_hosts in
		-O*) set -- "$@" -O localhost ;;
		esac
		[ "$#" -lt 5 ] || set -- "$@" -j 2 -D "$MOCKBIN_DIR/progress_dialog"
		(
			MOCK_ZFS_STRICT_RECEIVE=1
			export MOCK_ZFS_STRICT_RECEIVE
			sendrecv_run_zxfer "$JOB_TMP_DIR" "$STATE_DIR" "$@" -R "$S" "$ZXFER_MOCKBIN_DEST_ROOT"
		)
		l_run_status=$?
		assertEquals "[$l_codec_hosts] should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
		sendrecv_assert_streams_received_intact "$l_codec_hosts"
		case $l_codec_hosts in
		-O*)
			assertTrue "[$l_codec_hosts] the origin compresses before its hop: $(cat "$CASE_DIR/ssh.log")" \
				"grep -Fq -- \"-o BatchMode=yes -o StrictHostKeyChecking=yes -S \" '$CASE_DIR/ssh.log' &&
				grep -Fq -- \"localhost $l_codec_send\" '$CASE_DIR/ssh.log'"
			;;
		esac
		case $l_codec_hosts in
		*-T*)
			assertTrue "[$l_codec_hosts] the target decompresses after its hop: $(cat "$CASE_DIR/ssh.log")" \
				"grep -Fq -- \"localhost $l_codec_receive\" '$CASE_DIR/ssh.log'"
			;;
		esac
	done
	assertEquals "the dialog read every decompressed stream" \
		3 "$(grep -c '^ZXFERMOCKSTREAM send -I ' "$CASE_DIR/dialog.copy")"
	planning_assert_no_parallel_job_leftovers
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
