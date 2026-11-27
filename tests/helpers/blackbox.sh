#!/bin/sh
#
# Shared helpers for black-box suites that drive the real ./zxfer launcher
# against the canned zfs from tests/mock_toolchain_helper.sh and assert on the
# MOCK_ZFS_LOG argv, the exit status, and stderr.
#
# A suite entry sources tests/test_helper.sh, then this file, defines its
# test_* functions, and finally sources shunit2. This file supplies the shunit2
# lifecycle hooks: every case gets a private CASE_DIR, removed afterwards.
# Helpers publish their paths in MOCKBIN_DIR, FIXTURE_DIR, ZFS_LOG, STATE_DIR,
# JOB_TMP_DIR, SSH_LOG, ARGV_LOG, PARALLEL_ARGV_LOG, and the backup-mode
# variables named where they are set.
#
# shellcheck disable=SC1090,SC2034,SC2154

# shellcheck source=tests/mock_toolchain_helper.sh
. "$ZXFER_ROOT/tests/mock_toolchain_helper.sh"

# The fixture roots every case starts from (see planning_use_fixture_roots).
planning_default_source_root=$ZXFER_MOCKBIN_SOURCE_ROOT
planning_default_dest_root=$ZXFER_MOCKBIN_DEST_ROOT
planning_default_dest_mapped_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_blackbox"
}

planning_reset_mock_env() {
	unset MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS \
		MOCK_ZFS_STRICT_RECEIVE MOCK_ZFS_ARGV_LOG MOCK_ZFS_ARGV_VERBS \
		MOCK_ZFS_ARGV_FORMAT MOCK_ZFS_DELAY_RECEIVE_GLOB \
		MOCK_ZFS_DELAY_RECEIVE_SECONDS MOCK_ZFS_RECEIVE_BYTES_LOG \
		MOCK_ZFS_RECEIVE_STREAM_DIR MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR \
		MOCK_SPAWN_LOG MOCK_SSH_LOG MOCK_SSH_HANDSHAKE_S MOCK_SSH_MUX_S \
		MOCK_SSH_REJECT_CONTROL_MASTER MOCK_SSH_READ_STDIN \
		MOCK_SSH_CAPABILITY_RESPONSE_FILE MOCK_PARALLEL_ARGV_LOG \
		MOCK_SVCADM_FAIL MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_MATCH \
		MOCK_FAIL_DIR MOCK_FAIL_STDERR MOCK_FAIL_STATUS ZXFER_BACKUP_DIR
}

setUp() {
	planning_reset_mock_env
	planning_use_fixture_roots "$planning_default_source_root" \
		"$planning_default_dest_root" "$planning_default_dest_mapped_root"
	CASE_DIR=$(mktemp -d "$TEST_TMPDIR/case.XXXXXX") ||
		fail "Unable to create per-case temp directory."
}

tearDown() {
	if [ -n "${CASE_DIR:-}" ]; then
		rm -rf "$CASE_DIR"
	fi
	CASE_DIR=""
}

# ---------------------------------------------------------------------------
# Environment, runs, and argv-log assertions.

# Purpose: Point the fixture builders and the planning_* helpers at other
# dataset roots for the rest of the case; setUp restores the defaults.
# Usage: planning_use_fixture_roots <source-root> <destination-root>
# <mapped-destination-root>
planning_use_fixture_roots() {
	ZXFER_MOCKBIN_SOURCE_ROOT=$1
	ZXFER_MOCKBIN_DEST_ROOT=$2
	ZXFER_MOCKBIN_DEST_MAPPED_ROOT=$3
}

# Purpose: Build the standard environment: canned zfs in MOCKBIN_DIR plus the
# 2-child x 3-snapshot fixture tree in FIXTURE_DIR ("noop" and "incremental").
# Usage: planning_setup_env
planning_setup_env() {
	planning_reset_mock_env
	MOCKBIN_DIR="$CASE_DIR/mockbin"
	FIXTURE_DIR="$CASE_DIR/fixtures"
	ZFS_LOG="$CASE_DIR/zfs.log"

	mkdir -p "$MOCKBIN_DIR" || fail "Unable to create mock bin directory."
	zxfer_mockbin_write_canned_zfs "$MOCKBIN_DIR/zfs" ||
		fail "Unable to write canned zfs."
	zxfer_mockbin_build_fixture_tree "$FIXTURE_DIR" 2 3 ||
		fail "Unable to build fixture tree."
}

# Purpose: Run ./zxfer against one fixture state dir, capturing stdout and
# stderr in $CASE_DIR/zxfer.stdout and $CASE_DIR/zxfer.stderr.
# Usage: planning_run_zxfer <state-dir> [zxfer-arg...]; returns zxfer's status.
planning_run_zxfer() {
	l_state_dir=$1
	shift

	zxfer_mockbin_run_zxfer "$MOCKBIN_DIR" "$l_state_dir" "$ZFS_LOG" "$@" \
		>"$CASE_DIR/zxfer.stdout" 2>"$CASE_DIR/zxfer.stderr"
}

# Purpose: Run recursive replication between the current fixture roots.
# Usage: planning_run_recursive <state-dir> [zxfer-option...]
planning_run_recursive() {
	planning_run_zxfer "$@" -R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# Purpose: Copy a fixture state dir into a case-local STATE_DIR that a test may
# rewrite without touching the generated tree.
# Usage: planning_clone_state <source-state-dir> <name>
planning_clone_state() {
	l_clone_source=$1
	l_clone_name=$2

	STATE_DIR="$CASE_DIR/state_$l_clone_name"
	mkdir -p "$STATE_DIR" || fail "Unable to create scratch state directory."
	cp "$l_clone_source"/* "$STATE_DIR/" ||
		fail "Unable to clone fixture state directory."
}

# Purpose: Prepend one or more manifest rules in STATE_DIR so they match ahead
# of existing rules.
# Usage: blackbox_prepend_manifest_rule <rule>...
blackbox_prepend_manifest_rule() {
	rm -rf "$STATE_DIR/.exact"
	if ! {
		printf '%s\n' "$@"
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" ||
		! mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest"; then
		fail "Unable to prepend manifest rules."
	fi
}

# Purpose: Run a command with the mock fault injector armed for one call in an
# isolated counter directory.
# Usage: blackbox_run_with_fault <tool> <call> <match> <status> <stderr|->
# <command> [arg...]; "-" leaves MOCK_FAIL_STDERR unset so the mock uses its
# default operational error, while "" sets an empty stderr. Publishes and
# returns BLACKBOX_FAULT_STATUS.
blackbox_run_with_fault() {
	l_fault_tool=$1
	l_fault_call=$2
	l_fault_match=$3
	l_fault_status=$4
	l_fault_stderr=$5
	shift 5

	rm -rf "$CASE_DIR/fail_calls"
	mkdir "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	BLACKBOX_FAULT_STATUS=0
	(
		MOCK_FAIL_TOOL=$l_fault_tool
		MOCK_FAIL_CALL=$l_fault_call
		MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR
		if [ -n "$l_fault_match" ]; then
			MOCK_FAIL_MATCH=$l_fault_match
			export MOCK_FAIL_MATCH
		fi
		if [ -n "$l_fault_status" ]; then
			MOCK_FAIL_STATUS=$l_fault_status
			export MOCK_FAIL_STATUS
		fi
		if [ "$l_fault_stderr" != "-" ]; then
			MOCK_FAIL_STDERR=$l_fault_stderr
			export MOCK_FAIL_STDERR
		fi
		"$@"
	) </dev/null || BLACKBOX_FAULT_STATUS=$?
	return "$BLACKBOX_FAULT_STATUS"
}

# Purpose: Fail unless the last run exited EXPECTED with one runtime failure
# report naming STAGE and exactly MESSAGE, and started no change after the
# injected failure (or at all, when none was injected).
# Usage: blackbox_assert_stopped <label> <expected> <status> <stage> <message>
blackbox_assert_stopped() {
	assertEquals "$1: exit status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" "$2" "$3"
	for l_stopped_line in "exit_status: $2" "failure_class: runtime" "failure_stage: $4" \
		"message: $5"; do
		grep -Fqx -e "$l_stopped_line" "$CASE_DIR/zxfer.stderr" ||
			fail "$1: missing report line: $l_stopped_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	assertEquals "$1: exactly one failure report" \
		1 "$(grep -c '^zxfer: failure report begin$' "$CASE_DIR/zxfer.stderr")"
	assertEquals "$1: no change starts after the failure; zfs log: $(cat "$ZFS_LOG")" "" \
		"$(awk '/^FAIL / { failed = 1 } /^MUTATE / && (failed || !injected) { print }' \
			injected="$(grep -c '^FAIL ' "$ZFS_LOG")" "$ZFS_LOG")"
}

# Purpose: Print how many control bytes other than LF and TAB a file holds.
# Usage: blackbox_count_control_bytes <file>
blackbox_count_control_bytes() {
	LC_ALL=C tr -dc '\001-\010\013-\037\177' <"$1" | wc -c | tr -d ' '
}

# Purpose: Empty one destination child's snapshot listings in STATE_DIR while
# keeping the dataset itself listed.
# Usage: blackbox_empty_destination_child <N|childN>
blackbox_empty_destination_child() {
	l_empty_num=${1#child}
	l_empty_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_empty_num"
	grep -v "^$l_empty_dataset@" "$STATE_DIR/dst_snapshots.list" \
		>"$STATE_DIR/dst_snapshots.list.new" || :
	mv "$STATE_DIR/dst_snapshots.list.new" "$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to empty $l_empty_dataset."
	: >"$STATE_DIR/dst_d1_$l_empty_num.list"
}

# Purpose: Make one destination child missing in STATE_DIR: drop it from the
# destination dataset, snapshot, and property inventories, and answer its exact
# `zfs list -H` probe with zfs's missing-dataset line.
# Usage: blackbox_drop_destination_child <N|childN>
blackbox_drop_destination_child() {
	l_drop_num=${1#child}
	l_drop_name="child$l_drop_num"
	l_drop_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/$l_drop_name"

	blackbox_empty_destination_child "$l_drop_num"
	for l_drop_fixture in dst_datasets.list dst_props_tree.list dst_props_tree.names; do
		[ -f "$STATE_DIR/$l_drop_fixture" ] || continue
		if ! awk -F'\t' -v dataset="$l_drop_dataset" '
			$1 == dataset || index($1, dataset "@") == 1 { next }
			{ print }' "$STATE_DIR/$l_drop_fixture" >"$STATE_DIR/$l_drop_fixture.new" ||
			! mv "$STATE_DIR/$l_drop_fixture.new" "$STATE_DIR/$l_drop_fixture"; then
			fail "Unable to drop $l_drop_dataset from $l_drop_fixture."
		fi
	done
	printf "cannot open '%s': dataset does not exist\n" "$l_drop_dataset" \
		>"$STATE_DIR/missing_$l_drop_name.list" ||
		fail "Unable to write the missing-dataset line."
	blackbox_prepend_manifest_rule "list -H $l_drop_dataset	missing_$l_drop_name.list	1"
}

# Purpose: Make one exact manifest key in STATE_DIR answer with no output and
# a non-zero status.
# Usage: planning_force_manifest_failure <argv-key> <status>
planning_force_manifest_failure() {
	l_force_key=$1
	l_force_status=$2

	rm -rf "$STATE_DIR/.exact"
	awk -F'\t' -v key="$l_force_key" -v status="$l_force_status" '
		BEGIN { OFS = "\t" }
		$1 == key { print $1, "-", status; next }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to rewrite manifest rule."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install rewritten manifest."
}

# Purpose: Configure the canned zfs so a receive into a matching dataset sleeps
# first, keeping that receive in flight for ordering or signal tests.
# Usage: planning_delay_canned_zfs_receive <dataset-glob> <seconds>; the glob
# is a sh case pattern matched against the receive's last argument.
planning_delay_canned_zfs_receive() {
	MOCK_ZFS_DELAY_RECEIVE_GLOB=$1
	MOCK_ZFS_DELAY_RECEIVE_SECONDS=$2
	export MOCK_ZFS_DELAY_RECEIVE_GLOB MOCK_ZFS_DELAY_RECEIVE_SECONDS
}

# Purpose: Configure the canned zfs so each call also appends its arguments,
# each in brackets ("[list] ... [my data] "), to ARGV_LOG: the argument
# boundaries the space-joined zfs log cannot show.
# Usage: planning_log_canned_zfs_argv; sets ARGV_LOG.
planning_log_canned_zfs_argv() {
	ARGV_LOG="$CASE_DIR/zfs_argv.log"
	: >"$ARGV_LOG"
	unset MOCK_ZFS_ARGV_VERBS MOCK_ZFS_ARGV_FORMAT
	MOCK_ZFS_ARGV_LOG=$ARGV_LOG
	export MOCK_ZFS_ARGV_LOG
}

# Purpose: Fail unless the zfs log holds the exact argv line.
# Usage: planning_assert_log_has_line <argv-line>
planning_assert_log_has_line() {
	l_expected_line=$1

	grep -Fx "$l_expected_line" "$ZFS_LOG" >/dev/null 2>&1 ||
		fail "Expected zfs log line missing: $l_expected_line
zfs log: $(cat "$ZFS_LOG" 2>/dev/null)"
}

# Purpose: Fail when the zfs log holds any MUTATE line.
# Usage: planning_assert_no_mutations
planning_assert_no_mutations() {
	if grep -q '^MUTATE ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero MUTATE lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

# Purpose: Fail when the zfs log holds any send or receive line.
# Usage: planning_assert_no_send_receive
planning_assert_no_send_receive() {
	if grep -Eq '^(send|receive) ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero send/receive lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

# Purpose: Print the 1-based line number of the first exact match of an argv
# line in the zfs log, or nothing. Exact matching avoids prefix collisions such
# as "receive .../data" vs ".../data/child1".
# Usage: planning_log_line_number <argv-line>
planning_log_line_number() {
	awk -v needle="$1" '$0 == needle { print NR; exit }' "$ZFS_LOG"
}

# Purpose: Count ssh invocations in SSH_LOG whose rendered remote script
# contains a marker. Long scripts cross csh/tcsh as single-quoted `d` data
# chunks; only the fixed boundary between adjacent chunks is removed, so a
# marker split at byte 128 stays visible without evaluating the command.
# Usage: planning_count_remote_script_marker <marker>
planning_count_remote_script_marker() {
	l_marker=$1
	awk -v marker="$l_marker" '
		BEGIN {
			quote = sprintf("%c", 39)
			data_boundary = quote " " quote "d"
		}
		{
			line = $0
			while ((position = index(line, data_boundary)) != 0)
				line = substr(line, 1, position - 1) \
					substr(line, position + length(data_boundary))
			if (index(line, marker) != 0) count++
		}
		END { print count + 0 }
	' "$SSH_LOG"
}

# Purpose: Fail unless stderr holds a runtime-class structured failure report
# with the given stage and message fragment.
# Usage: planning_assert_failure_report <stage> <message-fragment>
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

# ---------------------------------------------------------------------------
# Destination snapshot fixtures.

# Purpose: Give a destination dataset in STATE_DIR one extra destination-only
# snapshot in the recursive and depth-1 listings, plus the creation-time rule
# -d planning issues.
# Usage: planning_add_extra_destination_snapshot [suffix [snap [created
# [anchor [anchor-created]]]]]; defaults to "" snap9 1700000009 snap3
# 1700000003.
planning_add_extra_destination_snapshot() {
	l_extradst_suffix=${1:-}
	l_extradst_snap=${2:-snap9}
	l_extradst_created=${3:-1700000009}
	l_extradst_anchor=${4:-snap3}
	l_extradst_anchor_created=${5:-1700000003}
	l_extradst_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_extradst_suffix"
	case $l_extradst_suffix in
	'')
		l_extradst_d1=dst_d1_0.list
		l_extradst_guid="9999900009000000007"
		l_extradst_creation_file="dst_creation.list"
		;;
	/child*)
		l_extradst_idx=${l_extradst_suffix#/child}
		l_extradst_d1="dst_d1_$l_extradst_idx.list"
		l_extradst_guid="9999900${l_extradst_idx}09000000007"
		l_extradst_creation_file="dst_child${l_extradst_idx}_creation.list"
		;;
	esac
	for l_extradst_fixture in dst_snapshots.list "$l_extradst_d1"; do
		printf '%s@%s\t%s\n' "$l_extradst_dataset" "$l_extradst_snap" "$l_extradst_guid" \
			>>"$STATE_DIR/$l_extradst_fixture" ||
			fail "Unable to append extra snapshot to $l_extradst_fixture."
	done
	printf '%s@%s\t%s\n%s@%s\t%s\n' \
		"$l_extradst_dataset" "$l_extradst_anchor" "$l_extradst_anchor_created" \
		"$l_extradst_dataset" "$l_extradst_snap" "$l_extradst_created" \
		>"$STATE_DIR/$l_extradst_creation_file" ||
		fail "Unable to write creation-time fixture."
	rm -rf "$STATE_DIR/.exact"
	printf 'get -H -o name,value -p creation %s@*\t%s\t0\n' \
		"$l_extradst_dataset" "$l_extradst_creation_file" >>"$STATE_DIR/manifest" ||
		fail "Unable to append creation-time manifest rule."
}

# Purpose: Diverge child1's newest destination snapshot in STATE_DIR until its
# convergence receive heals it, adding the creation-time and existence rules
# that destroy and rollback read.
# Usage: planning_make_child1_diverged_until_receive [diverged-snap
# [anchor-snap]]; defaults to snap3 and snap2 (for noop STATE_DIR; pass snap2
# snap1 for incremental STATE_DIR).
planning_make_child1_diverged_until_receive() {
	l_divchild_snap=${1:-snap3}
	l_divchild_anchor=${2:-snap2}
	l_divchild_num=${l_divchild_snap#snap}
	l_divchild_anchor_num=${l_divchild_anchor#snap}
	l_divchild_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"

	awk -F'\t' -v name="$l_divchild_dataset@$l_divchild_snap" \
		-v guid="999990010${l_divchild_num}000000007" 'BEGIN { OFS = "\t" }
		$1 == name { $2 = guid }
		{ print }
	' "$STATE_DIR/dst_snapshots.list" >"$STATE_DIR/dst_snapshots_diverged.list" ||
		fail "Unable to write the diverged child1 listing."
	grep -v "^$l_divchild_dataset@$l_divchild_snap" "$STATE_DIR/dst_d1_1.list" \
		>"$STATE_DIR/dst_d1_1_post_destroy.list" ||
		fail "Unable to write the post-destroy child1 listing."
	printf '%s@%s\t170000000%s\n%s@%s\t170000000%s\n' \
		"$l_divchild_dataset" "$l_divchild_anchor" "$l_divchild_anchor_num" \
		"$l_divchild_dataset" "$l_divchild_snap" "$l_divchild_num" \
		>"$STATE_DIR/dst_child1_creation.list" ||
		fail "Unable to write the child1 creation-time fixture."
	printf '%s\t96K\t1.0G\t24K\t/%s\n' "$l_divchild_dataset" "$l_divchild_dataset" \
		>"$STATE_DIR/dst_exists_child1.list" ||
		fail "Unable to write the child1 existence fixture."
	rm -rf "$STATE_DIR/.exact"
	awk -F'\t' \
		-v key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		-v d1_key="list -H -d 1 -o name,guid -t snapshot $l_divchild_dataset" '
		BEGIN { OFS = "\t" }
		$1 == key { print key, "dst_snapshots_diverged.list", 0, "once" }
		$1 == d1_key { print d1_key, "dst_d1_1_post_destroy.list", 0, "once" }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the consumable diverged child1 listing rules."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the consumable diverged child1 listing rules."
	printf '%s\t%s\t0\n' \
		"get -H -o name,value -p creation $l_divchild_dataset@*" dst_child1_creation.list \
		"list -H $l_divchild_dataset" dst_exists_child1.list >>"$STATE_DIR/manifest" ||
		fail "Unable to append the child1 creation-time and existence rules."
}

# Purpose: Diverge the destination root's newest snapshot in STATE_DIR (same
# name @snap3, guid 9999900003000000007) in the recursive and depth-1
# listings, plus the creation-time rule for the destroy candidate and the
# last-common rollback anchor.
# Usage: planning_make_destination_diverged
planning_make_destination_diverged() {
	for l_diverge_fixture in dst_snapshots.list dst_d1_0.list; do
		awk -F'\t' 'BEGIN { OFS = "\t" }
			$1 == "dstpool/back/data@snap3" { $2 = "9999900003000000007" }
			{ print }
		' "$FIXTURE_DIR/noop/$l_diverge_fixture" \
			>"$STATE_DIR/$l_diverge_fixture" ||
			fail "Unable to rewrite $l_diverge_fixture with divergent guid."
	done
	printf '%s@snap2\t1700000002\n%s@snap3\t1700000003\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		>"$STATE_DIR/dst_creation.list" ||
		fail "Unable to write creation-time fixture."
	rm -rf "$STATE_DIR/.exact"
	printf 'get -H -o name,value -p creation %s@*\tdst_creation.list\t0\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append creation-time manifest rule."
}

# Purpose: Diverge the destination root like planning_make_destination_diverged,
# but heal it at convergence: every listing answers what the real pool would
# hold at that moment.
# Usage: planning_make_destination_diverged_until_receive
#
# Two consumable 'once' rules serve the diverged recursive listing before
# convergence (today one lookup: the fast no-op proof's listing, which
# discovery reuses); no recursive listing runs after it. The root's destroy
# changes it, so its pre-send re-plan (between the destroy and the rollback)
# reads the post-destroy snap1+snap2 rows through one 'once' rule, and the
# post-receive verification falls through to the aligned depth-1 fixture the
# convergence receive would have produced.
planning_make_destination_diverged_until_receive() {
	planning_make_destination_diverged
	mv "$STATE_DIR/dst_snapshots.list" "$STATE_DIR/dst_snapshots_diverged.list" ||
		fail "Unable to stage the diverged destination listing fixture."
	cp "$FIXTURE_DIR/noop/dst_snapshots.list" "$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to restore the aligned destination listing fixture."
	cp "$FIXTURE_DIR/noop/dst_d1_0.list" "$STATE_DIR/dst_d1_0.list" ||
		fail "Unable to restore the aligned depth-1 root listing fixture."
	grep -v '@snap3' "$FIXTURE_DIR/noop/dst_d1_0.list" \
		>"$STATE_DIR/dst_d1_0_post_destroy.list" ||
		fail "Unable to stage the post-destroy depth-1 root listing fixture."
	awk -F'\t' \
		-v key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		-v d1_key="list -H -d 1 -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" '
		BEGIN { OFS = "\t" }
		$1 == key {
			print key, "dst_snapshots_diverged.list", 0, "once"
			print key, "dst_snapshots_diverged.list", 0, "once"
		}
		$1 == d1_key {
			print d1_key, "dst_d1_0_post_destroy.list", 0, "once"
		}
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the consumable diverged listing rules."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the consumable diverged listing rules."
}

# ---------------------------------------------------------------------------
# Parallel (-j) fixtures.

# Purpose: Write a minimal GNU-parallel stand-in that skips options through
# "--" and runs the command once per stdin line. Like GNU parallel it replaces
# each {} with the line single-quoted for the shell (or appends it when the
# command has no {}), so a pre-quoted '{}' splits a name with spaces here as
# it does there. Sequential execution is a valid serialization of parallel's
# interleaving, so canned-zfs fixtures stay deterministic; each job gets
# stdin from /dev/null, like the real helper.
# Usage: planning_write_mock_parallel <path>
planning_write_mock_parallel() {
	l_parallel_path=$1

	cat >"$l_parallel_path" <<'EOF'
#!/bin/sh
[ -z "${MOCK_PARALLEL_ARGV_LOG:-}" ] || printf '%s\n' "$*" >>"$MOCK_PARALLEL_ARGV_LOG"
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
mock_parallel_q="'"
mock_parallel_status=0
while IFS= read -r mock_parallel_line || [ -n "$mock_parallel_line" ]; do
	[ -n "$mock_parallel_line" ] || continue
	# Quote the line: each ' becomes '\'' inside single quotes.
	mock_parallel_rest=$mock_parallel_line
	mock_parallel_quoted=$mock_parallel_q
	while :; do
		case $mock_parallel_rest in
		*"$mock_parallel_q"*)
			mock_parallel_head=${mock_parallel_rest%%"$mock_parallel_q"*}
			mock_parallel_quoted="$mock_parallel_quoted$mock_parallel_head$mock_parallel_q\\$mock_parallel_q$mock_parallel_q"
			mock_parallel_rest=${mock_parallel_rest#*"$mock_parallel_q"}
			;;
		*)
			mock_parallel_quoted="$mock_parallel_quoted$mock_parallel_rest$mock_parallel_q"
			break
			;;
		esac
	done
	# Substitute every {} with the quoted line.
	mock_parallel_rest=$mock_parallel_cmd
	mock_parallel_job=""
	while :; do
		case $mock_parallel_rest in
		*"{}"*)
			mock_parallel_head=${mock_parallel_rest%%"{}"*}
			mock_parallel_job="$mock_parallel_job$mock_parallel_head$mock_parallel_quoted"
			mock_parallel_rest=${mock_parallel_rest#*"{}"}
			;;
		*)
			mock_parallel_job="$mock_parallel_job$mock_parallel_rest"
			break
			;;
		esac
	done
	[ "$mock_parallel_job" != "$mock_parallel_cmd" ] ||
		mock_parallel_job="$mock_parallel_cmd $mock_parallel_quoted"
	sh -c "$mock_parallel_job" </dev/null || mock_parallel_status=$?
done
exit $mock_parallel_status
EOF
	chmod +x "$l_parallel_path"
}

# Purpose: Add the parallel source-discovery answers a -j run issues to
# STATE_DIR: the dataset enumeration plus one depth-1 creation-ordered
# snapshot listing per source dataset.
# Usage: planning_add_parallel_source_discovery_fixtures
planning_add_parallel_source_discovery_fixtures() {
	rm -rf "$STATE_DIR/.exact"
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

# Purpose: Build the shared -j environment: the incremental state cloned into
# STATE_DIR with parallel discovery answers, the mock parallel helper, and a
# private 0700 JOB_TMP_DIR to pass as TMPDIR so leftovers are observable.
# Usage: planning_setup_parallel_jobs_env <state-name>
planning_setup_parallel_jobs_env() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" "$1"
	planning_add_parallel_source_discovery_fixtures
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."
	JOB_TMP_DIR="$CASE_DIR/jobtmp"
	mkdir -p "$JOB_TMP_DIR" || fail "Unable to create the per-run temp root."
	chmod 700 "$JOB_TMP_DIR" || fail "Unable to restrict the per-run temp root."
}

# Purpose: Fail when per-job control files or mock zfs job processes survived
# a -j run.
# Usage: planning_assert_no_parallel_job_leftovers
planning_assert_no_parallel_job_leftovers() {
	l_leftover_files=$(find "$JOB_TMP_DIR" -mindepth 1 2>/dev/null || :)
	assertEquals "the per-run temp root must hold no leftover files after exit" \
		"" "$l_leftover_files"
	# shellcheck disable=SC2009  # the assertion is about the raw process table
	l_leftover_processes=$(ps -axo command= 2>/dev/null |
		grep -F "$MOCKBIN_DIR/zfs" | grep -v grep || :)
	assertEquals "no mock zfs job processes may survive the run" \
		"" "$l_leftover_processes"
}

# ---------------------------------------------------------------------------
# Remote transport assertions.

# Purpose: Assert the ssh control-master lifecycle recorded in $SSH_LOG:
# exactly MASTERS masters are opened, each on its own socket and before any
# other remote command; every later command multiplexes over one of those
# sockets; and each socket gets exactly one `-O exit`, after the last command.
# Usage: planning_assert_ssh_commands_multiplexed <masters>
planning_assert_ssh_commands_multiplexed() {
	l_mux_masters=$1
	l_mux_report=$(awk -F '\t' '
		# Print the word after -S, or nothing.
		function socket_of(line,    n, i, w) {
			n = split(line, w, " ")
			for (i = 1; i < n; i++)
				if (w[i] == "-S") return w[i + 1]
			return ""
		}
		$1 == "version" { next }
		$1 == "master" {
			socket = socket_of($2)
			if (commands || exits) bad = bad " master-after-command:" NR
			if (socket == "" || (socket in open)) bad = bad " master-socket:" NR
			open[socket] = 1
			masters++
			next
		}
		$1 == "control" && index($2, " -O exit ") {
			socket = socket_of($2)
			if (!(socket in open)) { bad = bad " direct:" NR; next }
			if (socket in closed) bad = bad " second-exit:" NR
			closed[socket] = 1
			exits++
			next
		}
		$1 == "mux" {
			socket = socket_of($2)
			if (!(socket in open)) { bad = bad " direct:" NR; next }
			if (exits) bad = bad " command-after-exit:" NR
			commands++
			next
		}
		$1 == "direct" || $1 == "control" || $1 == "fail" || $1 == "reject" {
			bad = bad " " $1 ":" NR
		}
		END {
			if (!commands) bad = bad " no-commands"
			printf "masters=%d exits=%d bad=%s\n", masters, exits, bad
		}' "$SSH_LOG")
	assertEquals "every remote command must multiplex over one of the opened masters; ssh log: $(cat "$SSH_LOG")" \
		"masters=$l_mux_masters exits=$l_mux_masters bad=" "$l_mux_report"
}

# ---------------------------------------------------------------------------
# Property pass (-P / -o / -I / -U) fixtures.

# Purpose: Seed both sides of STATE_DIR with the same default property rows,
# so the -P pass reads everything without planning any set or inherit.
# Usage: planning_add_property_transfer_fixtures
planning_add_property_transfer_fixtures() {
	l_default_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_default_rows" \
		"$l_default_rows" "$l_default_rows" "$l_default_rows"
}

# Purpose: Print the default `zfs get -H` property rows: four settable local
# properties (mountpoint among them, read-only for zxfer), the dataset type,
# and the three creation-time properties every filesystem carries.
# Usage: planning_property_default_rows
planning_property_default_rows() {
	printf '%s\t%s\t%s\n' \
		type filesystem - \
		mountpoint /mnt/data local \
		compression lz4 local \
		readonly off local \
		atime off local \
		casesensitivity sensitive - \
		normalization none - \
		utf8only off -
}

# Purpose: Print a row set with one property row replaced, or appended when
# absent.
# Usage: planning_property_rows_with <rows> <name> <value> <source>
planning_property_rows_with() {
	printf '%s\n' "$1" | awk -F'\t' -v OFS='\t' -v name="$2" -v value="$3" \
		-v source="$4" '
		$1 == name { print name, value, source; seen = 1; next }
		{ print }
		END { if (!seen) print name, value, source }'
}

# Purpose: Write property fixtures with independent row sets per side and
# level, and answer every `zfs get` shape the property pass issues for the
# 2-child tree: recursive machine and human reads and name skeletons, the
# same three per-dataset fallbacks, and dataset-type probes. Recursive rows
# lead with the dataset.
# Usage: planning_add_property_fixtures_for_rows <src-root-rows>
# <src-child-rows> <dst-root-rows> <dst-child-rows>
planning_add_property_fixtures_for_rows() {
	l_src_root_rows=$1
	l_src_child_rows=$2
	l_dst_root_rows=$3
	l_dst_child_rows=$4

	{
		printf '%s\n' "$l_src_root_rows" |
			awk -v dataset="$ZXFER_MOCKBIN_SOURCE_ROOT" '{ print dataset "\t" $0 }'
		for l_prop_suffix in /child1 /child2; do
			printf '%s\n' "$l_src_child_rows" |
				awk -v dataset="$ZXFER_MOCKBIN_SOURCE_ROOT$l_prop_suffix" \
					'{ print dataset "\t" $0 }'
		done
	} >"$STATE_DIR/src_props_tree.list" ||
		fail "Unable to write source property tree fixture."
	{
		printf '%s\n' "$l_dst_root_rows" |
			awk -v dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" '{ print dataset "\t" $0 }'
		for l_prop_suffix in /child1 /child2; do
			printf '%s\n' "$l_dst_child_rows" |
				awk -v dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prop_suffix" \
					'{ print dataset "\t" $0 }'
		done
	} >"$STATE_DIR/dst_props_tree.list" ||
		fail "Unable to write destination property tree fixture."
	printf '%s\n' "$l_src_root_rows" >"$STATE_DIR/src_props_root.list"
	printf '%s\n' "$l_src_child_rows" >"$STATE_DIR/src_props_child.list"
	printf '%s\n' "$l_dst_root_rows" >"$STATE_DIR/dst_props_root.list"
	printf '%s\n' "$l_dst_child_rows" >"$STATE_DIR/dst_props_child.list"
	# The property-name skeletons zxfer reads after each pair of value views.
	for l_prop_list in src_props_tree dst_props_tree; do
		cut -f1,2 "$STATE_DIR/$l_prop_list.list" >"$STATE_DIR/$l_prop_list.names"
	done
	for l_prop_list in src_props_root src_props_child dst_props_root dst_props_child; do
		cut -f1 "$STATE_DIR/$l_prop_list.list" >"$STATE_DIR/$l_prop_list.names"
	done
	printf 'filesystem\n' >"$STATE_DIR/type_filesystem.list"
	rm -rf "$STATE_DIR/.exact"
	zxfer_mockbin_append_property_tree_rules "$STATE_DIR/manifest" ||
		fail "Unable to append shared property tree manifest rules."
	printf '%s\t%s\t0\n' \
		"get -Ho property all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_root.names \
		"get -Ho property all $ZXFER_MOCKBIN_SOURCE_ROOT/child*" src_props_child.names \
		"get -Ho property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" dst_props_root.names \
		"get -Ho property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child*" dst_props_child.names \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT/child*" src_props_child.list \
		"get -Ho property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT/child*" src_props_child.list \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child*" dst_props_child.list \
		"get -Ho property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child*" dst_props_child.list \
		"get -Hpo value type $ZXFER_MOCKBIN_SOURCE_ROOT*" type_filesystem.list \
		"get -Hpo value type $ZXFER_MOCKBIN_DEST_ROOT*" type_filesystem.list \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append property manifest rules."
}

# Purpose: Answer every destination-support probe the -U scan may issue:
# source dataset types, source and destination property-name inventories, and
# per-property destination probes where "overlay" is unknown and every other
# property supported. Call after planning_add_property_fixtures_for_rows so
# its exact per-dataset rules win over the trailing per-property glob.
# Usage: planning_add_unsupported_property_fixtures <src-rows> <dst-rows>
planning_add_unsupported_property_fixtures() {
	l_unsupported_src_rows=$1
	l_unsupported_dst_rows=$2

	rm -rf "$STATE_DIR/.exact"

	printf '%s\tfilesystem\n' "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/child1" "$ZXFER_MOCKBIN_SOURCE_ROOT/child2" \
		>"$STATE_DIR/src_types.list"
	printf '%s\n' "$l_unsupported_src_rows" | awk -F'\t' '{ print $1 }' \
		>"$STATE_DIR/src_prop_names.list"
	printf '%s\n' "$l_unsupported_dst_rows" | awk -F'\t' '{ print $1 }' \
		>"$STATE_DIR/dst_prop_names.list"
	printf "bad property list: invalid property 'overlay'\n" \
		>"$STATE_DIR/unsupported_overlay.list"
	printf 'compression\tlz4\tlocal\n' >"$STATE_DIR/probe_ok.list"
	printf '%s\t%s\t%s\n' \
		"get -Hpo name,value type $ZXFER_MOCKBIN_SOURCE_ROOT*" src_types.list 0 \
		"get -Hpo property all $ZXFER_MOCKBIN_SOURCE_ROOT*" src_prop_names.list 0 \
		"get -Hpo property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" dst_prop_names.list 0 \
		"get -Hpo property,value,source overlay $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" unsupported_overlay.list 1 \
		"get -Hpo property,value,source * $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" probe_ok.list 0 \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append unsupported-property manifest rules."
}

# Purpose: Check that property reads cross ssh only for the selected remote
# side, including live and lone reads. SSH_LOG holds single-quoted tokens.
# Usage: planning_assert_property_reads_routed_by_side O|T
planning_assert_property_reads_routed_by_side() {
	l_routed_remote=$ZXFER_MOCKBIN_DEST_ROOT
	l_routed_local=$ZXFER_MOCKBIN_SOURCE_ROOT
	if [ "$1" = O ]; then
		l_routed_remote=$ZXFER_MOCKBIN_SOURCE_ROOT
		l_routed_local=$ZXFER_MOCKBIN_DEST_ROOT
	fi
	l_routed_ssh_argv=$(tr -d "'" <"$SSH_LOG")
	l_routed_dst_gets=$(grep '^get ' "$ZFS_LOG" | grep -F -e " $l_routed_remote" || :)
	assertNotNull "-$1: the remote side's properties must be read; zfs log: $(cat "$ZFS_LOG")" \
		"$l_routed_dst_gets"
	while IFS= read -r l_routed_dst_get; do
		[ -n "$l_routed_dst_get" ] || continue
		case "$l_routed_ssh_argv" in
		*"$l_routed_dst_get"*) ;;
		*)
			fail "-$1: property read ran locally instead of over ssh: $l_routed_dst_get
ssh log: $(cat "$SSH_LOG")"
			;;
		esac
	done <<EOF
$l_routed_dst_gets
EOF
	assertEquals "-$1: no read of the local side may cross ssh" \
		0 "$(printf '%s\n' "$l_routed_ssh_argv" | grep -c -e "get .* $l_routed_local" || :)"
}

# Purpose: Run the property pass recursively against STATE_DIR and assert
# exit 0, leaving the zfs log ready for argv assertions.
# Usage: planning_run_property_pass [zxfer-arg...]
planning_run_property_pass() {
	planning_run_zxfer "$STATE_DIR" "$@" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "property pass ($*) should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
}

# ---------------------------------------------------------------------------
# -k / -e property backup metadata fixtures.
#
# The documented layout (man/zxfer.8) puts the exact-pair file at
# ZXFER_BACKUP_DIR/<source>/.zxfer_backup_info.v2/h/<chunks>/
# .zxfer_backup_info.v2, where <chunks> is the hex of "<source>\n<destination>"
# split into 48-character components. It is derived here independently
# (od + awk) so the pins fail if the layout drifts.

# Purpose: Print the documented exact-pair metadata path for one pair.
# Usage: planning_backup_metadata_file <backup-root> <source> <destination>
planning_backup_metadata_file() {
	l_backup_key=$(printf '%s\n%s' "$2" "$3" | od -An -tx1 -v | tr -d ' \n' |
		awk '{ p = "h"; for (i = 1; i <= length($0); i += 48) p = p "/" substr($0, i, 48); print p }')
	printf '%s/%s/.zxfer_backup_info.v2/%s/.zxfer_backup_info.v2\n' "$1" "$2" "$l_backup_key"
}

# Purpose: Build the -k environment: property fixtures on a noop STATE_DIR, a
# counting `mv` wrapper (one rename per published file is the write-once pin),
# and a case-local backup root. Publishes BACKUP_ROOT, PRIMARY_FILE,
# FORWARDED_FILE, and SPAWN_LOG.
# Usage: planning_setup_backup_env <state-name>
planning_setup_backup_env() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	planning_add_property_transfer_fixtures
	zxfer_mockbin_write_counting_wrapper "$MOCKBIN_DIR/mv" \
		"$(zxfer_mockbin_resolve_host_tool mv)" ||
		fail "Unable to write counting mv wrapper."
	BACKUP_ROOT="$CASE_DIR/backup"
	PRIMARY_FILE=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT")
	FORWARDED_FILE=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	SPAWN_LOG="$CASE_DIR/spawn.log"
}

# Purpose: Run ./zxfer recursively against STATE_DIR with
# ZXFER_BACKUP_DIR=$BACKUP_ROOT and a fresh spawn log armed.
# Usage: planning_run_backup_zxfer [zxfer-arg...]; returns zxfer's status.
planning_run_backup_zxfer() {
	: >"$SPAWN_LOG"
	export MOCK_SPAWN_LOG="$SPAWN_LOG" ZXFER_BACKUP_DIR="$BACKUP_ROOT"
	planning_run_zxfer "$STATE_DIR" "$@" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_backup_run_status=$?
	unset MOCK_SPAWN_LOG ZXFER_BACKUP_DIR
	return "$l_backup_run_status"
}

# Purpose: Fail unless a metadata file is mode 0600 with the version-2 header
# for the given roots and exactly one relative row per dataset, and no stage
# file is left under BACKUP_ROOT.
# Usage: planning_assert_backup_file_is_current_format <file> <source-root>
# <destination-root>
planning_assert_backup_file_is_current_format() {
	l_backup_file=$1
	l_backup_source_root=$2
	l_backup_destination_root=$3

	assertTrue "backup metadata file must exist: $l_backup_file
stderr: $(cat "$CASE_DIR/zxfer.stderr")" "[ -f '$l_backup_file' ]"
	case "$(ls -ldn "$l_backup_file")" in
	-rw-------*) ;;
	*) fail "backup metadata must be mode 0600: $(ls -ldn "$l_backup_file")" ;;
	esac
	for l_backup_header_line in \
		"#zxfer property backup file" \
		"#format_version:2" \
		"#source_root:$l_backup_source_root" \
		"#destination_root:$l_backup_destination_root"; do
		grep -Fxq "$l_backup_header_line" "$l_backup_file" ||
			fail "backup metadata header line missing: $l_backup_header_line
file: $(cat "$l_backup_file")"
	done
	assertEquals "the first line must be the header" \
		"#zxfer property backup file" "$(sed -n '1p' "$l_backup_file")"
	for l_backup_row_key in . child1 child2; do
		assertEquals "exactly one relative row for $l_backup_row_key" \
			1 "$(grep -c "^$l_backup_row_key	" "$l_backup_file")"
		grep -q "^$l_backup_row_key	.*compression=lz4=local" "$l_backup_file" ||
			fail "row $l_backup_row_key must record the source property; file: $(cat "$l_backup_file")"
	done
	assertEquals "no stage files may be left behind under the backup root" \
		"" "$(find "$BACKUP_ROOT" -name '.zxfer-backup-write.*' 2>/dev/null)"
}

# ---------------------------------------------------------------------------
# -m / -c service manager fixture.

# Purpose: Write a mock SMF svcadm that appends "svcadm <argv>" to MOCK_ZFS_LOG,
# so the log orders every service change against the zfs calls, and fails
# (status 1, with a stderr note) each call whose space-joined argv matches the
# sh glob in MOCK_SVCADM_FAIL. Placed in MOCKBIN_DIR, it shadows the real
# svcadm of an illumos host, which a test must never run.
# Usage: planning_write_mock_svcadm <path>
planning_write_mock_svcadm() {
	cat >"$1" <<'EOF'
#!/bin/sh
[ -z "${MOCK_ZFS_LOG:-}" ] || printf 'svcadm %s\n' "$*" >>"$MOCK_ZFS_LOG"
if [ -n "${MOCK_SVCADM_FAIL:-}" ]; then
	case "$*" in
	$MOCK_SVCADM_FAIL)
		printf 'svcadm: mock failure: %s\n' "$*" >&2
		exit 1
		;;
	esac
fi
exit 0
EOF
	chmod +x "$1"
}

# Purpose: Write the forwarded alias an earlier -k hop leaves for ROOT (the
# current-format file keyed ROOT/ROOT) under BACKUP_ROOT: 0700 directories,
# a 0600 file, and the given relative rows.
# Usage: planning_write_backup_alias <root> <row>...
planning_write_backup_alias() {
	l_alias_root=$1
	shift
	l_alias_file=$(planning_backup_metadata_file "$BACKUP_ROOT" "$l_alias_root" \
		"$l_alias_root")
	(umask 077 && mkdir -p "${l_alias_file%/*}") ||
		fail "Unable to create the alias directory of $l_alias_root."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$l_alias_root" "#destination_root:$l_alias_root" "$@" \
		>"$l_alias_file" || fail "Unable to write the alias of $l_alias_root."
	chmod 600 "$l_alias_file"
}

# Purpose: Run planning_run_backup_zxfer for an -O or -T case: the mock ssh
# logs to a fresh SSH_LOG, and PATH leads with MOCKBIN_DIR in a subshell so
# it never leaks into the suite.
# Usage: planning_run_backup_zxfer_over_ssh [zxfer-arg...]; needs SSH_LOG
# and a mock ssh in MOCKBIN_DIR. Returns zxfer's status.
planning_run_backup_zxfer_over_ssh() {
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"
	(
		PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		export PATH
		planning_run_backup_zxfer "$@"
	)
	l_over_ssh_status=$?
	unset MOCK_SSH_LOG
	return "$l_over_ssh_status"
}

# ---------------------------------------------------------------------------
# Listing fault and codec fixtures.

# Purpose: Make one exact manifest key in STATE_DIR succeed with no output: a
# listing that finds nothing.
# Usage: planning_answer_manifest_key_with_nothing <argv-key>
planning_answer_manifest_key_with_nothing() {
	planning_force_manifest_failure "$1" 0
}

# Purpose: Configure the canned zfs so the first call whose space-joined argv
# is exactly ARGV answers from the manifest as usual and then prints STDERR
# and exits STATUS: a listing that dies after streaming its rows, or with
# STATUS 0 one that succeeds with a warning. Later calls with that argv answer
# normally. Calling the helper again re-arms the state with new values.
# Usage: planning_fail_canned_zfs_after_output <argv> <status> [<stderr>]
planning_fail_canned_zfs_after_output() {
	l_after_dir="$CASE_DIR/after_output"
	mkdir -p "$l_after_dir" ||
		fail "Unable to create the after-output state."
	printf '%s\n' "$1" >"$l_after_dir/argv" ||
		fail "Unable to arm the after-output argv."
	printf '%s\n' "$2" >"$l_after_dir/status" ||
		fail "Unable to arm the after-output status."
	: >"$l_after_dir/stderr" || fail "Unable to arm the after-output stderr."
	if [ -n "${3:-}" ]; then
		printf '%s\n' "$3" >"$l_after_dir/stderr" ||
			fail "Unable to write the after-output stderr."
	fi
	rm -f "$l_after_dir/fired"
	MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR=$l_after_dir
	export MOCK_ZFS_FAIL_AFTER_OUTPUT_DIR
}

# Purpose: Write a zstd stand-in whose "compression" prefixes every line with
# "zstd:" and whose -d strips that prefix again, failing on a line without
# it, so a stream that skips either side of the codec no longer parses. Each
# call appends its arguments to <path>.argv.
# Usage: planning_write_mock_zstd <path>
planning_write_mock_zstd() {
	cat >"$1" <<EOF_ZSTD
#!/bin/sh
printf '%s\n' "\$*" >>"$1.argv"
for mock_zstd_arg in "\$@"; do
	[ "\$mock_zstd_arg" = -d ] || continue
	exec awk 'substr(\$0, 1, 5) != "zstd:" { bad = 1; next }
		{ print substr(\$0, 6) }
		END { exit bad }'
done
exec sed 's/^/zstd:/'
EOF_ZSTD
	chmod +x "$1"
}

# Purpose: Configure the mock parallel so each call appends its arguments,
# space-joined, to PARALLEL_ARGV_LOG, locally and on a mock-ssh origin alike.
# Usage: planning_log_mock_parallel_argv, after planning_setup_parallel_jobs_env;
# sets PARALLEL_ARGV_LOG.
planning_log_mock_parallel_argv() {
	PARALLEL_ARGV_LOG="$CASE_DIR/parallel.argv"
	: >"$PARALLEL_ARGV_LOG"
	MOCK_PARALLEL_ARGV_LOG=$PARALLEL_ARGV_LOG
	export MOCK_PARALLEL_ARGV_LOG
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
		blackbox_prepend_manifest_rule "list -H $l_missing_dataset	$l_missing_fixture	1"
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
