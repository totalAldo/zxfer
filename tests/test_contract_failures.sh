#!/bin/sh
#
# Black-box fail-closed contract for ./zxfer: whenever a zfs or ssh call
# fails, zxfer either absorbs the failure through a fallback that changes
# nothing, or stops without starting another mutation.
#
# Each scenario drives the REAL launcher against the canned zfs from
# tests/mock_toolchain_helper.sh on a small tree (a root and two children,
# three snapshots each) chosen so its mutating path really runs. A clean run
# numbers every zfs call (MOCK_FAIL_CALL=0) and records the ordered list of
# mutating calls; then each call is failed in turn (MOCK_FAIL_CALL=K of
# MOCK_FAIL_MATCH=<its argv>, so "the Kth call with this argv" is the same
# call however background discovery or -j jobs interleave) and every run
# must end in exactly one of:
#   (a) exit 0 with the same mutating calls as the clean run: a fallback
#       absorbed the failure (for example the fast no-op proof's listing
#       failing over to full discovery); or
#   (b) a non-zero exit, one structured runtime failure report on stderr
#       whose exit_status matches, and no mutating zfs call that STARTS
#       after the injected failure in MOCK_ZFS_LOG.
# Anything else is a fail-open and fails the case with the run's zfs log.
# Every run must also leave its private TMPDIR empty.
#
# Mutating calls are the canned zfs's MUTATE lines (create, destroy,
# rollback, set, inherit, snapshot, rename, clone, promote, hold, release,
# bookmark) plus receive, mount and unmount. Two exact exceptions let a
# mutation start after the failure, because it was started together with the
# failing call before zxfer could see the failure:
#   pipeline partner  a failed `zfs send` (or the -O ssh that runs it) shares
#                     its pipeline with the receive into the matching
#                     destination; that receive may log its start after the
#                     FAIL line. The receives here are strict
#                     (MOCK_ZFS_STRICT_RECEIVE=1): like zfs receive, they
#                     refuse the empty stream a failed send leaves.
#   -j siblings       with -j 2 both children become ready when the root's
#                     receive ends and are launched back to back, so when a
#                     call of one child's pipeline fails, the other child's
#                     receive may already be on its way.
# With -j the receives run concurrently, so (a) compares the mutating calls
# as a sorted list there.
#
# Remote scenarios (-O localhost, -T localhost) run through the socket-aware
# mock ssh and are swept twice: once failing each zfs call and once failing
# each ssh call. ssh argv carries per-run socket paths, so ssh calls are
# failed by position (MOCK_FAIL_CALL=N); zxfer issues its ssh calls one at a
# time in these scenarios.
#
# The -k scenario also holds the backup metadata to the contract: an
# absorbed failure must publish the same files as the clean run, and a run
# that stops must publish none. -e must never change its backup files.
#
# Modes: the default run fails every zfs call of every scenario and every
# ssh call of the remote scenarios. ZXFER_FAILURE_SWEEP=full repeats every
# failing run with the other failure shapes a real host produces: status 2
# with "dataset is busy" for zfs, and a closed control connection with
# status 255 but no stderr for ssh. Each scenario prints one summary line
# ("contract sweep: ...") with its run count and time, and
# ZXFER_FAILURE_SWEEP_TRACE=FILE appends one TSV line per failing run:
# scenario, verdict, exit status, and the failed call (tool, number, failure
# shape and argv).
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# Seconds one zxfer run may take before the watchdog stops it; a hang after
# an injected failure is itself a contract failure.
CONTRACT_RUN_TIMEOUT=60

# ---------------------------------------------------------------------------
# Scenario environment.

# Purpose: Build the case environment every scenario starts from: the
# canned zfs, the fixture tree, the socket-aware mock ssh and the run slot.
# Usage: contract_setup; publishes the planning_* paths plus RUN_DIR, and
# resets the launcher (CONTRACT_ZXFER_BIN) and the CONTRACT_* scenario knobs.
contract_setup() {
	planning_setup_env
	CONTRACT_ZXFER_BIN="$ZXFER_ROOT/zxfer"
	CONTRACT_RUNS=0
	zxfer_mockbin_write_socket_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write the socket-aware mock ssh."
	RUN_DIR="$CASE_DIR/run"
	CONTRACT_BACKUP_TEMPLATE=""
	CONTRACT_SIBLINGS=""
	CONTRACT_SORTED=0
	CONTRACT_BACKUP_EMPTY=0
	CONTRACT_BACKUP_CHECK=""
}

# Purpose: Give one destination child a destination-only snapshot @snap9
# (created 1700000009, Nov 2023) in the recursive listing, plus the
# creation-time rule -d planning issues. The child's depth-1 listing and the
# existence probe answer the state after the destroy, which is when zxfer
# reads them.
# Usage: contract_add_extra_child_snapshot <child-index>
contract_add_extra_child_snapshot() {
	l_extra_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$1"
	printf '%s@snap9\t9999900%s09000000007\n' "$l_extra_child" "$1" \
		>>"$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to append the destination-only snapshot."
	awk -F'\t' -v child="$l_extra_child@" \
		'index($1, child) == 1 { sub(/^.*@/, "", $1); print child $1 "\t" 1700000000 + substr($1, 5) }' \
		"$STATE_DIR/dst_snapshots.list" >"$STATE_DIR/dst_child$1_creation.list" ||
		fail "Unable to write the creation-time fixture."
	contract_add_child_existence_rule "$1"
	printf 'get -H -o name,value -p creation %s@*\tdst_child%s_creation.list\t0\n' \
		"$l_extra_child" "$1" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the creation-time rule."
}

# Purpose: Answer the live existence probe (`zfs list -H`) for one
# destination child, which zxfer issues once the child has been changed.
# Usage: contract_add_child_existence_rule <child-index>
contract_add_child_existence_rule() {
	l_exists_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$1"
	printf '%s\t96K\t1.0G\t24K\t/%s\n' "$l_exists_child" "$l_exists_child" \
		>"$STATE_DIR/dst_exists_child$1.list" ||
		fail "Unable to write the child existence fixture."
	printf 'list -H %s\tdst_exists_child%s.list\t0\n' "$l_exists_child" "$1" \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append the child existence rule."
}

# Purpose: Diverge child1's newest snapshot (same name @snap3, guid
# 9999900103000000007) until the convergence receive heals it: one 'once'
# rule serves the diverged recursive listing to the no-op proof (discovery
# reuses it), one serves the post-destroy depth-1 listing to the recheck
# between the destroy and the rollback, and the post-receive verification
# falls through to the aligned listing the receive would have produced.
# Usage: contract_make_child_diverged_until_receive (on a noop STATE_DIR)
contract_make_child_diverged_until_receive() {
	l_diverged_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	awk -F'\t' -v name="$l_diverged_child@snap3" 'BEGIN { OFS = "\t" }
		$1 == name { $2 = "9999900103000000007" }
		{ print }
	' "$STATE_DIR/dst_snapshots.list" >"$STATE_DIR/dst_snapshots_diverged.list" ||
		fail "Unable to write the diverged listing."
	grep -v "^$l_diverged_child@snap3" "$STATE_DIR/dst_d1_1.list" \
		>"$STATE_DIR/dst_d1_1_post_destroy.list" ||
		fail "Unable to write the post-destroy listing."
	printf '%s@snap2\t1700000002\n%s@snap3\t1700000003\n' \
		"$l_diverged_child" "$l_diverged_child" \
		>"$STATE_DIR/dst_child1_creation.list" ||
		fail "Unable to write the creation-time fixture."
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
	printf 'get -H -o name,value -p creation %s@*\tdst_child1_creation.list\t0\n' \
		"$l_diverged_child" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the creation-time rule."
	contract_add_child_existence_rule 1
}

# ---------------------------------------------------------------------------
# Runs.

# Purpose: Run ./zxfer once from a fresh copy of STATE_DIR (and of
# CONTRACT_BACKUP_TEMPLATE as ZXFER_BACKUP_DIR, when set) in RUN_DIR, with
# the counter armed for one tool, under a watchdog.
# Usage: contract_run <tool> <call> <match> [zxfer-arg...]; an empty match
# counts every call of the tool. Sets CONTRACT_STATUS; RUN_DIR keeps
# zfs.log, ssh.log, stdout, stderr and the calls/ counter. Returns 1 without
# running zxfer when the run cannot be staged.
contract_run() {
	l_run_tool=$1
	l_run_call=$2
	l_run_match=$3
	shift 3
	CONTRACT_STATUS=""

	if [ ! -x "$MOCKBIN_DIR/zfs" ]; then
		fail "The canned zfs is missing; refusing to run zxfer."
		return 1
	fi
	if ! rm -rf "$RUN_DIR" || ! mkdir "$RUN_DIR" ||
		! cp -Rp "$STATE_DIR" "$RUN_DIR/state" ||
		! mkdir "$RUN_DIR/calls" "$RUN_DIR/tmp" ||
		! chmod 700 "$RUN_DIR/tmp"; then
		fail "Unable to stage the run directory $RUN_DIR."
		return 1
	fi
	if [ -n "$CONTRACT_BACKUP_TEMPLATE" ] &&
		! cp -Rp "$CONTRACT_BACKUP_TEMPLATE" "$RUN_DIR/backup"; then
		fail "Unable to copy the backup template."
		return 1
	fi
	: >"$RUN_DIR/zfs.log"
	: >"$RUN_DIR/ssh.log"

	(
		MOCK_ZFS_LOG="$RUN_DIR/zfs.log"
		MOCK_ZFS_FIXTURE_DIR="$RUN_DIR/state"
		MOCK_ZFS_STRICT_RECEIVE=1
		MOCK_SSH_LOG="$RUN_DIR/ssh.log"
		MOCK_FAIL_TOOL=$l_run_tool
		MOCK_FAIL_CALL=$l_run_call
		MOCK_FAIL_DIR="$RUN_DIR/calls"
		ZXFER_SECURE_PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		ZXFER_SECURE_PATH_APPEND=""
		PATH=$ZXFER_SECURE_PATH
		TMPDIR="$RUN_DIR/tmp"
		export MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_STRICT_RECEIVE \
			MOCK_SSH_LOG MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR \
			ZXFER_SECURE_PATH ZXFER_SECURE_PATH_APPEND PATH TMPDIR
		if [ -n "$l_run_match" ]; then
			MOCK_FAIL_MATCH=$l_run_match
			export MOCK_FAIL_MATCH
		fi
		if [ -n "$CONTRACT_BACKUP_TEMPLATE" ] || [ "${CONTRACT_BACKUP_EMPTY:-0}" -eq 1 ]; then
			ZXFER_BACKUP_DIR="$RUN_DIR/backup"
			export ZXFER_BACKUP_DIR
		fi
		exec "$CONTRACT_ZXFER_BIN" "$@"
	) </dev/null >"$RUN_DIR/stdout" 2>"$RUN_DIR/stderr" &
	l_run_pid=$!
	contract_watchdog_start "$l_run_pid"
	wait "$l_run_pid"
	CONTRACT_STATUS=$?
	kill "$CONTRACT_WATCHDOG_PID" 2>/dev/null
	wait "$CONTRACT_WATCHDOG_PID" 2>/dev/null
	CONTRACT_RUNS=$((CONTRACT_RUNS + 1))
	return 0
}

# Purpose: Stop a zxfer run that outlives CONTRACT_RUN_TIMEOUT seconds and
# leave RUN_DIR/timed_out behind.
# Usage: contract_watchdog_start <pid>; sets CONTRACT_WATCHDOG_PID. The
# watchdog holds no descriptor of the suite, so killing it never delays the
# runner.
contract_watchdog_start() {
	(
		l_watchdog_ticks=0
		while kill -s 0 "$1" 2>/dev/null; do
			if [ "$l_watchdog_ticks" -ge "$CONTRACT_RUN_TIMEOUT" ]; then
				: >"$RUN_DIR/timed_out"
				kill -s TERM "$1" 2>/dev/null
				sleep 5
				kill -s KILL "$1" 2>/dev/null
				exit 0
			fi
			sleep 1
			l_watchdog_ticks=$((l_watchdog_ticks + 1))
		done
	) </dev/null >/dev/null 2>&1 &
	CONTRACT_WATCHDOG_PID=$!
}

# Purpose: Print the mutating calls of a zfs log, in log order.
# Usage: contract_mutations <zfs-log>
contract_mutations() {
	grep -E '^(MUTATE |receive |recv |unmount |mount )' "$1" || :
}

# Purpose: Print how many calls the run numbered in RUN_DIR/calls; numbers
# are claimed from 1 without gaps.
# Usage: contract_claimed_count
contract_claimed_count() {
	l_claimed_count=0
	while [ -f "$RUN_DIR/calls/$((l_claimed_count + 1))" ]; do
		l_claimed_count=$((l_claimed_count + 1))
	done
	printf '%s\n' "$l_claimed_count"
}

# Purpose: Print every file under a backup root with its contents, so two
# backup trees compare as one string. The #backup_date header line records
# when the file was written, so it is left out.
# Usage: contract_backup_digest <backup-root>; prints nothing for a missing
# or empty root.
contract_backup_digest() {
	[ -d "$1" ] || return 0
	find "$1" -type f -print | sort | while IFS= read -r l_digest_file; do
		printf '== %s\n' "${l_digest_file#"$1"/}"
		sed '/^#backup_date:/d' "$l_digest_file"
	done
}

# ---------------------------------------------------------------------------
# The sweep.

# Purpose: Run one scenario clean, then fail each of its calls of one tool
# in turn and hold every run to the (a)/(b) contract in the header.
# Usage: contract_sweep <label> <zfs|ssh> [zxfer-arg...]
contract_sweep() {
	l_sweep_label=$1
	l_sweep_tool=$2
	shift 2
	CONTRACT_RUNS=0
	CONTRACT_ABSORBED=0
	CONTRACT_STOPPED=0
	l_sweep_started=$(date '+%s')

	contract_run "$l_sweep_tool" 0 "" "$@" || return 0
	if [ "$CONTRACT_STATUS" -ne 0 ]; then
		fail "[$l_sweep_label] the clean run must succeed (status $CONTRACT_STATUS); stderr: $(cat "$RUN_DIR/stderr")"
		return 0
	fi
	assertEquals "[$l_sweep_label] the clean run must leave its TMPDIR empty" \
		"" "$(ls -A "$RUN_DIR/tmp" 2>/dev/null)"
	CONTRACT_CLEAN_DIR="$CASE_DIR/clean_$l_sweep_label"
	rm -rf "$CONTRACT_CLEAN_DIR"
	mv "$RUN_DIR" "$CONTRACT_CLEAN_DIR" ||
		fail "Unable to keep the clean run."
	contract_mutations "$CONTRACT_CLEAN_DIR/zfs.log" \
		>"$CONTRACT_CLEAN_DIR/mutations"
	sort "$CONTRACT_CLEAN_DIR/mutations" >"$CONTRACT_CLEAN_DIR/mutations.sorted"
	contract_backup_digest "$CONTRACT_CLEAN_DIR/backup" \
		>"$CONTRACT_CLEAN_DIR/backup.digest"
	if [ "$CONTRACT_BACKUP_CHECK" = readonly ] &&
		[ "$(contract_backup_digest "$CONTRACT_BACKUP_TEMPLATE")" != "$(cat "$CONTRACT_CLEAN_DIR/backup.digest")" ]; then
		fail "[$l_sweep_label] the clean run must not change its backup files"
	fi
	RUN_DIR="$CONTRACT_CLEAN_DIR"
	l_sweep_calls=$(contract_claimed_count)
	RUN_DIR="$CASE_DIR/run"
	assertTrue "[$l_sweep_label] the clean run must make $l_sweep_tool calls" \
		"[ '$l_sweep_calls' -gt 0 ]"
	# A scenario whose fixture drifted into a no-op would sweep nothing
	# worth sweeping: the clean run must mutate, or for -k publish metadata.
	l_sweep_mutating=$(wc -l <"$CONTRACT_CLEAN_DIR/mutations" | tr -d ' ')
	if [ "$CONTRACT_BACKUP_CHECK" = publish ]; then
		assertNotNull "[$l_sweep_label] the clean run must publish backup metadata" \
			"$(cat "$CONTRACT_CLEAN_DIR/backup.digest")"
	else
		assertTrue "[$l_sweep_label] the clean run must make a mutating zfs call" \
			"[ '$l_sweep_mutating' -gt 0 ]"
	fi

	# One target per clean call: "<occurrence><TAB><argv>" for zfs, where
	# the occurrence counts earlier calls with the same argv. A zfs argv
	# holding a newline could not be matched line by line, so it is refused.
	l_sweep_index=1
	while [ "$l_sweep_index" -le "$l_sweep_calls" ]; do
		cat "$CONTRACT_CLEAN_DIR/calls/$l_sweep_index"
		l_sweep_index=$((l_sweep_index + 1))
	done | awk '{ seen[$0]++; print seen[$0] "\t" $0 }' \
		>"$CONTRACT_CLEAN_DIR/targets"
	if [ "$l_sweep_tool" = zfs ] &&
		[ "$(wc -l <"$CONTRACT_CLEAN_DIR/targets" | tr -d ' ')" -ne "$l_sweep_calls" ]; then
		fail "[$l_sweep_label] a zfs argv holds a newline; the sweep cannot target it"
		return 0
	fi
	l_sweep_tab=$(printf '\t')

	for l_sweep_shape in $(contract_failure_shapes "$l_sweep_tool"); do
		l_sweep_index=0
		while [ "$l_sweep_index" -lt "$l_sweep_calls" ]; do
			l_sweep_index=$((l_sweep_index + 1))
			if [ "$l_sweep_tool" = zfs ]; then
				l_sweep_target=$(sed -n "${l_sweep_index}p" "$CONTRACT_CLEAN_DIR/targets")
				l_sweep_argv=${l_sweep_target#*"$l_sweep_tab"}
				l_sweep_call=${l_sweep_target%%"$l_sweep_tab"*}
				l_sweep_match=$(contract_glob_literal "$l_sweep_argv") || {
					fail "[$l_sweep_label] cannot match call $l_sweep_index literally: $l_sweep_argv"
					continue
				}
			else
				l_sweep_argv=$(tr '\n' ' ' <"$CONTRACT_CLEAN_DIR/calls/$l_sweep_index")
				l_sweep_argv=${l_sweep_argv% }
				l_sweep_call=$l_sweep_index
				l_sweep_match=""
			fi
			contract_apply_failure_shape "$l_sweep_shape"
			l_sweep_staged=0
			contract_run "$l_sweep_tool" "$l_sweep_call" "$l_sweep_match" "$@" ||
				l_sweep_staged=1
			contract_clear_failure_shape
			[ "$l_sweep_staged" -eq 0 ] || return 0
			contract_check_failed_run "$l_sweep_label" \
				"$l_sweep_tool call $l_sweep_index/$l_sweep_calls ($l_sweep_shape): $l_sweep_argv" \
				"FAIL $l_sweep_call $l_sweep_tool $l_sweep_argv"
		done
	done

	l_sweep_elapsed=$(($(date '+%s') - l_sweep_started))
	printf 'contract sweep: %s %s: %s calls (%s mutating), %s runs, %s absorbed, %s stopped, %ss\n' \
		"$l_sweep_label" "$l_sweep_tool" "$l_sweep_calls" "$l_sweep_mutating" \
		"$CONTRACT_RUNS" "$CONTRACT_ABSORBED" "$CONTRACT_STOPPED" "$l_sweep_elapsed"
}

# Purpose: Print the failure shapes this mode injects for a tool.
# Usage: contract_failure_shapes <zfs|ssh>
contract_failure_shapes() {
	if [ "${ZXFER_FAILURE_SWEEP:-}" = full ]; then
		printf '%s\n' default "busy_$1"
	else
		printf '%s\n' default
	fi
}

# Purpose: Export the MOCK_FAIL_STDERR/MOCK_FAIL_STATUS pair of one failure
# shape; "default" keeps the mock's defaults (an I/O error with status 1
# for zfs, a closed connection with status 255 for ssh).
# Usage: contract_apply_failure_shape <shape>; undo it with
# contract_clear_failure_shape.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the zfs message
contract_apply_failure_shape() {
	case $1 in
	busy_zfs)
		MOCK_FAIL_STDERR="cannot open 'dataset': dataset is busy"
		MOCK_FAIL_STATUS=2
		export MOCK_FAIL_STDERR MOCK_FAIL_STATUS
		;;
	busy_ssh)
		MOCK_FAIL_STDERR=""
		MOCK_FAIL_STATUS=255
		export MOCK_FAIL_STDERR MOCK_FAIL_STATUS
		;;
	esac
}

# Purpose: Drop the failure-shape overrides.
# Usage: contract_clear_failure_shape
contract_clear_failure_shape() {
	unset MOCK_FAIL_STDERR MOCK_FAIL_STATUS
}

# Purpose: Print an argv as an sh glob that matches only itself: *, ? and [
# become one-character bracket expressions. A backslash has no portable
# literal form, so it is refused.
# Usage: contract_glob_literal <argv>; returns 1 for an argv with a backslash.
contract_glob_literal() {
	case $1 in
	*\\*) return 1 ;;
	esac
	printf '%s\n' "$1" | sed 's/[*?[]/[&]/g'
}

# Purpose: Hold one failing run to the contract in the header and count it.
# Usage: contract_check_failed_run <label> <description> <expected-fail-line>;
# fails the case with the run's logs unless the run was absorbed or stopped.
contract_check_failed_run() {
	contract_classify_run "$3"
	case $CONTRACT_VERDICT in
	absorbed)
		CONTRACT_ABSORBED=$((CONTRACT_ABSORBED + 1))
		;;
	stopped)
		CONTRACT_STOPPED=$((CONTRACT_STOPPED + 1))
		;;
	*)
		fail "$CONTRACT_VERDICT_DETAIL
[$1] $2: status $CONTRACT_STATUS
zfs log:
$(cat "$RUN_DIR/zfs.log")
stderr (last 25 lines):
$(tail -n 25 "$RUN_DIR/stderr")"
		;;
	esac
	[ -z "${ZXFER_FAILURE_SWEEP_TRACE:-}" ] ||
		printf '%s\t%s\t%s\t%s\n' "$1" "$CONTRACT_VERDICT" \
			"$CONTRACT_STATUS" "$2" >>"$ZXFER_FAILURE_SWEEP_TRACE"
}

# Purpose: Classify one failing run against the contract in the header.
# Usage: contract_classify_run <expected-fail-line>; reads RUN_DIR,
# CONTRACT_STATUS, CONTRACT_CLEAN_DIR, CONTRACT_SORTED, CONTRACT_SIBLINGS
# and CONTRACT_BACKUP_CHECK. Sets CONTRACT_VERDICT to absorbed or stopped,
# or to hung, missed, report, fail-open or leftovers with the reason in
# CONTRACT_VERDICT_DETAIL. The expected line is the FAIL line of the
# intended call; ssh socket paths (-S) differ per run and are masked.
contract_classify_run() {
	l_classify_log="$RUN_DIR/zfs.log"
	CONTRACT_VERDICT_DETAIL=""

	if [ -f "$RUN_DIR/timed_out" ]; then
		CONTRACT_VERDICT=hung
		CONTRACT_VERDICT_DETAIL="zxfer hung after the injected failure and was stopped after ${CONTRACT_RUN_TIMEOUT}s."
		return 0
	fi
	l_classify_fail_at=$(awk '/^FAIL / { print NR; exit }' "$l_classify_log")
	l_classify_fail_line=$(grep '^FAIL ' "$l_classify_log" | contract_mask_sockets)
	if [ -z "$l_classify_fail_at" ] ||
		[ "$l_classify_fail_line" != "$(printf '%s\n' "$1" | contract_mask_sockets)" ]; then
		CONTRACT_VERDICT=missed
		CONTRACT_VERDICT_DETAIL="The injected failure must hit the intended call exactly once: expected $1"
		return 0
	fi

	if [ "$CONTRACT_STATUS" -eq 0 ]; then
		contract_mutations "$l_classify_log" >"$RUN_DIR/mutations"
		if [ "$CONTRACT_SORTED" -eq 1 ]; then
			sort "$RUN_DIR/mutations" >"$RUN_DIR/mutations.sorted"
			l_classify_clean="$CONTRACT_CLEAN_DIR/mutations.sorted"
			l_classify_ran="$RUN_DIR/mutations.sorted"
		else
			l_classify_clean="$CONTRACT_CLEAN_DIR/mutations"
			l_classify_ran="$RUN_DIR/mutations"
		fi
		if ! cmp -s "$l_classify_ran" "$l_classify_clean"; then
			CONTRACT_VERDICT=fail-open
			CONTRACT_VERDICT_DETAIL="FAIL-OPEN: exit 0 but the mutating calls differ from the clean run's:
$(cat "$l_classify_clean")"
			return 0
		fi
		if [ -n "$CONTRACT_BACKUP_CHECK" ] &&
			[ "$(contract_backup_digest "$RUN_DIR/backup")" != "$(cat "$CONTRACT_CLEAN_DIR/backup.digest")" ]; then
			CONTRACT_VERDICT=fail-open
			CONTRACT_VERDICT_DETAIL="FAIL-OPEN: exit 0 but the backup files differ from the clean run's:
$(contract_backup_digest "$RUN_DIR/backup")"
			return 0
		fi
		CONTRACT_VERDICT=absorbed
	else
		if ! contract_has_failure_report; then
			CONTRACT_VERDICT=report
			CONTRACT_VERDICT_DETAIL="A failed run must end with one structured runtime failure report whose exit_status is its status."
			return 0
		fi
		l_classify_late=$(contract_late_mutations "$l_classify_log" "$l_classify_fail_at")
		if [ -n "$l_classify_late" ]; then
			CONTRACT_VERDICT=fail-open
			CONTRACT_VERDICT_DETAIL="FAIL-OPEN: a mutating call started after the injected failure:
$l_classify_late"
			return 0
		fi
		l_classify_backup=$(contract_backup_digest "$RUN_DIR/backup")
		l_classify_backup_changed=0
		case $CONTRACT_BACKUP_CHECK in
		publish)
			[ -z "$l_classify_backup" ] || l_classify_backup_changed=1
			;;
		readonly)
			[ "$l_classify_backup" = "$(cat "$CONTRACT_CLEAN_DIR/backup.digest")" ] ||
				l_classify_backup_changed=1
			;;
		esac
		if [ "$l_classify_backup_changed" -eq 1 ]; then
			CONTRACT_VERDICT=fail-open
			CONTRACT_VERDICT_DETAIL="FAIL-OPEN: the stopped run changed backup files:
$l_classify_backup"
			return 0
		fi
		CONTRACT_VERDICT=stopped
	fi

	l_classify_leftovers=$(ls -A "$RUN_DIR/tmp" 2>/dev/null)
	if [ -n "$l_classify_leftovers" ]; then
		CONTRACT_VERDICT=leftovers
		CONTRACT_VERDICT_DETAIL="The run must leave its TMPDIR empty: $l_classify_leftovers"
	fi
}

# Purpose: Mask the per-run ssh control socket paths (-S operands) so a FAIL
# line compares with the clean call's argv.
# Usage: ... | contract_mask_sockets
contract_mask_sockets() {
	sed 's|-S [^ ]*|-S SOCKET|g'
}

# Purpose: Return 0 when RUN_DIR/stderr holds exactly one structured runtime
# failure report whose exit_status is CONTRACT_STATUS.
# Usage: contract_has_failure_report
contract_has_failure_report() {
	l_report_err="$RUN_DIR/stderr"
	[ "$(grep -c '^zxfer: failure report begin$' "$l_report_err")" -eq 1 ] &&
		[ "$(grep -c '^zxfer: failure report end$' "$l_report_err")" -eq 1 ] &&
		grep -qx "exit_status: $CONTRACT_STATUS" "$l_report_err" &&
		grep -qx 'failure_class: runtime' "$l_report_err" &&
		grep -q '^failure_stage: .' "$l_report_err" &&
		grep -q '^message: .' "$l_report_err"
}

# Purpose: Print the mutating calls that start after the injected failure,
# minus the pipeline partner and -j sibling starts the header allows.
# Usage: contract_late_mutations <zfs-log> <fail-line-number>
contract_late_mutations() {
	awk -v fail_at="$2" -v source_root="$ZXFER_MOCKBIN_SOURCE_ROOT" \
		-v dest_root="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		-v siblings="$CONTRACT_SIBLINGS" '
		# Drop the shell quoting a rendered remote command adds.
		function unquote(word) {
			gsub(quote, "", word)
			gsub(/\\/, "", word)
			return word
		}
		# Map a source dataset (or snapshot) to its destination dataset.
		function destination_of(name) {
			sub(/@.*/, "", name)
			if (name != source_root && index(name, source_root "/") != 1)
				return ""
			return dest_root substr(name, length(source_root) + 1)
		}
		BEGIN {
			quote = sprintf("%c", 39)
		}
		NR == fail_at {
			n = split(siblings, sibling, " ")
			for (i = 4; i <= NF; i++) {
				word = unquote($i)
				if (word == "send")
					is_send = 1
				for (s = 1; s <= n; s++) {
					sibling_dest = destination_of(sibling[s])
					if (word == sibling_dest || destination_of(word) == sibling_dest)
						touched[s] = 1
				}
			}
			# The pipeline partner of a failed send.
			if (is_send) {
				partner = destination_of(unquote($NF))
				if (partner != "")
					allowed[partner] = 1
			}
			# Every other sibling of a -j child whose call failed.
			for (s = 1; s <= n; s++)
				if (s in touched)
					for (t = 1; t <= n; t++)
						if (!(t in touched))
							allowed[destination_of(sibling[t])] = 1
			next
		}
		NR > fail_at && /^(MUTATE |receive |recv |unmount |mount )/ {
			if (($1 == "receive" || $1 == "recv") && ($NF in allowed)) {
				delete allowed[$NF]
				next
			}
			print
		}
	' "$1"
}

# ---------------------------------------------------------------------------
# Self-tests: the classifier must catch each kind of fail-open, so a sweep
# that passes means something.

# Purpose: Stage a synthetic failing run and its clean run for the
# classifier: RUN_DIR with the given status, zfs log and stderr, and
# CONTRACT_CLEAN_DIR with the clean log's mutations.
# Usage: contract_fake_runs <status> <run-zfs-log> <run-stderr> <clean-zfs-log>
contract_fake_runs() {
	RUN_DIR="$CASE_DIR/run"
	CONTRACT_CLEAN_DIR="$CASE_DIR/clean"
	rm -rf "$RUN_DIR" "$CONTRACT_CLEAN_DIR"
	mkdir -p "$RUN_DIR/tmp" "$CONTRACT_CLEAN_DIR" ||
		fail "Unable to create the synthetic run directories."
	printf '%s\n' "$2" >"$RUN_DIR/zfs.log"
	printf '%s\n' "$3" >"$RUN_DIR/stderr"
	printf '%s\n' "$4" >"$CONTRACT_CLEAN_DIR/zfs.log"
	contract_mutations "$CONTRACT_CLEAN_DIR/zfs.log" >"$CONTRACT_CLEAN_DIR/mutations"
	sort "$CONTRACT_CLEAN_DIR/mutations" >"$CONTRACT_CLEAN_DIR/mutations.sorted"
	: >"$CONTRACT_CLEAN_DIR/backup.digest"
	CONTRACT_STATUS=$1
}

# Purpose: Print a structured failure report for a status.
# Usage: contract_fake_report <status> [failure-class]
contract_fake_report() {
	printf '%s\n' "zxfer: failure report begin" "exit_status: $1" \
		"failure_class: ${2:-runtime}" "failure_stage: send/receive" \
		"message: injected" "zxfer: failure report end"
}

# Purpose: Classify the staged run and assert its verdict.
# Usage: contract_assert_verdict <message> <expected-verdict> <expected-fail-line>
contract_assert_verdict() {
	contract_classify_run "$3"
	assertEquals "$1 ($CONTRACT_VERDICT_DETAIL)" "$2" "$CONTRACT_VERDICT"
}

test_classifier_holds_exit_zero_runs_to_the_clean_mutations() {
	contract_setup
	l_clean="list -H srcpool/data
MUTATE destroy dstpool/back/data@snap9
receive dstpool/back/data
END receive dstpool/back/data"

	contract_fake_runs 0 "FAIL 1 zfs list -H srcpool/data
list -H srcpool/data
MUTATE destroy dstpool/back/data@snap9
receive dstpool/back/data
END receive dstpool/back/data" "" "$l_clean"
	contract_assert_verdict "a failed read retried with the same mutations is absorbed" \
		absorbed "FAIL 1 zfs list -H srcpool/data"

	contract_fake_runs 0 "list -H srcpool/data
FAIL 1 zfs destroy dstpool/back/data@snap9
receive dstpool/back/data
END receive dstpool/back/data" "" "$l_clean"
	contract_assert_verdict "exit 0 after a failed destroy is a fail-open" \
		fail-open "FAIL 1 zfs destroy dstpool/back/data@snap9"

	contract_fake_runs 0 "FAIL 1 zfs list -H srcpool/data
receive dstpool/back/data
MUTATE destroy dstpool/back/data@snap9" "" "$l_clean"
	contract_assert_verdict "reordered mutations are a fail-open outside -j" \
		fail-open "FAIL 1 zfs list -H srcpool/data"
	CONTRACT_SORTED=1
	contract_assert_verdict "-j compares the mutations as a sorted list" \
		absorbed "FAIL 1 zfs list -H srcpool/data"
}

test_classifier_holds_failed_runs_to_the_report_and_no_later_mutation() {
	contract_setup
	l_clean="list -H srcpool/data
MUTATE destroy dstpool/back/data@snap9"
	l_expected="FAIL 1 zfs list -H srcpool/data"

	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data" \
		"$(contract_fake_report 1)" "$l_clean"
	contract_assert_verdict "a failed read that stops the run is stopped" \
		stopped "$l_expected"

	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data
MUTATE destroy dstpool/back/data@snap9" "$(contract_fake_report 1)" "$l_clean"
	contract_assert_verdict "a destroy after the failure is a fail-open" \
		fail-open "$l_expected"

	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data" "" "$l_clean"
	contract_assert_verdict "a failed run without a report breaks the contract" \
		report "$l_expected"
	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data" \
		"$(contract_fake_report 2)" "$l_clean"
	contract_assert_verdict "the report must carry the run's exit status" \
		report "$l_expected"
	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data" \
		"$(contract_fake_report 1 usage)" "$l_clean"
	contract_assert_verdict "an injected zfs failure is a runtime failure" \
		report "$l_expected"
	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data" \
		"$(contract_fake_report 1)
$(contract_fake_report 1)" "$l_clean"
	contract_assert_verdict "the report must be printed once" \
		report "$l_expected"

	contract_fake_runs 1 "FAIL 1 zfs list -H srcpool/data" \
		"$(contract_fake_report 1)" "$l_clean"
	: >"$RUN_DIR/tmp/zxfer-temp.1"
	contract_assert_verdict "a run must leave its TMPDIR empty" \
		leftovers "$l_expected"
}

test_classifier_requires_the_intended_failure_exactly_once() {
	contract_setup
	contract_fake_runs 1 "list -H srcpool/data" "$(contract_fake_report 1)" ""
	contract_assert_verdict "a run whose failure never fired proves nothing" \
		missed "FAIL 1 zfs list -H srcpool/data"
	contract_fake_runs 1 "FAIL 1 zfs list -H dstpool/back" \
		"$(contract_fake_report 1)" ""
	contract_assert_verdict "a failure of another call proves nothing" \
		missed "FAIL 1 zfs list -H srcpool/data"
	contract_fake_runs 1 "FAIL 1 ssh -S /tmp/zxfer.ssh.abc/ssh-origin.sock localhost true" \
		"$(contract_fake_report 1)" ""
	contract_assert_verdict "ssh socket paths differ per run and are masked" \
		stopped "FAIL 1 ssh -S /tmp/zxfer.ssh.xyz/ssh-origin.sock localhost true"
	: >"$RUN_DIR/timed_out"
	contract_assert_verdict "a run the watchdog stopped is a hang" \
		hung "FAIL 1 ssh -S /tmp/zxfer.ssh.xyz/ssh-origin.sock localhost true"
}

test_classifier_allows_only_the_partner_and_sibling_receives_to_start_late() {
	contract_setup
	l_send="send -I srcpool/data/child1@snap2 srcpool/data/child1@snap3"

	contract_fake_runs 1 "FAIL 1 zfs $l_send
receive dstpool/back/data/child1" "$(contract_fake_report 1)" ""
	contract_assert_verdict "a failed send's own receive may start late" \
		stopped "FAIL 1 zfs $l_send"
	contract_fake_runs 1 "FAIL 1 ssh -S /tmp/s localhost '/x/zfs' 'send' '-I' 'srcpool/data/child1@snap2' 'srcpool/data/child1@snap3'
receive dstpool/back/data/child1" "$(contract_fake_report 1)" ""
	contract_assert_verdict "so may the receive of a send run over -O" \
		stopped "FAIL 1 ssh -S /tmp/s localhost '/x/zfs' 'send' '-I' 'srcpool/data/child1@snap2' 'srcpool/data/child1@snap3'"
	contract_fake_runs 1 "FAIL 1 zfs $l_send
receive dstpool/back/data/child1
receive -F dstpool/back/data/child1" "$(contract_fake_report 1)" ""
	contract_assert_verdict "but only once" fail-open "FAIL 1 zfs $l_send"
	contract_fake_runs 1 "FAIL 1 zfs $l_send
receive dstpool/back/data/child2" "$(contract_fake_report 1)" ""
	contract_assert_verdict "another dataset's receive may not start late" \
		fail-open "FAIL 1 zfs $l_send"

	CONTRACT_SIBLINGS="srcpool/data/child1 srcpool/data/child2"
	contract_fake_runs 1 "FAIL 1 zfs receive dstpool/back/data/child1
receive dstpool/back/data/child2" "$(contract_fake_report 1)" ""
	contract_assert_verdict "a -j sibling's receive may start late" \
		stopped "FAIL 1 zfs receive dstpool/back/data/child1"
	contract_fake_runs 1 "FAIL 1 zfs receive dstpool/back/data/child1
receive dstpool/back/data" "$(contract_fake_report 1)" ""
	contract_assert_verdict "a receive that is no sibling may not" \
		fail-open "FAIL 1 zfs receive dstpool/back/data/child1"
	contract_fake_runs 1 "FAIL 1 zfs receive dstpool/back/data
receive dstpool/back/data/child2" "$(contract_fake_report 1)" ""
	contract_assert_verdict "a failed parent receive allows no child receive" \
		fail-open "FAIL 1 zfs receive dstpool/back/data"
	contract_fake_runs 1 "FAIL 1 zfs receive dstpool/back/data/child1
MUTATE destroy dstpool/back/data/child2@snap9" "$(contract_fake_report 1)" ""
	contract_assert_verdict "siblings excuse receives only" \
		fail-open "FAIL 1 zfs receive dstpool/back/data/child1"
}

# End to end: a launcher that ignores a failed destroy must be caught
# through the real mock injection, run and classification.
test_sweep_catches_a_launcher_that_ignores_zfs_failures() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/noop" fail_open_launcher
	CONTRACT_ZXFER_BIN="$CASE_DIR/fail_open_zxfer"
	cat >"$CONTRACT_ZXFER_BIN" <<'EOF'
#!/bin/sh
zfs list -Hr -o name,guid -t snapshot srcpool/data >/dev/null 2>&1
zfs destroy dstpool/back/data@snap9 2>/dev/null
zfs list -H dstpool/back/data >/dev/null 2>&1 || exit 1
exit 0
EOF
	chmod +x "$CONTRACT_ZXFER_BIN"

	contract_run zfs 0 ""
	assertEquals "the fake launcher's clean run should succeed" 0 "$CONTRACT_STATUS"
	mv "$RUN_DIR" "$CASE_DIR/clean" || fail "Unable to keep the clean run."
	CONTRACT_CLEAN_DIR="$CASE_DIR/clean"
	RUN_DIR="$CASE_DIR/run"
	contract_mutations "$CONTRACT_CLEAN_DIR/zfs.log" >"$CONTRACT_CLEAN_DIR/mutations"
	: >"$CONTRACT_CLEAN_DIR/backup.digest"

	contract_run zfs 1 "list -Hr *"
	contract_assert_verdict "an ignored failed read with the same mutations is absorbed" \
		absorbed "FAIL 1 zfs list -Hr -o name,guid -t snapshot srcpool/data"
	contract_run zfs 1 "destroy *"
	contract_assert_verdict "an ignored failed destroy must be caught" \
		fail-open "FAIL 1 zfs destroy dstpool/back/data@snap9"
	contract_run zfs 1 "list -H dstpool/back/data"
	contract_assert_verdict "a failure without a structured report must be caught" \
		report "FAIL 1 zfs list -H dstpool/back/data"
}

# ---------------------------------------------------------------------------
# Scenarios.

# Local recursive incremental: one send and receive per dataset.
test_local_incremental_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" local_incremental
	contract_sweep local_incremental zfs -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -d: the destination-only root snapshot is destroyed.
test_delete_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/noop" delete
	planning_add_extra_destination_snapshot
	contract_sweep delete zfs -d -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -d -F with a diverged child: destroy, rollback and a forced resend.
test_diverged_child_convergence_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/noop" diverged_child
	contract_make_child_diverged_until_receive
	contract_sweep diverged_child zfs -d -F -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -d -g: the grandfather pre-pass allows destroying child2's young
# destination-only snapshot (-g 36500 days), then every dataset is sent.
test_grandfather_delete_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" grandfather_delete
	contract_add_extra_child_snapshot 2
	contract_sweep grandfather_delete zfs -d -g 36500 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -P: the root's differing compression is set and the children, whose
# source inherits it, are inherited.
test_property_transfer_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/noop" property_transfer
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_rows" \
		"$(planning_property_rows_with "$l_rows" compression lz4 "inherited from $ZXFER_MOCKBIN_SOURCE_ROOT")" \
		"$(planning_property_rows_with "$l_rows" compression gzip local)" "$l_rows"
	contract_sweep property_transfer zfs -P -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -k records the properties, then -e restores the recorded value over a
# drifted source and destination.
test_backup_then_restore_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/noop" backup_then_restore
	planning_add_property_transfer_fixtures
	CONTRACT_BACKUP_EMPTY=1
	CONTRACT_BACKUP_CHECK="publish"
	contract_sweep backup zfs -k -P -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"

	# The clean -k run's backup root is the -e template.
	CONTRACT_BACKUP_TEMPLATE="$CASE_DIR/backup_template"
	cp -Rp "$CONTRACT_CLEAN_DIR/backup" "$CONTRACT_BACKUP_TEMPLATE" ||
		fail "Unable to keep the clean -k backup."
	CONTRACT_BACKUP_EMPTY=0
	CONTRACT_BACKUP_CHECK="readonly"
	l_drifted_rows=$(planning_property_rows_with "$(planning_property_default_rows)" \
		compression gzip local)
	planning_add_property_fixtures_for_rows "$l_drifted_rows" "$l_drifted_rows" \
		"$l_drifted_rows" "$l_drifted_rows"
	contract_sweep restore zfs -e -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -j 2: parallel source discovery and concurrent child receives.
test_parallel_jobs_fail_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" parallel_jobs
	planning_add_parallel_source_discovery_fixtures
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."
	CONTRACT_SIBLINGS="$ZXFER_MOCKBIN_SOURCE_ROOT/child1 $ZXFER_MOCKBIN_SOURCE_ROOT/child2"
	CONTRACT_SORTED=1
	contract_sweep parallel_jobs zfs -j 2 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -O localhost: a pull through the mock ssh, failing each zfs call.
test_remote_origin_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" remote_origin_zfs
	contract_sweep remote_origin zfs -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -O localhost, failing each ssh call.
test_remote_origin_fails_closed_at_every_ssh_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" remote_origin_ssh
	contract_sweep remote_origin ssh -O localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -T localhost: a push through the mock ssh, failing each zfs call.
test_remote_target_fails_closed_at_every_zfs_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" remote_target_zfs
	contract_sweep remote_target zfs -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

# -T localhost, failing each ssh call.
test_remote_target_fails_closed_at_every_ssh_call() {
	contract_setup
	planning_clone_state "$FIXTURE_DIR/incremental" remote_target_ssh
	contract_sweep remote_target ssh -T localhost -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
}

. "$SHUNIT2_BIN"
