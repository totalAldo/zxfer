#!/bin/sh
#
# shunit2 self-test for tests/mock_toolchain_helper.sh.
#
# Proves the mock-toolchain foundation works end-to-end by driving the real
# ./zxfer launcher black-box with the canned zfs:
#   scenario (a) local recursive no-op replication completes with exit 0 and
#                zero mutating zfs commands;
#   scenario (b) destination-missing-last-snapshot with -n issues zero zfs
#                commands (current dry-run contract), and without -n runs the
#                full send|receive pipeline against the canned zfs.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/mock_toolchain_helper.sh
. "$TESTS_DIR/mock_toolchain_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_mock_toolchain"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	unset MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS \
		MOCK_ZFS_STRICT_RECEIVE MOCK_SPAWN_LOG MOCK_SSH_LOG MOCK_FAIL_TOOL \
		MOCK_FAIL_CALL MOCK_FAIL_MATCH MOCK_FAIL_DIR MOCK_FAIL_STDERR \
		MOCK_FAIL_STATUS
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
# bin dir, fixture tree with 2 child datasets x 3 snapshots, secure PATH.
mocktest_setup_zxfer_env() {
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
mocktest_run_zxfer() {
	l_state_dir=$1
	shift

	zxfer_mockbin_run_zxfer "$MOCKBIN_DIR" "$l_state_dir" "$ZFS_LOG" "$@" \
		>"$CASE_DIR/zxfer.stdout" 2>"$CASE_DIR/zxfer.stderr"
}

mocktest_assert_log_has_line() {
	l_expected_line=$1

	grep -Fx "$l_expected_line" "$ZFS_LOG" >/dev/null 2>&1 ||
		fail "Expected zfs log line missing: $l_expected_line
zfs log: $(cat "$ZFS_LOG" 2>/dev/null)"
}

# Print the call numbers claimed in a MOCK_FAIL_DIR, one per line, sorted.
mocktest_claimed_numbers() {
	for l_claimed in "$1"/*; do
		[ -f "$l_claimed" ] && printf '%s\n' "${l_claimed##*/}"
	done | sort -n
}

mocktest_assert_no_mutations() {
	if grep -q '^MUTATE ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero MUTATE lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

test_prepare_dir_symlinks_real_tools() {
	zxfer_mockbin_prepare_dir "$CASE_DIR/bin" awk sed sort
	assertEquals "prepare_dir should succeed for real host tools" 0 $?

	for l_tool in awk sed sort; do
		assertTrue "expected symlink for $l_tool" "[ -h '$CASE_DIR/bin/$l_tool' ]"
		assertTrue "expected executable $l_tool" "[ -x '$CASE_DIR/bin/$l_tool' ]"
	done

	l_output=$("$CASE_DIR/bin/awk" 'BEGIN { print "ok" }')
	assertEquals "symlinked awk should run" "ok" "$l_output"

	# Re-running against the same directory must replace entries, not fail.
	zxfer_mockbin_prepare_dir "$CASE_DIR/bin" awk
	assertEquals "prepare_dir should be idempotent" 0 $?
}

test_prepare_dir_rejects_missing_tool() {
	zxfer_test_capture_subshell \
		"zxfer_mockbin_prepare_dir '$CASE_DIR/bin' zxfer_no_such_tool_xyz"
	assertEquals "missing tool should fail" 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertTrue "missing tool should be named in the error" \
		"printf '%s\n' \"\$ZXFER_TEST_CAPTURE_OUTPUT\" | grep -q 'zxfer_no_such_tool_xyz'"
}

test_secure_path_env_lists_mockdir_first() {
	l_secure_path=$(zxfer_mockbin_secure_path_env "$CASE_DIR/mockbin")
	assertEquals "secure path should lead with the mock dir" \
		"$CASE_DIR/mockbin:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin" \
		"$l_secure_path"
}

test_counting_wrapper_logs_and_execs_real_tool() {
	l_real_awk=$(zxfer_mockbin_resolve_host_tool awk) ||
		fail "Host awk not found."
	zxfer_mockbin_write_counting_wrapper "$CASE_DIR/awk" "$l_real_awk"
	assertEquals "counting wrapper write should succeed" 0 $?

	l_output=$(MOCK_SPAWN_LOG="$CASE_DIR/spawn.log" \
		"$CASE_DIR/awk" 'BEGIN { print 41 + 1 }')
	assertEquals "wrapper must exec the real tool" "42" "$l_output"
	MOCK_SPAWN_LOG="$CASE_DIR/spawn.log" \
		"$CASE_DIR/awk" 'BEGIN { exit 0 }' </dev/null
	assertEquals "two spawns should be counted" 2 \
		"$(grep -cx awk "$CASE_DIR/spawn.log")"

	# Unset spawn log must not break execution.
	l_output=$("$CASE_DIR/awk" 'BEGIN { print "quiet" }')
	assertEquals "wrapper should run without MOCK_SPAWN_LOG" "quiet" "$l_output"
}

test_counting_wrapper_rejects_relative_real_tool() {
	zxfer_test_capture_subshell \
		"zxfer_mockbin_write_counting_wrapper '$CASE_DIR/awk' awk"
	assertEquals "relative real-tool path should fail" 1 \
		"$ZXFER_TEST_CAPTURE_STATUS"
}

test_canned_zfs_answers_read_only_commands_from_manifest() {
	mkdir -p "$CASE_DIR/fix"
	printf 'compression-value\n' >"$CASE_DIR/fix/get.out"
	{
		printf 'get -H -o value compression tank\tget.out\t0\n'
		printf 'list -H tank*\t-\t3\n'
	} >"$CASE_DIR/fix/manifest"
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"

	l_output=$(MOCK_ZFS_LOG="$CASE_DIR/zfs.log" \
		MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" \
		"$CASE_DIR/zfs" get -H -o value compression tank)
	l_status=$?
	assertEquals "exact manifest match should exit 0" 0 "$l_status"
	assertEquals "fixture bytes should be emitted" "compression-value" "$l_output"

	MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" \
		"$CASE_DIR/zfs" list -H tank/foo >/dev/null 2>&1
	assertEquals "glob manifest match should use rule status" 3 $?

	MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" \
		"$CASE_DIR/zfs" list -H other >/dev/null 2>"$CASE_DIR/unmatched.err"
	assertEquals "unmatched read-only command should exit 1 by default" 1 $?
	assertTrue "unmatched command should leave a stderr note" \
		"grep -q 'no manifest match for: list -H other' '$CASE_DIR/unmatched.err'"

	MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" \
		MOCK_ZFS_DEFAULT_STATUS=7 \
		"$CASE_DIR/zfs" list -H other >/dev/null 2>&1
	assertEquals "MOCK_ZFS_DEFAULT_STATUS should override the fallback" 7 $?

	assertTrue "argv log should record the exact invocation" \
		"grep -Fxq 'get -H -o value compression tank' '$CASE_DIR/zfs.log'"
}

# Pin the consumable-rule extension: a rule whose 4th field is "once" answers
# exactly one matching lookup, is dropped from the manifest, and later lookups
# for the same argv fall through to the next matching rule. This is what lets
# a stateless canned zfs answer the same listing differently before and after
# a receive (e.g. diverged guids healed by convergence).
test_canned_zfs_consumes_once_manifest_rules() {
	mkdir -p "$CASE_DIR/fix"
	printf 'first-answer\n' >"$CASE_DIR/fix/first.out"
	printf 'second-answer\n' >"$CASE_DIR/fix/second.out"
	{
		printf 'list -H tank\tfirst.out\t0\tonce\n'
		printf 'list -H tank\tsecond.out\t0\n'
	} >"$CASE_DIR/fix/manifest"
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"

	l_output=$(MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" "$CASE_DIR/zfs" list -H tank)
	assertEquals "the consumable rule should answer its first lookup" \
		"first-answer" "$l_output"
	assertFalse "the consumed rule must be dropped from the manifest" \
		"grep -q 'first.out' '$CASE_DIR/fix/manifest'"

	l_output=$(MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" "$CASE_DIR/zfs" list -H tank)
	assertEquals "the next lookup should fall through to the later rule" \
		"second-answer" "$l_output"
	l_output=$(MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" "$CASE_DIR/zfs" list -H tank)
	assertEquals "rules without the once flag stay permanent" \
		"second-answer" "$l_output"
}

test_canned_zfs_flags_mutating_commands() {
	mkdir -p "$CASE_DIR/fix"
	printf 'destroy tank@locked\t-\t1\n' >"$CASE_DIR/fix/manifest"
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"

	MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" \
		"$CASE_DIR/zfs" destroy tank@old
	assertEquals "unmatched mutating command should exit 0" 0 $?
	assertTrue "mutating command should be logged with MUTATE prefix" \
		"grep -Fxq 'MUTATE destroy tank@old' '$CASE_DIR/zfs.log'"

	MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_ZFS_FIXTURE_DIR="$CASE_DIR/fix" \
		"$CASE_DIR/zfs" destroy tank@locked
	assertEquals "manifest can force a mutating command to fail" 1 $?
}

test_canned_zfs_send_receive_defaults() {
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"

	l_output=$(MOCK_ZFS_LOG="$CASE_DIR/zfs.log" \
		"$CASE_DIR/zfs" send -I tank@1 tank@2)
	l_status=$?
	assertEquals "unmatched send should exit 0" 0 "$l_status"
	assertEquals "unmatched send should emit a dummy stream" \
		"ZXFERMOCKSTREAM send -I tank@1 tank@2" "$l_output"

	l_output=$(printf 'streambytes' | MOCK_ZFS_LOG="$CASE_DIR/zfs.log" \
		"$CASE_DIR/zfs" receive -F tank/dst)
	l_status=$?
	assertEquals "receive should consume stdin and exit 0" 0 "$l_status"
	assertEquals "receive should emit nothing by default" "" "$l_output"
	assertEquals "receive should be logged without MUTATE prefix and end with an END line naming its dataset" \
		"send -I tank@1 tank@2
receive -F tank/dst
END receive tank/dst" "$(cat "$CASE_DIR/zfs.log")"
}

# Pin the strict receive: MOCK_ZFS_STRICT_RECEIVE=1 refuses an empty stream
# the way zfs receive does, logging the start but no END line, and still
# accepts a real stream.
test_canned_zfs_strict_receive_refuses_an_empty_stream() {
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"
	MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_ZFS_STRICT_RECEIVE=1 \
		"$CASE_DIR/zfs" receive tank/empty </dev/null 2>"$CASE_DIR/strict.err"
	assertEquals "an empty stream should fail under the strict receive" 1 $?
	assertEquals "the refusal should use the zfs receive wording" \
		"cannot receive: failed to read from stream" "$(cat "$CASE_DIR/strict.err")"
	printf 'streambytes' | MOCK_ZFS_LOG="$CASE_DIR/zfs.log" \
		MOCK_ZFS_STRICT_RECEIVE=1 "$CASE_DIR/zfs" receive tank/full
	assertEquals "a stream, even without a final newline, should be received" 0 $?
	assertEquals "a refused receive logs its start but no END line" \
		"receive tank/empty
receive tank/full
END receive tank/full" "$(cat "$CASE_DIR/zfs.log")"
}

# Pin MOCK_FAIL_CALL on the canned zfs: calls are numbered from 1 in the
# counter directory (each file holds its argv), only the Nth fails, with the
# default operational error on its last operand and status 1, and the log
# shows "FAIL <n> zfs <argv>" in its place, even for a mutating call.
test_canned_zfs_fails_the_nth_call_with_an_operational_error() {
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"
	mkdir "$CASE_DIR/calls" || fail "Unable to create the counter directory."
	MOCK_ZFS_LOG="$CASE_DIR/zfs.log"
	MOCK_FAIL_DIR="$CASE_DIR/calls"
	MOCK_FAIL_CALL=2
	export MOCK_ZFS_LOG MOCK_FAIL_DIR MOCK_FAIL_CALL

	"$CASE_DIR/zfs" list -H tank >/dev/null 2>&1
	"$CASE_DIR/zfs" destroy tank@old 2>"$CASE_DIR/fail.err"
	l_status=$?
	"$CASE_DIR/zfs" destroy tank@older 2>/dev/null
	l_after_status=$?
	unset MOCK_ZFS_LOG MOCK_FAIL_DIR MOCK_FAIL_CALL

	assertEquals "the second call should fail with status 1" 1 "$l_status"
	assertEquals "the failure should be an operational error on the last operand" \
		"cannot open 'tank@old': I/O error" "$(cat "$CASE_DIR/fail.err")"
	assertEquals "later calls should answer normally" 0 "$l_after_status"
	assertEquals "the failing call replaces its log line with a FAIL line" \
		"list -H tank
FAIL 2 zfs destroy tank@old
MUTATE destroy tank@older" "$(cat "$CASE_DIR/zfs.log")"
	assertEquals "each counted call claims one numbered file holding its argv" \
		"list -H tank|destroy tank@old|destroy tank@older" \
		"$(cat "$CASE_DIR/calls/1" "$CASE_DIR/calls/2" "$CASE_DIR/calls/3" | tr '\n' '|' | sed 's/|$//')"
	assertFalse "a default failure must never claim the dataset is missing" \
		"grep -q 'does not exist' '$CASE_DIR/fail.err'"
}

# Pin the other knobs: MOCK_FAIL_CALL=0 only counts, MOCK_FAIL_MATCH counts
# only matching calls, MOCK_FAIL_STDERR and MOCK_FAIL_STATUS replace the
# defaults (set but empty prints nothing), and a failing receive reads no
# stream and logs no END line.
test_canned_zfs_fault_injection_knobs() {
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"
	mkdir "$CASE_DIR/count" "$CASE_DIR/match" ||
		fail "Unable to create the counter directories."

	MOCK_FAIL_DIR="$CASE_DIR/count" MOCK_FAIL_CALL=0 \
		"$CASE_DIR/zfs" list -H a >/dev/null 2>&1
	MOCK_FAIL_DIR="$CASE_DIR/count" MOCK_FAIL_CALL=0 \
		"$CASE_DIR/zfs" send tank@1 >/dev/null
	assertEquals "MOCK_FAIL_CALL=0 should succeed" 0 $?
	assertEquals "MOCK_FAIL_CALL=0 should still number every call" "1 2" \
		"$(mocktest_claimed_numbers "$CASE_DIR/count" | tr '\n' ' ' | sed 's/ $//')"

	MOCK_ZFS_LOG="$CASE_DIR/zfs.log"
	MOCK_FAIL_DIR="$CASE_DIR/match"
	MOCK_FAIL_MATCH="receive *"
	MOCK_FAIL_CALL=2
	MOCK_FAIL_STDERR="cannot receive: custom"
	MOCK_FAIL_STATUS=9
	export MOCK_ZFS_LOG MOCK_FAIL_DIR MOCK_FAIL_MATCH MOCK_FAIL_CALL \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
	printf 'one\n' | "$CASE_DIR/zfs" receive tank/a
	"$CASE_DIR/zfs" send tank@1 >/dev/null
	printf 'two\n' | "$CASE_DIR/zfs" receive tank/b 2>"$CASE_DIR/custom.err"
	l_status=$?
	MOCK_FAIL_STDERR=""
	MOCK_FAIL_CALL=3
	printf 'three\n' | "$CASE_DIR/zfs" receive tank/c 2>"$CASE_DIR/empty.err"
	l_empty_status=$?
	unset MOCK_ZFS_LOG MOCK_FAIL_DIR MOCK_FAIL_MATCH MOCK_FAIL_CALL \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS

	assertEquals "MOCK_FAIL_STATUS should set the status" 9 "$l_status"
	assertEquals "MOCK_FAIL_STDERR should replace the text" \
		"cannot receive: custom" "$(cat "$CASE_DIR/custom.err")"
	assertEquals "an empty MOCK_FAIL_STDERR should print nothing" 9 "$l_empty_status"
	assertEquals "an empty MOCK_FAIL_STDERR should leave stderr empty" "" \
		"$(cat "$CASE_DIR/empty.err")"
	assertEquals "MOCK_FAIL_MATCH should count only the matching receives" \
		"receive tank/a|receive tank/b|receive tank/c" \
		"$(cat "$CASE_DIR/match/1" "$CASE_DIR/match/2" "$CASE_DIR/match/3" | tr '\n' '|' | sed 's/|$//')"
	assertEquals "a failing receive logs FAIL instead of its start and END lines" \
		"receive tank/a
END receive tank/a
send tank@1
FAIL 2 zfs receive tank/b
FAIL 3 zfs receive tank/c" "$(cat "$CASE_DIR/zfs.log")"
}

# The counter is shared by concurrent calls (background discovery, -j jobs),
# so numbering must never skip or reuse a number.
test_fault_injection_counter_is_atomic_under_concurrent_calls() {
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"
	mkdir "$CASE_DIR/calls" || fail "Unable to create the counter directory."

	l_index=1
	while [ "$l_index" -le 24 ]; do
		MOCK_ZFS_LOG="$CASE_DIR/zfs.log" MOCK_FAIL_DIR="$CASE_DIR/calls" \
			MOCK_FAIL_CALL=11 "$CASE_DIR/zfs" list -H "tank/ds$l_index" \
			>/dev/null 2>&1 &
		l_index=$((l_index + 1))
	done
	wait

	assertEquals "24 concurrent calls should claim exactly 1..24" \
		"$(awk 'BEGIN { for (i = 1; i <= 24; i++) print i }')" \
		"$(mocktest_claimed_numbers "$CASE_DIR/calls")"
	assertEquals "every claimed number should hold a different call" 24 \
		"$(cat "$CASE_DIR/calls"/* | sort -u | wc -l | tr -d ' ')"
	assertEquals "exactly the 11th call should fail" 1 \
		"$(grep -c '^FAIL 11 zfs list -H tank/ds' "$CASE_DIR/zfs.log")"
	assertEquals "no other call may fail" 1 "$(grep -c '^FAIL ' "$CASE_DIR/zfs.log")"
}

# A misconfigured counter must fail loudly on every counted call instead of
# silently never injecting.
test_fault_injection_refuses_a_misconfigured_counter() {
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"

	MOCK_FAIL_CALL=1 "$CASE_DIR/zfs" list -H tank 2>"$CASE_DIR/nodir.err"
	assertEquals "a missing MOCK_FAIL_DIR should exit 125" 125 $?
	assertContains "the refusal should name the missing directory" \
		"$(cat "$CASE_DIR/nodir.err")" "needs an existing MOCK_FAIL_DIR"
	mkdir "$CASE_DIR/calls" || fail "Unable to create the counter directory."
	MOCK_FAIL_DIR="$CASE_DIR/calls" MOCK_FAIL_CALL=one \
		"$CASE_DIR/zfs" list -H tank 2>"$CASE_DIR/badcall.err"
	assertEquals "a non-numeric MOCK_FAIL_CALL should exit 125" 125 $?
	assertContains "the refusal should quote the bad value" \
		"$(cat "$CASE_DIR/badcall.err")" "MOCK_FAIL_CALL must be a number: one"
	MOCK_FAIL_TOOL=ssh MOCK_FAIL_CALL=1 "$CASE_DIR/zfs" send tank@1 >/dev/null
	assertEquals "a mock that is not MOCK_FAIL_TOOL never counts or checks" 0 $?
}

# Pin MOCK_FAIL_TOOL=ssh on the socket-aware mock ssh: its Nth call exits
# 255 with the connection-closed text, runs no command, creates no socket,
# is logged as kind "fail" and as a FAIL line in MOCK_ZFS_LOG with newlines
# flattened, and zfs calls are not counted.
test_socket_ssh_fails_the_nth_call() {
	zxfer_mockbin_write_socket_ssh "$CASE_DIR/ssh"
	zxfer_mockbin_write_canned_zfs "$CASE_DIR/zfs"
	mkdir "$CASE_DIR/calls" || fail "Unable to create the counter directory."
	l_socket="$CASE_DIR/target.sock"
	MOCK_SSH_LOG="$CASE_DIR/ssh.log"
	MOCK_ZFS_LOG="$CASE_DIR/zfs.log"
	MOCK_FAIL_TOOL=ssh
	MOCK_FAIL_DIR="$CASE_DIR/calls"
	MOCK_FAIL_CALL=2
	export MOCK_SSH_LOG MOCK_ZFS_LOG MOCK_FAIL_TOOL MOCK_FAIL_DIR MOCK_FAIL_CALL

	"$CASE_DIR/ssh" -M -S "$l_socket" -fN localhost
	l_master_status=$?
	"$CASE_DIR/ssh" -S "$l_socket" localhost "$CASE_DIR/zfs list -H tank
touch '$CASE_DIR/ran'" 2>"$CASE_DIR/ssh.err"
	l_status=$?
	"$CASE_DIR/ssh" -S "$CASE_DIR/other.sock" -M -fN 127.0.0.1
	"$CASE_DIR/ssh" -S "$l_socket" localhost "$CASE_DIR/zfs" list -H tank \
		>/dev/null 2>&1
	unset MOCK_SSH_LOG MOCK_ZFS_LOG MOCK_FAIL_TOOL MOCK_FAIL_DIR MOCK_FAIL_CALL

	assertEquals "the first call (the master) should succeed" 0 "$l_master_status"
	assertEquals "the second call should fail like a dropped connection" 255 "$l_status"
	assertEquals "the failure should say the connection closed" \
		"Connection to localhost closed by remote host." "$(cat "$CASE_DIR/ssh.err")"
	assertFalse "a failing call must not run its command" "[ -e '$CASE_DIR/ran' ]"
	assertTrue "later calls should still open masters" "[ -e '$CASE_DIR/other.sock' ]"
	assertEquals "the ssh log should mark the failing call" \
		"master fail master mux " "$(cut -f1 "$CASE_DIR/ssh.log" | grep -v '^touch' | tr '\n' ' ')"
	assertEquals "the zfs log should hold the flattened FAIL line and only the later zfs call" \
		"FAIL 2 ssh -S $l_socket localhost $CASE_DIR/zfs list -H tank touch '$CASE_DIR/ran'
list -H tank" "$(cat "$CASE_DIR/zfs.log")"
	assertEquals "zfs calls are not counted when MOCK_FAIL_TOOL is ssh" 4 \
		"$(mocktest_claimed_numbers "$CASE_DIR/calls" | wc -l | tr -d ' ')"
	assertEquals "the bench rows must not count the failed call" \
		"demo	ssh_connections	2
demo	ssh_invocations	3
demo	ssh_master_opens	2" "$(zxfer_mockbin_ssh_log_rows demo "$CASE_DIR/ssh.log")"
}

# Pin MOCK_FAIL_TOOL=<name> on a counting wrapper: its Nth spawn is still
# logged but fails without running the real tool.
test_counting_wrapper_fails_the_nth_call() {
	l_real_awk=$(zxfer_mockbin_resolve_host_tool awk) ||
		fail "Host awk not found."
	zxfer_mockbin_write_counting_wrapper "$CASE_DIR/awk" "$l_real_awk" ||
		fail "Unable to write the counting wrapper."
	mkdir "$CASE_DIR/calls" || fail "Unable to create the counter directory."
	MOCK_SPAWN_LOG="$CASE_DIR/spawn.log"
	MOCK_ZFS_LOG="$CASE_DIR/zfs.log"
	MOCK_FAIL_TOOL="awk"
	MOCK_FAIL_DIR="$CASE_DIR/calls"
	MOCK_FAIL_CALL=2
	export MOCK_SPAWN_LOG MOCK_ZFS_LOG MOCK_FAIL_TOOL MOCK_FAIL_DIR MOCK_FAIL_CALL

	l_first=$("$CASE_DIR/awk" 'BEGIN { print "first" }')
	l_second=$("$CASE_DIR/awk" 'BEGIN { print "second" }' 2>"$CASE_DIR/awk.err")
	l_status=$?
	unset MOCK_SPAWN_LOG MOCK_ZFS_LOG MOCK_FAIL_TOOL MOCK_FAIL_DIR MOCK_FAIL_CALL

	assertEquals "the first call should run the real tool" "first" "$l_first"
	assertEquals "the second call should fail with status 1" 1 "$l_status"
	assertEquals "the failing call must not run the real tool" "" "$l_second"
	assertEquals "the failure should name the tool" "awk: mock failure" \
		"$(cat "$CASE_DIR/awk.err")"
	assertEquals "both spawns should still be logged" 2 \
		"$(grep -cx awk "$CASE_DIR/spawn.log")"
	assertEquals "the FAIL line should name the wrapper" \
		"FAIL 2 awk BEGIN { print \"second\" }" "$(cat "$CASE_DIR/zfs.log")"
}

# Pin the socket-aware mock ssh: masters and commands that miss a live
# control socket are new connections, commands over the socket multiplex,
# and zxfer_mockbin_ssh_log_rows turns the log into the bench rows.
test_socket_ssh_classifies_connections() {
	zxfer_mockbin_write_socket_ssh "$CASE_DIR/ssh"
	assertEquals "socket ssh write should succeed" 0 $?
	l_socket="$CASE_DIR/origin.sock"
	MOCK_SSH_LOG="$CASE_DIR/ssh.log"
	export MOCK_SSH_LOG

	"$CASE_DIR/ssh" -M -V 2>"$CASE_DIR/version.err"
	assertEquals "the -M -V probe should succeed" 0 $?
	"$CASE_DIR/ssh" -o BatchMode=yes -M -S "$l_socket" -fN localhost
	assertTrue "a master should create its socket" "[ -e '$l_socket' ]"
	l_output=$("$CASE_DIR/ssh" -o BatchMode=yes -p 2222 -l root \
		-S "$l_socket" localhost printf "'%s|'" "'a b'" c)
	assertEquals "a command should skip option values and run locally with the remote shell's word splitting" \
		"a b|c|" "$l_output"
	"$CASE_DIR/ssh" -S "$l_socket" -O check localhost
	assertEquals "-O check should succeed on a live socket" 0 $?
	"$CASE_DIR/ssh" localhost true
	"$CASE_DIR/ssh" -S "$CASE_DIR/missing.sock" localhost true
	"$CASE_DIR/ssh" -S "$l_socket" -O exit localhost
	assertFalse "-O exit should remove the socket" "[ -e '$l_socket' ]"
	"$CASE_DIR/ssh" -S "$l_socket" -O check localhost 2>"$CASE_DIR/check.err"
	assertEquals "-O check should fail once the master exited" 255 $?
	unset MOCK_SSH_LOG

	assertEquals "each call should be logged with its kind" \
		"version master mux control direct direct control control " \
		"$(cut -f1 "$CASE_DIR/ssh.log" | tr '\n' ' ')"
	assertEquals "the log rows should count masters and direct calls as connections" \
		"demo	ssh_connections	3
demo	ssh_invocations	8
demo	ssh_master_opens	1" \
		"$(zxfer_mockbin_ssh_log_rows demo "$CASE_DIR/ssh.log")"
}

test_make_bench_workdir_creates_a_private_dir_under_tmp() {
	l_workdir=$(zxfer_mockbin_make_bench_workdir zxfer_mock_toolchain_test)
	assertEquals "work directory creation should succeed" 0 $?
	case "$l_workdir" in
	/tmp/zxfer_mock_toolchain_test.??????) ;;
	*) fail "the work directory should sit directly under /tmp: $l_workdir" ;;
	esac
	assertTrue "the work directory should hold a tmp/ for zxfer" \
		"[ -d '$l_workdir/tmp' ]"
	assertEquals "the work directory should be private (mode 0700)" \
		"$l_workdir" "$(find "$l_workdir" -prune -perm 700 -print)"
	rm -rf "$l_workdir"
}

test_build_fixture_tree_layout_and_guids() {
	zxfer_mockbin_build_fixture_tree "$CASE_DIR/fixtures" 2 3
	assertEquals "fixture tree build should succeed" 0 $?

	# 3 datasets (root + 2 children) x 3 snapshots.
	assertEquals "source snapshot fixture rows" 9 \
		"$(wc -l <"$CASE_DIR/fixtures/noop/src_snapshots.list" | tr -d '[:space:]')"
	assertEquals "noop destination snapshot rows" 9 \
		"$(wc -l <"$CASE_DIR/fixtures/noop/dst_snapshots.list" | tr -d '[:space:]')"
	assertEquals "incremental destination snapshot rows" 6 \
		"$(wc -l <"$CASE_DIR/fixtures/incremental/dst_snapshots.list" | tr -d '[:space:]')"
	assertEquals "incremental depth-1 root rows" 2 \
		"$(wc -l <"$CASE_DIR/fixtures/incremental/dst_d1_0.list" | tr -d '[:space:]')"
	# 5 discovery rules + 3 per-dataset depth-1 rules.
	assertEquals "manifest rule count" 8 \
		"$(wc -l <"$CASE_DIR/fixtures/noop/manifest" | tr -d '[:space:]')"

	assertTrue "every record needs a deterministic 19-digit guid" \
		"awk -F'\t' '\$2 !~ /^1[0-9]*$/ || length(\$2) != 19 { exit 1 }' \
			'$CASE_DIR/fixtures/noop/src_snapshots.list'"
	assertTrue "incremental destination must miss the last snapshot" \
		"! grep -q '@snap3' '$CASE_DIR/fixtures/incremental/dst_snapshots.list'"

	zxfer_test_capture_subshell \
		"zxfer_mockbin_build_fixture_tree '$CASE_DIR/fixtures2' 2 1"
	assertEquals "snaps-per-dataset below 2 should be rejected" 1 \
		"$ZXFER_TEST_CAPTURE_STATUS"
	zxfer_test_capture_subshell \
		"zxfer_mockbin_build_fixture_tree '$CASE_DIR/fixtures3' x 3"
	assertEquals "non-numeric dataset count should be rejected" 1 \
		"$ZXFER_TEST_CAPTURE_STATUS"
}

test_run_zxfer_refuses_without_canned_zfs() {
	mkdir -p "$CASE_DIR/emptybin"
	zxfer_test_capture_subshell \
		"zxfer_mockbin_run_zxfer '$CASE_DIR/emptybin' '$CASE_DIR/fix' '$CASE_DIR/zfs.log' -R srcpool/data dstpool/back"
	assertEquals "runner must refuse when the canned zfs is missing" 1 \
		"$ZXFER_TEST_CAPTURE_STATUS"
	assertTrue "runner should explain the refusal" \
		"printf '%s\n' \"\$ZXFER_TEST_CAPTURE_OUTPUT\" | grep -q 'refusing to run zxfer'"
}

# Scenario (a): local recursive no-op replication, pinned end-to-end.
test_zxfer_noop_recursive_completes_without_mutation() {
	mocktest_setup_zxfer_env

	mocktest_run_zxfer "$FIXTURE_DIR/noop" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_status=$?
	assertEquals "no-op replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_status"

	assertTrue "no-op run should have invoked the canned zfs" "[ -s '$ZFS_LOG' ]"
	mocktest_assert_no_mutations
	assertFalse "no-op run should not send" "grep -q '^send ' '$ZFS_LOG'"
	assertFalse "no-op run should not receive" "grep -q '^receive ' '$ZFS_LOG'"

	# Discovery shapes the current ./zxfer issues (log order is
	# nondeterministic because discovery runs in background jobs). A clean
	# recursive no-op is proven by the fast identity proof: one sorted
	# source listing plus one sorted destination listing, with no
	# creation-order listing and no destination existence check.
	mocktest_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT"
	mocktest_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertFalse "a proven clean no-op must skip the creation-order source listing" \
		"grep -q -- '-s creation' '$ZFS_LOG'"
	assertFalse "a proven clean no-op must skip the destination existence check" \
		"grep -Fxq 'list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT' '$ZFS_LOG'"
}

# Scenario (b) with -n: the current dry-run contract is that zxfer issues
# ZERO zfs commands and renders no send/receive plan.
test_zxfer_incremental_dryrun_issues_no_zfs_commands() {
	mocktest_setup_zxfer_env

	mocktest_run_zxfer "$FIXTURE_DIR/incremental" -n -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_status=$?
	assertEquals "dry run should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_status"
	assertFalse "dry run must not invoke zfs at all" "[ -s '$ZFS_LOG' ]"
	assertFalse "dry run prints nothing to stdout without -V" \
		"[ -s '$CASE_DIR/zxfer.stdout' ]"

	# With -V the dry run explains that planning is skipped (on stderr).
	mocktest_run_zxfer "$FIXTURE_DIR/incremental" -n -V -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "very verbose dry run should exit 0" 0 $?
	assertTrue "dry run should announce skipped planning" \
		"grep -q 'Dry run: send/receive and property-reconcile commands require live snapshot discovery' '$CASE_DIR/zxfer.stderr'"
	assertFalse "very verbose dry run still must not invoke zfs" \
		"[ -s '$ZFS_LOG' ]"
}

# Scenario (b) without -n: the full send|receive pipeline completes against
# the canned zfs with one incremental per dataset.
test_zxfer_incremental_live_sends_per_dataset_increments() {
	mocktest_setup_zxfer_env

	mocktest_run_zxfer "$FIXTURE_DIR/incremental" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_status=$?
	assertEquals "incremental replication should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_status"

	mocktest_assert_no_mutations
	for l_dataset_suffix in "" /child1 /child2; do
		mocktest_assert_log_has_line \
			"send -I $ZXFER_MOCKBIN_SOURCE_ROOT$l_dataset_suffix@snap2 $ZXFER_MOCKBIN_SOURCE_ROOT$l_dataset_suffix@snap3"
		mocktest_assert_log_has_line \
			"receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_dataset_suffix"
	done
	# -R live rechecks are served from the batched recursive view listing
	# (same argv shape as discovery), so it answers from the manifest too.
	mocktest_assert_log_has_line \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	mocktest_assert_log_has_line \
		"list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT"
}

. "$SHUNIT2_BIN"
