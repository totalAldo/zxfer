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
		MOCK_SPAWN_LOG
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
	assertTrue "receive should be logged without MUTATE prefix" \
		"grep -Fxq 'receive -F tank/dst' '$CASE_DIR/zfs.log'"
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
