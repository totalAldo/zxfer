#!/bin/sh
#
# Focused shunit2 tests for the offline architecture policy checker.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_check_architecture"
	CHECK_ARCHITECTURE_SOURCE="$ZXFER_ROOT/tests/check_architecture.sh"
	EVAL_SITE_AWK_SOURCE="$ZXFER_ROOT/tests/extract_eval_sites.awk"
	GLOBAL_WRITER_AWK_SOURCE="$ZXFER_ROOT/tests/extract_global_writes.awk"
	FUNCTION_SCRATCH_AWK_SOURCE="$ZXFER_ROOT/tests/extract_function_scratch.awk"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
create_architecture_fixture() {
	l_fixture_root=$1
	shift
	mkdir -p "$l_fixture_root/src" "$l_fixture_root/tests"
	cp "$CHECK_ARCHITECTURE_SOURCE" "$l_fixture_root/tests/check_architecture.sh"
	cp "$EVAL_SITE_AWK_SOURCE" "$l_fixture_root/tests/extract_eval_sites.awk"
	cp "$GLOBAL_WRITER_AWK_SOURCE" "$l_fixture_root/tests/extract_global_writes.awk"
	cp "$FUNCTION_SCRATCH_AWK_SOURCE" "$l_fixture_root/tests/extract_function_scratch.awk"
	chmod +x "$l_fixture_root/tests/check_architecture.sh"
	printf '%s\n' '#!/bin/sh' >"$l_fixture_root/zxfer"
	{
		printf "ZXFER_SOURCE_MODULE_MANIFEST='"
		for l_fixture_module in "$@"; do
			printf '%s\n' "$l_fixture_module"
		done
		printf "'\n"
	} >"$l_fixture_root/src/zxfer_modules.sh"
	: >"$l_fixture_root/tests/function_scratch_baseline.tsv"
	: >"$l_fixture_root/tests/architecture_eval_policy.tsv"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
write_two_module_architecture_policy() {
	l_fixture_root=$1
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_new_direct_function_scratch_collision() {
	l_fixture_root="$TEST_TMPDIR/new-scratch-collision"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	l_shared=callee
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_shared=caller
	zxfer_callee
	printf '%s\n' "$l_shared"
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A new caller/callee scratch overlap should fail architecture validation. Output: $output" \
		1 "$status"
	assertContains "The diagnostic should identify the exact call edge and scratch name." \
		"$output" "zxfer_caller -> zxfer_callee both write l_shared"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_case_arm_function_scratch_collision() {
	l_fixture_root="$TEST_TMPDIR/case-arm-scratch-collision"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	l_shared=callee
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_shared=caller
	case "${1:-}" in
	x) zxfer_callee ;;
	esac
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A direct current-shell call on a case arm must participate in scratch collision validation. Output: $output" \
		1 "$status"
	assertContains "The case-arm diagnostic should identify the exact call edge and scratch name." \
		"$output" "zxfer_caller -> zxfer_callee both write l_shared"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_keeps_scanning_after_word_internal_hash() {
	l_fixture_root="$TEST_TMPDIR/word-internal-hash-scratch-collision"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	l_shared=callee
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_shared=caller
	l_trimmed=${1#prefix}; zxfer_callee
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A parameter-trim operator must not hide a later current-shell call from scratch validation. Output: $output" \
		1 "$status"
	assertContains "The collision after a word-internal hash should identify the exact call edge." \
		"$output" "zxfer_caller -> zxfer_callee both write l_shared"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_treats_read_and_loop_targets_as_scratch_writes() {
	l_fixture_root="$TEST_TMPDIR/builtin-scratch-writes"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	for l_shared in value; do
		:
	done
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	IFS= read -r l_shared <<EOF_VALUE
caller
EOF_VALUE
	zxfer_callee
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "POSIX read and loop targets should participate in the scratch collision check." \
		1 "$status"
	assertContains "The builtin-write collision should identify the shared scratch target." \
		"$output" "zxfer_caller -> zxfer_callee both write l_shared"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_accepts_exact_documented_scratch_baseline() {
	l_fixture_root="$TEST_TMPDIR/accepted-scratch-baseline"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	l_shared=callee
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_shared=caller
	zxfer_callee
}
EOF
	printf '%s\n' \
		'zxfer_caller	zxfer_callee	l_shared	pre-ratchet fixture debt' \
		>"$l_fixture_root/tests/function_scratch_baseline.tsv"

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "An exact narrow baseline tuple should preserve the architecture gate." \
		"$output" "1 baselined function-scratch overlaps"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_stale_scratch_baseline_entry() {
	l_fixture_root="$TEST_TMPDIR/stale-scratch-baseline"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	l_shared=callee
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	zxfer_callee
}
EOF
	printf '%s\n' 'zxfer_caller	zxfer_callee	l_shared' \
		>"$l_fixture_root/tests/function_scratch_baseline.tsv"

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "Resolved scratch debt should require deleting its baseline row." 1 "$status"
	assertContains "Stale-row diagnostics should identify the exact obsolete tuple." \
		"$output" "stale function-scratch baseline entry: zxfer_caller -> zxfer_callee"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_ignores_non_current_shell_call_shapes() {
	l_fixture_root="$TEST_TMPDIR/scratch-false-positive-shapes"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	l_shared=callee
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_shared=caller
	case "${1:-}" in
	zxfer_callee) : ;;
	esac
	printf '%s\n' 'zxfer_callee l_shared=renderer-data'
	cat <<'PAYLOAD'
zxfer_callee
l_shared=heredoc-data
PAYLOAD
	l_capture=$(
		zxfer_callee
	)
	zxfer_callee | cat >/dev/null
	zxfer_callee &
	wait "$!"
	printf '%s\n' "$l_shared" "$l_capture"
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "Calls that cannot mutate the caller's current shell should not consume baseline." \
		"$output" "0 baselined function-scratch overlaps"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_uninventoried_indirect_option_write() {
	l_fixture_root="$TEST_TMPDIR/uninventoried-indirect-option-write"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	l_target=g_option_dangerous
	l_marker=value#fragment; eval "$l_target=1"
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "An indirect global write through a new eval site must fail architecture validation. Output: $output" \
		1 "$status"
	assertContains "The diagnostic should identify the uninventoried eval owner and expression." \
		"$output" 'uninventoried production eval: src/zxfer_worker.sh'
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal policy rows; invoked indirectly by shunit2.
test_architecture_check_accepts_exact_eval_inventory_and_rejects_stale_entries() {
	l_fixture_root="$TEST_TMPDIR/exact-eval-inventory"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	l_renderer='eval "$hidden=1"
still renderer data'
	cat <<'PAYLOAD'
eval "$hidden=2"
PAYLOAD
	l_target=l_result
	eval "$l_target=1"
}
EOF
	printf '%s\n' \
		'src/zxfer_worker.sh	zxfer_worker	eval "$l_target=1"	validated fixture assignment' \
		>"$l_fixture_root/tests/architecture_eval_policy.tsv"

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")
	assertContains "An exact, purpose-documented eval site should satisfy architecture validation." \
		"$output" "architecture check passed"

	printf '%s\n' \
		'src/zxfer_worker.sh	zxfer_worker	eval "$l_target=2"	stale fixture assignment' \
		>"$l_fixture_root/tests/architecture_eval_policy.tsv"
	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "Changing an inventoried eval expression must fail both the new and stale sides of the exact policy." \
		1 "$status"
	assertContains "A changed site should be reported as uninventoried." \
		"$output" "uninventoried production eval"
	assertContains "The replaced policy row should be reported as stale." \
		"$output" "stale production eval inventory"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_parsed_option_write_outside_cli_owner() {
	l_fixture_root="$TEST_TMPDIR/option-owner-reject"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	g_option_dangerous=1
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "Parsed option writes outside the CLI owner should fail architecture validation." \
		1 "$status"
	assertContains "The option-state diagnostic should identify the write site." \
		"$output" "parsed option write outside src/zxfer_cli.sh: g_option_dangerous written by src/zxfer_worker.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_ignores_quoted_and_heredoc_global_assignment_data() {
	l_fixture_root="$TEST_TMPDIR/global-assignment-data"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	printf '%s\n' 'g_option_quoted=1'
	cat <<'PAYLOAD'
g_option_heredoc=1
PAYLOAD
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "Renderer and heredoc data must not be attributed as mutable global writes." \
		"$output" "architecture check passed"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_detects_global_write_after_word_internal_hash() {
	l_fixture_root="$TEST_TMPDIR/global-write-after-word-internal-hash"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	l_trimmed=${1#prefix}; g_option_dangerous=1
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A parameter-trim operator must not hide a later mutable global write. Output: $output" \
		1 "$status"
	assertContains "The parsed-option diagnostic should identify the write after the word-internal hash." \
		"$output" "parsed option write outside src/zxfer_cli.sh: g_option_dangerous written by src/zxfer_worker.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_accepts_parsed_option_write_in_cli_owner() {
	l_fixture_root="$TEST_TMPDIR/option-owner-accept"
	create_architecture_fixture "$l_fixture_root" zxfer_cli.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_cli.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_cli.sh" <<'EOF'
#!/bin/sh
zxfer_cli_parse() {
	g_option_safe=1
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "The CLI module should remain the explicit parsed-option owner." \
		"$output" "architecture check passed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
