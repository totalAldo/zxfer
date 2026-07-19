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
	GLOBAL_REFERENCE_AWK_SOURCE="$ZXFER_ROOT/tests/extract_global_references.awk"
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
	cp "$GLOBAL_REFERENCE_AWK_SOURCE" "$l_fixture_root/tests/extract_global_references.awk"
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
edge	src/zxfer_caller.sh	src/zxfer_callee.sh
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

# shellcheck disable=SC2016,SC2317,SC2329  # Literal renderer data; invoked indirectly by shunit2.
test_architecture_check_ignores_quoted_and_heredoc_global_assignment_data() {
	l_fixture_root="$TEST_TMPDIR/global-assignment-data"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	printf '%s\n' 'g_option_quoted=1' "g_option_quoted_default:=1"
	cat <<'PAYLOAD'
g_option_heredoc=1
${g_option_heredoc_default:=1}
${g_option_heredoc_plain_default=1}
PAYLOAD
	cat <<PAYLOAD
\${g_option_escaped_default:=1}
\${g_option_escaped_plain_default=1}
PAYLOAD
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "Renderer and heredoc data must not be attributed as mutable global writes." \
		"$output" "architecture check passed"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal fixture expansion; invoked indirectly by shunit2.
test_architecture_check_detects_double_quoted_default_assignment_write() {
	l_fixture_root="$TEST_TMPDIR/double-quoted-default-assignment"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	: "${g_option_dangerous:=1}"
	: "${g_option_plain_dangerous=1}"
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A default assignment inside double quotes must retain global-write ownership. Output: $output" \
		1 "$status"
	assertContains "The quoted default assignment should identify the parsed-option write." \
		"$output" "parsed option write outside src/zxfer_cli.sh: g_option_dangerous written by src/zxfer_worker.sh"
	assertContains "The quoted non-colon default assignment should also identify the parsed-option write." \
		"$output" "parsed option write outside src/zxfer_cli.sh: g_option_plain_dangerous written by src/zxfer_worker.sh"
}

# shellcheck disable=SC2016,SC2317,SC2329  # Literal fixture expansion; invoked indirectly by shunit2.
test_architecture_check_detects_unquoted_heredoc_default_assignment_write() {
	l_fixture_root="$TEST_TMPDIR/unquoted-heredoc-default-assignment"
	create_architecture_fixture "$l_fixture_root" zxfer_worker.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_worker.sh	0	foundation
EOF
	cat >"$l_fixture_root/src/zxfer_worker.sh" <<'EOF'
#!/bin/sh
zxfer_worker() {
	cat <<PAYLOAD
${g_option_dangerous:=1}
${g_option_plain_dangerous=1}
PAYLOAD
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A default assignment in an expanding heredoc must retain global-write ownership. Output: $output" \
		1 "$status"
	assertContains "The heredoc default assignment should identify the parsed-option write." \
		"$output" "parsed option write outside src/zxfer_cli.sh: g_option_dangerous written by src/zxfer_worker.sh"
	assertContains "The heredoc non-colon default assignment should also identify the parsed-option write." \
		"$output" "parsed option write outside src/zxfer_cli.sh: g_option_plain_dangerous written by src/zxfer_worker.sh"
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

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_unapproved_dependency_edge() {
	l_fixture_root="$TEST_TMPDIR/unapproved-dependency-edge"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
EOF
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	:
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	zxfer_callee
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A new downward dependency should require an explicit edge-policy row. Output: $output" \
		1 "$status"
	assertContains "The diagnostic should name the unapproved edge." \
		"$output" "unapproved dependency edge: src/zxfer_caller.sh -> src/zxfer_callee.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_stale_dependency_edge() {
	l_fixture_root="$TEST_TMPDIR/stale-dependency-edge"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	:
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	:
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "Removing a dependency should require deleting its policy row. Output: $output" \
		1 "$status"
	assertContains "The diagnostic should name the stale edge." \
		"$output" "stale dependency-edge policy entry: src/zxfer_caller.sh -> src/zxfer_callee.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_strips_inline_dependency_comments() {
	l_fixture_root="$TEST_TMPDIR/inline-dependency-comment"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	:
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	: # zxfer_callee is documentation, not a dependency.
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A function name in a genuine inline comment must not preserve an obsolete edge. Output: $output" \
		1 "$status"
	assertContains "The comment-only dependency should leave the policy edge stale." \
		"$output" "stale dependency-edge policy entry: src/zxfer_caller.sh -> src/zxfer_callee.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_preserves_word_internal_hash_dependency_tokens() {
	l_fixture_root="$TEST_TMPDIR/word-internal-hash-dependency"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	:
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_renderer=prefix#zxfer_callee
	printf '%s\n' "$l_renderer"
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "A word-internal hash is data, so later dependency tokens must remain visible." \
		"$output" "1 approved acyclic dependency edges"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_preserves_hashes_in_multiline_quoted_renderers() {
	l_fixture_root="$TEST_TMPDIR/quoted-hash-dependency"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	:
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	l_renderer='generated command follows
# zxfer_callee
end generated command'
	printf '%s\n' "$l_renderer"
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "A hash inside a multiline quoted renderer must not hide its dependency token." \
		"$output" "1 approved acyclic dependency edges"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_preserves_hashes_in_rendered_heredocs() {
	l_fixture_root="$TEST_TMPDIR/heredoc-hash-dependency"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	:
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	cat <<'PAYLOAD'
# zxfer_callee
PAYLOAD
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "A hash in rendered heredoc data must not be stripped as a source-shell comment." \
		"$output" "1 approved acyclic dependency edges"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_accepts_exact_result_channel_consumer() {
	l_fixture_root="$TEST_TMPDIR/exact-result-consumer"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
edge	src/zxfer_caller.sh	src/zxfer_callee.sh
result-consumer	g_zxfer_payload_result	src/zxfer_callee.sh	src/zxfer_caller.sh
EOF
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	g_zxfer_payload_result=value
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	zxfer_callee
	printf '%s\n' "$g_zxfer_payload_result"
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "An exact owner/consumer tuple should preserve the architecture gate." \
		"$output" "1 approved cross-owner result-channel consumers"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_unapproved_result_channel_consumer() {
	l_fixture_root="$TEST_TMPDIR/unapproved-result-consumer"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	write_two_module_architecture_policy "$l_fixture_root"
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	g_zxfer_payload_result=value
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	zxfer_callee
	printf '%s\n' "${g_zxfer_payload_result:-}"
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A new result-channel consumer should require explicit approval. Output: $output" \
		1 "$status"
	assertContains "The diagnostic should name the channel, owner, and consumer." \
		"$output" "unapproved cross-owner result-channel consumer: g_zxfer_payload_result owned by src/zxfer_callee.sh read by src/zxfer_caller.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_counts_bare_result_reference_in_quoted_arithmetic() {
	l_fixture_root="$TEST_TMPDIR/quoted-arithmetic-result-consumer"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
EOF
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	g_zxfer_payload_result=1
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	printf '%s\n' "$((g_zxfer_payload_result + 1))"
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "A bare global name in quoted arithmetic must count as a result consumer. Output: $output" \
		1 "$status"
	assertContains "The quoted arithmetic reference should identify its channel consumer." \
		"$output" "unapproved cross-owner result-channel consumer: g_zxfer_payload_result owned by src/zxfer_callee.sh read by src/zxfer_caller.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_counts_unquoted_heredoc_result_expansion() {
	l_fixture_root="$TEST_TMPDIR/unquoted-heredoc-result-consumer"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
EOF
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	g_zxfer_payload_result=value
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	cat <<PAYLOAD
$((g_zxfer_payload_result + 1))
PAYLOAD
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "Bare arithmetic state in an unquoted heredoc expansion should count as a result consumer. Output: $output" \
		1 "$status"
	assertContains "The heredoc arithmetic expansion should identify its channel consumer." \
		"$output" "unapproved cross-owner result-channel consumer: g_zxfer_payload_result owned by src/zxfer_callee.sh read by src/zxfer_caller.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_rejects_stale_result_channel_consumer() {
	l_fixture_root="$TEST_TMPDIR/stale-result-consumer"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
edge	src/zxfer_caller.sh	src/zxfer_callee.sh
result-consumer	g_zxfer_payload_result	src/zxfer_callee.sh	src/zxfer_caller.sh
EOF
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	g_zxfer_payload_result=value
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	zxfer_callee
}
EOF

	set +e
	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh" 2>&1)
	status=$?
	set -e

	assertEquals "Removing a result-channel read should require deleting its policy row. Output: $output" \
		1 "$status"
	assertContains "The diagnostic should name the stale owner/consumer tuple." \
		"$output" "stale result-channel consumer policy entry: g_zxfer_payload_result owned by src/zxfer_callee.sh no longer read by src/zxfer_caller.sh"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_architecture_check_ignores_literal_result_channel_text() {
	l_fixture_root="$TEST_TMPDIR/literal-result-channel-text"
	create_architecture_fixture "$l_fixture_root" zxfer_callee.sh zxfer_caller.sh
	cat >"$l_fixture_root/tests/architecture_policy.tsv" <<'EOF'
layer	src/zxfer_callee.sh	0	foundation
layer	src/zxfer_caller.sh	1	composition
EOF
	cat >"$l_fixture_root/src/zxfer_callee.sh" <<'EOF'
#!/bin/sh
zxfer_callee() {
	g_zxfer_payload_result=value
}
EOF
	cat >"$l_fixture_root/src/zxfer_caller.sh" <<'EOF'
#!/bin/sh
zxfer_caller() {
	printf '%s\n' 'g_zxfer_payload_result' "g_zxfer_payload_result"
	# $g_zxfer_payload_result is documentation, not a shell read.
	cat <<'PAYLOAD'
$g_zxfer_payload_result
$((g_zxfer_payload_result + 1))
PAYLOAD
	cat <<PAYLOAD
\$g_zxfer_payload_result
\$((g_zxfer_payload_result + 1))
PAYLOAD
	cat <<\PAYLOAD
$g_zxfer_payload_result
$((g_zxfer_payload_result + 1))
PAYLOAD
}
EOF

	output=$(ZXFER_ARCHITECTURE_ROOT="$l_fixture_root" \
		"$l_fixture_root/tests/check_architecture.sh")

	assertContains "Comments and rendered literal data should not become consumers." \
		"$output" "0 approved cross-owner result-channel consumers"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
