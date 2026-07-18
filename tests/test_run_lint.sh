#!/bin/sh
#
# shunit2 tests for the lint runner script.
#
# shellcheck disable=SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_lint"
	RUN_LINT_BIN="$ZXFER_ROOT/tests/run_lint.sh"
	TEST_HELPER_EVAL_CHECK_BIN="$ZXFER_ROOT/tests/check_test_helper_eval.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

write_fake_lint_toolchain() {
	l_tool_root=$1
	# These are shell-script fakes, so use one pinned asset tuple independently
	# of the host running this unit suite. A companion uname fake makes the lint
	# runner resolve the same tuple on FreeBSD and other supported test guests.
	l_host_os=linux
	l_host_arch=amd64

	set -- \
		"$l_tool_root/checkbashisms/2.25.33/checkbashisms" \
		"$l_tool_root/shfmt/3.13.0/$l_host_os-$l_host_arch/shfmt" \
		"$l_tool_root/shellcheck/0.11.0/$l_host_os-$l_host_arch/shellcheck"
	for l_tool_path in "$@"; do
		mkdir -p "$(dirname "$l_tool_path")"
		cat >"$l_tool_path" <<'EOF'
#!/bin/sh
l_tool_name=$(basename "$0")
for l_tool_arg in "$@"; do
	printf '%s\t%s\n' "$l_tool_name" "$l_tool_arg" >>"${LINT_CAPTURE_LOG:?}"
done
EOF
		chmod +x "$l_tool_path"
	done
}

write_fake_lint_host_uname() {
	l_fake_bin=$1
	mkdir -p "$l_fake_bin"
	cat >"$l_fake_bin/uname" <<'EOF'
#!/bin/sh
case "${1:-}" in
-m) printf '%s\n' x86_64 ;;
-s | '') printf '%s\n' Linux ;;
*) exit 1 ;;
esac
EOF
	chmod +x "$l_fake_bin/uname"
}

initialize_fake_lint_repository() {
	l_fake_root=$1
	mkdir -p "$l_fake_root/tests"
	ln -s "$RUN_LINT_BIN" "$l_fake_root/tests/run_lint.sh"
	cat >"$l_fake_root/tests/check_test_helper_eval.sh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$l_fake_root/tests/check_test_helper_eval.sh"
	(
		cd "$l_fake_root"
		git init -q
		git config user.email zxfer-tests@example.invalid
		git config user.name "zxfer tests"
	)
}

lint_capture_path_count() {
	l_capture_path=$1
	l_expected_path=$2
	awk -F '\t' -v path="$l_expected_path" '$2 == path { count++ } END { print count + 0 }' \
		"$l_capture_path"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_lint_budget_skips_download_toolchain_preflight() {
	l_fake_root="$TEST_TMPDIR/fake-root"
	l_blocked_parent="$TEST_TMPDIR/not-a-directory"
	mkdir -p "$l_fake_root/tests"
	ln -s "$RUN_LINT_BIN" "$l_fake_root/tests/run_lint.sh"
	cat >"$l_fake_root/tests/run_budget_check.sh" <<'EOF'
#!/bin/sh
printf '%s\n' "isolated budget passed"
EOF
	cat >"$l_fake_root/tests/check_architecture.sh" <<'EOF'
#!/bin/sh
printf '%s\n' "isolated architecture passed"
EOF
	chmod +x "$l_fake_root/tests/run_budget_check.sh" \
		"$l_fake_root/tests/check_architecture.sh"
	: >"$l_blocked_parent"

	set +e
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			ZXFER_LINT_TOOL_DIR="$l_blocked_parent/cache" \
			"$l_fake_root/tests/run_lint.sh" budget 2>&1
	)
	status=$?
	set -e

	assertEquals "The dependency-free budget target should not initialize the downloadable lint toolchain." \
		0 "$status"
	assertContains "The isolated budget command should still be dispatched normally." \
		"$output" "isolated budget passed"
	assertContains "The budget target should also enforce the offline architecture policy." \
		"$output" "isolated architecture passed"
	assertNotContains "A dependency-free target should never try to create the configured lint cache." \
		"$output" "not a directory"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_lint_shell_targets_include_nonignored_untracked_sources_once() {
	l_fake_root="$TEST_TMPDIR/source-list-root"
	l_tool_root="$TEST_TMPDIR/source-list-tools"
	l_fake_bin="$TEST_TMPDIR/source-list-bin"
	l_capture_log="$TEST_TMPDIR/source-list.log"
	initialize_fake_lint_repository "$l_fake_root"
	write_fake_lint_toolchain "$l_tool_root"
	write_fake_lint_host_uname "$l_fake_bin"
	: >"$l_capture_log"

	mkdir -p "$l_fake_root/src" "$l_fake_root/tests/split fragments"
	printf '%s\n' '#!/bin/sh' >"$l_fake_root/zxfer"
	printf '%s\n' '#!/bin/sh' >"$l_fake_root/src/tracked.sh"
	printf '%s\n' '#!/bin/sh' >"$l_fake_root/src/untracked.sh"
	printf '%s\n' '#!/bin/sh' >"$l_fake_root/tests/split fragments/untracked helper.sh"
	printf '%s\n' '#!/bin/sh' >"$l_fake_root/ignored.sh"
	printf '%s\n' 'ignored.sh' >"$l_fake_root/.gitignore"
	(
		cd "$l_fake_root"
		git add .gitignore zxfer src/tracked.sh tests/run_lint.sh \
			tests/check_test_helper_eval.sh
		git commit -qm baseline
	)

	PATH="$l_fake_bin:${PATH:-/usr/bin:/bin}" \
		LINT_CAPTURE_LOG="$l_capture_log" \
		ZXFER_LINT_TOOL_DIR="$l_tool_root" \
		"$l_fake_root/tests/run_lint.sh" checkbashisms shfmt shellcheck >/dev/null

	for l_path in zxfer src/tracked.sh src/untracked.sh \
		"tests/split fragments/untracked helper.sh"; do
		assertEquals "Each shell target should receive $l_path exactly once." \
			3 "$(lint_capture_path_count "$l_capture_log" "$l_path")"
	done
	assertEquals "Ignored shell sources must remain outside every lint target." \
		0 "$(lint_capture_path_count "$l_capture_log" ignored.sh)"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_lint_shell_targets_skip_tool_invocation_for_an_empty_source_list() {
	l_fake_root="$TEST_TMPDIR/empty-source-root"
	l_tool_root="$TEST_TMPDIR/empty-source-tools"
	l_fake_bin="$TEST_TMPDIR/empty-source-bin"
	l_capture_log="$TEST_TMPDIR/empty-source.log"
	initialize_fake_lint_repository "$l_fake_root"
	write_fake_lint_toolchain "$l_tool_root"
	write_fake_lint_host_uname "$l_fake_bin"
	: >"$l_capture_log"
	printf '%s\n' 'tests/' '*.sh' 'zxfer' >"$l_fake_root/.gitignore"

	PATH="$l_fake_bin:${PATH:-/usr/bin:/bin}" \
		LINT_CAPTURE_LOG="$l_capture_log" \
		ZXFER_LINT_TOOL_DIR="$l_tool_root" \
		"$l_fake_root/tests/run_lint.sh" checkbashisms shfmt shellcheck >/dev/null

	assertEquals "An empty NUL stream should not invoke any shell lint tool." \
		"" "$(cat "$l_capture_log")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_codespell_skips_generated_coverage_trees() {
	l_codespell_skip=$(awk -F '=' '
		$1 ~ /^[[:space:]]*skip[[:space:]]*$/ {
			value = $2
			gsub(/[[:space:]]/, "", value)
			print value
			exit
		}
	' "$ZXFER_ROOT/.codespellrc")

	assertContains "Generated coverage traces must stay outside documentation spelling output." \
		",$l_codespell_skip," ",./coverage,"
	assertContains "Alternate generated coverage trees must stay outside documentation spelling output." \
		",$l_codespell_skip," ",./coverage-codex,"
}

# shellcheck disable=SC2329  # Invoked by shunit2 test functions.
write_test_helper_eval_policy_fixture() {
	l_fixture_root=$1
	mkdir -p "$l_fixture_root/tests/helpers"
	cat >"$l_fixture_root/tests/test_helper_eval_policy.tsv" <<'EOF'
# helper_path	function_name	rationale
tests/helpers/process_capture.sh	legacy_capture	Compatibility fixture.
EOF
	cat >"$l_fixture_root/tests/helpers/process_capture.sh" <<'EOF'
#!/bin/sh
legacy_capture() {
	eval "$1"
}
EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_eval_policy_accepts_the_inventoried_legacy_capture() {
	l_fixture_root="$TEST_TMPDIR/eval-policy-accept"
	write_test_helper_eval_policy_fixture "$l_fixture_root"

	set +e
	output=$("$TEST_HELPER_EVAL_CHECK_BIN" "$l_fixture_root" 2>&1)
	status=$?
	set -e

	assertEquals "The explicitly inventoried compatibility helper should remain available during migration." \
		0 "$status"
	assertEquals "A passing static helper policy should stay quiet." "" "$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_eval_policy_rejects_a_new_eval_based_capture_helper() {
	l_fixture_root="$TEST_TMPDIR/eval-policy-reject"
	write_test_helper_eval_policy_fixture "$l_fixture_root"
	cat >"$l_fixture_root/tests/helpers/new_capture.sh" <<'EOF'
#!/bin/sh
new_capture() {
	eval "$1"
}
EOF

	set +e
	output=$("$TEST_HELPER_EVAL_CHECK_BIN" "$l_fixture_root" 2>&1)
	status=$?
	set -e

	assertNotEquals "A new eval-based shared capture helper must fail static lint." 0 "$status"
	assertContains "The failure should identify the uninventoried helper and source line." \
		"$output" "tests/helpers/new_capture.sh:3 has an uninventoried eval command in new_capture"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
