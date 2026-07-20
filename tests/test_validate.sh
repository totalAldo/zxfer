#!/bin/sh
#
# shunit2 tests for the conservative validation dispatcher.
#
# shellcheck disable=SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_validate"
	VALIDATE_BIN="$ZXFER_ROOT/tests/validate.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

reset_validate_fixture_root() {
	FAKE_ROOT="$TEST_TMPDIR/fake-root"
	VALIDATION_LOG="$TEST_TMPDIR/validation.log"
	rm -rf "$FAKE_ROOT"
	mkdir -p "$FAKE_ROOT/tests"
	: >"$VALIDATION_LOG"
	ln -s "$VALIDATE_BIN" "$FAKE_ROOT/tests/validate.sh"
	ln -s "$ZXFER_ROOT/tests/validation_map.tsv" "$FAKE_ROOT/tests/validation_map.tsv"
	ln -s "$ZXFER_ROOT/tests/validation_profiles.tsv" "$FAKE_ROOT/tests/validation_profiles.tsv"
}

write_validate_fixture_commands() {
	cat >"$FAKE_ROOT/tests/run_lint.sh" <<'EOF'
#!/bin/sh
printf 'lint:%s\n' "$*" >>"${VALIDATION_LOG:?}"
EOF
	cat >"$FAKE_ROOT/tests/run_shunit_tests.sh" <<'EOF'
#!/bin/sh
printf 'shunit:%s\n' "$*" >>"${VALIDATION_LOG:?}"
EOF
	cat >"$FAKE_ROOT/tests/run_coverage.sh" <<'EOF'
#!/bin/sh
printf 'coverage:mode=%s args=%s\n' "${ZXFER_COVERAGE_MODE:-}" "$*" >>"${VALIDATION_LOG:?}"
EOF
	cat >"$FAKE_ROOT/tests/run_vm_matrix.sh" <<'EOF'
#!/bin/sh
printf 'vm:%s\n' "$*" >>"${VALIDATION_LOG:?}"
EOF
	cat >"$FAKE_ROOT/tests/run_property_prefetch_benchmark.sh" <<'EOF'
#!/bin/sh
printf 'property-prefetch:%s\n' "$*" >>"${VALIDATION_LOG:?}"
EOF
}

make_validate_fixture_commands_executable() {
	chmod +x "$FAKE_ROOT/tests/run_lint.sh" \
		"$FAKE_ROOT/tests/run_shunit_tests.sh" \
		"$FAKE_ROOT/tests/run_coverage.sh" \
		"$FAKE_ROOT/tests/run_vm_matrix.sh" \
		"$FAKE_ROOT/tests/run_property_prefetch_benchmark.sh"
}

setUp() {
	reset_validate_fixture_root
	write_validate_fixture_commands
	make_validate_fixture_commands_executable
}

validation_mapping_block() {
	l_mapping_output=$1
	l_mapping_path=$2
	printf '%s\n' "$l_mapping_output" | awk -v path="$l_mapping_path" '
		$0 == "==> quick map: " path { printing = 1 }
		printing { print }
		printing && /^    docs:/ { exit }
	'
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_explicit_paths_to_offline_checks() {
	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick tests/run_coverage.sh
	)

	assertContains "Quick validation should explain which path pattern selected the focused unit suite." \
		"$output" "Coverage-runner changes need policy and report-contract tests."
	assertContains "Quick validation should print wider recommendations without executing them." \
		"$output" "quick recommendations (reported, not executed)"
	assertEquals "Quick validation should run only the offline budget and mapped unit suites." \
		"lint:budget
shunit:--jobs 4 tests/test_run_coverage.sh" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_every_split_suite_fragment_to_its_stable_parent() {
	set --
	for l_fragment_path in \
		"$ZXFER_ROOT"/tests/suites/zxfer_backup_*_tests.sh \
		"$ZXFER_ROOT"/tests/suites/zxfer_exec_*_tests.sh \
		"$ZXFER_ROOT"/tests/suites/zxfer_property_*_tests.sh \
		"$ZXFER_ROOT"/tests/suites/zxfer_remote_hosts_*_tests.sh \
		"$ZXFER_ROOT"/tests/suites/zxfer_replication_*_tests.sh \
		"$ZXFER_ROOT"/tests/suites/zxfer_runtime_*_tests.sh \
		"$ZXFER_ROOT"/tests/suites/zxfer_send_receive_*_tests.sh; do
		[ -f "$l_fragment_path" ] || continue
		set -- "$@" "${l_fragment_path#"$ZXFER_ROOT"/}"
	done

	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick "$@"
	)

	for l_fragment_path in "$@"; do
		l_mapping_block=$(validation_mapping_block "$output" "$l_fragment_path")
		case "$l_fragment_path" in
		tests/suites/zxfer_backup_*)
			l_expected_suite=tests/test_zxfer_backup_metadata.sh
			l_expected_reason="Backup behavior-fragment changes"
			;;
		tests/suites/zxfer_exec_*)
			l_expected_suite=tests/test_zxfer_exec.sh
			l_expected_reason="Execution behavior-fragment changes"
			;;
		tests/suites/zxfer_property_*)
			l_expected_suite=tests/test_zxfer_property_reconcile.sh
			l_expected_reason="Property behavior-fragment changes"
			;;
		tests/suites/zxfer_remote_hosts_*)
			l_expected_suite=tests/test_zxfer_remote_hosts.sh
			l_expected_reason="Remote-host behavior-fragment changes"
			;;
		tests/suites/zxfer_replication_*)
			l_expected_suite=tests/test_zxfer_replication.sh
			l_expected_reason="Replication behavior-fragment changes"
			;;
		tests/suites/zxfer_runtime_*)
			l_expected_suite=tests/test_zxfer_runtime.sh
			l_expected_reason="Runtime behavior-fragment changes"
			;;
		tests/suites/zxfer_send_receive_*)
			l_expected_suite=tests/test_zxfer_send_receive.sh
			l_expected_reason="Send/receive behavior-fragment changes"
			;;
		esac
		assertContains "Every split behavior fragment should report its stable parent suite." \
			"$l_mapping_block" "$l_expected_suite"
		assertContains "Every split behavior fragment should explain its concern-specific mapping." \
			"$l_mapping_block" "$l_expected_reason"
	done

	assertEquals "Split fragments should deduplicate to the seven stable parent suites." \
		"lint:budget
shunit:--jobs 4 tests/test_zxfer_backup_metadata.sh tests/test_zxfer_exec.sh tests/test_zxfer_property_reconcile.sh tests/test_zxfer_remote_hosts.sh tests/test_zxfer_replication.sh tests/test_zxfer_runtime.sh tests/test_zxfer_send_receive.sh" \
		"$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_every_manifest_integration_fragment_to_exact_loader_contracts() {
	set --
	while IFS= read -r l_fragment_path; do
		[ -n "$l_fragment_path" ] || continue
		set -- "$@" "tests/$l_fragment_path"
	done <<EOF
$(awk 'NR > 1 { print }' "$ZXFER_ROOT/tests/integration_fragment_manifest.tsv")
EOF

	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick "$@"
	)

	for l_fragment_path in "$@"; do
		l_mapping_block=$(validation_mapping_block "$output" "$l_fragment_path")
		assertContains "Each manifest fragment should select its exact row before the wildcard fallback." \
			"$l_mapping_block" "matched:     $l_fragment_path"
		assertContains "Each manifest fragment should select the stable loader and safety-contract suite." \
			"$l_mapping_block" "tests/test_run_integration_zxfer.sh"
	done
	assertEquals "Manifest fragments should deduplicate to one host-safe loader-contract suite." \
		"lint:budget
shunit:--jobs 4 tests/test_run_integration_zxfer.sh" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_fragment_manifest_to_loader_and_map_contracts() {
	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick tests/integration_fragment_manifest.tsv
	)
	l_mapping_block=$(validation_mapping_block "$output" tests/integration_fragment_manifest.tsv)

	assertContains "The fragment manifest should use its exact validation-map row." \
		"$l_mapping_block" "matched:     tests/integration_fragment_manifest.tsv"
	assertContains "Manifest changes should validate integration loading and quick-map completeness together." \
		"$l_mapping_block" "tests/test_run_integration_zxfer.sh,tests/test_validate.sh"
	assertEquals "Manifest changes should execute both host-safe contract suites." \
		"lint:budget
shunit:--jobs 4 tests/test_run_integration_zxfer.sh tests/test_validate.sh" \
		"$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_unclassified_integration_fragments_to_loader_contracts() {
	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick tests/integration/future_concern_tests.sh
	)
	l_mapping_block=$(validation_mapping_block "$output" tests/integration/future_concern_tests.sh)

	assertContains "A future integration fragment should select the dedicated wildcard before generic test mappings." \
		"$l_mapping_block" "matched:     tests/integration/*.sh"
	assertContains "A new integration concern fragment should select the stable loader and safety-contract suite." \
		"$l_mapping_block" "tests/test_run_integration_zxfer.sh"
	assertContains "The integration fallback should explain why unclassified fragments cannot skip focused validation." \
		"$l_mapping_block" "Future integration fragments must run the host-safe loader and safety-contract suite"
	assertEquals "Quick validation should execute the integration loader contract for an unclassified fragment." \
		"lint:budget
shunit:--jobs 4 tests/test_run_integration_zxfer.sh" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_shared_unit_helpers_to_representative_suites() {
	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick \
			tests/helpers/lifecycle.sh \
			tests/helpers/process_capture.sh \
			tests/helpers/loader.sh \
			tests/helpers/backup_fixtures.sh
	)

	l_mapping_block=$(validation_mapping_block "$output" tests/helpers/lifecycle.sh)
	assertContains "Lifecycle helpers should select the unit runner." \
		"$l_mapping_block" "tests/test_run_shunit_tests.sh"
	assertContains "Lifecycle helpers should select a representative launcher suite." \
		"$l_mapping_block" "tests/test_zxfer_launcher.sh"
	assertContains "Lifecycle mappings should explain the cleanup concern." \
		"$l_mapping_block" "Shared unit lifecycle changes"

	l_mapping_block=$(validation_mapping_block "$output" tests/helpers/process_capture.sh)
	assertContains "Process-capture helpers should select runner supervision coverage." \
		"$l_mapping_block" "tests/test_run_shunit_tests.sh"
	assertContains "Process-capture mappings should explain their supervision concern." \
		"$l_mapping_block" "Shared process-capture changes"

	l_mapping_block=$(validation_mapping_block "$output" tests/helpers/loader.sh)
	assertContains "Loader helpers should select launcher and dependency coverage." \
		"$l_mapping_block" "tests/test_zxfer_launcher.sh,tests/test_zxfer_dependencies.sh"
	assertContains "Loader mappings should explain canonical module loading." \
		"$l_mapping_block" "Shared loader changes"

	l_mapping_block=$(validation_mapping_block "$output" tests/helpers/backup_fixtures.sh)
	assertContains "Backup fixtures should select the stable backup-metadata suite." \
		"$l_mapping_block" "tests/test_zxfer_backup_metadata.sh"
	assertContains "Backup-fixture mappings should explain their stable parent." \
		"$l_mapping_block" "Shared backup-fixture changes"

	assertEquals "Shared helper mappings should deduplicate conservative representative suites." \
		"lint:budget
shunit:--jobs 4 tests/test_run_shunit_tests.sh tests/test_zxfer_launcher.sh tests/test_zxfer_dependencies.sh tests/test_zxfer_backup_metadata.sh" \
		"$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_maps_remote_script_goldens_to_owning_suites() {
	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick \
			tests/golden/remote_backup_protocol_scripts.golden \
			tests/golden/remote_capability_probe_script.golden
	)

	l_mapping_block=$(validation_mapping_block "$output" tests/golden/remote_backup_protocol_scripts.golden)
	assertContains "The remote backup protocol golden should use its exact mapping row." \
		"$l_mapping_block" "matched:     tests/golden/remote_backup_protocol_scripts.golden"
	assertContains "The remote backup protocol golden should select the backup-metadata suite." \
		"$l_mapping_block" "tests/test_zxfer_backup_metadata.sh"

	l_mapping_block=$(validation_mapping_block "$output" tests/golden/remote_capability_probe_script.golden)
	assertContains "The remote capability-probe golden should use its exact mapping row." \
		"$l_mapping_block" "matched:     tests/golden/remote_capability_probe_script.golden"
	assertContains "The remote capability-probe golden should select the remote-host suite." \
		"$l_mapping_block" "tests/test_zxfer_remote_hosts.sh"

	assertEquals "Golden mappings should dispatch only their two stable owning suites." \
		"lint:budget
shunit:--jobs 4 tests/test_zxfer_backup_metadata.sh tests/test_zxfer_remote_hosts.sh" \
		"$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_full_uses_explicit_enforced_bash_xtrace_coverage() {
	VALIDATION_LOG="$VALIDATION_LOG" \
		"$FAKE_ROOT/tests/validate.sh" full >/dev/null

	assertEquals "Full validation should compose the existing lint, unit, and enforced coverage entrypoints in order." \
		"lint:
shunit:--jobs 4
coverage:mode=bash-xtrace args=--enforce" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_uses_configurable_bounded_unit_concurrency() {
	VALIDATION_LOG="$VALIDATION_LOG" \
		ZXFER_VALIDATE_JOBS=2 \
		"$FAKE_ROOT/tests/validate.sh" quick tests/run_coverage.sh >/dev/null

	assertEquals "Quick validation should forward the configured suite concurrency without changing the mapped selection." \
		"lint:budget
shunit:--jobs 2 tests/test_run_coverage.sh" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_rejects_invalid_unit_concurrency_before_dispatch() {
	zxfer_test_capture_subshell "
		VALIDATION_LOG=\"$VALIDATION_LOG\" \\
		ZXFER_VALIDATE_JOBS=0 \\
		\"$FAKE_ROOT/tests/validate.sh\" quick tests/run_coverage.sh
	"

	assertEquals "Invalid suite concurrency should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should name the positive-integer contract." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_VALIDATE_JOBS must be a positive integer"
	assertEquals "Invalid suite concurrency should stop before unit dispatch while retaining the offline budget gate." \
		"lint:budget" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_vm_profile_cannot_be_widened_beyond_smoke_or_local() {
	zxfer_test_capture_subshell "
		VALIDATION_LOG=\"$VALIDATION_LOG\" \
		\"$FAKE_ROOT/tests/validate.sh\" vm --profile full
	"

	assertEquals "The conservative VM alias should reject attempts to replace its local profile." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The VM validation error should name the only accepted profiles." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "vm profile must be smoke or local"
	assertEquals "Rejected VM widening should stop before the VM runner is invoked." \
		"" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_vm_forwards_bounded_smoke_and_local_profiles() {
	VALIDATION_LOG="$VALIDATION_LOG" \
		"$FAKE_ROOT/tests/validate.sh" vm smoke --guest ubuntu >/dev/null
	VALIDATION_LOG="$VALIDATION_LOG" \
		"$FAKE_ROOT/tests/validate.sh" vm local --guest freebsd >/dev/null

	assertEquals "VM validation should forward only explicitly bounded disposable-guest profiles." \
		"vm:--profile smoke --guest ubuntu
vm:--profile local --guest freebsd" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_quick_without_paths_inspects_git_changes() {
	mkdir -p "$FAKE_ROOT/src"
	printf '%s\n' "baseline" >"$FAKE_ROOT/src/zxfer_cli.sh"
	(
		cd "$FAKE_ROOT"
		git init -q
		git config user.email zxfer-tests@example.invalid
		git config user.name "zxfer tests"
		git add .
		git commit -qm baseline
	)
	printf '%s\n' "changed" >>"$FAKE_ROOT/src/zxfer_cli.sh"
	printf '%s\n' "staged" >"$FAKE_ROOT/src/zxfer_runtime.sh"
	(
		cd "$FAKE_ROOT"
		git add src/zxfer_runtime.sh
	)
	printf '%s\n' "untracked" >"$FAKE_ROOT/tests/test_untracked.sh"

	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			"$FAKE_ROOT/tests/validate.sh" quick
	)

	assertContains "Quick validation without paths should report Git as its staged/unstaged/untracked source." \
		"$output" "staged, unstaged, and untracked Git paths"
	assertContains "The changed CLI path should explain the public-interface reason for its mapping." \
		"$output" "CLI parsing changes are public interfaces"
	assertContains "Git-derived quick validation should include a staged path." \
		"$output" "quick map: src/zxfer_runtime.sh"
	assertContains "Git-derived quick validation should include an untracked path." \
		"$output" "quick map: tests/test_untracked.sh"
	assertEquals "Git-derived quick validation should deduplicate suites from unstaged, staged, and untracked paths." \
		"lint:budget
shunit:--jobs 4 tests/test_zxfer_cli.sh tests/test_zxfer_cli_golden.sh tests/test_zxfer_launcher.sh tests/test_zxfer_runtime.sh tests/test_zxfer_locking.sh tests/test_untracked.sh" \
		"$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validate_doctor_reports_optional_capabilities_without_running_them() {
	output=$(
		VALIDATION_LOG="$VALIDATION_LOG" \
			ZXFER_LINT_TOOL_DIR="$TEST_TMPDIR/empty-lint-cache" \
			"$FAKE_ROOT/tests/validate.sh" doctor
	)

	assertContains "Doctor should report shell availability." "$output" "Shells:"
	assertContains "Doctor should report the complete offline architecture/budget command baseline." \
		"$output" "comm"
	assertContains "Doctor should include the xargs dependency used to scan the budgeted source set." \
		"$output" "xargs"
	assertContains "Doctor should report the cmp dependency used by the offline property-prefetch benchmark." \
		"$output" "cmp"
	assertContains "Doctor should report the exact time utility required by the offline property-prefetch benchmark." \
		"$output" "/usr/bin/time"
	assertContains "Doctor should report QEMU availability without starting a guest." "$output" "QEMU commands (optional):"
	assertContains "Doctor should report ZFS command presence without invoking ZFS." "$output" "never invoked"
	assertContains "Doctor should report whether pinned lint tools are already cached." "$output" "not cached"
	assertEquals "Doctor should not dispatch any validation runner." "" "$(cat "$VALIDATION_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
validation_map_field_values() {
	l_field_number=$1
	awk -F '\t' -v field="$l_field_number" '
		$1 !~ /^#/ && $1 != "" && $field != "-" {
			count = split($field, values, ",")
			for (i = 1; i <= count; i++) print values[i]
		}
	' "$ZXFER_ROOT/tests/validation_map.tsv" | sort -u
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validation_map_references_existing_targets() {
	missing_units=
	while IFS= read -r l_suite; do
		[ -n "$l_suite" ] || continue
		[ "$l_suite" = @self ] && continue
		if [ ! -f "$ZXFER_ROOT/$l_suite" ]; then
			missing_units="$missing_units${missing_units:+
}$l_suite"
		fi
	done <<EOF
$(validation_map_field_values 2)
EOF

	missing_integration=
	integration_fragment_paths=$(awk 'NR > 1 { print }' \
		"$ZXFER_ROOT/tests/integration_fragment_manifest.tsv")
	while IFS= read -r l_test; do
		[ -n "$l_test" ] || continue
		l_integration_definition_found=0
		while IFS= read -r l_integration_fragment_path; do
			[ -n "$l_integration_fragment_path" ] || continue
			if grep -q "^$l_test() {" "$ZXFER_ROOT/tests/$l_integration_fragment_path"; then
				l_integration_definition_found=1
				break
			fi
		done <<-EOF
			$integration_fragment_paths
		EOF
		if [ "$l_integration_definition_found" -ne 1 ]; then
			missing_integration="$missing_integration${missing_integration:+
}$l_test"
		fi
	done <<EOF
$(validation_map_field_values 3)
EOF

	perf_cases=$(sed -n 's/^ZXFER_PERF_CASE_LIST="\([^"]*\)"/\1/p' \
		"$ZXFER_ROOT/tests/run_perf_tests.sh")
	missing_perf=
	while IFS= read -r l_case; do
		[ -n "$l_case" ] || continue
		case " $perf_cases " in
		*" $l_case "*) ;;
		*)
			missing_perf="$missing_perf${missing_perf:+
}$l_case"
			;;
		esac
	done <<EOF
$(validation_map_field_values 4)
EOF

	missing_docs=
	while IFS= read -r l_surface; do
		[ -n "$l_surface" ] || continue
		if [ ! -e "$ZXFER_ROOT/$l_surface" ]; then
			missing_docs="$missing_docs${missing_docs:+
}$l_surface"
		fi
	done <<EOF
$(validation_map_field_values 5)
EOF

	assertEquals "Every concrete unit suite in the validation map should exist." "" "$missing_units"
	assertEquals "Every integration recommendation should name a selectable harness test." "" "$missing_integration"
	assertEquals "Every performance recommendation should name a supported performance case." "" "$missing_perf"
	assertEquals "Every documentation recommendation should name an existing repository surface." "" "$missing_docs"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validation_map_has_an_exact_row_for_every_manifest_module() {
	missing_modules=
	while IFS= read -r l_module; do
		[ -n "$l_module" ] || continue
		l_module_path="src/$l_module"
		if ! awk -F '\t' -v module_path="$l_module_path" \
			'$1 == module_path { found = 1 } END { exit !found }' \
			"$ZXFER_ROOT/tests/validation_map.tsv"; then
			missing_modules="$missing_modules${missing_modules:+
}$l_module_path"
		fi
	done <<EOF
$ZXFER_SOURCE_MODULE_MANIFEST
EOF

	assertEquals "Every canonical module should have a concern-specific quick-validation row before the generic source fallback." \
		"" "$missing_modules"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_validation_map_has_an_exact_row_for_every_integration_fragment() {
	invalid_fragments=
	l_fallback_count=$(awk -F '\t' \
		'$1 == "tests/integration/*.sh" { count++ } END { print count + 0 }' \
		"$ZXFER_ROOT/tests/validation_map.tsv")
	l_fallback_line=$(awk -F '\t' \
		'$1 == "tests/integration/*.sh" { print NR; exit }' \
		"$ZXFER_ROOT/tests/validation_map.tsv")
	assertEquals "The integration quick-validation map should have one safe wildcard fallback." \
		1 "$l_fallback_count"
	while IFS= read -r l_fragment; do
		[ -n "$l_fragment" ] || continue
		l_fragment_path="tests/$l_fragment"
		if ! awk -F '\t' -v fragment_path="$l_fragment_path" \
			-v fallback_line="$l_fallback_line" '
			$1 == fragment_path {
				count++
				exact_line = NR
				suites = $2
			}
			END {
				has_owner = suites ~ /(^|,)tests\/test_run_integration_zxfer[.]sh(,|$)/
				exit !(count == 1 && exact_line < fallback_line && has_owner)
			}
		' \
			"$ZXFER_ROOT/tests/validation_map.tsv"; then
			invalid_fragments="$invalid_fragments${invalid_fragments:+
}$l_fragment_path"
		fi
	done <<EOF
$(awk 'NR > 1 { print }' "$ZXFER_ROOT/tests/integration_fragment_manifest.tsv")
EOF

	assertEquals "Every manifest-listed integration fragment should have one owning-suite row before the safe wildcard fallback." \
		"" "$invalid_fragments"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
