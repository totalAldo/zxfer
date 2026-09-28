#!/bin/sh
#
# shunit2 tests for the seeded argv-boundary fuzz: tests/run_argv_fuzz.sh
# and its helpers tests/helpers/argv_fuzz.sh and tests/helpers/argv_fuzz.awk.
# One fixed seed runs the real ./zxfer against the fake zfs in every mode (the
# argv-fuzz CI job runs a fresh seed on every push); the self-tests prove that
# an injected quoting bug is reported.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"
# shellcheck source=tests/helpers/argv_fuzz.sh
. "$TESTS_DIR/helpers/argv_fuzz.sh"

FUZZ_BIN="$ZXFER_ROOT/tests/run_argv_fuzz.sh"

# Purpose: Run the fuzz runner with its work directory inside CASE_DIR; the
# combined output lands in FUZZ_OUT and the status in FUZZ_STATUS.
# Usage: fuzz_run [runner-arg...]
# shellcheck disable=SC2329  # Called from the tests shunit2 invokes.
fuzz_run() {
	FUZZ_OUT=$TEST_TMPDIR/fuzz.out
	FUZZ_STATUS=0
	TMPDIR=$CASE_DIR sh "$FUZZ_BIN" "$@" >"$FUZZ_OUT" 2>&1 || FUZZ_STATUS=$?
}

# Purpose: Fail unless the last fuzz_run output holds TEXT (grep -F, so
# backslashes in the output stay literal).
# Usage: fuzz_assert_output_has MESSAGE TEXT
# shellcheck disable=SC2329  # Called from the tests shunit2 invokes.
fuzz_assert_output_has() {
	grep -F -e "$2" "$FUZZ_OUT" >/dev/null ||
		fail "$1: missing [$2] in: $(cat "$FUZZ_OUT")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_a_fixed_seed_passes_in_every_mode() {
	fuzz_run --seed 1 --iterations 2
	assertEquals "seed 1 should pass: $(cat "$FUZZ_OUT")" 0 "$FUZZ_STATUS"
	assertEquals "the seed line comes first" "seed=1" "$(sed -n '1p' "$FUZZ_OUT")"
	fuzz_assert_output_has "every mode ran for both cases" \
		"seed=1 cases=2 runs=14 failed_cases=0"
	assertEquals "the work directory is removed" "" "$(ls "$CASE_DIR")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_split_arguments_are_reported_with_a_reproduction() {
	ZXFER_ARGV_FUZZ_FAULT="split"
	export ZXFER_ARGV_FUZZ_FAULT
	fuzz_run --seed 1 --iterations 1
	unset ZXFER_ARGV_FUZZ_FAULT

	assertEquals "a split argument must fail the fuzz" 1 "$FUZZ_STATUS"
	fuzz_assert_output_has "the failing case is named" "FAIL seed=1 case=1"
	fuzz_assert_output_has "the fake zfs names the broken operand" \
		"operand is not a generated name"
	fuzz_assert_output_has "the recorded argv is shown" "zfs argv, one call per line"
	fuzz_assert_output_has "the run prints a reproduction command" \
		"reproduce: ./tests/run_argv_fuzz.sh --seed 1 --case 1 --iterations 1 --keep"
	fuzz_assert_output_has "the summary counts the failed case" \
		"seed=1 cases=1 runs=7 failed_cases=1"
}

# A value that loses its last byte still replicates, so only the argv check
# can see it.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_a_changed_property_value_is_reported_although_zxfer_succeeds() {
	ZXFER_ARGV_FUZZ_FAULT="truncate"
	export ZXFER_ARGV_FUZZ_FAULT
	fuzz_run --seed 1 --iterations 1
	unset ZXFER_ARGV_FUZZ_FAULT

	assertEquals "a changed value must fail the fuzz" 1 "$FUZZ_STATUS"
	fuzz_assert_output_has "the value zfs got is compared byte for byte" "]: got ["
	assertFalse "zxfer itself succeeded" "grep -q 'zxfer exited' '$FUZZ_OUT'"
	fuzz_assert_output_has "only the property modes fail" "case 1: FAILED in P L j O T"
}

# A property no source holds, such as one a record-shaped value could
# smuggle in, fails the case even when its value is valid.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_an_injected_native_property_is_reported() {
	ZXFER_ARGV_FUZZ_FAULT="inject"
	export ZXFER_ARGV_FUZZ_FAULT
	fuzz_run --seed 1 --iterations 1
	unset ZXFER_ARGV_FUZZ_FAULT

	assertEquals "an injected property must fail the fuzz" 1 "$FUZZ_STATUS"
	fuzz_assert_output_has "the injected pair is named" "sets [readonly=on] on ["
	fuzz_assert_output_has "only the property modes fail" "case 1: FAILED in P L j O T"
}

# Seed 1 case 1 keeps its values on one line, so mode P reads the tree
# through the recursive prefetch. A prefetch that reports local sources as
# received breaks every mode that prefetches, and mode L, whose per-dataset
# reads are right, reports that its changes differ from mode P's.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_a_prefetch_that_disagrees_with_per_dataset_reads_is_reported() {
	ZXFER_ARGV_FUZZ_FAULT="prefetch"
	export ZXFER_ARGV_FUZZ_FAULT
	fuzz_run --seed 1 --iterations 1
	unset ZXFER_ARGV_FUZZ_FAULT

	assertEquals "a lying prefetch must fail the fuzz" 1 "$FUZZ_STATUS"
	fuzz_assert_output_has "the case reads through the prefetch" "(one line each)"
	fuzz_assert_output_has "mode L names the first differing mutation" \
		"per-dataset reads (L) and the recursive prefetch (P) differ at mutation 1"
	fuzz_assert_output_has "only the property modes fail" "case 1: FAILED in P L j O T"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_a_seed_and_case_always_generate_the_same_case() {
	argv_fuzz_generate_case 7 3 "$CASE_DIR/a" || fail "generation failed"
	argv_fuzz_generate_case 7 3 "$CASE_DIR/b" || fail "generation failed"
	argv_fuzz_generate_case 7 4 "$CASE_DIR/c" || fail "generation failed"

	for l_file in model operands expect summary; do
		assertTrue "$l_file must be identical for the same seed and case" \
			"cmp -s '$CASE_DIR/a/$l_file' '$CASE_DIR/b/$l_file'"
	done
	assertFalse "another case must differ" "cmp -s '$CASE_DIR/a/model' '$CASE_DIR/c/model'"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_every_case_holds_each_required_hostile_byte() {
	for l_case in 1 2 3 4 5; do
		argv_fuzz_generate_case 11 "$l_case" "$CASE_DIR/$l_case" ||
			fail "generation failed"
		# The first fz1 row, on the dataset where fz1 starts, holds all of
		# them; the model stores it escaped, a backslash as \\ and other
		# bytes as \ooo.
		# A one-line case turns each LF into a TAB.
		l_value=$(awk -F '\t' '$1 == "P" && $3 ~ /^fz1:/ { print $4; exit }' \
			"$CASE_DIR/$l_case/model")
		# Hide escaped backslashes, so the text "\\001" cannot pass for \001.
		l_bytes=$(printf '%s\n' "$l_value" | sed 's/\\\\/~/g')
		# shellcheck disable=SC1003  # literal escaped pieces
		l_lf='\012'
		if grep -q 'one line each' "$CASE_DIR/$l_case/summary"; then
			# shellcheck disable=SC1003  # literal escaped pieces
			l_lf='\011'
			case $l_bytes in
			*'\012'*) fail "one-line case $l_case holds a LF in [$l_value]" ;;
			esac
		fi
		# shellcheck disable=SC1003,SC2016  # literal escaped pieces
		for l_piece in '\011' "$l_lf" '\015' '\001' "'" '"' '\\' '$(' '`' '%' ',' '='; do
			l_haystack=$l_value
			case $l_piece in \\0*) l_haystack=$l_bytes ;; esac
			case $l_haystack in
			*"$l_piece"*) ;;
			*) fail "case $l_case lacks [$l_piece] in [$l_value]" ;;
			esac
		done
		# A line that ends like a record (TAB, source word, LF) and starts
		# the next one with a name and a TAB.
		# shellcheck disable=SC1003  # literal escaped pieces
		case $l_bytes in
		*'\011local'"$l_lf"*'\011'* | *'\011-'"$l_lf"*'\011'* | \
			*'\011default'"$l_lf"*'\011'* | *'\011received'"$l_lf"*'\011'* | \
			*'\011inherited from '*"$l_lf"*'\011'*) ;;
		*) fail "case $l_case lacks a record-shaped line in [$l_value]" ;;
		esac
	done
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_fake_zfs_answers_whole_names_and_flags_split_ones() {
	argv_fuzz_generate_case 3 1 "$CASE_DIR/case" || fail "generation failed"
	mkdir "$CASE_DIR/state" || fail "Unable to create the state directory."
	cp "$CASE_DIR/case/model" "$CASE_DIR/state/model" || fail "Unable to stage the model."
	argv_fuzz_write_mockbin "$CASE_DIR/mockbin" "$CASE_DIR/state" ||
		fail "Unable to write the mock bin."
	l_source=$(sed -n '1p' "$CASE_DIR/case/operands")

	assertEquals "a whole name is listed" "$l_source" \
		"$("$CASE_DIR/mockbin/zfs" list -H -o name "$l_source")"
	assertFalse "no violation for a whole name" "[ -s '$CASE_DIR/state/violations' ]"

	"$CASE_DIR/mockbin/zfs" list -H -o name "${l_source%/*}" "${l_source##*/}" \
		>/dev/null 2>&1
	assertNotEquals "a name cut at a boundary does not exist" 0 $?
	assertContains "the cut operand is recorded" \
		"$(cat "$CASE_DIR/state/violations")" "operand is not a generated name"
	assertEquals "each call is logged on one line" 2 \
		"$(awk 'END { print NR }' "$CASE_DIR/state/argv.log")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_bad_arguments_are_usage_errors() {
	for l_args in "--seed abc" "--iterations 0" "--case" "--bogus"; do
		# shellcheck disable=SC2086  # one word per option on purpose
		fuzz_run $l_args
		assertEquals "[$l_args] must be a usage error: $(cat "$FUZZ_OUT")" 2 "$FUZZ_STATUS"
		fuzz_assert_output_has "[$l_args] prints the usage" "Usage: tests/run_argv_fuzz.sh"
	done
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
