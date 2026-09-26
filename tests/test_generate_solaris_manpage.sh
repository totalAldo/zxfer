#!/bin/sh
#
# shunit2 tests for deterministic Solaris/illumos man-page generation.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_generate_solaris_manpage"
	MANPAGE_GENERATOR="$ZXFER_ROOT/tests/generate_solaris_manpage.sh"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_solaris_manpage_generator_changes_only_the_rendering_contract() {
	canonical="$TEST_TMPDIR/canonical.8"
	cat >"$canonical" <<'EOF'
.\" Canonical zxfer command reference; edit this file only.
.TH "ZXFER" "8" "July 17, 2026" "zxfer" "System Manager's Manual"
.SH "NAME"
zxfer \- fixture
EOF

	output=$(
		ZXFER_MANPAGE_SOURCE="$canonical" \
			"$MANPAGE_GENERATOR" --stdout
	)

	assertContains "The generated page should identify its canonical source." \
		"$output" "Generated from man/zxfer.8"
	assertContains "The generator should preserve the canonical date while selecting Solaris section and title fields." \
		"$output" '.TH "ZXFER" "1M" "July 17, 2026" "zxfer" "System Administration Commands"'
	assertContains "Body content should pass through unchanged." \
		"$output" 'zxfer \- fixture'
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_checked_in_solaris_manpage_matches_the_canonical_rendering() {
	"$MANPAGE_GENERATOR" --check

	assertEquals "The checked-in Solaris/illumos page should be reproducible from canonical man/zxfer.8." \
		0 "$?"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_solaris_manpage_write_publishes_a_world_readable_file() {
	canonical="$TEST_TMPDIR/write-canonical.8"
	destination="$TEST_TMPDIR/write-generated.1m"
	cat >"$canonical" <<'EOF'
.\" Canonical zxfer command reference; edit this file only.
.TH "ZXFER" "8" "July 17, 2026" "zxfer" "System Manager's Manual"
.SH "NAME"
zxfer \- fixture
EOF

	(
		umask 077
		ZXFER_MANPAGE_SOURCE="$canonical" \
			ZXFER_MANPAGE_DESTINATION="$destination" \
			"$MANPAGE_GENERATOR" --write
	)

	assertEquals "Generated packaging documentation should remain readable by non-owner users even under a restrictive caller umask." \
		"644" "$(zxfer_get_path_mode_octal "$destination")"
	ZXFER_MANPAGE_SOURCE="$canonical" \
		ZXFER_MANPAGE_DESTINATION="$destination" \
		"$MANPAGE_GENERATOR" --check
	assertEquals "The published file should still satisfy deterministic regeneration." 0 "$?"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
