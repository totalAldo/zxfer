#!/bin/sh
#
# shunit2 entry point for src/zxfer_property_transfer.sh: the readonly list,
# the -o reader, the -U scan, the property plan (derive and diff), source
# collection, destination creation, set/inherit commands, child inheritance,
# apply, and zxfer_transfer_properties end to end.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# Cases that render backup metadata opt in to that fixture.
# shellcheck source=tests/helpers/backup_fixtures.sh
. "$TESTS_DIR/helpers/backup_fixtures.sh"
# shellcheck source=tests/helpers/property_fixtures.sh
. "$TESTS_DIR/helpers/property_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_property_transfer"
	zxfer_test_property_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_property_fixture_setup
}

# Each fragment has a "zxfer-test-fragment" marker, a source line and a path
# in suite(); keep the three in the same order so listing and execution agree.
# zxfer-test-fragment: suites/zxfer_property_transfer_policy_tests.sh
# shellcheck source=tests/suites/zxfer_property_transfer_policy_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_policy_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_transfer_plan_tests.sh
# shellcheck source=tests/suites/zxfer_property_transfer_plan_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_plan_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_transfer_apply_tests.sh
# shellcheck source=tests/suites/zxfer_property_transfer_apply_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_apply_tests.sh"
# zxfer-test-fragment: suites/zxfer_property_transfer_pass_tests.sh
# shellcheck source=tests/suites/zxfer_property_transfer_pass_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_pass_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_property_transfer.sh" \
		"$TESTS_DIR/suites/zxfer_property_transfer_policy_tests.sh" \
		"$TESTS_DIR/suites/zxfer_property_transfer_plan_tests.sh" \
		"$TESTS_DIR/suites/zxfer_property_transfer_apply_tests.sh" \
		"$TESTS_DIR/suites/zxfer_property_transfer_pass_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
