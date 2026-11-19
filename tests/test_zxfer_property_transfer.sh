#!/bin/sh
#
# shunit2 entry point for src/zxfer_property_transfer.sh: its pure helpers
# (the readonly list, the -o reader and check, the child filter and
# inheritance awk) and the branches the black-box suites cannot reach (the
# -U scan edge cases, create-metadata probes, create guards, dry runs, awk
# failures). The plan, creates, sets, inherits and their failures are pinned
# black-box in tests/test_contract_properties.sh.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/property_fixtures.sh
. "$TESTS_DIR/helpers/property_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_property_transfer"
	zxfer_test_property_fixture_one_time_setup
}

setUp() {
	zxfer_test_property_fixture_setup
}

# Literal fragment source lines define loading, registration and discovery order.
# shellcheck source=tests/suites/zxfer_property_transfer_policy_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_policy_tests.sh"
# shellcheck source=tests/suites/zxfer_property_transfer_plan_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_plan_tests.sh"
# shellcheck source=tests/suites/zxfer_property_transfer_apply_tests.sh
. "$TESTS_DIR/suites/zxfer_property_transfer_apply_tests.sh"

suite() {
	zxfer_test_register_suite_tests "$0"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
