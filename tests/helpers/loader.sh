#!/bin/sh
# Shared module-loading and behavior-fragment registration helpers.
# shellcheck disable=SC1090,SC2154,SC2317,SC2329

# shellcheck source=tests/helpers/test_catalog.sh
. "$ZXFER_ROOT/tests/helpers/test_catalog.sh"

# Purpose: Load every src module into the test shell, in manifest order.
# Usage: zxfer_source_modules_for_tests ROOT
# Loading again restores every src function a test replaced.
zxfer_source_modules_for_tests() {
	ZXFER_SOURCE_MODULES_ROOT=$1
	export ZXFER_SOURCE_MODULES_ROOT

	# shellcheck source=src/zxfer_modules.sh
	. "$1/src/zxfer_modules.sh"
	zxfer_load_modules
}

# Purpose: Register entry and fragment tests with shunit2 in source order.
# Usage: zxfer_test_register_suite_tests ENTRY; call it from suite().
# Names are validated before suite_addTest and never evaluated. Exit on failure:
# shunit2 ignores the suite hook status and otherwise runs only entry tests.
zxfer_test_register_suite_tests() {
	l_fragment_test_names=$(zxfer_test_suite_names "$1") || exit 1
	for l_fragment_test_name in $l_fragment_test_names; do
		suite_addTest "$l_fragment_test_name"
	done
}

# Purpose: Succeed when the running test is defined in FILE, so an entry's
# setUp can add the fixture that one behavior fragment was written for.
# Usage: zxfer_test_running_test_is_in FILE; call it from setUp. It reads
# shunit2's current test name, which holds only [A-Za-z0-9_] characters.
zxfer_test_running_test_is_in() {
	case "${_shunit_test_:-}" in
	test[A-Za-z0-9_]*) ;;
	*) return 1 ;;
	esac
	grep -q "^${_shunit_test_}()" "$1"
}
