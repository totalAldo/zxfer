#!/bin/sh
# Shared module-loading helper for unit suites.
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
