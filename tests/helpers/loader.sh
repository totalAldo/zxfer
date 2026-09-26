#!/bin/sh
# Shared module-loading and behavior-fragment registration helpers.
# shellcheck disable=SC1090,SC2154,SC2317,SC2329

# Purpose: Load every src module into the test shell, in manifest order.
# Usage: zxfer_source_modules_for_tests ROOT [BOUNDARY]; BOUNDARY is accepted
# for older callers and ignored, so every test sees the complete module set.
# Loading again restores every src function a test replaced.
zxfer_source_modules_for_tests() {
	ZXFER_SOURCE_MODULES_ROOT=$1
	export ZXFER_SOURCE_MODULES_ROOT

	# shellcheck source=src/zxfer_modules.sh
	. "$1/src/zxfer_modules.sh"
	zxfer_load_modules
}

# Purpose: Reload every src module, for callers written against the old
# partial-load boundaries; new code calls zxfer_source_modules_for_tests.
# Usage: zxfer_source_runtime_modules_through [BOUNDARY] [ROOT]
zxfer_source_runtime_modules_through() {
	zxfer_source_modules_for_tests "${2:-$ZXFER_ROOT}"
}

# Purpose: Register the test functions of each file with shunit2, in file
# order. Shunit2 otherwise scans only the entry file it was started from.
# Usage: zxfer_test_register_fragment_tests FILE...; call it from suite().
# Names are validated before suite_addTest and never evaluated.
zxfer_test_register_fragment_tests() {
	for l_fragment_file in "$@"; do
		[ -r "$l_fragment_file" ] || {
			echo "Missing test behavior fragment: $l_fragment_file" >&2
			return 1
		}
		# shellcheck disable=SC2016  # $0 is evaluated by awk, not the shell.
		l_fragment_test_names=$("${g_cmd_awk:-awk}" '
			/^test[A-Za-z0-9_]*\(\)[[:space:]]*\{/ {
				name = $0
				sub(/\(.*/, "", name)
				print name
			}
		' "$l_fragment_file") || return 1
		for l_fragment_test_name in $l_fragment_test_names; do
			case "$l_fragment_test_name" in
			test[A-Za-z0-9_]*) ;;
			*)
				echo "Invalid test function in fragment $l_fragment_file: $l_fragment_test_name" >&2
				return 1
				;;
			esac
			suite_addTest "$l_fragment_test_name"
		done
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
