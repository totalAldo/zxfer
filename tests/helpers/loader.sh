#!/bin/sh
# Shared module-loading and behavior-fragment registration helpers.
# shellcheck disable=SC1090,SC2154,SC2317,SC2329

zxfer_source_dependency_modules_for_tests() {
	l_root=$1

	ZXFER_SOURCE_MODULES_ROOT=$l_root
	export ZXFER_SOURCE_MODULES_ROOT

	# shellcheck source=src/zxfer_modules.sh
	. "$l_root/src/zxfer_modules.sh"
	zxfer_load_modules zxfer_dependencies.sh
	zxfer_initialize_dependency_defaults
}

zxfer_source_modules_for_tests() {
	l_root=$1
	l_last_module=$2

	ZXFER_SOURCE_MODULES_ROOT=$l_root
	export ZXFER_SOURCE_MODULES_ROOT

	# shellcheck source=src/zxfer_modules.sh
	. "$l_root/src/zxfer_modules.sh"
	zxfer_load_modules "$l_last_module"
}

zxfer_source_runtime_modules_through() {
	l_last_module=$1
	l_root=${2:-$ZXFER_ROOT}

	zxfer_source_modules_for_tests "$l_root" "$l_last_module"
}

# Register every test function defined in sourced behavior fragments with
# shunit2. Shunit2 otherwise scans only the stable suite entry file during an
# unfiltered run. Names are validated before suite_addTest and never evaluated.
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
