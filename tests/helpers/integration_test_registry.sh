#!/bin/sh
# Validated integration test and group ordering without dynamic shell execution.

zxfer_integration_registry_path() {
	printf '%s\n' "${ZXFER_INTEGRATION_REGISTRY_FILE:-$INTEGRATION_TESTS_DIR/integration_test_registry.tsv}"
}

zxfer_validate_integration_registry_file() {
	l_registry=${1:-$(zxfer_integration_registry_path)}
	l_tab=$(printf '\t')

	if [ ! -f "$l_registry" ] || [ ! -r "$l_registry" ]; then
		printf 'Invalid integration test registry [%s]: file is not readable.\n' "$l_registry" >&2
		return 1
	fi

	awk -F "$l_tab" -v registry="$l_registry" '
		function fail(message) {
			if (!failed) {
				printf "Invalid integration test registry [%s]: %s\n", registry, message
			}
			failed = 1
			exit 1
		}
		BEGIN {
			expected = "# name" FS "kind" FS "pre_pool"
		}
		NR == 1 {
			if ($0 != expected) {
				fail("header does not match the 3-field registry schema")
			}
			next
		}
		{
			if ($0 == "") {
				fail("blank data rows are not allowed")
			}
			if (NF != 3) {
				fail("line " NR " has " NF " fields; expected 3")
			}
			if ($1 !~ /^[a-z][a-z0-9_]*$/) {
				fail("line " NR " has an invalid function name")
			}
			if ($2 != "test" && $2 != "group") {
				fail("line " NR " has an invalid registry kind")
			}
			if ($3 != "yes" && $3 != "no") {
				fail("line " NR " has an invalid pre-pool marker")
			}
			if ($1 in names) {
				fail("line " NR " duplicates function [" $1 "]")
			}
			names[$1] = 1
			if ($3 == "yes") {
				pre_pool_count++
			}
			row_count++
		}
		END {
			if (failed) {
				exit 1
			}
			if (NR == 0) {
				fail("file is empty")
			}
			if (row_count == 0) {
				fail("registry contains no test or group rows")
			}
			if (pre_pool_count == 0) {
				fail("registry contains no pre-pool checks")
			}
		}
	' <"$l_registry" >&2
}

zxfer_integration_registry_names() {
	l_registry=$(zxfer_integration_registry_path) || return 1
	l_tab=$(printf '\t')

	zxfer_validate_integration_registry_file "$l_registry" || return 1
	awk -F "$l_tab" 'NR > 1 { print $1 }' <"$l_registry"
}

zxfer_integration_registry_pre_pool_names() {
	l_registry=$(zxfer_integration_registry_path) || return 1
	l_tab=$(printf '\t')

	zxfer_validate_integration_registry_file "$l_registry" || return 1
	awk -F "$l_tab" 'NR > 1 && $3 == "yes" { print $1 }' <"$l_registry"
}

zxfer_integration_harness_defines_function() {
	l_registry_function_name=$1
	l_registry_harness=$INTEGRATION_TESTS_DIR/run_integration_zxfer.sh

	[ -r "$l_registry_harness" ] || return 1
	awk -v function_name="$l_registry_function_name" '
		$0 == function_name "() {" { found = 1; exit }
		END { exit !found }
	' "$l_registry_harness"
}

zxfer_validate_integration_registry() {
	l_registry_names=$(zxfer_integration_registry_names) || return 1

	for l_registry_name in $l_registry_names; do
		# command -v alone would accept an external executable with the same
		# name. Require the repository-controlled harness definition as well as
		# the function loaded into this shell before any registry entry can run.
		if ! zxfer_integration_harness_defines_function "$l_registry_name" ||
			! command -v "$l_registry_name" >/dev/null 2>&1; then
			printf 'Invalid integration test registry: function [%s] is not defined.\n' \
				"$l_registry_name" >&2
			return 1
		fi
	done
}
