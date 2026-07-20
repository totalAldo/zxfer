#!/bin/sh
# Validated integration fragment loading plus test and group ordering.

zxfer_integration_registry_path() {
	printf '%s\n' "${ZXFER_INTEGRATION_REGISTRY_FILE:-$INTEGRATION_TESTS_DIR/integration_test_registry.tsv}"
}

zxfer_integration_fragment_manifest_path() {
	printf '%s\n' "${ZXFER_INTEGRATION_FRAGMENT_MANIFEST_FILE:-$INTEGRATION_TESTS_DIR/integration_fragment_manifest.tsv}"
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

zxfer_validate_integration_fragment_manifest_file() {
	l_fragment_manifest=${1:-$(zxfer_integration_fragment_manifest_path)}

	if [ ! -f "$l_fragment_manifest" ] || [ ! -r "$l_fragment_manifest" ]; then
		printf 'Invalid integration fragment manifest [%s]: file is not readable.\n' \
			"$l_fragment_manifest" >&2
		return 1
	fi

	awk -v manifest="$l_fragment_manifest" '
		function fail(message) {
			if (!failed) {
				printf "Invalid integration fragment manifest [%s]: %s\n", manifest, message
			}
			failed = 1
			exit 1
		}
		NR == 1 {
			if ($0 != "# path") {
				fail("header does not match the one-field manifest schema")
			}
			next
		}
		{
			if ($0 == "") {
				fail("blank data rows are not allowed")
			}
			if ($0 !~ /^integration\/[a-z][a-z0-9_]*_tests[.]sh$/) {
				fail("line " NR " has an invalid fragment path")
			}
			if ($0 in paths) {
				fail("line " NR " duplicates fragment [" $0 "]")
			}
			paths[$0] = 1
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
				fail("manifest contains no fragments")
			}
		}
	' <"$l_fragment_manifest" >&2 || return 1

	l_fragment_manifest_paths=$(awk 'NR > 1 { print }' "$l_fragment_manifest") || return 1
	l_fragment_manifest_integration_dir=$INTEGRATION_TESTS_DIR/integration
	if [ -L "$l_fragment_manifest_integration_dir" ] ||
		[ -h "$l_fragment_manifest_integration_dir" ]; then
		printf 'Invalid integration fragment manifest [%s]: integration directory must not be a symbolic link.\n' \
			"$l_fragment_manifest" >&2
		return 1
	fi
	l_fragment_manifest_physical_dir=$(cd -P \
		"$l_fragment_manifest_integration_dir" 2>/dev/null && pwd -P) || {
		printf 'Invalid integration fragment manifest [%s]: integration directory is not accessible.\n' \
			"$l_fragment_manifest" >&2
		return 1
	}
	while IFS= read -r l_fragment_manifest_relative_path; do
		[ -n "$l_fragment_manifest_relative_path" ] || continue
		l_fragment_manifest_file=$INTEGRATION_TESTS_DIR/$l_fragment_manifest_relative_path
		if [ ! -f "$l_fragment_manifest_file" ] || [ ! -r "$l_fragment_manifest_file" ]; then
			printf 'Invalid integration fragment manifest [%s]: fragment [%s] is not readable.\n' \
				"$l_fragment_manifest" "$l_fragment_manifest_relative_path" >&2
			return 1
		fi
		if [ -L "$l_fragment_manifest_file" ] || [ -h "$l_fragment_manifest_file" ]; then
			printf 'Invalid integration fragment manifest [%s]: fragment [%s] must not be a symbolic link.\n' \
				"$l_fragment_manifest" "$l_fragment_manifest_relative_path" >&2
			return 1
		fi
		l_fragment_manifest_parent=$(dirname "$l_fragment_manifest_file") || return 1
		l_fragment_manifest_physical_parent=$(cd -P \
			"$l_fragment_manifest_parent" 2>/dev/null && pwd -P) || return 1
		if [ "$l_fragment_manifest_physical_parent" != \
			"$l_fragment_manifest_physical_dir" ]; then
			printf 'Invalid integration fragment manifest [%s]: fragment [%s] is outside the approved integration directory.\n' \
				"$l_fragment_manifest" "$l_fragment_manifest_relative_path" >&2
			return 1
		fi
	done <<-EOF
		$l_fragment_manifest_paths
	EOF
}

zxfer_validate_integration_fragment_contents() {
	l_fragment_contents_paths=$(zxfer_integration_fragment_paths) || return 1
	l_fragment_contents_extractor=$INTEGRATION_TESTS_DIR/measure_shell_complexity.awk
	if [ ! -f "$l_fragment_contents_extractor" ] ||
		[ ! -r "$l_fragment_contents_extractor" ]; then
		printf 'Unable to inspect integration fragment contents: extractor [%s] is not readable.\n' \
			"$l_fragment_contents_extractor" >&2
		return 1
	fi
	while IFS= read -r l_fragment_contents_relative_path; do
		[ -n "$l_fragment_contents_relative_path" ] || continue
		awk -v definitions_only=1 -f "$l_fragment_contents_extractor" \
			"$INTEGRATION_TESTS_DIR/$l_fragment_contents_relative_path" >&2 ||
			return "$?"
	done <<-EOF
		$l_fragment_contents_paths
	EOF
}

zxfer_integration_fragment_paths() {
	l_fragment_paths_manifest=$(zxfer_integration_fragment_manifest_path) || return 1
	zxfer_validate_integration_fragment_manifest_file "$l_fragment_paths_manifest" || return 1
	awk 'NR > 1 { print }' "$l_fragment_paths_manifest"
}

zxfer_integration_fragment_files() {
	l_fragment_files_paths=$(zxfer_integration_fragment_paths) || return 1
	while IFS= read -r l_fragment_files_relative_path; do
		[ -n "$l_fragment_files_relative_path" ] || continue
		printf '%s/%s\n' "$INTEGRATION_TESTS_DIR" "$l_fragment_files_relative_path"
	done <<-EOF
		$l_fragment_files_paths
	EOF
}

zxfer_integration_fragment_definition_rows() {
	l_fragment_definition_paths=$(zxfer_integration_fragment_paths) || return 1
	l_fragment_definition_tab=$(printf '\t')
	l_fragment_definition_extractor=$INTEGRATION_TESTS_DIR/measure_shell_complexity.awk
	if [ ! -f "$l_fragment_definition_extractor" ] ||
		[ ! -r "$l_fragment_definition_extractor" ]; then
		printf 'Unable to inspect integration fragment definitions: extractor [%s] is not readable.\n' \
			"$l_fragment_definition_extractor" >&2
		return 1
	fi
	while IFS= read -r l_fragment_definition_relative_path; do
		[ -n "$l_fragment_definition_relative_path" ] || continue
		l_fragment_definition_metrics=$(awk -v headers_only=1 \
			-f "$l_fragment_definition_extractor" \
			"$INTEGRATION_TESTS_DIR/$l_fragment_definition_relative_path") || return "$?"
		printf '%s\n' "$l_fragment_definition_metrics" |
			awk -F "$l_fragment_definition_tab" \
				-v relative_path="$l_fragment_definition_relative_path" \
				'NF >= 2 { printf "%s\t%s\n", $2, relative_path }' || return "$?"
	done <<-EOF
		$l_fragment_definition_paths
	EOF
}

zxfer_validate_integration_registry_definitions() {
	l_definition_registry=${1:-$(zxfer_integration_registry_path)}
	l_definition_tab=$(printf '\t')

	zxfer_validate_integration_registry_file "$l_definition_registry" || return 1
	zxfer_validate_integration_fragment_contents || return 1
	l_definition_rows=$(zxfer_integration_fragment_definition_rows) || return 1

	awk -F "$l_definition_tab" '
		FILENAME == ARGV[1] {
			if (FNR == 1) {
				next
			}
			registry[$1] = 1
			registry_order[++registry_count] = $1
			next
		}
		FILENAME == "-" && NF == 2 {
			definition_count[$1]++
		}
		END {
			for (i = 1; i <= registry_count; i++) {
				name = registry_order[i]
				if (!(name in definition_count)) {
					printf "Invalid integration test registry: function [%s] is not defined by a manifest-listed integration fragment.\n", name
					failed = 1
				} else if (definition_count[name] != 1) {
					printf "Invalid integration test registry: function [%s] is defined by multiple integration fragments.\n", name
					failed = 1
				}
			}
			for (name in definition_count) {
				if (!(name in registry)) {
					printf "Invalid integration test registry: fragment function [%s] is not listed in the registry.\n", name
					failed = 1
				}
			}
			exit failed
		}
	' "$l_definition_registry" - >&2 <<-EOF
		$l_definition_rows
	EOF
}

zxfer_integration_shell_function_p() {
	l_function_name=$1
	l_function_description=$(LC_ALL=C command -V "$l_function_name" 2>/dev/null) ||
		return 1
	case "$l_function_description" in
	"$l_function_name is a function"* | "$l_function_name is a shell function"*)
		return 0
		;;
	esac
	return 1
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

zxfer_load_integration_test_fragments() {
	l_fragment_load_registry=$(zxfer_integration_registry_path) || return 1
	zxfer_validate_integration_registry_definitions "$l_fragment_load_registry" || return 1
	l_fragment_load_paths=$(zxfer_integration_fragment_paths) || return 1

	while IFS= read -r l_fragment_load_relative_path; do
		[ -n "$l_fragment_load_relative_path" ] || continue
		# shellcheck source=/dev/null
		. "$INTEGRATION_TESTS_DIR/$l_fragment_load_relative_path" || return $?
	done <<-EOF
		$l_fragment_load_paths
	EOF
}

zxfer_validate_integration_registry() {
	l_registry_path=$(zxfer_integration_registry_path) || return 1
	zxfer_validate_integration_registry_definitions "$l_registry_path" || return 1
	l_registry_names=$(zxfer_integration_registry_names) || return 1

	for l_registry_name in $l_registry_names; do
		# The static definition pass above rejects missing, duplicated, and
		# unlisted functions, so command -v now checks only that sourcing made the
		# exact registered function callable in this shell.
		if ! zxfer_integration_shell_function_p "$l_registry_name"; then
			printf 'Invalid integration test registry: function [%s] is not defined.\n' \
				"$l_registry_name" >&2
			return 1
		fi
	done
}
