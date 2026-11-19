#!/bin/sh
# Validated integration fragment loading plus test and group ordering.

zxfer_integration_registry_path() {
	printf '%s\n' "${ZXFER_INTEGRATION_REGISTRY_FILE:-$INTEGRATION_TESTS_DIR/integration_test_registry.tsv}"
}

# Validate the registry and retain its two ordered name lists for this load.
zxfer_validate_integration_registry_file() {
	l_registry=${1:-$(zxfer_integration_registry_path)}
	l_tab=$(printf '\t')

	if [ ! -f "$l_registry" ] || [ ! -r "$l_registry" ]; then
		printf 'Invalid integration test registry [%s]: file is not readable.\n' "$l_registry" >&2
		return 1
	fi

	l_registry_sequences=$(LC_ALL=C awk -F "$l_tab" -v registry="$l_registry" '
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
			ordered[++row_count] = $1
			if ($3 == "yes")
				pre_pool[++pre_pool_count] = $1
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
			# A tab separates the two newline-delimited lists. Valid names
			# cannot contain tabs or newlines.
			for (i = 1; i <= row_count; i++)
				printf "%s%s", (i > 1 ? "\n" : ""), ordered[i]
			printf "\t"
			for (i = 1; i <= pre_pool_count; i++)
				printf "%s%s", (i > 1 ? "\n" : ""), pre_pool[i]
		}
	' <"$l_registry") || {
		printf '%s\n' "$l_registry_sequences" >&2
		return 1
	}
	g_integration_registry_names=${l_registry_sequences%%"$l_tab"*}
	g_integration_pre_pool_names=${l_registry_sequences#*"$l_tab"}
}

# Purpose: Print the integration fragments, integration/NAME_tests.sh, in C
# sort order. In any locale, NAME must be a lower-case ASCII letter followed
# by lower-case ASCII letters, digits or _. Each fragment must be a regular
# readable file and not a symbolic link, in an integration directory that is
# not a symbolic link; the first violation stops the harness before any pool
# work.
# Usage: zxfer_integration_fragment_paths; paths are relative to
# INTEGRATION_TESTS_DIR. It runs in a subshell, so the glob works whatever the
# caller's noglob setting and leaves that setting alone.
zxfer_integration_fragment_paths() (
	set +f
	l_fragment_dir=$INTEGRATION_TESTS_DIR/integration
	if [ -L "$l_fragment_dir" ] || [ -h "$l_fragment_dir" ]; then
		printf 'Invalid integration fragments [%s]: the directory must not be a symbolic link.\n' \
			"$l_fragment_dir" >&2
		return 1
	fi
	if ! [ -d "$l_fragment_dir" ]; then
		printf 'Invalid integration fragments [%s]: the directory is not accessible.\n' \
			"$l_fragment_dir" >&2
		return 1
	fi
	l_fragment_list=
	for l_fragment_file in "$l_fragment_dir"/*_tests.sh; do
		[ -e "$l_fragment_file" ] || [ -L "$l_fragment_file" ] || continue
		l_fragment_name=${l_fragment_file##*/}
		# The characters are spelled out because bash 3.2 (macOS /bin/sh)
		# matches a range such as [a-z] by locale collation: in a UTF-8
		# locale it also takes upper-case and accented letters.
		case "${l_fragment_name%_tests.sh}" in
		'' | [!abcdefghijklmnopqrstuvwxyz]* | *[!abcdefghijklmnopqrstuvwxyz0123456789_]*)
			printf 'Invalid integration fragments [%s]: fragment [%s] must be named like name_tests.sh in lower case.\n' \
				"$l_fragment_dir" "$l_fragment_name" >&2
			return 1
			;;
		esac
		if [ -L "$l_fragment_file" ] || [ -h "$l_fragment_file" ]; then
			printf 'Invalid integration fragments [%s]: fragment [%s] must not be a symbolic link.\n' \
				"$l_fragment_dir" "$l_fragment_name" >&2
			return 1
		fi
		if [ ! -f "$l_fragment_file" ] || [ ! -r "$l_fragment_file" ]; then
			printf 'Invalid integration fragments [%s]: fragment [%s] is not a readable file.\n' \
				"$l_fragment_dir" "$l_fragment_name" >&2
			return 1
		fi
		l_fragment_list="${l_fragment_list}integration/$l_fragment_name
"
	done
	if [ -z "$l_fragment_list" ]; then
		printf 'Invalid integration fragments [%s]: no NAME_tests.sh fragment found.\n' \
			"$l_fragment_dir" >&2
		return 1
	fi
	printf '%s' "$l_fragment_list" | LC_ALL=C sort
)

# Purpose: Scan one shfmt-formatted shell file for its top-level functions.
# Usage: zxfer_scan_integration_fragment headers|definitions FILE [RELATIVE_PATH]. headers
# prints "FILE<TAB>NAME<TAB>LINE" for each "name() {" header at column 0.
# definitions prints one "Invalid integration fragment" line and returns 1
# for anything else outside a function (a fragment must be definition-only),
# for a line that starts with "}" but holds more, for a name() pattern inside
# a function body (a nested definition) and for an unterminated function. A
# function ends at the first lone "}" in column 0; shfmt, which lint runs on
# every fragment, puts each function's closing brace there and nothing else.
# With RELATIVE_PATH, a valid definitions scan also emits NAME<TAB>RELATIVE_PATH.
# awk runs in the C locale, as the registry check does, so a range such as
# [A-Za-z_] means ASCII in every awk.
zxfer_scan_integration_fragment() {
	LC_ALL=C awk -v mode="$1" -v relative_path="${3:-}" '
		function report(message, line) {
			if (mode == "headers")
				return
			printf "Invalid integration fragment [%s]: %s at line %d.\n", FILENAME, message, line
			bad = 1
		}
		BEGIN {
			in_function = 0
			bad = 0
		}
		/^[ \t]*(#|$)/ {
			next
		}
		!in_function {
			if ($0 ~ /^[A-Za-z_][A-Za-z0-9_]*\(\) \{$/) {
				in_function = FNR
				name = $0
				sub(/\(.*/, "", name)
				if (mode == "headers")
					printf "%s\t%s\t%d\n", FILENAME, name, FNR
				else if (relative_path != "")
					names[++count] = name
			} else {
				report("executable top-level shell code", FNR)
			}
			next
		}
		$0 == "}" {
			in_function = 0
			next
		}
		/^}/ {
			report("code after a function closing brace", FNR)
			in_function = 0
			next
		}
		/(^|[;&|(){}[:space:]])[A-Za-z_][A-Za-z0-9_]*[ \t]*\([ \t]*\)/ {
			report("nested function definition", FNR)
		}
		END {
			if (in_function)
				report("unterminated function", in_function)
			if (!bad)
				for (i = 1; i <= count; i++)
					print names[i] "\t" relative_path
			exit bad
		}
	' "$2"
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
	l_fragment_definition_paths=${1:-$(zxfer_integration_fragment_paths)} || return 1
	while IFS= read -r l_fragment_definition_relative_path; do
		l_fragment_definition_rows=$(zxfer_scan_integration_fragment definitions \
			"$INTEGRATION_TESTS_DIR/$l_fragment_definition_relative_path" "$l_fragment_definition_relative_path") || {
			printf '%s\n' "$l_fragment_definition_rows" >&2
			return 1
		}
		[ -z "$l_fragment_definition_rows" ] || printf '%s\n' "$l_fragment_definition_rows"
	done <<-EOF
		$l_fragment_definition_paths
	EOF
}

zxfer_validate_integration_registry_definitions() {
	l_definition_registry=${1:-$(zxfer_integration_registry_path)}
	l_definition_tab=$(printf '\t')

	zxfer_validate_integration_registry_file "$l_definition_registry" || return 1
	g_integration_fragment_paths=$(zxfer_integration_fragment_paths) || return 1
	l_definition_rows=$(zxfer_integration_fragment_definition_rows "$g_integration_fragment_paths") || return 1

	awk -F "$l_definition_tab" '
		# Registry names occupy one field, fragment definitions two.
		NF == 1 {
			registry[$1] = 1
			registry_order[++registry_count] = $1
			next
		}
		NF == 2 {
			definition_count[$1]++
		}
		END {
			for (i = 1; i <= registry_count; i++) {
				name = registry_order[i]
				if (!(name in definition_count)) {
					printf "Invalid integration test registry: function [%s] is not defined by an integration fragment.\n", name
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
	' >&2 <<-EOF
		$g_integration_registry_names
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
	zxfer_validate_integration_registry_file || return 1
	printf '%s\n' "$g_integration_registry_names"
}

zxfer_integration_registry_pre_pool_names() {
	zxfer_validate_integration_registry_file || return 1
	printf '%s\n' "$g_integration_pre_pool_names"
}

zxfer_load_integration_test_fragments() {
	zxfer_validate_integration_registry_definitions || return 1
	while IFS= read -r l_fragment_load_relative_path; do
		# shellcheck source=/dev/null
		. "$INTEGRATION_TESTS_DIR/$l_fragment_load_relative_path" || return $?
	done <<-EOF
		$g_integration_fragment_paths
	EOF
}

zxfer_validate_integration_registry() {
	zxfer_validate_integration_registry_definitions || return 1
	zxfer_check_loaded_integration_functions
}

zxfer_check_loaded_integration_functions() {
	for l_registry_name in $g_integration_registry_names; do
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
