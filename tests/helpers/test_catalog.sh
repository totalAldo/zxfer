#!/bin/sh
# Shared suite paths and static test discovery. Literal fragment source lines
# are the membership authority; discovery never executes a suite.
# shellcheck disable=SC2034,SC2154,SC2317,SC2329

# Purpose: Resolve the suite spellings accepted by unit and coverage runners.
# Usage: zxfer_test_resolve_suite_path ROOT SUITE
zxfer_test_resolve_suite_path() {
	case "$2" in
	/*) printf '%s\n' "$2" ;;
	tests/*) printf '%s/%s\n' "$1" "$2" ;;
	*) printf '%s/tests/%s\n' "$1" "$2" ;;
	esac
}

# Purpose: Print ENTRY<TAB>FRAGMENT rows in literal source order.
# Usage: zxfer_test_fragment_rows ENTRY...; paths are relative to each entry.
zxfer_test_fragment_rows() {
	# shellcheck disable=SC2016 # Match the literal variable in source text.
	LC_ALL=C awk '
		/^[[:space:]]*[.][[:space:]]+[^#[:space:]]*\/suites\// {
			fragment = $0
			sub(/^[[:space:]]*[.][[:space:]]+/, "", fragment)
			if (fragment !~ /^"(\$TESTS_DIR|\$\{TESTS_DIR\})\/suites\/[^"[:space:]]+"([[:space:]]+#.*)?[[:space:]]*$/) {
				print "Invalid suite test fragment source: " FILENAME ":" FNR > "/dev/stderr"
				bad = 1
				next
			}
			sub(/^"(\$TESTS_DIR|\$\{TESTS_DIR\})\//, "", fragment)
			sub(/"[[:space:]]*(#.*)?$/, "", fragment)
			printf "%s\t%s\n", FILENAME, fragment
		}
		END { exit bad }
	' "$@"
}

# Purpose: Discover test names without sourcing the suite or its fragments.
# Usage: zxfer_test_suite_names ENTRY; entry tests precede fragment tests.
zxfer_test_suite_names() {
	l_catalog_rows=$(zxfer_test_fragment_rows "$1") || return 1
	l_catalog_tab=$(printf '\t')
	while IFS="$l_catalog_tab" read -r l_catalog_entry l_catalog_fragment; do
		[ -n "$l_catalog_entry" ] || continue
		case "$l_catalog_fragment" in
		'' | /* | ../* | */../* | */.. | \
			*[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./-]*)
			echo "Invalid suite test fragment path: $l_catalog_fragment" >&2
			return 1
			;;
		esac
		case "$l_catalog_entry" in
		*/*) l_catalog_dir=${l_catalog_entry%/*} ;;
		*) l_catalog_dir=. ;;
		esac
		l_catalog_file=$l_catalog_dir/$l_catalog_fragment
		if [ ! -f "$l_catalog_file" ]; then
			echo "Missing suite test fragment: $l_catalog_file" >&2
			return 1
		fi
		set -- "$@" "$l_catalog_file"
	done <<EOF
$l_catalog_rows
EOF
	LC_ALL=C awk '
		/^test[A-Za-z0-9_]*\(\)[[:space:]]*\{/ {
			name = $0
			sub(/\(.*/, "", name)
			print name
		}
	' "$@"
}
