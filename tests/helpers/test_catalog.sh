#!/bin/sh
# Shared suite path resolution and static test discovery.
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

# Purpose: Discover test names without sourcing or executing the suite.
# Usage: zxfer_test_suite_names SUITE
zxfer_test_suite_names() {
	[ -f "$1" ] || return 1
	LC_ALL=C awk '
		/^test[A-Za-z0-9_]*\(\)[[:space:]]*\{/ {
			name = $0
			sub(/\(.*/, "", name)
			print name
		}
	' "$1"
}
