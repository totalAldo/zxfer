#!/bin/sh
#
# Anti-rebloat gate: check the sensitive-caller ratchets in
# tests/budget_policy.tsv.
#
# Each policy row is "callers<TAB>SYMBOL<TAB>MAX[<TAB>ALLOWED_FILES]". It
# counts the non-comment lines of src/*.sh and zxfer that contain SYMBOL
# literally and fails when the count exceeds MAX or, when ALLOWED_FILES (a
# comma-separated list of repo-relative paths) is given, when a match is in
# another file. Any other record kind fails closed.
#

set -eu

if [ -n "${ZXFER_BUDGET_ROOT:-}" ]; then
	ZXFER_ROOT=$(cd "$ZXFER_BUDGET_ROOT" && pwd)
else
	ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
fi
POLICY_FILE=$ZXFER_ROOT/tests/budget_policy.tsv
TAB=$(printf '\t')
VIOLATION_COUNT=0
ROW_COUNT=0

print_usage() {
	cat <<'EOF'
Usage: tests/run_budget_check.sh [--list]

Check the working tree against the ratchet-down-only sensitive-caller
budgets in tests/budget_policy.tsv.

Options:
  --list        print current measured values in policy format (for ratcheting)
  -h, --help    show this help
EOF
}

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

# Purpose: Print "file:line:text" for each non-comment line containing SYMBOL.
# Usage: measure_caller_matches SYMBOL
measure_caller_matches() {
	(
		cd "$ZXFER_ROOT"
		awk -v symbol="$1" '
			index($0, symbol) && $0 !~ /^[[:space:]]*#/ {
				printf "%s:%d:%s\n", FILENAME, FNR, $0
			}
		' src/*.sh zxfer
	)
}

# Purpose: Print the number of non-comment lines containing SYMBOL; a scan
# that cannot read src/*.sh or zxfer stops the gate.
# Usage: measure_caller_count SYMBOL
measure_caller_count() {
	l_matches=$(measure_caller_matches "$1") ||
		die "Cannot scan src/*.sh and zxfer under $ZXFER_ROOT."
	if [ -z "$l_matches" ]; then
		printf '0\n'
		return 0
	fi
	printf '%s\n' "$l_matches" | awk 'END { print NR }'
}

# Purpose: Print one violation row (and the table header before the first).
# Usage: report_violation KIND TARGET CURRENT MAX DETAIL
report_violation() {
	if [ "$VIOLATION_COUNT" -eq 0 ]; then
		printf '%-10s %-42s %8s %8s  %s\n' KIND TARGET CURRENT MAX DETAIL
	fi
	printf '%-10s %-42s %8s %8s  %s\n' "$1" "$2" "$3" "$4" "$5"
	VIOLATION_COUNT=$((VIOLATION_COUNT + 1))
}

# Purpose: Report each file with a SYMBOL match that ALLOWED does not list.
# Usage: check_caller_allowed_files SYMBOL ALLOWED
check_caller_allowed_files() {
	l_files=$(measure_caller_matches "$1" | cut -d: -f1 | sort -u)
	[ -n "$l_files" ] || return 0
	while IFS= read -r l_file; do
		case ",$2," in
		*",$l_file,"*) ;;
		*)
			report_violation callers "$1" - - "match outside allow-list: $l_file"
			;;
		esac
	done <<EOF
$l_files
EOF
}

# Purpose: Check every policy row and exit non-zero on any violation.
# Usage: run_check
run_check() {
	while IFS=$TAB read -r l_kind l_target l_max l_allowed; do
		case "$l_kind" in
		'' | '#'*)
			continue
			;;
		esac
		ROW_COUNT=$((ROW_COUNT + 1))
		if [ "$l_kind" != callers ]; then
			report_violation "$l_kind" "${l_target:--}" - "${l_max:--}" "unknown policy record kind"
			continue
		fi
		# A missing or non-numeric MAX would let the count check pass
		# vacuously. Tabs collapse on read, so an empty symbol also lands here.
		case "$l_max" in
		'' | *[!0-9]*)
			report_violation callers "${l_target:--}" - "${l_max:--}" "MAX must be a non-negative integer"
			continue
			;;
		esac
		l_current=$(measure_caller_count "$l_target")
		if [ "$l_current" -gt "$l_max" ]; then
			report_violation callers "$l_target" "$l_current" "$l_max" "over budget (ratchet-down-only)"
		fi
		if [ -n "$l_allowed" ]; then
			check_caller_allowed_files "$l_target" "$l_allowed"
		fi
	done <"$POLICY_FILE"

	if [ "$VIOLATION_COUNT" -gt 0 ]; then
		printf 'budget check failed: %s violation(s) across %s policy rows\n' "$VIOLATION_COUNT" "$ROW_COUNT" >&2
		exit 1
	fi
	printf 'budget check passed: %s policy rows within budget\n' "$ROW_COUNT"
}

# Purpose: Print every callers row with its measured count instead of MAX.
# Usage: print_list
print_list() {
	while IFS=$TAB read -r l_kind l_target l_max l_allowed; do
		[ "$l_kind" = callers ] || continue
		l_current=$(measure_caller_count "$l_target")
		if [ -n "$l_allowed" ]; then
			printf 'callers\t%s\t%s\t%s\n' "$l_target" "$l_current" "$l_allowed"
		else
			printf 'callers\t%s\t%s\n' "$l_target" "$l_current"
		fi
	done <"$POLICY_FILE"
}

MODE=check
for l_arg in "$@"; do
	case "$l_arg" in
	-h | --help)
		print_usage
		exit 0
		;;
	--list)
		MODE=list
		;;
	*)
		die "Unknown argument: $l_arg"
		;;
	esac
done

[ -f "$POLICY_FILE" ] || die "Missing budget policy: $POLICY_FILE"

if [ "$MODE" = list ]; then
	print_list
else
	run_check
fi
