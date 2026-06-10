#!/bin/sh
#
# Anti-rebloat budget gate: measure the working tree and compare it against
# the ratchet-down-only budgets committed in tests/budget_policy.tsv.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
POLICY_FILE=$ZXFER_ROOT/tests/budget_policy.tsv
FUNCTION_DEF_PATTERN='^[A-Za-z_][A-Za-z0-9_]*() {'
TAB=$(printf '\t')
VIOLATION_COUNT=0
ROW_COUNT=0

print_usage() {
	cat <<'EOF'
Usage: tests/run_budget_check.sh [--list]

Check the working tree against the ratchet-down-only size budgets in
tests/budget_policy.tsv. Exits non-zero when any measurement exceeds its
budget or when a policy row names a file that no longer exists.

Options:
  --list        print current measured values in policy format (for ratcheting)
  -h, --help    show this help
EOF
}

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

# The budgeted file set: every src module plus the launcher, as repo-relative
# paths. This list is the single authority for what lines/functions budgets
# cover.
budget_file_set() {
	(
		cd "$ZXFER_ROOT"
		for l_file in src/*.sh; do
			printf '%s\n' "$l_file"
		done
		printf 'zxfer\n'
	)
}

cat_budget_file_set() {
	(
		cd "$ZXFER_ROOT"
		budget_file_set | xargs cat
	)
}

# Measurement rules live only in the measure_* helpers below so the check and
# --list modes cannot drift apart.
measure_lines() {
	if [ "$1" = TOTAL ]; then
		cat_budget_file_set | wc -l | tr -d ' \t'
	else
		wc -l <"$ZXFER_ROOT/$1" | tr -d ' \t'
	fi
}

measure_functions() {
	if [ "$1" = TOTAL ]; then
		cat_budget_file_set | grep -c "$FUNCTION_DEF_PATTERN" || :
	else
		grep -c "$FUNCTION_DEF_PATTERN" "$ZXFER_ROOT/$1" || :
	fi
}

# Print file:line:text rows for one literal symbol across the budgeted tree.
measure_caller_matches() {
	(
		cd "$ZXFER_ROOT"
		grep -rnF -- "$1" src zxfer || :
	)
}

measure_caller_count() {
	measure_caller_matches "$1" | awk 'END { print NR }'
}

measure_caller_files() {
	measure_caller_matches "$1" | cut -d: -f1 | sort -u
}

report_violation() {
	if [ "$VIOLATION_COUNT" -eq 0 ]; then
		printf '%-10s %-42s %8s %8s  %s\n' KIND TARGET CURRENT MAX DETAIL
	fi
	printf '%-10s %-42s %8s %8s  %s\n' "$1" "$2" "$3" "$4" "$5"
	VIOLATION_COUNT=$((VIOLATION_COUNT + 1))
}

check_caller_allowed_files() {
	l_symbol=$1
	l_allowed=$2
	l_files=$(measure_caller_files "$l_symbol")
	[ -n "$l_files" ] || return 0
	while IFS= read -r l_file; do
		case ",$l_allowed," in
		*",$l_file,"*) ;;
		*)
			report_violation callers "$l_symbol" - - "match outside allow-list: $l_file"
			;;
		esac
	done <<EOF
$l_files
EOF
}

run_check() {
	while IFS=$TAB read -r l_kind l_target l_max l_allowed; do
		case "$l_kind" in
		'' | '#'*)
			continue
			;;
		esac
		ROW_COUNT=$((ROW_COUNT + 1))
		case "$l_kind" in
		lines | functions)
			if [ "$l_target" != TOTAL ] && [ ! -f "$ZXFER_ROOT/$l_target" ]; then
				report_violation "$l_kind" "$l_target" missing "$l_max" "policy row names a file that no longer exists"
				continue
			fi
			if [ "$l_kind" = lines ]; then
				l_current=$(measure_lines "$l_target")
			else
				l_current=$(measure_functions "$l_target")
			fi
			if [ "$l_current" -gt "$l_max" ]; then
				report_violation "$l_kind" "$l_target" "$l_current" "$l_max" "over budget (ratchet-down-only)"
			fi
			;;
		callers)
			l_current=$(measure_caller_count "$l_target")
			if [ "$l_current" -gt "$l_max" ]; then
				report_violation callers "$l_target" "$l_current" "$l_max" "over budget (ratchet-down-only)"
			fi
			if [ -n "$l_allowed" ]; then
				check_caller_allowed_files "$l_target" "$l_allowed"
			fi
			;;
		*)
			report_violation "$l_kind" "${l_target:--}" - "${l_max:--}" "unknown policy record kind"
			;;
		esac
	done <"$POLICY_FILE"

	if [ "$VIOLATION_COUNT" -gt 0 ]; then
		printf 'budget check failed: %s violation(s) across %s policy rows\n' "$VIOLATION_COUNT" "$ROW_COUNT" >&2
		exit 1
	fi
	printf 'budget check passed: %s policy rows within budget\n' "$ROW_COUNT"
}

print_list() {
	budget_file_set | while IFS= read -r l_file; do
		printf 'lines\t%s\t%s\n' "$l_file" "$(measure_lines "$l_file")"
	done
	printf 'lines\tTOTAL\t%s\n' "$(measure_lines TOTAL)"
	budget_file_set | while IFS= read -r l_file; do
		printf 'functions\t%s\t%s\n' "$l_file" "$(measure_functions "$l_file")"
	done
	printf 'functions\tTOTAL\t%s\n' "$(measure_functions TOTAL)"
	# Caller symbols cannot be discovered from the tree, so refresh the
	# measured counts for the symbols already tracked by the policy.
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
