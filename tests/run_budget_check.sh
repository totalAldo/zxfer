#!/bin/sh
#
# Complexity and anti-rebloat gate: enforce universal module/function/test
# ceilings plus the sensitive-caller ratchets selected by policy.
#

set -eu

if [ -n "${ZXFER_BUDGET_ROOT:-}" ]; then
	ZXFER_ROOT=$(cd "$ZXFER_BUDGET_ROOT" && pwd)
else
	ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
fi
POLICY_FILE=$ZXFER_ROOT/tests/budget_policy.tsv
FUNCTION_DEF_PATTERN='^[A-Za-z_][A-Za-z0-9_]*() {'
COMPLEXITY_AWK=$ZXFER_ROOT/tests/measure_shell_complexity.awk
TAB=$(printf '\t')
VIOLATION_COUNT=0
ROW_COUNT=0

print_usage() {
	cat <<'EOF'
Usage: tests/run_budget_check.sh [--list]

Check the working tree against the architecture-oriented ceilings and
ratchet-down-only budgets in tests/budget_policy.tsv.

Options:
  --list        print current measured values in policy format (for ratcheting)
  -h, --help    show this help
EOF
}

die() {
	printf '%s\n' "$*" >&2
	exit 1
}

# Every production module plus the launcher, as repo-relative paths.
budget_file_set() {
	(
		cd "$ZXFER_ROOT"
		for l_file in src/*.sh; do
			printf '%s\n' "$l_file"
		done
		printf 'zxfer\n'
	)
}

module_file_set() {
	(
		cd "$ZXFER_ROOT"
		for l_file in src/*.sh; do
			printf '%s\n' "$l_file"
		done
	)
}

test_definition_file_set() {
	(
		cd "$ZXFER_ROOT"
		for l_file in \
			tests/test_*.sh \
			tests/suites/*.sh \
			tests/fixtures/snapshot_discovery/*.sh; do
			[ -f "$l_file" ] || continue
			printf '%s\n' "$l_file"
		done
	)
}

test_helper_file_set() {
	(
		cd "$ZXFER_ROOT"
		for l_file in tests/helpers/*.sh; do
			[ -f "$l_file" ] || continue
			printf '%s\n' "$l_file"
		done
	)
}

integration_fragment_file_set() {
	(
		cd "$ZXFER_ROOT"
		for l_file in tests/integration/*.sh; do
			[ -f "$l_file" ] || continue
			printf '%s\n' "$l_file"
		done
	)
}

integration_composition_runner_file_set() {
	(
		cd "$ZXFER_ROOT"
		[ ! -f tests/run_integration_zxfer.sh ] ||
			printf '%s\n' tests/run_integration_zxfer.sh
	)
}

setup_definition_file_set() {
	test_definition_file_set
	test_helper_file_set
	integration_fragment_file_set
	integration_composition_runner_file_set
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

measure_executable_lines() {
	cat_budget_file_set | awk 'NF && $0 !~ /^[[:space:]]*#/ { count++ } END { print count + 0 }'
}

measure_function_metrics() {
	(
		cd "$ZXFER_ROOT"
		awk -f "$COMPLEXITY_AWK" src/*.sh zxfer
	)
}

measure_setup_metrics() {
	l_setup_file_list=$(setup_definition_file_set)
	set --
	while IFS= read -r l_file; do
		[ -n "$l_file" ] || continue
		set -- "$@" "$l_file"
	done <<EOF
$l_setup_file_list
EOF
	[ "$#" -gt 0 ] || return 0
	l_setup_metrics=$(
		cd "$ZXFER_ROOT"
		awk -f "$COMPLEXITY_AWK" "$@"
	) || return "$?"
	printf '%s\n' "$l_setup_metrics" | awk -F "$TAB" '$2 == "setUp"'
}

measure_setup_headers() {
	l_setup_header_file_list=$(setup_definition_file_set)
	set --
	while IFS= read -r l_file; do
		[ -n "$l_file" ] || continue
		set -- "$@" "$l_file"
	done <<EOF
$l_setup_header_file_list
EOF
	[ "$#" -gt 0 ] || return 0
	l_setup_headers=$(
		cd "$ZXFER_ROOT"
		awk -v headers_only=1 -f "$COMPLEXITY_AWK" "$@"
	) || return "$?"
	printf '%s\n' "$l_setup_headers" | awk -F "$TAB" '$2 == "setUp"'
}

file_set_for_line_ceiling() {
	case "$1" in
	module_lines)
		module_file_set
		;;
	test_lines)
		test_definition_file_set
		;;
	integration_fragment_lines)
		integration_fragment_file_set
		;;
	integration_runner_lines)
		integration_composition_runner_file_set
		;;
	esac
}

check_file_line_ceiling() {
	l_file_kind=$1
	l_file_max=$2
	l_file_list=$(file_set_for_line_ceiling "$l_file_kind")

	while IFS= read -r l_file; do
		[ -n "$l_file" ] || continue
		l_current=$(measure_lines "$l_file")
		if [ "$l_current" -gt "$l_file_max" ]; then
			report_violation "$l_file_kind" "$l_file" "$l_current" "$l_file_max" "universal physical-line ceiling exceeded"
		fi
	done <<EOF
$l_file_list
EOF
}

measure_max_file_lines() {
	l_measure_file_list=$(file_set_for_line_ceiling "$1")

	while IFS= read -r l_file; do
		[ -n "$l_file" ] || continue
		measure_lines "$l_file"
	done <<EOF | awk '$1 > maximum { maximum = $1 } END { print maximum + 0 }'
$l_measure_file_list
EOF
}

check_setup_ceiling() {
	l_setup_max=$1
	l_setup_metrics=$(measure_setup_metrics) || return "$?"
	l_setup_headers=$(measure_setup_headers) || return "$?"
	while IFS=$TAB read -r l_file l_function l_start l_lines l_decisions; do
		[ -n "$l_file" ] || continue
		if [ "$l_lines" -gt "$l_setup_max" ]; then
			printf 'setup_lines\t%s:%s:%s\t%s\t%s\n' \
				"$l_file" "$l_start" "$l_function" "$l_lines" "$l_setup_max"
		fi
	done <<EOF
$l_setup_metrics
EOF
	{
		printf '%s\n' "$l_setup_metrics" |
			awk -F "$TAB" 'NF >= 4 { printf "metric\t%s\t%s\t%s\n", $1, $2, $3 }'
		printf '%s\n' "$l_setup_headers" |
			awk -F "$TAB" 'NF >= 3 { printf "header\t%s\t%s\t%s\n", $1, $2, $3 }'
	} | awk -F "$TAB" -v maximum="$l_setup_max" '
		$1 == "metric" {
			measured[$2 SUBSEP $3 SUBSEP $4] = 1
			next
		}
		$1 == "header" && !(($2 SUBSEP $3 SUBSEP $4) in measured) {
			printf "setup_lines\t%s:%s:%s\tunmeasured\t%s\n", $2, $4, $3, maximum
		}
	'
}

report_setup_ceiling_violations() {
	l_setup_max=$1
	l_setup_violations=$(check_setup_ceiling "$l_setup_max") || return "$?"
	[ -n "$l_setup_violations" ] || return 0

	while IFS=$TAB read -r l_kind l_target l_current l_max; do
		report_violation "$l_kind" "$l_target" "$l_current" "$l_max" "test fixture setup ceiling or span validation failed"
	done <<EOF
$l_setup_violations
EOF
}

check_function_ceiling() {
	l_metric_kind=$1
	l_metric_max=$2
	l_metric_field=4
	[ "$l_metric_kind" != function_decisions ] || l_metric_field=5

	measure_function_metrics | while IFS=$TAB read -r l_file l_function l_start l_lines l_decisions; do
		if [ "$l_metric_field" -eq 4 ]; then
			l_current=$l_lines
		else
			l_current=$l_decisions
		fi
		if [ "$l_current" -gt "$l_metric_max" ]; then
			# This runs in a pipeline subshell on POSIX shells. Emit a stable
			# machine-readable row; the parent counts it below.
			printf '%s\t%s:%s:%s\t%s\t%s\n' "$l_metric_kind" "$l_file" "$l_start" "$l_function" "$l_current" "$l_metric_max"
		fi
	done
}

report_function_ceiling_violations() {
	l_metric_kind=$1
	l_metric_max=$2
	l_metric_violations=$(check_function_ceiling "$l_metric_kind" "$l_metric_max")
	[ -n "$l_metric_violations" ] || return 0

	while IFS=$TAB read -r l_kind l_target l_current l_max; do
		report_violation "$l_kind" "$l_target" "$l_current" "$l_max" "production function complexity ceiling exceeded"
	done <<EOF
$l_metric_violations
EOF
}

# Print file:line:text rows for one literal symbol across the budgeted tree.
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
		module_lines | test_lines | integration_fragment_lines | integration_runner_lines)
			[ "$l_target" = ALL ] || report_violation "$l_kind" "$l_target" - "$l_max" "target must be ALL"
			check_file_line_ceiling "$l_kind" "$l_max"
			;;
		setup_lines)
			[ "$l_target" = ALL ] || report_violation "$l_kind" "$l_target" - "$l_max" "target must be ALL"
			report_setup_ceiling_violations "$l_max"
			;;
		function_lines | function_decisions)
			[ "$l_target" = ALL ] || report_violation "$l_kind" "$l_target" - "$l_max" "target must be ALL"
			report_function_ceiling_violations "$l_kind" "$l_max"
			;;
		executable_lines)
			if [ "$l_target" != TOTAL ]; then
				report_violation "$l_kind" "$l_target" - "$l_max" "target must be TOTAL"
				continue
			fi
			l_current=$(measure_executable_lines)
			if [ "$l_current" -gt "$l_max" ]; then
				report_violation "$l_kind" "$l_target" "$l_current" "$l_max" "tree-wide executable-line ratchet exceeded"
			fi
			;;
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
	l_function_metrics=$(measure_function_metrics)
	l_setup_metrics=$(measure_setup_metrics)
	l_max_function_lines=$(printf '%s\n' "$l_function_metrics" | awk -F "$TAB" '$4 > maximum { maximum = $4 } END { print maximum + 0 }')
	l_max_function_decisions=$(printf '%s\n' "$l_function_metrics" | awk -F "$TAB" '$5 > maximum { maximum = $5 } END { print maximum + 0 }')
	printf 'module_lines\tALL\t%s\n' "$(measure_max_file_lines module_lines)"
	printf 'function_lines\tALL\t%s\n' "$l_max_function_lines"
	printf 'function_decisions\tALL\t%s\n' "$l_max_function_decisions"
	printf 'test_lines\tALL\t%s\n' "$(measure_max_file_lines test_lines)"
	printf 'integration_fragment_lines\tALL\t%s\n' "$(measure_max_file_lines integration_fragment_lines)"
	printf 'integration_runner_lines\tALL\t%s\n' "$(measure_max_file_lines integration_runner_lines)"
	printf 'setup_lines\tALL\t%s\n' "$(printf '%s\n' "$l_setup_metrics" | awk -F "$TAB" '$4 > maximum { maximum = $4 } END { print maximum + 0 }')"
	printf 'executable_lines\tTOTAL\t%s\n' "$(measure_executable_lines)"
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
