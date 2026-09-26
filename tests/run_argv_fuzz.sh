#!/bin/sh
#
# Seeded argv-boundary fuzz: prove that unusual dataset names and property
# values reach zfs as whole, byte-identical arguments. Each case generates a
# small tree (a root plus 1-4 children, some siblings whose names are
# prefixes of one another, some leaves volumes, 2-3 snapshots, the
# destination missing the newest one and sometimes whole children) with
# names made of alnum, space, - _ . and :, and user properties whose values
# mix bytes 0x01-0x7f with tabs, newlines, CR, quotes, backslashes, $(,
# backticks, %, commas and =, and lines shaped like another `zfs get -H`
# record. A property may start below the root, so some datasets lack it, and
# one case in three keeps every value on one line, so the recursive prefetch
# reads the tree. The real ./zxfer then runs black-box against a fake zfs
# that prints values raw, records every argv exactly and answers from the
# case's model, in the modes listed in tests/helpers/argv_fuzz.sh (local -R
# and -P, -P with per-dataset reads only, -j 2, -O and -T over a mock ssh,
# and an invalid operand that must fail closed). No real zfs, pool or ssh is
# ever used.
#
# A case passes when every run exits 0 (the invalid one non-zero with a
# structured failure report and no mutation), every operand the fake zfs saw
# was a whole generated name, every generated property value reached
# `zfs set` or `zfs create -o` as one byte-identical name=value argument, no
# property the source lacks was set, the destination converged (types,
# snapshots, and every user property byte for byte, or unchanged where the
# source lacks it), and the per-dataset reads mutated exactly as the
# recursive prefetch did. A failure prints the case, the problems, the
# recorded zfs argv, and the command that reproduces it.
#
# The seed alone fixes every case (the generator does not use awk's rand), so
# a reproduction works with any awk.
#
# shellcheck disable=SC1091

set -u

case "$0" in
/*) g_argv_fuzz_tests_dir=$(dirname "$0") ;;
*) g_argv_fuzz_tests_dir=${PWD:-.}/$(dirname "$0") ;;
esac
ZXFER_ROOT="$g_argv_fuzz_tests_dir/.."
g_argv_fuzz_tmp_root=${TMPDIR:-/tmp}

# shellcheck source=tests/helpers/blackbox.sh
. "$ZXFER_ROOT/tests/helpers/blackbox.sh"
# shellcheck source=tests/helpers/argv_fuzz.sh
. "$ZXFER_ROOT/tests/helpers/argv_fuzz.sh"

g_argv_fuzz_seed=""
g_argv_fuzz_iterations=50
g_argv_fuzz_first_case=1
g_argv_fuzz_keep=0
g_argv_fuzz_workdir=""

# Purpose: Print the help text.
# Usage: argv_fuzz_usage, to stdout for -h and to stderr on argument errors.
argv_fuzz_usage() {
	cat <<'EOF'
Usage: tests/run_argv_fuzz.sh [--seed N] [--iterations N] [--case K] [--keep]

Runs the real ./zxfer against a fake zfs over seeded cases of unusual dataset
names and property values, and checks that every argument reached zfs whole.

Options:
  --seed N        seed for the case generator (default: the clock)
  --iterations N  number of cases to run (default 50)
  --case K        number of the first case (default 1); with --iterations 1
                  it reruns one reported case
  --keep          keep the work directory and print its path
  -h, --help      show this help

Prints "seed=N" first and one line per case. Exits 0 when every case passes,
1 when any fails (each failure prints a reproduction command; a run that
cannot be set up fails its case), and 2 on a usage error or when the work
directory or a case cannot be created.
EOF
}

# Purpose: Remove the work directory unless --keep asked for it.
# Usage: Registered for EXIT.
argv_fuzz_cleanup() {
	[ -n "$g_argv_fuzz_workdir" ] || return 0
	if [ "$g_argv_fuzz_keep" -eq 1 ]; then
		printf 'kept: %s\n' "$g_argv_fuzz_workdir"
	else
		rm -rf "$g_argv_fuzz_workdir"
	fi
	g_argv_fuzz_workdir=""
}

# Purpose: Report a usage error with the help text and exit 2.
# Usage: argv_fuzz_usage_error MESSAGE
argv_fuzz_usage_error() {
	printf 'run_argv_fuzz.sh: %s\n' "$1" >&2
	argv_fuzz_usage >&2
	exit 2
}

# Purpose: Print one failed run: its command, problems, zfs argv and the
# tail of zxfer's stderr.
# Usage: argv_fuzz_report_failure RUN_DIR MODE
argv_fuzz_report_failure() {
	printf '  mode %s: %s\n' "$2" "$(cat "$1/command" 2>/dev/null)"
	printf '    problems:\n'
	if [ -s "$1/problems" ]; then
		sed 's/^/      /' "$1/problems"
	else
		printf '      the run could not be set up\n'
	fi
	printf '    zfs argv, one call per line (bytes outside printable ASCII shown as \\ooo):\n'
	argv_fuzz_render_argv_log "$1/state/argv.log" 2>/dev/null
	printf '    zxfer stderr (last 15 lines):\n'
	tail -n 15 "$1/stderr" 2>/dev/null |
		LC_ALL=C tr '\001-\010\013-\037\177' '[?*]' | sed 's/^/      /'
}

# The loop calls no function before it exits: posh loses $# after one.
while [ $# -gt 0 ]; do
	case $1 in
	--seed | --iterations | --case)
		[ $# -ge 2 ] || argv_fuzz_usage_error "$1 needs a value"
		case $1 in
		--seed) g_argv_fuzz_seed=$2 ;;
		--iterations) g_argv_fuzz_iterations=$2 ;;
		*) g_argv_fuzz_first_case=$2 ;;
		esac
		shift 2
		;;
	--keep)
		g_argv_fuzz_keep=1
		shift
		;;
	-h | --help)
		argv_fuzz_usage
		exit 0
		;;
	*) argv_fuzz_usage_error "unknown argument: $1" ;;
	esac
done
# awk's srand() returns the previous seed, so the second call prints the
# time-of-day seed the first one set.
[ -n "$g_argv_fuzz_seed" ] ||
	g_argv_fuzz_seed=$(awk 'BEGIN { srand(); printf "%d\n", srand() }')
for g_argv_fuzz_count in "$g_argv_fuzz_seed" "$g_argv_fuzz_iterations" \
	"$g_argv_fuzz_first_case"; do
	case $g_argv_fuzz_count in
	'' | *[!0-9]* | ????????????????*)
		argv_fuzz_usage_error "not a decimal integer of at most 15 digits: $g_argv_fuzz_count"
		;;
	esac
done
[ "$g_argv_fuzz_iterations" -ge 1 ] && [ "$g_argv_fuzz_first_case" -ge 1 ] ||
	argv_fuzz_usage_error "--iterations and --case must be at least 1"

printf 'seed=%s\n' "$g_argv_fuzz_seed"
trap argv_fuzz_cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
g_argv_fuzz_workdir=$(mktemp -d "$g_argv_fuzz_tmp_root/zxfer-argv-fuzz.XXXXXX") || {
	printf 'run_argv_fuzz.sh: cannot create a work directory\n' >&2
	exit 2
}

g_argv_fuzz_failures=0
g_argv_fuzz_runs=0
g_argv_fuzz_case=$g_argv_fuzz_first_case
g_argv_fuzz_last_case=$((g_argv_fuzz_first_case + g_argv_fuzz_iterations - 1))
while [ "$g_argv_fuzz_case" -le "$g_argv_fuzz_last_case" ]; do
	g_argv_fuzz_case_dir=$g_argv_fuzz_workdir/case-$g_argv_fuzz_case
	if ! argv_fuzz_generate_case "$g_argv_fuzz_seed" "$g_argv_fuzz_case" \
		"$g_argv_fuzz_case_dir"; then
		printf 'run_argv_fuzz.sh: cannot generate case %s\n' "$g_argv_fuzz_case" >&2
		exit 2
	fi
	g_argv_fuzz_failed_modes=""
	for g_argv_fuzz_mode in $ARGV_FUZZ_MODES; do
		g_argv_fuzz_runs=$((g_argv_fuzz_runs + 1))
		argv_fuzz_run_mode "$g_argv_fuzz_case_dir" "$g_argv_fuzz_mode" && continue
		if [ -z "$g_argv_fuzz_failed_modes" ]; then
			printf 'FAIL seed=%s case=%s\n  case:\n' "$g_argv_fuzz_seed" "$g_argv_fuzz_case"
			sed 's/^/    /' "$g_argv_fuzz_case_dir/summary"
		fi
		g_argv_fuzz_failed_modes="$g_argv_fuzz_failed_modes $g_argv_fuzz_mode"
		argv_fuzz_report_failure "$g_argv_fuzz_case_dir/$g_argv_fuzz_mode" \
			"$g_argv_fuzz_mode"
	done
	if [ -z "$g_argv_fuzz_failed_modes" ]; then
		printf 'case %s: ok\n' "$g_argv_fuzz_case"
		[ "$g_argv_fuzz_keep" -eq 1 ] || rm -rf "$g_argv_fuzz_case_dir"
	else
		printf '  reproduce: ./tests/run_argv_fuzz.sh --seed %s --case %s --iterations 1 --keep\n' \
			"$g_argv_fuzz_seed" "$g_argv_fuzz_case"
		printf 'case %s: FAILED in%s\n' "$g_argv_fuzz_case" "$g_argv_fuzz_failed_modes"
		g_argv_fuzz_failures=$((g_argv_fuzz_failures + 1))
	fi
	g_argv_fuzz_case=$((g_argv_fuzz_case + 1))
done

printf 'seed=%s cases=%s runs=%s failed_cases=%s\n' "$g_argv_fuzz_seed" \
	"$g_argv_fuzz_iterations" "$g_argv_fuzz_runs" "$g_argv_fuzz_failures"
[ "$g_argv_fuzz_failures" -eq 0 ]
