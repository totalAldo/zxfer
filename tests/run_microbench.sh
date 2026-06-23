#!/bin/sh
#
# Micro-benchmark for the real ./zxfer: counts helper-process spawns while
# replaying canned zfs fixtures, so hot-path regressions show up as exact
# process counts instead of noisy wall-clock numbers.
#
# The launcher is driven black-box through tests/mock_toolchain_helper.sh:
# a canned zfs answers every discovery command from deterministic fixtures,
# and counting wrappers shadow the hot userland helpers through
# ZXFER_SECURE_PATH so only the zxfer process tree is counted.
#
# Scenarios (operands; default is both):
#   noop         recursive replication where the destination already matches
#   dryrun_incr  -n dry run where every destination misses the last snapshot
#
# Output: stable TSV on stdout, one metric per row:
#   <scenario><TAB><tool><TAB><count>             spawns per counted tool
#   <scenario><TAB>TOTAL<TAB><count>              total counted spawns
#   <scenario><TAB>profile:<key><TAB><value>      only with -V; parsed from
#                                                 "zxfer profile: key=value"
#                                                 stderr lines
#   <scenario><TAB>advisory:wall_seconds<TAB><s>  wall time; ADVISORY ONLY,
#                                                 never budget it
#
# Budgets over these rows live in tests/perf_budgets.tsv and are enforced by
# tests/test_zxfer_microbench_budgets.sh. Exits non-zero when zxfer itself
# fails or the arguments are invalid.
#
# shellcheck disable=SC1091

set -u

g_microbench_tools="sed awk sort comm cut grep tr od date ps mktemp expr"
g_microbench_datasets=25
g_microbench_snaps=4
g_microbench_very_verbose=0
g_microbench_scenarios=""
g_microbench_workdir=""
g_microbench_mockdir=""

case "$0" in
/*)
	g_microbench_tests_dir=$(dirname "$0")
	;;
*)
	g_microbench_tests_dir=${PWD:-.}/$(dirname "$0")
	;;
esac
ZXFER_ROOT="$g_microbench_tests_dir/.."

# shellcheck source=tests/mock_toolchain_helper.sh
. "$g_microbench_tests_dir/mock_toolchain_helper.sh"

# Purpose: Print the operator-facing help text for this runner.
# Usage: Invoked for -h/--help and for argument errors (to stderr).
zxfer_microbench_usage() {
	cat <<EOF
Usage: tests/run_microbench.sh [-d num-datasets] [-s snaps-per-dataset] [-V]
       [-h|--help] [scenario ...]

Counts helper-process spawns of the real ./zxfer driven black-box against a
canned zfs (no real zfs/zpool is ever executed).

Scenarios (default: both):
  noop          recursive no-op replication (destination already in sync)
  dryrun_incr   incremental replication under -n (dry run; zero zfs argv)

Options:
  -d num-datasets       child datasets in the fixture tree (default 25)
  -s snaps-per-dataset  snapshots per dataset, minimum 2 (default 4)
  -V                    pass -V to zxfer and emit profile:<key> rows parsed
                        from "zxfer profile: <key>=<value>" stderr lines
  -h, --help            show this help

Output is TSV on stdout: <scenario> <metric> <value>. "TOTAL" sums all
counted tools (sed awk sort comm cut grep tr od date ps mktemp expr).
"advisory:wall_seconds" is informational only and must never be budgeted.
EOF
}

# Purpose: Remove the benchmark scratch directory on every exit path.
# Usage: Registered for EXIT and reused by the INT/TERM trap so interrupted
# runs do not leak fixture trees or spawn logs.
zxfer_microbench_cleanup() {
	if [ -n "$g_microbench_workdir" ]; then
		rm -rf "$g_microbench_workdir"
		g_microbench_workdir=""
	fi
}

# Purpose: Build the mock bin directory: canned zfs plus one counting wrapper
# per counted tool, all resolved ahead of the system dirs via the secure PATH.
# Usage: Called once before any scenario runs; returns non-zero when a real
# host tool cannot be resolved.
zxfer_microbench_build_toolchain() {
	g_microbench_mockdir="$g_microbench_workdir/mockbin"

	mkdir -p "$g_microbench_mockdir" || return 1
	zxfer_mockbin_write_canned_zfs "$g_microbench_mockdir/zfs" || return 1
	for l_tool in $g_microbench_tools; do
		l_real=$(zxfer_mockbin_resolve_host_tool "$l_tool") || return 1
		zxfer_mockbin_write_counting_wrapper \
			"$g_microbench_mockdir/$l_tool" "$l_real" || return 1
	done
}

# Purpose: Run one scenario against the real ./zxfer and emit its TSV rows.
# Usage: zxfer_microbench_run_scenario <noop|dryrun_incr>. Per-tool counts
# come from the spawn log, profile rows (with -V) from zxfer's stderr in
# zxfer's own stable emission order. Returns zxfer's exit status on failure
# after copying its stderr through for diagnosis.
zxfer_microbench_run_scenario() {
	l_scenario=$1

	case "$l_scenario" in
	noop)
		l_state_dir="$g_microbench_workdir/fixtures/noop"
		set --
		;;
	dryrun_incr)
		l_state_dir="$g_microbench_workdir/fixtures/incremental"
		set -- -n
		;;
	*)
		printf 'run_microbench.sh: unknown scenario: %s\n' "$l_scenario" >&2
		return 2
		;;
	esac
	if [ "$g_microbench_very_verbose" -eq 1 ]; then
		set -- "$@" -V
	fi

	l_spawn_log="$g_microbench_workdir/$l_scenario.spawn.log"
	l_zfs_log="$g_microbench_workdir/$l_scenario.zfs.log"
	l_stdout="$g_microbench_workdir/$l_scenario.stdout"
	l_stderr="$g_microbench_workdir/$l_scenario.stderr"
	: >"$l_spawn_log"
	: >"$l_zfs_log"

	# Export only for the zxfer run so the runner's own tool usage is never
	# counted; the wrappers log one line per spawn into this file.
	MOCK_SPAWN_LOG="$l_spawn_log"
	export MOCK_SPAWN_LOG
	l_wall_start=$(date '+%s')
	zxfer_mockbin_run_zxfer "$g_microbench_mockdir" "$l_state_dir" \
		"$l_zfs_log" "$@" -R "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_DEST_ROOT" >"$l_stdout" 2>"$l_stderr"
	l_status=$?
	l_wall_end=$(date '+%s')
	unset MOCK_SPAWN_LOG

	if [ "$l_status" -ne 0 ]; then
		printf 'run_microbench.sh: zxfer failed (status %s) in scenario %s\n' \
			"$l_status" "$l_scenario" >&2
		cat "$l_stderr" >&2
		return "$l_status"
	fi

	for l_tool in $g_microbench_tools; do
		# BSD and GNU grep both print the 0 count on no match (status 1).
		l_count=$(grep -Fxc "$l_tool" "$l_spawn_log" || :)
		printf '%s\t%s\t%s\n' "$l_scenario" "$l_tool" "${l_count:-0}"
	done
	l_total=$(wc -l <"$l_spawn_log" | tr -d '[:space:]')
	printf '%s\tTOTAL\t%s\n' "$l_scenario" "$l_total"

	if [ "$g_microbench_very_verbose" -eq 1 ]; then
		awk -v scenario="$l_scenario" '
			/^zxfer profile: / {
				line = substr($0, 16)
				eq = index(line, "=")
				if (eq > 1)
					printf "%s\tprofile:%s\t%s\n", scenario,
						substr(line, 1, eq - 1), substr(line, eq + 1)
			}
		' "$l_stderr"
	fi

	# Seconds-granularity on purpose: BSD date has no millisecond format and
	# this row is advisory context, not a budgetable metric.
	printf '%s\tadvisory:wall_seconds\t%s\n' "$l_scenario" \
		"$((l_wall_end - l_wall_start))"
}

for l_arg in "$@"; do
	case "$l_arg" in
	--help)
		zxfer_microbench_usage
		exit 0
		;;
	esac
done

while getopts d:s:Vh l_opt; do
	case "$l_opt" in
	d)
		g_microbench_datasets=$OPTARG
		;;
	s)
		g_microbench_snaps=$OPTARG
		;;
	V)
		g_microbench_very_verbose=1
		;;
	h)
		zxfer_microbench_usage
		exit 0
		;;
	*)
		zxfer_microbench_usage >&2
		exit 2
		;;
	esac
done
shift $((OPTIND - 1))

if [ $# -eq 0 ]; then
	g_microbench_scenarios="noop dryrun_incr"
else
	for l_scenario in "$@"; do
		case "$l_scenario" in
		noop | dryrun_incr) ;;
		*)
			printf 'run_microbench.sh: unknown scenario: %s\n' "$l_scenario" >&2
			zxfer_microbench_usage >&2
			exit 2
			;;
		esac
	done
	g_microbench_scenarios=$*
fi

# Mock-toolchain knobs inherited from a calling suite would skew counts or
# redirect fixtures; start from a clean slate.
unset MOCK_SPAWN_LOG MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS

g_microbench_workdir=$(mktemp -d "${TMPDIR:-/tmp}/zxfer_microbench.XXXXXX") || {
	printf 'run_microbench.sh: unable to create work directory\n' >&2
	exit 1
}
trap zxfer_microbench_cleanup EXIT
trap 'zxfer_microbench_cleanup; trap - EXIT; exit 130' INT TERM

zxfer_microbench_build_toolchain || exit 1
zxfer_mockbin_build_fixture_tree "$g_microbench_workdir/fixtures" \
	"$g_microbench_datasets" "$g_microbench_snaps" || exit 1

for l_scenario in $g_microbench_scenarios; do
	zxfer_microbench_run_scenario "$l_scenario" || exit "$?"
done
