#!/bin/sh
#
# Micro-benchmark for the real ./zxfer: counts helper-process spawns while
# replaying canned zfs fixtures, so hot-path regressions show up as exact
# process counts instead of noisy wall-clock numbers.
#
# The launcher is driven black-box through tests/mock_toolchain_helper.sh:
# a canned zfs answers every discovery command from deterministic fixtures,
# a socket-aware mock ssh runs "remote" commands locally, and counting
# wrappers shadow the hot userland helpers through ZXFER_SECURE_PATH so only
# the zxfer process tree is counted. In the remote scenarios that tree
# includes the remote side, because the mock ssh runs it on this host.
# The work directory sits directly under /tmp and zxfer runs with its tmp/
# subdirectory as TMPDIR, so the caller's TMPDIR cannot change the counts
# (see zxfer_mockbin_make_bench_workdir).
#
# Scenarios (operands; default is all six):
#   noop         recursive replication where the destination already matches
#   dryrun_incr  -n dry run where every destination misses the last snapshot
#   incr         live incremental replication of that same fixture (every
#                dataset receives one snapshot; the canned zfs accepts the
#                send/receive pipeline), the per-dataset hot path
#   remote_noop  noop with -O localhost -T localhost through the mock ssh
#   remote_incr  incr with -O localhost -T localhost through the mock ssh
#   props        incr with -P and 68 properties per dataset that already
#                match on both sides (zxfer_mockbin_add_property_fixtures),
#                the property pass's per-dataset path
#
# Output: stable TSV on stdout, one metric per row:
#   <scenario><TAB><tool><TAB><count>             spawns per counted tool
#   <scenario><TAB>TOTAL<TAB><count>              total counted spawns
#   <scenario><TAB>ssh_connections<TAB><n>        new ssh connections: master
#                                                 opens plus commands that did
#                                                 not ride a live control
#                                                 socket
#   <scenario><TAB>ssh_invocations<TAB><n>        every ssh call
#   <scenario><TAB>ssh_master_opens<TAB><n>       control masters opened
#   <scenario><TAB>profile:<key><TAB><value>      only with -V; parsed from
#                                                 "zxfer profile: key=value"
#                                                 stderr lines
#   <scenario><TAB>advisory:wall_seconds<TAB><s>  wall time; ADVISORY ONLY,
#                                                 never budget it
#   <scenario><TAB>advisory:forks_<phase><TAB><n> only with --forks; see below
#
# --forks reruns each scenario with the launcher sourced by bash 4.1+ under
# xtrace and counts the subshells the zxfer shell forks: command
# substitutions, pipeline stages, ( ) groups and background jobs, split into
# startup, run (from the first zxfer_run_zfs_mode_loop call) and exit (from
# zxfer_trap_exit), plus a total; a launcher without those functions reports
# every fork as startup. External helpers run directly by the main shell are
# not forks here; the helper rows count them. The rows are ADVISORY ONLY:
# bash forks differently from dash or ksh, so never budget them.
#
# Budgets over these rows live in tests/perf_budgets.tsv and are enforced by
# tests/test_zxfer_microbench_budgets.sh. Exits non-zero when zxfer itself
# fails or the arguments are invalid.
#
# shellcheck disable=SC1091

set -u

# Every userland helper the hot path may spawn is counted, including the
# ones the original list missed (cat, rm, id, stat...): those were where the
# per-dataset cost hid while the budgeted tools looked flat.
g_microbench_tools="sed awk sort comm cut grep tr od date ps mktemp expr cat rm mv ls mkdir cmp wc id stat uname"
g_microbench_datasets=25
g_microbench_snaps=4
g_microbench_very_verbose=0
g_microbench_forks=0
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
       [--forks] [-h|--help] [scenario ...]

Counts helper-process spawns and ssh connections of the real ./zxfer driven
black-box against a canned zfs and a mock ssh (no real zfs, zpool or ssh is
ever executed).

Scenarios (default: all six):
  noop          recursive no-op replication (destination already in sync)
  dryrun_incr   incremental replication under -n (dry run; zero zfs argv)
  incr          live incremental replication (one receive per dataset)
  remote_noop   noop with -O localhost -T localhost
  remote_incr   incr with -O localhost -T localhost
  props         incr with -P and 68 matching properties per dataset

Options:
  -d num-datasets       child datasets in the fixture tree (default 25)
  -s snaps-per-dataset  snapshots per dataset, minimum 2 (default 4)
  -V                    pass -V to zxfer and emit profile:<key> rows parsed
                        from "zxfer profile: <key>=<value>" stderr lines
  --forks               rerun each scenario under bash 4.1+ xtrace and emit
                        advisory:forks_{startup,run,exit,total} subshell
                        counts (bash from ZXFER_MICROBENCH_BASH, else PATH)
  -h, --help            show this help

Output is TSV on stdout: <scenario> <metric> <value>. "TOTAL" sums all
counted tools (sed awk sort comm cut grep tr od date ps mktemp expr cat rm
mv ls mkdir cmp wc id stat uname). "ssh_connections" counts master opens
plus ssh commands that did not ride a live control socket;
"ssh_invocations" counts every ssh call and "ssh_master_opens" the masters.
"advisory:*" rows are informational only and must never be budgeted.
EOF
}

# Purpose: Remove the benchmark scratch directory on every exit path.
# Usage: Registered for EXIT and reused by the INT/TERM trap so interrupted
# runs do not leak fixture trees, spawn logs or zxfer run roots.
zxfer_microbench_cleanup() {
	if [ -n "$g_microbench_workdir" ]; then
		rm -rf "$g_microbench_workdir"
		g_microbench_workdir=""
	fi
}

# Purpose: Build the mock bin directory: canned zfs, mock ssh and one counting
# wrapper per counted tool, all resolved ahead of the system dirs via the
# secure PATH.
# Usage: Called once before any scenario runs; returns non-zero when a real
# host tool cannot be resolved.
zxfer_microbench_build_toolchain() {
	g_microbench_mockdir="$g_microbench_workdir/mockbin"

	mkdir -p "$g_microbench_mockdir" || return 1
	zxfer_mockbin_write_canned_zfs "$g_microbench_mockdir/zfs" || return 1
	zxfer_mockbin_write_socket_ssh "$g_microbench_mockdir/ssh" || return 1
	for l_tool in $g_microbench_tools; do
		l_real=$(zxfer_mockbin_resolve_host_tool "$l_tool") || return 1
		zxfer_mockbin_write_counting_wrapper \
			"$g_microbench_mockdir/$l_tool" "$l_real" || return 1
	done
}

# Purpose: Check that ZXFER_MICROBENCH_BASH (default: bash from PATH) is bash
# 4.1+, which --forks needs for BASHPID and BASH_XTRACEFD, and export it.
# Usage: zxfer_microbench_find_trace_bash; returns 1 with a note otherwise.
zxfer_microbench_find_trace_bash() {
	ZXFER_MICROBENCH_BASH=${ZXFER_MICROBENCH_BASH:-$(command -v bash 2>/dev/null)}
	# shellcheck disable=SC2016  # expanded by the bash under test
	if [ -z "$ZXFER_MICROBENCH_BASH" ] ||
		! "$ZXFER_MICROBENCH_BASH" -c '[ "${BASH_VERSINFO[0]}${BASH_VERSINFO[1]}" -ge 41 ]' \
			>/dev/null 2>&1; then
		printf 'run_microbench.sh: --forks needs bash 4.1 or newer; set ZXFER_MICROBENCH_BASH\n' >&2
		return 1
	fi
	export ZXFER_MICROBENCH_BASH
}

# Purpose: Rerun one scenario under bash xtrace and print its advisory fork
# rows (see the header).
# Usage: zxfer_microbench_report_forks <scenario> <state-dir> [zxfer-arg...]
# Returns zxfer's exit status on failure after copying its stderr through.
zxfer_microbench_report_forks() {
	l_scenario=$1
	l_state_dir=$2
	shift 2
	l_tracer="$g_microbench_workdir/traced_zxfer"
	l_trace="$g_microbench_workdir/$l_scenario.trace"
	l_stderr="$g_microbench_workdir/$l_scenario.forks.stderr"

	# The tracer sources the launcher so PS4 is set inside bash (bash
	# ignores an inherited PS4 when run as root). Each trace line then
	# starts with "+<pid> <function> ", with one "+" per nesting level.
	cat >"$l_tracer" <<'EOF'
#!/bin/sh
exec "$ZXFER_MICROBENCH_BASH" -c '
set -o posix
PS4="+\${BASHPID} \${FUNCNAME[0]:-main} "
BASH_XTRACEFD=9
set -x
. "$0"
' "$ZXFER_MICROBENCH_LAUNCHER" "$@"
EOF
	chmod +x "$l_tracer" || return 1
	ZXFER_MICROBENCH_LAUNCHER=${ZXFER_MOCKBIN_ZXFER_BIN:-$ZXFER_ROOT/zxfer}
	export ZXFER_MICROBENCH_LAUNCHER

	(
		ZXFER_MOCKBIN_ZXFER_BIN=$l_tracer
		zxfer_mockbin_run_zxfer "$g_microbench_mockdir" "$l_state_dir" \
			/dev/null "$@" -R "$ZXFER_MOCKBIN_SOURCE_ROOT" \
			"$ZXFER_MOCKBIN_DEST_ROOT"
	) >/dev/null 2>"$l_stderr" 9>"$l_trace"
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		printf 'run_microbench.sh: traced zxfer failed (status %s) in scenario %s\n' \
			"$l_status" "$l_scenario" >&2
		cat "$l_stderr" >&2
		return "$l_status"
	fi

	# The first traced PID is the zxfer shell; every other PID is a fork,
	# counted once in the phase the zxfer shell was in when it first traced.
	awk -v scenario="$l_scenario" '
		BEGIN {
			phase = "startup"
		}
		$1 !~ /^\++[0-9]+$/ {
			next
		}
		{
			pid = $1
			sub(/^\++/, "", pid)
			if (main_pid == "")
				main_pid = pid
		}
		pid == main_pid {
			if ($2 == "zxfer_run_zfs_mode_loop" && phase == "startup")
				phase = "run"
			else if ($2 == "zxfer_trap_exit")
				phase = "exit"
			next
		}
		!(pid in seen) {
			seen[pid] = 1
			forks[phase]++
			total++
		}
		END {
			printf "%s\tadvisory:forks_startup\t%d\n", scenario, forks["startup"]
			printf "%s\tadvisory:forks_run\t%d\n", scenario, forks["run"]
			printf "%s\tadvisory:forks_exit\t%d\n", scenario, forks["exit"]
			printf "%s\tadvisory:forks_total\t%d\n", scenario, total
		}
	' "$l_trace"
}

# Purpose: Run one scenario against the real ./zxfer and emit its TSV rows.
# Usage: zxfer_microbench_run_scenario <scenario>. Per-tool counts come from
# the spawn log, ssh rows from the mock ssh log, profile rows (with -V) from
# zxfer's stderr in zxfer's own stable emission order, fork rows (with
# --forks) from a second, traced run. Returns zxfer's exit status on failure
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
	incr)
		l_state_dir="$g_microbench_workdir/fixtures/incremental"
		set --
		;;
	remote_noop)
		l_state_dir="$g_microbench_workdir/fixtures/noop"
		set -- -O localhost -T localhost
		;;
	remote_incr)
		l_state_dir="$g_microbench_workdir/fixtures/incremental"
		set -- -O localhost -T localhost
		;;
	props)
		l_state_dir="$g_microbench_workdir/fixtures/props"
		set -- -P
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
	l_ssh_log="$g_microbench_workdir/$l_scenario.ssh.log"
	l_zfs_log="$g_microbench_workdir/$l_scenario.zfs.log"
	l_stdout="$g_microbench_workdir/$l_scenario.stdout"
	l_stderr="$g_microbench_workdir/$l_scenario.stderr"
	: >"$l_spawn_log"
	: >"$l_ssh_log"
	: >"$l_zfs_log"

	# Export only for the zxfer run so the runner's own tool usage is never
	# counted; the wrappers log one line per spawn and the mock ssh one line
	# per call.
	MOCK_SPAWN_LOG="$l_spawn_log"
	MOCK_SSH_LOG="$l_ssh_log"
	export MOCK_SPAWN_LOG MOCK_SSH_LOG
	l_wall_start=$(date '+%s')
	zxfer_mockbin_run_zxfer "$g_microbench_mockdir" "$l_state_dir" \
		"$l_zfs_log" "$@" -R "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_DEST_ROOT" >"$l_stdout" 2>"$l_stderr"
	l_status=$?
	l_wall_end=$(date '+%s')
	unset MOCK_SPAWN_LOG MOCK_SSH_LOG

	if [ "$l_status" -ne 0 ]; then
		printf 'run_microbench.sh: zxfer failed (status %s) in scenario %s\n' \
			"$l_status" "$l_scenario" >&2
		cat "$l_stderr" >&2
		return "$l_status"
	fi
	# The props fixture already matches, so any mutating zfs command besides
	# the receives (a set or inherit, or a destroy, rollback or snapshot) means
	# the fixture and the launcher disagree and the counts measure other work.
	if [ "$l_scenario" = props ]; then
		l_mutations=$(grep -c '^MUTATE ' "$l_zfs_log" || :)
		if [ "${l_mutations:-0}" -ne 0 ]; then
			printf 'run_microbench.sh: scenario props ran %s mutating zfs command(s) besides its receives; its fixture must need none\n' \
				"$l_mutations" >&2
			return 1
		fi
	fi

	for l_tool in $g_microbench_tools; do
		# BSD and GNU grep both print the 0 count on no match (status 1).
		l_count=$(grep -Fxc "$l_tool" "$l_spawn_log" || :)
		printf '%s\t%s\t%s\n' "$l_scenario" "$l_tool" "${l_count:-0}"
	done
	l_total=$(wc -l <"$l_spawn_log" | tr -d '[:space:]')
	printf '%s\tTOTAL\t%s\n' "$l_scenario" "$l_total"
	zxfer_mockbin_ssh_log_rows "$l_scenario" "$l_ssh_log"

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

	if [ "$g_microbench_forks" -eq 1 ]; then
		zxfer_microbench_report_forks "$l_scenario" "$l_state_dir" "$@"
	fi
}

# Take the long options out before getopts sees the short ones.
for l_arg in "$@"; do
	shift
	case "$l_arg" in
	--help)
		zxfer_microbench_usage
		exit 0
		;;
	--forks)
		g_microbench_forks=1
		;;
	*)
		set -- "$@" "$l_arg"
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
	g_microbench_scenarios="noop dryrun_incr incr remote_noop remote_incr props"
else
	for l_scenario in "$@"; do
		case "$l_scenario" in
		noop | dryrun_incr | incr | remote_noop | remote_incr | props) ;;
		*)
			printf 'run_microbench.sh: unknown scenario: %s\n' "$l_scenario" >&2
			zxfer_microbench_usage >&2
			exit 2
			;;
		esac
	done
	g_microbench_scenarios=$*
fi
if [ "$g_microbench_forks" -eq 1 ]; then
	zxfer_microbench_find_trace_bash || exit 2
fi

# Mock-toolchain knobs inherited from a calling suite would skew counts or
# redirect fixtures; start from a clean slate.
unset MOCK_SPAWN_LOG MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS \
	MOCK_SSH_LOG MOCK_SSH_HANDSHAKE_S MOCK_SSH_MUX_S

g_microbench_workdir=$(zxfer_mockbin_make_bench_workdir zxfer_microbench) || {
	printf 'run_microbench.sh: unable to create a work directory under /tmp\n' >&2
	exit 1
}
trap zxfer_microbench_cleanup EXIT
trap 'zxfer_microbench_cleanup; trap - EXIT; exit 130' INT
trap 'zxfer_microbench_cleanup; trap - EXIT; exit 143' TERM
TMPDIR="$g_microbench_workdir/tmp"
export TMPDIR

zxfer_microbench_build_toolchain || exit 1
zxfer_mockbin_build_fixture_tree "$g_microbench_workdir/fixtures" \
	"$g_microbench_datasets" "$g_microbench_snaps" || exit 1
# The props state is its own copy, so the other scenarios' manifests keep
# their length and their counts.
case " $g_microbench_scenarios " in
*" props "*)
	cp -R "$g_microbench_workdir/fixtures/incremental" \
		"$g_microbench_workdir/fixtures/props" &&
		zxfer_mockbin_add_property_fixtures "$g_microbench_workdir/fixtures/props" \
			"$g_microbench_datasets" || exit 1
	;;
esac

for l_scenario in $g_microbench_scenarios; do
	zxfer_microbench_run_scenario "$l_scenario" || exit "$?"
done
