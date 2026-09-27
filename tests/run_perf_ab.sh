#!/bin/sh
#
# Advisory wall-clock A/B of ./zxfer against a baseline git ref, driven
# black-box through tests/mock_toolchain_helper.sh: a canned zfs answers
# every zfs command and a socket-aware mock ssh runs "remote" commands on
# this host, so no real zfs, zpool or ssh is ever executed.
#
# The baseline tree comes from `git archive REF` in the checkout that holds
# this script; REF may be any commit-ish, a SHA included. For each size, one
# fixture (N child datasets x --snapshots snapshots, 4 by default, plus the
# name-only listing rules the upstream-compat-final launcher issues) serves
# the scenarios (--scenarios; the first four by default):
#   noop         recursive replication where the destination already matches
#   incr         live incremental replication (one receive per dataset)
#   remote_noop  noop with -O localhost -T localhost through the mock ssh
#   remote_incr  incr with -O localhost -T localhost through the mock ssh
#   props        incr with -P, 68 properties per dataset that already match
#                on both sides (no zfs command but the receives may change
#                anything); opt-in, because
#                upstream-compat-final reads properties in per-property shell
#                loops (about 15 s for 3 children on macOS)
# In the remote scenarios the mock ssh sleeps --latency-ms for each new
# connection or master open and a sixteenth of it for each multiplexed call.
# Each scenario runs once per tree as a warm-up, then --reps times
# alternating candidate and baseline; every run must exit 0, reach the
# canned zfs and make the expected number of receives. Both launchers run
# under --shell (default /bin/sh). The work directory sits directly under
# /tmp, whatever the caller's TMPDIR, and each run gets its tmp/
# subdirectory as TMPDIR (see zxfer_mockbin_make_bench_workdir).
#
# Timing uses `date +%s%N` where it prints nanoseconds (GNU date, recent BSD
# date), else perl Time::HiRes, else python3, else whole seconds (with a
# warning). The median cost of two back-to-back clock reads is subtracted
# from every sample.
#
# Output: TSV on stdout (a header row, then one row per size and scenario
# with median, min and max seconds for each tree and the candidate/baseline
# ratio of the medians); with --summary, the same results as a Markdown
# table appended to FILE. Progress lines, the first naming the work
# directory, go to stderr. The comparison is advisory: a slowdown never
# changes the exit status. Exit 1 means a harness error (unknown ref, failed
# or wrong run), 2 a usage error.
#
# shellcheck disable=SC1091

set -u

g_perf_ab_baseline_ref=""
g_perf_ab_candidate_root=""
g_perf_ab_sizes="25,100"
g_perf_ab_reps=5
g_perf_ab_summary=""
g_perf_ab_latency_ms=80
g_perf_ab_workdir=""
g_perf_ab_samples=""
g_perf_ab_mockdir=""
g_perf_ab_secure_path=""
g_perf_ab_handshake_s=""
g_perf_ab_mux_s=""
g_perf_ab_clock=""
g_perf_ab_now=""
g_perf_ab_baseline_sha=""
g_perf_ab_baseline_note=""
g_perf_ab_snapshots=4
g_perf_ab_shell=/bin/sh
g_perf_ab_scenarios="noop,incr,remote_noop,remote_incr"

case "$0" in
/*)
	g_perf_ab_tests_dir=$(dirname "$0")
	;;
*)
	g_perf_ab_tests_dir=${PWD:-.}/$(dirname "$0")
	;;
esac
g_perf_ab_repo=$(cd "$g_perf_ab_tests_dir/.." && pwd) || exit 1

# shellcheck source=tests/mock_toolchain_helper.sh
. "$g_perf_ab_tests_dir/mock_toolchain_helper.sh"

# Purpose: Print the operator-facing help text.
# Usage: Invoked for -h/--help and, on stderr, for usage errors.
zxfer_perf_ab_usage() {
	cat <<'EOF'
Usage: tests/run_perf_ab.sh --baseline-ref REF [--candidate-root DIR]
       [--sizes 25,100] [--snapshots 4] [--scenarios LIST] [--shell PATH]
       [--reps N] [--summary FILE] [--latency-ms 80]

Advisory wall-clock A/B of ./zxfer (the candidate, by default this checkout)
against the tree of git ref REF, on a canned zfs and a mock ssh. Scenarios:
noop, incr, remote_noop and remote_incr (-O localhost -T localhost) by
default, and props (incr with -P and 68 matching properties per dataset).

Options:
  --baseline-ref REF    git commit-ish of the baseline tree, such as a
                        branch, tag or SHA (required)
  --candidate-root DIR  tree holding the candidate zxfer (default: this
                        checkout)
  --sizes LIST          comma-separated child-dataset counts (default 25,100)
  --snapshots N         snapshots per dataset in the fixture, at least 2
                        (default 4)
  --scenarios LIST      comma-separated scenarios to run, in order, from
                        noop, incr, remote_noop, remote_incr and props
                        (default noop,incr,remote_noop,remote_incr)
  --shell PATH          interpreter that runs both launchers, such as
                        /bin/dash (default /bin/sh)
  --reps N              timed runs per tree after one warm-up (default 5)
  --summary FILE        append a Markdown table to FILE
  --latency-ms MS       mock ssh cost of a new connection; a multiplexed call
                        costs MS/16 (default 80; 0 disables the sleeps)
  -h, --help            show this help

Output is TSV on stdout. A slowdown never fails the run; exit 1 means a
harness error and 2 a usage error.
EOF
}

# Purpose: Report a usage error with the help text and exit 2.
# Usage: zxfer_perf_ab_usage_error MESSAGE
zxfer_perf_ab_usage_error() {
	printf 'run_perf_ab.sh: %s\n' "$1" >&2
	zxfer_perf_ab_usage >&2
	exit 2
}

# Purpose: Report a harness error and exit 1; the EXIT trap cleans up.
# Usage: zxfer_perf_ab_die MESSAGE
zxfer_perf_ab_die() {
	printf 'run_perf_ab.sh: %s\n' "$1" >&2
	exit 1
}

# Purpose: Remove the work directory on every exit path.
# Usage: Registered for EXIT and reused by the INT/TERM trap.
zxfer_perf_ab_cleanup() {
	if [ -n "$g_perf_ab_workdir" ]; then
		cd / || :
		rm -rf "$g_perf_ab_workdir"
		g_perf_ab_workdir=""
	fi
}

# Purpose: Parse and validate the command line into the g_perf_ab_* options;
# g_perf_ab_sizes becomes a space-separated list.
# Usage: zxfer_perf_ab_parse_args "$@"; exits 2 on any usage error.
zxfer_perf_ab_parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--baseline-ref | --candidate-root | --sizes | --snapshots | --scenarios | --shell | --reps | --summary | --latency-ms)
			[ $# -ge 2 ] || zxfer_perf_ab_usage_error "$1 needs a value"
			case "$1" in
			--baseline-ref) g_perf_ab_baseline_ref=$2 ;;
			--candidate-root) g_perf_ab_candidate_root=$2 ;;
			--sizes) g_perf_ab_sizes=$2 ;;
			--snapshots) g_perf_ab_snapshots=$2 ;;
			--scenarios) g_perf_ab_scenarios=$2 ;;
			--shell) g_perf_ab_shell=$2 ;;
			--reps) g_perf_ab_reps=$2 ;;
			--summary) g_perf_ab_summary=$2 ;;
			--latency-ms) g_perf_ab_latency_ms=$2 ;;
			esac
			shift 2
			;;
		-h | --help)
			zxfer_perf_ab_usage
			exit 0
			;;
		*)
			zxfer_perf_ab_usage_error "unknown argument: $1"
			;;
		esac
	done

	[ -n "$g_perf_ab_baseline_ref" ] ||
		zxfer_perf_ab_usage_error "--baseline-ref is required"
	case "$g_perf_ab_reps" in
	'' | 0* | *[!0-9]*)
		zxfer_perf_ab_usage_error "--reps must be a positive integer"
		;;
	esac
	case "$g_perf_ab_latency_ms" in
	'' | *[!0-9]* | 0?*)
		zxfer_perf_ab_usage_error "--latency-ms must be a non-negative integer"
		;;
	esac
	case "$g_perf_ab_snapshots" in
	'' | *[!0-9]* | 0* | 1)
		zxfer_perf_ab_usage_error "--snapshots must be an integer of at least 2"
		;;
	esac
	# Scenarios become space-separated words, each known and listed once.
	case ",$g_perf_ab_scenarios," in
	,, | *,,*)
		zxfer_perf_ab_usage_error "--scenarios must be a comma-separated list of scenarios"
		;;
	esac
	l_scenarios_rest="$g_perf_ab_scenarios,"
	g_perf_ab_scenarios=""
	while [ -n "$l_scenarios_rest" ]; do
		l_scenario=${l_scenarios_rest%%,*}
		l_scenarios_rest=${l_scenarios_rest#*,}
		case "$l_scenario" in
		noop | incr | remote_noop | remote_incr | props) ;;
		*) zxfer_perf_ab_usage_error "unknown scenario in --scenarios: $l_scenario" ;;
		esac
		case " $g_perf_ab_scenarios " in
		*" $l_scenario "*)
			zxfer_perf_ab_usage_error "--scenarios lists $l_scenario more than once"
			;;
		esac
		g_perf_ab_scenarios="$g_perf_ab_scenarios $l_scenario"
	done
	# Every run starts in the work directory, so a relative --shell must
	# become absolute; a bare name is looked up in PATH.
	case "$g_perf_ab_shell" in
	*/*) l_shell_path=$g_perf_ab_shell ;;
	*) l_shell_path=$(command -v "$g_perf_ab_shell" 2>/dev/null) || l_shell_path="" ;;
	esac
	case "$l_shell_path" in
	'') ;;
	/*) ;;
	*) l_shell_path="$PWD/$l_shell_path" ;;
	esac
	if [ -z "$l_shell_path" ] || [ -d "$l_shell_path" ] || [ ! -x "$l_shell_path" ]; then
		zxfer_perf_ab_usage_error "--shell is not an executable: $g_perf_ab_shell"
	fi
	g_perf_ab_shell=$l_shell_path
	# Leading zeros are refused so shell arithmetic never reads octal.
	case ",$g_perf_ab_sizes," in
	,, | *,,* | *,0* | *[!0-9,]*)
		zxfer_perf_ab_usage_error "--sizes must be a comma-separated list of positive integers"
		;;
	esac
	# Turn the list into space-separated words; a repeated size would merge
	# its samples into one row with twice the reps.
	l_sizes_rest="$g_perf_ab_sizes,"
	g_perf_ab_sizes=""
	while [ -n "$l_sizes_rest" ]; do
		l_size=${l_sizes_rest%%,*}
		l_sizes_rest=${l_sizes_rest#*,}
		case " $g_perf_ab_sizes " in
		*" $l_size "*)
			zxfer_perf_ab_usage_error "--sizes lists $l_size more than once"
			;;
		esac
		g_perf_ab_sizes="$g_perf_ab_sizes $l_size"
	done

	g_perf_ab_candidate_root=${g_perf_ab_candidate_root:-$g_perf_ab_repo}
	[ -f "$g_perf_ab_candidate_root/zxfer" ] ||
		zxfer_perf_ab_usage_error "--candidate-root has no zxfer launcher: $g_perf_ab_candidate_root"
	g_perf_ab_candidate_root=$(cd "$g_perf_ab_candidate_root" && pwd) ||
		zxfer_perf_ab_usage_error "cannot enter --candidate-root"
	# true, not the special builtin :, so a failed redirection cannot exit
	# a POSIX shell before the usage error is printed.
	if [ -n "$g_perf_ab_summary" ]; then
		{ true >>"$g_perf_ab_summary"; } 2>/dev/null ||
			zxfer_perf_ab_usage_error "cannot append to --summary $g_perf_ab_summary"
		case "$g_perf_ab_summary" in
		/*) ;;
		*) g_perf_ab_summary="$PWD/$g_perf_ab_summary" ;;
		esac
	fi
}

# Purpose: Read the clock chosen by zxfer_perf_ab_start_clock.
# Usage: zxfer_perf_ab_now; sets g_perf_ab_now to seconds with a fraction.
zxfer_perf_ab_now() {
	case "$g_perf_ab_clock" in
	date_ns)
		l_now_ns=$(date '+%s%N')
		l_now_s=${l_now_ns%?????????}
		g_perf_ab_now=$l_now_s.${l_now_ns#"$l_now_s"}
		;;
	perl)
		g_perf_ab_now=$(perl -MTime::HiRes=time -e 'printf "%.6f", time')
		;;
	python3)
		g_perf_ab_now=$(python3 -c 'import time; print("%.6f" % time.time())')
		;;
	*)
		g_perf_ab_now=$(date '+%s')
		;;
	esac
}

# Purpose: Choose the best clock this host offers and record five
# back-to-back reads, whose median the report subtracts from every sample.
# Usage: zxfer_perf_ab_start_clock; sets g_perf_ab_clock to date_ns, perl,
# python3 or seconds (with a stderr warning).
zxfer_perf_ab_start_clock() {
	# %N must print nanoseconds: 10 digits of seconds plus 9 of fraction.
	l_clock_sample=$(date '+%s%N' 2>/dev/null)
	case "$l_clock_sample" in
	'' | *[!0-9]*) l_clock_sample="" ;;
	esac
	if [ "${#l_clock_sample}" -ge 19 ]; then
		g_perf_ab_clock=date_ns
	elif perl -MTime::HiRes -e 1 >/dev/null 2>&1; then
		g_perf_ab_clock=perl
	elif python3 -c 'import time' >/dev/null 2>&1; then
		g_perf_ab_clock=python3
	else
		g_perf_ab_clock=seconds
		printf '%s\n' 'run_perf_ab.sh: warning: no sub-second clock (date +%N, perl Time::HiRes or python3); timing in whole seconds' >&2
	fi

	l_clock_reads=0
	while [ "$l_clock_reads" -lt 5 ]; do
		zxfer_perf_ab_now
		l_clock_start=$g_perf_ab_now
		zxfer_perf_ab_now
		printf '0\tclock\tclock\t%s\t%s\n' "$l_clock_start" "$g_perf_ab_now" \
			>>"$g_perf_ab_samples"
		l_clock_reads=$((l_clock_reads + 1))
	done
}

# Purpose: Extract the baseline ref into $g_perf_ab_workdir/baseline.
# Usage: zxfer_perf_ab_extract_baseline; sets g_perf_ab_baseline_sha and
# exits 1 when the ref does not resolve or has no launcher.
# shellcheck disable=SC2016  # the patterns match a literal $l_timestamp
zxfer_perf_ab_extract_baseline() {
	l_base_dir="$g_perf_ab_workdir/baseline"
	if ! g_perf_ab_baseline_sha=$(git -C "$g_perf_ab_repo" rev-parse \
		--verify -q "$g_perf_ab_baseline_ref^{commit}"); then
		zxfer_perf_ab_die "cannot resolve --baseline-ref $g_perf_ab_baseline_ref in $g_perf_ab_repo"
	fi
	if ! git -C "$g_perf_ab_repo" archive --format=tar \
		-o "$g_perf_ab_workdir/baseline.tar" "$g_perf_ab_baseline_sha" ||
		! mkdir "$l_base_dir" ||
		! (cd "$l_base_dir" && tar -xf ../baseline.tar); then
		zxfer_perf_ab_die "cannot extract $g_perf_ab_baseline_ref"
	fi
	[ -f "$l_base_dir/zxfer" ] ||
		zxfer_perf_ab_die "$g_perf_ab_baseline_ref has no zxfer launcher"

	# upstream-compat-final hands `mktemp -t` templates without X's, which
	# GNU and busybox mktemp refuse (BSD mktemp adds its own suffix). On
	# such hosts, append .XXXXXX to those templates so the baseline runs at
	# all; the patch adds no process, so the timing stays comparable.
	mktemp -u -t zxfer_perf_ab_probe >/dev/null 2>&1 && return 0
	for l_base_file in "$l_base_dir/zxfer" "$l_base_dir"/src/*.sh; do
		grep -q 'mktemp .*-t "*zxfer[a-z_]*\.\$l_timestamp' "$l_base_file" \
			2>/dev/null || continue
		if ! sed 's/\(-t "*zxfer[a-z_]*\.\$l_timestamp\)/\1.XXXXXX/g' \
			"$l_base_file" >"$l_base_file.patched" ||
			! cat "$l_base_file.patched" >"$l_base_file"; then
			zxfer_perf_ab_die "cannot patch mktemp templates in $l_base_file"
		fi
		g_perf_ab_baseline_note=" The baseline's X-less mktemp templates were suffixed with .XXXXXX for this host's mktemp."
	done
}

# Purpose: Build the fixture for one size: the canned-zfs tree with
# --snapshots snapshots per dataset plus, in both states, the name-only
# listing rules the upstream-compat-final launcher issues (recursive name
# listings and one `zfs list -H` per child); and, when props is selected, a
# props state: the incremental one plus matching property fixtures, kept
# apart so the other scenarios' manifests stay as short as before.
# Usage: zxfer_perf_ab_build_fixture SIZE; returns 1 on a write failure.
zxfer_perf_ab_build_fixture() {
	l_fixture_size=$1
	l_fixture_root="$g_perf_ab_workdir/fx$l_fixture_size"

	zxfer_mockbin_build_fixture_tree "$l_fixture_root" "$l_fixture_size" \
		"$g_perf_ab_snapshots" || return 1
	for l_fixture_state in noop incremental; do
		l_fixture_dir="$l_fixture_root/$l_fixture_state"
		cut -f1 "$l_fixture_dir/src_snapshots.list" \
			>"$l_fixture_dir/names_src.list" || return 1
		cut -f1 "$l_fixture_dir/dst_snapshots.list" \
			>"$l_fixture_dir/names_dst.list" || return 1
		{
			printf '%s\t%s\t0\n' \
				"list -Hr -o name -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" \
				names_src.list
			printf '%s\t%s\t0\n' \
				"list -Hr -o name -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
				names_dst.list
			l_fixture_index=1
			while [ "$l_fixture_index" -le "$l_fixture_size" ]; do
				l_fixture_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_fixture_index"
				printf '%s\t96K\t1.0G\t24K\t/%s\n' "$l_fixture_dataset" \
					"$l_fixture_dataset" \
					>"$l_fixture_dir/exists_$l_fixture_index.list" || return 1
				printf '%s\t%s\t0\n' "list -H $l_fixture_dataset" \
					"exists_$l_fixture_index.list"
				l_fixture_index=$((l_fixture_index + 1))
			done
		} >>"$l_fixture_dir/manifest" || return 1
	done
	case " $g_perf_ab_scenarios " in
	*" props "*)
		cp -R "$l_fixture_root/incremental" "$l_fixture_root/props" &&
			zxfer_mockbin_add_property_fixtures "$l_fixture_root/props" \
				"$l_fixture_size" || return 1
		;;
	esac
}

# Purpose: Time one run of one tree and record it unless it is the warm-up.
# Usage: zxfer_perf_ab_run_once SIZE SCENARIO candidate|baseline REP; exits
# 1 when zxfer fails, never calls zfs, receives the wrong count, in a remote
# run skips ssh, or in a props run runs a mutating zfs command (set,
# inherit, destroy, ...) besides its receives.
zxfer_perf_ab_run_once() {
	l_run_size=$1
	l_run_scenario=$2
	l_run_side=$3
	l_run_rep=$4

	case "$l_run_side" in
	candidate) l_run_root=$g_perf_ab_candidate_root ;;
	*) l_run_root="$g_perf_ab_workdir/baseline" ;;
	esac
	case "$l_run_scenario" in
	*noop)
		l_run_state=noop
		l_run_want=0
		;;
	props)
		l_run_state=props
		l_run_want=$((l_run_size + 1))
		;;
	*)
		l_run_state=incremental
		l_run_want=$((l_run_size + 1))
		;;
	esac
	case "$l_run_scenario" in
	remote_*)
		set -- -O localhost -T localhost
		l_run_handshake=$g_perf_ab_handshake_s
		l_run_mux=$g_perf_ab_mux_s
		;;
	props)
		set -- -P
		l_run_handshake=""
		l_run_mux=""
		;;
	*)
		set --
		l_run_handshake=""
		l_run_mux=""
		;;
	esac
	l_run_zfs_log="$g_perf_ab_workdir/zfs.log"
	l_run_ssh_log="$g_perf_ab_workdir/ssh.log"
	l_run_stderr="$g_perf_ab_workdir/zxfer.stderr"
	: >"$l_run_zfs_log"
	: >"$l_run_ssh_log"

	# Both trees see the mocks first: the candidate through
	# ZXFER_SECURE_PATH, an older launcher through PATH.
	zxfer_perf_ab_now
	l_run_start=$g_perf_ab_now
	MOCK_ZFS_LOG=$l_run_zfs_log \
		MOCK_ZFS_FIXTURE_DIR="$g_perf_ab_workdir/fx$l_run_size/$l_run_state" \
		MOCK_SSH_LOG=$l_run_ssh_log \
		MOCK_SSH_HANDSHAKE_S=$l_run_handshake \
		MOCK_SSH_MUX_S=$l_run_mux \
		ZXFER_SECURE_PATH=$g_perf_ab_secure_path \
		ZXFER_SECURE_PATH_APPEND="" \
		PATH="$g_perf_ab_mockdir:$PATH" \
		TMPDIR="$g_perf_ab_workdir/tmp" \
		"$g_perf_ab_shell" "$l_run_root/zxfer" "$@" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT" \
		>/dev/null 2>"$l_run_stderr"
	l_run_status=$?
	zxfer_perf_ab_now

	l_run_label="$l_run_side $l_run_scenario at size $l_run_size"
	if [ "$l_run_status" -ne 0 ]; then
		tail -n 20 "$l_run_stderr" >&2
		zxfer_perf_ab_die "$l_run_label: zxfer exited with status $l_run_status"
	fi
	[ -s "$l_run_zfs_log" ] ||
		zxfer_perf_ab_die "$l_run_label: no zfs command reached the canned zfs"
	l_run_receives=$(grep -c '^receive ' "$l_run_zfs_log")
	[ "$l_run_receives" -eq "$l_run_want" ] ||
		zxfer_perf_ab_die "$l_run_label: $l_run_receives receives, expected $l_run_want"
	case "$l_run_scenario" in
	remote_*)
		[ -s "$l_run_ssh_log" ] ||
			zxfer_perf_ab_die "$l_run_label: no ssh call reached the mock ssh"
		;;
	props)
		l_run_mutations=$(grep -c '^MUTATE ' "$l_run_zfs_log")
		[ "$l_run_mutations" -eq 0 ] ||
			zxfer_perf_ab_die "$l_run_label: $l_run_mutations mutating zfs command(s) besides the receives, expected none"
		;;
	esac
	[ "$l_run_rep" -eq 0 ] ||
		printf '%s\t%s\t%s\t%s\t%s\n' "$l_run_size" "$l_run_scenario" \
			"$l_run_side" "$l_run_start" "$g_perf_ab_now" >>"$g_perf_ab_samples"
}

# Purpose: Summarize the samples as TSV on stdout and write the Markdown
# block to $g_perf_ab_workdir/summary.md.
# Usage: zxfer_perf_ab_report. Clock rows (scenario "clock") give the read
# overhead that is subtracted from every other sample.
zxfer_perf_ab_report() {
	l_report_candidate=$(git -C "$g_perf_ab_candidate_root" describe --always --dirty 2>/dev/null) ||
		l_report_candidate="(not a git checkout)"
	awk -F '\t' -v OFS='\t' \
		-v md="$g_perf_ab_workdir/summary.md" \
		-v candidate="$l_report_candidate" \
		-v ref="$g_perf_ab_baseline_ref" \
		-v sha="$g_perf_ab_baseline_sha" \
		-v reps="$g_perf_ab_reps" \
		-v snapshots="$g_perf_ab_snapshots" \
		-v shell="$g_perf_ab_shell" \
		-v scenarios="$g_perf_ab_scenarios" \
		-v clock="$g_perf_ab_clock" \
		-v latency_ms="$g_perf_ab_latency_ms" \
		-v note="$g_perf_ab_baseline_note" '
		# Sort a[1..n] ascending in place; n is small.
		function sort_numbers(a, n,    i, j, v) {
			for (i = 2; i <= n; i++) {
				v = a[i]
				for (j = i - 1; j >= 1 && a[j] > v; j--)
					a[j + 1] = a[j]
				a[j + 1] = v
			}
		}
		function median(a, n) {
			return (n % 2) ? a[(n + 1) / 2] : (a[n / 2] + a[n / 2 + 1]) / 2
		}
		$2 == "clock" {
			clock_delta[++clock_count] = $5 - $4
			next
		}
		{
			key = $1 SUBSEP $2
			if (!(key in seen)) {
				seen[key] = 1
				order[++key_count] = key
				size[key] = $1
				scenario[key] = $2
			}
			n = ++count[key, $3]
			elapsed[key, $3, n] = $5 - $4
		}
		END {
			overhead = 0
			if (clock_count) {
				sort_numbers(clock_delta, clock_count)
				overhead = median(clock_delta, clock_count)
			}
			print "size", "scenario", "candidate_median_s", "candidate_min_s",
				"candidate_max_s", "baseline_median_s", "baseline_min_s",
				"baseline_max_s", "ratio"
			printf("### zxfer wall-clock A/B (advisory)\n\n") > md
			printf("Candidate `%s` against baseline `%s` (`%.12s`) on the canned zfs, %d snapshots per dataset, both launchers run by `%s`: one warm-up, then %d alternating runs per tree. ",
				candidate, ref, sha, snapshots, shell, reps) > md
			if (index(scenarios " ", " props "))
				printf("The props rows run the incremental with `-P` and 68 properties per dataset that already match. ") > md
			if (latency_ms > 0)
				printf("Remote rows use `-O localhost -T localhost` through a mock ssh that charges %d ms per new connection and %.1f ms per multiplexed call. ",
					latency_ms, latency_ms / 16) > md
			else
				printf("Remote rows use `-O localhost -T localhost` through a mock ssh with no added latency. ") > md
			printf("Clock: %s (%.1f ms read overhead subtracted). Ratio = candidate median / baseline median; below 1.00 the candidate is faster.%s\n\n",
				clock, overhead * 1000, note) > md
			print("| size (child datasets) | scenario | candidate s (min-max) | baseline s (min-max) | ratio |") > md
			print("| ---: | --- | ---: | ---: | ---: |") > md
			for (k = 1; k <= key_count; k++) {
				key = order[k]
				for (s = 1; s <= 2; s++) {
					side = (s == 1) ? "candidate" : "baseline"
					n = count[key, side]
					split("", t)
					for (i = 1; i <= n; i++) {
						t[i] = elapsed[key, side, i] - overhead
						if (t[i] < 0)
							t[i] = 0
					}
					sort_numbers(t, n)
					med[side] = median(t, n)
					low[side] = t[1]
					high[side] = t[n]
				}
				ratio = (med["baseline"] > 0) ? sprintf("%.2f", med["candidate"] / med["baseline"]) : "n/a"
				printf "%s\t%s\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%s\n",
					size[key], scenario[key], med["candidate"], low["candidate"],
					high["candidate"], med["baseline"], low["baseline"],
					high["baseline"], ratio
				printf("| %s | %s | %.3f (%.3f-%.3f) | %.3f (%.3f-%.3f) | %s |\n",
					size[key], scenario[key], med["candidate"], low["candidate"],
					high["candidate"], med["baseline"], low["baseline"],
					high["baseline"], ratio) > md
			}
		}
	' "$g_perf_ab_samples"
}

zxfer_perf_ab_parse_args "$@"

# Mock knobs inherited from a calling suite would redirect logs or add
# latency to local runs; start from a clean slate.
unset MOCK_SPAWN_LOG MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS \
	MOCK_SSH_LOG MOCK_SSH_HANDSHAKE_S MOCK_SSH_MUX_S

g_perf_ab_workdir=$(zxfer_mockbin_make_bench_workdir zxfer_perf_ab) ||
	zxfer_perf_ab_die "unable to create a work directory under /tmp"
trap zxfer_perf_ab_cleanup EXIT
trap 'zxfer_perf_ab_cleanup; trap - EXIT; exit 130' INT
trap 'zxfer_perf_ab_cleanup; trap - EXIT; exit 143' TERM
printf 'run_perf_ab.sh: work directory %s\n' "$g_perf_ab_workdir" >&2
# Every run starts in the work directory, so a baseline that writes to a
# relative path cannot litter the caller's directory.
cd "$g_perf_ab_workdir" || zxfer_perf_ab_die "cannot enter the work directory"
g_perf_ab_samples="$g_perf_ab_workdir/samples.tsv"
g_perf_ab_mockdir="$g_perf_ab_workdir/mockbin"
g_perf_ab_secure_path=$(zxfer_mockbin_secure_path_env "$g_perf_ab_mockdir")
if ! mkdir "$g_perf_ab_mockdir" ||
	! zxfer_mockbin_write_canned_zfs "$g_perf_ab_mockdir/zfs" ||
	! zxfer_mockbin_write_socket_ssh "$g_perf_ab_mockdir/ssh"; then
	zxfer_perf_ab_die "unable to build the mock toolchain"
fi
if [ "$g_perf_ab_latency_ms" -gt 0 ]; then
	g_perf_ab_handshake_s=$(awk -v ms="$g_perf_ab_latency_ms" 'BEGIN { printf "%.3f", ms / 1000 }')
	g_perf_ab_mux_s=$(awk -v ms="$g_perf_ab_latency_ms" 'BEGIN { printf "%.4f", ms / 16000 }')
fi

zxfer_perf_ab_extract_baseline
zxfer_perf_ab_start_clock

for l_size in $g_perf_ab_sizes; do
	zxfer_perf_ab_build_fixture "$l_size" ||
		zxfer_perf_ab_die "unable to build the fixture for size $l_size"
	for l_scenario in $g_perf_ab_scenarios; do
		printf 'run_perf_ab.sh: size %s, %s\n' "$l_size" "$l_scenario" >&2
		l_rep=0
		while [ "$l_rep" -le "$g_perf_ab_reps" ]; do
			zxfer_perf_ab_run_once "$l_size" "$l_scenario" candidate "$l_rep"
			zxfer_perf_ab_run_once "$l_size" "$l_scenario" baseline "$l_rep"
			l_rep=$((l_rep + 1))
		done
	done
done

zxfer_perf_ab_report || zxfer_perf_ab_die "unable to summarize the samples"
if [ -n "$g_perf_ab_summary" ]; then
	cat "$g_perf_ab_workdir/summary.md" >>"$g_perf_ab_summary" ||
		zxfer_perf_ab_die "cannot append to --summary $g_perf_ab_summary"
fi
