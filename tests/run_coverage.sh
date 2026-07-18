#!/bin/sh
#
# Run zxfer shunit2 suites under a coverage collector.
# Prefers kcov when available; otherwise falls back to a bash xtrace report.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="$ZXFER_ROOT/tests"
COVERAGE_DIR=${COVERAGE_DIR:-"$ZXFER_ROOT/coverage"}
ZXFER_COVERAGE_MODE=${ZXFER_COVERAGE_MODE:-auto}
ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=${ZXFER_COVERAGE_INCLUDE_ENTRYPOINT:-0}
if [ "${ZXFER_COVERAGE_ENFORCE_POLICY+x}" = x ] &&
	[ -n "$ZXFER_COVERAGE_ENFORCE_POLICY" ]; then
	COVERAGE_POLICY_MODE=$ZXFER_COVERAGE_ENFORCE_POLICY
else
	COVERAGE_POLICY_MODE=auto
	ZXFER_COVERAGE_ENFORCE_POLICY=0
fi
ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE=${ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE:-2}
ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE=${ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE:-4}
COVERAGE_POLICY_FILE=${ZXFER_COVERAGE_POLICY_FILE:-"$TEST_DIR/coverage_policy.tsv"}
COVERAGE_BASELINE_DIR=${ZXFER_COVERAGE_BASELINE_DIR:-"$TEST_DIR/coverage_baseline/bash-xtrace"}
COVERAGE_BASELINE_SUMMARY_FILE=${ZXFER_COVERAGE_BASELINE_SUMMARY_FILE:-"$COVERAGE_BASELINE_DIR/summary.tsv"}
COVERAGE_BASELINE_MISSING_FILE=${ZXFER_COVERAGE_BASELINE_MISSING_FILE:-"$COVERAGE_BASELINE_DIR/missing.txt"}
COVERAGE_ACTIVE_SUITE_PID=
COVERAGE_ACTIVE_SUITE_TOKEN=
COVERAGE_DEFER_SIGNALS=0
COVERAGE_DEFERRED_SIGNAL=
TARGET_LIST_FILE=
COVERAGE_RUN_SCOPE=full
COVERAGE_EXPLICIT_POLICY_MODE=
COVERAGE_SIGNAL_SHUTDOWN_GRACE_SECONDS=${COVERAGE_SIGNAL_SHUTDOWN_GRACE_SECONDS:-2}

print_usage() {
	cat <<'EOF'
Usage: tests/run_coverage.sh [--enforce | --report-only] [--] [suite ...]

Runs the shunit2 suites under a coverage collector and writes results to
./coverage by default.

The bash-xtrace fallback covers sourced shell modules under src/. It excludes
the top-level ./zxfer entrypoint by default because child-shell execution is
not traced reliably without kcov. Set ZXFER_COVERAGE_INCLUDE_ENTRYPOINT=1 to
include it anyway.

Modes:
  auto        Prefer kcov when installed, otherwise use bash xtrace.
  kcov        Require kcov.
  bash-xtrace Require the bash xtrace fallback.

Policy options:
  --enforce      apply the committed policy to a full bash-xtrace run
  --report-only  write coverage reports without applying the policy

Coverage runs are report-only by default. Only a full run with --enforce
applies the repository policy; targeted suite traces are always report-only
because they cannot satisfy the full-tree policy.

Environment:
  ZXFER_COVERAGE_ENFORCE_POLICY=0  force report-only mode
  ZXFER_COVERAGE_ENFORCE_POLICY=1  compatibility alias for full-run --enforce
  ZXFER_COVERAGE_POLICY_FILE       override the minimum-coverage policy file
  ZXFER_COVERAGE_BASELINE_DIR      override the committed bash-xtrace baseline dir

The bash-xtrace mode writes repo-relative summary.tsv and missing.txt reports
and appends a TOTAL row. Full runs compare missing.txt to the committed
baseline for CI and pull request visibility; targeted runs skip that
incomparable full-tree diff.

The committed bash-xtrace baseline uses a small hit-count tolerance during the
no-regression comparison to absorb known shell / platform tracing jitter in the
approximation path.

Committed policy files:
  tests/coverage_policy.tsv
  tests/coverage_baseline/bash-xtrace/summary.tsv
  tests/coverage_baseline/bash-xtrace/missing.txt

Examples:
  tests/run_coverage.sh
  ZXFER_COVERAGE_MODE=bash-xtrace tests/run_coverage.sh tests/test_zxfer_reporting.sh
  ZXFER_COVERAGE_MODE=bash-xtrace tests/run_coverage.sh --enforce
  COVERAGE_DIR=/tmp/zxfer-coverage tests/run_coverage.sh
EOF
}

configure_coverage_policy_enforcement() {
	l_suite_count=$1
	if [ "$l_suite_count" -eq 0 ]; then
		COVERAGE_RUN_SCOPE=full
	else
		COVERAGE_RUN_SCOPE=targeted
	fi

	case "$COVERAGE_POLICY_MODE" in
	0)
		ZXFER_COVERAGE_ENFORCE_POLICY=0
		;;
	1)
		if [ "$l_suite_count" -ne 0 ]; then
			echo "Coverage policy enforcement requires a full run; targeted suites are always report-only." >&2
			return 1
		fi
		ZXFER_COVERAGE_ENFORCE_POLICY=1
		;;
	auto)
		ZXFER_COVERAGE_ENFORCE_POLICY=0
		;;
	*)
		echo "ZXFER_COVERAGE_ENFORCE_POLICY must be 0 or 1." >&2
		return 1
		;;
	esac
}

select_explicit_coverage_policy_mode() {
	l_select_explicit_coverage_policy_mode_requested=$1

	case "$l_select_explicit_coverage_policy_mode_requested" in
	0 | 1) ;;
	*)
		echo "Internal coverage policy mode must be 0 or 1." >&2
		return 1
		;;
	esac
	if [ -n "$COVERAGE_EXPLICIT_POLICY_MODE" ] &&
		[ "$COVERAGE_EXPLICIT_POLICY_MODE" != \
			"$l_select_explicit_coverage_policy_mode_requested" ]; then
		echo "--enforce and --report-only cannot be used together." >&2
		return 1
	fi
	COVERAGE_EXPLICIT_POLICY_MODE=$l_select_explicit_coverage_policy_mode_requested
	COVERAGE_POLICY_MODE=$l_select_explicit_coverage_policy_mode_requested
}

resolve_coverage_collector_mode() {
	case "$ZXFER_COVERAGE_MODE" in
	auto)
		if [ "$ZXFER_COVERAGE_ENFORCE_POLICY" = "1" ]; then
			printf '%s\n' bash-xtrace
		elif command -v kcov >/dev/null 2>&1; then
			printf '%s\n' kcov
		else
			printf '%s\n' bash-xtrace
		fi
		;;
	kcov)
		if [ "$ZXFER_COVERAGE_ENFORCE_POLICY" = "1" ]; then
			echo "Coverage policy enforcement requires ZXFER_COVERAGE_MODE=bash-xtrace." >&2
			return 1
		fi
		printf '%s\n' kcov
		;;
	bash-xtrace)
		printf '%s\n' bash-xtrace
		;;
	*)
		echo "Unknown coverage mode: $ZXFER_COVERAGE_MODE" >&2
		return 1
		;;
	esac
}

resolve_suite_path() {
	l_suite=$1
	case "$l_suite" in
	/*)
		printf '%s\n' "$l_suite"
		;;
	"$TEST_DIR"/*)
		printf '%s\n' "$l_suite"
		;;
	tests/*)
		printf '%s\n' "$ZXFER_ROOT/$l_suite"
		;;
	*)
		printf '%s\n' "$TEST_DIR/$l_suite"
		;;
	esac
}

resolve_suites() {
	if [ "$#" -eq 0 ]; then
		set -- "$TEST_DIR"/test_*.sh
	fi

	for l_suite in "$@"; do
		l_suite_path=$(resolve_suite_path "$l_suite")
		case "$(basename "$l_suite_path")" in
		test_helper.sh)
			continue
			;;
		esac
		if [ ! -f "$l_suite_path" ]; then
			echo "Missing suite: $l_suite_path" >&2
			return 1
		fi
		printf '%s\n' "$l_suite_path"
	done
}

list_coverage_target_labels() {
	if [ "$ZXFER_COVERAGE_INCLUDE_ENTRYPOINT" = "1" ]; then
		printf '%s\n' zxfer
	fi
	for l_coverage_target_path in "$ZXFER_ROOT"/src/*.sh; do
		[ -f "$l_coverage_target_path" ] || continue
		printf '%s\n' "${l_coverage_target_path#"$ZXFER_ROOT"/}"
	done
}

write_target_file_list() {
	l_target_list_file=$1
	list_coverage_target_labels |
		while IFS= read -r l_coverage_target_label; do
			printf '%s\n' "$ZXFER_ROOT/$l_coverage_target_label"
		done >"$l_target_list_file"
}

coverage_expected_policy_targets() {
	{
		list_coverage_target_labels
		printf '%s\n' TOTAL
	} | LC_ALL=C sort
}

coverage_configured_policy_targets() {
	awk -F '\t' '
		/^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
		{
			target = $1
			sub(/^[[:space:]]+/, "", target)
			sub(/[[:space:]]+$/, "", target)
			print target
		}
	' "$COVERAGE_POLICY_FILE" | LC_ALL=C sort
}

coverage_baseline_summary_targets() {
	awk -F '\t' '
		NF {
			target = $5
			sub(/^[[:space:]]+/, "", target)
			sub(/[[:space:]]+$/, "", target)
			print target
		}
	' "$COVERAGE_BASELINE_SUMMARY_FILE" | LC_ALL=C sort
}

check_coverage_policy_target_inventory() {
	l_coverage_inventory_expected=$(coverage_expected_policy_targets)
	l_coverage_inventory_policy=$(coverage_configured_policy_targets)
	l_coverage_inventory_baseline=$(coverage_baseline_summary_targets)
	l_coverage_inventory_status=0

	if [ "$l_coverage_inventory_policy" != "$l_coverage_inventory_expected" ]; then
		printf '%s\n' \
			"Coverage policy targets do not exactly match the production coverage targets." \
			"Expected targets:" "$l_coverage_inventory_expected" \
			"Policy targets:" "$l_coverage_inventory_policy" >&2
		l_coverage_inventory_status=1
	fi
	if [ "$l_coverage_inventory_baseline" != "$l_coverage_inventory_expected" ]; then
		printf '%s\n' \
			"Coverage baseline targets do not exactly match the production coverage targets." \
			"Expected targets:" "$l_coverage_inventory_expected" \
			"Baseline targets:" "$l_coverage_inventory_baseline" >&2
		l_coverage_inventory_status=1
	fi
	return "$l_coverage_inventory_status"
}

run_with_kcov() {
	l_target_list_file=$1
	shift
	mkdir -p "$COVERAGE_DIR/kcov"
	rm -rf "$COVERAGE_DIR/kcov"/*

	l_overall_status=0
	l_kcov_dirs=""
	for l_suite_path in "$@"; do
		l_suite_name=$(basename "$l_suite_path" .sh)
		l_suite_dir="$COVERAGE_DIR/kcov/$l_suite_name"
		echo "==> Running kcov for $l_suite_path"
		if ! kcov --include-pattern="$ZXFER_ROOT/src,$ZXFER_ROOT/zxfer" \
			"$l_suite_dir" "$l_suite_path"; then
			l_overall_status=1
		fi
		l_kcov_dirs="$l_kcov_dirs $l_suite_dir"
	done

	if [ -n "$l_kcov_dirs" ]; then
		# shellcheck disable=SC2086
		set -- $l_kcov_dirs
		kcov --merge "$COVERAGE_DIR/kcov/merged" "$@" >/dev/null
		echo "Coverage report: $COVERAGE_DIR/kcov/merged/index.html"
	fi

	return "$l_overall_status"
}

bash_supports_xtrace_line_numbers() {
	l_bash_bin=$1
	l_probe_script=$(mktemp "${TMPDIR:-/tmp}/zxfer.coverage.probe.XXXXXX") ||
		return 1
	l_probe_file=$l_probe_script.trace
	if ! (
		umask 077 && cat >"$l_probe_script" <<'EOF'
probe() {
	printf '%s\n' ok >/dev/null
}
probe
EOF
	); then
		rm -f "$l_probe_script" "$l_probe_file"
		return 1
	fi
	(
		capture_bash_xtrace_to_file \
			"$l_bash_bin" "$l_probe_file" "$l_probe_script" \
			>/dev/null 2>&1
	) || true

	if grep -Eq '^\+[^:]+:[0-9]+: ' "$l_probe_file" 2>/dev/null; then
		rm -f "$l_probe_script" "$l_probe_file"
		return 0
	fi

	rm -f "$l_probe_script" "$l_probe_file"
	return 1
}

capture_bash_xtrace_to_file() {
	l_bash_bin=$1
	l_trace_file=$2
	shift 2
	[ "$#" -gt 0 ] || return 1
	l_capture_script=$1
	shift
	l_capture_status=0
	l_capture_pid=
	l_capture_token=

	# Keep the coverage trace off fd 9 because send/receive tests exercise
	# their own queue descriptors on 8/9 and may close them during setUp().
	# Apply fd 7 directly to Bash: some POSIX shells mark descriptors opened by
	# an earlier exec builtin close-on-exec inside an asynchronous subshell.
	# Set PS4 inside Bash because privileged/root shells may reject an imported
	# PS4 environment value. $0 and the remaining arguments still match a direct
	# `bash suite [args...]` invocation while the wrapper sources the suite.
	COVERAGE_DEFER_SIGNALS=1
	# shellcheck disable=SC2016  # Expanded by the traced child Bash.
	ZXFER_COVERAGE_BASH_BIN=$l_bash_bin \
		"$l_bash_bin" --noprofile --norc -c '
l_zxfer_coverage_script=$1
shift
PS4="+\${BASH_SOURCE[0]-\$0}:\${LINENO:-0}: "
BASH_XTRACEFD=7
set -x
. "$l_zxfer_coverage_script"
' "$l_capture_script" "$l_capture_script" "$@" \
		7>"$l_trace_file" <&0 &
	l_capture_pid=$!
	l_capture_token=$(coverage_get_process_start_token "$l_capture_pid" 2>/dev/null || true)
	COVERAGE_ACTIVE_SUITE_PID=$l_capture_pid
	COVERAGE_ACTIVE_SUITE_TOKEN=$l_capture_token
	COVERAGE_DEFER_SIGNALS=0
	consume_deferred_coverage_signal
	if wait "$COVERAGE_ACTIVE_SUITE_PID"; then
		l_capture_status=0
	else
		l_capture_status=$?
	fi
	COVERAGE_ACTIVE_SUITE_PID=
	COVERAGE_ACTIVE_SUITE_TOKEN=
	return "$l_capture_status"
}

coverage_signal_exit_status() {
	case "$1" in
	HUP) printf '%s\n' 129 ;;
	INT) printf '%s\n' 130 ;;
	QUIT) printf '%s\n' 131 ;;
	TERM) printf '%s\n' 143 ;;
	*) printf '%s\n' 1 ;;
	esac
}

coverage_signal_number() {
	case "$1" in
	0) printf '%s\n' 0 ;;
	HUP) printf '%s\n' 1 ;;
	INT) printf '%s\n' 2 ;;
	QUIT) printf '%s\n' 3 ;;
	KILL) printf '%s\n' 9 ;;
	TERM) printf '%s\n' 15 ;;
	*) return 1 ;;
	esac
}

coverage_send_signal_to_pid() {
	l_coverage_send_signal_name=$1
	l_coverage_send_signal_pid=$2
	l_coverage_send_signal_number=

	case "$l_coverage_send_signal_pid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	kill -s "$l_coverage_send_signal_name" "$l_coverage_send_signal_pid" >/dev/null 2>&1 && return 0
	kill "-$l_coverage_send_signal_name" "$l_coverage_send_signal_pid" >/dev/null 2>&1 && return 0
	l_coverage_send_signal_number=$(coverage_signal_number "$l_coverage_send_signal_name" 2>/dev/null || true)
	[ -n "$l_coverage_send_signal_number" ] || return 1
	kill "-$l_coverage_send_signal_number" "$l_coverage_send_signal_pid" >/dev/null 2>&1
}

# Return a stable process-start token so a snapshotted descendant PID can be
# distinguished from an unrelated process that later reuses the same number.
coverage_get_process_start_token() {
	l_coverage_token_pid=$1

	case "$l_coverage_token_pid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	l_coverage_token_selector=lstart
	l_coverage_token_raw=$(LC_ALL=C ps -p "$l_coverage_token_pid" -o lstart= 2>/dev/null || :)
	case $- in
	*f*) l_coverage_token_restore_glob=0 ;;
	*)
		l_coverage_token_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_coverage_token_saved_ifs_set=1
		l_coverage_token_saved_ifs=$IFS
	else
		l_coverage_token_saved_ifs_set=0
		l_coverage_token_saved_ifs=
	fi
	unset IFS
	# shellcheck disable=SC2086
	set -- $l_coverage_token_raw
	if [ "$#" -eq 0 ]; then
		l_coverage_token_selector=stime
		l_coverage_token_raw=$(LC_ALL=C ps -p "$l_coverage_token_pid" -o stime= 2>/dev/null || :)
		# shellcheck disable=SC2086
		set -- $l_coverage_token_raw
	fi
	l_coverage_token_normalized=$*
	if [ "$l_coverage_token_saved_ifs_set" -eq 1 ]; then
		IFS=$l_coverage_token_saved_ifs
	else
		unset IFS
	fi
	if [ "$l_coverage_token_restore_glob" -eq 1 ]; then
		set +f
	fi
	[ "$#" -gt 0 ] || return 1
	printf '%s:%s\n' "$l_coverage_token_selector" "$l_coverage_token_normalized"
}

coverage_list_child_pids() {
	l_coverage_children_parent=$1
	l_coverage_children_pgrep=
	l_coverage_children_ps=

	case "$l_coverage_children_parent" in
	'' | *[!0-9]*) return 1 ;;
	esac
	if command -v pgrep >/dev/null 2>&1; then
		l_coverage_children_pgrep=$(pgrep -P "$l_coverage_children_parent" 2>/dev/null || true)
		if [ -n "$l_coverage_children_pgrep" ]; then
			printf '%s\n' "$l_coverage_children_pgrep"
			return 0
		fi
	fi
	if l_coverage_children_ps=$(ps -eo pid= -o ppid= 2>/dev/null); then
		:
	elif l_coverage_children_ps=$(ps -ax -o pid= -o ppid= 2>/dev/null); then
		:
	elif l_coverage_children_ps=$(ps -A -o pid= -o ppid= 2>/dev/null); then
		:
	else
		return 1
	fi
	printf '%s\n' "$l_coverage_children_ps" | awk -v parent="$l_coverage_children_parent" '
		$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $2 == parent { print $1 }
	'
}

coverage_child_pid_matches_parent() {
	l_coverage_child_parent=$1
	l_coverage_child_pid=$2

	for l_coverage_child_current in $(coverage_list_child_pids "$l_coverage_child_parent"); do
		[ "$l_coverage_child_current" = "$l_coverage_child_pid" ] && return 0
	done
	return 1
}

# Capture identity only while the PID is still a child of the expected parent,
# and require the start token to remain stable across that relationship check.
coverage_capture_child_identity() {
	l_coverage_identity_parent=$1
	l_coverage_identity_pid=$2
	l_coverage_identity_before=
	l_coverage_identity_after=

	l_coverage_identity_before=$(coverage_get_process_start_token "$l_coverage_identity_pid") || return 1
	coverage_child_pid_matches_parent "$l_coverage_identity_parent" "$l_coverage_identity_pid" || return 1
	l_coverage_identity_after=$(coverage_get_process_start_token "$l_coverage_identity_pid") || return 1
	[ "$l_coverage_identity_before" = "$l_coverage_identity_after" ] || return 1
	printf '%s\n' "$l_coverage_identity_before"
}

coverage_process_identity_matches() {
	l_coverage_match_pid=$1
	l_coverage_match_expected=$2
	l_coverage_match_current=

	[ -n "$l_coverage_match_expected" ] || return 1
	l_coverage_match_current=$(coverage_get_process_start_token "$l_coverage_match_pid") || return 1
	[ "$l_coverage_match_current" = "$l_coverage_match_expected" ]
}

coverage_tracked_process_running_p() {
	l_coverage_tracked_running_pid=$1
	l_coverage_tracked_running_token=$2

	coverage_process_identity_matches \
		"$l_coverage_tracked_running_pid" \
		"$l_coverage_tracked_running_token" || return 1
	coverage_process_running_p "$l_coverage_tracked_running_pid"
}

coverage_signal_tracked_process() {
	l_coverage_tracked_signal=$1
	l_coverage_tracked_signal_pid=$2
	l_coverage_tracked_signal_token=$3

	case "$l_coverage_tracked_signal_pid" in
	'' | *[!0-9]*) return 0 ;;
	esac
	coverage_process_identity_matches \
		"$l_coverage_tracked_signal_pid" \
		"$l_coverage_tracked_signal_token" || return 0
	coverage_send_signal_to_pid \
		"$l_coverage_tracked_signal" "$l_coverage_tracked_signal_pid" || :
}

# Snapshot descendants before signalling so a parent that exits promptly
# cannot reparent a still-running grandchild out of the traversal.
coverage_collect_process_tree() {
	l_coverage_tree_pending=$1
	l_coverage_tree_next=
	l_coverage_tree_descendants=
	l_coverage_tree_parent=
	l_coverage_tree_child=
	l_coverage_tree_token=
	l_coverage_tree_record=

	while [ -n "$l_coverage_tree_pending" ]; do
		l_coverage_tree_next=
		for l_coverage_tree_parent in $l_coverage_tree_pending; do
			for l_coverage_tree_child in $(coverage_list_child_pids "$l_coverage_tree_parent"); do
				l_coverage_tree_token=$(coverage_capture_child_identity \
					"$l_coverage_tree_parent" "$l_coverage_tree_child") || continue
				l_coverage_tree_next="${l_coverage_tree_next}${l_coverage_tree_next:+ }$l_coverage_tree_child"
				l_coverage_tree_record=$(printf '%s\t%s' \
					"$l_coverage_tree_child" "$l_coverage_tree_token")
				if [ -n "$l_coverage_tree_descendants" ]; then
					l_coverage_tree_descendants="$l_coverage_tree_record
$l_coverage_tree_descendants"
				else
					l_coverage_tree_descendants=$l_coverage_tree_record
				fi
			done
		done
		l_coverage_tree_pending=$l_coverage_tree_next
	done
	printf '%s\n' "$l_coverage_tree_descendants"
}

coverage_process_state() {
	l_coverage_state_pid=$1
	l_coverage_state_value=

	l_coverage_state_value=$(ps -o stat= -p "$l_coverage_state_pid" 2>/dev/null |
		awk '$1 != "STAT" && $1 != "STATE" && $1 != "" { print $1; exit }')
	if [ -z "$l_coverage_state_value" ]; then
		l_coverage_state_value=$(ps -o state= -p "$l_coverage_state_pid" 2>/dev/null |
			awk '$1 != "S" && $1 != "STAT" && $1 != "STATE" && $1 != "" { print $1; exit }')
	fi
	printf '%s\n' "$l_coverage_state_value"
}

coverage_process_running_p() {
	l_coverage_running_pid=$1
	l_coverage_running_state=

	coverage_send_signal_to_pid 0 "$l_coverage_running_pid" || return 1
	l_coverage_running_state=$(coverage_process_state "$l_coverage_running_pid")
	case "$l_coverage_running_state" in
	Z* | z* | *zombie* | *defunct*) return 1 ;;
	esac
	return 0
}

coverage_process_tree_running_p() {
	l_coverage_running_tree_records=$1
	l_coverage_running_tree_tab=$(printf '\t')

	while IFS="$l_coverage_running_tree_tab" read -r \
		l_coverage_running_tree_pid l_coverage_running_tree_token; do
		[ -n "$l_coverage_running_tree_pid" ] || continue
		coverage_process_identity_matches \
			"$l_coverage_running_tree_pid" "$l_coverage_running_tree_token" || continue
		if coverage_process_running_p "$l_coverage_running_tree_pid"; then
			return 0
		fi
	done <<EOF
$l_coverage_running_tree_records
EOF
	return 1
}

coverage_process_tree_exists_p() {
	l_coverage_existing_tree_records=$1
	l_coverage_existing_tree_tab=$(printf '\t')

	while IFS="$l_coverage_existing_tree_tab" read -r \
		l_coverage_existing_tree_pid l_coverage_existing_tree_token; do
		[ -n "$l_coverage_existing_tree_pid" ] || continue
		coverage_process_identity_matches \
			"$l_coverage_existing_tree_pid" "$l_coverage_existing_tree_token" || continue
		if coverage_send_signal_to_pid 0 "$l_coverage_existing_tree_pid"; then
			return 0
		fi
	done <<EOF
$l_coverage_existing_tree_records
EOF
	return 1
}

coverage_signal_process_tree() {
	l_coverage_signal_tree_signal=$1
	l_coverage_signal_tree_records=$2
	l_coverage_signal_tree_tab=$(printf '\t')

	while IFS="$l_coverage_signal_tree_tab" read -r \
		l_coverage_signal_tree_pid l_coverage_signal_tree_token; do
		[ -n "$l_coverage_signal_tree_pid" ] || continue
		coverage_process_identity_matches \
			"$l_coverage_signal_tree_pid" "$l_coverage_signal_tree_token" || continue
		coverage_send_signal_to_pid "$l_coverage_signal_tree_signal" "$l_coverage_signal_tree_pid" || :
	done <<EOF
$l_coverage_signal_tree_records
EOF
}

coverage_wait_for_process_tree_shutdown() {
	l_coverage_wait_tree_records=$1
	l_coverage_wait_tree_remaining=$COVERAGE_SIGNAL_SHUTDOWN_GRACE_SECONDS

	case "$l_coverage_wait_tree_remaining" in
	'' | *[!0-9]*) l_coverage_wait_tree_remaining=2 ;;
	esac
	while [ "$l_coverage_wait_tree_remaining" -gt 0 ]; do
		coverage_process_tree_running_p "$l_coverage_wait_tree_records" || return 0
		sleep 1 || :
		l_coverage_wait_tree_remaining=$((l_coverage_wait_tree_remaining - 1))
	done
	coverage_process_tree_running_p "$l_coverage_wait_tree_records" && return 1
	return 0
}

coverage_wait_for_direct_process_shutdown() {
	l_coverage_wait_pid=$1
	l_coverage_wait_token=$2
	l_coverage_wait_pid_remaining=$COVERAGE_SIGNAL_SHUTDOWN_GRACE_SECONDS

	case "$l_coverage_wait_pid_remaining" in
	'' | *[!0-9]*) l_coverage_wait_pid_remaining=2 ;;
	esac
	while [ "$l_coverage_wait_pid_remaining" -gt 0 ]; do
		coverage_tracked_process_running_p \
			"$l_coverage_wait_pid" "$l_coverage_wait_token" || return 0
		sleep 1 || :
		l_coverage_wait_pid_remaining=$((l_coverage_wait_pid_remaining - 1))
	done
	coverage_tracked_process_running_p \
		"$l_coverage_wait_pid" "$l_coverage_wait_token" && return 1
	return 0
}

# Once the directly owned suite has been waited for, give the system reaper a
# bounded opportunity to remove any orphaned descendant zombies as well.
coverage_wait_for_process_tree_reap() {
	l_coverage_reap_tree_pids=$1
	l_coverage_reap_tree_remaining=$COVERAGE_SIGNAL_SHUTDOWN_GRACE_SECONDS

	case "$l_coverage_reap_tree_remaining" in
	'' | *[!0-9]*) l_coverage_reap_tree_remaining=2 ;;
	esac
	while [ "$l_coverage_reap_tree_remaining" -gt 0 ]; do
		coverage_process_tree_exists_p "$l_coverage_reap_tree_pids" || return 0
		sleep 1 || :
		l_coverage_reap_tree_remaining=$((l_coverage_reap_tree_remaining - 1))
	done
	coverage_process_tree_exists_p "$l_coverage_reap_tree_pids" && return 1
	return 0
}

cleanup_coverage_runner() {
	if [ -n "${TARGET_LIST_FILE:-}" ]; then
		rm -f "$TARGET_LIST_FILE"
		TARGET_LIST_FILE=
	fi
}

remember_deferred_coverage_signal() {
	l_coverage_deferred_signal=$1

	if [ -z "${COVERAGE_DEFERRED_SIGNAL:-}" ]; then
		COVERAGE_DEFERRED_SIGNAL=$l_coverage_deferred_signal
	fi
}

consume_deferred_coverage_signal() {
	if [ -z "${COVERAGE_DEFERRED_SIGNAL:-}" ]; then
		return 0
	fi

	l_coverage_deferred_signal=$COVERAGE_DEFERRED_SIGNAL
	COVERAGE_DEFERRED_SIGNAL=
	handle_coverage_signal "$l_coverage_deferred_signal"
}

handle_coverage_signal() {
	l_signal=$1
	l_exit_status=$(coverage_signal_exit_status "$l_signal")
	l_coverage_signal_descendants=
	l_coverage_signal_pid=
	l_coverage_signal_token=

	if [ "${COVERAGE_DEFER_SIGNALS:-0}" = "1" ]; then
		remember_deferred_coverage_signal "$l_signal"
		return 0
	fi

	trap - EXIT HUP INT TERM QUIT
	case "${COVERAGE_ACTIVE_SUITE_PID:-}" in
	'' | *[!0-9]*) ;;
	*)
		l_coverage_signal_pid=$COVERAGE_ACTIVE_SUITE_PID
		l_coverage_signal_token=$COVERAGE_ACTIVE_SUITE_TOKEN
		if ! coverage_process_identity_matches \
			"$l_coverage_signal_pid" "$l_coverage_signal_token"; then
			COVERAGE_ACTIVE_SUITE_PID=
			COVERAGE_ACTIVE_SUITE_TOKEN=
			cleanup_coverage_runner
			exit "$l_exit_status"
		fi
		l_coverage_signal_descendants=$(coverage_collect_process_tree \
			"$l_coverage_signal_pid")
		# Keep the directly owned suite alive while its descendants stop so it
		# can reap them. Killing every level simultaneously leaves transient
		# orphan zombies on platforms whose system reaper runs less eagerly.
		if [ -n "$l_coverage_signal_descendants" ]; then
			coverage_signal_process_tree "$l_signal" "$l_coverage_signal_descendants"
			if ! coverage_wait_for_process_tree_shutdown "$l_coverage_signal_descendants"; then
				coverage_signal_process_tree KILL "$l_coverage_signal_descendants"
				coverage_wait_for_process_tree_shutdown "$l_coverage_signal_descendants" || :
			fi
		fi
		coverage_signal_tracked_process \
			"$l_signal" "$l_coverage_signal_pid" "$l_coverage_signal_token"
		if ! coverage_wait_for_direct_process_shutdown \
			"$l_coverage_signal_pid" "$l_coverage_signal_token"; then
			coverage_signal_tracked_process \
				KILL "$l_coverage_signal_pid" "$l_coverage_signal_token"
			coverage_wait_for_direct_process_shutdown \
				"$l_coverage_signal_pid" "$l_coverage_signal_token" || :
		fi
		wait "$l_coverage_signal_pid" >/dev/null 2>&1 || :
		COVERAGE_ACTIVE_SUITE_PID=
		COVERAGE_ACTIVE_SUITE_TOKEN=
		coverage_wait_for_process_tree_reap "$l_coverage_signal_descendants" || :
		;;
	esac
	cleanup_coverage_runner
	exit "$l_exit_status"
}

render_bash_xtrace_report() {
	l_target_list_file=$1
	l_trace_file=$2
	l_summary_file=$3
	l_missing_file=$4

	: >"$l_summary_file"
	: >"$l_missing_file"

	awk -v target_list_file="$l_target_list_file" \
		-v merged_trace_file="$l_trace_file" \
		-v summary_file="$l_summary_file" \
		-v missing_file="$l_missing_file" \
		-v zxfer_root="$ZXFER_ROOT" '
function trim(s) {
	sub(/^[[:space:]]+/, "", s)
	sub(/[[:space:]]+$/, "", s)
	return s
}
function canonicalize_path(path,    is_abs, part_count, i, part, out_count, result) {
	gsub(/\/+/, "/", path)
	is_abs = (substr(path, 1, 1) == "/")
	part_count = split(path, path_parts, "/")
	for (i in canonical_parts) {
		delete canonical_parts[i]
	}
	out_count = 0
	for (i = 1; i <= part_count; i++) {
		part = path_parts[i]
		if (part == "" || part == ".") {
			continue
		}
		if (part == "..") {
			if (out_count > 0 && canonical_parts[out_count] != "..") {
				delete canonical_parts[out_count]
				out_count--
			} else if (!is_abs) {
				out_count++
				canonical_parts[out_count] = part
			}
			continue
		}
		out_count++
		canonical_parts[out_count] = part
	}
	if (out_count == 0) {
		return is_abs ? "/" : "."
	}
	result = is_abs ? "/" canonical_parts[1] : canonical_parts[1]
	for (i = 2; i <= out_count; i++) {
		result = result "/" canonical_parts[i]
	}
	return result
}
function normalize_path(path, root_prefix) {
	if (root_prefix != "" && index(path, root_prefix) == 1) {
		return substr(path, length(root_prefix) + 1)
	}
	return path
}
function starts_shell_comment_at(line, position,    previous) {
	if (substr(line, position, 1) != "#") {
		return 0
	}
	if (position == 1) {
		return 1
	}
	previous = substr(line, position - 1, 1)
	return (previous == " " || previous == "\t" ||
		previous == ";" || previous == "|" || previous == "&" ||
		previous == "(" || previous == ")" ||
		previous == "<" || previous == ">")
}
function double_quote_state_after_line(line, in_double_quote,    i, ch, escaped, in_single_quote) {
	escaped = 0
	in_single_quote = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		if (in_single_quote) {
			if (ch == "'\''") {
				in_single_quote = 0
			}
			continue
		}
		if (escaped) {
			escaped = 0
			continue
		}
		if (ch == "\\") {
			escaped = 1
			continue
		}
		if (in_double_quote) {
			if (ch == "\"") {
				in_double_quote = 0
			}
			continue
		}
		if (starts_shell_comment_at(line, i)) {
			break
		}
		if (ch == "'\''") {
			in_single_quote = 1
			continue
		}
		if (ch == "\"") {
			in_double_quote = 1
		}
	}
	return in_double_quote
}
function has_unbalanced_double_quote(line) {
	return double_quote_state_after_line(line, 0)
}
function continues_multiline_double_quote(line) {
	return double_quote_state_after_line(line, 1)
}
function single_quote_state_after_line(line, in_single_quote,    i, ch, escaped, in_double_quote) {
	escaped = 0
	in_double_quote = 0
	for (i = 1; i <= length(line); i++) {
		ch = substr(line, i, 1)
		if (in_single_quote) {
			if (ch == "'\''") {
				in_single_quote = 0
			}
			continue
		}
		if (escaped) {
			escaped = 0
			continue
		}
		if (ch == "\\") {
			escaped = 1
			continue
		}
		if (ch == "\"") {
			in_double_quote = !in_double_quote
			continue
		}
		if (!in_double_quote && starts_shell_comment_at(line, i)) {
			break
		}
		if (!in_double_quote && ch == "'\''") {
			in_single_quote = 1
		}
	}
	return in_single_quote
}
function has_unbalanced_single_quote(line) {
	return single_quote_state_after_line(line, 0)
}
function continues_multiline_single_quote(line) {
	return single_quote_state_after_line(line, 1)
}
function starts_multiline_single_quote(line, t) {
	return has_unbalanced_single_quote(line)
}
function count_trailing_backslashes(line,    i, ch, count) {
	count = 0
	for (i = length(line); i >= 1; i--) {
		ch = substr(line, i, 1)
		if (ch == " " || ch == "\t")
			continue
		if (ch != "\\")
			break
		count++
	}
	return count
}
function ends_with_line_continuation(line, t, trailing_backslashes) {
	t = trim(line)
	if (t == "")
		return 0
	trailing_backslashes = count_trailing_backslashes(line)
	return (trailing_backslashes % 2) == 1
}
function heredoc_delimiter(line,    match_count, start, length_part, delimiter) {
	match_count = match(line, /<<-?[[:space:]]*[A-Za-z_][A-Za-z0-9_]*/)
	if (match_count == 0) {
		return ""
	}
	start = RSTART
	length_part = RLENGTH
	delimiter = substr(line, start, length_part)
	sub(/^<<-?[[:space:]]*/, "", delimiter)
	return delimiter
}
function is_case_pattern_line(line, t) {
	t = trim(line)
	if (coverage_case_depth == 0) {
		return 0
	}
	if (t ~ /^esac$/) {
		return 0
	}
	return (t ~ /^.+\)[[:space:]]*(;;)?$/)
}
function starts_multiline_command_substitution(line, t) {
	t = trim(line)
	return (t ~ /\$\([[:space:]]*$/)
}
function opens_command_substitution_subshell(line, t) {
	t = trim(line)
	return (t == "(")
}
function closes_command_substitution_scope(line, t) {
	t = trim(line)
	return (t ~ /^\)/)
}
function is_coverable_line(line, t, l_heredoc_delimiter) {
	t = trim(line)
	if (coverage_in_heredoc == 1) {
		if (t == coverage_heredoc_delimiter) {
			coverage_in_heredoc = 0
			coverage_heredoc_delimiter = ""
		}
		return 0
	}
	if (coverage_in_command_substitution == 1) {
		if (starts_multiline_command_substitution(line) || opens_command_substitution_subshell(line)) {
			coverage_command_substitution_depth++
		}
		if (closes_command_substitution_scope(line)) {
			coverage_command_substitution_depth--
			if (coverage_command_substitution_depth <= 0) {
				coverage_in_command_substitution = 0
				coverage_command_substitution_depth = 0
			}
		}
		return 0
	}
	if (coverage_in_multiline_double_quote == 1) {
		if (!continues_multiline_double_quote(line)) {
			coverage_in_multiline_double_quote = 0
		}
		return 0
	}
	if (coverage_in_multiline_single_quote == 1) {
		if (!continues_multiline_single_quote(line)) {
			coverage_in_multiline_single_quote = 0
		}
		return 0
	}
	if (coverage_in_backslash_continuation == 1) {
		if (starts_multiline_command_substitution(line)) {
			coverage_in_command_substitution = 1
			coverage_command_substitution_depth = 1
		} else if (has_unbalanced_double_quote(line)) {
			coverage_in_multiline_double_quote = 1
		} else if (starts_multiline_single_quote(line)) {
			coverage_in_multiline_single_quote = 1
		}
		if (!ends_with_line_continuation(line)) {
			coverage_in_backslash_continuation = 0
		}
		return 0
	}
	if (t == "") return 0
	if (t ~ /^#/) return 0
	if (t ~ /^[{}()]$/) return 0
	if (t ~ /^;;$/) return 0
	if (t ~ /^(then|do|else|fi|done|in)$/) return 0
	if (t ~ /^[A-Za-z_][A-Za-z0-9_]*\(\)[[:space:]]*\{$/) return 0
	if (t ~ /^case[[:space:]].*[[:space:]]in$/) {
		coverage_case_depth++
		return 0
	}
	if (t ~ /^esac$/) {
		if (coverage_case_depth > 0) {
			coverage_case_depth--
		}
		return 0
	}
	if (starts_multiline_command_substitution(line)) {
		coverage_in_command_substitution = 1
		coverage_command_substitution_depth = 1
		return 0
	}
	if (is_case_pattern_line(line)) return 0
	l_heredoc_delimiter = heredoc_delimiter(line)
	if (l_heredoc_delimiter != "") {
		coverage_in_heredoc = 1
		coverage_heredoc_delimiter = l_heredoc_delimiter
		if (t ~ /^(done|[{}])[[:space:]].*<<-?[[:space:]]*[A-Za-z_][A-Za-z0-9_]*$/) {
			return 0
		}
	}
	if (has_unbalanced_double_quote(line)) {
		coverage_in_multiline_double_quote = 1
		return 0
	}
	if (starts_multiline_single_quote(line)) {
		coverage_in_multiline_single_quote = 1
		return 0
	}
	if (ends_with_line_continuation(line)) {
		coverage_in_backslash_continuation = 1
	}
	return 1
}
BEGIN {
	root_prefix = canonicalize_path(zxfer_root) "/"
	while ((getline file < target_list_file) > 0) {
		normalized_file = canonicalize_path(file)
		target[normalized_file] = 1
		files[++file_count] = normalized_file
		target_label[normalized_file] = normalize_path(normalized_file, root_prefix)
		line_no = 0
		while ((getline source_line < file) > 0) {
			line_no++
			source[normalized_file, line_no] = source_line
			if (is_coverable_line(source_line)) {
				coverable[normalized_file, line_no] = 1
				coverable_count[normalized_file]++
			}
		}
		close(file)
	}
	while ((getline trace_line < merged_trace_file) > 0) {
		if (trace_line ~ /^\++[^:]+:[0-9]+: /) {
			sub(/^\++/, "", trace_line)
			trace_file = trace_line
			sub(/:[0-9]+: .*/, "", trace_file)
			trace_file = canonicalize_path(trace_file)
			trace_line_no = trace_line
			sub(/^[^:]+:/, "", trace_line_no)
			sub(/: .*/, "", trace_line_no)
			trace_line_no += 0
			if ((trace_file in target) && ((trace_file, trace_line_no) in coverable)) {
				hit[trace_file, trace_line_no] = 1
			}
		}
	}
	close(merged_trace_file)

	for (i = 1; i <= file_count; i++) {
		file = files[i]
		hit_count[file] = 0
		for (key in hit) {
			split(key, parts, SUBSEP)
			if (parts[1] == file) {
				hit_count[file]++
			}
		}
		miss_count[file] = coverable_count[file] - hit_count[file]
		if (coverable_count[file] > 0) {
			pct = (hit_count[file] * 100.0) / coverable_count[file]
		} else {
			pct = 100.0
		}
		printf "%.2f\t%d\t%d\t%d\t%s\n", pct, coverable_count[file], hit_count[file], miss_count[file], target_label[file] >> summary_file

		if (miss_count[file] > 0) {
			printf "%s\n", target_label[file] >> missing_file
			for (line_no = 1; (file, line_no) in source; line_no++) {
				if ((file, line_no) in coverable && !((file, line_no) in hit)) {
					printf "  %d:%s\n", line_no, source[file, line_no] >> missing_file
				}
			}
			printf "\n" >> missing_file
		}
	}
}
' /dev/null
}

append_total_summary_row() {
	l_summary_file=$1
	l_tmp_file=$l_summary_file.tmp.$$

	awk -F '\t' '
BEGIN {
	OFS = "\t"
	total_coverable = 0
	total_hit = 0
	total_miss = 0
}
NF >= 5 && $5 != "TOTAL" {
	print $0
	total_coverable += $2
	total_hit += $3
	total_miss += $4
}
END {
	if (total_coverable > 0) {
		pct = (total_hit * 100.0) / total_coverable
	} else {
		pct = 100.0
	}
	printf "%.2f\t%d\t%d\t%d\tTOTAL\n", pct, total_coverable, total_hit, total_miss
}
' "$l_summary_file" >"$l_tmp_file"
	mv "$l_tmp_file" "$l_summary_file"
}

write_missing_diff_file() {
	l_missing_file=$1
	l_missing_diff_file=$2
	l_missing_diff_scope=${3:-full}
	l_status=0

	case "$l_missing_diff_scope" in
	targeted)
		printf '%s\n' \
			"Full-baseline missing-line diff skipped for targeted coverage run." \
			>"$l_missing_diff_file"
		return 0
		;;
	full) ;;
	*)
		echo "Unknown coverage run scope: $l_missing_diff_scope" >&2
		return 1
		;;
	esac

	if [ ! -f "$COVERAGE_BASELINE_MISSING_FILE" ]; then
		printf '%s\n' "Committed missing.txt baseline not found: $COVERAGE_BASELINE_MISSING_FILE" >"$l_missing_diff_file"
		return 0
	fi

	set +e
	diff -u "$COVERAGE_BASELINE_MISSING_FILE" "$l_missing_file" >"$l_missing_diff_file"
	l_status=$?
	set -e
	case "$l_status" in
	0)
		printf '%s\n' "No missing-line changes relative to $COVERAGE_BASELINE_MISSING_FILE." >"$l_missing_diff_file"
		;;
	1)
		:
		;;
	*)
		return "$l_status"
		;;
	esac
}

write_policy_disabled_report() {
	l_policy_report_file=$1
	l_policy_failures_file=$2

	{
		printf '%s\n' "Coverage policy enforcement disabled (report-only run)."
		printf '%s\n' "No minimum or no-regression checks were applied."
	} >"$l_policy_report_file"
	printf '%s\n' "type	target	current_pct	required_pct	note" >"$l_policy_failures_file"
}

enforce_bash_xtrace_policy() {
	l_summary_file=$1
	l_policy_report_file=$2
	l_policy_failures_file=$3

	awk -F '\t' \
		-v summary_file="$l_summary_file" \
		-v policy_file="$COVERAGE_POLICY_FILE" \
		-v baseline_file="$COVERAGE_BASELINE_SUMMARY_FILE" \
		-v regression_hit_tolerance="${ZXFER_COVERAGE_REGRESSION_HIT_TOLERANCE:-2}" \
		-v total_regression_hit_tolerance="${ZXFER_COVERAGE_TOTAL_REGRESSION_HIT_TOLERANCE:-4}" \
		-v report_file="$l_policy_report_file" \
		-v failures_file="$l_policy_failures_file" '
function trim(s) {
	sub(/^[[:space:]]+/, "", s)
	sub(/[[:space:]]+$/, "", s)
	return s
}
function format_pct(value) {
	if (value == "") {
		return "-"
	}
	return sprintf("%.2f", value + 0)
}
function record_failure(type, target, current, expected, note) {
	failures++
	failure_type[failures] = type
	failure_target[failures] = target
	failure_current[failures] = current
	failure_expected[failures] = expected
	failure_note[failures] = note
}
function read_policy_file(   line, fields, target, min_pct) {
	while ((getline line < policy_file) > 0) {
		line = trim(line)
		if (line == "" || line ~ /^#/) {
			continue
		}
		split(line, fields, "\t")
		target = trim(fields[1])
		min_pct = trim(fields[2])
		if (target == "" || min_pct == "") {
			record_failure("invalid-policy", policy_file, "", "", "Malformed policy line: " line)
			continue
		}
		policy_min[target] = min_pct + 0
		policy_seen[target] = 1
	}
	close(policy_file)
}
function read_summary_file(path, pct_store, hit_store, seen_store,   line, fields, target, pct, hit) {
	while ((getline line < path) > 0) {
		if (line == "") {
			continue
		}
		split(line, fields, "\t")
		target = trim(fields[5])
		pct = trim(fields[1])
		hit = trim(fields[3])
		if (target == "" || pct == "" || hit == "") {
			record_failure("invalid-summary", path, "", "", "Malformed summary line: " line)
			continue
		}
		pct_store[target] = pct + 0
		hit_store[target] = hit + 0
		seen_store[target] = 1
	}
	close(path)
}
BEGIN {
	read_policy_file()
	read_summary_file(baseline_file, baseline_pct, baseline_hit, baseline_seen)
	read_summary_file(summary_file, current_pct, current_hit, current_seen)

	if (!("TOTAL" in current_seen)) {
		record_failure("missing-total", "TOTAL", "", "", "Current summary.tsv is missing the TOTAL row.")
	}

	for (target in current_seen) {
		if (!(target in policy_seen)) {
			record_failure("missing-policy", target, current_pct[target], "", "Target missing from coverage policy.")
		}
		if (!(target in baseline_seen)) {
			record_failure("missing-baseline", target, current_pct[target], "", "Target missing from committed coverage baseline.")
		}
	}

	for (target in policy_seen) {
		if (!(target in current_seen) && !(target in reported_missing_current)) {
			record_failure("missing-current", target, "", policy_min[target], "Policy target missing from current summary.")
			reported_missing_current[target] = 1
		}
	}

	for (target in baseline_seen) {
		if (!(target in current_seen) && !(target in reported_missing_current)) {
			record_failure("missing-current", target, "", baseline_pct[target], "Baseline target missing from current summary.")
			reported_missing_current[target] = 1
		}
	}

	for (target in current_seen) {
		if ((target in policy_seen) && (current_pct[target] + 0.000001 < policy_min[target])) {
			record_failure("minimum", target, current_pct[target], policy_min[target], "Coverage fell below the configured minimum.")
		}
		if ((target in baseline_seen) && (current_pct[target] + 0.000001 < baseline_pct[target])) {
			regression_tolerance = (target == "TOTAL" ? total_regression_hit_tolerance + 0 : regression_hit_tolerance + 0)
			if (!(target in current_hit) || !(target in baseline_hit) ||
				(current_hit[target] + regression_tolerance) < baseline_hit[target]) {
				record_failure("regression", target, current_pct[target], baseline_pct[target], "Coverage regressed relative to the committed baseline.")
			}
		}
	}

	print "type\ttarget\tcurrent_pct\trequired_pct\tnote" > failures_file
	for (i = 1; i <= failures; i++) {
		printf "%s\t%s\t%s\t%s\t%s\n", \
			failure_type[i], \
			failure_target[i], \
			format_pct(failure_current[i]), \
			format_pct(failure_expected[i]), \
			failure_note[i] >> failures_file
	}

	if (failures > 0) {
		print "Coverage policy failed." > report_file
		print "Minimums: " policy_file >> report_file
		print "Baseline: " baseline_file >> report_file
		print "" >> report_file
		for (i = 1; i <= failures; i++) {
			printf "- %s: %s (current=%s required=%s) %s\n", \
				failure_type[i], \
				failure_target[i], \
				format_pct(failure_current[i]), \
				format_pct(failure_expected[i]), \
				failure_note[i] >> report_file
		}
		exit 1
	}

	print "Coverage policy passed." > report_file
	print "Minimums: " policy_file >> report_file
	print "Baseline: " baseline_file >> report_file
	exit 0
}
' /dev/null
}

run_with_bash_xtrace() {
	l_target_list_file=$1
	shift
	l_bash_bin=${ZXFER_COVERAGE_BASH_BIN:-}
	if [ -z "$l_bash_bin" ]; then
		# Preserve the legacy BASH_BIN override without using a direct
		# $BASH_BIN expansion, which checkbashisms flags in POSIX scripts.
		l_bash_bin=$(env | awk -F= '
			$1 == "BASH_BIN" {
				sub(/^[^=]*=/, "", $0)
				print $0
				exit
			}
		')
	fi
	if [ -z "$l_bash_bin" ]; then
		l_bash_bin=$(command -v bash || true)
	fi
	if [ -z "$l_bash_bin" ]; then
		echo "bash is required for ZXFER_COVERAGE_MODE=bash-xtrace." >&2
		return 1
	fi
	if ! bash_supports_xtrace_line_numbers "$l_bash_bin"; then
		echo "The selected bash does not support PS4 line-number tracing." >&2
		return 1
	fi

	mkdir -p "$COVERAGE_DIR/bash-xtrace"
	l_trace_dir=$(mktemp -d "${TMPDIR:-/tmp}/zxfer.coverage.XXXXXX")
	l_merged_trace="$COVERAGE_DIR/bash-xtrace/merged.trace"
	l_summary_file="$COVERAGE_DIR/bash-xtrace/summary.tsv"
	l_missing_file="$COVERAGE_DIR/bash-xtrace/missing.txt"
	l_missing_diff_file="$COVERAGE_DIR/bash-xtrace/missing.diff"
	l_policy_report_file="$COVERAGE_DIR/bash-xtrace/policy_report.txt"
	l_policy_failures_file="$COVERAGE_DIR/bash-xtrace/policy_failures.tsv"
	: >"$l_merged_trace"
	: >"$l_summary_file"
	: >"$l_missing_file"

	l_overall_status=0
	for l_suite_path in "$@"; do
		l_suite_name=$(basename "$l_suite_path" .sh)
		l_trace_file="$l_trace_dir/$l_suite_name.trace"
		echo "==> Running bash-xtrace coverage for $l_suite_path"
		if ! capture_bash_xtrace_to_file "$l_bash_bin" "$l_trace_file" "$l_suite_path"; then
			l_overall_status=1
		fi
		cat "$l_trace_file" >>"$l_merged_trace"
	done

	render_bash_xtrace_report "$l_target_list_file" "$l_merged_trace" "$l_summary_file" "$l_missing_file"
	append_total_summary_row "$l_summary_file"
	write_missing_diff_file \
		"$l_missing_file" "$l_missing_diff_file" "$COVERAGE_RUN_SCOPE"

	l_policy_status=0
	if [ "$ZXFER_COVERAGE_ENFORCE_POLICY" = "0" ]; then
		write_policy_disabled_report "$l_policy_report_file" "$l_policy_failures_file"
	else
		if ! enforce_bash_xtrace_policy "$l_summary_file" "$l_policy_report_file" "$l_policy_failures_file"; then
			l_policy_status=1
		fi
	fi

	echo "Coverage summary: $l_summary_file"
	echo "Missing lines: $l_missing_file"
	echo "Missing diff: $l_missing_diff_file"
	echo "Coverage policy report: $l_policy_report_file"
	echo
	echo "Approximate line coverage (bash xtrace fallback):"
	sort -rn "$l_summary_file" | awk -F '\t' '
BEGIN {
	printf "%-8s %-10s %-10s %-10s %s\n", "pct", "coverable", "hit", "miss", "file"
}
{
	printf "%-8s %-10s %-10s %-10s %s\n", $1 "%", $2, $3, $4, $5
}'

	rm -rf "$l_trace_dir"
	if [ "$l_overall_status" -ne 0 ]; then
		return "$l_overall_status"
	fi
	return "$l_policy_status"
}

main() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		-h | --help)
			print_usage
			exit 0
			;;
		--enforce)
			select_explicit_coverage_policy_mode 1 || exit 1
			;;
		--report-only)
			select_explicit_coverage_policy_mode 0 || exit 1
			;;
		--)
			shift
			break
			;;
		-*)
			echo "Unknown argument: $1" >&2
			exit 1
			;;
		*)
			break
			;;
		esac
		shift
	done

	configure_coverage_policy_enforcement "$#"
	COVERAGE_COLLECTOR_MODE=$(resolve_coverage_collector_mode)

	SUITES=$(resolve_suites "$@")
	if [ -z "$SUITES" ]; then
		echo "No shunit2 suites found." >&2
		exit 1
	fi
	if [ "$ZXFER_COVERAGE_ENFORCE_POLICY" = "1" ]; then
		check_coverage_policy_target_inventory || exit 1
	fi

	mkdir -p "$COVERAGE_DIR"
	TARGET_LIST_FILE=$(mktemp "${TMPDIR:-/tmp}/zxfer.coverage.targets.XXXXXX")
	trap 'cleanup_coverage_runner' EXIT
	trap 'handle_coverage_signal HUP' HUP
	trap 'handle_coverage_signal INT' INT
	trap 'handle_coverage_signal QUIT' QUIT
	trap 'handle_coverage_signal TERM' TERM
	write_target_file_list "$TARGET_LIST_FILE"

	case "$COVERAGE_COLLECTOR_MODE" in
	kcov)
		if ! command -v kcov >/dev/null 2>&1; then
			echo "kcov is not installed." >&2
			exit 1
		fi
		# shellcheck disable=SC2086
		run_with_kcov "$TARGET_LIST_FILE" $SUITES
		;;
	bash-xtrace)
		# shellcheck disable=SC2086
		run_with_bash_xtrace "$TARGET_LIST_FILE" $SUITES
		;;
	esac
}

if [ "${ZXFER_RUN_COVERAGE_SOURCE_ONLY:-0}" != "1" ]; then
	main "$@"
fi
