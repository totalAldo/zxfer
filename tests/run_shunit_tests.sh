#!/bin/sh
#
# Run the zxfer shunit2 suites (tests/test_*.sh, or the suites named on the
# command line) with a bounded worker pool.
#
# Each suite runs under a worker subshell that writes "done ID STATUS" to a
# FIFO the runner reads, so a finished suite is noticed at once. Parallel
# output is buffered per suite and replayed in suite order; --jobs 1 streams
# it live. With more than one job the known-slow suites (RUNNER_SLOW_SUITES)
# start first. A ticker writes "tick" to the same FIFO once a second: the
# watchdog counts ticks to stop a suite that runs longer than --suite-timeout,
# and signal teardown counts them to bound its grace period. Signal traps
# only record the signal; the main loop tears down after the next event.
#
# Stopping a suite is best effort: TERM, then KILL, to the suite and every
# descendant of its worker found in one process-table snapshot. A process that
# leaves the tree (its parent exited) or starts its own session is missed, and
# a PID that exits and is reused between the snapshot and the signal could be
# hit; the window is milliseconds and only open on a timeout or a signal.
#
# Case patterns list their characters ([!0123456789], not [!0-9]): bash 3.2,
# which is macOS /bin/sh, matches a range by locale collation, and in a UTF-8
# locale [a-z] also takes A-Y and accented letters and [0-9] takes U+2185.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="$ZXFER_ROOT/tests"
TAB=$(printf '\t')

RUNNER_REQUESTED_JOBS=
RUNNER_PARALLEL_JOBS=1
RUNNER_SUITE_TIMEOUT=${ZXFER_TEST_SUITE_TIMEOUT:-900}
RUNNER_SKIP_TOOL_SUITES=0
RUNNER_LIST_MODE=
RUNNER_CURRENT_SUITE_OPTION=
RUNNER_SELECTED_SUITES=
RUNNER_NAMED_TEST_SELECTIONS=
RUNNER_POSITIONAL_TEST_NAMES=
RUNNER_HAS_NAMED_TESTS=0

# The suites that take far longer than the rest, longest first (each took 10
# to 80 s alone under macOS /bin/sh in 2026-09; the other suites take 9 s or
# less). With more than one job these start before the others, so that no
# long suite starts last and sets the length of the whole run
# (longest-processing-time-first scheduling); output is still replayed in
# suite order. A name that is not selected is ignored. Refresh the list when
# suite times shift: time each suite alone with
# ./tests/run_shunit_tests.sh SUITE.
RUNNER_SLOW_SUITES="
test_contract_failures.sh
test_contract_planning.sh
test_run_argv_fuzz.sh
test_contract_properties.sh
test_contract_send_receive.sh
test_contract_backup.sh
test_run_shunit_tests.sh
test_contract_remote.sh
test_run_perf_ab.sh
"

# Run state. RUNNER_WORKERS holds one ID:PID:START_TICK:PHASE:PHASE_TICK word
# per running worker; PHASE is run, term (watchdog sent TERM) or kill. A
# suite's ID is its position among the selected suites, so replay follows
# suite order whatever order the suites start in.
RUNNER_STATE_DIR=
RUNNER_TICKER_PID=
RUNNER_TICKS=0
RUNNER_GRACE_TICKS=3
RUNNER_WORKERS=
RUNNER_INFLIGHT=0
RUNNER_TOTAL=0
RUNNER_NEXT_REPLAY=1
RUNNER_PENDING_SIGNAL=
RUNNER_STOPPING=0
TEST_SHELL_RUNNER=
TEST_SHELL_LABEL=
overall_status=0
passed_count=0
failed_count=0

print_usage() {
	cat <<'EOF'
Usage: tests/run_shunit_tests.sh [options] [--] [suite ...]

Runs every shunit2 suite (tests/test_*.sh) when no arguments are provided.
Pass specific suite paths to limit execution, e.g.:

  tests/run_shunit_tests.sh --jobs 4
  tests/run_shunit_tests.sh test_zxfer_reporting.sh
  tests/run_shunit_tests.sh tests/test_zxfer_replication.sh
  tests/run_shunit_tests.sh --suite tests/test_zxfer_replication.sh --test test_name
  tests/run_shunit_tests.sh --list-tests tests/test_zxfer_replication.sh

Options:
  --jobs count   bound concurrent suite workers (default: CPU count, at most 4)
  --suite-timeout seconds
                 stop and fail a suite that runs longer (default 900,
                 or ZXFER_TEST_SUITE_TIMEOUT; 0 disables the watchdog)
  --skip-tool-suites
                 skip the tooling self-tests (test_run_*.sh, test_validate.sh,
                 test_ci_*.sh, test_generate_solaris_manpage.sh)
  --list-suites  list the selected suites without running them
  --list         compatibility alias for --list-suites
  --list-tests suite
                 list test names in one suite without running it
  --suite suite  select a suite and make it current for following --test options;
                 repeat to select named tests from multiple suites
  --test name    run a named shunit2 test in the current --suite; repeat until
                 the next --suite (or use before one positional suite)
  -h, --help     show this help

Every named test is validated before any suite starts. Repeated --suite values
are merged and each suite executes once in first-selection order. Options must
precede positional suite paths; use -- when a suite path begins with a dash.

Set ZXFER_TEST_SHELL to an alternate shell executable to run each suite through
that interpreter. For multi-word shell modes such as "bash --posix", point
ZXFER_TEST_SHELL at a wrapper script that execs the desired command.
EOF
}

valid_test_name_p() {
	case "${1:-}" in
	'' | [!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_]* | \
		*[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_]*)
		return 1
		;;
	esac
	return 0
}

append_positional_test_name() {
	l_test_name=$1
	if ! valid_test_name_p "$l_test_name"; then
		echo "--test requires a shell function name: $l_test_name" >&2
		return 1
	fi

	if [ -n "$RUNNER_POSITIONAL_TEST_NAMES" ]; then
		RUNNER_POSITIONAL_TEST_NAMES="$RUNNER_POSITIONAL_TEST_NAMES
$l_test_name"
	else
		RUNNER_POSITIONAL_TEST_NAMES=$l_test_name
	fi
	RUNNER_HAS_NAMED_TESTS=1
}

suite_selection_present_p() {
	l_selection_suite=$1
	while IFS= read -r l_selection_existing_suite; do
		[ -n "$l_selection_existing_suite" ] || continue
		[ "$l_selection_existing_suite" = "$l_selection_suite" ] && return 0
	done <<EOF
$RUNNER_SELECTED_SUITES
EOF
	return 1
}

append_suite_selection() {
	l_selection_input=$1
	l_selection_suite=$(resolve_suite_path "$l_selection_input")
	case "$l_selection_suite" in
	*"$TAB"* | *'
'*)
		echo "Suite paths may not contain tabs or newlines: $l_selection_input" >&2
		return 1
		;;
	esac

	if ! suite_selection_present_p "$l_selection_suite"; then
		if [ -n "$RUNNER_SELECTED_SUITES" ]; then
			RUNNER_SELECTED_SUITES="$RUNNER_SELECTED_SUITES
$l_selection_suite"
		else
			RUNNER_SELECTED_SUITES=$l_selection_suite
		fi
	fi
	RUNNER_CURRENT_SUITE_OPTION=$l_selection_suite
}

suite_test_selection_present_p() {
	l_selection_suite=$1
	l_selection_test=$2
	while IFS="$TAB" read -r l_selection_existing_suite l_selection_existing_test; do
		[ -n "$l_selection_existing_suite" ] || continue
		if [ "$l_selection_existing_suite" = "$l_selection_suite" ] &&
			[ "$l_selection_existing_test" = "$l_selection_test" ]; then
			return 0
		fi
	done <<EOF
$RUNNER_NAMED_TEST_SELECTIONS
EOF
	return 1
}

append_suite_test_selection() {
	l_selection_suite=$1
	l_selection_test=$2
	if ! valid_test_name_p "$l_selection_test"; then
		echo "--test requires a shell function name: $l_selection_test" >&2
		return 1
	fi

	if ! suite_test_selection_present_p "$l_selection_suite" "$l_selection_test"; then
		l_selection_record=$(printf '%s\t%s' "$l_selection_suite" "$l_selection_test")
		if [ -n "$RUNNER_NAMED_TEST_SELECTIONS" ]; then
			RUNNER_NAMED_TEST_SELECTIONS="$RUNNER_NAMED_TEST_SELECTIONS
$l_selection_record"
		else
			RUNNER_NAMED_TEST_SELECTIONS=$l_selection_record
		fi
	fi
	RUNNER_HAS_NAMED_TESTS=1
}

selected_test_names_for_suite() {
	l_selection_suite=$1
	while IFS="$TAB" read -r l_selection_existing_suite l_selection_existing_test; do
		[ -n "$l_selection_existing_suite" ] || continue
		if [ "$l_selection_existing_suite" = "$l_selection_suite" ]; then
			printf '%s\n' "$l_selection_existing_test"
		fi
	done <<EOF
$RUNNER_NAMED_TEST_SELECTIONS
EOF
}

positive_integer_p() {
	case "${1:-}" in
	'' | *[!0123456789]* | 0)
		return 1
		;;
	esac

	return 0
}

suite_count_label() {
	case "${1:-}" in
	1)
		printf '%s\n' "suite"
		;;
	*)
		printf '%s\n' "suites"
		;;
	esac
}

suite_count_availability_clause() {
	l_count=${1:-0}
	printf '%s runnable %s ' "$l_count" "$(suite_count_label "$l_count")"
	case "$l_count" in
	1)
		printf '%s\n' "is available"
		;;
	*)
		printf '%s\n' "are available"
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

resolve_test_shell_runner() {
	l_test_shell=${ZXFER_TEST_SHELL:-}

	if [ -z "$l_test_shell" ]; then
		TEST_SHELL_RUNNER=""
		TEST_SHELL_LABEL=""
		return 0
	fi

	case "$l_test_shell" in
	*/*)
		l_runner=$l_test_shell
		;;
	*)
		l_runner=$(command -v "$l_test_shell" 2>/dev/null || true)
		;;
	esac

	if [ -z "${l_runner:-}" ] || [ ! -x "$l_runner" ]; then
		echo "ZXFER_TEST_SHELL is not executable: $l_test_shell" >&2
		return 1
	fi

	TEST_SHELL_RUNNER=$l_runner
	TEST_SHELL_LABEL=$l_test_shell
	return 0
}

# Purpose: Succeed for a suite that tests the repository tooling rather than
# zxfer: the runners, validate.sh, the CI workflow contracts and the man-page
# generator. --skip-tool-suites leaves them to one Linux and one macOS lane.
# Usage: tool_suite_p SUITE_PATH
tool_suite_p() {
	case "${1##*/}" in
	test_run_*.sh | test_validate.sh | test_ci_*.sh | test_generate_solaris_manpage.sh)
		return 0
		;;
	esac
	return 1
}

count_runnable_suites() {
	l_count=0

	for l_suite in "$@"; do
		l_suite_path=$(resolve_suite_path "$l_suite")

		[ -f "$l_suite_path" ] || continue

		case "$(basename "$l_suite_path")" in
		test_helper.sh)
			continue
			;;
		esac

		l_count=$((l_count + 1))
	done

	printf '%s\n' "$l_count"
}

display_suite_path() {
	l_suite_path=$1
	case "$l_suite_path" in
	"$ZXFER_ROOT"/*)
		printf '%s\n' "${l_suite_path#"$ZXFER_ROOT"/}"
		;;
	*)
		printf '%s\n' "$l_suite_path"
		;;
	esac
}

list_selected_suites() {
	l_list_status=0

	for l_suite in "$@"; do
		l_suite_path=$(resolve_suite_path "$l_suite")
		if [ ! -f "$l_suite_path" ]; then
			echo "Missing suite: $l_suite_path" >&2
			l_list_status=1
			continue
		fi
		case "$(basename "$l_suite_path")" in
		test_helper.sh)
			continue
			;;
		esac
		display_suite_path "$l_suite_path"
	done

	return "$l_list_status"
}

# Print the shunit test definitions from one suite or sourced behavior fragment.
list_test_names_in_definition_file() {
	l_definition_file=$1
	l_definition_suite=$2

	awk -v suite="$l_definition_suite" '
		/^test[A-Za-z0-9_]*\(\)[[:space:]]*\{/ {
			name = $0
			sub(/\(.*/, "", name)
			printf "%s\t%s\n", suite, name
		}
	' "$l_definition_file"
}

list_selected_test_names() {
	l_list_status=0

	for l_suite in "$@"; do
		l_suite_path=$(resolve_suite_path "$l_suite")
		if [ ! -f "$l_suite_path" ]; then
			echo "Missing suite: $l_suite_path" >&2
			l_list_status=1
			continue
		fi
		case "$(basename "$l_suite_path")" in
		test_helper.sh)
			continue
			;;
		esac
		l_display_path=$(display_suite_path "$l_suite_path")
		list_test_names_in_definition_file "$l_suite_path" "$l_display_path"
		l_suite_dir=$(dirname "$l_suite_path")
		l_fragment_paths=$(awk '
			/^# zxfer-test-fragment: / {
				fragment = $0
				sub(/^# zxfer-test-fragment: /, "", fragment)
				print fragment
			}
		' "$l_suite_path")
		for l_fragment_path in $l_fragment_paths; do
			case "$l_fragment_path" in
			'' | /* | ../* | */../* | */.. | \
				*[!ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_./-]*)
				echo "Invalid suite test fragment path: $l_fragment_path" >&2
				l_list_status=1
				continue
				;;
			*) ;;
			esac
			l_fragment_file=$l_suite_dir/$l_fragment_path
			if [ ! -f "$l_fragment_file" ]; then
				echo "Missing suite test fragment: $l_fragment_file" >&2
				l_list_status=1
				continue
			fi
			list_test_names_in_definition_file "$l_fragment_file" "$l_display_path"
		done
	done

	return "$l_list_status"
}

test_name_list_contains() {
	l_available_test_rows=$1
	l_requested_test_name=$2
	while IFS="$TAB" read -r _l_available_suite l_available_test_name; do
		[ "$l_available_test_name" = "$l_requested_test_name" ] && return 0
	done <<EOF
$l_available_test_rows
EOF
	return 1
}

bind_positional_tests_to_suite() {
	l_positional_suite=$(resolve_suite_path "$1")
	while IFS= read -r l_positional_test_name; do
		[ -n "$l_positional_test_name" ] || continue
		append_suite_test_selection \
			"$l_positional_suite" "$l_positional_test_name" || return 1
	done <<EOF
$RUNNER_POSITIONAL_TEST_NAMES
EOF
}

validate_named_test_selections() {
	[ "$RUNNER_HAS_NAMED_TESTS" -eq 1 ] || return 0
	l_validation_status=0

	for l_validation_suite in "$@"; do
		l_validation_suite_path=$(resolve_suite_path "$l_validation_suite")
		l_validation_test_names=$(selected_test_names_for_suite \
			"$l_validation_suite_path")
		[ -n "$l_validation_test_names" ] || continue

		if [ ! -f "$l_validation_suite_path" ]; then
			echo "Missing suite for named-test selection: $l_validation_suite_path" >&2
			l_validation_status=1
			continue
		fi
		case "$(basename "$l_validation_suite_path")" in
		test_helper.sh)
			echo "Helper libraries cannot be selected for named tests: $l_validation_suite_path" >&2
			l_validation_status=1
			continue
			;;
		esac

		if l_validation_available_tests=$(list_selected_test_names \
			"$l_validation_suite_path"); then
			:
		else
			l_validation_status=1
			continue
		fi
		while IFS= read -r l_validation_test_name; do
			[ -n "$l_validation_test_name" ] || continue
			if ! test_name_list_contains \
				"$l_validation_available_tests" "$l_validation_test_name"; then
				l_validation_display_suite=$(display_suite_path \
					"$l_validation_suite_path")
				echo "Unknown test for $l_validation_display_suite: $l_validation_test_name" >&2
				l_validation_status=1
			fi
		done <<EOF
$l_validation_test_names
EOF
	done

	return "$l_validation_status"
}

detect_default_parallel_jobs() {
	l_runnable_count=$1
	l_detected_jobs=""

	if l_candidate=$(getconf _NPROCESSORS_ONLN 2>/dev/null); then
		if positive_integer_p "$l_candidate"; then
			l_detected_jobs=$l_candidate
		fi
	fi

	if [ -z "$l_detected_jobs" ] &&
		l_candidate=$(sysctl -n hw.ncpu 2>/dev/null); then
		if positive_integer_p "$l_candidate"; then
			l_detected_jobs=$l_candidate
		fi
	fi

	if [ -z "$l_detected_jobs" ]; then
		l_detected_jobs=1
	fi

	if [ "$l_detected_jobs" -gt 4 ]; then
		l_detected_jobs=4
	fi

	if [ "$l_runnable_count" -gt 0 ] &&
		[ "$l_detected_jobs" -gt "$l_runnable_count" ]; then
		l_detected_jobs=$l_runnable_count
	fi

	printf '%s\n' "$l_detected_jobs"
}

resolve_parallel_jobs() {
	l_runnable_count=$1

	if [ -n "$RUNNER_REQUESTED_JOBS" ]; then
		if ! positive_integer_p "$RUNNER_REQUESTED_JOBS"; then
			echo "--jobs must be a positive integer" >&2
			return 1
		fi
		RUNNER_PARALLEL_JOBS=$RUNNER_REQUESTED_JOBS
		if [ "$l_runnable_count" -gt 0 ] &&
			[ "$RUNNER_PARALLEL_JOBS" -gt "$l_runnable_count" ]; then
			echo "==> Requested $RUNNER_PARALLEL_JOBS shunit2 jobs, but only $(suite_count_availability_clause "$l_runnable_count"); limiting to $l_runnable_count."
			RUNNER_PARALLEL_JOBS=$l_runnable_count
		fi
		return 0
	fi

	RUNNER_PARALLEL_JOBS=$(detect_default_parallel_jobs "$l_runnable_count")
	return 0
}

emit_suite_banner() {
	l_suite_path=$1

	if [ -n "${TEST_SHELL_LABEL:-}" ]; then
		echo "==> Running shunit2 suite with test shell [$TEST_SHELL_LABEL]: $l_suite_path"
	else
		echo "==> Running shunit2 suite: $l_suite_path"
	fi
}

# Purpose: Create the private state directory and the event FIFO, open the
# FIFO read-write on fd 3 (so reading never sees EOF between writers), and
# start the ticker.
# Usage: runner_start; returns 1 when the state cannot be created.
runner_start() {
	l_start_parent=${TMPDIR:-/tmp}
	case "$l_start_parent" in
	/*) l_start_template="$l_start_parent/zxfer_shunit.XXXXXX" ;;
	*) l_start_template="./$l_start_parent/zxfer_shunit.XXXXXX" ;;
	esac

	# Pass the parent explicitly. Some execution wrappers preserve the TMPDIR
	# value in the environment while the platform mktemp -t implementation
	# still falls back to its default temporary directory.
	RUNNER_STATE_DIR=$(mktemp -d "$l_start_template") || {
		echo "Unable to create shunit2 runner state directory." >&2
		return 1
	}
	mkfifo "$RUNNER_STATE_DIR/events" || {
		echo "Unable to create the shunit2 runner event FIFO." >&2
		return 1
	}
	exec 3<>"$RUNNER_STATE_DIR/events"

	# The ticker exits once the runner is gone, so a KILLed runner leaves no
	# ticker behind. Its output goes to /dev/null so a leftover sleep never
	# holds the caller's pipe open.
	l_start_runner_pid=$$
	(
		while kill -s 0 "$l_start_runner_pid" 2>/dev/null; do
			sleep 1 || exit 0
			printf 'tick\n' >&3 || exit 0
		done
	) </dev/null >/dev/null 2>&1 &
	RUNNER_TICKER_PID=$!
}

# Purpose: Stop the ticker, close the FIFO and remove the state directory.
# Usage: runner_stop; safe to call more than once (the EXIT trap calls it).
runner_stop() {
	if [ -n "$RUNNER_TICKER_PID" ]; then
		kill -s TERM "$RUNNER_TICKER_PID" 2>/dev/null || :
		wait "$RUNNER_TICKER_PID" 2>/dev/null || :
		RUNNER_TICKER_PID=
	fi
	if [ -n "$RUNNER_STATE_DIR" ]; then
		exec 3>&-
		rm -rf "$RUNNER_STATE_DIR"
		RUNNER_STATE_DIR=
	fi
}

# Purpose: Run one suite command and report its status on the event FIFO.
# Usage: runner_worker ID LOG_FILE COMMAND...; runs in the background. An
# empty LOG_FILE streams the suite to the runner's own output. The suite does
# not inherit fd 3. The worker ignores HUP, INT and TERM once the suite runs,
# so a stopped suite is still reported; only KILL ends the worker early.
runner_worker() {
	set +e
	l_worker_id=$1
	l_worker_log=$2
	shift 2
	if [ -n "$l_worker_log" ]; then
		"$@" >"$l_worker_log" 2>&1 3>&- &
	else
		"$@" 3>&- &
	fi
	l_worker_suite_pid=$!
	trap '' HUP INT TERM
	printf '%s\n' "$l_worker_suite_pid" >"$RUNNER_STATE_DIR/$l_worker_id.pid"
	# 2>/dev/null drops the shell's "Terminated" notice for a stopped suite.
	wait "$l_worker_suite_pid" 2>/dev/null
	printf 'done %s %s\n' "$l_worker_id" "$?" >&3
}

# Purpose: Print the IDs of the given suites (their positions, 1 to N) in the
# order to start them: with more than one job, the RUNNER_SLOW_SUITES entries
# first, in that list's order, then the rest in suite order.
# Usage: runner_launch_order SUITE...
runner_launch_order() {
	l_order_first=" "
	if [ "$RUNNER_PARALLEL_JOBS" -gt 1 ]; then
		for l_order_slow in $RUNNER_SLOW_SUITES; do
			l_order_id=0
			for l_order_suite in "$@"; do
				l_order_id=$((l_order_id + 1))
				if [ "${l_order_suite##*/}" = "$l_order_slow" ]; then
					case "$l_order_first" in
					*" $l_order_id "*) ;;
					*) l_order_first="$l_order_first$l_order_id " ;;
					esac
				fi
			done
		done
	fi
	l_order_id=0
	for l_order_suite in "$@"; do
		l_order_id=$((l_order_id + 1))
		case "$l_order_first" in
		*" $l_order_id "*) ;;
		*) l_order_first="$l_order_first$l_order_id " ;;
		esac
	done
	printf '%s\n' "$l_order_first"
}

# Purpose: Start one suite, or record a missing suite or the helper library
# for in-order replay.
# Usage: runner_launch_suite ID SUITE_PATH TEST_NAMES (one name per line).
runner_launch_suite() {
	l_launch_id=$1
	l_launch_path=$2
	l_launch_tests=$3
	printf '%s\n' "$l_launch_path" >"$RUNNER_STATE_DIR/$l_launch_id.suite"

	if [ ! -f "$l_launch_path" ]; then
		printf '%s\n' missing >"$RUNNER_STATE_DIR/$l_launch_id.status"
		return 0
	fi
	case "${l_launch_path##*/}" in
	test_helper.sh)
		printf '%s\n' helper >"$RUNNER_STATE_DIR/$l_launch_id.status"
		return 0
		;;
	esac

	set -- "$l_launch_path"
	[ -z "$l_launch_tests" ] || set -- "$@" --
	while IFS= read -r l_launch_test; do
		[ -z "$l_launch_test" ] || set -- "$@" "$l_launch_test"
	done <<EOF
$l_launch_tests
EOF
	[ -z "$TEST_SHELL_RUNNER" ] || set -- "$TEST_SHELL_RUNNER" "$@"

	l_launch_log=
	if [ "$RUNNER_PARALLEL_JOBS" -eq 1 ]; then
		emit_suite_banner "$l_launch_path"
	else
		l_launch_log=$RUNNER_STATE_DIR/$l_launch_id.log
	fi
	runner_worker "$l_launch_id" "$l_launch_log" "$@" &
	RUNNER_WORKERS="${RUNNER_WORKERS:+$RUNNER_WORKERS }$l_launch_id:$!:$RUNNER_TICKS:run:0"
	RUNNER_INFLIGHT=$((RUNNER_INFLIGHT + 1))
}

# Purpose: Split one RUNNER_WORKERS word into RUNNER_W_ID, RUNNER_W_PID,
# RUNNER_W_START, RUNNER_W_PHASE and RUNNER_W_PHASE_TICK.
# Usage: runner_parse_worker WORD
runner_parse_worker() {
	l_parse_rest=$1
	RUNNER_W_ID=${l_parse_rest%%:*}
	l_parse_rest=${l_parse_rest#*:}
	RUNNER_W_PID=${l_parse_rest%%:*}
	l_parse_rest=${l_parse_rest#*:}
	RUNNER_W_START=${l_parse_rest%%:*}
	l_parse_rest=${l_parse_rest#*:}
	RUNNER_W_PHASE=${l_parse_rest%%:*}
	RUNNER_W_PHASE_TICK=${l_parse_rest#*:}
}

# Purpose: Print "PID<TAB>COMMAND" for every descendant of ROOT, parents
# first, from one process-table snapshot. Prints nothing when ps fails.
# Usage: runner_process_tree ROOT_PID
runner_process_tree() {
	ps -A -o pid= -o ppid= -o args= 2>/dev/null | awk -v root="$1" '
		$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ {
			pid = $1
			kids[$2] = kids[$2] " " pid
			sub(/^[ \t]*[0-9]+[ \t]+[0-9]+[ \t]*/, "")
			command[pid] = $0
		}
		END {
			n = split(kids[root], queue, " ")
			for (i = 1; i <= n; i++) {
				pid = queue[i]
				if (pid in seen) continue
				seen[pid] = 1
				printf "%s\t%s\n", pid, command[pid]
				m = split(kids[pid], more, " ")
				for (j = 1; j <= m; j++) queue[++n] = more[j]
			}
		}
	'
}

# Purpose: Send SIGNAL to one worker's suite and to every descendant of the
# worker, best effort. The worker itself is spared so it can report.
# Usage: runner_signal_worker SIGNAL ID WORKER_PID
runner_signal_worker() {
	l_signal_pids=$(runner_process_tree "$3" | awk -F "$TAB" '{ print $1 }')
	l_signal_suite_pid=
	if [ -r "$RUNNER_STATE_DIR/$2.pid" ]; then
		IFS= read -r l_signal_suite_pid <"$RUNNER_STATE_DIR/$2.pid" || :
	fi
	case "$l_signal_suite_pid" in
	'' | *[!0123456789]*) l_signal_suite_pid= ;;
	esac
	[ -n "$l_signal_pids$l_signal_suite_pid" ] || return 0
	# shellcheck disable=SC2086  # One PID per word.
	kill -s "$1" $l_signal_suite_pid $l_signal_pids 2>/dev/null || :
}

# Purpose: Print the stalled suite and its processes to stderr.
# Usage: runner_report_stall ID WORKER_PID
runner_report_stall() {
	l_stall_path=
	IFS= read -r l_stall_path <"$RUNNER_STATE_DIR/$1.suite" || :
	printf '!! Suite still running after %ss, stopping it: %s\n' \
		"$RUNNER_SUITE_TIMEOUT" "$l_stall_path" >&2
	runner_process_tree "$2" |
		awk -F "$TAB" '{ printf "!!   pid %s: %s\n", $1, $2 }' >&2
}

# Purpose: Record one worker's status for replay and forget the worker. A
# worker the watchdog stopped is recorded as a timeout.
# Usage: runner_finish_worker ID STATUS; an unknown ID is ignored.
runner_finish_worker() {
	l_finish_id=$1
	l_finish_status=$2
	l_finish_kept=
	l_finish_worker=
	for l_finish_word in $RUNNER_WORKERS; do
		if [ "${l_finish_word%%:*}" = "$l_finish_id" ]; then
			l_finish_worker=$l_finish_word
		else
			l_finish_kept="${l_finish_kept:+$l_finish_kept }$l_finish_word"
		fi
	done
	[ -n "$l_finish_worker" ] || return 0
	runner_parse_worker "$l_finish_worker"
	RUNNER_WORKERS=$l_finish_kept
	RUNNER_INFLIGHT=$((RUNNER_INFLIGHT - 1))
	wait "$RUNNER_W_PID" 2>/dev/null || :
	case "$l_finish_status" in
	'' | *[!0123456789]*) l_finish_status=1 ;;
	esac
	[ "$RUNNER_W_PHASE" = run ] || l_finish_status=timeout
	printf '%s\n' "$l_finish_status" >"$RUNNER_STATE_DIR/$l_finish_id.status"
}

# Purpose: Count one tick and advance the watchdog: TERM a suite that ran
# past the timeout, KILL it after the grace ticks, and give up on a worker
# that still has not reported after another grace period.
# Usage: runner_tick
runner_tick() {
	RUNNER_TICKS=$((RUNNER_TICKS + 1))
	[ "$RUNNER_STOPPING" -eq 0 ] || return 0
	[ "$RUNNER_SUITE_TIMEOUT" -gt 0 ] || return 0
	l_tick_workers=
	l_tick_actions=
	for l_tick_word in $RUNNER_WORKERS; do
		runner_parse_worker "$l_tick_word"
		l_tick_action=
		case "$RUNNER_W_PHASE" in
		run)
			[ $((RUNNER_TICKS - RUNNER_W_START)) -lt "$RUNNER_SUITE_TIMEOUT" ] ||
				l_tick_action="term"
			;;
		term)
			[ $((RUNNER_TICKS - RUNNER_W_PHASE_TICK)) -lt "$RUNNER_GRACE_TICKS" ] ||
				l_tick_action="kill"
			;;
		kill)
			[ $((RUNNER_TICKS - RUNNER_W_PHASE_TICK)) -lt "$RUNNER_GRACE_TICKS" ] ||
				l_tick_action="reap"
			;;
		esac
		if [ -n "$l_tick_action" ]; then
			RUNNER_W_PHASE=$l_tick_action
			RUNNER_W_PHASE_TICK=$RUNNER_TICKS
			l_tick_actions="$l_tick_actions $RUNNER_W_ID:$RUNNER_W_PID:$l_tick_action"
		fi
		l_tick_workers="${l_tick_workers:+$l_tick_workers }$RUNNER_W_ID:$RUNNER_W_PID:$RUNNER_W_START:$RUNNER_W_PHASE:$RUNNER_W_PHASE_TICK"
	done
	RUNNER_WORKERS=$l_tick_workers

	for l_tick_action in $l_tick_actions; do
		l_tick_id=${l_tick_action%%:*}
		l_tick_action=${l_tick_action#*:}
		l_tick_pid=${l_tick_action%%:*}
		case "${l_tick_action#*:}" in
		term)
			runner_report_stall "$l_tick_id" "$l_tick_pid"
			runner_signal_worker TERM "$l_tick_id" "$l_tick_pid"
			;;
		kill)
			runner_signal_worker KILL "$l_tick_id" "$l_tick_pid"
			;;
		reap)
			kill -s KILL "$l_tick_pid" 2>/dev/null || :
			runner_finish_worker "$l_tick_id" 137
			;;
		esac
	done
}

# Purpose: Wait for and handle the next FIFO event.
# Usage: runner_wait_event; returns 0 also when a signal interrupts the read.
runner_wait_event() {
	l_event_kind=
	l_event_id=
	l_event_status=
	IFS=' ' read -r l_event_kind l_event_id l_event_status <&3 || return 0
	case "$l_event_kind" in
	done)
		runner_finish_worker "$l_event_id" "$l_event_status"
		;;
	tick)
		runner_tick
		;;
	esac
}

# Purpose: Count one finished suite and print its failure line.
# Usage: runner_record_result SUITE_PATH STATUS
runner_record_result() {
	case "$2" in
	0)
		passed_count=$((passed_count + 1))
		;;
	timeout)
		echo "!! Suite failed: $1 (timed out after ${RUNNER_SUITE_TIMEOUT}s)" >&2
		overall_status=124
		failed_count=$((failed_count + 1))
		;;
	*)
		echo "!! Suite failed: $1 (exit status $2)" >&2
		overall_status=$2
		failed_count=$((failed_count + 1))
		;;
	esac
}

# Purpose: Replay every finished suite whose predecessors have all been
# replayed, in suite order.
# Usage: runner_replay_ready
runner_replay_ready() {
	while [ "$RUNNER_NEXT_REPLAY" -le "$RUNNER_TOTAL" ] &&
		[ -f "$RUNNER_STATE_DIR/$RUNNER_NEXT_REPLAY.status" ]; do
		l_replay_id=$RUNNER_NEXT_REPLAY
		l_replay_status=1
		l_replay_path=
		IFS= read -r l_replay_status <"$RUNNER_STATE_DIR/$l_replay_id.status" || :
		IFS= read -r l_replay_path <"$RUNNER_STATE_DIR/$l_replay_id.suite" || :
		case "$l_replay_status" in
		missing)
			echo "Skipping missing suite: $l_replay_path" >&2
			overall_status=1
			failed_count=$((failed_count + 1))
			;;
		helper)
			echo "==> Skipping helper library: $l_replay_path"
			;;
		*)
			if [ "$RUNNER_PARALLEL_JOBS" -gt 1 ]; then
				emit_suite_banner "$l_replay_path"
				[ ! -r "$RUNNER_STATE_DIR/$l_replay_id.log" ] ||
					cat "$RUNNER_STATE_DIR/$l_replay_id.log"
			fi
			runner_record_result "$l_replay_path" "$l_replay_status"
			;;
		esac
		rm -f "$RUNNER_STATE_DIR/$l_replay_id".*
		RUNNER_NEXT_REPLAY=$((l_replay_id + 1))
	done
}

# Purpose: Stop every running suite: TERM, a grace period, KILL, another
# grace period, then KILL the workers that still have not reported.
# Usage: runner_stop_workers
runner_stop_workers() {
	RUNNER_STOPPING=1
	for l_stop_signal in TERM KILL; do
		[ -n "$RUNNER_WORKERS" ] || return 0
		for l_stop_word in $RUNNER_WORKERS; do
			runner_parse_worker "$l_stop_word"
			runner_signal_worker "$l_stop_signal" "$RUNNER_W_ID" "$RUNNER_W_PID"
		done
		l_stop_deadline=$((RUNNER_TICKS + RUNNER_GRACE_TICKS))
		while [ -n "$RUNNER_WORKERS" ] &&
			[ "$RUNNER_TICKS" -lt "$l_stop_deadline" ]; do
			runner_wait_event
		done
	done
	for l_stop_word in $RUNNER_WORKERS; do
		runner_parse_worker "$l_stop_word"
		kill -s KILL "$RUNNER_W_PID" 2>/dev/null || :
		wait "$RUNNER_W_PID" 2>/dev/null || :
	done
	RUNNER_WORKERS=
}

# Purpose: Remember the first HUP, INT or TERM; the main loop acts on it after
# the current event, outside the trap.
# Usage: installed by the trap command only.
# shellcheck disable=SC2329  # Invoked by the HUP, INT and TERM traps.
runner_note_signal() {
	[ -n "$RUNNER_PENDING_SIGNAL" ] || RUNNER_PENDING_SIGNAL=$1
}

# Purpose: Stop the suites and exit 128+N once a signal was noted.
# Usage: runner_check_signal; returns when no signal is pending.
runner_check_signal() {
	[ -n "$RUNNER_PENDING_SIGNAL" ] || return 0
	# Once teardown starts, another catchable signal must not cut it short.
	trap '' HUP INT TERM
	case "$RUNNER_PENDING_SIGNAL" in
	HUP) l_check_exit=129 ;;
	INT) l_check_exit=130 ;;
	*) l_check_exit=143 ;;
	esac
	echo "!! shunit2 runner got SIG$RUNNER_PENDING_SIGNAL; stopping $RUNNER_INFLIGHT running $(suite_count_label "$RUNNER_INFLIGHT")" >&2
	runner_stop_workers
	runner_stop
	exit "$l_check_exit"
}

while [ "$#" -gt 0 ]; do
	case "$1" in
	--jobs)
		shift
		[ "$#" -gt 0 ] || {
			echo "--jobs requires a value" >&2
			exit 1
		}
		RUNNER_REQUESTED_JOBS=$1
		;;
	--suite-timeout)
		shift
		[ "$#" -gt 0 ] || {
			echo "--suite-timeout requires a value" >&2
			exit 1
		}
		RUNNER_SUITE_TIMEOUT=$1
		;;
	--skip-tool-suites)
		RUNNER_SKIP_TOOL_SUITES=1
		;;
	--)
		shift
		break
		;;
	-h | --help)
		print_usage
		exit 0
		;;
	--list | --list-suites)
		RUNNER_LIST_MODE=suites
		;;
	--list-tests)
		RUNNER_LIST_MODE=tests
		;;
	--test)
		shift
		[ "$#" -gt 0 ] || {
			echo "--test requires a value" >&2
			exit 1
		}
		if [ -n "$RUNNER_CURRENT_SUITE_OPTION" ]; then
			append_suite_test_selection \
				"$RUNNER_CURRENT_SUITE_OPTION" "$1" || exit 1
		else
			append_positional_test_name "$1" || exit 1
		fi
		;;
	--suite)
		shift
		[ "$#" -gt 0 ] || {
			echo "--suite requires a value" >&2
			exit 1
		}
		append_suite_selection "$1" || exit 1
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

case "$RUNNER_SUITE_TIMEOUT" in
'' | *[!0123456789]*)
	echo "--suite-timeout (or ZXFER_TEST_SUITE_TIMEOUT) must be a whole number of seconds" >&2
	exit 1
	;;
esac

if [ -n "$RUNNER_SELECTED_SUITES" ]; then
	[ "$#" -eq 0 ] || {
		echo "--suite cannot be combined with positional suite paths." >&2
		exit 1
	}
	[ -z "$RUNNER_POSITIONAL_TEST_NAMES" ] || {
		echo "--test must follow the --suite it selects." >&2
		exit 1
	}
	set --
	while IFS= read -r l_selected_suite; do
		[ -n "$l_selected_suite" ] || continue
		set -- "$@" "$l_selected_suite"
	done <<EOF
$RUNNER_SELECTED_SUITES
EOF
elif [ -n "$RUNNER_POSITIONAL_TEST_NAMES" ]; then
	if [ "$#" -eq 0 ]; then
		echo "--test requires a positional suite or a preceding --suite." >&2
		exit 1
	fi
	l_positional_runnable_count=$(count_runnable_suites "$@")
	if [ "$#" -ne 1 ] || [ "$l_positional_runnable_count" -ne 1 ]; then
		echo "--test requires exactly one runnable suite; found $l_positional_runnable_count." >&2
		exit 1
	fi
	bind_positional_tests_to_suite "$1" || exit 1
fi

if [ "$RUNNER_LIST_MODE" = tests ] && [ "$#" -ne 1 ]; then
	echo "--list-tests requires exactly one explicit suite; found $#." >&2
	exit 1
fi

if [ "$#" -eq 0 ]; then
	set -- "$TEST_DIR"/test_*.sh
	if [ "$#" -eq 1 ] &&
		[ "$1" = "$TEST_DIR/test_*.sh" ] &&
		[ ! -e "$1" ]; then
		echo "No shunit2 suites found in $TEST_DIR" >&2
		exit 1
	fi
fi

if [ "$RUNNER_SKIP_TOOL_SUITES" -eq 1 ]; then
	l_kept_suites=
	l_skipped_count=0
	for l_suite in "$@"; do
		if tool_suite_p "$l_suite"; then
			l_skipped_count=$((l_skipped_count + 1))
		else
			l_kept_suites="${l_kept_suites}${l_suite}
"
		fi
	done
	set --
	while IFS= read -r l_suite; do
		[ -z "$l_suite" ] || set -- "$@" "$l_suite"
	done <<EOF
$l_kept_suites
EOF
	[ -n "$RUNNER_LIST_MODE" ] ||
		echo "==> Skipping $l_skipped_count tool $(suite_count_label "$l_skipped_count") (--skip-tool-suites)"
fi

if [ "$#" -eq 0 ]; then
	echo "No shunit2 suites found in $TEST_DIR" >&2
	exit 1
fi

if [ -n "$RUNNER_LIST_MODE" ] && [ "$RUNNER_HAS_NAMED_TESTS" -eq 1 ]; then
	echo "--list/--list-tests cannot be combined with --test." >&2
	exit 1
fi

case "$RUNNER_LIST_MODE" in
suites)
	list_selected_suites "$@"
	exit $?
	;;
tests)
	list_selected_test_names "$@"
	exit $?
	;;
esac

resolve_test_shell_runner || exit 1
validate_named_test_selections "$@" || exit 1
resolve_parallel_jobs "$(count_runnable_suites "$@")" || exit 1

trap 'runner_note_signal HUP' HUP
trap 'runner_note_signal INT' INT
trap 'runner_note_signal TERM' TERM
trap 'runner_stop' EXIT
runner_start || exit 1

RUNNER_TOTAL=$#
for suite_id in $(runner_launch_order "$@"); do
	suite_path=$(
		shift "$((suite_id - 1))"
		resolve_suite_path "$1"
	)
	suite_test_names=$(selected_test_names_for_suite "$suite_path")
	while [ "$RUNNER_INFLIGHT" -ge "$RUNNER_PARALLEL_JOBS" ]; do
		runner_wait_event
		runner_check_signal
		runner_replay_ready
	done
	runner_launch_suite "$suite_id" "$suite_path" "$suite_test_names"
	runner_check_signal
	runner_replay_ready
done
while [ "$RUNNER_INFLIGHT" -gt 0 ]; do
	runner_wait_event
	runner_check_signal
	runner_replay_ready
done
runner_replay_ready

trap - HUP INT TERM
runner_stop

echo "==> shunit2 summary: ${passed_count} passed, ${failed_count} failed"

exit "$overall_status"
