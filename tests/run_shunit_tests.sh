#!/bin/sh
#
# Run all zxfer shunit2 suites (or a user-specified subset) with bounded
# parallelism.
#

set -eu

ZXFER_ROOT=$(cd "$(dirname "$0")/.." && pwd)
TEST_DIR="$ZXFER_ROOT/tests"

RUNNER_REQUESTED_JOBS=""
RUNNER_PARALLEL_JOBS=1
RUNNER_STATE_DIR=""
RUNNER_PENDING_WORKERS=""
RUNNER_INFLIGHT_COUNT=0
RUNNER_NEXT_WORKER_ID=1
RUNNER_DEFER_SIGNALS=0
RUNNER_DEFERRED_SIGNAL=""
RUNNER_FOREGROUND_SUITE_PID=""
RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=""
RUNNER_SHUTTING_DOWN=0
RUNNER_SIGNAL_SHUTDOWN_GRACE_SECONDS=2

print_usage() {
	cat <<'EOF'
Usage: tests/run_shunit_tests.sh [--jobs count] [--] [suite ...]

Runs every shunit2 suite (tests/test_*.sh) when no arguments are provided.
Pass specific suite paths to limit execution, e.g.:

  tests/run_shunit_tests.sh --jobs 4
  tests/run_shunit_tests.sh test_zxfer_reporting.sh
  tests/run_shunit_tests.sh tests/test_zxfer_replication.sh

Set ZXFER_TEST_SHELL to an alternate shell executable to run each suite through
that interpreter. For multi-word shell modes such as "bash --posix", point
ZXFER_TEST_SHELL at a wrapper script that execs the desired command.
EOF
}

positive_integer_p() {
	case "${1:-}" in
	'' | *[!0-9]* | 0)
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

list_child_pids_for_parent() {
	l_parent_pid=$1
	l_ps_output=
	l_pgrep_output=

	case "$l_parent_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	if command -v pgrep >/dev/null 2>&1; then
		l_pgrep_output=$(pgrep -P "$l_parent_pid" 2>/dev/null || true)
		if [ -n "$l_pgrep_output" ]; then
			printf '%s\n' "$l_pgrep_output"
			return 0
		fi
	fi

	if l_ps_output=$(ps -eo pid= -o ppid= 2>/dev/null); then
		:
	elif l_ps_output=$(ps -ax -o pid= -o ppid= 2>/dev/null); then
		:
	elif l_ps_output=$(ps -A -o pid= -o ppid= 2>/dev/null); then
		:
	else
		return 1
	fi

	printf '%s\n' "$l_ps_output" | awk -v parent="$l_parent_pid" '
		$1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $2 == parent {
			print $1
		}
	'
}

send_signal_to_pid() {
	l_send_signal_to_pid_signal=$1
	l_send_signal_to_pid_pid=$2

	case "$l_send_signal_to_pid_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# Some illumos/Solaris shells have historically been less consistent
	# about the POSIX `kill -s NAME` form; keep `kill -NAME` as a fallback.
	kill -s "$l_send_signal_to_pid_signal" "$l_send_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	kill "-$l_send_signal_to_pid_signal" "$l_send_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	return 1
}

signal_process_descendants() {
	l_signal_process_descendants_signal=$1
	l_signal_process_descendants_pending=$2
	l_signal_process_descendants_next=
	l_signal_process_descendants_parent=
	l_signal_process_descendants_child=

	while [ -n "$l_signal_process_descendants_pending" ]; do
		l_signal_process_descendants_next=
		for l_signal_process_descendants_parent in $l_signal_process_descendants_pending; do
			for l_signal_process_descendants_child in $(list_child_pids_for_parent "$l_signal_process_descendants_parent"); do
				send_signal_to_pid "$l_signal_process_descendants_signal" "$l_signal_process_descendants_child" || true
				l_signal_process_descendants_next="${l_signal_process_descendants_next}${l_signal_process_descendants_next:+ }$l_signal_process_descendants_child"
			done
		done
		l_signal_process_descendants_pending=$l_signal_process_descendants_next
	done
}

signal_pid_and_descendants() {
	l_signal=$1
	l_pid=$2

	case "$l_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	signal_process_descendants "$l_signal" "$l_pid"
	send_signal_to_pid "$l_signal" "$l_pid" || true
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

ensure_runner_state_dir() {
	[ -n "$RUNNER_STATE_DIR" ] && return 0

	RUNNER_STATE_DIR=$(mktemp -d -t "zxfer_shunit.XXXXXX") || {
		echo "Unable to create shunit2 runner state directory." >&2
		return 1
	}
}

cleanup_runner_state() {
	if [ -n "${RUNNER_STATE_DIR:-}" ] && [ -d "$RUNNER_STATE_DIR" ]; then
		rm -rf "$RUNNER_STATE_DIR"
	fi

	RUNNER_STATE_DIR=""
	RUNNER_PENDING_WORKERS=""
	RUNNER_INFLIGHT_COUNT=0
	RUNNER_FOREGROUND_SUITE_PID=""
	RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=""
}

append_worker_id() {
	l_queue=$1
	l_worker_id=$2

	if [ -n "$l_queue" ]; then
		printf '%s %s\n' "$l_queue" "$l_worker_id"
	else
		printf '%s\n' "$l_worker_id"
	fi
}

emit_suite_banner() {
	l_suite_path=$1

	if [ -n "${TEST_SHELL_LABEL:-}" ]; then
		echo "==> Running shunit2 suite with test shell [$TEST_SHELL_LABEL]: $l_suite_path"
	else
		echo "==> Running shunit2 suite: $l_suite_path"
	fi
}

remember_deferred_runner_signal() {
	l_signal=$1

	if [ -z "${RUNNER_DEFERRED_SIGNAL:-}" ]; then
		RUNNER_DEFERRED_SIGNAL=$l_signal
	fi
}

consume_deferred_runner_signal() {
	if [ -z "${RUNNER_DEFERRED_SIGNAL:-}" ]; then
		return 0
	fi

	l_signal=$RUNNER_DEFERRED_SIGNAL
	RUNNER_DEFERRED_SIGNAL=""
	handle_runner_signal "$l_signal"
}

signal_foreground_suite() {
	l_signal=$1
	l_child_pid=""

	case "${RUNNER_FOREGROUND_SUITE_PID:-}" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	if [ -r "${RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE:-}" ]; then
		l_child_pid=$(cat "$RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE" 2>/dev/null || true)
	fi
	case "$l_child_pid" in
	'' | *[!0-9]*)
		signal_pid_and_descendants "$l_signal" "$RUNNER_FOREGROUND_SUITE_PID"
		;;
	*)
		signal_pid_and_descendants "$l_signal" "$l_child_pid"
		send_signal_to_pid "$l_signal" "$RUNNER_FOREGROUND_SUITE_PID" || true
		;;
	esac
}

foreground_suite_running_p() {
	l_child_pid=""

	case "${RUNNER_FOREGROUND_SUITE_PID:-}" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	if send_signal_to_pid 0 "$RUNNER_FOREGROUND_SUITE_PID"; then
		return 0
	fi
	if [ -r "${RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE:-}" ]; then
		l_child_pid=$(cat "$RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE" 2>/dev/null || true)
	fi
	case "$l_child_pid" in
	'' | *[!0-9]*) ;;
	*)
		if send_signal_to_pid 0 "$l_child_pid"; then
			return 0
		fi
		;;
	esac

	return 1
}

run_suite_foreground() {
	l_suite_path=$1
	l_status_file=
	l_child_pid_file=
	l_wait_status=0

	emit_suite_banner "$l_suite_path"
	if ! ensure_runner_state_dir; then
		overall_status=1
		failed_count=$((failed_count + 1))
		return 0
	fi
	l_status_file="$RUNNER_STATE_DIR/foreground.status"
	l_child_pid_file="$RUNNER_STATE_DIR/foreground.child.pid"
	rm -f "$l_status_file"
	rm -f "$l_child_pid_file"
	RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=$l_child_pid_file

	# macOS /bin/sh can defer traps while blocked in wait, so serial mode
	# polls this wrapper's status file instead of waiting directly on the suite.
	RUNNER_DEFER_SIGNALS=1
	(
		set +e
		trap - HUP INT TERM
		if [ -n "${TEST_SHELL_RUNNER:-}" ]; then
			"$TEST_SHELL_RUNNER" "$l_suite_path" &
		else
			"$l_suite_path" &
		fi
		l_suite_pid=$!
		printf '%s\n' "$l_suite_pid" >"$l_child_pid_file" 2>/dev/null || :
		wait "$l_suite_pid"
		l_status=$?
		printf '%s\n' "$l_status" >"$l_status_file" 2>/dev/null || :
		exit "$l_status"
	) &
	RUNNER_FOREGROUND_SUITE_PID=$!
	RUNNER_DEFER_SIGNALS=0
	consume_deferred_runner_signal

	while [ ! -r "$l_status_file" ]; do
		if ! foreground_suite_running_p; then
			break
		fi
		sleep 1
	done

	if wait "$RUNNER_FOREGROUND_SUITE_PID"; then
		l_wait_status=0
	else
		l_wait_status=$?
	fi
	RUNNER_FOREGROUND_SUITE_PID=""
	RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=""

	l_status=$l_wait_status
	if [ -r "$l_status_file" ]; then
		l_status=$(cat "$l_status_file" 2>/dev/null || printf '%s\n' "$l_wait_status")
	fi
	rm -f "$l_status_file" "$l_child_pid_file"

	if [ "$l_status" -eq 0 ]; then
		passed_count=$((passed_count + 1))
	else
		echo "!! Suite failed: $l_suite_path (exit status $l_status)" >&2
		overall_status=$l_status
		failed_count=$((failed_count + 1))
	fi
}

launch_suite_worker() {
	l_suite_path=$1
	l_worker_id=$RUNNER_NEXT_WORKER_ID
	l_log_file="$RUNNER_STATE_DIR/$l_worker_id.log"
	l_status_file="$RUNNER_STATE_DIR/$l_worker_id.status"
	l_path_file="$RUNNER_STATE_DIR/$l_worker_id.path"
	l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
	l_child_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
	l_ready_file="$RUNNER_STATE_DIR/$l_worker_id.ready"

	printf '%s\n' "$l_suite_path" >"$l_path_file"
	rm -f "$l_child_pid_file"
	rm -f "$l_ready_file"

	RUNNER_DEFER_SIGNALS=1
	(
		set +e
		l_suite_pid=
		l_launching_suite_child=1
		l_deferred_signal=
		runner_remember_deferred_signal() {
			l_signal=$1
			if [ -z "${l_deferred_signal:-}" ]; then
				l_deferred_signal=$l_signal
			fi
		}
		runner_signal_suite_child() {
			l_signal=$1
			case "${l_suite_pid:-}" in
			'' | *[!0-9]*)
				return 0
				;;
			esac
			signal_pid_and_descendants "$l_signal" "$l_suite_pid"
		}
		runner_suite_child_running_p() {
			case "${l_suite_pid:-}" in
			'' | *[!0-9]*)
				return 1
				;;
			esac
			if send_signal_to_pid 0 "$l_suite_pid"; then
				return 0
			fi
			return 1
		}
		runner_wait_for_suite_child_shutdown() {
			l_remaining=$RUNNER_SIGNAL_SHUTDOWN_GRACE_SECONDS
			while [ "$l_remaining" -gt 0 ]; do
				if ! runner_suite_child_running_p; then
					return 0
				fi
				sleep 1
				l_remaining=$((l_remaining - 1))
			done
			if ! runner_suite_child_running_p; then
				return 0
			fi
			return 1
		}
		runner_handle_worker_signal() {
			l_signal=$1
			if [ "${l_launching_suite_child:-0}" = "1" ]; then
				runner_remember_deferred_signal "$l_signal"
				return 0
			fi
			l_status=$(signal_exit_status "$l_signal")
			runner_signal_suite_child "$l_signal"
			case "${l_suite_pid:-}" in
			'' | *[!0-9]*)
				exit "$l_status"
				;;
			esac
			if ! runner_wait_for_suite_child_shutdown; then
				runner_signal_suite_child KILL
			fi
			wait "$l_suite_pid" >/dev/null 2>&1 || :
			exit "$l_status"
		}
		runner_consume_deferred_signal() {
			if [ -z "${l_deferred_signal:-}" ]; then
				return 0
			fi
			l_signal=$l_deferred_signal
			l_deferred_signal=
			runner_handle_worker_signal "$l_signal"
		}
		trap 'runner_handle_worker_signal HUP' HUP
		trap 'runner_handle_worker_signal INT' INT
		trap 'runner_handle_worker_signal TERM' TERM
		if [ -n "${TEST_SHELL_RUNNER:-}" ]; then
			"$TEST_SHELL_RUNNER" "$l_suite_path" >"$l_log_file" 2>&1 &
		else
			"$l_suite_path" >"$l_log_file" 2>&1 &
		fi
		l_suite_pid=$!
		printf '%s\n' "$l_suite_pid" >"$l_child_pid_file" 2>/dev/null || :
		l_launching_suite_child=0
		runner_consume_deferred_signal
		if wait "$l_suite_pid"; then
			l_status=0
		else
			l_status=$?
		fi
		trap - HUP INT TERM
		printf '%s\n' "$l_status" >"$l_status_file" 2>/dev/null || :
		exit "$l_status"
	) &
	l_pid=$!

	printf '%s\n' "$l_pid" >"$l_pid_file"
	RUNNER_PENDING_WORKERS=$(append_worker_id "$RUNNER_PENDING_WORKERS" "$l_worker_id")
	RUNNER_INFLIGHT_COUNT=$((RUNNER_INFLIGHT_COUNT + 1))
	RUNNER_NEXT_WORKER_ID=$((RUNNER_NEXT_WORKER_ID + 1))
	RUNNER_DEFER_SIGNALS=0
	consume_deferred_runner_signal
}

replay_suite_worker() {
	l_worker_id=$1
	l_path_file="$RUNNER_STATE_DIR/$l_worker_id.path"
	l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
	l_child_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
	l_log_file="$RUNNER_STATE_DIR/$l_worker_id.log"
	l_status_file="$RUNNER_STATE_DIR/$l_worker_id.status"
	l_ready_file="$RUNNER_STATE_DIR/$l_worker_id.ready"
	l_wait_status=0

	l_suite_path=$(cat "$l_path_file")
	l_pid=$(cat "$l_pid_file")

	if wait "$l_pid"; then
		l_wait_status=0
	else
		l_wait_status=$?
	fi

	l_status=$l_wait_status
	if [ -r "$l_status_file" ]; then
		l_status=$(cat "$l_status_file" 2>/dev/null || printf '%s\n' "$l_wait_status")
	fi

	emit_suite_banner "$l_suite_path"
	if [ -r "$l_log_file" ]; then
		cat "$l_log_file"
	fi

	if [ "$l_status" -eq 0 ]; then
		passed_count=$((passed_count + 1))
	else
		echo "!! Suite failed: $l_suite_path (exit status $l_status)" >&2
		overall_status=$l_status
		failed_count=$((failed_count + 1))
	fi

	rm -f "$l_path_file" "$l_pid_file" "$l_child_pid_file" "$l_log_file" "$l_status_file" "$l_ready_file"
}

wait_for_next_worker_completion() {
	[ "$RUNNER_INFLIGHT_COUNT" -gt 0 ] || return 0

	while [ "$RUNNER_INFLIGHT_COUNT" -gt 0 ]; do
		for l_worker_id in $RUNNER_PENDING_WORKERS; do
			l_path_file="$RUNNER_STATE_DIR/$l_worker_id.path"
			l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
			l_status_file="$RUNNER_STATE_DIR/$l_worker_id.status"
			l_ready_file="$RUNNER_STATE_DIR/$l_worker_id.ready"
			[ -r "$l_path_file" ] || continue
			[ -e "$l_ready_file" ] && continue
			if [ -r "$l_status_file" ]; then
				: >"$l_ready_file"
				RUNNER_INFLIGHT_COUNT=$((RUNNER_INFLIGHT_COUNT - 1))
				return 0
			fi
			[ -r "$l_pid_file" ] || continue
			l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*)
				continue
				;;
			esac
			if ! send_signal_to_pid 0 "$l_pid"; then
				: >"$l_ready_file"
				RUNNER_INFLIGHT_COUNT=$((RUNNER_INFLIGHT_COUNT - 1))
				return 0
			fi
		done
		sleep 1
	done

	return 0
}

replay_ready_workers_in_order() {
	while [ -n "$RUNNER_PENDING_WORKERS" ]; do
		case "$RUNNER_PENDING_WORKERS" in
		*" "*)
			l_worker_id=${RUNNER_PENDING_WORKERS%% *}
			l_remaining_workers=${RUNNER_PENDING_WORKERS#* }
			;;
		*)
			l_worker_id=$RUNNER_PENDING_WORKERS
			l_remaining_workers=""
			;;
		esac
		l_ready_file="$RUNNER_STATE_DIR/$l_worker_id.ready"
		[ -e "$l_ready_file" ] || return 0
		RUNNER_PENDING_WORKERS=$l_remaining_workers
		replay_suite_worker "$l_worker_id"
	done
}

flush_all_workers() {
	while [ -n "$RUNNER_PENDING_WORKERS" ]; do
		replay_ready_workers_in_order
		[ -n "$RUNNER_PENDING_WORKERS" ] || break
		wait_for_next_worker_completion
	done
}

signal_pending_workers() {
	l_signal=$1

	for l_worker_id in $RUNNER_PENDING_WORKERS; do
		l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
		if [ -r "$l_pid_file" ]; then
			l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*) ;;
			*)
				signal_pid_and_descendants "$l_signal" "$l_pid"
				;;
			esac
		fi

		l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
		if [ -r "$l_pid_file" ]; then
			l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*) ;;
			*)
				signal_pid_and_descendants "$l_signal" "$l_pid"
				;;
			esac
		fi
	done
}

pending_worker_pids_running_p() {
	for l_worker_id in $RUNNER_PENDING_WORKERS; do
		for l_pid_file in \
			"$RUNNER_STATE_DIR/$l_worker_id.child.pid" \
			"$RUNNER_STATE_DIR/$l_worker_id.pid"; do
			[ -r "$l_pid_file" ] || continue
			l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*)
				continue
				;;
			esac
			if send_signal_to_pid 0 "$l_pid"; then
				return 0
			fi
		done
	done

	return 1
}

runner_tracked_processes_running_p() {
	if foreground_suite_running_p; then
		return 0
	fi

	if pending_worker_pids_running_p; then
		return 0
	fi

	return 1
}

wait_for_runner_tracked_shutdown() {
	l_remaining=$RUNNER_SIGNAL_SHUTDOWN_GRACE_SECONDS

	while [ "$l_remaining" -gt 0 ]; do
		if ! runner_tracked_processes_running_p; then
			return 0
		fi
		sleep 1
		l_remaining=$((l_remaining - 1))
	done

	if ! runner_tracked_processes_running_p; then
		return 0
	fi

	return 1
}

wait_for_runner_tracked_processes() {
	case "${RUNNER_FOREGROUND_SUITE_PID:-}" in
	'' | *[!0-9]*) ;;
	*)
		wait "$RUNNER_FOREGROUND_SUITE_PID" >/dev/null 2>&1 || true
		RUNNER_FOREGROUND_SUITE_PID=""
		;;
	esac

	for l_worker_id in $RUNNER_PENDING_WORKERS; do
		l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
		[ -r "$l_pid_file" ] || continue
		l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
		case "$l_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		wait "$l_pid" >/dev/null 2>&1 || true
	done
}

signal_exit_status() {
	case "$1" in
	HUP)
		printf '%s\n' 129
		;;
	INT)
		printf '%s\n' 130
		;;
	TERM)
		printf '%s\n' 143
		;;
	*)
		printf '%s\n' 1
		;;
	esac
}

handle_runner_signal() {
	l_signal=$1
	l_status=$(signal_exit_status "$l_signal")

	if [ "${RUNNER_DEFER_SIGNALS:-0}" = "1" ]; then
		remember_deferred_runner_signal "$l_signal"
		return 0
	fi

	if [ "$RUNNER_SHUTTING_DOWN" = "1" ]; then
		exit "$l_status"
	fi

	RUNNER_SHUTTING_DOWN=1
	trap - HUP INT TERM
	signal_foreground_suite TERM
	signal_pending_workers TERM
	if ! wait_for_runner_tracked_shutdown; then
		signal_foreground_suite KILL
		signal_pending_workers KILL
		wait_for_runner_tracked_shutdown || :
	fi
	wait_for_runner_tracked_processes
	cleanup_runner_state
	exit "$l_status"
}

if [ "$#" -gt 0 ]; then
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
		--)
			shift
			break
			;;
		-h | --help)
			print_usage
			exit 0
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

if [ "$#" -eq 0 ]; then
	echo "No shunit2 suites found in $TEST_DIR" >&2
	exit 1
fi

resolve_test_shell_runner

overall_status=0
passed_count=0
failed_count=0

runnable_count=$(count_runnable_suites "$@")
resolve_parallel_jobs "$runnable_count"

trap 'handle_runner_signal HUP' HUP
trap 'handle_runner_signal INT' INT
trap 'handle_runner_signal TERM' TERM

if [ "$RUNNER_PARALLEL_JOBS" -gt 1 ]; then
	ensure_runner_state_dir
fi

for suite in "$@"; do
	suite_path=$(resolve_suite_path "$suite")

	if [ ! -f "$suite_path" ]; then
		flush_all_workers
		echo "Skipping missing suite: $suite_path" >&2
		overall_status=1
		failed_count=$((failed_count + 1))
		continue
	fi

	case "$(basename "$suite_path")" in
	test_helper.sh)
		flush_all_workers
		echo "==> Skipping helper library: $suite_path"
		continue
		;;
	esac

	if [ "$RUNNER_PARALLEL_JOBS" -eq 1 ]; then
		run_suite_foreground "$suite_path"
		continue
	fi

	launch_suite_worker "$suite_path"
	if [ "$RUNNER_INFLIGHT_COUNT" -ge "$RUNNER_PARALLEL_JOBS" ]; then
		wait_for_next_worker_completion
		replay_ready_workers_in_order
	fi
done

if [ "$RUNNER_PARALLEL_JOBS" -gt 1 ]; then
	flush_all_workers
fi

trap - HUP INT TERM
cleanup_runner_state

echo "==> shunit2 summary: ${passed_count} passed, ${failed_count} failed"

exit "$overall_status"
