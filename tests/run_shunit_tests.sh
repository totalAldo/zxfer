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
RUNNER_FOREGROUND_SUITE_TOKEN=""
RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=""
RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE=""
RUNNER_FOREGROUND_SUITE_STATUS_FILE=""
RUNNER_SHUTTING_DOWN=0
RUNNER_SIGNAL_SHUTDOWN_GRACE_SECONDS=2
RUNNER_LIST_MODE=
RUNNER_CURRENT_SUITE_OPTION=
RUNNER_SELECTED_SUITES=
RUNNER_NAMED_TEST_SELECTIONS=
RUNNER_POSITIONAL_TEST_NAMES=
RUNNER_HAS_NAMED_TESTS=0
TAB=$(printf '\t')

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
  --jobs count   bound concurrent suite workers
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
	'' | [!A-Za-z_]* | *[!A-Za-z0-9_]*)
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

signal_number_for_name() {
	case "$1" in
	0)
		printf '%s\n' 0
		;;
	HUP | hup)
		printf '%s\n' 1
		;;
	INT | int)
		printf '%s\n' 2
		;;
	KILL | kill)
		printf '%s\n' 9
		;;
	TERM | term)
		printf '%s\n' 15
		;;
	*)
		return 1
		;;
	esac
}

send_signal_to_pid() {
	l_send_signal_to_pid_signal=$1
	l_send_signal_to_pid_pid=$2
	l_send_signal_to_pid_number=

	case "$l_send_signal_to_pid_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# Some illumos/Solaris shells have historically been less consistent
	# about symbolic signal forms; keep numeric signals as the final fallback.
	kill -s "$l_send_signal_to_pid_signal" "$l_send_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	kill "-$l_send_signal_to_pid_signal" "$l_send_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	l_send_signal_to_pid_number=$(signal_number_for_name "$l_send_signal_to_pid_signal" 2>/dev/null || true)
	if [ -n "$l_send_signal_to_pid_number" ]; then
		kill "-$l_send_signal_to_pid_number" "$l_send_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	fi
	return 1
}

runner_get_process_start_token_for_selector() {
	l_runner_selector_token_pid=$1
	l_runner_selector_token_selector=$2

	case "$l_runner_selector_token_pid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	case "$l_runner_selector_token_selector" in
	lstart | stime) ;;
	*) return 1 ;;
	esac
	if l_runner_selector_token_raw=$(LC_ALL=C ps \
		-p "$l_runner_selector_token_pid" \
		-o "$l_runner_selector_token_selector=" 2>/dev/null); then
		:
	else
		l_runner_selector_token_raw=
	fi
	case $- in
	*f*) l_runner_selector_token_restore_glob=0 ;;
	*)
		l_runner_selector_token_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_runner_selector_token_saved_ifs_set=1
		l_runner_selector_token_saved_ifs=$IFS
	else
		l_runner_selector_token_saved_ifs_set=0
		l_runner_selector_token_saved_ifs=
	fi
	unset IFS
	# shellcheck disable=SC2086
	set -- $l_runner_selector_token_raw
	l_runner_selector_token_normalized=$*
	if [ "$l_runner_selector_token_saved_ifs_set" -eq 1 ]; then
		IFS=$l_runner_selector_token_saved_ifs
	else
		unset IFS
	fi
	if [ "$l_runner_selector_token_restore_glob" -eq 1 ]; then
		set +f
	fi
	[ "$#" -gt 0 ] || return 1
	printf '%s:%s\n' \
		"$l_runner_selector_token_selector" "$l_runner_selector_token_normalized"
}

runner_get_process_start_token() {
	l_runner_token_pid=$1
	l_runner_token_value=

	if l_runner_token_value=$(runner_get_process_start_token_for_selector \
		"$l_runner_token_pid" lstart); then
		printf '%s\n' "$l_runner_token_value"
		return 0
	fi
	runner_get_process_start_token_for_selector "$l_runner_token_pid" stime
}

runner_child_pid_matches_parent() {
	l_runner_child_parent=$1
	l_runner_child_pid=$2
	l_runner_child_candidates=

	if l_runner_child_candidates=$(list_child_pids_for_parent \
		"$l_runner_child_parent"); then
		:
	else
		# Status 2 distinguishes unavailable process enumeration from an
		# observed parent/child mismatch. Existing boolean callers treat both as
		# failure; ownership cleanup uses the distinction for diagnostics.
		return 2
	fi
	for l_runner_child_current in $l_runner_child_candidates; do
		[ "$l_runner_child_current" = "$l_runner_child_pid" ] && return 0
	done
	return 1
}

runner_capture_child_identity() {
	l_runner_identity_parent=$1
	l_runner_identity_pid=$2
	l_runner_identity_before=

	l_runner_identity_before=$(runner_get_process_start_token "$l_runner_identity_pid") || return 1
	runner_child_pid_matches_parent "$l_runner_identity_parent" "$l_runner_identity_pid" || return 1
	runner_process_identity_matches \
		"$l_runner_identity_pid" "$l_runner_identity_before" || return 1
	printf '%s\n' "$l_runner_identity_before"
}

runner_capture_child_identity_with_retry() {
	l_runner_retry_identity_parent=$1
	l_runner_retry_identity_pid=$2
	l_runner_retry_identity_remaining=3
	l_runner_retry_identity_value=

	while [ "$l_runner_retry_identity_remaining" -gt 0 ]; do
		if l_runner_retry_identity_value=$(runner_capture_child_identity \
			"$l_runner_retry_identity_parent" "$l_runner_retry_identity_pid" \
			2>/dev/null); then
			printf '%s\n' "$l_runner_retry_identity_value"
			return 0
		fi
		l_runner_retry_identity_remaining=$((l_runner_retry_identity_remaining - 1))
	done
	return 1
}

runner_read_numeric_pid_file() {
	l_runner_pid_file_path=$1
	l_runner_pid_file_value=

	[ -r "$l_runner_pid_file_path" ] || return 1
	IFS= read -r l_runner_pid_file_value <"$l_runner_pid_file_path" ||
		[ -n "$l_runner_pid_file_value" ] || return 1
	case "$l_runner_pid_file_value" in
	'' | *[!0-9]*) return 1 ;;
	esac
	printf '%s\n' "$l_runner_pid_file_value"
}

# Capture a child identity only after the parent-published worker PID proves
# the relationship. This permits launch-race retries without adopting a PID
# that a shell may already have reaped and the kernel may have reused.
runner_capture_child_identity_from_parent_file() {
	l_runner_file_identity_parent_file=$1
	l_runner_file_identity_pid=$2
	l_runner_file_identity_remaining=3
	l_runner_file_identity_parent=
	l_runner_file_identity_value=

	while [ "$l_runner_file_identity_remaining" -gt 0 ]; do
		if l_runner_file_identity_parent=$(runner_read_numeric_pid_file \
			"$l_runner_file_identity_parent_file" 2>/dev/null); then
			if l_runner_file_identity_value=$(runner_capture_child_identity \
				"$l_runner_file_identity_parent" "$l_runner_file_identity_pid" \
				2>/dev/null); then
				printf '%s\n' "$l_runner_file_identity_value"
				return 0
			fi
		else
			l_runner_file_identity_parent=
		fi
		l_runner_file_identity_remaining=$((l_runner_file_identity_remaining - 1))
		if [ -z "$l_runner_file_identity_parent" ] &&
			[ "$l_runner_file_identity_remaining" -gt 0 ]; then
			sleep 1 || true
		fi
	done
	return 1
}

runner_process_identity_matches() {
	l_runner_match_pid=$1
	l_runner_match_expected=$2
	l_runner_match_current=
	l_runner_match_selector=

	case "$l_runner_match_expected" in
	lstart:* | stime:*)
		l_runner_match_selector=${l_runner_match_expected%%:*}
		;;
	*) return 1 ;;
	esac
	l_runner_match_current=$(runner_get_process_start_token_for_selector \
		"$l_runner_match_pid" "$l_runner_match_selector") || return 1
	[ "$l_runner_match_current" = "$l_runner_match_expected" ]
}

process_state_for_pid() {
	l_process_state_for_pid_pid=$1
	l_process_state_for_pid_state=

	l_process_state_for_pid_state=$(ps -o stat= -p "$l_process_state_for_pid_pid" 2>/dev/null |
		awk '
			$1 == "STAT" || $1 == "STATE" { next }
			$1 != "" { print $1; exit }
		')
	if [ -z "$l_process_state_for_pid_state" ]; then
		l_process_state_for_pid_state=$(ps -o state= -p "$l_process_state_for_pid_pid" 2>/dev/null |
			awk '
				$1 == "S" || $1 == "STAT" || $1 == "STATE" { next }
				$1 != "" { print $1; exit }
			')
	fi
	if [ -z "$l_process_state_for_pid_state" ]; then
		l_process_state_for_pid_state=$(ps -o s= -p "$l_process_state_for_pid_pid" 2>/dev/null |
			awk '
				$1 == "S" || $1 == "STAT" || $1 == "STATE" { next }
				$1 != "" { print $1; exit }
			')
	fi

	printf '%s\n' "$l_process_state_for_pid_state"
}

process_running_p() {
	l_process_running_p_pid=$1
	l_process_running_p_state=

	if ! send_signal_to_pid 0 "$l_process_running_p_pid"; then
		return 1
	fi

	l_process_running_p_state=$(process_state_for_pid "$l_process_running_p_pid")
	case "$l_process_running_p_state" in
	Z* | z* | *zombie* | *defunct*)
		return 1
		;;
	esac

	return 0
}

snapshot_process_descendants() {
	l_snapshot_pending=$1
	l_snapshot_next=
	l_snapshot_records=
	l_snapshot_parent=
	l_snapshot_child=
	l_snapshot_token=
	l_snapshot_record=

	while [ -n "$l_snapshot_pending" ]; do
		l_snapshot_next=
		for l_snapshot_parent in $l_snapshot_pending; do
			for l_snapshot_child in $(list_child_pids_for_parent "$l_snapshot_parent"); do
				l_snapshot_token=$(runner_capture_child_identity \
					"$l_snapshot_parent" "$l_snapshot_child") || continue
				l_snapshot_next="${l_snapshot_next}${l_snapshot_next:+ }$l_snapshot_child"
				l_snapshot_record=$(printf '%s\t%s' "$l_snapshot_child" "$l_snapshot_token")
				if [ -n "$l_snapshot_records" ]; then
					l_snapshot_records="$l_snapshot_record
$l_snapshot_records"
				else
					l_snapshot_records=$l_snapshot_record
				fi
			done
		done
		l_snapshot_pending=$l_snapshot_next
	done
	printf '%s\n' "$l_snapshot_records"
}

signal_process_descendant_records() {
	l_descendant_signal=$1
	l_descendant_records=$2
	l_descendant_tab=$(printf '\t')

	while IFS="$l_descendant_tab" read -r l_descendant_pid l_descendant_token; do
		[ -n "$l_descendant_pid" ] || continue
		runner_process_identity_matches "$l_descendant_pid" "$l_descendant_token" || continue
		send_signal_to_pid "$l_descendant_signal" "$l_descendant_pid" || true
	done <<EOF
$l_descendant_records
EOF
}

process_descendant_records_running_p() {
	l_descendant_records=$1
	l_descendant_tab=$(printf '\t')

	while IFS="$l_descendant_tab" read -r l_descendant_pid l_descendant_token; do
		[ -n "$l_descendant_pid" ] || continue
		runner_process_identity_matches "$l_descendant_pid" "$l_descendant_token" || continue
		process_running_p "$l_descendant_pid" && return 0
	done <<EOF
$l_descendant_records
EOF
	return 1
}

wait_for_process_descendant_records() {
	l_descendant_records=$1
	l_descendant_remaining=$RUNNER_SIGNAL_SHUTDOWN_GRACE_SECONDS

	while [ "$l_descendant_remaining" -gt 0 ]; do
		process_descendant_records_running_p "$l_descendant_records" || return 0
		sleep 1 || true
		l_descendant_remaining=$((l_descendant_remaining - 1))
	done
	process_descendant_records_running_p "$l_descendant_records" && return 1
	return 0
}

signal_process_descendant_records_with_escalation() {
	l_descendant_escalation_signal=$1
	l_descendant_escalation_records=$2

	[ -n "$l_descendant_escalation_records" ] || return 0
	signal_process_descendant_records \
		"$l_descendant_escalation_signal" "$l_descendant_escalation_records"
	case "$l_descendant_escalation_signal" in
	TERM | term)
		if ! wait_for_process_descendant_records \
			"$l_descendant_escalation_records"; then
			signal_process_descendant_records KILL \
				"$l_descendant_escalation_records"
			wait_for_process_descendant_records \
				"$l_descendant_escalation_records" || true
		fi
		;;
	esac
}

signal_process_descendants() {
	l_signal_process_descendants_signal=$1
	l_signal_process_descendants_root=$2
	l_signal_process_descendants_root_token=$3
	l_signal_process_descendants_records=

	runner_process_identity_matches \
		"$l_signal_process_descendants_root" \
		"$l_signal_process_descendants_root_token" || return 0
	l_signal_process_descendants_records=$(snapshot_process_descendants \
		"$l_signal_process_descendants_root")
	signal_process_descendant_records_with_escalation \
		"$l_signal_process_descendants_signal" \
		"$l_signal_process_descendants_records"
}

# Verify every direct parent/child link in one colon-separated PID chain.
# Callers construct chains only from numeric PIDs observed in private runner
# state and current process relationships.
runner_owned_process_chain_matches() {
	l_owned_chain_remaining=$1
	l_owned_chain_previous=${l_owned_chain_remaining%%:*}

	case "$l_owned_chain_remaining" in
	'' | *[!0-9:]* | *:) return 1 ;;
	*:*) ;;
	*) return 1 ;;
	esac
	case "$l_owned_chain_previous" in
	'' | *[!0-9]*) return 1 ;;
	esac
	l_owned_chain_remaining=${l_owned_chain_remaining#*:}

	while [ -n "$l_owned_chain_remaining" ]; do
		case "$l_owned_chain_remaining" in
		*:*)
			l_owned_chain_current=${l_owned_chain_remaining%%:*}
			l_owned_chain_remaining=${l_owned_chain_remaining#*:}
			;;
		*)
			l_owned_chain_current=$l_owned_chain_remaining
			l_owned_chain_remaining=
			;;
		esac
		case "$l_owned_chain_current" in
		'' | *[!0-9]*) return 1 ;;
		esac
		if runner_child_pid_matches_parent \
			"$l_owned_chain_previous" "$l_owned_chain_current"; then
			:
		else
			l_owned_chain_match_status=$?
			return "$l_owned_chain_match_status"
		fi
		l_owned_chain_previous=$l_owned_chain_current
	done
	return 0
}

# Snapshot descendant ownership paths deepest-first. These paths are used only
# when process-start tokens are unavailable. Revalidating the entire path
# immediately before each signal prevents adoption of reparented processes.
snapshot_owned_process_descendant_chains() {
	l_owned_snapshot_initial_chain=$1
	l_owned_snapshot_pending=$l_owned_snapshot_initial_chain
	l_owned_snapshot_records=

	if runner_owned_process_chain_matches \
		"$l_owned_snapshot_initial_chain"; then
		:
	else
		l_owned_snapshot_status=$?
		return "$l_owned_snapshot_status"
	fi
	while [ -n "$l_owned_snapshot_pending" ]; do
		l_owned_snapshot_next=
		while IFS= read -r l_owned_snapshot_chain; do
			[ -n "$l_owned_snapshot_chain" ] || continue
			if runner_owned_process_chain_matches \
				"$l_owned_snapshot_chain"; then
				:
			else
				l_owned_snapshot_status=$?
				[ "$l_owned_snapshot_status" -eq 2 ] && return 2
				continue
			fi
			l_owned_snapshot_parent=${l_owned_snapshot_chain##*:}
			if l_owned_snapshot_children=$(list_child_pids_for_parent \
				"$l_owned_snapshot_parent"); then
				:
			else
				return 2
			fi
			for l_owned_snapshot_child in $l_owned_snapshot_children; do
				case "$l_owned_snapshot_child" in
				'' | *[!0-9]*) continue ;;
				esac
				l_owned_snapshot_new_chain="${l_owned_snapshot_chain}:$l_owned_snapshot_child"
				if runner_owned_process_chain_matches \
					"$l_owned_snapshot_new_chain"; then
					:
				else
					l_owned_snapshot_status=$?
					[ "$l_owned_snapshot_status" -eq 2 ] && return 2
					continue
				fi
				if [ -n "$l_owned_snapshot_next" ]; then
					l_owned_snapshot_next="$l_owned_snapshot_next
$l_owned_snapshot_new_chain"
				else
					l_owned_snapshot_next=$l_owned_snapshot_new_chain
				fi
				if [ -n "$l_owned_snapshot_records" ]; then
					l_owned_snapshot_records="$l_owned_snapshot_new_chain
$l_owned_snapshot_records"
				else
					l_owned_snapshot_records=$l_owned_snapshot_new_chain
				fi
			done
		done <<EOF
$l_owned_snapshot_pending
EOF
		l_owned_snapshot_pending=$l_owned_snapshot_next
	done
	printf '%s\n' "$l_owned_snapshot_records"
}

kill_owned_process_descendant_chains() {
	l_owned_kill_records=$1

	while IFS= read -r l_owned_kill_chain; do
		[ -n "$l_owned_kill_chain" ] || continue
		if runner_owned_process_chain_matches "$l_owned_kill_chain"; then
			:
		else
			l_owned_kill_status=$?
			[ "$l_owned_kill_status" -eq 2 ] && return 2
			continue
		fi
		l_owned_kill_pid=${l_owned_kill_chain##*:}
		if send_signal_to_pid KILL "$l_owned_kill_pid"; then
			:
		elif runner_owned_process_chain_matches "$l_owned_kill_chain"; then
			# The descendant is still attached but could not be killed (for
			# example, after a credential change). Keep its ancestors alive.
			return 1
		else
			l_owned_kill_status=$?
			[ "$l_owned_kill_status" -eq 2 ] && return 2
		fi
	done <<EOF
$l_owned_kill_records
EOF
	return 0
}

# Signal only a currently attached direct child of a live two-level ownership
# chain. This fallback is intentionally narrower than the persisted-PID APIs:
# it is used only while the top-level runner still owns the wrapper and that
# wrapper still owns the suite, so reused or reparented PIDs remain fail-closed.
signal_owned_child_and_descendants() {
	l_owned_child_signal=$1
	l_owned_child_owner_parent_pid=$2
	l_owned_child_owner_pid=$3
	l_owned_child_pid=$4
	l_owned_child_chain=
	l_owned_child_descendant_chains=
	l_owned_child_snapshot_ok=0
	l_owned_child_snapshot_remaining=3

	case "$l_owned_child_owner_parent_pid" in
	'' | *[!0-9]*) return 0 ;;
	esac
	case "$l_owned_child_owner_pid" in
	'' | *[!0-9]*) return 0 ;;
	esac
	case "$l_owned_child_pid" in
	'' | *[!0-9]*) return 0 ;;
	esac
	l_owned_child_chain="${l_owned_child_owner_parent_pid}:${l_owned_child_owner_pid}:${l_owned_child_pid}"
	while [ "$l_owned_child_snapshot_remaining" -gt 0 ]; do
		if l_owned_child_descendant_chains=$(snapshot_owned_process_descendant_chains \
			"$l_owned_child_chain"); then
			l_owned_child_snapshot_ok=1
			break
		else
			l_owned_child_snapshot_status=$?
			[ "$l_owned_child_snapshot_status" -eq 1 ] && return 0
		fi
		l_owned_child_snapshot_remaining=$((l_owned_child_snapshot_remaining - 1))
		[ "$l_owned_child_snapshot_remaining" -gt 0 ] && sleep 1 || true
	done
	if [ "$l_owned_child_snapshot_ok" -ne 1 ]; then
		# Do not terminate a possible ancestor when its complete descendant set
		# could not be enumerated; keeping ownership intact is safer than orphaning
		# an unknown suite process.
		echo "Unable to verify shunit suite descendants; waiting for the owning wrapper to reap them." >&2
		return 0
	fi
	# A tokenless descendant cannot be followed safely after its parent exits.
	# Kill proven descendants deepest-first before giving the direct suite child
	# its requested graceful signal.
	if ! kill_owned_process_descendant_chains \
		"$l_owned_child_descendant_chains"; then
		echo "Unable to revalidate shunit suite descendants; waiting for the owning wrapper to reap them." >&2
		return 0
	fi
	if runner_owned_process_chain_matches "$l_owned_child_chain"; then
		:
	else
		l_owned_child_chain_status=$?
		if [ "$l_owned_child_chain_status" -eq 2 ]; then
			echo "Unable to revalidate the shunit suite owner; waiting for the owning wrapper to reap it." >&2
		fi
		return 0
	fi
	send_signal_to_pid "$l_owned_child_signal" "$l_owned_child_pid" || true
}

signal_pid_and_descendants() {
	l_signal=$1
	l_pid=$2
	l_pid_token=$3

	case "$l_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	runner_process_identity_matches "$l_pid" "$l_pid_token" || return 0
	signal_process_descendants "$l_signal" "$l_pid" "$l_pid_token"
	runner_process_identity_matches "$l_pid" "$l_pid_token" || return 0
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
			'' | /* | ../* | */../* | */.. | *[!A-Za-z0-9_./-]*)
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

ensure_runner_state_dir() {
	[ -n "$RUNNER_STATE_DIR" ] && return 0
	l_runner_state_parent=${TMPDIR:-/tmp}
	case "$l_runner_state_parent" in
	/*) l_runner_state_template="$l_runner_state_parent/zxfer_shunit.XXXXXX" ;;
	*) l_runner_state_template="./$l_runner_state_parent/zxfer_shunit.XXXXXX" ;;
	esac

	# Pass the parent explicitly. Some execution wrappers preserve the TMPDIR
	# value in the environment while the platform mktemp -t implementation
	# still falls back to its default temporary directory.
	RUNNER_STATE_DIR=$(mktemp -d "$l_runner_state_template") || {
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
	RUNNER_FOREGROUND_SUITE_TOKEN=""
	RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=""
	RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE=""
	RUNNER_FOREGROUND_SUITE_STATUS_FILE=""
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

read_runner_process_token() {
	l_read_runner_token_file=$1
	l_read_runner_token=

	[ -r "$l_read_runner_token_file" ] || return 1
	l_read_runner_token=$(cat "$l_read_runner_token_file" 2>/dev/null || true)
	[ -n "$l_read_runner_token" ] || return 1
	printf '%s\n' "$l_read_runner_token"
}

tracked_runner_process_running_p() {
	l_tracked_runner_running_pid=$1
	l_tracked_runner_running_token=$2
	l_tracked_runner_allow_unverified=${3:-0}

	if [ -n "$l_tracked_runner_running_token" ]; then
		runner_process_identity_matches \
			"$l_tracked_runner_running_pid" \
			"$l_tracked_runner_running_token" || return 1
	elif [ "$l_tracked_runner_allow_unverified" != "1" ]; then
		return 1
	fi
	process_running_p "$l_tracked_runner_running_pid"
}

signal_tracked_runner_process() {
	l_tracked_runner_signal=$1
	l_tracked_runner_signal_pid=$2
	l_tracked_runner_signal_token=$3

	case "$l_tracked_runner_signal_pid" in
	'' | *[!0-9]*) return 0 ;;
	esac
	runner_process_identity_matches \
		"$l_tracked_runner_signal_pid" \
		"$l_tracked_runner_signal_token" || return 0
	send_signal_to_pid \
		"$l_tracked_runner_signal" "$l_tracked_runner_signal_pid" || true
}

signal_foreground_suite() {
	l_signal=$1
	l_child_pid=""
	l_child_token=""

	case "${RUNNER_FOREGROUND_SUITE_PID:-}" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	if [ -r "${RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE:-}" ]; then
		l_child_pid=$(cat "$RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE" 2>/dev/null || true)
	fi
	if [ -n "${RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE:-}" ]; then
		l_child_token=$(read_runner_process_token \
			"$RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE" 2>/dev/null || true)
	fi
	case "$l_child_pid" in
	'' | *[!0-9]*)
		signal_pid_and_descendants \
			"$l_signal" "$RUNNER_FOREGROUND_SUITE_PID" \
			"$RUNNER_FOREGROUND_SUITE_TOKEN"
		;;
	*)
		if [ -n "$l_child_token" ]; then
			signal_pid_and_descendants \
				"$l_signal" "$l_child_pid" "$l_child_token"
			signal_tracked_runner_process \
				"$l_signal" "$RUNNER_FOREGROUND_SUITE_PID" \
				"$RUNNER_FOREGROUND_SUITE_TOKEN"
		else
			# The wrapper still owns a published child whose identity could not
			# be verified. Keep the wrapper alive to reap it rather than risk
			# orphaning the child by signalling its parent.
			return 0
		fi
		;;
	esac
}

signal_foreground_suite_child() {
	l_signal=$1
	l_child_pid=""
	l_child_token=""

	case "${RUNNER_FOREGROUND_SUITE_PID:-}" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	if [ -r "${RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE:-}" ]; then
		l_child_pid=$(cat "$RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE" 2>/dev/null || true)
	fi
	if [ -n "${RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE:-}" ]; then
		l_child_token=$(read_runner_process_token \
			"$RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE" 2>/dev/null || true)
	fi
	case "$l_child_pid" in
	'' | *[!0-9]*)
		signal_process_descendants \
			"$l_signal" "$RUNNER_FOREGROUND_SUITE_PID" \
			"$RUNNER_FOREGROUND_SUITE_TOKEN"
		;;
	*)
		if [ -n "$l_child_token" ]; then
			signal_pid_and_descendants \
				"$l_signal" "$l_child_pid" "$l_child_token"
		else
			signal_owned_child_and_descendants \
				"$l_signal" "$$" \
				"$RUNNER_FOREGROUND_SUITE_PID" "$l_child_pid"
		fi
		;;
	esac
}

foreground_suite_running_p() {
	l_child_pid=""
	l_child_token=""

	case "${RUNNER_FOREGROUND_SUITE_PID:-}" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# A readable status file is written only after the foreground wrapper has
	# reaped its suite child. Do not keep consulting a stale child PID after
	# that point; illumos/OmniOS can keep recently reaped PIDs observable long
	# enough to confuse signal-cleanup polling.
	if [ -r "${RUNNER_FOREGROUND_SUITE_STATUS_FILE:-}" ]; then
		return 1
	fi

	if tracked_runner_process_running_p \
		"$RUNNER_FOREGROUND_SUITE_PID" \
		"$RUNNER_FOREGROUND_SUITE_TOKEN" 1; then
		return 0
	fi
	if [ -r "${RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE:-}" ]; then
		l_child_pid=$(cat "$RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE" 2>/dev/null || true)
	fi
	if [ -n "${RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE:-}" ]; then
		l_child_token=$(read_runner_process_token \
			"$RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE" 2>/dev/null || true)
	fi
	case "$l_child_pid" in
	'' | *[!0-9]*) ;;
	*)
		if tracked_runner_process_running_p "$l_child_pid" "$l_child_token"; then
			return 0
		fi
		;;
	esac

	return 1
}

run_suite_foreground() {
	l_suite_path=$1
	l_selected_test_names=${2:-}
	l_status_file=
	l_wrapper_pid_file=
	l_wrapper_owner_pid_file=
	l_child_pid_file=
	l_child_owner_pid_file=
	l_child_token_file=
	l_wait_status=0

	emit_suite_banner "$l_suite_path"
	if ! ensure_runner_state_dir; then
		overall_status=1
		failed_count=$((failed_count + 1))
		return 0
	fi
	l_status_file="$RUNNER_STATE_DIR/foreground.status"
	l_wrapper_pid_file="$RUNNER_STATE_DIR/foreground.pid"
	l_wrapper_owner_pid_file="$RUNNER_STATE_DIR/foreground.owner.pid"
	l_child_pid_file="$RUNNER_STATE_DIR/foreground.child.pid"
	l_child_owner_pid_file="$RUNNER_STATE_DIR/foreground.child.owner.pid"
	l_child_token_file="$RUNNER_STATE_DIR/foreground.child.token"
	rm -f "$l_status_file"
	rm -f "$l_wrapper_pid_file"
	rm -f "$l_wrapper_owner_pid_file"
	rm -f "$l_child_pid_file"
	rm -f "$l_child_owner_pid_file"
	rm -f "$l_child_token_file"
	printf '%s\n' "$$" >"$l_wrapper_owner_pid_file"
	RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=$l_child_pid_file
	RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE=$l_child_token_file
	RUNNER_FOREGROUND_SUITE_STATUS_FILE=$l_status_file

	# macOS /bin/sh can defer traps while blocked in wait, so serial mode
	# polls this wrapper's status file instead of waiting directly on the suite.
	RUNNER_DEFER_SIGNALS=1
	(
		set +e
		trap - HUP INT TERM
		set -- "$l_suite_path"
		if [ -n "$l_selected_test_names" ]; then
			set -- "$@" --
		fi
		while IFS= read -r l_test_name; do
			[ -n "$l_test_name" ] || continue
			set -- "$@" "$l_test_name"
		done <<EOF
$l_selected_test_names
EOF
		if [ -n "${TEST_SHELL_RUNNER:-}" ]; then
			"$TEST_SHELL_RUNNER" "$@" &
		else
			"$@" &
		fi
		l_suite_pid=$!
		printf '%s\n' "$l_suite_pid" >"$l_child_pid_file" 2>/dev/null || :
		if l_suite_token=$(runner_capture_child_identity_from_parent_file \
			"$l_wrapper_pid_file" "$l_suite_pid" 2>/dev/null); then
			:
		else
			l_suite_token=
		fi
		if [ -n "$l_suite_token" ]; then
			printf '%s\n' "$l_suite_token" >"$l_child_token_file" 2>/dev/null || :
		fi
		wait "$l_suite_pid"
		l_status=$?
		printf '%s\n' "$l_status" >"$l_status_file" 2>/dev/null || :
		exit "$l_status"
	) &
	RUNNER_FOREGROUND_SUITE_PID=$!
	printf '%s\n' "$RUNNER_FOREGROUND_SUITE_PID" \
		>"$l_child_owner_pid_file"
	printf '%s\n' "$RUNNER_FOREGROUND_SUITE_PID" >"$l_wrapper_pid_file"
	if RUNNER_FOREGROUND_SUITE_TOKEN=$(runner_capture_child_identity_with_retry \
		"$$" "$RUNNER_FOREGROUND_SUITE_PID" 2>/dev/null); then
		:
	else
		RUNNER_FOREGROUND_SUITE_TOKEN=
	fi
	RUNNER_DEFER_SIGNALS=0
	consume_deferred_runner_signal

	while [ ! -r "$l_status_file" ]; do
		if ! foreground_suite_running_p; then
			break
		fi
		# OmniOS /bin/sh can let errexit win over a pending trap when a
		# foreground sleep is interrupted by TERM. Keep polling sleeps non-fatal
		# so signal traps still run cleanup.
		sleep 1 || true
	done

	if wait "$RUNNER_FOREGROUND_SUITE_PID"; then
		l_wait_status=0
	else
		l_wait_status=$?
	fi
	RUNNER_FOREGROUND_SUITE_PID=""
	RUNNER_FOREGROUND_SUITE_TOKEN=""
	RUNNER_FOREGROUND_SUITE_CHILD_PID_FILE=""
	RUNNER_FOREGROUND_SUITE_CHILD_TOKEN_FILE=""
	RUNNER_FOREGROUND_SUITE_STATUS_FILE=""

	l_status=$l_wait_status
	if [ -r "$l_status_file" ]; then
		l_status=$(cat "$l_status_file" 2>/dev/null || printf '%s\n' "$l_wait_status")
	fi
	rm -f "$l_status_file" "$l_wrapper_pid_file" \
		"$l_wrapper_owner_pid_file" "$l_child_pid_file" \
		"$l_child_owner_pid_file" "$l_child_token_file"

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
	l_selected_test_names=${2:-}
	l_worker_id=$RUNNER_NEXT_WORKER_ID
	l_log_file="$RUNNER_STATE_DIR/$l_worker_id.log"
	l_status_file="$RUNNER_STATE_DIR/$l_worker_id.status"
	l_path_file="$RUNNER_STATE_DIR/$l_worker_id.path"
	l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
	l_owner_pid_file="$RUNNER_STATE_DIR/$l_worker_id.owner.pid"
	l_token_file="$RUNNER_STATE_DIR/$l_worker_id.token"
	l_child_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
	l_child_owner_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.owner.pid"
	l_child_token_file="$RUNNER_STATE_DIR/$l_worker_id.child.token"
	l_ready_file="$RUNNER_STATE_DIR/$l_worker_id.ready"

	printf '%s\n' "$l_suite_path" >"$l_path_file"
	printf '%s\n' "$$" >"$l_owner_pid_file"
	rm -f "$l_token_file"
	rm -f "$l_child_pid_file"
	rm -f "$l_child_owner_pid_file"
	rm -f "$l_child_token_file"
	rm -f "$l_ready_file"

	RUNNER_DEFER_SIGNALS=1
	(
		set +e
		l_suite_pid=
		l_suite_token=
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
			l_suite_owner_pid=
			case "${l_suite_pid:-}" in
			'' | *[!0-9]*)
				return 0
				;;
			esac
			if [ -z "${l_suite_token:-}" ]; then
				if l_suite_token=$(runner_capture_child_identity_from_parent_file \
					"$l_pid_file" "$l_suite_pid" 2>/dev/null); then
					:
				else
					l_suite_token=
				fi
				if [ -n "$l_suite_token" ]; then
					printf '%s\n' "$l_suite_token" \
						>"$l_child_token_file" 2>/dev/null || :
				fi
			fi
			if [ -n "${l_suite_token:-}" ]; then
				signal_pid_and_descendants \
					"$l_signal" "$l_suite_pid" "$l_suite_token"
			else
				l_suite_owner_pid=$(runner_read_numeric_pid_file \
					"$l_pid_file" 2>/dev/null || true)
				signal_owned_child_and_descendants \
					"$l_signal" "$$" \
					"$l_suite_owner_pid" "$l_suite_pid"
			fi
		}
		runner_suite_child_running_p() {
			l_suite_owner_pid=
			case "${l_suite_pid:-}" in
			'' | *[!0-9]*)
				return 1
				;;
			esac
			if [ -n "${l_suite_token:-}" ]; then
				if tracked_runner_process_running_p \
					"$l_suite_pid" "$l_suite_token"; then
					return 0
				fi
			else
				l_suite_owner_pid=$(runner_read_numeric_pid_file \
					"$l_pid_file" 2>/dev/null || true)
				if runner_child_pid_matches_parent \
					"$l_suite_owner_pid" "$l_suite_pid" &&
					process_running_p "$l_suite_pid"; then
					return 0
				fi
			fi
			return 1
		}
		runner_wait_for_suite_child_shutdown() {
			l_remaining=$RUNNER_SIGNAL_SHUTDOWN_GRACE_SECONDS
			while [ "$l_remaining" -gt 0 ]; do
				if ! runner_suite_child_running_p; then
					return 0
				fi
				sleep 1 || true
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
				runner_wait_for_suite_child_shutdown || :
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
		set -- "$l_suite_path"
		if [ -n "$l_selected_test_names" ]; then
			set -- "$@" --
		fi
		while IFS= read -r l_test_name; do
			[ -n "$l_test_name" ] || continue
			set -- "$@" "$l_test_name"
		done <<EOF
$l_selected_test_names
EOF
		if [ -n "${TEST_SHELL_RUNNER:-}" ]; then
			"$TEST_SHELL_RUNNER" "$@" >"$l_log_file" 2>&1 &
		else
			"$@" >"$l_log_file" 2>&1 &
		fi
		l_suite_pid=$!
		printf '%s\n' "$l_suite_pid" >"$l_child_pid_file" 2>/dev/null || :
		if l_suite_token=$(runner_capture_child_identity_from_parent_file \
			"$l_pid_file" "$l_suite_pid" 2>/dev/null); then
			:
		else
			l_suite_token=
		fi
		if [ -n "$l_suite_token" ]; then
			printf '%s\n' "$l_suite_token" >"$l_child_token_file" 2>/dev/null || :
		fi
		l_launching_suite_child=0
		runner_consume_deferred_signal
		while runner_suite_child_running_p; do
			# Poll instead of blocking in wait: supported /bin/sh variants may
			# defer a pending trap until the waited-on child exits.
			sleep 1 || true
		done
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
	printf '%s\n' "$l_pid" >"$l_child_owner_pid_file"
	printf '%s\n' "$l_pid" >"$l_pid_file"
	if l_token=$(runner_capture_child_identity_with_retry \
		"$$" "$l_pid" 2>/dev/null); then
		:
	else
		l_token=
	fi
	if [ -n "$l_token" ]; then
		printf '%s\n' "$l_token" >"$l_token_file"
	fi
	if [ -n "$RUNNER_PENDING_WORKERS" ]; then
		RUNNER_PENDING_WORKERS="$RUNNER_PENDING_WORKERS $l_worker_id"
	else
		RUNNER_PENDING_WORKERS=$l_worker_id
	fi
	RUNNER_INFLIGHT_COUNT=$((RUNNER_INFLIGHT_COUNT + 1))
	RUNNER_NEXT_WORKER_ID=$((RUNNER_NEXT_WORKER_ID + 1))
	RUNNER_DEFER_SIGNALS=0
	consume_deferred_runner_signal
}

replay_suite_worker() {
	l_worker_id=$1
	l_path_file="$RUNNER_STATE_DIR/$l_worker_id.path"
	l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
	l_owner_pid_file="$RUNNER_STATE_DIR/$l_worker_id.owner.pid"
	l_token_file="$RUNNER_STATE_DIR/$l_worker_id.token"
	l_child_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
	l_child_owner_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.owner.pid"
	l_child_token_file="$RUNNER_STATE_DIR/$l_worker_id.child.token"
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

	rm -f "$l_path_file" "$l_pid_file" "$l_owner_pid_file" \
		"$l_token_file" "$l_child_pid_file" \
		"$l_child_owner_pid_file" "$l_child_token_file" \
		"$l_log_file" "$l_status_file" "$l_ready_file"
}

wait_for_next_worker_completion() {
	[ "$RUNNER_INFLIGHT_COUNT" -gt 0 ] || return 0

	while [ "$RUNNER_INFLIGHT_COUNT" -gt 0 ]; do
		for l_worker_id in $RUNNER_PENDING_WORKERS; do
			l_path_file="$RUNNER_STATE_DIR/$l_worker_id.path"
			l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
			l_token_file="$RUNNER_STATE_DIR/$l_worker_id.token"
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
			l_token=$(read_runner_process_token "$l_token_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*)
				continue
				;;
			esac
			if ! tracked_runner_process_running_p "$l_pid" "$l_token" 1; then
				: >"$l_ready_file"
				RUNNER_INFLIGHT_COUNT=$((RUNNER_INFLIGHT_COUNT - 1))
				return 0
			fi
		done
		sleep 1 || true
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
		l_pending_worker_child_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
		l_pending_worker_child_token_file="$RUNNER_STATE_DIR/$l_worker_id.child.token"
		l_pending_worker_child_pid=
		l_pending_worker_child_token=
		l_pending_worker_child_unverified=0
		if [ -r "$l_pending_worker_child_pid_file" ]; then
			l_pending_worker_child_pid=$(cat \
				"$l_pending_worker_child_pid_file" 2>/dev/null || true)
			l_pending_worker_child_token=$(read_runner_process_token \
				"$l_pending_worker_child_token_file" 2>/dev/null || true)
		fi

		# Never terminate a wrapper while its published child lacks a stable
		# identity. The wrapper is the remaining ownership proof and must stay
		# alive until it can reap that child.
		case "$l_pending_worker_child_pid" in
		'' | *[!0-9]*) ;;
		*)
			[ -n "$l_pending_worker_child_token" ] ||
				l_pending_worker_child_unverified=1
			;;
		esac
		if [ "$l_pending_worker_child_unverified" -eq 0 ]; then
			l_pending_worker_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
			l_pending_worker_token_file="$RUNNER_STATE_DIR/$l_worker_id.token"
			if [ -r "$l_pending_worker_pid_file" ]; then
				l_pending_worker_pid=$(cat \
					"$l_pending_worker_pid_file" 2>/dev/null || true)
				l_pending_worker_token=$(read_runner_process_token \
					"$l_pending_worker_token_file" 2>/dev/null || true)
				case "$l_pending_worker_pid" in
				'' | *[!0-9]*) ;;
				*)
					signal_pid_and_descendants \
						"$l_signal" "$l_pending_worker_pid" \
						"$l_pending_worker_token"
					;;
				esac
			fi
		fi

		case "$l_pending_worker_child_pid" in
		'' | *[!0-9]*) ;;
		*)
			signal_pid_and_descendants \
				"$l_signal" "$l_pending_worker_child_pid" \
				"$l_pending_worker_child_token"
			;;
		esac
	done
}

signal_pending_worker_children() {
	l_signal=$1

	for l_worker_id in $RUNNER_PENDING_WORKERS; do
		l_worker_owner_pid_file="$RUNNER_STATE_DIR/$l_worker_id.pid"
		l_pid_file="$RUNNER_STATE_DIR/$l_worker_id.child.pid"
		l_token_file="$RUNNER_STATE_DIR/$l_worker_id.child.token"
		if [ -r "$l_pid_file" ]; then
			l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
			l_token=$(read_runner_process_token "$l_token_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*) ;;
			*)
				if [ -n "$l_token" ]; then
					signal_pid_and_descendants \
						"$l_signal" "$l_pid" "$l_token"
				else
					l_worker_owner_pid=$(runner_read_numeric_pid_file \
						"$l_worker_owner_pid_file" 2>/dev/null || true)
					signal_owned_child_and_descendants \
						"$l_signal" "$$" \
						"$l_worker_owner_pid" "$l_pid"
				fi
				;;
			esac
		fi
	done
}

pending_worker_pids_running_p() {
	for l_worker_id in $RUNNER_PENDING_WORKERS; do
		l_status_file="$RUNNER_STATE_DIR/$l_worker_id.status"
		if [ -r "$l_status_file" ]; then
			continue
		fi
		for l_pid_file in \
			"$RUNNER_STATE_DIR/$l_worker_id.child.pid" \
			"$RUNNER_STATE_DIR/$l_worker_id.pid"; do
			l_allow_unverified=0
			[ -r "$l_pid_file" ] || continue
			l_pid=$(cat "$l_pid_file" 2>/dev/null || true)
			l_token_file=${l_pid_file%.pid}.token
			l_token=$(read_runner_process_token "$l_token_file" 2>/dev/null || true)
			case "$l_pid" in
			'' | *[!0-9]*)
				continue
				;;
			esac
			if [ "$l_pid_file" = "$RUNNER_STATE_DIR/$l_worker_id.pid" ]; then
				l_allow_unverified=1
			fi
			if tracked_runner_process_running_p \
				"$l_pid" "$l_token" "$l_allow_unverified"; then
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
		sleep 1 || true
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
		return 0
	fi

	RUNNER_SHUTTING_DOWN=1
	# Once teardown starts, a second catchable signal must not kill the owner
	# shells and orphan an unverified suite. Operators can still use SIGKILL if
	# the platform cannot provide either tokens or parent/child enumeration.
	trap '' HUP INT TERM
	# Keep wrapper shells alive while payload suites are terminated so they can
	# reap children before the runner exits. This avoids persistent suite PIDs
	# on illumos/OmniOS when a suite ignores TERM.
	signal_foreground_suite_child TERM
	signal_pending_worker_children TERM
	if ! wait_for_runner_tracked_shutdown; then
		signal_foreground_suite_child KILL
		signal_pending_worker_children KILL
		if ! wait_for_runner_tracked_shutdown; then
			signal_foreground_suite TERM
			signal_pending_workers TERM
			if ! wait_for_runner_tracked_shutdown; then
				signal_foreground_suite KILL
				signal_pending_workers KILL
				wait_for_runner_tracked_shutdown || :
			fi
		fi
	fi
	wait_for_runner_tracked_processes
	cleanup_runner_state
	exit "$l_status"
}

if [ "${ZXFER_RUN_SHUNIT_SOURCE_ONLY:-0}" = "1" ]; then
	# shellcheck disable=SC2317  # exit is the direct-execution fallback.
	return 0 2>/dev/null || exit 0
fi

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
fi

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

resolve_test_shell_runner

validate_named_test_selections "$@" || exit 1

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
	suite_test_names=$(selected_test_names_for_suite "$suite_path")

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
		run_suite_foreground "$suite_path" "$suite_test_names"
		continue
	fi

	launch_suite_worker "$suite_path" "$suite_test_names"
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
