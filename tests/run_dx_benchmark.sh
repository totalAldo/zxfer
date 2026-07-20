#!/bin/sh
#
# Report-only wall-time evidence for the contributor validation workflow.
# This runner never applies timing thresholds. It invokes only the validation
# commands selected by its fixed case dispatcher.

ZXFER_DX_BENCHMARK_ROOT=${ZXFER_DX_BENCHMARK_ROOT:-$(cd "$(dirname "$0")/.." && pwd -P)}
ZXFER_DX_BENCHMARK_SCRIPT="$ZXFER_DX_BENCHMARK_ROOT/tests/run_dx_benchmark.sh"
ZXFER_DX_BENCHMARK_TIME=${ZXFER_DX_BENCHMARK_TIME:-/usr/bin/time}
ZXFER_DX_BENCHMARK_OUTPUT_DIR=
ZXFER_DX_BENCHMARK_CASES=
ZXFER_DX_BENCHMARK_SAMPLES=1
ZXFER_DX_BENCHMARK_NAMED_WARMUPS=1
ZXFER_DX_BENCHMARK_NAMED_SUITE=tests/test_zxfer_dependencies.sh
ZXFER_DX_BENCHMARK_NAMED_TEST=test_zxfer_compute_secure_path_preserves_unset_ifs_and_disabled_globbing
ZXFER_DX_BENCHMARK_QUICK_PATH=tests/validate.sh
ZXFER_DX_BENCHMARK_NAMED_RUNNER="$ZXFER_DX_BENCHMARK_ROOT/tests/run_shunit_tests.sh"
ZXFER_DX_BENCHMARK_QUICK_RUNNER="$ZXFER_DX_BENCHMARK_ROOT/tests/validate.sh"
ZXFER_DX_BENCHMARK_SHUNIT_RUNNER="$ZXFER_DX_BENCHMARK_ROOT/tests/run_shunit_tests.sh"
ZXFER_DX_BENCHMARK_VALIDATE_RUNNER="$ZXFER_DX_BENCHMARK_ROOT/tests/validate.sh"
ZXFER_DX_BENCHMARK_RESULTS_FILE=
ZXFER_DX_BENCHMARK_ACTIVE_PID=
ZXFER_DX_BENCHMARK_ACTIVE_PGID=
ZXFER_DX_BENCHMARK_DEFER_SIGNALS=0
ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL=
ZXFER_DX_BENCHMARK_OVERALL_STATUS=0

zxfer_dx_benchmark_usage() {
	cat <<'EOF'
usage: ./tests/run_dx_benchmark.sh --output-dir DIR [options]

Record report-only wall times for contributor workflow cases. By default the
runner measures all four cases once, with one summary-excluded named-test
warmup.

  --output-dir DIR     new artifact directory (required)
  --case CASES         comma-delimited case selection; repeatable
  --samples N          timed samples per selected case (default: 1)
  --named-warmups N    summary-excluded named-test warmups (default: 1)
  --named-suite PATH   named-test suite argument
  --named-test NAME    named shunit2 test argument
  --quick-path PATH    representative quick-validation path argument
  --runner CASE PATH   override one executable runner without shell parsing
  --list-cases         list supported cases and exit
  -h, --help           show this help

Cases:
  named    one warmed named shunit2 test
  quick    representative validate.sh quick path
  shunit   complete shunit2 suite
  validate complete validate.sh full profile

The runner records command failures but never compares elapsed time with a
threshold. Runner overrides must be single executable paths; arguments remain
owned by the fixed case dispatcher and are never evaluated as shell text.
summary.tsv reports medians and nearest-rank P95 values without enforcing them.
EOF
}

zxfer_dx_benchmark_list_cases() {
	printf '%s\n' named quick shunit validate
}

zxfer_dx_benchmark_error() {
	printf 'ERROR: %s\n' "$*" >&2
	return 1
}

zxfer_dx_benchmark_uint_p() {
	case ${1:-} in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ "$1" -ge "${2:-0}" ]
}

zxfer_dx_benchmark_case_p() {
	case ${1:-} in
	named | quick | shunit | validate) return 0 ;;
	esac
	return 1
}

zxfer_dx_benchmark_case_selected_p() {
	l_dx_selected_want=$1
	for l_dx_selected_case in $ZXFER_DX_BENCHMARK_CASES; do
		[ "$l_dx_selected_case" = "$l_dx_selected_want" ] && return 0
	done
	return 1
}

zxfer_dx_benchmark_append_case() {
	l_dx_append_case=$1
	zxfer_dx_benchmark_case_p "$l_dx_append_case" || {
		zxfer_dx_benchmark_error "Unknown workflow timing case: $l_dx_append_case" || :
		return 1
	}
	zxfer_dx_benchmark_case_selected_p "$l_dx_append_case" && return 0
	if [ -n "$ZXFER_DX_BENCHMARK_CASES" ]; then
		ZXFER_DX_BENCHMARK_CASES="$ZXFER_DX_BENCHMARK_CASES $l_dx_append_case"
	else
		ZXFER_DX_BENCHMARK_CASES=$l_dx_append_case
	fi
}

zxfer_dx_benchmark_append_case_spec() {
	l_dx_case_spec=$1
	case "$l_dx_case_spec" in
	'' | ,* | *, | *,,*)
		zxfer_dx_benchmark_error "--case contains an empty case name" || :
		return 1
		;;
	esac
	l_dx_case_spec_values=$(printf '%s\n' "$l_dx_case_spec" | tr ',' '\n')
	while IFS= read -r l_dx_case_value; do
		[ -n "$l_dx_case_value" ] || {
			zxfer_dx_benchmark_error "--case contains an empty case name" || :
			return 1
		}
		zxfer_dx_benchmark_append_case "$l_dx_case_value" || return "$?"
	done <<EOF
$l_dx_case_spec_values
EOF
}

zxfer_dx_benchmark_set_runner() {
	l_dx_runner_case=$1
	l_dx_runner_path=$2
	zxfer_dx_benchmark_case_p "$l_dx_runner_case" || {
		zxfer_dx_benchmark_error "--runner names an unknown case: $l_dx_runner_case" || :
		return 1
	}
	case "$l_dx_runner_case" in
	named) ZXFER_DX_BENCHMARK_NAMED_RUNNER=$l_dx_runner_path ;;
	quick) ZXFER_DX_BENCHMARK_QUICK_RUNNER=$l_dx_runner_path ;;
	shunit) ZXFER_DX_BENCHMARK_SHUNIT_RUNNER=$l_dx_runner_path ;;
	validate) ZXFER_DX_BENCHMARK_VALIDATE_RUNNER=$l_dx_runner_path ;;
	esac
}

zxfer_dx_benchmark_parse_args() {
	while [ "$#" -gt 0 ]; do
		case "$1" in
		--output-dir | --case | --samples | --named-warmups | --named-suite | --named-test | --quick-path)
			l_dx_option=$1
			shift
			[ "$#" -gt 0 ] || {
				zxfer_dx_benchmark_error "$l_dx_option requires a value" || :
				return 1
			}
			case "$l_dx_option" in
			--output-dir) ZXFER_DX_BENCHMARK_OUTPUT_DIR=$1 ;;
			--case) zxfer_dx_benchmark_append_case_spec "$1" || return "$?" ;;
			--samples) ZXFER_DX_BENCHMARK_SAMPLES=$1 ;;
			--named-warmups) ZXFER_DX_BENCHMARK_NAMED_WARMUPS=$1 ;;
			--named-suite) ZXFER_DX_BENCHMARK_NAMED_SUITE=$1 ;;
			--named-test) ZXFER_DX_BENCHMARK_NAMED_TEST=$1 ;;
			--quick-path) ZXFER_DX_BENCHMARK_QUICK_PATH=$1 ;;
			esac
			;;
		--runner)
			shift
			[ "$#" -ge 2 ] || {
				zxfer_dx_benchmark_error "--runner requires CASE and PATH" || :
				return 1
			}
			zxfer_dx_benchmark_set_runner "$1" "$2" || return "$?"
			shift
			;;
		--list-cases)
			zxfer_dx_benchmark_list_cases
			return 2
			;;
		-h | --help)
			zxfer_dx_benchmark_usage
			return 2
			;;
		*)
			zxfer_dx_benchmark_error "Unknown argument: $1" || :
			return 1
			;;
		esac
		shift
	done
}

zxfer_dx_benchmark_safe_field_p() {
	l_dx_safe_field_cr=$(printf '\r')
	case ${1:-} in
	'' | *'	'* | *'
'* | *"$l_dx_safe_field_cr"*) return 1 ;;
	esac
	return 0
}

zxfer_dx_benchmark_validate_runner() {
	l_dx_validate_runner_case=$1
	l_dx_validate_runner_path=$2
	zxfer_dx_benchmark_safe_field_p "$l_dx_validate_runner_path" || {
		zxfer_dx_benchmark_error "Runner path for $l_dx_validate_runner_case contains a tab, carriage return, or newline" || :
		return 1
	}
	case "$l_dx_validate_runner_path" in
	*/*) ;;
	*)
		zxfer_dx_benchmark_error "Runner override for $l_dx_validate_runner_case must be an executable path" || :
		return 1
		;;
	esac
	[ -x "$l_dx_validate_runner_path" ] || {
		zxfer_dx_benchmark_error "Runner for $l_dx_validate_runner_case is not executable: $l_dx_validate_runner_path" || :
		return 1
	}
}

zxfer_dx_benchmark_validate_args() {
	zxfer_dx_benchmark_safe_field_p "$ZXFER_DX_BENCHMARK_TIME" || {
		zxfer_dx_benchmark_error "Time utility path contains a tab, carriage return, or newline" || :
		return 1
	}
	case "$ZXFER_DX_BENCHMARK_TIME" in
	*/*) ;;
	*)
		zxfer_dx_benchmark_error "Time utility must be an executable path: $ZXFER_DX_BENCHMARK_TIME" || :
		return 1
		;;
	esac
	[ -x "$ZXFER_DX_BENCHMARK_TIME" ] || {
		zxfer_dx_benchmark_error "POSIX time utility is not executable: $ZXFER_DX_BENCHMARK_TIME" || :
		return 1
	}
	[ -n "$ZXFER_DX_BENCHMARK_OUTPUT_DIR" ] || {
		zxfer_dx_benchmark_error "--output-dir is required" || :
		return 1
	}
	zxfer_dx_benchmark_uint_p "$ZXFER_DX_BENCHMARK_SAMPLES" 1 || {
		zxfer_dx_benchmark_error "--samples must be a positive integer" || :
		return 1
	}
	zxfer_dx_benchmark_uint_p "$ZXFER_DX_BENCHMARK_NAMED_WARMUPS" 0 || {
		zxfer_dx_benchmark_error "--named-warmups must be a non-negative integer" || :
		return 1
	}
	zxfer_dx_benchmark_safe_field_p "$ZXFER_DX_BENCHMARK_OUTPUT_DIR" || {
		zxfer_dx_benchmark_error "Output paths may not contain tabs, carriage returns, or newlines" || :
		return 1
	}
	case "$ZXFER_DX_BENCHMARK_OUTPUT_DIR" in
	-*) zxfer_dx_benchmark_error "Relative output paths may not begin with '-'" || return 1 ;;
	esac
	l_dx_output_parent=$(dirname "$ZXFER_DX_BENCHMARK_OUTPUT_DIR") || return "$?"
	[ -d "$l_dx_output_parent" ] || {
		zxfer_dx_benchmark_error "Output directory parent does not exist: $l_dx_output_parent" || :
		return 1
	}
	[ ! -e "$ZXFER_DX_BENCHMARK_OUTPUT_DIR" ] || {
		zxfer_dx_benchmark_error "Output directory already exists: $ZXFER_DX_BENCHMARK_OUTPUT_DIR" || :
		return 1
	}
	[ -n "$ZXFER_DX_BENCHMARK_CASES" ] || ZXFER_DX_BENCHMARK_CASES="named quick shunit validate"
	zxfer_dx_benchmark_safe_field_p "$ZXFER_DX_BENCHMARK_NAMED_SUITE" ||
		zxfer_dx_benchmark_error "Named suite may not contain tabs, carriage returns, or newlines" || return 1
	zxfer_dx_benchmark_safe_field_p "$ZXFER_DX_BENCHMARK_QUICK_PATH" ||
		zxfer_dx_benchmark_error "Quick path may not contain tabs, carriage returns, or newlines" || return 1
	case "$ZXFER_DX_BENCHMARK_NAMED_TEST" in
	'' | [!A-Za-z_]* | *[!A-Za-z0-9_]*)
		zxfer_dx_benchmark_error "Named test must be a shell function name: $ZXFER_DX_BENCHMARK_NAMED_TEST" || return 1
		;;
	esac
	for l_dx_validate_case in $ZXFER_DX_BENCHMARK_CASES; do
		case "$l_dx_validate_case" in
		named) l_dx_validate_runner=$ZXFER_DX_BENCHMARK_NAMED_RUNNER ;;
		quick) l_dx_validate_runner=$ZXFER_DX_BENCHMARK_QUICK_RUNNER ;;
		shunit) l_dx_validate_runner=$ZXFER_DX_BENCHMARK_SHUNIT_RUNNER ;;
		validate) l_dx_validate_runner=$ZXFER_DX_BENCHMARK_VALIDATE_RUNNER ;;
		esac
		zxfer_dx_benchmark_validate_runner "$l_dx_validate_case" "$l_dx_validate_runner" || return "$?"
	done
}

zxfer_dx_benchmark_create_output_dir() {
	l_dx_create_status=0
	(
		umask 077
		mkdir "$ZXFER_DX_BENCHMARK_OUTPUT_DIR"
	) || l_dx_create_status=$?
	if [ "$l_dx_create_status" -ne 0 ]; then
		zxfer_dx_benchmark_error "Unable to create new output directory: $ZXFER_DX_BENCHMARK_OUTPUT_DIR" || :
		return "$l_dx_create_status"
	fi
	l_dx_physical_output=$(cd "$ZXFER_DX_BENCHMARK_OUTPUT_DIR" && pwd -P) || return "$?"
	ZXFER_DX_BENCHMARK_OUTPUT_DIR=$l_dx_physical_output
	ZXFER_DX_BENCHMARK_RESULTS_FILE="$ZXFER_DX_BENCHMARK_OUTPUT_DIR/results.tsv"
	mkdir "$ZXFER_DX_BENCHMARK_OUTPUT_DIR/logs" || return "$?"
}

zxfer_dx_benchmark_load_supervisor_record() {
	l_dx_supervisor_path=$1
	[ -f "$l_dx_supervisor_path" ] && [ -r "$l_dx_supervisor_path" ] || return 1
	l_dx_supervisor_record=$(awk -F '	' '
		NR == 1 && NF == 2 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ &&
			$1 == $2 {
			record = $0
			next
		}
		{ invalid = 1 }
		END {
			if (!invalid && NR == 1 && record != "") print record
			else exit 1
		}
	' "$l_dx_supervisor_path") || return 1
	printf '%s\n' "$l_dx_supervisor_record"
}

zxfer_dx_benchmark_load_status_record() {
	l_dx_status_path=$1
	[ -f "$l_dx_status_path" ] && [ -r "$l_dx_status_path" ] || return 1
	l_dx_status_value=$(awk '
		NR == 1 && /^[0-9]+$/ && $1 >= 0 && $1 <= 255 { value = $1; next }
		{ invalid = 1 }
		END {
			if (!invalid && NR == 1 && value != "") print value
			else exit 1
		}
	' "$l_dx_status_path") || return 1
	printf '%s\n' "$l_dx_status_value"
}

zxfer_dx_benchmark_get_process_group() {
	l_dx_group_pid=$1
	case "$l_dx_group_pid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	for l_dx_group_selector in pgid= pgid; do
		l_dx_group_value=$(LC_ALL=C ps -o "$l_dx_group_selector" \
			-p "$l_dx_group_pid" 2>/dev/null |
			awk '$1 ~ /^[0-9]+$/ { print $1; exit }') || continue
		[ -n "$l_dx_group_value" ] || continue
		printf '%s\n' "$l_dx_group_value"
		return 0
	done
	return 1
}

zxfer_dx_benchmark_job_control_supported_p() {
	(
		set -m 2>/dev/null || exit 1
		case $- in
		*m*) exit 0 ;;
		esac
		exit 1
	) 2>/dev/null
}

zxfer_dx_benchmark_resolve_setsid() {
	for l_dx_setsid_path in /usr/bin/setsid /bin/setsid; do
		[ -x "$l_dx_setsid_path" ] || continue
		printf '%s\n' "$l_dx_setsid_path"
		return 0
	done
	return 1
}

zxfer_dx_benchmark_launch_supervisor() {
	g_zxfer_dx_benchmark_launch_pid=
	if zxfer_dx_benchmark_job_control_supported_p; then
		case $- in
		*m*) l_dx_launch_restore_monitor=0 ;;
		*)
			l_dx_launch_restore_monitor=1
			set -m 2>/dev/null || return 1
			;;
		esac
		"$@" &
		g_zxfer_dx_benchmark_launch_pid=$!
		[ "$l_dx_launch_restore_monitor" -eq 0 ] || set +m
		return 0
	fi
	l_dx_launch_setsid=$(zxfer_dx_benchmark_resolve_setsid) || return 1
	"$l_dx_launch_setsid" "$@" &
	g_zxfer_dx_benchmark_launch_pid=$!
}

zxfer_dx_benchmark_wait_for_supervisor_record() {
	l_dx_wait_supervisor_path=$1
	l_dx_wait_supervisor_pid=$2
	l_dx_wait_supervisor_try=0
	while [ "$l_dx_wait_supervisor_try" -lt 100 ]; do
		l_dx_wait_supervisor_record=$(zxfer_dx_benchmark_load_supervisor_record \
			"$l_dx_wait_supervisor_path" 2>/dev/null) || l_dx_wait_supervisor_record=
		if [ -n "$l_dx_wait_supervisor_record" ]; then
			l_dx_wait_supervisor_record_pid=${l_dx_wait_supervisor_record%%	*}
			[ "$l_dx_wait_supervisor_record_pid" = "$l_dx_wait_supervisor_pid" ] ||
				return 1
			printf '%s\n' "$l_dx_wait_supervisor_record"
			return 0
		fi
		sleep 0.1 2>/dev/null || sleep 1
		l_dx_wait_supervisor_try=$((l_dx_wait_supervisor_try + 1))
	done
	return 1
}

zxfer_dx_benchmark_wait_for_start_permission() {
	l_dx_start_go_path=$1
	l_dx_start_cancel_path=$2
	l_dx_start_try=0
	while [ "$l_dx_start_try" -lt 100 ]; do
		[ -e "$l_dx_start_cancel_path" ] && return 1
		[ -e "$l_dx_start_go_path" ] && return 0
		sleep 0.1 2>/dev/null || sleep 1
		l_dx_start_try=$((l_dx_start_try + 1))
	done
	return 1
}

zxfer_dx_benchmark_wait_for_status_record() {
	l_dx_wait_status_path=$1
	l_dx_wait_status_invalid=0
	while :; do
		l_dx_wait_status_value=$(zxfer_dx_benchmark_load_status_record \
			"$l_dx_wait_status_path" 2>/dev/null) || l_dx_wait_status_value=
		if [ -n "$l_dx_wait_status_value" ]; then
			printf '%s\n' "$l_dx_wait_status_value"
			return 0
		fi
		if [ -e "$l_dx_wait_status_path" ]; then
			l_dx_wait_status_invalid=$((l_dx_wait_status_invalid + 1))
			[ "$l_dx_wait_status_invalid" -lt 5 ] || return 1
		fi
		zxfer_dx_benchmark_active_group_exists_p || return 1
		sleep 0.1 2>/dev/null || sleep 1
	done
}

zxfer_dx_benchmark_active_group_exists_p() {
	[ -n "$ZXFER_DX_BENCHMARK_ACTIVE_PGID" ] || return 1
	kill -s 0 -- "-$ZXFER_DX_BENCHMARK_ACTIVE_PGID" 2>/dev/null
}

zxfer_dx_benchmark_signal_active_group() {
	l_dx_group_signal=$1
	[ -n "$ZXFER_DX_BENCHMARK_ACTIVE_PID" ] || return 0
	[ "$ZXFER_DX_BENCHMARK_ACTIVE_PID" = "$ZXFER_DX_BENCHMARK_ACTIVE_PGID" ] ||
		return 1
	kill -s "$l_dx_group_signal" -- \
		"-$ZXFER_DX_BENCHMARK_ACTIVE_PGID" 2>/dev/null
}

zxfer_dx_benchmark_clear_active() {
	ZXFER_DX_BENCHMARK_ACTIVE_PID=
	ZXFER_DX_BENCHMARK_ACTIVE_PGID=
}

zxfer_dx_benchmark_stop_active() {
	[ -n "$ZXFER_DX_BENCHMARK_ACTIVE_PID" ] || return 0
	# STOP is the ownership boundary: after it succeeds, the verified resident
	# group leader cannot exit and the process-group ID cannot be recycled before
	# the one forceful teardown signal reaches every inherited-group descendant.
	if ! zxfer_dx_benchmark_signal_active_group STOP; then
		zxfer_dx_benchmark_active_group_exists_p && return 1
		wait "$ZXFER_DX_BENCHMARK_ACTIVE_PID" 2>/dev/null || :
		zxfer_dx_benchmark_clear_active
		return 0
	fi
	zxfer_dx_benchmark_signal_active_group KILL || return 1
	wait "$ZXFER_DX_BENCHMARK_ACTIVE_PID" 2>/dev/null || :
	zxfer_dx_benchmark_clear_active
}

zxfer_dx_benchmark_begin_active_launch() {
	ZXFER_DX_BENCHMARK_DEFER_SIGNALS=1
	ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL=
}

zxfer_dx_benchmark_finish_deferred_signals() {
	ZXFER_DX_BENCHMARK_DEFER_SIGNALS=0
	[ -n "$ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL" ] || return 0
	l_dx_finish_signal=$ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL
	ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL=
	zxfer_dx_benchmark_handle_signal "$l_dx_finish_signal"
}

zxfer_dx_benchmark_publish_active_launch() {
	ZXFER_DX_BENCHMARK_ACTIVE_PID=$1
	ZXFER_DX_BENCHMARK_ACTIVE_PGID=$2
	ZXFER_DX_BENCHMARK_DEFER_SIGNALS=0
	[ -n "$ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL" ] || return 0
	l_dx_publish_signal=$ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL
	ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL=
	zxfer_dx_benchmark_handle_signal "$l_dx_publish_signal"
}

zxfer_dx_benchmark_handle_signal() {
	l_dx_handle_signal=$1
	if [ "$ZXFER_DX_BENCHMARK_DEFER_SIGNALS" -eq 1 ]; then
		[ -n "$ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL" ] ||
			ZXFER_DX_BENCHMARK_DEFERRED_SIGNAL=$l_dx_handle_signal
		return 0
	fi
	# Cleanup is already committed. Ignore repeated terminal signals so they
	# cannot interrupt STOP/KILL/wait and strand a measured process group.
	trap '' INT TERM
	zxfer_dx_benchmark_stop_active || exit 125
	case "$l_dx_handle_signal" in
	INT) exit 130 ;;
	TERM) exit 143 ;;
	esac
}

zxfer_dx_benchmark_exec_command() {
	l_dx_command_stderr=$1
	shift
	[ "$#" -gt 0 ] || return 64
	exec "$@" 2>"$l_dx_command_stderr"
}

zxfer_dx_benchmark_exec_supervisor() {
	l_dx_supervisor_time=$1
	l_dx_supervisor_time_file=$2
	l_dx_supervisor_ready_file=$3
	l_dx_supervisor_go_file=$4
	l_dx_supervisor_cancel_file=$5
	l_dx_supervisor_status_file=$6
	l_dx_supervisor_stderr_file=$7
	shift 7
	[ "$#" -gt 0 ] || return 64
	l_dx_supervisor_group=$(zxfer_dx_benchmark_get_process_group "$$") || return 69
	[ "$l_dx_supervisor_group" = "$$" ] || return 69
	trap '' HUP INT TERM
	printf '%s\t%s\n' "$$" "$l_dx_supervisor_group" \
		>"$l_dx_supervisor_ready_file" || return "$?"
	zxfer_dx_benchmark_wait_for_start_permission \
		"$l_dx_supervisor_go_file" "$l_dx_supervisor_cancel_file" || return 75
	l_dx_supervisor_status=0
	(
		trap - HUP INT TERM
		"$l_dx_supervisor_time" -p \
			"$ZXFER_DX_BENCHMARK_SCRIPT" --exec-command \
			"$l_dx_supervisor_stderr_file" "$@"
	) 2>"$l_dx_supervisor_time_file" || l_dx_supervisor_status=$?
	printf '%s\n' "$l_dx_supervisor_status" >"$l_dx_supervisor_status_file" ||
		return "$?"
	# The parent retires the complete group after consuming the durable status.
	# Remaining resident prevents PID/PGID reuse and contains background helpers
	# left behind by an otherwise completed runner.
	while :; do
		sleep 60
	done
}

zxfer_dx_benchmark_extract_real_time() {
	l_dx_time_file=$1
	awk '
		$1 == "real" && $2 ~ /^[0-9]+([.][0-9]+)?$/ { value = $2 }
		END { if (value != "") print value; else exit 1 }
	' "$l_dx_time_file"
}

zxfer_dx_benchmark_measure_argv() {
	l_dx_measure_case=$1
	l_dx_measure_phase=$2
	l_dx_measure_iteration=$3
	shift 3
	l_dx_measure_stem="$l_dx_measure_case-$l_dx_measure_phase-$l_dx_measure_iteration"
	l_dx_measure_stdout="logs/$l_dx_measure_stem.stdout"
	l_dx_measure_stderr="logs/$l_dx_measure_stem.stderr"
	l_dx_measure_time="logs/$l_dx_measure_stem.time"
	l_dx_measure_supervisor_error="logs/$l_dx_measure_stem.supervisor.stderr"
	l_dx_measure_supervisor="logs/$l_dx_measure_stem.supervisor.group"
	l_dx_measure_go="logs/$l_dx_measure_stem.supervisor.go"
	l_dx_measure_cancel="logs/$l_dx_measure_stem.supervisor.cancel"
	l_dx_measure_status_file="logs/$l_dx_measure_stem.supervisor.status"
	l_dx_measure_status=0
	zxfer_dx_benchmark_begin_active_launch
	if ! zxfer_dx_benchmark_launch_supervisor \
		"$ZXFER_DX_BENCHMARK_SCRIPT" --exec-supervisor \
		"$ZXFER_DX_BENCHMARK_TIME" \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_time" \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_supervisor" \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_go" \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_cancel" \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_status_file" \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_stderr" "$@" \
		>"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_stdout" \
		2>"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_supervisor_error"; then
		zxfer_dx_benchmark_finish_deferred_signals
		zxfer_dx_benchmark_error "No safe private process-group launcher is available" || :
		return 1
	fi
	l_dx_measure_pid=$g_zxfer_dx_benchmark_launch_pid
	l_dx_measure_record=$(zxfer_dx_benchmark_wait_for_supervisor_record \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_supervisor" \
		"$l_dx_measure_pid" 2>/dev/null) || {
		: >"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_cancel" 2>/dev/null || :
		wait "$l_dx_measure_pid" 2>/dev/null || :
		zxfer_dx_benchmark_finish_deferred_signals
		zxfer_dx_benchmark_error "Unable to verify a private supervisor process group" || :
		return 1
	}
	l_dx_measure_pgid=${l_dx_measure_record#*	}
	zxfer_dx_benchmark_publish_active_launch \
		"$l_dx_measure_pid" "$l_dx_measure_pgid"
	: >"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_go" || {
		zxfer_dx_benchmark_stop_active || :
		return 1
	}
	l_dx_measure_status=$(zxfer_dx_benchmark_wait_for_status_record \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_status_file") ||
		l_dx_measure_status=125
	# Defer a signal across normal group retirement so no trap can observe a
	# leader that has exited but whose active state has not yet been cleared.
	zxfer_dx_benchmark_begin_active_launch
	zxfer_dx_benchmark_stop_active || return "$?"
	zxfer_dx_benchmark_finish_deferred_signals
	l_dx_measure_elapsed=$(zxfer_dx_benchmark_extract_real_time \
		"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/$l_dx_measure_time" 2>/dev/null || printf '%s\n' unavailable)
	if [ "$l_dx_measure_elapsed" = unavailable ]; then
		ZXFER_DX_BENCHMARK_OVERALL_STATUS=1
	fi
	[ "$l_dx_measure_status" -eq 0 ] || ZXFER_DX_BENCHMARK_OVERALL_STATUS=1
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$l_dx_measure_case" "$l_dx_measure_phase" "$l_dx_measure_iteration" \
		"$l_dx_measure_elapsed" "$l_dx_measure_status" "$l_dx_measure_stdout" \
		"$l_dx_measure_stderr" "$l_dx_measure_time" >>"$ZXFER_DX_BENCHMARK_RESULTS_FILE" || {
		zxfer_dx_benchmark_error "Unable to append workflow timing evidence: $ZXFER_DX_BENCHMARK_RESULTS_FILE" || :
		return 1
	}
}

zxfer_dx_benchmark_measure_case() {
	l_dx_case=$1
	l_dx_phase=$2
	l_dx_iteration=$3
	printf '==> workflow timing: %s %s %s\n' "$l_dx_case" "$l_dx_phase" "$l_dx_iteration"
	case "$l_dx_case" in
	named)
		zxfer_dx_benchmark_measure_argv "$l_dx_case" "$l_dx_phase" "$l_dx_iteration" \
			"$ZXFER_DX_BENCHMARK_NAMED_RUNNER" --suite "$ZXFER_DX_BENCHMARK_NAMED_SUITE" \
			--test "$ZXFER_DX_BENCHMARK_NAMED_TEST"
		;;
	quick)
		zxfer_dx_benchmark_measure_argv "$l_dx_case" "$l_dx_phase" "$l_dx_iteration" \
			"$ZXFER_DX_BENCHMARK_QUICK_RUNNER" quick "$ZXFER_DX_BENCHMARK_QUICK_PATH"
		;;
	shunit)
		zxfer_dx_benchmark_measure_argv "$l_dx_case" "$l_dx_phase" "$l_dx_iteration" \
			"$ZXFER_DX_BENCHMARK_SHUNIT_RUNNER"
		;;
	validate)
		zxfer_dx_benchmark_measure_argv "$l_dx_case" "$l_dx_phase" "$l_dx_iteration" \
			"$ZXFER_DX_BENCHMARK_VALIDATE_RUNNER" full
		;;
	esac
}

zxfer_dx_benchmark_write_metadata() {
	{
		printf 'key\tvalue\n'
		printf 'report_only\tyes\n'
		printf 'cases\t%s\n' "$ZXFER_DX_BENCHMARK_CASES"
		printf 'samples\t%s\n' "$ZXFER_DX_BENCHMARK_SAMPLES"
		printf 'named_warmups\t%s\n' "$ZXFER_DX_BENCHMARK_NAMED_WARMUPS"
		printf 'named_suite\t%s\n' "$ZXFER_DX_BENCHMARK_NAMED_SUITE"
		printf 'named_test\t%s\n' "$ZXFER_DX_BENCHMARK_NAMED_TEST"
		printf 'quick_path\t%s\n' "$ZXFER_DX_BENCHMARK_QUICK_PATH"
		printf 'time_utility\t%s\n' "$ZXFER_DX_BENCHMARK_TIME"
		printf 'locale\tC\n'
		for l_dx_metadata_case in $ZXFER_DX_BENCHMARK_CASES; do
			case "$l_dx_metadata_case" in
			named) l_dx_metadata_runner=$ZXFER_DX_BENCHMARK_NAMED_RUNNER ;;
			quick) l_dx_metadata_runner=$ZXFER_DX_BENCHMARK_QUICK_RUNNER ;;
			shunit) l_dx_metadata_runner=$ZXFER_DX_BENCHMARK_SHUNIT_RUNNER ;;
			validate) l_dx_metadata_runner=$ZXFER_DX_BENCHMARK_VALIDATE_RUNNER ;;
			esac
			printf 'runner_%s\t%s\n' "$l_dx_metadata_case" "$l_dx_metadata_runner"
		done
	} >"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/metadata.tsv" || {
		zxfer_dx_benchmark_error "Unable to write workflow timing metadata" || :
		return 1
	}
}

zxfer_dx_benchmark_write_summary() {
	awk -F '\t' -v cases="$ZXFER_DX_BENCHMARK_CASES" '
		function median(case_name, count, values, i, j, value) {
			count = measured[case_name]
			for (i = 1; i <= count; i++) values[i] = elapsed[case_name, i]
			for (i = 2; i <= count; i++) {
				value = values[i]
				j = i - 1
				while (j >= 1 && values[j] > value) {
					values[j + 1] = values[j]
					j--
				}
				values[j + 1] = value
			}
			if (count % 2) return values[(count + 1) / 2]
			return (values[count / 2] + values[count / 2 + 1]) / 2
		}
		function nearest_rank_p95(case_name, count, values, i, j, value, rank) {
			count = measured[case_name]
			for (i = 1; i <= count; i++) values[i] = elapsed[case_name, i]
			for (i = 2; i <= count; i++) {
				value = values[i]
				j = i - 1
				while (j >= 1 && values[j] > value) {
					values[j + 1] = values[j]
					j--
				}
				values[j + 1] = value
			}
			rank = int((95 * count + 99) / 100)
			return values[rank]
		}
		BEGIN {
			print "case\tsamples\tsuccessful_samples\tmedian_seconds\tp95_seconds\tcommand_status"
			case_count = split(cases, ordered_cases, " ")
		}
		NR > 1 && $2 == "sample" {
			samples[$1]++
			if ($5 == 0 && $4 ~ /^[0-9]+([.][0-9]+)?$/) {
				elapsed[$1, ++measured[$1]] = $4 + 0
				successful[$1]++
			}
			if ($5 != 0 || $4 == "unavailable") failed[$1] = 1
		}
		END {
			for (case_index = 1; case_index <= case_count; case_index++) {
				case_name = ordered_cases[case_index]
				median_value = measured[case_name] ? sprintf("%.6f", median(case_name)) : "unavailable"
				p95_value = measured[case_name] ? sprintf("%.6f", nearest_rank_p95(case_name)) : "unavailable"
				status = failed[case_name] ? "failed" : "passed"
				printf "%s\t%d\t%d\t%s\t%s\t%s\n", case_name, samples[case_name], successful[case_name], median_value, p95_value, status
			}
		}
	' "$ZXFER_DX_BENCHMARK_RESULTS_FILE" >"$ZXFER_DX_BENCHMARK_OUTPUT_DIR/summary.tsv" || {
		zxfer_dx_benchmark_error "Unable to write workflow timing summary" || :
		return 1
	}
}

zxfer_dx_benchmark_main() {
	zxfer_dx_benchmark_parse_args "$@"
	l_dx_main_parse_status=$?
	[ "$l_dx_main_parse_status" -eq 0 ] || {
		[ "$l_dx_main_parse_status" -eq 2 ] && return 0
		return "$l_dx_main_parse_status"
	}
	zxfer_dx_benchmark_validate_args || return "$?"
	zxfer_dx_benchmark_create_output_dir || return "$?"
	trap 'zxfer_dx_benchmark_handle_signal INT' INT
	trap 'zxfer_dx_benchmark_handle_signal TERM' TERM
	printf 'case\tphase\titeration\telapsed_seconds\texit_status\tstdout_file\tstderr_file\ttime_file\n' \
		>"$ZXFER_DX_BENCHMARK_RESULTS_FILE" || {
		zxfer_dx_benchmark_error "Unable to initialize workflow timing evidence" || :
		return 1
	}
	zxfer_dx_benchmark_write_metadata || return "$?"

	for l_dx_main_case in $ZXFER_DX_BENCHMARK_CASES; do
		if [ "$l_dx_main_case" = named ]; then
			l_dx_main_iteration=1
			while [ "$l_dx_main_iteration" -le "$ZXFER_DX_BENCHMARK_NAMED_WARMUPS" ]; do
				zxfer_dx_benchmark_measure_case named warmup "$l_dx_main_iteration" || return "$?"
				l_dx_main_iteration=$((l_dx_main_iteration + 1))
			done
		fi
		l_dx_main_iteration=1
		while [ "$l_dx_main_iteration" -le "$ZXFER_DX_BENCHMARK_SAMPLES" ]; do
			zxfer_dx_benchmark_measure_case "$l_dx_main_case" sample "$l_dx_main_iteration" || return "$?"
			l_dx_main_iteration=$((l_dx_main_iteration + 1))
		done
	done
	zxfer_dx_benchmark_write_summary || return "$?"
	trap - INT TERM
	cat "$ZXFER_DX_BENCHMARK_OUTPUT_DIR/summary.tsv" || return "$?"
	printf 'Artifacts: %s\n' "$ZXFER_DX_BENCHMARK_OUTPUT_DIR" || return "$?"
	return "$ZXFER_DX_BENCHMARK_OVERALL_STATUS"
}

zxfer_dx_benchmark_set_machine_locale() {
	LC_ALL=C
	export LC_ALL
}

if [ "${ZXFER_RUN_DX_BENCHMARK_SOURCE_ONLY:-0}" != 1 ]; then
	set -u
	zxfer_dx_benchmark_set_machine_locale
	case ${1:-} in
	--exec-command)
		shift
		[ "$#" -ge 2 ] || exit 64
		zxfer_dx_benchmark_exec_command "$@"
		exit "$?"
		;;
	--exec-supervisor)
		shift
		[ "$#" -ge 8 ] || exit 64
		zxfer_dx_benchmark_exec_supervisor "$@"
		exit "$?"
		;;
	esac
	zxfer_dx_benchmark_main "$@"
	exit "$?"
fi
