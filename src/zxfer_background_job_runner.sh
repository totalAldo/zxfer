#!/bin/sh
# shellcheck shell=sh

ZXFER_BACKGROUND_JOB_METADATA_VERSION=1

zxfer_background_job_runner_atomic_write() {
	l_target_path=$1
	l_payload=$2
	l_stage_path=$l_target_path.stage.$$

	umask 077
	if ! printf '%s' "$l_payload" >"$l_stage_path" 2>/dev/null; then
		rm -f "$l_stage_path" 2>/dev/null || :
		return 1
	fi
	chmod 600 "$l_stage_path" 2>/dev/null || :
	if ! mv -f "$l_stage_path" "$l_target_path" 2>/dev/null; then
		rm -f "$l_stage_path" 2>/dev/null || :
		return 1
	fi
	chmod 600 "$l_target_path" 2>/dev/null || :
	return 0
}

zxfer_background_job_runner_write_launch() {
	l_control_dir=$1
	l_job_id=$2
	l_kind=$3
	l_runner_pid=$4
	l_runner_script=$5
	l_runner_token=$6
	l_worker_pid=$7
	l_worker_pgid=$8
	l_teardown_mode=$9
	l_started_epoch=${10:-}
	l_launch_path=$l_control_dir/launch.tsv

	l_payload="version	$ZXFER_BACKGROUND_JOB_METADATA_VERSION
job_id	$l_job_id
kind	$l_kind
runner_pid	$l_runner_pid
runner_script	$l_runner_script
runner_token	$l_runner_token
worker_pid	$l_worker_pid
worker_pgid	$l_worker_pgid
teardown_mode	$l_teardown_mode
started_epoch	$l_started_epoch"

	zxfer_background_job_runner_atomic_write "$l_launch_path" "$l_payload"
}

zxfer_background_job_runner_write_completion() {
	l_control_dir=$1
	l_status=$2
	l_report_failure=${3:-}
	l_completion_path=$l_control_dir/completion.tsv

	l_payload="version	$ZXFER_BACKGROUND_JOB_METADATA_VERSION
status	$l_status
report_failure	$l_report_failure
completed_epoch	$(date +%s 2>/dev/null || :)"

	zxfer_background_job_runner_atomic_write "$l_completion_path" "$l_payload"
}

zxfer_background_job_runner_get_pgid() {
	l_pid=$1
	ps -o pgid= -p "$l_pid" 2>/dev/null | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed -n '1p'
}

zxfer_background_job_runner_read_process_snapshot() {
	ps -o pid= -o ppid= -o pgid= 2>/dev/null
}

zxfer_background_job_runner_get_pid_set() {
	l_snapshot=$1
	l_root_pid=$2

	case "$l_root_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | awk -v root="$l_root_pid" '
	{
		pid = $1
		ppid = $2
		if (pid == "" || ppid == "") {
			next
		}
		seen[pid] = 1
		parent[pid] = ppid
	}
	END {
		target[root] = 1
		changed = 1
		while (changed) {
			changed = 0
			for (pid in seen) {
				if ((parent[pid] in target) && !(pid in target)) {
					target[pid] = 1
					changed = 1
				}
			}
		}
		for (pid in target) {
			print pid
		}
	}' | LC_ALL=C sort -n
}

zxfer_background_job_runner_snapshot_has_pid() {
	l_snapshot=$1
	l_pid=$2

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | awk -v want_pid="$l_pid" '
	$1 == want_pid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_background_job_runner_snapshot_has_pid_with_parent() {
	l_snapshot=$1
	l_pid=$2
	l_parent_pid=$3

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_parent_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | awk -v want_pid="$l_pid" -v want_parent_pid="$l_parent_pid" '
	$1 == want_pid && $2 == want_parent_pid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_background_job_runner_snapshot_has_pid_with_parent_and_pgid() {
	l_snapshot=$1
	l_pid=$2
	l_parent_pid=$3
	l_pgid=$4

	case "$l_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_parent_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_pgid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_snapshot" | awk -v want_pid="$l_pid" -v want_parent_pid="$l_parent_pid" -v want_pgid="$l_pgid" '
	$1 == want_pid && $2 == want_parent_pid && $3 == want_pgid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_background_job_runner_signal_pid_set() {
	l_pid_set=$1
	l_signal=$2
	l_status=0

	while IFS= read -r l_pid || [ -n "$l_pid" ]; do
		[ -n "$l_pid" ] || continue
		kill "-$l_signal" "$l_pid" 2>/dev/null || l_status=1
	done <<-EOF
		$l_pid_set
	EOF

	return "$l_status"
}

zxfer_background_job_runner_signal_process_group() {
	l_pgid=$1
	l_signal=$2

	case "$l_pgid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	kill "-$l_signal" "-$l_pgid" 2>/dev/null
}

zxfer_background_job_runner_abort_worker_scope() {
	l_worker_pid=$1
	l_worker_pgid=$2
	l_teardown_mode=$3
	l_signal=${4:-TERM}

	case "$l_worker_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	if ! l_snapshot=$(zxfer_background_job_runner_read_process_snapshot 2>/dev/null); then
		wait "$l_worker_pid" 2>/dev/null || :
		return 1
	fi
	if ! zxfer_background_job_runner_snapshot_has_pid_with_parent "$l_snapshot" "$l_worker_pid" "$$" >/dev/null 2>&1; then
		wait "$l_worker_pid" 2>/dev/null || :
		return 1
	fi
	if [ "$l_teardown_mode" = "process_group" ] &&
		zxfer_background_job_runner_snapshot_has_pid_with_parent_and_pgid "$l_snapshot" "$l_worker_pid" "$$" "$l_worker_pgid" >/dev/null 2>&1 &&
		zxfer_background_job_runner_signal_process_group "$l_worker_pgid" "$l_signal"; then
		wait "$l_worker_pid" 2>/dev/null || :
		return 0
	fi
	if ! l_pid_set=$(zxfer_background_job_runner_get_pid_set "$l_snapshot" "$l_worker_pid" 2>/dev/null); then
		wait "$l_worker_pid" 2>/dev/null || :
		return 1
	fi
	if [ -z "$l_pid_set" ]; then
		wait "$l_worker_pid" 2>/dev/null || :
		return 1
	fi
	if ! zxfer_background_job_runner_signal_pid_set "$l_pid_set" "$l_signal" 2>/dev/null; then
		wait "$l_worker_pid" 2>/dev/null || :
		return 1
	fi

	wait "$l_worker_pid" 2>/dev/null || :
	return 0
}

zxfer_background_job_runner_notify_completion() {
	l_job_id=$1
	l_notify_fd=${ZXFER_BACKGROUND_JOB_NOTIFY_FD:-}

	case "$l_notify_fd" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	eval "printf '%s\n' \"\$l_job_id\" >&$l_notify_fd" 2>/dev/null
}

zxfer_background_job_runner_notify_completion_write_failure() {
	l_job_id=$1
	l_status=$2
	l_notify_fd=${ZXFER_BACKGROUND_JOB_NOTIFY_FD:-}

	case "$l_notify_fd" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	eval "printf 'completion_write_failed\t%s\t%s\n' \"\$l_job_id\" \"\$l_status\" >&$l_notify_fd" 2>/dev/null || :
}

zxfer_background_job_runner_main() {
	l_runner_token=$1
	l_job_id=$2
	l_kind=$3
	l_control_dir=$4
	l_exec_cmd=$5
	# shellcheck disable=SC2034
	l_display_cmd=$6
	l_output_file=$7
	l_error_file=$8
	l_runner_pid=$$
	l_runner_script=$0
	l_started_epoch=$(date +%s 2>/dev/null || :)
	l_teardown_mode=child_set
	l_wait_status=0
	l_worker_pid=""
	l_worker_pgid=""

	if command -v setsid >/dev/null 2>&1; then
		l_use_setsid=1
	else
		l_use_setsid=0
	fi

	if [ "$l_use_setsid" -eq 1 ]; then
		if [ -n "$l_output_file" ] && [ -n "$l_error_file" ]; then
			setsid sh -c "$l_exec_cmd" >"$l_output_file" 2>"$l_error_file" &
		elif [ -n "$l_output_file" ]; then
			setsid sh -c "$l_exec_cmd" >"$l_output_file" &
		else
			setsid sh -c "$l_exec_cmd" &
		fi
	else
		if [ -n "$l_output_file" ] && [ -n "$l_error_file" ]; then
			sh -c "$l_exec_cmd" >"$l_output_file" 2>"$l_error_file" &
		elif [ -n "$l_output_file" ]; then
			sh -c "$l_exec_cmd" >"$l_output_file" &
		else
			sh -c "$l_exec_cmd" &
		fi
	fi
	l_worker_pid=$!
	l_worker_pgid=$(zxfer_background_job_runner_get_pgid "$l_worker_pid")

	case "$l_worker_pgid" in
	'' | *[!0-9]*)
		l_teardown_mode=child_set
		;;
	*)
		l_runner_pgid=$(zxfer_background_job_runner_get_pgid "$l_runner_pid")
		if [ "$l_use_setsid" -eq 1 ] && [ "$l_worker_pgid" != "${l_runner_pgid:-}" ]; then
			l_teardown_mode=process_group
		fi
		;;
	esac

	if ! zxfer_background_job_runner_write_launch \
		"$l_control_dir" \
		"$l_job_id" \
		"$l_kind" \
		"$l_runner_pid" \
		"$l_runner_script" \
		"$l_runner_token" \
		"$l_worker_pid" \
		"$l_worker_pgid" \
		"$l_teardown_mode" \
		"$l_started_epoch"; then
		printf '%s\n' "Failed to record launch metadata for background job [$l_job_id]." >&2
		if ! zxfer_background_job_runner_abort_worker_scope \
			"$l_worker_pid" \
			"$l_worker_pgid" \
			"$l_teardown_mode" \
			"TERM"; then
			printf '%s\n' "Failed to tear down background job [$l_job_id] worker scope after launch metadata failure." >&2
		fi
		return 125
	fi

	wait "$l_worker_pid" || l_wait_status=$?

	if ! zxfer_background_job_runner_write_completion "$l_control_dir" "$l_wait_status"; then
		printf '%s\n' "Failed to record background job completion in [$l_control_dir/completion.tsv]." >&2
		zxfer_background_job_runner_notify_completion_write_failure "$l_job_id" "$l_wait_status"
		return 125
	fi

	if ! zxfer_background_job_runner_notify_completion "$l_job_id"; then
		if ! zxfer_background_job_runner_write_completion "$l_control_dir" "$l_wait_status" "queue_write"; then
			rm -f "$l_control_dir/completion.tsv" 2>/dev/null || :
			printf '%s\n' "Failed to record background job queue publication failure in [$l_control_dir/completion.tsv]." >&2
		fi
		printf '%s\n' "Failed to publish background job completion for [$l_job_id]." >&2
		return 125
	fi

	return "$l_wait_status"
}

if [ "${ZXFER_BACKGROUND_JOB_RUNNER_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_background_job_runner_main "$@"
	exit $?
fi
