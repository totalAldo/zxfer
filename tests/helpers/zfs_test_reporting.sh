#!/bin/sh
# Shared reporting and ZFS fixture assertions for integration and performance harnesses.
# shellcheck disable=SC2034  # Color globals are consumed by the sourcing harness.

RED=$(printf '\033[31m')
GREEN=$(printf '\033[32m')
YELLOW=$(printf '\033[33m')
RESET=$(printf '\033[0m')

list_failed_tests_only_enabled() {
	[ "${ZXFER_LIST_FAILED_TESTS_ONLY:-0}" -eq 1 ]
}

log() {
	if list_failed_tests_only_enabled; then
		return
	fi
	printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

log_summary() {
	printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

emit_failed_tests_only_status_line() {
	l_index=$1
	l_total=$2
	l_result=$3
	l_func=${4:-}

	if list_failed_tests_only_enabled; then
		printf '[%s/%s] %s %s\n' "$l_index" "$l_total" "$l_result" "$l_func"
	fi
}

fail() {
	printf '%sERROR:%s %s\n' "$RED" "$RESET" "$*" >&2
	exit 1
}

assert_exists() {
	l_path=$1
	l_msg=$2
	if [ ! -e "$l_path" ]; then
		fail "$l_msg"
	fi
}

assert_dataset_absent() {
	l_dataset=$1

	if zfs list "$l_dataset" >/dev/null 2>&1; then
		fail "Dataset $l_dataset should not exist."
	fi
}

assert_snapshot_exists() {
	l_dataset=$1
	l_snapshot=$2

	wait_for_snapshot_exists "$l_dataset" "$l_snapshot"
}

wait_for_snapshot_exists() {
	l_dataset=$1
	l_snapshot=$2
	l_attempts=${3:-30}

	l_i=0
	while [ "$l_i" -lt "$l_attempts" ]; do
		if zfs list -t snapshot "$l_dataset@$l_snapshot" >/dev/null 2>&1; then
			return
		fi
		sleep 1
		l_i=$((l_i + 1))
	done

	l_snapshot_list=$(zfs list -H -t snapshot -o name -r "$l_dataset" 2>/dev/null || true)
	fail "Expected snapshot $l_dataset@$l_snapshot to exist after $l_attempts attempts. Visible snapshots under $l_dataset: ${l_snapshot_list:-<none>}"
}

wait_for_snapshot_absent() {
	l_dataset=$1
	l_snapshot=$2
	l_attempts=${3:-60}

	l_i=0
	while [ "$l_i" -lt "$l_attempts" ]; do
		if ! zfs list -t snapshot "$l_dataset@$l_snapshot" >/dev/null 2>&1; then
			return
		fi
		sleep 1
		l_i=$((l_i + 1))
	done

	fail "Snapshot $l_dataset@$l_snapshot still present after waiting."
}

get_latest_snapshot_name_for_dataset() {
	l_dataset=$1

	list_exact_snapshot_names_for_dataset "$l_dataset" |
		awk 'NF { last = $0 } END { if (last != "") print last }'
}

list_exact_snapshot_names_for_dataset() {
	l_dataset=$1

	if ! l_snapshot_list=$(zfs list -H -o name -t snapshot -s creation -r "$l_dataset" 2>&1); then
		fail "Failed to list snapshots for $l_dataset: $l_snapshot_list"
	fi

	printf '%s\n' "$l_snapshot_list" |
		awk -v dataset="$l_dataset" 'index($0, dataset "@") == 1 { print $0 }'
}

assert_output_mentions_snapshot_destroy() {
	l_output=$1
	l_dataset=$2
	l_snapshot=$3
	l_target="$l_dataset@$l_snapshot"

	if ! printf '%s\n' "$l_output" | grep -F "$l_target" >/dev/null 2>&1; then
		fail "Expected dry-run output to mention snapshot target $l_target. Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "destroy" >/dev/null 2>&1; then
		fail "Expected dry-run output to include a destroy operation for $l_target. Output: $l_output"
	fi
}

wait_for_destroy_process_to_finish() {
	l_dataset=$1
	l_snapshot=$2
	l_attempts=${3:-30}

	if ! command -v pgrep >/dev/null 2>&1; then
		return
	fi

	l_pattern="zfs destroy .*${l_dataset}@${l_snapshot}"
	l_i=0
	while [ "$l_i" -lt "$l_attempts" ]; do
		if ! pgrep -f "$l_pattern" >/dev/null 2>&1; then
			return
		fi
		sleep 1
		l_i=$((l_i + 1))
	done
}
