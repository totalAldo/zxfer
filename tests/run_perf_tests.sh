#!/bin/sh
#
# Manual performance checks for zxfer using temporary file-backed ZFS pools.
#

set -eu

if [ -n "${ZXFER_PERF_TESTS_DIR:-}" ]; then
	TESTS_DIR=$ZXFER_PERF_TESTS_DIR
else
	TESTS_DIR=$(dirname "$0")
fi
ZXFER_PERF_ROOT=$(cd "$TESTS_DIR/.." && pwd -P)

# shellcheck source=tests/run_integration_zxfer.sh
ZXFER_RUN_INTEGRATION_SOURCE_ONLY=1 . "$TESTS_DIR/run_integration_zxfer.sh"

ZXFER_PERF_CASE_LIST="chain_local fanout_local_j1_props fanout_local_j4_props chain_remote_mock chain_remote_mock_compressed"
ZXFER_PERF_DEFAULT_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
ZXFER_PERF_PROFILE=${ZXFER_PERF_PROFILE:-smoke}
ZXFER_PERF_CASES=""
ZXFER_PERF_SAMPLES=""
ZXFER_PERF_WARMUPS=""
ZXFER_PERF_OUTPUT_DIR=${ZXFER_PERF_OUTPUT_DIR:-}
ZXFER_PERF_BASELINE=""
ZXFER_PERF_YES=0
ZXFER_PERF_SPARSE_SIZE_MB=""
ZXFER_PERF_CHAIN_SNAPSHOTS=""
ZXFER_PERF_FANOUT_DATASETS=""
ZXFER_PERF_PAYLOAD_MB=""
ZXFER_PERF_SAMPLES_FILE=""
ZXFER_PERF_SUMMARY_FILE=""
ZXFER_PERF_COMPARE_FILE=""
ZXFER_PERF_MARKDOWN_FILE=""
ZXFER_PERF_CURRENT_CASE_DIR=""
ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=0
ZXFER_PERF_LAST_MOCK_SSH_LOG=""

zxfer_perf_print_usage() {
	cat <<'EOF'
usage: ./tests/run_perf_tests.sh [--yes] [--profile smoke|standard] [--case name[,name...]] [--samples N] [--warmups N] [--output-dir path] [--baseline summary.tsv] [--help]

Runs manual, non-gating performance checks against disposable file-backed ZFS
pools. By default the runner asks for one confirmation before creating pools.
Pass --yes only on a trusted throwaway host or inside a disposable VM guest.
EOF
}

zxfer_perf_die() {
	printf '%s\n' "ERROR: $*" >&2
	exit 1
}

zxfer_perf_warn() {
	printf '%s\n' "WARNING: $*" >&2
}

zxfer_perf_append_names() {
	l_current=$1
	l_spec=$2
	l_result=$l_current

	[ -n "$l_spec" ] || {
		printf '%s\n' "$l_result"
		return
	}

	for l_name in $(printf '%s\n' "$l_spec" | tr ',' ' '); do
		[ -n "$l_name" ] || continue
		if ! zxfer_perf_list_contains "$l_result" "$l_name"; then
			if [ -n "$l_result" ]; then
				l_result="$l_result $l_name"
			else
				l_result=$l_name
			fi
		fi
	done

	printf '%s\n' "$l_result"
}

zxfer_perf_list_contains() {
	l_list=$1
	l_want=$2

	case " $l_list " in
	*" $l_want "*) return 0 ;;
	esac
	return 1
}

zxfer_perf_positive_integer_p() {
	case ${1:-} in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ "$1" -gt 0 ]
}

zxfer_perf_nonnegative_integer_p() {
	case ${1:-} in
	'' | *[!0-9]*) return 1 ;;
	esac
	[ "$1" -ge 0 ]
}

zxfer_perf_parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--yes)
			ZXFER_PERF_YES=1
			;;
		--profile)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--profile requires a value"
			ZXFER_PERF_PROFILE=$1
			;;
		--case)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--case requires at least one case name"
			ZXFER_PERF_CASES=$(zxfer_perf_append_names "$ZXFER_PERF_CASES" "$1")
			;;
		--samples)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--samples requires a value"
			ZXFER_PERF_SAMPLES=$1
			;;
		--warmups)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--warmups requires a value"
			ZXFER_PERF_WARMUPS=$1
			;;
		--output-dir)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--output-dir requires a path"
			ZXFER_PERF_OUTPUT_DIR=$1
			;;
		--baseline)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--baseline requires a summary.tsv path"
			ZXFER_PERF_BASELINE=$1
			;;
		-h | --help)
			zxfer_perf_print_usage
			exit 0
			;;
		*)
			zxfer_perf_die "Unknown argument: $1"
			;;
		esac
		shift
	done
}

zxfer_perf_apply_profile_defaults() {
	case "$ZXFER_PERF_PROFILE" in
	smoke)
		: "${ZXFER_PERF_SAMPLES:=1}"
		: "${ZXFER_PERF_WARMUPS:=0}"
		ZXFER_PERF_SPARSE_SIZE_MB=512
		ZXFER_PERF_CHAIN_SNAPSHOTS=6
		ZXFER_PERF_FANOUT_DATASETS=8
		ZXFER_PERF_PAYLOAD_MB=1
		;;
	standard)
		: "${ZXFER_PERF_SAMPLES:=3}"
		: "${ZXFER_PERF_WARMUPS:=1}"
		ZXFER_PERF_SPARSE_SIZE_MB=2048
		ZXFER_PERF_CHAIN_SNAPSHOTS=32
		ZXFER_PERF_FANOUT_DATASETS=48
		ZXFER_PERF_PAYLOAD_MB=2
		;;
	*)
		zxfer_perf_die "Unsupported performance profile: $ZXFER_PERF_PROFILE"
		;;
	esac

	[ -n "$ZXFER_PERF_CASES" ] || ZXFER_PERF_CASES=$ZXFER_PERF_CASE_LIST
	zxfer_perf_positive_integer_p "$ZXFER_PERF_SAMPLES" ||
		zxfer_perf_die "--samples must be a positive integer"
	zxfer_perf_nonnegative_integer_p "$ZXFER_PERF_WARMUPS" ||
		zxfer_perf_die "--warmups must be a non-negative integer"

	for l_case in $ZXFER_PERF_CASES; do
		zxfer_perf_list_contains "$ZXFER_PERF_CASE_LIST" "$l_case" ||
			zxfer_perf_die "Unknown performance case requested: $l_case"
	done
}

zxfer_perf_now_ms() {
	l_now_ms=$(date '+%s%3N' 2>/dev/null || :)
	case "$l_now_ms" in
	'' | *[!0-9]*)
		l_now_epoch=$(date '+%s' 2>/dev/null || :)
		case "$l_now_epoch" in
		'' | *[!0-9]*) return 1 ;;
		esac
		l_now_ms=$((l_now_epoch * 1000))
		;;
	esac
	printf '%s\n' "$l_now_ms"
}

zxfer_perf_confirm_once() {
	[ "$ZXFER_PERF_YES" -eq 1 ] && return 0

	cat >&2 <<EOF
This performance runner creates and destroys file-backed ZFS pools under a
temporary work directory. Run it only on a disposable ZFS-capable host or via
tests/run_vm_matrix.sh --test-layer perf.
EOF
	printf '%s' "Type YES to continue: " >&2
	read -r l_answer || zxfer_perf_die "Confirmation was not provided."
	[ "$l_answer" = "YES" ] || zxfer_perf_die "Confirmation declined."
}

zxfer_perf_setup_output_dir() {
	if [ -n "$ZXFER_PERF_OUTPUT_DIR" ]; then
		mkdir -p "$ZXFER_PERF_OUTPUT_DIR"
		ZXFER_PERF_OUTPUT_DIR=$(cd -P "$ZXFER_PERF_OUTPUT_DIR" && pwd)
	else
		ZXFER_PERF_OUTPUT_DIR=$(mktemp -d "$ZXFER_PERF_ROOT/tests/.tmp-perf.XXXXXX")
	fi

	ZXFER_PERF_SAMPLES_FILE="$ZXFER_PERF_OUTPUT_DIR/samples.tsv"
	ZXFER_PERF_SUMMARY_FILE="$ZXFER_PERF_OUTPUT_DIR/summary.tsv"
	ZXFER_PERF_COMPARE_FILE="$ZXFER_PERF_OUTPUT_DIR/compare.tsv"
	ZXFER_PERF_MARKDOWN_FILE="$ZXFER_PERF_OUTPUT_DIR/summary.md"
	printf '%s\n' "case	sample_kind	sample_index	status	wall_ms	estimated_send_bytes	throughput_bytes_per_sec	startup_latency_ms	cleanup_ms	elapsed_seconds	ssh_setup_ms	source_snapshot_listing_ms	destination_snapshot_listing_ms	snapshot_diff_sort_ms	zfs_send_calls	zfs_receive_calls	ssh_shell_invocations	send_receive_pipeline_commands	send_receive_background_pipeline_commands	mock_ssh_invocations	stdout	stderr" >"$ZXFER_PERF_SAMPLES_FILE"
}

zxfer_perf_cleanup() {
	l_exit_status=$?
	set +e
	l_cleanup_ok=1

	destroy_test_pool_if_owned "source" "${SRC_POOL:-}" "${SRC_POOL_CREATED:-0}" "${SRC_IMG:-}" || l_cleanup_ok=0
	destroy_test_pool_if_owned "destination" "${DEST_POOL:-}" "${DEST_POOL_CREATED:-0}" "${DEST_IMG:-}" || l_cleanup_ok=0
	if [ -n "${WORKDIR:-}" ]; then
		if [ "$l_cleanup_ok" -eq 1 ]; then
			safe_rm_rf "$WORKDIR" || l_cleanup_ok=0
		fi
		if [ "$l_cleanup_ok" -ne 1 ]; then
			zxfer_perf_warn "preserving perf workdir $WORKDIR because cleanup did not fully complete"
		fi
	fi
	if [ "$l_cleanup_ok" -ne 1 ] && [ "$l_exit_status" -eq 0 ]; then
		l_exit_status=1
	fi

	return "$l_exit_status"
}

zxfer_perf_setup_pools() {
	configure_platform_tool_paths
	require_platform_permissions
	require_cmd zpool
	require_cmd zfs
	require_cmd mktemp

	if [ "${ZXFER_BIN:-./zxfer}" = "./zxfer" ] && [ ! -x "$ZXFER_BIN" ] && [ -x "$ZXFER_PERF_ROOT/zxfer" ]; then
		ZXFER_BIN="$ZXFER_PERF_ROOT/zxfer"
	fi
	assert_exists "$ZXFER_BIN" "zxfer binary not found at $ZXFER_BIN"

	WORKDIR=$(mktemp -d "$ZXFER_PERF_OUTPUT_DIR/workdir.XXXXXX")
	WORKDIR=$(cd -P "$WORKDIR" && pwd)
	TEST_RUN_ID="$(date +%s).$$"
	trap zxfer_perf_cleanup EXIT

	SPARSE_SIZE_MB=$ZXFER_PERF_SPARSE_SIZE_MB
	SRC_POOL=$(generate_test_pool_name "perf_src")
	DEST_POOL=$(generate_test_pool_name "perf_dest")
	SRC_MOUNT_ROOT="$WORKDIR/mnt/src"
	DEST_MOUNT_ROOT="$WORKDIR/mnt/dest"
	mkdir -p "$SRC_MOUNT_ROOT" "$DEST_MOUNT_ROOT"

	SRC_IMG="$WORKDIR/${SRC_POOL}.img"
	DEST_IMG="$WORKDIR/${DEST_POOL}.img"
	create_sparse_file "$SRC_IMG" "$SPARSE_SIZE_MB"
	create_sparse_file "$DEST_IMG" "$SPARSE_SIZE_MB"

	log_summary "Creating performance source pool $SRC_POOL"
	create_test_pool "source" "$SRC_POOL" "$SRC_IMG" "$SRC_MOUNT_ROOT"
	SRC_POOL_CREATED=1
	log_summary "Creating performance destination pool $DEST_POOL"
	create_test_pool "destination" "$DEST_POOL" "$DEST_IMG" "$DEST_MOUNT_ROOT"
	DEST_POOL_CREATED=1
}

zxfer_perf_write_payload_mb() {
	l_dataset=$1
	l_file=$2
	l_mb=$3
	l_mountpoint=$(get_mountpoint "$l_dataset")

	dd if=/dev/zero of="$l_mountpoint/$l_file" bs=1048576 count="$l_mb" 2>/dev/null
}

zxfer_perf_parse_send_size() {
	awk '$1 == "size" { value = $2 } END { if (value != "") print value; else print 0 }'
}

zxfer_perf_estimate_send_bytes() {
	l_previous_snapshot=$1
	l_current_snapshot=$2
	l_total=0

	if [ -z "$l_current_snapshot" ]; then
		printf '%s\n' 0
		return
	fi
	if [ -n "$l_previous_snapshot" ] && [ "$l_previous_snapshot" != "$l_current_snapshot" ]; then
		l_size=$(zfs send -nP "$l_previous_snapshot" 2>/dev/null | zxfer_perf_parse_send_size)
		l_total=$((l_total + l_size))
		l_size=$(zfs send -nP -I "$l_previous_snapshot" "$l_current_snapshot" 2>/dev/null | zxfer_perf_parse_send_size)
		l_total=$((l_total + l_size))
	else
		l_size=$(zfs send -nP "$l_current_snapshot" 2>/dev/null | zxfer_perf_parse_send_size)
		l_total=$((l_total + l_size))
	fi
	printf '%s\n' "$l_total"
}

zxfer_perf_profile_value() {
	l_file=$1
	l_key=$2
	awk -v key="$l_key" '
		$0 ~ "^zxfer profile: " key "=" {
			value = $0
			sub("^zxfer profile: " key "=", "", value)
		}
		END {
			if (value != "") print value
			else print 0
		}
	' "$l_file"
}

zxfer_perf_count_mock_ssh_invocations() {
	l_log=$1
	[ -n "$l_log" ] && [ -f "$l_log" ] || {
		printf '%s\n' 0
		return
	}
	awk 'index($0, "close ") != 1 { count++ } END { print count + 0 }' "$l_log"
}

zxfer_perf_record_sample() {
	l_case=$1
	l_sample_kind=$2
	l_sample_index=$3
	l_status=$4
	l_wall_ms=$5
	l_estimated_send_bytes=$6
	l_stdout_file=$7
	l_stderr_file=$8
	l_mock_ssh_log=${9:-}
	l_throughput=0

	if [ "$l_wall_ms" -gt 0 ]; then
		l_throughput=$(awk -v bytes="$l_estimated_send_bytes" -v ms="$l_wall_ms" 'BEGIN { printf "%.2f", (bytes * 1000) / ms }')
	fi
	l_mock_ssh_invocations=$(zxfer_perf_count_mock_ssh_invocations "$l_mock_ssh_log")

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$l_case" \
		"$l_sample_kind" \
		"$l_sample_index" \
		"$l_status" \
		"$l_wall_ms" \
		"$l_estimated_send_bytes" \
		"$l_throughput" \
		"$(zxfer_perf_profile_value "$l_stderr_file" startup_latency_ms)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" cleanup_ms)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" elapsed_seconds)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" ssh_setup_ms)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" source_snapshot_listing_ms)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" destination_snapshot_listing_ms)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" snapshot_diff_sort_ms)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" zfs_send_calls)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" zfs_receive_calls)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" ssh_shell_invocations)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" send_receive_pipeline_commands)" \
		"$(zxfer_perf_profile_value "$l_stderr_file" send_receive_background_pipeline_commands)" \
		"$l_mock_ssh_invocations" \
		"$l_stdout_file" \
		"$l_stderr_file" >>"$ZXFER_PERF_SAMPLES_FILE"
}

zxfer_perf_prepare_remote_mock_path() {
	l_mock_path=$1
	l_with_zstd=$2

	prepare_mock_bin_dir "$l_mock_path" ssh
	write_mock_ssh_script "$l_mock_path/ssh"
	if [ "$l_with_zstd" -eq 1 ]; then
		write_passthrough_zstd "$l_mock_path/zstd"
	fi
}

zxfer_perf_build_mock_secure_path() {
	l_mock_path=$1
	l_base_path=${ZXFER_SECURE_PATH:-$ZXFER_PERF_DEFAULT_SECURE_PATH}

	if [ -n "${ZXFER_SECURE_PATH_APPEND:-}" ]; then
		if [ -n "$l_base_path" ]; then
			l_base_path="$l_base_path:$ZXFER_SECURE_PATH_APPEND"
		else
			l_base_path=$ZXFER_SECURE_PATH_APPEND
		fi
	fi

	if [ -n "$l_base_path" ]; then
		printf '%s:%s\n' "$l_mock_path" "$l_base_path"
	else
		printf '%s\n' "$l_mock_path"
	fi
}

zxfer_perf_run_chain_case() {
	l_case=$1
	l_sample_kind=$2
	l_sample_index=$3
	l_remote=$4
	l_compressed=$5
	l_prefix="${l_case}_${l_sample_kind}_${l_sample_index}"
	l_src_dataset="$SRC_POOL/$l_prefix"
	l_dest_root="$DEST_POOL/${l_prefix}_dest"
	l_dest_dataset="$l_dest_root/${l_src_dataset##*/}"
	l_stdout_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stdout"
	l_stderr_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stderr"
	l_mock_path="$WORKDIR/${l_prefix}_mock"
	l_ssh_log="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.mock_ssh.log"
	l_secure_path=""

	destroy_test_datasets_if_present "$l_src_dataset" "$l_dest_root"
	zfs create "$l_src_dataset"
	zfs create "$l_dest_root"

	l_i=1
	while [ "$l_i" -le "$ZXFER_PERF_CHAIN_SNAPSHOTS" ]; do
		zxfer_perf_write_payload_mb "$l_src_dataset" "payload.$l_i.bin" "$ZXFER_PERF_PAYLOAD_MB"
		zfs snap "$l_src_dataset@s$l_i"
		l_i=$((l_i + 1))
	done
	ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=$(zxfer_perf_estimate_send_bytes "$l_src_dataset@s1" "$l_src_dataset@s$ZXFER_PERF_CHAIN_SNAPSHOTS")
	ZXFER_PERF_LAST_MOCK_SSH_LOG=""

	if [ "$l_remote" -eq 1 ]; then
		zxfer_perf_prepare_remote_mock_path "$l_mock_path" "$l_compressed"
		l_secure_path=$(zxfer_perf_build_mock_secure_path "$l_mock_path")
		ZXFER_PERF_LAST_MOCK_SSH_LOG=$l_ssh_log
	fi

	l_start_ms=$(zxfer_perf_now_ms)
	set +e
	if [ "$l_remote" -eq 1 ] && [ "$l_compressed" -eq 1 ]; then
		MOCK_SSH_LOG="$l_ssh_log" ZXFER_SECURE_PATH="$l_secure_path" \
			"$ZXFER_BIN" -V -z -O localhost -T localhost -R "$l_src_dataset" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	elif [ "$l_remote" -eq 1 ]; then
		MOCK_SSH_LOG="$l_ssh_log" ZXFER_SECURE_PATH="$l_secure_path" \
			"$ZXFER_BIN" -V -O localhost -T localhost -R "$l_src_dataset" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	else
		"$ZXFER_BIN" -V -R "$l_src_dataset" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	fi
	l_status=$?
	set -e
	l_end_ms=$(zxfer_perf_now_ms)
	l_wall_ms=$((l_end_ms - l_start_ms))

	zxfer_perf_record_sample "$l_case" "$l_sample_kind" "$l_sample_index" "$l_status" "$l_wall_ms" "$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES" "$l_stdout_file" "$l_stderr_file" "$ZXFER_PERF_LAST_MOCK_SSH_LOG"
	if [ "$l_status" -ne 0 ]; then
		zxfer_perf_die "$l_case $l_sample_kind $l_sample_index failed with status $l_status; see $l_stderr_file"
	fi
	assert_snapshot_exists "$l_dest_dataset" "s$ZXFER_PERF_CHAIN_SNAPSHOTS"
	destroy_test_datasets_if_present "$l_src_dataset" "$l_dest_root"
}

zxfer_perf_run_fanout_case() {
	l_case=$1
	l_sample_kind=$2
	l_sample_index=$3
	l_jobs=$4
	l_prefix="${l_case}_${l_sample_kind}_${l_sample_index}"
	l_src_parent="$SRC_POOL/$l_prefix"
	l_dest_root="$DEST_POOL/${l_prefix}_dest"
	l_dest_parent="$l_dest_root/${l_src_parent##*/}"
	l_stdout_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stdout"
	l_stderr_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stderr"
	l_estimated=0

	destroy_test_datasets_if_present "$l_src_parent" "$l_dest_root"
	zfs create "$l_src_parent"
	zfs create "$l_dest_root"
	zfs set compression=lz4 "$l_src_parent"

	l_i=1
	while [ "$l_i" -le "$ZXFER_PERF_FANOUT_DATASETS" ]; do
		l_child="$l_src_parent/ds$l_i"
		zfs create "$l_child"
		zfs set atime=off "$l_child"
		zxfer_perf_write_payload_mb "$l_child" "payload.bin" "$ZXFER_PERF_PAYLOAD_MB"
		l_i=$((l_i + 1))
	done
	zfs snap -r "$l_src_parent@s1"

	l_i=1
	while [ "$l_i" -le "$ZXFER_PERF_FANOUT_DATASETS" ]; do
		l_child="$l_src_parent/ds$l_i"
		l_size=$(zxfer_perf_estimate_send_bytes "" "$l_child@s1")
		l_estimated=$((l_estimated + l_size))
		l_i=$((l_i + 1))
	done
	ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=$l_estimated
	ZXFER_PERF_LAST_MOCK_SSH_LOG=""

	l_start_ms=$(zxfer_perf_now_ms)
	set +e
	"$ZXFER_BIN" -V -P -j "$l_jobs" -R "$l_src_parent" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	l_status=$?
	set -e
	l_end_ms=$(zxfer_perf_now_ms)
	l_wall_ms=$((l_end_ms - l_start_ms))

	zxfer_perf_record_sample "$l_case" "$l_sample_kind" "$l_sample_index" "$l_status" "$l_wall_ms" "$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES" "$l_stdout_file" "$l_stderr_file" ""
	if [ "$l_status" -ne 0 ]; then
		zxfer_perf_die "$l_case $l_sample_kind $l_sample_index failed with status $l_status; see $l_stderr_file"
	fi
	assert_snapshot_exists "$l_dest_parent/ds$ZXFER_PERF_FANOUT_DATASETS" "s1"
	destroy_test_datasets_if_present "$l_src_parent" "$l_dest_root"
}

zxfer_perf_run_case_sample() {
	l_case=$1
	l_sample_kind=$2
	l_sample_index=$3

	ZXFER_PERF_CURRENT_CASE_DIR="$ZXFER_PERF_OUTPUT_DIR/raw/$l_case"
	mkdir -p "$ZXFER_PERF_CURRENT_CASE_DIR"
	log_summary "Running perf case $l_case ($l_sample_kind $l_sample_index)"

	case "$l_case" in
	chain_local)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 0 0
		;;
	fanout_local_j1_props)
		zxfer_perf_run_fanout_case "$l_case" "$l_sample_kind" "$l_sample_index" 1
		;;
	fanout_local_j4_props)
		zxfer_perf_run_fanout_case "$l_case" "$l_sample_kind" "$l_sample_index" 4
		;;
	chain_remote_mock)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 1 0
		;;
	chain_remote_mock_compressed)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 1 1
		;;
	*)
		zxfer_perf_die "Unhandled performance case: $l_case"
		;;
	esac
}

zxfer_perf_run_cases() {
	for l_case in $ZXFER_PERF_CASES; do
		l_i=1
		while [ "$l_i" -le "$ZXFER_PERF_WARMUPS" ]; do
			zxfer_perf_run_case_sample "$l_case" warmup "$l_i"
			l_i=$((l_i + 1))
		done
		l_i=1
		while [ "$l_i" -le "$ZXFER_PERF_SAMPLES" ]; do
			zxfer_perf_run_case_sample "$l_case" sample "$l_i"
			l_i=$((l_i + 1))
		done
	done
}

zxfer_perf_render_summary() {
	awk -F '\t' '
		NR == 1 { next }
		$2 != "sample" { next }
		{
			if (!seen[$1]++) order[++case_count] = $1
			samples[$1]++
			if ($4 != 0) failed[$1]++
			wall[$1] += $5
			bytes[$1] += $6
			throughput[$1] += $7
			startup[$1] += $8
			cleanup[$1] += $9
			mockssh[$1] += $20
			zfssend[$1] += $15
			zfsrecv[$1] += $16
			ssh[$1] += $17
		}
		END {
			print "case\tsamples\twall_ms_avg\tthroughput_bytes_per_sec_avg\tstartup_latency_ms_avg\tcleanup_ms_avg\testimated_send_bytes_avg\tmock_ssh_invocations_avg\tzfs_send_calls_avg\tzfs_receive_calls_avg\tssh_shell_invocations_avg\tfailed_samples"
			for (i = 1; i <= case_count; i++) {
				c = order[i]
				n = samples[c]
				if (n == 0) n = 1
				printf "%s\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%d\n", c, samples[c], wall[c] / n, throughput[c] / n, startup[c] / n, cleanup[c] / n, bytes[c] / n, mockssh[c] / n, zfssend[c] / n, zfsrecv[c] / n, ssh[c] / n, failed[c] + 0
			}
		}
	' "$ZXFER_PERF_SAMPLES_FILE" >"$ZXFER_PERF_SUMMARY_FILE"

	awk -F '\t' '
		NR == 1 {
			print "# zxfer performance summary"
			print ""
			print "| case | samples | wall ms avg | throughput B/s avg | startup ms avg | cleanup ms avg | mock ssh avg | failures |"
			print "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
			next
		}
		{
			printf "| `%s` | %s | %s | %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5, $6, $8, $12
		}
	' "$ZXFER_PERF_SUMMARY_FILE" >"$ZXFER_PERF_MARKDOWN_FILE"
}

zxfer_perf_compare_baseline() {
	[ -n "$ZXFER_PERF_BASELINE" ] || return 0
	[ -r "$ZXFER_PERF_BASELINE" ] || zxfer_perf_die "Baseline summary is not readable: $ZXFER_PERF_BASELINE"

	awk -F '\t' '
		FNR == NR {
			if (FNR > 1) {
				base_wall[$1] = $3
				base_throughput[$1] = $4
				base_startup[$1] = $5
				base_cleanup[$1] = $6
			}
			next
		}
		FNR == 1 {
			print "case\tmetric\tbaseline\tcurrent\tpct_delta\twarning"
			next
		}
		FNR > 1 {
			compare($1, "wall_ms_avg", base_wall[$1], $3, 1)
			compare($1, "throughput_bytes_per_sec_avg", base_throughput[$1], $4, -1)
			compare($1, "startup_latency_ms_avg", base_startup[$1], $5, 1)
			compare($1, "cleanup_ms_avg", base_cleanup[$1], $6, 1)
		}
		function compare(c, metric, baseline, current, direction, pct, warning) {
			if (baseline == "" || baseline == 0) {
				return
			}
			pct = ((current - baseline) / baseline) * 100
			warning = ""
			if (direction > 0 && pct > 10) warning = "regression"
			if (direction < 0 && pct < -10) warning = "regression"
			printf "%s\t%s\t%s\t%s\t%.2f\t%s\n", c, metric, baseline, current, pct, warning
		}
	' "$ZXFER_PERF_BASELINE" "$ZXFER_PERF_SUMMARY_FILE" >"$ZXFER_PERF_COMPARE_FILE"

	if awk -F '\t' 'NR > 1 && $6 == "regression" { found = 1 } END { exit(found ? 0 : 1) }' "$ZXFER_PERF_COMPARE_FILE"; then
		zxfer_perf_warn "performance regressions were detected relative to $ZXFER_PERF_BASELINE; see $ZXFER_PERF_COMPARE_FILE"
	fi
}

zxfer_perf_main() {
	zxfer_perf_parse_args "$@"
	zxfer_perf_apply_profile_defaults
	zxfer_perf_confirm_once
	zxfer_perf_setup_output_dir
	zxfer_perf_setup_pools
	zxfer_perf_run_cases
	zxfer_perf_render_summary
	zxfer_perf_compare_baseline
	log_summary "Performance artifacts written to $ZXFER_PERF_OUTPUT_DIR"
}

if [ "${ZXFER_RUN_PERF_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_perf_main "$@"
fi
