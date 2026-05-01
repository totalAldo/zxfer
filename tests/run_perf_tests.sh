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

ZXFER_PERF_CASE_LIST="chain_local chain_local_noop fanout_local_j1_props fanout_local_j4_props fanout_local_j4_props_noop chain_remote_mock chain_remote_mock_noop chain_remote_mock_compressed"
ZXFER_PERF_PROFILE_METRICS="elapsed_seconds startup_latency_ms cleanup_ms ssh_setup_ms source_snapshot_listing_ms destination_snapshot_listing_ms snapshot_diff_sort_ms ssh_control_socket_lock_wait_count ssh_control_socket_lock_wait_ms remote_capability_cache_wait_count remote_capability_cache_wait_ms remote_capability_bootstrap_live remote_capability_bootstrap_cache remote_capability_bootstrap_memory remote_cli_tool_direct_probes source_zfs_calls destination_zfs_calls other_zfs_calls zfs_list_calls zfs_get_calls zfs_send_calls zfs_receive_calls ssh_shell_invocations source_ssh_shell_invocations destination_ssh_shell_invocations other_ssh_shell_invocations source_snapshot_list_commands source_snapshot_list_parallel_commands send_receive_pipeline_commands send_receive_background_pipeline_commands exists_destination_calls normalized_property_reads_source normalized_property_reads_destination normalized_property_reads_other required_property_backfill_gets parent_destination_property_reads bucket_source_inspection bucket_destination_inspection bucket_property_reconciliation bucket_send_receive_setup runtime_artifact_files_created runtime_artifact_dirs_created runtime_artifact_paths_cleaned runtime_cache_object_writes runtime_cache_object_readbacks command_render_calls live_destination_snapshot_rechecks"
ZXFER_PERF_DEFAULT_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
ZXFER_PERF_PROFILE=${ZXFER_PERF_PROFILE:-smoke}
ZXFER_PERF_LABEL=${ZXFER_PERF_LABEL:-current}
ZXFER_PERF_CASES=""
ZXFER_PERF_SAMPLES=""
ZXFER_PERF_WARMUPS=""
ZXFER_PERF_OUTPUT_DIR=${ZXFER_PERF_OUTPUT_DIR:-}
ZXFER_PERF_BASELINE=""
ZXFER_PERF_REGRESSION_THRESHOLD_PCT=${ZXFER_PERF_REGRESSION_THRESHOLD_PCT:-10}
ZXFER_PERF_YES=0
ZXFER_PERF_SPARSE_SIZE_MB=""
ZXFER_PERF_CHAIN_SNAPSHOTS=""
ZXFER_PERF_FANOUT_DATASETS=""
ZXFER_PERF_PAYLOAD_MB=""
ZXFER_PERF_SAMPLES_FILE=""
ZXFER_PERF_SUMMARY_FILE=""
ZXFER_PERF_COMPARE_FILE=""
ZXFER_PERF_MARKDOWN_FILE=""
ZXFER_PERF_RUN_INFO_FILE=""
ZXFER_PERF_CURRENT_CASE_DIR=""
ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=0
ZXFER_PERF_LAST_MOCK_SSH_LOG=""

zxfer_perf_print_usage() {
	cat <<'EOF'
usage: ./tests/run_perf_tests.sh [--yes] [--label LABEL] [--profile smoke|standard] [--case name[,name...]] [--samples N] [--warmups N] [--output-dir path] [--baseline summary.tsv] [--help]

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

zxfer_perf_count_words() {
	l_perf_count_words_count=0

	for l_perf_count_words_word in $1; do
		[ -n "$l_perf_count_words_word" ] || continue
		l_perf_count_words_count=$((l_perf_count_words_count + 1))
	done

	printf '%s\n' "$l_perf_count_words_count"
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
		--label)
			shift
			[ $# -gt 0 ] && [ -n "$1" ] || zxfer_perf_die "--label requires a value"
			ZXFER_PERF_LABEL=$1
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

zxfer_perf_case_description() {
	case "$1" in
	chain_local)
		printf '%s\n' "local recursive chain replication; measures startup latency, send throughput, receive cost, and cleanup"
		;;
	chain_local_noop)
		printf '%s\n' "local recursive no-op replication; seeds the fixture first, then measures a second no-op run"
		;;
	fanout_local_j1_props)
		printf '%s\n' "local fanout replication with one job; measures sibling dataset/property reconciliation without concurrency"
		;;
	fanout_local_j4_props)
		printf '%s\n' "local fanout replication with four jobs; measures sibling dataset/property reconciliation with concurrency"
		;;
	fanout_local_j4_props_noop)
		printf '%s\n' "local fanout no-op replication with four jobs; seeds the fixture first, then measures a second no-op run"
		;;
	chain_remote_mock)
		printf '%s\n' "mock-remote chain replication; measures ssh command construction and remote round-trip counters"
		;;
	chain_remote_mock_noop)
		printf '%s\n' "mock-remote no-op chain replication; seeds the fixture first, then measures a second no-op run"
		;;
	chain_remote_mock_compressed)
		printf '%s\n' "mock-remote compressed chain replication; measures ssh plus compression/decompression pipeline overhead"
		;;
	*)
		printf '%s\n' "custom performance case"
		;;
	esac
}

zxfer_perf_log_configuration() {
	log_summary "Performance profile $ZXFER_PERF_PROFILE label=$ZXFER_PERF_LABEL: cases=[$ZXFER_PERF_CASES] warmups=$ZXFER_PERF_WARMUPS samples=$ZXFER_PERF_SAMPLES sparse_pool_mb=$ZXFER_PERF_SPARSE_SIZE_MB chain_snapshots=$ZXFER_PERF_CHAIN_SNAPSHOTS fanout_datasets=$ZXFER_PERF_FANOUT_DATASETS payload_mb=$ZXFER_PERF_PAYLOAD_MB"
	log_summary "Performance artifacts will be written under $ZXFER_PERF_OUTPUT_DIR"
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

zxfer_perf_print_sample_header() {
	printf '%s' "run_label	case	sample_kind	sample_index	status	wall_ms	estimated_send_bytes	throughput_bytes_per_sec"
	for l_metric in $ZXFER_PERF_PROFILE_METRICS; do
		printf '\t%s' "$l_metric"
	done
	printf '%s\n' "	mock_ssh_invocations	stdout	stderr"
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
	ZXFER_PERF_RUN_INFO_FILE="$ZXFER_PERF_OUTPUT_DIR/run-info.tsv"
	zxfer_perf_print_sample_header >"$ZXFER_PERF_SAMPLES_FILE"
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

zxfer_perf_tsv_value() {
	printf '%s\n' "$1" | awk '
		{
			if (NR > 1) printf " "
			gsub(/\t|\r/, " ")
			printf "%s", $0
		}
	'
}

zxfer_perf_first_output_line() {
	"$@" 2>/dev/null | sed -n '1p'
}

zxfer_perf_abs_path() {
	l_path=$1

	case "$l_path" in
	/*)
		printf '%s\n' "$l_path"
		;;
	*)
		l_dir=$(dirname "$l_path")
		l_base=$(basename "$l_path")
		if l_dir_abs=$(cd "$l_dir" 2>/dev/null && pwd -P); then
			printf '%s/%s\n' "$l_dir_abs" "$l_base"
		else
			printf '%s\n' "$l_path"
		fi
		;;
	esac
}

zxfer_perf_write_run_info_pair() {
	l_key=$1
	l_value=$2

	printf '%s\t%s\n' "$l_key" "$(zxfer_perf_tsv_value "$l_value")" >>"$ZXFER_PERF_RUN_INFO_FILE"
}

zxfer_perf_write_run_info() {
	printf '%s\n' "key	value" >"$ZXFER_PERF_RUN_INFO_FILE"
	zxfer_perf_write_run_info_pair "label" "$ZXFER_PERF_LABEL"
	zxfer_perf_write_run_info_pair "profile" "$ZXFER_PERF_PROFILE"
	zxfer_perf_write_run_info_pair "cases" "$ZXFER_PERF_CASES"
	zxfer_perf_write_run_info_pair "samples" "$ZXFER_PERF_SAMPLES"
	zxfer_perf_write_run_info_pair "warmups" "$ZXFER_PERF_WARMUPS"
	zxfer_perf_write_run_info_pair "timestamp" "$(date '+%Y-%m-%dT%H:%M:%S%z' 2>/dev/null || date)"
	zxfer_perf_write_run_info_pair "platform" "$(uname -a 2>/dev/null || uname)"
	zxfer_perf_write_run_info_pair "ZXFER_BIN" "$(zxfer_perf_abs_path "$ZXFER_BIN")"
	zxfer_perf_write_run_info_pair "zfs_version" "$(zxfer_perf_first_output_line zfs version || :)"
	zxfer_perf_write_run_info_pair "zpool_version" "$(zxfer_perf_first_output_line zpool version || :)"
	zxfer_perf_write_run_info_pair "fixture_sparse_size_mb" "$ZXFER_PERF_SPARSE_SIZE_MB"
	zxfer_perf_write_run_info_pair "fixture_chain_snapshots" "$ZXFER_PERF_CHAIN_SNAPSHOTS"
	zxfer_perf_write_run_info_pair "fixture_fanout_datasets" "$ZXFER_PERF_FANOUT_DATASETS"
	zxfer_perf_write_run_info_pair "fixture_payload_mb" "$ZXFER_PERF_PAYLOAD_MB"
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
	[ -r "$l_file" ] || return 0
	awk -v key="$l_key" '
		$0 ~ "^zxfer profile: " key "=" {
			value = $0
			sub("^zxfer profile: " key "=", "", value)
		}
		END {
			if (value != "") print value
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

zxfer_perf_calculate_throughput() {
	l_perf_calculate_throughput_bytes=$1
	l_perf_calculate_throughput_wall_ms=$2

	if [ "$l_perf_calculate_throughput_wall_ms" -gt 0 ]; then
		awk -v bytes="$l_perf_calculate_throughput_bytes" -v ms="$l_perf_calculate_throughput_wall_ms" 'BEGIN { printf "%.2f", (bytes * 1000) / ms }'
	else
		printf '%s\n' 0
	fi
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

	l_throughput=$(zxfer_perf_calculate_throughput "$l_estimated_send_bytes" "$l_wall_ms")
	l_mock_ssh_invocations=$(zxfer_perf_count_mock_ssh_invocations "$l_mock_ssh_log")

	{
		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' \
			"$(zxfer_perf_tsv_value "$ZXFER_PERF_LABEL")" \
			"$l_case" \
			"$l_sample_kind" \
			"$l_sample_index" \
			"$l_status" \
			"$l_wall_ms" \
			"$l_estimated_send_bytes" \
			"$l_throughput"
		for l_metric in $ZXFER_PERF_PROFILE_METRICS; do
			printf '\t%s' "$(zxfer_perf_profile_value "$l_stderr_file" "$l_metric")"
		done
		printf '\t%s\t%s\t%s\n' \
			"$l_mock_ssh_invocations" \
			"$(zxfer_perf_tsv_value "$l_stdout_file")" \
			"$(zxfer_perf_tsv_value "$l_stderr_file")"
	} >>"$ZXFER_PERF_SAMPLES_FILE"
}

zxfer_perf_log_sample_result() {
	l_perf_result_case=$1
	l_perf_result_sample_kind=$2
	l_perf_result_sample_index=$3
	l_perf_result_status=$4
	l_perf_result_wall_ms=$5
	l_perf_result_estimated_send_bytes=$6
	l_perf_result_stderr_file=$7
	l_perf_result_mock_ssh_log=${8:-}
	l_perf_result_throughput=$(zxfer_perf_calculate_throughput "$l_perf_result_estimated_send_bytes" "$l_perf_result_wall_ms")
	l_perf_result_startup_ms=$(zxfer_perf_profile_value "$l_perf_result_stderr_file" startup_latency_ms)
	l_perf_result_cleanup_ms=$(zxfer_perf_profile_value "$l_perf_result_stderr_file" cleanup_ms)
	l_perf_result_mock_ssh_invocations=$(zxfer_perf_count_mock_ssh_invocations "$l_perf_result_mock_ssh_log")

	log_summary "Completed perf case $l_perf_result_case ($l_perf_result_sample_kind $l_perf_result_sample_index): status=$l_perf_result_status wall_ms=$l_perf_result_wall_ms throughput_Bps=$l_perf_result_throughput startup_latency_ms=$l_perf_result_startup_ms cleanup_ms=$l_perf_result_cleanup_ms mock_ssh_invocations=$l_perf_result_mock_ssh_invocations"
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

zxfer_perf_invoke_chain_replication() {
	l_remote=$1
	l_compressed=$2
	l_src_dataset=$3
	l_dest_root=$4
	l_stdout_file=$5
	l_stderr_file=$6
	l_ssh_log=${7:-}
	l_secure_path=${8:-}

	if [ "$l_remote" -eq 1 ] && [ "$l_compressed" -eq 1 ]; then
		MOCK_SSH_LOG="$l_ssh_log" ZXFER_SECURE_PATH="$l_secure_path" \
			"$ZXFER_BIN" -V -z -O localhost -T localhost -R "$l_src_dataset" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	elif [ "$l_remote" -eq 1 ]; then
		MOCK_SSH_LOG="$l_ssh_log" ZXFER_SECURE_PATH="$l_secure_path" \
			"$ZXFER_BIN" -V -O localhost -T localhost -R "$l_src_dataset" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	else
		"$ZXFER_BIN" -V -R "$l_src_dataset" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
	fi
}

zxfer_perf_run_chain_case() {
	l_case=$1
	l_sample_kind=$2
	l_sample_index=$3
	l_remote=$4
	l_compressed=$5
	l_noop=${6:-0}
	l_prefix="${l_case}_${l_sample_kind}_${l_sample_index}"
	l_src_dataset="$SRC_POOL/$l_prefix"
	l_dest_root="$DEST_POOL/${l_prefix}_dest"
	l_dest_dataset="$l_dest_root/${l_src_dataset##*/}"
	l_stdout_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stdout"
	l_stderr_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stderr"
	l_mock_path="$WORKDIR/${l_prefix}_mock"
	l_ssh_log="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.mock_ssh.log"
	l_secure_path=""

	log_summary "Preparing chain fixture for $l_case: snapshots=$ZXFER_PERF_CHAIN_SNAPSHOTS payload_mb_per_snapshot=$ZXFER_PERF_PAYLOAD_MB remote=$l_remote compressed=$l_compressed source=$l_src_dataset destination_root=$l_dest_root"
	destroy_test_datasets_if_present "$l_src_dataset" "$l_dest_root"
	zfs create "$l_src_dataset"
	zfs create "$l_dest_root"

	l_perf_chain_snapshot_index=1
	while [ "$l_perf_chain_snapshot_index" -le "$ZXFER_PERF_CHAIN_SNAPSHOTS" ]; do
		zxfer_perf_write_payload_mb "$l_src_dataset" "payload.$l_perf_chain_snapshot_index.bin" "$ZXFER_PERF_PAYLOAD_MB"
		zfs snap "$l_src_dataset@s$l_perf_chain_snapshot_index"
		l_perf_chain_snapshot_index=$((l_perf_chain_snapshot_index + 1))
	done
	ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=$(zxfer_perf_estimate_send_bytes "$l_src_dataset@s1" "$l_src_dataset@s$ZXFER_PERF_CHAIN_SNAPSHOTS")
	log_summary "Estimated send size for $l_case ($l_sample_kind $l_sample_index): estimated_send_bytes=$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES"
	ZXFER_PERF_LAST_MOCK_SSH_LOG=""

	if [ "$l_remote" -eq 1 ]; then
		zxfer_perf_prepare_remote_mock_path "$l_mock_path" "$l_compressed"
		l_secure_path=$(zxfer_perf_build_mock_secure_path "$l_mock_path")
		ZXFER_PERF_LAST_MOCK_SSH_LOG=$l_ssh_log
		log_summary "Prepared mock ssh path for $l_case: mock_path=$l_mock_path mock_log=$l_ssh_log"
	fi

	if [ "$l_noop" -eq 1 ]; then
		l_setup_stdout_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.setup.stdout"
		l_setup_stderr_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.setup.stderr"
		l_setup_ssh_log="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.setup.mock_ssh.log"
		log_summary "Seeding no-op fixture for $l_case ($l_sample_kind $l_sample_index): setup_stdout=$l_setup_stdout_file setup_stderr=$l_setup_stderr_file"
		set +e
		zxfer_perf_invoke_chain_replication "$l_remote" "$l_compressed" "$l_src_dataset" "$l_dest_root" "$l_setup_stdout_file" "$l_setup_stderr_file" "$l_setup_ssh_log" "$l_secure_path"
		l_setup_status=$?
		set -e
		if [ "$l_setup_status" -ne 0 ]; then
			zxfer_perf_die "$l_case setup $l_sample_kind $l_sample_index failed with status $l_setup_status; see $l_setup_stderr_file"
		fi
		rm -f "$l_ssh_log"
		ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=0
	fi

	log_summary "Measuring $l_case ($l_sample_kind $l_sample_index): invoking zxfer; raw_stdout=$l_stdout_file raw_stderr=$l_stderr_file"
	l_start_ms=$(zxfer_perf_now_ms)
	set +e
	zxfer_perf_invoke_chain_replication "$l_remote" "$l_compressed" "$l_src_dataset" "$l_dest_root" "$l_stdout_file" "$l_stderr_file" "$l_ssh_log" "$l_secure_path"
	l_status=$?
	set -e
	l_end_ms=$(zxfer_perf_now_ms)
	l_wall_ms=$((l_end_ms - l_start_ms))

	zxfer_perf_record_sample "$l_case" "$l_sample_kind" "$l_sample_index" "$l_status" "$l_wall_ms" "$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES" "$l_stdout_file" "$l_stderr_file" "$ZXFER_PERF_LAST_MOCK_SSH_LOG"
	zxfer_perf_log_sample_result "$l_case" "$l_sample_kind" "$l_sample_index" "$l_status" "$l_wall_ms" "$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES" "$l_stderr_file" "$ZXFER_PERF_LAST_MOCK_SSH_LOG"
	if [ "$l_status" -ne 0 ]; then
		zxfer_perf_die "$l_case $l_sample_kind $l_sample_index failed with status $l_status; see $l_stderr_file"
	fi
	assert_snapshot_exists "$l_dest_dataset" "s$ZXFER_PERF_CHAIN_SNAPSHOTS"
	destroy_test_datasets_if_present "$l_src_dataset" "$l_dest_root"
}

zxfer_perf_invoke_fanout_replication() {
	l_jobs=$1
	l_src_parent=$2
	l_dest_root=$3
	l_stdout_file=$4
	l_stderr_file=$5

	"$ZXFER_BIN" -V -P -j "$l_jobs" -R "$l_src_parent" "$l_dest_root" >"$l_stdout_file" 2>"$l_stderr_file"
}

zxfer_perf_run_fanout_case() {
	l_case=$1
	l_sample_kind=$2
	l_sample_index=$3
	l_jobs=$4
	l_noop=${5:-0}
	l_prefix="${l_case}_${l_sample_kind}_${l_sample_index}"
	l_src_parent="$SRC_POOL/$l_prefix"
	l_dest_root="$DEST_POOL/${l_prefix}_dest"
	l_dest_parent="$l_dest_root/${l_src_parent##*/}"
	l_stdout_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stdout"
	l_stderr_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.stderr"
	l_estimated=0

	log_summary "Preparing fanout fixture for $l_case: datasets=$ZXFER_PERF_FANOUT_DATASETS jobs=$l_jobs payload_mb_per_dataset=$ZXFER_PERF_PAYLOAD_MB source_parent=$l_src_parent destination_root=$l_dest_root"
	destroy_test_datasets_if_present "$l_src_parent" "$l_dest_root"
	zfs create "$l_src_parent"
	zfs create "$l_dest_root"
	zfs set compression=lz4 "$l_src_parent"

	l_perf_fanout_dataset_index=1
	while [ "$l_perf_fanout_dataset_index" -le "$ZXFER_PERF_FANOUT_DATASETS" ]; do
		l_child="$l_src_parent/ds$l_perf_fanout_dataset_index"
		zfs create "$l_child"
		zfs set atime=off "$l_child"
		zxfer_perf_write_payload_mb "$l_child" "payload.bin" "$ZXFER_PERF_PAYLOAD_MB"
		l_perf_fanout_dataset_index=$((l_perf_fanout_dataset_index + 1))
	done
	zfs snap -r "$l_src_parent@s1"

	l_perf_fanout_estimate_index=1
	while [ "$l_perf_fanout_estimate_index" -le "$ZXFER_PERF_FANOUT_DATASETS" ]; do
		l_child="$l_src_parent/ds$l_perf_fanout_estimate_index"
		l_size=$(zxfer_perf_estimate_send_bytes "" "$l_child@s1")
		l_estimated=$((l_estimated + l_size))
		l_perf_fanout_estimate_index=$((l_perf_fanout_estimate_index + 1))
	done
	ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=$l_estimated
	log_summary "Estimated send size for $l_case ($l_sample_kind $l_sample_index): estimated_send_bytes=$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES"
	ZXFER_PERF_LAST_MOCK_SSH_LOG=""

	if [ "$l_noop" -eq 1 ]; then
		l_setup_stdout_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.setup.stdout"
		l_setup_stderr_file="$ZXFER_PERF_CURRENT_CASE_DIR/${l_sample_kind}_${l_sample_index}.setup.stderr"
		log_summary "Seeding no-op fixture for $l_case ($l_sample_kind $l_sample_index): setup_stdout=$l_setup_stdout_file setup_stderr=$l_setup_stderr_file"
		set +e
		zxfer_perf_invoke_fanout_replication "$l_jobs" "$l_src_parent" "$l_dest_root" "$l_setup_stdout_file" "$l_setup_stderr_file"
		l_setup_status=$?
		set -e
		if [ "$l_setup_status" -ne 0 ]; then
			zxfer_perf_die "$l_case setup $l_sample_kind $l_sample_index failed with status $l_setup_status; see $l_setup_stderr_file"
		fi
		ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES=0
	fi

	log_summary "Measuring $l_case ($l_sample_kind $l_sample_index): invoking zxfer with jobs=$l_jobs; raw_stdout=$l_stdout_file raw_stderr=$l_stderr_file"
	l_start_ms=$(zxfer_perf_now_ms)
	set +e
	zxfer_perf_invoke_fanout_replication "$l_jobs" "$l_src_parent" "$l_dest_root" "$l_stdout_file" "$l_stderr_file"
	l_status=$?
	set -e
	l_end_ms=$(zxfer_perf_now_ms)
	l_wall_ms=$((l_end_ms - l_start_ms))

	zxfer_perf_record_sample "$l_case" "$l_sample_kind" "$l_sample_index" "$l_status" "$l_wall_ms" "$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES" "$l_stdout_file" "$l_stderr_file" ""
	zxfer_perf_log_sample_result "$l_case" "$l_sample_kind" "$l_sample_index" "$l_status" "$l_wall_ms" "$ZXFER_PERF_LAST_ESTIMATED_SEND_BYTES" "$l_stderr_file" ""
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
	l_case_number=${4:-?}
	l_case_count=${5:-?}
	l_sample_total=$ZXFER_PERF_SAMPLES
	l_case_description=$(zxfer_perf_case_description "$l_case")

	if [ "$l_sample_kind" = "warmup" ]; then
		l_sample_total=$ZXFER_PERF_WARMUPS
	fi

	ZXFER_PERF_CURRENT_CASE_DIR="$ZXFER_PERF_OUTPUT_DIR/raw/$l_case"
	mkdir -p "$ZXFER_PERF_CURRENT_CASE_DIR"
	log_summary "Starting perf case $l_case ($l_sample_kind $l_sample_index/$l_sample_total, case $l_case_number/$l_case_count): $l_case_description"

	case "$l_case" in
	chain_local)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 0 0
		;;
	chain_local_noop)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 0 0 1
		;;
	fanout_local_j1_props)
		zxfer_perf_run_fanout_case "$l_case" "$l_sample_kind" "$l_sample_index" 1
		;;
	fanout_local_j4_props)
		zxfer_perf_run_fanout_case "$l_case" "$l_sample_kind" "$l_sample_index" 4
		;;
	fanout_local_j4_props_noop)
		zxfer_perf_run_fanout_case "$l_case" "$l_sample_kind" "$l_sample_index" 4 1
		;;
	chain_remote_mock)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 1 0
		;;
	chain_remote_mock_noop)
		zxfer_perf_run_chain_case "$l_case" "$l_sample_kind" "$l_sample_index" 1 0 1
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
	l_perf_case_count=$(zxfer_perf_count_words "$ZXFER_PERF_CASES")
	l_perf_case_number=1

	for l_perf_run_case in $ZXFER_PERF_CASES; do
		l_perf_warmup_index=1
		while [ "$l_perf_warmup_index" -le "$ZXFER_PERF_WARMUPS" ]; do
			zxfer_perf_run_case_sample "$l_perf_run_case" warmup "$l_perf_warmup_index" "$l_perf_case_number" "$l_perf_case_count"
			l_perf_warmup_index=$((l_perf_warmup_index + 1))
		done
		l_perf_sample_index=1
		while [ "$l_perf_sample_index" -le "$ZXFER_PERF_SAMPLES" ]; do
			zxfer_perf_run_case_sample "$l_perf_run_case" sample "$l_perf_sample_index" "$l_perf_case_number" "$l_perf_case_count"
			l_perf_sample_index=$((l_perf_sample_index + 1))
		done
		l_perf_case_number=$((l_perf_case_number + 1))
	done
}

zxfer_perf_render_summary() {
	awk -F '\t' '
		function excluded(name) {
			return name == "run_label" ||
				name == "case" ||
				name == "sample_kind" ||
				name == "sample_index" ||
				name == "status" ||
				name == "stdout" ||
				name == "stderr"
		}
		function numeric(value) {
			return value ~ /^-?[0-9]+([.][0-9]+)?$/
		}
		NR == 1 {
			for (i = 1; i <= NF; i++) {
				name[i] = $i
				col[$i] = i
				if (!excluded($i)) {
					metric[++metric_count] = i
					metric_name[metric_count] = $i
				}
			}
			next
		}
		$(col["sample_kind"]) != "sample" { next }
		{
			c = $(col["case"])
			if (!seen[c]++) order[++case_count] = c
			if (run_label[c] == "") run_label[c] = $(col["run_label"])
			samples[c]++
			if ($(col["status"]) != "" && $(col["status"]) != 0) failed[c]++
			for (i = 1; i <= metric_count; i++) {
				idx = metric[i]
				if (numeric($idx)) {
					sum[c SUBSEP idx] += $idx
					count[c SUBSEP idx]++
				}
			}
		}
		END {
			printf "run_label\tcase\tsamples"
			for (i = 1; i <= metric_count; i++) {
				printf "\t%s_avg", metric_name[i]
			}
			printf "\tfailed_samples\n"
			for (i = 1; i <= case_count; i++) {
				c = order[i]
				printf "%s\t%s\t%d", run_label[c], c, samples[c]
				for (j = 1; j <= metric_count; j++) {
					idx = metric[j]
					n = count[c SUBSEP idx]
					if (n > 0) {
						printf "\t%.2f", sum[c SUBSEP idx] / n
					} else {
						printf "\t"
					}
				}
				printf "\t%d\n", failed[c] + 0
			}
		}
	' "$ZXFER_PERF_SAMPLES_FILE" >"$ZXFER_PERF_SUMMARY_FILE"

	awk -F '\t' '
		function cell(name) {
			if (col[name] == "") return ""
			return $(col[name])
		}
		NR == 1 {
			for (i = 1; i <= NF; i++) col[$i] = i
			print "# zxfer performance summary"
			print ""
			print "| label | case | samples | wall ms avg | throughput B/s avg | startup ms avg | cleanup ms avg | mock ssh avg | failures |"
			print "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
			next
		}
		{
			printf "| %s | `%s` | %s | %s | %s | %s | %s | %s | %s |\n", $1, $2, $3, cell("wall_ms_avg"), cell("throughput_bytes_per_sec_avg"), cell("startup_latency_ms_avg"), cell("cleanup_ms_avg"), cell("mock_ssh_invocations_avg"), cell("failed_samples")
		}
	' "$ZXFER_PERF_SUMMARY_FILE" >"$ZXFER_PERF_MARKDOWN_FILE"
}

zxfer_perf_compare_summary_files() {
	l_baseline_file=$1
	l_current_file=$2
	l_compare_file=$3
	l_threshold_pct=$4

	awk -F '\t' -v threshold="$l_threshold_pct" '
		function excluded(name) {
			return name == "run_label" ||
				name == "case" ||
				name == "samples" ||
				name == "failed_samples"
		}
		function numeric(value) {
			return value ~ /^-?[0-9]+([.][0-9]+)?$/
		}
		function pct_delta(baseline, current) {
			return ((current - baseline) / baseline) * 100
		}
		function compare(c, metric, baseline, current, pct, warning, pct_text) {
			if (!numeric(baseline) || !numeric(current)) return
			warning = ""
			pct_text = ""
			if (baseline != 0) {
				pct = pct_delta(baseline, current)
				pct_text = sprintf("%.2f", pct)
				if (metric ~ /throughput/ && pct < -threshold) warning = "regression"
				if (metric !~ /throughput/ && pct > threshold) warning = "regression"
			}
			printf "%s\t%s\t%s\t%s\t%s\t%s\n", c, metric, baseline, current, pct_text, warning
		}
		FNR == NR {
			if (FNR == 1) {
				for (i = 1; i <= NF; i++) base_col[$i] = i
				next
			}
			c = $(base_col["case"])
			base_seen[c] = 1
			for (name in base_col) {
				if (!excluded(name)) base[c SUBSEP name] = $(base_col[name])
			}
			next
		}
		FNR == 1 {
			print "case\tmetric\tbaseline\tcurrent\tpct_delta\twarning"
			for (i = 1; i <= NF; i++) {
				current_name[i] = $i
				current_col[$i] = i
			}
			next
		}
		FNR > 1 {
			c = $(current_col["case"])
			if (!base_seen[c]) next
			for (i = 1; i <= NF; i++) {
				metric = current_name[i]
				if (excluded(metric)) continue
				if (!(metric in base_col)) continue
				compare(c, metric, base[c SUBSEP metric], $i)
			}
		}
	' "$l_baseline_file" "$l_current_file" >"$l_compare_file"
}

zxfer_perf_compare_baseline() {
	[ -n "$ZXFER_PERF_BASELINE" ] || return 0
	[ -r "$ZXFER_PERF_BASELINE" ] || zxfer_perf_die "Baseline summary is not readable: $ZXFER_PERF_BASELINE"

	zxfer_perf_compare_summary_files "$ZXFER_PERF_BASELINE" "$ZXFER_PERF_SUMMARY_FILE" "$ZXFER_PERF_COMPARE_FILE" "$ZXFER_PERF_REGRESSION_THRESHOLD_PCT"

	if awk -F '\t' 'NR > 1 && $6 == "regression" { found = 1 } END { exit(found ? 0 : 1) }' "$ZXFER_PERF_COMPARE_FILE"; then
		zxfer_perf_warn "performance regressions were detected relative to $ZXFER_PERF_BASELINE; see $ZXFER_PERF_COMPARE_FILE"
	fi
}

zxfer_perf_main() {
	zxfer_perf_parse_args "$@"
	zxfer_perf_apply_profile_defaults
	zxfer_perf_confirm_once
	zxfer_perf_setup_output_dir
	zxfer_perf_log_configuration
	zxfer_perf_setup_pools
	zxfer_perf_write_run_info
	zxfer_perf_run_cases
	zxfer_perf_render_summary
	zxfer_perf_compare_baseline
	log_summary "Performance artifacts written to $ZXFER_PERF_OUTPUT_DIR"
}

if [ "${ZXFER_RUN_PERF_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_perf_main "$@"
fi
