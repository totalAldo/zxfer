#!/bin/sh
# shellcheck shell=sh
#
# Shared fake executables and reset helpers for snapshot-discovery behavior
# fragments. The stable suite entry point sources this file once.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

create_parallel_bin() {
	l_path=$1
	l_version_line=$2
	cat >"$l_path" <<EOF
#!/bin/sh
if [ "\$1" = "--version" ]; then
	printf '%s\n' "$l_version_line"
	exit 0
fi
exit 0
EOF
	chmod +x "$l_path"
}

create_functional_parallel_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
# Minimal GNU-parallel stand-in: runs the command after -- once per input
# line (replacing {}), serially, and exits nonzero when any job fails.
while [ $# -gt 0 ]; do
	case $1 in
	--)
		shift
		break
		;;
	*)
		shift
		;;
	esac
done
l_runner=$1
l_worst=0
while IFS= read -r l_line; do
	[ -n "$l_line" ] || continue
	l_expanded=$(printf '%s' "$l_runner" | sed "s|{}|$l_line|g")
	sh -c "$l_expanded" || l_worst=1
done
exit $l_worst
EOF
	chmod +x "$l_path"
}

create_discovery_fake_zfs_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
# Enumeration: zfs list -Hr -t filesystem,volume -o name SRC
if [ "$2" = "-Hr" ]; then
	if [ -n "${FAKE_ZFS_FAIL_ENUMERATION:-}" ]; then
		printf '%s\n' "cannot open source" >&2
		exit 1
	fi
	printf 'tank/src\ntank/src/a\ntank/src/b\n'
	exit 0
fi
# Per-dataset: zfs list -H -o name,guid -s creation -d 1 -t snapshot DS
l_dataset=${11}
case $l_dataset in
tank/src)
	printf 'tank/src@s1\t111\n'
	exit 0
	;;
tank/src/a)
	if [ -n "${FAKE_ZFS_FAIL_SUBLISTING:-}" ]; then
		printf '%s\n' "cannot open tank/src/a" >&2
		exit 1
	fi
	printf 'tank/src/a@s1\t222\n'
	exit 0
	;;
tank/src/b)
	printf 'tank/src/b@s1\t333\n'
	exit 0
	;;
esac
exit 1
EOF
	chmod +x "$l_path"
}

create_selective_awk_failure_bin() {
	l_path=$1
	l_exit_status=$2
	l_real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	cat >"$l_path" <<EOF
#!/bin/sh
if [ "\$1" = "-F@" ]; then
	printf '%s\n' "awk failed" >&2
	exit $l_exit_status
fi
exec "$l_real_awk" "\$@"
EOF
	chmod +x "$l_path"
}

create_fake_ssh_bin() {
	l_path=$1
	cat >"$l_path" <<'EOF'
#!/bin/sh
if [ "$1" = "-M" ] && [ "$2" = "-V" ]; then
	exit 1
fi
printf '%s\n' "$@"
exit 0
EOF
	chmod +x "$l_path"
}

create_fake_ssh_handshake_bin() {
	l_path=$1
	l_parallel_status=$2
	cat >"$l_path" <<EOF
#!/bin/sh
cat <<'INNER_EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	$l_parallel_status	$([ "$l_parallel_status" = "0" ] && printf '%s' /opt/bin/parallel || printf '%s' -)
tool	cat	0	/remote/bin/cat
end
INNER_EOF
EOF
	chmod +x "$l_path"
}

zxfer_test_start_fast_noop_destination_fifo_producer() {
	l_fifo=$1
	l_err_file=$2
	l_list_status_file=$3
	l_normalize_status_file=$4
	l_stream_status_file=$5
	l_payload=${ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED-}
	l_err_payload=${ZXFER_TEST_FAST_NOOP_DESTINATION_STDERR-}
	l_list_status=${ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS:-0}
	l_normalize_status=${ZXFER_TEST_FAST_NOOP_DESTINATION_NORMALIZE_STATUS:-0}
	l_stream_status=${ZXFER_TEST_FAST_NOOP_DESTINATION_STREAM_STATUS:-${ZXFER_TEST_FAST_NOOP_DESTINATION_SORT_STATUS:-0}}

	if [ "${ZXFER_TEST_FAST_NOOP_DESTINATION_SETUP_STATUS:-0}" -ne 0 ]; then
		return "$ZXFER_TEST_FAST_NOOP_DESTINATION_SETUP_STATUS"
	fi

	(
		if [ -n "$l_payload" ]; then
			printf '%s\n' "$l_payload" >"$l_fifo"
		else
			: >"$l_fifo"
		fi
		if [ -n "$l_err_payload" ]; then
			printf '%s\n' "$l_err_payload" >"$l_err_file"
		else
			: >"$l_err_file"
		fi
		printf '%s\n' "$l_list_status" >"$l_list_status_file"
		printf '%s\n' "$l_normalize_status" >"$l_normalize_status_file"
		printf '%s\n' "$l_stream_status" >"$l_stream_status_file"
	) &
	g_last_background_pid=$!
	zxfer_register_cleanup_pid "$g_last_background_pid" "test destination snapshot no-op proof helper"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_get_list"
	PARALLEL_BIN="$TEST_TMPDIR/parallel"
	ALT_PARALLEL_BIN="$TEST_TMPDIR/alt_parallel"
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_parallel_bin "$PARALLEL_BIN" "parallel (fake)"
	create_parallel_bin "$ALT_PARALLEL_BIN" "parallel from elsewhere"
	create_fake_ssh_bin "$FAKE_SSH_BIN"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

# Purpose: Reset public discovery options and root mappings.
# Usage: Called by setUp before every sourced behavior case.
zxfer_test_reset_snapshot_discovery_option_fixture() {
	TMPDIR="$TEST_TMPDIR"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_j_jobs=1
	g_option_O_origin_host=""
	g_option_R_recursive=""
	g_option_z_compress=0
	g_option_x_exclude_datasets=""
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_option_U_skip_unsupported_properties=0
	g_option_d_delete_destination_snapshots=0
	g_initial_source_had_trailing_slash=0
	g_initial_source="tank/src"
	g_destination="backup/dst"
}

# Purpose: Reset origin and target capability-cache fixtures.
# Usage: Called by setUp so remote capability tests never share probe state.
zxfer_test_reset_snapshot_discovery_remote_capability_fixture() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
}

# Purpose: Reset fake helper commands and rendered-command state.
# Usage: Called by setUp before producer, SSH, and ZFS command tests.
zxfer_test_reset_snapshot_discovery_command_fixture() {
	g_cmd_parallel="$PARALLEL_BIN"
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe="'zstd' '-3'"
	g_cmd_decompress_safe="'zstd' '-d'"
	g_origin_cmd_compress_safe=""
	g_origin_cmd_decompress_safe=""
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_RZFS="/sbin/zfs"
	g_LZFS="/sbin/zfs"
	g_cmd_zfs="/sbin/zfs"
	g_target_cmd_zfs=""
}

# Purpose: Reset discovery outputs, staged-result channels, and job scratch.
# Usage: Called by setUp before cache and orchestration behavior tests.
zxfer_test_reset_snapshot_discovery_result_fixture() {
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_recursive_dest_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_file_read_result=""
	g_zxfer_parallel_source_job_check_kind=""
	g_zxfer_recursive_dataset_list_result=""
	g_zxfer_linear_reverse_max_lines=""
	g_cmd_ps=${g_cmd_ps:-$(command -v ps 2>/dev/null || printf '%s\n' ps)}
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_last_background_pid=""
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_job_id=""
	g_source_snapshot_list_background_sort_requested=0
	g_source_snapshot_list_sorted_file=""
	g_zxfer_temp_file_result=""
}

setUp() {
	zxfer_test_reset_snapshot_discovery_option_fixture
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" || return "$?"
	zxfer_test_reset_snapshot_discovery_remote_capability_fixture
	zxfer_test_reset_snapshot_discovery_command_fixture
	zxfer_test_reset_snapshot_discovery_result_fixture
	zxfer_reset_background_job_state
	zxfer_reset_destination_existence_cache
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
}
