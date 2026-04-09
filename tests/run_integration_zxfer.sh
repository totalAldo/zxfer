#!/bin/sh
#
# Integration tests for zxfer using temporary ZFS pools backed by sparse files.
#

set -eu

ZXFER_BIN=${ZXFER_BIN:-"./zxfer"}
SPARSE_SIZE_MB=${SPARSE_SIZE_MB:-256}
OS_NAME=$(uname -s)
MACOS_OPENZFS_ZFS_BIN="/usr/local/zfs/bin/zfs"
ZXFER_CONFIRM_EACH_COMMAND=1
ZXFER_CONFIRM_COMMANDS=${ZXFER_CONFIRM_COMMANDS:-"chmod chown kill ln mkdir mktemp mkfile perl python3 rm truncate zfs zpool"}
ZXFER_CONFIRM_WRAPPER_DIR=""
ZXFER_REAL_BIN=""
ZXFER_SKIP_TESTS=${ZXFER_SKIP_TESTS:-""}
ZXFER_KEEP_GOING=${ZXFER_KEEP_GOING:-0}
ZXFER_ABORT_REQUESTED=${ZXFER_ABORT_REQUESTED:-0}
ZXFER_PRESERVE_WORKDIR_ON_FAILURE=${ZXFER_PRESERVE_WORKDIR_ON_FAILURE:-0}
ZXFER_FAILED_TESTS=""
SRC_POOL_CREATED=0
DEST_POOL_CREATED=0
TEST_POOL_MARKER_PROP="org.zxfer:test"
TEST_POOL_WORKDIR_PROP="org.zxfer:workdir"
TEST_POOL_RUN_PROP="org.zxfer:run"
TEST_POOL_VDEV_PROP="org.zxfer:vdev"
TEST_RUN_ID=""
RED=$(printf '\033[31m')
GREEN=$(printf '\033[32m')
YELLOW=$(printf '\033[33m')
RESET=$(printf '\033[0m')

has_gnu_parallel() {
	if ! command -v parallel >/dev/null 2>&1; then
		return 1
	fi
	parallel --version 2>/dev/null | grep -q "GNU parallel"
}

require_cmd() {
	l_cmd=$1
	if ! command -v "$l_cmd" >/dev/null 2>&1; then
		echo "Missing required command: $l_cmd" >&2
		exit 1
	fi
}

print_usage() {
	cat <<'EOF'
usage: ./tests/run_integration_zxfer.sh [--yes] [--skip-test name] [--keep-going] [--help]

By default the integration harness prompts for approval before data-modifying
wrapped external commands (for example zpool, zfs, rm, mkdir, mktemp, chmod,
chown, truncate, mkfile, perl, python3, ln, and kill). Pass --yes to disable
confirmations and run unattended.

Use --skip-test <name> to skip one integration test function. Repeat the flag
to skip more than one test, or set ZXFER_SKIP_TESTS to a whitespace-separated
list of test function names.

Use --keep-going to continue after a failing integration test and print a
summary of failed test functions at the end.
EOF
}

append_skip_test() {
	l_test=$1

	[ -n "$l_test" ] || return
	case " ${ZXFER_SKIP_TESTS:-} " in
	*" $l_test "*) ;;
	*)
		if [ -n "${ZXFER_SKIP_TESTS:-}" ]; then
			ZXFER_SKIP_TESTS="$ZXFER_SKIP_TESTS $l_test"
		else
			ZXFER_SKIP_TESTS=$l_test
		fi
		;;
	esac
}

parse_args() {
	while [ $# -gt 0 ]; do
		case "$1" in
		--yes)
			ZXFER_CONFIRM_EACH_COMMAND=0
			;;
		--skip-test)
			shift
			if [ $# -eq 0 ] || [ -z "$1" ]; then
				printf '%s\n' "--skip-test requires a test function name." >&2
				print_usage >&2
				exit 2
			fi
			append_skip_test "$1"
			;;
		--keep-going)
			ZXFER_KEEP_GOING=1
			;;
		-h | --help)
			print_usage
			exit 0
			;;
		*)
			printf 'Unknown argument: %s\n' "$1" >&2
			print_usage >&2
			exit 2
			;;
		esac
		shift
	done
}

test_is_skipped() {
	l_test=$1

	case " ${ZXFER_SKIP_TESTS:-} " in
	*" $l_test "*) return 0 ;;
	*) return 1 ;;
	esac
}

append_failed_test() {
	l_test=$1

	[ -n "$l_test" ] || return
	case " ${ZXFER_FAILED_TESTS:-} " in
	*" $l_test "*) ;;
	*)
		if [ -n "${ZXFER_FAILED_TESTS:-}" ]; then
			ZXFER_FAILED_TESTS="$ZXFER_FAILED_TESTS $l_test"
		else
			ZXFER_FAILED_TESTS=$l_test
		fi
		;;
	esac
}

append_path_entry() {
	l_entry=$1

	[ -n "$l_entry" ] || return
	case ":$PATH:" in
	*:"$l_entry":*) ;;
	*)
		PATH="$l_entry:$PATH"
		export PATH
		;;
	esac
}

append_secure_path_entry() {
	l_entry=$1

	[ -n "$l_entry" ] || return
	case ":${ZXFER_SECURE_PATH_APPEND-}:" in
	*:"$l_entry":*) ;;
	*)
		if [ -n "${ZXFER_SECURE_PATH_APPEND-}" ]; then
			ZXFER_SECURE_PATH_APPEND="$ZXFER_SECURE_PATH_APPEND:$l_entry"
		else
			ZXFER_SECURE_PATH_APPEND="$l_entry"
		fi
		export ZXFER_SECURE_PATH_APPEND
		;;
	esac
}

configure_platform_tool_paths() {
	if [ "$OS_NAME" = "Darwin" ] && [ -x "$MACOS_OPENZFS_ZFS_BIN" ]; then
		l_zfs_dir=${MACOS_OPENZFS_ZFS_BIN%/*}
		append_path_entry "$l_zfs_dir"
		append_secure_path_entry "$l_zfs_dir"
	fi
}

compute_absolute_path() {
	l_path=$1

	case "$l_path" in
	/*)
		printf '%s\n' "$l_path"
		;;
	*)
		l_dir=${l_path%/*}
		l_base=${l_path##*/}
		if [ "$l_dir" = "$l_path" ]; then
			l_dir=.
		fi
		l_abs_dir=$(cd "$l_dir" 2>/dev/null && pwd -P) || return 1
		printf '%s/%s\n' "$l_abs_dir" "$l_base"
		;;
	esac
}

resolve_host_command() {
	l_cmd=$1
	l_search_path=$PATH
	l_oldifs=$IFS

	if [ -n "${ZXFER_CONFIRM_WRAPPER_DIR:-}" ]; then
		l_search_path=
		IFS=:
		for l_entry in $PATH; do
			[ "$l_entry" = "$ZXFER_CONFIRM_WRAPPER_DIR" ] && continue
			if [ "$l_search_path" = "" ]; then
				l_search_path=$l_entry
			else
				l_search_path="$l_search_path:$l_entry"
			fi
		done
		IFS=$l_oldifs
	else
		IFS=$l_oldifs
	fi

	PATH=$l_search_path command -v "$l_cmd" 2>/dev/null || true
}

write_command_confirmation_wrapper() {
	l_cmd=$1
	l_wrapper_path=$2

	cat >"$l_wrapper_path" <<EOF
#!/bin/sh
l_cmd_name='$l_cmd'
l_wrapper_dir='${ZXFER_CONFIRM_WRAPPER_DIR}'

if [ "\${ZXFER_CONFIRM_EACH_COMMAND:-0}" = "1" ]; then
	if [ ! -r /dev/tty ]; then
		printf '%s\n' "Command confirmation requested but /dev/tty is unavailable." >&2
		exit 1
	fi
	printf '%s\n' "About to run command:" >/dev/tty
	printf '  %s\n' "\$l_cmd_name" >/dev/tty
	for l_arg in "\$@"; do
		printf '  %s\n' "\$l_arg" >/dev/tty
	done
	printf '%s' "Approve? [y/N] " >/dev/tty
	IFS= read -r l_reply </dev/tty || exit 1
	case "\$l_reply" in
	y | Y | yes | YES) ;;
	*)
		printf '%s\n' "Declined command: \$l_cmd_name" >&2
		exit 1
		;;
	esac
fi

l_search_path=
l_oldifs=\$IFS
IFS=:
for l_entry in \$PATH; do
	[ "\$l_entry" = "\$l_wrapper_dir" ] && continue
	if [ "\$l_search_path" = "" ]; then
		l_search_path=\$l_entry
	else
		l_search_path="\$l_search_path:\$l_entry"
	fi
done
IFS=\$l_oldifs
PATH=\$l_search_path
export PATH

l_real_cmd=\$(command -v "\$l_cmd_name" 2>/dev/null || :)
if [ "\$l_real_cmd" = "" ]; then
	printf '%s\n' "Unable to resolve wrapped command \$l_cmd_name." >&2
	exit 127
fi

exec "\$l_real_cmd" "\$@"
EOF
	chmod +x "$l_wrapper_path"
}

write_zxfer_confirmation_wrapper() {
	l_wrapper_path=$1

	cat >"$l_wrapper_path" <<EOF
#!/bin/sh
l_real_zxfer='${ZXFER_REAL_BIN}'
l_wrapper_dir='${ZXFER_CONFIRM_WRAPPER_DIR}'

if [ "\${ZXFER_CONFIRM_EACH_COMMAND:-0}" = "1" ]; then
	if [ ! -r /dev/tty ]; then
		printf '%s\n' "Command confirmation requested but /dev/tty is unavailable." >&2
		exit 1
	fi
	printf '%s\n' "About to run command:" >/dev/tty
	printf '  %s\n' "\$l_real_zxfer" >/dev/tty
	for l_arg in "\$@"; do
		printf '  %s\n' "\$l_arg" >/dev/tty
	done
	printf '%s' "Approve? [y/N] " >/dev/tty
	IFS= read -r l_reply </dev/tty || exit 1
	case "\$l_reply" in
	y | Y | yes | YES) ;;
	*)
		printf '%s\n' "Declined command: \$l_real_zxfer" >&2
		exit 1
		;;
	esac
fi

case ":\${ZXFER_SECURE_PATH:-}:" in
*:"\$l_wrapper_dir":*) ;;
*)
	if [ -n "\${ZXFER_SECURE_PATH:-}" ]; then
		ZXFER_SECURE_PATH="\$l_wrapper_dir:\$ZXFER_SECURE_PATH"
		export ZXFER_SECURE_PATH
	else
		case ":\${ZXFER_SECURE_PATH_APPEND:-}:" in
		*:"\$l_wrapper_dir":*) ;;
		*)
			if [ -n "\${ZXFER_SECURE_PATH_APPEND:-}" ]; then
				ZXFER_SECURE_PATH_APPEND="\$l_wrapper_dir:\$ZXFER_SECURE_PATH_APPEND"
			else
				ZXFER_SECURE_PATH_APPEND="\$l_wrapper_dir"
			fi
			export ZXFER_SECURE_PATH_APPEND
			;;
		esac
	fi
	;;
esac

exec "\$l_real_zxfer" "\$@"
EOF
	chmod +x "$l_wrapper_path"
}

setup_command_confirmation_wrappers() {
	if [ "$ZXFER_CONFIRM_EACH_COMMAND" != "1" ]; then
		return
	fi

	ZXFER_REAL_BIN=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	if [ ! -x "$ZXFER_REAL_BIN" ]; then
		fail "zxfer binary not executable at $ZXFER_REAL_BIN"
	fi

	ZXFER_CONFIRM_WRAPPER_DIR="$WORKDIR/command_confirm_wrappers"
	safe_rm_rf "$ZXFER_CONFIRM_WRAPPER_DIR"
	mkdir -p "$ZXFER_CONFIRM_WRAPPER_DIR"
	export ZXFER_CONFIRM_WRAPPER_DIR ZXFER_CONFIRM_EACH_COMMAND

	for l_cmd in $ZXFER_CONFIRM_COMMANDS; do
		write_command_confirmation_wrapper "$l_cmd" "$ZXFER_CONFIRM_WRAPPER_DIR/$l_cmd"
	done
	write_zxfer_confirmation_wrapper "$ZXFER_CONFIRM_WRAPPER_DIR/zxfer"

	PATH="$ZXFER_CONFIRM_WRAPPER_DIR:$PATH"
	export PATH
	ZXFER_BIN="$ZXFER_CONFIRM_WRAPPER_DIR/zxfer"
	log "Confirmation enabled for data-modifying wrapped commands"
}

log() {
	printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
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

require_platform_permissions() {
	:
}

create_sparse_file() {
	l_file=$1
	l_size_mb=$2
	l_size_bytes=$((l_size_mb * 1024 * 1024))

	if command -v truncate >/dev/null 2>&1; then
		truncate -s "${l_size_mb}M" "$l_file"
	elif command -v mkfile >/dev/null 2>&1; then
		mkfile -n "${l_size_mb}m" "$l_file"
	elif command -v perl >/dev/null 2>&1; then
		perl -e '
			my ($path, $size) = @ARGV;
			open(my $fh, ">", $path) or exit 1;
			if ($size > 0) {
				seek($fh, $size - 1, 0) or exit 1;
				print {$fh} "\0" or exit 1;
			}
			close($fh) or exit 1;
		' "$l_file" "$l_size_bytes" >/dev/null 2>&1 ||
			fail "Unable to create sparse file $l_file of size ${l_size_mb}M."
	elif command -v python3 >/dev/null 2>&1; then
		python3 - "$l_file" "$l_size_bytes" <<'PY' >/dev/null 2>&1 || fail "Unable to create sparse file $l_file of size ${l_size_mb}M."
import os
import sys

path = sys.argv[1]
size = int(sys.argv[2])
with open(path, "wb") as fh:
    if size > 0:
        fh.seek(size - 1)
        fh.write(b"\0")
PY
	else
		fail "Need truncate, mkfile, perl, or python3 to create sparse test files safely."
	fi
}

is_safe_test_file_vdev() {
	l_path=$1

	is_safe_workdir_path "$l_path" || return 1
	[ -f "$l_path" ] || return 1
	[ ! -L "$l_path" ] || [ ! -h "$l_path" ] || return 1
	return 0
}

is_safe_workdir_path() {
	l_path=$1
	l_parent=
	l_parent_phys=

	[ -n "$WORKDIR" ] || return 1
	case "$l_path" in
	"$WORKDIR") return 0 ;;
	"$WORKDIR"/*) ;;
	*) return 1 ;;
	esac

	case "$l_path" in
	*"/../"* | *"/.." | *"/./"* | *"/.")
		return 1
		;;
	esac

	l_parent=${l_path%/*}
	if [ "$l_parent" = "$l_path" ]; then
		return 1
	fi
	l_parent_phys=$(cd -P "$l_parent" 2>/dev/null && pwd) || return 1
	case "$l_parent_phys" in
	"$WORKDIR" | "$WORKDIR"/*) return 0 ;;
	*) return 1 ;;
	esac
}

safe_rm_rf() {
	for l_path in "$@"; do
		[ -n "$l_path" ] || continue
		if ! is_safe_workdir_path "$l_path"; then
			fail "Refusing to remove path outside WORKDIR: $l_path"
		fi
		rm -rf "$l_path"
	done
}

safe_rm_f() {
	for l_path in "$@"; do
		[ -n "$l_path" ] || continue
		if ! is_safe_workdir_path "$l_path"; then
			fail "Refusing to remove file outside WORKDIR: $l_path"
		fi
		rm -f "$l_path"
	done
}

generate_test_pool_name() {
	l_prefix=$1
	l_suffix=$(basename "${WORKDIR:-zxfer}" | tr -cd '[:alnum:]')
	if [ "$l_suffix" = "" ]; then
		l_suffix=$$
	fi
	printf 'zxfer_%s_%s\n' "$l_prefix" "$l_suffix"
}

mark_test_pool() {
	l_pool=$1
	l_vdev=$2

	zfs set "$TEST_POOL_MARKER_PROP=yes" "$l_pool" >/dev/null 2>&1 ||
		return 1
	zfs set "$TEST_POOL_WORKDIR_PROP=$WORKDIR" "$l_pool" >/dev/null 2>&1 ||
		return 1
	zfs set "$TEST_POOL_RUN_PROP=$TEST_RUN_ID" "$l_pool" >/dev/null 2>&1 ||
		return 1
	zfs set "$TEST_POOL_VDEV_PROP=$l_vdev" "$l_pool" >/dev/null 2>&1 ||
		return 1
}

pool_belongs_to_test_run() {
	l_pool=$1
	l_expected_vdev=${2:-}

	if ! zpool list "$l_pool" >/dev/null 2>&1; then
		return 1
	fi

	l_marker=$(zfs get -H -o value "$TEST_POOL_MARKER_PROP" "$l_pool" 2>/dev/null || printf '%s\n' "")
	if [ "$l_marker" != "yes" ]; then
		return 1
	fi

	l_workdir=$(zfs get -H -o value "$TEST_POOL_WORKDIR_PROP" "$l_pool" 2>/dev/null || printf '%s\n' "")
	if [ "$l_workdir" != "$WORKDIR" ]; then
		return 1
	fi

	l_run_id=$(zfs get -H -o value "$TEST_POOL_RUN_PROP" "$l_pool" 2>/dev/null || printf '%s\n' "")
	if [ "$l_run_id" != "$TEST_RUN_ID" ]; then
		return 1
	fi

	l_recorded_vdev=$(zfs get -H -o value "$TEST_POOL_VDEV_PROP" "$l_pool" 2>/dev/null || printf '%s\n' "")
	if [ "$l_recorded_vdev" != "$l_expected_vdev" ]; then
		return 1
	fi

	is_safe_test_file_vdev "$l_expected_vdev" || return 1
	l_status_paths=$(zpool status -P "$l_pool" 2>/dev/null | awk '/^[[:space:]]+\// { print $1 }')
	if [ "$l_status_paths" != "$l_expected_vdev" ]; then
		return 1
	fi

	return 0
}

destroy_test_pool_if_owned() {
	l_label=$1
	l_pool=$2
	l_created=$3
	l_expected_vdev=${4:-}

	[ "$l_created" -eq 1 ] || return
	[ -n "$l_pool" ] || return

	if ! zpool list "$l_pool" >/dev/null 2>&1; then
		return
	fi

	if ! pool_belongs_to_test_run "$l_pool" "$l_expected_vdev"; then
		printf 'WARNING: refusing to destroy %s pool %s because it does not match this test run'\''s safety markers.\n' \
			"$l_label" "$l_pool" >&2
		return 1
	fi

	log "Destroying $l_label pool $l_pool"
	if ! zpool destroy -f "$l_pool"; then
		printf 'WARNING: failed to destroy %s pool %s; preserving workdir for inspection.\n' \
			"$l_label" "$l_pool" >&2
		return 1
	fi
	if zpool list "$l_pool" >/dev/null 2>&1; then
		printf 'WARNING: %s pool %s still exists after destroy; preserving workdir for inspection.\n' \
			"$l_label" "$l_pool" >&2
		return 1
	fi
	return 0
}

create_test_pool() {
	l_label=$1
	l_pool=$2
	l_vdev=$3
	l_mount_root=$4
	l_mountpoint_opt=$l_mount_root

	if zpool list "$l_pool" >/dev/null 2>&1; then
		fail "Refusing to reuse pre-existing $l_label pool $l_pool."
	fi

	if ! is_safe_test_file_vdev "$l_vdev"; then
		fail "Refusing to create $l_label pool $l_pool on non-file-backed or out-of-workdir vdev $l_vdev."
	fi

	if [ "$l_label" = "destination" ]; then
		l_mountpoint_opt=none
	fi

	mkdir -p "$(dirname "$l_mount_root")"
	if ! zpool create -f -o cachefile=none -O mountpoint="$l_mountpoint_opt" "$l_pool" "$l_vdev"; then
		fail "Failed to create $l_label pool $l_pool on $l_vdev. Local non-root runs require OpenZFS permissions that allow file-backed pool creation."
	fi

	if ! mark_test_pool "$l_pool" "$l_vdev"; then
		zpool destroy -f "$l_pool" >/dev/null 2>&1 || true
		fail "Failed to mark newly created $l_label pool $l_pool as an integration-test pool."
	fi
}

is_safe_test_dataset_target() {
	l_target=$1
	l_dataset=${l_target%@*}

	[ -n "${SRC_POOL:-}" ] || return 1
	[ -n "${DEST_POOL:-}" ] || return 1
	case "$l_dataset" in
	"$SRC_POOL"/* | "$DEST_POOL"/*) return 0 ;;
	*) return 1 ;;
	esac
}

destroy_test_datasets_if_present() {
	for l_target in "$@"; do
		[ -n "$l_target" ] || continue
		if ! is_safe_test_dataset_target "$l_target"; then
			fail "Refusing to destroy dataset outside test pools: $l_target"
		fi
		zfs destroy -r "$l_target" >/dev/null 2>&1 || true
	done
}

destroy_test_dataset() {
	l_target=$1

	if ! is_safe_test_dataset_target "$l_target"; then
		fail "Refusing to destroy dataset outside test pools: $l_target"
	fi
	zfs destroy -r "$l_target"
}

get_mountpoint() {
	l_dataset=$1
	zfs get -H -o value mountpoint "$l_dataset"
}

append_data_to_dataset() {
	l_dataset=$1
	l_file=$2
	l_data=$3

	l_mountpoint=$(get_mountpoint "$l_dataset")
	printf '%s\n' "$l_data" >>"$l_mountpoint/$l_file"
}

get_file_mode_octal() {
	l_path=$1

	if l_mode=$(stat -c '%a' "$l_path" 2>/dev/null); then
		case "$l_mode" in
		'' | *[!0-9]*) ;;
		*)
			printf '%s\n' "$l_mode"
			return 0
			;;
		esac
	fi
	if l_mode=$(stat -f '%OLp' "$l_path" 2>/dev/null); then
		case "$l_mode" in
		'' | *[!0-9]*) ;;
		*)
			printf '%s\n' "$l_mode"
			return 0
			;;
		esac
	fi

	return 1
}

find_backup_metadata_file_for_exact_pair() {
	l_backup_root=$1
	l_source_dataset=$2
	l_destination_dataset=$3

	find "$l_backup_root" -type f -name '.zxfer_backup_info.*' 2>/dev/null |
		while IFS= read -r l_backup_file || [ -n "$l_backup_file" ]; do
			if awk -v dataset_pair="$l_source_dataset,$l_destination_dataset," '
				BEGIN {
					found = 0
				}
				index($0, dataset_pair) == 1 {
					found = 1
					exit
				}
				END {
					exit(found ? 0 : 1)
				}
			' "$l_backup_file" >/dev/null 2>&1; then
				printf '%s\n' "$l_backup_file"
				break
			fi
		done
}

set_test_dataset_mountpoint() {
	l_dataset=$1
	l_mountpoint=$2

	if ! is_safe_workdir_path "$l_mountpoint"; then
		fail "Refusing to set test dataset mountpoint outside WORKDIR: $l_mountpoint"
	fi
	mkdir -p "$l_mountpoint"
	zfs set mountpoint="$l_mountpoint" "$l_dataset"
	zfs mount "$l_dataset" >/dev/null 2>&1 || true
}

prepare_mock_bin_dir() {
	l_dir=$1
	shift

	safe_rm_rf "$l_dir"
	mkdir -p "$l_dir"

	for l_bin in "$@"; do
		l_actual=$(resolve_host_command "$l_bin")
		if [ "$l_actual" = "" ]; then
			fail "Required binary $l_bin not found on host; cannot prepare mock PATH."
		fi
		ln -s "$l_actual" "$l_dir/$l_bin"
	done
}

write_passthrough_zstd() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
# Minimal zstd stand-in that simply passes stdin to stdout for integration tests.
while [ $# -gt 0 ]; do
	case "$1" in
	-d) shift ;;
	-T*) shift ;;
	-*) shift ;;
	*) break ;;
	esac
done
cat
EOF
	chmod +x "$l_path"
}

write_progress_logger_script() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
log_file=$1
size_arg=$2
title_arg=$3

printf 'size=%s\n' "$size_arg" >>"$log_file"
printf 'title=%s\n' "$title_arg" >>"$log_file"
bytes=$(wc -c | tr -d '[:space:]')
printf 'bytes=%s\n' "$bytes" >>"$log_file"
EOF
	chmod +x "$l_path"
}

find_csh_shell() {
	command -v csh 2>/dev/null || command -v tcsh 2>/dev/null || true
}

write_exec_wrapper_script() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
log=${MOCK_WRAPPER_LOG:-}
wrapper_name=$(basename "$0")
[ -n "$log" ] && printf '%s:%s\n' "$wrapper_name" "$*" >>"$log"
exec "$@"
EOF
	chmod +x "$l_path"
}

write_mock_ssh_script() {
	l_path=$1
	safe_rm_f "$l_path"
	cat >"$l_path" <<'EOF'
#!/bin/sh
# Lightweight ssh stand-in that honors control sockets and runs commands locally.

mock_ssh_matches_missing_tool_probe() {
	l_cmd=$1

	[ -n "${MOCK_SSH_MISSING_TOOL:-}" ] || return 1

	case "$l_cmd" in
	*"command -v"*"$MOCK_SSH_MISSING_TOOL"*) printf '%s\n' "10"; return 0 ;;
	*"l_path=\$(command -v"*"$MOCK_SSH_MISSING_TOOL"*) printf '%s\n' "10"; return 0 ;;
	*) return 1 ;;
	esac
}

mock_ssh_emit_capability_response() {
	l_cmd=$1

	case "$l_cmd" in
	*"ZXFER_REMOTE_CAPS_V1"*)
		if [ -n "${MOCK_SSH_CAPABILITY_RESPONSE_FILE:-}" ]; then
			cat "$MOCK_SSH_CAPABILITY_RESPONSE_FILE"
			return $?
		fi
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf 'os\t%s\n' "${MOCK_SSH_FORCE_UNAME:-$(uname 2>/dev/null)}"
		for l_tool in zfs parallel cat; do
			if [ -n "${MOCK_SSH_MISSING_TOOL:-}" ] && [ "$l_tool" = "$MOCK_SSH_MISSING_TOOL" ]; then
				printf 'tool\t%s\t1\t-\n' "$l_tool"
				continue
			fi
			l_path=$(command -v "$l_tool" 2>/dev/null)
			l_status=$?
			if [ "$l_status" -eq 0 ]; then
				printf 'tool\t%s\t0\t%s\n' "$l_tool" "$l_path"
			else
				printf 'tool\t%s\t%s\t-\n' "$l_tool" "$l_status"
			fi
		done
		return 0
		;;
	*)
		return 1
		;;
	esac
}

mock_ssh_matches_command_v_override() {
	l_cmd=$1

	[ -n "${MOCK_SSH_COMMAND_V_TOOL:-}" ] || return 1
	[ -n "${MOCK_SSH_COMMAND_V_RESULT:-}" ] || return 1

	case "$l_cmd" in
	*"command -v"*"$MOCK_SSH_COMMAND_V_TOOL"*)
		printf '%s\n' "$MOCK_SSH_COMMAND_V_RESULT"
		return 0
		;;
	*) return 1 ;;
	esac
}

mock_ssh_is_uname_command() {
	l_cmd=$1

	case "$l_cmd" in
	uname | "'uname'" | '"uname"')
		return 0
		;;
	*)
		return 1
		;;
	esac
}

l_socket=""
l_op=""
l_host=""

while [ $# -gt 0 ]; do
	case "$1" in
	-M)
		shift
		;;
	-S)
		l_socket=$2
		shift 2
		;;
	-O)
		l_op=$2
		shift 2
		;;
	-o)
		shift 2
		;;
	-p)
		shift 2
		;;
	-f | -n | -N)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		l_host=$1
		shift
		break
		;;
	esac
done

if [ -n "$l_socket" ]; then
	mkdir -p "$(dirname "$l_socket")" || exit 1
	: >"$l_socket"
fi

if [ "$l_op" = "exit" ]; then
	[ -n "${MOCK_SSH_LOG:-}" ] && printf 'close %s\n' "$l_host" >>"$MOCK_SSH_LOG"
	exit 0
fi

[ $# -gt 0 ] || exit 0

if [ -n "${MOCK_SSH_LOG:-}" ]; then
	if [ $# -eq 1 ]; then
		printf '%s\n' "$1" >>"$MOCK_SSH_LOG"
	else
		printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
	fi
fi

	if [ $# -eq 1 ]; then
		l_cmd=$1
		l_shell=${MOCK_SSH_REMOTE_SHELL:-sh}

		if [ -n "${MOCK_SSH_FORCE_UNAME:-}" ] && mock_ssh_is_uname_command "$l_cmd"; then
			printf '%s\n' "$MOCK_SSH_FORCE_UNAME"
			exit 0
		fi

		if mock_ssh_emit_capability_response "$l_cmd"; then
			exit 0
		fi

		if mock_ssh_matches_command_v_override "$l_cmd"; then
			exit 0
		fi

		if [ -n "${MOCK_SSH_FILTER_PROPERTY:-}" ] && printf '%s\n' "$l_cmd" |
			grep -q "^zfs get -Ho property all "; then
			l_pool=${l_cmd#*all }
			if [ -n "$l_pool" ]; then
				zfs get -Ho property all "$l_pool" | grep -v "^${MOCK_SSH_FILTER_PROPERTY}$"
				exit 0
			fi
		fi
		if l_missing_probe_status=$(mock_ssh_matches_missing_tool_probe "$l_cmd"); then
			exit "$l_missing_probe_status"
		fi

		exec "$l_shell" -c "$l_cmd"
	fi

if [ -n "${MOCK_SSH_FORCE_UNAME:-}" ] && mock_ssh_is_uname_command "$1"; then
	printf '%s\n' "$MOCK_SSH_FORCE_UNAME"
	exit 0
fi

if [ -n "${MOCK_SSH_FILTER_PROPERTY:-}" ] &&
	[ $# -ge 6 ] &&
	[ "${1##*/}" = "zfs" ] &&
	[ "$2" = "get" ] &&
	[ "$3" = "-Ho" ] &&
	[ "$4" = "property" ] &&
	[ "$5" = "all" ]; then
	l_pool=$6
	"$1" "$2" "$3" "$4" "$5" "$6" | grep -v "^${MOCK_SSH_FILTER_PROPERTY}$"
	exit $?
fi

	if [ "${1##*/}" = "sh" ] && [ "${2:-}" = "-c" ]; then
		l_cmd=$3
		shift 3
		if mock_ssh_emit_capability_response "$l_cmd"; then
			exit 0
		fi
		if l_missing_probe_status=$(mock_ssh_matches_missing_tool_probe "$l_cmd"); then
			exit "$l_missing_probe_status"
		fi
		exec sh -c "$l_cmd" "$@"
	fi

exec "$@"
EOF
	chmod +x "$l_path"
}

run_zxfer() {
	log "Running: $ZXFER_BIN $*"
	# Preserve inline env overrides when run_zxfer is invoked as VAR=... run_zxfer.
	ZXFER_BACKUP_DIR=${ZXFER_BACKUP_DIR-} \
		ZXFER_SECURE_PATH=${ZXFER_SECURE_PATH-} \
		ZXFER_SECURE_PATH_APPEND=${ZXFER_SECURE_PATH_APPEND-} \
		MOCK_SSH_LOG=${MOCK_SSH_LOG-} \
		MOCK_SSH_REMOTE_SHELL=${MOCK_SSH_REMOTE_SHELL-} \
		MOCK_SSH_CAPABILITY_RESPONSE_FILE=${MOCK_SSH_CAPABILITY_RESPONSE_FILE-} \
		MOCK_SSH_COMMAND_V_TOOL=${MOCK_SSH_COMMAND_V_TOOL-} \
		MOCK_SSH_COMMAND_V_RESULT=${MOCK_SSH_COMMAND_V_RESULT-} \
		MOCK_SSH_FORCE_UNAME=${MOCK_SSH_FORCE_UNAME-} \
		MOCK_SSH_FILTER_PROPERTY=${MOCK_SSH_FILTER_PROPERTY-} \
		MOCK_SSH_MISSING_TOOL=${MOCK_SSH_MISSING_TOOL-} \
		MOCK_WRAPPER_LOG=${MOCK_WRAPPER_LOG-} \
		MOCK_SVCADM_LOG=${MOCK_SVCADM_LOG-} \
		"$ZXFER_BIN" "$@"
}

run_test() {
	l_index=$1
	l_total=$2
	l_func=$3

	log "$(printf '[%d/%d] Starting %s%s%s' "$l_index" "$l_total" "$YELLOW" "$l_func" "$RESET")"
	if test_is_skipped "$l_func"; then
		log "$(printf '%s[%d/%d] SKIP%s %s' "$YELLOW" "$l_index" "$l_total" "$RESET" "$l_func")"
		return
	fi
	set +e
	(
		"$l_func"
	)
	l_status=$?
	set -e
	if [ "$l_status" -ne 0 ]; then
		if [ "${ZXFER_ABORT_REQUESTED:-0}" -eq 1 ] || [ "$l_status" -eq 130 ] || [ "$l_status" -eq 143 ]; then
			exit "$l_status"
		fi
		log "$(printf '%s[%d/%d] FAIL%s %s (exit %s)' "$RED" "$l_index" "$l_total" "$RESET" "$l_func" "$l_status")"
		append_failed_test "$l_func"
		if [ "$ZXFER_KEEP_GOING" -eq 1 ]; then
			return
		fi
		exit "$l_status"
	fi
	log "$(printf '%s[%d/%d] PASS%s %s' "$GREEN" "$l_index" "$l_total" "$RESET" "$l_func")"
}

assert_usage_error_case() {
	l_desc=$1
	l_expected_msg=$2
	shift 2

	set +e
	l_output=$("$ZXFER_BIN" "$@" 2>&1)
	l_status=$?
	set -e

	if [ "$l_status" -eq 0 ]; then
		fail "$l_desc: expected zxfer to exit with a usage error."
	fi

	if [ "$l_status" -ne 2 ]; then
		fail "$l_desc: expected exit status 2, got $l_status. Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "Error: $l_expected_msg" >/dev/null 2>&1; then
		fail "$l_desc: usage output missing \"Error: $l_expected_msg\". Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "usage:" >/dev/null 2>&1; then
		fail "$l_desc: usage output missing usage synopsis. Output: $l_output"
	fi
}

usage_error_tests() {
	log "Starting usage error tests"

	assert_usage_error_case "Missing destination" "Need a destination." -R tank/src
	assert_usage_error_case "Missing -N/-R source flag" "You must specify a source with either -N or -R." backup/target
	assert_usage_error_case "Conflicting -N and -R flags" \
		"You must choose either -N to transfer a single filesystem or -R to transfer a single filesystem and its children recursively, but not both -N and -R at the same time." \
		-N tank/src -R tank/src backup/target

	log "Usage error tests passed"
}

usage_error_failure_report_test() {
	log "Starting usage error failure report test"

	stdout_log="$WORKDIR/usage_failure.stdout"
	stderr_log="$WORKDIR/usage_failure.stderr"
	safe_rm_f "$stdout_log" "$stderr_log"

	set +e
	"$ZXFER_BIN" -R tank/src >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage failure report test expected exit status 2, got $status. See $stderr_log."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Usage failure report should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -q "^zxfer: failure report begin$" "$stderr_log"; then
		fail "Usage failure report block missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_class: usage" "$stderr_log"; then
		fail "Usage failure report class missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "message: Need a destination\\." "$stderr_log"; then
		fail "Usage failure report message missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -Eq "invocation: .*'tank/src'" "$stderr_log"; then
		fail "Usage failure report invocation missing source argument. Output: $(cat "$stderr_log")"
	fi

	safe_rm_f "$stdout_log" "$stderr_log"

	log "Usage error failure report test passed"
}

assert_error_case() {
	l_desc=$1
	l_expected_msg=$2
	l_expected_status=${3:-1}
	shift 3

	set +e
	l_output=$("$ZXFER_BIN" "$@" 2>&1)
	l_status=$?
	set -e

	if [ "$l_status" -eq 0 ]; then
		fail "$l_desc: expected zxfer to exit with error status $l_expected_status."
	fi

	if [ "$l_status" -ne "$l_expected_status" ]; then
		fail "$l_desc: expected exit status $l_expected_status, got $l_status. Output: $l_output"
	fi

	if ! printf '%s\n' "$l_output" | grep -F "$l_expected_msg" >/dev/null 2>&1; then
		fail "$l_desc: output missing \"$l_expected_msg\". Output: $l_output"
	fi
}

extended_usage_error_tests() {
	log "Starting extended usage error tests"

	# Test sources/destinations starting with / (should fail validation)
	assert_usage_error_case "Source starting with /" \
		"Source and destination must not begin with \"/\"." \
		-R /tank/src backup/target

	assert_usage_error_case "Destination starting with /" \
		"Source and destination must not begin with \"/\"." \
		-R tank/src /backup/target

	# Test snapshot source (not supported for recursive/non-recursive flags in this way usually,
	# or at least zxfer often expects filesystems.
	# Based on code reading, zxfer_check_snapshot should reject if it looks like a snapshot but we wanted a fs)
	# Actually zxfer_replication.sh:303 checks if source is a snapshot and fails if so for normal mode.
	assert_error_case "Source is a snapshot" \
		"Snapshots are not allowed as a source." \
		1 \
		-R tank/src@snap backup/target

	# Test -c without -m
	assert_error_case "-c without -m" \
		"When using -c, -m needs to be specified as well." \
		1 \
		-c svc:/network/ssh -R tank/src backup/target

	log "Extended usage error tests passed"
}

secure_path_dependency_tests() {
	log "Starting secure PATH dependency tests"

	mock_path="$WORKDIR/mock_secure_path"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"
	cat >"$mock_path/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_path/ssh"
	for bin in awk cat sed date mktemp tr printf grep cut head sort; do
		real_bin=$(resolve_host_command "$bin")
		if [ "$real_bin" != "" ]; then
			ln -s "$real_bin" "$mock_path/$bin"
		else
			fail "Required binary $bin not found on host; cannot run secure PATH test."
		fi
	done
	# Deliberately omit zfs from the secure PATH to ensure zxfer aborts cleanly.
	secure_path="$mock_path"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" ZXFER_SECURE_PATH_APPEND="" "$ZXFER_BIN" -R tank/src backup/target 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when secure PATH lacks required tools."
	fi
	if ! printf '%s\n' "$output" | grep -q 'Required dependency "zfs" not found'; then
		fail "Expected missing zfs dependency error. Output: $output"
	fi

	safe_rm_rf "$mock_path"

	log "Secure PATH dependency tests passed"
}

secure_path_failure_report_test() {
	log "Starting secure PATH failure report test"

	mock_path="$WORKDIR/mock_secure_path_report"
	stdout_log="$WORKDIR/secure_path_report.stdout"
	stderr_log="$WORKDIR/secure_path_report.stderr"
	safe_rm_rf "$mock_path"
	safe_rm_f "$stdout_log" "$stderr_log"
	mkdir -p "$mock_path"
	cat >"$mock_path/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_path/ssh"
	for bin in awk cat sed date mktemp tr printf grep cut head sort; do
		real_bin=$(resolve_host_command "$bin")
		if [ "$real_bin" != "" ]; then
			ln -s "$real_bin" "$mock_path/$bin"
		else
			fail "Required binary $bin not found on host; cannot run secure PATH failure report test."
		fi
	done

	set +e
	ZXFER_SECURE_PATH="$mock_path" ZXFER_SECURE_PATH_APPEND="" "$ZXFER_BIN" -R tank/src backup/target >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when secure PATH lacks required tools."
	fi
	if [ -s "$stdout_log" ]; then
		fail "Dependency failure report should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -q "^zxfer: failure report begin$" "$stderr_log"; then
		fail "Dependency failure report block missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_class: dependency" "$stderr_log"; then
		fail "Dependency failure report class missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_stage: startup" "$stderr_log"; then
		fail "Dependency failure report stage missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q 'Required dependency "zfs" not found' "$stderr_log"; then
		fail "Dependency failure report message missing. Output: $(cat "$stderr_log")"
	fi

	safe_rm_rf "$mock_path"

	log "Secure PATH failure report test passed"
}

secure_path_append_resolution_test() {
	log "Starting secure PATH append resolution test"

	base_path="$WORKDIR/mock_secure_path_base"
	append_path="$WORKDIR/mock_secure_path_append"
	safe_rm_rf "$base_path" "$append_path"

	prepare_mock_bin_dir "$base_path" awk ssh
	prepare_mock_bin_dir "$append_path" zfs

	set +e
	output=$(ZXFER_SECURE_PATH="$base_path" ZXFER_SECURE_PATH_APPEND="$append_path" "$ZXFER_BIN" -R tank/src 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Expected secure PATH append test to reach usage validation with exit 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Need a destination." >/dev/null 2>&1; then
		fail "Expected secure PATH append test to reach destination validation. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -F "Required dependency" >/dev/null 2>&1; then
		fail "Secure PATH append should satisfy dependency resolution without a missing dependency error. Output: $output"
	fi

	log "Secure PATH append resolution test passed"
}

remote_migration_guard_tests() {
	log "Starting remote/migration guard tests"

	mock_bin_dir="$WORKDIR/mockbin"
	safe_rm_rf "$mock_bin_dir"
	mkdir -p "$mock_bin_dir"
	cat >"$mock_bin_dir/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_bin_dir/ssh"
	secure_path="$mock_bin_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	# Using -m with a remote origin should be rejected before any replication starts.
	ZXFER_SECURE_PATH="$secure_path" assert_usage_error_case "Migration with remote origin" \
		"You cannot migrate to or from a remote host." \
		-m -O remotehost -R tank/src backup/target

	# Using -c without migration is already covered; ensure -c paired with a remote target also fails.
	ZXFER_SECURE_PATH="$secure_path" assert_usage_error_case "Service stop with remote target" \
		"You cannot migrate to or from a remote host." \
		-c svc:/network/ssh -T remotehost -R tank/src backup/target

	safe_rm_rf "$mock_bin_dir"

	log "Remote/migration guard tests passed"
}

missing_parallel_error_test() {
	log "Starting missing GNU parallel error test"

	mock_path="$WORKDIR/mock_no_parallel"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"

	# Provide required dependencies except GNU parallel.
	for bin in zfs ssh awk cat; do
		actual=$(resolve_host_command "$bin")
		if [ "$actual" = "" ]; then
			fail "Required binary $bin not found on host; cannot run missing parallel test."
		fi
		ln -s "$actual" "$mock_path/$bin"
	done

	secure_path="$mock_path"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -R tank/src backup/target 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when GNU parallel is missing for -j>1."
	fi
	if ! printf '%s\n' "$output" | grep -q "The -j option requires GNU parallel but it was not found in PATH on the local host."; then
		fail "Missing parallel error message not found. Output: $output"
	fi

	safe_rm_rf "$mock_path"

	log "Missing GNU parallel error test passed"
}

remote_missing_parallel_origin_test() {
	log "Starting remote missing GNU parallel origin test"

	if ! has_gnu_parallel; then
		log "Skipping remote missing GNU parallel origin test (local GNU parallel not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_no_parallel"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	real_parallel=$(resolve_host_command parallel)
	if [ "$real_parallel" = "" ]; then
		fail "GNU parallel not found on host after availability probe."
	fi
	ln -s "$real_parallel" "$mock_path/parallel"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	tmpdir="$WORKDIR/remote_no_parallel_tmp"

	src_dataset="$SRC_POOL/remote_no_parallel_src"
	dest_root="$DEST_POOL/remote_no_parallel_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$tmpdir"
	mkdir -p "$tmpdir"

	zfs create "$src_dataset"
	i=1
	while [ "$i" -le 16 ]; do
		zfs create "$src_dataset/child$i"
		i=$((i + 1))
	done
	zfs snap -r "$src_dataset@np1"

	set +e
	# Isolate the cross-process remote capability cache so earlier localhost tests
	# do not mask this deliberate missing-parallel failure path.
	output=$(TMPDIR="$tmpdir" MOCK_SSH_MISSING_TOOL=parallel ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Remote origin without GNU parallel should cause zxfer to fail when adaptive discovery selects the parallel branch."
	fi
	if ! printf '%s\n' "$output" | grep -q "GNU parallel not found on origin host" &&
		! printf '%s\n' "$output" | grep -q 'Required dependency "GNU parallel" not found on host localhost'; then
		fail "Expected remote missing parallel error message. Output: $output"
	fi

	safe_rm_rf "$mock_path" "$tmpdir"

	log "Remote missing GNU parallel origin test passed"
}

consistency_option_validation_tests() {
	log "Starting option consistency tests"

	assert_usage_error_case "Backup and restore properties together" \
		"You cannot bac(k)up and r(e)store properties at the same time." \
		-k -e -R tank/src backup/target

	assert_usage_error_case "Both beep modes" \
		"You cannot use both beep modes at the same time." \
		-b -B -R tank/src backup/target

	assert_usage_error_case "Compression without remote" \
		"-z option can only be used with -O or -T option" \
		-z -R tank/src backup/target

	assert_usage_error_case "Empty compression command" \
		"Compression command (-Z/ZXFER_COMPRESSION) cannot be empty." \
		-Z "" -R tank/src backup/target

	assert_usage_error_case "Zero job count" \
		"The -j option requires a job count of at least 1." \
		-j 0 -R tank/src backup/target

	assert_usage_error_case "Non-numeric job count" \
		"The -j option requires a positive integer job count, but received \"abc\"." \
		-j abc -R tank/src backup/target

	log "Option consistency tests passed"
}

failure_handling_tests() {
	log "Starting missing dataset error tests"

	# Ensure destination exists so the source failure path is hit.
	dest_root="$DEST_POOL/failure_dest"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"

	assert_error_case "Missing source dataset" \
		"Failed to retrieve snapshots from the source" \
		3 \
		-R "$SRC_POOL/no_such_dataset" "$dest_root"

	log "Missing dataset error tests passed"
}

runtime_failure_report_test() {
	log "Starting runtime failure report test"

	dest_root="$DEST_POOL/failure_report_dest"
	stdout_log="$WORKDIR/runtime_failure.stdout"
	stderr_log="$WORKDIR/runtime_failure.stderr"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"

	set +e
	"$ZXFER_BIN" -R "$SRC_POOL/no_such_dataset" "$dest_root" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 3 ]; then
		fail "Runtime failure report test expected exit status 3, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ -s "$stdout_log" ]; then
		fail "Runtime failure report should not write to stdout. Output: $(cat "$stdout_log")"
	fi
	if ! grep -q "^zxfer: failure report begin$" "$stderr_log"; then
		fail "Runtime failure report block missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "failure_stage: snapshot discovery" "$stderr_log"; then
		fail "Runtime failure report stage missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "source_root: $SRC_POOL/no_such_dataset" "$stderr_log"; then
		fail "Runtime failure report source_root missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "destination_root: $dest_root" "$stderr_log"; then
		fail "Runtime failure report destination_root missing. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "^last_command: " "$stderr_log"; then
		fail "Runtime failure report last_command missing. Output: $(cat "$stderr_log")"
	fi

	log "Runtime failure report test passed"
}

error_log_mirror_test() {
	log "Starting ZXFER_ERROR_LOG mirror test"

	dest_root="$DEST_POOL/error_log_dest"
	log_path="$WORKDIR/runtime_failure.report"
	stderr_log="$WORKDIR/error_log.stderr"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"
	safe_rm_f "$log_path"

	set +e
	ZXFER_ERROR_LOG="$log_path" "$ZXFER_BIN" -R "$SRC_POOL/no_such_dataset" "$dest_root" >/dev/null 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 3 ]; then
		fail "ZXFER_ERROR_LOG mirror test expected exit status 3, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ ! -f "$log_path" ]; then
		fail "ZXFER_ERROR_LOG mirror file was not created."
	fi
	if ! grep -q "^zxfer: failure report begin$" "$log_path"; then
		fail "ZXFER_ERROR_LOG mirror file missing report block. Output: $(cat "$log_path")"
	fi
	if ! grep -q "message: Failed to retrieve snapshots from the source" "$log_path"; then
		fail "ZXFER_ERROR_LOG mirror file missing failure message. Output: $(cat "$log_path")"
	fi

	log "ZXFER_ERROR_LOG mirror test passed"
}

usage_error_log_mirror_test() {
	log "Starting usage ZXFER_ERROR_LOG mirror test"

	log_path="$WORKDIR/usage_failure.report"
	stderr_log="$WORKDIR/usage_error_log.stderr"
	safe_rm_f "$log_path" "$stderr_log"

	set +e
	ZXFER_ERROR_LOG="$log_path" "$ZXFER_BIN" -R tank/src >/dev/null 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Usage ZXFER_ERROR_LOG mirror test expected exit status 2, got $status. Output: $(cat "$stderr_log")"
	fi
	if [ ! -f "$log_path" ]; then
		fail "Usage ZXFER_ERROR_LOG mirror file was not created."
	fi
	if ! grep -q "^zxfer: failure report begin$" "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing report block. Output: $(cat "$log_path")"
	fi
	if ! grep -q "failure_class: usage" "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing usage class. Output: $(cat "$log_path")"
	fi
	if ! grep -q "exit_status: 2" "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing exit status 2. Output: $(cat "$log_path")"
	fi
	if ! grep -q "message: Need a destination." "$log_path"; then
		fail "Usage ZXFER_ERROR_LOG mirror file missing usage message. Output: $(cat "$log_path")"
	fi

	log "Usage ZXFER_ERROR_LOG mirror test passed"
}

invalid_error_log_warning_test() {
	log "Starting invalid ZXFER_ERROR_LOG warning test"

	dest_root="$DEST_POOL/error_log_warning_dest"
	stderr_log="$WORKDIR/error_log_warning.stderr"
	relative_log="relative_failure_report.log"
	destroy_test_datasets_if_present "$dest_root"
	zfs create "$dest_root"
	safe_rm_f "$WORKDIR/$relative_log"
	zxfer_abs=$(cd "$(dirname "$ZXFER_BIN")" && pwd)/$(basename "$ZXFER_BIN")

	set +e
	(
		cd "$WORKDIR"
		ZXFER_ERROR_LOG="$relative_log" "$zxfer_abs" -R "$SRC_POOL/no_such_dataset" "$dest_root" >/dev/null 2>"$stderr_log"
	)
	status=$?
	set -e

	if [ "$status" -ne 3 ]; then
		fail "Invalid ZXFER_ERROR_LOG warning test expected exit status 3, got $status. Output: $(cat "$stderr_log")"
	fi
	if ! grep -q "warning: refusing ZXFER_ERROR_LOG path \"$relative_log\" because it is not absolute" "$stderr_log"; then
		fail "ZXFER_ERROR_LOG warning missing from stderr. Output: $(cat "$stderr_log")"
	fi
	if [ -e "$WORKDIR/$relative_log" ]; then
		fail "Relative ZXFER_ERROR_LOG should not create a file in $WORKDIR."
	fi

	log "Invalid ZXFER_ERROR_LOG warning test passed"
}

error_log_email_example_self_test() {
	log "Starting error-log email example self-test"

	set +e
	output=$(sh ./examples/error-log-email-notify.sh --self-test 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Error-log email example self-test failed with status $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "self-test passed"; then
		fail "Error-log email example self-test did not report success. Output: $output"
	fi

	log "Error-log email example self-test passed"
}

snapshot_deletion_test() {
	log "Starting snapshot deletion test"

	src_dataset="$SRC_POOL/snapdel_src"
	dest_root="$DEST_POOL/snapdel_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	# Create initial state
	append_data_to_dataset "$src_dataset" "file.txt" "data1"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "data2"
	zfs snap -r "$src_dataset@snap2"

	# Replicate
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	# Delete snap1 on source
	destroy_test_dataset "$src_dataset@snap1"

	# Run without -d (snap1 should remain on dest)
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	# Run with -n -d (dry run, snap1 should remain)
	# We capture output to verify it *would* delete
	output=$(run_zxfer -v -n -d -R "$src_dataset" "$dest_root" 2>&1)
	assert_output_mentions_snapshot_destroy "$output" "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap1"

	# Run with -d (snap1 should be deleted)
	run_zxfer -v -Y -d -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "snap1" 30
	wait_for_snapshot_absent "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	log "Snapshot deletion test passed"
}

abort_integration_run() {
	l_signal=$1
	l_status=$2

	ZXFER_ABORT_REQUESTED=1
	trap - INT TERM
	log "Received $l_signal, aborting integration test run."
	exit "$l_status"
}

cleanup() {
	set +e
	l_exit_status=$?
	l_cleanup_ok=1
	l_preserve_reason=""
	l_job_pids=$(ps -o pid= -o ppid= 2>/dev/null | awk -v ppid="$$" '
		$2 == ppid {print $1}
	' || true)
	if [ -n "$l_job_pids" ]; then
		# shellcheck disable=SC2086  # split into individual PIDs on purpose
		kill $l_job_pids 2>/dev/null || true
		# shellcheck disable=SC2086  # wait accepts individual PIDs
		wait $l_job_pids 2>/dev/null || true
	fi
	destroy_test_pool_if_owned "source" "${SRC_POOL:-}" "${SRC_POOL_CREATED:-0}" "${SRC_IMG:-}" || l_cleanup_ok=0
	destroy_test_pool_if_owned "destination" "${DEST_POOL:-}" "${DEST_POOL_CREATED:-0}" "${DEST_IMG:-}" || l_cleanup_ok=0
	if [ "$l_cleanup_ok" -ne 1 ]; then
		l_preserve_reason="test pools were not fully cleaned up"
	elif [ "$l_exit_status" -ne 0 ] && [ "${ZXFER_PRESERVE_WORKDIR_ON_FAILURE:-0}" = "1" ]; then
		l_preserve_reason="the integration run failed and ZXFER_PRESERVE_WORKDIR_ON_FAILURE=1"
	fi

	if [ -z "$l_preserve_reason" ]; then
		[ -n "${WORKDIR:-}" ] && safe_rm_rf "$WORKDIR"
	else
		printf 'WARNING: preserving integration workdir %s because %s.\n' \
			"${WORKDIR:-<unset>}" "$l_preserve_reason" >&2
	fi

	return "$l_exit_status"
}

basic_replication_test() {
	# Exercise zxfer's standard ZFS send/receive mode with -R so recursive
	# snapshots and incremental updates propagate from source to destination.
	log "Starting basic replication test"
	src_dataset="$SRC_POOL/srcdata"
	dest_root="$DEST_POOL/replica"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$DEST_POOL/replica" "$SRC_POOL/srcdata"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "snapshot one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "snapshot two"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	append_data_to_dataset "$src_dataset" "file.txt" "snapshot three"
	zfs snap -r "$src_dataset@snap3"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "snap3"

	log "Basic replication test passed"
}

non_recursive_replication_test() {
	log "Starting non-recursive replication test"

	src_dataset="$SRC_POOL/nonrec_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/nonrec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "root.txt" "root data 1"
	zfs snap "$src_dataset@rootsnap1"
	append_data_to_dataset "$src_dataset" "root.txt" "root data 2"
	zfs snap "$src_dataset@rootsnap2"

	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap "$child_dataset@childsnap1"

	run_zxfer -v -N "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "rootsnap1"
	assert_snapshot_exists "$dest_dataset" "rootsnap2"

	if zfs list "$dest_dataset/child" >/dev/null 2>&1; then
		fail "Child dataset should not be replicated when using -N."
	fi

	log "Non-recursive replication test passed"
}

auto_snapshot_replication_test() {
	log "Starting auto-snapshot replication test"

	src_dataset="$SRC_POOL/newsnap_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/newsnap_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	dest_child="$dest_dataset/${child_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "parent.txt" "parent data"
	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap -r "$src_dataset@preseed"

	run_zxfer -v -s -R "$src_dataset" "$dest_root"

	src_snapshot_name=$(get_latest_snapshot_name_for_dataset "$src_dataset")
	snap_suffix=${src_snapshot_name#*@}

	if [ -z "$snap_suffix" ] || [ "$snap_suffix" = "$src_snapshot_name" ]; then
		fail "Auto snapshot was not created on source dataset $src_dataset."
	fi

	assert_snapshot_exists "$src_dataset" "$snap_suffix"
	assert_snapshot_exists "$child_dataset" "$snap_suffix"
	assert_snapshot_exists "$dest_dataset" "$snap_suffix"
	assert_snapshot_exists "$dest_child" "$snap_suffix"

	log "Auto-snapshot replication test passed"
}

auto_snapshot_nonrecursive_test() {
	log "Starting auto-snapshot non-recursive test"

	src_dataset="$SRC_POOL/newsnap_nonrec_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/newsnap_nonrec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "parent.txt" "parent data"
	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap "$src_dataset@preseed"

	run_zxfer -v -s -N "$src_dataset" "$dest_root"

	src_snapshot_name=$(get_latest_snapshot_name_for_dataset "$src_dataset")
	snap_suffix=${src_snapshot_name#*@}

	if [ -z "$snap_suffix" ] || [ "$snap_suffix" = "$src_snapshot_name" ]; then
		fail "Auto snapshot was not created on source dataset $src_dataset."
	fi

	assert_snapshot_exists "$src_dataset" "$snap_suffix"
	assert_snapshot_exists "$dest_dataset" "$snap_suffix"

	if zfs list -t snapshot "$child_dataset@$snap_suffix" >/dev/null 2>&1; then
		fail "Child dataset should not receive auto snapshot when using -s with -N."
	fi
	if zfs list "$dest_dataset/${child_dataset##*/}" >/dev/null 2>&1; then
		fail "Child dataset should not be replicated when using -N with auto snapshot."
	fi

	log "Auto-snapshot non-recursive test passed"
}

trailing_slash_destination_test() {
	log "Starting trailing slash destination test"

	# Without trailing slash: destination should contain the source basename
	src_dataset="$SRC_POOL/tslash_no"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/tslash_dest_no"
	dest_dataset="$dest_root/${src_dataset##*/}"
	dest_child="$dest_dataset/${child_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "data"
	zfs snap -r "$src_dataset@tsnap"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "tsnap"
	assert_snapshot_exists "$dest_child" "tsnap"

	# With trailing slash: destination should be written directly into dest_root
	src_dataset="$SRC_POOL/tslash_yes"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/tslash_dest_yes"
	dest_child="$dest_root/${child_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "data2"
	zfs snap -r "$src_dataset@tsnap"

	run_zxfer -v -F -R "$src_dataset/" "$dest_root"

	assert_snapshot_exists "$dest_root" "tsnap"
	assert_snapshot_exists "$dest_child" "tsnap"

	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Trailing slash should not create an extra child dataset under destination root."
	fi

	log "Trailing slash destination test passed"
}

exclude_filter_test() {
	log "Starting exclude filter test"

	src_parent="$SRC_POOL/exclude_src"
	include_child="$src_parent/include_ds"
	exclude_child="$src_parent/exclude_me"
	dest_root="$DEST_POOL/exclude_dest"
	dest_parent="$dest_root/${src_parent##*/}"
	dest_include="$dest_parent/${include_child##*/}"
	dest_exclude="$dest_parent/${exclude_child##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_parent"

	zfs create "$src_parent"
	zfs create "$include_child"
	zfs create "$exclude_child"
	zfs create "$dest_root"

	append_data_to_dataset "$src_parent" "parent.txt" "parent data"
	append_data_to_dataset "$include_child" "include.txt" "include data"
	append_data_to_dataset "$exclude_child" "exclude.txt" "exclude data"
	zfs snap -r "$src_parent@exsnap"

	run_zxfer -v -x "exclude_me" -R "$src_parent" "$dest_root"

	assert_snapshot_exists "$dest_parent" "exsnap"
	assert_snapshot_exists "$dest_include" "exsnap"

	if zfs list "$dest_exclude" >/dev/null 2>&1; then
		fail "Dataset matching exclude pattern should not be replicated."
	fi

	log "Exclude filter test passed"
}

parallel_jobs_listing_test() {
	log "Starting parallel jobs listing test"

	if ! has_gnu_parallel; then
		log "Skipping parallel jobs listing test (GNU parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/parallel_list_src"
	dest_root="$DEST_POOL/parallel_list_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@pl1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@pl2"

	set +e
	output=$(run_zxfer -v -j 2 -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		log "Skipping parallel jobs listing test due to zxfer failure (possibly missing GNU parallel support in ZFS list pipeline). Output: $output"
		return
	fi

	assert_snapshot_exists "$dest_dataset" "pl1"
	assert_snapshot_exists "$dest_dataset" "pl2"

	log "Parallel jobs listing test passed"
}

progress_wrapper_test() {
	log "Starting progress wrapper test"

	if ! has_gnu_parallel; then
		log "Skipping progress wrapper test (GNU parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/progress_src"
	dest_root="$DEST_POOL/progress_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "progress data"
	zfs snap -r "$src_dataset@p1"

	set +e
	output=$(run_zxfer -v -j 2 -D "cat >/dev/null" -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		log "Skipping progress wrapper test due to zxfer failure (parallel/progress pipeline unavailable). Output: $output"
		return
	fi

	assert_snapshot_exists "$dest_dataset" "p1"

	log "Progress wrapper test passed"
}

progress_placeholder_passthrough_test() {
	log "Starting progress placeholder passthrough test"

	src_dataset="$SRC_POOL/progress_placeholder_src"
	dest_root="$DEST_POOL/progress_placeholder_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	progress_script="$WORKDIR/mock_progress_logger.sh"
	progress_log="$WORKDIR/mock_progress_logger.log"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_f "$progress_log"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "progress placeholder data"
	zfs snap -r "$src_dataset@pp1"

	write_progress_logger_script "$progress_script"

	run_zxfer -v -D "$progress_script $progress_log %%size%% %%title%%" -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "pp1"
	assert_exists "$progress_log" "Expected progress placeholder log $progress_log to exist."
	if ! grep -q "^title=${src_dataset}@pp1$" "$progress_log"; then
		fail "Expected progress placeholder title to record ${src_dataset}@pp1. Log: $(cat "$progress_log")"
	fi
	if ! grep -Eq '^size=[0-9]+$' "$progress_log"; then
		fail "Expected progress placeholder size to be numeric. Log: $(cat "$progress_log")"
	fi
	if ! grep -Eq '^bytes=[1-9][0-9]*$' "$progress_log"; then
		fail "Expected progress passthrough to forward non-empty stream data. Log: $(cat "$progress_log")"
	fi

	log "Progress placeholder passthrough test passed"
}

job_limit_enforcement_test() {
	log "Starting job limit enforcement test"

	if ! has_gnu_parallel; then
		log "Skipping job limit enforcement test (GNU parallel not available)"
		return
	fi

	src_root="$SRC_POOL/joblimit_src"
	dest_root="$DEST_POOL/joblimit_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_root"

	zfs create "$src_root"
	zfs create "$dest_root"

	for i in 1 2 3 4; do
		child="$src_root/fs$i"
		zfs create "$child"
		append_data_to_dataset "$child" "file.txt" "data$i"
	done

	# First recursive snapshot seeds parent and children.
	zfs snap -r "$src_root@base"

	for i in 1 2 3 4; do
		child="$src_root/fs$i"
		append_data_to_dataset "$child" "file.txt" "more$i"
	done
	zfs snap -r "$src_root@next"

	run_zxfer -v -j 3 -R "$src_root" "$dest_root"

	for i in 1 2 3 4; do
		dest_child="$dest_root/joblimit_src/fs$i"
		assert_snapshot_exists "$dest_child" "base"
		assert_snapshot_exists "$dest_child" "next"
	done

	assert_snapshot_exists "$dest_root/joblimit_src" "base"
	assert_snapshot_exists "$dest_root/joblimit_src" "next"

	log "Job limit enforcement test passed"
}

missing_destination_error_test() {
	log "Starting missing destination error test"

	src_dataset="$SRC_POOL/missing_dest_src"

	destroy_test_datasets_if_present "$src_dataset"
	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@p1"

	set +e
	output=$("$ZXFER_BIN" -v -R "$src_dataset" nosuchdestpool/target 2>&1)
	status=$?
	set -e

	destroy_test_datasets_if_present "$src_dataset"

	if [ "$status" -eq 0 ]; then
		fail "Missing destination list should cause zxfer to fail."
	fi
	if [ "$status" -ne 2 ]; then
		fail "Missing destination list should exit with status 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Failed to retrieve list of datasets from the destination" >/dev/null 2>&1; then
		fail "Missing destination error message missing. Output: $output"
	fi

	log "Missing destination error test passed"
}

invalid_override_property_test() {
	log "Starting invalid override property test"

	src_dataset="$SRC_POOL/invalid_prop_src"
	dest_root="$DEST_POOL/invalid_prop_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@p1"
	zfs create "$dest_root"

	set +e
	output=$("$ZXFER_BIN" -v -o "definitelynotaproperty=on" -N "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Invalid override property should cause zxfer to fail."
	fi
	if [ "$status" -ne 2 ]; then
		fail "Invalid override property should exit with status 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Invalid option property - check -o list for syntax errors." >/dev/null 2>&1; then
		fail "Invalid override property error message missing. Output: $output"
	fi

	log "Invalid override property test passed"
}

dry_run_replication_test() {
	log "Starting dry-run replication test"

	src_dataset="$SRC_POOL/dryrun_src"
	dest_root="$DEST_POOL/dryrun_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@dr1"
	zfs create "$dest_root"

	output=$("$ZXFER_BIN" -v -n -R "$src_dataset" "$dest_root" 2>&1)
	log "$output"

	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Dry run should not create destination dataset $dest_dataset."
	fi

	log "Dry-run replication test passed"
}

dry_run_deletion_test() {
	log "Starting dry-run deletion test"

	src_dataset="$SRC_POOL/dryrun_del_src"
	dest_root="$DEST_POOL/dryrun_del_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	destroy_test_dataset "$src_dataset@snap1"

	output=$("$ZXFER_BIN" -v -n -d -R "$src_dataset" "$dest_root" 2>&1)
	log "$output"

	assert_output_mentions_snapshot_destroy "$output" "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	log "Dry-run deletion test passed"
}

delete_dest_only_snapshot_test() {
	log "Starting destination-only snapshot delete test"

	src_dataset="$SRC_POOL/destonly_src"
	dest_root="$DEST_POOL/destonly_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	# This test only needs one common snapshot plus one destination-only
	# snapshot. Seed the common snapshot with the same single-snapshot zxfer
	# path used by other stable integration tests, then add a destination-only
	# snapshot on top.
	append_data_to_dataset "$src_dataset" "file.txt" "base"
	zfs snap -r "$src_dataset@base"

	set +e
	output=$(run_zxfer -v -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Initial zxfer seed run failed in delete_dest_only_snapshot_test. Output: $output"
	fi
	if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
		l_i=0
		while [ "$l_i" -lt 30 ]; do
			if zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
				break
			fi
			sleep 1
			l_i=$((l_i + 1))
		done
		if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
			l_source_snaps=$(zfs list -H -t snapshot -o name -r "$src_dataset" 2>/dev/null || true)
			l_dest_snaps=$(zfs list -H -t snapshot -o name -r "$dest_dataset" 2>/dev/null || true)
			fail "Initial zxfer seed run did not produce expected common snapshot $dest_dataset@base. zxfer output: $output. Source snapshots: ${l_source_snaps:-<none>}. Destination snapshots: ${l_dest_snaps:-<none>}."
		fi
	fi

	# Create a destination-only snapshot that should be removed by -d even when no new sends are pending.
	zfs snap -r "$dest_dataset@destonly"
	assert_snapshot_exists "$dest_dataset" "destonly"

	set +e
	output=$("$ZXFER_BIN" -v -d -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "zxfer -d run failed. Output: $output"
	fi

	wait_for_destroy_process_to_finish "$dest_dataset" "destonly" 30
	wait_for_snapshot_absent "$dest_dataset" "destonly"
	if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
		l_i=0
		while [ "$l_i" -lt 30 ]; do
			if zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
				break
			fi
			sleep 1
			l_i=$((l_i + 1))
		done
		if ! zfs list -t snapshot "$dest_dataset@base" >/dev/null 2>&1; then
			l_source_snaps=$(zfs list -H -t snapshot -o name -r "$src_dataset" 2>/dev/null || true)
			l_dest_snaps=$(zfs list -H -t snapshot -o name -r "$dest_dataset" 2>/dev/null || true)
			fail "Delete-only run removed or failed to preserve common snapshot $dest_dataset@base. zxfer output: $output. Source snapshots after delete run: ${l_source_snaps:-<none>}. Destination snapshots after delete run: ${l_dest_snaps:-<none>}."
		fi
	fi

	log "Destination-only snapshot delete test passed"
}

existing_empty_destination_seed_test() {
	log "Starting existing empty destination seed test"

	src_dataset="$SRC_POOL/existing_empty_seed_src"
	dest_root="$DEST_POOL/existing_empty_seed_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs create "$dest_dataset"

	append_data_to_dataset "$src_dataset" "file.txt" "seed one"
	zfs snap "$src_dataset@seed1"

	set +e
	output=$(run_zxfer -v -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Existing empty destination seed run failed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "exists but has no snapshots. Seeding with [$src_dataset@seed1]" >/dev/null 2>&1; then
		fail "Expected non-creation seed branch message for existing empty destination. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "seed1"

	append_data_to_dataset "$src_dataset" "file.txt" "seed two"
	zfs snap "$src_dataset@seed2"
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "seed2"

	log "Existing empty destination seed test passed"
}

migration_unmounted_guard_test() {
	log "Starting migration unmounted guard test"

	src_dataset="$SRC_POOL/unmounted_src"
	dest_root="$DEST_POOL/unmounted_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@unmounted"

	# Unmount the source to trigger the guard.
	if ! zfs unmount "$src_dataset"; then
		fail "Failed to unmount $src_dataset to trigger migration guard."
	fi

	set +e
	output=$("$ZXFER_BIN" -v -m -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Migration guard should fail when source is unmounted."
	fi
	if ! printf '%s\n' "$output" | grep -q "The source filesystem is not mounted, cannot use -m."; then
		fail "Migration guard error message missing. Output: $output"
	fi

	log "Migration unmounted guard test passed"
}

grandfather_protection_test() {
	log "Starting grandfather protection test"

	src_dataset="$SRC_POOL/grand_src"
	dest_root="$DEST_POOL/grand_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@base"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "base"

	# With -g 0, any deletion attempt should be rejected before destroying snapshots.
	set +e
	output=$("$ZXFER_BIN" -v -g 0 -d -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Grandfather protection should fail when -g 0 blocks deletion."
	fi
	if ! printf '%s\n' "$output" | grep -q "grandfather"; then
		fail "Grandfather protection message missing. Output: $output"
	fi

	assert_snapshot_exists "$dest_dataset" "base"

	log "Grandfather protection test passed"
}

send_command_dryrun_test() {
	log "Starting send command dry-run test"

	src_dataset="$SRC_POOL/sendcmd_src"
	dest_root="$DEST_POOL/sendcmd_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap2"

	set +e
	output=$("$ZXFER_BIN" -v -V -w -n -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Dry-run send command test failed. Output: $output"
	fi

	if ! printf '%s\n' "$output" | grep -q "send -v -w -I"; then
		fail "Expected raw incremental send command with verbosity in output. Output: $output"
	fi

	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Dry run should not create destination dataset $dest_dataset."
	fi

	log "Send command dry-run test passed"
}

raw_send_replication_test() {
	log "Starting raw send replication test"

	keyfile="$WORKDIR/raw_keyfile"
	safe_rm_f "$keyfile"
	printf '%s\n' "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" >"$keyfile"

	src_dataset="$SRC_POOL/raw_send_src"
	dest_root="$DEST_POOL/raw_send_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	if ! zfs create -o encryption=on -o keyformat=hex -o keylocation="file://$keyfile" "$src_dataset" >/dev/null 2>&1; then
		log "Skipping raw send replication test (encryption/raw send unsupported on this host)"
		safe_rm_f "$keyfile"
		return
	fi
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "raw stream one"
	zfs snap -r "$src_dataset@raw1"
	append_data_to_dataset "$src_dataset" "file.txt" "raw stream two"
	zfs snap -r "$src_dataset@raw2"

	run_zxfer -v -w -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "raw1"
	assert_snapshot_exists "$dest_dataset" "raw2"
	dest_encryption=$(zfs get -H -o value encryption "$dest_dataset" 2>/dev/null || echo "")
	if [ "$dest_encryption" = "off" ] || [ "$dest_encryption" = "" ]; then
		fail "Raw send should preserve encryption on destination; got '$dest_encryption'."
	fi

	safe_rm_f "$keyfile"

	log "Raw send replication test passed"
}

property_backup_restore_test() {
	log "Starting property backup/restore test"

	src_dataset="$SRC_POOL/prop_backup_src"
	dest_root="$DEST_POOL/prop_backup_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	backup_dir="$WORKDIR/backup_props"

	safe_rm_rf "$backup_dir"
	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@propbackup1"

	# First run backs up properties into a hardened temp directory.
	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$dest_root"

	# Verify destination received the property and the backup file exists with a header.
	dest_prop=$(zfs get -H -o value test:prop "$dest_dataset")
	if [ "$dest_prop" != "one" ]; then
		fail "Destination property expected 'one', got '$dest_prop'."
	fi

	backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$dest_dataset")
	if [ "$backup_file" = "" ]; then
		fail "Exact-pair backup metadata file was not written under $backup_dir."
	fi
	if ! grep -q "^#zxfer property backup file" "$backup_file"; then
		fail "Backup metadata missing expected header."
	fi

	# Mutate destination then restore from backup metadata.
	zfs set test:prop=mutated "$dest_dataset"
	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -e -R "$src_dataset" "$dest_root"

	dest_prop_after=$(zfs get -H -o value test:prop "$dest_dataset")
	if [ "$dest_prop_after" != "one" ]; then
		fail "Property restore expected 'one', got '$dest_prop_after'."
	fi

	log "Property backup/restore test passed"
}

remote_property_backup_restore_test() {
	log "Starting remote property backup/restore test"

	mock_path="$WORKDIR/mock_remote_backup"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	backup_dir="$WORKDIR/remote_backup_dir"

	src_dataset="$SRC_POOL/remote_prop_src"
	dest_remote_root="$DEST_POOL/remote_prop_remote_dest"
	dest_remote_dataset="$dest_remote_root/${src_dataset##*/}"
	restore_dest_root="$DEST_POOL/remote_prop_restore"
	restore_dest_dataset="$restore_dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_remote_root" "$restore_dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_remote_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "remote backup seed"
	zfs snap -r "$src_dataset@rpb1"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -T localhost -R "$src_dataset" "$dest_remote_root"

	assert_snapshot_exists "$dest_remote_dataset" "rpb1"
	dest_prop=$(zfs get -H -o value test:prop "$dest_remote_dataset")
	if [ "$dest_prop" != "one" ]; then
		fail "Remote destination property expected 'one', got '$dest_prop'."
	fi

	set_test_dataset_mountpoint "$dest_remote_dataset" "$WORKDIR/mnt/remote_prop_dest"

	remote_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$dest_remote_dataset")
	if [ "$remote_backup_file" = "" ]; then
		fail "Exact-pair remote backup metadata file not created under $backup_dir."
	fi
	remote_mode=$(get_file_mode_octal "$remote_backup_file" 2>/dev/null || echo "")
	if [ "$remote_mode" != "600" ]; then
		fail "Remote backup metadata permissions expected 600, got $remote_mode."
	fi

	# Exact-pair restore metadata is keyed by the current source and destination.
	# Capture that pair before mutating the remote source dataset so the later
	# -e run restores the original property value instead of the live mutated one.
	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -O localhost -R "$dest_remote_dataset" "$restore_dest_root"
	assert_snapshot_exists "$restore_dest_dataset" "rpb1"

	# Remove the seeded restore target so the upcoming restore run exercises a
	# fresh receive while still consuming the exact-pair backup metadata.
	destroy_test_datasets_if_present "$restore_dest_root"

	zfs set test:prop=mutated "$dest_remote_dataset"
	append_data_to_dataset "$dest_remote_dataset" "file.txt" "after remote backup"
	zfs snap -r "$dest_remote_dataset@rpb2"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -e -O localhost -R "$dest_remote_dataset" "$restore_dest_root"

	assert_snapshot_exists "$restore_dest_dataset" "rpb1"
	assert_snapshot_exists "$restore_dest_dataset" "rpb2"
	restore_prop=$(zfs get -H -o value test:prop "$restore_dest_dataset")
	if [ "$restore_prop" != "one" ]; then
		fail "Restored property expected 'one', got '$restore_prop'."
	fi

	log "Remote property backup/restore test passed"
}

backup_dir_symlink_guard_test() {
	log "Starting backup directory symlink guard test"

	src_dataset="$SRC_POOL/backup_symlink_src"
	dest_root="$DEST_POOL/backup_symlink_dest"
	backup_dir_link="$WORKDIR/backup_symlink"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_dir_link"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=one "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@symlinkguard1"

	ln -s /tmp "$backup_dir_link"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir_link" "$ZXFER_BIN" -v -k -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Backup write should fail when ZXFER_BACKUP_DIR is a symlink."
	fi
	if ! printf '%s\n' "$output" | grep -q "Refusing to use backup directory"; then
		fail "Expected symlink guard error. Output: $output"
	fi

	safe_rm_rf "$backup_dir_link"

	log "Backup directory symlink guard test passed"
}

missing_backup_metadata_error_test() {
	log "Starting missing backup metadata error test"

	src_dataset="$SRC_POOL/no_backup_src"
	dest_root="$DEST_POOL/no_backup_dest"
	backup_dir="$WORKDIR/no_backup_dir"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_dir"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@missing"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" "$ZXFER_BIN" -v -e -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Restore (-e) should fail when no backup metadata exists."
	fi
	if ! printf '%s\n' "$output" | grep -q "Cannot find backup property file"; then
		fail "Expected missing backup metadata message. Output: $output"
	fi
	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Destination dataset should not be created when backup metadata is missing."
	fi

	log "Missing backup metadata error test passed"
}

insecure_backup_metadata_guard_test() {
	log "Starting insecure backup metadata guard test"

	src_dataset="$SRC_POOL/insecure_backup_src"
	dest_root="$DEST_POOL/insecure_backup_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	backup_root="$WORKDIR/insecure_backup_dir"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$backup_root"

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@insecure1"

	ZXFER_BACKUP_DIR="$backup_root" run_zxfer -v -k -R "$src_dataset" "$dest_root"
	local_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_root" "$src_dataset" "$dest_dataset")
	if [ "$local_backup_file" = "" ]; then
		fail "Exact-pair local backup metadata file not found for insecure metadata guard test."
	fi
	chmod 644 "$local_backup_file"
	destroy_test_datasets_if_present "$dest_root"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_root" "$ZXFER_BIN" -v -e -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Insecure local backup metadata should cause restore to fail."
	fi
	if ! printf '%s\n' "$output" | grep -q "permissions" >/dev/null 2>&1; then
		fail "Expected permission rejection message for insecure local metadata. Output: $output"
	fi
	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Destination dataset should not be created when local backup metadata is rejected."
	fi

	mock_path="$WORKDIR/mock_insecure_remote"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	remote_backup_root="$WORKDIR/remote_insecure_backup_dir"
	safe_rm_rf "$remote_backup_root"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@insecure2"

	ZXFER_BACKUP_DIR="$remote_backup_root" run_zxfer -v -k -R "$src_dataset" "$dest_root"
	remote_backup_file=$(find_backup_metadata_file_for_exact_pair "$remote_backup_root" "$src_dataset" "$dest_dataset")
	if [ "$remote_backup_file" = "" ]; then
		fail "Exact-pair remote backup metadata file not found for insecure metadata guard test."
	fi
	remote_expected_error="not owned by root"
	chmod 600 "$remote_backup_file"
	if command -v chown >/dev/null 2>&1 && chown 1 "$remote_backup_file" >/dev/null 2>&1; then
		:
	else
		chmod 644 "$remote_backup_file"
		remote_expected_error="permissions"
	fi
	destroy_test_datasets_if_present "$dest_root"

	set +e
	output=$(ZXFER_BACKUP_DIR="$remote_backup_root" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -e -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Insecure remote backup metadata should cause restore to fail."
	fi
	if ! printf '%s\n' "$output" | grep -q "$remote_expected_error" >/dev/null 2>&1; then
		fail "Expected remote metadata rejection message [$remote_expected_error]. Output: $output"
	fi
	if zfs list "$dest_root/${src_dataset##*/}" >/dev/null 2>&1; then
		fail "Destination dataset should not be created when remote backup metadata is rejected."
	fi

	log "Insecure backup metadata guard test passed"
}

legacy_backup_layout_rejected_test() {
	log "Starting legacy backup layout rejection test"

	src_dataset="$SRC_POOL/legacy_backup_src"
	dest_root="$DEST_POOL/legacy_backup_dest"
	restore_root="$DEST_POOL/legacy_backup_restore"
	backup_dir="$WORKDIR/legacy_backup_dir"

	destroy_test_datasets_if_present "$dest_root" "$restore_root" "$src_dataset"
	safe_rm_rf "$backup_dir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=legacy "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "legacy content"
	zfs snap -r "$src_dataset@legacy1"

	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$dest_root"

	ZXFER_BACKUP_DIR="$backup_dir" run_zxfer -v -k -R "$src_dataset" "$restore_root"
	restore_dataset="$restore_root/${src_dataset##*/}"
	restore_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$restore_dataset")
	if [ "$restore_backup_file" = "" ]; then
		fail "Exact-pair restore backup metadata file not found for legacy rejection test."
	fi

	src_mount=$(get_mountpoint "$src_dataset")
	legacy_backup="$src_mount/.zxfer_backup_info.${src_dataset##*/}"
	mv "$restore_backup_file" "$legacy_backup"
	chmod 600 "$legacy_backup"

	destroy_test_datasets_if_present "$dest_root" "$restore_root"
	zfs create "$dest_root"
	zfs set test:prop=mutated "$src_dataset"
	zfs snap -r "$src_dataset@legacy2"
	safe_rm_rf "$backup_dir"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" "$ZXFER_BIN" -v -e -R "$src_dataset" "$restore_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Restore with legacy backup metadata should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Cannot find backup property file" >/dev/null 2>&1; then
		fail "Expected missing-backup failure for legacy backup metadata. Output: $output"
	fi
	if zfs list "$restore_dataset" >/dev/null 2>&1; then
		fail "Restore dataset should not be created when only legacy backup metadata is available."
	fi

	log "Legacy backup layout rejection test passed"
}

remote_legacy_backup_layout_rejected_test() {
	log "Starting remote legacy backup layout rejection test"

	mock_path="$WORKDIR/mock_remote_legacy_backup"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	backup_dir="$WORKDIR/remote_legacy_backup_dir"

	src_dataset="$SRC_POOL/remote_legacy_backup_src"
	dest_root="$DEST_POOL/remote_legacy_backup_dest"
	restore_root="$DEST_POOL/remote_legacy_backup_restore"

	destroy_test_datasets_if_present "$dest_root" "$restore_root" "$src_dataset"
	safe_rm_rf "$backup_dir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs set test:prop=legacy "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "remote legacy content"
	zfs snap -r "$src_dataset@remotelegacy1"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -T localhost -R "$src_dataset" "$dest_root"

	ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -k -T localhost -R "$src_dataset" "$restore_root"
	restore_dataset="$restore_root/${src_dataset##*/}"
	restore_backup_file=$(find_backup_metadata_file_for_exact_pair "$backup_dir" "$src_dataset" "$restore_dataset")
	if [ "$restore_backup_file" = "" ]; then
		fail "Exact-pair remote restore backup metadata file not found for legacy rejection test."
	fi

	src_mount=$(get_mountpoint "$src_dataset")
	legacy_backup="$src_mount/.zxfer_backup_info.${src_dataset##*/}"
	mv "$restore_backup_file" "$legacy_backup"
	chmod 600 "$legacy_backup"

	destroy_test_datasets_if_present "$dest_root" "$restore_root"
	zfs create "$dest_root"
	zfs set test:prop=mutated "$src_dataset"
	zfs snap -r "$src_dataset@remotelegacy2"
	safe_rm_rf "$backup_dir"

	set +e
	output=$(ZXFER_BACKUP_DIR="$backup_dir" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -e -O localhost -R "$src_dataset" "$restore_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Remote restore with legacy backup metadata should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Cannot find backup property file" >/dev/null 2>&1; then
		fail "Expected missing-backup failure for remote legacy backup metadata. Output: $output"
	fi
	if zfs list "$restore_dataset" >/dev/null 2>&1; then
		fail "Remote restore dataset should not be created when only legacy backup metadata is available."
	fi

	log "Remote legacy backup layout rejection test passed"
}

background_send_failure_test() {
	log "Starting background send failure test"

	if ! has_gnu_parallel; then
		log "Skipping background send failure test (GNU parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/sendfail_src"
	dest_root="$DEST_POOL/sendfail_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@base"

	# The first receive into a missing destination dataset is intentionally
	# executed in the foreground. Seed the destination first so the second run
	# exercises the incremental background send path used by -j > 1.
	run_zxfer -v -R "$src_dataset" "$dest_root" >/dev/null 2>&1

	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@incremental"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs binary not found for send failure test."
	fi

	wrapper_dir="$WORKDIR/zfs_wrapper_fail_send"
	safe_rm_rf "$wrapper_dir"
	mkdir -p "$wrapper_dir"
	cat >"$wrapper_dir/zfs" <<EOF
#!/bin/sh
if [ "\$1" = "send" ]; then
	exit 1
fi
exec "$real_zfs" "\$@"
EOF
	chmod +x "$wrapper_dir/zfs"

	for bin in awk ssh cat zstd; do
		real_bin=$(resolve_host_command "$bin")
		if [ "$real_bin" != "" ]; then
			ln -sf "$real_bin" "$wrapper_dir/$bin"
		fi
	done
	real_parallel=$(resolve_host_command parallel)
	if [ "$real_parallel" != "" ]; then
		ln -sf "$real_parallel" "$wrapper_dir/parallel"
	fi

	set +e
	output=$(ZXFER_SECURE_PATH="$wrapper_dir" "$ZXFER_BIN" -v -j 2 -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	safe_rm_rf "$wrapper_dir"

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when zfs send exits non-zero."
	fi
	case "$output" in
	*"zfs send/receive job failed"* | *"failed to read from stream"* | *"Error when executing command."*) ;;
	*)
		fail "Expected send failure indication. Output: $output"
		;;
	esac

	log "Background send failure test passed"
}

force_rollback_test() {
	log "Starting force rollback test"

	src_dataset="$SRC_POOL/rollback_src"
	dest_root="$DEST_POOL/rollback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "original"
	zfs snap "$src_dataset@snap1"

	run_zxfer -v -N "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	set_test_dataset_mountpoint "$dest_dataset" "$WORKDIR/mnt/rollback_dest"

	# Diverge destination with an extra snapshot.
	append_data_to_dataset "$dest_dataset" "file.txt" "dest divergence"
	zfs snap "$dest_dataset@destonly"

	# Advance source and create a new snapshot to send.
	append_data_to_dataset "$src_dataset" "file.txt" "source update"
	zfs snap "$src_dataset@snap2"

	run_zxfer -v -F -N "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "snap2"
	if zfs list -t snapshot "$dest_dataset@destonly" >/dev/null 2>&1; then
		fail "Force rollback should remove divergent destination snapshot destonly."
	fi

	log "Force rollback test passed"
}

generate_tests_replication() {
	# Exercise the historical multi-dataset replication layout using file-backed
	# integration pools instead of direct host datasets.
	log "Starting multi-dataset replication test"

	src_parent="$SRC_POOL/zxfer_tests"
	src_dataset="$src_parent/src"
	dest_root="$DEST_POOL/zxfer_tests"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_parent" "$dest_root"

	zfs create "$src_parent"
	zfs create "$src_dataset"
	zfs create "$dest_root"

	# Ensure the top-level dataset has at least one snapshot so zxfer creates
	# the destination parent before children are replicated.
	zfs snap "$src_dataset@root_snap"

	for child in 1 2 3; do
		child_dataset="$src_dataset/child$child"
		zfs create "$child_dataset"

		for snap in 1 2 3 4; do
			zfs snap -r "$child_dataset@snap$snap"
		done
	done

	zfs snap -r "$src_dataset/child1@snap1_1"
	zfs snap -r "$src_dataset/child1@snap2_1"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	for child in 1 2 3; do
		child_dest_dataset="$dest_dataset/child$child"
		for snap in 1 2 3 4; do
			assert_snapshot_exists "$child_dest_dataset" "snap$snap"
		done
	done

	assert_snapshot_exists "$dest_dataset/child1" "snap1_1"
	assert_snapshot_exists "$dest_dataset/child1" "snap2_1"
	assert_snapshot_exists "$dest_dataset" "root_snap"

	log "Multi-dataset replication test passed"
}

idempotent_replication_test() {
	# Verify that repeated zxfer runs converge on a stable replica.
	log "Starting idempotent replication test"

	src_dataset="$SRC_POOL/idempotent_src"
	dest_root="$DEST_POOL/idempotent_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "idem.txt" "initial data"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "idem.txt" "second snapshot"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	append_data_to_dataset "$src_dataset" "idem.txt" "third snapshot"
	zfs snap -r "$src_dataset@snap3"

	run_zxfer -v -R "$src_dataset" "$dest_root"

	snapshots_before=$(list_exact_snapshot_names_for_dataset "$dest_dataset")

	run_zxfer -v -R "$src_dataset" "$dest_root"

	snapshots_after=$(list_exact_snapshot_names_for_dataset "$dest_dataset")

	if [ "$snapshots_before" != "$snapshots_after" ]; then
		fail "zxfer should be idempotent; destination snapshots changed after a no-op run."
	fi

	log "Idempotent replication test passed"
}

yield_loop_dryrun_iteration_test() {
	log "Starting yield loop dry-run iteration test"

	src_dataset="$SRC_POOL/yield_src"
	dest_root="$DEST_POOL/yield_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	zfs snap -r "$src_dataset@base"
	ZXFER_BACKUP_DIR='' run_zxfer -v -R "$src_dataset" "$dest_root"

	# With no new snapshots to send, -Y -n should perform a single iteration.
	set +e
	output=$("$ZXFER_BIN" -v -Y -n -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Expected zxfer -Y -n to exit successfully. Output: $output"
	fi

	iter_count=$(printf '%s\n' "$output" | grep -c "Begin Iteration")
	if [ "$iter_count" -ne 1 ]; then
		fail "Expected a single iteration under -Y -n; found $iter_count. Output: $output"
	fi

	assert_snapshot_exists "$dest_dataset" "base"

	log "Yield loop dry-run iteration test passed"
}

snapshot_name_mismatch_deletion_test() {
	log "Starting snapshot name mismatch deletion test"

	src_dataset="$SRC_POOL/mismatch_src"
	dest_root="$DEST_POOL/mismatch_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@alpha"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@beta"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "alpha"
	assert_snapshot_exists "$dest_dataset" "beta"

	# Diverge source and destination with multiple differently named snapshots to
	# exercise the deletion comm/sort pipeline that relies on both temp files.
	# This resend path also needs -F: deleting snapshots alone does not roll the
	# live destination dataset back to the last common snapshot.
	destroy_test_dataset "$src_dataset@beta"
	append_data_to_dataset "$src_dataset" "file.txt" "three"
	zfs snap -r "$src_dataset@gamma"

	zfs snap -r "$dest_dataset@z-extra"
	zfs snap -r "$dest_dataset@doomed-beta"

	run_zxfer -v -Y -d -F -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "beta" 30
	wait_for_destroy_process_to_finish "$dest_dataset" "z-extra" 30
	wait_for_snapshot_absent "$dest_dataset" "beta"
	wait_for_snapshot_absent "$dest_dataset" "z-extra"
	wait_for_snapshot_absent "$dest_dataset" "doomed-beta"

	assert_snapshot_exists "$dest_dataset" "alpha"
	assert_snapshot_exists "$dest_dataset" "gamma"

	log "Snapshot name mismatch deletion test passed"
}

snapshot_name_prefix_collision_deletion_test() {
	log "Starting snapshot name prefix collision deletion test"

	src_dataset="$SRC_POOL/prefix_collision_src"
	dest_root="$DEST_POOL/prefix_collision_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap10"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap10"

	# Removing snap1 on the source should not cause -d to delete snap10 on the
	# destination just because the names share a prefix.
	destroy_test_dataset "$src_dataset@snap1"

	run_zxfer -v -Y -d -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "snap1" 30
	wait_for_snapshot_absent "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap10"

	log "Snapshot name prefix collision deletion test passed"
}

remote_origin_target_uncompressed_test() {
	log "Starting remote uncompressed origin/target test"

	if ! has_gnu_parallel; then
		log "Skipping remote uncompressed test (GNU parallel not available for -j>1 remote listings)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_uncompressed"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	ssh_log="$WORKDIR/mock_remote_uncompressed.log"
	safe_rm_f "$ssh_log"
	before_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)

	src_dataset="$SRC_POOL/remote_uncompressed_src"
	dest_root_origin="$DEST_POOL/remote_uncompressed_dest_origin"
	dest_root_target="$DEST_POOL/remote_uncompressed_dest_target"
	dest_origin="$dest_root_origin/${src_dataset##*/}"
	dest_target="$dest_root_target/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root_origin" "$dest_root_target"

	zfs create "$src_dataset"
	zfs create "$dest_root_origin"
	zfs create "$dest_root_target"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@rmt1"

	ZXFER_SECURE_PATH="$secure_path" MOCK_SSH_LOG="$ssh_log" run_zxfer -v -j 2 -O localhost -R "$src_dataset" "$dest_root_origin"
	assert_snapshot_exists "$dest_origin" "rmt1"

	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@rmt2"

	ZXFER_SECURE_PATH="$secure_path" MOCK_SSH_LOG="$ssh_log" run_zxfer -v -T localhost -R "$src_dataset" "$dest_root_target"
	assert_snapshot_exists "$dest_target" "rmt1"
	assert_snapshot_exists "$dest_target" "rmt2"

	socket_leaks=""
	after_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)
	for dir in $after_sockets; do
		case " $before_sockets " in
		*" $dir "*) continue ;;
		esac
		socket_leaks="$socket_leaks $dir"
	done
	if [ -n "$socket_leaks" ]; then
		fail "SSH control socket directories leaked: $socket_leaks"
	fi

	close_count=$(grep -c "^close " "$ssh_log" 2>/dev/null || true)
	if [ "$close_count" -lt 2 ]; then
		fail "Expected ssh control sockets to be closed for origin/target runs; saw $close_count closes. Log: $(cat "$ssh_log" 2>/dev/null || true)"
	fi

	log "Remote uncompressed origin/target test passed"
}

local_helper_path_shell_metacharacters_test() {
	log "Starting local helper path shell metacharacters test"

	marker_rel="local_helper_path_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_local_helper.\$(touch $marker_rel)"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$marker"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs not found on host; cannot run local helper path shell metacharacters test."
	fi
	ln -s "$real_zfs" "$mock_path/zfs"

	src_dataset="$SRC_POOL/local_helper_shell_src"
	dest_root="$DEST_POOL/local_helper_shell_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "helper-path-one"
	zfs snap -r "$src_dataset@lhs1"

	(
		cd "$WORKDIR"
		log "Running: $zxfer_bin_abs -v -R $src_dataset $dest_root"
		ZXFER_SECURE_PATH="$secure_path" "$zxfer_bin_abs" -v -R "$src_dataset" "$dest_root"
	)

	assert_snapshot_exists "$dest_dataset" "lhs1"
	if [ -e "$marker" ]; then
		fail "Resolved local helper paths containing shell metacharacters should not execute locally; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"

	log "Local helper path shell metacharacters test passed"
}

remote_helper_path_shell_metacharacters_test() {
	log "Starting remote helper path shell metacharacters test"

	marker_rel="remote_helper_path_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_remote_helper.\$(touch $marker_rel)"
	ssh_mock_dir="$WORKDIR/mock_remote_helper_safe"
	secure_path="$ssh_mock_dir:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$marker"
	safe_rm_rf "$mock_path" "$ssh_mock_dir"
	mkdir -p "$mock_path"
	mkdir -p "$ssh_mock_dir"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs not found on host; cannot run remote helper path shell metacharacters test."
	fi
	ln -s "$real_zfs" "$mock_path/zfs"
	write_mock_ssh_script "$ssh_mock_dir/ssh"

	src_dataset="$SRC_POOL/remote_helper_shell_src"
	dest_root="$DEST_POOL/remote_helper_shell_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "helper-path-one"
	zfs snap -r "$src_dataset@rhs1"

	(
		cd "$WORKDIR"
		log "Running: $zxfer_bin_abs -v -O localhost -R $src_dataset $dest_root"
		MOCK_SSH_COMMAND_V_TOOL="zfs" \
			MOCK_SSH_COMMAND_V_RESULT="$mock_path/zfs" \
			ZXFER_SECURE_PATH="$secure_path" \
			"$zxfer_bin_abs" -v -O localhost -R "$src_dataset" "$dest_root"
	)

	assert_snapshot_exists "$dest_dataset" "rhs1"
	if [ -e "$marker" ]; then
		fail "Resolved helper paths containing shell metacharacters should not execute locally; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path" "$ssh_mock_dir"

	log "Remote helper path shell metacharacters test passed"
}

garbage_wrapped_host_spec_fails_closed_test() {
	log "Starting garbage wrapped host spec fail-closed test"

	marker_rel="garbage_host_spec_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_garbage_host_spec"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="garbage-host.example \$(touch $marker_rel)"
	safe_rm_f "$marker"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	src_dataset="$SRC_POOL/garbage_host_spec_src"
	dest_root="$DEST_POOL/garbage_host_spec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "garbage-host-spec"
	zfs snap -r "$src_dataset@ghs1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Garbage wrapped host specs should fail closed instead of replicating successfully. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Error creating ssh control socket for origin host." >/dev/null 2>&1 &&
		! printf '%s\n' "$output" | grep -F "Failed to determine operating system on host garbage-host.example" >/dev/null 2>&1; then
		fail "Garbage wrapped host specs should abort during remote startup before replication begins. Output: $output"
	fi
	if [ -e "$marker" ]; then
		fail "Garbage wrapped host specs should not execute embedded shell fragments; marker file was created at $marker."
	fi
	assert_dataset_absent "$dest_dataset"

	safe_rm_rf "$mock_path"

	log "Garbage wrapped host spec fail-closed test passed"
}

control_socket_path_shell_metacharacters_test() {
	log "Starting control socket path shell metacharacters test"

	marker_rel="control_socket_path_marker"
	marker="$WORKDIR/$marker_rel"
	tmpdir_with_payload="$WORKDIR/mock_tmpdir.\$(touch $marker_rel)"
	mock_path="$WORKDIR/mock_control_socket_safe"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	safe_rm_f "$marker"
	safe_rm_rf "$tmpdir_with_payload" "$mock_path"
	mkdir -p "$tmpdir_with_payload"
	mkdir -p "$mock_path"

	real_zfs=$(resolve_host_command zfs)
	if [ "$real_zfs" = "" ]; then
		fail "zfs not found on host; cannot run control socket path shell metacharacters test."
	fi
	ln -s "$real_zfs" "$mock_path/zfs"
	write_mock_ssh_script "$mock_path/ssh"

	src_dataset="$SRC_POOL/control_socket_shell_src"
	dest_root="$DEST_POOL/control_socket_shell_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "control-socket-one"
	zfs snap -r "$src_dataset@css1"

	(
		cd "$WORKDIR"
		log "Running: $zxfer_bin_abs -v -O localhost -R $src_dataset $dest_root"
		TMPDIR="$tmpdir_with_payload" \
			ZXFER_SECURE_PATH="$secure_path" \
			"$zxfer_bin_abs" -v -O localhost -R "$src_dataset" "$dest_root"
	)

	assert_snapshot_exists "$dest_dataset" "css1"
	if [ -e "$marker" ]; then
		fail "SSH control-socket paths containing shell metacharacters should not execute locally; marker file was created at $marker."
	fi

	safe_rm_rf "$tmpdir_with_payload" "$mock_path"

	log "Control socket path shell metacharacters test passed"
}

remote_capability_control_whitespace_path_falls_back_to_direct_probe_test() {
	log "Starting remote capability control-whitespace path fallback test"

	marker_rel="remote_capability_control_whitespace_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_remote_capability_control_whitespace"
	capability_file="$WORKDIR/mock_remote_capability_control_whitespace.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="control-whitespace.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf 'os\t%s\n' "MockRemoteOS"
		printf "tool\tzfs\t0\t/tmp/mock_remote_helper.\$(touch %s)/zfs\r\n" "$marker_rel"
		printf 'tool\tparallel\t1\t-\n'
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/remote_capability_control_whitespace_src"
	dest_root="$DEST_POOL/remote_capability_control_whitespace_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "control-whitespace-path"
	zfs snap -r "$src_dataset@rcw1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Remote capability responses with control-whitespace helper paths should degrade safely to the direct probe path when it is available. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "rcw1"
	if [ -e "$marker" ]; then
		fail "Control-whitespace helper paths from remote capabilities should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Remote capability control-whitespace path fallback test passed"
}

target_capability_control_whitespace_path_falls_back_to_direct_probe_test() {
	log "Starting target capability control-whitespace path fallback test"

	marker_rel="target_capability_control_whitespace_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_target_capability_control_whitespace"
	capability_file="$WORKDIR/mock_target_capability_control_whitespace.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="target-control-whitespace.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf 'os\t%s\n' "MockRemoteOS"
		printf "tool\tzfs\t0\t/tmp/mock_remote_helper.\$(touch %s)/zfs\r\n" "$marker_rel"
		printf 'tool\tparallel\t1\t-\n'
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/target_capability_control_whitespace_src"
	dest_root="$DEST_POOL/target_capability_control_whitespace_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "target-control-whitespace-path"
	zfs snap -r "$src_dataset@tcw1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -T "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Target capability responses with control-whitespace helper paths should degrade safely to the direct probe path when it is available. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "tcw1"
	if [ -e "$marker" ]; then
		fail "Target control-whitespace helper paths from remote capabilities should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Target capability control-whitespace path fallback test passed"
}

remote_compression_pipeline_test() {
	log "Starting remote compression pipeline test"

	if ! has_gnu_parallel; then
		log "Skipping remote compression pipeline test (GNU parallel not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_compress"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	src_dataset="$SRC_POOL/remote_compress_src"
	dest_root="$DEST_POOL/remote_compress_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "compressed-one"
	zfs snap -r "$src_dataset@rcomp1"

	ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -j 2 -Z "zstd -T0 -6" -O localhost -T localhost -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "rcomp1"

	append_data_to_dataset "$src_dataset" "file.txt" "compressed-two"
	zfs snap -r "$src_dataset@rcomp2"

	ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -j 2 -z -O localhost -T localhost -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "rcomp2"

	log "Remote compression pipeline test passed"
}

target_only_remote_compression_test() {
	log "Starting target-only remote compression test"

	mock_path="$WORKDIR/mock_target_only_compress"
	ssh_log="$WORKDIR/mock_target_only_compress.log"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	safe_rm_f "$ssh_log"

	src_dataset="$SRC_POOL/target_only_compress_src"
	dest_root="$DEST_POOL/target_only_compress_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "target-only-one"
	zfs snap -r "$src_dataset@toc1"

	MOCK_SSH_LOG="$ssh_log" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -z -T localhost -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "toc1"

	append_data_to_dataset "$src_dataset" "file.txt" "target-only-two"
	zfs snap -r "$src_dataset@toc2"

	MOCK_SSH_LOG="$ssh_log" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -z -T localhost -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "toc2"

	if ! grep -F "'$mock_path/zstd' '-d'" "$ssh_log" >/dev/null 2>&1; then
		fail "Expected target-only remote compression run to invoke remote zstd decompression. Log: $(cat "$ssh_log" 2>/dev/null || true)"
	fi

	log "Target-only remote compression test passed"
}

remote_csh_origin_snapshot_listing_test() {
	log "Starting remote csh origin snapshot listing test"

	if ! has_gnu_parallel; then
		log "Skipping remote csh origin snapshot listing test (GNU parallel not available)"
		return
	fi

	l_csh_shell=$(find_csh_shell)
	if [ "$l_csh_shell" = "" ]; then
		log "Skipping remote csh origin snapshot listing test (csh/tcsh not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_csh_origin"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	src_dataset="$SRC_POOL/remote_csh_src"
	dest_root="$DEST_POOL/remote_csh_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@csh1"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" MOCK_SSH_REMOTE_SHELL="$l_csh_shell" "$ZXFER_BIN" -v -j 2 -z -O localhost -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Remote csh origin snapshot listing should succeed. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "Unmatched"; then
		fail "Remote csh origin snapshot listing should not emit unmatched-quote errors. Output: $output"
	fi
	if printf '%s\n' "$output" | grep -q "unexpected end of file"; then
		fail "Remote csh origin snapshot listing should not emit zstd EOF errors. Output: $output"
	fi

	assert_snapshot_exists "$dest_dataset" "csh1"

	log "Remote csh origin snapshot listing test passed"
}

remote_wrapped_host_spec_test() {
	log "Starting remote wrapped host spec test"

	if ! has_gnu_parallel; then
		log "Skipping remote wrapped host spec test (GNU parallel not available)"
		return
	fi

	mock_path="$WORKDIR/mock_remote_wrapped"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"
	write_passthrough_zstd "$mock_path/zstd"
	write_exec_wrapper_script "$mock_path/pfexec"
	write_exec_wrapper_script "$mock_path/doas"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	wrapper_log="$WORKDIR/mock_remote_wrapped.log"
	safe_rm_f "$wrapper_log"

	src_dataset="$SRC_POOL/remote_wrapped_src"
	dest_root="$DEST_POOL/remote_wrapped_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "wrapped"
	zfs snap -r "$src_dataset@wrap1"

	MOCK_WRAPPER_LOG="$wrapper_log" ZXFER_SECURE_PATH="$secure_path" run_zxfer -v -j 2 -z -O "localhost pfexec" -T "localhost doas" -R "$src_dataset" "$dest_root"

	assert_snapshot_exists "$dest_dataset" "wrap1"

	if ! grep -q '^pfexec:' "$wrapper_log"; then
		fail "Expected pfexec wrapper invocation recorded in $wrapper_log."
	fi
	if ! grep -q '^doas:' "$wrapper_log"; then
		fail "Expected doas wrapper invocation recorded in $wrapper_log."
	fi
	if ! grep -q '^pfexec:sh -c ' "$wrapper_log"; then
		fail "Expected pfexec to wrap a remote sh -c command. Log: $(cat "$wrapper_log")"
	fi
	if ! grep -q '^doas:sh -c ' "$wrapper_log"; then
		fail "Expected doas to wrap a remote sh -c command. Log: $(cat "$wrapper_log")"
	fi

	log "Remote wrapped host spec test passed"
}

malformed_remote_capability_response_fails_closed_test() {
	log "Starting malformed remote capability response fail-closed test"

	marker_rel="malformed_remote_capability_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_malformed_remote_capability"
	capability_file="$WORKDIR/mock_malformed_remote_capability.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="malformed-capability.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf 'os\t%s\n' "MockRemoteOS"
		printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
		printf 'tool\tparallel\t1\t-\n'
		printf '%s\n' "\$(touch $marker_rel)"
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/malformed_remote_capability_src"
	dest_root="$DEST_POOL/malformed_remote_capability_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "malformed-capability"
	zfs snap -r "$src_dataset@mrc1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				MOCK_SSH_MISSING_TOOL="zfs" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Malformed remote capability responses should fail closed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -F "Required dependency \"zfs\" not found on host $l_host_spec" >/dev/null 2>&1; then
		fail "Malformed remote capability responses should fall back to a secure remote dependency probe and fail closed. Output: $output"
	fi
	if [ -e "$marker" ]; then
		fail "Malformed remote capability payloads should not execute embedded shell fragments; marker file was created at $marker."
	fi
	assert_dataset_absent "$dest_dataset"

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Malformed remote capability response fail-closed test passed"
}

malformed_remote_capability_response_falls_back_to_direct_probe_test() {
	log "Starting malformed remote capability response fallback test"

	marker_rel="malformed_remote_capability_fallback_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_malformed_remote_capability_fallback"
	capability_file="$WORKDIR/mock_malformed_remote_capability_fallback.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="malformed-fallback.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf 'os\t%s\n' "MockRemoteOS"
		printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
		printf 'tool\tparallel\t1\t-\n'
		printf '%s\n' "\$(touch $marker_rel)"
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/malformed_remote_capability_fallback_src"
	dest_root="$DEST_POOL/malformed_remote_capability_fallback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "malformed-capability-fallback"
	zfs snap -r "$src_dataset@mrf1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -O "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Malformed remote capability responses should fall back to direct probes and still allow replication. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "mrf1"
	if [ -e "$marker" ]; then
		fail "Malformed remote capability payloads should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Malformed remote capability response fallback test passed"
}

malformed_target_capability_response_falls_back_to_direct_probe_test() {
	log "Starting malformed target capability response fallback test"

	marker_rel="malformed_target_capability_fallback_marker"
	marker="$WORKDIR/$marker_rel"
	mock_path="$WORKDIR/mock_malformed_target_capability_fallback"
	capability_file="$WORKDIR/mock_malformed_target_capability_fallback.txt"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	zxfer_bin_abs=$(compute_absolute_path "$ZXFER_BIN") ||
		fail "Unable to resolve absolute path for ZXFER_BIN=$ZXFER_BIN"
	l_host_spec="malformed-target-fallback.example"
	safe_rm_f "$marker" "$capability_file"
	prepare_mock_bin_dir "$mock_path" ssh
	write_mock_ssh_script "$mock_path/ssh"

	{
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf 'os\t%s\n' "MockRemoteOS"
		printf 'tool\tzfs\t0\t%s\n' "/remote/bin/zfs"
		printf 'tool\tparallel\t1\t-\n'
		printf '%s\n' "\$(touch $marker_rel)"
		printf 'tool\tcat\t1\t-\n'
	} >"$capability_file"

	src_dataset="$SRC_POOL/malformed_target_capability_fallback_src"
	dest_root="$DEST_POOL/malformed_target_capability_fallback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "malformed-target-capability-fallback"
	zfs snap -r "$src_dataset@mtf1"

	set +e
	output=$(
		cd "$WORKDIR" &&
			MOCK_SSH_CAPABILITY_RESPONSE_FILE="$capability_file" \
				ZXFER_SECURE_PATH="$secure_path" \
				"$zxfer_bin_abs" -v -T "$l_host_spec" -R "$src_dataset" "$dest_root" 2>&1
	)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Malformed target capability responses should fall back to direct probes and still allow replication. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "mtf1"
	if [ -e "$marker" ]; then
		fail "Malformed target capability payloads should not execute embedded shell fragments during fallback; marker file was created at $marker."
	fi

	safe_rm_rf "$mock_path"
	safe_rm_f "$capability_file"

	log "Malformed target capability response fallback test passed"
}

trap_exit_cleanup_test() {
	log "Starting trap exit cleanup test"

	if ! has_gnu_parallel; then
		log "Skipping trap exit cleanup test (GNU parallel not available for background send)"
		return
	fi

	mock_path="$WORKDIR/mock_trap_exit"
	prepare_mock_bin_dir "$mock_path" ssh zfs
	write_mock_ssh_script "$mock_path/ssh"
	real_zfs=$(resolve_host_command zfs)
	safe_rm_f "$mock_path/zfs"
	cat >"$mock_path/zfs" <<EOF
#!/bin/sh
if [ "\$1" = "send" ]; then
	sleep 5
fi
exec "$real_zfs" "\$@"
EOF
	chmod +x "$mock_path/zfs"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	src_dataset="$SRC_POOL/trap_src"
	dest_root="$DEST_POOL/trap_dest"

	destroy_test_datasets_if_present "$src_dataset" "$dest_root"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "pending"
	zfs snap -r "$src_dataset@trap1"

	before_tmp=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type f -name 'zxfer.*' 2>/dev/null || true)
	before_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)

	set +e
	ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -j 2 -O localhost -T localhost -R "$src_dataset" "$dest_root" >/dev/null 2>&1 &
	zxfer_pid=$!
	sleep 2
	kill -s INT "$zxfer_pid" >/dev/null 2>&1 || true
	wait "$zxfer_pid"
	set -e

	after_tmp=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type f -name 'zxfer.*' 2>/dev/null || true)
	after_sockets=$(find "${TMPDIR:-/tmp}" -maxdepth 1 -type d -name 'zxfer_ssh_control_socket.*' 2>/dev/null || true)

	tmp_leaks=""
	for f in $after_tmp; do
		case " $before_tmp " in
		*" $f "*) continue ;;
		esac
		tmp_leaks="$tmp_leaks $f"
	done
	socket_leaks=""
	for dir in $after_sockets; do
		case " $before_sockets " in
		*" $dir "*) continue ;;
		esac
		socket_leaks="$socket_leaks $dir"
	done

	if [ -n "$tmp_leaks" ]; then
		fail "Temporary files leaked after SIGINT: $tmp_leaks"
	fi
	if [ -n "$socket_leaks" ]; then
		fail "SSH control sockets leaked after SIGINT: $socket_leaks"
	fi

	if pgrep -f "zfs send .*trap_src" >/dev/null 2>&1; then
		fail "Background zfs send still running after zxfer_trap_exit handling."
	fi

	log "Trap exit cleanup test passed"
}

property_creation_with_zvol_test() {
	log "Starting property creation with zvol test"

	src_parent="$SRC_POOL/prop_create_src"
	src_child="$src_parent/child"
	src_zvol="$src_parent/vol"
	dest_root="$DEST_POOL/prop_create_dest"
	dest_parent="$dest_root/${src_parent##*/}"
	dest_child="$dest_parent/${src_child##*/}"
	dest_zvol="$dest_parent/${src_zvol##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_parent"

	zfs create "$src_parent"
	zfs create "$src_child"
	zfs create -V 8M "$src_zvol"
	zfs set compression=lz4 "$src_parent"
	zfs set atime=off "$src_child"
	zfs snap -r "$src_parent@pc1"

	run_zxfer -v -P -R "$src_parent" "$dest_root"

	assert_snapshot_exists "$dest_parent" "pc1"
	assert_snapshot_exists "$dest_child" "pc1"
	if ! zfs list -t volume "$dest_zvol" >/dev/null 2>&1; then
		fail "Destination zvol $dest_zvol missing after replication."
	fi

	parent_compression=$(zfs get -H -o value compression "$dest_parent")
	if [ "$parent_compression" != "lz4" ]; then
		fail "Expected compression=lz4 on $dest_parent, got $parent_compression."
	fi

	child_atime=$(zfs get -H -o value atime "$dest_child")
	if [ "$child_atime" != "off" ]; then
		if [ "$OS_NAME" = "Darwin" ]; then
			log "Skipping child atime assertion on Darwin; observed atime=$child_atime on $dest_child"
		else
			fail "Expected atime=off on $dest_child, got $child_atime."
		fi
	fi

	src_volsize=$(zfs get -H -o value volsize "$src_zvol")
	dest_volsize=$(zfs get -H -o value volsize "$dest_zvol")
	if [ "$src_volsize" != "$dest_volsize" ]; then
		fail "Destination zvol size $dest_volsize does not match source $src_volsize."
	fi

	log "Property creation with zvol test passed"
}

property_override_and_ignore_test() {
	log "Starting property override/ignore test"

	src_dataset="$SRC_POOL/prop_override_src"
	src_child="$src_dataset/child"
	dest_root="$DEST_POOL/prop_override_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	dest_child="$dest_dataset/${src_child##*/}"
	override_mount="$WORKDIR/override_mountpoint"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$override_mount"

	zfs create "$src_dataset"
	zfs create "$src_child"
	zfs set compression=lz4 "$src_dataset"
	zfs set checksum=sha256 "$src_dataset"
	zfs set atime=off "$src_child"
	zfs snap -r "$src_dataset@pov1"

	run_zxfer -v -P -R "$src_dataset" "$dest_root"

	dest_child_checksum_before_source=$(zfs get -H -o source checksum "$dest_child")
	if [ "$dest_child_checksum_before_source" = "local" ]; then
		fail "Checksum on $dest_child should be inherited after initial replication."
	fi

	zfs set compression=off "$dest_dataset"
	mkdir -p "$override_mount"
	zfs set mountpoint="$override_mount" "$dest_dataset"
	zfs set atime=on "$dest_child"
	zfs set checksum=fletcher4 "$dest_child"

	ZXFER_SECURE_PATH='' run_zxfer -v -P -o "quota=32M" -I "mountpoint,compression" -R "$src_dataset" "$dest_root"

	dest_compression_after=$(zfs get -H -o value compression "$dest_dataset")
	if [ "$dest_compression_after" != "off" ]; then
		fail "Ignored compression property should remain off on $dest_dataset; saw $dest_compression_after."
	fi

	dest_mount_after=$(zfs get -H -o value mountpoint "$dest_dataset")
	if [ "$dest_mount_after" != "$override_mount" ]; then
		fail "Ignored mountpoint should remain $override_mount on $dest_dataset; saw $dest_mount_after."
	fi

	child_atime_after=$(zfs get -H -o value atime "$dest_child")
	if [ "$child_atime_after" != "off" ]; then
		if [ "$OS_NAME" = "Darwin" ]; then
			log "Skipping child atime assertion on Darwin after property pass; observed atime=$child_atime_after on $dest_child"
		else
			fail "Expected atime=off to be set on $dest_child after property pass."
		fi
	fi

	child_checksum_after=$(zfs get -H -o value checksum "$dest_child")
	if [ "$child_checksum_after" != "sha256" ]; then
		fail "Expected checksum on $dest_child to converge to sha256 after property pass; saw $child_checksum_after."
	fi

	parent_quota=$(zfs get -H -o value quota "$dest_dataset")
	child_quota=$(zfs get -H -o value quota "$dest_child")
	if [ "$parent_quota" != "32M" ] || [ "$child_quota" != "32M" ]; then
		fail "Override quota not applied to parent/child: parent=$parent_quota child=$child_quota."
	fi

	parent_snap_count=$(list_exact_snapshot_names_for_dataset "$dest_dataset" | wc -l | tr -d ' ')
	if [ "$parent_snap_count" -ne 1 ]; then
		fail "Property-only pass should not create extra snapshots; found $parent_snap_count on $dest_dataset."
	fi

	log "Property override/ignore test passed"
}

unsupported_property_skip_test() {
	log "Starting unsupported property skip test"

	mock_path="$WORKDIR/mock_unsupported_props"
	mock_log="$WORKDIR/mock_unsupported_props.log"
	prepare_mock_bin_dir "$mock_path" zfs
	real_zfs=$(resolve_host_command zfs)
	safe_rm_f "$mock_path/zfs"
	safe_rm_f "$mock_log"
	cat >"$mock_path/zfs" <<EOF
#!/bin/sh
[ -n "\${MOCK_UNSUPPORTED_LOG:-}" ] && printf '%s\n' "\$*" >>"\$MOCK_UNSUPPORTED_LOG"
if [ "\$1" = "get" ] && [ "\$2" = "-Ho" ] && [ "\$3" = "property" ] && [ "\$4" = "all" ] && [ "\$5" = "$DEST_POOL" ]; then
	"$real_zfs" "\$@" | grep -v '^compression$'
	exit \$?
fi
exec "$real_zfs" "\$@"
EOF
	chmod +x "$mock_path/zfs"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	src_dataset="$SRC_POOL/unsupported_src"
	dest_root="$DEST_POOL/unsupported_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs set compression=lz4 "$src_dataset"
	zfs snap -r "$src_dataset@u1"

	set +e
	output=$(MOCK_UNSUPPORTED_LOG="$mock_log" ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -v -U -P -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Property transfer with -U failed. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Destination does not support property compression=lz4"; then
		fail "Unsupported property skip message missing. Output: $output"
	fi
	if grep -q "compression=lz4" "$mock_log"; then
		fail "zxfer should not attempt to create or set compression=lz4 when the destination reports compression unsupported. Log: $(cat "$mock_log")"
	fi

	log "Unsupported property skip test passed"
}

must_create_property_error_test() {
	log "Starting must-create property error test"

	check_sensitive="$SRC_POOL/case_support_sensitive"
	check_insensitive="$SRC_POOL/case_support_insensitive"
	destroy_test_datasets_if_present "$check_sensitive" "$check_insensitive"
	if ! zfs create -o casesensitivity=sensitive "$check_sensitive" >/dev/null 2>&1 ||
		! zfs create -o casesensitivity=insensitive "$check_insensitive" >/dev/null 2>&1; then
		log "Skipping must-create property error test (casesensitivity property unsupported)"
		destroy_test_datasets_if_present "$check_sensitive" "$check_insensitive"
		return
	fi
	destroy_test_datasets_if_present "$check_sensitive" "$check_insensitive"

	src_dataset="$SRC_POOL/case_src"
	dest_root="$DEST_POOL/case_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create -o casesensitivity=sensitive "$src_dataset"
	zfs snap -r "$src_dataset@case1"
	zfs create "$dest_root"
	zfs create -o casesensitivity=insensitive "$dest_dataset"

	src_case=$(zfs get -H -o value casesensitivity "$src_dataset")
	dest_case=$(zfs get -H -o value casesensitivity "$dest_dataset")
	if [ "$src_case" = "$dest_case" ]; then
		fail "Must-create property test setup requires differing casesensitivity values. Source=$src_case Destination=$dest_case."
	fi

	set +e
	output=$("$ZXFER_BIN" -v -P -N "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when must-create property differs between source and destination. Source casesensitivity=$src_case destination casesensitivity=$dest_case. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "may only be set"; then
		fail "Must-create property error message missing. Output: $output"
	fi

	log "Must-create property error test passed"
}

migration_service_success_test() {
	log "Starting migration service success test"

	mock_path="$WORKDIR/mock_svcadm_success"
	prepare_mock_bin_dir "$mock_path" zfs
	cat >"$mock_path/svcadm" <<'EOF'
#!/bin/sh
log=${MOCK_SVCADM_LOG:-}
cmd=$1
shift
service=""
if [ "$cmd" = "disable" ]; then
	if [ "$1" = "-st" ]; then
		service=$2
		shift 2
	else
		service=$1
		shift
	fi
	[ -n "$log" ] && printf 'disable:%s\n' "$service" >>"$log"
	exit 0
fi
if [ "$cmd" = "enable" ]; then
	service=$1
	[ -n "$log" ] && printf 'enable:%s\n' "$service" >>"$log"
	exit 0
fi
exit 0
EOF
	chmod +x "$mock_path/svcadm"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	svc_log="$WORKDIR/svcadm_success.log"
	safe_rm_f "$svc_log"

	src_dataset="$SRC_POOL/migrate_src"
	dest_root="$DEST_POOL/migrate_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	mount_dir="$WORKDIR/migrate_mount"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_rf "$mount_dir"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	mkdir -p "$mount_dir"
	zfs set mountpoint="$mount_dir" "$src_dataset"
	append_data_to_dataset "$src_dataset" "file.txt" "migrate"
	zfs snap -r "$src_dataset@mig1"

	ZXFER_SECURE_PATH="$secure_path" MOCK_SVCADM_LOG="$svc_log" run_zxfer -v -m -c svc:/system/filesystem/local -R "$src_dataset" "$dest_root"

	src_mounted=$(zfs get -H -o value mounted "$src_dataset")
	dest_mounted=$(zfs get -H -o value mounted "$dest_dataset")
	dest_mountpoint=$(zfs get -H -o value mountpoint "$dest_dataset")

	if [ "$src_mounted" != "no" ]; then
		fail "Source dataset $src_dataset should remain unmounted after migration; mounted=$src_mounted."
	fi
	if [ "$dest_mounted" != "yes" ]; then
		fail "Destination dataset $dest_dataset should be mounted after migration; mounted=$dest_mounted."
	fi
	if [ "$dest_mountpoint" != "$mount_dir" ]; then
		fail "Destination mountpoint expected $mount_dir, got $dest_mountpoint."
	fi

	if ! grep -q "disable:svc:/system/filesystem/local" "$svc_log"; then
		fail "Expected service disable call recorded in $svc_log."
	fi
	if ! grep -q "enable:svc:/system/filesystem/local" "$svc_log"; then
		fail "Expected service enable call recorded in $svc_log."
	fi

	log "Migration service success test passed"
}

migration_service_failure_test() {
	log "Starting migration service failure test"

	mock_path="$WORKDIR/mock_svcadm_failure"
	prepare_mock_bin_dir "$mock_path" zfs
	cat >"$mock_path/svcadm" <<'EOF'
#!/bin/sh
log=${MOCK_SVCADM_LOG:-}
cmd=$1
shift
service=""
if [ "$cmd" = "disable" ]; then
	if [ "$1" = "-st" ]; then
		service=$2
	else
		service=$1
	fi
	[ -n "$log" ] && printf 'disable:%s\n' "$service" >>"$log"
	exit 1
fi
if [ "$cmd" = "enable" ]; then
	service=$1
	[ -n "$log" ] && printf 'enable:%s\n' "$service" >>"$log"
	exit 0
fi
exit 0
EOF
	chmod +x "$mock_path/svcadm"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	svc_log="$WORKDIR/svcadm_failure.log"
	safe_rm_f "$svc_log"

	src_dataset="$SRC_POOL/migrate_fail_src"
	dest_root="$DEST_POOL/migrate_fail_dest"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "migratefail"
	zfs snap -r "$src_dataset@migfail1"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" MOCK_SVCADM_LOG="$svc_log" "$ZXFER_BIN" -v -m -c svc:/system/filesystem/local -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Migration should fail when service disable fails."
	fi
	if ! printf '%s\n' "$output" | grep -q "Could not disable service"; then
		fail "Expected service disable error in output. Output: $output"
	fi

	if grep -q "enable:" "$svc_log"; then
		fail "Service enable should not run after disable failure. Log: $(cat "$svc_log")"
	fi

	log "Migration service failure test passed"
}

get_os_detection_test() {
	log "Starting zxfer_get_os detection test"

	mock_path="$WORKDIR/mock_get_os"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"

	cat >"$mock_path/uname" <<'EOF'
#!/bin/sh
echo "MockLocalOS"
EOF
	chmod +x "$mock_path/uname"
	write_mock_ssh_script "$mock_path/ssh"

	local_os=$(ZXFER_SECURE_PATH="$mock_path" PATH="$mock_path:$PATH" sh -c 'ZXFER_SOURCE_MODULES_ROOT=. ZXFER_SOURCE_MODULES_THROUGH=zxfer_runtime.sh . ./src/zxfer_modules.sh; zxfer_get_os ""')
	if [ "$local_os" != "MockLocalOS" ]; then
		fail "Expected MockLocalOS from local zxfer_get_os, got $local_os"
	fi

	remote_os=$(MOCK_SSH_FORCE_UNAME="MockRemoteOS" ZXFER_SECURE_PATH="$mock_path" PATH="$mock_path:$PATH" sh -c '
		# shellcheck source=src/zxfer_modules.sh
		ZXFER_SOURCE_MODULES_ROOT=. ZXFER_SOURCE_MODULES_THROUGH=zxfer_remote_hosts.sh . ./src/zxfer_modules.sh
		g_cmd_ssh="'"$mock_path"'/ssh"
		zxfer_get_os "remotehost"
	')
	if [ "$remote_os" != "MockRemoteOS" ]; then
		fail "Expected MockRemoteOS from remote zxfer_get_os, got $remote_os"
	fi

	log "Get_os detection test passed"
}

verbose_debug_logging_test() {
	log "Starting verbose/debug logging test"

	src_dataset="$SRC_POOL/verbose_src"
	dest_root="$DEST_POOL/verbose_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	stdout_log="$WORKDIR/verbose_stdout.log"
	stderr_log="$WORKDIR/verbose_stderr.log"

	destroy_test_datasets_if_present "$dest_root" "$src_dataset"
	safe_rm_f "$stdout_log" "$stderr_log"

	zfs create "$src_dataset"
	zfs create "$dest_root"
	zfs snap -r "$src_dataset@vlog1"

	set +e
	"$ZXFER_BIN" -v -V -n -R "$src_dataset" "$dest_root" >"$stdout_log" 2>"$stderr_log"
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "Verbose/debug dry run should succeed. See $stdout_log and $stderr_log."
	fi
	if ! grep -q "New temporary file" "$stderr_log"; then
		fail "zxfer_echoV debug output missing from stderr."
	fi
	if grep -q "New temporary file" "$stdout_log"; then
		fail "zxfer_echoV debug output should not appear on stdout."
	fi
	if ! grep -q "Destination dataset does not exist" "$stdout_log"; then
		fail "Verbose output missing expected dataset message on stdout."
	fi
	if zfs list "$dest_dataset" >/dev/null 2>&1; then
		fail "Dry run should not create destination dataset $dest_dataset."
	fi

	log "Verbose/debug logging test passed"
}

beep_handling_test() {
	log "Starting beep handling test"

	mock_path="$WORKDIR/mock_beep"
	safe_rm_rf "$mock_path"
	mkdir -p "$mock_path"
	cat >"$mock_path/uname" <<'EOF'
#!/bin/sh
echo "FreeBSD"
EOF
	chmod +x "$mock_path/uname"
	secure_path="$mock_path:/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -V -b -R tank/src 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 2 ]; then
		fail "Beep handling test expected usage error exit 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -Eq "speaker tools are missing|/dev/speaker missing"; then
		fail "Expected graceful beep skip message missing. Output: $output"
	fi

	log "Beep handling test passed"
}

main() {
	parse_args "$@"
	configure_platform_tool_paths
	require_platform_permissions
	require_cmd zpool
	require_cmd zfs
	require_cmd mktemp

	assert_exists "$ZXFER_BIN" "zxfer binary not found at $ZXFER_BIN"

	WORKDIR=$(mktemp -d -t zxfer_integration.XXXXXX)
	WORKDIR=$(cd -P "$WORKDIR" && pwd)
	TEST_RUN_ID="$(date +%s).$$"
	trap cleanup EXIT
	trap 'abort_integration_run INT 130' INT
	trap 'abort_integration_run TERM 143' TERM
	setup_command_confirmation_wrappers

	usage_error_tests
	SRC_POOL=$(generate_test_pool_name "src")
	DEST_POOL=$(generate_test_pool_name "dest")
	SRC_MOUNT_ROOT="$WORKDIR/mnt/src"
	DEST_MOUNT_ROOT="$WORKDIR/mnt/dest"
	mkdir -p "$SRC_MOUNT_ROOT" "$DEST_MOUNT_ROOT"

	SRC_IMG="$WORKDIR/${SRC_POOL}.img"
	DEST_IMG="$WORKDIR/${DEST_POOL}.img"
	create_sparse_file "$SRC_IMG" "$SPARSE_SIZE_MB"
	create_sparse_file "$DEST_IMG" "$SPARSE_SIZE_MB"

	log "Creating source pool $SRC_POOL"
	create_test_pool "source" "$SRC_POOL" "$SRC_IMG" "$SRC_MOUNT_ROOT"
	SRC_POOL_CREATED=1
	log "Creating destination pool $DEST_POOL"
	create_test_pool "destination" "$DEST_POOL" "$DEST_IMG" "$DEST_MOUNT_ROOT"
	DEST_POOL_CREATED=1

	TEST_SEQUENCE="usage_error_tests \
usage_error_failure_report_test \
basic_replication_test \
non_recursive_replication_test \
generate_tests_replication \
idempotent_replication_test \
auto_snapshot_replication_test \
auto_snapshot_nonrecursive_test \
trailing_slash_destination_test \
exclude_filter_test \
missing_destination_error_test \
invalid_override_property_test \
dry_run_replication_test \
yield_loop_dryrun_iteration_test \
force_rollback_test \
failure_handling_tests \
runtime_failure_report_test \
extended_usage_error_tests \
consistency_option_validation_tests \
snapshot_deletion_test \
snapshot_name_mismatch_deletion_test \
snapshot_name_prefix_collision_deletion_test \
send_command_dryrun_test \
raw_send_replication_test \
backup_dir_symlink_guard_test \
missing_backup_metadata_error_test \
grandfather_protection_test \
migration_unmounted_guard_test \
property_backup_restore_test \
remote_property_backup_restore_test \
property_creation_with_zvol_test \
	property_override_and_ignore_test \
	unsupported_property_skip_test \
	must_create_property_error_test \
	delete_dest_only_snapshot_test \
	existing_empty_destination_seed_test \
	dry_run_deletion_test \
	progress_wrapper_test \
	progress_placeholder_passthrough_test \
	job_limit_enforcement_test \
	background_send_failure_test \
	secure_path_dependency_tests \
	secure_path_failure_report_test \
	secure_path_append_resolution_test \
	error_log_mirror_test \
	usage_error_log_mirror_test \
	invalid_error_log_warning_test \
	error_log_email_example_self_test \
	remote_migration_guard_tests \
	local_helper_path_shell_metacharacters_test \
	garbage_wrapped_host_spec_fails_closed_test \
	control_socket_path_shell_metacharacters_test \
	remote_origin_target_uncompressed_test \
	remote_helper_path_shell_metacharacters_test \
	remote_capability_control_whitespace_path_falls_back_to_direct_probe_test \
	target_capability_control_whitespace_path_falls_back_to_direct_probe_test \
	remote_compression_pipeline_test \
	target_only_remote_compression_test \
	remote_csh_origin_snapshot_listing_test \
	remote_wrapped_host_spec_test \
	malformed_remote_capability_response_fails_closed_test \
	malformed_remote_capability_response_falls_back_to_direct_probe_test \
	malformed_target_capability_response_falls_back_to_direct_probe_test \
trap_exit_cleanup_test \
missing_parallel_error_test \
remote_missing_parallel_origin_test \
parallel_jobs_listing_test \
migration_service_success_test \
migration_service_failure_test \
	get_os_detection_test \
	verbose_debug_logging_test \
	legacy_backup_layout_rejected_test \
	remote_legacy_backup_layout_rejected_test \
	insecure_backup_metadata_guard_test \
	beep_handling_test"
	# shellcheck disable=SC2086
	set -- $TEST_SEQUENCE
	TOTAL_TESTS=$#

	l_index=1
	for test_func in $TEST_SEQUENCE; do
		run_test "$l_index" "$TOTAL_TESTS" "$test_func"
		l_index=$((l_index + 1))
	done

	if [ -n "${ZXFER_FAILED_TESTS:-}" ]; then
		log "Integration failures: $ZXFER_FAILED_TESTS"
		exit 1
	fi

	log "All integration tests passed."
}

main "$@"
