#!/bin/sh
# Shared file-backed ZFS pool fixtures with strict ownership and path guards.

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
