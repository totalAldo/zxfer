#!/bin/sh
#
# Integration tests for zxfer using temporary ZFS pools backed by sparse files.
#

set -eu

ZXFER_BIN=${ZXFER_BIN:-"./zxfer"}
SPARSE_SIZE_MB=${SPARSE_SIZE_MB:-256}
OS_NAME=$(uname -s)
MD_DEVICES=""
MD_DRIVER_READY=0

require_cmd() {
	l_cmd=$1
	if ! command -v "$l_cmd" >/dev/null 2>&1; then
		echo "Missing required command: $l_cmd" >&2
		exit 1
	fi
}

log() {
	printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

fail() {
	echo "ERROR: $*" >&2
	exit 1
}

assert_exists() {
	l_path=$1
	l_msg=$2
	if [ ! -e "$l_path" ]; then
		fail "$l_msg"
	fi
}

assert_snapshot_exists() {
	l_dataset=$1
	l_snapshot=$2
	if ! zfs list -t snapshot "$l_dataset@$l_snapshot" >/dev/null 2>&1; then
		fail "Expected snapshot $l_dataset@$l_snapshot to exist."
	fi
}

require_root() {
	if [ "$(id -u)" -ne 0 ]; then
		fail "Integration tests must be run as root."
	fi
}

create_sparse_file() {
	l_file=$1
	l_size_mb=$2

	if command -v truncate >/dev/null 2>&1; then
		truncate -s "${l_size_mb}M" "$l_file"
	else
		dd if=/dev/zero of="$l_file" bs=1M count=0 seek="$l_size_mb" >/dev/null 2>&1
	fi
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

run_zxfer() {
	log "Running: $ZXFER_BIN $*"
	$ZXFER_BIN "$@"
}

cleanup() {
	set +e
	if [ -n "${SRC_POOL:-}" ] && zpool list "$SRC_POOL" >/dev/null 2>&1; then
		log "Destroying source pool $SRC_POOL"
		zpool destroy -f "$SRC_POOL"
	fi
	if [ -n "${DEST_POOL:-}" ] && zpool list "$DEST_POOL" >/dev/null 2>&1; then
		log "Destroying destination pool $DEST_POOL"
		zpool destroy -f "$DEST_POOL"
	fi
	for dev in $MD_DEVICES; do
		[ -n "$dev" ] || continue
		if command -v mdconfig >/dev/null 2>&1; then
			unit=${dev#md}
			log "Detaching memory disk $dev"
			mdconfig -d -u "$unit" >/dev/null 2>&1 || true
		fi
	done
	rm -rf "${WORKDIR:-}"
}

ensure_md_driver_loaded() {
	if [ "$MD_DRIVER_READY" -eq 1 ]; then
		return
	fi
	if [ -e /dev/mdctl ]; then
		MD_DRIVER_READY=1
		return
	fi
	for module in geom_md md; do
		if kldstat -n "$module" >/dev/null 2>&1; then
			if [ -e /dev/mdctl ]; then
				MD_DRIVER_READY=1
				return
			fi
		elif kldload "$module" >/dev/null 2>&1; then
			if [ -e /dev/mdctl ]; then
				MD_DRIVER_READY=1
				return
			fi
		fi
	done
	fail "Memory disk driver is unavailable; load geom_md(4) (md.ko) and retry."
}

create_md_device() {
	l_size_mb=$1
	ensure_md_driver_loaded
	md_unit=$(mdconfig -a -t swap -s "${l_size_mb}m") || fail "Failed to create memory disk of size ${l_size_mb}m."
	echo "$md_unit"
}

basic_replication_test() {
	# Exercise zxfer's standard ZFS send/receive mode with -R so recursive
	# snapshots and incremental updates propagate from source to destination.
	log "Starting basic replication test"
	src_dataset="$SRC_POOL/srcdata"
	dest_root="$DEST_POOL/replica"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$DEST_POOL/replica" >/dev/null 2>&1 || true
	zfs destroy -r "$SRC_POOL/srcdata" >/dev/null 2>&1 || true

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

generate_tests_replication() {
	# Mirror the old tests/generateTests.sh workflow using the integration pools.
	log "Starting multi-dataset replication test"

	src_parent="$SRC_POOL/zxfer_tests"
	src_dataset="$src_parent/src"
	dest_root="$DEST_POOL/zxfer_tests"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$src_parent" >/dev/null 2>&1 || true
	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true

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

	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true
	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true

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

	snapshots_before=$(zfs list -H -o name -t snapshot "$dest_dataset")

	run_zxfer -v -R "$src_dataset" "$dest_root"

	snapshots_after=$(zfs list -H -o name -t snapshot "$dest_dataset")

	if [ "$snapshots_before" != "$snapshots_after" ]; then
		fail "zxfer should be idempotent; destination snapshots changed after a no-op run."
	fi

	log "Idempotent replication test passed"
}

parallel_jobs_test() {
	# Ensure zxfer operates correctly when multiple zfs send jobs run in parallel.
	log "Starting parallel jobs test"

	if ! command -v parallel >/dev/null 2>&1; then
		log "Skipping parallel jobs test: GNU parallel not installed."
		return
	fi

	src_root="$SRC_POOL/parallel_src"
	dest_root="$DEST_POOL/parallel_dest"
	dest_dataset_root="$dest_root/${src_root##*/}"

	zfs destroy -r "$src_root" >/dev/null 2>&1 || true
	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true

	zfs create "$src_root"
	zfs create "$dest_root"
	append_data_to_dataset "$src_root" "root.txt" "parallel root snapshot"
	zfs snap "$src_root@root_parallel"

	for child in 1 2 3; do
		child_dataset="$src_root/dataset$child"
		zfs create "$child_dataset"

		append_data_to_dataset "$child_dataset" "parallel.txt" "dataset$child snapshot one"
		zfs snap -r "$child_dataset@psnap1"
		append_data_to_dataset "$child_dataset" "parallel.txt" "dataset$child snapshot two"
		zfs snap -r "$child_dataset@psnap2"
	done

	run_zxfer -v -j 3 -R "$src_root" "$dest_root"

	for child in 1 2 3; do
		dest_dataset="$dest_dataset_root/dataset$child"
		assert_snapshot_exists "$dest_dataset" "psnap1"
		assert_snapshot_exists "$dest_dataset" "psnap2"
	done

	log "Parallel jobs test passed"
}

main() {
	require_root
	require_cmd zpool
	require_cmd zfs
	require_cmd mktemp
	if [ "$OS_NAME" = "FreeBSD" ]; then
		require_cmd mdconfig
		require_cmd kldstat
		require_cmd kldload
	fi

	assert_exists "$ZXFER_BIN" "zxfer binary not found at $ZXFER_BIN"

	WORKDIR=$(mktemp -d -t zxfer_integration.XXXXXX)
	trap cleanup EXIT INT TERM

	SRC_POOL="zxfer_src_$$"
	DEST_POOL="zxfer_dest_$$"

	if [ "$OS_NAME" = "FreeBSD" ]; then
		SRC_IMG=$(create_md_device "$SPARSE_SIZE_MB")
		DEST_IMG=$(create_md_device "$SPARSE_SIZE_MB")
		MD_DEVICES="$MD_DEVICES /dev/$SRC_IMG /dev/$DEST_IMG"
		SRC_IMG="/dev/$SRC_IMG"
		DEST_IMG="/dev/$DEST_IMG"
	else
		SRC_IMG="$WORKDIR/${SRC_POOL}.img"
		DEST_IMG="$WORKDIR/${DEST_POOL}.img"
		create_sparse_file "$SRC_IMG" "$SPARSE_SIZE_MB"
		create_sparse_file "$DEST_IMG" "$SPARSE_SIZE_MB"
	fi

	log "Creating source pool $SRC_POOL"
	zpool create "$SRC_POOL" "$SRC_IMG"
	log "Creating destination pool $DEST_POOL"
	zpool create "$DEST_POOL" "$DEST_IMG"

	basic_replication_test
	generate_tests_replication
	idempotent_replication_test
	parallel_jobs_test

	log "All integration tests passed."
}

main "$@"
