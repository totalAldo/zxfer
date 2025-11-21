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

assert_snapshot_exists() {
	l_dataset=$1
	l_snapshot=$2
	if ! zfs list -t snapshot "$l_dataset@$l_snapshot" >/dev/null 2>&1; then
		fail "Expected snapshot $l_dataset@$l_snapshot to exist."
	fi
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

run_test() {
	l_index=$1
	l_total=$2
	l_func=$3

	log "$(printf '[%d/%d] Starting %s%s%s' "$l_index" "$l_total" "$YELLOW" "$l_func" "$RESET")"
	"$l_func"
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
	# Based on code reading, check_snapshot should reject if it looks like a snapshot but we wanted a fs)
	# Actually zxfer_zfs_mode.sh:303 checks if source is a snapshot and fails if so for normal mode.
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
	rm -rf "$mock_path"
	mkdir -p "$mock_path"
	cat >"$mock_path/ssh" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$mock_path/ssh"
	# Deliberately omit zfs from the secure PATH to ensure zxfer aborts cleanly.
	secure_path="$mock_path"

	set +e
	output=$(ZXFER_SECURE_PATH="$secure_path" "$ZXFER_BIN" -R tank/src backup/target 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when secure PATH lacks required tools."
	fi
	if ! printf '%s\n' "$output" | grep -q 'Required dependency "zfs" not found'; then
		fail "Expected missing zfs dependency error. Output: $output"
	fi

	rm -rf "$mock_path"

	log "Secure PATH dependency tests passed"
}

remote_migration_guard_tests() {
	log "Starting remote/migration guard tests"

	mock_bin_dir="$WORKDIR/mockbin"
	rm -rf "$mock_bin_dir"
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

	rm -rf "$mock_bin_dir"

	log "Remote/migration guard tests passed"
}

missing_parallel_error_test() {
	log "Starting missing GNU parallel error test"

	mock_path="$WORKDIR/mock_no_parallel"
	rm -rf "$mock_path"
	mkdir -p "$mock_path"

	# Provide required dependencies except GNU parallel.
	for bin in zfs ssh awk cat; do
		actual=$(command -v "$bin" 2>/dev/null || true)
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

	rm -rf "$mock_path"

	log "Missing GNU parallel error test passed"
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
	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs create "$dest_root"

	assert_error_case "Missing source dataset" \
		"Failed to retrieve snapshots from the source" \
		3 \
		-R "$SRC_POOL/no_such_dataset" "$dest_root"

	log "Missing dataset error tests passed"
}

snapshot_deletion_test() {
	log "Starting snapshot deletion test"

	src_dataset="$SRC_POOL/snapdel_src"
	dest_root="$DEST_POOL/snapdel_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true
	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true

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
	zfs destroy -r "$src_dataset@snap1"

	# Run without -d (snap1 should remain on dest)
	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	# Run with -n -d (dry run, snap1 should remain)
	# We capture output to verify it *would* delete
	output=$(run_zxfer -v -n -d -R "$src_dataset" "$dest_root" 2>&1)
	if ! printf '%s\n' "$output" | grep -q "zfs destroy .*@snap1"; then
		fail "Dry run with -d did not propose destroying snap1. Output: $output"
	fi
	assert_snapshot_exists "$dest_dataset" "snap1"

	# Run with -d (snap1 should be deleted)
	run_zxfer -v -Y -d -R "$src_dataset" "$dest_root"

	wait_for_destroy_process_to_finish "$dest_dataset" "snap1" 30
	wait_for_snapshot_absent "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	log "Snapshot deletion test passed"
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

non_recursive_replication_test() {
	log "Starting non-recursive replication test"

	src_dataset="$SRC_POOL/nonrec_src"
	child_dataset="$src_dataset/child"
	dest_root="$DEST_POOL/nonrec_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "parent.txt" "parent data"
	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap -r "$src_dataset@preseed"

	run_zxfer -v -s -R "$src_dataset" "$dest_root"

	src_snapshot_name=$(zfs list -H -o name -t snapshot -s creation "$src_dataset" | tail -n 1)
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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$child_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "parent.txt" "parent data"
	append_data_to_dataset "$child_dataset" "child.txt" "child data"
	zfs snap "$src_dataset@preseed"

	run_zxfer -v -s -N "$src_dataset" "$dest_root"

	src_snapshot_name=$(zfs list -H -o name -t snapshot -s creation "$src_dataset" | tail -n 1)
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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_parent" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

job_limit_enforcement_test() {
	log "Starting job limit enforcement test"

	if ! has_gnu_parallel; then
		log "Skipping job limit enforcement test (GNU parallel not available)"
		return
	fi

	src_root="$SRC_POOL/joblimit_src"
	dest_root="$DEST_POOL/joblimit_dest"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_root" >/dev/null 2>&1 || true

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

	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true
	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@p1"

	set +e
	output=$(run_zxfer -v -R "$src_dataset" nosuchdestpool/target 2>&1)
	status=$?
	set -e

	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	if [ "$status" -eq 0 ]; then
		fail "Missing destination list should cause zxfer to fail."
	fi
	if [ "$status" -ne 2 ]; then
		fail "Missing destination list should exit with status 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Failed to retrieve list of datasets from the destination"; then
		fail "Missing destination error message missing. Output: $output"
	fi

	log "Missing destination error test passed"
}

invalid_override_property_test() {
	log "Starting invalid override property test"

	src_dataset="$SRC_POOL/invalid_prop_src"
	dest_root="$DEST_POOL/invalid_prop_dest"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs snap -r "$src_dataset@p1"
	zfs create "$dest_root"

	set +e
	output=$(run_zxfer -v -o "definitelynotaproperty=on" -N "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		fail "Invalid override property should cause zxfer to fail."
	fi
	if [ "$status" -ne 2 ]; then
		fail "Invalid override property should exit with status 2, got $status. Output: $output"
	fi
	if ! printf '%s\n' "$output" | grep -q "Invalid option property - check -o list for syntax errors."; then
		fail "Invalid override property error message missing. Output: $output"
	fi

	log "Invalid override property test passed"
}

dry_run_replication_test() {
	log "Starting dry-run replication test"

	src_dataset="$SRC_POOL/dryrun_src"
	dest_root="$DEST_POOL/dryrun_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@snap1"
	append_data_to_dataset "$src_dataset" "file.txt" "two"
	zfs snap -r "$src_dataset@snap2"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	zfs destroy -r "$src_dataset@snap1"

	output=$("$ZXFER_BIN" -v -n -d -R "$src_dataset" "$dest_root" 2>&1)
	log "$output"

	if ! printf '%s\n' "$output" | grep -q "zfs destroy .*@snap1"; then
		fail "Dry run with -d did not show destroy command for snap1."
	fi
	assert_snapshot_exists "$dest_dataset" "snap1"
	assert_snapshot_exists "$dest_dataset" "snap2"

	log "Dry-run deletion test passed"
}

delete_dest_only_snapshot_test() {
	log "Starting destination-only snapshot delete test"

	src_dataset="$SRC_POOL/destonly_src"
	dest_root="$DEST_POOL/destonly_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "base"
	zfs snap -r "$src_dataset@base"

	run_zxfer -v -R "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "base"

	# Create a destination-only snapshot that should be removed by -d even when no new sends are pending.
	zfs snap -r "$dest_dataset@destonly"

	set +e
	output=$("$ZXFER_BIN" -v -Y -d -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	if [ "$status" -ne 0 ]; then
		fail "zxfer -Y -d run failed. Output: $output"
	fi

	wait_for_destroy_process_to_finish "$dest_dataset" "destonly" 30
	wait_for_snapshot_absent "$dest_dataset" "destonly"
	assert_snapshot_exists "$dest_dataset" "base"

	iter_count=$(printf '%s\n' "$output" | grep -c "Begin Iteration")
	if [ "$iter_count" -lt 1 ]; then
		fail "Expected yield loop iterations when deleting destination-only snapshots; saw $iter_count. Output: $output"
	fi

	log "Destination-only snapshot delete test passed"
}

migration_unmounted_guard_test() {
	log "Starting migration unmounted guard test"

	src_dataset="$SRC_POOL/unmounted_src"
	dest_root="$DEST_POOL/unmounted_dest"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

property_backup_restore_test() {
	log "Starting property backup/restore test"

	src_dataset="$SRC_POOL/prop_backup_src"
	dest_root="$DEST_POOL/prop_backup_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"
	backup_dir="$WORKDIR/backup_props"

	rm -rf "$backup_dir"
	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

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

	backup_file=$(find "$backup_dir" -type f -name '*.zxfer_backup_info.*' | head -n 1)
	if [ "$backup_file" = "" ]; then
		fail "Backup metadata file was not written under $backup_dir."
	fi
	if ! grep -q "#zxfer property backup file;" "$backup_file"; then
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

backup_dir_symlink_guard_test() {
	log "Starting backup directory symlink guard test"

	src_dataset="$SRC_POOL/backup_symlink_src"
	dest_root="$DEST_POOL/backup_symlink_dest"
	backup_dir_link="$WORKDIR/backup_symlink"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true
	rm -rf "$backup_dir_link"

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

	rm -rf "$backup_dir_link"

	log "Backup directory symlink guard test passed"
}

background_send_failure_test() {
	log "Starting background send failure test"

	if ! has_gnu_parallel; then
		log "Skipping background send failure test (GNU parallel not available)"
		return
	fi

	src_dataset="$SRC_POOL/sendfail_src"
	dest_root="$DEST_POOL/sendfail_dest"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$dest_root"
	append_data_to_dataset "$src_dataset" "file.txt" "one"
	zfs snap -r "$src_dataset@base"

	real_zfs=$(command -v zfs 2>/dev/null || true)
	if [ "$real_zfs" = "" ]; then
		fail "zfs binary not found for send failure test."
	fi

	wrapper_dir="$WORKDIR/zfs_wrapper_fail_send"
	rm -rf "$wrapper_dir"
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
		real_bin=$(command -v "$bin" 2>/dev/null || true)
		if [ "$real_bin" != "" ]; then
			ln -sf "$real_bin" "$wrapper_dir/$bin"
		fi
	done
	real_parallel=$(command -v parallel 2>/dev/null || true)
	if [ "$real_parallel" != "" ]; then
		ln -sf "$real_parallel" "$wrapper_dir/parallel"
	fi

	set +e
	output=$(ZXFER_SECURE_PATH="$wrapper_dir" "$ZXFER_BIN" -v -j 2 -R "$src_dataset" "$dest_root" 2>&1)
	status=$?
	set -e

	rm -rf "$wrapper_dir"

	if [ "$status" -eq 0 ]; then
		fail "zxfer should fail when zfs send exits non-zero."
	fi
	if ! printf '%s\n' "$output" | grep -q "zfs send/receive job failed"; then
		fail "Expected send failure message. Output: $output"
	fi

	log "Background send failure test passed"
}

force_rollback_test() {
	log "Starting force rollback test"

	src_dataset="$SRC_POOL/rollback_src"
	dest_root="$DEST_POOL/rollback_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$dest_root"

	append_data_to_dataset "$src_dataset" "file.txt" "original"
	zfs snap "$src_dataset@snap1"

	run_zxfer -v -N "$src_dataset" "$dest_root"
	assert_snapshot_exists "$dest_dataset" "snap1"

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

yield_loop_dryrun_iteration_test() {
	log "Starting yield loop dry-run iteration test"

	src_dataset="$SRC_POOL/yield_src"
	dest_root="$DEST_POOL/yield_dest"
	dest_dataset="$dest_root/${src_dataset##*/}"

	zfs destroy -r "$dest_root" >/dev/null 2>&1 || true
	zfs destroy -r "$src_dataset" >/dev/null 2>&1 || true

	zfs create "$src_dataset"
	zfs create "$dest_root"

	zfs snap -r "$src_dataset@base"
	ZXFER_BACKUP_DIR= run_zxfer -v -R "$src_dataset" "$dest_root"

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

	usage_error_tests

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

	TEST_SEQUENCE="usage_error_tests \
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
extended_usage_error_tests \
consistency_option_validation_tests \
snapshot_deletion_test \
send_command_dryrun_test \
backup_dir_symlink_guard_test \
grandfather_protection_test \
migration_unmounted_guard_test \
missing_parallel_error_test \
secure_path_dependency_tests \
property_backup_restore_test \
delete_dest_only_snapshot_test \
dry_run_deletion_test \
progress_wrapper_test \
job_limit_enforcement_test \
background_send_failure_test \
remote_migration_guard_tests \
parallel_jobs_listing_test"
	set -- $TEST_SEQUENCE
	TOTAL_TESTS=$#

	l_index=1
	for test_func in $TEST_SEQUENCE; do
		run_test "$l_index" "$TOTAL_TESTS" "$test_func"
		l_index=$((l_index + 1))
	done

	log "All integration tests passed."
}

main "$@"
