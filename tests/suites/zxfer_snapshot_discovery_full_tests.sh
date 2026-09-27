#!/bin/sh
# shellcheck shell=sh
# Full discovery, destination routing (local and -T), record-cache, stage
# timing and fast recursive no-op cases for src/zxfer_snapshot_discovery.sh.
# Run by tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Print one dataset's rows of a staged snapshot record file: the rows whose
# part before the first "@" is exactly DATASET.
# Usage: zxfer_test_snapshot_records_for_dataset FILE DATASET
zxfer_test_snapshot_records_for_dataset() {
	# shellcheck disable=SC2016  # awk program should see literal $1.
	awk -F@ -v ds="$2" '$1 == ds' "$1"
}

test_get_zfs_list_bootstraps_missing_destination_dataset_when_pool_exists() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_get_zfs_list.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/zxfer_get_zfs_list.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snapA
tank/src@snapB
EOF
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "dataset does not exist" >&2
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "backup"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'source=%s\n' "$(zxfer_test_snapshot_records_for_dataset "$g_zxfer_source_snapshot_record_cache_file" "tank/src")"
		)
	)

	assertContains "Bootstrap path should treat the missing destination dataset as an empty recursive list." "$output" "dest="
	assertContains "Per-dataset source lookups should still lazily return newest-first records for send planning." \
		"$output" "source=tank/src@snapB
tank/src@snapA"
}

test_get_zfs_list_bootstraps_missing_destination_dataset_when_omnios_reports_no_such_pool_or_dataset() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_get_zfs_list_omnios.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/zxfer_get_zfs_list_omnios.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snapA
tank/src@snapB
EOF
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "cannot open 'backup/tank/src': no such pool or dataset" >&2
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "backup"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'dest=%s\n' "$g_recursive_dest_list"
			printf 'source=%s\n' "$(zxfer_test_snapshot_records_for_dataset "$g_zxfer_source_snapshot_record_cache_file" "tank/src")"
		)
	)

	assertContains "OmniOS-style missing destination errors should still bootstrap the recursive destination list as empty." \
		"$output" "dest="
	assertContains "OmniOS-style destination bootstrap should still preserve the source snapshot planning list." \
		"$output" "source=tank/src@snapB
tank/src@snapA"
}

test_get_zfs_list_reports_pool_lookup_failure_when_destination_root_has_no_slash() {
	set +e
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_list_root_missing.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_list_root_missing.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
					printf '%s\n' "dataset does not exist" >&2
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] && [ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "pool lookup failed" >&2
					return 1
				fi
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			g_destination="backup"
			zxfer_get_zfs_list
		)
	)
	status=$?

	assertEquals "Missing destination roots without a slash should still fail closed when the pool lookup fails." 1 "$status"
	assertContains "Destination-root lookup failures should report the missing destination and failed pool probe." \
		"$output" "Destination dataset [backup] is missing and destination pool [backup] could not be listed: pool lookup failed"
}

test_publish_destination_dataset_inventory_bootstraps_rootless_missing_destination() {
	dest_file="$TEST_TMPDIR/dest_inventory_rootless_missing.out"
	err_file="$TEST_TMPDIR/dest_inventory_rootless_missing.err"
	: >"$dest_file"
	printf '%s\n' "dataset does not exist" >"$err_file"

	output=$(
		(
			g_destination="backup"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-o" ] &&
					[ "$4" = "name" ] && [ "$5" = "backup" ]; then
					printf '%s\n' "pool-probe:$5"
					return 0
				fi
				return 1
			}
			zxfer_publish_destination_dataset_inventory_from_stage "$dest_file" "$err_file" 1
			printf 'dest=<%s>\n' "$g_recursive_dest_list"
			printf 'missing=%s\n' "$(zxfer_lookup_destination_existence_cache "backup" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
		)
	)

	assertContains "Rootless missing destinations should bootstrap as an empty inventory when the pool probe succeeds." \
		"$output" "dest=<>"
	assertContains "Rootless missing destinations should seed the destination existence cache as absent." \
		"$output" "missing=0"
}

# Local and -T inventories share this publisher, so its operator messages and
# statuses are pinned here once: a listing failure that names no missing
# dataset, with and without stderr, and a missing root whose pool probe fails.
test_publish_destination_dataset_inventory_reports_listing_and_pool_failures() {
	dest_file="$TEST_TMPDIR/dest_inventory_failures.out"
	err_file="$TEST_TMPDIR/dest_inventory_failures.err"
	probe_log="$TEST_TMPDIR/dest_inventory_failures.probe"
	: >"$dest_file"
	: >"$probe_log"
	: >"$TEST_TMPDIR/dest_inventory_failures.out.all"

	for l_test_case in "permission denied|13" "|14" "cannot open 'backup/dst': dataset does not exist|1"; do
		l_test_stderr=${l_test_case%|*}
		: >"$err_file"
		[ -z "$l_test_stderr" ] || printf '%s\n' "$l_test_stderr" >"$err_file"
		output=$(
			(
				PROBE_LOG=$probe_log
				zxfer_run_destination_zfs_cmd() {
					printf '%s\n' "$*" >>"$PROBE_LOG"
					return 2
				}
				zxfer_throw_error() {
					printf 'error=%s status=%s\n' "$1" "${2:-1}"
					exit "${2:-1}"
				}
				zxfer_publish_destination_dataset_inventory_from_stage \
					"$dest_file" "$err_file" "${l_test_case##*|}"
			)
		)
		printf '%s\n' "$output" >>"$TEST_TMPDIR/dest_inventory_failures.out.all"
	done

	assertEquals "Inventory failures should report the listing's stderr and status, and a missing root's failed pool probe its own status." \
		"error=Failed to retrieve list of datasets from the destination: permission denied status=13
error=Failed to retrieve list of datasets from the destination status=14
error=Destination dataset [backup/dst] is missing and destination pool [backup] could not be listed. status=2" \
		"$(cat "$TEST_TMPDIR/dest_inventory_failures.out.all")"
	assertEquals "Only the missing root should probe its pool, once." \
		"list -H -o name backup" "$(cat "$probe_log")"
}

test_collect_destination_dataset_inventory_preserves_setup_and_publish_failures() {
	temp_status=$(
		(
			zxfer_create_temp_file_group() {
				return 66
			}
			set +e
			zxfer_collect_destination_dataset_inventory
			printf '%s\n' "$?"
		)
	)
	publish_status=$(
		(
			l_one="$TEST_TMPDIR/dest_inventory_publish.one"
			l_two="$TEST_TMPDIR/dest_inventory_publish.two"
			g_option_V_very_verbose=1
			zxfer_create_temp_file_group() {
				: >"$l_one"
				: >"$l_two"
				g_zxfer_temp_file_group_result=$(printf '%s\n%s' "$l_one" "$l_two")
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_publish_destination_dataset_inventory_from_stage() {
				return 67
			}
			set +e
			zxfer_collect_destination_dataset_inventory 2>/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Destination inventory should preserve temp-file group allocation failures." \
		66 "$temp_status"
	assertEquals "Destination inventory should preserve publish failures after cleanup." \
		67 "$publish_status"
}

test_get_zfs_list_seeds_destination_existence_cache_from_recursive_dataset_list() {
	output=$(
		(
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_reverse_file_lines() {
				cat "$1"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/existing"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'root=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'existing=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst/existing" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
			printf 'missing=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst/missing" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
		)
	)

	assertContains "Destination discovery should seed the root dataset into the existence cache." \
		"$output" "root=1"
	assertContains "Destination discovery should seed known descendants into the existence cache." \
		"$output" "existing=1"
	assertContains "Destination discovery should let later callers infer missing descendants without another probe." \
		"$output" "missing=0"
}

test_get_zfs_list_reports_destination_inventory_readback_failures() {
	set +e
	output=$(
		(
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/existing"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				return 27
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Destination inventory readback failures should preserve the staged read status." \
		27 "$status"
	assertContains "Destination inventory readback failures should report the staged destination inventory context." \
		"$output" "Failed to read staged destination dataset inventory."
}

test_get_zfs_list_reports_destination_inventory_stderr_readback_failures() {
	set +e
	probe_log="$TEST_TMPDIR/get_zfs_destination_inventory_stderr_probe.log"
	: >"$probe_log"
	output=$(
		(
			PROBE_LOG="$probe_log"
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_destination_probe_reports_missing() {
				printf '%s\n' "called" >>"$PROBE_LOG"
				return 0
			}
			zxfer_read_snapshot_discovery_capture_file() {
				return 28
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Destination inventory stderr readback failures should preserve the staged stderr read status." \
		28 "$status"
	assertContains "Destination inventory stderr readback failures should report the staged stderr context." \
		"$output" "Failed to read staged destination dataset inventory stderr."
	assertFalse "Destination inventory stderr readback failures should not continue into missing-destination fallback checks." \
		"[ -s '$probe_log' ]"
}

test_get_zfs_list_reports_empty_destination_inventory_readbacks() {
	set +e
	output=$(
		(
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/existing"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				g_zxfer_snapshot_discovery_file_read_result=""
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		) 2>&1
	)
	status=$?

	assertEquals "Empty staged destination inventory readbacks should abort snapshot discovery." \
		1 "$status"
	assertContains "Empty staged destination inventory readbacks should report the specific empty-inventory context." \
		"$output" "Staged destination dataset inventory was empty."
}

test_get_zfs_list_preserves_source_snapshot_record_cache_tempfile_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			l_temp_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
				printf '%s\n' "tank/src@snapA" >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst"
				elif [ "$l_read_count" -eq 2 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst@snapA"
				elif [ "$l_read_count" -eq 3 ]; then
					g_zxfer_snapshot_discovery_file_read_result="tank/src@snapA"
				else
					return 1
				fi
				return 0
			}
			zxfer_get_temp_file() {
				l_temp_count=$((l_temp_count + 1))
				if [ "$l_temp_count" -le 6 ]; then
					g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-source-cache-$l_temp_count.tmp"
					: >"$g_zxfer_temp_file_result"
					return 0
				fi
				return 37
			}
			zxfer_get_zfs_list
		)
	)
	status=$?
	set -e

	assertEquals "Snapshot discovery should preserve the exact tempfile allocation failure status when the staged source snapshot-record cache tempfile cannot be allocated." \
		37 "$status"
	assertEquals "Snapshot discovery should not emit output for staged source snapshot-record cache tempfile failures." \
		"" "$output"
}

test_get_zfs_list_reports_source_snapshot_record_cache_stage_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			l_temp_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
				printf '%s\n' "tank/src@snapA" >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst"
				else
					return 1
				fi
				return 0
			}
			zxfer_get_temp_file() {
				l_temp_count=$((l_temp_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-source-cache-stage-$l_temp_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_reverse_file_lines() {
				return 1
			}
			zxfer_throw_error() {
				printf 'msg=%s\n' "$1"
				exit "${2:-1}"
			}
			zxfer_get_zfs_list
		)
	)
	status=$?
	set -e

	assertEquals "Source snapshot record-cache staging failures should abort snapshot discovery." \
		1 "$status"
	assertContains "Source snapshot record-cache staging failures should report the staged source-cache context." \
		"$output" "msg=Failed to stage source snapshot record cache."
}

test_get_zfs_list_skips_snapshot_record_caches_for_recursive_noop_without_later_work() {
	inventory_log="$TEST_TMPDIR/recursive_noop_destination_inventory.log"
	: >"$inventory_log"

	output=$(
		(
			INVENTORY_LOG="$inventory_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_R_recursive="tank/src"
			g_option_d_delete_destination_snapshots=1
			g_option_P_transfer_property=0
			g_option_o_override_property=""
			# Local recursive runs are proof-eligible since Phase 8; force
			# the fallback so this test keeps pinning the full-discovery
			# no-op record-cache skips.
			zxfer_try_fast_recursive_noop_discovery() {
				return 1
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\t%s\n' "tank/src@snapA" "guidA" >"$1"
				: >"$2"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\t%s\n' "backup/dst/src@snapA" "guidA" >"$1"
				printf '%s\t%s\n' "tank/src@snapA" "guidA" >"$2"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "inventory" >>"$INVENTORY_LOG"
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			# shellcheck disable=SC2031
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			# shellcheck disable=SC2031
			printf 'source_datasets=<%s>\n' "${g_recursive_source_dataset_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'source_cache=<%s>\n' "${g_zxfer_source_snapshot_record_cache_file:-}"
			printf 'dest_cache=<%s>\n' "${g_zxfer_destination_snapshot_record_cache_file:-}"
		)
	)

	assertContains "Recursive no-op discovery should prove that there are no source snapshot deltas." \
		"$output" "source_list=<>"
	assertContains "Recursive no-op discovery should skip the source dataset inventory when no later property work can consume it." \
		"$output" "source_datasets=<>"
	assertContains "Recursive no-op discovery should prove that there are no destination delete deltas." \
		"$output" "dest_extra=<>"
	assertContains "Recursive no-op discovery should skip the source snapshot-record cache when no later work can consume it." \
		"$output" "source_cache=<>"
	assertContains "Recursive no-op discovery should skip the destination snapshot-record cache when no later work can consume it." \
		"$output" "dest_cache=<>"
	assertEquals "Recursive no-op discovery should skip recursive destination dataset inventory when no later work can consume it." \
		"" "$(cat "$inventory_log")"
}

test_get_zfs_list_fast_remote_recursive_noop_skips_creation_order_discovery() {
	full_discovery_log="$TEST_TMPDIR/fast_remote_noop_full_discovery.log"
	: >"$full_discovery_log"

	output=$(
		(
			FULL_DISCOVERY_LOG="$full_discovery_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			g_option_d_delete_destination_snapshots=1
			g_option_x_exclude_datasets="replica"
			g_option_j_jobs=6
			g_option_V_very_verbose=1
			zxfer_build_source_snapshot_name_list_cmd() {
				g_source_snapshot_list_uses_parallel=0
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "unexpected-full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				return 99
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "fast_attempted=1"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_get_zfs_list
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'source_datasets=<%s>\n' "${g_recursive_source_dataset_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'parallel_profile=%s\n' "${g_zxfer_profile_source_snapshot_list_parallel_commands:-0}"
		)
	)

	assertEquals "Fast remote recursive no-op proof should avoid the full creation-order source discovery." \
		"" "$(cat "$full_discovery_log")"
	assertContains "Fast remote recursive no-op proof should record that the optimization ran." \
		"$output" "fast_attempted=1"
	assertContains "Pre-filtered excluded dataset differences should still allow the exact no-op proof to short-circuit." \
		"$output" "source_list=<>"
	assertContains "Fast no-op proof should not publish a source dataset inventory when no later work can consume it." \
		"$output" "source_datasets=<>"
	assertContains "Fast no-op proof should not queue destination deletes when only excluded datasets differ." \
		"$output" "dest_extra=<>"
	assertContains "Fast remote recursive no-op proof should not account source parallel fanout before work is proven." \
		"$output" "parallel_profile=0"
}

test_get_zfs_list_fast_remote_recursive_noop_allows_noop_safe_property_and_delete_flags() {
	full_discovery_log="$TEST_TMPDIR/fast_remote_noop_safe_flags_full_discovery.log"
	: >"$full_discovery_log"

	output=$(
		(
			FULL_DISCOVERY_LOG="$full_discovery_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			g_option_j_jobs=6
			g_option_U_skip_unsupported_properties=1
			g_option_g_grandfather_protection="enabled"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_source_snapshot_list_uses_parallel=0
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "unexpected-full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				return 99
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "fast_attempted=1"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_get_zfs_list
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'source_datasets=<%s>\n' "${g_recursive_source_dataset_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'parallel_profile=%s\n' "${g_zxfer_profile_source_snapshot_list_parallel_commands:-0}"
		)
	)

	assertEquals "Fast no-op proof should not force full discovery only because -U or -g are enabled." \
		"" "$(cat "$full_discovery_log")"
	assertContains "Fast no-op proof should run when -U cannot be consumed by later no-op work." \
		"$output" "fast_attempted=1"
	assertContains "Fast no-op proof should leave no source transfer queue for grandfather checks." \
		"$output" "source_list=<>"
	assertContains "Fast no-op proof should leave no source dataset inventory for unsupported-property scans." \
		"$output" "source_datasets=<>"
	assertContains "Fast no-op proof should leave no destination delete queue for grandfather checks." \
		"$output" "dest_extra=<>"
	assertContains "Fast no-op proof should still defer full parallel source discovery under -U and -g." \
		"$output" "parallel_profile=0"
}

test_get_zfs_list_fast_remote_recursive_noop_shortcuts_exact_match_without_exclude_filter() {
	filter_log="$TEST_TMPDIR/fast_remote_exact_noop_filter.log"
	: >"$filter_log"

	output=$(
		(
			FILTER_LOG="$filter_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			g_option_d_delete_destination_snapshots=1
			g_option_x_exclude_datasets=""
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "fast_attempted=1"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_filter_snapshot_file_with_excludes() {
				printf '%s\n' "unexpected-filter" >>"$FILTER_LOG"
				return 99
			}
			zxfer_get_zfs_list
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
		)
	)

	assertEquals "Exact fast no-op proofs should not run exclude filtering when no exclude is configured." \
		"" "$(cat "$filter_log")"
	assertContains "Exact fast no-op proof should record that it attempted." \
		"$output" "fast_attempted=1"
	assertContains "Exact fast no-op proof should not queue source transfers." \
		"$output" "source_list=<>"
	assertContains "Exact fast no-op proof should not queue destination deletes." \
		"$output" "dest_extra=<>"
}

test_get_zfs_list_fast_remote_recursive_noop_falls_back_when_snapshot_names_differ() {
	full_discovery_log="$TEST_TMPDIR/fast_remote_noop_fallback.log"
	destination_call_log="$TEST_TMPDIR/fast_remote_noop_fallback_destination.log"
	: >"$full_discovery_log"
	: >"$destination_call_log"

	output=$(
		(
			FULL_DISCOVERY_LOG="$full_discovery_log"
			DESTINATION_CALL_LOG="$destination_call_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			g_option_d_delete_destination_snapshots=1
			g_option_x_exclude_datasets=""
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "fast_attempted=1"
				printf '%s\n' "destination-discovery" >>"$DESTINATION_CALL_LOG"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapB" "guid-b")
				ZXFER_TEST_FAST_NOOP_DESTINATION_RAW=$(printf '%s\t%s' "backup/dst/src@snapB" "guid-b")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "destination-discovery" >>"$DESTINATION_CALL_LOG"
				printf '%s\n' "backup/dst/src@snapB" >"$1"
				printf '%s\n' "tank/src@snapB" >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				printf '%s\n' "full-diff-planning" >>"$FULL_DISCOVERY_LOG"
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'dest_cache=<%s>\n' "$(cat "$g_zxfer_destination_snapshot_record_cache_file")"
		)
	)

	assertEquals "A non-no-op name comparison should fall back to full source discovery and full diff planning." \
		"full-source-discovery
full-diff-planning" "$(cat "$full_discovery_log")"
	assertEquals "Full discovery should reuse the proof's destination listing instead of listing again." \
		"1" "$(wc -l <"$destination_call_log" | tr -d '[:space:]')"
	assertContains "The reused raw destination listing should become the destination record cache." \
		"$output" "dest_cache=<backup/dst/src@snapB	guid-b>"
	assertContains "Fast remote recursive no-op proof should record that it attempted before falling back." \
		"$output" "fast_attempted=1"
	assertContains "Fallback discovery should publish the normal recursive work list." \
		"$output" "source_list=<tank/src>"
}

test_get_zfs_list_fast_remote_recursive_noop_falls_back_when_snapshot_guids_differ() {
	full_discovery_log="$TEST_TMPDIR/fast_remote_noop_guid_fallback.log"
	destination_call_log="$TEST_TMPDIR/fast_remote_noop_guid_destination.log"
	: >"$full_discovery_log"
	: >"$destination_call_log"

	output=$(
		(
			FULL_DISCOVERY_LOG="$full_discovery_log"
			DESTINATION_CALL_LOG="$destination_call_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			g_option_d_delete_destination_snapshots=1
			g_option_x_exclude_datasets=""
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\t%s\n' 'tank/src@snapA' 'source-guid'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				printf '%s\t%s\n' "tank/src@snapA" "source-guid" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "fast_attempted=1"
				printf '%s\n' "destination-discovery" >>"$DESTINATION_CALL_LOG"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "destination-guid")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "destination-discovery" >>"$DESTINATION_CALL_LOG"
				printf '%s\t%s\n' "backup/dst/src@snapA" "destination-guid" >"$1"
				printf '%s\t%s\n' "tank/src@snapA" "destination-guid" >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				printf '%s\n' "full-diff-planning" >>"$FULL_DISCOVERY_LOG"
				g_recursive_source_list="tank/src"
				g_recursive_destination_extra_dataset_list="tank/src"
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
		)
	)

	assertEquals "A same-name GUID mismatch should fall back to full source discovery and full diff planning." \
		"full-source-discovery
full-diff-planning" "$(cat "$full_discovery_log")"
	assertEquals "Full discovery should reuse the identity proof's destination listing instead of listing again." \
		"1" "$(wc -l <"$destination_call_log" | tr -d '[:space:]')"
	assertContains "Fast remote recursive no-op proof should record that it attempted before GUID fallback." \
		"$output" "fast_attempted=1"
	assertContains "Fallback discovery should queue the source dataset after GUID divergence." \
		"$output" "source_list=<tank/src>"
	assertContains "Fallback discovery should preserve destination-side divergence for delete/common-snapshot inspection." \
		"$output" "dest_extra=<tank/src>"
}

test_get_zfs_list_fast_remote_recursive_noop_falls_back_when_excludes_filter_all_source_snapshots() {
	full_discovery_log="$TEST_TMPDIR/fast_remote_noop_excluded_all_fallback.log"
	: >"$full_discovery_log"

	output=$(
		(
			FULL_DISCOVERY_LOG="$full_discovery_log"
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			g_option_x_exclude_datasets='/replica$'
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src/replica@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "fast_attempted=1"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				printf '%s\n' "tank/src/replica@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				printf '%s\n' "full-diff-planning" >>"$FULL_DISCOVERY_LOG"
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_get_zfs_list
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
		)
	)

	assertEquals "Fast no-op proof should fall back instead of treating an exclude-filtered empty source list as a source failure." \
		"full-source-discovery
full-diff-planning" "$(cat "$full_discovery_log")"
	assertContains "Fast no-op proof should record the attempted optimization before fallback." \
		"$output" "fast_attempted=1"
	assertContains "Fallback discovery should be allowed to prove the all-excluded no-op." \
		"$output" "source_list=<>"
}

# Probe zxfer_fast_recursive_noop_options_are_eligible with a clean option
# state plus the supplied overrides; prints the helper's exit status.
# Errexit-safe so the suite's set -e tests cannot abort the caller.
zxfer_test_noop_proof_eligibility_status() {
	l_eligibility_status=0
	(
		g_option_O_origin_host=""
		g_option_T_target_host=""
		g_option_R_recursive="tank/src"
		g_option_s_make_snapshot=0
		g_option_m_migrate=0
		g_option_P_transfer_property=0
		g_option_o_override_property=""
		g_option_e_restore_property_mode=0
		g_option_k_backup_property_mode=0
		for l_eligibility_override in "$@"; do
			eval "$l_eligibility_override"
		done
		zxfer_fast_recursive_noop_options_are_eligible
	) || l_eligibility_status=$?
	printf '%s\n' "$l_eligibility_status"
	return 0
}

test_fast_recursive_noop_discovery_eligibility_gates() {
	# Local sources are eligible since Phase 8: -O is no longer consulted.
	# Every other gate stays: -R required, -T absent, and the no-op-unsafe
	# options (-s/-m/-P/-o/-e/-k) must all be off.
	assertEquals "A plain local recursive run must be proof-eligible." \
		0 "$(zxfer_test_noop_proof_eligibility_status)"
	assertEquals "A remote-origin recursive run must stay proof-eligible." \
		0 "$(zxfer_test_noop_proof_eligibility_status \
			"g_option_O_origin_host=origin.example")"
	assertEquals "Non-recursive runs must stay ineligible." \
		1 "$(zxfer_test_noop_proof_eligibility_status \
			"g_option_R_recursive=")"
	assertEquals "-T target-host runs must stay ineligible." \
		1 "$(zxfer_test_noop_proof_eligibility_status \
			"g_option_T_target_host=target.example")"
	for l_eligibility_gate in \
		"g_option_s_make_snapshot=1" \
		"g_option_m_migrate=1" \
		"g_option_P_transfer_property=1" \
		"g_option_o_override_property=copies=2" \
		"g_option_e_restore_property_mode=1" \
		"g_option_k_backup_property_mode=1"; do
		assertEquals "Runs with $l_eligibility_gate must stay ineligible for the no-op proof." \
			1 "$(zxfer_test_noop_proof_eligibility_status "$l_eligibility_gate")"
	done
}

test_try_fast_recursive_noop_discovery_records_parallel_source_profile_counter() {
	output=$(
		(
			g_initial_source="tank/src"
			g_destination="backup/dst"
			g_option_R_recursive="tank/src"
			g_option_V_very_verbose=1
			zxfer_build_source_snapshot_name_list_cmd() {
				g_source_snapshot_list_uses_parallel=1
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_try_fast_recursive_noop_discovery
			printf 'commands=%s\n' "${g_zxfer_profile_source_snapshot_list_commands:-0}"
			printf 'parallel=%s\n' "${g_zxfer_profile_source_snapshot_list_parallel_commands:-0}"
		)
	)

	assertContains "Fast no-op proof should profile each source snapshot listing command." \
		"$output" "commands=1"
	assertContains "Fast no-op proof should profile source commands that already used parallel fanout." \
		"$output" "parallel=1"
}

test_try_fast_recursive_noop_discovery_preserves_setup_failures() {
	temp_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_create_temp_file_group() {
				return 42
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	build_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				return 43
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	execute_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_execute_source_snapshot_name_list_background_sort_cmd() {
				return 44
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	destination_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_execute_source_snapshot_name_list_background_sort_cmd() {
				sleep 5 &
				g_last_background_pid=$!
				zxfer_register_cleanup_pid "$g_last_background_pid" "background source snapshot no-op proof helper" || :
				return 0
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				return 45
			}
			zxfer_abort_direct_child_pid() {
				kill "$1" 2>/dev/null || :
				return 0
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)

	assertEquals "Fast no-op proof should preserve temp-file allocation failures." \
		42 "$temp_status"
	assertEquals "Fast no-op proof should preserve source command render failures." \
		43 "$build_status"
	assertEquals "Fast no-op proof should preserve background source launch failures." \
		44 "$execute_status"
	assertEquals "Fast no-op proof should preserve destination discovery failures and abort the background source proof." \
		45 "$destination_status"
}

test_try_fast_recursive_noop_discovery_reports_source_failures() {
	source_error_status=0
	source_error_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="sh -c 'printf %s denied >&2; exit 17'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || source_error_status=$?
	empty_source_error_status=0
	empty_source_error_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="sh -c 'exit 17'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || empty_source_error_status=$?
	empty_source_status=0
	empty_source_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result=":"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || empty_source_status=$?
	stderr_read_status=0
	stderr_read_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="sh -c 'exit 17'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			# The staged stderr is the first file this proof reads back.
			zxfer_read_snapshot_discovery_capture_file() {
				return 68
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || stderr_read_status=$?
	count_read_status=0
	count_read_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			# Only the source count sidecar goes through this reader.
			zxfer_read_snapshot_discovery_status_file() {
				g_zxfer_snapshot_discovery_status_file_result=0
				return 72
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || count_read_status=$?

	assertContains "Fast no-op proof should preserve source snapshot stderr when the identity-aware source command fails." \
		"$source_error_output" "throw:Failed to retrieve snapshots from the source: denied:17"
	assertEquals "Fast no-op proof should return the source command status when source discovery fails." \
		17 "$source_error_status"
	assertContains "Fast no-op proof should use the generic source failure when the failed command has no stderr." \
		"$empty_source_error_output" "throw:Failed to retrieve snapshots from the source:17"
	assertEquals "Fast no-op proof should preserve source command status when stderr is empty." \
		17 "$empty_source_error_status"
	assertContains "Fast no-op proof should fail closed when the identity-aware source discovery returns no snapshots." \
		"$empty_source_output" "throw:Failed to retrieve snapshots from the source:1"
	assertEquals "Fast no-op proof should return failure for an empty source snapshot list." \
		1 "$empty_source_status"
	assertContains "Fast no-op proof should report staged stderr readback failures before surfacing source failure context." \
		"$stderr_read_output" "throw:Failed to read staged source snapshot stderr.:68"
	assertEquals "Fast no-op proof should preserve staged stderr readback failure status." \
		68 "$stderr_read_status"
	assertContains "Fast no-op proof should fail closed when source snapshot count sidecar validation fails." \
		"$count_read_output" "throw:Failed to retrieve snapshots from the source:1"
	assertEquals "Fast no-op proof should use the generic source failure status for invalid source count sidecars." \
		1 "$count_read_status"
}

test_try_fast_recursive_noop_discovery_reports_destination_fifo_status_failures() {
	malformed_status=0
	malformed_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS="bad"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		)
	) || malformed_status=$?
	malformed_normalize_status=0
	malformed_normalize_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_NORMALIZE_STATUS="bad"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		)
	) || malformed_normalize_status=$?
	malformed_sort_status=0
	malformed_sort_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORT_STATUS="bad"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		)
	) || malformed_sort_status=$?
	destination_error=0
	destination_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS=17
				ZXFER_TEST_FAST_NOOP_DESTINATION_STDERR="permission denied"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || destination_error=$?
	destination_stderr_read_status=0
	destination_stderr_read_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS=17
				ZXFER_TEST_FAST_NOOP_DESTINATION_STDERR="permission denied"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			# The staged destination stderr is the first file read back.
			zxfer_read_snapshot_discovery_capture_file() {
				return 70
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || destination_stderr_read_status=$?
	normalize_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_NORMALIZE_STATUS=19
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)
	sort_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_STREAM_STATUS=23
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)
	destination_wait_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				l_fifo=$1
				l_err_file=$2
				l_status_file=$3
				(
					printf '%s\n' "tank/src@snapA" >"$l_fifo"
					: >"$l_err_file"
					printf '%s\n' "0 0 0" >"$l_status_file"
					exit 31
				) &
				g_last_background_pid=$!
				zxfer_register_cleanup_pid "$g_last_background_pid" "test destination snapshot no-op proof helper"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)
	missing_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS=1
				ZXFER_TEST_FAST_NOOP_DESTINATION_STDERR="cannot open 'backup/dst/src': dataset does not exist"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertContains "Fast no-op proof should fail closed on malformed destination status sidecars." \
		"$malformed_output" "throw:Failed to validate destination snapshot status for recursive no-op proof.:1"
	assertEquals "Fast no-op proof should return failure for malformed destination status sidecars." \
		1 "$malformed_status"
	assertContains "Fast no-op proof should fail closed on malformed destination normalize sidecars." \
		"$malformed_normalize_output" "throw:Failed to validate destination snapshot status for recursive no-op proof.:1"
	assertEquals "Fast no-op proof should return failure for malformed destination normalize sidecars." \
		1 "$malformed_normalize_status"
	assertContains "Fast no-op proof should fail closed on malformed destination stream sidecars." \
		"$malformed_sort_output" "throw:Failed to validate destination snapshot status for recursive no-op proof.:1"
	assertEquals "Fast no-op proof should return failure for malformed destination stream sidecars." \
		1 "$malformed_sort_status"
	assertContains "Fast no-op proof should preserve destination snapshot-list stderr." \
		"$destination_output" "permission denied"
	assertContains "Fast no-op proof should keep destination snapshot-list context." \
		"$destination_output" "throw:Failed to retrieve snapshot list from the destination.:17"
	assertEquals "Fast no-op proof should preserve destination snapshot-list status." \
		17 "$destination_error"
	assertContains "Fast no-op proof should report destination stderr readback failures before surfacing destination snapshot context." \
		"$destination_stderr_read_output" "throw:Failed to read staged destination snapshot stderr.:70"
	assertEquals "Fast no-op proof should preserve destination stderr readback failure status." \
		70 "$destination_stderr_read_status"
	assertEquals "Fast no-op proof should preserve destination normalization failures." \
		19 "$normalize_status"
	assertEquals "Fast no-op proof should preserve destination stream failures." \
		23 "$sort_status"
	assertEquals "Fast no-op proof should preserve destination producer wait failures." \
		31 "$destination_wait_status"
	assertEquals "Fast no-op proof should fall back when exact compare output conflicts with missing destination status." \
		1 "$missing_status"
}

test_try_fast_recursive_noop_discovery_reports_compare_failures() {
	compare_status=0
	compare_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			comm() {
				return 2
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		)
	) || compare_status=$?

	assertContains "Fast no-op proof should report compare failures with no-op proof context." \
		"$compare_output" "throw:Failed to compare source and destination snapshots for recursive no-op proof.:2"
	assertEquals "Fast no-op proof should preserve compare failure status." \
		2 "$compare_status"
}

test_get_zfs_list_throws_on_stage_failures_that_did_not_throw() {
	set +e
	output=$(
		(
			zxfer_try_fast_recursive_noop_discovery() {
				return 58
			}
			zxfer_start_full_source_snapshot_discovery() {
				printf '%s\n' "unexpected full discovery"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_get_zfs_list
		)
		printf 'fast_status=%s\n' "$?"
		(
			zxfer_try_fast_recursive_noop_discovery() {
				return 1
			}
			zxfer_start_full_source_snapshot_discovery() {
				:
			}
			zxfer_collect_full_destination_snapshot_discovery() {
				return 9
			}
			zxfer_wait_for_full_source_snapshot_discovery() {
				printf '%s\n' "unexpected source wait"
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_get_zfs_list
		)
		printf 'full_status=%s\n' "$?"
	)

	assertNotContains "A fast no-op proof hard failure should not continue into full discovery." \
		"$output" "unexpected"
	assertContains "Its callers ignore the status, so discovery should throw a fast proof hard failure." \
		"$output" "throw:Failed to discover source and destination snapshots.:58"
	assertContains "The thrown failure should keep the fast proof status." \
		"$output" "fast_status=58"
	assertContains "Discovery should throw a full-discovery stage failure that returned without throwing." \
		"$output" "throw:Failed to discover source and destination snapshots.:9"
	assertContains "The thrown failure should keep the stage status." \
		"$output" "full_status=9"
}

test_get_zfs_list_stages_file_backed_snapshot_record_lookups() {
	output=$(
		(
			source_root_file="$TEST_TMPDIR/get_zfs_lazy_source_root.records"
			source_child_file="$TEST_TMPDIR/get_zfs_lazy_source_child.records"
			dest_root_file="$TEST_TMPDIR/get_zfs_lazy_dest_root.records"
			dest_child_file="$TEST_TMPDIR/get_zfs_lazy_dest_child.records"
			zxfer_write_source_snapshot_list_to_file() {
				cat <<'EOF' >"$1"
tank/src@snap1
tank/src/child@child1
tank/src@snap2
EOF
			}
			zxfer_write_destination_snapshot_list_to_files() {
				cat <<'EOF' >"$1"
backup/dst@snap2
backup/dst@legacy1
backup/dst/child@child1
EOF
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list="tank/src"
				g_recursive_source_dataset_list=$(printf '%s\n%s' "tank/src" "tank/src/child")
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					printf '%s\n' "backup/dst/child"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_file_staged=%s\n' "$([ -n "${g_zxfer_source_snapshot_record_cache_file:-}" ] && [ -r "$g_zxfer_source_snapshot_record_cache_file" ] && printf '%s' yes || printf '%s' no)"
			printf 'dest_file_staged=%s\n' "$([ -n "${g_zxfer_destination_snapshot_record_cache_file:-}" ] && [ -r "$g_zxfer_destination_snapshot_record_cache_file" ] && printf '%s' yes || printf '%s' no)"
			zxfer_test_snapshot_records_for_dataset "$g_zxfer_source_snapshot_record_cache_file" "tank/src" >"$source_root_file"
			zxfer_test_snapshot_records_for_dataset "$g_zxfer_source_snapshot_record_cache_file" "tank/src/child" >"$source_child_file"
			zxfer_test_snapshot_records_for_dataset "$g_zxfer_destination_snapshot_record_cache_file" "backup/dst" >"$dest_root_file"
			zxfer_test_snapshot_records_for_dataset "$g_zxfer_destination_snapshot_record_cache_file" "backup/dst/child" >"$dest_child_file"
			printf 'source_root=%s\n' "$(cat "$source_root_file")"
			printf 'source_child=%s\n' "$(cat "$source_child_file")"
			printf 'dest_root=%s\n' "$(cat "$dest_root_file")"
			printf 'dest_child=%s\n' "$(cat "$dest_child_file")"
		)
	)

	assertContains "Snapshot discovery should stage the flat source snapshot record file for later lookups." \
		"$output" "source_file_staged=yes"
	assertContains "Snapshot discovery should stage the flat destination snapshot record file for later lookups." \
		"$output" "dest_file_staged=yes"
	assertContains "Snapshot discovery should cache newest-first source snapshots for the root dataset." \
		"$output" "source_root=tank/src@snap2
tank/src@snap1"
	assertContains "Snapshot discovery should cache source snapshots for child datasets separately." \
		"$output" "source_child=tank/src/child@child1"
	assertContains "Snapshot discovery should cache destination snapshots in live destination order." \
		"$output" "dest_root=backup/dst@snap2
backup/dst@legacy1"
	assertContains "Snapshot discovery should cache destination child snapshots separately." \
		"$output" "dest_child=backup/dst/child@child1"
}

test_set_g_recursive_source_list_updates_dataset_caches() {
	source_tmp=$(mktemp -t zxfer_srcsnap.XXXXXX)
	dest_tmp=$(mktemp -t zxfer_dstsnap.XXXXXX)
	cat <<'EOF' >"$source_tmp"
tank/src@a
tank/src@b
tank/src/child@a
EOF
	cat <<'EOF' >"$dest_tmp"
tank/src@a
EOF
	g_cmd_awk=${g_cmd_awk:-$(command -v awk)}
	g_option_x_exclude_datasets=""
	zxfer_set_g_recursive_source_list "$source_tmp" "$dest_tmp"
	expected_list=$(printf '%s\n%s' "tank/src" "tank/src/child")
	assertEquals "Missing datasets should be identified for replication." "$expected_list" "$g_recursive_source_list"
	expected_datasets=$(printf '%s\n%s' "tank/src" "tank/src/child")
	assertEquals "Dataset cache should include every source filesystem." "$expected_datasets" "$g_recursive_source_dataset_list"
	rm -f "$source_tmp" "$dest_tmp"
}

# A -T target is listed like a local destination: both destination listings go
# through zxfer_run_destination_zfs_cmd (which routes to the -T host over its
# control master), never through a separate remote script.
test_get_zfs_list_routes_destination_listings_through_the_destination_role() {
	for l_test_target_host in "" target.example; do
		ssh_log="$TEST_TMPDIR/get_zfs_destination_role${l_test_target_host:+.remote}.ssh"
		zfs_log="$TEST_TMPDIR/get_zfs_destination_role${l_test_target_host:+.remote}.zfs"
		: >"$ssh_log"
		: >"$zfs_log"

		output=$(
			(
				SSH_LOG="$ssh_log"
				ZFS_LOG="$zfs_log"
				g_option_T_target_host=$l_test_target_host
				zxfer_write_source_snapshot_list_to_file() {
					printf '%s\n' "tank/src@snapA" >"$1"
					: >"$2"
					g_source_snapshot_list_pid=""
				}
				zxfer_invoke_ssh_shell_command_for_host() {
					printf '%s\n' "unexpected-ssh" >>"$SSH_LOG"
					return 99
				}
				zxfer_run_destination_zfs_cmd() {
					printf '%s\n' "$*" >>"$ZFS_LOG"
					if [ "$1" = "list" ] && [ "$2" = "-t" ]; then
						printf '%s\n' "backup/dst"
						printf '%s\n' "backup/dst/src"
						return 0
					fi
					if [ "$1" = "list" ] && [ "$2" = "-Hr" ]; then
						printf '%s\t%s\n' "backup/dst/src@snapA" "guid-a"
						return 0
					fi
					return 99
				}
				zxfer_set_g_recursive_source_list() {
					g_recursive_source_list="tank/src"
					g_recursive_source_dataset_list="tank/src"
				}
				zxfer_get_zfs_list
				printf 'dest=%s\n' "$g_recursive_dest_list"
				printf 'root_cache=%s\n' "$(zxfer_lookup_destination_existence_cache "backup/dst" && printf '%s' "$g_zxfer_destination_existence_cache_entry_result")"
				printf 'raw=%s\n' "$(cat "$g_zxfer_destination_snapshot_record_cache_file")"
			)
		)

		assertEquals "Destination discovery should not invoke ssh outside the destination role [target:$l_test_target_host]." \
			"" "$(cat "$ssh_log")"
		assertEquals "Destination discovery should list the snapshots, then the dataset inventory, through the destination role [target:$l_test_target_host]." \
			"list -Hr -o name,guid -t snapshot backup/dst/src
list -t filesystem,volume -Hr -o name backup/dst" "$(cat "$zfs_log")"
		assertContains "Destination discovery should publish the recursive destination inventory [target:$l_test_target_host]." \
			"$output" "dest=backup/dst
backup/dst/src"
		assertContains "Destination discovery should seed the destination root existence cache [target:$l_test_target_host]." \
			"$output" "root_cache=1"
		assertContains "Destination discovery should keep the raw destination snapshot cache [target:$l_test_target_host]." \
			"$output" "raw=backup/dst/src@snapA	guid-a"
	done
}

test_get_zfs_list_tracks_stage_timings_when_very_verbose() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_profile.counter"
			now_counter_file="$TEST_TMPDIR/get_zfs_profile.now.counter"
			printf '%s\n' 0 >"$counter_file"
			printf '%s\n' 0 >"$now_counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_profile.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 1000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 1500
				elif [ "$idx" = "3" ]; then
					printf '%s\n' 1900
				elif [ "$idx" = "4" ]; then
					printf '%s\n' 2600
				elif [ "$idx" = "5" ]; then
					printf '%s\n' 3000
				elif [ "$idx" = "6" ]; then
					printf '%s\n' 3550
				fi
			}
			zxfer_echoV() {
				:
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_write_destination_snapshot_list_to_files() {
				: >"$1"
				: >"$2"
			}
			zxfer_set_g_recursive_source_list() {
				g_recursive_source_list=""
				g_recursive_source_dataset_list=""
			}
			zxfer_reverse_file_lines() {
				cat "$1"
			}
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-t" ] && [ "$3" = "filesystem,volume" ] &&
					[ "$4" = "-Hr" ] && [ "$5" = "-o" ] && [ "$6" = "name" ] &&
					[ "$7" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				return 1
			}
			zxfer_get_zfs_list
			printf 'source_ms=%s\n' "${g_zxfer_profile_source_snapshot_listing_ms:-0}"
			printf 'destination_ms=%s\n' "${g_zxfer_profile_destination_snapshot_listing_ms:-0}"
			printf 'diff_ms=%s\n' "${g_zxfer_profile_snapshot_diff_sort_ms:-0}"
		)
	)

	assertContains "Very-verbose snapshot discovery should accumulate source snapshot listing timings." \
		"$output" "source_ms=1600"
	assertContains "Very-verbose snapshot discovery should accumulate destination listing timings." \
		"$output" "destination_ms=400"
	assertContains "Very-verbose snapshot discovery should accumulate diff/sort timings." \
		"$output" "diff_ms=550"
}

# Without -R the work list is the initial source alone. It is set after full
# discovery published the recursive delta (the -v report and the inventory
# decision describe the listing itself); with -R the delta list stands.
test_get_zfs_list_publishes_only_the_initial_source_without_recursion() {
	for l_recursive_flag in "" "tank/src"; do
		output=$(
			g_option_R_recursive=$l_recursive_flag
			zxfer_try_fast_recursive_noop_discovery() { return 1; }
			zxfer_start_full_source_snapshot_discovery() { :; }
			zxfer_collect_full_destination_snapshot_discovery() { :; }
			zxfer_wait_for_full_source_snapshot_discovery() { :; }
			zxfer_publish_full_snapshot_discovery_results() {
				g_recursive_source_list="tank/src/child"
				printf 'published=%s\n' "$g_recursive_source_list"
			}
			zxfer_get_zfs_list
			printf 'work=%s\n' "$g_recursive_source_list"
		)
		case $l_recursive_flag in
		"") l_expected_work=tank/src ;;
		*) l_expected_work=tank/src/child ;;
		esac
		assertEquals "The work list after discovery [-R '$l_recursive_flag']." \
			"published=tank/src/child
work=$l_expected_work" "$output"
	done
}
