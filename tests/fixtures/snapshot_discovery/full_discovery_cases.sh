#!/bin/sh
# shellcheck shell=sh
# Full discovery, record-cache, and fast recursive no-op behavior cases.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
				printf '%s\n' "$TEST_TMPDIR/stdout-only-zxfer_get_zfs_list.$idx"
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
			printf 'source_reversed_before=%s\n' "${g_lzfs_list_hr_S_snap:-}"
			printf 'source=%s\n' "$(zxfer_get_snapshot_records_for_dataset source "tank/src")"
		)
	)

	assertContains "Bootstrap path should treat the missing destination dataset as an empty recursive list." "$output" "dest="
	assertContains "Snapshot discovery should leave the reversed source cache unset until a later consumer asks for per-dataset records." \
		"$output" "source_reversed_before="
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
				printf '%s\n' "$TEST_TMPDIR/stdout-only-zxfer_get_zfs_list_omnios.$idx"
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
			printf 'source=%s\n' "$(zxfer_get_snapshot_records_for_dataset source "tank/src")"
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
				printf '%s\n' "$TEST_TMPDIR/stdout-only-get_zfs_list_root_missing.$idx"
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
			printf 'missing=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup")"
		)
	)

	assertContains "Rootless missing destinations should bootstrap as an empty inventory when the pool probe succeeds." \
		"$output" "dest=<>"
	assertContains "Rootless missing destinations should seed the destination existence cache as absent." \
		"$output" "missing=0"
}

test_collect_local_destination_dataset_inventory_preserves_setup_and_publish_failures() {
	temp_status=$(
		(
			zxfer_create_temp_file_group() {
				return 66
			}
			set +e
			zxfer_collect_local_destination_dataset_inventory
			printf '%s\n' "$?"
		)
	)
	publish_status=$(
		(
			l_one="$TEST_TMPDIR/local_dest_inventory_publish.one"
			l_two="$TEST_TMPDIR/local_dest_inventory_publish.two"
			g_option_V_very_verbose=1
			zxfer_create_temp_file_group() {
				: >"$l_one"
				: >"$l_two"
				g_zxfer_temp_file_group_result=$(printf '%s\n%s' "$l_one" "$l_two")
				printf '%s\n' "$g_zxfer_temp_file_group_result"
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_publish_destination_dataset_inventory_from_stage() {
				return 67
			}
			set +e
			zxfer_collect_local_destination_dataset_inventory 2>/dev/null
			printf '%s\n' "$?"
		)
	)

	assertEquals "Local destination inventory should preserve temp-file group allocation failures." \
		66 "$temp_status"
	assertEquals "Local destination inventory should preserve publish failures after cleanup." \
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
			printf 'root=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
			printf 'existing=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/existing")"
			printf 'missing=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/missing")"
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

test_get_zfs_list_reports_destination_snapshot_list_readback_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
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
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst
backup/dst/existing"
					return 0
				fi
				if [ "$l_read_count" -eq 2 ]; then
					return 28
				fi
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

	assertEquals "Destination snapshot-list readback failures should preserve the readback status." \
		28 "$status"
	assertContains "Destination snapshot-list readback failures should report the staged destination snapshot context." \
		"$output" "Failed to read staged destination snapshot list."
}

test_get_zfs_list_reports_source_snapshot_list_readback_failures() {
	set +e
	output=$(
		(
			l_read_count=0
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "tank/src@snapA" >"$1"
			}
			zxfer_write_destination_snapshot_list_to_files() {
				printf '%s\n' "backup/dst@snapA" >"$1"
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
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst
backup/dst/existing"
					return 0
				fi
				if [ "$l_read_count" -eq 2 ]; then
					g_zxfer_snapshot_discovery_file_read_result="backup/dst@snapA"
					return 0
				fi
				if [ "$l_read_count" -eq 3 ]; then
					return 29
				fi
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

	assertEquals "Source snapshot-list readback failures should preserve the readback status." \
		29 "$status"
	assertContains "Source snapshot-list readback failures should report the staged source snapshot context." \
		"$output" "Failed to read staged source snapshot list."
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
			cleanup_log="$TEST_TMPDIR/get-zfs-source-cache-stage.cleanup"
			cache_cleanup_log="$TEST_TMPDIR/get-zfs-source-cache-stage.cache-cleanup"
			: >"$cleanup_log"
			: >"$cache_cleanup_log"
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
				g_zxfer_temp_file_result="$TEST_TMPDIR/get-zfs-source-cache-stage-$l_temp_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			zxfer_cleanup_runtime_artifact_paths() {
				printf '%s\n' "$*" >>"$cleanup_log"
				return 0
			}
			zxfer_cleanup_snapshot_record_cache_files() {
				printf '%s\n' "cache-cleanup" >>"$cache_cleanup_log"
				return 0
			}
			zxfer_reverse_file_lines() {
				return 1
			}
			zxfer_throw_error() {
				printf 'cleanup=%s\n' "$(cat "$cleanup_log" 2>/dev/null || :)"
				printf 'cache_cleanup=%s\n' "$(cat "$cache_cleanup_log" 2>/dev/null || :)"
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
	assertContains "Source snapshot record-cache staging failures should clean up the staged source snapshot list file." \
		"$output" "get-zfs-source-cache-stage-1.tmp"
	assertContains "Source snapshot record-cache staging failures should clean up the staged source snapshot stderr file." \
		"$output" "get-zfs-source-cache-stage-2.tmp"
	assertContains "Source snapshot record-cache staging failures should clean up the staged destination snapshot diff file." \
		"$output" "get-zfs-source-cache-stage-4.tmp"
	assertContains "Source snapshot record-cache staging failures should clean up the staged source snapshot-record cache file." \
		"$output" "get-zfs-source-cache-stage-7.tmp"
	assertContains "Source snapshot record-cache staging failures should run the snapshot-record cache cleanup helper." \
		"$output" "cache_cleanup=cache-cleanup"
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
				g_source_snapshot_fast_noop_attempted=1
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
			printf 'source_raw=<%s>\n' "${g_lzfs_list_hr_snap:-}"
			printf 'dest_raw=<%s>\n' "${g_rzfs_list_hr_snap:-}"
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
	assertContains "Recursive no-op discovery should not load the full source snapshot list into shell state when no later work can consume it." \
		"$output" "source_raw=<>"
	assertContains "Recursive no-op discovery should not load the full destination snapshot list into shell state when no later work can consume it." \
		"$output" "dest_raw=<>"
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
				printf "%s\n" "printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "unexpected-full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				return 99
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_get_zfs_list
			printf 'fast_attempted=%s\n' "${g_source_snapshot_fast_noop_attempted:-0}"
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'source_datasets=<%s>\n' "${g_recursive_source_dataset_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'source_raw=<%s>\n' "${g_lzfs_list_hr_snap:-}"
			printf 'dest_raw=<%s>\n' "${g_rzfs_list_hr_snap:-}"
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
	assertContains "Fast no-op proof should not load full source records into shell state." \
		"$output" "source_raw=<>"
	assertContains "Fast no-op proof should not load full destination records into shell state." \
		"$output" "dest_raw=<>"
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
				printf "%s\n" "printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "unexpected-full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				return 99
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_get_zfs_list
			printf 'fast_attempted=%s\n' "${g_source_snapshot_fast_noop_attempted:-0}"
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
				printf "%s\n" "printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapA" "guid-a")
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_filter_snapshot_file_with_excludes() {
				printf '%s\n' "unexpected-filter" >>"$FILTER_LOG"
				return 99
			}
			zxfer_get_zfs_list
			printf 'fast_attempted=%s\n' "${g_source_snapshot_fast_noop_attempted:-0}"
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
			printf 'dest_raw=<%s>\n' "${g_rzfs_list_hr_snap:-}"
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
	assertContains "Exact fast no-op proof should avoid loading destination records into shell state." \
		"$output" "dest_raw=<>"
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
				printf "%s\n" "printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				printf '%s\n' "tank/src@snapA" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				printf '%s\n' "destination-discovery" >>"$DESTINATION_CALL_LOG"
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=$(printf '%s\t%s' "tank/src@snapB" "guid-b")
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
			printf 'fast_attempted=%s\n' "${g_source_snapshot_fast_noop_attempted:-0}"
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
		)
	)

	assertEquals "A non-no-op name comparison should fall back to full source discovery and full diff planning." \
		"full-source-discovery
full-diff-planning" "$(cat "$full_discovery_log")"
	assertEquals "Destination discovery is expected once for the proof and once again for the full fallback path." \
		"2" "$(wc -l <"$destination_call_log" | tr -d '[:space:]')"
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
				printf "%s\n" "printf '%s\t%s\n' 'tank/src@snapA' 'source-guid'"
			}
			zxfer_write_source_snapshot_list_to_file() {
				printf '%s\n' "full-source-discovery" >>"$FULL_DISCOVERY_LOG"
				printf '%s\t%s\n' "tank/src@snapA" "source-guid" >"$1"
				: >"$2"
				g_source_snapshot_list_pid=""
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
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
			printf 'fast_attempted=%s\n' "${g_source_snapshot_fast_noop_attempted:-0}"
			printf 'source_list=<%s>\n' "${g_recursive_source_list:-}"
			printf 'dest_extra=<%s>\n' "${g_recursive_destination_extra_dataset_list:-}"
		)
	)

	assertEquals "A same-name GUID mismatch should fall back to full source discovery and full diff planning." \
		"full-source-discovery
full-diff-planning" "$(cat "$full_discovery_log")"
	assertEquals "Destination discovery should run once for the identity proof and once again for the full fallback path." \
		"2" "$(wc -l <"$destination_call_log" | tr -d '[:space:]')"
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
				printf "%s\n" "printf '%s\n' 'tank/src/replica@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
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
			printf 'fast_attempted=%s\n' "${g_source_snapshot_fast_noop_attempted:-0}"
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

# Probe zxfer_fast_recursive_noop_discovery_is_eligible with a clean option
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
		zxfer_fast_recursive_noop_discovery_is_eligible
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
				printf "%s\n" "printf '%s\t%s\n' 'tank/src@snapA' 'guid-a'"
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
	command_read_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_read_source_snapshot_discovery_command_file() {
				return 46
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
			printf '%s\n' "$?"
		)
	)
	empty_command_status=0
	empty_command_output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				:
			}
			zxfer_throw_error() {
				printf 'throw:%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			set +e
			zxfer_try_fast_recursive_noop_discovery
		)
	) || empty_command_status=$?
	execute_status=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
	assertEquals "Fast no-op proof should preserve staged source command readback failures." \
		46 "$command_read_status"
	assertContains "Fast no-op proof should fail closed when staged source command readback is empty." \
		"$empty_command_output" "throw:Staged source snapshot no-op proof command was empty.:1"
	assertEquals "Fast no-op proof should return failure when staged source command readback is empty." \
		1 "$empty_command_status"
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
				printf "%s\n" "sh -c 'printf %s denied >&2; exit 17'"
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
				printf "%s\n" "sh -c 'exit 17'"
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
				printf '%s\n' ":"
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
			l_read_count=0
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				printf "%s\n" "sh -c 'exit 17'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="sh -c 'exit 17'"
					return 0
				fi
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
			l_status_read_count=0
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_read_snapshot_discovery_status_file() {
				l_status_read_count=$((l_status_read_count + 1))
				g_zxfer_snapshot_discovery_status_file_result=0
				[ "$l_status_read_count" -lt 4 ] && return 0
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
			l_read_count=0
			g_option_O_origin_host="origin.example"
			g_option_R_recursive="tank/src"
			zxfer_build_source_snapshot_name_list_cmd() {
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS=17
				ZXFER_TEST_FAST_NOOP_DESTINATION_STDERR="permission denied"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			zxfer_read_snapshot_discovery_capture_file() {
				l_read_count=$((l_read_count + 1))
				if [ "$l_read_count" -eq 1 ]; then
					g_zxfer_snapshot_discovery_file_read_result="printf '%s\n' 'tank/src@snapA'"
					return 0
				fi
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				l_fifo=$1
				l_err_file=$2
				l_list_status_file=$3
				l_normalize_status_file=$4
				l_stream_status_file=$5
				(
					printf '%s\n' "tank/src@snapA" >"$l_fifo"
					: >"$l_err_file"
					printf '%s\n' 0 >"$l_list_status_file"
					printf '%s\n' 0 >"$l_normalize_status_file"
					printf '%s\n' 0 >"$l_stream_status_file"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
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
				printf "%s\n" "printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			comm() {
				cat "$2" >/dev/null &
				l_left_cat_pid=$!
				cat "$3" >/dev/null
				wait "$l_left_cat_pid" 2>/dev/null || :
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

test_get_zfs_list_preserves_fast_noop_hard_failure_status() {
	status=$(
		(
			zxfer_try_fast_recursive_noop_discovery() {
				return 58
			}
			set +e
			zxfer_get_zfs_list
			printf '%s\n' "$?"
		)
	)

	assertEquals "Snapshot discovery should return fast no-op proof hard failures without continuing into full discovery." \
		58 "$status"
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
			zxfer_get_snapshot_records_for_dataset source "tank/src" >"$source_root_file"
			zxfer_get_snapshot_records_for_dataset source "tank/src/child" >"$source_child_file"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst" >"$dest_root_file"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst/child" >"$dest_child_file"
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
