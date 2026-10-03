#!/bin/sh
# shellcheck shell=sh
# Full discovery and fast recursive no-op proof cases for
# src/zxfer_snapshot_discovery.sh that only a unit test can reach: staged-file
# readback, mktemp, spawn, registration and compare failures, malformed
# producer statuses, stage failures that return without throwing, the -V
# stage timers, and the proof's eligibility gates. Run by
# tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
	# A no-op cannot need -U, -g or -d work, so they keep the proof.
	for l_eligibility_safe in \
		"g_option_U_skip_unsupported_properties=1" \
		"g_option_g_grandfather_protection=30" \
		"g_option_d_delete_destination_snapshots=1"; do
		assertEquals "Runs with $l_eligibility_safe must stay proof-eligible." \
			0 "$(zxfer_test_noop_proof_eligibility_status "$l_eligibility_safe")"
	done
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

# The proof's source validation reads two staged files the fault injector
# cannot fail: a failed stderr readback stops the proof with its own status
# before any source diagnostic, and an unreadable count sidecar fails closed
# as an empty source.
test_try_fast_recursive_noop_discovery_reports_source_failures() {
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
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || count_read_status=$?

	assertContains "Fast no-op proof should report staged stderr readback failures before surfacing source failure context." \
		"$stderr_read_output" "throw:Failed to read staged source snapshot stderr.:68"
	assertEquals "Fast no-op proof should preserve staged stderr readback failure status." \
		68 "$stderr_read_status"
	assertContains "Fast no-op proof should fail closed when source snapshot count sidecar validation fails." \
		"$count_read_output" "throw:Failed to retrieve snapshots from the source:1"
	assertEquals "Fast no-op proof should use the generic source failure status for invalid source count sidecars." \
		1 "$count_read_status"
}

# The proof's destination producer reports its listing, normalize and sort
# statuses on one line that the fault injector cannot corrupt: a malformed
# field fails the proof closed, a failed stderr readback keeps its own
# status, and a failed sort or producer exit keeps its status.
test_try_fast_recursive_noop_discovery_reports_destination_fifo_status_failures() {
	for l_malformed_field in LIST NORMALIZE SORT; do
		malformed_status=0
		malformed_output=$(
			(
				g_option_O_origin_host="origin.example"
				g_option_R_recursive="tank/src"
				zxfer_build_source_snapshot_name_list_cmd() {
					g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
				}
				MALFORMED_FIELD=$l_malformed_field
				zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
					ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
					# if, not case: bash 3.2 misparses case patterns inside $(...).
					if [ "$MALFORMED_FIELD" = LIST ]; then
						ZXFER_TEST_FAST_NOOP_DESTINATION_LIST_STATUS="bad"
					elif [ "$MALFORMED_FIELD" = NORMALIZE ]; then
						ZXFER_TEST_FAST_NOOP_DESTINATION_NORMALIZE_STATUS="bad"
					else
						ZXFER_TEST_FAST_NOOP_DESTINATION_SORT_STATUS="bad"
					fi
					zxfer_test_start_fast_noop_destination_fifo_producer "$@"
				}
				zxfer_throw_error() {
					printf 'throw:%s:%s\n' "$1" "${2:-1}"
					exit "${2:-1}"
				}
				zxfer_try_fast_recursive_noop_discovery
			)
		) || malformed_status=$?
		assertContains "Fast no-op proof should fail closed on a malformed $l_malformed_field status." \
			"$malformed_output" "throw:Failed to validate destination snapshot status for recursive no-op proof.:1"
		assertEquals "Fast no-op proof should return failure for a malformed $l_malformed_field status." \
			1 "$malformed_status"
	done
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
			zxfer_try_fast_recursive_noop_discovery
		) 2>&1
	) || destination_stderr_read_status=$?
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
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf '%s\n' "$?"
		)
	)

	assertContains "Fast no-op proof should report destination stderr readback failures before surfacing destination snapshot context." \
		"$destination_stderr_read_output" "throw:Failed to read staged destination snapshot stderr.:70"
	assertEquals "Fast no-op proof should preserve destination stderr readback failure status." \
		70 "$destination_stderr_read_status"
	assertEquals "Fast no-op proof should preserve destination stream failures." \
		23 "$sort_status"
	assertEquals "Fast no-op proof should preserve destination producer wait failures." \
		31 "$destination_wait_status"
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
			zxfer_write_source_snapshot_list_to_file() {
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
			zxfer_write_source_snapshot_list_to_file() {
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

test_get_zfs_list_tracks_stage_timings_when_very_verbose() {
	output=$(
		(
			counter_file="$TEST_TMPDIR/get_zfs_profile.counter"
			printf '%s\n' 0 >"$counter_file"
			zxfer_get_temp_file() {
				idx=$(cat "$counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$counter_file"
				g_zxfer_temp_file_result="$TEST_TMPDIR/get_zfs_profile.$idx"
				: >"$g_zxfer_temp_file_result"
			}
			# One reading per clock read, in this shell: source start,
			# destination start and end, source end, diff start and end.
			clock_readings="1000 1500 1900 2600 3000 3550"
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=${clock_readings%% *}
				clock_readings=${clock_readings#* }
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

# Status is a completed-producer result, consumed by both raw-list handoff and
# proof validation. A second parse would make their view of one operation differ.
test_fast_recursive_noop_reads_destination_status_once() {
	output=$(
		(
			g_option_R_recursive="-R"
			status_reads=0
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'tank/src@snapA'"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED="tank/src@snapA"
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			read() {
				if [ "$*" = '-r l_fast_noop_list_status l_fast_noop_normalize_status l_fast_noop_sort_status' ]; then
					status_reads=$((status_reads + 1))
				fi
				# shellcheck disable=SC2162 # Forward the production read arguments.
				command read "$@"
			}
			zxfer_try_fast_recursive_noop_discovery >/dev/null
			printf 'status=%s reads=%s\n' "$?" "$status_reads"
		)
	)
	assertEquals "A successful proof consumes one completed status record." \
		"status=0 reads=1" "$output"
}

# The operation owner must retain the failure status and diagnostic before it
# clears staged state. Cleanup deliberately returns a different status here.
test_fast_recursive_noop_owner_reports_failure_after_releasing_scratch() {
	status=0
	output=$(
		(
			g_option_R_recursive="-R"
			zxfer_build_source_snapshot_name_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' 'original source failure' >&2; exit 37"
			}
			zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
				ZXFER_TEST_FAST_NOOP_DESTINATION_SORTED=""
				zxfer_test_start_fast_noop_destination_fifo_producer "$@"
			}
			release_called=0
			zxfer_cleanup_runtime_artifact_path_list() {
				release_called=1
				return 91
			}
			zxfer_throw_error() {
				printf 'status=%s diagnostic=<%s> released=%s\n' \
					"$2" "$1" "$release_called"
				exit "$2"
			}
			zxfer_try_fast_recursive_noop_discovery
		)
	) || status=$?
	assertEquals "Cleanup cannot overwrite the original failed producer status." 37 "$status"
	assertEquals "The diagnostic survives scratch reset and is reported afterwards." \
		"status=37 diagnostic=<Failed to retrieve snapshots from the source: original source failure> released=1" "$output"
}

# A destination/setup failure can reach release before source wait. Keep the
# unreaped producer's files for the immediate failure throw's ordered EXIT trap.
test_full_discovery_release_preserves_unreaped_producer_files() {
	zxfer_create_temp_file_group 2
	{
		IFS= read -r source_file
		IFS= read -r error_file
	} <<-EOF
		$g_zxfer_temp_file_group_result
	EOF
	# Use an ownership marker instead of an extra child for the release decision.
	g_source_snapshot_list_pid=12345
	zxfer_cleanup_full_snapshot_discovery_operation_state 29 "$source_file" "$error_file" "" "" ""
	assertTrue "An unreaped producer keeps its source stage until trap teardown." \
		"[ -f '$source_file' ] && [ -f '$error_file' ]"
	g_source_snapshot_list_pid=""
	zxfer_cleanup_full_snapshot_discovery_operation_state 29 "$source_file" "$error_file" "" "" ""
	assertFalse "A completed producer's stages are released." "[ -f '$source_file' ]"
}

# Helper scratch and shared allocator result channels must not become the
# operation's file registry: stages can overwrite them without losing ownership.
test_full_discovery_keeps_owned_paths_across_helper_scratch_mutation() {
	output=$(
		(
			zxfer_write_source_snapshot_list_to_file() {
				source_seen=$1
				error_seen=$2
				sorted_seen=$3
				printf 'tank/src@snapA\n' >"$1"
				: >"$2"
				printf 'tank/src@snapA\n' >"$3"
				l_full_operation_source_file=clobbered
				l_full_operation_sorted_file=clobbered
				g_zxfer_temp_file_group_result=clobbered
			}
			zxfer_collect_full_destination_snapshot_discovery() {
				destination_seen=$1
				destination_sorted_seen=$2
				: >"$1"
				: >"$2"
				g_zxfer_temp_file_result=clobbered
			}
			zxfer_publish_full_snapshot_discovery_results() {
				[ "$1" = "$source_seen" ] && [ "$2" = "$sorted_seen" ] &&
					[ "$3" = "$destination_seen" ] && [ "$4" = "$destination_sorted_seen" ] || return 73
			}
			zxfer_run_full_snapshot_discovery
			printf 'status=%s\n' "$?"
			for owned_path in "$source_seen" "$error_seen" "$sorted_seen" \
				"$destination_seen" "$destination_sorted_seen"; do
				[ ! -e "$owned_path" ] || printf 'unreleased=%s\n' "$owned_path"
			done
		)
	)
	assertEquals "The owner retains and releases its real paths after helpers mutate scratch." \
		"status=0" "$output"
}
