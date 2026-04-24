#!/bin/sh
#
# shunit2 tests for zxfer_runtime.sh helpers.
#
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# zxfer_init_globals() now delegates reset to the owner helpers that live
# through the replication layer, so source the full runtime stack that defines
# those helpers.
zxfer_source_runtime_modules_through "zxfer_replication.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_runtime"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	unset ZXFER_BACKUP_DIR
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_runtime_artifact_state
	zxfer_reset_background_job_state
	zxfer_reset_cleanup_pid_tracking
	zxfer_reset_failure_context "unit"
	g_option_Y_yield_iterations=1
	g_option_z_compress=0
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
}

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

test_refresh_backup_storage_root_rejects_relative_override() {
	zxfer_test_capture_subshell '
		ZXFER_BACKUP_DIR="relative-backups"
		zxfer_refresh_backup_storage_root
	'

	assertEquals "Relative ZXFER_BACKUP_DIR overrides should fail closed." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative backup-root errors should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_BACKUP_DIR must be an absolute path"
}

test_get_temp_file_creates_unique_paths() {
	file_one=$(zxfer_get_temp_file)
	file_two=$(zxfer_get_temp_file)

	assertNotEquals "Each temp-file request should return a unique path." \
		"$file_one" "$file_two"
	assertTrue "The first temp file should exist." '[ -f "$file_one" ]'
	assertTrue "The second temp file should exist." '[ -f "$file_two" ]'
}

test_zxfer_cleanup_pid_helpers_cover_current_shell_paths() {
	sleep 30 &
	first_pid=$!
	sleep 30 &
	second_pid=$!

	output=$(
		(
			zxfer_register_cleanup_pid ""
			zxfer_register_cleanup_pid "$first_pid" "unit cleanup helper"
			zxfer_register_cleanup_pid "$second_pid" "unit cleanup helper"
			zxfer_register_cleanup_pid "$second_pid" "unit cleanup helper"
			printf 'registered=<%s>\n' "$g_zxfer_cleanup_pids"

			zxfer_unregister_cleanup_pid "$first_pid"
			printf 'after_unregister=<%s>\n' "$g_zxfer_cleanup_pids"

			zxfer_register_cleanup_pid "$$" "current shell"
			zxfer_abort_cleanup_pid() {
				printf 'abort:%s\n' "$1"
				zxfer_unregister_cleanup_pid "$1"
				return 0
			}
			zxfer_kill_registered_cleanup_pids
			printf 'after_kill=<%s>\n' "$g_zxfer_cleanup_pids"
		)
	)

	kill -s TERM "$first_pid" >/dev/null 2>&1 || true
	kill -s TERM "$second_pid" >/dev/null 2>&1 || true
	wait "$first_pid" 2>/dev/null || true
	wait "$second_pid" 2>/dev/null || true

	assertContains "Cleanup PID registration should keep unique live helper PIDs." \
		"$output" "registered=<$first_pid $second_pid>"
	assertContains "Cleanup PID unregistration should remove only the requested helper PID." \
		"$output" "after_unregister=<$second_pid>"
	assertContains "Cleanup PID teardown should delegate validated teardown for the remaining helper PID." \
		"$output" "abort:$second_pid"
	assertContains "Cleanup PID teardown should clear the registered helper PID list after delegated validated teardown." \
		"$output" "after_kill=<>"
}

test_zxfer_abort_cleanup_pid_untracks_stale_start_token_mismatches() {
	sleep 30 &
	tracked_pid=$!

	zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
	zxfer_find_cleanup_pid_record "$tracked_pid"
	g_zxfer_cleanup_pid_records="$tracked_pid	unit cleanup helper	bogus-start-token	$g_zxfer_cleanup_pid_record_hostname	$g_zxfer_cleanup_pid_record_pgid	$g_zxfer_cleanup_pid_record_teardown_mode"

	zxfer_abort_cleanup_pid "$tracked_pid" TERM
	status=$?

	assertEquals "Validated cleanup teardown should treat a start-token mismatch as a stale record and return success." \
		0 "$status"
	assertEquals "Validated cleanup teardown should keep the abort-failure message empty when a tracked helper record is stale." \
		"" "$g_zxfer_cleanup_pid_abort_failure_message"
	assertTrue "Validated cleanup teardown should leave the live helper running when the tracked record is stale and no longer validated for teardown." \
		"kill -s 0 \"$tracked_pid\" >/dev/null 2>&1"
	assertEquals "Validated cleanup teardown should unregister stale helper records once ownership validation fails cleanly." \
		"" "$g_zxfer_cleanup_pids"

	kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
	wait "$tracked_pid" 2>/dev/null || true
}

test_zxfer_abort_cleanup_pid_fails_closed_when_ownership_validation_errors() {
	output=$(
		(
			sleep 30 &
			tracked_pid=$!
			zxfer_register_cleanup_pid "$tracked_pid" "unit cleanup helper"
			zxfer_owned_lock_owner_is_live() {
				return 2
			}
			zxfer_abort_cleanup_pid "$tracked_pid" TERM
			printf 'status=%s\n' "$?"
			printf 'message=%s\n' "$g_zxfer_cleanup_pid_abort_failure_message"
			printf 'pids=<%s>\n' "$g_zxfer_cleanup_pids"
			kill -s TERM "$tracked_pid" >/dev/null 2>&1 || true
			wait "$tracked_pid" 2>/dev/null || true
		)
	)

	assertContains "Validated cleanup teardown should fail closed when ownership validation returns an internal error." \
		"$output" "status=1"
	assertContains "Validated cleanup teardown should preserve the ownership-validation failure message when validation errors out." \
		"$output" "message=Failed to validate ownership for cleanup helper [unit cleanup helper] (PID "
	assertContains "Validated cleanup teardown should preserve the tracked PID list after an ownership-validation error." \
		"$output" "pids=<"
	assertNotContains "Validated cleanup teardown should not clear the tracked PID list after an ownership-validation error." \
		"$output" "pids=<>"
}

test_zxfer_cleanup_pid_low_level_helpers_cover_validation_paths() {
	zxfer_test_capture_subshell '
		set +e
		fake_ps() {
			if [ "$1" = "-o" ] && [ "$2" = "pgid=" ] && [ "$3" = "-p" ]; then
				case "$4" in
				4242)
					printf "%s\n" " 900 "
					;;
				5050)
					printf "%s\n" "not-a-pgid"
					;;
				*)
					return 1
					;;
				esac
				return 0
			fi
			if [ "$1" = "-o" ] && [ "$2" = "pid=" ] && [ "$3" = "-o" ] &&
				[ "$4" = "ppid=" ] && [ "$5" = "-o" ] && [ "$6" = "pgid=" ]; then
				printf "%s\n" "4242 1 900"
				printf "%s\n" "4243 4242 900"
				return 0
			fi
			return 1
		}
		kill() {
			case "$1:$2" in
			-TERM:4242 | -TERM:4243 | -TERM:-900)
				return 0
				;;
			esac
			return 1
		}

		g_cmd_ps=fake_ps
		g_zxfer_cleanup_pid_records="4242	unit cleanup helper	start-token	unit-host	900	process_group"
		zxfer_validate_cleanup_pid_teardown_mode child_set
		printf "mode_child=%s\n" "$?"
		zxfer_validate_cleanup_pid_teardown_mode process_group
		printf "mode_group=%s\n" "$?"
		zxfer_validate_cleanup_pid_teardown_mode invalid
		printf "mode_invalid=%s\n" "$?"
		printf "pgid_ok=%s\n" "$(zxfer_read_cleanup_pid_process_group 4242)"
		zxfer_read_cleanup_pid_process_group 5050 >/dev/null 2>&1
		printf "pgid_bad=%s\n" "$?"
		zxfer_find_cleanup_pid_record 4242
		printf "find_hit=%s:%s:%s\n" "$?" \
			"$g_zxfer_cleanup_pid_record_purpose" \
			"$g_zxfer_cleanup_pid_record_teardown_mode"
		zxfer_find_cleanup_pid_record 9999 >/dev/null 2>&1
		printf "find_miss=%s:<%s>\n" "$?" "$g_zxfer_cleanup_pid_record_purpose"
		snapshot_read=$(zxfer_read_cleanup_pid_process_snapshot)
		printf "snapshot_read_status=%s\n" "$?"
		printf "snapshot_read=<%s>\n" "$(printf "%s" "$snapshot_read" | tr "\n" ";")"
		g_cmd_ps=false
		zxfer_read_cleanup_pid_process_snapshot >/dev/null 2>&1
		printf "snapshot_read_fail=%s\n" "$?"
		g_cmd_ps=fake_ps
		l_snapshot=$(printf "%s\n%s\n" "4242 1 900" "4243 4242 900")
		zxfer_cleanup_pid_snapshot_has_pid_with_pgid "$l_snapshot" 4242 900
		printf "has_pgid=%s\n" "$?"
		zxfer_cleanup_pid_snapshot_has_pid_with_pgid "$l_snapshot" bad 900 >/dev/null 2>&1
		printf "has_pgid_bad=%s\n" "$?"
		zxfer_cleanup_pid_snapshot_has_pid_with_parent "$l_snapshot" 4243 4242
		printf "has_parent=%s\n" "$?"
		zxfer_cleanup_pid_snapshot_has_pid_with_parent "$l_snapshot" 4243 bad >/dev/null 2>&1
		printf "has_parent_bad=%s\n" "$?"
		l_pid_set=$(zxfer_get_cleanup_pid_set "$l_snapshot" 4242)
		printf "pid_set_status=%s\n" "$?"
		printf "pid_set=<%s>\n" "$(printf "%s" "$l_pid_set" | tr "\n" " " | sed "s/[[:space:]]*$//")"
		zxfer_get_cleanup_pid_set "$l_snapshot" "" >/dev/null 2>&1
		printf "pid_set_blank=%s\n" "$?"
		l_signal_input=$(printf "%s\n%s\n" "4242" "4243")
		zxfer_signal_cleanup_pid_set "$l_signal_input" TERM
		printf "signal_set=%s\n" "$?"
		zxfer_signal_cleanup_process_group 900 TERM
		printf "signal_group=%s\n" "$?"
		zxfer_signal_cleanup_process_group bad TERM >/dev/null 2>&1
		printf "signal_group_bad=%s\n" "$?"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated cleanup-helper mode validation should accept child-set teardown." \
		"$output" "mode_child=0"
	assertContains "Validated cleanup-helper mode validation should accept process-group teardown." \
		"$output" "mode_group=0"
	assertContains "Validated cleanup-helper mode validation should reject unknown teardown modes." \
		"$output" "mode_invalid=1"
	assertContains "Validated cleanup-helper process-group reads should trim whitespace around numeric pgids." \
		"$output" "pgid_ok=900"
	assertContains "Validated cleanup-helper process-group reads should reject nonnumeric pgids." \
		"$output" "pgid_bad=1"
	assertContains "Validated cleanup-helper record lookup should publish the stored purpose and teardown mode for matching PIDs." \
		"$output" "find_hit=0:unit cleanup helper:process_group"
	assertContains "Validated cleanup-helper record lookup should clear the published record fields after a miss." \
		"$output" "find_miss=1:<>"
	assertContains "Validated cleanup-helper snapshot reads should preserve the ps output in current-shell scratch state." \
		"$output" "snapshot_read_status=0"
	assertContains "Validated cleanup-helper snapshot reads should fail closed when ps cannot be queried." \
		"$output" "snapshot_read_fail=1"
	assertContains "Validated cleanup-helper snapshot scans should detect matching pgids." \
		"$output" "has_pgid=0"
	assertContains "Validated cleanup-helper snapshot scans should reject malformed pgid probes." \
		"$output" "has_pgid_bad=1"
	assertContains "Validated cleanup-helper snapshot scans should detect matching parent-child ownership." \
		"$output" "has_parent=0"
	assertContains "Validated cleanup-helper snapshot scans should reject malformed parent probes." \
		"$output" "has_parent_bad=1"
	assertContains "Validated cleanup-helper child-set derivation should return the owned root and descendants." \
		"$output" "pid_set=<4242 4243>"
	assertContains "Validated cleanup-helper child-set derivation should fail closed when the root pid is blank." \
		"$output" "pid_set_blank=1"
	assertContains "Validated cleanup-helper child-set signaling should return success when every tracked pid is signaled." \
		"$output" "signal_set=0"
	assertContains "Validated cleanup-helper process-group signaling should return success for numeric pgids." \
		"$output" "signal_group=0"
	assertContains "Validated cleanup-helper process-group signaling should reject malformed pgids." \
		"$output" "signal_group_bad=1"
}

test_zxfer_register_cleanup_pid_covers_validation_and_transient_lookup_paths() {
	zxfer_test_capture_subshell '
		set +e
		kill() {
			[ "$1" = "-s" ] && [ "$2" = "0" ] || return 1
			[ "$3" = "${live_pid:-}" ]
		}
		zxfer_normalize_owned_lock_text_field() {
			[ "${normalize_fail:-0}" -eq 1 ] && return 1
			printf "%s\n" "$1"
		}
		zxfer_get_process_start_token() {
			[ "${start_fail:-0}" -eq 1 ] && return 1
			printf "start-%s\n" "$1"
		}
		zxfer_get_owned_lock_hostname() {
			[ "${hostname_fail:-0}" -eq 1 ] && return 1
			printf "%s\n" "unit-host"
		}
		zxfer_read_cleanup_pid_process_group() {
			printf "%s\n" "900"
		}

		live_pid=101
		normalize_fail=1
		zxfer_register_cleanup_pid 101 "unit cleanup helper" child_set
		printf "normalize=%s\n" "$?"
		normalize_fail=0

		live_pid=102
		zxfer_register_cleanup_pid 102 "unit cleanup helper" invalid
		printf "mode=%s\n" "$?"

		start_fail=1
		live_pid=""
		zxfer_register_cleanup_pid 103 "unit cleanup helper" child_set
		printf "start_gone=%s\n" "$?"

		live_pid=104
		zxfer_register_cleanup_pid 104 "unit cleanup helper" child_set
		printf "start_live=%s\n" "$?"
		start_fail=0

		hostname_fail=1
		live_pid=""
		zxfer_register_cleanup_pid 105 "unit cleanup helper" child_set
		printf "host_gone=%s\n" "$?"

		live_pid=106
		zxfer_register_cleanup_pid 106 "unit cleanup helper" child_set
		printf "host_live=%s\n" "$?"
		hostname_fail=0

		live_pid=107
		zxfer_register_cleanup_pid 107 "unit cleanup helper" process_group
		printf "success=%s\n" "$?"
		printf "pids=<%s>\n" "$g_zxfer_cleanup_pids"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Cleanup-helper registration should fail closed when purpose normalization fails." \
		"$output" "normalize=1"
	assertContains "Cleanup-helper registration should fail closed when teardown mode validation fails." \
		"$output" "mode=1"
	assertContains "Cleanup-helper registration should treat disappearing helpers as already gone when start-token lookup races with exit." \
		"$output" "start_gone=0"
	assertContains "Cleanup-helper registration should fail closed when start-token lookup fails for a still-live helper." \
		"$output" "start_live=1"
	assertContains "Cleanup-helper registration should treat disappearing helpers as already gone when hostname lookup races with exit." \
		"$output" "host_gone=0"
	assertContains "Cleanup-helper registration should fail closed when hostname lookup fails for a still-live helper." \
		"$output" "host_live=1"
	assertContains "Cleanup-helper registration should preserve the success path for validated process-group helpers." \
		"$output" "success=0"
	assertContains "Cleanup-helper registration should only track helpers that published validated metadata successfully." \
		"$output" "pids=<107>"
	assertContains "Cleanup-helper registration should store the requested teardown mode in the validated record set." \
		"$output" "records=<107	unit cleanup helper	start-107	unit-host	900	process_group>"
}

test_zxfer_register_cleanup_pid_replaces_stale_records_when_numeric_pids_are_reused() {
	zxfer_test_capture_subshell '
		set +e
		kill() {
			[ "$1" = "-s" ] && [ "$2" = "0" ] || return 1
			[ "$3" = "108" ]
		}
		zxfer_normalize_owned_lock_text_field() {
			printf "%s\n" "$1"
		}
		zxfer_get_process_start_token() {
			printf "start-108-new\n"
		}
		zxfer_get_owned_lock_hostname() {
			printf "%s\n" "unit-host"
		}
		zxfer_read_cleanup_pid_process_group() {
			printf "%s\n" "901"
		}

		g_zxfer_cleanup_pids="108"
		g_zxfer_cleanup_pid_records="108	stale cleanup helper	start-108-old	unit-host	700	child_set"

		zxfer_register_cleanup_pid 108 "replacement cleanup helper" process_group
		printf "status=%s\n" "$?"
		printf "pids=<%s>\n" "$g_zxfer_cleanup_pids"
		printf "records=<%s>\n" "$g_zxfer_cleanup_pid_records"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Cleanup-helper registration should accept a reused numeric pid when the live helper has a different validated start token." \
		"$output" "status=0"
	assertContains "Cleanup-helper registration should preserve only one tracked pid after replacing a stale record for a reused numeric pid." \
		"$output" "pids=<108>"
	assertContains "Cleanup-helper registration should replace stale reused-pid metadata with the current validated helper identity." \
		"$output" "records=<108	replacement cleanup helper	start-108-new	unit-host	901	process_group>"
	assertNotContains "Cleanup-helper registration should discard the stale reused-pid metadata once the live helper has been revalidated." \
		"$output" "start-108-old"
}

test_zxfer_abort_direct_child_pid_reports_lookup_and_ownership_failures() {
	zxfer_test_capture_subshell '
		set +e
		kill() {
			[ "$1" = "-s" ] && [ "$2" = "0" ] && [ "$3" = "201" ]
		}
		zxfer_normalize_owned_lock_text_field() {
			printf "%s\n" "$1"
		}

			zxfer_read_cleanup_pid_process_snapshot() {
				return 23
			}
			zxfer_abort_direct_child_pid 201 TERM "unit cleanup helper"
			printf "snapshot=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"

		zxfer_read_cleanup_pid_process_snapshot() {
			g_zxfer_cleanup_pid_process_snapshot_result="201 999 700"
			printf "%s\n" "$g_zxfer_cleanup_pid_process_snapshot_result"
		}
		zxfer_abort_direct_child_pid 201 TERM "unit cleanup helper"
		printf "ownership=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"

		zxfer_read_cleanup_pid_process_snapshot() {
			g_zxfer_cleanup_pid_process_snapshot_result="201 $$ 700"
			printf "%s\n" "$g_zxfer_cleanup_pid_process_snapshot_result"
		}
		zxfer_get_cleanup_pid_set() {
			return 1
		}
		zxfer_abort_direct_child_pid 201 TERM "unit cleanup helper"
		printf "pidset=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated direct-child abort should fail closed when the process table cannot be inspected." \
		"$output" "snapshot=23:Failed to inspect the process table for cleanup helper [unit cleanup helper] (PID 201)."
	assertContains "Validated direct-child abort should refuse to tear down helpers that are no longer direct children of the current zxfer process." \
		"$output" "ownership=1:Refusing to tear down cleanup helper [unit cleanup helper] (PID 201) because it is no longer an owned child of the current zxfer process."
	assertContains "Validated direct-child abort should fail closed when it cannot derive the owned child set." \
		"$output" "pidset=1:Failed to derive the owned child set for cleanup helper [unit cleanup helper] (PID 201)."
}

test_zxfer_abort_direct_child_pid_signals_owned_child_sets_and_reports_signal_failures() {
	zxfer_test_capture_subshell '
		set +e
		kill() {
			if [ "$1" = "-s" ] && [ "$2" = "0" ] && [ "$3" = "202" ]; then
				return 0
			fi
			if [ "$1" = "-TERM" ]; then
				printf "signal:%s\n" "$2"
				[ "${signal_fail:-0}" -eq 0 ]
				return $?
			fi
			return 1
		}
		zxfer_normalize_owned_lock_text_field() {
			printf "%s\n" "$1"
		}
		zxfer_read_cleanup_pid_process_snapshot() {
			g_zxfer_cleanup_pid_process_snapshot_result=$(printf "%s\n%s\n" "202 $$ 700" "203 202 700")
			printf "%s\n" "$g_zxfer_cleanup_pid_process_snapshot_result"
		}

		signal_fail=0
		zxfer_abort_direct_child_pid 202 TERM "unit cleanup helper"
		printf "success=%s\n" "$?"

		signal_fail=1
		zxfer_abort_direct_child_pid 202 TERM "unit cleanup helper"
		printf "signal_failure=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated direct-child abort should signal the owned root helper pid." \
		"$output" "signal:202"
	assertContains "Validated direct-child abort should signal owned descendants as part of the derived child set." \
		"$output" "signal:203"
	assertContains "Validated direct-child abort should return success when the owned child set is signaled successfully." \
		"$output" "success=0"
	assertContains "Validated direct-child abort should fail closed when signaling the derived child set fails while the helper is still live." \
		"$output" "signal_failure=1:Failed to signal the owned child set for cleanup helper [unit cleanup helper] (PID 202)."
}

test_zxfer_abort_direct_child_pid_current_shell_shortcuts_cover_remaining_paths() {
	zxfer_abort_direct_child_pid "" TERM "unit cleanup helper" >/dev/null 2>&1
	l_invalid_status=$?

	zxfer_abort_direct_child_pid "$$" TERM "unit cleanup helper" >/dev/null 2>&1
	l_self_status=$?

	zxfer_normalize_owned_lock_text_field() {
		return 1
	}
	zxfer_abort_direct_child_pid 201 TERM "unit cleanup helper" >/dev/null 2>&1
	l_normalize_status=$?

	zxfer_normalize_owned_lock_text_field() {
		printf '%s\n' "$1"
	}
	kill() {
		if [ "$1" = "-s" ] && [ "$2" = "0" ]; then
			return 1
		fi
		return 1
	}
	zxfer_abort_direct_child_pid 202 TERM "unit cleanup helper" >/dev/null 2>&1
	l_gone_status=$?

	l_kill_probe_calls=0
	kill() {
		if [ "$1" = "-s" ] && [ "$2" = "0" ]; then
			l_kill_probe_calls=$((l_kill_probe_calls + 1))
			[ "$l_kill_probe_calls" -eq 1 ] && return 0
			return 1
		fi
		return 1
	}
	zxfer_normalize_owned_lock_text_field() {
		printf '%s\n' "$1"
	}
	zxfer_read_cleanup_pid_process_snapshot() {
		g_zxfer_cleanup_pid_process_snapshot_result="203 999 700"
		printf '%s\n' "$g_zxfer_cleanup_pid_process_snapshot_result"
	}
	zxfer_abort_direct_child_pid 203 TERM "unit cleanup helper" >/dev/null 2>&1
	l_orphan_gone_status=$?

	l_kill_probe_calls=0
	kill() {
		if [ "$1" = "-s" ] && [ "$2" = "0" ]; then
			l_kill_probe_calls=$((l_kill_probe_calls + 1))
			[ "$l_kill_probe_calls" -eq 1 ] && return 0
			return 1
		fi
		if [ "$1" = "-TERM" ]; then
			return 1
		fi
		return 1
	}
	zxfer_read_cleanup_pid_process_snapshot() {
		g_zxfer_cleanup_pid_process_snapshot_result=$(printf '%s\n%s\n' "204 $$ 700" "205 204 700")
		printf '%s\n' "$g_zxfer_cleanup_pid_process_snapshot_result"
	}
	zxfer_abort_direct_child_pid 204 TERM "unit cleanup helper" >/dev/null 2>&1
	l_signal_gone_status=$?

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Validated direct-child abort should treat blank pid inputs as already handled." \
		0 "$l_invalid_status"
	assertEquals "Validated direct-child abort should refuse to target the current shell pid." \
		1 "$l_self_status"
	assertEquals "Validated direct-child abort should fail closed when purpose normalization fails in the current shell." \
		1 "$l_normalize_status"
	assertEquals "Validated direct-child abort should treat already-exited helpers as already gone before any ownership checks." \
		0 "$l_gone_status"
	assertEquals "Validated direct-child abort should return success when an unowned helper exits before the ownership failure is reported." \
		0 "$l_orphan_gone_status"
	assertEquals "Validated direct-child abort should return success when signal delivery races with helper exit after the owned child set has been derived." \
		0 "$l_signal_gone_status"
}

test_zxfer_abort_cleanup_pid_rejects_missing_records_and_uses_process_group_teardown() {
	zxfer_test_capture_subshell '
		set +e
		fake_ps() {
			if [ "$1" = "-o" ] && [ "$2" = "pid=" ] && [ "$3" = "-o" ] &&
				[ "$4" = "ppid=" ] && [ "$5" = "-o" ] && [ "$6" = "pgid=" ]; then
				printf "%s\n" "301 1 900"
				return 0
			fi
			if [ "$1" = "-o" ] && [ "$2" = "pgid=" ] && [ "$3" = "-p" ] &&
				[ "$4" = "$$" ]; then
				printf "%s\n" "700"
				return 0
			fi
			return 1
		}

		g_zxfer_cleanup_pids="301"
		zxfer_abort_cleanup_pid 301 TERM
		printf "missing=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"

		g_cmd_ps=fake_ps
		g_zxfer_cleanup_pids="301"
		g_zxfer_cleanup_pid_records="301	unit cleanup helper	start-token	unit-host	900	process_group"
		zxfer_owned_lock_owner_is_live() {
			return 0
		}
		zxfer_signal_cleanup_process_group() {
			printf "process_group:%s:%s\n" "$1" "$2"
			return 0
		}
		zxfer_abort_cleanup_pid 301 TERM
		printf "process_group_status=%s\n" "$?"
		printf "remaining=<%s>\n" "$g_zxfer_cleanup_pids"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated cleanup abort should refuse teardown when a tracked pid has no matching validated ownership record." \
		"$output" "missing=1:Refusing to tear down cleanup helper [PID 301] because no validated ownership record was found."
	assertContains "Validated cleanup abort should prefer process-group teardown when the helper record requested it and the pgid remains isolated from the current shell." \
		"$output" "process_group:900:TERM"
	assertContains "Validated cleanup abort should return success after process-group teardown succeeds." \
		"$output" "process_group_status=0"
	assertContains "Validated cleanup abort should unregister helpers that were torn down successfully." \
		"$output" "remaining=<>"
}

test_zxfer_abort_cleanup_pid_current_shell_revalidation_paths_cover_remaining_branches() {
	l_saved_cmd_ps=${g_cmd_ps:-ps}
	l_tab=$(printf '\t')

	g_zxfer_cleanup_pids="301"
	g_zxfer_cleanup_pid_records="301${l_tab}unit cleanup helper${l_tab}start-token${l_tab}unit-host${l_tab}900${l_tab}process_group"
	zxfer_owned_lock_owner_is_live() {
		return 0
	}
	zxfer_read_cleanup_pid_process_snapshot() {
		return 29
	}
	zxfer_abort_cleanup_pid 301 TERM >/dev/null 2>&1
	l_snapshot_status=$?
	l_snapshot_message=$g_zxfer_cleanup_pid_abort_failure_message

	fake_ps() {
		if [ "$1" = "-o" ] && [ "$2" = "pgid=" ] && [ "$3" = "-p" ] && [ "$4" = "$$" ]; then
			printf '%s\n' "700"
			return 0
		fi
		return 1
	}
	g_cmd_ps=fake_ps
	g_zxfer_cleanup_pids="302"
	g_zxfer_cleanup_pid_records="302${l_tab}unit cleanup helper${l_tab}start-token${l_tab}unit-host${l_tab}bad${l_tab}child_set"
	zxfer_owned_lock_owner_is_live() {
		return 0
	}
	zxfer_read_cleanup_pid_process_snapshot() {
		g_zxfer_cleanup_pid_process_snapshot_result="302 1 700"
		printf '%s\n' "$g_zxfer_cleanup_pid_process_snapshot_result"
	}
	zxfer_get_cleanup_pid_set() {
		return 1
	}
	zxfer_abort_cleanup_pid 302 TERM >/dev/null 2>&1
	l_child_set_status=$?
	l_child_set_message=$g_zxfer_cleanup_pid_abort_failure_message

	l_recheck_calls=0
	g_zxfer_cleanup_pids="303"
	g_zxfer_cleanup_pid_records="303${l_tab}unit cleanup helper${l_tab}start-token${l_tab}unit-host${l_tab}bad${l_tab}child_set"
	zxfer_owned_lock_owner_is_live() {
		l_recheck_calls=$((l_recheck_calls + 1))
		if [ "$l_recheck_calls" -eq 1 ]; then
			return 0
		fi
		return 1
	}
	zxfer_read_cleanup_pid_process_snapshot() {
		g_zxfer_cleanup_pid_process_snapshot_result="303 1 700"
		printf '%s\n' "$g_zxfer_cleanup_pid_process_snapshot_result"
	}
	zxfer_get_cleanup_pid_set() {
		g_zxfer_cleanup_pid_set_result="303"
		return 0
	}
	zxfer_signal_cleanup_pid_set() {
		return 1
	}
	zxfer_abort_cleanup_pid 303 TERM >/dev/null 2>&1
	l_stale_signal_status=$?
	l_stale_signal_remaining=$g_zxfer_cleanup_pids

	l_recheck_calls=0
	g_zxfer_cleanup_pids="304"
	g_zxfer_cleanup_pid_records="304${l_tab}unit cleanup helper${l_tab}start-token${l_tab}unit-host${l_tab}bad${l_tab}child_set"
	zxfer_owned_lock_owner_is_live() {
		l_recheck_calls=$((l_recheck_calls + 1))
		if [ "$l_recheck_calls" -eq 1 ]; then
			return 0
		fi
		return 2
	}
	zxfer_read_cleanup_pid_process_snapshot() {
		g_zxfer_cleanup_pid_process_snapshot_result="304 1 700"
		printf '%s\n' "$g_zxfer_cleanup_pid_process_snapshot_result"
	}
	zxfer_get_cleanup_pid_set() {
		g_zxfer_cleanup_pid_set_result="304"
		return 0
	}
	zxfer_signal_cleanup_pid_set() {
		return 1
	}
	zxfer_abort_cleanup_pid 304 TERM >/dev/null 2>&1
	l_validation_status=$?
	l_validation_message=$g_zxfer_cleanup_pid_abort_failure_message

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp
	g_cmd_ps=$l_saved_cmd_ps

	assertEquals "Validated cleanup abort should fail closed when the process table cannot be inspected after ownership validation succeeds." \
		29 "$l_snapshot_status"
	assertEquals "Validated cleanup abort should preserve the process-table failure message in the current shell." \
		"Failed to inspect the process table for cleanup helper [unit cleanup helper] (PID 301)." "$l_snapshot_message"
	assertEquals "Validated cleanup abort should fail closed when child-set derivation fails for a still-live helper on the child-set path." \
		1 "$l_child_set_status"
	assertEquals "Validated cleanup abort should preserve the child-set derivation failure message on the child-set fallback path." \
		"Failed to derive the owned child set for cleanup helper [unit cleanup helper] (PID 302)." "$l_child_set_message"
	assertEquals "Validated cleanup abort should treat a signal-delivery failure as success when the helper exits before revalidation completes." \
		0 "$l_stale_signal_status"
	assertEquals "Validated cleanup abort should unregister helpers that exit before post-signal revalidation completes." \
		"" "$l_stale_signal_remaining"
	assertEquals "Validated cleanup abort should fail closed when post-signal ownership revalidation errors." \
		1 "$l_validation_status"
	assertEquals "Validated cleanup abort should preserve the ownership-validation failure message when post-signal revalidation errors." \
		"Failed to validate ownership for cleanup helper [unit cleanup helper] (PID 304)." "$l_validation_message"
}

test_zxfer_abort_cleanup_pid_handles_child_set_revalidation_and_signal_failures() {
	zxfer_test_capture_subshell '
		set +e
		zxfer_read_cleanup_pid_process_snapshot() {
			g_zxfer_cleanup_pid_process_snapshot_result="302 1 700"
			printf "%s\n" "$g_zxfer_cleanup_pid_process_snapshot_result"
		}
		zxfer_get_cleanup_pid_set() {
			if [ "${case_name:-}" = "signal_failure" ]; then
				g_zxfer_cleanup_pid_set_result=$(printf "%s\n%s\n" "304" "305")
				return 0
			fi
			return 1
		}
		zxfer_signal_cleanup_pid_set() {
			return 1
		}

		case_name=stale
		recheck_calls=0
		g_zxfer_cleanup_pids="302"
		g_zxfer_cleanup_pid_records="302	unit cleanup helper	start-token	unit-host	700	child_set"
		zxfer_owned_lock_owner_is_live() {
			recheck_calls=$((recheck_calls + 1))
			if [ "$recheck_calls" -eq 1 ]; then
				return 0
			fi
			return 1
		}
		zxfer_abort_cleanup_pid 302 TERM
		printf "stale=%s:<%s>\n" "$?" "$g_zxfer_cleanup_pids"

		case_name=recheck_error
		recheck_calls=0
		g_zxfer_cleanup_pids="303"
		g_zxfer_cleanup_pid_records="303	unit cleanup helper	start-token	unit-host	700	child_set"
		zxfer_owned_lock_owner_is_live() {
			recheck_calls=$((recheck_calls + 1))
			if [ "$recheck_calls" -eq 1 ]; then
				return 0
			fi
			return 2
		}
		zxfer_abort_cleanup_pid 303 TERM
		printf "recheck_error=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"

		case_name=signal_failure
		g_zxfer_cleanup_pids="304"
		g_zxfer_cleanup_pid_records="304	unit cleanup helper	start-token	unit-host	700	child_set"
		zxfer_owned_lock_owner_is_live() {
			return 0
		}
		zxfer_abort_cleanup_pid 304 TERM
		printf "signal_failure=%s:%s\n" "$?" "$g_zxfer_cleanup_pid_abort_failure_message"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated cleanup abort should treat a failed child-set derivation as a stale record when ownership revalidation says the helper has already exited." \
		"$output" "stale=0:<>"
	assertContains "Validated cleanup abort should fail closed when ownership revalidation errors while recovering from a child-set derivation failure." \
		"$output" "recheck_error=1:Failed to validate ownership for cleanup helper [unit cleanup helper] (PID 303)."
	assertContains "Validated cleanup abort should fail closed when signaling a derived child set fails and the helper still validates as live." \
		"$output" "signal_failure=1:Failed to signal the validated teardown target for cleanup helper [unit cleanup helper] (PID 304)."
}

test_zxfer_abort_cleanup_pid_fails_closed_when_child_set_revalidation_errors_immediately() {
	zxfer_test_capture_subshell '
		set +e
		zxfer_read_cleanup_pid_process_snapshot() {
			g_zxfer_cleanup_pid_process_snapshot_result="306 1 700"
			printf "%s\n" "$g_zxfer_cleanup_pid_process_snapshot_result"
		}
		zxfer_get_cleanup_pid_set() {
			return 1
		}
		g_zxfer_cleanup_pids="306"
		g_zxfer_cleanup_pid_records="306	unit cleanup helper	start-token	unit-host	700	child_set"
		zxfer_owned_lock_owner_is_live() {
			return 2
		}
		zxfer_abort_cleanup_pid 306 TERM
		printf "status=%s\n" "$?"
		printf "message=%s\n" "$g_zxfer_cleanup_pid_abort_failure_message"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated cleanup abort should fail closed when ownership revalidation errors immediately after child-set derivation fails." \
		"$output" "status=1"
	assertContains "Validated cleanup abort should preserve the ownership-validation failure message when revalidation errors immediately after child-set derivation fails." \
		"$output" "message=Failed to validate ownership for cleanup helper [unit cleanup helper] (PID 306)."
}

test_zxfer_kill_registered_cleanup_pids_preserves_first_failure_message_and_rebuilds_tracked_pids() {
	zxfer_test_capture_subshell '
		set +e
		g_zxfer_cleanup_pids="401 402"
		g_zxfer_cleanup_pid_records="401	first helper	start-one	host-a	700	child_set
402	second helper	start-two	host-a	700	child_set"
		zxfer_abort_cleanup_pid() {
			if [ "$1" = "401" ]; then
				g_zxfer_cleanup_pid_abort_failure_message="first cleanup abort failed"
				return 1
			fi
			return 0
		}

		zxfer_kill_registered_cleanup_pids
		printf "status=%s\n" "$?"
		printf "message=%s\n" "$g_zxfer_cleanup_pid_abort_failure_message"
		printf "remaining=<%s>\n" "$g_zxfer_cleanup_pids"
	'
	output=$ZXFER_TEST_CAPTURE_OUTPUT

	assertContains "Validated cleanup-helper shutdown should preserve the first abort failure status." \
		"$output" "status=1"
	assertContains "Validated cleanup-helper shutdown should preserve the first abort failure message." \
		"$output" "message=first cleanup abort failed"
	assertContains "Validated cleanup-helper shutdown should rebuild the tracked pid list from the remaining validated records after a failed aggregate pass." \
		"$output" "remaining=<401 402>"
}

test_runtime_init_default_helpers_cover_current_shell_paths() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}

			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress_safe="gzip"
			g_cmd_decompress_safe="gunzip"

			zxfer_init_runtime_metadata
			zxfer_init_option_defaults
			zxfer_init_transport_remote_defaults
			zxfer_init_runtime_state_defaults
			zxfer_init_temp_artifacts

			printf 'version=%s\n' "$g_zxfer_version"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'origin_caps=<%s>\n' "$g_origin_remote_capabilities_response"
			printf 'control_sockets=%s\n' "$g_ssh_supports_control_sockets"
			printf 'local_zfs=%s\n' "$g_LZFS"
			printf 'backup_root=%s\n' "$g_backup_storage_root"
			printf 'backup_ext=%s\n' "$g_backup_file_extension"
			printf 'delete_source=<%s>\n' "$g_delete_source_tmp_file"
			printf 'temp_prefix=%s\n' "$g_zxfer_temp_prefix"
		)
	)

	assertContains "Runtime metadata initialization should set the current zxfer version string." \
		"$output" "version=2.0.0-20260423"
	assertContains "Option default initialization should restore the single-job default." \
		"$output" "jobs=1"
	assertContains "Transport runtime defaults should clear cached remote capability payloads." \
		"$output" "origin_caps=<>"
	assertContains "Transport runtime defaults should publish the ssh control-socket support marker in current-shell state." \
		"$output" "control_sockets="
	assertContains "Transport runtime defaults should seed the local zfs helpers from the base zfs path." \
		"$output" "local_zfs=/sbin/zfs"
	assertContains "Runtime state defaults should restore the default backup metadata root." \
		"$output" "backup_root=/var/db/zxfer"
	assertContains "Runtime state defaults should restore the secure backup-file suffix." \
		"$output" "backup_ext=.zxfer_backup_info"
	assertContains "Temporary artifact initialization should leave delete-planning scratch paths unset until needed." \
		"$output" "delete_source=<>"
	assertContains "Temporary artifact initialization should publish the current run temp prefix." \
		"$output" "temp_prefix=zxfer."
}

test_zxfer_init_globals_applies_secure_path_after_reset_helpers() {
	output=$(
		(
			reset_replication_calls=0
			zxfer_refresh_secure_path_state() {
				printf '%s\n' "refresh"
			}
			zxfer_reset_replication_runtime_state() {
				reset_replication_calls=$((reset_replication_calls + 1))
			}
			zxfer_init_dependency_tool_defaults() {
				printf '%s\n' "deps"
			}
			zxfer_init_transport_remote_defaults() {
				printf '%s\n' "transport"
			}
			zxfer_init_temp_artifacts() {
				printf '%s\n' "temp"
			}
			zxfer_apply_secure_path() {
				g_zxfer_runtime_path="/secure/path"
				printf '%s\n' "apply"
			}
			zxfer_init_globals
			printf 'runtime=<%s>\n' "${g_zxfer_runtime_path:-}"
			printf 'replication_resets=%s\n' "$reset_replication_calls"
		)
	)

	assertContains "Global runtime initialization should refresh the secure-path state before rebuilding defaults." \
		"$output" "refresh"
	assertContains "Global runtime initialization should reset replication state through the public helper when it is available." \
		"$output" "replication_resets=1"
	assertContains "Global runtime initialization should still run the dependency, transport, and temp default helpers." \
		"$output" "deps"
	assertContains "Global runtime initialization should still reapply the secure runtime PATH after rebuilding defaults." \
		"$output" "apply"
	assertContains "Global runtime initialization should leave the secure runtime PATH published in current-shell state." \
		"$output" "runtime=</secure/path>"
}

test_runtime_execution_context_init_helpers_cover_local_and_dry_run_remote_paths() {
	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$1"
			}
			zxfer_get_os() {
				if [ -n "$1" ]; then
					printf '%s\n' "RemoteOS"
				else
					printf '%s\n' "LocalOS"
				fi
			}
			zxfer_assign_required_tool() {
				eval "$1='/usr/bin/$2'"
			}
			zxfer_quote_cli_tokens() {
				printf 'quoted<%s>\n' "$1"
			}

			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="local-compress"
			g_cmd_decompress_safe="local-decompress"
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			g_origin_cmd_zfs=""
			g_target_cmd_zfs=""
			g_cmd_cat=""

			zxfer_init_transfer_command_context
			printf 'transfer_origin=%s\n' "$g_origin_cmd_compress_safe"
			printf 'transfer_target=%s\n' "$g_target_cmd_decompress_safe"

			g_option_e_restore_property_mode=1
			g_option_O_origin_host=""
			zxfer_init_restore_property_helpers
			printf 'local_cat=%s\n' "$g_cmd_cat"

			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_option_n_dryrun=1
			g_option_z_compress=1
			g_cmd_cat=""
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			zxfer_init_source_execution_context
			zxfer_init_destination_execution_context
			zxfer_init_restore_property_helpers

			printf 'source_os=<%s>\n' "$g_source_operating_system"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
			printf 'origin_compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'dest_os=<%s>\n' "$g_destination_operating_system"
			printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
			printf 'target_decompress=%s\n' "$g_target_cmd_decompress_safe"
			printf 'remote_cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Transfer command context initialization should copy the local compression helper to the origin transport defaults." \
		"$output" "transfer_origin=local-compress"
	assertContains "Transfer command context initialization should copy the local decompression helper to the target transport defaults." \
		"$output" "transfer_target=local-decompress"
	assertContains "Restore-helper initialization should resolve the local cat helper when restore mode is enabled without an origin host." \
		"$output" "local_cat=/usr/bin/cat"
	assertContains "Dry-run remote source initialization should skip live OS probing and leave the cached source OS blank." \
		"$output" "source_os=<>"
	assertContains "Dry-run remote source initialization should still seed the origin zfs helper from the local zfs path." \
		"$output" "origin_zfs=/sbin/zfs"
	assertContains "Dry-run remote source initialization should quote the remote compression command when compression is enabled." \
		"$output" "origin_compress=quoted<zstd -3>"
	assertContains "Dry-run remote destination initialization should skip live OS probing and leave the cached destination OS blank." \
		"$output" "dest_os=<>"
	assertContains "Dry-run remote destination initialization should still seed the target zfs helper from the local zfs path." \
		"$output" "target_zfs=/sbin/zfs"
	assertContains "Dry-run remote destination initialization should quote the remote decompression command when compression is enabled." \
		"$output" "target_decompress=quoted<zstd -d>"
	assertContains "Dry-run remote restore-helper initialization should fall back to a literal cat helper." \
		"$output" "remote_cat=cat"
}

test_runtime_artifact_allocators_use_validated_temp_root_for_files_and_dirs() {
	zxfer_create_runtime_artifact_file "runtime-file" >/dev/null
	file_status=$?
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-dir" >/dev/null
	dir_status=$?
	dir_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "Runtime artifact file allocation should succeed under the validated temp root." \
		0 "$file_status"
	assertEquals "Runtime artifact directory allocation should succeed under the validated temp root." \
		0 "$dir_status"
	assertContains "Runtime artifact files should be allocated under the validated temp root." \
		"$file_path" "$TEST_TMPDIR/"
	assertContains "Runtime artifact directories should be allocated under the validated temp root." \
		"$dir_path" "$TEST_TMPDIR/"
	assertTrue "Runtime artifact file allocation should create the requested file." \
		"[ -f \"$file_path\" ]"
	assertTrue "Runtime artifact directory allocation should create the requested directory." \
		"[ -d \"$dir_path\" ]"
	assertContains "Runtime artifact allocation should register the created file for cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertContains "Runtime artifact allocation should register the created directory for cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
}

test_runtime_artifact_file_allocator_in_parent_uses_validated_parent_and_registers_path() {
	parent_dir="$TEST_TMPDIR/runtime-parent"
	mkdir -p "$parent_dir"

	zxfer_create_runtime_artifact_file_in_parent "$parent_dir" "runtime-parent-file" >/dev/null
	status=$?
	file_path=$g_zxfer_runtime_artifact_path_result

	assertEquals "Parent-scoped runtime artifact allocation should succeed for validated directories." \
		0 "$status"
	assertContains "Parent-scoped runtime artifact allocation should create files in the requested directory." \
		"$file_path" "$parent_dir/"
	assertTrue "Parent-scoped runtime artifact allocation should create the requested file." \
		"[ -f \"$file_path\" ]"
	assertContains "Parent-scoped runtime artifact allocation should register the file for cleanup." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
}

test_zxfer_reset_runtime_artifact_state_cleans_registered_artifacts() {
	zxfer_create_runtime_artifact_file "runtime-reset-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-reset-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result

	zxfer_reset_runtime_artifact_state

	assertFalse "Resetting runtime artifact state should remove registered runtime files." \
		"[ -e \"$file_path\" ]"
	assertFalse "Resetting runtime artifact state should remove registered runtime directories." \
		"[ -e \"$dir_path\" ]"
	assertEquals "Resetting runtime artifact state should clear the registered cleanup path list." \
		"" "$g_zxfer_runtime_artifact_cleanup_paths"
	assertEquals "Resetting runtime artifact state should clear the shared path scratch result." \
		"" "$g_zxfer_runtime_artifact_path_result"
	assertEquals "Resetting runtime artifact state should clear the shared readback scratch result." \
		"" "$g_zxfer_runtime_artifact_read_result"
}

test_zxfer_reset_runtime_artifact_state_preserves_failed_cleanup_registrations() {
	artifact_path="$TEST_TMPDIR/runtime-reset-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			g_zxfer_runtime_artifact_path_result="stale-path"
			g_zxfer_runtime_artifact_read_result="stale-read"
			rm() {
				return 1
			}
			zxfer_reset_runtime_artifact_state
			status=$?
			printf 'status=%s\n' "$status"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
			printf 'path_result=<%s>\n' "$g_zxfer_runtime_artifact_path_result"
			printf 'read_result=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)

	assertContains "Resetting runtime artifact state should preserve cleanup failures." \
		"$output" "status=1"
	assertContains "Resetting runtime artifact state should keep undeleted artifacts registered for later cleanup." \
		"$output" "registered=<$artifact_path>"
	assertContains "Resetting runtime artifact state should still clear the shared path scratch result after cleanup failures." \
		"$output" "path_result=<>"
	assertContains "Resetting runtime artifact state should still clear the shared readback scratch result after cleanup failures." \
		"$output" "read_result=<>"
	assertTrue "Resetting runtime artifact state should leave undeleted artifacts in place when cleanup fails." \
		"[ -e \"$artifact_path\" ]"
}

test_zxfer_trap_exit_cleans_registered_runtime_artifacts() {
	registered_file="$TEST_TMPDIR/registered-runtime-file"
	registered_dir="$TEST_TMPDIR/registered-runtime-dir"
	: >"$registered_file"
	mkdir -p "$registered_dir/subdir"
	: >"$registered_dir/subdir/payload"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_register_runtime_artifact_path "$registered_dir"
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success after removing registered runtime artifacts." \
		0 "$status"
	assertEquals "zxfer_trap_exit should keep stdout clean while removing registered runtime artifacts." \
		"" "$output"
	assertFalse "zxfer_trap_exit should remove registered runtime files." \
		"[ -e \"$registered_file\" ]"
	assertFalse "zxfer_trap_exit should remove registered runtime directories." \
		"[ -e \"$registered_dir\" ]"
}

test_zxfer_trap_exit_aborts_supervised_background_jobs_before_legacy_pid_cleanup() {
	cleanup_log="$TEST_TMPDIR/trap_supervisor_cleanup.log"
	: >"$cleanup_log"

	output=$(
		(
			CLEANUP_LOG="$cleanup_log"
			zxfer_abort_all_background_jobs() {
				printf '%s\n' "abort" >>"$CLEANUP_LOG"
			}
			zxfer_kill_registered_cleanup_pids() {
				printf '%s\n' "legacy" >>"$CLEANUP_LOG"
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when supervised background cleanup succeeds." \
		0 "$status"
	assertEquals "zxfer_trap_exit should run supervised background cleanup before legacy bare-PID cleanup." \
		"abort
legacy" "$(cat "$cleanup_log")"
	assertEquals "zxfer_trap_exit should keep stdout clean when cleanup succeeds." \
		"" "$output"
}

test_zxfer_trap_exit_fails_closed_when_supervised_background_cleanup_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_abort_all_background_jobs() {
				g_zxfer_background_job_abort_failure_message="validated abort failed"
				return 17
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve supervised background cleanup failure status." \
		17 "$status"
	assertContains "Supervised background cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Supervised background cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Supervised background cleanup failures should preserve the validated abort failure message." \
		"$output" "message=validated abort failed"
}

test_zxfer_trap_exit_fails_closed_when_validated_cleanup_helper_abort_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_kill_registered_cleanup_pids() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup helper abort failed"
				return 23
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve validated cleanup-helper teardown failure status." \
		23 "$status"
	assertContains "Validated cleanup-helper teardown failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Validated cleanup-helper teardown failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Validated cleanup-helper teardown failures should preserve the validated abort failure message." \
		"$output" "message=validated cleanup helper abort failed"
}

test_zxfer_trap_exit_releases_registered_owned_locks() {
	lock_dir="$TEST_TMPDIR/trap-owned.lock"
	zxfer_create_owned_lock_dir "$lock_dir" lock "trap-owned-lock" >/dev/null
	zxfer_register_owned_lock_path "$lock_dir"

	output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success after releasing registered owned locks." \
		0 "$status"
	assertEquals "zxfer_trap_exit should keep stderr clean when registered owned locks release cleanly." \
		"" "$output"
	assertFalse "zxfer_trap_exit should remove registered owned lock directories." \
		"[ -e \"$lock_dir\" ]"
}

test_zxfer_trap_exit_fails_closed_when_ssh_socket_cleanup_fails_after_success() {
	registered_file="$TEST_TMPDIR/trap-close-failure-artifact"
	: >"$registered_file"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_close_all_ssh_control_sockets() {
				printf '%s\n' "close failed" >&2
				return 19
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when ssh socket cleanup fails after an otherwise successful run." \
		19 "$status"
	assertContains "zxfer_trap_exit should preserve ssh socket cleanup diagnostics on stderr." \
		"$output" "close failed"
	assertContains "ssh socket cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "ssh socket cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "ssh socket cleanup failures should preserve the cleanup-specific failure message." \
		"$output" "message=Failed to close one or more ssh control sockets during exit."
	assertFalse "zxfer_trap_exit should continue removing registered runtime artifacts after ssh socket cleanup failures." \
		"[ -e \"$registered_file\" ]"
}

test_zxfer_trap_exit_preserves_failed_owned_lock_cleanup_paths_under_temp_prefix() {
	g_zxfer_temp_prefix="zxfer.trap-owned"
	cache_root=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR") ||
		fail "Unable to derive the shared remote-host cache root."
	lock_dir="$cache_root/repro.lock"
	current_hostname=$(zxfer_get_owned_lock_hostname) ||
		fail "Unable to derive the current hostname for the owned-lock fixture."
	created_at=$(zxfer_get_owned_lock_created_at) ||
		fail "Unable to derive the creation timestamp for the owned-lock fixture."

	mkdir -p "$lock_dir" || fail "Unable to create the owned-lock cleanup fixture."
	chmod 700 "$lock_dir" || fail "Unable to chmod the owned-lock cleanup fixture."
	{
		printf '%s\n' "$ZXFER_LOCK_METADATA_HEADER"
		printf 'kind\tlock\n'
		printf 'purpose\ttrap-owned-lock\n'
		printf 'pid\t%s\n' "$$"
		printf 'start_token\tlstart:not-the-current-process\n'
		printf 'hostname\t%s\n' "$current_hostname"
		printf 'created_at\t%s\n' "$created_at"
	} >"$lock_dir/metadata"
	chmod 600 "$lock_dir/metadata" || fail "Unable to chmod the owned-lock cleanup fixture metadata."
	zxfer_register_owned_lock_path "$lock_dir"

	output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve the original success status when a later owned-lock release only produces cleanup warnings." \
		0 "$status"
	assertContains "zxfer_trap_exit should warn when a registered owned lock cannot be released during cleanup." \
		"$output" "unable to release owned lock or lease"
	assertTrue "zxfer_trap_exit should preserve the failed owned lock directory for later inspection instead of deleting it through generic temp-prefix cleanup." \
		"[ -d \"$lock_dir\" ]"
	assertTrue "zxfer_trap_exit should preserve the enclosing remote-host cache root when it still contains a failed owned-lock cleanup path." \
		"[ -d \"$cache_root\" ]"
}

test_zxfer_cleanup_runtime_artifact_path_preserves_registration_when_delete_fails() {
	artifact_path="$TEST_TMPDIR/runtime-cleanup-failure"
	: >"$artifact_path"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$artifact_path"
			rm() {
				return 1
			}
			zxfer_cleanup_runtime_artifact_path "$artifact_path"
			status=$?
			printf 'status=%s\n' "$status"
			printf 'registered=<%s>\n' "$g_zxfer_runtime_artifact_cleanup_paths"
		)
	)

	assertContains "Runtime artifact cleanup should preserve failure when an artifact cannot be deleted." \
		"$output" "status=1"
	assertContains "Runtime artifact cleanup should keep undeleted artifacts registered for later cleanup." \
		"$output" "registered=<$artifact_path>"
	assertTrue "Runtime artifact cleanup failures should leave the undeleted artifact in place." \
		"[ -e \"$artifact_path\" ]"
}

test_zxfer_cleanup_runtime_artifact_paths_removes_and_unregisters_multiple_paths() {
	zxfer_create_runtime_artifact_file "runtime-cleanup-file" >/dev/null
	file_path=$g_zxfer_runtime_artifact_path_result
	zxfer_create_runtime_artifact_dir "runtime-cleanup-dir" >/dev/null
	dir_path=$g_zxfer_runtime_artifact_path_result

	zxfer_cleanup_runtime_artifact_paths "$file_path" "$dir_path"
	cleanup_status=$?

	assertEquals "Multi-path runtime artifact cleanup should succeed when every registered path can be deleted." \
		0 "$cleanup_status"
	assertFalse "Multi-path runtime artifact cleanup should remove registered files." \
		"[ -e \"$file_path\" ]"
	assertFalse "Multi-path runtime artifact cleanup should remove registered directories." \
		"[ -e \"$dir_path\" ]"
	assertNotContains "Multi-path runtime artifact cleanup should unregister deleted files." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$file_path"
	assertNotContains "Multi-path runtime artifact cleanup should unregister deleted directories." \
		"$g_zxfer_runtime_artifact_cleanup_paths" "$dir_path"
}

test_zxfer_cleanup_runtime_artifact_paths_preserves_failures_when_one_path_cannot_be_removed() {
	output_file="$TEST_TMPDIR/runtime_cleanup_paths_failure.out"

	(
		zxfer_cleanup_runtime_artifact_path() {
			case "$1" in
			fail-path) return 1 ;;
			esac
			command printf 'cleaned=%s\n' "$1"
			return 0
		}
		set +e
		zxfer_cleanup_runtime_artifact_paths "fail-path" "ok-path"
		status=$?
		set -e
		command printf 'status=%s\n' "$status"
	) >"$output_file"
	output=$(cat "$output_file")

	assertContains "Multi-path runtime artifact cleanup should still attempt later paths after an earlier failure." \
		"$output" "cleaned=ok-path"
	assertContains "Multi-path runtime artifact cleanup should return failure when any one path cannot be removed." \
		"$output" "status=1"
}

test_zxfer_write_and_read_runtime_artifact_file_preserve_multiline_payloads() {
	read_output_file="$TEST_TMPDIR/runtime-readback.out"
	zxfer_create_runtime_artifact_file "runtime-readback" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	payload=$(printf '%s\n' \
		"line one" \
		"line two")

	zxfer_write_runtime_artifact_file "$artifact_path" "$payload"
	write_status=$?
	zxfer_read_runtime_artifact_file "$artifact_path" >"$read_output_file"
	read_status=$?
	read_output=$(cat "$read_output_file")

	assertEquals "Runtime artifact writes should succeed for multiline payloads." \
		0 "$write_status"
	assertEquals "Runtime artifact reads should succeed for multiline payloads." \
		0 "$read_status"
	assertEquals "Runtime artifact reads should reproduce the exact multiline payload on stdout." \
		"$payload" "$read_output"
	assertEquals "Runtime artifact reads should publish the exact multiline payload in shared scratch state." \
		"$payload" "$g_zxfer_runtime_artifact_read_result"
}

test_zxfer_read_runtime_artifact_file_preserves_trailing_blank_lines_exactly() {
	read_output_file="$TEST_TMPDIR/runtime-readback-trailing.out"
	scratch_output_file="$TEST_TMPDIR/runtime-readback-trailing.scratch"
	expected_hex="6c696e65206f6e650a0a0a"
	zxfer_create_runtime_artifact_file "runtime-readback-trailing" >/dev/null
	artifact_path=$g_zxfer_runtime_artifact_path_result
	printf 'line one\n\n\n' >"$artifact_path"

	zxfer_read_runtime_artifact_file "$artifact_path" >"$read_output_file"
	read_status=$?
	printf '%s' "$g_zxfer_runtime_artifact_read_result" >"$scratch_output_file"
	read_output_hex=$(od -An -tx1 -v "$read_output_file" | tr -d ' \n')
	scratch_output_hex=$(od -An -tx1 -v "$scratch_output_file" | tr -d ' \n')

	assertEquals "Runtime artifact reads should preserve trailing blank lines on stdout." \
		0 "$read_status"
	assertEquals "Runtime artifact reads should preserve trailing blank lines in stdout payloads." \
		"$expected_hex" "$read_output_hex"
	assertEquals "Runtime artifact reads should preserve trailing blank lines in shared scratch state." \
		"$expected_hex" "$scratch_output_hex"
}

test_zxfer_read_runtime_artifact_file_preserves_nonzero_status_and_clears_scratch() {
	artifact_path="$TEST_TMPDIR/runtime-readback-failure"
	: >"$artifact_path"

	output=$(
		(
			g_zxfer_runtime_artifact_read_result="stale-runtime-readback"
			cat() {
				return 26
			}
			zxfer_read_runtime_artifact_file "$artifact_path" >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'scratch=<%s>\n' "$g_zxfer_runtime_artifact_read_result"
		)
	)

	assertContains "Runtime artifact readback failures should preserve the original nonzero status." \
		"$output" "status=26"
	assertContains "Runtime artifact readback failures should clear the shared readback scratch state." \
		"$output" "scratch=<>"
}

test_zxfer_write_runtime_artifact_file_creates_empty_files_without_caller_truncation() {
	artifact_path="$TEST_TMPDIR/runtime-empty-payload"

	zxfer_write_runtime_artifact_file "$artifact_path" ""
	write_status=$?

	assertEquals "Runtime artifact writes should succeed when asked to create an empty file." \
		0 "$write_status"
	assertTrue "Runtime artifact writes should create the destination file for empty payloads." \
		"[ -f \"$artifact_path\" ]"
	assertTrue "Runtime artifact writes should leave empty payload files at zero bytes." \
		"[ ! -s \"$artifact_path\" ]"
}

test_zxfer_write_runtime_artifact_file_suppresses_shell_redirection_stderr() {
	artifact_path="$TEST_TMPDIR/runtime-missing-parent/payload"

	output=$(
		(
			zxfer_write_runtime_artifact_file "$artifact_path" "payload"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertEquals "Runtime artifact write failures should stay silent so callers control the operator-facing error." \
		"status=1" "$output"
}

test_runtime_artifact_parent_and_stage_helpers_reject_invalid_parent_contexts() {
	set +e
	zxfer_create_runtime_artifact_file_in_parent "relative-parent" "runtime-parent-file" >/dev/null 2>&1
	parent_status=$?
	stage_output=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			zxfer_stage_runtime_artifact_file_for_path "$TEST_TMPDIR/runtime-target" >/dev/null 2>&1
			printf 'stage_status=%s\n' "$?"
		)
	)
	set -e

	assertEquals "Runtime artifact files staged in explicit parents should reject unvalidated parent directories." \
		1 "$parent_status"
	assertContains "Runtime artifact staging should preserve parent-directory lookup failures." \
		"$stage_output" "stage_status=1"
}

test_zxfer_write_runtime_artifact_file_preserves_non_redirection_failure_status() {
	artifact_path="$TEST_TMPDIR/runtime-nonredirection-failure"

	output=$(
		(
			printf() {
				return 7
			}
			set +e
			zxfer_write_runtime_artifact_file "$artifact_path" "payload"
			status=$?
			set -e
			command printf 'status=%s\n' "$status"
		)
	)

	assertContains "Runtime artifact writes should preserve non-redirection shell failures from the payload writer." \
		"$output" "status=7"
}

test_zxfer_write_runtime_cache_file_atomically_cleans_up_on_write_and_publish_failures() {
	stage_root="$TEST_TMPDIR/runtime-cache-stage-cleanup"
	write_target="$stage_root/write-failure.entry"
	publish_target="$stage_root/publish-failure.entry"
	mkdir -p "$stage_root" || fail "Unable to create runtime cache stage root."

	set +e
	(
		zxfer_write_runtime_artifact_file() {
			return 1
		}
		zxfer_write_runtime_cache_file_atomically \
			"$write_target" "payload" "zxfer-runtime-cache-test"
	)
	write_status=$?
	set -- "$stage_root"/.zxfer-runtime-cache-test.*
	if [ -e "$1" ]; then
		write_stage_count=$#
	else
		write_stage_count=0
	fi

	set +e
	(
		zxfer_publish_runtime_artifact_file() {
			return 1
		}
		zxfer_write_runtime_cache_file_atomically \
			"$publish_target" "payload" "zxfer-runtime-cache-test"
	)
	publish_status=$?
	set -- "$stage_root"/.zxfer-runtime-cache-test.*
	if [ -e "$1" ]; then
		publish_stage_count=$#
	else
		publish_stage_count=0
	fi

	assertEquals "Atomic runtime cache writes should fail closed when the staged payload cannot be written." \
		1 "$write_status"
	assertFalse "Failed runtime cache writes should not leave a published cache target behind." \
		"[ -e \"$write_target\" ]"
	assertEquals "Failed runtime cache writes should clean up their staged artifact files." \
		0 "$write_stage_count"
	assertEquals "Atomic runtime cache writes should fail closed when the staged payload cannot be published." \
		1 "$publish_status"
	assertFalse "Failed runtime cache publishes should not leave a published cache target behind." \
		"[ -e \"$publish_target\" ]"
	assertEquals "Failed runtime cache publishes should clean up their staged artifact files." \
		0 "$publish_stage_count"
}

test_zxfer_write_runtime_cache_and_cache_object_helpers_cover_success_and_redirection_failures() {
	cache_target="$TEST_TMPDIR/runtime-cache-success.entry"
	object_path="$TEST_TMPDIR/cache-object-open-failure"
	object_target_dir="$TEST_TMPDIR/cache-object-target-dir"
	stage_dir="$TEST_TMPDIR/cache-object-publish-stage"
	missing_parent_target="$TEST_TMPDIR/missing-cache-parent/object.dir"
	published_target="$TEST_TMPDIR/runtime-cache-object.dir"
	mkdir -p "$object_target_dir" "$stage_dir" || fail "Unable to create the runtime helper fixture directories."
	ln -s "$object_target_dir" "$object_path" || fail "Unable to create the cache-object redirection failure fixture."

	zxfer_write_runtime_cache_file_atomically "$cache_target" "payload" "zxfer-runtime-cache-test"
	cache_status=$?
	cache_mode=$(zxfer_get_path_mode_octal "$cache_target")

	set +e
	zxfer_write_cache_object_contents_to_path "$object_path" "demo-kind" "" "payload" >/dev/null 2>&1
	object_write_status=$?
	if [ -e "$object_path" ] && [ ! -L "$object_path" ]; then
		object_partial_exists=yes
	else
		object_partial_exists=no
	fi
	zxfer_publish_cache_object_directory "$stage_dir" "$missing_parent_target" >/dev/null 2>&1
	missing_parent_status=$?
	publish_move_output=$(
		(
			set +e
			mv() {
				return 1
			}
			zxfer_publish_cache_object_directory "$stage_dir" "$published_target" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	publish_move_status=$(printf '%s\n' "$publish_move_output" | awk -F= '/^status=/{print $2; exit}')
	set -e

	assertEquals "Atomic runtime cache writes should succeed on the direct helper success path." \
		0 "$cache_status"
	assertEquals "Successful atomic runtime cache writes should leave the published target mode at 0600." \
		600 "$cache_mode"
	assertEquals "Cache-object content writes should fail closed when the destination path cannot be opened for writing." \
		1 "$object_write_status"
	assertEquals "Failed cache-object content writes should not leave a partially published target behind." \
		no "$object_partial_exists"
	assertEquals "Publishing cache-object directories should fail closed when the target parent directory is missing." \
		1 "$missing_parent_status"
	assertEquals "Publishing cache-object directories should preserve move failures from the live publish step." \
		1 "$publish_move_status"
}

test_zxfer_write_runtime_cache_file_atomically_requires_existing_parent_dir() {
	missing_parent="$TEST_TMPDIR/runtime-cache-missing-parent"
	target_path="$missing_parent/cache.entry"

	set +e
	zxfer_write_runtime_cache_file_atomically "$target_path" "payload" "zxfer-runtime-cache-test"
	status=$?

	assertEquals "Atomic runtime cache writes should fail closed when the target parent directory is missing." \
		1 "$status"
	assertFalse "Atomic runtime cache writes should not create a missing parent directory implicitly." \
		"[ -d \"$missing_parent\" ]"
	assertFalse "Atomic runtime cache writes should not leave a published cache target behind when the parent is missing." \
		"[ -e \"$target_path\" ]"
}

test_zxfer_write_runtime_cache_file_atomically_rejects_non_file_targets_and_parent_lookup_failures() {
	dir_target="$TEST_TMPDIR/runtime-cache-dir-target"
	parent_lookup_target="$TEST_TMPDIR/runtime-cache-parent-lookup.entry"
	stage_failure_target="$TEST_TMPDIR/runtime-cache-stage-failure.entry"
	mkdir -p "$dir_target" || fail "Unable to create runtime cache directory target fixture."

	set +e
	zxfer_write_runtime_cache_file_atomically "$dir_target" "payload" "zxfer-runtime-cache-test" >/dev/null 2>&1
	dir_status=$?
	parent_lookup_output=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			zxfer_write_runtime_cache_file_atomically \
				"$parent_lookup_target" "payload" "zxfer-runtime-cache-test" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	stage_failure_output=$(
		(
			zxfer_stage_runtime_artifact_file_for_path() {
				return 1
			}
			zxfer_write_runtime_cache_file_atomically \
				"$stage_failure_target" "payload" "zxfer-runtime-cache-test" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertEquals "Atomic runtime cache writes should fail closed when the existing target path is not a regular file." \
		1 "$dir_status"
	assertContains "Atomic runtime cache writes should preserve target-parent lookup failures exactly." \
		"$parent_lookup_output" "status=1"
	assertContains "Atomic runtime cache writes should fail closed when the staging helper cannot allocate the private artifact path." \
		"$stage_failure_output" "status=1"
}

test_get_os_handles_local_and_remote_invocations() {
	assertEquals "A local zxfer_get_os call should match uname." \
		"$(uname)" "$(zxfer_get_os "")"

	remote_os=$(
		zxfer_get_remote_host_operating_system() {
			printf '%s\n' "RemoteOS"
		}
		zxfer_get_os "origin.example" source
	)

	assertEquals "A remote zxfer_get_os call should delegate to the remote helper." \
		"RemoteOS" "$remote_os"
}

test_init_globals_initializes_dependency_state_and_temp_files() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			g_zxfer_services_to_restart="stale-service"
			g_backup_file_contents="stale-backup"
			g_restored_backup_file_contents="stale-restore"
			g_zxfer_remote_capability_response_result="stale-caps"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_ssh_control_socket_action_result="stale-action"
			g_zxfer_ssh_control_socket_action_stderr="stale-stderr"
			g_recursive_source_list="stale-source"
			g_last_common_snap="stale@snap"
			g_zfs_send_job_pids="123 456"
			g_zxfer_background_job_records="stale-job	kind	111	/tmp/bg	/runner	token"
			g_zxfer_background_job_wait_job_id="stale-job"
			g_zxfer_property_cache_path="/tmp/stale-cache"
			g_zxfer_source_pvs_raw="stale=property=local"
			g_zxfer_property_stage_file_read_result="stale-stage-read"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_init_globals
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'path=%s\n' "$PATH"
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'ps=%s\n' "$g_cmd_ps"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'tmp_source=%s\n' "$g_delete_source_tmp_file"
			printf 'tmp_dest=%s\n' "$g_delete_dest_tmp_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'backup=<%s>\n' "$g_backup_file_contents"
			printf 'restored=<%s>\n' "$g_restored_backup_file_contents"
			printf 'remote_caps=<%s>\n' "$g_zxfer_remote_capability_response_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'socket_action=<%s>\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'socket_stderr=<%s>\n' "$g_zxfer_ssh_control_socket_action_stderr"
			printf 'recursive=<%s>\n' "$g_recursive_source_list"
			printf 'last_common=<%s>\n' "$g_last_common_snap"
			printf 'send_pids=<%s>\n' "$g_zfs_send_job_pids"
			printf 'background_records=<%s>\n' "$g_zxfer_background_job_records"
			printf 'background_wait_job=<%s>\n' "$g_zxfer_background_job_wait_job_id"
			printf 'cache_path=<%s>\n' "$g_zxfer_property_cache_path"
			printf 'source_pvs=<%s>\n' "$g_zxfer_source_pvs_raw"
			printf 'property_stage_read=<%s>\n' "$g_zxfer_property_stage_file_read_result"
		)
	)

	assertContains "zxfer_init_globals should initialize the secure path." \
		"$output" "secure=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should export the strict runtime PATH once runtime startup begins." \
		"$output" "path=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "zxfer_init_globals should resolve the awk helper." \
		"$output" "awk=/usr/bin/awk"
	assertContains "zxfer_init_globals should resolve the ps helper for supervised background-job validation." \
		"$output" "ps=/usr/bin/ps"
	assertContains "zxfer_init_globals should record ssh control-socket support." \
		"$output" "control=1"
	assertContains "zxfer_init_globals should leave snapshot-delete temp paths empty until delete planning needs them." \
		"$output" "tmp_source="
	assertContains "zxfer_init_globals should leave the paired snapshot-delete temp path empty until delete planning needs it." \
		"$output" "tmp_dest="
	assertContains "zxfer_init_globals should reset orchestration restart scratch state." \
		"$output" "restart=<>"
	assertContains "zxfer_init_globals should reset backup-metadata accumulation state." \
		"$output" "backup=<>"
	assertContains "zxfer_init_globals should reset restored backup scratch state." \
		"$output" "restored=<>"
	assertContains "zxfer_init_globals should reset remote capability handshake scratch state." \
		"$output" "remote_caps=<>"
	assertContains "zxfer_init_globals should reset remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "zxfer_init_globals should reset ssh control-socket action classification state." \
		"$output" "socket_action=<>"
	assertContains "zxfer_init_globals should reset ssh control-socket action stderr scratch state." \
		"$output" "socket_stderr=<>"
	assertContains "zxfer_init_globals should reset snapshot-discovery scratch state." \
		"$output" "recursive=<>"
	assertContains "zxfer_init_globals should reset snapshot-reconcile scratch state." \
		"$output" "last_common=<>"
	assertContains "zxfer_init_globals should reset send/receive PID tracking state." \
		"$output" "send_pids=<>"
	assertContains "zxfer_init_globals should reset supervised background-job registry state." \
		"$output" "background_records=<>"
	assertContains "zxfer_init_globals should reset supervised background-job wait scratch state." \
		"$output" "background_wait_job=<>"
	assertContains "zxfer_init_globals should reset property-cache path scratch state." \
		"$output" "cache_path=<>"
	assertContains "zxfer_init_globals should reset property-reconcile source scratch state." \
		"$output" "source_pvs=<>"
	assertContains "zxfer_init_globals should reset staged property-file read scratch state." \
		"$output" "property_stage_read=<>"
}

test_init_globals_defers_strict_path_export_until_startup_helpers_finish() {
	secure_path_dir="$TEST_TMPDIR/narrow-secure-path"
	mkdir -p "$secure_path_dir"

	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_SECURE_PATH="$secure_path_dir"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_init_globals
			status=$?
			printf 'status=%s\n' "$status"
			printf 'path=%s\n' "$PATH"
			printf 'tmp_source=%s\n' "$g_delete_source_tmp_file"
		) 2>&1
	)

	assertContains "zxfer_init_globals should still finish startup when ZXFER_SECURE_PATH omits date/mktemp directories." \
		"$output" "status=0"
	assertContains "zxfer_init_globals should export the narrow secure PATH after startup completes." \
		"$output" "path=$secure_path_dir"
	assertContains "zxfer_init_globals should still finish startup before switching to the strict runtime PATH even when delete tempfiles are deferred." \
		"$output" "tmp_source="
	assertNotContains "Startup should not trip over missing bootstrap utilities when the strict PATH is applied at the end of init." \
		"$output" "command not found"
}

test_ensure_snapshot_delete_temp_artifacts_allocates_paths_lazily_in_current_shell() {
	output=$(
		(
			counter=0
			zxfer_reset_delete_temp_artifacts
			zxfer_get_temp_file() {
				counter=$((counter + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/delete.$counter"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$TEST_TMPDIR/stdout-only.$counter"
			}

			zxfer_ensure_snapshot_delete_temp_artifacts
			first_source=$g_delete_source_tmp_file
			first_dest=$g_delete_dest_tmp_file
			first_diff=$g_delete_snapshots_to_delete_tmp_file

			zxfer_ensure_snapshot_delete_temp_artifacts

			printf 'source=%s\n' "$g_delete_source_tmp_file"
			printf 'dest=%s\n' "$g_delete_dest_tmp_file"
			printf 'diff=%s\n' "$g_delete_snapshots_to_delete_tmp_file"
			printf 'reused=%s\n' \
				"$([ "$first_source" = "$g_delete_source_tmp_file" ] &&
					[ "$first_dest" = "$g_delete_dest_tmp_file" ] &&
					[ "$first_diff" = "$g_delete_snapshots_to_delete_tmp_file" ] &&
					printf yes || printf no)"
			printf 'count=%s\n' "$counter"
		)
	)

	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the source path." \
		"$output" "source=$TEST_TMPDIR/delete.1"
	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the destination path." \
		"$output" "dest=$TEST_TMPDIR/delete.2"
	assertContains "Lazy snapshot-delete tempfile setup should use the current-shell scratch result for the diff path." \
		"$output" "diff=$TEST_TMPDIR/delete.3"
	assertContains "Lazy snapshot-delete tempfile setup should reuse already-assigned paths on later calls." \
		"$output" "reused=yes"
	assertContains "Lazy snapshot-delete tempfile setup should allocate exactly once per required path." \
		"$output" "count=3"
}

test_ensure_snapshot_delete_temp_artifacts_preserves_allocation_failures_without_publishing_paths() {
	output=$(
		(
			zxfer_reset_delete_temp_artifacts
			zxfer_get_temp_file() {
				return 71
			}

			set +e
			zxfer_ensure_snapshot_delete_temp_artifacts
			status=$?
			set -e

			printf 'status=%s\n' "$status"
			printf 'source=<%s>\n' "${g_delete_source_tmp_file:-}"
			printf 'dest=<%s>\n' "${g_delete_dest_tmp_file:-}"
			printf 'diff=<%s>\n' "${g_delete_snapshots_to_delete_tmp_file:-}"
		)
	)

	assertContains "Lazy snapshot-delete tempfile setup should preserve the first allocation failure status." \
		"$output" "status=71"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a source temp path when allocation fails." \
		"$output" "source=<>"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a destination temp path when allocation fails." \
		"$output" "dest=<>"
	assertContains "Lazy snapshot-delete tempfile setup should not publish a diff temp path when allocation fails." \
		"$output" "diff=<>"
}

test_ensure_snapshot_delete_temp_artifacts_cleans_up_after_second_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/delete_temp_cleanup_second.log"
	zxfer_reset_delete_temp_artifacts
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-second-source"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			return 71
			;;
		esac
		return 72
	}
	zxfer_cleanup_runtime_artifact_paths() {
		printf '%s\n' "$*" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_ensure_snapshot_delete_temp_artifacts >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	source_path=${g_delete_source_tmp_file:-}
	dest_path=${g_delete_dest_tmp_file:-}
	diff_path=${g_delete_snapshots_to_delete_tmp_file:-}

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell delete-temp setup should preserve the second allocation failure status." \
		71 "$status"
	assertEquals "Current-shell delete-temp setup should clean up the already allocated source tempfile when the second allocation fails." \
		"$TEST_TMPDIR/delete-second-source" "$cleanup_paths"
	assertEquals "Current-shell delete-temp setup should not publish the source tempfile after the second allocation fails." \
		"" "$source_path"
	assertEquals "Current-shell delete-temp setup should not publish the destination tempfile after the second allocation fails." \
		"" "$dest_path"
	assertEquals "Current-shell delete-temp setup should not publish the diff tempfile after the second allocation fails." \
		"" "$diff_path"
}

test_ensure_snapshot_delete_temp_artifacts_cleans_up_after_third_allocation_failure_in_current_shell() {
	cleanup_log="$TEST_TMPDIR/delete_temp_cleanup_third.log"
	zxfer_reset_delete_temp_artifacts
	call_count=0

	zxfer_get_temp_file() {
		call_count=$((call_count + 1))
		case "$call_count" in
		1)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-third-source"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			g_zxfer_temp_file_result="$TEST_TMPDIR/delete-third-dest"
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		3)
			return 72
			;;
		esac
		return 73
	}
	zxfer_cleanup_runtime_artifact_paths() {
		printf '%s\n' "$*" >"$cleanup_log"
		return 0
	}

	set +e
	zxfer_ensure_snapshot_delete_temp_artifacts >/dev/null 2>&1
	status=$?
	set -e
	cleanup_paths=$(cat "$cleanup_log" 2>/dev/null || :)
	source_path=${g_delete_source_tmp_file:-}
	dest_path=${g_delete_dest_tmp_file:-}
	diff_path=${g_delete_snapshots_to_delete_tmp_file:-}

	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	setUp

	assertEquals "Current-shell delete-temp setup should preserve the third allocation failure status." \
		72 "$status"
	assertEquals "Current-shell delete-temp setup should clean up both already allocated tempfiles when the third allocation fails." \
		"$TEST_TMPDIR/delete-third-source $TEST_TMPDIR/delete-third-dest" "$cleanup_paths"
	assertEquals "Current-shell delete-temp setup should not publish the source tempfile after the third allocation fails." \
		"" "$source_path"
	assertEquals "Current-shell delete-temp setup should not publish the destination tempfile after the third allocation fails." \
		"" "$dest_path"
	assertEquals "Current-shell delete-temp setup should not publish the diff tempfile after the third allocation fails." \
		"" "$diff_path"
}

test_init_globals_calls_owner_reset_helpers() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			reset_log="$TEST_TMPDIR/init_globals_resets.log"
			: >"$reset_log"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_reset_replication_runtime_state() {
				printf 'replication\n' >>"$reset_log"
			}
			zxfer_reset_send_receive_state() {
				printf 'send_receive\n' >>"$reset_log"
			}
			zxfer_reset_background_job_state() {
				printf 'background_jobs\n' >>"$reset_log"
			}
			zxfer_reset_destination_existence_cache() {
				printf 'destination_cache\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_record_indexes() {
				printf 'snapshot_indexes\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_discovery_state() {
				printf 'snapshot_discovery\n' >>"$reset_log"
			}
			zxfer_reset_snapshot_reconcile_state() {
				printf 'snapshot_reconcile\n' >>"$reset_log"
			}
			zxfer_reset_backup_metadata_state() {
				printf 'backup_metadata\n' >>"$reset_log"
			}
			zxfer_reset_property_runtime_state() {
				printf 'property_runtime\n' >>"$reset_log"
			}
			zxfer_reset_property_iteration_caches() {
				printf 'property_cache\n' >>"$reset_log"
			}
			zxfer_reset_property_reconcile_state() {
				printf 'property_reconcile\n' >>"$reset_log"
			}

			zxfer_init_globals
			cat "$reset_log"
		)
	)

	assertContains "zxfer_init_globals should delegate replication scratch reset to the replication owner helper." \
		"$output" "replication"
	assertContains "zxfer_init_globals should delegate send/receive scratch reset to the send/receive owner helper." \
		"$output" "send_receive"
	assertContains "zxfer_init_globals should delegate supervised background-job scratch reset to the background-job owner helper." \
		"$output" "background_jobs"
	assertContains "zxfer_init_globals should delegate destination cache reset to the snapshot-state owner helper." \
		"$output" "destination_cache"
	assertContains "zxfer_init_globals should delegate snapshot index reset to the snapshot-state owner helper." \
		"$output" "snapshot_indexes"
	assertContains "zxfer_init_globals should delegate snapshot discovery reset to the snapshot-discovery owner helper." \
		"$output" "snapshot_discovery"
	assertContains "zxfer_init_globals should delegate snapshot reconcile reset to the snapshot-reconcile owner helper." \
		"$output" "snapshot_reconcile"
	assertContains "zxfer_init_globals should delegate backup metadata reset to the backup owner helper." \
		"$output" "backup_metadata"
	assertContains "zxfer_init_globals should delegate run-wide property state reset to the property owner helper." \
		"$output" "property_runtime"
	assertContains "zxfer_init_globals should delegate property-cache reset to the property-cache owner helper." \
		"$output" "property_cache"
	assertContains "zxfer_init_globals should delegate per-call property reconcile reset to the property owner helper." \
		"$output" "property_reconcile"
}

test_init_globals_reinitializes_property_module_scratch_state_when_reinvoked() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			zxfer_assign_required_tool() {
				eval "$1=/usr/bin/$2"
			}
			zxfer_validate_resolved_tool_path() {
				printf '%s\n' "$1"
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}

			zxfer_init_globals

			stale_cache_dir="$TEST_TMPDIR/stale-property-cache"
			mkdir -p "$stale_cache_dir/normalized/source"
			: >"$stale_cache_dir/normalized/source/entry"
			g_zxfer_property_cache_dir=$stale_cache_dir
			g_zxfer_required_properties_result="stale-required"
			g_zxfer_property_cache_key="stale-key"
			g_zxfer_adjusted_set_list="compression=lz4"
			g_zxfer_adjusted_inherit_list="mountpoint"
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result="compression=lz4=local"
			g_zxfer_property_stage_file_read_result="stale-stage-read"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_destination_property_tree_prefetch_state=2
			g_unsupported_properties="compression"
			g_zxfer_unsupported_filesystem_properties="compression"
			g_zxfer_unsupported_volume_properties="volblocksize"

			zxfer_init_globals

			printf 'required=<%s>\n' "$g_zxfer_required_properties_result"
			printf 'cache_key=<%s>\n' "$g_zxfer_property_cache_key"
			printf 'adjusted_set=<%s>\n' "$g_zxfer_adjusted_set_list"
			printf 'adjusted_inherit=<%s>\n' "$g_zxfer_adjusted_inherit_list"
			printf 'override_result=<%s>\n' "$g_zxfer_override_pvs_result"
			printf 'creation_result=<%s>\n' "$g_zxfer_creation_pvs_result"
			printf 'property_stage_read=<%s>\n' "$g_zxfer_property_stage_file_read_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'cache_dir=<%s>\n' "$g_zxfer_property_cache_dir"
			printf 'prefetch_state=%s\n' "$g_zxfer_destination_property_tree_prefetch_state"
			printf 'unsupported=<%s>\n' "$g_unsupported_properties"
			printf 'unsupported_fs=<%s>\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'unsupported_vol=<%s>\n' "$g_zxfer_unsupported_volume_properties"
			if [ -d "$stale_cache_dir" ]; then
				printf 'stale_dir_exists=1\n'
			else
				printf 'stale_dir_exists=0\n'
			fi
		)
	)

	assertContains "Re-running zxfer_init_globals should clear required-property scratch results." \
		"$output" "required=<>"
	assertContains "Re-running zxfer_init_globals should clear property-cache key scratch state." \
		"$output" "cache_key=<>"
	assertContains "Re-running zxfer_init_globals should clear adjusted set scratch state." \
		"$output" "adjusted_set=<>"
	assertContains "Re-running zxfer_init_globals should clear adjusted inherit scratch state." \
		"$output" "adjusted_inherit=<>"
	assertContains "Re-running zxfer_init_globals should clear derived override scratch state." \
		"$output" "override_result=<>"
	assertContains "Re-running zxfer_init_globals should clear derived creation-property scratch state." \
		"$output" "creation_result=<>"
	assertContains "Re-running zxfer_init_globals should clear staged property-file read scratch state." \
		"$output" "property_stage_read=<>"
	assertContains "Re-running zxfer_init_globals should clear remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "Re-running zxfer_init_globals should reset the cache directory pointer." \
		"$output" "cache_dir=<>"
	assertContains "Re-running zxfer_init_globals should rearm destination property prefetch state." \
		"$output" "prefetch_state=0"
	assertContains "Re-running zxfer_init_globals should clear run-wide unsupported-property scratch state." \
		"$output" "unsupported=<>"
	assertContains "Re-running zxfer_init_globals should clear filesystem unsupported-property cache state." \
		"$output" "unsupported_fs=<>"
	assertContains "Re-running zxfer_init_globals should clear volume unsupported-property cache state." \
		"$output" "unsupported_vol=<>"
	assertContains "Re-running zxfer_init_globals should remove stale property cache directories." \
		"$output" "stale_dir_exists=0"
}

test_zxfer_cache_object_file_round_trip_preserves_metadata_and_payload() {
	object_path="$TEST_TMPDIR/cache-object-round-trip.entry"
	output_file="$TEST_TMPDIR/cache-object-round-trip.out"
	metadata=$(printf '%s\n' \
		"created_epoch=123" \
		"side=source")
	payload=$(printf '%s\n' \
		"line one" \
		"line two")

	zxfer_write_cache_object_file_atomically \
		"$object_path" "demo-kind" "$metadata" "$payload" >/dev/null
	write_status=$?
	zxfer_read_cache_object_file "$object_path" "demo-kind" >"$output_file"
	read_status=$?

	assertEquals "Atomic cache-object writes should publish a readable cache object." \
		0 "$write_status"
	assertEquals "Cache-object reads should succeed for valid published objects." \
		0 "$read_status"
	assertEquals "Valid cache-object reads should reproduce the original payload on stdout." \
		"$payload" "$(cat "$output_file")"
	assertEquals "Valid cache-object reads should publish the parsed object kind in shared scratch state." \
		"demo-kind" "$g_zxfer_cache_object_kind_result"
	assertEquals "Valid cache-object reads should preserve metadata lines in shared scratch state." \
		"$metadata" "$g_zxfer_cache_object_metadata_result"
	assertEquals "Valid cache-object reads should preserve the full payload in shared scratch state." \
		"$payload" "$g_zxfer_cache_object_payload_result"
}

test_cache_object_metadata_helpers_cover_invalid_lines_missing_keys_and_max_yield_constant() {
	set +e
	zxfer_validate_cache_object_metadata_lines "broken-metadata-line" >/dev/null 2>&1
	metadata_status=$?
	zxfer_get_cache_object_metadata_value "kind=demo" "missing" >/dev/null 2>&1
	missing_key_status=$?
	set -e
	max_yield=$(zxfer_get_max_yield_iterations)

	assertEquals "Cache-object metadata validation should fail closed on lines without key separators." \
		1 "$metadata_status"
	assertEquals "Cache-object metadata lookup should fail when the requested key is absent." \
		1 "$missing_key_status"
	assertEquals "Runtime max-yield helpers should return the exported runtime constant." \
		"$ZXFER_MAX_YIELD_ITERATIONS" "$max_yield"
}

test_zxfer_read_cache_object_file_rejects_missing_end_marker() {
	object_path="$TEST_TMPDIR/cache-object-missing-end.entry"
	output_file="$TEST_TMPDIR/cache-object-missing-end.out"

	cat >"$object_path" <<-EOF
		$ZXFER_CACHE_OBJECT_HEADER_LINE
		kind=demo-kind

		payload
	EOF

	g_zxfer_cache_object_kind_result="stale-kind"
	g_zxfer_cache_object_metadata_result="stale=metadata"
	g_zxfer_cache_object_payload_result="stale-payload"
	set +e
	zxfer_read_cache_object_file "$object_path" "demo-kind" >"$output_file"
	status=$?

	assertEquals "Cache-object reads should fail closed when the end marker is missing." \
		1 "$status"
	assertEquals "Rejected cache objects should not emit a payload." \
		"" "$(cat "$output_file")"
	assertEquals "Rejected cache objects should clear the kind scratch result." \
		"" "$g_zxfer_cache_object_kind_result"
	assertEquals "Rejected cache objects should clear the metadata scratch result." \
		"" "$g_zxfer_cache_object_metadata_result"
	assertEquals "Rejected cache objects should clear the payload scratch result." \
		"" "$g_zxfer_cache_object_payload_result"
}

test_zxfer_read_cache_object_file_rejects_wrong_kind() {
	object_path="$TEST_TMPDIR/cache-object-wrong-kind.entry"
	output_file="$TEST_TMPDIR/cache-object-wrong-kind.out"

	zxfer_write_cache_object_file_atomically \
		"$object_path" "actual-kind" "" "payload" >/dev/null ||
		fail "Unable to create a valid cache object fixture."

	g_zxfer_cache_object_kind_result="stale-kind"
	g_zxfer_cache_object_payload_result="stale-payload"
	set +e
	zxfer_read_cache_object_file "$object_path" "expected-kind" >"$output_file"
	status=$?

	assertEquals "Cache-object reads should fail closed when the published object kind does not match the expected kind." \
		1 "$status"
	assertEquals "Wrong-kind cache objects should not emit a payload." \
		"" "$(cat "$output_file")"
	assertEquals "Wrong-kind cache objects should clear the cached kind scratch state." \
		"" "$g_zxfer_cache_object_kind_result"
	assertEquals "Wrong-kind cache objects should clear the cached payload scratch state." \
		"" "$g_zxfer_cache_object_payload_result"
}

test_zxfer_read_cache_object_file_rejects_runtime_read_failures() {
	unreadable_path="$TEST_TMPDIR/cache-object-unreadable.entry"
	unreadable_output="$TEST_TMPDIR/cache-object-unreadable.out"
	output=$(
		(
			zxfer_write_cache_object_file_atomically \
				"$unreadable_path" "demo-kind" "" "payload" >/dev/null ||
				fail "Unable to create a cache object fixture for readback failure coverage."

			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result="stale-runtime-read"
				return 1
			}

			g_zxfer_cache_object_kind_result="stale-kind"
			g_zxfer_cache_object_metadata_result="stale=metadata"
			g_zxfer_cache_object_payload_result="stale-payload"
			set +e
			zxfer_read_cache_object_file "$unreadable_path" "demo-kind" >"$unreadable_output"
			unreadable_status=$?
			set -e

			printf 'status=%s\n' "$unreadable_status"
			printf 'payload=<%s>\n' "$(cat "$unreadable_output")"
			printf 'kind=<%s>\n' "$g_zxfer_cache_object_kind_result"
			printf 'metadata=<%s>\n' "$g_zxfer_cache_object_metadata_result"
			printf 'cache_payload=<%s>\n' "$g_zxfer_cache_object_payload_result"
		)
	)

	assertContains "Cache-object reads should fail closed when the staged runtime read helper fails." \
		"$output" "status=1"
	assertContains "Runtime read failures should not emit a payload." \
		"$output" "payload=<>"
	assertContains "Runtime read failures should clear the cached kind scratch state." \
		"$output" "kind=<>"
	assertContains "Runtime read failures should clear the cached metadata scratch state." \
		"$output" "metadata=<>"
	assertContains "Runtime read failures should clear the cached payload scratch state." \
		"$output" "cache_payload=<>"
}

test_zxfer_read_cache_object_file_rejects_invalid_kind_and_metadata_lines() {
	invalid_kind_path="$TEST_TMPDIR/cache-object-invalid-kind"
	invalid_metadata_path="$TEST_TMPDIR/cache-object-invalid-metadata"
	printf '%s\n%s\n\npayload\n%s\n' \
		"$ZXFER_CACHE_OBJECT_HEADER_LINE" \
		"broken" \
		"$ZXFER_CACHE_OBJECT_END_LINE" >"$invalid_kind_path"
	printf '%s\n%s\n%s\n\npayload\n%s\n' \
		"$ZXFER_CACHE_OBJECT_HEADER_LINE" \
		"kind=demo-kind" \
		"broken-metadata-line" \
		"$ZXFER_CACHE_OBJECT_END_LINE" >"$invalid_metadata_path"

	set +e
	zxfer_read_cache_object_file "$invalid_kind_path" "demo-kind" >/dev/null 2>&1
	invalid_kind_status=$?
	zxfer_read_cache_object_file "$invalid_metadata_path" "demo-kind" >/dev/null 2>&1
	invalid_metadata_status=$?
	set -e

	assertEquals "Cache-object reads should fail closed when the kind header is malformed." \
		1 "$invalid_kind_status"
	assertEquals "Cache-object reads should fail closed when metadata lines are malformed." \
		1 "$invalid_metadata_status"
}

test_zxfer_read_cache_object_file_rejects_empty_payloads() {
	empty_path="$TEST_TMPDIR/cache-object-empty.entry"
	empty_output="$TEST_TMPDIR/cache-object-empty.out"

	cat >"$empty_path" <<-EOF
		$ZXFER_CACHE_OBJECT_HEADER_LINE
		kind=demo-kind

		$ZXFER_CACHE_OBJECT_END_LINE
	EOF

	set +e
	zxfer_read_cache_object_file "$empty_path" "demo-kind" >"$empty_output"
	empty_status=$?

	assertEquals "Cache-object reads should fail closed when the published payload is empty." \
		1 "$empty_status"
	assertEquals "Empty-payload cache objects should not emit a payload." \
		"" "$(cat "$empty_output")"
}

test_zxfer_write_cache_object_file_atomically_cleans_up_stage_dirs_on_write_readback_and_rename_failures() {
	stage_root="$TEST_TMPDIR/cache-object-stage-cleanup"
	write_target="$stage_root/write-failure.entry"
	readback_target="$stage_root/readback-failure.entry"
	rename_target="$stage_root/rename-failure.entry"
	mkdir -p "$stage_root" || fail "Unable to create cache-object stage root."

	set +e
	(
		zxfer_write_cache_object_contents_to_path() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$write_target" "demo-kind" "" "payload"
	)
	write_status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		write_stage_count=$#
	else
		write_stage_count=0
	fi

	set +e
	(
		mv() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$rename_target" "demo-kind" "" "payload"
	)
	rename_status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		rename_stage_count=$#
	else
		rename_stage_count=0
	fi

	assertEquals "Atomic cache-object writes should fail closed when the staged payload cannot be written." \
		1 "$write_status"
	assertFalse "Failed staged payload writes should not leave a published cache object behind." \
		"[ -e \"$write_target\" ]"
	assertEquals "Failed staged payload writes should clean up their private stage directory." \
		0 "$write_stage_count"

	set +e
	(
		zxfer_read_cache_object_file() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$readback_target" "demo-kind" "" "payload"
	)
	readback_status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		readback_stage_count=$#
	else
		readback_stage_count=0
	fi

	assertEquals "Atomic cache-object writes should fail closed when the staged object cannot be read back for validation." \
		1 "$readback_status"
	assertFalse "Failed staged readback validation should not leave a published cache object behind." \
		"[ -e \"$readback_target\" ]"
	assertEquals "Failed staged readback validation should clean up their private stage directory." \
		0 "$readback_stage_count"

	assertEquals "Atomic cache-object writes should fail closed when the staged object cannot be renamed into place." \
		1 "$rename_status"
	assertFalse "Failed cache-object renames should not leave a published cache object behind." \
		"[ -e \"$rename_target\" ]"
	assertEquals "Failed cache-object renames should clean up their private stage directory." \
		0 "$rename_stage_count"
}

test_zxfer_write_cache_object_file_atomically_cleans_up_stage_dirs_when_rmdir_would_fail() {
	stage_root="$TEST_TMPDIR/cache-object-stage-rmdir-failure"
	target_path="$stage_root/published.entry"
	mkdir -p "$stage_root" || fail "Unable to create cache-object publish root."

	set +e
	(
		rmdir() {
			return 1
		}
		zxfer_write_cache_object_file_atomically \
			"$target_path" "demo-kind" "" "payload"
	)
	status=$?
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		stage_count=$#
	else
		stage_count=0
	fi

	assertEquals "Successful cache-object publishes should not depend on a direct rmdir cleanup path." \
		0 "$status"
	assertTrue "Successful cache-object publishes should still create the published target." \
		"[ -f \"$target_path\" ]"
	assertEquals "Successful cache-object publishes should clean up their private stage directory even when rmdir would fail." \
		0 "$stage_count"
}

test_zxfer_write_cache_object_file_atomically_reports_stage_dir_creation_failures_and_publish_dir_rejections() {
	set +e
	stage_output=$(
		(
			zxfer_create_cache_object_stage_dir_for_path() {
				return 1
			}
			zxfer_write_cache_object_file_atomically \
				"$TEST_TMPDIR/cache-object-stage-dir-failure" "demo-kind" "" "payload" >/dev/null 2>&1
			printf 'status=%s\n' "$?"
		)
	)
	publish_output=$(
		(
			stage_dir="$TEST_TMPDIR/publish-cache-object-stage"
			mkdir -p "$stage_dir" || exit 1
			relative_parent="relative-publish-parent"
			rm -rf "$relative_parent"
			mkdir -p "$relative_parent" || exit 1
			set +e
			zxfer_publish_cache_object_directory "$stage_dir" "$relative_parent/object-dir" >/dev/null 2>&1
			status=$?
			rm -rf "$relative_parent"
			set -e
			printf 'status=%s\n' "$status"
		)
	)
	set -e

	assertContains "Atomic cache-object writes should fail closed when the stage directory cannot be allocated." \
		"$stage_output" "status=1"
	assertContains "Publishing cache-object directories should reject existing relative parents that are outside the validated temp-root rules." \
		"$publish_output" "status=1"
}

test_zxfer_create_cache_object_stage_dir_for_path_preserves_parent_lookup_failures() {
	output=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			set +e
			zxfer_create_cache_object_stage_dir_for_path "$TEST_TMPDIR/cache-object-parent-lookup" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Cache-object stage-dir creation should preserve target-parent lookup failures." \
		"$output" "status=1"
}

test_zxfer_write_cache_object_file_atomically_registers_stage_dirs_in_current_shell_before_failures() {
	stage_root="$TEST_TMPDIR/cache-object-stage-current-shell"
	target_path="$stage_root/published.entry"
	trace_file="$TEST_TMPDIR/cache-object-stage-current-shell.trace"
	mkdir -p "$stage_root" || fail "Unable to create the cache-object stage root."

	output=$(
		(
			zxfer_write_cache_object_contents_to_path() {
				printf 'registered=<%s>\n' "${g_zxfer_runtime_artifact_cleanup_paths:-}" >"$trace_file"
				return 1
			}
			set +e
			zxfer_write_cache_object_file_atomically \
				"$target_path" "demo-kind" "" "payload" >/dev/null
			status=$?
			set -e
			printf 'status=%s\n' "$status"
		)
	)
	set -- "$stage_root"/.zxfer-cache-object.*
	if [ -e "$1" ]; then
		stage_count=$#
	else
		stage_count=0
	fi

	assertContains "Atomic cache-object writes should still fail closed when the staged payload helper fails." \
		"$output" "status=1"
	assertContains "Atomic cache-object writes should register their private stage dir in current-shell cleanup state before helper failures." \
		"$(cat "$trace_file")" "/.zxfer-cache-object."
	assertEquals "Atomic cache-object writes should still clean up their private stage directory after helper failures." \
		0 "$stage_count"
}

test_zxfer_publish_cache_object_directory_preserves_parent_lookup_failures() {
	stage_dir="$TEST_TMPDIR/cache-object-parent-lookup-stage"
	target_path="$TEST_TMPDIR/cache-object-parent-lookup-target/object.dir"
	mkdir -p "$stage_dir" || fail "Unable to create the staged cache-object directory."

	output=$(
		(
			zxfer_get_path_parent_dir() {
				return 1
			}
			set +e
			zxfer_publish_cache_object_directory "$stage_dir" "$target_path" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Publishing cache-object directories should preserve target-parent lookup failures." \
		"$output" "status=1"
}

test_zxfer_write_cache_object_contents_to_path_rejects_invalid_metadata_and_failed_writes() {
	write_failure_target="$TEST_TMPDIR/cache-object-write-failure.entry"

	set +e
	zxfer_write_cache_object_contents_to_path \
		"$TEST_TMPDIR/cache-object-invalid-metadata.entry" \
		"demo-kind" "broken-metadata-line" "payload" >/dev/null 2>&1
	metadata_status=$?
	write_failure_output=$(
		(
			printf() {
				return 1
			}
			set +e
			zxfer_write_cache_object_contents_to_path \
				"$write_failure_target" \
				"demo-kind" "kind=demo" "payload" >/dev/null 2>&1
			command printf 'status=%s\n' "$?"
		)
	)

	assertEquals "Cache-object content writes should fail closed when metadata lines are malformed." \
		1 "$metadata_status"
	assertContains "Cache-object content writes should fail closed when the staged write operation fails." \
		"$write_failure_output" "status=1"
	assertFalse "Failed cache-object content writes should not create the destination path when the staged write operation fails." \
		"[ -e \"$write_failure_target\" ]"
}

test_try_get_effective_tmpdir_fails_cleanly_when_no_safe_default_exists() {
	output=$(
		(
			unset TMPDIR
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			zxfer_try_get_default_tmpdir() {
				return 1
			}
			set +e
			zxfer_try_get_effective_tmpdir >/dev/null
			status=$?
			printf 'status=%s\n' "$status"
			printf 'requested=%s\n' "${g_zxfer_effective_tmpdir_requested:-}"
			printf 'effective=<%s>\n' "${g_zxfer_effective_tmpdir:-}"
		)
	)

	assertEquals "Temp-root resolution should fail cleanly when both TMPDIR and the built-in defaults are unavailable." \
		"status=1
requested=__ZXFER_DEFAULT_TMPDIR__
effective=<>" "$output"
}

test_zxfer_register_runtime_traps_installs_exit_handler() {
	output=$(
		(
			zxfer_register_runtime_traps
			trap
		)
	)

	assertContains "Runtime trap registration should install the shared zxfer_trap_exit handler." \
		"$output" "zxfer_trap_exit"
}

test_zxfer_init_destination_execution_context_reports_remote_decompress_resolution_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			g_option_z_compress=1
			g_cmd_decompress="zstd -d"
			g_cmd_zfs="/sbin/zfs"
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/remote/bin/$2"
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "decompress lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_init_destination_execution_context
		)
	)
	status=$?

	assertEquals "Destination execution-context initialization should fail closed when the remote decompressor cannot be resolved safely." \
		1 "$status"
	assertContains "Remote decompressor resolution failures should preserve the dependency error." \
		"$output" "decompress lookup failed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
