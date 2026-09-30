#!/bin/sh
# Lifecycle tests for src/zxfer_session.sh: the session reset, the signal
# traps, and how zxfer_trap_exit restores the shell modes and reports its
# own failed cleanup steps. Run by tests/test_zxfer_session.sh under the
# runtime fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

# zxfer_reset_session_state runs every owner module's reset, so no stale
# value a caller exports survives into the run. Each line names one owner's
# global, stale before the reset, and the value the reset leaves.
test_zxfer_reset_session_state_clears_each_owners_inherited_state() {
	output=$(
		(
			# 0 skips the start-clock read of a -V run.
			g_zxfer_profile_prescan=0
			g_cmd_parallel=stale
			g_option_j_jobs=9
			g_zxfer_failure_message=stale
			g_zxfer_effective_uid=stale
			g_zxfer_run_tmp_root=stale
			g_zxfer_send_jobs=stale
			g_zxfer_background_shell_spawn_mode=stale
			g_ssh_origin_control_socket=stale
			g_zxfer_services_to_restart=stale
			g_zxfer_profile_source_zfs_calls=9
			g_zxfer_new_snapshot_name=stale
			g_zxfer_new_snapshot_taken=1
			g_zxfer_wrapped_command_result=stale
			g_destination_existence_cache=stale
			g_zxfer_live_destination_listing_file=stale
			g_origin_parallel_cmd=stale
			g_recursive_source_list=stale
			g_last_common_snap=stale
			g_zxfer_snapshot_plan_file=stale
			g_backup_file_contents=stale
			g_zxfer_unsupported_filesystem_properties=stale
			g_zxfer_source_property_table=stale
			g_zxfer_property_error_result=stale
			g_zxfer_remote_capability_response_result=stale
			g_zxfer_local_os=stale

			zxfer_reset_session_state

			printf '%s\n' \
				"dependencies: parallel=<$g_cmd_parallel>" \
				"cli: jobs=$g_option_j_jobs" \
				"failure: message=<$g_zxfer_failure_message> stage=$g_zxfer_failure_stage" \
				"path security: uid=<$g_zxfer_effective_uid>" \
				"runtime: run root=<$g_zxfer_run_tmp_root>" \
				"send jobs: <$g_zxfer_send_jobs>" \
				"exec: spawn mode=<$g_zxfer_background_shell_spawn_mode>" \
				"ssh transport: origin socket=<$g_ssh_origin_control_socket>" \
				"migration: restart=<$g_zxfer_services_to_restart>" \
				"profile: source zfs calls=$g_zxfer_profile_source_zfs_calls" \
				"replication: snapshot name=<$g_zxfer_new_snapshot_name> taken=<$g_zxfer_new_snapshot_taken>" \
				"send/receive: wrapped=<$g_zxfer_wrapped_command_result>" \
				"destination existence: <$g_destination_existence_cache>" \
				"destination listing: <$g_zxfer_live_destination_listing_file>" \
				"snapshot producer: origin parallel=<$g_origin_parallel_cmd>" \
				"snapshot discovery: sources=<$g_recursive_source_list>" \
				"snapshot plan: last common=<$g_last_common_snap>" \
				"snapshot delete: plan file=<$g_zxfer_snapshot_plan_file>" \
				"backup metadata: rows=<$g_backup_file_contents>" \
				"property transfer: unsupported=<$g_zxfer_unsupported_filesystem_properties>" \
				"property caches: source table=<$g_zxfer_source_property_table>" \
				"property reads: error=<$g_zxfer_property_error_result>" \
				"remote hosts: capabilities=<$g_zxfer_remote_capability_response_result>" \
				"session: local os=<$g_zxfer_local_os>"
		)
	)

	assertEquals "The session reset should leave each owner's state at its start value." \
		"dependencies: parallel=<>
cli: jobs=1
failure: message=<> stage=startup
path security: uid=<>
runtime: run root=<>
send jobs: <>
exec: spawn mode=<>
ssh transport: origin socket=<>
migration: restart=<>
profile: source zfs calls=0
replication: snapshot name=<> taken=<0>
send/receive: wrapped=<>
destination existence: <>
destination listing: <>
snapshot producer: origin parallel=<>
snapshot discovery: sources=<>
snapshot plan: last common=<>
snapshot delete: plan file=<>
backup metadata: rows=<>
property transfer: unsupported=<>
property caches: source table=<>
property reads: error=<>
remote hosts: capabilities=<>
session: local os=<>" "$output"
}

test_zxfer_trap_exit_restores_shell_modes_before_mirroring_the_report() {
	# A signal can land while zxfer_create_runtime_artifact_file holds umask
	# 077 and noclobber, or between zxfer_split_begin and zxfer_split_end.
	# The report must still reach ZXFER_ERROR_LOG, appended in place under a
	# read-only parent (unless running as root).
	log_dir="$TEST_TMPDIR/trap-modes-log"
	log_path="$log_dir/failure.log"
	modes_file="$TEST_TMPDIR/trap-modes.out"
	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	chmod 500 "$log_dir"
	rm -f "$modes_file"

	status=0
	(
		set +e
		ZXFER_ERROR_LOG=$log_path
		zxfer_profile_emit_summary() {
			case $- in *C*) l_noclobber=on ;; *) l_noclobber=off ;; esac
			case $- in *f*) l_noglob=on ;; *) l_noglob=off ;; esac
			printf 'noclobber=%s noglob=%s umask=%s ifs=%s\n' "$l_noclobber" \
				"$l_noglob" "$(umask)" "$(printf '%s' "$IFS" | od -An -tx1 | tr -d ' \n')" \
				>"$modes_file"
		}
		g_zxfer_failure_class="runtime"
		g_zxfer_failure_stage="unit"
		g_zxfer_failure_message="interrupted mid-allocation"
		g_services_need_relaunch=0
		g_zxfer_run_umask=0027
		umask 077
		set -C -f
		IFS=:
		false
		zxfer_trap_exit
	) >/dev/null 2>&1 || status=$?
	chmod 700 "$log_dir"

	assertEquals "zxfer_trap_exit should keep the failing status." 1 "$status"
	assertEquals "zxfer_trap_exit should restore noclobber, noglob, the run umask and default IFS before reporting." \
		"noclobber=off noglob=off umask=0027 ifs=20090a" "$(cat "$modes_file")"
	assertContains "The failure report should reach ZXFER_ERROR_LOG after an interrupted allocation." \
		"$(cat "$log_path")" "interrupted mid-allocation"
	assertContains "Mirroring should keep the earlier log contents." \
		"$(cat "$log_path")" "existing: keep-me"
}

# Each row fails one cleanup step of zxfer_trap_exit after a run that ended
# with the given status: step|status before the trap|status and report. A
# failed step turns a clean exit into its status (1 for a failed removal)
# and its message; after an earlier failure it changes neither, and a failed
# ssh close does not even fill that failure's empty message. Each run still
# reports, and removes the run root again after the report.
test_zxfer_trap_exit_reports_each_failed_cleanup_step() {
	sweep_log="$TEST_TMPDIR/trap-cleanup-steps.log"
	while IFS='|' read -r l_failed_step l_status_before l_expected; do
		: >"$sweep_log"
		l_output=$(
			(
				trap - EXIT INT TERM HUP QUIT
				zxfer_reset_failure_context "unit"
				STEP_LOG=$sweep_log
				FAILED_STEP=$l_failed_step
				g_option_V_very_verbose=0
				zxfer_abort_all_send_jobs() {
					[ "$FAILED_STEP" = send_jobs ] || return 0
					g_zxfer_send_job_abort_failure_message="validated abort failed"
					return 17
				}
				zxfer_kill_registered_cleanup_pids() {
					[ "$FAILED_STEP" = cleanup_pids ] || return 0
					g_zxfer_cleanup_pid_abort_failure_message="validated cleanup helper abort failed"
					return 23
				}
				zxfer_close_all_ssh_control_sockets() {
					[ "$FAILED_STEP" = ssh_close ] || return 0
					return 19
				}
				zxfer_remove_ssh_control_socket_dir() { return 0; }
				zxfer_remove_run_tmp_root() {
					printf '%s\n' sweep >>"$STEP_LOG"
					[ "$FAILED_STEP" != run_root ]
				}
				zxfer_restore_migration_services_on_exit() {
					[ "$FAILED_STEP" = migration ] || return 0
					g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service svc:/broken:default."
					return 37
				}
				zxfer_echoV() { :; }
				zxfer_profile_emit_summary() { :; }
				zxfer_emit_failure_report() {
					printf '%s\n' report >>"$STEP_LOG"
					printf '%s|%s|%s|%s\n' "$1" "$g_zxfer_failure_class" \
						"$g_zxfer_failure_stage" "$g_zxfer_failure_message"
				}
				(exit "$l_status_before")
				zxfer_trap_exit
			)
		)
		l_status=$?

		assertEquals "[$l_failed_step after status $l_status_before] exit status and report" \
			"$l_expected" "$l_status: $l_output"
		assertEquals "[$l_failed_step] the run root is removed before and after the report" \
			"sweep
report
sweep" "$(cat "$sweep_log")"
	done <<EOF
send_jobs|0|17: 17|runtime|trap cleanup|validated abort failed
cleanup_pids|0|23: 23|runtime|trap cleanup|validated cleanup helper abort failed
run_root|0|1: 1|runtime|trap cleanup|Failed to remove one or more runtime temp artifacts during exit.
migration|0|37: 37|runtime|trap cleanup|Couldn't re-enable service svc:/broken:default.
ssh_close|5|5: 5||unit|
EOF
}

# Purpose: Rewrite `trap` listing lines as "SIGNAL action", dropping the SIG
# prefix and quoting that differ between shells.
# Usage: trap | zxfer_test_normalize_trap_listing
zxfer_test_normalize_trap_listing() {
	awk -v quote="'" '{
		signal = $NF
		sub(/^SIG/, "", signal)
		action = $0
		sub(/^trap -- /, "", action)
		sub(/ [^ ]*$/, "", action)
		gsub(quote, "", action)
		print signal " " action
	}'
}

# EXIT runs the plain handler; each signal passes its 128+signo status. A
# signal ignored on entry (INT and QUIT in an async suite) cannot be trapped,
# so the expectation covers only the signals a probe trap shows this shell
# accepts.
test_zxfer_session_initialize_maps_each_signal_to_its_exit_status() {
	trappable=$(
		(
			trap 'zxfer_trap_probe' HUP INT QUIT TERM
			trap
		) | zxfer_test_normalize_trap_listing |
			awk '$2 == "zxfer_trap_probe" { print $1 }'
	)
	expected="EXIT zxfer_trap_exit"
	for signal in $trappable; do
		case $signal in
		HUP) expected="$expected
HUP zxfer_trap_exit 129" ;;
		INT) expected="$expected
INT zxfer_trap_exit 130" ;;
		QUIT) expected="$expected
QUIT zxfer_trap_exit 131" ;;
		TERM) expected="$expected
TERM zxfer_trap_exit 143" ;;
		esac
	done
	output=$(
		(
			zxfer_init_session_environment() {
				:
			}
			zxfer_session_initialize
			trap
			trap - EXIT HUP INT QUIT TERM
		) | zxfer_test_normalize_trap_listing | grep ' zxfer_trap_exit' | sort
	)

	assertContains "TERM must be trappable in the test shell." "$trappable" "TERM"
	assertEquals "Session startup should route EXIT and each signal through zxfer_trap_exit with its exit status." \
		"$(printf '%s\n' "$expected" | sort)" "$output"
}
