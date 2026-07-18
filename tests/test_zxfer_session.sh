#!/bin/sh
#
# shunit2 tests for the session composition root in src/zxfer_session.sh.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_session.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_session"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

test_session_boundary_owns_legacy_lifecycle_entry_points() {
	assertTrue "Session should own the legacy global initializer name." \
		"command -v zxfer_init_globals >/dev/null 2>&1"
	assertTrue "Session should own the legacy runtime trap registration name." \
		"command -v zxfer_register_runtime_traps >/dev/null 2>&1"
	assertTrue "Session should own the shared trap exit name." \
		"command -v zxfer_trap_exit >/dev/null 2>&1"
	assertTrue "Session should own post-CLI variable initialization." \
		"command -v zxfer_init_variables >/dev/null 2>&1"
}

test_zxfer_init_globals_preserves_owner_initialization_order() {
	output=$(
		(
			zxfer_session_test_log() {
				printf '%s\n' "$1"
			}
			zxfer_reset_failure_context() { zxfer_session_test_log failure; }
			zxfer_refresh_secure_path_state() { zxfer_session_test_log secure-path; }
			zxfer_init_cli_option_defaults() { zxfer_session_test_log cli; }
			zxfer_reset_quoting_state() { zxfer_session_test_log quoting; }
			zxfer_init_runtime_state_defaults() { zxfer_session_test_log runtime; }
			zxfer_reset_owned_lock_tracking() { zxfer_session_test_log locking; }
			zxfer_reset_profile_state() { zxfer_session_test_log profile; }
			zxfer_init_backup_storage_root() { zxfer_session_test_log backup-root; }
			zxfer_reset_replication_runtime_state() { zxfer_session_test_log replication; }
			zxfer_reset_migration_service_state() { zxfer_session_test_log migration-services; }
			zxfer_reset_send_job_state() { zxfer_session_test_log send-jobs; }
			zxfer_reset_send_receive_state() { zxfer_session_test_log send-receive; }
			zxfer_reset_background_job_state() { zxfer_session_test_log background; }
			zxfer_reset_operation_state() { zxfer_session_test_log operation-state; }
			zxfer_reset_destination_existence_cache() { zxfer_session_test_log destination-cache; }
			zxfer_reset_snapshot_record_indexes() { zxfer_session_test_log snapshot-index; }
			zxfer_reset_snapshot_producer_session_state() { zxfer_session_test_log snapshot-producers; }
			zxfer_reset_snapshot_discovery_state() { zxfer_session_test_log snapshot-discovery; }
			zxfer_reset_snapshot_reconcile_state() { zxfer_session_test_log snapshot-reconcile; }
			zxfer_reset_backup_metadata_state() { zxfer_session_test_log backup-metadata; }
			zxfer_reset_property_runtime_state() { zxfer_session_test_log property-runtime; }
			zxfer_reset_property_iteration_caches() { zxfer_session_test_log property-iteration; }
			zxfer_reset_property_reconcile_state() { zxfer_session_test_log property-reconcile; }
			zxfer_init_dependency_tool_defaults() { zxfer_session_test_log dependencies; }
			zxfer_reset_ssh_transport_state() { zxfer_session_test_log ssh-transport; }
			zxfer_reset_remote_host_state() { zxfer_session_test_log remote-hosts; }
			zxfer_init_temp_artifacts() { zxfer_session_test_log temp-artifacts; }
			zxfer_ensure_run_tmp_root() {
				zxfer_session_test_log run-root
				return 0
			}
			zxfer_apply_secure_path() { zxfer_session_test_log apply-path; }

			zxfer_init_globals
		)
	)
	expected='failure
secure-path
cli
quoting
runtime
locking
profile
backup-root
replication
migration-services
send-jobs
send-receive
background
operation-state
destination-cache
snapshot-index
snapshot-producers
snapshot-discovery
snapshot-reconcile
backup-metadata
property-runtime
property-iteration
property-reconcile
dependencies
ssh-transport
remote-hosts
temp-artifacts
run-root
apply-path'

	assertEquals "Session startup should preserve the established owner initialization order." \
		"$expected" "$output"
}

test_zxfer_session_initialize_preserves_bootstrap_and_trap_order() {
	output=$(
		(
			zxfer_discard_inherited_cleanup_state() {
				printf '%s\n' discard-cleanup
			}
			zxfer_initialize_dependency_defaults() {
				printf '%s\n' dependency-bootstrap
			}
			zxfer_register_runtime_traps() {
				printf '%s\n' traps
			}
			zxfer_init_globals() {
				printf '%s\n' globals
			}

			zxfer_session_initialize
		)
	)

	assertEquals "Session bootstrap should discard inherited cleanup handles before resolving dependencies or installing traps." \
		'discard-cleanup
dependency-bootstrap
traps
globals' "$output"
}

test_zxfer_session_initialize_discards_inherited_cleanup_handles_before_early_failure_trap() {
	external_root="$TEST_TMPDIR/session-inherited-root"
	mkdir -p "$external_root"
	printf '%s\n' sentinel >"$external_root/sentinel"

	set +e
	output=$(
		(
			g_zxfer_background_job_records="inherited-job	unit	424242	wrapper	$external_root/status"
			g_zxfer_cleanup_pids="424243"
			g_zxfer_cleanup_pid_records="424243	inherited helper"
			g_option_O_origin_host="operator@origin"
			g_option_T_target_host="operator@target"
			g_ssh_origin_control_socket="$external_root/origin.sock"
			g_ssh_target_control_socket="$external_root/target.sock"
			g_services_need_relaunch=1
			g_zxfer_services_to_restart="svc:/operator/service:default"
			g_zxfer_run_tmp_root=$external_root
			g_zxfer_owned_run_tmp_root=$external_root
			g_zxfer_owned_run_tmp_root_parent=$TEST_TMPDIR
			g_zxfer_runtime_artifact_cleanup_paths="$TEST_TMPDIR/.zxfer-inherited-stage"
			g_zxfer_failure_report_emitted=1
			g_option_V_very_verbose=1

			zxfer_initialize_dependency_defaults() { :; }
			zxfer_init_globals() { exit 73; }
			zxfer_abort_all_background_jobs() {
				[ -z "${g_zxfer_background_job_records:-}" ] || printf '%s\n' background-action
				return 0
			}
			zxfer_kill_registered_cleanup_pids() {
				[ -z "${g_zxfer_cleanup_pid_records:-}" ] || printf '%s\n' cleanup-pid-action
				return 0
			}
			zxfer_close_all_ssh_control_sockets() {
				if [ -n "${g_ssh_origin_control_socket:-}${g_ssh_target_control_socket:-}" ]; then
					printf '%s\n' ssh-action
				fi
				return 0
			}
			zxfer_cleanup_registered_runtime_artifacts() {
				[ -z "${g_zxfer_runtime_artifact_cleanup_paths:-}" ] || printf '%s\n' artifact-action
				return 0
			}
			zxfer_remove_run_tmp_root() {
				[ -z "${g_zxfer_run_tmp_root:-}" ] || printf '%s\n' run-root-action
				return 0
			}
			zxfer_relaunch() { printf '%s\n' migration-action; }
			zxfer_profile_metrics_enabled() { return 1; }
			zxfer_profile_add_elapsed_ms() { :; }
			zxfer_echoV() { :; }
			zxfer_profile_emit_summary() { :; }
			zxfer_emit_failure_report() {
				[ "${g_zxfer_failure_report_emitted:-0}" -eq 0 ] || printf '%s\n' report-suppressed
			}

			zxfer_session_initialize
		) 2>&1
	)
	status=$?

	assertEquals "An initialization failure after trap registration should preserve its original status." \
		73 "$status"
	assertEquals "Early-failure cleanup must not act on any inherited internal cleanup handle." \
		"" "$output"
	assertTrue "Early-failure cleanup must leave an inherited external run-root sentinel untouched." \
		"[ -f '$external_root/sentinel' ]"
}

test_zxfer_session_initialize_replaces_inherited_awk_before_early_failure_reporting() {
	marker="$TEST_TMPDIR/inherited-awk-executed"
	fake_awk="$TEST_TMPDIR/inherited-awk"
	cat >"$fake_awk" <<EOF
#!/bin/sh
: >"$marker"
exit 99
EOF
	chmod +x "$fake_awk"
	rm -f "$marker"

	(
		unset ZXFER_SECURE_PATH ZXFER_SECURE_PATH_APPEND
		g_zxfer_secure_path=""
		g_zxfer_dependency_path=""
		g_cmd_awk=$fake_awk
		zxfer_init_globals() { exit 73; }
		zxfer_abort_all_background_jobs() { return 0; }
		zxfer_kill_registered_cleanup_pids() { return 0; }
		zxfer_close_all_ssh_control_sockets() { return 0; }
		zxfer_cleanup_registered_runtime_artifacts() { return 0; }
		zxfer_remove_run_tmp_root() { return 0; }
		zxfer_profile_metrics_enabled() { return 1; }
		zxfer_profile_add_elapsed_ms() { :; }
		zxfer_echoV() { :; }
		zxfer_profile_emit_summary() { :; }
		zxfer_emit_failure_report() {
			zxfer_escape_report_value "early initialization failure" >/dev/null
		}

		zxfer_session_initialize
	) >/dev/null 2>&1
	status=$?

	assertEquals "An early initialization failure should preserve its original status." 73 "$status"
	assertFalse "Early failure reporting must not execute an inherited internal awk command." \
		"[ -e '$marker' ]"
}

test_zxfer_session_run_does_not_promote_optional_beep_failure() {
	(
		OPTIND=1
		g_option_k_backup_property_mode=0
		zxfer_set_failure_stage() { :; }
		zxfer_read_command_line_switches() { OPTIND=1; }
		zxfer_set_failure_roots() { :; }
		zxfer_consistency_check() { :; }
		zxfer_prepare_remote_host_connections() { :; }
		zxfer_init_variables() { :; }
		zxfer_run_zfs_mode_loop() { :; }
		zxfer_beep() { return 37; }

		zxfer_session_run backup/destination
	)
	l_status=$?

	assertEquals "A best-effort beep failure must not change a successful replication exit status." \
		0 "$l_status"
}

test_migration_service_status_only_restore_returns_failure_without_throwing() {
	output=$(
		(
			g_option_n_dryrun=0
			g_zxfer_services_to_restart="svc:/broken:default"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			zxfer_echov() { :; }
			svcadm() { return 1; }
			zxfer_throw_error() {
				printf '%s\n' throw-called
				exit 91
			}

			l_restore_status=0
			zxfer_restore_migration_services_status_only ||
				l_restore_status=$?
			printf 'status=%s\n' "$l_restore_status"
			printf 'message=%s\n' "$g_zxfer_migration_service_restore_failure_message"
			printf 'pending=%s\n' "$g_zxfer_services_to_restart"
			printf 'need=%s guard=%s\n' \
				"$g_services_need_relaunch" "$g_services_relaunch_in_progress"
		)
	)

	assertContains "Status-only migration restore should report a service enable failure without exiting its caller." \
		"$output" "status=1"
	assertContains "Status-only migration restore should publish the established operator-facing failure message." \
		"$output" "message=Couldn't re-enable service svc:/broken:default."
	assertContains "Status-only migration restore should retain failed services for recovery." \
		"$output" "pending=svc:/broken:default"
	assertContains "Status-only migration restore should retain the failure guards after an incomplete restore." \
		"$output" "need=1 guard=1"
	assertNotContains "Status-only migration restore must not invoke the exiting error API." \
		"$output" "throw-called"
}

test_zxfer_trap_exit_promotes_migration_restore_failure_and_finishes_reporting() {
	shutdown_log="$TEST_TMPDIR/session-migration-shutdown.log"
	: >"$shutdown_log"
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			zxfer_profile_metrics_enabled() { return 1; }
			zxfer_abort_all_background_jobs() { return 0; }
			zxfer_kill_registered_cleanup_pids() { return 0; }
			zxfer_close_all_ssh_control_sockets() { return 0; }
			zxfer_cleanup_registered_runtime_artifacts() {
				printf '%s\n' artifact-sweep >>"$shutdown_log"
				return 0
			}
			zxfer_remove_run_tmp_root() {
				printf '%s\n' root-sweep >>"$shutdown_log"
				return 0
			}
			zxfer_restore_migration_services_status_only() {
				printf '%s\n' restore-attempt
				g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service svc:/broken:default."
				return 37
			}
			zxfer_relaunch() {
				printf '%s\n' exiting-relaunch-called
				exit 91
			}
			zxfer_set_failure_context_if_empty() {
				printf 'failure-context=%s|%s|%s\n' "$1" "$2" "$3"
			}
			zxfer_profile_add_elapsed_ms() { printf '%s\n' profile-finalized; }
			zxfer_echoV() { printf 'verbose=%s\n' "$*"; }
			zxfer_profile_emit_summary() { printf '%s\n' profile-summary; }
			zxfer_emit_failure_report() { printf 'failure-report=%s\n' "$1"; }

			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "Trap cleanup should promote a migration-service restore failure over an otherwise successful exit." \
		37 "$status"
	assertContains "Trap cleanup should call the status-only migration owner operation." \
		"$output" "restore-attempt"
	assertContains "Trap cleanup should record the migration restoration failure in structured context." \
		"$output" "failure-context=runtime|trap cleanup|Couldn't re-enable service svc:/broken:default."
	assertContains "Trap cleanup should continue through profile rendering after migration restoration fails." \
		"$output" "profile-summary"
	assertContains "Trap cleanup should continue through structured failure reporting with the promoted status." \
		"$output" "failure-report=37"
	assertNotContains "Trap cleanup must not call the exiting ordinary relaunch API." \
		"$output" "exiting-relaunch-called"
	assertEquals "Trap cleanup should run both the pre-report and final artifact sweeps." \
		2 "$(grep -c '^artifact-sweep$' "$shutdown_log")"
	assertEquals "Trap cleanup should run both the pre-report and final run-root sweeps." \
		2 "$(grep -c '^root-sweep$' "$shutdown_log")"
}

test_zxfer_trap_exit_warns_when_migration_restore_fails_after_primary_failure() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			g_zxfer_failure_class=runtime
			g_zxfer_failure_stage=replication
			g_zxfer_failure_message="primary replication failure"
			zxfer_profile_metrics_enabled() { return 1; }
			zxfer_abort_all_background_jobs() { return 0; }
			zxfer_kill_registered_cleanup_pids() { return 0; }
			zxfer_close_all_ssh_control_sockets() { return 0; }
			zxfer_cleanup_registered_runtime_artifacts() { return 0; }
			zxfer_remove_run_tmp_root() { return 0; }
			zxfer_restore_migration_services_status_only() {
				g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service svc:/broken:default."
				return 37
			}
			zxfer_set_failure_context_if_empty() {
				printf '%s\n' secondary-context-replaced-primary
			}
			zxfer_warn_stderr() { printf 'warning=%s\n' "$*" >&2; }
			zxfer_profile_add_elapsed_ms() { :; }
			zxfer_echoV() { :; }
			zxfer_profile_emit_summary() { :; }
			zxfer_emit_failure_report() {
				printf 'report=%s|%s|%s|%s\n' \
					"$1" "$g_zxfer_failure_class" \
					"$g_zxfer_failure_stage" "$g_zxfer_failure_message"
			}

			(exit 23)
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "A secondary migration restore failure must preserve the primary exit status." \
		23 "$status"
	assertContains "A failed service restart must remain operator-visible beside the primary failure." \
		"$output" "warning=Couldn't re-enable service svc:/broken:default."
	assertContains "The primary structured failure context must remain unchanged." \
		"$output" "report=23|runtime|replication|primary replication failure"
	assertNotContains "The secondary cleanup failure must not replace the primary structured context." \
		"$output" "secondary-context-replaced-primary"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
