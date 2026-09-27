#!/bin/sh
#
# shunit2 tests for the session composition root in src/zxfer_session.sh:
# startup order, zxfer_main, remote connection preparation and trap exit.
#
# The fragments keep the fixture they were written for: the exec fixture for
# the main-path cases, the remote-host fixture for the remote cases and the
# runtime fixture for the lifecycle cases. The cases in this file use none.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/exec_fixtures.sh
. "$TESTS_DIR/helpers/exec_fixtures.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"
# shellcheck source=tests/helpers/runtime_fixtures.sh
. "$TESTS_DIR/helpers/runtime_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_session"
	zxfer_test_exec_fixture_one_time_setup
	zxfer_test_remote_host_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	relax_test_tmpdir_permissions
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_main_tests.sh"; then
		zxfer_test_exec_fixture_setup
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_remote_tests.sh"; then
		zxfer_test_remote_host_fixture_setup
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"; then
		zxfer_test_runtime_fixture_setup
	fi
}

tearDown() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_main_tests.sh"; then
		relax_test_tmpdir_permissions
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_remote_tests.sh"; then
		zxfer_test_remote_host_fixture_teardown
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"; then
		zxfer_test_runtime_fixture_teardown
	fi
}

# Startup must settle the secure PATH before it creates anything, and create
# the run root before it narrows PATH, so mktemp still resolves when
# ZXFER_SECURE_PATH omits its directory.
test_session_environment_creates_the_run_root_between_secure_path_and_path_narrowing() {
	secure_dir="$TEST_TMPDIR/secure-path-without-mktemp"
	run_parent="$TEST_TMPDIR/session-run-parent"
	mkdir -p "$secure_dir" "$run_parent"
	chmod 700 "$run_parent"
	ln -s "$(command -v awk)" "$secure_dir/awk"
	printf '#!/bin/sh\nexit 0\n' >"$secure_dir/zfs"
	printf '#!/bin/sh\nexit 0\n' >"$secure_dir/ps"
	chmod 755 "$secure_dir/zfs" "$secure_dir/ps"

	(
		unset ZXFER_SECURE_PATH_APPEND ZXFER_BACKUP_DIR ZXFER_ERROR_LOG
		ZXFER_SECURE_PATH=$(printf '/bin\t/untrusted')
		TMPDIR=$run_parent
		zxfer_reset_session_state
		zxfer_init_session_environment
	) >/dev/null 2>&1
	invalid_status=$?
	invalid_entries=$(ls -A "$run_parent")
	narrowed_path=$(
		(
			unset ZXFER_SECURE_PATH_APPEND ZXFER_BACKUP_DIR ZXFER_ERROR_LOG
			ZXFER_SECURE_PATH=$secure_dir
			TMPDIR=$run_parent
			zxfer_reset_session_state
			zxfer_init_session_environment
			printf '%s\n' "$PATH"
		) 2>/dev/null
	)
	valid_entries=$(ls -A "$run_parent")

	assertEquals "An invalid secure PATH should stop startup before the run root is created." \
		"status=1 entries=" "status=$invalid_status entries=$invalid_entries"
	assertContains "The run root should be created before PATH is narrowed to a secure PATH without mktemp." \
		"$valid_entries" "zxfer."
	assertEquals "Startup should narrow PATH to the secure PATH after creating the run root." \
		"$secure_dir" "$narrowed_path"
}

test_zxfer_session_initialize_preserves_bootstrap_and_trap_order() {
	output=$(
		(
			zxfer_reset_session_state() {
				printf '%s\n' reset
				trap
			}
			zxfer_initialize_dependency_reporting_defaults() {
				printf '%s\n' reporting-awk
				trap
			}
			zxfer_init_session_environment() {
				printf '%s\n' environment
				trap
			}

			zxfer_session_initialize
			trap - EXIT HUP INT QUIT TERM
		) | awk '
			/zxfer_trap_exit/ {
				if (!(marker in listed)) print marker " with-traps"
				listed[marker] = 1
				next
			}
			/^trap/ { next }
			{ marker = $0; print }
		'
	)

	assertEquals "Session bootstrap should reset state and secure awk before installing traps, then prepare the environment." \
		'reset
reporting-awk
environment
environment with-traps' "$output"
}

test_zxfer_session_initialize_discards_inherited_cleanup_handles_before_early_failure_trap() {
	external_root="$TEST_TMPDIR/session-inherited-root"
	external_socket_dir="$TEST_TMPDIR/zxfer.ssh.inherited"
	mkdir -p "$external_root" "$external_socket_dir"
	printf '%s\n' sentinel >"$external_root/sentinel"
	printf '%s\n' sentinel >"$external_socket_dir/ssh-origin.sock"

	set +e
	output=$(
		(
			g_zxfer_send_jobs="inherited-job	424242	backup/inherited	$external_root/status	tank/inherited@snap	pgid"
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
			g_zxfer_ssh_control_socket_short_dir=$external_socket_dir
			g_zxfer_failure_report_emitted=1
			g_option_V_very_verbose=1

			zxfer_init_session_environment() { exit 73; }
			zxfer_abort_all_send_jobs() {
				[ -z "${g_zxfer_send_jobs:-}" ] || printf '%s\n' background-action
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
			zxfer_remove_run_tmp_root() {
				[ -z "${g_zxfer_run_tmp_root:-}" ] || printf '%s\n' run-root-action
				return 0
			}
			zxfer_relaunch() { printf '%s\n' migration-action; }
			g_option_V_very_verbose=0
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
	assertTrue "Early-failure cleanup must not remove anything from an inherited socket-directory handle." \
		"[ -f '$external_socket_dir/ssh-origin.sock' ]"
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
		g_cmd_awk=$fake_awk
		zxfer_init_session_environment() { exit 73; }
		zxfer_abort_all_send_jobs() { return 0; }
		zxfer_kill_registered_cleanup_pids() { return 0; }
		zxfer_close_all_ssh_control_sockets() { return 0; }
		zxfer_remove_ssh_control_socket_dir() { return 0; }
		zxfer_remove_run_tmp_root() { return 0; }
		g_option_V_very_verbose=0
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
		g_option_j_jobs=1
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

test_zxfer_session_run_promotes_final_backup_write_failure() {
	log="$TEST_TMPDIR/session_backup_write_failure.log"
	: >"$log"

	set +e
	(
		SESSION_LOG="$log"
		OPTIND=1
		g_option_j_jobs=1
		g_option_k_backup_property_mode=1
		zxfer_set_failure_stage() { :; }
		zxfer_read_command_line_switches() { OPTIND=1; }
		zxfer_set_failure_roots() { :; }
		zxfer_consistency_check() { :; }
		zxfer_prepare_remote_host_connections() { :; }
		zxfer_init_variables() { :; }
		zxfer_run_zfs_mode_loop() { :; }
		zxfer_write_backup_properties() { return 41; }
		zxfer_throw_error() {
			printf 'throw=%s status=%s\n' "$1" "$2" >>"$SESSION_LOG"
			return "$2"
		}
		zxfer_beep() { printf 'unexpected-success-beep\n' >>"$SESSION_LOG"; }

		zxfer_session_run backup/destination
	)
	status=$?

	assertEquals "The session boundary should preserve final backup-write failures." \
		41 "$status"
	assertEquals "Final backup-write failures should be reported and must not continue to the success beep." \
		"throw=Failed to write backup metadata. status=41" "$(cat "$log")"
}

# Local -j source discovery runs through parallel, so a missing local parallel
# must stop the run before any ssh master or background helper starts. With
# -O the origin's parallel is checked by discovery instead.
test_zxfer_session_run_requires_local_parallel_for_jobs_before_any_helper_starts() {
	log="$TEST_TMPDIR/session_parallel_preflight.log"
	: >"$log"

	for origin in "" "operator@origin"; do
		(
			SESSION_LOG="$log"
			OPTIND=1
			g_option_j_jobs=2
			g_option_O_origin_host=$origin
			g_option_k_backup_property_mode=0
			g_cmd_parallel=""
			zxfer_set_failure_stage() { :; }
			zxfer_read_command_line_switches() { OPTIND=1; }
			zxfer_set_failure_roots() { :; }
			zxfer_consistency_check() { :; }
			zxfer_prepare_remote_host_connections() {
				printf 'connect origin=%s\n' "$g_option_O_origin_host" >>"$SESSION_LOG"
			}
			zxfer_init_variables() { :; }
			zxfer_run_zfs_mode_loop() { :; }
			zxfer_beep() { :; }
			zxfer_throw_error() {
				printf 'throw=%s class=%s\n' "$1" "$g_zxfer_failure_class" >>"$SESSION_LOG"
				exit 1
			}

			zxfer_session_run backup/destination
		)
	done

	assertEquals "Only a local -j run without parallel fails, as a dependency error, before connecting." \
		"throw=The -j option requires parallel but it was not found in PATH on the local host. class=dependency
connect origin=operator@origin" "$(cat "$log")"
}

test_zxfer_trap_exit_promotes_migration_restore_failure_and_finishes_reporting() {
	shutdown_log="$TEST_TMPDIR/session-migration-shutdown.log"
	: >"$shutdown_log"
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			g_option_V_very_verbose=0
			zxfer_abort_all_send_jobs() { return 0; }
			zxfer_kill_registered_cleanup_pids() { return 0; }
			zxfer_close_all_ssh_control_sockets() { return 0; }
			zxfer_remove_ssh_control_socket_dir() {
				printf '%s\n' socket-dir-sweep >>"$shutdown_log"
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
			zxfer_profile_stop_timer() { printf '%s\n' profile-finalized; }
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
	assertEquals "Trap cleanup should remove the ssh socket directory once, before the report." \
		1 "$(grep -c '^socket-dir-sweep$' "$shutdown_log")"
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
			g_option_V_very_verbose=0
			zxfer_abort_all_send_jobs() { return 0; }
			zxfer_kill_registered_cleanup_pids() { return 0; }
			zxfer_close_all_ssh_control_sockets() { return 0; }
			zxfer_remove_ssh_control_socket_dir() { return 0; }
			zxfer_remove_run_tmp_root() { return 0; }
			zxfer_restore_migration_services_status_only() {
				g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service svc:/broken:default."
				return 37
			}
			zxfer_warn_stderr() { printf 'warning=%s\n' "$*" >&2; }
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
	assertEquals "The failed service restart should be warned about exactly once." \
		1 "$(printf '%s\n' "$output" | grep -c '^warning=')"
}

test_zxfer_note_trap_cleanup_failure_promotes_only_a_clean_exit_and_keeps_the_first_message() {
	output=$(
		(
			zxfer_reset_failure_context "unit"
			g_zxfer_trap_exit_status=0
			zxfer_note_trap_cleanup_failure 0 "ignored"
			printf 'zero=%s <%s>\n' "$g_zxfer_trap_exit_status" "$g_zxfer_failure_message"
			zxfer_note_trap_cleanup_failure 17 "first cleanup failure"
			printf 'first=%s <%s|%s|%s>\n' "$g_zxfer_trap_exit_status" \
				"$g_zxfer_failure_class" "$g_zxfer_failure_stage" "$g_zxfer_failure_message"
			zxfer_note_trap_cleanup_failure 23 "second cleanup failure"
			printf 'second=%s <%s>\n' "$g_zxfer_trap_exit_status" "$g_zxfer_failure_message"
		)
	)

	assertEquals "A zero status changes nothing; the first failure sets the status and message; a later one keeps both." \
		"zero=0 <>
first=17 <runtime|trap cleanup|first cleanup failure>
second=17 <first cleanup failure>" "$output"
}

# A failed ssh close fills an empty report message only when it is the run's
# first failure; every other cleanup step fills it whenever it is empty.
test_zxfer_trap_exit_keeps_a_failed_ssh_close_out_of_an_earlier_failures_report() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			zxfer_reset_failure_context "unit"
			g_option_V_very_verbose=0
			zxfer_abort_all_send_jobs() { return 0; }
			zxfer_kill_registered_cleanup_pids() { return 0; }
			zxfer_close_all_ssh_control_sockets() { return 19; }
			zxfer_remove_ssh_control_socket_dir() { return 0; }
			zxfer_remove_run_tmp_root() { return 0; }
			zxfer_echoV() { :; }
			zxfer_profile_emit_summary() { :; }
			zxfer_emit_failure_report() {
				printf 'report=%s|%s|<%s>\n' "$1" "${g_zxfer_failure_stage:-}" \
					"${g_zxfer_failure_message:-}"
			}
			(exit 5)
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "An earlier failure keeps its exit status." 5 "$status"
	assertEquals "A failed ssh close must not fill the report of an earlier failure." \
		"report=5|unit|<>" "$output"
}

# Purpose: Send one signal to a background subshell that installed the
# session traps and sits in a sleep loop, like zxfer's -j poll, so the last
# command status is 0 when the trap runs.
# Usage: zxfer_session_test_signal_trapped_subshell <signal>; returns the
# subshell's exit status and leaves its stderr in $TEST_TMPDIR/signal.stderr.
zxfer_session_test_signal_trapped_subshell() {
	l_signal_ready="$TEST_TMPDIR/signal.ready"
	rm -f "$l_signal_ready"
	(
		zxfer_init_session_environment() { :; }
		zxfer_session_initialize
		: >"$l_signal_ready"
		l_signal_polls=0
		while [ "$l_signal_polls" -lt 30 ]; do
			l_signal_polls=$((l_signal_polls + 1))
			sleep 1
		done
	) >/dev/null 2>"$TEST_TMPDIR/signal.stderr" &
	l_signal_pid=$!

	l_signal_tries=0
	while [ ! -e "$l_signal_ready" ] && [ "$l_signal_tries" -lt 100 ]; do
		l_signal_tries=$((l_signal_tries + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	kill -s "$1" "$l_signal_pid" 2>/dev/null
	wait "$l_signal_pid"
}

# Purpose: Assert one exit status and exactly one structured signal failure
# report in $TEST_TMPDIR/signal.stderr.
# Usage: zxfer_session_test_assert_signal_exit <actual-status> <expected-status>
zxfer_session_test_assert_signal_exit() {
	assertEquals "A signal should exit with status 128+signo." "$2" "$1"
	assertEquals "A signal should emit exactly one structured failure report." \
		1 "$(grep -c '^zxfer: failure report begin$' "$TEST_TMPDIR/signal.stderr")"
	assertTrue "The report should name the signal stage." \
		"grep -Fqx 'failure_stage: signal' '$TEST_TMPDIR/signal.stderr'"
	assertTrue "The report should carry the signal exit status." \
		"grep -Fqx 'message: zxfer was interrupted by a signal (exit status $2).' '$TEST_TMPDIR/signal.stderr'"
}

# A signal used to exit with the interrupted command's status: 0 after a
# poll sleep, with no report. The handler now exits 128+signo and reports.
test_zxfer_trap_exit_term_exits_143_with_one_signal_report() {
	zxfer_session_test_signal_trapped_subshell TERM
	zxfer_session_test_assert_signal_exit "$?" 143
}

# The INT trap runs `zxfer_trap_exit 130`. It is called directly because the
# runner starts suites as async lists, which inherit INT ignored on some shells
# (bash 3.2), and a non-interactive shell cannot trap a signal ignored on entry.
test_zxfer_trap_exit_int_status_exits_130_with_one_signal_report() {
	(
		trap - EXIT INT TERM HUP QUIT
		true
		zxfer_trap_exit 130
	) >/dev/null 2>"$TEST_TMPDIR/signal.stderr"
	zxfer_session_test_assert_signal_exit "$?" 130
}

# zxfer-test-fragment: suites/zxfer_session_main_tests.sh
# shellcheck source=tests/suites/zxfer_session_main_tests.sh
. "$TESTS_DIR/suites/zxfer_session_main_tests.sh"
# zxfer-test-fragment: suites/zxfer_session_remote_tests.sh
# shellcheck source=tests/suites/zxfer_session_remote_tests.sh
. "$TESTS_DIR/suites/zxfer_session_remote_tests.sh"
# zxfer-test-fragment: suites/zxfer_session_lifecycle_tests.sh
# shellcheck source=tests/suites/zxfer_session_lifecycle_tests.sh
. "$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_session.sh" \
		"$TESTS_DIR/suites/zxfer_session_main_tests.sh" \
		"$TESTS_DIR/suites/zxfer_session_remote_tests.sh" \
		"$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
