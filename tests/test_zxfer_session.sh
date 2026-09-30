#!/bin/sh
#
# shunit2 tests for the session composition root in src/zxfer_session.sh:
# startup, zxfer_session_run's exit status, remote connection preparation,
# endpoint initialization and the EXIT trap. Its black-box behavior (signal
# exit statuses, one failure report, the run root and socket directory
# removed, dry runs contacting no host, migration service restarts) is pinned
# by the contract suites; these cases pin what those cannot reach.
#
# The fragments keep the fixture they were written for: the remote-host
# fixture for the remote cases and the runtime fixture for the lifecycle
# cases. The cases in this file use neither.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"
# shellcheck source=tests/helpers/runtime_fixtures.sh
. "$TESTS_DIR/helpers/runtime_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_session"
	zxfer_test_remote_host_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	# A failed case may leave a directory read-only.
	chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || :
	zxfer_test_cleanup_tmpdir
}

setUp() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_remote_tests.sh"; then
		zxfer_test_remote_host_fixture_setup
	elif zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"; then
		zxfer_test_runtime_fixture_setup
	fi
}

tearDown() {
	if zxfer_test_running_test_is_in "$TESTS_DIR/suites/zxfer_session_remote_tests.sh"; then
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

# zxfer-test-fragment: suites/zxfer_session_remote_tests.sh
# shellcheck source=tests/suites/zxfer_session_remote_tests.sh
. "$TESTS_DIR/suites/zxfer_session_remote_tests.sh"
# zxfer-test-fragment: suites/zxfer_session_lifecycle_tests.sh
# shellcheck source=tests/suites/zxfer_session_lifecycle_tests.sh
. "$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_session.sh" \
		"$TESTS_DIR/suites/zxfer_session_remote_tests.sh" \
		"$TESTS_DIR/suites/zxfer_session_lifecycle_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
