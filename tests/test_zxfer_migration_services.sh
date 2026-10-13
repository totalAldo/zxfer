#!/bin/sh
#
# shunit2 tests for src/zxfer_migration_services.sh: the service-list split
# and the restart decisions the EXIT trap makes. The -m/-c flows (the stop,
# mount check, unmount, snapshot and restart order, the dry-run preview and
# each failure path) are pinned black-box in tests/test_contract_failures.sh
# through a mock svcadm.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_migration_services"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_reset_all_owner_state
}

# Both operations own their service arguments. Splitting preserves wildcard
# patterns, caller IFS and noglob, even when service calls replace split scratch.
test_service_lists_split_on_whitespace_and_keep_patterns_literal() {
	work_dir="$TEST_TMPDIR/service-pattern-cwd"
	svcadm_log="$TEST_TMPDIR/service-split-svcadm.log"
	mkdir -p "$work_dir/svc:/site"
	: >"$work_dir/svc:/site/local-match"
	: >"$svcadm_log"
	output=$(
		cd "$work_dir" || exit 90
		svcadm() {
			printf '%s|%s|%s\n' "$1" "${2:-}" "${3:-}" >>"$svcadm_log"
			zxfer_split_tokens_into_result 'unrelated helper scratch'
		}
		for l_caller_ifs in default colon; do
			if [ "$l_caller_ifs" = colon ]; then
				IFS=:
				set -f
			fi
			for l_list in "" "$(printf ' \t\n ')" \
				"$(printf 'svc:/a\nsvc:/b    svc:/c')" 'svc:/site/*'; do
				zxfer_stopsvcs "$l_list"
				printf 'queue=<%s>\n' "$g_zxfer_services_to_restart"
				zxfer_restore_migration_services_status_only
				printf 'restored=<%s>\n' "$g_zxfer_services_to_restart"
			done
			l_flags=$-
			if [ "${l_flags#*f}" != "$l_flags" ]; then
				printf 'noglob kept, ifs=<%s>\n' "$IFS"
			else
				printf '%s\n' 'globbing kept'
			fi
		done
	)
	assertEquals "Operations keep literal ordered names and caller shell modes." \
		"queue=<>
restored=<>
queue=<>
restored=<>
queue=< svc:/a svc:/b svc:/c>
restored=<>
queue=< svc:/site/*>
restored=<>
globbing kept
queue=<>
restored=<>
queue=<>
restored=<>
queue=< svc:/a svc:/b svc:/c>
restored=<>
queue=< svc:/site/*>
restored=<>
noglob kept, ifs=<:>" "$output"
	l_expected_calls="disable|-st|svc:/a
disable|-st|svc:/b
disable|-st|svc:/c
enable|svc:/a|
enable|svc:/b|
enable|svc:/c|
disable|-st|svc:/site/*
enable|svc:/site/*|"
	assertEquals "Empty lists do nothing; every literal name stops and restarts in order." \
		"$l_expected_calls
$l_expected_calls" "$(cat "$svcadm_log")"
}

test_relaunch_returns_success_when_no_services_are_pending_in_current_shell() {
	g_zxfer_services_to_restart=""
	g_services_relaunch_in_progress=1

	zxfer_relaunch

	assertEquals "Empty zxfer_relaunch queues should clear the in-progress guard." \
		"0" "$g_services_relaunch_in_progress"
}

test_restore_retains_only_failed_services_and_exit_does_not_retry_them() {
	(
		svcadm() {
			printf '%s\n' "$*" >>"$TEST_TMPDIR/service-restore-calls"
			zxfer_split_tokens_into_result 'unrelated restore scratch'
			[ "$2" != svc:/bad1 ] && [ "$2" != svc:/bad2 ]
		}
		g_zxfer_services_to_restart=' svc:/good svc:/bad1 svc:/bad2'
		l_test_restore_status=0
		zxfer_restore_migration_services_status_only || l_test_restore_status=$?
		printf 'status=%s queue=<%s> guard=%s\nmessage=%s\n' \
			"$l_test_restore_status" "$g_zxfer_services_to_restart" \
			"$g_services_relaunch_in_progress" "$g_zxfer_migration_service_restore_failure_message"
		zxfer_restore_migration_services_on_exit
		svcadm() { return 0; }
		zxfer_restore_migration_services_status_only
		printf 'retry=%s queue=<%s> guard=%s message=<%s>\n' "$?" \
			"$g_zxfer_services_to_restart" "$g_services_relaunch_in_progress" \
			"$g_zxfer_migration_service_restore_failure_message"
	) >"$TEST_TMPDIR/service-restore-results"
	assertEquals "A partial restore retains failed names and its diagnostic; an explicit retry clears recovery state." \
		"status=1 queue=<svc:/bad1 svc:/bad2> guard=1
message=Couldn't re-enable services: svc:/bad1 svc:/bad2.
retry=0 queue=<> guard=0 message=<>" "$(cat "$TEST_TMPDIR/service-restore-results")"
	assertEquals "Exit must not repeat a failed ordinary restore." \
		"enable svc:/good
enable svc:/bad1
enable svc:/bad2" "$(cat "$TEST_TMPDIR/service-restore-calls")"
}

test_restore_migration_services_on_exit_restarts_pending_services_and_reports_failures() {
	output=$(
		(
			zxfer_echoV() { printf 'verbose=%s\n' "$1"; }
			zxfer_warn_stderr() { printf 'warning=%s\n' "$1"; }
			zxfer_restore_migration_services_status_only() {
				printf '%s\n' restore-attempt
				g_zxfer_migration_service_restore_failure_message=$l_test_restore_message
				return "$l_test_restore_status"
			}

			g_zxfer_services_to_restart=""
			zxfer_restore_migration_services_on_exit
			printf 'none=%s\n' "$?"

			g_zxfer_services_to_restart="svc:/pending:default"
			g_services_relaunch_in_progress=1
			zxfer_restore_migration_services_on_exit
			printf 'in_progress=%s\n' "$?"

			g_services_relaunch_in_progress=0
			l_test_restore_status=0
			l_test_restore_message=""
			zxfer_restore_migration_services_on_exit
			printf 'restored=%s\n' "$?"

			l_test_restore_status=37
			g_zxfer_failure_message=""
			zxfer_restore_migration_services_on_exit
			printf 'failed=%s message=<%s>\n' "$?" \
				"$g_zxfer_migration_service_restore_failure_message"

			l_test_restore_message="Couldn't re-enable service svc:/broken:default."
			g_zxfer_failure_message="primary replication failure"
			zxfer_restore_migration_services_on_exit
			printf 'after_primary=%s\n' "$?"
		)
	)

	assertEquals "Exit-time restore should skip when nothing waits or a relaunch already failed, restart pending services, and warn about a failed restart only beside an earlier failure." \
		"none=0
verbose=zxfer exiting with services still stopped after a failed zxfer_relaunch attempt.
in_progress=0
verbose=zxfer exiting early; restarting stopped services.
restore-attempt
restored=0
verbose=zxfer exiting early; restarting stopped services.
restore-attempt
failed=37 message=<Failed to restore stopped migration services during exit.>
verbose=zxfer exiting early; restarting stopped services.
restore-attempt
warning=Couldn't re-enable service svc:/broken:default.
after_primary=37" "$output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
