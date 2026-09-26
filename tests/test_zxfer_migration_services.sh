#!/bin/sh
#
# shunit2 tests for src/zxfer_migration_services.sh: -m/-c service stop,
# source unmount, and relaunch, live and in dry runs. The cases share the
# replication fixture, whose command stubs log what each step would run.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/replication_fixtures.sh
. "$TESTS_DIR/helpers/replication_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_migration_services"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_replication_fixture_setup
}

test_prepare_migration_services_stops_services_and_unmounts_sources() {
	g_option_m_migrate=1
	g_option_n_dryrun=0
	g_option_c_services="svc:/network/iscsi_target svc:/network/nfs/server"
	g_option_R_recursive="tank/src"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"

	l_saved_ifs=$IFS
	IFS=:
	set -f
	zxfer_prepare_migration_services
	l_after_ifs=$IFS
	case $- in
	*f*) l_after_globbing=disabled ;;
	*) l_after_globbing=enabled ;;
	esac
	IFS=$l_saved_ifs
	set +f

	assertEquals "Services should be piped to zxfer_stopsvcs intact." \
		"svc:/network/iscsi_target svc:/network/nfs/server" "$(cat "$STUB_STOPSVCS_LOG")"
	assertEquals "All recursive datasets must be unmounted before migrating." \
		"unmount tank/src
unmount tank/src/child" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Migration must create a final snapshot for the initial source." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset lists should run exactly once." "1" "$STUB_ZFS_LIST_CALLS"
	assertEquals "Migration dataset iteration should preserve a caller-defined IFS." ":" "$l_after_ifs"
	assertEquals "Migration dataset iteration should preserve disabled globbing." "disabled" "$l_after_globbing"
	case "$(zxfer_resolve_readonly_properties && printf '%s' "$g_zxfer_readonly_properties_result")" in
	*mountpoint*)
		fail "Readonly properties list should drop mountpoint during migration."
		;;
	esac
	assertEquals "Migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
}

test_prepare_migration_services_dry_run_previews_without_mutating_state() {
	g_option_m_migrate=1
	g_option_n_dryrun=1
	g_option_v_verbose=1
	g_option_c_services="svc:/network/iscsi_target svc:/network/nfs/server"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src
tank/src/child"
	state_log="$TEST_TMPDIR/migration_dry_run_state.log"
	output=$(
		(
			# The dry run renders through the real zxfer_stopsvcs.
			# shellcheck source=src/zxfer_migration_services.sh
			. "$ZXFER_ROOT/src/zxfer_migration_services.sh"
			IFS=:
			set -f
			zxfer_prepare_migration_services
			{
				printf 'readonly=%s\n' "$(zxfer_resolve_readonly_properties && printf '%s' "$g_zxfer_readonly_properties_result")"
				printf 'restart=%s\n' "$g_zxfer_services_to_restart"
				printf 'need=%s\n' "$g_services_need_relaunch"
				printf 'ifs=%s\n' "$IFS"
				printf 'flags=%s\n' "$-"
			} >"$state_log"
		) 2>&1
	)

	assertEquals "Dry-run migration should not unmount any datasets." "" "$(cat "$STUB_ZFS_CMD_LOG")"
	assertEquals "Dry-run migration should still preview the final snapshot helper once." \
		"tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dry-run migration should not refresh cached dataset state." "0" "$STUB_ZFS_LIST_CALLS"
	case "$(grep '^readonly=' "$state_log")" in
	*mountpoint*)
		fail "Dry-run migration should still drop mountpoint from the effective readonly-property list."
		;;
	esac
	assertEquals "Dry-run migration should leave the base readonly-property defaults unchanged." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
	assertContains "Dry-run migration should still track which services would need zxfer_relaunch later." \
		"$(cat "$state_log")" "restart= svc:/network/iscsi_target svc:/network/nfs/server"
	assertContains "Dry-run migration should still flag zxfer_relaunch as required." \
		"$(cat "$state_log")" "need=1"
	assertContains "Dry-run migration dataset iteration should preserve a caller-defined IFS." \
		"$(cat "$state_log")" "ifs=:"
	assertContains "Dry-run migration dataset iteration should preserve disabled globbing." \
		"$(sed -n 's/^flags=//p' "$state_log")" "f"
	assertContains "Dry-run migration should preview service-disabling commands." \
		"$output" "Dry run: 'svcadm' 'disable' '-st' 'svc:/network/iscsi_target'"
	assertContains "Dry-run migration should preview unmount commands for each source dataset." \
		"$output" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/src'"
	assertContains "Dry-run migration should preview descendant unmount commands too." \
		"$output" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/src/child'"
}

test_prepare_migration_services_treats_spaced_dataset_names_as_one_dataset() {
	g_option_m_migrate=1
	g_option_v_verbose=1
	g_initial_source="tank/my data"
	g_recursive_source_list="tank/my data
tank/my data/child two"

	g_option_n_dryrun=1
	preview=$(zxfer_prepare_migration_services 2>&1)
	g_option_n_dryrun=0
	zxfer_prepare_migration_services >/dev/null 2>&1

	assertContains "The -m preview should render the spaced root as one unmount." \
		"$preview" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/my data'"
	assertContains "The -m preview should render the spaced child as one unmount." \
		"$preview" "Dry run: 'mock_zfs_tool' 'unmount' 'tank/my data/child two'"
	assertNotContains "The -m preview must not split a dataset name on spaces." \
		"$preview" "'unmount' 'tank/my'"
	assertEquals "A live -m pass should unmount each spaced dataset once, whole." \
		"unmount tank/my data
unmount tank/my data/child two" "$(cat "$STUB_ZFS_CMD_LOG")"
}

test_prepare_migration_services_dry_run_uses_mountpoint_free_effective_readonly_list() {
	g_option_m_migrate=1
	g_option_n_dryrun=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	ZXFER_BASE_READONLY_PROPERTIES="type,mountpoint,creation"

	zxfer_prepare_migration_services

	assertEquals "Dry-run migration should drop mountpoint from the effective readonly-property list." \
		"type,creation" "$(zxfer_resolve_readonly_properties && printf '%s' "$g_zxfer_readonly_properties_result")"
	assertEquals "Dry-run migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
}

test_prepare_migration_services_preserves_service_restart_state_in_current_shell() {
	g_option_m_migrate=1
	g_option_c_services="svc:/system/filesystem/local"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"

	zxfer_stopsvcs() {
		g_zxfer_services_to_restart=" $1"
		g_services_need_relaunch=1
	}

	zxfer_prepare_migration_services

	assertEquals "Migration preflight should retain the service restart list in the parent shell." \
		" svc:/system/filesystem/local" "$g_zxfer_services_to_restart"
	assertEquals "Migration preflight should retain the zxfer_relaunch flag in the parent shell." \
		"1" "$g_services_need_relaunch"
}

test_prepare_migration_services_passes_multiline_service_input_to_stopsvcs_in_current_shell() {
	g_option_m_migrate=1
	g_option_c_services="svc:/network/nfs/server
svc:/system/filesystem/local"
	g_recursive_source_list=""
	service_input_file="$TEST_TMPDIR/prepare_migration_services.stdin"

	zxfer_stopsvcs() {
		printf '%s\n' "$1" >"$service_input_file"
	}

	zxfer_prepare_migration_services

	assertEquals "Migration preflight should pass the configured multiline service list to zxfer_stopsvcs unchanged." \
		"svc:/network/nfs/server
svc:/system/filesystem/local" "$(cat "$service_input_file")"
}

test_prepare_migration_services_propagates_service_disable_failures() {
	g_option_m_migrate=1
	g_option_c_services="svc:/system/filesystem/local"
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"

	set +e
	output=$(
		(
			zxfer_stopsvcs() {
				zxfer_throw_error "Could not disable service $1."
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should stop when service disabling fails." "1" "$status"
	assertContains "Migration preflight should surface the service-disable failure." \
		"$output" "Could not disable service svc:/system/filesystem/local."
}

test_prepare_migration_services_rejects_unmounted_sources() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					printf 'no\n'
					return 0
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should abort when a source dataset is not mounted." 2 "$status"
	assertContains "Unmounted migration sources should use the documented usage error." \
		"$output" "The source filesystem is not mounted, cannot use -m."
}

test_prepare_migration_services_reports_mounted_probe_failures() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					return 1
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		) 2>&1
	)
	status=$?

	assertEquals "Migration preflight should abort when mounted-state lookup fails." 1 "$status"
	assertContains "Mounted-state lookup failures should not be misreported as an unmounted source." \
		"$output" "Couldn't determine whether source tank/src is mounted."
}

test_prepare_migration_services_live_uses_mountpoint_free_effective_readonly_list() {
	g_option_m_migrate=1
	g_initial_source="tank/src"
	g_recursive_source_list="tank/src"
	ZXFER_BASE_READONLY_PROPERTIES="type,mountpoint,creation"

	zxfer_prepare_migration_services

	assertEquals "Live migration should drop mountpoint from the effective readonly-property list." \
		"type,creation" "$(zxfer_resolve_readonly_properties && printf '%s' "$g_zxfer_readonly_properties_result")"
	assertEquals "Live migration should not mutate the base readonly-property defaults." \
		"type,mountpoint,creation" "$ZXFER_BASE_READONLY_PROPERTIES"
}

test_prepare_migration_services_relaunches_when_unmount_fails() {
	g_option_m_migrate=1
	g_recursive_source_list="tank/src"
	g_initial_source="tank/src"

	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				if [ "$1" = "get" ] && [ "$4" = "mounted" ]; then
					printf 'yes\n'
					return 0
				fi
				if [ "$1" = "unmount" ]; then
					return 1
				fi
				return 0
			}
			zxfer_relaunch() {
				printf 'zxfer_relaunch\n'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_prepare_migration_services
		)
	)
	status=$?

	assertEquals "Failed unmounts during migration should abort." 1 "$status"
	assertContains "Failed unmounts should zxfer_relaunch services before aborting." "$output" "zxfer_relaunch"
	assertContains "Failed unmounts should identify the affected source." \
		"$output" "Couldn't unmount source tank/src."
}

test_stopsvcs_disables_services_and_tracks_restart_state() {
	log="$TEST_TMPDIR/stopsvcs_actions.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
trap - EXIT INT TERM HUP QUIT
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
svcadm() {
	printf '%s %s %s\n' "$1" "$2" "$3" >>"$SVC_LOG"
}
zxfer_stopsvcs 'svc:/network/nfs/server svc:/network/ssh'
printf 'restart=%s\n' "$g_zxfer_services_to_restart"
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "zxfer_stopsvcs should disable each requested service with -st." \
		"disable -st svc:/network/nfs/server
disable -st svc:/network/ssh" "$(cat "$log")"
	assertContains "Disabled services should be tracked for zxfer_relaunch." \
		"$output" "restart= svc:/network/nfs/server svc:/network/ssh"
	assertContains "Disabling services should mark zxfer_relaunch as required." \
		"$output" "need=1"
}

test_stopsvcs_returns_when_no_services_are_provided() {
	log="$TEST_TMPDIR/stopsvcs_empty.log"
	: >"$log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_services_need_relaunch=0
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
zxfer_stopsvcs ''
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "Empty service lists should not invoke svcadm." "" "$(cat "$log")"
	assertContains "Empty service lists should leave zxfer_relaunch tracking disabled." "$output" "need=0"
}

test_stopsvcs_ignores_whitespace_only_service_input() {
	log="$TEST_TMPDIR/stopsvcs_whitespace.log"
	: >"$log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_services_need_relaunch=0
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
zxfer_stopsvcs '
	 '
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	assertEquals "Whitespace-only service lists should not invoke svcadm." "" "$(cat "$log")"
	assertContains "Whitespace-only service lists should leave zxfer_relaunch tracking disabled." "$output" "need=0"
}

test_stopsvcs_relaunches_and_errors_when_disable_fails() {
	set +e
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
trap - EXIT INT TERM HUP QUIT
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
zxfer_relaunch() {
	printf 'zxfer_relaunch\n'
}
zxfer_throw_error() {
	printf '%s\n' "$1"
	exit 1
}
svcadm() {
	return 1
}
zxfer_stopsvcs 'svc:/network/nfs/server'
EOF
	)
	status=$?

	assertEquals "Service-disable failures should abort zxfer_stopsvcs." 1 "$status"
	assertContains "zxfer_stopsvcs should zxfer_relaunch services before failing." "$output" "zxfer_relaunch"
	assertContains "zxfer_stopsvcs failures should identify the offending service." \
		"$output" "Could not disable service svc:/network/nfs/server."
}

test_stopsvcs_normalizes_multiline_service_input_in_current_shell() {
	log="$TEST_TMPDIR/stopsvcs_current.log"
	: >"$log"
	g_zxfer_services_to_restart=""
	g_services_need_relaunch=0
	# Reload the owner after setUp's orchestration stub replaced zxfer_stopsvcs.
	# shellcheck source=src/zxfer_migration_services.sh
	. "$ZXFER_ROOT/src/zxfer_migration_services.sh"
	svcadm() {
		printf '%s %s %s\n' "$1" "$2" "$3" >>"$log"
	}

	zxfer_stopsvcs 'svc:/network/nfs/server
svc:/network/ssh    svc:/system/test'

	unset -f svcadm

	assertEquals "Current-shell service handling should normalize multiline input into one disable per service." \
		"disable -st svc:/network/nfs/server
disable -st svc:/network/ssh
disable -st svc:/system/test" "$(cat "$log")"
	assertEquals "Current-shell service handling should track every disabled service for zxfer_relaunch." \
		" svc:/network/nfs/server svc:/network/ssh svc:/system/test" "$g_zxfer_services_to_restart"
	assertEquals "Disabling services should still mark zxfer_relaunch as required." "1" "$g_services_need_relaunch"
}

test_relaunch_enables_services_and_clears_need_flag() {
	log="$TEST_TMPDIR/relaunch_actions.log"
	output=$(
		(
			SVC_LOG="$log"
			svcadm() {
				printf '%s %s\n' "$1" "$2" >>"$SVC_LOG"
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server svc:/network/ssh"
			g_services_need_relaunch=1
			zxfer_relaunch
			printf 'need=%s\n' "$g_services_need_relaunch"
		)
	)

	assertEquals "zxfer_relaunch should enable each previously disabled service." \
		"enable svc:/network/nfs/server
enable svc:/network/ssh" "$(cat "$log")"
	assertContains "Successful zxfer_relaunch should clear the zxfer_relaunch-needed flag." "$output" "need=0"
}

test_relaunch_preserves_smf_patterns_without_pathname_or_ifs_expansion() {
	work_dir="$TEST_TMPDIR/relaunch-pattern-cwd"
	log="$TEST_TMPDIR/relaunch-pattern.log"
	mkdir -p "$work_dir/svc:/site"
	: >"$work_dir/svc:/site/local-match"
	: >"$log"

	output=$(
		(
			cd "$work_dir" || exit 90
			SVC_LOG=$log
			svcadm() {
				printf '<%s>\n' "$2" >>"$SVC_LOG"
			}
			g_option_n_dryrun=0
			g_zxfer_services_to_restart='svc:/site/*'
			g_services_need_relaunch=1
			set +f
			zxfer_relaunch

			IFS=:
			set -f
			g_zxfer_services_to_restart='svc:/site/*'
			g_services_need_relaunch=1
			zxfer_relaunch
			shell_flags=$-
			if [ "${shell_flags#*f}" != "$shell_flags" ]; then
				glob_state=off
			else
				glob_state=on
			fi
			printf 'ifs=<%s> glob=%s\n' "$IFS" "$glob_state"
		)
	)

	assertEquals "SMF patterns must reach svcadm literally even when matching pathnames exist in the working directory." \
		"<svc:/site/*>
<svc:/site/*>" "$(cat "$log")"
	assertContains "Service relaunch should preserve a custom caller IFS and pre-existing noglob state." \
		"$output" "ifs=<:> glob=off"
}

test_relaunch_returns_success_when_no_services_are_pending_in_current_shell() {
	g_zxfer_services_to_restart=""
	g_services_need_relaunch=1
	g_services_relaunch_in_progress=1

	zxfer_relaunch

	assertEquals "Empty zxfer_relaunch queues should clear the zxfer_relaunch-needed flag." \
		"0" "$g_services_need_relaunch"
	assertEquals "Empty zxfer_relaunch queues should clear the in-progress guard." \
		"0" "$g_services_relaunch_in_progress"
}

test_relaunch_throws_when_service_enable_fails() {
	set +e
	output=$(
		(
			svcadm() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server"
			g_services_need_relaunch=1
			zxfer_relaunch
		)
	)
	status=$?

	assertEquals "zxfer_relaunch should abort when a service cannot be re-enabled." 1 "$status"
	assertContains "zxfer_relaunch failures should identify the service that failed to start." \
		"$output" "Couldn't re-enable service svc:/network/nfs/server."
}

test_relaunch_continues_after_failures_and_keeps_only_failed_services_pending() {
	log="$TEST_TMPDIR/relaunch_partial_failure.log"

	set +e
	output=$(
		(
			SVC_LOG="$log"
			svcadm() {
				printf '%s %s\n' "$1" "$2" >>"$SVC_LOG"
				if [ "$2" = "svc:/network/ssh" ]; then
					return 1
				fi
				return 0
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				printf 'need=%s\n' "$g_services_need_relaunch"
				printf 'pending=%s\n' "$g_zxfer_services_to_restart"
				printf 'guard=%s\n' "$g_services_relaunch_in_progress"
				exit 1
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server svc:/network/ssh svc:/system/test"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			zxfer_relaunch
		)
	)
	status=$?

	assertEquals "zxfer_relaunch should still fail when any service cannot be re-enabled." 1 "$status"
	assertEquals "zxfer_relaunch should still attempt every queued service even after one enable fails." \
		"enable svc:/network/nfs/server
enable svc:/network/ssh
enable svc:/system/test" "$(cat "$log")"
	assertContains "Partial zxfer_relaunch failures should keep the zxfer_relaunch-needed flag asserted." \
		"$output" "need=1"
	assertContains "Partial zxfer_relaunch failures should keep only failed services queued for later recovery." \
		"$output" "pending=svc:/network/ssh"
	assertContains "Partial zxfer_relaunch failures should leave the in-progress guard asserted until exit cleanup finishes." \
		"$output" "guard=1"
}

test_relaunch_reports_all_failed_services() {
	set +e
	output=$(
		(
			svcadm() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_zxfer_services_to_restart="svc:/network/nfs/server svc:/network/ssh"
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=0
			zxfer_relaunch
		)
	)
	status=$?

	assertEquals "zxfer_relaunch should fail when multiple services cannot be re-enabled." 1 "$status"
	assertContains "Multi-service zxfer_relaunch failures should mention every failed service." \
		"$output" "Couldn't re-enable services: svc:/network/nfs/server svc:/network/ssh."
}

test_relaunch_dry_run_previews_enable_commands_without_executing() {
	log="$TEST_TMPDIR/relaunch_dry_run.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SVC_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=1
g_option_v_verbose=1
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_zxfer_services_to_restart=" svc:/network/nfs/server svc:/network/ssh"
g_services_need_relaunch=1
svcadm() {
	printf '%s\n' "$*" >>"$SVC_LOG"
}
zxfer_relaunch
printf 'need=%s\n' "$g_services_need_relaunch"
EOF
	)

	l_relaunch_log=""
	if [ -f "$log" ]; then
		l_relaunch_log=$(cat "$log")
	fi
	assertEquals "Dry-run zxfer_relaunch should not execute svcadm enable." "" "$l_relaunch_log"
	assertContains "Dry-run zxfer_relaunch should preview the first enable command." \
		"$output" "Dry run: 'svcadm' 'enable' 'svc:/network/nfs/server'"
	assertContains "Dry-run zxfer_relaunch should preview every queued enable command." \
		"$output" "Dry run: 'svcadm' 'enable' 'svc:/network/ssh'"
	assertContains "Dry-run zxfer_relaunch should still clear the zxfer_relaunch-needed flag." \
		"$output" "need=0"
}

test_relaunch_dry_run_previews_enable_commands_in_current_shell() {
	log="$TEST_TMPDIR/relaunch_dry_run_current_shell.log"
	: >"$log"

	zxfer_echov() {
		printf '%s\n' "$*" >>"$log"
	}
	svcadm() {
		printf '%s\n' "$*" >>"$log"
	}
	g_option_n_dryrun=1
	g_zxfer_services_to_restart=" svc:/network/nfs/server"
	g_services_need_relaunch=1

	zxfer_relaunch

	# Restore the shared verbose helper so later tests are not affected by the stub.
	zxfer_echov() {
		if [ "$g_option_v_verbose" -eq 1 ]; then
			echo "$@"
		fi
	}
	unset -f svcadm

	assertEquals "Current-shell dry-run zxfer_relaunch should preview enable commands without executing svcadm." \
		"Restarting service svc:/network/nfs/server
Dry run: 'svcadm' 'enable' 'svc:/network/nfs/server'" "$(cat "$log")"
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
