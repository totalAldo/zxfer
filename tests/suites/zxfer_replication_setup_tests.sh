#!/bin/sh
# Source selection, snapshot creation, and rollback behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Run zxfer_prepare_zfs_mode_roots in a subshell; prints its stderr and
# returns its exit status.
zxfer_test_prepare_roots_in_subshell() {
	(
		trap - EXIT INT TERM HUP QUIT
		zxfer_prepare_zfs_mode_roots >/dev/null
	) 2>&1
}

test_prepare_zfs_mode_roots_rejects_services_without_svcadm() {
	g_option_R_recursive="tank/src"
	g_option_m_migrate=1
	g_option_c_services="svc:/network/nfs/server"
	empty_path="$TEST_TMPDIR/no_svcadm"
	mkdir -p "$empty_path"
	old_path=$PATH

	status=0
	PATH="$empty_path"
	zxfer_test_prepare_roots_in_subshell >/dev/null || status=$?
	PATH=$old_path

	assertEquals "Service migration should fail fast when svcadm is unavailable." "2" "$status"
}

test_prepare_zfs_mode_roots_requires_m_for_services() {
	g_option_R_recursive="tank/src"
	g_option_c_services="svc:/network/nfs/server"
	g_option_m_migrate=0

	status=0
	output=$(zxfer_test_prepare_roots_in_subshell) || status=$?

	assertEquals "Service-management requests should require -m." "1" "$status"
	assertContains "The -c without -m failure should say why." \
		"$output" "When using -c, -m needs to be specified as well."
}

test_prepare_zfs_mode_roots_strips_trailing_slashes() {
	g_option_R_recursive="tank/src///"
	g_destination="backup/target//"

	zxfer_prepare_zfs_mode_roots

	assertEquals "Trailing slashes should be removed from source." "tank/src" "$g_initial_source"
	assertEquals "Trailing slashes should be removed from destination." "backup/target" "$g_destination"
	assertEquals "Trailing slash flag should record the original suffix." "1" "$g_initial_source_had_trailing_slash"
}

test_prepare_zfs_mode_roots_rejects_absolute_paths() {
	g_option_R_recursive="/tank/src"
	status_source=0
	zxfer_test_prepare_roots_in_subshell >/dev/null || status_source=$?

	g_option_R_recursive="tank/src"
	g_destination="/backup/target"
	status_dest=0
	zxfer_test_prepare_roots_in_subshell >/dev/null || status_dest=$?

	assertEquals "Absolute source paths should be rejected." "2" "$status_source"
	assertEquals "Absolute destination paths should be rejected." "2" "$status_dest"
}

test_prepare_zfs_mode_roots_rejects_control_characters() {
	for operand in N R destination; do
		g_option_N_nonrecursive=""
		g_option_R_recursive=""
		g_destination="backup/target"
		case $operand in
		N) g_option_N_nonrecursive="tank/src
tank/other" ;;
		R) g_option_R_recursive="tank/src
/tank/other" ;;
		destination)
			g_option_R_recursive="tank/src"
			g_destination="backup/target
backup/other"
			;;
		esac

		status=0
		output=$(zxfer_test_prepare_roots_in_subshell) || status=$?

		assertEquals "A newline in the $operand operand must be a usage error." "2" "$status"
		assertContains "The $operand rejection should name the control-character rule." \
			"$output" "Source and destination must not contain control characters."
	done

	g_option_R_recursive="tank/src"
	g_destination="backup/tar	get"
	status=0
	zxfer_test_prepare_roots_in_subshell >/dev/null || status=$?
	assertEquals "A tab in the destination must be a usage error." "2" "$status"
}

test_prepare_zfs_mode_roots_rejects_snapshot_sources() {
	g_option_R_recursive="tank/src@snap1"

	status=0
	output=$(zxfer_test_prepare_roots_in_subshell) || status=$?

	assertEquals "Snapshot-source validation should abort when the requested source is already a snapshot." \
		1 "$status"
	assertContains "Snapshot-source validation should explain why snapshot sources are rejected." \
		"$output" "Snapshots are not allowed as a source."
}

test_rollback_destination_to_last_common_snapshot_shortcuts_non_destructive_cases_in_current_shell() {
	output=$(
		(
			log="$TEST_TMPDIR/rollback_shortcuts_current.log"
			: >"$log"
			g_actual_dest="backup/target/src"
			g_last_common_snap="tank/src@snap1"
			zxfer_probe_destination_existence() {
				printf '%s\n' "exists" >>"$log"
				g_zxfer_destination_exists_result=0
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "rollback" >>"$log"
			}

			g_option_F_force_rollback=""
			g_did_delete_dest_snapshots=1
			g_deleted_dest_newer_snapshots=1
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'no_force_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_option_F_force_rollback=1
			g_did_delete_dest_snapshots=0
			g_deleted_dest_newer_snapshots=1
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'no_delete_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_did_delete_dest_snapshots=1
			g_deleted_dest_newer_snapshots=0
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'no_newer_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_deleted_dest_newer_snapshots=1
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'missing_dest_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"

			g_last_common_snap=""
			zxfer_probe_destination_existence() {
				printf '%s\n' "exists" >>"$log"
				g_zxfer_destination_exists_result=1
			}
			zxfer_rollback_destination_to_last_common_snapshot
			printf 'empty_common_exists=%s\n' "$(awk '/^exists$/ { count++ } END { print count + 0 }' "$log")"
			printf 'rollback_calls=%s\n' "$(awk '/^rollback$/ { count++ } END { print count + 0 }' "$log")"
		)
	)

	assertContains "Destination rollback should not probe live state when receive-side forcing is disabled." \
		"$output" "no_force_exists=0"
	assertContains "Destination rollback should not probe live state when no destination snapshots were deleted." \
		"$output" "no_delete_exists=0"
	assertContains "Destination rollback should not probe live state when no newer destination snapshots were deleted." \
		"$output" "no_newer_exists=0"
	assertContains "Destination rollback should stop without rolling back when the destination no longer exists." \
		"$output" "missing_dest_exists=1"
	assertContains "Destination rollback should stop without issuing a rollback when there is no last common snapshot name." \
		"$output" "empty_common_exists=2"
	assertContains "Destination rollback should not issue rollback commands in any non-destructive shortcut path." \
		"$output" "rollback_calls=0"
}

test_set_actual_dest_without_trailing_slash_appends_relative_path() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0

	zxfer_set_actual_dest "tank/src/projects/alpha"

	assertEquals "Destination should mirror the relative source suffix." "backup/target/src/projects/alpha" "$g_actual_dest"
}

test_set_actual_dest_with_trailing_slash_preserves_destination_prefix() {
	g_initial_source="tank/src"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=1

	zxfer_set_actual_dest "tank/src/projects/beta"

	assertEquals "Trailing slash should replicate directly under the destination root." "backup/target/projects/beta" "$g_actual_dest"
}

test_set_actual_dest_treats_regex_significant_source_names_as_literal_paths() {
	g_initial_source="tank/app.v1"
	g_destination="backup/target"
	g_initial_source_had_trailing_slash=0

	zxfer_set_actual_dest "tank/app.v1/projects.release"

	assertEquals "Destination mapping should preserve dots in the source root and child dataset names as literal path components." \
		"backup/target/app.v1/projects.release" "$g_actual_dest"
}

test_refresh_dataset_iteration_state_refreshes_property_tree_prefetch_context_when_available() {
	log="$TEST_TMPDIR/refresh_prefetch_context.log"
	: >"$log"

	(
		REFRESH_LOG="$log"
		zxfer_refresh_property_tree_prefetch_context() {
			printf 'refresh-prefetch\n' >>"$REFRESH_LOG"
		}
		zxfer_refresh_dataset_iteration_state
	)

	assertEquals "Refreshing dataset iteration state should also refresh the recursive property-tree prefetch context when that optimization helper is available." \
		"refresh-prefetch" "$(cat "$log")"
}

test_maybe_capture_preflight_snapshot_captures_when_enabled() {
	g_option_s_make_snapshot=1
	g_option_n_dryrun=0
	g_initial_source="tank/src"

	zxfer_maybe_capture_preflight_snapshot

	assertEquals "Snapshot helper should run once when -s is enabled." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Refreshing dataset state should call zxfer_get_zfs_list exactly once." "1" "$STUB_ZFS_LIST_CALLS"
}

test_maybe_capture_preflight_snapshot_dry_run_skips_refresh() {
	g_option_s_make_snapshot=1
	g_option_n_dryrun=1
	g_initial_source="tank/src"

	zxfer_maybe_capture_preflight_snapshot

	assertEquals "Dry-run -s should still preview the snapshot helper once." "tank/src" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dry-run -s should not refresh cached dataset state." "0" "$STUB_ZFS_LIST_CALLS"
}

test_maybe_capture_preflight_snapshot_skips_when_migrating() {
	g_option_s_make_snapshot=1
	g_option_m_migrate=1
	g_initial_source="tank/src"

	zxfer_maybe_capture_preflight_snapshot

	assertEquals "Migration path should not trigger new snapshots from -s." "" "$(cat "$STUB_NEW_SNAP_LOG")"
	assertEquals "Dataset refresh should not run when snapshot is skipped." "0" "$STUB_ZFS_LIST_CALLS"
}

test_newsnap_uses_recursive_snapshot_flag() {
	log="$TEST_TMPDIR/zxfer_newsnap.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive="tank/src"
g_zxfer_new_snapshot_name="zxfer_unit"
g_cmd_zfs="mock_zfs_tool"
zxfer_run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
zxfer_newsnap "tank/src@old"
EOF
	)
	: "$output"

	assertEquals "Recursive snapshots should use the -r flag and strip the old snapshot suffix." \
		"snapshot -r tank/src@zxfer_unit" "$(cat "$log")"
}

test_newsnap_uses_nonrecursive_snapshot_without_r_flag() {
	log="$TEST_TMPDIR/newsnap_single.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=0
g_option_v_verbose=0
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive=""
g_zxfer_new_snapshot_name="zxfer_single"
g_cmd_zfs="mock_zfs_tool"
zxfer_run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
zxfer_newsnap "tank/src@old"
EOF
	)
	: "$output"

	assertEquals "Non-recursive snapshots should omit the -r flag." \
		"snapshot tank/src@zxfer_single" "$(cat "$log")"
}

test_newsnap_builds_recursive_command_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_recursive.log"
	g_option_R_recursive="tank/src"
	g_zxfer_new_snapshot_name="zxfer_current"
	g_cmd_zfs="mock_zfs_tool"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	zxfer_newsnap "tank/src@old"

	unset -f zxfer_run_source_zfs_cmd

	assertEquals "Current-shell recursive snapshot generation should include the -r flag." \
		"snapshot -r tank/src@zxfer_current" "$(cat "$log")"
}

test_newsnap_builds_nonrecursive_command_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_single.log"
	g_option_R_recursive=""
	g_zxfer_new_snapshot_name="zxfer_current_single"
	g_cmd_zfs="mock_zfs_tool"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	zxfer_run_source_zfs_cmd() {
		printf '%s\n' "$*" >"$log"
	}

	zxfer_newsnap "tank/src@old"

	unset -f zxfer_run_source_zfs_cmd

	assertEquals "Current-shell non-recursive snapshot generation should omit the -r flag." \
		"snapshot tank/src@zxfer_current_single" "$(cat "$log")"
}

test_newsnap_dry_run_previews_in_current_shell() {
	log="$TEST_TMPDIR/newsnap_current_dry_run.log"
	: >"$log"
	g_option_n_dryrun=1
	g_option_v_verbose=1
	g_option_R_recursive="tank/src"
	g_zxfer_new_snapshot_name="zxfer_current_dry_run"
	g_cmd_zfs="mock_zfs_tool"
	# shellcheck source=src/zxfer_replication.sh
	. "$ZXFER_ROOT/src/zxfer_replication.sh"
	zxfer_echov() {
		printf '%s\n' "$*" >>"$log"
	}
	zxfer_run_source_zfs_cmd() {
		printf 'executed %s\n' "$*" >>"$log"
	}

	zxfer_newsnap "tank/src@old"

	zxfer_echov() {
		if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
			echo "$@"
		fi
	}
	unset -f zxfer_run_source_zfs_cmd

	assertContains "Current-shell dry-run snapshots should render the dry-run command preview." \
		"$(cat "$log")" "Dry run: 'mock_zfs_tool' 'snapshot' '-r' 'tank/src@zxfer_current_dry_run'"
	assertNotContains "Current-shell dry-run snapshots should not execute the source zfs command." \
		"$(cat "$log")" "executed "
}

test_newsnap_dry_run_previews_without_executing() {
	log="$TEST_TMPDIR/newsnap_dry_run.log"
	output=$(
		ZXFER_TEST_ROOT=$ZXFER_ROOT SNAPSHOT_LOG="$log" /bin/sh <<'EOF'
TESTS_DIR=$ZXFER_TEST_ROOT/tests
# shellcheck source=tests/test_helper.sh
. "$ZXFER_TEST_ROOT/tests/test_helper.sh"
zxfer_source_runtime_modules_through "zxfer_replication.sh" "$ZXFER_TEST_ROOT"
g_option_n_dryrun=1
g_option_v_verbose=1
g_option_V_very_verbose=0
g_option_b_beep_always=0
g_option_B_beep_on_success=0
g_option_R_recursive="tank/src"
g_zxfer_new_snapshot_name="zxfer_dry_run"
g_cmd_zfs="mock_zfs_tool"
zxfer_run_source_zfs_cmd() {
	printf '%s\n' "$*" >>"$SNAPSHOT_LOG"
}
zxfer_newsnap "tank/src@old"
EOF
	)

	l_snapshot_log=""
	if [ -f "$log" ]; then
		l_snapshot_log=$(cat "$log")
	fi
	assertEquals "Dry-run snapshots should not execute the source zfs command." "" "$l_snapshot_log"
	assertContains "Dry-run snapshots should render the snapshot command." \
		"$output" "Dry run: 'mock_zfs_tool' 'snapshot' '-r' 'tank/src@zxfer_dry_run'"
}

test_newsnap_names_the_snapshot_lazily_once_per_run() {
	log="$TEST_TMPDIR/newsnap_lazy_name.log"
	: >"$log"
	# The suite stubs zxfer_newsnap: load the real one in a subshell only.
	first_name=$(
		g_option_R_recursive=""
		g_zxfer_new_snapshot_name="zxfer_inherited"
		# shellcheck source=src/zxfer_replication.sh
		. "$ZXFER_ROOT/src/zxfer_replication.sh"
		# The session reset drops an inherited name.
		zxfer_reset_replication_runtime_state
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "$*" >>"$log"
		}
		zxfer_newsnap "tank/src" >/dev/null
		l_name=$g_zxfer_new_snapshot_name
		zxfer_newsnap "tank/src" >/dev/null
		printf '%s\n' "$l_name"
	)

	case $first_name in
	"zxfer_$$_"[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]) name_shape=ok ;;
	*) name_shape="unexpected <$first_name>" ;;
	esac
	assertEquals "The first snapshot should get the zxfer_PID_YYYYmmddHHMMSS name." ok "$name_shape"
	assertEquals "Later snapshots in the same run should reuse the name." \
		"snapshot tank/src@$first_name
snapshot tank/src@$first_name" "$(cat "$log")"
}

test_rollback_destination_to_last_common_snapshot_rolls_back_to_the_anchor() {
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_actual_dest="backup/target/src"
	g_last_common_snap="tank/src@snap1"
	log="$TEST_TMPDIR/rollback.log"
	: >"$log"

	(
		ROLLBACK_LOG="$log"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s %s %s\n' "$1" "$2" "$3" >>"$ROLLBACK_LOG"
			return 0
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	assertEquals "Rollback should target the destination snapshot matching the last common snapshot." \
		"rollback -r backup/target/src@snap1" "$(cat "$log")"
}

test_rollback_destination_to_last_common_snapshot_skips_when_not_needed() {
	log="$TEST_TMPDIR/rollback_skip.log"
	: >"$log"

	(
		ROLLBACK_LOG="$log"
		g_option_F_force_rollback=""
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=0
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=0
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap="tank/src@snap1"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=0
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	(
		ROLLBACK_LOG="$log"
		g_did_delete_dest_snapshots=1
		g_deleted_dest_newer_snapshots=1
		g_actual_dest="backup/target/src"
		g_last_common_snap=""
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$ROLLBACK_LOG"
		}
		zxfer_rollback_destination_to_last_common_snapshot
	)

	assertEquals "Rollback should no-op when -F is absent, deletions did not occur, deleted snapshots were not newer than the last common snapshot, destination is absent, or no common snapshot exists." \
		"" "$(cat "$log")"
}

test_rollback_destination_to_last_common_snapshot_reports_probe_failures() {
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_actual_dest="backup/target/src"
	g_last_common_snap="tank/src@snap1"

	set +e
	output=$(
		(
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/target/src] exists: ssh failure"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_rollback_destination_to_last_common_snapshot
		)
	)
	status=$?

	assertEquals "Rollback should fail closed when destination existence checks fail." 1 "$status"
	assertContains "Rollback should surface the destination probe failure." \
		"$output" "Failed to determine whether destination dataset [backup/target/src] exists: ssh failure"
}

test_rollback_destination_to_last_common_snapshot_reports_rollback_failures() {
	g_option_F_force_rollback="-F"
	g_did_delete_dest_snapshots=1
	g_deleted_dest_newer_snapshots=1
	g_actual_dest="backup/target/src"
	g_last_common_snap="tank/src@snap1"

	set +e
	output=$(
		(
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_rollback_destination_to_last_common_snapshot
		)
	)
	status=$?

	assertEquals "Rollback failures should abort instead of silently continuing." 1 "$status"
	assertContains "Rollback failures should identify the destination snapshot that could not be rolled back." \
		"$output" "Failed to roll back destination [backup/target/src] to backup/target/src@snap1 after deleting snapshots."
}
