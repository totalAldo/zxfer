#!/bin/sh
# Secure-path, transport policy, runtime cleanup, and consistency behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_get_path_owner_uid_falls_back_to_ls_for_dash_prefixed_paths() {
	result=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >"-owner_file"
			chmod 600 "./-owner_file"
			stat() {
				return 1
			}
			zxfer_get_path_owner_uid "-owner_file"
		)
	)

	assertEquals "LS fallback should recover the owner for dash-prefixed paths." "$(id -u)" "$result"
}

test_get_path_mode_octal_falls_back_to_ls_for_dash_prefixed_paths() {
	result=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >"-mode_file"
			chmod 600 "./-mode_file"
			stat() {
				return 1
			}
			ls() {
				printf '%s\n' "-rw------- 1 0 0 0 Jan 1 00:00 ./-mode_file"
			}
			zxfer_get_path_mode_octal "-mode_file"
		)
	)

	assertEquals "LS fallback should recover 0600 permissions for dash-prefixed paths." "600" "$result"
}

test_zxfer_apply_secure_path_exports_runtime_path() {
	result=$(
		(
			ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/sbin"
			ZXFER_SECURE_PATH_APPEND="/custom/bin"
			zxfer_apply_secure_path
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'runtime=%s\n' "$g_zxfer_runtime_path"
			printf 'path=%s\n' "$PATH"
		)
	)

	assertContains "zxfer_apply_secure_path should honor the configured secure PATH." \
		"$result" "secure=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Runtime PATH should now remain equal to the computed secure allowlist." \
		"$result" "runtime=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Exported PATH should match the computed runtime PATH." \
		"$result" "path=/opt/zfs/bin:/usr/sbin:/custom/bin"
}

test_ssh_supports_control_sockets_reflects_ssh_status() {
	g_cmd_ssh="$FAKE_SSH_BIN"

	FAKE_SSH_EXIT_STATUS=0
	export FAKE_SSH_EXIT_STATUS
	if zxfer_ssh_supports_control_sockets; then
		status_supported=0
	else
		status_supported=1
	fi

	FAKE_SSH_EXIT_STATUS=1
	export FAKE_SSH_EXIT_STATUS
	if zxfer_ssh_supports_control_sockets; then
		status_unsupported=0
	else
		status_unsupported=1
	fi

	unset FAKE_SSH_EXIT_STATUS

	assertEquals "zxfer_ssh_supports_control_sockets should succeed when ssh -M -V succeeds." 0 "$status_supported"
	assertEquals "zxfer_ssh_supports_control_sockets should fail when ssh -M -V fails." 1 "$status_unsupported"
}

test_get_ssh_transport_tokens_for_host_prefers_matching_control_socket() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	assertEquals "Origin host ssh transport tokens should reuse the origin control socket." \
		"$(printf '%s\n' "$FAKE_SSH_BIN" -o BatchMode=yes -o StrictHostKeyChecking=yes -S "$TEST_TMPDIR/origin.sock")" \
		"$(zxfer_get_ssh_transport_tokens_for_host "origin.example")"
	assertEquals "Target host ssh transport tokens should reuse the target control socket." \
		"$(printf '%s\n' "$FAKE_SSH_BIN" -o BatchMode=yes -o StrictHostKeyChecking=yes -S "$TEST_TMPDIR/target.sock")" \
		"$(zxfer_get_ssh_transport_tokens_for_host "target.example")"
	assertEquals "Unmatched hosts should use the base ssh transport tokens." \
		"$(printf '%s\n' "$FAKE_SSH_BIN" -o BatchMode=yes -o StrictHostKeyChecking=yes)" \
		"$(zxfer_get_ssh_transport_tokens_for_host "other.example")"
}

test_echoV_ssh_control_socket_command_for_host_renders_only_when_very_verbose() {
	quiet_output=$(
		(
			g_option_V_very_verbose=0
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_echoV_ssh_control_socket_command_for_host \
				"other.example" "Checking ssh control socket" /bin/echo probe
		)
	)
	verbose_output=$(
		(
			g_option_V_very_verbose=1
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_echoV_ssh_control_socket_command_for_host \
				"other.example" "Checking ssh control socket" /bin/echo probe
		)
	)

	assertEquals "Quiet runs should not render ssh control socket commands for display." \
		"" "$quiet_output"
	assertEquals "Very-verbose runs should keep the current control-socket operator line text." \
		"Checking ssh control socket [remote: other.example]: '/bin/echo' 'probe'" \
		"$verbose_output"
}

test_setup_ssh_control_socket_propagates_transport_policy_validation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "ssh control socket setup should propagate the underlying ssh policy validation message instead of a generic cache-dir error." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
	assertNotContains "ssh control socket setup should not mask transport-policy validation failures behind the generic tempdir message." \
		"$output" "Error creating temporary directory for ssh control socket."
}

test_trap_exit_relaunches_services_when_requested() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_restore_migration_services_status_only() {
				printf 'restore_migration_services_status_only need=%s\n' "$g_services_need_relaunch"
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when cleanup zxfer_relaunch succeeds." 0 "$status"
	assertContains "zxfer_trap_exit should log that it is restarting stopped services." \
		"$output" "zxfer exiting early; restarting stopped services."
	assertContains "zxfer_trap_exit should invoke the status-only migration restore operation when services are still marked for restart." \
		"$output" "restore_migration_services_status_only need=1"
}

test_trap_exit_skips_relaunch_when_relaunch_is_already_in_progress() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=1
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_restore_migration_services_status_only() {
				printf 'restore-migration-services-called\n'
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when zxfer_relaunch already failed earlier." 0 "$status"
	assertContains "zxfer_trap_exit should log that it is preserving stopped-service state after a failed zxfer_relaunch attempt." \
		"$output" "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
	assertNotContains "zxfer_trap_exit should not retry migration restoration while a failed zxfer_relaunch attempt is already in progress." \
		"$output" "restore-migration-services-called"
}

test_trap_exit_has_loaded_migration_relaunch_dependency() {
	assertTrue "Full session loading should preserve the ordinary migration-service relaunch operation." \
		"command -v zxfer_relaunch >/dev/null 2>&1"
	assertTrue "Full session loading should provide the status-only migration restore dependency used by trap exit." \
		"command -v zxfer_restore_migration_services_status_only >/dev/null 2>&1"
}

test_trap_exit_removes_run_root_dirs_with_legacy_like_entries() {
	g_zxfer_temp_prefix="zxfer.trap-cleanup"
	trap_root_file="$TEST_TMPDIR/trap-cleanup-run-root"

	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=0
			zxfer_ensure_run_tmp_root || exit 90
			printf '%s\n' "$g_zxfer_run_tmp_root" >"$trap_root_file"
			fake_root="$g_zxfer_run_tmp_root/fake-root"
			mkdir -p "$fake_root/entry/leases" "$fake_root/cache.lock" || exit 91
			chmod 700 "$fake_root/cache.lock" || exit 92
			printf '%s\n' "$$" >"$fake_root/cache.lock/pid" || exit 93
			chmod 600 "$fake_root/cache.lock/pid" || exit 94
			: >"$fake_root/entry/leases/lease.legacy"
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?
	run_tmp_root=$(cat "$trap_root_file")

	assertEquals "zxfer_trap_exit should preserve a successful exit status while removing run-root directories with legacy-like child names." \
		0 "$status"
	assertFalse "zxfer_trap_exit should remove everything under the per-run temp root even when entries contain lease-like or pid-lock-like names." \
		"[ -e '$run_tmp_root' ]"
}

test_zxfer_check_ssh_control_socket_for_host_classifies_stale_master_failures() {
	FAKE_SSH_EXIT_STATUS=255
	FAKE_SSH_STDERR="Control socket connect($TEST_TMPDIR/check.sock): No such file or directory"
	export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
	g_cmd_ssh="$FAKE_SSH_BIN"

	if zxfer_check_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/check.sock"; then
		status=0
	else
		status=$?
	fi

	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR

	assertEquals "Control-socket checks should still return nonzero when the master is stale." 1 "$status"
	assertEquals "Control-socket checks should classify missing masters separately from transport failures." \
		"stale" "$g_zxfer_ssh_control_socket_action_result"
	assertContains "Control-socket checks should preserve the stale-master diagnostic for callers." \
		"$g_zxfer_ssh_control_socket_action_stderr" "No such file or directory"
}

test_zxfer_check_ssh_control_socket_for_host_preserves_transport_failure_diagnostics() {
	FAKE_SSH_EXIT_STATUS=255
	FAKE_SSH_STDERR="Host key verification failed."
	export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
	g_cmd_ssh="$FAKE_SSH_BIN"

	if zxfer_check_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/check.sock"; then
		status=0
	else
		status=$?
	fi

	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR

	assertEquals "Control-socket checks should fail when ssh transport setup fails." 1 "$status"
	assertEquals "Control-socket checks should classify ssh transport failures distinctly from stale masters." \
		"error" "$g_zxfer_ssh_control_socket_action_result"
	assertContains "Control-socket checks should preserve ssh transport stderr for the caller." \
		"$g_zxfer_ssh_control_socket_action_stderr" "Host key verification failed."
}

test_zxfer_check_ssh_control_socket_for_host_reports_stderr_capture_failures() {
	set +e
	output=$(
		(
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Host key verification failed."
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"
			zxfer_read_ssh_control_socket_action_stderr_file() {
				return 1
			}

			if zxfer_check_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/check.sock"; then
				l_status=0
			else
				l_status=$?
			fi

			printf 'status=%s\n' "$l_status"
			printf 'result=%s\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'stderr=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)
	status=$?

	assertEquals "Control-socket capture-failure probes should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Control-socket checks should fail closed when staged stderr cannot be reloaded." \
		"$output" "status=1"
	assertContains "Control-socket checks should classify staged stderr reload failures distinctly." \
		"$output" "result=capture_error"
	assertContains "Control-socket checks should preserve a specific capture-failure diagnostic." \
		"$output" "stderr=Failed to read ssh control socket stderr for check action."
}

test_zxfer_check_ssh_control_socket_for_host_reports_stderr_stage_failures() {
	set +e
	output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			zxfer_get_temp_file() {
				return 73
			}

			if zxfer_check_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/check.sock"; then
				l_status=0
			else
				l_status=$?
			fi

			printf 'status=%s\n' "$l_status"
			printf 'result=%s\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'stderr=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)
	status=$?
	set -e

	assertEquals "Control-socket stderr-stage failures should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Control-socket checks should preserve the exact stderr-stage allocation failure status." \
		"$output" "status=73"
	assertContains "Control-socket checks should classify stderr-stage allocation failures as capture errors." \
		"$output" "result=capture_error"
	assertContains "Control-socket checks should preserve a specific stderr-stage failure diagnostic." \
		"$output" "stderr=Failed to stage ssh control socket stderr for check action."
}

test_zxfer_close_all_ssh_control_sockets_prefers_origin_failure_and_uses_target_failure_when_origin_succeeds() {
	set +e
	output=$(
		(
			zxfer_close_origin_ssh_control_socket() {
				return 7
			}
			zxfer_close_target_ssh_control_socket() {
				return 9
			}

			set +e
			zxfer_close_all_ssh_control_sockets
			printf 'origin_failure_status=%s\n' "$?"

			zxfer_close_origin_ssh_control_socket() {
				return 0
			}
			zxfer_close_target_ssh_control_socket() {
				return 9
			}

			zxfer_close_all_ssh_control_sockets
			printf 'target_failure_status=%s\n' "$?"
		)
	)
	set -e

	assertContains "close-all socket cleanup should preserve the origin close status when origin cleanup fails first." \
		"$output" "origin_failure_status=7"
	assertContains "close-all socket cleanup should propagate the target close status when origin cleanup succeeds." \
		"$output" "target_failure_status=9"
}

test_consistency_check_rejects_backup_and_restore_modes_together() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_k_backup_property_mode=1
			g_option_e_restore_property_mode=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Backup and restore mode conflicts should fail validation." 2 "$status"
	assertContains "Backup and restore mode conflicts should use the documented error." \
		"$output" "You cannot bac(k)up and r(e)store properties at the same time."
}

test_consistency_check_rejects_dual_beep_modes() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_b_beep_always=1
			g_option_B_beep_on_success=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Conflicting beep modes should fail validation." 2 "$status"
	assertContains "Conflicting beep modes should use the documented error." \
		"$output" "You cannot use both beep modes at the same time."
}

test_consistency_check_rejects_invalid_grandfather_values() {
	set +e
	output_non_numeric=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="abc"
			zxfer_consistency_check
		)
	)
	status_non_numeric=$?

	output_zero=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="0"
			zxfer_consistency_check
		)
	)
	status_zero=$?

	assertEquals "Non-numeric grandfather values should fail validation." 2 "$status_non_numeric"
	assertContains "Non-numeric grandfather errors should mention the received value." \
		"$output_non_numeric" "grandfather protection requires a positive integer; received \"abc\"."
	assertEquals "Zero-day grandfather values should fail validation." 2 "$status_zero"
	assertContains "Zero-day grandfather errors should require days greater than zero." \
		"$output_zero" "grandfather protection requires days greater than 0; received \"0\"."
}
