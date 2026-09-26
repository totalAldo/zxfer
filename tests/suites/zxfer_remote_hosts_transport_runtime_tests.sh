#!/bin/sh
# Secure-path, transport policy, runtime cleanup, and consistency behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

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

test_select_ssh_control_socket_prefers_matching_control_socket() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	zxfer_select_ssh_control_socket "origin.example"
	assertEquals "The origin host should reuse the origin control socket." \
		"$TEST_TMPDIR/origin.sock" "$g_zxfer_ssh_control_socket_result"
	zxfer_select_ssh_control_socket "target.example"
	assertEquals "The target host should reuse the target control socket." \
		"$TEST_TMPDIR/target.sock" "$g_zxfer_ssh_control_socket_result"
	zxfer_select_ssh_control_socket "other.example"
	assertEquals "Unmatched hosts should use no control socket." \
		"" "$g_zxfer_ssh_control_socket_result"
	zxfer_select_ssh_control_socket ""
	assertEquals "An empty host spec should use no control socket." \
		"" "$g_zxfer_ssh_control_socket_result"
}

test_ssh_control_socket_open_renders_only_when_very_verbose() {
	quiet_output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_V_very_verbose=0
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_run_ssh_control_socket_action open \
				"other.example" "$TEST_TMPDIR/open.sock"
			wait "$g_zxfer_ssh_control_socket_action_pid"
		)
	)
	verbose_output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_V_very_verbose=1
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_run_ssh_control_socket_action open \
				"other.example" "$TEST_TMPDIR/open.sock"
			wait "$g_zxfer_ssh_control_socket_action_pid"
		)
	)

	assertEquals "Quiet runs should not render ssh control socket commands for display." \
		"" "$quiet_output"
	assertEquals "Very-verbose runs should keep the current control-socket operator line text." \
		"Opening ssh control socket [remote: other.example]: '$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-M' '-S' '$TEST_TMPDIR/open.sock' '-fN' 'other.example'" \
		"$verbose_output"
}

test_open_ssh_control_sockets_propagates_transport_policy_validation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_supports_control_sockets=1
			g_option_O_origin_host="origin.example"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_open_ssh_control_sockets
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

test_ssh_control_socket_exit_classifies_closed_stale_and_error_results() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	socket="$TEST_TMPDIR/check.sock"
	stale_stderr="Control socket connect($socket): No such file or directory"
	results=""
	for action_case in \
		"exit|0||closed" \
		"exit|255|$stale_stderr|stale" \
		"exit|255|Host key verification failed.|error"; do
		action=${action_case%%|*}
		action_rest=${action_case#*|}
		FAKE_SSH_EXIT_STATUS=${action_rest%%|*}
		action_rest=${action_rest#*|}
		FAKE_SSH_STDERR=${action_rest%%|*}
		export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
		zxfer_run_ssh_control_socket_action "$action" "origin.example" "$socket"
		results="$results$action:$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr
"
	done
	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR

	assertEquals "A control-socket close should classify each ssh outcome and keep its stderr for the caller." \
		"exit:0:closed:
exit:1:stale:$stale_stderr
exit:1:error:Host key verification failed.
" "$results"
}

test_ssh_control_socket_action_reports_policy_and_host_spec_failures_as_errors() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
	zxfer_run_ssh_control_socket_action open "origin.example" "$TEST_TMPDIR/check.sock"
	policy_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr:$g_zxfer_ssh_control_socket_action_pid"
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	zxfer_run_ssh_control_socket_action exit 'origin.example "doas"' "$TEST_TMPDIR/check.sock"
	host_result="$?:$g_zxfer_ssh_control_socket_action_result:$g_zxfer_ssh_control_socket_action_stderr"
	zxfer_run_ssh_control_socket_action check "origin.example" "$TEST_TMPDIR/check.sock"
	check_status=$?

	assertEquals "An invalid ssh policy should be an action error with its diagnostic and start no ssh." \
		"1:error:ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path.:" "$policy_result"
	assertEquals "A host spec that needs shell quoting should be an action error with its diagnostic." \
		"1:error:Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported." \
		"$host_result"
	assertEquals "An unknown action, including the retired check, should fail closed." 1 "$check_status"
}

test_zxfer_close_all_ssh_control_sockets_prefers_origin_failure_and_uses_target_failure_when_origin_succeeds() {
	set +e
	output=$(
		(
			zxfer_close_ssh_control_socket_for_role() {
				[ "$1" != origin ] || return 7
				return 9
			}

			set +e
			zxfer_close_all_ssh_control_sockets
			printf 'origin_failure_status=%s\n' "$?"

			zxfer_close_ssh_control_socket_for_role() {
				[ "$1" != origin ] || return 0
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
