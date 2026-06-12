#!/bin/sh
#
# Additional shunit2 coverage for per-run ssh control-socket and remote-host
# action/tool-resolution error branches.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_replication.sh"

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

create_fake_ssh_bin() {
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
if [ -n "${FAKE_SSH_STDOUT:-}" ] && [ -z "${FAKE_SSH_SUPPRESS_STDOUT:-}" ]; then
	printf '%s' "$FAKE_SSH_STDOUT"
fi
if [ -n "${FAKE_SSH_STDERR:-}" ]; then
	printf '%s' "$FAKE_SSH_STDERR" >&2
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts_coverage"
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rhc.XXXXXX) || {
		echo "Unable to create private remote-host coverage temp root." >&2
		exit 1
	}
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	mkdir -p "$TEST_PRIVATE_DEFAULT_TMPDIR"
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset FAKE_SSH_STDOUT
	unset FAKE_SSH_STDERR
	unset FAKE_SSH_SUPPRESS_STDOUT
	unset ZXFER_SSH_BATCH_MODE
	unset ZXFER_SSH_STRICT_HOST_KEY_CHECKING
	unset ZXFER_SSH_USER_KNOWN_HOSTS_FILE
	unset ZXFER_SSH_USE_AMBIENT_CONFIG
	unset ZXFER_SECURE_PATH
	unset ZXFER_SECURE_PATH_APPEND
	TMPDIR="$TEST_TMPDIR"
	zxfer_list_default_tmpdir_candidates() {
		printf '%s\n' "$TEST_PRIVATE_DEFAULT_TMPDIR"
	}
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_Y_yield_iterations=1
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	zxfer_init_temp_artifacts
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	if command -v zxfer_reset_owned_lock_tracking >/dev/null 2>&1; then
		zxfer_reset_owned_lock_tracking
	fi
	create_fake_ssh_bin
}

test_zxfer_ssh_control_socket_action_failure_helpers_cover_stale_classification_and_output() {
	zxfer_reset_ssh_control_socket_action_state
	blank_output=$(zxfer_emit_ssh_control_socket_action_failure_message)
	blank_status=$?
	default_output=$(zxfer_emit_ssh_control_socket_action_failure_message "default action failure.")
	default_status=$?
	g_zxfer_ssh_control_socket_action_stderr="staged action failure"
	staged_output=$(zxfer_emit_ssh_control_socket_action_failure_message "ignored default")
	staged_status=$?

	classification_output=$(
		(
			set +e
			zxfer_ssh_control_socket_failure_is_stale_master \
				"Control socket connect($TEST_TMPDIR/check.sock): No such file or directory"
			printf 'missing=%s\n' "$?"
			zxfer_ssh_control_socket_failure_is_stale_master \
				"Control socket connect($TEST_TMPDIR/check.sock): Broken pipe"
			printf 'broken_pipe=%s\n' "$?"
			zxfer_ssh_control_socket_failure_is_stale_master \
				"Host key verification failed."
			printf 'other=%s\n' "$?"
		)
	)

	assertEquals "ssh control socket action failure message emission should stay silent when neither a staged nor a default message is present." \
		"" "$blank_output"
	assertEquals "ssh control socket action failure message emission should still succeed when no message is emitted." \
		0 "$blank_status"
	assertEquals "ssh control socket action failure message emission should print the default message when no staged stderr is present." \
		"default action failure." "$default_output"
	assertEquals "ssh control socket action failure message emission should succeed when printing the default action message." \
		0 "$default_status"
	assertEquals "ssh control socket action failure message emission should prefer the staged stderr over the default message." \
		"staged action failure" "$staged_output"
	assertEquals "ssh control socket action failure message emission should succeed when printing the staged stderr." \
		0 "$staged_status"
	assertContains "ssh control socket stale-master detection should classify missing control sockets as stale masters." \
		"$classification_output" "missing=0"
	assertContains "ssh control socket stale-master detection should classify broken pipes as stale masters." \
		"$classification_output" "broken_pipe=0"
	assertContains "ssh control socket stale-master detection should not classify unrelated transport failures as stale masters." \
		"$classification_output" "other=1"
}

test_zxfer_read_ssh_control_socket_action_stderr_file_trims_trailing_newline_and_preserves_read_failures() {
	stderr_path="$TEST_TMPDIR/ssh_action.stderr"
	printf '%s\n' "control socket failed" >"$stderr_path" ||
		fail "Unable to write ssh action stderr fixture."

	success_output=$(
		(
			set +e
			zxfer_read_ssh_control_socket_action_stderr_file "$stderr_path"
			printf 'status=%s\n' "$?"
			printf 'stored=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)
	read_failure_output=$(
		(
			set +e
			zxfer_read_runtime_artifact_file() {
				return 73
			}
			zxfer_read_ssh_control_socket_action_stderr_file "$stderr_path" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'stored=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)

	assertContains "ssh control socket action stderr reloads should succeed for readable staged stderr files." \
		"$success_output" "status=0"
	assertContains "ssh control socket action stderr reloads should trim a single trailing newline before storing the staged stderr." \
		"$success_output" "stored=control socket failed"
	assertContains "ssh control socket action stderr reloads should preserve runtime-artifact read failure statuses." \
		"$read_failure_output" "status=73"
	assertContains "ssh control socket action stderr reloads should clear staged stderr when the runtime-artifact read fails." \
		"$read_failure_output" "stored="
}

test_close_origin_and_target_ssh_control_socket_return_early_without_state() {
	output=$(
		(
			zxfer_close_origin_ssh_control_socket
			printf 'origin=%s\n' "$?"
			zxfer_close_target_ssh_control_socket
			printf 'target=%s\n' "$?"
		)
	)

	assertContains "Origin ssh control socket close should return early without state." \
		"$output" "origin=0"
	assertContains "Target ssh control socket close should return early without state." \
		"$output" "target=0"
}

test_zxfer_ssh_action_and_remote_tool_resolution_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_action_tool_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			zxfer_run_ssh_control_socket_action_for_host "user@example" "$branch_root/s" bogus >/dev/null
			printf 'action_invalid_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "bad host"
				return 1
			}
			zxfer_run_ssh_control_socket_action_for_host "bad host" "$branch_root/s" check >/dev/null
			printf 'action_split_status=%s\n' "$?"
			printf 'action_split_result=%s\n' "${g_zxfer_ssh_control_socket_action_result:-}"
			printf 'action_split_stderr=%s\n' "${g_zxfer_ssh_control_socket_action_stderr:-}"
		)
		(
			set +e
			action_stderr="$branch_root/action-check.err"
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_split_host_spec_tokens() {
				return 0
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$action_stderr
				: >"$action_stderr"
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host "user@example" "$branch_root/s" check >/dev/null
			printf 'action_check_status=%s\n' "$?"
			printf 'action_check_result=%s\n' "${g_zxfer_ssh_control_socket_action_result:-}"
		)
		(
			set +e
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "transport failure"
				return 7
			}
			zxfer_throw_error() {
				printf 'open_transport_throw=%s:%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			(
				zxfer_open_ssh_control_socket_for_host "user@example" "$branch_root/s"
			)
			printf 'open_transport_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "split failure"
				return 1
			}
			zxfer_throw_error() {
				printf 'open_split_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_open_ssh_control_socket_for_host "bad host" "$branch_root/s"
			)
			printf 'open_split_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_get_remote_capability_requested_tools_for_resolved_tool() {
				printf '%s\n' zfs
			}
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' /sbin/zfs
				return 0
			}
			resolved=$(zxfer_resolve_remote_required_tool "user@example" zfs ZFS source)
			printf 'resolve_ensure_fallback_status=%s\n' "$?"
			printf 'resolve_ensure_fallback=%s\n' "$resolved"
		)
		(
			set +e
			zxfer_get_remote_capability_requested_tools_for_resolved_tool() {
				printf '%s\n' zfs
			}
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2" "os	Linux"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' /sbin/zfs
				return 0
			}
			resolved=$(zxfer_resolve_remote_required_tool "user@example" zfs ZFS source)
			printf 'resolve_missing_tool_fallback_status=%s\n' "$?"
			printf 'resolve_missing_tool_fallback=%s\n' "$resolved"
		)
		(
			set +e
			zxfer_get_remote_capability_requested_tools_for_resolved_tool() {
				printf '%s\n' tar
			}
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2" "os	Linux"
			}
			zxfer_parse_remote_capability_response() {
				l_tool=tar
				return 0
			}
			resolved=$(zxfer_resolve_remote_required_tool "user@example" tar TAR source)
			printf 'resolve_unknown_status=%s\n' "$?"
			printf 'resolve_unknown=%s\n' "$resolved"
		)
	)

	assertContains "Invalid ssh control socket actions should be rejected before execution." \
		"$output" "action_invalid_status=1"
	assertContains "Host token split failures should be surfaced as ssh action errors." \
		"$output" "action_split_status=1"
	assertContains "Host token split failures should mark the action as an error." \
		"$output" "action_split_result=error"
	assertContains "Host token split failures should preserve the split diagnostic." \
		"$output" "action_split_stderr=bad host"
	assertContains "Successful ssh control socket checks should mark the socket live." \
		"$output" "action_check_status=0"
	assertContains "Successful ssh control socket checks should record a live result." \
		"$output" "action_check_result=live"
	assertContains "Open-socket transport failures should be routed through throw_error." \
		"$output" "open_transport_throw=transport failure:7"
	assertContains "Open-socket host token failures should be routed through throw_error." \
		"$output" "open_split_throw=split failure"
	assertContains "Remote tool resolution should fall back to the direct probe when capability bootstrap fails." \
		"$output" "resolve_ensure_fallback_status=0"
	assertContains "Remote tool resolution should preserve the direct fallback path after capability bootstrap failure." \
		"$output" "resolve_ensure_fallback=/sbin/zfs"
	assertContains "Remote tool resolution should fall back to the direct probe when parsed capabilities omit a supported tool." \
		"$output" "resolve_missing_tool_fallback_status=0"
	assertContains "Remote tool resolution should preserve the direct fallback path for omitted supported tools." \
		"$output" "resolve_missing_tool_fallback=/sbin/zfs"
	assertContains "Remote tool resolution should fail closed for unsupported tool labels." \
		"$output" "resolve_unknown_status=1"
	assertContains "Remote tool resolution should preserve the unsupported tool diagnostic." \
		"$output" "Failed to query dependency \"TAR\" on host user@example."
}

test_zxfer_ssh_setup_and_close_error_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_setup_close_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/close-error.sock"
			: >"$g_ssh_origin_control_socket"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="exit action failed"
				g_zxfer_ssh_control_socket_action_command="exit $1 $2"
				return 1
			}
			zxfer_close_origin_ssh_control_socket 2>"$branch_root/close-error.err"
			printf 'close_error_status=%s\n' "$?"
			printf 'close_error_err=%s\n' "$(cat "$branch_root/close-error.err")"
			printf 'close_error_state=%s\n' "$g_ssh_origin_control_socket"
			if [ -e "$branch_root/close-error.sock" ]; then
				printf 'close_error_socket=kept\n'
			else
				printf 'close_error_socket=removed\n'
			fi
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/close-stale.sock"
			: >"$g_ssh_origin_control_socket"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result=stale
				g_zxfer_ssh_control_socket_action_command="exit $1 $2"
				return 1
			}
			zxfer_close_origin_ssh_control_socket 2>"$branch_root/close-stale.err"
			printf 'close_stale_status=%s\n' "$?"
			printf 'close_stale_state=<%s>\n' "$g_ssh_origin_control_socket"
			if [ -e "$branch_root/close-stale.sock" ]; then
				printf 'close_stale_socket=kept\n'
			else
				printf 'close_stale_socket=removed\n'
			fi
		)
		(
			set +e
			control_socket="$branch_root/setup-check-error.sock"
			: >"$control_socket"
			zxfer_ensure_ssh_control_socket_dir() {
				g_zxfer_ssh_control_socket_dir_result=$branch_root
				printf '%s\n' "$branch_root"
			}
			zxfer_get_ssh_control_socket_path_for_role() {
				printf '%s\n' "$control_socket"
			}
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="check failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'setup_check_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_setup_ssh_control_socket "origin.example" origin
			) 2>"$branch_root/setup-check-error.err"
			printf 'setup_check_status=%s\n' "$?"
			printf 'setup_check_err=%s\n' "$(cat "$branch_root/setup-check-error.err")"
		)
		(
			set +e
			zxfer_ensure_ssh_control_socket_dir() {
				return 1
			}
			zxfer_throw_error() {
				printf 'setup_dir_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_setup_ssh_control_socket "origin.example" origin
			)
			printf 'setup_dir_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_ensure_ssh_control_socket_dir() {
				g_zxfer_ssh_control_socket_dir_result=$branch_root
				printf '%s\n' "$branch_root"
			}
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "transport policy failure"
				return 7
			}
			zxfer_throw_error() {
				printf 'setup_transport_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_setup_ssh_control_socket "origin.example" origin
			)
			printf 'setup_transport_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_ensure_ssh_control_socket_dir() {
				g_zxfer_ssh_control_socket_dir_result=$branch_root
				printf '%s\n' "$branch_root"
			}
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_open_ssh_control_socket_for_host() {
				return 1
			}
			zxfer_throw_error() {
				printf 'setup_open_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_setup_ssh_control_socket "origin.example" origin
			)
			printf 'setup_open_status=%s\n' "$?"
		)
		(
			set +e
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$branch_root/replace-target.sock"
			zxfer_close_target_ssh_control_socket() {
				return 1
			}
			zxfer_throw_error() {
				printf 'setup_close_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_setup_ssh_control_socket "target.example" target
			)
			printf 'setup_close_status=%s\n' "$?"
		)
	)

	assertContains "Socket close should fail closed when the exit action reports a non-stale error." \
		"$output" "close_error_status=1"
	assertContains "Socket close should surface the exit action diagnostic." \
		"$output" "close_error_err=exit action failed"
	assertContains "Socket close should preserve the role state when the exit action fails." \
		"$output" "close_error_state=$branch_root/close-error.sock"
	assertContains "Socket close should keep the socket path for trap-time retry when the exit action fails." \
		"$output" "close_error_socket=kept"
	assertContains "Socket close should treat a stale master as already closed." \
		"$output" "close_stale_status=0"
	assertContains "Socket close should clear the role state after a stale master." \
		"$output" "close_stale_state=<>"
	assertContains "Socket close should remove the stale socket path." \
		"$output" "close_stale_socket=removed"
	assertContains "Setup should route non-stale check errors through throw_error." \
		"$output" "setup_check_throw=Error creating ssh control socket for origin host."
	assertContains "Setup should surface the check diagnostic before throwing." \
		"$output" "setup_check_err=check failed"
	assertContains "Setup should fail closed when the per-run socket directory cannot be created." \
		"$output" "setup_dir_throw=Error creating temporary directory for ssh control socket."
	assertContains "Setup should route transport policy failures through throw_error." \
		"$output" "setup_transport_throw=transport policy failure"
	assertContains "Setup should route master open failures through throw_error." \
		"$output" "setup_open_throw=Error creating ssh control socket for origin host."
	assertContains "Setup should fail closed when an existing role socket cannot be closed first." \
		"$output" "setup_close_throw=Error closing ssh control socket for target host."
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
