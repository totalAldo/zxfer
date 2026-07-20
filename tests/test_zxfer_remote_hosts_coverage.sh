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
# shellcheck source=tests/helpers/fake_tool_fixtures.sh
. "$TESTS_DIR/helpers/fake_tool_fixtures.sh"

zxfer_source_runtime_modules_through "zxfer_replication.sh"

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts_coverage"
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rhc.XXXXXX) || {
		echo "Unable to create private remote-host coverage temp root." >&2
		exit 1
	}
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
}

oneTimeTearDown() {
	rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"
	zxfer_test_cleanup_tmpdir
}

reset_remote_hosts_coverage_environment() {
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
}

reset_remote_hosts_coverage_options() {
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_Y_yield_iterations=1
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
}

reset_remote_hosts_coverage_runtime_state() {
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
}

setUp() {
	zxfer_source_runtime_modules_through "zxfer_replication.sh"
	reset_remote_hosts_coverage_environment
	reset_remote_hosts_coverage_options
	reset_remote_hosts_coverage_runtime_state
	zxfer_test_write_env_fake_ssh "$FAKE_SSH_BIN"
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

test_zxfer_ssh_transport_directory_and_render_failure_branches_fail_closed() {
	branch_root="$TEST_TMPDIR/ssh_transport_directory_branch_coverage"
	mkdir -p "$branch_root/private"
	chmod 700 "$branch_root/private"

	output=$(
		(
			set +e
			zxfer_split_host_spec_tokens() {
				printf '%s\n' "invalid host specification"
				return 7
			}
			quoted=$(zxfer_quote_host_spec_tokens "invalid host")
			printf 'quote_status=%s\n' "$?"
			printf 'quote_output=%s\n' "$quoted"
		)
		(
			set +e
			zxfer_runtime_artifact_path_is_registered() {
				return 0
			}
			zxfer_get_registered_runtime_artifact_directory_identity() {
				g_zxfer_runtime_artifact_directory_identity_result="device:inode"
				return 0
			}
			zxfer_get_path_device_inode() {
				printf '%s\n' "device:inode"
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 501
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 501
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ssh_control_socket_dir_is_current_private "$branch_root/private"
			printf 'private_status=%s\n' "$?"
		)
		(
			set +e
			g_zxfer_ssh_control_socket_dir_result=""
			g_zxfer_run_tmp_root="$branch_root/long-run-root"
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_is_ssh_control_socket_path_short_enough() {
				return 1
			}
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_create_unpredictable_staging_entry() {
				return 71
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'create_status=%s\n' "$?"
		)
		(
			set +e
			g_zxfer_ssh_control_socket_dir_result=""
			g_zxfer_run_tmp_root="$branch_root/long-run-root"
			unregistered_dir="$branch_root/unregistered"
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_is_ssh_control_socket_path_short_enough() {
				if [ "${1#"$g_zxfer_run_tmp_root"/}" != "$1" ]; then
					return 1
				fi
				return 0
			}
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_create_unpredictable_staging_entry() {
				mkdir "$unregistered_dir" || return "$?"
				printf '%s\n' "$unregistered_dir"
			}
			zxfer_register_runtime_artifact_path() {
				return 72
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'register_status=%s\n' "$?"
			if [ -e "$unregistered_dir" ]; then
				printf '%s\n' 'register_cleanup=kept'
			else
				printf '%s\n' 'register_cleanup=removed'
			fi
		)
		(
			set +e
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "transport policy failure"
				return 73
			}
			zxfer_run_ssh_control_socket_action_for_host \
				"user@example" "$branch_root/action.sock" check >/dev/null
			printf 'action_status=%s\n' "$?"
			printf 'action_result=%s\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'action_stderr=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)

	assertContains "Host-spec quoting should preserve splitter failures." \
		"$output" "quote_status=1"
	assertContains "Host-spec quoting should retain the splitter diagnostic." \
		"$output" "quote_output=invalid host specification"
	assertContains "Registered private socket directories should pass all identity, owner, and mode checks." \
		"$output" "private_status=0"
	assertContains "Short socket-directory staging failures should fail closed." \
		"$output" "create_status=1"
	assertContains "Runtime artifact registration failures should fail closed." \
		"$output" "register_status=1"
	assertContains "Unregistered socket directories should be removed immediately." \
		"$output" "register_cleanup=removed"
	assertContains "SSH action transport-policy failures should fail closed." \
		"$output" "action_status=1"
	assertContains "SSH action transport-policy failures should publish an error result." \
		"$output" "action_result=error"
	assertContains "SSH action transport-policy failures should preserve the diagnostic." \
		"$output" "action_stderr=transport policy failure"
}

test_zxfer_ssh_transport_owner_guards_cover_setup_and_bootstrap_failures() {
	branch_root="$TEST_TMPDIR/ssh_transport_owner_guard_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			g_ssh_origin_control_socket="$branch_root/origin.sock"
			zxfer_close_origin_ssh_control_socket() {
				return 76
			}
			zxfer_throw_error() {
				printf 'origin_close_throw=%s\n' "$1"
				exit 9
			}
			zxfer_setup_ssh_control_socket origin.example origin
			printf 'origin_close_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_ensure_ssh_control_socket_dir() {
				return 0
			}
			zxfer_get_ssh_control_socket_path_for_role() {
				return 77
			}
			zxfer_throw_error() {
				printf 'socket_path_throw=%s\n' "$1"
				exit 9
			}
			zxfer_setup_ssh_control_socket origin.example origin
			printf 'socket_path_status=%s\n' "$?"
		)
		(
			set +e
			stale_socket="$branch_root/stale.sock"
			: >"$stale_socket"
			zxfer_ensure_ssh_control_socket_dir() {
				return 0
			}
			zxfer_get_ssh_control_socket_path_for_role() {
				printf '%s\n' "$stale_socket"
			}
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result=stale
				return 1
			}
			zxfer_open_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_set_ssh_control_socket_role_state() {
				return 0
			}
			zxfer_setup_ssh_control_socket origin.example origin
			printf 'stale_setup_status=%s\n' "$?"
			if [ -e "$stale_socket" ]; then
				printf '%s\n' 'stale_socket=kept'
			else
				printf '%s\n' 'stale_socket=removed'
			fi
		)
		(
			set +e
			zxfer_close_ssh_control_socket_for_role invalid
			printf 'close_invalid_role_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host=origin.example
			g_option_T_target_host=""
			g_option_n_dryrun=1
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			printf 'prepare_dryrun_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host=origin.example
			g_option_T_target_host=""
			g_option_n_dryrun=0
			g_cmd_ssh=""
			zxfer_profile_metrics_enabled() {
				return 1
			}
			zxfer_ensure_local_ssh_command() {
				g_zxfer_resolved_local_ssh_command_result="ssh dependency missing"
				return 78
			}
			zxfer_set_failure_class() {
				g_zxfer_failure_class=$1
			}
			zxfer_throw_error() {
				printf 'prepare_dependency_class=%s\n' "$g_zxfer_failure_class"
				printf 'prepare_dependency_throw=%s\n' "$1"
				exit 9
			}
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			printf 'prepare_dependency_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host=""
			g_option_T_target_host="invalid target"
			zxfer_quote_host_spec_tokens() {
				printf '%s\n' "target host rejected"
				return 1
			}
			zxfer_throw_usage_error() {
				printf 'target_usage_throw=%s\n' "$1"
				exit "$2"
			}
			zxfer_refresh_remote_zfs_commands
			printf 'target_usage_status=%s\n' "$?"
		)
	)

	assertContains "Replacing an origin socket should fail closed when the old socket cannot be closed." \
		"$output" "origin_close_throw=Error closing ssh control socket for origin host."
	assertContains "Socket setup should reject a role path that cannot be resolved." \
		"$output" "socket_path_throw=Error creating ssh control socket for origin host."
	assertContains "Socket setup should continue safely after removing a stale role socket." \
		"$output" "stale_setup_status=0"
	assertContains "Socket setup should remove a stale role socket before opening a replacement." \
		"$output" "stale_socket=removed"
	assertContains "Socket close dispatch should reject unknown roles." \
		"$output" "close_invalid_role_status=1"
	assertContains "Dry-run bootstrap should skip SSH socket setup successfully." \
		"$output" "prepare_dryrun_status=0"
	assertContains "SSH bootstrap dependency failures should use dependency classification." \
		"$output" "prepare_dependency_class=dependency"
	assertContains "SSH bootstrap dependency failures should preserve the lookup diagnostic." \
		"$output" "prepare_dependency_throw=ssh dependency missing"
	assertContains "Target host quoting failures should retain usage-error handling." \
		"$output" "target_usage_throw=target host rejected"
}

test_zxfer_remote_capability_owner_and_cache_error_branches_fail_closed() {
	output=$(
		(
			set +e
			zxfer_publish_endpoint_runtime_context invalid Linux /sbin/zfs
			printf 'publish_endpoint_status=%s\n' "$?"

			g_zxfer_remote_capability_tool_records=$(printf 'zfs\t0')
			zxfer_get_parsed_remote_capability_tool_record zfs >/dev/null
			printf 'malformed_record_status=%s\n' "$?"

			zxfer_render_remote_capability_cache_identity_for_host \
				"host.example" zfs invalid >/dev/null
			printf 'identity_role_status=%s\n' "$?"

			zxfer_clear_parsed_remote_capability_state_for_role invalid
			printf 'clear_role_status=%s\n' "$?"

			g_zxfer_remote_capability_os=Linux
			g_zxfer_remote_capability_zfs_status=0
			g_zxfer_remote_capability_tool_records=$(printf 'zfs\t0\t/sbin/zfs')
			zxfer_publish_parsed_remote_capability_state_for_role \
				invalid identity
			printf 'publish_role_status=%s\n' "$?"

			g_target_remote_capabilities_parsed_identity=target-identity
			g_target_remote_capabilities_os=FreeBSD
			g_target_remote_capabilities_zfs_status=0
			g_target_remote_capabilities_tool_records=$(printf 'zfs\t0\t/sbin/zfs')
			zxfer_load_parsed_remote_capability_state_for_role \
				target target-identity
			printf 'load_target_status=%s\n' "$?"
			printf 'load_target_os=%s\n' "$g_zxfer_remote_capability_os"
			zxfer_load_parsed_remote_capability_state_for_role invalid identity
			printf 'load_invalid_status=%s\n' "$?"

			zxfer_store_remote_capability_response_for_role \
				invalid host identity response
			printf 'store_invalid_status=%s\n' "$?"
		)
		(
			set +e
			g_target_remote_capabilities_host=old.example
			g_target_remote_capabilities_cache_identity=old-identity
			g_target_remote_capabilities_response=old-response
			g_target_remote_capabilities_bootstrap_source=live
			g_target_remote_capabilities_parsed_identity=old-identity
			g_target_remote_capabilities_os=Linux
			g_target_remote_capabilities_zfs_status=0
			g_target_remote_capabilities_tool_records=$(printf 'zfs\t0\t/sbin/zfs')
			zxfer_store_remote_capability_response_for_role \
				target new.example new-identity new-response
			printf 'store_target_status=%s\n' "$?"
			printf 'store_target_host=%s\n' "$g_target_remote_capabilities_host"
			printf 'store_target_bootstrap=<%s>\n' "$g_target_remote_capabilities_bootstrap_source"
			printf 'store_target_parsed=<%s>\n' "$g_target_remote_capabilities_parsed_identity"
		)
	)

	assertContains "Endpoint context publication should reject unknown roles." \
		"$output" "publish_endpoint_status=2"
	assertContains "Malformed parsed tool records should fail closed." \
		"$output" "malformed_record_status=1"
	assertContains "Capability cache identities should reject unknown roles." \
		"$output" "identity_role_status=1"
	assertContains "Parsed capability clearing should reject unknown roles." \
		"$output" "clear_role_status=2"
	assertContains "Parsed capability publication should reject unknown roles." \
		"$output" "publish_role_status=2"
	assertContains "Target parsed capability state should load through the explicit owner path." \
		"$output" "load_target_status=0"
	assertContains "Target parsed capability state should publish the cached operating system." \
		"$output" "load_target_os=FreeBSD"
	assertContains "Parsed capability loads should reject unknown roles." \
		"$output" "load_invalid_status=2"
	assertContains "Raw capability storage should reject unknown roles." \
		"$output" "store_invalid_status=2"
	assertContains "Target cache replacement should publish the new host." \
		"$output" "store_target_host=new.example"
	assertContains "Target cache replacement should clear stale bootstrap provenance." \
		"$output" "store_target_bootstrap=<>"
	assertContains "Target cache replacement should clear stale parsed state." \
		"$output" "store_target_parsed=<>"
}

test_zxfer_remote_capability_lookup_guard_branches_preserve_cache_ownership() {
	output=$(
		(
			set +e
			zxfer_get_remote_capability_requested_tools_for_host() {
				printf '%s\n' zfs cat
			}
			requested=$(zxfer_get_remote_capability_requested_tools_for_resolved_tool \
				"host.example" zfs)
			printf 'requested_status=%s\n' "$?"
			printf 'requested_tools=%s\n' "$(printf '%s\n' "$requested" | tr '\n' ',')"
		)
		(
			set +e
			zxfer_resolve_remote_capability_requested_tools_for_host() {
				return 74
			}
			zxfer_parsed_remote_capabilities_cover_requested_tools \
				"host.example" zfs
			printf 'coverage_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_load_cached_remote_capability_state_for_host \
				"host.example" zfs invalid
			printf 'load_invalid_role_status=%s\n' "$?"
			zxfer_get_cached_remote_capability_response_for_host \
				"host.example" zfs invalid >/dev/null
			printf 'get_invalid_role_status=%s\n' "$?"
			zxfer_store_cached_remote_capability_response_for_host \
				"host.example" response zfs invalid
			printf 'store_invalid_role_status=%s\n' "$?"
			zxfer_note_remote_capability_bootstrap_source_for_host \
				"host.example" live zfs invalid
			printf 'note_invalid_role_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_render_remote_capability_cache_identity_for_host() {
				return 75
			}
			zxfer_load_cached_remote_capability_state_for_host \
				"host.example" zfs origin
			printf 'load_identity_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' target-identity
			}
			g_origin_remote_capabilities_host=other.example
			g_origin_remote_capabilities_cache_identity=origin-identity
			g_origin_remote_capabilities_response=origin-response
			g_target_remote_capabilities_host=target.example
			g_target_remote_capabilities_cache_identity=target-identity
			g_target_remote_capabilities_response=target-response
			zxfer_load_cached_remote_capability_state_for_host \
				target.example zfs ""
			printf 'legacy_target_status=%s\n' "$?"
			printf 'legacy_target_role=%s\n' "$g_zxfer_remote_capability_cache_role_result"
			printf 'legacy_target_response=%s\n' "$g_zxfer_remote_capability_response_result"
		)
	)

	assertContains "Resolved-tool requests should reuse a matching host-scoped request set." \
		"$output" "requested_status=0"
	assertContains "Resolved-tool requests should retain the complete matching host scope." \
		"$output" "requested_tools=zfs,cat,"
	assertContains "Requested-tool coverage should fail when request normalization fails." \
		"$output" "coverage_status=1"
	assertContains "Capability cache loads should reject unknown roles." \
		"$output" "load_invalid_role_status=1"
	assertContains "Capability cache stdout reads should reject unknown roles." \
		"$output" "get_invalid_role_status=1"
	assertContains "Capability cache stores should reject unknown roles." \
		"$output" "store_invalid_role_status=1"
	assertContains "Capability bootstrap provenance should ignore unknown roles safely." \
		"$output" "note_invalid_role_status=0"
	assertContains "Capability cache loads should reject identity-rendering failures." \
		"$output" "load_identity_status=1"
	assertContains "Legacy unassigned cache loads should select an exact matching target slot." \
		"$output" "legacy_target_status=0"
	assertContains "Legacy unassigned cache loads should publish the selected target role." \
		"$output" "legacy_target_role=target"
	assertContains "Legacy unassigned cache loads should publish the exact target response." \
		"$output" "legacy_target_response=target-response"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
