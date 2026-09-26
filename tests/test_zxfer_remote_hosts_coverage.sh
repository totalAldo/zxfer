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
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
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

test_close_ssh_control_socket_for_role_returns_early_without_state() {
	output=$(
		(
			zxfer_close_ssh_control_socket_for_role origin
			printf 'origin=%s\n' "$?"
			zxfer_close_ssh_control_socket_for_role target
			printf 'target=%s\n' "$?"
		)
	)

	assertContains "Origin ssh control socket close should return early without state." \
		"$output" "origin=0"
	assertContains "Target ssh control socket close should return early without state." \
		"$output" "target=0"
}

test_zxfer_remote_tool_resolution_branches_cover_current_shell_paths() {
	output=$(
		(
			set +e
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_run_remote_probe_script() {
				g_zxfer_remote_probe_stdout=/sbin/zfs
				return 0
			}
			zxfer_resolve_remote_required_tool "user@example" zfs ZFS source
			printf 'resolve_ensure_fallback_status=%s\n' "$?"
			printf 'resolve_ensure_fallback=%s\n' "$g_zxfer_required_tool_result"
		)
		(
			set +e
			zxfer_ensure_remote_host_capabilities() {
				zxfer_parse_remote_capability_response 'ZXFER_REMOTE_CAPS_V2
os	Linux
tool	zfs	0	/sbin/zfs
end'
			}
			zxfer_run_remote_probe_script() {
				printf 'probe=%s\n' "$3" >&2
				g_zxfer_remote_probe_stdout=/usr/bin/tar
				return 0
			}
			zxfer_resolve_remote_required_tool "user@example" tar TAR source 2>&1
			printf 'resolve_missing_tool_status=%s\n' "$?"
			printf 'resolve_missing_tool=%s\n' "$g_zxfer_required_tool_result"
		)
		(
			set +e
			zxfer_resolve_remote_required_tool "" zfs ZFS source
			printf 'resolve_empty_host_status=%s output=<%s>\n' "$?" "$g_zxfer_required_tool_result"
		)
	)

	assertContains "Remote tool resolution should fall back to the direct probe when capability bootstrap fails." \
		"$output" "resolve_ensure_fallback_status=0"
	assertContains "Remote tool resolution should keep the direct probe path after a failed bootstrap." \
		"$output" "resolve_ensure_fallback=/sbin/zfs"
	assertContains "A tool without a capability record should be probed directly." \
		"$output" "resolve_missing_tool_status=0"
	assertContains "The direct probe should look the tool up by name." \
		"$output" "probe=l_path=\$(command -v 'tar' 2>/dev/null);"
	assertContains "The direct probe result should be printed." \
		"$output" "/usr/bin/tar"
	assertContains "An empty host should fail without output." \
		"$output" "resolve_empty_host_status=1 output=<>"
}

test_zxfer_ssh_open_and_close_error_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_setup_close_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/close-error.sock"
			: >"$g_ssh_origin_control_socket"
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="exit action failed"
				g_zxfer_ssh_control_socket_action_command="$1 $2 $3"
				return 1
			}
			zxfer_close_ssh_control_socket_for_role origin 2>"$branch_root/close-error.err"
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
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_result=stale
				g_zxfer_ssh_control_socket_action_command="$1 $2 $3"
				return 1
			}
			zxfer_close_ssh_control_socket_for_role origin 2>"$branch_root/close-stale.err"
			printf 'close_stale_status=%s\n' "$?"
			printf 'close_stale_state=<%s>\n' "$g_ssh_origin_control_socket"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_supports_control_sockets=1
			zxfer_ensure_ssh_control_socket_dir() {
				return 1
			}
			zxfer_throw_error() {
				printf 'open_dir_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_open_ssh_control_sockets
			)
			printf 'open_dir_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_ssh_transport() {
				g_zxfer_ssh_transport_error="transport policy failure"
				return 1
			}
			zxfer_throw_error() {
				printf 'open_transport_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_open_ssh_control_sockets
			)
			printf 'open_transport_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_supports_control_sockets=1
			zxfer_ensure_ssh_control_socket_dir() {
				g_zxfer_ssh_control_socket_dir_result=$branch_root
			}
			zxfer_run_ssh_control_socket_action() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="host spec rejected"
				return 1
			}
			zxfer_throw_error() {
				printf 'open_action_throw=%s\n' "$1"
				printf 'open_action_state=<%s>\n' "$g_ssh_origin_control_socket"
				exit 9
			}
			(
				zxfer_open_ssh_control_sockets
			) 2>"$branch_root/open-action.err"
			printf 'open_action_status=%s\n' "$?"
			printf 'open_action_err=%s\n' "$(cat "$branch_root/open-action.err")"
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
	assertContains "Opening should fail closed when the per-run socket directory cannot be created." \
		"$output" "open_dir_throw=Error creating temporary directory for ssh control socket."
	assertContains "Opening should route transport policy failures through throw_error." \
		"$output" "open_transport_throw=transport policy failure"
	assertContains "Opening should route a rejected master start through throw_error." \
		"$output" "open_action_throw=Error creating ssh control socket for origin host."
	assertContains "A rejected master start should forget the role socket." \
		"$output" "open_action_state=<>"
	assertContains "A rejected master start should surface the action diagnostic first." \
		"$output" "open_action_err=host spec rejected"
}

test_zxfer_ssh_transport_directory_and_quoting_failure_branches_fail_closed() {
	branch_root="$TEST_TMPDIR/ssh_transport_directory_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			zxfer_parse_ssh_host_spec 'invalid "host"'
			printf 'quote_status=%s\n' "$?"
			printf 'quote_output=%s\n' "$g_zxfer_ssh_shell_context_error_result"
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
			zxfer_find_default_tmpdir() {
				g_zxfer_default_tmpdir_result=$branch_root
			}
			zxfer_create_unpredictable_staging_dir() {
				return 71
			}
			zxfer_ensure_ssh_control_socket_dir
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
			zxfer_find_default_tmpdir() {
				g_zxfer_default_tmpdir_result=$branch_root
			}
			zxfer_create_unpredictable_staging_dir() {
				mkdir "$unregistered_dir" || return "$?"
				g_zxfer_staging_dir_result=$unregistered_dir
			}
			zxfer_register_runtime_artifact_path() {
				return 72
			}
			zxfer_ensure_ssh_control_socket_dir
			printf 'register_status=%s\n' "$?"
			if [ -e "$unregistered_dir" ]; then
				printf '%s\n' 'register_cleanup=kept'
			else
				printf '%s\n' 'register_cleanup=removed'
			fi
		)
	)

	assertContains "Host-spec quoting should reject specs that need shell quoting." \
		"$output" "quote_status=1"
	assertContains "Host-spec quoting should keep the literal-token diagnostic." \
		"$output" "quote_output=Host spec (-O/-T) must use literal whitespace-delimited tokens only"
	assertContains "Short socket-directory staging failures should fail closed." \
		"$output" "create_status=1"
	assertContains "Runtime artifact registration failures should fail closed." \
		"$output" "register_status=1"
	assertContains "Unregistered socket directories should be removed immediately." \
		"$output" "register_cleanup=removed"
}

test_zxfer_ssh_transport_owner_guards_fail_closed() {
	output=$(
		(
			set +e
			zxfer_close_ssh_control_socket_for_role invalid
			printf 'close_invalid_role_status=%s\n' "$?"
		)
		(
			set +e
			g_option_O_origin_host=""
			g_option_T_target_host='invalid "target"'
			zxfer_throw_usage_error() {
				printf 'target_usage_throw=%s\n' "$1"
				exit "$2"
			}
			zxfer_refresh_remote_zfs_commands
			printf 'target_usage_status=%s\n' "$?"
		)
	)

	assertContains "Socket close dispatch should reject unknown roles." \
		"$output" "close_invalid_role_status=1"
	assertContains "Target host quoting failures should retain usage-error handling." \
		"$output" "target_usage_throw=Host spec (-O/-T) must use literal whitespace-delimited tokens only"
}

test_zxfer_remote_capability_owner_branches_fail_closed() {
	output=$(
		(
			set +e
			zxfer_publish_endpoint_runtime_context invalid Linux /sbin/zfs
			printf 'publish_endpoint_status=%s\n' "$?"

			g_zxfer_remote_capability_tool_records=$(printf 'zfs\t0')
			zxfer_get_parsed_remote_capability_tool_record zfs >/dev/null
			printf 'malformed_record_status=%s\n' "$?"

			zxfer_fetch_remote_host_capabilities_live() {
				printf 'unexpected probe\n'
			}
			zxfer_ensure_remote_host_capabilities host.example invalid >/dev/null
			printf 'ensure_invalid_side_status=%s\n' "$?"
		)
		(
			set +e
			g_target_remote_capabilities_host=target.example
			g_target_remote_capabilities_tools=zfs
			g_target_remote_capabilities_response=cached-response
			g_target_remote_capabilities_os=FreeBSD
			g_target_remote_capabilities_zfs_status=0
			g_target_remote_capabilities_tool_records=$(printf 'zfs\t0\t/sbin/zfs')
			zxfer_ensure_remote_host_capabilities target.example destination >/dev/null
			printf 'load_target_status=%s\n' "$?"
			printf 'load_target_os=%s\n' "$g_zxfer_remote_capability_os"
			printf 'load_target_response=%s\n' "$g_zxfer_remote_capability_response_result"
		)
	)

	assertContains "Endpoint context publication should reject unknown roles." \
		"$output" "publish_endpoint_status=2"
	assertContains "Malformed parsed tool records should fail closed." \
		"$output" "malformed_record_status=1"
	assertContains "Capability lookups should reject an unknown side." \
		"$output" "ensure_invalid_side_status=1"
	assertNotContains "An unknown side must not probe." \
		"$output" "unexpected probe"
	assertContains "A filled target slot should load without a probe." \
		"$output" "load_target_status=0"
	assertContains "A filled target slot should publish its operating system." \
		"$output" "load_target_os=FreeBSD"
	assertContains "A filled target slot should publish its response." \
		"$output" "load_target_response=cached-response"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
