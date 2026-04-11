#!/bin/sh
#
# Additional shunit2 coverage for zxfer_remote_hosts.sh branches that are hard
# to exercise without isolated helper overrides.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_replication.sh"

cleanup_private_remote_host_cache_dirs() {
	if effective_uid=$(zxfer_get_effective_user_uid 2>/dev/null); then
		rm -rf "$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
		rm -rf "$TEST_TMPDIR/zxfer-s.$effective_uid.d"
		rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
		rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR/zxfer-s.$effective_uid.d"
		rm -rf "$TEST_TMPDIR"/*.remote-capabilities."$effective_uid".d
		rm -rf "$TEST_TMPDIR"/*.s."$effective_uid".d
		rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"/*.remote-capabilities."$effective_uid".d
		rm -rf "$TEST_PRIVATE_DEFAULT_TMPDIR"/*.s."$effective_uid".d
	fi
}

tearDown() {
	PATH=$TEST_ORIGINAL_PATH
	export PATH
	cleanup_private_remote_host_cache_dirs
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

fake_remote_capability_response() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
EOF
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts_coverage"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
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
	OPTIND=1
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset FAKE_SSH_STDOUT
	unset FAKE_SSH_STDERR
	unset FAKE_SSH_SUPPRESS_STDOUT
	unset ZXFER_BACKUP_DIR
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
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_z_compress=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_g_grandfather_protection=""
	g_option_j_jobs=1
	g_option_m_migrate=0
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_dependency_path=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_dependency_path=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_zxfer_remote_capability_response_result=""
	g_zxfer_backup_file_read_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
	g_zxfer_ssh_control_socket_lock_dir_result=""
	g_zxfer_ssh_control_socket_lock_error=""
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	g_ssh_supports_control_sockets=0
	g_test_max_yield_iterations=8
	g_zxfer_remote_capability_cache_wait_retries=5
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	zxfer_get_max_yield_iterations() {
		printf '%s\n' "$g_test_max_yield_iterations"
	}
	zxfer_init_temp_artifacts
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	create_fake_ssh_bin
}

test_zxfer_render_ssh_control_socket_entry_identity_reports_transport_policy_failures() {
	set +e
	output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_render_ssh_control_socket_entry_identity "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket entry identities should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "Shared ssh control socket entry identity failures should preserve the underlying ssh policy validation message." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_get_ssh_control_socket_cache_dir_for_key_prefers_current_root_when_short_enough() {
	cache_dir="/tmp/zxfer-shcache.$$"
	mkdir -p "$cache_dir"

	output=$(
		(
			zxfer_ensure_ssh_control_socket_cache_dir() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key "kshort"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache-dir lookups should succeed when the current cache root keeps socket paths under the length limit." \
		0 "$status"
	assertEquals "Shared ssh control socket cache-dir lookups should reuse the current cache root when it is already short enough." \
		"$cache_dir" "$output"
}

test_zxfer_get_ssh_control_socket_cache_dir_for_key_fails_when_fallback_root_is_still_too_long() {
	long_suffix="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	long_cache_dir="$TEST_TMPDIR/$long_suffix"
	long_short_cache_dir="/tmp/$long_suffix"

	set +e
	output=$(
		(
			zxfer_ensure_ssh_control_socket_cache_dir() {
				if [ "${TMPDIR+x}" = x ]; then
					printf '%s\n' "$long_cache_dir"
				else
					printf '%s\n' "$long_short_cache_dir"
				fi
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key "kstilltoolong"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache-dir lookups should fail when even the fallback root still exceeds the socket-path limit." \
		1 "$status"
	assertEquals "Failed shared ssh control socket cache-dir lookups should not print a payload." \
		"" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_creates_secure_entry_and_leases_dir() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")

	assertTrue "Shared ssh control socket entry dirs should be created on demand." \
		"[ -d '$entry_dir' ]"
	assertTrue "Shared ssh control socket entry dirs should create a leases subdirectory." \
		"[ -d '$entry_dir/leases' ]"
	assertTrue "Shared ssh control socket entry dirs should persist a secure identity file." \
		"[ -f '$entry_dir/id' ]"
	assertEquals "Shared ssh control socket entry dirs should be mode 0700." \
		"700" "$(zxfer_get_path_mode_octal "$entry_dir")"
	assertEquals "Shared ssh control socket lease dirs should be mode 0700." \
		"700" "$(zxfer_get_path_mode_octal "$entry_dir/leases")"
	assertEquals "Shared ssh control socket identity files should be mode 0600." \
		"600" "$(zxfer_get_path_mode_octal "$entry_dir/id")"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_uses_suffix_when_identity_mismatches_existing_key() {
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "kshared")
	result=$(
		(
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' "kshared"
			}
			first=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			second=$(zxfer_ensure_ssh_control_socket_entry_dir "other.example")
			printf 'first=%s\n' "$first"
			printf 'second=%s\n' "$second"
		)
	)

	assertContains "Mismatched shared ssh control socket identities should keep the first cache entry on the base key." \
		"$result" "first=$cache_dir/kshared"
	assertContains "Mismatched shared ssh control socket identities should fall back to a suffixed cache entry instead of reusing the wrong socket." \
		"$result" "second=$cache_dir/kshared.1"
}

test_zxfer_check_ssh_control_socket_for_host_uses_transport_and_host_tokens() {
	log="$TEST_TMPDIR/check_socket.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	output=$(
		(
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes"
			}
			zxfer_check_ssh_control_socket_for_host "origin.example pfexec" "$TEST_TMPDIR/origin.sock"
		)
	)
	status=$?

	unset FAKE_SSH_LOG

	assertEquals "Shared ssh control socket checks should succeed when ssh accepts the probe." 0 "$status"
	assertEquals "Successful shared ssh control socket checks should not print a payload." "" "$output"
	assertEquals "Shared ssh control socket checks should preserve both the managed ssh transport tokens and the host wrapper tokens." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$TEST_TMPDIR/origin.sock
-O
check
origin.example
pfexec" "$(cat "$log")"
}

test_zxfer_check_ssh_control_socket_for_host_emits_very_verbose_command_prefix() {
	log="$TEST_TMPDIR/check_socket_verbose.log"
	stderr_file="$TEST_TMPDIR/check_socket_verbose.err"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"

	zxfer_check_ssh_control_socket_for_host "origin.example pfexec" "$TEST_TMPDIR/origin.sock" \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/origin.sock" "-O" "check" "origin.example" "pfexec")

	assertContains "Very-verbose control-socket checks should prefix the origin-host probe." \
		"$(cat "$stderr_file")" "Checking ssh control socket [origin: origin.example pfexec]:"
	assertContains "Very-verbose control-socket checks should print the full rendered ssh probe." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_zxfer_check_ssh_control_socket_for_host_classifies_stale_master_failures() {
	output=$(
		(
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Control socket connect($TEST_TMPDIR/check.sock): No such file or directory"
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"

			if zxfer_check_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/check.sock"; then
				status=0
			else
				status=$?
			fi

			printf 'status=%s\n' "$status"
			printf 'result=%s\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'stderr=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)

	assertContains "Control-socket checks should still return nonzero when the master is stale." \
		"$output" "status=1"
	assertContains "Control-socket checks should classify missing masters separately from transport failures." \
		"$output" "result=stale"
	assertContains "Control-socket checks should preserve the stale-master diagnostic for callers." \
		"$output" "No such file or directory"
}

test_zxfer_check_ssh_control_socket_for_host_preserves_transport_failure_diagnostics() {
	output=$(
		(
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Host key verification failed."
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"

			if zxfer_check_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/check.sock"; then
				status=0
			else
				status=$?
			fi

			printf 'status=%s\n' "$status"
			printf 'result=%s\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'stderr=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)

	assertContains "Control-socket checks should fail when ssh transport setup fails." \
		"$output" "status=1"
	assertContains "Control-socket checks should classify ssh transport failures distinctly from stale masters." \
		"$output" "result=error"
	assertContains "Control-socket checks should preserve ssh transport stderr for the caller." \
		"$output" "Host key verification failed."
}

test_zxfer_open_ssh_control_socket_for_host_reports_transport_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_open_ssh_control_socket_for_host "origin.example" "$TEST_TMPDIR/origin.sock"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket opens should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "Shared ssh control socket open failures should preserve the underlying ssh policy validation message." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_open_ssh_control_socket_for_host_emits_very_verbose_command_prefix() {
	log="$TEST_TMPDIR/open_socket_verbose.log"
	stderr_file="$TEST_TMPDIR/open_socket_verbose.err"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"

	zxfer_open_ssh_control_socket_for_host "origin.example pfexec" "$TEST_TMPDIR/origin.sock" \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-M" "-S" "$TEST_TMPDIR/origin.sock" "-fN" "origin.example" "pfexec")

	assertContains "Very-verbose control-socket opens should prefix the origin-host startup command." \
		"$(cat "$stderr_file")" "Opening ssh control socket [origin: origin.example pfexec]:"
	assertContains "Very-verbose control-socket opens should print the full rendered ssh startup command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_zxfer_render_remote_capability_cache_identity_reports_transport_policy_failures() {
	set +e
	output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_render_remote_capability_cache_identity
		)
	)
	status=$?

	assertEquals "Remote capability cache identities should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "Remote capability cache identity failures should preserve the underlying ssh policy validation message." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_read_remote_probe_capture_file_preserves_multiline_output() {
	capture_path="$TEST_TMPDIR/remote_probe_capture.txt"
	cat >"$capture_path" <<'EOF'
line one
line two
EOF

	output=$(
		(
			zxfer_read_remote_probe_capture_file "$capture_path"
			printf 'status=%s\n' "$?"
			printf 'payload=%s\n' "$g_zxfer_remote_probe_capture_read_result"
		)
	)

	assertContains "Remote probe capture-file reads should succeed for readable files." \
		"$output" "status=0"
	assertContains "Remote probe capture-file reads should preserve multiline payloads." \
		"$output" "line one"
	assertContains "Remote probe capture-file reads should preserve the trailing lines in multiline payloads." \
		"$output" "line two"
}

test_zxfer_emit_remote_probe_failure_message_uses_default_when_stderr_is_empty() {
	output=$(
		(
			g_zxfer_remote_probe_stderr=""
			zxfer_emit_remote_probe_failure_message "fallback probe error"
		)
	)

	assertEquals "Remote probe failure messages should fall back to the provided default when stderr is empty." \
		"fallback probe error" "$output"
}

test_zxfer_get_cached_remote_capability_response_for_host_returns_failure_when_identity_lookup_fails() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_response=$(fake_remote_capability_response)
	g_target_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH

	set +e
	output=$(
		(
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_get_cached_remote_capability_response_for_host "target.example"
		)
	)
	status=$?

	assertEquals "Cached remote capability reads should fail cleanly when the cache identity cannot be computed." \
		1 "$status"
	assertEquals "Failed cached remote capability reads should not print a payload." \
		"" "$output"
}

test_zxfer_store_cached_remote_capability_response_for_host_clears_identity_when_lookup_fails() {
	output=$(
		(
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_store_cached_remote_capability_response_for_host "shared.example" "$(fake_remote_capability_response)"
			printf 'host=%s\n' "$g_origin_remote_capabilities_host"
			printf 'cache_identity=%s\n' "${g_origin_remote_capabilities_cache_identity:-}"
		)
	)

	assertContains "Fallback capability response storage should still remember the host when cache-identity computation fails." \
		"$output" "host=shared.example"
	assertContains "Fallback capability response storage should clear the cached identity when the identity helper fails." \
		"$output" "cache_identity="
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_ignores_identity_lookup_failures() {
	g_option_O_origin_host="origin.example"
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_bootstrap_source=""

	output=$(
		(
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" live
			printf 'status=%s\n' "$?"
			printf 'bootstrap=%s\n' "${g_origin_remote_capabilities_bootstrap_source:-}"
		)
	)

	assertContains "Capability bootstrap-source updates should still return success when cache-identity computation fails." \
		"$output" "status=0"
	assertContains "Capability bootstrap-source updates should leave the bootstrap source empty when cache-identity computation fails." \
		"$output" "bootstrap="
}

test_zxfer_remote_capability_cache_key_returns_identity_lookup_failure() {
	set +e
	output=$(
		(
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_remote_capability_cache_key "origin.example" "zfs"
		)
	)
	status=$?

	assertEquals "Capability cache-key generation should fail cleanly when the host-scoped cache identity cannot be computed." \
		1 "$status"
	assertContains "Capability cache-key generation should preserve the host-scoped identity diagnostic." \
		"$output" "Managed ssh policy invalid."
}

test_zxfer_read_ssh_control_socket_action_stderr_file_trims_newline_and_preserves_readback_failures() {
	stderr_path="$TEST_TMPDIR/ssh_control_socket_action.stderr"
	printf '%s\n' "control socket diagnostic" >"$stderr_path"

	output=$(
		(
			zxfer_read_ssh_control_socket_action_stderr_file "$stderr_path"
			printf 'status=%s\n' "$?"
			printf 'stderr=%s\n' "$g_zxfer_ssh_control_socket_action_stderr"
		)
	)

	assertContains "Control-socket stderr capture reads should succeed for readable staged files." \
		"$output" "status=0"
	assertContains "Control-socket stderr capture reads should trim a single trailing newline from the staged payload." \
		"$output" "stderr=control socket diagnostic"

	set +e
	output=$(
		(
			zxfer_read_runtime_artifact_file() {
				return 73
			}
			zxfer_read_ssh_control_socket_action_stderr_file "$stderr_path"
		)
	)
	status=$?

	assertEquals "Control-socket stderr capture reads should preserve staged readback failure statuses." \
		73 "$status"
	assertEquals "Failed control-socket stderr capture reads should not print a payload." \
		"" "$output"
}

test_zxfer_emit_ssh_control_socket_action_failure_message_uses_default_when_stderr_is_empty() {
	output=$(
		(
			g_zxfer_ssh_control_socket_action_stderr=""
			zxfer_emit_ssh_control_socket_action_failure_message "fallback control socket error"
		)
	)

	assertEquals "Control-socket failure messages should fall back to the provided default when staged stderr is empty." \
		"fallback control socket error" "$output"
}

test_zxfer_emit_ssh_control_socket_lock_failure_message_uses_staged_error_without_default() {
	output=$(
		(
			g_zxfer_ssh_control_socket_lock_error="staged lock diagnostic"
			zxfer_emit_ssh_control_socket_lock_failure_message
		)
	)

	assertEquals "ssh control socket lock failure messages should emit the staged diagnostic when no default is supplied." \
		"staged lock diagnostic" "$output"
}

test_zxfer_acquire_ssh_control_socket_lock_treats_invalid_fast_retry_values_as_zero() {
	sleep_log="$TEST_TMPDIR/ssh_lock_invalid_fast_retry.sleep"
	entry_dir="$TEST_TMPDIR/ssh_lock_invalid_fast_retry_entry"
	mkdir -p "$entry_dir"

	set +e
	output=$(
		(
			ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=invalid
			mkdir() {
				return 1
			}
			sleep() {
				printf '%s\n' "slept" >>"$sleep_log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket lock acquisition should still fail cleanly when the fast-retry count is invalid." \
		1 "$status"
	assertEquals "Failed shared ssh control socket lock acquisition should not print a payload when the fast-retry count is invalid." \
		"" "$output"
	assertEquals "Invalid fast-retry counts should fall back to the slower retry path immediately." \
		"9" "$(wc -l <"$sleep_log" | tr -d ' ')"
}

test_zxfer_acquire_ssh_control_socket_lock_returns_failure_when_initial_reap_errors() {
	entry_dir="$TEST_TMPDIR/ssh_lock_initial_reap_error_entry"
	lock_dir="$entry_dir.lock"
	mkdir -p "$entry_dir" "$lock_dir"

	set +e
	output=$(
		(
			zxfer_create_ssh_control_socket_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir() {
				return 1
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "ssh control socket lock acquisition should fail cleanly when stale-lock validation fails before the wait path starts." \
		1 "$status"
	assertEquals "Failed ssh control socket lock acquisition should not print a payload when the initial stale-lock reap fails." \
		"" "$output"
}

test_zxfer_acquire_ssh_control_socket_lock_returns_failure_when_final_reap_errors() {
	sleep_log="$TEST_TMPDIR/ssh_lock_final_reap_error.sleep"
	entry_dir="$TEST_TMPDIR/ssh_lock_final_reap_error_entry"
	lock_dir="$entry_dir.lock"
	mkdir -p "$entry_dir" "$lock_dir"

	zxfer_test_capture_subshell "
		ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=0
		zxfer_create_ssh_control_socket_lock_dir() {
			return 1
		}
		zxfer_try_reap_stale_ssh_control_socket_lock_dir() {
			case \"\${2:-0}\" in
			1)
				return 1
				;;
			esac
			return 2
		}
		sleep() {
			printf 'retry\n' >>\"$sleep_log\"
		}
		zxfer_acquire_ssh_control_socket_lock \"$entry_dir\"
	"
	output=$ZXFER_TEST_CAPTURE_OUTPUT
	status=$ZXFER_TEST_CAPTURE_STATUS

	assertEquals "ssh control socket lock acquisition should fail cleanly when the final stale-lock reap attempt fails after the bounded wait." \
		1 "$status"
	assertEquals "Failed ssh control socket lock acquisition should not print a payload when the final stale-lock reap fails." \
		"" "$output"
	assertEquals "ssh control socket lock acquisition should still consume the bounded retry window before failing the final stale-lock reap." \
		"9" "$(wc -l <"$sleep_log" | tr -d ' ')"
}

test_zxfer_validate_ssh_control_socket_lock_dir_rejects_untrusted_metadata_sources() {
	lock_dir="$TEST_TMPDIR/ssh_lock_validate"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 17
			}
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock-dir validation should fail cleanly when the effective uid cannot be queried." \
		1 "$status"
	assertEquals "Failed ssh control socket lock-dir validation should not print a payload when the effective uid lookup fails." \
		"" "$output"

	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 23
			}
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock-dir validation should fail cleanly when the owner uid cannot be queried." \
		1 "$status"
	assertEquals "Failed ssh control socket lock-dir validation should not print a payload when the owner uid lookup fails." \
		"" "$output"

	output=$(
		(
			zxfer_get_effective_user_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1001"
			}
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock-dir validation should fail closed when the lock dir owner does not match the current user." \
		1 "$status"
	assertEquals "Rejected ssh control socket lock dirs should not print a payload when the owner uid mismatches." \
		"" "$output"

	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 29
			}
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock-dir validation should fail cleanly when the mode cannot be queried." \
		1 "$status"
	assertEquals "Failed ssh control socket lock-dir validation should not print a payload when the mode lookup fails." \
		"" "$output"

	output=$(
		(
			zxfer_get_effective_user_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' "755"
			}
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock-dir validation should fail closed when the lock dir permissions are not 0700." \
		1 "$status"
	assertEquals "Rejected ssh control socket lock dirs should not print a payload when the mode mismatches." \
		"" "$output"
}

test_zxfer_validate_ssh_control_socket_lock_dir_reports_missing_and_symlink_paths() {
	missing_lock_dir="$TEST_TMPDIR/ssh_lock_validate_missing"
	lock_target_dir="$TEST_TMPDIR/ssh_lock_validate_target"
	lock_link_dir="$TEST_TMPDIR/ssh_lock_validate_link"
	mkdir "$lock_target_dir"
	ln -s "$lock_target_dir" "$lock_link_dir"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir "$missing_lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket lock-dir validation should reject missing lock dirs." \
		"$output" "status=1"
	assertContains "ssh control socket lock-dir validation should preserve a missing-lock diagnostic." \
		"$output" "error=ssh control socket lock path \"$missing_lock_dir\" is not a directory."

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir "$lock_link_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket lock-dir validation should reject symlinked lock dirs." \
		"$output" "status=1"
	assertContains "ssh control socket lock-dir validation should preserve a symlink diagnostic." \
		"$output" "error=Refusing symlinked ssh control socket lock path \"$lock_link_dir\"."
}

test_zxfer_validate_ssh_control_socket_lock_dir_for_reap_reports_error_context() {
	lock_dir="$TEST_TMPDIR/ssh_lock_reap_validate"
	lock_target_dir="$TEST_TMPDIR/ssh_lock_reap_target"
	lock_link_dir="$TEST_TMPDIR/ssh_lock_reap_link"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	mkdir "$lock_target_dir"
	ln -s "$lock_target_dir" "$lock_link_dir"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_link_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket reap-time validation should reject symlinked lock dirs." \
		"$output" "status=1"
	assertContains "ssh control socket reap-time validation should preserve the symlink diagnostic." \
		"$output" "error=Refusing symlinked ssh control socket lock path \"$lock_link_dir\"."

	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 17
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket reap-time validation should fail cleanly when the effective uid cannot be queried." \
		"$output" "status=1"
	assertContains "ssh control socket reap-time validation should preserve the effective-uid diagnostic." \
		"$output" "error=Unable to determine the effective uid for ssh control socket lock validation."

	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 23
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket reap-time validation should fail cleanly when the owner uid cannot be queried." \
		"$output" "status=1"
	assertContains "ssh control socket reap-time validation should preserve the owner-lookup diagnostic." \
		"$output" "error=Unable to determine the owner of ssh control socket lock path \"$lock_dir\"."

	output=$(
		(
			zxfer_get_effective_user_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1001"
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket reap-time validation should fail closed when the lock dir owner does not match the current user." \
		"$output" "status=1"
	assertContains "ssh control socket reap-time validation should preserve the owner-mismatch diagnostic." \
		"$output" "error=ssh control socket lock path \"$lock_dir\" is not owned by the effective uid."

	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 29
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket reap-time validation should fail cleanly when the mode cannot be queried." \
		"$output" "status=1"
	assertContains "ssh control socket reap-time validation should preserve the mode-lookup diagnostic." \
		"$output" "error=Unable to determine permissions for ssh control socket lock path \"$lock_dir\"."

	output=$(
		(
			zxfer_get_path_mode_octal() {
				printf '%s\n' "666"
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket reap-time validation should reject unsupported legacy lock-dir permissions." \
		"$output" "status=1"
	assertContains "ssh control socket reap-time validation should preserve the unsupported-permissions diagnostic." \
		"$output" "error=Existing ssh control socket lock path \"$lock_dir\" has unsupported permissions (666). Remove the stale lock directory and retry."
}

test_zxfer_read_ssh_control_socket_lock_pid_file_rejects_untrusted_metadata_and_invalid_payloads() {
	pid_path="$TEST_TMPDIR/ssh_lock_pid"
	printf '%s\n' "4242" >"$pid_path"
	chmod 600 "$pid_path"

	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 17
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_path"
		)
	)
	status=$?
	assertEquals "ssh control socket pid-file reads should fail cleanly when the effective uid cannot be queried." \
		1 "$status"
	assertEquals "Failed ssh control socket pid-file reads should not print a payload when the effective uid lookup fails." \
		"" "$output"

	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 23
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_path"
		)
	)
	status=$?
	assertEquals "ssh control socket pid-file reads should fail cleanly when the owner uid cannot be queried." \
		1 "$status"
	assertEquals "Failed ssh control socket pid-file reads should not print a payload when the owner uid lookup fails." \
		"" "$output"

	output=$(
		(
			zxfer_get_effective_user_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1001"
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_path"
		)
	)
	status=$?
	assertEquals "ssh control socket pid-file reads should fail closed when the pid file owner does not match the current user." \
		1 "$status"
	assertEquals "Rejected ssh control socket pid-file reads should not print a payload when the owner uid mismatches." \
		"" "$output"

	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 29
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_path"
		)
	)
	status=$?
	assertEquals "ssh control socket pid-file reads should fail cleanly when the mode cannot be queried." \
		1 "$status"
	assertEquals "Failed ssh control socket pid-file reads should not print a payload when the mode lookup fails." \
		"" "$output"

	output=$(
		(
			zxfer_get_effective_user_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1000"
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' "644"
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_path"
		)
	)
	status=$?
	assertEquals "ssh control socket pid-file reads should fail closed when the pid file permissions are not 0600." \
		1 "$status"
	assertEquals "Rejected ssh control socket pid-file reads should not print a payload when the mode mismatches." \
		"" "$output"

	output=$(
		(
			zxfer_read_runtime_artifact_file() {
				printf '%s\n' "invalid.pid"
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_path"
		)
	)
	status=$?
	assertEquals "ssh control socket pid-file reads should fail cleanly when the staged pid payload is not numeric." \
		1 "$status"
	assertEquals "Rejected ssh control socket pid-file reads should not print a payload when the staged pid is invalid." \
		"" "$output"
}

test_zxfer_read_ssh_control_socket_lock_pid_file_reports_missing_symlink_and_read_failures() {
	pid_target_path="$TEST_TMPDIR/ssh_lock_pid_target"
	pid_link_path="$TEST_TMPDIR/ssh_lock_pid_link"
	pid_read_fail_path="$TEST_TMPDIR/ssh_lock_pid_read_fail"
	printf '%s\n' "4242" >"$pid_target_path"
	chmod 600 "$pid_target_path"
	ln -s "$pid_target_path" "$pid_link_path"
	printf '%s\n' "4242" >"$pid_read_fail_path"
	chmod 600 "$pid_read_fail_path"

	output=$(
		(
			zxfer_read_ssh_control_socket_lock_pid_file "$TEST_TMPDIR/ssh_lock_pid_missing"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket pid-file reads should reject missing pid files." \
		"$output" "status=1"
	assertContains "ssh control socket pid-file reads should preserve the missing-file diagnostic." \
		"$output" "error=ssh control socket lock pid file \"$TEST_TMPDIR/ssh_lock_pid_missing\" is missing or invalid."

	output=$(
		(
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_link_path"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket pid-file reads should reject symlinked pid files." \
		"$output" "status=1"
	assertContains "ssh control socket pid-file reads should preserve the symlink diagnostic." \
		"$output" "error=Refusing symlinked ssh control socket lock pid file \"$pid_link_path\"."

	output=$(
		(
			zxfer_read_runtime_artifact_file() {
				return 74
			}
			zxfer_read_ssh_control_socket_lock_pid_file "$pid_read_fail_path"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket pid-file reads should fail cleanly when the staged pid file cannot be read." \
		"$output" "status=1"
	assertContains "ssh control socket pid-file reads should preserve the staged readback diagnostic." \
		"$output" "error=Unable to read ssh control socket lock pid file \"$pid_read_fail_path\"."
}

test_zxfer_write_ssh_control_socket_lock_pid_file_returns_failure_when_stage_write_fails() {
	lock_dir="$TEST_TMPDIR/ssh_lock_pid_write_fail"
	mkdir "$lock_dir"

	set +e
	output=$(
		(
			zxfer_write_runtime_cache_file_atomically() {
				return 71
			}
			zxfer_write_ssh_control_socket_lock_pid_file "$lock_dir"
		)
	)
	status=$?

	assertEquals "ssh control socket pid-file writes should fail cleanly when staging the pid file fails." \
		1 "$status"
	assertEquals "Failed ssh control socket pid-file writes should not print a payload." \
		"" "$output"
}

test_zxfer_create_ssh_control_socket_lock_dir_cleans_up_failed_validation_and_pid_writes() {
	validation_lock_dir="$TEST_TMPDIR/ssh_lock_create_validation"
	pid_write_lock_dir="$TEST_TMPDIR/ssh_lock_create_pid_write"

	set +e
	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 1
			}
			zxfer_create_ssh_control_socket_lock_dir "$validation_lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock creation should fail cleanly when the freshly-created lock dir fails validation." \
		1 "$status"
	assertEquals "Failed ssh control socket lock creation should not print a payload when validation fails." \
		"" "$output"
	assertFalse "ssh control socket lock creation should clean up freshly-created lock dirs that fail validation." \
		"[ -e '$validation_lock_dir' ]"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 0
			}
			zxfer_write_ssh_control_socket_lock_pid_file() {
				return 1
			}
			zxfer_create_ssh_control_socket_lock_dir "$pid_write_lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock creation should fail cleanly when the pid file cannot be written." \
		1 "$status"
	assertEquals "Failed ssh control socket lock creation should not print a payload when the pid write fails." \
		"" "$output"
	assertFalse "ssh control socket lock creation should clean up freshly-created lock dirs when the pid write fails." \
		"[ -e '$pid_write_lock_dir' ]"
}

test_zxfer_cleanup_ssh_control_socket_lock_dir_reports_symlink_and_remove_failures() {
	lock_target_dir="$TEST_TMPDIR/ssh_lock_cleanup_target"
	lock_link_dir="$TEST_TMPDIR/ssh_lock_cleanup_link"
	lock_fail_dir="$TEST_TMPDIR/ssh_lock_cleanup_fail"
	mkdir "$lock_target_dir"
	ln -s "$lock_target_dir" "$lock_link_dir"
	mkdir "$lock_fail_dir"

	output=$(
		(
			zxfer_cleanup_ssh_control_socket_lock_dir "$lock_link_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket lock-dir cleanup should reject symlinked lock dirs." \
		"$output" "status=1"
	assertContains "ssh control socket lock-dir cleanup should preserve the symlink diagnostic." \
		"$output" "error=Refusing symlinked ssh control socket lock path \"$lock_link_dir\"."

	output=$(
		(
			rm() {
				return 1
			}
			zxfer_cleanup_ssh_control_socket_lock_dir "$lock_fail_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket lock-dir cleanup should fail cleanly when the stale lock dir cannot be removed." \
		"$output" "status=1"
	assertContains "ssh control socket lock-dir cleanup should preserve the removal diagnostic." \
		"$output" "error=Unable to remove stale ssh control socket lock path \"$lock_fail_dir\"."
}

test_zxfer_try_reap_stale_ssh_control_socket_lock_dir_covers_validation_pid_and_cleanup_failures() {
	invalid_lock_dir="$TEST_TMPDIR/ssh_lock_reap_invalid"
	unreadable_lock_dir="$TEST_TMPDIR/ssh_lock_reap_unreadable"
	live_pid_lock_dir="$TEST_TMPDIR/ssh_lock_reap_live_pid"
	pidless_lock_dir="$TEST_TMPDIR/ssh_lock_reap_pidless"
	reapable_lock_dir="$TEST_TMPDIR/ssh_lock_reap_reapable"

	set +e
	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$invalid_lock_dir" 0
		)
	)
	status=$?
	assertEquals "ssh control socket stale-lock reaping should fail cleanly when the candidate lock dir is invalid." \
		1 "$status"
	assertEquals "Failed ssh control socket stale-lock reaping should not print a payload when lock-dir validation fails." \
		"" "$output"

	mkdir "$unreadable_lock_dir"
	: >"$unreadable_lock_dir/pid"
	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 0
			}
			zxfer_read_ssh_control_socket_lock_pid_file() {
				return 1
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$unreadable_lock_dir" 0
		)
	)
	status=$?
	assertEquals "ssh control socket stale-lock reaping should defer unreadable pid files when pidless reaping is disabled." \
		2 "$status"
	assertEquals "Deferred ssh control socket stale-lock reaping should not print a payload when pidless reaping is disabled." \
		"" "$output"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 0
			}
			zxfer_read_ssh_control_socket_lock_pid_file() {
				return 1
			}
			zxfer_cleanup_ssh_control_socket_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$unreadable_lock_dir" 1
		)
	)
	status=$?
	assertEquals "ssh control socket stale-lock reaping should fail cleanly when cleanup of an unreadable pid lock fails." \
		1 "$status"
	assertEquals "Failed ssh control socket stale-lock reaping should not print a payload when cleanup fails after an unreadable pid file." \
		"" "$output"

	mkdir "$live_pid_lock_dir"
	: >"$live_pid_lock_dir/pid"
	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 0
			}
			zxfer_read_ssh_control_socket_lock_pid_file() {
				printf '%s\n' "4242"
			}
			kill() {
				return 0
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$live_pid_lock_dir" 1
		)
	)
	status=$?
	assertEquals "ssh control socket stale-lock reaping should defer locks whose owner pid is still alive." \
		2 "$status"
	assertEquals "Deferred ssh control socket stale-lock reaping should not print a payload when the owner pid is still alive." \
		"" "$output"

	mkdir "$pidless_lock_dir"
	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir() {
				return 0
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$pidless_lock_dir" 0
		)
	)
	status=$?
	assertEquals "ssh control socket stale-lock reaping should defer pidless lock dirs until pidless reaping is enabled." \
		2 "$status"
	assertEquals "Deferred ssh control socket stale-lock reaping should not print a payload for pidless lock dirs before the timeout path enables reaping." \
		"" "$output"

	mkdir "$reapable_lock_dir"
	chmod 700 "$reapable_lock_dir"
	output=$(zxfer_try_reap_stale_ssh_control_socket_lock_dir "$reapable_lock_dir" on)
	status=$?
	assertEquals "ssh control socket stale-lock reaping should remove pidless lock dirs once the pidless reaping path is enabled." \
		0 "$status"
	assertEquals "Successful ssh control socket stale-lock reaping should not print a payload." \
		"" "$output"
	assertFalse "ssh control socket stale-lock reaping should remove pidless lock dirs after the timeout path enables reaping." \
		"[ -e '$reapable_lock_dir' ]"
}

test_zxfer_ssh_control_socket_failure_is_stale_master_matches_control_socket_errors() {
	stale_output=$(
		(
			if zxfer_ssh_control_socket_failure_is_stale_master \
				"Control socket connect($TEST_TMPDIR/check.sock): Broken pipe"; then
				printf 'stale=yes\n'
			else
				printf 'stale=no\n'
			fi
		)
	)
	fresh_output=$(
		(
			if zxfer_ssh_control_socket_failure_is_stale_master \
				"Host key verification failed."; then
				printf 'stale=yes\n'
			else
				printf 'stale=no\n'
			fi
		)
	)

	assertContains "ssh control socket stale-master detection should classify broken-pipe control-socket errors as stale masters." \
		"$stale_output" "stale=yes"
	assertContains "ssh control socket stale-master detection should reject unrelated transport diagnostics." \
		"$fresh_output" "stale=no"
}

test_zxfer_run_ssh_control_socket_action_for_host_rejects_unknown_actions() {
	set +e
	output=$(
		(
			zxfer_run_ssh_control_socket_action_for_host \
				"origin.example" "$TEST_TMPDIR/origin.sock" invalid
			printf 'status=%s\n' "$?"
			printf 'result=%s\n' "${g_zxfer_ssh_control_socket_action_result:-}"
		)
	)
	status=$?

	assertEquals "ssh control socket action helpers should complete the test subshell cleanly when an invalid action is requested." \
		0 "$status"
	assertContains "ssh control socket action helpers should reject invalid action names." \
		"$output" "status=1"
	assertContains "ssh control socket action helpers should leave the action result empty when an invalid action is rejected before transport setup." \
		"$output" "result="
}

test_zxfer_release_ssh_control_socket_lock_reports_pid_and_directory_cleanup_failures() {
	rm_failure_lock_dir="$TEST_TMPDIR/ssh_lock_release_rm_failure"
	post_rm_failure_lock_dir="$TEST_TMPDIR/ssh_lock_release_post_rm_failure"
	rmdir_failure_lock_dir="$TEST_TMPDIR/ssh_lock_release_rmdir_failure"

	mkdir "$rm_failure_lock_dir"
	: >"$rm_failure_lock_dir/pid"
	set +e
	output=$(
		(
			rm() {
				return 1
			}
			zxfer_release_ssh_control_socket_lock "$rm_failure_lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock release should fail cleanly when the pid file cannot be removed." \
		1 "$status"
	assertEquals "Failed ssh control socket lock release should not print a payload when pid-file removal fails." \
		"" "$output"

	mkdir "$post_rm_failure_lock_dir"
	: >"$post_rm_failure_lock_dir/pid"
	output=$(
		(
			rm() {
				return 0
			}
			zxfer_release_ssh_control_socket_lock "$post_rm_failure_lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock release should fail closed when the pid file remains after rm reports success." \
		1 "$status"
	assertEquals "Rejected ssh control socket lock releases should not print a payload when the pid file remains after removal." \
		"" "$output"

	mkdir "$rmdir_failure_lock_dir"
	output=$(
		(
			rmdir() {
				return 1
			}
			zxfer_release_ssh_control_socket_lock "$rmdir_failure_lock_dir"
		)
	)
	status=$?
	assertEquals "ssh control socket lock release should fail cleanly when the lock dir cannot be removed." \
		1 "$status"
	assertEquals "Failed ssh control socket lock release should not print a payload when directory removal fails." \
		"" "$output"
}

test_zxfer_render_remote_capability_cache_identity_for_host_returns_requested_tool_failures() {
	set +e
	output=$(
		(
			zxfer_resolve_remote_capability_requested_tools_for_host() {
				return 1
			}
			zxfer_render_remote_capability_cache_identity_for_host "origin.example" "zfs"
		)
	)
	status=$?

	assertEquals "Capability cache identities should fail cleanly when requested-tool resolution fails for the target host." \
		1 "$status"
	assertEquals "Failed capability cache-identity generation should not print a payload when requested-tool resolution fails." \
		"" "$output"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_returns_failure_when_lookup_or_validation_fails() {
	output=$(
		(
			zxfer_remote_capability_cache_lock_path() {
				return 1
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example"
			printf 'status=%s\n' "$?"
		)
	)
	assertContains "Pidless remote capability cache-lock reaping should fail cleanly when the lock path cannot be derived." \
		"$output" "status=1"

	lock_dir="$TEST_TMPDIR/remote_caps_lock_validate"
	mkdir "$lock_dir"
	output=$(
		(
			zxfer_remote_capability_cache_lock_path() {
				printf '%s\n' "$lock_dir"
			}
			zxfer_validate_remote_capability_cache_lock_dir() {
				return 1
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example"
			printf 'status=%s\n' "$?"
		)
	)
	assertContains "Pidless remote capability cache-lock reaping should fail cleanly when the existing lock dir is invalid." \
		"$output" "status=1"
}

test_zxfer_wait_for_remote_capability_cache_fill_uses_slow_retry_path_when_fast_retry_count_is_invalid() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_invalid_fast_wait.attempts"
	sleep_log="$TEST_TMPDIR/remote_caps_invalid_fast_wait.sleep"
	printf '%s\n' 0 >"$read_attempt_file"

	output=$(
		(
			ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES=invalid
			g_option_V_very_verbose=1
			g_zxfer_profile_remote_capability_cache_wait_count=0
			g_zxfer_remote_capability_cache_wait_retries=3
			zxfer_read_remote_capability_cache_file() {
				read_attempts=$(cat "$read_attempt_file")
				read_attempts=$((read_attempts + 1))
				printf '%s\n' "$read_attempts" >"$read_attempt_file"
				if [ "$read_attempts" -eq 2 ]; then
					fake_remote_capability_response
					return 0
				fi
				return 1
			}
			sleep() {
				printf '%s\n' "slept" >>"$sleep_log"
			}
			zxfer_wait_for_remote_capability_cache_fill "origin.example"
			printf 'wait_count=%s\n' "${g_zxfer_profile_remote_capability_cache_wait_count:-0}"
		)
	)
	status=$?

	assertEquals "Capability cache waits should still succeed when the fast-retry count is invalid and a sibling populates the cache during the slower wait path." \
		0 "$status"
	assertContains "Capability cache waits should return the cached payload from the slower retry path." \
		"$output" "tool	zfs	0	/remote/bin/zfs"
	assertContains "Capability cache waits should record that a wait occurred when the slower retry path is used." \
		"$output" "wait_count=1"
	assertEquals "Invalid fast-retry counts should force capability waits onto the slower whole-second retry path." \
		"1" "$(wc -l <"$sleep_log" | tr -d ' ')"
}

test_zxfer_get_remote_host_operating_system_returns_capability_payload_os() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				fake_remote_capability_response
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should return the parsed capability payload when the capability handshake succeeds." \
		0 "$status"
	assertEquals "Remote OS lookups should return the OS field from the parsed capability payload." \
		"RemoteOS" "$output"
}

test_resolve_remote_required_tool_reads_parallel_and_cat_from_handshake() {
	parallel_output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				fake_remote_capability_response
			}
			zxfer_resolve_remote_required_tool "origin.example" parallel "GNU parallel"
		)
	)
	parallel_status=$?

	cat_output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				fake_remote_capability_response
			}
			zxfer_resolve_remote_required_tool "origin.example" cat "cat"
		)
	)
	cat_status=$?

	assertEquals "Capability handshakes should resolve GNU parallel from the parsed handshake payload." \
		0 "$parallel_status"
	assertEquals "Capability handshakes should return the parsed GNU parallel path." \
		"/opt/bin/parallel" "$parallel_output"
	assertEquals "Capability handshakes should resolve cat from the parsed handshake payload." \
		0 "$cat_status"
	assertEquals "Capability handshakes should return the parsed cat path." \
		"/remote/bin/cat" "$cat_output"
}

test_resolve_remote_required_tool_reports_missing_parallel_dependency_from_handshake() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	0	/remote/bin/cat
EOF
			}
			zxfer_resolve_remote_required_tool "origin.example" parallel "GNU parallel"
		)
	)
	status=$?

	assertEquals "Capability handshakes should fail when GNU parallel is reported missing on the remote host." \
		1 "$status"
	assertContains "Capability handshake failures should preserve the missing-dependency error for GNU parallel." \
		"$output" "Required dependency \"GNU parallel\" not found on host origin.example in secure PATH"
}

test_close_origin_and_target_ssh_control_socket_return_early_without_state() {
	g_option_O_origin_host=""
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket=""

	zxfer_close_origin_ssh_control_socket
	origin_status=$?
	zxfer_close_target_ssh_control_socket
	target_status=$?

	assertEquals "Origin control-socket cleanup should return early without error when no origin host is active." \
		0 "$origin_status"
	assertEquals "Target control-socket cleanup should return early without error when no target control socket is active." \
		0 "$target_status"
	assertEquals "Origin control-socket cleanup should leave the existing origin socket path unchanged when it returns early." \
		"$TEST_TMPDIR/origin.sock" "$g_ssh_origin_control_socket"
	assertEquals "Target control-socket cleanup should leave the target socket path empty when it returns early." \
		"" "$g_ssh_target_control_socket"
}

test_close_origin_and_target_ssh_control_socket_preserve_shared_entries_when_lock_fails() {
	origin_entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	origin_lease_file=$(zxfer_create_ssh_control_socket_lease_file "$origin_entry_dir")
	: >"$origin_entry_dir/s"
	target_entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	target_lease_file=$(zxfer_create_ssh_control_socket_lease_file "$target_entry_dir")
	: >"$target_entry_dir/s"

	(
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host="origin.example"
		g_ssh_origin_control_socket="$origin_entry_dir/s"
		g_ssh_origin_control_socket_dir="$origin_entry_dir"
		g_ssh_origin_control_socket_lease_file="$origin_lease_file"
		zxfer_acquire_ssh_control_socket_lock() {
			return 1
		}
		zxfer_close_origin_ssh_control_socket
	)

	(
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_T_target_host="target.example"
		g_ssh_target_control_socket="$target_entry_dir/s"
		g_ssh_target_control_socket_dir="$target_entry_dir"
		g_ssh_target_control_socket_lease_file="$target_lease_file"
		zxfer_acquire_ssh_control_socket_lock() {
			return 1
		}
		zxfer_close_target_ssh_control_socket
	)

	assertTrue "Origin shared control-socket cleanup should preserve the shared entry when lease-lock acquisition fails." \
		"[ -d '$origin_entry_dir' ]"
	assertTrue "Target shared control-socket cleanup should preserve the shared entry when lease-lock acquisition fails." \
		"[ -d '$target_entry_dir' ]"
}

test_close_origin_ssh_control_socket_preserves_state_on_transport_failure_without_lease() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_transport.err"
			socket_dir="$TEST_TMPDIR/origin_transport_socket_dir"
			mkdir -p "$socket_dir"
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Host key verification failed."
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$TEST_TMPDIR/origin_transport.sock"
			g_ssh_origin_control_socket_dir="$socket_dir"

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_transport.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' "$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'socket_dir=%s\n' "$g_ssh_origin_control_socket_dir"
		)
	)

	assertContains "Direct origin-socket closes should fail when ssh transport shutdown fails." \
		"$output" "status=1"
	assertContains "Direct origin-socket close failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Direct origin-socket close failures should preserve the cache directory for retry." \
		"$output" "dir_exists=yes"
	assertContains "Direct origin-socket close failures should preserve the in-process socket path." \
		"$output" "socket=$TEST_TMPDIR/origin_transport.sock"
	assertContains "Direct origin-socket close failures should preserve the in-process socket dir." \
		"$output" "socket_dir=$TEST_TMPDIR/origin_transport_socket_dir"
}

test_close_origin_ssh_control_socket_restores_last_lease_on_transport_failure() {
	errlog="$TEST_TMPDIR/close_origin_shared_transport.err"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_EXIT_STATUS=255
	FAKE_SSH_STDERR="Host key verification failed."
	export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"

	zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_shared_transport.out" 2>"$errlog"
	status=$?

	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR

	assertEquals "Last shared origin-socket lease release should fail closed on ssh transport errors." 1 "$status"
	assertContains "Last shared origin-socket lease failures should preserve the ssh transport diagnostic." \
		"$(cat "$errlog")" "Host key verification failed."
	assertTrue "Last shared origin-socket lease failures should preserve the cache entry for retry." \
		"[ -d '$entry_dir' ]"
	assertTrue "Last shared origin-socket lease failures should restore an active lease file for retry." \
		"[ -f '$g_ssh_origin_control_socket_lease_file' ]"
	assertNotEquals "Restored origin-socket leases should not reuse the removed lease path." \
		"$lease_file" "$g_ssh_origin_control_socket_lease_file"
	assertEquals "Last shared origin-socket lease failures should preserve the in-process socket path." \
		"$entry_dir/s" "$g_ssh_origin_control_socket"
}

test_close_origin_ssh_control_socket_restores_last_lease_on_transport_token_failure() {
	set +e
	output=$(
		(
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}

			zxfer_close_origin_ssh_control_socket 2>&1
			status=$?

			printf 'status=%s\n' "$status"
			printf 'lease=%s\n' "$g_ssh_origin_control_socket_lease_file"
			printf 'lease_exists=%s\n' \
				"$([ -f "$g_ssh_origin_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'lease_reused=%s\n' \
				"$([ "$lease_file" = "$g_ssh_origin_control_socket_lease_file" ] && printf yes || printf no)"
		)
	)
	status=$?

	assertEquals "Last shared origin-socket lease release should fail closed when transport token validation fails." \
		0 "$status"
	assertContains "Transport-token validation failures should preserve the original diagnostic." \
		"$output" "Managed ssh policy invalid."
	assertContains "Transport-token validation failures should preserve the failing close status." \
		"$output" "status=1"
	assertContains "Transport-token validation failures should restore an active lease file for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored origin-socket leases should not reuse the removed lease path after transport token failures." \
		"$output" "lease_reused=no"
}

test_close_origin_ssh_control_socket_reports_restore_lease_failure_after_close_error() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_restore_close.err"
			release_log="$TEST_TMPDIR/close_origin_restore_close.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-close-error.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_stderr="Host key verification failed."
				return 1
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_restore_close.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Origin-socket close failures should still fail closed when the replacement lease cannot be recreated." \
		"$output" "status=1"
	assertContains "Origin-socket close failures should preserve the ssh transport diagnostic when lease restoration fails." \
		"$output" "Host key verification failed."
	assertContains "Origin-socket close failures should report the lease-restoration failure explicitly." \
		"$output" "Error restoring ssh control socket lease for origin host."
	assertContains "Origin-socket close failures should still release the shared lock after lease restoration fails." \
		"$output" "released=$TEST_TMPDIR/origin-close-error.lock"
}

test_close_origin_ssh_control_socket_reports_restore_lease_failure_after_check_error() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_restore_check.err"
			release_log="$TEST_TMPDIR/close_origin_restore_check.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-check-error.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_stderr="Host key verification failed."
				return 1
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_restore_check.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Origin-socket check failures should still fail closed when the replacement lease cannot be recreated." \
		"$output" "status=1"
	assertContains "Origin-socket check failures should preserve the ssh transport diagnostic when lease restoration fails." \
		"$output" "Host key verification failed."
	assertContains "Origin-socket check failures should report the lease-restoration failure explicitly." \
		"$output" "Error restoring ssh control socket lease for origin host."
	assertContains "Origin-socket check failures should still release the shared lock after lease restoration fails." \
		"$output" "released=$TEST_TMPDIR/origin-check-error.lock"
}

test_close_origin_ssh_control_socket_reports_cleanup_failure_after_stale_exit_action() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_stale_exit_cleanup.err"
			release_log="$TEST_TMPDIR/close_origin_stale_exit_cleanup.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-stale-exit.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 54
			}

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_stale_exit_cleanup.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Shared origin-socket stale-master exit cleanup should fail closed when cache removal fails." \
		"$output" "status=54"
	assertContains "Shared origin-socket stale-master exit cleanup failures should report the cache cleanup error." \
		"$output" "Error removing ssh control socket cache directory for origin host."
	assertContains "Shared origin-socket stale-master exit cleanup failures should still release the shared lock." \
		"$output" "released=$TEST_TMPDIR/origin-stale-exit.lock"
}

test_close_origin_ssh_control_socket_reports_cleanup_failure_for_stale_shared_entry() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_stale_check_cleanup.err"
			release_log="$TEST_TMPDIR/close_origin_stale_check_cleanup.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-stale-check.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 55
			}

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_stale_check_cleanup.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Shared origin-socket stale-entry cleanup should fail closed when cache removal fails after the liveness check." \
		"$output" "status=55"
	assertContains "Shared origin-socket stale-entry cleanup failures should report the cache cleanup error." \
		"$output" "Error removing ssh control socket cache directory for origin host."
	assertContains "Shared origin-socket stale-entry cleanup failures should still release the shared lock." \
		"$output" "released=$TEST_TMPDIR/origin-stale-check.lock"
}

test_close_origin_ssh_control_socket_removes_stale_direct_entry_without_lease() {
	output=$(
		(
			socket_dir="$TEST_TMPDIR/origin_stale_socket_dir"
			mkdir -p "$socket_dir"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$socket_dir/s"
			g_ssh_origin_control_socket_dir="$socket_dir"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				g_zxfer_ssh_control_socket_action_command="ssh -S $socket_dir/s -O exit origin.example"
				return 1
			}

			if zxfer_close_origin_ssh_control_socket; then
				status=0
			else
				status=$?
			fi

			printf 'status=%s\n' "$status"
			printf 'dir_exists=%s\n' "$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "${g_ssh_origin_control_socket:-}"
			printf 'socket_dir=%s\n' "${g_ssh_origin_control_socket_dir:-}"
		)
	)

	assertContains "Direct origin-socket stale-master cleanup should still return success." \
		"$output" "status=0"
	assertContains "Direct origin-socket stale-master cleanup should remove the cached socket directory." \
		"$output" "dir_exists=no"
	assertContains "Direct origin-socket stale-master cleanup should clear the in-process socket path." \
		"$output" "socket="
	assertContains "Direct origin-socket stale-master cleanup should clear the in-process socket dir." \
		"$output" "socket_dir="
}

test_close_origin_ssh_control_socket_reports_cleanup_failure_for_stale_direct_entry() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_stale_direct_cleanup.err"
			socket_dir="$TEST_TMPDIR/origin_stale_direct_cleanup_dir"
			mkdir -p "$socket_dir"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$socket_dir/s"
			g_ssh_origin_control_socket_dir="$socket_dir"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 56
			}

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_stale_direct_cleanup.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' "$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'socket_dir=%s\n' "$g_ssh_origin_control_socket_dir"
		)
	)

	assertContains "Direct origin-socket stale-master cleanup should fail closed when cache removal fails." \
		"$output" "status=56"
	assertContains "Direct origin-socket stale-master cleanup failures should report the cache cleanup error." \
		"$output" "Error removing ssh control socket cache directory for origin host."
	assertContains "Direct origin-socket stale-master cleanup failures should preserve the cache directory for retry." \
		"$output" "dir_exists=yes"
	assertContains "Direct origin-socket stale-master cleanup failures should preserve the in-process socket path." \
		"$output" "socket=$TEST_TMPDIR/origin_stale_direct_cleanup_dir/s"
	assertContains "Direct origin-socket stale-master cleanup failures should preserve the in-process socket dir." \
		"$output" "socket_dir=$TEST_TMPDIR/origin_stale_direct_cleanup_dir"
}

test_close_target_ssh_control_socket_restores_last_lease_on_transport_failure() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_shared_transport.err"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Host key verification failed."
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_shared_transport.out" 2>"$errlog" || status=$?
			status=${status:-0}

			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'entry_exists=%s\n' "$([ -d "$entry_dir" ] && printf yes || printf no)"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
			printf 'lease_exists=%s\n' \
				"$([ -f "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'lease_reused=%s\n' \
				"$([ "$lease_file" = "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
		)
	)

	assertContains "Last shared target-socket lease release should fail closed on ssh transport errors." \
		"$output" "status=1"
	assertContains "Last shared target-socket lease failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Last shared target-socket lease failures should preserve the cache entry for retry." \
		"$output" "entry_exists=yes"
	assertContains "Last shared target-socket lease failures should restore an active lease file for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored target-socket leases should not reuse the removed lease path." \
		"$output" "lease_reused=no"
	assertContains "Last shared target-socket lease failures should preserve the in-process socket path." \
		"$output" "socket="
}

test_close_target_ssh_control_socket_restores_last_lease_on_check_failure() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_shared_check.err"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Host key verification failed."
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_shared_check.out" 2>"$errlog" || status=$?
			status=${status:-0}

			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'entry_exists=%s\n' "$([ -d "$entry_dir" ] && printf yes || printf no)"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
			printf 'lease_exists=%s\n' \
				"$([ -f "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'lease_reused=%s\n' \
				"$([ "$lease_file" = "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
		)
	)

	assertContains "Last shared target-socket check failures should fail closed." \
		"$output" "status=1"
	assertContains "Last shared target-socket check failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Last shared target-socket check failures should preserve the cache entry for retry." \
		"$output" "entry_exists=yes"
	assertContains "Last shared target-socket check failures should restore an active lease file for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored target-socket leases should not reuse the removed lease path after check failures." \
		"$output" "lease_reused=no"
	assertContains "Last shared target-socket check failures should preserve the in-process socket path." \
		"$output" "socket="
}

test_close_target_ssh_control_socket_reports_restore_lease_failure_after_close_error() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_restore_close.err"
			release_log="$TEST_TMPDIR/close_target_restore_close.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-close-error.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_stderr="Host key verification failed."
				return 1
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_restore_close.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Target-socket close failures should still fail closed when the replacement lease cannot be recreated." \
		"$output" "status=1"
	assertContains "Target-socket close failures should preserve the ssh transport diagnostic when lease restoration fails." \
		"$output" "Host key verification failed."
	assertContains "Target-socket close failures should report the lease-restoration failure explicitly." \
		"$output" "Error restoring ssh control socket lease for target host."
	assertContains "Target-socket close failures should still release the shared lock after lease restoration fails." \
		"$output" "released=$TEST_TMPDIR/target-close-error.lock"
}

test_close_target_ssh_control_socket_reports_cleanup_failure_after_stale_exit_action() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_stale_exit_cleanup.err"
			release_log="$TEST_TMPDIR/close_target_stale_exit_cleanup.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-stale-exit.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 57
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_stale_exit_cleanup.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Shared target-socket stale-master exit cleanup should fail closed when cache removal fails." \
		"$output" "status=57"
	assertContains "Shared target-socket stale-master exit cleanup failures should report the cache cleanup error." \
		"$output" "Error removing ssh control socket cache directory for target host."
	assertContains "Shared target-socket stale-master exit cleanup failures should still release the shared lock." \
		"$output" "released=$TEST_TMPDIR/target-stale-exit.lock"
}

test_close_target_ssh_control_socket_reports_cleanup_failure_for_stale_shared_entry() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_stale_check_cleanup.err"
			release_log="$TEST_TMPDIR/close_target_stale_check_cleanup.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-stale-check.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 58
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_stale_check_cleanup.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Shared target-socket stale-entry cleanup should fail closed when cache removal fails after the liveness check." \
		"$output" "status=58"
	assertContains "Shared target-socket stale-entry cleanup failures should report the cache cleanup error." \
		"$output" "Error removing ssh control socket cache directory for target host."
	assertContains "Shared target-socket stale-entry cleanup failures should still release the shared lock." \
		"$output" "released=$TEST_TMPDIR/target-stale-check.lock"
}

test_close_target_ssh_control_socket_reports_restore_lease_failure_after_check_error() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_restore_check.err"
			release_log="$TEST_TMPDIR/close_target_restore_check.release"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-check-error.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				:
			}
			zxfer_count_ssh_control_socket_leases() {
				printf '%s\n' "0"
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_stderr="Host key verification failed."
				return 1
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_restore_check.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'released=%s\n' "$(cat "$release_log")"
		)
	)

	assertContains "Target-socket check failures should still fail closed when the replacement lease cannot be recreated." \
		"$output" "status=1"
	assertContains "Target-socket check failures should preserve the ssh transport diagnostic when lease restoration fails." \
		"$output" "Host key verification failed."
	assertContains "Target-socket check failures should report the lease-restoration failure explicitly." \
		"$output" "Error restoring ssh control socket lease for target host."
	assertContains "Target-socket check failures should still release the shared lock after lease restoration fails." \
		"$output" "released=$TEST_TMPDIR/target-check-error.lock"
}

test_close_target_ssh_control_socket_clears_state_when_other_leases_remain() {
	log="$TEST_TMPDIR/close_target_shared_sibling.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example doas")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	sibling_lease=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"

	zxfer_close_target_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Shared target sockets should clear the in-process socket path after releasing the local lease." "" \
		"$g_ssh_target_control_socket"
	assertEquals "Shared target sockets should clear the in-process lease path after releasing the local lease." "" \
		"$g_ssh_target_control_socket_lease_file"
	assertFalse "Closing a shared target socket should remove only the current process lease when other leases remain." \
		"[ -e '$lease_file' ]"
	assertTrue "Closing a shared target socket should preserve the sibling lease while other clients still depend on the socket." \
		"[ -f '$sibling_lease' ]"
	assertTrue "Closing a shared target socket should preserve the cache entry while sibling leases remain." \
		"[ -d '$entry_dir' ]"
	assertEquals "Closing a shared target socket should not send ssh -O exit while sibling leases remain." "" \
		"$(cat "$log" 2>/dev/null)"
}

test_close_target_ssh_control_socket_removes_stale_direct_entry_without_lease() {
	output=$(
		(
			socket_dir="$TEST_TMPDIR/target_stale_socket_dir"
			mkdir -p "$socket_dir"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$socket_dir/s"
			g_ssh_target_control_socket_dir="$socket_dir"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				g_zxfer_ssh_control_socket_action_command="ssh -S $socket_dir/s -O exit target.example"
				return 1
			}

			if zxfer_close_target_ssh_control_socket; then
				status=0
			else
				status=$?
			fi

			printf 'status=%s\n' "$status"
			printf 'dir_exists=%s\n' "$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "${g_ssh_target_control_socket:-}"
			printf 'socket_dir=%s\n' "${g_ssh_target_control_socket_dir:-}"
		)
	)

	assertContains "Direct target-socket stale-master cleanup should still return success." \
		"$output" "status=0"
	assertContains "Direct target-socket stale-master cleanup should remove the cached socket directory." \
		"$output" "dir_exists=no"
	assertContains "Direct target-socket stale-master cleanup should clear the in-process socket path." \
		"$output" "socket="
	assertContains "Direct target-socket stale-master cleanup should clear the in-process socket dir." \
		"$output" "socket_dir="
}

test_close_target_ssh_control_socket_reports_cleanup_failure_for_stale_direct_entry() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_stale_direct_cleanup.err"
			socket_dir="$TEST_TMPDIR/target_stale_direct_cleanup_dir"
			mkdir -p "$socket_dir"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$socket_dir/s"
			g_ssh_target_control_socket_dir="$socket_dir"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 59
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_stale_direct_cleanup.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' "$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'socket_dir=%s\n' "$g_ssh_target_control_socket_dir"
		)
	)

	assertContains "Direct target-socket stale-master cleanup should fail closed when cache removal fails." \
		"$output" "status=59"
	assertContains "Direct target-socket stale-master cleanup failures should report the cache cleanup error." \
		"$output" "Error removing ssh control socket cache directory for target host."
	assertContains "Direct target-socket stale-master cleanup failures should preserve the cache directory for retry." \
		"$output" "dir_exists=yes"
	assertContains "Direct target-socket stale-master cleanup failures should preserve the in-process socket path." \
		"$output" "socket=$TEST_TMPDIR/target_stale_direct_cleanup_dir/s"
	assertContains "Direct target-socket stale-master cleanup failures should preserve the in-process socket dir." \
		"$output" "socket_dir=$TEST_TMPDIR/target_stale_direct_cleanup_dir"
}

test_close_target_ssh_control_socket_preserves_state_on_transport_failure_without_lease() {
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_transport.err"
			socket_dir="$TEST_TMPDIR/target_transport_socket_dir"
			mkdir -p "$socket_dir"
			FAKE_SSH_EXIT_STATUS=255
			FAKE_SSH_STDERR="Host key verification failed."
			export FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$TEST_TMPDIR/target_transport.sock"
			g_ssh_target_control_socket_dir="$socket_dir"

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_transport.out" 2>"$errlog" || status=$?
			status=${status:-0}
			printf 'status=%s\n' "$status"
			printf 'stderr=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' "$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'socket_dir=%s\n' "$g_ssh_target_control_socket_dir"
		)
	)

	assertContains "Direct target-socket closes should fail when ssh transport shutdown fails." \
		"$output" "status=1"
	assertContains "Direct target-socket close failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Direct target-socket close failures should preserve the cache directory for retry." \
		"$output" "dir_exists=yes"
	assertContains "Direct target-socket close failures should preserve the in-process socket path." \
		"$output" "socket=$TEST_TMPDIR/target_transport.sock"
	assertContains "Direct target-socket close failures should preserve the in-process socket dir." \
		"$output" "socket_dir=$TEST_TMPDIR/target_transport_socket_dir"
}

test_close_all_ssh_control_sockets_preserves_first_failure_status() {
	output=$(
		(
			zxfer_close_origin_ssh_control_socket() {
				return 7
			}
			zxfer_close_target_ssh_control_socket() {
				return 9
			}

			if zxfer_close_all_ssh_control_sockets; then
				status=0
			else
				status=$?
			fi
			printf 'status=%s\n' "$status"
		)
	)

	assertContains "Global ssh control-socket cleanup should preserve the first close failure status." \
		"$output" "status=7"
}

test_close_all_ssh_control_sockets_delegates_to_both_roles() {
	output=$(
		(
			zxfer_close_origin_ssh_control_socket() {
				printf '%s\n' origin
			}
			zxfer_close_target_ssh_control_socket() {
				printf '%s\n' target
			}
			zxfer_close_all_ssh_control_sockets
		)
	)

	assertEquals "Global ssh control-socket cleanup should delegate to both the origin and target cleanup helpers." \
		"origin
target" "$output"
}

test_setup_ssh_control_socket_reports_existing_target_close_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_close_target_ssh_control_socket() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "should-not-run"
			}
			g_ssh_target_control_socket="$TEST_TMPDIR/existing.sock"
			g_ssh_target_control_socket_dir="$TEST_TMPDIR/existing.dir"
			zxfer_setup_ssh_control_socket "target.example" "target"
		) 2>&1
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when an existing target socket cannot be closed." 1 "$status"
	assertContains "Existing target-socket close failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Existing target-socket close failures should use the current setup error message." \
		"$output" "Error closing ssh control socket for target host."
	assertNotContains "Target setup should stop before allocating a new shared cache entry when the old close fails." \
		"$output" "should-not-run"
}

test_setup_ssh_control_socket_reports_target_check_failures_before_open() {
	set +e
	output=$(
		(
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_stderr="Host key verification failed."
				return 1
			}
			zxfer_open_ssh_control_socket_for_host() {
				printf '%s\n' "should-not-run"
			}

			zxfer_setup_ssh_control_socket "target.example" "target"
		) 2>&1
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when target socket health checks return transport errors." \
		1 "$status"
	assertContains "Target socket health-check failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Target socket health-check failures should keep the setup error message." \
		"$output" "Error creating ssh control socket for target host."
	assertNotContains "Target socket setup should stop before opening a new master when the health check fails." \
		"$output" "should-not-run"
}

test_resolve_remote_required_tool_reports_generic_failure_for_unknown_tool() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				fake_remote_capability_response
			}
			zxfer_resolve_remote_required_tool "origin.example" unknown "mystery tool"
		)
	)
	status=$?

	assertEquals "Capability handshakes should fail cleanly for unknown remote helper names." \
		1 "$status"
	assertContains "Unknown remote helper lookups should preserve the generic dependency-query failure." \
		"$output" "Failed to query dependency \"mystery tool\" on host origin.example."
}

test_zxfer_get_remote_host_operating_system_preserves_direct_probe_failure_after_parse_failure() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
			}
			zxfer_parse_remote_capability_response() {
				return 1
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "Host key verification failed."
				return 1
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should fail cleanly when both the capability payload and the direct probe fail." \
		1 "$status"
	assertContains "Remote OS fallback failures should preserve the direct-probe diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_get_remote_resolved_tool_version_line_preserves_probe_failure_output() {
	set +e
	output=$(
		(
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "citation preamble"
				return 1
			}
			zxfer_get_remote_resolved_tool_version_line "origin.example" "/opt/bin/parallel" "GNU parallel"
		)
	)
	status=$?

	assertEquals "Remote helper version-line probes should fail cleanly when the version query fails." \
		1 "$status"
	assertContains "Remote helper version-line probe failures should preserve the captured output." \
		"$output" "citation preamble"
}

test_setup_ssh_control_socket_reports_existing_close_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_close_origin_ssh_control_socket() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "should-not-run"
			}
			g_ssh_origin_control_socket="$TEST_TMPDIR/existing.sock"
			g_ssh_origin_control_socket_dir="$TEST_TMPDIR/existing.dir"
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		) 2>&1
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when an existing origin socket cannot be closed." 1 "$status"
	assertContains "Existing-socket close failures should preserve the ssh transport diagnostic." \
		"$output" "Host key verification failed."
	assertContains "Existing-socket close failures should use the current setup error message." \
		"$output" "Error closing ssh control socket for origin host."
	assertNotContains "Setup should stop before allocating a new shared cache entry when the old close fails." \
		"$output" "should-not-run"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
