#!/bin/sh
#
# Additional shunit2 coverage for metadata-backed remote-host locking paths.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_replication.sh"

cleanup_private_remote_host_cache_dirs() {
	if effective_uid=$(zxfer_get_effective_user_uid 2>/dev/null); then
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
	g_zxfer_remote_capability_cache_wait_retries=5
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	zxfer_init_temp_artifacts
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	if command -v zxfer_reset_owned_lock_tracking >/dev/null 2>&1; then
		zxfer_reset_owned_lock_tracking
	fi
	create_fake_ssh_bin
}

write_owned_lock_metadata_fixture() {
	l_lock_dir=$1
	l_kind=$2
	l_purpose=$3
	l_pid=${4:-$$}
	l_start_token=${5:-}
	l_hostname=${6:-}
	l_created_at=${7:-}

	mkdir -p "$l_lock_dir" || fail "Unable to create owned lock fixture directory."
	chmod 700 "$l_lock_dir" || fail "Unable to chmod owned lock fixture directory."
	if [ -z "$l_start_token" ]; then
		l_start_token=$(zxfer_get_process_start_token "$$" 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture start token."
	fi
	if [ -z "$l_hostname" ]; then
		l_hostname=$(zxfer_get_owned_lock_hostname 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture hostname."
	fi
	if [ -z "$l_created_at" ]; then
		l_created_at=$(zxfer_get_owned_lock_created_at 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture creation timestamp."
	fi

	cat >"$l_lock_dir/metadata" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	$l_kind
purpose	$l_purpose
pid	$l_pid
start_token	$l_start_token
hostname	$l_hostname
created_at	$l_created_at
EOF
	chmod 600 "$l_lock_dir/metadata" || fail "Unable to chmod owned lock fixture metadata."
}

test_zxfer_validate_ssh_control_socket_lock_dir_reports_missing_and_corrupt_metadata() {
	lock_dir="$TEST_TMPDIR/ssh_lock_validate"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "Missing ssh control socket lock dirs should fail cleanly." \
		"$output" "status=1"
	assertContains "Missing ssh control socket lock dirs should preserve the missing-path diagnostic." \
		"$output" "error=ssh control socket lock path \"$lock_dir\" is missing."

	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "not-lock-metadata" >"$lock_dir/metadata"
	chmod 600 "$lock_dir/metadata"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "Corrupt ssh control socket lock metadata should fail cleanly." \
		"$output" "status=1"
	assertContains "Corrupt ssh control socket lock metadata should preserve the lock-path context in the staged diagnostic." \
		"$output" "error=ssh control socket lock path \"$lock_dir\""
}

test_zxfer_validate_ssh_control_socket_lock_dir_accepts_valid_metadata() {
	lock_dir="$TEST_TMPDIR/ssh_lock_validate_valid"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_ssh_control_socket_lock_purpose)"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Valid ssh control socket lock dirs should pass metadata validation." \
		"$output" "status=0"
}

test_zxfer_emit_ssh_control_socket_lock_failure_message_covers_staged_and_default_paths() {
	zxfer_reset_ssh_control_socket_lock_state
	blank_output=$(zxfer_emit_ssh_control_socket_lock_failure_message)
	blank_status=$?
	default_output=$(zxfer_emit_ssh_control_socket_lock_failure_message "default failure.")
	default_status=$?
	zxfer_note_ssh_control_socket_lock_error "staged ssh lock failure"
	staged_output=$(zxfer_emit_ssh_control_socket_lock_failure_message)
	staged_status=$?
	staged_default_output=$(zxfer_emit_ssh_control_socket_lock_failure_message "Lock failure.")
	staged_default_status=$?

	assertEquals "ssh control socket lock failure message emission should stay silent when neither a staged nor a default message is present." \
		"" "$blank_output"
	assertEquals "ssh control socket lock failure message emission should still succeed when no message is emitted." \
		0 "$blank_status"
	assertEquals "ssh control socket lock failure message emission should print the default message when no staged error is present." \
		"default failure." "$default_output"
	assertEquals "ssh control socket lock failure message emission should succeed when printing the default message." \
		0 "$default_status"
	assertEquals "ssh control socket lock failure message emission should print the staged error directly when no default is supplied." \
		"staged ssh lock failure" "$staged_output"
	assertEquals "ssh control socket lock failure message emission should succeed when printing the staged message directly." \
		0 "$staged_status"
	assertEquals "ssh control socket lock failure message emission should append the staged detail to the default prefix when both are available." \
		"Lock failure: staged ssh lock failure" "$staged_default_output"
	assertEquals "ssh control socket lock failure message emission should succeed when combining the default prefix with the staged detail." \
		0 "$staged_default_status"
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

test_zxfer_ssh_control_socket_identity_helpers_propagate_transport_policy_failures() {
	output=$(
		(
			set +e
			zxfer_render_ssh_transport_policy_identity() {
				printf '%s\n' "transport policy failed"
				return 7
			}
			cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
			printf 'cache_status=%s\n' "$?"
			printf 'cache_output=%s\n' "$cache_key"
			identity=$(zxfer_render_ssh_control_socket_entry_identity "origin.example")
			printf 'identity_status=%s\n' "$?"
			printf 'identity_output=%s\n' "$identity"
		)
	)

	assertContains "ssh control socket cache-key rendering should preserve non-empty transport-policy failure output." \
		"$output" "cache_status=1"
	assertContains "ssh control socket cache-key rendering should surface the transport-policy failure text." \
		"$output" "cache_output=transport policy failed"
	assertContains "ssh control socket identity rendering should fail when transport-policy rendering fails." \
		"$output" "identity_status=1"
	assertContains "ssh control socket identity rendering should surface the transport-policy failure text." \
		"$output" "identity_output=transport policy failed"
}

test_zxfer_validate_ssh_control_socket_lock_dir_distinguishes_missing_metadata_from_hard_validation_failures() {
	lock_dir="$TEST_TMPDIR/ssh_lock_validate_statuses"
	mkdir "$lock_dir" || fail "Unable to create ssh lock validation fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod ssh lock validation fixture directory."

	output=$(
		(
			set +e
			g_test_load_status=2
			zxfer_load_owned_lock_metadata_for_kind_and_purpose() {
				return "$g_test_load_status"
			}
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
			printf 'missing_status=%s\n' "$?"
			printf 'missing_error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
			g_test_load_status=1
			zxfer_validate_ssh_control_socket_lock_dir "$lock_dir"
			printf 'hard_status=%s\n' "$?"
			printf 'hard_error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "ssh control socket lock validation should treat missing or invalid metadata as a distinct status." \
		"$output" "missing_status=1"
	assertContains "ssh control socket lock validation should preserve the missing-or-invalid-metadata diagnostic." \
		"$output" "missing_error=ssh control socket lock path \"$lock_dir\" has missing or invalid metadata."
	assertContains "ssh control socket lock validation should fail closed for hard ownership or metadata validation failures." \
		"$output" "hard_status=1"
	assertContains "ssh control socket lock validation should preserve the generic hard-validation diagnostic." \
		"$output" "hard_error=ssh control socket lock path \"$lock_dir\" failed ownership, permission, or metadata validation."
}

test_zxfer_validate_ssh_control_socket_lock_dir_for_reap_rejects_symlinked_and_insecure_dirs() {
	lock_target="$TEST_TMPDIR/ssh_lock_reap_target"
	lock_link="$TEST_TMPDIR/ssh_lock_reap_link"
	lock_dir="$TEST_TMPDIR/ssh_lock_reap_dir"
	mkdir "$lock_target" "$lock_dir"
	chmod 700 "$lock_target" "$lock_dir"
	ln -s "$lock_target" "$lock_link"

	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_link"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "Reap-time validation should reject symlinked ssh control socket lock dirs." \
		"$output" "status=1"
	assertContains "Reap-time validation should preserve the symlink diagnostic." \
		"$output" "error=Refusing symlinked ssh control socket lock path \"$lock_link\"."

	chmod 777 "$lock_dir"
	output=$(
		(
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "Reap-time validation should reject insecure ssh control socket lock dirs." \
		"$output" "status=1"
	assertContains "Reap-time validation should preserve the specific unsupported-permissions diagnostic." \
		"$output" "error=Existing ssh control socket lock path \"$lock_dir\" has unsupported permissions (777). Remove the stale lock directory and retry."
}

test_zxfer_validate_ssh_control_socket_lock_dir_for_reap_reports_lookup_failures() {
	lock_dir="$TEST_TMPDIR/ssh_lock_reap_lookup"
	mkdir "$lock_dir" || fail "Unable to create ssh lock reap lookup fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod ssh lock reap lookup fixture directory."

	uid_output=$(
		(
			set +e
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	owner_output=$(
		(
			set +e
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	mismatch_output=$(
		(
			set +e
			zxfer_get_effective_user_uid() {
				printf '%s\n' "111"
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' "222"
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	mode_output=$(
		(
			set +e
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "Reap-time validation should fail closed when effective-uid lookup fails." \
		"$uid_output" "status=1"
	assertContains "Reap-time validation should preserve the effective-uid lookup diagnostic." \
		"$uid_output" "error=Unable to determine the effective uid for ssh control socket lock validation."
	assertContains "Reap-time validation should fail closed when owner lookup fails." \
		"$owner_output" "status=1"
	assertContains "Reap-time validation should preserve the owner-lookup diagnostic." \
		"$owner_output" "error=Unable to determine the owner of ssh control socket lock path \"$lock_dir\"."
	assertContains "Reap-time validation should fail closed when the lock directory is owned by another uid." \
		"$mismatch_output" "status=1"
	assertContains "Reap-time validation should preserve the ownership-mismatch diagnostic." \
		"$mismatch_output" "error=ssh control socket lock path \"$lock_dir\" is not owned by the effective uid."
	assertContains "Reap-time validation should fail closed when mode lookup fails." \
		"$mode_output" "status=1"
	assertContains "Reap-time validation should preserve the mode-lookup diagnostic." \
		"$mode_output" "error=Unable to determine permissions for ssh control socket lock path \"$lock_dir\"."
}

test_zxfer_try_reap_stale_ssh_control_socket_lock_dir_defers_and_then_reaps_corrupt_dirs() {
	lock_dir="$TEST_TMPDIR/ssh_lock_reap_corrupt"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	output=$(
		(
			set +e
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_dir" 0 >/dev/null
			printf 'defer=%s\n' "$?"
			printf 'exists_after_defer=%s\n' "$([ -d "$lock_dir" ] && printf yes || printf no)"
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_dir" 1 >/dev/null
			printf 'reap=%s\n' "$?"
			printf 'exists_after_reap=%s\n' "$([ -e "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Corrupt ssh control socket lock dirs should be deferred before the bounded wait enables corrupt reaping." \
		"$output" "defer=2"
	assertContains "Deferred corrupt ssh control socket lock dirs should remain in place." \
		"$output" "exists_after_defer=yes"
	assertContains "Corrupt ssh control socket lock dirs should be reaped once corrupt reaping is enabled." \
		"$output" "reap=0"
	assertContains "Enabled corrupt ssh control socket lock reaping should remove the directory." \
		"$output" "exists_after_reap=no"
}

test_zxfer_cleanup_ssh_control_socket_lock_dir_surfaces_shared_cleanup_failures() {
	lock_dir="$TEST_TMPDIR/ssh_lock_cleanup_fail"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	output=$(
		(
			zxfer_cleanup_owned_lock_dir() {
				return 1
			}
			zxfer_cleanup_ssh_control_socket_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)
	assertContains "ssh control socket lock cleanup should fail when the shared cleanup helper fails." \
		"$output" "status=1"
	assertContains "ssh control socket lock cleanup should preserve the removal diagnostic." \
		"$output" "error=Unable to remove stale ssh control socket lock path \"$lock_dir\"."
}

test_zxfer_cleanup_ssh_control_socket_lock_dir_returns_success_for_shared_cleanup_success() {
	lock_dir="$TEST_TMPDIR/ssh_lock_cleanup_success"

	output=$(
		(
			zxfer_cleanup_owned_lock_dir() {
				return 0
			}
			zxfer_cleanup_ssh_control_socket_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "ssh control socket lock cleanup should return success when the shared cleanup helper succeeds." \
		"$output" "status=0"
}

test_zxfer_create_ssh_control_socket_lock_dir_validates_existing_invalid_dir() {
	lock_dir="$TEST_TMPDIR/ssh_lock_create_invalid"
	marker_file="$TEST_TMPDIR/ssh_lock_create_invalid.marker"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	output=$(
		(
			zxfer_create_owned_lock_dir() {
				return 1
			}
			zxfer_validate_ssh_control_socket_lock_dir() {
				printf '%s\n' "$1" >"$marker_file"
				return 1
			}
			zxfer_create_ssh_control_socket_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
		)
	)

	assertEquals "ssh control socket lock creation should validate an existing lock dir when shared creation fails." \
		"$lock_dir" "$(cat "$marker_file")"
	assertContains "ssh control socket lock creation should return failure when shared creation fails for an existing lock dir." \
		"$output" "status=1"
}

test_zxfer_try_reap_stale_ssh_control_socket_lock_dir_maps_shared_reap_statuses() {
	lock_dir="$TEST_TMPDIR/ssh_lock_reap_status_map"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_ssh_control_socket_lock_purpose)" "999999999"

	output=$(
		(
			set +e
			g_test_reap_status=0
			zxfer_validate_ssh_control_socket_lock_dir_for_reap() {
				return 0
			}
			zxfer_try_reap_stale_owned_lock_dir() {
				return "$g_test_reap_status"
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_dir" 1 >/dev/null
			printf 'status0=%s\n' "$?"
			g_test_reap_status=2
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_dir" 1 >/dev/null
			printf 'status2=%s\n' "$?"
			g_test_reap_status=1
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_dir" 1 >/dev/null
			printf 'status1=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "ssh control socket reap should report success when the shared helper removes the stale lock dir." \
		"$output" "status0=0"
	assertContains "ssh control socket reap should defer when the shared helper reports a live owner." \
		"$output" "status2=2"
	assertContains "ssh control socket reap should fail closed when the shared helper reports an unrecoverable error." \
		"$output" "status1=1"
	assertContains "ssh control socket reap should preserve the stale-lock diagnostic when the shared helper fails." \
		"$output" "error=Unable to reap stale or corrupt ssh control socket lock path \"$lock_dir\"."
}

test_zxfer_acquire_ssh_control_socket_lock_invalid_fast_retry_env_still_surfaces_final_reap_failure() {
	entry_dir="$TEST_TMPDIR/ssh_lock_invalid_fast_retry_entry"
	lock_dir="$entry_dir.lock"
	sleep_log="$TEST_TMPDIR/ssh_lock_invalid_fast_retry.sleep"
	mkdir "$entry_dir" "$lock_dir" ||
		fail "Unable to create ssh lock invalid fast-retry fixtures."
	chmod 700 "$lock_dir" ||
		fail "Unable to chmod ssh lock invalid fast-retry lock directory."

	output=$(
		(
			set +e
			ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=bogus
			zxfer_create_ssh_control_socket_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_ssh_control_socket_lock_dir() {
				if [ "$2" = "1" ]; then
					zxfer_note_ssh_control_socket_lock_error \
						"Unable to reap stale or corrupt ssh control socket lock path \"$1\"."
					return 1
				fi
				return 2
			}
			sleep() {
				printf 'sleep\n' >>"$sleep_log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "ssh control socket lock acquisition should fail closed when the final reap attempt reports an unrecoverable error." \
		"$output" "status=1"
	assertContains "ssh control socket lock acquisition should preserve the final reap failure diagnostic." \
		"$output" "error=Unable to reap stale or corrupt ssh control socket lock path \"$lock_dir\"."
	assertEquals "ssh control socket lock acquisition should treat invalid fast-retry settings as zero and fall back to the whole-second wait path." \
		"9" "$(wc -l <"$sleep_log" | tr -d ' ')"
}

test_zxfer_try_reap_stale_ssh_control_socket_lock_dir_rejects_unsupported_pid_file_layout() {
	lock_dir="$TEST_TMPDIR/ssh_lock_unsupported_pid"

	mkdir "$lock_dir" || fail "Unable to create unsupported ssh pid-lock fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod unsupported ssh pid-lock fixture directory."
	printf '%s\n' "$$" >"$lock_dir/pid" ||
		fail "Unable to write unsupported ssh pid-lock fixture pid file."
	chmod 600 "$lock_dir/pid" ||
		fail "Unable to chmod unsupported ssh pid-lock fixture pid file."

	output=$(
		(
			set +e
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_dir" 1 >/dev/null
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "ssh control socket reap should fail closed for unsupported pid-file lock directories." \
		"$output" "status=1"
	assertContains "ssh control socket reap should preserve the unsupported-layout diagnostic for pid-file lock directories." \
		"$output" "error=ssh control socket lock path \"$lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
}

test_zxfer_release_ssh_control_socket_lease_file_unregisters_owned_cleanup_path() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	zxfer_create_ssh_control_socket_lease_file "$entry_dir" >/dev/null
	lease_dir=$g_zxfer_runtime_artifact_path_result

	assertContains "Creating a shared ssh control socket lease should register it for owned-lock cleanup." \
		"${g_zxfer_owned_lock_cleanup_paths:-}" "$lease_dir"

	zxfer_release_ssh_control_socket_lease_file "$lease_dir"

	assertFalse "Releasing a shared ssh control socket lease should remove the lease directory." \
		"[ -e '$lease_dir' ]"
	assertNotContains "Releasing a shared ssh control socket lease should unregister it from owned-lock cleanup." \
		"${g_zxfer_owned_lock_cleanup_paths:-}" "$lease_dir"
}

test_zxfer_release_ssh_control_socket_lock_delegates_to_shared_release_helper() {
	lock_dir="$TEST_TMPDIR/ssh_lock_release_delegate"
	call_log="$TEST_TMPDIR/ssh_lock_release_delegate.log"

	(
		zxfer_release_owned_lock_dir() {
			printf 'path=%s kind=%s purpose=%s\n' "$1" "$2" "$3" >>"$call_log"
			return 0
		}
		zxfer_release_ssh_control_socket_lock "$lock_dir"
	)
	output=$(cat "$call_log")

	assertContains "ssh control socket lock release should delegate to the shared release helper with the lock purpose." \
		"$output" "path=$lock_dir kind=lock purpose=ssh-control-socket-lock"
}

test_zxfer_release_ssh_control_socket_lock_records_failure_message() {
	lock_dir="$TEST_TMPDIR/ssh_lock_release_failure"

	output=$(
		(
			set +e
			zxfer_release_owned_lock_dir() {
				return 19
			}
			zxfer_release_ssh_control_socket_lock "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "ssh control socket lock release should fail closed when the shared release helper fails." \
		"$output" "status=1"
	assertContains "ssh control socket lock release should preserve a specific failure message for later callers." \
		"$output" "error=Failed to release ssh control socket lock path \"$lock_dir\"."
}

test_zxfer_release_ssh_control_socket_lock_with_precedence_warns_and_preserves_primary_status() {
	lock_dir="$TEST_TMPDIR/ssh_lock_release_precedence"

	output=$(
		(
			set +e
			zxfer_release_ssh_control_socket_lock() {
				zxfer_note_ssh_control_socket_lock_error \
					"Failed to release ssh control socket lock path \"$1\"."
				return 19
			}
			zxfer_release_ssh_control_socket_lock_with_precedence \
				origin "$lock_dir" 73
			printf 'status=%s\n' "$?"
			zxfer_release_ssh_control_socket_lock_with_precedence \
				origin "$lock_dir" 0
			printf 'status2=%s\n' "$?"
		) 2>&1
	)

	assertContains "ssh control socket lock-release precedence should preserve the primary failure status." \
		"$output" "status=73"
	assertContains "ssh control socket lock-release precedence should fail closed when release fails without a primary failure." \
		"$output" "status2=1"
	assertContains "ssh control socket lock-release precedence should emit the release warning with the shared failure detail." \
		"$output" "Warning: Failed to release ssh control socket lock for origin host: Failed to release ssh control socket lock path \"$lock_dir\"."
}

test_zxfer_release_ssh_control_socket_lock_with_precedence_returns_primary_for_empty_and_successful_release() {
	output=$(
		(
			set +e
			zxfer_release_ssh_control_socket_lock_with_precedence \
				origin "" 73
			printf 'empty=%s\n' "$?"
			zxfer_release_ssh_control_socket_lock() {
				return 0
			}
			zxfer_release_ssh_control_socket_lock_with_precedence \
				origin "$TEST_TMPDIR/ssh_lock_release_ok" 41
			printf 'ok=%s\n' "$?"
		)
	)

	assertContains "ssh control socket lock-release precedence should return the primary status unchanged when no lock path was recorded." \
		"$output" "empty=73"
	assertContains "ssh control socket lock-release precedence should return the primary status unchanged when release succeeds." \
		"$output" "ok=41"
}

test_zxfer_create_ssh_control_socket_lock_dir_and_remote_capability_lock_dir_delegate_to_shared_purposes() {
	lock_dir="$TEST_TMPDIR/ssh_lock_delegate"
	remote_lock_dir="$TEST_TMPDIR/remote_lock_delegate"
	call_log="$TEST_TMPDIR/lock_delegate.log"

	(
		zxfer_create_owned_lock_dir() {
			printf 'path=%s kind=%s purpose=%s\n' "$1" "$2" "$3" >>"$call_log"
			return 0
		}
		zxfer_create_ssh_control_socket_lock_dir "$lock_dir"
		zxfer_create_remote_capability_cache_lock_dir "$remote_lock_dir"
	)
	output=$(cat "$call_log")

	assertContains "ssh control socket lock creation should delegate to the shared lock helper with the lock purpose." \
		"$output" "path=$lock_dir kind=lock purpose=ssh-control-socket-lock"
	assertContains "Remote capability lock creation should delegate to the shared lock helper with the lock purpose." \
		"$output" "path=$remote_lock_dir kind=lock purpose=remote-capability-cache-lock"
}

test_zxfer_create_ssh_control_socket_lease_file_returns_failure_when_shared_creation_fails() {
	entry_dir="$TEST_TMPDIR/ssh_lease_create_fail"
	mkdir -p "$entry_dir/leases"

	output=$(
		(
			set +e
			g_zxfer_runtime_artifact_path_result="$TEST_TMPDIR/stale-lease-result"
			zxfer_create_owned_lock_dir_in_parent() {
				return 1
			}
			zxfer_create_ssh_control_socket_lease_file "$entry_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'result=%s\n' "${g_zxfer_runtime_artifact_path_result:-}"
		)
	)

	assertContains "ssh control socket lease creation should fail closed when shared owned-lock creation fails." \
		"$output" "status=1"
	assertContains "ssh control socket lease creation failures should clear any stale runtime-artifact result path." \
		"$output" "result="
}

test_zxfer_validate_remote_capability_cache_lock_dir_rejects_wrong_metadata_purpose() {
	lock_dir="$TEST_TMPDIR/remote_caps_wrong_purpose"
	write_owned_lock_metadata_fixture "$lock_dir" lock "ssh-control-socket-lock"

	set +e
	zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	status=$?

	assertEquals "Remote capability lock validation should reject owned locks written for another purpose." \
		2 "$status"
}

test_zxfer_validate_remote_capability_cache_lock_dir_accepts_valid_metadata() {
	lock_dir="$TEST_TMPDIR/remote_caps_valid"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"

	set +e
	zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	status=$?

	assertEquals "Remote capability lock validation should accept owned locks written with the remote capability cache purpose." \
		0 "$status"
}

test_zxfer_try_acquire_remote_capability_cache_lock_maps_shared_reap_statuses() {
	lock_dir="$TEST_TMPDIR/remote_caps_map.lock"

	output=$(
		(
			set +e
			g_test_reap_status=0
			g_test_create_calls=0
			zxfer_remote_capability_cache_lock_path() {
				printf '%s\n' "$lock_dir"
			}
			zxfer_create_remote_capability_cache_lock_dir() {
				g_test_create_calls=$((g_test_create_calls + 1))
				if [ "$g_test_create_calls" -eq 1 ]; then
					return 1
				fi
				return 0
			}
			zxfer_validate_remote_capability_cache_lock_dir() {
				return 0
			}
			zxfer_try_reap_stale_owned_lock_dir() {
				return "$g_test_reap_status"
			}

			write_owned_lock_metadata_fixture \
				"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" "999999999"
			result0=$(zxfer_try_acquire_remote_capability_cache_lock "origin.example")
			printf 'status0=%s\n' "$?"
			printf 'result0=%s\n' "$result0"

			rm -rf "$lock_dir"
			write_owned_lock_metadata_fixture \
				"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" "999999999"
			g_test_reap_status=2
			g_test_create_calls=0
			zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
			printf 'status2=%s\n' "$?"

			rm -rf "$lock_dir"
			write_owned_lock_metadata_fixture \
				"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" "999999999"
			g_test_reap_status=1
			g_test_create_calls=0
			zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
			printf 'status1=%s\n' "$?"
		)
	)

	assertContains "Remote capability lock acquisition should retry creation after stale-lock reaping succeeds." \
		"$output" "status0=0"
	assertContains "Remote capability lock acquisition should emit the lock path after stale-lock reaping succeeds." \
		"$output" "result0=$lock_dir"
	assertContains "Remote capability lock acquisition should report busy when the shared reaper reports a live owner." \
		"$output" "status2=2"
	assertContains "Remote capability lock acquisition should fail closed when the shared reaper reports an unrecoverable error." \
		"$output" "status1=1"
}

test_zxfer_release_remote_capability_cache_lock_delegates_to_shared_release_helper() {
	lock_dir="$TEST_TMPDIR/remote_caps_release_delegate.lock"
	call_log="$TEST_TMPDIR/remote_caps_release_delegate.log"

	(
		zxfer_release_owned_lock_dir() {
			printf 'path=%s kind=%s purpose=%s\n' "$1" "$2" "$3" >>"$call_log"
			return 0
		}
		zxfer_release_remote_capability_cache_lock "$lock_dir"
	)
	output=$(cat "$call_log")

	assertContains "Remote capability lock release should delegate to the shared release helper with the lock purpose." \
		"$output" "path=$lock_dir kind=lock purpose=remote-capability-cache-lock"
}

test_zxfer_release_remote_capability_cache_lock_with_precedence_warns_and_preserves_primary_status() {
	output=$(
		(
			set +e
			zxfer_release_remote_capability_cache_lock() {
				return 19
			}
			zxfer_release_remote_capability_cache_lock_with_precedence \
				"origin.example" "$TEST_TMPDIR/remote_caps_release.lock" 73
			printf 'status=%s\n' "$?"
			zxfer_release_remote_capability_cache_lock_with_precedence \
				"origin.example" "$TEST_TMPDIR/remote_caps_release.lock" 0
			printf 'status2=%s\n' "$?"
		) 2>&1
	)

	assertContains "Remote capability lock-release precedence should preserve the primary failure status." \
		"$output" "status=73"
	assertContains "Remote capability lock-release precedence should fail closed when release fails without a primary failure." \
		"$output" "status2=1"
	assertContains "Remote capability lock-release precedence should emit the release warning with the shared status." \
		"$output" "Warning: Failed to release local remote capability cache lock for host origin.example (status 19)."
}

test_zxfer_remote_capability_cache_write_unavailable_helpers_track_origin_and_target_state() {
	output=$(
		(
			set +e
			zxfer_render_remote_capability_cache_identity_for_host() {
				if [ "$1" = "broken.example" ]; then
					return 1
				fi
				printf '%s\n' "$1|${2:-zfs}"
			}

			g_origin_remote_capabilities_host="origin.example"
			g_origin_remote_capabilities_cache_identity="origin.example|zfs"
			g_origin_remote_capabilities_cache_write_unavailable=0
			zxfer_remote_capability_cache_write_is_unavailable_for_host "origin.example"
			printf 'origin_before=%s\n' "$?"
			zxfer_note_remote_capability_cache_write_unavailable_for_host "origin.example"
			printf 'origin_flag=%s\n' "${g_origin_remote_capabilities_cache_write_unavailable:-0}"
			zxfer_remote_capability_cache_write_is_unavailable_for_host "origin.example"
			printf 'origin_after=%s\n' "$?"

			g_target_remote_capabilities_host="target.example"
			g_target_remote_capabilities_cache_identity="target.example|parallel cat"
			g_target_remote_capabilities_cache_write_unavailable=0
			zxfer_remote_capability_cache_write_is_unavailable_for_host "target.example" "parallel cat"
			printf 'target_before=%s\n' "$?"
			zxfer_note_remote_capability_cache_write_unavailable_for_host "target.example" "parallel cat"
			printf 'target_flag=%s\n' "${g_target_remote_capabilities_cache_write_unavailable:-0}"
			zxfer_remote_capability_cache_write_is_unavailable_for_host "target.example" "parallel cat"
			printf 'target_after=%s\n' "$?"

			zxfer_note_remote_capability_cache_write_unavailable_for_host "broken.example"
			printf 'broken_note=%s\n' "$?"
			zxfer_remote_capability_cache_write_is_unavailable_for_host "broken.example"
			printf 'broken_check=%s\n' "$?"
		)
	)

	assertContains "Remote capability cache-write tracking should report origin caches available before they are marked unavailable." \
		"$output" "origin_before=1"
	assertContains "Remote capability cache-write tracking should mark origin caches unavailable after a checked write failure." \
		"$output" "origin_flag=1"
	assertContains "Remote capability cache-write tracking should report origin caches unavailable after they are marked." \
		"$output" "origin_after=0"
	assertContains "Remote capability cache-write tracking should report target caches available before they are marked unavailable." \
		"$output" "target_before=1"
	assertContains "Remote capability cache-write tracking should mark target caches unavailable after a checked write failure." \
		"$output" "target_flag=1"
	assertContains "Remote capability cache-write tracking should report target caches unavailable after they are marked." \
		"$output" "target_after=0"
	assertContains "Remote capability cache-write tracking should treat identity-render failures as no-op success when marking unavailability." \
		"$output" "broken_note=0"
	assertContains "Remote capability cache-write tracking should fail closed when identity rendering fails during availability checks." \
		"$output" "broken_check=1"
}

test_zxfer_remote_capability_cache_helper_failures_cover_current_shell_direct() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	cache_key_output=$(
		(
			set +e
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "identity render failed"
				return 1
			}
			zxfer_remote_capability_cache_key "origin.example"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	path_uid_status=$(
		(
			set +e
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR" >/dev/null 2>&1
			printf '%s\n' "$?"
		)
	)

	path_prefix_status=$(
		(
			set +e
			zxfer_get_remote_host_cache_root_prefix() {
				return 1
			}
			zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR" >/dev/null 2>&1
			printf '%s\n' "$?"
		)
	)

	ensure_path_status=$(
		(
			set +e
			zxfer_remote_capability_cache_dir_path_for_tmpdir() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir >/dev/null 2>&1
			printf '%s\n' "$?"
		)
	)

	rm -rf "$cache_dir"
	mkdir -p "$cache_dir" || fail "Unable to create the remote capability cache-dir fixture."
	chmod 700 "$cache_dir"

	existing_owner_status=$(
		(
			set +e
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir >/dev/null 2>&1
			printf '%s\n' "$?"
		)
	)

	lock_path_status=$(
		(
			set +e
			zxfer_remote_capability_cache_path() {
				return 1
			}
			zxfer_remote_capability_cache_lock_path "origin.example" >/dev/null 2>&1
			printf '%s\n' "$?"
		)
	)

	lock_lookup_status=$(
		(
			set +e
			zxfer_remote_capability_cache_lock_path() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null 2>&1
			printf '%s\n' "$?"
		)
	)

	assertEquals "Remote capability cache-key derivation should fail closed when the cache identity renderer fails in the current shell." \
		"status=1" "$(printf '%s\n' "$cache_key_output" | tail -n 1)"
	assertContains "Remote capability cache-key derivation should surface non-empty renderer failure output in the current shell." \
		"$cache_key_output" "identity render failed"
	assertEquals "Remote capability cache-dir path derivation should fail closed when the effective uid lookup fails in the current shell." \
		1 "$path_uid_status"
	assertEquals "Remote capability cache-dir path derivation should no longer depend on the run-unique remote-host cache root prefix." \
		0 "$path_prefix_status"
	assertEquals "Remote capability cache-dir setup should fail closed when cache-dir path derivation fails in the current shell." \
		1 "$ensure_path_status"
	assertEquals "Remote capability cache-dir setup should fail closed when an existing cache-dir owner lookup fails in the current shell." \
		1 "$existing_owner_status"
	assertEquals "Remote capability cache-lock path derivation should fail closed when the cache-path lookup fails in the current shell." \
		1 "$lock_path_status"
	assertEquals "Remote capability cache-lock acquisition should fail closed when the lock-path lookup fails in the current shell." \
		1 "$lock_lookup_status"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_preserves_live_owned_entry() {
	lock_dir="$TEST_TMPDIR/remote_caps_live_reap"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"

	output=$(
		(
			set +e
			zxfer_remote_capability_cache_lock_path() {
				printf '%s\n' "$lock_dir"
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Pidless remote capability stale-lock reaping should leave live metadata-backed owners in place so the caller can fall back cleanly." \
		"$output" "status=0"
	assertTrue "Pidless remote capability stale-lock reaping should preserve live metadata-backed lock directories." \
		"[ -d '$lock_dir' ]"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_rejects_unsupported_pid_file_layout() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	err_file="$TEST_TMPDIR/remote_capability_lock_pidless_unsupported.err"

	mkdir -p "$lock_dir" || fail "Unable to create unsupported remote capability pid-lock fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod unsupported remote capability pid-lock fixture directory."
	printf '%s\n' "$$" >"$lock_dir/pid" ||
		fail "Unable to write unsupported remote capability pid-lock fixture pid file."
	chmod 600 "$lock_dir/pid" ||
		fail "Unable to chmod unsupported remote capability pid-lock fixture pid file."

	set +e
	zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example" \
		2>"$err_file"
	status=$?

	assertEquals "Pidless capability-cache cleanup should fail closed for unsupported pid-file lock directories." \
		1 "$status"
	assertContains "Pidless capability-cache cleanup should emit the unsupported-layout diagnostic for pid-file lock directories." \
		"$(cat "$err_file")" \
		"Error: remote capability cache lock path \"$lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
}

test_zxfer_cleanup_empty_remote_host_cache_root_handles_empty_and_nonempty_dirs() {
	empty_dir="$TEST_TMPDIR/remote_cache_empty"
	nonempty_dir="$TEST_TMPDIR/remote_cache_nonempty"
	mkdir "$empty_dir" "$nonempty_dir"
	: >"$nonempty_dir/cache"

	output=$(
		(
			set +e
			zxfer_cleanup_empty_remote_host_cache_root ""
			printf 'blank=%s\n' "$?"
			zxfer_cleanup_empty_remote_host_cache_root "$TEST_TMPDIR/missing-remote-cache"
			printf 'missing=%s\n' "$?"
			zxfer_cleanup_empty_remote_host_cache_root "$nonempty_dir"
			printf 'nonempty=%s\n' "$?"
			zxfer_cleanup_empty_remote_host_cache_root "$empty_dir"
			printf 'empty=%s\n' "$?"
			printf 'empty_exists=%s\n' "$([ -e "$empty_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Empty-cache cleanup should treat a blank path as a no-op success." \
		"$output" "blank=0"
	assertContains "Empty-cache cleanup should treat a missing path as a no-op success." \
		"$output" "missing=0"
	assertContains "Empty-cache cleanup should leave non-empty cache roots in place." \
		"$output" "nonempty=0"
	assertContains "Empty-cache cleanup should remove empty cache roots." \
		"$output" "empty=0"
	assertContains "Empty-cache cleanup should remove the emptied directory." \
		"$output" "empty_exists=no"
}

test_zxfer_remote_host_cache_root_helpers_cover_current_prefix_and_supported_entries() {
	current_prefix="zxfer.remote-cache-helper"
	current_socket_root="$TEST_TMPDIR/$current_prefix.s.501.d"
	current_cap_root="$TEST_TMPDIR/$current_prefix.remote-capabilities.501.d"
	fallback_socket_root="$TEST_TMPDIR/zxfer.fallback.s.501.d"
	fallback_cap_root="$TEST_TMPDIR/zxfer.fallback.remote-capabilities.501.d"
	non_root_path="$TEST_TMPDIR/not-zxfer.s.501.d"
	supported_root="$TEST_TMPDIR/remote_cache_supported"

	mkdir -p "$supported_root/entry/leases/lease.current" ||
		fail "Unable to create supported remote-host cache helper fixture."
	: >"$supported_root/cache.lock" ||
		fail "Unable to create supported remote-host cache helper lock fixture."

	output=$(
		(
			g_zxfer_temp_prefix="$current_prefix"
			if zxfer_is_remote_host_cache_root_path "$current_socket_root"; then
				printf 'current_socket=0\n'
			else
				printf 'current_socket=1\n'
			fi
			if zxfer_is_remote_host_cache_root_path "$current_cap_root"; then
				printf 'current_cap=0\n'
			else
				printf 'current_cap=1\n'
			fi
			unset g_zxfer_temp_prefix
			if zxfer_is_remote_host_cache_root_path "$fallback_socket_root"; then
				printf 'fallback_socket=0\n'
			else
				printf 'fallback_socket=1\n'
			fi
			if zxfer_is_remote_host_cache_root_path "$fallback_cap_root"; then
				printf 'fallback_cap=0\n'
			else
				printf 'fallback_cap=1\n'
			fi
			if zxfer_is_remote_host_cache_root_path "$non_root_path"; then
				printf 'non_root=0\n'
			else
				printf 'non_root=1\n'
			fi
			set +e
			zxfer_remote_host_cache_root_contains_unsupported_entries "$supported_root"
			printf 'supported_status=%s\n' "$?"
		)
	)

	assertContains "Remote-host cache root detection should match current-prefix ssh cache roots." \
		"$output" "current_socket=0"
	assertContains "Remote-host cache root detection should match current-prefix capability cache roots." \
		"$output" "current_cap=0"
	assertContains "Remote-host cache root detection should match fallback zxfer ssh cache roots when current temp-prefix state is unavailable." \
		"$output" "fallback_socket=0"
	assertContains "Remote-host cache root detection should match fallback zxfer capability cache roots when current temp-prefix state is unavailable." \
		"$output" "fallback_cap=0"
	assertContains "Remote-host cache root detection should reject non-zxfer-like scratch paths." \
		"$output" "non_root=1"
	assertContains "Unsupported-entry detection should ignore regular *.lock files and directory lease entries that use the current format." \
		"$output" "supported_status=1"
}

test_zxfer_cleanup_remote_host_cache_root_removes_symlink_paths() {
	target_dir="$TEST_TMPDIR/remote_cache_target"
	link_path="$TEST_TMPDIR/remote_cache_link"
	mkdir "$target_dir" || fail "Unable to create remote-host cache symlink target."
	ln -s "$target_dir" "$link_path" || fail "Unable to create remote-host cache symlink fixture."

	output=$(
		(
			set +e
			zxfer_cleanup_remote_host_cache_root "$link_path"
			printf 'status=%s\n' "$?"
			printf 'link_exists=%s\n' "$([ -L "$link_path" ] && printf yes || printf no)"
			printf 'target_exists=%s\n' "$([ -d "$target_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Remote-host cache cleanup should treat symlink cleanup as a success path." \
		"$output" "status=0"
	assertContains "Remote-host cache cleanup should remove symlinked cache-root paths instead of preserving them." \
		"$output" "link_exists=no"
	assertContains "Remote-host cache cleanup should leave the symlink target untouched." \
		"$output" "target_exists=yes"
}

test_zxfer_cleanup_remote_host_cache_root_preserves_roots_with_unsupported_entries() {
	g_zxfer_temp_prefix="zxfer.remote-cache-preserve"
	ssh_root=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	cap_root=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	mkdir -p "$ssh_root/entry/leases" "$cap_root/cache.lock" ||
		fail "Unable to create remote-host cache preservation fixtures."
	chmod 700 "$cap_root/cache.lock" ||
		fail "Unable to chmod unsupported capability lock fixture directory."
	printf '%s\n' "$$" >"$cap_root/cache.lock/pid" ||
		fail "Unable to write unsupported capability lock fixture pid file."
	chmod 600 "$cap_root/cache.lock/pid" ||
		fail "Unable to chmod unsupported capability lock fixture pid file."
	: >"$ssh_root/entry/leases/lease.legacy"

	output=$(
		(
			set +e
			zxfer_cleanup_remote_host_cache_root "$cap_root"
			printf 'cap_status=%s\n' "$?"
			printf 'cap_exists=%s\n' "$([ -d "$cap_root" ] && printf yes || printf no)"
			zxfer_cleanup_remote_host_cache_root "$ssh_root"
			printf 'ssh_status=%s\n' "$?"
			printf 'ssh_exists=%s\n' "$([ -d "$ssh_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Remote-host cache cleanup should preserve roots that contain unsupported pid-file lock layouts." \
		"$output" "cap_status=0"
	assertContains "Remote-host cache cleanup should keep roots that contain unsupported pid-file lock layouts in place." \
		"$output" "cap_exists=yes"
	assertContains "Remote-host cache cleanup should preserve roots that contain unsupported plain-file lease entries." \
		"$output" "ssh_status=0"
	assertContains "Remote-host cache cleanup should keep roots that contain unsupported plain-file lease entries in place." \
		"$output" "ssh_exists=yes"
}

test_zxfer_cleanup_remote_host_cache_root_preserves_roots_with_unsupported_entries_without_current_temp_prefix_state() {
	g_zxfer_temp_prefix="zxfer.remote-cache-preserve-fallback"
	ssh_root=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	cap_root=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	mkdir -p "$ssh_root/entry/leases" "$cap_root/cache.lock" ||
		fail "Unable to create remote-host fallback preservation fixtures."
	chmod 700 "$cap_root/cache.lock" ||
		fail "Unable to chmod fallback capability lock fixture directory."
	printf '%s\n' "$$" >"$cap_root/cache.lock/pid" ||
		fail "Unable to write fallback capability lock fixture pid file."
	chmod 600 "$cap_root/cache.lock/pid" ||
		fail "Unable to chmod fallback capability lock fixture pid file."
	: >"$ssh_root/entry/leases/lease.legacy"

	output=$(
		(
			set +e
			unset g_zxfer_temp_prefix
			zxfer_cleanup_remote_host_cache_root "$cap_root"
			printf 'cap_status=%s\n' "$?"
			printf 'cap_exists=%s\n' "$([ -d "$cap_root" ] && printf yes || printf no)"
			zxfer_cleanup_remote_host_cache_root "$ssh_root"
			printf 'ssh_status=%s\n' "$?"
			printf 'ssh_exists=%s\n' "$([ -d "$ssh_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Remote-host cache cleanup should still preserve unsupported pid-file lock layouts when the current temp-prefix state is unavailable." \
		"$output" "cap_status=0"
	assertContains "Remote-host cache cleanup should keep unsupported pid-file lock layouts in place when the current temp-prefix state is unavailable." \
		"$output" "cap_exists=yes"
	assertContains "Remote-host cache cleanup should still preserve unsupported plain-file lease entries when the current temp-prefix state is unavailable." \
		"$output" "ssh_status=0"
	assertContains "Remote-host cache cleanup should keep unsupported plain-file lease entries in place when the current temp-prefix state is unavailable." \
		"$output" "ssh_exists=yes"
}

test_zxfer_cleanup_remote_host_cache_root_preserves_roots_with_unsupported_entries_when_temp_prefix_state_drifts() {
	g_zxfer_temp_prefix="zxfer.remote-cache-preserve-stale"
	ssh_root=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	cap_root=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	mkdir -p "$ssh_root/entry/leases" "$cap_root/cache.lock" ||
		fail "Unable to create remote-host stale-prefix preservation fixtures."
	chmod 700 "$cap_root/cache.lock" ||
		fail "Unable to chmod stale-prefix capability lock fixture directory."
	printf '%s\n' "$$" >"$cap_root/cache.lock/pid" ||
		fail "Unable to write stale-prefix capability lock fixture pid file."
	chmod 600 "$cap_root/cache.lock/pid" ||
		fail "Unable to chmod stale-prefix capability lock fixture pid file."
	: >"$ssh_root/entry/leases/lease.legacy"

	output=$(
		(
			set +e
			g_zxfer_temp_prefix="zxfer.remote-cache-preserve-other"
			zxfer_cleanup_remote_host_cache_root "$cap_root"
			printf 'cap_status=%s\n' "$?"
			printf 'cap_exists=%s\n' "$([ -d "$cap_root" ] && printf yes || printf no)"
			zxfer_cleanup_remote_host_cache_root "$ssh_root"
			printf 'ssh_status=%s\n' "$?"
			printf 'ssh_exists=%s\n' "$([ -d "$ssh_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Remote-host cache cleanup should still preserve unsupported pid-file lock layouts when temp-prefix state drifts before cleanup." \
		"$output" "cap_status=0"
	assertContains "Remote-host cache cleanup should keep unsupported pid-file lock layouts in place when temp-prefix state drifts before cleanup." \
		"$output" "cap_exists=yes"
	assertContains "Remote-host cache cleanup should still preserve unsupported plain-file lease entries when temp-prefix state drifts before cleanup." \
		"$output" "ssh_status=0"
	assertContains "Remote-host cache cleanup should keep unsupported plain-file lease entries in place when temp-prefix state drifts before cleanup." \
		"$output" "ssh_exists=yes"
}

test_zxfer_cleanup_remote_host_cache_root_removes_non_root_dirs_with_legacy_like_entries() {
	fake_root="$TEST_TMPDIR/not-a-remote-cache-root"

	mkdir -p "$fake_root/entry/leases" "$fake_root/cache.lock" ||
		fail "Unable to create non-root remote-host cleanup fixtures."
	chmod 700 "$fake_root/cache.lock" ||
		fail "Unable to chmod non-root legacy-like lock fixture directory."
	printf '%s\n' "$$" >"$fake_root/cache.lock/pid" ||
		fail "Unable to write non-root legacy-like lock fixture pid file."
	chmod 600 "$fake_root/cache.lock/pid" ||
		fail "Unable to chmod non-root legacy-like lock fixture pid file."
	: >"$fake_root/entry/leases/lease.legacy"

	output=$(
		(
			set +e
			zxfer_cleanup_remote_host_cache_root "$fake_root"
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$fake_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Remote-host cache cleanup should still remove directories that are not actual remote-host cache roots even when they contain legacy-like names." \
		"$output" "status=0"
	assertContains "Remote-host cache cleanup should not preserve arbitrary directories with legacy-like child names." \
		"$output" "exists=no"
}

test_zxfer_cleanup_remote_host_cache_root_removes_non_zxfer_root_shaped_dirs_with_unsupported_entries() {
	fake_root="$TEST_TMPDIR/not-zxfer.s.123.d"

	mkdir -p "$fake_root/entry/leases" "$fake_root/cache.lock" ||
		fail "Unable to create non-zxfer root-shaped cleanup fixtures."
	chmod 700 "$fake_root/cache.lock" ||
		fail "Unable to chmod non-zxfer root-shaped lock fixture directory."
	printf '%s\n' "$$" >"$fake_root/cache.lock/pid" ||
		fail "Unable to write non-zxfer root-shaped lock fixture pid file."
	chmod 600 "$fake_root/cache.lock/pid" ||
		fail "Unable to chmod non-zxfer root-shaped lock fixture pid file."
	: >"$fake_root/entry/leases/lease.legacy"

	output=$(
		(
			set +e
			unset g_zxfer_temp_prefix
			zxfer_cleanup_remote_host_cache_root "$fake_root"
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$fake_root" ] && printf yes || printf no)"
		)
	)

	assertContains "Remote-host cache cleanup should not preserve arbitrary root-shaped directories that do not use the current zxfer naming prefix." \
		"$output" "status=0"
	assertContains "Remote-host cache cleanup should remove arbitrary root-shaped directories that do not use the current zxfer naming prefix." \
		"$output" "exists=no"
}

test_zxfer_setup_ssh_control_socket_fails_closed_when_lease_prune_fails() {
	release_log="$TEST_TMPDIR/setup_ssh_prune_fail.release"

	set +e
	output=$(
		(
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/setup-prune.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
				return 0
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				zxfer_note_ssh_control_socket_lock_error \
					"Unable to inspect ssh control socket lease entry \"$entry_dir/leases/lease.bad\"."
				return 73
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		) 2>&1
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when sibling lease inspection fails." \
		1 "$status"
	assertContains "ssh control socket setup should surface the failing sibling lease path when lease pruning fails." \
		"$output" "Error pruning ssh control socket lease entries for origin host: Unable to inspect ssh control socket lease entry"
	assertContains "ssh control socket setup should keep the top-level setup failure message." \
		"$output" "Error creating ssh control socket for origin host."
	assertEquals "ssh control socket setup should still release the per-entry lock after lease pruning fails." \
		"$TEST_TMPDIR/setup-prune.lock" "$(cat "$release_log")"
}

test_zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure_warns_when_cache_dir_cleanup_fails() {
	set +e
	output=$(
		(
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="closed"
				g_zxfer_ssh_control_socket_action_command="ssh -S $2 -O $3 $1"
				return 0
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 1
			}
			zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure \
				"origin.example" origin "$TEST_TMPDIR/cleanup-fail.socket" \
				"$TEST_TMPDIR/cleanup-fail.entry"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertContains "Fresh-master setup cleanup should warn when cache-dir removal fails." \
		"$output" "Warning: Failed to remove ssh control socket cache directory for origin host after lease creation failure."
	assertContains "Fresh-master setup cleanup should return failure when cache-dir removal fails." \
		"$output" "status=1"
}

test_zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure_ignores_stale_socket_and_cleans_cache_dir() {
	cleanup_log="$TEST_TMPDIR/cleanup_stale_socket.log"

	output=$(
		(
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				g_zxfer_ssh_control_socket_action_command="ssh -S $2 -O $3 $1"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				printf '%s\n' "$1" >"$cleanup_log"
				return 0
			}
			zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure \
				"origin.example" origin "$TEST_TMPDIR/stale.socket" \
				"$TEST_TMPDIR/stale.entry"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertContains "Fresh-master cleanup should treat stale sockets as already closed." \
		"$output" "status=0"
	assertEquals "Fresh-master cleanup should still remove the cache directory after stale-socket detection." \
		"$TEST_TMPDIR/stale.entry" "$(cat "$cleanup_log")"
}

test_zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure_warns_when_transport_cleanup_fails() {
	output=$(
		(
			g_zxfer_ssh_control_socket_action_stderr=""
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_command="ssh -S $2 -O $3 $1"
				return 1
			}
			zxfer_try_cleanup_opened_ssh_control_socket_after_lease_failure \
				"origin.example" origin "$TEST_TMPDIR/action-fail.socket" \
				"$TEST_TMPDIR/action-fail.entry"
			printf 'status=%s\n' "$?"
		) 2>&1
	)

	assertContains "Fresh-master cleanup should warn when the control-socket transport cannot be closed." \
		"$output" "Warning: Failed to close ssh control socket for origin host after lease creation failure."
	assertContains "Fresh-master cleanup should return failure when transport cleanup fails for a non-stale socket." \
		"$output" "status=1"
}

test_close_origin_ssh_control_socket_reclaims_stale_shared_socket_and_releases_lock_in_current_shell() {
	release_log="$TEST_TMPDIR/close_origin_stale.release"
	cleanup_log="$TEST_TMPDIR/close_origin_stale.cleanup"

	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/close_origin_stale.socket"
	g_ssh_origin_control_socket_dir="$TEST_TMPDIR/close_origin_stale.entry"
	g_ssh_origin_control_socket_lease_file="$TEST_TMPDIR/close_origin_stale.entry/leases/lease.1"
	mkdir -p "$g_ssh_origin_control_socket_dir/leases" ||
		fail "Unable to create the stale shared-socket fixture directory."
	zxfer_reset_ssh_control_socket_action_state

	zxfer_acquire_ssh_control_socket_lock() {
		g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/close_origin_stale.lock"
		return 0
	}
	zxfer_release_ssh_control_socket_lease_file() {
		return 0
	}
	zxfer_prune_stale_ssh_control_socket_leases() {
		return 0
	}
	zxfer_count_ssh_control_socket_leases() {
		g_zxfer_ssh_control_socket_lease_count_result=0
		return 0
	}
	zxfer_check_ssh_control_socket_for_host() {
		g_zxfer_ssh_control_socket_action_result="stale"
		g_zxfer_ssh_control_socket_action_command="ssh -S $2 -O check $1"
		return 1
	}
	zxfer_cleanup_ssh_control_socket_entry_dir() {
		printf '%s\n' "$1" >"$cleanup_log"
		return 0
	}
	zxfer_release_ssh_control_socket_lock() {
		printf '%s\n' "$1" >"$release_log"
		return 0
	}

	zxfer_close_origin_ssh_control_socket
	status=$?

	assertEquals "Current-shell origin ssh control socket close should treat a stale shared socket as successful cleanup." \
		0 "$status"
	assertEquals "Current-shell stale shared-socket cleanup should remove the cache directory." \
		"$TEST_TMPDIR/close_origin_stale.entry" "$(cat "$cleanup_log")"
	assertEquals "Current-shell stale shared-socket cleanup should release the per-entry lock after cleanup." \
		"$TEST_TMPDIR/close_origin_stale.lock" "$(cat "$release_log")"
	assertEquals "Current-shell stale shared-socket cleanup should clear the remembered origin socket path." \
		"" "${g_ssh_origin_control_socket:-}"
	assertEquals "Current-shell stale shared-socket cleanup should clear the remembered origin socket directory." \
		"" "${g_ssh_origin_control_socket_dir:-}"
	assertEquals "Current-shell stale shared-socket cleanup should clear the remembered origin lease file." \
		"" "${g_ssh_origin_control_socket_lease_file:-}"
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

test_zxfer_remote_host_supervisor_related_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_supervisor_branch_coverage"
	mkdir -p "$branch_root"
	caps_payload=$(printf '%s\n' \
		"ZXFER_REMOTE_CAPS_V2" \
		"os	Linux" \
		"tool	zfs	0	/sbin/zfs")

	output=$(
		(
			set +e
			g_cmd_ssh=/usr/bin/ssh
			zxfer_render_ssh_transport_policy_identity() {
				printf '%s\n' "policy"
			}
			cksum() {
				return 1
			}
			zxfer_ssh_control_socket_cache_key "user@example"
			printf 'ssh_key_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_ssh_control_socket_cache_dir_path_for_tmpdir() {
				printf '%s\n' "$branch_root/socket-cache"
			}
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
			printf 'socket_uid_status=%s\n' "$?"
		)
		(
			set +e
			cache_dir="$branch_root/socket-cache-existing-owner"
			mkdir -p "$cache_dir"
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_ssh_control_socket_cache_dir_path_for_tmpdir() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
			printf 'socket_owner_status=%s\n' "$?"
		)
		(
			set +e
			cache_dir="$branch_root/socket-cache-new-owner"
			rm -rf "$cache_dir"
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_ssh_control_socket_cache_dir_path_for_tmpdir() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
			printf 'socket_new_owner_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-existing-owner"
			mkdir -p "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_owner_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-new-owner"
			rm -rf "$entry_cache"
			mkdir -p "$entry_cache"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_new_owner_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-lease-owner"
			mkdir -p "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_owner_uid() {
				if [ "${1##*/}" = "leases" ]; then
					return 1
				fi
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_owner_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-identity-write"
			rm -rf "$entry_cache"
			mkdir -p "$entry_cache"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_write_ssh_control_socket_entry_identity_file() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'identity_write_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_render_ssh_transport_policy_identity() {
				printf '%s\n' policy
			}
			zxfer_resolve_remote_capability_requested_tools_for_host() {
				g_zxfer_remote_capability_requested_tools_result=zfs
				return 0
			}
			cksum() {
				return 1
			}
			zxfer_remote_capability_cache_key "user@example" zfs >/dev/null
			printf 'remote_key_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_remote_capability_cache_dir_path_for_tmpdir() {
				printf '%s\n' "$branch_root/remote-cap-cache"
			}
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir >/dev/null
			printf 'remote_cache_uid_status=%s\n' "$?"
		)
		(
			set +e
			cache_dir="$branch_root/remote-cap-cache-existing-owner"
			mkdir -p "$cache_dir"
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$branch_root"
			}
			zxfer_remote_capability_cache_dir_path_for_tmpdir() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir >/dev/null
			printf 'remote_cache_owner_status=%s\n' "$?"
		)
		(
			set +e
			live_response=$caps_payload
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				printf '%s\n' "$branch_root/live.lock"
				return 0
			}
			zxfer_fetch_remote_host_capabilities_live() {
				g_zxfer_remote_capability_response_result=$live_response
				return 0
			}
			zxfer_store_cached_remote_capability_response_for_host() {
				printf 'store:%s\n' "$1"
			}
			zxfer_note_remote_capability_bootstrap_source_for_host() {
				printf 'source:%s\n' "$2"
			}
			zxfer_profile_record_remote_capability_bootstrap_source() {
				:
			}
			zxfer_remote_capability_cache_write_is_unavailable_for_host() {
				return 1
			}
			zxfer_write_remote_capability_cache_file() {
				return 27
			}
			zxfer_note_remote_capability_cache_write_unavailable_for_host() {
				printf 'write_unavailable:%s\n' "$1"
			}
			zxfer_warn_remote_capability_cache_write_failure() {
				printf 'write_warning:%s:%s\n' "$1" "$2"
			}
			zxfer_release_remote_capability_cache_lock_with_precedence() {
				printf 'release:%s:%s:%s\n' "$1" "$2" "$3"
				return 0
			}
			zxfer_ensure_remote_host_capabilities "user@example" source zfs >/dev/null
			printf 'live_status=%s\n' "$?"
		)
		(
			set +e
			wait_response=$caps_payload
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				return 2
			}
			zxfer_wait_for_remote_capability_cache_fill() {
				printf '%s\n' "$wait_response"
			}
			zxfer_store_cached_remote_capability_response_for_host() {
				printf 'wait_store:%s\n' "$1"
			}
			zxfer_note_remote_capability_bootstrap_source_for_host() {
				printf 'wait_source:%s\n' "$2"
			}
			zxfer_profile_record_remote_capability_bootstrap_source() {
				:
			}
			zxfer_ensure_remote_host_capabilities "user@example" source zfs >/dev/null
			printf 'wait_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				return 2
			}
			zxfer_wait_for_remote_capability_cache_fill() {
				return 1
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock() {
				return 1
			}
			zxfer_ensure_remote_host_capabilities "user@example" source zfs >/dev/null
			printf 'reap_failure_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			l_try_count=0
			zxfer_try_acquire_remote_capability_cache_lock() {
				l_try_count=$((l_try_count + 1))
				[ "$l_try_count" -eq 1 ] && return 2
				return 1
			}
			zxfer_wait_for_remote_capability_cache_fill() {
				return 1
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock() {
				return 0
			}
			zxfer_ensure_remote_host_capabilities "user@example" source zfs >/dev/null
			printf 'second_lock_status=%s\n' "$?"
		)
		(
			set +e
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				printf '%s\n' "$branch_root/fail-live.lock"
				return 0
			}
			zxfer_fetch_remote_host_capabilities_live() {
				return 5
			}
			zxfer_release_remote_capability_cache_lock_with_precedence() {
				printf 'release_failure:%s:%s:%s\n' "$1" "$2" "$3"
				return "$3"
			}
			zxfer_ensure_remote_host_capabilities "user@example" source zfs >/dev/null
			printf 'live_failure_status=%s\n' "$?"
		)
	)

	assertContains "SSH cache-key fallback should remain covered." \
		"$output" "ssh_key_status=0"
	assertContains "SSH cache-dir effective uid failures should remain covered." \
		"$output" "socket_uid_status=1"
	assertContains "SSH cache-dir existing owner lookup failures should remain covered." \
		"$output" "socket_owner_status=1"
	assertContains "SSH cache-dir post-create owner lookup failures should remain covered." \
		"$output" "socket_new_owner_status=1"
	assertContains "SSH entry owner lookup failures should remain covered." \
		"$output" "entry_owner_status=1"
	assertContains "SSH entry post-create owner lookup failures should remain covered." \
		"$output" "entry_new_owner_status=1"
	assertContains "SSH lease owner lookup failures should remain covered." \
		"$output" "lease_owner_status=1"
	assertContains "SSH identity write failures should remain covered." \
		"$output" "identity_write_status=1"
	assertContains "Remote capability cache-key derivation should remain covered." \
		"$output" "remote_key_status=0"
	assertContains "Remote capability cache-dir effective uid failures should remain covered." \
		"$output" "remote_cache_uid_status=1"
	assertContains "Remote capability cache-dir existing owner failures should remain covered." \
		"$output" "remote_cache_owner_status=1"
	assertContains "Live remote capability cache-write failure handling should remain covered." \
		"$output" "live_status=0"
	assertContains "Sibling remote capability cache-fill success handling should remain covered." \
		"$output" "wait_status=0"
	assertContains "Stale pidless remote capability cache reap failures should remain covered." \
		"$output" "reap_failure_status=1"
	assertContains "Second remote capability lock acquisition failures should remain covered." \
		"$output" "second_lock_status=1"
	assertContains "Live remote capability probe failures should remain covered." \
		"$output" "live_failure_status=5"
}

test_zxfer_ssh_control_socket_entry_dir_error_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_entry_dir_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			entry_cache="$branch_root/entry-existing-uid"
			mkdir -p "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_existing_uid_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-existing-mode"
			mkdir -p "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_existing_mode_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-mkdir"
			mkdir -p "$entry_cache"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			mkdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_mkdir_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-new-uid"
			mkdir -p "$entry_cache"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_new_uid_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/entry-new-mode"
			mkdir -p "$entry_cache"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'entry_new_mode_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/lease-symlink"
			mkdir -p "$entry_cache/k"
			ln -s missing "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_symlink_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/lease-existing-uid"
			uid_count_file="$branch_root/lease-existing-uid.count"
			: >"$uid_count_file"
			mkdir -p "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				l_uid_calls=$(wc -l <"$uid_count_file" | tr -d '[:space:]')
				printf '%s\n' x >>"$uid_count_file"
				if [ "$l_uid_calls" -eq 0 ]; then
					printf '%s\n' 1000
					return 0
				fi
				return 1
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_existing_uid_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/lease-existing-mode"
			mkdir -p "$entry_cache/k/leases"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				if [ "${1##*/}" = "leases" ]; then
					return 1
				fi
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_existing_mode_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/lease-mkdir"
			mkdir -p "$entry_cache/k"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			mkdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_mkdir_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/lease-new-uid"
			uid_count_file="$branch_root/lease-new-uid.count"
			: >"$uid_count_file"
			mkdir -p "$entry_cache/k"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				l_uid_calls=$(wc -l <"$uid_count_file" | tr -d '[:space:]')
				printf '%s\n' x >>"$uid_count_file"
				if [ "$l_uid_calls" -eq 0 ]; then
					printf '%s\n' 1000
					return 0
				fi
				return 1
			}
			zxfer_get_path_mode_octal() {
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_new_uid_status=%s\n' "$?"
		)
		(
			set +e
			entry_cache="$branch_root/lease-new-mode"
			mkdir -p "$entry_cache/k"
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' k
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$entry_cache"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' identity
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			zxfer_get_path_owner_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_effective_user_uid() {
				printf '%s\n' 1000
			}
			zxfer_get_path_mode_octal() {
				if [ "${1##*/}" = "leases" ]; then
					return 1
				fi
				printf '%s\n' 700
			}
			zxfer_ensure_ssh_control_socket_entry_dir "user@example" >/dev/null
			printf 'lease_new_mode_status=%s\n' "$?"
		)
	)

	assertContains "Existing entry-dir effective uid failures should remain covered." \
		"$output" "entry_existing_uid_status=1"
	assertContains "Existing entry-dir mode lookup failures should remain covered." \
		"$output" "entry_existing_mode_status=1"
	assertContains "Entry-dir creation failures should remain covered." \
		"$output" "entry_mkdir_status=1"
	assertContains "New entry-dir effective uid failures should remain covered." \
		"$output" "entry_new_uid_status=1"
	assertContains "New entry-dir mode lookup failures should remain covered." \
		"$output" "entry_new_mode_status=1"
	assertContains "Lease symlink rejection should remain covered." \
		"$output" "lease_symlink_status=1"
	assertContains "Existing lease-dir effective uid failures should remain covered." \
		"$output" "lease_existing_uid_status=1"
	assertContains "Existing lease-dir mode lookup failures should remain covered." \
		"$output" "lease_existing_mode_status=1"
	assertContains "Lease-dir creation failures should remain covered." \
		"$output" "lease_mkdir_status=1"
	assertContains "New lease-dir effective uid failures should remain covered." \
		"$output" "lease_new_uid_status=1"
	assertContains "New lease-dir mode lookup failures should remain covered." \
		"$output" "lease_new_mode_status=1"
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

test_zxfer_ssh_lock_and_lease_failure_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_lock_lease_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			lock_path="$branch_root/not-a-directory.lock"
			: >"$lock_path"
			zxfer_validate_ssh_control_socket_lock_dir_for_reap "$lock_path" >/dev/null
			printf 'validate_nondir_status=%s\n' "$?"
			printf 'validate_nondir_error=%s\n' "${g_zxfer_ssh_control_socket_lock_error:-}"
			zxfer_try_reap_stale_ssh_control_socket_lock_dir "$lock_path" 1 >/dev/null
			printf 'try_reap_validate_status=%s\n' "$?"
		)
		(
			set +e
			entry_dir="$branch_root/prune-error"
			mkdir -p "$entry_dir/leases/lease.bad"
			zxfer_try_reap_stale_owned_lock_dir() {
				return 1
			}
			zxfer_prune_stale_ssh_control_socket_leases "$entry_dir" >/dev/null
			printf 'prune_error_status=%s\n' "$?"
			printf 'prune_error_message=%s\n' "${g_zxfer_ssh_control_socket_lock_error:-}"
		)
		(
			set +e
			entry_dir="$branch_root/count-reaped"
			mkdir -p "$entry_dir/leases/lease.done"
			zxfer_try_reap_stale_owned_lock_dir() {
				return 0
			}
			count=$(zxfer_count_ssh_control_socket_leases "$entry_dir")
			printf 'count_reaped_status=%s\n' "$?"
			printf 'count_reaped=%s\n' "$count"
		)
		(
			set +e
			entry_dir="$branch_root/count-error"
			mkdir -p "$entry_dir/leases/lease.bad"
			zxfer_try_reap_stale_owned_lock_dir() {
				return 1
			}
			zxfer_count_ssh_control_socket_leases "$entry_dir" >/dev/null
			printf 'count_error_status=%s\n' "$?"
			printf 'count_error_message=%s\n' "${g_zxfer_ssh_control_socket_lock_error:-}"
		)
	)

	assertContains "Lock reap validation should reject non-directory lock paths." \
		"$output" "validate_nondir_status=1"
	assertContains "Lock reap validation should preserve the non-directory diagnostic." \
		"$output" "is not a directory"
	assertContains "Lock reap should propagate validation failures." \
		"$output" "try_reap_validate_status=1"
	assertContains "Lease pruning should fail closed when lease ownership cannot be inspected." \
		"$output" "prune_error_status=1"
	assertContains "Lease pruning should preserve the failing lease path diagnostic." \
		"$output" "Unable to inspect ssh control socket lease entry"
	assertContains "Lease counting should ignore leases reaped as stale." \
		"$output" "count_reaped_status=0"
	assertContains "Lease counting should report zero when all leases are reaped." \
		"$output" "count_reaped=0"
	assertContains "Lease counting should fail closed when lease ownership cannot be inspected." \
		"$output" "count_error_status=1"
	assertContains "Lease counting should preserve the failing lease path diagnostic." \
		"$output" "count_error_message=Unable to inspect ssh control socket lease entry"
}

test_zxfer_ssh_setup_and_close_error_branches_cover_current_shell_paths() {
	branch_root="$TEST_TMPDIR/remote_host_setup_close_branch_coverage"
	mkdir -p "$branch_root"

	output=$(
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/origin-success-cleanup-fail/s"
			g_ssh_origin_control_socket_dir="$branch_root/origin-success-cleanup-fail"
			g_ssh_origin_control_socket_lease_file="$branch_root/origin-success-cleanup-fail/leases/lease.1"
			mkdir -p "$g_ssh_origin_control_socket_dir/leases"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$branch_root/origin-success-cleanup-fail.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lease_file() {
				return 0
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				return 0
			}
			zxfer_count_ssh_control_socket_leases() {
				g_zxfer_ssh_control_socket_lease_count_result=0
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result=live
				g_zxfer_ssh_control_socket_action_command="check $1 $2"
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_command="exit $1 $2"
				return 0
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 5
			}
			zxfer_release_ssh_control_socket_lock_with_precedence() {
				return "$3"
			}
			zxfer_close_origin_ssh_control_socket 2>"$branch_root/origin-success-cleanup-fail.err"
			printf 'origin_success_cleanup_status=%s\n' "$?"
			printf 'origin_success_cleanup_err=%s\n' "$(cat "$branch_root/origin-success-cleanup-fail.err")"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/origin-stale-cleanup-fail/s"
			g_ssh_origin_control_socket_dir="$branch_root/origin-stale-cleanup-fail"
			g_ssh_origin_control_socket_lease_file="$branch_root/origin-stale-cleanup-fail/leases/lease.1"
			mkdir -p "$g_ssh_origin_control_socket_dir/leases"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$branch_root/origin-stale-cleanup-fail.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lease_file() {
				return 0
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				return 0
			}
			zxfer_count_ssh_control_socket_leases() {
				g_zxfer_ssh_control_socket_lease_count_result=0
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result=live
				g_zxfer_ssh_control_socket_action_command="check $1 $2"
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result=stale
				g_zxfer_ssh_control_socket_action_command="exit $1 $2"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 6
			}
			zxfer_release_ssh_control_socket_lock_with_precedence() {
				return "$3"
			}
			zxfer_close_origin_ssh_control_socket 2>"$branch_root/origin-stale-cleanup-fail.err"
			printf 'origin_stale_cleanup_status=%s\n' "$?"
			printf 'origin_stale_cleanup_err=%s\n' "$(cat "$branch_root/origin-stale-cleanup-fail.err")"
		)
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$branch_root/origin-no-lease-stale/s"
			g_ssh_origin_control_socket_dir="$branch_root/origin-no-lease-stale"
			g_ssh_origin_control_socket_lease_file=""
			mkdir -p "$g_ssh_origin_control_socket_dir"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result=stale
				g_zxfer_ssh_control_socket_action_command="exit $1 $2"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 7
			}
			zxfer_close_origin_ssh_control_socket 2>"$branch_root/origin-no-lease-stale.err"
			printf 'origin_no_lease_stale_status=%s\n' "$?"
			printf 'origin_no_lease_stale_err=%s\n' "$(cat "$branch_root/origin-no-lease-stale.err")"
		)
		(
			set +e
			control_dir="$branch_root/setup-check-error"
			release_log="$branch_root/setup-check-error.release"
			mkdir -p "$control_dir"
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$control_dir"
			}
			zxfer_get_ssh_base_transport_tokens() {
				printf '%s\n' "$FAKE_SSH_BIN"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$branch_root/setup-check-error.lock"
				return 0
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result=error
				g_zxfer_ssh_control_socket_action_stderr="check failed"
				return 1
			}
			zxfer_release_ssh_control_socket_lock_with_precedence() {
				printf 'release_setup_check:%s:%s:%s\n' "$1" "$2" "$3" >>"$release_log"
				return "$3"
			}
			zxfer_throw_error() {
				printf 'setup_check_throw=%s\n' "$1"
				exit 9
			}
			(
				zxfer_setup_ssh_control_socket "origin.example" origin
			) 2>"$branch_root/setup-check-error.err"
			printf 'setup_check_status=%s\n' "$?"
			printf 'setup_check_release=%s\n' "$(cat "$release_log")"
			printf 'setup_check_err=%s\n' "$(cat "$branch_root/setup-check-error.err")"
		)
	)

	assertContains "Origin socket close should preserve cleanup failures after a successful exit action." \
		"$output" "origin_success_cleanup_status=5"
	assertContains "Origin socket close should report cache cleanup failures after a successful exit action." \
		"$output" "origin_success_cleanup_err=Error removing ssh control socket cache directory for origin host."
	assertContains "Origin socket close should preserve cleanup failures after a stale exit action." \
		"$output" "origin_stale_cleanup_status=6"
	assertContains "Origin socket close should report cache cleanup failures after a stale exit action." \
		"$output" "origin_stale_cleanup_err=Error removing ssh control socket cache directory for origin host."
	assertContains "Origin socket close without a lease should preserve stale cleanup failures." \
		"$output" "origin_no_lease_stale_status=7"
	assertContains "Origin socket close without a lease should report stale cleanup failures." \
		"$output" "origin_no_lease_stale_err=Error removing ssh control socket cache directory for origin host."
	assertContains "SSH control socket setup should release the lock when check reports a non-stale error." \
		"$output" "setup_check_release=release_setup_check:origin:$branch_root/setup-check-error.lock:1"
	assertContains "SSH control socket setup should route non-stale check errors through throw_error." \
		"$output" "setup_check_throw=Error creating ssh control socket for origin host."
	assertContains "SSH control socket setup should preserve the action diagnostic before throwing." \
		"$output" "setup_check_err=check failed"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
