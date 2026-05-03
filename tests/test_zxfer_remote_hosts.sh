#!/bin/sh
#
# shunit2 tests for zxfer_remote_hosts.sh and related runtime helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

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

find_csh_shell_for_tests() {
	command -v csh 2>/dev/null || command -v tcsh 2>/dev/null || true
}

create_fake_ssh_join_csh_exec_bin() {
	l_path=$1
	l_csh_shell=$2
	cat >"$l_path" <<EOF
#!/bin/sh
while [ \$# -gt 0 ]; do
	case "\$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=\$1
shift
remote_cmd=""
for arg in "\$@"; do
	if [ "\$remote_cmd" = "" ]; then
		remote_cmd=\$arg
	else
		remote_cmd="\$remote_cmd \$arg"
	fi
done
if [ -n "\${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "\$host" >>"\$FAKE_SSH_LOG"
	printf '%s\n' "\$remote_cmd" >>"\$FAKE_SSH_LOG"
fi
"$l_csh_shell" -fc "\$remote_cmd"
EOF
	chmod +x "$l_path"
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

write_remote_capability_cache_fixture() {
	l_cache_path=$1
	l_cache_epoch=${2:-$(date '+%s')}
	l_cache_payload=${3:-$(fake_remote_capability_response)}
	l_cache_host_spec=${4:-origin.example}
	l_cache_requested_tools=${5:-}

	l_cache_identity_hex=$(zxfer_remote_capability_cache_identity_hex_for_host \
		"$l_cache_host_spec" "$l_cache_requested_tools") ||
		fail "Unable to derive remote capability cache fixture identity."

	zxfer_write_cache_object_contents_to_path \
		"$l_cache_path" \
		"$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" \
		"created_epoch=$l_cache_epoch
identity_hex=$l_cache_identity_hex" \
		"$l_cache_payload" >/dev/null ||
		fail "Unable to write remote capability cache fixture."
	chmod 600 "$l_cache_path"
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

create_stale_owned_lock_fixture() {
	l_lock_dir=$1
	l_kind=$2
	l_purpose=$3
	l_pid=${4:-999999999}

	write_owned_lock_metadata_fixture "$l_lock_dir" "$l_kind" "$l_purpose" "$l_pid"
}

write_unsupported_pid_lock_fixture() {
	l_lock_dir=$1
	l_pid=${2:-}
	l_dir_mode=${3:-700}
	l_pid_mode=${4:-600}

	mkdir -p "$l_lock_dir" || fail "Unable to create unsupported pid-lock fixture directory."
	chmod "$l_dir_mode" "$l_lock_dir" || fail "Unable to chmod unsupported pid-lock fixture directory."
	if [ -n "$l_pid" ]; then
		printf '%s\n' "$l_pid" >"$l_lock_dir/pid" ||
			fail "Unable to write unsupported pid-lock fixture pid file."
		chmod "$l_pid_mode" "$l_lock_dir/pid" ||
			fail "Unable to chmod unsupported pid-lock fixture pid file."
	fi
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	TEST_PRIVATE_DEFAULT_TMPDIR=$(mktemp -d /tmp/zxfer-rh.XXXXXX) || {
		echo "Unable to create private remote-host test temp root." >&2
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
	g_origin_remote_capabilities_cache_write_unavailable=0
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_dependency_path=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_cache_write_unavailable=0
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
	g_zxfer_temp_prefix=""
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
	if command -v zxfer_reset_owned_lock_tracking >/dev/null 2>&1; then
		zxfer_reset_owned_lock_tracking
	fi
	create_fake_ssh_bin
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

	assertEquals "ssh control socket action failure message emission should stay silent when no detail is staged and no default is supplied." \
		"" "$blank_output"
	assertEquals "ssh control socket action failure message emission should still succeed when no message is emitted." \
		0 "$blank_status"
	assertEquals "ssh control socket action failure message emission should print the default message when no detail is staged." \
		"default action failure." "$default_output"
	assertEquals "ssh control socket action failure message emission should succeed when printing the default message." \
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

test_zxfer_validate_remote_capability_cache_lock_dir_helpers_cover_metadata_purpose_paths() {
	lock_dir="$TEST_TMPDIR/remote_caps_wrong_purpose"
	write_owned_lock_metadata_fixture "$lock_dir" lock "ssh-control-socket-lock"

	output=$(
		(
			set +e
			zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
			printf 'wrong=%s\n' "$?"
		)
	)
	assertContains "Remote capability lock validation should reject owned locks written for another purpose." \
		"$output" "wrong=2"

	lock_dir="$TEST_TMPDIR/remote_caps_valid"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"

	output=$(
		(
			set +e
			zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
			printf 'valid=%s\n' "$?"
		)
	)
	assertContains "Remote capability lock validation should accept owned locks written with the remote capability cache purpose." \
		"$output" "valid=0"
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

	assertContains "Remote capability cache write-unavailable tracking should initially report origin cache writes as available." \
		"$output" "origin_before=1"
	assertContains "Remote capability cache write-unavailable tracking should mark the origin slot when write availability is disabled." \
		"$output" "origin_flag=1"
	assertContains "Remote capability cache write-unavailable tracking should then report origin cache writes as unavailable." \
		"$output" "origin_after=0"
	assertContains "Remote capability cache write-unavailable tracking should initially report target cache writes as available for the matching requested-tool identity." \
		"$output" "target_before=1"
	assertContains "Remote capability cache write-unavailable tracking should mark the target slot when write availability is disabled." \
		"$output" "target_flag=1"
	assertContains "Remote capability cache write-unavailable tracking should then report target cache writes as unavailable for the matching requested-tool identity." \
		"$output" "target_after=0"
	assertContains "Remote capability cache write-unavailable tracking should ignore failed cache identity refreshes when recording unavailability." \
		"$output" "broken_note=0"
	assertContains "Remote capability cache write-unavailable tracking should fail open when cache identity refresh fails during availability checks." \
		"$output" "broken_check=1"
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
	assertContains "Remote-host cache cleanup should remove directories that are not actual remote-host cache roots even when they contain legacy-like names." \
		"$output" "exists=no"
}

test_zxfer_reset_snapshot_record_indexes_removes_directory_and_resets_state() {
	g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index-reset"
	mkdir -p "$g_zxfer_snapshot_index_dir"
	printf '%s\n' "stale" >"$g_zxfer_snapshot_index_dir/source.records"
	g_zxfer_snapshot_index_unavailable=1
	g_zxfer_source_snapshot_record_index_dir="$g_zxfer_snapshot_index_dir/source.1.obj"
	g_zxfer_source_snapshot_record_index="tank/src	$g_zxfer_snapshot_index_dir/source.records"
	g_zxfer_source_snapshot_record_index_ready=1
	g_zxfer_destination_snapshot_record_index_dir="$g_zxfer_snapshot_index_dir/destination.1.obj"
	g_zxfer_destination_snapshot_record_index="backup/dst	$g_zxfer_snapshot_index_dir/dest.records"
	g_zxfer_destination_snapshot_record_index_ready=1

	zxfer_reset_snapshot_record_indexes

	assertFalse "Resetting snapshot-record indexes should remove the backing temp directory." \
		"[ -d '$TEST_TMPDIR/snapshot-index-reset' ]"
	assertEquals "Resetting snapshot-record indexes should clear the unavailable flag." \
		0 "${g_zxfer_snapshot_index_unavailable:-1}"
	assertEquals "Resetting snapshot-record indexes should clear the source index map." \
		"" "${g_zxfer_source_snapshot_record_index:-}"
	assertEquals "Resetting snapshot-record indexes should clear the destination index map." \
		"" "${g_zxfer_destination_snapshot_record_index:-}"
	assertEquals "Resetting snapshot-record indexes should clear the source generation directory." \
		"" "${g_zxfer_source_snapshot_record_index_dir:-}"
	assertEquals "Resetting snapshot-record indexes should clear the destination generation directory." \
		"" "${g_zxfer_destination_snapshot_record_index_dir:-}"
	assertEquals "Resetting snapshot-record indexes should clear the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-1}"
	assertEquals "Resetting snapshot-record indexes should clear the destination ready flag." \
		0 "${g_zxfer_destination_snapshot_record_index_ready:-1}"
}

test_zxfer_ensure_snapshot_index_dir_handles_unavailable_and_mktemp_failures() {
	set +e
	output=$(
		(
			g_zxfer_snapshot_index_unavailable=1
			zxfer_ensure_snapshot_index_dir
		)
	)
	status=$?
	assertEquals "Snapshot-index dir creation should short-circuit once the cache is marked unavailable." 1 "$status"
	assertEquals "Unavailable snapshot-index cache creation should not produce a payload." "" "$output"

	set +e
	output=$(
		(
			mktemp() {
				return 1
			}
			zxfer_ensure_snapshot_index_dir || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				printf 'dir=%s\n' "${g_zxfer_snapshot_index_dir:-}"
				return 1
			}
		)
	)
	status=$?

	assertEquals "Snapshot-index dir creation should fail cleanly when mktemp fails." 1 "$status"
	assertContains "mktemp failures should mark the snapshot-index cache unavailable." \
		"$output" "unavailable=1"
	assertContains "mktemp failures should leave the snapshot-index dir unset." \
		"$output" "dir="
}

test_zxfer_ensure_snapshot_index_dir_marks_cache_unavailable_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				return 1
			}
			zxfer_ensure_snapshot_index_dir || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				printf 'dir=%s\n' "${g_zxfer_snapshot_index_dir:-}"
				return 1
			}
		)
	)
	status=$?

	assertEquals "Snapshot-index dir creation should fail cleanly when effective temp-root lookup fails." \
		1 "$status"
	assertContains "Effective temp-root lookup failures should mark the snapshot-index cache unavailable." \
		"$output" "unavailable=1"
	assertContains "Effective temp-root lookup failures should leave the snapshot-index dir unset." \
		"$output" "dir="
}

test_zxfer_ensure_snapshot_index_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR_PHYSICAL/snapshot_index_effective_root"
	mkdir -p "$cache_root"

	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_snapshot_index_dir || exit $?
			printf 'dir=%s\n' "$g_zxfer_snapshot_index_dir"
		)
	)
	status=$?

	assertEquals "Snapshot-index dir creation should succeed when the validated effective temp root is available." \
		0 "$status"
	assertContains "Snapshot-index dirs should be created under the effective temp root instead of raw TMPDIR." \
		"$output" "dir=$cache_root/zxfer-snapshot-index."
}

test_zxfer_build_snapshot_record_index_handles_invalid_side_and_failures() {
	set +e
	output=$(
		(
			zxfer_build_snapshot_record_index other "tank/src@snap1"
		)
	)
	status=$?
	assertEquals "Snapshot-record index builds should reject unknown sides." 1 "$status"
	assertEquals "Rejected snapshot-record index builds should not produce a payload." "" "$output"

	set +e
	output=$(
		(
			g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index-build"
			mkdir -p "$g_zxfer_snapshot_index_dir"
			mkdir() {
				return 1
			}
			zxfer_build_snapshot_record_index source "tank/src@snap1" || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				return 1
			}
		)
	)
	status=$?
	assertEquals "Snapshot-record index builds should fail cleanly when their side directory cannot be created." \
		1 "$status"
	assertContains "Side-directory creation failures should stay local to that build attempt." \
		"$output" "unavailable=0"

	set +e
	output=$(
		(
			g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index-awk"
			mkdir -p "$g_zxfer_snapshot_index_dir"
			g_cmd_awk=false
			zxfer_build_snapshot_record_index destination "backup/dst@snap1" || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				return 1
			}
		)
	)
	status=$?
	assertEquals "Snapshot-record index builds should fail cleanly when awk cannot build the index." \
		1 "$status"
	assertContains "Awk build failures should stay local to that build attempt." \
		"$output" "unavailable=0"
}

test_zxfer_build_snapshot_record_index_returns_failure_when_index_dir_setup_fails_in_current_shell() {
	set +e
	(
		zxfer_ensure_snapshot_index_dir() {
			return 1
		}
		zxfer_build_snapshot_record_index source "tank/src@snap1"
	)
	status=$?

	assertEquals "Snapshot-record index builds should fail when the index directory cannot be prepared." \
		1 "$status"
}

test_zxfer_note_destination_dataset_exists_appends_new_children_in_current_shell() {
	g_recursive_dest_list="backup/dst"

	zxfer_note_destination_dataset_exists "backup/dst/child"

	assertEquals "New destination datasets should be appended as exact newline-delimited entries." \
		"backup/dst
backup/dst/child" "$g_recursive_dest_list"
}

test_zxfer_note_destination_dataset_exists_sets_first_entry_when_list_is_empty() {
	g_recursive_dest_list=""

	zxfer_note_destination_dataset_exists "backup/dst"

	assertEquals "The first observed destination dataset should seed the recursive destination list directly." \
		"backup/dst" "$g_recursive_dest_list"
}

test_zxfer_get_snapshot_record_helpers_handle_missing_files_and_invalid_sides() {
	output_file="$TEST_TMPDIR/missing_snapshot_record.out"
	zxfer_build_snapshot_record_index source "$(printf '%s\n' \
		"tank/src@snap2" \
		"tank/src@snap1")"
	rm -f "$g_zxfer_source_snapshot_record_index_dir/records/1.records"

	set +e
	zxfer_get_indexed_snapshot_records_for_dataset source "tank/src" >"$output_file"
	status=$?
	output=$(cat "$output_file")
	assertEquals "Indexed snapshot lookups should fail when the recorded cache file is missing." 1 "$status"
	assertEquals "Missing snapshot-index files should not produce a payload." "" "$output"

	set +e
	output=$(zxfer_get_indexed_snapshot_records_for_dataset other "tank/src")
	status=$?
	assertEquals "Indexed snapshot lookups should reject unknown sides." 1 "$status"
	assertEquals "Rejected indexed snapshot lookups should not produce a payload." "" "$output"

	zxfer_build_snapshot_record_index destination "$(printf '%s\n' \
		"backup/other@snap1" \
		"backup/other@snap2")"
	output=$(zxfer_get_indexed_snapshot_records_for_dataset destination "backup/dst")
	status=$?
	assertEquals "Ready snapshot indexes should return success with empty output when the dataset is absent from the index." \
		0 "$status"
	assertEquals "Absent indexed datasets should yield an empty payload." "" "$output"

	set +e
	output=$(zxfer_get_snapshot_records_for_dataset other "tank/src")
	status=$?
	assertEquals "Snapshot-record retrieval should reject unknown sides even after index fallback." 1 "$status"
	assertEquals "Rejected snapshot-record retrieval should not produce a payload." "" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_filters_global_records_without_building_indexes() {
	output=$(
		(
			source_root_file="$TEST_TMPDIR/lazy_source_root.records"
			dest_root_file="$TEST_TMPDIR/lazy_dest_root.records"
			g_lzfs_list_hr_snap=$(printf '%s\n%s\n%s' \
				"tank/src@snap1" \
				"tank/src/child@child1" \
				"tank/src@snap2")
			g_rzfs_list_hr_snap=$(printf '%s\n%s\n%s' \
				"backup/dst@snap2" \
				"backup/dst@legacy1" \
				"backup/dst/child@child1")
			printf 'source_ready_before=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready_before=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
			printf 'source_reversed_before=%s\n' "${g_lzfs_list_hr_S_snap:-}"
			zxfer_get_snapshot_records_for_dataset source "tank/src" >"$source_root_file"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst" >"$dest_root_file"
			printf 'source_root=%s\n' "$(cat "$source_root_file")"
			printf 'dest_root=%s\n' "$(cat "$dest_root_file")"
			printf 'source_ready_after=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready_after=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
			printf 'source_reversed_after=%s\n' "${g_lzfs_list_hr_S_snap:-}"
		)
	)

	assertContains "Snapshot-record lookup should leave the source index unset before direct filtering." \
		"$output" "source_ready_before=0"
	assertContains "Snapshot-record lookup should leave the destination index unset before direct filtering." \
		"$output" "dest_ready_before=0"
	assertContains "Source record filtering should not precompute the reversed source cache before lookup." \
		"$output" "source_reversed_before="
	assertContains "Source record filtering should still return newest-first source records once requested." \
		"$output" "source_root=tank/src@snap2
tank/src@snap1"
	assertContains "Destination record filtering should still return the live destination records once requested." \
		"$output" "dest_root=backup/dst@snap2
backup/dst@legacy1"
	assertContains "Source record filtering should avoid building the heavy source snapshot index after lookup." \
		"$output" "source_ready_after=0"
	assertContains "Destination record filtering should avoid building the heavy destination snapshot index after lookup." \
		"$output" "dest_ready_after=0"
	assertContains "Source record filtering should populate the reversed cache only after a consumer requests source records." \
		"$output" "source_reversed_after=tank/src@snap2
tank/src/child@child1
tank/src@snap1"
}

test_zxfer_get_snapshot_records_for_dataset_falls_back_to_cached_source_records_when_lazy_index_build_fails() {
	output=$(
		(
			g_lzfs_list_hr_snap=$(printf '%s\n%s' \
				"tank/src@snap1" \
				"tank/src@snap2")
			zxfer_build_snapshot_record_index() {
				return 1
			}
			zxfer_get_snapshot_records_for_dataset source "tank/src"
		)
	)

	assertEquals "When lazy source index creation fails, source snapshot-record lookup should still fall back to the reversed in-memory cache." \
		"tank/src@snap2
tank/src@snap1" "$output"
}

test_zxfer_parse_remote_capability_response_extracts_fields() {
	result=$(
		(
			zxfer_parse_remote_capability_response "$(fake_remote_capability_response)"
			printf 'os=%s\n' "$g_zxfer_remote_capability_os"
			printf 'zfs=%s:%s\n' "$g_zxfer_remote_capability_zfs_status" "$g_zxfer_remote_capability_zfs_path"
			printf 'parallel=%s:%s\n' "$g_zxfer_remote_capability_parallel_status" "$g_zxfer_remote_capability_parallel_path"
			printf 'cat=%s:%s\n' "$g_zxfer_remote_capability_cat_status" "$g_zxfer_remote_capability_cat_path"
		)
	)

	assertContains "The parser should extract the remote operating system." "$result" "os=RemoteOS"
	assertContains "The parser should extract the remote zfs helper path." "$result" "zfs=0:/remote/bin/zfs"
	assertContains "The parser should extract the remote parallel helper path." "$result" "parallel=0:/opt/bin/parallel"
	assertContains "The parser should extract the remote cat helper path." "$result" "cat=0:/remote/bin/cat"
}

test_zxfer_parse_remote_capability_response_clears_optional_paths_for_missing_tools() {
	result=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	1	-"
			printf 'parallel=%s:%s\n' "$g_zxfer_remote_capability_parallel_status" "$g_zxfer_remote_capability_parallel_path"
			printf 'cat=%s:%s\n' "$g_zxfer_remote_capability_cat_status" "$g_zxfer_remote_capability_cat_path"
		)
	)

	assertContains "The parser should preserve missing parallel status codes." \
		"$result" "parallel=1:"
	assertContains "The parser should clear the parallel path when the tool is missing." \
		"$result" "parallel=1:"
	assertContains "The parser should preserve missing cat status codes." \
		"$result" "cat=1:"
	assertContains "The parser should clear the cat path when the tool is missing." \
		"$result" "cat=1:"
}

test_zxfer_parse_remote_capability_response_rejects_retired_v1_protocol() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs"
		)
	)
	status=$?

	assertEquals "Capability payloads that still advertise the retired V1 protocol should be rejected." \
		1 "$status"
	assertEquals "Rejected V1 capability payloads should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_malformed_records() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	oops	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Malformed capability records should be rejected." 1 "$status"
	assertEquals "Malformed capability records should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_missing_os_payload() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Capability records without an OS payload should be rejected." 1 "$status"
	assertEquals "Capability records without an OS payload should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_preserves_additional_tool_entries() {
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	weirdtool	0	/remote/bin/weirdtool
tool	cat	0	/remote/bin/cat"
			printf 'zfs_status=%s\n' "$g_zxfer_remote_capability_zfs_status"
			printf 'cat_path=%s\n' "$g_zxfer_remote_capability_cat_path"
			zxfer_get_parsed_remote_capability_tool_record weirdtool
			printf 'weirdtool_status=%s\n' "$g_zxfer_remote_capability_tool_status_result"
			printf 'weirdtool_path=%s\n' "$g_zxfer_remote_capability_tool_path_result"
		)
	)
	status=$?

	assertEquals "Capability records should tolerate additional advertised tool names." 0 "$status"
	assertContains "Capability records with additional tool names should preserve the required zfs status." \
		"$output" "zfs_status=0"
	assertContains "Capability records with additional tool names should preserve known helper paths." \
		"$output" "cat_path=/remote/bin/cat"
	assertContains "Capability records with additional tool names should preserve those extra tool records for later lookups." \
		"$output" "weirdtool_status=0"
	assertContains "Capability records with additional tool names should keep the extra helper path." \
		"$output" "weirdtool_path=/remote/bin/weirdtool"
}

test_zxfer_render_remote_capability_cache_identity_includes_requested_tool_set_for_host() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_option_j_jobs=4
	g_option_e_restore_property_mode=1
	g_option_k_backup_property_mode=1
	g_option_z_compress=1
	g_cmd_compress="zstd -T0 -9"
	g_cmd_decompress="zstd -d"

	origin_identity=$(zxfer_render_remote_capability_cache_identity_for_host "origin.example")
	target_identity=$(zxfer_render_remote_capability_cache_identity_for_host "target.example")

	assertContains "Origin-side capability-cache identities should include the required zfs helper." \
		"$origin_identity" "zfs"
	assertContains "Origin-side capability-cache identities should include parallel when remote source jobs are enabled." \
		"$origin_identity" "parallel"
	assertContains "Origin-side capability-cache identities should include cat when restore-property mode needs it." \
		"$origin_identity" "cat"
	assertContains "Origin-side capability-cache identities should include the remote compression command head for -z/-Z runs." \
		"$origin_identity" "zstd"
	assertContains "Target-side capability-cache identities should include cat when backup-property mode needs it." \
		"$target_identity" "cat"
	assertContains "Target-side capability-cache identities should include the remote decompression command head for -z/-Z runs." \
		"$target_identity" "zstd"
	assertNotContains "Target-side capability-cache identities should not include parallel when only the origin host uses source-job fan-out." \
		"$target_identity" "parallel"
	assertNotEquals "Capability-cache identities should change when the requested tool set differs by host role." \
		"$origin_identity" "$target_identity"
}

test_zxfer_render_remote_capability_cache_identity_accepts_explicit_requested_tool_scope_for_host() {
	g_option_O_origin_host="origin.example"
	g_option_j_jobs=4
	g_option_e_restore_property_mode=1
	g_option_z_compress=1
	g_cmd_compress="zstd -T0 -9"

	minimal_identity=$(zxfer_render_remote_capability_cache_identity_for_host "origin.example" "zfs")
	parallel_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "$(zxfer_get_remote_capability_requested_tools_for_tool parallel)")

	assertContains "Explicit capability-cache scopes should still include zfs." \
		"$minimal_identity" "zfs"
	assertNotContains "Minimal startup capability scopes should not preload parallel." \
		"$minimal_identity" "parallel"
	assertNotContains "Minimal startup capability scopes should not preload restore-property helpers." \
		"$minimal_identity" "cat"
	assertNotContains "Minimal startup capability scopes should not preload compression helpers." \
		"$minimal_identity" "zstd"
	assertContains "Tool-specific capability scopes should include the requested helper." \
		"$parallel_identity" "parallel"
	assertNotEquals "Minimal startup scopes should key capability caches differently from later parallel lookups." \
		"$minimal_identity" "$parallel_identity"
}

test_zxfer_render_remote_capability_cache_identity_canonicalizes_explicit_requested_tool_scope_for_host() {
	g_option_O_origin_host="origin.example"

	literal_identity=$(zxfer_render_remote_capability_cache_identity_for_host "origin.example" "parallel")
	helper_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "$(zxfer_get_remote_capability_requested_tools_for_tool parallel)")
	reordered_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "parallel
zfs")

	assertEquals "Literal explicit tool scopes should normalize to the helper-generated zfs-first scope." \
		"$helper_identity" "$literal_identity"
	assertEquals "Explicit tool scopes should normalize away reordered duplicate zfs entries." \
		"$helper_identity" "$reordered_identity"
}

test_zxfer_render_remote_capability_cache_identity_propagates_transport_and_requested_tool_failures() {
	transport_output=$(
		(
			set +e
			zxfer_render_ssh_transport_policy_identity() {
				printf '%s\n' "transport policy failed"
				return 7
			}
			identity=$(zxfer_render_remote_capability_cache_identity_for_host "origin.example")
			printf 'status=%s\n' "$?"
			printf 'output=%s\n' "$identity"
		)
	)
	resolve_output=$(
		(
			set +e
			zxfer_resolve_remote_capability_requested_tools_for_host() {
				return 9
			}
			identity=$(zxfer_render_remote_capability_cache_identity_for_host "origin.example" "parallel")
			printf 'status=%s\n' "$?"
			printf 'output=%s\n' "$identity"
		)
	)

	assertContains "Capability-cache identity rendering should surface staged ssh transport policy failures." \
		"$transport_output" "status=1"
	assertContains "Capability-cache identity rendering should preserve non-empty ssh transport policy diagnostics." \
		"$transport_output" "output=transport policy failed"
	assertContains "Capability-cache identity rendering should fail closed when requested-tool resolution fails." \
		"$resolve_output" "status=1"
	assertContains "Capability-cache identity rendering should not print a partial identity when requested-tool resolution fails." \
		"$resolve_output" "output="
}

test_zxfer_parse_remote_capability_response_rejects_extra_lines() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "$(fake_remote_capability_response)
extra	line"
		)
	)
	status=$?

	assertEquals "Capability records with extra lines should be rejected." 1 "$status"
	assertEquals "Capability records with extra lines should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_control_whitespace_helper_paths() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os${tab}RemoteOS
tool${tab}zfs${tab}0${tab}/remote/bin/zfs${cr}
tool${tab}parallel${tab}0${tab}/opt/bin/parallel
tool${tab}cat${tab}0${tab}/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Capability payloads with control-whitespace helper paths should be rejected as invalid handshakes." \
		1 "$status"
	assertEquals "Rejected control-whitespace capability payloads should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_duplicate_tool_records_in_current_shell() {
	set +e
	zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	zfs	0	/remote/bin/zfs-second" >/dev/null 2>&1
	status=$?

	assertEquals "Direct current-shell capability parsing should reject duplicate tool records." \
		1 "$status"
}

test_zxfer_parse_remote_capability_response_fails_closed_when_tool_record_append_fails_in_current_shell() {
	set +e
	zxfer_append_remote_capability_tool_record() {
		return 1
	}
	zxfer_parse_remote_capability_response "$(fake_remote_capability_response)" >/dev/null 2>&1
	status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Direct current-shell capability parsing should fail closed when appending a parsed tool record fails." \
		1 "$status"
}

test_zxfer_store_cached_remote_capability_response_for_host_updates_target_slot() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"

	zxfer_store_cached_remote_capability_response_for_host "target.example" "$(fake_remote_capability_response)"

	assertEquals "Target-side host caching should update the target cache slot." \
		"target.example" "$g_target_remote_capabilities_host"
	assertEquals "Target-side host caching should key the cache slot by the active secure dependency path." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$g_target_remote_capabilities_dependency_path"
	assertEquals "Target-side host caching should also key the cache slot by the active capability-cache identity." \
		"$(zxfer_render_remote_capability_cache_identity)" "$g_target_remote_capabilities_cache_identity"
	assertContains "Target-side host caching should store the capability payload." \
		"$g_target_remote_capabilities_response" "tool	cat	0	/remote/bin/cat"
}

test_zxfer_store_cached_remote_capability_response_for_host_updates_origin_slot() {
	g_option_O_origin_host="origin.example"

	zxfer_store_cached_remote_capability_response_for_host "origin.example" "$(fake_remote_capability_response)"

	assertEquals "Origin-side host caching should update the origin cache slot." \
		"origin.example" "$g_origin_remote_capabilities_host"
	assertEquals "Origin-side host caching should key the cache slot by the active secure dependency path." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$g_origin_remote_capabilities_dependency_path"
	assertEquals "Origin-side host caching should also key the cache slot by the active capability-cache identity." \
		"$(zxfer_render_remote_capability_cache_identity)" "$g_origin_remote_capabilities_cache_identity"
	assertContains "Origin-side host caching should store the capability payload." \
		"$g_origin_remote_capabilities_response" "tool	parallel	0	/opt/bin/parallel"
}

test_zxfer_store_cached_remote_capability_response_for_host_resets_target_bootstrap_source_when_identity_refresh_fails() {
	output=$(
		(
			g_option_T_target_host="target.example"
			g_target_remote_capabilities_host="old-target.example"
			g_target_remote_capabilities_cache_identity="stale-target-identity"
			g_target_remote_capabilities_bootstrap_source="memory"
			zxfer_render_remote_capability_cache_identity_for_host() {
				return 1
			}
			zxfer_store_cached_remote_capability_response_for_host \
				"target.example" "$(fake_remote_capability_response)"
			printf 'host=%s\n' "${g_target_remote_capabilities_host:-}"
			printf 'identity=<%s>\n' "${g_target_remote_capabilities_cache_identity:-}"
			printf 'bootstrap=<%s>\n' "${g_target_remote_capabilities_bootstrap_source:-}"
			printf 'response=%s\n' "${g_target_remote_capabilities_response:-}"
		)
	)

	assertContains "Target-side host caching should still update the target slot when capability-cache identity refresh fails." \
		"$output" "host=target.example"
	assertContains "Target-side host caching should clear the stored cache identity when the identity refresh fails closed." \
		"$output" "identity=<>"
	assertContains "Target-side host caching should reset bootstrap-source tracking when reusing the target slot for a different host after an identity refresh failure." \
		"$output" "bootstrap=<>"
	assertContains "Target-side host caching should still retain the capability payload after an identity refresh failure." \
		"$output" "response=ZXFER_REMOTE_CAPS_V2"
}

test_zxfer_get_cached_remote_capability_response_for_host_reads_origin_slot() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity)
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	result=$(zxfer_get_cached_remote_capability_response_for_host "origin.example")

	assertContains "Origin-side cached capability reads should return the cached payload." \
		"$result" "tool	parallel	0	/opt/bin/parallel"
}

test_zxfer_get_cached_remote_capability_response_for_host_reads_target_slot() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity)
	g_target_remote_capabilities_response=$(fake_remote_capability_response)

	result=$(zxfer_get_cached_remote_capability_response_for_host "target.example")

	assertContains "Target-side cached capability reads should return the cached payload." \
		"$result" "tool	cat	0	/remote/bin/cat"
}

test_zxfer_get_cached_remote_capability_response_for_host_rejects_mismatched_requested_tool_identity() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "zfs")
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	set +e
	result=$(zxfer_get_cached_remote_capability_response_for_host "origin.example" "parallel")
	status=$?

	assertEquals "Cached capability reads should fail closed when the requested tool scope does not match the cached identity." \
		1 "$status"
	assertEquals "Mismatched requested-tool cache reads should not print the cached payload." "" "$result"
}

test_zxfer_get_cached_remote_capability_response_for_host_ignores_stale_dependency_path_entries() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_dependency_path="/stale/secure/path"
	g_target_remote_capabilities_cache_identity=$(printf '%s\n%s' "/stale/secure/path" "$(zxfer_render_ssh_transport_policy_identity)")
	g_target_remote_capabilities_response=$(fake_remote_capability_response)
	ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"

	set +e
	result=$(zxfer_get_cached_remote_capability_response_for_host "target.example")
	status=$?

	assertEquals "Cached capability entries should be ignored when they were populated for a different secure dependency path." \
		1 "$status"
	assertEquals "Ignored stale cached capability entries should not print a payload." "" "$result"
}

test_zxfer_get_cached_remote_capability_response_for_host_ignores_stale_ssh_transport_policy_entries() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_target_remote_capabilities_cache_identity=$(printf '%s\n%s' "$ZXFER_DEFAULT_SECURE_PATH" "ambient")
	g_target_remote_capabilities_response=$(fake_remote_capability_response)

	set +e
	result=$(zxfer_get_cached_remote_capability_response_for_host "target.example")
	status=$?

	assertEquals "Cached capability entries should be ignored when they were populated for a different ssh transport policy." \
		1 "$status"
	assertEquals "Ignored stale ssh-policy cache entries should not print a payload." "" "$result"
}

test_zxfer_get_cached_remote_capability_response_for_host_fails_when_identity_refresh_fails() {
	output=$(
		(
			set +e
			zxfer_render_remote_capability_cache_identity_for_host() {
				return 1
			}
			response=$(zxfer_get_cached_remote_capability_response_for_host "target.example")
			printf 'status=%s\n' "$?"
			printf 'output=%s\n' "$response"
		)
	)

	assertContains "Cached capability reads should fail closed when capability-cache identity refresh fails." \
		"$output" "status=1"
	assertContains "Failed cached capability reads should not print a payload when capability-cache identity refresh fails." \
		"$output" "output="
}

test_zxfer_store_cached_remote_capability_response_for_host_falls_back_to_origin_slot() {
	zxfer_store_cached_remote_capability_response_for_host "shared.example" "$(fake_remote_capability_response)"

	assertEquals "Unassigned cached capability responses should populate the origin fallback slot first." \
		"shared.example" "$g_origin_remote_capabilities_host"
}

test_zxfer_store_cached_remote_capability_response_for_host_falls_back_to_target_slot_after_origin() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity)
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	zxfer_store_cached_remote_capability_response_for_host "other.example" "$(fake_remote_capability_response)"

	assertEquals "Once the origin fallback slot is occupied, later unassigned cache responses should populate the target slot." \
		"other.example" "$g_target_remote_capabilities_host"
}

test_zxfer_ensure_remote_capability_cache_dir_creates_secure_directory() {
	cache_dir=$(zxfer_ensure_remote_capability_cache_dir)
	mode=$(zxfer_get_path_mode_octal "$cache_dir")
	owner=$(zxfer_get_path_owner_uid "$cache_dir")
	effective_uid=$(zxfer_get_effective_user_uid)

	assertTrue "Capability cache directory creation should succeed." "[ -d '$cache_dir' ]"
	assertEquals "Capability cache directories should be created with 0700 permissions." "700" "$mode"
	assertEquals "Capability cache directories should be owned by the current effective uid." \
		"$effective_uid" "$owner"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_uid_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directory creation should fail cleanly when the effective uid cannot be determined." 1 "$status"
	assertEquals "Capability cache directory failures should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directory creation should fail cleanly when effective temp-root lookup fails." \
		1 "$status"
	assertEquals "Capability cache directory failures should not produce a payload when the temp root cannot be resolved." \
		"" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_rejects_insecure_existing_mode() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	mkdir "$cache_dir"
	chmod 755 "$cache_dir"

	set +e
	output=$(zxfer_ensure_remote_capability_cache_dir)
	status=$?

	assertEquals "Existing capability cache directories with insecure permissions should be rejected." 1 "$status"
	assertEquals "Rejected capability cache directories should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_existing_owner_lookup_fails() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	mkdir "$cache_dir"
	chmod 700 "$cache_dir"

	set +e
	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directories should fail cleanly when existing-directory owner lookup fails." 1 "$status"
	assertEquals "Owner-lookup failures for existing capability cache directories should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_reports_existing_owner_lookup_failure_in_current_shell() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	mkdir "$cache_dir"
	chmod 700 "$cache_dir"
	fake_bin_dir="$TEST_TMPDIR/remote_capability_owner_lookup_fail_bin"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/stat" <<'EOF'
#!/bin/sh
exit 1
EOF
	cat >"$fake_bin_dir/ls" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin_dir/stat" "$fake_bin_dir/ls"

	PATH="$fake_bin_dir:$original_path"
	zxfer_ensure_remote_capability_cache_dir >/dev/null 2>&1
	status=$?
	PATH=$original_path

	assertEquals "Capability cache directories should fail in the current shell when existing-directory owner lookup fails." \
		1 "$status"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_existing_mode_lookup_fails() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	mkdir "$cache_dir"
	chmod 700 "$cache_dir"

	set +e
	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directories should fail cleanly when existing-directory mode lookup fails." 1 "$status"
	assertEquals "Mode-lookup failures for existing capability cache directories should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_mkdir_fails() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	rm -rf "$cache_dir"

	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directory creation should fail cleanly when mkdir fails." 1 "$status"
	assertEquals "Capability cache directory mkdir failures should not produce a payload." "" "$output"
}

test_zxfer_remote_capability_cache_path_rejects_symlinked_cache_dir() {
	cache_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	rm -rf "$cache_dir"
	ln -s "$TEST_TMPDIR/other-cache-dir" "$cache_dir"

	set +e
	output=$(zxfer_remote_capability_cache_path "origin.example")
	status=$?

	assertEquals "Symlinked capability cache directories should be rejected." 1 "$status"
	assertEquals "Rejected cache-directory paths should not produce a payload." "" "$output"
}

test_zxfer_ssh_control_socket_cache_key_falls_back_when_hex_encoding_is_empty() {
	result=$(
		(
			cksum() {
				return 1
			}
			od() {
				:
			}
			zxfer_ssh_control_socket_cache_key "origin.example"
		)
	)

	assertEquals "Shared ssh control socket cache keys should fall back to a stable sentinel when hex encoding is empty." \
		"k00" "$result"
}

test_zxfer_ssh_control_socket_cache_key_uses_hex_fallback_in_current_shell() {
	output_file="$TEST_TMPDIR/ssh_control_socket_cache_key.out"

	(
		cksum() {
			return 1
		}
		od() {
			printf '%s\n' " 61 62 63 64"
		}
		zxfer_ssh_control_socket_cache_key "origin.example" >"$output_file"
	)

	assertEquals "Shared ssh control socket cache keys should fall back to a truncated hex digest when cksum is unavailable." \
		"k61626364" "$(cat "$output_file")"
}

test_zxfer_ssh_control_socket_cache_key_uses_path_shadowed_hex_fallback_in_current_shell() {
	fake_bin_dir="$TEST_TMPDIR/ssh_control_socket_cache_key_bin"
	output_file="$TEST_TMPDIR/ssh_control_socket_cache_key_shadowed.out"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/cksum" <<'EOF'
#!/bin/sh
exit 1
EOF
	cat >"$fake_bin_dir/od" <<'EOF'
#!/bin/sh
printf '%s\n' " 61 62 63 64"
EOF
	chmod +x "$fake_bin_dir/cksum" "$fake_bin_dir/od"

	PATH="$fake_bin_dir:$original_path"
	zxfer_ssh_control_socket_cache_key "origin.example" >"$output_file"
	PATH=$original_path

	assertEquals "Shared ssh control socket cache keys should exercise the hex fallback in the current shell when cksum is unavailable from PATH." \
		"k61626364" "$(cat "$output_file")"
}

test_zxfer_ssh_control_socket_cache_helper_failures_cover_current_shell_direct() {
	cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	set +e
	zxfer_get_effective_user_uid() {
		return 1
	}
	zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR" >/dev/null 2>&1
	path_uid_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	zxfer_get_remote_host_cache_root_prefix() {
		return 1
	}
	zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR" >/dev/null 2>&1
	path_prefix_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	zxfer_ssh_control_socket_cache_dir_path_for_tmpdir() {
		return 1
	}
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	ensure_path_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	rm -rf "$cache_dir"
	mkdir -p "$cache_dir" || fail "Unable to create the ssh control socket cache-dir fixture."
	chmod 700 "$cache_dir"
	zxfer_get_path_owner_uid() {
		return 1
	}
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	existing_owner_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	rm -rf "$cache_dir"
	zxfer_get_path_owner_uid() {
		return 1
	}
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	post_create_owner_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	rm -rf "$cache_dir"
	zxfer_get_path_mode_octal() {
		return 1
	}
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	post_create_mode_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"
	set -e

	assertEquals "Shared ssh control socket cache-dir path derivation should fail closed when the effective uid lookup fails in the current shell." \
		1 "$path_uid_status"
	assertEquals "Shared ssh control socket cache-dir path derivation should fail closed when the remote-host cache root prefix cannot be derived in the current shell." \
		1 "$path_prefix_status"
	assertEquals "Shared ssh control socket cache-dir setup should fail closed when cache-dir path derivation fails in the current shell." \
		1 "$ensure_path_status"
	assertEquals "Shared ssh control socket cache-dir setup should fail closed when an existing cache-dir owner lookup fails in the current shell." \
		1 "$existing_owner_status"
	assertEquals "Shared ssh control socket cache-dir setup should fail closed when the post-create owner lookup fails in the current shell." \
		1 "$post_create_owner_status"
	assertEquals "Shared ssh control socket cache-dir setup should fail closed when the post-create mode lookup fails in the current shell." \
		1 "$post_create_mode_status"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_returns_failure_when_uid_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should fail cleanly when uid lookup fails." 1 "$status"
	assertEquals "Failed shared ssh control socket cache dir creation should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_returns_failure_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_socket_cache_tmpdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should fail cleanly when effective temp-root lookup fails." \
		1 "$status"
	assertEquals "Failed shared ssh control socket cache dir creation should not produce a payload when the temp root cannot be resolved." \
		"" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_rejects_insecure_existing_mode() {
	cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	mkdir -p "$cache_dir"
	chmod 755 "$cache_dir"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_cache_dir)
	status=$?

	assertEquals "Shared ssh control socket cache dirs should reject insecure pre-existing permissions." 1 "$status"
	assertEquals "Rejected shared ssh control socket cache dirs should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_reports_existing_owner_lookup_failure_in_current_shell() {
	cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	mkdir -p "$cache_dir"
	chmod 700 "$cache_dir"
	fake_bin_dir="$TEST_TMPDIR/ssh_control_socket_owner_lookup_fail_bin"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/stat" <<'EOF'
#!/bin/sh
exit 1
EOF
	cat >"$fake_bin_dir/ls" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin_dir/stat" "$fake_bin_dir/ls"

	PATH="$fake_bin_dir:$original_path"
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	status=$?
	PATH=$original_path

	assertEquals "Shared ssh control socket cache dirs should fail in the current shell when existing-directory owner lookup fails." \
		1 "$status"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_returns_failure_when_mkdir_fails() {
	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should fail cleanly when mkdir fails." 1 "$status"
	assertEquals "Failed shared ssh control socket cache dir creation should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_reports_direct_lookup_failures_in_current_shell() {
	cache_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	rm -rf "$cache_dir"
	ln -s "$TEST_TMPDIR/other-shared-cache-dir" "$cache_dir"

	set +e
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	symlink_status=$?
	rm -f "$cache_dir"
	mkdir -p "$cache_dir"
	chmod 700 "$cache_dir"

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	mode_status=$?

	rm -rf "$cache_dir"
	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	create_owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	create_mode_status=$?

	assertEquals "Shared ssh control socket cache dir creation should reject symlinked cache dirs." \
		1 "$symlink_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when existing-directory owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when existing-directory mode lookup fails." \
		1 "$mode_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when post-create owner lookup fails." \
		1 "$create_owner_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when post-create mode lookup fails." \
		1 "$create_mode_status"
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

test_zxfer_ensure_ssh_control_socket_entry_dir_succeeds_when_entry_dir_creation_races_in_current_shell() {
	mkdir_bin=$(command -v mkdir)
	chmod_bin=$(command -v chmod)
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "krace")
	entry_dir="$cache_dir/krace"

	result=$(
		(
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' "krace"
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$cache_dir"
			}
			mkdir() {
				if [ "$1" = "$entry_dir" ]; then
					"$mkdir_bin" "$1" || :
					"$chmod_bin" 700 "$1" 2>/dev/null || :
					return 1
				fi
				"$mkdir_bin" "$@"
			}
			zxfer_ensure_ssh_control_socket_entry_dir "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket entry creation should tolerate a concurrent creator winning the entry-dir mkdir race." \
		0 "$status"
	assertEquals "Shared ssh control socket entry creation should still return the raced entry path." \
		"$entry_dir" "$result"
	assertTrue "Shared ssh control socket entry creation should leave the raced entry dir ready for use." \
		"[ -d '$entry_dir' ]"
	assertTrue "Shared ssh control socket entry creation should still create the leases dir after an entry-dir mkdir race." \
		"[ -d '$entry_dir/leases' ]"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_succeeds_when_leases_dir_creation_races_in_current_shell() {
	mkdir_bin=$(command -v mkdir)
	chmod_bin=$(command -v chmod)
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "krace")
	entry_dir="$cache_dir/krace"
	leases_dir="$entry_dir/leases"
	mkdir -p "$entry_dir"
	chmod 700 "$entry_dir"

	result=$(
		(
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' "krace"
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$cache_dir"
			}
			mkdir() {
				if [ "$1" = "$leases_dir" ]; then
					"$mkdir_bin" "$1" || :
					"$chmod_bin" 700 "$1" 2>/dev/null || :
					return 1
				fi
				"$mkdir_bin" "$@"
			}
			zxfer_ensure_ssh_control_socket_entry_dir "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket entry creation should tolerate a concurrent creator winning the leases-dir mkdir race." \
		0 "$status"
	assertEquals "Shared ssh control socket entry creation should still return the existing entry path after a leases-dir mkdir race." \
		"$entry_dir" "$result"
	assertTrue "Shared ssh control socket entry creation should leave the raced leases dir ready for use." \
		"[ -d '$leases_dir' ]"
	assertTrue "Shared ssh control socket entry creation should still publish an identity file after a leases-dir mkdir race." \
		"[ -f '$entry_dir/id' ]"
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

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_invalid_existing_identity_file() {
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "kinvalid")
	entry_dir="$cache_dir/kinvalid"
	mkdir -p "$entry_dir/leases"
	chmod 700 "$entry_dir" "$entry_dir/leases"
	printf '%s\n' "stale identity" >"$entry_dir/id"
	chmod 644 "$entry_dir/id"

	result=$(
		(
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' "kinvalid"
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_ensure_ssh_control_socket_entry_dir "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket entry creation should fail closed when an existing identity file is invalid." \
		1 "$status"
	assertEquals "Invalid shared ssh control socket identity files should not fall through to a suffixed sibling cache entry." \
		"" "$result"
	assertFalse "Shared ssh control socket entry creation should not create a suffixed sibling cache entry when existing identity metadata is corrupt." \
		"[ -e '$cache_dir/kinvalid.1' ]"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_keeps_socket_paths_short_for_long_hosts() {
	long_tmpdir="$TEST_TMPDIR_PHYSICAL/socket-root-segment-0123456789/socket-root-segment-0123456789/socket-root-segment-0123456789"
	mkdir -p "$long_tmpdir"
	expected_tmpdir=$(
		unset TMPDIR
		zxfer_try_get_socket_cache_tmpdir
	)

	result=$(
		TMPDIR=$long_tmpdir
		export TMPDIR
		g_cmd_ssh="/opt/local/bin/really-long-custom-ssh-wrapper"
		entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "aldo@doBackup.clientsupportsoftware.com pfexec -u root")
		socket_path="$entry_dir/s"
		temp_listener_path="$socket_path.Mvij6x1tYLn6woxm"
		printf 'socket=%s\n' "$socket_path"
		printf 'socket_length=%s\n' "${#socket_path}"
		printf 'temp_listener_length=%s\n' "${#temp_listener_path}"
	)
	temp_listener_length=$(printf '%s\n' "$result" | awk -F= '/^temp_listener_length=/{print $2}')

	assertContains "Short shared ssh control socket paths should still be rooted under the per-user cache dir." \
		"$result" "socket=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$expected_tmpdir")/"
	assertTrue "Shared ssh control socket paths should stay below the Unix domain socket limit even after OpenSSH appends its temporary suffix." \
		"[ \"$temp_listener_length\" -lt 104 ]"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_symlinked_entry_dir() {
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$cache_key")
	ln -s "$TEST_TMPDIR/other-entry-dir" "$cache_dir/$cache_key"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	status=$?

	assertEquals "Shared ssh control socket entry dirs should reject symlinked cache entries." 1 "$status"
	assertEquals "Rejected shared ssh control socket entry dirs should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_paths_that_are_too_long_in_current_shell() {
	cache_dir=$(zxfer_ensure_ssh_control_socket_cache_dir)

	set +e
	output=$(
		(
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' "klong"
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_render_ssh_control_socket_entry_identity() {
				printf '%s\n' "identity"
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket entry creation should fail closed when the computed entry path is too long." \
		1 "$status"
	assertEquals "Path-length failures should not emit a partial entry path." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_plain_file_entries() {
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$cache_key")
	: >"$cache_dir/$cache_key"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	status=$?

	assertEquals "Shared ssh control socket entry dirs should reject plain files where a metadata directory is expected." \
		1 "$status"
	assertEquals "Plain-file entry conflicts should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_insecure_leases_dir() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	chmod 755 "$entry_dir/leases"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	status=$?

	assertEquals "Shared ssh control socket entry dirs should reject insecure leases directories." 1 "$status"
	assertEquals "Rejected shared ssh control socket entry dirs with insecure leases dirs should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_existing_entry_lookup_failures_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	rm -rf "$entry_dir/leases"
	mkdir "$entry_dir/leases"
	chmod 700 "$entry_dir/leases"

	set +e
	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	uid_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket entry reuse should fail when entry-directory owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket entry reuse should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket entry reuse should fail when entry-directory mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_cache_key_failure_in_current_shell_direct() {
	set +e
	zxfer_ssh_control_socket_cache_key() {
		return 4
	}
	zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null 2>&1
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket entry creation should fail when cache-key derivation fails in the current shell." \
		1 "$status"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_identity_render_failure_in_current_shell_direct() {
	cache_dir=$(zxfer_ensure_ssh_control_socket_cache_dir)

	set +e
	zxfer_ssh_control_socket_cache_key() {
		printf '%s\n' "kidentity"
	}
	zxfer_get_ssh_control_socket_cache_dir_for_key() {
		printf '%s\n' "$cache_dir"
	}
	zxfer_render_ssh_control_socket_entry_identity() {
		printf '%s\n' "identity render failed"
		return 4
	}
	output=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket entry creation should fail when identity rendering fails in the current shell." \
		1 "$status"
	assertEquals "Shared ssh control socket entry creation should surface non-empty identity-render failure output." \
		"identity render failed" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_existing_entry_uid_and_mode_failures_after_cache_lookup_in_current_shell() {
	cache_dir="$TEST_TMPDIR/shared-entry-cache"
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	entry_dir="$cache_dir/$cache_key"
	mkdir -p "$entry_dir"
	chmod 700 "$entry_dir"

	set +e
	(
		zxfer_ensure_ssh_control_socket_cache_dir() {
			printf '%s\n' "$cache_dir"
		}
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	uid_status=$?

	(
		zxfer_ensure_ssh_control_socket_cache_dir() {
			printf '%s\n' "$cache_dir"
		}
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket entry reuse should fail when entry-branch effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket entry reuse should fail when entry-branch mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_leases_owner_mismatches_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	effective_uid=$(zxfer_get_effective_user_uid)
	entry_mode=$(zxfer_get_path_mode_octal "$entry_dir")
	leases_dir="$entry_dir/leases"

	set +e
	(
		zxfer_get_path_owner_uid() {
			case "$1" in
			"$entry_dir")
				printf '%s\n' "$effective_uid"
				;;
			"$leases_dir")
				printf '%s\n' "999999"
				;;
			esac
		}
		zxfer_get_effective_user_uid() {
			printf '%s\n' "$effective_uid"
		}
		zxfer_get_path_mode_octal() {
			printf '%s\n' "$entry_mode"
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	leases_owner_mismatch_status=$?
	set -e

	assertEquals "Shared ssh control socket entry reuse should fail when the leases dir is owned by a different uid." \
		1 "$leases_owner_mismatch_status"
}

test_zxfer_read_ssh_control_socket_entry_identity_file_reports_lookup_failures_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket identity reads should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket identity reads should fail when owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket identity reads should fail when mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_read_ssh_control_socket_entry_identity_file_rejects_owner_and_mode_mismatches_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	printf '%s\n' "ssh" >"$identity_path"
	chmod 600 "$identity_path"

	set +e
	(
		zxfer_get_effective_user_uid() {
			printf '%s\n' "111"
		}
		zxfer_get_path_owner_uid() {
			printf '%s\n' "222"
		}
		zxfer_get_path_mode_octal() {
			printf '%s\n' "600"
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_effective_user_uid() {
			printf '%s\n' "111"
		}
		zxfer_get_path_owner_uid() {
			printf '%s\n' "111"
		}
		zxfer_get_path_mode_octal() {
			printf '%s\n' "644"
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket identity reads should reject identity files owned by a different uid." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket identity reads should reject identity files with non-0600 permissions." \
		1 "$mode_status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_reports_failures_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	owner_status=$?

	rm -f "$identity_path"
	(
		zxfer_render_ssh_control_socket_entry_identity() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	render_status=$?

	rm -f "$identity_path"
	(
		mv() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	move_status=$?

	assertEquals "Shared ssh control socket identity writes should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket identity writes should fail when owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket identity writes should fail when the identity payload cannot be rendered." \
		1 "$render_status"
	assertEquals "Shared ssh control socket identity writes should fail when the identity file cannot be moved into place." \
		1 "$move_status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_rejects_mismatched_owner_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	printf '%s\n' "stale" >"$identity_path"
	chmod 600 "$identity_path"

	set +e
	zxfer_get_effective_user_uid() {
		printf '%s\n' "111"
	}
	zxfer_get_path_owner_uid() {
		printf '%s\n' "222"
	}
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket identity writes should reject existing identity files owned by a different uid." \
		1 "$status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_reports_render_failure_in_current_shell_direct() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	rm -f "$identity_path"

	set +e
	zxfer_render_ssh_control_socket_entry_identity() {
		return 4
	}
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket identity writes should fail when the identity renderer fails in the current shell." \
		1 "$status"
	assertFalse "Failed identity writes should not leave a partial installed identity file behind." \
		"[ -f \"$identity_path\" ]"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_reports_mktemp_failure_in_current_shell_direct() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	rm -f "$identity_path"

	set +e
	mktemp() {
		return 4
	}
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	status=$?
	unset -f mktemp
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket identity writes should fail when temporary-file creation fails in the current shell." \
		1 "$status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_rejects_symlink_and_non_regular_targets_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"
	other_path="$TEST_TMPDIR/other-identity"

	rm -f "$identity_path"
	: >"$other_path"
	ln -s "$other_path" "$identity_path"

	set +e
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	symlink_status=$?

	rm -f "$identity_path"
	mkdir "$identity_path"
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	directory_status=$?

	assertEquals "Shared ssh control socket identity writes should reject symlinked identity targets." \
		1 "$symlink_status"
	assertEquals "Shared ssh control socket identity writes should reject non-regular existing identity targets." \
		1 "$directory_status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_cleans_up_staged_path_when_redirection_fails() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"
	staged_dir="$TEST_TMPDIR/staged-identity-dir"
	staged_path="$TEST_TMPDIR/staged-identity-path"

	rm -f "$identity_path"
	mkdir "$staged_dir"
	ln -s "$staged_dir" "$staged_path"

	set +e
	(
		zxfer_stage_runtime_artifact_file_for_path() {
			g_zxfer_runtime_artifact_path_result=$staged_path
			printf '%s\n' "$staged_path"
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null
	)
	status=$?

	assertEquals "Shared ssh control socket identity writes should fail closed when the staged identity file cannot be written." \
		1 "$status"
	assertFalse "Failed shared ssh control socket identity writes should clean up the staged path." \
		"[ -e \"$staged_path\" ] || [ -L \"$staged_path\" ] || [ -h \"$staged_path\" ]"
	assertFalse "Failed shared ssh control socket identity writes should not publish an installed identity file." \
		"[ -f \"$identity_path\" ]"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_leases_lookup_failures_in_current_shell_direct() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	effective_uid=$(zxfer_get_effective_user_uid)
	entry_mode=$(zxfer_get_path_mode_octal "$entry_dir")
	leases_dir="$entry_dir/leases"

	set +e
	zxfer_get_path_owner_uid() {
		case "$1" in
		"$entry_dir")
			printf '%s\n' "$effective_uid"
			;;
		"$leases_dir")
			return 1
			;;
		esac
	}
	zxfer_get_path_mode_octal() {
		printf '%s\n' "$entry_mode"
	}
	zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null 2>&1
	leases_owner_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	leases_uid_should_fail=0
	zxfer_get_path_owner_uid() {
		case "$1" in
		"$entry_dir" | "$leases_dir")
			if [ "$1" = "$leases_dir" ]; then
				leases_uid_should_fail=1
			fi
			printf '%s\n' "$effective_uid"
			;;
		esac
	}
	zxfer_get_effective_user_uid() {
		if [ "${leases_uid_should_fail:-0}" -eq 1 ]; then
			return 1
		fi
		printf '%s\n' "$effective_uid"
	}
	zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null 2>&1
	leases_uid_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"

	zxfer_get_path_mode_octal() {
		case "$1" in
		"$entry_dir")
			printf '%s\n' "$entry_mode"
			;;
		"$leases_dir")
			return 1
			;;
		esac
	}
	zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null 2>&1
	leases_mode_status=$?
	zxfer_source_runtime_modules_through "zxfer_backup_metadata.sh"
	set -e

	assertEquals "Shared ssh control socket entry reuse should fail when the leases-dir owner lookup fails in the current shell." \
		1 "$leases_owner_status"
	assertEquals "Shared ssh control socket entry reuse should fail when the leases-dir effective uid lookup fails in the current shell." \
		1 "$leases_uid_status"
	assertEquals "Shared ssh control socket entry reuse should fail when the leases-dir mode lookup fails in the current shell." \
		1 "$leases_mode_status"
}

test_zxfer_acquire_ssh_control_socket_lock_returns_failure_when_lock_is_symlinked() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	ln -s "$TEST_TMPDIR/other-lock" "$entry_dir.lock"

	set +e
	output=$(zxfer_acquire_ssh_control_socket_lock "$entry_dir")
	status=$?

	assertEquals "Shared ssh control socket lock acquisition should reject symlinked lock dirs." 1 "$status"
	assertEquals "Rejected shared ssh control socket lock acquisition should not produce a payload." "" "$output"
}

test_zxfer_acquire_ssh_control_socket_lock_returns_failure_after_retries() {
	log="$TEST_TMPDIR/ssh_lock_retry.log"
	entry_dir="$TEST_TMPDIR/ssh_lock_retry_entry"
	mkdir -p "$entry_dir"

	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			sleep() {
				printf 'retry\n' >>"$log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket lock acquisition should fail after repeated contention." 1 "$status"
	assertEquals "Failed shared ssh control socket lock acquisition should not produce a payload." "" "$output"
	assertEquals "Shared ssh control socket lock acquisition should retry before failing." \
		"9" "$(wc -l <"$log" | tr -d ' ')"
}

test_zxfer_acquire_ssh_control_socket_lock_uses_fast_retry_before_sleep() {
	attempt_file="$TEST_TMPDIR/ssh_lock_fast_retry.attempts"
	sleep_log="$TEST_TMPDIR/ssh_lock_fast_retry.sleep"
	entry_dir="$TEST_TMPDIR/ssh_lock_fast_retry_entry"
	mkdir -p "$entry_dir"
	printf '%s\n' 0 >"$attempt_file"

	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_ssh_control_socket_lock_wait_count=0
			mkdir() {
				lock_attempts=$(cat "$attempt_file")
				lock_attempts=$((lock_attempts + 1))
				printf '%s\n' "$lock_attempts" >"$attempt_file"
				if [ "$lock_attempts" -lt 4 ]; then
					return 1
				fi
				command mkdir "$@"
			}
			sleep() {
				printf '%s\n' "slept" >"$sleep_log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
			printf 'wait_count=%s\n' "${g_zxfer_profile_ssh_control_socket_lock_wait_count:-0}"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket lock acquisition should succeed when a sibling releases the lock during the fast-retry window." \
		0 "$status"
	assertContains "Fast-retry ssh control socket lock acquisition should still return the created lock directory." \
		"$output" "$entry_dir.lock"
	assertContains "Fast-retry ssh control socket lock acquisition should record that contention was observed." \
		"$output" "wait_count=1"
	assertEquals "Fast-retry ssh control socket lock acquisition should not fall through to whole-second sleeps." \
		"" "$(cat "$sleep_log" 2>/dev/null)"
}

test_zxfer_acquire_ssh_control_socket_lock_reaps_dead_pid_lock_without_sleeping() {
	sleep_log="$TEST_TMPDIR/ssh_lock_dead_pid.sleep"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lock_dir="$entry_dir.lock"
	create_stale_owned_lock_fixture \
		"$lock_dir" lock "$(zxfer_get_ssh_control_socket_lock_purpose)" "999999999"

	output=$(
		(
			sleep() {
				printf '%s\n' "slept" >"$sleep_log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "Dead ssh control socket locks should be reaped and reacquired immediately." \
		0 "$status"
	assertEquals "Dead ssh control socket lock reaping should still return the lock directory." \
		"$lock_dir" "$output"
	assertEquals "Dead ssh control socket lock reaping should not wait once the owner PID is gone." \
		"" "$(cat "$sleep_log" 2>/dev/null)"
	assertTrue "Reacquired ssh control socket locks should install fresh ownership metadata." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_acquire_ssh_control_socket_lock_reaps_pidless_lock_after_timeout() {
	sleep_log="$TEST_TMPDIR/ssh_lock_pidless.sleep"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lock_dir="$entry_dir.lock"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	output=$(
		(
			ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=0
			sleep() {
				printf 'retry\n' >>"$sleep_log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "Pidless ssh control socket locks left behind by older runs should be reaped after the bounded wait." \
		0 "$status"
	assertEquals "Pidless ssh control socket lock reaping should still return the lock directory." \
		"$lock_dir" "$output"
	assertEquals "Pidless ssh control socket lock reaping should only happen after the existing retry window." \
		"9" "$(wc -l <"$sleep_log" | tr -d ' ')"
	assertTrue "Pidless ssh control socket lock reaping should install fresh ownership metadata after reacquiring the lock." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_acquire_ssh_control_socket_lock_fails_closed_for_missing_metadata_0755_lock() {
	sleep_log="$TEST_TMPDIR/ssh_lock_missing_metadata_0755.sleep"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lock_dir="$entry_dir.lock"
	mkdir "$lock_dir"
	chmod 755 "$lock_dir"

	output=$(
		(
			ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=0
			sleep() {
				printf 'retry\n' >>"$sleep_log"
			}
			set +e
			zxfer_acquire_ssh_control_socket_lock "$entry_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "Missing-metadata ssh control socket lock dirs with unsupported permissions should now fail closed instead of being treated as a supported upgrade path." \
		"$output" "status=1"
	assertContains "Missing-metadata ssh control socket lock failures should now surface the specific reap failure directly instead of timing out generically." \
		"$output" "error=Unable to reap stale or corrupt ssh control socket lock path \"$lock_dir\"."
	assertEquals "Missing-metadata ssh control socket lock failures should not burn the retry budget once the unsupported permissions are detected." \
		"" "$(cat "$sleep_log" 2>/dev/null)"
	assertFalse "Missing-metadata ssh control socket lock failures should not create replacement metadata." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_acquire_ssh_control_socket_lock_rejects_unsupported_pid_file_lock() {
	sleep_log="$TEST_TMPDIR/ssh_lock_unsupported_pid.sleep"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lock_dir="$entry_dir.lock"
	write_unsupported_pid_lock_fixture "$lock_dir" "$$" 700

	output=$(
		(
			set +e
			zxfer_acquire_ssh_control_socket_lock "$entry_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'error=%s\n' "$g_zxfer_ssh_control_socket_lock_error"
		)
	)

	assertContains "Unsupported pid-file ssh control socket lock dirs should now fail closed instead of being reclaimed." \
		"$output" "status=1"
	assertContains "Unsupported pid-file ssh control socket lock failures should surface the unsupported-layout diagnostic." \
		"$output" "error=ssh control socket lock path \"$lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
	assertEquals "Unsupported pid-file ssh control socket lock failures should not spend the normal retry budget once the unsupported layout is detected." \
		"" "$(cat "$sleep_log" 2>/dev/null)"
	assertTrue "Unsupported pid-file ssh control socket lock failures should keep the unsupported directory in place for explicit operator cleanup." \
		"[ -d '$lock_dir' ]"
	assertTrue "Unsupported pid-file ssh control socket lock failures should preserve the old pid file." \
		"[ -f '$lock_dir/pid' ]"
	assertFalse "Unsupported pid-file ssh control socket lock failures should not create replacement metadata ownership." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_acquire_ssh_control_socket_lock_reaps_corrupt_metadata_lock_after_timeout() {
	sleep_log="$TEST_TMPDIR/ssh_lock_corrupt_metadata.sleep"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lock_dir="$entry_dir.lock"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "not-lock-metadata" >"$lock_dir/metadata"
	chmod 600 "$lock_dir/metadata"

	output=$(
		(
			ZXFER_SSH_CONTROL_SOCKET_LOCK_FAST_RETRIES=0
			sleep() {
				printf 'retry\n' >>"$sleep_log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "Corrupt ssh control socket lock metadata should still be reaped after the bounded wait." \
		0 "$status"
	assertEquals "Corrupt ssh control socket lock reaping should still return the lock directory." \
		"$lock_dir" "$output"
	assertEquals "Corrupt ssh control socket lock reaping should preserve the existing wait window before reaping." \
		"9" "$(wc -l <"$sleep_log" | tr -d ' ')"
	assertTrue "Corrupt ssh control socket lock reaping should install fresh ownership metadata after reacquiring the lock." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_release_ssh_control_socket_lock_returns_failure_for_invalid_targets() {
	lock_file="$TEST_TMPDIR/ssh_lock_file"
	lock_link="$TEST_TMPDIR/ssh_lock_link"
	printf '%s\n' "not-a-dir" >"$lock_file"
	ln -s "$lock_file" "$lock_link"

	set +e
	zxfer_release_ssh_control_socket_lock "$lock_file" >/dev/null
	file_status=$?
	zxfer_release_ssh_control_socket_lock "$lock_link" >/dev/null
	link_status=$?

	assertEquals "ssh control socket lock release should fail for non-directory targets." \
		1 "$file_status"
	assertEquals "ssh control socket lock release should fail for symlink targets." \
		1 "$link_status"
}

test_zxfer_prune_stale_ssh_control_socket_leases_preserves_corrupt_entries_and_reaps_dead_entries() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	invalid_lease="$entry_dir/leases/lease.invalid"
	dead_lease="$entry_dir/leases/lease.dead"
	live_lease=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	mkdir "$invalid_lease"
	chmod 700 "$invalid_lease"
	create_stale_owned_lock_fixture \
		"$dead_lease" lease "$(zxfer_get_ssh_control_socket_lease_purpose)" "999999999"

	zxfer_prune_stale_ssh_control_socket_leases "$entry_dir"

	assertTrue "Corrupt ssh control socket lease entries should remain in place until an operator can inspect them." \
		"[ -e '$invalid_lease' ]"
	assertFalse "Dead ssh control socket leases should be pruned." \
		"[ -e '$dead_lease' ]"
	assertTrue "Live ssh control socket leases should be preserved." \
		"[ -e '$live_lease' ]"
}

test_zxfer_prune_stale_ssh_control_socket_leases_rejects_plain_file_entries() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "plain-file-prune.example")
	plain_lease="$entry_dir/leases/lease.legacy"
	: >"$plain_lease"

	zxfer_reset_ssh_control_socket_lock_state
	set +e
	zxfer_prune_stale_ssh_control_socket_leases "$entry_dir" >/dev/null
	status=$?

	assertEquals "Plain-file ssh control socket lease entries should now fail closed instead of being interpreted as live or stale owners." \
		1 "$status"
	assertContains "Plain-file ssh control socket lease entries should surface the unsupported-format diagnostic." \
		"$g_zxfer_ssh_control_socket_lock_error" \
		"ssh control socket lease entry \"$plain_lease\" is not a metadata-bearing directory. Remove the stale entry and retry."
	assertTrue "Plain-file ssh control socket lease entries should be left in place for explicit operator cleanup." \
		"[ -e '$plain_lease' ]"
}

test_zxfer_prune_stale_ssh_control_socket_leases_rejects_broken_symlink_entries() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "prune-broken.example")
	broken_lease="$entry_dir/leases/lease.broken"
	ln -s "$TEST_TMPDIR/missing-lease-target" "$broken_lease"

	zxfer_reset_ssh_control_socket_lock_state
	set +e
	zxfer_prune_stale_ssh_control_socket_leases "$entry_dir" >/dev/null
	status=$?

	assertEquals "Broken symlink ssh control socket lease entries should fail prune inspection closed." \
		1 "$status"
	assertContains "Broken symlink ssh control socket lease entries should surface the symlink diagnostic." \
		"$g_zxfer_ssh_control_socket_lock_error" \
		"Refusing symlinked ssh control socket lease entry \"$broken_lease\"."
}

test_zxfer_count_ssh_control_socket_leases_handles_empty_and_nonempty_dirs() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	output_file="$TEST_TMPDIR/ssh_control_socket_lease_count_live.out"

	set +e
	zxfer_count_ssh_control_socket_leases "$entry_dir" >"$output_file"
	empty_status=$?
	assertEquals "Empty shared ssh control socket lease dirs should return success." \
		0 "$empty_status"
	assertEquals "Empty shared ssh control socket lease dirs should count as zero leases." \
		"0" "$(cat "$output_file")"

	zxfer_create_ssh_control_socket_lease_file "$entry_dir" >/dev/null
	zxfer_create_ssh_control_socket_lease_file "$entry_dir" >/dev/null

	set +e
	zxfer_count_ssh_control_socket_leases "$entry_dir" >"$output_file"
	live_status=$?
	assertEquals "Shared ssh control socket lease counting should succeed for live lease directories." \
		0 "$live_status"
	assertEquals "Shared ssh control socket lease counting should reflect the current number of live lease directories." \
		"2" "$(cat "$output_file")"
}

test_zxfer_count_ssh_control_socket_leases_rejects_plain_file_entries() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "plain-file-count.example")
	plain_lease="$entry_dir/leases/lease.legacy"
	output_file="$TEST_TMPDIR/ssh_control_socket_plain_file_count.out"
	: >"$plain_lease"

	zxfer_reset_ssh_control_socket_lock_state
	set +e
	zxfer_count_ssh_control_socket_leases "$entry_dir" >"$output_file"
	status=$?

	assertEquals "Plain-file ssh control socket lease counting should now fail closed instead of treating old files as live owners." \
		1 "$status"
	assertEquals "Failed ssh control socket lease counts should not print a lease total." \
		"" "$(cat "$output_file")"
	assertContains "Plain-file ssh control socket lease counting should surface the unsupported-format diagnostic." \
		"$g_zxfer_ssh_control_socket_lock_error" \
		"ssh control socket lease entry \"$plain_lease\" is not a metadata-bearing directory. Remove the stale entry and retry."
	assertTrue "Plain-file ssh control socket lease counting should leave the unsupported entry in place for explicit operator cleanup." \
		"[ -e '$plain_lease' ]"
}

test_zxfer_count_ssh_control_socket_leases_counts_corrupt_entries_as_busy() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	corrupt_lease="$entry_dir/leases/lease.corrupt"
	output_file="$TEST_TMPDIR/ssh_control_socket_lease_count_corrupt.out"
	mkdir "$corrupt_lease"
	chmod 700 "$corrupt_lease"
	zxfer_create_ssh_control_socket_lease_file "$entry_dir" >/dev/null

	set +e
	zxfer_count_ssh_control_socket_leases "$entry_dir" >"$output_file"
	status=$?
	assertEquals "Corrupt ssh control socket lease entries should still return success so callers can treat them as busy." \
		0 "$status"
	assertEquals "Corrupt ssh control socket lease entries should still count as busy so shared sockets are not closed unsafely." \
		"2" "$(cat "$output_file")"
}

test_zxfer_count_ssh_control_socket_leases_rejects_broken_symlink_entries() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "count-broken.example")
	broken_lease="$entry_dir/leases/lease.broken"
	output_file="$TEST_TMPDIR/ssh_control_socket_lease_count_broken.out"
	ln -s "$TEST_TMPDIR/missing-lease-target" "$broken_lease"

	zxfer_reset_ssh_control_socket_lock_state
	set +e
	zxfer_count_ssh_control_socket_leases "$entry_dir" >"$output_file"
	status=$?

	assertEquals "Broken symlink ssh control socket lease entries should fail counting closed." \
		1 "$status"
	assertEquals "Broken symlink ssh control socket lease count failures should not print a lease count." \
		"" "$(cat "$output_file")"
	assertContains "Broken symlink ssh control socket lease count failures should surface the symlink diagnostic." \
		"$g_zxfer_ssh_control_socket_lock_error" \
		"Refusing symlinked ssh control socket lease entry \"$broken_lease\"."
}

test_zxfer_count_ssh_control_socket_leases_returns_zero_for_missing_dir_in_current_shell() {
	output_file="$TEST_TMPDIR/ssh_control_socket_lease_count.out"

	zxfer_count_ssh_control_socket_leases "$TEST_TMPDIR/missing-entry" >"$output_file"

	assertEquals "Missing shared ssh control socket lease dirs should count as zero leases." \
		"0" "$(cat "$output_file")"
}

test_zxfer_ensure_remote_host_capabilities_prefers_memory_cache() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity)
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)
	g_origin_remote_capabilities_bootstrap_source="cache"
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_EXIT_STATUS

	result=$(zxfer_ensure_remote_host_capabilities "origin.example" source)

	unset FAKE_SSH_EXIT_STATUS

	assertContains "In-memory capability cache hits should satisfy lookups without ssh." \
		"$result" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "In-memory capability cache hits should preserve the original bootstrap source." \
		"cache" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_store_cached_remote_capability_response_for_host_resets_bootstrap_source_when_host_changes() {
	g_option_O_origin_host="origin.example"
	g_origin_remote_capabilities_host="old-origin.example"
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"old-origin.example")
	g_origin_remote_capabilities_bootstrap_source="memory"

	zxfer_store_cached_remote_capability_response_for_host \
		"origin.example" "$(fake_remote_capability_response)"

	assertEquals "Capability bootstrap tracking should reset when the cached origin slot is reused for a different host, even when the cache identity matches." \
		"" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_store_cached_remote_capability_response_for_host_resets_target_bootstrap_source_when_requested_tool_identity_changes() {
	g_option_T_target_host="target.example"
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"target.example" "zfs")
	g_target_remote_capabilities_bootstrap_source="memory"
	g_target_remote_capabilities_cache_write_unavailable=1

	zxfer_store_cached_remote_capability_response_for_host \
		"target.example" "$(fake_remote_capability_response)" "parallel"

	assertEquals "Capability bootstrap tracking should reset when the target-side cached identity changes for the same host." \
		"" "$g_target_remote_capabilities_bootstrap_source"
	assertEquals "Capability cache-write unavailability should reset when the target-side cached identity changes for the same host." \
		0 "${g_target_remote_capabilities_cache_write_unavailable:-0}"
	assertEquals "Target-side cached identity tracking should refresh to the new requested-tool scope." \
		"$(zxfer_render_remote_capability_cache_identity_for_host "target.example" "parallel")" \
		"$g_target_remote_capabilities_cache_identity"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_preserves_first_source() {
	g_option_O_origin_host="origin.example"

	zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" live
	zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" cache

	assertEquals "Bootstrap source tracking should preserve the first remote discovery source for the origin host." \
		"live" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_sets_origin_source_from_cached_slot() {
	g_origin_remote_capabilities_host="cached-origin.example"
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"cached-origin.example")

	zxfer_note_remote_capability_bootstrap_source_for_host "cached-origin.example" cache

	assertEquals "Bootstrap source tracking should also match the cached origin slot when no active origin host is configured." \
		"cache" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_sets_target_source_in_current_shell() {
	g_option_T_target_host="target.example"

	zxfer_note_remote_capability_bootstrap_source_for_host "target.example" live
	zxfer_note_remote_capability_bootstrap_source_for_host "target.example" cache

	assertEquals "Bootstrap source tracking should preserve the first remote discovery source for the target host." \
		"live" "$g_target_remote_capabilities_bootstrap_source"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_sets_target_source_from_cached_slot() {
	g_target_remote_capabilities_host="cached-target.example"
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"cached-target.example")

	zxfer_note_remote_capability_bootstrap_source_for_host "cached-target.example" live

	assertEquals "Bootstrap source tracking should also match the cached target slot when no active target host is configured." \
		"live" "$g_target_remote_capabilities_bootstrap_source"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_ignores_mismatched_requested_tool_identity_in_current_shell() {
	g_option_O_origin_host="origin.example"
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "zfs")
	g_option_T_target_host="target.example"
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"target.example" "zfs")

	zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" live "parallel"
	zxfer_note_remote_capability_bootstrap_source_for_host "target.example" cache "parallel"

	assertEquals "Bootstrap-source tracking should ignore origin-side updates when the requested-tool identity does not match the cached slot." \
		"" "${g_origin_remote_capabilities_bootstrap_source:-}"
	assertEquals "Bootstrap-source tracking should ignore target-side updates when the requested-tool identity does not match the cached slot." \
		"" "${g_target_remote_capabilities_bootstrap_source:-}"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_ignores_identity_refresh_failures() {
	output=$(
		(
			set +e
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_render_remote_capability_cache_identity_for_host() {
				return 1
			}
			zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" live
			printf 'origin_status=%s\n' "$?"
			zxfer_note_remote_capability_bootstrap_source_for_host "target.example" cache
			printf 'target_status=%s\n' "$?"
			printf 'origin=<%s>\n' "${g_origin_remote_capabilities_bootstrap_source:-}"
			printf 'target=<%s>\n' "${g_target_remote_capabilities_bootstrap_source:-}"
		)
	)

	assertContains "Bootstrap-source tracking should treat capability-cache identity refresh failures as a no-op for origin hosts." \
		"$output" "origin_status=0"
	assertContains "Bootstrap-source tracking should treat capability-cache identity refresh failures as a no-op for target hosts." \
		"$output" "target_status=0"
	assertContains "Bootstrap-source tracking should not publish an origin bootstrap source when identity refresh fails." \
		"$output" "origin=<>"
	assertContains "Bootstrap-source tracking should not publish a target bootstrap source when identity refresh fails." \
		"$output" "target=<>"
}

test_zxfer_ensure_remote_host_capabilities_marks_cache_backed_bootstrap_source() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	result_file="$TEST_TMPDIR/remote_caps_cache_backed.out"
	g_option_O_origin_host="origin.example"
	write_remote_capability_cache_fixture "$cache_path"

	zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
	result=$(cat "$result_file")

	assertContains "Cache-backed capability lookups should still return the cached payload." \
		"$result" "tool	zfs	0	/remote/bin/zfs"
	assertEquals "Cache-backed capability lookups should record that startup was satisfied from cache." \
		"cache" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_profile_record_remote_capability_bootstrap_source_increments_matching_counter() {
	g_option_V_very_verbose=1
	g_zxfer_profile_remote_capability_bootstrap_live=0
	g_zxfer_profile_remote_capability_bootstrap_cache=0
	g_zxfer_profile_remote_capability_bootstrap_memory=0

	zxfer_profile_record_remote_capability_bootstrap_source live
	zxfer_profile_record_remote_capability_bootstrap_source cache
	zxfer_profile_record_remote_capability_bootstrap_source memory
	zxfer_profile_record_remote_capability_bootstrap_source unknown

	assertEquals "Bootstrap-source profiling should count live remote capability fetches." \
		"1" "${g_zxfer_profile_remote_capability_bootstrap_live:-0}"
	assertEquals "Bootstrap-source profiling should count on-disk capability-cache hits." \
		"1" "${g_zxfer_profile_remote_capability_bootstrap_cache:-0}"
	assertEquals "Bootstrap-source profiling should count in-memory capability-cache hits." \
		"1" "${g_zxfer_profile_remote_capability_bootstrap_memory:-0}"
}

test_zxfer_fetch_remote_host_capabilities_live_refreshes_secure_path_from_environment() {
	log_file="$TEST_TMPDIR/remote_caps_live_env.log"
	output=$(
		(
			g_zxfer_dependency_path="/stale/secure/path"
			ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "$2" >"$log_file"
				fake_remote_capability_response
			}
			zxfer_fetch_remote_host_capabilities_live "origin.example" source
		)
	)

	assertContains "Live remote capability probes should still return the parsed capability payload." \
		"$output" "tool	cat	0	/remote/bin/cat"
	assertContains "Live remote capability probes should refresh the secure PATH from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$(cat "$log_file")" "/fresh/secure/path:/usr/bin"
	assertNotContains "Live remote capability probes should not keep probing with a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$(cat "$log_file")" "/stale/secure/path"
}

test_zxfer_fetch_remote_host_capabilities_live_handles_csh_remote_shell() {
	l_csh_shell=$(find_csh_shell_for_tests)
	if [ "$l_csh_shell" = "" ]; then
		return 0
	fi

	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_caps_csh_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_caps_csh_exec.log"
	secure_bin_dir="$TEST_TMPDIR/remote_caps_csh_secure_bin"
	stdout_file="$TEST_TMPDIR/remote_caps_csh.out"
	stderr_file="$TEST_TMPDIR/remote_caps_csh.err"
	mkdir -p "$secure_bin_dir"
	create_fake_ssh_join_csh_exec_bin "$realistic_ssh_bin" "$l_csh_shell"
	cat >"$secure_bin_dir/uname" <<'EOF'
#!/bin/sh
printf '%s\n' "RemoteOS"
EOF
	chmod +x "$secure_bin_dir/uname"
	cat >"$secure_bin_dir/zfs" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$secure_bin_dir/zfs"

	g_cmd_ssh="$realistic_ssh_bin"
	g_option_O_origin_host="backup@example.com"
	ZXFER_SECURE_PATH="$secure_bin_dir"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	zxfer_fetch_remote_host_capabilities_live "backup@example.com" source "zfs" \
		>"$stdout_file" 2>"$stderr_file"
	status=$?

	unset FAKE_SSH_LOG

	assertEquals "Live remote capability probes should succeed when the remote login shell is csh/tcsh." \
		0 "$status"
	assertEquals "Live remote capability probes should not emit unmatched-quote syntax errors through csh/tcsh." \
		"" "$(cat "$stderr_file")"
	assertContains "Live remote capability probes should still advertise the negotiated V2 payload." \
		"$(cat "$stdout_file")" "ZXFER_REMOTE_CAPS_V2"
	assertContains "Live remote capability probes should preserve the remote operating-system record through csh/tcsh." \
		"$(cat "$stdout_file")" "os	RemoteOS"
	assertContains "Live remote capability probes should preserve the requested remote zfs helper through csh/tcsh." \
		"$(cat "$stdout_file")" "tool	zfs	0	$secure_bin_dir/zfs"
	assertNotContains "The csh/tcsh-backed ssh emulation should not receive a multiline here-doc payload." \
		"$(cat "$realistic_ssh_log")" "ZXFER_REMOTE_CAPABILITY_TOOLS"
}

test_zxfer_try_acquire_remote_capability_cache_lock_creates_secure_lock_and_metadata_file() {
	output_file="$TEST_TMPDIR/remote_capability_lock_create.out"

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" >"$output_file"
	status=$?
	lock_dir=$(cat "$output_file")

	assertEquals "Capability cache lock acquisition should succeed for a new lock path." \
		0 "$status"
	assertNotEquals "Capability cache lock acquisition should print the acquired lock path." \
		"" "$lock_dir"
	assertTrue "Capability cache lock acquisition should create the lock directory." \
		"[ -d '$lock_dir' ]"
	if [ -d "$lock_dir" ]; then
		metadata_path="$lock_dir/metadata"
		assertEquals "Capability cache lock directories should be owner-only." \
			"700" "$(zxfer_get_path_mode_octal "$lock_dir")"
		assertTrue "Capability cache lock acquisition should create an ownership metadata file." \
			"[ -f '$metadata_path' ]"
		if [ -f "$metadata_path" ]; then
			assertEquals "Capability cache lock metadata files should be owner-only." \
				"600" "$(zxfer_get_path_mode_octal "$metadata_path")"
			assertContains "Capability cache lock metadata should record the common lock header." \
				"$(cat "$metadata_path")" "$ZXFER_LOCK_METADATA_HEADER"
		fi
	fi

	zxfer_release_remote_capability_cache_lock "$lock_dir"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_busy_for_live_owner() {
	create_output="$TEST_TMPDIR/remote_capability_lock_busy_create.out"
	busy_output="$TEST_TMPDIR/remote_capability_lock_busy_second.out"

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" >"$create_output"
	create_status=$?
	lock_dir=$(cat "$create_output")

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" >"$busy_output"
	status=$?

	assertEquals "Initial capability cache lock acquisition should succeed for the busy-owner test." \
		0 "$create_status"
	assertNotEquals "Initial capability cache lock acquisition should publish the acquired lock path for the busy-owner test." \
		"" "$lock_dir"
	assertEquals "A live sibling capability cache lock should report the lock as busy." \
		2 "$status"
	assertEquals "Busy capability cache lock acquisitions should not print a path." \
		"" "$(cat "$busy_output")"

	zxfer_release_remote_capability_cache_lock "$lock_dir"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reaps_stale_lock() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	create_stale_owned_lock_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" "999999999"
	output_file="$TEST_TMPDIR/remote_capability_lock_reap.out"

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" >"$output_file"
	status=$?
	result=$(cat "$output_file")

	assertEquals "Stale capability cache locks should be reaped and reacquired successfully." \
		0 "$status"
	assertNotEquals "Stale capability cache locks should publish the reacquired lock path." \
		"" "$result"
	assertEquals "Stale capability cache locks should be reaped and reacquired." \
		"$lock_dir" "$result"

	zxfer_release_remote_capability_cache_lock "$lock_dir"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_busy_for_missing_metadata_lock_dir() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	output_file="$TEST_TMPDIR/remote_capability_lock_missing_metadata_busy.out"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" >"$output_file"
	status=$?

	assertEquals "Missing-metadata capability cache lock dirs should stay busy until the caller's bounded wait or pidless reap path runs." \
		2 "$status"
	assertEquals "Busy missing-metadata capability cache lock acquisitions should not print a path." \
		"" "$(cat "$output_file")"
	assertTrue "Busy missing-metadata capability cache lock acquisitions should leave the existing directory in place." \
		"[ -d '$lock_dir' ]"
	assertFalse "Busy missing-metadata capability cache lock acquisitions should not synthesize metadata ownership." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_failure_for_unsupported_pid_file_lock() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	write_unsupported_pid_lock_fixture "$lock_dir" "$$"
	output_file="$TEST_TMPDIR/remote_capability_lock_pid_file_busy.out"
	err_file="$TEST_TMPDIR/remote_capability_lock_pid_file_busy.err"

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" \
		>"$output_file" 2>"$err_file"
	status=$?

	assertEquals "Unsupported pid-file capability cache lock dirs should now fail closed instead of being treated as reclaimable busy state." \
		1 "$status"
	assertEquals "Unsupported pid-file capability cache lock acquisition failures should not print a path." \
		"" "$(cat "$output_file")"
	assertContains "Unsupported pid-file capability cache lock acquisition failures should emit a specific operator diagnostic." \
		"$(cat "$err_file")" \
		"Error: remote capability cache lock path \"$lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
	assertTrue "Unsupported pid-file capability cache lock acquisition failures should keep the old directory in place." \
		"[ -d '$lock_dir' ]"
	assertTrue "Unsupported pid-file capability cache lock acquisition failures should preserve the old pid file for explicit operator cleanup." \
		"[ -f '$lock_dir/pid' ]"
	assertFalse "Unsupported pid-file capability cache lock acquisition failures should not be replaced with metadata ownership." \
		"[ -f '$lock_dir/metadata' ]"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_rejects_unsupported_pid_file_lock() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	write_unsupported_pid_lock_fixture "$lock_dir" "$$"
	err_file="$TEST_TMPDIR/remote_capability_lock_pid_file_reap.err"

	set +e
	zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example" \
		2>"$err_file"
	status=$?

	assertEquals "Pidless capability-cache cleanup should fail closed for unsupported pid-file lock directories instead of removing them." \
		1 "$status"
	assertContains "Pidless capability-cache cleanup failures should emit a specific unsupported-layout diagnostic." \
		"$(cat "$err_file")" \
		"Error: remote capability cache lock path \"$lock_dir\" uses an unsupported pid-file layout. Remove the stale lock directory and retry."
	assertTrue "Pidless capability-cache cleanup failures should preserve unsupported pid-file lock directories for explicit operator cleanup." \
		"[ -d '$lock_dir' ]"
	assertTrue "Pidless capability-cache cleanup failures should preserve the old pid file." \
		"[ -f '$lock_dir/pid' ]"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_reports_lookup_and_reap_failures() {
	lock_dir="$TEST_TMPDIR/remote_caps_pidless_reap_failure.lock"
	mkdir "$lock_dir" || fail "Unable to create remote capability pidless reap fixture."
	chmod 700 "$lock_dir" || fail "Unable to chmod remote capability pidless reap fixture."

	path_output=$(
		(
			set +e
			zxfer_remote_capability_cache_lock_path() {
				return 1
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	reap_output=$(
		(
			set +e
			zxfer_remote_capability_cache_lock_path() {
				printf '%s\n' "$lock_dir"
			}
			zxfer_try_reap_stale_owned_lock_dir() {
				return 1
			}
			zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Pidless capability-cache cleanup should fail closed when lock-path lookup fails." \
		"$path_output" "status=1"
	assertContains "Pidless capability-cache cleanup should fail closed when stale-lock cleanup fails." \
		"$reap_output" "status=1"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_failure_for_insecure_metadata_file() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"
	chmod 644 "$lock_dir/metadata"
	output_file="$TEST_TMPDIR/remote_capability_lock_insecure.out"

	set +e
	zxfer_try_acquire_remote_capability_cache_lock "origin.example" >"$output_file"
	status=$?

	assertEquals "Malformed or insecure capability cache lock metadata files should fail closed." \
		1 "$status"
	assertEquals "Failed capability cache lock acquisitions should not print a path." \
		"" "$(cat "$output_file")"
}

test_zxfer_wait_for_remote_capability_cache_fill_retries_until_cache_is_populated() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_wait.attempts"
	printf '%s\n' 0 >"$read_attempt_file"

	set +e
	output=$(
		(
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
				:
			}
			zxfer_wait_for_remote_capability_cache_fill "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache waits should retry until a sibling populates the cache." \
		0 "$status"
	assertContains "Capability cache waits should return the populated cached payload." \
		"$output" "tool	zfs	0	/remote/bin/zfs"
}

test_zxfer_wait_for_remote_capability_cache_fill_uses_fast_retry_before_sleep() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_fast_wait.attempts"
	sleep_log="$TEST_TMPDIR/remote_caps_fast_wait.sleep"
	printf '%s\n' 0 >"$read_attempt_file"

	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_remote_capability_cache_wait_count=0
			g_zxfer_remote_capability_cache_wait_retries=2
			zxfer_read_remote_capability_cache_file() {
				read_attempts=$(cat "$read_attempt_file")
				read_attempts=$((read_attempts + 1))
				printf '%s\n' "$read_attempts" >"$read_attempt_file"
				if [ "$read_attempts" -eq 4 ]; then
					fake_remote_capability_response
					return 0
				fi
				return 1
			}
			sleep() {
				printf '%s\n' "slept" >"$sleep_log"
			}
			zxfer_wait_for_remote_capability_cache_fill "origin.example"
			printf 'wait_count=%s\n' "${g_zxfer_profile_remote_capability_cache_wait_count:-0}"
		)
	)
	status=$?

	assertEquals "Capability cache waits should succeed when a sibling publishes the cache during the fast-retry window." \
		0 "$status"
	assertContains "Fast-retry capability waits should still return the cached capability payload." \
		"$output" "tool	zfs	0	/remote/bin/zfs"
	assertContains "Fast-retry capability waits should record that the caller observed cache contention." \
		"$output" "wait_count=1"
	assertEquals "Fast-retry capability waits should avoid whole-second sleeps when the cache becomes available quickly." \
		"" "$(cat "$sleep_log" 2>/dev/null)"
}

test_zxfer_wait_for_remote_capability_cache_fill_invalid_retry_inputs_fall_back_to_defaults() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_invalid_wait.attempts"
	sleep_log="$TEST_TMPDIR/remote_caps_invalid_wait.sleep"
	printf '%s\n' 0 >"$read_attempt_file"

	output=$(
		(
			g_zxfer_remote_capability_cache_wait_retries=0
			ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES=bogus
			export ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES
			zxfer_read_remote_capability_cache_file() {
				read_attempts=$(cat "$read_attempt_file")
				read_attempts=$((read_attempts + 1))
				printf '%s\n' "$read_attempts" >"$read_attempt_file"
				if [ "$read_attempts" -eq 5 ]; then
					fake_remote_capability_response
					return 0
				fi
				return 1
			}
			sleep() {
				printf '%s\n' "slept" >>"$sleep_log"
			}
			zxfer_wait_for_remote_capability_cache_fill "origin.example"
			printf 'attempts=%s\n' "$(cat "$read_attempt_file")"
			printf 'sleeps=%s\n' "$(wc -l <"$sleep_log" | tr -d ' ')"
		)
	)
	status=$?

	assertEquals "Capability cache waits should fall back to the default retry budget when the configured wait retries are non-positive." \
		0 "$status"
	assertContains "Capability cache waits should still return the cached payload after falling back to the default retry budget." \
		"$output" "tool	zfs	0	/remote/bin/zfs"
	assertContains "Capability cache waits should probe five times when the wait-retry budget falls back to the default." \
		"$output" "attempts=5"
	assertContains "Capability cache waits should skip fast retries and sleep between default retry attempts when the fast-retry setting is invalid." \
		"$output" "sleeps=4"
}

test_zxfer_wait_for_remote_capability_cache_fill_succeeds_on_fast_retry_in_current_shell() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_fast_wait_current_shell.attempts"
	sleep_log="$TEST_TMPDIR/remote_caps_fast_wait_current_shell.sleep"
	result_file="$TEST_TMPDIR/remote_caps_fast_wait_current_shell.out"
	printf '%s\n' 0 >"$read_attempt_file"

	g_option_V_very_verbose=1
	g_zxfer_profile_remote_capability_cache_wait_count=0
	g_zxfer_remote_capability_cache_wait_retries=2
	ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES=3
	export ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES
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

	zxfer_wait_for_remote_capability_cache_fill "origin.example" >"$result_file"
	status=$?
	unset ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Direct current-shell capability cache waits should succeed when the cache appears during the fast-retry window." \
		0 "$status"
	assertEquals "Direct current-shell fast-retry waits should avoid whole-second sleeps." \
		"" "$(cat "$sleep_log" 2>/dev/null)"
	assertEquals "Direct current-shell fast-retry waits should record two cache read attempts before success." \
		"2" "$(cat "$read_attempt_file")"
	assertContains "Direct current-shell fast-retry waits should write the cached payload to stdout." \
		"$(cat "$result_file")" "tool	zfs	0	/remote/bin/zfs"
	assertEquals "Direct current-shell fast-retry waits should still record observed contention once." \
		"1" "${g_zxfer_profile_remote_capability_cache_wait_count:-0}"
}

test_zxfer_wait_for_remote_capability_cache_fill_succeeds_after_sleep_in_current_shell() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_slow_wait_current_shell.attempts"
	sleep_log="$TEST_TMPDIR/remote_caps_slow_wait_current_shell.sleep"
	result_file="$TEST_TMPDIR/remote_caps_slow_wait_current_shell.out"
	printf '%s\n' 0 >"$read_attempt_file"

	g_option_V_very_verbose=1
	g_zxfer_profile_remote_capability_cache_wait_count=0
	g_zxfer_remote_capability_cache_wait_retries=3
	ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES=0
	export ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES
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

	zxfer_wait_for_remote_capability_cache_fill "origin.example" >"$result_file"
	status=$?
	unset ZXFER_REMOTE_CAPABILITY_CACHE_WAIT_FAST_RETRIES
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Direct current-shell capability cache waits should succeed when the cache appears after a timed retry." \
		0 "$status"
	assertEquals "Direct current-shell timed waits should perform exactly one whole-second sleep before success." \
		"1" "$(wc -l <"$sleep_log" | tr -d ' ')"
	assertEquals "Direct current-shell timed waits should record two cache read attempts before success." \
		"2" "$(cat "$read_attempt_file")"
	assertContains "Direct current-shell timed waits should write the cached payload to stdout." \
		"$(cat "$result_file")" "tool	zfs	0	/remote/bin/zfs"
	assertEquals "Direct current-shell timed waits should still record observed contention once." \
		"1" "${g_zxfer_profile_remote_capability_cache_wait_count:-0}"
}

test_zxfer_ensure_remote_host_capabilities_waits_for_sibling_cache_fill() {
	result_file="$TEST_TMPDIR/remote_caps_wait.out"
	bootstrap_file="$TEST_TMPDIR/remote_caps_wait.bootstrap"
	scratch_file="$TEST_TMPDIR/remote_caps_wait.scratch"
	live_marker="$TEST_TMPDIR/remote_caps_wait.live"

	set +e
	(
		g_option_O_origin_host="origin.example"
		zxfer_get_cached_remote_capability_response_for_host() {
			return 4
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			return 2
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			fake_remote_capability_response
		}
		zxfer_fetch_remote_host_capabilities_live() {
			printf '%s\n' "live-fetch" >"$live_marker"
			return 1
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
		printf '%s\n' "$g_origin_remote_capabilities_bootstrap_source" >"$bootstrap_file"
		printf '%s\n' "$g_zxfer_remote_capability_response_result" >"$scratch_file"
	)
	status=$?

	assertEquals "Sibling capability cache locks should be satisfied by the populated cache." \
		0 "$status"
	assertContains "Capability lookups satisfied by a sibling should return the cached payload." \
		"$(cat "$result_file")" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "Sibling-populated capability cache hits should be marked as cache-backed startup." \
		"cache" "$(cat "$bootstrap_file")"
	assertContains "Sibling-populated capability cache hits should also seed the current-shell capability scratch payload." \
		"$(cat "$scratch_file")" "$(printf 'tool\tparallel\t0\t/opt/bin/parallel')"
	assertFalse "Sibling-populated capability cache hits should not fall back to a live ssh probe." \
		"[ -e '$live_marker' ]"
}

test_zxfer_ensure_remote_capability_cache_dir_reports_post_create_lookup_failures_in_current_shell() {
	owner_tmp="$TEST_TMPDIR/remote_caps_ownerfail"
	mode_tmp="$TEST_TMPDIR/remote_caps_modefail"
	mkdir -p "$owner_tmp" "$mode_tmp"

	set +e
	(
		TMPDIR="$owner_tmp"
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_remote_capability_cache_dir >/dev/null
	)
	owner_status=$?

	(
		TMPDIR="$mode_tmp"
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_remote_capability_cache_dir >/dev/null
	)
	mode_status=$?

	assertEquals "Remote capability cache dir setup should fail when post-create owner lookup fails." \
		1 "$owner_status"
	assertEquals "Remote capability cache dir setup should fail when post-create mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_remote_capability_cache_lock_path_returns_failure_when_cache_path_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_remote_capability_cache_path() {
				return 1
			}
			zxfer_remote_capability_cache_lock_path "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache lock-path lookups should fail cleanly when cache-path resolution fails." \
		1 "$status"
	assertEquals "Capability cache lock-path lookup failures should not print a path." \
		"" "$output"
}

test_zxfer_remote_capability_cache_lock_helpers_report_lookup_and_timeout_failures() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_lookup"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	)
	validate_uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	)
	validate_owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	)
	validate_mode_status=$?

	(
		g_zxfer_remote_capability_cache_wait_retries="not-a-number"
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		sleep() {
			:
		}
		zxfer_wait_for_remote_capability_cache_fill "origin.example" >/dev/null
	)
	wait_status=$?

	assertEquals "Capability cache lock-dir validation should fail when uid lookup fails." \
		1 "$validate_uid_status"
	assertEquals "Capability cache lock-dir validation should fail when owner lookup fails." \
		1 "$validate_owner_status"
	assertEquals "Capability cache lock-dir validation should fail when mode lookup fails." \
		1 "$validate_mode_status"
	assertEquals "Capability cache waits should fail after retry normalization when no sibling populates the cache." \
		1 "$wait_status"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_removes_valid_lock_dir() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example"
	status=$?

	assertEquals "Pidless capability cache locks should be reaped after a bounded wait." \
		0 "$status"
	assertFalse "Reaping a pidless capability cache lock should remove the stale lock directory." \
		"[ -e '$lock_dir' ]"
}

test_zxfer_create_remote_capability_cache_lock_dir_propagates_shared_lock_creation_failures() {
	validate_lock_dir="$TEST_TMPDIR/remote_caps_lock_validate_cleanup"

	set +e
	(
		zxfer_create_owned_lock_dir() {
			return 1
		}
		zxfer_create_remote_capability_cache_lock_dir "$validate_lock_dir"
	)
	validate_status=$?

	assertEquals "Capability cache lock-dir creation should fail when the shared metadata lock helper fails." \
		1 "$validate_status"
	assertFalse "Capability cache lock-dir creation should not leave a partially created directory when the shared helper fails." \
		"[ -e '$validate_lock_dir' ]"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reports_path_and_reap_failures() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_reap_failure"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" "999999999"

	set +e
	(
		zxfer_remote_capability_cache_lock_path() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	path_status=$?

	(
		zxfer_remote_capability_cache_lock_path() {
			printf '%s\n' "$lock_dir"
		}
		zxfer_try_reap_stale_owned_lock_dir() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	reap_status=$?

	assertEquals "Capability cache lock acquisition should fail when lock-path resolution fails." \
		1 "$path_status"
	assertEquals "Capability cache lock acquisition should fail closed when stale-lock cleanup fails." \
		1 "$reap_status"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_busy_for_existing_live_metadata_dir_in_current_shell() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_validate_failure"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)"

	set +e
	(
		zxfer_remote_capability_cache_lock_path() {
			printf '%s\n' "$lock_dir"
		}
		zxfer_create_remote_capability_cache_lock_dir() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	status=$?

	assertEquals "Capability cache lock acquisition should report busy when creation loses the race to an existing live metadata-backed owner." \
		2 "$status"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reports_post_reap_validation_failure_in_current_shell() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_post_reap_failure"
	write_owned_lock_metadata_fixture \
		"$lock_dir" lock "$(zxfer_get_remote_capability_cache_lock_purpose)" "999999999"

	set +e
	(
		create_attempts=0
		zxfer_remote_capability_cache_lock_path() {
			printf '%s\n' "$lock_dir"
		}
		zxfer_try_reap_stale_owned_lock_dir() {
			return 0
		}
		zxfer_create_remote_capability_cache_lock_dir() {
			create_attempts=$((create_attempts + 1))
			if [ "$create_attempts" -eq 2 ]; then
				rm -rf "$lock_dir"
				mkdir "$lock_dir"
				chmod 700 "$lock_dir"
			fi
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir() {
			[ "$create_attempts" -lt 2 ]
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	status=$?

	assertEquals "Capability cache lock acquisition should fail when a stale lock is reaped but the recreated directory cannot be revalidated." \
		1 "$status"
}

test_zxfer_release_remote_capability_cache_lock_returns_failure_for_invalid_targets() {
	lock_file="$TEST_TMPDIR/remote_caps_lock_file"
	lock_link="$TEST_TMPDIR/remote_caps_lock_link"
	printf '%s\n' "not-a-dir" >"$lock_file"
	ln -s "$lock_file" "$lock_link"

	set +e
	zxfer_release_remote_capability_cache_lock "$lock_file" >/dev/null
	file_status=$?
	zxfer_release_remote_capability_cache_lock "$lock_link" >/dev/null
	link_status=$?

	assertEquals "Capability cache lock release should fail for non-directory targets." \
		1 "$file_status"
	assertEquals "Capability cache lock release should fail for symlink targets." \
		1 "$link_status"
}

test_zxfer_ensure_remote_host_capabilities_falls_back_to_live_probe_after_wait_timeout() {
	result_file="$TEST_TMPDIR/remote_caps_live_fallback.out"
	bootstrap_file="$TEST_TMPDIR/remote_caps_live_fallback.bootstrap"
	lock_attempt_file="$TEST_TMPDIR/remote_caps_live_fallback.lock_attempts"
	printf '%s\n' 0 >"$lock_attempt_file"

	set +e
	(
		g_option_O_origin_host="origin.example"
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			lock_attempts=$(cat "$lock_attempt_file")
			lock_attempts=$((lock_attempts + 1))
			printf '%s\n' "$lock_attempts" >"$lock_attempt_file"
			return 2
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			return 1
		}
		zxfer_fetch_remote_host_capabilities_live() {
			g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
			printf '%s\n' "$g_zxfer_remote_capability_response_result"
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
		printf '%s\n' "$g_origin_remote_capabilities_bootstrap_source" >"$bootstrap_file"
	)
	status=$?

	assertEquals "Capability lookups should fall back to a live probe after a bounded sibling-cache wait timeout." \
		0 "$status"
	assertContains "Live fallback after a sibling-cache timeout should still return the capability payload." \
		"$(cat "$result_file")" "tool	zfs	0	/remote/bin/zfs"
	assertEquals "Live fallback after a sibling-cache timeout should mark startup as live." \
		"live" "$(cat "$bootstrap_file")"
}

test_zxfer_ensure_remote_host_capabilities_reaps_pidless_lock_after_wait_timeout() {
	result_file="$TEST_TMPDIR/remote_caps_pidless_reap.out"
	bootstrap_file="$TEST_TMPDIR/remote_caps_pidless_reap.bootstrap"
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	lock_attempt_file="$TEST_TMPDIR/remote_caps_pidless_reap.lock_attempts"
	printf '%s\n' 0 >"$lock_attempt_file"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	set +e
	(
		g_option_O_origin_host="origin.example"
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			lock_attempts=$(cat "$lock_attempt_file")
			lock_attempts=$((lock_attempts + 1))
			printf '%s\n' "$lock_attempts" >"$lock_attempt_file"
			if [ "$lock_attempts" -eq 1 ]; then
				return 2
			fi
			printf '%s\n' "$lock_dir"
			return 0
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			return 1
		}
		zxfer_fetch_remote_host_capabilities_live() {
			g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
			printf '%s\n' "$g_zxfer_remote_capability_response_result"
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
		printf '%s\n' "$g_origin_remote_capabilities_bootstrap_source" >"$bootstrap_file"
	)
	status=$?

	assertEquals "Capability lookups should reclaim stale pidless locks after the bounded wait and continue with a live probe." \
		0 "$status"
	assertContains "Pidless-lock recovery should still return the capability payload." \
		"$(cat "$result_file")" "tool	cat	0	/remote/bin/cat"
	assertEquals "Pidless-lock recovery should mark startup as live." \
		"live" "$(cat "$bootstrap_file")"
	assertFalse "Pidless-lock recovery should remove the stale lock directory before reacquiring." \
		"[ -e '$lock_dir' ]"
}

test_zxfer_ensure_remote_host_capabilities_returns_failure_when_second_lock_attempt_fails() {
	lock_attempt_file="$TEST_TMPDIR/remote_caps_second_lock_attempts"
	printf '%s\n' 0 >"$lock_attempt_file"

	set +e
	(
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			lock_attempts=$(cat "$lock_attempt_file")
			lock_attempts=$((lock_attempts + 1))
			printf '%s\n' "$lock_attempts" >"$lock_attempt_file"
			if [ "$lock_attempts" -eq 1 ]; then
				return 2
			fi
			return 1
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			return 1
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	)
	status=$?

	assertEquals "Capability lookups should fail closed when the second lock attempt reports a hard failure." \
		1 "$status"
}

test_zxfer_ensure_remote_host_capabilities_returns_failure_for_unexpected_lock_status() {
	set +e
	(
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			return 3
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	)
	status=$?

	assertEquals "Capability lookups should fail closed on unexpected lock statuses." \
		1 "$status"
}

test_zxfer_read_remote_capability_cache_file_rejects_expired_entries() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path" 1

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Expired remote capability cache files should be ignored." 1 "$status"
	assertEquals "Expired remote capability cache files should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_path_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_remote_capability_cache_path() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when cache-path lookup fails." 1 "$status"
	assertEquals "Capability cache reads with cache-path lookup failures should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_headerless_legacy_payloads() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Headerless legacy remote capability cache payloads should now be treated as cache misses." \
		1 "$status"
	assertEquals "Headerless legacy remote capability cache payloads should not produce a payload." \
		"" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_retired_v1_payloads() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture \
		"$cache_path" \
		"$(date '+%s')" \
		"ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Retired V1 remote capability cache payloads should now be treated as cache misses." \
		1 "$status"
	assertEquals "Retired V1 remote capability cache payloads should not produce a payload." \
		"" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_non_numeric_epoch() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path" "not-a-timestamp"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Non-numeric remote capability cache epochs should be rejected." 1 "$status"
	assertEquals "Non-numeric remote capability cache epochs should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_malformed_payload() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture \
		"$cache_path" \
		"$(date '+%s')" \
		"ZXFER_REMOTE_CAPS_V2
os	RemoteOS"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Malformed remote capability cache payloads should be rejected." 1 "$status"
	assertEquals "Malformed remote capability cache payloads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_identity_mismatches() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	wrong_identity_hex=$(zxfer_remote_capability_cache_identity_hex_for_host "other.example") ||
		fail "Unable to derive alternate remote capability cache identity."
	zxfer_write_cache_object_contents_to_path \
		"$cache_path" \
		"$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" \
		"created_epoch=$(date '+%s')
identity_hex=$wrong_identity_hex" \
		"$(fake_remote_capability_response)" >/dev/null ||
		fail "Unable to write identity-mismatched remote capability cache fixture."
	chmod 600 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?
	set -e

	assertEquals "Remote capability cache reads should reject cache objects whose embedded identity does not match the requested host." \
		1 "$status"
	assertEquals "Identity-mismatched remote capability cache files should not produce a payload." \
		"" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_insecure_permissions() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"
	chmod 644 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Insecurely permissioned remote capability cache payloads should be rejected." 1 "$status"
	assertEquals "Insecurely permissioned remote capability cache payloads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_non_regular_target() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	rm -f "$cache_path"
	mkfifo "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Non-regular remote capability cache targets should be rejected." 1 "$status"
	assertEquals "Non-regular remote capability cache targets should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_uid_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when uid lookup fails." 1 "$status"
	assertEquals "Uid-lookup failures during capability cache reads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_owner_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when owner lookup fails." 1 "$status"
	assertEquals "Owner-lookup failures during capability cache reads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_mode_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when mode lookup fails." 1 "$status"
	assertEquals "Mode-lookup failures during capability cache reads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_reports_direct_lookup_failures_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_read_remote_capability_cache_file "origin.example" >/dev/null
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_read_remote_capability_cache_file "origin.example" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_read_remote_capability_cache_file "origin.example" >/dev/null
	)
	mode_status=$?

	assertEquals "Remote capability cache reads should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Remote capability cache reads should fail when owner lookup fails." \
		1 "$owner_status"
	assertEquals "Remote capability cache reads should fail when mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_read_remote_capability_cache_file_rejects_missing_created_epoch_metadata_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	zxfer_write_cache_object_contents_to_path \
		"$cache_path" \
		"$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" \
		"scope=zfs" \
		"$(fake_remote_capability_response)" >/dev/null ||
		fail "Unable to write created_epoch-less remote capability cache fixture."
	chmod 600 "$cache_path" || fail "Unable to chmod created_epoch-less remote capability cache fixture."

	set +e
	zxfer_read_remote_capability_cache_file "origin.example" >/dev/null 2>&1
	status=$?

	assertEquals "Remote capability cache reads should reject cache objects that omit created_epoch metadata." \
		1 "$status"
}

test_zxfer_read_remote_capability_cache_file_rejects_owner_mismatches_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"

	set +e
	zxfer_get_effective_user_uid() {
		printf '%s\n' 111
	}
	zxfer_get_path_owner_uid() {
		printf '%s\n' 222
	}
	zxfer_read_remote_capability_cache_file "origin.example" >/dev/null 2>&1
	status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Remote capability cache reads should reject cache files owned by another uid." \
		1 "$status"
}

test_zxfer_read_remote_capability_cache_file_reports_path_and_metadata_lookup_failures_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"

	set +e
	zxfer_remote_capability_cache_path() {
		return 1
	}
	zxfer_read_remote_capability_cache_file "origin.example" >/dev/null 2>&1
	path_status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path"
	zxfer_get_cache_object_metadata_value() {
		return 1
	}
	zxfer_read_remote_capability_cache_file "origin.example" >/dev/null 2>&1
	metadata_status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Remote capability cache reads should fail when cache-path resolution fails." \
		1 "$path_status"
	assertEquals "Remote capability cache reads should fail when created_epoch metadata lookup fails." \
		1 "$metadata_status"
}

test_zxfer_read_remote_capability_cache_file_rejects_future_created_epoch_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	future_epoch=$(($(date '+%s') + 60))
	write_remote_capability_cache_fixture "$cache_path" "$future_epoch"

	set +e
	zxfer_read_remote_capability_cache_file "origin.example" >/dev/null 2>&1
	status=$?

	assertEquals "Remote capability cache reads should reject cache objects dated in the future." \
		1 "$status"
}

test_zxfer_write_remote_capability_cache_file_writes_timestamped_payload() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")

	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"

	assertTrue "Successful capability cache writes should create the cache file." \
		"[ -f '$cache_path' ]"
	assertEquals "Capability cache writes should emit the shared cache-object header." \
		"$ZXFER_CACHE_OBJECT_HEADER_LINE" "$(sed -n '1p' "$cache_path")"
	assertEquals "Capability cache writes should stamp the capability cache-object kind on line two." \
		"kind=$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" "$(sed -n '2p' "$cache_path")"
	case "$(sed -n '3p' "$cache_path")" in
	created_epoch=)
		fail "Capability cache writes should persist a numeric created_epoch metadata field."
		;;
	created_epoch=*[!0-9]*)
		fail "Capability cache writes should persist a numeric created_epoch metadata field."
		;;
	esac
	case "$(sed -n '4p' "$cache_path")" in
	identity_hex=)
		fail "Capability cache writes should persist a non-empty identity_hex metadata field."
		;;
	identity_hex=*[!0123456789abcdef]*)
		fail "Capability cache writes should persist identity_hex as lowercase hex."
		;;
	identity_hex=*) ;;
	*)
		fail "Capability cache writes should persist identity_hex metadata on line four."
		;;
	esac
	zxfer_read_cache_object_file "$cache_path" "$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" >/dev/null
	assertEquals "Capability cache writes should preserve the capability payload inside the cache object." \
		"$(fake_remote_capability_response)" "$g_zxfer_cache_object_payload_result"
}

test_zxfer_write_remote_capability_cache_file_rewrites_existing_secure_file() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture \
		"$cache_path" \
		1 \
		"ZXFER_REMOTE_CAPS_V2
os	StaleOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat"

	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"

	zxfer_read_cache_object_file "$cache_path" "$ZXFER_REMOTE_CAPABILITY_CACHE_OBJECT_KIND" >/dev/null
	assertEquals "Capability cache writes should replace existing secure cache contents." \
		"$(fake_remote_capability_response)" "$g_zxfer_cache_object_payload_result"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_existing_uid_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path" 1 "stale"

	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
		)
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when uid lookup fails for an existing target." 1 "$status"
	assertEquals "Uid-lookup failures during capability cache writes should not produce a payload." "" "$output"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_existing_owner_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path" 1 "stale"

	set +e
	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
		)
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when owner lookup fails for an existing target." 1 "$status"
	assertEquals "Owner-lookup failures during capability cache writes should not produce a payload." "" "$output"
}

test_zxfer_write_remote_capability_cache_file_rejects_symlink_target() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	rm -f "$cache_path"
	ln -s "$TEST_TMPDIR/somewhere-else" "$cache_path"

	set +e
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	status=$?

	assertEquals "Capability cache writes should fail closed when the target path is a symlink." 1 "$status"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_mktemp_fails() {
	mktemp() {
		return 4
	}

	set +e
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	status=$?
	unset -f mktemp

	assertEquals "Capability cache writes should preserve mktemp failure status." 4 "$status"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_path_resolution_fails() {
	set +e
	(
		zxfer_remote_capability_cache_path() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when cache-path resolution fails." 1 "$status"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_payload_write_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")

	set +e
	(
		zxfer_write_cache_object_contents_to_path() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when the cache payload cannot be written." 1 "$status"
	assertFalse "Failed capability cache writes should not leave a published partial cache object behind." \
		"[ -e '$cache_path' ]"
}

test_zxfer_write_remote_capability_cache_file_reports_existing_lookup_failures_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	: >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	owner_status=$?

	assertEquals "Remote capability cache writes should fail when effective uid lookup fails for an existing cache file." \
		1 "$uid_status"
	assertEquals "Remote capability cache writes should fail when owner lookup fails for an existing cache file." \
		1 "$owner_status"
}

test_zxfer_write_remote_capability_cache_file_rejects_existing_owner_mismatch_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	write_remote_capability_cache_fixture "$cache_path" 1 "stale"

	set +e
	zxfer_get_effective_user_uid() {
		printf '%s\n' 111
	}
	zxfer_get_path_owner_uid() {
		printf '%s\n' 222
	}
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)" >/dev/null 2>&1
	status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Remote capability cache writes should reject existing cache files owned by another uid." \
		1 "$status"
}

test_zxfer_write_remote_capability_cache_file_reports_path_and_existing_target_validation_failures_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")

	set +e
	zxfer_remote_capability_cache_path() {
		return 1
	}
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)" >/dev/null 2>&1
	path_status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	rm -f "$cache_path"
	mkdir -p "$cache_path" || fail "Unable to create directory-backed remote capability cache target."
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)" >/dev/null 2>&1
	target_status=$?

	assertEquals "Remote capability cache writes should fail when cache-path resolution fails." \
		1 "$path_status"
	assertEquals "Remote capability cache writes should reject non-regular existing cache targets." \
		1 "$target_status"
}

test_zxfer_remote_capability_cache_write_unavailability_helpers_are_identity_scoped_in_current_shell() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "zfs")
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"target.example" "parallel")

	zxfer_note_remote_capability_cache_write_unavailable_for_host "origin.example" "zfs"
	zxfer_note_remote_capability_cache_write_unavailable_for_host "target.example" "parallel"

	set +e
	zxfer_remote_capability_cache_write_is_unavailable_for_host "origin.example" "zfs"
	origin_match_status=$?
	zxfer_remote_capability_cache_write_is_unavailable_for_host "origin.example" "parallel"
	origin_mismatch_status=$?
	zxfer_remote_capability_cache_write_is_unavailable_for_host "target.example" "parallel"
	target_match_status=$?
	zxfer_remote_capability_cache_write_is_unavailable_for_host "target.example" "zfs"
	target_mismatch_status=$?

	assertEquals "Origin-side cache-write unavailability should match the recorded requested-tool identity." \
		0 "$origin_match_status"
	assertEquals "Origin-side cache-write unavailability should ignore other requested-tool identities." \
		1 "$origin_mismatch_status"
	assertEquals "Target-side cache-write unavailability should match the recorded requested-tool identity." \
		0 "$target_match_status"
	assertEquals "Target-side cache-write unavailability should ignore other requested-tool identities." \
		1 "$target_mismatch_status"
}

test_zxfer_remote_capability_cache_write_unavailability_helpers_ignore_identity_refresh_failures_in_current_shell() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_cache_identity="stale-origin-identity"
	g_origin_remote_capabilities_cache_write_unavailable=0

	set +e
	zxfer_render_remote_capability_cache_identity_for_host() {
		return 1
	}
	zxfer_note_remote_capability_cache_write_unavailable_for_host "origin.example" "zfs"
	note_status=$?
	note_flag=${g_origin_remote_capabilities_cache_write_unavailable:-0}
	zxfer_remote_capability_cache_write_is_unavailable_for_host "origin.example" "zfs"
	query_status=$?
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "Cache-write unavailability notes should degrade to a no-op when identity refresh fails." \
		0 "$note_status"
	assertEquals "Identity-refresh failures should not mark cache writes unavailable." \
		0 "$note_flag"
	assertEquals "Cache-write availability checks should fail closed when identity refresh fails." \
		1 "$query_status"
}

test_zxfer_preload_remote_host_capabilities_delegates_to_ensure() {
	log="$TEST_TMPDIR/preload_remote_caps.log"
	: >"$log"
	tools_file="$TEST_TMPDIR/preload_remote_caps.tools"
	g_option_O_origin_host="origin.example"
	g_option_j_jobs=4
	g_option_e_restore_property_mode=1
	g_option_z_compress=1
	g_cmd_compress="zstd -T0 -9"

	(
		zxfer_ensure_remote_host_capabilities() {
			printf 'ensure host=%s side=%s\n' \
				"$1" "${2:-}" >>"$log"
			printf '%s\n' "${3:-}" >"$tools_file"
		}
		zxfer_preload_remote_host_capabilities "origin.example" source
	)

	assertContains "Capability preloading should delegate to the shared ensure helper." \
		"$(cat "$log")" "host=origin.example side=source"
	assertContains "Capability preloading should warm zfs for remote origin discovery." \
		"$(cat "$tools_file")" "zfs"
	assertContains "Capability preloading should include parallel when -j requests origin-side source fan-out." \
		"$(cat "$tools_file")" "parallel"
	assertContains "Capability preloading should include origin-side property-restore helpers when requested." \
		"$(cat "$tools_file")" "cat"
	assertContains "Capability preloading should include origin-side compression helpers when remote metadata compression is active." \
		"$(cat "$tools_file")" "zstd"
}

test_zxfer_preload_remote_host_capabilities_suppresses_failures_without_verbose() {
	set +e
	output=$(
		(
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			zxfer_preload_remote_host_capabilities "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Quiet capability preloads should still return the shared ensure failure status." \
		1 "$status"
	assertEquals "Quiet capability preloads should suppress opportunistic preload diagnostics." \
		"" "$output"
}

test_zxfer_preload_remote_host_capabilities_surfaces_failures_in_verbose_mode() {
	set +e
	output=$(
		(
			g_option_v_verbose=1
			g_option_V_very_verbose=0
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			zxfer_preload_remote_host_capabilities "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Verbose capability preloads should still return the shared ensure failure status." \
		1 "$status"
	assertContains "Verbose capability preloads should surface opportunistic preload diagnostics." \
		"$output" "Host key verification failed."
}

test_zxfer_preload_remote_host_capabilities_falls_back_to_minimal_zfs_scope_when_host_scope_lookup_fails() {
	tools_file="$TEST_TMPDIR/preload_remote_caps_fallback.tools"

	(
		zxfer_get_remote_capability_requested_tools_for_host() {
			return 1
		}
		zxfer_ensure_remote_host_capabilities() {
			printf '%s\n' "${3:-}" >"$tools_file"
		}
		zxfer_preload_remote_host_capabilities "origin.example" source
	)

	assertEquals "Capability preloading should fall back to the minimum zfs scope when host-scoped helper discovery fails." \
		"zfs" "$(cat "$tools_file")"
}

test_zxfer_get_remote_host_operating_system_returns_failure_when_capabilities_are_unavailable() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_get_remote_host_operating_system_direct() {
				return 1
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should fail when both the capability handshake and direct fallback are unavailable." 1 "$status"
	assertEquals "Failed remote OS lookups should not print a payload." "" "$output"
}

test_zxfer_get_remote_host_operating_system_preserves_direct_probe_failure_when_capabilities_are_unavailable() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "uname probe failed"
				return 1
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should still fail when the direct fallback fails after the capability handshake is unavailable." 1 "$status"
	assertEquals "Remote OS lookups should preserve a non-empty direct-fallback failure message when the capability handshake is unavailable." \
		"uname probe failed" "$output"
}

test_zxfer_get_remote_host_operating_system_falls_back_to_direct_probe_when_capabilities_are_unavailable() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "FallbackOS"
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)

	assertEquals "Remote OS lookups should fall back to a direct uname probe when the capability handshake is unavailable." \
		"FallbackOS" "$output"
}

test_zxfer_get_remote_host_operating_system_requests_minimal_capabilities() {
	log="$TEST_TMPDIR/remote_os_scope.log"
	output=$(
		(
			LOG_PATH="$log"
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "${3:-}" >"$LOG_PATH"
				fake_remote_capability_response
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should still succeed through the capability handshake." \
		0 "$status"
	assertEquals "Remote OS lookups should return the capability payload OS." \
		"RemoteOS" "$output"
	assertContains "Remote OS lookups should request the minimum zfs capability scope." \
		"$(cat "$log")" "zfs"
	assertNotContains "Remote OS lookups should not preload parallel." \
		"$(cat "$log")" "parallel"
}

test_zxfer_get_remote_host_operating_system_reuses_active_host_capability_scope() {
	log="$TEST_TMPDIR/remote_os_active_scope.log"
	g_option_O_origin_host="origin.example"
	g_option_j_jobs=4
	g_option_z_compress=1
	g_cmd_compress="zstd -9"

	output=$(
		(
			LOG_PATH="$log"
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "${3:-}" >"$LOG_PATH"
				fake_remote_capability_response
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should still succeed through the full active-host capability handshake." \
		0 "$status"
	assertEquals "Remote OS lookups should return the capability payload OS." \
		"RemoteOS" "$output"
	assertContains "Active origin OS lookups should warm the zfs helper needed later in startup." \
		"$(cat "$log")" "zfs"
	assertContains "Active origin OS lookups should warm parallel when source fan-out is enabled." \
		"$(cat "$log")" "parallel"
	assertContains "Active origin OS lookups should warm the compression helper when metadata compression is enabled." \
		"$(cat "$log")" "zstd"
}

test_zxfer_get_remote_host_operating_system_falls_back_to_direct_probe_when_capability_payload_is_malformed() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2
tool	zfs	0	/remote/bin/zfs"
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "FallbackOS"
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)

	assertEquals "Remote OS lookups should fall back to a direct uname probe when the capability payload is malformed." \
		"FallbackOS" "$output"
}

test_zxfer_get_remote_host_operating_system_preserves_direct_probe_failure_when_capability_payload_is_malformed() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2
tool	zfs	0	/remote/bin/zfs"
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "fallback uname parse failed"
				return 1
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should fail when malformed capability payloads are followed by a failing direct probe." 1 "$status"
	assertEquals "Remote OS lookups should preserve a non-empty direct-fallback failure message after a malformed capability payload." \
		"fallback uname parse failed" "$output"
}

test_zxfer_get_remote_host_operating_system_falls_back_to_direct_probe_when_capability_payload_has_invalid_helper_path() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf 'ZXFER_REMOTE_CAPS_V2\n'
				printf 'os%sRemoteOS\n' "$tab"
				printf 'tool%szfs%s0%s/remote/bin/zfs%s\n' "$tab" "$tab" "$tab" "$cr"
				printf 'tool%sparallel%s1%s-\n' "$tab" "$tab" "$tab"
				printf 'tool%scat%s1%s-\n' "$tab" "$tab" "$tab"
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "FallbackOS"
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)

	assertEquals "Remote OS lookups should fall back to a direct uname probe when the capability payload includes an invalid helper path." \
		"FallbackOS" "$output"
}

test_zxfer_get_remote_host_operating_system_direct_returns_first_output_line() {
	output=$(
		(
			g_zxfer_dependency_path="/stale/secure/path"
			ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s|%s|%s\n' "$1" "$2" "${3:-}" >"$TEST_TMPDIR/remote_os_direct.log"
				printf '%s\n' "MockRemoteOS" "ignored-extra-line"
			}
			zxfer_get_remote_host_operating_system_direct "origin.example" source
		)
	)

	assertEquals "Direct remote OS lookups should return the first line of uname output." \
		"MockRemoteOS" "$output"
	assertContains "Direct remote OS lookups should target the requested host." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "origin.example|"
	assertContains "Direct remote OS lookups should scope the remote probe to the secure dependency path." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "PATH='"
	assertContains "Direct remote OS lookups should refresh the secure PATH from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "/fresh/secure/path:/usr/bin"
	assertNotContains "Direct remote OS lookups should not keep using a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "/stale/secure/path"
	assertContains "Direct remote OS lookups should run uname through the remote shell wrapper." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "uname 2>/dev/null"
}

test_zxfer_get_remote_host_operating_system_direct_rejects_empty_output() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				return 0
			}
			zxfer_get_remote_host_operating_system_direct "origin.example" source
		)
	)
	status=$?

	assertEquals "Direct remote OS lookups should fail when uname returns no output." 1 "$status"
	assertEquals "Failed direct remote OS lookups should not print a payload." "" "$output"
}

test_zxfer_get_remote_host_operating_system_direct_uses_local_capture_when_restore_cat_is_remote() {
	output=$(
		(
			g_cmd_cat="/remote/bin/cat"
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "MockRemoteOS"
			}
			zxfer_get_remote_host_operating_system_direct "origin.example" source
		)
	)
	status=$?

	assertEquals "Direct remote OS lookups should not depend on a restore-mode remote cat helper when reading local probe temp files." 0 "$status"
	assertEquals "Direct remote OS lookups should still return the remote uname output when g_cmd_cat points at a remote helper." \
		"MockRemoteOS" "$output"
}

test_zxfer_capture_remote_probe_output_rethrows_transport_setup_failures_without_leaking_temp_files() {
	l_probe_tmpdir="$TEST_TMPDIR/remote_probe_capture"
	rm -rf "$l_probe_tmpdir"

	set +e
	output=$(
		(
			g_zxfer_profile_ssh_shell_invocations=0
			g_zxfer_profile_source_ssh_shell_invocations=0
			g_zxfer_profile_destination_ssh_shell_invocations=0
			g_zxfer_profile_other_ssh_shell_invocations=0
			zxfer_profile_metrics_enabled() {
				return 0
			}
			zxfer_get_ssh_transport_tokens_for_host() {
				printf '%s\n' "Managed ssh policy invalid."
				return 1
			}
			zxfer_get_temp_file() {
				mkdir -p "$l_probe_tmpdir" || return 1
				: >"$l_probe_tmpdir/should-not-exist"
				printf '%s\n' "$l_probe_tmpdir/should-not-exist"
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
				printf 'source=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
				printf 'destination=%s\n' "${g_zxfer_profile_destination_ssh_shell_invocations:-0}"
				printf 'other=%s\n' "${g_zxfer_profile_other_ssh_shell_invocations:-0}"
				exit 1
			}
			zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote probe capture should fail closed when ssh transport setup fails before the probe runs." \
		1 "$status"
	assertContains "Remote probe capture should preserve the transport setup validation error." \
		"$output" "message=Managed ssh policy invalid."
	assertContains "Remote probe capture transport preflight failures should still count as one ssh invocation." \
		"$output" "ssh=1"
	assertContains "Remote probe capture transport preflight failures should be attributed to the requested source side." \
		"$output" "source=1"
	assertContains "Remote probe capture transport preflight failures should not increment destination counters." \
		"$output" "destination=0"
	assertContains "Remote probe capture transport preflight failures should not increment other-host counters." \
		"$output" "other=0"
	assertFalse "Remote probe capture should not allocate temp files once transport setup has already failed." \
		"[ -e '$l_probe_tmpdir' ]"
}

test_zxfer_capture_remote_probe_output_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_create_private_temp_dir() {
				return 1
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				exit 1
			}
			zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote probe capture should fail closed when capture staging cannot allocate a private temp directory." \
		1 "$status"
	assertContains "Remote probe capture should preserve the tempfile-allocation diagnostic instead of collapsing it into a generic probe error." \
		"$output" "message=Error creating temporary file."
}

test_zxfer_capture_remote_probe_output_reports_stderr_capture_failures() {
	l_probe_tmpdir="$TEST_TMPDIR/remote_probe_capture_readback"
	rm -rf "$l_probe_tmpdir"

	set +e
	output=$(
		(
			zxfer_create_private_temp_dir() {
				mkdir -p "$l_probe_tmpdir" || return 1
				g_zxfer_runtime_artifact_path_result=$l_probe_tmpdir
				printf '%s\n' "$l_probe_tmpdir"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "probe-stdout"
				printf '%s\n' "Permission denied (publickey)." >&2
				return 255
			}
			cat() {
				if [ "$1" = "$l_probe_tmpdir/stderr" ]; then
					printf '%s\n' "capture read failed" >&2
					return 9
				fi
				command cat "$@"
			}

			if zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source; then
				l_status=0
			else
				l_status=$?
			fi

			printf 'status=%s\n' "$l_status"
			printf 'capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'stdout=<%s>\n' "$g_zxfer_remote_probe_stdout"
			printf 'stderr=<%s>\n' "$g_zxfer_remote_probe_stderr"
		) 2>&1
	)
	status=$?

	assertEquals "Remote probe capture readback-failure tests should complete the subshell cleanly." \
		0 "$status"
	assertContains "Remote probe capture should fail closed when the staged stderr payload cannot be reloaded." \
		"$output" "status=9"
	assertContains "Remote probe capture should classify staged readback failures distinctly." \
		"$output" "capture_failed=1"
	assertContains "Remote probe capture should preserve the underlying staged-read diagnostic." \
		"$output" "capture read failed"
	assertContains "Remote probe capture should surface a specific staged stderr readback message." \
		"$output" "stderr=<Failed to read remote probe stderr capture from local staging.>"
	assertContains "Remote probe capture should discard partial stdout payloads once capture reload fails." \
		"$output" "stdout=<>"
	assertFalse "Remote probe capture should clean up the local capture directory after staged readback failures." \
		"[ -e '$l_probe_tmpdir' ]"
}

test_zxfer_load_remote_probe_capture_files_distinguishes_stdout_and_dual_read_failures() {
	stdout_failure_output=$(
		(
			set +e
			read_calls=0
			zxfer_read_remote_probe_capture_file() {
				read_calls=$((read_calls + 1))
				if [ "$read_calls" -eq 1 ]; then
					g_zxfer_remote_probe_capture_read_result=""
					return 41
				fi
				g_zxfer_remote_probe_capture_read_result="stderr payload"
				return 0
			}
			zxfer_load_remote_probe_capture_files "remote probe" "$TEST_TMPDIR/stdout" "$TEST_TMPDIR/stderr" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'stdout=<%s>\n' "${g_zxfer_remote_probe_stdout:-}"
			printf 'stderr=<%s>\n' "${g_zxfer_remote_probe_stderr:-}"
		)
	)
	both_failure_output=$(
		(
			set +e
			read_calls=0
			zxfer_read_remote_probe_capture_file() {
				read_calls=$((read_calls + 1))
				g_zxfer_remote_probe_capture_read_result=""
				if [ "$read_calls" -eq 1 ]; then
					return 52
				fi
				return 63
			}
			zxfer_load_remote_probe_capture_files "remote probe" "$TEST_TMPDIR/stdout" "$TEST_TMPDIR/stderr" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'stdout=<%s>\n' "${g_zxfer_remote_probe_stdout:-}"
			printf 'stderr=<%s>\n' "${g_zxfer_remote_probe_stderr:-}"
		)
	)

	assertContains "Remote probe capture reloads should preserve the stdout read failure status when the staged stdout payload cannot be reloaded." \
		"$stdout_failure_output" "status=41"
	assertContains "Remote probe capture reloads should classify stdout read failures distinctly." \
		"$stdout_failure_output" "failed=1"
	assertContains "Remote probe capture reloads should leave stdout empty when the staged stdout payload cannot be reloaded." \
		"$stdout_failure_output" "stdout=<>"
	assertContains "Remote probe capture reloads should surface a specific staged stdout readback message." \
		"$stdout_failure_output" "stderr=<Failed to read remote probe stdout capture from local staging.>"
	assertContains "Remote probe capture reloads should preserve the first readback failure status when both staged capture files fail to reload." \
		"$both_failure_output" "status=52"
	assertContains "Remote probe capture reloads should classify dual readback failures distinctly." \
		"$both_failure_output" "failed=1"
	assertContains "Remote probe capture reloads should leave stdout empty when both staged capture files fail to reload." \
		"$both_failure_output" "stdout=<>"
	assertContains "Remote probe capture reloads should surface a specific dual readback message when both staged capture files fail." \
		"$both_failure_output" "stderr=<Failed to read remote probe stdout and stderr capture from local staging.>"
}

test_zxfer_emit_remote_probe_failure_message_prefers_staged_stderr() {
	default_output=$(zxfer_emit_remote_probe_failure_message "default probe failure.")
	default_status=$?
	g_zxfer_remote_probe_stderr="staged probe failure"
	staged_output=$(zxfer_emit_remote_probe_failure_message "ignored default")
	staged_status=$?

	assertEquals "Remote probe failure message emission should print the default message when staged stderr is empty." \
		"default probe failure." "$default_output"
	assertEquals "Remote probe failure message emission should succeed when printing the default message." \
		0 "$default_status"
	assertEquals "Remote probe failure message emission should prefer staged stderr over the default message." \
		"staged probe failure" "$staged_output"
	assertEquals "Remote probe failure message emission should succeed when printing staged stderr." \
		0 "$staged_status"
}

test_zxfer_capture_remote_probe_output_emits_very_verbose_probe_prefix_before_capture_redirection() {
	set +e
	output=$(
		(
			g_option_V_very_verbose=1
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_O_origin_host="origin.example"
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "probe-stdout"
			}

			zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source >/dev/null
		) 2>&1
	)
	status=$?

	assertEquals "Very-verbose remote probe capture should still succeed when the mocked ssh probe returns stdout." \
		0 "$status"
	assertContains "Very-verbose remote probe capture should print the in-flight probe command before stdout/stderr redirection begins." \
		"$output" "Running remote probe [origin: origin.example]: 'sh' '-c' 'printf ok'"
}

test_zxfer_fetch_remote_host_capabilities_live_preserves_transport_diagnostic() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_fetch_remote_host_capabilities_live "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability handshakes should fail when ssh transport setup fails." 1 "$status"
	assertContains "Remote capability handshakes should preserve the underlying transport diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_ensure_remote_host_capabilities_preserves_live_probe_diagnostic() {
	set +e
	output=$(
		(
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				printf '%s\n' "$TEST_TMPDIR/remote_caps_live_failure.lock"
			}
			zxfer_release_remote_capability_cache_lock() {
				:
			}
			zxfer_fetch_remote_host_capabilities_live() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability ensure should fail when the live capability probe fails." 1 "$status"
	assertContains "Remote capability ensure should preserve the underlying live-probe transport diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_ensure_remote_host_capabilities_warns_when_cache_write_fails_after_live_probe() {
	set +e
	output=$(
		(
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				printf '%s\n' "$TEST_TMPDIR/remote_caps_cache_write_warn.lock"
			}
			zxfer_release_remote_capability_cache_lock() {
				return 0
			}
			zxfer_fetch_remote_host_capabilities_live() {
				g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
				return 0
			}
			zxfer_write_remote_capability_cache_file() {
				return 17
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability ensure should keep the live capability payload when only the local cache write fails." \
		0 "$status"
	assertContains "Remote capability ensure should warn when the local cache write fails after a live probe succeeds." \
		"$output" "Warning: Failed to write local remote capability cache for host origin.example (status 17); disabling further local cache writes for this host during this run."
	assertContains "Remote capability ensure should still publish the live capability payload when cache persistence fails." \
		"$output" "ZXFER_REMOTE_CAPS_V2"
}

test_zxfer_ensure_remote_host_capabilities_marks_cache_write_unavailable_for_rest_of_run() {
	set +e
	output=$(
		(
			write_calls=0
			fetch_calls=0
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				printf '%s\n' "$TEST_TMPDIR/remote_caps_cache_write_disable.lock"
			}
			zxfer_release_remote_capability_cache_lock() {
				return 0
			}
			zxfer_fetch_remote_host_capabilities_live() {
				fetch_calls=$((fetch_calls + 1))
				g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
				return 0
			}
			zxfer_write_remote_capability_cache_file() {
				write_calls=$((write_calls + 1))
				return 17
			}

			zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null || exit 1
			g_origin_remote_capabilities_response=""
			zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null || exit 1

			printf 'write_calls=%s\n' "$write_calls"
			printf 'fetch_calls=%s\n' "$fetch_calls"
			printf 'write_unavailable=%s\n' "${g_origin_remote_capabilities_cache_write_unavailable:-0}"
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability ensure should keep succeeding after local cache persistence is disabled for the run." \
		0 "$status"
	assertContains "Remote capability ensure should mark local cache persistence unavailable after the first write failure." \
		"$output" "write_unavailable=1"
	assertContains "Remote capability ensure should avoid retrying the failed local cache write later in the same run." \
		"$output" "write_calls=1"
	assertContains "Remote capability ensure should still allow later live probes after local cache persistence is disabled." \
		"$output" "fetch_calls=2"
}

test_zxfer_ensure_remote_host_capabilities_warns_when_lock_release_fails_after_live_probe() {
	set +e
	output=$(
		(
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_read_remote_capability_cache_file() {
				return 1
			}
			zxfer_try_acquire_remote_capability_cache_lock() {
				printf '%s\n' "$TEST_TMPDIR/remote_caps_release_warn.lock"
			}
			zxfer_release_remote_capability_cache_lock() {
				return 19
			}
			zxfer_fetch_remote_host_capabilities_live() {
				g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
				return 0
			}
			zxfer_write_remote_capability_cache_file() {
				return 0
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability ensure should fail closed when the local capability lock cannot be released after a successful live probe." \
		1 "$status"
	assertContains "Remote capability ensure should warn when the local capability lock cannot be released after a successful live probe." \
		"$output" "Warning: Failed to release local remote capability cache lock for host origin.example (status 19)."
	assertNotContains "Remote capability ensure should not publish the live capability payload when lock cleanup fails after success." \
		"$output" "ZXFER_REMOTE_CAPS_V2"
}

test_zxfer_get_remote_host_operating_system_direct_preserves_transport_diagnostic() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Permission denied (publickey)." >&2
				return 255
			}
			zxfer_get_remote_host_operating_system_direct "origin.example" source
		) 2>&1
	)
	status=$?

	assertEquals "Direct remote OS lookups should fail when ssh transport setup fails." 1 "$status"
	assertContains "Direct remote OS lookups should preserve the underlying transport diagnostic." \
		"$output" "Permission denied (publickey)."
}

test_zxfer_remote_capability_cache_path_returns_failure_when_key_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_remote_capability_cache_key() {
				return 1
			}
			zxfer_remote_capability_cache_path "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache path lookups should fail cleanly when key generation fails." 1 "$status"
	assertEquals "Capability cache path lookup failures should not produce a payload." "" "$output"
}

test_zxfer_remote_capability_cache_key_propagates_identity_render_failures() {
	output=$(
		(
			set +e
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "identity refresh failed"
				return 1
			}
			key=$(zxfer_remote_capability_cache_key "origin.example")
			printf 'status=%s\n' "$?"
			printf 'output=%s\n' "$key"
		)
	)

	assertContains "Capability cache key generation should fail closed when capability-cache identity rendering fails." \
		"$output" "status=1"
	assertContains "Capability cache key generation should preserve non-empty capability-cache identity diagnostics." \
		"$output" "output=identity refresh failed"
}

test_zxfer_remote_capability_cache_key_fails_when_hex_encoding_is_empty() {
	set +e
	output=$(
		(
			od() {
				:
			}
			zxfer_remote_capability_cache_key "origin.example" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)
	status=$?
	set -e

	assertEquals "Capability cache key empty-hex tests should complete the subshell cleanly." \
		0 "$status"
	assertContains "Capability cache key derivation should fail closed when exact identity hex encoding produces no output." \
		"$output" "status=1"
}

test_zxfer_remote_capability_cache_key_uses_bounded_hex_identity_in_current_shell() {
	output_file="$TEST_TMPDIR/remote_capability_cache_key.out"

	(
		od() {
			printf '%s\n' " 61 62 63 64"
		}
		zxfer_remote_capability_cache_key "origin.example" >"$output_file"
	)

	assertEquals "Capability cache keys should be derived from the rendered identity hex when od succeeds." \
		"h4.61626364" "$(cat "$output_file")"
}

test_zxfer_remote_capability_cache_key_uses_path_shadowed_hex_identity_in_current_shell() {
	fake_bin_dir="$TEST_TMPDIR/remote_capability_cache_key_bin"
	output_file="$TEST_TMPDIR/remote_capability_cache_key_shadowed.out"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/od" <<'EOF'
#!/bin/sh
printf '%s\n' " 61 62 63 64"
EOF
	chmod +x "$fake_bin_dir/od"

	PATH="$fake_bin_dir:$original_path"
	zxfer_remote_capability_cache_key "origin.example" >"$output_file"
	PATH=$original_path

	assertEquals "Capability cache keys should exercise the rendered identity hex in the current shell when od is shadowed through PATH." \
		"h4.61626364" "$(cat "$output_file")"
}

test_zxfer_remote_capability_cache_key_refreshes_secure_path_from_environment() {
	output_file="$TEST_TMPDIR/remote_capability_cache_key_env.out"
	input_file="$TEST_TMPDIR/remote_capability_cache_key_env.input"

	(
		od() {
			cat >"$input_file"
			printf '%s\n' " 61"
		}
		g_zxfer_dependency_path="/stale/secure/path"
		ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
		ZXFER_SSH_USE_AMBIENT_CONFIG=1
		zxfer_remote_capability_cache_key "origin.example" >"$output_file"
	)

	assertEquals "Capability cache keys should refresh from ZXFER_SECURE_PATH instead of a stale cached dependency path." \
		"h1.61" "$(cat "$output_file")"
	assertEquals "Capability cache keys should render the current secure dependency path, ssh transport policy, and requested tool set into the cache identity." \
		"$(printf '%s\n%s\n%s\n%s' "origin.example" "/fresh/secure/path:/usr/bin" "ambient" "zfs")" "$(cat "$input_file")"
}

test_zxfer_remote_capability_cache_key_distinguishes_known_legacy_cksum_collision_hosts() {
	key_one=$(
		(
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "fixed-cache-identity"
			}
			zxfer_remote_capability_cache_key "host-e00sy5"
		)
	)
	key_two=$(
		(
			zxfer_render_remote_capability_cache_identity_for_host() {
				printf '%s\n' "fixed-cache-identity"
			}
			zxfer_remote_capability_cache_key "host-entjr8"
		)
	)

	assertNotEquals "Capability cache keys should not collapse known legacy cksum-collision host identities." \
		"$key_one" "$key_two"
}

test_zxfer_remote_capability_cache_key_tracks_ssh_transport_policy() {
	default_key=$(zxfer_remote_capability_cache_key "origin.example")
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ambient_key=$(zxfer_remote_capability_cache_key "origin.example")
	unset ZXFER_SSH_USE_AMBIENT_CONFIG
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"
	pinned_key=$(zxfer_remote_capability_cache_key "origin.example")

	assertNotEquals "Capability cache keys should change when zxfer falls back to ambient ssh policy." \
		"$default_key" "$ambient_key"
	assertNotEquals "Capability cache keys should change when the pinned known-hosts file changes." \
		"$default_key" "$pinned_key"
}

test_resolve_remote_required_tool_falls_back_to_direct_probe_when_capability_handshake_fails() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/bin/zfs"
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Remote helper lookups should fall back to the direct secure probe when the capability handshake fails." 0 "$status"
	assertEquals "Capability-handshake fallback should return the direct probe result." \
		"/remote/bin/zfs" "$output"
}

test_resolve_remote_required_tool_falls_back_to_direct_probe_for_malformed_handshake_payload() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
				printf '%s\n' "os	RemoteOS"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/bin/zfs"
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Malformed handshake payloads should also fall back to the direct secure probe." 0 "$status"
	assertEquals "Malformed-handshake fallback should return the direct probe result." \
		"/remote/bin/zfs" "$output"
}

test_resolve_remote_required_tool_falls_back_to_direct_probe_for_handshake_payload_with_invalid_helper_path() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf 'ZXFER_REMOTE_CAPS_V2\n'
				printf 'os%sRemoteOS\n' "$tab"
				printf 'tool%szfs%s0%s/remote/bin/zfs%s\n' "$tab" "$tab" "$tab" "$cr"
				printf 'tool%sparallel%s1%s-\n' "$tab" "$tab" "$tab"
				printf 'tool%scat%s1%s-\n' "$tab" "$tab" "$tab"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/direct/zfs"
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Invalid helper paths inside capability payloads should trigger the secure direct-probe fallback." \
		0 "$status"
	assertEquals "Invalid-helper-path fallback should return the direct probe result." \
		"/remote/direct/zfs" "$output"
}

test_resolve_remote_required_tool_propagates_direct_probe_failure_when_capability_handshake_fails() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Remote helper lookups should still fail when both the handshake and direct secure probe fail." 1 "$status"
	assertContains "Capability-handshake fallback failures should preserve the direct probe message." \
		"$output" "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin)."
}

test_resolve_remote_required_tool_propagates_direct_probe_failure_for_malformed_handshake_payload() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
				printf '%s\n' "os\tRemoteOS"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Malformed remote capability payloads should still fail when the direct secure probe also fails." 1 "$status"
	assertContains "Malformed-payload fallback failures should preserve the direct probe message." \
		"$output" "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin)."
}

test_resolve_remote_required_tool_propagates_direct_probe_failure_when_requested_tool_is_absent_from_capabilities() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	cat	0	/remote/bin/cat
EOF
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "Required dependency \"parallel\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_resolve_remote_required_tool "origin.example" parallel "parallel" source
		)
	)
	status=$?

	assertEquals "Remote required-tool resolution should still fail when the capability payload omits the helper and the direct probe also fails." 1 "$status"
	assertContains "Missing-helpers in the capability payload should preserve the direct-probe failure message when fallback probing also fails." \
		"$output" "Required dependency \"parallel\" not found on host origin.example in secure PATH (/secure/bin)."
}

test_resolve_remote_required_tool_requests_scoped_capabilities_for_parallel() {
	log="$TEST_TMPDIR/resolve_remote_parallel_scope.log"
	output=$(
		(
			LOG_PATH="$log"
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "${3:-}" >"$LOG_PATH"
				fake_remote_capability_response
			}
			zxfer_resolve_remote_required_tool "origin.example" parallel "parallel" source
		)
	)
	status=$?

	assertEquals "Remote parallel resolution should still succeed through the capability handshake." \
		0 "$status"
	assertEquals "Remote parallel resolution should return the parsed helper path." \
		"/opt/bin/parallel" "$output"
	assertContains "Remote parallel resolution should request a scoped capability payload that includes zfs." \
		"$(cat "$log")" "zfs"
	assertContains "Remote parallel resolution should request parallel on demand." \
		"$(cat "$log")" "parallel"
	assertNotContains "Remote parallel resolution should not preload unrelated helpers." \
		"$(cat "$log")" "cat"
}

test_resolve_remote_required_tool_prefers_prewarmed_host_scope_for_parallel() {
	log="$TEST_TMPDIR/resolve_remote_parallel_host_scope.log"
	output=$(
		(
			LOG_PATH="$log"
			g_option_O_origin_host="origin.example"
			g_option_j_jobs=4
			g_option_e_restore_property_mode=1
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "${3:-}" >"$LOG_PATH"
				fake_remote_capability_response
			}
			zxfer_resolve_remote_required_tool "origin.example" parallel "parallel" source
		)
	)
	status=$?

	assertEquals "Remote parallel resolution should still succeed when the broader host scope is reused." \
		0 "$status"
	assertEquals "Remote parallel resolution should still return the parsed helper path from the broader host scope." \
		"/opt/bin/parallel" "$output"
	assertContains "Remote parallel resolution should reuse the host-scoped preload identity when it already includes zfs." \
		"$(cat "$log")" "zfs"
	assertContains "Remote parallel resolution should reuse the broader host-scoped preload identity for cat when restore-property mode is active." \
		"$(cat "$log")" "cat"
	assertContains "Remote parallel resolution should reuse the broader host-scoped preload identity for compression helpers when -z is active." \
		"$(cat "$log")" "zstd"
}

test_zxfer_get_remote_capability_requested_tools_for_resolved_tool_prefers_host_scope_when_it_includes_the_requested_helper() {
	output=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_j_jobs=4
			g_option_e_restore_property_mode=1
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			zxfer_get_remote_capability_requested_tools_for_resolved_tool "origin.example" parallel
		)
	)
	status=$?

	assertEquals "Resolved-tool capability requests should succeed when the host-scoped preload already includes the helper." \
		0 "$status"
	assertContains "Resolved-tool capability requests should preserve the host-scoped zfs helper." \
		"$output" "zfs"
	assertContains "Resolved-tool capability requests should preserve the requested helper from the host-scoped preload." \
		"$output" "parallel"
	assertContains "Resolved-tool capability requests should preserve related host-scoped helpers such as cat." \
		"$output" "cat"
	assertContains "Resolved-tool capability requests should preserve host-scoped compression helpers when enabled." \
		"$output" "zstd"
}

test_zxfer_get_remote_capability_requested_tools_for_resolved_tool_falls_back_to_tool_scope_when_host_scope_is_unavailable() {
	output=$(
		(
			zxfer_get_remote_capability_requested_tools_for_host() {
				return 1
			}
			zxfer_get_remote_capability_requested_tools_for_resolved_tool "origin.example" zstd
		)
	)
	status=$?

	assertEquals "Resolved-tool capability requests should still succeed when they fall back to the tool-scoped identity." \
		0 "$status"
	assertContains "Resolved-tool capability requests should include zfs in the fallback tool scope." \
		"$output" "zfs"
	assertContains "Resolved-tool capability requests should include the requested helper in the fallback tool scope." \
		"$output" "zstd"
	assertNotContains "Resolved-tool capability requests should not inject unrelated helpers when they fall back to the tool-scoped identity." \
		"$output" "parallel"
}

test_zxfer_get_remote_capability_requested_tools_for_resolved_tool_falls_back_to_tool_scope_when_host_scope_omits_helper() {
	output=$(
		(
			zxfer_get_remote_capability_requested_tools_for_host() {
				printf '%s\n' "zfs
cat"
			}
			zxfer_get_remote_capability_requested_tools_for_resolved_tool "origin.example" parallel
		)
	)
	status=$?

	assertEquals "Resolved-tool capability requests should still succeed when the broader host scope does not include the helper being resolved." \
		0 "$status"
	assertContains "Resolved-tool capability requests should keep zfs in the fallback tool scope." \
		"$output" "zfs"
	assertContains "Resolved-tool capability requests should add the requested helper when the broader host scope omits it." \
		"$output" "parallel"
	assertNotContains "Resolved-tool capability requests should not keep unrelated host-only helpers when they fall back to the helper-specific scope." \
		"$output" "cat"
}

test_zxfer_resolve_remote_cli_tool_direct_preserves_transport_diagnostic() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_resolve_remote_cli_tool_direct "origin.example" zfs "zfs" source
		) 2>&1
	)
	status=$?

	assertEquals "Direct remote helper probes should fail when ssh transport setup fails." 1 "$status"
	assertContains "Direct remote helper probes should preserve the underlying transport diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_resolve_remote_cli_tool_direct_ignores_stdout_only_probe_noise() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "wrapper startup noise"
				return 255
			}
			zxfer_resolve_remote_cli_tool_direct "origin.example" zfs "zfs" source
		)
	)
	status=$?

	assertEquals "Direct remote helper probes should still fail when the remote probe returns only stdout noise." 1 "$status"
	assertEquals "Stdout-only remote probe noise should not replace the generic dependency query failure." \
		"Failed to query dependency \"zfs\" on host origin.example." "$output"
}

test_zxfer_resolve_remote_cli_tool_requests_scoped_capabilities_for_generic_helpers() {
	log="$TEST_TMPDIR/resolve_remote_generic_scope.log"
	output=$(
		(
			LOG_PATH="$log"
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "${3:-}" >"$LOG_PATH"
				cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	zstd	0	/remote/bin/zstd
EOF
			}
			zxfer_resolve_remote_cli_tool "origin.example" zstd "compression command" source
		)
	)
	status=$?

	assertEquals "Generic remote helper resolution should still succeed through the scoped capability handshake." \
		0 "$status"
	assertEquals "Generic remote helper resolution should return the parsed helper path." \
		"/remote/bin/zstd" "$output"
	assertContains "Generic remote helper resolution should request a scoped capability payload that includes zfs." \
		"$(cat "$log")" "zfs"
	assertContains "Generic remote helper resolution should request the generic helper on demand." \
		"$(cat "$log")" "zstd"
	assertNotContains "Generic remote helper resolution should not preload parallel when it is unrelated." \
		"$(cat "$log")" "parallel"
}

test_resolve_remote_required_tool_preserves_transport_diagnostic_when_handshake_fails() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "Host key verification failed."
				return 1
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Remote helper lookups should still fail when both the capability handshake and direct secure probe fail." 1 "$status"
	assertContains "Capability-handshake fallback failures should preserve the direct transport diagnostic." \
		"$output" "Host key verification failed."
}

test_resolve_remote_required_tool_reports_generic_failure_for_unexpected_tool_status() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	2	-
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
EOF
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Unexpected handshake tool statuses should fail closed." 1 "$status"
	assertEquals "Unexpected handshake tool statuses should surface the generic dependency query error." \
		"Failed to query dependency \"zfs\" on host origin.example." "$output"
}

test_zxfer_resolve_remote_cli_tool_falls_back_to_direct_probe_when_capability_handshake_fails_for_generic_tool() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/bin/zstd"
			}
			zxfer_resolve_remote_cli_tool "origin.example" zstd "compression command" source
		)
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should fall back to a direct probe when the capability handshake fails." 0 "$status"
	assertEquals "Generic remote CLI tool handshake fallback should return the direct-probe result." \
		"/remote/bin/zstd" "$output"
}

test_zxfer_resolve_remote_cli_tool_falls_back_to_direct_probe_for_malformed_handshake_payload_for_generic_tool() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V2"
				printf '%s\n' "os	RemoteOS"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/bin/zstd"
			}
			zxfer_resolve_remote_cli_tool "origin.example" zstd "compression command" source
		)
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should fall back to a direct probe when the capability payload is malformed." 0 "$status"
	assertEquals "Generic remote CLI tool malformed-payload fallback should return the direct-probe result." \
		"/remote/bin/zstd" "$output"
}

test_zxfer_get_remote_resolved_tool_version_output_returns_full_output() {
	log_file="$TEST_TMPDIR/remote_tool_version_output.log"
	: >"$log_file"

	output=$(
		(
			LOG_FILE="$log_file"
			zxfer_invoke_ssh_shell_command_for_host() {
				{
					printf 'host=%s\n' "$1"
					printf 'cmd=%s\n' "$2"
					printf 'side=%s\n' "$3"
				} >>"$LOG_FILE"
				cat <<'EOF'
Academic tradition requires you to cite works you base your article on.
parallel 20260122 ('Maduro').
EOF
			}
			zxfer_get_remote_resolved_tool_version_output "origin.example" "/opt/bin/parallel" "parallel" source
		)
	)
	status=$?

	assertEquals "Resolved remote tool version probes should succeed when ssh returns multiline output." 0 "$status"
	assertEquals "Resolved remote tool version probes should preserve the full remote version output." \
		"Academic tradition requires you to cite works you base your article on.
parallel 20260122 ('Maduro')." "$output"
	assertContains "Resolved remote tool version probes should target the requested host." \
		"$(cat "$log_file")" "host=origin.example"
	assertContains "Resolved remote tool version probes should include the resolved helper path in the remote command." \
		"$(cat "$log_file")" "/opt/bin/parallel"
	assertContains "Resolved remote tool version probes should request --version from the resolved helper." \
		"$(cat "$log_file")" "--version"
	assertContains "Resolved remote tool version probes should preserve the source-side profile tag." \
		"$(cat "$log_file")" "side=source"
}

test_zxfer_get_remote_resolved_tool_version_output_uses_plain_version_only() {
	log_file="$TEST_TMPDIR/remote_tool_version_plain.log"
	remote_parallel_bin="$TEST_TMPDIR/remote_parallel_version_plain"
	: >"$log_file"
	cat >"$remote_parallel_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >>"$log_file"
if [ "\$1" = "--version" ]; then
	printf '%s\n' "parallel 20260122 ('Maduro')."
	exit 0
fi
exit 1
EOF
	chmod +x "$remote_parallel_bin"

	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_get_remote_resolved_tool_version_output \
				"origin.example" "$remote_parallel_bin" "parallel" source
		)
	)
	status=$?

	assertEquals "Resolved remote tool version probes should use the plain --version form." \
		0 "$status"
	assertEquals "Resolved remote tool version probes should return the plain --version output." \
		"parallel 20260122 ('Maduro')." "$output"
	assertContains "Resolved remote tool version probes should request plain --version." \
		"$(cat "$log_file")" "--version"
	assertNotContains "Resolved remote tool version probes should not use a GNU-specific --will-cite check." \
		"$(cat "$log_file")" "--will-cite"
}

test_zxfer_get_remote_resolved_tool_version_line_returns_first_line() {
	log_file="$TEST_TMPDIR/remote_tool_version.log"
	: >"$log_file"

	output=$(
		(
			LOG_FILE="$log_file"
			zxfer_invoke_ssh_shell_command_for_host() {
				{
					printf 'host=%s\n' "$1"
					printf 'cmd=%s\n' "$2"
					printf 'side=%s\n' "$3"
				} >>"$LOG_FILE"
				cat <<'EOF'
Academic tradition requires you to cite works you base your article on.
parallel 20260122 ('Maduro').
EOF
			}
			zxfer_get_remote_resolved_tool_version_line "origin.example" "/opt/bin/parallel" "parallel" source
		)
	)
	status=$?

	assertEquals "Resolved remote tool version probes should succeed when ssh returns a version line." 0 "$status"
	assertEquals "Resolved remote tool version probes should return the remote version line." \
		"Academic tradition requires you to cite works you base your article on." "$output"
	assertContains "Resolved remote tool version probes should target the requested host." \
		"$(cat "$log_file")" "host=origin.example"
	assertContains "Resolved remote tool version probes should include the resolved helper path in the remote command." \
		"$(cat "$log_file")" "/opt/bin/parallel"
	assertContains "Resolved remote tool version probes should request --version from the resolved helper." \
		"$(cat "$log_file")" "--version"
	assertContains "Resolved remote tool version probes should preserve the source-side profile tag." \
		"$(cat "$log_file")" "side=source"
}

test_zxfer_get_remote_resolved_tool_version_line_preserves_nonempty_probe_failure_output() {
	set +e
	output=$(
		(
			zxfer_get_remote_resolved_tool_version_output() {
				printf '%s\n' "remote version probe failed"
				return 1
			}
			zxfer_get_remote_resolved_tool_version_line "origin.example" "/remote/bin/tool" "tool" source
		)
	)
	status=$?

	assertEquals "Resolved remote tool version line probes should fail when the underlying version probe fails." \
		1 "$status"
	assertEquals "Resolved remote tool version line probes should preserve a non-empty underlying version-probe failure message." \
		"remote version probe failed" "$output"
}

test_zxfer_get_remote_resolved_tool_version_line_reports_probe_failures() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				return 255
			}
			zxfer_get_remote_resolved_tool_version_line "origin.example" "/opt/bin/parallel" "parallel" source
		)
	)
	status=$?

	assertEquals "Resolved remote tool version probes should fail when ssh cannot execute the remote probe." 1 "$status"
	assertEquals "Resolved remote tool version probe failures should surface the generic dependency query error." \
		"Failed to query dependency \"parallel\" on host origin.example." "$output"
}

test_zxfer_get_remote_resolved_tool_version_output_preserves_transport_diagnostic() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_get_remote_resolved_tool_version_output "origin.example" "/opt/bin/parallel" "parallel" source
		) 2>&1
	)
	status=$?

	assertEquals "Resolved remote tool version probes should fail when ssh transport setup fails." 1 "$status"
	assertContains "Resolved remote tool version probes should preserve the underlying transport diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_get_remote_resolved_tool_version_output_ignores_stdout_only_probe_noise() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "wrapper startup noise"
				return 255
			}
			zxfer_get_remote_resolved_tool_version_output "origin.example" "/opt/bin/parallel" "parallel" source
		)
	)
	status=$?

	assertEquals "Resolved remote tool version probes should fail when the remote probe returns only stdout noise." 1 "$status"
	assertEquals "Stdout-only remote tool probe noise should not replace the generic dependency query failure." \
		"Failed to query dependency \"parallel\" on host origin.example." "$output"
}

test_init_globals_initializes_defaults_and_temp_files() {
	real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	result=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_init_globals.counter"
			printf '%s\n' 0 >"$counter_file"
			g_zxfer_services_to_restart="stale-service"
			g_zxfer_property_cache_path="/tmp/stale-cache"
			zxfer_get_temp_file() {
				temp_index=$(cat "$counter_file")
				temp_index=$((temp_index + 1))
				printf '%s\n' "$temp_index" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/tmp.$temp_index"
			}
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					eval "$1=$(command -v awk 2>/dev/null || printf '%s\n' awk)"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				[ -n "${g_cmd_ssh:-}" ]
			}
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/backup_root"
			zxfer_init_globals
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'zfs=%s\n' "$g_cmd_zfs"
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'backup=%s\n' "$g_backup_storage_root"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'tmp1=%s\n' "$g_delete_source_tmp_file"
			printf 'tmp2=%s\n' "$g_delete_dest_tmp_file"
			printf 'tmp3=%s\n' "$g_delete_snapshots_to_delete_tmp_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'cache_path=<%s>\n' "$g_zxfer_property_cache_path"
		)
	)

	assertContains "zxfer_init_globals should resolve awk through the helper." "$result" "awk=$real_awk"
	assertContains "zxfer_init_globals should resolve zfs through the helper." "$result" "zfs=/stub/zfs"
	assertContains "zxfer_init_globals should defer ssh resolution until remote transport is actually needed." "$result" "ssh="
	assertContains "zxfer_init_globals should honor ZXFER_BACKUP_DIR when set." "$result" "backup=$TEST_TMPDIR/backup_root"
	assertContains "zxfer_init_globals should leave control-socket support disabled until ssh is resolved on demand." "$result" "control=0"
	assertContains "Yield iterations should default to 1." "$result" "yield=1"
	assertContains "Delete source temp file path should stay empty until delete planning needs it." "$result" "tmp1="
	assertContains "Delete destination temp file path should stay empty until delete planning needs it." "$result" "tmp2="
	assertContains "Delete diff temp file path should stay empty until delete planning needs it." "$result" "tmp3="
	assertContains "Runtime init should clear stale service restart state." "$result" "restart=<>"
	assertContains "Runtime init should clear stale property-cache path state." "$result" "cache_path=<>"
}

test_prepare_remote_host_connections_resolves_ssh_on_demand() {
	log="$TEST_TMPDIR/prepare_remote_hosts_resolve_ssh.log"
	: >"$log"

	result=$(
		(
			zxfer_find_required_tool() {
				if [ "$1" = "ssh" ]; then
					printf '%s\n' "$FAKE_SSH_BIN"
					return 0
				fi
				printf '%s\n' "/stub/$1"
			}
			zxfer_ssh_supports_control_sockets() {
				[ "${g_cmd_ssh:-}" = "$FAKE_SSH_BIN" ]
			}
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			g_cmd_ssh=""
			g_option_O_origin_host="origin.example pfexec"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			zxfer_prepare_remote_host_connections
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'lzfs=%s\n' "$g_LZFS"
		)
	)

	assertContains "Remote preparation should resolve ssh on demand when a remote host is configured." \
		"$result" "ssh=$FAKE_SSH_BIN"
	assertContains "Remote preparation should refresh control-socket capability after lazy ssh resolution." \
		"$result" "control=1"
	assertContains "Origin control-socket setup should still run after lazy ssh resolution." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Origin capability preload should still run after lazy ssh resolution." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Origin zfs rendering should still refresh after lazy ssh resolution." \
		"$result" "lzfs=/remote/origin/zfs"
}

test_zxfer_local_ssh_resolution_helpers_cover_success_and_failure_paths() {
	output=$(
		(
			set +e
			g_cmd_ssh=""
			zxfer_find_required_tool() {
				if [ "$1" = "ssh" ]; then
					printf '%s\n' "$FAKE_SSH_BIN"
					return 0
				fi
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_success=%s:%s:%s\n' "$?" "$g_cmd_ssh" "$g_zxfer_resolved_local_ssh_command_result"

			g_cmd_ssh=""
			zxfer_find_required_tool() {
				printf '%s\n' "missing ssh"
				return 1
			}
			zxfer_ensure_local_ssh_command
			printf 'ensure_failure=%s:%s\n' "$?" "$g_zxfer_resolved_local_ssh_command_result"

			g_cmd_ssh=""
			zxfer_get_resolved_local_ssh_command
			printf 'resolved_failure=%s\n' "$?"
		)
	)

	assertContains "Lazy local ssh resolution should cache the resolved ssh helper on success." \
		"$output" "ensure_success=0:$FAKE_SSH_BIN:$FAKE_SSH_BIN"
	assertContains "Lazy local ssh resolution should preserve the dependency diagnostic when ssh lookup fails." \
		"$output" "ensure_failure=1:missing ssh"
	assertContains "Resolved local ssh lookups should print the dependency diagnostic when ssh lookup fails." \
		"$output" "missing ssh"
	assertContains "Resolved local ssh lookups should fail closed when ssh lookup fails." \
		"$output" "resolved_failure=1"
}

test_zxfer_remote_host_lock_metric_and_purpose_helpers_cover_current_shell_paths() {
	g_option_V_very_verbose=1
	g_zxfer_profile_ssh_control_socket_lock_wait_count=0
	g_zxfer_profile_ssh_control_socket_lock_wait_ms=""
	g_zxfer_profile_remote_capability_cache_wait_count=0
	g_zxfer_profile_remote_capability_cache_wait_ms=""
	g_zxfer_ssh_control_socket_lock_error="staged lock error"
	g_zxfer_ssh_control_socket_lease_count_result="9"

	zxfer_record_ssh_control_socket_lock_wait_metrics 0 ""
	zxfer_record_ssh_control_socket_lock_wait_metrics 1 ""
	zxfer_record_remote_capability_cache_wait_metrics 0 ""
	zxfer_record_remote_capability_cache_wait_metrics 1 ""

	lock_purpose=$(zxfer_get_ssh_control_socket_lock_purpose)
	lease_purpose=$(zxfer_get_ssh_control_socket_lease_purpose)
	cache_lock_purpose=$(zxfer_get_remote_capability_cache_lock_purpose)

	zxfer_reset_ssh_control_socket_lock_state

	assertEquals "SSH control-socket lock wait metrics should increment only for waited attempts." \
		1 "${g_zxfer_profile_ssh_control_socket_lock_wait_count:-0}"
	assertEquals "Remote capability cache wait metrics should increment only for waited attempts." \
		1 "${g_zxfer_profile_remote_capability_cache_wait_count:-0}"
	assertEquals "SSH control-socket lock purpose helpers should return the stable metadata purpose." \
		"ssh-control-socket-lock" "$lock_purpose"
	assertEquals "SSH control-socket lease purpose helpers should return the stable metadata purpose." \
		"ssh-control-socket-lease" "$lease_purpose"
	assertEquals "Remote capability cache lock purpose helpers should return the stable metadata purpose." \
		"remote-capability-cache-lock" "$cache_lock_purpose"
	assertEquals "Resetting SSH control-socket lock state should clear the staged error." \
		"" "${g_zxfer_ssh_control_socket_lock_error:-}"
	assertEquals "Resetting SSH control-socket lock state should clear the staged lease count scratch." \
		"" "${g_zxfer_ssh_control_socket_lease_count_result:-}"
}

test_zxfer_remote_host_cache_prefix_and_socket_support_helpers_cover_current_shell_paths() {
	fake_support_bin="$TEST_TMPDIR/fake_ssh_support"
	long_suffix=""
	short_entry_dir="/tmp/zxfer-short-entry"
	long_entry_dir=""

	cat >"$fake_support_bin" <<'EOF'
#!/bin/sh
if [ "$1" = "-M" ] && [ "$2" = "-V" ]; then
	exit 0
fi
exit 1
EOF
	chmod +x "$fake_support_bin"

	g_option_Y_yield_iterations=17
	g_zxfer_temp_prefix=""
	zxfer_get_remote_host_cache_root_prefix >/dev/null
	first_prefix=${g_zxfer_temp_prefix:-}
	second_prefix=$(zxfer_get_remote_host_cache_root_prefix)
	g_cmd_ssh="$fake_support_bin"
	set +e
	zxfer_ssh_supports_control_sockets >/dev/null 2>&1
	support_status=$?
	g_cmd_ssh="$TEST_TMPDIR/missing_ssh"
	zxfer_ssh_supports_control_sockets >/dev/null 2>&1
	missing_status=$?
	set -e

	while [ "${#long_suffix}" -lt 150 ]; do
		long_suffix="${long_suffix}xxxxxxxxxx"
	done
	long_entry_dir="/tmp/$long_suffix"

	set +e
	zxfer_is_ssh_control_socket_entry_path_short_enough "$short_entry_dir"
	short_status=$?
	zxfer_is_ssh_control_socket_entry_path_short_enough "$long_entry_dir"
	long_status=$?
	set -e

	assertEquals "Remote host cache root prefix helpers should cache the generated prefix in the current shell." \
		"$first_prefix" "$second_prefix"
	assertContains "Remote host cache root prefix helpers should include the current yield-iteration component in generated prefixes." \
		"$first_prefix" ".17."
	assertEquals "SSH control-socket support helpers should detect a transport that accepts -M -V probes." \
		0 "$support_status"
	assertEquals "SSH control-socket support helpers should fail closed when the configured ssh helper cannot be probed." \
		"yes" "$(if [ "$missing_status" -ne 0 ]; then printf '%s' yes; else printf '%s' no; fi)"
	assertEquals "SSH control-socket path-length helpers should accept short entry paths." \
		0 "$short_status"
	assertEquals "SSH control-socket path-length helpers should reject overly long entry paths." \
		1 "$long_status"
}

test_zxfer_ssh_control_socket_identity_helpers_cover_current_shell_paths() {
	g_cmd_ssh="$FAKE_SSH_BIN"

	output=$(
		(
			zxfer_render_ssh_transport_policy_identity() {
				printf '%s\n' "policy"
			}
			cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
			cache_key_status=$?
			identity=$(zxfer_render_ssh_control_socket_entry_identity "origin.example")
			identity_status=$?
			printf 'cache_key_status=%s\n' "$cache_key_status"
			printf 'cache_key=%s\n' "$cache_key"
			printf 'identity_status=%s\n' "$identity_status"
			printf 'identity=%s\n' "$identity"
		)
	)

	assertContains "SSH control-socket cache-key helpers should succeed when transport policy rendering succeeds." \
		"$output" "cache_key_status=0"
	assertContains "SSH control-socket cache-key helpers should emit the stable key prefix." \
		"$output" "cache_key=k"
	assertContains "SSH control-socket identity helpers should succeed when transport policy rendering succeeds." \
		"$output" "identity_status=0"
	assertContains "SSH control-socket identity helpers should include the configured ssh path." \
		"$output" "identity=$FAKE_SSH_BIN"
	assertContains "SSH control-socket identity helpers should include the rendered policy identity." \
		"$output" "policy"
	assertContains "SSH control-socket identity helpers should include the rendered host spec." \
		"$output" "origin.example"
}

test_zxfer_get_resolved_local_ssh_command_returns_cached_value_in_current_shell() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_zxfer_resolved_local_ssh_command_result="$FAKE_SSH_BIN"

	assertEquals "Resolved local ssh lookups should return the cached helper path directly in the current shell." \
		"$FAKE_SSH_BIN" "$(zxfer_get_resolved_local_ssh_command)"
}

test_init_globals_rejects_relative_backup_dir_override() {
	set +e
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_BACKUP_DIR="relative-backups"
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					eval "$1=$(command -v awk 2>/dev/null || printf '%s\n' awk)"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_init_globals
		) 2>&1
	)
	status=$?
	set -e

	assertEquals "Relative ZXFER_BACKUP_DIR overrides should abort startup." 1 "$status"
	assertContains "Startup should report that ZXFER_BACKUP_DIR must be absolute." \
		"$output" "ZXFER_BACKUP_DIR must be an absolute path"
}

test_zxfer_find_required_tool_reports_missing_dependency() {
	empty_path="$TEST_TMPDIR/empty_path"
	mkdir -p "$empty_path"
	g_zxfer_secure_path="$empty_path"
	g_zxfer_dependency_path="$empty_path"

	set +e
	result=$(zxfer_find_required_tool definitely_missing "missing-tool")
	status=$?

	assertEquals "Missing dependencies should fail lookup." 1 "$status"
	assertEquals "Missing dependencies should mention the secure PATH guidance." \
		"Required dependency \"missing-tool\" not found in secure PATH ($empty_path). Set ZXFER_SECURE_PATH or install the binary." \
		"$result"
}

test_zxfer_find_required_tool_rejects_relative_resolution() {
	set +e
	result=$(
		(
			mocktool() {
				:
			}
			g_zxfer_secure_path="$ZXFER_DEFAULT_SECURE_PATH"
			g_zxfer_dependency_path="$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_find_required_tool mocktool "mocktool"
		)
	)
	status=$?

	assertEquals "Relative command -v results should be rejected." 1 "$status"
	assertEquals "Relative paths should be rejected explicitly." \
		"Required dependency \"mocktool\" resolved to \"mocktool\", but zxfer requires an absolute path." \
		"$result"
}

test_zxfer_find_required_tool_returns_absolute_path_from_secure_path() {
	tool_dir="$TEST_TMPDIR/required_tool_path"
	mkdir -p "$tool_dir"
	cat >"$tool_dir/mocktool" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$tool_dir/mocktool"
	g_zxfer_secure_path="$tool_dir"
	g_zxfer_dependency_path="$tool_dir"

	result=$(zxfer_find_required_tool mocktool "mocktool")

	assertEquals "Required tool lookup should return the resolved absolute path from the secure PATH." \
		"$tool_dir/mocktool" "$result"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace() {
	tab=$(printf '\t')

	set +e
	result=$(zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool")
	status=$?

	assertEquals "Resolved tool paths with control whitespace should be rejected." 1 "$status"
	assertContains "Rejected tool paths should explain the control-whitespace requirement." \
		"$result" "single-line absolute path without control whitespace"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace_with_scope() {
	tab=$(printf '\t')

	set +e
	result=$(zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool" "host origin.example")
	status=$?

	assertEquals "Scoped control-whitespace tool paths should be rejected." 1 "$status"
	assertContains "Scoped control-whitespace failures should mention the host scope." \
		"$result" "Required dependency \"mocktool\" on host origin.example resolved to"
}

test_zxfer_assign_required_tool_marks_dependency_failures() {
	set +e
	output=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_assign_required_tool g_cmd_test mocktool "mocktool"
		)
	)
	status=$?

	assertEquals "zxfer_assign_required_tool should abort when lookup fails." 1 "$status"
	assertContains "Dependency lookup failures should be classified correctly." "$output" "class=dependency"
	assertContains "Dependency lookup failures should preserve the lookup message." "$output" "msg=lookup failed"
}

test_zxfer_assign_required_tool_sets_target_variable_on_success() {
	result=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "/opt/mock/mocktool"
			}
			g_cmd_mock=""
			zxfer_assign_required_tool g_cmd_mock mocktool "mocktool"
			printf '%s\n' "$g_cmd_mock"
		)
	)

	assertEquals "Successful tool assignment should populate the requested variable." "/opt/mock/mocktool" "$result"
}

test_init_globals_rejects_control_whitespace_in_optional_parallel_path() {
	tab=$(printf '\t')
	parallel_dir="$TEST_TMPDIR/parallel${tab}bin"
	mkdir -p "$parallel_dir"
	cat >"$parallel_dir/parallel" <<'EOF'
#!/bin/sh
printf '%s\n' "parallel (fake)"
exit 0
EOF
	chmod +x "$parallel_dir/parallel"

	set +e
	output=$(
		(
			ZXFER_SECURE_PATH="$parallel_dir:/usr/bin:/bin:/usr/sbin:/sbin"
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					# shellcheck disable=SC2034
					l_real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
					eval "$1=\$l_real_awk"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_get_temp_file() {
				printf '%s\n' "$TEST_TMPDIR/tmp"
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_init_globals
		)
	)
	status=$?

	assertEquals "zxfer_init_globals should fail when optional parallel resolves to a path with control whitespace." 1 "$status"
	assertContains "Invalid optional parallel resolutions should be classified as dependency failures." \
		"$output" "class=dependency"
	assertContains "Invalid optional parallel resolutions should explain the path validation failure." \
		"$output" "single-line absolute path without control whitespace"
}

test_extract_snapshot_identity_returns_empty_for_non_snapshot_path() {
	result=$(zxfer_extract_snapshot_identity "tank/src")

	assertEquals "Snapshot identities should be empty when the record does not include a snapshot suffix." \
		"" "$result"
}

test_extract_snapshot_dataset_and_guid_detection_helpers() {
	assertEquals "Snapshot dataset extraction should strip the snapshot suffix from guid-bearing records." \
		"tank/src" "$(zxfer_extract_snapshot_dataset "tank/src@snap1	123")"
	assertEquals "Snapshot dataset extraction should return empty for non-snapshot records." \
		"" "$(zxfer_extract_snapshot_dataset "tank/src")"
	assertTrue "Guid detection should report true when a snapshot record includes a guid field." \
		'zxfer_snapshot_record_list_contains_guid "tank/src@snap1	123"'
	assertFalse "Guid detection should report false for name-only snapshot records." \
		'zxfer_snapshot_record_list_contains_guid "tank/src@snap1"'
}

test_zxfer_reverse_snapshot_record_list_and_name_overlap_helpers() {
	reversed=$(zxfer_reverse_snapshot_record_list "tank/src@snap1	111
tank/src@snap2	222
tank/src@snap3	333")

	assertEquals "Snapshot-record reversal should preserve full records while reversing their order." \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" "$reversed"

	set +e
	zxfer_snapshot_record_lists_share_snapshot_name "tank/src@snap2
tank/src@snap1" "backup/dst@snap9
backup/dst@snap1"
	status=$?
	assertEquals "Snapshot-name overlap detection should succeed when both sides share any snapshot name." \
		0 "$status"

	zxfer_snapshot_record_lists_share_snapshot_name "tank/src@snap2
tank/src@snap1" "backup/dst@other"
	status=$?
	assertEquals "Snapshot-name overlap detection should fail when the lists do not share any snapshot name." \
		1 "$status"
}

test_zxfer_filter_snapshot_identity_records_to_reference_paths_preserves_identity_order() {
	result=$(zxfer_filter_snapshot_identity_records_to_reference_paths \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" \
		"tank/src@snap2
tank/src@snap1")

	assertEquals "Reference-path filtering should keep only matching identity records in their original identity-record order." \
		"tank/src@snap2	222
tank/src@snap1	111" "$result"
}

test_zxfer_get_source_snapshot_identity_records_for_dataset_reverses_creation_order() {
	result=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' \
					"tank/src@snap1	111" \
					"tank/src@snap2	222" \
					"tank/src@snap3	333"
			}

			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src"
		)
	)

	assertEquals "Source identity-record retrieval should reverse creation-ordered zfs output into newest-first order." \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" "$result"
}

test_zxfer_get_destination_snapshot_identity_records_for_dataset_filters_descendants() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' \
					"backup/dst@snap1	111" \
					"backup/dst/child@snap1	211" \
					"backup/dst@snap2	222"
			}

			zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst"
		)
	)

	assertEquals "Destination identity-record retrieval should keep only the exact dataset snapshots and drop descendant records." \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$result"
}

test_zxfer_get_snapshot_identity_records_for_dataset_dispatches_and_filters_reference_records() {
	result=$(
		(
			zxfer_get_source_snapshot_identity_records_for_dataset() {
				printf '%s\n' \
					"tank/src@snap3	333" \
					"tank/src@snap2	222" \
					"tank/src@snap1	111"
			}
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n' \
					"backup/dst@snap2	222" \
					"backup/dst@snap1	111"
			}

			zxfer_get_snapshot_identity_records_for_dataset source "tank/src" "tank/src@snap2
tank/src@snap1"
		)
	)

	assertEquals "Generic identity-record lookup should dispatch to the requested side and honor reference-path filtering." \
		"tank/src@snap2	222
tank/src@snap1	111" "$result"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset invalid "tank/src"
		)
	)
	status=$?

	assertEquals "Generic identity-record lookup should reject unknown lookup sides." 1 "$status"
	assertEquals "Rejected identity-record lookups should not emit an output payload." "" "$output"
}

test_zxfer_snapshot_identity_record_helpers_report_lookup_failures_and_destination_dispatch() {
	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				return 1
			}

			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src"
		)
	)
	status=$?
	assertEquals "Source identity-record lookup should fail cleanly when the zfs query fails." 1 "$status"
	assertEquals "Failed source identity lookups should not emit a payload." "" "$output"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}

			zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst"
		)
	)
	status=$?
	assertEquals "Destination identity-record lookup should fail cleanly when the zfs query fails." 1 "$status"
	assertEquals "Failed destination identity lookups should not emit a payload." "" "$output"

	output=$(
		(
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n' "backup/dst@snap2	222"
			}

			zxfer_get_snapshot_identity_records_for_dataset destination "backup/dst"
		)
	)
	status=$?
	assertEquals "Generic identity-record lookup should support the destination side without requiring reference filters." 0 "$status"
	assertEquals "Destination-side identity dispatch should return the destination helper payload unchanged when no reference filter is supplied." \
		"backup/dst@snap2	222" "$output"
}

test_read_command_line_switches_sets_options_and_remote_paths() {
	log="$TEST_TMPDIR/read_switches.log"
	: >"$log"
	result=$(
		(
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_refresh_compression_commands() {
				printf 'refresh\n' >>"$log"
				g_cmd_compress_safe="zstd -9"
				g_cmd_decompress_safe="zstd -d"
			}
			g_ssh_supports_control_sockets=1
			g_cmd_zfs="/sbin/zfs"
			g_test_max_yield_iterations=8
			OPTIND=1
			zxfer_read_command_line_switches \
				-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
				-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
				-N "tank/nonrecursive" -o "atime=off" -O "origin.example pfexec" \
				-P -R "tank/src" -s -T "target.example doas" -U -v -V -w \
				-x "child" -Y -z -Z "zstd -9"
			printf 'origin=%s\n' "$g_option_O_origin_host"
			printf 'target=%s\n' "$g_option_T_target_host"
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'compress=%s\n' "$g_cmd_compress"
			printf 'props=%s\n' "$g_option_P_transfer_property"
			printf 'verbose=%s/%s\n' "$g_option_v_verbose" "$g_option_V_very_verbose"
		)
	)

	assertContains "Origin host should be recorded from -O." "$result" "origin=origin.example pfexec"
	assertContains "Target host should be recorded from -T." "$result" "target=target.example doas"
	assertContains "Origin zfs spec should remain the resolved zfs path until remote execution is rendered." "$result" "lzfs=/sbin/zfs"
	assertContains "Target zfs spec should remain the resolved zfs path until remote execution is rendered." "$result" "rzfs=/sbin/zfs"
	assertContains "Parallel job count should come from -j." "$result" "jobs=4"
	assertContains "Yield iterations should expand to the max when -Y is set." "$result" "yield=8"
	assertContains "Custom compression should be recorded from -Z." "$result" "compress=zstd -9"
	assertContains "Property transfer should be enabled by -e/-k/-m/-P." "$result" "props=1"
	assertContains "Very verbose mode should imply verbose mode." "$result" "verbose=1/1"
	assertContains "Compression refresh should run after parsing options." "$(cat "$log")" "refresh"
}

test_zxfer_refresh_remote_zfs_commands_rejects_shell_quoted_host_specs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_O_origin_host='origin.example "pfexec -u zfs"'
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			zxfer_refresh_remote_zfs_commands
		)
	)
	status=$?
	set -e

	assertEquals "Remote host-spec refresh should fail closed when the configured host spec relies on shell quoting." \
		2 "$status"
	assertContains "Rejected remote host specs should explain the literal-token requirement." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_prepare_remote_host_connections_sets_up_control_sockets_after_validation() {
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	now_counter_file="$TEST_TMPDIR/prepare_remote_hosts.now.counter"
	: >"$log"
	printf '%s\n' 0 >"$now_counter_file"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 1000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 1250
				fi
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertContains "Origin control socket setup should happen during remote preparation." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Target control socket setup should happen during remote preparation." \
		"$(cat "$log")" "setup target.example doas target"
	assertContains "Origin capability discovery should be preloaded once sockets are ready." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Target capability discovery should be preloaded once sockets are ready." \
		"$(cat "$log")" "preload target.example doas destination"
	assertContains "Origin zfs spec should refresh to the resolved origin helper path." \
		"$result" "lzfs=/remote/origin/zfs"
	assertContains "Target zfs spec should refresh to the resolved target helper path." \
		"$result" "rzfs=/remote/target/zfs"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_remote_host_connections_logs_when_control_sockets_are_unavailable() {
	log="$TEST_TMPDIR/prepare_remote_hosts_no_mux.log"
	: >"$log"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=0
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertContains "Origin preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for origin host."
	assertContains "Target preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for target host."
	assertContains "Origin capability discovery should still be preloaded without control sockets." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Target capability discovery should still be preloaded without control sockets." \
		"$(cat "$log")" "preload target.example doas destination"
	assertContains "Remote zfs specs should still refresh even without control socket support." \
		"$output" "lzfs=/remote/origin/zfs"
	assertContains "Remote zfs specs should still refresh target commands even without control socket support." \
		"$output" "rzfs=/remote/target/zfs"
}

test_prepare_remote_host_connections_surfaces_verbose_preload_failures() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_setup_ssh_control_socket() {
				:
			}
			zxfer_preload_remote_host_capabilities() {
				printf '%s\n' "Host key verification failed." >&2
				return 1
			}
			g_option_v_verbose=1
			g_option_O_origin_host="origin.example pfexec"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
		) 2>&1
	)

	assertContains "Verbose remote preparation should surface opportunistic preload diagnostics instead of discarding them." \
		"$output" "Host key verification failed."
}

test_prepare_remote_host_connections_skips_live_setup_in_dry_run() {
	log="$TEST_TMPDIR/prepare_remote_hosts_dry_run.log"
	: >"$log"

	output=$(
		(
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertEquals "Dry-run remote preparation should not open control sockets or preload capabilities." \
		"" "$(cat "$log")"
	assertContains "Dry-run remote preparation should explain that origin ssh preflight is skipped." \
		"$output" "Dry run: skipping ssh control-socket setup and remote capability preload for origin host."
	assertContains "Dry-run remote preparation should explain that target ssh preflight is skipped." \
		"$output" "Dry run: skipping ssh control-socket setup and remote capability preload for target host."
	assertContains "Dry-run remote preparation should still refresh the origin zfs render command." \
		"$output" "lzfs=/remote/origin/zfs"
	assertContains "Dry-run remote preparation should still refresh the target zfs render command." \
		"$output" "rzfs=/remote/target/zfs"
}

test_read_command_line_switches_sets_flags_in_current_shell() {
	OPTIND=1
	g_cmd_ssh="/usr/bin/ssh"
	g_cmd_zfs="/sbin/zfs"
	g_test_max_yield_iterations=9
	g_ssh_supports_control_sockets=0
	zxfer_refresh_compression_commands() {
		:
	}

	zxfer_read_command_line_switches \
		-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
		-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
		-N "tank/nonrecursive" -o "atime=off" -V \
		-O "origin.example pfexec" -P -R "tank/src" -s \
		-T "target.example doas" -U -w -x "child" -Y -z -Z "zstd -9"

	assertEquals "Beep-always should be enabled by -b." "1" "$g_option_b_beep_always"
	assertEquals "Beep-on-success should be enabled by -B." "1" "$g_option_B_beep_on_success"
	assertEquals "Service list should be captured from -c." "svc:/network/nfs/server" "$g_option_c_services"
	assertEquals "Snapshot deletion should be enabled by -d." "1" "$g_option_d_delete_destination_snapshots"
	assertEquals "Progress display command should be captured from -D." "pv -N %%title%%" "$g_option_D_display_progress_bar"
	assertEquals "Grandfather protection should be captured from -g." "7" "$g_option_g_grandfather_protection"
	assertEquals "Ignore-properties list should be captured from -I." "mountpoint" "$g_option_I_ignore_properties"
	assertEquals "Parallel job count should be captured from -j." "4" "$g_option_j_jobs"
	assertEquals "Nonrecursive source should be captured from -N." "tank/nonrecursive" "$g_option_N_nonrecursive"
	assertEquals "Override property should be captured from -o." "atime=off" "$g_option_o_override_property"
	# zxfer_read_command_line_switches runs in the current shell here; the SC2031
	# warning is triggered by separate subshell-based coverage elsewhere.
	# shellcheck disable=SC2031
	assertEquals "Origin host should be captured from -O." "origin.example pfexec" "$g_option_O_origin_host"
	assertEquals "Recursive source should be captured from -R." "tank/src" "$g_option_R_recursive"
	# shellcheck disable=SC2031
	assertEquals "Target host should be captured from -T." "target.example doas" "$g_option_T_target_host"
	assertEquals "Exclude list should be captured from -x." "child" "$g_option_x_exclude_datasets"
	assertEquals "Very-verbose mode should imply verbose mode." "1" "$g_option_v_verbose"
	assertEquals "Very-verbose mode should be enabled by -V." "1" "$g_option_V_very_verbose"
	assertEquals "Raw-send mode should be enabled by -w." "1" "$g_option_w_raw_send"
	assertEquals "Unsupported-property skipping should be enabled by -U." "1" "$g_option_U_skip_unsupported_properties"
	assertEquals "Compression should be enabled by -z/-Z." "1" "$g_option_z_compress"
	assertEquals "Yield iterations should expand to the configured maximum." "9" "$g_option_Y_yield_iterations"
	assertEquals "The parser should preserve the custom compression command from -Z." "zstd -9" "$g_cmd_compress"
	assertEquals "Property transfer should be enabled by property-affecting switches." "1" "$g_option_P_transfer_property"
	assertEquals "Origin zfs spec should remain the resolved zfs path after parsing." \
		"/sbin/zfs" "$g_LZFS"
	assertEquals "Target zfs spec should remain the resolved zfs path after parsing." \
		"/sbin/zfs" "$g_RZFS"

	unset -f zxfer_refresh_compression_commands
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"
}

test_read_command_line_switches_rejects_invalid_option() {
	set +e
	output=$(
		(
			zxfer_refresh_compression_commands() {
				:
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			OPTIND=1
			zxfer_read_command_line_switches -Q 2>/dev/null
		)
	)
	status=$?

	assertEquals "Invalid options should exit with usage status." 2 "$status"
	assertContains "Invalid options should use the generic usage error." "$output" "Invalid option provided."
}

test_read_command_line_switches_exits_zero_for_help() {
	set +e
	output=$(
		(
			zxfer_usage() {
				printf '%s\n' "usage output"
			}
			OPTIND=1
			zxfer_read_command_line_switches -h
			printf '%s\n' "after-help"
		)
	)
	status=$?

	assertEquals "The help switch should exit successfully." 0 "$status"
	assertEquals "The help switch should print usage and stop parsing immediately." "usage output" "$output"
}

test_consistency_check_rejects_non_numeric_jobs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=abc
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Non-numeric job counts should fail validation." 2 "$status"
	assertContains "The validation error should mention the invalid job count." \
		"$output" "The -j option requires a positive integer job count"
}

test_consistency_check_rejects_zero_jobs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=0
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Zero job counts should fail validation." 2 "$status"
	assertContains "The validation error should require at least one job." \
		"$output" "requires a job count of at least 1"
}

test_consistency_check_rejects_remote_migration_conflicts() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_O_origin_host="origin.example"
			g_option_m_migrate=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Remote migration should be rejected." 2 "$status"
	assertContains "Remote migration conflicts should use the documented error." \
		"$output" "You cannot migrate to or from a remote host."
}

test_consistency_check_rejects_compression_without_remote_host() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_z_compress=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Compression without -O/-T should be rejected." 2 "$status"
	assertContains "Compression validation should point to the missing remote host." \
		"$output" "-z option can only be used with -O or -T option"
}

test_init_variables_uses_gawk_on_sunos_when_available() {
	gawk_dir="$TEST_TMPDIR/gawk_path"
	mkdir -p "$gawk_dir"
	cat >"$gawk_dir/gawk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$gawk_dir/gawk"

	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_dependency_path="$gawk_dir"
			zxfer_init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "FreeBSD"
			}
			zxfer_assign_required_tool() {
				if [ "$2" = "cat" ]; then
					eval "$1=/bin/cat"
				else
					eval "$1=/stub/$2"
				fi
			}
			g_option_e_restore_property_mode=1
			zxfer_init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat through the required-tool helper." \
		"$result" "cat=/bin/cat"
}

test_refresh_compression_commands_resolves_local_helpers_when_enabled() {
	result=$(
		(
			zxfer_find_required_tool() {
				if [ "$1" = "zstd" ]; then
					printf '%s\n' "/secure/bin/zstd"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
			printf 'compress=%s\n' "$g_cmd_compress_safe"
			printf 'decompress=%s\n' "$g_cmd_decompress_safe"
		)
	)

	assertContains "Enabled compression should resolve the compressor head token through the secure local path." \
		"$result" "compress='/secure/bin/zstd' '-T0' '-9'"
	assertContains "Enabled compression should resolve the decompressor head token through the secure local path." \
		"$result" "decompress='/secure/bin/zstd' '-d'"
}

test_zxfer_resolve_remote_cli_command_safe_resolves_first_token_and_preserves_args() {
	result=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "/remote/bin/zstd"
			}
			zxfer_resolve_remote_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source
		)
	)

	assertEquals "Remote CLI command resolution should replace only the first token and keep the remaining arguments intact." \
		"'/remote/bin/zstd' '-T0' '-9'" "$result"
}

test_zxfer_resolve_remote_cli_command_safe_uses_cached_capability_tool_for_generic_heads() {
	result_file="$TEST_TMPDIR/resolve_remote_cli_cached_generic.out"
	probe_file="$TEST_TMPDIR/resolve_remote_cli_cached_generic.probes"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_cached_generic.direct"

	(
		g_option_O_origin_host="origin.example"
		g_option_j_jobs=4
		g_option_z_compress=1
		g_cmd_compress="zstd -T0 -9"
		g_zxfer_profile_remote_cli_tool_direct_probes=0
		zxfer_ensure_remote_host_capabilities() {
			cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	zstd	0	/remote/bin/zstd
EOF
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			return 1
		}
		zxfer_resolve_remote_cli_command_safe \
			"origin.example" "zstd -T0 -9" "compression command" source >"$result_file"
		printf '%s\n' "${g_zxfer_profile_remote_cli_tool_direct_probes:-0}" >"$probe_file"
	)
	status=$?

	assertEquals "Remote CLI command resolution should reuse cached capability tool records for generic helper heads." \
		0 "$status"
	assertEquals "Cached generic helper resolution should replace only the first command token." \
		"'/remote/bin/zstd' '-T0' '-9'" "$(cat "$result_file")"
	assertEquals "Cached generic helper resolution should not fall back to a direct remote helper probe when the capability payload already advertises the tool." \
		"" "$(cat "$direct_log" 2>/dev/null)"
	assertEquals "Cached generic helper resolution should leave the direct-probe counter at zero when no probe is needed." \
		"0" "$(cat "$probe_file")"
}

test_zxfer_resolve_remote_cli_tool_prefers_prewarmed_host_scope_for_generic_heads() {
	result_file="$TEST_TMPDIR/resolve_remote_cli_host_scope.out"
	log_file="$TEST_TMPDIR/resolve_remote_cli_host_scope.log"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_host_scope.direct"

	(
		LOG_PATH="$log_file"
		g_option_O_origin_host="origin.example"
		g_option_j_jobs=4
		g_option_e_restore_property_mode=1
		g_option_z_compress=1
		g_cmd_compress="zstd -T0 -9"
		zxfer_ensure_remote_host_capabilities() {
			printf '%s\n' "${3:-}" >"$LOG_PATH"
			cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
tool	zstd	0	/remote/bin/zstd
EOF
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			return 1
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$result_file"
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should succeed when the broader host scope already advertises the helper." \
		0 "$status"
	assertEquals "Generic remote CLI tool resolution should return the parsed helper path from the broader host scope." \
		"/remote/bin/zstd" "$(cat "$result_file")"
	assertContains "Generic remote CLI tool resolution should reuse the broader host-scoped preload identity for parallel when -j is active." \
		"$(cat "$log_file")" "parallel"
	assertContains "Generic remote CLI tool resolution should reuse the broader host-scoped preload identity for cat when restore-property mode is active." \
		"$(cat "$log_file")" "cat"
	assertContains "Generic remote CLI tool resolution should still include the requested generic helper in the reused host scope." \
		"$(cat "$log_file")" "zstd"
	assertEquals "Generic remote CLI tool resolution should not reopen a direct probe when the broader host scope already advertises the helper." \
		"" "$(cat "$direct_log" 2>/dev/null)"
}

test_zxfer_resolve_local_cli_command_safe_rejects_blank_commands_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_local_cli_blank.out"

	(
		zxfer_resolve_local_cli_command_safe "   " "compression command" >"$output_file"
	)
	status=$?

	assertEquals "Blank local CLI commands should be rejected." 1 "$status"
	assertContains "Blank local CLI command failures should use the documented validation message." \
		"$(cat "$output_file")" "Required dependency \"compression command\" must not be empty or whitespace-only."
}

test_zxfer_resolve_local_cli_command_safe_surfaces_lookup_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_local_cli_lookup_failure.out"

	(
		zxfer_find_required_tool() {
			printf '%s\n' "missing helper"
			return 1
		}
		zxfer_resolve_local_cli_command_safe "zstd -T0 -9" "compression command" >"$output_file"
	)
	status=$?

	assertEquals "Local CLI command resolution should fail when the head token cannot be resolved." 1 "$status"
	assertEquals "Local CLI command resolution should surface the dependency lookup failure verbatim." \
		"missing helper" "$(cat "$output_file")"
}

test_zxfer_resolve_remote_cli_tool_delegates_known_tools_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_tool_known.out"
	log_file="$TEST_TMPDIR/resolve_remote_cli_tool_known.log"

	(
		zxfer_resolve_remote_required_tool() {
			printf '%s:%s:%s:%s\n' "$1" "$2" "$3" "$4" >"$log_file"
			printf '%s\n' "/remote/bin/zfs"
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zfs" "source zfs" source >"$output_file"
	)
	status=$?

	assertEquals "Known remote CLI tools should delegate to zxfer_resolve_remote_required_tool." 0 "$status"
	assertEquals "Known remote CLI tool delegation should preserve the host, tool, label, and profile side." \
		"origin.example:zfs:source zfs:source" "$(cat "$log_file")"
	assertEquals "Known remote CLI tool delegation should return the resolved remote helper path." \
		"/remote/bin/zfs" "$(cat "$output_file")"
}

test_zxfer_resolve_remote_cli_tool_reports_missing_and_query_failures_in_current_shell() {
	missing_output="$TEST_TMPDIR/resolve_remote_cli_tool_missing.out"
	missing_log="$TEST_TMPDIR/resolve_remote_cli_tool_missing.log"
	error_output="$TEST_TMPDIR/resolve_remote_cli_tool_error.out"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			printf '%s\n' "$2" >"$missing_log"
			return 10
		}
		g_zxfer_dependency_path="/stale/secure/path"
		ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$missing_output"
	)
	missing_status=$?

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			return 77
		}
		g_zxfer_dependency_path="/secure/bin"
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$error_output"
	)
	error_status=$?

	assertEquals "Missing remote CLI tools should return failure." 1 "$missing_status"
	assertContains "Missing remote CLI tool probes should refresh the secure PATH from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$(cat "$missing_log")" "/fresh/secure/path:/usr/bin"
	assertNotContains "Missing remote CLI tool probes should not keep using a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$(cat "$missing_log")" "/stale/secure/path"
	assertContains "Missing remote CLI tools should use the documented secure-PATH guidance." \
		"$(cat "$missing_output")" "Required dependency \"compression command\" not found on host origin.example in secure PATH (/fresh/secure/path:/usr/bin)."
	assertEquals "Remote CLI probe errors should return failure." 1 "$error_status"
	assertContains "Remote CLI probe errors should use the documented generic failure message." \
		"$(cat "$error_output")" "Failed to query dependency \"compression command\" on host origin.example."
}

test_zxfer_resolve_remote_cli_tool_falls_back_to_direct_probe_when_generic_tool_is_absent_from_capabilities() {
	result_file="$TEST_TMPDIR/resolve_remote_cli_absent_fallback.out"
	probe_file="$TEST_TMPDIR/resolve_remote_cli_absent_fallback.probes"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_absent_fallback.direct"

	(
		g_option_V_very_verbose=1
		g_zxfer_profile_remote_cli_tool_direct_probes=0
		zxfer_ensure_remote_host_capabilities() {
			fake_remote_capability_response
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			zxfer_profile_increment_counter g_zxfer_profile_remote_cli_tool_direct_probes
			printf '%s\n' "/remote/bin/zstd"
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$result_file"
		printf '%s\n' "${g_zxfer_profile_remote_cli_tool_direct_probes:-0}" >"$probe_file"
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should fall back to a direct probe when the capability payload does not advertise the requested tool." \
		0 "$status"
	assertEquals "Generic remote CLI tool fallback should return the direct-probe helper path." \
		"/remote/bin/zstd" "$(cat "$result_file")"
	assertEquals "Generic remote CLI tool fallback should call the direct-probe helper when the capability payload omits the requested tool." \
		"direct-probe-called" "$(cat "$direct_log")"
	assertEquals "Generic remote CLI tool fallback should make the direct-probe counter visible when it has to probe." \
		"1" "$(cat "$probe_file")"
}

test_zxfer_resolve_remote_cli_tool_reports_missing_generic_dependency_from_capabilities_without_direct_probe() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_cached_missing.out"
	direct_log="$TEST_TMPDIR/resolve_remote_cli_cached_missing.direct"

	set +e
	(
		zxfer_ensure_remote_host_capabilities() {
			cat <<'EOF'
ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	zstd	1	-
EOF
		}
		zxfer_resolve_remote_cli_tool_direct() {
			printf '%s\n' "direct-probe-called" >"$direct_log"
			return 1
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$output_file"
	)
	status=$?

	assertEquals "Generic remote CLI tool resolution should fail closed when the cached capability payload reports the helper missing." \
		1 "$status"
	assertContains "Generic remote CLI tool resolution should surface the documented secure-PATH guidance directly from the cached capability payload." \
		"$(cat "$output_file")" "Required dependency \"compression command\" not found on host origin.example in secure PATH"
	assertEquals "Generic remote CLI tool resolution should not fall back to a direct probe when the cached capability payload already reports the helper missing." \
		"" "$(cat "$direct_log" 2>/dev/null)"
}

test_zxfer_resolve_remote_cli_command_safe_rejects_blank_commands_and_surfaces_lookup_failures_in_current_shell() {
	blank_output="$TEST_TMPDIR/resolve_remote_cli_blank.out"
	lookup_output="$TEST_TMPDIR/resolve_remote_cli_lookup.out"

	(
		zxfer_resolve_remote_cli_command_safe "origin.example" "   " "compression command" source >"$blank_output"
	)
	blank_status=$?

	(
		zxfer_resolve_remote_cli_tool() {
			printf '%s\n' "remote helper lookup failed"
			return 1
		}
		zxfer_resolve_remote_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source >"$lookup_output"
	)
	lookup_status=$?

	assertEquals "Blank remote CLI commands should be rejected." 1 "$blank_status"
	assertContains "Blank remote CLI command failures should use the documented validation message." \
		"$(cat "$blank_output")" "Required dependency \"compression command\" must not be empty or whitespace-only."
	assertEquals "Remote CLI command resolution should fail when the head token cannot be resolved." 1 "$lookup_status"
	assertEquals "Remote CLI command resolution should surface the remote helper lookup failure verbatim." \
		"remote helper lookup failed" "$(cat "$lookup_output")"
}

test_zxfer_extract_remote_cli_command_head_surfaces_split_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/extract_remote_cli_head_failure.out"

	(
		zxfer_extract_remote_cli_command_head '"/opt/parallel dir/parallel" --jobs 4' "parallel command" >"$output_file"
	)
	status=$?

	assertEquals "Remote CLI head extraction should fail when the configured command relies on shell quoting." \
		1 "$status"
	assertContains "Remote CLI head extraction should preserve the splitter diagnostic." \
		"$(cat "$output_file")" "parallel command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_zxfer_resolve_remote_cli_command_safe_surfaces_split_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_split_failure.out"

	(
		zxfer_resolve_remote_cli_command_safe \
			"origin.example" \
			'"/opt/zstd dir/zstd" -T0 -9' \
			"compression command" \
			source >"$output_file"
	)
	status=$?

	assertEquals "Remote CLI command resolution should fail when the configured command relies on shell quoting." \
		1 "$status"
	assertContains "Remote CLI command resolution should preserve splitter diagnostics before remote lookup begins." \
		"$(cat "$output_file")" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_init_variables_resolves_remote_compression_helpers() {
	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "/remote/target/zfs"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				if [ "$1:$2" = "origin.example:zstd -T0 -9" ]; then
					printf '%s\n' "'/remote/origin/zstd' '-T0' '-9'"
				elif [ "$1:$2" = "target.example:zstd -d" ]; then
					printf '%s\n' "'/remote/target/zstd' '-d'"
				else
					printf '%s\n' "unexpected compression command"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
			printf 'origin-compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'origin-decompress=%s\n' "$g_origin_cmd_decompress_safe"
			printf 'target-compress=%s\n' "$g_target_cmd_compress_safe"
			printf 'target-decompress=%s\n' "$g_target_cmd_decompress_safe"
		)
	)

	assertContains "Origin initialization should resolve the remote compression helper." \
		"$result" "origin-compress='/remote/origin/zstd' '-T0' '-9'"
	assertContains "Origin initialization should leave the unused remote decompression helper on the local safe default." \
		"$result" "origin-decompress='/local/bin/zstd' '-d'"
	assertContains "Target initialization should leave the unused remote compression helper on the local safe default." \
		"$result" "target-compress='/local/bin/zstd' '-T0' '-9'"
	assertContains "Target initialization should resolve the remote decompression helper." \
		"$result" "target-decompress='/remote/target/zstd' '-d'"
}

test_init_variables_marks_remote_compression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "remote compression lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote compression lookup failures should abort initialization." 1 "$status"
	assertContains "Remote compression lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote compression lookup failures should preserve the failing message." \
		"$output" "msg=remote compression lookup failed"
}

test_init_variables_marks_remote_target_zfs_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "target zfs lookup failed"
					return 1
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Target-side remote zfs lookup failures should abort initialization." 1 "$status"
	assertContains "Target-side remote zfs lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Target-side remote zfs lookup failures should preserve the failing message." \
		"$output" "msg=target zfs lookup failed"
}

test_init_variables_marks_remote_source_os_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote source OS lookup failures should abort initialization." 1 "$status"
	assertContains "Remote source OS lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote source OS lookup failures should use the documented host-scoped message." \
		"$output" "msg=Failed to determine operating system on host origin.example."
}

test_init_variables_marks_remote_destination_os_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				if [ "$1" = "target.example" ]; then
					return 1
				fi
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/resolved/$2"
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote destination OS lookup failures should abort initialization." 1 "$status"
	assertContains "Remote destination OS lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote destination OS lookup failures should use the documented host-scoped message." \
		"$output" "msg=Failed to determine operating system on host target.example."
}

test_init_variables_marks_remote_target_decompression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "/remote/target/zfs"
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "target decompression lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-3'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Target-side remote decompression lookup failures should abort initialization." 1 "$status"
	assertContains "Target-side remote decompression lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Target-side remote decompression lookup failures should preserve the failing message." \
		"$output" "msg=target decompression lookup failed"
}

test_init_variables_marks_remote_restore_cat_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "origin.example:cat" ]; then
					printf '%s\n' "remote cat lookup failed"
					return 1
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_e_restore_property_mode=1
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote restore-mode cat lookup failures should abort initialization." 1 "$status"
	assertContains "Remote restore-mode cat lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote restore-mode cat lookup failures should preserve the failing message." \
		"$output" "msg=remote cat lookup failed"
}

test_init_variables_skips_remote_dependency_validation_in_dry_run() {
	log="$TEST_TMPDIR/init_variables_dry_run.log"
	: >"$log"

	output=$(
		(
			LOG_FILE="$log"
			zxfer_get_os() {
				printf 'get_os %s\n' "$1" >>"$LOG_FILE"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf 'resolve-tool %s %s\n' "$1" "$2" >>"$LOG_FILE"
				printf '%s\n' "/remote/$2"
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf 'resolve-cli %s %s\n' "$1" "$2" >>"$LOG_FILE"
				printf '%s\n' "'/remote/zstd' '-d'"
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			g_option_z_compress=1
			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_option_e_restore_property_mode=1
			g_cmd_cat=""
			zxfer_init_variables
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
			printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
			printf 'origin_compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'target_decompress=%s\n' "$g_target_cmd_decompress_safe"
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertNotContains "Dry-run variable initialization should not probe the origin host operating system." \
		"$(cat "$log")" "get_os origin.example"
	assertNotContains "Dry-run variable initialization should not probe the target host operating system." \
		"$(cat "$log")" "get_os target.example"
	assertNotContains "Dry-run variable initialization should not resolve any remote helper paths." \
		"$(cat "$log")" "resolve-tool "
	assertNotContains "Dry-run variable initialization should not resolve any remote CLI helper commands." \
		"$(cat "$log")" "resolve-cli "
	assertContains "Dry-run variable initialization should explain that origin helper validation is skipped." \
		"$output" "Dry run: skipping live remote source helper validation."
	assertContains "Dry-run variable initialization should explain that target helper validation is skipped." \
		"$output" "Dry run: skipping live remote destination helper validation."
	assertContains "Dry-run restore initialization should explain that remote cat validation is skipped." \
		"$output" "Dry run: skipping live remote backup-restore helper validation."
	assertContains "Dry-run variable initialization should keep the unresolved origin zfs render helper." \
		"$output" "origin_zfs=/sbin/zfs"
	assertContains "Dry-run variable initialization should keep the unresolved target zfs render helper." \
		"$output" "target_zfs=/sbin/zfs"
	assertContains "Dry-run variable initialization should preserve the local safe compression command for rendering." \
		"$output" "origin_compress='/local/bin/zstd' '-T0' '-9'"
	assertContains "Dry-run variable initialization should preserve the local safe decompression command for rendering." \
		"$output" "target_decompress='/local/bin/zstd' '-d'"
	assertContains "Dry-run restore initialization should fall back to a plain cat helper name for rendering." \
		"$output" "cat=cat"
}

test_refresh_compression_commands_rejects_empty_compression_command() {
	set +e
	output=$(
		(
			zxfer_quote_cli_tokens() {
				if [ "$1" = "" ]; then
					printf '%s' ""
				else
					printf "'%s'\n" "$1"
				fi
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress=""
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when the configured compression command is empty." 2 "$status"
	assertContains "Empty compression commands should use the documented usage error." \
		"$output" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_whitespace_only_compression_command() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress="   "
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only compression commands as empty." 2 "$status"
	assertContains "Whitespace-only compression commands should use the documented usage error." \
		"$output" "Compression command (-Z) cannot be empty."
}

test_refresh_compression_commands_rejects_missing_decompress_command() {
	set +e
	output=$(
		(
			zxfer_quote_cli_tokens() {
				if [ "$1" = "zstd -3" ]; then
					printf '%s\n' "'zstd' '-3'"
				else
					printf '%s' ""
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress=""
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when no decompressor can be derived." 1 "$status"
	assertContains "Missing decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_refresh_compression_commands_rejects_whitespace_only_decompress_command() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="   "
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only decompression commands as missing." 1 "$status"
	assertContains "Whitespace-only decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_close_origin_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_origin.log"
	socket_dir="$TEST_TMPDIR/origin_socket_dir"
	mkdir -p "$socket_dir"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_origin_control_socket_dir="$socket_dir"

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Origin socket path should be cleared after closing." "" "$g_ssh_origin_control_socket"
	assertEquals "Origin socket directory should be cleared after closing." "" "$g_ssh_origin_control_socket_dir"
	assertFalse "Origin socket directory should be removed during cleanup." "[ -d \"$socket_dir\" ]"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$TEST_TMPDIR/origin.sock
-O
exit
origin.example
pfexec" "$(cat "$log")"
}

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

test_merge_path_allowlists_deduplicates_entries() {
	result=$(zxfer_merge_path_allowlists "/sbin:/bin:/usr/bin" "/bin:/usr/local/bin:/usr/bin")

	assertEquals "Merged PATH allowlists should keep first-seen ordering and drop duplicates." \
		"/sbin:/bin:/usr/bin:/usr/local/bin" "$result"
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

test_zxfer_ensure_ssh_control_socket_cache_dir_creates_secure_directory() {
	cache_dir=$(zxfer_ensure_ssh_control_socket_cache_dir)

	assertEquals "Shared ssh control socket cache directories should be created under TMPDIR for the current run." \
		"$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")" "$cache_dir"
	assertTrue "Shared ssh control socket cache directories should exist after creation." \
		"[ -d '$cache_dir' ]"
	assertEquals "Shared ssh control socket cache directories should be mode 0700." \
		"700" "$(zxfer_get_path_mode_octal "$cache_dir")"
}

test_zxfer_remote_host_cache_dir_paths_are_run_unique() {
	original_prefix=${g_zxfer_temp_prefix:-}
	g_zxfer_temp_prefix="zxfer.run-one"
	first_socket_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	first_capability_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	g_zxfer_temp_prefix="zxfer.run-two"
	second_socket_dir=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	second_capability_dir=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	g_zxfer_temp_prefix=$original_prefix

	assertNotEquals "ssh control-socket cache roots should differ across distinct run temp prefixes." \
		"$first_socket_dir" "$second_socket_dir"
	assertNotEquals "Remote capability cache roots should differ across distinct run temp prefixes." \
		"$first_capability_dir" "$second_capability_dir"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR_PHYSICAL/ssh_cache_effective_root"
	mkdir -p "$cache_root"

	cache_dir=$(
		(
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should succeed when the effective temp root is available." \
		0 "$status"
	assertEquals "Shared ssh control socket cache directories should use the validated effective temp root instead of raw TMPDIR." \
		"$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$cache_root")" "$cache_dir"
}

test_zxfer_get_ssh_control_socket_cache_dir_for_key_returns_current_root_when_short_enough() {
	cache_dir="$TEST_TMPDIR/ssh-cache-short-enough"
	log_file="$TEST_TMPDIR/ssh_cache_short_enough.log"
	: >"$log_file"
	output=$(
		(
			zxfer_ensure_ssh_control_socket_cache_dir() {
				if [ -n "${TMPDIR:-set}" ]; then
					printf '%s\n' "$cache_dir"
				else
					printf '%s\n' "fallback" >>"$log_file"
					printf '%s\n' "$TEST_TMPDIR/unexpected-fallback"
				fi
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 0
			}
			result=$(zxfer_get_ssh_control_socket_cache_dir_for_key "kshared")
			printf 'result=%s\n' "$result"
			printf 'fallback_calls=%s\n' "$(grep -c '^fallback$' "$log_file" 2>/dev/null || printf '0')"
		)
	)

	assertContains "Shared ssh control socket cache-dir lookup should return the current cache root immediately when the socket path is already short enough." \
		"$output" "result=$cache_dir"
	assertContains "Shared ssh control socket cache-dir lookup should not probe a shorter fallback root when the current root already fits." \
		"$output" "fallback_calls=0"
}

test_zxfer_get_ssh_control_socket_cache_dir_for_key_falls_back_to_shorter_default_root() {
	long_cache_dir="$TEST_TMPDIR/ssh-cache-long"
	short_cache_dir="$TEST_PRIVATE_DEFAULT_TMPDIR/ssh-cache-short"

	output=$(
		(
			TMPDIR="$TEST_TMPDIR/very-long-socket-root"
			export TMPDIR
			zxfer_ensure_ssh_control_socket_cache_dir() {
				if [ -n "${TMPDIR:-}" ]; then
					printf '%s\n' "$long_cache_dir"
				else
					printf '%s\n' "$short_cache_dir"
				fi
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				if [ "${1#"$long_cache_dir"/}" != "$1" ]; then
					return 1
				fi
				if [ "${1#"$short_cache_dir"/}" != "$1" ]; then
					return 0
				fi
				return 1
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key "kshared"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache-dir lookup should fall back to the shorter default temp root when the requested root would make the socket path too long." \
		0 "$status"
	assertEquals "Shared ssh control socket cache-dir lookup should return the shorter fallback cache root after dropping TMPDIR." \
		"$short_cache_dir" "$output"
}

test_zxfer_get_ssh_control_socket_cache_dir_for_key_fails_when_no_shorter_root_is_available() {
	cache_dir="$TEST_TMPDIR/ssh-cache-same"

	output=$(
		(
			zxfer_ensure_ssh_control_socket_cache_dir() {
				printf '%s\n' "$cache_dir"
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 1
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key "kshared"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache-dir lookup should fail closed when neither the requested nor the fallback cache root can satisfy the socket-length limit." \
		1 "$status"
	assertEquals "Failed shared ssh control socket cache-dir fallback should not emit a cache-root payload." \
		"" "$output"
}

test_zxfer_get_ssh_control_socket_cache_dir_for_key_fails_when_shorter_fallback_is_still_too_long() {
	long_cache_dir="$TEST_TMPDIR/ssh-cache-long"
	short_cache_dir="$TEST_PRIVATE_DEFAULT_TMPDIR/ssh-cache-short"

	output=$(
		(
			TMPDIR="$TEST_TMPDIR/very-long-socket-root"
			export TMPDIR
			zxfer_ensure_ssh_control_socket_cache_dir() {
				if [ -n "${TMPDIR:-}" ]; then
					printf '%s\n' "$long_cache_dir"
				else
					printf '%s\n' "$short_cache_dir"
				fi
			}
			zxfer_is_ssh_control_socket_entry_path_short_enough() {
				return 1
			}
			zxfer_get_ssh_control_socket_cache_dir_for_key "kshared"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache-dir lookup should fail closed when both the requested root and the shorter fallback still exceed the socket-length limit." \
		1 "$status"
	assertEquals "Shared ssh control socket cache-dir lookup should not emit a payload when both cache roots remain too long." \
		"" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR_PHYSICAL/remote_capability_effective_root"
	mkdir -p "$cache_root"

	cache_dir=$(
		(
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Remote capability cache dir creation should succeed when the effective temp root is available." \
		0 "$status"
	assertEquals "Remote capability cache directories should use the validated effective temp root instead of raw TMPDIR." \
		"$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$cache_root")" "$cache_dir"
}

test_get_ssh_cmd_for_host_prefers_matching_control_socket() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	assertEquals "Origin host ssh command should reuse the origin control socket." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/origin.sock'" "$(zxfer_get_ssh_cmd_for_host "origin.example")"
	assertEquals "Target host ssh command should reuse the target control socket." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' '-S' '$TEST_TMPDIR/target.sock'" "$(zxfer_get_ssh_cmd_for_host "target.example")"
	assertEquals "Unmatched hosts should use the base ssh command." \
		"'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes'" "$(zxfer_get_ssh_cmd_for_host "other.example")"
}

test_zxfer_ssh_control_socket_cache_key_tracks_transport_policy() {
	g_cmd_ssh="$FAKE_SSH_BIN"

	default_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ambient_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	unset ZXFER_SSH_USE_AMBIENT_CONFIG
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"
	pinned_key=$(zxfer_ssh_control_socket_cache_key "origin.example")

	assertNotEquals "Shared ssh control socket cache keys should change when zxfer falls back to ambient ssh policy." \
		"$default_key" "$ambient_key"
	assertNotEquals "Shared ssh control socket cache keys should change when the pinned known-hosts file changes." \
		"$default_key" "$pinned_key"
}

test_zxfer_ssh_control_socket_cache_key_rejects_invalid_transport_policy() {
	set +e
	output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_ssh_control_socket_cache_key "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache keys should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "Shared ssh control socket cache-key failures should preserve the underlying ssh policy validation message." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
	assertNotContains "Shared ssh control socket cache-key failures should not leak a partial ssh command path into the diagnostic." \
		"$output" "$FAKE_SSH_BIN"
	assertNotContains "Shared ssh control socket cache-key failures should not leak the internal managed-policy identity prefix into the diagnostic." \
		"$output" "managed"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_invalid_transport_policy() {
	set +e
	output=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_ensure_ssh_control_socket_entry_dir "origin.example"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket entry creation should fail closed when the managed ssh transport policy is invalid." \
		1 "$status"
	assertContains "Shared ssh control socket entry failures should preserve the underlying ssh policy validation message." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
	assertNotContains "Shared ssh control socket entry failures should not leak the internal managed-policy identity prefix into the diagnostic." \
		"$output" "managed"
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

test_setup_ssh_control_socket_rejects_invalid_transport_policy_before_lock_acquisition() {
	entry_dir="$TEST_TMPDIR/ssh_policy_precheck_entry"
	lock_dir="$entry_dir.lock"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				mkdir -p "$lock_dir"
				printf '%s\n' "$lock_dir"
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			ZXFER_SSH_BATCH_MODE=$(printf 'bad\nmode')
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when managed ssh policy validation fails before lock acquisition." \
		1 "$status"
	assertContains "ssh control socket setup should preserve the underlying ssh policy validation message during prevalidation." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
	assertFalse "ssh control socket setup should not leave a shared lock directory behind when transport-policy validation fails." \
		"[ -d '$lock_dir' ]"
}

test_close_target_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_target.log"
	socket_dir="$TEST_TMPDIR/target_socket_dir"
	mkdir -p "$socket_dir"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"
	g_ssh_target_control_socket_dir="$socket_dir"

	zxfer_close_target_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Target socket path should be cleared after closing." "" "$g_ssh_target_control_socket"
	assertEquals "Target socket directory should be cleared after closing." "" "$g_ssh_target_control_socket_dir"
	assertFalse "Target socket directory should be removed during cleanup." "[ -d \"$socket_dir\" ]"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$TEST_TMPDIR/target.sock
-O
exit
target.example
doas" "$(cat "$log")"
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
			zxfer_relaunch() {
				printf 'zxfer_relaunch need=%s\n' "$g_services_need_relaunch"
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when cleanup zxfer_relaunch succeeds." 0 "$status"
	assertContains "zxfer_trap_exit should log that it is restarting stopped services." \
		"$output" "zxfer exiting early; restarting stopped services."
	assertContains "zxfer_trap_exit should invoke zxfer_relaunch when services are still marked for restart." \
		"$output" "zxfer_relaunch need=1"
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
			zxfer_relaunch() {
				printf 'zxfer_relaunch-called\n'
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when zxfer_relaunch already failed earlier." 0 "$status"
	assertContains "zxfer_trap_exit should log that it is preserving stopped-service state after a failed zxfer_relaunch attempt." \
		"$output" "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
	assertNotContains "zxfer_trap_exit should not invoke zxfer_relaunch again while a failed zxfer_relaunch attempt is already in progress." \
		"$output" "zxfer_relaunch-called"
}

test_trap_exit_logs_when_relaunch_is_unavailable() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			unset -f zxfer_relaunch 2>/dev/null
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
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when zxfer_relaunch is unavailable." 0 "$status"
	assertContains "zxfer_trap_exit should log when stopped services cannot be restarted because zxfer_relaunch() is missing." \
		"$output" "zxfer exiting with services still stopped; zxfer_relaunch() unavailable."
}

test_trap_exit_removes_temp_files_and_iteration_cache_dirs() {
	g_zxfer_temp_prefix="trap-cleanup"
	socket_cache_root=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	remote_capability_cache_root=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=0
			g_delete_source_tmp_file="$TEST_TMPDIR/delete-source.tmp"
			g_delete_dest_tmp_file="$TEST_TMPDIR/delete-dest.tmp"
			g_delete_snapshots_to_delete_tmp_file="$TEST_TMPDIR/delete-diff.tmp"
			: >"$g_delete_source_tmp_file"
			: >"$g_delete_dest_tmp_file"
			: >"$g_delete_snapshots_to_delete_tmp_file"
			g_zxfer_temp_prefix="trap-cleanup"
			: >"$TEST_TMPDIR/trap-cleanup.stale"
			mkdir -p "$TEST_TMPDIR/trap-cleanup.dir/subdir"
			: >"$TEST_TMPDIR/trap-cleanup.dir/subdir/stale"
			g_zxfer_property_cache_dir="$TEST_TMPDIR/property-cache"
			g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index"
			mkdir -p \
				"$g_zxfer_property_cache_dir" \
				"$g_zxfer_snapshot_index_dir" \
				"$socket_cache_root/active-entry" \
				"$remote_capability_cache_root/active-entry"
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

	assertEquals "zxfer_trap_exit should preserve a successful exit status after cleaning temp files and cache directories." 0 "$status"
	assertFalse "zxfer_trap_exit should remove the delete-source temp file." "[ -e '$TEST_TMPDIR/delete-source.tmp' ]"
	assertFalse "zxfer_trap_exit should remove the delete-destination temp file." "[ -e '$TEST_TMPDIR/delete-dest.tmp' ]"
	assertFalse "zxfer_trap_exit should remove the delete-diff temp file." "[ -e '$TEST_TMPDIR/delete-diff.tmp' ]"
	assertFalse "zxfer_trap_exit should remove prefixed tmpdir scratch files for the current run." "[ -e '$TEST_TMPDIR/trap-cleanup.stale' ]"
	assertFalse "zxfer_trap_exit should remove prefixed tmpdir scratch directories for the current run." "[ -d '$TEST_TMPDIR/trap-cleanup.dir' ]"
	assertFalse "zxfer_trap_exit should remove the property cache directory." "[ -d '$TEST_TMPDIR/property-cache' ]"
	assertFalse "zxfer_trap_exit should remove the snapshot index directory." "[ -d '$TEST_TMPDIR/snapshot-index' ]"
	assertFalse "zxfer_trap_exit should remove the current run ssh control-socket cache root even when it still contains entry state." "[ -d '$socket_cache_root' ]"
	assertFalse "zxfer_trap_exit should remove the current run remote capability cache root even when it still contains cache files." "[ -d '$remote_capability_cache_root' ]"
}

test_trap_exit_preserves_remote_host_cache_roots_with_unsupported_entries() {
	g_zxfer_temp_prefix="zxfer.trap-cleanup"
	socket_cache_root=$(zxfer_ssh_control_socket_cache_dir_path_for_tmpdir "$TEST_TMPDIR")
	remote_capability_cache_root=$(zxfer_remote_capability_cache_dir_path_for_tmpdir "$TEST_TMPDIR")

	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=0
			mkdir -p "$socket_cache_root/entry/leases" "$remote_capability_cache_root/cache.lock" ||
				exit 91
			chmod 700 "$remote_capability_cache_root/cache.lock" || exit 92
			printf '%s\n' "$$" >"$remote_capability_cache_root/cache.lock/pid" || exit 93
			chmod 600 "$remote_capability_cache_root/cache.lock/pid" || exit 94
			: >"$socket_cache_root/entry/leases/lease.legacy"
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

	assertEquals "zxfer_trap_exit should preserve a successful exit status while leaving unsupported remote-host cache entries in place." \
		0 "$status"
	assertTrue "zxfer_trap_exit should preserve the current run ssh control-socket cache root when it contains an unsupported plain-file lease entry." \
		"[ -d '$socket_cache_root' ]"
	assertTrue "zxfer_trap_exit should preserve the current run remote capability cache root when it contains an unsupported pid-file lock layout." \
		"[ -d '$remote_capability_cache_root' ]"
}

test_trap_exit_removes_non_remote_host_temp_dirs_with_legacy_like_entries() {
	g_zxfer_temp_prefix="zxfer.trap-cleanup"
	fake_root="$TEST_TMPDIR/zxfer.trap-cleanup.fake-root"

	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=0
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

	assertEquals "zxfer_trap_exit should preserve a successful exit status while removing non-cache temp directories with legacy-like child names." \
		0 "$status"
	assertFalse "zxfer_trap_exit should not preserve arbitrary current-run temp directories just because they contain lease-like or pid-lock-like names." \
		"[ -e '$fake_root' ]"
}

test_setup_ssh_control_socket_replaces_existing_target_socket_state() {
	log="$TEST_TMPDIR/setup_target.log"
	FAKE_SSH_LOG="$log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	result=$(
		(
			zxfer_close_target_ssh_control_socket() {
				printf 'closed\n'
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_target_control_socket="$TEST_TMPDIR/old_target.sock"
			g_ssh_target_control_socket_dir="$TEST_TMPDIR/old_target_dir"
			zxfer_setup_ssh_control_socket "target.example doas" "target"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'dir=%s\n' "$g_ssh_target_control_socket_dir"
		)
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertContains "Replacing an existing target control socket should close the old socket first." \
		"$result" "closed"
	assertContains "Target socket setup should store the new control socket path." "$result" "socket="
	assertContains "Target socket setup should store the new control socket directory." "$result" "dir="
	assertEquals "New target control socket setup should preserve host token boundaries for ssh." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-M
-S
$(printf '%s\n' "$result" | awk -F= '/^socket=/{print $2}')
-fN
target.example
doas" "$(cat "$log")"
}

test_setup_ssh_control_socket_reuses_live_cached_socket_without_opening_new_master() {
	log="$TEST_TMPDIR/setup_cached_socket.log"
	: >"$log"
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example pfexec")
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$cache_key")
	expected_entry_dir="$cache_dir/$cache_key"

	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_open_ssh_control_socket_for_host() {
				printf 'open\n' >>"$log"
				return 0
			}
			zxfer_setup_ssh_control_socket "origin.example pfexec" "origin"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'dir=%s\n' "$g_ssh_origin_control_socket_dir"
			printf 'lease=%s\n' "$g_ssh_origin_control_socket_lease_file"
		)
	)

	assertEquals "Reusing a live cached control socket should not start a second ssh master." "" "$(cat "$log")"
	assertContains "Cached control socket reuse should still publish the socket path for the origin host." \
		"$result" "socket=$expected_entry_dir/s"
	assertContains "Cached control socket reuse should still publish the shared cache entry directory." \
		"$result" "dir=$expected_entry_dir"
	assertContains "Cached control socket reuse should register a per-process lease directory." \
		"$result" "lease=$expected_entry_dir/leases/lease."
}

test_setup_ssh_control_socket_reports_cache_dir_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the shared cache entry dir cannot be prepared." 1 "$status"
	assertContains "ssh control socket setup should preserve the current cache-dir failure message." \
		"$output" "Error creating temporary directory for ssh control socket."
}

test_setup_ssh_control_socket_reports_lock_failures() {
	entry_dir="$TEST_TMPDIR/ssh_lock_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the shared lock cannot be acquired." 1 "$status"
	assertContains "ssh control socket setup should preserve the current lock-failure message." \
		"$output" "Error creating ssh control socket for origin host."
}

test_setup_ssh_control_socket_surfaces_specific_lock_failure_message() {
	entry_dir="$TEST_TMPDIR/ssh_lock_specific_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_error="Timed out waiting for ssh control socket lock path \"$entry_dir.lock\"."
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should still fail closed when the shared lock cannot be acquired." 1 "$status"
	assertContains "ssh control socket setup should surface the specific shared-lock failure detail when it is available." \
		"$output" "Error creating ssh control socket for origin host: Timed out waiting for ssh control socket lock path \"$entry_dir.lock\"."
}

test_setup_ssh_control_socket_reports_master_open_failures() {
	entry_dir="$TEST_TMPDIR/ssh_open_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$entry_dir.lock"
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_open_ssh_control_socket_for_host() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the ssh master cannot be opened." 1 "$status"
	assertContains "ssh control socket setup should preserve the current master-open failure message." \
		"$output" "Error creating ssh control socket for origin host."
}

test_setup_ssh_control_socket_reports_lease_creation_failures() {
	entry_dir="$TEST_TMPDIR/ssh_lease_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$entry_dir.lock"
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when a process lease cannot be created." 1 "$status"
	assertContains "ssh control socket setup should preserve the current lease-creation failure message." \
		"$output" "Error creating ssh control socket for origin host."
}

test_setup_ssh_control_socket_cleans_up_fresh_master_when_lease_creation_fails() {
	entry_dir="$TEST_TMPDIR/ssh_lease_fail_cleanup_entry"
	cleanup_log="$TEST_TMPDIR/ssh_lease_fail_cleanup.log"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$entry_dir.lock"
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_open_ssh_control_socket_for_host() {
				printf '%s\n' "open:$2" >>"$cleanup_log"
				return 0
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				printf '%s\n' "$3:$2" >>"$cleanup_log"
				g_zxfer_ssh_control_socket_action_result="closed"
				g_zxfer_ssh_control_socket_action_command="ssh -S $2 -O $3 $1"
				return 0
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				printf '%s\n' "cleanup:$1" >>"$cleanup_log"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "release:$1" >>"$cleanup_log"
				return 0
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when a fresh master loses its lease creation race." \
		1 "$status"
	assertContains "ssh control socket setup should keep the current lease-creation failure message after cleaning up a fresh master." \
		"$output" "Error creating ssh control socket for origin host."
	assertEquals "ssh control socket setup should close and reap a freshly opened master before releasing the shared lock." \
		"open:$entry_dir/s
exit:$entry_dir/s
cleanup:$entry_dir
release:$entry_dir.lock" "$(cat "$cleanup_log")"
}

test_setup_ssh_control_socket_reports_lock_release_failures_after_shared_setup() {
	entry_dir="$TEST_TMPDIR/ssh_release_fail_entry"
	mkdir -p "$entry_dir/leases"
	restore_errexit=0
	case $- in
	*e*)
		restore_errexit=1
		;;
	esac

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$entry_dir.lock"
				return 0
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_create_ssh_control_socket_lease_file() {
				g_zxfer_runtime_artifact_path_result="$entry_dir/leases/lease.test"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				zxfer_note_ssh_control_socket_lock_error \
					"Failed to release ssh control socket lock path \"$1\"."
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		) 2>&1
	)
	status=$?
	if [ "$restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "ssh control socket setup should fail closed when the shared lock cannot be released after lease creation succeeds." \
		1 "$status"
	assertContains "ssh control socket setup should report the checked shared-lock release failure." \
		"$output" "Error releasing ssh control socket lock for origin host: Failed to release ssh control socket lock path \"$entry_dir.lock\"."
	assertContains "ssh control socket setup should keep the current top-level setup failure message when lock release fails." \
		"$output" "Error creating ssh control socket for origin host."
}

test_zxfer_create_ssh_control_socket_lease_file_registers_owned_lock_cleanup_path() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")

	zxfer_create_ssh_control_socket_lease_file "$entry_dir" >/dev/null
	lease_file=$g_zxfer_runtime_artifact_path_result

	assertTrue "ssh control socket lease creation should produce a lease directory." \
		"[ -d \"$lease_file\" ]"
	assertContains "ssh control socket lease creation should register the lease directory for owned-lock cleanup." \
		"${g_zxfer_owned_lock_cleanup_paths:-}" "$lease_file"
}

test_close_origin_ssh_control_socket_preserves_shared_socket_when_other_leases_exist() {
	log="$TEST_TMPDIR/close_origin_shared.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	other_lease=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG
	if [ -e "$lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertEquals "Shared origin sockets should clear the in-process socket path after releasing the local lease." "" \
		"$g_ssh_origin_control_socket"
	assertEquals "Shared origin sockets should clear the in-process lease path after releasing the local lease." "" \
		"$g_ssh_origin_control_socket_lease_file"
	assertEquals "Closing a shared origin socket should remove only the current process lease when other leases remain." \
		0 "$lease_exists"
	assertEquals "Closing a shared origin socket should preserve the cache entry while sibling leases remain." \
		1 "$entry_dir_exists"
	assertEquals "Closing a shared origin socket should not send ssh -O exit while sibling leases remain." "" \
		"$(cat "$log" 2>/dev/null)"
}

test_close_origin_ssh_control_socket_reports_lock_release_failure_after_other_leases_remain() {
	errlog="$TEST_TMPDIR/close_origin_shared_release.err"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	sibling_lease=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"
	restore_errexit=0
	case $- in
	*e*)
		restore_errexit=1
		;;
	esac

	set +e
	output=$(
		(
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-shared-release.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				zxfer_note_ssh_control_socket_lock_error \
					"Failed to release ssh control socket lock path \"$1\"."
				return 1
			}
			zxfer_close_origin_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "${g_ssh_origin_control_socket:-}"
			printf 'lease=%s\n' "${g_ssh_origin_control_socket_lease_file:-}"
		)
	)
	if [ "$restore_errexit" -eq 1 ]; then
		set -e
	fi
	stderr_contents=$(cat "$errlog" 2>/dev/null || :)
	if [ -e "$lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi
	if [ -d "$sibling_lease" ]; then
		sibling_exists=1
	else
		sibling_exists=0
	fi
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertContains "Origin-socket close should fail closed when the shared lock cannot be released after sibling lease cleanup." \
		"$output" "status=1"
	assertContains "Origin-socket close should clear the socket path after the local lease is removed even when lock release fails." \
		"$output" "socket="
	assertContains "Origin-socket close should clear the lease path after the local lease is removed even when lock release fails." \
		"$output" "lease="
	assertContains "Origin-socket close should surface the checked shared-lock release failure." \
		"$stderr_contents" "Error releasing ssh control socket lock for origin host: Failed to release ssh control socket lock path \"$TEST_TMPDIR/origin-shared-release.lock\"."
	assertEquals "Origin-socket close should still remove the current process lease before reporting the shared-lock release failure." \
		0 "$lease_exists"
	assertEquals "Origin-socket close should preserve sibling leases while reporting a shared-lock release failure." \
		1 "$sibling_exists"
	assertEquals "Origin-socket close should preserve the cache entry while sibling leases remain." \
		1 "$entry_dir_exists"
}

test_close_origin_ssh_control_socket_preserves_lease_cleanup_failures() {
	errlog="$TEST_TMPDIR/close_origin_lease_cleanup.err"
	release_log="$TEST_TMPDIR/close_origin_lease_cleanup.release"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"

	set +e
	output=$(
		(
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_release_ssh_control_socket_lease_file() {
				return 73
			}
			zxfer_close_origin_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'lease=%s\n' "$g_ssh_origin_control_socket_lease_file"
		)
	)
	set -e

	assertContains "Origin-socket close should preserve the lease cleanup failure status." \
		"$output" "status=73"
	assertContains "Origin-socket close should preserve the in-process socket path when lease cleanup fails." \
		"$output" "socket=$entry_dir/s"
	assertContains "Origin-socket close should preserve the in-process lease path when lease cleanup fails." \
		"$output" "lease=$lease_file"
	assertContains "Origin-socket close should surface a specific lease cleanup error." \
		"$(cat "$errlog")" "Error removing ssh control socket lease for origin host."
	assertEquals "Origin-socket close should still release the per-entry lock after lease cleanup failures." \
		"$TEST_TMPDIR/origin-lock" "$(cat "$release_log")"
}

test_close_origin_ssh_control_socket_preserves_lease_count_failures() {
	errlog="$TEST_TMPDIR/close_origin_lease_count.err"
	release_log="$TEST_TMPDIR/close_origin_lease_count.release"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"

	set +e
	output=$(
		(
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/origin-count-lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				return 0
			}
			zxfer_count_ssh_control_socket_leases() {
				zxfer_note_ssh_control_socket_lock_error \
					"Unable to inspect ssh control socket lease entry \"$entry_dir/leases/lease.bad\"."
				return 97
			}
			zxfer_close_origin_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'lease=%s\n' "$g_ssh_origin_control_socket_lease_file"
		)
	)
	set -e

	assertContains "Origin-socket close should preserve lease-count failure statuses." \
		"$output" "status=97"
	assertContains "Origin-socket close should preserve the socket path when sibling lease inspection fails." \
		"$output" "socket=$entry_dir/s"
	assertContains "Origin-socket close should preserve the lease path in process state when sibling lease inspection fails." \
		"$output" "lease=$lease_file"
	assertContains "Origin-socket close should surface the failing sibling lease path when lease counting fails." \
		"$(cat "$errlog")" "Error counting ssh control socket lease entries for origin host: Unable to inspect ssh control socket lease entry \"$entry_dir/leases/lease.bad\"."
	assertEquals "Origin-socket close should still release the per-entry lock when sibling lease inspection fails." \
		"$TEST_TMPDIR/origin-count-lock" "$(cat "$release_log")"
}

test_close_origin_ssh_control_socket_surfaces_specific_lock_failure_message() {
	errlog="$TEST_TMPDIR/close_origin_lock_specific.err"

	set +e
	output=$(
		(
			lock_dir="$TEST_TMPDIR/origin-lock"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
			g_ssh_origin_control_socket_dir="$TEST_TMPDIR/origin-entry"
			g_ssh_origin_control_socket_lease_file="$TEST_TMPDIR/origin.lease"

			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_error="Existing ssh control socket lock path \"$lock_dir\" has unsupported permissions (777). Remove the stale lock directory and retry."
				return 1
			}

			zxfer_close_origin_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertContains "Origin-socket close should fail closed when the shared lock cannot be reacquired." \
		"$output" "status=1"
	assertContains "Origin-socket close should preserve the host context when surfacing a specific shared-lock failure." \
		"$(cat "$errlog")" "Error acquiring ssh control socket lock for origin host: Existing ssh control socket lock path \"$TEST_TMPDIR/origin-lock\" has unsupported permissions (777). Remove the stale lock directory and retry."
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

test_close_origin_ssh_control_socket_closes_shared_socket_when_last_lease_exits() {
	log="$TEST_TMPDIR/close_origin_last_lease.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example pfexec")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		return 0
	}

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertEquals "Last shared origin-socket lease release should clear the in-process socket path." "" \
		"$g_ssh_origin_control_socket"
	assertEquals "Last shared origin-socket lease release should clear the in-process lease path." "" \
		"$g_ssh_origin_control_socket_lease_file"
	assertEquals "Last shared origin-socket lease release should remove the shared cache entry after ssh exits." \
		0 "$entry_dir_exists"
	assertEquals "Last shared origin-socket lease release should close the shared ssh master with preserved host tokens." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$entry_dir/s
-O
exit
origin.example
pfexec" "$(cat "$log")"
}

test_close_origin_ssh_control_socket_preserves_cache_dir_cleanup_failures() {
	errlog="$TEST_TMPDIR/close_origin_dir_cleanup.err"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"

	set +e
	output=$(
		(
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="closed"
				return 0
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 83
			}

			zxfer_close_origin_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'dir=%s\n' "$g_ssh_origin_control_socket_dir"
			printf 'lease=%s\n' "$g_ssh_origin_control_socket_lease_file"
		)
	)
	status=$?
	set -e
	if [ -d "$entry_dir" ]; then
		dir_exists=1
	else
		dir_exists=0
	fi
	if [ -e "$lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi

	assertEquals "Origin-socket cache-dir cleanup failure probes should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Origin-socket cache-dir cleanup failures should preserve the exact local cleanup status." \
		"$output" "status=83"
	assertContains "Origin-socket cache-dir cleanup failures should preserve the socket path for retry." \
		"$output" "socket=$entry_dir/s"
	assertContains "Origin-socket cache-dir cleanup failures should preserve the cache directory for retry." \
		"$output" "dir=$entry_dir"
	assertContains "Origin-socket cache-dir cleanup failures should preserve the lease path in process state." \
		"$output" "lease=$lease_file"
	assertContains "Origin-socket cache-dir cleanup failures should emit a specific cleanup diagnostic." \
		"$(cat "$errlog")" "Error removing ssh control socket cache directory for origin host."
	assertEquals "Origin-socket cache-dir cleanup failures should leave the shared cache entry on disk for retry." \
		1 "$dir_exists"
	assertEquals "Origin-socket cache-dir cleanup failures should still remove the lease directory before reporting the local cleanup error." \
		0 "$lease_exists"
}

test_close_target_ssh_control_socket_closes_shared_socket_when_last_lease_exits() {
	log="$TEST_TMPDIR/close_target_shared.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example doas")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		return 0
	}

	zxfer_close_target_ssh_control_socket

	unset FAKE_SSH_LOG
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertEquals "Last shared target-socket lease release should clear the in-process socket path." "" \
		"$g_ssh_target_control_socket"
	assertEquals "Last shared target-socket lease release should clear the in-process lease path." "" \
		"$g_ssh_target_control_socket_lease_file"
	assertEquals "Last shared target-socket lease release should remove the shared cache entry after ssh exits." \
		0 "$entry_dir_exists"
	assertEquals "Last shared target-socket lease release should close the shared ssh master with preserved host tokens." \
		"-o
BatchMode=yes
-o
StrictHostKeyChecking=yes
-S
$entry_dir/s
-O
exit
target.example
doas" "$(cat "$log")"
}

test_close_target_ssh_control_socket_preserves_shared_socket_when_other_leases_exist() {
	log="$TEST_TMPDIR/close_target_shared_other.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	sibling_lease=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"

	set +e
	zxfer_close_target_ssh_control_socket
	close_status=$?
	set -e

	unset FAKE_SSH_LOG
	if [ -e "$lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi
	if [ -d "$sibling_lease" ]; then
		sibling_exists=1
	else
		sibling_exists=0
	fi
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertEquals "Closing a shared target socket with sibling leases should succeed." \
		0 "$close_status"
	assertEquals "Shared target sockets should clear the in-process socket path after releasing the local lease." "" \
		"$g_ssh_target_control_socket"
	assertEquals "Shared target sockets should clear the in-process lease path after releasing the local lease." "" \
		"$g_ssh_target_control_socket_lease_file"
	assertEquals "Closing a shared target socket should remove only the current process lease when other leases remain." \
		0 "$lease_exists"
	assertEquals "Closing a shared target socket should preserve sibling leases while the shared master remains active." \
		1 "$sibling_exists"
	assertEquals "Closing a shared target socket should preserve the cache entry while sibling leases remain." \
		1 "$entry_dir_exists"
	assertEquals "Closing a shared target socket should not send ssh -O exit while sibling leases remain." "" \
		"$(cat "$log" 2>/dev/null || :)"
}

test_close_target_ssh_control_socket_reports_lock_release_failure_after_other_leases_remain() {
	errlog="$TEST_TMPDIR/close_target_shared_release.err"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	sibling_lease=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"
	restore_errexit=0
	case $- in
	*e*)
		restore_errexit=1
		;;
	esac

	set +e
	output=$(
		(
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-shared-release.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				zxfer_note_ssh_control_socket_lock_error \
					"Failed to release ssh control socket lock path \"$1\"."
				return 1
			}
			zxfer_close_target_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "${g_ssh_target_control_socket:-}"
			printf 'lease=%s\n' "${g_ssh_target_control_socket_lease_file:-}"
		)
	)
	if [ "$restore_errexit" -eq 1 ]; then
		set -e
	fi
	stderr_contents=$(cat "$errlog" 2>/dev/null || :)
	if [ -e "$lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi
	if [ -d "$sibling_lease" ]; then
		sibling_exists=1
	else
		sibling_exists=0
	fi
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertContains "Target-socket close should fail closed when the shared lock cannot be released after sibling lease cleanup." \
		"$output" "status=1"
	assertContains "Target-socket close should clear the socket path after the local lease is removed even when lock release fails." \
		"$output" "socket="
	assertContains "Target-socket close should clear the lease path after the local lease is removed even when lock release fails." \
		"$output" "lease="
	assertContains "Target-socket close should surface the checked shared-lock release failure." \
		"$stderr_contents" "Error releasing ssh control socket lock for target host: Failed to release ssh control socket lock path \"$TEST_TMPDIR/target-shared-release.lock\"."
	assertEquals "Target-socket close should still remove the current process lease before reporting the shared-lock release failure." \
		0 "$lease_exists"
	assertEquals "Target-socket close should preserve sibling leases while reporting a shared-lock release failure." \
		1 "$sibling_exists"
	assertEquals "Target-socket close should preserve the cache entry while sibling leases remain." \
		1 "$entry_dir_exists"
}

test_close_target_ssh_control_socket_preserves_cache_dir_cleanup_failures() {
	errlog="$TEST_TMPDIR/close_target_dir_cleanup.err"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"

	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="closed"
				return 0
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 89
			}

			zxfer_close_target_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'dir=%s\n' "$g_ssh_target_control_socket_dir"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
		)
	)
	status=$?
	set -e
	if [ -d "$entry_dir" ]; then
		dir_exists=1
	else
		dir_exists=0
	fi
	if [ -e "$lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi

	assertEquals "Target-socket cache-dir cleanup failure probes should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Target-socket cache-dir cleanup failures should preserve the exact local cleanup status." \
		"$output" "status=89"
	assertContains "Target-socket cache-dir cleanup failures should preserve the socket path for retry." \
		"$output" "socket=$entry_dir/s"
	assertContains "Target-socket cache-dir cleanup failures should preserve the cache directory for retry." \
		"$output" "dir=$entry_dir"
	assertContains "Target-socket cache-dir cleanup failures should preserve the lease path in process state." \
		"$output" "lease=$lease_file"
	assertContains "Target-socket cache-dir cleanup failures should emit a specific cleanup diagnostic." \
		"$(cat "$errlog")" "Error removing ssh control socket cache directory for target host."
	assertEquals "Target-socket cache-dir cleanup failures should leave the shared cache entry on disk for retry." \
		1 "$dir_exists"
	assertEquals "Target-socket cache-dir cleanup failures should still remove the lease directory before reporting the local cleanup error." \
		0 "$lease_exists"
}

test_close_target_ssh_control_socket_preserves_lease_cleanup_failures() {
	errlog="$TEST_TMPDIR/close_target_lease_cleanup.err"
	release_log="$TEST_TMPDIR/close_target_lease_cleanup.release"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"

	set +e
	output=$(
		(
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_release_ssh_control_socket_lease_file() {
				return 79
			}
			zxfer_close_target_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
		)
	)
	set -e

	assertContains "Target-socket close should preserve the lease cleanup failure status." \
		"$output" "status=79"
	assertContains "Target-socket close should preserve the in-process socket path when lease cleanup fails." \
		"$output" "socket=$entry_dir/s"
	assertContains "Target-socket close should preserve the in-process lease path when lease cleanup fails." \
		"$output" "lease=$lease_file"
	assertContains "Target-socket close should surface a specific lease cleanup error." \
		"$(cat "$errlog")" "Error removing ssh control socket lease for target host."
	assertEquals "Target-socket close should still release the per-entry lock after lease cleanup failures." \
		"$TEST_TMPDIR/target-lock" "$(cat "$release_log")"
}

test_close_target_ssh_control_socket_preserves_lease_count_failures() {
	errlog="$TEST_TMPDIR/close_target_lease_count.err"
	release_log="$TEST_TMPDIR/close_target_lease_count.release"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"

	set +e
	output=$(
		(
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-count-lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				printf '%s\n' "$1" >>"$release_log"
			}
			zxfer_prune_stale_ssh_control_socket_leases() {
				return 0
			}
			zxfer_count_ssh_control_socket_leases() {
				zxfer_note_ssh_control_socket_lock_error \
					"Unable to inspect ssh control socket lease entry \"$entry_dir/leases/lease.bad\"."
				return 98
			}
			zxfer_close_target_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
		)
	)
	set -e

	assertContains "Target-socket close should preserve lease-count failure statuses." \
		"$output" "status=98"
	assertContains "Target-socket close should preserve the socket path when sibling lease inspection fails." \
		"$output" "socket=$entry_dir/s"
	assertContains "Target-socket close should preserve the lease path in process state when sibling lease inspection fails." \
		"$output" "lease=$lease_file"
	assertContains "Target-socket close should surface the failing sibling lease path when lease counting fails." \
		"$(cat "$errlog")" "Error counting ssh control socket lease entries for target host: Unable to inspect ssh control socket lease entry \"$entry_dir/leases/lease.bad\"."
	assertEquals "Target-socket close should still release the per-entry lock when sibling lease inspection fails." \
		"$TEST_TMPDIR/target-count-lock" "$(cat "$release_log")"
}

test_close_target_ssh_control_socket_surfaces_specific_lock_failure_message() {
	errlog="$TEST_TMPDIR/close_target_lock_specific.err"

	set +e
	output=$(
		(
			lock_dir="$TEST_TMPDIR/target-lock"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"
			g_ssh_target_control_socket_dir="$TEST_TMPDIR/target-entry"
			g_ssh_target_control_socket_lease_file="$TEST_TMPDIR/target.lease"

			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_error="Existing ssh control socket lock path \"$lock_dir\" has unsupported permissions (777). Remove the stale lock directory and retry."
				return 1
			}

			zxfer_close_target_ssh_control_socket 2>"$errlog"
			printf 'status=%s\n' "$?"
		)
	)
	set -e

	assertContains "Target-socket close should fail closed when the shared lock cannot be reacquired." \
		"$output" "status=1"
	assertContains "Target-socket close should preserve the host context when surfacing a specific shared-lock failure." \
		"$(cat "$errlog")" "Error acquiring ssh control socket lock for target host: Existing ssh control socket lock path \"$TEST_TMPDIR/target-lock\" has unsupported permissions (777). Remove the stale lock directory and retry."
}

test_close_origin_ssh_control_socket_preserves_state_on_transport_failure_without_lease() {
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

	set +e
	zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_transport.out" 2>"$errlog"
	status=$?
	set -e

	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
	if [ -d "$socket_dir" ]; then
		socket_dir_exists=1
	else
		socket_dir_exists=0
	fi

	assertEquals "Direct origin-socket closes should fail when ssh transport shutdown fails." 1 "$status"
	assertContains "Direct origin-socket close failures should preserve the ssh transport diagnostic." \
		"$(cat "$errlog")" "Host key verification failed."
	assertEquals "Direct origin-socket close failures should preserve the cache directory for retry." \
		1 "$socket_dir_exists"
	assertEquals "Direct origin-socket close failures should preserve the in-process socket path." \
		"$TEST_TMPDIR/origin_transport.sock" "$g_ssh_origin_control_socket"
	assertEquals "Direct origin-socket close failures should preserve the in-process socket dir." \
		"$socket_dir" "$g_ssh_origin_control_socket_dir"
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

	set +e
	zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_shared_transport.out" 2>"$errlog"
	status=$?
	set -e

	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi
	if [ -d "$g_ssh_origin_control_socket_lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi

	assertEquals "Last shared origin-socket lease release should fail closed on ssh transport errors." 1 "$status"
	assertContains "Last shared origin-socket lease failures should preserve the ssh transport diagnostic." \
		"$(cat "$errlog")" "Host key verification failed."
	assertEquals "Last shared origin-socket lease failures should preserve the cache entry for retry." \
		1 "$entry_dir_exists"
	assertEquals "Last shared origin-socket lease failures should restore an active lease directory for retry." \
		1 "$lease_exists"
	assertNotEquals "Restored origin-socket leases should not reuse the removed lease path." \
		"$lease_file" "$g_ssh_origin_control_socket_lease_file"
	assertEquals "Last shared origin-socket lease failures should preserve the in-process socket path." \
		"$entry_dir/s" "$g_ssh_origin_control_socket"
}

test_close_origin_ssh_control_socket_restores_last_lease_on_capture_failure() {
	set +e
	output=$(
		(
			errlog="$TEST_TMPDIR/close_origin_shared_capture.err"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_O_origin_host="origin.example"
			g_ssh_origin_control_socket="$entry_dir/s"
			g_ssh_origin_control_socket_dir="$entry_dir"
			g_ssh_origin_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="capture_error"
				g_zxfer_ssh_control_socket_action_stderr="Failed to read ssh control socket stderr for exit action."
				return 1
			}

			zxfer_close_origin_ssh_control_socket >"$TEST_TMPDIR/close_origin_shared_capture.out" 2>"$errlog"
			l_status=$?

			printf 'status=%s\n' "$l_status"
			printf 'errlog=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' \
				"$([ -d "$entry_dir" ] && printf yes || printf no)"
			printf 'lease_exists=%s\n' \
				"$([ -d "$g_ssh_origin_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'lease_reused=%s\n' \
				"$([ "$lease_file" = "$g_ssh_origin_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'socket_empty=%s\n' \
				"$([ -n "$g_ssh_origin_control_socket" ] && printf no || printf yes)"
		)
	)
	status=$?

	assertEquals "Origin-socket capture-failure close tests should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Last shared origin-socket lease release should fail closed when ssh exit stderr cannot be reloaded." \
		"$output" "status=1"
	assertContains "Last shared origin-socket capture failures should preserve the staged stderr diagnostic." \
		"$output" "errlog=Failed to read ssh control socket stderr for exit action."
	assertContains "Last shared origin-socket capture failures should preserve the cache entry for retry." \
		"$output" "dir_exists=yes"
	assertContains "Last shared origin-socket capture failures should restore an active lease directory for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored origin-socket leases should not reuse the removed lease path after capture failures." \
		"$output" "lease_reused=no"
	assertContains "Last shared origin-socket capture failures should preserve the in-process socket path." \
		"$output" "socket_empty=no"
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
				"$([ -d "$g_ssh_origin_control_socket_lease_file" ] && printf yes || printf no)"
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
	assertContains "Transport-token validation failures should restore an active lease directory for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored origin-socket leases should not reuse the removed lease path after transport token failures." \
		"$output" "lease_reused=no"
}

test_close_origin_ssh_control_socket_removes_stale_shared_entry_when_socket_is_not_live() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		g_zxfer_ssh_control_socket_action_result="stale"
		return 1
	}

	zxfer_close_origin_ssh_control_socket

	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertEquals "Last shared origin-socket lease release should remove stale cache entries when the socket is no longer live." \
		0 "$entry_dir_exists"
}

test_close_target_ssh_control_socket_preserves_state_on_transport_failure_without_lease() {
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

	set +e
	zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_transport.out" 2>"$errlog"
	status=$?
	set -e

	unset FAKE_SSH_EXIT_STATUS FAKE_SSH_STDERR
	if [ -d "$socket_dir" ]; then
		socket_dir_exists=1
	else
		socket_dir_exists=0
	fi

	assertEquals "Direct target-socket closes should fail when ssh transport shutdown fails." 1 "$status"
	assertContains "Direct target-socket close failures should preserve the ssh transport diagnostic." \
		"$(cat "$errlog")" "Host key verification failed."
	assertEquals "Direct target-socket close failures should preserve the cache directory for retry." \
		1 "$socket_dir_exists"
	assertEquals "Direct target-socket close failures should preserve the in-process socket path." \
		"$TEST_TMPDIR/target_transport.sock" "$g_ssh_target_control_socket"
	assertEquals "Direct target-socket close failures should preserve the in-process socket dir." \
		"$socket_dir" "$g_ssh_target_control_socket_dir"
}

test_close_target_ssh_control_socket_restores_last_lease_on_transport_failure() {
	errlog="$TEST_TMPDIR/close_target_shared_transport.err"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		g_zxfer_ssh_control_socket_action_result="error"
		g_zxfer_ssh_control_socket_action_stderr="Host key verification failed."
		return 1
	}

	set +e
	zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_shared_transport.out" 2>"$errlog"
	status=$?
	set -e

	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi
	if [ -d "$g_ssh_target_control_socket_lease_file" ]; then
		lease_exists=1
	else
		lease_exists=0
	fi

	assertEquals "Last shared target-socket lease release should fail closed on ssh transport errors." 1 "$status"
	assertContains "Last shared target-socket lease failures should preserve the ssh transport diagnostic." \
		"$(cat "$errlog")" "Host key verification failed."
	assertEquals "Last shared target-socket lease failures should preserve the cache entry for retry." \
		1 "$entry_dir_exists"
	assertEquals "Last shared target-socket lease failures should restore an active lease directory for retry." \
		1 "$lease_exists"
	assertNotEquals "Restored target-socket leases should not reuse the removed lease path." \
		"$lease_file" "$g_ssh_target_control_socket_lease_file"
	assertEquals "Last shared target-socket lease failures should preserve the in-process socket path." \
		"$entry_dir/s" "$g_ssh_target_control_socket"
}

test_close_target_ssh_control_socket_restores_last_lease_on_capture_failure() {
	set +e
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_shared_capture.err"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="capture_error"
				g_zxfer_ssh_control_socket_action_stderr="Failed to read ssh control socket stderr for exit action."
				return 1
			}

			zxfer_close_target_ssh_control_socket >"$TEST_TMPDIR/close_target_shared_capture.out" 2>"$errlog"
			l_status=$?

			printf 'status=%s\n' "$l_status"
			printf 'errlog=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' \
				"$([ -d "$entry_dir" ] && printf yes || printf no)"
			printf 'lease_exists=%s\n' \
				"$([ -d "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'lease_reused=%s\n' \
				"$([ "$lease_file" = "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'socket_empty=%s\n' \
				"$([ -n "$g_ssh_target_control_socket" ] && printf no || printf yes)"
		)
	)
	status=$?

	assertEquals "Target-socket capture-failure close tests should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Last shared target-socket lease release should fail closed when ssh exit stderr cannot be reloaded." \
		"$output" "status=1"
	assertContains "Last shared target-socket capture failures should preserve the staged stderr diagnostic." \
		"$output" "errlog=Failed to read ssh control socket stderr for exit action."
	assertContains "Last shared target-socket capture failures should preserve the cache entry for retry." \
		"$output" "dir_exists=yes"
	assertContains "Last shared target-socket capture failures should restore an active lease directory for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored target-socket leases should not reuse the removed lease path after capture failures." \
		"$output" "lease_reused=no"
	assertContains "Last shared target-socket capture failures should preserve the in-process socket path." \
		"$output" "socket_empty=no"
}

test_close_target_ssh_control_socket_restores_last_lease_on_transport_token_failure() {
	set +e
	output=$(
		(
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				g_zxfer_ssh_control_socket_action_result="error"
				g_zxfer_ssh_control_socket_action_stderr="Managed ssh policy invalid."
				return 1
			}

			set +e
			zxfer_close_target_ssh_control_socket 2>&1
			status=$?
			set -e

			printf 'status=%s\n' "$status"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
			printf 'lease_exists=%s\n' \
				"$([ -d "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
			printf 'lease_reused=%s\n' \
				"$([ "$lease_file" = "$g_ssh_target_control_socket_lease_file" ] && printf yes || printf no)"
		)
	)
	status=$?

	assertEquals "Last shared target-socket lease release should fail closed when transport token validation fails." \
		0 "$status"
	assertContains "Target transport-token validation failures should preserve the original diagnostic." \
		"$output" "Managed ssh policy invalid."
	assertContains "Target transport-token validation failures should preserve the failing close status." \
		"$output" "status=1"
	assertContains "Target transport-token validation failures should restore an active lease directory for retry." \
		"$output" "lease_exists=yes"
	assertContains "Restored target-socket leases should not reuse the removed lease path after transport token failures." \
		"$output" "lease_reused=no"
}

test_close_target_ssh_control_socket_preserves_stale_cleanup_failures_for_last_lease() {
	set +e
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_stale_cleanup.err"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 73
			}

			set +e
			zxfer_close_target_ssh_control_socket 2>"$errlog"
			l_status=$?
			set -e

			printf 'status=%s\n' "$l_status"
			printf 'errlog=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' \
				"$([ -d "$entry_dir" ] && printf yes || printf no)"
			printf 'lease_exists=%s\n' \
				"$([ -d "$lease_file" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'lease=%s\n' "$g_ssh_target_control_socket_lease_file"
		)
	)
	status=$?

	assertEquals "Target stale-cleanup failure close tests should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Last shared target-socket stale cleanup failures should preserve the local cleanup status." \
		"$output" "status=73"
	assertContains "Last shared target-socket stale cleanup failures should emit the specific cleanup diagnostic." \
		"$output" "errlog=Error removing ssh control socket cache directory for target host."
	assertContains "Last shared target-socket stale cleanup failures should preserve the cache directory for retry." \
		"$output" "dir_exists=yes"
	assertContains "Last shared target-socket stale cleanup failures should still remove the current lease directory first." \
		"$output" "lease_exists=no"
}

test_close_target_ssh_control_socket_reports_lock_release_failure_after_last_lease_exits() {
	set +e
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_last_release.err"
			entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
			lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
			: >"$entry_dir/s"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$entry_dir/s"
			g_ssh_target_control_socket_dir="$entry_dir"
			g_ssh_target_control_socket_lease_file="$lease_file"
			zxfer_acquire_ssh_control_socket_lock() {
				g_zxfer_ssh_control_socket_lock_dir_result="$TEST_TMPDIR/target-last-release.lock"
				return 0
			}
			zxfer_release_ssh_control_socket_lock() {
				zxfer_note_ssh_control_socket_lock_error \
					"Failed to release ssh control socket lock path \"$1\"."
				return 1
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="closed"
				return 0
			}

			zxfer_close_target_ssh_control_socket 2>"$errlog"
			l_status=$?

			printf 'status=%s\n' "$l_status"
			printf 'socket=%s\n' "${g_ssh_target_control_socket:-}"
			printf 'lease=%s\n' "${g_ssh_target_control_socket_lease_file:-}"
			printf 'errlog=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' \
				"$([ -d "$entry_dir" ] && printf yes || printf no)"
		)
	)
	status=$?

	assertEquals "Target last-lease lock-release failure close tests should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Last shared target-socket close should fail closed when the shared lock cannot be released after a successful exit." \
		"$output" "status=1"
	assertContains "Last shared target-socket close should clear the socket path after the last lease exits even when lock release fails." \
		"$output" "socket="
	assertContains "Last shared target-socket close should clear the lease path after the last lease exits even when lock release fails." \
		"$output" "lease="
	assertContains "Last shared target-socket close should surface the checked shared-lock release failure." \
		"$output" "errlog=Error releasing ssh control socket lock for target host: Failed to release ssh control socket lock path \"$TEST_TMPDIR/target-last-release.lock\"."
	assertContains "Last shared target-socket close should still remove the cache entry before reporting the shared-lock release failure." \
		"$output" "dir_exists=no"
}

test_close_target_ssh_control_socket_preserves_stale_cleanup_failures_without_lease() {
	set +e
	output=$(
		(
			errlog="$TEST_TMPDIR/close_target_direct_stale_cleanup.err"
			socket_dir="$TEST_TMPDIR/target_direct_stale_socket_dir"
			mkdir -p "$socket_dir"
			g_option_T_target_host="target.example"
			g_ssh_target_control_socket="$socket_dir/s"
			g_ssh_target_control_socket_dir="$socket_dir"
			zxfer_run_ssh_control_socket_action_for_host() {
				g_zxfer_ssh_control_socket_action_result="stale"
				return 1
			}
			zxfer_cleanup_ssh_control_socket_entry_dir() {
				return 61
			}

			set +e
			zxfer_close_target_ssh_control_socket 2>"$errlog"
			l_status=$?
			set -e

			printf 'status=%s\n' "$l_status"
			printf 'errlog=%s\n' "$(cat "$errlog")"
			printf 'dir_exists=%s\n' \
				"$([ -d "$socket_dir" ] && printf yes || printf no)"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'dir=%s\n' "$g_ssh_target_control_socket_dir"
		)
	)
	status=$?

	assertEquals "Direct target stale-cleanup failure close tests should complete the test subshell cleanly." \
		0 "$status"
	assertContains "Direct target-socket stale cleanup failures should preserve the local cleanup status." \
		"$output" "status=61"
	assertContains "Direct target-socket stale cleanup failures should emit the specific cleanup diagnostic." \
		"$output" "errlog=Error removing ssh control socket cache directory for target host."
	assertContains "Direct target-socket stale cleanup failures should preserve the cache directory for retry." \
		"$output" "dir_exists=yes"
}

test_close_target_ssh_control_socket_removes_stale_shared_entry_when_socket_is_not_live() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		g_zxfer_ssh_control_socket_action_result="stale"
		return 1
	}

	zxfer_close_target_ssh_control_socket

	if [ -d "$entry_dir" ]; then
		entry_dir_exists=1
	else
		entry_dir_exists=0
	fi

	assertEquals "Last shared target-socket lease release should remove stale cache entries when the socket is no longer live." \
		0 "$entry_dir_exists"
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

test_get_backup_storage_dir_for_dataset_tree_uses_dataset_hierarchy() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"

	assertEquals "Dataset-tree backup storage should mirror the dataset hierarchy under ZXFER_BACKUP_DIR." \
		"$g_backup_storage_root/tank/src/child" "$(zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/child")"
	assertEquals "Dataset-tree backup storage should trim trailing slashes." \
		"$g_backup_storage_root/tank/src" "$(zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/")"
	assertEquals "Empty dataset-tree lookups should use the dataset placeholder bucket." \
		"$g_backup_storage_root/dataset" "$(zxfer_get_backup_storage_dir_for_dataset_tree "/")"
}

test_get_backup_storage_dir_for_dataset_tree_runs_in_current_shell() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root_current_shell"
	output_file="$TEST_TMPDIR/backup_helper_current_shell.out"

	zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/child" >"$output_file"
	assertEquals "Dataset-tree storage lookups should run in the current shell for coverage." \
		"$g_backup_storage_root/tank/src/child" "$(cat "$output_file")"

	zxfer_get_backup_storage_dir_for_dataset_tree "/" >"$output_file"
	assertEquals "Rootlike dataset-tree lookups should use the dataset placeholder in the current shell." \
		"$g_backup_storage_root/dataset" "$(cat "$output_file")"
}

test_backup_storage_helpers_cover_identity_encoding_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_helper_fallback.out"
	status_file="$TEST_TMPDIR/backup_helper_fallback.status"

	(
		od() {
			:
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
		printf '%s\n' "$?" >"$status_file"
	)
	assertEquals "Backup metadata file keys should fail closed in the current shell when exact identity hex encoding produces no output." \
		1 "$(cat "$status_file")"
	assertEquals "Failed exact identity key derivation should not emit a placeholder key." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_backup_metadata_filename_runs_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_filename_current_shell.out"
	g_backup_file_extension=".zxfer_backup_info"

	zxfer_get_backup_metadata_filename "tank/src" "backup/dst" >"$output_file"

	assertContains "Backup metadata filename rendering should run in the current shell." \
		"$(cat "$output_file")" ".zxfer_backup_info.v2/h/"
	assertContains "Backup metadata filename rendering should use the fixed v2 leaf name." \
		"$(cat "$output_file")" "/.zxfer_backup_info.v2"
}

test_backup_metadata_matches_source_accepts_only_v2_relative_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	current_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	legacy_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"tank/src,backup/dst,compression=lz4")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "Backup metadata matching should accept current v2 relative rows." \
		0 "$(
			(
				zxfer_backup_metadata_matches_source "$current_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject legacy exact-pair rows in v2 files." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "$legacy_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_backup_metadata_matches_source_rejects_wrong_destination_and_ambiguous_relative_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/other"
	wrong_destination_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	ambiguous_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "Backup metadata matching should reject rows for the requested source dataset when the destination root does not match." \
		1 "$(
			(
				zxfer_backup_metadata_matches_source "$wrong_destination_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject files that contain multiple relative rows for the same source/destination root." \
		2 "$(
			(
				zxfer_backup_metadata_matches_source "$ambiguous_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_backup_metadata_matches_source_rejects_malformed_current_format_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	missing_tab_contents=$(zxfer_test_render_current_backup_metadata_contents "broken-row")
	extra_comma_contents=$(zxfer_test_render_current_backup_metadata_contents "broken,legacy,row")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "Backup metadata matching should reject rows that do not contain the current relative-path/properties format." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "$missing_tab_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject rows that contain extra raw field delimiters." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "$extra_comma_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_missing_for_missing_local_candidate() {
	assertEquals "Missing local backup candidates should return the candidate-missing sentinel." \
		1 "$(
			(
				zxfer_read_local_backup_file() {
					return 4
				}
				zxfer_try_backup_restore_candidate "/tmp/missing.meta" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_missing_for_missing_remote_candidate() {
	assertEquals "Missing remote backup candidates should return the candidate-missing sentinel." \
		1 "$(
			(
				zxfer_read_remote_backup_file() {
					return 4
				}
				zxfer_try_backup_restore_candidate "/tmp/missing.meta" "tank/src" "backup/dst" "backup@example.com" source
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_failure_for_unexpected_match_status() {
	assertEquals "Unexpected backup-metadata match statuses should fail closed as read/parse errors." \
		5 "$(
			(
				zxfer_read_local_backup_file() {
					g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
						"tank/src,backup/dst,compression=lz4")
					return 0
				}
				zxfer_backup_metadata_matches_source() {
					return 99
				}
				zxfer_try_backup_restore_candidate "/tmp/weird.meta" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_get_backup_metadata_filename_uses_source_and_destination_identity() {
	g_backup_file_extension=".zxfer_backup_info"

	first_name=$(zxfer_get_backup_metadata_filename "tank/a/src" "backup/one")
	second_name=$(zxfer_get_backup_metadata_filename "tank/b/src" "backup/one")
	third_name=$(zxfer_get_backup_metadata_filename "tank/a/src" "backup/two")

	assertContains "Backup metadata filenames should use the current chunked v2 identity path." \
		"$first_name" ".zxfer_backup_info.v2/h/"
	assertNotEquals "Distinct source datasets that share the same tail should produce different backup metadata filenames." \
		"$first_name" "$second_name"
	assertNotEquals "Distinct destination roots for the same source should produce different backup metadata filenames." \
		"$first_name" "$third_name"
}

test_zxfer_backup_metadata_file_key_fails_when_hex_encoding_is_empty() {
	(
		od() {
			:
		}

		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >/dev/null
		status=$?
		assertEquals "Backup metadata file keys should fail closed when exact identity hex encoding produces no output." \
			1 "$status"
	)
}

test_get_path_owner_uid_and_mode_use_numeric_stat_output() {
	result_uid=$(
		(
			stat() {
				if [ "$1" = "-c" ] && [ "$2" = "%u" ]; then
					printf '%s\n' "1234"
					return 0
				fi
				return 1
			}
			ls() {
				return 1
			}
			: >"$TEST_TMPDIR/stat-owner-file"
			zxfer_get_path_owner_uid "$TEST_TMPDIR/stat-owner-file"
		)
	)
	result_mode=$(
		(
			stat() {
				if [ "$1" = "-c" ] && [ "$2" = "%a" ]; then
					printf '%s\n' "640"
					return 0
				fi
				return 1
			}
			ls() {
				return 1
			}
			: >"$TEST_TMPDIR/stat-mode-file"
			zxfer_get_path_mode_octal "$TEST_TMPDIR/stat-mode-file"
		)
	)

	assertEquals "Numeric GNU stat output should be accepted directly for owner lookups." "1234" "$result_uid"
	assertEquals "Numeric GNU stat output should be accepted directly for mode lookups." "640" "$result_mode"
}

test_get_path_owner_uid_and_mode_return_failure_for_missing_paths() {
	missing_path="$TEST_TMPDIR/does_not_exist"

	zxfer_get_path_owner_uid "$missing_path" >/dev/null 2>&1
	owner_status=$?
	zxfer_get_path_mode_octal "$missing_path" >/dev/null 2>&1
	mode_status=$?

	assertEquals "Owner lookups should fail cleanly for missing paths." 1 "$owner_status"
	assertEquals "Mode lookups should fail cleanly for missing paths." 1 "$mode_status"
}

test_get_ssh_cmd_for_host_returns_base_command_for_empty_host() {
	g_cmd_ssh="/usr/bin/ssh"

	assertEquals "Hosts omitted from wrapper lookups should return the base ssh command." \
		"'/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes'" "$(zxfer_get_ssh_cmd_for_host "")"
}

test_get_effective_user_uid_returns_failure_when_id_is_unavailable() {
	empty_path="$TEST_TMPDIR/no_id_path"
	mkdir -p "$empty_path"
	old_path=$PATH
	PATH="$empty_path"
	outfile="$TEST_TMPDIR/effective_uid.out"

	zxfer_get_effective_user_uid >"$outfile"
	status=$?
	PATH=$old_path

	assertEquals "Missing id binaries should make effective-UID detection fail cleanly." 1 "$status"
	assertEquals "Failed effective-UID detection should not emit output." "" "$(cat "$outfile")"
}

test_get_path_owner_uid_and_mode_use_stat_when_available() {
	owned_file="$TEST_TMPDIR/stat_owned_file"
	: >"$owned_file"

	owner_result=$(
		(
			stat() {
				if [ "$1" = "-f" ] && [ "$2" = "%u" ]; then
					printf '%s\n' "4242"
					return 0
				fi
				return 1
			}
			zxfer_get_path_owner_uid "$owned_file"
		)
	)

	mode_result=$(
		(
			stat() {
				if [ "$1" = "-f" ] && [ "$2" = "%OLp" ]; then
					printf '%s\n' "600"
					return 0
				fi
				return 1
			}
			zxfer_get_path_mode_octal "$owned_file"
		)
	)

	assertEquals "Owner lookup should use stat when available." "4242" "$owner_result"
	assertEquals "Mode lookup should use stat when available." "600" "$mode_result"
}

test_ensure_local_backup_dir_rejects_symlink_and_non_directory_targets() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_real"
	symlink_dir="$physical_tmpdir/ensure_local_link"
	non_dir="$physical_tmpdir/ensure_local_file"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$symlink_dir"
	: >"$non_dir"

	set +e
	symlink_output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$symlink_dir"
		)
	)
	symlink_status=$?

	non_dir_output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$non_dir"
		)
	)
	non_dir_status=$?

	assertEquals "Symlinked backup directories should be rejected." 1 "$symlink_status"
	assertContains "Symlinked backup directories should use the documented error." \
		"$symlink_output" "Refusing to use backup directory $symlink_dir because it is a symlink."
	assertEquals "Non-directory backup paths should be rejected." 1 "$non_dir_status"
	assertContains "Non-directory backup paths should use the documented error." \
		"$non_dir_output" "Refusing to use backup directory $non_dir because it is not a directory."
}

test_ensure_local_backup_dir_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_nested_real"
	link_dir="$physical_tmpdir/ensure_local_nested_link"
	target_dir="$link_dir/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$target_dir"
		)
	)
	status=$?

	assertEquals "Backup directories with symlinked parent components should be rejected before mkdir -p follows them." \
		1 "$status"
	assertContains "Nested symlink failures should identify the offending path component." \
		"$output" "Refusing to use backup directory $target_dir because path component $link_dir is a symlink."
}

test_ensure_local_backup_dir_rejects_relative_nested_symlink_components() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_relative_nested_real"
	link_dir="$physical_tmpdir/ensure_local_relative_nested_link"
	target_dir="./ensure_local_relative_nested_link/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$target_dir"
		)
	)
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative backup directories with symlinked parent components should be rejected before mkdir -p follows them." \
		1 "$status"
	assertContains "Relative nested symlink failures should identify the offending relative path component." \
		"$output" "Refusing to use backup directory $target_dir because path component ./ensure_local_relative_nested_link is a symlink."
}

test_ensure_local_backup_dir_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-local-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	zxfer_ensure_local_backup_dir "$target_dir"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block local backup directory creation, which keeps default /var- or /tmp-backed paths working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the secure backup directory to be created under the symlink target." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

test_ensure_local_backup_dir_rejects_unknown_or_disallowed_owner() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/ensure_local_owner"
	mkdir -p "$backup_dir"

	set +e
	unknown_owner_output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		)
	)
	unknown_owner_status=$?

	disallowed_owner_output=$(
		(
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1234"
			}
			zxfer_backup_owner_uid_is_allowed() {
				return 1
			}
			zxfer_describe_expected_backup_owner() {
				printf '%s\n' "root (UID 0)"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		)
	)
	disallowed_owner_status=$?

	assertEquals "Backup directories with unknown owners should be rejected." 1 "$unknown_owner_status"
	assertContains "Unknown owner failures should use the documented error." \
		"$unknown_owner_output" "Cannot determine the owner of backup directory $backup_dir."
	assertEquals "Backup directories owned by other UIDs should be rejected." 1 "$disallowed_owner_status"
	assertContains "Disallowed owner failures should identify the unexpected UID." \
		"$disallowed_owner_output" "Refusing to use backup directory $backup_dir because it is owned by UID 1234 instead of root (UID 0)."
}

test_ensure_local_backup_dir_reports_chmod_failures_in_current_shell() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/ensure_local_chmod_fail"
	fake_bin="$TEST_TMPDIR/ensure_local_chmod_bin"
	mkdir -p "$backup_dir" "$fake_bin"
	cat >"$fake_bin/chmod" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin/chmod"
	old_path=$PATH
	PATH="$fake_bin:$PATH"
	THROW_MSG=""
	zxfer_throw_error() {
		THROW_MSG=$1
		return 1
	}

	zxfer_ensure_local_backup_dir "$backup_dir"
	status=$?

	unset -f zxfer_throw_error
	PATH=$old_path

	assertEquals "chmod failures should cause zxfer_ensure_local_backup_dir to fail." 1 "$status"
	assertContains "chmod failures should use the documented backup-directory error." \
		"$THROW_MSG" "Error securing backup directory $backup_dir."
}

test_ensure_local_backup_dir_reports_mkdir_failures_in_current_shell() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/ensure_local_mkdir_fail"
	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		)
	)
	status=$?

	assertEquals "mkdir failures should cause zxfer_ensure_local_backup_dir to fail." 1 "$status"
	assertContains "mkdir failures should use the documented secure backup-directory error." \
		"$output" "Error creating secure backup directory $backup_dir."
}

test_ensure_remote_backup_dir_skips_without_host_and_reports_ssh_failures() {
	if zxfer_ensure_remote_backup_dir "$TEST_TMPDIR/remote_backup" ""; then
		empty_host_status=0
	else
		empty_host_status=1
	fi

	set +e
	ssh_failure_output=$(
		(
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "-remote_backup" "backup@example.com"
		)
	)
	ssh_failure_status=$?

	assertEquals "Remote backup directory preparation should no-op when no host is provided." 0 "$empty_host_status"
	assertEquals "Remote backup directory ssh failures should abort the helper." 1 "$ssh_failure_status"
	assertContains "Remote backup directory ssh failures should use the documented error." \
		"$ssh_failure_output" "Error preparing backup directory on backup@example.com."
}

test_ensure_remote_backup_dir_marks_missing_secure_path_helpers_as_dependency_errors() {
	empty_dir="$TEST_TMPDIR/ensure_remote_missing_helper_bin"
	mkdir -p "$empty_dir"

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$empty_dir"
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "/tmp/remote_backup" "backup@example.com"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup directory preparation should fail closed when required secure-PATH helpers are missing." \
		1 "$status"
	assertContains "Missing remote backup-dir helpers should surface the exact dependency name from the remote precheck." \
		"$output" "Required dependency \"mkdir\" not found on host backup@example.com in secure PATH ($empty_dir)."
	assertContains "Missing remote backup-dir helpers should be classified as dependency failures locally." \
		"$output" "class=dependency"
	assertContains "Missing remote backup-dir helpers should use the dependency-specific local error." \
		"$output" "Required remote backup-directory helper dependency not found on host backup@example.com in secure PATH ($empty_dir)."
}

test_ensure_remote_backup_dir_quotes_dash_prefixed_paths() {
	ssh_log="$TEST_TMPDIR/ensure_remote_dash.log"
	ssh_bin="$TEST_TMPDIR/ensure_remote_dash_ssh"
	cat >"$ssh_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >"$ssh_log"
exit 0
EOF
	chmod +x "$ssh_bin"
	g_cmd_ssh="$ssh_bin"
	g_zxfer_dependency_path="/stale/secure/path"
	ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"

	zxfer_ensure_remote_backup_dir "-remote_backup" "backup@example.com"

	assertContains "Remote backup directory preparation should scope auxiliary tools to the secure dependency path." \
		"$(cat "$ssh_log")" "PATH="
	assertContains "Remote backup directory preparation should refresh the secure-PATH wrapper from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$(cat "$ssh_log")" "/fresh/secure/path:/usr/bin"
	assertNotContains "Remote backup directory preparation should not keep using a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$(cat "$ssh_log")" "/stale/secure/path"
	assertContains "Dash-prefixed remote backup paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup"
}

test_ensure_remote_backup_dir_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_remote_nested_real"
	link_dir="$physical_tmpdir/ensure_remote_nested_link"
	target_dir="$link_dir/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup directory preparation should reject symlinked parent components before mkdir -p follows them." \
		1 "$status"
	assertContains "Remote backup directory preparation should surface the offending symlinked path component." \
		"$output" "Refusing to use backup directory $target_dir because path component $link_dir is a symlink."
	assertContains "Remote backup directory preparation should still fail through the documented host-scoped error path." \
		"$output" "Error preparing backup directory on backup@example.com."
}

test_ensure_remote_backup_dir_rejects_root_owned_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_remote_nested_root_real"
	link_dir="$physical_tmpdir/ensure_remote_nested_root_link"
	target_dir="$link_dir/subdir"
	fake_bin="$physical_tmpdir/ensure_remote_nested_root_bin"
	mkdir -p "$real_dir" "$fake_bin"
	ln -s "$real_dir" "$link_dir"
	cat >"$fake_bin/stat" <<'EOF'
#!/bin/sh
case "$1 $2" in
	"-c %u"|"-f %u")
		printf '0\n'
		exit 0
		;;
esac
exit 1
EOF
	cat >"$fake_bin/ls" <<'EOF'
#!/bin/sh
for last_arg do :; done
	printf 'drwxr-xr-x 1 0 0 0 Jan  1 00:00 %s\n' "$last_arg"
EOF
	chmod +x "$fake_bin/stat" "$fake_bin/ls"

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup directory preparation should reject nested symlink components even when remote ownership probes report root-owned secure paths." \
		1 "$status"
	assertContains "Root-owned nested symlink rejection should still identify the offending path component." \
		"$output" "Refusing to use backup directory $target_dir because path component $link_dir is a symlink."
	assertContains "Root-owned nested symlink rejection should still fail through the documented host-scoped error path." \
		"$output" "Error preparing backup directory on backup@example.com."
}

test_ensure_remote_backup_dir_rejects_relative_nested_symlink_components() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_remote_relative_nested_real"
	link_dir="$physical_tmpdir/ensure_remote_relative_nested_link"
	target_dir="./ensure_remote_relative_nested_link/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
		) 2>&1
	)
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Remote backup directory preparation should reject relative symlinked parent components before mkdir -p follows them." \
		1 "$status"
	assertContains "Relative remote backup directory preparation should surface the offending relative symlinked path component." \
		"$output" "Refusing to use backup directory $target_dir because path component ./ensure_remote_relative_nested_link is a symlink."
	assertContains "Relative remote backup directory preparation should still fail through the documented host-scoped error path." \
		"$output" "Error preparing backup directory on backup@example.com."
}

test_ensure_remote_backup_dir_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-remote-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
	)
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup directory preparation, which keeps default /var- or /tmp-backed remote roots working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the remote backup directory helper to create the requested directory." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
