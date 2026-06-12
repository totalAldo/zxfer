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
	g_option_o_override_property=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_s_make_snapshot=0
	g_option_U_skip_unsupported_properties=0
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
	g_ssh_origin_control_socket=""
	g_ssh_target_control_socket=""
	g_zxfer_ssh_control_socket_dir_result=""
	g_zxfer_ssh_transport_tokens_origin=""
	g_zxfer_ssh_transport_tokens_origin_socket=""
	g_zxfer_ssh_transport_tokens_origin_set=0
	g_zxfer_ssh_transport_tokens_target=""
	g_zxfer_ssh_transport_tokens_target_socket=""
	g_zxfer_ssh_transport_tokens_target_set=0
	g_zxfer_ssh_shell_context_memo_origin_spec=""
	g_zxfer_ssh_shell_context_memo_origin_host=""
	g_zxfer_ssh_shell_context_memo_origin_wrapper=""
	g_zxfer_ssh_shell_context_memo_target_spec=""
	g_zxfer_ssh_shell_context_memo_target_host=""
	g_zxfer_ssh_shell_context_memo_target_wrapper=""
	g_ssh_supports_control_sockets=0
	g_test_max_yield_iterations=8
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

test_zxfer_remote_capability_requested_tools_defer_parallel_for_fast_noop_scope() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host=""
	g_option_R_recursive="tank/src"
	g_option_j_jobs=4
	g_option_s_make_snapshot=0
	g_option_m_migrate=0
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_option_U_skip_unsupported_properties=1
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_g_grandfather_protection="enabled"

	host_tools=$(zxfer_get_remote_capability_requested_tools_for_host "origin.example")
	parallel_tools=$(zxfer_get_remote_capability_requested_tools_for_resolved_tool "origin.example" parallel)

	assertContains "Fast recursive no-op startup scopes should still preload zfs." \
		"$host_tools" "zfs"
	assertNotContains "Fast recursive no-op startup scopes should defer parallel because -U and -g cannot be consumed until the proof finds work." \
		"$host_tools" "parallel"
	assertContains "On-demand parallel resolution should still request parallel explicitly." \
		"$parallel_tools" "parallel"
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
		"$(zxfer_render_remote_capability_cache_identity_for_host "" "")" "$g_target_remote_capabilities_cache_identity"
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
		"$(zxfer_render_remote_capability_cache_identity_for_host "" "")" "$g_origin_remote_capabilities_cache_identity"
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
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	result=$(zxfer_get_cached_remote_capability_response_for_host "origin.example")

	assertContains "Origin-side cached capability reads should return the cached payload." \
		"$result" "tool	parallel	0	/opt/bin/parallel"
}

test_zxfer_get_cached_remote_capability_response_for_host_reads_target_slot() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
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
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	zxfer_store_cached_remote_capability_response_for_host "other.example" "$(fake_remote_capability_response)"

	assertEquals "Once the origin fallback slot is occupied, later unassigned cache responses should populate the target slot." \
		"other.example" "$g_target_remote_capabilities_host"
}

test_zxfer_ensure_remote_host_capabilities_prefers_memory_cache() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
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

	zxfer_store_cached_remote_capability_response_for_host \
		"target.example" "$(fake_remote_capability_response)" "parallel"

	assertEquals "Capability bootstrap tracking should reset when the target-side cached identity changes for the same host." \
		"" "$g_target_remote_capabilities_bootstrap_source"
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

test_zxfer_preload_remote_host_capabilities_defers_parallel_for_fast_noop_scope() {
	log="$TEST_TMPDIR/preload_remote_caps_fast_noop.log"
	: >"$log"
	tools_file="$TEST_TMPDIR/preload_remote_caps_fast_noop.tools"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host=""
	g_option_R_recursive="tank/src"
	g_option_j_jobs=4
	g_option_s_make_snapshot=0
	g_option_m_migrate=0
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_option_U_skip_unsupported_properties=1
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_g_grandfather_protection="enabled"

	(
		zxfer_ensure_remote_host_capabilities() {
			printf 'ensure host=%s side=%s\n' \
				"$1" "${2:-}" >>"$log"
			printf '%s\n' "${3:-}" >"$tools_file"
		}
		zxfer_preload_remote_host_capabilities "origin.example" source
	)

	assertContains "Fast no-op capability preloading should still warm zfs for remote origin discovery." \
		"$(cat "$tools_file")" "zfs"
	assertNotContains "Fast no-op capability preloading should defer parallel because -U and -g cannot be consumed until the proof finds work." \
		"$(cat "$tools_file")" "parallel"
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
			g_zxfer_property_table_lookup_result="stale-lookup"
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
			printf 'table_lookup=<%s>\n' "$g_zxfer_property_table_lookup_result"
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
	assertContains "Runtime init should clear stale property-table lookup state." "$result" "table_lookup=<>"
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
	assertNotContains "Remote capability preparation should not open an SSH control socket before replication work exists." \
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

test_prepare_remote_host_connections_preloads_capabilities_without_opening_control_sockets() {
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

	assertNotContains "Origin control socket setup should be deferred until replication work exists." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertNotContains "Target control socket setup should be deferred until replication work exists." \
		"$(cat "$log")" "setup target.example doas target"
	assertContains "Origin capability discovery should be preloaded during remote preparation." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Target capability discovery should be preloaded during remote preparation." \
		"$(cat "$log")" "preload target.example doas destination"
	assertContains "Origin zfs spec should refresh to the resolved origin helper path." \
		"$result" "lzfs=/remote/origin/zfs"
	assertContains "Target zfs spec should refresh to the resolved target helper path." \
		"$result" "rzfs=/remote/target/zfs"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_ssh_control_sockets_for_active_hosts_sets_up_control_sockets_after_validation() {
	log="$TEST_TMPDIR/prepare_active_control_sockets.log"
	now_counter_file="$TEST_TMPDIR/prepare_active_control_sockets.now.counter"
	: >"$log"
	printf '%s\n' 0 >"$now_counter_file"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
				if [ "$2" = "origin" ]; then
					g_ssh_origin_control_socket="/tmp/origin.sock"
				elif [ "$2" = "target" ]; then
					g_ssh_target_control_socket="/tmp/target.sock"
				fi
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 2000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 2250
				fi
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertContains "Origin control socket setup should happen once replication work exists." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Target control socket setup should happen once replication work exists." \
		"$(cat "$log")" "setup target.example doas target"
	assertEquals "Active control-socket preparation should not replace sockets that are already ready." \
		"2" "$(wc -l <"$log" | tr -d '[:space:]')"
	assertContains "Origin zfs spec should refresh after deferred socket setup." \
		"$result" "lzfs=/remote/origin/zfs"
	assertContains "Target zfs spec should refresh after deferred socket setup." \
		"$result" "rzfs=/remote/target/zfs"
	assertContains "Very-verbose deferred socket preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_ssh_control_sockets_for_active_hosts_logs_when_control_sockets_are_unavailable() {
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
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=0
			zxfer_prepare_ssh_control_sockets_for_active_hosts
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertContains "Origin active socket preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for origin host."
	assertContains "Target active socket preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for target host."
	assertEquals "Deferred socket setup should not preload remote capabilities." "" "$(cat "$log")"
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

test_get_ssh_transport_tokens_for_host_returns_base_tokens_for_empty_host() {
	g_cmd_ssh="/usr/bin/ssh"

	assertEquals "Hosts omitted from wrapper lookups should return the base ssh transport tokens." \
		"$(printf '%s\n' /usr/bin/ssh -o BatchMode=yes -o StrictHostKeyChecking=yes)" \
		"$(zxfer_get_ssh_transport_tokens_for_host "")"
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

test_zxfer_ensure_ssh_control_socket_dir_prefers_run_tmp_root_and_memoizes() {
	output=$(
		(
			set +e
			short_root="$TEST_PRIVATE_DEFAULT_TMPDIR/run-root.$$"
			mkdir -p "$short_root" || exit 90
			chmod 700 "$short_root" || exit 90
			g_zxfer_run_tmp_root=$short_root
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'first_status=%s\n' "$?"
			printf 'first=%s\n' "$g_zxfer_ssh_control_socket_dir_result"
			# The memo must answer later lookups without re-resolving the root.
			zxfer_ensure_run_tmp_root() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'memo_status=%s\n' "$?"
			printf 'memo=%s\n' "$g_zxfer_ssh_control_socket_dir_result"
			printf 'expected=%s\n' "$short_root"
		)
	)
	expected_root=$(printf '%s\n' "$output" | awk -F= '/^expected=/{print $2}')

	assertContains "Per-run socket directory resolution should succeed under a short run temp root." \
		"$output" "first_status=0"
	assertContains "Per-run sockets should live directly under the private run temp root." \
		"$output" "first=$expected_root"
	assertContains "Repeated socket-directory lookups should reuse the memoized directory." \
		"$output" "memo_status=0"
	assertContains "The memoized socket directory should match the first resolution." \
		"$output" "memo=$expected_root"
}

test_zxfer_ensure_ssh_control_socket_dir_falls_back_to_short_root_for_long_tmpdir() {
	long_component="zxfer-long-tmpdir-component-000000000000000000000000000000000000"
	long_tmpdir="$TEST_TMPDIR/$long_component/$long_component"
	mkdir -p "$long_tmpdir" || fail "Unable to create the long TMPDIR fixture."
	chmod 700 "$long_tmpdir" || fail "Unable to restrict the long TMPDIR fixture."

	output=$(
		(
			set +e
			TMPDIR=$long_tmpdir
			g_option_V_very_verbose=1
			g_zxfer_effective_tmpdir=""
			g_zxfer_effective_tmpdir_requested=""
			g_zxfer_run_tmp_root=""
			g_zxfer_ssh_control_socket_dir_result=""
			socket_dir=$(zxfer_ensure_ssh_control_socket_dir 2>"$TEST_TMPDIR/socket-dir-fallback.note")
			printf 'status=%s\n' "$?"
			printf 'socket_dir=%s\n' "$socket_dir"
			# Prefix-strip instead of a case glob: bash 3.2 as /bin/sh
			# misparses unparenthesized case patterns inside $( ).
			if [ "${socket_dir#"$long_tmpdir"/}" != "$socket_dir" ]; then
				printf 'under_long_tmpdir=yes\n'
			else
				printf 'under_long_tmpdir=no\n'
			fi
			if [ -d "$socket_dir" ]; then
				printf 'socket_dir_mode=%s\n' "$(zxfer_get_path_mode_octal "$socket_dir")"
			fi
			printf 'note=%s\n' "$(cat "$TEST_TMPDIR/socket-dir-fallback.note")"
		)
	)

	assertContains "Long-TMPDIR socket directory resolution should still succeed." \
		"$output" "status=0"
	assertContains "Long-TMPDIR runs should not place control sockets under the long run temp root." \
		"$output" "under_long_tmpdir=no"
	assertContains "The fallback socket directory should be private to the effective user." \
		"$output" "socket_dir_mode=700"
	assertContains "Long-TMPDIR fallback should explain the shorter socket root under -V." \
		"$output" "for ssh control sockets; using shorter socket root"
}

test_zxfer_ensure_ssh_control_socket_dir_fails_closed_when_no_short_root_exists() {
	output=$(
		(
			set +e
			g_zxfer_run_tmp_root=""
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'no_root=%s\n' "$?"
		)
		(
			set +e
			long_component="zxfer-long-fallback-component-00000000000000000000000000000000"
			g_zxfer_run_tmp_root="/tmp/$long_component/$long_component"
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_try_get_socket_cache_tmpdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'no_fallback=%s\n' "$?"
		)
		(
			set +e
			long_component="zxfer-long-fallback-component-00000000000000000000000000000000"
			long_fallback_root="$TEST_TMPDIR/$long_component/$long_component"
			mkdir -p "$long_fallback_root" || exit 90
			g_zxfer_run_tmp_root="/tmp/$long_component/$long_component"
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_ensure_run_tmp_root() {
				return 0
			}
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$long_fallback_root"
			}
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'fallback_too_long=%s\n' "$?"
		)
	)

	assertContains "Socket directory resolution should fail closed when the run temp root cannot be created." \
		"$output" "no_root=1"
	assertContains "Socket directory resolution should fail closed when no short fallback temp root exists." \
		"$output" "no_fallback=1"
	assertContains "Socket directory resolution should fail closed when even the fallback root exceeds sun_path limits." \
		"$output" "fallback_too_long=1"
}

test_zxfer_get_ssh_control_socket_path_for_role_names_per_role_sockets() {
	output=$(
		(
			set +e
			g_zxfer_ssh_control_socket_dir_result="$TEST_TMPDIR/socket-root"
			origin_path=$(zxfer_get_ssh_control_socket_path_for_role origin)
			printf 'origin_status=%s\n' "$?"
			printf 'origin=%s\n' "$origin_path"
			target_path=$(zxfer_get_ssh_control_socket_path_for_role target)
			printf 'target=%s\n' "$target_path"
			zxfer_get_ssh_control_socket_path_for_role bogus >/dev/null
			printf 'bogus=%s\n' "$?"
			g_zxfer_ssh_control_socket_dir_result=""
			zxfer_get_ssh_control_socket_path_for_role origin >/dev/null
			printf 'missing_dir=%s\n' "$?"
		)
	)

	assertContains "Role socket paths should resolve for the origin role." \
		"$output" "origin_status=0"
	assertContains "Origin sockets should use the per-role socket name under the per-run directory." \
		"$output" "origin=$TEST_TMPDIR/socket-root/ssh-origin.sock"
	assertContains "Target sockets should use the per-role socket name under the per-run directory." \
		"$output" "target=$TEST_TMPDIR/socket-root/ssh-target.sock"
	assertContains "Unknown roles should fail closed." \
		"$output" "bogus=1"
	assertContains "Role socket paths should fail closed before the per-run directory is resolved." \
		"$output" "missing_dir=1"
}

test_zxfer_is_ssh_control_socket_path_short_enough_enforces_sun_path_limit() {
	long_suffix=""
	while [ "${#long_suffix}" -lt 150 ]; do
		long_suffix="${long_suffix}xxxxxxxxxx"
	done

	set +e
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/zxfer-short/ssh-origin.sock"
	short_status=$?
	zxfer_is_ssh_control_socket_path_short_enough "/tmp/$long_suffix/ssh-origin.sock"
	long_status=$?

	assertEquals "SSH control-socket path-length checks should accept short socket paths." \
		0 "$short_status"
	assertEquals "SSH control-socket path-length checks should reject socket paths beyond the sun_path limit." \
		1 "$long_status"
}

test_ssh_control_socket_support_helper_covers_probe_success_and_failure() {
	fake_support_bin="$TEST_TMPDIR/fake_ssh_support"
	cat >"$fake_support_bin" <<'EOF'
#!/bin/sh
if [ "$1" = "-M" ] && [ "$2" = "-V" ]; then
	exit 0
fi
exit 1
EOF
	chmod +x "$fake_support_bin"

	g_cmd_ssh="$fake_support_bin"
	set +e
	zxfer_ssh_supports_control_sockets >/dev/null 2>&1
	support_status=$?
	g_cmd_ssh="$TEST_TMPDIR/missing_ssh"
	zxfer_ssh_supports_control_sockets >/dev/null 2>&1
	missing_status=$?

	assertEquals "SSH control-socket support helpers should detect a transport that accepts -M -V probes." \
		0 "$support_status"
	assertEquals "SSH control-socket support helpers should fail closed when the configured ssh helper cannot be probed." \
		"yes" "$(if [ "$missing_status" -ne 0 ]; then printf '%s' yes; else printf '%s' no; fi)"
}

test_setup_ssh_control_socket_replaces_existing_target_socket_state() {
	log="$TEST_TMPDIR/setup_target.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	result=$(
		(
			zxfer_close_target_ssh_control_socket() {
				printf 'closed\n'
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_target_control_socket="$TEST_TMPDIR/old_target.sock"
			zxfer_setup_ssh_control_socket "target.example doas" "target"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
		)
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertContains "Replacing an existing target control socket should close the old socket first." \
		"$result" "closed"
	assertContains "Target socket setup should store the per-role control socket path." \
		"$result" "socket=$(printf '%s\n' "$result" | awk -F= '/^socket=/{print $2}')"
	assertContains "Target socket setup should store a per-role socket name." \
		"$result" "/ssh-target.sock"
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

test_setup_ssh_control_socket_reuses_live_socket_without_opening_new_master() {
	open_log="$TEST_TMPDIR/setup_reuse_open.log"
	: >"$open_log"
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	g_zxfer_ssh_control_socket_dir_result=""
	zxfer_ensure_ssh_control_socket_dir >/dev/null ||
		fail "Unable to resolve the per-run socket directory."
	expected_socket=$(zxfer_get_ssh_control_socket_path_for_role origin) ||
		fail "Unable to resolve the per-run origin socket path."
	: >"$expected_socket"

	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			zxfer_check_ssh_control_socket_for_host() {
				printf 'checked %s %s\n' "$1" "$2"
				return 0
			}
			zxfer_open_ssh_control_socket_for_host() {
				printf 'open\n' >>"$open_log"
				return 0
			}
			zxfer_setup_ssh_control_socket "origin.example pfexec" "origin"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
		)
	)
	rm -f "$expected_socket"

	assertEquals "Reusing a live per-run control socket should not start a second ssh master." \
		"" "$(cat "$open_log")"
	assertContains "Live socket reuse should run the -O check gate against the per-run socket." \
		"$result" "checked origin.example pfexec $expected_socket"
	assertContains "Live socket reuse should keep publishing the per-run socket path." \
		"$result" "socket=$expected_socket"
}

test_setup_ssh_control_socket_opens_master_once_for_fresh_run() {
	zxfer_ensure_run_tmp_root || fail "Unable to create the per-run temp root."
	g_zxfer_ssh_control_socket_dir_result=""

	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			check_log="$TEST_TMPDIR/setup_fresh_check.log"
			open_log="$TEST_TMPDIR/setup_fresh_open.log"
			: >"$check_log"
			: >"$open_log"
			zxfer_check_ssh_control_socket_for_host() {
				printf 'check\n' >>"$check_log"
				return 0
			}
			zxfer_open_ssh_control_socket_for_host() {
				printf 'open %s %s\n' "$1" "$2" >>"$open_log"
				return 0
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'checks=%s\n' "$(grep -c . "$check_log")"
			printf 'opens=%s\n' "$(grep -c . "$open_log")"
		)
	)

	assertContains "A fresh per-run setup should open the ssh master exactly once." \
		"$result" "opens=1"
	assertContains "A fresh per-run socket path cannot pre-exist, so no -O check runs before the first open." \
		"$result" "checks=0"
	assertContains "Fresh setup should publish the per-role socket path." \
		"$result" "/ssh-origin.sock"
}

test_close_origin_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_origin.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	: >"$g_ssh_origin_control_socket"

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Origin socket path should be cleared after closing." "" "$g_ssh_origin_control_socket"
	assertFalse "The origin socket file should be removed during close." \
		"[ -e \"$TEST_TMPDIR/origin.sock\" ]"
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

test_close_target_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_target.log"
	: >"$log"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"
	: >"$g_ssh_target_control_socket"

	zxfer_close_target_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Target socket path should be cleared after closing." "" "$g_ssh_target_control_socket"
	assertFalse "The target socket file should be removed during close." \
		"[ -e \"$TEST_TMPDIR/target.sock\" ]"
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

test_zxfer_ensure_remote_host_capabilities_fills_memory_from_one_live_probe() {
	probe_count_file="$TEST_TMPDIR/ensure-live-probe-count"
	printf '0\n' >"$probe_count_file"
	g_option_O_origin_host="origin.example"
	zxfer_fetch_remote_host_capabilities_live() {
		l_count=$(($(cat "$probe_count_file") + 1))
		printf '%s\n' "$l_count" >"$probe_count_file"
		g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
		printf '%s\n' "$g_zxfer_remote_capability_response_result"
	}

	first=$(zxfer_ensure_remote_host_capabilities "origin.example" source)
	first_status=$?
	# Plain (non-command-substitution) call so the in-memory store persists in
	# this shell, mirroring the preload flow.
	zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	second=$(zxfer_ensure_remote_host_capabilities "origin.example" source)
	second_status=$?
	probe_count=$(cat "$probe_count_file")
	bootstrap_source=$g_origin_remote_capabilities_bootstrap_source

	unset -f zxfer_fetch_remote_host_capabilities_live
	zxfer_source_runtime_modules_through "zxfer_replication.sh"

	assertEquals "The first capability bootstrap should succeed from the live probe." 0 "$first_status"
	assertContains "The first capability bootstrap should publish the live payload." \
		"$first" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "Memory-backed lookups should succeed after the warm-up call." 0 "$second_status"
	assertContains "Memory-backed lookups should replay the stored payload." \
		"$second" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "One warmed host should cost exactly two live probes before the memory tier fills (one per command-substituted call) and zero after." \
		2 "$probe_count"
	assertEquals "The warmed slot should record the live bootstrap source." \
		"live" "$bootstrap_source"
}

test_zxfer_ensure_remote_host_capabilities_preserves_live_probe_diagnostic() {
	set +e
	output=$(
		(
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
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

test_zxfer_ensure_remote_host_capabilities_never_treats_failed_probe_as_empty() {
	set +e
	output=$(
		(
			zxfer_get_cached_remote_capability_response_for_host() {
				return 1
			}
			zxfer_fetch_remote_host_capabilities_live() {
				return 37
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
			printf 'status=%s\n' "$?"
			printf 'stored=<%s>\n' "${g_origin_remote_capabilities_response:-}"
		)
	)

	assertContains "Remote capability ensure should propagate the live probe failure status." \
		"$output" "status=37"
	assertContains "A failed probe must never populate the in-memory capability state." \
		"$output" "stored=<>"
}

test_zxfer_refresh_ssh_transport_tokens_for_role_memoizes_rendered_tokens() {
	g_option_O_origin_host="origin.example pfexec"
	g_cmd_ssh="$FAKE_SSH_BIN"

	rendered=$(zxfer_render_ssh_transport_tokens_for_host "origin.example pfexec")
	zxfer_refresh_ssh_transport_tokens_for_role origin
	refresh_status=$?

	assertEquals "Per-role transport memo refresh should succeed for a valid origin spec." \
		0 "$refresh_status"
	assertEquals "Per-role transport memo refresh should mark the origin memo warm." \
		1 "${g_zxfer_ssh_transport_tokens_origin_set:-0}"
	assertEquals "The origin transport memo must be byte-identical to a fresh render." \
		"$rendered" "$g_zxfer_ssh_transport_tokens_origin"
	assertEquals "Warm memo reads should replay the rendered tokens." \
		"$rendered" "$(zxfer_get_ssh_transport_tokens_for_host "origin.example pfexec")"
}

test_zxfer_get_ssh_transport_tokens_for_host_falls_back_when_socket_state_changes() {
	g_option_O_origin_host="origin.example"
	g_cmd_ssh="$FAKE_SSH_BIN"
	zxfer_refresh_ssh_transport_tokens_for_role origin

	count_file="$TEST_TMPDIR/transport-render-count"
	printf '0\n' >"$count_file"
	output=$(
		(
			zxfer_render_ssh_transport_tokens_for_host() {
				l_count=$(($(cat "$count_file") + 1))
				printf '%s\n' "$l_count" >"$count_file"
				printf 'fresh-render\n'
			}
			zxfer_get_ssh_transport_tokens_for_host "origin.example" >/dev/null
			printf 'warm_renders=%s\n' "$(cat "$count_file")"
			# A socket opened after the memo was filled invalidates it.
			g_ssh_origin_control_socket="$TEST_TMPDIR/origin-memo.sock"
			fresh=$(zxfer_get_ssh_transport_tokens_for_host "origin.example")
			printf 'stale_renders=%s\n' "$(cat "$count_file")"
			printf 'fresh=%s\n' "$fresh"
			# Hosts outside the -O/-T roles always render fresh.
			zxfer_get_ssh_transport_tokens_for_host "elsewhere.example" >/dev/null
			printf 'other_renders=%s\n' "$(cat "$count_file")"
		)
	)

	assertContains "A warm matching memo should answer without a fresh render." \
		"$output" "warm_renders=0"
	assertContains "A control-socket change should bypass the stale memo." \
		"$output" "stale_renders=1"
	assertContains "Memo misses should return the freshly rendered tokens." \
		"$output" "fresh=fresh-render"
	assertContains "Non-role hosts should always render fresh tokens." \
		"$output" "other_renders=2"
}

test_zxfer_refresh_ssh_transport_tokens_for_role_skips_target_equal_to_origin() {
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_cmd_ssh="$FAKE_SSH_BIN"

	zxfer_refresh_ssh_transport_tokens_for_role origin
	zxfer_refresh_ssh_transport_tokens_for_role target

	assertEquals "The origin memo should warm for a shared origin/target host." \
		1 "${g_zxfer_ssh_transport_tokens_origin_set:-0}"
	assertEquals "The target memo must stay cold when the target spec equals the origin spec." \
		0 "${g_zxfer_ssh_transport_tokens_target_set:-0}"
}

test_zxfer_prepare_ssh_shell_command_context_memoizes_role_specs() {
	g_option_O_origin_host="origin.example pfexec"
	split_count_file="$TEST_TMPDIR/context-split-count"
	printf '0\n' >"$split_count_file"

	output=$(
		(
			zxfer_split_host_spec_tokens_real() {
				zxfer_validate_literal_token_string "$1" "Host spec (-O/-T)" >/dev/null || return 1
				zxfer_split_tokens_on_whitespace "$1"
			}
			zxfer_split_host_spec_tokens() {
				l_count=$(($(cat "$split_count_file") + 1))
				printf '%s\n' "$l_count" >"$split_count_file"
				zxfer_split_host_spec_tokens_real "$1"
			}
			zxfer_prepare_ssh_shell_command_context "origin.example pfexec" "echo one" || exit 91
			first_host=$g_zxfer_ssh_shell_host_result
			first_cmd=$g_zxfer_ssh_shell_full_remote_command_result
			zxfer_prepare_ssh_shell_command_context "origin.example pfexec" "echo two" || exit 92
			printf 'splits=%s\n' "$(cat "$split_count_file")"
			printf 'first_host=%s\n' "$first_host"
			printf 'first_cmd=%s\n' "$first_cmd"
			printf 'second_host=%s\n' "$g_zxfer_ssh_shell_host_result"
			printf 'second_cmd=%s\n' "$g_zxfer_ssh_shell_full_remote_command_result"
			# Non-role specs are parsed fresh every time.
			zxfer_prepare_ssh_shell_command_context "elsewhere.example sudo" "echo three" || exit 93
			printf 'other_splits=%s\n' "$(cat "$split_count_file")"
			zxfer_prepare_ssh_shell_command_context "elsewhere.example sudo" "echo four" || exit 94
			printf 'other_splits_again=%s\n' "$(cat "$split_count_file")"
		)
	)

	assertContains "The first role-spec parse should run the host-spec splitter once." \
		"$output" "splits=1"
	assertContains "The first parse should publish the bare ssh host." \
		"$output" "first_host=origin.example"
	assertContains "The first parse should wrap the remote command with the quoted wrapper tokens." \
		"$output" "first_cmd='pfexec' echo one"
	assertContains "Memoized role-spec parses should publish the same ssh host." \
		"$output" "second_host=origin.example"
	assertContains "Memoized role-spec parses should rewrap the new remote command identically." \
		"$output" "second_cmd='pfexec' echo two"
	assertContains "Non-role specs should not be memoized." \
		"$output" "other_splits=2"
	assertContains "Repeated non-role specs should parse fresh each time." \
		"$output" "other_splits_again=3"
}

test_zxfer_set_and_clear_ssh_control_socket_role_state_refresh_transport_memo() {
	g_option_O_origin_host="origin.example"
	g_cmd_ssh="$FAKE_SSH_BIN"

	zxfer_set_ssh_control_socket_role_state origin "$TEST_TMPDIR/role-state.sock"
	warm_socket=${g_zxfer_ssh_transport_tokens_origin_socket:-}
	warm_set=${g_zxfer_ssh_transport_tokens_origin_set:-0}
	warm_tokens=$g_zxfer_ssh_transport_tokens_origin

	zxfer_clear_ssh_control_socket_role_state origin
	cleared_socket=${g_zxfer_ssh_transport_tokens_origin_socket:-}
	cleared_tokens=$g_zxfer_ssh_transport_tokens_origin

	assertEquals "Setting role socket state should record the socket in the transport memo." \
		"$TEST_TMPDIR/role-state.sock" "$warm_socket"
	assertEquals "Setting role socket state should warm the transport memo." 1 "$warm_set"
	assertContains "The warmed memo should carry the -S socket tokens." \
		"$warm_tokens" "-S
$TEST_TMPDIR/role-state.sock"
	assertEquals "Clearing role socket state should drop the memoized socket." \
		"" "$cleared_socket"
	assertNotContains "The refreshed memo should no longer carry socket tokens." \
		"$cleared_tokens" "-S"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
