#!/bin/sh
# Remote capability parsing, caching, live probing, and tool-resolution behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_remote_host_direct_load_includes_transport_but_not_snapshot_state() {
	/bin/sh -c '
		ZXFER_SOURCE_MODULES_ROOT=$1
		. "$1/src/zxfer_modules.sh"
		zxfer_load_modules zxfer_remote_hosts.sh || exit 1
		command -v zxfer_ensure_remote_host_capabilities >/dev/null 2>&1 || exit 2
		command -v zxfer_build_ssh_shell_command_for_host >/dev/null 2>&1 || exit 3
		command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1 && exit 4
		exit 0
	' zxfer-remote-direct-load "$ZXFER_ROOT"

	assertEquals "Remote capability loading should include transport without pulling snapshot state." \
		0 "$?"
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
			zxfer_get_parsed_remote_capability_tool_record zfs
			printf 'zfs=%s:%s\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
			zxfer_get_parsed_remote_capability_tool_record parallel
			printf 'parallel=%s:%s\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
			zxfer_get_parsed_remote_capability_tool_record cat
			printf 'cat=%s:%s\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
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
tool	cat	1	-
end"
			zxfer_get_parsed_remote_capability_tool_record parallel
			printf 'parallel=%s:%s\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
			zxfer_get_parsed_remote_capability_tool_record cat
			printf 'cat=%s:%s\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
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
tool	cat	0	/remote/bin/cat
end"
			printf 'zfs_status=%s\n' "$g_zxfer_remote_capability_zfs_status"
			zxfer_get_parsed_remote_capability_tool_record cat
			printf 'cat_path=%s\n' "$g_zxfer_remote_capability_tool_path_result"
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

test_zxfer_parse_remote_capability_response_preserves_unset_ifs_and_globbing() {
	(
		unset IFS
		set -f
		zxfer_parse_remote_capability_response "$(fake_remote_capability_response)" || exit 8
		[ "${IFS+set}" != "set" ] || exit 9
		case $- in
		*f*) ;;
		*) exit 10 ;;
		esac
	)
	l_status=$?

	assertEquals "Capability parsing must restore an originally unset IFS and disabled globbing." \
		0 "$l_status"
}

test_zxfer_remote_capability_scope_rejects_truncated_requested_tool_payload() {
	truncated_response='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs'

	zxfer_parse_remote_capability_response "$truncated_response"
	parse_status=$?

	assertEquals "Capability framing must reject a zfs-only prefix that is truncated before the explicit end record." \
		1 "$parse_status"
}

test_zxfer_remote_capability_probe_script_matches_framed_protocol_golden() {
	actual_script="$TEST_TMPDIR/remote-capability-probe-script.actual"
	golden_script="$ZXFER_ROOT/tests/golden/remote_capability_probe_script.golden"
	(
		zxfer_get_effective_dependency_path() {
			printf '%s\n' '/secure/bin:/usr/bin'
		}
		zxfer_build_remote_capability_probe_script \
			"origin.example" "zfs
parallel"
	) >"$actual_script"
	build_status=$?

	assertEquals "The capability probe renderer should succeed for a fixed secure PATH and requested-tool scope." \
		0 "$build_status"
	assertEquals "The rendered remote capability probe must retain the exact framed V2 protocol, including its end sentinel." \
		"$(cat "$golden_script")" "$(cat "$actual_script")"
}

test_zxfer_truncated_remote_capability_probe_falls_back_to_direct_tool_resolution() {
	probe_count_file="$TEST_TMPDIR/truncated-capability-probe-count"
	printf '0\n' >"$probe_count_file"
	output=$(
		(
			set +e
			zxfer_capture_remote_probe_output() {
				l_probe_count=$(($(cat "$probe_count_file") + 1))
				printf '%s\n' "$l_probe_count" >"$probe_count_file"
				if [ "$l_probe_count" -eq 1 ]; then
					g_zxfer_remote_probe_stdout='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs'
				else
					g_zxfer_remote_probe_stdout='/fallback/bin/parallel'
				fi
				g_zxfer_remote_probe_stderr=""
				return 0
			}
			resolved=$(zxfer_resolve_remote_required_tool \
				"origin.example" parallel parallel source)
			printf 'status=%s\n' "$?"
			printf 'resolved=%s\n' "$resolved"
		)
	)
	probe_count=$(cat "$probe_count_file")

	assertContains "A truncated multi-tool handshake should fail closed and use the established direct secure probe." \
		"$output" "status=0"
	assertContains "Direct fallback after a truncated handshake should publish only the validated helper path." \
		"$output" "resolved=/fallback/bin/parallel"
	assertEquals "Truncation fallback should perform one capability handshake and one direct helper probe." \
		2 "$probe_count"
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

test_zxfer_remote_capability_cache_isolates_roles_with_the_same_host_spec() {
	shared_host="shared.example"
	g_option_O_origin_host=$shared_host
	g_option_T_target_host=$shared_host
	origin_response=$(fake_remote_capability_response)
	target_response=$(printf '%s\n' "$origin_response" | sed 's/os\tRemoteOS/os\tTargetOS/')

	zxfer_store_cached_remote_capability_response_for_host \
		"$shared_host" "$origin_response" zfs source
	zxfer_store_cached_remote_capability_response_for_host \
		"$shared_host" "$target_response" zfs destination
	origin_cached=$(zxfer_get_cached_remote_capability_response_for_host \
		"$shared_host" zfs source)
	target_cached=$(zxfer_get_cached_remote_capability_response_for_host \
		"$shared_host" zfs destination)
	origin_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$shared_host" zfs source)
	target_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$shared_host" zfs destination)

	assertContains "An origin cache lookup must retain the origin response when both roles use one host spec." \
		"$origin_cached" "os	RemoteOS"
	assertNotContains "An origin lookup must not cross-read the target cache slot for a shared host." \
		"$origin_cached" "os	TargetOS"
	assertContains "A target cache lookup must retain the target response when both roles use one host spec." \
		"$target_cached" "os	TargetOS"
	assertNotEquals "Capability cache identities must include endpoint role even when host and requested tools match." \
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
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	result=$(zxfer_get_cached_remote_capability_response_for_host "origin.example")

	assertContains "Origin-side cached capability reads should return the cached payload." \
		"$result" "tool	parallel	0	/opt/bin/parallel"
}

test_zxfer_get_cached_remote_capability_response_for_host_reads_target_slot() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
	g_target_remote_capabilities_response=$(fake_remote_capability_response)

	result=$(zxfer_get_cached_remote_capability_response_for_host "target.example")

	assertContains "Target-side cached capability reads should return the cached payload." \
		"$result" "tool	cat	0	/remote/bin/cat"
}

test_zxfer_get_cached_remote_capability_response_for_host_rejects_mismatched_requested_tool_identity() {
	g_origin_remote_capabilities_host="origin.example"
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
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host "" "")
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	zxfer_store_cached_remote_capability_response_for_host "other.example" "$(fake_remote_capability_response)"

	assertEquals "Once the origin fallback slot is occupied, later unassigned cache responses should populate the target slot." \
		"other.example" "$g_target_remote_capabilities_host"
}

test_zxfer_ensure_remote_host_capabilities_prefers_memory_cache() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_cache_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"origin.example" "" source)
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
	l_probe_tmpdir="$g_zxfer_run_tmp_root/remote_probe_capture_readback"
	rm -rf "$l_probe_tmpdir"

	set +e
	output=$(
		(
			# The production allocator returns a direct child of the genuine
			# run root allocated above. Keep this failure injection inside that
			# ownership boundary so hardened cleanup exercises its normal path.
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
end
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
end
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
end
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
