#!/bin/sh
# Remote capability parsing, tool scope, per-role cache, probe capture, and
# tool-resolution tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Stand in for zxfer_capture_remote_probe_output: count probes in
# ZXFER_TEST_PROBE_COUNT_FILE, log each probe's side and command to
# ZXFER_TEST_PROBE_LOG, and answer with ZXFER_TEST_PROBE_RESPONSE (the
# origin response) or ZXFER_TEST_TARGET_PROBE_RESPONSE for the target side.
zxfer_test_stub_remote_probe_capture() {
	zxfer_capture_remote_probe_output() {
		l_test_probe_count=$(($(cat "$ZXFER_TEST_PROBE_COUNT_FILE") + 1))
		printf '%s\n' "$l_test_probe_count" >"$ZXFER_TEST_PROBE_COUNT_FILE"
		printf '%s %s\n' "${3:-}" "$2" >>"$ZXFER_TEST_PROBE_LOG"
		g_zxfer_remote_probe_stdout=$ZXFER_TEST_PROBE_RESPONSE
		if [ "${3:-}" = destination ] && [ -n "${ZXFER_TEST_TARGET_PROBE_RESPONSE:-}" ]; then
			g_zxfer_remote_probe_stdout=$ZXFER_TEST_TARGET_PROBE_RESPONSE
		fi
		g_zxfer_remote_probe_stderr=""
		return 0
	}
}

# Stand in for ssh in the OS fallback test: the capability probe fails
# (CASE_NAME=unavailable) or returns a response without an os line
# (CASE_NAME=malformed); the direct uname probe logs to $log and answers.
zxfer_test_stub_os_fallback_invoke() {
	zxfer_invoke_ssh_shell_command_for_host() {
		case $2 in
		*ZXFER_REMOTE_CAPS_V2*)
			[ "$CASE_NAME" = malformed ] || return 255
			printf 'ZXFER_REMOTE_CAPS_V2\ntool\tzfs\t0\t/remote/bin/zfs\nend\n'
			return 0
			;;
		esac
		printf '%s|%s|%s\n' "$1" "$2" "${3:-}" >"$log"
		printf '%s\n' "FallbackOS" "ignored-extra-line"
	}
}

# Stand in for the live capability fetch: it fails (CASE_NAME=unavailable),
# publishes an unparsable response (malformed) or one without the requested
# tool (absent).
zxfer_test_stub_fetch_without_tool_record() {
	zxfer_fetch_remote_host_capabilities_live() {
		case $CASE_NAME in
		unavailable) return 1 ;;
		malformed) zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2" ;;
		*)
			zxfer_parse_remote_capability_response 'ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
end'
			;;
		esac
	}
}

# Stand in for ssh in the direct tool probe: DIRECT_CASE picks the outcome.
zxfer_test_stub_direct_tool_probe_invoke() {
	zxfer_invoke_ssh_shell_command_for_host() {
		case $DIRECT_CASE in
		missing) return 10 ;;
		stderr)
			printf '%s\n' "Host key verification failed." >&2
			return 255
			;;
		noise)
			printf '%s\n' "wrapper startup noise"
			return 255
			;;
		*) printf '%s\n' "bin/zfs" ;;
		esac
	}
}

# Reset the probe count and log used by zxfer_test_stub_remote_probe_capture.
zxfer_test_reset_remote_probe_counters() {
	ZXFER_TEST_PROBE_COUNT_FILE="$TEST_TMPDIR/remote-probe.count"
	ZXFER_TEST_PROBE_LOG="$TEST_TMPDIR/remote-probe.log"
	printf '0\n' >"$ZXFER_TEST_PROBE_COUNT_FILE"
	: >"$ZXFER_TEST_PROBE_LOG"
	ZXFER_TEST_PROBE_RESPONSE=$(fake_remote_capability_response)
	ZXFER_TEST_TARGET_PROBE_RESPONSE=""
}

test_remote_host_direct_load_includes_transport_but_not_snapshot_state() {
	/bin/sh -c '
		ZXFER_SOURCE_MODULES_ROOT=$1
		. "$1/src/zxfer_modules.sh"
		zxfer_load_modules zxfer_remote_hosts.sh || exit 1
		command -v zxfer_ensure_remote_host_capabilities >/dev/null 2>&1 || exit 2
		command -v zxfer_invoke_ssh_shell_command_for_host >/dev/null 2>&1 || exit 3
		command -v zxfer_reset_live_destination_listing_state >/dev/null 2>&1 && exit 4
		exit 0
	' zxfer-remote-direct-load "$ZXFER_ROOT"

	assertEquals "Remote capability loading should include transport without pulling snapshot state." \
		0 "$?"
}

################################################################################
# Parser
################################################################################

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
tool	cat	127	-
end"
			zxfer_get_parsed_remote_capability_tool_record parallel
			printf 'parallel=<%s:%s>\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
			zxfer_get_parsed_remote_capability_tool_record cat
			printf 'cat=<%s:%s>\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
		)
	)

	assertContains "A missing helper should keep status 1 with an empty path." \
		"$result" "parallel=<1:>"
	assertContains "Any other lookup status should be kept with an empty path." \
		"$result" "cat=<127:>"
}

test_zxfer_parse_remote_capability_response_rejects_retired_v1_protocol() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
end"
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
tool	cat	0	/remote/bin/cat
end"
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
end"
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
			printf 'weirdtool=%s:%s\n' "$g_zxfer_remote_capability_tool_status_result" "$g_zxfer_remote_capability_tool_path_result"
		)
	)
	status=$?

	assertEquals "Capability records should tolerate additional advertised tool names." 0 "$status"
	assertContains "Capability records with additional tool names should preserve the required zfs status." \
		"$output" "zfs_status=0"
	assertContains "Capability records with additional tool names should preserve known helper paths." \
		"$output" "cat_path=/remote/bin/cat"
	assertContains "Capability records with additional tool names should keep the extra tool record." \
		"$output" "weirdtool=0:/remote/bin/weirdtool"
}

test_zxfer_parse_remote_capability_response_preserves_unset_ifs_and_globbing() {
	(
		unset IFS
		set -f
		zxfer_parse_remote_capability_response "$(fake_remote_capability_response)" \
			"zfs parallel" || exit 8
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

test_zxfer_parse_remote_capability_response_rejects_a_response_truncated_before_end() {
	truncated_response='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs'

	zxfer_parse_remote_capability_response "$truncated_response"
	parse_status=$?

	assertEquals "Capability framing must reject a zfs-only prefix that is truncated before the explicit end record." \
		1 "$parse_status"
}

test_zxfer_parse_remote_capability_response_rejects_extra_lines() {
	set +e
	zxfer_parse_remote_capability_response "$(fake_remote_capability_response)
extra	line"
	status=$?

	assertEquals "Capability records with a line after the end record should be rejected." 1 "$status"
}

test_zxfer_parse_remote_capability_response_rejects_control_whitespace_helper_paths() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	set +e
	zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os${tab}RemoteOS
tool${tab}zfs${tab}0${tab}/remote/bin/zfs${cr}
tool${tab}parallel${tab}0${tab}/opt/bin/parallel
end"
	status=$?

	assertEquals "Capability payloads with control-whitespace helper paths should be rejected as invalid handshakes." \
		1 "$status"
}

test_zxfer_parse_remote_capability_response_rejects_duplicate_tool_records() {
	set +e
	zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	zfs	0	/remote/bin/zfs-second
end"
	status=$?

	assertEquals "Capability parsing should reject duplicate tool records." \
		1 "$status"
}

test_zxfer_parse_remote_capability_response_fails_closed_on_each_record_shape_problem() {
	tab=$(printf '\t')
	cr=$(printf '\r')
	header="ZXFER_REMOTE_CAPS_V2
os${tab}RemoteOS"
	zfs_record="tool${tab}zfs${tab}0${tab}/remote/bin/zfs"
	failures=""
	for case_spec in \
		"five fields|$zfs_record${tab}extra" \
		"three fields|tool${tab}zfs${tab}0" \
		"empty tool name|tool${tab}${tab}0${tab}/remote/bin/zfs" \
		"carriage return in tool name|tool${tab}zfs${cr}${tab}0${tab}/remote/bin/zfs" \
		"empty status|tool${tab}zfs${tab}${tab}/remote/bin/zfs" \
		"found tool without a path|tool${tab}zfs${tab}0${tab}-" \
		"missing tool with a path|$zfs_record
tool${tab}cat${tab}1${tab}/remote/bin/cat" \
		"relative helper path|tool${tab}zfs${tab}0${tab}bin/zfs" \
		"unknown record kind|$zfs_record
helper${tab}cat${tab}0${tab}/remote/bin/cat" \
		"no zfs record|tool${tab}cat${tab}0${tab}/remote/bin/cat" \
		"blank record line|$zfs_record
"; do
		case_name=${case_spec%%|*}
		case_records=${case_spec#*|}
		if zxfer_parse_remote_capability_response "$header
$case_records
end"; then
			failures="$failures [$case_name]"
		fi
	done

	assertEquals "Every malformed record shape should fail the whole response." \
		"" "$failures"
}

test_zxfer_parse_remote_capability_response_requires_each_requested_tool() {
	response='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	cat	1	-
end'

	zxfer_parse_remote_capability_response "$response" "zfs cat"
	covered_status=$?
	zxfer_parse_remote_capability_response "$response" "zfs parallel"
	missing_status=$?

	assertEquals "A response with a record for every requested tool should be accepted." \
		0 "$covered_status"
	assertEquals "A response without a record for a requested tool should be rejected as truncated." \
		1 "$missing_status"
}

test_zxfer_parse_remote_capability_response_stores_normalized_helper_paths() {
	zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V2
os	SunOS
tool	zfs	0	'/usr/sbin/zfs'
end"
	status=$?
	zxfer_get_parsed_remote_capability_tool_record zfs

	assertEquals "A quoted command -v path (OmniOS sh) should be accepted." 0 "$status"
	assertEquals "The stored path should be the validated, unquoted path." \
		"/usr/sbin/zfs" "$g_zxfer_remote_capability_tool_path_result"
}

test_zxfer_get_parsed_remote_capability_tool_record_rejects_missing_and_partial_records() {
	g_zxfer_remote_capability_tool_records=$(printf 'zfs\t0')
	zxfer_get_parsed_remote_capability_tool_record zfs
	partial_status=$?
	g_zxfer_remote_capability_tool_records=$(printf 'zfs\t0\t/sbin/zfs')
	zxfer_get_parsed_remote_capability_tool_record parallel
	missing_status=$?
	zxfer_get_parsed_remote_capability_tool_record ""
	empty_status=$?

	assertEquals "A record without its path field should not be returned." 1 "$partial_status"
	assertEquals "A tool without a record should not be returned." 1 "$missing_status"
	assertEquals "An empty tool name should not match any record." 1 "$empty_status"
}

################################################################################
# Probe script
################################################################################

test_zxfer_remote_capability_probe_script_matches_framed_protocol_golden() {
	actual_script="$TEST_TMPDIR/remote-capability-probe-script.actual"
	golden_script="$ZXFER_ROOT/tests/golden/remote_capability_probe_script.golden"
	g_zxfer_secure_path='/secure/bin:/usr/bin'

	zxfer_build_remote_capability_probe_script "zfs parallel" >"$actual_script"
	build_status=$?
	expected_transport=$(awk 'NF' "$golden_script" | paste -s -d ' ' -)

	assertEquals "The capability probe renderer should succeed for a fixed secure PATH and requested-tool scope." \
		0 "$build_status"
	assertEquals "The rendered remote capability probe must retain the exact framed V2 protocol, including its end sentinel." \
		"$(cat "$golden_script")" "$(cat "$actual_script")"
	assertEquals "The transport form should be the nonblank lines joined by single spaces." \
		"$expected_transport" "$g_zxfer_remote_capability_probe_script_result"
}

test_zxfer_remote_capability_probe_rejects_multiline_secure_path_before_rendering() {
	g_zxfer_secure_path=$(printf "/opt/trusted/bin\n/opt/translated/bin")
	set +e
	probe_script=$(zxfer_build_remote_capability_probe_script "zfs")
	probe_status=$?
	zxfer_build_remote_capability_probe_script "zfs" >/dev/null

	assertEquals "Capability rendering must fail before a secure PATH newline can be translated by one-line transport." \
		1 "$probe_status"
	assertEquals "Rejected secure-PATH configuration must not print a partial capability script." \
		"" "$probe_script"
	assertEquals "Rejected secure-PATH configuration must not publish a transport script." \
		"" "$g_zxfer_remote_capability_probe_script_result"
}

################################################################################
# Tool scope
################################################################################

test_zxfer_get_remote_capability_requested_tools_for_host_covers_each_role() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_option_j_jobs=4
	g_option_e_restore_property_mode=1
	g_option_k_backup_property_mode=1
	g_option_z_compress=1
	g_cmd_compress="zstd -T0 -9"
	g_cmd_decompress="xz -d"

	zxfer_get_remote_capability_requested_tools_for_host origin.example
	origin_tools=$g_zxfer_remote_capability_requested_tools_result
	zxfer_get_remote_capability_requested_tools_for_host target.example
	target_tools=$g_zxfer_remote_capability_requested_tools_result
	zxfer_get_remote_capability_requested_tools_for_host other.example
	other_tools=$g_zxfer_remote_capability_requested_tools_result

	assertEquals "The origin scope should hold zfs, parallel for -j, cat for -e and the compression head." \
		"zfs parallel cat zstd" "$origin_tools"
	assertEquals "The target scope should hold zfs, cat for -k and the decompression head." \
		"zfs cat xz" "$target_tools"
	assertEquals "A host that is neither -O nor -T should only need zfs." \
		"zfs" "$other_tools"
}

test_zxfer_get_remote_capability_requested_tools_for_host_takes_the_union_for_a_shared_host() {
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_option_j_jobs=4
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=1
	g_option_z_compress=1
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"

	zxfer_get_remote_capability_requested_tools_for_host shared.example

	assertEquals "A host that is both -O and -T should get each helper once." \
		"zfs parallel zstd cat" "$g_zxfer_remote_capability_requested_tools_result"
}

test_zxfer_get_remote_capability_requested_tools_for_host_defers_parallel_for_fast_noop_scope() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host=""
	g_option_R_recursive="tank/src"
	g_option_j_jobs=4
	g_option_U_skip_unsupported_properties=1
	g_option_g_grandfather_protection="enabled"

	zxfer_get_remote_capability_requested_tools_for_host origin.example
	host_tools=$g_zxfer_remote_capability_requested_tools_result
	zxfer_get_remote_capability_requested_tools_for_host origin.example parallel
	parallel_tools=$g_zxfer_remote_capability_requested_tools_result

	assertEquals "Fast recursive no-op startup scopes should defer parallel until the proof finds work." \
		"zfs" "$host_tools"
	assertEquals "An on-demand parallel lookup should still ask for parallel." \
		"zfs parallel" "$parallel_tools"
}

test_zxfer_get_remote_capability_requested_tools_for_host_narrows_to_a_tool_outside_the_scope() {
	g_option_O_origin_host="origin.example"
	g_option_e_restore_property_mode=1

	zxfer_get_remote_capability_requested_tools_for_host origin.example cat
	inside_tools=$g_zxfer_remote_capability_requested_tools_result
	zxfer_get_remote_capability_requested_tools_for_host origin.example zfs
	zfs_tools=$g_zxfer_remote_capability_requested_tools_result
	zxfer_get_remote_capability_requested_tools_for_host origin.example xz
	outside_tools=$g_zxfer_remote_capability_requested_tools_result

	assertEquals "A tool inside the host scope should keep the whole scope, so the cache is shared." \
		"zfs cat" "$inside_tools"
	assertEquals "zfs is always inside the scope." \
		"zfs cat" "$zfs_tools"
	assertEquals "A tool outside the host scope should narrow the probe to zfs and that tool." \
		"zfs xz" "$outside_tools"
}

test_zxfer_get_remote_capability_requested_tools_for_host_keeps_ifs_and_globbing() {
	(
		unset IFS
		set +f
		g_option_O_origin_host="origin.example"
		g_option_z_compress=1
		g_cmd_compress="zst* -3"
		zxfer_get_remote_capability_requested_tools_for_host origin.example
		[ "$g_zxfer_remote_capability_requested_tools_result" = "zfs zst*" ] || exit 8
		[ "${IFS+set}" != "set" ] || exit 9
		case $- in
		*f*) exit 10 ;;
		esac
	)

	assertEquals "The tool scope must take heads literally and restore IFS and globbing." \
		0 "$?"
}

test_zxfer_get_remote_capability_requested_tools_for_host_splits_heads_like_the_resolver() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_option_z_compress=1
	g_cmd_compress="zstd|x -3"
	g_cmd_decompress="'zstd' -d"

	zxfer_get_remote_capability_requested_tools_for_host origin.example
	origin_tools=$g_zxfer_remote_capability_requested_tools_result
	zxfer_get_remote_capability_requested_tools_for_host target.example
	target_tools=$g_zxfer_remote_capability_requested_tools_result

	assertEquals "A separator should end the compression head, as it does for the resolver." \
		"zfs zstd|" "$origin_tools"
	assertEquals "A quoted command the resolver rejects should add no helper." \
		"zfs" "$target_tools"
}

################################################################################
# Per-role cache
################################################################################

test_zxfer_ensure_remote_host_capabilities_probes_once_per_role_host_and_scope() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			set +e
			zxfer_test_stub_remote_probe_capture
			g_option_V_very_verbose=1
			g_zxfer_profile_remote_capability_bootstrap_live=0
			g_zxfer_profile_remote_capability_bootstrap_memory=0
			g_option_O_origin_host="origin.example"

			zxfer_ensure_remote_host_capabilities origin.example source >/dev/null || exit 11
			zxfer_ensure_remote_host_capabilities origin.example source >/dev/null || exit 12
			zxfer_ensure_remote_host_capabilities origin.example source zfs >/dev/null || exit 13
			printf 'after_reuse=%s\n' "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
			zxfer_ensure_remote_host_capabilities origin.example source parallel >/dev/null || exit 14
			zxfer_ensure_remote_host_capabilities origin.example source >/dev/null || exit 15
			zxfer_ensure_remote_host_capabilities other.example source >/dev/null || exit 16
			zxfer_ensure_remote_host_capabilities origin.example destination >/dev/null || exit 17
			printf 'live=%s\n' "$g_zxfer_profile_remote_capability_bootstrap_live"
			printf 'memory=%s\n' "$g_zxfer_profile_remote_capability_bootstrap_memory"
			printf 'origin_slot=%s|%s\n' "$g_origin_remote_capabilities_host" "$g_origin_remote_capabilities_tools"
			printf 'target_slot=%s|%s\n' "$g_target_remote_capabilities_host" "$g_target_remote_capabilities_tools"
		) 2>/dev/null
	)

	assertContains "Repeat lookups for the same role, host and scope should come from memory." \
		"$output" "after_reuse=1"
	assertEquals "A new scope, a new host and the other role should each probe once more." \
		5 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
	assertContains "-V should count every live probe." "$output" "live=5"
	assertContains "-V should count every memory hit." "$output" "memory=2"
	assertContains "The origin slot should hold the last origin host and scope." \
		"$output" "origin_slot=other.example|zfs"
	assertContains "The target slot should be filled only by destination lookups." \
		"$output" "target_slot=origin.example|zfs"
}

test_zxfer_ensure_remote_host_capabilities_isolates_roles_for_a_shared_host() {
	zxfer_test_reset_remote_probe_counters
	ZXFER_TEST_PROBE_RESPONSE='ZXFER_REMOTE_CAPS_V2
os	OriginOS
tool	zfs	0	/origin/bin/zfs
end'
	ZXFER_TEST_TARGET_PROBE_RESPONSE='ZXFER_REMOTE_CAPS_V2
os	TargetOS
tool	zfs	0	/target/bin/zfs
end'
	output=$(
		(
			set +e
			zxfer_test_stub_remote_probe_capture
			g_option_O_origin_host="shared.example"
			g_option_T_target_host="shared.example"

			zxfer_ensure_remote_host_capabilities shared.example source >/dev/null || exit 31
			printf 'origin_os=%s\n' "$g_zxfer_remote_capability_os"
			zxfer_ensure_remote_host_capabilities shared.example destination >/dev/null || exit 32
			printf 'target_os=%s\n' "$g_zxfer_remote_capability_os"
			zxfer_get_parsed_remote_capability_tool_record zfs || exit 33
			printf 'target_zfs=%s\n' "$g_zxfer_remote_capability_tool_path_result"
			zxfer_ensure_remote_host_capabilities shared.example source >/dev/null || exit 34
			printf 'origin_reloaded_os=%s\n' "$g_zxfer_remote_capability_os"
			zxfer_get_parsed_remote_capability_tool_record zfs || exit 35
			printf 'origin_reloaded_zfs=%s\n' "$g_zxfer_remote_capability_tool_path_result"
		)
	)
	status=$?

	assertEquals "Both roles of a shared host should load their capabilities." 0 "$status"
	assertContains "The origin role should keep the origin response." "$output" "origin_os=OriginOS"
	assertContains "The target role should keep the target response." "$output" "target_os=TargetOS"
	assertContains "The target role should publish its own tool records." "$output" "target_zfs=/target/bin/zfs"
	assertContains "Reloading the origin role must not cross-read the target slot." \
		"$output" "origin_reloaded_os=OriginOS"
	assertContains "Reloading the origin role should restore its tool records." \
		"$output" "origin_reloaded_zfs=/origin/bin/zfs"
	assertEquals "Each role should probe once." 2 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
}

test_zxfer_ensure_remote_host_capabilities_prints_the_response_on_a_miss_and_a_hit() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			zxfer_test_stub_remote_probe_capture
			zxfer_ensure_remote_host_capabilities origin.example source
			printf '%s\n' ---
			zxfer_ensure_remote_host_capabilities origin.example source
		)
	)

	assertEquals "Both the live probe and the memory hit should print the accepted response." \
		"$(fake_remote_capability_response)
---
$(fake_remote_capability_response)" "$output"
}

test_zxfer_ensure_remote_host_capabilities_stores_nothing_after_a_failed_validation() {
	zxfer_test_reset_remote_probe_counters
	ZXFER_TEST_PROBE_RESPONSE='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs'
	output=$(
		(
			set +e
			zxfer_test_stub_remote_probe_capture
			zxfer_ensure_remote_host_capabilities origin.example source >/dev/null
			printf 'first=%s\n' "$?"
			zxfer_ensure_remote_host_capabilities origin.example source >/dev/null
			printf 'second=%s\n' "$?"
			printf 'slot=<%s|%s>\n' "$g_origin_remote_capabilities_host" "$g_origin_remote_capabilities_os"
			printf 'response=<%s>\n' "$g_zxfer_remote_capability_response_result"
		)
	)

	assertContains "A truncated response should fail the lookup." "$output" "first=1"
	assertContains "A later lookup should fail again rather than reuse anything." "$output" "second=1"
	assertContains "A rejected response must not fill the role slot." "$output" "slot=<|>"
	assertContains "A rejected response must not be published." "$output" "response=<>"
	assertEquals "Each lookup after a rejected response should probe again." \
		2 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
}

test_zxfer_ensure_remote_host_capabilities_clears_the_channel_when_a_probe_fails() {
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				[ "$1" = good.example ] || return 255
				fake_remote_capability_response
			}
			zxfer_ensure_remote_host_capabilities good.example source >/dev/null || exit 1
			zxfer_ensure_remote_host_capabilities bad.example source >/dev/null 2>&1 && exit 2
			printf 'os=<%s> zfs=<%s> records=<%s> response=<%s>\n' \
				"$g_zxfer_remote_capability_os" "$g_zxfer_remote_capability_zfs_status" \
				"$g_zxfer_remote_capability_tool_records" "$g_zxfer_remote_capability_response_result"
		)
	)

	assertEquals "A failed probe must not leave the previous host's capabilities in the channel." \
		"os=<> zfs=<> records=<> response=<>" "$output"
}

test_zxfer_ensure_remote_host_capabilities_rejects_unknown_sides_and_empty_hosts() {
	zxfer_test_reset_remote_probe_counters
	(
		zxfer_test_stub_remote_probe_capture
		zxfer_ensure_remote_host_capabilities origin.example other >/dev/null && exit 1
		zxfer_ensure_remote_host_capabilities "" source >/dev/null && exit 2
		exit 0
	)

	assertEquals "Lookups need a host and, when given, a source or destination side." 0 "$?"
	assertEquals "Rejected lookups must not probe." 0 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
}

test_zxfer_ensure_remote_host_capabilities_picks_the_slot_from_the_host_role_without_a_side() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			zxfer_test_stub_remote_probe_capture
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_ensure_remote_host_capabilities target.example >/dev/null || exit 1
			zxfer_ensure_remote_host_capabilities unlisted.example >/dev/null || exit 2
			printf 'origin=%s target=%s\n' "$g_origin_remote_capabilities_host" \
				"$g_target_remote_capabilities_host"
		)
	)

	assertEquals "The -T host should use the target slot and any other host the origin slot." \
		"origin=unlisted.example target=target.example" "$output"
}

################################################################################
# Live probe
################################################################################

test_zxfer_fetch_remote_host_capabilities_live_pins_the_secure_path_and_requested_tools() {
	log_file="$TEST_TMPDIR/remote_caps_live_env.log"
	output=$(
		(
			g_zxfer_secure_path="/fresh/secure/path:/usr/bin"
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "$2" >"$log_file"
				fake_remote_capability_response
			}
			zxfer_fetch_remote_host_capabilities_live "origin.example" source "zfs cat" || exit 1
			printf '%s\n' "$g_zxfer_remote_capability_response_result"
		)
	)

	assertContains "Live probes should publish the accepted capability payload." \
		"$output" "tool	cat	0	/remote/bin/cat"
	assertContains "Live probes should pin the secure PATH." \
		"$(cat "$log_file")" "PATH='\\''/fresh/secure/path:/usr/bin'\\''"
	assertContains "Live probes should ask for exactly the requested tools." \
		"$(cat "$log_file")" "for l_tool in '\\''zfs'\\'' '\\''cat'\\''; do"
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
	g_zxfer_secure_path="$secure_bin_dir"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	zxfer_fetch_remote_host_capabilities_live "backup@example.com" source "zfs" \
		2>"$stderr_file"
	status=$?
	printf '%s\n' "$g_zxfer_remote_capability_response_result" >"$stdout_file"

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
	assertEquals "The csh/tcsh transport should receive one physical command line after the logged host line." \
		2 "$(sed -n '$=' "$realistic_ssh_log")"
}

test_zxfer_fetch_remote_host_capabilities_live_preserves_transport_diagnostic() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Host key verification failed." >&2
				return 255
			}
			zxfer_fetch_remote_host_capabilities_live "origin.example" source zfs
		) 2>&1
	)
	status=$?

	assertEquals "Remote capability handshakes should fail when ssh transport setup fails." 1 "$status"
	assertContains "Remote capability handshakes should preserve the underlying transport diagnostic." \
		"$output" "Host key verification failed."
}

test_zxfer_preload_remote_host_capabilities_delegates_to_ensure() {
	log="$TEST_TMPDIR/preload_remote_caps.log"
	: >"$log"

	(
		zxfer_ensure_remote_host_capabilities() {
			printf 'ensure host=%s side=%s args=%s\n' "$1" "${2:-}" "$#" >>"$log"
		}
		zxfer_preload_remote_host_capabilities "origin.example" source
	)

	assertEquals "Capability preloading should ask ensure for the whole host scope." \
		"ensure host=origin.example side=source args=2" "$(cat "$log")"
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

################################################################################
# Remote OS
################################################################################

test_zxfer_get_os_answers_from_the_host_capabilities() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			zxfer_test_stub_remote_probe_capture
			g_option_O_origin_host="origin.example"
			zxfer_get_os "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should succeed through the capability probe." 0 "$status"
	assertEquals "Remote OS lookups should return the capability OS." "RemoteOS" "$output"
	assertEquals "Remote OS lookups should need only the capability probe." \
		1 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
}

test_zxfer_get_os_falls_back_to_a_direct_uname_probe() {
	log="$TEST_TMPDIR/remote_os_direct.log"
	for case_name in unavailable malformed; do
		: >"$log"
		output=$(
			(
				CASE_NAME=$case_name
				g_zxfer_secure_path="/fresh/secure/path:/usr/bin"
				zxfer_test_stub_os_fallback_invoke
				zxfer_get_os "origin.example" source
			)
		)

		assertEquals "A $case_name capability probe should fall back to the first line of a direct uname probe." \
			"FallbackOS" "$output"
		assertContains "The direct OS probe should target the requested host." \
			"$(cat "$log")" "origin.example|"
		assertContains "The direct OS probe should pin the secure PATH." \
			"$(cat "$log")" "PATH='\\''/fresh/secure/path:/usr/bin'\\''; export PATH; uname 2>/dev/null"
		assertContains "The direct OS probe should keep the profile side." \
			"$(cat "$log")" "|source"
	done
}

test_zxfer_get_os_preserves_the_direct_probe_failure() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Permission denied (publickey)." >&2
				return 255
			}
			zxfer_get_os "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should fail when both probes fail." 1 "$status"
	assertEquals "Remote OS lookups should print the direct probe's stderr." \
		"Permission denied (publickey)." "$output"
}

test_zxfer_get_os_rejects_empty_direct_probe_output() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 0
			}
			zxfer_get_os "origin.example" source
		)
	)
	status=$?

	assertEquals "Direct remote OS lookups should fail when uname returns no output." 1 "$status"
	assertEquals "Failed direct remote OS lookups should not print a payload." "" "$output"
}

################################################################################
# Probe capture
################################################################################

test_zxfer_capture_remote_probe_output_throws_transport_setup_failures_before_staging() {
	l_probe_marker="$TEST_TMPDIR/remote_probe_capture_marker"
	rm -f "$l_probe_marker"

	set +e
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_ssh_shell_invocations=0
			g_zxfer_profile_source_ssh_shell_invocations=0
			zxfer_prepare_ssh_transport() {
				g_zxfer_ssh_transport_error="Managed ssh policy invalid."
				return 1
			}
			zxfer_get_temp_file() {
				: >"$l_probe_marker"
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				printf 'ssh=%s\n' "${g_zxfer_profile_ssh_shell_invocations:-0}"
				printf 'source=%s\n' "${g_zxfer_profile_source_ssh_shell_invocations:-0}"
				exit 7
			}
			zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source
		) 2>&1
	)
	status=$?

	assertEquals "Transport setup failures should throw." 7 "$status"
	assertContains "The throw should carry the transport diagnostic." \
		"$output" "message=Managed ssh policy invalid."
	assertContains "A transport setup failure should still count one ssh invocation." \
		"$output" "ssh=1"
	assertContains "The failed invocation should be attributed to the requested side." \
		"$output" "source=1"
	assertFalse "No staging file should exist once transport setup has failed." \
		"[ -e '$l_probe_marker' ]"
}

test_zxfer_capture_remote_probe_output_throws_host_spec_failures_before_ssh_runs() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'unexpected ssh\n'
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1"
				exit 7
			}
			zxfer_capture_remote_probe_output 'origin.example "pfexec"' "'true'" source
		) 2>&1
	)
	status=$?

	assertEquals "A host spec that needs shell quoting should throw in this shell." 7 "$status"
	assertContains "The throw should carry the host-spec diagnostic." \
		"$output" "message=Host spec (-O/-T) must use literal whitespace-delimited tokens only"
	assertNotContains "ssh must not run for a rejected host spec." "$output" "unexpected ssh"
}

test_zxfer_capture_remote_probe_output_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_create_runtime_artifact_file() {
				return 1
			}
			zxfer_throw_error() {
				printf 'message=%s\n' "$1" >&2
				exit 1
			}
			zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source
		) 2>&1
	)
	status=$?

	assertEquals "Remote probe capture should fail closed when it cannot stage stderr." \
		1 "$status"
	assertContains "Remote probe capture should keep the temp-file diagnostic." \
		"$output" "message=Error creating temporary file."
}

test_zxfer_capture_remote_probe_output_captures_stdout_stderr_and_status_in_this_shell() {
	output=$(
		(
			set +e
			g_option_V_very_verbose=1
			g_zxfer_profile_ssh_shell_invocations=0
			g_zxfer_profile_destination_ssh_shell_invocations=0
			zxfer_invoke_ssh_shell_command_for_host() {
				printf 'probe line\n\n'
				printf 'remote warning\n' >&2
				return 3
			}
			zxfer_capture_remote_probe_output "target.example" "'true'" destination 2>/dev/null
			printf 'status=%s\n' "$?"
			printf 'stdout=<%s>\n' "$g_zxfer_remote_probe_stdout"
			printf 'stderr=<%s>\n' "$g_zxfer_remote_probe_stderr"
			printf 'capture_failed=%s\n' "$g_zxfer_remote_probe_capture_failed"
			printf 'ssh=%s destination=%s\n' "$g_zxfer_profile_ssh_shell_invocations" \
				"$g_zxfer_profile_destination_ssh_shell_invocations"
		)
	)

	assertContains "The remote status should be returned." "$output" "status=3"
	assertContains "Stdout should be captured without trailing newlines." "$output" "stdout=<probe line>"
	assertContains "Stderr should be captured as written." "$output" "stderr=<remote warning
>"
	assertContains "A readable capture is not a capture failure." "$output" "capture_failed=0"
	assertContains "The ssh run inside the substitution should be counted once in this shell." \
		"$output" "ssh=1 destination=1"
}

test_zxfer_capture_remote_probe_output_reports_stderr_capture_failures() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "probe-stdout"
				printf '%s\n' "Permission denied (publickey)." >&2
				return 255
			}
			zxfer_read_runtime_artifact_file() {
				return 9
			}
			zxfer_capture_remote_probe_output "origin.example" "'sh' '-c' 'printf ok'" source
			printf 'status=%s\n' "$?"
			printf 'capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'stdout=<%s>\n' "$g_zxfer_remote_probe_stdout"
			printf 'stderr=<%s>\n' "$g_zxfer_remote_probe_stderr"
		) 2>&1
	)

	assertContains "A stderr read failure should return the read status." \
		"$output" "status=9"
	assertContains "A stderr read failure should be flagged as a capture failure." \
		"$output" "capture_failed=1"
	assertContains "A failed capture should drop stdout." \
		"$output" "stdout=<>"
	assertContains "A failed capture should explain what could not be read." \
		"$output" "stderr=<Failed to read remote probe stderr capture from local staging.>"
}

test_zxfer_capture_remote_probe_output_skips_the_read_for_an_empty_stderr() {
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "quiet"
			}
			zxfer_read_runtime_artifact_file() {
				printf 'unexpected read\n'
				return 9
			}
			zxfer_capture_remote_probe_output "origin.example" "'true'" source
			printf 'status=%s stdout=%s stderr=<%s>\n' "$?" \
				"$g_zxfer_remote_probe_stdout" "$g_zxfer_remote_probe_stderr"
		)
	)

	assertEquals "An empty stderr capture should not be read back." \
		"status=0 stdout=quiet stderr=<>" "$output"
}

test_zxfer_capture_remote_probe_output_keeps_stdin_for_the_remote_command() {
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				cat
			}
			zxfer_capture_remote_probe_output "target.example" "'cat'" destination <<EOF
payload line
EOF
			printf 'stdout=%s\n' "$g_zxfer_remote_probe_stdout"
		)
	)

	assertEquals "Probe capture should pass the caller's stdin to the remote command." \
		"stdout=payload line" "$output"
}

test_zxfer_capture_remote_probe_output_records_the_failed_ssh_argv_as_the_last_command() {
	output=$(
		(
			FAKE_SSH_EXIT_STATUS=255
			export FAKE_SSH_EXIT_STATUS
			unset ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
			g_zxfer_failure_last_command="'zfs' 'receive' 'backup/stale'"
			zxfer_capture_remote_probe_output "origin.example" "'false'" source
			printf 'status=%s\n' "$?"
			printf 'safe=<%s>\n' "$g_zxfer_failure_last_command"

			ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1
			g_zxfer_failure_last_command="'zfs' 'receive' 'backup/stale'"
			zxfer_capture_remote_probe_output "origin.example" "'false'" source
			printf 'unsafe=<%s>\n' "$g_zxfer_failure_last_command"
		)
	)

	assertContains "The failing ssh status should be returned." "$output" "status=255"
	assertContains "Safe report mode should record the probe as a redacted last command." \
		"$output" "safe=<[redacted]>"
	assertContains "Unsafe report mode should record the probe's ssh argv and host." \
		"$output" "unsafe=<'$FAKE_SSH_BIN' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'origin.example' "
	assertNotContains "A failed probe must not leave the stale command in the report." \
		"$output" "backup/stale"
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

################################################################################
# Tool resolution
################################################################################

test_resolve_remote_required_tool_maps_each_capability_record_status() {
	output=$(
		(
			set +e
			g_zxfer_secure_path="/secure/bin"
			zxfer_fetch_remote_host_capabilities_live() {
				g_zxfer_remote_capability_response_result='ZXFER_REMOTE_CAPS_V2
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	2	-
end'
				zxfer_parse_remote_capability_response "$g_zxfer_remote_capability_response_result"
			}
			zxfer_run_remote_probe_script() {
				printf 'unexpected direct probe\n'
				return 1
			}
			for l_test_tool in zfs parallel cat; do
				zxfer_resolve_remote_required_tool origin.example \
					"$l_test_tool" "$l_test_tool" source
				printf '%s_status=%s\n' "$l_test_tool" "$?"
				printf '%s=%s\n' "$l_test_tool" "$g_zxfer_required_tool_result"
			done
		)
	)

	assertContains "Status 0 should succeed." "$output" "zfs_status=0"
	assertContains "Status 0 should print the stored helper path." \
		"$output" "zfs=/remote/bin/zfs"
	assertContains "Status 1 should report the missing dependency on the secure PATH." \
		"$output" "parallel=Required dependency \"parallel\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
	assertContains "A missing dependency should fail." "$output" "parallel_status=1"
	assertContains "Any other status should report a failed query." \
		"$output" "cat=Failed to query dependency \"cat\" on host origin.example."
	assertContains "A failed query should fail." "$output" "cat_status=1"
	assertNotContains "Recorded tools must not need a direct probe." \
		"$output" "unexpected direct probe"
}

test_resolve_remote_required_tool_probes_directly_without_a_record() {
	for case_name in unavailable malformed absent; do
		output=$(
			(
				set +e
				CASE_NAME=$case_name
				g_option_V_very_verbose=1
				g_zxfer_profile_remote_cli_tool_direct_probes=0
				zxfer_test_stub_fetch_without_tool_record
				zxfer_run_remote_probe_script() {
					printf 'direct %s|%s|%s\n' "$1" "$2" "$3" >&2
					g_zxfer_remote_probe_stdout=/direct/bin/parallel
					g_zxfer_remote_probe_stderr=""
					return 0
				}
				zxfer_resolve_remote_required_tool origin.example parallel parallel source 2>&1
				printf 'status=%s resolved=%s\n' "$?" "$g_zxfer_required_tool_result"
				printf 'direct_probes=%s\n' "$g_zxfer_profile_remote_cli_tool_direct_probes"
			)
		)

		assertContains "A $case_name capability record should fall back to the direct probe." \
			"$output" "status=0 resolved=/direct/bin/parallel"
		assertContains "The direct probe should run command -v for the tool on the requested side." \
			"$output" "direct origin.example|source|l_path=\$(command -v 'parallel' 2>/dev/null);"
		assertContains "-V should count the direct probe." "$output" "direct_probes=1"
	done
}

test_resolve_remote_required_tool_maps_each_direct_probe_outcome() {
	output=$(
		(
			set +e
			g_zxfer_secure_path="/secure/bin"
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_test_stub_direct_tool_probe_invoke
			for DIRECT_CASE in missing stderr noise relative; do
				zxfer_resolve_remote_required_tool origin.example zfs zfs source
				printf '%s=%s|%s\n' "$DIRECT_CASE" "$?" "$g_zxfer_required_tool_result"
			done
		)
	)

	assertContains "Exit 10 should report the missing dependency." \
		"$output" "missing=1|Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin)."
	assertContains "Other failures should print the probe's stderr." \
		"$output" "stderr=1|Host key verification failed."
	assertContains "Stdout-only noise should not replace the generic query failure." \
		"$output" "noise=1|Failed to query dependency \"zfs\" on host origin.example."
	assertContains "A relative path should be rejected by path validation." \
		"$output" "relative=1|Required dependency \"zfs\" on host origin.example resolved to \"bin/zfs\""
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
			zxfer_resolve_remote_required_tool "origin.example" parallel parallel source
			printf 'status=%s\n' "$?"
			printf 'resolved=%s\n' "$g_zxfer_required_tool_result"
		)
	)
	probe_count=$(cat "$probe_count_file")

	assertContains "A truncated multi-tool handshake should fail closed and use the direct secure probe." \
		"$output" "status=0"
	assertContains "Direct fallback after a truncated handshake should publish only the validated helper path." \
		"$output" "resolved=/fallback/bin/parallel"
	assertEquals "Truncation fallback should perform one capability handshake and one direct helper probe." \
		2 "$probe_count"
}

test_resolve_remote_required_tool_reuses_the_host_scope_when_it_holds_the_tool() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			zxfer_test_stub_remote_probe_capture
			g_option_O_origin_host="origin.example"
			g_option_j_jobs=4
			g_option_e_restore_property_mode=1
			zxfer_ensure_remote_host_capabilities origin.example source >/dev/null || exit 1
			zxfer_resolve_remote_required_tool origin.example parallel parallel source || exit 2
			printf '%s\n' "$g_zxfer_required_tool_result"
			zxfer_resolve_remote_required_tool origin.example cat cat source || exit 3
			printf '%s\n' "$g_zxfer_required_tool_result"
		)
	)
	status=$?

	assertEquals "Helpers inside the host scope should resolve." 0 "$status"
	assertEquals "Both helpers should come from the preloaded capabilities." \
		"/opt/bin/parallel
/remote/bin/cat" "$output"
	assertEquals "Helpers inside the host scope should not probe again." \
		1 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
	assertContains "The one probe should cover the whole host scope." \
		"$(cat "$ZXFER_TEST_PROBE_LOG")" "'\\''zfs'\\'' '\\''parallel'\\'' '\\''cat'\\''"
}

test_resolve_remote_required_tool_probes_a_narrow_scope_for_a_tool_outside_the_host_scope() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			zxfer_test_stub_remote_probe_capture
			g_option_O_origin_host="origin.example"
			g_option_e_restore_property_mode=1
			zxfer_resolve_remote_required_tool origin.example parallel parallel source
			printf '%s\n' "$g_zxfer_required_tool_result"
		)
	)

	assertEquals "A tool outside the host scope should still resolve from its own probe." \
		"/opt/bin/parallel" "$output"
	assertContains "That probe should ask for zfs and the tool only." \
		"$(cat "$ZXFER_TEST_PROBE_LOG")" "for l_tool in '\\''zfs'\\'' '\\''parallel'\\''; do"
	assertNotContains "That probe should not ask for the rest of the host scope." \
		"$(cat "$ZXFER_TEST_PROBE_LOG")" "'\\''cat'\\''"
}

test_zxfer_ensure_remote_host_capabilities_fills_memory_from_one_live_probe() {
	probe_count_file="$TEST_TMPDIR/ensure-live-probe-count"
	printf '0\n' >"$probe_count_file"
	g_option_O_origin_host="origin.example"
	zxfer_fetch_remote_host_capabilities_live() {
		l_count=$(($(cat "$probe_count_file") + 1))
		printf '%s\n' "$l_count" >"$probe_count_file"
		g_zxfer_remote_capability_response_result=$(fake_remote_capability_response)
		zxfer_parse_remote_capability_response "$g_zxfer_remote_capability_response_result"
	}

	first=$(zxfer_ensure_remote_host_capabilities "origin.example" source)
	first_status=$?
	# Plain (non-command-substitution) call so the in-memory store persists in
	# this shell, mirroring the preload flow.
	zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	second=$(zxfer_ensure_remote_host_capabilities "origin.example" source)
	second_status=$?
	probe_count=$(cat "$probe_count_file")

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
}

test_zxfer_ensure_remote_host_capabilities_preserves_live_probe_diagnostic() {
	set +e
	output=$(
		(
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
			zxfer_fetch_remote_host_capabilities_live() {
				return 37
			}
			zxfer_ensure_remote_host_capabilities "origin.example" source
			printf 'status=%s\n' "$?"
			printf 'stored=<%s|%s>\n' "${g_origin_remote_capabilities_host:-}" \
				"${g_origin_remote_capabilities_response:-}"
		)
	)

	assertContains "Remote capability ensure should propagate the live probe failure status." \
		"$output" "status=37"
	assertContains "A failed probe must never populate the in-memory capability state." \
		"$output" "stored=<|>"
}

test_zxfer_reset_remote_host_state_resets_capability_and_resolved_tool_state() {
	result=$(
		(
			g_cmd_zfs="/stub/zfs"
			g_origin_remote_capabilities_host="origin.example"
			g_origin_remote_capabilities_response="dirty-origin"
			g_origin_remote_capabilities_os="DirtyOriginOS"
			g_target_remote_capabilities_tools="zfs cat"
			g_target_remote_capabilities_response="dirty-target"
			g_target_remote_capabilities_tool_records="dirty-target-tools"
			g_zxfer_remote_probe_capture_failed=1
			g_origin_cmd_zfs="/dirty/origin-zfs"

			zxfer_reset_remote_host_state
			printf 'origin=<%s|%s|%s>\n' "$g_origin_remote_capabilities_host" \
				"$g_origin_remote_capabilities_response" "$g_origin_remote_capabilities_os"
			printf 'target=<%s|%s|%s>\n' "$g_target_remote_capabilities_tools" \
				"$g_target_remote_capabilities_response" "$g_target_remote_capabilities_tool_records"
			printf 'capture_failed=%s\n' "$g_zxfer_remote_probe_capture_failed"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		)
	)

	assertContains "Remote-host reset should empty the origin capability slot." \
		"$result" "origin=<||>"
	assertContains "Remote-host reset should empty the target capability slot." \
		"$result" "target=<||>"
	assertContains "Remote-host reset should clear remote capture failure state." \
		"$result" "capture_failed=0"
	assertContains "Remote-host reset should restore origin zfs to the local default." \
		"$result" "origin_zfs=/stub/zfs"
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
