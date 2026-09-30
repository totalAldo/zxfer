#!/bin/sh
# Remote capability tests for src/zxfer_remote_hosts.sh: the V2 answer
# parser, the probe script, the tool scope, the per-role cache, and the
# fail-closed probe and resolution branches the black-box suites cannot
# reach. Probes over the mock ssh, direct-probe fallbacks and dependency
# reports are pinned black-box in tests/test_contract_remote.sh.
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
		*) printf '%s\n' "bin/zstd" ;;
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

# Set the options a host's capability scope depends on, then print the
# requested tools for each HOST:TOOL operand (TOOL may be empty), one per line.
# Usage: zxfer_test_print_capability_scopes O_HOST T_HOST R_SOURCE JOBS
# RESTORE BACKUP COMPRESS COMPRESS_CMD DECOMPRESS_CMD HOST:TOOL...
zxfer_test_print_capability_scopes() {
	g_option_O_origin_host=$1
	g_option_T_target_host=$2
	g_option_R_recursive=$3
	g_option_j_jobs=$4
	g_option_e_restore_property_mode=$5
	g_option_k_backup_property_mode=$6
	g_option_z_compress=$7
	g_cmd_compress=$8
	g_cmd_decompress=$9
	shift 9
	for l_scope_operand in "$@"; do
		zxfer_get_remote_capability_requested_tools_for_host \
			"${l_scope_operand%%:*}" "${l_scope_operand#*:}"
		printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
	done
}

################################################################################
# Parser
################################################################################

test_parse_remote_capability_response_accepts_well_formed_answers() {
	tab=$(printf '\t')
	# One answer holds every accepted record shape: a found tool whose path
	# OmniOS sh quotes, a missing one (status 1), another status, and a tool
	# zxfer did not ask about. It is parsed with IFS unset and globbing off,
	# which the parser must restore.
	answer="ZXFER_REMOTE_CAPS_V2
os${tab}SunOS
tool${tab}zfs${tab}0${tab}'/usr/sbin/zfs'
tool${tab}parallel${tab}1${tab}-
tool${tab}cat${tab}127${tab}-
tool${tab}weirdtool${tab}0${tab}/remote/bin/weirdtool
end
"
	(
		unset IFS
		set -f
		zxfer_parse_remote_capability_response "$answer" "zfs parallel cat" || exit 8
		[ "${IFS+set}" != set ] || exit 9
		[ "${-#*f}" != "$-" ] || exit 10
		printf 'os=%s zfs_status=%s\n' "$g_zxfer_remote_capability_os" \
			"$g_zxfer_remote_capability_zfs_status"
		for l_test_tool in zfs parallel cat weirdtool absent ""; do
			zxfer_get_parsed_remote_capability_tool_record "$l_test_tool"
			printf '%s=%s:%s:%s\n' "$l_test_tool" "$?" \
				"$g_zxfer_remote_capability_tool_status_result" \
				"$g_zxfer_remote_capability_tool_path_result"
		done
		# A stored record without its path field is never returned.
		g_zxfer_remote_capability_tool_records=$(printf 'zfs\t0')
		zxfer_get_parsed_remote_capability_tool_record zfs
		printf 'partial=%s\n' "$?"
	) >"$TEST_TMPDIR/accepted_answer.out"
	status=$?

	assertEquals "Parsing must restore an unset IFS and disabled globbing." 0 "$status"
	assertEquals "Each record should keep its status, and a validated, unquoted path only for status 0." \
		"os=SunOS zfs_status=0
zfs=0:0:/usr/sbin/zfs
parallel=0:1:
cat=0:127:
weirdtool=0:0:/remote/bin/weirdtool
absent=1::
=1::
partial=1" "$(cat "$TEST_TMPDIR/accepted_answer.out")"
}

test_parse_remote_capability_response_rejects_every_malformed_answer() {
	tab=$(printf '\t')
	cr=$(printf '\r')
	header="ZXFER_REMOTE_CAPS_V2
os${tab}RemoteOS"
	zfs_record="tool${tab}zfs${tab}0${tab}/remote/bin/zfs"
	failures=""
	# Each case: name|the whole answer. A malformed line anywhere fails it all.
	for case_spec in \
		"retired V1 header|ZXFER_REMOTE_CAPS_V1
os${tab}RemoteOS
$zfs_record
end" \
		"os line without a value|ZXFER_REMOTE_CAPS_V2
os
$zfs_record
end" \
		"no end line|$header
$zfs_record" \
		"a line after end|$header
$zfs_record
end
extra${tab}line" \
		"non-numeric status|$header
tool${tab}zfs${tab}oops${tab}/remote/bin/zfs
end" \
		"five fields|$header
$zfs_record${tab}extra
end" \
		"three fields|$header
tool${tab}zfs${tab}0
end" \
		"empty tool name|$header
tool${tab}${tab}0${tab}/remote/bin/zfs
end" \
		"carriage return in a tool name|$header
tool${tab}zfs${cr}${tab}0${tab}/remote/bin/zfs
end" \
		"carriage return in a path|$header
tool${tab}zfs${tab}0${tab}/remote/bin/zfs${cr}
end" \
		"empty status|$header
tool${tab}zfs${tab}${tab}/remote/bin/zfs
end" \
		"found tool without a path|$header
tool${tab}zfs${tab}0${tab}-
end" \
		"missing tool with a path|$header
$zfs_record
tool${tab}cat${tab}1${tab}/remote/bin/cat
end" \
		"relative helper path|$header
tool${tab}zfs${tab}0${tab}bin/zfs
end" \
		"unknown record kind|$header
$zfs_record
helper${tab}cat${tab}0${tab}/remote/bin/cat
end" \
		"no zfs record|$header
tool${tab}cat${tab}0${tab}/remote/bin/cat
end" \
		"blank record line|$header
$zfs_record

end" \
		"duplicate tool|$header
$zfs_record
tool${tab}zfs${tab}0${tab}/remote/bin/zfs-second
end"; do
		if zxfer_parse_remote_capability_response "${case_spec#*|}"; then
			failures="$failures [${case_spec%%|*}]"
		fi
	done
	# An answer without a record for a requested tool is truncated.
	if zxfer_parse_remote_capability_response "$header
$zfs_record
end" "zfs parallel"; then
		failures="$failures [missing requested tool]"
	fi

	assertEquals "Every malformed answer should fail as a whole." "" "$failures"
}

################################################################################
# Probe script and tool scope
################################################################################

test_remote_capability_probe_script_matches_the_golden_and_refuses_a_multiline_secure_path() {
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

	g_zxfer_secure_path=$(printf "/opt/trusted/bin\n/opt/translated/bin")
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

test_remote_capability_requested_tools_follow_each_host_role() {
	output=$(
		# -O asks parallel (-j), cat (-e) and the compression head; -T asks
		# cat (-k) and the decompression head; any other host only zfs.
		zxfer_test_print_capability_scopes origin.example target.example "" 4 1 1 1 \
			"zstd -T0 -9" "xz -d" origin.example: target.example: other.example:
		# A host that is both -O and -T gets the union, each helper once.
		zxfer_test_print_capability_scopes shared.example shared.example "" 4 0 1 1 \
			"zstd -3" "zstd -d" shared.example:
		# The fast no-op proof defers parallel until an on-demand lookup.
		zxfer_test_print_capability_scopes origin.example "" tank/src 4 0 0 0 "" "" \
			origin.example: origin.example:parallel
		# A tool inside the scope keeps it (so the cache is shared); one
		# outside narrows the probe to zfs and that tool.
		zxfer_test_print_capability_scopes origin.example "" "" 1 1 0 0 "" "" \
			origin.example:cat origin.example:zfs origin.example:xz
		# Heads split as the resolver splits them; a command it rejects adds
		# nothing.
		zxfer_test_print_capability_scopes origin.example target.example "" 1 0 0 1 \
			"zstd|x -3" "'zstd' -d" origin.example: target.example:
	)
	(
		unset IFS
		set +f
		zxfer_test_print_capability_scopes origin.example "" "" 1 0 0 1 "zst* -3" "" \
			origin.example: >"$TEST_TMPDIR/literal_scope.out"
		[ "${IFS+set}" != set ] || exit 9
		[ "${-#*f}" = "$-" ] || exit 10
	)
	literal_status=$?

	assertEquals "Each host should ask for exactly the helpers its roles need." \
		"zfs parallel cat zstd
zfs cat xz
zfs
zfs parallel zstd cat
zfs
zfs parallel
zfs cat
zfs cat
zfs xz
zfs zstd|
zfs" "$output"
	assertEquals "A head must be taken literally, and IFS and globbing restored." \
		"0|zfs zst*" "$literal_status|$(cat "$TEST_TMPDIR/literal_scope.out")"
}

################################################################################
# Per-role cache
################################################################################

test_remote_host_capabilities_are_cached_per_host_and_tool_scope() {
	zxfer_test_reset_remote_probe_counters
	output=$(
		(
			set +e
			zxfer_test_stub_remote_probe_capture
			g_option_V_very_verbose=1
			g_zxfer_profile_remote_capability_bootstrap_live=0
			g_zxfer_profile_remote_capability_bootstrap_memory=0
			g_zxfer_profile_remote_cli_tool_direct_probes=0
			g_option_O_origin_host=origin.example
			g_option_T_target_host=target.example
			g_option_e_restore_property_mode=1
			# A miss probes and prints the answer; the same host and scope
			# answer from memory with the same output, and a helper inside the
			# scope resolves from its record without a direct probe.
			zxfer_ensure_remote_host_capabilities origin.example source >"$TEST_TMPDIR/miss.out" || exit 11
			zxfer_ensure_remote_host_capabilities origin.example source >"$TEST_TMPDIR/hit.out" || exit 12
			zxfer_resolve_remote_required_tool origin.example cat cat source || exit 13
			printf 'resolved_cat=%s direct_probes=%s\n' "$g_zxfer_required_tool_result" \
				"$g_zxfer_profile_remote_cli_tool_direct_probes"
			# A tool outside the scope probes zfs and that tool only.
			zxfer_ensure_remote_host_capabilities origin.example source parallel >/dev/null || exit 14
			# Without a side the -T host fills the target slot, which then
			# answers destination lookups; any other host uses the origin's.
			zxfer_ensure_remote_host_capabilities target.example >/dev/null || exit 15
			zxfer_ensure_remote_host_capabilities target.example destination >/dev/null || exit 16
			zxfer_ensure_remote_host_capabilities unlisted.example >/dev/null || exit 17
			# Bad lookups and roles fail without probing.
			zxfer_ensure_remote_host_capabilities origin.example other >/dev/null && exit 18
			zxfer_ensure_remote_host_capabilities "" source >/dev/null && exit 19
			# A failed lookup leaves nothing of the last host in the channel.
			printf 'channel=<%s|%s|%s|%s>\n' "$g_zxfer_remote_capability_os" \
				"$g_zxfer_remote_capability_zfs_status" \
				"$g_zxfer_remote_capability_tool_records" \
				"$g_zxfer_remote_capability_response_result"
			zxfer_publish_endpoint_runtime_context invalid Linux /sbin/zfs
			printf 'publish_invalid=%s\n' "$?"
			# A failed probe keeps its status and fills no slot.
			zxfer_fetch_remote_host_capabilities_live() {
				return 37
			}
			zxfer_ensure_remote_host_capabilities failing.example destination >/dev/null
			printf 'failed_probe=%s target_slot=%s\n' "$?" "$g_target_remote_capabilities_host"
			printf 'live=%s memory=%s\n' "$g_zxfer_profile_remote_capability_bootstrap_live" \
				"$g_zxfer_profile_remote_capability_bootstrap_memory"
			printf 'origin_slot=%s|%s target_slot=%s|%s\n' \
				"$g_origin_remote_capabilities_host" "$g_origin_remote_capabilities_tools" \
				"$g_target_remote_capabilities_host" "$g_target_remote_capabilities_tools"
		) 2>/dev/null
	)
	status=$?

	assertEquals "Every lookup should behave as expected." 0 "$status"
	assertEquals "Four scopes should cost four probes and three memory hits." \
		"resolved_cat=/remote/bin/cat direct_probes=0
channel=<|||>
publish_invalid=2
failed_probe=37 target_slot=target.example
live=4 memory=3
origin_slot=unlisted.example|zfs target_slot=target.example|zfs" "$output"
	assertEquals "The probe count should match." 4 "$(cat "$ZXFER_TEST_PROBE_COUNT_FILE")"
	assertEquals "A miss and a hit should both print the accepted answer." \
		"$(fake_remote_capability_response)
$(fake_remote_capability_response)" "$(cat "$TEST_TMPDIR/miss.out" "$TEST_TMPDIR/hit.out")"
	assertContains "The first probe should ask for the whole scope." \
		"$(sed -n '1p' "$ZXFER_TEST_PROBE_LOG")" "for l_tool in '\\''zfs'\\'' '\\''cat'\\''; do"
	assertContains "A tool outside the scope should be probed with zfs and that tool only." \
		"$(sed -n '2p' "$ZXFER_TEST_PROBE_LOG")" "for l_tool in '\\''zfs'\\'' '\\''parallel'\\''; do"
}

################################################################################
# Live probe, remote OS and probe capture
################################################################################

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

test_get_os_fails_closed_when_the_direct_probe_fails_or_prints_nothing() {
	failed_output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "Permission denied (publickey)." >&2
				return 255
			}
			zxfer_get_os origin.example source
		)
	)
	failed_status=$?
	empty_output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				return 0
			}
			zxfer_get_os origin.example source
		)
	)
	empty_status=$?

	assertEquals "A failed direct probe should fail and print the probe's stderr." \
		"1|Permission denied (publickey)." "$failed_status|$failed_output"
	assertEquals "A direct probe that prints nothing should fail silently." \
		"1|" "$empty_status|$empty_output"
}

test_get_os_treats_local_ssh_path_as_literal() {
	marker="$TEST_TMPDIR/get_os_ssh_marker"
	old_cmd_ssh=${g_cmd_ssh:-}
	g_cmd_ssh="/bin/echo; touch $marker #"

	if zxfer_get_os "backup@example.com" >/dev/null 2>&1; then
		status=0
	else
		status=$?
	fi
	g_cmd_ssh=$old_cmd_ssh

	: "$status"
	assertFalse "Local ssh helper paths should not execute shell metacharacters during OS detection." \
		"[ -e '$marker' ]"
}

test_zxfer_capture_remote_probe_output_throws_before_staging_or_running_ssh() {
	# ssh runs in a command substitution with stderr staged, so a bad policy
	# or host spec must throw here first, where the report reaches the
	# operator.
	for l_capture_case in policy host_spec; do
		l_capture_spec=origin.example
		[ "$l_capture_case" = policy ] || l_capture_spec='origin.example "pfexec"'
		output=$(
			(
				[ "$l_capture_case" != policy ] ||
					ZXFER_SSH_USER_KNOWN_HOSTS_FILE=relative/known_hosts
				zxfer_get_temp_file() {
					printf 'staged\n'
				}
				zxfer_invoke_ssh_shell_command_for_host() {
					printf 'ssh ran\n'
				}
				zxfer_throw_error() {
					printf 'throw: %s\n' "$1"
					exit 7
				}
				zxfer_capture_remote_probe_output "$l_capture_spec" "'true'" source
			) 2>&1
		)
		status=$?
		l_capture_message="Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
		[ "$l_capture_case" != policy ] ||
			l_capture_message="ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."

		assertEquals "[$l_capture_case] should throw before staging stderr or running ssh." \
			"7|throw: $l_capture_message" "$status|$output"
	done
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
			# The caller's label names the tool in its messages.
			for l_test_case in zfs:zfs "parallel:GNU parallel" cat:cat; do
				l_test_tool=${l_test_case%%:*}
				zxfer_resolve_remote_required_tool origin.example \
					"$l_test_tool" "${l_test_case#*:}" source
				printf '%s_status=%s\n' "$l_test_tool" "$?"
				printf '%s=%s\n' "$l_test_tool" "$g_zxfer_required_tool_result"
			done
		)
	)

	assertContains "Status 0 should succeed." "$output" "zfs_status=0"
	assertContains "Status 0 should print the stored helper path." \
		"$output" "zfs=/remote/bin/zfs"
	assertContains "Status 1 should report the missing dependency on the secure PATH." \
		"$output" "parallel=Required dependency \"GNU parallel\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
	assertContains "A missing dependency should fail." "$output" "parallel_status=1"
	assertContains "Any other status should report a failed query." \
		"$output" "cat=Failed to query dependency \"cat\" on host origin.example."
	assertContains "A failed query should fail." "$output" "cat_status=1"
	assertNotContains "Recorded tools must not need a direct probe." \
		"$output" "unexpected direct probe"
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
				zxfer_resolve_remote_required_tool origin.example zstd \
					"compression command" source
				printf '%s=%s|%s\n' "$DIRECT_CASE" "$?" "$g_zxfer_required_tool_result"
			done
		)
	)

	assertContains "Exit 10 should report the missing dependency." \
		"$output" "missing=1|Required dependency \"compression command\" not found on host origin.example in secure PATH (/secure/bin)."
	assertContains "Other failures should print the probe's stderr." \
		"$output" "stderr=1|Host key verification failed."
	assertContains "Stdout-only noise should not replace the generic query failure." \
		"$output" "noise=1|Failed to query dependency \"compression command\" on host origin.example."
	assertContains "A relative path should be rejected by path validation." \
		"$output" "relative=1|Required dependency \"compression command\" on host origin.example resolved to \"bin/zstd\""
}
