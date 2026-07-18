#!/bin/sh
# SSH control-socket directory, lifecycle, capability, and memoization behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_zxfer_ensure_ssh_control_socket_dir_prefers_run_tmp_root_and_memoizes() {
	output=$(
		(
			set +e
			short_root="$TEST_PRIVATE_DEFAULT_TMPDIR/zxfer.$$.run-root"
			mkdir -p "$short_root" || exit 90
			chmod 700 "$short_root" || exit 90
			g_zxfer_run_tmp_root=$short_root
			g_zxfer_owned_run_tmp_root=$short_root
			g_zxfer_owned_run_tmp_root_parent=$TEST_PRIVATE_DEFAULT_TMPDIR
			g_zxfer_owned_run_tmp_root_identity=$(zxfer_get_path_device_inode \
				"$short_root") || exit 90
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

test_zxfer_ensure_ssh_control_socket_dir_rejects_same_owner_run_root_replacement() {
	output=$(
		(
			set +e
			zxfer_discard_runtime_cleanup_state
			replacement_parent="$TEST_TMPDIR/run-root-memo-replacement"
			mkdir -p "$replacement_parent" || exit 90
			chmod 700 "$replacement_parent" || exit 91
			replacement_root="$replacement_parent/zxfer.$$.memo"
			mkdir "$replacement_root" || exit 92
			chmod 700 "$replacement_root" || exit 93
			g_zxfer_run_tmp_root=$replacement_root
			g_zxfer_owned_run_tmp_root=$replacement_root
			g_zxfer_owned_run_tmp_root_parent=$replacement_parent
			g_zxfer_owned_run_tmp_root_identity=$(zxfer_get_path_device_inode \
				"$replacement_root") || exit 94
			g_zxfer_ssh_control_socket_dir_result=$replacement_root

			mv "$replacement_root" "$replacement_root.original" || exit 95
			mkdir "$replacement_root" || exit 96
			chmod 700 "$replacement_root" || exit 97
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'status=%s\n' "$?"
			printf 'memo=<%s>\n' "$g_zxfer_ssh_control_socket_dir_result"
			[ -d "$replacement_root" ] && printf 'replacement=present\n'
			[ -d "$replacement_root.original" ] && printf 'original=present\n'
		)
	)

	assertContains "A same-owner replacement of the memoized run root should fail closed." \
		"$output" "status=1"
	assertContains "Rejected run-root replacements should clear the socket-directory memo." \
		"$output" "memo=<>"
	assertContains "Socket memo revalidation must not delete the replacement directory." \
		"$output" "replacement=present"
	assertContains "Socket memo revalidation must not delete the originally allocated directory." \
		"$output" "original=present"
}

test_zxfer_ensure_ssh_control_socket_dir_rejects_same_owner_adjacent_replacement() {
	output=$(
		(
			set +e
			zxfer_discard_runtime_cleanup_state
			replacement_dir="$TEST_TMPDIR/.zxfer-ssh-memo-replacement.$$"
			mkdir "$replacement_dir" || exit 90
			chmod 700 "$replacement_dir" || exit 91
			zxfer_register_runtime_artifact_path "$replacement_dir" || exit 92
			g_zxfer_ssh_control_socket_dir_result=$replacement_dir

			mv "$replacement_dir" "$replacement_dir.original" || exit 93
			mkdir "$replacement_dir" || exit 94
			chmod 700 "$replacement_dir" || exit 95
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'status=%s\n' "$?"
			printf 'memo=<%s>\n' "$g_zxfer_ssh_control_socket_dir_result"
			[ -d "$replacement_dir" ] && printf 'replacement=present\n'
			[ -d "$replacement_dir.original" ] && printf 'original=present\n'
		)
	)

	assertContains "A same-owner replacement of a registered adjacent socket directory should fail closed." \
		"$output" "status=1"
	assertContains "Rejected adjacent replacements should clear the socket-directory memo." \
		"$output" "memo=<>"
	assertContains "Adjacent memo revalidation must not delete the replacement directory." \
		"$output" "replacement=present"
	assertContains "Adjacent memo revalidation must not delete the originally registered directory." \
		"$output" "original=present"
}

test_zxfer_ensure_ssh_control_socket_dir_rejects_insecure_adjacent_memo() {
	output=$(
		(
			set +e
			zxfer_discard_runtime_cleanup_state
			insecure_dir="$TEST_TMPDIR/.zxfer-ssh-memo-insecure.$$"
			mkdir "$insecure_dir" || exit 90
			chmod 700 "$insecure_dir" || exit 91
			zxfer_register_runtime_artifact_path "$insecure_dir" || exit 92

			chmod 755 "$insecure_dir" || exit 93
			g_zxfer_ssh_control_socket_dir_result=$insecure_dir
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'mode_status=%s\n' "$?"
			printf 'mode_memo=<%s>\n' "$g_zxfer_ssh_control_socket_dir_result"

			chmod 700 "$insecure_dir" || exit 94
			actual_uid=$(zxfer_get_effective_user_uid) || exit 95
			if [ "$actual_uid" = "999999" ]; then
				mismatched_uid=999998
			else
				mismatched_uid=999999
			fi
			zxfer_get_path_owner_uid() {
				printf '%s\n' "$mismatched_uid"
			}
			g_zxfer_ssh_control_socket_dir_result=$insecure_dir
			zxfer_ensure_ssh_control_socket_dir >/dev/null
			printf 'owner_status=%s\n' "$?"
			printf 'owner_memo=<%s>\n' "$g_zxfer_ssh_control_socket_dir_result"
			[ -d "$insecure_dir" ] && printf 'directory=present\n'
		)
	)

	assertContains "A non-private adjacent socket-directory memo should fail closed." \
		"$output" "mode_status=1"
	assertContains "A rejected mode should clear the socket-directory memo." \
		"$output" "mode_memo=<>"
	assertContains "An owner mismatch on an adjacent socket-directory memo should fail closed." \
		"$output" "owner_status=1"
	assertContains "A rejected owner should clear the socket-directory memo." \
		"$output" "owner_memo=<>"
	assertContains "Private-directory validation failures must not remove the registered path." \
		"$output" "directory=present"
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
			zxfer_discard_runtime_cleanup_state
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
