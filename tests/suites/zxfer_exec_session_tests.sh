#!/bin/sh
# Tests for src/zxfer_session.sh and launcher startup paths, run by
# tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_init_variables_resolves_remote_tool_paths_and_restore_cat() {
	result=$(
		zxfer_get_os() {
			if [ "$1" = "" ]; then
				g_zxfer_os_result="LocalOS"
				printf '%s\n' "LocalOS"
			else
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			fi
		}
		zxfer_resolve_remote_required_tool() {
			if [ "$1:$2" = "origin.example pfexec:zfs" ]; then
				g_zxfer_required_tool_result="/remote/origin/zfs"
			elif [ "$1:$2" = "target.example doas:zfs" ]; then
				g_zxfer_required_tool_result="/remote/target/zfs"
			elif [ "$1:$2" = "origin.example pfexec:cat" ]; then
				g_zxfer_required_tool_result="/remote/origin/cat"
			else
				return 1
			fi
		}
		g_option_z_compress=0
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_zfs="/sbin/zfs"
		g_option_O_origin_host="origin.example pfexec"
		g_option_T_target_host="target.example doas"
		g_option_e_restore_property_mode=1
		zxfer_init_variables
		printf 'source_os=%s\n' "$g_source_operating_system"
		printf 'dest_os=%s\n' "$g_destination_operating_system"
		printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
		printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
		printf 'cat=%s\n' "$g_cmd_cat"
	)

	assertContains "Origin OS should be populated from remote zxfer_get_os()." "$result" "source_os=RemoteOS"
	assertContains "Destination OS should be populated from remote zxfer_get_os()." "$result" "dest_os=RemoteOS"
	assertContains "Origin zfs path should use the remote lookup result." "$result" "origin_zfs=/remote/origin/zfs"
	assertContains "Target zfs path should use the remote lookup result." "$result" "target_zfs=/remote/target/zfs"
	assertContains "Restore mode should resolve cat on the origin host." "$result" "cat=/remote/origin/cat"
}

test_init_variables_passes_explicit_profile_sides_when_origin_and_target_match() {
	log_file="$TEST_TMPDIR/init_variables_profile_sides.log"
	: >"$log_file"

	(
		zxfer_get_os() {
			printf 'os:%s:%s\n' "$1" "${2:-}" >>"$log_file"
			g_zxfer_os_result="RemoteOS"
			printf '%s\n' "RemoteOS"
		}
		zxfer_resolve_remote_required_tool() {
			printf 'tool:%s:%s:%s:%s\n' "$1" "$2" "$3" "${4:-}" >>"$log_file"
			case "$2" in
			zfs)
				g_zxfer_required_tool_result="/remote/$2"
				;;
			cat)
				g_zxfer_required_tool_result="/remote/$2"
				;;
			esac
		}
		g_option_z_compress=0
		g_cmd_ssh="/usr/bin/ssh"
		g_cmd_zfs="/sbin/zfs"
		g_option_O_origin_host="shared.example"
		g_option_T_target_host="shared.example"
		g_option_e_restore_property_mode=1
		zxfer_init_variables
	)

	result=$(cat "$log_file")
	assertContains "Origin OS probes should be tagged as source-side even when origin and target share the same host spec." \
		"$result" "os:shared.example:source"
	assertContains "Target OS probes should be tagged as destination-side even when origin and target share the same host spec." \
		"$result" "os:shared.example:destination"
	assertContains "Origin zfs dependency probes should be tagged as source-side." \
		"$result" "tool:shared.example:zfs:zfs:source"
	assertContains "Target zfs dependency probes should be tagged as destination-side." \
		"$result" "tool:shared.example:zfs:zfs:destination"
	assertContains "Origin restore-metadata cat probes should be tagged as source-side." \
		"$result" "tool:shared.example:cat:cat:source"
}

test_init_variables_marks_remote_zfs_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_cmd_zfs="/sbin/zfs"
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote zfs lookup failures should abort zxfer_init_variables." 1 "$status"
	assertContains "Remote zfs lookup failures should be classified as dependency errors." "$output" "class=dependency"
	assertContains "Remote zfs lookup failures should surface the lookup message." "$output" "msg=lookup failed"
}

test_zxfer_help_bypasses_dependency_init() {
	secure_path_dir="$TEST_TMPDIR/help_secure_path"
	hostile_path_dir="$TEST_TMPDIR/help_hostile_path"
	marker_file="$TEST_TMPDIR/help_hostile_path.marker"
	stdout_file="$TEST_TMPDIR/help.stdout"
	stderr_file="$TEST_TMPDIR/help.stderr"
	real_awk=$(command -v awk 2>/dev/null || :)
	real_sed=$(command -v sed 2>/dev/null || :)
	mkdir -p "$secure_path_dir"
	mkdir -p "$hostile_path_dir"

	if [ -z "$real_awk" ] || [ -z "$real_sed" ]; then
		fail "Host test requires awk and sed on the local system PATH."
	fi

	cat >"$hostile_path_dir/awk" <<EOF
#!/bin/sh
printf '%s\n' "awk" >>"$marker_file"
exec "$real_awk" "\$@"
EOF
	cat >"$hostile_path_dir/sed" <<EOF
#!/bin/sh
printf '%s\n' "sed" >>"$marker_file"
exec "$real_sed" "\$@"
EOF
	chmod +x "$hostile_path_dir/awk" "$hostile_path_dir/sed"

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="$hostile_path_dir:/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -h >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Help output should succeed even when the secure PATH lacks required tools." 0 "$status"
	assertContains "$(cat "$stdout_file")" "usage:"
	assertContains "Help output should advertise the standalone -c service list option." \
		"$(cat "$stdout_file")" "[-c FMRI|pattern[ FMRI|pattern]...]"
	assertContains "Help output should advertise the migration flag separately from -c." \
		"$(cat "$stdout_file")" "[-m]"
	assertContains "Help output should advertise the unsupported-property skip flag." \
		"$(cat "$stdout_file")" "[-U]"
	assertEquals "Help prescan should bypass dependency initialization errors." "" "$(cat "$stderr_file")"
	if [ -f "$marker_file" ]; then
		marker_contents=$(cat "$marker_file")
	else
		marker_contents=""
	fi
	assertEquals "Early invocation capture should not execute PATH-injected awk/sed helpers." "" "$marker_contents"
}

test_zxfer_usage_error_with_very_verbose_does_not_emit_profile_summary() {
	secure_path_dir="$TEST_TMPDIR/usage_secure_path"
	stdout_file="$TEST_TMPDIR/usage.stdout"
	stderr_file="$TEST_TMPDIR/usage.stderr"

	create_launcher_usage_secure_path "$secure_path_dir" || return

	set +e
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$secure_path_dir" \
		"$ZXFER_ROOT/zxfer" -V >"$stdout_file" 2>"$stderr_file"
	status=$?

	assertEquals "Very-verbose usage errors should still exit with usage status." 2 "$status"
	assertEquals "Usage errors should not write to stdout." "" "$(cat "$stdout_file")"
	assertContains "$(cat "$stderr_file")" "Error: Need a destination."
	assertNotContains "Usage-mode very-verbose exits should not emit profiling counters." \
		"$(cat "$stderr_file")" "zxfer profile:"
}

test_trap_exit_emits_failure_report_once() {
	set +e
	output=$(
		(
			g_zxfer_failure_class="runtime"
			g_zxfer_failure_stage="unit"
			g_zxfer_failure_message="trap failure"
			g_services_need_relaunch=0
			false
			zxfer_trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	count=$(printf '%s\n' "$output" | grep -c "^zxfer: failure report begin$")
	assertEquals "zxfer_trap_exit helper path should preserve the failing exit status." 1 "$status"
	assertEquals "zxfer_trap_exit should emit the failure report only once even when EXIT re-triggers cleanup." 1 "$count"
}

test_trap_exit_emits_profile_summary_once_in_very_verbose_mode() {
	set +e
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_summary_emitted=0
			g_zxfer_profile_startup_latency_ms=99
			g_zxfer_profile_cleanup_ms=55
			g_zxfer_profile_ssh_setup_ms=111
			g_zxfer_profile_source_snapshot_listing_ms=222
			g_zxfer_profile_destination_snapshot_listing_ms=333
			g_zxfer_profile_snapshot_diff_sort_ms=444
			g_zxfer_profile_source_zfs_calls=3
			g_zxfer_profile_destination_zfs_calls=4
			g_zxfer_profile_ssh_shell_invocations=2
			g_zxfer_profile_source_snapshot_list_commands=1
			g_zxfer_profile_send_receive_pipeline_commands=2
			g_zxfer_profile_exists_destination_calls=5
			g_zxfer_profile_normalized_property_reads_source=6
			g_zxfer_profile_normalized_property_reads_destination=7
			g_zxfer_profile_required_property_backfill_gets=1
			g_zxfer_profile_parent_destination_property_reads=2
			g_zxfer_profile_bucket_source_inspection=8
			g_zxfer_profile_bucket_destination_inspection=9
			g_zxfer_profile_bucket_property_reconciliation=10
			g_zxfer_profile_bucket_send_receive_setup=11
			g_services_need_relaunch=0
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when only emitting profiling output." 0 "$status"
	assertContains "Very-verbose exits should emit the source zfs profile counter." \
		"$output" "zxfer profile: source_zfs_calls=3"
	assertContains "Very-verbose exits should emit startup latency timing." \
		"$output" "zxfer profile: startup_latency_ms=99"
	assertContains "Very-verbose exits should emit cleanup timing." \
		"$output" "zxfer profile: cleanup_ms="
	assertContains "Very-verbose exits should emit the accumulated ssh setup stage timing." \
		"$output" "zxfer profile: ssh_setup_ms=111"
	assertContains "Very-verbose exits should emit the accumulated snapshot diff/sort stage timing." \
		"$output" "zxfer profile: snapshot_diff_sort_ms=444"
	assertContains "Very-verbose exits should emit the property-read profile counter." \
		"$output" "zxfer profile: normalized_property_reads_destination=7"
	assertContains "Very-verbose exits should emit the send/receive bucket counter." \
		"$output" "zxfer profile: bucket_send_receive_setup=11"
	count=$(printf '%s\n' "$output" | grep -c "^zxfer profile: source_zfs_calls=3$")
	assertEquals "zxfer_trap_exit should emit the profile summary only once." 1 "$count"
}

test_trap_exit_preserves_failure_status_when_error_log_warning_fails() {
	set +e
	output=$(
		(
			ZXFER_ERROR_LOG="relative.log"
			g_zxfer_failure_class="runtime"
			g_zxfer_failure_stage="unit"
			g_zxfer_failure_message="trap failure"
			g_services_need_relaunch=0
			false
			zxfer_trap_exit
		) 2>&1 >/dev/null
	)
	status=$?

	assertEquals "Failure-report log warnings must not replace the original exit status." 1 "$status"
	assertContains "zxfer_trap_exit should still emit the report before warning about the log sink." "$output" "zxfer: failure report begin"
	assertContains "zxfer_trap_exit should warn when ZXFER_ERROR_LOG is invalid." \
		"$output" "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute"
}
