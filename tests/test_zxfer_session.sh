#!/bin/sh
#
# shunit2 tests for the session composition root in src/zxfer_session.sh:
# startup, zxfer_session_run's exit status, remote connection preparation,
# endpoint initialization and the EXIT trap. Its black-box behavior (signal
# exit statuses, one failure report, the run root and socket directory
# removed, dry runs contacting no host, migration service restarts) is pinned
# by the contract suites; these cases pin what those cannot reach.
#
# Remote cases initialize the remote-host fixture and lifecycle cases
# initialize the runtime fixture explicitly; startup/order cases use neither.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")
TEST_ORIGINAL_PATH=$PATH

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/remote_host_fixtures.sh
. "$TESTS_DIR/helpers/remote_host_fixtures.sh"
# shellcheck source=tests/helpers/runtime_fixtures.sh
. "$TESTS_DIR/helpers/runtime_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_session"
	zxfer_test_remote_host_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_remote_host_fixture_one_time_teardown
	# A failed case may leave a directory read-only.
	chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || :
	zxfer_test_cleanup_tmpdir
}

tearDown() {
	zxfer_test_remote_host_fixture_teardown
}

# Startup must settle the secure PATH before it creates anything, and create
# the run root before it narrows PATH, so mktemp still resolves when
# ZXFER_SECURE_PATH omits its directory.
test_session_environment_creates_the_run_root_between_secure_path_and_path_narrowing() {
	secure_dir="$TEST_TMPDIR/secure-path-without-mktemp"
	run_parent="$TEST_TMPDIR/session-run-parent"
	mkdir -p "$secure_dir" "$run_parent"
	chmod 700 "$run_parent"
	ln -s "$(command -v awk)" "$secure_dir/awk"
	printf '#!/bin/sh\nexit 0\n' >"$secure_dir/zfs"
	printf '#!/bin/sh\nexit 0\n' >"$secure_dir/ps"
	chmod 755 "$secure_dir/zfs" "$secure_dir/ps"

	(
		unset ZXFER_SECURE_PATH_APPEND ZXFER_BACKUP_DIR ZXFER_ERROR_LOG
		ZXFER_SECURE_PATH=$(printf '/bin\t/untrusted')
		TMPDIR=$run_parent
		zxfer_reset_session_state
		zxfer_init_session_environment
	) >/dev/null 2>&1
	invalid_status=$?
	invalid_entries=$(ls -A "$run_parent")
	narrowed_path=$(
		(
			unset ZXFER_SECURE_PATH_APPEND ZXFER_BACKUP_DIR ZXFER_ERROR_LOG
			ZXFER_SECURE_PATH=$secure_dir
			TMPDIR=$run_parent
			zxfer_reset_session_state
			zxfer_init_session_environment
			printf '%s\n' "$PATH"
		) 2>/dev/null
	)
	valid_entries=$(ls -A "$run_parent")

	assertEquals "An invalid secure PATH should stop startup before the run root is created." \
		"status=1 entries=" "status=$invalid_status entries=$invalid_entries"
	assertContains "The run root should be created before PATH is narrowed to a secure PATH without mktemp." \
		"$valid_entries" "zxfer."
	assertEquals "Startup should narrow PATH to the secure PATH after creating the run root." \
		"$secure_dir" "$narrowed_path"
}

test_zxfer_session_initialize_discards_inherited_cleanup_handles_before_early_failure_trap() {
	external_root="$TEST_TMPDIR/session-inherited-root"
	external_socket_dir="$TEST_TMPDIR/zxfer.ssh.inherited"
	mkdir -p "$external_root" "$external_socket_dir"
	printf '%s\n' sentinel >"$external_root/sentinel"
	printf '%s\n' sentinel >"$external_socket_dir/ssh-origin.sock"

	set +e
	output=$(
		(
			g_zxfer_send_jobs="inherited-job	424242	backup/inherited	$external_root/status	tank/inherited@snap	pgid"
			g_zxfer_cleanup_pid_records="424243	inherited helper"
			g_option_O_origin_host="operator@origin"
			g_option_T_target_host="operator@target"
			g_ssh_origin_control_socket="$external_root/origin.sock"
			g_ssh_target_control_socket="$external_root/target.sock"
			g_zxfer_services_to_restart="svc:/operator/service:default"
			g_zxfer_run_tmp_root=$external_root
			g_zxfer_owned_run_tmp_root=$external_root
			g_zxfer_owned_run_tmp_root_parent=$TEST_TMPDIR
			g_zxfer_ssh_control_socket_short_dir=$external_socket_dir
			g_zxfer_failure_report_emitted=1
			g_option_V_very_verbose=1

			zxfer_init_session_environment() { exit 73; }
			zxfer_abort_all_send_jobs() {
				[ -z "${g_zxfer_send_jobs:-}" ] || printf '%s\n' background-action
				return 0
			}
			zxfer_kill_registered_cleanup_pids() {
				[ -z "${g_zxfer_cleanup_pid_records:-}" ] || printf '%s\n' cleanup-pid-action
				return 0
			}
			zxfer_close_all_ssh_control_sockets() {
				if [ -n "${g_ssh_origin_control_socket:-}${g_ssh_target_control_socket:-}" ]; then
					printf '%s\n' ssh-action
				fi
				return 0
			}
			zxfer_remove_run_tmp_root() {
				[ -z "${g_zxfer_run_tmp_root:-}" ] || printf '%s\n' run-root-action
				return 0
			}
			zxfer_relaunch() { printf '%s\n' migration-action; }
			g_option_V_very_verbose=0
			zxfer_echoV() { :; }
			zxfer_profile_emit_summary() { :; }
			zxfer_emit_failure_report() {
				[ "${g_zxfer_failure_report_emitted:-0}" -eq 0 ] || printf '%s\n' report-suppressed
			}

			zxfer_session_initialize
		) 2>&1
	)
	status=$?

	assertEquals "An initialization failure after trap registration should preserve its original status." \
		73 "$status"
	assertEquals "Early-failure cleanup must not act on any inherited internal cleanup handle." \
		"" "$output"
	assertTrue "Early-failure cleanup must leave an inherited external run-root sentinel untouched." \
		"[ -f '$external_root/sentinel' ]"
	assertTrue "Early-failure cleanup must not remove anything from an inherited socket-directory handle." \
		"[ -f '$external_socket_dir/ssh-origin.sock' ]"
}

test_zxfer_session_run_does_not_promote_optional_beep_failure() {
	(
		OPTIND=1
		g_option_j_jobs=1
		g_option_k_backup_property_mode=0
		zxfer_set_failure_stage() { :; }
		zxfer_read_command_line_switches() { OPTIND=1; }
		zxfer_set_failure_roots() { :; }
		zxfer_consistency_check() { :; }
		zxfer_prepare_remote_host_connections() { :; }
		zxfer_init_variables() { :; }
		zxfer_run_zfs_mode_loop() { :; }
		zxfer_beep() { return 37; }

		zxfer_session_run backup/destination
	)
	l_status=$?

	assertEquals "A best-effort beep failure must not change a successful replication exit status." \
		0 "$l_status"
}

test_zxfer_session_run_promotes_final_backup_write_failure() {
	log="$TEST_TMPDIR/session_backup_write_failure.log"
	: >"$log"

	set +e
	(
		SESSION_LOG="$log"
		OPTIND=1
		g_option_j_jobs=1
		g_option_k_backup_property_mode=1
		zxfer_set_failure_stage() { :; }
		zxfer_read_command_line_switches() { OPTIND=1; }
		zxfer_set_failure_roots() { :; }
		zxfer_consistency_check() { :; }
		zxfer_prepare_remote_host_connections() { :; }
		zxfer_init_variables() { :; }
		zxfer_run_zfs_mode_loop() { :; }
		zxfer_write_backup_properties() { return 41; }
		zxfer_throw_error() {
			printf 'throw=%s status=%s\n' "$1" "$2" >>"$SESSION_LOG"
			return "$2"
		}
		zxfer_beep() { printf 'unexpected-success-beep\n' >>"$SESSION_LOG"; }

		zxfer_session_run backup/destination
	)
	status=$?

	assertEquals "The session boundary should preserve final backup-write failures." \
		41 "$status"
	assertEquals "Final backup-write failures should be reported and must not continue to the success beep." \
		"throw=Failed to write backup metadata. status=41" "$(cat "$log")"
}

# Local -j source discovery runs through parallel, so a missing local parallel
# must stop the run before any ssh master or background helper starts. With
# -O the origin's parallel is checked by discovery instead.
test_zxfer_session_run_requires_local_parallel_for_jobs_before_any_helper_starts() {
	log="$TEST_TMPDIR/session_parallel_preflight.log"
	: >"$log"

	for origin in "" "operator@origin"; do
		(
			SESSION_LOG="$log"
			OPTIND=1
			g_option_j_jobs=2
			g_option_O_origin_host=$origin
			g_option_k_backup_property_mode=0
			g_cmd_parallel=""
			zxfer_set_failure_stage() { :; }
			zxfer_read_command_line_switches() { OPTIND=1; }
			zxfer_set_failure_roots() { :; }
			zxfer_consistency_check() { :; }
			zxfer_prepare_remote_host_connections() {
				printf 'connect origin=%s\n' "$g_option_O_origin_host" >>"$SESSION_LOG"
			}
			zxfer_init_variables() { :; }
			zxfer_run_zfs_mode_loop() { :; }
			zxfer_beep() { :; }
			zxfer_throw_error() {
				printf 'throw=%s class=%s\n' "$1" "$g_zxfer_failure_class" >>"$SESSION_LOG"
				exit 1
			}

			zxfer_session_run backup/destination
		)
	done

	assertEquals "Only a local -j run without parallel fails, as a dependency error, before connecting." \
		"throw=The -j option requires parallel but it was not found in PATH on the local host. class=dependency
connect origin=operator@origin" "$(cat "$log")"
}

test_zxfer_note_trap_cleanup_failure_promotes_only_a_clean_exit_and_keeps_the_first_message() {
	output=$(
		(
			zxfer_reset_failure_context "unit"
			g_zxfer_trap_exit_status=0
			zxfer_note_trap_cleanup_failure 0 "ignored"
			printf 'zero=%s <%s>\n' "$g_zxfer_trap_exit_status" "$g_zxfer_failure_message"
			zxfer_note_trap_cleanup_failure 17 "first cleanup failure"
			printf 'first=%s <%s|%s|%s>\n' "$g_zxfer_trap_exit_status" \
				"$g_zxfer_failure_class" "$g_zxfer_failure_stage" "$g_zxfer_failure_message"
			zxfer_note_trap_cleanup_failure 23 "second cleanup failure"
			printf 'second=%s <%s>\n' "$g_zxfer_trap_exit_status" "$g_zxfer_failure_message"
		)
	)

	assertEquals "A zero status changes nothing; the first failure sets the status and message; a later one keeps both." \
		"zero=0 <>
first=17 <runtime|trap cleanup|first cleanup failure>
second=17 <first cleanup failure>" "$output"
}

test_endpoint_initialization_rejects_invalid_roles_before_lookup_or_publication() {
	zxfer_test_remote_host_fixture_setup || return
	result=$(
		zxfer_get_os() { printf 'unexpected OS lookup\n'; }
		zxfer_resolve_remote_required_tool() { printf 'unexpected tool lookup\n'; }
		g_option_O_origin_host=origin.example
		g_option_T_target_host=target.example
		g_origin_cmd_zfs=old-origin
		g_target_cmd_zfs=old-target
		g_destination_operating_system=OldOS
		zxfer_init_endpoint_execution_context invalid
		printf '%s|%s|%s|%s\n' "$?" "$g_origin_cmd_zfs" "$g_target_cmd_zfs" "$g_destination_operating_system"
	)

	assertEquals "An invalid role should return 2 without looking up or publishing an endpoint." \
		"2|old-origin|old-target|OldOS" "$result"
}

# An endpoint is published only after all of its lookups succeed. Check both
# roles and each failing stage, including the original nonzero lookup status.
test_endpoint_initialization_preserves_context_and_status_when_a_lookup_fails() {
	zxfer_test_remote_host_fixture_setup || return
	for l_init_test_role in origin target; do
		while IFS='|' read -r l_init_test_failure l_init_test_status; do
			result=$(
				g_option_O_origin_host=origin.example
				g_option_T_target_host=target.example
				g_option_z_compress=1
				g_origin_cmd_zfs=old-origin
				g_target_cmd_zfs=old-target
				g_destination_operating_system=OldOS
				g_origin_cmd_compress_safe=old-compressor
				g_target_cmd_decompress_safe=old-decompressor
				zxfer_get_os() {
					[ "$l_init_test_failure" != os ] || return "$l_init_test_status"
					g_zxfer_os_result=NewOS
				}
				zxfer_resolve_remote_required_tool() {
					g_zxfer_required_tool_result=/remote/zfs
					[ "$l_init_test_failure" != zfs ] || return "$l_init_test_status"
				}
				zxfer_resolve_cli_command_safe() {
					g_zxfer_resolved_cli_command_result="'/remote/zstd'"
					return "$l_init_test_status"
				}
				zxfer_throw_error() {
					printf '%s|%s|%s|%s|%s|%s\n' "$g_zxfer_failure_class" \
						"$g_origin_cmd_zfs" "$g_target_cmd_zfs" "$g_destination_operating_system" \
						"$g_origin_cmd_compress_safe" "$g_target_cmd_decompress_safe"
					exit "${2:-1}"
				}
				zxfer_init_endpoint_execution_context "$l_init_test_role"
			)
			status=$?

			assertEquals "[$l_init_test_role/$l_init_test_failure] must leave endpoint state intact and preserve the lookup status." \
				"$l_init_test_status|dependency|old-origin|old-target|OldOS|old-compressor|old-decompressor" \
				"$status|$result"
		done <<EOF
os|31
zfs|37
codec|45
EOF
	done
}

test_prepare_remote_host_connections_fails_with_dependency_class_without_ssh() {
	zxfer_test_remote_host_fixture_setup || return
	set +e
	output=$(
		(
			zxfer_ensure_local_ssh_command() {
				g_zxfer_required_tool_result="ssh dependency missing"
				return 1
			}
			zxfer_open_ssh_control_sockets() {
				printf '%s\n' "open"
			}
			zxfer_throw_error() {
				printf 'class=%s throw=%s\n' "$g_zxfer_failure_class" "$1"
				exit 9
			}
			g_option_T_target_host="target.example"
			zxfer_prepare_remote_host_connections
		)
	)
	status=$?

	assertEquals "A missing local ssh should stop remote preparation." 9 "$status"
	assertEquals "A missing local ssh should be a dependency failure with the lookup diagnostic, before any master opens." \
		"class=dependency throw=ssh dependency missing" "$output"
}

test_prepare_remote_host_connections_opens_masters_before_preloading_capabilities() {
	zxfer_test_remote_host_fixture_setup || return
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	: >"$log"

	result=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_open_ssh_control_sockets() {
				printf 'open %s %s\n' "$g_option_O_origin_host" "$g_option_T_target_host" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			# One reading per clock read, in this shell.
			clock_readings="1000 1250"
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=${clock_readings%% *}
				clock_readings=${clock_readings#* }
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertEquals "Both masters should open before either capability probe runs over them." \
		"open origin.example pfexec target.example doas
preload origin.example pfexec source
preload target.example doas destination" "$(cat "$log")"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"

	l_support_out=$(
		(
			g_option_O_origin_host="origin.example"
			g_option_T_target_host=""
			zxfer_ensure_local_ssh_command() { g_cmd_ssh=/usr/bin/ssh; }
			zxfer_open_ssh_control_sockets() { :; }
			zxfer_preload_remote_host_capabilities() { :; }
			zxfer_ssh_supports_control_sockets() { return 0; }
			zxfer_prepare_remote_host_connections
			l_yes=$g_ssh_supports_control_sockets
			zxfer_ssh_supports_control_sockets() { return 1; }
			zxfer_prepare_remote_host_connections
			printf 'yes=%s no=%s\n' "$l_yes" "$g_ssh_supports_control_sockets"
		)
	)
	assertEquals "Remote preparation should record control-socket support directly from zxfer_ssh_supports_control_sockets." \
		"yes=1 no=0" "$l_support_out"
}

test_init_variables_uses_gawk_on_sunos_when_available() {
	zxfer_test_remote_host_fixture_setup || return
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
				g_zxfer_os_result="SunOS"
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_secure_path="$gawk_dir"
			zxfer_init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	zxfer_test_remote_host_fixture_setup || return
	cat_dir="$TEST_TMPDIR/restore_cat_path"
	mkdir -p "$cat_dir"
	printf '#!/bin/sh\nexit 0\n' >"$cat_dir/cat"
	chmod 755 "$cat_dir/cat"

	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result="FreeBSD"
				printf '%s\n' "FreeBSD"
			}
			g_zxfer_secure_path=$cat_dir
			g_option_e_restore_property_mode=1
			zxfer_init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat on the secure PATH." \
		"$result" "cat=$cat_dir/cat"
}

# zxfer_init_variables resolves each endpoint on its own host: the origin's
# operating system, zfs, compressor and restore cat, and the target's
# operating system, zfs and decompressor. Wrapper-style host specs reach each
# lookup whole.
test_init_variables_resolves_each_endpoint_on_its_own_host() {
	zxfer_test_remote_host_fixture_setup || return
	result=$(
		(
			zxfer_get_os() {
				g_zxfer_os_result=LocalOS
				[ -z "$1" ] || g_zxfer_os_result="OS of $1"
				printf '%s\n' "$g_zxfer_os_result"
			}
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="$2 on $1"
			}
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result="$2 on $1"
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_z_compress=1
			g_option_e_restore_property_mode=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			zxfer_init_variables
			printf '%s\n' "local=$g_zxfer_local_os" \
				"source=$g_origin_cmd_zfs|$g_origin_cmd_compress_safe" \
				"destination=$g_destination_operating_system|$g_target_cmd_zfs|$g_target_cmd_decompress_safe" \
				"cat=$g_cmd_cat"
		)
	)

	assertEquals "Each endpoint should be resolved on its own host." \
		"local=LocalOS
source=zfs on origin.example pfexec|zstd -T0 -9 on origin.example pfexec
destination=OS of target.example doas|zfs on target.example doas|zstd -d on target.example doas
cat=cat on origin.example pfexec" "$result"
}

test_init_variables_passes_explicit_profile_sides_when_origin_and_target_match() {
	zxfer_test_remote_host_fixture_setup || return
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

# Each row makes one lookup of zxfer_init_variables fail, with -O and -T on
# different hosts and -z and -e so every lookup runs: the run must stop as a
# dependency failure with that lookup's message.
test_init_variables_stops_on_each_failed_lookup_as_a_dependency_error() {
	zxfer_test_remote_host_fixture_setup || return
	while IFS='|' read -r l_failed_lookup l_expected; do
		l_output=$(
			(
				FAILED_LOOKUP=$l_failed_lookup
				zxfer_get_os() {
					[ "os:${1:-local}" != "$FAILED_LOOKUP" ] || return 1
					g_zxfer_os_result=RemoteOS
					printf '%s\n' RemoteOS
				}
				zxfer_resolve_remote_required_tool() {
					g_zxfer_required_tool_result="/remote/$2"
					[ "tool:$1:$2" != "$FAILED_LOOKUP" ] && return 0
					g_zxfer_required_tool_result="$2 lookup failed on $1"
					return 1
				}
				zxfer_resolve_cli_command_safe() {
					g_zxfer_resolved_cli_command_result="'/remote/zstd'"
					[ "codec:$1" != "$FAILED_LOOKUP" ] && return 0
					g_zxfer_resolved_cli_command_result="codec lookup failed on $1"
					return 1
				}
				zxfer_throw_error() {
					printf 'class=%s message=%s\n' "$g_zxfer_failure_class" "$1"
					exit 1
				}
				g_option_O_origin_host=origin.example
				g_option_T_target_host=target.example
				g_option_z_compress=1
				g_option_e_restore_property_mode=1
				g_cmd_compress="zstd -3"
				g_cmd_decompress="zstd -d"
				zxfer_init_variables
			)
		)
		l_status=$?

		assertEquals "[$l_failed_lookup] must stop as a dependency failure" \
			"status=1 class=dependency message=$l_expected" "status=$l_status $l_output"
	done <<EOF
os:local|Failed to determine the local operating system.
os:origin.example|Failed to determine operating system on host origin.example.
os:target.example|Failed to determine operating system on host target.example.
tool:origin.example:zfs|zfs lookup failed on origin.example
tool:target.example:zfs|zfs lookup failed on target.example
codec:origin.example|codec lookup failed on origin.example
codec:target.example|codec lookup failed on target.example
tool:origin.example:cat|cat lookup failed on origin.example
EOF
}

test_zxfer_reset_session_state_clears_each_owners_inherited_state() {
	zxfer_test_runtime_fixture_setup
	output=$(
		(
			# 0 skips the start-clock read of a -V run.
			g_zxfer_profile_prescan=0
			g_cmd_parallel=stale
			g_option_j_jobs=9
			g_zxfer_failure_message=stale
			g_zxfer_effective_uid=stale
			g_zxfer_run_tmp_root=stale
			g_zxfer_send_jobs=stale
			g_zxfer_background_shell_spawn_mode=stale
			g_ssh_origin_control_socket=stale
			g_zxfer_services_to_restart=stale
			g_zxfer_profile_source_zfs_calls=9
			g_zxfer_new_snapshot_name=stale
			g_zxfer_new_snapshot_taken=1
			g_zxfer_progress_bar_command_result=stale
			g_destination_existence_cache=stale
			g_zxfer_live_destination_listing_file=stale
			g_origin_parallel_cmd=stale
			g_recursive_source_list=stale
			g_last_common_snap=stale
			g_zxfer_snapshot_plan_file=stale
			g_backup_file_contents=stale
			g_zxfer_unsupported_filesystem_properties=stale
			g_zxfer_source_property_table=stale
			g_zxfer_property_error_result=stale
			g_zxfer_remote_capability_slot_result=stale
			g_zxfer_local_os=stale

			zxfer_reset_session_state

			printf '%s\n' \
				"dependencies: parallel=<$g_cmd_parallel>" \
				"cli: jobs=$g_option_j_jobs" \
				"failure: message=<$g_zxfer_failure_message> stage=$g_zxfer_failure_stage" \
				"path security: uid=<$g_zxfer_effective_uid>" \
				"runtime: run root=<$g_zxfer_run_tmp_root>" \
				"send jobs: <$g_zxfer_send_jobs>" \
				"exec: spawn mode=<$g_zxfer_background_shell_spawn_mode>" \
				"ssh transport: origin socket=<$g_ssh_origin_control_socket>" \
				"migration: restart=<$g_zxfer_services_to_restart>" \
				"profile: source zfs calls=$g_zxfer_profile_source_zfs_calls" \
				"replication: snapshot name=<$g_zxfer_new_snapshot_name> taken=<$g_zxfer_new_snapshot_taken>" \
				"send/receive: progress=<$g_zxfer_progress_bar_command_result>" \
				"destination existence: <$g_destination_existence_cache>" \
				"destination listing: <$g_zxfer_live_destination_listing_file>" \
				"snapshot producer: origin parallel=<$g_origin_parallel_cmd>" \
				"snapshot discovery: sources=<$g_recursive_source_list>" \
				"snapshot plan: last common=<$g_last_common_snap>" \
				"snapshot delete: plan file=<$g_zxfer_snapshot_plan_file>" \
				"backup metadata: rows=<$g_backup_file_contents>" \
				"property transfer: unsupported=<$g_zxfer_unsupported_filesystem_properties>" \
				"property caches: source table=<$g_zxfer_source_property_table>" \
				"property reads: error=<$g_zxfer_property_error_result>" \
				"remote hosts: capabilities=<$g_zxfer_remote_capability_slot_result>" \
				"session: local os=<$g_zxfer_local_os>"
		)
	)

	assertEquals "The session reset should leave each owner's state at its start value." \
		"dependencies: parallel=<>
cli: jobs=1
failure: message=<> stage=startup
path security: uid=<>
runtime: run root=<>
send jobs: <>
exec: spawn mode=<>
ssh transport: origin socket=<>
migration: restart=<>
profile: source zfs calls=0
replication: snapshot name=<> taken=<0>
send/receive: progress=<>
destination existence: <>
destination listing: <>
snapshot producer: origin parallel=<>
snapshot discovery: sources=<>
snapshot plan: last common=<>
snapshot delete: plan file=<>
backup metadata: rows=<>
property transfer: unsupported=<>
property caches: source table=<>
property reads: error=<>
remote hosts: capabilities=<>
session: local os=<>" "$output"
}

test_zxfer_trap_exit_restores_shell_modes_before_mirroring_the_report() {
	zxfer_test_runtime_fixture_setup
	# A signal can land while zxfer_create_runtime_artifact_file holds umask
	# 077 and noclobber, or between zxfer_split_begin and zxfer_split_end.
	# The report must still reach ZXFER_ERROR_LOG, appended in place under a
	# read-only parent (unless running as root).
	log_dir="$TEST_TMPDIR/trap-modes-log"
	log_path="$log_dir/failure.log"
	modes_file="$TEST_TMPDIR/trap-modes.out"
	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	chmod 500 "$log_dir"
	rm -f "$modes_file"

	status=0
	(
		set +e
		ZXFER_ERROR_LOG=$log_path
		zxfer_profile_emit_summary() {
			case $- in *C*) l_noclobber=on ;; *) l_noclobber=off ;; esac
			case $- in *f*) l_noglob=on ;; *) l_noglob=off ;; esac
			case $- in *m*) l_monitor=on ;; *) l_monitor=off ;; esac
			printf 'noclobber=%s noglob=%s monitor=%s umask=%s ifs=%s\n' "$l_noclobber" \
				"$l_noglob" "$l_monitor" "$(umask)" "$(printf '%s' "$IFS" | od -An -tx1 | tr -d ' \n')" \
				>"$modes_file"
		}
		g_zxfer_failure_class="runtime"
		g_zxfer_failure_stage="unit"
		g_zxfer_failure_message="interrupted mid-allocation"
		g_zxfer_services_to_restart=""
		g_zxfer_run_umask=0027
		umask 077
		set -C -f
		set -m 2>/dev/null || :
		IFS=:
		false
		zxfer_trap_exit
	) >/dev/null 2>&1 || status=$?
	chmod 700 "$log_dir"

	assertEquals "zxfer_trap_exit should keep the failing status." 1 "$status"
	assertEquals "zxfer_trap_exit should restore noclobber, noglob, monitor mode, the run umask and default IFS before reporting." \
		"noclobber=off noglob=off monitor=off umask=0027 ifs=20090a" "$(cat "$modes_file")"
	assertContains "The failure report should reach ZXFER_ERROR_LOG after an interrupted allocation." \
		"$(cat "$log_path")" "interrupted mid-allocation"
	assertContains "Mirroring should keep the earlier log contents." \
		"$(cat "$log_path")" "existing: keep-me"
}

# Each row fails one cleanup step of zxfer_trap_exit after a run that ended
# with the given status: step|status before the trap|status and report. A
# failed step turns a clean exit into its status (1 for a failed removal)
# and its message; after an earlier failure it changes neither, and a failed
# ssh close does not even fill that failure's empty message. Each run still
# reports, and removes the run root again after the report.
test_zxfer_trap_exit_reports_each_failed_cleanup_step() {
	zxfer_test_runtime_fixture_setup
	sweep_log="$TEST_TMPDIR/trap-cleanup-steps.log"
	while IFS='|' read -r l_failed_step l_status_before l_expected; do
		: >"$sweep_log"
		l_output=$(
			(
				trap - EXIT INT TERM HUP QUIT
				zxfer_reset_failure_context "unit"
				STEP_LOG=$sweep_log
				FAILED_STEP=$l_failed_step
				g_option_V_very_verbose=0
				zxfer_abort_all_send_jobs() {
					[ "$FAILED_STEP" = send_jobs ] || return 0
					g_zxfer_send_job_abort_failure_message="validated abort failed"
					return 17
				}
				zxfer_kill_registered_cleanup_pids() {
					[ "$FAILED_STEP" = cleanup_pids ] || return 0
					g_zxfer_cleanup_pid_abort_failure_message="validated cleanup helper abort failed"
					return 23
				}
				zxfer_close_all_ssh_control_sockets() {
					[ "$FAILED_STEP" = ssh_close ] || return 0
					return 19
				}
				zxfer_remove_ssh_control_socket_dir() { return 0; }
				zxfer_remove_run_tmp_root() {
					printf '%s\n' sweep >>"$STEP_LOG"
					[ "$FAILED_STEP" != run_root ]
				}
				zxfer_restore_migration_services_on_exit() {
					[ "$FAILED_STEP" = migration ] || return 0
					g_zxfer_migration_service_restore_failure_message="Couldn't re-enable service svc:/broken:default."
					return 37
				}
				zxfer_echoV() { :; }
				zxfer_profile_emit_summary() { :; }
				zxfer_emit_failure_report() {
					printf '%s\n' report >>"$STEP_LOG"
					printf '%s|%s|%s|%s\n' "$1" "$g_zxfer_failure_class" \
						"$g_zxfer_failure_stage" "$g_zxfer_failure_message"
				}
				(exit "$l_status_before")
				zxfer_trap_exit
			)
		)
		l_status=$?

		assertEquals "[$l_failed_step after status $l_status_before] exit status and report" \
			"$l_expected" "$l_status: $l_output"
		assertEquals "[$l_failed_step] the run root is removed before and after the report" \
			"sweep
report
sweep" "$(cat "$sweep_log")"
	done <<EOF
send_jobs|0|17: 17|runtime|trap cleanup|validated abort failed
cleanup_pids|0|23: 23|runtime|trap cleanup|validated cleanup helper abort failed
run_root|0|1: 1|runtime|trap cleanup|Failed to remove one or more runtime temp artifacts during exit.
migration|0|37: 37|runtime|trap cleanup|Couldn't re-enable service svc:/broken:default.
ssh_close|5|5: 5||unit|
EOF
}

# Purpose: Rewrite `trap` listing lines as "SIGNAL action", dropping the SIG
# prefix and quoting that differ between shells.
# Usage: trap | zxfer_test_normalize_trap_listing
zxfer_test_normalize_trap_listing() {
	awk -v quote="'" '{
		signal = $NF
		sub(/^SIG/, "", signal)
		action = $0
		sub(/^trap -- /, "", action)
		sub(/ [^ ]*$/, "", action)
		gsub(quote, "", action)
		print signal " " action
	}'
}

# EXIT runs the plain handler; each signal passes its 128+signo status. A
# signal ignored on entry (INT and QUIT in an async suite) cannot be trapped,
# so the expectation covers only the signals a probe trap shows this shell
# accepts.
test_zxfer_session_initialize_maps_each_signal_to_its_exit_status() {
	zxfer_test_runtime_fixture_setup
	trappable=$(
		(
			trap 'zxfer_trap_probe' HUP INT QUIT TERM
			trap
		) | zxfer_test_normalize_trap_listing |
			awk '$2 == "zxfer_trap_probe" { print $1 }'
	)
	expected="EXIT zxfer_trap_exit"
	for signal in $trappable; do
		case $signal in
		HUP) expected="$expected
HUP zxfer_trap_exit 129" ;;
		INT) expected="$expected
INT zxfer_trap_exit 130" ;;
		QUIT) expected="$expected
QUIT zxfer_trap_exit 131" ;;
		TERM) expected="$expected
TERM zxfer_trap_exit 143" ;;
		esac
	done
	output=$(
		(
			zxfer_init_session_environment() {
				:
			}
			zxfer_session_initialize
			trap
			trap - EXIT HUP INT QUIT TERM
		) | zxfer_test_normalize_trap_listing | grep ' zxfer_trap_exit' | sort
	)

	assertContains "TERM must be trappable in the test shell." "$trappable" "TERM"
	assertEquals "Session startup should route EXIT and each signal through zxfer_trap_exit with its exit status." \
		"$(printf '%s\n' "$expected" | sort)" "$output"
}

test_zxfer_trap_exit_passes_stop_timer_clock_to_profile_summary() {
	zxfer_test_runtime_fixture_setup
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_V_very_verbose=1
			g_zxfer_profile_has_data=1
			g_zxfer_profile_start_ms=10000
			g_zxfer_profile_summary_emitted=0
			clock_readings="11000 14000"
			zxfer_profile_read_clock_ms() {
				g_zxfer_profile_clock_ms=${clock_readings%% *}
				clock_readings=${clock_readings#* }
			}
			zxfer_abort_all_send_jobs() { return 0; }
			zxfer_kill_registered_cleanup_pids() { return 0; }
			zxfer_close_all_ssh_control_sockets() { return 0; }
			zxfer_remove_ssh_control_socket_dir() { return 0; }
			zxfer_remove_run_tmp_root() { return 0; }
			zxfer_restore_migration_services_on_exit() { return 0; }
			zxfer_emit_failure_report() { :; }
			zxfer_profile_emit_summary() {
				printf 'end_ms=<%s>\n' "${1:-}"
			}
			true
			zxfer_trap_exit
		) 2>/dev/null
	)

	assertEquals "zxfer_trap_exit should pass the cleanup stop-timer reading to zxfer_profile_emit_summary." \
		"end_ms=<14000>" "$output"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
