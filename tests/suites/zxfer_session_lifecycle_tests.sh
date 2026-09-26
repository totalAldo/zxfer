#!/bin/sh
# Lifecycle tests for src/zxfer_session.sh: global and execution-context
# initialization, signal traps, and zxfer_trap_exit cleanup of runtime
# artifacts, jobs and sockets. Run by tests/test_zxfer_session.sh under the
# runtime fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329,SC2016

test_runtime_global_init_covers_default_assignments_in_current_shell() {
	output=$(
		(
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_refresh_secure_path_state() {
				:
			}
			zxfer_init_dependency_tool_defaults() {
				g_cmd_zfs="/sbin/zfs"
				g_cmd_compress_safe="gzip"
				g_cmd_decompress_safe="gunzip"
			}
			zxfer_apply_secure_path() {
				:
			}
			zxfer_ensure_run_tmp_root() {
				:
			}

			zxfer_reset_session_state
			zxfer_init_session_environment

			printf 'version=%s\n' "$g_zxfer_version"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'origin_caps=<%s>\n' "$g_origin_remote_capabilities_response"
			printf 'control_sockets=%s\n' "$g_ssh_supports_control_sockets"
			printf 'backup_root=%s\n' "$g_backup_storage_root"
			printf 'backup_ext=%s\n' "$g_backup_file_extension"
			printf 'temp_file=<%s>\n' "$g_zxfer_temp_file_result"
		)
	)

	assertContains "Runtime metadata initialization should set the current zxfer version string." \
		"$output" "version=2.0.0-20260623"
	assertContains "Option default initialization should restore the single-job default." \
		"$output" "jobs=1"
	assertContains "Transport runtime defaults should clear cached remote capability payloads." \
		"$output" "origin_caps=<>"
	assertContains "Transport runtime defaults should publish the ssh control-socket support marker in current-shell state." \
		"$output" "control_sockets="
	assertContains "Runtime state defaults should restore the default backup metadata root." \
		"$output" "backup_root=/var/db/zxfer"
	assertContains "Runtime state defaults should restore the secure backup-file suffix." \
		"$output" "backup_ext=.zxfer_backup_info"
	assertContains "Temporary artifact initialization should not allocate a temp file until a caller needs one." \
		"$output" "temp_file=<>"
}

test_runtime_execution_context_init_helpers_cover_local_and_dry_run_remote_paths() {
	cat_dir="$TEST_TMPDIR/endpoint_context_cat"
	mkdir -p "$cat_dir"
	printf '#!/bin/sh\nexit 0\n' >"$cat_dir/cat"
	chmod 755 "$cat_dir/cat"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$1"
			}
			zxfer_get_os() {
				if [ -n "$1" ]; then
					g_zxfer_os_result="RemoteOS"
					printf '%s\n' "RemoteOS"
				else
					g_zxfer_os_result="LocalOS"
					printf '%s\n' "LocalOS"
				fi
			}
			zxfer_quote_cli_tokens() {
				g_zxfer_shell_command_result="quoted<$1>"
			}

			g_zxfer_secure_path=$cat_dir
			g_cmd_zfs="/sbin/zfs"
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="local-compress"
			g_cmd_decompress_safe="local-decompress"
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			g_origin_cmd_zfs=""
			g_target_cmd_zfs=""
			g_cmd_cat=""

			(
				zxfer_refresh_remote_zfs_commands() {
					:
				}
				zxfer_init_restore_property_helpers() {
					:
				}
				zxfer_init_local_awk_compatibility() {
					:
				}
				zxfer_init_variables
				printf 'local_os=%s\n' "$g_zxfer_local_os"
				printf 'local_source_os=%s local_origin_zfs=%s\n' \
					"$g_source_operating_system" "$g_origin_cmd_zfs"
				printf 'local_dest_os=%s local_target_zfs=%s\n' \
					"$g_destination_operating_system" "$g_target_cmd_zfs"
				printf 'transfer_origin=%s\n' "$g_origin_cmd_compress_safe"
				printf 'transfer_target=%s\n' "$g_target_cmd_decompress_safe"
			)

			g_option_e_restore_property_mode=1
			g_option_O_origin_host=""
			zxfer_init_restore_property_helpers
			printf 'local_cat=%s\n' "$g_cmd_cat"

			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			g_option_n_dryrun=1
			g_option_z_compress=1
			g_cmd_cat=""
			g_origin_cmd_compress_safe=""
			g_target_cmd_decompress_safe=""
			zxfer_init_endpoint_execution_context origin
			zxfer_init_endpoint_execution_context target
			zxfer_init_restore_property_helpers

			printf 'source_os=<%s>\n' "$g_source_operating_system"
			printf 'origin_zfs=%s\n' "$g_origin_cmd_zfs"
			printf 'origin_compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'dest_os=<%s>\n' "$g_destination_operating_system"
			printf 'target_zfs=%s\n' "$g_target_cmd_zfs"
			printf 'target_decompress=%s\n' "$g_target_cmd_decompress_safe"
			printf 'remote_cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "zxfer_init_variables should look up the local operating system once and publish it." \
		"$output" "local_os=LocalOS"
	assertContains "A local origin should use the local operating system and zfs." \
		"$output" "local_source_os=LocalOS local_origin_zfs=/sbin/zfs"
	assertContains "A local target should use the local operating system and zfs." \
		"$output" "local_dest_os=LocalOS local_target_zfs=/sbin/zfs"
	assertContains "Transfer command context initialization should copy the local compression helper to the origin transport defaults." \
		"$output" "transfer_origin=local-compress"
	assertContains "Transfer command context initialization should copy the local decompression helper to the target transport defaults." \
		"$output" "transfer_target=local-decompress"
	assertContains "Restore-helper initialization should resolve the local cat helper on the secure PATH when restore mode is enabled without an origin host." \
		"$output" "local_cat=$cat_dir/cat"
	assertContains "Dry-run remote source initialization should skip live OS probing and leave the cached source OS blank." \
		"$output" "source_os=<>"
	assertContains "Dry-run remote source initialization should still seed the origin zfs helper from the local zfs path." \
		"$output" "origin_zfs=/sbin/zfs"
	assertContains "Dry-run remote source initialization should quote the remote compression command when compression is enabled." \
		"$output" "origin_compress=quoted<zstd -3>"
	assertContains "Dry-run remote destination initialization should skip live OS probing and leave the cached destination OS blank." \
		"$output" "dest_os=<>"
	assertContains "Dry-run remote destination initialization should still seed the target zfs helper from the local zfs path." \
		"$output" "target_zfs=/sbin/zfs"
	assertContains "Dry-run remote destination initialization should quote the remote decompression command when compression is enabled." \
		"$output" "target_decompress=quoted<zstd -d>"
	assertContains "Dry-run remote restore-helper initialization should fall back to a literal cat helper." \
		"$output" "remote_cat=cat"
}

test_zxfer_trap_exit_cleans_registered_runtime_artifacts() {
	registered_file="$TEST_TMPDIR/zxfer.registered-runtime-file"
	registered_dir="$TEST_TMPDIR/zxfer.registered-runtime-dir"
	: >"$registered_file"
	mkdir -p "$registered_dir/subdir"
	: >"$registered_dir/subdir/payload"

	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_register_runtime_artifact_path "$registered_dir"
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success after removing registered runtime artifacts." \
		0 "$status"
	assertEquals "zxfer_trap_exit should keep stdout clean while removing registered runtime artifacts." \
		"" "$output"
	assertFalse "zxfer_trap_exit should remove registered runtime files." \
		"[ -e \"$registered_file\" ]"
	assertFalse "zxfer_trap_exit should remove registered runtime directories." \
		"[ -e \"$registered_dir\" ]"
}

test_zxfer_trap_exit_restores_shell_modes_before_mirroring_the_report() {
	# A signal can land while zxfer_create_runtime_artifact_file holds umask
	# 077 and noclobber, or between zxfer_split_begin and zxfer_split_end.
	# The report must still reach ZXFER_ERROR_LOG; a read-only parent (unless
	# running as root) sends it through the private fallback lock.
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
			printf 'noclobber=%s noglob=%s umask=%s ifs=%s\n' "$l_noclobber" \
				"$l_noglob" "$(umask)" "$(printf '%s' "$IFS" | od -An -tx1 | tr -d ' \n')" \
				>"$modes_file"
		}
		g_zxfer_failure_class="runtime"
		g_zxfer_failure_stage="unit"
		g_zxfer_failure_message="interrupted mid-allocation"
		g_services_need_relaunch=0
		g_zxfer_run_umask=0027
		umask 077
		set -C -f
		IFS=:
		false
		zxfer_trap_exit
	) >/dev/null 2>&1 || status=$?
	chmod 700 "$log_dir"

	assertEquals "zxfer_trap_exit should keep the failing status." 1 "$status"
	assertEquals "zxfer_trap_exit should restore noclobber, noglob, the run umask and default IFS before reporting." \
		"noclobber=off noglob=off umask=0027 ifs=20090a" "$(cat "$modes_file")"
	assertContains "The failure report should reach ZXFER_ERROR_LOG after an interrupted allocation." \
		"$(cat "$log_path")" "interrupted mid-allocation"
	assertContains "Mirroring should keep the earlier log contents." \
		"$(cat "$log_path")" "existing: keep-me"
}

test_zxfer_trap_exit_removes_the_per_run_temp_root_on_success_and_failure_paths() {
	success_output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_get_temp_file >/dev/null
			printf 'root=%s\n' "$g_zxfer_run_tmp_root" >&2
			true
			zxfer_trap_exit
		) 2>&1
	)
	success_status=$?
	success_root=${success_output#root=}

	assertEquals "zxfer_trap_exit should preserve success after removing the per-run temp root." \
		0 "$success_status"
	assertNotEquals "The per-run temp root should exist before the trap runs." \
		"" "$success_root"
	assertFalse "zxfer_trap_exit should remove the per-run temp root and everything below it." \
		"[ -e \"$success_root\" ]"

	failure_root_file="$TEST_TMPDIR/trap-failure-root.path"
	(
		zxfer_close_all_ssh_control_sockets() {
			:
		}
		zxfer_echoV() {
			:
		}
		zxfer_get_temp_file >/dev/null
		printf '%s\n' "$g_zxfer_run_tmp_root" >"$failure_root_file"
		false
		zxfer_trap_exit
	) 2>/dev/null
	failure_status=$?
	failure_root=$(cat "$failure_root_file")

	assertEquals "zxfer_trap_exit should preserve the failing exit status while removing the per-run temp root." \
		1 "$failure_status"
	assertFalse "zxfer_trap_exit should remove the per-run temp root on failure paths too." \
		"[ -e \"$failure_root\" ]"
}

test_zxfer_trap_exit_surfaces_failed_run_tmp_root_removal_as_trap_cleanup_failure() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			zxfer_get_temp_file >/dev/null
			rm() {
				return 1
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when the per-run temp root cannot be removed." \
		1 "$status"
	assertContains "Failed run-root removal should surface as a runtime trap-cleanup failure." \
		"$output" "class=runtime"
	assertContains "Failed run-root removal should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Failed run-root removal should report the runtime temp-artifact cleanup message." \
		"$output" "message=Failed to remove one or more runtime temp artifacts during exit."
}

test_run_tmp_root_is_removed_when_the_process_is_terminated_mid_run() {
	leak_tmpdir="$TEST_TMPDIR/sigterm-leak-tmp"
	ready_flag="$TEST_TMPDIR/sigterm-child.ready"
	child_script="$TEST_TMPDIR/sigterm-child.sh"
	child_stderr="$TEST_TMPDIR/sigterm-child.stderr"
	rm -rf "$leak_tmpdir" "$ready_flag"
	mkdir -p "$leak_tmpdir"

	cat >"$child_script" <<EOF
#!/bin/sh
ZXFER_SOURCE_MODULES_ROOT="$ZXFER_ROOT" \\
	ZXFER_SOURCE_MODULES_THROUGH=zxfer_session.sh \\
	. "$ZXFER_ROOT/src/zxfer_modules.sh"
zxfer_load_modules zxfer_session.sh
TMPDIR="$leak_tmpdir"
export TMPDIR
zxfer_init_session_environment() { :; }
zxfer_session_initialize
zxfer_get_temp_file >/dev/null
: >"$ready_flag"
# An interruptible wait so the TERM trap runs promptly.
sleep 30 &
wait \$!
EOF

	/bin/sh "$child_script" >/dev/null 2>"$child_stderr" &
	child_pid=$!
	wait_count=0
	while [ ! -e "$ready_flag" ] && [ "$wait_count" -lt 100 ]; do
		wait_count=$((wait_count + 1))
		sleep 0.1 2>/dev/null || sleep 1
	done
	assertTrue "The SIGTERM fixture child should reach its ready state." \
		"[ -e \"$ready_flag\" ]"
	assertNotEquals "The SIGTERM fixture child should allocate under the per-run temp root before the signal." \
		"" "$(ls -A "$leak_tmpdir")"

	kill -s TERM "$child_pid" 2>/dev/null
	wait "$child_pid" 2>/dev/null

	assertEquals "A SIGTERM mid-run must not leak any temp state into TMPDIR." \
		"" "$(ls -A "$leak_tmpdir")"
}

test_zxfer_trap_exit_aborts_supervised_background_jobs_before_legacy_pid_cleanup() {
	cleanup_log="$TEST_TMPDIR/trap_supervisor_cleanup.log"
	: >"$cleanup_log"

	output=$(
		(
			CLEANUP_LOG="$cleanup_log"
			zxfer_abort_all_send_jobs() {
				printf '%s\n' "abort" >>"$CLEANUP_LOG"
			}
			zxfer_kill_registered_cleanup_pids() {
				printf '%s\n' "legacy" >>"$CLEANUP_LOG"
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				:
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve success when supervised background cleanup succeeds." \
		0 "$status"
	assertEquals "zxfer_trap_exit should run supervised background cleanup before legacy bare-PID cleanup." \
		"abort
legacy" "$(cat "$cleanup_log")"
	assertEquals "zxfer_trap_exit should keep stdout clean when cleanup succeeds." \
		"" "$output"
}

test_zxfer_trap_exit_fails_closed_when_supervised_background_cleanup_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_abort_all_send_jobs() {
				g_zxfer_send_job_abort_failure_message="validated abort failed"
				return 17
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve supervised background cleanup failure status." \
		17 "$status"
	assertContains "Supervised background cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Supervised background cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Supervised background cleanup failures should preserve the validated abort failure message." \
		"$output" "message=validated abort failed"
}

test_zxfer_trap_exit_fails_closed_when_validated_cleanup_helper_abort_fails() {
	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_kill_registered_cleanup_pids() {
				g_zxfer_cleanup_pid_abort_failure_message="validated cleanup helper abort failed"
				return 23
			}
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should preserve validated cleanup-helper teardown failure status." \
		23 "$status"
	assertContains "Validated cleanup-helper teardown failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "Validated cleanup-helper teardown failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "Validated cleanup-helper teardown failures should preserve the validated abort failure message." \
		"$output" "message=validated cleanup helper abort failed"
}

test_zxfer_trap_exit_fails_closed_when_ssh_socket_cleanup_fails_after_success() {
	registered_file="$TEST_TMPDIR/zxfer.trap-close-failure-artifact"
	: >"$registered_file"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	output=$(
		(
			zxfer_register_runtime_artifact_path "$registered_file"
			zxfer_close_all_ssh_control_sockets() {
				printf '%s\n' "close failed" >&2
				return 19
			}
			zxfer_echoV() {
				:
			}
			zxfer_profile_emit_summary() {
				:
			}
			zxfer_emit_failure_report() {
				printf 'status=%s\n' "$1"
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf 'stage=%s\n' "${g_zxfer_failure_stage:-}"
				printf 'message=%s\n' "${g_zxfer_failure_message:-}"
			}
			true
			zxfer_trap_exit
		) 2>&1
	)
	status=$?
	if [ "$l_restore_errexit" -eq 1 ]; then
		set -e
	fi

	assertEquals "zxfer_trap_exit should fail closed when ssh socket cleanup fails after an otherwise successful run." \
		19 "$status"
	assertContains "zxfer_trap_exit should preserve ssh socket cleanup diagnostics on stderr." \
		"$output" "close failed"
	assertContains "ssh socket cleanup failures should surface as runtime trap-cleanup failures." \
		"$output" "class=runtime"
	assertContains "ssh socket cleanup failures should mark the trap-cleanup stage." \
		"$output" "stage=trap cleanup"
	assertContains "ssh socket cleanup failures should preserve the cleanup-specific failure message." \
		"$output" "message=Failed to close one or more ssh control sockets during exit."
	assertFalse "zxfer_trap_exit should continue removing registered runtime artifacts after ssh socket cleanup failures." \
		"[ -e \"$registered_file\" ]"
}

test_session_init_initializes_dependency_state_and_temp_files() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			g_zxfer_services_to_restart="stale-service"
			g_backup_file_contents="stale-backup"
			g_restored_backup_file_contents="stale-restore"
			g_zxfer_remote_capability_response_result="stale-caps"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_ssh_control_socket_action_result="stale-action"
			g_zxfer_ssh_control_socket_action_stderr="stale-stderr"
			g_recursive_source_list="stale-source"
			g_last_common_snap="stale@snap"
			g_zxfer_send_jobs="123 456"
			g_zxfer_send_job_abort_failure_message="stale-job	kind	111	wrapper	/tmp/bg"
			g_zxfer_property_table_lookup_result="stale-lookup"
			g_zxfer_source_pvs_raw="stale=property=local"
			zxfer_init_dependency_tool_defaults() {
				:
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			zxfer_reset_session_state
			zxfer_init_session_environment
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'path=%s\n' "$PATH"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'temp_file=<%s>\n' "$g_zxfer_temp_file_result"
			printf 'temp_group=<%s>\n' "$g_zxfer_temp_file_group_result"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'backup=<%s>\n' "$g_backup_file_contents"
			printf 'restored=<%s>\n' "$g_restored_backup_file_contents"
			printf 'remote_caps=<%s>\n' "$g_zxfer_remote_capability_response_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'socket_action=<%s>\n' "$g_zxfer_ssh_control_socket_action_result"
			printf 'socket_stderr=<%s>\n' "$g_zxfer_ssh_control_socket_action_stderr"
			printf 'recursive=<%s>\n' "$g_recursive_source_list"
			printf 'last_common=<%s>\n' "$g_last_common_snap"
			printf 'send_pids=<%s>\n' "$g_zxfer_send_jobs"
			printf 'background_records=<%s>\n' "$g_zxfer_send_job_abort_failure_message"
			printf 'table_lookup=<%s>\n' "$g_zxfer_property_table_lookup_result"
			printf 'source_pvs=<%s>\n' "$g_zxfer_source_pvs_raw"
		)
	)

	assertContains "Session initialization should initialize the secure path." \
		"$output" "secure=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "Session initialization should export the strict runtime PATH once runtime startup begins." \
		"$output" "path=/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "Session initialization should record ssh control-socket support." \
		"$output" "control=1"
	assertContains "Session initialization should not allocate a temp file until a caller needs one." \
		"$output" "temp_file=<>"
	assertContains "Session initialization should not allocate a temp-file group until a caller needs one." \
		"$output" "temp_group=<>"
	assertContains "Session initialization should reset orchestration restart scratch state." \
		"$output" "restart=<>"
	assertContains "Session initialization should reset backup-metadata accumulation state." \
		"$output" "backup=<>"
	assertContains "Session initialization should reset restored backup scratch state." \
		"$output" "restored=<>"
	assertContains "Session initialization should reset remote capability handshake scratch state." \
		"$output" "remote_caps=<>"
	assertContains "Session initialization should reset remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "Session initialization should reset ssh control-socket action classification state." \
		"$output" "socket_action=<>"
	assertContains "Session initialization should reset ssh control-socket action stderr scratch state." \
		"$output" "socket_stderr=<>"
	assertContains "Session initialization should reset snapshot-discovery scratch state." \
		"$output" "recursive=<>"
	assertContains "Session initialization should reset snapshot-reconcile scratch state." \
		"$output" "last_common=<>"
	assertContains "Session initialization should reset send/receive PID tracking state." \
		"$output" "send_pids=<>"
	assertContains "Session initialization should reset supervised background-job registry state." \
		"$output" "background_records=<>"
	assertContains "Session initialization should reset property-table lookup scratch state." \
		"$output" "table_lookup=<>"
	assertContains "Session initialization should reset property-reconcile source scratch state." \
		"$output" "source_pvs=<>"
}

test_session_init_defers_strict_path_export_until_startup_helpers_finish() {
	secure_path_dir="$TEST_TMPDIR/narrow-secure-path"
	mkdir -p "$secure_path_dir"

	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			ZXFER_SECURE_PATH="$secure_path_dir"
			zxfer_init_dependency_tool_defaults() {
				:
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_reset_session_state
			zxfer_init_session_environment
			status=$?
			printf 'status=%s\n' "$status"
			printf 'path=%s\n' "$PATH"
			printf 'temp_file=<%s>\n' "$g_zxfer_temp_file_result"
		) 2>&1
	)

	assertContains "Session initialization should still finish startup when ZXFER_SECURE_PATH omits date/mktemp directories." \
		"$output" "status=0"
	assertContains "Session initialization should export the narrow secure PATH after startup completes." \
		"$output" "path=$secure_path_dir"
	assertContains "Session initialization should finish startup without allocating a temp file before the strict runtime PATH applies." \
		"$output" "temp_file=<>"
	assertNotContains "Startup should not trip over missing bootstrap utilities when the strict PATH is applied at the end of init." \
		"$output" "command not found"
}

test_session_init_reinitializes_property_module_scratch_state_when_reinvoked() {
	output=$(
		(
			TMPDIR="$TEST_TMPDIR"
			zxfer_init_dependency_tool_defaults() {
				:
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}

			zxfer_reset_session_state
			zxfer_init_session_environment

			g_zxfer_source_property_table="tank/src\tcompression=stale=local"
			g_zxfer_destination_property_table="backup/dst\tcompression=stale=local"
			g_zxfer_required_properties_result="stale-required"
			g_zxfer_adjusted_set_list="compression=lz4"
			g_zxfer_adjusted_inherit_list="mountpoint"
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result="compression=lz4=local"
			g_zxfer_remote_probe_capture_failed=1
			g_zxfer_destination_property_tree_prefetch_state=2
			g_zxfer_unsupported_filesystem_properties="compression"
			g_zxfer_unsupported_volume_properties="volblocksize"

			zxfer_reset_session_state
			zxfer_init_session_environment

			printf 'required=<%s>\n' "$g_zxfer_required_properties_result"
			printf 'source_table=<%s>\n' "${g_zxfer_source_property_table:-}"
			printf 'destination_table=<%s>\n' "${g_zxfer_destination_property_table:-}"
			printf 'adjusted_set=<%s>\n' "$g_zxfer_adjusted_set_list"
			printf 'adjusted_inherit=<%s>\n' "$g_zxfer_adjusted_inherit_list"
			printf 'override_result=<%s>\n' "$g_zxfer_override_pvs_result"
			printf 'creation_result=<%s>\n' "$g_zxfer_creation_pvs_result"
			printf 'remote_capture_failed=%s\n' "${g_zxfer_remote_probe_capture_failed:-0}"
			printf 'prefetch_state=%s\n' "$g_zxfer_destination_property_tree_prefetch_state"
			printf 'unsupported_fs=<%s>\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'unsupported_vol=<%s>\n' "$g_zxfer_unsupported_volume_properties"
		)
	)

	assertContains "Re-running session initialization should clear required-property scratch results." \
		"$output" "required=<>"
	assertContains "Re-running session initialization should clear the in-memory source property table." \
		"$output" "source_table=<>"
	assertContains "Re-running session initialization should clear the in-memory destination property table." \
		"$output" "destination_table=<>"
	assertContains "Re-running session initialization should clear adjusted set scratch state." \
		"$output" "adjusted_set=<>"
	assertContains "Re-running session initialization should clear adjusted inherit scratch state." \
		"$output" "adjusted_inherit=<>"
	assertContains "Re-running session initialization should clear derived override scratch state." \
		"$output" "override_result=<>"
	assertContains "Re-running session initialization should clear derived creation-property scratch state." \
		"$output" "creation_result=<>"
	assertContains "Re-running session initialization should clear remote probe capture-failure scratch state." \
		"$output" "remote_capture_failed=0"
	assertContains "Re-running session initialization should rearm destination property prefetch state." \
		"$output" "prefetch_state=0"
	assertContains "Re-running session initialization should clear filesystem unsupported-property cache state." \
		"$output" "unsupported_fs=<>"
	assertContains "Re-running session initialization should clear volume unsupported-property cache state." \
		"$output" "unsupported_vol=<>"
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

test_zxfer_init_endpoint_execution_context_reports_remote_decompress_resolution_failures() {
	set +e
	output=$(
		(
			g_option_T_target_host="target.example"
			g_option_z_compress=1
			g_cmd_decompress="zstd -d"
			g_cmd_zfs="/sbin/zfs"
			zxfer_get_os() {
				g_zxfer_os_result="RemoteOS"
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				g_zxfer_required_tool_result="/remote/bin/$2"
			}
			zxfer_resolve_cli_command_safe() {
				g_zxfer_resolved_cli_command_result="decompress lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_init_endpoint_execution_context target
		)
	)
	status=$?

	assertEquals "Target execution-context initialization should fail closed when the remote decompressor cannot be resolved safely." \
		1 "$status"
	assertContains "Remote decompressor resolution failures should preserve the dependency error." \
		"$output" "decompress lookup failed"
}
