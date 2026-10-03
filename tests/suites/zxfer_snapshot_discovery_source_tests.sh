#!/bin/sh
# shellcheck shell=sh
# Source listing builders, the -j parallel check, background producers and
# staged capture and status files for src/zxfer_snapshot_discovery.sh: the
# decisions and failures a unit test alone can reach (render, mktemp, spawn
# and registration failures, the per-session parallel cache). Run by
# tests/test_zxfer_snapshot_discovery.sh.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_zxfer_limit_snapshot_discovery_capture_lines_keeps_the_first_lines() {
	l_capture=$(printf '%s\n' line1 line2 line3)
	assertEquals "A limit should keep that many lines." \
		"line1
line2" "$(zxfer_limit_snapshot_discovery_capture_lines "$l_capture" 2)"
	for l_limit in invalid 0 ""; do
		assertEquals "A limit that is not a positive number should fall back to ten lines [limit:$l_limit]." \
			"$l_capture" "$(zxfer_limit_snapshot_discovery_capture_lines "$l_capture" "$l_limit")"
	done
	assertEquals "The default limit should be ten lines." \
		"$(printf 'l%s\n' 1 2 3 4 5 6 7 8 9 10)" \
		"$(zxfer_limit_snapshot_discovery_capture_lines "$(printf 'l%s\n' 1 2 3 4 5 6 7 8 9 10 11 12)")"
}

# Run zxfer_ensure_parallel_available_for_source_jobs with -j JOBS, -O ORIGIN,
# the local parallel LOCAL and the origin helper CACHED cached for
# CACHED_HOST, over a stale reason; the stubbed remote lookup answers RESULT
# with STATUS. Prints the status, the reason, the cached origin helper and
# host, and each lookup made.
# Usage: zxfer_test_ensure_parallel_outcome JOBS ORIGIN LOCAL CACHED CACHED_HOST RESULT STATUS
zxfer_test_ensure_parallel_outcome() {
	(
		g_option_j_jobs=$1
		g_option_O_origin_host=$2
		g_cmd_parallel=$3
		g_origin_parallel_cmd=$4
		g_origin_parallel_cmd_host=$5
		LOOKUP_RESULT=$6
		LOOKUP_STATUS=$7
		g_zxfer_parallel_source_job_check_result="stale reason"
		l_test_lookups=""
		zxfer_resolve_remote_required_tool() {
			l_test_lookups="$l_test_lookups $1:$2"
			g_zxfer_required_tool_result=$LOOKUP_RESULT
			return "$LOOKUP_STATUS"
		}
		zxfer_ensure_parallel_available_for_source_jobs
		printf 'status=%s reason=<%s> cmd=<%s@%s> lookups=<%s>\n' "$?" \
			"$g_zxfer_parallel_source_job_check_result" "$g_origin_parallel_cmd" \
			"$g_origin_parallel_cmd_host" "${l_test_lookups# }"
	)
}

test_ensure_parallel_available_for_source_jobs_decides_by_jobs_host_and_cache() {
	assertEquals "-j 1 needs no parallel anywhere." \
		"status=0 reason=<> cmd=<@> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 1 "" "" "" "" "" 0)"
	assertEquals "A local -j run trusts the local parallel without a version probe." \
		"status=0 reason=<> cmd=<@> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 2 "" "$PARALLEL_BIN" "" "" "" 0)"
	assertEquals "A local -j run without parallel fails with the local-host reason." \
		"status=1 reason=<The -j option requires parallel but it was not found in PATH on the local host.> cmd=<@> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 2 "" "" "" "" "" 0)"
	assertEquals "An -O run resolves the origin's parallel, trusts it and caches it for that host." \
		"status=0 reason=<> cmd=</opt/bin/parallel@origin.example> lookups=<origin.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" "" "" /opt/bin/parallel 0)"
	assertEquals "A helper cached for the same origin is reused without a lookup." \
		"status=0 reason=<> cmd=</opt/bin/parallel@origin.example> lookups=<>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" /opt/bin/parallel origin.example /usr/bin/parallel 0)"
	assertEquals "A helper cached for another origin is resolved again." \
		"status=0 reason=<> cmd=</usr/local/bin/parallel@origin-b.example> lookups=<origin-b.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin-b.example "" /opt/bin/parallel origin-a.example /usr/local/bin/parallel 0)"
	assertEquals "A parallel the origin lacks becomes the -j guidance." \
		"status=1 reason=<parallel not found on origin host origin.example but -j 2 was requested. Install parallel remotely or rerun without -j.> cmd=<@> lookups=<origin.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" "" "" 'Required dependency "parallel" not found on host origin.example in secure PATH (/usr/sbin).' 1)"
	assertEquals "Any other lookup failure keeps its message and status." \
		"status=3 reason=<Failed to query dependency \"parallel\" on host origin.example.> cmd=<@> lookups=<origin.example:parallel>" \
		"$(zxfer_test_ensure_parallel_outcome 2 origin.example "" "" "" 'Failed to query dependency "parallel" on host origin.example.' 3)"
}

# A -j listing never falls back to the serial one: when the parallel check
# fails, the builder returns the check's status and publishes its reason, or
# a generic one, instead of a command.
test_build_source_snapshot_list_cmd_fails_closed_when_the_parallel_check_fails() {
	g_option_j_jobs=2
	for l_check_case in "68|parallel check failed" "69|"; do
		l_check_reason=${l_check_case#*|}
		output=$(
			CHECK_STATUS=${l_check_case%%|*}
			CHECK_REASON=$l_check_reason
			zxfer_ensure_parallel_available_for_source_jobs() {
				# Helpers share one variable namespace; clobbering the
				# builder's scratch name must not change its result.
				l_list_status=0
				g_zxfer_parallel_source_job_check_result=$CHECK_REASON
				return "$CHECK_STATUS"
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'status=%s\n' "$?"
		)
		assertEquals "A failed check should publish its reason, or the generic one, and keep its status [check:$l_check_case]." \
			"${l_check_reason:-Failed to prepare parallel source discovery.}
status=${l_check_case%%|*}" "$output"
	done
	g_cmd_parallel=""
	assertEquals "The real check should fail a local -j run without parallel, with no serial fallback." \
		"The -j option requires parallel but it was not found in PATH on the local host.
status=1" "$(
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'status=%s\n' "$?"
		)"
}

# A builder whose rendering fails returns that status and publishes no
# command: the serial listing's role render, the origin's ssh wrapper, and an
# -O -z listing whose origin compressor is unresolved, for the proof's name
# listing and the -j listing alike.
test_source_listing_builders_fail_closed_when_rendering_fails() {
	output=$(
		(
			zxfer_render_zfs_command_for_role() {
				return 67
			}
			zxfer_test_print_source_listing zxfer_build_source_snapshot_list_cmd
			printf 'serial render=%s\n' "$?"
		)
		g_option_j_jobs=2
		g_option_O_origin_host="origin.example"
		g_origin_cmd_zfs="/remote/bin/zfs"
		g_origin_parallel_cmd="/opt/bin/parallel"
		g_origin_parallel_cmd_host="origin.example"
		for l_builder in zxfer_build_source_snapshot_name_list_cmd \
			zxfer_build_source_snapshot_list_cmd; do
			(
				zxfer_ssh_shell_command_for_host() {
					return 34
				}
				zxfer_test_print_source_listing "$l_builder"
				printf '%s ssh=%s\n' "$l_builder" "$?"
			)
			(
				g_option_z_compress=1
				g_origin_cmd_compress_safe=""
				zxfer_test_print_source_listing "$l_builder"
				printf '%s compressor=%s\n' "$l_builder" "$?"
			)
		done
	)

	assertEquals "Each failed render should keep its status and publish only a reason, never a partial command." \
		"serial render=67
zxfer_build_source_snapshot_name_list_cmd ssh=34
The origin host compression command is not resolved.
zxfer_build_source_snapshot_name_list_cmd compressor=1
zxfer_build_source_snapshot_list_cmd ssh=34
The origin host compression command is not resolved.
zxfer_build_source_snapshot_list_cmd compressor=1" "$output"
}

# A failed build throws the builder's message, or a generic one with the
# build's status when the builder published none.
test_write_source_snapshot_list_to_file_throws_the_builder_message_or_a_generic_one() {
	outfile="$TEST_TMPDIR/source_build_failure.out"
	for l_build_case in "builder failed|1" "|7"; do
		output=$(
			(
				BUILD_MESSAGE=${l_build_case%|*}
				BUILD_STATUS=${l_build_case#*|}
				zxfer_build_source_snapshot_list_cmd() {
					g_zxfer_source_snapshot_list_cmd_result=$BUILD_MESSAGE
					return "$BUILD_STATUS"
				}
				zxfer_throw_error() {
					printf '<%s:%s>' "$1" "$2"
					exit 1
				}
				zxfer_write_source_snapshot_list_to_file "$outfile" "$outfile.err" "$outfile.sorted"
			)
		)
		l_build_message=${l_build_case%|*}
		assertEquals "A failed build should throw its message and status [build:$l_build_case]." \
			"<${l_build_message:-Failed to build source snapshot discovery command.}:${l_build_case#*|}>" \
			"$output"
	done
}

# A failed launch keeps its status; all output paths stay with the operation
# owner, including when a pipeline status file cannot be allocated.
test_write_source_snapshot_list_to_file_keeps_the_status_of_a_failed_launch() {
	outfile="$TEST_TMPDIR/source_launch_failure.out"
	errfile="$TEST_TMPDIR/source_launch_failure.err"

	output=$(
		(
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' snap"
			}
			zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
				return 32
			}
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile" "$outfile.sorted"
			printf 'launch=%s\n' "$?"
		)
		(
			temp_calls=0
			zxfer_get_temp_file() {
				temp_calls=$((temp_calls + 1))
				[ "$temp_calls" -eq 1 ] || return 33
				g_zxfer_temp_file_result="$TEST_TMPDIR/source_launch_failure.sorted"
				: >"$g_zxfer_temp_file_result"
			}
			zxfer_build_source_snapshot_list_cmd() {
				g_zxfer_source_snapshot_list_cmd_result="printf '%s\n' snap"
			}
			zxfer_write_source_snapshot_list_to_file "$outfile" "$errfile" "$outfile.sorted"
			printf 'status file=%s calls=%s\n' "$?" "$temp_calls"
		)
	)

	assertEquals "A failed launch should keep its status with caller-owned output paths." \
		"launch=32
status file=33 calls=2" "$output"
}

# A producer that cannot allocate its status files never starts and keeps
# the allocation's status: the full listing's group, and the proof listing's
# first and count status files.
test_source_snapshot_producers_keep_the_status_of_a_failed_setup() {
	sorted_file="$TEST_TMPDIR/producer_setup.sorted"

	output=$(
		(
			zxfer_create_temp_file_group() {
				return 14
			}
			zxfer_execute_source_snapshot_list_background_cmd_with_sort "printf x" \
				"$TEST_TMPDIR/producer_setup.out" "" "$sorted_file"
			printf 'records=%s\n' "$?"
		)
		for l_setup_case in 1:23 2:57; do
			(
				FAIL_AT=${l_setup_case%:*}
				FAIL_STATUS=${l_setup_case#*:}
				temp_calls=0
				zxfer_get_temp_file() {
					temp_calls=$((temp_calls + 1))
					[ "$temp_calls" -ne "$FAIL_AT" ] || return "$FAIL_STATUS"
					g_zxfer_temp_file_result="$TEST_TMPDIR/producer_setup.$temp_calls"
					: >"$g_zxfer_temp_file_result"
				}
				zxfer_execute_source_snapshot_name_list_background_sort_cmd "printf x" \
					"$sorted_file" "" "$TEST_TMPDIR/producer_setup.count"
				printf 'names %s=%s\n' "$FAIL_AT" "$?"
			)
		done
	)

	assertEquals "Each failed status-file allocation should keep its status before any launch." \
		"records=14
names 1=23
names 2=57" "$output"
}

test_source_snapshot_producers_preserve_spawn_failure_without_registering_stale_pid() {
	for producer in names records; do
		status=0
		output=$(
			g_last_background_pid=12345
			zxfer_spawn_background_shell() { return 79; }
			zxfer_register_cleanup_pid() { printf 'registered stale PID'; }
			if [ "$producer" = names ]; then
				zxfer_execute_source_snapshot_name_list_background_sort_cmd \
					'printf x' "$TEST_TMPDIR/failed-spawn.sorted"
			else
				zxfer_execute_source_snapshot_list_background_cmd_with_sort \
					'printf x' "$TEST_TMPDIR/failed-spawn.out" "" \
					"$TEST_TMPDIR/failed-spawn.sorted"
			fi
		) || status=$?

		assertEquals "$producer discovery must preserve the spawn failure." 79 "$status"
		assertEquals "$producer discovery must not register a previous helper PID after a failed spawn." \
			"" "$output"
	done
}

# A producer whose cleanup registration fails is stopped at once: TERM, then
# KILL to its whole owned scope, and a failed KILL keeps that scope
# registered for the trap's ordered retry. Both producers share this path; the
# proof's producer also runs it for real, in the probed spawn mode and the
# wrapper, with a descendant that ignores TERM.
test_source_snapshot_producers_stop_an_unregistered_producer_and_its_descendants() {
	child_file="$TEST_TMPDIR/source_register.child"
	sorted_file="$TEST_TMPDIR/source_register.sorted"
	outfile="$TEST_TMPDIR/source_register.out"
	errfile="$TEST_TMPDIR/source_register.err"
	for producer_mode in names records; do
		output=$(
			g_zxfer_cleanup_pid_abort_grace_seconds=0
			zxfer_spawn_background_shell() {
				g_last_background_pid=12345
				g_zxfer_background_shell_scope=pgid
			}
			zxfer_register_cleanup_pid() { return 1; }
			zxfer_signal_background_shell() {
				printf 'signal=%s:%s:%s\n' "$1" "$2" "$3"
				[ "$3" = TERM ]
			}
			if [ "$producer_mode" = names ]; then
				zxfer_execute_source_snapshot_name_list_background_sort_cmd \
					'printf x' "$sorted_file" "$errfile"
			else
				zxfer_execute_source_snapshot_list_background_cmd_with_sort \
					'printf x' "$outfile" "$errfile" "$sorted_file"
			fi
			printf 'status=%s pid=%s\n' "$?" "$g_last_background_pid"
			zxfer_find_cleanup_pid_record 12345
			printf 'retained=%s scope=%s\n' "$?" "$g_zxfer_cleanup_pid_record_scope"
		)
		assertContains "$producer_mode preserves the registration failure and published PID on failed KILL." \
			"$output" "status=1 pid=12345"
		assertContains "$producer_mode escalates the entire owned scope after TERM." \
			"$output" "signal=12345:pgid:KILL"
		assertContains "$producer_mode retains the failed scope for ordered trap cleanup." \
			"$output" "retained=0 scope=pgid"
	done

	# The producer's shell exits on TERM; this bounded descendant ignores it.
	# Both the probed spawn mode and the cleanup wrapper must stop it.
	# shellcheck disable=SC2016 # The fixture child expands its own PID/path.
	zxfer_render_shell_command_from_argv sh -c \
		'trap "" TERM; printf "%s\n" "$$" >"$1"; exec sleep 30' \
		zxfer-test "$child_file"
	producer_cmd=$g_zxfer_shell_command_result
	zxfer_init_background_shell_spawn_mode
	spawn_modes=$g_zxfer_background_shell_spawn_mode
	[ "$spawn_modes" = wrapper ] || spawn_modes="$spawn_modes wrapper"
	for spawn_mode in $spawn_modes; do
		rm -f "$child_file"
		output=$(
			g_zxfer_background_shell_spawn_mode=$spawn_mode
			g_zxfer_cleanup_pid_abort_grace_seconds=0
			zxfer_register_cleanup_pid() {
				tries=0
				while [ ! -s "$child_file" ] && [ "$tries" -lt 30 ]; do
					sleep 0.1 2>/dev/null || sleep 1
					tries=$((tries + 1))
				done
				return 1
			}
			zxfer_execute_source_snapshot_name_list_background_sort_cmd \
				"$producer_cmd" "$sorted_file" "$errfile"
			printf 'status=%s pid=<%s> records=<%s>\n' \
				"$?" "$g_last_background_pid" "$g_zxfer_cleanup_pid_records"
		)
		assertContains "$spawn_mode completes failed-registration cleanup and clears ownership." \
			"$output" 'status=1 pid=<> records=<>'
		assertTrue "$spawn_mode starts the TERM-ignoring descendant before cleanup." \
			"[ -s '$child_file' ]"
		child_pid=$(cat "$child_file" 2>/dev/null)
		[ -n "$child_pid" ] || continue
		tries=0
		while kill -s 0 "$child_pid" 2>/dev/null && [ "$tries" -lt 20 ]; do
			sleep 0.1 2>/dev/null || sleep 1
			tries=$((tries + 1))
		done
		if kill -s 0 "$child_pid" 2>/dev/null; then
			command kill -s KILL "$child_pid" 2>/dev/null || :
			fail "$spawn_mode forgot a TERM-ignoring descendant after registration failed."
		fi
	done
}

test_zxfer_read_snapshot_discovery_capture_file_reads_or_fails_closed() {
	capture_file="$TEST_TMPDIR/snapshot_discovery_capture.txt"
	capture_dir="$TEST_TMPDIR/snapshot_discovery_capture_dir"
	printf '%s\n' "first line" "second line" >"$capture_file"
	mkdir -p "$capture_dir"

	zxfer_read_snapshot_discovery_capture_file "$capture_file"
	assertEquals "A staged capture should be read whole, trailing newline included." \
		"first line
second line
" "$g_zxfer_snapshot_discovery_file_read_result"

	g_zxfer_snapshot_discovery_file_read_result="stale-capture"
	zxfer_read_snapshot_discovery_capture_file "$capture_dir" 2>/dev/null
	assertNotEquals "A capture that cannot be opened for reading should fail." 0 "$?"
	assertEquals "A failed read should publish no stale or partial capture." \
		"" "$g_zxfer_snapshot_discovery_file_read_result"
}

test_read_snapshot_discovery_status_file_defaults_and_validates() {
	status_file="$TEST_TMPDIR/snapshot_discovery_status.out"
	# file contents ("-" for none, "empty" for an empty file)|default|status|result
	while IFS='|' read -r l_contents l_default l_status l_result; do
		rm -f "$status_file"
		case $l_contents in
		-) ;;
		empty) : >"$status_file" ;;
		*) printf '%s\n' "$l_contents" >"$status_file" ;;
		esac
		if [ -n "$l_default" ]; then
			zxfer_read_snapshot_discovery_status_file "$status_file" "$l_default"
		else
			zxfer_read_snapshot_discovery_status_file "$status_file"
		fi
		assertEquals "The reader's status [file:$l_contents default:$l_default]." "$l_status" "$?"
		assertEquals "The published status [file:$l_contents default:$l_default]." \
			"$l_result" "$g_zxfer_snapshot_discovery_status_file_result"
	done <<'EOF_STATUS'
-|37|0|37
empty|37|0|37
7|37|0|7
bad|37|1|bad
-||0|1
EOF_STATUS
}
