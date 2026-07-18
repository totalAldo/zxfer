#!/bin/sh
#
# shunit2 tests for zxfer_cleanup_child_wrapper.sh.
#
# shellcheck disable=SC1090,SC2016,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_cleanup_child_wrapper"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1 \
		. "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh"
	unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
}

test_cleanup_child_wrapper_start_token_validates_inputs_and_preserves_shell_state() {
	token_file="$TEST_TMPDIR/cleanup_child_wrapper.start_token"
	zxfer_test_capture_subshell '
		ps() {
			printf "%s\n" "Wed Jul 15 12:34:56 2026"
		}

		zxfer_cleanup_child_wrapper_get_process_start_token "" lstart >/dev/null
		printf "invalid_pid_status=%s\n" "$?"
		zxfer_cleanup_child_wrapper_get_process_start_token 701 invalid >/dev/null
		printf "invalid_selector_status=%s\n" "$?"

		unset IFS
		set -f
		zxfer_cleanup_child_wrapper_get_process_start_token 701 lstart >"'"$token_file"'"
		printf "token_status=%s\n" "$?"
		printf "token=<%s>\n" "$(cat "'"$token_file"'")"
		case $- in
		*f*) printf "globbing=disabled\n" ;;
		*) printf "globbing=enabled\n" ;;
		esac
		if [ "${IFS+set}" = set ]; then
			printf "ifs=set\n"
		else
			printf "ifs=unset\n"
		fi
	'

	assertContains "Start-token lookup should reject an empty PID." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "invalid_pid_status=1"
	assertContains "Start-token lookup should reject an unsupported ps selector." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "invalid_selector_status=1"
	assertContains "Start-token lookup should normalize the validated ps record." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "token=<lstart:Wed Jul 15 12:34:56 2026>"
	assertContains "Start-token lookup should preserve caller-disabled globbing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "globbing=disabled"
	assertContains "Start-token lookup should restore an originally unset IFS." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ifs=unset"
}

test_cleanup_child_wrapper_start_token_fails_closed_when_ps_has_no_record() {
	zxfer_test_capture_subshell '
		ps() {
			return 37
		}

		zxfer_cleanup_child_wrapper_get_process_start_token 701 stime
	'

	assertEquals "Start-token lookup should fail closed after both ps forms return no record." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed start-token lookup should not publish a partial identity." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_cleanup_child_wrapper_helpers_cover_current_shell_paths() {
	zxfer_test_capture_subshell '
		set +e
		ps() {
			case "$*" in
			-A* | *" -A "*)
				printf "%s\n" "$$ 1 wrapper-token"
				printf "%s\n" "701 $$ child-token"
				printf "%s\n" "702 701 grandchild-token"
				;;
			*" -p 701") printf "%s\n" "child-token" ;;
			*" -p 702") printf "%s\n" "grandchild-token" ;;
			esac
		}
		kill() {
			printf "kill:%s\n" "${3:-$2}"
			return 0
		}

		descendants=$(zxfer_cleanup_child_wrapper_list_descendants)
		printf "descendants=<%s>\n" "$(printf "%s\n" "$descendants" | cut -f1 | tr "\n" " " | sed "s/[[:space:]]*$//")"
		zxfer_cleanup_child_wrapper_abort_descendants
		printf "abort=%s\n" "$?"
	'

	assertContains "Cleanup child wrapper should enumerate descendant pids in descending order." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "descendants=<702 701>"
	assertContains "Cleanup child wrapper should signal each enumerated descendant during abort handling." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "kill:702"
	assertContains "Cleanup child wrapper should signal later descendants during abort handling too." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "kill:701"
	assertContains "Cleanup child wrapper abort helper should return success when every descendant signal succeeds." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "abort=0"
}

test_cleanup_child_wrapper_snapshot_and_getter_share_stime_header_fallback() {
	ps_log="$TEST_TMPDIR/cleanup_child_wrapper.stime_ps"
	signal_log="$TEST_TMPDIR/cleanup_child_wrapper.stime_signal"
	zxfer_test_capture_subshell '
		LC_ALL=fr_FR
		ps() {
			printf "LC_ALL=%s args=%s\n" "$LC_ALL" "$*" >>"'"$ps_log"'"
			if [ "$1" = "-A" ]; then
				[ "${7:-}" = "stime" ] || return 1
				printf "PID PPID STIME\n%s 1 root-stime\n701 %s child-stime\n" "$$" "$$"
				return 0
			fi
			[ "${2:-}" = "stime" ] || return 1
			printf "STIME\nchild-stime\n"
		}
		kill() {
			printf "signal:%s\n" "$*" >>"'"$signal_log"'"
			return 0
		}
		records=$(zxfer_cleanup_child_wrapper_list_descendants)
		printf "records=<%s>\n" "$records"
		zxfer_cleanup_child_wrapper_signal_descendant_records "$records" TERM
		printf "status=%s\n" "$?"
	'

	assertContains "Header-form stime fallback should retain the same selector in the snapshot record." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "701	stime:child-stime"
	assertContains "The matching getter fallback should validate and signal the descendant." \
		"$(cat "$signal_log")" "signal:-s TERM 701"
	assertNotContains "Every snapshot and getter ps call must force the C locale." \
		"$(cat "$ps_log")" "LC_ALL=fr_FR"
}

test_cleanup_child_wrapper_snapshot_accepts_header_lstart_fallback() {
	zxfer_test_capture_subshell '
		ps() {
			case "$*" in
			*"lstart="*) return 1 ;;
			*" lstart")
				printf "PID PPID STARTED\n%s 1 wrapper-token\n701 %s child-token\n" "$$" "$$"
				return 0
				;;
			esac
			return 37
		}

		zxfer_cleanup_child_wrapper_list_descendants
	'

	assertEquals "Header-form lstart discovery should succeed before the stime fallback is needed." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Header-form lstart discovery should retain the selected identity format." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "701	lstart:child-token"
}

test_cleanup_child_wrapper_validated_roots_retain_matches_and_fail_closed_on_live_unknowns() {
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_get_process_start_token() {
			case "$1" in
			701) printf "%s\n" "lstart:matching-token" ;;
			*) return 1 ;;
			esac
		}
		kill() {
			[ "$2" = "0" ] && [ "$3" = "702" ]
		}

		zxfer_cleanup_child_wrapper_build_validated_descendant_roots \
			"701	lstart:matching-token
702	lstart:unavailable-token"
	'

	assertEquals "A live descendant with an unreadable identity should make root refresh fail closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A matching retained identity should remain an eligible refresh root." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "$$ 701"
	assertNotContains "A live descendant with an unreadable identity must not become a refresh root." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "702"
}

test_cleanup_child_wrapper_signal_treats_exited_signal_race_as_success() {
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_get_process_start_token() {
			printf "%s\n" "lstart:matching-token"
		}
		kill() {
			return 1
		}
		zxfer_cleanup_child_wrapper_signal_descendant_records \
			"701	lstart:matching-token" TERM
		printf "status=%s\n" "$?"
	'

	assertContains "A descendant that exits between identity validation and signal delivery should be treated as gone." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=0"
}

test_cleanup_child_wrapper_signal_fails_closed_for_live_unverifiable_or_unsignallable_descendants() {
	signal_log="$TEST_TMPDIR/cleanup_child_wrapper.signal_failures"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_get_process_start_token() {
			case "$1" in
			702) printf "%s\n" "lstart:matching-token" ;;
			*) return 1 ;;
			esac
		}
		kill() {
			if [ "$2" = "0" ]; then
				return 0
			fi
			printf "%s\n" "$*" >>"'"$signal_log"'"
			return 1
		}

		zxfer_cleanup_child_wrapper_signal_descendant_records \
			"701	lstart:unavailable-token
702	lstart:matching-token" TERM
	'

	assertEquals "Live descendants that cannot be verified or signalled should fail teardown closed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertNotContains "An unverifiable live PID must never receive the requested signal." \
		"$(cat "$signal_log")" "TERM 701"
	assertContains "A verified live PID should still receive the requested signal attempt." \
		"$(cat "$signal_log")" "TERM 702"
}

test_cleanup_child_wrapper_extend_stopped_records_publishes_validated_work_after_failures() {
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_build_validated_descendant_roots() {
			printf "%s\n" "$$"
			return 37
		}
		zxfer_cleanup_child_wrapper_list_descendants() {
			printf "%s\n" "701	lstart:fresh-token"
		}
		zxfer_cleanup_child_wrapper_signal_descendant_records() {
			return 41
		}

		zxfer_cleanup_child_wrapper_extend_stopped_descendant_records ""
		printf "first_status=%s\n" "$?"
		printf "first_records=<%s>\n" "$g_zxfer_cleanup_wrapper_extended_records"

		zxfer_cleanup_child_wrapper_build_validated_descendant_roots() {
			printf "%s\n" "$$ 700"
		}
		zxfer_cleanup_child_wrapper_extend_stopped_descendant_records \
			"700	lstart:retained-token"
		printf "second_status=%s\n" "$?"
		printf "second_records=<%s>\n" "$g_zxfer_cleanup_wrapper_extended_records"
	'

	assertContains "Refresh should preserve the first identity-validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "first_status=37"
	assertContains "Refresh should publish independently discovered records after an earlier validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "first_records=<701	lstart:fresh-token>"
	assertContains "Refresh should surface a STOP failure when prior validation succeeded." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "second_status=41"
	assertContains "Refresh should retain old and newly discovered validated records for later KILL attempts." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "second_records=<700	lstart:retained-token
701	lstart:fresh-token>"
}

test_cleanup_child_wrapper_on_signal_returns_143() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.on_signal"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() {
			return 0
		}
		zxfer_cleanup_child_wrapper_signal_descendant_records() {
			printf "%s\n" "signalled:$2" >>"'"$marker_file"'"
		}
		zxfer_cleanup_child_wrapper_abort_grace_wait() { :; }
		zxfer_cleanup_child_wrapper_on_signal
	'

	assertEquals "Cleanup child wrapper signal handling should use the documented 143 exit status." \
		143 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Cleanup child wrapper signal handling should TERM descendants before exiting." \
		"$(cat "$marker_file")" "signalled:TERM"
	assertContains "Cleanup child wrapper signal handling should KILL survivors before exiting." \
		"$(cat "$marker_file")" "signalled:KILL"
}

test_cleanup_child_wrapper_on_signal_waits_for_wrapped_child() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.on_signal_wait"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() { return 0; }
		zxfer_cleanup_child_wrapper_signal_descendant_records() { :; }
		zxfer_cleanup_child_wrapper_abort_grace_wait() { :; }
		wait() {
			printf "%s\n" "waited:$1" >"'"$marker_file"'"
			return 0
		}
		l_cleanup_wrapper_child_pid=4242
		zxfer_cleanup_child_wrapper_on_signal
	'

	assertEquals "Cleanup child wrapper signal handling should keep the documented 143 exit status after waiting." \
		143 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Cleanup child wrapper signal handling should wait for the direct wrapped child before exiting." \
		"waited:4242" "$(tr -d '[:space:]' <"$marker_file")"
}

test_cleanup_child_wrapper_abort_descendants_preserves_listing_failures() {
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() {
			return 37
		}
		zxfer_cleanup_child_wrapper_abort_descendants
		printf "status=%s\n" "$?"
	'

	assertContains "Cleanup child wrapper abort handling should preserve descendant-listing failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=37"
}

test_cleanup_child_wrapper_list_descendants_preserves_awk_failures() {
	zxfer_test_capture_subshell '
		ps() {
			printf "%s\n" "$$ 1 wrapper-token" "701 $$ child-token"
		}
		awk() {
			return 41
		}

		zxfer_cleanup_child_wrapper_list_descendants >/dev/null
		printf "status=%s\n" "$?"
	'

	assertContains "Cleanup child wrapper descendant discovery must not let sort mask an awk failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=41"
}

test_cleanup_child_wrapper_abort_descendants_refuses_recycled_pid() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.recycled-descendant"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() {
			printf "701\tlstart:original-token\n"
		}
		zxfer_cleanup_child_wrapper_get_process_start_token() {
			printf "%s\n" "lstart:replacement-token"
		}
		kill() {
			printf "kill:%s\n" "$*" >"'"$marker_file"'"
			return 0
		}
		zxfer_cleanup_child_wrapper_abort_descendants
		printf "status=%s\n" "$?"
	'

	assertContains "A recycled descendant PID should make wrapper teardown fail closed." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
	assertFalse "A recycled descendant PID must never receive TERM." \
		"[ -e '$marker_file' ]"
}

test_cleanup_child_wrapper_on_signal_aborts_known_child_when_discovery_fails() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.discovery_failure"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() {
			return 37
		}
		zxfer_cleanup_child_wrapper_abort_grace_wait() { :; }
		kill() {
			printf "kill:%s:%s:%s\n" "$1" "$2" "$3" >>"'"$marker_file"'"
			return 0
		}
		wait() {
			printf "wait:%s\n" "$1" >>"'"$marker_file"'"
			return 0
		}
		l_cleanup_wrapper_child_pid=4242
		zxfer_cleanup_child_wrapper_on_signal
	'

	assertEquals "Cleanup child wrapper signal handling should fail closed when descendant discovery remains unproven." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Cleanup child wrapper signal handling should TERM its known direct child when descendant discovery fails." \
		"$(cat "$marker_file")" "kill:-s:TERM:4242"
	assertContains "Cleanup child wrapper signal handling should reap the known direct child after signalling it." \
		"$(cat "$marker_file")" "wait:4242"
}

test_cleanup_child_wrapper_on_signal_fails_closed_on_recycled_descendant() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.recycled-signal"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() {
			printf "701\tlstart:original-token\n"
		}
		zxfer_cleanup_child_wrapper_get_process_start_token() {
			printf "%s\n" "lstart:replacement-token"
		}
		zxfer_cleanup_child_wrapper_abort_grace_wait() { :; }
		kill() {
			[ "$2" = "0" ] && return 0
			printf "signal:%s\n" "$*" >"'"$marker_file"'"
			return 0
		}
		zxfer_cleanup_child_wrapper_on_signal
	'

	assertEquals "Unproven descendant identity during KILL escalation should fail wrapper teardown closed." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertFalse "A recycled descendant must not receive TERM or KILL." \
		"[ -e '$marker_file' ]"
}

test_cleanup_child_wrapper_on_signal_fails_when_known_child_cannot_be_signalled() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.signal-failure"
	zxfer_test_capture_subshell '
		zxfer_cleanup_child_wrapper_list_descendants() {
			return 37
		}
		zxfer_cleanup_child_wrapper_abort_grace_wait() { :; }
		kill() {
			printf "kill:%s\n" "$*" >>"'"$marker_file"'"
			[ "$2" = "0" ]
		}
		wait() {
			printf "wait:%s\n" "$1" >>"'"$marker_file"'"
			return 0
		}
		l_cleanup_wrapper_child_pid=4242
		zxfer_cleanup_child_wrapper_on_signal
	'

	assertEquals "Discovery failure should report teardown failure when the owned direct child remains live after a failed TERM." \
		125 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The wrapper should attempt TERM on its owned direct child." \
		"$(cat "$marker_file")" "kill:-s TERM 4242"
	assertNotContains "A still-live child whose TERM failed must not be waited indefinitely." \
		"$(cat "$marker_file")" "wait:4242"
}

test_cleanup_child_wrapper_main_requires_command() {
	zxfer_test_capture_subshell "sh '$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh'"

	assertEquals "Cleanup child wrapper should fail closed when no command is supplied." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_main_reports_stdin_duplication_failures() {
	sh -c 'exec 3<&0' <&- >/dev/null 2>&1
	expected_status=$?
	assertNotEquals "Closed-stdin duplication should fail in the active test shell." \
		0 "$expected_status"

	zxfer_test_capture_subshell "sh '$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh' 'exit 0' <&-"

	assertEquals "Cleanup child wrapper should preserve stdin-duplication failures before launching the worker." \
		"$expected_status" "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_source_executes_main_when_not_source_only() {
	zxfer_test_capture_subshell '
		unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
		set -- "exit 0"
		. "'"$ZXFER_ROOT"'/src/zxfer_cleanup_child_wrapper.sh"
	'

	assertEquals "Sourcing the cleanup child wrapper without the source-only guard should execute the main entrypoint." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_source_requires_command_when_not_source_only() {
	zxfer_test_capture_subshell '
		unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
		set --
		. "'"$ZXFER_ROOT"'/src/zxfer_cleanup_child_wrapper.sh"
	'

	assertEquals "Sourcing the cleanup child wrapper without arguments should fail closed through the main entrypoint." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_main_preserves_worker_exit_status() {
	zxfer_test_capture_subshell "sh '$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh' 'exit 7'"

	assertEquals "Cleanup child wrapper should preserve the wrapped command's exit status." \
		7 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_cleanup_child_wrapper_main_success_does_not_inspect_process_table() {
	marker_file="$TEST_TMPDIR/cleanup_child_wrapper.normal_ps"
	zxfer_test_capture_subshell '
		ps() {
			: >"'"$marker_file"'"
			return 1
		}
		zxfer_cleanup_child_wrapper_main "exit 0"
	'

	assertEquals "A clean wrapped command should complete successfully." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertFalse "Normal wrapper execution must not inspect the process table." \
		"[ -e '$marker_file' ]"
}

test_cleanup_child_wrapper_main_uses_absolute_shell_with_restricted_path() {
	no_sh_path="$TEST_TMPDIR/no-sh-path"
	mkdir -p "$no_sh_path"

	zxfer_test_capture_subshell \
		"PATH='$no_sh_path' /bin/sh '$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh' 'printf \"%s\\n\" wrapped'"

	assertEquals "Cleanup child wrapper should not depend on PATH to launch the command shell." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Cleanup child wrapper should still run shell builtins through /bin/sh." \
		"wrapped" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_cleanup_child_wrapper_main_preserves_worker_stdin_for_background_children() {
	stdin_capture="$TEST_TMPDIR/cleanup_child_wrapper.stdin"
	stdin_worker="$TEST_TMPDIR/cleanup_child_wrapper_stdin_worker.sh"

	cat >"$stdin_worker" <<EOF
#!/bin/sh
wc -c | tr -d '[:space:]' >"$stdin_capture"
EOF
	chmod +x "$stdin_worker"

	printf '%s' "wrapped stdin payload" |
		sh "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh" \
			"sh \"$stdin_worker\""
	status=$?

	assertEquals "Cleanup child wrapper should preserve success when the wrapped stdin reader exits cleanly." \
		0 "$status"
	assertEquals "Cleanup child wrapper should keep the caller's stdin attached to the background child." \
		"21" "$(tr -d '[:space:]' <"$stdin_capture")"
}

test_cleanup_child_wrapper_main_aborts_descendants_on_signal() {
	child_pid_file="$TEST_TMPDIR/cleanup_child_wrapper.child"
	child_script="$TEST_TMPDIR/cleanup_child_wrapper_child.sh"
	fake_bin="$TEST_TMPDIR/cleanup_child_wrapper_fake_bin"
	mkdir -p "$fake_bin"
	cat >"$fake_bin/ps" <<'EOF'
#!/bin/sh
case "$*" in
*lstart*)
	printf '%s\n' 'wrapper-test-start-token'
	;;
*)
	exit 37
	;;
esac
EOF
	chmod +x "$fake_bin/ps"

	cat >"$child_script" <<EOF
#!/bin/sh
printf '%s\n' "\$\$" >"$child_pid_file"
while :; do
	sleep 1
done
EOF
	chmod +x "$child_script"

	PATH="$fake_bin:$PATH" sh "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh" \
		"sh \"$child_script\"" &
	wrapper_pid=$!

	wait_tries=0
	while [ ! -s "$child_pid_file" ] && [ "$wait_tries" -lt 50 ]; do
		sleep 1
		wait_tries=$((wait_tries + 1))
	done

	assertTrue "Cleanup child wrapper should publish the wrapped child pid before the test sends a termination signal." \
		"[ -s \"$child_pid_file\" ]"
	child_pid=$(tr -d '[:space:]' <"$child_pid_file")

	kill -s TERM "$wrapper_pid"
	wait "$wrapper_pid"
	status=$?

	assertEquals "A deliberately incomplete process snapshot should make wrapper teardown fail closed." \
		125 "$status"

	kill -s TERM "$child_pid" >/dev/null 2>&1 || :
	wait "$child_pid" >/dev/null 2>&1 || :
}

test_cleanup_child_wrapper_signal_kills_term_resistant_direct_child() {
	child_pid_file="$TEST_TMPDIR/cleanup_child_wrapper.term_resistant"

	sh "$ZXFER_ROOT/src/zxfer_cleanup_child_wrapper.sh" \
		"trap '' TERM; printf '%s\n' \"\$\$\" > '$child_pid_file'; while :; do sleep 1; done" &
	wrapper_pid=$!
	wait_tries=0
	while [ ! -s "$child_pid_file" ] && [ "$wait_tries" -lt 50 ]; do
		sleep 0.1 2>/dev/null || sleep 1
		wait_tries=$((wait_tries + 1))
	done
	assertTrue "The TERM-resistant child should start before wrapper teardown." \
		"[ -s '$child_pid_file' ]"
	child_pid=$(tr -d '[:space:]' <"$child_pid_file")

	kill -s TERM "$wrapper_pid"
	wait "$wrapper_pid"
	wrapper_status=$?

	assertEquals "Wrapper teardown should retain its documented signal status after KILL escalation." \
		143 "$wrapper_status"
	assertFalse "Wrapper teardown must not leak a TERM-resistant direct child." \
		"kill -s 0 '$child_pid' 2>/dev/null"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
