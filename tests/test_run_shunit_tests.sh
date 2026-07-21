#!/bin/sh
#
# shunit2 tests for the shunit2 runner script.
#

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_run_shunit_tests"
	RUN_SHUNIT_TESTS_BIN="$ZXFER_ROOT/tests/run_shunit_tests.sh"
	RUN_SHUNIT_TESTS_ORIGINAL_PATH=${PATH:-/usr/bin:/bin}
	RUN_SHUNIT_TESTS_IDENTITY_BIN="$TEST_TMPDIR/process-identity-bin"
	RUN_SHUNIT_TESTS_IDENTITY_RELATIONS="$TEST_TMPDIR/process-identity-relations"
	ZXFER_TEST_REAL_PS=$(command -v ps 2>/dev/null || printf '%s\n' /bin/ps)
	ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT=${TMPDIR:-/tmp}
	ZXFER_TEST_PROCESS_IDENTITY_RELATION_ROOT=$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS
	mkdir -p "$RUN_SHUNIT_TESTS_IDENTITY_BIN" \
		"$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS"
	zxfer_test_write_process_identity_ps "$RUN_SHUNIT_TESTS_IDENTITY_BIN/ps"
	PATH="$RUN_SHUNIT_TESTS_IDENTITY_BIN:$RUN_SHUNIT_TESTS_ORIGINAL_PATH"
	export PATH ZXFER_TEST_REAL_PS ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT \
		ZXFER_TEST_PROCESS_IDENTITY_RELATION_ROOT
}

oneTimeTearDown() {
	PATH=$RUN_SHUNIT_TESTS_ORIGINAL_PATH
	export PATH
	unset ZXFER_TEST_REAL_PS ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT \
		ZXFER_TEST_PROCESS_IDENTITY_RELATION_ROOT
	zxfer_test_cleanup_tmpdir
}

setUp() {
	FAKE_SUITE_LOG="$TEST_TMPDIR/fake-suite.log"
	FAKE_TEST_SHELL_LOG="$TEST_TMPDIR/fake-test-shell.log"
	FAKE_SUITE_STARTED="$TEST_TMPDIR/fake-suite.started"
	FAKE_SUITE_RELEASE="$TEST_TMPDIR/fake-suite.release"
	RUN_SHUNIT_TESTS_WAIT_LIMIT=${ZXFER_RUN_SHUNIT_TESTS_WAIT_LIMIT:-15}
	RUN_SHUNIT_TESTS_WATCHDOG_SECONDS=${ZXFER_RUN_SHUNIT_TESTS_WATCHDOG_SECONDS:-20}
	: >"$FAKE_SUITE_LOG"
	: >"$FAKE_TEST_SHELL_LOG"
	rm -f "$FAKE_SUITE_STARTED" "$FAKE_SUITE_RELEASE"
	for l_relation_path in "$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS"/*; do
		[ -e "$l_relation_path" ] || continue
		rm -f "$l_relation_path"
	done
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
find_runner_ancestor_for_suite_pid() {
	l_pid=$1
	l_expected_runner_pid=${2:-}
	l_current_test_pid=$$
	l_runner_pid=
	l_cmd=
	l_runner_basename=$(basename "$RUN_SHUNIT_TESTS_BIN")

	while :; do
		case "$l_pid" in
		'' | *[!0-9]* | 0 | 1)
			break
			;;
		esac

		if [ "$l_pid" = "$l_current_test_pid" ]; then
			break
		fi

		l_cmd=$(process_command_for_pid "$l_pid")
		case "$l_cmd" in
		*"$RUN_SHUNIT_TESTS_BIN"* | *"$l_runner_basename"*)
			l_runner_pid=$l_pid
			;;
		esac

		if [ -n "$l_expected_runner_pid" ] && [ "$l_pid" = "$l_expected_runner_pid" ]; then
			printf '%s\n' "$l_runner_pid"
			return 0
		fi

		l_pid=$(process_parent_pid "$l_pid")
	done

	printf '%s\n' "$l_runner_pid"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_suite() {
	l_fake_suite_path=$1
	l_fake_suite_marker=$2
	cat >"$l_fake_suite_path" <<EOF
#!/bin/sh
printf '%s\n' "$l_fake_suite_marker" >>"\${FAKE_SUITE_LOG:?}"
exit 0
EOF
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_suite_with_body() {
	l_fake_suite_path=$1
	cat >"$l_fake_suite_path"
}

# Generate named fake functions at test runtime. Keeping the definitions out of
# this suite's literal source prevents shunit2 from discovering fixture-only
# names as tests of tests/test_run_shunit_tests.sh itself.
# shellcheck disable=SC2016,SC2317,SC2329  # Generates runtime-expanded fixture source; invoked indirectly by shunit2.
write_fake_named_suite() {
	l_fake_suite_path=$1
	l_fake_suite_marker=$2
	shift 2
	case "$l_fake_suite_marker" in
	'' | *[!A-Za-z0-9_-]*) fail "Invalid fake suite marker: $l_fake_suite_marker" ;;
	esac
	{
		printf '%s\n' '#!/bin/sh'
		for l_fake_test_name in "$@"; do
			case "$l_fake_test_name" in
			test*) ;;
			*) fail "Invalid fake test name: $l_fake_test_name" ;;
			esac
			case "$l_fake_test_name" in
			'' | [!A-Za-z_]* | *[!A-Za-z0-9_]*)
				fail "Invalid fake test name: $l_fake_test_name"
				;;
			esac
			printf '%s() {\n' "$l_fake_test_name"
			printf '\t:\n'
			printf '%s\n' '}'
		done
		printf "FAKE_NAMED_SUITE_MARKER='%s'\n" "$l_fake_suite_marker"
		printf '%s\n' 'printf "%s" "$FAKE_NAMED_SUITE_MARKER" >>"${FAKE_SUITE_LOG:?}"'
		printf '%s\n' 'printf ":%s" "$@" >>"${FAKE_SUITE_LOG:?}"'
		printf '%s\n' 'printf "\n" >>"${FAKE_SUITE_LOG:?}"'
	} >"$l_fake_suite_path"
	chmod +x "$l_fake_suite_path"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_test_shell() {
	l_fake_shell_path=$1
	cat >"$l_fake_shell_path" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >>"${FAKE_TEST_SHELL_LOG:?}"
exec /bin/sh "$@"
EOF
	chmod +x "$l_fake_shell_path"
}

# Coverage wrappers can obscure command lines and ancestry. The cleanup tests
# need the exact nested runner PID they intentionally signal, not a ps guess.
# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
write_fake_runner_pid_wrapper() {
	l_fake_runner_path=$1
	cat >"$l_fake_runner_path" <<'EOF'
#!/bin/sh
printf '%s\n' "$$" >"${FAKE_RUNNER_PID_FILE:?}" || exit 125
exec "${FAKE_RUNNER_TARGET:?}" "$@"
EOF
	chmod +x "$l_fake_runner_path"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
process_command_for_pid() {
	l_pid=$1
	l_cmd=

	if command -v pargs >/dev/null 2>&1; then
		l_cmd=$(pargs -l "$l_pid" 2>/dev/null | sed -n '1p')
	fi
	l_cmd=$(printf '%s\n' "$l_cmd" | sed -n '1p')
	if [ -z "$l_cmd" ]; then
		l_cmd=$(ps -o args= -p "$l_pid" 2>/dev/null | sed -n '1p')
	fi
	case "$(printf '%s\n' "$l_cmd" | tr -d '[:space:]')" in
	ARGS | COMMAND | CMD)
		l_cmd=
		;;
	esac
	if [ -z "$l_cmd" ]; then
		l_cmd=$(ps -o command= -p "$l_pid" 2>/dev/null | sed -n '1p')
	fi
	case "$(printf '%s\n' "$l_cmd" | tr -d '[:space:]')" in
	ARGS | COMMAND | CMD)
		l_cmd=
		;;
	esac

	printf '%s\n' "$l_cmd"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
process_parent_pid() {
	l_pid=$1
	l_ppid=

	l_ppid=$(ps -o ppid= -p "$l_pid" 2>/dev/null |
		awk '$1 ~ /^[0-9]+$/ { print $1; exit }')
	if [ -z "$l_ppid" ]; then
		l_ppid=$(ps -o ppid -p "$l_pid" 2>/dev/null |
			awk '$1 ~ /^[0-9]+$/ { value = $1 } END { if (value != "") print value }')
	fi

	printf '%s\n' "$l_ppid"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
process_state_for_pid() {
	l_pid=$1
	l_state=

	l_state=$(ps -o stat= -p "$l_pid" 2>/dev/null |
		awk '
			$1 == "STAT" || $1 == "STATE" { next }
			$1 != "" { print $1; exit }
		')
	if [ -z "$l_state" ]; then
		l_state=$(ps -o state= -p "$l_pid" 2>/dev/null |
			awk '
				$1 == "S" || $1 == "STAT" || $1 == "STATE" { next }
				$1 != "" { print $1; exit }
			')
	fi
	if [ -z "$l_state" ]; then
		l_state=$(ps -o s= -p "$l_pid" 2>/dev/null |
			awk '
				$1 == "S" || $1 == "STAT" || $1 == "STATE" { next }
				$1 != "" { print $1; exit }
			')
	fi

	printf '%s\n' "$l_state"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
signal_number_for_name() {
	case "$1" in
	0)
		printf '%s\n' 0
		;;
	HUP | hup)
		printf '%s\n' 1
		;;
	INT | int)
		printf '%s\n' 2
		;;
	KILL | kill)
		printf '%s\n' 9
		;;
	TERM | term)
		printf '%s\n' 15
		;;
	*)
		return 1
		;;
	esac
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
send_test_signal_to_pid() {
	l_test_signal_to_pid_signal=$1
	l_test_signal_to_pid_pid=$2
	l_test_signal_to_pid_number=

	case "$l_test_signal_to_pid_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	kill -s "$l_test_signal_to_pid_signal" "$l_test_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	kill "-$l_test_signal_to_pid_signal" "$l_test_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	l_test_signal_to_pid_number=$(signal_number_for_name "$l_test_signal_to_pid_signal" 2>/dev/null || true)
	if [ -n "$l_test_signal_to_pid_number" ]; then
		kill "-$l_test_signal_to_pid_number" "$l_test_signal_to_pid_pid" >/dev/null 2>&1 && return 0
	fi
	return 1
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
find_process_ancestor_by_marker() {
	l_pid=$1
	l_expected_marker=${2:-}

	while :; do
		case "$l_pid" in
		'' | *[!0-9]* | 0 | 1)
			return 1
			;;
		esac

		l_cmd=$(process_command_for_pid "$l_pid")
		if [ -n "$l_expected_marker" ] && [ -n "$l_cmd" ]; then
			case "$l_cmd" in
			*"$l_expected_marker"*)
				printf '%s\n' "$l_pid"
				return 0
				;;
			esac
		fi

		l_pid=$(process_parent_pid "$l_pid")
	done
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
fake_suite_process_running_p() {
	l_pid=$1
	l_expected_marker=${2:-}
	l_state=
	l_cmd=

	if ! send_test_signal_to_pid 0 "$l_pid"; then
		return 1
	fi

	if [ -n "$l_expected_marker" ]; then
		l_cmd=$(process_command_for_pid "$l_pid")
		case "$l_cmd" in
		*"$l_expected_marker"*) ;;
		*)
			return 1
			;;
		esac
	fi

	l_state=$(process_state_for_pid "$l_pid")
	case "$l_state" in
	Z* | z* | *zombie* | *defunct*)
		return 1
		;;
	esac

	return 0
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_find_runner_ancestor_does_not_select_outer_runner_when_expected_pid_is_missing() {
	result=$(
		l_current_pid=$$
		process_parent_pid() {
			if [ "$1" = "$l_current_pid" ]; then
				printf '%s\n' 300
				return 0
			fi
			if [ "$1" = 100 ]; then
				printf '%s\n' 200
			elif [ "$1" = 200 ]; then
				printf '%s\n' "$l_current_pid"
			elif [ "$1" = 300 ]; then
				printf '%s\n' 1
			else
				printf '%s\n' ""
			fi
		}
		process_command_for_pid() {
			if [ "$1" = 300 ]; then
				printf '%s\n' "$RUN_SHUNIT_TESTS_BIN --jobs 1 tests/test_run_shunit_tests.sh"
			else
				printf '%s\n' "unrelated-process"
			fi
		}

		find_runner_ancestor_for_suite_pid 100 999
	)

	assertEquals "When the expected nested runner PID is not in the process tree, the helper must not fall back to an outer shunit runner ancestor." \
		"" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_find_runner_ancestor_uses_nearest_nested_runner_before_current_test_shell() {
	result=$(
		l_current_pid=$$
		process_parent_pid() {
			if [ "$1" = "$l_current_pid" ]; then
				printf '%s\n' 300
				return 0
			fi
			if [ "$1" = 100 ]; then
				printf '%s\n' 200
			elif [ "$1" = 200 ]; then
				printf '%s\n' "$l_current_pid"
			elif [ "$1" = 300 ]; then
				printf '%s\n' 1
			else
				printf '%s\n' ""
			fi
		}
		process_command_for_pid() {
			if [ "$1" = 200 ]; then
				printf '%s\n' "$RUN_SHUNIT_TESTS_BIN --jobs 1 nested-suite.sh"
			elif [ "$1" = 300 ]; then
				printf '%s\n' "$RUN_SHUNIT_TESTS_BIN --jobs 1 tests/test_run_shunit_tests.sh"
			else
				printf '%s\n' "unrelated-process"
			fi
		}

		find_runner_ancestor_for_suite_pid 100 999
	)

	assertEquals "When shell wrappers hide the original background PID, the helper should still signal the nearest nested runner and stop before the outer runner." \
		200 "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_find_runner_ancestor_prefers_top_level_runner_before_expected_launcher() {
	result=$(
		l_current_pid=$$
		process_parent_pid() {
			if [ "$1" = "$l_current_pid" ]; then
				printf '%s\n' 400
				return 0
			fi
			if [ "$1" = 100 ]; then
				printf '%s\n' 200
			elif [ "$1" = 200 ]; then
				printf '%s\n' 250
			elif [ "$1" = 250 ]; then
				printf '%s\n' 300
			elif [ "$1" = 300 ]; then
				printf '%s\n' "$l_current_pid"
			elif [ "$1" = 400 ]; then
				printf '%s\n' 1
			else
				printf '%s\n' ""
			fi
		}
		process_command_for_pid() {
			if [ "$1" = 200 ]; then
				printf '%s\n' "/bin/sh $RUN_SHUNIT_TESTS_BIN --jobs 1 foreground-wrapper"
			elif [ "$1" = 250 ]; then
				printf '%s\n' "/bin/sh $RUN_SHUNIT_TESTS_BIN --jobs 1 nested-suite.sh"
			elif [ "$1" = 300 ]; then
				printf '%s\n' "bash launcher"
			else
				printf '%s\n' "unrelated-process"
			fi
		}

		find_runner_ancestor_for_suite_pid 100 300
	)

	assertEquals "When wrapper processes share the runner command line, the helper should signal the top-level nested runner below the launcher." \
		250 "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_process_command_for_pid_prefers_full_pargs_output() {
	result=$(
		command() {
			if [ "$1:$2" = "-v:pargs" ]; then
				printf '%s\n' "pargs"
				return 0
			fi
			"$@"
		}
		pargs() {
			if [ "$1:$2" = "-l:123" ]; then
				printf '%s\n' "/bin/sh /very/long/path/term-resistant-serial-suite.sh"
				return 0
			fi
			return 1
		}
		ps() {
			if [ "$1:$2:$3:$4" = "-o:args=:-p:123" ]; then
				printf '%s\n' "/bin/sh /very/long/path/term-resistant-s"
				return 0
			fi
			return 1
		}

		process_command_for_pid 123
	)

	assertEquals "OmniOS command matching should prefer pargs so truncated ps output does not hide live term-resistant suites." \
		"/bin/sh /very/long/path/term-resistant-serial-suite.sh" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_process_state_for_pid_ignores_omnios_headers_before_zombie_state() {
	result=$(
		ps() {
			if [ "$1:$2:$3:$4" = "-o:stat=:-p:123" ]; then
				printf '%s\n' "STAT"
				printf '%s\n' "Z"
				return 0
			fi
			return 1
		}

		process_state_for_pid 123
	)

	assertEquals "Process-state parsing should ignore OmniOS-style headers before checking for zombie processes." \
		"Z" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_process_state_for_pid_uses_state_column_when_stat_is_unavailable() {
	result=$(
		ps() {
			if [ "$1:$2:$3:$4" = "-o:stat=:-p:123" ]; then
				return 1
			elif [ "$1:$2:$3:$4" = "-o:state=:-p:123" ]; then
				printf '%s\n' "STATE"
				printf '%s\n' "Z"
				return 0
			fi
			return 1
		}

		process_state_for_pid 123
	)

	assertEquals "Process-state parsing should use the state column when stat is unavailable." \
		"Z" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_process_state_for_pid_falls_back_to_short_state_column() {
	result=$(
		ps() {
			if [ "$1:$2:$3:$4" = "-o:stat=:-p:123" ]; then
				return 1
			elif [ "$1:$2:$3:$4" = "-o:state=:-p:123" ]; then
				return 1
			elif [ "$1:$2:$3:$4" = "-o:s=:-p:123" ]; then
				printf '%s\n' "S"
				printf '%s\n' "Z"
				return 0
			fi
			return 1
		}

		process_state_for_pid 123
	)

	assertEquals "Process-state parsing should use the short state column when stat is unavailable." \
		"Z" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_send_test_signal_to_pid_uses_numeric_signal_fallback() {
	result=$(
		kill() {
			if [ "$1:$2" = "-s:KILL" ] || [ "$1:$2" = "-KILL:123" ]; then
				return 1
			elif [ "$1:$2" = "-9:123" ]; then
				return 0
			fi
			return 1
		}

		if send_test_signal_to_pid KILL 123; then
			printf '%s\n' "ok"
		else
			printf '%s\n' "failed"
		fi
	)

	assertEquals "Signal helpers should fall back to numeric KILL for shell kill variants that reject symbolic forms." \
		"ok" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_refuses_to_signal_a_reused_root_pid() {
	l_matcher_log="$TEST_TMPDIR/reused-root-token-matcher.called"
	rm -f "$l_matcher_log"
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			TMPDIR="${TMPDIR:-/tmp}" \
			RUN_SHUNIT_TESTS_BIN="$RUN_SHUNIT_TESTS_BIN" \
			TOKEN_MATCHER_LOG="$l_matcher_log" \
			ZXFER_RUN_SHUNIT_SOURCE_ONLY=1 \
			/bin/sh -s <<'EOF'
. "$RUN_SHUNIT_TESTS_BIN"
runner_get_process_start_token_for_selector() {
	: >"$TOKEN_MATCHER_LOG"
	printf '%s:%s\n' "$2" 'new-process'
}
snapshot_process_descendants() {
	printf 'snapshot=%s\n' "$1"
}
send_signal_to_pid() {
	printf 'signal=%s pid=%s\n' "$1" "$2"
}
signal_pid_and_descendants TERM 43210 'lstart:original-process'
signal_tracked_runner_process TERM 43210 'lstart:original-process'
EOF
	)

	assertEquals "A changed root process-start token must prevent descendant discovery and signalling of a reused PID." \
		"" "$output"
	l_matcher_called=1
	if [ -f "$l_matcher_log" ]; then
		l_matcher_called=0
	fi
	assertEquals "The reused-PID regression should exercise the selector-specific token matcher." \
		0 "$l_matcher_called"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_refuses_a_reused_tokenless_owner_chain() {
	l_chain_log="$TEST_TMPDIR/reused-tokenless-owner-chain.log"
	rm -f "$l_chain_log"
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			OWNER_CHAIN_LOG="$l_chain_log" \
			RUN_SHUNIT_TESTS_BIN="$RUN_SHUNIT_TESTS_BIN" \
			ZXFER_RUN_SHUNIT_SOURCE_ONLY=1 \
			/bin/sh -s <<'EOF'
. "$RUN_SHUNIT_TESTS_BIN"
runner_child_pid_matches_parent() {
	printf '%s:%s\n' "$1" "$2" >>"$OWNER_CHAIN_LOG"
	if [ "$1:$2" = "200:300" ]; then
		return 0
	fi
	return 1
}
snapshot_owned_process_descendant_chains() {
	printf '%s\n' snapshot
}
send_signal_to_pid() {
	printf 'signal=%s pid=%s\n' "$1" "$2"
}
signal_owned_child_and_descendants TERM 100 200 300
EOF
	)

	assertEquals "A reused tokenless owner must prevent descendant discovery and signalling even when the lower owner-child pair matches." \
		"" "$output"
	assertEquals "The tokenless fallback should validate the top runner-to-owner link before trusting the lower pair." \
		"100:200" "$(cat "$l_chain_log")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_a_malformed_ownership_chain() {
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			RUN_SHUNIT_TESTS_BIN="$RUN_SHUNIT_TESTS_BIN" \
			ZXFER_RUN_SHUNIT_SOURCE_ONLY=1 \
			/bin/sh -s <<'EOF'
. "$RUN_SHUNIT_TESTS_BIN"
runner_child_pid_matches_parent() {
	return 0
}
if runner_owned_process_chain_matches '1:2:'; then
	printf '%s\n' accepted
fi
EOF
	)

	assertEquals "Ownership chains with an empty trailing component must fail closed." \
		"" "$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_reports_unavailable_ownership_enumeration() {
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			RUN_SHUNIT_TESTS_BIN="$RUN_SHUNIT_TESTS_BIN" \
			ZXFER_RUN_SHUNIT_SOURCE_ONLY=1 \
			/bin/sh -s 2>&1 <<'EOF'
. "$RUN_SHUNIT_TESTS_BIN"
list_child_pids_for_parent() {
	return 1
}
sleep() {
	:
}
send_signal_to_pid() {
	printf 'signal=%s pid=%s\n' "$1" "$2"
}
signal_owned_child_and_descendants TERM 100 200 300
EOF
	)

	assertEquals "Unavailable relationship enumeration should fail closed with an actionable diagnostic and no signal." \
		"Unable to verify shunit suite descendants; waiting for the owning wrapper to reap them." \
		"$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_keeps_the_suite_when_a_proven_descendant_rejects_kill() {
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			RUN_SHUNIT_TESTS_BIN="$RUN_SHUNIT_TESTS_BIN" \
			ZXFER_RUN_SHUNIT_SOURCE_ONLY=1 \
			/bin/sh -s 2>&1 <<'EOF'
. "$RUN_SHUNIT_TESTS_BIN"
snapshot_owned_process_descendant_chains() {
	printf '%s\n' '100:200:300:400'
}
runner_owned_process_chain_matches() {
	return 0
}
send_signal_to_pid() {
	if [ "$2" = "400" ]; then
		return 1
	fi
	printf 'signal=%s pid=%s\n' "$1" "$2"
}
signal_owned_child_and_descendants TERM 100 200 300
EOF
	)

	assertEquals "A still-attached descendant that rejects KILL must keep the suite owner alive." \
		"Unable to revalidate shunit suite descendants; waiting for the owning wrapper to reap them." \
		"$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_matches_the_recorded_process_token_selector() {
	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			RUN_SHUNIT_TESTS_BIN="$RUN_SHUNIT_TESTS_BIN" \
			ZXFER_RUN_SHUNIT_SOURCE_ONLY=1 \
			/bin/sh -s <<'EOF'
. "$RUN_SHUNIT_TESTS_BIN"
runner_get_process_start_token_for_selector() {
	printf '%s:%s\n' "$2" stable
}
if runner_process_identity_matches 43210 'stime:stable'; then
	printf '%s\n' matched
fi
EOF
	)

	assertEquals "Identity checks should reuse the selector embedded in the captured token instead of switching back to lstart." \
		"matched" "$output"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_fake_suite_process_running_requires_expected_marker() {
	result=$(
		send_test_signal_to_pid() {
			return 0
		}
		process_command_for_pid() {
			printf '%s\n' ""
		}
		process_state_for_pid() {
			printf '%s\n' ""
		}

		if fake_suite_process_running_p 123 "term-resistant-suite.sh"; then
			printf '%s\n' "running"
		else
			printf '%s\n' "gone"
		fi
	)

	assertEquals "A stale or reused PID without the fake-suite marker should not be reported as the term-resistant suite." \
		"gone" "$result"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_runs_explicit_suite_directly_by_default() {
	l_suite_path="$TEST_TMPDIR/default-suite.sh"
	write_fake_suite "$l_suite_path" "direct-default"
	chmod +x "$l_suite_path"

	output=$(
		env -i \
			PATH="${PATH:-/usr/bin:/bin}" \
			TMPDIR="${TMPDIR:-/tmp}" \
			FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" "$l_suite_path"
	)

	assertContains "The runner should execute explicit suites successfully with the default direct-exec path." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertContains "The default banner should not mention an alternate shell when ZXFER_TEST_SHELL is unset." \
		"$output" "==> Running shunit2 suite: $l_suite_path"
	assertEquals "The fake suite should run once through its own shebang when no alternate shell is configured." \
		"direct-default" "$(cat "$FAKE_SUITE_LOG")"
	assertEquals "The fake alternate-shell log should remain empty during the default dispatch path." \
		"" "$(cat "$FAKE_TEST_SHELL_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_lists_suites_and_named_tests_without_running_them() {
	l_suite_path="$TEST_TMPDIR/listable-suite.sh"
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' 'test_first_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
		printf '%s\n' 'helper_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
		printf '%s\n' 'test_second_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
	} >"$l_suite_path"
	chmod +x "$l_suite_path"

	suite_output=$("$RUN_SHUNIT_TESTS_BIN" --list-suites "$l_suite_path")
	compat_suite_output=$("$RUN_SHUNIT_TESTS_BIN" --list "$l_suite_path")
	test_output=$("$RUN_SHUNIT_TESTS_BIN" --list-tests "$l_suite_path")

	assertEquals "Suite listing should print the resolved explicit suite once." \
		"$l_suite_path" "$suite_output"
	assertEquals "The original --list spelling should remain a compatibility alias for --list-suites." \
		"$suite_output" "$compat_suite_output"
	assertContains "Named-test listing should identify the first test and its suite." \
		"$test_output" "$l_suite_path	test_first_case"
	assertContains "Named-test listing should identify every test function." \
		"$test_output" "$l_suite_path	test_second_case"
	assertNotContains "Named-test listing should exclude non-test helpers." \
		"$test_output" "helper_case"
	assertEquals "Listing should not execute the selected suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_lists_named_tests_from_declared_behavior_fragments() {
	l_suite_path="$TEST_TMPDIR/fragmented-suite.sh"
	l_fragment_path="$TEST_TMPDIR/fragmented-suite-cases.sh"
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' '# zxfer-test-fragment: fragmented-suite-cases.sh'
		printf '%s\n' 'test_main_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
	} >"$l_suite_path"
	{
		printf '%s\n' '#!/bin/sh'
		printf '%s\n' 'test_fragment_case() {'
		printf '\t:\n'
		printf '%s\n' '}'
		printf '%s\n' 'fragment_helper() {'
		printf '\t:\n'
		printf '%s\n' '}'
	} >"$l_fragment_path"
	chmod +x "$l_suite_path"

	test_output=$("$RUN_SHUNIT_TESTS_BIN" --list-tests "$l_suite_path")

	assertContains "Fragment-aware listing should preserve tests defined in the stable suite entry point." \
		"$test_output" "$l_suite_path	test_main_case"
	assertContains "Fragment-aware listing should include tests from every declared behavior fragment." \
		"$test_output" "$l_suite_path	test_fragment_case"
	assertNotContains "Fragment-aware listing should continue to exclude non-test helpers." \
		"$test_output" "fragment_helper"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_list_tests_requires_one_explicit_suite() {
	l_first_suite="$TEST_TMPDIR/list-first-suite.sh"
	l_second_suite="$TEST_TMPDIR/list-second-suite.sh"
	write_fake_suite "$l_first_suite" "list-first"
	write_fake_suite "$l_second_suite" "list-second"
	chmod +x "$l_first_suite" "$l_second_suite"

	zxfer_test_capture_subshell "
		FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \\
		\"$RUN_SHUNIT_TESTS_BIN\" --list-tests
	"

	assertEquals "Named-test listing should reject an omitted suite instead of expanding to the full suite set." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The missing-suite error should explain the single-suite contract." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--list-tests requires exactly one explicit suite; found 0."

	zxfer_test_capture_subshell "
		FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \\
		\"$RUN_SHUNIT_TESTS_BIN\" --list-tests \\
		\"$l_first_suite\" \"$l_second_suite\"
	"

	assertEquals "Named-test listing should reject ambiguous multi-suite selection." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The multi-suite error should report how many suites were supplied." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--list-tests requires exactly one explicit suite; found 2."
	assertEquals "Rejected named-test listing must not execute either suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_forwards_named_tests_to_one_suite() {
	l_suite_path="$TEST_TMPDIR/named-suite.sh"
	write_fake_named_suite "$l_suite_path" named \
		test_first_case test_second_case

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" \
			--suite "$l_suite_path" \
			--test test_first_case \
			--test test_second_case
	)

	assertContains "Named test execution should preserve the passing suite summary." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "The runner should use shunit2's documented -- separator before forwarding selected test names." \
		"named:--:test_first_case:test_second_case" \
		"$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_preserves_positional_suite_named_test_selection() {
	l_suite_path="$TEST_TMPDIR/named-positional-suite.sh"
	write_fake_named_suite "$l_suite_path" positional \
		test_positional_one test_positional_two

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 \
			--test test_positional_one \
			--test test_positional_two \
			"$l_suite_path"
	)

	assertContains "The legacy positional-suite form should retain named-test selection." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Positional named-test selection should preserve requested order and shunit2's separator." \
		"positional:--:test_positional_one:test_positional_two" \
		"$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_runs_cross_suite_named_selections_in_first_suite_order() {
	l_first_suite="$TEST_TMPDIR/named-cross-first.sh"
	l_second_suite="$TEST_TMPDIR/named-cross-second.sh"
	write_fake_named_suite "$l_first_suite" first \
		test_first_one test_first_two
	write_fake_named_suite "$l_second_suite" second test_second_one

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 \
			--suite "$l_first_suite" \
			--test test_first_one \
			--test test_first_two \
			--suite "$l_second_suite" \
			--test test_second_one
	)

	assertContains "Cross-suite named selection should report both suites as passing." \
		"$output" "==> shunit2 summary: 2 passed, 0 failed"
	assertEquals "Each suite should run once in first-selection order with only its associated names." \
		"first:--:test_first_one:test_first_two
second:--:test_second_one" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_merges_duplicate_suite_selectors_without_reordering_tests() {
	l_suite_path="$TEST_TMPDIR/named-duplicate-suite.sh"
	write_fake_named_suite "$l_suite_path" duplicate \
		test_duplicate_one test_duplicate_two

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 \
			--suite "$l_suite_path" --test test_duplicate_one \
			--suite "$l_suite_path" --test test_duplicate_two
	)

	assertContains "A repeated suite selector should still count as one suite execution." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Duplicate suite selectors should merge ordered names into one invocation." \
		"duplicate:--:test_duplicate_one:test_duplicate_two" \
		"$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_unknown_named_test_before_starting_any_suite() {
	l_first_suite="$TEST_TMPDIR/named-known-suite.sh"
	l_second_suite="$TEST_TMPDIR/named-unknown-suite.sh"
	write_fake_named_suite "$l_first_suite" first test_known_first
	write_fake_named_suite "$l_second_suite" second test_known_second

	set +e
	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 \
			--suite "$l_first_suite" --test test_known_first \
			--suite "$l_second_suite" --test test_missing 2>&1
	)
	status=$?
	set -e

	assertEquals "An unknown selected name should fail preflight." 1 "$status"
	assertContains "The preflight failure should identify the exact suite and unknown test." \
		"$output" "Unknown test for $l_second_suite: test_missing"
	assertEquals "Named-test validation must finish before launching even an earlier valid suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_test_option_before_its_suite() {
	l_suite_path="$TEST_TMPDIR/named-orphan-suite.sh"
	write_fake_named_suite "$l_suite_path" orphan test_orphan

	set +e
	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" \
			--test test_orphan --suite "$l_suite_path" 2>&1
	)
	status=$?
	set -e

	assertEquals "A --test without a current suite should fail closed in option-selector mode." \
		1 "$status"
	assertContains "The orphan-test diagnostic should require ordering the suite first." \
		"$output" "--test must follow the --suite it selects."
	assertEquals "An orphan named-test option must not launch its later suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_named_tests_without_one_suite() {
	l_first_suite="$TEST_TMPDIR/named-first-suite.sh"
	l_second_suite="$TEST_TMPDIR/named-second-suite.sh"
	write_fake_suite "$l_first_suite" "named-first"
	write_fake_suite "$l_second_suite" "named-second"
	chmod +x "$l_first_suite" "$l_second_suite"

	zxfer_test_capture_subshell "
		FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \
		\"$RUN_SHUNIT_TESTS_BIN\" --test test_case \
		\"$l_first_suite\" \"$l_second_suite\"
	"

	assertEquals "Named test selection should fail before launching an ambiguous multi-suite run." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The validation error should explain the one-suite requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--test requires exactly one runnable suite; found 2."
	assertEquals "Rejected named selection should not execute either suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_non_positive_and_nonnumeric_jobs() {
	l_suite_path="$TEST_TMPDIR/jobs-suite.sh"
	write_fake_suite "$l_suite_path" "jobs"
	chmod +x "$l_suite_path"

	zxfer_test_capture_subshell "
		\"$RUN_SHUNIT_TESTS_BIN\" --jobs 0 \"$l_suite_path\"
	"

	assertEquals "A zero job count should fail before any suites are launched." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A zero job count should report the positive-integer requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--jobs must be a positive integer"

	zxfer_test_capture_subshell "
		\"$RUN_SHUNIT_TESTS_BIN\" --jobs nope \"$l_suite_path\"
	"

	assertEquals "A nonnumeric job count should fail before any suites are launched." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "A nonnumeric job count should report the positive-integer requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "--jobs must be a positive integer"
	assertEquals "Rejected job-count parsing should not run the fake suite." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_limits_explicit_jobs_to_runnable_suite_count() {
	l_suite_path="$TEST_TMPDIR/clamped-jobs-suite.sh"
	write_fake_suite "$l_suite_path" "clamped"
	chmod +x "$l_suite_path"

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 4 "$l_suite_path"
	)

	assertContains "The runner should announce when an explicit job count exceeds the number of runnable suites." \
		"$output" "==> Requested 4 shunit2 jobs, but only 1 runnable suite is available; limiting to 1."
	assertContains "Clamped job counts should still run the suite successfully." \
		"$output" "==> shunit2 summary: 1 passed, 0 failed"
	assertEquals "Clamped job counts should still execute the requested suite exactly once." \
		"clamped" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_replays_parallel_output_in_suite_order() {
	l_slow_suite="$TEST_TMPDIR/slow-suite.sh"
	l_fast_suite="$TEST_TMPDIR/fast-suite.sh"

	write_fake_suite_with_body "$l_slow_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "slow-start"
	printf '%s\n' "slow-start" >>"${FAKE_SUITE_LOG:?}"
	: >"${FAKE_SUITE_STARTED:?}"
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_RELEASE:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_RELEASE:?}" ]; then
		printf '%s\n' "slow-timeout" >&2
		exit 9
	fi
	printf '%s\n' "slow-end"
	printf '%s\n' "slow-end" >>"${FAKE_SUITE_LOG:?}"
EOF
	chmod +x "$l_slow_suite"

	write_fake_suite_with_body "$l_fast_suite" <<'EOF'
#!/bin/sh
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_STARTED:?}" ] && [ "$l_wait_count" -lt 10 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_STARTED:?}" ]; then
		printf '%s\n' "fast-missed-slow" >&2
		exit 7
	fi
	: >"${FAKE_SUITE_RELEASE:?}"
	printf '%s\n' "fast-sees-slow"
	printf '%s\n' "fast-sees-slow" >>"${FAKE_SUITE_LOG:?}"
EOF
	chmod +x "$l_fast_suite"

	output=$(
		FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			FAKE_SUITE_STARTED="$FAKE_SUITE_STARTED" \
			FAKE_SUITE_RELEASE="$FAKE_SUITE_RELEASE" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_slow_suite" "$l_fast_suite"
	)

	assertContains "Parallel suite runs should still report a passing summary when every suite succeeds." \
		"$output" "==> shunit2 summary: 2 passed, 0 failed"
	case "$output" in
	*"==> Running shunit2 suite: $l_slow_suite"**"slow-start"**"slow-end"**"==> Running shunit2 suite: $l_fast_suite"**"fast-sees-slow"*)
		l_ordered_output=0
		;;
	*)
		l_ordered_output=1
		;;
	esac
	assertEquals "Parallel suite output should still be replayed in suite order instead of interleaving later suites ahead of earlier ones." \
		0 "$l_ordered_output"
	assertContains "The fast suite should observe that the slow suite had already started, proving the background queue launched both suites concurrently." \
		"$(cat "$FAKE_SUITE_LOG")" "fast-sees-slow"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_reuses_parallel_slots_when_newer_suite_finishes_first() {
	l_first_suite="$TEST_TMPDIR/slot-reuse-one.sh"
	l_second_suite="$TEST_TMPDIR/slot-reuse-two.sh"
	l_third_suite="$TEST_TMPDIR/slot-reuse-three.sh"
	l_first_started="$TEST_TMPDIR/slot-reuse.first-started"
	l_third_started="$TEST_TMPDIR/slot-reuse.third-started"
	rm -f "$l_first_started" "$l_third_started"

	write_fake_suite_with_body "$l_first_suite" <<'EOF'
#!/bin/sh
	: >"${FAKE_SUITE_STARTED:?}"
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_RELEASE:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_RELEASE:?}" ]; then
		printf '%s\n' "slot-reuse-timeout" >&2
		exit 9
	fi
	printf '%s\n' "slot-one-done"
EOF
	chmod +x "$l_first_suite"

	write_fake_suite_with_body "$l_second_suite" <<'EOF'
#!/bin/sh
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_STARTED:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_STARTED:?}" ]; then
		printf '%s\n' "slot-reuse-missed-first" >&2
		exit 7
	fi
	printf '%s\n' "slot-two-done"
EOF
	chmod +x "$l_second_suite"

	write_fake_suite_with_body "$l_third_suite" <<'EOF'
#!/bin/sh
	: >"${FAKE_SUITE_RELEASE:?}"
	printf '%s\n' "slot-three-done"
EOF
	chmod +x "$l_third_suite"

	output=$(
		FAKE_SUITE_STARTED="$l_first_started" \
			FAKE_SUITE_RELEASE="$l_third_started" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_first_suite" "$l_second_suite" "$l_third_suite"
	)

	assertContains "Parallel execution should reuse a freed worker slot even when the oldest suite is still running." \
		"$output" "==> shunit2 summary: 3 passed, 0 failed"
	assertContains "A later suite should start before the oldest suite finishes once another worker frees a slot." \
		"$output" "slot-three-done"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_streams_serial_output_when_jobs_is_one() {
	l_suite_path="$TEST_TMPDIR/serial-stream-suite.sh"
	l_output_path="$TEST_TMPDIR/serial-stream.output"
	l_status_path="$TEST_TMPDIR/serial-stream.status"
	l_restore_errexit=0

	write_fake_suite_with_body "$l_suite_path" <<'EOF'
#!/bin/sh
	printf '%s\n' "serial-start"
	: >"${FAKE_SUITE_STARTED:?}"
	l_wait_count=0
	while [ ! -f "${FAKE_SUITE_RELEASE:?}" ] && [ "$l_wait_count" -lt 5 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ ! -f "${FAKE_SUITE_RELEASE:?}" ]; then
		printf '%s\n' "serial-timeout" >&2
		exit 9
	fi
	printf '%s\n' "serial-end"
EOF
	chmod +x "$l_suite_path"

	(
		FAKE_SUITE_STARTED="$FAKE_SUITE_STARTED" \
			FAKE_SUITE_RELEASE="$FAKE_SUITE_RELEASE" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 1 "$l_suite_path" >"$l_output_path" 2>&1
		printf '%s\n' "$?" >"$l_status_path"
	) &
	l_runner_pid=$!

	l_saw_live_output=1
	l_wait_count=0
	while [ "$l_wait_count" -lt "$RUN_SHUNIT_TESTS_WAIT_LIMIT" ]; do
		if [ -f "$FAKE_SUITE_STARTED" ]; then
			case "$(cat "$l_output_path" 2>/dev/null || true)" in
			*"serial-start"*)
				l_saw_live_output=0
				break
				;;
			esac
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	assertEquals "Serial execution should stream suite output before the suite finishes when --jobs 1 is selected." \
		0 "$l_saw_live_output"

	: >"$FAKE_SUITE_RELEASE"
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac
	set +e
	wait "$l_runner_pid" >/dev/null 2>&1
	[ "$l_restore_errexit" = "1" ] && set -e

	assertEquals "Serial execution should still complete successfully after streaming live output." \
		0 "$(cat "$l_status_path")"
	assertContains "Serial execution should preserve the suite summary after streaming live output." \
		"$(cat "$l_output_path")" "==> shunit2 summary: 1 passed, 0 failed"
	assertContains "Serial execution should still include the suite's trailing output." \
		"$(cat "$l_output_path")" "serial-end"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_signal_cleanup_kills_term_resistant_serial_suite() {
	l_suite_path="$TEST_TMPDIR/term-resistant-serial-suite.sh"
	l_runner_wrapper_path="$TEST_TMPDIR/term-resistant-serial-runner-wrapper.sh"
	l_output_path="$TEST_TMPDIR/term-resistant-serial.output"
	l_started_path="$TEST_TMPDIR/term-resistant-serial.started"
	l_suite_pid_path="$TEST_TMPDIR/term-resistant-serial.pid"
	l_intermediate_owner_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/serial-intermediate.owner.pid"
	l_intermediate_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/serial-intermediate.pid"
	l_grandchild_owner_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/serial-grandchild.owner.pid"
	l_grandchild_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/serial-grandchild.pid"
	l_grandchild_term_path="$TEST_TMPDIR/term-resistant-serial-grandchild.term"
	l_runner_pid_path="$TEST_TMPDIR/term-resistant-serial-runner.pid"
	rm -f "$l_output_path" "$l_started_path" "$l_suite_pid_path" \
		"$l_intermediate_owner_pid_path" "$l_intermediate_pid_path" \
		"$l_grandchild_owner_pid_path" "$l_grandchild_pid_path" \
		"$l_grandchild_term_path" "$l_runner_pid_path"
	write_fake_runner_pid_wrapper "$l_runner_wrapper_path"

	write_fake_suite_with_body "$l_suite_path" <<'EOF'
#!/bin/sh
	(
		trap 'exit 0' TERM
		/bin/sh -c '
			trap ": >\"${FAKE_GRANDCHILD_TERM_FILE:?}\"" TERM
			printf "%s\n" "$$" >"${FAKE_GRANDCHILD_PID_FILE:?}"
			while :; do sleep 1; done
		' term-resistant-grandchild-marker &
		wait
	) &
	l_intermediate_pid=$!
	printf '%s\n' "$$" >"${FAKE_INTERMEDIATE_OWNER_PID_FILE:?}"
	printf '%s\n' "$l_intermediate_pid" >"${FAKE_INTERMEDIATE_PID_FILE:?}"
	printf '%s\n' "$l_intermediate_pid" >"${FAKE_GRANDCHILD_OWNER_PID_FILE:?}"
	trap '' TERM
	printf '%s\n' "$$" >"${FAKE_SUITE_LOG:?}"
	l_wait_count=0
	while [ ! -s "${FAKE_GRANDCHILD_PID_FILE:?}" ] && [ "$l_wait_count" -lt 10 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	[ -s "${FAKE_GRANDCHILD_PID_FILE:?}" ] || exit 9
	: >"${FAKE_SUITE_STARTED:?}"
	while :; do
		sleep 1
	done
EOF
	chmod +x "$l_suite_path"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	FAKE_SUITE_LOG="$l_suite_pid_path" \
		FAKE_SUITE_STARTED="$l_started_path" \
		FAKE_INTERMEDIATE_OWNER_PID_FILE="$l_intermediate_owner_pid_path" \
		FAKE_INTERMEDIATE_PID_FILE="$l_intermediate_pid_path" \
		FAKE_GRANDCHILD_OWNER_PID_FILE="$l_grandchild_owner_pid_path" \
		FAKE_GRANDCHILD_PID_FILE="$l_grandchild_pid_path" \
		FAKE_GRANDCHILD_TERM_FILE="$l_grandchild_term_path" \
		FAKE_RUNNER_PID_FILE="$l_runner_pid_path" \
		FAKE_RUNNER_TARGET="$RUN_SHUNIT_TESTS_BIN" \
		"$l_runner_wrapper_path" --jobs 1 "$l_suite_path" >"$l_output_path" 2>&1 &
	l_runner_pid=$!
	[ "$l_restore_errexit" = "1" ] && set -e

	l_started=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if [ -f "$l_started_path" ]; then
			l_started=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	if [ "$l_started" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_runner_pid" || :
		wait "$l_runner_pid" >/dev/null 2>&1 || :
		fail "The term-resistant suite never started under the serial runner."
		return 0
	fi

	l_suite_pid=$(cat "$l_suite_pid_path")
	l_grandchild_pid=$(cat "$l_grandchild_pid_path")
	l_runner_signal_pid=$(cat "$l_runner_pid_path" 2>/dev/null || true)
	case "$l_runner_signal_pid" in
	'' | *[!0-9]*)
		l_runner_signal_pid=$(find_runner_ancestor_for_suite_pid "$l_suite_pid" "$l_runner_pid")
		case "$l_runner_signal_pid" in
		'' | *[!0-9]*)
			send_test_signal_to_pid KILL "$l_runner_pid" || :
			wait "$l_runner_pid" >/dev/null 2>&1 || :
			send_test_signal_to_pid KILL "$l_suite_pid" || :
			fail "The serial cleanup test could not identify the nested shunit runner to signal."
			return 0
			;;
		esac
		;;
	esac
	send_test_signal_to_pid TERM "$l_runner_signal_pid" || :
	(
		sleep "$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS"
		send_test_signal_to_pid KILL "$l_runner_signal_pid" || :
		case "$l_runner_pid:$l_runner_signal_pid" in
		"$l_runner_signal_pid:$l_runner_signal_pid") ;;
		*)
			send_test_signal_to_pid KILL "$l_runner_pid" || :
			;;
		esac
	) &
	l_watchdog_pid=$!

	set +e
	wait "$l_runner_pid"
	l_runner_status=$?
	[ "$l_restore_errexit" = "1" ] && set -e
	kill "$l_watchdog_pid" >/dev/null 2>&1 || :
	wait "$l_watchdog_pid" >/dev/null 2>&1 || :

	l_suite_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_suite_pid"; then
			l_suite_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	l_runner_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_runner_signal_pid"; then
			l_runner_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	if [ "$l_suite_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_suite_pid" || :
	fi
	l_grandchild_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_grandchild_pid"; then
			l_grandchild_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_grandchild_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_grandchild_pid" || :
	fi
	if [ "$l_runner_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_runner_signal_pid" || :
		case "$l_runner_pid:$l_runner_signal_pid" in
		"$l_runner_signal_pid:$l_runner_signal_pid") ;;
		*)
			send_test_signal_to_pid KILL "$l_runner_pid" || :
			;;
		esac
	fi

	assertEquals "Serial signal cleanup should terminate the runner with the signal-derived exit status even when the suite ignores TERM." \
		143 "$l_runner_status"
	assertEquals "Serial signal cleanup should not leave a TERM-resistant suite running after the runner exits." \
		0 "$l_suite_gone"
	assertEquals "Serial signal cleanup should retain and KILL a TERM-resistant grandchild after its intermediate parent exits on TERM." \
		0 "$l_grandchild_gone"
	l_grandchild_signalled=1
	if [ -f "$l_grandchild_term_path" ]; then
		l_grandchild_signalled=0
	fi
	assertEquals "Serial cleanup should discover and signal the recorded grandchild before escalation." \
		0 "$l_grandchild_signalled"
	assertEquals "Serial signal cleanup should not leave the nested shunit runner alive after the caller regains control." \
		0 "$l_runner_gone"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_signal_cleanup_kills_term_resistant_workers() {
	l_suite_path="$TEST_TMPDIR/term-resistant-suite.sh"
	l_second_suite_path="$TEST_TMPDIR/term-resistant-support-suite.sh"
	l_runner_wrapper_path="$TEST_TMPDIR/term-resistant-runner-wrapper.sh"
	l_output_path="$TEST_TMPDIR/term-resistant.output"
	l_started_path="$TEST_TMPDIR/term-resistant.started"
	l_suite_pid_path="$TEST_TMPDIR/term-resistant.pid"
	l_runner_pid_path="$TEST_TMPDIR/term-resistant-runner.pid"
	l_runner_tmpdir="$TEST_TMPDIR/term-resistant-state"
	rm -f "$l_output_path" "$l_started_path" "$l_suite_pid_path" "$l_runner_pid_path"
	mkdir -p "$l_runner_tmpdir"
	write_fake_runner_pid_wrapper "$l_runner_wrapper_path"

	write_fake_suite_with_body "$l_suite_path" <<'EOF'
#!/bin/sh
	trap '' TERM
	printf '%s\n' "$$" >"${FAKE_SUITE_LOG:?}"
	: >"${FAKE_SUITE_STARTED:?}"
	while :; do
		sleep 1
	done
EOF
	chmod +x "$l_suite_path"

	write_fake_suite_with_body "$l_second_suite_path" <<'EOF'
#!/bin/sh
	printf '%s\n' "support-suite-done"
EOF
	chmod +x "$l_second_suite_path"

	l_restore_errexit=0
	case $- in
	*e*)
		l_restore_errexit=1
		;;
	esac

	set +e
	FAKE_SUITE_LOG="$l_suite_pid_path" \
		FAKE_SUITE_STARTED="$l_started_path" \
		FAKE_RUNNER_PID_FILE="$l_runner_pid_path" \
		FAKE_RUNNER_TARGET="$RUN_SHUNIT_TESTS_BIN" \
		TMPDIR="$l_runner_tmpdir" \
		ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT="$l_runner_tmpdir" \
		"$l_runner_wrapper_path" --jobs 2 "$l_suite_path" "$l_second_suite_path" >"$l_output_path" 2>&1 &
	l_runner_pid=$!
	[ "$l_restore_errexit" = "1" ] && set -e

	l_started=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if [ -f "$l_started_path" ]; then
			l_started=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	if [ "$l_started" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_runner_pid" || :
		wait "$l_runner_pid" >/dev/null 2>&1 || :
		fail "The term-resistant suite never started under the parallel runner."
		return 0
	fi

	l_suite_pid=$(cat "$l_suite_pid_path")
	l_identity_published=1
	l_wait_count=0
	while [ "$l_wait_count" -lt "$RUN_SHUNIT_TESTS_WAIT_LIMIT" ]; do
		for l_child_pid_file in \
			"$l_runner_tmpdir"/zxfer_shunit.*/*.child.pid; do
			[ -r "$l_child_pid_file" ] || continue
			l_recorded_child_pid=$(cat "$l_child_pid_file" 2>/dev/null || true)
			l_child_token_file=${l_child_pid_file%.pid}.token
			if [ "$l_recorded_child_pid" = "$l_suite_pid" ] &&
				[ -s "$l_child_token_file" ]; then
				l_identity_published=0
				break
			fi
		done
		[ "$l_identity_published" -eq 0 ] && break
		if ! send_test_signal_to_pid 0 "$l_runner_pid"; then
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_identity_published" -ne 0 ]; then
		send_test_signal_to_pid TERM "$l_runner_pid" || :
		(
			sleep "$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS"
			send_test_signal_to_pid KILL "$l_suite_pid" || :
			send_test_signal_to_pid KILL "$l_runner_pid" || :
		) &
		l_readiness_watchdog_pid=$!
		wait "$l_runner_pid" >/dev/null 2>&1 || :
		kill "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
		wait "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
		fail "The parallel cleanup test did not observe published child identity state."
		return 0
	fi
	l_runner_signal_pid=$(cat "$l_runner_pid_path" 2>/dev/null || true)
	case "$l_runner_signal_pid" in
	'' | *[!0-9]*)
		l_runner_signal_pid=$(find_runner_ancestor_for_suite_pid "$l_suite_pid" "$l_runner_pid")
		case "$l_runner_signal_pid" in
		'' | *[!0-9]*)
			send_test_signal_to_pid KILL "$l_runner_pid" || :
			wait "$l_runner_pid" >/dev/null 2>&1 || :
			send_test_signal_to_pid KILL "$l_suite_pid" || :
			fail "The parallel cleanup test could not identify the nested shunit runner to signal."
			return 0
			;;
		esac
		;;
	esac

	send_test_signal_to_pid TERM "$l_runner_signal_pid" || :
	(
		sleep "$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS"
		send_test_signal_to_pid KILL "$l_runner_signal_pid" || :
		case "$l_runner_pid:$l_runner_signal_pid" in
		"$l_runner_signal_pid:$l_runner_signal_pid") ;;
		*)
			send_test_signal_to_pid KILL "$l_runner_pid" || :
			;;
		esac
	) &
	l_watchdog_pid=$!

	set +e
	wait "$l_runner_pid"
	l_runner_status=$?
	[ "$l_restore_errexit" = "1" ] && set -e
	kill "$l_watchdog_pid" >/dev/null 2>&1 || :
	wait "$l_watchdog_pid" >/dev/null 2>&1 || :

	l_suite_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_suite_pid"; then
			l_suite_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	l_runner_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_runner_signal_pid"; then
			l_runner_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	if [ "$l_suite_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_suite_pid" || :
	fi
	if [ "$l_runner_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_runner_signal_pid" || :
		case "$l_runner_pid:$l_runner_signal_pid" in
		"$l_runner_signal_pid:$l_runner_signal_pid") ;;
		*)
			send_test_signal_to_pid KILL "$l_runner_pid" || :
			;;
		esac
	fi

	assertEquals "Signal cleanup should terminate the runner with the signal-derived exit status even when a suite ignores TERM." \
		143 "$l_runner_status"
	assertEquals "Signal cleanup should not leave a TERM-resistant suite running after the runner exits." \
		0 "$l_suite_gone"
	assertEquals "Signal cleanup should not leave the nested shunit runner alive after the caller regains control." \
		0 "$l_runner_gone"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_tracks_worker_child_before_handling_launch_signals() {
	l_race_suite="$TEST_TMPDIR/race-launch-suite.sh"
	l_support_suite="$TEST_TMPDIR/race-launch-support-suite.sh"
	l_shell_path="$TEST_TMPDIR/race-launch-shell"
	l_started_path="$TEST_TMPDIR/race-launch.started"
	l_suite_pid_path="$TEST_TMPDIR/race-launch.pid"
	l_capture_path="$TEST_TMPDIR/race-launch.output"
	l_empty_token_done_path="$TEST_TMPDIR/race-launch.empty-token"
	rm -f "$l_started_path" "$l_suite_pid_path" \
		"$l_empty_token_done_path" "$l_empty_token_done_path.active"

	write_fake_suite_with_body "$l_race_suite" <<'EOF'
#!/bin/sh
	exit 0
EOF
	write_fake_suite_with_body "$l_support_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "race-launch-support"
	exit 0
EOF
	chmod 644 "$l_race_suite" "$l_support_suite"

	cat >"$l_shell_path" <<'EOF'
#!/bin/sh
case "$1" in
*race-launch-suite.sh)
	printf '%s\n' "$$" >"${FAKE_SUITE_LOG:?}"
	: >"${FAKE_SUITE_STARTED:?}"
	trap '' TERM
	kill -TERM "$PPID"
	l_fake_wait_remaining=${FAKE_SUITE_WAIT_LIMIT:?}
	while [ "$l_fake_wait_remaining" -gt 0 ]; do
		sleep 1
		l_fake_wait_remaining=$((l_fake_wait_remaining - 1))
	done
	exit 124
	;;
*)
	exec /bin/sh "$1"
	;;
esac
EOF
	chmod +x "$l_shell_path"

	if ZXFER_TEST_SHELL="$l_shell_path" \
		FAKE_SUITE_LOG="$l_suite_pid_path" \
		FAKE_SUITE_STARTED="$l_started_path" \
		FAKE_SUITE_WAIT_LIMIT="$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS" \
		ZXFER_TEST_EMPTY_PROCESS_TOKEN_PID_FILE="$l_suite_pid_path" \
		ZXFER_TEST_EMPTY_PROCESS_TOKEN_DONE_FILE="$l_empty_token_done_path" \
		ZXFER_TEST_EMPTY_PROCESS_TOKEN_READY_FILE="$l_started_path" \
		"$RUN_SHUNIT_TESTS_BIN" --jobs 2 \
		"$l_race_suite" "$l_support_suite" >"$l_capture_path" 2>&1; then
		ZXFER_TEST_CAPTURE_STATUS=0
	else
		ZXFER_TEST_CAPTURE_STATUS=$?
	fi
	ZXFER_TEST_CAPTURE_OUTPUT=$(cat "$l_capture_path")

	assertEquals "A worker that receives TERM during child launch should still report the signal-derived suite status after the child PID is tracked." \
		143 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The launch-race regression should still record the failed suite output." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "!! Suite failed: $l_race_suite (exit status 143)"
	assertContains "The support suite should still complete once the launch-race worker is cleaned up." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "race-launch-support"
	assertContains "The grouped summary should reflect one failed and one passed suite." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "==> shunit2 summary: 1 passed, 1 failed"
	l_started_present=1
	if [ -f "$l_started_path" ]; then
		l_started_present=0
	fi
	assertEquals "The launch-race wrapper should have reached its startup marker before being torn down." \
		0 "$l_started_present"
	l_empty_token_forced=1
	if [ -f "$l_empty_token_done_path" ]; then
		l_empty_token_forced=0
	fi
	assertEquals "The launch-race fixture should force the first complete child identity lookup to return no token." \
		0 "$l_empty_token_forced"

	l_suite_pid=$(cat "$l_suite_pid_path")
	l_suite_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 5 ]; do
		if ! send_test_signal_to_pid 0 "$l_suite_pid"; then
			l_suite_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done

	assertEquals "Signals that land during worker child launch should not orphan the launched test-shell process." \
		0 "$l_suite_gone"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_consumes_parent_signal_during_worker_identity_capture() {
	l_race_suite="$TEST_TMPDIR/parent-race-suite.sh"
	l_support_suite="$TEST_TMPDIR/parent-race-support-suite.sh"
	l_runner_wrapper="$TEST_TMPDIR/parent-race-runner.sh"
	l_runner_pid_path="$TEST_TMPDIR/parent-race-runner.pid"
	l_worker_pid_path="$TEST_TMPDIR/parent-race-worker.pid"
	l_suite_pid_path="$TEST_TMPDIR/parent-race-suite.pid"
	l_started_path="$TEST_TMPDIR/parent-race-suite.started"
	l_signal_done_path="$TEST_TMPDIR/parent-race-signal.done"
	l_capture_path="$TEST_TMPDIR/parent-race.output"
	l_runner_tmpdir="$TEST_TMPDIR/parent-race-state"
	rm -f "$l_runner_pid_path" "$l_worker_pid_path" \
		"$l_suite_pid_path" "$l_started_path" \
		"$l_signal_done_path" "$l_capture_path"
	mkdir -p "$l_runner_tmpdir"
	write_fake_runner_pid_wrapper "$l_runner_wrapper"

	write_fake_suite_with_body "$l_race_suite" <<'EOF'
#!/bin/sh
	trap '' TERM
	printf '%s\n' "$PPID" >"${FAKE_WORKER_PID_LOG:?}"
	printf '%s\n' "$$" >"${FAKE_SUITE_LOG:?}"
	: >"${FAKE_SUITE_STARTED:?}"
	l_fake_wait_remaining=${FAKE_SUITE_WAIT_LIMIT:?}
	while [ "$l_fake_wait_remaining" -gt 0 ]; do
		sleep 1
		l_fake_wait_remaining=$((l_fake_wait_remaining - 1))
	done
	exit 124
EOF
	write_fake_suite_with_body "$l_support_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' parent-race-support
	exit 0
EOF
	chmod +x "$l_race_suite" "$l_support_suite"

	if FAKE_RUNNER_PID_FILE="$l_runner_pid_path" \
		FAKE_RUNNER_TARGET="$RUN_SHUNIT_TESTS_BIN" \
		FAKE_WORKER_PID_LOG="$l_worker_pid_path" \
		FAKE_SUITE_LOG="$l_suite_pid_path" \
		FAKE_SUITE_STARTED="$l_started_path" \
		FAKE_SUITE_WAIT_LIMIT="$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS" \
		TMPDIR="$l_runner_tmpdir" \
		ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT="$l_runner_tmpdir" \
		ZXFER_TEST_SIGNAL_PARENT_RUNNER_PID_FILE="$l_runner_pid_path" \
		ZXFER_TEST_SIGNAL_PARENT_WORKER_PID_FILE="$l_worker_pid_path" \
		ZXFER_TEST_SIGNAL_PARENT_DONE_FILE="$l_signal_done_path" \
		ZXFER_TEST_SIGNAL_PARENT_READY_FILE="$l_started_path" \
		"$l_runner_wrapper" --jobs 2 \
		"$l_race_suite" "$l_support_suite" >"$l_capture_path" 2>&1; then
		l_runner_status=0
	else
		l_runner_status=$?
	fi

	assertEquals "A parent-side TERM during worker identity capture should retain the signal-derived runner status." \
		143 "$l_runner_status"
	l_signal_forced=1
	if [ -f "$l_signal_done_path" ]; then
		l_signal_forced=0
	fi
	assertEquals "The identity fixture should deliver TERM while the parent runner is inspecting its worker." \
		0 "$l_signal_forced"

	l_suite_pid=$(cat "$l_suite_pid_path" 2>/dev/null || true)
	l_suite_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 5 ]; do
		if ! send_test_signal_to_pid 0 "$l_suite_pid"; then
			l_suite_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_suite_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_suite_pid" || :
	fi
	assertEquals "Parent launch-window cleanup should not orphan the suite child." \
		0 "$l_suite_gone"

	l_state_dir_count=0
	for l_state_dir in "$l_runner_tmpdir"/zxfer_shunit.*; do
		[ -d "$l_state_dir" ] || continue
		l_state_dir_count=$((l_state_dir_count + 1))
	done
	assertEquals "The parent launch-window signal should still reach normal runner state cleanup." \
		0 "$l_state_dir_count"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_reaps_child_when_identity_stays_unavailable() {
	l_race_suite="$TEST_TMPDIR/tokenless-suite.sh"
	l_support_suite="$TEST_TMPDIR/tokenless-support-suite.sh"
	l_shell_path="$TEST_TMPDIR/tokenless-test-shell"
	l_runner_wrapper="$TEST_TMPDIR/tokenless-runner.sh"
	l_runner_pid_path="$TEST_TMPDIR/tokenless-runner.pid"
	l_runner_tmpdir="$TEST_TMPDIR/tokenless-state"
	l_started_path="$TEST_TMPDIR/tokenless-suite.started"
	l_support_started_path="$TEST_TMPDIR/tokenless-support.started"
	l_suite_pid_path="$TEST_TMPDIR/tokenless-suite.pid"
	l_grandchild_owner_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/tokenless-grandchild.owner.pid"
	l_grandchild_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/tokenless-grandchild.pid"
	l_capture_path="$TEST_TMPDIR/tokenless.output"
	l_force_empty_hit_path="$TEST_TMPDIR/tokenless-force-empty.hit"
	l_watchdog_fired_path="$TEST_TMPDIR/tokenless-watchdog.fired"
	rm -f "$l_runner_pid_path" "$l_started_path" \
		"$l_support_started_path" "$l_suite_pid_path" \
		"$l_grandchild_owner_pid_path" "$l_grandchild_pid_path" \
		"$l_capture_path" "$l_force_empty_hit_path" \
		"$l_watchdog_fired_path"
	mkdir -p "$l_runner_tmpdir"
	write_fake_runner_pid_wrapper "$l_runner_wrapper"

	write_fake_suite_with_body "$l_race_suite" <<'EOF'
#!/bin/sh
	exit 0
EOF
	write_fake_suite_with_body "$l_support_suite" <<'EOF'
#!/bin/sh
	: >"${FAKE_SUPPORT_STARTED:?}"
	printf '%s\n' tokenless-support
	exit 0
EOF
	chmod 644 "$l_race_suite" "$l_support_suite"

	cat >"$l_shell_path" <<'EOF'
#!/bin/sh
case "$1" in
*tokenless-suite.sh)
	trap '' TERM
	/bin/sh -c '
		trap "" TERM
		printf "%s\n" "$$" >"${FAKE_GRANDCHILD_PID_FILE:?}"
		exec sleep "${FAKE_DESCENDANT_WAIT_SECONDS:?}"
	' tokenless-grandchild-marker &
	l_grandchild_pid=$!
	printf '%s\n' "$$" >"${FAKE_GRANDCHILD_OWNER_PID_FILE:?}"
	printf '%s\n' "$$" >"${FAKE_SUITE_LOG:?}"
	l_wait_count=0
	while [ ! -s "${FAKE_GRANDCHILD_PID_FILE:?}" ] &&
		[ "$l_wait_count" -lt 10 ]; do
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	[ -s "${FAKE_GRANDCHILD_PID_FILE:?}" ] || exit 9
	: >"${FAKE_SUITE_STARTED:?}"
	wait "$l_grandchild_pid"
	;;
*)
	exec /bin/sh "$1"
	;;
esac
EOF
	chmod +x "$l_shell_path"

	l_restore_errexit=0
	case $- in
	*e*) l_restore_errexit=1 ;;
	esac
	set +e
	FAKE_RUNNER_PID_FILE="$l_runner_pid_path" \
		FAKE_RUNNER_TARGET="$RUN_SHUNIT_TESTS_BIN" \
		ZXFER_TEST_SHELL="$l_shell_path" \
		FAKE_SUITE_LOG="$l_suite_pid_path" \
		FAKE_SUITE_STARTED="$l_started_path" \
		FAKE_SUPPORT_STARTED="$l_support_started_path" \
		FAKE_GRANDCHILD_OWNER_PID_FILE="$l_grandchild_owner_pid_path" \
		FAKE_GRANDCHILD_PID_FILE="$l_grandchild_pid_path" \
		FAKE_DESCENDANT_WAIT_SECONDS=$((RUN_SHUNIT_TESTS_WATCHDOG_SECONDS + 10)) \
		TMPDIR="$l_runner_tmpdir" \
		ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT="$l_runner_tmpdir" \
		ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_ALL=1 \
		ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_HIT_FILE="$l_force_empty_hit_path" \
		ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_READY_FILE="$l_started_path" \
		"$l_runner_wrapper" --jobs 2 \
		"$l_race_suite" "$l_support_suite" >"$l_capture_path" 2>&1 &
	l_runner_launch_pid=$!
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
	(
		sleep $((RUN_SHUNIT_TESTS_WATCHDOG_SECONDS + 20))
		send_test_signal_to_pid KILL "$l_runner_launch_pid" || :
	) &
	l_readiness_watchdog_pid=$!

	l_relationship_ready=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if [ -s "$l_runner_pid_path" ] && [ -s "$l_suite_pid_path" ] &&
			[ -s "$l_grandchild_pid_path" ] && [ -e "$l_started_path" ] &&
			[ -e "$l_support_started_path" ] &&
			[ -e "$l_force_empty_hit_path" ]; then
			l_runner_signal_pid=$(cat "$l_runner_pid_path")
			l_suite_pid=$(cat "$l_suite_pid_path")
			l_grandchild_pid=$(cat "$l_grandchild_pid_path")
			case "$l_runner_signal_pid:$l_suite_pid:$l_grandchild_pid" in
			*[!0-9:]* | :* | *: | *::*)
				l_wait_count=$((l_wait_count + 1))
				sleep 1
				continue
				;;
			esac
			l_recorded_grandchild_owner=$(cat \
				"$l_grandchild_owner_pid_path" 2>/dev/null || true)
			for l_child_pid_file in \
				"$l_runner_tmpdir"/zxfer_shunit.*/*.child.pid; do
				[ -r "$l_child_pid_file" ] || continue
				l_recorded_suite_pid=$(cat "$l_child_pid_file" 2>/dev/null || true)
				[ "$l_recorded_suite_pid" = "$l_suite_pid" ] || continue
				l_worker_prefix=${l_child_pid_file%.child.pid}
				l_worker_pid=$(cat "$l_worker_prefix.pid" 2>/dev/null || true)
				case "$l_worker_pid" in
				'' | *[!0-9]*) continue ;;
				esac
				l_recorded_worker_owner=$(cat \
					"$l_worker_prefix.owner.pid" 2>/dev/null || true)
				l_recorded_suite_owner=$(cat \
					"$l_worker_prefix.child.owner.pid" 2>/dev/null || true)
				if [ "$l_recorded_worker_owner" = "$l_runner_signal_pid" ] &&
					[ "$l_recorded_suite_owner" = "$l_worker_pid" ] &&
					[ "$l_recorded_grandchild_owner" = "$l_suite_pid" ]; then
					l_relationship_ready=0
					break
				fi
			done
			[ "$l_relationship_ready" -eq 0 ] && break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_relationship_ready" -ne 0 ]; then
		set +e
		wait "$l_runner_launch_pid" >/dev/null 2>&1 || :
		if [ "$l_restore_errexit" = "1" ]; then
			set -e
		fi
		kill "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
		wait "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
		fail "The tokenless top-level cleanup fixture did not publish its complete ownership chain."
		return 0
	fi
	kill "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
	wait "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
	send_test_signal_to_pid TERM "$l_runner_signal_pid" || :
	(
		sleep "$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS"
		: >"$l_watchdog_fired_path"
		send_test_signal_to_pid KILL "$l_grandchild_pid" || :
		send_test_signal_to_pid KILL "$l_suite_pid" || :
		send_test_signal_to_pid KILL "$l_worker_pid" || :
		send_test_signal_to_pid KILL "$l_runner_signal_pid" || :
		case "$l_runner_launch_pid:$l_runner_signal_pid" in
		"$l_runner_signal_pid:$l_runner_signal_pid") ;;
		*) send_test_signal_to_pid KILL "$l_runner_launch_pid" || : ;;
		esac
	) &
	l_watchdog_pid=$!

	set +e
	wait "$l_runner_launch_pid"
	l_runner_status=$?
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
	kill "$l_watchdog_pid" >/dev/null 2>&1 || :
	wait "$l_watchdog_pid" >/dev/null 2>&1 || :

	assertEquals "Top-level tokenless cleanup should retain the signal-derived runner status." \
		143 "$l_runner_status"
	l_force_empty_hit=1
	if [ -f "$l_force_empty_hit_path" ]; then
		l_force_empty_hit=0
	fi
	assertEquals "The persistent-empty fixture should intercept the suite token lookup." \
		0 "$l_force_empty_hit"
	l_watchdog_fired=0
	if [ -f "$l_watchdog_fired_path" ]; then
		l_watchdog_fired=1
	fi
	assertEquals "Tokenless cleanup should complete before the external watchdog fires." \
		0 "$l_watchdog_fired"

	l_suite_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_suite_pid"; then
			l_suite_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_suite_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_suite_pid" || :
	fi
	assertEquals "Top-level persistent identity failure must not leave the suite child alive." \
		0 "$l_suite_gone"

	l_grandchild_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_grandchild_pid"; then
			l_grandchild_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_grandchild_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_grandchild_pid" || :
	fi
	assertEquals "Tokenless ownership cleanup must not orphan a TERM-resistant grandchild." \
		0 "$l_grandchild_gone"

	l_worker_gone=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		if ! send_test_signal_to_pid 0 "$l_worker_pid"; then
			l_worker_gone=0
			break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_worker_gone" -ne 0 ]; then
		send_test_signal_to_pid KILL "$l_worker_pid" || :
	fi
	assertEquals "Persistent identity failure must not leave the owning worker alive." \
		0 "$l_worker_gone"

	l_state_dir_count=0
	for l_state_dir in "$l_runner_tmpdir"/zxfer_shunit.*; do
		[ -d "$l_state_dir" ] || continue
		l_state_dir_count=$((l_state_dir_count + 1))
	done
	assertEquals "Top-level tokenless cleanup should remove private runner state." \
		0 "$l_state_dir_count"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_reaps_serial_child_when_identity_stays_unavailable() {
	l_suite_path="$TEST_TMPDIR/tokenless-serial-suite.sh"
	l_shell_path="$TEST_TMPDIR/tokenless-serial-test-shell"
	l_runner_wrapper="$TEST_TMPDIR/tokenless-serial-runner.sh"
	l_runner_pid_path="$TEST_TMPDIR/tokenless-serial-runner.pid"
	l_runner_tmpdir="$TEST_TMPDIR/tokenless-serial-state"
	l_started_path="$TEST_TMPDIR/tokenless-serial-suite.started"
	l_suite_pid_path="$TEST_TMPDIR/tokenless-serial-suite.pid"
	l_descendant_owner_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/tokenless-serial-descendant.owner.pid"
	l_descendant_pid_path="$RUN_SHUNIT_TESTS_IDENTITY_RELATIONS/tokenless-serial-descendant.pid"
	l_capture_path="$TEST_TMPDIR/tokenless-serial.output"
	l_force_empty_hit_path="$TEST_TMPDIR/tokenless-serial-force-empty.hit"
	l_watchdog_fired_path="$TEST_TMPDIR/tokenless-serial-watchdog.fired"
	rm -f "$l_runner_pid_path" "$l_started_path" "$l_suite_pid_path" \
		"$l_descendant_owner_pid_path" "$l_descendant_pid_path" \
		"$l_capture_path" "$l_force_empty_hit_path" \
		"$l_watchdog_fired_path"
	mkdir -p "$l_runner_tmpdir"
	write_fake_runner_pid_wrapper "$l_runner_wrapper"

	write_fake_suite_with_body "$l_suite_path" <<'EOF'
#!/bin/sh
	exit 0
EOF
	chmod 644 "$l_suite_path"
	cat >"$l_shell_path" <<'EOF'
#!/bin/sh
trap '' TERM
/bin/sh -c '
	trap "" TERM
	printf "%s\n" "$$" >"${FAKE_DESCENDANT_PID_FILE:?}"
	exec sleep "${FAKE_DESCENDANT_WAIT_SECONDS:?}"
' tokenless-serial-descendant-marker &
l_descendant_pid=$!
printf '%s\n' "$$" >"${FAKE_DESCENDANT_OWNER_PID_FILE:?}"
printf '%s\n' "$$" >"${FAKE_SUITE_LOG:?}"
l_wait_count=0
while [ ! -s "${FAKE_DESCENDANT_PID_FILE:?}" ] &&
	[ "$l_wait_count" -lt 10 ]; do
	l_wait_count=$((l_wait_count + 1))
	sleep 1
done
[ -s "${FAKE_DESCENDANT_PID_FILE:?}" ] || exit 9
: >"${FAKE_SUITE_STARTED:?}"
wait "$l_descendant_pid"
EOF
	chmod +x "$l_shell_path"

	l_restore_errexit=0
	case $- in
	*e*) l_restore_errexit=1 ;;
	esac
	set +e
	FAKE_RUNNER_PID_FILE="$l_runner_pid_path" \
		FAKE_RUNNER_TARGET="$RUN_SHUNIT_TESTS_BIN" \
		ZXFER_TEST_SHELL="$l_shell_path" \
		FAKE_SUITE_LOG="$l_suite_pid_path" \
		FAKE_SUITE_STARTED="$l_started_path" \
		FAKE_DESCENDANT_OWNER_PID_FILE="$l_descendant_owner_pid_path" \
		FAKE_DESCENDANT_PID_FILE="$l_descendant_pid_path" \
		FAKE_DESCENDANT_WAIT_SECONDS=$((RUN_SHUNIT_TESTS_WATCHDOG_SECONDS + 10)) \
		TMPDIR="$l_runner_tmpdir" \
		ZXFER_TEST_PROCESS_IDENTITY_STATE_ROOT="$l_runner_tmpdir" \
		ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_ALL=1 \
		ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_HIT_FILE="$l_force_empty_hit_path" \
		ZXFER_TEST_FORCE_EMPTY_PROCESS_TOKEN_READY_FILE="$l_started_path" \
		"$l_runner_wrapper" --jobs 1 "$l_suite_path" \
		>"$l_capture_path" 2>&1 &
	l_runner_launch_pid=$!
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
	(
		sleep $((RUN_SHUNIT_TESTS_WATCHDOG_SECONDS + 20))
		send_test_signal_to_pid KILL "$l_runner_launch_pid" || :
	) &
	l_readiness_watchdog_pid=$!

	l_relationship_ready=1
	l_wait_count=0
	while [ "$l_wait_count" -lt 10 ]; do
		l_state_dir=
		for l_candidate_state_dir in "$l_runner_tmpdir"/zxfer_shunit.*; do
			[ -d "$l_candidate_state_dir" ] || continue
			l_state_dir=$l_candidate_state_dir
			break
		done
		if [ -n "$l_state_dir" ] && [ -s "$l_runner_pid_path" ] &&
			[ -s "$l_suite_pid_path" ] && [ -s "$l_descendant_pid_path" ] &&
			[ -e "$l_started_path" ] && [ -e "$l_force_empty_hit_path" ]; then
			l_runner_signal_pid=$(cat "$l_runner_pid_path")
			l_wrapper_pid=$(cat "$l_state_dir/foreground.pid" 2>/dev/null || true)
			l_suite_pid=$(cat "$l_suite_pid_path")
			l_descendant_pid=$(cat "$l_descendant_pid_path")
			l_recorded_wrapper_owner=$(cat \
				"$l_state_dir/foreground.owner.pid" 2>/dev/null || true)
			l_recorded_suite_owner=$(cat \
				"$l_state_dir/foreground.child.owner.pid" 2>/dev/null || true)
			l_recorded_suite_pid=$(cat \
				"$l_state_dir/foreground.child.pid" 2>/dev/null || true)
			l_recorded_descendant_owner=$(cat \
				"$l_descendant_owner_pid_path" 2>/dev/null || true)
			case "$l_runner_signal_pid:$l_wrapper_pid:$l_suite_pid:$l_descendant_pid" in
			*[!0-9:]* | :* | *: | *::*) ;;
			*)
				if [ "$l_recorded_wrapper_owner" = "$l_runner_signal_pid" ] &&
					[ "$l_recorded_suite_owner" = "$l_wrapper_pid" ] &&
					[ "$l_recorded_suite_pid" = "$l_suite_pid" ] &&
					[ "$l_recorded_descendant_owner" = "$l_suite_pid" ]; then
					l_relationship_ready=0
				fi
				;;
			esac
			[ "$l_relationship_ready" -eq 0 ] && break
		fi
		l_wait_count=$((l_wait_count + 1))
		sleep 1
	done
	if [ "$l_relationship_ready" -ne 0 ]; then
		set +e
		wait "$l_runner_launch_pid" >/dev/null 2>&1 || :
		if [ "$l_restore_errexit" = "1" ]; then
			set -e
		fi
		kill "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
		wait "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
		fail "The serial tokenless fixture did not publish its ownership chain."
		return 0
	fi
	kill "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :
	wait "$l_readiness_watchdog_pid" >/dev/null 2>&1 || :

	send_test_signal_to_pid TERM "$l_runner_signal_pid" || :
	(
		sleep "$RUN_SHUNIT_TESTS_WATCHDOG_SECONDS"
		: >"$l_watchdog_fired_path"
		send_test_signal_to_pid KILL "$l_descendant_pid" || :
		send_test_signal_to_pid KILL "$l_suite_pid" || :
		send_test_signal_to_pid KILL "$l_wrapper_pid" || :
		send_test_signal_to_pid KILL "$l_runner_signal_pid" || :
		case "$l_runner_launch_pid:$l_runner_signal_pid" in
		"$l_runner_signal_pid:$l_runner_signal_pid") ;;
		*) send_test_signal_to_pid KILL "$l_runner_launch_pid" || : ;;
		esac
	) &
	l_watchdog_pid=$!

	set +e
	wait "$l_runner_launch_pid"
	l_runner_status=$?
	if [ "$l_restore_errexit" = "1" ]; then
		set -e
	fi
	kill "$l_watchdog_pid" >/dev/null 2>&1 || :
	wait "$l_watchdog_pid" >/dev/null 2>&1 || :

	l_watchdog_fired=0
	if [ -f "$l_watchdog_fired_path" ]; then
		l_watchdog_fired=1
	fi
	for l_tracked_pid in \
		"$l_descendant_pid" "$l_suite_pid" "$l_wrapper_pid"; do
		l_wait_count=0
		while [ "$l_wait_count" -lt 10 ] &&
			send_test_signal_to_pid 0 "$l_tracked_pid"; do
			l_wait_count=$((l_wait_count + 1))
			sleep 1
		done
	done
	l_descendant_gone=0
	l_suite_gone=0
	l_wrapper_gone=0
	send_test_signal_to_pid 0 "$l_descendant_pid" && l_descendant_gone=1
	send_test_signal_to_pid 0 "$l_suite_pid" && l_suite_gone=1
	send_test_signal_to_pid 0 "$l_wrapper_pid" && l_wrapper_gone=1
	[ "$l_descendant_gone" -eq 0 ] || send_test_signal_to_pid KILL "$l_descendant_pid" || :
	[ "$l_suite_gone" -eq 0 ] || send_test_signal_to_pid KILL "$l_suite_pid" || :
	[ "$l_wrapper_gone" -eq 0 ] || send_test_signal_to_pid KILL "$l_wrapper_pid" || :

	l_state_dir_count=0
	for l_state_dir in "$l_runner_tmpdir"/zxfer_shunit.*; do
		[ -d "$l_state_dir" ] || continue
		l_state_dir_count=$((l_state_dir_count + 1))
	done
	assertEquals "Serial top-level tokenless cleanup should preserve the TERM-derived status." \
		143 "$l_runner_status"
	assertEquals "Serial tokenless cleanup should finish before its watchdog." \
		0 "$l_watchdog_fired"
	assertEquals "Serial tokenless cleanup should terminate its resistant descendant." \
		0 "$l_descendant_gone"
	assertEquals "Serial tokenless cleanup should terminate its suite." \
		0 "$l_suite_gone"
	assertEquals "Serial tokenless cleanup should reap its foreground wrapper." \
		0 "$l_wrapper_gone"
	assertEquals "Serial tokenless cleanup should remove private runner state." \
		0 "$l_state_dir_count"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_preserves_last_failed_suite_status_in_parallel() {
	l_first_suite="$TEST_TMPDIR/first-fail-suite.sh"
	l_second_suite="$TEST_TMPDIR/second-fail-suite.sh"

	write_fake_suite_with_body "$l_first_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "first-fail"
	exit 3
EOF
	chmod +x "$l_first_suite"

	write_fake_suite_with_body "$l_second_suite" <<'EOF'
#!/bin/sh
	printf '%s\n' "second-fail"
	exit 7
EOF
	chmod +x "$l_second_suite"

	zxfer_test_capture_subshell "
		\"$RUN_SHUNIT_TESTS_BIN\" --jobs 2 \"$l_first_suite\" \"$l_second_suite\"
	"

	assertEquals "Parallel execution should preserve the exit status from the last failed suite in input order." \
		7 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Parallel execution should still report both suite failures in the grouped replay output." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "!! Suite failed: $l_first_suite (exit status 3)"
	assertContains "Parallel execution should still report the final failed suite's status." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "!! Suite failed: $l_second_suite (exit status 7)"
	assertContains "Parallel execution should keep the failure summary accurate." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "==> shunit2 summary: 0 passed, 2 failed"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_uses_zxfer_test_shell_for_non_executable_suites_in_parallel() {
	l_first_suite="$TEST_TMPDIR/nonexec-suite-one.sh"
	l_second_suite="$TEST_TMPDIR/nonexec-suite-two.sh"
	l_shell_path="$TEST_TMPDIR/fake-test-shell"
	write_fake_suite "$l_first_suite" "runner-dispatch-one"
	write_fake_suite "$l_second_suite" "runner-dispatch-two"
	chmod 644 "$l_first_suite" "$l_second_suite"
	write_fake_test_shell "$l_shell_path"

	output=$(
		ZXFER_TEST_SHELL="$l_shell_path" \
			FAKE_SUITE_LOG="$FAKE_SUITE_LOG" \
			FAKE_TEST_SHELL_LOG="$FAKE_TEST_SHELL_LOG" \
			"$RUN_SHUNIT_TESTS_BIN" --jobs 2 "$l_first_suite" "$l_second_suite"
	)

	assertContains "The runner banner should include the configured alternate shell during parallel dispatch." \
		"$output" "with test shell [$l_shell_path]"
	assertContains "The alternate-shell dispatch path should still report a passing suite summary when background workers are enabled." \
		"$output" "==> shunit2 summary: 2 passed, 0 failed"
	assertContains "The first fake suite should run under the configured alternate shell." \
		"$(cat "$FAKE_SUITE_LOG")" "runner-dispatch-one"
	assertContains "The second fake suite should run under the configured alternate shell." \
		"$(cat "$FAKE_SUITE_LOG")" "runner-dispatch-two"
	assertContains "The alternate shell should receive the first suite path during parallel dispatch." \
		"$(cat "$FAKE_TEST_SHELL_LOG")" "$l_first_suite"
	assertContains "The alternate shell should receive the second suite path during parallel dispatch." \
		"$(cat "$FAKE_TEST_SHELL_LOG")" "$l_second_suite"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_run_shunit_tests_rejects_missing_zxfer_test_shell() {
	l_suite_path="$TEST_TMPDIR/missing-shell-suite.sh"
	write_fake_suite "$l_suite_path" "missing-shell"
	chmod +x "$l_suite_path"

	zxfer_test_capture_subshell "
		ZXFER_TEST_SHELL=\"$TEST_TMPDIR/does-not-exist\" \
			FAKE_SUITE_LOG=\"$FAKE_SUITE_LOG\" \
			\"$RUN_SHUNIT_TESTS_BIN\" \"$l_suite_path\"
	"

	assertEquals "A missing alternate shell should fail before any suites are run." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The runner should surface a clear error when ZXFER_TEST_SHELL cannot be executed." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_TEST_SHELL is not executable: $TEST_TMPDIR/does-not-exist"
	assertEquals "The fake suite should not run when the alternate shell is invalid." \
		"" "$(cat "$FAKE_SUITE_LOG")"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_vendored_shunit_escape_characters_handles_bsd_userland() {
	actual=$(_shunit_escapeCharactersInString "has'quote\`and\$dollar")
	expected="has\\'quote\\\`and\\\$dollar"

	assertEquals "The vendored shunit2 string escaper should not depend on GNU sed extensions." \
		"$expected" "$actual"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_clears_unsafe_failure_report_commands_from_ambient_env() {
	zxfer_test_capture_subshell "
		TESTS_DIR=\"$TESTS_DIR\" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		PATH=\"${PATH:-/usr/bin:/bin}\" \
		/bin/sh -c '
			. \"\$1/test_helper.sh\"
			if [ -n \"\${ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS+x}\" ]; then
				exit 1
			fi
		' sh \"$TESTS_DIR\"
	"

	assertEquals "Sourcing the shared test helper should clear ambient unsafe failure-report command overrides so unrelated suites stay deterministic." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_clears_ambient_runner_test_shell() {
	zxfer_test_capture_subshell "
		TESTS_DIR=\"$TESTS_DIR\" \
		ZXFER_TEST_SHELL=/bin/dash \
		PATH=\"${PATH:-/usr/bin:/bin}\" \
		/bin/sh -c '
			. \"\$1/test_helper.sh\"
			if [ -n \"\${ZXFER_TEST_SHELL+x}\" ]; then
				exit 1
			fi
		' sh \"$TESTS_DIR\"
	"

	assertEquals "Sourcing the shared test helper should clear ambient runner-shell overrides so nested runner tests use their own explicit shell selection." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_test_helper_keeps_domain_fixtures_opt_in() {
	/bin/sh -c '
		TESTS_DIR=$1
		. "$1/test_helper.sh"
		if command -v zxfer_test_render_current_backup_metadata_contents >/dev/null 2>&1 ||
			command -v zxfer_test_write_env_fake_ssh >/dev/null 2>&1; then
			exit 1
		fi
		. "$1/helpers/backup_fixtures.sh"
		. "$1/helpers/fake_tool_fixtures.sh"
		command -v zxfer_test_render_current_backup_metadata_contents >/dev/null 2>&1 &&
			command -v zxfer_test_write_env_fake_ssh >/dev/null 2>&1
	' sh "$TESTS_DIR"
	l_status=$?

	assertEquals "Domain-specific backup and fake-tool fixtures should load only after a suite explicitly sources them." \
		0 "$l_status"
}

# shellcheck disable=SC2317,SC2329  # Invoked indirectly by shunit2.
test_fake_tool_fixture_returns_write_failure_before_chmod() {
	l_fake_chmod_log="$TEST_TMPDIR/fake-tool-chmod.log"
	l_fake_ssh_path="$TEST_TMPDIR/fake-tool-ssh"

	if /bin/sh -c '
		. "$1/helpers/fake_tool_fixtures.sh"
		fake_chmod_log=$2
		cat() { return 73; }
		chmod() {
			: >"$fake_chmod_log"
			return 0
		}
		zxfer_test_write_env_fake_ssh "$3"
	' sh "$TESTS_DIR" "$l_fake_chmod_log" "$l_fake_ssh_path"; then
		l_status=0
	else
		l_status=$?
	fi
	if [ -e "$l_fake_chmod_log" ]; then
		l_chmod_called=yes
	else
		l_chmod_called=no
	fi

	assertEquals "The fake-tool writer should preserve the executable write failure." \
		73 "$l_status"
	assertEquals "The fake-tool writer should not chmod a path after its write fails." \
		no "$l_chmod_called"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
