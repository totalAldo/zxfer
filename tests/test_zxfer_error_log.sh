#!/bin/sh
#
# shunit2 tests for src/zxfer_error_log.sh: the ZXFER_ERROR_LOG mirror (path
# validation, fallback lock directories, staged creation and append) and its
# owned-lock protocol.
#
# Lock metadata is owner pid + process start token only (V2). These tests pin
# pid+start-token liveness, stale reaping, checked release, and the
# old-format-treated-as-corrupt policy. The ps parser behind the start token
# belongs to the cleanup wrapper and is tested in
# tests/test_zxfer_cleanup_child_wrapper.sh.
#
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_error_log.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_error_log"
}

oneTimeTearDown() {
	chmod -R u+rwx "$TEST_TMPDIR" >/dev/null 2>&1 || true
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_reset_all_owner_state
	unset ZXFER_ERROR_LOG ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS
	TMPDIR="$TEST_TMPDIR"
	export TMPDIR
	zxfer_reset_owned_lock_tracking
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_R_recursive="tank/src"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_option_Y_yield_iterations=3
	g_zxfer_version="test-version"
	g_zxfer_original_invocation="'./zxfer' 'backup/dst'"
	g_zxfer_secure_staging_dir_result=""
	g_zxfer_runtime_artifact_cleanup_paths=""
	zxfer_test_allocate_runtime_root "$TEST_TMPDIR" ||
		fail "Unable to allocate the error-log test run root."
	zxfer_reset_failure_context "unit"
}

write_owned_lock_metadata_fixture() {
	l_lock_dir=$1
	l_pid=${2:-$$}
	l_start_token=${3:-}

	mkdir -p "$l_lock_dir" || fail "Unable to create owned lock fixture directory."
	chmod 700 "$l_lock_dir" || fail "Unable to chmod owned lock fixture directory."
	if [ -z "$l_start_token" ]; then
		l_start_token=$(zxfer_get_process_start_token "$$" 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture start token."
	fi

	cat >"$l_lock_dir/metadata" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	$l_pid
start_token	$l_start_token
EOF
	chmod 600 "$l_lock_dir/metadata" || fail "Unable to chmod owned lock fixture metadata."
}

write_old_format_owned_lock_metadata_fixture() {
	l_lock_dir=$1
	l_pid=${2:-$$}

	mkdir -p "$l_lock_dir" || fail "Unable to create old-format owned lock fixture directory."
	chmod 700 "$l_lock_dir" || fail "Unable to chmod old-format owned lock fixture directory."
	cat >"$l_lock_dir/metadata" <<EOF
ZXFER_LOCK_METADATA_V1
kind	lock
purpose	old-format-lock
pid	$l_pid
start_token	lstart:old-format-token
hostname	old-host
created_at	2026-04-13T00:00:00+0000
EOF
	chmod 600 "$l_lock_dir/metadata" || fail "Unable to chmod old-format owned lock fixture metadata."
}

test_zxfer_get_process_start_token_returns_nonempty_token_for_current_process() {
	token_file="$TEST_TMPDIR/current-process.token"
	unset ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY
	# Call in this shell (not in $(...)) to prove the wrapper is sourced in a
	# subshell: its source-only guard must not reach later wrapper launches.
	zxfer_get_process_start_token "$$" >"$token_file"
	status=$?
	token=$(cat "$token_file")

	assertEquals "The current process should have a start token." 0 "$status"
	case $token in
	lstart:?* | stime:?*) token_format=ok ;;
	*) token_format=bad ;;
	esac
	assertEquals "Process-start tokens should be SELECTOR:TIME with a known selector." \
		ok "$token_format"
	assertEquals "Sourcing the wrapper must not leave its source-only guard set." \
		"" "${ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY+set}"
	assertFalse "Sourcing the wrapper must not define its helpers in this shell." \
		"command -v zxfer_cleanup_child_wrapper_get_process_start_token >/dev/null"
}

test_zxfer_get_process_start_token_covers_invalid_pids_and_selector_fallback() {
	output=$(
		(
			set +e
			zxfer_get_process_start_token "" >/dev/null
			printf 'empty_pid=%s\n' "$?"
			zxfer_get_process_start_token "invalid" >/dev/null
			printf 'invalid_pid=%s\n' "$?"
		)
	)
	fallback_output=$(
		(
			set +e
			ps() {
				if [ "$2" = "lstart=" ]; then
					printf '   \n'
				elif [ "$2" = "stime=" ]; then
					printf '  Apr 13   12:00 \n'
				else
					return 1
				fi
			}
			token=$(zxfer_get_process_start_token "$$")
			printf 'fallback=<%s>\n' "$token"
		)
	)
	failure_output=$(
		(
			set +e
			ps() {
				return 1
			}
			zxfer_get_process_start_token "$$" >/dev/null
			printf 'ps=%s\n' "$?"
		)
	)

	assertContains "Owned lock start-token lookup should reject empty PIDs." \
		"$output" "empty_pid=1"
	assertContains "Owned lock start-token lookup should reject invalid PIDs." \
		"$output" "invalid_pid=1"
	assertContains "Owned lock start-token lookup should fall back to the stime selector and normalize whitespace in pure shell." \
		"$fallback_output" "fallback=<stime:Apr 13 12:00>"
	assertContains "Owned lock start-token lookup should fail when every ps selector path fails." \
		"$failure_output" "ps=1"
}

test_zxfer_get_own_process_start_token_memoizes_one_ps_capture() {
	output=$(
		(
			set +e
			capture_count_file="$TEST_TMPDIR/own-token-captures"
			printf '0\n' >"$capture_count_file"
			zxfer_get_process_start_token() {
				l_count=$(($(cat "$capture_count_file") + 1))
				printf '%s\n' "$l_count" >"$capture_count_file"
				printf 'lstart:memo-test\n'
			}
			g_zxfer_own_process_start_token=""
			zxfer_get_own_process_start_token
			printf 'first=%s <%s>\n' "$?" "$g_zxfer_own_process_start_token"
			zxfer_get_own_process_start_token
			printf 'second=%s <%s>\n' "$?" "$g_zxfer_own_process_start_token"
			printf 'captures=%s\n' "$(cat "$capture_count_file")"
		)
	)
	failure_output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				return 1
			}
			g_zxfer_own_process_start_token=""
			zxfer_get_own_process_start_token
			printf 'status=%s memo=<%s>\n' "$?" "$g_zxfer_own_process_start_token"
		)
	)

	assertContains "The own-process start token should be captured into the memo." \
		"$output" "first=0 <lstart:memo-test>"
	assertContains "The memoized own-process start token should be reused on later calls." \
		"$output" "second=0 <lstart:memo-test>"
	assertContains "Repeated own-token lookups should not re-capture the start token." \
		"$output" "captures=1"
	assertContains "Own-token lookup should fail closed, leaving the memo empty, when no start token can be captured." \
		"$failure_output" "status=1 memo=<>"
}

test_zxfer_owned_lock_create_and_release_memoize_one_main_shell_ps_capture() {
	# Lock creation and checked release run in the main shell; the first ps
	# capture must memoize there so a create+release pair costs one probe.
	lock_dir="$TEST_TMPDIR/memo-main-shell.lock"
	capture_count_file="$TEST_TMPDIR/memo-main-shell-captures"
	printf '0\n' >"$capture_count_file"
	zxfer_reset_owned_lock_tracking
	zxfer_get_process_start_token() {
		l_count=$(($(cat "$capture_count_file") + 1))
		printf '%s\n' "$l_count" >"$capture_count_file"
		printf 'lstart:memo-main-shell\n'
	}

	zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
	create_status=$?
	memoized_token=${g_zxfer_own_process_start_token:-}
	zxfer_release_owned_lock_dir "$lock_dir"
	release_status=$?
	capture_count=$(cat "$capture_count_file")

	unset -f zxfer_get_process_start_token
	zxfer_source_runtime_modules_through "zxfer_error_log.sh"
	setUp

	assertEquals "Owned lock creation should succeed with the mocked start-token probe." \
		0 "$create_status"
	assertEquals "The first lock operation should memoize the own start token in the main shell." \
		"lstart:memo-main-shell" "$memoized_token"
	assertEquals "Checked release should succeed against the memoized token." \
		0 "$release_status"
	assertEquals "A create+release pair should spawn exactly one start-token probe." \
		1 "$capture_count"
	assertFalse "Checked release should remove the released lock directory." \
		"[ -e \"$lock_dir\" ]"
}

test_owned_lock_validation_helpers_reject_insecure_paths() {
	lock_dir="$TEST_TMPDIR/insecure.lock"
	metadata_path="$lock_dir/metadata"
	mkdir "$lock_dir" || fail "Unable to create insecure lock fixture directory."
	chmod 755 "$lock_dir" || fail "Unable to chmod insecure lock fixture directory."
	: >"$metadata_path" || fail "Unable to create insecure lock fixture metadata."
	chmod 644 "$metadata_path" || fail "Unable to chmod insecure lock fixture metadata."

	output=$(
		(
			set +e
			zxfer_validate_owned_lock_path "$lock_dir" d 700 >/dev/null
			printf 'dir_mode=%s\n' "$?"
			chmod 700 "$lock_dir"
			zxfer_validate_owned_lock_path "$lock_dir" d 700 >/dev/null
			printf 'dir_ok=%s\n' "$?"
			zxfer_validate_owned_lock_path "$metadata_path" f 600 >/dev/null
			printf 'metadata_mode=%s\n' "$?"
			chmod 600 "$metadata_path"
			zxfer_validate_owned_lock_path "$metadata_path" f 600 >/dev/null
			printf 'metadata_ok=%s\n' "$?"
			zxfer_validate_owned_lock_path "$metadata_path" d 600 >/dev/null
			printf 'file_as_dir=%s\n' "$?"
			zxfer_validate_owned_lock_path "$lock_dir" f 700 >/dev/null
			printf 'dir_as_file=%s\n' "$?"
			zxfer_validate_owned_lock_path "$lock_dir" x 700 >/dev/null
			printf 'bad_kind=%s\n' "$?"
		)
	)

	assertContains "Owned lock container validation should reject non-0700 directories." \
		"$output" "dir_mode=1"
	assertContains "Owned lock container validation should accept a private 0700 directory." \
		"$output" "dir_ok=0"
	assertContains "Owned lock metadata validation should reject non-0600 files." \
		"$output" "metadata_mode=1"
	assertContains "Owned lock metadata validation should accept a private 0600 file." \
		"$output" "metadata_ok=0"
	assertContains "Directory validation should reject regular files." \
		"$output" "file_as_dir=1"
	assertContains "File validation should reject directories." \
		"$output" "dir_as_file=1"
	assertContains "Unknown path kinds should be rejected." \
		"$output" "bad_kind=1"
}

test_zxfer_create_and_load_owned_lock_metadata_round_trip() {
	lock_dir="$TEST_TMPDIR/roundtrip.lock"
	output=$(
		(
			set +e
			zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
			printf 'create=%s\n' "$?"
			if [ -f "$lock_dir/metadata" ]; then
				printf 'metadata=yes\n'
			else
				printf 'metadata=no\n'
			fi
			zxfer_load_owned_lock_metadata_from_dir "$lock_dir"
			printf 'load=%s\n' "$?"
			printf 'pid=<%s>\n' "$g_zxfer_owned_lock_pid_result"
			printf 'start_token=<%s>\n' "$g_zxfer_owned_lock_start_token_result"
		)
	)

	assertContains "Owned lock creation should succeed for a valid metadata-backed lock dir." \
		"$output" "create=0"
	assertContains "Owned lock creation should create the metadata file." \
		"$output" "metadata=yes"
	assertContains "Owned lock metadata should reload cleanly after creation." \
		"$output" "load=0"
	assertContains "Owned lock metadata should preserve the owning pid." \
		"$output" "pid=<$$>"
	assertNotContains "Owned lock metadata should preserve a non-empty process-start token." \
		"$output" "start_token=<>"
}

test_zxfer_create_owned_lock_dir_keeps_secure_modes_and_blank_targets_fail() {
	lock_dir="$TEST_TMPDIR/modes.lock"

	output=$(
		(
			set +e
			zxfer_create_owned_lock_dir "" >/dev/null
			printf 'blank_create=%s\n' "$?"
			zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
			printf 'create=%s\n' "$?"
			printf 'dir_mode=%s\n' "$(zxfer_get_path_mode_octal "$lock_dir")"
			printf 'metadata_mode=%s\n' "$(zxfer_get_path_mode_octal "$lock_dir/metadata")"
		)
	)

	assertContains "Owned lock directory creation should reject blank target paths." \
		"$output" "blank_create=1"
	assertContains "Owned lock directory creation should succeed for valid targets." \
		"$output" "create=0"
	assertContains "Owned lock directories should be created mode 0700." \
		"$output" "dir_mode=700"
	assertContains "Owned lock metadata files should be created mode 0600." \
		"$output" "metadata_mode=600"
}

test_zxfer_create_and_load_owned_lock_metadata_cover_success_paths_in_current_shell() {
	lock_dir="$TEST_TMPDIR/direct-write-parse.lock"
	metadata_path="$lock_dir/metadata"
	tab=$(printf '\t')

	output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				printf '%s\n' "lstart:direct-test"
			}
			g_zxfer_own_process_start_token=""
			zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
			printf 'create=%s\n' "$?"
			zxfer_load_owned_lock_metadata_from_dir "$lock_dir" >/dev/null
			printf 'load=%s\n' "$?"
			printf 'pid=<%s>\n' "$g_zxfer_owned_lock_pid_result"
			printf 'start_token=<%s>\n' "$g_zxfer_owned_lock_start_token_result"
			printf 'stage=%s\n' "$([ -e "$lock_dir/.metadata.stage" ] && printf yes || printf no)"
		)
	)
	metadata_contents=$(cat "$metadata_path")

	assertContains "Owned lock creation should succeed on the direct success path." \
		"$output" "create=0"
	assertContains "Owned lock metadata loading should succeed on freshly published metadata." \
		"$output" "load=0"
	assertContains "Loaded metadata should recover the owning pid." \
		"$output" "pid=<$$>"
	assertContains "Loaded metadata should recover the start token." \
		"$output" "start_token=<lstart:direct-test>"
	assertContains "Publishing should rename the staged metadata away." \
		"$output" "stage=no"
	assertEquals "Published metadata should be exactly the three V2 lines." \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}$$
start_token${tab}lstart:direct-test" "$metadata_contents"
}

test_zxfer_create_owned_lock_dir_handles_token_write_and_publish_failures() {
	lock_dir="$TEST_TMPDIR/write-owned.lock"
	block_target_dir="$TEST_TMPDIR/block-write-target"
	mkdir -p "$block_target_dir" || fail "Unable to create the blocked metadata target."
	rm -rf "$lock_dir"

	token_output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				return 1
			}
			g_zxfer_own_process_start_token=""
			zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
			printf 'token=%s\n' "$?"
			printf 'token_exists=%s\n' "$([ -e "$lock_dir" ] && printf yes || printf no)"
		)
	)
	block_stderr="$TEST_TMPDIR/write_owned_lock_metadata.stderr"
	block_output=$(
		(
			set +e
			# Plant a symlink at the stage name right after the atomic mkdir
			# so the staged metadata write is refused.
			mkdir() {
				command mkdir "$@" || return
				ln -s "$block_target_dir" "$lock_dir/.metadata.stage"
			}
			zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
			printf 'block=%s\n' "$?"
			printf 'block_exists=%s\n' "$([ -e "$lock_dir" ] && printf yes || printf no)"
			printf 'block_target=%s\n' "$([ -d "$block_target_dir" ] && printf kept || printf gone)"
		) 2>"$block_stderr"
	)
	publish_output=$(
		(
			set +e
			mv() {
				return 1
			}
			zxfer_create_owned_lock_dir "$lock_dir" >/dev/null
			printf 'publish=%s\n' "$?"
			printf 'publish_exists=%s\n' "$([ -e "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock creation should fail closed when the current start token is unavailable." \
		"$token_output" "token=1"
	assertContains "A lock whose metadata cannot be written should be removed." \
		"$token_output" "token_exists=no"
	assertContains "Owned lock creation should fail closed when the staged metadata file cannot be written." \
		"$block_output" "block=1"
	assertContains "A lock whose staged metadata write fails should be removed with its stage entry." \
		"$block_output" "block_exists=no"
	assertContains "Removing a planted stage symlink must not follow it." \
		"$block_output" "block_target=kept"
	assertEquals "Owned lock metadata write failures should not leak raw shell redirection errors." \
		"" "$(cat "$block_stderr")"
	assertContains "Owned lock creation should fail closed when publishing the staged metadata file fails." \
		"$publish_output" "publish=1"
	assertContains "A lock whose metadata cannot be published should be removed." \
		"$publish_output" "publish_exists=no"
}

write_raw_owned_lock_fixture() {
	l_fixture_dir=$1
	l_fixture_payload=$2

	mkdir -m 700 "$l_fixture_dir" || fail "Unable to create malformed metadata fixture $l_fixture_dir."
	printf '%s\n' "$l_fixture_payload" >"$l_fixture_dir/metadata"
	chmod 600 "$l_fixture_dir/metadata" || fail "Unable to chmod malformed metadata fixture."
}

test_zxfer_create_owned_lock_dir_fails_closed_without_the_cleanup_wrapper() {
	lock_dir="$TEST_TMPDIR/no-wrapper.lock"
	rm -rf "$lock_dir"

	output=$(
		(
			set +e
			# The start token comes from the wrapper's parser; without the
			# wrapper there is no token, so no lock may be created.
			zxfer_get_cleanup_child_wrapper_script_path() {
				return 1
			}
			g_zxfer_own_process_start_token=""
			zxfer_create_owned_lock_dir "$lock_dir"
			printf 'status=%s\n' "$?"
			printf 'memo=<%s>\n' "$g_zxfer_own_process_start_token"
			[ -e "$lock_dir" ] && printf '%s\n' 'lock=present'
		)
	)

	assertContains "Lock creation should fail when the start-token wrapper cannot be found." \
		"$output" "status=1"
	assertContains "No start token should be memoized without the wrapper." \
		"$output" "memo=<>"
	assertNotContains "No lock directory may be left behind." "$output" "lock=present"
}

test_zxfer_load_owned_lock_metadata_rejects_malformed_payloads() {
	tab=$(printf '\t')
	write_raw_owned_lock_fixture "$TEST_TMPDIR/invalid-pid.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}not-a-pid
start_token${tab}lstart:test"
	write_raw_owned_lock_fixture "$TEST_TMPDIR/invalid-layout.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}123
start_token${tab}lstart:test
extra${tab}line"
	write_raw_owned_lock_fixture "$TEST_TMPDIR/invalid-no-tab.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}123
start_token lstart:test"
	write_raw_owned_lock_fixture "$TEST_TMPDIR/invalid-tab-value.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}123
start_token${tab}lstart:test${tab}extra"
	write_raw_owned_lock_fixture "$TEST_TMPDIR/invalid-key.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}123
starting_token${tab}lstart:test"
	write_raw_owned_lock_fixture "$TEST_TMPDIR/short.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}123"
	write_raw_owned_lock_fixture "$TEST_TMPDIR/empty-token.lock" \
		"$ZXFER_LOCK_METADATA_HEADER
pid${tab}123
start_token${tab}"

	output=$(
		(
			set +e
			for fixture in invalid-pid invalid-layout invalid-no-tab invalid-tab-value invalid-key short empty-token; do
				g_zxfer_owned_lock_pid_result=stale
				g_zxfer_owned_lock_start_token_result=stale
				zxfer_load_owned_lock_metadata_from_dir "$TEST_TMPDIR/$fixture.lock" >/dev/null
				printf '%s=%s pid=<%s> token=<%s>\n' "$fixture" "$?" \
					"$g_zxfer_owned_lock_pid_result" "$g_zxfer_owned_lock_start_token_result"
			done
		)
	)

	assertContains "Owned lock metadata loading should reject nonnumeric PIDs as corrupt." \
		"$output" "invalid-pid=2 pid=<> token=<>"
	assertContains "Owned lock metadata loading should reject unexpected extra lines as corrupt." \
		"$output" "invalid-layout=2 pid=<> token=<>"
	assertContains "Owned lock metadata loading should reject field rows without the tab separator." \
		"$output" "invalid-no-tab=2 pid=<> token=<>"
	assertContains "Owned lock metadata loading should reject field values that contain tabs." \
		"$output" "invalid-tab-value=2 pid=<> token=<>"
	assertContains "Owned lock metadata loading should reject unknown metadata keys." \
		"$output" "invalid-key=2 pid=<> token=<>"
	assertContains "Owned lock metadata loading should reject truncated metadata files." \
		"$output" "short=2 pid=<> token=<>"
	assertContains "Owned lock metadata loading should reject an empty start token." \
		"$output" "empty-token=2 pid=<> token=<>"
}

test_zxfer_load_owned_lock_metadata_helpers_distinguish_missing_and_malformed() {
	missing_dir="$TEST_TMPDIR/missing-metadata.lock"
	malformed_dir="$TEST_TMPDIR/malformed-metadata.lock"

	mkdir "$missing_dir" "$malformed_dir" || fail "Unable to create owned lock metadata loader fixtures."
	chmod 700 "$missing_dir" "$malformed_dir" || fail "Unable to chmod owned lock metadata loader fixtures."
	cat >"$malformed_dir/metadata" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	123
starting_token	lstart:test
EOF
	chmod 600 "$malformed_dir/metadata" || fail "Unable to chmod malformed owned lock metadata loader fixture."

	output=$(
		(
			set +e
			zxfer_load_owned_lock_metadata_from_dir "$missing_dir" >/dev/null
			printf 'missing=%s\n' "$?"
			zxfer_load_owned_lock_metadata_from_dir "$malformed_dir" >/dev/null
			printf 'malformed=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata loading should treat missing metadata files as corrupt or incomplete state." \
		"$output" "missing=2"
	assertContains "Owned lock metadata loading should treat malformed metadata payloads as corrupt state." \
		"$output" "malformed=2"
}

test_old_format_owned_lock_metadata_is_treated_as_corrupt_and_reaped_per_policy() {
	old_format_dir="$TEST_TMPDIR/old-format.lock"
	write_old_format_owned_lock_metadata_fixture "$old_format_dir" "$$"

	output=$(
		(
			set +e
			zxfer_load_owned_lock_metadata_from_dir "$old_format_dir" >/dev/null
			printf 'load=%s\n' "$?"
			zxfer_try_reap_stale_owned_lock_dir "$old_format_dir" 0 >/dev/null
			printf 'defer=%s\n' "$?"
			printf 'defer_exists=%s\n' "$([ -d "$old_format_dir" ] && printf yes || printf no)"
			zxfer_try_reap_stale_owned_lock_dir "$old_format_dir" 1 >/dev/null
			printf 'reap=%s\n' "$?"
			printf 'reap_exists=%s\n' "$([ -e "$old_format_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Old-format (V1) owned lock metadata should load as corrupt, never crash." \
		"$output" "load=2"
	assertContains "Old-format owned lock dirs should defer reaping without the corrupt-reap policy." \
		"$output" "defer=2"
	assertContains "Deferred old-format owned lock dirs should remain in place." \
		"$output" "defer_exists=yes"
	assertContains "Old-format owned lock dirs should be reaped once corrupt cleanup is enabled." \
		"$output" "reap=0"
	assertContains "Reaped old-format owned lock dirs should be removed." \
		"$output" "reap_exists=no"
}

test_zxfer_try_reap_stale_owned_lock_dir_distinguishes_live_and_stale_owners() {
	live_lock_dir="$TEST_TMPDIR/live.lock"
	stale_lock_dir="$TEST_TMPDIR/stale.lock"

	zxfer_create_owned_lock_dir "$live_lock_dir" >/dev/null
	write_owned_lock_metadata_fixture "$stale_lock_dir" "999999999"

	output=$(
		(
			set +e
			zxfer_try_reap_stale_owned_lock_dir "$live_lock_dir" 1 >/dev/null
			printf 'live=%s\n' "$?"
			printf 'live_exists=%s\n' "$([ -d "$live_lock_dir" ] && printf yes || printf no)"
			zxfer_try_reap_stale_owned_lock_dir "$stale_lock_dir" 1 >/dev/null
			printf 'stale=%s\n' "$?"
			printf 'stale_exists=%s\n' "$([ -e "$stale_lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Live owned lock dirs should report as busy instead of being reaped." \
		"$output" "live=2"
	assertContains "Live owned lock dirs should remain in place." \
		"$output" "live_exists=yes"
	assertContains "Stale owned lock dirs should be reaped." \
		"$output" "stale=0"
	assertContains "Reaped stale owned lock dirs should be removed." \
		"$output" "stale_exists=no"
}

test_zxfer_try_reap_stale_owned_lock_dir_defers_and_then_reaps_corrupt_entries() {
	lock_dir="$TEST_TMPDIR/corrupt.lock"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	output=$(
		(
			set +e
			zxfer_try_reap_stale_owned_lock_dir "$lock_dir" 0 >/dev/null
			printf 'defer=%s\n' "$?"
			zxfer_try_reap_stale_owned_lock_dir "$lock_dir" 1 >/dev/null
			printf 'reap=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Corrupt owned lock dirs should defer reaping until the caller enables corrupt cleanup." \
		"$output" "defer=2"
	assertContains "Corrupt owned lock dirs should be reaped once corrupt cleanup is enabled." \
		"$output" "reap=0"
	assertContains "Reaped corrupt owned lock dirs should be removed." \
		"$output" "exists=no"
}

test_owned_lock_reap_and_cleanup_helpers_cover_stale_unknown_and_invalid_targets() {
	current_token=$(zxfer_get_process_start_token "$$") ||
		fail "Unable to derive owned lock test start token."
	file_path="$TEST_TMPDIR/not-a-lock-file"
	target_dir="$TEST_TMPDIR/cleanup-target.lock"
	link_path="$TEST_TMPDIR/cleanup-link.lock"
	: >"$file_path" || fail "Unable to create owned lock cleanup file fixture."
	mkdir "$target_dir" || fail "Unable to create owned lock cleanup target directory."
	chmod 700 "$target_dir"
	ln -s "$target_dir" "$link_path" || fail "Unable to create owned lock cleanup symlink."
	write_owned_lock_metadata_fixture "$TEST_TMPDIR/dead-owner.lock" "999999999" "$current_token"
	write_owned_lock_metadata_fixture "$TEST_TMPDIR/reused-pid.lock" "$$" "${current_token}mismatch"
	write_owned_lock_metadata_fixture "$TEST_TMPDIR/live-owner.lock" "$$" "$current_token"
	write_owned_lock_metadata_fixture "$TEST_TMPDIR/unknown-owner.lock" "$$" "$current_token"

	liveness_output=$(
		(
			set +e
			zxfer_try_reap_stale_owned_lock_dir "$TEST_TMPDIR/dead-owner.lock" >/dev/null
			printf 'dead=%s\n' "$?"
			zxfer_try_reap_stale_owned_lock_dir "$TEST_TMPDIR/reused-pid.lock" >/dev/null
			printf 'token_mismatch=%s\n' "$?"
			zxfer_try_reap_stale_owned_lock_dir "$TEST_TMPDIR/live-owner.lock" >/dev/null
			printf 'live=%s\n' "$?"
			printf 'live_exists=%s\n' "$([ -d "$TEST_TMPDIR/live-owner.lock" ] && printf yes || printf no)"
		)
	)
	unknown_output=$(
		(
			set +e
			kill() {
				return 0
			}
			zxfer_get_process_start_token() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir "$TEST_TMPDIR/unknown-owner.lock" >/dev/null
			printf 'unknown=%s\n' "$?"
		)
	)
	cleanup_output=$(
		(
			set +e
			zxfer_cleanup_owned_lock_dir "" >/dev/null
			printf 'blank=%s\n' "$?"
			zxfer_cleanup_owned_lock_dir "$TEST_TMPDIR/missing.lock" >/dev/null
			printf 'missing=%s\n' "$?"
			zxfer_cleanup_owned_lock_dir "$link_path" >/dev/null
			printf 'symlink=%s\n' "$?"
			zxfer_cleanup_owned_lock_dir "$file_path" >/dev/null
			printf 'file=%s\n' "$?"
			mkdir -m 700 "$TEST_TMPDIR/rm-fallback.lock" || exit 1
			rmdir() {
				command rmdir "$1"
				return 1
			}
			zxfer_cleanup_owned_lock_dir "$TEST_TMPDIR/rm-fallback.lock" >/dev/null
			printf 'rm_fallback=%s\n' "$?"
		)
	)

	assertContains "Reaping should remove a lock whose owner pid is gone." \
		"$liveness_output" "dead=0"
	assertContains "Reaping should remove a lock whose live pid has a different start token." \
		"$liveness_output" "token_mismatch=0"
	assertContains "Reaping should report a lock held by a live owner as busy." \
		"$liveness_output" "live=2"
	assertContains "A busy lock should stay in place." \
		"$liveness_output" "live_exists=yes"
	assertContains "Reaping should fail closed when a live pid's start token cannot be read." \
		"$unknown_output" "unknown=1"
	assertContains "Owned lock cleanup should ignore blank targets." \
		"$cleanup_output" "blank=0"
	assertContains "Owned lock cleanup should ignore missing targets." \
		"$cleanup_output" "missing=0"
	assertContains "Owned lock cleanup should reject symlink targets." \
		"$cleanup_output" "symlink=1"
	assertContains "Owned lock cleanup should reject non-directory targets." \
		"$cleanup_output" "file=1"
	assertContains "Owned lock cleanup should still succeed when rmdir reports failure but the directory is already gone by the post-check." \
		"$cleanup_output" "rm_fallback=0"
}

test_owned_lock_cleanup_fails_closed_when_rm_failures_persist() {
	lock_dir="$TEST_TMPDIR/cleanup-hard-fail.lock"
	mkdir -m 700 "$lock_dir" || fail "Unable to create hard-fail owned lock cleanup fixture."

	cleanup_output=$(
		(
			set +e
			rmdir() {
				return 1
			}
			zxfer_cleanup_owned_lock_dir "$lock_dir" >/dev/null
			printf 'cleanup=%s\n' "$?"
			printf 'exists=%s\n' "$([ -d "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock cleanup should fail when rmdir reports failure and the lock directory still exists afterward." \
		"$cleanup_output" "cleanup=1"
	assertContains "Owned lock cleanup failure paths should leave the existing directory in place for inspection." \
		"$cleanup_output" "exists=yes"
}

test_zxfer_cleanup_owned_lock_dir_revalidates_and_removes_only_known_entries() {
	arbitrary_dir="$TEST_TMPDIR/arbitrary-cleanup-target.lock"
	wrong_mode_dir="$TEST_TMPDIR/wrong-mode-cleanup.lock"
	wrong_owner_dir="$TEST_TMPDIR/wrong-owner-cleanup.lock"
	mismatch_dir="$TEST_TMPDIR/metadata-mismatch-cleanup.lock"
	symlink_dir="$TEST_TMPDIR/symlink-metadata-cleanup.lock"
	hardlink_dir="$TEST_TMPDIR/hardlink-metadata-cleanup.lock"
	external_symlink_target="$TEST_TMPDIR/external-symlink-target"
	external_hardlink_target="$TEST_TMPDIR/external-hardlink-target"

	mkdir -m 700 "$arbitrary_dir" "$wrong_owner_dir" "$symlink_dir" "$hardlink_dir"
	: >"$arbitrary_dir/operator-data"
	mkdir -m 755 "$wrong_mode_dir"
	write_owned_lock_metadata_fixture "$mismatch_dir"
	printf '%s\n' symlink-sentinel >"$external_symlink_target"
	ln -s "$external_symlink_target" "$symlink_dir/metadata"
	printf '%s\n' hardlink-sentinel >"$external_hardlink_target"
	ln "$external_hardlink_target" "$hardlink_dir/metadata"

	zxfer_cleanup_owned_lock_dir "$arbitrary_dir" >/dev/null
	arbitrary_status=$?
	zxfer_cleanup_owned_lock_dir "$wrong_mode_dir" >/dev/null
	wrong_mode_status=$?
	wrong_owner_status=$(
		(
			zxfer_get_effective_user_uid() { printf '%s\n' 100; }
			zxfer_get_path_owner_uid() { printf '%s\n' 101; }
			zxfer_cleanup_owned_lock_dir "$wrong_owner_dir" >/dev/null
			printf '%s\n' "$?"
		)
	)
	zxfer_cleanup_owned_lock_dir "$mismatch_dir" 999999 "lstart:not-the-owner" >/dev/null
	mismatch_status=$?
	zxfer_cleanup_owned_lock_dir "$symlink_dir" >/dev/null
	symlink_status=$?
	zxfer_cleanup_owned_lock_dir "$hardlink_dir" >/dev/null
	hardlink_status=$?

	assertEquals "Cleanup should reject arbitrary directories containing unknown entries." 1 "$arbitrary_status"
	assertTrue "Rejected arbitrary directories should retain operator data." \
		"[ -f '$arbitrary_dir/operator-data' ]"
	assertEquals "Cleanup should reject a lock container whose mode is not 0700." 1 "$wrong_mode_status"
	assertTrue "Wrong-mode lock containers should be leaked intact." "[ -d '$wrong_mode_dir' ]"
	assertEquals "Cleanup should reject a lock container not owned by the effective uid." 1 "$wrong_owner_status"
	assertTrue "Wrong-owner lock containers should be leaked intact." "[ -d '$wrong_owner_dir' ]"
	assertEquals "Cleanup should reject metadata that changed after caller ownership proof." 1 "$mismatch_status"
	assertTrue "Metadata-mismatch lock containers should be leaked intact." "[ -d '$mismatch_dir' ]"
	assertEquals "Known-name metadata symlinks may be unlinked without following them." 0 "$symlink_status"
	assertEquals "Removing a metadata symlink must preserve its external target." \
		"symlink-sentinel" "$(cat "$external_symlink_target")"
	assertEquals "Known-name metadata hard links may be unlinked without recursive deletion." 0 "$hardlink_status"
	assertEquals "Removing a metadata hard link must preserve its external inode through the other link." \
		"hardlink-sentinel" "$(cat "$external_hardlink_target")"
}

test_zxfer_create_owned_lock_dir_failure_paths_clean_up_partial_directories() {
	validate_lock_dir="$TEST_TMPDIR/validate-fail.lock"
	write_lock_dir="$TEST_TMPDIR/write-fail.lock"

	validate_output=$(
		(
			set +e
			zxfer_validate_owned_lock_path() {
				return 1
			}
			zxfer_create_owned_lock_dir "$validate_lock_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$validate_lock_dir" ] && printf yes || printf no)"
		)
	)
	write_output=$(
		(
			set +e
			zxfer_get_own_process_start_token() {
				return 1
			}
			zxfer_create_owned_lock_dir "$write_lock_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$write_lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock creation should fail closed when the created directory cannot be revalidated." \
		"$validate_output" "status=1"
	assertContains "Owned lock creation should leak rather than delete a directory that cannot be revalidated safely." \
		"$validate_output" "exists=yes"
	assertContains "Owned lock creation should fail closed when metadata publication fails." \
		"$write_output" "status=1"
	assertContains "Owned lock creation should remove directories whose metadata write fails." \
		"$write_output" "exists=no"
}

test_zxfer_try_reap_stale_owned_lock_dir_propagates_unknown_states_and_cleanup_failures() {
	liveness_dir="$TEST_TMPDIR/reap-liveness.lock"
	cleanup_dir="$TEST_TMPDIR/reap-cleanup.lock"
	write_owned_lock_metadata_fixture "$liveness_dir"
	write_owned_lock_metadata_fixture "$cleanup_dir" "999999999"

	liveness_output=$(
		(
			set +e
			kill() {
				return 0
			}
			zxfer_get_process_start_token() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir "$liveness_dir" 1 >/dev/null
			printf 'liveness=%s\n' "$?"
		)
	)
	cleanup_output=$(
		(
			set +e
			zxfer_cleanup_owned_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir "$cleanup_dir" 1 >/dev/null
			printf 'cleanup=%s\n' "$?"
		)
	)
	unknown_load_output=$(
		(
			set +e
			zxfer_load_owned_lock_metadata_from_dir() {
				return 7
			}
			zxfer_try_reap_stale_owned_lock_dir "$TEST_TMPDIR/unknown-load.lock" 1 >/dev/null
			printf 'unknown=%s\n' "$?"
		)
	)
	hard_load_output=$(
		(
			set +e
			zxfer_load_owned_lock_metadata_from_dir() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir "$TEST_TMPDIR/hard-load.lock" 1 >/dev/null
			printf 'hard=%s\n' "$?"
		)
	)

	assertContains "Owned lock reaping should fail closed when live-owner validation is inconclusive." \
		"$liveness_output" "liveness=1"
	assertContains "Owned lock reaping should fail closed when cleanup of a stale entry fails." \
		"$cleanup_output" "cleanup=1"
	assertContains "Owned lock reaping should fail closed on unexpected metadata-loader statuses." \
		"$unknown_load_output" "unknown=1"
	assertContains "Owned lock reaping should preserve hard metadata-validation failures." \
		"$hard_load_output" "hard=1"
}

test_zxfer_release_owned_lock_dir_requires_current_owner_identity() {
	lock_dir="$TEST_TMPDIR/release-mismatch.lock"
	write_owned_lock_metadata_fixture \
		"$lock_dir" "$$" "lstart:not-the-current-process"

	output=$(
		(
			set +e
			zxfer_release_owned_lock_dir "$lock_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -d "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock release should fail when the current process identity does not match the metadata owner." \
		"$output" "status=1"
	assertContains "Failed owned lock release should preserve the directory for later inspection." \
		"$output" "exists=yes"
}

test_zxfer_release_owned_lock_dir_never_releases_live_foreign_pids() {
	lock_dir="$TEST_TMPDIR/release-foreign.lock"
	# PID 1 is always live and never this test process.
	write_owned_lock_metadata_fixture "$lock_dir" "1" "lstart:foreign-owner"

	output=$(
		(
			set +e
			zxfer_release_owned_lock_dir "$lock_dir" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -d "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock release should refuse locks recorded for another pid." \
		"$output" "status=1"
	assertContains "Owned lock release should preserve foreign-owned lock directories." \
		"$output" "exists=yes"
}

test_owned_lock_validation_and_release_helpers_cover_lookup_failures() {
	lock_dir="$TEST_TMPDIR/lookup-fail.lock"
	metadata_path="$lock_dir/metadata"
	write_owned_lock_metadata_fixture "$lock_dir"

	uid_output=$(
		(
			set +e
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_validate_owned_lock_path "$metadata_path" f 600 >/dev/null
			printf 'uid=%s\n' "$?"
		)
	)
	owner_output=$(
		(
			set +e
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_validate_owned_lock_path "$metadata_path" f 600 >/dev/null
			printf 'owner=%s\n' "$?"
		)
	)
	mode_output=$(
		(
			set +e
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_validate_owned_lock_path "$metadata_path" f 600 >/dev/null
			printf 'mode=%s\n' "$?"
		)
	)
	start_token_output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				return 1
			}
			g_zxfer_own_process_start_token=""
			zxfer_release_owned_lock_dir "$lock_dir" >/dev/null
			printf 'token=%s\n' "$?"
			printf 'token_exists=%s\n' "$([ -d "$lock_dir" ] && printf yes || printf no)"
		)
	)
	load_failure_output=$(
		(
			set +e
			mkdir -m 700 "$TEST_TMPDIR/metadata-less-owner.lock" || exit 1
			zxfer_release_owned_lock_dir "$TEST_TMPDIR/metadata-less-owner.lock" >/dev/null
			printf 'load=%s\n' "$?"
		)
	)
	release_output=$(
		(
			set +e
			zxfer_cleanup_owned_lock_dir() {
				return 1
			}
			zxfer_release_owned_lock_dir "$lock_dir" >/dev/null
			printf 'release=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata validation should fail closed when effective-uid lookup fails." \
		"$uid_output" "uid=1"
	assertContains "Owned lock metadata validation should fail closed when owner lookup fails." \
		"$owner_output" "owner=1"
	assertContains "Owned lock metadata validation should fail closed when mode lookup fails." \
		"$mode_output" "mode=1"
	assertContains "Release should fail closed when the own start token cannot be read." \
		"$start_token_output" "token=1"
	assertContains "Release should keep the lock when the own start token cannot be read." \
		"$start_token_output" "token_exists=yes"
	assertContains "Release should fail closed when the lock metadata cannot be loaded." \
		"$load_failure_output" "load=1"
	assertContains "Owned lock release should fail closed when directory cleanup fails after ownership validation succeeds." \
		"$release_output" "release=1"
}

# Make a log parent read-only for this user. When the user can still write
# it (root, or an ACL), restore it and skip the test, which needs a parent
# that refuses the atomic-rename path.
make_error_log_parent_read_only() {
	chmod 500 "$1" || fail "Unable to make $1 read-only."
	if [ -w "$1" ]; then
		chmod 700 "$1"
		startSkipping
		return 1
	fi
}

test_zxfer_append_failure_report_to_log_rejects_direct_symlink_target_when_component_scan_does_not_fire() {
	log_target="$TEST_TMPDIR/failure-direct-target.log"
	log_symlink="$TEST_TMPDIR/failure-direct-link.log"

	: >"$log_target"
	ln -s "$log_target" "$log_symlink"

	zxfer_test_capture_subshell "
		zxfer_find_symlink_path_component() {
			return 1
		}
		ZXFER_ERROR_LOG=\"$log_symlink\" \\
			zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Direct symlinked error-log targets should still be rejected when path-component scanning does not catch them first." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Direct symlinked error-log target rejection should explain the refusal." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "because it is a symlink"
}

test_zxfer_append_failure_report_to_log_warns_when_append_fails() {
	log_path="$TEST_TMPDIR/append-failure.log"
	stdout_file="$TEST_TMPDIR/append_failure.stdout"
	stderr_file="$TEST_TMPDIR/append_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		printf() {
			if [ \"\$2\" = \"append-failure-report\" ]; then
				return 1
			fi
			command printf \"\$@\"
		}
		zxfer_append_failure_report_to_log \"append-failure-report\"
	"

	assertEquals "Append failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Append failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
	assertEquals "Append failures should not write partial report data." \
		"" "$(cat "$log_path")"
}

test_zxfer_append_failure_report_to_log_appends_existing_file_when_parent_is_not_writable() {
	log_dir="$TEST_TMPDIR/nonwritable-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_parent.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_parent.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Existing secure ZXFER_ERROR_LOG files should still append cleanly when the trusted parent is not writable." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Non-writable trusted-parent appends should not emit warnings." \
		"" "$(cat "$stderr_file")"
	assertContains "Non-writable trusted-parent appends should preserve prior contents." \
		"$(cat "$log_path")" "existing: keep-me"
	assertContains "Non-writable trusted-parent appends should add the new report payload." \
		"$(cat "$log_path")" "message: appended-report"
}

test_zxfer_append_failure_report_to_log_rechecks_existence_under_lock_before_creating() {
	# A concurrent winner can create the log while this run waits on the
	# error-log lock; the stale pre-lock existence answer must not route the
	# waiting run onto the create path, which would clobber the winner's
	# freshly published report with an empty staged file.
	log_path="$TEST_TMPDIR/recheck-under-lock.log"
	rm -f "$log_path"

	output=$(
		(
			ZXFER_ERROR_LOG="$log_path"
			zxfer_acquire_error_log_lock() {
				# Simulate the concurrent winner publishing the log while
				# this run was blocked on lock acquisition.
				printf 'winner: report-kept\n' >"$log_path"
				chmod 600 "$log_path"
				return 0
			}
			set +e
			zxfer_append_failure_report_to_log "loser: report-appended"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Appending after a lock wait should still succeed." \
		"$output" "status=0"
	assertContains "The concurrent winner's report must survive the recheck (no create-path clobber)." \
		"$(cat "$log_path")" "winner: report-kept"
	assertContains "The waiting run's report must append behind the winner's content." \
		"$(cat "$log_path")" "loser: report-appended"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_system_tmp_fallback_chain() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			case "$1" in
			"/unsafe-tmpdir"|"/dev/shm"|"/run/shm")
				return 1
				;;
			"/tmp")
				printf "%s\n" "/tmp"
				return 0
				;;
			esac
			return 1
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "/tmp/.zxfer-error-log.lock.d/prepared/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /tmp is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the prepared exact lock path under the first safe tmpdir candidate." \
		"/tmp/.zxfer-error-log.lock.d/prepared/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_dev_shm_fallback_when_available() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			case "$1" in
			"/dev/shm"|"/run/shm"|"/tmp")
				printf "%s\n" "$1"
				return 0
				;;
			esac
			return 1
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "$1/.zxfer-error-log.lock.d/prepared-for:$2/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /dev/shm is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should prepare the lock for the log under /dev/shm when TMPDIR is unsafe." \
		"/dev/shm/.zxfer-error-log.lock.d/prepared-for:/tmp/failure.log/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_uses_run_shm_fallback_when_dev_shm_is_unavailable() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			case "$1" in
			"/run/shm"|"/tmp")
				printf "%s\n" "$1"
				return 0
				;;
			esac
			return 1
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "$1/.zxfer-error-log.lock.d/prepared/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when /run/shm is the first safe system tmpdir candidate." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the prepared exact lock path under /run/shm when /dev/shm is unavailable." \
		"/run/shm/.zxfer-error-log.lock.d/prepared/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_prefers_a_trusted_tmpdir() {
	zxfer_test_capture_subshell '
		TMPDIR="/trusted tmp"
		zxfer_validate_temp_root_candidate() {
			printf "%s\n" "/physical$1"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf "%s\n" "$1/.zxfer-error-log.lock.d/prepared/lock"
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should succeed when TMPDIR is trusted." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should use the validated physical TMPDIR, spaces intact, before system candidates." \
		"/physical/trusted tmp/.zxfer-error-log.lock.d/prepared/lock" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_skips_an_empty_tmpdir() {
	candidates_log="$TEST_TMPDIR/empty-tmpdir-candidates.log"
	rm -f "$candidates_log"

	zxfer_test_capture_subshell "
		TMPDIR=''
		zxfer_validate_temp_root_candidate() {
			printf 'candidate=<%s>\n' \"\$1\" >>'$candidates_log'
			[ \"\$1\" = /tmp ] || return 1
			printf '%s\n' \"\$1\"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf '%s\n' \"\$1/lock\"
		}
		zxfer_get_error_log_fallback_lock_dir /tmp/failure.log
	"

	assertEquals "Fallback lock-dir lookup should succeed through the system candidates." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "An empty TMPDIR should not be validated as a candidate." \
		"candidate=</dev/shm>
candidate=</run/shm>
candidate=</tmp>" "$(cat "$candidates_log")"
}

test_zxfer_get_error_log_fallback_lock_dir_returns_failure_when_no_safe_tmpdir_exists() {
	zxfer_test_capture_subshell '
		TMPDIR="/unsafe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should fail closed when no safe temp-root candidate exists." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed fallback lock-dir lookups should not emit a path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_get_error_log_fallback_lock_dir_returns_failure_when_lock_path_prepare_fails() {
	zxfer_test_capture_subshell '
		TMPDIR="/safe-tmpdir"
		zxfer_validate_temp_root_candidate() {
			printf "%s\n" "/safe-tmpdir"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir "/tmp/failure.log"
	'

	assertEquals "Fallback lock-dir lookup should fail when the exact lock path cannot be prepared." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed fallback lock-dir lookups should not emit a partial path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_ensure_error_log_fallback_lock_component_dir_rejects_symlinks() {
	component_target="$TEST_TMPDIR/error-log-component-target"
	component_link="$TEST_TMPDIR/error-log-component-link"
	mkdir "$component_target"
	ln -s "$component_target" "$component_link"

	set +e
	zxfer_ensure_error_log_fallback_lock_component_dir "$component_link"
	component_status=$?

	assertEquals "Fallback error-log lock components must reject symlinks before changing their mode or contents." \
		1 "$component_status"
}

test_zxfer_error_log_lock_identity_hex_fails_when_hex_encoding_is_empty() {
	output=$(
		(
			set +e
			od() {
				:
			}
			zxfer_error_log_lock_identity_hex "/tmp/failure.log" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Error-log lock identity derivation should fail closed when exact hex encoding produces no output." \
		"$output" "status=1"
}

test_zxfer_error_log_lock_identity_hex_uses_exact_hex_in_current_shell() {
	od() {
		printf ' 66 6f 6f 0a \n'
	}
	identity_hex=$(zxfer_error_log_lock_identity_hex "/tmp/failure.log")
	unset -f od

	assertEquals "Current-shell error-log lock identity should use exact lowercase hex from od." \
		"666f6f0a" "$identity_hex"
}

test_zxfer_prepare_error_log_fallback_lock_dir_distinguishes_known_legacy_cksum_collision_paths() {
	path_one="/var/log/zxfer-3kzpfymt.log"
	path_two="/var/log/zxfer-amu2x4ex.log"

	lock_one=$(zxfer_prepare_error_log_fallback_lock_dir "$TEST_TMPDIR" "$path_one") ||
		fail "Expected fallback lock preparation to succeed for the first legacy collision path."
	lock_two=$(zxfer_prepare_error_log_fallback_lock_dir "$TEST_TMPDIR" "$path_two") ||
		fail "Expected fallback lock preparation to succeed for the second legacy collision path."

	assertNotEquals "Fallback error-log lock paths should not collapse known legacy cksum-collision log paths." \
		"$lock_one" "$lock_two"
	assertTrue "Fallback lock preparation should create the exact lock parent directory." \
		"[ -d \"${lock_one%/lock}\" ]"
}

test_zxfer_acquire_error_log_lock_retries_before_failing() {
	zxfer_test_capture_subshell '
		g_test_sleep_calls=0
		mkdir() {
			return 1
		}
		sleep() {
			g_test_sleep_calls=$((g_test_sleep_calls + 1))
			return 0
		}
		zxfer_acquire_error_log_lock "/tmp/lock-dir"
		l_status=$?
		printf "status=%s\n" "$l_status"
		printf "sleeps=%s\n" "$g_test_sleep_calls"
		[ "$l_status" -eq 1 ]
	'

	assertEquals "Repeated lock-dir creation failures should eventually return a non-zero status." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Lock acquisition should report the expected failure status after exhausting retries." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=1"
	assertContains "Lock acquisition should sleep between failed retries before giving up." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "sleeps=2"
}

test_zxfer_acquire_error_log_lock_reaps_stale_lock_and_retries_successfully() {
	zxfer_test_capture_subshell '
		g_test_create_calls=0
		g_test_reap_calls=0
		zxfer_create_owned_lock_dir() {
			g_test_create_calls=$((g_test_create_calls + 1))
			if [ "$g_test_create_calls" -eq 1 ]; then
				mkdir -p "$1"
				return 1
			fi
			return 0
		}
		zxfer_try_reap_stale_owned_lock_dir() {
			g_test_reap_calls=$((g_test_reap_calls + 1))
			rm -rf "$1"
			return 0
		}
		zxfer_acquire_error_log_lock "'"$TEST_TMPDIR"'/reapable.lock"
		printf "status=%s\n" "$?"
		printf "creates=%s\n" "$g_test_create_calls"
		printf "reaps=%s\n" "$g_test_reap_calls"
	'

	assertContains "Error-log lock acquisition should succeed after reaping one stale lock directory." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=0"
	assertContains "Error-log lock acquisition should retry lock creation after a successful stale-lock reap." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "creates=2"
	assertContains "Error-log lock acquisition should attempt exactly one stale-lock reap in this path." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "reaps=1"
}

test_zxfer_acquire_error_log_lock_defers_corrupt_reap_until_recheck_round() {
	# A 0700 lock dir without metadata models a live winner inside its
	# mkdir-to-metadata publish window: the first sighting must be treated as
	# busy (lock preserved across the sleep) and the corrupt reap may only
	# happen after the recheck still reports corrupt metadata.
	lock_dir="$TEST_TMPDIR/midpublish.lock"
	mkdir -m 700 "$lock_dir" || fail "Unable to create the mid-publish lock fixture."
	g_test_sleep_calls=0
	g_test_lock_present_at_sleep=no
	sleep() {
		g_test_sleep_calls=$((g_test_sleep_calls + 1))
		if [ -d "$lock_dir" ]; then
			g_test_lock_present_at_sleep=yes
		fi
		return 0
	}

	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?
	unset -f sleep

	assertEquals "Acquisition should still succeed after the corrupt recheck round reaps the metadata-less lock." \
		0 "$status"
	assertEquals "The metadata-less lock must survive the first sighting (treated as busy, not reaped)." \
		yes "$g_test_lock_present_at_sleep"
	assertEquals "Exactly one recheck sleep should separate the corrupt sighting from the corrupt reap." \
		1 "$g_test_sleep_calls"
	assertTrue "The acquired lock should carry this process's published metadata." \
		"[ -f \"$lock_dir/metadata\" ]"

	zxfer_release_error_log_lock "$lock_dir" ||
		fail "Unable to release the acquired error-log lock fixture."
}

test_zxfer_acquire_error_log_lock_fails_closed_when_stale_reap_errors() {
	zxfer_test_capture_subshell '
		lock_dir="'"$TEST_TMPDIR"'/reap_error.lock"
		mkdir -p "$lock_dir"
		zxfer_create_owned_lock_dir() {
			return 1
		}
		zxfer_try_reap_stale_owned_lock_dir() {
			return 1
		}
		zxfer_acquire_error_log_lock "$lock_dir"
	'

	assertEquals "Error-log lock acquisition should fail closed when stale-lock reaping errors." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_zxfer_acquire_error_log_lock_covers_stale_reap_error_in_current_shell() {
	lock_dir="$TEST_TMPDIR/reap_error_current.lock"
	mkdir -p "$lock_dir" || fail "Unable to create stale error-log lock directory."
	g_test_sleep_calls=0

	zxfer_create_owned_lock_dir() {
		return 1
	}
	zxfer_try_reap_stale_owned_lock_dir() {
		return 1
	}
	sleep() {
		g_test_sleep_calls=$((g_test_sleep_calls + 1))
		return 0
	}

	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?
	sleep_calls=$g_test_sleep_calls
	unset -f zxfer_create_owned_lock_dir zxfer_try_reap_stale_owned_lock_dir sleep
	zxfer_source_runtime_modules_through "zxfer_error_log.sh"

	assertEquals "Current-shell error-log lock acquisition should fail closed when stale-lock reaping errors." \
		1 "$status"
	assertEquals "Current-shell error-log lock acquisition should not retry hard stale-lock reap errors." \
		0 "$sleep_calls"
}

test_zxfer_release_error_log_lock_warns_and_returns_failure() {
	log_path="$TEST_TMPDIR/release_failure.log"
	lock_dir="$TEST_TMPDIR/release_failure.lock"
	stdout_file="$TEST_TMPDIR/release_failure.stdout"
	stderr_file="$TEST_TMPDIR/release_failure.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		zxfer_release_owned_lock_dir() {
			[ \"\$1\" = '$lock_dir' ] || return 99
			return 23
		}
		zxfer_release_error_log_lock '$log_path' '$lock_dir'
	"

	assertEquals "Error-log lock release should fail closed when the owned-lock release fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Error-log lock release failures should emit the documented warning with the owned-lock status." \
		"$(cat "$stderr_file")" "unable to release ZXFER_ERROR_LOG lock for \"$log_path\" (status 23)"
}

test_zxfer_release_error_log_lock_is_silent_on_success() {
	log_path="$TEST_TMPDIR/release_success.log"
	lock_dir="$TEST_TMPDIR/release_success.lock"
	stdout_file="$TEST_TMPDIR/release_success.stdout"
	stderr_file="$TEST_TMPDIR/release_success.stderr"
	rm -rf "$lock_dir"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		zxfer_create_owned_lock_dir '$lock_dir' >/dev/null || exit 9
		zxfer_release_error_log_lock '$log_path' '$lock_dir'
	"

	assertEquals "Releasing a held error-log lock should succeed." 0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A successful release should not warn." "" "$(cat "$stderr_file")"
	assertFalse "A successful release should remove the lock directory." "[ -e '$lock_dir' ]"
}

test_zxfer_append_failure_report_to_log_keeps_the_append_failure_when_release_also_fails() {
	log_path="$TEST_TMPDIR/append-and-release-failure.log"
	stdout_file="$TEST_TMPDIR/append_and_release_failure.stdout"
	stderr_file="$TEST_TMPDIR/append_and_release_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG='$log_path'
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_release_owned_lock_dir() {
			return 5
		}
		zxfer_append_failure_report_to_log report
	"

	assertEquals "The append failure should stay the reported status when lock release also fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "The primary staging failure should still be reported." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG staging directory"
	assertContains "The secondary release failure should be reported as a warning." \
		"$(cat "$stderr_file")" "unable to release ZXFER_ERROR_LOG lock for \"$log_path\" (status 5)"
}

test_zxfer_append_failure_report_to_log_warns_when_nonwritable_parent_needs_create() {
	log_dir="$TEST_TMPDIR/nonwritable-create-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_create.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_create.stderr"

	mkdir -p "$log_dir"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Missing ZXFER_ERROR_LOG files should fail closed when the trusted parent is not writable." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Missing ZXFER_ERROR_LOG files in non-writable trusted parents should warn that creation is not possible." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_fallback_lock_lookup_fails() {
	log_dir="$TEST_TMPDIR/nonwritable-lock-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_lock.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_lock.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_get_error_log_fallback_lock_dir() {
			return 1
		}
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Fallback lock-path lookup failures should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Fallback lock-path lookup failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to acquire ZXFER_ERROR_LOG lock"
}

test_zxfer_append_failure_report_to_log_warns_when_direct_append_fails_in_nonwritable_parent() {
	log_dir="$TEST_TMPDIR/nonwritable-append-parent"
	log_path="$log_dir/failure.log"
	stdout_file="$TEST_TMPDIR/nonwritable_append.stdout"
	stderr_file="$TEST_TMPDIR/nonwritable_append.stderr"

	mkdir -p "$log_dir"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"
	make_error_log_parent_read_only "$log_dir" || return 0

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		printf() {
			if [ \"\$2\" = \"message: appended-report\" ]; then
				return 1
			fi
			command printf \"\$@\"
		}
		zxfer_append_failure_report_to_log \"message: appended-report\"
	"
	chmod 700 "$log_dir"

	assertEquals "Direct append failures in the non-writable-parent fallback path should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Direct append failures in the non-writable-parent fallback path should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_lock_acquisition_fails() {
	log_path="$TEST_TMPDIR/lock-failure.log"
	stdout_file="$TEST_TMPDIR/lock_failure.stdout"
	stderr_file="$TEST_TMPDIR/lock_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_acquire_error_log_lock() {
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Lock-acquisition failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Lock-acquisition failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to acquire ZXFER_ERROR_LOG lock"
}

test_zxfer_append_failure_report_to_log_warns_when_staging_dir_creation_fails() {
	log_path="$TEST_TMPDIR/stage-failure.log"
	stdout_file="$TEST_TMPDIR/stage_failure.stdout"
	stderr_file="$TEST_TMPDIR/stage_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Staging-dir creation failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Staging-dir creation failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG staging directory"
}

test_zxfer_get_error_log_fallback_lock_dir_does_not_try_later_roots_after_prepare_fails() {
	prepare_log="$TEST_TMPDIR/prepare-once.log"
	rm -f "$prepare_log"

	zxfer_test_capture_subshell "
		TMPDIR=/first-trusted
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' \"\$1\"
		}
		zxfer_prepare_error_log_fallback_lock_dir() {
			printf 'prepare=<%s>\n' \"\$1\" >>'$prepare_log'
			return 1
		}
		zxfer_get_error_log_fallback_lock_dir /tmp/failure.log
	"

	assertEquals "Fallback lock-dir lookup should fail closed when the first trusted root cannot hold the lock." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Fallback lock-dir lookup should not fall through to later roots after a prepare failure." \
		"prepare=</first-trusted>" "$(cat "$prepare_log")"
}

test_zxfer_create_error_log_file_cleans_up_stage_dir_when_write_or_move_fails() {
	write_stage_dir="$TEST_TMPDIR/.zxfer-error-log.write.$$"
	move_stage_dir="$TEST_TMPDIR/.zxfer-error-log.move.$$"
	write_output=$(
		(
			set +e
			mkdir -p "$write_stage_dir"
			zxfer_register_runtime_artifact_path "$write_stage_dir" || exit 90
			zxfer_create_secure_staging_dir_for_path() {
				g_zxfer_secure_staging_dir_result="$write_stage_dir"
				return 0
			}
			zxfer_write_runtime_artifact_file() {
				return 1
			}
			zxfer_create_error_log_file "$TEST_TMPDIR/write_failure.log"
			printf 'status=%s\n' "$?"
			printf 'stage_exists=%s\n' "$([ -e "$write_stage_dir" ] && printf yes || printf no)"
		)
	)
	move_output=$(
		(
			set +e
			mkdir -p "$move_stage_dir"
			zxfer_register_runtime_artifact_path "$move_stage_dir" || exit 90
			zxfer_create_secure_staging_dir_for_path() {
				g_zxfer_secure_staging_dir_result="$move_stage_dir"
				return 0
			}
			mv() {
				return 1
			}
			zxfer_create_error_log_file "$TEST_TMPDIR/move_failure.log"
			printf 'status=%s\n' "$?"
			printf 'stage_exists=%s\n' "$([ -e "$move_stage_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Error-log file creation should fail when the staged file cannot be written." \
		"$write_output" "status=1"
	assertContains "Error-log file creation should remove the stage directory when the staged write fails." \
		"$write_output" "stage_exists=no"
	assertContains "Error-log file creation should fail when the staged file cannot be moved into place." \
		"$move_output" "status=1"
	assertContains "Error-log file creation should remove the stage directory when the final move fails." \
		"$move_output" "stage_exists=no"
}

test_zxfer_create_error_log_file_helpers_cover_current_shell_paths() {
	create_fail_target="$TEST_TMPDIR/error_log_create_fail.log"
	create_success_target="$TEST_TMPDIR/error_log_create_success.log"
	create_success_stage="$TEST_TMPDIR/.zxfer-error-log.success.$$"

	zxfer_test_capture_subshell "
		set +e
		zxfer_create_secure_staging_dir_for_path() {
			return 1
		}
		zxfer_create_error_log_file \"$create_fail_target\" >/dev/null
		printf 'fail=%s\\n' \"\$?\"
		unset -f zxfer_create_secure_staging_dir_for_path

		mkdir -p \"$create_success_stage\" || exit 91
		zxfer_register_runtime_artifact_path \"$create_success_stage\" || exit 92
		zxfer_create_secure_staging_dir_for_path() {
			g_zxfer_secure_staging_dir_result=\"$create_success_stage\"
			return 0
		}
		zxfer_create_error_log_file \"$create_success_target\" >/dev/null
		printf 'success=%s\\n' \"\$?\"
		unset -f zxfer_create_secure_staging_dir_for_path
		printf 'target=%s\\n' \"\$([ -f \"$create_success_target\" ] && printf yes || printf no)\"
		printf 'contents=<%s>\\n' \"\$([ -f \"$create_success_target\" ] && cat \"$create_success_target\")\"
		printf 'stage=%s\\n' \"\$([ -e \"$create_success_stage\" ] && printf yes || printf no)\"
	"

	assertEquals "Current-shell error-log creation helper coverage should complete the subshell cleanly." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Current-shell error-log creation should preserve staging-dir allocation failures." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "fail=1"
	assertContains "Current-shell error-log creation should succeed when staging and publish both succeed." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "success=0"
	assertContains "Current-shell error-log creation should publish the target log file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "target=yes"
	assertContains "Current-shell error-log creation should create an empty secure log file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "contents=<>"
	assertContains "Current-shell error-log creation should remove the staging directory after publishing the file." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "stage=no"
}

test_zxfer_acquire_error_log_lock_rejects_symlink_and_reap_validation_failures() {
	lock_target="$TEST_TMPDIR/error_log_lock_target"
	lock_symlink="$TEST_TMPDIR/error_log_lock_symlink"
	lock_dir="$TEST_TMPDIR/error_log_lock_dir"
	mkdir -p "$lock_target" "$lock_dir" || fail "Unable to create error-log lock fixtures."
	ln -s "$lock_target" "$lock_symlink" || fail "Unable to create the error-log lock symlink fixture."

	symlink_output=$(
		(
			set +e
			zxfer_create_owned_lock_dir() {
				return 1
			}
			zxfer_acquire_error_log_lock "$lock_symlink"
			printf 'status=%s\n' "$?"
		)
	)
	reap_output=$(
		(
			set +e
			zxfer_create_owned_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir() {
				return 1
			}
			zxfer_acquire_error_log_lock "$lock_dir"
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Error-log lock acquisition should fail closed when the target path is a symlink." \
		"$symlink_output" "status=1"
	assertContains "Error-log lock acquisition should fail closed when stale-lock reaping reports a validation failure." \
		"$reap_output" "status=1"
}

test_zxfer_acquire_error_log_lock_reports_reap_validation_failures_in_current_shell() {
	lock_dir="$TEST_TMPDIR/error_log_lock_reap_current"
	mkdir -p "$lock_dir" || fail "Unable to create the current-shell error-log lock fixture."

	zxfer_create_owned_lock_dir() {
		return 1
	}
	zxfer_try_reap_stale_owned_lock_dir() {
		return 1
	}

	zxfer_acquire_error_log_lock "$lock_dir"
	status=$?

	zxfer_source_runtime_modules_through "zxfer_error_log.sh"
	setUp

	assertEquals "Current-shell error-log lock acquisition should fail closed when stale-lock reaping returns a validation failure." \
		1 "$status"
}

test_zxfer_append_failure_report_to_log_warns_when_snapshot_link_fails() {
	log_path="$TEST_TMPDIR/snapshot-link-failure.log"
	stdout_file="$TEST_TMPDIR/snapshot_link_failure.stdout"
	stderr_file="$TEST_TMPDIR/snapshot_link_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		ln() {
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Snapshot-link failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Snapshot-link failures should emit the append warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_snapshot_validation_fails() {
	log_path="$TEST_TMPDIR/snapshot-validation-failure.log"
	stdout_file="$TEST_TMPDIR/snapshot_validation_failure.stdout"
	stderr_file="$TEST_TMPDIR/snapshot_validation_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		g_test_validation_calls=0
		zxfer_validate_existing_error_log_file() {
			g_test_validation_calls=\$((g_test_validation_calls + 1))
			if [ \"\$g_test_validation_calls\" -eq 1 ]; then
				return 0
			fi
			printf '%s\n' \"zxfer: warning: refusing ZXFER_ERROR_LOG file \\\"\$2\\\" because its permissions could not be determined.\" >&2
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Snapshot-validation failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Snapshot-validation failures should preserve the validation warning." \
		"$(cat "$stderr_file")" "permissions could not be determined"
}

test_zxfer_append_failure_report_to_log_warns_when_snapshot_copy_fails() {
	log_path="$TEST_TMPDIR/snapshot-copy-failure.log"
	stdout_file="$TEST_TMPDIR/snapshot_copy_failure.stdout"
	stderr_file="$TEST_TMPDIR/snapshot_copy_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		cat() {
			case \"\$1\" in
			*/log.snapshot)
				return 1
				;;
			esac
			command cat \"\$@\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Snapshot-copy failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Snapshot-copy failures should emit the append warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_warns_when_atomic_move_fails() {
	log_path="$TEST_TMPDIR/move-failure.log"
	stdout_file="$TEST_TMPDIR/move_failure.stdout"
	stderr_file="$TEST_TMPDIR/move_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		mv() {
			case \"\$1:\$2\" in
			-f:*/log.write | */log.write:*)
				return 1
				;;
			esac
			case \"\$1\" in
			*/log.write)
				return 1
				;;
			esac
			command mv \"\$@\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Atomic-move failures should return a non-zero status." 1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Atomic-move failures should emit the append warning." \
		"$(cat "$stderr_file")" "unable to append failure report to ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_returns_failure_when_created_file_fails_validation() {
	log_path="$TEST_TMPDIR/create-validation-failure.log"
	stdout_file="$TEST_TMPDIR/create_validation_failure.stdout"
	stderr_file="$TEST_TMPDIR/create_validation_failure.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_validate_existing_error_log_file() {
			printf '%s\n' \"zxfer: warning: refusing ZXFER_ERROR_LOG file \\\"\$2\\\" because its permissions could not be determined.\" >&2
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Validation failures after secure file creation should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Validation failures after secure file creation should preserve the validation warning." \
		"$(cat "$stderr_file")" "permissions could not be determined"
}

test_zxfer_append_failure_report_to_log_warns_when_staged_log_chmod_fails() {
	log_path="$TEST_TMPDIR/staged-chmod-failure.log"
	stdout_file="$TEST_TMPDIR/staged_chmod_failure.stdout"
	stderr_file="$TEST_TMPDIR/staged_chmod_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		chmod() {
			case \"\$2\" in
			*/log.write)
				return 1
				;;
			esac
			command chmod \"\$@\"
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Staged-log chmod failures should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Staged-log chmod failures should emit the documented warning." \
		"$(cat "$stderr_file")" "unable to chmod ZXFER_ERROR_LOG file"
}

test_zxfer_append_failure_report_to_log_creates_secure_file() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure.log"
	ZXFER_ERROR_LOG="$log_path"
	report_contents=$(printf 'zxfer: failure report begin\nmessage: failed\nzxfer: failure report end\n')

	set +e
	zxfer_append_failure_report_to_log "$report_contents"
	status=$?
	if [ -f "$log_path" ]; then
		file_exists=1
	else
		file_exists=0
	fi
	perms=$(stat -c '%a' "$log_path" 2>/dev/null || stat -f '%Lp' "$log_path" 2>/dev/null)
	perms_status=$?
	grep -F "message: failed" "$log_path" >/dev/null 2>&1
	grep_status=$?

	assertEquals "ZXFER_ERROR_LOG appends should succeed for valid absolute paths." 0 "$status"
	assertEquals "Failure log should be created when ZXFER_ERROR_LOG is valid." 1 "$file_exists"
	assertEquals "Log file mode should be readable for assertions." 0 "$perms_status"
	assertEquals "ZXFER_ERROR_LOG files should be created with mode 600." "600" "$perms"
	assertEquals "Failure log should contain the rendered report payload." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_preserves_existing_contents() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure_append.log"
	ZXFER_ERROR_LOG="$log_path"
	printf '%s\n' "existing: keep-me" >"$log_path"
	chmod 600 "$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: appended-report"
	status=$?
	grep -F "existing: keep-me" "$log_path" >/dev/null 2>&1
	existing_status=$?
	grep -F "message: appended-report" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files should still accept appended reports." 0 "$status"
	assertEquals "Atomic ZXFER_ERROR_LOG appends should preserve prior log contents." 0 "$existing_status"
	assertEquals "Atomic ZXFER_ERROR_LOG appends should add the new report payload." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_relative_path() {
	stderr_file="$TEST_TMPDIR/error_log.stderr"
	ZXFER_ERROR_LOG="relative.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log.stdout" 2>"$stderr_file"
	status=$?
	grep -F "refusing ZXFER_ERROR_LOG path \"relative.log\" because it is not absolute" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	if [ -e "$TEST_TMPDIR/relative.log" ]; then
		file_exists=1
	else
		file_exists=0
	fi

	assertEquals "Relative ZXFER_ERROR_LOG paths should be rejected." 1 "$status"
	assertEquals "Relative ZXFER_ERROR_LOG rejection should emit a warning." 0 "$grep_status"
	assertEquals "Relative ZXFER_ERROR_LOG should not create a local file." 0 "$file_exists"
}

test_zxfer_append_failure_report_to_log_rejects_missing_parent_dir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	stderr_file="$TEST_TMPDIR/error_log_parent.stderr"
	ZXFER_ERROR_LOG="$physical_tmpdir/missing/subdir/failure.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_parent.stdout" 2>"$stderr_file"
	status=$?
	grep -F "parent directory \"$physical_tmpdir/missing/subdir\" does not exist" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Missing parent directories should be rejected for ZXFER_ERROR_LOG." 1 "$status"
	assertEquals "Missing parent directory rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_untrusted_parent_dir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_dir="$physical_tmpdir/untrusted_error_log_parent"
	stderr_file="$TEST_TMPDIR/error_log_untrusted_parent.stderr"
	mkdir -p "$log_dir"
	chmod 0777 "$log_dir"
	ZXFER_ERROR_LOG="$log_dir/failure.log"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_untrusted_parent.stdout" 2>"$stderr_file"
	status=$?
	grep -F "writable by others without sticky-bit protection" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	chmod 0700 "$log_dir"

	assertEquals "ZXFER_ERROR_LOG parents that are writable by others without sticky-bit protection should be rejected." 1 "$status"
	assertEquals "Untrusted ZXFER_ERROR_LOG parent rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_symlinked_parent_component() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/real_parent"
	link_dir="$physical_tmpdir/link_parent"
	log_path="$link_dir/failure.log"
	stderr_file="$TEST_TMPDIR/error_log_symlink.stderr"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_symlink.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path component \"$link_dir\" is a symlink" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	if [ -e "$real_dir/failure.log" ]; then
		file_exists=1
	else
		file_exists=0
	fi

	assertEquals "Symlinked parent components should be rejected for ZXFER_ERROR_LOG." 1 "$status"
	assertEquals "Symlinked parent component rejection should emit a warning." 0 "$grep_status"
	assertEquals "Symlinked parent component rejection should not create the target file." 0 "$file_exists"
}

test_zxfer_append_failure_report_to_log_rejects_symlink_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_path="$physical_tmpdir/real_failure.log"
	log_path="$physical_tmpdir/failure_link.log"
	stderr_file="$TEST_TMPDIR/error_log_target_symlink.stderr"
	: >"$real_path"
	chmod 600 "$real_path"
	ln -s "$real_path" "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_target_symlink.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path component \"$log_path\" is a symlink" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Symlinked ZXFER_ERROR_LOG targets should be rejected." 1 "$status"
	assertEquals "Symlinked target rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_non_regular_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/failure_dir"
	stderr_file="$TEST_TMPDIR/error_log_nonregular.stderr"
	mkdir -p "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "report" >"$TEST_TMPDIR/error_log_nonregular.stdout" 2>"$stderr_file"
	status=$?
	grep -F "path \"$log_path\" because it is not a regular file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?

	assertEquals "Non-regular ZXFER_ERROR_LOG targets should be rejected." 1 "$status"
	assertEquals "Non-regular target rejection should emit a warning." 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_rejects_existing_insecure_mode() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/insecure_mode.log"
	stderr_file="$TEST_TMPDIR/error_log_mode.stderr"
	: >"$log_path"
	chmod 644 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: should-not-append" >"$TEST_TMPDIR/error_log_mode.stdout" 2>"$stderr_file"
	status=$?
	grep -F "permissions (644) are not 0600" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing insecure ZXFER_ERROR_LOG files should be rejected." 1 "$status"
	assertEquals "Insecure mode rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected insecure log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_existing_insecure_owner() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/insecure_owner.log"
	stderr_file="$TEST_TMPDIR/error_log_owner.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1"
		}
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock() {
			:
		}
		zxfer_get_path_owner_uid() { printf '%s\n' "1234"; }
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_owner.stdout" 2>"$stderr_file"
	status=$?
	grep -F "owned by UID 1234 instead of" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with insecure owners should be rejected." 1 "$status"
	assertEquals "Insecure owner rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected insecure-owner log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_owner() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_owner.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_owner.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_validate_temp_root_candidate() {
			printf '%s\n' "$1"
		}
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock() {
			:
		}
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_owner.stdout" 2>"$stderr_file"
	status=$?
	grep -F "owner could not be determined" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with unknown owners should be rejected." 1 "$status"
	assertEquals "Unknown-owner rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected unknown-owner log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_mode() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_mode.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_mode.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_acquire_error_log_lock() {
			return 0
		}
		zxfer_release_error_log_lock() {
			:
		}
		zxfer_get_path_owner_uid() {
			printf '%s\n' "0"
		}
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_mode.stdout" 2>"$stderr_file"
	status=$?
	grep -F "permissions could not be determined" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	grep -F "should-not-append" "$log_path" >/dev/null 2>&1
	append_status=$?

	assertEquals "Existing ZXFER_ERROR_LOG files with unknown modes should be rejected." 1 "$status"
	assertEquals "Unknown-mode rejection should emit a warning." 0 "$grep_status"
	assertNotEquals "Rejected unknown-mode log files must not receive appended report data." 0 "$append_status"
}

test_zxfer_append_failure_report_to_log_warns_when_file_creation_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/create_failure.log"
	stderr_file="$TEST_TMPDIR/error_log_create_failure.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_create_error_log_file() {
			return 1
		}
		zxfer_append_failure_report_to_log "message: create-failed"
	) >"$TEST_TMPDIR/error_log_create_failure.stdout" 2>"$stderr_file"
	status=$?
	grep -F "unable to create ZXFER_ERROR_LOG file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	stderr_contents=$(cat "$stderr_file" 2>/dev/null || true)

	assertEquals "ZXFER_ERROR_LOG creation failures should be reported without succeeding. status=$status stderr=$stderr_contents" 1 "$status"
	assertEquals "ZXFER_ERROR_LOG creation failures should emit a warning. status=$status stderr=$stderr_contents" 0 "$grep_status"
}

test_zxfer_append_failure_report_to_log_warns_when_chmod_fails() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/chmod_failure.log"
	stderr_file="$TEST_TMPDIR/error_log_chmod_failure.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		chmod() {
			[ "$2" != "$log_path" ] || return 1
			command chmod "$@"
		}
		zxfer_append_failure_report_to_log "message: chmod-failed"
	) >"$TEST_TMPDIR/error_log_chmod_failure.stdout" 2>"$stderr_file"
	status=$?
	grep -F "unable to chmod ZXFER_ERROR_LOG file" "$stderr_file" >/dev/null 2>&1
	grep_status=$?
	stderr_contents=$(cat "$stderr_file" 2>/dev/null || true)

	assertEquals "ZXFER_ERROR_LOG chmod failures should be reported without succeeding. status=$status stderr=$stderr_contents" 1 "$status"
	assertEquals "ZXFER_ERROR_LOG chmod failures should emit a warning. status=$status stderr=$stderr_contents" 0 "$grep_status"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
