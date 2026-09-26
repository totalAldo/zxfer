#!/bin/sh
#
# shunit2 tests for the owned-lock protocol in src/zxfer_error_log.sh.
#
# Lock metadata is owner pid + process start token only (V2). These tests pin
# pid+start-token liveness, stale reaping, checked release, and the
# old-format-treated-as-corrupt policy. The ps parser behind the start token
# belongs to the cleanup wrapper and is tested in
# tests/test_zxfer_cleanup_child_wrapper.sh.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_error_log.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_locking"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	TMPDIR="$TEST_TMPDIR"
	zxfer_reset_owned_lock_tracking
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
