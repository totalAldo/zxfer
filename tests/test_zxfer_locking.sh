#!/bin/sh
#
# shunit2 tests for the owned-lock helpers (the OWNED LOCK / LEASE
# COORDINATION section of src/zxfer_runtime.sh, formerly zxfer_locking.sh).
#
# Lock metadata is owner pid + process start token only (V2). These tests pin
# pid+start-token liveness, stale reaping, checked release, and the
# old-format-treated-as-corrupt policy.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_runtime.sh"

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
	token=$(zxfer_get_process_start_token "$$")

	assertContains "Process-start tokens should include the selector prefix." \
		"$token" ":"
	assertNotEquals "Process-start tokens should not be empty for the current process." \
		"" "$token"
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
				if [ "$4" = "lstart=" ]; then
					printf '   \n'
					return 0
				fi
				if [ "$4" = "stime=" ]; then
					printf '  Apr 13   12:00 \n'
					return 0
				fi
				return 1
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
			# Memoize in the current shell first; later command-substitution
			# callers then reuse the captured token without another ps spawn.
			zxfer_get_own_process_start_token >/dev/null
			first=$(zxfer_get_own_process_start_token)
			second=$(zxfer_get_own_process_start_token)
			printf 'first=<%s>\n' "$first"
			printf 'second=<%s>\n' "$second"
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
			zxfer_get_own_process_start_token >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "The own-process start token should be captured once." \
		"$output" "first=<lstart:memo-test>"
	assertContains "The memoized own-process start token should be reused on later calls." \
		"$output" "second=<lstart:memo-test>"
	assertContains "Repeated own-token lookups should not re-capture the start token." \
		"$output" "captures=1"
	assertContains "Own-token lookup should fail closed when no start token can be captured." \
		"$failure_output" "status=1"
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
	zxfer_source_runtime_modules_through "zxfer_runtime.sh"
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

test_zxfer_normalize_owned_lock_text_field_runs_in_current_shell() {
	tab=$(printf '\t')
	normalized_value=$(zxfer_normalize_owned_lock_text_field "  owned${tab}lock   value  ")

	assertEquals "Current-shell owned-lock text normalization should trim and squeeze whitespace in one validated pass." \
		"owned lock value" "$normalized_value"
}

test_zxfer_normalize_owned_lock_text_field_rejects_blank_values_and_keeps_globs_literal() {
	output=$(
		(
			set +e
			zxfer_normalize_owned_lock_text_field "   " >/dev/null
			printf 'blank=%s\n' "$?"
			normalized=$(zxfer_normalize_owned_lock_text_field "  glob * purpose  ")
			printf 'glob=<%s>\n' "$normalized"
		)
	)

	assertContains "Owned lock text normalization should reject values that collapse to empty." \
		"$output" "blank=1"
	assertContains "Owned lock text normalization should keep glob characters literal." \
		"$output" "glob=<glob * purpose>"
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
			zxfer_validate_owned_lock_container_dir "$lock_dir" >/dev/null
			printf 'dir_mode=%s\n' "$?"
			chmod 700 "$lock_dir"
			zxfer_validate_owned_lock_metadata_file "$metadata_path" >/dev/null
			printf 'metadata_mode=%s\n' "$?"
		)
	)

	assertContains "Owned lock container validation should reject non-0700 directories." \
		"$output" "dir_mode=1"
	assertContains "Owned lock metadata validation should reject non-0600 files." \
		"$output" "metadata_mode=1"
}

test_zxfer_create_and_load_owned_lock_metadata_round_trip() {
	lock_dir="$TEST_TMPDIR/roundtrip.lock"
	output=$(
		(
			set +e
			zxfer_create_owned_lock_dir "$lock_dir" lock "roundtrip-lock" >/dev/null
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
			zxfer_create_owned_lock_dir "" lock "purpose" >/dev/null
			printf 'blank_create=%s\n' "$?"
			zxfer_create_owned_lock_dir "$lock_dir" lock "modes-lock" >/dev/null
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

test_zxfer_write_and_parse_owned_lock_metadata_file_cover_success_paths_in_current_shell() {
	lock_dir="$TEST_TMPDIR/direct-write-parse.lock"
	metadata_path="$lock_dir/metadata"
	tab=$(printf '\t')
	mkdir "$lock_dir" || fail "Unable to create direct owned lock metadata fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod direct owned lock metadata fixture directory."

	output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				printf '%s\n' "lstart:direct-test"
			}
			g_zxfer_own_process_start_token=""
			zxfer_write_owned_lock_metadata_file "$lock_dir" >/dev/null
			printf 'write=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$metadata_path" >/dev/null
			printf 'parse=%s\n' "$?"
			printf 'pid=<%s>\n' "$g_zxfer_owned_lock_pid_result"
			printf 'start_token=<%s>\n' "$g_zxfer_owned_lock_start_token_result"
		)
	)
	metadata_contents=$(cat "$metadata_path")

	assertContains "Owned lock metadata writes should succeed on the direct success path." \
		"$output" "write=0"
	assertContains "Owned lock metadata parsing should succeed on a freshly written direct metadata file." \
		"$output" "parse=0"
	assertContains "Direct metadata parsing should recover the owning pid." \
		"$output" "pid=<$$>"
	assertContains "Direct metadata parsing should recover the staged start token." \
		"$output" "start_token=<lstart:direct-test>"
	assertContains "Direct metadata writes should store the staged start token in the metadata file itself." \
		"$metadata_contents" "start_token${tab}lstart:direct-test"
}

test_zxfer_write_owned_lock_metadata_file_handles_token_write_and_publish_failures() {
	lock_dir="$TEST_TMPDIR/write-owned.lock"
	mkdir "$lock_dir" || fail "Unable to create owned lock metadata fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod owned lock metadata fixture directory."

	token_output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				return 1
			}
			g_zxfer_own_process_start_token=""
			zxfer_write_owned_lock_metadata_file "$lock_dir" >/dev/null
			printf 'token=%s\n' "$?"
		)
	)
	block_stderr="$TEST_TMPDIR/write_owned_lock_metadata.stderr"
	block_output=$(
		(
			set +e
			block_target_dir="$TEST_TMPDIR/block-write-target"
			mkdir -p "$block_target_dir" || exit 1
			rm -f "$lock_dir/.metadata.stage" || exit 1
			ln -s "$block_target_dir" "$lock_dir/.metadata.stage" || exit 1
			zxfer_write_owned_lock_metadata_file "$lock_dir" >/dev/null
			printf 'block=%s\n' "$?"
			printf 'leftover=%s\n' "$(find "$lock_dir" -maxdepth 1 -name '.metadata.*' -print)"
		) 2>"$block_stderr"
	)
	publish_output=$(
		(
			set +e
			mv() {
				return 1
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" >/dev/null
			printf 'publish=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata writes should fail closed when the current start token is unavailable." \
		"$token_output" "token=1"
	assertContains "Owned lock metadata writes should fail closed when the staged metadata file cannot be written." \
		"$block_output" "block=1"
	assertContains "Owned lock metadata writes should clean up staged metadata files when the staged write fails." \
		"$block_output" "leftover="
	assertEquals "Owned lock metadata write failures should not leak raw shell redirection errors." \
		"" "$(cat "$block_stderr")"
	assertContains "Owned lock metadata writes should fail closed when publishing the staged metadata file fails." \
		"$publish_output" "publish=1"
}

test_zxfer_parse_owned_lock_metadata_file_rejects_malformed_payloads() {
	invalid_pid_path="$TEST_TMPDIR/invalid-pid.metadata"
	invalid_layout_path="$TEST_TMPDIR/invalid-layout.metadata"
	invalid_no_tab_path="$TEST_TMPDIR/invalid-no-tab.metadata"
	invalid_tab_value_path="$TEST_TMPDIR/invalid-tab-value.metadata"
	invalid_key_path="$TEST_TMPDIR/invalid-key.metadata"
	short_path="$TEST_TMPDIR/short.metadata"

	cat >"$invalid_pid_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	not-a-pid
start_token	lstart:test
EOF
	cat >"$invalid_layout_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	123
start_token	lstart:test
extra	line
EOF
	cat >"$invalid_no_tab_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	123
start_token lstart:test
EOF
	cat >"$invalid_tab_value_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	123
start_token	lstart:test	extra
EOF
	cat >"$invalid_key_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	123
starting_token	lstart:test
EOF
	cat >"$short_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
pid	123
EOF
	chmod 600 "$invalid_pid_path" "$invalid_layout_path" "$invalid_no_tab_path" \
		"$invalid_tab_value_path" "$invalid_key_path" "$short_path" ||
		fail "Unable to chmod malformed metadata fixtures."

	output=$(
		(
			set +e
			zxfer_parse_owned_lock_metadata_file "$invalid_pid_path" >/dev/null
			printf 'pid=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$invalid_layout_path" >/dev/null
			printf 'layout=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$invalid_no_tab_path" >/dev/null
			printf 'no_tab=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$invalid_tab_value_path" >/dev/null
			printf 'tab_value=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$invalid_key_path" >/dev/null
			printf 'key=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$short_path" >/dev/null
			printf 'short=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata parsing should reject nonnumeric PIDs." \
		"$output" "pid=1"
	assertContains "Owned lock metadata parsing should reject unexpected extra lines." \
		"$output" "layout=1"
	assertContains "Owned lock metadata parsing should reject field rows that are missing the tab separator." \
		"$output" "no_tab=1"
	assertContains "Owned lock metadata parsing should reject field values that contain tabs." \
		"$output" "tab_value=1"
	assertContains "Owned lock metadata parsing should reject unknown metadata keys." \
		"$output" "key=1"
	assertContains "Owned lock metadata parsing should reject truncated metadata files." \
		"$output" "short=1"
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

	zxfer_create_owned_lock_dir "$live_lock_dir" lock "live-lock" >/dev/null
	write_owned_lock_metadata_fixture "$stale_lock_dir" "999999999"

	output=$(
		(
			set +e
			zxfer_try_reap_stale_owned_lock_dir "$live_lock_dir" 1 lock "live-lock" >/dev/null
			printf 'live=%s\n' "$?"
			printf 'live_exists=%s\n' "$([ -d "$live_lock_dir" ] && printf yes || printf no)"
			zxfer_try_reap_stale_owned_lock_dir "$stale_lock_dir" 1 lock "stale-lock" >/dev/null
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
			zxfer_try_reap_stale_owned_lock_dir "$lock_dir" 0 lock "corrupt-lock" >/dev/null
			printf 'defer=%s\n' "$?"
			zxfer_try_reap_stale_owned_lock_dir "$lock_dir" 1 lock "corrupt-lock" >/dev/null
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

test_owned_lock_owner_and_cleanup_helpers_cover_stale_unknown_and_invalid_targets() {
	current_token=$(zxfer_get_process_start_token "$$") ||
		fail "Unable to derive owned lock test start token."
	file_path="$TEST_TMPDIR/not-a-lock-file"
	target_dir="$TEST_TMPDIR/cleanup-target.lock"
	link_path="$TEST_TMPDIR/cleanup-link.lock"
	: >"$file_path" || fail "Unable to create owned lock cleanup file fixture."
	mkdir "$target_dir" || fail "Unable to create owned lock cleanup target directory."
	ln -s "$target_dir" "$link_path" || fail "Unable to create owned lock cleanup symlink."

	liveness_output=$(
		(
			set +e
			zxfer_owned_lock_owner_is_live "999999999" "$current_token" >/dev/null
			printf 'stale=%s\n' "$?"
			zxfer_owned_lock_owner_is_live "$$" "${current_token}mismatch" >/dev/null
			printf 'token_mismatch=%s\n' "$?"
			zxfer_owned_lock_owner_is_live "$$" "$current_token" >/dev/null
			printf 'live=%s\n' "$?"
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
			zxfer_owned_lock_owner_is_live "$$" "$current_token" >/dev/null
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
			mkdir "$TEST_TMPDIR/rm-fallback.lock" || exit 1
			rm() {
				rmdir "$2"
				return 1
			}
			zxfer_cleanup_owned_lock_dir "$TEST_TMPDIR/rm-fallback.lock" >/dev/null
			printf 'rm_fallback=%s\n' "$?"
		)
	)

	assertContains "Owned lock liveness should treat dead PIDs as stale owners." \
		"$liveness_output" "stale=1"
	assertContains "Owned lock liveness should treat mismatched start tokens for a live PID as stale owners." \
		"$liveness_output" "token_mismatch=1"
	assertContains "Owned lock liveness should report the current process as live for its own token." \
		"$liveness_output" "live=0"
	assertContains "Owned lock liveness should fail closed when it cannot retrieve a current start token for a live PID." \
		"$unknown_output" "unknown=2"
	assertContains "Owned lock cleanup should ignore blank targets." \
		"$cleanup_output" "blank=0"
	assertContains "Owned lock cleanup should ignore missing targets." \
		"$cleanup_output" "missing=0"
	assertContains "Owned lock cleanup should reject symlink targets." \
		"$cleanup_output" "symlink=1"
	assertContains "Owned lock cleanup should reject non-directory targets." \
		"$cleanup_output" "file=1"
	assertContains "Owned lock cleanup should still succeed when rm reports failure but the directory is already gone by the post-check." \
		"$cleanup_output" "rm_fallback=0"
}

test_owned_lock_cleanup_fails_closed_when_rm_failures_persist() {
	lock_dir="$TEST_TMPDIR/cleanup-hard-fail.lock"
	mkdir "$lock_dir" || fail "Unable to create hard-fail owned lock cleanup fixture."

	cleanup_output=$(
		(
			set +e
			rm() {
				return 1
			}
			zxfer_cleanup_owned_lock_dir "$lock_dir" >/dev/null
			printf 'cleanup=%s\n' "$?"
			printf 'exists=%s\n' "$([ -d "$lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock cleanup should fail when rm reports failure and the lock directory still exists afterward." \
		"$cleanup_output" "cleanup=1"
	assertContains "Owned lock cleanup failure paths should leave the existing directory in place for inspection." \
		"$cleanup_output" "exists=yes"
}

test_zxfer_create_owned_lock_dir_failure_paths_clean_up_partial_directories() {
	validate_lock_dir="$TEST_TMPDIR/validate-fail.lock"
	write_lock_dir="$TEST_TMPDIR/write-fail.lock"

	validate_output=$(
		(
			set +e
			zxfer_validate_owned_lock_container_dir() {
				return 1
			}
			zxfer_create_owned_lock_dir "$validate_lock_dir" lock "validate-fail" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$validate_lock_dir" ] && printf yes || printf no)"
		)
	)
	write_output=$(
		(
			set +e
			zxfer_write_owned_lock_metadata_file() {
				return 1
			}
			zxfer_create_owned_lock_dir "$write_lock_dir" lock "write-fail" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'exists=%s\n' "$([ -e "$write_lock_dir" ] && printf yes || printf no)"
		)
	)

	assertContains "Owned lock creation should fail closed when the created directory cannot be revalidated." \
		"$validate_output" "status=1"
	assertContains "Owned lock creation should remove directories that fail post-create validation." \
		"$validate_output" "exists=no"
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
			zxfer_try_reap_stale_owned_lock_dir \
				"$liveness_dir" 1 lock "reap-liveness" >/dev/null
			printf 'liveness=%s\n' "$?"
		)
	)
	cleanup_output=$(
		(
			set +e
			zxfer_cleanup_owned_lock_dir() {
				return 1
			}
			zxfer_try_reap_stale_owned_lock_dir \
				"$cleanup_dir" 1 lock "reap-cleanup" >/dev/null
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

	assertContains "Owned lock reaping should fail closed when live-owner validation is inconclusive." \
		"$liveness_output" "liveness=1"
	assertContains "Owned lock reaping should fail closed when cleanup of a stale entry fails." \
		"$cleanup_output" "cleanup=1"
	assertContains "Owned lock reaping should fail closed on unexpected metadata-loader statuses." \
		"$unknown_load_output" "unknown=1"
}

test_zxfer_release_owned_lock_dir_requires_current_owner_identity() {
	lock_dir="$TEST_TMPDIR/release-mismatch.lock"
	write_owned_lock_metadata_fixture \
		"$lock_dir" "$$" "lstart:not-the-current-process"

	output=$(
		(
			set +e
			zxfer_release_owned_lock_dir "$lock_dir" lock "release-mismatch" >/dev/null
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
			zxfer_validate_owned_lock_metadata_file "$metadata_path" >/dev/null
			printf 'uid=%s\n' "$?"
		)
	)
	owner_output=$(
		(
			set +e
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_validate_owned_lock_metadata_file "$metadata_path" >/dev/null
			printf 'owner=%s\n' "$?"
		)
	)
	mode_output=$(
		(
			set +e
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_validate_owned_lock_metadata_file "$metadata_path" >/dev/null
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
			zxfer_current_process_owns_owned_lock_dir "$lock_dir" lock "lookup-fail" >/dev/null
			printf 'token=%s\n' "$?"
		)
	)
	load_failure_output=$(
		(
			set +e
			zxfer_current_process_owns_owned_lock_dir "$TEST_TMPDIR/missing-current-owner.lock" >/dev/null
			printf 'load=%s\n' "$?"
		)
	)
	release_output=$(
		(
			set +e
			zxfer_cleanup_owned_lock_dir() {
				return 1
			}
			zxfer_release_owned_lock_dir "$lock_dir" lock "lookup-fail" >/dev/null
			printf 'release=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata validation should fail closed when effective-uid lookup fails." \
		"$uid_output" "uid=1"
	assertContains "Owned lock metadata validation should fail closed when owner lookup fails." \
		"$owner_output" "owner=1"
	assertContains "Owned lock metadata validation should fail closed when mode lookup fails." \
		"$mode_output" "mode=1"
	assertContains "Current-process owned-lock checks should fail closed when start-token lookup fails." \
		"$start_token_output" "token=1"
	assertContains "Current-process owned-lock checks should fail closed when metadata cannot be loaded." \
		"$load_failure_output" "load=1"
	assertContains "Owned lock release should fail closed when directory cleanup fails after ownership validation succeeds." \
		"$release_output" "release=1"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
