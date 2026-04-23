#!/bin/sh
#
# shunit2 tests for zxfer_locking.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_locking.sh"

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
	l_kind=$2
	l_purpose=$3
	l_pid=${4:-$$}
	l_start_token=${5:-}
	l_hostname=${6:-}
	l_created_at=${7:-}

	mkdir -p "$l_lock_dir" || fail "Unable to create owned lock fixture directory."
	chmod 700 "$l_lock_dir" || fail "Unable to chmod owned lock fixture directory."
	if [ -z "$l_start_token" ]; then
		l_start_token=$(zxfer_get_process_start_token "$$" 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture start token."
	fi
	if [ -z "$l_hostname" ]; then
		l_hostname=$(zxfer_get_owned_lock_hostname 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture hostname."
	fi
	if [ -z "$l_created_at" ]; then
		l_created_at=$(zxfer_get_owned_lock_created_at 2>/dev/null) ||
			fail "Unable to derive an owned lock fixture creation timestamp."
	fi

	cat >"$l_lock_dir/metadata" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	$l_kind
purpose	$l_purpose
pid	$l_pid
start_token	$l_start_token
hostname	$l_hostname
created_at	$l_created_at
EOF
	chmod 600 "$l_lock_dir/metadata" || fail "Unable to chmod owned lock fixture metadata."
}

test_zxfer_get_process_start_token_returns_nonempty_token_for_current_process() {
	token=$(zxfer_get_process_start_token "$$")

	assertContains "Process-start tokens should include the selector prefix." \
		"$token" ":"
	assertNotEquals "Process-start tokens should not be empty for the current process." \
		"" "$token"
}

test_owned_lock_helper_validators_cover_invalid_inputs_and_fallbacks() {
	bad_text=$(printf 'bad\tvalue')

	output=$(
		(
			set +e
			zxfer_validate_owned_lock_kind "invalid" >/dev/null
			printf 'kind=%s\n' "$?"
			zxfer_validate_owned_lock_text_field "" >/dev/null
			printf 'empty=%s\n' "$?"
			zxfer_validate_owned_lock_text_field "$bad_text" >/dev/null
			printf 'tab=%s\n' "$?"
			normalized=$(zxfer_normalize_owned_lock_text_field "  lock   purpose  ")
			printf 'normalized=<%s>\n' "$normalized"
			zxfer_normalize_owned_lock_text_field "   " >/dev/null
			printf 'blank_norm=%s\n' "$?"
			zxfer_get_process_start_token "invalid" >/dev/null
			printf 'invalid_pid=%s\n' "$?"
			zxfer_create_owned_lock_dir "" lock "purpose" >/dev/null
			printf 'blank_create=%s\n' "$?"
			zxfer_create_owned_lock_dir "$TEST_TMPDIR/invalid-kind.lock" bogus "purpose" >/dev/null
			printf 'invalid_kind=%s\n' "$?"
			bad_purpose=$(printf 'bad\npurpose')
			zxfer_create_owned_lock_dir "$TEST_TMPDIR/invalid-purpose.lock" lock "$bad_purpose" >/dev/null
			printf 'invalid_purpose=%s\n' "$?"
		)
	)
	hostname_output=$(
		(
			set +e
			uname() {
				return 1
			}
			hostname() {
				printf '\n'
			}
			zxfer_get_owned_lock_hostname >/dev/null
			printf 'hostname=%s\n' "$?"
		)
	)
	created_output=$(
		(
			set +e
			date() {
				printf '\n'
			}
			zxfer_get_owned_lock_created_at >/dev/null
			printf 'created=%s\n' "$?"
		)
	)
	ps_output=$(
		(
			set +e
			ps() {
				if [ "$4" = "lstart=" ]; then
					printf '   \n'
					return 0
				fi
				if [ "$4" = "start=" ]; then
					printf 'Apr 13 12:00\n'
					return 0
				fi
				return 1
			}
			token=$(zxfer_get_process_start_token "$$")
			printf 'fallback=<%s>\n' "$token"
		)
	)
	ps_failure_output=$(
		(
			set +e
			ps() {
				return 1
			}
			zxfer_get_process_start_token_from_procfs() {
				return 1
			}
			zxfer_get_process_start_token "$$" >/dev/null
			printf 'ps=%s\n' "$?"
		)
	)
	procfs_output=$(
		(
			set +e
			ps() {
				return 1
			}
			zxfer_get_process_start_token_from_procfs() {
				printf '%s\n' "procfs:bootid:4242"
			}
			token=$(zxfer_get_process_start_token "$$")
			printf 'procfs=<%s>\n' "$token"
		)
	)

	assertContains "Owned lock kinds should reject unsupported values." \
		"$output" "kind=1"
	assertContains "Owned lock text fields should reject empty strings." \
		"$output" "empty=1"
	assertContains "Owned lock text fields should reject tabs." \
		"$output" "tab=1"
	assertContains "Owned lock text fields should normalize surrounding and repeated spaces." \
		"$output" "normalized=<lock purpose>"
	assertContains "Owned lock text normalization should reject values that collapse to empty." \
		"$output" "blank_norm=1"
	assertContains "Owned lock start-token lookup should reject invalid PIDs." \
		"$output" "invalid_pid=1"
	assertContains "Owned lock directory creation should reject blank target paths." \
		"$output" "blank_create=1"
	assertContains "Owned lock directory creation should reject invalid lock kinds." \
		"$output" "invalid_kind=1"
	assertContains "Owned lock directory creation should reject invalid purposes before creating any directory state." \
		"$output" "invalid_purpose=1"
	assertContains "Owned lock hostname lookup should fail closed when the hostname cannot be normalized." \
		"$hostname_output" "hostname=1"
	assertContains "Owned lock creation timestamps should fail closed when date output is unusable." \
		"$created_output" "created=1"
	assertContains "Owned lock start-token lookup should fall back to the next supported ps selector when an earlier selector normalizes to empty output." \
		"$ps_output" "fallback=<start:Apr 13 12:00>"
	assertContains "Owned lock start-token lookup should fall back to the procfs token path when every ps selector fails." \
		"$procfs_output" "procfs=<procfs:bootid:4242>"
	assertContains "Owned lock start-token lookup should fail when every ps selector path fails." \
		"$ps_failure_output" "ps=1"
}

test_owned_lock_text_and_procfs_helpers_cover_current_shell_paths() {
	normalized_file="$TEST_TMPDIR/owned-lock-normalized.out"

	output=$(
		(
			set +e
			tab=$(printf '\t')
			zxfer_normalize_owned_lock_text_field \
				"  lock${tab}purpose   with   spaces  " >"$normalized_file"
			printf 'normalize=%s\n' "$?"
			zxfer_get_process_start_token_from_procfs "" >/dev/null
			printf 'empty_pid=%s\n' "$?"
			zxfer_get_process_start_token_from_procfs "not-a-pid" >/dev/null
			printf 'invalid_pid=%s\n' "$?"
		)
	)

	assertContains "Owned-lock text normalization should succeed after collapsing tabs and repeated spaces into a single validated field." \
		"$output" "normalize=0"
	assertEquals "Owned-lock text normalization should trim surrounding whitespace and squeeze internal runs to single spaces." \
		"lock purpose with spaces" "$(cat "$normalized_file")"
	assertContains "Procfs start-token lookups should reject empty pid inputs before probing procfs." \
		"$output" "empty_pid=1"
	assertContains "Procfs start-token lookups should reject nonnumeric pid inputs before probing procfs." \
		"$output" "invalid_pid=1"
}

test_zxfer_normalize_owned_lock_text_field_runs_in_current_shell() {
	tab=$(printf '\t')
	normalized_value=$(zxfer_normalize_owned_lock_text_field "  owned${tab}lock   value  ")

	assertEquals "Current-shell owned-lock text normalization should trim and squeeze whitespace in one validated pass." \
		"owned lock value" "$normalized_value"
}

test_zxfer_get_process_start_token_from_procfs_falls_back_to_btime_when_boot_id_is_unusable() {
	if [ ! -r "/proc/$$/stat" ]; then
		assertTrue "Procfs start-token fallback coverage is skipped on hosts without readable /proc support." "true"
		return 0
	fi

	fake_tools_dir="$TEST_TMPDIR/procfs_fallback_tools"
	real_cat=$(command -v cat)
	real_awk=$(command -v awk)
	mkdir -p "$fake_tools_dir" || fail "Unable to create fake procfs helper directory."
	cat >"$fake_tools_dir/cat" <<EOF
#!/bin/sh
case "\$1" in
/proc/sys/kernel/random/boot_id)
	printf '   \n'
	;;
*)
	exec "$real_cat" "\$@"
	;;
esac
EOF
	chmod 755 "$fake_tools_dir/cat" || fail "Unable to chmod fake cat helper."
	cat >"$fake_tools_dir/awk" <<EOF
#!/bin/sh
if [ "\$#" -ge 2 ] && [ "\$2" = "/proc/stat" ]; then
	printf '%s\n' "123456"
	exit 0
fi
exec "$real_awk" "\$@"
EOF
	chmod 755 "$fake_tools_dir/awk" || fail "Unable to chmod fake awk helper."

	output=$(
		(
			set +e
			PATH="$fake_tools_dir:$PATH"
			token=$(zxfer_get_process_start_token_from_procfs "$$")
			printf 'status=%s\n' "$?"
			printf 'token=%s\n' "$token"
		)
	)

	assertContains "Procfs start-token lookup should still succeed when it must fall back from boot_id to btime." \
		"$output" "status=0"
	assertContains "Procfs start-token lookup should encode the fallback btime value in the returned token." \
		"$output" "token=procfs:123456:"
}

test_zxfer_get_process_start_token_from_procfs_uses_unknown_boot_identity_when_boot_id_and_btime_are_unusable() {
	if [ ! -r "/proc/$$/stat" ]; then
		assertTrue "Procfs unknown-boot fallback coverage is skipped on hosts without readable /proc support." "true"
		return 0
	fi

	fake_tools_dir="$TEST_TMPDIR/procfs_unknown_tools"
	real_cat=$(command -v cat)
	real_awk=$(command -v awk)
	saved_path=$PATH
	mkdir -p "$fake_tools_dir" || fail "Unable to create fake procfs helper directory."
	cat >"$fake_tools_dir/cat" <<EOF
#!/bin/sh
case "\$1" in
/proc/sys/kernel/random/boot_id)
	printf '   \n'
	;;
*)
	exec "$real_cat" "\$@"
	;;
esac
EOF
	chmod 755 "$fake_tools_dir/cat" || fail "Unable to chmod fake cat helper."
	cat >"$fake_tools_dir/awk" <<EOF
#!/bin/sh
if [ "\$#" -ge 2 ] && [ "\$2" = "/proc/stat" ]; then
	printf '   \n'
	exit 0
fi
exec "$real_awk" "\$@"
EOF
	chmod 755 "$fake_tools_dir/awk" || fail "Unable to chmod fake awk helper."

	PATH="$fake_tools_dir:$PATH"
	token=$(zxfer_get_process_start_token_from_procfs "$$")
	status=$?
	PATH=$saved_path

	assertEquals "Procfs start-token lookup should still succeed when both boot_id and btime normalization fail." \
		0 "$status"
	assertContains "Procfs start-token lookup should fall back to an unknown boot identity when no normalized boot marker is available." \
		"$token" "procfs:unknown:"
}

test_zxfer_get_process_start_token_from_procfs_rejects_proc_stat_without_start_field_in_current_shell() {
	if [ ! -r "/proc/$$/stat" ]; then
		assertTrue "Procfs stat-shape validation coverage is skipped on hosts without readable /proc support." "true"
		return 0
	fi

	fake_tools_dir="$TEST_TMPDIR/procfs_short_stat_tools"
	saved_path=$PATH
	mkdir -p "$fake_tools_dir" || fail "Unable to create fake procfs short-stat helper directory."
	cat >"$fake_tools_dir/cat" <<EOF
#!/bin/sh
case "\$1" in
/proc/$$/stat)
	printf '%s\n' "$$ (zxfer) S 1 2 3"
	;;
*)
	exec /bin/cat "\$@"
	;;
esac
EOF
	chmod 755 "$fake_tools_dir/cat" || fail "Unable to chmod fake cat helper."

	PATH="$fake_tools_dir:$PATH"
	set +e
	zxfer_get_process_start_token_from_procfs "$$" >/dev/null
	status=$?
	set -e
	PATH=$saved_path

	assertEquals "Procfs start-token lookup should fail closed when /proc stat data does not include a parseable start field." \
		1 "$status"
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
			zxfer_load_owned_lock_metadata_for_kind_and_purpose \
				"$lock_dir" lock "roundtrip-lock"
			printf 'load=%s\n' "$?"
			printf 'kind=<%s>\n' "$g_zxfer_owned_lock_kind_result"
			printf 'purpose=<%s>\n' "$g_zxfer_owned_lock_purpose_result"
			printf 'pid=<%s>\n' "$g_zxfer_owned_lock_pid_result"
			printf 'start_token=<%s>\n' "$g_zxfer_owned_lock_start_token_result"
			printf 'hostname=<%s>\n' "$g_zxfer_owned_lock_hostname_result"
			printf 'created_at=<%s>\n' "$g_zxfer_owned_lock_created_at_result"
		)
	)

	assertContains "Owned lock creation should succeed for a valid metadata-backed lock dir." \
		"$output" "create=0"
	assertContains "Owned lock creation should create the metadata file." \
		"$output" "metadata=yes"
	assertContains "Owned lock metadata should reload cleanly after creation." \
		"$output" "load=0"
	assertContains "Owned lock metadata should preserve the lock kind." \
		"$output" "kind=<lock>"
	assertContains "Owned lock metadata should preserve the normalized purpose." \
		"$output" "purpose=<roundtrip-lock>"
	assertContains "Owned lock metadata should preserve the owning pid." \
		"$output" "pid=<$$>"
	assertNotContains "Owned lock metadata should preserve a non-empty process-start token." \
		"$output" "start_token=<>"
	assertNotContains "Owned lock metadata should preserve a non-empty hostname." \
		"$output" "hostname=<>"
	assertNotContains "Owned lock metadata should preserve a non-empty creation timestamp." \
		"$output" "created_at=<>"
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
			zxfer_get_owned_lock_hostname() {
				printf '%s\n' "direct-host"
			}
			zxfer_get_owned_lock_created_at() {
				printf '%s\n' "2026-04-20T00:00:00+0000"
			}
			zxfer_write_owned_lock_metadata_file \
				"$lock_dir" lease "  direct   purpose  " >/dev/null
			printf 'write=%s\n' "$?"
			zxfer_parse_owned_lock_metadata_file "$metadata_path" >/dev/null
			printf 'parse=%s\n' "$?"
			printf 'kind=<%s>\n' "$g_zxfer_owned_lock_kind_result"
			printf 'purpose=<%s>\n' "$g_zxfer_owned_lock_purpose_result"
			printf 'start_token=<%s>\n' "$g_zxfer_owned_lock_start_token_result"
			printf 'hostname=<%s>\n' "$g_zxfer_owned_lock_hostname_result"
			printf 'created_at=<%s>\n' "$g_zxfer_owned_lock_created_at_result"
		)
	)
	metadata_contents=$(cat "$metadata_path")

	assertContains "Owned lock metadata writes should succeed on the direct success path." \
		"$output" "write=0"
	assertContains "Owned lock metadata parsing should succeed on a freshly written direct metadata file." \
		"$output" "parse=0"
	assertContains "Direct metadata parsing should recover the written kind." \
		"$output" "kind=<lease>"
	assertContains "Direct metadata parsing should recover the normalized purpose." \
		"$output" "purpose=<direct purpose>"
	assertContains "Direct metadata parsing should recover the staged start token." \
		"$output" "start_token=<lstart:direct-test>"
	assertContains "Direct metadata parsing should recover the staged hostname." \
		"$output" "hostname=<direct-host>"
	assertContains "Direct metadata parsing should recover the staged creation timestamp." \
		"$output" "created_at=<2026-04-20T00:00:00+0000>"
	assertContains "Direct metadata writes should normalize the staged purpose line in the metadata file itself." \
		"$metadata_contents" "purpose${tab}direct purpose"
	assertContains "Direct metadata writes should store the staged start token in the metadata file itself." \
		"$metadata_contents" "start_token${tab}lstart:direct-test"
	assertContains "Direct metadata writes should store the staged hostname in the metadata file itself." \
		"$metadata_contents" "hostname${tab}direct-host"
}

test_zxfer_write_owned_lock_metadata_file_handles_invalid_inputs_and_publish_failures() {
	lock_dir="$TEST_TMPDIR/write-owned.lock"
	mkdir "$lock_dir" || fail "Unable to create owned lock metadata fixture directory."
	chmod 700 "$lock_dir" || fail "Unable to chmod owned lock metadata fixture directory."

	invalid_output=$(
		(
			set +e
			bad_purpose=$(printf 'bad\npurpose')
			zxfer_write_owned_lock_metadata_file "$lock_dir" bogus "purpose" >/dev/null
			printf 'kind=%s\n' "$?"
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "$bad_purpose" >/dev/null
			printf 'purpose=%s\n' "$?"
		)
	)
	token_output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				return 1
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "token-failure" >/dev/null
			printf 'token=%s\n' "$?"
		)
	)
	hostname_output=$(
		(
			set +e
			zxfer_get_owned_lock_hostname() {
				return 1
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "hostname-failure" >/dev/null
			printf 'hostname=%s\n' "$?"
		)
	)
	created_at_output=$(
		(
			set +e
			zxfer_get_owned_lock_created_at() {
				return 1
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "created-at-failure" >/dev/null
			printf 'created_at=%s\n' "$?"
		)
	)
	mktemp_output=$(
		(
			set +e
			mktemp() {
				return 1
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "mktemp-failure" >/dev/null
			printf 'mktemp=%s\n' "$?"
		)
	)
	block_output=$(
		(
			set +e
			block_target_dir="$TEST_TMPDIR/block-write-target"
			mkdir "$block_target_dir" || exit 1
			mktemp() {
				l_stage_link="$lock_dir/.metadata.block-link"
				rm -f "$l_stage_link" || return 1
				ln -s "$block_target_dir" "$l_stage_link" || return 1
				printf '%s\n' "$l_stage_link"
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "printf-block-failure" >/dev/null
			printf 'block=%s\n' "$?"
			printf 'leftover=%s\n' "$(find "$lock_dir" -maxdepth 1 -name '.metadata.*' -print)"
		)
	)
	write_stderr="$TEST_TMPDIR/write_owned_lock_metadata.stderr"
	write_output=$(
		(
			set +e
			mktemp() {
				printf '%s\n' "$TEST_TMPDIR/missing-stage/.metadata.fail"
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "write-failure" >/dev/null
			printf 'write=%s\n' "$?"
		) 2>"$write_stderr"
	)
	publish_output=$(
		(
			set +e
			mv() {
				return 1
			}
			zxfer_write_owned_lock_metadata_file "$lock_dir" lock "publish-failure" >/dev/null
			printf 'publish=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata writes should reject unsupported kinds." \
		"$invalid_output" "kind=1"
	assertContains "Owned lock metadata writes should reject unnormalizable purposes." \
		"$invalid_output" "purpose=1"
	assertContains "Owned lock metadata writes should fail closed when the current start token is unavailable." \
		"$token_output" "token=1"
	assertContains "Owned lock metadata writes should fail closed when the current hostname is unavailable." \
		"$hostname_output" "hostname=1"
	assertContains "Owned lock metadata writes should fail closed when the creation timestamp cannot be derived." \
		"$created_at_output" "created_at=1"
	assertContains "Owned lock metadata writes should fail closed when they cannot allocate a staged metadata file." \
		"$mktemp_output" "mktemp=1"
	assertContains "Owned lock metadata writes should fail closed when the staged metadata block cannot be written after staging succeeds." \
		"$block_output" "block=1"
	assertContains "Owned lock metadata writes should clean up staged metadata files when the metadata block write fails." \
		"$block_output" "leftover="
	assertContains "Owned lock metadata writes should fail closed when the staged metadata file cannot be written." \
		"$write_output" "write=1"
	assertEquals "Owned lock metadata write failures should not leak raw shell redirection errors." \
		"" "$(cat "$write_stderr")"
	assertContains "Owned lock metadata writes should fail closed when publishing the staged metadata file fails." \
		"$publish_output" "publish=1"
}

test_zxfer_parse_owned_lock_metadata_file_rejects_malformed_payloads_and_mismatches() {
	invalid_pid_path="$TEST_TMPDIR/invalid-pid.metadata"
	invalid_layout_path="$TEST_TMPDIR/invalid-layout.metadata"
	invalid_no_tab_path="$TEST_TMPDIR/invalid-no-tab.metadata"
	invalid_tab_value_path="$TEST_TMPDIR/invalid-tab-value.metadata"
	invalid_key_path="$TEST_TMPDIR/invalid-key.metadata"
	lock_dir="$TEST_TMPDIR/mismatch.lock"

	cat >"$invalid_pid_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	lock
purpose	mismatch-lock
pid	not-a-pid
start_token	lstart:test
hostname	host
created_at	2026-04-13T00:00:00+0000
EOF
	cat >"$invalid_layout_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	lock
purpose	mismatch-lock
pid	123
start_token	lstart:test
hostname	host
created_at	2026-04-13T00:00:00+0000
extra	line
EOF
	cat >"$invalid_no_tab_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	lock
purpose	mismatch-lock
pid	123
start_token	lstart:test
hostname host
created_at	2026-04-13T00:00:00+0000
EOF
	cat >"$invalid_tab_value_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	lock
purpose	mismatch-lock
pid	123
start_token	lstart:test	extra
hostname	host
created_at	2026-04-13T00:00:00+0000
EOF
	cat >"$invalid_key_path" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	lock
purpose	mismatch-lock
pid	123
start_token	lstart:test
host_name	host
created_at	2026-04-13T00:00:00+0000
EOF
	chmod 600 "$invalid_pid_path" "$invalid_layout_path" "$invalid_no_tab_path" "$invalid_tab_value_path" "$invalid_key_path" ||
		fail "Unable to chmod malformed metadata fixtures."
	write_owned_lock_metadata_fixture "$lock_dir" lock "mismatch-lock"

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
			bad_purpose=$(printf 'bad\npurpose')
			zxfer_load_owned_lock_metadata_for_kind_and_purpose \
				"$lock_dir" lock "$bad_purpose" >/dev/null
			printf 'bad_purpose=%s\n' "$?"
			zxfer_load_owned_lock_metadata_for_kind_and_purpose \
				"$lock_dir" lock "wrong-purpose" >/dev/null
			printf 'mismatch=%s\n' "$?"
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
	assertContains "Owned lock metadata loading should reject invalid requested purposes before comparing metadata." \
		"$output" "bad_purpose=1"
	assertContains "Owned lock metadata loading should report mismatched purpose lookups as corrupt or incompatible." \
		"$output" "mismatch=2"
}

test_zxfer_load_owned_lock_metadata_helpers_distinguish_missing_malformed_and_kind_mismatches() {
	missing_dir="$TEST_TMPDIR/missing-metadata.lock"
	malformed_dir="$TEST_TMPDIR/malformed-metadata.lock"
	valid_dir="$TEST_TMPDIR/valid-kind-mismatch.lock"

	mkdir "$missing_dir" "$malformed_dir" || fail "Unable to create owned lock metadata loader fixtures."
	chmod 700 "$missing_dir" "$malformed_dir" || fail "Unable to chmod owned lock metadata loader fixtures."
	cat >"$malformed_dir/metadata" <<EOF
$ZXFER_LOCK_METADATA_HEADER
kind	lock
purpose	mismatch-lock
pid	123
start_token	lstart:test
host_name	host
created_at	2026-04-13T00:00:00+0000
EOF
	chmod 600 "$malformed_dir/metadata" || fail "Unable to chmod malformed owned lock metadata loader fixture."
	write_owned_lock_metadata_fixture "$valid_dir" lock "mismatch-lock"

	output=$(
		(
			set +e
			zxfer_load_owned_lock_metadata_from_dir "$missing_dir" >/dev/null
			printf 'missing=%s\n' "$?"
			zxfer_load_owned_lock_metadata_from_dir "$malformed_dir" >/dev/null
			printf 'malformed=%s\n' "$?"
			zxfer_load_owned_lock_metadata_for_kind_and_purpose \
				"$valid_dir" lease "mismatch-lock" >/dev/null
			printf 'kind=%s\n' "$?"
		)
	)

	assertContains "Owned lock metadata loading should treat missing metadata files as corrupt or incomplete state." \
		"$output" "missing=2"
	assertContains "Owned lock metadata loading should treat malformed metadata payloads as corrupt state." \
		"$output" "malformed=2"
	assertContains "Owned lock metadata loading should treat kind mismatches as incompatible metadata." \
		"$output" "kind=2"
}

test_zxfer_try_reap_stale_owned_lock_dir_distinguishes_live_and_stale_owners() {
	live_lock_dir="$TEST_TMPDIR/live.lock"
	stale_lock_dir="$TEST_TMPDIR/stale.lock"

	zxfer_create_owned_lock_dir "$live_lock_dir" lock "live-lock" >/dev/null
	write_owned_lock_metadata_fixture "$stale_lock_dir" lock "stale-lock" "999999999"

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
	current_hostname=$(zxfer_get_owned_lock_hostname) ||
		fail "Unable to derive owned lock test hostname."
	file_path="$TEST_TMPDIR/not-a-lock-file"
	target_dir="$TEST_TMPDIR/cleanup-target.lock"
	link_path="$TEST_TMPDIR/cleanup-link.lock"
	: >"$file_path" || fail "Unable to create owned lock cleanup file fixture."
	mkdir "$target_dir" || fail "Unable to create owned lock cleanup target directory."
	ln -s "$target_dir" "$link_path" || fail "Unable to create owned lock cleanup symlink."

	liveness_output=$(
		(
			set +e
			zxfer_owned_lock_owner_is_live "$$" "$current_token" "other-host" >/dev/null
			printf 'host=%s\n' "$?"
			zxfer_owned_lock_owner_is_live "999999999" "$current_token" "$current_hostname" >/dev/null
			printf 'stale=%s\n' "$?"
			zxfer_owned_lock_owner_is_live "$$" "${current_token}mismatch" "$current_hostname" >/dev/null
			printf 'token_mismatch=%s\n' "$?"
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
			zxfer_owned_lock_owner_is_live "$$" "$current_token" "$current_hostname" >/dev/null
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

	assertContains "Owned lock liveness should treat hostname mismatches as stale owners." \
		"$liveness_output" "host=1"
	assertContains "Owned lock liveness should treat dead PIDs on the local host as stale owners." \
		"$liveness_output" "stale=1"
	assertContains "Owned lock liveness should treat mismatched start tokens for a live PID as stale owners." \
		"$liveness_output" "token_mismatch=1"
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

test_owned_lock_owner_and_cleanup_helpers_fail_closed_when_hostname_lookup_or_rm_failures_persist() {
	lock_dir="$TEST_TMPDIR/cleanup-hard-fail.lock"
	mkdir "$lock_dir" || fail "Unable to create hard-fail owned lock cleanup fixture."

	hostname_output=$(
		(
			set +e
			zxfer_get_owned_lock_hostname() {
				return 1
			}
			zxfer_owned_lock_owner_is_live "$$" "lstart:test" "host" >/dev/null
			printf 'hostname=%s\n' "$?"
		)
	)
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

	assertContains "Owned lock liveness should fail closed when it cannot determine the current hostname." \
		"$hostname_output" "hostname=2"
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

test_zxfer_create_owned_lock_dir_in_parent_and_release_owned_locks_cover_success_and_failures() {
	parent_dir="$TEST_TMPDIR/parent-owned"
	insecure_parent_dir="$TEST_TMPDIR/insecure-parent-owned"
	mkdir "$parent_dir" "$insecure_parent_dir" || fail "Unable to create owned lock parent directories."
	chmod 700 "$parent_dir" || fail "Unable to chmod owned lock parent directory."
	chmod 755 "$insecure_parent_dir" || fail "Unable to chmod insecure owned lock parent directory."

	success_output=$(
		(
			set +e
			lock_dir=$(zxfer_create_owned_lock_dir_in_parent \
				"$parent_dir" "lease" lease "lease-purpose")
			printf 'create=%s\n' "$?"
			zxfer_current_process_owns_owned_lock_dir "$lock_dir" lease "lease-purpose"
			printf 'owns=%s\n' "$?"
			zxfer_register_owned_lock_path "$lock_dir"
			zxfer_release_owned_lock_dir "$lock_dir" lease "lease-purpose" >/dev/null
			printf 'release=%s\n' "$?"
			if [ -e "$lock_dir" ]; then
				printf 'exists=yes\n'
			else
				printf 'exists=no\n'
			fi
			printf 'registered=<%s>\n' "${g_zxfer_owned_lock_cleanup_paths:-}"
		)
	)
	missing_release_output=$(
		(
			set +e
			zxfer_release_owned_lock_dir "$TEST_TMPDIR/missing-release.lock" >/dev/null
			printf 'missing=%s\n' "$?"
		)
	)
	parent_fail_output=$(
		(
			set +e
			zxfer_create_owned_lock_dir_in_parent \
				"$insecure_parent_dir" "lease" lease "lease-purpose" >/dev/null
			printf 'parent=%s\n' "$?"
		)
	)
	mktemp_fail_output=$(
		(
			set +e
			mktemp() {
				return 1
			}
			zxfer_create_owned_lock_dir_in_parent \
				"$parent_dir" "lease" lease "lease-purpose" >/dev/null
			printf 'mktemp=%s\n' "$?"
		)
	)
	validate_child_output=$(
		(
			set +e
			zxfer_validate_owned_lock_container_dir() {
				[ "$1" = "$parent_dir" ] && return 0
				return 1
			}
			zxfer_create_owned_lock_dir_in_parent \
				"$parent_dir" "lease" lease "lease-purpose" >/dev/null
			printf 'validate=%s\n' "$?"
		)
	)
	write_child_output=$(
		(
			set +e
			zxfer_write_owned_lock_metadata_file() {
				return 1
			}
			zxfer_create_owned_lock_dir_in_parent \
				"$parent_dir" "lease" lease "lease-purpose" >/dev/null
			printf 'write=%s\n' "$?"
		)
	)

	assertContains "Parent-scoped owned lock creation should succeed for validated parents." \
		"$success_output" "create=0"
	assertContains "Current-process ownership checks should accept freshly created owned locks." \
		"$success_output" "owns=0"
	assertContains "Owned lock release should remove directories owned by the current process." \
		"$success_output" "release=0"
	assertContains "Successful owned lock release should remove the owned directory." \
		"$success_output" "exists=no"
	assertContains "Successful owned lock release should unregister cleanup state." \
		"$success_output" "registered=<>"
	assertContains "Releasing missing owned locks should succeed after unregistering the path." \
		"$missing_release_output" "missing=0"
	assertContains "Parent-scoped owned lock creation should reject insecure parents." \
		"$parent_fail_output" "parent=1"
	assertContains "Parent-scoped owned lock creation should fail closed when mktemp cannot allocate a directory." \
		"$mktemp_fail_output" "mktemp=1"
	assertContains "Parent-scoped owned lock creation should fail closed when the created child directory cannot be validated." \
		"$validate_child_output" "validate=1"
	assertContains "Parent-scoped owned lock creation should fail closed when metadata publication fails." \
		"$write_child_output" "write=1"
}

test_zxfer_try_reap_stale_owned_lock_dir_propagates_unknown_states_and_cleanup_failures() {
	liveness_dir="$TEST_TMPDIR/reap-liveness.lock"
	cleanup_dir="$TEST_TMPDIR/reap-cleanup.lock"
	write_owned_lock_metadata_fixture "$liveness_dir" lock "reap-liveness"
	write_owned_lock_metadata_fixture "$cleanup_dir" lock "reap-cleanup" "999999999"

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
		"$lock_dir" lock "release-mismatch" "$$" "lstart:not-the-current-process"

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

test_zxfer_warn_owned_lock_cleanup_failure_falls_back_to_stderr_without_warning_helper() {
	warnings=$(
		(
			unset zxfer_warn_stderr >/dev/null 2>&1 || :
			zxfer_warn_owned_lock_cleanup_failure "$TEST_TMPDIR/fallback.lock" 9
		) 2>&1
	)

	assertContains "Owned lock cleanup warnings should fall back to direct stderr output when reporting helpers are unavailable." \
		"$warnings" "status 9"
}

test_zxfer_release_registered_owned_locks_warns_and_preserves_remaining_paths() {
	cleanup_paths=$(printf '%s\n%s\n' \
		"$TEST_TMPDIR/first.lock" "$TEST_TMPDIR/second.lock")
	stdout_file="$TEST_TMPDIR/owned_lock_release.stdout"
	stderr_file="$TEST_TMPDIR/owned_lock_release.stderr"

	(
		set +e
		g_zxfer_owned_lock_cleanup_paths=$cleanup_paths
		zxfer_release_owned_lock_dir() {
			case "$1" in
			"$TEST_TMPDIR/first.lock")
				return 0
				;;
			esac
			return 23
		}
		zxfer_release_registered_owned_locks
		printf 'status=%s\n' "$?"
		printf 'remaining=<%s>\n' "$g_zxfer_owned_lock_cleanup_paths"
	) >"$stdout_file" 2>"$stderr_file"
	output=$(cat "$stdout_file")
	warnings=$(cat "$stderr_file")

	assertContains "Registered owned-lock cleanup should fail closed when one release fails." \
		"$output" "status=1"
	assertContains "Registered owned-lock cleanup should warn with the release status for failed paths." \
		"$warnings" "status 23"
	assertContains "Registered owned-lock cleanup should keep failed paths registered for later cleanup." \
		"$output" "remaining=<$TEST_TMPDIR/second.lock>"
}

test_zxfer_owned_lock_cleanup_conflicts_with_path_normalizes_parent_aliases() {
	physical_root="$TEST_TMPDIR/owned-lock-path-physical"
	alias_root="$TEST_TMPDIR/owned-lock-path-alias"
	registered_lock_path="$alias_root/entry/lease.lock"
	physical_entry_dir="$physical_root/entry"

	mkdir -p "$physical_entry_dir" || fail "Unable to create the physical owned-lock cleanup alias fixture."
	ln -s "$physical_root" "$alias_root" ||
		fail "Unable to create the owned-lock cleanup alias symlink."

	output=$(
		(
			g_zxfer_owned_lock_cleanup_paths=$registered_lock_path
			zxfer_owned_lock_cleanup_conflicts_with_path "$physical_entry_dir" >/dev/null
			printf 'status=%s\n' "$?"
		)
	)

	assertContains "Owned-lock cleanup conflict checks should resolve parent-directory aliases before deciding whether a cleanup target overlaps a registered owned lock." \
		"$output" "status=0"
}

test_zxfer_owned_lock_cleanup_path_helpers_cover_relative_root_and_failure_paths() {
	relative_output=$(zxfer_normalize_owned_lock_cleanup_path "relative/path")
	root_output=$(zxfer_normalize_owned_lock_cleanup_path "/root-lock")

	mkdir -p "$TEST_TMPDIR/owned-lock-conflict-root" ||
		fail "Unable to create the owned-lock conflict fixture root."
	output=$(
		(
			set +e
			g_zxfer_owned_lock_cleanup_paths=$(printf '%s\n%s\n' \
				"$TEST_TMPDIR/missing-parent/lease.lock" \
				"$TEST_TMPDIR/owned-lock-conflict-root/lease.lock")
			zxfer_owned_lock_cleanup_conflicts_with_path \
				"$TEST_TMPDIR/owned-lock-conflict-root" >/dev/null
			printf 'ancestor=%s\n' "$?"
			g_zxfer_owned_lock_cleanup_paths="$TEST_TMPDIR/owned-lock-conflict-exact"
			zxfer_owned_lock_cleanup_conflicts_with_path \
				"$TEST_TMPDIR/owned-lock-conflict-exact" >/dev/null
			printf 'exact=%s\n' "$?"
			zxfer_owned_lock_cleanup_conflicts_with_path \
				"$TEST_TMPDIR/missing-parent/candidate" >/dev/null
			printf 'missing=%s\n' "$?"
		)
	)

	assertEquals "Owned-lock cleanup path normalization should preserve relative paths as-is." \
		"relative/path" "$relative_output"
	assertEquals "Owned-lock cleanup path normalization should preserve absolute paths under root after normalization." \
		"/root-lock" "$root_output"
	assertContains "Owned-lock cleanup conflict checks should skip registered paths whose parents can no longer be normalized and still detect ancestor conflicts." \
		"$output" "ancestor=0"
	assertContains "Owned-lock cleanup conflict checks should treat exact path matches as conflicts." \
		"$output" "exact=0"
	assertContains "Owned-lock cleanup conflict checks should fail closed when the cleanup candidate path cannot be normalized." \
		"$output" "missing=1"
}

test_owned_lock_validation_and_release_helpers_cover_lookup_failures() {
	lock_dir="$TEST_TMPDIR/lookup-fail.lock"
	metadata_path="$lock_dir/metadata"
	write_owned_lock_metadata_fixture "$lock_dir" lock "lookup-fail"

	normalize_output=$(
		(
			set +e
			zxfer_get_path_parent_dir() {
				return 1
			}
			zxfer_normalize_owned_lock_cleanup_path "$lock_dir" >/dev/null
			printf 'normalize=%s\n' "$?"
		)
	)
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
	hostname_output=$(
		(
			set +e
			zxfer_get_owned_lock_hostname() {
				return 1
			}
			zxfer_current_process_owns_owned_lock_dir "$lock_dir" lock "lookup-fail" >/dev/null
			printf 'hostname=%s\n' "$?"
		)
	)
	start_token_output=$(
		(
			set +e
			zxfer_get_process_start_token() {
				return 1
			}
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
			g_zxfer_owned_lock_cleanup_paths=$lock_dir
			zxfer_cleanup_owned_lock_dir() {
				return 1
			}
			zxfer_release_owned_lock_dir "$lock_dir" lock "lookup-fail" >/dev/null
			printf 'release=%s\n' "$?"
			printf 'remaining=%s\n' "$g_zxfer_owned_lock_cleanup_paths"
		)
	)

	assertContains "Owned-lock cleanup path normalization should fail closed when parent lookup fails." \
		"$normalize_output" "normalize=1"
	assertContains "Owned lock metadata validation should fail closed when effective-uid lookup fails." \
		"$uid_output" "uid=1"
	assertContains "Owned lock metadata validation should fail closed when owner lookup fails." \
		"$owner_output" "owner=1"
	assertContains "Owned lock metadata validation should fail closed when mode lookup fails." \
		"$mode_output" "mode=1"
	assertContains "Current-process owned-lock checks should fail closed when hostname lookup fails." \
		"$hostname_output" "hostname=1"
	assertContains "Current-process owned-lock checks should fail closed when start-token lookup fails." \
		"$start_token_output" "token=1"
	assertContains "Current-process owned-lock checks should fail closed when metadata cannot be loaded without an explicit kind or purpose." \
		"$load_failure_output" "load=1"
	assertContains "Owned lock release should fail closed when directory cleanup fails after ownership validation succeeds." \
		"$release_output" "release=1"
	assertContains "Owned lock release should preserve the registered cleanup path when cleanup fails." \
		"$release_output" "remaining=$lock_dir"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
