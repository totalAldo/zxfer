#!/bin/sh
# Path validation tests for src/zxfer_path_security.sh: backup-file owner and
# mode checks, symlink path components, trusted root symlinks, and temp-root
# candidates. Run by tests/test_zxfer_path_security.sh, whose fake ls and id
# (zxfer_path_security_test_write_fake_tools) they use.
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

find_trusted_root_symlink_for_tests() {
	for l_candidate in /tmp /bin /sbin /lib /lib64 /home /var/run /var/lock /*; do
		[ -L "$l_candidate" ] || continue
		if zxfer_is_trusted_symlink_path_component "$l_candidate" >/dev/null 2>&1; then
			printf '%s\n' "$l_candidate"
			return 0
		fi
	done

	return 1
}

require_trusted_root_symlink_for_tests() {
	trusted_root_symlink=$(find_trusted_root_symlink_for_tests) || {
		startSkipping
		return 1
	}

	return 0
}

test_backup_owner_uid_is_allowed_accepts_root_and_effective_uid() {
	result=$(
		zxfer_get_effective_user_uid() { g_zxfer_effective_uid=4242; }
		for l_owner in 0 4242 4343; do
			if zxfer_backup_owner_uid_is_allowed "$l_owner"; then
				printf '%s=ok\n' "$l_owner"
			else
				printf '%s=refused\n' "$l_owner"
			fi
		done
		zxfer_get_effective_user_uid() { return 1; }
		zxfer_backup_owner_uid_is_allowed 4242 && echo unknown=ok || echo unknown=refused
		zxfer_backup_owner_uid_is_allowed 0 && echo unknown_root=ok || echo unknown_root=refused
	)
	assertEquals "Root and the effective UID are allowed, another owner is not, and an unknown effective UID allows only root." \
		"0=ok
4242=ok
4343=refused
unknown=refused
unknown_root=ok" "$result"
}

test_describe_expected_backup_owner_includes_effective_uid_when_non_root() {
	result=$(
		zxfer_get_effective_user_uid() { g_zxfer_effective_uid=9999; }
		zxfer_describe_expected_backup_owner
		zxfer_get_effective_user_uid() { g_zxfer_effective_uid=0; }
		zxfer_describe_expected_backup_owner
		zxfer_get_effective_user_uid() { return 1; }
		zxfer_describe_expected_backup_owner
	)
	assertEquals "root (UID 0) or UID 9999
root (UID 0)
root (UID 0)" "$result"
}

test_zxfer_get_path_parent_dir_handles_root_and_relative_inputs() {
	assertEquals "Absolute paths should return their containing directory." \
		"/var/log" "$(zxfer_get_path_parent_dir "/var/log/zxfer.log")"
	assertEquals "Paths without a slash should fall back to root for parent-dir validation." \
		"/" "$(zxfer_get_path_parent_dir "zxfer.log")"
}

# The first untrusted symlink component is reported as given, for absolute
# and relative paths. A root-owned top-level system symlink (such as /tmp ->
# private/tmp on macOS) is skipped silently; a host without one skips that row.
test_zxfer_find_symlink_path_component_reports_the_first_untrusted_symlink() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/find_real/subdir" "$physical_tmpdir/find_plain/subdir"
	ln -s "$physical_tmpdir/find_real" "$physical_tmpdir/find_link"
	trusted_root_symlink=$(find_trusted_root_symlink_for_tests) || trusted_root_symlink=""

	zxfer_test_capture_subshell '
		cd "$physical_tmpdir" || exit 1
		for l_path in "$physical_tmpdir/find_link/subdir/file" \
			./find_link/subdir/file ./find_plain/subdir/file; do
			l_component=$(zxfer_find_symlink_path_component "$l_path")
			printf "%s=%s <%s>\n" "$l_path" "$?" "$l_component"
		done
		if [ -n "$trusted_root_symlink" ]; then
			l_component=$(zxfer_find_symlink_path_component \
				"$trusted_root_symlink/zxfer-trusted-root-symlink-probe/subdir/file")
			printf "trusted=%s <%s>\n" "$?" "$l_component"
		fi
	'
	expected="$physical_tmpdir/find_link/subdir/file=0 <$physical_tmpdir/find_link>
./find_link/subdir/file=0 <./find_link>
./find_plain/subdir/file=1 <>"
	[ -z "$trusted_root_symlink" ] || expected="$expected
trusted=1 <>"

	assertEquals "Only untrusted symlink components should be reported, and nothing else printed." \
		"$expected" "$ZXFER_TEST_CAPTURE_OUTPUT"
	[ -n "$trusted_root_symlink" ] || startSkipping
	assertNotEquals "The host should expose a trusted top-level symlink to skip." \
		"" "$trusted_root_symlink"
}

test_zxfer_is_trusted_symlink_path_component_fails_closed_on_each_metadata_check() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	good_link="7 lrwxrwxrwx 1 0 0 11 Jan  1 00:00 $trusted_root_symlink -> target"
	good_root="2 drwxr-xr-x 23 0 0 736 Jan  1 00:00 /"
	zxfer_test_capture_subshell '
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		export FAKE_LS_ANSWERS
		while IFS="|" read -r l_case l_link l_root; do
			FAKE_LS_ANSWERS="'"$trusted_root_symlink"'|$l_link
/|$l_root"
			zxfer_is_trusted_symlink_path_component "'"$trusted_root_symlink"'"
			printf "%s=%s\n" "$l_case" "$?"
		done <<EOF
trusted|'"$good_link"'|'"$good_root"'
link_unreadable|FAIL|'"$good_root"'
link_not_root|7 lrwxrwxrwx 1 501 0 11 Jan  1 00:00 x -> y|'"$good_root"'
root_unreadable|'"$good_link"'|FAIL
root_not_root|'"$good_link"'|2 drwxr-xr-x 23 501 0 736 Jan  1 00:00 /
root_unparseable|'"$good_link"'|bad-perms
root_world_writable|'"$good_link"'|2 drwxrwxrwx 23 0 0 736 Jan  1 00:00 /
root_sticky|'"$good_link"'|2 drwxrwxrwt 23 0 0 736 Jan  1 00:00 /
EOF
	'

	assertEquals "A top-level symlink is trusted only when it and / are root-owned and / is not writable by others unless sticky." \
		"trusted=0
link_unreadable=1
link_not_root=1
root_unreadable=1
root_not_root=1
root_unparseable=1
root_world_writable=1
root_sticky=0" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_nested_symlinks_without_reading_metadata() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_nested"
	tool_log="$TEST_TMPDIR/trusted_symlink_nested.log"
	mkdir -p "$symlink_parent/target"
	ln -sf "$symlink_parent/target" "$symlink_parent/link"
	rm -f "$tool_log"

	zxfer_test_capture_subshell '
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_TOOL_LOG="'"$tool_log"'"
		FAKE_LS_ANSWERS="*|7 lrwxrwxrwx 1 0 0 11 Jan  1 00:00 x -> y"
		export FAKE_TOOL_LOG FAKE_LS_ANSWERS
		zxfer_is_trusted_symlink_path_component "'"$symlink_parent/link"'"
		printf "nested=%s\n" "$?"
		zxfer_is_trusted_symlink_path_component relative-link
		printf "relative=%s\n" "$?"
		zxfer_is_trusted_symlink_path_component /
		printf "root=%s\n" "$?"
	'

	assertEquals "Only a top-level absolute symlink can be trusted." \
		"nested=1
relative=1
root=1" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertFalse "Rejecting by position must not read any metadata." "[ -s '$tool_log' ]"
}

test_zxfer_require_backup_metadata_path_without_symlinks_rejects_symlink_target() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_file="$physical_tmpdir/backup.meta.real"
	link_file="$physical_tmpdir/backup.meta.link"
	: >"$real_file"
	ln -s "$real_file" "$link_file"

	zxfer_test_capture_subshell "
		zxfer_require_backup_metadata_path_without_symlinks \"$link_file\"
	"

	assertEquals "Exact backup metadata symlink paths should be rejected." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Exact backup metadata symlink rejections should identify the symlink itself." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Refusing to use backup metadata $link_file because it is a symlink."
	assertNotContains "An exact symlink should get one refusal, not a second path-component line." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "because path component"
}

test_zxfer_validate_temp_root_candidate_fails_closed_on_ls_owner_and_mode() {
	candidate="$TEST_TMPDIR/validate_tmp_root_checks"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell '
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		export FAKE_LS_ANSWERS FAKE_ID_UID
		g_zxfer_temp_root_candidate_result=stale
		while IFS="|" read -r l_case l_uid l_line; do
			FAKE_ID_UID=$l_uid
			FAKE_LS_ANSWERS=".|$l_line"
			g_zxfer_effective_uid=""
			zxfer_validate_temp_root_candidate "'"$candidate"'"
			printf "%s=%s <%s>\n" "$l_case" "$?" "$g_zxfer_temp_root_candidate_result"
		done <<EOF
ls_fails|1234|FAIL
unknown_effective_uid||2 drwx------ 2 1234 0 64 Jan  1 00:00 .
other_owner|1234|2 drwx------ 2 4321 0 64 Jan  1 00:00 .
world_writable|1234|2 drwxrwxrwx 2 0 0 64 Jan  1 00:00 .
group_writable|1234|2 drwxrwx--- 2 1234 0 64 Jan  1 00:00 .
EOF
	'

	assertEquals "Temp-root validation must fail closed, publishing nothing, when ls fails, the effective UID is unknown for a non-root owner, another user owns it, or others can write it without the sticky bit." \
		"ls_fails=1 <>
unknown_effective_uid=1 <>
other_owner=1 <>
world_writable=1 <>
group_writable=1 <>" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_reads_owner_and_mode_from_one_ls() {
	candidate="$TEST_TMPDIR/validate_tmp_root_one_ls"
	tool_log="$TEST_TMPDIR/validate_tmp_root_one_ls.log"
	mkdir -p "$candidate"
	rm -f "$tool_log"

	zxfer_test_capture_subshell '
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_TOOL_LOG="'"$tool_log"'"
		FAKE_ID_UID=4242
		export FAKE_TOOL_LOG FAKE_ID_UID FAKE_LS_ANSWERS
		for l_test_owner in 0 4242 4343 owner; do
			FAKE_LS_ANSWERS=".|9 drwxrwxrwt 9 $l_test_owner 0 0 Jan  1 00:00 ."
			zxfer_validate_temp_root_candidate "'"$candidate"'"
			printf "%s=%s\n" "$l_test_owner" "$?"
		done
	'

	assertEquals "A root-owned sticky candidate and an effective-user candidate pass; another owner and a non-numeric owner fail." \
		"0=0
4242=0
4343=1
owner=1" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "Each validation should run one ls; id runs once, for the first non-root owner, and is memoized." \
		"ls -ldin .
ls -ldin .
id -u
ls -ldin .
ls -ldin ." "$(cat "$tool_log")"
}

test_zxfer_validate_temp_root_candidate_rejects_relative_physical_pwd_output() {
	candidate="$TEST_TMPDIR/validate_tmp_root_relative_pwd"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		pwd() {
			printf '%s\n' 'relative-path'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
		printf 'status=%s <%s>\n' \"\$?\" \"\$g_zxfer_temp_root_candidate_result\"
	"

	assertEquals "Validated temp-root selection should reject a non-absolute physical-directory result and publish nothing." \
		"status=1 <>" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_keeps_line_feeds_in_the_physical_path() {
	nl_dir="$TEST_TMPDIR/$(printf 'tmp\nroot')"
	mkdir -p "$nl_dir" || fail "Unable to create the line-feed fixture."
	chmod 700 "$nl_dir"
	expected=$(CDPATH='' cd -P "$nl_dir" && pwd && printf x)
	expected=${expected%?}
	expected=${expected%"$ZXFER_LF"}

	zxfer_validate_temp_root_candidate "$nl_dir"
	status=$?

	assertEquals "A private directory whose name holds a line feed should validate." 0 "$status"
	assertEquals "The physical path should keep its line feed; only the ls line is split off." \
		"$expected" "$g_zxfer_temp_root_candidate_result"
}
