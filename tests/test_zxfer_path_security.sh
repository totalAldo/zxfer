#!/bin/sh
#
# shunit2 tests for the path-security helpers in src/zxfer_path_security.sh:
# the one `ls -ldin` metadata read, the effective-UID memo and the temp-root
# and backup-owner checks.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_path_security"
	zxfer_path_security_test_write_fake_tools "$TEST_TMPDIR/fake-bin"
}

setUp() {
	zxfer_test_reset_all_owner_state
}

# Purpose: Write fake ls and id commands. The metadata reader runs `exec ls`
# and `exec id`, which a shell function cannot stand in for, so a case puts
# DIR first on PATH instead.
# Usage: zxfer_path_security_test_write_fake_tools DIR. The fake ls logs its
# argv to $FAKE_TOOL_LOG and answers from $FAKE_LS_ANSWERS, one "PATH|LINE"
# row per line: the first row whose PATH is its last argument or * wins, a
# LINE of FAIL exits 1, and no row exits 1. The fake id logs "id ARGS" and
# prints $FAKE_ID_UID, or exits 1 when it is empty.
zxfer_path_security_test_write_fake_tools() {
	mkdir -p "$1" || return 1
	cat >"$1/ls" <<'EOF'
#!/bin/sh
printf 'ls %s\n' "$*" >>"${FAKE_TOOL_LOG:-/dev/null}"
for l_fake_ls_path; do :; done
while IFS='|' read -r l_fake_ls_key l_fake_ls_line; do
	[ "$l_fake_ls_key" = "$l_fake_ls_path" ] || [ "$l_fake_ls_key" = '*' ] ||
		continue
	[ "$l_fake_ls_line" != FAIL ] || exit 1
	printf '%s\n' "$l_fake_ls_line"
	exit 0
done <<ANSWERS
${FAKE_LS_ANSWERS:-}
ANSWERS
exit 1
EOF
	cat >"$1/id" <<'EOF'
#!/bin/sh
printf 'id %s\n' "$*" >>"${FAKE_TOOL_LOG:-/dev/null}"
[ -n "${FAKE_ID_UID:-}" ] || exit 1
printf '%s\n' "$FAKE_ID_UID"
EOF
	chmod 755 "$1/ls" "$1/id"
}

# The fields relied on are the POSIX ones, printed alike by every supported ls:
# inode, mode string, link count and numeric owner, then group, size, date and
# name. Each row is PLATFORM|LINE|INODE|OWNER|MODE.
test_zxfer_parse_path_metadata_line_reads_every_supported_ls_format() {
	rows='GNU coreutils, SELinux marker|335 -rw-------. 1 1000 1000 0 Jan  1 00:00 name with spaces|335|1000|600
GNU coreutils, setgid dir under a setgid parent|12 drwx--S--- 2 1000 1000 4096 Jan  1 00:00 zxfer.1.abc|12|1000|2700
macOS, extended attributes|335309700 drwxr-xr-x@   5 501  0    160 Sep 26 21:28 .|335309700|501|755
macOS, 64-bit firmlink inode|1152921500312607500 lrwxr-xr-x@ 1 0 0 11 Sep  3 06:34 /tmp -> private/tmp|1152921500312607500|0|755
FreeBSD, sticky /tmp|3 drwxrwxrwt  8 0  0  512 Sep 26 12:00 /tmp|3|0|1777
illumos, padded inode and ACL|   424242 drwx--S---+  2 1000     1000         512 Jan  1 00:00 /export/tmp|424242|1000|2700
illumos, mandatory locking|  12345 -rw---l---   1 0        0            512 Jan  1 00:00 f|12345|0|2600
BusyBox, padded inode|  1234567 drwxrwxrwt   12 0        0            4096 Jan  1 00:00 /tmp|1234567|0|1777
set-user-ID and sticky without execute|42 d--S-----T 2 501 0 64 Sep 26 21:28 sg|42|501|5000
no bits|42 ---------- 1 501 0 0 Sep 26 21:28 none|42|501|0
execute only|42 ------x--x 1 501 0 0 Sep 26 21:28 low|42|501|11
leading dash and tab in the name|7 -rw-r----- 1 0 0 0 Jan  1 00:00 -lead	tab|7|0|640'

	capture_case() {
		IFS="|"
		set -f
		zxfer_parse_path_metadata_line "$1" || return "$?"
		printf 'inode=%s owner=%s mode=%s links=%s\n' \
			"$g_zxfer_path_inode_result" "$g_zxfer_path_owner_uid_result" \
			"$g_zxfer_path_mode_result" "$g_zxfer_path_link_count_result"
		printf "ifs=<%s>\n" "$IFS"
		case $- in
		*f*) echo "globbing=disabled" ;;
		*) echo "globbing=enabled" ;;
		esac
	}
	while IFS='|' read -r platform line inode owner mode; do
		zxfer_test_capture_subshell capture_case "$line"
		# shellcheck disable=SC2086 # Split the fixture's ls fields deliberately.
		set -- $line
		assertEquals "$platform should parse successfully." 0 "$ZXFER_TEST_CAPTURE_STATUS"
		assertEquals "$platform should retain all four fields and preserve shell options." \
			"inode=$inode owner=$owner mode=$mode links=$3
ifs=<|>
globbing=disabled" "$ZXFER_TEST_CAPTURE_OUTPUT"
	done <<EOF
$rows
EOF
}

test_zxfer_parse_path_metadata_line_fails_closed_on_lines_it_cannot_trust() {
	capture_case() {
		g_zxfer_path_inode_result=stale
		g_zxfer_path_owner_uid_result=stale
		g_zxfer_path_mode_result=stale
		g_zxfer_path_permissions_result=stale
		g_zxfer_path_link_count_result=stale
		while IFS="|" read -r l_case l_line; do
			zxfer_parse_path_metadata_line "$l_line"
			printf "%s=%s <%s%s%s%s%s>\n" "$l_case" "$?" \
				"$g_zxfer_path_inode_result" "$g_zxfer_path_owner_uid_result" \
				"$g_zxfer_path_mode_result" "$g_zxfer_path_permissions_result" \
				"$g_zxfer_path_link_count_result"
		done <<EOF
empty|
named_owner|42 drwx------ 2 owner 0 64 Jan  1 00:00 named
missing_owner|42 drwx------ 2
missing_links|42 drwx------
letter_inode|x42 drwx------ 2 0 0 64 Jan  1 00:00 bad
letter_links|42 drwx------ 2x 0 0 64 Jan  1 00:00 bad
two_markers|42 drwx------@+ 2 0 0 64 Jan  1 00:00 bad
short_mode|42 drwx----- 2 0 0 64 Jan  1 00:00 bad
bad_other_execute|42 drwxrwxrwz 2 0 0 64 Jan  1 00:00 bad
sticky_in_owner|42 drwt------ 2 0 0 64 Jan  1 00:00 bad
EOF
	}
	zxfer_test_capture_subshell capture_case

	for failure_case in empty named_owner missing_owner missing_links \
		letter_inode letter_links two_markers short_mode bad_other_execute \
		sticky_in_owner; do
		assertContains "A line with $failure_case must fail closed with every result empty." \
			"$ZXFER_TEST_CAPTURE_OUTPUT" "$failure_case=1 <>"
	done
}

# Odd names are real files: the reader must hand them to ls as one argument,
# with ./ in front of a leading -, and read the same metadata a plain name
# gives.
test_zxfer_read_path_metadata_reads_real_paths_with_odd_names() {
	odd_dir="$TEST_TMPDIR/odd names"
	mkdir -p "$odd_dir" || fail "Unable to create the odd-name fixture."
	nl_name=$(printf 'line\nfeed')
	(
		cd "$odd_dir" || exit 1
		: >"with space"
		: >"-leading-dash"
		: >"tab	name"
		# ./ keeps GNU and BusyBox mkdir, which permute arguments, from
		# reading -dash-dir as options.
		mkdir ./"$nl_name" ./-dash-dir
		chmod 640 "with space"
		chmod 600 ./-leading-dash
		chmod 604 "tab	name"
		chmod 750 "$nl_name"
		chmod 1700 ./-dash-dir
	) || fail "Unable to populate the odd-name fixture."
	effective_uid=$(id -u)

	output=$(
		cd "$odd_dir" || exit 1
		for name in "with space" "-leading-dash" "tab	name" "$nl_name" \
			-dash-dir "$odd_dir/with space"; do
			zxfer_read_path_metadata "$name"
			read_status=$?
			# ls -di is the independent inode reference; the name follows ./.
			# shellcheck disable=SC2012
			expected_inode=$(ls -di ./"${name#"$odd_dir"/}" | awk '{ print $1; exit }')
			inode_check="inode differs"
			[ -z "$expected_inode" ] ||
				[ "$g_zxfer_path_inode_result" != "$expected_inode" ] ||
				inode_check="inode-ok"
			printf '%s|%s|%s|%s\n' "$read_status" \
				"$g_zxfer_path_owner_uid_result" "$g_zxfer_path_mode_result" \
				"$inode_check"
		done
		printf 'owner=%s mode=%s\n' "$(zxfer_get_path_owner_uid -leading-dash)" \
			"$(zxfer_get_path_mode_octal -dash-dir)"
	)

	assertEquals "Names with blanks, a leading -, a tab or a line feed read like plain names." \
		"0|$effective_uid|640|inode-ok
0|$effective_uid|600|inode-ok
0|$effective_uid|604|inode-ok
0|$effective_uid|750|inode-ok
0|$effective_uid|1700|inode-ok
0|$effective_uid|640|inode-ok
owner=$effective_uid mode=1700" "$output"
}

test_zxfer_get_path_owner_uid_and_mode_fail_closed_when_ls_fails_or_path_is_missing() {
	owner_path="$TEST_TMPDIR/path-owner-unavailable"
	: >"$owner_path"

	capture_case() {
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_LS_ANSWERS="*|FAIL"
		export FAKE_LS_ANSWERS
		zxfer_get_path_owner_uid "$TEST_TMPDIR/path-owner-unavailable"
		printf "owner_status=%s\n" "$?"
		zxfer_get_path_mode_octal "$TEST_TMPDIR/path-owner-unavailable"
		printf "mode_status=%s\n" "$?"
		FAKE_LS_ANSWERS="*|42 -rw-r-----x+ 1 0 0 0 Jan  1 00:00 f"
		zxfer_get_path_mode_octal "$TEST_TMPDIR/path-owner-unavailable"
		printf "unparsed_mode_status=%s\n" "$?"
	}
	zxfer_test_capture_subshell capture_case
	zxfer_get_path_owner_uid "$TEST_TMPDIR/does_not_exist" >/dev/null 2>&1
	missing_owner_status=$?
	zxfer_get_path_mode_octal "$TEST_TMPDIR/does_not_exist" >/dev/null 2>&1
	missing_mode_status=$?

	assertEquals "Owner and mode lookups must fail closed, printing nothing, when ls fails or its line does not parse." \
		"owner_status=1
mode_status=1
unparsed_mode_status=1" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "Owner lookups should fail for a missing path." 1 "$missing_owner_status"
	assertEquals "Mode lookups should fail for a missing path." 1 "$missing_mode_status"
}

test_zxfer_get_private_directory_security_record_reads_one_ls_line() {
	private_dir="$TEST_TMPDIR/private-record"
	tool_log="$TEST_TMPDIR/private-record-tools.log"
	mkdir -m 700 "$private_dir"
	rm -f "$tool_log"

	capture_case() {
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_TOOL_LOG="$tool_log"
		FAKE_LS_ANSWERS="*|    424242 drwx--S---  2 4242     0      512 Jan  1 00:00 private-record"
		export FAKE_TOOL_LOG FAKE_LS_ANSWERS
		zxfer_get_private_directory_security_record "$TEST_TMPDIR/private-record"
		printf "status=%s record=%s\n" "$?" "$g_zxfer_private_directory_record_result"
		zxfer_get_private_directory_security_record "$TEST_TMPDIR/does-not-exist"
		printf "missing=%s record=<%s>\n" "$?" "$g_zxfer_private_directory_record_result"
	}
	zxfer_test_capture_subshell capture_case

	assertEquals "The private-directory record should hold the inode, owner and mode of one ls line; a missing directory records nothing." \
		"status=0 record=424242	4242	2700
missing=1 record=<>" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "The record should cost exactly one ls and no id." \
		"ls -ldin $private_dir" "$(cat "$tool_log")"
}

test_path_mode_parent_and_backup_file_validation_share_one_boundary() {
	secure_file="$TEST_TMPDIR/secure-backup-file"
	: >"$secure_file"
	chmod 600 "$secure_file"

	assertEquals "Path mode lookup should recognize private backup metadata." \
		"600" "$(zxfer_get_path_mode_octal "$secure_file")"
	assertEquals "Parent lookup should preserve the containing directory." \
		"$TEST_TMPDIR" "$(zxfer_get_path_parent_dir "$secure_file")"
	assertTrue "A private file owned by the effective user should pass validation." \
		"zxfer_check_secure_backup_file \"$secure_file\" >/dev/null"

	chmod 640 "$secure_file"
	validation_output=$(zxfer_check_secure_backup_file "$secure_file" 2>&1)
	validation_status=$?

	assertEquals "Backup metadata with broader permissions should fail closed." \
		1 "$validation_status"
	assertContains "Backup metadata rejection should report the observed mode." \
		"$validation_output" "permissions (640) are not 0600"
}

test_symlink_component_detection_and_temp_root_validation() {
	real_dir="$TEST_TMPDIR/real"
	link_dir="$TEST_TMPDIR/link"
	mkdir "$real_dir"
	ln -s "$real_dir" "$link_dir"

	assertEquals "The first untrusted symlink component should be reported." \
		"$link_dir" "$(zxfer_find_symlink_path_component "$link_dir/child")"

	expected_root=$(CDPATH='' cd -P "$TEST_TMPDIR" && pwd)
	zxfer_validate_temp_root_candidate "$TEST_TMPDIR"
	candidate_status=$?
	assertEquals "A private caller-owned temp root should validate." 0 "$candidate_status"
	assertEquals "A private caller-owned temp root should resolve to its physical path." \
		"$expected_root" "$g_zxfer_temp_root_candidate_result"

	insecure_root="$TEST_TMPDIR/insecure"
	mkdir "$insecure_root"
	chmod 0777 "$insecure_root"
	zxfer_validate_temp_root_candidate "$insecure_root"
	insecure_status=$?
	assertEquals "A writable non-sticky temp root should fail closed." 1 "$insecure_status"
	assertEquals "A rejected temp root should publish no path." \
		"" "$g_zxfer_temp_root_candidate_result"
	chmod 0700 "$insecure_root"
}

test_get_effective_user_uid_is_memoized_and_fails_closed() {
	tool_log="$TEST_TMPDIR/effective-uid-tools.log"
	rm -f "$tool_log"

	capture_case() {
		# shellcheck disable=SC2123 # Exercise helper lookup on a controlled PATH.
		PATH="$TEST_TMPDIR/fake-bin"
		FAKE_TOOL_LOG="$tool_log"
		export FAKE_TOOL_LOG
		FAKE_ID_UID=""
		export FAKE_ID_UID
		zxfer_get_effective_user_uid
		printf "failed=%s memo=<%s>\n" "$?" "$g_zxfer_effective_uid"
		FAKE_ID_UID=4242x
		zxfer_get_effective_user_uid
		printf "non_numeric=%s memo=<%s>\n" "$?" "$g_zxfer_effective_uid"
		FAKE_ID_UID=4242
		zxfer_get_effective_user_uid
		printf "first=%s memo=<%s>\n" "$?" "$g_zxfer_effective_uid"
		FAKE_ID_UID=4343
		zxfer_get_effective_user_uid
		printf "second=%s memo=<%s>\n" "$?" "$g_zxfer_effective_uid"
		# shellcheck disable=SC2123 # Exercise lookup when id cannot be found.
		PATH="$TEST_TMPDIR/no-such-bin"
		g_zxfer_effective_uid=""
		zxfer_get_effective_user_uid
		printf "missing_id=%s memo=<%s>\n" "$?" "$g_zxfer_effective_uid"
	}
	zxfer_test_capture_subshell capture_case

	assertEquals "The effective UID should be read once, kept, and never taken from a failed or non-numeric id." \
		"failed=1 memo=<>
non_numeric=1 memo=<>
first=0 memo=<4242>
second=0 memo=<4242>
missing_id=1 memo=<>" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "id should run only until it answers." "id -u
id -u
id -u" "$(cat "$tool_log")"
}

test_session_reset_discards_an_inherited_effective_uid_memo() {
	g_zxfer_effective_uid=0
	g_zxfer_path_owner_uid_result=0
	g_zxfer_temp_root_candidate_result=/inherited

	zxfer_reset_session_state

	assertEquals "An exported effective-UID memo must never stand in for id." \
		"" "$g_zxfer_effective_uid"
	assertEquals "Session reset should clear the metadata results." \
		"" "$g_zxfer_path_owner_uid_result"
	assertEquals "Session reset should clear the temp-root result." \
		"" "$g_zxfer_temp_root_candidate_result"
}

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

	capture_case() {
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
	}
	zxfer_test_capture_subshell capture_case
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
	capture_case() {
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		export FAKE_LS_ANSWERS
		while IFS="|" read -r l_case l_link l_root; do
			FAKE_LS_ANSWERS="$trusted_root_symlink|$l_link
/|$l_root"
			zxfer_is_trusted_symlink_path_component "$trusted_root_symlink"
			printf "%s=%s\n" "$l_case" "$?"
		done <<EOF
trusted|$good_link|$good_root
link_unreadable|FAIL|$good_root
link_not_root|7 lrwxrwxrwx 1 501 0 11 Jan  1 00:00 x -> y|$good_root
root_unreadable|$good_link|FAIL
root_not_root|$good_link|2 drwxr-xr-x 23 501 0 736 Jan  1 00:00 /
root_unparseable|$good_link|bad-perms
root_world_writable|$good_link|2 drwxrwxrwx 23 0 0 736 Jan  1 00:00 /
root_sticky|$good_link|2 drwxrwxrwt 23 0 0 736 Jan  1 00:00 /
EOF
	}
	zxfer_test_capture_subshell capture_case

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

	capture_case() {
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_TOOL_LOG="$tool_log"
		FAKE_LS_ANSWERS="*|7 lrwxrwxrwx 1 0 0 11 Jan  1 00:00 x -> y"
		export FAKE_TOOL_LOG FAKE_LS_ANSWERS
		zxfer_is_trusted_symlink_path_component "$symlink_parent/link"
		printf "nested=%s\n" "$?"
		zxfer_is_trusted_symlink_path_component relative-link
		printf "relative=%s\n" "$?"
		zxfer_is_trusted_symlink_path_component /
		printf "root=%s\n" "$?"
	}
	zxfer_test_capture_subshell capture_case

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

	zxfer_test_capture_subshell zxfer_require_backup_metadata_path_without_symlinks "$link_file"

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

	capture_case() {
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		export FAKE_LS_ANSWERS FAKE_ID_UID
		g_zxfer_temp_root_candidate_result=stale
		while IFS="|" read -r l_case l_uid l_line; do
			FAKE_ID_UID=$l_uid
			FAKE_LS_ANSWERS=".|$l_line"
			g_zxfer_effective_uid=""
			zxfer_validate_temp_root_candidate "$candidate"
			printf "%s=%s <%s>\n" "$l_case" "$?" "$g_zxfer_temp_root_candidate_result"
		done <<EOF
ls_fails|1234|FAIL
unknown_effective_uid||2 drwx------ 2 1234 0 64 Jan  1 00:00 .
other_owner|1234|2 drwx------ 2 4321 0 64 Jan  1 00:00 .
world_writable|1234|2 drwxrwxrwx 2 0 0 64 Jan  1 00:00 .
group_writable|1234|2 drwxrwx--- 2 1234 0 64 Jan  1 00:00 .
EOF
	}
	zxfer_test_capture_subshell capture_case

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

	capture_case() {
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_TOOL_LOG="$tool_log"
		FAKE_ID_UID=4242
		export FAKE_TOOL_LOG FAKE_ID_UID FAKE_LS_ANSWERS
		for l_test_owner in 0 4242 4343 owner; do
			FAKE_LS_ANSWERS=".|9 drwxrwxrwt 9 $l_test_owner 0 0 Jan  1 00:00 ."
			zxfer_validate_temp_root_candidate "$candidate"
			printf "%s=%s\n" "$l_test_owner" "$?"
		done
	}
	zxfer_test_capture_subshell capture_case

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

	capture_case() {
		pwd() {
			printf '%s\n' 'relative-path'
		}
		zxfer_validate_temp_root_candidate "$candidate"
		printf 'status=%s <%s>\n' "$?" "$g_zxfer_temp_root_candidate_result"
	}
	zxfer_test_capture_subshell capture_case

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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
