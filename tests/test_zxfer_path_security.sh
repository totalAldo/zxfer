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

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
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

	zxfer_test_capture_subshell '
		IFS="|"
		set -f
		while IFS="|" read -r l_platform l_line l_inode l_owner l_mode; do
			zxfer_parse_path_metadata_line "$l_line"
			printf "%s: status=%s inode=%s owner=%s mode=%s expected=%s/%s/%s\n" \
				"$l_platform" "$?" "$g_zxfer_path_inode_result" \
				"$g_zxfer_path_owner_uid_result" "$g_zxfer_path_mode_result" \
				"$l_inode" "$l_owner" "$l_mode"
		done <<EOF
$rows
EOF
		printf "ifs=<%s>\n" "$IFS"
		case $- in
		*f*) echo "globbing=disabled" ;;
		*) echo "globbing=enabled" ;;
		esac
	'

	printf '%s\n' "$rows" | while IFS='|' read -r platform line inode owner mode; do
		assertContains "$platform should parse to inode, owner and mode." \
			"$ZXFER_TEST_CAPTURE_OUTPUT" \
			"$platform: status=0 inode=$inode owner=$owner mode=$mode expected="
	done
	assertContains "Parsing should keep the caller's IFS." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ifs=<|>"
	assertContains "Parsing should keep disabled globbing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "globbing=disabled"
}

test_zxfer_parse_path_metadata_line_fails_closed_on_lines_it_cannot_trust() {
	zxfer_test_capture_subshell '
		g_zxfer_path_inode_result=stale
		g_zxfer_path_owner_uid_result=stale
		g_zxfer_path_mode_result=stale
		g_zxfer_path_permissions_result=stale
		while IFS="|" read -r l_case l_line; do
			zxfer_parse_path_metadata_line "$l_line"
			printf "%s=%s <%s%s%s%s>\n" "$l_case" "$?" \
				"$g_zxfer_path_inode_result" "$g_zxfer_path_owner_uid_result" \
				"$g_zxfer_path_mode_result" "$g_zxfer_path_permissions_result"
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
	'

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

	zxfer_test_capture_subshell '
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
	'
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

	zxfer_test_capture_subshell '
		PATH="$TEST_TMPDIR/fake-bin:$PATH"
		FAKE_TOOL_LOG="'"$tool_log"'"
		FAKE_LS_ANSWERS="*|    424242 drwx--S---  2 4242     0      512 Jan  1 00:00 private-record"
		export FAKE_TOOL_LOG FAKE_LS_ANSWERS
		zxfer_get_private_directory_security_record "$TEST_TMPDIR/private-record"
		printf "status=%s record=%s\n" "$?" "$g_zxfer_private_directory_record_result"
		zxfer_get_private_directory_security_record "$TEST_TMPDIR/does-not-exist"
		printf "missing=%s record=<%s>\n" "$?" "$g_zxfer_private_directory_record_result"
	'

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

	zxfer_test_capture_subshell '
		PATH="$TEST_TMPDIR/fake-bin"
		FAKE_TOOL_LOG="'"$tool_log"'"
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
		PATH="$TEST_TMPDIR/no-such-bin"
		g_zxfer_effective_uid=""
		zxfer_get_effective_user_uid
		printf "missing_id=%s memo=<%s>\n" "$?" "$g_zxfer_effective_uid"
	'

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

# zxfer-test-fragment: suites/zxfer_path_security_validation_tests.sh
# shellcheck source=tests/suites/zxfer_path_security_validation_tests.sh
. "$TESTS_DIR/suites/zxfer_path_security_validation_tests.sh"

suite() {
	zxfer_test_register_fragment_tests \
		"$TESTS_DIR/test_zxfer_path_security.sh" \
		"$TESTS_DIR/suites/zxfer_path_security_validation_tests.sh"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
