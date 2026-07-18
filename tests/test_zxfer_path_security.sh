#!/bin/sh
#
# shunit2 tests for the path-security helpers in src/zxfer_path_security.sh.
#
# shellcheck disable=SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_path_security.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_path_security"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

test_path_security_boundary_excludes_runtime_dependent_staging() {
	assertTrue "Path-security helpers should be available at their module boundary." \
		"command -v zxfer_validate_temp_root_candidate >/dev/null 2>&1"
	assertFalse "Secure staging should not be pulled into the pure path-security boundary." \
		"command -v zxfer_create_secure_staging_dir_for_path >/dev/null 2>&1"
	assertFalse "Runtime artifact registration should not be pulled into the pure path-security boundary." \
		"command -v zxfer_register_runtime_artifact_path >/dev/null 2>&1"
}

test_zxfer_get_path_owner_uid_preserves_caller_ifs_and_globbing_state() {
	owner_path="$TEST_TMPDIR/path-owner-state"
	: >"$owner_path"

	zxfer_test_capture_subshell '
		stat() {
			return 1
		}
		ls() {
			printf "%s\n" "-rw------- 1 4242 0 0 Jan 1 00:00 path-owner-state"
		}

		IFS="|"
		set -f
		zxfer_get_path_owner_uid "$TEST_TMPDIR/path-owner-state" >"$TEST_TMPDIR/path-owner-custom.out"
		printf "custom_status=%s\n" "$?"
		printf "custom_ifs=<%s>\n" "$IFS"
		case $- in
		*f*) printf "%s\n" "custom_globbing=disabled" ;;
		*) printf "%s\n" "custom_globbing=enabled" ;;
		esac

		unset IFS
		set +f
		zxfer_get_path_owner_uid "$TEST_TMPDIR/path-owner-state" >"$TEST_TMPDIR/path-owner-unset.out"
		printf "unset_status=%s\n" "$?"
		if [ "${IFS+set}" = "set" ]; then
			printf "%s\n" "unset_ifs=set"
		else
			printf "%s\n" "unset_ifs=unset"
		fi
		case $- in
		*f*) printf "%s\n" "unset_globbing=disabled" ;;
		*) printf "%s\n" "unset_globbing=enabled" ;;
		esac
	'

	assertContains "LS owner fallback should split fields independently of caller IFS." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "custom_status=0"
	assertEquals "LS owner fallback should recover the numeric owner under a custom IFS." \
		"4242" "$(cat "$TEST_TMPDIR/path-owner-custom.out")"
	assertContains "LS owner fallback should restore a caller-defined IFS exactly." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "custom_ifs=<|>"
	assertContains "LS owner fallback should preserve disabled globbing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "custom_globbing=disabled"
	assertContains "LS owner fallback should succeed when caller IFS is unset." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unset_status=0"
	assertEquals "LS owner fallback should recover the numeric owner with unset IFS." \
		"4242" "$(cat "$TEST_TMPDIR/path-owner-unset.out")"
	assertContains "LS owner fallback should restore an originally unset IFS." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unset_ifs=unset"
	assertContains "LS owner fallback should leave caller-enabled globbing enabled." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "unset_globbing=enabled"
}

test_zxfer_get_path_device_inode_parses_leading_whitespace_ls_fallback_portably() {
	identity_path="$TEST_TMPDIR/path-identity-state"
	mkdir "$identity_path"

	zxfer_test_capture_subshell '
		stat() {
			return 1
		}
		ls() {
			printf "%s\n" "    424242 drwx------ 2 1000 1000 0 Jan 1 00:00 path-identity-state"
		}
		IFS="|"
		set -f
		zxfer_get_path_device_inode "$TEST_TMPDIR/path-identity-state" >"$TEST_TMPDIR/path-identity.out"
		printf "status=%s\n" "$?"
		printf "ifs=<%s>\n" "$IFS"
		case $- in
		*f*) printf "%s\n" "globbing=disabled" ;;
		*) printf "%s\n" "globbing=enabled" ;;
		esac
	'

	assertContains "The stat-less identity fallback should accept ls output with a left-padded inode field." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "status=0"
	assertEquals "The stat-less identity fallback should publish the first numeric ls field." \
		"inode:424242" "$(cat "$TEST_TMPDIR/path-identity.out")"
	assertContains "Filesystem identity probing should restore a caller-defined IFS exactly." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ifs=<|>"
	assertContains "Filesystem identity probing should preserve disabled globbing." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "globbing=disabled"
}

test_zxfer_get_private_directory_security_record_uses_one_formatted_stat_snapshot() {
	private_dir="$TEST_TMPDIR/private-record"
	stat_log="$TEST_TMPDIR/private-record-stat.log"
	mkdir -m 700 "$private_dir"

	zxfer_test_capture_subshell '
		stat() {
			printf "%s\n" "$*" >>"$TEST_TMPDIR/private-record-stat.log"
			case "$1" in
			-c) printf "%s\n" "11:22:4242:700" ;;
			*) return 1 ;;
			esac
		}
		zxfer_get_private_directory_security_record \
			"$TEST_TMPDIR/private-record"
	'

	assertEquals "The consolidated private-directory probe should publish identity, owner, and mode." \
		"device-inode:11:22	4242	700" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "A GNU-style formatted stat should require exactly one metadata probe." \
		1 "$(wc -l <"$stat_log" | tr -d ' ')"
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
	assertEquals "A private caller-owned temp root should resolve to its physical path." \
		"$expected_root" "$(zxfer_validate_temp_root_candidate "$TEST_TMPDIR")"

	insecure_root="$TEST_TMPDIR/insecure"
	mkdir "$insecure_root"
	chmod 0777 "$insecure_root"
	assertFalse "A writable non-sticky temp root should fail closed." \
		"zxfer_validate_temp_root_candidate \"$insecure_root\" >/dev/null 2>&1"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
