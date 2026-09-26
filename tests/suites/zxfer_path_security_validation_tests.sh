#!/bin/sh
# Path validation tests for src/zxfer_path_security.sh: backup-file owner and
# mode checks, symlink path components, trusted root symlinks, and temp-root
# candidates. Run by tests/test_zxfer_path_security.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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
	result_root=$(
		zxfer_get_effective_user_uid() { printf '%s\n' 1000; }
		if zxfer_backup_owner_uid_is_allowed 0; then echo ok; else echo fail; fi
	)
	assertEquals "Root must always be allowed." "ok" "$result_root"

	result_user=$(
		zxfer_get_effective_user_uid() { printf '%s\n' 4242; }
		if zxfer_backup_owner_uid_is_allowed 4242; then echo ok; else echo fail; fi
	)
	assertEquals "Effective UID should be permitted when matching the owner." "ok" "$result_user"
}

test_describe_expected_backup_owner_includes_effective_uid_when_non_root() {
	result=$(
		zxfer_get_effective_user_uid() { printf '%s\n' 9999; }
		zxfer_describe_expected_backup_owner
	)
	assertEquals "root (UID 0) or UID 9999" "$result"
}

test_check_secure_backup_file_rejects_non_0600_permissions() {
	tmp_file="$TEST_TMPDIR/insecure_backup"
	: >"$tmp_file"
	(
		zxfer_get_path_owner_uid() { printf '%s\n' 0; }
		zxfer_get_path_mode_octal() { printf '%s\n' 644; }
		zxfer_check_secure_backup_file "$tmp_file"
	) >/dev/null 2>&1
	status=$?
	assertEquals "Insecure permissions should trigger an error." 1 "$status"
}

test_check_secure_backup_file_accepts_secure_metadata() {
	tmp_file="$TEST_TMPDIR/secure_backup"
	: >"$tmp_file"
	(
		zxfer_get_path_owner_uid() { printf '%s\n' 0; }
		zxfer_get_path_mode_octal() { printf '%s\n' 600; }
		zxfer_check_secure_backup_file "$tmp_file"
	)
	status=$?
	assertEquals "Secure metadata should pass validation." 0 "$status"
}

test_zxfer_get_path_parent_dir_handles_root_and_relative_inputs() {
	assertEquals "Absolute paths should return their containing directory." \
		"/var/log" "$(zxfer_get_path_parent_dir "/var/log/zxfer.log")"
	assertEquals "Paths without a slash should fall back to root for parent-dir validation." \
		"/" "$(zxfer_get_path_parent_dir "zxfer.log")"
}

test_zxfer_find_symlink_path_component_detects_nested_symlink() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/real_dir"
	link_dir="$physical_tmpdir/link_dir"
	mkdir -p "$real_dir/subdir"
	ln -s "$real_dir" "$link_dir"

	result=$(zxfer_find_symlink_path_component "$link_dir/subdir/file")
	status=$?

	assertEquals "Nested symlink detection should succeed when any path component is a symlink." 0 "$status"
	assertEquals "Nested symlink detection should return the offending path component." "$link_dir" "$result"
}

test_zxfer_find_symlink_path_component_detects_relative_symlink() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/relative_real_dir"
	link_dir="$physical_tmpdir/relative_link_dir"
	mkdir -p "$real_dir/subdir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	result=$(zxfer_find_symlink_path_component "./relative_link_dir/subdir/file")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative paths should be scanned for nested symlink components." 0 "$status"
	assertEquals "Relative symlink checks should return the offending relative path component." "./relative_link_dir" "$result"
}

test_zxfer_find_symlink_path_component_ignores_trusted_absolute_root_symlink() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	result=$(zxfer_find_symlink_path_component "$trusted_root_symlink/zxfer-trusted-root-symlink-probe/subdir/file")
	status=$?

	assertEquals "Trusted top-level system symlink components should be ignored regardless of platform-specific root layout." 1 "$status"
	assertEquals "Trusted absolute symlink components should not be reported as unsafe." "" "$result"
}

test_zxfer_is_trusted_symlink_path_component_accepts_known_root_symlink() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Known trusted root-level symlinks should be accepted by the trust check when the current host exposes one." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Trusted root-symlink checks should stay silent on success." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_owner_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the symlink owner lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Owner-lookup failures should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_owner_lookup_failures_for_absolute_nonroot_symlinks() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_owner_lookup_failure"
	symlink_target="$symlink_parent/target"
	symlink_path="$symlink_parent/link"
	mkdir -p "$symlink_target"
	ln -sf "$symlink_target" "$symlink_path"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$symlink_path\") return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$symlink_path\"
	"

	assertEquals "Trusted-symlink checks should fail closed when the symlink owner lookup fails for absolute non-root symlinks." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Absolute non-root symlink owner-lookup failures should not emit a trusted result payload." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_parent_owner_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$trusted_root_symlink\") printf '%s\n' '0' ;;
			/) return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the root-parent owner lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Parent-owner lookup failures should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_parent_owner_lookup_failures_for_absolute_nonroot_symlinks() {
	symlink_parent="$TEST_TMPDIR/trusted_symlink_parent_lookup_failure"
	symlink_target="$symlink_parent/target"
	symlink_path="$symlink_parent/link"
	mkdir -p "$symlink_target"
	ln -sf "$symlink_target" "$symlink_path"

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			case \"\$1\" in
			\"$symlink_path\") printf '%s\n' '0' ;;
			\"$symlink_parent\") return 1 ;;
			*) printf '%s\n' '0' ;;
			esac
		}
		zxfer_is_trusted_symlink_path_component \"$symlink_path\"
	"

	assertEquals "Trusted-symlink checks should fail closed when the parent owner lookup fails for absolute non-root symlinks." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Absolute non-root parent-owner lookup failures should not emit a trusted result payload." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_ls_lookup_failures() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			return 1
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should fail closed when the root permission lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed root-permission lookups should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_unparseable_root_permissions() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'bad-perms'
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should reject malformed root permission strings." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Malformed root-permission strings should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_is_trusted_symlink_path_component_rejects_world_writable_root_without_sticky_bit() {
	if ! require_trusted_root_symlink_for_tests; then
		return 0
	fi

	zxfer_test_capture_subshell "
		zxfer_get_path_owner_uid() {
			printf '%s\n' '0'
		}
		ls() {
			printf '%s\n' 'drwxrwxrwx 1 0 0 0 Jan 1 00:00 /'
		}
		zxfer_is_trusted_symlink_path_component \"$trusted_root_symlink\"
	"

	assertEquals "Trusted-root symlink checks should reject world-writable root parents without a sticky bit." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Untrusted root-permission layouts should not emit a trusted result payload." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_find_symlink_path_component_returns_empty_for_relative_non_symlink_path() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/relative_plain_dir/subdir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	result=$(zxfer_find_symlink_path_component "./relative_plain_dir/subdir/file")
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative paths without symlink components should still return failure." 1 "$status"
	assertEquals "Relative non-symlink checks should not report a component." "" "$result"
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

test_zxfer_get_path_mode_octal_returns_failure_when_ls_fallback_cannot_map_permissions() {
	zxfer_test_capture_subshell "
		cd \"$TEST_TMPDIR\" || exit 1
		: >\"mode_unknown\"
		stat() {
			return 1
		}
		ls() {
			printf '%s\n' '-rw-r----- 1 0 0 0 Jan 1 00:00 ./mode_unknown'
		}
		zxfer_get_path_mode_octal \"mode_unknown\"
	"

	assertEquals "Mode lookups should fail when the ls fallback cannot map permissions to an octal value." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed ls-mode fallbacks should not emit a value." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_returns_failure_when_ls_lookup_fails() {
	candidate="$TEST_TMPDIR/validate_tmp_root_ls_failure"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		ls() {
			return 1
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should fail closed when directory permission lookup fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed temp-root validation should not emit a physical directory path." "" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_nonroot_owned_dir_when_effective_uid_lookup_fails() {
	candidate="$TEST_TMPDIR/validate_tmp_root_effective_uid_failure"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		ls() {
			printf '%s\n' 'drwx------ 2 1234 0 0 Jan 1 00:00 $candidate'
		}
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should fail closed when a non-root directory cannot be matched to the effective uid." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed effective-uid validation should not emit a physical directory path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_rejects_non_sticky_world_writable_dir_directly() {
	candidate="$TEST_TMPDIR/validate_tmp_root_insecure_mode"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		ls() {
			printf '%s\n' 'drwxrwxrwx 1 0 0 0 Jan 1 00:00 $candidate'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should reject world-writable directories without a sticky bit." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Rejected insecure temp-root candidates should not emit a physical directory path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_temp_root_candidate_reads_owner_and_mode_from_one_ls() {
	candidate="$TEST_TMPDIR/validate_tmp_root_one_ls"
	probe_log="$TEST_TMPDIR/validate_tmp_root_one_ls.log"
	mkdir -p "$candidate"
	rm -f "$probe_log"

	zxfer_test_capture_subshell "
		ls() {
			printf 'ls\\n' >>\"$probe_log\"
			printf '%s\\n' \"drwxrwxrwt 9 \$l_test_owner 0 0 Jan 1 00:00 $candidate\"
		}
		stat() {
			printf 'stat\\n' >>\"$probe_log\"
			return 1
		}
		zxfer_get_effective_user_uid() {
			printf 'id\\n' >>\"$probe_log\"
			printf '%s\\n' 4242
		}
		l_test_owner=0
		zxfer_validate_temp_root_candidate \"$candidate\" >/dev/null
		printf 'root_status=%s\\n' \"\$?\"
		l_test_owner=4242
		zxfer_validate_temp_root_candidate \"$candidate\" >/dev/null
		printf 'user_status=%s\\n' \"\$?\"
		l_test_owner=4343
		zxfer_validate_temp_root_candidate \"$candidate\" >/dev/null
		printf 'other_status=%s\\n' \"\$?\"
		l_test_owner=owner
		zxfer_validate_temp_root_candidate \"$candidate\" >/dev/null
		printf 'named_status=%s\\n' \"\$?\"
	"

	assertEquals "A root-owned sticky candidate should pass, an effective-user candidate should pass, another owner and a non-numeric owner should fail." \
		"root_status=0
user_status=0
other_status=1
named_status=1" "$ZXFER_TEST_CAPTURE_OUTPUT"
	assertEquals "Each validation should run one ls and never stat; id runs only for numeric non-root owners." \
		"ls
ls
id
ls
id
ls" "$(cat "$probe_log")"
}

test_zxfer_validate_temp_root_candidate_rejects_relative_physical_pwd_output() {
	candidate="$TEST_TMPDIR/validate_tmp_root_relative_pwd"
	mkdir -p "$candidate"

	zxfer_test_capture_subshell "
		pwd() {
			printf '%s\n' 'relative-path'
		}
		zxfer_validate_temp_root_candidate \"$candidate\"
	"

	assertEquals "Validated temp-root selection should reject non-absolute physical-directory results." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Rejected relative physical-directory results should not emit a temp-root path." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}
