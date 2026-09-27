#!/bin/sh
# ZXFER_ERROR_LOG tests for src/zxfer_reporting.sh: path, parent, owner, mode,
# hard-link and FIFO refusals, exclusive 0600 creation (narrowed to 0600 only
# once the checks pass), the one-write append of small and large reports, and
# failing runs that mirror at the same time. Run by
# tests/test_zxfer_reporting.sh.
# shellcheck disable=SC1090,SC2016,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Make a log parent read-only for this user. When the user can still write
# it (root, or an ACL), restore it and skip the test, which needs a parent
# where this user cannot create files.
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

test_zxfer_append_failure_report_to_log_warns_when_the_write_fails() {
	log_path="$TEST_TMPDIR/append-failure.log"
	stdout_file="$TEST_TMPDIR/append_failure.stdout"
	stderr_file="$TEST_TMPDIR/append_failure.stderr"

	: >"$log_path"
	chmod 600 "$log_path"

	# The report is written by the awk in g_cmd_awk; false stands in for an
	# awk whose write or close fails.
	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		g_cmd_awk=false
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

test_zxfer_append_failure_report_to_log_returns_failure_when_created_file_fails_validation() {
	log_path="$TEST_TMPDIR/create-validation-failure.log"
	stdout_file="$TEST_TMPDIR/create_validation_failure.stdout"
	stderr_file="$TEST_TMPDIR/create_validation_failure.stderr"

	zxfer_test_capture_subshell_split "$stdout_file" "$stderr_file" "
		ZXFER_ERROR_LOG=\"$log_path\"
		zxfer_validate_existing_error_log_file() {
			printf '%s\n' \"zxfer: warning: refusing ZXFER_ERROR_LOG file \\\"\$1\\\" because its permissions could not be determined.\" >&2
			return 1
		}
		zxfer_append_failure_report_to_log \"report\"
	"

	assertEquals "Validation failures after secure file creation should return a non-zero status." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Validation failures after secure file creation should preserve the validation warning." \
		"$(cat "$stderr_file")" "permissions could not be determined"
}

test_zxfer_append_failure_report_to_log_creates_secure_file() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_dir="$physical_tmpdir/created_log_parent"
	log_path="$log_dir/failure.log"
	mkdir -p "$log_dir"
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

	assertEquals "ZXFER_ERROR_LOG appends should succeed for valid absolute paths." 0 "$status"
	assertEquals "Failure log should be created when ZXFER_ERROR_LOG is valid." 1 "$file_exists"
	assertEquals "Log file mode should be readable for assertions." 0 "$perms_status"
	assertEquals "ZXFER_ERROR_LOG files should be created with mode 600." "600" "$perms"
	assertEquals "A new log should hold exactly the report and its newline." \
		"$report_contents" "$(cat "$log_path")"
	assertEquals "Creating and appending should leave no lock or staging entry beside the log." \
		"failure.log" "$(ls -A "$log_dir")"
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
	assertEquals "ZXFER_ERROR_LOG appends should preserve prior log contents." 0 "$existing_status"
	assertEquals "ZXFER_ERROR_LOG appends should add the new report payload." 0 "$append_status"
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

test_zxfer_append_failure_report_to_log_rejects_a_log_with_another_hard_link() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	other_path="$physical_tmpdir/hard_link_other"
	log_path="$physical_tmpdir/hard_link.log"
	stderr_file="$TEST_TMPDIR/error_log_hard_link.stderr"
	printf '%s\n' "other: keep-me" >"$other_path"
	chmod 600 "$other_path"
	ln "$other_path" "$log_path" || fail "Unable to create the hard-link fixture."
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "message: should-not-append" \
		>"$TEST_TMPDIR/error_log_hard_link.stdout" 2>"$stderr_file"
	status=$?

	assertEquals "A log that is also reachable by another name should be rejected." 1 "$status"
	assertContains "The hard-link rejection should give the link count." \
		"$(cat "$stderr_file")" "because it has 2 hard links"
	assertEquals "The file's other name must not receive the report." \
		"other: keep-me" "$(cat "$other_path")"
}

test_zxfer_append_failure_report_to_log_rejects_unknown_link_count() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/unknown_links.log"
	stderr_file="$TEST_TMPDIR/error_log_unknown_links.stderr"
	: >"$log_path"
	chmod 600 "$log_path"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		ls() {
			[ "$2" != "$log_path" ] || return 1
			command ls "$@"
		}
		zxfer_append_failure_report_to_log "message: should-not-append"
	) >"$TEST_TMPDIR/error_log_unknown_links.stdout" 2>"$stderr_file"
	status=$?

	assertEquals "A log whose link count cannot be read should be rejected." 1 "$status"
	assertContains "The unknown link-count rejection should explain the refusal." \
		"$(cat "$stderr_file")" "because its link count could not be determined"
	assertEquals "A rejected log must not receive the report." "" "$(cat "$log_path")"
}

test_zxfer_append_failure_report_to_log_warns_when_the_log_cannot_be_created() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	stderr_file="$TEST_TMPDIR/error_log_create_failure.stderr"
	# No supported file system accepts a 300-byte name, so the create fails
	# even for root, which a read-only parent cannot stop.
	ZXFER_ERROR_LOG="$physical_tmpdir/$(printf '%0300d' 0)"

	set +e
	zxfer_append_failure_report_to_log "message: create-failed" \
		>"$TEST_TMPDIR/error_log_create_failure.stdout" 2>"$stderr_file"
	status=$?

	assertEquals "A log that cannot be created should be reported as a failure." 1 "$status"
	assertContains "A log that cannot be created should produce the documented warning." \
		"$(cat "$stderr_file")" "unable to create ZXFER_ERROR_LOG file"
}

# A default ACL on the parent can give a new log more than the umask allows;
# the umask stub stands in for one.
test_zxfer_append_failure_report_to_log_narrows_a_wider_new_log_to_0600() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/wider_new.log"
	stderr_file="$TEST_TMPDIR/error_log_wider_new.stderr"
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		umask() {
			command umask 022
		}
		zxfer_append_failure_report_to_log "message: wider-new-log"
	) >"$TEST_TMPDIR/error_log_wider_new.stdout" 2>"$stderr_file"
	status=$?
	perms=$(stat -c '%a' "$log_path" 2>/dev/null || stat -f '%Lp' "$log_path" 2>/dev/null)

	assertEquals "A new log created wider than 0600 should still take the report." 0 "$status"
	assertEquals "zxfer should narrow a new log to mode 600." "600" "$perms"
	assertEquals "The new log should hold the report." \
		"message: wider-new-log" "$(cat "$log_path")"
	assertEquals "Narrowing a new log should not warn." "" "$(cat "$stderr_file")"
}

# CREATED 1 stands for a create step that opened what another user put at the
# name; the chmod for a new log must not reach that entry's target.
test_zxfer_validate_existing_error_log_file_never_changes_a_planted_entry() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	target_path="$physical_tmpdir/planted_target"
	symlink_path="$physical_tmpdir/planted_symlink.log"
	hard_link_path="$physical_tmpdir/planted_hard_link.log"
	printf '%s\n' "target: keep-me" >"$target_path"
	chmod 644 "$target_path"
	ln -s "$target_path" "$symlink_path"
	ln "$target_path" "$hard_link_path" || fail "Unable to create the hard-link fixture."

	set +e
	zxfer_validate_existing_error_log_file "$symlink_path" 1 \
		2>"$TEST_TMPDIR/planted_symlink.stderr"
	symlink_status=$?
	zxfer_validate_existing_error_log_file "$hard_link_path" 1 \
		2>"$TEST_TMPDIR/planted_hard_link.stderr"
	hard_link_status=$?
	perms=$(stat -c '%a' "$target_path" 2>/dev/null || stat -f '%Lp' "$target_path" 2>/dev/null)

	assertEquals "A symlink at the new log's name should be refused." 1 "$symlink_status"
	assertContains "The symlink refusal should explain it." \
		"$(cat "$TEST_TMPDIR/planted_symlink.stderr")" "because it is a symlink"
	assertEquals "A hard link at the new log's name should be refused." 1 "$hard_link_status"
	assertContains "The hard-link refusal should give the link count." \
		"$(cat "$TEST_TMPDIR/planted_hard_link.stderr")" "because it has 2 hard links"
	assertEquals "The planted entries' target must keep its mode." "644" "$perms"
}

# Opening a FIFO for writing blocks until someone reads it, which would hang
# zxfer's EXIT trap; a FIFO at the log path must be refused without an open.
test_zxfer_append_failure_report_to_log_refuses_a_fifo_without_opening_it() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/fifo.log"
	stderr_file="$TEST_TMPDIR/error_log_fifo.stderr"
	status_file="$TEST_TMPDIR/error_log_fifo.status"
	mkfifo "$log_path" || fail "Unable to create the FIFO fixture."
	ZXFER_ERROR_LOG="$log_path"

	set +e
	(
		zxfer_append_failure_report_to_log "message: should-not-append" \
			>/dev/null 2>"$stderr_file"
		printf '%s\n' "$?" >"$status_file"
	) &
	append_pid=$!
	l_tries=0
	while [ ! -s "$status_file" ] && [ "$l_tries" -lt 50 ]; do
		sleep 0.1 2>/dev/null || sleep 1
		l_tries=$((l_tries + 1))
	done
	blocked=0
	if [ ! -s "$status_file" ]; then
		# A read-write open of the FIFO releases a writer blocked in its open.
		blocked=1
		(true <>"$log_path")
	fi
	wait "$append_pid"

	assertEquals "zxfer must not open a FIFO at the log path." 0 "$blocked"
	assertEquals "A FIFO at the log path should be refused." "1" "$(cat "$status_file")"
	assertContains "The FIFO refusal should explain it." \
		"$(cat "$stderr_file")" "because it is not a regular file"
}

# Linux caps one exec argument or environment string at 128 KiB, so a report
# this size arrives whole only through the pipe to awk.
test_zxfer_append_failure_report_to_log_mirrors_a_report_larger_than_an_exec_string() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	log_path="$physical_tmpdir/large_report.log"
	expected_file="$TEST_TMPDIR/large_report.expected"
	awk 'BEGIN {
		printf "zxfer: failure report begin\nlast_command: "
		for (i = 0; i < 300000; i++)
			printf "x"
		printf "\nzxfer: failure report end\n"
	}' >"$expected_file"
	report=$(cat "$expected_file")
	ZXFER_ERROR_LOG="$log_path"

	set +e
	zxfer_append_failure_report_to_log "$report" 2>"$TEST_TMPDIR/large_report.stderr"
	status=$?

	assertEquals "A report larger than an exec string should be mirrored." 0 "$status"
	assertTrue "The log should hold the whole report." \
		"cmp -s '$expected_file' '$log_path'"
}

# Purpose: Run the launcher to a usage failure that mirrors its report to
# LOG_PATH, with verbatim command fields so the report names TAG's source.
# Usage: zxfer_test_error_log_usage_failure SECURE_PATH_DIR LOG_PATH TAG;
# writes TEST_TMPDIR/concurrent_TAG.stdout and .stderr.
zxfer_test_error_log_usage_failure() {
	env -i \
		HOME="${HOME:-$TEST_TMPDIR}" \
		TMPDIR="$TEST_TMPDIR" \
		PATH="/usr/bin:/bin:/usr/sbin:/sbin" \
		ZXFER_SECURE_PATH="$1:/sbin:/bin:/usr/sbin:/usr/bin" \
		ZXFER_ERROR_LOG="$2" \
		ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
		"$ZXFER_ROOT/zxfer" -R "tank/$3" \
		>"$TEST_TMPDIR/concurrent_$3.stdout" 2>"$TEST_TMPDIR/concurrent_$3.stderr"
}

# Purpose: Print each failure-report block of FILE... as one tab-joined line,
# sorted, so logs holding the same reports in any order compare equal and an
# interleaved block does not.
# Usage: zxfer_test_error_log_blocks_as_lines FILE...
zxfer_test_error_log_blocks_as_lines() {
	awk '
		{ block = block (block == "" ? "" : "\t") $0 }
		$0 == "zxfer: failure report end" { print block; block = "" }
		END { if (block != "") print "unterminated\t" block }
	' "$@" | LC_ALL=C sort
}

# Failing runs that start together race to create the log and then append at
# the same time. Each report must land whole and once, in any order, and
# nothing but the log may be left beside it. Four runs, not two: bash
# line-buffers its printf, and a printf writer broke about one round in
# twenty with two runs but most rounds with four.
test_concurrent_failing_runs_append_intact_reports() {
	secure_path_dir="$TEST_TMPDIR/concurrent_secure_path"
	log_dir="$TEST_TMPDIR/concurrent_log_parent"
	log_path="$log_dir/failure.log"
	create_launcher_usage_secure_path "$secure_path_dir" || return
	mkdir -p "$log_dir"

	set +e
	l_pids=""
	for l_tag in one two three four; do
		zxfer_test_error_log_usage_failure "$secure_path_dir" "$log_path" "$l_tag" &
		l_pids="$l_pids $!"
	done
	l_statuses=""
	for l_pid in $l_pids; do
		wait "$l_pid"
		l_statuses="$l_statuses $?"
	done
	l_reports=""
	for l_tag in one two three four; do
		sed -n '/^zxfer: failure report begin$/,/^zxfer: failure report end$/p' \
			"$TEST_TMPDIR/concurrent_$l_tag.stderr" >"$TEST_TMPDIR/concurrent_$l_tag.report"
		l_reports="$l_reports $TEST_TMPDIR/concurrent_$l_tag.report"
	done
	# shellcheck disable=SC2086 # One word per report file.
	expected_blocks=$(zxfer_test_error_log_blocks_as_lines $l_reports)

	assertEquals "Every run should exit with the usage status." " 2 2 2 2" "$l_statuses"
	assertEquals "Each run's stderr should hold one report naming its own source." \
		"1 1 1 1" "$(for l_tag in one two three four; do
			grep -c "'tank/$l_tag'" "$TEST_TMPDIR/concurrent_$l_tag.report"
		done | tr '\n' ' ' | sed 's/ $//')"
	assertEquals "The log should hold every stderr report, each whole and once." \
		"$expected_blocks" "$(zxfer_test_error_log_blocks_as_lines "$log_path")"
	assertNotContains "No run should warn." \
		"$(cat "$TEST_TMPDIR"/concurrent_*.stderr)" "warning:"
	assertEquals "Only the log should remain in its parent." \
		"failure.log" "$(ls -A "$log_dir")"
}
