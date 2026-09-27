#!/bin/sh
# Backup path tests for src/zxfer_backup_metadata.sh: metadata filenames,
# restore candidates, local backup directories, and the backup-directory
# preflight locally, remotely and in dry runs (the records fragment covers
# the file check). Run by tests/test_zxfer_backup_metadata.sh under the
# remote-host fixture.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Run the -T backup-root check for DIR on backup@example.com.
zxfer_backup_test_check_remote_dir() {
	g_option_k_backup_property_mode=1
	g_option_n_dryrun=0
	g_option_T_target_host="backup@example.com"
	g_backup_storage_root=$1
	zxfer_check_backup_storage_dir_if_needed
}

test_backup_storage_helpers_cover_identity_encoding_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_helper_fallback.out"
	status_file="$TEST_TMPDIR/backup_helper_fallback.status"
	g_backup_file_extension=".zxfer_backup_info"

	(
		g_cmd_awk=false
		zxfer_get_backup_metadata_filename "tank/src" "backup/dst" >"$output_file"
		printf '%s\n' "$?" >"$status_file"
	)
	assertEquals "Backup metadata filenames should fail closed in the current shell when the identity hex pass fails." \
		1 "$(cat "$status_file")"
	assertEquals "Failed exact identity key derivation should not emit a placeholder filename." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_backup_metadata_filename_runs_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_filename_current_shell.out"
	g_backup_file_extension=".zxfer_backup_info"

	zxfer_get_backup_metadata_filename "tank/src" "backup/dst" >"$output_file"

	assertContains "Backup metadata filename rendering should run in the current shell." \
		"$(cat "$output_file")" ".zxfer_backup_info.v2/h/"
	assertContains "Backup metadata filename rendering should use the fixed v2 leaf name." \
		"$(cat "$output_file")" "/.zxfer_backup_info.v2"
}

test_zxfer_try_backup_restore_candidate_returns_missing_for_missing_local_candidate() {
	assertEquals "Missing local backup candidates should return the candidate-missing sentinel." \
		1 "$(
			(
				zxfer_read_local_backup_file() {
					return 4
				}
				zxfer_try_backup_restore_candidate "$TEST_TMPDIR/missing" "tank/src" "backup/dst" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_missing_for_missing_remote_candidate() {
	assertEquals "Missing remote backup candidates should return the candidate-missing sentinel." \
		1 "$(
			(
				zxfer_read_remote_backup_file() {
					return 4
				}
				zxfer_try_backup_restore_candidate "$TEST_TMPDIR/missing" "tank/src" "backup/dst" "tank/src" "backup/dst" "backup@example.com" source
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_failure_for_unexpected_match_status() {
	assertEquals "Unexpected backup-metadata match statuses should fail closed as read/parse errors." \
		5 "$(
			(
				zxfer_read_local_backup_file() {
					g_zxfer_backup_file_read_result=$(zxfer_test_render_current_backup_metadata_contents \
						"tank/src,backup/dst,compression=lz4")
					return 0
				}
				zxfer_backup_metadata_extract_properties_for_dataset_pair() {
					return 99
				}
				zxfer_try_backup_restore_candidate "$TEST_TMPDIR/weird" "tank/src" "backup/dst" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_get_backup_metadata_filename_uses_source_and_destination_identity() {
	g_backup_file_extension=".zxfer_backup_info"

	first_name=$(zxfer_get_backup_metadata_filename "tank/a/src" "backup/one")
	second_name=$(zxfer_get_backup_metadata_filename "tank/b/src" "backup/one")
	third_name=$(zxfer_get_backup_metadata_filename "tank/a/src" "backup/two")

	assertContains "Backup metadata filenames should use the current chunked v2 identity path." \
		"$first_name" ".zxfer_backup_info.v2/h/"
	assertNotEquals "Distinct source datasets that share the same tail should produce different backup metadata filenames." \
		"$first_name" "$second_name"
	assertNotEquals "Distinct destination roots for the same source should produce different backup metadata filenames." \
		"$first_name" "$third_name"
}

test_ensure_local_backup_dir_rejects_symlink_and_non_directory_targets() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_real"
	symlink_dir="$physical_tmpdir/ensure_local_link"
	non_dir="$physical_tmpdir/ensure_local_file"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$symlink_dir"
	: >"$non_dir"

	set +e
	symlink_output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$symlink_dir"
		)
	)
	symlink_status=$?

	non_dir_output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$non_dir"
		)
	)
	non_dir_status=$?

	assertEquals "Symlinked backup directories should be rejected." 1 "$symlink_status"
	assertContains "Symlinked backup directories should use the documented error." \
		"$symlink_output" "Refusing to use backup directory $symlink_dir because it is a symlink."
	assertEquals "Non-directory backup paths should be rejected." 1 "$non_dir_status"
	assertContains "Non-directory backup paths should use the documented error." \
		"$non_dir_output" "Refusing to use backup directory $non_dir because it is not a directory."
}

test_ensure_local_backup_dir_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_nested_real"
	link_dir="$physical_tmpdir/ensure_local_nested_link"
	target_dir="$link_dir/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$target_dir"
		)
	)
	status=$?

	assertEquals "Backup directories with symlinked parent components should be rejected before mkdir -p follows them." \
		1 "$status"
	assertContains "Nested symlink failures should identify the offending path component." \
		"$output" "Refusing to use backup directory $target_dir because path component $link_dir is a symlink."
}

test_ensure_local_backup_dir_rejects_relative_nested_symlink_components() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_local_relative_nested_real"
	link_dir="$physical_tmpdir/ensure_local_relative_nested_link"
	target_dir="./ensure_local_relative_nested_link/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$target_dir"
		)
	)
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative backup directories with symlinked parent components should be rejected before mkdir -p follows them." \
		1 "$status"
	assertContains "Relative nested symlink failures should identify the offending relative path component." \
		"$output" "Refusing to use backup directory $target_dir because path component ./ensure_local_relative_nested_link is a symlink."
}

test_ensure_local_backup_dir_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-local-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	zxfer_ensure_local_backup_dir "$target_dir"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block local backup directory creation, which keeps default /var- or /tmp-backed paths working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the secure backup directory to be created under the symlink target." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}
test_ensure_local_backup_dir_rejects_unknown_or_disallowed_owner() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/ensure_local_owner"
	mkdir -p "$backup_dir"

	set +e
	unknown_owner_output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		)
	)
	unknown_owner_status=$?

	disallowed_owner_output=$(
		(
			zxfer_get_path_owner_uid() {
				printf '%s\n' "1234"
			}
			zxfer_backup_owner_uid_is_allowed() {
				return 1
			}
			zxfer_describe_expected_backup_owner() {
				printf '%s\n' "root (UID 0)"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		)
	)
	disallowed_owner_status=$?

	assertEquals "Backup directories with unknown owners should be rejected." 1 "$unknown_owner_status"
	assertContains "Unknown owner failures should use the documented error." \
		"$unknown_owner_output" "Cannot determine the owner of backup directory $backup_dir."
	assertEquals "Backup directories owned by other UIDs should be rejected." 1 "$disallowed_owner_status"
	assertContains "Disallowed owner failures should identify the unexpected UID." \
		"$disallowed_owner_output" "Refusing to use backup directory $backup_dir because it is owned by UID 1234 instead of root (UID 0)."
}

test_ensure_local_backup_dir_reports_chmod_failures_in_current_shell() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/ensure_local_chmod_fail"
	fake_bin="$TEST_TMPDIR/ensure_local_chmod_bin"
	throw_file="$TEST_TMPDIR/ensure_local_chmod_throw"
	mkdir -p "$backup_dir" "$fake_bin"
	cat >"$fake_bin/chmod" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin/chmod"
	: >"$throw_file"
	(
		PATH="$fake_bin:$PATH"
		export PATH
		zxfer_throw_error() {
			printf '%s\n' "$1" >"$throw_file"
			return 1
		}

		zxfer_ensure_local_backup_dir "$backup_dir"
	)
	status=$?
	THROW_MSG=$(cat "$throw_file")

	assertEquals "chmod failures should cause zxfer_ensure_local_backup_dir to fail." 1 "$status"
	assertContains "chmod failures should use the documented backup-directory error." \
		"$THROW_MSG" "Error securing backup directory $backup_dir."
}

test_ensure_local_backup_dir_reports_mkdir_failures_in_current_shell() {
	backup_dir="$TEST_TMPDIR_PHYSICAL/ensure_local_mkdir_fail"
	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_local_backup_dir "$backup_dir"
		)
	)
	status=$?

	assertEquals "mkdir failures should cause zxfer_ensure_local_backup_dir to fail." 1 "$status"
	assertContains "mkdir failures should use the documented secure backup-directory error." \
		"$output" "Error creating secure backup directory $backup_dir."
}

test_check_backup_storage_dir_if_needed_reports_remote_ssh_failures() {
	set +e
	ssh_failure_output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_backup_test_check_remote_dir "-remote_backup"
		)
	)
	ssh_failure_status=$?

	assertEquals "Remote backup directory ssh failures should abort the helper." 1 "$ssh_failure_status"
	assertContains "Remote backup directory ssh failures should use the documented error." \
		"$ssh_failure_output" "Error preparing backup directory on backup@example.com."
}

test_check_backup_storage_dir_if_needed_remote_marks_missing_secure_path_helpers_as_dependency_errors() {
	empty_dir="$TEST_TMPDIR/ensure_remote_missing_helper_bin"
	mkdir -p "$empty_dir"

	set +e
	output=$(
		(
			g_zxfer_secure_path="$empty_dir"
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf 'class=%s\n' "${g_zxfer_failure_class:-}"
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_backup_test_check_remote_dir "/tmp/remote_backup"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup directory preparation should fail closed when required secure-PATH helpers are missing." \
		1 "$status"
	assertContains "Missing remote backup-dir helpers should surface the exact dependency name from the remote precheck." \
		"$output" "Required dependency \"mkdir\" not found on host backup@example.com in secure PATH ($empty_dir)."
	assertContains "Missing remote backup-dir helpers should be classified as dependency failures locally." \
		"$output" "class=dependency"
	assertContains "Missing remote backup-dir helpers should use the dependency-specific local error." \
		"$output" "Required remote backup-directory helper dependency not found on host backup@example.com in secure PATH ($empty_dir)."
}

test_check_backup_storage_dir_if_needed_remote_quotes_dash_prefixed_paths() {
	ssh_log="$TEST_TMPDIR/ensure_remote_dash.log"
	ssh_bin="$TEST_TMPDIR/ensure_remote_dash_ssh"
	cat >"$ssh_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >"$ssh_log"
exit 0
EOF
	chmod +x "$ssh_bin"
	(
		g_cmd_ssh="$ssh_bin"
		g_zxfer_secure_path="/session/secure/path:/usr/bin"
		zxfer_backup_test_check_remote_dir "-remote_backup"
	)

	assertContains "Remote backup directory preparation should scope auxiliary tools to a secure PATH." \
		"$(cat "$ssh_log")" "PATH="
	assertContains "That PATH is the session secure PATH." \
		"$(cat "$ssh_log")" "/session/secure/path:/usr/bin"
	assertContains "Dash-prefixed remote backup paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup"
}

test_check_backup_storage_dir_if_needed_remote_handles_csh_remote_login_shell() {
	l_csh_shell=$(find_csh_shell_for_tests)
	if [ "$l_csh_shell" = "" ]; then
		return 0
	fi

	realistic_ssh_bin="$TEST_TMPDIR/fake_ssh_backup_csh_exec"
	realistic_ssh_log="$TEST_TMPDIR/fake_ssh_backup_csh_exec.log"
	target_dir="$TEST_TMPDIR_PHYSICAL/ensure_remote_backup_csh/child"
	create_fake_ssh_join_csh_exec_bin "$realistic_ssh_bin" "$l_csh_shell"

	g_cmd_ssh="$realistic_ssh_bin"
	FAKE_SSH_LOG="$realistic_ssh_log"
	export FAKE_SSH_LOG

	zxfer_backup_test_check_remote_dir "$target_dir"
	status=$?
	unset FAKE_SSH_LOG

	assertEquals "Remote backup directory preparation should succeed through csh/tcsh login shells." \
		0 "$status"
	assertTrue "The csh/tcsh remote handoff should create the requested secure backup directory." \
		"[ -d \"$target_dir\" ]"
	assertEquals "The csh/tcsh backup handoff should receive one physical command line after the host line." \
		2 "$(sed -n '$=' "$realistic_ssh_log")"
}

test_check_backup_storage_dir_if_needed_remote_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_remote_nested_real"
	link_dir="$physical_tmpdir/ensure_remote_nested_link"
	target_dir="$link_dir/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_backup_test_check_remote_dir "$target_dir"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup directory preparation should reject symlinked parent components before mkdir -p follows them." \
		1 "$status"
	assertContains "Remote backup directory preparation should surface the offending symlinked path component." \
		"$output" "Refusing to use backup directory $target_dir because path component $link_dir is a symlink."
	assertContains "Remote backup directory preparation should still fail through the documented host-scoped error path." \
		"$output" "Error preparing backup directory on backup@example.com."
}

test_check_backup_storage_dir_if_needed_remote_rejects_root_owned_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_remote_nested_root_real"
	link_dir="$physical_tmpdir/ensure_remote_nested_root_link"
	target_dir="$link_dir/subdir"
	fake_bin="$physical_tmpdir/ensure_remote_nested_root_bin"
	mkdir -p "$real_dir" "$fake_bin"
	ln -s "$real_dir" "$link_dir"
	cat >"$fake_bin/stat" <<'EOF'
#!/bin/sh
case "$1 $2" in
	"-c %u"|"-f %u")
		printf '0\n'
		exit 0
		;;
esac
exit 1
EOF
	cat >"$fake_bin/ls" <<'EOF'
#!/bin/sh
for last_arg do :; done
	printf 'drwxr-xr-x 1 0 0 0 Jan  1 00:00 %s\n' "$last_arg"
EOF
	chmod +x "$fake_bin/stat" "$fake_bin/ls"

	set +e
	output=$(
		(
			g_zxfer_secure_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_backup_test_check_remote_dir "$target_dir"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup directory preparation should reject nested symlink components even when remote ownership probes report root-owned secure paths." \
		1 "$status"
	assertContains "Root-owned nested symlink rejection should still identify the offending path component." \
		"$output" "Refusing to use backup directory $target_dir because path component $link_dir is a symlink."
	assertContains "Root-owned nested symlink rejection should still fail through the documented host-scoped error path." \
		"$output" "Error preparing backup directory on backup@example.com."
}

test_check_backup_storage_dir_if_needed_remote_rejects_relative_nested_symlink_components() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/ensure_remote_relative_nested_real"
	link_dir="$physical_tmpdir/ensure_remote_relative_nested_link"
	target_dir="./ensure_remote_relative_nested_link/subdir"
	mkdir -p "$real_dir"
	ln -s "$real_dir" "$link_dir"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_backup_test_check_remote_dir "$target_dir"
		) 2>&1
	)
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Remote backup directory preparation should reject relative symlinked parent components before mkdir -p follows them." \
		1 "$status"
	assertContains "Relative remote backup directory preparation should surface the offending relative symlinked path component." \
		"$output" "Refusing to use backup directory $target_dir because path component ./ensure_remote_relative_nested_link is a symlink."
	assertContains "Relative remote backup directory preparation should still fail through the documented host-scoped error path." \
		"$output" "Error preparing backup directory on backup@example.com."
}

test_check_backup_storage_dir_if_needed_remote_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-remote-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	(
		zxfer_build_remote_sh_c_command() {
			g_zxfer_remote_sh_c_command_result=$1
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_backup_test_check_remote_dir "$target_dir"
	)
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup directory preparation, which keeps default /var- or /tmp-backed remote roots working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the remote backup directory helper to create the requested directory." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}

test_check_backup_storage_dir_if_needed_routes_local_and_remote() {
	local_log="$TEST_TMPDIR/check_backup_local.log"
	remote_log="$TEST_TMPDIR/check_backup_remote.log"
	: >"$local_log"
	: >"$remote_log"

	(
		LOCAL_LOG="$local_log"
		zxfer_ensure_local_backup_dir() {
			printf '%s\n' "$1" >>"$LOCAL_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host=""
		g_backup_storage_root="$TEST_TMPDIR/local_backup"
		zxfer_check_backup_storage_dir_if_needed
	)

	(
		REMOTE_LOG="$remote_log"
		zxfer_run_remote_backup_script() {
			printf '%s|%s\n' "$1" "$4" >>"$REMOTE_LOG"
		}
		g_option_k_backup_property_mode=1
		g_option_T_target_host="target.example"
		g_backup_storage_root="$TEST_TMPDIR/remote_backup"
		zxfer_check_backup_storage_dir_if_needed
	)

	assertEquals "Local backup checks should validate the local backup root." \
		"$TEST_TMPDIR/local_backup" "$(cat "$local_log")"
	assertEquals "Remote backup checks should run the directory program on the target host." \
		"target.example|preparing backup directory $TEST_TMPDIR/remote_backup" "$(cat "$remote_log")"
}

test_check_backup_storage_dir_if_needed_returns_success_when_disabled() {
	g_option_k_backup_property_mode=0

	zxfer_check_backup_storage_dir_if_needed
	status=$?

	assertEquals "Disabled backup metadata mode should remain a successful no-op at checked composition boundaries." \
		0 "$status"
}

# The root comes from ZXFER_BACKUP_DIR once, at session init; the check uses
# that validated root and never re-reads the environment.
test_check_backup_storage_dir_if_needed_refreshes_backup_root_from_environment() {
	output=$(
		(
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/stale_backup"
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/session backup"
			zxfer_init_backup_storage_root
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/later backup"
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)

	assertContains "Backup-dir preflight should preview the root that session init validated." \
		"$output" "'$TEST_TMPDIR/session backup'"
	assertNotContains "Backup-dir preflight should not preview an inherited root." \
		"$output" "'$TEST_TMPDIR/stale_backup'"
	assertNotContains "Backup-dir preflight should not re-read ZXFER_BACKUP_DIR after init." \
		"$output" "'$TEST_TMPDIR/later backup'"
}

test_check_backup_storage_dir_if_needed_dry_run_previews_without_mutating_dirs() {
	local_log="$TEST_TMPDIR/check_backup_dry_run_local.log"
	remote_log="$TEST_TMPDIR/check_backup_dry_run_remote.log"
	: >"$local_log"
	: >"$remote_log"

	output=$(
		(
			LOCAL_LOG="$local_log"
			REMOTE_LOG="$remote_log"
			zxfer_ensure_local_backup_dir() {
				printf '%s\n' "$1" >>"$LOCAL_LOG"
			}
			zxfer_run_remote_backup_script() {
				printf '%s %s\n' "$1" "$4" >>"$REMOTE_LOG"
			}
			g_cmd_ssh="/usr/bin/ssh"
			g_option_k_backup_property_mode=1
			g_option_n_dryrun=1
			g_option_v_verbose=1
			g_option_T_target_host=""
			g_backup_storage_root="$TEST_TMPDIR/local backup"
			zxfer_check_backup_storage_dir_if_needed
			g_zxfer_secure_path="/fresh/secure/path:/usr/bin"
			g_option_T_target_host="target.example doas"
			g_backup_storage_root="/var/db/zxfer remote"
			zxfer_check_backup_storage_dir_if_needed
		) 2>&1
	)

	assertEquals "Dry-run backup preflight should not call the live local backup-dir helper." \
		"" "$(cat "$local_log")"
	assertEquals "Dry-run backup preflight should not call the live remote backup-dir helper." \
		"" "$(cat "$remote_log")"
	assertContains "Dry-run backup preflight should preview the local secure backup-dir creation command." \
		"$output" "Dry run: umask 077; 'mkdir' '-p' '$TEST_TMPDIR/local backup'; 'chmod' '700' '$TEST_TMPDIR/local backup'"
	assertContains "Dry-run backup preflight should preview the remote ssh transport instead of executing it." \
		"$output" "Dry run: '/usr/bin/ssh' '-o' 'BatchMode=yes' '-o' 'StrictHostKeyChecking=yes' 'target.example'"
	assertContains "Dry-run backup preflight should preserve remote wrapper tokens in the rendered preview." \
		"$output" "doas"
	assertContains "Dry-run remote backup preflight should preview the secure-PATH prologue that live execution now applies." \
		"$output" "PATH="
	assertContains "Dry-run remote backup preflight should preview the session secure PATH." \
		"$output" "/fresh/secure/path:/usr/bin"
	assertContains "Dry-run remote backup preflight should preview the remote symlink guard that live execution enforces." \
		"$output" "Refusing to use symlinked zxfer backup directory."
	assertContains "Dry-run backup preflight should preview the remote secure backup-dir path." \
		"$output" "'/var/db/zxfer remote'"
	assertContains "Dry-run backup preflight should preview the remote chmod command." \
		"$output" "'chmod'"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_render_failures() {
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_render_remote_backup_dry_run_shell_command() {
			return 46
		}
		zxfer_check_backup_storage_dir_if_needed
	'

	assertEquals "Remote dry-run backup preflight should preserve prepared renderer failures." \
		46 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_check_backup_storage_dir_if_needed_preserves_remote_dry_run_builder_failures() {
	l_builder_log="$TEST_TMPDIR/remote_backup_dry_run_builder_failure.log"
	: >"$l_builder_log"

	# shellcheck disable=SC2016  # Evaluated by zxfer_test_capture_subshell.
	zxfer_test_capture_subshell '
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_option_T_target_host="target.example doas"
		g_backup_storage_root="/var/db/zxfer"
		zxfer_build_remote_backup_dir_prepare_cmd() {
			return 45
		}
		zxfer_render_remote_backup_dry_run_shell_command() {
			printf "%s\n" unexpected-renderer-call >>"$l_builder_log"
		}
		zxfer_check_backup_storage_dir_if_needed
	'

	assertEquals "Remote dry-run backup preflight should preserve directory-script builder failures." \
		45 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "A failed directory-script build must stop before remote rendering." \
		"" "$(cat "$l_builder_log")"
}

# Session init validates ZXFER_BACKUP_DIR before the check can run.
test_check_backup_storage_dir_if_needed_rejects_relative_backup_dir_override() {
	zxfer_test_capture_subshell "
		g_option_k_backup_property_mode=1
		g_option_n_dryrun=1
		g_option_v_verbose=1
		g_backup_storage_root='$TEST_TMPDIR/stale_backup'
		ZXFER_BACKUP_DIR='relative-backups'
		zxfer_init_backup_storage_root
		zxfer_check_backup_storage_dir_if_needed
	"

	assertEquals "Backup-dir preflight should fail closed when ZXFER_BACKUP_DIR is relative." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Relative backup-root preflight failures should explain the absolute-path requirement." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_BACKUP_DIR must be an absolute path"
}
