#!/bin/sh
# Backup metadata, local/remote directory, and path-security behavior tests.
# shellcheck disable=SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

test_get_backup_storage_dir_for_dataset_tree_uses_dataset_hierarchy() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"

	assertEquals "Dataset-tree backup storage should mirror the dataset hierarchy under ZXFER_BACKUP_DIR." \
		"$g_backup_storage_root/tank/src/child" "$(zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/child")"
	assertEquals "Dataset-tree backup storage should trim trailing slashes." \
		"$g_backup_storage_root/tank/src" "$(zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/")"
	assertEquals "Empty dataset-tree lookups should use the dataset placeholder bucket." \
		"$g_backup_storage_root/dataset" "$(zxfer_get_backup_storage_dir_for_dataset_tree "/")"
}

test_get_backup_storage_dir_for_dataset_tree_runs_in_current_shell() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root_current_shell"
	output_file="$TEST_TMPDIR/backup_helper_current_shell.out"

	zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/child" >"$output_file"
	assertEquals "Dataset-tree storage lookups should run in the current shell for coverage." \
		"$g_backup_storage_root/tank/src/child" "$(cat "$output_file")"

	zxfer_get_backup_storage_dir_for_dataset_tree "/" >"$output_file"
	assertEquals "Rootlike dataset-tree lookups should use the dataset placeholder in the current shell." \
		"$g_backup_storage_root/dataset" "$(cat "$output_file")"
}

test_backup_storage_helpers_cover_identity_encoding_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_helper_fallback.out"
	status_file="$TEST_TMPDIR/backup_helper_fallback.status"

	(
		od() {
			:
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
		printf '%s\n' "$?" >"$status_file"
	)
	assertEquals "Backup metadata file keys should fail closed in the current shell when exact identity hex encoding produces no output." \
		1 "$(cat "$status_file")"
	assertEquals "Failed exact identity key derivation should not emit a placeholder key." \
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

test_backup_metadata_matches_source_accepts_only_v2_relative_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	current_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	legacy_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"tank/src,backup/dst,compression=lz4")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "Backup metadata matching should accept current v2 relative rows." \
		0 "$(
			(
				zxfer_backup_metadata_matches_source "$current_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject legacy exact-pair rows in v2 files." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "$legacy_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_backup_metadata_matches_source_rejects_wrong_destination_and_ambiguous_relative_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/other"
	wrong_destination_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")")
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	ambiguous_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "Backup metadata matching should reject rows for the requested source dataset when the destination root does not match." \
		1 "$(
			(
				zxfer_backup_metadata_matches_source "$wrong_destination_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject files that contain multiple relative rows for the same source/destination root." \
		2 "$(
			(
				zxfer_backup_metadata_matches_source "$ambiguous_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_backup_metadata_matches_source_rejects_malformed_current_format_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	missing_tab_contents=$(zxfer_test_render_current_backup_metadata_contents "broken-row")
	extra_comma_contents=$(zxfer_test_render_current_backup_metadata_contents "broken,legacy,row")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "Backup metadata matching should reject rows that do not contain the current relative-path/properties format." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "$missing_tab_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject rows that contain extra raw field delimiters." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "$extra_comma_contents" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_missing_for_missing_local_candidate() {
	assertEquals "Missing local backup candidates should return the candidate-missing sentinel." \
		1 "$(
			(
				zxfer_read_local_backup_file() {
					return 4
				}
				zxfer_try_backup_restore_candidate "/tmp/missing.meta" "tank/src" "backup/dst"
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
				zxfer_try_backup_restore_candidate "/tmp/missing.meta" "tank/src" "backup/dst" "backup@example.com" source
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
				zxfer_backup_metadata_matches_source() {
					return 99
				}
				zxfer_try_backup_restore_candidate "/tmp/weird.meta" "tank/src" "backup/dst"
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

test_zxfer_backup_metadata_file_key_fails_when_hex_encoding_is_empty() {
	(
		od() {
			:
		}

		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >/dev/null
		status=$?
		assertEquals "Backup metadata file keys should fail closed when exact identity hex encoding produces no output." \
			1 "$status"
	)
}

test_get_path_owner_uid_and_mode_use_numeric_stat_output() {
	result_uid=$(
		(
			stat() {
				if [ "$1" = "-c" ] && [ "$2" = "%u" ]; then
					printf '%s\n' "1234"
					return 0
				fi
				return 1
			}
			ls() {
				return 1
			}
			: >"$TEST_TMPDIR/stat-owner-file"
			zxfer_get_path_owner_uid "$TEST_TMPDIR/stat-owner-file"
		)
	)
	result_mode=$(
		(
			stat() {
				if [ "$1" = "-c" ] && [ "$2" = "%a" ]; then
					printf '%s\n' "640"
					return 0
				fi
				return 1
			}
			ls() {
				return 1
			}
			: >"$TEST_TMPDIR/stat-mode-file"
			zxfer_get_path_mode_octal "$TEST_TMPDIR/stat-mode-file"
		)
	)

	assertEquals "Numeric GNU stat output should be accepted directly for owner lookups." "1234" "$result_uid"
	assertEquals "Numeric GNU stat output should be accepted directly for mode lookups." "640" "$result_mode"
}

test_get_path_owner_uid_and_mode_return_failure_for_missing_paths() {
	missing_path="$TEST_TMPDIR/does_not_exist"

	zxfer_get_path_owner_uid "$missing_path" >/dev/null 2>&1
	owner_status=$?
	zxfer_get_path_mode_octal "$missing_path" >/dev/null 2>&1
	mode_status=$?

	assertEquals "Owner lookups should fail cleanly for missing paths." 1 "$owner_status"
	assertEquals "Mode lookups should fail cleanly for missing paths." 1 "$mode_status"
}

test_get_ssh_transport_tokens_for_host_returns_base_tokens_for_empty_host() {
	g_cmd_ssh="/usr/bin/ssh"

	assertEquals "Hosts omitted from wrapper lookups should return the base ssh transport tokens." \
		"$(printf '%s\n' /usr/bin/ssh -o BatchMode=yes -o StrictHostKeyChecking=yes)" \
		"$(zxfer_get_ssh_transport_tokens_for_host "")"
}

test_get_effective_user_uid_returns_failure_when_id_is_unavailable() {
	empty_path="$TEST_TMPDIR/no_id_path"
	mkdir -p "$empty_path"
	old_path=$PATH
	PATH="$empty_path"
	outfile="$TEST_TMPDIR/effective_uid.out"

	zxfer_get_effective_user_uid >"$outfile"
	status=$?
	PATH=$old_path

	assertEquals "Missing id binaries should make effective-UID detection fail cleanly." 1 "$status"
	assertEquals "Failed effective-UID detection should not emit output." "" "$(cat "$outfile")"
}

test_get_path_owner_uid_and_mode_use_stat_when_available() {
	owned_file="$TEST_TMPDIR/stat_owned_file"
	: >"$owned_file"

	owner_result=$(
		(
			stat() {
				if [ "$1" = "-f" ] && [ "$2" = "%u" ]; then
					printf '%s\n' "4242"
					return 0
				fi
				return 1
			}
			zxfer_get_path_owner_uid "$owned_file"
		)
	)

	mode_result=$(
		(
			stat() {
				if [ "$1" = "-f" ] && [ "$2" = "%OLp" ]; then
					printf '%s\n' "600"
					return 0
				fi
				return 1
			}
			zxfer_get_path_mode_octal "$owned_file"
		)
	)

	assertEquals "Owner lookup should use stat when available." "4242" "$owner_result"
	assertEquals "Mode lookup should use stat when available." "600" "$mode_result"
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

test_ensure_remote_backup_dir_skips_without_host_and_reports_ssh_failures() {
	if zxfer_ensure_remote_backup_dir "$TEST_TMPDIR/remote_backup" ""; then
		empty_host_status=0
	else
		empty_host_status=1
	fi

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
			zxfer_ensure_remote_backup_dir "-remote_backup" "backup@example.com"
		)
	)
	ssh_failure_status=$?

	assertEquals "Remote backup directory preparation should no-op when no host is provided." 0 "$empty_host_status"
	assertEquals "Remote backup directory ssh failures should abort the helper." 1 "$ssh_failure_status"
	assertContains "Remote backup directory ssh failures should use the documented error." \
		"$ssh_failure_output" "Error preparing backup directory on backup@example.com."
}

test_ensure_remote_backup_dir_marks_missing_secure_path_helpers_as_dependency_errors() {
	empty_dir="$TEST_TMPDIR/ensure_remote_missing_helper_bin"
	mkdir -p "$empty_dir"

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$empty_dir"
			zxfer_build_remote_sh_c_command() {
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
			zxfer_ensure_remote_backup_dir "/tmp/remote_backup" "backup@example.com"
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

test_ensure_remote_backup_dir_quotes_dash_prefixed_paths() {
	ssh_log="$TEST_TMPDIR/ensure_remote_dash.log"
	ssh_bin="$TEST_TMPDIR/ensure_remote_dash_ssh"
	cat >"$ssh_bin" <<EOF
#!/bin/sh
printf '%s\n' "\$@" >"$ssh_log"
exit 0
EOF
	chmod +x "$ssh_bin"
	g_cmd_ssh="$ssh_bin"
	g_zxfer_dependency_path="/stale/secure/path"
	ZXFER_SECURE_PATH="/fresh/secure/path:/usr/bin"

	zxfer_ensure_remote_backup_dir "-remote_backup" "backup@example.com"

	assertContains "Remote backup directory preparation should scope auxiliary tools to the secure dependency path." \
		"$(cat "$ssh_log")" "PATH="
	assertContains "Remote backup directory preparation should refresh the secure-PATH wrapper from ZXFER_SECURE_PATH instead of a stale cached value." \
		"$(cat "$ssh_log")" "/fresh/secure/path:/usr/bin"
	assertNotContains "Remote backup directory preparation should not keep using a stale cached secure PATH after ZXFER_SECURE_PATH changes." \
		"$(cat "$ssh_log")" "/stale/secure/path"
	assertContains "Dash-prefixed remote backup paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup"
}

test_ensure_remote_backup_dir_handles_csh_remote_login_shell() {
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

	zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com" destination
	status=$?
	unset FAKE_SSH_LOG

	assertEquals "Remote backup directory preparation should succeed through csh/tcsh login shells." \
		0 "$status"
	assertTrue "The csh/tcsh remote handoff should create the requested secure backup directory." \
		"[ -d \"$target_dir\" ]"
	assertEquals "The csh/tcsh backup handoff should receive one physical command line after the host line." \
		2 "$(sed -n '$=' "$realistic_ssh_log")"
}

test_ensure_remote_backup_dir_rejects_nested_symlink_components() {
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
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
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

test_ensure_remote_backup_dir_rejects_root_owned_nested_symlink_components() {
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
			g_zxfer_dependency_path="$fake_bin:$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
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

test_ensure_remote_backup_dir_rejects_relative_nested_symlink_components() {
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
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				sh -c "$2"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
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

test_ensure_remote_backup_dir_allows_trusted_absolute_root_symlink_components() {
	target_dir=$(mktemp -d /tmp/zxfer-remote-trusted.XXXXXX)/subdir
	rm -rf "${target_dir%/subdir}"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_throw_error() {
			printf '%s\n' "$1"
			exit 1
		}
		zxfer_ensure_remote_backup_dir "$target_dir" "backup@example.com"
	)
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup directory preparation, which keeps default /var- or /tmp-backed remote roots working on macOS." \
		0 "$status"
	assertTrue "Trusted absolute symlink components should still allow the remote backup directory helper to create the requested directory." \
		"[ -d \"$target_dir\" ]"

	rm -rf "${target_dir%/subdir}"
}
