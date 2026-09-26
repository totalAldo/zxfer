#!/bin/sh
# Backup file tests for src/zxfer_backup_metadata.sh: the documented identity
# filename, literal writes, and remote reads through the resolved cat and the
# remote symlink guards. Run by tests/test_zxfer_backup_metadata.sh under the
# exec fixture.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

fake_zfs_mountpoint_cmd() {
	if [ "$1" = "get" ]; then
		printf '%s\n' "$FAKE_ZFS_MOUNTPOINT"
		return 0
	fi

	return 1
}

test_zxfer_get_backup_metadata_filename_fails_closed_when_identity_hex_pass_fails() {
	output_file="$TEST_TMPDIR/backup_metadata_filename_current_shell.out"
	status_file="$TEST_TMPDIR/backup_metadata_filename_current_shell.status"
	g_backup_file_extension=".zxfer_backup_info"

	(
		g_cmd_awk=false
		zxfer_get_backup_metadata_filename "tank/src" "backup/dst" >"$output_file"
		printf '%s\n' "$?" >"$status_file"
	)

	assertEquals "Backup metadata filenames should fail closed when the lossless identity hex cannot be derived." \
		1 "$(cat "$status_file")"
	assertEquals "Failed backup metadata filename derivation should not emit a partial filename." \
		"" "$(cat "$output_file")"
}

test_zxfer_get_backup_metadata_filename_renders_documented_identity_path() {
	g_backup_file_extension=".zxfer_backup_info"

	# The man page example: the hex of "tank/src\nbackup/dst" in 48-character
	# chunks under the fixed v2 leaf name.
	assertEquals "Backup metadata filenames must render the documented lossless identity path." \
		".zxfer_backup_info.v2/h/74616e6b2f7372630a6261636b75702f647374/.zxfer_backup_info.v2" \
		"$(zxfer_get_backup_metadata_filename "tank/src" "backup/dst")"
	# Retired writers keyed the name by cksum of "tank/src<LF>backup/dst"
	# with no final newline.
	assertEquals "The retired cksum-keyed filename stays readable for restore fallback." \
		".zxfer_backup_info.src.k1537737481.19" \
		"$(zxfer_get_backup_metadata_filename "tank/src" "backup/dst" legacy)"
}

test_write_backup_properties_treats_backup_data_as_literal() {
	# Property backups must never interpret dataset-controlled data as shell
	# commands. Ensure values containing command substitutions are written
	# verbatim and do not execute locally.
	mount_dir="$TEST_TMPDIR/mnt"
	mkdir -p "$mount_dir"
	FAKE_ZFS_MOUNTPOINT="$mount_dir"
	old_g_cmd_zfs=${g_cmd_zfs-}
	g_cmd_zfs=fake_zfs_mountpoint_cmd

	g_initial_source="pool/src"
	g_destination="pool/dst"
	g_actual_dest="$g_destination"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_T_target_host=""
	g_option_n_dryrun=0

	sentinel_file="$TEST_TMPDIR/sentinel_touch"
	rm -f "$sentinel_file"
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "user:note=\$(touch $sentinel_file)")

	zxfer_write_backup_properties

	secure_dir=$g_backup_storage_root/$g_initial_source
	backup_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination")
	backup_file="$secure_dir/$backup_name"

	assertTrue "Backup property file should be written." "[ -f \"$backup_file\" ]"
	assertFalse "Backup file must not be written into dataset mountpoints." "[ -f \"$mount_dir/$backup_name\" ]"
	assertFalse "Command substitutions within properties must not run." "[ -f \"$sentinel_file\" ]"

	backup_contents=$(cat "$backup_file")
	needle="\$(touch $sentinel_file)"
	case "$backup_contents" in
	*"$needle"*) found=0 ;;
	*) found=1 ;;
	esac
	assertEquals "Backup file should contain literal property data." 0 "$found"

	g_cmd_zfs=$old_g_cmd_zfs
	unset FAKE_ZFS_MOUNTPOINT
	rm -f "$backup_file"
}

test_write_backup_properties_skips_when_no_data() {
	old_g_backup_storage_root=${g_backup_storage_root-}
	g_backup_storage_root="$TEST_TMPDIR/backup-skip"
	rm -rf "$g_backup_storage_root"

	g_initial_source="pool/src"
	g_destination="pool/dst"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents=""

	zxfer_write_backup_properties

	assertFalse "Backup metadata should not be written when no properties were collected." "[ -d \"$g_backup_storage_root\" ]"

	if [ -n "${old_g_backup_storage_root-}" ]; then
		g_backup_storage_root=$old_g_backup_storage_root
	else
		unset g_backup_storage_root
	fi
}

test_read_remote_backup_file_uses_resolved_remote_cat_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_cat="/remote/bin/cat"
	remote_log="$TEST_TMPDIR/zxfer_read_remote_backup_file.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(
		(
			# Keep the csh-safe transport chunker out of the substring
			# assertions below: they inspect the rendered read program.
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s' "$1"
			}
			zxfer_read_remote_backup_file "backup@example.com pfexec" "/tmp/backup.meta"
		)
	)
	status=$?

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote backup reads should succeed when the ssh probe succeeds." 0 "$status"
	assertEquals "Remote backup reads should forward the remote payload." "payload" "$result"
	assertEquals "Remote backup reads should force batch mode before the host token." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "Remote backup reads should pass BatchMode=yes before the host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "Remote backup reads should force strict host-key checking before the host token." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "Remote backup reads should pass StrictHostKeyChecking=yes before the host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "Remote backup reads should keep the host token separate." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6,$p' "$remote_log")
	assertContains "Remote backup reads should keep wrapper tokens in the remote command string." "$log_line_remote_cmd" "'pfexec'"
	assertContains "Remote backup reads should use the resolved remote cat path." "$log_line_remote_cmd" "/remote/bin/cat"
	assertContains "Remote backup reads should validate the file mode before printing it." "$log_line_remote_cmd" "-rw-------*"
	assertContains "Remote backup reads should preserve the requested remote metadata path." "$log_line_remote_cmd" "/tmp/backup.meta"
}

test_read_remote_backup_file_accepts_ssh_user_owned_metadata() {
	realistic_ssh_bin="$TEST_TMPDIR/read_remote_backup_exec_ssh"
	remote_file="$TEST_TMPDIR_PHYSICAL/remote_backup.meta"
	printf '%s\n' "payload" >"$remote_file"
	chmod 600 "$remote_file"
	create_fake_ssh_join_exec_bin "$realistic_ssh_bin"
	g_cmd_ssh="$realistic_ssh_bin"
	g_cmd_cat="/bin/cat"

	result=$(zxfer_read_remote_backup_file "backup@example.com" "$remote_file")
	status=$?

	assertEquals "Remote backup reads should accept secure metadata owned by the remote ssh user." 0 "$status"
	assertEquals "Remote backup reads should pass through the payload for ssh-user-owned secure metadata." \
		"payload" "$result"
}

test_read_remote_backup_file_quotes_resolved_remote_cat_path() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	marker="$TEST_TMPDIR/read_remote_backup_marker"
	g_cmd_cat="/remote/bin/cat; touch $marker #"
	remote_log="$TEST_TMPDIR/read_remote_backup_quoted.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_STDOUT_OVERRIDE="payload"
	export FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	result=$(
		(
			# This case verifies helper-token quoting before transport; keep the
			# csh-safe transport chunker out of its string assertion.
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s' "$1"
			}
			zxfer_read_remote_backup_file "backup@example.com" "/tmp/backup.meta"
		)
	)
	status=$?

	unset FAKE_SSH_LOG FAKE_SSH_STDOUT_OVERRIDE

	assertEquals "Remote backup reads should still succeed when the resolved helper path contains metacharacters." 0 "$status"
	assertEquals "payload" "$result"
	assertFalse "Resolved remote cat paths should not execute locally when rendered into the remote shell helper." \
		"[ -e '$marker' ]"
	assertEquals "Remote backup reads should force batch mode before the host token." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "Remote backup reads should pass BatchMode=yes before the host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "Remote backup reads should force strict host-key checking before the host token." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "Remote backup reads should pass StrictHostKeyChecking=yes before the host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "Remote backup reads should keep the host token separate." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	log_line_remote_cmd=$(sed -n '6,$p' "$remote_log")
	assertContains "The resolved remote cat path should be quoted as one token in the remote helper script." \
		"$log_line_remote_cmd" "'/remote/bin/cat; touch $marker #'"
}

test_read_remote_backup_file_rejects_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/read_remote_backup_real"
	link_dir="$physical_tmpdir/read_remote_backup_link"
	backup_file="$link_dir/backup.meta"
	mkdir -p "$real_dir"
	printf '%s\n' "trusted" >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
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
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup reads should reject symlinked parent components before cat runs." 1 "$status"
	assertContains "Remote nested symlink reads should identify the offending path component." \
		"$output" "Refusing to use backup metadata $backup_file because path component $link_dir is a symlink."
}

test_read_remote_backup_file_rejects_root_owned_nested_symlink_components() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_dir="$physical_tmpdir/read_remote_backup_root_real"
	link_dir="$physical_tmpdir/read_remote_backup_root_link"
	backup_file="$link_dir/backup.meta"
	fake_bin="$physical_tmpdir/read_remote_backup_root_bin"
	mkdir -p "$real_dir" "$fake_bin"
	printf '%s\n' "trusted" >"$real_dir/backup.meta"
	chmod 600 "$real_dir/backup.meta"
	ln -s "$real_dir" "$link_dir"
	cat >"$fake_bin/stat" <<'EOF'
#!/bin/sh
case "$1 $2" in
	"-c %u"|"-f %u")
		printf '0\n'
		exit 0
		;;
	"-c %a"|"-f %OLp")
		printf '600\n'
		exit 0
		;;
esac
exit 1
EOF
	cat >"$fake_bin/ls" <<'EOF'
#!/bin/sh
for last_arg do :; done
	printf '%s\n' "-rw------- 1 0 0 0 Jan  1 00:00 $last_arg"
EOF
	cat >"$fake_bin/id" <<'EOF'
#!/bin/sh
if [ "${1-}" = "-u" ]; then
	printf '1000\n'
	exit 0
fi
exit 1
EOF
	chmod +x "$fake_bin/stat" "$fake_bin/ls" "$fake_bin/id"

	set +e
	output=$(
		(
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				PATH="$fake_bin:$PATH" sh -c "$2"
			}
			g_cmd_cat="/bin/cat"
			zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
		) 2>&1
	)
	status=$?

	assertEquals "Remote backup reads should reject nested symlink components even when remote ownership probes report a secure root-owned path." 1 "$status"
	assertContains "Root-owned nested symlink reads should still identify the offending path component." \
		"$output" "Refusing to use backup metadata $backup_file because path component $link_dir is a symlink."
}

test_read_remote_backup_file_allows_trusted_absolute_root_symlink_components() {
	backup_file=$(mktemp /tmp/read_remote_trusted.XXXXXX)
	outfile="$TEST_TMPDIR/read_remote_trusted.out"
	printf '%s\n' "backup-data" >"$backup_file"
	chmod 600 "$backup_file"
	g_cmd_cat="/bin/cat"

	(
		zxfer_build_remote_sh_c_command() {
			g_zxfer_remote_sh_c_command_result=$1
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			sh -c "$2"
		}
		zxfer_read_remote_backup_file "backup@example.com" "$backup_file"
	) >"$outfile"
	status=$?

	assertEquals "Trusted top-level system symlink components should not block remote backup reads, which keeps default /var- or /tmp-backed remote roots working on macOS." 0 "$status"
	assertEquals "Trusted absolute symlink components should still allow the secure metadata contents to be read." \
		"backup-data" "$(cat "$outfile")"

	rm -f "$backup_file"
}
