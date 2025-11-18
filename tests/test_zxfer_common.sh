#!/bin/sh
#
# Basic shunit2 tests for zxfer_common.sh helpers.
#

# shellcheck source=tests/test_helper.sh
. "$(dirname "$0")/test_helper.sh"

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_inspect_delete_snap.sh
. "$ZXFER_ROOT/src/zxfer_inspect_delete_snap.sh"

create_fake_ssh_bin() {
	# Re-create the fake ssh helper after each cleanup so setUp() can freely
	# truncate the temp directory without leaving a stale interpreter. The helper
	# echoes both argv[0] and all arguments so tests can assert the full command
	# line.
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
printf '%s\n' "$0"
printf '%s\n' "$@"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_shunit.XXXXXX)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	# Reset the global option flags before each test so we always start from a
	# consistent CLI state, and isolate each test to its own temp directory.
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_backup_file_contents=""
	g_backup_storage_root="$TEST_TMPDIR/backup_store"
	TMPDIR="$TEST_TMPDIR"
	if [ -n "${TEST_TMPDIR:-}" ]; then
		rm -rf "${TEST_TMPDIR:?}/"*
	fi
	create_fake_ssh_bin
}

fake_zfs_mountpoint_cmd() {
	if [ "$1" = "get" ]; then
		printf '%s\n' "$FAKE_ZFS_MOUNTPOINT"
		return 0
	fi

	return 1
}

read_backup_file_with_mocked_security() {
	l_path=$1

	(
		get_path_owner_uid() { printf '%s\n' "0"; }
		get_path_mode_octal() { printf '%s\n' "600"; }
		read_local_backup_file "$l_path"
	)
}

test_zxfer_compute_secure_path_defaults_to_allowlist() {
	result=$(ZXFER_SECURE_PATH="" ZXFER_SECURE_PATH_APPEND="" zxfer_compute_secure_path)

	assertEquals "Default secure PATH should only include trusted system directories." "$ZXFER_DEFAULT_SECURE_PATH" "$result"
}

test_zxfer_compute_secure_path_filters_relative_entries() {
	result=$(ZXFER_SECURE_PATH="./bin:/tmp/bin:relative:/usr/sbin" ZXFER_SECURE_PATH_APPEND="" zxfer_compute_secure_path)

	assertEquals "Relative path segments must be dropped from the secure PATH." "/tmp/bin:/usr/sbin" "$result"
}

test_zxfer_compute_secure_path_appends_extra_entries() {
	result=$(ZXFER_SECURE_PATH="/sbin:/bin" ZXFER_SECURE_PATH_APPEND=":/opt/zfs/bin:./malicious" zxfer_compute_secure_path)

	assertEquals "ZXFER_SECURE_PATH_APPEND should only add absolute directories to the allowlist." "/sbin:/bin:/opt/zfs/bin" "$result"
}

test_escape_for_double_quotes_escapes_special_chars() {
	# Validate that the helper escapes characters which would break options
	# passed via the shell, such as quotes, backticks, and dollars.
	input=$(printf '%s' "text\"with\`special\$chars\\and normal")
	expected=$(printf '%s' "text\\\"with\\\`special\\\$chars\\\\and normal")

	result=$(escape_for_double_quotes "$input")

	assertEquals "Input should be properly escaped for double quotes." "$expected" "$result"
}

test_escape_for_single_quotes_escapes_apostrophes() {
	# Single-quoted contexts require reopening the quotes around apostrophes,
	# so ensure the helper inserts the standard '\''' sequence.
	input=$(printf "%s" "needs'single'quotes")
	expected=$(printf "%s" "needs'\\''single'\\''quotes")

	result=$(escape_for_single_quotes "$input")

	assertEquals "Input should be properly escaped for single quotes." "$expected" "$result"
}

test_split_host_spec_tokens_handles_multi_word_hosts() {
	# Host specs may append privilege wrappers like "pfexec" or ssh options.
	result=$(split_host_spec_tokens "user@host pfexec -p 2222")
	expected=$(printf "%s\n" "user@host" "pfexec" "-p" "2222")

	assertEquals "Host spec should be split into whitespace-delimited tokens." "$expected" "$result"
}

test_quote_host_spec_tokens_neutralizes_metacharacters() {
	# Ensure characters such as semicolons are quoted so they cannot escape
	# into new local commands when eval'd later.
	result=$(quote_host_spec_tokens "backup.example.com; touch /tmp/pwn")
	expected="'backup.example.com;' 'touch' '/tmp/pwn'"

	assertEquals "Host spec should be rendered as safely quoted tokens." "$expected" "$result"
}

test_quote_cli_tokens_preserves_argument_boundaries() {
	# Compression commands should behave like arrays, preserving each argument.
	result=$(quote_cli_tokens "zstd -3 --long=27")
	expected="'zstd' '-3' '--long=27'"

	assertEquals "CLI tokens should be individually quoted." "$expected" "$result"
}

test_quote_cli_tokens_blocks_shell_metacharacters() {
	# Metacharacters such as ';' or '|' must be neutralized instead of being
	# interpreted as new commands or pipelines.
	result=$(quote_cli_tokens "zstd -3; touch /tmp/pwn | cat")
	expected="'zstd' '-3;' 'touch' '/tmp/pwn' '|' 'cat'"

	assertEquals "CLI tokens should remain literal even with metacharacters." "$expected" "$result"
}

test_invoke_ssh_command_for_host_preserves_argument_boundaries() {
	fake_cmd="$FAKE_SSH_BIN -q -p 2222"
	host_spec="backup@example.com pfexec doas"
	result=$(invoke_ssh_command_for_host "$fake_cmd" "$host_spec" "--" "cmd arg" "with spaces" "umask 077; cat > /tmp/backup")
	expected=$(printf "%s\\n" "$FAKE_SSH_BIN" "-q" "-p" "2222" "backup@example.com" "pfexec" "doas" "--" "cmd arg" "with spaces" "umask 077; cat > /tmp/backup")

	assertEquals "ssh helper should keep multi-word host specs and remote commands intact." "$expected" "$result"
}

test_refresh_compression_commands_tokenizes_custom_pipeline() {
	# When -Z/ZXFER_COMPRESSION supplies a custom command, ensure zxfer stores
	# the quoted representation so eval never executes the raw string.
	if [ "${g_cmd_compress+x}" = x ]; then
		old_g_cmd_compress=$g_cmd_compress
		old_g_cmd_compress_set=1
	else
		old_g_cmd_compress_set=0
	fi
	if [ "${g_cmd_decompress+x}" = x ]; then
		old_g_cmd_decompress=$g_cmd_decompress
		old_g_cmd_decompress_set=1
	else
		old_g_cmd_decompress_set=0
	fi
	if [ "${g_cmd_compress_safe+x}" = x ]; then
		old_g_cmd_compress_safe=$g_cmd_compress_safe
		old_g_cmd_compress_safe_set=1
	else
		old_g_cmd_compress_safe_set=0
	fi
	if [ "${g_cmd_decompress_safe+x}" = x ]; then
		old_g_cmd_decompress_safe=$g_cmd_decompress_safe
		old_g_cmd_decompress_safe_set=1
	else
		old_g_cmd_decompress_safe_set=0
	fi
	if [ "${g_option_z_compress+x}" = x ]; then
		old_g_option_z_compress=$g_option_z_compress
		old_g_option_z_compress_set=1
	else
		old_g_option_z_compress_set=0
	fi

	g_cmd_compress="zstd -3;touch /tmp/pwn"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	g_option_z_compress=0

	refresh_compression_commands

	assertEquals "Compression command tokens should be quoted." "'zstd' '-3;' 'touch' '/tmp/pwn'" "$g_cmd_compress_safe"

	if [ $old_g_cmd_compress_set -eq 1 ]; then
		g_cmd_compress=$old_g_cmd_compress
	else
		unset g_cmd_compress
	fi
	if [ $old_g_cmd_decompress_set -eq 1 ]; then
		g_cmd_decompress=$old_g_cmd_decompress
	else
		unset g_cmd_decompress
	fi
	if [ $old_g_option_z_compress_set -eq 1 ]; then
		g_option_z_compress=$old_g_option_z_compress
	else
		unset g_option_z_compress
	fi
	if [ $old_g_cmd_compress_safe_set -eq 1 ]; then
		g_cmd_compress_safe=$old_g_cmd_compress_safe
	else
		unset g_cmd_compress_safe
	fi
	if [ $old_g_cmd_decompress_safe_set -eq 1 ]; then
		g_cmd_decompress_safe=$old_g_cmd_decompress_safe
	else
		unset g_cmd_decompress_safe
	fi
}

test_strip_trailing_slashes_trims_dataset_suffixes() {
	# Datasets may be provided with a trailing slash; ensure we drop all trailing
	# separators so concatenated child names never gain a double slash.
	result=$(strip_trailing_slashes "pool/dst///")
	assertEquals "Trailing slashes should be removed." "pool/dst" "$result"

	result=$(strip_trailing_slashes "pool/dst")
	assertEquals "Paths without trailing slashes should be unchanged." "pool/dst" "$result"
}

test_strip_trailing_slashes_preserves_absolute_placeholders() {
	# Absolute paths are rejected later, so inputs that consist entirely of
	# slashes must be passed through untouched.
	result=$(strip_trailing_slashes "/")
	assertEquals "Single slash inputs should be preserved." "/" "$result"

	result=$(strip_trailing_slashes "")
	assertEquals "Empty inputs should stay empty." "" "$result"
}

test_execute_command_respects_dry_run_mode() {
	# With --dry-run enabled, execute_command should not run but still
	# describe the action, so no temp files should be created.
	temp_file="$TEST_TMPDIR/dry_run_output"
	g_option_n_dryrun=1

	execute_command "printf 'should not run' > '$temp_file'"

	assertFalse "Dry run should skip running the command." "[ -f \"$temp_file\" ]"
}

test_execute_command_runs_command_when_not_dry_run() {
	# When --dry-run is off, the helper must execute commands verbatim.
	temp_file="$TEST_TMPDIR/run_output"

	execute_command "printf 'ran' > '$temp_file'"

	assertTrue "Command should run when dry run is disabled." "[ -f \"$temp_file\" ]"
	assertEquals "ran" "$(cat "$temp_file")"
}

test_get_temp_file_creates_unique_file() {
	# get_temp_file should provide unique temp files so concurrent options do
	# not collide or overwrite each other.
	file_one=$(get_temp_file)
	file_two=$(get_temp_file)

	assertTrue "First temp file should exist." "[ -f \"$file_one\" ]"
	assertTrue "Second temp file should exist." "[ -f \"$file_two\" ]"
	assertNotEquals "Two consecutive temp file names should be unique." "$file_one" "$file_two"

	rm -f "$file_one" "$file_two"
}

test_get_temp_file_honors_tmpdir_variable() {
	# Honor the TMPDIR override so tests or CLI invocations can direct
	# scratch files to a specific filesystem.
	custom_tmp="$TEST_TMPDIR/custom"
	mkdir -p "$custom_tmp"
	TMPDIR="$custom_tmp"

	file=$(get_temp_file)

	case "$file" in
	"$custom_tmp"/*) inside=0 ;;
	*) inside=1 ;;
	esac

	assertEquals "Temp file should be created inside TMPDIR." 0 "$inside"
	assertTrue "Temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_echov_outputs_only_when_verbose_enabled() {
	# echov should emit text only when -v/--verbose is set.
	g_option_v_verbose=1
	output=$(echov "verbose message")

	assertEquals "verbose message" "$output"

	g_option_v_verbose=0
	output=$(echov "hidden message")

	assertEquals "" "$output"
}

test_echoV_outputs_only_when_very_verbose_enabled() {
	# echoV uses -V/--very-verbose, so it should stay quiet unless the
	# highest verbosity level is requested.
	g_option_V_very_verbose=1
	output=$(echoV "debug message" 2>&1)

	assertEquals "debug message" "$output"

	g_option_V_very_verbose=0
	output=$(echoV "hidden debug" 2>&1)

	assertEquals "" "$output"
}

test_execute_background_cmd_writes_output_file() {
	# Background commands are used for option pipelines; ensure their stdout
	# still lands in the provided tempfile.
	temp_file="$TEST_TMPDIR/bg_output"

	execute_background_cmd "printf bg-data" "$temp_file"
	bg_pid=$!
	wait "$bg_pid"

	assertTrue "Background output file should be created." "[ -f \"$temp_file\" ]"
	assertEquals "bg-data" "$(cat "$temp_file")"
}

test_exists_destination_returns_one_on_success() {
	# exists_destination checks the remote ZFS command stored in g_RZFS;
	# when the check succeeds, the helper returns 1.
	old_g_RZFS=${g_RZFS-}
	g_RZFS=true

	result=$(exists_destination "pool/fs")

	assertEquals "Destination should exist when command succeeds." "1" "$result"

	g_RZFS=$old_g_RZFS
}

test_exists_destination_returns_zero_on_failure() {
	# When the remote ZFS check fails, the helper should return 0 so callers
	# can detect that the destination needs to be created.
	old_g_RZFS=${g_RZFS-}
	g_RZFS=false

	result=$(exists_destination "pool/fs")

	assertEquals "Destination should not exist when command fails." "0" "$result"

	g_RZFS=$old_g_RZFS
}

test_write_backup_properties_treats_backup_data_as_literal() {
	# Property backups must never interpret dataset-controlled data as shell
	# commands. Ensure values containing command substitutions are written
	# verbatim and do not execute locally.
	mount_dir="$TEST_TMPDIR/mnt"
	mkdir -p "$mount_dir"
	FAKE_ZFS_MOUNTPOINT="$mount_dir"
	old_g_RZFS=${g_RZFS-}
	g_RZFS=fake_zfs_mountpoint_cmd

	initial_source="pool/src"
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
	g_backup_file_contents=";$initial_source,$g_destination,user:note=\$(touch $sentinel_file)"

	write_backup_properties

	l_tail=${initial_source##*/}
	secure_dir=$(get_backup_storage_dir "$mount_dir" "$g_destination")
	backup_file="$secure_dir/$g_backup_file_extension.$l_tail"

	assertTrue "Backup property file should be written." "[ -f \"$backup_file\" ]"
	assertFalse "Backup file must not be written into dataset mountpoints." "[ -f \"$mount_dir/$g_backup_file_extension.$l_tail\" ]"
	assertFalse "Command substitutions within properties must not run." "[ -f \"$sentinel_file\" ]"

	backup_contents=$(cat "$backup_file")
	needle="\$(touch $sentinel_file)"
	case "$backup_contents" in
	*"$needle"*) found=0 ;;
	*) found=1 ;;
	esac
	assertEquals "Backup file should contain literal property data." 0 "$found"

	if [ -n "${old_g_RZFS-}" ]; then
		g_RZFS=$old_g_RZFS
	else
		unset g_RZFS
	fi
	unset FAKE_ZFS_MOUNTPOINT
	rm -f "$backup_file"
}

test_write_backup_properties_skips_when_no_data() {
	old_g_backup_storage_root=${g_backup_storage_root-}
	g_backup_storage_root="$TEST_TMPDIR/backup-skip"
	rm -rf "$g_backup_storage_root"

	initial_source="pool/src"
	g_destination="pool/dst"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive=""
	g_option_N_nonrecursive=""
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents=""

	write_backup_properties

	assertFalse "Backup metadata should not be written when no properties were collected." "[ -d \"$g_backup_storage_root\" ]"

	if [ -n "${old_g_backup_storage_root-}" ]; then
		g_backup_storage_root=$old_g_backup_storage_root
	else
		unset g_backup_storage_root
	fi
}

test_read_local_backup_file_refuses_non_root_owned_metadata() {
	# read_local_backup_file must refuse to parse metadata when the file is
	# not root-owned to prevent tampering from less-privileged users. Stub
	# the ownership/mode helpers so the test does not rely on the invoking
	# user's UID or default umask.
	backup_file="$TEST_TMPDIR/insecure_backup"
	printf '%s\n' "tampered" >"$backup_file"
	chmod 600 "$backup_file"

	if output=$(
		(
			get_path_owner_uid() { printf '%s\n' "1234"; }
			get_path_mode_octal() { printf '%s\n' "600"; }
			read_local_backup_file "$backup_file"
		) 2>&1
	); then
		status=0
	else
		status=$?
	fi

	assertEquals "Reading non-root metadata should exit with an error." 1 "$status"

	expected_owner_desc="root (UID 0)"
	if command -v id >/dev/null 2>&1; then
		if current_uid=$(id -u 2>/dev/null); then
			if [ "$current_uid" != "0" ]; then
				expected_owner_desc="$expected_owner_desc or UID $current_uid"
			fi
		fi
	fi

	case "$output" in
	*"Refusing to use backup metadata $backup_file because it is owned by UID 1234 instead of $expected_owner_desc."*) ;;
	*)
		fail "read_local_backup_file did not report an insecure owner: $output"
		;;
	esac

	rm -f "$backup_file"
}

test_read_local_backup_file_returns_contents_when_secure() {
	# When metadata ownership and permissions pass validation, the helper
	# should return the literal on-disk contents.
	backup_file="$TEST_TMPDIR/secure_backup"
	printf '%s\n' "trusted" >"$backup_file"

	result=$(read_backup_file_with_mocked_security "$backup_file")

	assertEquals "trusted" "$result"
	rm -f "$backup_file"
}

test_get_last_common_snapshot_matches_snapshot_name_only() {
	# Destination snapshot names use the destination dataset prefix, so the
	# helper must compare on the snapshot component rather than the full path.
	l_source_snaps="tank/doET/tank@zxfer_2
tank/doET/tank@zxfer_1"
	l_dest_snaps="tank/backups/nucbackup/tank/doET/tank@zxfer_1"

	result=$(get_last_common_snapshot "$l_source_snaps" "$l_dest_snaps")

	assertEquals "tank/doET/tank@zxfer_1" "$result"
}

test_get_last_common_snapshot_returns_empty_when_no_snapshot_match() {
	# If the destination never reported the snapshot name, the helper must
	# return an empty string so zxfer performs a full send.
	l_source_snaps="tank/doET/tank@zxfer_2
tank/doET/tank@zxfer_1"
	l_dest_snaps="tank/backups/nucbackup/tank/doET/tank@zxfer_3"

	result=$(get_last_common_snapshot "$l_source_snaps" "$l_dest_snaps")

	assertEquals "" "$result"
}

test_inspect_delete_snap_filters_exact_dataset_matches() {
	init_globals
	g_option_d_delete_destination_snapshots=0
	g_lzfs_list_hr_S_snap=$(
		cat <<'EOF'
tank/zfsbackup/doCGA/tank@zxfer_30473_20251114214157
tank/zfsbackup/doET/tank@zxfer_98767_20251117000001
tank/zfsbackup/doET/tank@zxfer_30473_20251114214157
EOF
	)
	g_rzfs_list_hr_snap=$(
		cat <<'EOF'
tank/backups/nucbackup/tank/zfsbackup/doCGA/tank@zxfer_30473_20251114214157
tank/backups/nucbackup/tank/zfsbackup/doET/tank@zxfer_30473_20251114214157
EOF
	)
	g_actual_dest="tank/backups/nucbackup/tank/zfsbackup/doET/tank"

	inspect_delete_snap 0 "tank/zfsbackup/doET/tank"

	assertEquals "tank/zfsbackup/doET/tank@zxfer_30473_20251114214157" "$g_last_common_snap"
}

. "$SHUNIT2_BIN"
