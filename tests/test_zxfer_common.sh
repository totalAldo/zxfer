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

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_shunit.XXXXXX)
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
}

fake_zfs_mountpoint_cmd() {
	if [ "$1" = "get" ]; then
		printf '%s\n' "$FAKE_ZFS_MOUNTPOINT"
		return 0
	fi

	return 1
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
