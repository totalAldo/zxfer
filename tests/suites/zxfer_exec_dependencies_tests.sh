#!/bin/sh
# Tests for src/zxfer_dependencies.sh, run by tests/test_zxfer_exec.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_zxfer_compute_secure_path_filters_relative_entries() {
	result=$(
		ZXFER_SECURE_PATH="./bin:/tmp/bin:relative:/usr/sbin"
		ZXFER_SECURE_PATH_APPEND=""
		zxfer_compute_secure_path
		printf '%s\n' "$g_zxfer_computed_secure_path"
	)

	assertEquals "Relative path segments must be dropped from the secure PATH." "/tmp/bin:/usr/sbin" "$result"
}

test_zxfer_compute_secure_path_appends_extra_entries() {
	result=$(
		ZXFER_SECURE_PATH="/sbin:/bin"
		ZXFER_SECURE_PATH_APPEND=":/opt/zfs/bin:./malicious"
		zxfer_compute_secure_path
		printf '%s\n' "$g_zxfer_computed_secure_path"
	)

	assertEquals "ZXFER_SECURE_PATH_APPEND should only add absolute directories to the allowlist." "/sbin:/bin:/opt/zfs/bin" "$result"
}

test_zxfer_compute_secure_path_uses_append_when_default_is_empty() {
	result=$(
		ZXFER_DEFAULT_SECURE_PATH=""
		ZXFER_SECURE_PATH=""
		ZXFER_SECURE_PATH_APPEND="/opt/trusted/bin"
		zxfer_compute_secure_path
		printf '%s\n' "$g_zxfer_computed_secure_path"
	)

	assertEquals "Append-only secure-path configuration should still work when the built-in allowlist is empty." \
		"/opt/trusted/bin" "$result"
}

test_zxfer_compute_secure_path_falls_back_to_default_when_all_entries_are_filtered() {
	result=$(
		ZXFER_SECURE_PATH="relative:.:./bin"
		ZXFER_SECURE_PATH_APPEND="also-relative:./still-bad"
		zxfer_compute_secure_path
		printf '%s\n' "$g_zxfer_computed_secure_path"
	)

	assertEquals "When every configured secure-PATH entry is filtered out, zxfer should fall back to the built-in allowlist." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$result"
}

test_refresh_compression_commands_tokenizes_custom_pipeline() {
	# A -Z command is resolved and quoted token by token, so the shell never
	# runs the raw string.
	zstd_dir="$TEST_TMPDIR/custom_pipeline_bin"
	mkdir -p "$zstd_dir"
	printf '#!/bin/sh\nexit 0\n' >"$zstd_dir/zstd"
	chmod 755 "$zstd_dir/zstd"

	result=$(
		g_zxfer_secure_path=$zstd_dir
		g_option_z_compress=1
		g_cmd_compress="zstd -3;touch /tmp/pwn"
		g_cmd_decompress="zstd -d"
		zxfer_refresh_compression_commands
		printf '%s\n' "$g_cmd_compress_safe"
	)

	assertEquals "Compression command tokens should be quoted." \
		"'$zstd_dir/zstd' '-3;' 'touch' '/tmp/pwn'" "$result"
}
