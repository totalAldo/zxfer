#!/bin/sh
# Core execution, token, path, and destination-probe behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_exec_direct_load_does_not_require_transport_capability_or_snapshot_state() {
	/bin/sh -c '
		ZXFER_SOURCE_MODULES_ROOT=$1
		. "$1/src/zxfer_modules.sh"
		zxfer_load_modules zxfer_exec.sh || exit 1
		command -v zxfer_build_shell_command_from_argv >/dev/null 2>&1 || exit 2
		command -v zxfer_build_ssh_shell_command_for_host >/dev/null 2>&1 && exit 3
		command -v zxfer_ensure_remote_host_capabilities >/dev/null 2>&1 && exit 4
		command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1 && exit 5
		exit 0
	' zxfer-exec-direct-load "$ZXFER_ROOT"

	assertEquals "Generic exec should load without transport, capability, or snapshot modules." \
		0 "$?"
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

test_zxfer_compute_secure_path_uses_append_when_default_is_empty() {
	old_default_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	old_secure_path=${ZXFER_SECURE_PATH-}
	old_secure_path_append=${ZXFER_SECURE_PATH_APPEND-}
	outfile="$TEST_TMPDIR/secure_path_append_only.out"
	ZXFER_DEFAULT_SECURE_PATH=""
	ZXFER_SECURE_PATH=""
	ZXFER_SECURE_PATH_APPEND="/opt/trusted/bin"

	zxfer_compute_secure_path >"$outfile"

	ZXFER_DEFAULT_SECURE_PATH=$old_default_secure_path
	ZXFER_SECURE_PATH=$old_secure_path
	ZXFER_SECURE_PATH_APPEND=$old_secure_path_append

	assertEquals "Append-only secure-path configuration should still work when the built-in allowlist is empty." \
		"/opt/trusted/bin" "$(cat "$outfile")"
}

test_zxfer_compute_secure_path_falls_back_to_default_when_all_entries_are_filtered() {
	result=$(ZXFER_SECURE_PATH="relative:.:./bin" ZXFER_SECURE_PATH_APPEND="also-relative:./still-bad" zxfer_compute_secure_path)

	assertEquals "When every configured secure-PATH entry is filtered out, zxfer should fall back to the built-in allowlist." \
		"$ZXFER_DEFAULT_SECURE_PATH" "$result"
}

test_escape_for_single_quotes_escapes_apostrophes() {
	# Single-quoted contexts require reopening the quotes around apostrophes,
	# so ensure the helper inserts the standard '\''' sequence.
	input=$(printf "%s" "needs'single'quotes")
	expected=$(printf "%s" "needs'\\''single'\\''quotes")

	result=$(zxfer_escape_for_single_quotes "$input")

	assertEquals "Input should be properly escaped for single quotes." "$expected" "$result"
}

test_split_host_spec_tokens_handles_multi_word_hosts() {
	# Host specs may append privilege wrappers like "pfexec" or ssh options.
	result=$(zxfer_split_host_spec_tokens "user@host pfexec -p 2222")
	expected=$(printf "%s\n" "user@host" "pfexec" "-p" "2222")

	assertEquals "Host spec should be split into whitespace-delimited tokens." "$expected" "$result"
}

test_split_host_spec_tokens_rejects_shell_quotes_and_backslashes() {
	set +e
	output=$(zxfer_split_host_spec_tokens 'user@host "ZFS Admin"')
	status=$?
	set -e

	assertEquals "Host-spec tokenization should fail closed when the input requires shell quoting semantics." \
		1 "$status"
	assertContains "Rejected host specs should explain the literal-token requirement." \
		"$output" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_quote_host_spec_tokens_neutralizes_metacharacters() {
	# Ensure characters such as semicolons are quoted so they cannot escape
	# into new local commands when eval'd later.
	result=$(zxfer_quote_host_spec_tokens "backup.example.com; touch /tmp/pwn")
	expected="'backup.example.com;' 'touch' '/tmp/pwn'"

	assertEquals "Host spec should be rendered as safely quoted tokens." "$expected" "$result"
}

test_quote_cli_tokens_preserves_argument_boundaries() {
	# Compression commands should behave like arrays, preserving each argument.
	result=$(zxfer_quote_cli_tokens "zstd -3 --long=27")
	expected="'zstd' '-3' '--long=27'"

	assertEquals "CLI tokens should be individually quoted." "$expected" "$result"
}

test_split_cli_tokens_rejects_shell_quotes_and_backslashes() {
	set +e
	output=$(zxfer_split_cli_tokens 'zstd -T0\ -3' "compression command")
	status=$?
	set -e

	assertEquals "CLI tokenization should fail closed when the input relies on shell escaping." \
		1 "$status"
	assertContains "Rejected CLI command strings should explain the literal-token requirement." \
		"$output" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_quote_cli_tokens_blocks_shell_metacharacters() {
	# Metacharacters such as ';' or '|' must be neutralized instead of being
	# interpreted as new commands or pipelines.
	result=$(zxfer_quote_cli_tokens "zstd -3; touch /tmp/pwn | cat")
	expected="'zstd' '-3;' 'touch' '/tmp/pwn' '|' 'cat'"

	assertEquals "CLI tokens should remain literal even with metacharacters." "$expected" "$result"
}

test_quote_cli_tokens_preserves_validation_failures() {
	set +e
	output=$(zxfer_quote_cli_tokens '"/opt/zstd dir/zstd" -3' "compression command")
	status=$?
	set -e

	assertEquals "CLI quoting should fail closed when token validation rejects the input." \
		1 "$status"
	assertContains "CLI quoting should preserve the literal-token validation message." \
		"$output" "compression command must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_split_tokens_on_whitespace_breaks_metacharacters() {
	result=$(zxfer_split_tokens_on_whitespace "cmd;rm -rf|grep foo&echo done")
	expected=$(printf '%s\n' "cmd;" "rm" "-rf|" "grep" "foo&" "echo" "done")

	assertEquals "Tokenizer should break arguments on whitespace while leaving metacharacters literal." "$expected" "$result"
}

test_split_tokens_on_whitespace_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/split_tokens_empty.out"

	zxfer_split_tokens_on_whitespace "" >"$outfile"

	assertEquals "Blank token streams should produce no output." "" "$(cat "$outfile")"
}

test_quote_token_stream_preserves_each_token() {
	tokens=$(printf '%s\n' "alpha" "beta value" "" "gamma")
	result=$(zxfer_quote_token_stream "$tokens")
	expected="'alpha' 'beta value' 'gamma'"

	assertEquals "Token stream quoting should ignore blank lines and wrap each entry." "$expected" "$result"
}

test_quote_token_stream_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_token_stream_empty.out"

	zxfer_quote_token_stream "" >"$outfile"

	assertEquals "Blank token streams should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_host_spec_tokens_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_host_empty.out"

	zxfer_quote_host_spec_tokens "" >"$outfile"

	assertEquals "Blank host specs should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_cli_tokens_returns_empty_for_blank_input() {
	outfile="$TEST_TMPDIR/quote_cli_empty.out"

	zxfer_quote_cli_tokens "" >"$outfile"

	assertEquals "Blank CLI strings should remain blank after quoting." "" "$(cat "$outfile")"
}

test_quote_host_spec_tokens_returns_empty_when_splitter_yields_no_tokens() {
	zxfer_test_capture_subshell '
		zxfer_split_host_spec_tokens() {
			:
		}
		zxfer_quote_host_spec_tokens "backup.example"
	'

	assertEquals "Host-spec quoting should stay empty when tokenization succeeds but yields no tokens." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Empty host-token streams should not render placeholder quotes." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_quote_cli_tokens_returns_empty_when_splitter_yields_no_tokens() {
	zxfer_test_capture_subshell '
		zxfer_split_cli_tokens() {
			:
		}
		zxfer_quote_cli_tokens "zstd -3"
	'

	assertEquals "CLI quoting should stay empty when tokenization succeeds but yields no tokens." \
		0 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Empty CLI-token streams should not render placeholder quotes." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
}

test_zxfer_validate_ssh_option_value_rejects_control_whitespace() {
	invalid_value=$(printf 'bad\nvalue')

	set +e
	output=$(zxfer_validate_ssh_option_value "$invalid_value" "ZXFER_SSH_BATCH_MODE")
	status=$?

	assertEquals "SSH transport option values should reject control whitespace." 1 "$status"
	assertContains "Rejected ssh transport option values should explain the single-line requirement." \
		"$output" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_validate_ssh_option_path_preserves_single_line_validation_failure() {
	invalid_path=$(printf 'bad\npath')

	set +e
	output=$(zxfer_validate_ssh_option_path "$invalid_path" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE")
	status=$?

	assertEquals "SSH known-hosts path validation should fail closed on control-whitespace input." \
		1 "$status"
	assertContains "SSH known-hosts path validation should preserve the underlying single-line validation message." \
		"$output" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be a single-line non-empty value."
}

test_run_zfs_cmd_for_spec_routes_to_source_runner() {
	# shellcheck disable=SC2030,SC2031
	result=$(
		g_LZFS="/sbin/zfs"
		g_RZFS="/usr/sbin/zfs"
		zxfer_run_source_zfs_cmd() { printf 'source %s %s\n' "$1" "$2"; }
		zxfer_run_destination_zfs_cmd() { printf 'destination %s\n' "$1"; }
		zxfer_run_zfs_cmd_for_spec "/sbin/zfs" list tank/fs
	)

	assertEquals "Spec matching g_LZFS should call zxfer_run_source_zfs_cmd." "source list tank/fs" "$result"
}

test_run_zfs_cmd_for_spec_routes_to_destination_runner() {
	# shellcheck disable=SC2030,SC2031
	result=$(
		g_LZFS="/sbin/zfs"
		g_RZFS="/usr/sbin/zfs"
		zxfer_run_source_zfs_cmd() { printf 'source %s\n' "$1"; }
		zxfer_run_destination_zfs_cmd() { printf 'destination %s %s\n' "$1" "$2"; }
		zxfer_run_zfs_cmd_for_spec "/usr/sbin/zfs" get name tank/dst
	)

	assertEquals "Spec matching g_RZFS should call zxfer_run_destination_zfs_cmd." "destination get name" "$result"
}

test_run_zfs_cmd_for_spec_executes_literal_command_when_not_wrapped() {
	tool="$TEST_TMPDIR/echo_tool"
	cat >"$tool" <<'EOF'
#!/bin/sh
echo "$@"
EOF
	chmod +x "$tool"

	result=$(zxfer_run_zfs_cmd_for_spec "$tool" alpha beta)

	assertEquals "Arbitrary command specs should be executed directly." "alpha beta" "$result"
}

test_run_zfs_cmd_for_spec_tracks_other_profile_counter_when_very_verbose() {
	tool="$TEST_TMPDIR/profile_other_zfs"
	cat >"$tool" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$tool"

	g_option_V_very_verbose=1
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_get_calls=0

	zxfer_run_zfs_cmd_for_spec "$tool" get name tank/other >/dev/null

	assertEquals "Very-verbose profiling should count direct-spec zfs calls in the other bucket." \
		1 "$g_zxfer_profile_other_zfs_calls"
	assertEquals "Very-verbose profiling should still classify the direct-spec verb." \
		1 "$g_zxfer_profile_zfs_get_calls"
}

test_run_source_zfs_cmd_tracks_profile_counters_when_very_verbose() {
	tool="$TEST_TMPDIR/profile_source_zfs"
	cat >"$tool" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$tool"

	g_option_V_very_verbose=1
	g_zxfer_failure_stage="snapshot discovery"
	g_cmd_zfs="$tool"
	g_LZFS="$tool"
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_bucket_source_inspection=0

	zxfer_run_source_zfs_cmd list tank/src >/dev/null

	assertEquals "Very-verbose profiling should count source-side zfs calls." \
		1 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Very-verbose profiling should count list verbs separately." \
		1 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Snapshot discovery source calls should contribute to the source-inspection bucket." \
		1 "$g_zxfer_profile_bucket_source_inspection"
}

test_invoke_ssh_shell_command_for_host_tracks_profile_counters_when_very_verbose() {
	FAKE_SSH_LOG="$TEST_TMPDIR/ssh_profile.log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0

	zxfer_invoke_ssh_shell_command_for_host "origin.example" "'/bin/true'" >/dev/null \
		2>/dev/null

	unset FAKE_SSH_LOG

	assertEquals "Very-verbose profiling should count ssh shell invocations." \
		1 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Very-verbose profiling should attribute origin-host ssh invocations to the source side." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_invoke_ssh_shell_command_for_host_tracks_explicit_profile_side_when_origin_and_target_match() {
	FAKE_SSH_LOG="$TEST_TMPDIR/ssh_profile_same_host.log"
	export FAKE_SSH_LOG
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0

	zxfer_invoke_ssh_shell_command_for_host "shared.example" "'/bin/true'" source >/dev/null \
		2>/dev/null
	zxfer_invoke_ssh_shell_command_for_host "shared.example" "'/bin/true'" destination >/dev/null \
		2>/dev/null

	unset FAKE_SSH_LOG

	assertEquals "Explicit profile sides should still count total ssh invocations." \
		2 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Explicit source-side attribution should remain correct when origin and target share the same host spec." \
		1 "$g_zxfer_profile_source_ssh_shell_invocations"
	assertEquals "Explicit destination-side attribution should remain correct when origin and target share the same host spec." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
}

test_zxfer_profile_record_ssh_invocation_tracks_other_and_inferred_sides() {
	g_option_V_very_verbose=1
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0

	zxfer_profile_record_ssh_invocation "wrapper.example" other
	zxfer_profile_record_ssh_invocation "target.example"
	zxfer_profile_record_ssh_invocation "unknown.example"

	assertEquals "Explicit other-side attribution should still count toward total ssh invocations." \
		3 "$g_zxfer_profile_ssh_shell_invocations"
	assertEquals "Inferred destination-side attribution should count target-host ssh invocations." \
		1 "$g_zxfer_profile_destination_ssh_shell_invocations"
	assertEquals "Explicit other-side attribution and unknown hosts should both count toward the other-side ssh bucket." \
		2 "$g_zxfer_profile_other_ssh_shell_invocations"
	assertEquals "Origin-side attribution should remain unchanged when only other and destination paths are exercised." \
		0 "$g_zxfer_profile_source_ssh_shell_invocations"
}

test_zxfer_profile_record_zfs_call_tracks_remaining_verbs_and_buckets() {
	g_option_V_very_verbose=1
	g_zxfer_failure_stage=""
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0

	zxfer_profile_record_bucket destination_inspection
	zxfer_profile_record_bucket property_reconciliation

	g_zxfer_failure_stage="property transfer"
	zxfer_profile_record_zfs_call destination send

	g_zxfer_failure_stage="send/receive"
	zxfer_profile_record_zfs_call destination receive
	zxfer_profile_record_zfs_call destination list
	zxfer_profile_record_zfs_call source get

	assertEquals "Destination-side zfs calls should include send, receive, and list verbs." \
		3 "$g_zxfer_profile_destination_zfs_calls"
	assertEquals "Source-side zfs calls should include the source get verb." \
		1 "$g_zxfer_profile_source_zfs_calls"
	assertEquals "Send verbs should increment the send counter." \
		1 "$g_zxfer_profile_zfs_send_calls"
	assertEquals "Receive verbs should increment the receive counter." \
		1 "$g_zxfer_profile_zfs_receive_calls"
	assertEquals "List verbs should increment the list counter." \
		1 "$g_zxfer_profile_zfs_list_calls"
	assertEquals "Get verbs should increment the get counter." \
		1 "$g_zxfer_profile_zfs_get_calls"
	assertEquals "Destination-inspection bucket accounting should include the direct bucket hit and send/receive destination list probes." \
		2 "$g_zxfer_profile_bucket_destination_inspection"
	assertEquals "Property-reconciliation bucket accounting should include the direct hit and property-transfer send probe." \
		2 "$g_zxfer_profile_bucket_property_reconciliation"
	assertEquals "Send/receive setup bucket accounting should include receive-side send/receive probes." \
		1 "$g_zxfer_profile_bucket_send_receive_setup"
	assertEquals "Source-inspection bucket accounting should include source-side get probes during send/receive setup." \
		1 "$g_zxfer_profile_bucket_source_inspection"
}

test_get_backup_storage_dir_for_dataset_tree_derives_source_relative_layout() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"
	result=$(zxfer_get_backup_storage_dir_for_dataset_tree "tank/src/child")
	expected="$g_backup_storage_root/tank/src/child"
	slash_prefixed_result=$(zxfer_get_backup_storage_dir_for_dataset_tree "/tank/src/child/")

	assertEquals "Backup metadata storage should now derive only from the source dataset tree." \
		"$expected" "$result"
	assertEquals "Backup metadata storage should normalize slash-prefixed dataset inputs without introducing duplicate separators." \
		"$expected" "$slash_prefixed_result"
}

test_get_backup_storage_dir_for_dataset_tree_treats_rootlike_inputs_as_dataset_placeholder_in_current_shell() {
	g_backup_storage_root="$TEST_TMPDIR/backup_root"
	root_output="$TEST_TMPDIR/get_backup_storage_dir_root.out"
	blank_output="$TEST_TMPDIR/get_backup_storage_dir_blank.out"

	zxfer_get_backup_storage_dir_for_dataset_tree "/" >"$root_output"
	zxfer_get_backup_storage_dir_for_dataset_tree "" >"$blank_output"

	assertEquals "Rootlike dataset-tree lookups should collapse to the dataset placeholder bucket." \
		"$g_backup_storage_root/dataset" "$(cat "$root_output")"
	assertEquals "Blank dataset-tree lookups should also collapse to the dataset placeholder bucket." \
		"$g_backup_storage_root/dataset" "$(cat "$blank_output")"
}

test_zxfer_backup_metadata_file_key_fails_when_identity_hex_is_empty_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_metadata_file_key_current_shell.out"
	status_file="$TEST_TMPDIR/backup_metadata_file_key_current_shell.status"

	(
		od() {
			:
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
		printf '%s\n' "$?" >"$status_file"
	)

	assertEquals "Backup metadata keys should fail closed when the lossless identity hex cannot be derived." \
		1 "$(cat "$status_file")"
	assertEquals "Failed backup metadata key derivation should not emit a placeholder key." \
		"" "$(cat "$output_file")"
}

test_zxfer_backup_metadata_file_key_uses_identity_hex_output() {
	output_file="$TEST_TMPDIR/backup_metadata_file_key_od_hex.out"

	(
		od() {
			printf ' 61 62 63 64\n'
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
	)

	assertEquals "Backup metadata keys should use the exact od-derived identity hex." \
		"h/61626364" "$(cat "$output_file")"
}

test_zxfer_get_backup_metadata_filename_propagates_key_lookup_failures() {
	zxfer_test_capture_subshell '
		zxfer_backup_metadata_file_key() {
			return 1
		}
		zxfer_get_backup_metadata_filename "tank/src" "backup/dst"
	'

	assertEquals "Backup metadata filename generation should fail when the keyed suffix cannot be computed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertEquals "Failed backup metadata filename generation should not emit a partial filename." \
		"" "$ZXFER_TEST_CAPTURE_OUTPUT"
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

test_ensure_local_backup_dir_creates_secure_directory() {
	l_dir=$(cd -P "$TEST_TMPDIR" && pwd)/local_backup
	rm -rf "$l_dir"
	zxfer_ensure_local_backup_dir "$l_dir"
	assertTrue "Secure directory should be created." "[ -d '$l_dir' ]"
	perms=$(stat -c '%a' "$l_dir" 2>/dev/null || stat -f '%Lp' "$l_dir" 2>/dev/null)
	assertEquals "Backup directory must be chmod 700." "700" "$perms"
}

test_invoke_ssh_shell_command_for_host_emits_very_verbose_remote_prefix() {
	log_file="$TEST_TMPDIR/invoke_cmd_verbose.log"
	stderr_file="$TEST_TMPDIR/invoke_cmd_verbose.err"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup@example.com pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"

	zxfer_invoke_ssh_shell_command_for_host "backup@example.com pfexec" "zfs list -H tank/src" \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/origin.sock" "backup@example.com" \
		"'pfexec' zfs list -H tank/src")

	assertContains "Very-verbose ssh shell execution should prefix origin-host remote commands." \
		"$(cat "$stderr_file")" "Running remote command [origin: backup@example.com pfexec]:"
	assertContains "Very-verbose ssh shell execution should print the full rendered ssh command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_invoke_ssh_shell_command_for_host_skips_remote_render_when_quiet() {
	log_file="$TEST_TMPDIR/invoke_cmd_quiet.log"
	stderr_file="$TEST_TMPDIR/invoke_cmd_quiet.err"
	render_count_file="$TEST_TMPDIR/invoke_cmd_quiet.renders"
	: >"$log_file"
	printf '%s\n' 0 >"$render_count_file"

	(
		FAKE_SSH_LOG="$log_file"
		FAKE_SSH_SUPPRESS_STDOUT=1
		export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
		RENDER_COUNT_FILE="$render_count_file"
		zxfer_render_command_for_report() {
			printf '%s\n' 1 >>"$RENDER_COUNT_FILE"
			printf '%s\n' "rendered"
		}
		g_option_v_verbose=0
		g_option_V_very_verbose=0
		g_cmd_ssh="$FAKE_SSH_BIN"
		g_option_O_origin_host="backup@example.com"
		g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
		zxfer_invoke_ssh_shell_command_for_host "backup@example.com" "zfs list -H tank/src" \
			>/dev/null 2>"$stderr_file"
	)

	assertEquals "Quiet ssh shell execution should not render the remote command for display." \
		"0" "$(cat "$render_count_file")"
	assertEquals "Quiet ssh shell execution should emit no very-verbose output." \
		"" "$(cat "$stderr_file")"
	assertContains "Quiet ssh shell execution should still invoke the remote command." \
		"$(cat "$log_file")" "zfs list -H tank/src"
}

test_get_ssh_base_transport_tokens_preserves_local_ssh_resolution_failures() {
	set +e
	output=$(
		(
			zxfer_get_managed_ssh_option_tokens() {
				printf '%s\n' "-o\nBatchMode=yes"
			}
			zxfer_ensure_local_ssh_command() {
				g_zxfer_resolved_local_ssh_command_result="ssh lookup failed"
				return 1
			}
			zxfer_get_ssh_base_transport_tokens
		)
	)
	status=$?

	assertEquals "SSH base transport token discovery should fail when local ssh resolution fails." \
		1 "$status"
	assertEquals "SSH base transport token discovery should preserve the local ssh resolution diagnostic." \
		"ssh lookup failed" "$output"
}

test_invoke_ssh_shell_command_for_host_honors_explicit_ambient_policy_opt_out() {
	log_file="$TEST_TMPDIR/invoke_cmd_ambient.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="backup.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	ZXFER_SSH_USE_AMBIENT_CONFIG=1
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_shell_command_for_host "backup.example" "/bin/true"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' "-S" "$TEST_TMPDIR/origin.sock" "backup.example" "/bin/true")

	assertEquals "Ambient-policy opt-out should suppress zxfer-managed ssh -o options on the live invocation path while preserving control-socket reuse." \
		"$expected" "$(cat "$log_file")"
}

test_zxfer_get_managed_ssh_option_tokens_rejects_invalid_batch_mode() {
	zxfer_test_capture_subshell "
		ZXFER_SSH_BATCH_MODE=\$(printf 'bad\nmode')
		zxfer_get_managed_ssh_option_tokens
	"

	assertEquals "Managed ssh transport tokens should fail closed when ZXFER_SSH_BATCH_MODE is malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed ZXFER_SSH_BATCH_MODE values should preserve the specific validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_BATCH_MODE must be a single-line non-empty value."
}

test_zxfer_get_managed_ssh_option_tokens_rejects_invalid_strict_host_key_checking() {
	zxfer_test_capture_subshell "
		ZXFER_SSH_STRICT_HOST_KEY_CHECKING=\$(printf 'bad\npolicy')
		zxfer_get_managed_ssh_option_tokens
	"

	assertEquals "Managed ssh transport tokens should fail closed when ZXFER_SSH_STRICT_HOST_KEY_CHECKING is malformed." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "Malformed ZXFER_SSH_STRICT_HOST_KEY_CHECKING values should preserve the specific validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_STRICT_HOST_KEY_CHECKING must be a single-line non-empty value."
}

test_build_ssh_shell_command_for_host_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_build_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_build_ssh_shell_command_for_host_preserves_transport_token_status() {
	zxfer_test_capture_subshell "
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' 'custom transport failure'
			return 73
		}
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit \"\${2:-1}\"
		}
		zxfer_build_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should preserve the exact transport-token failure status." \
		73 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should preserve the transport-token failure text." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "custom transport failure"
}

test_ssh_host_spec_helpers_reject_invalid_literal_token_strings() {
	zxfer_test_capture_subshell "
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' '/usr/bin/ssh'
		}
		zxfer_build_ssh_shell_command_for_host 'backup.example \"pfexec -u zfs\"' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command rendering should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command rendering should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."

	zxfer_test_capture_subshell "
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_get_ssh_transport_tokens_for_host() {
			printf '%s\n' '/usr/bin/ssh'
		}
		zxfer_invoke_ssh_shell_command_for_host 'backup.example \"pfexec -u zfs\"' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command execution should reject host specs that rely on shell quoting." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command execution should preserve the host-spec literal-token validation message." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "Host spec (-O/-T) must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
}

test_invoke_ssh_shell_command_for_host_rethrows_transport_policy_validation_failures() {
	zxfer_test_capture_subshell "
		g_cmd_ssh='$FAKE_SSH_BIN'
		ZXFER_SSH_USER_KNOWN_HOSTS_FILE='relative-known-hosts'
		zxfer_throw_error() {
			printf '%s\n' \"\$1\"
			exit 1
		}
		zxfer_invoke_ssh_shell_command_for_host 'backup.example' \"'sh' '-c' 'printf ok'\"
	"

	assertEquals "ssh shell-command execution should fail closed when managed ssh policy validation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
	assertContains "ssh shell-command execution should rethrow the known-hosts validation failure." \
		"$ZXFER_TEST_CAPTURE_OUTPUT" "ZXFER_SSH_USER_KNOWN_HOSTS_FILE must be an absolute path."
}

test_invoke_ssh_shell_command_for_host_includes_explicit_known_hosts_override() {
	log_file="$TEST_TMPDIR/invoke_shell_known_hosts.log"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_cmd_ssh="$FAKE_SSH_BIN"
	ZXFER_SSH_USER_KNOWN_HOSTS_FILE="$TEST_TMPDIR/known_hosts"

	zxfer_invoke_ssh_shell_command_for_host "backup.example" "'sh' '-c' 'printf ok >/dev/null'"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected=$(printf '%s\n' \
		"-o" "BatchMode=yes" \
		"-o" "StrictHostKeyChecking=yes" \
		"-o" "UserKnownHostsFile=$TEST_TMPDIR/known_hosts" \
		"backup.example" "'sh' '-c' 'printf ok >/dev/null'")

	assertEquals "ssh shell-command execution should pass the explicit managed known-hosts override through the live argv path." \
		"$expected" "$(cat "$log_file")"
}

test_invoke_ssh_shell_command_for_host_emits_explicit_very_verbose_remote_prefix() {
	log_file="$TEST_TMPDIR/invoke_shell_verbose.log"
	stderr_file="$TEST_TMPDIR/invoke_shell_verbose.err"
	: >"$log_file"
	FAKE_SSH_LOG="$log_file"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	g_option_V_very_verbose=1
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	zxfer_invoke_ssh_shell_command_for_host \
		"shared.example" "'sh' '-c' 'printf ok >/dev/null'" destination \
		>/dev/null 2>"$stderr_file"

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	expected_verbose_command=$(zxfer_render_command_for_report "" \
		"$FAKE_SSH_BIN" "-o" "BatchMode=yes" "-o" "StrictHostKeyChecking=yes" \
		"-S" "$TEST_TMPDIR/target.sock" "shared.example" \
		"'sh' '-c' 'printf ok >/dev/null'")

	assertContains "Very-verbose ssh shell execution should honor the explicit target-side prefix when hosts match." \
		"$(cat "$stderr_file")" "Running remote command [target: shared.example]:"
	assertContains "Very-verbose ssh shell execution should print the full rendered ssh command." \
		"$(cat "$stderr_file")" "$expected_verbose_command"
}

test_run_source_zfs_cmd_uses_remote_ssh_when_origin_specified() {
	old_cmd_ssh=$g_cmd_ssh
	old_cmd_zfs=$g_cmd_zfs
	old_origin_cmd_zfs=${g_origin_cmd_zfs:-}
	old_origin_host=$g_option_O_origin_host

	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_origin_cmd_zfs="/usr/sbin/zfs"
	g_option_O_origin_host="backup@example.com pfexec -p 2222"
	g_ssh_origin_control_socket=""

	remote_log="$TEST_TMPDIR/zxfer_run_source_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_run_source_zfs_cmd list tank/fs@snap

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "ssh should force batch mode before connecting to the origin host." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the origin host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before connecting to the origin host." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the origin host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should target the origin host without literal quotes." "backup@example.com" "$(sed -n '5p' "$remote_log")"
	arg_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Privilege wrappers must remain quoted inside the remote shell command." "$arg_remote_cmd" "'pfexec' '-p' '2222'"
	assertContains "zfs binary should use the origin-host path." "$arg_remote_cmd" "'$g_origin_cmd_zfs'"
	assertContains "Remote command should preserve requested subcommand." "$arg_remote_cmd" "'list'"
	assertContains "Dataset argument should remain a single remote-shell token." "$arg_remote_cmd" "'tank/fs@snap'"

	g_cmd_ssh=$old_cmd_ssh
	g_cmd_zfs=$old_cmd_zfs
	g_origin_cmd_zfs=$old_origin_cmd_zfs
	g_option_O_origin_host=$old_origin_host
	g_option_O_origin_host_safe=""
}

test_run_source_zfs_cmd_uses_default_local_zfs_when_wrapper_is_unset() {
	fake_zfs="$TEST_TMPDIR/default_source_zfs"
	outfile="$TEST_TMPDIR/default_source_zfs.out"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$fake_zfs"
	g_option_O_origin_host=""
	g_cmd_zfs="$fake_zfs"
	g_LZFS="$fake_zfs"

	zxfer_run_source_zfs_cmd list -H tank/src >"$outfile"

	assertEquals "The default local source path should execute the resolved zfs binary directly." \
		"list -H tank/src" "$(cat "$outfile")"
	assertEquals "Default local source execution should redact the last command." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_run_destination_zfs_cmd_uses_remote_ssh_when_target_specified() {
	old_cmd_ssh=$g_cmd_ssh
	old_cmd_zfs=$g_cmd_zfs
	old_target_cmd_zfs=${g_target_cmd_zfs:-}
	old_target_host=$g_option_T_target_host

	g_cmd_ssh="$FAKE_SSH_BIN"
	g_cmd_zfs="/sbin/zfs"
	g_target_cmd_zfs="/usr/sbin/zfs"
	g_option_T_target_host="target@example.com doas"
	g_ssh_target_control_socket=""

	remote_log="$TEST_TMPDIR/zxfer_run_destination_zfs_cmd.log"
	: >"$remote_log"
	FAKE_SSH_LOG="$remote_log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	zxfer_run_destination_zfs_cmd get -H name tank/dst

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT
	assertEquals "ssh should force batch mode before connecting to the target host." "-o" "$(sed -n '1p' "$remote_log")"
	assertEquals "ssh should pass BatchMode=yes before the target host token." "BatchMode=yes" "$(sed -n '2p' "$remote_log")"
	assertEquals "ssh should force strict host-key checking before connecting to the target host." "-o" "$(sed -n '3p' "$remote_log")"
	assertEquals "ssh should pass StrictHostKeyChecking=yes before the target host token." "StrictHostKeyChecking=yes" "$(sed -n '4p' "$remote_log")"
	assertEquals "ssh should connect to the target host without stray quotes." "target@example.com" "$(sed -n '5p' "$remote_log")"
	targ_remote_cmd=$(sed -n '6p' "$remote_log")
	assertContains "Additional host-spec tokens must survive inside the remote shell command." "$targ_remote_cmd" "'doas'"
	assertContains "Remote call should include the target-host zfs path." "$targ_remote_cmd" "'$g_target_cmd_zfs'"
	assertContains "Command verb should pass through untouched." "$targ_remote_cmd" "'get'"
	assertContains "Original flags should be preserved." "$targ_remote_cmd" "'-H'"
	assertContains "Property argument should pass through verbatim." "$targ_remote_cmd" "'name'"
	assertContains "Dataset argument should remain literal." "$targ_remote_cmd" "'tank/dst'"

	g_cmd_ssh=$old_cmd_ssh
	g_cmd_zfs=$old_cmd_zfs
	g_target_cmd_zfs=$old_target_cmd_zfs
	g_option_T_target_host=$old_target_host
	g_option_T_target_host_safe=""
}

test_run_destination_zfs_cmd_uses_default_local_zfs_when_wrapper_is_unset() {
	fake_zfs="$TEST_TMPDIR/default_dest_zfs"
	outfile="$TEST_TMPDIR/default_dest_zfs.out"
	cat >"$fake_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$*"
EOF
	chmod +x "$fake_zfs"
	g_option_T_target_host=""
	g_cmd_zfs="$fake_zfs"
	g_RZFS="$fake_zfs"

	zxfer_run_destination_zfs_cmd get name tank/dst >"$outfile"

	assertEquals "The default local destination path should execute the resolved zfs binary directly." \
		"get name tank/dst" "$(cat "$outfile")"
	assertEquals "Default local destination execution should redact the last command." \
		"[redacted]" "$g_zxfer_failure_last_command"
}

test_refresh_compression_commands_tokenizes_custom_pipeline() {
	# When -Z supplies a custom command, ensure zxfer stores
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

	zxfer_refresh_compression_commands

	assertEquals "Compression command tokens should be quoted." "'zstd' '-3;' 'touch' '/tmp/pwn'" "$g_cmd_compress_safe"

	if [ "$old_g_cmd_compress_set" -eq 1 ]; then
		g_cmd_compress=$old_g_cmd_compress
	else
		unset g_cmd_compress
	fi
	if [ "$old_g_cmd_decompress_set" -eq 1 ]; then
		g_cmd_decompress=$old_g_cmd_decompress
	else
		unset g_cmd_decompress
	fi
	if [ "$old_g_option_z_compress_set" -eq 1 ]; then
		g_option_z_compress=$old_g_option_z_compress
	else
		unset g_option_z_compress
	fi
	if [ "$old_g_cmd_compress_safe_set" -eq 1 ]; then
		g_cmd_compress_safe=$old_g_cmd_compress_safe
	else
		unset g_cmd_compress_safe
	fi
	if [ "$old_g_cmd_decompress_safe_set" -eq 1 ]; then
		g_cmd_decompress_safe=$old_g_cmd_decompress_safe
	else
		unset g_cmd_decompress_safe
	fi
}

test_derive_override_lists_handles_overrides_only() {
	result=$(zxfer_derive_override_lists "compression=lz4=local" "compression=lzjb" 0 "filesystem")

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$result
EOF

	assertEquals "Override list should reflect -o values with override sources." "compression=lzjb=override" "$override_pvs"
	assertEquals "Creation list should keep explicit overrides for source-local properties." "compression=lzjb=override" "$creation_pvs"
}

test_derive_override_lists_includes_local_props_for_creation() {
	source_pvs="compression=lz4=local,refreservation=4G=received,quota=none=local"
	override_opts="quota=8G"
	result=$(zxfer_derive_override_lists "$source_pvs" "$override_opts" 1 "volume")

	{
		IFS= read -r override_pvs
		IFS= read -r creation_pvs
	} <<EOF
$result
EOF

	expected_override="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Overrides should include source properties with user overrides applied." "$(sort_property_list "$expected_override")" "$(sort_property_list "$override_pvs")"
	expected_creation="compression=lz4=local,quota=8G=override,refreservation=4G=received"
	assertEquals "Creation list should keep local props, explicit local overrides, and zvol refreservation even if not local." "$(sort_property_list "$expected_creation")" "$(sort_property_list "$creation_pvs")"
}

test_diff_properties_separates_set_and_inherit_lists() {
	override_pvs="compression=lz4=local,atime=off=received"
	dest_pvs="compression=lzjb=local,atime=on=local"
	result=$(zxfer_diff_properties "$override_pvs" "$dest_pvs" "casesensitivity,normalization,jailed,utf8only")

	{
		IFS= read -r initial_set_list
		IFS= read -r set_list
		IFS= read -r inherit_list
	} <<EOF
$result
EOF

	assertEquals "Initial pass should require setting every diverging property." "compression=lz4,atime=off" "$initial_set_list"
	assertEquals "Child dataset should only set properties sourced locally on the parent." "compression=lz4" "$set_list"
	assertEquals "Child dataset should inherit properties whose source is not local." "atime=off" "$inherit_list"
}

test_apply_property_changes_skips_inherit_for_initial_source() {
	FAKE_SET_CALLS=""
	FAKE_INHERIT_CALLS=""

	zxfer_apply_property_changes "pool/src" 1 "compression=lz4" "" "" fake_property_set_runner fake_property_inherit_runner

	assertEquals "Initial source should call the set runner once with the full initial diff list." "compression=lz4@pool/src;" "$FAKE_SET_CALLS"
	assertEquals "Initial source should not inherit properties." "" "$FAKE_INHERIT_CALLS"
}

test_apply_property_changes_invokes_inherit_runner_for_children() {
	FAKE_SET_CALLS=""
	FAKE_INHERIT_CALLS=""

	zxfer_apply_property_changes "pool/src" 0 "" "compression=lz4" "atime=off" fake_property_set_runner fake_property_inherit_runner

	assertEquals "Child dataset should apply the full child set list in one runner call." "compression=lz4@pool/src;" "$FAKE_SET_CALLS"
	assertEquals "Child dataset should inherit requested properties." "atime@pool/src;" "$FAKE_INHERIT_CALLS"
}

test_strip_trailing_slashes_trims_dataset_suffixes() {
	# Datasets may be provided with a trailing slash; ensure we drop all trailing
	# separators so concatenated child names never gain a double slash.
	result=$(zxfer_strip_trailing_slashes "pool/dst///")
	assertEquals "Trailing slashes should be removed." "pool/dst" "$result"

	result=$(zxfer_strip_trailing_slashes "pool/dst")
	assertEquals "Paths without trailing slashes should be unchanged." "pool/dst" "$result"
}

test_strip_trailing_slashes_preserves_absolute_placeholders() {
	# Absolute paths are rejected later, so inputs that consist entirely of
	# slashes must be passed through untouched.
	result=$(zxfer_strip_trailing_slashes "/")
	assertEquals "Single slash inputs should be preserved." "/" "$result"

	result=$(zxfer_strip_trailing_slashes "")
	assertEquals "Empty inputs should stay empty." "" "$result"
}

test_execute_command_respects_dry_run_mode() {
	# With --dry-run enabled, zxfer_execute_rendered_shell_command should not run but still
	# describe the action, so no temp files should be created.
	temp_file="$TEST_TMPDIR/dry_run_output"
	g_option_n_dryrun=1

	zxfer_execute_rendered_shell_command "printf 'should not run' > '$temp_file'"

	assertFalse "Dry run should skip running the command." "[ -f \"$temp_file\" ]"
}

test_execute_command_runs_command_when_not_dry_run() {
	# When --dry-run is off, the helper must execute commands verbatim.
	temp_file="$TEST_TMPDIR/run_output"

	zxfer_execute_rendered_shell_command "printf 'ran' > '$temp_file'"

	assertTrue "Command should run when dry run is disabled." "[ -f \"$temp_file\" ]"
	assertEquals "ran" "$(cat "$temp_file")"
}

test_execute_argv_command_preserves_argument_boundaries_without_shell_reparse() {
	marker="$TEST_TMPDIR/direct_argv_marker"
	zxfer_test_direct_argv_probe() {
		printf 'argc=%s\narg1=<%s>\narg2=<%s>\n' "$#" "$1" "$2"
	}

	output=$(zxfer_execute_argv_command 0 -- zxfer_test_direct_argv_probe \
		"value with spaces" "\$(touch $marker)")

	assertContains "Direct argv execution should preserve a spaced argument as one token." \
		"$output" "argc=2"
	assertContains "Direct argv execution should preserve argument contents literally." \
		"$output" "arg1=<value with spaces>"
	assertContains "Direct argv execution must not evaluate command substitutions in arguments." \
		"$output" "arg2=<\$(touch $marker)>"
	assertFalse "Direct argv execution must not run shell syntax embedded in data." \
		"[ -e '$marker' ]"
}

test_execute_argv_command_respects_dry_run_without_invoking_command() {
	marker="$TEST_TMPDIR/direct_argv_dry_run_marker"
	g_option_n_dryrun=1

	zxfer_execute_argv_command 0 -- touch "$marker"

	assertFalse "Direct argv execution should skip the command in dry-run mode." \
		"[ -e '$marker' ]"
}

test_execute_argv_command_continue_on_fail_reports_noncritical_error() {
	set +e
	output=$(zxfer_execute_argv_command 1 -- false)
	status=$?

	assertEquals "Continue-on-fail argv execution should return success after a command failure." \
		0 "$status"
	assertEquals "Continue-on-fail argv execution should report the non-critical failure." \
		"Non-critical error when executing command. Continuing." "$output"
}

test_get_temp_file_creates_unique_file() {
	# zxfer_get_temp_file should provide unique temp files so concurrent options do
	# not collide or overwrite each other.
	file_one=$(zxfer_get_temp_file)
	file_two=$(zxfer_get_temp_file)

	assertTrue "First temp file should exist." "[ -f \"$file_one\" ]"
	assertTrue "Second temp file should exist." "[ -f \"$file_two\" ]"
	assertNotEquals "Two consecutive temp file names should be unique." "$file_one" "$file_two"

	rm -f "$file_one" "$file_two"
}

test_get_temp_file_honors_tmpdir_variable() {
	# Honor the TMPDIR override so tests or CLI invocations can direct
	# scratch files to a specific filesystem, but use the validated
	# physical directory path rather than a logical symlinked alias.
	custom_tmp="$TEST_TMPDIR/custom"
	mkdir -p "$custom_tmp"
	physical_custom_tmp=$(cd -P "$custom_tmp" && pwd)
	TMPDIR="$custom_tmp"

	file=$(zxfer_get_temp_file)

	case "$file" in
	"$physical_custom_tmp"/*) inside=0 ;;
	*) inside=1 ;;
	esac

	assertEquals "Temp file should be created inside the validated TMPDIR root." 0 "$inside"
	assertTrue "Temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_uses_physical_tmpdir_for_symlinked_tmpdir_paths() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	real_tmp="$physical_tmpdir/tmp_real"
	link_tmp="$physical_tmpdir/tmp_link"
	mkdir -p "$real_tmp"
	ln -s "$real_tmp" "$link_tmp"
	TMPDIR="$link_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)

	case "$file" in
	"$real_tmp"/*) inside_real=0 ;;
	*) inside_real=1 ;;
	esac
	case "$file" in
	"$link_tmp"/*) inside_link=0 ;;
	*) inside_link=1 ;;
	esac

	assertEquals "Temp files should use the physical TMPDIR target instead of the symlinked path." 0 "$inside_real"
	assertEquals "Temp files should not be created through the symlinked TMPDIR path itself." 1 "$inside_link"
	assertTrue "Temp file should exist under the physical TMPDIR path." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_rejects_non_sticky_world_writable_tmpdir_and_falls_back_to_system_tmp() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	insecure_tmp="$physical_tmpdir/insecure_tmp"
	mkdir -p "$insecure_tmp"
	chmod 0777 "$insecure_tmp"
	TMPDIR="$insecure_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)
	status=$?

	assertEquals "Non-sticky world-writable TMPDIR values should not prevent temporary file creation." 0 "$status"
	case "$file" in
	"$insecure_tmp"/*) inside_insecure=0 ;;
	*) inside_insecure=1 ;;
	esac
	assertEquals "Non-sticky world-writable TMPDIR values should be rejected." 1 "$inside_insecure"
	assertTrue "Fallback temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	chmod 0700 "$insecure_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_allows_sticky_world_writable_tmpdir() {
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	sticky_tmp="$physical_tmpdir/sticky_tmp"
	mkdir -p "$sticky_tmp"
	chmod 1777 "$sticky_tmp"
	TMPDIR="$sticky_tmp"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)

	case "$file" in
	"$sticky_tmp"/*) inside_sticky=0 ;;
	*) inside_sticky=1 ;;
	esac

	assertEquals "Sticky world-writable TMPDIR values should remain usable." 0 "$inside_sticky"
	assertTrue "Sticky TMPDIR temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	chmod 0700 "$sticky_tmp"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_ignores_relative_tmpdir_and_falls_back_to_system_tmp() {
	old_pwd=$(pwd)
	physical_tmpdir=$(cd -P "$TEST_TMPDIR" && pwd)
	mkdir -p "$physical_tmpdir/relative_tmp_root"
	cd "$physical_tmpdir" || fail "Unable to cd into physical tempdir."
	TMPDIR="relative_tmp_root"
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""

	file=$(zxfer_get_temp_file)
	status=$?

	cd "$old_pwd" || fail "Unable to restore working directory."

	assertEquals "Relative TMPDIR values should not prevent temporary file creation." 0 "$status"
	case "$file" in
	"$physical_tmpdir"/relative_tmp_root/*) inside_relative=0 ;;
	*) inside_relative=1 ;;
	esac
	assertEquals "Relative TMPDIR values should be ignored instead of being used directly." 1 "$inside_relative"
	assertTrue "Fallback temp file should exist." "[ -f \"$file\" ]"

	rm -f "$file"
	TMPDIR="$TEST_TMPDIR"
}

test_get_temp_file_throws_when_mktemp_fails() {
	set +e
	output=$(
		(
			mktemp() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_get_temp_file
		)
	)
	status=$?

	assertEquals "Temporary-file allocation failures should abort." 1 "$status"
	assertContains "Temporary-file allocation failures should use the documented error." \
		"$output" "Error creating temporary file."
}

test_echov_outputs_only_when_verbose_enabled() {
	# zxfer_echov should emit text only when -v/--verbose is set.
	g_option_v_verbose=1
	output=$(zxfer_echov "verbose message")

	assertEquals "verbose message" "$output"

	g_option_v_verbose=0
	output=$(zxfer_echov "hidden message")

	assertEquals "" "$output"
}

test_echoV_outputs_only_when_very_verbose_enabled() {
	# zxfer_echoV uses -V/--very-verbose, so it should stay quiet unless the
	# highest verbosity level is requested.
	g_option_V_very_verbose=1
	output=$(zxfer_echoV "debug message" 2>&1)

	assertEquals "debug message" "$output"

	g_option_V_very_verbose=0
	output=$(zxfer_echoV "hidden debug" 2>&1)

	assertEquals "" "$output"
}

test_beep_skips_on_non_freebsd_hosts() {
	output=$(
		(
			uname() {
				printf '%s\n' "Linux"
			}
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "Non-FreeBSD hosts should skip beep handling with a debug message." \
		"$output" "Beep requested but unsupported on Linux; skipping."
}

test_beep_skips_when_speaker_device_is_missing() {
	output=$(
		(
			uname() {
				printf '%s\n' "FreeBSD"
			}
			kldstat() {
				printf '%s\n' "speaker.ko"
			}
			kldload() {
				return 0
			}
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without /dev/speaker should skip beep handling with a debug message." \
		"$output" "Beep requested but /dev/speaker missing; skipping."
}

test_beep_skips_when_speaker_tools_are_missing() {
	fake_bin_dir="$TEST_TMPDIR/no_speaker_tools"
	fake_uname_bin="$fake_bin_dir/uname"
	mkdir -p "$fake_bin_dir"
	cat >"$fake_uname_bin" <<'EOF'
#!/bin/sh
printf '%s\n' "FreeBSD"
EOF
	chmod +x "$fake_uname_bin"

	output=$(
		(
			PATH="$fake_bin_dir"
			g_option_b_beep_always=1
			g_option_V_very_verbose=1
			zxfer_beep 1
		) 2>&1
	)

	assertContains "FreeBSD hosts without speaker helper tools should skip beep handling with a debug message." \
		"$output" "Beep requested but speaker tools are missing; skipping."
}

test_execute_background_cmd_writes_output_file() {
	# Background commands are used for option pipelines; ensure their stdout
	# still lands in the provided tempfile.
	temp_file="$TEST_TMPDIR/bg_output"
	g_last_background_pid=""

	zxfer_execute_rendered_background_shell_command "printf bg-data" "$temp_file"
	bg_pid=$g_last_background_pid
	wait "$bg_pid"

	assertTrue "zxfer_execute_rendered_background_shell_command should expose the spawned PID for callers." \
		"[ -n \"$bg_pid\" ]"
	assertTrue "Background output file should be created." "[ -f \"$temp_file\" ]"
	assertEquals "bg-data" "$(cat "$temp_file")"
}

test_execute_background_cmd_fails_closed_when_cleanup_registration_fails() {
	temp_file="$TEST_TMPDIR/bg_register_fail_output"

	output=$(
		(
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_direct_child_pid() {
				printf 'abort:%s:%s:%s\n' "$1" "$2" "$3"
				kill -s TERM "$1" 2>/dev/null || :
				wait "$1" 2>/dev/null || :
				return 0
			}
			zxfer_execute_rendered_background_shell_command \
				"sleep 30" \
				"$temp_file"
			printf 'status=%s\n' "$?"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Background helper registration failures should fail closed." \
		"$output" "status=1"
	assertContains "Background helper registration failures should clear the published PID." \
		"$output" "pid="
	assertContains "Background helper registration failures should route teardown through the validated direct-child abort helper." \
		"$output" "abort:"
	assertContains "Background helper registration failures should preserve the cleanup-helper purpose when invoking the validated direct-child abort helper." \
		"$output" "background command helper"
}

test_execute_background_cmd_fails_closed_when_cleanup_wrapper_lookup_fails() {
	temp_file="$TEST_TMPDIR/bg_wrapper_lookup_fail_output"

	output=$(
		(
			zxfer_get_cleanup_child_wrapper_script_path() {
				printf '%s\n' "cleanup wrapper lookup failed"
				return 1
			}
			zxfer_execute_rendered_background_shell_command "sleep 30" "$temp_file"
			printf 'status=%s\n' "$?"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Background execution should fail closed when the cleanup-wrapper lookup fails." \
		"$output" "status=1"
	assertContains "Cleanup-wrapper lookup failures should not publish a background PID." \
		"$output" "pid="
}

test_execute_background_cmd_preserves_abort_failures_when_cleanup_registration_fails() {
	temp_file="$TEST_TMPDIR/bg_abort_fail_output"

	output=$(
		(
			zxfer_register_cleanup_pid() {
				return 1
			}
			zxfer_abort_direct_child_pid() {
				printf 'abort:%s:%s:%s\n' "$1" "$2" "$3"
				kill -s TERM "$1" 2>/dev/null || :
				wait "$1" 2>/dev/null || :
				return 1
			}
			zxfer_execute_rendered_background_shell_command \
				"sleep 30" \
				"$temp_file"
			printf 'status=%s\n' "$?"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)
	retained_pid=$(printf '%s\n' "$output" | sed -n 's/^pid=//p')

	assertContains "Background helper registration failures should preserve validated abort failures." \
		"$output" "status=1"
	assertNotNull "Background helper registration failures should retain the published direct-child handle when immediate abort fails." \
		"$retained_pid"
	assertContains "Background helper registration failures should still route teardown through the validated direct-child abort helper before returning the abort failure." \
		"$output" "abort:"
}

test_execute_background_cmd_respects_dry_run_mode() {
	temp_file="$TEST_TMPDIR/bg_dry_run_output"
	err_file="$TEST_TMPDIR/bg_dry_run_error"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			g_option_n_dryrun=1
			zxfer_execute_rendered_background_shell_command "printf bg-data" "$temp_file" "$err_file"
			printf 'pid=%s\n' "${g_last_background_pid:-}"
		)
	)

	assertContains "Dry-run background execution should render the skipped command." \
		"$output" "Dry run: printf bg-data"
	assertContains "Dry-run background execution should leave the background PID unset." \
		"$output" "pid="
	assertTrue "Dry-run background execution should still create the placeholder output file." \
		"[ -f \"$temp_file\" ]"
	assertTrue "Dry-run background execution should still create the placeholder error file." \
		"[ -f \"$err_file\" ]"
	assertEquals "Dry-run background execution should leave the placeholder output empty." \
		"" "$(cat "$temp_file")"
	assertEquals "Dry-run background execution should leave the placeholder error file empty." \
		"" "$(cat "$err_file")"
}

test_execute_background_cmd_dry_run_fails_closed_when_placeholder_creation_fails() {
	temp_dir="$TEST_TMPDIR/bg_dry_run_output_dir"
	err_dir="$TEST_TMPDIR/bg_dry_run_error_dir"
	mkdir -p "$temp_dir" "$err_dir"

	zxfer_test_capture_subshell '
		g_option_n_dryrun=1
		zxfer_execute_rendered_background_shell_command "printf bg-data" "'"$temp_dir"'" "'"$err_dir"'"
	'

	assertEquals "Dry-run background execution should return failure when placeholder file creation fails." \
		1 "$ZXFER_TEST_CAPTURE_STATUS"
}

test_execute_background_cmd_dry_run_clears_stale_pid_and_partial_output_on_second_placeholder_failure() {
	zxfer_get_temp_file >/dev/null
	output_file=$g_zxfer_temp_file_result
	err_dir="$TEST_TMPDIR/bg_dry_run_partial_error_dir"
	mkdir -p "$err_dir"

	output=$(
		(
			g_option_n_dryrun=1
			g_last_background_pid=43210
			zxfer_execute_rendered_background_shell_command "printf bg-data" "$output_file" "$err_dir" 2>/dev/null
			printf 'status=%s\n' "$?"
			printf 'pid=<%s>\n' "${g_last_background_pid:-}"
			if [ -e "$output_file" ]; then
				printf 'output_exists=yes\n'
			else
				printf 'output_exists=no\n'
			fi
		)
	)

	assertContains "Dry-run placeholder failures should preserve the write failure status." \
		"$output" "status=1"
	assertContains "Dry-run placeholder failures should clear any stale background PID state." \
		"$output" "pid=<>"
	assertContains "Dry-run placeholder failures should not leave a partially published output placeholder behind." \
		"$output" "output_exists=no"
}

test_exists_destination_returns_one_on_success() {
	# zxfer_exists_destination checks the remote ZFS command stored in g_RZFS;
	# when the check succeeds, the helper returns 1.
	# shellcheck disable=SC2031
	old_g_RZFS=${g_RZFS-}
	g_RZFS=true

	result=$(zxfer_exists_destination "pool/fs")

	assertEquals "Destination should exist when command succeeds." "1" "$result"

	g_RZFS=$old_g_RZFS
}

test_destination_parent_missing_confirmed_by_ancestor_listing_covers_listing_outcomes() {
	present_status=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "backup/dst/src"
				return 0
			}
			set +e
			zxfer_destination_parent_missing_confirmed_by_ancestor_listing "backup/dst/src"
			printf '%s\n' "$?"
		)
	)
	unrelated_status=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "unrelated/dataset"
				return 0
			}
			set +e
			zxfer_destination_parent_missing_confirmed_by_ancestor_listing "backup/dst/src"
			printf '%s\n' "$?"
		)
	)
	missing_output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'backup/dst': dataset does not exist" >&2
				return 1
			}
			set +e
			zxfer_destination_parent_missing_confirmed_by_ancestor_listing "backup/dst/src"
			printf 'status=%s\n' "$?"
			printf 'cached=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/src")"
		)
	)

	assertEquals "Ancestor listings that still contain the missing dataset should not confirm absence." \
		"1" "$present_status"
	assertEquals "Ancestor listings that lack both datasets should not confirm absence." \
		"1" "$unrelated_status"
	assertContains "Ancestor listings that report a missing ancestor should confirm absence." \
		"$missing_output" "status=0"
	assertContains "Confirmed missing parents should seed the destination existence cache as absent." \
		"$missing_output" "cached=0"
}

test_exists_destination_skips_probe_render_when_quiet() {
	render_count_file="$TEST_TMPDIR/exists_quiet.renders"
	printf '%s\n' 0 >"$render_count_file"

	result=$(
		(
			RENDER_COUNT_FILE="$render_count_file"
			zxfer_render_destination_zfs_command() {
				printf '%s\n' 1 >>"$RENDER_COUNT_FILE"
				printf '%s\n' "rendered"
			}
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			zxfer_exists_destination "pool/fs" live
		)
	)

	assertEquals "Quiet destination probes should still report existence." "1" "$result"
	assertEquals "Quiet destination probes should not render the probe command for display." \
		"0" "$(cat "$render_count_file")"
}

test_exists_destination_renders_probe_display_when_very_verbose() {
	stderr_file="$TEST_TMPDIR/exists_verbose.err"

	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			g_option_T_target_host=""
			g_cmd_zfs="/sbin/zfs"
			g_RZFS="/sbin/zfs"
			g_option_V_very_verbose=1
			zxfer_exists_destination "pool/fs" live 2>"$stderr_file"
		)
	)

	assertEquals "Very-verbose destination probes should still report existence." "1" "$result"
	assertEquals "Very-verbose destination probes should keep the current operator line text." \
		"Checking if destination exists: '/sbin/zfs' 'list' '-H' 'pool/fs'" \
		"$(cat "$stderr_file")"
}

test_exists_destination_returns_zero_when_dataset_is_missing() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'pool/fs': dataset does not exist" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Explicit missing-dataset errors should map to destination absent." 0 "$status"
	assertEquals "Missing destinations should still return 0." "0" "$result"
}

test_exists_destination_returns_zero_when_dataset_is_missing_with_stdout_only_error() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'pool/fs': dataset does not exist"
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Missing-dataset probes should still map to destination absent when the platform reports the error on stdout." 0 "$status"
	assertEquals "Stdout-only missing-dataset probes should still return 0." "0" "$result"
}

test_exists_destination_returns_zero_when_dataset_is_missing_with_omnios_error() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "cannot open 'pool/fs': no such pool or dataset" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "OmniOS-style missing-dataset probes should still map to destination absent." 0 "$status"
	assertEquals "OmniOS-style missing-dataset probes should still return 0." "0" "$result"
}

test_exists_destination_reports_probe_failures() {
	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "ssh: permission denied" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Operational probe failures should return non-zero." 1 "$status"
	assertContains "Operational probe failures should preserve the destination context." \
		"$output" "Failed to determine whether destination dataset [pool/fs] exists: ssh: permission denied"
}

test_exists_destination_reports_probe_failures_without_stderr() {
	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)
	status=$?

	assertEquals "Silent destination probe failures should still return non-zero." 1 "$status"
	assertContains "Silent destination probe failures should still report the destination dataset." \
		"$output" "Failed to determine whether destination dataset [pool/fs] exists."
}

test_exists_destination_uses_cached_exact_result_without_reprobing() {
	output=$(
		(
			zxfer_set_destination_existence_cache_entry "pool/fs" 1
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs"
		)
	)

	assertEquals "Exact cached destination results should be returned without another zfs probe." \
		"1" "$output"
}

test_exists_destination_infers_missing_descendants_from_seeded_tree() {
	output=$(
		(
			zxfer_seed_destination_existence_cache_from_recursive_list "backup/dst" "backup/dst
backup/dst/existing"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "backup/dst/missing"
		)
	)

	assertEquals "Datasets omitted from a seeded destination subtree should be treated as missing without another zfs probe." \
		"0" "$output"
}

test_exists_destination_live_bypasses_cache_and_refreshes_exact_entry() {
	output=$(
		(
			l_result_file="$TEST_TMPDIR/exists_destination_cache_refresh.out"
			zxfer_mark_destination_root_missing_in_cache "backup/dst"
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			zxfer_exists_destination "backup/dst/child" live >"$l_result_file"
			l_live_result=$(cat "$l_result_file")
			printf 'live=%s\n' "$l_live_result"
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "backup/dst/child" >"$l_result_file"
			l_cached_result=$(cat "$l_result_file")
			printf 'cached=%s\n' "$l_cached_result"
		)
	)

	assertContains "Live destination probes should bypass cached subtree-missing state." \
		"$output" "live=1"
	assertContains "Successful live probes should refresh the exact cache entry for later callers." \
		"$output" "cached=1"
}

test_exists_destination_uses_parent_recursive_listing_for_ambiguous_omnios_child_probes() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					printf '%s\n' "backup/dst/src/child"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Ambiguous OmniOS child probes should fall back to a parent recursive listing." 0 "$status"
	assertEquals "A parent recursive listing that contains the child should report that it exists." \
		"1" "$output"
}

test_exists_destination_uses_parent_recursive_listing_to_confirm_missing_omnios_child() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/src"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Ambiguous OmniOS child probes should still return successfully when the parent listing proves the child is missing." \
		0 "$status"
	assertEquals "A parent recursive listing that omits the child should report it missing." \
		"0" "$output"
}

test_exists_destination_reports_parent_recursive_listing_failures_for_ambiguous_omnios_child_probe() {
	set +e
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "permission denied" >&2
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Ambiguous OmniOS child probes should still fail closed when the parent recursive fallback errors." \
		1 "$status"
	assertContains "Fallback failures should preserve the child dataset context." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] failed: permission denied"
}

test_exists_destination_reports_parent_recursive_listing_without_parent_dataset_for_ambiguous_omnios_child_probe() {
	set +e
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "backup/dst/other"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "A parent recursive fallback that does not list the parent dataset should fail closed." \
		1 "$status"
	assertContains "The missing-parent fallback failure should identify the child and parent datasets." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] did not contain the parent dataset."
}

test_exists_destination_parent_recursive_listing_treats_missing_parent_as_missing_child_for_ambiguous_omnios_probe() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					printf '%s\n' "cannot open 'backup/dst/src': no such pool or dataset" >&2
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "A missing parent discovered through the recursive fallback should map to a missing child." \
		0 "$status"
	assertEquals "Missing-parent fallback should report the child as absent." \
		"0" "$output"
}

test_exists_destination_parent_recursive_listing_treats_silent_missing_parent_as_missing_child_when_ancestor_confirms_absence() {
	output=$(
		(
			g_destination_operating_system="SunOS"
			g_option_V_very_verbose=1
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst" ]; then
					printf '%s\n' "backup/dst"
					return 0
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "A silent SunOS parent-listing failure should map to missing only when an ancestor listing proves the parent is absent." \
		0 "$status"
	assertEquals "Confirmed silent missing-parent fallback should report the child as absent." \
		"0" "$output"
}

test_exists_destination_reports_silent_parent_recursive_listing_failures_for_ambiguous_omnios_child_probe() {
	set +e
	output=$(
		(
			g_destination_operating_system="SunOS"
			zxfer_run_destination_zfs_cmd() {
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "backup/dst/src/child" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst/src" ]; then
					return 1
				fi
				if [ "$1" = "list" ] && [ "$2" = "-H" ] && [ "$3" = "-r" ] &&
					[ "$4" = "-o" ] && [ "$5" = "name" ] && [ "$6" = "backup/dst" ]; then
					return 1
				fi
				printf '%s\n' "unexpected command: $*"
				return 1
			}
			zxfer_exists_destination "backup/dst/src/child" live
		)
	)
	status=$?

	assertEquals "Silent parent recursive fallback failures should still fail closed." 1 "$status"
	assertContains "Silent parent fallback failures should emit the dedicated recursive-listing error." \
		"$output" "Failed to determine whether destination dataset [backup/dst/src/child] exists: parent recursive listing for [backup/dst/src] failed."
}

test_exists_destination_cached_hits_do_not_increment_probe_counter() {
	output=$(
		(
			g_option_V_very_verbose=1
			g_zxfer_profile_exists_destination_calls=0
			zxfer_set_destination_existence_cache_entry "pool/fs" 1
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "probe should not run" >&2
				return 1
			}
			zxfer_exists_destination "pool/fs" >/dev/null
			printf 'cached_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
			zxfer_run_destination_zfs_cmd() {
				return 0
			}
			zxfer_exists_destination "pool/other" live >/dev/null
			printf 'live_calls=%s\n' "$g_zxfer_profile_exists_destination_calls"
		)
	)

	assertContains "Cached destination answers should not count as live destination probes." \
		"$output" "cached_calls=0"
	assertContains "Live destination probes should still increment the destination-probe profile counter." \
		"$output" "live_calls=1"
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

	secure_dir=$(zxfer_get_backup_storage_dir_for_dataset_tree "$g_initial_source")
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

	if [ -n "${old_g_RZFS-}" ]; then
		g_RZFS=$old_g_RZFS
	else
		unset g_RZFS
	fi
	unset FAKE_ZFS_MOUNTPOINT
	rm -f "$backup_file"
}
