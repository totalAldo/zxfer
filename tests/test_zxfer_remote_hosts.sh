#!/bin/sh
#
# shunit2 tests for zxfer_remote_hosts.sh and related runtime helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_replication.sh"

tearDown() {
	if effective_uid=$(zxfer_get_effective_user_uid 2>/dev/null); then
		rm -rf "$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
		rm -rf "$TEST_TMPDIR/zxfer-s.$effective_uid.d"
		default_effective_tmpdir=$(
			unset TMPDIR
			zxfer_try_get_effective_tmpdir 2>/dev/null
		)
		default_socket_tmpdir=$(
			unset TMPDIR
			zxfer_try_get_socket_cache_tmpdir 2>/dev/null
		)
		if [ -n "$default_effective_tmpdir" ]; then
			rm -rf "$default_effective_tmpdir/zxfer-remote-capabilities.$effective_uid.d"
		fi
		if [ -n "$default_socket_tmpdir" ]; then
			rm -rf "$default_socket_tmpdir/zxfer-s.$effective_uid.d"
		fi
	fi
}

create_fake_ssh_bin() {
	cat >"$FAKE_SSH_BIN" <<'EOF'
#!/bin/sh
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$@" >>"$FAKE_SSH_LOG"
fi
exit "${FAKE_SSH_EXIT_STATUS:-0}"
EOF
	chmod +x "$FAKE_SSH_BIN"
}

fake_remote_capability_response() {
	cat <<'EOF'
ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
EOF
}

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_remote_hosts"
	TEST_TMPDIR_PHYSICAL=$(cd -P "$TEST_TMPDIR" && pwd)
	FAKE_SSH_BIN="$TEST_TMPDIR/fake_ssh"
	create_fake_ssh_bin
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	OPTIND=1
	unset FAKE_SSH_LOG
	unset FAKE_SSH_EXIT_STATUS
	unset ZXFER_BACKUP_DIR
	TMPDIR="$TEST_TMPDIR"
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_z_compress=0
	g_option_O_origin_host=""
	g_option_T_target_host=""
	g_option_g_grandfather_protection=""
	g_option_j_jobs=1
	g_option_m_migrate=0
	g_cmd_awk=${g_cmd_awk:-$(command -v awk 2>/dev/null || printf '%s\n' awk)}
	g_cmd_zfs="/sbin/zfs"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	g_ssh_supports_control_sockets=0
	g_test_max_yield_iterations=8
	g_zxfer_remote_capability_cache_wait_retries=5
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_secure_path=$ZXFER_DEFAULT_SECURE_PATH
	g_zxfer_dependency_path=$ZXFER_DEFAULT_SECURE_PATH
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	zxfer_get_max_yield_iterations() {
		printf '%s\n' "$g_test_max_yield_iterations"
	}
	zxfer_reset_snapshot_record_indexes
	zxfer_reset_failure_context "unit"
	create_fake_ssh_bin
}

test_zxfer_reset_snapshot_record_indexes_removes_directory_and_resets_state() {
	g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index-reset"
	mkdir -p "$g_zxfer_snapshot_index_dir"
	printf '%s\n' "stale" >"$g_zxfer_snapshot_index_dir/source.records"
	g_zxfer_snapshot_index_unavailable=1
	g_zxfer_source_snapshot_record_index="tank/src	$g_zxfer_snapshot_index_dir/source.records"
	g_zxfer_source_snapshot_record_index_ready=1
	g_zxfer_destination_snapshot_record_index="backup/dst	$g_zxfer_snapshot_index_dir/dest.records"
	g_zxfer_destination_snapshot_record_index_ready=1

	zxfer_reset_snapshot_record_indexes

	assertFalse "Resetting snapshot-record indexes should remove the backing temp directory." \
		"[ -d '$TEST_TMPDIR/snapshot-index-reset' ]"
	assertEquals "Resetting snapshot-record indexes should clear the unavailable flag." \
		0 "${g_zxfer_snapshot_index_unavailable:-1}"
	assertEquals "Resetting snapshot-record indexes should clear the source index map." \
		"" "${g_zxfer_source_snapshot_record_index:-}"
	assertEquals "Resetting snapshot-record indexes should clear the destination index map." \
		"" "${g_zxfer_destination_snapshot_record_index:-}"
	assertEquals "Resetting snapshot-record indexes should clear the source ready flag." \
		0 "${g_zxfer_source_snapshot_record_index_ready:-1}"
	assertEquals "Resetting snapshot-record indexes should clear the destination ready flag." \
		0 "${g_zxfer_destination_snapshot_record_index_ready:-1}"
}

test_zxfer_ensure_snapshot_index_dir_handles_unavailable_and_mktemp_failures() {
	set +e
	output=$(
		(
			g_zxfer_snapshot_index_unavailable=1
			zxfer_ensure_snapshot_index_dir
		)
	)
	status=$?
	assertEquals "Snapshot-index dir creation should short-circuit once the cache is marked unavailable." 1 "$status"
	assertEquals "Unavailable snapshot-index cache creation should not produce a payload." "" "$output"

	set +e
	output=$(
		(
			mktemp() {
				return 1
			}
			zxfer_ensure_snapshot_index_dir || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				printf 'dir=%s\n' "${g_zxfer_snapshot_index_dir:-}"
				return 1
			}
		)
	)
	status=$?

	assertEquals "Snapshot-index dir creation should fail cleanly when mktemp fails." 1 "$status"
	assertContains "mktemp failures should mark the snapshot-index cache unavailable." \
		"$output" "unavailable=1"
	assertContains "mktemp failures should leave the snapshot-index dir unset." \
		"$output" "dir="
}

test_zxfer_ensure_snapshot_index_dir_marks_cache_unavailable_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				return 1
			}
			zxfer_ensure_snapshot_index_dir || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				printf 'dir=%s\n' "${g_zxfer_snapshot_index_dir:-}"
				return 1
			}
		)
	)
	status=$?

	assertEquals "Snapshot-index dir creation should fail cleanly when effective temp-root lookup fails." \
		1 "$status"
	assertContains "Effective temp-root lookup failures should mark the snapshot-index cache unavailable." \
		"$output" "unavailable=1"
	assertContains "Effective temp-root lookup failures should leave the snapshot-index dir unset." \
		"$output" "dir="
}

test_zxfer_ensure_snapshot_index_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR_PHYSICAL/snapshot_index_effective_root"
	mkdir -p "$cache_root"

	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_snapshot_index_dir || exit $?
			printf 'dir=%s\n' "$g_zxfer_snapshot_index_dir"
		)
	)
	status=$?

	assertEquals "Snapshot-index dir creation should succeed when the validated effective temp root is available." \
		0 "$status"
	assertContains "Snapshot-index dirs should be created under the effective temp root instead of raw TMPDIR." \
		"$output" "dir=$cache_root/zxfer-snapshot-index."
}

test_zxfer_build_snapshot_record_index_handles_invalid_side_and_failures() {
	set +e
	output=$(
		(
			zxfer_build_snapshot_record_index other "tank/src@snap1"
		)
	)
	status=$?
	assertEquals "Snapshot-record index builds should reject unknown sides." 1 "$status"
	assertEquals "Rejected snapshot-record index builds should not produce a payload." "" "$output"

	set +e
	output=$(
		(
			g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index-build"
			mkdir -p "$g_zxfer_snapshot_index_dir"
			mkdir() {
				return 1
			}
			zxfer_build_snapshot_record_index source "tank/src@snap1" || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				return 1
			}
		)
	)
	status=$?
	assertEquals "Snapshot-record index builds should fail cleanly when their side directory cannot be created." \
		1 "$status"
	assertContains "Side-directory creation failures should mark the snapshot index unavailable." \
		"$output" "unavailable=1"

	set +e
	output=$(
		(
			g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index-awk"
			mkdir -p "$g_zxfer_snapshot_index_dir"
			g_cmd_awk=false
			zxfer_build_snapshot_record_index destination "backup/dst@snap1" || {
				printf 'unavailable=%s\n' "$g_zxfer_snapshot_index_unavailable"
				return 1
			}
		)
	)
	status=$?
	assertEquals "Snapshot-record index builds should fail cleanly when awk cannot build the index." \
		1 "$status"
	assertContains "Awk build failures should mark the snapshot index unavailable." \
		"$output" "unavailable=1"
}

test_zxfer_build_snapshot_record_index_returns_failure_when_index_dir_setup_fails_in_current_shell() {
	set +e
	(
		zxfer_ensure_snapshot_index_dir() {
			return 1
		}
		zxfer_build_snapshot_record_index source "tank/src@snap1"
	)
	status=$?

	assertEquals "Snapshot-record index builds should fail when the index directory cannot be prepared." \
		1 "$status"
}

test_zxfer_note_destination_dataset_exists_appends_new_children_in_current_shell() {
	g_recursive_dest_list="backup/dst"

	zxfer_note_destination_dataset_exists "backup/dst/child"

	assertEquals "New destination datasets should be appended as exact newline-delimited entries." \
		"backup/dst
backup/dst/child" "$g_recursive_dest_list"
}

test_zxfer_note_destination_dataset_exists_sets_first_entry_when_list_is_empty() {
	g_recursive_dest_list=""

	zxfer_note_destination_dataset_exists "backup/dst"

	assertEquals "The first observed destination dataset should seed the recursive destination list directly." \
		"backup/dst" "$g_recursive_dest_list"
}

test_zxfer_get_snapshot_record_helpers_handle_missing_files_and_invalid_sides() {
	g_zxfer_source_snapshot_record_index="tank/src	$TEST_TMPDIR/missing-source-index"
	g_zxfer_source_snapshot_record_index_ready=1

	set +e
	output=$(zxfer_get_indexed_snapshot_records_for_dataset source "tank/src")
	status=$?
	assertEquals "Indexed snapshot lookups should fail when the recorded cache file is missing." 1 "$status"
	assertEquals "Missing snapshot-index files should not produce a payload." "" "$output"

	set +e
	output=$(zxfer_get_indexed_snapshot_records_for_dataset other "tank/src")
	status=$?
	assertEquals "Indexed snapshot lookups should reject unknown sides." 1 "$status"
	assertEquals "Rejected indexed snapshot lookups should not produce a payload." "" "$output"

	g_zxfer_destination_snapshot_record_index_ready=1
	g_zxfer_destination_snapshot_record_index=""
	output=$(zxfer_get_indexed_snapshot_records_for_dataset destination "backup/dst")
	status=$?
	assertEquals "Ready snapshot indexes should return success with empty output when the dataset is absent from the index." \
		0 "$status"
	assertEquals "Absent indexed datasets should yield an empty payload." "" "$output"

	set +e
	output=$(zxfer_get_snapshot_records_for_dataset other "tank/src")
	status=$?
	assertEquals "Snapshot-record retrieval should reject unknown sides even after index fallback." 1 "$status"
	assertEquals "Rejected snapshot-record retrieval should not produce a payload." "" "$output"
}

test_zxfer_get_snapshot_records_for_dataset_lazily_builds_snapshot_indexes() {
	output=$(
		(
			source_root_file="$TEST_TMPDIR/lazy_source_root.records"
			dest_root_file="$TEST_TMPDIR/lazy_dest_root.records"
			g_lzfs_list_hr_snap=$(printf '%s\n%s\n%s' \
				"tank/src@snap1" \
				"tank/src/child@child1" \
				"tank/src@snap2")
			g_rzfs_list_hr_snap=$(printf '%s\n%s\n%s' \
				"backup/dst@snap2" \
				"backup/dst@legacy1" \
				"backup/dst/child@child1")
			printf 'source_ready_before=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready_before=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
			printf 'source_reversed_before=%s\n' "${g_lzfs_list_hr_S_snap:-}"
			zxfer_get_snapshot_records_for_dataset source "tank/src" >"$source_root_file"
			zxfer_get_snapshot_records_for_dataset destination "backup/dst" >"$dest_root_file"
			printf 'source_root=%s\n' "$(cat "$source_root_file")"
			printf 'dest_root=%s\n' "$(cat "$dest_root_file")"
			printf 'source_ready_after=%s\n' "${g_zxfer_source_snapshot_record_index_ready:-0}"
			printf 'dest_ready_after=%s\n' "${g_zxfer_destination_snapshot_record_index_ready:-0}"
			printf 'source_reversed_after=%s\n' "${g_lzfs_list_hr_S_snap:-}"
		)
	)

	assertContains "Lazy snapshot indexing should leave the source index unset until a lookup occurs." \
		"$output" "source_ready_before=0"
	assertContains "Lazy snapshot indexing should leave the destination index unset until a lookup occurs." \
		"$output" "dest_ready_before=0"
	assertContains "Lazy source indexing should not precompute the reversed source cache." \
		"$output" "source_reversed_before="
	assertContains "Lazy source indexing should still return newest-first source records once requested." \
		"$output" "source_root=tank/src@snap2
tank/src@snap1"
	assertContains "Lazy destination indexing should still return the live destination records once requested." \
		"$output" "dest_root=backup/dst@snap2
backup/dst@legacy1"
	assertContains "Lazy snapshot indexing should mark the source index ready after the first lookup." \
		"$output" "source_ready_after=1"
	assertContains "Lazy snapshot indexing should mark the destination index ready after the first lookup." \
		"$output" "dest_ready_after=1"
	assertContains "Lazy source indexing should populate the reversed cache only after a consumer requests source records." \
		"$output" "source_reversed_after=tank/src@snap2
tank/src/child@child1
tank/src@snap1"
}

test_zxfer_get_snapshot_records_for_dataset_falls_back_to_cached_source_records_when_lazy_index_build_fails() {
	output=$(
		(
			g_lzfs_list_hr_snap=$(printf '%s\n%s' \
				"tank/src@snap1" \
				"tank/src@snap2")
			zxfer_build_snapshot_record_index() {
				return 1
			}
			zxfer_get_snapshot_records_for_dataset source "tank/src"
		)
	)

	assertEquals "When lazy source index creation fails, source snapshot-record lookup should still fall back to the reversed in-memory cache." \
		"tank/src@snap2
tank/src@snap1" "$output"
}

test_zxfer_parse_remote_capability_response_extracts_fields() {
	result=$(
		(
			zxfer_parse_remote_capability_response "$(fake_remote_capability_response)"
			printf 'os=%s\n' "$g_zxfer_remote_capability_os"
			printf 'zfs=%s:%s\n' "$g_zxfer_remote_capability_zfs_status" "$g_zxfer_remote_capability_zfs_path"
			printf 'parallel=%s:%s\n' "$g_zxfer_remote_capability_parallel_status" "$g_zxfer_remote_capability_parallel_path"
			printf 'cat=%s:%s\n' "$g_zxfer_remote_capability_cat_status" "$g_zxfer_remote_capability_cat_path"
		)
	)

	assertContains "The parser should extract the remote operating system." "$result" "os=RemoteOS"
	assertContains "The parser should extract the remote zfs helper path." "$result" "zfs=0:/remote/bin/zfs"
	assertContains "The parser should extract the remote GNU parallel helper path." "$result" "parallel=0:/opt/bin/parallel"
	assertContains "The parser should extract the remote cat helper path." "$result" "cat=0:/remote/bin/cat"
}

test_zxfer_parse_remote_capability_response_clears_optional_paths_for_missing_tools() {
	result=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	parallel	1	-
tool	cat	1	-"
			printf 'parallel=%s:%s\n' "$g_zxfer_remote_capability_parallel_status" "$g_zxfer_remote_capability_parallel_path"
			printf 'cat=%s:%s\n' "$g_zxfer_remote_capability_cat_status" "$g_zxfer_remote_capability_cat_path"
		)
	)

	assertContains "The parser should preserve missing GNU parallel status codes." \
		"$result" "parallel=1:"
	assertContains "The parser should clear the GNU parallel path when the tool is missing." \
		"$result" "parallel=1:"
	assertContains "The parser should preserve missing cat status codes." \
		"$result" "cat=1:"
	assertContains "The parser should clear the cat path when the tool is missing." \
		"$result" "cat=1:"
}

test_zxfer_parse_remote_capability_response_rejects_malformed_records() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	oops	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Malformed capability records should be rejected." 1 "$status"
	assertEquals "Malformed capability records should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_missing_os_payload() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os
tool	zfs	0	/remote/bin/zfs
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Capability records without an OS payload should be rejected." 1 "$status"
	assertEquals "Capability records without an OS payload should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_unknown_tool_entries() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	0	/remote/bin/zfs
tool	weirdtool	0	/remote/bin/weirdtool
tool	cat	0	/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Capability records with unexpected tool names should be rejected." 1 "$status"
	assertEquals "Capability records with unexpected tool names should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_extra_lines() {
	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "$(fake_remote_capability_response)
extra	line"
		)
	)
	status=$?

	assertEquals "Capability records with extra lines should be rejected." 1 "$status"
	assertEquals "Capability records with extra lines should not print a parsed payload." "" "$output"
}

test_zxfer_parse_remote_capability_response_rejects_control_whitespace_helper_paths() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	set +e
	output=$(
		(
			zxfer_parse_remote_capability_response "ZXFER_REMOTE_CAPS_V1
os${tab}RemoteOS
tool${tab}zfs${tab}0${tab}/remote/bin/zfs${cr}
tool${tab}parallel${tab}0${tab}/opt/bin/parallel
tool${tab}cat${tab}0${tab}/remote/bin/cat"
		)
	)
	status=$?

	assertEquals "Capability payloads with control-whitespace helper paths should be rejected as invalid handshakes." \
		1 "$status"
	assertEquals "Rejected control-whitespace capability payloads should not print a parsed payload." "" "$output"
}

test_zxfer_store_cached_remote_capability_response_for_host_updates_target_slot() {
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"

	zxfer_store_cached_remote_capability_response_for_host "target.example" "$(fake_remote_capability_response)"

	assertEquals "Target-side host caching should update the target cache slot." \
		"target.example" "$g_target_remote_capabilities_host"
	assertContains "Target-side host caching should store the capability payload." \
		"$g_target_remote_capabilities_response" "tool	cat	0	/remote/bin/cat"
}

test_zxfer_get_cached_remote_capability_response_for_host_reads_target_slot() {
	g_target_remote_capabilities_host="target.example"
	g_target_remote_capabilities_response=$(fake_remote_capability_response)

	result=$(zxfer_get_cached_remote_capability_response_for_host "target.example")

	assertContains "Target-side cached capability reads should return the cached payload." \
		"$result" "tool	cat	0	/remote/bin/cat"
}

test_zxfer_store_cached_remote_capability_response_for_host_falls_back_to_origin_slot() {
	zxfer_store_cached_remote_capability_response_for_host "shared.example" "$(fake_remote_capability_response)"

	assertEquals "Unassigned cached capability responses should populate the origin fallback slot first." \
		"shared.example" "$g_origin_remote_capabilities_host"
}

test_zxfer_store_cached_remote_capability_response_for_host_falls_back_to_target_slot_after_origin() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)

	zxfer_store_cached_remote_capability_response_for_host "other.example" "$(fake_remote_capability_response)"

	assertEquals "Once the origin fallback slot is occupied, later unassigned cache responses should populate the target slot." \
		"other.example" "$g_target_remote_capabilities_host"
}

test_zxfer_ensure_remote_capability_cache_dir_creates_secure_directory() {
	cache_dir=$(zxfer_ensure_remote_capability_cache_dir)
	mode=$(zxfer_get_path_mode_octal "$cache_dir")
	owner=$(zxfer_get_path_owner_uid "$cache_dir")
	effective_uid=$(zxfer_get_effective_user_uid)

	assertTrue "Capability cache directory creation should succeed." "[ -d '$cache_dir' ]"
	assertEquals "Capability cache directories should be created with 0700 permissions." "700" "$mode"
	assertEquals "Capability cache directories should be owned by the current effective uid." \
		"$effective_uid" "$owner"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_uid_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directory creation should fail cleanly when the effective uid cannot be determined." 1 "$status"
	assertEquals "Capability cache directory failures should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directory creation should fail cleanly when effective temp-root lookup fails." \
		1 "$status"
	assertEquals "Capability cache directory failures should not produce a payload when the temp root cannot be resolved." \
		"" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_rejects_insecure_existing_mode() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
	mkdir "$cache_dir"
	chmod 755 "$cache_dir"

	set +e
	output=$(zxfer_ensure_remote_capability_cache_dir)
	status=$?

	assertEquals "Existing capability cache directories with insecure permissions should be rejected." 1 "$status"
	assertEquals "Rejected capability cache directories should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_existing_owner_lookup_fails() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
	mkdir "$cache_dir"
	chmod 700 "$cache_dir"

	set +e
	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directories should fail cleanly when existing-directory owner lookup fails." 1 "$status"
	assertEquals "Owner-lookup failures for existing capability cache directories should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_reports_existing_owner_lookup_failure_in_current_shell() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
	mkdir "$cache_dir"
	chmod 700 "$cache_dir"
	fake_bin_dir="$TEST_TMPDIR/remote_capability_owner_lookup_fail_bin"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/stat" <<'EOF'
#!/bin/sh
exit 1
EOF
	cat >"$fake_bin_dir/ls" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin_dir/stat" "$fake_bin_dir/ls"

	PATH="$fake_bin_dir:$original_path"
	zxfer_ensure_remote_capability_cache_dir >/dev/null 2>&1
	status=$?
	PATH=$original_path

	assertEquals "Capability cache directories should fail in the current shell when existing-directory owner lookup fails." \
		1 "$status"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_existing_mode_lookup_fails() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
	mkdir "$cache_dir"
	chmod 700 "$cache_dir"

	set +e
	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directories should fail cleanly when existing-directory mode lookup fails." 1 "$status"
	assertEquals "Mode-lookup failures for existing capability cache directories should not produce a payload." "" "$output"
}

test_zxfer_ensure_remote_capability_cache_dir_returns_failure_when_mkdir_fails() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
	rm -rf "$cache_dir"

	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Capability cache directory creation should fail cleanly when mkdir fails." 1 "$status"
	assertEquals "Capability cache directory mkdir failures should not produce a payload." "" "$output"
}

test_zxfer_remote_capability_cache_path_rejects_symlinked_cache_dir() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-remote-capabilities.$effective_uid.d"
	rm -rf "$cache_dir"
	ln -s "$TEST_TMPDIR/other-cache-dir" "$cache_dir"

	set +e
	output=$(zxfer_remote_capability_cache_path "origin.example")
	status=$?

	assertEquals "Symlinked capability cache directories should be rejected." 1 "$status"
	assertEquals "Rejected cache-directory paths should not produce a payload." "" "$output"
}

test_zxfer_ssh_control_socket_cache_key_falls_back_when_hex_encoding_is_empty() {
	result=$(
		(
			cksum() {
				return 1
			}
			od() {
				:
			}
			zxfer_ssh_control_socket_cache_key "origin.example"
		)
	)

	assertEquals "Shared ssh control socket cache keys should fall back to a stable sentinel when hex encoding is empty." \
		"k00" "$result"
}

test_zxfer_ssh_control_socket_cache_key_uses_hex_fallback_in_current_shell() {
	output_file="$TEST_TMPDIR/ssh_control_socket_cache_key.out"

	(
		cksum() {
			return 1
		}
		od() {
			printf '%s\n' " 61 62 63 64"
		}
		zxfer_ssh_control_socket_cache_key "origin.example" >"$output_file"
	)

	assertEquals "Shared ssh control socket cache keys should fall back to a truncated hex digest when cksum is unavailable." \
		"k61626364" "$(cat "$output_file")"
}

test_zxfer_ssh_control_socket_cache_key_uses_path_shadowed_hex_fallback_in_current_shell() {
	fake_bin_dir="$TEST_TMPDIR/ssh_control_socket_cache_key_bin"
	output_file="$TEST_TMPDIR/ssh_control_socket_cache_key_shadowed.out"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/cksum" <<'EOF'
#!/bin/sh
exit 1
EOF
	cat >"$fake_bin_dir/od" <<'EOF'
#!/bin/sh
printf '%s\n' " 61 62 63 64"
EOF
	chmod +x "$fake_bin_dir/cksum" "$fake_bin_dir/od"

	PATH="$fake_bin_dir:$original_path"
	zxfer_ssh_control_socket_cache_key "origin.example" >"$output_file"
	PATH=$original_path

	assertEquals "Shared ssh control socket cache keys should exercise the hex fallback in the current shell when cksum is unavailable from PATH." \
		"k61626364" "$(cat "$output_file")"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_returns_failure_when_uid_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should fail cleanly when uid lookup fails." 1 "$status"
	assertEquals "Failed shared ssh control socket cache dir creation should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_returns_failure_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_socket_cache_tmpdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should fail cleanly when effective temp-root lookup fails." \
		1 "$status"
	assertEquals "Failed shared ssh control socket cache dir creation should not produce a payload when the temp root cannot be resolved." \
		"" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_rejects_insecure_existing_mode() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-s.$effective_uid.d"
	mkdir -p "$cache_dir"
	chmod 755 "$cache_dir"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_cache_dir)
	status=$?

	assertEquals "Shared ssh control socket cache dirs should reject insecure pre-existing permissions." 1 "$status"
	assertEquals "Rejected shared ssh control socket cache dirs should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_reports_existing_owner_lookup_failure_in_current_shell() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-s.$effective_uid.d"
	mkdir -p "$cache_dir"
	chmod 700 "$cache_dir"
	fake_bin_dir="$TEST_TMPDIR/ssh_control_socket_owner_lookup_fail_bin"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/stat" <<'EOF'
#!/bin/sh
exit 1
EOF
	cat >"$fake_bin_dir/ls" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin_dir/stat" "$fake_bin_dir/ls"

	PATH="$fake_bin_dir:$original_path"
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	status=$?
	PATH=$original_path

	assertEquals "Shared ssh control socket cache dirs should fail in the current shell when existing-directory owner lookup fails." \
		1 "$status"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_returns_failure_when_mkdir_fails() {
	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should fail cleanly when mkdir fails." 1 "$status"
	assertEquals "Failed shared ssh control socket cache dir creation should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_reports_direct_lookup_failures_in_current_shell() {
	effective_uid=$(zxfer_get_effective_user_uid)
	cache_dir="$TEST_TMPDIR/zxfer-s.$effective_uid.d"
	rm -rf "$cache_dir"
	ln -s "$TEST_TMPDIR/other-shared-cache-dir" "$cache_dir"

	set +e
	zxfer_ensure_ssh_control_socket_cache_dir >/dev/null 2>&1
	symlink_status=$?
	rm -f "$cache_dir"
	mkdir -p "$cache_dir"
	chmod 700 "$cache_dir"

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	mode_status=$?

	rm -rf "$cache_dir"
	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	create_owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_cache_dir >/dev/null
	)
	create_mode_status=$?

	assertEquals "Shared ssh control socket cache dir creation should reject symlinked cache dirs." \
		1 "$symlink_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when existing-directory owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when existing-directory mode lookup fails." \
		1 "$mode_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when post-create owner lookup fails." \
		1 "$create_owner_status"
	assertEquals "Shared ssh control socket cache dir creation should fail when post-create mode lookup fails." \
		1 "$create_mode_status"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_creates_secure_entry_and_leases_dir() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")

	assertTrue "Shared ssh control socket entry dirs should be created on demand." \
		"[ -d '$entry_dir' ]"
	assertTrue "Shared ssh control socket entry dirs should create a leases subdirectory." \
		"[ -d '$entry_dir/leases' ]"
	assertTrue "Shared ssh control socket entry dirs should persist a secure identity file." \
		"[ -f '$entry_dir/id' ]"
	assertEquals "Shared ssh control socket entry dirs should be mode 0700." \
		"700" "$(zxfer_get_path_mode_octal "$entry_dir")"
	assertEquals "Shared ssh control socket lease dirs should be mode 0700." \
		"700" "$(zxfer_get_path_mode_octal "$entry_dir/leases")"
	assertEquals "Shared ssh control socket identity files should be mode 0600." \
		"600" "$(zxfer_get_path_mode_octal "$entry_dir/id")"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_uses_suffix_when_identity_mismatches_existing_key() {
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "kshared")
	result=$(
		(
			zxfer_ssh_control_socket_cache_key() {
				printf '%s\n' "kshared"
			}
			first=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
			second=$(zxfer_ensure_ssh_control_socket_entry_dir "other.example")
			printf 'first=%s\n' "$first"
			printf 'second=%s\n' "$second"
		)
	)

	assertContains "Mismatched shared ssh control socket identities should keep the first cache entry on the base key." \
		"$result" "first=$cache_dir/kshared"
	assertContains "Mismatched shared ssh control socket identities should fall back to a suffixed cache entry instead of reusing the wrong socket." \
		"$result" "second=$cache_dir/kshared.1"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_keeps_socket_paths_short_for_long_hosts() {
	long_tmpdir="$TEST_TMPDIR_PHYSICAL/socket-root-segment-0123456789/socket-root-segment-0123456789/socket-root-segment-0123456789"
	mkdir -p "$long_tmpdir"
	expected_tmpdir=$(
		unset TMPDIR
		zxfer_try_get_socket_cache_tmpdir
	)

	result=$(
		TMPDIR=$long_tmpdir
		export TMPDIR
		g_cmd_ssh="/opt/local/bin/really-long-custom-ssh-wrapper"
		entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "aldo@doBackup.clientsupportsoftware.com pfexec -u root")
		socket_path="$entry_dir/s"
		temp_listener_path="$socket_path.Mvij6x1tYLn6woxm"
		printf 'socket=%s\n' "$socket_path"
		printf 'socket_length=%s\n' "${#socket_path}"
		printf 'temp_listener_length=%s\n' "${#temp_listener_path}"
	)
	temp_listener_length=$(printf '%s\n' "$result" | awk -F= '/^temp_listener_length=/{print $2}')

	assertContains "Short shared ssh control socket paths should still be rooted under the per-user cache dir." \
		"$result" "socket=$expected_tmpdir/zxfer-s.$(id -u).d/"
	assertTrue "Shared ssh control socket paths should stay below the Unix domain socket limit even after OpenSSH appends its temporary suffix." \
		"[ \"$temp_listener_length\" -lt 104 ]"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_symlinked_entry_dir() {
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$cache_key")
	ln -s "$TEST_TMPDIR/other-entry-dir" "$cache_dir/$cache_key"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	status=$?

	assertEquals "Shared ssh control socket entry dirs should reject symlinked cache entries." 1 "$status"
	assertEquals "Rejected shared ssh control socket entry dirs should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_rejects_insecure_leases_dir() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	chmod 755 "$entry_dir/leases"

	set +e
	output=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	status=$?

	assertEquals "Shared ssh control socket entry dirs should reject insecure leases directories." 1 "$status"
	assertEquals "Rejected shared ssh control socket entry dirs with insecure leases dirs should not produce a payload." "" "$output"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_existing_entry_lookup_failures_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	rm -rf "$entry_dir/leases"
	mkdir "$entry_dir/leases"
	chmod 700 "$entry_dir/leases"

	set +e
	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	uid_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket entry reuse should fail when entry-directory owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket entry reuse should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket entry reuse should fail when entry-directory mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_cache_key_failure_in_current_shell_direct() {
	set +e
	zxfer_ssh_control_socket_cache_key() {
		return 4
	}
	zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null 2>&1
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket entry creation should fail when cache-key derivation fails in the current shell." \
		1 "$status"
}

test_zxfer_ensure_ssh_control_socket_entry_dir_reports_existing_entry_uid_and_mode_failures_after_cache_lookup_in_current_shell() {
	cache_dir="$TEST_TMPDIR/shared-entry-cache"
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example")
	entry_dir="$cache_dir/$cache_key"
	mkdir -p "$entry_dir"
	chmod 700 "$entry_dir"

	set +e
	(
		zxfer_ensure_ssh_control_socket_cache_dir() {
			printf '%s\n' "$cache_dir"
		}
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	uid_status=$?

	(
		zxfer_ensure_ssh_control_socket_cache_dir() {
			printf '%s\n' "$cache_dir"
		}
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_ssh_control_socket_entry_dir "origin.example" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket entry reuse should fail when entry-branch effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket entry reuse should fail when entry-branch mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_read_ssh_control_socket_entry_identity_file_reports_lookup_failures_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_read_ssh_control_socket_entry_identity_file "$identity_path" >/dev/null
	)
	mode_status=$?

	assertEquals "Shared ssh control socket identity reads should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket identity reads should fail when owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket identity reads should fail when mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_reports_failures_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	owner_status=$?

	rm -f "$identity_path"
	(
		zxfer_render_ssh_control_socket_entry_identity() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	render_status=$?

	rm -f "$identity_path"
	(
		mv() {
			return 1
		}
		zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example"
	)
	move_status=$?

	assertEquals "Shared ssh control socket identity writes should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Shared ssh control socket identity writes should fail when owner lookup fails." \
		1 "$owner_status"
	assertEquals "Shared ssh control socket identity writes should fail when the identity payload cannot be rendered." \
		1 "$render_status"
	assertEquals "Shared ssh control socket identity writes should fail when the identity file cannot be moved into place." \
		1 "$move_status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_rejects_mismatched_owner_in_current_shell() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	printf '%s\n' "stale" >"$identity_path"
	chmod 600 "$identity_path"

	set +e
	zxfer_get_effective_user_uid() {
		printf '%s\n' "111"
	}
	zxfer_get_path_owner_uid() {
		printf '%s\n' "222"
	}
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket identity writes should reject existing identity files owned by a different uid." \
		1 "$status"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_reports_render_failure_in_current_shell_direct() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	rm -f "$identity_path"

	set +e
	zxfer_render_ssh_control_socket_entry_identity() {
		return 4
	}
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	status=$?
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket identity writes should fail when the identity renderer fails in the current shell." \
		1 "$status"
	assertFalse "Failed identity writes should not leave a partial installed identity file behind." \
		"[ -f \"$identity_path\" ]"
}

test_zxfer_write_ssh_control_socket_entry_identity_file_reports_mktemp_failure_in_current_shell_direct() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	identity_path="$entry_dir/id"

	rm -f "$identity_path"

	set +e
	mktemp() {
		return 4
	}
	zxfer_write_ssh_control_socket_entry_identity_file "$entry_dir" "origin.example" >/dev/null 2>&1
	status=$?
	unset -f mktemp
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Shared ssh control socket identity writes should fail when temporary-file creation fails in the current shell." \
		1 "$status"
}

test_zxfer_acquire_ssh_control_socket_lock_returns_failure_when_lock_is_symlinked() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	ln -s "$TEST_TMPDIR/other-lock" "$entry_dir.lock"

	set +e
	output=$(zxfer_acquire_ssh_control_socket_lock "$entry_dir")
	status=$?

	assertEquals "Shared ssh control socket lock acquisition should reject symlinked lock dirs." 1 "$status"
	assertEquals "Rejected shared ssh control socket lock acquisition should not produce a payload." "" "$output"
}

test_zxfer_acquire_ssh_control_socket_lock_returns_failure_after_retries() {
	log="$TEST_TMPDIR/ssh_lock_retry.log"
	entry_dir="$TEST_TMPDIR/ssh_lock_retry_entry"
	mkdir -p "$entry_dir"

	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			sleep() {
				printf 'retry\n' >>"$log"
			}
			zxfer_acquire_ssh_control_socket_lock "$entry_dir"
		)
	)
	status=$?

	assertEquals "Shared ssh control socket lock acquisition should fail after repeated contention." 1 "$status"
	assertEquals "Failed shared ssh control socket lock acquisition should not produce a payload." "" "$output"
	assertEquals "Shared ssh control socket lock acquisition should retry before failing." \
		"9" "$(wc -l <"$log" | tr -d ' ')"
}

test_zxfer_prune_stale_ssh_control_socket_leases_removes_invalid_and_dead_entries() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	invalid_lease="$entry_dir/leases/lease.invalid"
	dead_lease="$entry_dir/leases/lease.999999.dead"
	live_lease="$entry_dir/leases/lease.$$.live"
	: >"$invalid_lease"
	: >"$dead_lease"
	: >"$live_lease"

	zxfer_prune_stale_ssh_control_socket_leases "$entry_dir"

	assertFalse "Invalid ssh control socket lease names should be pruned." \
		"[ -e '$invalid_lease' ]"
	assertFalse "Dead ssh control socket leases should be pruned." \
		"[ -e '$dead_lease' ]"
	assertTrue "Live ssh control socket leases should be preserved." \
		"[ -e '$live_lease' ]"
}

test_zxfer_count_ssh_control_socket_leases_handles_empty_and_nonempty_dirs() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")

	assertEquals "Empty shared ssh control socket lease dirs should count as zero leases." \
		"0" "$(zxfer_count_ssh_control_socket_leases "$entry_dir")"

	: >"$entry_dir/leases/lease.$$.one"
	: >"$entry_dir/leases/lease.$$.two"

	assertEquals "Shared ssh control socket lease counting should reflect the current number of live lease files." \
		"2" "$(zxfer_count_ssh_control_socket_leases "$entry_dir")"
}

test_zxfer_count_ssh_control_socket_leases_returns_zero_for_missing_dir_in_current_shell() {
	output_file="$TEST_TMPDIR/ssh_control_socket_lease_count.out"

	zxfer_count_ssh_control_socket_leases "$TEST_TMPDIR/missing-entry" >"$output_file"

	assertEquals "Missing shared ssh control socket lease dirs should count as zero leases." \
		"0" "$(cat "$output_file")"
}

test_zxfer_ensure_remote_host_capabilities_prefers_memory_cache() {
	g_origin_remote_capabilities_host="origin.example"
	g_origin_remote_capabilities_response=$(fake_remote_capability_response)
	g_origin_remote_capabilities_bootstrap_source="cache"
	g_cmd_ssh="$FAKE_SSH_BIN"
	FAKE_SSH_EXIT_STATUS=255
	export FAKE_SSH_EXIT_STATUS

	result=$(zxfer_ensure_remote_host_capabilities "origin.example" source)

	unset FAKE_SSH_EXIT_STATUS

	assertContains "In-memory capability cache hits should satisfy lookups without ssh." \
		"$result" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "In-memory capability cache hits should preserve the original bootstrap source." \
		"cache" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_preserves_first_source() {
	g_option_O_origin_host="origin.example"

	zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" live
	zxfer_note_remote_capability_bootstrap_source_for_host "origin.example" cache

	assertEquals "Bootstrap source tracking should preserve the first remote discovery source for the origin host." \
		"live" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_note_remote_capability_bootstrap_source_for_host_sets_target_source_in_current_shell() {
	g_option_T_target_host="target.example"

	zxfer_note_remote_capability_bootstrap_source_for_host "target.example" live
	zxfer_note_remote_capability_bootstrap_source_for_host "target.example" cache

	assertEquals "Bootstrap source tracking should preserve the first remote discovery source for the target host." \
		"live" "$g_target_remote_capabilities_bootstrap_source"
}

test_zxfer_ensure_remote_host_capabilities_marks_cache_backed_bootstrap_source() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	result_file="$TEST_TMPDIR/remote_caps_cache_backed.out"
	g_option_O_origin_host="origin.example"
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
	result=$(cat "$result_file")

	assertContains "Cache-backed capability lookups should still return the cached payload." \
		"$result" "tool	zfs	0	/remote/bin/zfs"
	assertEquals "Cache-backed capability lookups should record that startup was satisfied from cache." \
		"cache" "$g_origin_remote_capabilities_bootstrap_source"
}

test_zxfer_try_acquire_remote_capability_cache_lock_creates_secure_lock_and_pid_file() {
	lock_dir=$(zxfer_try_acquire_remote_capability_cache_lock "origin.example")
	pid_path="$lock_dir/pid"

	assertTrue "Capability cache lock acquisition should create the lock directory." \
		"[ -d '$lock_dir' ]"
	assertEquals "Capability cache lock directories should be owner-only." \
		"700" "$(zxfer_get_path_mode_octal "$lock_dir")"
	assertTrue "Capability cache lock acquisition should create a pid file." \
		"[ -f '$pid_path' ]"
	assertEquals "Capability cache lock pid files should be owner-only." \
		"600" "$(zxfer_get_path_mode_octal "$pid_path")"
	lock_pid=$(cat "$pid_path")
	case "$lock_pid" in
	'' | *[!0-9]*)
		fail "Capability cache lock pid files should contain a numeric pid."
		;;
	esac

	zxfer_release_remote_capability_cache_lock "$lock_dir"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_busy_for_live_owner() {
	lock_dir=$(zxfer_try_acquire_remote_capability_cache_lock "origin.example")

	set +e
	output=$(zxfer_try_acquire_remote_capability_cache_lock "origin.example")
	status=$?

	assertEquals "A live sibling capability cache lock should report the lock as busy." \
		2 "$status"
	assertEquals "Busy capability cache lock acquisitions should not print a path." \
		"" "$output"

	zxfer_release_remote_capability_cache_lock "$lock_dir"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reaps_stale_lock() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "999999999" >"$lock_dir/pid"
	chmod 600 "$lock_dir/pid"

	result=$(zxfer_try_acquire_remote_capability_cache_lock "origin.example")

	assertEquals "Stale capability cache locks should be reaped and reacquired." \
		"$lock_dir" "$result"

	zxfer_release_remote_capability_cache_lock "$lock_dir"
}

test_zxfer_try_acquire_remote_capability_cache_lock_returns_failure_for_insecure_pid_file() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "$$" >"$lock_dir/pid"
	chmod 644 "$lock_dir/pid"

	set +e
	output=$(zxfer_try_acquire_remote_capability_cache_lock "origin.example")
	status=$?

	assertEquals "Malformed or insecure capability cache lock pid files should fail closed." \
		1 "$status"
	assertEquals "Failed capability cache lock acquisitions should not print a path." \
		"" "$output"
}

test_zxfer_wait_for_remote_capability_cache_fill_retries_until_cache_is_populated() {
	read_attempt_file="$TEST_TMPDIR/remote_caps_wait.attempts"
	printf '%s\n' 0 >"$read_attempt_file"

	set +e
	output=$(
		(
			g_zxfer_remote_capability_cache_wait_retries=3
			zxfer_read_remote_capability_cache_file() {
				read_attempts=$(cat "$read_attempt_file")
				read_attempts=$((read_attempts + 1))
				printf '%s\n' "$read_attempts" >"$read_attempt_file"
				if [ "$read_attempts" -eq 2 ]; then
					fake_remote_capability_response
					return 0
				fi
				return 1
			}
			sleep() {
				:
			}
			zxfer_wait_for_remote_capability_cache_fill "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache waits should retry until a sibling populates the cache." \
		0 "$status"
	assertContains "Capability cache waits should return the populated cached payload." \
		"$output" "tool	zfs	0	/remote/bin/zfs"
}

test_zxfer_ensure_remote_host_capabilities_waits_for_sibling_cache_fill() {
	result_file="$TEST_TMPDIR/remote_caps_wait.out"
	bootstrap_file="$TEST_TMPDIR/remote_caps_wait.bootstrap"
	live_marker="$TEST_TMPDIR/remote_caps_wait.live"

	set +e
	(
		g_option_O_origin_host="origin.example"
		zxfer_get_cached_remote_capability_response_for_host() {
			return 4
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			return 2
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			fake_remote_capability_response
		}
		zxfer_fetch_remote_host_capabilities_live() {
			printf '%s\n' "live-fetch" >"$live_marker"
			return 1
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
		printf '%s\n' "$g_origin_remote_capabilities_bootstrap_source" >"$bootstrap_file"
	)
	status=$?

	assertEquals "Sibling capability cache locks should be satisfied by the populated cache." \
		0 "$status"
	assertContains "Capability lookups satisfied by a sibling should return the cached payload." \
		"$(cat "$result_file")" "tool	parallel	0	/opt/bin/parallel"
	assertEquals "Sibling-populated capability cache hits should be marked as cache-backed startup." \
		"cache" "$(cat "$bootstrap_file")"
	assertFalse "Sibling-populated capability cache hits should not fall back to a live ssh probe." \
		"[ -e '$live_marker' ]"
}

test_zxfer_ensure_remote_capability_cache_dir_reports_post_create_lookup_failures_in_current_shell() {
	owner_tmp="$TEST_TMPDIR/remote_caps_ownerfail"
	mode_tmp="$TEST_TMPDIR/remote_caps_modefail"
	mkdir -p "$owner_tmp" "$mode_tmp"

	set +e
	(
		TMPDIR="$owner_tmp"
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_ensure_remote_capability_cache_dir >/dev/null
	)
	owner_status=$?

	(
		TMPDIR="$mode_tmp"
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_ensure_remote_capability_cache_dir >/dev/null
	)
	mode_status=$?

	assertEquals "Remote capability cache dir setup should fail when post-create owner lookup fails." \
		1 "$owner_status"
	assertEquals "Remote capability cache dir setup should fail when post-create mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_remote_capability_cache_lock_path_returns_failure_when_cache_path_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_remote_capability_cache_path() {
				return 1
			}
			zxfer_remote_capability_cache_lock_path "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache lock-path lookups should fail cleanly when cache-path resolution fails." \
		1 "$status"
	assertEquals "Capability cache lock-path lookup failures should not print a path." \
		"" "$output"
}

test_zxfer_remote_capability_cache_lock_helpers_report_lookup_and_timeout_failures() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_lookup"
	pid_path="$lock_dir/pid"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "$$" >"$pid_path"
	chmod 600 "$pid_path"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	)
	validate_uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	)
	validate_owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir "$lock_dir" >/dev/null
	)
	validate_mode_status=$?

	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_read_remote_capability_cache_lock_pid_file "$pid_path" >/dev/null
	)
	pid_uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_read_remote_capability_cache_lock_pid_file "$pid_path" >/dev/null
	)
	pid_owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_read_remote_capability_cache_lock_pid_file "$pid_path" >/dev/null
	)
	pid_mode_status=$?

	printf '%s\n' "not-a-pid" >"$pid_path"
	(
		zxfer_read_remote_capability_cache_lock_pid_file "$pid_path" >/dev/null
	)
	pid_parse_status=$?

	(
		g_zxfer_remote_capability_cache_wait_retries="not-a-number"
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		sleep() {
			:
		}
		zxfer_wait_for_remote_capability_cache_fill "origin.example" >/dev/null
	)
	wait_status=$?

	assertEquals "Capability cache lock-dir validation should fail when uid lookup fails." \
		1 "$validate_uid_status"
	assertEquals "Capability cache lock-dir validation should fail when owner lookup fails." \
		1 "$validate_owner_status"
	assertEquals "Capability cache lock-dir validation should fail when mode lookup fails." \
		1 "$validate_mode_status"
	assertEquals "Capability cache lock pid reads should fail when uid lookup fails." \
		1 "$pid_uid_status"
	assertEquals "Capability cache lock pid reads should fail when owner lookup fails." \
		1 "$pid_owner_status"
	assertEquals "Capability cache lock pid reads should fail when mode lookup fails." \
		1 "$pid_mode_status"
	assertEquals "Capability cache lock pid reads should reject malformed pid contents." \
		1 "$pid_parse_status"
	assertEquals "Capability cache waits should fail after retry normalization when no sibling populates the cache." \
		1 "$wait_status"
}

test_zxfer_reap_stale_pidless_remote_capability_cache_lock_removes_valid_lock_dir() {
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	zxfer_reap_stale_pidless_remote_capability_cache_lock "origin.example"
	status=$?

	assertEquals "Pidless capability cache locks should be reaped after a bounded wait." \
		0 "$status"
	assertFalse "Reaping a pidless capability cache lock should remove the stale lock directory." \
		"[ -e '$lock_dir' ]"
}

test_zxfer_write_remote_capability_cache_lock_pid_file_reports_write_and_move_failures() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_write"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	set +e
	(
		mktemp() {
			return 1
		}
		zxfer_write_remote_capability_cache_lock_pid_file "$lock_dir" >/dev/null
	)
	mktemp_status=$?

	(
		mktemp() {
			printf '%s\n' "$TEST_TMPDIR/remote_caps_lock_write_missing/pid"
		}
		zxfer_write_remote_capability_cache_lock_pid_file "$lock_dir" >/dev/null
	)
	write_status=$?

	(
		mv() {
			return 1
		}
		zxfer_write_remote_capability_cache_lock_pid_file "$lock_dir" >/dev/null
	)
	move_status=$?

	assertEquals "Capability cache lock pid writes should fail cleanly when mktemp fails." \
		1 "$mktemp_status"
	assertEquals "Capability cache lock pid writes should fail cleanly when the pid file cannot be written." \
		1 "$write_status"
	assertEquals "Capability cache lock pid writes should fail cleanly when the pid file cannot be moved into place." \
		1 "$move_status"
}

test_zxfer_create_remote_capability_cache_lock_dir_cleans_up_after_helper_failures() {
	validate_lock_dir="$TEST_TMPDIR/remote_caps_lock_validate_cleanup"
	write_lock_dir="$TEST_TMPDIR/remote_caps_lock_write_cleanup"

	set +e
	(
		zxfer_validate_remote_capability_cache_lock_dir() {
			return 1
		}
		zxfer_create_remote_capability_cache_lock_dir "$validate_lock_dir"
	)
	validate_status=$?

	(
		zxfer_write_remote_capability_cache_lock_pid_file() {
			return 1
		}
		zxfer_create_remote_capability_cache_lock_dir "$write_lock_dir"
	)
	write_status=$?

	assertEquals "Capability cache lock-dir creation should fail when validation fails." \
		1 "$validate_status"
	assertFalse "Capability cache lock-dir creation should clean up failed validation directories." \
		"[ -e '$validate_lock_dir' ]"
	assertEquals "Capability cache lock-dir creation should fail when pid-file creation fails." \
		1 "$write_status"
	assertFalse "Capability cache lock-dir creation should clean up failed pid-file directories." \
		"[ -e '$write_lock_dir' ]"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reports_path_and_reap_failures() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_reap_failure"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "999999999" >"$lock_dir/pid"
	chmod 600 "$lock_dir/pid"

	set +e
	(
		zxfer_remote_capability_cache_lock_path() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	path_status=$?

	(
		zxfer_remote_capability_cache_lock_path() {
			printf '%s\n' "$lock_dir"
		}
		rm() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	reap_status=$?

	assertEquals "Capability cache lock acquisition should fail when lock-path resolution fails." \
		1 "$path_status"
	assertEquals "Capability cache lock acquisition should fail closed when stale-lock cleanup fails." \
		1 "$reap_status"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reports_existing_dir_validation_failure_in_current_shell() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_validate_failure"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	set +e
	(
		zxfer_remote_capability_cache_lock_path() {
			printf '%s\n' "$lock_dir"
		}
		zxfer_create_remote_capability_cache_lock_dir() {
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	status=$?

	assertEquals "Capability cache lock acquisition should fail when an existing lock directory cannot be revalidated." \
		1 "$status"
}

test_zxfer_try_acquire_remote_capability_cache_lock_reports_post_reap_validation_failure_in_current_shell() {
	lock_dir="$TEST_TMPDIR/remote_caps_lock_post_reap_failure"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"
	printf '%s\n' "999999999" >"$lock_dir/pid"
	chmod 600 "$lock_dir/pid"

	set +e
	(
		create_attempts=0
		zxfer_remote_capability_cache_lock_path() {
			printf '%s\n' "$lock_dir"
		}
		zxfer_create_remote_capability_cache_lock_dir() {
			create_attempts=$((create_attempts + 1))
			if [ "$create_attempts" -eq 2 ]; then
				mkdir "$lock_dir"
				chmod 700 "$lock_dir"
			fi
			return 1
		}
		zxfer_validate_remote_capability_cache_lock_dir() {
			[ "$create_attempts" -lt 2 ]
		}
		zxfer_try_acquire_remote_capability_cache_lock "origin.example" >/dev/null
	)
	status=$?

	assertEquals "Capability cache lock acquisition should fail when a stale lock is reaped but the recreated directory cannot be revalidated." \
		1 "$status"
}

test_zxfer_release_remote_capability_cache_lock_returns_failure_for_invalid_targets() {
	lock_file="$TEST_TMPDIR/remote_caps_lock_file"
	lock_link="$TEST_TMPDIR/remote_caps_lock_link"
	printf '%s\n' "not-a-dir" >"$lock_file"
	ln -s "$lock_file" "$lock_link"

	set +e
	zxfer_release_remote_capability_cache_lock "$lock_file" >/dev/null
	file_status=$?
	zxfer_release_remote_capability_cache_lock "$lock_link" >/dev/null
	link_status=$?

	assertEquals "Capability cache lock release should fail for non-directory targets." \
		1 "$file_status"
	assertEquals "Capability cache lock release should fail for symlink targets." \
		1 "$link_status"
}

test_zxfer_ensure_remote_host_capabilities_falls_back_to_live_probe_after_wait_timeout() {
	result_file="$TEST_TMPDIR/remote_caps_live_fallback.out"
	bootstrap_file="$TEST_TMPDIR/remote_caps_live_fallback.bootstrap"
	lock_attempt_file="$TEST_TMPDIR/remote_caps_live_fallback.lock_attempts"
	printf '%s\n' 0 >"$lock_attempt_file"

	set +e
	(
		g_option_O_origin_host="origin.example"
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			lock_attempts=$(cat "$lock_attempt_file")
			lock_attempts=$((lock_attempts + 1))
			printf '%s\n' "$lock_attempts" >"$lock_attempt_file"
			return 2
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			return 1
		}
		zxfer_fetch_remote_host_capabilities_live() {
			fake_remote_capability_response
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
		printf '%s\n' "$g_origin_remote_capabilities_bootstrap_source" >"$bootstrap_file"
	)
	status=$?

	assertEquals "Capability lookups should fall back to a live probe after a bounded sibling-cache wait timeout." \
		0 "$status"
	assertContains "Live fallback after a sibling-cache timeout should still return the capability payload." \
		"$(cat "$result_file")" "tool	zfs	0	/remote/bin/zfs"
	assertEquals "Live fallback after a sibling-cache timeout should mark startup as live." \
		"live" "$(cat "$bootstrap_file")"
}

test_zxfer_ensure_remote_host_capabilities_reaps_pidless_lock_after_wait_timeout() {
	result_file="$TEST_TMPDIR/remote_caps_pidless_reap.out"
	bootstrap_file="$TEST_TMPDIR/remote_caps_pidless_reap.bootstrap"
	lock_dir=$(zxfer_remote_capability_cache_lock_path "origin.example")
	lock_attempt_file="$TEST_TMPDIR/remote_caps_pidless_reap.lock_attempts"
	printf '%s\n' 0 >"$lock_attempt_file"
	mkdir "$lock_dir"
	chmod 700 "$lock_dir"

	set +e
	(
		g_option_O_origin_host="origin.example"
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			lock_attempts=$(cat "$lock_attempt_file")
			lock_attempts=$((lock_attempts + 1))
			printf '%s\n' "$lock_attempts" >"$lock_attempt_file"
			if [ "$lock_attempts" -eq 1 ]; then
				return 2
			fi
			printf '%s\n' "$lock_dir"
			return 0
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			return 1
		}
		zxfer_fetch_remote_host_capabilities_live() {
			fake_remote_capability_response
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >"$result_file"
		printf '%s\n' "$g_origin_remote_capabilities_bootstrap_source" >"$bootstrap_file"
	)
	status=$?

	assertEquals "Capability lookups should reclaim stale pidless locks after the bounded wait and continue with a live probe." \
		0 "$status"
	assertContains "Pidless-lock recovery should still return the capability payload." \
		"$(cat "$result_file")" "tool	cat	0	/remote/bin/cat"
	assertEquals "Pidless-lock recovery should mark startup as live." \
		"live" "$(cat "$bootstrap_file")"
	assertFalse "Pidless-lock recovery should remove the stale lock directory before reacquiring." \
		"[ -e '$lock_dir' ]"
}

test_zxfer_ensure_remote_host_capabilities_returns_failure_when_second_lock_attempt_fails() {
	lock_attempt_file="$TEST_TMPDIR/remote_caps_second_lock_attempts"
	printf '%s\n' 0 >"$lock_attempt_file"

	set +e
	(
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			lock_attempts=$(cat "$lock_attempt_file")
			lock_attempts=$((lock_attempts + 1))
			printf '%s\n' "$lock_attempts" >"$lock_attempt_file"
			if [ "$lock_attempts" -eq 1 ]; then
				return 2
			fi
			return 1
		}
		zxfer_wait_for_remote_capability_cache_fill() {
			return 1
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	)
	status=$?

	assertEquals "Capability lookups should fail closed when the second lock attempt reports a hard failure." \
		1 "$status"
}

test_zxfer_ensure_remote_host_capabilities_returns_failure_for_unexpected_lock_status() {
	set +e
	(
		zxfer_get_cached_remote_capability_response_for_host() {
			return 1
		}
		zxfer_read_remote_capability_cache_file() {
			return 1
		}
		zxfer_try_acquire_remote_capability_cache_lock() {
			return 3
		}
		zxfer_ensure_remote_host_capabilities "origin.example" source >/dev/null
	)
	status=$?

	assertEquals "Capability lookups should fail closed on unexpected lock statuses." \
		1 "$status"
}

test_zxfer_read_remote_capability_cache_file_rejects_expired_entries() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' 1
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Expired remote capability cache files should be ignored." 1 "$status"
	assertEquals "Expired remote capability cache files should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_path_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_remote_capability_cache_path() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when cache-path lookup fails." 1 "$status"
	assertEquals "Capability cache reads with cache-path lookup failures should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_non_numeric_epoch() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "not-a-timestamp"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Non-numeric remote capability cache epochs should be rejected." 1 "$status"
	assertEquals "Non-numeric remote capability cache epochs should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_malformed_payload() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
		printf '%s\n' "os	RemoteOS"
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Malformed remote capability cache payloads should be rejected." 1 "$status"
	assertEquals "Malformed remote capability cache payloads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_insecure_permissions() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 644 "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Insecurely permissioned remote capability cache payloads should be rejected." 1 "$status"
	assertEquals "Insecurely permissioned remote capability cache payloads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_rejects_non_regular_target() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	rm -f "$cache_path"
	mkfifo "$cache_path"

	set +e
	output=$(zxfer_read_remote_capability_cache_file "origin.example")
	status=$?

	assertEquals "Non-regular remote capability cache targets should be rejected." 1 "$status"
	assertEquals "Non-regular remote capability cache targets should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_uid_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when uid lookup fails." 1 "$status"
	assertEquals "Uid-lookup failures during capability cache reads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_owner_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when owner lookup fails." 1 "$status"
	assertEquals "Owner-lookup failures during capability cache reads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_returns_failure_when_mode_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_path_mode_octal() {
				return 1
			}
			zxfer_read_remote_capability_cache_file "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache reads should fail cleanly when mode lookup fails." 1 "$status"
	assertEquals "Mode-lookup failures during capability cache reads should not produce a payload." "" "$output"
}

test_zxfer_read_remote_capability_cache_file_reports_direct_lookup_failures_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "$(date '+%s')"
		fake_remote_capability_response
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_read_remote_capability_cache_file "origin.example" >/dev/null
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_read_remote_capability_cache_file "origin.example" >/dev/null
	)
	owner_status=$?

	(
		zxfer_get_path_mode_octal() {
			return 1
		}
		zxfer_read_remote_capability_cache_file "origin.example" >/dev/null
	)
	mode_status=$?

	assertEquals "Remote capability cache reads should fail when effective uid lookup fails." \
		1 "$uid_status"
	assertEquals "Remote capability cache reads should fail when owner lookup fails." \
		1 "$owner_status"
	assertEquals "Remote capability cache reads should fail when mode lookup fails." \
		1 "$mode_status"
}

test_zxfer_write_remote_capability_cache_file_writes_timestamped_payload() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")

	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"

	assertTrue "Successful capability cache writes should create the cache file." \
		"[ -f '$cache_path' ]"
	first_line=$(sed -n '1p' "$cache_path")
	payload=$(sed '1d' "$cache_path")
	case "$first_line" in
	'' | *[!0-9]*)
		fail "Capability cache writes should prefix the payload with a numeric timestamp."
		;;
	esac
	assertEquals "Capability cache writes should preserve the capability payload after the timestamp." \
		"$(fake_remote_capability_response)" "$payload"
}

test_zxfer_write_remote_capability_cache_file_rewrites_existing_secure_file() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "1"
		printf '%s\n' "stale"
	} >"$cache_path"
	chmod 600 "$cache_path"

	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"

	payload=$(sed '1d' "$cache_path")
	assertEquals "Capability cache writes should replace existing secure cache contents." \
		"$(fake_remote_capability_response)" "$payload"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_existing_uid_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "1"
		printf '%s\n' "stale"
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_effective_user_uid() {
				return 1
			}
			zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
		)
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when uid lookup fails for an existing target." 1 "$status"
	assertEquals "Uid-lookup failures during capability cache writes should not produce a payload." "" "$output"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_existing_owner_lookup_fails() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	{
		printf '%s\n' "1"
		printf '%s\n' "stale"
	} >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	output=$(
		(
			zxfer_get_path_owner_uid() {
				return 1
			}
			zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
		)
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when owner lookup fails for an existing target." 1 "$status"
	assertEquals "Owner-lookup failures during capability cache writes should not produce a payload." "" "$output"
}

test_zxfer_write_remote_capability_cache_file_rejects_symlink_target() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	rm -f "$cache_path"
	ln -s "$TEST_TMPDIR/somewhere-else" "$cache_path"

	set +e
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	status=$?

	assertEquals "Capability cache writes should fail closed when the target path is a symlink." 1 "$status"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_mktemp_fails() {
	mktemp() {
		return 4
	}

	set +e
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	status=$?
	unset -f mktemp

	assertEquals "Capability cache writes should fail cleanly when mktemp fails." 1 "$status"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_path_resolution_fails() {
	set +e
	(
		zxfer_remote_capability_cache_path() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	status=$?

	assertEquals "Capability cache writes should fail cleanly when cache-path resolution fails." 1 "$status"
}

test_zxfer_write_remote_capability_cache_file_returns_failure_when_payload_write_fails() {
	mktemp() {
		printf '%s\n' "$TEST_TMPDIR/missing-subdir/cache"
	}

	set +e
	zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	status=$?
	unset -f mktemp

	assertEquals "Capability cache writes should fail cleanly when the cache payload cannot be written." 1 "$status"
}

test_zxfer_write_remote_capability_cache_file_reports_existing_lookup_failures_in_current_shell() {
	cache_path=$(zxfer_remote_capability_cache_path "origin.example")
	: >"$cache_path"
	chmod 600 "$cache_path"

	set +e
	(
		zxfer_get_effective_user_uid() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	uid_status=$?

	(
		zxfer_get_path_owner_uid() {
			return 1
		}
		zxfer_write_remote_capability_cache_file "origin.example" "$(fake_remote_capability_response)"
	)
	owner_status=$?

	assertEquals "Remote capability cache writes should fail when effective uid lookup fails for an existing cache file." \
		1 "$uid_status"
	assertEquals "Remote capability cache writes should fail when owner lookup fails for an existing cache file." \
		1 "$owner_status"
}

test_zxfer_preload_remote_host_capabilities_delegates_to_ensure() {
	log="$TEST_TMPDIR/preload_remote_caps.log"
	: >"$log"

	(
		zxfer_ensure_remote_host_capabilities() {
			printf 'ensure %s %s\n' "$1" "${2:-}" >>"$log"
		}
		zxfer_preload_remote_host_capabilities "origin.example" source
	)

	assertContains "Capability preloading should delegate to the shared ensure helper." \
		"$(cat "$log")" "ensure origin.example source"
}

test_zxfer_get_remote_host_operating_system_returns_failure_when_capabilities_are_unavailable() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_get_remote_host_operating_system_direct() {
				return 1
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)
	status=$?

	assertEquals "Remote OS lookups should fail when both the capability handshake and direct fallback are unavailable." 1 "$status"
	assertEquals "Failed remote OS lookups should not print a payload." "" "$output"
}

test_zxfer_get_remote_host_operating_system_falls_back_to_direct_probe_when_capabilities_are_unavailable() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "FallbackOS"
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)

	assertEquals "Remote OS lookups should fall back to a direct uname probe when the capability handshake is unavailable." \
		"FallbackOS" "$output"
}

test_zxfer_get_remote_host_operating_system_falls_back_to_direct_probe_when_capability_payload_is_malformed() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V1
tool	zfs	0	/remote/bin/zfs"
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "FallbackOS"
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)

	assertEquals "Remote OS lookups should fall back to a direct uname probe when the capability payload is malformed." \
		"FallbackOS" "$output"
}

test_zxfer_get_remote_host_operating_system_falls_back_to_direct_probe_when_capability_payload_has_invalid_helper_path() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf 'ZXFER_REMOTE_CAPS_V1\n'
				printf 'os%sRemoteOS\n' "$tab"
				printf 'tool%szfs%s0%s/remote/bin/zfs%s\n' "$tab" "$tab" "$tab" "$cr"
				printf 'tool%sparallel%s1%s-\n' "$tab" "$tab" "$tab"
				printf 'tool%scat%s1%s-\n' "$tab" "$tab" "$tab"
			}
			zxfer_get_remote_host_operating_system_direct() {
				printf '%s\n' "FallbackOS"
			}
			zxfer_get_remote_host_operating_system "origin.example" source
		)
	)

	assertEquals "Remote OS lookups should fall back to a direct uname probe when the capability payload includes an invalid helper path." \
		"FallbackOS" "$output"
}

test_zxfer_get_remote_host_operating_system_direct_returns_first_output_line() {
	output=$(
		(
			g_zxfer_dependency_path="/secure/bin:/usr/bin"
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s|%s|%s\n' "$1" "$2" "${3:-}" >"$TEST_TMPDIR/remote_os_direct.log"
				printf '%s\n' "MockRemoteOS" "ignored-extra-line"
			}
			zxfer_get_remote_host_operating_system_direct "origin.example" source
		)
	)

	assertEquals "Direct remote OS lookups should return the first line of uname output." \
		"MockRemoteOS" "$output"
	assertContains "Direct remote OS lookups should target the requested host." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "origin.example|"
	assertContains "Direct remote OS lookups should scope the remote probe to the secure dependency path." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "PATH='"
	assertContains "Direct remote OS lookups should run uname through the remote shell wrapper." \
		"$(cat "$TEST_TMPDIR/remote_os_direct.log")" "uname 2>/dev/null"
}

test_zxfer_get_remote_host_operating_system_direct_rejects_empty_output() {
	set +e
	output=$(
		(
			zxfer_invoke_ssh_shell_command_for_host() {
				return 0
			}
			zxfer_get_remote_host_operating_system_direct "origin.example" source
		)
	)
	status=$?

	assertEquals "Direct remote OS lookups should fail when uname returns no output." 1 "$status"
	assertEquals "Failed direct remote OS lookups should not print a payload." "" "$output"
}

test_zxfer_remote_capability_cache_path_returns_failure_when_key_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_remote_capability_cache_key() {
				return 1
			}
			zxfer_remote_capability_cache_path "origin.example"
		)
	)
	status=$?

	assertEquals "Capability cache path lookups should fail cleanly when key generation fails." 1 "$status"
	assertEquals "Capability cache path lookup failures should not produce a payload." "" "$output"
}

test_zxfer_remote_capability_cache_key_falls_back_when_hex_encoding_is_empty() {
	result=$(
		(
			od() {
				:
			}
			zxfer_remote_capability_cache_key "origin.example"
		)
	)

	assertEquals "Capability cache keys should fall back to a sentinel value when hex encoding produces no output." \
		"00" "$result"
}

test_zxfer_remote_capability_cache_key_uses_hex_fallback_in_current_shell() {
	output_file="$TEST_TMPDIR/remote_capability_cache_key.out"

	(
		od() {
			printf '%s\n' " 61 62 63 64"
		}
		zxfer_remote_capability_cache_key "origin.example" >"$output_file"
	)

	assertEquals "Capability cache keys should be derived from the rendered hex fallback when od succeeds." \
		"61626364" "$(cat "$output_file")"
}

test_zxfer_remote_capability_cache_key_uses_path_shadowed_hex_fallback_in_current_shell() {
	fake_bin_dir="$TEST_TMPDIR/remote_capability_cache_key_bin"
	output_file="$TEST_TMPDIR/remote_capability_cache_key_shadowed.out"
	original_path=${PATH:-}

	mkdir -p "$fake_bin_dir"
	cat >"$fake_bin_dir/od" <<'EOF'
#!/bin/sh
printf '%s\n' " 61 62 63 64"
EOF
	chmod +x "$fake_bin_dir/od"

	PATH="$fake_bin_dir:$original_path"
	zxfer_remote_capability_cache_key "origin.example" >"$output_file"
	PATH=$original_path

	assertEquals "Capability cache keys should exercise the rendered hex fallback in the current shell when od is shadowed through PATH." \
		"61626364" "$(cat "$output_file")"
}

test_resolve_remote_required_tool_falls_back_to_direct_probe_when_capability_handshake_fails() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/bin/zfs"
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Remote helper lookups should fall back to the direct secure probe when the capability handshake fails." 0 "$status"
	assertEquals "Capability-handshake fallback should return the direct probe result." \
		"/remote/bin/zfs" "$output"
}

test_resolve_remote_required_tool_falls_back_to_direct_probe_for_malformed_handshake_payload() {
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
				printf '%s\n' "os	RemoteOS"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/bin/zfs"
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Malformed handshake payloads should also fall back to the direct secure probe." 0 "$status"
	assertEquals "Malformed-handshake fallback should return the direct probe result." \
		"/remote/bin/zfs" "$output"
}

test_resolve_remote_required_tool_falls_back_to_direct_probe_for_handshake_payload_with_invalid_helper_path() {
	tab=$(printf '\t')
	cr=$(printf '\r')

	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf 'ZXFER_REMOTE_CAPS_V1\n'
				printf 'os%sRemoteOS\n' "$tab"
				printf 'tool%szfs%s0%s/remote/bin/zfs%s\n' "$tab" "$tab" "$tab" "$cr"
				printf 'tool%sparallel%s1%s-\n' "$tab" "$tab" "$tab"
				printf 'tool%scat%s1%s-\n' "$tab" "$tab" "$tab"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "/remote/direct/zfs"
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Invalid helper paths inside capability payloads should trigger the secure direct-probe fallback." \
		0 "$status"
	assertEquals "Invalid-helper-path fallback should return the direct probe result." \
		"/remote/direct/zfs" "$output"
}

test_resolve_remote_required_tool_propagates_direct_probe_failure_when_capability_handshake_fails() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				return 1
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Remote helper lookups should still fail when both the handshake and direct secure probe fail." 1 "$status"
	assertContains "Capability-handshake fallback failures should preserve the direct probe message." \
		"$output" "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin)."
}

test_resolve_remote_required_tool_propagates_direct_probe_failure_for_malformed_handshake_payload() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				printf '%s\n' "ZXFER_REMOTE_CAPS_V1"
				printf '%s\n' "os\tRemoteOS"
			}
			zxfer_resolve_remote_cli_tool_direct() {
				printf '%s\n' "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
				return 1
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Malformed remote capability payloads should still fail when the direct secure probe also fails." 1 "$status"
	assertContains "Malformed-payload fallback failures should preserve the direct probe message." \
		"$output" "Required dependency \"zfs\" not found on host origin.example in secure PATH (/secure/bin)."
}

test_resolve_remote_required_tool_reports_generic_failure_for_unexpected_tool_status() {
	set +e
	output=$(
		(
			zxfer_ensure_remote_host_capabilities() {
				cat <<'EOF'
ZXFER_REMOTE_CAPS_V1
os	RemoteOS
tool	zfs	2	-
tool	parallel	0	/opt/bin/parallel
tool	cat	0	/remote/bin/cat
EOF
			}
			zxfer_resolve_remote_required_tool "origin.example" zfs "zfs"
		)
	)
	status=$?

	assertEquals "Unexpected handshake tool statuses should fail closed." 1 "$status"
	assertEquals "Unexpected handshake tool statuses should surface the generic dependency query error." \
		"Failed to query dependency \"zfs\" on host origin.example." "$output"
}

test_init_globals_initializes_defaults_and_temp_files() {
	real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
	result=$(
		(
			counter_file="$TEST_TMPDIR/zxfer_init_globals.counter"
			printf '%s\n' 0 >"$counter_file"
			g_zxfer_services_to_restart="stale-service"
			g_zxfer_property_cache_path="/tmp/stale-cache"
			zxfer_get_temp_file() {
				temp_index=$(cat "$counter_file")
				temp_index=$((temp_index + 1))
				printf '%s\n' "$temp_index" >"$counter_file"
				printf '%s\n' "$TEST_TMPDIR/tmp.$temp_index"
			}
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					eval "$1=$(command -v awk 2>/dev/null || printf '%s\n' awk)"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				return 0
			}
			ZXFER_BACKUP_DIR="$TEST_TMPDIR/backup_root"
			zxfer_init_globals
			printf 'awk=%s\n' "$g_cmd_awk"
			printf 'zfs=%s\n' "$g_cmd_zfs"
			printf 'ssh=%s\n' "$g_cmd_ssh"
			printf 'backup=%s\n' "$g_backup_storage_root"
			printf 'control=%s\n' "$g_ssh_supports_control_sockets"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'tmp1=%s\n' "$g_delete_source_tmp_file"
			printf 'tmp2=%s\n' "$g_delete_dest_tmp_file"
			printf 'tmp3=%s\n' "$g_delete_snapshots_to_delete_tmp_file"
			printf 'restart=<%s>\n' "$g_zxfer_services_to_restart"
			printf 'cache_path=<%s>\n' "$g_zxfer_property_cache_path"
		)
	)

	assertContains "zxfer_init_globals should resolve awk through the helper." "$result" "awk=$real_awk"
	assertContains "zxfer_init_globals should resolve zfs through the helper." "$result" "zfs=/stub/zfs"
	assertContains "zxfer_init_globals should resolve ssh through the helper." "$result" "ssh=/stub/ssh"
	assertContains "zxfer_init_globals should honor ZXFER_BACKUP_DIR when set." "$result" "backup=$TEST_TMPDIR/backup_root"
	assertContains "zxfer_init_globals should enable control sockets when ssh supports them." "$result" "control=1"
	assertContains "Yield iterations should default to 1." "$result" "yield=1"
	assertContains "Delete source temp file should be initialized." "$result" "tmp1=$TEST_TMPDIR/tmp.1"
	assertContains "Delete destination temp file should be initialized." "$result" "tmp2=$TEST_TMPDIR/tmp.2"
	assertContains "Delete diff temp file should be initialized." "$result" "tmp3=$TEST_TMPDIR/tmp.3"
	assertContains "Runtime init should clear stale service restart state." "$result" "restart=<>"
	assertContains "Runtime init should clear stale property-cache path state." "$result" "cache_path=<>"
}

test_zxfer_find_required_tool_reports_missing_dependency() {
	empty_path="$TEST_TMPDIR/empty_path"
	mkdir -p "$empty_path"
	g_zxfer_secure_path="$empty_path"
	g_zxfer_dependency_path="$empty_path"

	set +e
	result=$(zxfer_find_required_tool definitely_missing "missing-tool")
	status=$?

	assertEquals "Missing dependencies should fail lookup." 1 "$status"
	assertEquals "Missing dependencies should mention the secure PATH guidance." \
		"Required dependency \"missing-tool\" not found in secure PATH ($empty_path). Set ZXFER_SECURE_PATH or install the binary." \
		"$result"
}

test_zxfer_find_required_tool_rejects_relative_resolution() {
	set +e
	result=$(
		(
			mocktool() {
				:
			}
			g_zxfer_secure_path="$ZXFER_DEFAULT_SECURE_PATH"
			g_zxfer_dependency_path="$ZXFER_DEFAULT_SECURE_PATH"
			zxfer_find_required_tool mocktool "mocktool"
		)
	)
	status=$?

	assertEquals "Relative command -v results should be rejected." 1 "$status"
	assertEquals "Relative paths should be rejected explicitly." \
		"Required dependency \"mocktool\" resolved to \"mocktool\", but zxfer requires an absolute path." \
		"$result"
}

test_zxfer_find_required_tool_returns_absolute_path_from_secure_path() {
	tool_dir="$TEST_TMPDIR/required_tool_path"
	mkdir -p "$tool_dir"
	cat >"$tool_dir/mocktool" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$tool_dir/mocktool"
	g_zxfer_secure_path="$tool_dir"
	g_zxfer_dependency_path="$tool_dir"

	result=$(zxfer_find_required_tool mocktool "mocktool")

	assertEquals "Required tool lookup should return the resolved absolute path from the secure PATH." \
		"$tool_dir/mocktool" "$result"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace() {
	tab=$(printf '\t')

	set +e
	result=$(zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool")
	status=$?

	assertEquals "Resolved tool paths with control whitespace should be rejected." 1 "$status"
	assertContains "Rejected tool paths should explain the control-whitespace requirement." \
		"$result" "single-line absolute path without control whitespace"
}

test_zxfer_validate_resolved_tool_path_rejects_control_whitespace_with_scope() {
	tab=$(printf '\t')

	set +e
	result=$(zxfer_validate_resolved_tool_path "/tmp/mock${tab}tool" "mocktool" "host origin.example")
	status=$?

	assertEquals "Scoped control-whitespace tool paths should be rejected." 1 "$status"
	assertContains "Scoped control-whitespace failures should mention the host scope." \
		"$result" "Required dependency \"mocktool\" on host origin.example resolved to"
}

test_zxfer_assign_required_tool_marks_dependency_failures() {
	set +e
	output=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_assign_required_tool g_cmd_test mocktool "mocktool"
		)
	)
	status=$?

	assertEquals "zxfer_assign_required_tool should abort when lookup fails." 1 "$status"
	assertContains "Dependency lookup failures should be classified correctly." "$output" "class=dependency"
	assertContains "Dependency lookup failures should preserve the lookup message." "$output" "msg=lookup failed"
}

test_zxfer_assign_required_tool_sets_target_variable_on_success() {
	result=$(
		(
			zxfer_find_required_tool() {
				printf '%s\n' "/opt/mock/mocktool"
			}
			g_cmd_mock=""
			zxfer_assign_required_tool g_cmd_mock mocktool "mocktool"
			printf '%s\n' "$g_cmd_mock"
		)
	)

	assertEquals "Successful tool assignment should populate the requested variable." "/opt/mock/mocktool" "$result"
}

test_init_globals_rejects_control_whitespace_in_optional_parallel_path() {
	tab=$(printf '\t')
	parallel_dir="$TEST_TMPDIR/parallel${tab}bin"
	mkdir -p "$parallel_dir"
	cat >"$parallel_dir/parallel" <<'EOF'
#!/bin/sh
printf '%s\n' "GNU parallel (fake)"
exit 0
EOF
	chmod +x "$parallel_dir/parallel"

	set +e
	output=$(
		(
			g_zxfer_dependency_path="$parallel_dir"
			zxfer_assign_required_tool() {
				if [ "$2" = "awk" ]; then
					# shellcheck disable=SC2034
					l_real_awk=$(command -v awk 2>/dev/null || printf '%s\n' awk)
					eval "$1=\$l_real_awk"
				else
					eval "$1=/stub/$2"
				fi
			}
			zxfer_ssh_supports_control_sockets() {
				return 1
			}
			zxfer_get_temp_file() {
				printf '%s\n' "$TEST_TMPDIR/tmp"
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			zxfer_init_globals
		)
	)
	status=$?

	assertEquals "zxfer_init_globals should fail when optional GNU parallel resolves to a path with control whitespace." 1 "$status"
	assertContains "Invalid optional parallel resolutions should be classified as dependency failures." \
		"$output" "class=dependency"
	assertContains "Invalid optional parallel resolutions should explain the path validation failure." \
		"$output" "single-line absolute path without control whitespace"
}

test_extract_snapshot_identity_returns_empty_for_non_snapshot_path() {
	result=$(zxfer_extract_snapshot_identity "tank/src")

	assertEquals "Snapshot identities should be empty when the record does not include a snapshot suffix." \
		"" "$result"
}

test_extract_snapshot_dataset_and_guid_detection_helpers() {
	assertEquals "Snapshot dataset extraction should strip the snapshot suffix from guid-bearing records." \
		"tank/src" "$(zxfer_extract_snapshot_dataset "tank/src@snap1	123")"
	assertEquals "Snapshot dataset extraction should return empty for non-snapshot records." \
		"" "$(zxfer_extract_snapshot_dataset "tank/src")"
	assertTrue "Guid detection should report true when a snapshot record includes a guid field." \
		'zxfer_snapshot_record_list_contains_guid "tank/src@snap1	123"'
	assertFalse "Guid detection should report false for name-only snapshot records." \
		'zxfer_snapshot_record_list_contains_guid "tank/src@snap1"'
}

test_zxfer_reverse_snapshot_record_list_and_name_overlap_helpers() {
	reversed=$(zxfer_reverse_snapshot_record_list "tank/src@snap1	111
tank/src@snap2	222
tank/src@snap3	333")

	assertEquals "Snapshot-record reversal should preserve full records while reversing their order." \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" "$reversed"

	set +e
	zxfer_snapshot_record_lists_share_snapshot_name "tank/src@snap2
tank/src@snap1" "backup/dst@snap9
backup/dst@snap1"
	status=$?
	assertEquals "Snapshot-name overlap detection should succeed when both sides share any snapshot name." \
		0 "$status"

	zxfer_snapshot_record_lists_share_snapshot_name "tank/src@snap2
tank/src@snap1" "backup/dst@other"
	status=$?
	assertEquals "Snapshot-name overlap detection should fail when the lists do not share any snapshot name." \
		1 "$status"
}

test_zxfer_filter_snapshot_identity_records_to_reference_paths_preserves_identity_order() {
	result=$(zxfer_filter_snapshot_identity_records_to_reference_paths \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" \
		"tank/src@snap2
tank/src@snap1")

	assertEquals "Reference-path filtering should keep only matching identity records in their original identity-record order." \
		"tank/src@snap2	222
tank/src@snap1	111" "$result"
}

test_zxfer_get_source_snapshot_identity_records_for_dataset_reverses_creation_order() {
	result=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' \
					"tank/src@snap1	111" \
					"tank/src@snap2	222" \
					"tank/src@snap3	333"
			}

			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src"
		)
	)

	assertEquals "Source identity-record retrieval should reverse creation-ordered zfs output into newest-first order." \
		"tank/src@snap3	333
tank/src@snap2	222
tank/src@snap1	111" "$result"
}

test_zxfer_get_destination_snapshot_identity_records_for_dataset_filters_descendants() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' \
					"backup/dst@snap1	111" \
					"backup/dst/child@snap1	211" \
					"backup/dst@snap2	222"
			}

			zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst"
		)
	)

	assertEquals "Destination identity-record retrieval should keep only the exact dataset snapshots and drop descendant records." \
		"backup/dst@snap1	111
backup/dst@snap2	222" "$result"
}

test_zxfer_get_snapshot_identity_records_for_dataset_dispatches_and_filters_reference_records() {
	result=$(
		(
			zxfer_get_source_snapshot_identity_records_for_dataset() {
				printf '%s\n' \
					"tank/src@snap3	333" \
					"tank/src@snap2	222" \
					"tank/src@snap1	111"
			}
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n' \
					"backup/dst@snap2	222" \
					"backup/dst@snap1	111"
			}

			zxfer_get_snapshot_identity_records_for_dataset source "tank/src" "tank/src@snap2
tank/src@snap1"
		)
	)

	assertEquals "Generic identity-record lookup should dispatch to the requested side and honor reference-path filtering." \
		"tank/src@snap2	222
tank/src@snap1	111" "$result"

	set +e
	output=$(
		(
			zxfer_get_snapshot_identity_records_for_dataset invalid "tank/src"
		)
	)
	status=$?

	assertEquals "Generic identity-record lookup should reject unknown lookup sides." 1 "$status"
	assertEquals "Rejected identity-record lookups should not emit an output payload." "" "$output"
}

test_zxfer_snapshot_identity_record_helpers_report_lookup_failures_and_destination_dispatch() {
	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				return 1
			}

			zxfer_get_source_snapshot_identity_records_for_dataset "tank/src"
		)
	)
	status=$?
	assertEquals "Source identity-record lookup should fail cleanly when the zfs query fails." 1 "$status"
	assertEquals "Failed source identity lookups should not emit a payload." "" "$output"

	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				return 1
			}

			zxfer_get_destination_snapshot_identity_records_for_dataset "backup/dst"
		)
	)
	status=$?
	assertEquals "Destination identity-record lookup should fail cleanly when the zfs query fails." 1 "$status"
	assertEquals "Failed destination identity lookups should not emit a payload." "" "$output"

	output=$(
		(
			zxfer_get_destination_snapshot_identity_records_for_dataset() {
				printf '%s\n' "backup/dst@snap2	222"
			}

			zxfer_get_snapshot_identity_records_for_dataset destination "backup/dst"
		)
	)
	status=$?
	assertEquals "Generic identity-record lookup should support the destination side without requiring reference filters." 0 "$status"
	assertEquals "Destination-side identity dispatch should return the destination helper payload unchanged when no reference filter is supplied." \
		"backup/dst@snap2	222" "$output"
}

test_read_command_line_switches_sets_options_and_remote_paths() {
	log="$TEST_TMPDIR/read_switches.log"
	: >"$log"
	result=$(
		(
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_refresh_compression_commands() {
				printf 'refresh\n' >>"$log"
				g_cmd_compress_safe="zstd -9"
				g_cmd_decompress_safe="zstd -d"
			}
			g_ssh_supports_control_sockets=1
			g_cmd_zfs="/sbin/zfs"
			g_test_max_yield_iterations=8
			OPTIND=1
			zxfer_read_command_line_switches \
				-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
				-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
				-N "tank/nonrecursive" -o "atime=off" -O "origin.example pfexec" \
				-P -R "tank/src" -s -T "target.example doas" -U -v -V -w \
				-x "child" -Y -z -Z "zstd -9"
			printf 'origin=%s\n' "$g_option_O_origin_host"
			printf 'target=%s\n' "$g_option_T_target_host"
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'jobs=%s\n' "$g_option_j_jobs"
			printf 'yield=%s\n' "$g_option_Y_yield_iterations"
			printf 'compress=%s\n' "$g_cmd_compress"
			printf 'props=%s\n' "$g_option_P_transfer_property"
			printf 'verbose=%s/%s\n' "$g_option_v_verbose" "$g_option_V_very_verbose"
		)
	)

	assertContains "Origin host should be recorded from -O." "$result" "origin=origin.example pfexec"
	assertContains "Target host should be recorded from -T." "$result" "target=target.example doas"
	assertContains "Origin zfs spec should remain the resolved zfs path until remote execution is rendered." "$result" "lzfs=/sbin/zfs"
	assertContains "Target zfs spec should remain the resolved zfs path until remote execution is rendered." "$result" "rzfs=/sbin/zfs"
	assertContains "Parallel job count should come from -j." "$result" "jobs=4"
	assertContains "Yield iterations should expand to the max when -Y is set." "$result" "yield=8"
	assertContains "Custom compression should be recorded from -Z." "$result" "compress=zstd -9"
	assertContains "Property transfer should be enabled by -e/-k/-m/-P." "$result" "props=1"
	assertContains "Very verbose mode should imply verbose mode." "$result" "verbose=1/1"
	assertContains "Compression refresh should run after parsing options." "$(cat "$log")" "refresh"
}

test_prepare_remote_host_connections_sets_up_control_sockets_after_validation() {
	log="$TEST_TMPDIR/prepare_remote_hosts.log"
	now_counter_file="$TEST_TMPDIR/prepare_remote_hosts.now.counter"
	: >"$log"
	printf '%s\n' 0 >"$now_counter_file"

	result=$(
		(
			zxfer_setup_ssh_control_socket() {
				printf 'setup %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			zxfer_profile_now_ms() {
				idx=$(cat "$now_counter_file")
				idx=$((idx + 1))
				printf '%s\n' "$idx" >"$now_counter_file"
				if [ "$idx" = "1" ]; then
					printf '%s\n' 1000
				elif [ "$idx" = "2" ]; then
					printf '%s\n' 1250
				fi
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_option_V_very_verbose=1
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=1
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
			printf 'ssh_setup_ms=%s\n' "${g_zxfer_profile_ssh_setup_ms:-0}"
		)
	)

	assertContains "Origin control socket setup should happen during remote preparation." \
		"$(cat "$log")" "setup origin.example pfexec origin"
	assertContains "Target control socket setup should happen during remote preparation." \
		"$(cat "$log")" "setup target.example doas target"
	assertContains "Origin capability discovery should be preloaded once sockets are ready." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Target capability discovery should be preloaded once sockets are ready." \
		"$(cat "$log")" "preload target.example doas destination"
	assertContains "Origin zfs spec should refresh to the resolved origin helper path." \
		"$result" "lzfs=/remote/origin/zfs"
	assertContains "Target zfs spec should refresh to the resolved target helper path." \
		"$result" "rzfs=/remote/target/zfs"
	assertContains "Very-verbose remote preparation should accumulate ssh setup timing." \
		"$result" "ssh_setup_ms=250"
}

test_prepare_remote_host_connections_logs_when_control_sockets_are_unavailable() {
	log="$TEST_TMPDIR/prepare_remote_hosts_no_mux.log"
	: >"$log"

	output=$(
		(
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_preload_remote_host_capabilities() {
				printf 'preload %s %s\n' "$1" "$2" >>"$log"
			}
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
			g_option_O_origin_host="origin.example pfexec"
			g_option_T_target_host="target.example doas"
			g_cmd_zfs="/sbin/zfs"
			g_origin_cmd_zfs="/remote/origin/zfs"
			g_target_cmd_zfs="/remote/target/zfs"
			g_ssh_supports_control_sockets=0
			zxfer_prepare_remote_host_connections
			printf 'lzfs=%s\n' "$g_LZFS"
			printf 'rzfs=%s\n' "$g_RZFS"
		)
	)

	assertContains "Origin preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for origin host."
	assertContains "Target preparation should explain when ssh control sockets are unavailable." \
		"$output" "ssh client does not support control sockets; continuing without connection reuse for target host."
	assertContains "Origin capability discovery should still be preloaded without control sockets." \
		"$(cat "$log")" "preload origin.example pfexec source"
	assertContains "Target capability discovery should still be preloaded without control sockets." \
		"$(cat "$log")" "preload target.example doas destination"
	assertContains "Remote zfs specs should still refresh even without control socket support." \
		"$output" "lzfs=/remote/origin/zfs"
	assertContains "Remote zfs specs should still refresh target commands even without control socket support." \
		"$output" "rzfs=/remote/target/zfs"
}

test_read_command_line_switches_sets_flags_in_current_shell() {
	OPTIND=1
	g_cmd_ssh="/usr/bin/ssh"
	g_cmd_zfs="/sbin/zfs"
	g_test_max_yield_iterations=9
	g_ssh_supports_control_sockets=0
	zxfer_refresh_compression_commands() {
		:
	}

	zxfer_read_command_line_switches \
		-b -B -c "svc:/network/nfs/server" -d -D "pv -N %%title%%" \
		-e -F -g 7 -I "mountpoint" -j 4 -k -m -n \
		-N "tank/nonrecursive" -o "atime=off" -V \
		-O "origin.example pfexec" -P -R "tank/src" -s \
		-T "target.example doas" -U -w -x "child" -Y -z -Z "zstd -9"

	assertEquals "Beep-always should be enabled by -b." "1" "$g_option_b_beep_always"
	assertEquals "Beep-on-success should be enabled by -B." "1" "$g_option_B_beep_on_success"
	assertEquals "Service list should be captured from -c." "svc:/network/nfs/server" "$g_option_c_services"
	assertEquals "Snapshot deletion should be enabled by -d." "1" "$g_option_d_delete_destination_snapshots"
	assertEquals "Progress display command should be captured from -D." "pv -N %%title%%" "$g_option_D_display_progress_bar"
	assertEquals "Grandfather protection should be captured from -g." "7" "$g_option_g_grandfather_protection"
	assertEquals "Ignore-properties list should be captured from -I." "mountpoint" "$g_option_I_ignore_properties"
	assertEquals "Parallel job count should be captured from -j." "4" "$g_option_j_jobs"
	assertEquals "Nonrecursive source should be captured from -N." "tank/nonrecursive" "$g_option_N_nonrecursive"
	assertEquals "Override property should be captured from -o." "atime=off" "$g_option_o_override_property"
	# zxfer_read_command_line_switches runs in the current shell here; the SC2031
	# warning is triggered by separate subshell-based coverage elsewhere.
	# shellcheck disable=SC2031
	assertEquals "Origin host should be captured from -O." "origin.example pfexec" "$g_option_O_origin_host"
	assertEquals "Recursive source should be captured from -R." "tank/src" "$g_option_R_recursive"
	# shellcheck disable=SC2031
	assertEquals "Target host should be captured from -T." "target.example doas" "$g_option_T_target_host"
	assertEquals "Exclude list should be captured from -x." "child" "$g_option_x_exclude_datasets"
	assertEquals "Very-verbose mode should imply verbose mode." "1" "$g_option_v_verbose"
	assertEquals "Very-verbose mode should be enabled by -V." "1" "$g_option_V_very_verbose"
	assertEquals "Raw-send mode should be enabled by -w." "1" "$g_option_w_raw_send"
	assertEquals "Unsupported-property skipping should be enabled by -U." "1" "$g_option_U_skip_unsupported_properties"
	assertEquals "Compression should be enabled by -z/-Z." "1" "$g_option_z_compress"
	assertEquals "Yield iterations should expand to the configured maximum." "9" "$g_option_Y_yield_iterations"
	assertEquals "The parser should preserve the custom compression command from -Z." "zstd -9" "$g_cmd_compress"
	assertEquals "Property transfer should be enabled by property-affecting switches." "1" "$g_option_P_transfer_property"
	assertEquals "Origin zfs spec should remain the resolved zfs path after parsing." \
		"/sbin/zfs" "$g_LZFS"
	assertEquals "Target zfs spec should remain the resolved zfs path after parsing." \
		"/sbin/zfs" "$g_RZFS"

	unset -f zxfer_refresh_compression_commands
	# shellcheck source=src/zxfer_modules.sh
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_backup_metadata.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"
}

test_read_command_line_switches_rejects_invalid_option() {
	set +e
	output=$(
		(
			zxfer_refresh_compression_commands() {
				:
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			OPTIND=1
			zxfer_read_command_line_switches -Q 2>/dev/null
		)
	)
	status=$?

	assertEquals "Invalid options should exit with usage status." 2 "$status"
	assertContains "Invalid options should use the generic usage error." "$output" "Invalid option provided."
}

test_read_command_line_switches_exits_zero_for_help() {
	set +e
	output=$(
		(
			zxfer_usage() {
				printf '%s\n' "usage output"
			}
			OPTIND=1
			zxfer_read_command_line_switches -h
			printf '%s\n' "after-help"
		)
	)
	status=$?

	assertEquals "The help switch should exit successfully." 0 "$status"
	assertEquals "The help switch should print usage and stop parsing immediately." "usage output" "$output"
}

test_consistency_check_rejects_non_numeric_jobs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=abc
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Non-numeric job counts should fail validation." 2 "$status"
	assertContains "The validation error should mention the invalid job count." \
		"$output" "The -j option requires a positive integer job count"
}

test_consistency_check_rejects_zero_jobs() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_j_jobs=0
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Zero job counts should fail validation." 2 "$status"
	assertContains "The validation error should require at least one job." \
		"$output" "requires a job count of at least 1"
}

test_consistency_check_rejects_remote_migration_conflicts() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_O_origin_host="origin.example"
			g_option_m_migrate=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Remote migration should be rejected." 2 "$status"
	assertContains "Remote migration conflicts should use the documented error." \
		"$output" "You cannot migrate to or from a remote host."
}

test_consistency_check_rejects_compression_without_remote_host() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_z_compress=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Compression without -O/-T should be rejected." 2 "$status"
	assertContains "Compression validation should point to the missing remote host." \
		"$output" "-z option can only be used with -O or -T option"
}

test_init_variables_uses_gawk_on_sunos_when_available() {
	gawk_dir="$TEST_TMPDIR/gawk_path"
	mkdir -p "$gawk_dir"
	cat >"$gawk_dir/gawk" <<'EOF'
#!/bin/sh
exit 0
EOF
	chmod +x "$gawk_dir/gawk"

	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "SunOS"
			}
			g_cmd_zfs="/sbin/zfs"
			g_cmd_awk="/usr/bin/awk"
			g_zxfer_dependency_path="$gawk_dir"
			zxfer_init_variables
			printf '%s\n' "$g_cmd_awk"
		)
	)

	assertEquals "SunOS initialization should prefer gawk when it is available." "$gawk_dir/gawk" "$result"
}

test_init_variables_uses_local_cat_lookup_in_restore_mode() {
	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "FreeBSD"
			}
			zxfer_assign_required_tool() {
				if [ "$2" = "cat" ]; then
					eval "$1=/bin/cat"
				else
					eval "$1=/stub/$2"
				fi
			}
			g_option_e_restore_property_mode=1
			zxfer_init_variables
			printf 'cat=%s\n' "$g_cmd_cat"
		)
	)

	assertContains "Restore mode on the local host should resolve cat through the required-tool helper." \
		"$result" "cat=/bin/cat"
}

test_refresh_compression_commands_resolves_local_helpers_when_enabled() {
	result=$(
		(
			zxfer_find_required_tool() {
				if [ "$1" = "zstd" ]; then
					printf '%s\n' "/secure/bin/zstd"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
			printf 'compress=%s\n' "$g_cmd_compress_safe"
			printf 'decompress=%s\n' "$g_cmd_decompress_safe"
		)
	)

	assertContains "Enabled compression should resolve the compressor head token through the secure local path." \
		"$result" "compress='/secure/bin/zstd' '-T0' '-9'"
	assertContains "Enabled compression should resolve the decompressor head token through the secure local path." \
		"$result" "decompress='/secure/bin/zstd' '-d'"
}

test_zxfer_resolve_remote_cli_command_safe_resolves_first_token_and_preserves_args() {
	result=$(
		(
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				printf '%s\n' "/remote/bin/zstd"
			}
			zxfer_resolve_remote_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source
		)
	)

	assertEquals "Remote CLI command resolution should replace only the first token and keep the remaining arguments intact." \
		"'/remote/bin/zstd' '-T0' '-9'" "$result"
}

test_zxfer_resolve_local_cli_command_safe_rejects_blank_commands_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_local_cli_blank.out"

	(
		zxfer_resolve_local_cli_command_safe "   " "compression command" >"$output_file"
	)
	status=$?

	assertEquals "Blank local CLI commands should be rejected." 1 "$status"
	assertContains "Blank local CLI command failures should use the documented validation message." \
		"$(cat "$output_file")" "Required dependency \"compression command\" must not be empty or whitespace-only."
}

test_zxfer_resolve_local_cli_command_safe_surfaces_lookup_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_local_cli_lookup_failure.out"

	(
		zxfer_find_required_tool() {
			printf '%s\n' "missing helper"
			return 1
		}
		zxfer_resolve_local_cli_command_safe "zstd -T0 -9" "compression command" >"$output_file"
	)
	status=$?

	assertEquals "Local CLI command resolution should fail when the head token cannot be resolved." 1 "$status"
	assertEquals "Local CLI command resolution should surface the dependency lookup failure verbatim." \
		"missing helper" "$(cat "$output_file")"
}

test_zxfer_resolve_remote_cli_tool_delegates_known_tools_in_current_shell() {
	output_file="$TEST_TMPDIR/resolve_remote_cli_tool_known.out"
	log_file="$TEST_TMPDIR/resolve_remote_cli_tool_known.log"

	(
		zxfer_resolve_remote_required_tool() {
			printf '%s:%s:%s:%s\n' "$1" "$2" "$3" "$4" >"$log_file"
			printf '%s\n' "/remote/bin/zfs"
		}
		zxfer_resolve_remote_cli_tool "origin.example" "zfs" "source zfs" source >"$output_file"
	)
	status=$?

	assertEquals "Known remote CLI tools should delegate to zxfer_resolve_remote_required_tool." 0 "$status"
	assertEquals "Known remote CLI tool delegation should preserve the host, tool, label, and profile side." \
		"origin.example:zfs:source zfs:source" "$(cat "$log_file")"
	assertEquals "Known remote CLI tool delegation should return the resolved remote helper path." \
		"/remote/bin/zfs" "$(cat "$output_file")"
}

test_zxfer_resolve_remote_cli_tool_reports_missing_and_query_failures_in_current_shell() {
	missing_output="$TEST_TMPDIR/resolve_remote_cli_tool_missing.out"
	error_output="$TEST_TMPDIR/resolve_remote_cli_tool_error.out"

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			return 10
		}
		g_zxfer_dependency_path="/secure/bin"
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$missing_output"
	)
	missing_status=$?

	(
		zxfer_build_remote_sh_c_command() {
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			return 77
		}
		g_zxfer_dependency_path="/secure/bin"
		zxfer_resolve_remote_cli_tool "origin.example" "zstd" "compression command" source >"$error_output"
	)
	error_status=$?

	assertEquals "Missing remote CLI tools should return failure." 1 "$missing_status"
	assertContains "Missing remote CLI tools should use the documented secure-PATH guidance." \
		"$(cat "$missing_output")" "Required dependency \"compression command\" not found on host origin.example in secure PATH (/secure/bin)."
	assertEquals "Remote CLI probe errors should return failure." 1 "$error_status"
	assertContains "Remote CLI probe errors should use the documented generic failure message." \
		"$(cat "$error_output")" "Failed to query dependency \"compression command\" on host origin.example."
}

test_zxfer_resolve_remote_cli_command_safe_rejects_blank_commands_and_surfaces_lookup_failures_in_current_shell() {
	blank_output="$TEST_TMPDIR/resolve_remote_cli_blank.out"
	lookup_output="$TEST_TMPDIR/resolve_remote_cli_lookup.out"

	(
		zxfer_resolve_remote_cli_command_safe "origin.example" "   " "compression command" source >"$blank_output"
	)
	blank_status=$?

	(
		zxfer_resolve_remote_cli_tool() {
			printf '%s\n' "remote helper lookup failed"
			return 1
		}
		zxfer_resolve_remote_cli_command_safe "origin.example" "zstd -T0 -9" "compression command" source >"$lookup_output"
	)
	lookup_status=$?

	assertEquals "Blank remote CLI commands should be rejected." 1 "$blank_status"
	assertContains "Blank remote CLI command failures should use the documented validation message." \
		"$(cat "$blank_output")" "Required dependency \"compression command\" must not be empty or whitespace-only."
	assertEquals "Remote CLI command resolution should fail when the head token cannot be resolved." 1 "$lookup_status"
	assertEquals "Remote CLI command resolution should surface the remote helper lookup failure verbatim." \
		"remote helper lookup failed" "$(cat "$lookup_output")"
}

test_init_variables_resolves_remote_compression_helpers() {
	result=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "/remote/target/zfs"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				if [ "$1:$2" = "origin.example:zstd -T0 -9" ]; then
					printf '%s\n' "'/remote/origin/zstd' '-T0' '-9'"
				elif [ "$1:$2" = "target.example:zstd -d" ]; then
					printf '%s\n' "'/remote/target/zstd' '-d'"
				else
					printf '%s\n' "unexpected compression command"
					return 1
				fi
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
			printf 'origin-compress=%s\n' "$g_origin_cmd_compress_safe"
			printf 'origin-decompress=%s\n' "$g_origin_cmd_decompress_safe"
			printf 'target-compress=%s\n' "$g_target_cmd_compress_safe"
			printf 'target-decompress=%s\n' "$g_target_cmd_decompress_safe"
		)
	)

	assertContains "Origin initialization should resolve the remote compression helper." \
		"$result" "origin-compress='/remote/origin/zstd' '-T0' '-9'"
	assertContains "Origin initialization should leave the unused remote decompression helper on the local safe default." \
		"$result" "origin-decompress='/local/bin/zstd' '-d'"
	assertContains "Target initialization should leave the unused remote compression helper on the local safe default." \
		"$result" "target-compress='/local/bin/zstd' '-T0' '-9'"
	assertContains "Target initialization should resolve the remote decompression helper." \
		"$result" "target-decompress='/remote/target/zstd' '-d'"
}

test_init_variables_marks_remote_compression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				else
					printf '%s\n' "unexpected tool"
					return 1
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "remote compression lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -T0 -9"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-T0' '-9'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote compression lookup failures should abort initialization." 1 "$status"
	assertContains "Remote compression lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote compression lookup failures should preserve the failing message." \
		"$output" "msg=remote compression lookup failed"
}

test_init_variables_marks_remote_target_zfs_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "target zfs lookup failed"
					return 1
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Target-side remote zfs lookup failures should abort initialization." 1 "$status"
	assertContains "Target-side remote zfs lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Target-side remote zfs lookup failures should preserve the failing message." \
		"$output" "msg=target zfs lookup failed"
}

test_init_variables_marks_remote_source_os_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote source OS lookup failures should abort initialization." 1 "$status"
	assertContains "Remote source OS lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote source OS lookup failures should use the documented host-scoped message." \
		"$output" "msg=Failed to determine operating system on host origin.example."
}

test_init_variables_marks_remote_destination_os_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				if [ "$1" = "target.example" ]; then
					return 1
				fi
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				printf '%s\n' "/resolved/$2"
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote destination OS lookup failures should abort initialization." 1 "$status"
	assertContains "Remote destination OS lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote destination OS lookup failures should use the documented host-scoped message." \
		"$output" "msg=Failed to determine operating system on host target.example."
}

test_init_variables_marks_remote_target_decompression_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "target.example:zfs" ]; then
					printf '%s\n' "/remote/target/zfs"
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_resolve_remote_cli_command_safe() {
				printf '%s\n' "target decompression lookup failed"
				return 1
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="zstd -d"
			g_cmd_compress_safe="'/local/bin/zstd' '-3'"
			g_cmd_decompress_safe="'/local/bin/zstd' '-d'"
			g_option_O_origin_host="origin.example"
			g_option_T_target_host="target.example"
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Target-side remote decompression lookup failures should abort initialization." 1 "$status"
	assertContains "Target-side remote decompression lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Target-side remote decompression lookup failures should preserve the failing message." \
		"$output" "msg=target decompression lookup failed"
}

test_init_variables_marks_remote_restore_cat_lookup_failures_as_dependency_errors() {
	set +e
	output=$(
		(
			zxfer_get_os() {
				printf '%s\n' "RemoteOS"
			}
			zxfer_resolve_remote_required_tool() {
				if [ "$1:$2" = "origin.example:zfs" ]; then
					printf '%s\n' "/remote/origin/zfs"
				elif [ "$1:$2" = "origin.example:cat" ]; then
					printf '%s\n' "remote cat lookup failed"
					return 1
				else
					printf '%s\n' "/resolved/$2"
				fi
			}
			zxfer_throw_error() {
				printf 'class=%s msg=%s\n' "$g_zxfer_failure_class" "$1"
				exit 1
			}
			g_option_O_origin_host="origin.example"
			g_option_e_restore_property_mode=1
			zxfer_init_variables
		)
	)
	status=$?

	assertEquals "Remote restore-mode cat lookup failures should abort initialization." 1 "$status"
	assertContains "Remote restore-mode cat lookup failures should be classified as dependency errors." \
		"$output" "class=dependency"
	assertContains "Remote restore-mode cat lookup failures should preserve the failing message." \
		"$output" "msg=remote cat lookup failed"
}

test_refresh_compression_commands_rejects_empty_compression_command() {
	set +e
	output=$(
		(
			zxfer_quote_cli_tokens() {
				if [ "$1" = "" ]; then
					printf '%s' ""
				else
					printf "'%s'\n" "$1"
				fi
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress=""
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when the configured compression command is empty." 2 "$status"
	assertContains "Empty compression commands should use the documented usage error." \
		"$output" "Compression command (-Z/ZXFER_COMPRESSION) cannot be empty."
}

test_refresh_compression_commands_rejects_whitespace_only_compression_command() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit "${2:-2}"
			}
			g_option_z_compress=1
			g_cmd_compress="   "
			g_cmd_decompress="zstd -d"
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only compression commands as empty." 2 "$status"
	assertContains "Whitespace-only compression commands should use the documented usage error." \
		"$output" "Compression command (-Z/ZXFER_COMPRESSION) cannot be empty."
}

test_refresh_compression_commands_rejects_missing_decompress_command() {
	set +e
	output=$(
		(
			zxfer_quote_cli_tokens() {
				if [ "$1" = "zstd -3" ]; then
					printf '%s\n' "'zstd' '-3'"
				else
					printf '%s' ""
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress=""
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should fail when no decompressor can be derived." 1 "$status"
	assertContains "Missing decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_refresh_compression_commands_rejects_whitespace_only_decompress_command() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_z_compress=1
			g_cmd_compress="zstd -3"
			g_cmd_decompress="   "
			zxfer_refresh_compression_commands
		)
	)
	status=$?

	assertEquals "Compression validation should treat whitespace-only decompression commands as missing." 1 "$status"
	assertContains "Whitespace-only decompression commands should use the documented runtime error." \
		"$output" "Compression requested but decompression command missing."
}

test_close_origin_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_origin.log"
	socket_dir="$TEST_TMPDIR/origin_socket_dir"
	mkdir -p "$socket_dir"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_origin_control_socket_dir="$socket_dir"

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Origin socket path should be cleared after closing." "" "$g_ssh_origin_control_socket"
	assertEquals "Origin socket directory should be cleared after closing." "" "$g_ssh_origin_control_socket_dir"
	assertFalse "Origin socket directory should be removed during cleanup." "[ -d \"$socket_dir\" ]"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-S
$TEST_TMPDIR/origin.sock
-O
exit
origin.example
pfexec" "$(cat "$log")"
}

test_get_path_owner_uid_falls_back_to_ls_for_dash_prefixed_paths() {
	result=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >"-owner_file"
			chmod 600 "./-owner_file"
			stat() {
				return 1
			}
			zxfer_get_path_owner_uid "-owner_file"
		)
	)

	assertEquals "LS fallback should recover the owner for dash-prefixed paths." "$(id -u)" "$result"
}

test_get_path_mode_octal_falls_back_to_ls_for_dash_prefixed_paths() {
	result=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >"-mode_file"
			chmod 600 "./-mode_file"
			stat() {
				return 1
			}
			ls() {
				printf '%s\n' "-rw------- 1 0 0 0 Jan 1 00:00 ./-mode_file"
			}
			zxfer_get_path_mode_octal "-mode_file"
		)
	)

	assertEquals "LS fallback should recover 0600 permissions for dash-prefixed paths." "600" "$result"
}

test_merge_path_allowlists_deduplicates_entries() {
	result=$(zxfer_merge_path_allowlists "/sbin:/bin:/usr/bin" "/bin:/usr/local/bin:/usr/bin")

	assertEquals "Merged PATH allowlists should keep first-seen ordering and drop duplicates." \
		"/sbin:/bin:/usr/bin:/usr/local/bin" "$result"
}

test_zxfer_apply_secure_path_exports_runtime_path() {
	result=$(
		(
			ZXFER_SECURE_PATH="/opt/zfs/bin:/usr/sbin"
			ZXFER_SECURE_PATH_APPEND="/custom/bin"
			zxfer_apply_secure_path
			printf 'secure=%s\n' "$g_zxfer_secure_path"
			printf 'runtime=%s\n' "$g_zxfer_runtime_path"
			printf 'path=%s\n' "$PATH"
		)
	)

	assertContains "zxfer_apply_secure_path should honor the configured secure PATH." \
		"$result" "secure=/opt/zfs/bin:/usr/sbin:/custom/bin"
	assertContains "Runtime PATH should append the built-in allowlist without duplicates." \
		"$result" "runtime=/opt/zfs/bin:/usr/sbin:/custom/bin:/sbin:/bin:/usr/bin:/usr/local/sbin:/usr/local/bin"
	assertContains "Exported PATH should match the computed runtime PATH." \
		"$result" "path=/opt/zfs/bin:/usr/sbin:/custom/bin:/sbin:/bin:/usr/bin:/usr/local/sbin:/usr/local/bin"
}

test_ssh_supports_control_sockets_reflects_ssh_status() {
	g_cmd_ssh="$FAKE_SSH_BIN"

	FAKE_SSH_EXIT_STATUS=0
	export FAKE_SSH_EXIT_STATUS
	if zxfer_ssh_supports_control_sockets; then
		status_supported=0
	else
		status_supported=1
	fi

	FAKE_SSH_EXIT_STATUS=1
	export FAKE_SSH_EXIT_STATUS
	if zxfer_ssh_supports_control_sockets; then
		status_unsupported=0
	else
		status_unsupported=1
	fi

	unset FAKE_SSH_EXIT_STATUS

	assertEquals "zxfer_ssh_supports_control_sockets should succeed when ssh -M -V succeeds." 0 "$status_supported"
	assertEquals "zxfer_ssh_supports_control_sockets should fail when ssh -M -V fails." 1 "$status_unsupported"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_creates_secure_directory() {
	cache_dir=$(zxfer_ensure_ssh_control_socket_cache_dir)

	assertEquals "Shared ssh control socket cache directories should be created under TMPDIR for the effective user." \
		"$TEST_TMPDIR/zxfer-s.$(id -u).d" "$cache_dir"
	assertTrue "Shared ssh control socket cache directories should exist after creation." \
		"[ -d '$cache_dir' ]"
	assertEquals "Shared ssh control socket cache directories should be mode 0700." \
		"700" "$(zxfer_get_path_mode_octal "$cache_dir")"
}

test_zxfer_ensure_ssh_control_socket_cache_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR_PHYSICAL/ssh_cache_effective_root"
	mkdir -p "$cache_root"

	cache_dir=$(
		(
			zxfer_try_get_socket_cache_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_ssh_control_socket_cache_dir
		)
	)
	status=$?

	assertEquals "Shared ssh control socket cache dir creation should succeed when the effective temp root is available." \
		0 "$status"
	assertEquals "Shared ssh control socket cache directories should use the validated effective temp root instead of raw TMPDIR." \
		"$cache_root/zxfer-s.$(id -u).d" "$cache_dir"
}

test_zxfer_ensure_remote_capability_cache_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR_PHYSICAL/remote_capability_effective_root"
	mkdir -p "$cache_root"

	cache_dir=$(
		(
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_remote_capability_cache_dir
		)
	)
	status=$?

	assertEquals "Remote capability cache dir creation should succeed when the effective temp root is available." \
		0 "$status"
	assertEquals "Remote capability cache directories should use the validated effective temp root instead of raw TMPDIR." \
		"$cache_root/zxfer-remote-capabilities.$(id -u).d" "$cache_dir"
}

test_get_ssh_cmd_for_host_prefers_matching_control_socket() {
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_option_T_target_host="target.example"
	g_ssh_origin_control_socket="$TEST_TMPDIR/origin.sock"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"

	assertEquals "Origin host ssh command should reuse the origin control socket." \
		"'$FAKE_SSH_BIN' '-S' '$TEST_TMPDIR/origin.sock'" "$(zxfer_get_ssh_cmd_for_host "origin.example")"
	assertEquals "Target host ssh command should reuse the target control socket." \
		"'$FAKE_SSH_BIN' '-S' '$TEST_TMPDIR/target.sock'" "$(zxfer_get_ssh_cmd_for_host "target.example")"
	assertEquals "Unmatched hosts should use the base ssh command." \
		"'$FAKE_SSH_BIN'" "$(zxfer_get_ssh_cmd_for_host "other.example")"
}

test_close_target_ssh_control_socket_uses_host_tokens_and_cleans_state() {
	log="$TEST_TMPDIR/close_target.log"
	socket_dir="$TEST_TMPDIR/target_socket_dir"
	mkdir -p "$socket_dir"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$TEST_TMPDIR/target.sock"
	g_ssh_target_control_socket_dir="$socket_dir"

	zxfer_close_target_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Target socket path should be cleared after closing." "" "$g_ssh_target_control_socket"
	assertEquals "Target socket directory should be cleared after closing." "" "$g_ssh_target_control_socket_dir"
	assertFalse "Target socket directory should be removed during cleanup." "[ -d \"$socket_dir\" ]"
	assertEquals "SSH close command should preserve host token boundaries." \
		"-S
$TEST_TMPDIR/target.sock
-O
exit
target.example
doas" "$(cat "$log")"
}

test_trap_exit_relaunches_services_when_requested() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_relaunch() {
				printf 'zxfer_relaunch need=%s\n' "$g_services_need_relaunch"
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when cleanup zxfer_relaunch succeeds." 0 "$status"
	assertContains "zxfer_trap_exit should log that it is restarting stopped services." \
		"$output" "zxfer exiting early; restarting stopped services."
	assertContains "zxfer_trap_exit should invoke zxfer_relaunch when services are still marked for restart." \
		"$output" "zxfer_relaunch need=1"
}

test_trap_exit_skips_relaunch_when_relaunch_is_already_in_progress() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			g_services_relaunch_in_progress=1
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			zxfer_relaunch() {
				printf 'zxfer_relaunch-called\n'
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when zxfer_relaunch already failed earlier." 0 "$status"
	assertContains "zxfer_trap_exit should log that it is preserving stopped-service state after a failed zxfer_relaunch attempt." \
		"$output" "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
	assertNotContains "zxfer_trap_exit should not invoke zxfer_relaunch again while a failed zxfer_relaunch attempt is already in progress." \
		"$output" "zxfer_relaunch-called"
}

test_trap_exit_logs_when_relaunch_is_unavailable() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			unset -f zxfer_relaunch 2>/dev/null
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=1
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status when zxfer_relaunch is unavailable." 0 "$status"
	assertContains "zxfer_trap_exit should log when stopped services cannot be restarted because zxfer_relaunch() is missing." \
		"$output" "zxfer exiting with services still stopped; zxfer_relaunch() unavailable."
}

test_trap_exit_removes_temp_files_and_iteration_cache_dirs() {
	output=$(
		(
			trap - EXIT INT TERM HUP QUIT
			g_option_n_dryrun=0
			g_option_v_verbose=0
			g_option_V_very_verbose=0
			g_option_b_beep_always=0
			g_option_B_beep_on_success=0
			g_services_need_relaunch=0
			g_delete_source_tmp_file="$TEST_TMPDIR/delete-source.tmp"
			g_delete_dest_tmp_file="$TEST_TMPDIR/delete-dest.tmp"
			g_delete_snapshots_to_delete_tmp_file="$TEST_TMPDIR/delete-diff.tmp"
			: >"$g_delete_source_tmp_file"
			: >"$g_delete_dest_tmp_file"
			: >"$g_delete_snapshots_to_delete_tmp_file"
			g_zxfer_temp_prefix="trap-cleanup"
			: >"$TEST_TMPDIR/trap-cleanup.stale"
			mkdir -p "$TEST_TMPDIR/trap-cleanup.dir/subdir"
			: >"$TEST_TMPDIR/trap-cleanup.dir/subdir/stale"
			g_zxfer_property_cache_dir="$TEST_TMPDIR/property-cache"
			g_zxfer_snapshot_index_dir="$TEST_TMPDIR/snapshot-index"
			mkdir -p "$g_zxfer_property_cache_dir" "$g_zxfer_snapshot_index_dir"
			zxfer_close_all_ssh_control_sockets() {
				:
			}
			zxfer_echoV() {
				printf '%s\n' "$*"
			}
			true
			zxfer_trap_exit
		)
	)
	status=$?

	assertEquals "zxfer_trap_exit should preserve a successful exit status after cleaning temp files and cache directories." 0 "$status"
	assertFalse "zxfer_trap_exit should remove the delete-source temp file." "[ -e '$TEST_TMPDIR/delete-source.tmp' ]"
	assertFalse "zxfer_trap_exit should remove the delete-destination temp file." "[ -e '$TEST_TMPDIR/delete-dest.tmp' ]"
	assertFalse "zxfer_trap_exit should remove the delete-diff temp file." "[ -e '$TEST_TMPDIR/delete-diff.tmp' ]"
	assertFalse "zxfer_trap_exit should remove prefixed tmpdir scratch files for the current run." "[ -e '$TEST_TMPDIR/trap-cleanup.stale' ]"
	assertFalse "zxfer_trap_exit should remove prefixed tmpdir scratch directories for the current run." "[ -d '$TEST_TMPDIR/trap-cleanup.dir' ]"
	assertFalse "zxfer_trap_exit should remove the property cache directory." "[ -d '$TEST_TMPDIR/property-cache' ]"
	assertFalse "zxfer_trap_exit should remove the snapshot index directory." "[ -d '$TEST_TMPDIR/snapshot-index' ]"
}

test_setup_ssh_control_socket_replaces_existing_target_socket_state() {
	log="$TEST_TMPDIR/setup_target.log"
	FAKE_SSH_LOG="$log"
	FAKE_SSH_SUPPRESS_STDOUT=1
	export FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	result=$(
		(
			zxfer_close_target_ssh_control_socket() {
				printf 'closed\n'
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 1
			}
			g_cmd_ssh="$FAKE_SSH_BIN"
			g_ssh_target_control_socket="$TEST_TMPDIR/old_target.sock"
			g_ssh_target_control_socket_dir="$TEST_TMPDIR/old_target_dir"
			zxfer_setup_ssh_control_socket "target.example doas" "target"
			printf 'socket=%s\n' "$g_ssh_target_control_socket"
			printf 'dir=%s\n' "$g_ssh_target_control_socket_dir"
		)
	)

	unset FAKE_SSH_LOG FAKE_SSH_SUPPRESS_STDOUT

	assertContains "Replacing an existing target control socket should close the old socket first." \
		"$result" "closed"
	assertContains "Target socket setup should store the new control socket path." "$result" "socket="
	assertContains "Target socket setup should store the new control socket directory." "$result" "dir="
	assertEquals "New target control socket setup should preserve host token boundaries for ssh." \
		"-M
-S
$(printf '%s\n' "$result" | awk -F= '/^socket=/{print $2}')
-fN
target.example
doas" "$(cat "$log")"
}

test_setup_ssh_control_socket_reuses_live_cached_socket_without_opening_new_master() {
	log="$TEST_TMPDIR/setup_cached_socket.log"
	: >"$log"
	cache_key=$(zxfer_ssh_control_socket_cache_key "origin.example pfexec")
	cache_dir=$(zxfer_get_ssh_control_socket_cache_dir_for_key "$cache_key")
	expected_entry_dir="$cache_dir/$cache_key"

	result=$(
		(
			g_cmd_ssh="$FAKE_SSH_BIN"
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_open_ssh_control_socket_for_host() {
				printf 'open\n' >>"$log"
				return 0
			}
			zxfer_setup_ssh_control_socket "origin.example pfexec" "origin"
			printf 'socket=%s\n' "$g_ssh_origin_control_socket"
			printf 'dir=%s\n' "$g_ssh_origin_control_socket_dir"
			printf 'lease=%s\n' "$g_ssh_origin_control_socket_lease_file"
		)
	)

	assertEquals "Reusing a live cached control socket should not start a second ssh master." "" "$(cat "$log")"
	assertContains "Cached control socket reuse should still publish the socket path for the origin host." \
		"$result" "socket=$expected_entry_dir/s"
	assertContains "Cached control socket reuse should still publish the shared cache entry directory." \
		"$result" "dir=$expected_entry_dir"
	assertContains "Cached control socket reuse should register a per-process lease file." \
		"$result" "lease=$expected_entry_dir/leases/lease."
}

test_setup_ssh_control_socket_reports_cache_dir_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the shared cache entry dir cannot be prepared." 1 "$status"
	assertContains "ssh control socket setup should preserve the current cache-dir failure message." \
		"$output" "Error creating temporary directory for ssh control socket."
}

test_setup_ssh_control_socket_reports_lock_failures() {
	entry_dir="$TEST_TMPDIR/ssh_lock_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the shared lock cannot be acquired." 1 "$status"
	assertContains "ssh control socket setup should preserve the current lock-failure message." \
		"$output" "Error creating ssh control socket for origin host."
}

test_setup_ssh_control_socket_reports_master_open_failures() {
	entry_dir="$TEST_TMPDIR/ssh_open_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				printf '%s\n' "$entry_dir.lock"
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 1
			}
			zxfer_open_ssh_control_socket_for_host() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when the ssh master cannot be opened." 1 "$status"
	assertContains "ssh control socket setup should preserve the current master-open failure message." \
		"$output" "Error creating ssh control socket for origin host."
}

test_setup_ssh_control_socket_reports_lease_creation_failures() {
	entry_dir="$TEST_TMPDIR/ssh_lease_fail_entry"
	mkdir -p "$entry_dir/leases"

	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_ssh_control_socket_entry_dir() {
				printf '%s\n' "$entry_dir"
			}
			zxfer_acquire_ssh_control_socket_lock() {
				printf '%s\n' "$entry_dir.lock"
			}
			zxfer_check_ssh_control_socket_for_host() {
				return 0
			}
			zxfer_create_ssh_control_socket_lease_file() {
				return 1
			}
			zxfer_setup_ssh_control_socket "origin.example" "origin"
		)
	)
	status=$?

	assertEquals "ssh control socket setup should fail closed when a process lease cannot be created." 1 "$status"
	assertContains "ssh control socket setup should preserve the current lease-creation failure message." \
		"$output" "Error creating ssh control socket for origin host."
}

test_close_origin_ssh_control_socket_preserves_shared_socket_when_other_leases_exist() {
	log="$TEST_TMPDIR/close_origin_shared.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	other_lease="$entry_dir/leases/lease.$$.other"
	: >"$other_lease"
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Shared origin sockets should clear the in-process socket path after releasing the local lease." "" \
		"$g_ssh_origin_control_socket"
	assertEquals "Shared origin sockets should clear the in-process lease path after releasing the local lease." "" \
		"$g_ssh_origin_control_socket_lease_file"
	assertFalse "Closing a shared origin socket should remove only the current process lease when other leases remain." \
		"[ -e '$lease_file' ]"
	assertTrue "Closing a shared origin socket should preserve the cache entry while sibling leases remain." \
		"[ -d '$entry_dir' ]"
	assertEquals "Closing a shared origin socket should not send ssh -O exit while sibling leases remain." "" \
		"$(cat "$log" 2>/dev/null)"
}

test_close_origin_ssh_control_socket_closes_shared_socket_when_last_lease_exits() {
	log="$TEST_TMPDIR/close_origin_last_lease.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example pfexec")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example pfexec"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		return 0
	}

	zxfer_close_origin_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Last shared origin-socket lease release should clear the in-process socket path." "" \
		"$g_ssh_origin_control_socket"
	assertEquals "Last shared origin-socket lease release should clear the in-process lease path." "" \
		"$g_ssh_origin_control_socket_lease_file"
	assertFalse "Last shared origin-socket lease release should remove the shared cache entry after ssh exits." \
		"[ -d '$entry_dir' ]"
	assertEquals "Last shared origin-socket lease release should close the shared ssh master with preserved host tokens." \
		"-S
$entry_dir/s
-O
exit
origin.example
pfexec" "$(cat "$log")"
}

test_close_target_ssh_control_socket_closes_shared_socket_when_last_lease_exits() {
	log="$TEST_TMPDIR/close_target_shared.log"
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example doas")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	FAKE_SSH_LOG="$log"
	export FAKE_SSH_LOG
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example doas"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		return 0
	}

	zxfer_close_target_ssh_control_socket

	unset FAKE_SSH_LOG

	assertEquals "Last shared target-socket lease release should clear the in-process socket path." "" \
		"$g_ssh_target_control_socket"
	assertEquals "Last shared target-socket lease release should clear the in-process lease path." "" \
		"$g_ssh_target_control_socket_lease_file"
	assertFalse "Last shared target-socket lease release should remove the shared cache entry after ssh exits." \
		"[ -d '$entry_dir' ]"
	assertEquals "Last shared target-socket lease release should close the shared ssh master with preserved host tokens." \
		"-S
$entry_dir/s
-O
exit
target.example
doas" "$(cat "$log")"
}

test_close_origin_ssh_control_socket_removes_stale_shared_entry_when_socket_is_not_live() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "origin.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_O_origin_host="origin.example"
	g_ssh_origin_control_socket="$entry_dir/s"
	g_ssh_origin_control_socket_dir="$entry_dir"
	g_ssh_origin_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		return 1
	}

	zxfer_close_origin_ssh_control_socket

	assertFalse "Last shared origin-socket lease release should remove stale cache entries when the socket is no longer live." \
		"[ -d '$entry_dir' ]"
}

test_close_target_ssh_control_socket_removes_stale_shared_entry_when_socket_is_not_live() {
	entry_dir=$(zxfer_ensure_ssh_control_socket_entry_dir "target.example")
	lease_file=$(zxfer_create_ssh_control_socket_lease_file "$entry_dir")
	: >"$entry_dir/s"
	g_cmd_ssh="$FAKE_SSH_BIN"
	g_option_T_target_host="target.example"
	g_ssh_target_control_socket="$entry_dir/s"
	g_ssh_target_control_socket_dir="$entry_dir"
	g_ssh_target_control_socket_lease_file="$lease_file"
	zxfer_check_ssh_control_socket_for_host() {
		return 1
	}

	zxfer_close_target_ssh_control_socket

	assertFalse "Last shared target-socket lease release should remove stale cache entries when the socket is no longer live." \
		"[ -d '$entry_dir' ]"
}

test_consistency_check_rejects_backup_and_restore_modes_together() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_k_backup_property_mode=1
			g_option_e_restore_property_mode=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Backup and restore mode conflicts should fail validation." 2 "$status"
	assertContains "Backup and restore mode conflicts should use the documented error." \
		"$output" "You cannot bac(k)up and r(e)store properties at the same time."
}

test_consistency_check_rejects_dual_beep_modes() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_b_beep_always=1
			g_option_B_beep_on_success=1
			zxfer_consistency_check
		)
	)
	status=$?

	assertEquals "Conflicting beep modes should fail validation." 2 "$status"
	assertContains "Conflicting beep modes should use the documented error." \
		"$output" "You cannot use both beep modes at the same time."
}

test_consistency_check_rejects_invalid_grandfather_values() {
	set +e
	output_non_numeric=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="abc"
			zxfer_consistency_check
		)
	)
	status_non_numeric=$?

	output_zero=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_g_grandfather_protection="0"
			zxfer_consistency_check
		)
	)
	status_zero=$?

	assertEquals "Non-numeric grandfather values should fail validation." 2 "$status_non_numeric"
	assertContains "Non-numeric grandfather errors should mention the received value." \
		"$output_non_numeric" "grandfather protection requires a positive integer; received \"abc\"."
	assertEquals "Zero-day grandfather values should fail validation." 2 "$status_zero"
	assertContains "Zero-day grandfather errors should require days greater than zero." \
		"$output_zero" "grandfather protection requires days greater than 0; received \"0\"."
}

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

test_backup_storage_helpers_cover_fallback_encoding_failures_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_helper_fallback.out"

	(
		cksum() {
			:
		}
		od() {
			:
		}
		zxfer_backup_metadata_file_key "tank/src" "backup/dst" >"$output_file"
	)
	assertEquals "Backup metadata file keys should fall back to k00 in the current shell when hex encoding produces no output." \
		"k00" "$(cat "$output_file")"
}

test_zxfer_get_backup_metadata_filename_runs_in_current_shell() {
	output_file="$TEST_TMPDIR/backup_filename_current_shell.out"
	g_backup_file_extension=".zxfer_backup_info"

	zxfer_get_backup_metadata_filename "tank/src" "backup/dst" >"$output_file"

	assertContains "Backup metadata filename rendering should run in the current shell." \
		"$(cat "$output_file")" ".zxfer_backup_info.src.k"
}

test_backup_metadata_matches_source_accepts_only_exact_modern_pairs() {
	assertEquals "Backup metadata matching should accept the modern source,destination,properties record order." \
		0 "$(
			(
				zxfer_backup_metadata_matches_source "tank/src,backup/dst,compression=lz4" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject the legacy destination,source,properties record order." \
		1 "$(
			(
				zxfer_backup_metadata_matches_source "backup/dst,tank/src,compression=lz4" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_backup_metadata_matches_source_rejects_wrong_destination_and_ambiguous_pairs() {
	assertEquals "Backup metadata matching should reject rows for the requested source dataset when the recorded destination does not match." \
		1 "$(
			(
				zxfer_backup_metadata_matches_source "tank/src,backup/other,compression=lz4" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject files that contain multiple exact matches for the same source/destination pair." \
		2 "$(
			(
				zxfer_backup_metadata_matches_source "$(printf '%s\n%s\n' \
					'tank/src,backup/dst,compression=lz4' \
					'tank/src,backup/dst,compression=off')" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_backup_metadata_matches_source_rejects_malformed_current_format_rows() {
	assertEquals "Backup metadata matching should reject rows that do not contain the current source,destination,properties format." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "tank/src,backup/dst" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
	assertEquals "Backup metadata matching should reject rows that contain extra raw field delimiters." \
		3 "$(
			(
				zxfer_backup_metadata_matches_source "tank/src,backup/dst,compression=lz4,extra" "tank/src" "backup/dst"
				printf '%s\n' "$?"
			)
		)"
}

test_zxfer_try_backup_restore_candidate_returns_missing_for_missing_local_candidate() {
	assertEquals "Missing local backup candidates should return the candidate-missing sentinel so ancestor lookup can continue." \
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
	assertEquals "Missing remote backup candidates should return the candidate-missing sentinel so ancestor lookup can continue." \
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
					printf '%s\n' "tank/src,backup/dst,compression=lz4"
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

	assertContains "Backup metadata filenames should keep the readable source tail." \
		"$first_name" ".zxfer_backup_info.src.k"
	assertNotEquals "Distinct source datasets that share the same tail should produce different backup metadata filenames." \
		"$first_name" "$second_name"
	assertNotEquals "Distinct destination roots for the same source should produce different backup metadata filenames." \
		"$first_name" "$third_name"
}

test_zxfer_backup_metadata_file_key_falls_back_when_hex_encoding_is_empty() {
	(
		cksum() {
			:
		}
		od() {
			:
		}

		assertEquals "Backup metadata file keys should fall back to a deterministic placeholder when hex encoding produces no output." \
			"k00" "$(zxfer_backup_metadata_file_key "tank/src" "backup/dst")"
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

test_get_ssh_cmd_for_host_returns_base_command_for_empty_host() {
	g_cmd_ssh="/usr/bin/ssh"

	assertEquals "Hosts omitted from wrapper lookups should return the base ssh command." \
		"'/usr/bin/ssh'" "$(zxfer_get_ssh_cmd_for_host "")"
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
	mkdir -p "$backup_dir" "$fake_bin"
	cat >"$fake_bin/chmod" <<'EOF'
#!/bin/sh
exit 1
EOF
	chmod +x "$fake_bin/chmod"
	old_path=$PATH
	PATH="$fake_bin:$PATH"
	THROW_MSG=""
	zxfer_throw_error() {
		THROW_MSG=$1
		return 1
	}

	zxfer_ensure_local_backup_dir "$backup_dir"
	status=$?

	unset -f zxfer_throw_error
	PATH=$old_path

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
			zxfer_get_ssh_cmd_for_host() {
				printf '%s\n' "/usr/bin/ssh"
			}
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

	zxfer_ensure_remote_backup_dir "-remote_backup" "backup@example.com"

	assertContains "Dash-prefixed remote backup paths should be rewritten for ls-based owner checks." \
		"$(cat "$ssh_log")" "./-remote_backup"
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
			zxfer_build_remote_sh_c_command() {
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				PATH="$fake_bin:$PATH" sh -c "$2"
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

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
