#!/bin/sh
#
# shunit2 tests for zxfer_property_cache.sh and zxfer_transfer_properties.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2034,SC2317,SC2329

case "$0" in
/*)
	TESTS_DIR=$(dirname "$0")
	;;
*)
	TESTS_DIR=${PWD:-.}/$(dirname "$0")
	;;
esac

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_transfer_properties.sh"

usage() {
	:
}

oneTimeSetUp() {
	TEST_TMPDIR=$(mktemp -d -t zxfer_transfer_props.XXXXXX)
}

oneTimeTearDown() {
	rm -rf "$TEST_TMPDIR"
}

setUp() {
	g_option_n_dryrun=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_e_restore_property_mode=0
	g_option_k_backup_property_mode=0
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_option_I_ignore_properties=""
	g_destination_operating_system=""
	g_source_operating_system=""
	g_readonly_properties="readonly,mountpoint"
	g_fbsd_readonly_properties="aclmode"
	g_solexp_readonly_properties="jailed"
	g_RZFS="/sbin/zfs"
	g_LZFS="/sbin/zfs"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	g_backup_file_contents=""
	g_restored_backup_file_contents=""
	g_ensure_writable=0
	g_dest_created_by_zxfer=0
	g_dest_seed_requires_property_reconcile=0
	g_destination="backup/dst"
	zxfer_reset_destination_existence_cache
	zxfer_reset_property_iteration_caches
	g_zxfer_property_cache_dir=""
	g_zxfer_property_cache_unavailable=0
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_profile_parent_destination_property_reads=0
	initial_source="tank/src"
	unsupported_properties=""
	zxfer_reset_failure_context "unit"
}

test_select_mc_picks_requested_properties() {
	l_oldifs=$IFS
	IFS=","
	select_mc "casesensitivity=mixed=local,compression=lz4=local,utf8only=on=local" "utf8only,casesensitivity"
	IFS=$l_oldifs

	assertEquals "Must-create selection should preserve only the requested properties." \
		"casesensitivity=mixed=local,utf8only=on=local" "$m_new_mc_pvs"
}

test_remove_properties_preserves_override_entries() {
	l_oldifs=$IFS
	IFS=","
	remove_properties "mountpoint=/mnt=local,readonly=off=override,compression=lz4=local" "readonly,mountpoint"
	IFS=$l_oldifs

	assertEquals "Override entries should survive property filtering even when the property is listed for removal." \
		"readonly=off=override,compression=lz4=local" "$m_new_rmv_pvs"
}

test_run_zfs_create_with_properties_executes_live_create() {
	result=$(
		(
			run_destination_zfs_cmd() {
				printf '%s\n' "$*"
			}
			run_zfs_create_with_properties yes volume 10G "compression=lz4,atime=off" "backup/dst"
		)
	)

	assertEquals "Live zfs create should pass each argument separately." \
		"create -p -V 10G -o compression=lz4 -o atime=off backup/dst" "$result"
}

test_run_zfs_create_with_properties_renders_dry_run_command() {
	g_option_n_dryrun=1
	g_RZFS="/usr/bin/ssh 'host' /sbin/zfs"

	result=$(run_zfs_create_with_properties no filesystem "" "compression=lz4,quota=1G" "backup/dst")

	assertEquals "Dry-run zfs create should render a safely quoted command line." \
		"/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4' '-o' 'quota=1G' 'backup/dst'" "$result"
}

test_run_zfs_create_with_properties_rejects_volume_without_size() {
	set +e
	run_zfs_create_with_properties yes volume "" "compression=lz4" "backup/dst" >/dev/null 2>&1
	status=$?

	assertEquals "Volume creates should fail closed when the source volsize is unavailable." 1 "$status"
}

test_run_zfs_create_with_properties_decodes_delimiter_heavy_assignments_for_exec() {
	result=$(
		(
			run_destination_zfs_cmd() {
				printf '%s\n' "$*"
			}
			run_zfs_create_with_properties no filesystem "" \
				"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" \
				"backup/dst"
		)
	)

	assertEquals "Live zfs create should decode delimiter-heavy property values before execution." \
		"create -o user:note=value,with,commas=and;semi backup/dst" "$result"
}

test_get_normalized_dataset_properties_defaults_to_g_lzfs() {
	result=$(
		(
			run_zfs_cmd_for_spec() {
				if [ "$3" = "-Hpo" ]; then
					printf 'quota\t1073741824\tlocal\ncompression\tlz4\tlocal\n'
				else
					printf 'quota\tnone\tlocal\ncompression\tlz4\tlocal\n'
				fi
			}
			g_LZFS="/remote/zfs"
			get_normalized_dataset_properties "tank/src" ""
		)
	)

	assertEquals "Normalized property lookup should merge machine and human values, preserving human none values." \
		"quota=none=local,compression=lz4=local" "$result"
}

test_get_normalized_dataset_properties_escapes_delimiter_heavy_values() {
	result=$(
		(
			run_zfs_cmd_for_spec() {
				printf 'user:note\tvalue,=; mix\tlocal\n'
			}
			get_normalized_dataset_properties "tank/src" "/sbin/zfs"
		)
	)

	assertEquals "Normalized property lookup should escape delimiter-heavy values in the internal serialized form." \
		"user:note=value%2C%3D%3B mix=local" "$result"
}

test_get_normalized_dataset_properties_tracks_profile_counters_by_lookup_side() {
	output_file="$TEST_TMPDIR/normalized_profile.out"
	old_very_verbose=${g_option_V_very_verbose-}

	g_option_V_very_verbose=1

	(
		run_zfs_cmd_for_spec() {
			if [ "$3" = "-Hpo" ]; then
				printf 'compression\tlz4\tlocal\n'
			else
				printf 'compression\tlz4\tlocal\n'
			fi
		}
		get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >/dev/null
		get_normalized_dataset_properties "backup/dst" "/sbin/zfs" destination >/dev/null
		printf 'source=%s\n' "${g_zxfer_profile_normalized_property_reads_source:-0}" >"$output_file"
		printf 'destination=%s\n' "${g_zxfer_profile_normalized_property_reads_destination:-0}" >>"$output_file"
	)

	g_option_V_very_verbose=$old_very_verbose

	assertEquals "Very-verbose profiling should track normalized property reads by lookup side." \
		"source=1
destination=1" "$(cat "$output_file")"
}

test_get_normalized_dataset_properties_caches_same_side_dataset_in_current_shell() {
	log="$TEST_TMPDIR/normalized_cache_same_side.log"
	: >"$log"
	g_option_V_very_verbose=1
	calls_log="$TEST_TMPDIR/normalized_cache_same_side.calls"
	: >"$calls_log"

	run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
		else
			printf 'compression\tlz4\tlocal\n'
		fi
	}

	get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$log"
	get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >>"$log"
	{
		printf 'calls=%s\n' "$(awk 'END {print NR + 0}' "$calls_log")"
		printf 'reads=%s\n' "${g_zxfer_profile_normalized_property_reads_source:-0}"
	} >>"$log"

	assertEquals "Repeated normalized property reads for the same source dataset in one iteration should hit the cache after the first lookup." \
		"compression=lz4=local
compression=lz4=local
calls=2
reads=1" "$(cat "$log")"
}

test_get_normalized_dataset_properties_separates_source_and_destination_cache_keys() {
	log="$TEST_TMPDIR/normalized_cache_side_keys.log"
	: >"$log"
	g_option_V_very_verbose=1
	calls_log="$TEST_TMPDIR/normalized_cache_side_keys.calls"
	: >"$calls_log"

	run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
		else
			printf 'compression\tlz4\tlocal\n'
		fi
	}

	get_normalized_dataset_properties "tank/shared" "/sbin/zfs" source >"$log"
	get_normalized_dataset_properties "tank/shared" "/sbin/zfs" destination >>"$log"
	{
		printf 'calls=%s\n' "$(awk 'END {print NR + 0}' "$calls_log")"
		printf 'source=%s\n' "${g_zxfer_profile_normalized_property_reads_source:-0}"
		printf 'destination=%s\n' "${g_zxfer_profile_normalized_property_reads_destination:-0}"
	} >>"$log"

	assertEquals "Source and destination normalized property caches should not collide for the same dataset name." \
		"compression=lz4=local
compression=lz4=local
calls=4
source=1
destination=1" "$(cat "$log")"
}

test_get_normalized_dataset_properties_does_not_cache_failed_reads() {
	err_log="$TEST_TMPDIR/normalized_cache_failure.err"
	out_log="$TEST_TMPDIR/normalized_cache_failure.out"
	calls_log="$TEST_TMPDIR/normalized_cache_failure.calls"
	: >"$calls_log"
	l_mode="fail"

	run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$l_mode" = "fail" ]; then
			printf '%s\n' "permission denied"
			return 1
		fi
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
		else
			printf 'compression\tlz4\tlocal\n'
		fi
	}

	set +e
	get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?

	l_mode="success"
	get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$out_log"
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$m_property_cache_path

	unset -f run_zfs_cmd_for_spec

	assertEquals "Failed normalized-property reads should return a non-zero status." 1 "$status"
	assertEquals "Failed normalized-property reads should surface the underlying zfs error." \
		"permission denied" "$(cat "$err_log")"
	assertEquals "A later successful lookup for the same dataset should still execute the full normalized read instead of reusing a poisoned cache entry." \
		"compression=lz4=local" "$(cat "$out_log")"
	assertEquals "A failed normalized lookup should not create a cache entry before the later successful read stores one." \
		"3" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertTrue "Successful normalized lookups after a failure should still populate the cache." \
		"[ -f \"$cache_path\" ]"
}

test_get_normalized_dataset_properties_reports_human_probe_failures_without_caching() {
	err_log="$TEST_TMPDIR/normalized_human_failure.err"
	calls_log="$TEST_TMPDIR/normalized_human_failure.calls"
	: >"$calls_log"

	run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
			return 0
		fi
		printf '%s\n' "ssh timeout"
		return 1
	}

	set +e
	get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$m_property_cache_path

	unset -f run_zfs_cmd_for_spec

	assertEquals "Human normalized-property probe failures should return a non-zero status." \
		"1" "$status"
	assertEquals "Human normalized-property probe failures should surface the underlying zfs error." \
		"ssh timeout" "$(cat "$err_log")"
	assertEquals "Human normalized-property probe failures should still execute both normalized probes before failing." \
		"2" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Human normalized-property probe failures should not populate the cache." \
		"[ -e \"$cache_path\" ]"
}

test_zxfer_property_cache_dataset_path_encodes_dataset_name_safely() {
	l_dataset="../unsafe path:/child"
	l_expected_key=$(printf '%s' "$l_dataset" | LC_ALL=C od -An -tx1 | tr -d ' \n')

	zxfer_property_cache_dataset_path normalized destination "$l_dataset" >/dev/null

	assertEquals "Dataset cache paths should encode dataset names instead of treating them as filesystem structure." \
		"${g_zxfer_property_cache_dir}/normalized/destination/${l_expected_key}" "$m_property_cache_path"
}

test_zxfer_property_cache_encode_key_does_not_collapse_repeated_od_lines() {
	l_dataset="pool/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	l_encoded_key=$(zxfer_property_cache_encode_key "$l_dataset")

	assertFalse "Cache-key encoding should not emit BSD od repetition markers for long repeated dataset names." \
		"printf '%s' \"$l_encoded_key\" | grep -F '\\*' >/dev/null"
}

test_zxfer_invalidate_destination_property_cache_removes_encoded_cache_entries() {
	l_dataset="../unsafe path:/child"

	zxfer_property_cache_dataset_path normalized destination "$l_dataset" >/dev/null
	l_normalized_cache_path=$m_property_cache_path
	zxfer_property_cache_property_path required destination "$l_dataset" "casesensitivity" >/dev/null
	l_required_cache_path=$m_property_cache_path
	mkdir -p "${l_normalized_cache_path%/*}"
	mkdir -p "${l_required_cache_path%/*}"
	printf 'compression=lz4=local\n' >"$l_normalized_cache_path"
	printf 'casesensitivity=sensitive=local\n' >"$l_required_cache_path"

	zxfer_invalidate_destination_property_cache "$l_dataset"

	assertFalse "Destination cache invalidation should remove encoded normalized-property cache entries." \
		"[ -e \"$l_normalized_cache_path\" ]"
	assertFalse "Destination cache invalidation should remove encoded required-property cache entries." \
		"[ -e \"$l_required_cache_path\" ]"
}

test_zxfer_reset_property_iteration_caches_removes_cache_dir_and_resets_state() {
	cache_dir="$TEST_TMPDIR/property-cache-reset"
	mkdir -p "$cache_dir/normalized/source"
	printf '%s\n' "compression=lz4=local" >"$cache_dir/normalized/source/cache"
	g_zxfer_property_cache_dir=$cache_dir
	g_zxfer_property_cache_unavailable=1
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/source/zfs"
	g_zxfer_source_property_tree_prefetch_state=1
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/dest/zfs"
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_reset_property_iteration_caches

	assertFalse "Resetting property caches should remove the per-iteration cache directory." \
		"[ -d \"$cache_dir\" ]"
	assertEquals "Resetting property caches should clear the cache-directory pointer." \
		"" "${g_zxfer_property_cache_dir:-}"
	assertEquals "Resetting property caches should make cache creation eligible again." \
		"0" "${g_zxfer_property_cache_unavailable:-1}"
	assertEquals "Resetting property caches should clear the source recursive property-tree root." \
		"" "${g_zxfer_source_property_tree_prefetch_root:-}"
	assertEquals "Resetting property caches should clear the destination recursive property-tree root." \
		"" "${g_zxfer_destination_property_tree_prefetch_root:-}"
	assertEquals "Resetting property caches should reset the source recursive property-tree state." \
		"0" "${g_zxfer_source_property_tree_prefetch_state:-1}"
	assertEquals "Resetting property caches should reset the destination recursive property-tree state." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"
}

test_zxfer_ensure_property_cache_dir_reuses_existing_directory() {
	cache_dir="$TEST_TMPDIR/property-cache-existing"
	mkdir -p "$cache_dir"
	g_zxfer_property_cache_dir=$cache_dir

	zxfer_ensure_property_cache_dir
	status=$?

	assertEquals "Existing cache directories should be reused instead of creating a new one." \
		"0" "$status"
	assertEquals "Reused cache directories should remain unchanged." \
		"$cache_dir" "$g_zxfer_property_cache_dir"
}

test_zxfer_ensure_property_cache_dir_uses_effective_tmpdir_in_current_shell() {
	cache_root="$TEST_TMPDIR/property-cache-effective-root"
	mkdir -p "$cache_root"

	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				printf '%s\n' "$cache_root"
			}

			zxfer_ensure_property_cache_dir || exit $?
			printf 'dir=%s\n' "$g_zxfer_property_cache_dir"
		)
	)
	status=$?

	assertEquals "Property cache directory creation should succeed when the validated effective temp root is available." \
		"0" "$status"
	assertContains "Property cache directories should be created under the effective temp root instead of raw TMPDIR." \
		"$output" "dir=$cache_root/zxfer-property-cache."
}

test_zxfer_ensure_property_cache_dir_marks_cache_unavailable_when_effective_tmpdir_lookup_fails() {
	set +e
	output=$(
		(
			zxfer_try_get_effective_tmpdir() {
				return 1
			}

			zxfer_ensure_property_cache_dir >/dev/null 2>&1 || {
				printf 'unavailable=%s\n' "${g_zxfer_property_cache_unavailable:-0}"
				printf 'dir=%s\n' "${g_zxfer_property_cache_dir:-}"
				return 1
			}
		)
	)
	status=$?

	assertEquals "Effective temp-root lookup failures should disable the property cache for the rest of the iteration." \
		"1" "$status"
	assertContains "Effective temp-root lookup failures should mark the property cache as unavailable." \
		"$output" "unavailable=1"
	assertContains "Effective temp-root lookup failures should leave the cache directory unset." \
		"$output" "dir="
}

test_zxfer_ensure_property_cache_dir_marks_cache_unavailable_when_mktemp_fails() {
	mktemp() {
		return 1
	}

	set +e
	zxfer_ensure_property_cache_dir >/dev/null 2>&1
	status=$?

	unset -f mktemp

	assertEquals "mktemp failures should disable the property cache for the rest of the iteration." \
		"1" "$status"
	assertEquals "mktemp failures should mark the property cache as unavailable." \
		"1" "${g_zxfer_property_cache_unavailable:-0}"
	assertEquals "mktemp failures should leave the cache directory unset." \
		"" "${g_zxfer_property_cache_dir:-}"
}

test_zxfer_invalidate_dataset_property_cache_removes_source_side_entries() {
	l_dataset="../unsafe path:/child"

	zxfer_property_cache_dataset_path normalized source "$l_dataset" >/dev/null
	l_normalized_cache_path=$m_property_cache_path
	zxfer_property_cache_property_path required source "$l_dataset" "casesensitivity" >/dev/null
	l_required_cache_path=$m_property_cache_path
	mkdir -p "${l_normalized_cache_path%/*}"
	mkdir -p "${l_required_cache_path%/*}"
	printf 'compression=lz4=local\n' >"$l_normalized_cache_path"
	printf 'casesensitivity=sensitive=local\n' >"$l_required_cache_path"

	zxfer_invalidate_dataset_property_cache source "$l_dataset"

	assertFalse "Generic dataset cache invalidation should remove source normalized-property cache entries." \
		"[ -e \"$l_normalized_cache_path\" ]"
	assertFalse "Generic dataset cache invalidation should remove source required-property cache entries." \
		"[ -e \"$l_required_cache_path\" ]"
}

test_zxfer_reset_destination_property_iteration_cache_preserves_source_cache_entries() {
	l_dataset="tank/src"

	zxfer_property_cache_dataset_path normalized source "$l_dataset" >/dev/null
	l_source_normalized_cache_path=$m_property_cache_path
	zxfer_property_cache_property_path required source "$l_dataset" "casesensitivity" >/dev/null
	l_source_required_cache_path=$m_property_cache_path
	zxfer_property_cache_dataset_path normalized destination "$l_dataset" >/dev/null
	l_destination_normalized_cache_path=$m_property_cache_path
	zxfer_property_cache_property_path required destination "$l_dataset" "casesensitivity" >/dev/null
	l_destination_required_cache_path=$m_property_cache_path
	mkdir -p "${l_source_normalized_cache_path%/*}"
	mkdir -p "${l_source_required_cache_path%/*}"
	mkdir -p "${l_destination_normalized_cache_path%/*}"
	mkdir -p "${l_destination_required_cache_path%/*}"
	printf 'compression=lz4=local\n' >"$l_source_normalized_cache_path"
	printf 'casesensitivity=sensitive=local\n' >"$l_source_required_cache_path"
	printf 'compression=lz4=local\n' >"$l_destination_normalized_cache_path"
	printf 'casesensitivity=sensitive=local\n' >"$l_destination_required_cache_path"

	zxfer_reset_destination_property_iteration_cache

	assertTrue "Destination-cache resets should preserve source normalized-property cache entries." \
		"[ -e \"$l_source_normalized_cache_path\" ]"
	assertTrue "Destination-cache resets should preserve source required-property cache entries." \
		"[ -e \"$l_source_required_cache_path\" ]"
	assertFalse "Destination-cache resets should remove destination normalized-property cache entries." \
		"[ -e \"$l_destination_normalized_cache_path\" ]"
	assertFalse "Destination-cache resets should remove destination required-property cache entries." \
		"[ -e \"$l_destination_required_cache_path\" ]"
}

test_zxfer_reset_destination_property_iteration_cache_rearms_destination_tree_prefetch() {
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_reset_destination_property_iteration_cache

	assertEquals "Destination-cache resets should allow the recursive destination property tree to be prefetched again when needed." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"
}

test_zxfer_refresh_property_tree_prefetch_context_tracks_recursive_property_roots() {
	g_option_R_recursive="tank/src"
	g_option_P_transfer_property=1
	g_option_V_very_verbose=1
	initial_source="tank/src"
	g_destination="backup/dst"
	g_LZFS="/source/zfs"
	g_RZFS="/dest/zfs"
	g_zxfer_source_property_tree_prefetch_state=1
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_refresh_property_tree_prefetch_context

	assertEquals "Recursive property runs should prefetch source properties from the initial source root." \
		"tank/src" "$g_zxfer_source_property_tree_prefetch_root"
	assertEquals "Recursive property runs should prefetch destination properties from the destination root." \
		"backup/dst" "$g_zxfer_destination_property_tree_prefetch_root"
	assertEquals "Source property-tree context should keep the configured source zfs command." \
		"/source/zfs" "$g_zxfer_source_property_tree_prefetch_zfs_cmd"
	assertEquals "Destination property-tree context should keep the configured destination zfs command." \
		"/dest/zfs" "$g_zxfer_destination_property_tree_prefetch_zfs_cmd"
	assertEquals "Refreshing the prefetch context should re-arm source-side tree prefetching." \
		"0" "${g_zxfer_source_property_tree_prefetch_state:-1}"
	assertEquals "Refreshing the prefetch context should re-arm destination-side tree prefetching." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"
}

test_zxfer_refresh_property_tree_prefetch_context_clears_state_when_prefetch_is_inapplicable() {
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/source/zfs"
	g_zxfer_source_property_tree_prefetch_state=1
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/dest/zfs"
	g_zxfer_destination_property_tree_prefetch_state=1

	g_option_R_recursive=""
	zxfer_refresh_property_tree_prefetch_context

	assertEquals "Non-recursive runs should clear the source property-tree prefetch root." \
		"" "${g_zxfer_source_property_tree_prefetch_root:-}"
	assertEquals "Non-recursive runs should clear the destination property-tree prefetch root." \
		"" "${g_zxfer_destination_property_tree_prefetch_root:-}"
	assertEquals "Non-recursive runs should reset the source property-tree state." \
		"0" "${g_zxfer_source_property_tree_prefetch_state:-1}"
	assertEquals "Non-recursive runs should reset the destination property-tree state." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"

	g_option_R_recursive="tank/src"
	g_option_P_transfer_property=0
	g_option_o_override_property=""
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_source_property_tree_prefetch_state=1
	g_zxfer_destination_property_tree_prefetch_state=1
	zxfer_refresh_property_tree_prefetch_context

	assertEquals "Recursive runs without property transfer work should also clear source prefetch state." \
		"" "${g_zxfer_source_property_tree_prefetch_root:-}"
	assertEquals "Recursive runs without property transfer work should also clear destination prefetch state." \
		"" "${g_zxfer_destination_property_tree_prefetch_root:-}"
}

test_zxfer_ensure_property_cache_dir_returns_failure_when_marked_unavailable() {
	g_zxfer_property_cache_unavailable=1

	set +e
	zxfer_ensure_property_cache_dir >/dev/null 2>&1
	status=$?

	assertEquals "An unavailable property cache should fail immediately without creating a directory." \
		"1" "$status"
}

test_zxfer_property_cache_helpers_cover_empty_encoding_and_failure_paths() {
	od() {
		:
	}
	assertEquals "Empty key encodings should fall back to 00 so cache helpers still produce safe file names." \
		"00" "$(zxfer_property_cache_encode_key "tank/src")"
	unset -f od

	g_zxfer_property_cache_unavailable=1
	set +e
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null 2>&1
	dataset_status=$?
	zxfer_property_cache_property_path required source "tank/src" "casesensitivity" >/dev/null 2>&1
	property_status=$?
	g_zxfer_property_cache_unavailable=0

	assertEquals "Dataset cache-path lookups should fail when the property cache is unavailable." \
		"1" "$dataset_status"
	assertEquals "Per-property cache-path lookups should fail when the property cache is unavailable." \
		"1" "$property_status"
}

test_zxfer_get_property_tree_prefetch_dataset_list_uses_source_and_destination_fallbacks() {
	g_recursive_source_dataset_list=""
	g_recursive_source_list="tank/src tank/src/child"

	assertEquals "Source property-tree dataset selection should fall back to the recursive source list when the explicit dataset list is empty." \
		"tank/src
tank/src/child" "$(zxfer_get_property_tree_prefetch_dataset_list source)"

	g_recursive_source_list=""
	initial_source="tank/src"
	assertEquals "Source property-tree dataset selection should finally fall back to the initial source." \
		"tank/src" "$(zxfer_get_property_tree_prefetch_dataset_list source)"

	g_recursive_dest_list=""
	set +e
	zxfer_get_property_tree_prefetch_dataset_list destination >/dev/null 2>&1
	status=$?

	assertEquals "Destination property-tree dataset selection should fail when there is no recursive destination list yet." \
		"1" "$status"
}

test_get_normalized_dataset_properties_prefetches_recursive_source_tree_and_slices_locally() {
	log="$TEST_TMPDIR/prefetch_source_tree.calls"
	first_out="$TEST_TMPDIR/prefetch_source_tree.first"
	second_out="$TEST_TMPDIR/prefetch_source_tree.second"
	: >"$log"
	g_option_R_recursive="tank/src"
	g_option_P_transfer_property=1
	g_option_V_very_verbose=1
	initial_source="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_LZFS="/sbin/zfs"
	zxfer_refresh_property_tree_prefetch_context

	run_zfs_cmd_for_spec() {
		printf '%s %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$log"
		case "$3" in
		-r)
			case "$4" in
			-Hpo)
				printf '%s\t%s\t%s\t%s\n' \
					"tank/src" "compression" "lz4" "local" \
					"tank/src" "readonly" "off" "local" \
					"tank/src/child" "compression" "gzip" "inherited" \
					"tank/src/child" "readonly" "off" "inherited"
				;;
			-Ho)
				printf '%s\t%s\t%s\t%s\n' \
					"tank/src" "compression" "lz4" "local" \
					"tank/src" "readonly" "off" "local" \
					"tank/src/child" "compression" "gzip" "inherited" \
					"tank/src/child" "readonly" "off" "inherited"
				;;
			esac
			return 0
			;;
		esac
		printf '%s\n' "unexpected live property lookup"
		return 1
	}

	get_normalized_dataset_properties "tank/src/child" "/sbin/zfs" source >"$first_out"
	get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$second_out"

	unset -f run_zfs_cmd_for_spec

	assertEquals "Recursive source-tree prefetch should return the cached child dataset properties without exact per-dataset zfs gets." \
		"compression=gzip=inherited,readonly=off=inherited" "$(cat "$first_out")"
	assertEquals "Recursive source-tree prefetch should also cache the root dataset properties for later lookups in the same iteration." \
		"compression=lz4=local,readonly=off=local" "$(cat "$second_out")"
	assertEquals "Prefetching the recursive source property tree should use exactly one machine-readable and one human-readable zfs get." \
		"2" "$(awk 'END {print NR + 0}' "$log")"
	assertEquals "Recursive source-tree prefetch should count as a single source normalized-property read." \
		"1" "${g_zxfer_profile_normalized_property_reads_source:-0}"
}

test_get_normalized_dataset_properties_prefetches_recursive_destination_tree_and_slices_locally() {
	log="$TEST_TMPDIR/prefetch_destination_tree.calls"
	first_out="$TEST_TMPDIR/prefetch_destination_tree.first"
	second_out="$TEST_TMPDIR/prefetch_destination_tree.second"
	: >"$log"
	g_option_R_recursive="tank/src"
	g_option_P_transfer_property=1
	g_option_V_very_verbose=1
	g_destination="backup/dst"
	g_recursive_dest_list="backup/dst
backup/dst/child"
	g_RZFS="/sbin/zfs"
	zxfer_refresh_property_tree_prefetch_context

	run_zfs_cmd_for_spec() {
		printf '%s %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$log"
		case "$3" in
		-r)
			case "$4" in
			-Hpo)
				printf '%s\t%s\t%s\t%s\n' \
					"backup/dst" "compression" "lz4" "local" \
					"backup/dst" "readonly" "off" "local" \
					"backup/dst/child" "compression" "gzip" "inherited" \
					"backup/dst/child" "readonly" "off" "inherited"
				;;
			-Ho)
				printf '%s\t%s\t%s\t%s\n' \
					"backup/dst" "compression" "lz4" "local" \
					"backup/dst" "readonly" "off" "local" \
					"backup/dst/child" "compression" "gzip" "inherited" \
					"backup/dst/child" "readonly" "off" "inherited"
				;;
			esac
			return 0
			;;
		esac
		printf '%s\n' "unexpected live destination property lookup"
		return 1
	}

	get_normalized_dataset_properties "backup/dst/child" "/sbin/zfs" destination >"$first_out"
	get_normalized_dataset_properties "backup/dst" "/sbin/zfs" destination >"$second_out"

	unset -f run_zfs_cmd_for_spec

	assertEquals "Recursive destination-tree prefetch should return the cached child dataset properties without exact per-dataset zfs gets." \
		"compression=gzip=inherited,readonly=off=inherited" "$(cat "$first_out")"
	assertEquals "Recursive destination-tree prefetch should also cache the root dataset properties for later lookups in the same iteration." \
		"compression=lz4=local,readonly=off=local" "$(cat "$second_out")"
	assertEquals "Prefetching the recursive destination property tree should use exactly one machine-readable and one human-readable zfs get." \
		"2" "$(awk 'END {print NR + 0}' "$log")"
	assertEquals "Recursive destination-tree prefetch should count as a single destination normalized-property read." \
		"1" "${g_zxfer_profile_normalized_property_reads_destination:-0}"
}

test_adjust_child_inherit_to_match_parent_uses_prefetched_destination_tree() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_prefetch.out"
	log="$TEST_TMPDIR/adjust_child_inherit_prefetch.calls"
	: >"$log"
	g_option_R_recursive="tank/src"
	g_option_P_transfer_property=1
	g_destination="backup/dst"
	g_RZFS="/sbin/zfs"
	g_recursive_dest_list="backup/dst
backup/dst/child"
	zxfer_refresh_property_tree_prefetch_context

	run_zfs_cmd_for_spec() {
		printf '%s %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" "$6" >>"$log"
		case "$3" in
		-r)
			case "$4" in
			-Hpo)
				printf '%s\t%s\t%s\t%s\n' \
					"backup/dst" "checksum" "sha256" "local" \
					"backup/dst" "atime" "off" "local" \
					"backup/dst/child" "checksum" "sha256" "inherited" \
					"backup/dst/child" "atime" "off" "inherited"
				;;
			-Ho)
				printf '%s\t%s\t%s\t%s\n' \
					"backup/dst" "checksum" "sha256" "local" \
					"backup/dst" "atime" "off" "local" \
					"backup/dst/child" "checksum" "sha256" "inherited" \
					"backup/dst/child" "atime" "off" "inherited"
				;;
			esac
			return 0
			;;
		esac
		printf '%s\n' "unexpected exact destination lookup"
		return 1
	}
	exists_destination() {
		printf '1\n'
	}

	adjust_child_inherit_to_match_parent "backup/dst/child" \
		"checksum=sha256=inherited,atime=off=inherited" \
		"" \
		"checksum=sha256,atime=off" \
		"$g_readonly_properties" >"$outfile"

	unset -f run_zfs_cmd_for_spec
	unset -f exists_destination

	assertEquals "Prefetched destination tree properties should still preserve inheritance when the parent already matches." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Parent comparison should use the prefetched destination tree instead of issuing a separate exact parent lookup." \
		"checksum=sha256,atime=off" "$(sed -n '2p' "$outfile")"
	assertEquals "Parent comparisons backed by destination-tree prefetch should use only the two recursive zfs get calls." \
		"2" "$(awk 'END {print NR + 0}' "$log")"
}

test_zxfer_prefetch_recursive_normalized_properties_handles_invalid_side_and_state_shortcuts() {
	set +e
	zxfer_prefetch_recursive_normalized_properties other >/dev/null 2>&1
	invalid_status=$?

	g_zxfer_source_property_tree_prefetch_state=1
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	ready_status=$?

	g_zxfer_source_property_tree_prefetch_state=2
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	disabled_status=$?

	assertEquals "Unknown property-tree prefetch sides should fail immediately." \
		"1" "$invalid_status"
	assertEquals "Ready source property-tree prefetch state should short-circuit successfully." \
		"0" "$ready_status"
	assertEquals "Disabled source property-tree prefetch state should short-circuit as unavailable." \
		"1" "$disabled_status"
}

test_zxfer_prefetch_recursive_normalized_properties_disables_missing_context_and_empty_filters() {
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"
	set +e
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	missing_root_status=$?

	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	missing_cmd_status=$?

	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list=" "
	g_recursive_source_list=""
	initial_source=""
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	empty_filter_status=$?

	assertEquals "Missing source property-tree roots should disable recursive prefetch for the iteration." \
		"1" "$missing_root_status"
	assertEquals "Missing source property-tree commands should disable recursive prefetch for the iteration." \
		"1" "$missing_cmd_status"
	assertEquals "Empty filtered dataset lists should disable recursive prefetch for the iteration." \
		"1" "$empty_filter_status"
	assertEquals "Source property-tree prefetch should stay disabled after an empty-filter failure." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
}

test_zxfer_prefetch_recursive_normalized_properties_disables_destination_missing_context_and_empty_filters() {
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_destination_property_tree_prefetch_state=0
	g_recursive_dest_list=""
	set +e
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	missing_dataset_list_status=$?

	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_destination_property_tree_prefetch_state=0
	g_recursive_dest_list="backup/dst"
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	missing_root_status=$?

	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	missing_cmd_status=$?

	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_destination_property_tree_prefetch_state=0
	g_recursive_dest_list=" "
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	empty_filter_status=$?

	assertEquals "Missing destination dataset lists should disable recursive destination property-tree prefetch for the iteration." \
		"1" "$missing_dataset_list_status"
	assertEquals "Missing destination property-tree roots should disable destination-side prefetch for the iteration." \
		"1" "$missing_root_status"
	assertEquals "Missing destination property-tree commands should disable destination-side prefetch for the iteration." \
		"1" "$missing_cmd_status"
	assertEquals "Empty destination filter lists should disable destination-side prefetch for the iteration." \
		"1" "$empty_filter_status"
	assertEquals "Destination property-tree prefetch should stay disabled after an empty-filter failure." \
		"2" "${g_zxfer_destination_property_tree_prefetch_state:-0}"
}

test_zxfer_prefetch_recursive_normalized_properties_disables_failed_recursive_reads_and_grouping() {
	g_option_V_very_verbose=1
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"

	run_zfs_cmd_for_spec() {
		printf '%s\n' "ssh timeout"
		return 1
	}
	set +e
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	read_failure_status=$?
	unset -f run_zfs_cmd_for_spec

	g_zxfer_source_property_tree_prefetch_state=0
	run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo | -Ho)
			printf '%s\t%s\t%s\t%s\n' "tank/src" "compression" "lz4" "local"
			return 0
			;;
		esac
		return 1
	}
	zxfer_group_recursive_property_tree_by_dataset() {
		return 1
	}
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	group_failure_status=$?
	unset -f run_zfs_cmd_for_spec
	unset -f zxfer_group_recursive_property_tree_by_dataset

	assertEquals "Recursive source property-tree prefetch should fail closed when the recursive zfs get probe fails." \
		"1" "$read_failure_status"
	assertEquals "Failed recursive zfs get probes should disable source property-tree prefetch for the rest of the iteration." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertEquals "Grouping failures should also fail closed when building the prefetched property tree." \
		"1" "$group_failure_status"
}

test_zxfer_prefetch_recursive_normalized_properties_disables_failed_destination_reads_and_grouping() {
	g_option_V_very_verbose=1
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_destination_property_tree_prefetch_state=0
	g_recursive_dest_list="backup/dst"

	run_zfs_cmd_for_spec() {
		printf '%s\n' "ssh timeout"
		return 1
	}
	set +e
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	read_failure_status=$?
	unset -f run_zfs_cmd_for_spec

	g_zxfer_destination_property_tree_prefetch_state=0
	run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo | -Ho)
			printf '%s\t%s\t%s\t%s\n' "backup/dst" "compression" "lz4" "local"
			return 0
			;;
		esac
		return 1
	}
	group_call_count=0
	zxfer_group_recursive_property_tree_by_dataset() {
		group_call_count=$((group_call_count + 1))
		if [ "$group_call_count" -eq 1 ]; then
			printf '%s\t%s\n' "backup/dst" "compression=lz4=local"
			return 0
		fi
		return 1
	}
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	group_failure_status=$?
	unset -f run_zfs_cmd_for_spec
	unset -f zxfer_group_recursive_property_tree_by_dataset

	assertEquals "Recursive destination property-tree prefetch should fail closed when the recursive zfs get probe fails." \
		"1" "$read_failure_status"
	assertEquals "Failed recursive destination zfs get probes should disable destination-side property-tree prefetch for the rest of the iteration." \
		"2" "${g_zxfer_destination_property_tree_prefetch_state:-0}"
	assertEquals "Destination grouping failures should also fail closed when building the prefetched property tree." \
		"1" "$group_failure_status"
}

test_zxfer_maybe_prefetch_recursive_normalized_properties_handles_mismatches_and_missing_cache_entries() {
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/source/zfs"
	g_recursive_source_dataset_list="tank/src"
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/dest/zfs"
	g_recursive_dest_list="backup/dst"

	set +e
	zxfer_maybe_prefetch_recursive_normalized_properties "tank/src" "/wrong/zfs" source >/dev/null 2>&1
	wrong_cmd_status=$?
	zxfer_maybe_prefetch_recursive_normalized_properties "tank/other" "/source/zfs" source >/dev/null 2>&1
	missing_source_status=$?
	zxfer_maybe_prefetch_recursive_normalized_properties "backup/other" "/dest/zfs" destination >/dev/null 2>&1
	missing_dest_status=$?

	zxfer_prefetch_recursive_normalized_properties() {
		return 0
	}
	zxfer_maybe_prefetch_recursive_normalized_properties "tank/src" "/source/zfs" source >/dev/null 2>&1
	missing_cache_status=$?
	unset -f zxfer_prefetch_recursive_normalized_properties

	assertEquals "Mismatched source zfs commands should bypass recursive property-tree prefetch." \
		"1" "$wrong_cmd_status"
	assertEquals "Datasets outside the recursive source tree should bypass source-side property-tree prefetch." \
		"1" "$missing_source_status"
	assertEquals "Datasets outside the recursive destination tree should bypass destination-side property-tree prefetch." \
		"1" "$missing_dest_status"
	assertEquals "A prefetch pass that does not materialize the requested dataset cache entry should still fall back to exact live reads." \
		"1" "$missing_cache_status"
}

test_force_readonly_off_handles_empty_and_rewrites_property() {
	assertEquals "Empty property lists should stay empty." "" "$(force_readonly_off "")"
	assertEquals "readonly=on entries should be forced to readonly=off." \
		"readonly=off=local,compression=lz4=local" \
		"$(force_readonly_off "readonly=on=local,compression=lz4=local")"
}

test_collect_source_props_uses_backup_restore_and_force_writable() {
	output_file="$TEST_TMPDIR/collect_source_restore.out"

	(
		get_normalized_dataset_properties() {
			printf '%s\n' "compression=lz4=local,readonly=on=local"
		}
		g_option_e_restore_property_mode=1
		g_restored_backup_file_contents="tank/src,backup/dst,readonly=on=local,compression=lz4=local"
		collect_source_props "tank/src" "backup/dst" 1 ""
		printf 'raw=%s\n' "$m_source_pvs_raw" >"$output_file"
		printf 'effective=%s\n' "$m_source_pvs_effective" >>"$output_file"
	)

	result=$(cat "$output_file")
	assertContains "Raw source properties should come from the live source query." \
		"$result" "raw=compression=lz4=local,readonly=on=local"
	assertContains "Restore mode should pull the backup entry and force readonly=off when requested." \
		"$result" "effective=readonly=off=local,compression=lz4=local"
}

test_collect_source_props_supports_legacy_backup_order() {
	output_file="$TEST_TMPDIR/collect_source_legacy.out"

	(
		get_normalized_dataset_properties() {
			printf '%s\n' "compression=lz4=local"
		}
		g_option_e_restore_property_mode=1
		g_restored_backup_file_contents="backup/dst,tank/src,quota=1G=local"
		collect_source_props "tank/src" "backup/dst" 0 ""
		printf '%s\n' "$m_source_pvs_effective" >"$output_file"
	)

	assertEquals "Legacy backup ordering should still be restored when present." \
		"quota=1G=local" "$(cat "$output_file")"
}

test_collect_source_props_restore_mode_requires_exact_destination_match() {
	set +e
	output=$(
		(
			get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=$(printf '%s\n%s\n' \
				"tank/src,backup/other,compression=off=local" \
				"backup/other,tank/src,compression=off=local")
			collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed when backup metadata has only wrong-destination rows for the requested source dataset." \
		2 "$status"
	assertContains "Wrong-destination restore failures should identify both the source and destination datasets." \
		"$output" "Can't find the properties for the filesystem tank/src and destination backup/dst"
}

test_collect_source_props_fails_when_backup_entry_missing() {
	set +e
	output=$(
		(
			get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=""
			collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Missing restored property metadata should abort with usage status." 2 "$status"
	assertContains "Missing restored property metadata should identify both the source and destination datasets." \
		"$output" "Can't find the properties for the filesystem tank/src and destination backup/dst"
}

test_collect_source_props_propagates_normalized_property_lookup_failures() {
	set +e
	output=$(
		(
			get_normalized_dataset_properties() {
				printf '%s\n' "permission denied"
				return 1
			}
			collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Source property collection should return a failure when normalized source inspection fails." \
		"1" "$status"
	assertEquals "Source property collection should preserve the normalized-property lookup failure output." \
		"permission denied" "$output"
}

test_collect_source_props_rejects_ambiguous_restore_entries_for_exact_pair() {
	set +e
	output=$(
		(
			get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=$(printf '%s\n%s\n' \
				"tank/src,backup/dst,compression=lz4=local" \
				"tank/src,backup/dst,compression=off=local")
			collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should reject duplicate exact source/destination backup rows." \
		2 "$status"
	assertContains "Ambiguous restore failures should identify both the source and destination datasets." \
		"$output" "Multiple restored property entries matched filesystem tank/src and destination backup/dst"
}

test_collect_source_props_restore_mode_matches_exact_awkward_dataset_tails() {
	output_file="$TEST_TMPDIR/collect_source_awkward_tail.out"

	(
		get_normalized_dataset_properties() {
			printf '%s\n' "compression=off=local"
		}
		g_option_e_restore_property_mode=1
		g_restored_backup_file_contents=$(printf '%s\n%s\n' \
			"tank/src/child.tail-010,backup/dst,user:note=value%2Cwith%2Ccommas=local" \
			"tank/src/child.tail-01,backup/dst,user:note=value%3Dwith%3Dequals%3Band%3Bsemicolon=local")
		collect_source_props "tank/src/child.tail-01" "backup/dst" 0 ""
		printf '%s\n' "$m_source_pvs_effective" >"$output_file"
	)

	assertEquals "Restore-mode source matching should select the exact awkward dataset tail and preserve the encoded serialized payload." \
		"user:note=value%3Dwith%3Dequals%3Band%3Bsemicolon=local" "$(cat "$output_file")"
}

test_collect_source_props_restore_mode_matches_exact_awkward_dataset_tails() {
	output_file="$TEST_TMPDIR/collect_source_awkward_tail.out"

	(
		get_normalized_dataset_properties() {
			printf '%s\n' "compression=off=local"
		}
		g_option_e_restore_property_mode=1
		g_restored_backup_file_contents=$(printf '%s\n%s\n' \
			"tank/src/child.tail-010,backup/dst,user:note=value,with,commas=local" \
			"tank/src/child.tail-01,backup/dst,user:note=value=with=equals;and:semicolon=local")
		collect_source_props "tank/src/child.tail-01" "backup/dst" 0 ""
		printf '%s\n' "$m_source_pvs_effective" >"$output_file"
	)

	assertEquals "Restore-mode source matching should select the exact awkward dataset tail and preserve the raw serialized payload." \
		"user:note=value=with=equals;and:semicolon=local" "$(cat "$output_file")"
}

test_validate_override_properties_returns_success_for_empty_list_in_current_shell() {
	validate_override_properties "" "compression=lz4=local"
	status=$?

	assertEquals "Empty override lists should validate successfully." 0 "$status"
}

test_validate_override_properties_rejects_missing_source_property() {
	set +e
	output=$(
		(
			throw_usage_error() {
				printf '%s\n' "$1"
				exit 1
			}
			validate_override_properties "quota=1G" "compression=lz4=local"
		)
	)
	status=$?

	assertEquals "Override validation should fail when -o references a property absent from the source set." \
		"1" "$status"
	assertContains "Override validation failures should preserve the current usage-error message." \
		"$output" "Invalid option property"
}

test_derive_override_lists_preserves_override_only_mode_order() {
	output_file="$TEST_TMPDIR/derive_override_only.out"

	derive_override_lists "" "compression=lz4,quota=1G" "0" "filesystem" >"$output_file"

	assertEquals "Override-only derivation should emit the requested override list in option order." \
		"compression=lz4=override,quota=1G=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should leave the creation-property list empty." \
		"" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_prefers_first_matching_override_when_transferring_all_properties() {
	output_file="$TEST_TMPDIR/derive_override_all.out"

	derive_override_lists \
		"compression=lz4=local,quota=1G=inherited,refreservation=8G=received" \
		"compression=gzip-9,compression=off,atime=off" \
		"1" \
		"volume" >"$output_file"

	assertEquals "Transfer-all derivation should keep source order and apply only the first matching override for a property." \
		"compression=gzip-9=override,quota=1G=inherited,refreservation=8G=received" "$(sed -n '1p' "$output_file")"
	assertEquals "Transfer-all derivation should still keep zvol refreservation in the creation-property set even when it is not local." \
		"refreservation=8G=received" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_escapes_override_values_after_first_equals() {
	output_file="$TEST_TMPDIR/derive_override_delimiter_values.out"

	derive_override_lists "" "user:note=value=with=equals;semi" "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should preserve the full override value by escaping internal delimiters after the first equals sign." \
		"user:note=value%3Dwith%3Dequals%3Bsemi=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should still leave the creation-property list empty for delimiter-heavy values." \
		"" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_skips_volblocksize_for_filesystems() {
	output_file="$TEST_TMPDIR/derive_override_no_volblocksize.out"

	derive_override_lists \
		"volblocksize=16K=local,compression=lz4=local" \
		"" \
		"1" \
		"filesystem" >"$output_file"

	assertEquals "Filesystem property transfer should not carry zvol-only volblocksize into the override list." \
		"compression=lz4=local" "$(sed -n '1p' "$output_file")"
	assertEquals "Filesystem property transfer should still keep legitimate local filesystem creation properties when zvol-only volblocksize is filtered out." \
		"compression=lz4=local" "$(sed -n '2p' "$output_file")"
}

test_validate_override_properties_reports_awk_failures() {
	set +e
	output=$(
		(
			broken_awk() {
				return 2
			}
			g_cmd_awk="broken_awk"
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			validate_override_properties "compression=lz4" "compression=lz4=local"
		)
	)
	status=$?

	assertEquals "Override validation should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Override validation awk failures should surface the helper failure message." \
		"$output" "Failed to validate override properties."
}

test_derive_override_lists_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			derive_override_lists "compression=lz4=local" "compression=gzip-9" "1" "filesystem"
		)
	)
	status=$?

	assertEquals "Override-list derivation should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Override-list derivation awk failures should surface the helper failure message." \
		"$output" "Failed to derive override property lists."
}

test_sanitize_property_list_returns_empty_for_empty_input() {
	assertEquals "Empty property lists should remain empty after sanitization." "" \
		"$(sanitize_property_list "" "$g_readonly_properties" "$g_option_I_ignore_properties")"
}

test_strip_unsupported_properties_returns_input_when_no_unsupported_properties() {
	assertEquals "Unsupported-property stripping should no-op when no unsupported list is present." \
		"compression=lz4=local" "$(strip_unsupported_properties "compression=lz4=local" "")"
}

test_strip_unsupported_properties_keeps_stdout_clean_when_verbose() {
	stdout_log="$TEST_TMPDIR/unsupported_stdout.log"
	stderr_log="$TEST_TMPDIR/unsupported_stderr.log"
	unsupported_properties="compression"
	g_option_v_verbose=1

	strip_unsupported_properties "compression=lz4=local,quota=1G=local" "$unsupported_properties" >"$stdout_log" 2>"$stderr_log"

	assertEquals "Unsupported-property filtering should return only supported properties on stdout." \
		"quota=1G=local" "$(cat "$stdout_log")"
	assertContains "Verbose unsupported-property notices should go to stderr." \
		"$(cat "$stderr_log")" "Destination does not support property compression=lz4"
}

test_strip_unsupported_properties_decodes_verbose_delimiter_heavy_values() {
	stdout_log="$TEST_TMPDIR/unsupported_encoded_stdout.log"
	stderr_log="$TEST_TMPDIR/unsupported_encoded_stderr.log"
	unsupported_properties="user:note"
	g_option_v_verbose=1

	strip_unsupported_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local" "$unsupported_properties" >"$stdout_log" 2>"$stderr_log"

	assertEquals "Unsupported-property filtering should still remove encoded delimiter-heavy properties from stdout." \
		"" "$(cat "$stdout_log")"
	assertContains "Verbose unsupported-property notices should decode delimiter-heavy values before logging." \
		"$(cat "$stderr_log")" "Destination does not support property user:note=value,with,commas=and;semi"
}

test_strip_unsupported_properties_reports_awk_failures() {
	set +e
	# shellcheck disable=SC2030  # Test-local subshell stubs intentionally do not escape.
	output=$(
		(
			g_cmd_awk="false"
			unsupported_properties="compression"
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			strip_unsupported_properties "compression=lz4=local" "$unsupported_properties"
		)
	)
	status=$?

	assertEquals "Unsupported-property filtering should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Unsupported-property filtering awk failures should surface the helper failure message." \
		"$output" "Failed to filter unsupported destination properties."
}

test_ensure_required_properties_present_appends_missing_creation_time_props() {
	result=$(
		(
			run_zfs_cmd_for_spec() {
				if [ "$5" = "casesensitivity" ]; then
					printf 'casesensitivity\tsensitive\tlocal\n'
					return 0
				fi
				printf '%s\n' "invalid property"
				return 1
			}
			ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only"
		)
	)

	assertEquals "Missing required creation-time properties should be appended from explicit zfs get queries." \
		"compression=lz4=local,casesensitivity=sensitive=local" "$result"
}

test_ensure_required_properties_present_appends_missing_creation_time_props_to_empty_lists() {
	result=$(
		(
			g_LZFS="/default/source/zfs"
			run_zfs_cmd_for_spec() {
				if [ "$1" != "/default/source/zfs" ]; then
					printf '%s\n' "unexpected zfs command: $1"
					return 1
				fi
				printf 'casesensitivity\tsensitive\tlocal\n'
			}
			ensure_required_properties_present "tank/src" "" "" "casesensitivity"
		)
	)

	assertEquals "Required creation-time properties should seed an empty property list and default to the source zfs command when none is supplied." \
		"casesensitivity=sensitive=local" "$result"
}

test_ensure_required_properties_present_tracks_backfill_probe_count_when_very_verbose() {
	output_file="$TEST_TMPDIR/required_property_profile.out"
	old_very_verbose=${g_option_V_very_verbose-}

	g_option_V_very_verbose=1

	(
		run_zfs_cmd_for_spec() {
			if [ "$5" = "casesensitivity" ]; then
				printf 'casesensitivity\tsensitive\tlocal\n'
				return 0
			fi
			printf '%s\n' "not supported"
			return 1
		}
		ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" >/dev/null
		printf '%s\n' "${g_zxfer_profile_required_property_backfill_gets:-0}" >"$output_file"
	)

	g_option_V_very_verbose=$old_very_verbose

	assertEquals "Very-verbose profiling should count explicit must-create backfill probes." \
		"2" "$(cat "$output_file")"
}

test_ensure_required_properties_present_caches_explicit_probe_results_by_side_and_dataset() {
	log="$TEST_TMPDIR/required_property_cache.log"
	: >"$log"
	g_option_V_very_verbose=1
	calls_log="$TEST_TMPDIR/required_property_cache.calls"
	: >"$calls_log"

	run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$5" = "casesensitivity" ]; then
			printf 'casesensitivity\tsensitive\tlocal\n'
			return 0
		fi
		printf '%s\n' "not supported"
		return 1
	}

	ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" source >"$log"
	ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" source >>"$log"
	{
		printf 'calls=%s\n' "$(awk 'END {print NR + 0}' "$calls_log")"
		printf 'reads=%s\n' "${g_zxfer_profile_required_property_backfill_gets:-0}"
	} >>"$log"

	assertEquals "Explicit must-create property probes should be cached per side and dataset, including unsupported-property misses." \
		"compression=lz4=local,casesensitivity=sensitive=local
compression=lz4=local,casesensitivity=sensitive=local
calls=2
reads=2" "$(cat "$log")"
}

test_ensure_required_properties_present_skips_nonapplicable_creation_time_props() {
	result=$(
		(
			run_zfs_cmd_for_spec() {
				printf '%s\n' "cannot get property: property does not apply to datasets of this type"
				return 1
			}
			ensure_required_properties_present "tank/vol" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only"
		)
	)

	assertEquals "Explicit must-create probes that clearly do not apply to the dataset type should be skipped." \
		"compression=lz4=local" "$result"
}

test_ensure_required_properties_present_reports_parse_failures_for_malformed_probe_output() {
	set +e
	output=$(
		(
			run_zfs_cmd_for_spec() {
				printf 'casesensitivity\tinvalid\n'
				return 0
			}
			ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity"
		)
	)
	status=$?

	assertEquals "Malformed must-create probe output should return non-zero." 1 "$status"
	assertContains "Malformed must-create probe output should identify the property and dataset." \
		"$output" "Failed to parse required creation-time property [casesensitivity] for dataset [tank/src]"
}

test_ensure_required_properties_present_reports_probe_failures_for_required_props() {
	set +e
	output=$(
		(
			run_zfs_cmd_for_spec() {
				printf '%s\n' "permission denied"
				return 1
			}
			ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity"
		)
	)
	status=$?

	assertEquals "Unexpected must-create probe failures should return non-zero." 1 "$status"
	assertContains "Probe failures should identify the missing required property and dataset." \
		"$output" "Failed to retrieve required creation-time property [casesensitivity] for dataset [tank/src]: permission denied"
}

test_get_validated_source_dataset_create_metadata_returns_filesystem_without_volsize() {
	result=$(
		(
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			get_validated_source_dataset_create_metadata "tank/src"
		)
	)

	assertEquals "Filesystem metadata validation should return the type and a blank volume size." \
		"filesystem" "$result"
}

test_get_validated_source_dataset_create_metadata_reports_type_probe_failures() {
	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				printf '%s\n' "permission denied"
				return 1
			}
			get_validated_source_dataset_create_metadata "tank/src"
		)
	)
	status=$?

	assertEquals "Source type probe failures should abort metadata validation." 1 "$status"
	assertContains "Type probe failures should identify the source dataset." \
		"$output" "Failed to retrieve source dataset type for [tank/src]: permission denied"
}

test_get_validated_source_dataset_create_metadata_reports_unknown_type_output() {
	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				printf '%s\n' "snapshot"
			}
			get_validated_source_dataset_create_metadata "tank/src"
		)
	)
	status=$?

	assertEquals "Unexpected source type output should abort metadata validation." 1 "$status"
	assertContains "Unexpected source type output should be surfaced." \
		"$output" "Invalid source dataset type for [tank/src]: snapshot"
}

test_get_validated_source_dataset_create_metadata_requires_nonempty_volsize_for_volumes() {
	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "volume"
				elif [ "$4" = "volsize" ]; then
					printf '\n'
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			get_validated_source_dataset_create_metadata "tank/vol"
		)
	)
	status=$?

	assertEquals "Volume metadata validation should reject empty volsize output." 1 "$status"
	assertContains "Empty volsize output should identify the source zvol." \
		"$output" "Failed to retrieve source zvol size for [tank/vol]: empty volsize"
}

test_get_validated_source_dataset_create_metadata_reports_volsize_probe_failures() {
	set +e
	output=$(
		(
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "volume"
				elif [ "$4" = "volsize" ]; then
					printf '%s\n' "ssh timeout"
					return 1
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			get_validated_source_dataset_create_metadata "tank/vol"
		)
	)
	status=$?

	assertEquals "Volume metadata validation should abort on volsize probe failures." 1 "$status"
	assertContains "Volsize probe failures should identify the source zvol." \
		"$output" "Failed to retrieve source zvol size for [tank/vol]: ssh timeout"
}

test_get_required_creation_properties_for_dataset_type_skips_filesystem_only_props_for_volumes() {
	assertEquals "Volumes should not probe filesystem-only must-create properties." \
		"" "$(get_required_creation_properties_for_dataset_type "volume")"
	assertEquals "Filesystems should continue to enforce must-create creation properties." \
		"casesensitivity,normalization,jailed,utf8only" "$(get_required_creation_properties_for_dataset_type "filesystem")"
}

test_ensure_destination_exists_returns_one_when_dataset_already_exists() {
	set +e
	ensure_destination_exists 1 1 "" "" filesystem "" "backup/dst" "$g_readonly_properties" ""
	status=$?

	assertEquals "Existing destinations should skip creation and return 1." 1 "$status"
}

test_ensure_destination_exists_initial_source_adds_parents_when_missing() {
	result=$(
		(
			exists_destination() {
				printf '0\n'
			}
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			ensure_destination_exists 0 1 "compression=lz4=local,atime=off=override" "" filesystem "" "backup/dst/child" "$g_readonly_properties" create_runner
		)
	)

	assertEquals "Initial-source creation should add parents when the parent dataset is missing." \
		"yes|filesystem||compression=lz4,atime=off|backup/dst/child" "$result"
}

test_ensure_destination_exists_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/dst] exists: permission denied"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$g_readonly_properties" create_runner
		)
	)
	status=$?

	assertEquals "Parent destination probe failures should abort creation planning." 1 "$status"
	assertContains "Parent destination probe failures should surface the probe error." \
		"$output" "Failed to determine whether destination dataset [backup/dst] exists: permission denied"
}

test_ensure_destination_exists_child_uses_creation_properties() {
	result=$(
		(
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			g_option_I_ignore_properties="mountpoint"
			ensure_destination_exists 0 0 "" "mountpoint=/mnt=local,readonly=off=local,compression=lz4=local" filesystem "" "backup/dst/child" "readonly" create_runner
		)
	)

	assertEquals "Child dataset creation should use the supplied readonly list, filtered creation properties, and always create parents." \
		"yes|filesystem||compression=lz4|backup/dst/child" "$result"
}

test_ensure_destination_exists_reports_create_failures() {
	set +e
	output=$(
		(
			create_runner() {
				return 1
			}
			exists_destination() {
				printf '0\n'
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst" "$g_readonly_properties" create_runner
		)
	)
	status=$?

	assertEquals "Create-runner failures should abort destination creation." 1 "$status"
	assertContains "Create-runner failures should use the destination-creation error." \
		"$output" "Error when creating destination filesystem."
}

test_ensure_destination_exists_uses_default_runner_when_unspecified_in_current_shell() {
	log="$TEST_TMPDIR/default_create_runner.log"
	run_zfs_create_with_properties() {
		printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5" >"$log"
	}
	g_readonly_properties=""
	g_option_I_ignore_properties=""

	ensure_destination_exists 0 0 "" "readonly=off=local,compression=lz4=local" filesystem "" "backup/dst/child" "readonly" ""
	status=$?

	unset -f run_zfs_create_with_properties

	assertEquals "Blank create-runner arguments should fall back to the default zfs create helper." 0 "$status"
	assertEquals "Default create-runner selection should sanitize creation properties using the supplied readonly list before invocation." \
		"yes|filesystem||compression=lz4|backup/dst/child" "$(cat "$log")"
}

test_ensure_destination_exists_marks_created_hierarchy_in_cache() {
	# shellcheck disable=SC2030
	output=$(
		g_recursive_dest_list=""
		zxfer_mark_destination_root_missing_in_cache "backup/dst"
		exists_destination() {
			printf '0\n'
		}
		create_runner() {
			return 0
		}
		ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$g_readonly_properties" create_runner
		printf 'root=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst")"
		printf 'child=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/child")"
		printf 'sibling=%s\n' "$(zxfer_get_destination_existence_cache_entry "backup/dst/sibling")"
		printf 'dests=%s\n' "$g_recursive_dest_list"
	)

	assertContains "Successful destination creates should mark the destination root as existing in the cache." \
		"$output" "root=1"
	assertContains "Successful destination creates should mark the created dataset as existing in the cache." \
		"$output" "child=1"
	assertContains "Uncreated siblings should still be inferred missing under the authoritative root cache." \
		"$output" "sibling=0"
	assertContains "Successful destination creates should append the created dataset to the in-memory recursive destination list." \
		"$output" "dests=backup/dst/child"
}

test_ensure_destination_exists_appends_created_dataset_without_whitespace_prefix_when_root_already_tracked() {
	# shellcheck disable=SC2030
	output=$(
		g_recursive_dest_list="backup/dst"
		zxfer_mark_destination_root_missing_in_cache "backup/dst"
		zxfer_set_destination_existence_cache_entry "backup/dst" 1
		exists_destination() {
			printf '1\n'
		}
		create_runner() {
			return 0
		}
		ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$g_readonly_properties" create_runner
		printf 'dests=%s\n' "$g_recursive_dest_list"
	)

	assertContains "Appending a created dataset under an already-tracked destination root should preserve exact newline-delimited dataset names without leading whitespace." \
		"$output" "dests=backup/dst
backup/dst/child"
}

test_collect_destination_props_defaults_to_g_rzfs() {
	output_file="$TEST_TMPDIR/collect_destination_props_default.out"

	load_destination_props() {
		m_destination_pvs_raw="$1|${2:-$g_RZFS}"
	}
	g_RZFS="/remote/zfs"
	collect_destination_props "backup/dst" "" >"$output_file"
	# shellcheck source=src/zxfer_property_cache.sh
	. "$ZXFER_ROOT/src/zxfer_property_cache.sh"
	# shellcheck source=src/zxfer_transfer_properties.sh
	. "$ZXFER_ROOT/src/zxfer_transfer_properties.sh"

	assertEquals "Destination property collection should default to g_RZFS." \
		"backup/dst|/remote/zfs" "$(cat "$output_file")"
}

test_load_destination_props_defaults_to_g_rzfs_and_records_raw_props() {
	output_file="$TEST_TMPDIR/load_destination_props_default.out"

	zxfer_load_normalized_dataset_properties() {
		printf 'dataset=%s|zfs=%s|side=%s\n' "$1" "$2" "$3" >"$output_file"
		m_normalized_dataset_properties="compression=lz4=local"
	}
	g_RZFS="/remote/zfs"
	load_destination_props "backup/dst" ""
	printf 'raw=%s\n' "$m_destination_pvs_raw" >>"$output_file"
	# shellcheck source=src/zxfer_property_cache.sh
	. "$ZXFER_ROOT/src/zxfer_property_cache.sh"
	# shellcheck source=src/zxfer_transfer_properties.sh
	. "$ZXFER_ROOT/src/zxfer_transfer_properties.sh"

	assertEquals "Destination property loading should default to g_RZFS, use the destination cache side, and store the raw normalized properties." \
		"dataset=backup/dst|zfs=/remote/zfs|side=destination
raw=compression=lz4=local" "$(cat "$output_file")"
}

test_load_destination_props_propagates_lookup_failures() {
	set +e
	output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			load_destination_props "backup/dst" ""
		)
	)
	status=$?

	assertEquals "Destination property loading should fail when normalized inspection fails." \
		"1" "$status"
	assertEquals "Destination property loading should preserve normalized inspection errors." \
		"ssh timeout" "$output"
}

test_ensure_destination_exists_invalidates_destination_cache_after_live_create() {
	log="$TEST_TMPDIR/create_invalidation.log"
	: >"$log"

	(
		exists_destination() {
			printf '1\n'
		}
		create_runner() {
			return 0
		}
		zxfer_invalidate_destination_property_cache() {
			printf '%s\n' "$1" >>"$log"
		}
		ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst" "$g_readonly_properties" create_runner
	)

	assertEquals "Successful live destination creation should invalidate the destination property cache for that dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_set_property_handles_dry_run_and_failures() {
	g_option_n_dryrun=1
	g_RZFS="/remote/zfs"
	assertEquals "Dry-run property sets should render the destination command." \
		"/remote/zfs 'set' 'quota=1G' 'backup/dst'" \
		"$(zxfer_run_zfs_set_property quota 1G backup/dst)"

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_n_dryrun=0
			zxfer_run_zfs_set_property quota 1G backup/dst
		)
	)
	status=$?

	assertEquals "Live property-set failures should abort." 1 "$status"
	assertContains "Live property-set failures should surface the set error." \
		"$output" "Error when setting properties on destination filesystem."
}

test_zxfer_run_zfs_set_property_preserves_literal_assignment_for_local_exec() {
	log="$TEST_TMPDIR/set_property_local.log"
	l_property="user:test\$\\\`\"\\\\"
	l_value="value with spaces \$\\\`\"\\\\"

	run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_set_property "$l_property" "$l_value" "backup/dst"

	unset -f run_destination_zfs_cmd

	assertEquals "Live local property sets should pass the literal assignment to zfs without shell-style escaping." \
		"$(printf '%s\n' "set" "$l_property=$l_value" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_property_invalidates_destination_cache_after_live_set() {
	log="$TEST_TMPDIR/set_invalidation.log"
	: >"$log"

	(
		run_destination_zfs_cmd() {
			return 0
		}
		zxfer_invalidate_destination_property_cache() {
			printf '%s\n' "$1" >>"$log"
		}

		g_option_n_dryrun=0
		zxfer_run_zfs_set_property quota 1G "backup/dst"
	)

	assertEquals "Successful live property sets should invalidate the destination property cache for that dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_set_properties_batches_assignments_for_local_exec() {
	log="$TEST_TMPDIR/set_properties_local.log"

	run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_set_properties "compression=lz4,atime=off" "backup/dst"

	unset -f run_destination_zfs_cmd

	assertEquals "Live local batched property sets should pass every assignment to a single zfs set invocation." \
		"$(printf '%s\n' "set" "compression=lz4" "atime=off" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_properties_renders_single_dry_run_command() {
	g_option_n_dryrun=1
	g_RZFS="/remote/zfs"

	assertEquals "Dry-run batched property sets should render one readable destination command per dataset." \
		"/remote/zfs 'set' 'compression=lz4' 'atime=off' 'backup/dst'" \
		"$(zxfer_run_zfs_set_properties "compression=lz4,atime=off" backup/dst)"
}

test_zxfer_run_zfs_set_properties_invalidates_destination_cache_once_after_live_set() {
	log="$TEST_TMPDIR/set_properties_invalidation.log"
	: >"$log"

	(
		run_destination_zfs_cmd() {
			return 0
		}
		zxfer_invalidate_destination_property_cache() {
			printf '%s\n' "$1" >>"$log"
		}

		g_option_n_dryrun=0
		zxfer_run_zfs_set_properties "compression=lz4,atime=off" "backup/dst"
	)

	assertEquals "Successful live batched property sets should invalidate the destination property cache once for the dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_set_properties_skips_empty_assignments_when_batching() {
	log="$TEST_TMPDIR/set_properties_skip_empty.log"

	(
		zxfer_run_zfs_set_assignments() {
			printf '%s\n' "$@" >"$log"
		}
		zxfer_run_zfs_set_properties "compression=lz4,,atime=off," "backup/dst"
	)

	assertEquals "Batched property sets should drop empty assignments before invoking the set runner." \
		"$(printf '%s\n' "backup/dst" "compression=lz4" "atime=off")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_assignments_returns_success_without_assignments() {
	zxfer_run_zfs_set_assignments "backup/dst"
	status=$?

	assertEquals "Batched assignment execution should no-op successfully when there are no assignments." \
		"0" "$status"
}

test_zxfer_run_zfs_set_property_fuzz_preserves_delimiter_heavy_values_for_local_exec() {
	current_log=""
	l_property="user:zxfer.fuzz"
	case_file="$TEST_TMPDIR/set_property_local_fuzz_cases.txt"
	cat >"$case_file" <<'EOF'
value,with,commas|backup/dst.child-01
value=with=equals|backup/dst.tail-02
value;with:semicolon|backup/dst.tail-03
value,=; mixed|backup/dst.tail-04
EOF

	run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$current_log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	case_index=0
	while IFS='|' read -r l_value l_destination || [ -n "$l_value$l_destination" ]; do
		[ -n "$l_destination" ] || continue
		case_index=$((case_index + 1))
		current_log="$TEST_TMPDIR/set_property_local_fuzz_$case_index.log"
		zxfer_run_zfs_set_property "$l_property" "$l_value" "$l_destination"
		assertEquals "Local property fuzz case $case_index should preserve the literal assignment and dataset tail." \
			"$(printf '%s\n' "set" "$l_property=$l_value" "$l_destination")" "$(cat "$current_log")"
	done <"$case_file"

	unset -f run_destination_zfs_cmd
}

test_zxfer_run_zfs_set_property_preserves_literal_assignment_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set.log"
	l_property="user:test\$\\\`\"\\\\"
	l_value="value with spaces \$\\\`\"\\\\"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
/bin/sh -c "$remote_cmd"
EOF
	chmod +x "$fake_ssh"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example"
	g_target_cmd_zfs="$remote_zfs"

	zxfer_run_zfs_set_property "$l_property" "$l_value" "backup/dst"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote property sets should preserve the literal assignment after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "set" "$l_property=$l_value" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "Remote property sets should keep the target host as the ssh destination." \
		"target.example" "$(sed -n '1p' "$ssh_log")"
}

test_zxfer_run_destination_zfs_property_command_passes_destination_profile_side_when_hosts_match() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_destination_profile_side"
	ssh_log="$TEST_TMPDIR/fake_ssh_destination_profile_side.log"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_origin_host=$g_option_O_origin_host
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}
	old_very_verbose=${g_option_V_very_verbose-}
	old_total_ssh=${g_zxfer_profile_ssh_shell_invocations-}
	old_source_ssh=${g_zxfer_profile_source_ssh_shell_invocations-}
	old_destination_ssh=${g_zxfer_profile_destination_ssh_shell_invocations-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
host=$1
shift
	printf '%s\n' "$host" >"$FAKE_SSH_LOG"
exit 0
EOF
	chmod +x "$fake_ssh"

	FAKE_SSH_LOG="$ssh_log"
	export FAKE_SSH_LOG

	g_cmd_ssh="$fake_ssh"
	g_option_O_origin_host="shared.example"
	g_option_T_target_host="shared.example"
	g_target_cmd_zfs="/remote/zfs"
	g_option_V_very_verbose=1
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0

	zxfer_run_destination_zfs_property_command set "user:test=value" "backup/dst"

	result_total_ssh=${g_zxfer_profile_ssh_shell_invocations:-0}
	result_source_ssh=${g_zxfer_profile_source_ssh_shell_invocations:-0}
	result_destination_ssh=${g_zxfer_profile_destination_ssh_shell_invocations:-0}

	unset FAKE_SSH_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_O_origin_host=$old_origin_host
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs
	g_option_V_very_verbose=$old_very_verbose
	g_zxfer_profile_ssh_shell_invocations=$old_total_ssh
	g_zxfer_profile_source_ssh_shell_invocations=$old_source_ssh
	g_zxfer_profile_destination_ssh_shell_invocations=$old_destination_ssh

	assertEquals "Destination-side property commands should keep the shared host as the ssh destination." \
		"shared.example" "$(cat "$ssh_log")"
	assertEquals "Destination-side property commands should increment the total ssh shell counter." \
		"1" "$result_total_ssh"
	assertEquals "Destination-side property commands should not increment the source-side ssh shell counter when origin and target share the same host spec." \
		"0" "$result_source_ssh"
	assertEquals "Destination-side property commands should increment the destination-side ssh shell counter when origin and target share the same host spec." \
		"1" "$result_destination_ssh"
}

test_zxfer_run_zfs_set_property_fuzz_preserves_delimiter_heavy_values_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set_fuzz"
	fake_doas="$TEST_TMPDIR/doas"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set_fuzz"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set_fuzz.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set_fuzz.log"
	case_file="$TEST_TMPDIR/set_property_remote_fuzz_cases.txt"
	l_property="user:zxfer.fuzz"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}
	old_fake_remote_path=${FAKE_REMOTE_PATH-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
PATH=${FAKE_REMOTE_PATH:-$PATH}
export PATH
/bin/sh -c "$remote_cmd"
EOF
	chmod +x "$fake_ssh"

	cat >"$fake_doas" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$fake_doas"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	cat >"$case_file" <<'EOF'
value,with,commas|backup/dst.child-01
value=with=equals|backup/dst.tail-02
value;with:semicolon|backup/dst.tail-03
value,=; mixed|backup/dst.tail-04
EOF

	FAKE_SSH_LOG="$ssh_log"
	FAKE_REMOTE_PATH="$TEST_TMPDIR:$PATH"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG FAKE_REMOTE_PATH ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example doas"
	g_target_cmd_zfs="$remote_zfs"
	case_index=0
	while IFS='|' read -r l_value l_destination || [ -n "$l_value$l_destination" ]; do
		[ -n "$l_destination" ] || continue
		case_index=$((case_index + 1))
		: >"$ssh_log"
		zxfer_run_zfs_set_property "$l_property" "$l_value" "$l_destination"
		assertEquals "Remote property fuzz case $case_index should preserve the literal assignment after ssh joins the remote command." \
			"$(printf '%s\n' "set" "$l_property=$l_value" "$l_destination")" "$(cat "$remote_log")"
		assertEquals "Remote property fuzz case $case_index should keep the target host separate from wrapper tokens." \
			"target.example" "$(sed -n '1p' "$ssh_log")"
		assertContains "Remote property fuzz case $case_index should preserve the doas wrapper in the remote command string." \
			"$(sed -n '2p' "$ssh_log")" "'doas'"
	done <"$case_file"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	if [ -n "${old_fake_remote_path:+set}" ]; then
		FAKE_REMOTE_PATH=$old_fake_remote_path
		export FAKE_REMOTE_PATH
	else
		unset FAKE_REMOTE_PATH
	fi
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs
}

test_zxfer_run_zfs_set_properties_preserves_literal_assignments_for_remote_exec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_set_properties"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_set_properties"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_set_properties.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_set_properties.log"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
	if [ -n "${FAKE_SSH_LOG:-}" ]; then
		printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
		printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
	fi
/bin/sh -c "$remote_cmd"
EOF
	chmod +x "$fake_ssh"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example"
	g_target_cmd_zfs="$remote_zfs"

	zxfer_run_zfs_set_properties "user:first=value with spaces,user:second=keep=equals" "backup/dst"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote batched property sets should preserve each literal assignment after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "set" "user:first=value with spaces" "user:second=keep=equals" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "Remote batched property sets should keep the target host as the ssh destination." \
		"target.example" "$(sed -n '1p' "$ssh_log")"
}

test_zxfer_run_zfs_set_properties_decodes_delimiter_heavy_assignments_for_local_exec() {
	log="$TEST_TMPDIR/set_properties_local_decoded.log"

	run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	zxfer_run_zfs_set_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "backup/dst"

	assertEquals "Batched property sets should decode delimiter-heavy values before invoking zfs set." \
		"$(printf '%s\n' "set" "user:note=value,with,commas=and;semi" "backup/dst")" "$(cat "$log")"

	unset -f run_destination_zfs_cmd
}

test_zxfer_run_zfs_inherit_property_handles_dry_run_and_failures() {
	g_option_n_dryrun=1
	g_RZFS="/remote/zfs"
	assertEquals "Dry-run inherit operations should render the destination command." \
		"/remote/zfs 'inherit' 'quota' 'backup/dst'" \
		"$(zxfer_run_zfs_inherit_property quota backup/dst)"

	set +e
	output=$(
		(
			run_destination_zfs_cmd() {
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_n_dryrun=0
			zxfer_run_zfs_inherit_property quota backup/dst
		)
	)
	status=$?

	assertEquals "Live inherit failures should abort." 1 "$status"
	assertContains "Live inherit failures should surface the inherit error." \
		"$output" "Error when inheriting properties on destination filesystem."
}

test_zxfer_run_zfs_inherit_property_invalidates_destination_cache_after_live_inherit() {
	log="$TEST_TMPDIR/inherit_invalidation.log"
	: >"$log"

	(
		run_destination_zfs_cmd() {
			return 0
		}
		zxfer_invalidate_destination_property_cache() {
			printf '%s\n' "$1" >>"$log"
		}

		g_option_n_dryrun=0
		zxfer_run_zfs_inherit_property quota "backup/dst"
	)

	assertEquals "Successful live property inheritance should invalidate the destination property cache for that dataset." \
		"backup/dst" "$(cat "$log")"
}

test_zxfer_run_zfs_inherit_property_preserves_literal_property_for_local_exec() {
	log="$TEST_TMPDIR/inherit_property_local.log"
	l_property="user:test\$\\\`\"\\\\"

	run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_inherit_property "$l_property" "backup/dst"

	unset -f run_destination_zfs_cmd

	assertEquals "Live local property inheritance should pass the literal property name to zfs without shell-style escaping." \
		"$(printf '%s\n' "inherit" "$l_property" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_inherit_property_preserves_literal_property_for_remote_exec_with_wrapper_host_spec() {
	fake_ssh="$TEST_TMPDIR/fake_ssh_join_exec_inherit"
	fake_doas="$TEST_TMPDIR/doas"
	remote_zfs="$TEST_TMPDIR/fake_remote_zfs_inherit"
	ssh_log="$TEST_TMPDIR/fake_ssh_join_exec_inherit.log"
	remote_log="$TEST_TMPDIR/fake_remote_zfs_inherit.log"
	l_property="user:test'quote"
	old_g_cmd_ssh=${g_cmd_ssh-}
	old_target_host=$g_option_T_target_host
	old_target_cmd_zfs=${g_target_cmd_zfs-}
	old_fake_remote_path=${FAKE_REMOTE_PATH-}

	cat >"$fake_ssh" <<'EOF'
#!/bin/sh
host=$1
shift
remote_cmd=""
for arg in "$@"; do
	if [ "$remote_cmd" = "" ]; then
		remote_cmd=$arg
	else
		remote_cmd="$remote_cmd $arg"
	fi
done
if [ -n "${FAKE_SSH_LOG:-}" ]; then
	printf '%s\n' "$host" >>"$FAKE_SSH_LOG"
	printf '%s\n' "$remote_cmd" >>"$FAKE_SSH_LOG"
fi
PATH=${FAKE_REMOTE_PATH:-$PATH}
export PATH
/bin/sh -c "$remote_cmd"
EOF
	chmod +x "$fake_ssh"

	cat >"$fake_doas" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$fake_doas"

	cat >"$remote_zfs" <<'EOF'
#!/bin/sh
printf '%s\n' "$@" >"$ZXFER_REMOTE_ZFS_LOG"
EOF
	chmod +x "$remote_zfs"

	FAKE_SSH_LOG="$ssh_log"
	FAKE_REMOTE_PATH="$TEST_TMPDIR:$PATH"
	ZXFER_REMOTE_ZFS_LOG="$remote_log"
	export FAKE_SSH_LOG FAKE_REMOTE_PATH ZXFER_REMOTE_ZFS_LOG

	g_option_n_dryrun=0
	g_cmd_ssh="$fake_ssh"
	g_option_T_target_host="target.example doas"
	g_target_cmd_zfs="$remote_zfs"

	zxfer_run_zfs_inherit_property "$l_property" "backup/dst"

	unset FAKE_SSH_LOG ZXFER_REMOTE_ZFS_LOG
	if [ -n "${old_fake_remote_path:+set}" ]; then
		FAKE_REMOTE_PATH=$old_fake_remote_path
		export FAKE_REMOTE_PATH
	else
		unset FAKE_REMOTE_PATH
	fi
	g_cmd_ssh=$old_g_cmd_ssh
	g_option_T_target_host=$old_target_host
	g_target_cmd_zfs=$old_target_cmd_zfs

	assertEquals "Remote property inheritance should preserve literal property names after ssh joins the remote command into a shell string." \
		"$(printf '%s\n' "inherit" "$l_property" "backup/dst")" "$(cat "$remote_log")"
	assertEquals "Wrapper-style target specs should still keep the ssh destination host separate from wrapper tokens." \
		"target.example" "$(sed -n '1p' "$ssh_log")"
	assertContains "Wrapper-style target specs should preserve the wrapper token in the remote shell command." \
		"$(sed -n '2p' "$ssh_log")" "'doas'"
}

test_diff_properties_rejects_must_create_mismatches() {
	set +e
	output=$(
		(
			throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			diff_properties "casesensitivity=mixed=local" "casesensitivity=sensitive=local" "casesensitivity"
		)
	)
	status=$?

	assertEquals "Must-create property mismatches should abort." 1 "$status"
	assertContains "Must-create mismatches should explain that the property may only be set at creation time." \
		"$output" "may only be set"
}

test_diff_properties_sets_local_value_when_destination_source_is_inherited() {
	outfile="$TEST_TMPDIR/diff_set_local.out"

	diff_properties "compression=lz4=local" "compression=lz4=inherited" "" >"$outfile"

	assertEquals "Initial-source property sets should still include matching values when the destination source is not local." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Child property sets should force local values when the destination inherits the same value." \
		"compression=lz4" "$(sed -n '2p' "$outfile")"
	assertEquals "No inherit list should be produced when the source is already local." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_inherits_value_when_destination_is_local_but_source_is_not() {
	outfile="$TEST_TMPDIR/diff_inherit_same_value.out"

	diff_properties "compression=lz4=inherited" "compression=lz4=local" "" >"$outfile"

	assertEquals "No initial-source set list is needed when the destination already has the matching local value." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "No child set list should be produced when the source value is inherited." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Child property diffs should request inheritance when the destination has a local copy of an inherited source value." \
		"compression=lz4" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_treats_overrides_as_local_sets() {
	outfile="$TEST_TMPDIR/diff_override_local.out"

	diff_properties "quota=32M=override" "quota=32M=local" "" >"$outfile"

	assertEquals "Matching local override values should not request any additional root-level set." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching local override values should not request a child set." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Matching local override values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"

	diff_properties "quota=32M=override" "quota=none=local" "" >"$outfile"

	assertEquals "Changed override values should still appear in the root set list." \
		"quota=32M" "$(sed -n '1p' "$outfile")"
	assertEquals "Changed override values should be set locally on child datasets." \
		"quota=32M" "$(sed -n '2p' "$outfile")"
	assertEquals "Changed override values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			diff_properties "compression=lz4=local" "compression=lz4=local" ""
		)
	)
	status=$?

	assertEquals "Property diffing should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Property diffing awk failures should surface the helper failure message." \
		"$output" "Failed to diff dataset properties."
}

test_adjust_child_inherit_to_match_parent_promotes_mismatched_parent_values_to_sets() {
	outfile="$TEST_TMPDIR/adjust_child_inherit.out"

	(
		exists_destination() {
			printf '1\n'
		}
		collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,atime=on=local"
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,atime=off=inherited" \
			"quota=32M" \
			"checksum=sha256,atime=off" \
			"$g_readonly_properties"
	) >"$outfile"

	assertEquals "Parent-matching inherited properties should remain in the inherit list." \
		"quota=32M,atime=off" "$(sed -n '1p' "$outfile")"
	assertEquals "Only properties whose parent already matches should remain inherited." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_preserves_inherit_when_parent_matches() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_match.out"

	(
		exists_destination() {
			printf '1\n'
		}
		collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,atime=off=local"
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,atime=off=inherited" \
			"" \
			"checksum=sha256,atime=off" \
			"$g_readonly_properties"
	) >"$outfile"

	assertEquals "When the parent already has the desired values, no local sets are needed." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching parent values should preserve inheritance requests." \
		"checksum=sha256,atime=off" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_moves_inherited_source_properties_out_of_set_list_when_parent_matches() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_demote.out"

	(
		exists_destination() {
			printf '1\n'
		}
		collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,compression=lz4=local"
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,compression=lz4=local" \
			"checksum=sha256,compression=lz4" \
			"" \
			"$g_readonly_properties"
	) >"$outfile"

	assertEquals "Inherited source properties should be removed from the child set list when the parent already provides the same value." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Inherited source properties whose parent already matches should be converted back into inherit operations." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_uses_supplied_readonly_list() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_readonly.out"

	(
		exists_destination() {
			printf '1\n'
		}
		collect_destination_props() {
			printf '%s\n' "compression=lz4=local,atime=off=local"
		}
		g_readonly_properties=""
		g_option_I_ignore_properties=""
		adjust_child_inherit_to_match_parent "backup/dst/child" \
			"compression=lz4=inherited,atime=off=inherited" \
			"" \
			"compression=lz4,atime=off" \
			"atime"
	) >"$outfile"

	assertEquals "Supplied readonly properties should be removed from the parent comparison set before deciding whether a child may inherit." \
		"atime=off" "$(sed -n '1p' "$outfile")"
	assertEquals "Only properties still visible after readonly filtering should remain inherited." \
		"compression=lz4" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_when_inherit_list_is_empty() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_empty.out"

	(
		exists_destination() {
			printf '1\n'
		}
		collect_destination_props() {
			printf '%s\n' "compression=lz4=local"
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		adjust_child_inherit_to_match_parent "backup/dst/child" \
			"compression=lz4=local" \
			"compression=lz4" \
			"" \
			""
	) >"$outfile"

	assertEquals "Child-inherit adjustment should preserve the existing set list when there is no inherit list to reconcile." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Child-inherit adjustment should emit an empty inherit list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_empty_lists_when_nothing_needs_reconciliation() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_both_empty.out"

	adjust_child_inherit_to_match_parent "backup/dst/child" "" "" "" "" >"$outfile"

	assertEquals "Child-inherit adjustment should leave an empty set list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Child-inherit adjustment should leave an empty inherit list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_for_root_dataset() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_root.out"

	adjust_child_inherit_to_match_parent "backup" \
		"compression=lz4=local" \
		"compression=lz4" \
		"quota=none" \
		"" >"$outfile"

	assertEquals "Root datasets should preserve the existing set list because there is no parent dataset to inspect." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Root datasets should preserve the inherit list because there is no parent dataset to inspect." \
		"quota=none" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_when_parent_is_missing() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_parent_missing.out"

	(
		exists_destination() {
			printf '0\n'
		}
		adjust_child_inherit_to_match_parent "backup/dst/child" \
			"compression=lz4=local" \
			"compression=lz4" \
			"quota=none" \
			""
	) >"$outfile"

	assertEquals "Missing destination parents should preserve the existing set list." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Missing destination parents should preserve the existing inherit list." \
		"quota=none" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			exists_destination() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		)
	)
	status=$?

	assertEquals "Parent existence probe failures should abort child-inherit reconciliation." \
		"1" "$status"
	assertContains "Parent existence probe failures should surface the original probe error." \
		"$output" "ssh timeout"
}

test_adjust_child_inherit_to_match_parent_returns_failure_when_parent_props_cannot_be_loaded() {
	set +e
	output=$(
		(
			exists_destination() {
				printf '1\n'
			}
			collect_destination_props() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=local" \
				"compression=lz4" \
				"quota=none" \
				""
		)
	)
	status=$?

	assertEquals "Parent-property load failures should abort child-inherit reconciliation." \
		"1" "$status"
	assertEquals "Parent-property load failures should not emit partial adjusted lists." \
		"" "$output"
}

test_adjust_child_inherit_to_match_parent_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			exists_destination() {
				printf '1\n'
			}
			collect_destination_props() {
				printf '%s\n' "compression=lz4=local"
			}
			sanitize_property_list() {
				printf '%s\n' "$1"
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		)
	)
	status=$?

	assertEquals "Child-inherit reconciliation should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Child-inherit reconciliation awk failures should surface the helper failure message." \
		"$output" "Failed to reconcile child property inheritance."
}

test_apply_property_changes_uses_default_runners_when_unspecified() {
	log="$TEST_TMPDIR/apply_default_runners.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_run_zfs_set_properties() {
			printf 'set %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		zxfer_run_zfs_inherit_property() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		apply_property_changes "backup/dst" 0 "" "compression=lz4,atime=off" "quota=none" "" ""
	)

	assertEquals "Default property runners should be used when no custom runner is supplied." \
		"set compression=lz4,atime=off backup/dst
inherit quota backup/dst" "$(cat "$log")"
}

test_apply_property_changes_logs_when_child_only_inherits() {
	log="$TEST_TMPDIR/apply_inherit_only.log"
	: >"$log"

	(
		echov() {
			printf '%s\n' "$*" >>"$log"
		}
		inherit_runner() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$log"
		}

		apply_property_changes "backup/dst" 0 "" "" "quota=none" "" inherit_runner
	)

	assertContains "Child-only inheritance changes should still log the property-update banner." \
		"$(cat "$log")" "Setting properties/sources on destination filesystem \"backup/dst\"."
	assertContains "Child-only inheritance changes should still call the inherit runner." \
		"$(cat "$log")" "inherit quota backup/dst"
}

test_apply_property_changes_logs_decoded_delimiter_heavy_values() {
	log="$TEST_TMPDIR/apply_encoded_display.log"
	: >"$log"

	(
		echov() {
			printf '%s\n' "$*" >>"$log"
		}
		set_runner() {
			:
		}
		inherit_runner() {
			:
		}

		apply_property_changes "backup/dst" 0 "" \
			"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" \
			"user:inherit=value%2Ctwo" \
			set_runner inherit_runner
	)

	assertContains "Verbose property-set summaries should decode delimiter-heavy values before logging." \
		"$(cat "$log")" "Property set list: user:note=value,with,commas=and;semi"
	assertContains "Verbose property-inherit summaries should decode delimiter-heavy values before logging." \
		"$(cat "$log")" "Property inherit list: user:inherit=value,two"
}

test_apply_property_changes_batches_multiple_child_sets_in_one_runner_call() {
	log="$TEST_TMPDIR/apply_child_batch.log"
	: >"$log"

	(
		LOG_FILE="$log"
		set_runner() {
			printf 'set %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		inherit_runner() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		apply_property_changes "backup/dst/child" 0 "" "compression=lz4,atime=off" "quota=none" set_runner inherit_runner
	)

	assertEquals "Child-property application should batch all set operations into one set-runner call while still inheriting properties one at a time." \
		"set compression=lz4,atime=off backup/dst/child
inherit quota backup/dst/child" "$(cat "$log")"
}

test_transfer_properties_fails_when_source_property_collection_fails() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="permission denied"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			transfer_properties "tank/src"
		)
	)
	status=$?

	assertEquals "Property transfer should fail closed when source property collection fails." \
		"1" "$status"
	assertEquals "Property transfer should surface the source property-collection failure output." \
		"permission denied" "$output"
}

test_transfer_properties_marks_created_destinations_and_records_backup() {
	log="$TEST_TMPDIR/transfer_create.log"
	: >"$log"

	(
		LOG_FILE="$log"
		collect_source_props() {
			m_source_pvs_raw="compression=lz4=local"
			m_source_pvs_effective="compression=lz4=local"
		}
		run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		validate_override_properties() {
			printf 'validate %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		derive_override_lists() {
			printf 'compression=lz4=local\n'
			printf '\n'
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		ensure_destination_exists() {
			printf 'ensure %s\n' "$2" >>"$LOG_FILE"
			return 0
		}
		g_option_k_backup_property_mode=1
		initial_source="tank/src"
		g_actual_dest="backup/dst"
		transfer_properties "tank/src"
		printf 'created=%s\n' "$g_dest_created_by_zxfer" >>"$LOG_FILE"
		printf 'backup=%s\n' "$g_backup_file_contents" >>"$LOG_FILE"
	)

	result=$(cat "$log")
	assertContains "Initial-source transfer should validate override properties." \
		"$result" "validate  compression=lz4=local"
	assertContains "Successful destination creation should mark the dataset as zxfer-created." \
		"$result" "created=1"
	assertContains "Backup mode should append raw source properties for later restore." \
		"$result" "backup=;tank/src,backup/dst,compression=lz4=local"
}

test_transfer_properties_diffs_existing_destinations_and_applies_changes() {
	log="$TEST_TMPDIR/transfer_existing.log"
	: >"$log"

	(
		LOG_FILE="$log"
		collect_source_props() {
			m_source_pvs_raw="compression=lz4=local"
			m_source_pvs_effective="compression=lz4=local"
		}
		run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		derive_override_lists() {
			printf 'compression=lz4=local\n'
			printf '\n'
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		ensure_destination_exists() {
			return 1
		}
		exists_destination() {
			printf '0\n'
		}
		collect_destination_props() {
			printf '%s\n' "compression=off=local"
		}
		diff_properties() {
			printf 'compression=lz4\n'
			printf 'compression=lz4\n'
			printf '\n'
		}
		apply_property_changes() {
			printf 'apply %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" >>"$LOG_FILE"
		}
		g_recursive_dest_list="backup/dst"
		g_actual_dest="backup/dst"
		transfer_properties "tank/src/child"
		printf 'created=%s\n' "$g_dest_created_by_zxfer" >>"$LOG_FILE"
	)

	assertEquals "Existing destinations should diff and apply property changes instead of marking creation." \
		"apply backup/dst 0 compression=lz4 compression=lz4
created=0" "$(cat "$log")"
}

test_transfer_properties_queries_missing_must_create_properties_before_diffing() {
	log="$TEST_TMPDIR/transfer_required_create_props.log"
	: >"$log"

	(
		LOG_FILE="$log"
		collect_source_props() {
			m_source_pvs_raw="compression=lz4=local"
			m_source_pvs_effective="compression=lz4=local"
		}
		run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		ensure_required_properties_present() {
			printf 'ensure-required %s %s %s\n' "$1" "$2" "$4" >>"$LOG_FILE"
			case "$1" in
			tank/src) printf '%s\n' "compression=lz4=local,casesensitivity=sensitive=local" ;;
			backup/dst) printf '%s\n' "compression=off=local,casesensitivity=insensitive=local" ;;
			esac
		}
		validate_override_properties() {
			:
		}
		derive_override_lists() {
			printf 'compression=lz4=local,casesensitivity=sensitive=local\n'
			printf '\n'
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		ensure_destination_exists() {
			return 1
		}
		collect_destination_props() {
			printf '%s\n' "compression=off=local"
		}
		diff_properties() {
			printf 'diff %s || %s || %s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			printf '\n'
			printf '\n'
			printf '\n'
		}
		apply_property_changes() {
			:
		}
		g_recursive_dest_list="backup/dst"
		g_actual_dest="backup/dst"
		transfer_properties "tank/src"
	)

	result=$(cat "$log")
	assertContains "Source properties should be augmented with missing must-create entries before diffing." \
		"$result" "ensure-required tank/src compression=lz4=local casesensitivity,normalization,jailed,utf8only"
	assertContains "Destination properties should be augmented with missing must-create entries before diffing." \
		"$result" "ensure-required backup/dst compression=off=local casesensitivity,normalization,jailed,utf8only"
	assertContains "Property diffing should run after the must-create source properties are appended." \
		"$result" "compression=lz4=local,casesensitivity=sensitive=local"
	assertContains "Property diffing should run after the must-create destination properties are appended." \
		"$result" "compression=off=local,casesensitivity=insensitive=local"
	assertContains "Property diffing should still receive the must-create property list." \
		"$result" "casesensitivity,normalization,jailed,utf8only"
}

test_transfer_properties_propagates_must_create_diff_failures() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="compression=lz4=local"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			validate_override_properties() {
				:
			}
			derive_override_lists() {
				printf 'compression=lz4=local\n'
				printf '\n'
			}
			sanitize_property_list() {
				printf '%s\n' "$1"
			}
			strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			ensure_destination_exists() {
				return 1
			}
			collect_destination_props() {
				printf '%s\n' "compression=off=local"
			}
			ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			diff_properties() {
				throw_error_with_usage "must-create mismatch"
			}
			throw_error_with_usage() {
				printf '%s\n' "$1" >&2
				exit 2
			}
			get_temp_file() {
				printf '%s\n' "$TEST_TMPDIR/transfer_diff_failure.tmp"
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			transfer_properties "tank/src"
		) 2>&1
	)
	status=$?

	assertEquals "Must-create diff failures should propagate out of transfer_properties." 2 "$status"
	assertContains "Must-create diff failures should preserve the diff error text." \
		"$output" "must-create mismatch"
}

test_transfer_properties_fails_when_source_required_property_probe_fails() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="compression=lz4=local"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			run_zfs_cmd_for_spec() {
				if [ "$5" = "casesensitivity" ] && [ "$6" = "tank/src" ]; then
					printf '%s\n' "permission denied"
					return 1
				fi
				printf '%s\n' "unexpected probe $*"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_actual_dest="backup/dst"
			transfer_properties "tank/src"
		)
	)
	status=$?

	assertEquals "Source must-create probe failures should abort property transfer." 1 "$status"
	assertContains "Property transfer should preserve the source required-property probe failure." \
		"$output" "Failed to retrieve required creation-time property [casesensitivity] for dataset [tank/src]: permission denied"
}

test_transfer_properties_fails_when_effective_source_required_property_probe_fails() {
	set +e
	output=$(
		(
			required_call_count=0
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="compression=lz4=local"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			ensure_required_properties_present() {
				required_call_count=$((required_call_count + 1))
				if [ "$required_call_count" -eq 1 ]; then
					printf '%s\n' "$2"
					return 0
				fi
				printf '%s\n' "effective property probe failed"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_actual_dest="backup/dst"
			transfer_properties "tank/src"
		)
	)
	status=$?

	assertEquals "Effective source must-create probe failures should abort property transfer." 1 "$status"
	assertEquals "Property transfer should preserve the effective source required-property probe failure." \
		"effective property probe failed" "$output"
}

test_transfer_properties_fails_when_destination_required_property_probe_fails() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local,casesensitivity=sensitive=local,normalization=none=local,jailed=off=local,utf8only=on=local"
				m_source_pvs_effective="$m_source_pvs_raw"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			run_zfs_cmd_for_spec() {
				if [ "$5" = "casesensitivity" ] && [ "$6" = "backup/dst" ]; then
					printf '%s\n' "ssh timeout"
					return 1
				fi
				printf '%s\n' "invalid property"
				return 1
			}
			validate_override_properties() {
				:
			}
			derive_override_lists() {
				printf 'compression=lz4=local,casesensitivity=sensitive=local\n'
				printf '\n'
			}
			sanitize_property_list() {
				printf '%s\n' "$1"
			}
			strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			ensure_destination_exists() {
				return 1
			}
			collect_destination_props() {
				printf '%s\n' "compression=off=local,normalization=none=local,jailed=off=local,utf8only=on=local"
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			transfer_properties "tank/src"
		)
	)
	status=$?

	assertEquals "Destination must-create probe failures should abort property transfer." 1 "$status"
	assertContains "Property transfer should preserve the destination required-property probe failure." \
		"$output" "Failed to retrieve required creation-time property [casesensitivity] for dataset [backup/dst]: ssh timeout"
}

test_transfer_properties_fails_when_destination_property_collection_fails() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="compression=lz4=local"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			validate_override_properties() {
				:
			}
			derive_override_lists() {
				printf 'compression=lz4=local\n'
				printf '\n'
			}
			sanitize_property_list() {
				printf '%s\n' "$1"
			}
			strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			ensure_destination_exists() {
				return 1
			}
			collect_destination_props() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			transfer_properties "tank/src"
		)
	)
	status=$?

	assertEquals "Destination property collection failures should abort property transfer." 1 "$status"
	assertEquals "Destination property collection failures should use the destination-property retrieval error." \
		"Failed to retrieve destination properties for [backup/dst]." "$output"
}

test_transfer_properties_skips_filesystem_only_required_property_probes_for_volumes() {
	log="$TEST_TMPDIR/transfer_volume_required_props.log"
	: >"$log"

	(
		LOG_FILE="$log"
		collect_source_props() {
			m_source_pvs_raw="compression=lz4=local"
			m_source_pvs_effective="$m_source_pvs_raw"
		}
		run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "volume"
			elif [ "$4" = "volsize" ]; then
				printf '%s\n' "8M"
			else
				printf '%s\n' "-"
			fi
		}
		ensure_required_properties_present() {
			printf 'ensure-required %s %s %s\n' "$1" "$2" "$4" >>"$LOG_FILE"
			if [ -n "$4" ]; then
				printf '%s\n' "unexpected required property list: $4"
				exit 1
			fi
			printf '%s\n' "$2"
		}
		validate_override_properties() {
			:
		}
		derive_override_lists() {
			printf 'compression=lz4=local\n'
			printf '\n'
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		ensure_destination_exists() {
			printf 'ensure %s %s %s\n' "$5" "$6" "$7" >>"$LOG_FILE"
			return 0
		}
		initial_source="tank/vol"
		g_actual_dest="backup/vol"
		transfer_properties "tank/vol"
		printf 'created=%s\n' "$g_dest_created_by_zxfer" >>"$LOG_FILE"
	)

	assertEquals "Volume transfers should not probe filesystem-only creation-time properties before creation." \
		"ensure-required tank/vol compression=lz4=local
ensure-required tank/vol compression=lz4=local
ensure volume 8M backup/vol
created=1" "$(cat "$log")"
}

test_transfer_properties_fails_when_source_type_probe_fails() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="$m_source_pvs_raw"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "permission denied"
					return 1
				fi
				printf '%s\n' "unexpected $*"
				return 1
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			transfer_properties "tank/src"
		)
	)
	status=$?

	assertEquals "Source type probe failures should abort property transfer." 1 "$status"
	assertContains "Property transfer should preserve the source type probe failure." \
		"$output" "Failed to retrieve source dataset type for [tank/src]: permission denied"
}

test_transfer_properties_fails_when_source_volume_size_probe_fails() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="$m_source_pvs_raw"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "volume"
				elif [ "$4" = "volsize" ]; then
					printf '%s\n' "ssh timeout"
					return 1
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			transfer_properties "tank/vol"
		)
	)
	status=$?

	assertEquals "Source volume-size probe failures should abort property transfer." 1 "$status"
	assertContains "Property transfer should preserve the source zvol size probe failure." \
		"$output" "Failed to retrieve source zvol size for [tank/vol]: ssh timeout"
}

test_transfer_properties_fails_when_source_volume_size_is_empty() {
	set +e
	output=$(
		(
			collect_source_props() {
				m_source_pvs_raw="compression=lz4=local"
				m_source_pvs_effective="$m_source_pvs_raw"
			}
			run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "volume"
				elif [ "$4" = "volsize" ]; then
					printf '\n'
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			transfer_properties "tank/vol"
		)
	)
	status=$?

	assertEquals "Empty source volume sizes should abort property transfer." 1 "$status"
	assertContains "Property transfer should reject empty zvol sizes." \
		"$output" "Failed to retrieve source zvol size for [tank/vol]: empty volsize"
}

test_transfer_properties_forces_readonly_overrides_in_current_shell() {
	log="$TEST_TMPDIR/transfer_writable.log"
	: >"$log"
	g_option_k_backup_property_mode=1
	g_ensure_writable=1
	g_option_o_override_property="readonly=on"
	initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	collect_source_props() {
		m_source_pvs_raw="readonly=on=local,compression=lz4=local"
		m_source_pvs_effective="readonly=off=local,compression=lz4=local"
	}
	run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	validate_override_properties() {
		printf 'validate %s\n' "$1" >>"$log"
	}
	derive_override_lists() {
		printf 'readonly=off=override,compression=lz4=local\n'
		printf '\n'
	}
	sanitize_property_list() {
		printf '%s\n' "$1"
	}
	strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	ensure_destination_exists() {
		return 0
	}

	transfer_properties "tank/src"

	unset -f collect_source_props
	unset -f run_source_zfs_cmd
	unset -f validate_override_properties
	unset -f derive_override_lists
	unset -f sanitize_property_list
	unset -f strip_unsupported_properties
	unset -f ensure_destination_exists

	assertContains "Writable-mode transfers should validate overrides after forcing readonly=off." \
		"$(cat "$log")" "validate readonly=off"
	assertEquals "Writable-mode backup capture should preserve the raw source properties for restore mode." \
		";tank/src,backup/dst,readonly=on=local,compression=lz4=local" "$g_backup_file_contents"
	assertEquals "Created destinations should still be tracked in current-shell transfer tests." 1 "$g_dest_created_by_zxfer"
}

test_transfer_properties_skip_backup_capture_preserves_existing_backup_contents() {
	g_option_k_backup_property_mode=1
	g_backup_file_contents="existing"
	g_recursive_dest_list=""
	initial_source="tank/src"
	g_actual_dest="backup/dst"
	collect_source_props() {
		m_source_pvs_raw="readonly=on=local,compression=lz4=local"
		m_source_pvs_effective="$m_source_pvs_raw"
	}
	run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	validate_override_properties() {
		:
	}
	derive_override_lists() {
		printf 'readonly=on=local,compression=lz4=local\n'
		printf '\n'
	}
	sanitize_property_list() {
		printf '%s\n' "$1"
	}
	strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	ensure_destination_exists() {
		return 0
	}

	transfer_properties "tank/src" 1

	unset -f collect_source_props
	unset -f run_source_zfs_cmd
	unset -f validate_override_properties
	unset -f derive_override_lists
	unset -f sanitize_property_list
	unset -f strip_unsupported_properties
	unset -f ensure_destination_exists

	assertEquals "Post-seed reconciliation should not duplicate -k backup metadata." \
		"existing" "$g_backup_file_contents"
}

test_transfer_properties_adjusts_child_inherit_lists_for_existing_children() {
	log="$TEST_TMPDIR/transfer_child_adjust.log"
	: >"$log"
	(
		g_option_V_very_verbose=1
		initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list="backup/dst
backup/dst/child"

		collect_source_props() {
			m_source_pvs_raw="compression=lz4=inherited"
			m_source_pvs_effective="$m_source_pvs_raw"
		}
		run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		derive_override_lists() {
			printf 'compression=lz4=inherited\n'
			printf '\n'
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		exists_destination() {
			printf '1\n'
		}
		diff_properties() {
			printf '\n'
			printf '\n'
			printf 'compression=lz4\n'
		}
		collect_destination_props() {
			case "$1" in
			backup/dst/child) printf '%s\n' "compression=lz4=local" ;;
			backup/dst) printf '%s\n' "compression=lz4=local" ;;
			*)
				printf '%s\n' "unexpected dataset $1"
				return 1
				;;
			esac
		}
		apply_property_changes() {
			printf 'apply %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$log"
		}
		ensure_destination_exists() {
			return 1
		}

		transfer_properties "tank/src/child"
	) >"$log" 2>&1

	assertContains "Child transfers should reconcile inherit-vs-set state before applying destination property changes." \
		"$(cat "$log")" "transfer_properties adjusted child_set:"
	assertContains "Child transfers should preserve the reconciled inherit list in very-verbose output." \
		"$(cat "$log")" "transfer_properties adjusted inherit:"
}

test_transfer_properties_adjusts_set_only_inherited_child_properties_for_existing_children() {
	log="$TEST_TMPDIR/transfer_child_set_only_adjust.log"
	: >"$log"
	(
		g_option_V_very_verbose=1
		initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list="backup/dst
backup/dst/child"

		collect_source_props() {
			m_source_pvs_raw="checksum=sha256=inherited"
			m_source_pvs_effective="$m_source_pvs_raw"
		}
		run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		derive_override_lists() {
			printf 'checksum=sha256=inherited\n'
			printf '\n'
		}
		sanitize_property_list() {
			printf '%s\n' "$1"
		}
		strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		exists_destination() {
			printf '1\n'
		}
		diff_properties() {
			printf '\n'
			printf 'checksum=sha256\n'
			printf '\n'
		}
		collect_destination_props() {
			case "$1" in
			backup/dst/child) printf '%s\n' "checksum=fletcher4=local" ;;
			backup/dst) printf '%s\n' "checksum=sha256=local" ;;
			*)
				printf '%s\n' "unexpected dataset $1"
				return 1
				;;
			esac
		}
		apply_property_changes() {
			printf 'apply %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" >>"$log"
		}
		ensure_destination_exists() {
			return 1
		}

		transfer_properties "tank/src/child"
	) >"$log" 2>&1

	assertContains "Child transfers should still reconcile inherited-source properties that initially appear only in the set list." \
		"$(cat "$log")" "transfer_properties adjusted child_set: "
	assertContains "Set-only inherited-source properties should be demoted back into the inherit list when the parent already matches." \
		"$(cat "$log")" "transfer_properties adjusted inherit: checksum=sha256"
	assertContains "The final property application should inherit the reconciled property instead of setting it locally." \
		"$(cat "$log")" "apply backup/dst/child 0   checksum=sha256"
}

test_transfer_properties_uses_freebsd_readonly_properties_without_mutating_global_state() {
	log="$TEST_TMPDIR/transfer_freebsd_readonly.log"
	: >"$log"
	g_destination_operating_system="FreeBSD"
	g_source_operating_system="Linux"
	g_readonly_properties="readonly"
	g_fbsd_readonly_properties="aclmode"
	initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	collect_source_props() {
		m_source_pvs_raw="compression=lz4=local"
		m_source_pvs_effective="compression=lz4=local"
	}
	run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	validate_override_properties() {
		:
	}
	derive_override_lists() {
		printf '\n'
		printf '\n'
	}
	sanitize_property_list() {
		printf '%s\n' "$2" >>"$log"
		printf '%s\n' "$1"
	}
	strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	ensure_destination_exists() {
		return 0
	}

	transfer_properties "tank/src"
	transfer_properties "tank/src"

	unset -f collect_source_props
	unset -f run_source_zfs_cmd
	unset -f validate_override_properties
	unset -f derive_override_lists
	unset -f sanitize_property_list
	unset -f strip_unsupported_properties
	unset -f ensure_destination_exists

	assertEquals "FreeBSD-specific readonly properties should be applied per transfer without mutating the global base list." \
		"readonly" "$g_readonly_properties"
	assertEquals "Repeated transfers should reuse the same effective FreeBSD readonly list instead of appending duplicates." \
		"readonly,aclmode
readonly,aclmode" "$(cat "$log")"
}

test_transfer_properties_uses_solexp_readonly_properties_without_mutating_global_state() {
	log="$TEST_TMPDIR/transfer_solexp_readonly.log"
	: >"$log"
	g_destination_operating_system="SunOS"
	g_source_operating_system="FreeBSD"
	g_readonly_properties="readonly"
	g_solexp_readonly_properties="jailed"
	initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	collect_source_props() {
		m_source_pvs_raw="compression=lz4=local"
		m_source_pvs_effective="compression=lz4=local"
	}
	run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	validate_override_properties() {
		:
	}
	derive_override_lists() {
		printf '\n'
		printf '\n'
	}
	sanitize_property_list() {
		printf '%s\n' "$2" >>"$log"
		printf '%s\n' "$1"
	}
	strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	ensure_destination_exists() {
		return 0
	}

	transfer_properties "tank/src"
	transfer_properties "tank/src"

	unset -f collect_source_props
	unset -f run_source_zfs_cmd
	unset -f validate_override_properties
	unset -f derive_override_lists
	unset -f sanitize_property_list
	unset -f strip_unsupported_properties
	unset -f ensure_destination_exists

	assertEquals "SunOS-specific readonly properties should be applied per transfer without mutating the global base list." \
		"readonly" "$g_readonly_properties"
	assertEquals "Repeated FreeBSD-to-SunOS transfers should reuse the same effective readonly list instead of appending duplicates." \
		"readonly,jailed
readonly,jailed" "$(cat "$log")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
